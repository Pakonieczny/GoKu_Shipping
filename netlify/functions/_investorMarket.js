/*  netlify/functions/_investorMarket.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — market data lane: adapters, quality grading, bar storage,
 *  and the execution clock.
 *
 *  THE ONE PLACE THE NO-NEW-VENDOR RULE IS RELAXED
 *  ------------------------------------------------------------------------
 *  The research settled this: 5-minute refresh with live charts is impossible
 *  without a market-data account. Every other lane in this system stays
 *  zero-credential. This module supports three providers behind one interface
 *  so the choice is configuration, not code:
 *
 *    alpaca   free tier, no card. IEX-only real-time (one venue, ~2.5-3% of
 *             consolidated volume) + 15-min-delayed consolidated via REST.
 *             Quality grade capped at B because single-venue prints gap.
 *    massive  (ex-Polygon) Stocks Starter $29/mo. 15-min delayed consolidated,
 *             UNLIMITED calls — the decisive feature at 288 cycles/day.
 *    manual   CSV import. Always available, always permitted, grade C.
 *
 *  Yahoo is absent by design and cannot be added: _investorFetch denies those
 *  hosts permanently.
 *
 *  QUALITY GRADES gate execution, they are not decoration:
 *    A  two permitted sources agree within tolerance                 normal
 *    B  one permitted consolidated source, complete OHLCV            wider slippage
 *    C  single-venue / delayed / manual                              research only
 *    F  stale, contradictory, impossible OHLC, no provenance         FREEZE
 *
 *  THE EXECUTION CLOCK — the anti-look-ahead invariant
 *  ------------------------------------------------------------------------
 *  A bar's timestamp is NOT the time this system could have seen it. With a
 *  15-minute-delayed feed, the 10:00 bar is retrievable at 10:15 at the
 *  earliest. firstEligibleBar() therefore takes the decision time, adds the
 *  feed delay, adds configured execution latency, and returns the first bar
 *  whose OPEN is strictly after that instant. An order is committed and sized
 *  BEFORE that bar begins, and no future high/low/close/volume may influence
 *  it. Realized bar data is a diagnostic, never an input.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const { fetchPublic } = require("./_investorFetch");

/* ── provider configuration ────────────────────────────────────────────── */
const PROVIDERS = {
  alpaca: {
    id: "alpaca",
    host: "data.alpaca.markets",
    delayMinutes: 0,
    maxGrade: "C",
    keyEnv: ["ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"],
    consolidated: false,
    liquidityEligible: false,
  },
  massive: {
    id: "massive",
    host: "api.massive.com",
    delayMinutes: 15,
    maxGrade: "A",
    keyEnv: ["MASSIVE_API_KEY"],
    consolidated: true,
    liquidityEligible: true,
  },
  manual: {
    id: "manual",
    host: null,
    delayMinutes: 24 * 60,
    maxGrade: "C",
    keyEnv: [],
    consolidated: false,
    liquidityEligible: false,
  },
};

/* ── market settings: Firestore first, environment as fallback ──────────
 *
 * `INVESTOR_MARKET_PROVIDER` and `ALPACA_FEED` are static operator choices,
 * not per-environment secrets, and every variable a Lambda can read counts
 * against its 4KB environment budget. They live in
 * InvestorAI_Control/marketConfig instead.
 *
 * Resolution order is Firestore -> environment -> a complete Alpaca
 * credential pair on delayed_sip -> "manual"/"iex". The environment path is
 * kept so tests, local runs and an unreachable Firestore remain deterministic.
 * An unrecognised provider or feed resolves to
 * the same place an unset one does: manual, grade C, not tradable. There is
 * no value of this document that can turn an untrusted feed into a tradable
 * one — gradeSeries still decides that.
 *
 * The cache is read synchronously by providerConfig/activeProvider, which are
 * called from non-async paths. Handlers call loadMarketSettings() once on
 * entry; the TTL bounds how long a warm container can hold a stale choice. */
const MARKET_SETTINGS_DOC = "marketConfig";
const MARKET_SETTINGS_TTL_MS = 60000;
const FEEDS = new Set(["iex", "delayed_sip", "sip", "otc"]);
let _marketSettings = null;
let _marketSettingsAtMs = 0;

/* The one place a provider/feed choice is judged, whichever layer offered it.
   Extracted so the invariant "an unrecognised or absent lane resolves to
   manual/iex" can be attested as a PURE function. Attesting it through
   marketSettings() instead was wrong: that reads a cache which, once Firestore
   has answered, no longer depends on the environment at all — so the check
   passed on a cold process and failed in the deployed one, where the cache is
   always warm. A fixture whose result depends on call order attests nothing. */
function normalizeMarketChoice(rawProvider, rawFeed) {
  const p = String(rawProvider == null ? "" : rawProvider).toLowerCase();
  const f = String(rawFeed == null ? "" : rawFeed).toLowerCase();
  return {
    provider: PROVIDERS[p] ? p : "manual",
    feed: FEEDS.has(f) ? f : "iex",
    providerRecognised: !!PROVIDERS[p],
    feedRecognised: FEEDS.has(f),
  };
}

/* Every name an operator plausibly used for the Alpaca pair. The canonical
   names are first; Alpaca's own SDK convention (APCA_*) and the short forms
   follow. A pair is taken from ONE row only — a key id from one convention is
   never paired with a secret from another. */
const ALPACA_ENV_PAIRS = Object.freeze([
  ["ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"],
  ["APCA_API_KEY_ID", "APCA_API_SECRET_KEY"],
  ["ALPACA_KEY_ID", "ALPACA_SECRET_KEY"],
  ["ALPACA_KEY", "ALPACA_SECRET"],
  ["ALPACA_API_KEY", "ALPACA_API_SECRET"],
]);
const ALPACA_DOC_PAIRS = Object.freeze([
  ["alpacaKeyId", "alpacaSecretKey"],
  ["alpacaApiKeyId", "alpacaApiSecretKey"],
  ["keyId", "secretKey"],
  ["apiKeyId", "apiSecretKey"],
  ["alpacaKey", "alpacaSecret"],
  ["ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"],
  ["APCA_API_KEY_ID", "APCA_API_SECRET_KEY"],
]);
/* Firestore documents under the control collection that may carry the pair.
   marketConfig is canonical; authConfig is where the other operator secrets
   live and where an earlier setup may have put these too. */
const ALPACA_DOC_CANDIDATES = Object.freeze(["marketConfig", "authConfig", "secrets", "credentials"]);

function pairFrom(obj, pairs) {
  for (const [k, sK] of pairs) {
    const keyId = typeof obj[k] === "string" ? obj[k].trim() : "";
    const secretKey = typeof obj[sK] === "string" ? obj[sK].trim() : "";
    if (keyId && secretKey) return { keyId, secretKey, names: `${k}/${sK}` };
  }
  return null;
}
function envAlpacaPair() { return pairFrom(process.env, ALPACA_ENV_PAIRS); }

function envMarketSettings() {
  const envPair = envAlpacaPair();
  const alpacaKeyId = envPair ? envPair.keyId : "";
  const alpacaSecretKey = envPair ? envPair.secretKey : "";
  const completeAlpacaPair = !!(alpacaKeyId && alpacaSecretKey);
  /* A complete pair is an unambiguous, paper-only capability. If no explicit
     provider choice exists, use Alpaca's Basic-plan-safe delayed SIP lane
     instead of silently parking those credentials behind the manual provider. */
  const choice = normalizeMarketChoice(
    process.env.INVESTOR_MARKET_PROVIDER || (completeAlpacaPair ? "alpaca" : undefined),
    process.env.ALPACA_FEED || (completeAlpacaPair ? "delayed_sip" : undefined));
  const sipRealtime = /^(1|true|yes)$/i.test(String(process.env.ALPACA_SIP_REALTIME || ""));
  return {
    provider: choice.provider,
    feed: choice.provider === "alpaca" && choice.feed === "sip" && !sipRealtime
      ? "delayed_sip" : choice.feed,
    alpacaSipRealtime: sipRealtime,
    alpacaKeyId,
    alpacaSecretKey,
    source: process.env.INVESTOR_MARKET_PROVIDER ? "environment"
      : (completeAlpacaPair ? "credential_default" : "default"),
    credentialSource: completeAlpacaPair ? "environment" : "unset",
    credentialNames: envPair ? envPair.names : null,
  };
}

/** Credentials for a provider, resolved the same way as provider/feed. */
function providerCredentials(providerId) {
  const s = marketSettings();
  if (String(providerId || "").toLowerCase() === "alpaca") {
    return { keyId: s.alpacaKeyId || "", secretKey: s.alpacaSecretKey || "" };
  }
  return { keyId: "", secretKey: "" };
}

/** Whether every credential the provider needs has resolved from some layer. */
function providerCredentialed(providerId) {
  const id = String(providerId || "").toLowerCase();
  if (id === "alpaca") {
    const c = providerCredentials("alpaca");
    return !!(c.keyId && c.secretKey);
  }
  if (id === "massive") return !!process.env.MASSIVE_API_KEY;
  return true;
}

/** Synchronous view of the resolved settings. Never throws. */
function marketSettings() {
  if (_marketSettings && Date.now() - _marketSettingsAtMs <= MARKET_SETTINGS_TTL_MS) {
    return _marketSettings;
  }
  return envMarketSettings();
}

/** Refresh the cache from Firestore. Call once per handler invocation. */
async function loadMarketSettings({ force = false } = {}) {
  if (!force && _marketSettings && Date.now() - _marketSettingsAtMs <= MARKET_SETTINGS_TTL_MS) {
    return _marketSettings;
  }
  const fallback = envMarketSettings();
  try {
    const snap = await A.col(A.COL.control).doc(MARKET_SETTINGS_DOC).get();
    /* Where the pair was looked for, in order, and what was found — so the
       Health card can say "looked in X, Y, Z; found in Y" instead of the
       operator and the desk disagreeing about whether keys exist. */
    const lookup = [];
    let docPair = null;
    const d = snap.exists ? (snap.data() || {}) : {};
    docPair = pairFrom(d, ALPACA_DOC_PAIRS);
    lookup.push({ place: `${A.COL.control}/${MARKET_SETTINGS_DOC}`, exists: snap.exists,
      found: !!docPair, names: docPair ? docPair.names : null });
    if (!docPair) {
      for (const docId of ALPACA_DOC_CANDIDATES.filter((x) => x !== MARKET_SETTINGS_DOC)) {
        try {
          const other = await A.col(A.COL.control).doc(docId).get();
          const p = other.exists ? pairFrom(other.data() || {}, ALPACA_DOC_PAIRS) : null;
          lookup.push({ place: `${A.COL.control}/${docId}`, exists: other.exists, found: !!p, names: p ? p.names : null });
          if (p) { docPair = { ...p, names: `${docId}:${p.names}` }; break; }
        } catch (e) {
          lookup.push({ place: `${A.COL.control}/${docId}`, error: String(e.message).slice(0, 80) });
        }
      }
    }
    lookup.push({ place: "environment", found: !!fallback.alpacaKeyId, names: fallback.credentialNames });
    if (!snap.exists && !docPair) {
      _marketSettings = { ...fallback, credentialLookup: lookup, note: "InvestorAI_Control/marketConfig not found" };
    } else {
      const rawProvider = String(d.provider || "").toLowerCase();
      const rawFeed = String(d.feed || "").toLowerCase();
      const choice = normalizeMarketChoice(rawProvider, rawFeed);
      const provider = choice.providerRecognised ? choice.provider : null;
      const feed = choice.feedRecognised ? choice.feed : null;
      /* Credentials are taken as a PAIR or not at all. A document carrying
         only a key id must not silently pair it with an environment secret. */
      const bothPresent = !!docPair;
      const keyId = bothPresent ? docPair.keyId : "";
      const secretKey = bothPresent ? docPair.secretKey : "";
      const anyPair = bothPresent || !!(fallback.alpacaKeyId && fallback.alpacaSecretKey);
      /* A stored provider of "manual" beside a complete Alpaca pair is almost
         always a leftover from before the pair existed, not a choice: the
         manual provider cannot rank or fill at all. Honour it, but say so. */
      const manualWithKeys = provider === "manual" && anyPair;
      _marketSettings = {
        provider: provider || fallback.provider,
        feed: feed || fallback.feed,
        alpacaSipRealtime: typeof d.alpacaSipRealtime === "boolean"
          ? d.alpacaSipRealtime : fallback.alpacaSipRealtime,
        alpacaKeyId: bothPresent ? keyId : fallback.alpacaKeyId,
        alpacaSecretKey: bothPresent ? secretKey : fallback.alpacaSecretKey,
        credentialSource: bothPresent ? "firestore" : fallback.credentialSource,
        credentialNames: bothPresent ? docPair.names : fallback.credentialNames,
        credentialLookup: lookup,
        ...(manualWithKeys ? { providerNote: "Alpaca credentials are present but the stored provider is \"manual\" — select alpaca / delayed_sip in the Market data form and Save (leave the key fields blank to keep the stored pair)." } : {}),
        source: provider ? "firestore" : fallback.source,
        ...(rawProvider && !provider
          ? { note: `unrecognised provider "${rawProvider.slice(0, 24)}" ignored` } : {}),
        ...(rawFeed && !feed
          ? { feedNote: `unrecognised feed "${rawFeed.slice(0, 24)}" ignored` } : {}),
      };
      if (_marketSettings.provider === "alpaca" && _marketSettings.feed === "sip"
          && _marketSettings.alpacaSipRealtime !== true) {
        _marketSettings.feed = "delayed_sip";
        _marketSettings.feedNote = "Basic-plan SIP selection migrated to explicit delayed_sip";
      }
      /* A stored Alpaca feed of "iex" is the single-venue real-time lane: it
         is non-consolidated, so the execution-source gate refuses every paper
         fill from it and the desk ranks nothing tradable. It was the old
         form's default, not a choice anyone made on purpose. delayed_sip is
         consolidated and included in every Alpaca plan, so resolve to it —
         an operator who really wants IEX sets allowIex: true on the document. */
      if (_marketSettings.provider === "alpaca" && _marketSettings.feed === "iex"
          && d.allowIex !== true) {
        _marketSettings.feed = "delayed_sip";
        _marketSettings.feedNote = "stored feed \"iex\" cannot pass the execution-source gate (single venue, non-consolidated); resolved to delayed_sip — set allowIex: true on marketConfig to keep IEX";
      }
    }
  } catch (e) {
    /* Fail to the environment, then to manual. A market-config read failure
       must never open a lane the operator did not choose. */
    _marketSettings = { ...fallback, note: `marketConfig read failed: ${String(e.message).slice(0, 90)}` };
  }
  _marketSettingsAtMs = Date.now();
  return _marketSettings;
}

function providerConfig(provider, feed = null, opts = {}) {
  const id = String(provider || "manual").toLowerCase();
  const base = PROVIDERS[id] || PROVIDERS.manual;
  if (id !== "alpaca") return { ...base };
  const f = String(feed || marketSettings().feed || "iex").toLowerCase();
  const sipRealtime = Object.prototype.hasOwnProperty.call(opts, "sipRealtime")
    ? opts.sipRealtime === true : marketSettings().alpacaSipRealtime === true;
  /* Alpaca exposes delayed_sip as its explicit 15-minute-delayed,
     consolidated tape. Treating Basic-plan SIP as "sip with an end-time
     haircut" was brittle: entitlement errors degraded the entire scan to
     manual data, so hundreds of companies disappeared before ranking. */
  if (f === "delayed_sip") return { ...base, feed: f, delayMinutes: 15, maxGrade: "B",
    sipRealtime: false, consolidated: true, liquidityEligible: true };
  if (f === "sip") return { ...base, feed: f, delayMinutes: sipRealtime ? 0 : 15, maxGrade: "B",
    sipRealtime,
    consolidated: true, liquidityEligible: true };
  return { ...base, feed: f, delayMinutes: 0, maxGrade: "C",
    consolidated: false, liquidityEligible: false };
}

/* Explicitly bind volume estimates to the feed that produced them. Alpaca's
   default IEX lane represents only a small fraction of consolidated volume;
   treating it as the whole tape corrupts liquidity and cost estimates. The
   environment override is deliberately narrow so an operator cannot turn a
   single-venue feed into an asserted consolidated one. */
const FEED_VOLUME_SHARE = Object.freeze({ delayed_sip: 1, sip: 1, iex: 0.025, otc: 1, unknown: 1 });
function feedVolumeShare(provider, feed = null) {
  const cfg = providerConfig(provider, feed);
  if (cfg.consolidated) return 1;
  const f = String(feed || cfg.feed || "unknown").toLowerCase();
  if (f === "iex") {
    return Math.max(0.005, Math.min(0.25,
      Number(process.env.INVESTOR_IEX_VOLUME_SHARE) || FEED_VOLUME_SHARE.iex));
  }
  const share = FEED_VOLUME_SHARE[f];
  /* Unknown/custom feeds are left uninflated (share=1). That is a conservative
     liquidity estimate, not an assertion that they are consolidated. */
  return Number.isFinite(share) && share > 0 ? share : FEED_VOLUME_SHARE.unknown;
}

function activeProvider() {
  const want = String(marketSettings().provider || "manual").toLowerCase();
  const p = PROVIDERS[want] || PROVIDERS.manual;
  if (p.keyEnv.length && !providerCredentialed(p.id)) {
    // Configured but not credentialed — degrade loudly rather than fail a cycle.
    return { ...PROVIDERS.manual, degradedFrom: p.id,
      reason: `missing credentials for ${p.id} (InvestorAI_Control/marketConfig or ${p.keyEnv.join("/")})` };
  }
  return p.id === "alpaca" ? providerConfig(p.id) : p;
}

/* ── exchange calendar (versioned; half-days matter for the open rule) ──── */
const CALENDAR_VERSION = "us-equity-rule-v2";
const _calendarCache = new Map();

function isoUtc(d) { return d.toISOString().slice(0, 10); }
function nthWeekday(year, month, weekday, nth) {
  const d = new Date(Date.UTC(year, month, 1));
  d.setUTCDate(1 + ((7 + weekday - d.getUTCDay()) % 7) + (nth - 1) * 7);
  return d;
}
function lastWeekday(year, month, weekday) {
  const d = new Date(Date.UTC(year, month + 1, 0));
  d.setUTCDate(d.getUTCDate() - ((7 + d.getUTCDay() - weekday) % 7));
  return d;
}
function observed(year, month, day) {
  const d = new Date(Date.UTC(year, month, day));
  if (d.getUTCDay() === 6) d.setUTCDate(d.getUTCDate() - 1);
  else if (d.getUTCDay() === 0) d.setUTCDate(d.getUTCDate() + 1);
  return d;
}
function easterSunday(year) {
  const a = year % 19, b = Math.floor(year / 100), c = year % 100;
  const d = Math.floor(b / 4), e = b % 4, f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3), h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4), k = c % 4, l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31) - 1;
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return new Date(Date.UTC(year, month, day));
}
function marketCalendar(year) {
  if (_calendarCache.has(year)) return _calendarCache.get(year);
  const holidays = new Set();
  const add = (d) => holidays.add(isoUtc(d));
  add(observed(year, 0, 1));
  /* Jan 1 of the NEXT year can be observed on Dec 31 of this year. Building
     only holidays whose nominal date belongs to `year` incorrectly opened
     that Friday (for example 2027-12-31 for New Year 2028). */
  const nextNewYearObserved = observed(year + 1, 0, 1);
  if (nextNewYearObserved.getUTCFullYear() === year) add(nextNewYearObserved);
  add(nthWeekday(year, 0, 1, 3));
  add(nthWeekday(year, 1, 1, 3));
  const goodFriday = easterSunday(year); goodFriday.setUTCDate(goodFriday.getUTCDate() - 2); add(goodFriday);
  add(lastWeekday(year, 4, 1));
  if (year >= 2022) add(observed(year, 5, 19));
  add(observed(year, 6, 4));
  add(nthWeekday(year, 8, 1, 1));
  add(nthWeekday(year, 10, 4, 4));
  add(observed(year, 11, 25));

  const halfDays = new Set();
  const thanksgiving = nthWeekday(year, 10, 4, 4);
  const blackFriday = new Date(thanksgiving); blackFriday.setUTCDate(blackFriday.getUTCDate() + 1);
  halfDays.add(isoUtc(blackFriday));
  const julyEarly = observed(year, 6, 4);
  julyEarly.setUTCDate(julyEarly.getUTCDate() - 1);
  while (julyEarly.getUTCDay() === 0 || julyEarly.getUTCDay() === 6) {
    julyEarly.setUTCDate(julyEarly.getUTCDate() - 1);
  }
  halfDays.add(isoUtc(julyEarly));
  const christmasEve = new Date(Date.UTC(year, 11, 24));
  if (christmasEve.getUTCDay() >= 1 && christmasEve.getUTCDay() <= 5
      && !holidays.has(isoUtc(christmasEve))) halfDays.add(isoUtc(christmasEve));
  const out = { holidays, halfDays };
  _calendarCache.set(year, out);
  return out;
}

function nyParts(d) {
  // Intl gives us the wall clock in New York including DST, without a tz lib.
  const f = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false, weekday: "short",
  }).formatToParts(d);
  const g = (t) => (f.find((p) => p.type === t) || {}).value;
  return {
    date: `${g("year")}-${g("month")}-${g("day")}`,
    minutes: Number(g("hour")) * 60 + Number(g("minute")),
    weekday: g("weekday"),
  };
}

const OPEN_MIN = 9 * 60 + 30;      // 09:30 ET
const CLOSE_MIN = 16 * 60;         // 16:00 ET
const HALF_CLOSE_MIN = 13 * 60;    // 13:00 ET

function sessionState(d = new Date()) {
  const { date, minutes, weekday } = nyParts(d);
  const cal = marketCalendar(Number(date.slice(0, 4)));
  const isWeekend = weekday === "Sat" || weekday === "Sun";
  const isHoliday = cal.holidays.has(date);
  const isHalf = cal.halfDays.has(date);
  const close = isHalf ? HALF_CLOSE_MIN : CLOSE_MIN;
  const tradingDay = !isWeekend && !isHoliday;
  let phase = "closed";
  if (tradingDay) {
    if (minutes < OPEN_MIN - 60) phase = "premarket_early";
    else if (minutes < OPEN_MIN) phase = "premarket";
    else if (minutes < OPEN_MIN + 15) phase = "opening_auction_window";
    else if (minutes < close - 15) phase = "regular";
    else if (minutes < close) phase = "closing_auction_window";
    else phase = "postmarket";
  }
  return {
    date, minutesEt: minutes, weekday, tradingDay, isHalfDay: isHalf, isHoliday,
    regularOpenMinutesEt: OPEN_MIN, regularCloseMinutesEt: close,
    phase, open: phase === "regular" || phase === "opening_auction_window" || phase === "closing_auction_window",
    calendarVersion: CALENDAR_VERSION,
    // Spreads run 2-5x wider in the first and last 15 minutes. The research is
    // explicit that this intraday penalty exceeds the mid-vs-large-cap spread
    // difference, so the cost model must know about it.
    wideSpreadWindow: phase === "opening_auction_window" || phase === "closing_auction_window",
  };
}

/** Epoch ms of today's regular close (16:00 ET, 13:00 on half days), for
 *  the instant `d`. Derived from the same wall-clock parts as sessionState,
 *  so it agrees with the phase logic across DST. Null on non-trading days. */
function sessionCloseMs(d = new Date()) {
  const st = sessionState(d);
  if (!st.tradingDay) return null;
  const minutesToClose = st.regularCloseMinutesEt - st.minutesEt;
  return d.getTime() + minutesToClose * 60000 - (d.getTime() % 60000);
}

/**
 * A daily observation is immutable only after the exchange has closed and a
 * small delivery buffer has elapsed. The fixed twenty-minute default covers
 * the Basic SIP embargo plus clock/transport slack while keeping IEX and paid
 * SIP on the same deterministic research boundary.
 */
function dailyFinalizationState(value = new Date(), { bufferMinutes = 20 } = {}) {
  /* Scheduler retries and deterministic fixtures may carry a serialized or
     deliberately partial session. Treat those as incomplete information —
     never try to coerce an arbitrary object into Date, which produces an
     Invalid Date and used to throw inside Intl. */
  let state;
  if (value && typeof value === "object" && !(value instanceof Date) && value.date) {
    state = value;
  } else {
    const candidate = value instanceof Date ? value : new Date(value);
    state = Number.isFinite(candidate.getTime()) ? sessionState(candidate) : {};
  }
  const close = Number(state.regularCloseMinutesEt != null
    ? state.regularCloseMinutesEt : (state.isHalfDay ? HALF_CLOSE_MIN : CLOSE_MIN));
  const buffer = Math.max(15, Math.min(120, Number(bufferMinutes) || 20));
  const eligibleAtMinutesEt = close + buffer;
  const hasMinute = Number.isFinite(Number(state.minutesEt));
  const ready = state.tradingDay === true && state.phase === "postmarket"
    && hasMinute && Number(state.minutesEt) >= eligibleAtMinutesEt;
  return { ready, date: state.date || null, eligibleAtMinutesEt, bufferMinutes: buffer,
    reason: ready ? "post_close_buffer_elapsed"
      : (state.tradingDay
        ? (hasMinute ? "awaiting_post_close_buffer" : "session_time_missing")
        : "not_trading_day") };
}

/* Convert a New York wall-clock minute to UTC without adding a timezone
   dependency. Iterating the Intl-derived offset handles both sides of DST. */
function nyWallClockToUtcMs(date, minutes) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(date || ""))) return null;
  const hh = Math.floor(minutes / 60), mm = minutes % 60;
  const targetNaive = Date.parse(`${date}T${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}:00Z`);
  let guess = targetNaive;
  for (let i = 0; i < 3; i += 1) {
    const p = nyParts(new Date(guess));
    const ph = Math.floor(p.minutes / 60), pm = p.minutes % 60;
    const actualNaive = Date.parse(`${p.date}T${String(ph).padStart(2, "0")}:${String(pm).padStart(2, "0")}:00Z`);
    guess += targetNaive - actualNaive;
  }
  return guess;
}

/**
 * Holding age in exchange sessions. Weekends and holidays contribute zero;
 * a half day contributes one session when held for that half day's full
 * regular interval. This replaces mislabeled wall-clock "days" everywhere a
 * holding horizon is enforced or reported.
 */
function tradingDaysHeld(openedAt, asOf = Date.now(), { maxCalendarDays = 8000 } = {}) {
  const startMs = typeof openedAt === "number" ? openedAt : Date.parse(openedAt);
  const endMs = typeof asOf === "number" ? asOf : Date.parse(asOf);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return null;
  if (endMs <= startMs) return 0;
  const startDate = nyParts(new Date(startMs)).date;
  const endDate = nyParts(new Date(endMs)).date;
  const cursor = new Date(`${startDate}T12:00:00Z`);
  let sessions = 0, reachedEnd = false;
  for (let step = 0; step <= maxCalendarDays; step += 1) {
    const iso = cursor.toISOString().slice(0, 10);
    if (iso > endDate) { reachedEnd = true; break; }
    const state = sessionState(cursor);
    if (state.tradingDay) {
      const openMs = nyWallClockToUtcMs(iso, OPEN_MIN);
      const closeMin = state.isHalfDay ? HALF_CLOSE_MIN : CLOSE_MIN;
      const closeMs = nyWallClockToUtcMs(iso, closeMin);
      const overlap = Math.max(0, Math.min(endMs, closeMs) - Math.max(startMs, openMs));
      if (overlap > 0 && closeMs > openMs) sessions += overlap / (closeMs - openMs);
    }
    if (iso === endDate) { reachedEnd = true; break; }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  if (!reachedEnd) return null;
  return Number(sessions.toFixed(8));
}

/**
 * Count trading sessions in the half-open interval (fromDate, toDate].
 *
 * Consecutive rows in a daily series are only "consecutive sessions" if this
 * returns 1. A portfolio-day whose return compounds across a skipped session
 * is not a daily observation and must not be annualised as one.
 * Returns null when either date is unusable, and caps the walk so a stale or
 * corrupt anchor cannot spin.
 */
function tradingSessionsBetween(fromDate, toDate, { maxSessions = 400 } = {}) {
  const iso = /^\d{4}-\d{2}-\d{2}$/;
  if (!iso.test(String(fromDate || "")) || !iso.test(String(toDate || ""))) return null;
  if (String(toDate) <= String(fromDate)) return 0;
  let count = 0;
  const cursor = new Date(`${fromDate}T12:00:00Z`);
  const end = `${toDate}T12:00:00Z`;
  for (let step = 0; step < maxSessions * 3; step += 1) {
    cursor.setUTCDate(cursor.getUTCDate() + 1);
    const state = sessionState(cursor);
    if (state.tradingDay) count += 1;
    if (state.date >= toDate) return count;
    if (cursor.getTime() > Date.parse(end) + maxSessions * 3 * 864e5) break;
    if (count > maxSessions) return count;
  }
  return count;
}

/**
 * The last instant the regular session was open at or before `d`.
 *
 * Bars must be aged in MARKET time, not wall time. At 20:00 ET the 15:59 bar
 * is not stale data — it is the only price that exists, and grading it "F"
 * was what silenced the held-position guard for every hour the exchange was
 * closed. Coarse hourly walk back, then a one-minute refinement forward.
 */
function lastRegularOpenMs(d = new Date()) {
  const start = d instanceof Date ? d.getTime() : Number(d);
  if (!Number.isFinite(start)) return null;
  if (sessionState(new Date(start)).open) return start;
  let t = start;
  for (let h = 0; h < 24 * 10; h += 1) {
    t -= 3600e3;
    if (!sessionState(new Date(t)).open) continue;
    let last = t;
    for (let m = 0; m < 120; m += 1) {
      const next = last + 60000;
      if (next > start || !sessionState(new Date(next)).open) break;
      last = next;
    }
    return last;
  }
  return null;
}

/* ── slippage model ────────────────────────────────────────────────────── */
/* Anchored on de Groot/Huij/Zhou's 7.4bp round trip for the 100 largest and
   Frazzini/Israel/Moskowitz's ~11-12bp institutional large-cap impact. These
   are HALF-TRIP basis points; the ledger applies them on entry and on exit. */
function slippageBps({ advUsd, grade, wideSpreadWindow, vixNorm = 1 }) {
  let bps;
  if (advUsd >= 2e9) bps = 1.0;
  else if (advUsd >= 1e9) bps = 1.5;
  else if (advUsd >= 5e8) bps = 2.5;
  else if (advUsd >= 3e8) bps = 4.0;
  else if (advUsd >= 1e8) bps = 6.0;
  else bps = 12.0;
  if (grade === "B") bps *= 1.4;
  if (grade === "C") bps *= 2.0;
  if (wideSpreadWindow) bps *= 2.0;           // confirmed 2-5x; we use the low end
  if (vixNorm > 1) bps *= Math.min(2.5, vixNorm);  // impact rises with volatility
  return Math.round(bps * 100) / 100;
}

function executionCostContext({ advUsd, grade, wideSpreadWindow, vixNorm, measuredAtMs = Date.now() } = {}) {
  const context = {
    advUsd: Number.isFinite(Number(advUsd)) && Number(advUsd) > 0 ? Number(advUsd) : 0,
    grade: ["A", "B", "C"].includes(String(grade || "").toUpperCase())
      ? String(grade).toUpperCase() : "F",
    wideSpreadWindow: wideSpreadWindow === true,
    vixNorm: Number.isFinite(Number(vixNorm)) && Number(vixNorm) > 0 ? Number(vixNorm) : 1,
    measuredAtMs: Number.isFinite(Number(measuredAtMs)) ? Number(measuredAtMs) : Date.now(),
  };
  return { ...context, slippageBps: slippageBps(context), modelVersion: "public-adv-half-trip-v2" };
}

/* ── adapters ──────────────────────────────────────────────────────────── */
/**
 * Alpaca multi-symbol bars.
 *
 * TWO CORRECTIONS THAT MATTER, both found by audit:
 *
 * 1. `limit` on this endpoint is the TOTAL bar count across every symbol in the
 *    request, not per symbol. Asking for 342 symbols with limit=120 returned
 *    ~120 bars for the whole roster — a handful of names got data and the rest
 *    got nothing, silently. The limit is therefore scaled by the symbol count.
 *
 * 2. The response is paginated by `next_page_token`, which was never read. Any
 *    request larger than one page was silently truncated. We now follow pages
 *    until the token is exhausted or a safety bound is hit.
 */
/* The bars endpoint has NO implicit lookback. Omitting `start` returns the
   current day only, which is why a 1300-day backfill silently produced one bar
   per symbol and every name failed the history gate. `limit` alone cannot ask
   for history — it only caps the page. The window must be stated.

   `end` matters just as much on the Basic plan: it does not serve the most
   recent 15 minutes, so an unbounded request walks into a subscription error
   at the one edge of the series the cycle cares about. Asking for the window
   we are actually entitled to is not a workaround; it is the correct request. */
const ALPACA_BASIC_EMBARGO_MS = 16 * 60000;   // 15m restriction + a minute of slack
function alpacaWindow(timeframe, limit, feed, opts = {}) {
  const nowMs = Number.isFinite(Number(opts.nowMs)) ? Number(opts.nowMs) : Date.now();
  const f = String(feed || "iex").toLowerCase();
  /* Alpaca Basic's IEX lane is real-time. It is the consolidated SIP lane
     that is embargoed unless the account explicitly declares a real-time SIP
     entitlement. Reversing these conditions made the real-time feed stale
     and sent Basic SIP requests into data the account could not retrieve. */
  const sipRealtime = opts.sipRealtime === true;
  /* delayed_sip is a distinct server-side feed: requesting it at "now" still
     returns only data whose 15-minute embargo has elapsed. The legacy sip lane
     keeps the explicit end haircut for Basic accounts. */
  const endMs = f === "sip" && !sipRealtime ? nowMs - ALPACA_BASIC_EMBARGO_MS : nowMs;
  let spanMs;
  if (/day/i.test(timeframe)) {
    /* `limit` is in TRADING days; a calendar window has to cover weekends and
       holidays or the tail of the request is short. 252 sessions ≈ 365 days. */
    spanMs = Math.ceil(limit * (365 / 252)) * 86400000;
  } else {
    const m = /^(\d+)\s*Min/i.exec(timeframe);
    const barMs = m ? Number(m[1]) * 60000 : 5 * 60000;
    /* ~78 five-minute bars per session; widen so a long weekend still fills. */
    const sessionsNeeded = Math.ceil(limit / Math.max(1, Math.floor(23400000 / barMs)));
    spanMs = Math.max(3, Math.ceil(sessionsNeeded * (365 / 252)) + 4) * 86400000;
  }
  return { start: new Date(endMs - spanMs).toISOString(), end: new Date(endMs).toISOString() };
}

/* Pages are 10k bars. Truncation here is silent — the tail of the request is
   simply absent — so the ceiling has to be derived from the size of the ask,
   never left as a constant a larger request quietly outgrows. */
function alpacaPageBudget(limit, symbolCount) {
  return Math.min(64, Math.max(12, Math.ceil((limit * symbolCount) / 10000) + 4));
}

async function fetchBarsAlpaca(symbols, { timeframe = "5Min", limit = 120, feed } = {}) {
  const out = {}, pageHashes = [];
  let pageToken = null, pages = 0, lastAt = null;
  const settings = marketSettings();
  const useFeed = feed || settings.feed || "iex";
  // per-symbol limit -> total, capped at the endpoint maximum
  const total = Math.min(10000, Math.max(limit, limit * symbols.length));
  const win = alpacaWindow(timeframe, limit, useFeed,
    { sipRealtime: settings.alpacaSipRealtime === true });
  /* Pages are 10k bars. A 90-symbol x 1300-day backfill is 117k bars, which
     silently truncated under the old 12-page ceiling. Size the ceiling to the
     request instead of to a constant. */
  const maxPages = alpacaPageBudget(limit, symbols.length);

  do {
    const url = `https://data.alpaca.markets/v2/stocks/bars?symbols=${encodeURIComponent(symbols.join(","))}`
              + `&timeframe=${timeframe}&limit=${total}&adjustment=all&feed=${encodeURIComponent(useFeed)}&sort=asc`
              + `&start=${encodeURIComponent(win.start)}&end=${encodeURIComponent(win.end)}`
              + (pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : "");
    const r = await fetchPublic(url, {
      sourceId: "alpaca.bars", accept: ["json"], timeoutMs: 20000,
      headers: {
        "APCA-API-KEY-ID": providerCredentials("alpaca").keyId,
        "APCA-API-SECRET-KEY": providerCredentials("alpaca").secretKey,
      },
    });
    const bars = (r.json && r.json.bars) || {};
    for (const [sym, arr] of Object.entries(bars)) {
      const mapped = (arr || []).map((b) => ({
        t: b.t, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v, n: b.n || null, vw: b.vw || null,
      }));
      out[sym] = out[sym] ? out[sym].concat(mapped) : mapped;
    }
    pageToken = (r.json && r.json.next_page_token) || null;
    if (r.sha256) pageHashes.push(r.sha256);
    lastAt = r.fetchedAt;
    pages += 1;
  } while (pageToken && pages < maxPages);

  const manifestSha256 = crypto.createHash("sha256").update(pageHashes.join("|")).digest("hex");
  return { bars: out, provider: "alpaca", sha256: manifestSha256, manifestSha256,
           symbolSha256: Object.fromEntries(Object.keys(out).map((s) => [s, manifestSha256])),
           fetchedAt: lastAt, pages, truncated: !!pageToken, feed: useFeed,
           window: win };
}

async function fetchBarsMassive(symbols, { timeframe = "5Min", limit = 120 }) {
  // Massive uses one ticker per aggregates call; unlimited calls make that fine.
  /* "1Day" used to fall through this ternary to mult=1/span="minute", so the
     13-month daily backfill silently requested 1-MINUTE bars over a hardcoded
     5-day window. Every "daily" bar was one arbitrary minute of trading, the
     series never reached the 40-day minimum, and the downtrend gate abstained
     for every name on this provider while appearing to work. */
  const daily = /day/i.test(timeframe);
  const mult = daily ? 1 : (timeframe === "1Min" ? 1 : 5);
  const span = daily ? "day" : "minute";
  const to = new Date().toISOString().slice(0, 10);
  // enough calendar days to yield `limit` trading days, plus slack for holidays
  const lookbackDays = daily ? Math.ceil(limit * 1.5) + 10 : 5;
  const from = new Date(Date.now() - lookbackDays * 864e5).toISOString().slice(0, 10);
  const out = {}, symbolSha256 = {}, errors = {}, fetchedTimes = [];
  const requested = [...new Set(symbols.map(String))].sort();
  let cursor = 0;
  const concurrency = Math.max(1, Math.min(12, Number(process.env.INVESTOR_MARKET_CONCURRENCY) || 8));
  const fetchOne = async (sym) => {
    const url = `https://api.massive.com/v2/aggs/ticker/${encodeURIComponent(sym)}`
              + `/range/${mult}/${span}/${from}/${to}?adjusted=true&sort=desc&limit=${limit}`
              + `&apiKey=${encodeURIComponent(process.env.MASSIVE_API_KEY || "")}`;
    try {
      const r = await fetchPublic(url, { sourceId: "massive.aggs", accept: ["json"], timeoutMs: 15000 });
      /* NOTE ON ADJUSTMENT: Polygon-shaped `adjusted=true` is SPLIT-adjusted
         but not dividend-adjusted, whereas Alpaca's `adjustment=all` is both.
         The two providers therefore return different series for the same name.
         The convention is recorded on every stored bar so a series built from
         one is never silently compared against the other. */
      /* Massive applies `limit` before returning. Ascending order therefore
         returned the oldest bars in the requested range. Request newest-first,
         then normalize back to chronological order for every downstream use. */
      out[sym] = mapMassiveResults((r.json && r.json.results) || [], limit);
      if (r.sha256) symbolSha256[sym] = r.sha256;
      if (r.fetchedAt) fetchedTimes.push(r.fetchedAt);
    } catch (e) {
      out[sym] = []; // one symbol failing must not fail the cycle
      errors[sym] = String(e.code || e.message).slice(0, 100);
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, requested.length) }, async () => {
    while (true) {
      const i = cursor++;
      if (i >= requested.length) return;
      await fetchOne(requested[i]);
    }
  }));
  const manifestSha256 = crypto.createHash("sha256").update(JSON.stringify(
    requested.map((symbol) => [symbol, symbolSha256[symbol] || null, errors[symbol] || null])
  )).digest("hex");
  return { bars: out, provider: "massive", sha256: manifestSha256, manifestSha256,
           symbolSha256, fetchedAt: fetchedTimes.sort().at(-1) || new Date().toISOString(),
           failedSymbols: Object.keys(errors), failureCount: Object.keys(errors).length,
           adjustment: "split_only" };
}

function mapMassiveResults(results, limit) {
  const mapped = (results || []).map((b) => ({
    t: new Date(b.t).toISOString(), o: b.o, h: b.h, l: b.l, c: b.c, v: b.v,
    n: b.n || null, vw: b.vw || null,
  }));
  return normalizeBars(mapped).slice(-Math.max(1, Number(limit) || mapped.length));
}

async function fetchBars(symbols, opts = {}) {
  const p = activeProvider();
  if (p.id === "alpaca") return { ...(await fetchBarsAlpaca(symbols, opts)), adjustment: "split_and_dividend" };
  if (p.id === "massive") return fetchBarsMassive(symbols, opts);
  return { bars: {}, provider: "manual", sha256: null, fetchedAt: new Date().toISOString(),
           note: p.degradedFrom ? `degraded from ${p.degradedFrom}: ${p.reason}` : "manual import only" };
}

/**
 * Chunked, retrying bar fetch for the whole roster.
 *
 * One request for 300 symbols is one point of failure: a transport error, a
 * page budget or an entitlement hiccup lost every name in the cycle, and the
 * cycle then fell back to yesterday's stored bars for all of them. Chunks
 * fail independently, a failed chunk is retried once, and every symbol's
 * provenance hash is the hash of the response that actually contained it.
 * The combined manifest is the hash of the chunk manifests, so the cycle-level
 * identity is still one immutable value. Symbols the provider omitted are
 * listed so a delisted or renamed ticker is visible instead of silently
 * "lacking bars".
 */
/** PURE. Merge per-chunk results into one roster response. `results[i]` is
 *  the successful response for `chunks[i]`, or `{ error }` when the chunk
 *  failed after retries. Exported so a runtime fixture can attest it. */
function mergeChunkResults(list, chunks, results, providerId) {
  const out = { bars: {}, symbolSha256: {}, provider: null, feed: null, adjustment: null,
    fetchedAt: null, pages: 0, truncated: false,
    chunks: { total: chunks.length, failed: [], retried: 0 }, missingSymbols: [], note: null };
  const chunkHashes = [];
  results.forEach((got, ci) => {
    if (!got || got.error) {
      out.chunks.failed.push({ index: ci, symbols: chunks[ci].length,
        error: String(got && got.error || "unknown").slice(0, 120) });
      return;
    }
    out.chunks.retried += Number(got.retried) || 0;
    if (got.provider === "manual" && !Object.keys(got.bars || {}).length) {
      out.provider = out.provider || got.provider; out.note = got.note || out.note;
      return;
    }
    const chunkHash = got.manifestSha256 || got.sha256 || null;
    if (chunkHash) chunkHashes.push(chunkHash);
    for (const [sym, arr] of Object.entries(got.bars || {})) {
      out.bars[sym] = arr;
      out.symbolSha256[sym] = (got.symbolSha256 && got.symbolSha256[sym]) || chunkHash;
    }
    out.provider = out.provider || got.provider;
    out.feed = out.feed || got.feed || null;
    out.adjustment = out.adjustment || got.adjustment || null;
    out.fetchedAt = got.fetchedAt || out.fetchedAt;
    out.pages += Number(got.pages) || 0;
    out.truncated = out.truncated || got.truncated === true;
    if (got.note && !out.note) out.note = got.note;
  });
  out.provider = out.provider || providerId || "manual";
  out.manifestSha256 = chunkHashes.length
    ? crypto.createHash("sha256").update(chunkHashes.join("|")).digest("hex") : null;
  out.sha256 = out.manifestSha256;
  const failedSymbols = new Set(out.chunks.failed.flatMap((f) => chunks[f.index]));
  out.missingSymbols = list.filter((sym) => !out.bars[sym] && !failedSymbols.has(sym));
  out.failedSymbols = [...failedSymbols];
  out.failureCount = out.failedSymbols.length;
  return out;
}

async function fetchBarsChunked(symbols, opts = {}, { chunkSize = 60, retries = 1 } = {}) {
  const list = [...new Set((symbols || []).filter(Boolean))];
  const size = Math.max(5, Math.min(200, Number(chunkSize) || 60));
  const chunks = [];
  for (let i = 0; i < list.length; i += size) chunks.push(list.slice(i, i + size));
  const results = [];
  for (const chunk of chunks) {
    let got = null, lastError = null, retried = 0;
    for (let attempt = 0; attempt <= retries; attempt += 1) {
      try { got = await fetchBars(chunk, opts); lastError = null; break; }
      catch (e) { lastError = e; if (attempt < retries) retried += 1; }
    }
    results.push(got ? { ...got, retried }
      : { error: String(lastError && (lastError.code || lastError.message) || "unknown") });
  }
  return mergeChunkResults(list, chunks, results, activeProvider().id);
}

/* ── validation + grading ──────────────────────────────────────────────── */
function validateBar(b) {
  const errs = [];
  for (const k of ["o", "h", "l", "c"]) {
    if (typeof b[k] !== "number" || !isFinite(b[k]) || b[k] <= 0) errs.push(`${k}_invalid`);
  }
  if (!errs.length) {
    if (b.h < b.l) errs.push("high_below_low");
    if (b.o > b.h || b.o < b.l) errs.push("open_outside_range");
    if (b.c > b.h || b.c < b.l) errs.push("close_outside_range");
  }
  if (typeof b.v !== "number" || b.v < 0) errs.push("volume_invalid");
  if (!b.t || isNaN(Date.parse(b.t))) errs.push("timestamp_invalid");
  return errs;
}

function normalizeBars(bars) {
  const byTime = new Map();
  for (const b of Array.isArray(bars) ? bars : []) {
    if (b && b.t) byTime.set(String(b.t), { ...b, t: new Date(b.t).toISOString() });
  }
  return [...byTime.values()].sort((a, b) => Date.parse(a.t) - Date.parse(b.t));
}

function partitionBarsBySession(bars) {
  const out = {};
  for (const b of normalizeBars(bars)) {
    if (validateBar(b).length) continue;
    const date = nyParts(new Date(b.t)).date;
    (out[date] ||= []).push(b);
  }
  return out;
}

function gradeSeries(bars, { provider, feed = null, sourceSha256 = null,
                             nowMs = Date.now(), maxStaleMinutes = 45 }) {
  const series = normalizeBars(bars);
  if (series.length === 0) {
    return { grade: "F", reasons: ["no_bars"], tradable: false, researchEligible: false };
  }
  const reasons = [];
  let bad = 0;
  for (const b of series) { if (validateBar(b).length) bad += 1; }
  if (bad > 0) reasons.push(`invalid_bars:${bad}`);

  const last = series[series.length - 1];
  const ageMin = (nowMs - Date.parse(last.t)) / 60000;
  const cfg = providerConfig(provider, feed);
  if (provider !== "manual" && !/^[a-f0-9]{64}$/.test(String(sourceSha256 || ""))) {
    reasons.push("missing_source_hash");
  }
  // A delayed feed is expected to be behind by its delay; only count the excess.
  const excessStale = ageMin - cfg.delayMinutes;
  if (excessStale > maxStaleMinutes) reasons.push(`stale:${Math.round(excessStale)}m`);
  if (ageMin < -2) reasons.push(`future_timestamp:${Math.round(-ageMin)}m`);

  let grade;
  if (bad > 0 || reasons.some((r) => r.startsWith("stale") || r.startsWith("future_timestamp")
      || r === "missing_source_hash")) grade = "F";
  else if (provider === "manual") grade = "C";
  else if (provider === "alpaca") grade = cfg.consolidated ? "B" : "C";
  else grade = "B";                                    // one consolidated source = B; A needs two
  // maxGrade ceiling from provider config
  const order = { A: 3, B: 2, C: 1, F: 0 };
  if (order[grade] > order[cfg.maxGrade]) grade = cfg.maxGrade;

  return {
    grade, reasons,
    tradable: (grade === "A" || grade === "B") && cfg.liquidityEligible !== false,
    /* `tradable` answers "may an order be priced against this?" and a
       single-venue feed must always answer no. It is a different question from
       "is this series good enough to MEASURE with", and conflating the two is
       what left a paper-only research desk with nothing to observe.

       researchEligible is the measurement answer: the bars are internally
       valid, carry a source hash, are not stale and are not from the future.
       It never authorises an order — every execution path keeps testing
       `tradable`. What it authorises is ranking and recording, and any record
       made under it is stamped with this grade and, because the market
       identity is part of the shadow experiment hash, accumulates in its own
       experiment rather than being blended into consolidated-feed history. */
    researchEligible: grade !== "F",
    consolidated: !!cfg.consolidated,
    lastBarAt: last.t, ageMinutes: Math.round(ageMin), barCount: series.length,
    provider, feed, feedDelayMinutes: cfg.delayMinutes,
  };
}

/* ── THE EXECUTION CLOCK ───────────────────────────────────────────────── */
/**
 * Given a decision instant, return the first bar the system could actually
 * have traded. Never returns a bar that opened before the decision was made,
 * and never a bar the feed had not yet published.
 */
function firstEligibleBar(bars, { decisionAtMs, provider, feed = null, executionLatencyMs = 60000 }) {
  const cfg = providerConfig(provider, feed);
  const availableFrom = decisionAtMs + cfg.delayMinutes * 60000 + executionLatencyMs;
  for (const b of normalizeBars(bars)) {
    const openMs = Date.parse(b.t);
    // With delayed data, a timestamp after the decision but before the
    // feed-delay horizon was still unobservable and is never fill-eligible.
    if (openMs > decisionAtMs && openMs >= availableFrom) {
      return { bar: b, barOpenAt: b.t, availableFromMs: availableFrom, reason: "ok" };
    }
  }
  return { bar: null, barOpenAt: null, availableFromMs: availableFrom, reason: "no_eligible_bar" };
}

/* ── storage: ONE DOC PER SYMBOL PER DAY holding the bar array ──────────── */
/* Confirmed cost analysis: one doc per bar means a 100-symbol chart load is
   7,800 reads; one doc per symbol-day makes it 100. 288 bars of OHLCV is far
   under the 1MiB document ceiling. Timestamp fields are NOT indexed — an
   indexed sequential field caps a collection at 500 writes/sec. */
function barDocId(symbol, dateStr) { return `${symbol}_${dateStr}`; }

async function writeBars(symbol, dateStr, bars, meta) {
  const ids = [];
  for (const [sessionDate, sessionBars] of Object.entries(partitionBarsBySession(bars))) {
    const ref = A.col(A.COL.marketLatest).doc(barDocId(symbol, sessionDate));
    const prior = await ref.get();
    let existing = prior.exists ? prior.data() : {};
    let superseded = null;
    if (existing.provider && meta && (existing.provider !== meta.provider
        || existing.feed !== (meta.feed || null)
        || existing.adjustment !== (meta.adjustment || null))) {
      /* FEED MIGRATION. A session document written under a previous market
         identity (e.g. alpaca/iex before the switch to delayed_sip) must not
         be merged with bars from the new one — but refusing the write left
         every stored session on the old identity for good: the morning
         fallback then found no same-identity bars, names lacked their 24
         usable bars until ~two hours into the session, and the nightly
         archive could never repair the day. When the incoming bars carry the
         ACTIVE identity, they replace the document; the old identity is
         recorded, not blended. Any other mismatch is still refused. */
      const active = activeProvider();
      const incomingIsActive = meta.provider === active.id
        && (meta.feed || null) === (active.feed || null);
      if (!incomingIsActive) {
        throw new Error(`writeBars ${symbol}/${sessionDate}: provider/feed/adjustment mismatch`);
      }
      superseded = { provider: existing.provider, feed: existing.feed || null,
        adjustment: existing.adjustment || null, barCount: (existing.bars || []).length,
        replacedAtMs: Date.now() };
      existing = {};
    }
    const merged = normalizeBars([...(existing.bars || []), ...sessionBars]).slice(-400);
    const sourceHashes = [...new Set([...(existing.sourceHashes || []), meta && meta.sourceSha256]
      .filter((h) => /^[a-f0-9]{64}$/.test(String(h))))].sort();
    const sourceSha256 = sourceHashes.length ? crypto.createHash("sha256")
      .update(sourceHashes.join("|")).digest("hex") : null;
    await ref.set({
      symbol, date: sessionDate, bars: merged, barCount: merged.length,
      ...meta,
      ...(sourceSha256 ? { sourceHashes, sourceSha256 } : {}),
      ...(superseded ? { supersededIdentity: superseded } : {}),
      updated_at: A.FV.serverTimestamp(),
    }, { merge: !superseded });
    ids.push(ref.id);
  }
  return ids;
}

async function readBars(symbol, dateStr) {
  const s = await A.col(A.COL.marketLatest).doc(barDocId(symbol, dateStr)).get();
  return s.exists ? (s.data().bars || []) : [];
}

/** Read the trailing N sessions of bars for one symbol, oldest first. */
async function readRecentBars(symbol, sessions = 3) {
  return (await readRecentBarsWithMeta(symbol, sessions)).bars;
}

async function readRecentBarsWithMeta(symbol, sessions = 3) {
  const dates = [];
  const d = new Date();
  let guard = 0;
  while (dates.length < sessions && guard < 20) {
    const st = sessionState(d);
    if (st.tradingDay) dates.push(st.date);
    d.setUTCDate(d.getUTCDate() - 1);
    guard += 1;
  }
  dates.reverse();
  const snaps = await Promise.all(dates.map((dt) =>
    A.col(A.COL.marketLatest).doc(barDocId(symbol, dt)).get()));
  let docs = [];
  for (const s of snaps) if (s.exists) docs.push(s.data());
  const identityOf = (d) => JSON.stringify([d.provider || "manual", d.feed || null, d.adjustment || null]);
  const identities = [...new Set(docs.map(identityOf))];
  let mixedNote = null;
  if (identities.length > 1) {
    /* Sessions written under different market identities are never blended.
       Keep the LATEST session's identity and drop the rest (a feed migration
       leaves older sessions on the old identity); the caller sees fewer bars,
       not a silent mixture, and the note says why. */
    const keep = identityOf(docs[docs.length - 1]);
    const dropped = docs.filter((d) => identityOf(d) !== keep).length;
    docs = docs.filter((d) => identityOf(d) === keep);
    mixedNote = `dropped ${dropped} session(s) on a previous market identity`;
  }
  const out = [];
  for (const d of docs) out.push(...(d.bars || []));
  const latest = docs.at(-1) || {};
  return { bars: normalizeBars(out), provenance: docs.length ? {
    provider: latest.provider || "manual", feed: latest.feed || null,
    adjustment: latest.adjustment || null, sourceSha256: latest.sourceSha256 || null,
  } : null, reason: docs.length ? "ok" : "missing", ...(mixedNote ? { note: mixedNote } : {}) };
}

module.exports = {
  PROVIDERS, providerConfig, activeProvider, FEED_VOLUME_SHARE, feedVolumeShare,
  sessionState, marketCalendar, nyParts, CALENDAR_VERSION, lastRegularOpenMs,
  dailyFinalizationState, tradingDaysHeld,
  marketSettings, loadMarketSettings, MARKET_SETTINGS_DOC,
  providerCredentials, providerCredentialed,
  tradingSessionsBetween,
  slippageBps, executionCostContext,
  sessionCloseMs, fetchBars, fetchBarsChunked, mergeChunkResults, mapMassiveResults, validateBar, normalizeBars, partitionBarsBySession, gradeSeries, firstEligibleBar,
  alpacaWindow, alpacaPageBudget, normalizeMarketChoice,
  writeBars, readBars, readRecentBars, readRecentBarsWithMeta, barDocId,
};
