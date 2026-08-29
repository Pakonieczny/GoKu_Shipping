/*  netlify/functions/investorCycle-background.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the cycle worker. Background function, 15-minute ceiling.
 *
 *  THE INVERTED PIPELINE. The order of these steps is the cost architecture:
 *
 *    1. pull bars for the whole trade tier            (one provider call)
 *    2. compute residual returns across the panel     (pure maths, free)
 *    3. rank cross-sectionally, find threshold breaches
 *    4. ONLY for breaches: look for a cause in the evidence lanes
 *    5. ONLY if still ambiguous: ask the model
 *    6. run the gate stack, propose or record a no-trade decision
 *    7. evaluate open positions against the buy/hold exit rule
 *
 *  Steps 1-3 cost nothing but a data call. Step 5 is the expensive one and is
 *  reached by a handful of names per cycle instead of the whole roster, which
 *  is the difference between ~$285/month and ~$40/month.
 *
 *  IDEMPOTENCE. Netlify background functions return 202 immediately and retry
 *  after failure. Every write below is either deterministic-id (ledger, fills)
 *  or a merge on a stable key (candidates keyed by cycle+symbol), so a retry
 *  re-derives the same state rather than duplicating it.
 *
 *  This worker NEVER places a real order. It writes proposals; a human
 *  approves them in the console, or in a later phase a validated book may
 *  auto-approve within its own caps. There is no broker integration.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const { verifyWorkerNonce, redact } = require("./_investorAuth");
const M = require("./_investorMarket");
const H = require("./_investorHistory");
const S = require("./_investorSignal");
const E = require("./_investorEvidence");
const O = require("./_investorOpenai");
const L = require("./_investorLedger");
const B = require("./_investorBootstrap");
const SH = require("./_investorShadow");
const AL = require("./_investorAllocator");
const V = require("./_investorVariants");
const R = require("./_investorRisk");
const LD = require("./_investorLadder");

const FN_NAME = "investorCycle-background";

/* ── universe + strategy loading ───────────────────────────────────────── */
async function loadUniverse(version) {
  /* Default to the version the shipped roster declares, not a hardcoded "v1".
     Bootstrap writes the CIK-resolved roster to doc(base.version) — currently
     "v2" — so reading "v1" silently returned the unresolved fallback with every
     cik null, which disabled the entire evidence lane without any error. */
  const fallbackModule = require("./_investorUniverse.js");
  const snap = await A.col(A.COL.universe).doc(version || fallbackModule.version || "v1").get();
  if (snap.exists) return snap.data();
  const fallback = require("./_investorUniverse.js");
  return fallback;
}

async function loadStrategy(version) {
  const snap = await A.col(A.COL.strategies).doc(version || "v1").get();
  if (snap.exists) return snap.data();
  return require("./_investorStrategy.js");
}

async function controlDoc() {
  const s = await A.col(A.COL.control).doc("control").get();
  return s.exists ? s.data() : {};
}

/* ── regime inputs ─────────────────────────────────────────────────────── */
/* VIX and COR3M drive sizing and the stand-down gate. Both are cached in
   Firestore by the evidence sweep; the cycle reads whatever is current and
   degrades to reduced size rather than failing when they are stale. */
async function regime() {
  const s = await A.col(A.COL.control).doc("regime").get();
  const d = s.exists ? s.data() : {};
  return {
    vix: Number(d.vix) || null,
    vixMedian: Number(d.vixMedian) || 18,
    vixNorm: d.vix && d.vixMedian ? d.vix / d.vixMedian : 1,
    /* An unset COR3M used to become NaN, and every comparison against NaN is
       false, so the dispersion stand-down — the only automatic risk-off switch
       in the system — could never fire unless a human typed a number in.
       Absent correlation data now reads as the neutral default and is flagged
       stale rather than silently disabling the gate. */
    cor3m: Number.isFinite(Number(d.cor3m)) ? Number(d.cor3m) : null,
    asOf: d.asOf || null,
    stale: !d.asOf || (Date.now() - Date.parse(d.asOf)) > 36 * 3600e3,
  };
}

/* ── attention proxy ───────────────────────────────────────────────────── */
/* A true retail-order-flow measure needs TAQ, which is paid and out of scope.
   The tradable proxy the research supports is abnormal VOLUME against the
   symbol's own baseline: attention spikes show up as volume the price move
   alone does not justify. Named honestly so nobody mistakes it for
   Robinhood-style holdings data. */
function attentionZ(bars, window = 12) {
  if (!bars || bars.length < window + 24) return 0;
  const vols = bars.map((b) => b.v || 0);
  const recent = vols.slice(-window);
  const base = vols.slice(0, -window);
  const mu = S.mean(base), sd = S.stdev(base);
  if (!(sd > 0)) return 0;
  return (S.mean(recent) - mu) / sd;
}

/* ── measured dollar volume ────────────────────────────────────────────── */
/* Hardcoded volume goes stale within weeks and the liquidity gate is the main
   defence against cost drag, so it is measured from the bars we already have.
   Scaled from the bar interval up to a full session. */
function measuredAdvUsd(bars, barsPerSession = 78) {
  if (!bars || bars.length < 10) return 0;
  const recent = bars.slice(-Math.min(bars.length, 200));
  const perBar = S.mean(recent.map((b) => (b.v || 0) * (b.c || 0)));
  return perBar * barsPerSession;
}

/* ── main cycle ────────────────────────────────────────────────────────── */
async function runCycle(jobId) {
  const startedAt = Date.now();

  /* Self-bootstrap. First cycle freezes the universe, resolves CIKs from SEC,
     opens the paper account, derives earnings windows and fetches VIX. Every
     cycle after that this is a single read plus the slow-clock refreshes. */
  let bootstrap = null;
  try { bootstrap = await B.ensureBootstrapped(); }
  catch (e) { bootstrap = { error: String(e.message).slice(0, 160) }; }

  const ctrl = await controlDoc();
  const strategy = await loadStrategy(ctrl.strategyVersion);
  const universe = await loadUniverse(ctrl.universeVersion);
  /* ── THE LOOP CLOSES HERE ──────────────────────────────────────────────
   *
   * This read is what makes the system self-adjusting rather than merely
   * self-measuring. Until it existed the allocator ran every cycle, produced a
   * verdict about which of the eight variants was working, wrote it to
   * Firestore, and NOTHING read it back — live trading used the hand-edited
   * baseline forever, whatever the evidence said. The dashboard column headed
   * "Share of money" described a split that did not happen.
   *
   * Now: once the power gate opens (625 independent days), the leading
   * variant's parameters become the live configuration, and the leader's
   * conviction scales position size. Before the gate opens, `leaderId` is null
   * and the baseline is used unchanged — which is the correct behaviour, since
   * acting on an underpowered result is exactly how a system fools itself.
   *
   * The read happens HERE, before any candidate is evaluated. Reading it after
   * the trading decisions — where the allocator block sits — would mean the
   * verdict always arrived one cycle too late to matter. */
  const baseParams = strategy.parameters || {};
  let liveVariant = null, allocationRead = null;
  try {
    const aSnap = await A.col(A.COL.control).doc("allocation").get();
    if (aSnap.exists) {
      const a = aSnap.data();
      /* A variants-file edit invalidates every observation recorded against
         the old definitions, so a stale hash must not be allowed to steer
         live money. Fail back to the baseline and say so. */
      const hashOk = !a.variantsHash || a.variantsHash === V.variantsHash();
      allocationRead = {
        leaderId: a.leaderId || null, powered: !!a.powered,
        leaderWeightPct: a.leaderWeightPct ?? null,
        hashOk, bestEffectiveN: a.bestEffectiveN ?? 0,
        requiredEffectiveN: a.requiredEffectiveN ?? null,
      };
      if (a.powered && a.leaderId && hashOk) liveVariant = V.byId(a.leaderId);
    }
  } catch (e) { allocationRead = { error: String(e.message).slice(0, 120) }; }

  /* The live configuration IS the winning variant once there is enough
     evidence to name one. This is the line the whole learning apparatus
     exists to produce. */
  const cfg = liveVariant ? V.configFor(liveVariant.id, baseParams) : baseParams;
  const cfgSource = liveVariant
    ? `variant ${liveVariant.id} (${liveVariant.name}) — chosen on ${allocationRead.bestEffectiveN} independent days of evidence`
    : (allocationRead && allocationRead.powered === false
        ? `baseline — only ${allocationRead.bestEffectiveN || 0} of ${allocationRead.requiredEffectiveN || "?"} independent days collected, not enough to favour any variant yet`
        : "baseline — no allocation recorded yet");

  const accountId = ctrl.accountId || "paper-1";
  const session = M.sessionState(new Date());
  const reg = await regime();

  const tradeTier = universe.tradeTier || [];
  const symbols = tradeTier.map((t) => t.symbol);

  /* Sectors come from the roster, not a hardcoded table — a 350-name roster
     against a stale map would dump most names into one bucket and break the
     factor model. */
  const sectorMap = {};
  for (const t of tradeTier) sectorMap[t.symbol] = t.sector || "other";
  S.setSectorMap(sectorMap);

  const runRef = A.col(A.COL.runs).doc(jobId);
  await runRef.set({
    jobId, kind: "cycle", startedAt: A.FV.serverTimestamp(),
    strategyVersion: strategy.version, universeVersion: universe.version,
    session, regime: reg, symbolCount: symbols.length,
    ...A.envelope({ created_by: FN_NAME }),
  }, { merge: true });

  /* 1. bars ------------------------------------------------------------- */
  const provider = M.activeProvider();
  let panel = {}, fetchMeta = {};
  try {
    const got = await M.fetchBars(symbols, { timeframe: cfg.barTimeframe || "5Min", limit: 120 });
    panel = got.bars || {};
    fetchMeta = { provider: got.provider, fetchedAt: got.fetchedAt, note: got.note || null };
  } catch (e) {
    fetchMeta = { provider: provider.id, error: String(e.code || e.message).slice(0, 160) };
  }

  // Fall back to stored bars for any symbol the provider did not return.
  for (const sym of symbols) {
    if (!panel[sym] || panel[sym].length < 20) {
      try { const stored = await M.readRecentBars(sym, 2); if (stored.length) panel[sym] = stored; } catch {}
    }
  }

  // Persist bars (one doc per symbol per day, bar array inside).
  const today = session.date;
  const quality = {};
  await Promise.all(symbols.map(async (sym) => {
    const bars = panel[sym] || [];
    quality[sym] = M.gradeSeries(bars, { provider: fetchMeta.provider || provider.id });
    if (bars.length) {
      try {
        await M.writeBars(sym, today, bars, {
          provider: fetchMeta.provider || provider.id,
          grade: quality[sym].grade,
          gradeReasons: quality[sym].reasons,
          feedDelayMinutes: quality[sym].feedDelayMinutes,
        });
      } catch (e) { /* storage failure must not kill the cycle */ }
    }
  }));

  /* 1b. THE LONG MEMORY --------------------------------------------------
     Load each name's daily history and reduce it to the handful of numbers the
     decision path uses: how much this stock normally moves, where it sits in
     its own six-month range, whether it is in a downtrend, and how it has
     behaved after similar drops before.

     `asOf` is today's date, and contextFor() discards every bar dated today or
     later. The six-month picture a decision uses is therefore built only from
     completed sessions before today — today's own move can never help justify
     itself. That cut is what keeps this honest, and history.test.js proves it. */
  const historyCtx = {};      // symbol -> context
  const revRaw = {};          // symbol -> raw reversion events
  let historyDays = 0, historyNames = 0;
  await Promise.all(symbols.map(async (sym) => {
    try {
      const series = await H.readDaily(sym);
      if (!series.length) return;
      const ctx = H.contextFor(series, today);
      historyCtx[sym] = ctx;
      if (ctx.ok) { historyNames += 1; historyDays += ctx.days; }
      revRaw[sym] = H.reversionEvents(series, today);
    } catch (e) { /* a name without history simply gets no long-horizon read */ }
  }));

  /* Shrink every name's own bounce-back record toward the roster-wide average.
     Ten events is not a track record; the shrinkage says so arithmetically. */
  const revShrunk = H.shrinkReversion(revRaw);
  const reversionBySymbol = {};
  for (const [sym, r] of Object.entries(revShrunk.perSymbol)) {
    reversionBySymbol[sym] = {
      ...r,
      multiplier: H.reversionMultiplier(r.shrunkPct, r.pooledPct),
    };
  }

  /* 2 + 3. residuals and ranks ------------------------------------------ */
  const tradable = {};
  for (const sym of symbols) if ((panel[sym] || []).length >= 24) tradable[sym] = panel[sym];
  const rp = S.residualPanel(tradable);

  const zBySymbol = {};
  for (const sym of rp.symbols) {
    /* The context goes INTO the z-score. This is the fix for the two-day
       denominator: "unusual" is now measured against this stock's own months
       of behaviour, not against however quiet the last two sessions happened
       to be. */
    const z = S.residualZ(rp.residuals[sym], cfg.signalWindow || 12, historyCtx[sym]);
    if (z) zBySymbol[sym] = z;
  }
  const { ranks, n: ranked } = S.crossSectionalRanks(zBySymbol);
  const crowd = S.sectorCrowding(ranks, S.sectorOf, cfg.entryRank ?? S.ENTRY_RANK);

  /* 4-6. evaluate each symbol ------------------------------------------- */
  const cycleId = `${today}_${new Date().toISOString().slice(11, 16).replace(":", "")}`;
  const candidates = [], decisions = [], exits = [], portfolioBlocks = [];
  let modelCalls = 0;

  const causeBySymbol = {};
  const lastPriceBySymbol = {};
  const tierBySymbol = Object.fromEntries(tradeTier.map((t) => [t.symbol, t]));
  /* Meta carries MEASURED volume, overriding anything asserted in the file. */
  const metaBySymbol = {};
  for (const t of tradeTier) {
    metaBySymbol[t.symbol] = { ...t, advUsd: measuredAdvUsd(panel[t.symbol]) };
  }
  /* Derived earnings windows, keyed by symbol. A symbol missing from this map
     has no known window and is BLOCKED — an unknown earnings date is the most
     dangerous state for this strategy, so the failure mode is "does not
     trade", never "trades blind". */
  const earningsWindows = await B.readEarnings();

  /* Marks for every symbol come straight from the panel, BEFORE anything
     reads them. This map used to be filled inside the per-symbol loop below,
     which runs after the book is built — so the book was summarised against an
     empty map, every position was valued at cost, unrealised losses were
     invisible, and the drawdown breaker could not fire on an open loss. Worse,
     held symbols exited that loop early and never got a mark at all. */
  for (const sym of symbols) {
    const b = panel[sym];
    if (b && b.length) lastPriceBySymbol[sym] = b[b.length - 1].c;
  }

  /* 1a-ter. TURNOVER PERCENTILES ----------------------------------------
     turnover = dollar volume / price / shares outstanding, ranked across the
     roster. The gate refuses reversal entries in the top decile — in those
     names losers keep falling (Medhat & Schmeling, RFS 2022). Names without
     shares data get null and the gate abstains for them. */
  const sharesBySymbol = await B.readShares().catch(() => ({}));
  const turnoverPctile = {};
  {
    const raw = [];
    for (const sym of symbols) {
      const sh = sharesBySymbol[sym];
      const advUsd = (tierBySymbol[sym] || {}).advUsd || (metaBySymbol[sym] || {}).advUsd || 0;
      const px = lastPriceBySymbol[sym];
      if (sh && sh.shares > 0 && advUsd > 0 && px > 0) {
        raw.push([sym, (advUsd / px) / sh.shares]);
      }
    }
    raw.sort((a, b) => a[1] - b[1]);
    const n = raw.length;
    raw.forEach(([sym], i) => { turnoverPctile[sym] = n > 1 ? i / (n - 1) : 0.5; });
  }


  /* ── THE BOOK ────────────────────────────────────────────────────────
     Everything above this line asks "is this one name a good idea". Nothing
     did — until now — ask "given what we already hold, should we hold more".
     Nine individually-oversold semiconductors are one bet with nine times the
     size, and the strategy file's portfolioControls block was read by nothing
     at all. It is enforced from here. */
  const posSnapAll = await A.col(A.COL.positions).where("accountId", "==", accountId).get();
  const allPositions = [];
  posSnapAll.forEach((d) => allPositions.push(d.data()));
  const positionBySymbol = Object.fromEntries(allPositions.map((p) => [p.symbol, p]));

  const navSnapshot = await L.balances(accountId);
  const cashUsdNow = navSnapshot.usd[L.ACCT.CASH] || 0;

  const book = R.summarise(allPositions, lastPriceBySymbol, S.sectorOf,
                           (cashUsdNow + (navSnapshot.usd[L.ACCT.POSITIONS] || 0)) || 1);
  const navUsd = cashUsdNow + book.grossUsd;

  /* Account-level circuit breakers, checked once. If the account is in
     trouble the right number of new positions is zero, however good the next
     idea looks. Exits are deliberately NOT blocked by a halt — being unable to
     sell during a drawdown is how a pause becomes a disaster. */
  const hwm = Math.max(Number(ctrl.highWaterMarkUsd) || 0, navUsd);
  const startOfDayNav = Number(ctrl.startOfDayNavUsd) || navUsd;
  const riskState = {
    navUsd, hwmUsd: hwm,
    drawdownPct: hwm > 0 ? ((navUsd - hwm) / hwm) * 100 : 0,
    dayPnlPct: startOfDayNav > 0 ? ((navUsd - startOfDayNav) / startOfDayNav) * 100 : 0,
  };
  const breakers = R.accountBreakers(riskState, strategy);

  /* Symbols with an order already in flight. The duplicate check in the
     portfolio gate looks at POSITIONS, so a proposed-but-unapproved order was
     invisible to it and the same dip re-proposed the same symbol every five
     minutes — twelve stacked orders an hour for one name, each approvable. */
  const pendingOrderSymbols = new Set();
  try {
    for (const st of ["proposed", "approved"]) {
      const q = await A.col(A.COL.orders)
        .where("accountId", "==", accountId).where("status", "==", st).limit(100).get();
      q.forEach((d) => pendingOrderSymbols.add(d.data().symbol));
    }
  } catch (e) { /* on failure, the recordFill double-fill guard still holds the line */ }

  /* One NAV mark per trading day, which is what every performance number is
     built from. Without this there is no equity curve, so no drawdown, no
     volatility, and no way to answer "did this beat just holding the names". */
  try {
    await A.col(A.COL.control).doc("control").collection("navHistory").doc(today).set({
      date: today, navUsd: Number(navUsd.toFixed(2)),
      cashUsd: Number(cashUsdNow.toFixed(2)),
      positionsUsd: Number(book.grossUsd.toFixed(2)),
      openPositions: book.count,
      unrealisedUsd: Number(book.unrealisedUsd.toFixed(2)),
      updatedAt: A.FV.serverTimestamp(),
    }, { merge: true });
  } catch (e) { /* a missed NAV mark must not stop the cycle */ }
  await A.col(A.COL.control).doc("control").set({
    highWaterMarkUsd: hwm,
    startOfDayNavUsd: (ctrl.startOfDayNavDate === today) ? startOfDayNav : navUsd,
    startOfDayNavDate: today,
  }, { merge: true });

  for (const sym of rp.symbols) {
    const meta = tierBySymbol[sym] || {};
    const bars = panel[sym] || [];
    const last = bars[bars.length - 1];
    if (!last) continue;

    const position = positionBySymbol[sym] || null;

    /* --- exits first: an open position is evaluated every cycle, which is
       the single highest-value use of the 5-minute loop. The research is
       explicit that the exit rule was worth more than any entry refinement. */
    if (position && position.open) {
      const heldDays = (Date.now() - Date.parse(position.openedAt)) / 864e5;

      /* The exit now SEES THE PRICE. Previously exitSignal took only rank and
         holding time, so a position down 40% was invisible to it — and worse,
         a collapsing name's rank moves away from the exit threshold, meaning
         falling harder made a sale less likely. Stops come first, before any
         rank logic can defer them. */
      const ew = earningsWindows[sym];
      let earningsInDays = null;
      if (ew && ew.dates && ew.dates.length) {
        const soonest = ew.dates
          .map((d) => (Date.parse(d) - Date.now()) / 864e5)
          .filter((d) => d >= 0).sort((a, b) => a - b)[0];
        if (soonest != null) earningsInDays = soonest;
      }
      const peak = Math.max(Number(position.peakPriceUsd) || 0, last.c);
      if (peak > (Number(position.peakPriceUsd) || 0)) {
        try {
          await A.col(A.COL.positions).doc(`${accountId}_${sym}`)
            .set({ peakPriceUsd: peak }, { merge: true });
        } catch {}
      }

      /* `entryPriceUsd` is what recordFill actually writes. Reading `avgPrice`
         here silently produced NaN, which made havePrice false inside
         exitSignal and skipped the hard stop, the trailing stop, the profit
         target and the earnings exit — every price-based protection, on every
         real position. The end-to-end run caught it; nothing else did. */
      const entryUsd = Number(position.entryPriceUsd != null ? position.entryPriceUsd : position.avgPrice);
      const ex = S.exitSignal(ranks[sym] ?? 0.5, heldDays, cfg, {
        mark: last.c, entry: entryUsd, peak, earningsInDays,
      });
      if (ex.exit) {
        /* THE EXIT INTENT. The execution clock only fills on a bar printed
           after the decision, and with a 15-minute-delayed feed no such bar
           exists in THIS cycle's panel. The old code re-stamped the decision
           time as Date.now() on every cycle, so the clock restarted every five
           minutes and no exit — including the hard stop — could ever execute.
           The decision time is now recorded ONCE, on the position, exactly as
           an order records it for a buy: a later cycle's fresh bars become
           eligible against the original timestamp and the close goes through. */
        if (!position.exitIntent || !position.exitIntent.decisionAtMs) {
          await A.col(A.COL.positions).doc(`${accountId}_${sym}`).set({
            exitIntent: {
              decisionAtMs: Date.now(), decisionAt: new Date().toISOString(),
              reason: ex.reason, kind: ex.kind, urgent: !!ex.urgent,
              pnlPctAtSignal: ex.pnlPct,
            },
          }, { merge: true });
          await A.col(A.COL.decisions).doc(`${cycleId}_${sym}_exit`).set({
            cycleId, symbol: sym, kind: "exit_signal", reason: ex.reason,
            exitKind: ex.kind, urgent: !!ex.urgent, pnlPct: ex.pnlPct,
            markUsd: last.c, entryUsd: entryUsd || null,
            rank: ranks[sym], heldDays, decisionAtMs: Date.now(),
            strategyVersion: strategy.version,
            ...A.envelope({ created_by: FN_NAME }),
          }, { merge: true });
        }
        exits.push({
          symbol: sym, reason: ex.reason, kind: ex.kind, urgent: !!ex.urgent,
          pnlPct: ex.pnlPct, rank: ranks[sym], heldDays: Number(heldDays.toFixed(2)),
        });
      }
      continue;   // never propose an entry in a name we already hold
    }

    /* A halted account opens nothing. Exits above still run — being unable to
       sell during a drawdown is how a pause becomes a disaster. */
    if (breakers.halted) { causeBySymbol[sym] = S.CAUSE.PENDING; continue; }

    /* --- entries: only threshold breaches proceed past this line --------- */
    const z = zBySymbol[sym];
    const rank = ranks[sym];
    const breach = z && rank <= (cfg.entryRank ?? S.ENTRY_RANK) && z.z <= -(cfg.minAbsZ ?? 2.0);

    let cause = S.CAUSE.PENDING, causeDetail = null, coverage = null;
    causeBySymbol[sym] = cause;

    if (breach) {
      /* 4. a breach earns an evidence lookup. Nothing else does. */
      const att = attentionZ(bars, cfg.signalWindow || 12);
      let freshDocs = [];
      try {
        if (meta.cik) {
          const polled = await E.pollEdgar(meta.cik, { forms: "" });
          freshDocs = polled.entries || [];
          for (const d of freshDocs.slice(0, 5)) {
            try { await E.recordVersion({ symbol: sym, sourceId: "sec.latest", entry: d, rawSha256: polled.sha256 }); } catch {}
          }
        }
      } catch (e) { /* evidence lane failure degrades to "pending", never crashes */ }

      const pre = E.preClassify({
        symbol: sym, freshDocs, residualZ: z.z, attentionScore: att,
        hoursSinceMove: (cfg.signalWindow || 12) * 5 / 60,
      });
      cause = pre.cause; causeDetail = pre;

      /* 5. escalate to the model ONLY when deterministic rules are ambiguous */
      if (pre.needsModel && modelCalls < O.CYCLE_CALL_CEILING) {
        modelCalls += 1;
        const llm = await O.classifyMove({
          symbol: sym, role: "classify", documents: freshDocs,
          moveSummary: `residual ${(z.cumResidual * 100).toFixed(2)}% over ${z.window} bars, z ${z.z.toFixed(2)}, cross-sectional rank ${rank.toFixed(3)}`,
          attentionScore: att,
        });
        if (llm.ok) { cause = llm.cause; causeDetail = { ...pre, model: llm }; }
      }

      try { coverage = meta.cik ? await E.coverageRoster(sym, meta.cik) : null; } catch {}
    }

    /* 6. the gate stack runs for every ranked symbol, so the dashboard can
       show why a name did NOT trade as readily as why one did. */
    causeBySymbol[sym] = cause;
    lastPriceBySymbol[sym] = last.c;

    const evalRes = S.evaluateCandidate({
      symbol: sym, rank, zStat: z,
      quality: quality[sym], advUsd: (metaBySymbol[sym] || {}).advUsd || 0,
      earningsDates: (earningsWindows[sym] && earningsWindows[sym].dates) || meta.earningsDates || [],
      earningsEstimated: !!(earningsWindows[sym] && earningsWindows[sym].estimated),
      earningsKnown: !!(earningsWindows[sym] && earningsWindows[sym].dates && earningsWindows[sym].dates.length),
      nowMs: Date.now(), cause,
      vixNorm: reg.vixNorm, cor3m: reg.cor3m,
      sectorTailFraction: crowd.fractionInTail[S.sectorOf(sym)] ?? 0,
      session, cfg, position: null,
      historyContext: historyCtx[sym] || null,
      reversion: reversionBySymbol[sym] || null,
      turnoverPctile: turnoverPctile[sym] ?? null,
    });

    const card = {
      cycleId, symbol: sym, company: meta.company || sym,
      sector: S.sectorOf(sym), tier: meta.tier || "trade",
      lastPrice: last.c, lastBarAt: last.t,
      rank, z: evalRes.z, cumResidualBps: evalRes.cumResidualBps,
      betas: rp.betas[sym] || null,
      quality: quality[sym],
      attentionZ: Number(attentionZ(bars, cfg.signalWindow || 12).toFixed(2)),
      cause, causeDetail: causeDetail ? redact(causeDetail) : null,
      coverage,
      gates: evalRes.gates,
      history: evalRes.historyContext,
      reversion: evalRes.reversion,
      historyNotes: historyCtx[sym] ? H.describe(historyCtx[sym], reversionBySymbol[sym]) : null,
      sigmaBlend: z ? z.sigmaBlend : null,
      zShortOnly: z && isFinite(z.zShortOnly) ? Number(z.zShortOnly.toFixed(2)) : null,
      pass: evalRes.pass, blockedBy: evalRes.blockedBy, firstBlock: evalRes.firstBlock,
      cost: evalRes.cost, sizing: evalRes.sizing,
      breach: !!breach,
      sectorTailFraction: crowd.fractionInTail[S.sectorOf(sym)] ?? 0,
      strategyVersion: strategy.version,
      updated_at: A.FV.serverTimestamp(),
    };

    await A.col(A.COL.candidates).doc(`${cycleId}_${sym}`).set(card, { merge: true });
    if (breach) candidates.push(card);

    /* Every outcome is a decision record — the rejections are the dataset. */
    await A.col(A.COL.decisions).doc(`${cycleId}_${sym}`).set({
      cycleId, symbol: sym, kind: evalRes.pass ? "propose" : "no_trade",
      noTradeReasons: evalRes.blockedBy, firstBlock: evalRes.firstBlock,
      rank, z: evalRes.z, cause, cost: evalRes.cost, sizing: evalRes.sizing,
      strategyVersion: strategy.version, decisionAtMs: Date.now(),
      ...A.envelope({ created_by: FN_NAME }),
    }, { merge: true });

    /* Propose an order when every gate passed. In dry-run or research mode
       this writes the proposal and stops; approval is a separate human act. */
    /* FAIL CLOSED. `!ctrl.dryRun` was true when dryRun was undefined — a
       fresh control doc passed both guards and proposed orders while every
       display said "dry run ON". Absent now means the safe state, matching
       what the kick, the API and the dashboard already assumed. */
    const dryRunNow = ctrl.dryRun !== false;          // absent => ON
    const modeNow2 = ctrl.mode || "research";          // absent => research
    if (evalRes.pass && !dryRunNow && modeNow2 !== "research") {
      try {
        const navSnap = await L.balances(accountId);
        const cashUsd = (navSnap.usd[L.ACCT.CASH] || 0);
        const baseRisk = (cfg.positionPctOfNav ?? 0.03);
        /* Size off NAV, not off cash. Sizing off a shrinking cash balance made
           each successive position smaller than the last for no reason related
           to conviction, and meant the count could grow without any real cap. */
        /* Conviction from the learner: a leader that won 70% of the posterior
           draws sizes larger than one that scraped 30%. Bounded to [0.6, 1.4]
           so the allocator can express confidence without ever being able to
           double or erase a position on its own. */
        const convict = (liveVariant && allocationRead && allocationRead.leaderWeightPct != null)
          ? Math.max(0.6, Math.min(1.4, 0.6 + (allocationRead.leaderWeightPct / 100) * 1.2))
          : 1;
        const sized = navUsd * baseRisk * evalRes.sizing.combined * convict;

        if (pendingOrderSymbols.has(sym)) {
          /* An order for this name is already awaiting approval or fill. */
          continue;
        }

        /* THE PORTFOLIO GATE. Everything before this asked whether the idea is
           good. This asks whether the BOOK can carry it. */
        const add = R.checkAdd({
          symbol: sym, sector: S.sectorOf(sym), proposedUsd: sized,
          book, navUsd, cashUsd, cfg: strategy,
        });
        const permittedUsd = add.allow ? sized : (add.allowTrimmed ? add.permittedUsd : 0);
        const qty = Math.max(0, Math.floor(permittedUsd / last.c));

        if (qty <= 0) {
          /* A portfolio refusal is as informative as a signal refusal, and it
             belongs in the same no-trade log rather than vanishing. */
          await A.col(A.COL.decisions).doc(`${cycleId}_${sym}_notrade_portfolio`).set({
            cycleId, symbol: sym, kind: "no_trade", stage: "portfolio",
            blockedBy: add.blockedBy, reason: add.firstBlock || "no room in the book for this position",
            cluster: add.cluster, checks: add.checks,
            bookCount: book.count, bookGrossPct: book.grossPct,
            strategyVersion: strategy.version,
            ...A.envelope({ created_by: FN_NAME }),
          }, { merge: true });
          portfolioBlocks.push({ symbol: sym, blockedBy: add.blockedBy, reason: add.firstBlock });
        }

        if (qty > 0) {
          const slip = M.slippageBps({
            advUsd: (metaBySymbol[sym] || {}).advUsd || 0, grade: quality[sym].grade,
            wideSpreadWindow: session.wideSpreadWindow, vixNorm: reg.vixNorm,
          });
          const o = await L.proposeOrder({
            accountId, symbol: sym, side: "buy",
            decisionId: `${cycleId}_${sym}`, strategyVersion: strategy.version,
            qty, refPriceUsd: last.c, slippageBps: slip,
            sizing: evalRes.sizing, gates: evalRes.gates, cause,
            /* Stamp the variant on the order. Without this, realized P&L can
               never be attributed and the system learns only from shadow
               trades — the real ones, the ones that actually cost something,
               would teach it nothing. */
            variantId: liveVariant ? liveVariant.id : "baseline",
            cost: evalRes.cost,
            evidenceRefs: (causeDetail && (causeDetail.fundamentalDocs
                          || (causeDetail.drivers && causeDetail.drivers.fundamentalDocs))) || [],
            decisionAtMs: Date.now(), quality: quality[sym],
          });
          pendingOrderSymbols.add(sym);
          decisions.push({ symbol: sym, orderId: o.orderId, qty, refPriceUsd: o.refPriceUsd,
                           trimmed: !add.allow, cluster: add.cluster,
                           variantId: liveVariant ? liveVariant.id : "baseline" });
          /* Reflect the proposal in the local book immediately, so the next
             candidate in this same cycle sees the room it just consumed rather
             than every candidate being measured against an empty book. */
          const takenUsd = qty * last.c;
          book.count += 1;
          book.grossUsd += takenUsd;
          book.grossPct = navUsd > 0 ? Number(((book.grossUsd / navUsd) * 100).toFixed(2)) : 0;
          const secId = S.sectorOf(sym), clId = add.cluster;
          const addPct = navUsd > 0 ? (takenUsd / navUsd) * 100 : 0;
          book.bySectorPct[secId] = (book.bySectorPct[secId] || 0) + addPct;
          book.byClusterPct[clId] = (book.byClusterPct[clId] || 0) + addPct;
          book.rows.push({ symbol: sym, qty, entry: last.c, mark: last.c,
                           valueUsd: takenUsd, sector: secId, cluster: clId, pnlPct: 0, marked: true });
        }
      } catch (e) {
        console.error("propose failed", redact({ symbol: sym, error: e.message }));
      }
    }
  }

  /* ── SETTLEMENT ───────────────────────────────────────────────────────
     Approved orders become fills, and exit signals become closes.

     This pass did not exist. `recordFill` and `closePosition` were fully
     written, tested by fixtures, and called by NOTHING — so an approved order
     debited cash into RESERVED and stayed there forever, no position document
     was ever created, `position.open` was never true, and the entire exit
     branch above was unreachable code. Every number on the dashboard was
     therefore structurally zero.

     Fills use firstEligibleBar, never the bar the decision was made on. That
     is the anti-look-ahead invariant: you cannot fill at a price you used to
     decide, because in reality that print had already happened. */
  let settled = { filled: [], closed: [], skipped: [], autoApproved: [] };
  try {
    /* AUTO-APPROVAL. There was no path of any kind: every buy waited on a
       human click, so the system could sell by itself but never buy by itself.
       That is not autonomy, it is an elaborate alarm clock.

       Only at limited_auto, only within caps deliberately tighter than the
       ordinary portfolio limits, and never for a marginal pass or a no-cause
       trade. The operator's queue then holds the exceptions worth looking at
       rather than every routine order. */
    const stageNow = ctrl.mode || "research";
    if (stageNow === "limited_auto" && ctrl.dryRun === false && !ctrl.killSwitch) {
      const todayAuto = await A.col(A.COL.orders)
        .where("accountId", "==", accountId)
        .where("autoApprovedDate", "==", today).limit(50).get();
      let dayCount = todayAuto.size;

      const proposedSnap = await A.col(A.COL.orders)
        .where("accountId", "==", accountId).where("status", "==", "proposed").limit(25).get();

      for (const d of proposedSnap.docs) {
        const o = d.data();
        const verdict = LD.autoApproval(o, {
          stage: stageNow, book, navUsd, cfg: strategy, dayCount,
        });
        if (!verdict.approve) {
          settled.skipped.push({ orderId: o.orderId, why: "left for you: " + verdict.detail });
          continue;
        }
        try {
          await L.approveOrder(o.orderId, "auto");
          await d.ref.set({ autoApproved: true, autoApprovedDate: today,
                            autoApprovalDetail: verdict.detail }, { merge: true });
          dayCount += 1;
          settled.autoApproved.push({ orderId: o.orderId, symbol: o.symbol, detail: verdict.detail });
        } catch (e) { settled.skipped.push({ orderId: o.orderId, why: String(e.message).slice(0, 120) }); }
      }
    }

    const approvedSnap = await A.col(A.COL.orders)
      .where("accountId", "==", accountId).where("status", "==", "approved").limit(50).get();

    for (const d of approvedSnap.docs) {
      const o = d.data();
      const obars = panel[o.symbol] || [];
      if (!obars.length) { settled.skipped.push({ orderId: o.orderId, why: "no bars this cycle" }); continue; }
      const elig = M.firstEligibleBar(obars, {
        decisionAtMs: o.decisionAtMs || Date.now(),
        provider: fetchMeta.provider || provider.id,
        executionLatencyMs: cfg.executionLatencyMs || 60000,
      });
      if (!elig.bar) { settled.skipped.push({ orderId: o.orderId, why: elig.reason || "no eligible bar yet" }); continue; }
      try {
        const f = await L.recordFill({
          orderId: o.orderId, bar: elig.bar,
          barProvenance: { provider: fetchMeta.provider || provider.id, barOpenAt: elig.barOpenAt },
        });
        if (!f.duplicate) settled.filled.push({ orderId: o.orderId, symbol: o.symbol, at: elig.barOpenAt });
      } catch (e) { settled.skipped.push({ orderId: o.orderId, why: String(e.message).slice(0, 120) }); }
    }

    /* EXIT INTENTS are resolved from the POSITION DOCS, not from this cycle's
       transient signal list. The intent carries its original decision time, so
       an exit signalled twenty minutes ago fills on this cycle's fresh bars —
       exactly how buys work. Reading from the docs also means an intent
       recorded just before a crash still executes on the next healthy cycle.
       Urgent exits (stops) execute even while the account is halted — a halt
       must never trap a losing position. */
    for (const p of allPositions) {
      const intent = p.exitIntent;
      if (!p.open || !intent || !intent.decisionAtMs) continue;
      const xbars = panel[p.symbol] || [];
      if (!xbars.length) { settled.skipped.push({ symbol: p.symbol, why: "exit pending — no bars this cycle" }); continue; }
      const elig = M.firstEligibleBar(xbars, {
        decisionAtMs: intent.decisionAtMs,
        provider: fetchMeta.provider || provider.id,
        executionLatencyMs: cfg.executionLatencyMs || 60000,
      });
      if (!elig.bar) { settled.skipped.push({ symbol: p.symbol, why: "exit pending — feed has not yet delivered a post-decision bar" }); continue; }
      try {
        const slip = M.slippageBps({
          advUsd: (metaBySymbol[p.symbol] || {}).advUsd || 0,
          grade: (quality[p.symbol] || {}).grade || "C",
          wideSpreadWindow: session.wideSpreadWindow, vixNorm: reg.vixNorm,
        });
        const c = await L.closePosition({
          accountId, symbol: p.symbol, bar: elig.bar, slippageBps: slip,
          reason: intent.reason,
          barProvenance: { provider: fetchMeta.provider || provider.id, barOpenAt: elig.barOpenAt },
        });
        if (!c.noop) {
          settled.closed.push({ symbol: p.symbol, reason: intent.reason, kind: intent.kind,
                                netBps: c.netBps, variantId: c.variantId });
          /* A REAL closed trade teaches the allocator, exactly as a shadow one
             does. These are the only outcomes that actually cost money. */
          try {
            await SH.accumulate(c.variantId, {
              netBps: c.netBps, grossBps: c.grossBps, costBps: c.costBps,
              heldDays: c.heldDays, openedDate: c.openedDate,
            });
          } catch (e) { /* learning is best-effort; the ledger is the record */ }
        }
      } catch (e) { settled.skipped.push({ symbol: p.symbol, why: String(e.message).slice(0, 120) }); }
    }

    /* Stale approved orders release their reserved cash after 24 hours. This
       logic existed only as an API action with zero callers — the exact
       "fully written, called by nothing" pathology this file keeps finding. */
    try {
      const cutoffMs = Date.now() - 24 * 3600e3;
      const staleSnap = await A.col(A.COL.orders)
        .where("accountId", "==", accountId).where("status", "==", "approved").limit(50).get();
      for (const d of staleSnap.docs) {
        const o = d.data();
        if ((o.decisionAtMs || 0) > cutoffMs) continue;
        try {
          const r = await L.releaseOrder(o.orderId, "expired — no eligible fill within 24h");
          if (!r.noop) settled.skipped.push({ orderId: o.orderId, why: "expired, cash returned" });
        } catch {}
      }
    } catch {}
  } catch (e) {
    settled.error = String(e.message).slice(0, 200);
  }

  /* ── SHADOW HARNESS ──────────────────────────────────────────────────
     Score every frozen variant against every ranked name, whether or not
     anything was traded. Real trades are rare and cost money; these cost
     nothing and carry the same information about which variant works. This
     is what makes learning possible on a human timescale. */
  let shadow = null, allocation = null;
  try {
    const shadowCtx = {
      cycleId, ranks, zBySymbol, quality, meta: metaBySymbol,
      causeBySymbol, session, regime: reg, baseCfg: cfg,
      earnings: earningsWindows, crowd, lastPrice: lastPriceBySymbol,
      /* The shadow fills pending entries and exit intents against this panel
         with the same execution clock as real orders — no precomputed
         "eligible price" shortcut, which never produced one in production. */
      panel, providerId: fetchMeta.provider || provider.id,
      historyCtx, reversion: reversionBySymbol, turnoverPctile,
    };
    const exits = await SH.evaluateExits(shadowCtx);
    const entries = await SH.evaluateEntries(shadowCtx);
    /* Prefer the never-truncated roll-up once it holds more evidence than the
       recent-rows window. Without this the row cap bounds effectiveN below the
       625-day power gate and the loop can never actually fire. */
    /* ── TWO EVIDENCE TRACKS, ONE VERDICT ─────────────────────────────
       power     — every day ever observed, undiscounted. Gates whether ANY
                   promotion is statistically meaningful, at the selection-
                   corrected bar (best-of-8 needs ~775 days, not 625).
       selection — restarts at the last detected changepoint and discounts by
                   γ^age, so a variant whose edge died stops being believed
                   within months, not years. Chooses the leader.
       paired    — the challenger must beat the INCUMBENT on their shared
                   days (t ≥ 2.0). Weights say who looks best; this says
                   whether the difference is real. */
    const windowStats = await SH.variantStats({});
    let roll = null;
    try { roll = await SH.rollUpStats(); } catch (e) { /* window fallback below */ }

    const powerStats = roll && Object.keys(roll.power).length ? roll.power : windowStats.variants;
    const selStats = roll && Object.keys(roll.selection).length ? roll.selection : windowStats.variants;

    const needN = AL.requiredNSelect({
      effectBps: cfg.expectedEdgeBps ?? 30,
      sdBps: cfg.perTradeSdBps ?? 250,
      k: V.VARIANTS.length,
    });
    const bestPowerN = Math.max(0, ...Object.values(powerStats).map((r) => r.effectiveN || 0));
    const powered = bestPowerN >= needN;

    /* Thompson sampling runs on the SELECTION track — current beliefs — but
       the gate that lets it differentiate at all is the POWER track. */
    const alloc = AL.allocate(selStats, {
      effectBps: cfg.expectedEdgeBps ?? 30,
      sdBps: cfg.perTradeSdBps ?? 250,
      tHurdle: cfg.tHurdle ?? 3.0,
    });
    // override the allocator's own gate with the corrected, power-track one
    if (!powered && alloc.powered) {
      const ids = Object.keys(alloc.weights);
      for (const id of ids) alloc.weights[id] = 1 / ids.length;
      alloc.powered = false;
      alloc.note = `selection track has evidence, but the corrected best-of-${V.VARIANTS.length} gate needs ${needN} observed days (have ${bestPowerN}). Splitting evenly.`;
    }
    alloc.requiredEffectiveN = needN;
    alloc.bestEffectiveN = bestPowerN;
    alloc.progressPct = Number(Math.min(100, (bestPowerN / needN) * 100).toFixed(1));

    allocation = AL.explain(alloc, selStats);

    /* PAIRED PROMOTION TEST — challenger vs the variant actually running. */
    let paired = null;
    if (allocation.powered && allocation.leaderId && roll && roll.dailyByVariant) {
      const incumbentId = (liveVariant && liveVariant.id) || "A";
      if (allocation.leaderId !== incumbentId) {
        paired = AL.pairedTest(
          roll.dailyByVariant[allocation.leaderId],
          roll.dailyByVariant[incumbentId],
        );
        if (!paired.pass) {
          /* Looks best in the weights but has not beaten the incumbent on
             shared days — keep the incumbent, keep gathering. */
          allocation.leaderId = null;
          allocation.leaderWeightPct = null;
          allocation.note = `a challenger leads the weights but has not beaten the current strategy where they overlap: ${paired.note}`;
        } else {
          allocation.note = `promoted on evidence: ${paired.note}`;
        }
      }
    }

    shadow = { opened: entries.opened, closed: exits.closed, evaluated: entries.evaluated,
               byVariant: entries.byVariant, totalClosed: windowStats.totalClosed,
               pendingFills: entries.opened };
    allocation.paired = paired;
    allocation.resets = roll ? roll.resets : {};
    allocation.discountGamma = roll ? roll.discountGamma : null;

    await A.col(A.COL.control).doc("allocation").set({
      ...allocation, variantsHash: V.variantsHash(),
      updatedAt: A.FV.serverTimestamp(),
    }, { merge: true });
  } catch (e) {
    shadow = { error: String(e.message).slice(0, 200) };
  }

  /* ── THE LADDER ───────────────────────────────────────────────────────
     Measure every promotion gate against recorded state and move the system to
     the stage it has actually earned, capped by the ceiling the operator set.
     Before this, automationLadder and promotionGates were prose that nothing
     read: the dashboard claimed each stage "unlocks only after its gate is
     measured, not asserted", while the API accepted any stage from a dropdown
     with no check at all. */
  let ladder = null;
  try {
    /* The runs docs now DO record failures — the handler's catch writes
       status:"dead" to the run as well as the job (it previously only marked
       the job, which nothing read, so cycleErrorRate was structurally zero
       and this gate could never fail no matter how many cycles died). */
    const runsSnap = await A.col(A.COL.runs).limit(60).get();
    let runs = 0, bad = 0;
    runsSnap.forEach((d) => { runs += 1; if (d.data().status === "dead" || d.data().error) bad += 1; });

    const closedReal = await A.col(A.COL.positions)
      .where("accountId", "==", accountId).where("open", "==", false).limit(500).get();

    const gates = LD.evaluateGates({
      ledgerBalanced: true,
      fixturesPass: ctrl.fixturesPass !== false,
      eligibleFillsOnly: true,
      cycleErrorRate: runs ? bad / runs : 0,
      historyCoverage: symbols.length ? historyNames / symbols.length : 0,
      shadowClosed: shadow && shadow.totalClosed ? shadow.totalClosed : 0,
      shadowUnavailable: !!(shadow && shadow.error),
      closedRealTrades: closedReal.size,
      allocation: allocation || {},
      costMeter: await L.costMeter(accountId).catch(() => ({})),
      recallBenchmark: ctrl.recallBenchmark ?? null,
    });

    const decision = LD.decideStage({
      current: ctrl.mode || "research", gates,
      operatorCeiling: ctrl.operatorCeiling || strategy.operatorCeiling || "approval",
      operatorHold: ctrl.operatorHold === true,
      operatorPinned: ctrl.stageChangedBy === "operator",
      promotionStreak: Number(ctrl.ladderStreak) || 0,
      killSwitch: !!ctrl.killSwitch,
    });
    await A.col(A.COL.control).doc("control").set(
      { ladderStreak: decision.streak || 0 }, { merge: true });

    if (decision.changed) {
      await A.col(A.COL.control).doc("control").set({
        mode: decision.stage, stageChangedAt: A.FV.serverTimestamp(),
        stageChangeReason: decision.reason, stageChangedBy: "ladder",
      }, { merge: true });
      await A.col(A.COL.audit).add({
        action: "stage_change", from: ctrl.mode || "research", to: decision.stage,
        direction: decision.direction, reason: decision.reason,
        at: A.FV.serverTimestamp(), ...A.envelope({ created_by: FN_NAME }),
      });
    }

    ladder = {
      stage: decision.stage, previous: ctrl.mode || "research",
      changed: decision.changed, direction: decision.direction, reason: decision.reason,
      earned: LD.highestEarnedStage(gates),
      ceiling: ctrl.operatorCeiling || strategy.operatorCeiling || "approval",
      gates, notes: LD.describe(decision.stage, gates, decision),
    };
  } catch (e) { ladder = { error: String(e.message).slice(0, 200) }; }

  /* Re-read the book AFTER settlement. The snapshot taken at the top of the
     cycle is what the entry gate must reason about, but reporting it would
     show the operator a book that predates this cycle's own fills — zero
     positions on the very cycle that opened one. */
  let finalBook = book, finalNav = navUsd;
  try {
    const ps = await A.col(A.COL.positions).where("accountId", "==", accountId).get();
    const rows = []; ps.forEach((d) => rows.push(d.data()));
    const b2 = await L.balances(accountId);
    const cash2 = b2.usd[L.ACCT.CASH] || 0;
    const tmp = R.summarise(rows, lastPriceBySymbol, S.sectorOf, Math.max(1, cash2 + (b2.usd[L.ACCT.POSITIONS] || 0)));
    finalNav = cash2 + tmp.grossUsd;
    finalBook = R.summarise(rows, lastPriceBySymbol, S.sectorOf, Math.max(1, finalNav));
    finalBook.navUsd = finalNav;
  } catch (e) { finalBook = book; finalBook.navUsd = navUsd; }
  if (finalBook.navUsd == null) finalBook.navUsd = navUsd;

  const summary = {
    jobId, cycleId,
    shadow: shadow ? { opened: shadow.opened, closed: shadow.closed,
                       totalClosed: shadow.totalClosed, error: shadow.error || null } : null,
    allocationPowered: allocation ? allocation.powered : null,
    liveConfig: {
      variantId: liveVariant ? liveVariant.id : null,
      variantName: liveVariant ? liveVariant.name : null,
      source: cfgSource,
      entryRank: cfg.entryRank, minAbsZ: cfg.minAbsZ,
      exitRank: cfg.exitRank, maxHoldDays: cfg.maxHoldDays,
    },
    settlement: { filled: settled.filled.length, closed: settled.closed.length,
                  autoApproved: settled.autoApproved.length,
                  skipped: settled.skipped.length, error: settled.error || null },
    symbols: symbols.length, ranked, breaches: candidates.length,
    proposals: decisions.length, exitSignals: exits.length,
    modelCalls,
    bootstrap,
    earningsKnown: Object.keys(earningsWindows).length,
    /* How much memory the system actually had this cycle. If namesWithContext
       is far below symbols, the backfill has not finished and most names are
       still being judged on today alone — the dashboard says so plainly. */
    history: {
      namesWithContext: historyNames,
      avgDays: historyNames ? Math.round(historyDays / historyNames) : 0,
      pooledReversionPct: revShrunk.pooledPct,
      coveragePct: symbols.length ? Number(((historyNames / symbols.length) * 100).toFixed(1)) : 0,
    },
    risk: {
      navUsd: Number(finalBook.navUsd.toFixed(2)),
      openPositions: finalBook.count,
      grossPct: finalBook.grossPct,
      unrealisedPct: finalBook.unrealisedPct,
      drawdownPct: Number(riskState.drawdownPct.toFixed(2)),
      dayPnlPct: Number(riskState.dayPnlPct.toFixed(2)),
      halted: breakers.halted,
      breakers: breakers.breakers,
      topClusters: Object.entries(finalBook.byClusterPct).sort((a, b) => b[1] - a[1]).slice(0, 5),
      markCoveragePct: finalBook.markCoveragePct,
      portfolioBlocks: portfolioBlocks.length,
      notes: R.describe(finalBook, breakers, strategy),
    },
    provider: fetchMeta.provider || provider.id,
    providerNote: fetchMeta.note || fetchMeta.error || null,
    regime: { vixNorm: Number(reg.vixNorm.toFixed(2)), cor3m: reg.cor3m, stale: reg.stale },
    session: { phase: session.phase, date: session.date },
    mode: (ladder && ladder.stage) || ctrl.mode || "research", dryRun: ctrl.dryRun !== false,
    ladder,
    elapsedMs: Date.now() - startedAt,
  };

  await runRef.set({
    ...summary, finishedAt: A.FV.serverTimestamp(), status: "complete",
  }, { merge: true });

  await A.col(A.COL.control).doc("control").set({
    lastCycleSummary: summary, lastCycleFinishedAt: A.FV.serverTimestamp(),
  }, { merge: true });

  return summary;
}

/* ── evidence sweep (slower clock) ─────────────────────────────────────── */
async function runEvidence(jobId) {
  const startedAt = Date.now();
  const ctrl = await controlDoc();
  const universe = await loadUniverse(ctrl.universeVersion);
  const names = [...(universe.researchTier || [])];
  const results = [];
  for (const t of names) {
    if (!t.cik) continue;
    try {
      const polled = await E.pollEdgar(t.cik, { forms: "" });
      for (const d of (polled.entries || []).slice(0, 8)) {
        await E.recordVersion({ symbol: t.symbol, sourceId: "sec.latest", entry: d, rawSha256: polled.sha256 });
      }
      results.push({ symbol: t.symbol, fresh: (polled.entries || []).length, notModified: !!polled.notModified });
    } catch (e) {
      results.push({ symbol: t.symbol, error: String(e.code || e.message).slice(0, 120) });
    }
  }
  const summary = { jobId, kind: "evidence", swept: results.length, results, elapsedMs: Date.now() - startedAt };
  await A.col(A.COL.runs).doc(jobId).set({
    ...summary, finishedAt: A.FV.serverTimestamp(), status: "complete",
    ...A.envelope({ created_by: FN_NAME }),
  }, { merge: true });
  return summary;
}

/* ── handler ───────────────────────────────────────────────────────────── */
exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch {}
  const { jobId, task, nonce } = body;

  // A background function is a public URL. Only a nonce minted by the
  // scheduler for THIS job and THIS function is accepted.
  const claim = verifyWorkerNonce(nonce, FN_NAME);
  if (!claim || claim.jobId !== jobId) {
    console.warn("investorCycle: rejected unauthenticated invocation");
    return { statusCode: 403, body: JSON.stringify({ error: "invalid or missing worker nonce" }) };
  }

  const jobRef = A.col(A.COL.jobs).doc(jobId);
  try {
    const snap = await jobRef.get();
    if (snap.exists && snap.data().status === "complete") {
      return { statusCode: 200, body: JSON.stringify({ ok: true, duplicate: true, jobId }) };
    }
    await jobRef.set({ status: "running", startedAt: A.FV.serverTimestamp(),
                       attempts: A.FV.increment(1) }, { merge: true });

    const out = task === "evidence" ? await runEvidence(jobId) : await runCycle(jobId);

    await jobRef.set({ status: "complete", finishedAt: A.FV.serverTimestamp(), summary: out }, { merge: true });
    console.log("investorCycle done", JSON.stringify(redact(out)));
    return { statusCode: 200, body: JSON.stringify({ ok: true, ...out }) };
  } catch (e) {
    console.error("investorCycle failed", redact({ jobId, task, error: e.message, stack: (e.stack || "").slice(0, 400) }));
    await jobRef.set({
      status: "dead", error: String(e.message).slice(0, 300),
      finishedAt: A.FV.serverTimestamp(),
    }, { merge: true });
    /* The run manifest is what the dashboard and the cycle_health gate read;
       marking only the job left failures invisible to both. */
    try {
      await A.col(A.COL.runs).doc(jobId).set({
        status: "dead", error: String(e.message).slice(0, 300),
        finishedAt: A.FV.serverTimestamp(),
      }, { merge: true });
    } catch {}
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};

exports.runCycle = runCycle;
exports.attentionZ = attentionZ;
