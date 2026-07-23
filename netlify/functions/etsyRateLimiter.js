// netlify/functions/_shared/etsyRateLimiter.js
// Distributed token-bucket limiter + resilient Etsy fetch with 429 handling.
// Uses Firestore if FIREBASE_SERVICE_ACCOUNT is provided; otherwise falls back to in-memory
// (in-memory helps single instance, Firestore makes it safe across many instances).

const fetch = require("node-fetch");

// ---- Config
// Etsy app limits: 5 QPS and 5,000 calls/day. Operator rule: this system may
// consume at most 50% of both, leaving the rest for other apps on the key.
const ETSY_QPS_LIMIT   = 5;
const ETSY_DAILY_LIMIT = 5000;
const INTERNAL_FRACTION = 0.5;
const RATE_PER_SEC = ETSY_QPS_LIMIT * INTERNAL_FRACTION;             // 2.5/s hard cap
const DAILY_BUDGET = Math.floor(ETSY_DAILY_LIMIT * INTERNAL_FRACTION); // 2,500/day hard cap
const BURST        = 2;      // bucket capacity (floor of the QPS cap)
const MAX_RETRIES  = 5;      // on 429 / contention
const JITTER_MS    = 50;     // random jitter to avoid thundering herd

// ---- Firestore (optional but recommended)
let useFirestore = false;
let db = null;

function buildSvcFromSplitEnv() {
  const {
    FIREBASE_PROJECT_ID,
    FIREBASE_CLIENT_EMAIL,
    FIREBASE_PRIVATE_KEY,
  } = process.env;

  // Minimum needed: project_id, client_email, private_key
  if (!FIREBASE_PROJECT_ID || !FIREBASE_CLIENT_EMAIL || !FIREBASE_PRIVATE_KEY) return null;

  // Fix escaped newlines in Netlify envs
  const pk = FIREBASE_PRIVATE_KEY.includes("\\n")
    ? FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n")
    : FIREBASE_PRIVATE_KEY;

  return {
    type: "service_account",
    project_id: FIREBASE_PROJECT_ID,
    private_key: pk,
    client_email: FIREBASE_CLIENT_EMAIL,
  };
}

try {
  let svc = null;

  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    // Option A: single JSON var
    svc = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } else {
    // Option B: split vars like in your screenshot
    svc = buildSvcFromSplitEnv();
  }

  if (svc) {
    const admin = require("firebase-admin");
    if (!admin.apps.length) {
      admin.initializeApp({ credential: admin.credential.cert(svc), projectId: svc.project_id });
    }
    db = admin.firestore();
    useFirestore = true;
  }
} catch (_) {
  useFirestore = false; // falls back to in-memory token-bucket
}

// ---- In-memory fallback (per instance)
// FIFO slot scheduler: each caller reserves the next free 1/RATE slot and
// sleeps until it. Unlike a naive sleep-once bucket, this holds the cap
// under any concurrency (the previous implementation let all concurrent
// waiters through after a single shared sleep).
const memState = { nextFreeMs: 0 };

// Helpers
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const jitter = () => Math.floor(Math.random() * JITTER_MS);

function backoff(attempt) {
  // 250ms per token at 4 rps; exponential with cap
  const base = 250;
  const ms = Math.min(2000, base * Math.pow(2, attempt));
  return ms + jitter();
}

function parseRetryAfter(h) {
  if (!h) return null;
  const s = Number(h);
  if (Number.isFinite(s)) return Math.ceil(s * 1000);
  const when = Date.parse(h);
  return Number.isFinite(when) ? Math.max(0, when - Date.now()) : null;
}

// ---- Token bucket (Firestore-backed if available)
async function takeToken(bucket = "etsy-global") {
  const now = Date.now();

  if (!useFirestore) {
    // In-memory (per VM) — good safety net; Firestore is the robust path
    const interval = 500 + 15; // 2/s (strictly under the 2.5 cap even across sliding windows); +15ms wake-jitter guard
    const slot = Math.max(now, memState.nextFreeMs);
    memState.nextFreeMs = slot + interval; // reserve synchronously: FIFO, race-free in one VM
    if (slot > now) await sleep(slot - now);
    return;
  }

  // Firestore transaction to make it safe across instances
  const ref = db.collection("rate_limits").doc(bucket);
  let attempt = 0;

  while (true) {
    attempt++;
    try {
      await db.runTransaction(async tx => {
        const snap = await tx.get(ref);
        const data = snap.exists ? snap.data() : { tokens: BURST, lastMs: now };
        const last = Number(data.lastMs || now);
        const tokensStored = Number.isFinite(data.tokens) ? data.tokens : BURST;

        const elapsed = Math.max(0, now - last);
        const refill  = (elapsed / 1000) * RATE_PER_SEC;
        let tokens    = Math.min(BURST, tokensStored + refill);

        if (tokens < 1) {
          // Not enough tokens → throw with recommended retry delay
          const need   = 1 - tokens;
          const waitMs = Math.ceil((need / RATE_PER_SEC) * 1000) + jitter();
          const err    = new Error("rate-limit-wait");
          err.waitMs   = waitMs;
          throw err;
        }

        tokens -= 1;
        tx.set(ref, { tokens, lastMs: now }, { merge: true });
      });
      return; // success
    } catch (e) {
      if (e && e.message === "rate-limit-wait") {
        await sleep(e.waitMs);
      } else {
        // Transaction contention/backoff
        if (attempt >= MAX_RETRIES) throw e;
        await sleep(backoff(attempt));
      }
    }
  }
}

// ---- Daily API-call counter (America/Toronto; the day key is computed on
// now+60s so the counter rolls over at 23:59 Toronto time, per operator spec)
function torontoDayKey() {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" })
    .format(new Date(Date.now() + 60000)); // en-CA gives YYYY-MM-DD
}
// Transactional pre-call gate: atomically (a) refuses the call if the daily
// 50% budget is already spent — judged by BOTH our own count and Etsy's own
// reported usage when available — and (b) records the call and the observed
// per-second concurrency peak. Throws DAILY_BUDGET_EXHAUSTED when over.
const PER_SECOND_CAP = Math.floor(RATE_PER_SEC); // 2 calls in any epoch second — Etsy meters QPS per second, so this is airtight cluster-wide
async function chargeDailyBudget() {
  if (!useFirestore) return; // no distributed accounting possible; local pacing still applies
  const ref = db.collection("EtsyPricing_ApiUsage").doc(torontoDayKey());
  for (let attempt = 0; attempt < 40; attempt++) {
    let wait = 0;
    await db.runTransaction(async tx => {
      const snap = await tx.get(ref);
      const d = snap.exists ? snap.data() : {};
      const ourUsed = Number(d.count || 0);
      // Budget rule: THIS system may spend at most DAILY_BUDGET of its OWN
      // calls. Etsy's key-wide meter (which includes EtsyMail crons and any
      // other app on the key) does NOT count against our budget — but when
      // the whole key is nearly exhausted, stop rather than cause real 429s.
      if (ourUsed >= DAILY_BUDGET) {
        const err = new Error("DAILY_BUDGET_EXHAUSTED: this system has spent its " + DAILY_BUDGET + "-call internal daily budget (50% of Etsy's " + ETSY_DAILY_LIMIT + "). Calls resume after the 11:59 PM Toronto reset.");
        err.code = "DAILY_BUDGET_EXHAUSTED";
        throw err;
      }
      const headerFresh = d.etsy_reported_at && (Date.now() - Number(d.etsy_reported_at)) < 30 * 60 * 1000;
      if (headerFresh && d.etsy_remaining_today != null && Number(d.etsy_remaining_today) <= 100) {
        // Whole-key emergency reserve (protects every app from hard 429s).
        // Only honored while the reading is fresh — a stale reading must not
        // wedge the gate shut after Etsy's midnight-UTC reset.
        const err = new Error("DAILY_BUDGET_EXHAUSTED: Etsy reports only " + d.etsy_remaining_today + " calls left on the WHOLE API key today (all apps combined). Holding the last 100 in reserve; Etsy's key resets at midnight UTC (8:00 PM Toronto).");
        err.code = "DAILY_BUDGET_EXHAUSTED";
        throw err;
      }
      const nowMs = Date.now();
      const nowSec = Math.floor(nowMs / 1000);
      const sameSec = Number(d.sec_key) === nowSec;
      const secUsed = sameSec ? Number(d.sec_count || 0) : 0;
      if (secUsed >= PER_SECOND_CAP) {
        // this second is full — release the transaction and sleep into the next second
        wait = 1000 - (nowMs % 1000) + 5 + Math.floor(Math.random() * 30);
        return;
      }
      tx.set(ref, {
        count: ourUsed + 1,
        count_since: d.count_since || nowMs, // stamps the first call this counter ever recorded for the day
        sec_key: nowSec,
        sec_count: secUsed + 1,
        max_qps: Math.max(Number(d.max_qps || 0), secUsed + 1),
        updated_at: nowMs
      }, { merge: true });
    });
    if (!wait) return;
    await sleep(wait);
  }
  const err = new Error("Rate gate contention: could not reserve an Etsy call slot after 40 attempts.");
  err.code = "RATE_GATE_CONTENTION";
  throw err;
}
// Post-call: persist Etsy's own rate headers — the authoritative ground truth
// for what Etsy has recorded against the key today (includes other apps).
function captureEtsyHeaders(res) {
  if (!useFirestore || !res || !res.headers) return;
  try {
    const limit = res.headers.get("x-limit-per-day");
    const remaining = res.headers.get("x-remaining-today");
    if (limit == null && remaining == null) return;
    db.collection("EtsyPricing_ApiUsage").doc(torontoDayKey()).set({
      etsy_limit_per_day: limit != null ? Number(limit) : null,
      etsy_remaining_today: remaining != null ? Number(remaining) : null,
      etsy_reported_at: Date.now()
    }, { merge: true }).catch(() => {});
  } catch (_) { /* header capture must never break API calls */ }
}

// ---- Public Etsy fetch with gating + 429 retry
async function etsyFetch(url, init = {}, opts = {}) {
  const { bucket = "etsy-global", retries = MAX_RETRIES } = opts;

  let attempt = 0;
  while (true) {
    attempt++;
    await takeToken(bucket);
    await chargeDailyBudget(); // hard gate + count, incl. 429 retries

    const res = await fetch(url, init);
    captureEtsyHeaders(res);

    if (res.status !== 429) return res;

    // 429 → respect Retry-After if present, else exponential backoff
    const ra = parseRetryAfter(res.headers.get("retry-after"));
    const waitMs = ra != null ? ra + jitter() : backoff(attempt);
    if (attempt >= retries) return res; // propagate the 429 payload to caller
    await sleep(waitMs);
  }
}

module.exports = { etsyFetch, takeToken };