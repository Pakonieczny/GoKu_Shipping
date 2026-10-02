/*  netlify/functions/_stationPinLogin.js
 *  The stations' Employee Number login, answered by the server (firebaseOrders {pinLogin}). The roster is ONE Firestore
 *  document, Brites_Orders/"Employee Numbers": each employee is a field, the field NAME is the 6-digit number and the value
 *  is the name. The station pages used to download the whole document and look the typed number up in the browser, so
 *  anyone with the link could read every number. Now a page sends the number it was given and gets back that person's
 *  name, or "no". The document itself, any other number, and the number just typed never leave this function.
 *
 *    POST { pinLogin: "123456" }  →  200 { ok: true, name }          a known number: the name as stored (spaces tidied)
 *                                    200 { ok: false, error }        a number that is not on the list (and says nothing more)
 *                                    400 { ok: false, error }        not exactly six digits, or a body that is too large
 *                                    429 { ok: false, tooMany: true, error: "Too many tries, wait a minute" }
 *
 *  Anything else (a Firestore failure, the roster missing) is an error WITHOUT an `ok` field, so a page knows the check was
 *  not answered; only then may it fall back to the old list download. An `ok` answer is final.
 *
 *  Guessing: there are a million numbers, so a guess costs the guesser time.
 *    · per address (per warm instance, as firebaseOrders' other limits): 10 wrong tries a minute, or 40 requests a minute, start
 *      a one-minute lockout (nothing is answered meanwhile, not even a right number: a guesser must never learn a hit);
 *    · everybody together: 30 wrong tries a minute start the same lockout. This count is kept in Firestore
 *      (Station_PinLogin/limits) so it holds across instances; it is read and written only around WRONG tries (a few a day
 *      in a shop) and it FAILS OPEN: a Firestore error here never keeps an employee out. The in-memory count of this
 *      instance still applies. A right number never adds to any count.
 *    · a wrong try is answered no sooner than about 0.4 s after it arrived, so a guess is slow and an unknown number looks
 *      the same as a refused record;
 *    · an address that went quiet starts clean after the minute; nothing is stored per address.
 *  The number is never logged and never put in a response, an error message or a document; the only thing written is the
 *  global counter (a time window, a count and a lockout time).  Nothing here calls Etsy. */
"use strict";

const ROSTER_COLL = "Brites_Orders", ROSTER_DOC = "Employee Numbers";
const LIMITS_COLL = "Station_PinLogin", LIMITS_DOC = "limits";
const WINDOW_MS = 60000, LOCK_MS = 60000;
const IP_MAX_FAILS = 10, IP_MAX_REQS = 40, GLOBAL_MAX_FAILS = 30;
const FAIL_MIN_MS = 400, FAIL_JITTER_MS = 80;
const MAX_BODY_CHARS = 1024;
const TOO_MANY = "Too many tries, wait a minute";

/* time, waiting and chance are replaceable so a test runs in no time */
const deps = {
  now: () => Date.now(),
  sleep: ms => new Promise(r => setTimeout(r, ms)),
  rand: () => Math.random()
};

/* per-address counters of this instance, and this instance's own count of everybody's wrong tries */
const ips = new Map();
const glob = { t0: 0, f: 0, lockedUntil: 0 };
function reset() { ips.clear(); glob.t0 = 0; glob.f = 0; glob.lockedUntil = 0; }

function clientIp(event) {
  const h = (event && event.headers) || {};
  return String(h["x-nf-client-connection-ip"] || h["client-ip"] || String(h["x-forwarded-for"] || "").split(",")[0] || "?").trim() || "?";
}
function entry(ip, now) {
  let e = ips.get(ip);
  if (e && e.lockedUntil <= now && now - e.t0 >= WINDOW_MS) e = null;      // quiet for a minute (or the lockout is over): clean
  if (!e) {
    if (ips.size > 5000) { for (const [k, v] of ips) if (v.lockedUntil <= now && now - v.t0 >= WINDOW_MS) ips.delete(k); if (ips.size > 5000) ips.clear(); }
    e = { t0: now, n: 0, f: 0, lockedUntil: 0 }; ips.set(ip, e);
  }
  return e;
}
const retryAfter = (until, now) => Math.max(1, Math.ceil((until - now) / 1000));

/** { locked: seconds } while this address, or everybody, is locked out; otherwise null (and the request counts). */
function admit(event) {
  const now = deps.now(), ip = clientIp(event), e = entry(ip, now);
  if (e.lockedUntil > now) return { locked: retryAfter(e.lockedUntil, now) };
  if (glob.lockedUntil > now) return { locked: retryAfter(glob.lockedUntil, now) };
  e.n++;
  if (e.n > IP_MAX_REQS) { e.lockedUntil = now + LOCK_MS; console.warn("[pinLogin] one address asked too often: a minute's lockout"); return { locked: retryAfter(e.lockedUntil, now) }; }
  return null;
}
/** a wrong try (or a wrong manager key): counted for this address and for this instance's view of everybody */
function failed(event) {
  const now = deps.now(), e = entry(clientIp(event), now);
  e.f++;
  if (e.f >= IP_MAX_FAILS && e.lockedUntil <= now) { e.lockedUntil = now + LOCK_MS; console.warn("[pinLogin] too many wrong tries from one address: a minute's lockout"); }
  if (now - glob.t0 >= WINDOW_MS) { glob.t0 = now; glob.f = 0; }
  glob.f++;
  if (glob.f >= GLOBAL_MAX_FAILS && glob.lockedUntil <= now) { glob.lockedUntil = now + LOCK_MS; console.warn("[pinLogin] too many wrong tries in all: a minute's lockout"); }
}

const resp = (statusCode, body, extra) => ({ statusCode, body, headers: Object.assign({ "Cache-Control": "no-store" }, extra || {}) });
const tooMany = secs => resp(429, { ok: false, tooMany: true, error: TOO_MANY }, { "Retry-After": String(secs) });

/* everybody's wrong tries, across instances. Fails open: nothing here may keep an employee out. */
async function sharedLockedUntil(db) {
  try {
    const s = await db.collection(LIMITS_COLL).doc(LIMITS_DOC).get();
    const d = s && s.exists ? (s.data() || {}) : {};
    return Number(d.lockedUntil) || 0;
  } catch (e) { console.warn("[pinLogin] shared count unavailable (reads fail open):", e && e.message); return 0; }
}
async function sharedFailed(db) {
  try {
    const ref = db.collection(LIMITS_COLL).doc(LIMITS_DOC);
    const until = await db.runTransaction(async tx => {
      const now = deps.now(), s = await tx.get(ref), d = s && s.exists ? (s.data() || {}) : {};
      let ws = Number(d.windowStart) || 0, f = Number(d.fails) || 0, lu = Number(d.lockedUntil) || 0;
      if (now - ws >= WINDOW_MS) { ws = now; f = 0; }
      f++;
      if (f >= GLOBAL_MAX_FAILS && lu <= now) lu = now + LOCK_MS;
      tx.set(ref, { windowStart: ws, fails: f, lockedUntil: lu });
      return lu;
    });
    if (until > glob.lockedUntil) glob.lockedUntil = until;
  } catch (e) { console.warn("[pinLogin] shared count not written (fails open):", e && e.message); }
}

/** the stored value as a name: one tidy line, never empty, never only digits (a number alone is not a name); "" otherwise */
function tidyName(v) {
  if (typeof v !== "string") return "";
  const s = v.replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim();
  return s && !/^\d+$/.test(s) ? s : "";
}

/** Answers one { pinLogin } request: { statusCode, body, headers }. The number is only ever compared with the roster's field names. */
async function pinLogin(db, event, rawPin) {
  const t0 = deps.now();
  const lock = admit(event);
  if (lock) return tooMany(lock.locked);
  if (String((event && event.body) || "").length > MAX_BODY_CHARS) return resp(413, { ok: false, error: "request too large" });
  if (typeof rawPin !== "string" || !/^\d{6}$/.test(rawPin)) return resp(400, { ok: false, error: "an Employee Number is 6 digits" });

  const [snap, until] = await Promise.all([db.collection(ROSTER_COLL).doc(ROSTER_DOC).get(), sharedLockedUntil(db)]);
  if (until > deps.now()) { if (until > glob.lockedUntil) glob.lockedUntil = until; return tooMany(retryAfter(until, deps.now())); }
  if (!snap || !snap.exists) return resp(503, { error: "the sign-in list is not available" });      // no `ok`: not an answer
  const data = snap.data() || {};
  const name = Object.prototype.hasOwnProperty.call(data, rawPin) ? tidyName(data[rawPin]) : "";
  if (name) return resp(200, { ok: true, name });

  failed(event);
  await sharedFailed(db);
  const wait = Math.max(0, FAIL_MIN_MS - (deps.now() - t0)) + Math.floor(deps.rand() * FAIL_JITTER_MS);
  if (wait > 0) await deps.sleep(wait);
  return resp(200, { ok: false, error: "that Employee Number is not on the list" });
}

module.exports = { pinLogin, admit, failed, tidyName, clientIp, reset, deps, ROSTER_COLL, ROSTER_DOC, LIMITS_COLL, LIMITS_DOC, WINDOW_MS, LOCK_MS, IP_MAX_FAILS, IP_MAX_REQS, GLOBAL_MAX_FAILS, FAIL_MIN_MS, MAX_BODY_CHARS, TOO_MANY };
