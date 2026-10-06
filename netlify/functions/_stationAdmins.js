/*  netlify/functions/_stationAdmins.js
 *  Who is an Admin at the stations (Paul, 6 Oct 2026: "Auto log-out all employees other than myself (Admin)"). An Admin is exempt from the idle
 *  sign-out (10 minutes without input) and from the 5:00 pm Toronto sign-out; the midnight New York sign-out still ends everybody.
 *
 *  THE LIST   Firestore config/stationAdmins, field `names` (array of strings). The document or the field missing, or no usable name in it: the
 *             fallback ["Paul K", "Paul"]. Read only: the app never writes it (Paul edits it in the Firebase console). Kept 60 s per server instance.
 *  THE NAME   compared after the same cleanup the sessions use (control characters and runs of spaces tidied, four or more digits dropped, 80
 *             characters) and then folded to ONE key per person: accents stripped, case folded, apostrophes dropped, every other punctuation mark
 *             (underscore, period, hyphen ...) read as a space, spaces collapsed. "Paul_K", "Paul K.", "paul k" and "PAUL  K" are one name (the PIN
 *             list holds "Paul_K"); "Paul" and "Paul K" stay two (a bare "Paul" never matches "Paul K").
 *  THE DOOR   firebaseOrders { stationAdmin: "<name>" } asks about ONE name and answers { ok: true, admin: true | false }. It never lists the
 *             names and never returns a PIN. Rate limited like the PIN door (per address: 40 requests a minute start a one-minute lockout,
 *             counters of its own). A list that cannot be read (and none kept) answers 503 WITHOUT `ok`: the page then treats the person as not an
 *             Admin (fail closed), while the server's own readers leave a session alone (they never end somebody on a read error).
 *  Nothing here calls Etsy, the PIN roster, or writes anywhere.  Contract: /mnt/project-files/plans/stations-round2/api.md ("AD2"). */
"use strict";

const CONFIG_COLL = "config", CONFIG_DOC = "stationAdmins";
const DEFAULT_ADMINS = Object.freeze(["Paul K", "Paul"]);
const TTL_MS = 60000;
const WINDOW_MS = 60000, LOCK_MS = 60000, IP_MAX_REQS = 40;
const MAX_BODY_CHARS = 1024, MAX_NAME_CHARS = 200, MAX_NAMES = 200;

/* time is replaceable so a test runs in no time */
const deps = { now: () => Date.now() };

/* ── names: the sessions' cleanup, then one key per person ── */
const clean = v => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim();
/** a name as the sessions keep it: tidy, 4+ digit runs dropped (a login number that slipped in), 80 characters */
function cleanName(v) {
  let s = clean(String(v == null ? "" : v).slice(0, MAX_NAME_CHARS));
  if ((s.match(/\p{Nd}/gu) || []).length >= 4) s = s.replace(/\p{Nd}+/gu, " ").replace(/\s+/g, " ").trim();
  return s.slice(0, 80);
}
/** one key per person ("" when the name has no letter: digits are a PIN, never a person) */
function keyOf(v) {
  const k = cleanName(v).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase()
    .replace(/['‘’`´]/g, "").replace(/[^\p{L}\p{N}]+/gu, " ").trim();
  return /\p{L}/u.test(k) ? k : "";
}
const keysOf = names => new Set((Array.isArray(names) ? names.slice(0, MAX_NAMES) : []).filter(n => typeof n === "string").map(keyOf).filter(Boolean));
const DEFAULT_KEYS = keysOf(DEFAULT_ADMINS);

/* ── the list, kept per Firestore handle ── */
const memos = new WeakMap();
function memoOf(db) {
  if (!db || typeof db !== "object") return { entry: null };
  let m = memos.get(db); if (!m) memos.set(db, m = { entry: null, pending: null });
  return m;
}
/**
 * The list now: { ok: true, keys: Set, source: "config" | "default", stale?: true } or { ok: false } (not readable and none kept).
 * Never throws. A failed read keeps serving the last list that was read (marked stale); only with none at all is it { ok: false }.
 */
async function load(db, opts) {
  const m = memoOf(db), now = deps.now(), force = !!(opts && opts.force);
  if (!force && m.entry && now - m.entry.at < TTL_MS) return m.entry.value;
  if (m.pending) return m.pending;
  m.pending = (async () => {
    try {
      const snap = await db.collection(CONFIG_COLL).doc(CONFIG_DOC).get();
      const data = snap && snap.exists ? (snap.data() || {}) : {};
      const keys = keysOf(data.names);
      const value = keys.size ? { ok: true, keys, source: "config" } : { ok: true, keys: new Set(DEFAULT_KEYS), source: "default" };
      m.entry = { at: deps.now(), value };
      return value;
    } catch (e) {
      console.warn("[stationAdmins] the list could not be read:", String((e && (e.message || e.code)) || e).slice(0, 120));
      if (m.entry) { const v = Object.assign({}, m.entry.value, { stale: true }); m.entry = { at: deps.now() - TTL_MS + 5000, value: v }; return v; }   // keep the last list; look again in 5 s
      return { ok: false };
    } finally { m.pending = null; }
  })();
  return m.pending;
}
/** true | false | null (null: the list could not be read and none is kept: unknown) */
async function isAdmin(db, name) {
  const k = keyOf(name);
  if (!k) return false;
  const r = await load(db);
  return r.ok ? r.keys.has(k) : null;
}
/** forget what was kept (tests; and a reader that must see a change at once) */
function forget(db) { const m = memoOf(db); m.entry = null; }

/* ── the door: per-address counters of this instance, like the PIN door ── */
const ips = new Map();
function clientIp(event) {
  const h = (event && event.headers) || {};
  return String(h["x-nf-client-connection-ip"] || h["client-ip"] || String(h["x-forwarded-for"] || "").split(",")[0] || "?").trim() || "?";
}
const resp = (statusCode, body, extra) => ({ statusCode, body, headers: Object.assign({ "Cache-Control": "no-store" }, extra || {}) });
/** { locked: seconds } while this address is locked out; otherwise null (and the request counts) */
function admit(event) {
  const now = deps.now(), ip = clientIp(event);
  let e = ips.get(ip);
  if (e && e.lockedUntil <= now && now - e.t0 >= WINDOW_MS) e = null;
  if (!e) {
    if (ips.size > 5000) { for (const [k, v] of ips) if (v.lockedUntil <= now && now - v.t0 >= WINDOW_MS) ips.delete(k); if (ips.size > 5000) ips.clear(); }
    e = { t0: now, n: 0, lockedUntil: 0 }; ips.set(ip, e);
  }
  if (e.lockedUntil > now) return { locked: Math.max(1, Math.ceil((e.lockedUntil - now) / 1000)) };
  e.n++;
  if (e.n > IP_MAX_REQS) { e.lockedUntil = now + LOCK_MS; return { locked: Math.max(1, Math.ceil(LOCK_MS / 1000)) }; }
  return null;
}
function reset() { ips.clear(); }

/**
 * Answers one { stationAdmin } request: { statusCode, body, headers }. `raw` is the name the page sent.
 * The answer is { ok, admin } and nothing else: no name, no list, no count.
 */
async function door(db, event, raw) {
  const lock = admit(event);
  if (lock) return resp(429, { ok: false, tooMany: true, error: "Too many tries, wait a minute" }, { "Retry-After": String(lock.locked) });
  if (String((event && event.body) || "").length > MAX_BODY_CHARS) return resp(413, { ok: false, error: "request too large" });
  if (typeof raw !== "string" || !raw.trim() || raw.length > MAX_NAME_CHARS) return resp(400, { ok: false, error: "one name, as text" });
  const k = keyOf(raw);
  if (!k) return resp(200, { ok: true, admin: false });                              // digits or no letter: a PIN is never a person
  const list = await load(db);
  if (!list.ok) return resp(503, { error: "the list is not available" });            // no `ok`: not an answer
  return resp(200, { ok: true, admin: list.keys.has(k) });
}

module.exports = { CONFIG_COLL, CONFIG_DOC, DEFAULT_ADMINS, TTL_MS, WINDOW_MS, LOCK_MS, IP_MAX_REQS, MAX_BODY_CHARS, MAX_NAME_CHARS,
  cleanName, keyOf, load, isAdmin, forget, door, admit, reset, clientIp, deps };
