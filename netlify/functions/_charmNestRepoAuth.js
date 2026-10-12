/*  netlify/functions/_charmNestRepoAuth.js
 *  The password door of the Charm Repo Library (Paul, 11 Oct 2026): "upload repositories, delete repositories, download any
 *  charm or make any modifications must be password-protected using my login password ... every attempt must be recorded and
 *  I must be informed with a visible Alert". Contract and shapes: plans/repo-protect-1011/contract.md and REPOAUTH-api.md.
 *
 *  WHO IS "ME"   The sorter has no personal login (a shared passcode and a typed name), so the unlock password is Paul's EMAIL-APP
 *                operator login: EtsyMail_Operators/paul (role owner = "Master Admin"; PBKDF2, written by etsyMailAuth.js), checked
 *                here on the server, every time. config/repoProtect {operators:[ids]} may name other accounts; Shell's is never
 *                accepted unless Paul lists it. The password, its hash, salt and length are never returned, logged or stored here.
 *  THE ALERT     repoAlerts / repoAlertClose answer only for a caller whose `by` is on config/stationAdmins (_stationAdmins.js).
 *  TOKENS        repoUnlock answers with a token: 32 random bytes + a short HMAC (so a document forged in Firestore is no token).
 *                Only sha256(token) is stored (CharmRepo_Tokens/{sha256}), with its expiry, its action and whether it is single use.
 *  SESSION       (Paul, 11 Oct 2026: "password protect the settings menu ... if I'm logged in and working from my account do not prompt me")
 *                The sorter cannot show the server an email-app operator session (the inbox is another origin and its endpoints need its own
 *                secret header), so a right password buys a SESSION token instead: one unlock for Settings and for the repo's upload / change /
 *                download, kept by the browser until the server's expiry (the earliest of 12 hours, the next 5:00 pm Toronto, midnight Toronto).
 *                Lock now (repoLock) deletes it here; changing the operator's password or revoking the account ends every session; a delete never
 *                rides on it by itself (a session buys a single-use delete token WITHOUT the password, with the confirm sentence still asked by the page).
 *  LOCKOUT       5 wrong from one source in 10 minutes lock that source for 15 minutes; 20 wrong from all sources lock everyone.
 *  RECORDS       EVERY attempt (right, wrong, locked) writes CharmRepo_AuthAttempts/{id} and CharmRepo_Alerts/{id}: flat fields only.
 *  THE GATE      requirement(fn, op, body) names what an HTTP call needs; authorize() checks the token. The gate sits in the HTTP
 *                handlers only (charmNestLibrary.js, charmNestOutput.js); code that calls an op in-process (charmNestCheck, the
 *                background functions) never goes through it. This file loads no firebase module until it needs Storage.          */
"use strict";
const crypto = require("crypto");
const Admins = require("./_stationAdmins");
const { json, str, safePath, header } = require("./_charmNestAuth");

const C = { ATTEMPTS: "CharmRepo_AuthAttempts", ALERTS: "CharmRepo_Alerts", TOKENS: "CharmRepo_Tokens", LOCKS: "CharmRepo_Lock", OPERATORS: "EtsyMail_Operators", CONFIG: "config", CONFIG_DOC: "repoProtect", INDEX: "Charm_Master_Index", FILES: "Charm_Master_Files" };
const MIN = 60000;
const ACTIONS = new Set(["upload", "delete", "download", "modify", "history", "other", "settings"]);
/* how long a token lives, and which are single use (a destructive click asks again) */
const TTL_MS = { upload: 15 * MIN, modify: 5 * MIN, download: 5 * MIN, other: 5 * MIN, settings: 5 * MIN, delete: 2 * MIN, history: 2 * MIN };   // (settings and any unlock asked with session:true get a SESSION token instead: sessionEnd below)
const ONCE = new Set(["delete", "history"]);
/* what each kind of token opens: the needs below are upload | modify | download | delete-file | delete-sku */
const OPENS = { upload: ["upload", "modify"], modify: ["modify"], download: ["download"], other: ["upload", "modify", "download"], delete: ["delete-file", "delete-sku"], history: ["delete-file"], session: ["settings", "upload", "modify", "download"] };
const WINDOW_MS = 10 * MIN, LOCK_MS = 15 * MIN, SRC_MAX = 5, ALL_MAX = 20;
const OPERATOR_CACHE_MS = 30000, TOKEN_CACHE_MS = 30000, SESSION_CACHE_MS = 5000, SESSION_MAX_MS = 12 * 60 * MIN, ALERT_LIST = 100, ALERT_READ = 300;

/* time is replaceable so a test runs in no time */
const deps = { now: () => Date.now() };
const sha256 = s => crypto.createHash("sha256").update(String(s), "utf8").digest("hex");
const same = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const warn = (what, e) => { try { console.warn("[repoAuth] " + what + ":", String((e && (e.message || e.code)) || e).slice(0, 160)); } catch (_) {} };

/* ── what needs a token ── */
const LIBRARY = { masterPutIndex: "upload", masterPutFile: "upload", startMaster: "upload", masterPatch: "modify", masterRemoveFile: "delete-file", masterRemoveSku: "delete-sku", masterDownload: "download", sandboxPullLimitSet: "modify", customSaveCommit: "upload", listingEtsySkuWrite: "modify" };   // (sandboxPullLimitSet: Settings → how many newest Etsy orders the sandbox pulls; a setting, 11 Oct 2026)
/* the buttons of the Settings menu that change shared state (plans/save-to-repo-1011/ADMINSESSION-settings.md): they need the SESSION unlock, from a person at the
   Settings menu or at the sandbox's Start button. Other sandbox ops (sandboxStream, sandboxStatus, sandboxCancel) are the sandbox page's own all-day traffic and stay free. */
const SETTINGS_OPS = { purgeHistory: "settings", sandboxReset: "settings", sandboxPullOrders: "settings" };
const MASTER_PATH = /^charmnest\/(?:sandbox\/)?master\//, MASTER_FILE_PATH = /^charmnest\/(?:sandbox\/)?master\/files\//;
/** What an HTTP call to function `fn` with this op and body needs: { op, need } or null (free). Pure. */
function requirement(fn, op, body) {
  if (fn === "charmNestLibrary") {
    if (Object.prototype.hasOwnProperty.call(LIBRARY, op)) return { op, need: LIBRARY[op] };
    return Object.prototype.hasOwnProperty.call(SETTINGS_OPS, op) ? { op, need: SETTINGS_OPS[op] } : null;
  }
  if (fn === "charmNestOutput") {
    const p = safePath(body && body.path);
    if (op === "sign" || op === "put" || op === "finalize") return MASTER_PATH.test(p) ? { op, need: "upload" } : null;
    if (op === "url") return MASTER_FILE_PATH.test(p) ? { op, need: "download" } : null;
  }
  return null;
}
const needWord = need => (need.startsWith("delete") ? "delete" : need);

/* ── tokens ── */
function macKey() { return crypto.createHash("sha256").update("charm-repo-token-v1\n" + (process.env.FIREBASE_PRIVATE_KEY || process.env.EDIT_PASSCODE || "no-secret-in-this-environment")).digest(); }
const macOf = rand => crypto.createHmac("sha256", macKey()).update(rand).digest("hex").slice(0, 32);
function mint() { const rand = crypto.randomBytes(32).toString("hex"); return rand + "." + macOf(rand); }
function wellFormed(t) { const m = /^([0-9a-f]{64})\.([0-9a-f]{32})$/.exec(typeof t === "string" ? t : ""); return !!m && same(m[2], macOf(m[1])); }
const tokenCache = new Map();   // sha256(token) → { rec, until } for REUSABLE tokens only; a single-use token is never kept

/* ── sessions: how long one password lasts ── */
const TORONTO = "America/Toronto";
const dtf = new Intl.DateTimeFormat("en-CA", { timeZone: TORONTO, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" });
/** Toronto's clock minus UTC at instant t, in ms (-4 h in summer, -5 h in winter). */
function torontoOffsetMs(t) {
  const p = {}; for (const x of dtf.formatToParts(new Date(t))) p[x.type] = x.value;
  return Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour % 24, +p.minute, +p.second) - Math.floor(t / 1000) * 1000;
}
/** The first instant after t at which Toronto's wall clock reads hour:00 (hour 24 = the coming midnight). */
function nextTorontoHour(t, hour) {
  const off = torontoOffsetMs(t), local = t + off;
  let cand = Math.floor(local / 86400000) * 86400000 + hour * 3600000;
  if (cand <= local) cand += 86400000;
  return cand - torontoOffsetMs(cand - off);
}
/** When a session opened at `now` ends: the earliest of 12 hours, the next 5:00 pm Toronto and the coming Toronto midnight (Paul: "until 5 pm or 12 hours, whichever first"; never over a midnight). */
const sessionEnd = now => Math.min(now + SESSION_MAX_MS, nextTorontoHour(now, 17), nextTorontoHour(now, 24));
const tsMs = v => { if (!v) return 0; if (typeof v === "number") return v; if (v instanceof Date) return v.getTime(); if (typeof v.toMillis === "function") { try { return v.toMillis(); } catch (_) { return 0; } } return typeof v._seconds === "number" ? v._seconds * 1000 : typeof v.seconds === "number" ? v.seconds * 1000 : 0; };

/** Writes a token document and returns { token, exp, once, action, session }. o.session (or action "settings") makes it a SESSION token: reusable, one unlock for Settings and for the repo's upload / change / download. */
async function issue(db, o) {
  const action = ACTIONS.has(o && o.action) ? o.action : "other", now = deps.now(), token = mint();
  const session = !ONCE.has(action) && (action === "settings" || (o && o.session === true));
  const kind = session ? "session" : action, exp = session ? sessionEnd(now) : now + TTL_MS[action];
  await db.collection(C.TOKENS).doc(sha256(token)).set(Object.assign({ exp, action: kind, once: ONCE.has(kind), used: false, at: now, by: str(o && o.by, 80), tab: str(o && o.tab, 24) }, session ? { for: action } : {}));
  return { token, exp, once: ONCE.has(kind), action, session };
}
const deny = (need, why, status) => ({ status: status || 401, body: status === 503 ? { error: "repo-unavailable" } : { error: "repo-locked", need: needWord(need), once: need.startsWith("delete"), why } });
/**
 * Checks the repoToken of a body against what `req` ({ op, need }) needs. Returns null to let the call go on, or { status, body } to answer.
 * A single-use token is spent here, atomically; a check that cannot be made is a 503, never a pass.
 */
async function authorize(db, req, body, event) {
  const need = req.need, t = body && body.repoToken;
  if (event && event.httpMethod && event.httpMethod !== "POST") return deny(need, "post-only");   // (a token never travels in a URL: a GET merges its query into the body)
  if (typeof t !== "string" || !t) return deny(need, "missing");
  if (!wellFormed(t)) return deny(need, "invalid");
  const id = sha256(t), now = deps.now();
  try {
    const hit = tokenCache.get(id);
    let rec;
    if (hit && now < hit.until) rec = hit.rec;
    else { const s = await db.collection(C.TOKENS).doc(id).get(); if (!s.exists) return deny(need, "invalid"); rec = s.data() || {}; }
    if (!(rec.exp > now)) { tokenCache.delete(id); return deny(need, "expired"); }
    if (!(OPENS[rec.action] || []).includes(need)) return deny(need, "scope");
    if (rec.action === "session") { const why = await sessionEnded(db, rec); if (why) { tokenCache.delete(id); return deny(need, why); } }
    if (rec.once) {
      const ref = db.collection(C.TOKENS).doc(id);
      const out = await db.runTransaction(async tx => {
        const s = await tx.get(ref), d = s.exists ? s.data() || {} : null;
        if (!d) return "invalid"; if (d.used) return "used"; if (!(d.exp > now)) return "expired";
        tx.update(ref, { used: true, usedAt: now, usedFor: str(req.op, 40) });
        return "ok";
      });
      return out === "ok" ? null : deny(need, out);
    }
    // a session is looked at again within seconds (Lock now on another instance, a changed password); the shorter tokens keep their 30 s
    tokenCache.set(id, { rec, until: Math.min(rec.exp, now + (rec.action === "session" ? SESSION_CACHE_MS : TOKEN_CACHE_MS)) });
    if (tokenCache.size > 500) for (const [k, v] of tokenCache) if (v.until <= now) tokenCache.delete(k);
    return null;
  } catch (e) { warn("token check", e); return deny(need, "unavailable", 503); }
}
/** Why a SESSION token no longer counts although its time has not run out: "revoked" when the operator account is gone / revoked, or its password was
    changed after the session was opened. "" while it stands. Throws when Firestore cannot be read (the caller answers 503, never a pass). */
async function sessionEnded(db, rec) {
  const list = await operatorRecords(db);
  if (!list.length) return "revoked";
  const changed = Math.max(0, ...list.map(o => tsMs(o.passwordChangedAt)));
  return changed > (+rec.at || 0) ? "revoked" : "";
}

/* ── Paul's login: the operator accounts that may unlock ── */
const operatorMemo = new WeakMap();
const OWNER_ROLES = new Set(["owner", "master_admin"]);
const PAUL_KEYS = new Set(["paul", "paul k", "paulk"]);
const usable = d => !!d && !d.revokedAt && !!d.passwordHash && !!d.salt && OWNER_ROLES.has(String(d.role || "").toLowerCase());
/** The accounts whose password unlocks: config/repoProtect.operators, else `paul`, else the active owner accounts named Paul. Throws when Firestore cannot be read. */
async function operatorRecords(db) {
  const memo = operatorMemo.get(db), now = deps.now();
  if (memo && now - memo.at >= 0 && now - memo.at < OPERATOR_CACHE_MS) return memo.list;   // (a clock that went back is never trusted as fresh)
  let ids = [];
  const cfg = await db.collection(C.CONFIG).doc(C.CONFIG_DOC).get();
  const named = cfg && cfg.exists && cfg.data() && Array.isArray(cfg.data().operators) ? cfg.data().operators : [];
  for (const x of named) { const id = String(x == null ? "" : x).trim().toLowerCase(); if (/^[a-z0-9_-]{1,40}$/.test(id) && !ids.includes(id)) ids.push(id); }
  const configured = ids.length > 0;
  if (!configured) ids = ["paul"];
  let list = [];
  for (const s of await Promise.all(ids.slice(0, 5).map(id => db.collection(C.OPERATORS).doc(id).get()))) { const d = s.exists ? s.data() : null; if (usable(d)) list.push(d); }
  if (!list.length && !configured) {
    // no account called `paul`: the active owner accounts whose username or display name is Paul (never anybody else's)
    const snap = await db.collection(C.OPERATORS).where("role", "==", "owner").limit(20).get();
    for (const s of snap.docs) { const d = s.data() || {}; if (usable(d) && (PAUL_KEYS.has(Admins.keyOf(s.id)) || PAUL_KEYS.has(Admins.keyOf(d.displayName)) || PAUL_KEYS.has(Admins.keyOf(d.username)))) list.push(d); }
  }
  if (list.length) operatorMemo.set(db, { at: now, list });
  return list;
}
const pbkdf2 = (pw, salt, iter, len, digest) => new Promise((res, rej) => crypto.pbkdf2(pw, salt, iter, len, digest, (e, k) => (e ? rej(e) : res(k))));
async function passwordMatches(password, o) {
  try {
    const iterations = Math.min(Math.max(parseInt(o.iterations, 10) || 200000, 1000), 1000000);
    const digest = ["sha512", "sha256", "sha1"].includes(o.digest) ? o.digest : "sha512";
    const stored = Buffer.from(String(o.passwordHash), "hex"), salt = Buffer.from(String(o.salt), "hex");
    if (!stored.length || !salt.length) return false;
    const got = await pbkdf2(password, salt, iterations, stored.length, digest);
    return got.length === stored.length && crypto.timingSafeEqual(got, stored);
  } catch (_) { return false; }
}
/** "ok" | "wrong" | "unavailable" (no account to check against). Throws when Firestore cannot be read. */
async function checkPassword(db, password) {
  const list = await operatorRecords(db);
  if (!list.length) return "unavailable";
  if (typeof password !== "string" || !password || password.length > 1024) return "wrong";
  let match = false;
  for (const o of list) if (await passwordMatches(password, o)) match = true;   // (every account is computed: no early exit to time)
  return match ? "ok" : "wrong";
}

/* ── lockout ── */
const srcHash = event => sha256("src:" + Admins.clientIp(event));
const lockKeys = event => [{ id: "src_" + srcHash(event).slice(0, 16), max: SRC_MAX }, { id: "all", max: ALL_MAX }];
/** { wait: seconds still locked (0 = free), states: { id → stored counter } } */
async function lockState(db, keys, now) {
  const snaps = await Promise.all(keys.map(k => db.collection(C.LOCKS).doc(k.id).get()));
  let until = 0; const states = {};
  snaps.forEach((s, i) => { const d = s.exists ? s.data() || {} : null; if (d) { states[keys[i].id] = d; until = Math.max(until, +d.lockedUntil || 0); } });
  return { wait: until > now ? Math.max(1, Math.ceil((until - now) / 1000)) : 0, states };
}
/** One wrong password counted against every key; returns the seconds the strictest lock now lasts (0 = not locked). */
async function noteWrong(db, keys, now) {
  let until = 0;
  for (const k of keys) {
    const u = await db.runTransaction(async tx => {
      const ref = db.collection(C.LOCKS).doc(k.id), s = await tx.get(ref);
      let d = s.exists ? s.data() || {} : null;
      if (!d || (+d.lockedUntil > 0 && d.lockedUntil <= now) || now - (+d.t0 || 0) > WINDOW_MS) d = { t0: now, n: 0, lockedUntil: 0 };
      d.n = (+d.n || 0) + 1;
      if (d.n >= k.max) d.lockedUntil = now + LOCK_MS;
      tx.set(ref, { t0: d.t0, n: d.n, lockedUntil: +d.lockedUntil || 0, at: now });
      return +d.lockedUntil || 0;
    });
    until = Math.max(until, u);
  }
  return until > now ? Math.ceil((until - now) / 1000) : 0;
}

/* ── records: every attempt, in two flat documents with the same id ── */
async function record(db, rec) {
  const ref = db.collection(C.ATTEMPTS).doc(), batch = db.batch();
  batch.set(ref, rec);
  batch.set(db.collection(C.ALERTS).doc(ref.id), Object.assign({}, rec, { closed: false, closedAt: null, closedBy: "" }));
  await batch.commit();
  return ref.id;
}
const tryRecord = async (db, rec) => { try { await record(db, rec); return true; } catch (e) { warn("attempt not recorded", e); return false; } };
async function prune(db, now) {
  try {
    const snap = await db.collection(C.TOKENS).where("exp", "<", now - 60 * MIN).limit(25).get();
    if (snap.empty) return;
    const batch = db.batch(); snap.docs.forEach(d => batch.delete(d.ref)); await batch.commit();
  } catch (e) { warn("token cleanup", e); }
}

/** repoUnlock. Always HTTP 200 for a recorded attempt; 503 when nothing could be checked or recorded. */
async function unlock(db, event, body) {
  const now = deps.now(), action = ACTIONS.has(body.action) ? body.action : "other";
  if (ONCE.has(action) && typeof body.sessionToken === "string" && body.sessionToken && !body.password) return unlockBySession(db, event, body, action);
  const rec = { at: now, by: str(body.by, 80), action, target: str(body.target, 80), ok: false, reason: "wrong", tab: str(body.tab, 24).replace(/[^\w-]/g, ""), ua: str(header(event, "user-agent"), 80), src: srcHash(event).slice(0, 8) };
  const keys = lockKeys(event);
  let ls;
  try { ls = await lockState(db, keys, now); } catch (e) { warn("lock state", e); rec.reason = "unavailable"; await tryRecord(db, rec); return json(503, { ok: false, error: "unavailable" }); }
  if (ls.wait > 0) { rec.reason = "locked"; await tryRecord(db, rec); return json(200, { ok: false, error: "locked", retryAfterSec: ls.wait }); }
  let verdict;
  try { verdict = await checkPassword(db, body.password); } catch (e) { warn("password check", e); verdict = "unavailable"; }
  if (verdict === "unavailable") { rec.reason = "unavailable"; await tryRecord(db, rec); return json(503, { ok: false, error: "unavailable" }); }
  if (verdict === "wrong") {
    let wait = 0; try { wait = await noteWrong(db, keys, now); } catch (e) { warn("lock count", e); }
    await tryRecord(db, rec);
    return json(200, wait > 0 ? { ok: false, error: "wrong", lockedNow: true, retryAfterSec: wait } : { ok: false, error: "wrong" });
  }
  rec.ok = true; rec.reason = "ok";
  try {
    await record(db, rec);
    const t = await issue(db, { action, by: rec.by, tab: rec.tab, session: body.session === true });
    const mine = ls.states[keys[0].id];
    if (mine && +mine.n > 0) db.collection(C.LOCKS).doc(keys[0].id).set({ t0: now, n: 0, lockedUntil: 0, at: now }).catch(e => warn("lock reset", e));
    await prune(db, now);
    return json(200, Object.assign({ ok: true, token: t.token, expiresAt: t.exp, once: t.once, action }, t.session ? { session: true } : {}));
  } catch (e) { warn("unlock", e); return json(503, { ok: false, error: "unavailable" }); }
}

/** The session behind a repoUnlock / repoLock / repoSession body: { id, rec } while it stands, else { why: "invalid" | "expired" | "revoked" }. Throws when Firestore cannot be read. */
async function readSession(db, token) {
  if (!wellFormed(token)) return { why: "invalid" };
  const id = sha256(token), s = await db.collection(C.TOKENS).doc(id).get();
  if (!s.exists) return { why: "invalid" };
  const rec = s.data() || {};
  if (rec.action !== "session") return { why: "invalid" };
  if (!(rec.exp > deps.now())) return { why: "expired" };
  const why = await sessionEnded(db, rec);
  return why ? { why } : { id, rec };
}
/** A single-use delete token for a person who holds a standing session: no password (the page still asks its confirm sentence). It is recorded in
    CharmRepo_AuthAttempts (reason "session": who deleted what, when) and raises no Alert; if that record cannot be written no token is given. */
async function unlockBySession(db, event, body, action) {
  const now = deps.now();
  let ses; try { ses = await readSession(db, body.sessionToken); } catch (e) { warn("session check", e); return json(503, { ok: false, error: "unavailable" }); }
  if (ses.why) return json(200, { ok: false, error: "session", why: ses.why });
  const rec = { at: now, by: str(body.by, 80), action, target: str(body.target, 80), ok: true, reason: "session", tab: str(body.tab, 24).replace(/[^\w-]/g, ""), ua: str(header(event, "user-agent"), 80), src: srcHash(event).slice(0, 8) };
  try {
    await db.collection(C.ATTEMPTS).doc().set(rec);
    const t = await issue(db, { action, by: rec.by, tab: rec.tab });
    return json(200, { ok: true, token: t.token, expiresAt: t.exp, once: t.once, action, viaSession: true });
  } catch (e) { warn("session unlock", e); return json(503, { ok: false, error: "unavailable" }); }
}
/** Lock now: the session is deleted here, so its token opens nothing from this moment (an instance that looked within the last seconds may still honour it that long). */
async function lockNow(db, body) {
  if (!wellFormed(body.sessionToken)) return json(200, { ok: true, closed: false });
  const id = sha256(body.sessionToken); tokenCache.delete(id);
  try {
    const ref = db.collection(C.TOKENS).doc(id), s = await ref.get();
    if (s.exists && (s.data() || {}).action === "session") { await ref.delete(); return json(200, { ok: true, closed: true }); }
    return json(200, { ok: true, closed: false });
  } catch (e) { warn("lock now", e); return json(503, { ok: false, error: "unavailable" }); }
}
/** Is this browser's session still good? (read only; the Settings menu asks once when it opens) */
async function sessionStatus(db, body) {
  try { const ses = await readSession(db, body.sessionToken); return json(200, ses.why ? { ok: true, active: false, why: ses.why } : { ok: true, active: true, expiresAt: ses.rec.exp }); }
  catch (e) { warn("session status", e); return json(503, { ok: false, error: "unavailable" }); }
}

/* ── the Alert: master admin only ── */
async function adminGate(db, by) {
  const name = str(by, 80).trim();
  if (!name) return json(403, { error: "admin-only" });
  let is = null; try { is = await Admins.isAdmin(db, name); } catch (e) { warn("admin list", e); }
  if (is === null) return json(503, { error: "unavailable" });
  return is ? null : json(403, { error: "admin-only" });
}
async function alerts(db, body) {
  const no = await adminGate(db, body.by); if (no) return no;
  try {
    const snap = await db.collection(C.ALERTS).where("closed", "==", false).limit(ALERT_READ).get();
    const rows = snap.docs.map(d => { const r = d.data() || {}; return { id: d.id, at: +r.at || 0, by: str(r.by, 80), action: str(r.action, 20), target: str(r.target, 80), ok: r.ok === true, reason: str(r.reason, 20) }; });
    rows.sort((a, b) => b.at - a.at);
    return json(200, { ok: true, alerts: rows.slice(0, ALERT_LIST), total: rows.length, more: rows.length > ALERT_LIST || snap.size >= ALERT_READ });
  } catch (e) { warn("alerts", e); return json(503, { error: "unavailable" }); }
}
async function alertClose(db, body) {
  const no = await adminGate(db, body.by); if (no) return no;
  try {
    let ids;
    if (body.all === true) ids = (await db.collection(C.ALERTS).where("closed", "==", false).limit(ALERT_READ).get()).docs.map(d => d.id);
    else ids = [...new Set((Array.isArray(body.ids) ? body.ids : []).map(x => String(x == null ? "" : x)).filter(x => /^[\w-]{4,60}$/.test(x)))].slice(0, ALERT_READ);
    if (!ids.length) return json(200, { ok: true, closed: 0 });
    const refs = ids.map(id => db.collection(C.ALERTS).doc(id)), snaps = await db.getAll(...refs), now = deps.now(), by = str(body.by, 80);
    const batch = db.batch(); let n = 0;
    snaps.forEach((s, i) => { if (s.exists && (s.data() || {}).closed !== true) { batch.update(refs[i], { closed: true, closedAt: now, closedBy: by }); n++; } });
    if (n) await batch.commit();
    return json(200, { ok: true, closed: n });
  } catch (e) { warn("alert close", e); return json(503, { error: "unavailable" }); }
}

/* ── download: a link to a charm file, behind a download token ── */
async function masterDownload(db, body) {
  let rel = "", name = "";
  const Master = require("./_charmNestMaster");
  if (body.sku) {
    const sku = String(body.sku).trim().toUpperCase(); if (!Master.isSku(sku)) return json(400, { error: "bad sku" });
    const s = await db.collection(C.INDEX).doc(sku).get(); if (!s.exists) return json(404, { error: "not indexed: " + sku });
    const e = s.data() || {}, size = body.size ? String(body.size).toUpperCase() : "", sizes = Object.keys(e.sizes || {});
    if (!size && !e.aiPath && sizes.length) return json(400, { error: "size required", sizes });
    const g = size ? (e.sizes || {})[size] : (e.aiPath ? e : null);
    if (!g || !g.aiPath) return json(404, { error: "no file for " + sku + (size ? " " + size : "") });
    rel = g.aiPath; name = sku + (size ? "-" + size : "") + ".ai";
  } else if (body.masterHash) {
    const hash = str(body.masterHash, 80); if (!/^[0-9a-f]{8,64}$/i.test(hash)) return json(400, { error: "bad master hash" });
    const s = await db.collection(C.FILES).doc(hash).get(); if (!s.exists || !(s.data() || {}).path) return json(404, { error: "no stored file for that master" });
    rel = s.data().path; name = str(s.data().name, 120) || hash;
  } else if (body.path) { rel = String(body.path); name = rel.split("/").pop(); }
  else return json(400, { error: "sku, masterHash or path required" });
  const p = safePath(rel);
  if (!/^charmnest\/(?:sandbox\/)?(?:master|charms)\//.test(p)) return json(400, { error: "not a charm file" });
  const admin = require("./firebaseAdmin"), bucket = admin.storage().bucket(), file = bucket.file(p);
  const [exists] = await file.exists(); if (!exists) return json(404, { error: "not found" });
  const [meta] = await file.getMetadata();
  let token = meta.metadata && meta.metadata.firebaseStorageDownloadTokens;
  if (!token) { token = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex"); await file.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } }); }
  const url = "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucket.name) + "/o/" + encodeURIComponent(p) + "?alt=media&token=" + encodeURIComponent(String(token).split(",")[0]);
  return json(200, { ok: true, url, path: p, name, bytes: Number(meta.size) || 0 });
}

/* ── the ops this file answers itself (kept out of charmNestLibrary's OPS list on purpose) ── */
const OWN = new Set(["repoUnlock", "repoAlerts", "repoAlertClose", "masterDownload", "repoLock", "repoSession"]);
const owns = op => OWN.has(op);
async function handle(db, event, body) {
  if (event.httpMethod !== "POST") return json(405, { error: "post-only" });   // (a password, a token or a close never travels in a URL)
  switch (body.op) {
    case "repoUnlock": return unlock(db, event, body);
    case "repoAlerts": return alerts(db, body);
    case "repoAlertClose": return alertClose(db, body);
    case "repoLock": return lockNow(db, body);
    case "repoSession": return sessionStatus(db, body);
    case "masterDownload": {
      const no = await authorize(db, { op: "masterDownload", need: "download" }, body, event); if (no) return json(no.status, no.body);
      try { return await masterDownload(db, body); } catch (e) { warn("download", e); return json(500, { error: "download failed" }); }
    }
    default: return json(400, { error: "unknown op" });
  }
}

module.exports = { C, ACTIONS, TTL_MS, ONCE, OPENS, LIBRARY, SETTINGS_OPS, sessionEnd, WINDOW_MS, LOCK_MS, SRC_MAX, ALL_MAX, deps, requirement, authorize, unlock, alerts, alertClose, masterDownload, owns, handle, issue, forget: () => { tokenCache.clear(); } };
