/*  AI replay ("simulation") sandbox for the inbox's AI.
 *
 *  Inside simulate(...) the real drafter, sales agent and classifier run
 *  against real Firestore data, but:
 *    - every Firestore write is captured instead of applied,
 *    - message reads of the thread stop at asOfMs (the conversation as it
 *      stood when the customer wrote),
 *    - outbound HTTP is blocked except to Anthropic and Google (auth,
 *      Firestore, Storage) and Etsy's image CDN: no Etsy API calls, no
 *      calls to our own functions, no mail.
 *  The scope is an AsyncLocalStorage context, so a live invocation that
 *  shares the warm container is never affected.
 */
const { AsyncLocalStorage } = require("async_hooks");
const http  = require("http");
const https = require("https");
const admin = require("./firebaseAdmin");

const als = new AsyncLocalStorage();
const current = () => als.getStore() || null;

const ALLOWED_HOST_RX = /(^|\.)(anthropic\.com|etsystatic\.com)$|^(oauth2|www|storage|firestore|firebasestorage|iamcredentials|securetoken|sts)\.googleapis\.com$/i;

function toMs(v) {
  if (!v) return null;
  if (typeof v === "number") return v;
  if (typeof v.toMillis === "function") return v.toMillis();
  if (v._seconds != null) return v._seconds * 1000;
  const n = Date.parse(v);
  return Number.isFinite(n) ? n : null;
}

function plain(v, depth = 0) {
  if (v == null || depth > 6) return v == null ? v : "[deep]";
  if (typeof v !== "object") return v;
  if (typeof v.toMillis === "function") return { ms: v.toMillis() };
  if (v.constructor && /FieldValue|Transform/.test(v.constructor.name)) return `[${v.constructor.name}]`;
  if (Array.isArray(v)) return v.slice(0, 50).map(x => plain(x, depth + 1));
  const out = {};
  for (const k of Object.keys(v)) out[k] = plain(v[k], depth + 1);
  return out;
}

function hostOf(arg) {
  try {
    if (typeof arg === "string") return new URL(arg).hostname;
    if (arg instanceof URL) return arg.hostname;
    if (arg && (arg.hostname || arg.host)) return String(arg.hostname || arg.host).split(":")[0];
  } catch (_) {}
  return "";
}

let installed = false;
function install() {
  if (installed) return;
  installed = true;
  const fs = admin.firestore;

  const capture = (op, path, data) => {
    const s = current();
    s.writes.push({ op, path, data: plain(data) });
  };

  const DR = fs.DocumentReference.prototype;
  for (const op of ["set", "update", "create", "delete"]) {
    const orig = DR[op];
    DR[op] = function (...args) {
      if (!current()) return orig.apply(this, args);
      capture(op, this.path, args[0]);
      return Promise.resolve({ writeTime: fs.Timestamp.now() });
    };
  }
  const CR = fs.CollectionReference.prototype;
  const origAdd = CR.add;
  CR.add = function (data) {
    if (!current()) return origAdd.apply(this, arguments);
    const ref = this.doc();
    capture("add", ref.path, data);
    return Promise.resolve(ref);
  };
  const WB = fs.WriteBatch.prototype;
  for (const op of ["set", "update", "create", "delete"]) {
    const orig = WB[op];
    WB[op] = function (ref, data) {
      if (!current()) return orig.apply(this, arguments);
      capture("batch." + op, ref && ref.path, data);
      return this;
    };
  }
  const origCommit = WB.commit;
  WB.commit = function () {
    if (!current()) return origCommit.apply(this, arguments);
    return Promise.resolve([]);
  };
  const TX = fs.Transaction.prototype;
  for (const op of ["set", "update", "create", "delete"]) {
    const orig = TX[op];
    TX[op] = function (ref, data) {
      if (!current()) return orig.apply(this, arguments);
      capture("tx." + op, ref && ref.path, data);
      return this;
    };
  }

  // The conversation as it stood at asOfMs.
  const Q = fs.Query.prototype;
  const origGet = Q.get;
  Q.get = async function () {
    const snap = await origGet.apply(this, arguments);
    const s = current();
    if (!s || !s.asOfMs) return snap;
    const docs = snap.docs.filter(d => {
      if (!/^EtsyMail_Threads\/[^/]+\/messages\//.test(d.ref.path)) return true;
      const data = d.data() || {};
      const ms = toMs(data.timestamp) || toMs(data.createdAt);
      return !ms || ms <= s.asOfMs;
    });
    if (docs.length === snap.docs.length) return snap;
    return { docs, empty: docs.length === 0, size: docs.length, forEach: fn => docs.forEach(fn),
             docChanges: () => [], query: snap.query, readTime: snap.readTime };
  };

  const guard = (mod, name) => {
    const orig = mod[name];
    mod[name] = function (...args) {
      const s = current();
      if (s) {
        const h = hostOf(args[0]);
        if (!ALLOWED_HOST_RX.test(h)) {
          s.blocked.push(h || "(unknown)");
          throw new Error(`SIMULATION: outbound call to ${h || "unknown host"} blocked`);
        }
      }
      return orig.apply(this, args);
    };
  };
  guard(https, "request"); guard(https, "get"); guard(http, "request"); guard(http, "get");
  if (typeof globalThis.fetch === "function") {
    const origFetch = globalThis.fetch;
    globalThis.fetch = function (input, init) {
      const s = current();
      if (s) {
        const h = hostOf(typeof input === "string" || input instanceof URL ? input : input && input.url);
        if (!ALLOWED_HOST_RX.test(h)) {
          s.blocked.push(h || "(unknown)");
          return Promise.reject(new Error(`SIMULATION: outbound call to ${h || "unknown host"} blocked`));
        }
      }
      return origFetch.apply(this, arguments);
    };
  }
}

/** Run fn inside a replay context. Returns { result, writes, blocked }. */
async function simulate({ asOfMs = null, nowMs = null }, fn) {
  install();
  const store = { asOfMs, nowMs, startedAt: Date.now(), writes: [], blocked: [] };
  const result = await als.run(store, fn);
  return { result, writes: store.writes, blocked: [...new Set(store.blocked)] };
}

/** "Now" for prompts: the replayed moment inside a replay, else the clock. */
function simNow() {
  const s = current();
  if (s && s.nowMs) return new Date(s.nowMs + (Date.now() - s.startedAt));
  return new Date();
}

module.exports = { simulate, simNow, isSimulating: () => !!current(), toMs };
