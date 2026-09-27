/*  AI replay ("simulation") sandbox for the inbox's AI.
 *
 *  Inside simulate(...) the real drafter, sales agent and classifier run
 *  against real Firestore data, but:
 *    - every Firestore write is captured instead of applied,
 *    - message reads of the thread stop at asOfMs (the conversation as it
 *      stood when the customer wrote), and receipts-mirror orders show
 *      their state at asOfMs (no later orders, shipments or completions),
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

/** A receipts-mirror document as it stood at asOfMs: null if the order
 *  did not exist yet; shipments, "shipped", "completed" and "canceled"
 *  states that came later are taken back to "paid". */
function receiptAsOf(data, asOfMs) {
  if (!data) return { data, changed: false };
  const raw = (data.raw && typeof data.raw === "object") ? data.raw : null;
  const created = (Number(data.created_timestamp || (raw && raw.created_timestamp)) || 0) * 1000;
  if (created && created > asOfMs) return null;
  const out = { ...data };
  let changed = false;
  if (raw) {
    const r = { ...raw };
    const ships = Array.isArray(raw.shipments) ? raw.shipments : [];
    const kept = ships.filter(x => !x || !x.shipment_notification_timestamp || x.shipment_notification_timestamp * 1000 <= asOfMs);
    if (kept.length !== ships.length) { r.shipments = kept; changed = true; }
    const updated = (Number(raw.updated_timestamp || raw.update_timestamp) || 0) * 1000;
    const laterState = updated && updated > asOfMs;
    if ((kept.length === 0 && raw.is_shipped) || (laterState && /completed|canceled|cancelled|fully refunded/i.test(String(raw.status || "")))) {
      r.is_shipped = kept.length > 0;
      if (!r.is_shipped) { r.status = "Paid"; r.refunds = []; }
      changed = true;
    }
    if (changed) {
      out.raw = r;
      out.is_shipped = r.is_shipped;
      out.status = r.status;
    }
  }
  return { data: out, changed };
}

// Listing ids a thread's messages mention after the cut: a custom listing
// made later must not show up in the replayed moment (thread fields, sales
// context, listings mirror). Message times can be import times, so the
// listing id, not a timestamp, decides what is "later".
const LISTING_ID_RX = /listing\/(\d{8,12})/g;
async function futureListingIds(s, threadRef, origGet) {
  s.future = s.future || {};
  const key = threadRef.path;
  if (s.future[key]) return s.future[key];
  const ids = new Set();
  try {
    const ts = admin.firestore.Timestamp.fromMillis(s.asOfMs);
    const snap = await origGet.call(threadRef.collection("messages").where("timestamp", ">", ts).limit(400));
    for (const d of snap.docs) {
      const m = d.data() || {};
      const txt = String(m.text || "") + " " + JSON.stringify(m.listingCards || []);
      for (const x of txt.matchAll(LISTING_ID_RX)) ids.add(x[1]);
    }
  } catch (_) {}
  s.future[key] = ids;
  s.futureAll = new Set([...(s.futureAll || []), ...ids]);
  return ids;
}

/** A thread doc as it stood at asOfMs: a custom listing, acceptance or
 *  completed sale that came later is taken off. */
function threadAsOf(data, asOfMs, future) {
  if (!data) return { data, changed: false };
  const out = { ...data };
  let changed = false;
  const later = (...keys) => keys.some(k => { const ms = toMs(data[k]); return ms && ms > asOfMs; });
  const drop = rx => { for (const k of Object.keys(out)) if (rx.test(k)) { delete out[k]; changed = true; } };
  const listingLater = (data.customListingId && future.has(String(data.customListingId)))
    || later("customListingStartedAt", "customListingDraftCreatedAt", "customListingCreatedAt", "customListingSentAt");
  if (listingLater) drop(/^customListing/);
  if (listingLater || later("customerAcceptedAt")) drop(/^(customerAccepted|acceptedQuote)/);
  if (listingLater || later("salesCompletedAt")) {
    drop(/^(salesCompletedAt|salesSynopsis)$/);
    if (out.status === "sales_completed") { out.status = "open"; changed = true; }
  }
  return { data: out, changed };
}

/** A sales context as it stood at asOfMs: no later completion, abandonment
 *  or quotes; a context created later does not exist yet. */
function salesCtxAsOf(data, asOfMs, future) {
  if (!data) return { data, changed: false };
  if ((toMs(data.createdAt) || 0) > asOfMs) return null;
  const out = { ...data };
  let changed = false;
  const doneLater = (data.completedListingId && future.has(String(data.completedListingId)))
    || (toMs(data.stageCompletedAt) || 0) > asOfMs || (toMs(data.abandonedAt) || 0) > asOfMs;
  const quotes = Array.isArray(data.quoteHistory) ? data.quoteHistory : [];
  const keptQuotes = quotes.filter(q => !q || !q.at || q.at <= asOfMs);
  if (keptQuotes.length !== quotes.length) { out.quoteHistory = keptQuotes; changed = true; }
  if (doneLater || (toMs(data.lastTurnAt) || 0) > asOfMs) {
    for (const k of ["completedListingId", "completionReason", "stageCompletedAt", "abandonedAt", "lastCustomerFacingReply"]) {
      if (k in out) { delete out[k]; changed = true; }
    }
    if (!/^(discovery|spec|quote|revision|pending_close_approval)$/.test(String(out.stage || ""))) {
      out.stage = keptQuotes.length ? "quote" : "spec";
      changed = true;
    }
    if (!keptQuotes.length && ("_lastResolverResult" in out || "totalQuotedUsd" in out)) {
      delete out._lastResolverResult; delete out.totalQuotedUsd; changed = true;
    }
  }
  return { data: out, changed };
}

function wrapSnap(snap, data) {
  return new Proxy(snap, {
    get(t, p) {
      if (p === "data") return () => data;
      if (p === "exists") return data !== undefined && t.exists;
      if (p === "get") return (f) => String(f).split(".").reduce((o, k) => (o == null ? o : o[k]), data);
      const v = t[p];
      return typeof v === "function" ? v.bind(t) : v;
    }
  });
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
    if (!s || (!s.asOfMs && !(s.extra && s.extra.length))) return snap;
    let changed = false;
    const docs = [];
    for (const d of snap.docs) {
      if (/^EtsyMail_Threads\/[^/]+\/messages\//.test(d.ref.path)) {
        const data = d.data() || {};
        const ms = toMs(data.timestamp) || toMs(data.createdAt);
        if (ms && ms > s.asOfMs) { changed = true; continue; }
        docs.push(d);
      } else if (/^EtsyMail_Receipts\//.test(d.ref.path)) {
        const v = receiptAsOf(d.data(), s.asOfMs);
        if (v === null) { changed = true; continue; }
        if (v.changed) { changed = true; docs.push(wrapSnap(d, v.data)); } else docs.push(d);
      } else docs.push(d);
    }
    // Test-only turns added after the cut (e.g. our offer and the
    // customer's "yes"), newest first like the thread queries ask.
    const first = snap.docs[0] && snap.docs[0].ref.path;
    if (s.extra && s.extra.length && first && s.extraPath && first.startsWith(s.extraPath)) {
      changed = true;
      for (const m of s.extra.slice().reverse()) {
        const data = Object.assign({}, m.data, { timestamp: fs.Timestamp.fromMillis(m.ms), createdAt: fs.Timestamp.fromMillis(m.ms) });
        docs.unshift({ id: m.id, exists: true, ref: { path: s.extraPath + m.id, id: m.id }, data: () => data, get: k => data[k] });
      }
    }
    if (!changed) return snap;
    return { docs, empty: docs.length === 0, size: docs.length, forEach: fn => docs.forEach(fn),
             docChanges: () => [], query: snap.query, readTime: snap.readTime };
  };

  const origDocGet = DR.get;
  // One document as it stood at asOfMs (receipts, thread, sales context,
  // a custom listing made later).
  const docAsOf = async (s, ref, snap) => {
    if (!s || !s.asOfMs || !snap || !snap.exists) return snap;
    const path = ref.path;
    if (/^EtsyMail_Receipts\//.test(path)) {
      const v = receiptAsOf(snap.data(), s.asOfMs);
      if (v === null) return wrapSnap(snap, undefined);
      return v.changed ? wrapSnap(snap, v.data) : snap;
    }
    if (/^EtsyMail_Threads\/[^/]+$/.test(path)) {
      const future = await futureListingIds(s, ref, origGet);
      const v = threadAsOf(snap.data(), s.asOfMs, future);
      return v.changed ? wrapSnap(snap, v.data) : snap;
    }
    if (/^EtsyMail_SalesContext\/[^/]+$/.test(path)) {
      const future = await futureListingIds(s, admin.firestore().collection("EtsyMail_Threads").doc(ref.id), origGet);
      const v = salesCtxAsOf(snap.data(), s.asOfMs, future);
      if (v === null) return wrapSnap(snap, undefined);
      return v.changed ? wrapSnap(snap, v.data) : snap;
    }
    if (/^EtsyMail_Listings\//.test(path) && s.futureAll && s.futureAll.has(ref.id)) return wrapSnap(snap, undefined);
    return snap;
  };
  DR.get = async function () {
    const snap = await origDocGet.apply(this, arguments);
    return docAsOf(current(), this, snap);
  };
  const FS = fs.Firestore && fs.Firestore.prototype;
  const origGetAll = FS && FS.getAll;
  if (FS && origGetAll) FS.getAll = async function (...refs) {
    const snaps = await origGetAll.apply(this, refs);
    const s = current();
    if (!s || !s.asOfMs) return snaps;
    return Promise.all(snaps.map(sn => docAsOf(s, sn.ref, sn)));
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
        const u = typeof input === "string" || input instanceof URL ? input : input && input.url;
        const h = hostOf(u);
        if (!ALLOWED_HOST_RX.test(h)) {
          s.blocked.push(h || "(unknown)");
          try { (s.blockedPaths = s.blockedPaths || []).push(h + new URL(String(u)).pathname); } catch (_) {}
          return Promise.reject(new Error(`SIMULATION: outbound call to ${h || "unknown host"} blocked`));
        }
      }
      return origFetch.apply(this, arguments);
    };
  }
}

/** Run fn inside a replay context. Returns { result, writes, blocked }. */
async function simulate({ asOfMs = null, nowMs = null, threadId = null, extraMessages = null }, fn) {
  install();
  const store = { asOfMs, nowMs, startedAt: Date.now(), writes: [], blocked: [] };
  if (threadId && Array.isArray(extraMessages) && extraMessages.length) {
    const base = asOfMs || Date.now();
    store.extraPath = `EtsyMail_Threads/${threadId}/messages/`;
    store.extra = extraMessages.slice(0, 4).map((m, i) => {
      const inbound = m.direction !== "outbound";
      return { id: "simextra_" + i, ms: base + (i + 1) * 60000, data: {
        direction: inbound ? "inbound" : "outbound",
        senderRole: inbound ? "customer" : "staff",
        senderName: inbound ? (m.senderName || "Customer") : "CustomBrites",
        source: "etsy", text: String(m.text || "").slice(0, 2000) } };
    });
  }
  const result = await als.run(store, fn);
  return { result, writes: store.writes, blocked: [...new Set(store.blocked)],
           blockedPaths: [...new Set(store.blockedPaths || [])] };
}

/** "Now" for prompts: the replayed moment inside a replay, else the clock. */
function simNow() {
  const s = current();
  if (s && s.nowMs) return new Date(s.nowMs + (Date.now() - s.startedAt));
  return new Date();
}

module.exports = { simulate, simNow, isSimulating: () => !!current(), toMs };
