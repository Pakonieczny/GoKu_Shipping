/*  netlify/functions/_etsyMailOrderLink.js
 *
 *  The Charm Sorter's line to a customer, carried by the inbox.
 *
 *  A production question about an order, or about one line of it for engraving, is an
 *  "engagement" (EtsyMail_OrderLinks/{id}). It holds what the sorter sent (its outbox) and a
 *  window on the order's Etsy conversation: the customer's messages from the first question on
 *  are the answers to it, so the sorter shows that stretch of the conversation and none of the
 *  customer's older history.
 *
 *  Sending uses the inbox's own path. The conversation's one draft slot
 *  (EtsyMail_Drafts/draft_<threadId>) is queued for the Chrome extension exactly as a click on
 *  "Send via Etsy" queues it. Whatever the inbox had in that slot (an AI reply awaiting review, a
 *  half-written reply, the record of its last send) is parked on the draft by etsyMailDraftSend
 *  and put back exactly as it was once the sorter's message has gone, so nobody in the inbox
 *  loses anything.
 *
 *  Receiving uses the inbox's own scrape: etsyMailSnapshot hands every new message to
 *  onThreadMessages, so a reply reaches the sorter in the same moment it reaches the inbox. The
 *  sorter learns of it through one small "bell" document it polls (EtsyMail_OrderLinkMeta/bell),
 *  which costs one Firestore read per poll and never touches Etsy.
 *
 *  Every entry point the inbox calls (onThreadMessages, onDraftSettled, reconcile) is
 *  best-effort: a failure here is logged and swallowed, and never fails a scrape, a send or a
 *  reaper pass.
 */
"use strict";

const crypto = require("crypto");
const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;
const TS = admin.firestore.Timestamp;

const COLL = {
  eng      : "EtsyMail_OrderLinks",
  stations : "EtsyMail_OrderLinkStations",
  pairs    : "EtsyMail_OrderLinkPairs",
  meta     : "EtsyMail_OrderLinkMeta",
  buyers   : "EtsyMail_OrderLinkBuyers",
  trans    : "EtsyMail_Translations",
  threads  : "EtsyMail_Threads",
  drafts   : "EtsyMail_Drafts",
  receipts : "EtsyMail_Receipts",
  operators: "EtsyMail_Operators"
};

const MIN = 60 * 1000, HOUR = 60 * MIN, DAY = 24 * HOUR;
const WINDOW_SLACK_MS = 2 * MIN;      // messages this much before the first question still belong to it (clock skew)
const DELTA_OVERLAP_MS = 30 * 1000;   // a sorter's catch-up read reaches this far behind what it has seen (commits land out of order)
const TEXT_MAX = 4000;
const OUTBOX_MAX = 80;
const SEEN_MAX = 150;
const PAIR_TTL_MS = 10 * MIN;
const SERVER_RETRIES = 3;             // a send the inbox could not take for a server fault is tried this many times
const AUTO_RESOLVE_IDLE_MS = 21 * DAY;
const KEEP_RESOLVED_MS = 180 * DAY;
const KEEP_TRANSLATION_MS = 60 * DAY;
const INBOX_URL = "https://etsy-mail-1.goldenspike.app/";

const UNSENT = new Set(["new", "waiting"]);
const IN_FLIGHT = new Set(["queued", "sending"]);
const TERMINAL_DRAFT = new Set(["sent", "sent_text_only", "sent_unverified", "failed"]);

/* The draft fields a sorter send overwrites. etsyMailDraftSend parks their values on the draft
   (parkedCopy) when it queues a sorter message, and restoreInboxDraft writes them back once that
   message is done, deleting the ones that were not there before. */
const PARK_KEYS = [
  "text", "attachments", "status", "createdBy", "sendOrigin", "generatedByAI", "aiModel",
  "aiReasoning", "aiActiveQuestion", "queuedAt", "sentAt", "sendSessionId", "sendClaimedAt",
  "sendHeartbeatAt", "sendAttempts", "sendError", "sendErrorCode", "sendPartialSuccess", "sendStage",
  "sendUnverified", "sendImagesSent", "sendImagesTotal", "sendListingsSent", "sendListingsTotal",
  "sendTextSent", "sendNote", "etsyMessageId", "sendWorkerId", "sendProgress"
];
// what a sorter send leaves on a slot that had no draft before, besides PARK_KEYS
const SLOT_FRAME = new Set(["draftId", "threadId", "etsyConversationUrl", "createdAt", "updatedAt", "orderLink", "orderLinkParked"]);

// ─── small helpers ─────────────────────────────────────────────────────────

const sha = s => crypto.createHash("sha256").update(String(s)).digest("hex");
let _lastStamp = 0;
/** A change stamp: milliseconds, strictly increasing within this instance. */
function stamp() { const t = Date.now(); _lastStamp = t > _lastStamp ? t : _lastStamp + 1; return _lastStamp; }
const cleanId = v => { const s = String(v == null ? "" : v).trim(); return /^[A-Za-z0-9_-]{1,160}$/.test(s) ? s : ""; };
const isThreadId = v => /^etsy_conv_\d+$/.test(String(v || ""));
function cleanText(t, max = TEXT_MAX) {
  return String(t == null ? "" : t).replace(/\r\n?/g, "\n").replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "").trim().slice(0, max);
}
function tsMs(v) {
  if (!v) return 0;
  if (typeof v === "number") return v;
  if (typeof v.toMillis === "function") return v.toMillis();
  if (v._seconds != null) return v._seconds * 1000 + Math.round((v._nanoseconds || 0) / 1e6);
  const n = Date.parse(v); return Number.isFinite(n) ? n : 0;
}
/** Text reduced for matching a sent message against its scraped copy on Etsy. */
function normText(s) {
  return String(s || "").normalize("NFKC").toLowerCase().replace(/[‘’“”'"`]/g, "")
    .replace(/[^\p{L}\p{N}]+/gu, " ").trim();
}
function sameText(a, b) {
  const x = normText(a), y = normText(b);
  if (!x || !y) return false;
  if (x === y) return true;
  // Etsy trims, re-wraps and sometimes shortens a long message's preview; a shared long start is the same message
  const n = Math.min(160, x.length, y.length);
  return n >= 40 && x.slice(0, n) === y.slice(0, n);
}
const firstLine = t => (cleanText(t, 400).split("\n").map(s => s.trim()).find(Boolean) || "").slice(0, 90);
const preview = t => cleanText(t, 400).replace(/\s+/g, " ").slice(0, 160);
function httpError(status, message, code) { const e = new Error(message); e.status = status; if (code) e.code = code; return e; }
function header(event, name) {
  const h = (event && event.headers) || {};
  const want = name.toLowerCase();
  for (const k of Object.keys(h)) if (k.toLowerCase() === want) return h[k];
  return "";
}
const DELETE = FV.delete();
function isDelete(v) {
  if (v === DELETE) return true;
  try { return !!(v && typeof v === "object" && typeof v.isEqual === "function" && v.isEqual(DELETE)); } catch (_) { return false; }
}
function applyPatch(obj, patch) {
  const out = Object.assign({}, obj);
  for (const [k, v] of Object.entries(patch)) { if (isDelete(v)) delete out[k]; else out[k] = v; }
  return out;
}
const has = (o, k) => Object.prototype.hasOwnProperty.call(o, k);

// ─── the bell: one document the sorter polls ─────────────────────────────

const bellRef = () => db.collection(COLL.meta).doc("bell");
/* A change rings the bell once per request, however many documents it touched: the bell is one
   document and Firestore wants no more than about one write a second on it. ring() marks that
   something a sorter shows has changed, note() adds bookkeeping no sorter needs to wake for, and
   flush() writes both at the end of the request. The counter n only ever grows (an increment on
   the server), so a sorter never misses a change to clocks that disagree. */
let _bell = null, _rung = false;
function ring() { _rung = true; }
function note(fields) {
  _bell = _bell || {};
  for (const [k, v] of Object.entries(fields)) {
    const cur = _bell[k];
    if (v && typeof v === "object" && !isDelete(v) && cur && typeof cur === "object" && !isDelete(cur)) _bell[k] = Object.assign({}, cur, v);
    else _bell[k] = v;
  }
}
async function flush() {
  if (!_bell && !_rung) return;
  const patch = _bell || {};
  if (_rung) Object.assign(patch, { n: FV.increment(1), atMs: Date.now() });
  _bell = null; _rung = false;
  try { await bellRef().set(patch, { merge: true }); }
  catch (e) { console.warn("orderLink bell:", e.message); }
}
function withFlush(fn) {
  return async (...args) => { try { return await fn(...args); } finally { await flush(); } };
}
const flightKey = (e, i) => `${e}~${i}`;
function addInflight(engId, itemId, threadId, draftId) {
  note({ inflight: { [flightKey(engId, itemId)]: { e: engId, i: itemId, t: threadId, d: draftId, at: Date.now() } }, waiting: { [threadId]: FV.delete() } });
}
function dropInflight(engId, itemId) { note({ inflight: { [flightKey(engId, itemId)]: FV.delete() } }); }
function markWaiting(threadId) { note({ waiting: { [threadId]: Date.now() } }); }

// ─── engagements ───────────────────────────────────────────────────────────

const engRef = id => db.collection(COLL.eng).doc(id);

/** Read-modify-write one engagement. fn(current) returns a patch, or null to leave it alone.
 *  Resolves to { e: the engagement afterwards (null when it does not exist), changed }. */
async function change(id, fn) {
  let e = null, changed = false;
  if (!id) return { e, changed };
  await db.runTransaction(async tx => {
    e = null; changed = false;
    const snap = await tx.get(engRef(id));
    if (!snap.exists) return;
    const cur = snap.data();
    const patch = fn(cur);
    if (!patch) { e = cur; return; }
    patch.v = stamp(); patch.updatedAtMs = Date.now();
    tx.set(engRef(id), patch, { merge: true });
    e = applyPatch(cur, patch); changed = true;
  });
  if (changed) ring();
  return { e, changed };
}
const mutate = async (id, fn) => (await change(id, fn)).e;

/** Change one outbox message, only while it is in one of the onlyFrom states. */
function patchItem(id, itemId, patch, onlyFrom) {
  return change(id, cur => {
    const outbox = (cur.outbox || []).slice();
    const at = outbox.findIndex(x => x.id === itemId);
    if (at < 0) return null;
    if (onlyFrom && !onlyFrom.has(outbox[at].status)) return null;
    const next = Object.assign({}, outbox[at], patch);
    for (const k of Object.keys(next)) if (next[k] === undefined || next[k] === null) delete next[k];
    outbox[at] = next;
    return { outbox };
  });
}

/** The summary a sorter keeps for badges and lists: no message bodies beyond one preview. */
function summary(e) {
  const out = e.outbox || [];
  const last = out[out.length - 1] || null;
  return {
    id: e.id, receiptId: e.receiptId, lineId: e.lineId || null, scope: e.scope || "order", lineLabel: e.lineLabel || "",
    orderNumber: e.orderNumber || "", status: e.status, sandbox: !!e.sandbox,
    link: e.link || (e.threadId ? "thread" : "waiting"), linkedBy: e.linkedBy || null, threadId: e.threadId || null,
    customer: e.customer || null, title: e.title || "", lang: e.lang || null, createdBy: e.createdBy || "",
    createdAtMs: e.createdAtMs || 0, startedAtMs: e.startedAtMs || 0, updatedAtMs: e.updatedAtMs || 0, v: e.v || 0,
    resolvedAtMs: e.resolvedAtMs || 0, resolvedBy: e.resolvedBy || "",
    unread: e.unread || 0, inboundCount: e.inboundCount || 0,
    lastInboundAtMs: e.lastInboundAtMs || 0, lastInboundPreview: e.lastInboundPreview || "", lastInboundBy: e.lastInboundBy || "",
    lastOutboundAtMs: e.lastOutboundAtMs || 0, lastShopReplyAtMs: e.lastShopReplyAtMs || 0,
    pending: out.filter(x => UNSENT.has(x.status) || IN_FLIGHT.has(x.status)).length,
    failed: out.filter(x => x.status === "failed").length,
    manual: out.filter(x => x.status === "manual").length,
    lastOut: last ? { id: last.id, status: last.status, atMs: last.atMs || 0, sentAtMs: last.sentAtMs || 0 } : null
  };
}

async function engagementsForReceipt(receiptId, sandbox) {
  const q = await db.collection(COLL.eng).where("receiptId", "==", String(receiptId)).limit(60).get();
  return q.docs.map(d => d.data()).filter(e => !!e.sandbox === !!sandbox)
    .sort((a, b) => (b.createdAtMs || 0) - (a.createdAtMs || 0));
}
const scopeKey = (scope, lineId) => scope === "engraving" ? "l" + (lineId || "") : "o";

async function createEngagement(station, o) {
  const conv = o.sandbox ? { thread: null, buyerUserId: null, by: null } : await findConversation(o.receiptId);
  const now = Date.now();
  const t = conv.thread;
  const id = (o.sandbox ? "olsb_" : "ol_") + o.receiptId + "_" + scopeKey(o.scope, o.lineId) + "_" + now.toString(36) + crypto.randomBytes(2).toString("hex");
  const e = {
    id, receiptId: o.receiptId, scope: o.scope, lineId: o.lineId || null, lineLabel: o.lineLabel || "", orderNumber: o.orderNumber || "",
    sandbox: !!o.sandbox, status: "open", createdAtMs: now, createdBy: station.name, startedAtMs: now, updatedAtMs: now, v: stamp(),
    threadId: t ? t.id : null, conversationUrl: t ? (t.etsyConversationUrl || null) : null,
    link: o.sandbox ? "sandbox" : t ? "thread" : "waiting", linkedBy: t ? conv.by : null,
    customer: {
      name: (t && t.customerName) || o.buyerName || "",
      username: (t && t.etsyUsername) || "",
      buyerUserId: (t && t.buyerUserId) || conv.buyerUserId || null
    },
    title: firstLine(o.text), outbox: [], seen: [], sim: [], unread: 0, inboundCount: 0,
    lastInboundAtMs: 0, lastInboundPreview: "", lastOutboundAtMs: 0, lastShopReplyAtMs: 0
  };
  await engRef(id).create(e);
  ring();
  if (e.threadId) await syncThreadFlags(e.threadId);
  else if (!e.sandbox) await addWaiting(e);
  return e;
}

// ─── finding the order's Etsy conversation ────────────────────────────────

function latestThread(list) {
  const t = x => Math.max(tsMs(x.lastInboundAt), tsMs(x.lastOutboundAt), tsMs(x.lastOperatorReplyAt), tsMs(x.updatedAt));
  return list.filter(x => isThreadId(x.id)).sort((a, b) => t(b) - t(a))[0] || null;
}
const _convCache = new Map();
/** The order's conversation: the one whose Etsy heading names this order, else the buyer's latest. */
async function findConversation(receiptId, { fresh = false } = {}) {
  const key = String(receiptId);
  const hit = _convCache.get(key);
  if (!fresh && hit && Date.now() - hit.at < MIN) return hit.value;
  let value = { thread: null, buyerUserId: null, by: null };
  const byOrder = await db.collection(COLL.threads).where("etsyOrderId", "==", key).limit(10).get();
  const t1 = latestThread(byOrder.docs.map(d => Object.assign({ id: d.id }, d.data())));
  if (t1) value = { thread: t1, buyerUserId: t1.buyerUserId || null, by: "order" };
  else {
    const buyer = await buyerOf(key);
    if (buyer) {
      const byBuyer = await db.collection(COLL.threads).where("buyerUserId", "==", String(buyer)).limit(25).get();
      const t2 = latestThread(byBuyer.docs.map(d => Object.assign({ id: d.id }, d.data())));
      value = { thread: t2, buyerUserId: String(buyer), by: t2 ? "buyer" : null };
    }
  }
  _convCache.set(key, { at: Date.now(), value });
  if (_convCache.size > 300) _convCache.delete(_convCache.keys().next().value);
  return value;
}
let _etsyReads = [];
/** The buyer of a receipt, from the inbox's receipt mirror, our own cache, or at worst one metered
 *  Etsy read (at most once every six hours per receipt, and a few a minute in all). */
async function buyerOf(receiptId) {
  const [r, c] = await db.getAll(db.collection(COLL.receipts).doc(receiptId), db.collection(COLL.buyers).doc(receiptId));
  const rd = r.exists ? r.data() : null;
  const fromMirror = rd && (rd.buyer_user_id || rd.buyerUserId || (rd.raw && rd.raw.buyer_user_id));
  if (fromMirror) return String(fromMirror);
  if (c.exists) {
    const d = c.data();
    if (d.buyerUserId) return String(d.buyerUserId);
    if (Date.now() - (d.atMs || 0) < 6 * HOUR) return null;
  }
  const now = Date.now();
  _etsyReads = _etsyReads.filter(t => now - t < MIN);
  if (_etsyReads.length >= 6) return null;
  _etsyReads.push(now);
  let buyer = null;
  try {
    const { getShopReceiptFull } = require("./_etsyMailEtsy");
    const full = await getShopReceiptFull(receiptId);
    buyer = full && full.buyer_user_id ? String(full.buyer_user_id) : null;
  } catch (e) { console.warn("orderLink buyerOf:", receiptId, e.message); }
  await db.collection(COLL.buyers).doc(receiptId).set({ buyerUserId: buyer, atMs: Date.now() }).catch(() => {});
  return buyer;
}

/** The inbox marks conversations with an open production question: its folder, its badge, and a
 *  hold on the AI's automatic replies all read these fields. */
async function syncThreadFlags(threadId) {
  if (!isThreadId(threadId)) return;
  try {
    const q = await db.collection(COLL.eng).where("threadId", "==", threadId).limit(60).get();
    const open = q.docs.map(d => d.data()).filter(e => e.status === "open" && !e.sandbox)
      .sort((a, b) => (b.createdAtMs || 0) - (a.createdAtMs || 0));
    await db.collection(COLL.threads).doc(threadId).update({   // update, never create: a missing thread stays missing
      orderLinkIds: open.map(e => e.id),
      orderLinkOpen: open.length,
      orderLinkTitle: open.length ? (open[0].title || "Production question") : FV.delete(),
      orderLinkReceipts: [...new Set(open.map(e => String(e.receiptId)))],
      orderLinkOpenAt: open.length ? TS.fromMillis(open[0].createdAtMs || Date.now()) : FV.delete(),
      updatedAt: FV.serverTimestamp()
    });
  } catch (e) { console.warn("orderLink thread flags:", threadId, e.message); }
}

// Questions still waiting for a conversation, indexed by receipt and by buyer, so a scrape can find
// the question a brand-new conversation answers with one document read.
const waitingRef = () => db.collection(COLL.meta).doc("waiting");
let _waitingCache = { at: 0, value: null };
async function addWaiting(e) {
  const patch = { byReceipt: { [String(e.receiptId)]: FV.arrayUnion(e.id) } };
  if (e.customer && e.customer.buyerUserId) patch.byBuyer = { [String(e.customer.buyerUserId)]: FV.arrayUnion(e.id) };
  await waitingRef().set(patch, { merge: true }).catch(err => console.warn("orderLink waiting:", err.message));
  _waitingCache.at = 0;
}
async function removeWaiting(e) {
  const patch = { byReceipt: { [String(e.receiptId)]: FV.arrayRemove(e.id) } };
  if (e.customer && e.customer.buyerUserId) patch.byBuyer = { [String(e.customer.buyerUserId)]: FV.arrayRemove(e.id) };
  await waitingRef().set(patch, { merge: true }).catch(() => {});
  _waitingCache.at = 0;
}
async function waitingIndex() {
  if (_waitingCache.value && Date.now() - _waitingCache.at < 20 * 1000) return _waitingCache.value;
  const s = await waitingRef().get();
  const value = s.exists ? s.data() : {};
  _waitingCache = { at: Date.now(), value };
  return value;
}
function customerFrom(thread, cur) {
  const was = (cur && cur.customer) || {};
  return {
    name: thread.customerName || was.name || "",
    username: thread.etsyUsername || was.username || "",
    buyerUserId: thread.buyerUserId || was.buyerUserId || null
  };
}
/** Point one waiting question at a conversation. */
async function linkEngagement(id, threadId, thread, by) {
  const r = await change(id, cur => {
    if (cur.status !== "open" || cur.threadId || cur.sandbox) return null;
    return {
      threadId, link: "thread", linkedAtMs: Date.now(),
      linkedBy: by || (String(thread.etsyOrderId || "") === String(cur.receiptId) ? "order" : "buyer"),
      conversationUrl: thread.etsyConversationUrl || null, customer: customerFrom(thread, cur)
    };
  });
  if (r.changed) await removeWaiting(r.e);
  return r.changed ? r.e : null;
}
/** Link waiting questions to a conversation that has just appeared for their order or buyer. */
async function linkWaiting(threadId, thread) {
  const idx = await waitingIndex();
  const ids = new Set([
    ...((idx.byReceipt || {})[String(thread.etsyOrderId || "")] || []),
    ...((idx.byBuyer || {})[String(thread.buyerUserId || "")] || [])
  ]);
  const linked = [];
  for (const id of ids) { const e = await linkEngagement(id, threadId, thread); if (e) linked.push(e); }
  if (linked.length) await syncThreadFlags(threadId);
  return linked;
}

// ─── sending ─────────────────────────────────────────────────────────────────

const senderLabel = name => "Charm Sorter · " + String(name || "staff").slice(0, 60);
function friendlyFailure(code, error) {
  switch (code) {
    case "QUEUED_EXPIRED": return "Not sent: the inbox's Etsy helper did not pick it up within 30 minutes. Is a browser with the extension open?";
    case "CLAIM_ABANDONED": return "Not sent: the Etsy tab closed before the message went out.";
    case "CLIENT_CIRCUIT_BREAKER": return "Not sent: the Etsy helper could not deliver it after several tries.";
    case "SEND_DISABLED": return "Not sent: sending is paused in the inbox.";
    case "REPLACED": return "Not sent: someone in the inbox sent their own reply in this conversation first.";
    case "SLOT_CLEARED": return "Not sent: the inbox cleared this conversation's reply box before it went.";
    case "INBOX_CANCELLED": return "Not sent: it was cancelled in the inbox.";
    default: return "Not sent: " + String(error || "Etsy did not accept the message").slice(0, 200);
  }
}

/** Put each unsent message of an engagement on its way: through the inbox for a conversation,
 *  straight to "sent" in the sandbox, to the person's hands when there is no conversation yet. */
async function sendPending(e) {
  if (e.sandbox) {
    return mutate(e.id, cur => {
      if (!(cur.outbox || []).some(x => UNSENT.has(x.status))) return null;
      return { outbox: cur.outbox.map(x => UNSENT.has(x.status) ? Object.assign({}, x, { status: "sent", sentAtMs: Date.now(), sandbox: true }) : x) };
    });
  }
  if (!e.threadId) {
    return mutate(e.id, cur => {
      if (!(cur.outbox || []).some(x => UNSENT.has(x.status))) return null;
      return { outbox: cur.outbox.map(x => UNSENT.has(x.status) ? Object.assign({}, x, { status: "manual" }) : x) };
    });
  }
  await dispatchNext(e.threadId);
  const s = await engRef(e.id).get();
  return s.exists ? s.data() : e;
}

/** The oldest unsent sorter message in this conversation goes next, one at a time: the
 *  conversation has a single send slot, shared with the inbox. */
async function dispatchNext(threadId) {
  if (!isThreadId(threadId)) return false;
  const q = await db.collection(COLL.eng).where("threadId", "==", threadId).limit(60).get();
  let pick = null, busy = false;
  for (const d of q.docs) {
    const e = d.data();
    if (e.status !== "open" || e.sandbox) continue;
    for (const x of e.outbox || []) {
      if (IN_FLIGHT.has(x.status)) busy = true;
      else if (UNSENT.has(x.status) && (!pick || (x.atMs || 0) < (pick.x.atMs || 0))) pick = { e, x };
    }
  }
  if (!pick) { note({ waiting: { [threadId]: FV.delete() } }); return false; }
  if (busy) return false;
  const ds = await db.collection(COLL.drafts).doc("draft_" + threadId).get();
  const slot = ds.exists ? ds.data().status : null;
  if (slot === "queued" || slot === "sending") {
    // the inbox is sending in this conversation right now; this message goes the moment it is done
    await patchItem(pick.e.id, pick.x.id, { status: "waiting", waitReason: "inbox" }, new Set(["new"]));
    markWaiting(threadId);
    return false;
  }
  return enqueueItem(pick.e, pick.x);
}

async function enqueueItem(e, x) {
  // Claim the message first, so two dispatchers never queue it twice.
  const token = crypto.randomBytes(6).toString("hex");
  const claim = await patchItem(e.id, x.id, { status: "queued", claim: token, queuedAtMs: Date.now(), waitReason: null, error: null }, UNSENT);
  const mine = claim.e && (claim.e.outbox || []).find(y => y.id === x.id);
  if (!claim.changed || !mine || mine.claim !== token) return false;
  const QUEUED = new Set(["queued"]);
  let status = 0, data = {};
  try {
    const t = await db.collection(COLL.threads).doc(e.threadId).get();
    const url = (t.exists && t.data().etsyConversationUrl) || e.conversationUrl || ("https://www.etsy.com/your/conversations/" + e.threadId.slice("etsy_conv_".length));
    const { handler } = require("./etsyMailDraftSend");
    const res = await handler({
      httpMethod: "POST",
      headers: { "x-etsymail-secret": process.env.ETSYMAIL_EXTENSION_SECRET || "" },
      body: JSON.stringify({
        op: "enqueue", threadId: e.threadId, etsyConversationUrl: url, text: mine.text, attachments: [],
        employeeName: senderLabel(mine.by), sendOrigin: "manual", allowSendWithoutPendingTracking: true,
        orderLink: { e: e.id, i: x.id }
      })
    });
    status = res.statusCode;
    try { data = JSON.parse(res.body || "{}"); } catch (_) { data = {}; }
  } catch (err) { status = 500; data = { error: err.message }; }
  if (status === 200) { addInflight(e.id, x.id, e.threadId, data.draftId || ("draft_" + e.threadId)); return true; }
  if (status === 409 || status === 503) {
    await patchItem(e.id, x.id, { status: "waiting", waitReason: data.errorCode === "SEND_DISABLED" ? "paused" : "inbox", queuedAtMs: null, claim: null }, QUEUED);
    markWaiting(e.threadId);
    return false;
  }
  const faults = (mine.faults || 0) + 1;
  if (status >= 500 && faults < SERVER_RETRIES) {
    await patchItem(e.id, x.id, { status: "waiting", waitReason: "retry", faults, queuedAtMs: null, claim: null }, QUEUED);
    markWaiting(e.threadId);
    return false;
  }
  await patchItem(e.id, x.id, { status: "failed", error: friendlyFailure(data.errorCode, data.error), errorCode: data.errorCode || String(status || "ERROR"), faults, claim: null }, QUEUED);
  return false;
}

/** What a sorter send is about to overwrite on a conversation's draft slot, to be put back
 *  afterwards. Called by etsyMailDraftSend's enqueue inside its transaction. */
function parkedCopy(prev) {
  if (!prev) return { none: true };
  if (prev.orderLink) return prev.orderLinkParked || { none: true };   // an earlier sorter message has not given it back yet
  const had = {}, missing = [];
  for (const k of PARK_KEYS) { if (prev[k] === undefined) missing.push(k); else had[k] = prev[k]; }
  return { had, missing };
}

/** A sorter message's draft is done (sent, failed, or taken back in the inbox): record it, give
 *  the inbox its reply box back, and let the next waiting message go. */
async function settle(engId, itemId, draftId, draft) {
  const es = await engRef(engId).get();
  const item = es.exists ? (es.data().outbox || []).find(y => y.id === itemId) : null;
  const onSlot = !!(draft && draft.orderLink && draft.orderLink.e === engId && draft.orderLink.i === itemId);
  const ours = !!item && onSlot && normText(draft.text) === normText(item.text);
  // A message is settled once. One still marked unsent is settled too when the slot shows it was
  // queued after all (the inbox took it but its answer was lost).
  const from = ours ? new Set([...IN_FLIGHT, ...UNSENT]) : IN_FLIGHT;
  if (!item || !from.has(item.status)) {
    if (onSlot && item && !ours) await restoreInboxDraft(draftId, engId, itemId, item.text, false);
    dropInflight(engId, itemId);
    return false;
  }
  const status = draft ? draft.status : null;
  let patch;
  if (!ours) patch = { status: "failed", errorCode: draft ? "REPLACED" : "SLOT_CLEARED" };
  else if (status === "sent" || status === "sent_text_only") patch = { status: "sent", sentAtMs: tsMs(draft.sentAt) || Date.now() };
  else if (status === "sent_unverified") patch = { status: "sent", unverified: true, sentAtMs: tsMs(draft.sentAt) || Date.now(), note: "Etsy did not confirm it; it most likely went through." };
  else if (status === "failed") patch = { status: "failed", errorCode: draft.sendErrorCode || "FAILED", sendError: draft.sendError || null };
  else if (status === "draft") patch = { status: "failed", errorCode: "INBOX_CANCELLED" };
  else if (status === "sending") {
    if (item.status !== "sending") await patchItem(engId, itemId, { status: "sending", sendingAtMs: Date.now(), waitReason: null }, from);
    return false;
  } else {                                                               // still queued for the helper
    if (UNSENT.has(item.status)) {
      await patchItem(engId, itemId, { status: "queued", queuedAtMs: Date.now(), waitReason: null }, UNSENT);
      if (isThreadId(draft.threadId)) addInflight(engId, itemId, draft.threadId, draftId);
    }
    return false;
  }
  if (patch.status === "failed") { patch.error = friendlyFailure(patch.errorCode, patch.sendError); delete patch.sendError; }
  patch.claim = null; patch.waitReason = null;
  const r = await patchItem(engId, itemId, patch, from);
  if (onSlot) await restoreInboxDraft(draftId, engId, itemId, item.text, patch.status === "failed");
  dropInflight(engId, itemId);
  if (r.e && r.e.threadId) await dispatchNext(r.e.threadId);
  return r.changed;
}

/** Give the inbox back what it had in the conversation's reply box before the sorter's message. */
async function restoreInboxDraft(draftId, engId, itemId, itemText, failed) {
  const ref = db.collection(COLL.drafts).doc(draftId);
  let threadId = null, stillOurs = false;
  try {
    await db.runTransaction(async tx => {
      threadId = null; stillOurs = false;
      const snap = await tx.get(ref);
      if (!snap.exists) return;
      const d = snap.data();
      if (!d.orderLink || d.orderLink.e !== engId || d.orderLink.i !== itemId) return;
      if (!TERMINAL_DRAFT.has(d.status) && d.status !== "draft") return;              // still on its way
      threadId = d.threadId || null;
      stillOurs = normText(d.text) === normText(itemText);
      const patch = { orderLink: FV.delete(), orderLinkParked: FV.delete(), updatedAt: FV.serverTimestamp() };
      if (stillOurs) {
        const parked = d.orderLinkParked || { none: true };
        if (parked.none) {
          // there was no draft before: take ours away, and the document too when nothing of the inbox's is on it
          if (!Object.keys(d).some(k => !SLOT_FRAME.has(k) && !PARK_KEYS.includes(k))) { tx.delete(ref); return; }
          for (const k of PARK_KEYS) patch[k] = FV.delete();
        } else {
          const had = parked.had || {};
          for (const k of PARK_KEYS) patch[k] = has(had, k) ? had[k] : FV.delete();
        }
      }
      tx.set(ref, patch, { merge: true });
    });
  } catch (e) { console.warn("orderLink restore draft:", draftId, e.message); }
  // a message that did not go leaves no "syncing…" copy behind in the inbox's conversation
  if (failed && stillOurs && threadId) {
    try {
      const oRef = db.collection(COLL.threads).doc(threadId).collection("messages").doc("optim_" + draftId);
      const o = await oRef.get();
      if (o.exists && sameText(o.data().text, itemText)) await oRef.delete();
    } catch (_) {}
  }
}

/** Hook for etsyMailDraftSend: a draft was sent, failed, expired or was cancelled. */
async function onDraftSettled(draftId, threadId) {
  try {
    const snap = await db.collection(COLL.drafts).doc(String(draftId)).get();
    const d = snap.exists ? snap.data() : null;
    if (d && d.orderLink && d.orderLink.e && d.orderLink.i) { await settle(d.orderLink.e, d.orderLink.i, snap.id, d); return; }
    // the inbox's own send is done: a sorter message waiting for the slot can go now
    const tid = (d && d.threadId) || threadId;
    if (!isThreadId(tid)) return;
    const t = await db.collection(COLL.threads).doc(tid).get();
    if (t.exists && (t.data().orderLinkOpen || 0) > 0) await dispatchNext(tid);
  } catch (e) { console.warn("orderLink onDraftSettled:", draftId, e.message); }
}

/** The sends the bell lists as in flight: settle each whose draft is done. */
async function checkInflight(inflight) {
  const entries = Object.values(inflight || {}).filter(x => x && x.e && x.i).slice(0, 25);
  let n = 0;
  if (entries.length) {
    const drafts = await db.getAll(...entries.map(x => db.collection(COLL.drafts).doc(x.d || ("draft_" + x.t))));
    for (let k = 0; k < entries.length; k++) {
      const x = entries[k], snap = drafts[k];
      const d = snap.exists ? snap.data() : null;
      const onSlot = d && d.orderLink && d.orderLink.e === x.e && d.orderLink.i === x.i;
      if (onSlot && d.status === "queued") continue;                   // waiting for the Etsy helper
      if (await settle(x.e, x.i, snap.id, d).catch(e => { console.warn("orderLink settle:", e.message); return false; })) n++;
    }
  }
  note({ inflightCheckedAtMs: Date.now() });
  return n;
}
/** Conversations where a sorter message waits for the slot: try again. */
async function checkWaiting(waiting) {
  let n = 0;
  for (const t of Object.keys(waiting || {}).filter(isThreadId).slice(0, 10)) {
    if (await dispatchNext(t).catch(e => { console.warn("orderLink waiting:", t, e.message); return false; })) n++;
  }
  note({ waitingCheckedAtMs: Date.now() });
  return n;
}

// ─── receiving ────────────────────────────────────────────────────────────

const MATCHABLE = new Set(["new", "waiting", "queued", "sending", "sent", "manual", "failed"]);
/** The sorter message a scraped shop message is the Etsy copy of, if any. */
const findMine = (outbox, m) => outbox.find(x => !x.msgId && MATCHABLE.has(x.status) && m.tsMs >= (x.atMs || 0) - WINDOW_SLACK_MS && sameText(x.text, m.text));

/** Fold new conversation messages into an engagement: customer replies raise its unread count,
 *  our own messages seen on Etsy are marked delivered, other shop replies are noted. */
function foldMessages(cur, msgs) {
  const from = (cur.startedAtMs || cur.createdAtMs || 0) - WINDOW_SLACK_MS;
  const until = cur.status === "resolved" && cur.resolvedAtMs ? cur.resolvedAtMs + WINDOW_SLACK_MS : Infinity;
  const seen = new Set(cur.seen || []);
  const fresh = msgs.filter(m => m && m.id && !seen.has(m.id) && m.tsMs >= from && m.tsMs <= until && !String(m.id).startsWith("optim_"))
    .sort((a, b) => a.tsMs - b.tsMs);
  if (!fresh.length) return null;
  const outbox = (cur.outbox || []).map(x => Object.assign({}, x));
  const patch = {};
  let unread = cur.unread || 0, inbound = cur.inboundCount || 0;
  for (const m of fresh) {
    seen.add(m.id);
    if (m.direction === "inbound") {
      unread++; inbound++;
      patch.lastInboundAtMs = Math.max(cur.lastInboundAtMs || 0, patch.lastInboundAtMs || 0, m.tsMs);
      patch.lastInboundPreview = preview(m.text) || (m.hasImages ? "Sent a photo" : "");
      patch.lastInboundBy = String(m.senderName || "").slice(0, 80);
      continue;
    }
    const mine = findMine(outbox, m);
    if (mine) {
      mine.msgId = m.id; mine.deliveredAtMs = m.tsMs;
      // a message someone sent by hand, or one Etsy took although the helper reported a failure
      const was = mine.status;
      if (["new", "waiting", "manual", "failed"].includes(was)) {
        mine.status = "sent"; mine.sentAtMs = m.tsMs; delete mine.error; delete mine.errorCode; delete mine.waitReason;
        if (was !== "failed") mine.manualSent = true;
      }
    } else patch.lastShopReplyAtMs = Math.max(cur.lastShopReplyAtMs || 0, patch.lastShopReplyAtMs || 0, m.tsMs);
  }
  patch.outbox = outbox;
  patch.unread = unread; patch.inboundCount = inbound;
  patch.seen = [...seen].slice(-SEEN_MAX);
  patch.pulledToMs = Math.max(cur.pulledToMs || 0, ...fresh.map(m => m.tsMs));
  return patch;
}

/** Hook for etsyMailSnapshot: new messages were written to a conversation.
 *  thread: the conversation's fields (the stored document merged with this scrape's patch).
 *  messages: [{ id, direction, senderName, tsMs, text, hasImages }] */
async function onThreadMessages(threadId, thread, messages) {
  try {
    if (!isThreadId(threadId) || !Array.isArray(messages) || !messages.length) return;
    thread = thread || {};
    const ids = Array.isArray(thread.orderLinkIds) ? thread.orderLinkIds.filter(x => typeof x === "string" && x).slice(0, 20) : [];
    let engs = [];
    if (ids.length) engs = (await db.getAll(...ids.map(engRef))).filter(s => s.exists).map(s => s.data()).filter(e => e.status === "open" && e.threadId === threadId);
    const linked = await linkWaiting(threadId, thread);
    for (const e of linked) if (!engs.some(x => x.id === e.id)) engs.push(e);
    for (const e of engs) await change(e.id, cur => foldMessages(cur, messages));
    // a question written before this conversation existed goes out now, unless the scrape shows it was sent by hand
    for (const e of linked) { await pullMissed(e).catch(() => 0); await releaseManual(e.id); }
    if (linked.length) await dispatchNext(threadId);
  } catch (e) { console.warn("orderLink onThreadMessages:", threadId, e.message); }
  finally { await flush(); }
}
/** Messages written for a buyer who had no conversation yet go through the inbox once one appears,
 *  unless the person already copied them to Etsy by hand. */
async function releaseManual(id) {
  return mutate(id, cur => {
    const fresh = Date.now() - 2 * DAY;
    if (!(cur.outbox || []).some(x => x.status === "manual" && !x.copiedAtMs && (x.atMs || 0) > fresh)) return null;
    return { outbox: cur.outbox.map(x => x.status === "manual" && !x.copiedAtMs && (x.atMs || 0) > fresh ? Object.assign({}, x, { status: "new" }) : x) };
  });
}

function messageFromDoc(doc) {
  const m = doc.data() || {};
  return {
    id: doc.id, direction: m.direction === "inbound" ? "inbound" : "outbound", senderName: m.senderName || "",
    tsMs: tsMs(m.timestamp) || tsMs(m.createdAt), text: m.text || "",
    hasImages: Array.isArray(m.imageUrls) && m.imageUrls.length > 0, optimistic: !!m.localOptimistic || doc.id.startsWith("optim_"),
    raw: m
  };
}

/** Messages the scrape hook may have missed (a failed hook, a crash): read them from the conversation. */
async function pullMissed(e, activityMs) {
  if (!isThreadId(e.threadId)) return 0;
  const since = Math.max((e.startedAtMs || e.createdAtMs || 0) - WINDOW_SLACK_MS, (e.pulledToMs || 0) - 10 * MIN);
  const q = await db.collection(COLL.threads).doc(e.threadId).collection("messages")
    .where("timestamp", ">=", TS.fromMillis(since)).orderBy("timestamp").limit(200).get();
  const msgs = q.docs.map(messageFromDoc).filter(m => !m.optimistic);
  const r = await change(e.id, cur => {
    const patch = msgs.length ? foldMessages(cur, msgs) : null;
    if (activityMs && activityMs > (cur.checkedToMs || 0)) return Object.assign(patch || {}, { checkedToMs: activityMs });
    return patch;
  });
  return r.changed ? 1 : 0;
}

// ─── what the sorter shows: the engagement's stretch of the conversation ─────

function imageList(m) {
  const urls = Array.isArray(m.imageUrls) ? m.imageUrls : [];
  const paths = Array.isArray(m.storageImagePaths) ? m.storageImagePaths : [];
  return urls.slice(0, 8).map((u, i) => paths[i]
    ? { src: "/.netlify/functions/etsyMailImage?path=" + encodeURIComponent(paths[i]), href: "/.netlify/functions/etsyMailImage?path=" + encodeURIComponent(paths[i]) }
    : { src: null, href: String(u || "") }).filter(x => x.src || /^https:\/\//.test(x.href));
}
function cardList(m) {
  return (Array.isArray(m.listingCards) ? m.listingCards : []).slice(0, 4)
    .map(c => ({ title: String((c && c.title) || "Listing").slice(0, 120), url: String((c && c.listingUrl) || "") }))
    .filter(c => /^https:\/\//.test(c.url));
}

async function conversation(e, { earlier = false } = {}) {
  const outbox = (e.outbox || []).map(x => Object.assign({}, x));
  const rows = [];
  let before = [];
  if (e.sandbox) {
    for (const s of e.sim || []) rows.push({ id: s.id, side: "customer", who: (e.customer && e.customer.name) || "Customer", atMs: s.atMs, text: s.text, images: [], cards: [] });
  } else if (isThreadId(e.threadId)) {
    const from = (e.startedAtMs || e.createdAtMs || 0) - WINDOW_SLACK_MS;
    const until = e.status === "resolved" && e.resolvedAtMs ? e.resolvedAtMs + WINDOW_SLACK_MS : Infinity;
    const col = db.collection(COLL.threads).doc(e.threadId).collection("messages");
    const q = await col.where("timestamp", ">=", TS.fromMillis(from)).orderBy("timestamp").limit(300).get();
    for (const doc of q.docs) {
      const m = messageFromDoc(doc);
      if (m.optimistic || m.tsMs > until) continue;
      if (m.direction === "inbound") {
        rows.push({ id: m.id, side: "customer", who: m.senderName || (e.customer && e.customer.name) || "Customer", atMs: m.tsMs, text: m.text, images: imageList(m.raw), cards: cardList(m.raw) });
        continue;
      }
      const mine = outbox.find(x => x.msgId === m.id) || findMine(outbox, m);
      if (mine) { mine.msgId = m.id; mine.deliveredAtMs = mine.deliveredAtMs || m.tsMs; continue; }
      rows.push({ id: m.id, side: "shop", who: m.senderName || "The shop", atMs: m.tsMs, text: m.text, images: imageList(m.raw), cards: cardList(m.raw) });
    }
    if (earlier) {
      const b = await col.where("timestamp", "<", TS.fromMillis(from)).orderBy("timestamp", "desc").limit(10).get();
      before = b.docs.map(messageFromDoc).filter(m => !m.optimistic).reverse()
        .map(m => ({ id: m.id, side: m.direction === "inbound" ? "customer" : "shop", who: m.senderName || "", atMs: m.tsMs, text: m.text, images: imageList(m.raw), cards: cardList(m.raw) }));
    }
  }
  for (const x of outbox) {
    if (x.status === "cancelled") continue;
    rows.push({
      id: "out_" + x.id, itemId: x.id, side: "us", who: x.by || "Sorter", atMs: x.atMs || 0, text: x.text, images: [], cards: [],
      status: x.status, error: x.error || null, note: x.note || null, unverified: !!x.unverified, delivered: !!(x.deliveredAtMs || x.msgId),
      sentAtMs: x.sentAtMs || 0, waitReason: x.waitReason || null, copied: !!x.copiedAtMs, manualSent: !!x.manualSent
    });
  }
  rows.sort((a, b) => a.atMs - b.atMs);
  return { messages: rows, earlier: before };
}

// ─── who is asking: a sorter connected to the inbox ───────────────────────

const _stations = new Map();
async function requireStation(event) {
  const key = String(header(event, "x-mail-station") || "").trim();
  const notConnected = { ok: false, status: 401, code: "NOT_CONNECTED", error: "This sorter is not connected to the inbox yet" };
  if (key.length < 30 || key.length > 120) return notConnected;
  const id = sha(key);
  const hit = _stations.get(id);
  if (hit && Date.now() - hit.at < MIN) return { ok: true, station: hit.station };
  const snap = await db.collection(COLL.stations).doc(id).get();
  if (!snap.exists || snap.data().revokedAtMs) { _stations.delete(id); return notConnected; }
  const st = snap.data();
  const op = await db.collection(COLL.operators).doc(String(st.username || "_")).get();
  if (!op.exists || op.data().revokedAt) return { ok: false, status: 401, code: "OPERATOR_REVOKED", error: "The inbox account that connected this sorter is no longer active" };
  const station = { id, username: st.username, name: String(op.data().displayName || st.displayName || st.username).slice(0, 60) };
  if (Date.now() - (st.lastSeenAtMs || 0) > HOUR) snap.ref.set({ lastSeenAtMs: Date.now() }, { merge: true }).catch(() => {});
  _stations.set(id, { station, at: Date.now() });
  if (_stations.size > 200) _stations.delete(_stations.keys().next().value);
  return { ok: true, station };
}

// Pairing: the sorter asks, a signed-in inbox approves, and only the sorter that asked (it alone
// holds the pair id) collects the key. The key itself is never stored, only its hash.
let _pairStarts = [];
async function pairStart(body) {
  const now = Date.now();
  _pairStarts = _pairStarts.filter(t => now - t < MIN);
  if (_pairStarts.length >= 30) throw httpError(429, "Too many connect attempts; try again in a minute");
  _pairStarts.push(now);
  const pairId = crypto.randomBytes(24).toString("base64url");
  const code = crypto.randomBytes(18).toString("base64url");
  await db.collection(COLL.pairs).doc(sha(pairId)).set({
    codeHash: sha(code), status: "pending", createdAtMs: now, expiresAtMs: now + PAIR_TTL_MS,
    label: cleanText(body.label, 80) || "Charm Sorter"
  });
  return { pairId, code, expiresAtMs: now + PAIR_TTL_MS, inboxUrl: INBOX_URL + "#pair=" + code };
}
async function pairFind(code) {
  const c = String(code || "");
  if (c.length < 16 || c.length > 64) return null;
  const q = await db.collection(COLL.pairs).where("codeHash", "==", sha(c)).limit(1).get();
  return q.empty ? null : q.docs[0];
}
/** The inbox side: what is asking to connect (for its confirm card), and the answer. */
async function pairInfo(code) {
  const doc = await pairFind(code);
  if (!doc) return { found: false };
  const d = doc.data();
  return { found: true, status: d.expiresAtMs < Date.now() ? "expired" : d.status, label: d.label || "Charm Sorter", createdAtMs: d.createdAtMs, expiresAtMs: d.expiresAtMs };
}
async function pairAnswer(code, session, approve) {
  const doc = await pairFind(code);
  if (!doc) throw httpError(404, "This connect link has expired. Press Connect in the sorter again.");
  let err = null;
  await db.runTransaction(async tx => {
    err = null;
    const s = await tx.get(doc.ref);
    const d = s.exists ? s.data() : null;
    if (!d || d.expiresAtMs < Date.now()) { err = httpError(410, "This connect link has expired. Press Connect in the sorter again."); return; }
    if (d.status !== "pending") { err = httpError(409, d.status === "approved" ? "Already connected" : "This request was declined"); return; }
    tx.set(doc.ref, approve
      ? { status: "approved", username: session.username, displayName: session.displayName || session.username, answeredAtMs: Date.now() }
      : { status: "denied", answeredAtMs: Date.now() }, { merge: true });
  });
  if (err) throw err;
  return { ok: true, status: approve ? "approved" : "denied" };
}
async function pairClaim(body) {
  const pairId = String(body.pairId || "");
  if (pairId.length < 20 || pairId.length > 80) throw httpError(400, "Missing pair id");
  const ref = db.collection(COLL.pairs).doc(sha(pairId));
  let out = { pending: true };
  await db.runTransaction(async tx => {
    out = { pending: true };
    const s = await tx.get(ref);
    if (!s.exists) { out = { expired: true }; return; }
    const d = s.data();
    if (d.expiresAtMs < Date.now()) { tx.delete(ref); out = { expired: true }; return; }
    if (d.status === "denied") { tx.delete(ref); out = { denied: true }; return; }
    if (d.status !== "approved") return;
    const key = crypto.randomBytes(32).toString("base64url");
    tx.set(db.collection(COLL.stations).doc(sha(key)), {
      username: d.username, displayName: d.displayName || d.username, label: d.label || "Charm Sorter",
      createdAtMs: Date.now(), lastSeenAtMs: Date.now()
    });
    tx.delete(ref);
    out = { stationKey: key, operator: { username: d.username, name: d.displayName || d.username } };
  });
  return out;
}
async function disconnect(station) {
  await db.collection(COLL.stations).doc(station.id).set({ revokedAtMs: Date.now() }, { merge: true });
  _stations.delete(station.id);
  return { ok: true };
}

// ─── the sorter's requests ─────────────────────────────────────────────────

/** What changed since the sorter last asked. One document read when nothing did. */
async function sync(body) {
  const seenN = Number.isFinite(Number(body.n)) ? Number(body.n) : -1;
  const since = Math.max(0, Number(body.since) || 0);
  const sandbox = body.sandbox === true;
  const bs = await bellRef().get();
  const bell = bs.exists ? bs.data() : {};
  const now = Date.now();
  // the sorters' polls also keep sends moving when a hook in the inbox was missed
  let moved = 0;
  if (bell.inflight && Object.keys(bell.inflight).length && now - (bell.inflightCheckedAtMs || 0) > 8000) moved += await checkInflight(bell.inflight).catch(e => { console.warn("orderLink inflight:", e.message); return 0; });
  if (bell.waiting && Object.keys(bell.waiting).length && now - (bell.waitingCheckedAtMs || 0) > 15000) moved += await checkWaiting(bell.waiting).catch(e => { console.warn("orderLink waiting:", e.message); return 0; });
  const n = Number(bell.n) || 0;
  let full = !since || body.full === true;
  if (!full && !moved && seenN === n) return { n, v: since, changes: [], full: false, now };
  let docs = null;
  if (!full) {
    const q = await db.collection(COLL.eng).where("v", ">", since - DELTA_OVERLAP_MS).orderBy("v").limit(300).get();
    if (q.size < 300) docs = q.docs.map(d => d.data());
    else full = true;
  }
  if (full) {
    const [open, recent] = await Promise.all([
      db.collection(COLL.eng).where("status", "==", "open").limit(500).get(),
      db.collection(COLL.eng).where("resolvedAtMs", ">", now - 3 * DAY).limit(200).get()
    ]);
    const m = new Map();
    for (const d of open.docs.concat(recent.docs)) m.set(d.id, d.data());
    docs = [...m.values()];
  }
  let v = since;
  for (const e of docs) if ((e.v || 0) > v) v = e.v;
  return { n, v, changes: docs.filter(e => !!e.sandbox === sandbox).map(summary), full, now };
}

/** Everything the Customer panel of one order needs, in one round trip. */
async function order(body) {
  const receiptId = cleanId(body.receiptId);
  if (!receiptId) throw httpError(400, "Which order?");
  const sandbox = body.sandbox === true;
  const list = await engagementsForReceipt(receiptId, sandbox);
  let conv = null;
  if (!sandbox) {
    const withThread = list.find(e => e.threadId);
    if (withThread) conv = { threadId: withThread.threadId, customer: withThread.customer || null, by: withThread.linkedBy || "order" };
    else {
      const c = await findConversation(receiptId);
      conv = c.thread ? { threadId: c.thread.id, customer: customerFrom(c.thread, null), by: c.by } : null;
    }
  }
  const want = cleanId(body.engagementId);
  const scope = body.scope === "engraving" ? "engraving" : "order";
  const lineId = cleanId(body.lineId);
  const active = (want && list.find(e => e.id === want))
    || (scope === "engraving" && list.find(e => e.status === "open" && e.scope === "engraving" && e.lineId === lineId))
    || list.find(e => e.unread > 0 && e.status === "open")
    || (lineId && list.find(e => e.status === "open" && e.scope === "engraving" && e.lineId === lineId))
    || list.find(e => e.status === "open" && e.scope === "order")
    || list.find(e => e.status === "open")
    || list[0] || null;
  return {
    receiptId, sandbox, conversation: conv, inboxUrl: INBOX_URL,
    engagements: list.map(summary),
    active: active ? Object.assign(summary(active), await conversation(active)) : null
  };
}

async function thread(body) {
  const id = cleanId(body.engagementId);
  const s = id ? await engRef(id).get() : null;
  if (!s || !s.exists) throw httpError(404, "That conversation is gone");
  const e = s.data();
  if (!!e.sandbox !== (body.sandbox === true)) throw httpError(404, "That conversation is gone");
  return Object.assign(summary(e), await conversation(e, { earlier: body.earlier === true }));
}
async function fresh(id) {
  const s = await engRef(id).get();
  if (!s.exists) throw httpError(404, "That conversation is gone");
  const e = s.data();
  return Object.assign(summary(e), await conversation(e));
}

async function ask(station, body) {
  const receiptId = cleanId(body.receiptId);
  if (!receiptId) throw httpError(400, "Which order?");
  const text = cleanText(body.text);
  if (!text) throw httpError(400, "Write a message first");
  const clientId = cleanId(body.clientId) || ("m" + crypto.randomBytes(8).toString("hex"));
  const sandbox = body.sandbox === true;
  const scope = body.scope === "engraving" ? "engraving" : "order";
  const lineId = scope === "engraving" ? cleanId(body.lineId) : null;
  let e = null;
  const want = cleanId(body.engagementId);
  if (want) {
    const s = await engRef(want).get();
    if (s.exists && s.data().receiptId === receiptId && !!s.data().sandbox === sandbox) e = s.data();
  }
  if (!e && !body.newQuestion) e = (await engagementsForReceipt(receiptId, sandbox)).find(x => x.status === "open" && x.scope === scope && (scope !== "engraving" || x.lineId === lineId)) || null;
  if (!e) e = await createEngagement(station, {
    receiptId, scope, lineId, sandbox, text,
    lineLabel: cleanText(body.lineLabel, 120), orderNumber: cleanText(body.orderNumber, 40), buyerName: cleanText(body.buyerName, 80)
  });
  const wasOpen = e.status === "open";
  const r = await change(e.id, cur => {
    if ((cur.outbox || []).some(x => x.id === clientId)) return null;      // a retried request: already recorded
    const item = { id: clientId, text, by: station.name, atMs: Date.now(), status: "new" };
    const patch = { outbox: (cur.outbox || []).filter(x => x.status !== "cancelled" || (x.atMs || 0) > Date.now() - DAY).concat(item).slice(-OUTBOX_MAX), lastOutboundAtMs: item.atMs };
    if (!cur.title) patch.title = firstLine(text);
    if (cur.status !== "open") Object.assign(patch, { status: "open", resolvedAtMs: FV.delete(), resolvedBy: FV.delete(), reopenedAtMs: item.atMs });
    return patch;
  });
  if (!r.e) throw httpError(404, "That conversation is gone");
  e = r.e;
  if (!wasOpen && e.threadId) await syncThreadFlags(e.threadId);
  if (!wasOpen && !e.threadId && !e.sandbox) await addWaiting(e);
  e = await sendPending(e);
  return Object.assign(summary(e), await conversation(e));
}

async function retry(body) {
  const id = cleanId(body.engagementId), itemId = cleanId(body.itemId);
  const r = await patchItem(id, itemId, { status: "new", error: null, errorCode: null, waitReason: null, faults: null, copiedAtMs: null, retriedAtMs: Date.now() }, new Set(["failed", "waiting", "manual"]));
  if (!r.e) throw httpError(404, "That message is gone");
  const after = await sendPending(r.e);
  return Object.assign(summary(after), await conversation(after));
}

async function cancel(body) {
  const id = cleanId(body.engagementId), itemId = cleanId(body.itemId);
  const s = await engRef(id).get();
  if (!s.exists) throw httpError(404, "That message is gone");
  const e = s.data();
  const item = (e.outbox || []).find(x => x.id === itemId);
  if (!item) throw httpError(404, "That message is gone");
  if (item.status === "queued" && e.threadId) {
    // still waiting for the Etsy helper: take it back out, and give the inbox its reply box back
    const draftId = "draft_" + e.threadId;
    const ref = db.collection(COLL.drafts).doc(draftId);
    let took = false;
    await db.runTransaction(async tx => {
      took = false;
      const ds = await tx.get(ref);
      const d = ds.exists ? ds.data() : null;
      if (!d || d.status !== "queued" || !d.orderLink || d.orderLink.i !== itemId) return;
      took = true;
      tx.set(ref, { status: "draft", updatedAt: FV.serverTimestamp() }, { merge: true });
    });
    if (!took) throw httpError(409, "Too late: the inbox's Etsy helper has already picked it up");
    await patchItem(id, itemId, { status: "cancelled", cancelledAtMs: Date.now(), claim: null }, new Set(["queued"]));
    await restoreInboxDraft(draftId, id, itemId, item.text, true);
    dropInflight(id, itemId);
  } else {
    const r = await patchItem(id, itemId, { status: "cancelled", cancelledAtMs: Date.now() }, new Set(["new", "waiting", "manual", "failed"]));
    if (!r.changed) throw httpError(409, "Too late: it has already gone");
  }
  if (e.threadId) await dispatchNext(e.threadId);
  return fresh(id);
}

/** The person copied a message to send it on Etsy by hand: never send it for them afterwards. */
async function markCopied(body) {
  const id = cleanId(body.engagementId), itemId = cleanId(body.itemId);
  await patchItem(id, itemId, { copiedAtMs: Date.now() }, new Set(["manual", "failed"]));
  return fresh(id);
}
/** The person sent a message on Etsy by hand. */
async function markSent(body) {
  const id = cleanId(body.engagementId), itemId = cleanId(body.itemId);
  const r = await patchItem(id, itemId, { status: "sent", manualSent: true, sentAtMs: Date.now(), error: null, errorCode: null, waitReason: null }, new Set(["manual", "failed", "waiting"]));
  if (!r.e) throw httpError(404, "That message is gone");
  return fresh(id);
}

async function read(body) {
  const id = cleanId(body.engagementId);
  const s = id ? await engRef(id).get() : null;
  if (!s || !s.exists) return { ok: true };
  const e = s.data();
  const ids = [id];
  if (e.threadId) {
    // one reply read once: the other open questions of the same conversation showed it too
    const q = await db.collection(COLL.eng).where("threadId", "==", e.threadId).limit(60).get();
    for (const d of q.docs) if (d.id !== id && d.data().status === "open" && (d.data().unread || 0) > 0) ids.push(d.id);
  }
  for (const x of ids) await change(x, cur => (cur.unread || 0) > 0 ? { unread: 0, readAtMs: Date.now() } : null);
  return { ok: true };
}

async function setStatus(station, body, status) {
  const id = cleanId(body.engagementId);
  const r = await change(id, cur => {
    if (cur.status === status) return null;
    return status === "resolved"
      ? { status, resolvedAtMs: Date.now(), resolvedBy: station.name, unread: 0 }
      : { status, resolvedAtMs: FV.delete(), resolvedBy: FV.delete(), reopenedAtMs: Date.now() };
  });
  if (!r.e) throw httpError(404, "That conversation is gone");
  if (r.changed) {
    if (r.e.threadId) await syncThreadFlags(r.e.threadId);
    else if (!r.e.sandbox) { if (status === "resolved") await removeWaiting(r.e); else await addWaiting(r.e); }
    if (status === "open" && r.e.threadId) await dispatchNext(r.e.threadId);
  }
  return Object.assign(summary(r.e), await conversation(r.e));
}

async function setLang(body) {
  const id = cleanId(body.engagementId);
  const lang = String(body.lang || "").toLowerCase();
  if (!/^(|[a-z]{2,3})$/.test(lang)) return { ok: true };
  await change(id, cur => (cur.lang || "") === lang ? null : { lang: lang || FV.delete() });
  return { ok: true };
}

/** The person points a waiting question at a conversation themselves, by pasting its Etsy address. */
async function linkUrl(body) {
  const id = cleanId(body.engagementId);
  const m = String(body.url || "").match(/etsy\.com\/(?:your\/conversations|conversations|your\/messages\/(?:buyer|thread)|messages)\/(\d+)/);
  if (!m) throw httpError(400, "Paste the address of the Etsy conversation");
  const threadId = "etsy_conv_" + m[1];
  const t = await db.collection(COLL.threads).doc(threadId).get();
  if (!t.exists) throw httpError(404, "The inbox does not have that conversation yet. It appears once a message in it has been read by the inbox.");
  const s = await engRef(id).get();
  if (!s.exists || s.data().sandbox) throw httpError(404, "That conversation is gone");
  if (s.data().threadId && s.data().threadId !== threadId) throw httpError(409, "This question already belongs to a conversation");
  const e = await linkEngagement(id, threadId, t.data(), "hand");
  if (e) {
    await syncThreadFlags(threadId);
    await pullMissed(e).catch(() => 0);
    await releaseManual(e.id);
    await dispatchNext(threadId);
  }
  return fresh(id);
}

/** Sandbox only: play the customer's side so the whole loop can be tried without a real customer. */
async function simulateReply(body) {
  const id = cleanId(body.engagementId);
  const text = cleanText(body.text, 1000) || "Thanks! Yes, that works for me.";
  const r = await change(id, cur => {
    if (!cur.sandbox) return null;
    const msg = { id: "sim_" + Date.now().toString(36), text, atMs: Date.now() };
    return {
      sim: (cur.sim || []).concat(msg).slice(-40), unread: (cur.unread || 0) + 1, inboundCount: (cur.inboundCount || 0) + 1,
      lastInboundAtMs: msg.atMs, lastInboundPreview: preview(text), lastInboundBy: (cur.customer && cur.customer.name) || "Customer"
    };
  });
  if (!r.e || !r.e.sandbox) throw httpError(400, "Only a sandbox conversation can be simulated");
  return Object.assign(summary(r.e), await conversation(r.e));
}

// ─── translation ──────────────────────────────────────────────────────────

const LANG_NAMES = { en: "English", uk: "Ukrainian" };
const TRANSLATE_MODEL = "claude-opus-5";
const TRANSLATE_SCHEMA = {
  type: "object", additionalProperties: false, required: ["translations"],
  properties: {
    translations: {
      type: "array",
      items: {
        type: "object", additionalProperties: false, required: ["id", "text", "from", "same"],
        properties: {
          id: { type: "string" },
          text: { type: "string", description: "The message in the target language" },
          from: { type: "string", description: "ISO 639-1 code of the language the message was written in" },
          same: { type: "boolean", description: "True when the message was already in the target language" }
        }
      }
    }
  }
};
let _claudeCalls = [];

async function claudeTranslate(list, target) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw httpError(503, "Translation is not set up on the server");
  const now = Date.now();
  _claudeCalls = _claudeCalls.filter(t => now - t < MIN);
  if (_claudeCalls.length >= 60) throw httpError(429, "Too many translations at once; try again in a minute");
  _claudeCalls.push(now);
  const fetch = require("node-fetch");
  const lang = LANG_NAMES[target];
  const system = `You translate messages between a small jewellery workshop that sells on Etsy and its customers, for the workshop's staff. Translate each message into ${lang}, keeping its meaning, tone and line breaks. Words the customer wants engraved or printed, names, dates, order numbers, SKUs, sizes and anything inside quotation marks stay exactly as written. If a message is already in ${lang}, return it unchanged with same set to true.`;
  const chars = list.reduce((n, x) => n + x.text.length, 0);
  const body = {
    model: TRANSLATE_MODEL,
    max_tokens: Math.min(16000, 2000 + Math.ceil(chars * 1.6)),
    system,
    messages: [{ role: "user", content: JSON.stringify({ messages: list.map(x => ({ id: x.id, text: x.text })) }) }],
    output_config: { effort: "low", format: { type: "json_schema", schema: TRANSLATE_SCHEMA } },
    fallbacks: "default"
  };
  let res, data;
  try {
    res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01", "anthropic-beta": "server-side-fallback-2026-07-01" },
      body: JSON.stringify(body),
      timeout: 22000
    });
    data = await res.json().catch(() => ({}));
  } catch (e) { throw httpError(504, "Translation is taking too long; try again"); }
  if (!res.ok) throw httpError(502, "Translation failed: " + ((data.error && data.error.message) || res.status));
  if (data.stop_reason === "refusal") throw httpError(422, "This message could not be translated");
  if (data.stop_reason === "max_tokens") throw httpError(502, "Translation was cut short; try fewer messages at once");
  const block = (data.content || []).find(b => b.type === "text");
  let parsed;
  try { parsed = JSON.parse(block ? block.text : ""); } catch (_) { throw httpError(502, "Translation came back unreadable"); }
  const out = new Map();
  for (const t of parsed.translations || []) if (t && t.id) out.set(String(t.id), { text: String(t.text || ""), from: String(t.from || "").toLowerCase().slice(0, 8), same: !!t.same });
  return out;
}

/** Translate up to 12 messages into English or Ukrainian. Every translation is kept, so a message
 *  is only ever translated once for each language. */
async function translate(body) {
  const target = body.target === "uk" ? "uk" : "en";
  const items = (Array.isArray(body.items) ? body.items : []).slice(0, 12)
    .map((x, i) => ({ id: String((x && x.id) || i).slice(0, 80), text: cleanText(x && x.text, TEXT_MAX) }))
    .filter(x => x.text);
  if (!items.length) return { target, translations: {} };
  const keys = items.map(x => sha("v1|" + target + "|" + x.text));
  const snaps = await db.getAll(...keys.map(k => db.collection(COLL.trans).doc(k)));
  const result = {}, missing = [];
  snaps.forEach((s, i) => {
    if (s.exists) {
      const d = s.data();
      result[items[i].id] = { text: d.text, from: d.from || "", same: !!d.same };
      if (Date.now() - (d.usedAtMs || 0) > DAY) s.ref.set({ usedAtMs: Date.now() }, { merge: true }).catch(() => {});
    } else missing.push(i);
  });
  if (missing.length) {
    const got = await claudeTranslate(missing.map(i => ({ id: "m" + i, text: items[i].text })), target);
    const batch = db.batch();
    for (const i of missing) {
      const t = got.get("m" + i);
      if (!t) continue;
      result[items[i].id] = t;
      batch.set(db.collection(COLL.trans).doc(keys[i]), { text: t.text, from: t.from, same: t.same, target, chars: items[i].text.length, atMs: Date.now(), usedAtMs: Date.now() });
    }
    await batch.commit().catch(e => console.warn("orderLink translation cache:", e.message));
  }
  return { target, translations: result };
}

// ─── upkeep, from the inbox's five-minute reaper ────────────────────────

async function reconcile({ budgetMs = 20000 } = {}) {
  const t0 = Date.now();
  const out = { open: 0, settled: 0, dispatched: 0, pulled: 0, linked: 0, autoResolved: 0, deleted: 0 };
  const left = () => budgetMs - (Date.now() - t0);
  try {
    const bs = await bellRef().get();
    const bell = bs.exists ? bs.data() : {};
    out.settled += await checkInflight(bell.inflight);
    out.dispatched += await checkWaiting(bell.waiting);

    const open = await db.collection(COLL.eng).where("status", "==", "open").limit(400).get();
    out.open = open.size;
    const byThread = new Map();
    for (const d of open.docs) {
      const e = d.data();
      if (e.sandbox || !e.threadId) continue;
      if (!byThread.has(e.threadId)) byThread.set(e.threadId, []);
      byThread.get(e.threadId).push(e);
    }
    const threadIds = [...byThread.keys()];
    const threadDocs = new Map();
    for (let i = 0; i < threadIds.length && left() > 5000; i += 100) {
      const snaps = await db.getAll(...threadIds.slice(i, i + 100).map(id => db.collection(COLL.threads).doc(id)));
      for (const s of snaps) if (s.exists) threadDocs.set(s.id, s.data());
    }
    for (const [threadId, engs] of byThread) {
      if (left() < 4000) break;
      const t = threadDocs.get(threadId);
      if (!t) continue;
      const flagged = Array.isArray(t.orderLinkIds) ? t.orderLinkIds : [];
      if (flagged.length !== engs.length || engs.some(e => !flagged.includes(e.id))) await syncThreadFlags(threadId);
      const activity = Math.max(tsMs(t.lastInboundAt), tsMs(t.lastOutboundAt), tsMs(t.lastOperatorReplyAt));
      let resolved = false;
      for (const e of engs) {
        if (activity > (e.checkedToMs || 0) && activity >= (e.startedAtMs || 0) - WINDOW_SLACK_MS) out.pulled += await pullMissed(e, activity).catch(() => 0);
        const quiet = Math.max(e.lastInboundAtMs || 0, e.lastOutboundAtMs || 0, e.createdAtMs || 0, e.reopenedAtMs || 0);
        const pending = (e.outbox || []).some(x => UNSENT.has(x.status) || IN_FLIGHT.has(x.status));
        if (!pending && Date.now() - quiet > AUTO_RESOLVE_IDLE_MS) {
          const r = await change(e.id, cur => cur.status === "open" ? { status: "resolved", resolvedAtMs: Date.now(), resolvedBy: "Quiet for three weeks", unread: 0 } : null);
          if (r.changed) { out.autoResolved++; resolved = true; }
        }
      }
      if (resolved) await syncThreadFlags(threadId);
      if (engs.some(e => (e.outbox || []).some(x => UNSENT.has(x.status)))) { if (await dispatchNext(threadId)) out.dispatched++; }
    }
    // questions still waiting for a conversation: look again now and then
    for (const d of open.docs) {
      if (left() < 3000) break;
      const e = d.data();
      if (e.sandbox || e.threadId) continue;
      if (Date.now() - Math.max(e.createdAtMs || 0, e.lastOutboundAtMs || 0, e.reopenedAtMs || 0) > AUTO_RESOLVE_IDLE_MS) {
        const r = await change(e.id, cur => cur.status === "open" && !cur.threadId ? { status: "resolved", resolvedAtMs: Date.now(), resolvedBy: "Quiet for three weeks" } : null);
        if (r.changed) { await removeWaiting(e); out.autoResolved++; }
        continue;
      }
      if (Date.now() - (e.lastLookAtMs || e.createdAtMs || 0) < 20 * MIN) continue;
      await change(e.id, () => ({ lastLookAtMs: Date.now() }));
      const c = await findConversation(e.receiptId, { fresh: true }).catch(() => ({ thread: null }));
      if (!c.thread) continue;
      const linked = await linkEngagement(e.id, c.thread.id, c.thread, c.by);
      if (!linked) continue;
      out.linked++;
      await syncThreadFlags(c.thread.id);
      await pullMissed(linked).catch(() => 0);
      await releaseManual(linked.id);
      if (await dispatchNext(c.thread.id)) out.dispatched++;
    }
    if (left() > 3000) out.deleted += await prune();
  } catch (e) { console.warn("orderLink reconcile:", e.message); out.error = e.message; }
  finally { await flush(); }
  return out;
}

/** Nothing piles up: old conversations, translations, pairing requests and dead connections go. */
async function prune() {
  let n = 0;
  const now = Date.now();
  const del = async q => { const s = await q.get(); if (s.empty) return 0; const b = db.batch(); s.docs.forEach(d => b.delete(d.ref)); await b.commit(); return s.size; };
  try {
    const old = await db.collection(COLL.eng).where("resolvedAtMs", "<", now - KEEP_RESOLVED_MS).limit(50).get();
    for (const d of old.docs) if (d.data().status === "resolved") { await d.ref.delete(); n++; }
    n += await del(db.collection(COLL.trans).where("usedAtMs", "<", now - KEEP_TRANSLATION_MS).limit(200));
    n += await del(db.collection(COLL.pairs).where("expiresAtMs", "<", now - HOUR).limit(50));
    n += await del(db.collection(COLL.buyers).where("atMs", "<", now - 90 * DAY).limit(100));
    n += await del(db.collection(COLL.stations).where("revokedAtMs", "<", now - 30 * DAY).limit(20));
    n += await del(db.collection(COLL.stations).where("lastSeenAtMs", "<", now - 365 * DAY).limit(20));
    // the waiting index is rebuilt once a day from the questions actually waiting, so emptied keys do not linger
    const w = await waitingRef().get();
    if (!w.exists || now - (w.data().rebuiltAtMs || 0) > DAY) {
      const open = await db.collection(COLL.eng).where("status", "==", "open").limit(500).get();
      const byReceipt = {}, byBuyer = {};
      for (const d of open.docs) {
        const e = d.data();
        if (e.sandbox || e.threadId) continue;
        (byReceipt[e.receiptId] = byReceipt[e.receiptId] || []).push(e.id);
        if (e.customer && e.customer.buyerUserId) (byBuyer[e.customer.buyerUserId] = byBuyer[e.customer.buyerUserId] || []).push(e.id);
      }
      await waitingRef().set({ byReceipt, byBuyer, rebuiltAtMs: now });
      _waitingCache.at = 0;
    }
    // the bell's lists only ever hold live entries
    const bs = await bellRef().get();
    const bell = bs.exists ? bs.data() : {};
    const staleFlights = Object.entries(bell.inflight || {}).filter(([, x]) => !x || now - (x.at || 0) > 2 * DAY).map(([k]) => k);
    const staleWaits = Object.entries(bell.waiting || {}).filter(([, at]) => now - (Number(at) || 0) > 2 * DAY).map(([k]) => k);
    if (staleFlights.length) note({ inflight: Object.fromEntries(staleFlights.map(k => [k, FV.delete()])) });
    if (staleWaits.length) note({ waiting: Object.fromEntries(staleWaits.map(k => [k, FV.delete()])) });
  } catch (e) { console.warn("orderLink prune:", e.message); }
  return n;
}

module.exports = {
  COLL, PARK_KEYS, INBOX_URL,
  // hooks for the inbox's own functions
  parkedCopy, onDraftSettled: withFlush(onDraftSettled), onThreadMessages, reconcile,
  // the sorter endpoint
  requireStation, pairStart, pairInfo, pairAnswer, pairClaim, disconnect: withFlush(disconnect),
  sync: withFlush(sync), order: withFlush(order), thread, ask: withFlush(ask), retry: withFlush(retry),
  cancel: withFlush(cancel), markCopied: withFlush(markCopied), markSent: withFlush(markSent), read: withFlush(read),
  setStatus: withFlush(setStatus), setLang: withFlush(setLang), linkUrl: withFlush(linkUrl),
  simulateReply: withFlush(simulateReply), translate,
  // exposed for tests
  _internal: { foldMessages, sameText, normText, summary, cleanText, cleanId, applyPatch, friendlyFailure, parkedCopy, isDelete }
};
