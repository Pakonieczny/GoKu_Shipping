/*  netlify/functions/_etsyMailSendQueue.js
 *
 *  The dispatcher: ONE durable queue and ONE lease for every outgoing buyer message (inbox staff
 *  replies, Charm Sorter questions, automated replies). Design: plans/mail-conn-1010/MAILQUEUE-design.md.
 *
 *  What it guarantees
 *   - Exactly one message is in flight at a time, for the one Etsy account behind the one Chrome
 *     extension: every start goes through a transaction on the lease document (EtsyMail_SendQueueMeta/lease).
 *   - The draft slot (EtsyMail_Drafts/draft_<threadId>) stays the transport register the extension
 *     reads and writes. Only the lease holder's message is ever written into a slot as "queued", so an
 *     extension that knows nothing of this module (0.9.52) sees one job at a time.
 *   - Idempotency: a message has a key; the same key (a double click, a network retry, a reload)
 *     is one message.
 *   - The click on Etsy's Send button is fenced: mark_clicked succeeds only for the lease holder's
 *     session, once. A superseded tab cannot click. A message that may have been clicked is never
 *     re-sent without checking the stored conversation and a person's word.
 *   - Everything is in Firestore: a function timeout, a deploy or a reload loses nothing; the next
 *     call (enqueue, peek, state read, the 3-minute maintenance) repairs what a dead process left.
 *
 *  States: queued -> claimed -> sending -> sent -> confirmed | failed | needs_attention | cancelled
 *
 *  Zero Etsy API calls. Reads/writes are Firestore only.
 */
"use strict";

const crypto = require("crypto");
const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;
const TS = admin.firestore.Timestamp;

const Q_COLL = "EtsyMail_SendQueue";
const META = "EtsyMail_SendQueueMeta";
const DRAFTS = "EtsyMail_Drafts";
const THREADS = "EtsyMail_Threads";
const CONFIG_COLL = "EtsyMail_Config";

const MIN = 60 * 1000, HOUR = 60 * MIN, DAY = 24 * HOUR;

/* Every number here can be changed live in EtsyMail_SendQueueMeta/config (op queue_config). */
const DEFAULTS = Object.freeze({
  gapMs: 12000,                 // pause after a send finishes, before the next may start (human pace)
  gapJitter: 0.25,              // +/- share of the gap, random
  claimTtlMs: 2 * MIN,          // the extension must claim a job it was offered within this
  sendTtlMs: 60 * 1000,         // a sending job's lease without a heartbeat (heartbeats come every 5 s)
  maxAttempts: 4,
  backoffBaseMs: 30 * 1000,     // 30 s, 2 min, 8 min ...
  backoffFactor: 4,
  backoffCapMs: 15 * MIN,
  backoffJitter: 0.2,
  agingMs: 15 * MIN,            // an automated message that waited this long ranks as a human one
  offlineWaitMs: 30 * MIN,      // waiting this long with a silent helper: failed, HELPER_OFFLINE
  helperQuietMs: 10 * MIN,      // "silent helper": no check-in and no finished send for this long
  dupWindowMs: 20 * 1000,       // the same words again within this (no explicit key) are the same press
  keepDoneMs: 14 * DAY
});
const LIMITS = {
  gapMs: [0, 5 * MIN], gapJitter: [0, 0.9], claimTtlMs: [20000, 10 * MIN], sendTtlMs: [20000, 10 * MIN], maxAttempts: [1, 10],
  backoffBaseMs: [1000, 30 * MIN], backoffFactor: [1, 10], backoffCapMs: [5000, 6 * HOUR], backoffJitter: [0, 0.9],
  agingMs: [0, 6 * HOUR], offlineWaitMs: [MIN, 12 * HOUR], helperQuietMs: [MIN, 12 * HOUR], dupWindowMs: [0, 5 * MIN], keepDoneMs: [DAY, 90 * DAY]
};

const PROMOTE_AHEAD_MS = 30 * 1000;
const OPEN_STATES = new Set(["queued", "claimed", "sending"]);
const DEAD_STATES = new Set(["failed", "needs_attention"]);
const DONE_STATES = new Set(["sent", "confirmed", "cancelled"]);
const SENT_SLOT = new Set(["sent", "sent_text_only", "sent_unverified"]);

/* Failures the extension reports that happen before the click and are worth another try. */
const RETRYABLE_CODES = new Set([
  "EXECUTE_THREW", "MARK_CLICKED_FAILED", "ATTACHMENT_FETCH_FAILED", "DOM_FILEINPUT_MISS", "IMAGE_INJECT_FAILED",
  "DOM_TEXTAREA_MISS", "NETWORK", "TIMEOUT", "STRANDED_REQUEUED", "CLAIM_ABANDONED"
]);

let _rand = Math.random;
function _setRandom(fn) { _rand = fn || Math.random; }

// ─── small helpers ─────────────────────────────────────────────────────────

const sha1 = s => crypto.createHash("sha1").update(String(s)).digest("hex");
function normText(s) {
  return String(s || "").normalize("NFKC").toLowerCase().replace(/[‘’“”'"`]/g, "")
    .replace(/[^\p{L}\p{N}]+/gu, " ").trim();
}
function sameText(a, b) {
  const x = normText(a), y = normText(b);
  if (!x || !y) return false;
  if (x === y) return true;
  const n = Math.min(160, x.length, y.length);
  return n >= 40 && x.slice(0, n) === y.slice(0, n);
}
const isThreadId = v => /^etsy_conv_\d+$/.test(String(v || ""));
const nowMs = () => Date.now();
/** A creation time that is strictly increasing in this process, so two messages queued in the same millisecond keep their order. */
let _lastCreated = 0;
const createdStamp = () => { const t = nowMs(); _lastCreated = t > _lastCreated ? t : _lastCreated + 1; return _lastCreated; };
function tsMs(v) {
  if (!v) return 0;
  if (typeof v === "number") return v;
  if (typeof v.toMillis === "function") return v.toMillis();
  if (v._seconds != null) return v._seconds * 1000;
  const n = Date.parse(v); return Number.isFinite(n) ? n : 0;
}
/** Firestore refuses `undefined`: drop it (null stays). */
function clean(o) {
  const out = {};
  for (const [k, v] of Object.entries(o)) if (v !== undefined) out[k] = v;
  return out;
}
const qRef = id => db.collection(Q_COLL).doc(id);
const metaRef = id => db.collection(META).doc(id);
const slotRef = threadId => db.collection(DRAFTS).doc("draft_" + threadId);
const threadRef = id => db.collection(THREADS).doc(id);

let _del;      // looked up on first use: an offline test may load this module with a database stand-in that has no field deletes
function isDel(v) {
  try {
    if (_del === undefined) _del = typeof FV.delete === "function" ? FV.delete() : null;
    if (!_del) return false;
    if (v === _del) return true;
    return !!(v && typeof v === "object" && typeof v.isEqual === "function" && v.isEqual(_del));
  } catch (e) { return false; }
}
/** An item with a patch applied the way Firestore would (field deletes resolved), for use in memory. */
function applied(it, patch) {
  const o = Object.assign({}, it);
  for (const [k, v] of Object.entries(patch)) { if (isDel(v)) delete o[k]; else o[k] = v; }
  return o;
}
function eventsAppend(events, state, note, at) {
  const list = Array.isArray(events) ? events.slice(-15) : [];
  list.push({ t: at, s: state, n: String(note || "").slice(0, 160) });
  return list;
}

// ─── configuration ─────────────────────────────────────────────────────────

let _cfg = { at: 0, value: DEFAULTS };
function cfgFrom(raw) {
  const out = Object.assign({}, DEFAULTS);
  for (const [k, [lo, hi]] of Object.entries(LIMITS)) {
    const v = raw && Number(raw[k]);
    if (raw && raw[k] != null && Number.isFinite(v)) out[k] = Math.min(hi, Math.max(lo, v));
  }
  return out;
}
async function getConfig({ fresh = false } = {}) {
  const now = nowMs();
  if (!fresh && now - _cfg.at < 30 * 1000) return _cfg.value;
  try {
    const s = await metaRef("config").get();
    _cfg = { at: now, value: cfgFrom(s.exists ? s.data() : null) };
  } catch (e) { _cfg = { at: now, value: _cfg.value || DEFAULTS }; }
  return _cfg.value;
}
async function setConfig(patch) {
  const clean2 = {};
  for (const k of Object.keys(LIMITS)) if (patch && patch[k] != null && Number.isFinite(Number(patch[k]))) clean2[k] = Number(patch[k]);
  if (!Object.keys(clean2).length) return getConfig({ fresh: true });
  await metaRef("config").set(Object.assign(clean2, { updatedAtMs: nowMs() }), { merge: true });
  return getConfig({ fresh: true });
}

/* The global send switch (EtsyMail_Config/global.sendDisabled): while it is on nothing is started. */
let _pause = { at: 0, value: false };
async function isPaused() {
  const now = nowMs();
  if (now - _pause.at < 15 * 1000) return _pause.value;
  try { const s = await db.collection(CONFIG_COLL).doc("global").get(); _pause = { at: now, value: !!(s.exists && s.data().sendDisabled) }; }
  catch (e) { _pause = { at: now, value: false }; }
  return _pause.value;
}
function _resetCaches() { _cfg = { at: 0, value: DEFAULTS }; _pause = { at: 0, value: false }; }

// ─── pure rules (exported for the tests) ───────────────────────────────────

const effPriority = (it, now, cfg) => ((it.priority === 1 && now - (it.createdAtMs || 0) >= cfg.agingMs) ? 0 : (it.priority || 0));
const byAge = (a, b) => (a.createdAtMs || 0) - (b.createdAtMs || 0) || (a.sendId < b.sendId ? -1 : a.sendId > b.sendId ? 1 : 0);

/** The queued messages in the order they will go: human first, then automated, oldest first inside each,
 *  with an automated one that has waited long ranked as human, and each conversation strictly in order. */
function orderQueue(items, now, cfg) {
  cfg = cfg || DEFAULTS;
  const byThread = new Map();
  for (const it of items) {
    if (it.state !== "queued") continue;
    if (!byThread.has(it.threadId)) byThread.set(it.threadId, []);
    byThread.get(it.threadId).push(it);
  }
  for (const l of byThread.values()) l.sort(byAge);
  const out = [];
  while (byThread.size) {
    let best = null;
    for (const l of byThread.values()) {
      const h = l[0];
      if (!best) { best = h; continue; }
      const a = effPriority(h, now, cfg), b = effPriority(best, now, cfg);
      if (a < b || (a === b && byAge(h, best) < 0)) best = h;
    }
    out.push(best);
    const l = byThread.get(best.threadId); l.shift(); if (!l.length) byThread.delete(best.threadId);
  }
  return out;
}
/** The one that may start now: first in order whose time has come and whose conversation has no earlier message waiting out a delay. */
function pickNext(items, now, cfg) {
  const blocked = new Set();
  for (const it of orderQueue(items, now, cfg)) {
    if ((it.notBeforeMs || 0) > now) { blocked.add(it.threadId); continue; }
    if (blocked.has(it.threadId)) continue;
    return it;
  }
  return null;
}
function backoffMs(attempt, cfg, rnd) {
  cfg = cfg || DEFAULTS; rnd = rnd || _rand;
  const raw = Math.min(cfg.backoffCapMs, cfg.backoffBaseMs * Math.pow(cfg.backoffFactor, Math.max(0, attempt - 1)));
  return Math.round(raw * (1 + (rnd() * 2 - 1) * cfg.backoffJitter));
}
function gapFor(cfg, rnd) {
  cfg = cfg || DEFAULTS; rnd = rnd || _rand;
  return Math.max(0, Math.round(cfg.gapMs * (1 + (rnd() * 2 - 1) * cfg.gapJitter)));
}
const ordinal = n => { const s = ["th", "st", "nd", "rd"], v = n % 100; return n + (s[(v - 20) % 10] || s[v] || s[0]); };

/** Plain words for a failure, for people (never a code on its own). */
function plainReason(code, error) {
  switch (code) {
    case "HELPER_OFFLINE": return "The Etsy helper (the Chrome extension) is not running or not picking messages up. Open Chrome with the extension and press Try again.";
    case "NO_HELPER": return "The Etsy helper did not pick it up in time; it will try again by itself.";
    case "CLAIM_ABANDONED": return "The Etsy tab closed before the message went out; it will try again by itself.";
    case "STRANDED_POST_CLICK": return "Etsy's Send button was clicked but Etsy did not confirm it, so it may already have gone. Check the conversation on Etsy before sending it again.";
    case "DOM_TEXTAREA_MISS": return "Etsy's message box did not open in the helper's tab. Is the helper's Etsy tab logged in and open?";
    case "DOM_SEND_BUTTON_MISS": return "The helper could not find Etsy's Send button. Etsy may have changed its page.";
    case "ATTACHMENT_FETCH_FAILED": case "DOM_FILEINPUT_MISS": case "IMAGE_INJECT_FAILED": return "The helper could not attach the picture to the message.";
    case "SEND_DISABLED": return "Sending is paused in the inbox.";
    case "MAX_ATTEMPTS": return "Etsy did not take it after several tries" + (error ? ": " + String(error).slice(0, 160) : ".");
    case "CLIENT_CIRCUIT_BREAKER": return "The Etsy helper could not deliver it after several tries.";
    case "QUEUED_EXPIRED": return "It waited too long for the Etsy helper.";
    case "CANCELLED": return "It was cancelled.";
    default: return error ? String(error).replace(/\s+/g, " ").slice(0, 220) : "Etsy did not accept the message.";
  }
}

/** What to do with a failure the extension reported. Never retries after the click. */
function classifyFail({ errorCode, error, retry, stage, attempts, cfg }) {
  cfg = cfg || DEFAULTS;
  if (stage === "post_click") return { action: "attention", code: "STRANDED_POST_CLICK", plain: plainReason("STRANDED_POST_CLICK", error) };
  const retryable = (retry === true || RETRYABLE_CODES.has(errorCode)) && errorCode !== "DOM_SEND_BUTTON_MISS";
  if (retryable && attempts < cfg.maxAttempts) return { action: "retry", code: errorCode || "RETRY", plain: plainReason(errorCode, error), delayMs: backoffMs(attempts, cfg) };
  if (retryable) return { action: "failed", code: "MAX_ATTEMPTS", plain: plainReason("MAX_ATTEMPTS", error || plainReason(errorCode, "")) };
  return { action: "failed", code: errorCode || "FAILED", plain: plainReason(errorCode, error) };
}

function deriveKey(threadId, text, attachments) {
  const att = (Array.isArray(attachments) ? attachments : []).map(a => (a && (a.attachmentId || a.storagePath || a.listingId || a.filename)) || "").filter(Boolean).sort().join(",");
  return "d:" + threadId + ":" + sha1(normText(text) + "|" + att).slice(0, 24);
}
const sendIdFor = key => "q_" + sha1(key).slice(0, 24);
/** The words (and attachments) of a message, as one short fingerprint: two presses with the same words are the same message. */
function fingerprint(text, attachments) {
  return deriveKey("", text, attachments).slice(3);
}

// ─── views ─────────────────────────────────────────────────────────────────

/** The compact shape pages show. Positions are computed here, never stored. */
function viewItems(items, lease, now, cfg, { withText = true } = {}) {
  const order = orderQueue(items, now, cfg);
  const holder = lease && lease.holderSendId ? 1 : 0;
  const pos = new Map(order.map((it, i) => [it.sendId, holder + i + 1]));
  return items.map(it => {
    const v = {
      id: it.sendId, t: it.threadId, st: it.state, at: it.createdAtMs || 0, src: it.source || "", by: it.createdBy || "",
      tp: String(it.text || "").replace(/\s+/g, " ").slice(0, 90), att: Array.isArray(it.attachments) ? it.attachments.length : 0,
      n: it.attempts || 0, open: !!it.open
    };
    if (it.state === "queued") {
      v.pos = pos.get(it.sendId) || null;
      if ((it.notBeforeMs || 0) > now) { v.nb = it.notBeforeMs; v.wait = it.lastErrorCode === "NO_HELPER" ? "helper" : "retry"; }
      else if (lease && lease.paceUntilMs > now && !holder) v.wait = "pace";
    }
    if (it.state === "claimed") v.wait = lease && lease.paceUntilMs > now ? "pace" : "helper";
    if (it.sentAtMs) v.sentAt = it.sentAtMs;
    if (it.confirmedAtMs) v.confirmedAt = it.confirmedAtMs;
    if (it.unverified) v.unverified = true;
    if (it.partial) v.partial = true;
    if (it.plain) v.why = it.plain;
    if (it.lastErrorCode) v.code = it.lastErrorCode;
    if (it.orderLink && it.orderLink.i) v.ol = it.orderLink.i;
    if (it.orderLink && it.orderLink.e) v.oe = it.orderLink.e;
    if (withText && DEAD_STATES.has(it.state)) { v.text = String(it.text || "").slice(0, 4000); v.url = it.conversationUrl || null; }
    return v;
  });
}

// ─── submit: a message enters the queue ────────────────────────────────────

/** A sandbox message never enters the real queue, whoever asks: the page says so, or the sorter engagement it belongs to is the
 *  sandbox's (ids olsb_...). The refusal is the first thing every entry point does, before any read or write. */
function isSandboxInput(input) {
  if (!input || typeof input !== "object") return false;
  const ol = input.orderLink && typeof input.orderLink === "object" ? input.orderLink : null;
  return input.sandbox === true || !!(ol && (ol.sandbox === true || /^olsb_/.test(String(ol.e || ""))))
    || /^sorter:olsb_/.test(String(input.idempotencyKey || ""));
}

/**
 * input: { threadId, conversationUrl, text, attachments, origin: "manual"|"auto", source, employeeName, aiMeta, orderLink,
 *          polished, idempotencyKey, parentThreadFinalizePatch, admitOnly }
 * Resolves { item, created, deduped, learnPrev, threadFinalizeApplied } or { duplicateAutoSend, ... }.
 */
async function submit(input) {
  if (isSandboxInput(input)) return { sandboxRefused: true };
  const cfg = await getConfig();
  const now = nowMs();
  const threadId = input.threadId;
  const origin = input.origin === "auto" ? "auto" : "manual";
  const explicit = typeof input.idempotencyKey === "string" && input.idempotencyKey.trim() ? input.idempotencyKey.trim().slice(0, 200) : null;
  const key = explicit || deriveKey(threadId, input.text, input.attachments);
  const sendId = sendIdFor(key);
  const ref = qRef(sendId), sRef = slotRef(threadId), tRef = threadRef(threadId);
  const fin = input.parentThreadFinalizePatch && input.parentThreadFinalizePatch.threadId ? input.parentThreadFinalizePatch : null;
  const finRef = fin ? threadRef(fin.threadId) : null;
  let out = null;

  await db.runTransaction(async tx => {
    out = null;
    const cur = await tx.get(ref);
    const slotSnap = await tx.get(sRef);
    const tSnap = await tx.get(tRef);
    const fSnap = finRef && fin.threadId !== threadId ? await tx.get(finRef) : (finRef ? tSnap : null);
    const existing = cur.exists ? cur.data() : null;
    const slot = slotSnap.exists ? slotSnap.data() : null;
    // The same words pressed again a moment later under another key (two tabs with the same draft, two people) are the same
    // message. A Charm Sorter question is exempt: its key is authoritative (two lines may ask the same thing).
    const fp = fingerprint(input.text, input.attachments);
    const recentSnap = !existing && !input.orderLink && slot && slot.queueRecentFp === fp && slot.queueRecentSendId && slot.queueRecentSendId !== sendId
      && now - (Number(slot.queueRecentAtMs) || 0) < cfg.dupWindowMs ? await tx.get(qRef(slot.queueRecentSendId)) : null;

    if (existing) {
      const live = OPEN_STATES.has(existing.state) || (existing.state === "sent" && now - (existing.sentAtMs || 0) < 10 * MIN) || (DEAD_STATES.has(existing.state) && existing.open);
      if (explicit || live || now - (existing.createdAtMs || 0) < cfg.dupWindowMs) {
        out = { item: existing, created: false, deduped: true };
        return;
      }
    }

    if (recentSnap && recentSnap.exists) {
      const ri = recentSnap.data();
      if (OPEN_STATES.has(ri.state) || ri.state === "sent" || ri.state === "confirmed") { out = { item: ri, created: false, deduped: true, sameWords: true }; return; }
    }

    // An automated sender never goes in behind a reply a person has on its way in this conversation (audit F1).
    if (origin === "auto" && slot) {
      const manualBusy = ((slot.status === "queued" || slot.status === "sending") && slot.sendOrigin === "manual") ||
        (slot.queueWaiting === true && slot.queueWaitingOrigin === "manual");
      if (manualBusy) { out = { conflict: true, prevStatus: "queued (an operator reply is waiting to go)" }; return; }
    }
    // An automated reply identical to what was last sent in this conversation is not sent again (v1.6 guard).
    if (origin === "auto" && slot) {
      const prevIsSent = SENT_SLOT.has(slot.status);
      const prevSentText = String((prevIsSent ? slot.text : slot.lastSentText) || "").trim();
      const text = String(input.text || "").trim();
      if (prevSentText && text && prevSentText === text) {
        const at = prevIsSent ? slot.sentAt : slot.lastSentAt;
        out = { duplicateAutoSend: true, prevStatus: prevIsSent ? slot.status : (slot.lastSentStatus || slot.status), prevSentAtMs: at ? tsMs(at) : null, prevTextLen: prevSentText.length };
        return;
      }
    }

    const ai = input.aiMeta || {};
    const item = clean({
      sendId, idemKey: key, derived: !explicit, threadId, draftId: "draft_" + threadId, conversationUrl: input.conversationUrl,
      text: String(input.text || ""), attachments: Array.isArray(input.attachments) ? input.attachments : [],
      source: input.source || (input.orderLink ? "sorter" : origin === "auto" ? "auto" : "inbox"), origin,
      priority: origin === "auto" ? 1 : 0, createdBy: input.employeeName || (slot && slot.createdBy) || null,
      createdAtMs: createdStamp(), epoch: existing ? (existing.epoch || 0) + 1 : 0,
      orderLink: input.orderLink || null,
      generatedByAI: ai.generatedByAI != null ? !!ai.generatedByAI : !!(slot && slot.generatedByAI),
      aiModel: ai.model || (slot && slot.aiModel) || null, aiReasoning: ai.reasoning || (slot && slot.aiReasoning) || null,
      aiActiveQuestion: ai.activeQuestion || (slot && slot.aiActiveQuestion) || null,
      polished: input.polished === true ? true : undefined,
      state: "queued", open: true, attempts: 0, notBeforeMs: 0, stage: "pre_click", helperMisses: 0,
      sessions: [], events: eventsAppend([], "queued", "entered the queue", now), updatedAtMs: now
    });
    tx.set(ref, item, { merge: false });
    tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
    // the slot says "something is on its way in this conversation" for the AI senders that look there
    tx.set(sRef, { queueWaiting: true, queueWaitingOrigin: origin, queueWaitingSendId: sendId, queueRecentFp: fp, queueRecentSendId: sendId, queueRecentAtMs: now, updatedAt: FV.serverTimestamp() }, { merge: true });
    let threadFinalizeApplied = false;
    if (fin) {
      const p = fin;
      const patch = {
        status: p.newStatus, lastAutoDecision: p.decision, lastAutoDecisionAt: FV.serverTimestamp(),
        aiConfidence: p.aiConfidence != null ? p.aiConfidence : null, aiDifficulty: p.aiDifficulty != null ? p.aiDifficulty : null,
        aiDraftStatus: "ready", latestDraftId: "draft_" + threadId, updatedAt: FV.serverTimestamp()
      };
      if (typeof p.inboundMs === "number" && p.inboundMs > 0) patch.lastAutoProcessedInboundAt = TS.fromMillis(p.inboundMs);
      try {
        const td = fSnap && fSnap.exists ? (fSnap.data() || {}) : {};
        const rush = td.productionRush;
        if (td.salesCompletedAt) { delete patch.status; delete patch.aiConfidence; delete patch.aiDifficulty; }
        else if (rush && rush.acceptedAt && !rush.removedAt) delete patch.status;
      } catch (e) { /* keep the patch */ }
      tx.set(finRef, Object.assign(patch, fin.threadId === threadId ? { sendQueueState: "queued" } : {}), { merge: true });
      if (fin.threadId !== threadId && tSnap.exists) tx.set(tRef, { sendQueueState: "queued", updatedAt: FV.serverTimestamp() }, { merge: true });
      threadFinalizeApplied = true;
    } else if (tSnap.exists) {
      tx.set(tRef, { sendQueueState: "queued", updatedAt: FV.serverTimestamp() }, { merge: true });
    }
    const learnPrev = slot ? {
      text: slot.text || "", status: slot.status || null, generatedByAI: !!slot.generatedByAI,
      generatedBySalesAgent: !!slot.generatedBySalesAgent, aiConfidence: slot.aiConfidence,
      aiModel: slot.aiModel || null, aiActiveQuestion: slot.aiActiveQuestion || slot.activeQuestion || null,
      aiMissingFacts: Array.isArray(slot.aiMissingFacts) ? slot.aiMissingFacts : []
    } : null;
    // slotBusy: another message of this conversation is at the slot or waiting for it (its stand-in bubble is the one on show)
    const slotBusy = !!slot && (slot.status === "queued" || slot.status === "sending" || slot.queueWaiting === true);
    out = { item, created: true, deduped: false, learnPrev, threadFinalizeApplied, slotWasEmpty: !slot, slotBusy };
  });
  return out;
}

// ─── promote: the next message takes the lease and the slot ───────────────

/** What the extension reads: the old enqueue payload, plus the queue's marks. */
function slotPayload(item, prev, attemptsDone, now, parkedCopy, others) {
  others = Array.isArray(others) ? others : [];
  const p = {
    draftId: item.draftId, threadId: item.threadId, etsyConversationUrl: item.conversationUrl,
    text: item.text, attachments: item.attachments || [], status: "queued",
    createdBy: item.createdBy || (prev && prev.createdBy) || null, sendOrigin: item.origin,
    generatedByAI: !!item.generatedByAI, aiModel: item.aiModel || null, aiReasoning: item.aiReasoning || null,
    aiActiveQuestion: item.aiActiveQuestion || null,
    queuedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp(),
    sendSessionId: null, sendClaimedAt: null, sendHeartbeatAt: null, sendAttempts: attemptsDone,
    sendError: null, sendErrorCode: null, sendPartialSuccess: false, sendStage: "pre_click", sentAt: null, sendReportedAtMs: null,
    queueSendId: item.sendId, queueLeaseId: null, queueRetrying: FV.delete(), queueReleased: FV.delete(),
    queueWaiting: others.length ? true : FV.delete(),
    queueWaitingOrigin: others.length ? (others.some(o => o.origin === "manual") ? "manual" : "auto") : FV.delete(),
    queueWaitingSendId: others.length ? others[0].sendId : FV.delete()
  };
  // what went out last in this conversation stays on the slot for the "never send the same automated words twice" guard
  if (prev && SENT_SLOT.has(prev.status)) {
    p.lastSentText = prev.text || ""; p.lastSentAt = prev.sentAt || null; p.lastSentStatus = prev.status;
  }
  if (!prev) p.createdAt = FV.serverTimestamp();
  if (item.orderLink) {
    p.orderLink = item.orderLink;
    p.orderLinkParked = parkedCopy ? parkedCopy(prev) : { none: true };
    p.generatedByAI = false; p.aiModel = null; p.aiReasoning = null; p.aiActiveQuestion = null;
  } else {
    p.orderLink = FV.delete(); p.orderLinkParked = FV.delete();
  }
  return p;
}

/**
 * Start the next message if the lease is free. Safe to call from anywhere, any number of times at once.
 * Resolves { promoted: sendId|null, why }.
 */
async function pump(opts = {}) {
  const cfg = await getConfig();
  let now = nowMs();
  if (await isPaused()) return { promoted: null, why: "paused" };
  // 1. a dead or finished holder is dealt with first
  const readLease = async () => { const s0 = await metaRef("lease").get(); return s0.exists ? s0.data() : {}; };
  let lease = await readLease();
  if (lease.holderSendId) {
    if (now <= (lease.leaseUntilMs || 0)) return { promoted: null, why: "busy" };
    await resolveHolder({ reason: "expired" });
    now = nowMs();
    lease = await readLease();
    if (lease.holderSendId && now <= (lease.leaseUntilMs || 0)) return { promoted: null, why: "busy" };
  }
  // 2. who is next
  const q = await db.collection(Q_COLL).where("state", "==", "queued").limit(120).get();
  const items = q.docs.map(d => d.data());
  if (!items.length) return { promoted: null, why: "empty" };
  const aged = await failForgotten(items, now, cfg);
  const live = items.filter(i => !aged.has(i.sendId));
  const next = pickNext(live, now, cfg);
  if (!next) return { promoted: null, why: "nothing_due" };
  // the slot is shown to the extension only shortly before the pause ends (its tab-opening poll counts every look)
  if ((lease.paceUntilMs || 0) - now > PROMOTE_AHEAD_MS) return { promoted: null, why: "pacing", wakeAtMs: lease.paceUntilMs - PROMOTE_AHEAD_MS };
  // 3. the promotion: one transaction on the lease
  const parked = (() => { try { return require("./_etsyMailOrderLink").parkedCopy; } catch (e) { return null; } })();
  let result = null;
  await db.runTransaction(async tx => {
    result = null;
    const ls = await tx.get(metaRef("lease"));
    const is = await tx.get(qRef(next.sendId));
    const ss = await tx.get(slotRef(next.threadId));
    const L = ls.exists ? ls.data() : {};
    const it = is.exists ? is.data() : null;
    const slot = ss.exists ? ss.data() : null;
    const t = nowMs();
    if (L.holderSendId && t <= (L.leaseUntilMs || 0)) { result = { promoted: null, why: "busy" }; return; }
    if (L.holderSendId) { result = { promoted: null, why: "stale_holder" }; return; }     // an expired holder is resolved first, then we try again
    if (!it || it.state !== "queued" || (it.notBeforeMs || 0) > t) { result = { promoted: null, why: "changed" }; return; }
    // a send that does not belong to the queue (an older build, or a person's hand) is still in this slot
    if (slot && (slot.status === "queued" || slot.status === "sending") && !slot.queueSendId) { result = { promoted: null, why: "legacy_in_flight" }; return; }
    const leaseId = (L.leaseId || 0) + 1;
    const others = live.filter(i => i.threadId === next.threadId && i.sendId !== next.sendId);
    const payload = slotPayload(it, slot, it.attempts || 0, t, parked, others);
    payload.queueLeaseId = leaseId;
    tx.set(slotRef(it.threadId), payload, { merge: true });
    tx.set(qRef(it.sendId), {
      state: "claimed", leaseId, claimedAtMs: t, stage: "pre_click", sessionId: null, updatedAtMs: t,
      events: eventsAppend(it.events, "claimed", "taken from the queue, waiting for the Etsy helper", t)
    }, { merge: true });
    tx.set(metaRef("lease"), {
      holderSendId: it.sendId, holderSession: null, leaseId, leaseUntilMs: Math.max(t, L.paceUntilMs || 0) + cfg.claimTtlMs, heartbeatAtMs: t, startedAtMs: t,
      holderThreadId: it.threadId
    }, { merge: true });
    tx.set(metaRef("rev"), { n: FV.increment(1), atMs: t }, { merge: true });
    result = { promoted: it.sendId, why: "ok", item: Object.assign({}, it, { state: "claimed", leaseId }), prevSlot: slot, wasFirstOfThread: true };
  });
  if (result && result.promoted) {
    await afterPromote(result);
  } else if (result && result.why === "stale_holder") {
    await resolveHolder({ reason: "expired" });
  }
  return { promoted: result && result.promoted || null, why: result ? result.why : "none" };
}

let _lazyPumpAt = 0;
/** Pump, but not more than once per `minMs` in this process: the cheap way for a page's poll or an extension peek to keep the queue moving. */
async function lazyPump(minMs) {
  const now = nowMs();
  if (now - _lazyPumpAt < minMs) return null;
  _lazyPumpAt = now;
  return pump().catch(e => { console.warn("sendQueue lazy pump:", e.message); return null; });
}

async function afterPromote(r) {
  const it = r.item;
  // the stand-in bubble "syncing..." in the inbox, for the message now at the slot
  try {
    const { buildOptimisticDoc } = require("./etsyMailOptimisticMessage");
    const doc = buildOptimisticDoc({ draftId: it.draftId, text: it.text, employeeName: it.createdBy || "AI", attachments: it.attachments || [] });
    await db.collection(THREADS).doc(it.threadId).collection("messages").doc("optim_" + it.draftId).set(doc, { merge: false });
  } catch (e) { /* best effort */ }
  try { await require("./_etsyMailOrderLink").onQueueChange(Object.assign({}, it, { state: "claimed" })); } catch (e) { /* best effort */ }
}

/** A message that has waited past offlineWaitMs while the helper is silent is given up on (HELPER_OFFLINE), visibly. */
async function failForgotten(items, now, cfg) {
  const gone = new Set();
  const old = items.filter(i => now - (i.createdAtMs || 0) > cfg.offlineWaitMs);
  if (!old.length) return gone;
  let quiet = false;
  try {
    const [h, l] = await Promise.all([db.collection("EtsyMail_OrderLinkMeta").doc("helper").get(), metaRef("lease").get()]);
    const seen = h.exists ? Number(h.data().seenAtMs) || 0 : 0;
    const prog = l.exists ? Number(l.data().lastProgressAtMs) || 0 : 0;
    quiet = now - Math.max(seen, prog) > cfg.helperQuietMs;
  } catch (e) { quiet = false; }
  if (!quiet) return gone;
  for (const it of old) {
    try {
      const done = await finishDead(it.sendId, "failed", "HELPER_OFFLINE", plainReason("HELPER_OFFLINE"), { onlyFrom: new Set(["queued"]) });
      if (done) gone.add(it.sendId);
    } catch (e) { /* next pass */ }
  }
  return gone;
}

// ─── lease repair and slot-driven outcomes ─────────────────────────────────

/**
 * Looks at the lease holder and makes the books true: follows the slot when the extension has
 * reported (sent, failed, cancelled), or applies the expiry rules when it has gone silent.
 * opts.reason is for the event log only. Idempotent; safe to call at any time.
 */
async function resolveHolder(opts = {}) {
  const cfg = await getConfig();
  let outcome = null;
  const reaper = require_lazy_demote();
  await db.runTransaction(async tx => {
    outcome = null;
    const ls = await tx.get(metaRef("lease"));
    const L = ls.exists ? ls.data() : {};
    const sendId = opts.sendId || L.holderSendId;
    if (!sendId) return;
    const is = await tx.get(qRef(sendId));
    const it = is.exists ? is.data() : null;
    if (!it) {
      if (L.holderSendId === sendId) { tx.set(metaRef("lease"), releaseFields(L, nowMs(), 0, "missing", cfg), { merge: true }); outcome = { released: true }; }
      return;
    }
    const ss = await tx.get(slotRef(it.threadId));
    const slot = ss.exists ? ss.data() : null;
    const now = nowMs();
    const mine = slot && slot.queueSendId === sendId;
    const holds = L.holderSendId === sendId;
    // the thread doc, only when a terminal state may need it
    const ts = await tx.get(threadRef(it.threadId));
    const thread = ts.exists ? ts.data() : null;
    const sessionOfSlot = mine ? slot.sendSessionId : null;
    const sessions = Array.isArray(it.sessions) ? it.sessions : [];
    const knownSession = !sessionOfSlot || sessionOfSlot === it.sessionId || sessions.includes(sessionOfSlot);

    // 1. the extension reported a send
    const activeNow = it.state === "claimed" || it.state === "sending";
    const reportedAfter = !!slot && (slot.sendReportedAtMs || 0) > 0 && slot.sendReportedAtMs >= (it.updatedAtMs || 0);   // the helper's report arrived after the message was already put back or closed
    if (mine && slot.status === "sent_unverified" && slot.sendErrorCode === "STRANDED_POST_CLICK" && activeNow) {
      // the helper went silent after the click: probably sent, never re-sent, a person decides
      const cls = classifyFail({ errorCode: "STRANDED_POST_CLICK", error: slot.sendError, retry: false, stage: "post_click", attempts: it.attempts || 0, cfg });
      outcome = applyClassified(tx, it, L, holds, cls, now, cfg, thread, slot);
      outcome.silence = true;
      return;
    }
    if (mine && SENT_SLOT.has(slot.status) && knownSession && !DONE_STATES.has(it.state) && (activeNow || reportedAfter)) {
      const positive = slot.status === "sent";
      const patch = {
        state: positive ? "confirmed" : "sent", open: !positive ? true : false, sentAtMs: tsMs(slot.sentAt) || now,
        updatedAtMs: now, stage: "post_click",
        unverified: slot.status === "sent_unverified" ? true : FV.delete(), partial: slot.status === "sent_text_only" ? true : FV.delete(),
        lastError: FV.delete(), lastErrorCode: FV.delete(), plain: slot.status === "sent_text_only" ? (slot.sendNote ? "Text sent. " + String(slot.sendNote).slice(0, 160) : "The text went, but a picture could not be attached.") : FV.delete(),
        events: eventsAppend(it.events, positive ? "confirmed" : "sent", positive ? "Etsy confirmed" : (slot.status === "sent_text_only" ? "text sent, pictures not attached" : "clicked Send, no confirmation seen"), now)
      };
      if (positive) { patch.confirmedAtMs = now; patch.confirmedBy = "extension"; }
      if (slot.status === "sent_unverified") patch.open = false;       // it went; confirmation, if any, comes from the conversation
      if (slot.status === "sent_text_only") patch.open = false;
      tx.set(qRef(sendId), patch, { merge: true });
      if (holds) tx.set(metaRef("lease"), releaseFields(L, now, gapFor(cfg), "sent", cfg, true), { merge: true });
      tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
      outcome = { item: applied(it, patch), kind: positive ? "confirmed" : "sent", released: holds, thread };
      return;
    }

    // 2. the extension reported a failure (or the slot says so)
    if (mine && slot.status === "failed" && knownSession && (it.state === "claimed" || it.state === "sending") && !DONE_STATES.has(it.state)) {
      const stage = it.stage === "post_click" || slot.sendStage === "post_click" ? "post_click" : "pre_click";
      const code = slot.sendErrorOriginalCode || slot.sendErrorCode || null;
      const cls = classifyFail({ errorCode: code, error: slot.sendError, retry: opts.retry === true || slot.queueRetry === true, stage, attempts: it.attempts || 0, cfg });
      outcome = applyClassified(tx, it, L, holds, cls, now, cfg, thread, slot);
      return;
    }

    // 3. cancelled at the slot
    if (mine && slot.status === "draft" && it.state === "claimed") {
      const patch = { state: "cancelled", open: false, updatedAtMs: now, lastErrorCode: "CANCELLED", plain: plainReason("CANCELLED"), events: eventsAppend(it.events, "cancelled", "taken back before the helper started", now) };
      tx.set(qRef(sendId), patch, { merge: true });
      if (holds) tx.set(metaRef("lease"), releaseFields(L, now, 0, "cancelled", cfg), { merge: true });
      tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
      outcome = { item: applied(it, patch), kind: "cancelled", released: holds, thread };
      return;
    }

    // 5. a "queued" mark left on the slot by a message that no longer holds the turn (cancelled, put back, finished)
    if (mine && slot.status === "queued" && !(holds && activeNow)) {
      tx.set(slotRef(it.threadId), { status: "draft", queuedAt: null, queueReleased: true, updatedAt: FV.serverTimestamp() }, { merge: true });
      outcome = { item: it, kind: "tidied", released: false, thread };
      return;
    }

    // 4. silence
    if (holds && now > (L.leaseUntilMs || 0) && (it.state === "claimed" || it.state === "sending")) {
      if (it.state === "claimed") {
        const misses = (it.helperMisses || 0) + 1;
        const delay = Math.min(2 * MIN, 15000 * misses);
        const patch = {
          state: "queued", helperMisses: misses, notBeforeMs: now + delay, lastErrorCode: "NO_HELPER", plain: plainReason("NO_HELPER"),
          updatedAtMs: now, sessionId: null, events: eventsAppend(it.events, "queued", "the Etsy helper did not pick it up; back in the queue", now)
        };
        tx.set(qRef(sendId), patch, { merge: true });
        if (mine && (slot.status === "queued")) tx.set(slotRef(it.threadId), { status: "failed", sendError: patch.plain, sendErrorCode: "NO_HELPER", queueRetrying: true, updatedAt: FV.serverTimestamp() }, { merge: true });
        tx.set(metaRef("lease"), releaseFields(L, now, 0, "no_helper", cfg), { merge: true });
        tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
        outcome = { item: applied(it, patch), kind: "requeued", released: true, thread };
        return;
      }
      // sending, silent
      const stage = it.stage === "post_click" || (mine && slot.sendStage === "post_click") ? "post_click" : "pre_click";
      const cls = classifyFail({ errorCode: stage === "post_click" ? "STRANDED_POST_CLICK" : "CLAIM_ABANDONED", error: "", retry: true, stage, attempts: it.attempts || 0, cfg });
      outcome = applyClassified(tx, it, L, holds, cls, now, cfg, thread, slot);
      outcome.silence = true;
      return;
    }
  });
  if (outcome) await afterChange(outcome, reaper);
  return outcome;
}
function require_lazy_demote() { try { return require("./etsyMailDraftSend"); } catch (e) { return null; } }

function releaseFields(L, now, gapMs, outcome, cfg, progress) {
  return {
    holderSendId: null, holderSession: null, holderThreadId: null, leaseUntilMs: 0, heartbeatAtMs: now,
    paceUntilMs: gapMs > 0 ? now + gapMs : (L && L.paceUntilMs) || 0, lastOutcome: outcome, lastReleaseAtMs: now,
    lastProgressAtMs: progress || outcome === "sent" || outcome === "failed" ? now : ((L && L.lastProgressAtMs) || 0)
  };
}

/** Writes a failure's consequence: back in the queue with a delay, or a dead letter. Called inside a transaction. */
function applyClassified(tx, it, L, holds, cls, now, cfg, thread, slot) {
  const sendId = it.sendId;
  let patch;
  if (cls.action === "retry") {
    patch = {
      state: "queued", notBeforeMs: now + cls.delayMs, lastErrorCode: cls.code, lastError: cls.plain, plain: cls.plain, updatedAtMs: now, sessionId: null,
      events: eventsAppend(it.events, "queued", "will try again in " + Math.round(cls.delayMs / 1000) + " s: " + cls.plain, now)
    };
  } else if (cls.action === "attention") {
    patch = {
      state: "needs_attention", open: true, lastErrorCode: cls.code, lastError: cls.plain, plain: cls.plain, updatedAtMs: now,
      events: eventsAppend(it.events, "needs_attention", cls.plain, now)
    };
  } else {
    patch = {
      state: "failed", open: true, lastErrorCode: cls.code, lastError: cls.plain, plain: cls.plain, updatedAtMs: now,
      events: eventsAppend(it.events, "failed", cls.plain, now)
    };
  }
  tx.set(qRef(sendId), patch, { merge: true });
  const mine = slot && slot.queueSendId === sendId;
  if (mine) {
    if (cls.action === "attention") tx.set(slotRef(it.threadId), { status: "sent_unverified", sentAt: FV.serverTimestamp(), sendError: cls.plain, sendErrorCode: "STRANDED_POST_CLICK", queueRetrying: FV.delete(), updatedAt: FV.serverTimestamp() }, { merge: true });
    else tx.set(slotRef(it.threadId), { status: "failed", sendError: cls.plain, sendErrorCode: cls.code, queueRetrying: cls.action === "retry" ? true : FV.delete(), updatedAt: FV.serverTimestamp() }, { merge: true });
  }
  if (holds) tx.set(metaRef("lease"), releaseFields(L, now, gapFor(cfg), cls.action === "retry" ? "retry" : "failed", cfg, cls.action !== "retry"), { merge: true });
  tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
  return { item: applied(it, patch), kind: cls.action === "retry" ? "requeued" : cls.action === "attention" ? "needs_attention" : "failed", released: holds, thread, code: cls.code };
}

/** After a transition: the conversation's flags, the sorter's copy, the thread's status, the next message. */
async function afterChange(o, reaper) {
  if (o.kind === "tidied") return;
  const it = o.item;
  if (o.kind === "needs_attention" && await alreadyDelivered(it)) {
    // the stored conversation already shows the message: it went
    await markSent(it.sendId, { by: "the conversation", viaConversation: true });
    return;
  }
  try { await refreshThreadFlags(it.threadId); } catch (e) { console.warn("sendQueue thread flags:", e.message); }
  if (o.kind === "needs_attention" || o.kind === "failed") {
    try {
      const mod = reaper || require_lazy_demote();
      if (mod && mod.demoteThreadStandalone) await mod.demoteThreadStandalone(it.threadId, o.kind === "failed" ? "human_review_after_send_failure" : "human_review_after_stranded_post_click");
    } catch (e) { console.warn("sendQueue demote:", e.message); }
  }
  try { await require("./_etsyMailOrderLink").onQueueChange(it); } catch (e) { console.warn("sendQueue orderLink:", e.message); }
  if (o.released || o.kind === "requeued") { try { await pump(); } catch (e) { console.warn("sendQueue pump:", e.message); } }
}

/** The inbox's conversation flags, from the queue: one small query. */
async function refreshThreadFlags(threadId) {
  if (!isThreadId(threadId)) return;
  const q = await db.collection(Q_COLL).where("threadId", "==", threadId).limit(40).get();
  const list = q.docs.map(d => d.data());
  const active = list.some(i => OPEN_STATES.has(i.state));
  const problem = list.some(i => DEAD_STATES.has(i.state) && i.open);
  const unconf = list.some(i => i.state === "sent" || (i.state === "needs_attention" && i.open));
  try {
    await threadRef(threadId).update({
      sendQueueState: active ? "queued" : FV.delete(), sendQueueProblem: problem ? true : FV.delete(),
      sendQueueUnconfirmed: unconf ? true : FV.delete(), updatedAt: FV.serverTimestamp()
    });
  } catch (e) { /* the conversation is not stored (yet): nothing to flag */ }
  if (!list.some(i => i.state === "queued")) {
    try { await slotRef(threadId).update({ queueWaiting: FV.delete(), queueWaitingOrigin: FV.delete(), queueWaitingSendId: FV.delete() }); }
    catch (e) { /* no slot: nothing waits on it */ }
  }
}

/** A queued message given up on without ever touching the slot. */
async function finishDead(sendId, state, code, plain, { onlyFrom } = {}) {
  let item = null;
  await db.runTransaction(async tx => {
    item = null;
    const is = await tx.get(qRef(sendId));
    if (!is.exists) return;
    const it = is.data();
    if (onlyFrom && !onlyFrom.has(it.state)) return;
    const now = nowMs();
    const patch = { state, open: true, lastErrorCode: code, lastError: plain, plain, updatedAtMs: now, events: eventsAppend(it.events, state, plain, now) };
    tx.set(qRef(sendId), patch, { merge: true });
    tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
    item = applied(it, patch);
  });
  if (item) await afterChange({ item, kind: state === "failed" ? "failed" : "needs_attention", released: false });
  return !!item;
}

// ─── gates used inside etsyMailDraftSend's transactions ─────────────────────

/**
 * The extension asks to claim the slot. Reads (the lease and the message) happen here, so call it after
 * the slot has been read and before anything is written. Resolves { ok, patch } or { reject }.
 * slot: the slot's data. legacy slots (no queueSendId) are taken into the queue when the lease is free.
 */
async function claimGate(tx, { slot, draftId, sessionId, workerId }) {
  const cfg = await getConfig();
  const now = nowMs();
  const ls = await tx.get(metaRef("lease"));
  const L = ls.exists ? ls.data() : {};
  const reject = (status, errorCode, error, extra) => ({ reject: Object.assign({ status, errorCode, error }, extra || {}) });
  if (!slot.queueSendId) {
    // adopt: a message that arrived without the queue (an older build wrote it) becomes the lease holder, or waits
    if (L.holderSendId && now <= (L.leaseUntilMs || 0)) return reject(409, "QUEUE_BUSY", "Another message is being sent right now; this one goes next.", { retryAfterMs: 5000 });
    if (now < (L.paceUntilMs || 0)) return reject(429, "PACING", "Pausing between sends.", { retryAfterMs: Math.min(60000, L.paceUntilMs - now + 250) });
    const sendId = "q_legacy_" + sha1(draftId + ":" + (tsMs(slot.queuedAt) || now)).slice(0, 18);
    const leaseId = (L.leaseId || 0) + 1;
    const item = clean({
      sendId, idemKey: "legacy:" + draftId, derived: true, threadId: slot.threadId, draftId, conversationUrl: slot.etsyConversationUrl,
      text: String(slot.text || ""), attachments: Array.isArray(slot.attachments) ? slot.attachments : [], source: "legacy", origin: slot.sendOrigin === "auto" ? "auto" : "manual",
      priority: 0, createdBy: slot.createdBy || null, createdAtMs: tsMs(slot.queuedAt) || now, epoch: 0, orderLink: slot.orderLink || null,
      state: "sending", open: true, attempts: 1, notBeforeMs: 0, stage: "pre_click", helperMisses: 0, sessionId, leaseId, claimedAtMs: now,
      sessions: [sessionId], events: eventsAppend([], "sending", "taken into the queue by the helper", now), updatedAtMs: now
    });
    tx.set(qRef(sendId), item, { merge: false });
    tx.set(metaRef("lease"), { holderSendId: sendId, holderSession: sessionId, leaseId, leaseUntilMs: now + cfg.sendTtlMs, heartbeatAtMs: now, startedAtMs: now, holderThreadId: slot.threadId }, { merge: true });
    tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
    return { ok: true, adopted: true, attempts: 1, patch: { queueSendId: sendId, queueLeaseId: leaseId } };
  }
  const is = await tx.get(qRef(slot.queueSendId));
  const it = is.exists ? is.data() : null;
  if (!it || !(OPEN_STATES.has(it.state))) return reject(409, "QUEUE_NOT_ACTIVE", "This message is no longer waiting to be sent.");
  if (slot.status !== "queued" && slot.status !== "sending") return reject(409, "QUEUE_NOT_ACTIVE", "This message is no longer waiting to be sent.");
  if (L.holderSendId !== it.sendId || (slot.queueLeaseId != null && L.leaseId !== slot.queueLeaseId)) {
    return reject(409, "QUEUE_NOT_YOURS", "Another message has the turn.", { retryAfterMs: 5000 });
  }
  if (it.state === "claimed") {
    if (now < (L.paceUntilMs || 0)) return reject(429, "PACING", "Pausing between sends.", { retryAfterMs: Math.min(60000, L.paceUntilMs - now + 250) });
  } else if (it.state === "sending") {
    // only a silent holder can be taken over, and never after the click
    if (now <= (L.leaseUntilMs || 0)) return reject(409, "QUEUE_TAKEN", "Another helper tab is sending this message.");
    if (it.stage === "post_click") return reject(410, "STRANDED_POST_CLICK", "Previous attempt clicked Send and went silent. Check the conversation on Etsy.");
  } else return reject(409, "QUEUE_NOT_YOURS", "Another message has the turn.");
  if ((it.attempts || 0) >= cfg.maxAttempts) return reject(410, "MAX_ATTEMPTS", "This message used up its tries.");
  const attempts = (it.attempts || 0) + 1;
  const sessions = (Array.isArray(it.sessions) ? it.sessions : []).concat(sessionId).slice(-6);
  tx.set(qRef(it.sendId), {
    state: "sending", sessionId, attempts, attemptStartedAtMs: now, stage: "pre_click", updatedAtMs: now, sessions,
    events: eventsAppend(it.events, "sending", "the Etsy helper started" + (attempts > 1 ? " (try " + attempts + ")" : ""), now)
  }, { merge: true });
  tx.set(metaRef("lease"), { holderSession: sessionId, leaseUntilMs: now + cfg.sendTtlMs, heartbeatAtMs: now }, { merge: true });
  tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
  return { ok: true, attempts, patch: {} };
}

/** The extension is alive: renew the lease. { ok } or { reject }. */
async function heartbeatGate(tx, { slot, sessionId }) {
  if (!slot.queueSendId) return { ok: true };
  const cfg = await getConfig();
  const now = nowMs();
  const ls = await tx.get(metaRef("lease"));
  const L = ls.exists ? ls.data() : {};
  const is = await tx.get(qRef(slot.queueSendId));
  const it = is.exists ? is.data() : null;
  if (!it || it.state !== "sending" || it.sessionId !== sessionId || L.holderSendId !== it.sendId) {
    return { reject: { status: 403, errorCode: "LEASE_LOST", error: "This message was taken over or finished." } };
  }
  tx.set(metaRef("lease"), { heartbeatAtMs: now, leaseUntilMs: now + cfg.sendTtlMs }, { merge: true });
  return { ok: true };
}

/** The fence in front of Etsy's Send button. Only the holder's session, only once. */
async function clickGate(tx, { slot, sessionId }) {
  if (!slot.queueSendId) return { ok: true };
  const cfg = await getConfig();
  const now = nowMs();
  const ls = await tx.get(metaRef("lease"));
  const L = ls.exists ? ls.data() : {};
  const is = await tx.get(qRef(slot.queueSendId));
  const it = is.exists ? is.data() : null;
  if (!it || it.state !== "sending" || it.sessionId !== sessionId || L.holderSendId !== it.sendId) {
    return { reject: { status: 403, errorCode: "LEASE_LOST", error: "mark_clicked from a session that no longer holds the turn." } };
  }
  if (it.stage === "post_click") return { reject: { status: 409, errorCode: "ALREADY_CLICKED", error: "Send was already clicked for this message." } };
  tx.set(qRef(it.sendId), { stage: "post_click", clickedAtMs: now, updatedAtMs: now, events: eventsAppend(it.events, "sending", "clicked Etsy's Send", now) }, { merge: true });
  tx.set(metaRef("lease"), { heartbeatAtMs: now, leaseUntilMs: now + cfg.sendTtlMs }, { merge: true });
  return { ok: true };
}

/** Does the dispatcher own this slot? (A slot it wrote carries the message's id.) */
const isManaged = slot => !!(slot && slot.queueSendId);

/**
 * The extension looks at a slot that says "queued". For a slot the dispatcher wrote: is it really this message's turn?
 * One lease read. A holder that went silent is dealt with here, so a peek is also a repair. { live } is false when
 * the extension must be told there is nothing to send.
 */
async function peekGate(slot) {
  const ls = await metaRef("lease").get();
  const L = ls.exists ? ls.data() : {};
  const now = nowMs();
  if (L.holderSendId === slot.queueSendId && (slot.queueLeaseId == null || L.leaseId === slot.queueLeaseId)) {
    if (now <= (L.leaseUntilMs || 0)) return { live: true };
    try { await resolveHolder({ reason: "peek" }); } catch (e) { console.warn("sendQueue peek repair:", e.message); }
    return { live: false };
  }
  // a mark left behind (the message was cancelled, put back or finished): tidy it
  try { await resolveHolder({ sendId: slot.queueSendId, reason: "peek_stale" }); } catch (e) { console.warn("sendQueue peek tidy:", e.message); }
  return { live: false };
}

/**
 * After complete / fail / cancel / reaper changed a slot: make the queue follow it. Safe to repeat.
 * info: { retry } from the extension's fail report.
 */
async function settleSlot(draftId, info = {}) {
  const s = await db.collection(DRAFTS).doc(String(draftId)).get();
  const slot = s.exists ? s.data() : null;
  if (!slot || !slot.queueSendId) return { managed: false };
  const o = await resolveHolder({ sendId: slot.queueSendId, retry: info.retry === true });
  return { managed: true, outcome: o ? o.kind : null };
}

// ─── people's actions ──────────────────────────────────────────────────────

/** Take a message back that has not been sent. */
async function cancel(sendId, by) {
  let out = null;
  await db.runTransaction(async tx => {
    out = null;
    const is = await tx.get(qRef(sendId));
    if (!is.exists) { out = { notFound: true }; return; }
    const it = is.data();
    if (it.state !== "queued" && it.state !== "claimed") { out = { tooLate: true, state: it.state }; return; }
    const ls = await tx.get(metaRef("lease"));
    const L = ls.exists ? ls.data() : {};
    const ss = await tx.get(slotRef(it.threadId));
    const slot = ss.exists ? ss.data() : null;
    const ts = await tx.get(threadRef(it.threadId));
    // a claimed job may have been claimed by the extension in the meantime: then it is too late
    const now = nowMs();
    const patch = { state: "cancelled", open: false, updatedAtMs: now, lastErrorCode: "CANCELLED", plain: plainReason("CANCELLED"), cancelledBy: by || null, events: eventsAppend(it.events, "cancelled", "cancelled" + (by ? " by " + by : ""), now) };
    tx.set(qRef(sendId), patch, { merge: true });
    if (it.state === "claimed" && L.holderSendId === sendId) {
      tx.set(metaRef("lease"), releaseFields(L, now, 0, "cancelled", loadedCfg()), { merge: true });
      if (slot && slot.queueSendId === sendId && slot.status === "queued") tx.set(slotRef(it.threadId), { status: "draft", queuedAt: null, updatedAt: FV.serverTimestamp() }, { merge: true });
    }
    tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
    out = { item: applied(it, patch), kind: "cancelled", released: it.state === "claimed" && L.holderSendId === sendId, thread: ts.exists ? ts.data() : null };
  });
  if (out && out.item) await afterChange(out);
  return out;
}
const loadedCfg = () => _cfg.value || DEFAULTS;

/** Put a dead letter back in the queue. A message that may have gone is checked for first. */
async function humanRetry(sendId, { confirmMaybeSent = false, by = null } = {}) {
  const it0 = (await qRef(sendId).get());
  if (!it0.exists) return { notFound: true };
  const first = it0.data();
  if (!DEAD_STATES.has(first.state)) return { notDead: true, state: first.state };
  const maybe = first.state === "needs_attention" || first.stage === "post_click" || first.unverified;
  if (maybe) {
    const hit = await alreadyDelivered(first);
    if (hit) { const r = await markSent(sendId, { by: "the conversation", viaConversation: true }); return Object.assign({ alreadyDelivered: true }, r); }
    if (!confirmMaybeSent) return { needsConfirm: true, item: first };
  }
  let out = null;
  await db.runTransaction(async tx => {
    out = null;
    const is = await tx.get(qRef(sendId));
    const it = is.data();
    if (!DEAD_STATES.has(it.state)) { out = { notDead: true, state: it.state }; return; }
    const now = nowMs();
    const patch = {
      state: "queued", open: true, attempts: 0, epoch: (it.epoch || 0) + 1, notBeforeMs: 0, stage: "pre_click", helperMisses: 0, sessionId: null,
      retriedBy: by || null, retriedAtMs: now, prevError: it.lastError || null, lastError: FV.delete(), lastErrorCode: FV.delete(), plain: FV.delete(),
      unverified: FV.delete(), updatedAtMs: now, events: eventsAppend(it.events, "queued", "sent again on request" + (by ? " by " + by : ""), now)
    };
    tx.set(qRef(sendId), patch, { merge: true });
    tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
    out = { item: applied(it, patch), kind: "requeued" };
  });
  if (out && out.item) { await afterChange(out); }
  return out || {};
}

/** A person says it went (they saw it on Etsy), or the conversation shows it. */
async function markSent(sendId, { by = null, viaConversation = false } = {}) {
  let out = null;
  await db.runTransaction(async tx => {
    out = null;
    const is = await tx.get(qRef(sendId));
    if (!is.exists) { out = { notFound: true }; return; }
    const it = is.data();
    if (!(DEAD_STATES.has(it.state) || it.state === "sent")) { out = { notDead: true, state: it.state }; return; }
    const now = nowMs();
    const patch = {
      state: "confirmed", open: false, confirmedAtMs: now, confirmedBy: by || "a person", sentAtMs: it.sentAtMs || now, updatedAtMs: now,
      lastError: FV.delete(), plain: FV.delete(), events: eventsAppend(it.events, "confirmed", viaConversation ? "found in the conversation" : "marked as sent" + (by ? " by " + by : ""), now)
    };
    tx.set(qRef(sendId), patch, { merge: true });
    tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
    out = { item: applied(it, patch), kind: "confirmed" };
  });
  if (out && out.item) await afterChange(out);
  return out || {};
}

async function dismiss(sendId, by) {
  let out = null;
  await db.runTransaction(async tx => {
    out = null;
    const is = await tx.get(qRef(sendId));
    if (!is.exists) { out = { notFound: true }; return; }
    const it = is.data();
    if (!DEAD_STATES.has(it.state)) { out = { notDead: true, state: it.state }; return; }
    const now = nowMs();
    tx.set(qRef(sendId), { open: false, dismissedBy: by || null, updatedAtMs: now, events: eventsAppend(it.events, it.state, "put away by " + (by || "a person"), now) }, { merge: true });
    tx.set(metaRef("rev"), { n: FV.increment(1), atMs: now }, { merge: true });
    out = { item: Object.assign({}, it, { open: false }), kind: it.state };
  });
  if (out && out.item) { try { await refreshThreadFlags(out.item.threadId); } catch (e) {} try { await require("./_etsyMailOrderLink").onQueueChange(out.item); } catch (e) {} }
  return out || {};
}

/** Is a message with these words already in the stored conversation (written after the message was made)? No Etsy call. */
async function alreadyDelivered(item) {
  try {
    const since = (item.attemptStartedAtMs || item.createdAtMs || 0) - 2 * MIN;
    const q = await db.collection(THREADS).doc(item.threadId).collection("messages")
      .where("timestamp", ">=", TS.fromMillis(Math.max(0, since))).orderBy("timestamp").limit(80).get();
    return q.docs.some(d => {
      const m = d.data() || {};
      return m.direction !== "inbound" && !String(d.id).startsWith("optim_") && !m.localOptimistic && sameText(m.text, item.text);
    });
  } catch (e) { return false; }
}

/** Hook for the scrape: our own message seen in the conversation confirms it (no re-scrape is ever queued). */
async function onThreadMessages(threadId, thread, fresh) {
  try {
    if (!isThreadId(threadId) || !thread || !thread.sendQueueUnconfirmed) return 0;
    const out = (Array.isArray(fresh) ? fresh : []).filter(m => m && m.direction !== "inbound" && !String(m.id || "").startsWith("optim_"));
    if (!out.length) return 0;
    const q = await db.collection(Q_COLL).where("threadId", "==", threadId).limit(40).get();
    let n = 0;
    for (const d of q.docs) {
      const it = d.data();
      if (it.state !== "sent" && !(it.state === "needs_attention" && it.open)) continue;
      const hit = out.find(m => (m.tsMs || 0) >= (it.sentAtMs || it.createdAtMs || 0) - 3 * MIN && sameText(m.text, it.text));
      if (!hit) continue;
      const r = await markSent(it.sendId, { by: "the conversation", viaConversation: true });
      if (r && r.item) n++;
    }
    return n;
  } catch (e) { console.warn("sendQueue onThreadMessages:", e.message); return 0; }
}

// ─── reading ───────────────────────────────────────────────────────────────

/**
 * What a watching page needs. { n } is the page's last revision: nothing changed -> { unchanged }.
 * opts: { n, hasOpen, threadId, withText }
 */
async function stateView(opts = {}) {
  const cfg = await getConfig();
  const rs = await metaRef("rev").get();
  const rev = rs.exists ? Number(rs.data().n) || 0 : 0;
  const now = nowMs();
  if (opts.n != null && Number(opts.n) === rev) {
    if (!opts.hasOpen) return { n: rev, unchanged: true, now };
    // a page waiting on a message also keeps the lease honest: a silent holder is dealt with here
    const ls = await metaRef("lease").get();
    const L = ls.exists ? ls.data() : {};
    if (L.holderSendId && now > (L.leaseUntilMs || 0)) await resolveHolder({ reason: "state_read" });
    else if (!L.holderSendId) await lazyPump(10000);
    const rs2 = await metaRef("rev").get();
    const rev2 = rs2.exists ? Number(rs2.data().n) || 0 : 0;
    if (rev2 === rev) return { n: rev, unchanged: true, now };
    return stateView(Object.assign({}, opts, { n: null, hasOpen: false }));
  }
  const [qs, ls, hs] = await Promise.all([
    db.collection(Q_COLL).where("open", "==", true).limit(150).get(),
    metaRef("lease").get(),
    db.collection("EtsyMail_OrderLinkMeta").doc("helper").get().catch(() => null)
  ]);
  const L = ls.exists ? ls.data() : {};
  const items = qs.docs.map(d => d.data());
  const view = viewItems(items, L, now, cfg, { withText: opts.withText !== false });
  let recent = [];
  try {
    const rq = await db.collection(Q_COLL).where("updatedAtMs", ">", now - 5 * MIN).limit(60).get();
    recent = rq.docs.map(d => d.data()).filter(i => !i.open).map(i => ({ id: i.sendId, t: i.threadId, st: i.state, at: i.updatedAtMs || 0, ol: i.orderLink && i.orderLink.i || null, oe: i.orderLink && i.orderLink.e || null, unverified: !!i.unverified, partial: !!i.partial }));
  } catch (e) { recent = []; }
  return {
    recent: opts.threadId ? recent.filter(r => r.t === opts.threadId) : recent,
    n: rev, now, gapMs: cfg.gapMs, helperSeenAtMs: hs && hs.exists ? Number(hs.data().seenAtMs) || 0 : 0,
    lease: { holder: L.holderSendId || null, until: L.leaseUntilMs || 0, paceUntil: L.paceUntilMs || 0, progressAt: L.lastProgressAtMs || 0 },
    items: opts.threadId ? view.filter(v => v.t === opts.threadId) : view
  };
}

/** For the health light: counts and the oldest wait. A failed read THROWS (never fake zeros): the health light catches it and
 *  says "could not look at the queue" instead of showing a green "nothing waiting". */
async function summary() {
  const now = nowMs();
  const [qs, ls, hs] = await Promise.all([
    db.collection(Q_COLL).where("open", "==", true).limit(300).get(),
    metaRef("lease").get(),
    db.collection("EtsyMail_OrderLinkMeta").doc("helper").get().catch(() => null)
  ]);
  const items = qs.docs.map(d => d.data());
  const L = ls.exists ? ls.data() : {};
  const count = s => items.filter(i => i.state === s).length;
  const queued = items.filter(i => i.state === "queued");
  const oldest = queued.reduce((m, i) => (m && m < i.createdAtMs ? m : i.createdAtMs), 0);
  const progress = Number(L.lastProgressAtMs) || 0;
  return {
    queued: count("queued"), claimed: count("claimed"), sending: count("sending"), failed: count("failed"), needsAttention: count("needs_attention"),
    oldestQueuedAtMs: oldest || 0, helperSeenAtMs: hs && hs.exists ? Number(hs.data().seenAtMs) || 0 : 0,
    lastProgressAtMs: progress, stalled: !!(oldest && now - oldest > 10 * MIN && now - progress > 10 * MIN),
    holder: L.holderSendId || null, leaseUntilMs: L.leaseUntilMs || 0, paceUntilMs: L.paceUntilMs || 0
  };
}

/** One message by id (for the sorter and tests). */
async function getItem(sendId) { const s = await qRef(sendId).get(); return s.exists ? s.data() : null; }

// ─── maintenance (the 3-minute cron, and lazily from other calls) ──────────

/**
 * The upkeep pass the 3-minute cron runs. An idle queue costs ONE read: the revision document says whether anything
 * changed since the last pass and when something becomes due by itself (a delayed retry, the next stale check).
 */
async function maintain() {
  const rs = await metaRef("rev").get();
  if (!rs.exists) return { skipped: true, never: true };                 // nothing has ever been queued
  const rv = rs.data() || {};
  const n0 = Number(rv.n) || 0;
  const now0 = nowMs();
  if (rv.maintN === n0 && now0 < (Number(rv.dueMs) || 0)) return { skipped: true };
  const cfg = await getConfig({ fresh: true });
  const stats = { resolved: false, promoted: null, pruned: 0, confirmed: 0 };
  const r = await resolveHolder({ reason: "maintain" }).catch(e => { console.warn("sendQueue maintain resolve:", e.message); return null; });
  stats.resolved = !!r;
  const p = await pump().catch(e => { console.warn("sendQueue maintain pump:", e.message); return null; });
  stats.promoted = p && p.promoted || null;
  let houseMs = Number(rv.houseMs) || 0;
  if (nowMs() >= houseMs) {
    // settle messages whose conversation now shows them (a scrape that did not carry the hook)
    try {
      const sent = await db.collection(Q_COLL).where("state", "==", "sent").limit(20).get();
      for (const d of sent.docs) {
        const it = d.data();
        if (nowMs() - (it.sentAtMs || 0) > 10 * MIN && await alreadyDelivered(it)) { await markSent(it.sendId, { by: "the conversation", viaConversation: true }); stats.confirmed++; }
      }
    } catch (e) { /* next time */ }
    // done messages older than keepDoneMs are removed; dead letters that are still open never are
    try {
      const old = await db.collection(Q_COLL).where("updatedAtMs", "<", nowMs() - cfg.keepDoneMs).limit(50).get();
      for (const d of old.docs) { if (d.data().open === false) { await d.ref.delete(); stats.pruned++; } }
    } catch (e) { /* a failure here is not fatal */ }
    houseMs = nowMs() + 10 * MIN;
  }
  // when to look again by itself: soon while anything is waiting or being sent, else at the next housekeeping
  let busy = !!(p && p.why !== "empty");
  try { const ls = await metaRef("lease").get(); if (ls.exists && ls.data().holderSendId) busy = true; } catch (e) { busy = true; }
  try { await metaRef("rev").set({ maintN: n0, dueMs: nowMs() + (busy ? 60 * 1000 : 10 * MIN), houseMs }, { merge: true }); } catch (e) { /* the next pass does it again */ }
  return stats;
}

module.exports = {
  // names
  Q_COLL, META, DRAFTS, DEFAULTS, LIMITS, OPEN_STATES, DEAD_STATES, DONE_STATES, RETRYABLE_CODES,
  // pure rules
  orderQueue, pickNext, backoffMs, gapFor, classifyFail, plainReason, deriveKey, sendIdFor, viewItems, normText, sameText, ordinal, cfgFrom,
  // io
  getConfig, setConfig, isPaused, isSandboxInput, submit, pump, resolveHolder, settleSlot, claimGate, heartbeatGate, clickGate, peekGate, isManaged,
  cancel, humanRetry, markSent, dismiss, alreadyDelivered, onThreadMessages, stateView, summary, getItem, maintain, refreshThreadFlags,
  // tests
  _setRandom, _resetCaches, lazyPump, applied
};
