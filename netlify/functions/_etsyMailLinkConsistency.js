/*  netlify/functions/_etsyMailLinkConsistency.js
 *
 *  The nightly check that what the inbox stores is what its conversations say. Run once a night by etsyMailLinkWatchdog
 *  (between 02:00 and 05:00 New York time, or whenever more than 40 hours have passed without a run).
 *
 *  For the 150 conversations changed most recently it compares what each conversation's own record claims with the messages
 *  actually stored under it:
 *    · claimed_but_none   the record counts messages, none are stored
 *    · fewer_than_counted the record counts at least 3 more messages than are stored
 *    · inbound_missing    the record says the customer wrote at a time later than the newest stored message
 *  A conversation still being read (created from a Gmail notice, or changed in the last 30 minutes) is left for the next night.
 *
 *  Self-healing: for up to 5 mismatched conversations it queues ONE re-read (a scrape job on a deterministic id, one per
 *  conversation per day, so a second run never queues a second one; a conversation re-read within the last 7 days is not
 *  queued again, so a count that can never match, such as one left over from a purge, does not cost a re-read every night). Nothing is deleted or rewritten here, no Etsy API call is
 *  made, and nothing is sent to a customer. The result is EtsyMail_Config/linkConsistency, which the link monitor shows as
 *  amber ("N conversations show fewer messages ...") until a later night finds none.
 *
 *  Cost: 150 record reads, 150 counts and 150 one-document reads a night (about 0.01 M operations a month).
 */
"use strict";

const admin = require("./firebaseAdmin");
const FV = admin.firestore.FieldValue;

const MIN = 60 * 1000, HOUR = 60 * MIN;
const WINDOW = 150, REPAIR_MAX = 5, REQUEUE_AFTER_MS = 7 * 24 * HOUR, KEEP_REQUEUED = 60, SLACK_MS = 3 * MIN, FRESH_MS = 30 * MIN, BUDGET_MS = 14000, SAMPLES = 10;
const THREADS = "EtsyMail_Threads", JOBS = "EtsyMail_Jobs", CFG = "EtsyMail_Config", DOC = "linkConsistency";

const ms = v => (v == null ? 0 : typeof v.toMillis === "function" ? v.toMillis() : typeof v === "object" && typeof v.ms === "number" ? v.ms : typeof v === "number" ? v : 0);
/** 02:00-05:00 in New York is 06:00-09:00 UTC in summer and 07:00-10:00 in winter: 07:00-09:00 UTC is inside the window all year. */
const nightUtc = now => { const h = new Date(now).getUTCHours(); return h >= 7 && h < 9; };

/** Judge one conversation; null when it matches (or is still being read). */
async function judge(db, id, t, now) {
  if (t.status === "detected_from_gmail" || now - ms(t.updatedAt) < FRESH_MS) return { skipped: true };
  const col = db.collection(THREADS).doc(id).collection("messages");
  const [c, newest] = await Promise.all([
    col.count().get().then(s => s.data().count),
    col.orderBy("timestamp", "desc").limit(1).select("timestamp").get().then(s => (s.empty ? 0 : ms(s.docs[0].data().timestamp)))
  ]);
  const claimed = typeof t.messageCount === "number" ? t.messageCount : null, lastIn = ms(t.lastInboundAt);
  if (claimed != null && claimed > 0 && c === 0) return { kind: "claimed_but_none", claimed, stored: c };
  if (claimed != null && claimed - c >= 3) return { kind: "fewer_than_counted", claimed, stored: c };
  if (lastIn > 0 && newest < lastIn - SLACK_MS) return { kind: "inbound_missing", claimed, stored: c };
  return null;
}

/** One re-read for a mismatched conversation: a scrape job on a deterministic id (one per conversation per day). */
async function queueReread(db, id, t, kind, now) {
  const url = t.etsyConversationUrl;
  if (!url) return null;
  const jobId = `consistency_${id}_${new Date(now).toISOString().slice(0, 10).replace(/-/g, "")}`;
  try {
    await db.collection(JOBS).doc(jobId).create({
      jobId, jobType: "scrape", status: "queued", threadId: id,
      payload: { etsyConversationUrl: url, source: "linkConsistency", gmailMessageId: null, gmailThreadId: t.gmailThreadId || null, rescrape: true, reason: kind },
      attempts: 0, claimedBy: null, claimedAt: null, lastError: null, lastHeartbeatAt: null, result: null,
      createdAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp()
    });
    return jobId;
  } catch (e) {
    if (e && (e.code === 6 || /already.?exists/i.test(String(e.message)))) return null;   // queued earlier today
    throw e;
  }
}

async function run(db, now, prev) {
  const t0 = Date.now();
  const snap = await db.collection(THREADS).orderBy("updatedAt", "desc").limit(WINDOW)
    .select("messageCount", "lastInboundAt", "updatedAt", "status", "etsyConversationUrl", "gmailThreadId").get();
  const rows = snap.docs.map(d => ({ id: d.id, t: d.data() || {} }));
  const out = { atMs: now, checked: 0, mismatched: 0, skippedFresh: 0, partial: false, repairQueued: 0, kinds: {}, samples: [] };
  let next = 0;
  const bad = [];
  await Promise.all(Array.from({ length: 8 }, async () => {
    while (next < rows.length) {
      if (Date.now() - t0 > BUDGET_MS) { out.partial = true; return; }
      const r = rows[next++];
      let v;
      try { v = await judge(db, r.id, r.t, now); } catch (e) { console.warn("linkConsistency", r.id, e && e.message); out.errors = (out.errors || 0) + 1; continue; }
      if (v && v.skipped) { out.skippedFresh++; continue; }
      out.checked++;
      if (v) { out.mismatched++; out.kinds[v.kind] = (out.kinds[v.kind] || 0) + 1; bad.push({ r, v }); if (out.samples.length < SAMPLES) out.samples.push({ threadId: r.id, kind: v.kind, claimed: v.claimed == null ? -1 : v.claimed, stored: v.stored }); }
    }
  }));
  // who was re-read lately (a map of conversation → time), carried from night to night and kept short
  const requeued = {};
  for (const [id, at] of Object.entries((prev && prev.requeued) || {})) if (now - at < REQUEUE_AFTER_MS) requeued[id] = at;
  const fresh = bad.filter(b => !requeued[b.r.id]);
  out.alreadyReread = bad.length - fresh.length;
  out.noAddress = fresh.filter(b => !b.r.t.etsyConversationUrl).length;
  out.unresolved = fresh.length - out.noAddress;   // what the light counts: not yet re-read, and able to be
  let queuedNow = 0;
  for (const { r, v } of fresh) {
    if (queuedNow >= REPAIR_MAX) break;
    try { if (await queueReread(db, r.id, r.t, v.kind, now)) { queuedNow++; requeued[r.id] = now; } } catch (e) { console.warn("linkConsistency re-read", r.id, e && e.message); }
  }
  out.repairQueued = queuedNow;
  out.requeued = Object.fromEntries(Object.entries(requeued).sort((a, b) => b[1] - a[1]).slice(0, KEEP_REQUEUED));
  return out;
}

/** Runs the check when it is due (night, a day since the last one) and records the result. Never throws for a missing record. */
async function maybeRun(db, { now = Date.now(), force = false } = {}) {
  const ref = db.collection(CFG).doc(DOC);
  const s = await ref.get();
  const prev = s.exists ? s.data() : null;
  const age = prev && prev.atMs ? now - prev.atMs : Infinity;
  // (the first run ever waits for a night: it may queue re-reads, which should not start in the middle of the working day)
  const due = force || (age > 23 * HOUR && nightUtc(now)) || (!!prev && age > 40 * HOUR);
  if (!due) return { ran: false };
  if (!force && prev && prev.startedAtMs && now - prev.startedAtMs < 10 * MIN) return { ran: false, busy: true };
  await ref.set({ startedAtMs: now }, { merge: true });
  let doc;
  try { doc = await run(db, now, prev); }
  catch (e) { await ref.set(Object.assign({}, prev || {}, { startedAtMs: 0, lastError: String(e && e.message).slice(0, 160), lastErrorAtMs: now })); throw e; }
  await ref.set(doc);
  return { ran: true, doc };
}

module.exports = { maybeRun, run, judge };
