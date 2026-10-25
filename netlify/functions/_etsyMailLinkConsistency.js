/*  netlify/functions/_etsyMailLinkConsistency.js
 *
 *  The check that what the inbox stores is what its conversations say, and the self-heal when it is not.
 *
 *  THE NIGHTLY SCAN (etsyMailLinkWatchdog runs it once a night, between 02:00 and 05:00 New York time, or whenever more than
 *  40 hours have passed without a run). For the 150 conversations changed most recently it compares what each conversation's own
 *  record claims with the messages actually stored under it:
 *    · claimed_but_none   the record counts messages, none are stored
 *    · fewer_than_counted the record counts at least 3 more messages than are stored
 *    · inbound_missing    the record says the customer wrote at a time later than the newest stored message
 *  A conversation still being read (created from a Gmail notice, or changed in the last 30 minutes) is left for the next pass.
 *  "Expected" is therefore the conversation record's own counter (messageCount, kept by every scrape and every send) and its
 *  lastInboundAt, set when messages were stored; "stored" is a count of the message documents under it. It is NOT a count taken
 *  from Etsy's page, so a counter that drifted (two scrapes of one conversation at the same moment, a message Etsy removed) looks
 *  exactly like a lost message until the conversation is read from Etsy in full.
 *
 *  THE LEDGER (EtsyMail_Config/linkConsistency.gaps). Every mismatch becomes one open gap, kept until it is settled, with when it
 *  began, how many re-reads were asked and how many finished. Before this the notice was written once a night and could not clear
 *  for a day whatever happened, and a re-read that did not fix a count made the notice vanish: now a gap closes only when
 *    · matched   the stored messages and the record agree again (the re-read stored what was missing), or
 *    · proven    a COMPLETE read of the Etsy conversation found every message already stored (the conversation record carries
 *                readComplete, written by etsyMailSnapshot.js for a conversation asked to be re-read in full): nothing is missing,
 *                the counter was off. How far off is remembered (proven), so the same drift is not reported again while it stays
 *                the same, and any change in it is judged afresh; or
 *    · gone      the conversation no longer exists;
 *    · settled   (a missing NEWEST message only) SETTLE_READS re-reads in a row FINISHED (the extension opened and read the conversation)
 *                and stored nothing: the count and the newest stored message stayed exactly as they were. Every read, complete or not,
 *                reads the newest messages, so the message the record claims is not on Etsy's page and re-reading cannot change anything. Before this, a conversation the extension could read but never
 *                read "completely" (readComplete is written only for a full scrape with no scrape issue: one whose messages matched
 *                none stored, or came in without their text, never gets it) stayed red for good, re-read every few hours, and after a
 *                long interruption nothing ever cleared it (Paul, 25 Oct 2026: "short for 6 days after 25 tries"). Remembered like
 *                proven, so it comes back only when the conversation changes;
 *    · checked   a person compared the conversation with Etsy and pressed "Matches Etsy" (resolve()): the same, on a person's word.
 *  A person's Dismiss only hides the notice (see dismiss()): the gap, the checking and the re-reads go on. Why a re-read was not a
 *  complete one (readIncomplete, written by etsyMailSnapshot.js) is kept on the gap (why), so the notice can say it.
 *
 *  THE SELF-HEAL. For each open gap one re-read is queued as a scrape job (an ordinary job for the Chrome extension, which reads
 *  the Etsy PAGE: no Etsy API call, nothing sent to a customer), and the conversation is flagged fullReread so that the extension
 *  does a complete pass, not just the newest messages (threadExists answers "not known", the scrape answer says needsFullScrape;
 *  both in etsyMailSnapshot.js). One re-read at a time per conversation. When it has finished and the gap is still there the next
 *  try waits 20 minutes, then 1 hour, 3 hours, then 6 hours, at most 6 a day. A person's Check now may add a try no sooner than
 *  10 minutes after the last (3 a day). A gap that is still open after 72 hours, 4 tries and 2 finished re-reads turns red.
 *
 *  WHO RUNS IT. tick() is called by the 5-minute watchdog: it looks at each open gap again (the job's state, the stored messages
 *  against the record) and clears the notice by itself the moment the counts agree, without anyone pressing anything.
 *  recheck({ manual: true }) is the same pass on demand (the sorter's Check now).
 *
 *  Cost: the nightly scan is 150 record reads, 150 counts and 150 one-document reads (about 0.01 M operations a month); a pass over
 *  the open gaps is about four reads per gap and happens only while a gap is open.
 */
"use strict";

const admin = require("./firebaseAdmin");
const LinkHealth = require("./_etsyMailLinkHealth");
const FV = admin.firestore.FieldValue;

const MIN = 60 * 1000, HOUR = 60 * MIN, DAY = 24 * HOUR;
const WINDOW = 150, REPAIR_MAX = 5, SLACK_MS = 3 * MIN, FRESH_MS = 30 * MIN, BUDGET_MS = 14000, SAMPLES = 10;
const GAPS_MAX = 50, LOOK_EVERY_MS = 30 * MIN, PROVEN_MAX = 40, PROVEN_KEEP_MS = 30 * DAY, HEALED_MAX = 5;
const BACKOFF = [20 * MIN, HOUR, 3 * HOUR, 6 * HOUR];   // the wait after the 1st, 2nd, 3rd and every later finished try
const TRIES_PER_DAY = 6, MANUAL_GAP_MS = 10 * MIN, MANUAL_PER_DAY = 3, TICK_EVERY_MS = 4 * MIN;
const SETTLE_READS = 3;   // finished re-reads in a row that stored nothing: the gap is settled (see the header)
const THREADS = "EtsyMail_Threads", JOBS = "EtsyMail_Jobs", CFG = "EtsyMail_Config", DOC = "linkConsistency";
const THREAD_FIELDS = ["messageCount", "lastInboundAt", "updatedAt", "status", "etsyConversationUrl", "etsyOrderId", "gmailThreadId", "readComplete", "readIncomplete"];

const ms = v => (v == null ? 0 : typeof v.toMillis === "function" ? v.toMillis() : typeof v === "object" && typeof v.ms === "number" ? v.ms : typeof v === "number" ? v : 0);
/** 02:00-05:00 in New York is 06:00-09:00 UTC in summer and 07:00-10:00 in winter: 07:00-09:00 UTC is inside the window all year. */
const nightUtc = now => { const h = new Date(now).getUTCHours(); return h >= 7 && h < 9; };
const dayKey = now => new Date(now).toISOString().slice(0, 10).replace(/-/g, "");
/** The order number when the conversation heading gave one (never a name or an address). */
const orderOf = t => { const o = String((t && t.etsyOrderId) || ""); return /^\d{5,15}$/.test(o) ? o : ""; };
const backoffMs = n => BACKOFF[Math.min(Math.max(n, 1), BACKOFF.length) - 1];

/** Does a drift that a complete read proved harmless still hold? (The same shortfall, or for a missing newest message the same newest time.) */
function provenHolds(p, v) {
  if (!p) return false;
  if (v.kind === "inbound_missing") return p.k === "inbound_missing" && p.lastIn === (v.lastIn || 0);
  return p.k !== "inbound_missing" && p.d === (v.claimed == null ? 0 : v.claimed) - v.stored;
}

/** Judge one conversation; null when it matches (or its drift was proven harmless by a complete read), { skipped } while it is still being read. */
async function judge(db, id, t, now, opts) {
  opts = opts || {};
  if (t.status === "detected_from_gmail" || (!opts.ignoreFresh && now - ms(t.updatedAt) < FRESH_MS)) return { skipped: true };
  const col = db.collection(THREADS).doc(id).collection("messages");
  const [c, newest] = await Promise.all([
    col.count().get().then(s => s.data().count),
    col.orderBy("timestamp", "desc").limit(1).select("timestamp").get().then(s => (s.empty ? 0 : ms(s.docs[0].data().timestamp)))
  ]);
  const claimed = typeof t.messageCount === "number" ? t.messageCount : null, lastIn = ms(t.lastInboundAt);
  if (opts.seen) { opts.seen.stored = c; opts.seen.claimed = claimed; opts.seen.newest = newest; }
  let v = null;
  if (claimed != null && claimed > 0 && c === 0) v = { kind: "claimed_but_none", claimed, stored: c };
  else if (claimed != null && claimed - c >= 3) v = { kind: "fewer_than_counted", claimed, stored: c };
  else if (lastIn > 0 && newest < lastIn - SLACK_MS) v = { kind: "inbound_missing", claimed, stored: c };
  if (!v) return null;
  v.lastIn = lastIn; v.newest = newest;
  if (opts.proven && provenHolds(opts.proven[id], v)) return null;
  return v;
}

/** What the extension's scrape job for a gap is doing: { status, atMs }, or { status: "gone" } when it no longer exists. */
async function jobInfo(db, jobId) {
  const s = await db.collection(JOBS).doc(jobId).get();
  if (!s.exists) return { status: "gone", atMs: 0 };
  const d = s.data() || {};
  return { status: String(d.status || ""), atMs: ms(d.updatedAt) };
}
const pendingJob = j => !!j && (j.status === "queued" || j.status === "claimed");

/**
 * One re-read for a gap: flag the conversation (a complete read is wanted) and queue a scrape job on a deterministic id
 * (consistency_<conversation>_<day>, then _r1, _r2 ... for a later try the same day), so two callers can never queue the same one twice.
 * Returns { jobId, day, dayTries } when queued (or already there), null when the conversation is gone.
 */
async function queueReread(db, id, g, now) {
  const day = dayKey(now), n = g.day === day ? (g.dayTries || 0) : 0;
  const jobId = `consistency_${id}_${day}` + (n ? `_r${n}` : "");
  try { await db.collection(THREADS).doc(id).update({ fullReread: { atMs: now, jobId } }); }
  catch (e) { if (e && (e.code === 5 || /not.?found/i.test(String(e.message)))) return null; throw e; }
  try {
    await db.collection(JOBS).doc(jobId).create({
      jobId, jobType: "scrape", status: "queued", threadId: id,
      payload: { etsyConversationUrl: g.u, source: "linkConsistency", gmailMessageId: null, gmailThreadId: g.gt || null, rescrape: true, fullRead: true, reason: g.k },
      attempts: 0, claimedBy: null, claimedAt: null, lastError: null, lastHeartbeatAt: null, result: null,
      createdAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp()
    });
  } catch (e) {
    if (!(e && (e.code === 6 || /already.?exists/i.test(String(e.message))))) throw e;   // queued earlier (by the other caller): it is that job
  }
  return { jobId, day, dayTries: n + 1 };
}

const newGap = (t, v, now) => ({
  o: orderOf(t), k: v.kind, c: v.claimed == null ? -1 : v.claimed, s: v.stored, li: v.lastIn || 0, sn: v.newest || 0, same: 0, why: "",
  url: !!t.etsyConversationUrl, u: t.etsyConversationUrl || "", gt: t.gmailThreadId || "",
  firstAtMs: now, checkedAtMs: now, tries: 0, reads: 0, lastTryAtMs: 0, nextTryAtMs: 0, jobId: "", seenJob: "", pend: false,
  day: "", dayTries: 0, manualDay: "", manualN: 0
});
/** What a fresh look at the conversation says about its gap (the numbers, the order number, whether it can be re-read). */
const refreshGap = (g, t, v, now) => Object.assign(g, {
  k: v.kind, c: v.claimed == null ? -1 : v.claimed, s: v.stored, li: v.lastIn || 0, sn: v.newest || 0, o: orderOf(t) || g.o || "",
  url: !!t.etsyConversationUrl, u: t.etsyConversationUrl || g.u || "", checkedAtMs: now
});

/**
 * One pass over the open gaps: look at each again (unless it was looked at a moment ago), close what is settled, and ask for the
 * next re-read where one is due.
 *   st: { gaps, proven, healed }   now: the time   manual: a person pressed Check now   max: re-reads that may be queued in this pass
 * Returns the new gaps / proven / healed, one result per gap, and how many re-reads were queued.
 */
async function advance(db, st, { now, manual = false, max = REPAIR_MAX, budgetMs = 9000 } = {}) {
  const t0 = Date.now();
  const gaps = {}, proven = Object.assign({}, st.proven || {}), healed = (st.healed || []).slice();
  const results = [];
  let queued = 0;
  const day = dayKey(now);
  const heal = (g, how) => { healed.unshift({ o: g.o || "", atMs: now, how }); healed.length = Math.min(healed.length, HEALED_MAX); };
  const ids = Object.keys(st.gaps || {}).sort((a, b) => (st.gaps[a].firstAtMs || 0) - (st.gaps[b].firstAtMs || 0));
  for (const id of ids) {
    const g = Object.assign({}, st.gaps[id]);
    if (Date.now() - t0 > budgetMs) { gaps[id] = g; results.push({ id, o: g.o || "", outcome: "later", c: g.c, s: g.s, tr: g.tries || 0, lt: g.lastTryAtMs || 0, nt: g.nextTryAtMs || 0 }); continue; }
    const res = { id, o: g.o || "", outcome: "short", c: g.c, s: g.s, tr: g.tries || 0, lt: g.lastTryAtMs || 0, nt: g.nextTryAtMs || 0, queuedNow: false };
    try {
      // the re-read's job: has it finished? (the back-off counts from its end)
      const job = g.jobId ? await jobInfo(db, g.jobId) : null;
      let finishedNow = false, readNow = false;
      if (job && !pendingJob(job) && g.seenJob !== g.jobId) {
        finishedNow = true;
        g.seenJob = g.jobId;
        if (job.status === "succeeded") { g.reads = (g.reads || 0) + 1; readNow = true; }
        g.nextTryAtMs = Math.max(g.nextTryAtMs || 0, (job.atMs || g.lastTryAtMs || now) + backoffMs(g.tries || 1));   // (a job that is gone finished no later than now: its own end time is not known, so count from when it was asked)
      }
      g.pend = pendingJob(job);
      // the stored messages against the record, unless that was done a moment ago (the scan that found it): a gap closes the moment the
      // counts agree, whether or not the read that fixed it has been reported finished. While a read is waiting, when one has just
      // finished, or when a person asks, it is looked at every time; a gap that is only waiting for its next try every half hour
      // (nothing changes the counts but a new message, so this costs next to nothing).
      const justNow = !!g.checkedAtMs && now - g.checkedAtMs < 1000;
      const look = manual || g.pend || finishedNow || !g.checkedAtMs || now - g.checkedAtMs >= LOOK_EVERY_MS;
      let closed = null;
      if (!justNow && look) {
        const [ts] = await db.getAll(db.collection(THREADS).doc(id), { fieldMask: THREAD_FIELDS });
        if (!ts.exists) closed = "gone";
        else {
          const t = ts.data() || {};
          const seen = {};
          const v = await judge(db, id, t, now, { ignoreFresh: true, proven, seen });
          if (v && v.skipped) res.outcome = "reading";
          else if (!v) { closed = "matched"; res.s = seen.stored; res.c = seen.stored; }
          else {
            const rc = t.readComplete && typeof t.readComplete === "object" ? t.readComplete : null;
            const ri = t.readIncomplete && typeof t.readIncomplete === "object" ? t.readIncomplete : null;
            if (ri && g.lastTryAtMs && ms(ri.atMs) >= g.lastTryAtMs - SLACK_MS) g.why = String(ri.why || "").slice(0, 120);   // (what kept the last re-read from being a complete one: the notice says it)
            // a finished re-read that stored nothing (the same count, the same newest message) counts towards settling; one that stored anything starts again
            // (a gap written before the newest time was kept has sn undefined: its count alone is compared, once)
            if (readNow) g.same = v.stored === g.s && (g.sn === undefined || v.newest === g.sn) ? (g.same || 0) + 1 : 0;
            if (rc && g.lastTryAtMs && ms(rc.atMs) >= g.lastTryAtMs - SLACK_MS) {
              // Etsy's conversation was read in full after our request and every message on it was already stored (or has just been):
              // nothing is missing; the record's counter was off. Remember how far off, so the same drift is not reported again.
              proven[id] = { k: v.kind, d: (v.claimed == null ? 0 : v.claimed) - v.stored, lastIn: v.lastIn || 0, atMs: now };
              closed = "proven"; res.s = v.stored; res.c = v.stored;
            } else if (v.kind === "inbound_missing" && (g.same || 0) >= SETTLE_READS) {
              // read SETTLE_READS times in a row, every read finished, nothing stored: the newest message the record claims is not on Etsy's page (see the header).
              // Only for the newest message: every read, complete or not, reads the newest messages. A count that is off may hide OLDER messages a read that was
              // not complete never reached, so those still need a complete read's proof (or a person's Matches Etsy).
              proven[id] = { k: v.kind, d: (v.claimed == null ? 0 : v.claimed) - v.stored, lastIn: v.lastIn || 0, atMs: now, how: "settled" };
              closed = "settled"; res.s = v.stored; res.c = v.stored;
            } else { refreshGap(g, t, v, now); res.c = g.c; res.s = g.s; res.o = g.o || ""; }
          }
        }
      }
      if (closed) { heal(g, closed); res.outcome = closed; results.push(res); continue; }
      // the next re-read, when one is due: none while one is waiting; a person's press may add one, but not too soon after the last
      if (g.url === false) res.outcome = "noaddr";
      else if (g.pend) res.outcome = "waiting";
      else {
        const due = !(g.tries > 0) || now >= (g.nextTryAtMs || 0);
        const dayTries = g.day === day ? (g.dayTries || 0) : 0;
        const manualN = g.manualDay === day ? (g.manualN || 0) : 0;
        const byPress = manual && g.tries > 0 && now - (g.lastTryAtMs || 0) >= MANUAL_GAP_MS && manualN < MANUAL_PER_DAY;
        if ((due || byPress) && dayTries < TRIES_PER_DAY && queued < max && g.u) {
          const q = await queueReread(db, id, g, now);
          if (q) {
            queued++;
            Object.assign(g, { jobId: q.jobId, day: q.day, dayTries: q.dayTries, tries: (g.tries || 0) + 1, lastTryAtMs: now, nextTryAtMs: now + backoffMs((g.tries || 0) + 1), pend: true });
            if (byPress && !due) { g.manualDay = day; g.manualN = manualN + 1; }
            res.queuedNow = true; res.outcome = "waiting";
          }
        }
      }
      Object.assign(res, { tr: g.tries || 0, lt: g.lastTryAtMs || 0, nt: g.nextTryAtMs || 0 });
      gaps[id] = g;
    } catch (e) {
      console.warn("linkConsistency advance", id, e && e.message);
      res.outcome = "error"; gaps[id] = g;
    }
    results.push(res);
  }
  return { gaps, proven, healed, results, queued };
}

/** The proven drifts worth keeping: recent, and not more than PROVEN_MAX. */
function pruneProven(p, now) {
  const rows = Object.entries(p || {}).filter(([, v]) => v && now - (v.atMs || 0) < PROVEN_KEEP_MS).sort((a, b) => b[1].atMs - a[1].atMs).slice(0, PROVEN_MAX);
  return Object.fromEntries(rows);
}

/** Read-modify-write of the one record, the other fields (the dismissal above all) taken from what is stored at that moment. */
async function commit(db, mutate) {
  const ref = db.collection(CFG).doc(DOC);
  return db.runTransaction(async tx => {
    const s = await tx.get(ref);
    const cur = s.exists ? s.data() : {};
    const next = mutate(cur);
    if (next) tx.set(ref, next);
    return next || cur;
  });
}

async function run(db, now, prev) {
  const t0 = Date.now();
  const proven0 = pruneProven(prev && prev.proven, now);
  const snap = await db.collection(THREADS).orderBy("updatedAt", "desc").limit(WINDOW)
    .select("messageCount", "lastInboundAt", "updatedAt", "status", "etsyConversationUrl", "gmailThreadId", "etsyOrderId").get();
  const rows = snap.docs.map(d => ({ id: d.id, t: d.data() || {} }));
  const out = { atMs: now, checked: 0, mismatched: 0, skippedFresh: 0, partial: false, repairQueued: 0, kinds: {}, samples: [] };
  let next = 0;
  const bad = [], okIds = new Set();
  await Promise.all(Array.from({ length: 8 }, async () => {
    while (next < rows.length) {
      if (Date.now() - t0 > BUDGET_MS) { out.partial = true; return; }
      const r = rows[next++];
      let v;
      try { v = await judge(db, r.id, r.t, now, { proven: proven0 }); } catch (e) { console.warn("linkConsistency", r.id, e && e.message); out.errors = (out.errors || 0) + 1; continue; }
      if (v && v.skipped) { out.skippedFresh++; continue; }
      out.checked++;
      if (v) { out.mismatched++; out.kinds[v.kind] = (out.kinds[v.kind] || 0) + 1; bad.push({ r, v }); if (out.samples.length < SAMPLES) out.samples.push({ threadId: r.id, kind: v.kind, claimed: v.claimed == null ? -1 : v.claimed, stored: v.stored }); }
      else okIds.add(r.id);
    }
  }));
  // the ledger: what was open stays open (its re-reads go on), new mismatches join it, a conversation that now matches leaves it
  const gaps = Object.assign({}, (prev && prev.gaps) || {});
  const healed = ((prev && prev.healed) || []).slice();
  for (const id of Object.keys(gaps)) if (okIds.has(id)) { healed.unshift({ o: gaps[id].o || "", atMs: now, how: "matched" }); delete gaps[id]; }
  healed.length = Math.min(healed.length, HEALED_MAX);
  let alreadyReread = 0, overflow = 0;
  for (const { r, v } of bad) {
    const g = gaps[r.id];
    if (g) { if (g.tries > 0) alreadyReread++; refreshGap(g, r.t, v, now); }
    else if (Object.keys(gaps).length < GAPS_MAX) gaps[r.id] = newGap(r.t, v, now);
    else overflow++;   // more short conversations than the ledger holds: counted, so the notice never understates it
  }
  const step = await advance(db, { gaps, proven: proven0, healed }, { now, max: REPAIR_MAX, budgetMs: Math.max(1000, BUDGET_MS - (Date.now() - t0)) });
  out.gaps = step.gaps; out.proven = pruneProven(step.proven, now); out.healed = step.healed;
  out.repairQueued = step.queued;
  out.alreadyReread = alreadyReread; out.overflow = overflow;
  const open = Object.values(step.gaps);
  out.noAddress = open.filter(g => g.url === false).length;
  out.unresolved = open.length - out.noAddress + overflow;   // what the light counts: every open gap a re-read can fix, until it is settled
  out.tickAtMs = now;
  return out;
}

/** Runs the nightly scan when it is due (night, a day since the last one) and records the result. Never throws for a missing record. */
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
  doc = await commit(db, fresh => Object.assign({}, doc, { dismissed: Object.keys(doc.gaps).length ? (fresh.dismissed || null) : null }));
  return { ran: true, doc };
}

/**
 * The same pass on the open gaps without the scan: the 5-minute watchdog calls it (tick), and the sorter's Check now (manual).
 * Does nothing, and reads nothing but the one record, when no gap is open. Returns { ran, doc } (doc: the record as now stored).
 */
async function recheck(db, { now = Date.now(), manual = false, prev = null } = {}) {
  let cur = prev;
  if (!cur) { const s = await db.collection(CFG).doc(DOC).get(); cur = s.exists ? s.data() : null; }
  const gaps0 = (cur && cur.gaps) || {};
  if (!Object.keys(gaps0).length) return { ran: false, doc: cur, results: [], queued: 0 };
  const step = await advance(db, { gaps: gaps0, proven: pruneProven(cur.proven, now), healed: cur.healed || [] }, { now, manual });
  const doc = await commit(db, fresh => {
    // gaps another run closed or added meanwhile: closed stays closed, new ones are kept
    const fg = fresh.gaps || {}, gaps = {};
    for (const id of Object.keys(step.gaps)) if (fg[id] || !gaps0[id]) gaps[id] = step.gaps[id];
    for (const id of Object.keys(fg)) if (!(id in gaps0)) gaps[id] = fg[id];
    const open = Object.values(gaps);
    const noAddress = open.filter(g => g.url === false).length;
    return Object.assign({}, fresh, {
      gaps, proven: pruneProven(Object.assign({}, fresh.proven, step.proven), now), healed: step.healed,
      noAddress, unresolved: open.length - noAddress + (open.length ? fresh.overflow || 0 : 0), overflow: open.length ? fresh.overflow || 0 : 0, tickAtMs: now, dismissed: open.length ? (fresh.dismissed || null) : null
    });
  });
  return { ran: true, doc, results: step.results, queued: step.queued };
}

/** The watchdog's call, every 5 minutes: look at the open gaps again, so a notice clears by itself when the counts agree. */
async function tick(db, { now = Date.now(), prev = null } = {}) {
  if (prev && !Object.keys(prev.gaps || {}).length) return { ran: false, doc: prev };
  if (prev && prev.tickAtMs && now - prev.tickAtMs < TICK_EVERY_MS) return { ran: false, doc: prev };
  return recheck(db, { now, manual: false, prev });
}

/**
 * A person closes the notice. It hides the notice for THIS gap (the same conversations, the same number of messages short,
 * the same colour); the gap itself, the checking and the re-reads are untouched, and the notice is back as soon as the gap
 * changes, a new conversation goes missing, or it turns red. sig is the one the person saw. Returns { ok }, or { changed } when
 * the gap is no longer the one they saw, or { nothing } when none is open.
 */
async function dismiss(db, { sig, by = "", now = Date.now() } = {}) {
  let outcome = { nothing: true };
  const doc = await commit(db, fresh => {
    const gp = LinkHealth.gapSummary(fresh, now);
    if (!gp || !gp.n) { outcome = { nothing: true }; return null; }
    if (!sig || gp.sig !== sig) { outcome = { changed: true }; return null; }
    outcome = { ok: true };
    return Object.assign({}, fresh, { dismissed: { sig, atMs: now, by: String(by || "").replace(/\s+/g, " ").trim().slice(0, 40) } });
  });
  return Object.assign({ doc }, outcome);
}

/**
 * A person compared the conversation(s) of the notice with Etsy and pressed "Matches Etsy" (Paul, 25 Oct 2026: the link could
 * not be reset). Every open gap of the notice they saw (sig) closes as "checked": its drift is remembered as proven, so it does not
 * come back while the conversation stays as it is, and does the moment it changes (a new message, another count). The re-reads stop;
 * the notice, the inbox's banner and the conversation's chip clear everywhere at once. Nothing else is touched: no message, no
 * conversation field. Returns { ok, n }, { changed } when the gaps are not the ones they saw, or { nothing }.
 */
async function resolve(db, { sig, by = "", now = Date.now() } = {}) {
  let outcome = { nothing: true };
  const who = String(by || "").replace(/\s+/g, " ").trim().slice(0, 40);
  const doc = await commit(db, fresh => {
    const gp = LinkHealth.gapSummary(fresh, now);
    if (!gp || !gp.n) { outcome = { nothing: true }; return null; }
    if (!sig || gp.sig !== sig) { outcome = { changed: true }; return null; }
    const proven = Object.assign({}, fresh.proven || {}), healed = (fresh.healed || []).slice();
    const ids = Object.keys(fresh.gaps || {});
    for (const id of ids) {
      const g = fresh.gaps[id] || {};
      proven[id] = { k: g.k, d: g.k === "inbound_missing" ? 0 : (g.c == null || g.c < 0 ? 0 : g.c) - (g.s || 0), lastIn: g.li || 0, atMs: now, how: "checked", by: who };
      healed.unshift({ o: g.o || "", atMs: now, how: "checked", by: who });
    }
    healed.length = Math.min(healed.length, HEALED_MAX);
    outcome = { ok: true, n: ids.length };
    return Object.assign({}, fresh, { gaps: {}, proven: pruneProven(proven, now), healed, unresolved: 0, noAddress: 0, overflow: 0, dismissed: null, tickAtMs: now });
  });
  return Object.assign({ doc }, outcome);
}

module.exports = { maybeRun, run, judge, recheck, tick, dismiss, resolve, advance, BACKOFF, TRIES_PER_DAY, MANUAL_GAP_MS, SETTLE_READS };
