/*  netlify/functions/_etsyMailLinkRecover.js
 *
 *  "Reconnect & retry all" (LINKRECOVER, Paul 20 Oct 2026: "the user should be able to reset these errors and by doing so the
 *  errors should re-test and reconnect all of the connections and pull all missing emails and send all unsent messages ... the
 *  system does not gracefully/automatically recover from this loss of communication").
 *
 *  A red line on the link light is a FACT about the customer mail (a message has not gone, an answer has not been read), so it is
 *  never hidden by a button: it is repaired, and the light clears by itself once the evidence says the part works again. One
 *  recovery run does, in order, and says in plain words what each step found and did:
 *     1. check      the line is judged (before)
 *     2. send       the send queue's books are made true and every message that was never clicked is put back on its way
 *                   (_etsyMailSendQueue.repair: nothing that may have gone is ever sent again)
 *     3. read       stuck read jobs are freed, read jobs that were given up on are put back, and the job list the helper asks
 *                   for is test-read (a failing query would look like a quiet helper)
 *     4. gmail      the Gmail sign-in is tested and a Gmail pull is started (it resumes from the last message it saw, so every
 *                   email missed during an outage is pulled, and its 6-hour sweep catches any skipped one)
 *     5. catchup    the catch-up pass (answers already stored are folded into the sorter's conversations)
 *     6. gaps       (a person asked) the conversations that were short are re-read
 *     7. orders     a stale Etsy order mirror is told to run at its next tick
 *     8. helper     the Etsy helper (the Chrome extension, the only thing that can send on Etsy and read its pages) is watched
 *                   for up to two minutes: does it now send and read? If it is heard but does nothing, the step says what a
 *                   person must do on its computer (the server cannot restart a browser extension).
 *     9. after      the line is judged again and published, so every sorter and the inbox's banner follow at once
 *
 *  WHO STARTS IT
 *   · a person: the sorter's popover and banner (etsyMailOrderLink op link_recover), the inbox's link chip (etsyMailLinkRecover op
 *     start). One run at a time: a press while one runs joins it.
 *   · the link monitor (etsyMailLinkWatchdog, every 5 minutes) by itself, when sending, reading, noticing or catching up is failing
 *     and the helper is not simply away: once for each problem, then again after 10, 20, 40 ... minutes (at most every 2 hours)
 *     while it lasts. An automatic run is gentler than a person's (see repair's modes).
 *
 *  The run itself is etsyMailLinkRecover-background.js (a background function: up to 15 minutes). Its progress and result live in
 *  EtsyMail_Config/linkRecover, which the pages read.
 */
"use strict";

const crypto = require("crypto");
const LinkHealth = require("./_etsyMailLinkHealth");

const MIN = 60 * 1000, HOUR = 60 * MIN, DAY = 24 * HOUR;
const CFG = "EtsyMail_Config", DOC = "linkRecover";
const RUN_STALE_MS = 6 * MIN;                        // a run that has not finished after this long died (the background function's budget is 15 min; a run takes 2-3)
const AUTO_FIRST_GAP_MS = 10 * MIN, AUTO_MAX_GAP_MS = 2 * HOUR;
const CORE = new Set(["send", "read", "notice", "catchup"]);
const WATCH_MS = 2 * MIN, WATCH_EVERY_MS = 15 * 1000;
const JOBS_REVIVE_MANUAL_MS = 7 * DAY, JOBS_REVIVE_AUTO_MS = DAY, JOBS_AUTO_REVIVES = 2;
const HELPER_RELOAD = "On the computer that has the inbox's Etsy extension: open chrome://extensions and press the extension's reload arrow (or quit and reopen Chrome), and check that its Etsy tab is signed in.";

const ref = db => db.collection(CFG).doc(DOC);
const plural = (n, one, many) => n + " " + (n === 1 ? one : many);
const ago = LinkHealth.ago;
const ms = LinkHealth.tsMs;
const short = s => String(s == null ? "" : s).replace(/\s+/g, " ").trim().slice(0, 200);

/**
 * Should the link monitor start a recovery by itself? res: the judgement (evaluate's answer, with incidentId when published);
 * rec: the linkRecover document. Resolves { go, why }.
 */
function shouldAuto(res, rec, now) {
  if (!res || res.level === "ok") return { go: false, why: "healthy" };
  const failing = (res.checks || []).filter(c => CORE.has(c.id) && (c.level === "warn" || c.level === "down") && !/switched off in the inbox/.test(c.text || ""));
  if (!failing.length) return { go: false, why: "nothing a recovery can fix" };
  rec = rec || {};
  if (rec.state === "running" && now - (rec.startedAtMs || 0) < RUN_STALE_MS) return { go: false, why: "running" };
  const incident = String(res.incidentId || res.downSinceMs || "");
  const same = incident && rec.autoIncident === incident;
  const n = same ? Number(rec.autoCount) || 0 : 0;
  const gap = n ? Math.min(AUTO_MAX_GAP_MS, AUTO_FIRST_GAP_MS * Math.pow(2, n - 1)) : AUTO_FIRST_GAP_MS;
  const last = Math.max(Number(rec.autoAtMs) || 0, Number(rec.startedAtMs) || 0);
  if (last && now - last < (n ? gap : AUTO_FIRST_GAP_MS)) return { go: false, why: "backing off" };
  return { go: true, why: failing.map(c => c.id).join(","), incident, n: n + 1 };
}

/**
 * Claim the one run. opts: { by, auto, incident, n, now }. Resolves { started, run } (run: the document as it is now).
 * A run already going (younger than RUN_STALE_MS) is joined, not doubled.
 */
async function start(db, opts = {}) {
  const now = opts.now || Date.now();
  let out = null;
  await db.runTransaction(async tx => {
    out = null;
    const s = await tx.get(ref(db));
    const cur = s.exists ? s.data() : {};
    if (cur.state === "running" && now - (cur.startedAtMs || 0) < RUN_STALE_MS) { out = { started: false, run: cur }; return; }
    const runId = "r" + now.toString(36) + crypto.randomBytes(3).toString("hex");
    const last = cur.runId ? [{ runId: cur.runId, at: cur.startedAtMs || 0, by: cur.by || null, auto: !!cur.auto, state: cur.state === "running" ? "died" : cur.state || "", after: cur.after ? cur.after.level : "" }] : [];
    const doc = {
      runId, state: "running", startedAtMs: now, finishedAtMs: 0, by: opts.by ? short(opts.by).slice(0, 60) : null, auto: !!opts.auto,
      steps: [], before: null, after: null, summary: "", error: "",
      history: last.concat(Array.isArray(cur.history) ? cur.history : []).slice(0, 8),
      autoIncident: opts.auto ? String(opts.incident || "") : (cur.autoIncident || ""),
      autoCount: opts.auto ? Number(opts.n) || 1 : (Number(cur.autoCount) || 0),
      autoAtMs: opts.auto ? now : (Number(cur.autoAtMs) || 0)
    };
    tx.set(ref(db), doc);
    out = { started: true, run: doc };
  });
  return out;
}

/** Start the background run (a POST to the background function, which answers 202 at once). Never throws. */
async function kick(runId, deps = {}) {
  const origin = process.env.URL || process.env.DEPLOY_URL || process.env.NETLIFY_BASE_URL;
  const doFetch = deps.fetch || (typeof fetch === "function" ? fetch : null);
  if (!origin || !doFetch) return { ok: false, error: "no site address to start the run from" };
  try {
    const r = await doFetch(origin + "/.netlify/functions/etsyMailLinkRecover-background", {
      method: "POST", headers: { "Content-Type": "application/json", "X-EtsyMail-Secret": process.env.ETSYMAIL_EXTENSION_SECRET || "" },
      body: JSON.stringify({ runId })
    });
    return { ok: r.status >= 200 && r.status < 300, status: r.status };
  } catch (e) { return { ok: false, error: String(e && e.message || e).slice(0, 120) }; }
}

/** Ask for a run and start it (the people's buttons and the monitor). Resolves { started, run, kicked }. */
async function request(db, opts = {}, deps = {}) {
  const r = await start(db, opts);
  if (!r.started) return Object.assign({ kicked: null }, r);
  const k = await (deps.kick || kick)(r.run.runId, deps);
  if (!k.ok) {
    // the run could not be started in the background: say so on the document (a page shows it, the next press tries again)
    await ref(db).set({ state: "failed", finishedAtMs: Date.now(), error: "Could not start the recovery: " + (k.error || "status " + k.status) }, { merge: true }).catch(() => {});
    r.run = Object.assign({}, r.run, { state: "failed", error: "Could not start the recovery" });
  }
  return Object.assign({ kicked: k }, r);
}

/** The run's state for the pages. */
async function status(db) {
  const s = await ref(db).get();
  const d = s.exists ? s.data() : null;
  if (!d) return { state: "never" };
  const now = Date.now();
  if (d.state === "running" && now - (d.startedAtMs || 0) >= RUN_STALE_MS) d.state = "died";
  return {
    runId: d.runId || "", state: d.state || "", startedAtMs: d.startedAtMs || 0, finishedAtMs: d.finishedAtMs || 0, by: d.by || null, auto: !!d.auto,
    steps: Array.isArray(d.steps) ? d.steps : [], before: d.before || null, after: d.after || null, summary: d.summary || "", error: d.error || "", now
  };
}

// ─── the steps ─────────────────────────────────────────────────────────────

function sendText(r) {
  const did = [];
  if (r.lease === "released") did.push("freed the send turn from a message that no longer existed");
  if (r.stuckSend) did.push("took the turn back from a send that had stalled before Etsy's Send button");
  if (r.tookBack) did.push(`put ${plural(r.tookBack, "message", "messages")} that had lost ${r.tookBack === 1 ? "its" : "their"} turn back in the queue`);
  if (r.slots) did.push(`cleared ${plural(r.slots, "stale “sending” mark", "stale “sending” marks")}`);
  if (r.adopted) did.push(`moved ${plural(r.adopted, "old-style waiting message", "old-style waiting messages")} into the queue`);
  if (r.unblocked) did.push(`cut the wait short for ${plural(r.unblocked, "message", "messages")}`);
  if (r.revived) did.push(`put ${plural(r.revived, "message that had failed", "messages that had failed")} (never sent) back in the queue`);
  if (r.settled) did.push(`found ${plural(r.settled, "message", "messages")} already in ${r.settled === 1 ? "its conversation" : "their conversations"} and marked ${r.settled === 1 ? "it" : "them"} sent`);
  let t = did.length ? cap(did.join("; ")) + "." : "The send queue's books were already right.";
  if (r.promoted) t += " The next message has been handed to the Etsy helper.";
  if (r.stranded || r.attention) {
    const n = (r.stranded || 0) + (r.attention || 0);
    t += ` ${plural(n, "message needs", "messages need")} a person (${n === 1 ? "it" : "they"} may already have gone, so ${n === 1 ? "it is" : "they are"} never sent twice by itself): open the conversation to check and send again.`;
  }
  if (r.errors && r.errors.length) t += " Some parts could not be looked at: " + r.errors.slice(0, 2).join("; ") + ".";
  return t;
}
const cap = s => s.charAt(0).toUpperCase() + s.slice(1);

async function stepSend(db, ctx) {
  const Q = ctx.deps.queue || require("./_etsyMailSendQueue");
  const r = await Q.repair({ mode: ctx.manual ? "manual" : "auto", by: ctx.by });
  ctx.sendRepair = r;
  return { level: r.errors && r.errors.length ? "warn" : (r.stranded || r.attention) ? "warn" : "ok", text: sendText(r) };
}

async function stepRead(db, ctx) {
  const reapers = ctx.deps.reapers || (() => { try { return require("./etsyMailReapers"); } catch (e) { return null; } })();
  const did = [], warns = [];
  if (reapers && reapers.reapStuckScrapeClaims) {
    try { const r = await reapers.reapStuckScrapeClaims(); if (r && r.requeued) did.push(`freed ${plural(r.requeued, "read", "reads")} the helper had stopped on`); if (r && r.exhausted) warns.push(`${plural(r.exhausted, "conversation", "conversations")} the helper keeps stopping on ${r.exhausted === 1 ? "was" : "were"} set aside`); }
    catch (e) { warns.push("could not free stuck reads (" + short(e.message).slice(0, 80) + ")"); }
  }
  // read jobs that were given up on go back in the queue (a person asked: every one of the last week; the monitor: the last day, twice at most)
  try {
    const qs = await db.collection("EtsyMail_Jobs").where("status", "==", "failed").where("jobType", "==", "scrape").limit(200).get();
    const now = Date.now();
    let n = 0;
    for (const d of qs.docs) {
      const j = d.data();
      const at = ms(j.updatedAt) || ms(j.createdAt);
      if (!at || now - at > (ctx.manual ? JOBS_REVIVE_MANUAL_MS : JOBS_REVIVE_AUTO_MS)) continue;
      if (!ctx.manual && (j.recoverRevives || 0) >= JOBS_AUTO_REVIVES) continue;
      if (!j.threadId || !(j.payload && j.payload.etsyConversationUrl)) continue;
      await d.ref.set({
        status: "queued", attempts: 0, reaps: 0, claimedBy: null, claimedAt: null, lastHeartbeatAt: null,
        lastError: "Put back by " + (ctx.manual ? (ctx.by || "a person") : "the automatic recovery") + " after: " + short(j.lastError).slice(0, 160),
        recoverRevives: (j.recoverRevives || 0) + 1, updatedAt: ctx.serverTs()
      }, { merge: true });
      n++;
      if (n >= 60) break;
    }
    if (n) did.push(`put ${plural(n, "conversation read that had been given up on", "conversation reads that had been given up on")} back in line`);
  } catch (e) { warns.push("could not look at the reads that were given up on (" + short(e.message).slice(0, 80) + ")"); }
  if (reapers && reapers.reapDetectedThreadsWithoutLiveJobs) {
    try { const r = await reapers.reapDetectedThreadsWithoutLiveJobs(); if (r && r.requeued) did.push(`asked again for ${plural(r.requeued, "new conversation", "new conversations")} that had no read waiting`); }
    catch (e) { warns.push("could not look for new conversations without a read (" + short(e.message).slice(0, 80) + ")"); }
  }
  // the very question the helper asks for its next job: if Firestore refuses it (an index, a permission), the helper is heard but can never get work
  try {
    await db.collection("EtsyMail_Jobs").where("status", "==", "queued").where("jobType", "in", ["scrape"]).orderBy("createdAt", "asc").limit(1).get();
  } catch (e) {
    ctx.jobQueryBroken = true;
    warns.push("the list of reads the helper asks for cannot be read (" + short(e.message).slice(0, 120) + "); the helper can not get any work until this is fixed in Firestore");
  }
  try {
    const q = await db.collection("EtsyMail_Jobs").where("status", "==", "queued").limit(80).get();
    ctx.jobsBefore = new Set(q.docs.filter(d => (d.data() || {}).jobType === "scrape").map(d => d.id));
  } catch (e) { ctx.jobsBefore = null; }
  let t = did.length ? cap(did.join("; ")) + "." : "No read was stuck or given up on.";
  if (ctx.jobsBefore && ctx.jobsBefore.size) t += ` ${plural(ctx.jobsBefore.size, "conversation is", "conversations are")} waiting for the helper to read ${ctx.jobsBefore.size === 1 ? "it" : "them"}.`;
  if (warns.length) t += " " + cap(warns.join("; ")) + ".";
  return { level: ctx.jobQueryBroken ? "down" : warns.length ? "warn" : "ok", text: t };
}

async function stepGmail(db, ctx) {
  const cfg = await db.collection(CFG).doc("gmailWatcher").get().then(s => (s.exists ? s.data() : null), () => null);
  if (!cfg || cfg.enabled !== true) return { level: "down", text: "The Gmail watcher is switched off, so new Etsy messages are not noticed. Turn it on in the inbox's Gmail settings (it is left as a person set it)." };
  const G = ctx.deps.gmail || require("./_etsyMailGmail");
  try {
    await G.getValidGmailAccessToken();
    await G.listMessages({ q: "from:etsy.com", maxResults: 1 });
  } catch (e) {
    const plain = LinkHealth.plainError(e && e.message);
    if (/sign-in has expired/.test(plain)) return { level: "down", text: "Gmail refused the inbox's sign-in, so it cannot be reconnected from here: sign in to Gmail again in the inbox's Gmail settings." };
    return { level: "warn", text: `Could not reach Gmail just now (${plain}). The watcher tries again every minute by itself.` };
  }
  // a pull from where it last stopped, unless one is running right now (a run older than 20 minutes is dead: the watcher's own rule)
  const st = await db.collection(CFG).doc("gmailSyncState").get().then(s => (s.exists ? s.data() : {}), () => ({}));
  const startedAt = ms(st.lastSyncStartedAt);
  if (st.lastSyncInProgress && startedAt && Date.now() - startedAt < 20 * MIN) return { level: "ok", text: `Gmail answers. A pull is already running (started ${ago(Date.now() - startedAt)} ago); it picks up every email since the last one it saw.` };
  const k = await (ctx.deps.kickGmail || kickGmail)(ctx.deps);
  if (!k.ok) return { level: "warn", text: `Gmail answers, but a pull could not be started (${k.error || "status " + k.status}). The watcher starts one by itself within a minute.` };
  return { level: "ok", text: "Gmail answers. A pull has started from the last email it saw, so every Etsy email that arrived while the link was down is picked up (plus a sweep of the last 6 hours for any it skipped)." };
}
async function kickGmail(deps = {}) {
  const origin = process.env.URL || process.env.DEPLOY_URL || process.env.NETLIFY_BASE_URL;
  const doFetch = deps.fetch || (typeof fetch === "function" ? fetch : null);
  if (!origin || !doFetch) return { ok: false, error: "no site address" };
  try {
    const r = await doFetch(origin + "/.netlify/functions/etsyMailGmail-background", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ mode: "incremental", recoverTriggered: true }) });
    return { ok: r.status >= 200 && r.status < 300, status: r.status };
  } catch (e) { return { ok: false, error: String(e && e.message || e).slice(0, 120) }; }
}

async function stepCatchUp(db, ctx) {
  const OL = ctx.deps.orderLink || require("./_etsyMailOrderLink");
  const r = await OL.reconcile({ budgetMs: 20000 });
  if (r && r.error) return { level: "warn", text: `The catch-up pass failed: ${LinkHealth.plainError(r.error)}. It runs again every 5 minutes.` };
  const n = r ? (r.pulled || 0) + (r.settled || 0) + (r.linked || 0) : 0;
  return { level: "ok", text: n ? `The catch-up pass brought ${plural(n, "change", "changes")} into the sorter's conversations.` : "The catch-up pass ran; nothing was behind." };
}

async function stepGaps(db, ctx) {
  if (!ctx.manual) return null;
  let C;
  try { C = ctx.deps.consistency || require("./_etsyMailLinkConsistency"); } catch (e) { return null; }
  const r = await C.recheck(db, { now: Date.now(), manual: true });
  const gp = r && r.doc ? LinkHealth.gapSummary(r.doc, Date.now()) : null;
  if (!gp || !gp.n) return { level: "ok", text: "No conversation is short of messages." };
  return { level: "ok", text: `${plural(gp.n, "conversation is", "conversations are")} still short; ${r.queued ? plural(r.queued, "re-read was", "re-reads were") + " asked for now" : "their re-reads are already waiting for the helper"}.` };
}

async function stepOrders(db, ctx) {
  const d = await db.collection(CFG).doc("receiptsMirrorState").get().then(s => (s.exists ? s.data() : null), () => null);
  if (!d || d.enabled === false) return null;
  const done = ms(d.lastSyncCompletedAt);
  if (done && Date.now() - done < 30 * MIN && !d.lastSyncErrorMsg) return { level: "ok", text: `Up to date (updated ${ago(Date.now() - done)} ago).` };
  await db.collection(CFG).doc("receiptsMirrorState").set({ nextEligibleAtMs: 0 }, { merge: true });
  return { level: "ok", text: "The Etsy order update was told to run at its next turn (within 3 minutes)." };
}

/** Watch the helper for up to WATCH_MS: does it send, does it read? The step that tells a person when only its computer can be fixed. */
async function stepHelper(db, ctx) {
  const Q = ctx.deps.queue || require("./_etsyMailSendQueue");
  const sleep = ctx.deps.sleep || (t => new Promise(r => setTimeout(r, t)));
  const t0 = Date.now();
  const watchMs = ctx.deps.watchMs != null ? ctx.deps.watchMs : WATCH_MS;
  let sum = null, jobsLeft = null, helper = null;
  const look = async () => {
    sum = await Q.summary().catch(() => null);
    helper = await db.collection("EtsyMail_OrderLinkMeta").doc("helper").get().then(s => (s.exists ? s.data() : {}), () => ({}));
    if (ctx.jobsBefore) {
      const q = await db.collection("EtsyMail_Jobs").where("status", "==", "queued").limit(80).get().catch(() => null);
      if (q) { const now = new Set(q.docs.map(d => d.id)); jobsLeft = [...ctx.jobsBefore].filter(id => now.has(id)).length; }
    }
  };
  const sendWaiting = () => !!sum && (sum.queued || 0) + (sum.claimed || 0) + (sum.sending || 0) > 0;
  const sentSince = () => !!sum && (sum.lastProgressAtMs || 0) >= ctx.startedAtMs;
  const readSince = () => ctx.jobsBefore && jobsLeft != null && jobsLeft < ctx.jobsBefore.size;
  await look();
  while (Date.now() - t0 < watchMs) {
    const needSend = sendWaiting() && !sentSince(), needRead = ctx.jobsBefore && ctx.jobsBefore.size && !readSince();
    if (!needSend && !needRead) break;
    await sleep(WATCH_EVERY_MS);
    await look();
  }
  const now = Date.now();
  const seen = Math.max(Number(helper && helper.seenAtMs) || 0, Number(sum && sum.helperSeenAtMs) || 0);
  if (!seen) return { level: "warn", text: "The Etsy helper has never been heard from. " + LinkHealth.HELPER_DO };
  if (now - seen > LinkHealth.T.helperAway) return { level: "wait", text: `The Etsy helper is away (last heard ${ago(now - seen)} ago): its computer is off or Chrome is closed. Everything waits safely and goes out, in order, by itself when it is back. ${LinkHealth.HELPER_DO}` };
  const parts = [], bad = [];
  if (sendWaiting()) (sentSince() ? parts : bad).push(sentSince() ? "it is sending again" : "it was given a message and did not send it");
  else if (sum) parts.push("nothing is waiting to be sent");
  if (ctx.jobsBefore && ctx.jobsBefore.size) (readSince() ? parts : bad).push(readSince() ? `it is reading again (${ctx.jobsBefore.size - jobsLeft} of ${ctx.jobsBefore.size} read)` : "it has not taken any of the conversations waiting to be read");
  const jobsAt = Number(helper && helper.jobsAtMs) || 0;
  if (!bad.length) return { level: "ok", text: `The Etsy helper is answering: ${parts.join("; ")}.` };
  let t = `The Etsy helper is heard (last ${ago(now - seen)} ago) but ${bad.join(", and ")} in ${ago(now - t0)} of watching`;
  if (jobsAt && now - jobsAt > 15 * MIN && ctx.jobsBefore && ctx.jobsBefore.size) t += `; it has not asked for a conversation to read for ${ago(now - jobsAt)}`;
  if (ctx.jobQueryBroken) t += "; it cannot get any read job because the job list cannot be read (see above)";
  t += ". The server side is reset, so the helper itself is stuck. " + HELPER_RELOAD + " Nothing is lost: everything waits and goes out by itself as soon as it works again.";
  return { level: "down", text: t };
}

const STEPS = [
  { id: "send", label: "Sending", run: stepSend },
  { id: "read", label: "Reading answers", run: stepRead },
  { id: "gmail", label: "Gmail", run: stepGmail },
  { id: "catchup", label: "Catching up", run: stepCatchUp },
  { id: "gaps", label: "Missing messages", run: stepGaps },
  { id: "orders", label: "Etsy order data", run: stepOrders },
  { id: "helper", label: "Etsy helper", run: stepHelper }
];

/**
 * The run. opts: { runId, now }. deps (tests): { queue, reapers, gmail, orderLink, consistency, fetch, kickGmail, sleep, watchMs }.
 * Each step's answer is written as soon as it is known, so a page can show the run moving. Never sends anything that may have gone.
 */
async function run(db, opts = {}, deps = {}) {
  const s0 = await ref(db).get();
  const doc = s0.exists ? s0.data() : null;
  if (!doc || doc.state !== "running" || (opts.runId && doc.runId !== opts.runId)) return { skipped: true, why: "not this run" };
  const admin = deps.admin || require("./firebaseAdmin");
  const ctx = { deps, manual: !doc.auto, by: doc.by || null, startedAtMs: doc.startedAtMs || Date.now(), serverTs: () => admin.firestore.FieldValue.serverTimestamp() };
  const steps = [];
  const save = patch => ref(db).set(Object.assign({ steps }, patch || {}), { merge: true }).catch(e => console.warn("linkRecover progress:", e && e.message));
  const judge = async () => { const ev = await LinkHealth.gather(db); return LinkHealth.evaluate(ev, Date.now()); };
  try {
    const before = await judge().catch(() => null);
    await save({ before: before ? { level: before.level, short: before.short, problem: before.problem } : null });
    for (const st of STEPS) {
      if (st.id === "helper") { steps.push({ id: st.id, label: st.label, level: "run", text: "Watching the Etsy helper for up to 2 minutes: does it send and read now?", atMs: Date.now() }); await save(); steps.pop(); }
      let r;
      try { r = await st.run(db, ctx); }
      catch (e) { console.warn("linkRecover " + st.id + ":", e && e.message); r = { level: "warn", text: "This step failed: " + LinkHealth.plainError(e && e.message) + ". The rest still ran." }; }
      if (!r) continue;
      steps.push({ id: st.id, label: st.label, level: r.level, text: String(r.text || "").slice(0, 700), atMs: Date.now() });
      await save();
    }
    const { res, doc: pub } = await LinkHealth.publish(db, Date.now(), { keepStamp: true });
    const after = { level: res.level, short: res.short, problem: res.problem };
    const worst = steps.find(s => s.level === "down") || steps.find(s => s.level === "warn");
    const summary = res.level === "ok" ? "Everything works again."
      : worst ? worst.text.split(/(?<=\.)\s/)[0] + (worst.id === "helper" ? " " + HELPER_RELOAD : "")
      : res.problem || "Some parts are still catching up; they clear by themselves.";
    await save({ state: "done", finishedAtMs: Date.now(), after, summary });
    return { ok: true, steps, after, link: LinkHealth.summaryOf(pub) };
  } catch (e) {
    console.error("linkRecover run:", e);
    await save({ state: "failed", finishedAtMs: Date.now(), error: "The recovery stopped: " + LinkHealth.plainError(e && e.message) });
    return { ok: false, error: e && e.message };
  }
}

module.exports = { shouldAuto, start, kick, request, status, run, STEPS, RUN_STALE_MS, AUTO_FIRST_GAP_MS, AUTO_MAX_GAP_MS, HELPER_RELOAD, sendText };
