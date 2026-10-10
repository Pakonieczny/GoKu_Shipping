/*  netlify/functions/_etsyMailLinkHealth.js
 *
 *  Is the customer-mail line really working? One answer, in plain words, for every place that shows it: the
 *  Charm Sorter's "Active" light and top-bar envelope (health op of _etsyMailOrderLink.js), the inbox's top-bar
 *  banner (EtsyMail_Config/linkHealth, written by etsyMailLinkWatchdog.js every five minutes) and the tests.
 *
 *  Rules (Paul, 10 Oct 2026: "it can never silently go down"):
 *   - Every part a message passes through is read from what that part leaves behind: the inbox's send switch, the drafts
 *     waiting for the Etsy helper, the helper's last check-in, the Gmail watcher and its sign-in, the page-read check of
 *     the scrape (scrapeHealth), the scrape jobs waiting, the Etsy order data, and the watchers of the watchers (the
 *     monitor and the catch-up pass must have run lately).
 *   - Green only when every core part is known to work. A missing signal is never green: a helper never heard from, a
 *     Gmail check never recorded and a health read that failed all show amber.
 *   - Nothing here writes, sends, or calls Etsy. gather() is a handful of Firestore reads.
 */
"use strict";

const MIN = 60 * 1000, HOUR = 60 * MIN, DAY = 24 * HOUR;
const RANK = { ok: 0, wait: 0, warn: 1, down: 2 };

// How long each part may be quiet before it is called slow (amber) or stopped (red).
const T = {
  helperWarn: 12 * MIN, helperDown: 30 * MIN,        // the Etsy helper (Chrome extension) checks in every ~20 s; it may idle a few minutes
  helperBusyWarn: 5 * MIN, helperBusyDown: 15 * MIN, // ...with work waiting for it
  workWarn: 5 * MIN, workDown: 15 * MIN,             // a message waiting to be sent
  readWarn: 10 * MIN, readDown: 30 * MIN,            // a new Etsy message waiting to be read in
  gmailWarn: 10 * MIN, gmailDown: 30 * MIN,          // the Gmail watcher polls every minute
  monitorWarn: 12 * MIN, monitorDown: 45 * MIN,      // the watchdog runs every 5 minutes
  catchUpWarn: 20 * MIN, catchUpDown: 3 * HOUR,      // the reaper's order_links pass runs every 5 minutes
  mirrorWarn: 30 * MIN, mirrorDown: 6 * HOUR,        // the Etsy receipts mirror runs every ~3-13 minutes
  consistencyStale: 36 * HOUR,                       // the nightly check
  pageBadRuns: 3                                     // bad page reads in a row before Etsy's page is called unreadable
};

function tsMs(v) {
  if (!v) return 0;
  if (typeof v === "number") return v;
  if (typeof v.toMillis === "function") return v.toMillis();
  if (v._seconds != null) return v._seconds * 1000 + Math.round((v._nanoseconds || 0) / 1e6);
  const n = Date.parse(v); return Number.isFinite(n) ? n : 0;
}
const clean = (s, n) => String(s == null ? "" : s).replace(/\s+/g, " ").trim().slice(0, n || 140);
const ago = ms => ms < 90 * 1000 ? Math.max(1, Math.round(ms / 1000)) + " s"
  : ms < 90 * MIN ? Math.round(ms / MIN) + " min" : ms < 48 * HOUR ? Math.round(ms / HOUR) + " h" : Math.round(ms / DAY) + " days";

/** The plain-words reason behind a raw error (what the person reads, never a stack or a JSON body). */
function plainError(raw) {
  const s = String(raw == null ? "" : raw);
  if (/invalid_grant|token (has been )?(expired|revoked)|Gmail token refresh failed: 4\d\d/i.test(s)) return "its Gmail sign-in has expired";
  if (/Etsy token refresh failed|Etsy OAuth not seeded|No refresh_token/i.test(s)) return "its Etsy sign-in needs renewing";
  if (/daily_rate_limit|rate.?limited|\b429\b/i.test(s)) return "Etsy's daily limit was reached";
  if (/ETIMEDOUT|ECONNRESET|ECONNREFUSED|connection (reset|refused|closed)|ENOTFOUND|EAI_AGAIN|fetch failed|network|timed? ?out|aborted/i.test(s)) return "the connection dropped";
  if (/\b5\d\d\b|unavailable|deadline/i.test(s)) return "the service answered with an error";
  return clean(s, 100) || "an unknown error";
}

/**
 * Judge the evidence.
 * evidence: {
 *   global, watcher, gmail, helper, scrapeHealth, mirror, bell, monitor, consistency: document data or null (null = missing)
 *   readOk: { [name]: boolean }            (false: the read itself failed; undefined counts as true)
 *   queuedDrafts: [ms]|null, scrapeJobs: [ms]|null    (ages come from these times; null: could not be read)
 *   queue: optional { queued, claimed, sending, failed, needsAttention, oldestQueuedAtMs, helperSeenAtMs, stalled }   (MAILQUEUE's summary())
 *   self: true when the watchdog itself is judging (it does not judge its own stamp)
 * }
 */
function evaluate(ev, now) {
  now = now || Date.now();
  ev = ev || {};
  const ok = n => !ev.readOk || ev.readOk[n] !== false;
  const checks = [];
  const add = (id, label, level, text, short, pri) => checks.push({ id, label, level, text, short: short || "", pri: pri == null ? 9 : pri });

  const g = ev.global || {};
  const drafts = Array.isArray(ev.queuedDrafts) ? ev.queuedDrafts : null;
  const jobs = Array.isArray(ev.scrapeJobs) ? ev.scrapeJobs : null;
  const oldest = drafts && drafts.length ? now - Math.min(...drafts.map(t => t || now)) : 0;
  const nq = drafts ? drafts.length : 0;
  const ns = jobs ? jobs.length : 0;
  const late = ns ? Math.max(...jobs.map(t => now - (t || now))) : 0;

  // ── the Etsy helper: the inbox's Chrome extension, which sends on Etsy and reads new messages ──
  const hp = ev.helper || null;
  const seenAt = Math.max(hp && hp.seenAtMs || 0, ev.queue && ev.queue.helperSeenAtMs || 0);
  const seen = seenAt ? now - seenAt : null;
  const stuck = Math.max(oldest, late);
  const offline = `The inbox's Etsy helper (its Chrome extension) last checked in ${seen == null ? "" : ago(seen) + " ago"}. Until it is back, messages wait and answers are not read.`;
  if (!ok("helper")) add("helper", "Etsy helper", "warn", "Could not look at the Etsy helper's last check-in.", "Helper unknown", 1);
  else if (seen == null) add("helper", "Etsy helper", "warn", "The inbox's Etsy helper (its Chrome extension) has not been heard from yet. Is a browser with the extension open?", "Helper not heard from", 1);
  else if (seen > T.helperBusyWarn && stuck > T.helperBusyDown) add("helper", "Etsy helper", "down", offline, "Helper offline", 1);
  else if (seen > T.helperBusyWarn && stuck > T.helperBusyWarn) add("helper", "Etsy helper", "warn", `The inbox's Etsy helper last checked in ${ago(seen)} ago, and work is waiting for it.`, "Helper slow", 1);
  else if (seen > T.helperDown) add("helper", "Etsy helper", "down", offline, "Helper offline", 1);
  else if (seen > T.helperWarn) add("helper", "Etsy helper", "warn", `The inbox's Etsy helper last checked in ${ago(seen)} ago. Is a browser with the inbox's extension open?`, "Helper quiet", 1);
  else add("helper", "Etsy helper", "ok", `Checked in ${ago(seen)} ago`);

  // ── sending: the inbox's switch, messages waiting for the helper, and MAILQUEUE's queue (EtsyMail_SendQueue) when it is there ──
  const q = ev.queue || null;   // { queued, claimed, sending, failed, needsAttention, oldestQueuedAtMs, helperSeenAtMs, stalled }
  const qOldest = q && q.oldestQueuedAtMs ? now - q.oldestQueuedAtMs : 0;
  const waited = Math.max(oldest, qOldest);
  const nQueued = Math.max(nq, q ? (q.queued || 0) + (q.claimed || 0) + (q.sending || 0) : 0);
  const nDead = q ? (q.failed || 0) + (q.needsAttention || 0) : 0;
  if (g.sendDisabled) add("send", "Sending", "down", "Sending is switched off in the inbox" + (g.sendDisabledReason ? ` (${clean(g.sendDisabledReason, 120)})` : "") + ". Messages wait here until it is on again.", "Sending paused", 0);
  else if (!ok("drafts") || (!drafts && !q)) add("send", "Sending", "warn", "Could not look at the messages waiting to go out.", "Sending unknown", 2);
  else if (q && q.stalled) add("send", "Sending", "down", `Sending is stuck: a message has waited ${waited ? ago(waited) : "10 min"} with no progress.`, "Sending stuck", 2);
  else if (waited > T.workDown) add("send", "Sending", "down", `A message has waited ${ago(waited)} for the inbox's Etsy helper to send it.`, "Not sending", 2);
  else if (waited > T.workWarn) add("send", "Sending", "warn", `A message has waited ${ago(waited)} for the inbox's Etsy helper.`, "Sending slowly", 2);
  else if (nDead > 0) add("send", "Sending", "warn", `${nDead} ${nDead === 1 ? "message to a customer needs" : "messages to customers need"} a person: ${nDead === 1 ? "it" : "they"} did not go out. Open the conversation to retry or send by hand.`, "Messages need attention", 2);
  else add("send", "Sending", "ok", nQueued ? `${nQueued} ${nQueued === 1 ? "message" : "messages"} on the way` : "Nothing waiting to go out");

  // ── noticing answers: the Gmail watcher sees Etsy's email about each new message within a minute or two ──
  const w = ev.watcher || null, gs = ev.gmail || null;
  const done = gs ? tsMs(gs.lastSyncCompletedAt) : 0;
  const failedRaw = gs && gs.lastSyncError && tsMs(gs.lastSyncErrorAt) > done ? String(gs.lastSyncError) : "";
  const failed = failedRaw ? plainError(failedRaw) : "";
  const tokenDead = /invalid_grant|Gmail token refresh failed: 4\d\d|token (has been )?(expired|revoked)/i.test(failedRaw);
  if (!ok("watcher") || !ok("gmail")) add("notice", "Noticing answers", "warn", "Could not look at the inbox's Gmail watcher.", "Receiving unknown", 3);
  else if (!w || w.enabled !== true) add("notice", "Noticing answers", "down", "The inbox is not watching for new Etsy messages (its Gmail watcher is off), so answers will not arrive.", "Not receiving", 3);
  else if (tokenDead) add("notice", "Noticing answers", "down", "The inbox's Gmail sign-in has expired, so new Etsy messages are not being noticed. Sign in to Gmail again in the inbox's Gmail settings.", "Gmail sign-in expired", 3);
  else if (!done) add("notice", "Noticing answers", "warn", failed ? `The inbox's check for new Etsy messages failed: ${failed}.` : "No check for new Etsy messages recorded yet.", failed ? "Answers delayed" : "Receiving unknown", 3);
  else if (now - done > T.gmailDown) add("notice", "Noticing answers", "down", `The inbox last checked for new Etsy messages ${ago(now - done)} ago${failed ? ", and then failed: " + failed : ""}.`, "Not receiving", 3);
  else if (failed) add("notice", "Noticing answers", "warn", `The inbox's last check for new Etsy messages failed: ${failed}.`, "Answers delayed", 3);
  else if (now - done > T.gmailWarn) add("notice", "Noticing answers", "warn", `The inbox last checked for new Etsy messages ${ago(now - done)} ago.`, "Answers delayed", 3);
  else add("notice", "Noticing answers", "ok", `Checked for new Etsy messages ${ago(now - done)} ago`);

  // ── reading answers in: each new message is a scrape job for the helper, and its page must still be readable ──
  const sh = ev.scrapeHealth || null;
  const badRun = sh && (sh.consecutiveBad || 0) >= T.pageBadRuns && typeof sh.lastBadAtMs === "number" && now - sh.lastBadAtMs < DAY;
  const waitText = `${ns}${ns >= 40 ? "+" : ""} new Etsy ${ns === 1 ? "message is" : "messages are"} waiting to be read into the inbox, the oldest for ${ago(late)}.`;
  if (badRun) {
    const signedOut = sh.lastBadReason === "Etsy signed the extension out";
    const since = ` The last failure was ${ago(now - sh.lastBadAtMs)} ago and nothing has been read since.`;
    add("read", "Reading answers", now - sh.lastBadAtMs < 2 * HOUR ? "down" : "warn", (signedOut
      ? "The inbox's Etsy helper is signed out of Etsy, so new messages cannot be read. Sign in to Etsy in the browser that has the extension."
      : `The last ${sh.consecutiveBad} readings of an Etsy conversation failed (${clean(sh.lastBadReason, 100) || "no messages read"}). Etsy may have changed its page.`) + since,
    signedOut ? "Signed out of Etsy" : "Etsy page not read", 4);
  }
  else if (!ok("jobs") || !jobs) add("read", "Reading answers", "warn", "Could not look at the messages waiting to be read.", "Reading unknown", 4);
  else if (late > T.readDown) add("read", "Reading answers", "down", waitText, "Answers delayed", 4);
  else if (late > T.readWarn) add("read", "Reading answers", "warn", waitText, "Answers delayed", 4);
  else add("read", "Reading answers", "ok", ns ? `${ns} being read now` : "Nothing waiting to be read");

  // ── the catch-up pass (the reaper's order_links pass): runs every five minutes and re-reads anything a scrape missed ──
  const reAt = ev.bell && ev.bell.reconcileAtMs ? ev.bell.reconcileAtMs : 0;
  const reErrAt = ev.bell && ev.bell.reconcileErrorAtMs ? ev.bell.reconcileErrorAtMs : 0;
  if (!ok("bell")) add("catchup", "Catching up", "warn", "Could not look at the catch-up pass.", "Catch-up unknown", 6);
  else if (reErrAt > reAt && now - reErrAt < T.catchUpDown) add("catchup", "Catching up", "warn", `The catch-up pass that re-reads missed answers failed ${ago(now - reErrAt)} ago (${plainError(ev.bell.reconcileError)}).`, "Catch-up failed", 6);
  else if (!reAt) add("catchup", "Catching up", "warn", "The catch-up pass that re-reads missed answers has not reported yet.", "Catch-up not reported", 6);
  else if (now - reAt > T.catchUpDown) add("catchup", "Catching up", "down", `The catch-up pass that re-reads missed answers has not run for ${ago(now - reAt)}.`, "Catch-up stopped", 6);
  else if (now - reAt > T.catchUpWarn) add("catchup", "Catching up", "warn", `The catch-up pass that re-reads missed answers last ran ${ago(now - reAt)} ago.`, "Catch-up late", 6);
  else add("catchup", "Catching up", "ok", `Ran ${ago(now - reAt)} ago`);

  // ── the Etsy order data (buyers of orders): only a nuisance for messages, so never red ──
  const mr = ev.mirror || null;
  if (mr && mr.enabled !== false && ok("mirror")) {
    const mdone = tsMs(mr.lastSyncCompletedAt);
    const merr = mr.lastSyncErrorMsg ? plainError(mr.lastSyncErrorMsg) : "";
    if (/sign-in needs renewing/.test(merr)) add("orders", "Etsy order data", "warn", `Etsy's sign-in needs renewing, so order data is not updating${mdone ? " (last update " + ago(now - mdone) + " ago)" : ""}. A new order's buyer may not be found until the Etsy connection is renewed in the inbox settings.`, "Etsy sign-in", 8);
    else if (merr && mdone && now - mdone > T.mirrorWarn) add("orders", "Etsy order data", "warn", `Order data from Etsy is not updating (${merr}); the last update was ${ago(now - mdone)} ago. A new order's buyer may not be found.`, "Order data old", 8);
    else if (mdone && now - mdone > T.mirrorDown) add("orders", "Etsy order data", "warn", `Order data from Etsy has not updated for ${ago(now - mdone)}. A new order's buyer may not be found.`, "Order data old", 8);
    else if (mdone && now - mdone > T.mirrorWarn) add("orders", "Etsy order data", "warn", `Order data from Etsy last updated ${ago(now - mdone)} ago.`, "Order data old", 8);
    else add("orders", "Etsy order data", "ok", mdone ? `Updated ${ago(now - mdone)} ago` : "No update recorded yet");
  }

  // ── the watcher of the watchers: the monitor that writes this judgement, and the nightly check ──
  if (!ev.self) {
    const mo = ev.monitor && ev.monitor.atMs ? now - ev.monitor.atMs : null;
    if (!ok("monitor")) add("monitor", "Link monitor", "warn", "Could not look at the link monitor.", "Monitor unknown", 7);
    else if (mo == null) add("monitor", "Link monitor", "warn", "The link monitor has not reported yet.", "Monitor not reported", 7);
    else if (mo > T.monitorDown) add("monitor", "Link monitor", "down", `The link monitor has not run for ${ago(mo)}, so a problem could go unreported.`, "Monitor stopped", 7);
    else if (mo > T.monitorWarn) add("monitor", "Link monitor", "warn", `The link monitor last ran ${ago(mo)} ago.`, "Monitor late", 7);
    else add("monitor", "Link monitor", "ok", `Ran ${ago(mo)} ago`);
  }
  const cs = ev.consistency || null;
  if (cs && cs.atMs && now - cs.atMs < T.consistencyStale && (cs.mismatched || 0) > 0) {
    const n = cs.mismatched;
    add("consistency", "Nightly check", "warn", `${n} ${n === 1 ? "conversation shows" : "conversations show"} fewer messages in the inbox than Etsy sent; the inbox is re-reading ${n === 1 ? "it" : "them"}.`, "Messages missing", 9);
  } else if (cs && cs.atMs && now - cs.atMs < T.consistencyStale) add("consistency", "Nightly check", "ok", `Every conversation checked matched (${cs.checked || 0}), ${ago(now - cs.atMs)} ago`);

  const worst = checks.slice().sort((a, b) => RANK[b.level] - RANK[a.level] || a.pri - b.pri)[0];
  const level = worst && RANK[worst.level] ? worst.level : "ok";
  return {
    at: now, level, short: level === "ok" ? "" : worst.short, problem: level === "ok" ? "" : worst.text,
    checks: checks.map(({ id, label, level, text }) => ({ id, label, level, text }))
  };
}

/** Read the evidence: a handful of Firestore reads, none of them Etsy. A read that fails is named in readOk, never taken as "fine". */
async function gather(db, opts = {}) {
  const readOk = {};
  const soft = (name, p) => p.then(v => v, e => { readOk[name] = false; console.warn("linkHealth read " + name + ":", e && e.message); return null; });
  const doc = s => s && s.exists ? s.data() : null;
  const cfg = db.collection("EtsyMail_Config"), meta = db.collection("EtsyMail_OrderLinkMeta");
  const [global, watcher, gmail, helper, drafts, jobs, scrapeHealth, mirror, bell, monitor, consistency] = await Promise.all([
    soft("global", cfg.doc("global").get()), soft("watcher", cfg.doc("gmailWatcher").get()), soft("gmail", cfg.doc("gmailSyncState").get()),
    soft("helper", meta.doc("helper").get()),
    soft("drafts", db.collection("EtsyMail_Drafts").where("status", "==", "queued").limit(25).get()),
    soft("jobs", db.collection("EtsyMail_Jobs").where("status", "==", "queued").limit(40).get()),
    soft("scrapeHealth", cfg.doc("scrapeHealth").get()), soft("mirror", cfg.doc("receiptsMirrorState").get()),
    soft("bell", meta.doc("bell").get()),
    opts.self ? Promise.resolve(null) : soft("monitor", cfg.doc("linkHealth").get()),
    soft("consistency", cfg.doc("linkConsistency").get())
  ]);
  // MAILQUEUE's send queue, when this build has it (read-only; a failing read is named, never taken as "fine")
  let queue = null;
  try {
    const Q = require("./_etsyMailSendQueue");
    if (Q && typeof Q.summary === "function") queue = await Q.summary();
  } catch (e) { if (!/Cannot find module/.test(String(e && e.message))) { readOk.drafts = false; console.warn("linkHealth queue summary:", e && e.message); } }
  // the bell is a large document; only its stamp matters here
  const b = doc(bell);
  return {
    global: doc(global), watcher: doc(watcher), gmail: doc(gmail), helper: doc(helper), scrapeHealth: doc(scrapeHealth), mirror: doc(mirror),
    bell: b ? { reconcileAtMs: b.reconcileAtMs || 0, reconcileErrorAtMs: b.reconcileErrorAtMs || 0, reconcileError: b.reconcileError || "", link: b.link || null } : null,
    monitor: doc(monitor), consistency: doc(consistency), queue,
    queuedDrafts: drafts ? drafts.docs.map(d => tsMs(d.data().queuedAt) || 0) : null,
    scrapeJobs: jobs ? jobs.docs.map(d => d.data()).filter(j => j.jobType === "scrape").map(j => tsMs(j.createdAt) || 0) : null,
    readOk, self: !!opts.self
  };
}

/** The next linkHealth document: the judgement, plus when this trouble began (the alert counts from there). */
function nextDoc(prev, res, now) {
  const bad = res.level !== "ok";
  const wasBad = !!(prev && prev.level && prev.level !== "ok" && prev.downSinceMs);
  const downSinceMs = bad ? (wasBad ? prev.downSinceMs : now) : 0;
  return {
    atMs: now, level: res.level, short: res.short, problem: res.problem,
    checks: res.checks, downSinceMs, incidentId: bad ? String(downSinceMs) : "",
    worstSinceMs: bad ? (wasBad && prev.level === res.level ? (prev.worstSinceMs || downSinceMs) : now) : 0
  };
}
/** The four fields every sorter reads in its sync answer. */
const summaryOf = d => ({ level: d.level, short: d.short, problem: d.problem, atMs: d.atMs, downSinceMs: d.downSinceMs || 0, incidentId: d.incidentId || "" });

module.exports = { evaluate, gather, nextDoc, summaryOf, plainError, T, RANK, tsMs, ago };
