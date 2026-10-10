// MAILROBUST (10 Oct 2026): the customer-mail line's health must be TRUE and an error must never read as "no messages".
// Offline: the in-memory Firestore of tests/charm-nest/_sandboxFakes.cjs (refuses nested arrays), the real
// _etsyMailLinkHealth / etsyMailLinkWatchdog / _etsyMailOrderLink code. No network, no Etsy, no AI, nothing is sent.
//
//   node tests/etsy-mail/link-health.cjs
"use strict";
const path = require("path"), assert = require("assert"), Module = require("module");
const Fk = require("../charm-nest/_sandboxFakes.cjs"); Fk.install();
const { store, TS, writes } = Fk;
const fnDir = path.join(__dirname, "../../netlify/functions");
const warn = [], realWarn = console.warn; console.warn = (...a) => warn.push(a.map(String).join(" "));

// Etsy and the sandbox's order emulation are stubs the test steers
const etsy = { fail: null, calls: 0 }, sandbox = { status: 200, body: { receipt: { buyer_user_id: "9001" } }, throws: null };
const stub = (rel, exports) => { const p = require.resolve(path.join(fnDir, rel)); require.cache[p] = { id: p, filename: p, loaded: true, exports }; };
stub("_etsyMailEtsy.js", { getShopReceiptFull: async () => { etsy.calls++; if (etsy.fail) throw new Error(etsy.fail); return { buyer_user_id: "7001" }; } });
stub("etsySandbox.js", { serve: async () => { if (sandbox.throws) throw new Error(sandbox.throws); return { statusCode: sandbox.status, body: JSON.stringify(sandbox.body) }; } });
process.env.ETSYMAIL_EXTENSION_SECRET = "test-secret-not-real";

const LH = require(path.join(fnDir, "_etsyMailLinkHealth.js"));
const OL = require(path.join(fnDir, "_etsyMailOrderLink.js"));
const WD = require(path.join(fnDir, "etsyMailLinkWatchdog.js"));

const fails = [];
const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
const MIN = 60e3, HOUR = 60 * MIN;
const realDateNow = Date.now; let clock = null; Date.now = () => (clock == null ? realDateNow() : clock);
const T0 = realDateNow(); clock = T0;
const at = ms => new TS(ms);

// ── a healthy line, then one thing broken at a time ──
const healthy = () => ({
  global: {}, watcher: { enabled: true }, gmail: { lastSyncCompletedAt: at(T0 - MIN) }, helper: { seenAtMs: T0 - 30e3 },
  scrapeHealth: { consecutiveBad: 0, lastOkAtMs: T0 - 5 * MIN }, mirror: { enabled: true, lastSyncCompletedAt: at(T0 - 3 * MIN) },
  bell: { reconcileAtMs: T0 - 2 * MIN }, monitor: { atMs: T0 - 2 * MIN }, consistency: null,
  queuedDrafts: [], scrapeJobs: [], readOk: {}, queue: null
});
const judge = patch => LH.evaluate(Object.assign(healthy(), patch), T0);
const lvl = (r, id) => (r.checks.find(c => c.id === id) || {}).level;
console.log("The judgement");
let r = judge({});
check(r.level === "ok" && r.short === "" && r.checks.every(c => c.level === "ok"), "everything working: green, every check ok");
r = judge({ helper: null });
check(r.level === "warn" && /not heard from/i.test(r.short), "helper never heard from is amber, not green (" + r.short + ")");
check(judge({ helper: { seenAtMs: T0 - 13 * MIN } }).level === "warn" && judge({ helper: { seenAtMs: T0 - 31 * MIN } }).level === "down", "helper silent 13 min: amber; 31 min: red, with nothing waiting");
check(judge({ helper: { seenAtMs: T0 - 7 * MIN }, queuedDrafts: [T0 - 6 * MIN] }).level === "warn" && judge({ helper: { seenAtMs: T0 - 17 * MIN }, queuedDrafts: [T0 - 16 * MIN] }).level === "down", "helper quiet with a message waiting: amber at 5 min, red at 15");
check(judge({ watcher: { enabled: false } }).level === "down" && judge({ watcher: null }).level === "down", "Gmail watcher off or missing: red");
r = judge({ gmail: { lastSyncCompletedAt: at(T0 - 3 * MIN), lastSyncError: 'Gmail token refresh failed: 400 {"error":"invalid_grant"}', lastSyncErrorAt: at(T0 - 60e3) } });
check(r.level === "down" && /Gmail sign-in expired/.test(r.short) && !/invalid_grant|400/.test(r.problem), "an expired Gmail sign-in is named in plain words (red), without the raw error: " + r.problem.slice(0, 60));
check(judge({ gmail: { lastSyncCompletedAt: at(T0 - 11 * MIN) } }).level === "warn" && judge({ gmail: { lastSyncCompletedAt: at(T0 - 31 * MIN) } }).level === "down", "Gmail check 11 min old: amber; 31 min: red");
r = judge({ scrapeHealth: { consecutiveBad: 3, lastBadAtMs: T0 - 10 * MIN, lastBadReason: "Etsy signed the extension out" } });
check(r.level === "down" && r.short === "Signed out of Etsy", "three bad page reads (signed out): red, plain words");
check(judge({ scrapeHealth: { consecutiveBad: 3, lastBadAtMs: T0 - 5 * HOUR, lastBadReason: "x" } }).level === "warn", "the same failure five hours old: amber (nothing read since)");
check(judge({ scrapeHealth: { consecutiveBad: 2, lastBadAtMs: T0 - MIN, lastBadReason: "x" } }).level === "ok", "two bad page reads are not yet a verdict");
check(judge({ scrapeJobs: [T0 - 11 * MIN] }).level === "warn" && judge({ scrapeJobs: [T0 - 31 * MIN] }).level === "down", "a new Etsy message waiting to be read: amber at 10 min, red at 30");
check(judge({ global: { sendDisabled: true, sendDisabledReason: "kill switch" } }).short === "Sending paused", "the inbox's send switch off is red and named first");
r = judge({ global: { sendDisabled: true }, helper: { seenAtMs: T0 - HOUR } });
check(r.short === "Sending paused", "when two things are red the most basic one is named first");
check(judge({ queue: { queued: 1, claimed: 0, sending: 0, failed: 0, needsAttention: 0, oldestQueuedAtMs: T0 - MIN, stalled: true } }).level === "down", "MAILQUEUE's stalled flag: red");
r = judge({ queue: { queued: 0, claimed: 0, sending: 0, failed: 1, needsAttention: 1, oldestQueuedAtMs: 0, stalled: false } });
check(r.level === "warn" && /2 messages to customers need a person/.test(r.problem), "failed or needs-attention messages are amber with the count");
check(judge({ bell: { reconcileAtMs: 0 } }).level === "warn" && judge({ bell: { reconcileAtMs: T0 - 21 * MIN } }).level === "warn" && judge({ bell: { reconcileAtMs: T0 - 4 * HOUR } }).level === "down", "the catch-up pass: never reported amber, 21 min amber, 4 h red");
check(/failed/.test((judge({ bell: { reconcileAtMs: T0 - 8 * MIN, reconcileErrorAtMs: T0 - 2 * MIN, reconcileError: "boom" } }).checks.find(c => c.id === "catchup") || {}).text), "a failed catch-up pass says so");
check(judge({ monitor: null }).level === "warn" && judge({ monitor: { atMs: T0 - 13 * MIN } }).level === "warn" && judge({ monitor: { atMs: T0 - HOUR } }).level === "down", "the link monitor not reporting or stopped is shown");
check(!LH.evaluate(Object.assign(healthy(), { monitor: null, self: true }), T0).checks.some(c => c.id === "monitor"), "the monitor does not judge its own stamp");
check(judge({ mirror: { enabled: true, lastSyncCompletedAt: at(T0 - 8 * HOUR), lastSyncErrorMsg: "daily_rate_limit" } }).level === "warn", "Etsy order data stale: amber, never red");
r = judge({ mirror: { enabled: true, lastSyncCompletedAt: at(T0 - 2 * MIN), lastSyncErrorMsg: "auth: Etsy token refresh failed: 401" } });
check(r.level === "warn" && r.short === "Etsy sign-in" && /sign-in needs renewing/.test(r.problem), "the Etsy token failing to refresh is amber at once and named (" + r.short + ")");
check(judge({ mirror: { enabled: true, lastSyncCompletedAt: at(T0 - 2 * MIN), lastSyncErrorMsg: null } }).level === "ok", "no error recorded by the last order-data run: green");
check(judge({ consistency: { atMs: T0 - HOUR, checked: 100, mismatched: 3 } }).level === "warn", "the nightly check finding missing messages: amber");
check(judge({ consistency: { atMs: T0 - HOUR, checked: 100, mismatched: 0 } }).level === "ok", "the nightly check finding nothing: still green");
for (const name of ["watcher", "gmail", "helper", "jobs", "drafts", "bell", "monitor"]) {
  const x = judge({ readOk: { [name]: false } });
  check(x.level !== "ok", "a read that failed (" + name + ") is never taken as fine (" + x.level + ")");
}
check(JSON.stringify(judge({})).length < 2500, "the judgement stays small");

// ── the monitor: judgement written, trouble timed, catch-up run when the reaper has not ──
(async () => {
  console.log("The watchdog");
  store.clear();
  const seed = (p, d) => store.set(p, d);
  const base = () => {
    seed("EtsyMail_Config/global", {}); seed("EtsyMail_Config/gmailWatcher", { enabled: true });
    seed("EtsyMail_Config/gmailSyncState", { lastSyncCompletedAt: at(clock - MIN) });
    seed("EtsyMail_OrderLinkMeta/helper", { seenAtMs: clock - 20e3 });
    seed("EtsyMail_OrderLinkMeta/bell", { n: 3, reconcileAtMs: clock - 2 * MIN });
    seed("EtsyMail_Config/receiptsMirrorState", { enabled: true, lastSyncCompletedAt: at(clock - 3 * MIN) });
  };
  base();
  const fresh = (extra = {}) => {   // every part of the line reporting at the current clock, then what the step changes
    seed("EtsyMail_Config/gmailSyncState", { lastSyncCompletedAt: at(clock - MIN) });
    seed("EtsyMail_Config/receiptsMirrorState", { enabled: true, lastSyncCompletedAt: at(clock - 3 * MIN) });
    seed("EtsyMail_OrderLinkMeta/helper", { seenAtMs: clock - 20e3 });
    seed("EtsyMail_OrderLinkMeta/bell", Object.assign({}, store.get("EtsyMail_OrderLinkMeta/bell"), { reconcileAtMs: clock - MIN }, extra));
  };
  let ran = 0;
  const fakeReconcile = async () => { ran++; const b = store.get("EtsyMail_OrderLinkMeta/bell"); store.set("EtsyMail_OrderLinkMeta/bell", Object.assign({}, b, { reconcileAtMs: clock })); return { open: 0 }; };
  let out = await WD.run({ now: clock, reconcile: fakeReconcile });
  let doc = store.get("EtsyMail_Config/linkHealth"), bell = store.get("EtsyMail_OrderLinkMeta/bell");
  check(out.level === "ok" && doc.level === "ok" && doc.atMs === clock && bell.link.level === "ok" && bell.link.atMs === clock, "a healthy line: linkHealth and the bell summary say ok");
  check(ran === 0, "the reaper reported lately: the monitor does not run the catch-up itself");
  check(doc.auth && typeof doc.auth.secretSet === "boolean" && !JSON.stringify(doc).includes("test-secret-not-real"), "the document records only whether a secret is set, never its value");
  check(bell.n === 3, "the bell's change counter is untouched (no sorter wakes for a judgement)");
  // the helper goes quiet
  clock += 14 * MIN; fresh(); seed("EtsyMail_OrderLinkMeta/helper", { seenAtMs: clock - 14 * MIN });
  out = await WD.run({ now: clock, reconcile: fakeReconcile });
  doc = store.get("EtsyMail_Config/linkHealth");
  check(doc.level === "warn" && /Helper quiet/.test(doc.short) && doc.downSinceMs === clock && doc.incidentId === String(clock), "helper quiet 14 min: amber with the time the trouble began");
  const began = doc.downSinceMs;
  clock += 5 * MIN; fresh(); seed("EtsyMail_OrderLinkMeta/helper", { seenAtMs: clock - 19 * MIN });
  await WD.run({ now: clock, reconcile: fakeReconcile });
  doc = store.get("EtsyMail_Config/linkHealth");
  check(doc.downSinceMs === began && doc.incidentId === String(began), "five minutes later it is the same incident (the alert counts from the first sign)");
  clock += 15 * MIN; fresh(); seed("EtsyMail_OrderLinkMeta/helper", { seenAtMs: clock - 34 * MIN });
  await WD.run({ now: clock, reconcile: fakeReconcile });
  doc = store.get("EtsyMail_Config/linkHealth");
  check(doc.level === "down" && doc.downSinceMs === began, "helper silent 34 min: red, same incident");
  clock += 5 * MIN; fresh();
  await WD.run({ now: clock, reconcile: fakeReconcile });
  doc = store.get("EtsyMail_Config/linkHealth");
  check(doc.level === "ok" && !doc.downSinceMs && doc.incidentId === "" && store.get("EtsyMail_OrderLinkMeta/bell").link.level === "ok", "the helper is back: green again and the incident closes");

  // the reaper stopped: the monitor runs the catch-up pass itself
  fresh({ reconcileAtMs: clock - 15 * MIN });
  ran = 0; out = await WD.run({ now: clock, reconcile: fakeReconcile });
  check(ran === 1 && out.catchUp && out.catchUp.ran && out.level === "ok", "the reaper has not reported for 15 min: the monitor runs the catch-up pass once, and the line is judged with it");
  out = await WD.run({ now: clock, reconcile: fakeReconcile });
  check(ran === 1, "the next run (stamp fresh again) does not run it again");
  // the catch-up itself fails, and has not worked for 25 minutes
  fresh({ reconcileAtMs: clock - 25 * MIN });
  out = await WD.run({ now: clock, reconcile: async () => { throw new Error("firestore unavailable"); } });
  doc = store.get("EtsyMail_Config/linkHealth");
  check(out.catchUp.error && doc.level === "warn" && /Catch-up/.test(doc.short), "a catch-up that throws does not stop the monitor: it is shown amber (" + doc.short + ")");
  // an unreadable evidence document is amber, never green
  base(); fresh(); const realGet = Fk.db.collection.bind(Fk.db);
  Fk.db.collection = c => { const q = realGet(c); if (c !== "EtsyMail_Config") return q; const d = q.doc; q.doc = id => { const x = d(id); if (id === "gmailWatcher") x.get = async () => { throw new Error("14 UNAVAILABLE"); }; return x; }; return q; };
  await WD.run({ now: clock, reconcile: fakeReconcile });
  doc = store.get("EtsyMail_Config/linkHealth");
  check(doc.level !== "ok" && /Receiving unknown/.test(doc.short), "the Gmail watcher document cannot be read: amber 'Receiving unknown', not green");
  Fk.db.collection = realGet;
  check(writes.every(w => !/EtsyMail_(Threads|Drafts|Jobs|OrderLinks|Receipts)\b/.test(w.path)), "the monitor wrote nothing in the conversation, draft, job, question or receipt collections");

  // ── the sorter's answers: sync carries the judgement; health uses the same rules ──
  console.log("The sorter's answers");
  base(); seed("EtsyMail_OrderLinkMeta/bell", { n: 9, reconcileAtMs: clock - MIN, link: { level: "down", short: "Helper offline", problem: "x", atMs: clock - 60e3, downSinceMs: clock - 600e3, incidentId: "abc" } });
  const station = { id: "s" };
  const sync = await OL.sync({ n: 9, since: 5, sandbox: false });
  check(sync.link && sync.link.level === "down" && sync.link.incidentId === "abc" && sync.changes.length === 0, "sync (nothing changed) still carries the monitor's judgement, at no extra read");
  clock += 60e3; let h = await OL.health({ fresh: true });
  check(h.level === "ok", "health on a healthy line is ok (" + h.level + ")");
  clock += 60e3; seed("EtsyMail_OrderLinkMeta/helper", { seenAtMs: clock - 40 * MIN });
  h = await OL.health({ fresh: true });
  check(h.level === "down" && h.short === "Helper offline" && h.checks.some(c => c.id === "catchup") && h.checks.some(c => c.id === "monitor"), "health: helper silent 40 min is red, and the details list the catch-up pass and the monitor");
  seed("EtsyMail_OrderLinkMeta/helper", { seenAtMs: clock - 5e3 });

  // ── "no messages" is never shown for an error ──
  console.log("Honest empties");
  const RID = "4170000001";
  seed("EtsyMail_Threads/etsy_conv_1", { id: "etsy_conv_1", etsyOrderId: RID, buyerUserId: "7001", customerName: "Ann", updatedAt: at(clock - HOUR) });
  seed("EtsyMail_Threads/etsy_conv_1/messages/m1", { direction: "inbound", text: "hi", timestamp: at(clock - HOUR) });
  seed("EtsyMail_Threads/etsy_conv_1/messages/m2", { direction: "outbound", text: "hello", timestamp: at(clock - 50 * MIN) });
  let info = await OL.historyInfo({ receiptId: RID });
  check(info.why === "ok" && info.total === 2 && info.exact === true, "a buyer with a conversation: counted, why ok");
  // the count fails
  const rc = Fk.db.collection.bind(Fk.db);
  Fk.db.collection = c => { const q = rc(c); if (c !== "EtsyMail_Threads") return q; const d = q.doc; q.doc = id => { const x = d(id); const sub = x.collection; x.collection = s => { const y = sub(s); if (s === "messages") y.count = () => ({ get: async () => { throw new Error("14 UNAVAILABLE: connection reset"); } }); return y; }; return x; }; return q; };
  info = await OL.historyInfo({ receiptId: RID });
  check(info.why === "count_failed" && info.exact === false && /connection dropped/.test(info.reason) && info.total === 0, "the count fails: why count_failed with a plain reason (it used to read as total 0, 'No messages with this buyer')");
  Fk.db.collection = rc;
  // an order with no conversation at all, Etsy fails
  const R2 = "4170000002";
  etsy.fail = "Etsy API 503 service unavailable"; etsy.calls = 0;
  info = await OL.historyInfo({ receiptId: R2 });
  check(info.why === "lookup_failed" && /Etsy could not be asked/.test(info.reason) && info.threads.length === 0, "no conversation and Etsy failed: lookup_failed with the reason, never 'none' (" + info.reason + ")");
  const ord = await OL.order({ receiptId: R2 });
  check(ord.conversation === null && ord.lookupFailed && /Etsy could not be asked/.test(ord.lookupFailed), "the order answer says the lookup failed instead of 'this buyer has not written'");
  const calls1 = etsy.calls;
  clock += 20e3; await OL.order({ receiptId: R2 }); await OL.historyInfo({ receiptId: R2 });
  check(etsy.calls === calls1, "asked again 20 s later: the failure is remembered, Etsy is not called again (" + calls1 + " calls)");
  clock += 6 * MIN; etsy.fail = null;
  info = await OL.historyInfo({ receiptId: R2 });
  check(etsy.calls === calls1 + 1, "five minutes on, the lookup is tried again (a failure used to hide the buyer for 6 hours)");
  check(info.why === "ok" || info.why === "none", "and now it answers (" + info.why + ")");
  // Etsy answers: no buyer on record (a guest order) is a real "none" and is remembered
  const R3 = "4170000003"; etsy.fail = null;
  const stubOk = require.cache[require.resolve(path.join(fnDir, "_etsyMailEtsy.js"))].exports; const was = stubOk.getShopReceiptFull;
  stubOk.getShopReceiptFull = async () => ({ buyer_user_id: null });
  info = await OL.historyInfo({ receiptId: R3 });
  check(info.why === "no_buyer" && info.threads.length === 0, "Etsy answered with no buyer: 'no_buyer' (the inbox does not know who bought it), not 'no messages'");
  stubOk.getShopReceiptFull = was;
  // a 404 from Etsy is final for hours and says so
  const R4 = "4170000004"; etsy.fail = "Etsy API 404 not found"; etsy.calls = 0;
  await OL.historyInfo({ receiptId: R4 }); const c4 = etsy.calls; clock += 10 * MIN; await OL.historyInfo({ receiptId: R4 });
  check(etsy.calls === c4, "a 404 (no such order) is not asked again within hours");
  etsy.fail = null;
  // sandbox
  sandbox.throws = "emulator offline";
  info = await OL.historyInfo({ receiptId: "4170000005", sandbox: true });
  check(info.why === "lookup_failed" && /sandbox/.test(info.reason), "a sandbox order whose buyer cannot be found: lookup_failed, not 'No messages with this buyer' (" + info.reason + ")");
  sandbox.throws = null; sandbox.status = 502;
  clock += MIN; info = await OL.historyInfo({ receiptId: "4170000005", sandbox: true });
  check(info.why === "lookup_failed" && /502/.test(info.reason), "the sandbox's lookup answering an error: the same");
  sandbox.status = 404; clock += MIN;
  info = await OL.historyInfo({ receiptId: "4170000005", sandbox: true });
  check(info.why === "no_buyer", "the sandbox has no copy of the order and nothing stored names the buyer: 'no_buyer', not a failure and not 'no messages'");
  sandbox.status = 200; clock += 3 * MIN;
  info = await OL.historyInfo({ receiptId: "4170000005", sandbox: true });
  check(info.why === "none" && info.reason === "", "the sandbox names a buyer who has no conversation: a real 'none'");

  check(!writes.some(w => /EtsyMail_Threads\/[^/]+\/messages/.test(w.path) && w.kind !== "set"), "none of this deleted or rewrote a stored message");
  console.warn = realWarn;
  console.log(fails.length ? "FAIL " + fails.length + ": " + fails.join(" | ") : "PASS");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.warn = realWarn; console.error(e); process.exit(1); });
