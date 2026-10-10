// The nightly consistency check (_etsyMailLinkConsistency.js): stored messages against what each conversation's record claims.
// Offline: the in-memory Firestore of tests/charm-nest/_sandboxFakes.cjs and the real module and watchdog. No network, no Etsy.
//   node tests/etsy-mail/link-consistency.cjs
"use strict";
const path = require("path");
const Fk = require("../charm-nest/_sandboxFakes.cjs"); Fk.install();
const { store, TS, writes, canon } = Fk;
const fnDir = path.join(__dirname, "../../netlify/functions");
const warn = [], realWarn = console.warn; console.warn = (...a) => warn.push(a.map(String).join(" "));
process.env.ETSYMAIL_EXTENSION_SECRET = "test-secret-not-real";

const CON = require(path.join(fnDir, "_etsyMailLinkConsistency.js"));
const WD = require(path.join(fnDir, "etsyMailLinkWatchdog.js"));
const LH = require(path.join(fnDir, "_etsyMailLinkHealth.js"));

const fails = [];
const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
const HOUR = 3600e3, MIN = 60e3;
// 08:00 UTC on a day: inside the night window (02:00-05:00 New York all year)
const NIGHT = Date.UTC(2026, 9, 11, 8, 0, 0), NOON = Date.UTC(2026, 9, 11, 15, 0, 0);
const T = ms => new TS(ms);
const put = (k, v) => store.set(k, v);
const msg = (tid, id, dir, ms) => put(`EtsyMail_Threads/${tid}/messages/${id}`, { direction: dir, text: id, timestamp: T(ms) });

const DAYAGO = NIGHT - 26 * HOUR;
// a: matches. b: counts 2, stores none. h: counts 3, stores none, and has no address to re-read from (so no job).
//  c: the customer wrote an hour ago, nothing that new is stored. d: still being read (from Gmail).
// e: matches, with the inbox's own just-sent stand-in. f: counts 5, stores 1 (fewer than counted). g: changed 5 minutes ago (in flight). h: no URL.
put("EtsyMail_Threads/ta", { status: "etsy_scraped", messageCount: 3, lastInboundAt: T(NIGHT - 5 * HOUR), updatedAt: T(NIGHT - 5 * HOUR), etsyConversationUrl: "https://e/a" });
msg("ta", "m1", "inbound", NIGHT - 9 * HOUR); msg("ta", "m2", "outbound", NIGHT - 7 * HOUR); msg("ta", "m3", "inbound", NIGHT - 5 * HOUR);
put("EtsyMail_Threads/tb", { status: "etsy_scraped", messageCount: 2, lastInboundAt: T(NIGHT - 6 * HOUR), updatedAt: T(NIGHT - 6 * HOUR), etsyConversationUrl: "https://e/b" });
put("EtsyMail_Threads/tc", { status: "etsy_scraped", messageCount: 2, lastInboundAt: T(NIGHT - 3 * HOUR), updatedAt: T(NIGHT - 3 * HOUR), etsyConversationUrl: "https://e/c" });
msg("tc", "m1", "inbound", NIGHT - 10 * HOUR); msg("tc", "m2", "outbound", NIGHT - 9 * HOUR);
put("EtsyMail_Threads/td", { status: "detected_from_gmail", messageCount: 0, lastInboundAt: T(NIGHT - 4 * HOUR), updatedAt: T(NIGHT - 4 * HOUR), etsyConversationUrl: "https://e/d" });
put("EtsyMail_Threads/te", { status: "etsy_scraped", messageCount: 2, lastInboundAt: T(NIGHT - 8 * HOUR), updatedAt: T(NIGHT - 2 * HOUR), etsyConversationUrl: "https://e/e" });
msg("te", "m1", "inbound", NIGHT - 8 * HOUR); msg("te", "m2", "outbound", NIGHT - 2 * HOUR); msg("te", "optim_draft_te", "outbound", NIGHT - 2 * HOUR);
put("EtsyMail_Threads/tf", { status: "etsy_scraped", messageCount: 5, lastInboundAt: T(NIGHT - 12 * HOUR), updatedAt: T(NIGHT - 11 * HOUR), etsyConversationUrl: "https://e/f" });
msg("tf", "m1", "inbound", NIGHT - 12 * HOUR);
put("EtsyMail_Threads/tg", { status: "etsy_scraped", messageCount: 4, lastInboundAt: T(NIGHT - 5 * MIN), updatedAt: T(NIGHT - 5 * MIN), etsyConversationUrl: "https://e/g" });
put("EtsyMail_Threads/th", { status: "etsy_scraped", messageCount: 3, lastInboundAt: T(NIGHT - 14 * HOUR), updatedAt: T(NIGHT - 14 * HOUR) });
put("EtsyMail_OrderLinkMeta/bell", { n: 1, reconcileAtMs: NIGHT - MIN });
put("EtsyMail_OrderLinkMeta/helper", { seenAtMs: NIGHT - 20e3 });
put("EtsyMail_Config/gmailWatcher", { enabled: true }); put("EtsyMail_Config/gmailSyncState", { lastSyncCompletedAt: T(NIGHT - MIN) });
put("EtsyMail_Config/scrapeHealth", { consecutiveBad: 0, lastOkAtMs: NIGHT - MIN });
put("EtsyMail_Config/receiptsMirrorState", { enabled: true, lastSyncCompletedAt: T(NIGHT - 2 * MIN) });
put("EtsyMail_Config/global", {});

const threadsBefore = () => [...store].filter(([k]) => k.startsWith("EtsyMail_Threads/")).map(([k, v]) => k + canon(v)).join("|");

(async () => {
  const before = threadsBefore();
  console.log("Not due");
  put("EtsyMail_Config/linkConsistency", { atMs: NIGHT - 3 * HOUR, checked: 5, mismatched: 0 });
  const db = require(path.join(fnDir, "firebaseAdmin")).firestore();
  let r = await CON.maybeRun(db, { now: NIGHT });
  check(r.ran === false, "checked 3 hours ago: not run again");
  store.set("EtsyMail_Config/linkConsistency", { atMs: NIGHT - 25 * HOUR, checked: 5, mismatched: 0 });
  r = await CON.maybeRun(db, { now: NOON });
  check(r.ran === false, "a day old but it is the middle of the working day (15:00 UTC): left for the night");
  store.set("EtsyMail_Config/linkConsistency", { atMs: NOON - 41 * HOUR, checked: 5, mismatched: 0 });
  r = await CON.maybeRun(db, { now: NOON });
  check(r.ran === true, "41 hours without a run: runs even by day (the nightly check itself must not stay silent)");
  store.set("EtsyMail_Config/linkConsistency", { atMs: NIGHT - 25 * HOUR, checked: 5, mismatched: 0 });
  for (const [k] of [...store]) if (k.startsWith("EtsyMail_Jobs/")) store.delete(k);

  console.log("The night run");
  writes.length = 0;
  r = await CON.maybeRun(db, { now: NIGHT });
  const d = r.doc;
  check(r.ran === true && d, "due: it ran");
  check(d.mismatched === 4 && d.kinds.claimed_but_none === 2 && d.kinds.inbound_missing === 1 && d.kinds.fewer_than_counted === 1, "found exactly the four bad conversations (claimed but none x2, customer message missing, fewer than counted): " + JSON.stringify(d.kinds));
  check(d.skippedFresh === 2, "a conversation still being read (from Gmail) or changed 5 minutes ago is left for the next night (" + d.skippedFresh + ")");
  check(d.checked === 6, "the others were checked (" + d.checked + ")");
  check(!d.samples.some(s => ["ta", "te", "td", "tg"].includes(s.threadId)), "matching conversations (one with the inbox's just-sent stand-in) are not reported");
  const jobs = [...store.keys()].filter(k => k.startsWith("EtsyMail_Jobs/"));
  check(jobs.length === 3 && jobs.every(k => /^EtsyMail_Jobs\/consistency_t[bcf]_20261011$/.test(k)), "one re-read per mismatched conversation, on a deterministic id: " + jobs.join(", "));
  const j = store.get(jobs[0]);
  check(j.jobType === "scrape" && j.status === "queued" && j.payload.rescrape === true && /^https:\/\/e\//.test(j.payload.etsyConversationUrl), "the job is an ordinary queued scrape of the conversation's own address");
  check(d.repairQueued === 3, "the record says three re-reads were queued");
  const nonCfg = writes.filter(w => !/^EtsyMail_(Config|Jobs)\//.test(w.path));
  check(nonCfg.length === 0 && !writes.some(w => w.kind === "delete"), "it wrote only its own record and the jobs; nothing deleted, no conversation or message touched");
  check(threadsBefore() === before, "every conversation and message is byte-identical");
  check(JSON.stringify(store.get("EtsyMail_Config/linkConsistency")).includes('"mismatched":4'), "the result is stored in EtsyMail_Config/linkConsistency");

  console.log("Again, and the monitor");
  r = await CON.maybeRun(db, { now: NIGHT + 10 * MIN });
  check(r.ran === false, "ten minutes later: not run again");
  const forced = await CON.maybeRun(db, { now: NIGHT + 20 * MIN, force: true });
  check(forced.ran === true && forced.doc.repairQueued === 0 && [...store.keys()].filter(k => k.startsWith("EtsyMail_Jobs/")).length === 3, "forced a second time the same day: no second job is queued for the same conversation");
  const res = LH.evaluate(Object.assign({ global: {}, watcher: { enabled: true }, gmail: { lastSyncCompletedAt: T(NIGHT - MIN) }, helper: { seenAtMs: NIGHT - 20e3 }, scrapeHealth: { consecutiveBad: 0 }, mirror: { enabled: true, lastSyncCompletedAt: T(NIGHT - 2 * MIN) }, bell: { reconcileAtMs: NIGHT - MIN }, monitor: { atMs: NIGHT - MIN }, queuedDrafts: [], scrapeJobs: [], readOk: {}, queue: null }, { consistency: forced.doc }), NIGHT + 20 * MIN);
  check(res.level === "warn" && (res.checks.find(c => c.id === "consistency") || {}).level === "warn", "the light turns amber while a night's check has mismatches: " + res.short);

  // the watchdog runs it by itself when it is due, and publishes the judgement
  store.set("EtsyMail_Config/linkConsistency", { atMs: NIGHT - 25 * HOUR, checked: 5, mismatched: 0 });
  for (const [k] of [...store]) if (k.startsWith("EtsyMail_Jobs/")) store.delete(k);
  const W = NIGHT + 30 * MIN;   // (the rest of the line is healthy at that moment)
  put("EtsyMail_OrderLinkMeta/helper", { seenAtMs: W - 20e3 }); put("EtsyMail_OrderLinkMeta/bell", { n: 1, reconcileAtMs: W - MIN });
  put("EtsyMail_Config/gmailSyncState", { lastSyncCompletedAt: T(W - MIN) }); put("EtsyMail_Config/scrapeHealth", { consecutiveBad: 0, lastOkAtMs: W - MIN });
  put("EtsyMail_Config/receiptsMirrorState", { enabled: true, lastSyncCompletedAt: T(W - 2 * MIN) });
  const out = await WD.run({ now: W, reconcile: async () => ({}) });
  check(out.consistency && out.consistency.ran === true, "the watchdog ran the nightly check on its own schedule");
  const ld = store.get("EtsyMail_Config/linkHealth");
  console.log("   linkHealth:", ld && ld.level, JSON.stringify(ld && ld.checks && ld.checks.map(c => c.id + ":" + c.level)));
  check(ld && ld.level === "warn" && ld.checks.filter(c => c.level !== "ok").map(c => c.id).join() === "consistency", "and published it: linkHealth is amber, and the nightly check is the only line that is not ok");
  const bell = store.get("EtsyMail_OrderLinkMeta/bell");
  check(bell && bell.link && bell.link.level === "warn", "the sorters' summary carries it too");

  // an unreadable Firestore is not 'all matched'
  const broken = { collection: () => ({ doc: () => ({ get: async () => { throw new Error("Firestore unavailable"); } }) }) };
  let threw = false; try { await CON.maybeRun(broken, { now: NIGHT }); } catch (_) { threw = true; }
  check(threw, "a check that cannot read says so (it throws; the watchdog records the error) instead of reporting a clean night");

  console.warn = realWarn;
  console.log(fails.length ? `\nFAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\nlink-consistency OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.warn = realWarn; console.error(e); console.log(warn.slice(-5).join("\n")); process.exit(1); });
