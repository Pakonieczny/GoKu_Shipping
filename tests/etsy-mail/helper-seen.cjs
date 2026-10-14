// MAILDOWN (12 Oct 2026): the inbox's Etsy helper (the Chrome extension) must be recorded as alive whenever it is heard from,
// not only when its random-skipping scrape poll happens to go ahead. Offline: the in-memory Firestore of
// tests/charm-nest/_sandboxFakes.cjs and the REAL etsyMailJobs, firestoreProxy and etsyMailDraftSend handlers plus
// _etsyMailLinkHealth. No network, no Etsy, no AI, nothing is sent.
//
//   1. the scrape poll (etsyMailJobs claim), the extension's check for queued drafts (firestoreProxy list) and the open Etsy
//      tab's peek (etsyMailDraftSend peek) each leave the mark; nothing else does (the inbox's own list of conversations
//      queued_for_auto_send, a list of drafts without that filter, a write, a call without the secret);
//   2. one write a minute per source at most; a failing write never fails the request;
//   3. a helper whose scrape poll has been quiet for hours but whose queue check runs every 30 s is judged alive, not "down";
//   4. the requests still get their normal answers (nothing is gated or changed).
//   node tests/etsy-mail/helper-seen.cjs
"use strict";
const path = require("path");
const Fk = require("../charm-nest/_sandboxFakes.cjs"); Fk.install();
const { store, TS, writes } = Fk;
const fnDir = path.join(__dirname, "../../netlify/functions");
const warn = [], realWarn = console.warn; console.warn = (...a) => warn.push(a.map(String).join(" "));
process.env.ETSYMAIL_EXTENSION_SECRET = "test-secret-not-real";

const HS = require(path.join(fnDir, "_etsyMailHelperSeen.js"));
const Jobs = require(path.join(fnDir, "etsyMailJobs.js"));
const Proxy = require(path.join(fnDir, "firestoreProxy.js"));
const Send = require(path.join(fnDir, "etsyMailDraftSend.js"));
const LH = require(path.join(fnDir, "_etsyMailLinkHealth.js"));

const fails = [];
const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
const MIN = 60e3, HOUR = 60 * MIN;
const realDateNow = Date.now; let clock = Date.UTC(2026, 9, 14, 7, 0, 0);   // 3 am on the East coast, where the extension reads least
Date.now = () => clock;
const HELPER = "EtsyMail_OrderLinkMeta/helper";
const helper = () => store.get(HELPER) || {};
const helperWrites = () => writes.filter(w => w.path === HELPER).length;
const hdr = { "x-etsymail-secret": "test-secret-not-real" };
const QUEUED = { op: "list", coll: "EtsyMail_Drafts", where: "status,==,queued", limit: "50" };
const get = (qs, headers = hdr) => Proxy.handler({ httpMethod: "GET", headers, queryStringParameters: qs, multiValueQueryStringParameters: {} });
const claim = (body = { op: "claim", workerId: "ext_abc_1", jobTypes: ["scrape"] }, headers = hdr) => Jobs.handler({ httpMethod: "POST", headers, body: JSON.stringify(body) });
const peek = (threadId, headers = hdr) => Send.handler({ httpMethod: "POST", headers, body: JSON.stringify({ op: "peek", threadId }) });
const sec = async ms => { clock += ms; };

(async () => {
  console.log("The marks");
  store.set("EtsyMail_Drafts/draft_t1", { status: "queued", text: "hello", threadId: "t1", queuedAt: new TS(clock - 60e3) });
  store.set("EtsyMail_Drafts/draft_t2", { status: "draft", text: "not queued", threadId: "t2" });
  store.set("EtsyMail_Config/global", {});

  // 1 · each of the three signs of life
  let r = await claim();
  check(r.statusCode === 200 && JSON.parse(r.body).job === null, "a scrape poll with no job still answers as before (job: null)");
  check(helper().seenAtMs === clock && helper().jobsAtMs === clock, "the scrape poll leaves the mark (seenAtMs and jobsAtMs)");
  await sec(5 * MIN);
  r = await get(QUEUED);
  let body = JSON.parse(r.body);
  check(r.statusCode === 200 && Array.isArray(body.docs) && body.docs.length === 1 && body.docs[0].id === "draft_t1", "the extension's queued-drafts check still answers with the queued drafts");
  check(helper().seenAtMs === clock && helper().pollAtMs === clock && helper().jobsAtMs === clock - 5 * MIN, "…and leaves the mark with its own field (pollAtMs), keeping the scrape poll's");
  await sec(5 * MIN);
  r = await peek("t1");
  check(r.statusCode === 200 && helper().seenAtMs === clock && helper().peekAtMs === clock, "an open Etsy tab's peek leaves the mark (peekAtMs) and is still answered (" + r.statusCode + ")");
  check(JSON.parse(r.body).queued === true, "…and the peek still offers the queued message");

  // 2 · what does not count
  const seen0 = helper().seenAtMs, w0 = helperWrites();
  await sec(5 * MIN);
  const noMark = async (what, fn) => { const before = helperWrites(); const out = await fn(); check(helperWrites() === before && helper().seenAtMs === seen0, "not a sign of life: " + what + " (" + out.statusCode + ")"); };
  await noMark("the inbox's list of conversations queued for auto-send", () => get({ op: "list", coll: "EtsyMail_Threads", where: "status,==,queued_for_auto_send", limit: "100" }));
  await noMark("a list of drafts that is not the extension's (queued_for_auto_send)", () => get({ op: "list", coll: "EtsyMail_Drafts", where: "status,==,queued_for_auto_send" }));
  await noMark("a list of drafts with no filter", () => get({ op: "list", coll: "EtsyMail_Drafts" }));
  await noMark("one draft read by id", () => get({ op: "get", coll: "EtsyMail_Drafts", id: "draft_t1" }));
  await noMark("a queued-drafts check on another collection", () => get({ op: "list", coll: "EtsyMail_Jobs", where: "status,==,queued" }));
  await noMark("the same check without the secret (refused)", () => get(QUEUED, {}));
  await noMark("the same check with a wrong secret (refused)", () => get(QUEUED, { "x-etsymail-secret": "wrong" }));
  await noMark("a job claim without the secret (refused)", () => claim(undefined, {}));
  await noMark("a peek without the secret (refused)", () => peek("t1", {}));
  await noMark("a peek with no thread named", () => peek(""));
  const post = await Proxy.handler({ httpMethod: "POST", headers: hdr, body: JSON.stringify({ op: "update", coll: "EtsyMail_Drafts", id: "draft_t2", data: { text: "edited" } }) });
  check(helper().seenAtMs === seen0, "not a sign of life: a write through the proxy (" + post.statusCode + ")");
  check(helperWrites() === w0, "no extra write to the helper document for any of them");

  // 3 · at most one write a minute per source, and a failing write is not a failure
  HS.reset(); clock += 10 * MIN;
  const base = helperWrites();
  await get(QUEUED); await sec(20e3); await get(QUEUED); await sec(20e3); await get(QUEUED);
  check(helperWrites() === base + 1, "three queue checks in 40 s: one write");
  await sec(30e3); await get(QUEUED);
  check(helperWrites() === base + 2 && helper().seenAtMs === clock, "the next one, a minute later: one more write, the mark is current");
  await claim(); await claim();
  check(helperWrites() === base + 3, "two scrape polls at once: one write (its own source)");
  HS.reset();
  const real = Fk.db.collection.bind(Fk.db);
  Fk.db.collection = c => { const q = real(c); if (c !== "EtsyMail_OrderLinkMeta") return q; const d = q.doc; q.doc = id => { const x = d(id); if (id === "helper") x.set = async () => { throw new Error("14 UNAVAILABLE"); }; return x; }; return q; };
  r = await get(QUEUED); body = JSON.parse(r.body);
  check(r.statusCode === 200 && body.docs.length === 1, "the helper mark cannot be written: the queued-drafts answer is the same");
  r = await peek("t1");
  check(r.statusCode === 200 && JSON.parse(r.body).queued === true, "…and the peek is answered");
  r = await claim();
  check(r.statusCode === 200, "…and the scrape poll is answered");
  Fk.db.collection = real; HS.reset();

  // 4 · the judgement: the scrape poll has been quiet for hours, the queue check runs every 30 s
  console.log("The judgement");
  store.clear(); writes.length = 0;
  const night = Date.UTC(2026, 9, 14, 7, 0, 0); clock = night;
  const seed = (p, d) => store.set(p, d);
  seed("EtsyMail_Config/global", {}); seed("EtsyMail_Config/gmailWatcher", { enabled: true });
  seed("EtsyMail_Config/gmailSyncState", { lastSyncCompletedAt: new TS(clock - MIN) });
  seed("EtsyMail_Config/receiptsMirrorState", { enabled: true, lastSyncCompletedAt: new TS(clock - 3 * MIN) });
  seed("EtsyMail_OrderLinkMeta/bell", { n: 1, reconcileAtMs: clock - MIN });
  seed("EtsyMail_Config/linkHealth", { atMs: clock - 2 * MIN });
  seed("EtsyMail_Config/scrapeHealth", { consecutiveBad: 0 });
  seed(HELPER, { seenAtMs: clock - 3 * HOUR, jobsAtMs: clock - 3 * HOUR });
  seed("EtsyMail_Jobs/j1", { jobType: "scrape", status: "queued", createdAt: new TS(clock - 20 * MIN) });
  let ev = await LH.gather(Fk.db, { self: false });
  let res = LH.evaluate(ev, clock);
  check(res.level === "ok" && res.problem === "" && res.checks.find(c => c.id === "helper").level === "wait" && /away \(last checked 3 h ago\)/.test(res.checks.find(c => c.id === "helper").text), "before: the helper last heard from 3 hours ago, a message waiting 20 min: its computer is off, which is a calm 'wait' and no alarm (14 Oct 2026)");
  HS.reset(); await get(QUEUED);   // the extension's 30-second queue check
  ev = await LH.gather(Fk.db, { self: false });
  res = LH.evaluate(ev, clock);
  check(ev.helper.pollAtMs === clock && ev.helper.jobsAtMs === clock - 3 * HOUR, "the queue check was heard; the scrape poll still has not run for 3 hours");
  check(res.level === "ok" && res.checks.find(c => c.id === "helper").level === "ok" && res.checks.find(c => c.id === "read").level === "ok", "after: the helper is alive and the message waiting 20 min at 3 am is just waiting its turn: green");
  clock += 31 * MIN;   // nothing is heard from the helper any more (everything else keeps reporting: the Gmail watcher, the monitor, the catch-up)
  seed("EtsyMail_Config/gmailSyncState", { lastSyncCompletedAt: new TS(clock - MIN) });
  seed("EtsyMail_Config/receiptsMirrorState", { enabled: true, lastSyncCompletedAt: new TS(clock - 3 * MIN) });
  ev = await LH.gather(Fk.db, { self: false }); ev.monitor = { atMs: clock - MIN }; ev.bell = Object.assign({}, ev.bell, { reconcileAtMs: clock - MIN });
  res = LH.evaluate(ev, clock);
  check(res.level === "ok" && res.problem === "" && res.checks.find(c => c.id === "helper").level === "wait" && /last checked 31 min ago/.test(res.checks.find(c => c.id === "helper").text), "31 minutes without a sign from any call: the helper is 'away' again (a calm line, no alarm), and it says how long");

  console.warn = realWarn; Date.now = realDateNow;
  console.log(fails.length ? "FAIL " + fails.length + ": " + fails.join(" | ") : "PASS");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.warn = realWarn; console.error(e); process.exit(1); });
