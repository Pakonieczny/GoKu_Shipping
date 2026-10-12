// The complete re-read that "Messages missing" asks for (MAILMISS): the real etsyMailSnapshot.js (what the Chrome extension talks to) and
// the real consistency check, over the in-memory Firestore of tests/charm-nest/_sandboxFakes.cjs. No network, no Etsy, no live data.
//   · a conversation the check marked (thread.fullReread) is "not known" to the extension's threadExists question, so it scrolls the whole
//     history; a scrape that was not a full one is answered needsFullScrape; an ordinary conversation is untouched by all of this;
//   · the complete read that follows stores what was missing, removes the mark and leaves readComplete (the proof that Etsy's page has
//     nothing the inbox lacks); an incomplete one (not full, signed out, nothing read) leaves the mark and proves nothing; a mark
//     older than 3 days is ignored;
//   · the whole chain: gap found at night -> re-read queued and marked -> extension asks threadExists -> full scrape stores the missing
//     messages -> the 5-minute look closes the notice with nobody pressing anything.
//   node tests/etsy-mail/link-reread.cjs
"use strict";
const path = require("path");
const Fk = require("../charm-nest/_sandboxFakes.cjs"); Fk.install();
const { store, TS, writes } = Fk;
const fnDir = path.join(__dirname, "../../netlify/functions");
process.env.ETSYMAIL_EXTENSION_SECRET = "s3cret-for-test";
const warn = [], realWarn = console.warn, realLog = console.log; console.warn = (...a) => warn.push(a.map(String).join(" "));
const net = []; const okFetch = global.fetch; global.fetch = async (...a) => { net.push(String(a[0])); return okFetch(...a); };

const snapshot = require(path.join(fnDir, "etsyMailSnapshot.js"));
const CON = require(path.join(fnDir, "_etsyMailLinkConsistency.js"));
const LH = require(path.join(fnDir, "_etsyMailLinkHealth.js"));
const db = require(path.join(fnDir, "firebaseAdmin")).firestore();

const fails = [];
const check = (ok, what) => { if (!ok) fails.push(what); realLog((ok ? "  ok   " : "  FAIL ") + what); };
const MIN = 60e3, HOUR = 3600e3, NOW = Fk.realNow();
const T = ms => new TS(ms);
const H = { "x-etsymail-secret": "s3cret-for-test" };
const call = async body => { const r = await snapshot.handler({ httpMethod: "POST", headers: H, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body) }; };

const CONV = "7001", TID = "etsy_conv_" + CONV;
const MSGS = Array.from({ length: 8 }, (_, i) => ({ i, senderRole: i % 2 ? "staff" : "customer", senderName: i % 2 ? "Shop" : "Cust", text: "message number " + i + " about the order", timestampMs: NOW - (60 - i) * HOUR, contentHash: "h" + i, messageType: "text", imageUrls: [], attachmentUrls: [] }));
const stored = m => ({ source: "etsy", direction: m.senderRole === "staff" ? "outbound" : "inbound", senderName: m.senderName, senderRole: m.senderRole, timestamp: T(m.timestampMs), text: m.text, normalizedText: m.text.toLowerCase(), contentHash: m.contentHash, messageType: "text", imageUrls: [], thumbnailUrls: [], listingCards: [], storageImagePaths: [], attachmentUrls: [], createdAt: T(m.timestampMs) });
const scrapeBody = (msgs, over) => Object.assign({
  scrapedAt: NOW, etsyConversationId: CONV, etsyConversationUrl: "https://www.etsy.com/your/conversations/" + CONV, threadDomHash: "dom" + Math.random(),
  participants: [], subject: "About the order", messages: msgs, session: { etsyLoggedIn: true }, diagnostics: { scrapeMode: "incremental" }
}, over || {});
const seed = (have, over) => {
  for (const k of [...store.keys()]) if (/^EtsyMail_(Threads|Jobs)\//.test(k)) store.delete(k);
  store.set("EtsyMail_Threads/" + TID, Object.assign({
    threadId: TID, etsyConversationId: CONV, etsyConversationUrl: "https://www.etsy.com/your/conversations/" + CONV, customerName: "Cust Name", status: "etsy_scraped",
    etsyOrderId: "4174999999", subject: "About the order", messageCount: have.length, lastInboundAt: T(MSGS[6].timestampMs), lastOutboundAt: T(MSGS[7].timestampMs),
    searchableText: "x", searchableMessageText: "x", updatedAt: T(NOW - 30 * HOUR), createdAt: T(NOW - 70 * HOUR), lastSyncedAt: T(NOW - 30 * HOUR), lastScrapedDomHash: "old"
  }, over || {}));
  for (const m of have) store.set(`EtsyMail_Threads/${TID}/messages/etsy_${m.contentHash}`, stored(m));
};
const th = () => store.get("EtsyMail_Threads/" + TID);
const countMsgs = () => [...store.keys()].filter(k => k.startsWith(`EtsyMail_Threads/${TID}/messages/`)).length;
const FRESH = { atMs: NOW - HOUR, jobId: "consistency_x" };

(async () => {
  console.log("threadExists");
  seed(MSGS.slice(0, 8));
  let r = await call({ op: "threadExists", etsyConversationId: CONV });
  check(r.status === 200 && r.body.exists === true, "an ordinary conversation is known (the extension reads only the newest messages, as before)");
  store.set("EtsyMail_Threads/" + TID, Object.assign({}, th(), { fullReread: FRESH }));
  r = await call({ op: "threadExists", etsyConversationId: CONV });
  check(r.body.exists === false && r.body.reason === "full_reread_requested", "a conversation marked for a complete re-read is 'not known': the extension scrolls the whole history");
  store.set("EtsyMail_Threads/" + TID, Object.assign({}, th(), { fullReread: { atMs: NOW - 4 * 24 * HOUR, jobId: "old" } }));
  r = await call({ op: "threadExists", etsyConversationId: CONV });
  check(r.body.exists === true, "a mark older than 3 days is ignored");

  console.log("Scrapes of a marked conversation");
  seed(MSGS.slice(0, 5), { fullReread: FRESH, messageCount: 8 });   // 3 messages never stored, though the record counts them
  const before = JSON.stringify(th().fullReread);
  r = await call(scrapeBody(MSGS.slice(3, 5)));     // an incremental scrape of what is visible: all stored
  check(r.body.success && r.body.needsFullScrape === true && r.body.newMessages === 0, "an incremental scrape of a marked conversation is answered needsFullScrape (the extension then does the full one)");
  check(JSON.stringify(th().fullReread) === before && !th().readComplete, "and proves nothing: the mark stays, no readComplete");
  r = await call(scrapeBody([], { diagnostics: { scrapeMode: "full" } }));
  check(r.status === 200 && !th().readComplete && th().fullReread, "a full scrape that read no messages proves nothing either");
  r = await call(scrapeBody(MSGS.slice(0, 5), { diagnostics: { scrapeMode: "full" }, session: { etsyLoggedIn: false } }));
  check(!th().readComplete && th().fullReread, "nor does one made while Etsy has the extension signed out");
  r = await call(scrapeBody(MSGS.slice(0, 8), { diagnostics: { scrapeMode: "full" } }));
  check(r.body.success && r.body.newMessages === 3 && r.body.needsFullScrape === false && countMsgs() === 8, "the complete read stores the 3 missing messages (8 of 8 now) and does not ask again");
  check(!th().fullReread && th().readComplete && th().readComplete.pageMessages === 8 && th().readComplete.inserted === 3 && th().readComplete.reqAtMs === FRESH.atMs && Math.abs(th().readComplete.atMs - Date.now()) < 5000, "the mark is gone and readComplete records the read (8 on the page, 3 stored, which request it answered)");

  console.log("An ordinary conversation");
  seed(MSGS.slice(0, 8), { updatedAt: T(NOW - HOUR), lastSyncedAt: T(NOW - HOUR) });
  const w0 = writes.length;
  r = await call(scrapeBody(MSGS.slice(4, 8), { diagnostics: { scrapeMode: "full" } }));
  check(r.body.success && r.body.newMessages === 0 && !th().readComplete && !th().fullReread, "a full scrape of a conversation nobody marked leaves no readComplete and no mark (nothing extra stored)");
  check(!writes.slice(w0).some(w => w.path === "EtsyMail_Threads/" + TID), "and a quiet scrape still does not write the conversation (the cost rule of FC13b holds)");

  console.log("The whole chain, with nobody pressing anything");
  // the record counts 11 (3 more than the 8 it should: drift), 5 stored of the 8 on Etsy's page
  seed(MSGS.slice(0, 5), { messageCount: 8, lastInboundAt: T(MSGS[4].timestampMs) });
  const night = Date.now() - 2 * MIN;
  const aged = Object.assign({}, th(), { updatedAt: T(night - 30 * HOUR) });
  store.set("EtsyMail_Threads/" + TID, aged);
  let run = await CON.maybeRun(db, { now: night, force: true });
  const gap = run.doc.gaps[TID];
  check(gap && gap.o === "4174999999" && gap.c === 8 && gap.s === 5 && th().fullReread && th().fullReread.atMs === night, "night: the gap is found (8 expected, 5 stored), the re-read queued, the conversation marked");
  const jobId = gap.jobId, job = store.get("EtsyMail_Jobs/" + jobId);
  check(job && job.jobType === "scrape" && job.status === "queued" && job.payload.fullRead === true, "the re-read is an ordinary scrape job for the extension");
  r = await call({ op: "threadExists", etsyConversationId: CONV });
  check(r.body.exists === false, "the extension, claiming that job, asks threadExists and is told to read it all");
  // the extension: claim, scrape in full, complete
  store.set("EtsyMail_Jobs/" + jobId, Object.assign({}, job, { status: "claimed", claimedBy: "w1" }));
  r = await call(scrapeBody(MSGS.slice(0, 8), { scrapedAt: night + 10 * MIN, diagnostics: { scrapeMode: "full" } }));
  check(r.body.newMessages === 3 && countMsgs() === 8 && th().readComplete, "the full scrape stores the 3 missing messages and records the complete read");
  store.set("EtsyMail_Jobs/" + jobId, Object.assign({}, store.get("EtsyMail_Jobs/" + jobId), { status: "succeeded", updatedAt: T(Date.now()) }));
  // the snapshot stamped real time; the 5-minute look runs a few minutes after
  const later = Date.now() + 4 * MIN;
  const t = await CON.tick(db, { now: later, prev: store.get("EtsyMail_Config/linkConsistency") });
  const d = store.get("EtsyMail_Config/linkConsistency");
  check(t.ran && !Object.keys(d.gaps).length && d.healed[0].how === "proven", "the 5-minute look closes the notice by itself (proven by the complete read)");
  const ev = { global: {}, watcher: { enabled: true }, gmail: { lastSyncCompletedAt: T(later - MIN) }, helper: { seenAtMs: later - 20e3 }, scrapeHealth: { consecutiveBad: 0 }, mirror: { enabled: true, lastSyncCompletedAt: T(later - 2 * MIN) }, bell: { reconcileAtMs: later - MIN }, monitor: { atMs: later - MIN }, queuedDrafts: [], scrapeJobs: [], readOk: {}, queue: null, consistency: d };
  const res = LH.evaluate(ev, later);
  check(res.level === "ok" && res.problem === "", "and the light is green");
  check(net.every(u => !/etsy\.com\/v3|openapi\.etsy/.test(u)), "no Etsy API call was made");
  console.warn = realWarn;
  realLog(fails.length ? `\nFAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\nlink-reread OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.warn = realWarn; console.error(e); realLog(warn.slice(-6).join("\n")); process.exit(1); });
