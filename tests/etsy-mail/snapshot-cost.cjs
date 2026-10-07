// FC13b (Firebase cost): what one Etsy scrape costs on the server (extension -> etsyMailSnapshot), before and after, and proof that
// what the extension gets back and what a real scrape stores did not change. Drives the REAL handler (and, for the buyer-sync
// call every scrape starts, the real etsyMailSync-background handler) over the cost meter's in-memory Firestore (tests/cost/meter.cjs)
// with a thread of 100 messages (12 KB thread document), once with the version before the change (taken from git) and once with the
// version in this tree. No network, no Etsy call, no AI call, no real data.
//
//   node tests/etsy-mail/snapshot-cost.cjs        (BEFORE_COMMIT=<sha> picks the "before" version; default cb263c79)
//
// The scrape rate is NOT measured here: the extension is job driven (one scrape per Etsy notice e-mail the Gmail watcher turns into a
// job, plus the reapers' one-shot retries and manual rescrapes), so the per-hour figures printed at the end are "scrapes per hour x
// per scrape", with the scrape rate as a stated assumption.
"use strict";
const assert = require("assert"), path = require("path"), fs = require("fs"), Module = require("module"), cp = require("child_process");
const meter = require("../cost/meter.cjs");
const ROOT = path.join(__dirname, "../..");
const FN_DIR = path.join(ROOT, "netlify/functions");
const BEFORE = process.env.BEFORE_COMMIT || "cb263c79";
process.env.ETSYMAIL_EXTENSION_SECRET = "s3cret-for-test";
process.env.URL = "https://test.invalid";
const H = { "x-etsymail-secret": "s3cret-for-test" };
const THREADS = "EtsyMail_Threads";

let oldSrc = null;
try { oldSrc = cp.execFileSync("git", ["show", BEFORE + ":netlify/functions/etsyMailSnapshot.js"], { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }); } catch (_) { /* no history: only the new version runs */ }
const newSrc = fs.readFileSync(path.join(FN_DIR, "etsyMailSnapshot.js"), "utf8");

/* ───────────── fake clock, fake network ───────────── */
const realNow = Date.now.bind(Date);
let clockMs = Date.UTC(2026, 9, 7, 15, 0, 0);
Date.now = () => clockMs;
const net = { sync: [], pipeline: [] };
let syncHandler = null;
const m = meter.create();
m.install();
{
  const prev = Module._load;
  Module._load = function (req, ...rest) {
    if (req === "node-fetch") return async (url, opts) => {
      if (/etsyMailSync-background/.test(url)) {
        const body = JSON.parse(opts.body); net.sync.push(body);
        // the real buyer-sync function runs (as Netlify would); its cost is booked under the same scenario
        const p = syncHandler({ httpMethod: "POST", headers: {}, body: opts.body });
        net.syncPromises.push(p); return { status: 202, ok: true };
      }
      throw new Error("the test allows no network call: " + url);
    };
    if (/(^|[\\/])_etsyApiMeter(\.js)?$/.test(req)) return { bump: () => ({ failNet() {}, fromHttp() {} }), bumpSimple: () => {}, wrapHandler: fn => fn, flushNow: async () => {} };
    return prev.call(this, req, ...rest);
  };
}
global.fetch = async (url, opts) => { net.pipeline.push(JSON.parse(opts.body)); return { status: 202, ok: true }; };   // etsyMailAutoPipeline-background trigger
net.syncPromises = [];
function load(file, src) {                 // a fresh copy of a function (its remembered state starts empty)
  const f = path.join(FN_DIR, file);
  const mod = new Module(f, module); mod.filename = f; mod.paths = Module._nodeModulePaths(FN_DIR);
  mod._compile(src, f);
  return mod.exports;
}
syncHandler = load("etsyMailSync-background.js", fs.readFileSync(path.join(FN_DIR, "etsyMailSync-background.js"), "utf8")).handler;

/* ───────────── fixture: one conversation of 100 messages ───────────── */
let seedN = 424242;
const rnd = () => { seedN = (seedN * 1103515245 + 12345) & 0x7fffffff; return seedN / 0x7fffffff; };
const WORDS = "hello thanks order mug engraved charm silver gold ring necklace shipping tracking arrived lovely photo proof approve change size chain clasp gift birthday wedding custom name date refund replace question".split(" ");
const text = n => { let s = ""; while (s.length < n) s += WORDS[Math.floor(rnd() * WORDS.length)] + " "; return s.slice(0, n).trim(); };
const CONV = "5001", TID = "etsy_conv_" + CONV, BUYER = "777001";
const T0 = Date.UTC(2026, 8, 20, 10, 0, 0);
const NMSG = 100;
function makeMessages(n) {
  seedN = 424242;
  const out = [];
  for (let i = 0; i < n; i++) {
    const staff = i % 2 === 1;
    out.push({ i, senderRole: staff ? "staff" : "customer", senderName: staff ? "Shop" : "Jane Buyer", text: "Msg " + i + " " + text(170), tsMs: T0 + i * 3600e3, contentHash: "h" + i });
  }
  return out;
}
const MSGS = makeMessages(NMSG + 1);                   // the last one (index 100) is the "new" message in the new-message scenario
const T = ms => meter.Timestamp.fromMillis(ms);
function storedMsg(x) {
  const dir = x.senderRole === "staff" ? "outbound" : "inbound";
  return {
    source: "etsy", direction: dir, senderName: x.senderName, senderRole: x.senderRole, timestamp: T(x.tsMs), text: x.text,
    normalizedText: x.text.toLowerCase(), contentHash: x.contentHash, messageType: "text", imageUrls: [], thumbnailUrls: [], listingCards: [],
    storageImagePaths: [], storageMirrorState: "none", attachmentUrls: [], etsyDomSelector: null, timestampSource: "page", createdAt: T(T0 + 5000)
  };
}
function threadDoc(over) {
  const body = text(5800);
  return Object.assign({
    threadId: TID, etsyConversationId: CONV, etsyConversationUrl: "https://www.etsy.com/your/conversations/" + CONV,
    customerName: "Jane Buyer", etsyUsername: "janebuyer", buyerUserId: BUYER, buyerPeopleUrl: "https://www.etsy.com/people/janebuyer",
    buyerAvatarUrl: "https://i.etsystatic.com/iusa/a.jpg", buyerIsRepeatBuyer: true, customerEmail: null, status: "pending_human_review",
    subject: "Question about my order", linkedOrderId: "3900000001", etsyOrderId: "3900000001", etsyHeadingBadge: "Help request",
    etsyHeadingTitle: "Help with order", etsyViewOrderUrl: "https://www.etsy.com/your/orders/3900000001",
    messageCount: NMSG, lastInboundAt: T(MSGS[NMSG - 2].tsMs), lastOutboundAt: T(MSGS[NMSG - 1].tsMs), unread: false,
    lastInboundPreview: MSGS[NMSG - 2].text.slice(0, 160), lastOutboundPreview: MSGS[NMSG - 1].text.slice(0, 160),
    searchableText: "jane buyer janebuyer question about my order 3900000001 " + body, searchableMessageText: body,
    createdAt: T(T0), updatedAt: T(clockMs - 3600e3), lastSyncedAt: T(clockMs - 3600e3), lastScrapedDomHash: "dom-old",     // last touched an hour ago
    tags: [], riskFlags: [], assignedTo: null, needsHumanReview: true, aiDraftStatus: "none", latestDraftId: null
  }, over || {});
}
function seedAll(opts) {
  opts = opts || {};
  m.db.docs.clear();
  const seed = {};
  if (!opts.noThread) {
    seed["EtsyMail_Threads/" + TID] = threadDoc(opts.thread);
    for (let i = 0; i < NMSG; i++) seed["EtsyMail_Threads/" + TID + "/messages/etsy_h" + i] = storedMsg(MSGS[i]);
  }
  // the buyer: customer record + 18 receipts of 3 KB (the whole Etsy receipt sits in `raw`)
  const next = opts.syncWindow === "inside" ? clockMs + 120e3 : clockMs - 3600e3;
  if (!opts.noCustomer) seed["EtsyMail_Customers/" + BUYER] = { buyerUserId: BUYER, displayName: "Jane Buyer", currency: "USD", orderCount: opts.customerOrders == null ? 18 : opts.customerOrders, totalSpent: 900, recentReceipts: Array.from({ length: 10 }, (_, i) => ({ receiptId: "r" + i, grandTotal: 50, status: "paid" })), nextBuyerSyncEligibleAtMs: next, syncSource: "mirror", updatedAt: T(clockMs - 3600e3), lastBuyerSyncAt: T(clockMs - 3600e3) };
  for (let i = 0; i < 18; i++) seed["EtsyMail_Receipts/r" + i] = { receipt_id: "r" + i, buyer_user_id: BUYER, buyer_name: "Jane Buyer", created_timestamp: 1700000000 + i * 1000, updated_timestamp: 1700000100 + i * 1000, grandtotal_amount: 50, grandtotal_currency: "USD", status: "paid", is_paid: true, is_shipped: true, raw: { blob: text(2800) } };
  if (opts.extraDup) { const d = storedMsg(MSGS[95]); d.timestamp = T(MSGS[95].tsMs + 30e3); d.createdAt = T(T0 + 200 * 3600e3); d.contentHash = "dup95"; seed["EtsyMail_Threads/" + TID + "/messages/etsy_dup95"] = d; seed["EtsyMail_Threads/" + TID].messageCount = NMSG + 1; }   // a second stored copy of message 95
  seed["EtsyMail_Config/scrapeHealth"] = { lastAtMs: clockMs - 600e3, lastOkAtMs: clockMs - 600e3, consecutiveBad: 0 };
  m.db.seed(seed);
}

/* ───────────── the scrape the extension posts ───────────── */
function scrapeBody(opts) {
  opts = opts || {};
  const from = opts.from == null ? NMSG - 12 : opts.from, to = opts.to == null ? NMSG : opts.to;
  const msgs = MSGS.slice(from, to).map(x => ({ senderName: x.senderName, senderRole: x.senderRole, timestampMs: x.tsMs, text: x.text, imageUrls: [], attachmentUrls: [], contentHash: x.contentHash, messageType: "text" }));
  return Object.assign({
    scrapedAt: clockMs, etsyConversationId: CONV, etsyConversationUrl: "https://www.etsy.com/your/conversations/" + CONV,
    threadDomHash: opts.hash || "dom-" + Math.floor(clockMs / 1000),     // a volatile page hash: different on every scrape
    participants: [{ name: "Jane Buyer", etsyUsername: "janebuyer", role: "customer", peopleUrl: "https://www.etsy.com/people/janebuyer", avatarUrl: "https://i.etsystatic.com/iusa/a.jpg", buyerUserId: BUYER, isRepeatBuyer: true }],
    subject: "Question about my order", messages: msgs,
    conversationHeading: { orderId: "3900000001", categoryBadge: "Help request", title: "Help with order", viewOrderUrl: "https://www.etsy.com/your/orders/3900000001" },
    session: { etsyLoggedIn: true, etsyUsername: "shop" }, diagnostics: { scrapeMode: opts.mode || "incremental" }
  }, opts.extra || {});
}

/* ───────────── running one scenario ───────────── */
const evt = body => ({ httpMethod: "POST", headers: H, body: JSON.stringify(body) });
const ts2ms = v => (v && typeof v.toMillis === "function") ? v.toMillis() : (v && typeof v.seconds === "number") ? v.seconds * 1000 + Math.floor((v.nanoseconds || 0) / 1e6) : v;   // a stored Timestamp, or its JSON form in a dump
function normDump() {                           // the stored data without ids that are random and without server-time noise
  const out = {};
  for (const [p, d] of Object.entries(m.db.dump())) {
    const auto = /\/auto\d{6}/.test("/" + p.split("/").pop()) || /^sync_/.test(p.split("/").pop());
    const key = auto ? p.split("/").slice(0, -1).join("/") + "/(auto)" : p;
    const RANDOM = new Set(["invocationId", "nextBuyerSyncEligibleAtMs", "lastBuyerSyncJitterSeconds", "elapsedMs"]);      // random per run (ids, the 3-5 minute jitter)
    const clean = JSON.parse(JSON.stringify(d, (k, v) => RANDOM.has(k) ? undefined : (v && typeof v.toMillis === "function") ? v.toMillis() : v));
    if (auto) { (out[key] = out[key] || []).push(clean); out[key].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))); } else out[key] = clean;
  }
  return out;
}
async function run(which, label, setup, calls) {
  const src = which === "before" ? oldSrc : newSrc;
  const fn = load("etsyMailSnapshot.js", src);
  clockMs = Date.UTC(2026, 9, 7, 15, 0, 0);
  seedN = 99; seedAll(setup);
  net.sync = []; net.pipeline = []; net.syncPromises = [];
  const seeded = normDump();
  const res = [];
  const a = m.snapshot();
  for (const c of calls) {
    if (c.at != null) clockMs = Date.UTC(2026, 9, 7, 15, 0, 0) + c.at * 1000;
    const r = await m.op(which + ":" + label, () => fn.handler(evt(c.body)));
    res.push({ status: r.statusCode, body: JSON.parse(r.body) });
    await Promise.all(net.syncPromises); net.syncPromises = [];
  }
  const cost = m.since(a);
  return { res, cost, seeded, dump: normDump(), sync: net.sync.slice(), pipeline: net.pipeline.slice() };
}
const fails = [];
const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
const kb = d => (d.bytes / 1024).toFixed(1) + " KB";
const row = (what, d) => console.log("       " + what.padEnd(46) + String(d.reads + d.aggs).padStart(6) + " reads " + kb(d).padStart(10) + String(d.writes).padStart(5) + " writes " + String(d.deletes).padStart(3) + " deletes");
const stripTimes = o => JSON.parse(JSON.stringify(o));

(async () => {
  console.log("\netsyMailSnapshot per scrape, thread of " + NMSG + " messages (before = " + (oldSrc ? BEFORE : "n/a") + ", after = this tree)\n");
  const scen = [
    { id: "quiet", title: "(1) quiet scrape: the 12 visible bubbles are all stored already", setup: { syncWindow: "outside" }, calls: [{ body: scrapeBody() }], quiet: true },
    { id: "quietInWindow", title: "(2) quiet scrape, buyer synced 2 minutes ago", setup: { syncWindow: "inside" }, calls: [{ body: scrapeBody() }], quiet: true },
    { id: "news", title: "(3) one new customer message", setup: { syncWindow: "outside" }, calls: [{ body: scrapeBody({ from: NMSG - 11, to: NMSG + 1 }) }] },
    { id: "newThread", title: "(4) first scrape of a conversation we do not have (20 messages)", setup: { noThread: true, noCustomer: true }, calls: [{ body: scrapeBody({ from: 0, to: 20, mode: "full" }) }] },
    { id: "burst", title: "(5) five quiet scrapes within 40 s (a drained backlog)", setup: { syncWindow: "outside" }, calls: [0, 10, 20, 30, 40].map(s => ({ at: s, body: scrapeBody() })), quiet: true },
    { id: "exists", title: "(6) threadExists question (asked before every scrape)", setup: {}, calls: [{ body: { op: "threadExists", etsyConversationId: CONV } }] }
  ];
  // the first scrape with news loads the Sorter hook (and its caches): do it once before anything is measured
  await run("after", "warm", { syncWindow: "outside" }, [{ body: scrapeBody({ from: NMSG - 11, to: NMSG + 1 }) }]);
  if (oldSrc) await run("before", "warm", { syncWindow: "outside" }, [{ body: scrapeBody({ from: NMSG - 11, to: NMSG + 1 }) }]);
  const results = {};
  for (const s of scen) {
    console.log(s.title);
    const after = await run("after", s.id, s.setup, s.calls);
    const before = oldSrc ? await run("before", s.id, s.setup, s.calls) : null;
    results[s.id] = { before, after };
    if (before) row("before", before.cost);
    row("after", after.cost);
    // what the extension is told must be identical, call for call
    if (before) check(JSON.stringify(before.res) === JSON.stringify(after.res), s.id + ": the answers to the extension are identical");
    check(after.res.every(r => r.status === 200 && (r.body.success || r.body.exists !== undefined)), s.id + ": every call answered 200");
  }

  // ── what a scrape with news stores must be exactly what it stored before ──
  console.log("\nstored data, scrapes that change something (must be identical before and after)");
  for (const id of ["news", "newThread"]) {
    const { before, after } = results[id];
    if (!before) continue;
    let same = JSON.stringify(before.dump) === JSON.stringify(after.dump);
    if (!same) {
      for (const k of new Set([...Object.keys(before.dump), ...Object.keys(after.dump)])) if (JSON.stringify(before.dump[k]) !== JSON.stringify(after.dump[k])) console.log("       differs: " + k + "\n         before " + JSON.stringify(before.dump[k]).slice(0, 300) + "\n         after  " + JSON.stringify(after.dump[k]).slice(0, 300));
    }
    check(same, id + ": every stored document is the same as before (thread, messages, audit, customer, scrapeHealth, buyer-sync diagnostics)");
    check(JSON.stringify(before.sync) === JSON.stringify(after.sync), id + ": the buyer-sync call is the same");
    check(JSON.stringify(before.pipeline) === JSON.stringify(after.pipeline), id + ": the auto-pipeline trigger is the same");
  }
  {
    const { after } = results.news, th = after.dump["EtsyMail_Threads/" + TID];
    check(th.messageCount === NMSG + 1 && !!after.dump["EtsyMail_Threads/" + TID + "/messages/etsy_h" + NMSG], "news: the new message is stored and counted");
    check(after.pipeline.length === 1, "news: the auto-pipeline is asked once for the new customer message");
  }

  // ── the quiet scrape: no lasting change, nothing lost ──
  console.log("\nquiet scrapes");
  {
    const { before, after } = results.quiet;
    const th = after.dump["EtsyMail_Threads/" + TID], th0 = after.seeded["EtsyMail_Threads/" + TID];
    const same = k => JSON.stringify(th[k]) === JSON.stringify(th0[k]);
    check(Object.keys(th0).filter(k => !["updatedAt", "lastSyncedAt", "lastScrapedDomHash"].includes(k)).every(same), "quiet: the thread document is unchanged (no field of the conversation differs)");
    check(ts2ms(th.updatedAt) === Date.UTC(2026, 9, 7, 14, 0, 0), "quiet: updatedAt is NOT bumped (the open Inbox tabs do not re-read the thread)");
    check(Object.keys(after.dump).filter(k => k.startsWith("EtsyMail_Threads/" + TID + "/messages/")).length === NMSG, "quiet: the stored messages are untouched (" + NMSG + ")");
    check(!after.dump["EtsyMail_Audit/(auto)"], "quiet: no audit row for a scrape that found nothing");
    if (before) check(!!before.dump["EtsyMail_Audit/(auto)"] && ts2ms(before.dump["EtsyMail_Threads/" + TID].updatedAt) === Date.UTC(2026, 9, 7, 15, 0, 0), "quiet: (before the change the same scrape bumped updatedAt and wrote an audit row)");
    check(after.pipeline.length === 0, "quiet: the auto-pipeline is not asked (no new customer message)");
    check(after.sync.length === 0, "quiet: buyer sync is skipped (the customer is known and has orders)");
    const sh = after.dump["EtsyMail_Config/scrapeHealth"];
    check(sh.consecutiveBad === 0 && sh.lastOkAtMs > 0, "quiet: scrapeHealth still reads good");
  }
  {
    const { after } = results.quietInWindow;
    check(after.sync.length === 0, "quiet inside the buyer debounce window: no buyer-sync invocation");
  }
  {
    const { before, after } = results.burst;
    const sh = after.dump["EtsyMail_Config/scrapeHealth"];
    check(sh.consecutiveBad === 0 && sh.lastOkAtMs >= Date.UTC(2026, 9, 7, 15, 0, 0), "burst: scrapeHealth is current after the burst");
    if (before) console.log("       scrapeHealth writes in the burst: before " + (before.cost.writes) + " writes in all, after " + after.cost.writes);
  }

  // ── safety: things that must still write / still run ──
  console.log("\nthings that must still happen");
  const edge = async (label, setup, body, test, quietEdge) => {
    const after = await run("after", label, setup, [{ body }]);
    const before = oldSrc ? await run("before", label, setup, [{ body }]) : null;
    const strip = d => { const o = Object.assign({}, d); if (quietEdge) { delete o["EtsyMail_Audit/(auto)"]; const t = Object.assign({}, o["EtsyMail_Threads/" + TID]); delete t.updatedAt; delete t.lastSyncedAt; delete t.lastScrapedDomHash; o["EtsyMail_Threads/" + TID] = t; if (after.sync.length === 0) { delete o["EtsyMail_Customers/" + BUYER]; delete o["EtsyMail_DiagnosticLog/(auto)"]; } } return JSON.stringify(o); };
    if (before) check(JSON.stringify(before.res) === JSON.stringify(after.res), label + ": answer identical to before");
    if (before) {
      const same = strip(before.dump) === strip(after.dump);
      if (!same) for (const k of new Set([...Object.keys(before.dump), ...Object.keys(after.dump)])) if (JSON.stringify(before.dump[k]) !== JSON.stringify(after.dump[k])) console.log("       differs: " + k + (k.indexOf("/messages/") < 0 ? "\n         before " + JSON.stringify(before.dump[k]).slice(0, 400) + "\n         after  " + JSON.stringify(after.dump[k]).slice(0, 400) : ""));
      check(same, label + ": stored data identical to before" + (quietEdge ? " (apart from the audit row and the three scrape stamps a quiet scrape no longer writes)" : ""));
    }
    test(after, before);
    return { before, after };
  };
  await edge("changed subject", {}, Object.assign(scrapeBody(), { subject: "A new subject" }), (a) => {
    check(a.dump["EtsyMail_Threads/" + TID].subject === "A new subject", "a changed subject is stored");
  });
  await edge("status advance", { thread: { status: "detected_from_gmail" } }, scrapeBody(), (a) => {
    check(a.dump["EtsyMail_Threads/" + TID].status === "etsy_scraped", "a detected_from_gmail thread advances to etsy_scraped on a quiet scrape");
  });
  await edge("unknown name", { thread: { customerName: "Unknown", status: "etsy_scraped" } }, scrapeBody(), (a) => {
    check(a.dump["EtsyMail_Threads/" + TID].customerName === "Jane Buyer", "an Unknown name is filled from the scrape");
  });
  await edge("etsy signed out", {}, Object.assign(scrapeBody(), { session: { etsyLoggedIn: false } }), (a) => {
    check(a.dump["EtsyMail_Threads/" + TID].status === "pending_human_review" && a.dump["EtsyMail_Audit/(auto)"].some(x => x.eventType === "held"), "a signed-out Etsy session still holds the thread and writes its audit row");
    check(a.dump["EtsyMail_Config/scrapeHealth"].consecutiveBad === 1, "a signed-out scrape still counts as a bad scrape");
  });
  await edge("no messages read", {}, Object.assign(scrapeBody(), { messages: [] }), (a) => {
    check(a.dump["EtsyMail_Config/scrapeHealth"].consecutiveBad === 1, "a scrape that read no messages is still recorded as bad at once");
  });
  await edge("older bubbles", {}, scrapeBody({ from: 40, to: 50 }), (a) => {
    check(a.res[0].body.newMessages === 0 && a.res[0].body.needsFullScrape === false, "a scrape of older stored bubbles finds nothing new");
  }, true);
  {
    const odd = scrapeBody(); odd.messages = odd.messages.map((x, i) => Object.assign({}, x, { text: "unrelated text " + i, contentHash: "g" + i }));
    await edge("nothing matches", {}, odd, (a) => {
      check(a.res[0].body.needsFullScrape === true, "a partial scrape that matches nothing still asks for a full scrape");
    });
  }
  await edge("stored copy", { extraDup: true }, scrapeBody(), (a) => {
    check(a.dump["EtsyMail_Threads/" + TID].messageCount === NMSG && !a.dump["EtsyMail_Threads/" + TID + "/messages/etsy_dup95"] && !!a.dump["EtsyMail_MessageArchive/" + TID + "__etsy_dup95"], "an extra stored copy of a message is still removed (and kept in the archive), the count corrected");
    check(!!a.dump["EtsyMail_Audit/(auto)"], "... and that scrape still writes its audit row");
  });
  await edge("buyer unknown to us", { noCustomer: true }, scrapeBody(), (a) => {
    check(a.sync.length === 1, "a quiet scrape for a buyer with no customer record still starts the buyer sync");
  }, true);
  await edge("customer with no orders", { customerOrders: 0 }, scrapeBody(), (a) => {
    check(a.sync.length === 1, "a quiet scrape for a customer record that shows no orders still starts the buyer sync");
  }, true);
  {
    // a quiet scrape of a thread not touched for 7 hours refreshes updatedAt (the reapers' 90-day rule), still without audit row
    const a = await run("after", "stale", { thread: { updatedAt: T(clockMs - 7 * 3600e3) } }, [{ body: scrapeBody() }]);
    check(ts2ms(a.dump["EtsyMail_Threads/" + TID].updatedAt) === clockMs, "a quiet scrape of a thread untouched for 7 hours refreshes updatedAt (once per 6 hours at most)");
    check(!a.dump["EtsyMail_Audit/(auto)"], "... and still writes no audit row");
    const b = await run("after", "fresh", { thread: { updatedAt: T(clockMs - 5 * 3600e3) } }, [{ body: scrapeBody() }]);
    check(ts2ms(b.dump["EtsyMail_Threads/" + TID].updatedAt) === clockMs - 5 * 3600e3, "a quiet scrape of a thread touched 5 hours ago leaves updatedAt alone");
  }

  /* ───────────── numbers per hour ───────────── */
  console.log("\nper scrape (this fixture) and per hour at an ASSUMED rate");
  const per = (x, n) => meter.perHour(x, n);
  for (const id of ["quiet", "quietInWindow", "news", "newThread", "exists"]) {
    const { before, after } = results[id];
    if (!before) continue;
    console.log("  " + id.padEnd(14) + " before " + String(before.cost.reads).padStart(4) + " reads " + (before.cost.bytes / 1024).toFixed(0).padStart(4) + " KB " + String(before.cost.writes).padStart(2) + " writes   after " + String(after.cost.reads).padStart(4) + " reads " + (after.cost.bytes / 1024).toFixed(0).padStart(4) + " KB " + String(after.cost.writes).padStart(2) + " writes");
  }
  console.log("\n" + (fails.length ? "FAILED: " + fails.length + "\n - " + fails.join("\n - ") : "all checks passed"));
  Date.now = realNow;
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
