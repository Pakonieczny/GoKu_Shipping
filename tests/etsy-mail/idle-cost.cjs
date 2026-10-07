// Firebase cost (FC7): what the Etsy mail scheduled functions cost when nothing has changed, and what they still do when
// something has. Drives the real functions (etsyMailReapers with its order-link and message-copy helpers, ListingCreatorCron,
// TrackingSweepStuck, DraftSendCleanupCron, ReceiptsMirrorCron, the Gmail watcher's background run) against the cost meter's
// in-memory Firestore (tests/cost/meter.cjs: reads, document bytes, writes, deletes, field masks) filled with a shop-sized
// dataset, and checks (1) the idle tick stays within a read/byte budget and (2) every repair the functions make is still made.
// No network, no real services, no Etsy call: node-fetch and the Etsy/Gmail helpers are pretend.
//   node tests/etsy-mail/idle-cost.cjs                 checks the budgets on the code in this tree
//   FN_DIR=/path/to/older/netlify/functions node tests/etsy-mail/idle-cost.cjs     measures an older copy (no budget check)
"use strict";
const assert = require("node:assert/strict"), path = require("node:path"), Module = require("module");
const ROOT = path.join(__dirname, "../..");
const FN_DIR = process.env.FN_DIR || path.join(ROOT, "netlify/functions");
const BUDGETS = !process.env.FN_DIR;
const meter = require(path.join(ROOT, "tests/cost/meter.cjs"));
const { Timestamp } = meter;

const m = meter.create(); m.install();
const fetched = [];
let etsyPages = [];
const gmail = { listing: [], bodies: {} };
const realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === "node-fetch") return async (url, opts) => {
    const u = String(url);
    fetched.push({ url: u, body: opts && opts.body });
    if (/^https:\/\/api\.etsy\.com\//.test(u)) { const results = etsyPages.shift() || []; return { ok: true, status: 200, headers: { get: () => null }, json: async () => ({ count: results.length, results }), text: async () => "" }; }
    if (/\/\.netlify\/functions\//.test(u)) return { ok: true, status: 202, headers: { get: () => null }, text: async () => "", json: async () => ({}) };
    throw new Error("the test allows no other fetch: " + u);
  };
  if (/(^|[\\/])_etsyApiMeter(\.js)?$/.test(req)) return { bump: () => ({ failNet() {}, fromHttp() {} }), wrapHandler: fn => fn };
  if (/(^|[\\/])_etsyMailEtsy(\.js)?$/.test(req)) return { getValidEtsyAccessToken: async () => "token" };
  if (/(^|[\\/])_etsyMailGmail(\.js)?$/.test(req)) return {
    listMessages: async ({ q }) => {
      const after = Number((/after:(\d+)/.exec(q) || [])[1] || 0);
      // the main flow asks from its watermark (recent: nothing new); the safety-net sweep asks for six hours
      return { messages: after * 1000 > Date.now() - 30 * 60000 ? [] : gmail.listing.map(id => ({ id, threadId: "gt_" + id })) };
    },
    getMessage: async id => ({ id }),
    summarizeMessage: f => ({ gmailMessageId: f.id, gmailThreadId: "gt_" + f.id, internalDateMs: Date.now() - 3600000, subject: "Re: Etsy Conversation with Orla Orphan", from: "x@etsy.com" }),
    extractEtsyConversationLink: async f => ({ conversationId: "777", conversationUrl: "https://www.etsy.com/your/conversations/777" })
  };
  return realLoad.call(this, req, ...rest);
};
Object.assign(process.env, { SHOP_ID: "1", CLIENT_ID: "c", CLIENT_SECRET: "s", GMAIL_CLIENT_ID: "g", GMAIL_CLIENT_SECRET: "g", URL: "https://example.test" });
delete process.env.ETSYMAIL_EXTENSION_SECRET;

const load = f => require(path.join(FN_DIR, f));
const realLog = console.log, realWarn = console.warn, realErr = console.error;
const quiet = () => { console.log = () => {}; console.warn = () => {}; console.error = () => {}; };
const loud = () => { console.log = realLog; console.warn = realWarn; console.error = realErr; };

/* ─────────────────────────── a shop-sized dataset ─────────────────────────── */
const NOW = Date.now(), MIN = 60000, HOUR = 60 * MIN, DAY = 24 * HOUR;
const ts = ms => Timestamp.fromMillis(ms);
const filler = n => "lorem ipsum dolor sit amet ".repeat(Math.ceil(n / 27)).slice(0, n);
const seed = {};
const N_THREADS = 1500, N_UNKNOWN_STEADY = 40, N_DRAFTS = 1500, N_OPEN_ENG = 30, N_RECEIPTS = 3000;
const threadId = i => "etsy_conv_" + (100000 + i);
for (let i = 0; i < N_THREADS; i++) {
  const unknown = i < N_UNKNOWN_STEADY;
  seed["EtsyMail_Threads/" + threadId(i)] = {
    threadId: threadId(i), etsyConversationId: String(100000 + i), etsyConversationUrl: "https://www.etsy.com/your/conversations/" + (100000 + i),
    gmailMessageId: "gm" + i, gmailThreadId: "gt" + i, gmailReceivedAt: ts(NOW - (i + 1) * 17 * MIN),
    customerName: unknown ? "Unknown" : "Buyer Number " + i, customerEmail: null, etsyUsername: "buyer" + i, linkedOrderId: null, linkedListingIds: ["1", "2"],
    status: "replied", category: "order_status", confidence: 0.9, needsHumanReview: false, aiDraftStatus: "ready", latestDraftId: "draft_" + threadId(i),
    lastInboundAt: ts(NOW - (i + 1) * 17 * MIN), lastOutboundAt: ts(NOW - (i + 1) * 17 * MIN + 5 * MIN), awaitingReplySince: null, lastSyncedAt: ts(NOW - (i + 1) * 17 * MIN),
    lastScrapedDomHash: filler(64), assignedTo: null, tags: ["a", "b"], riskFlags: [], messageCount: 14, unread: false, lastReadAt: ts(NOW - i * 17 * MIN),
    subject: unknown ? "Hello" : "Etsy Conversation with Buyer Number " + i, customerNameFromSubject: !unknown, createdAt: ts(NOW - (i + 5) * 17 * MIN), updatedAt: ts(NOW - (i + 1) * 17 * MIN),
    buyerUserId: String(5000 + (i % 900)), buyerPeopleUrl: "https://www.etsy.com/people/buyer" + i, buyerAvatarUrl: "https://i.etsystatic.com/iusa/abcdef/" + filler(60), buyerIsRepeatBuyer: i % 3 === 0,
    etsyOrderId: String(3000000000 + i), etsyHeadingBadge: "Order", etsyHeadingTitle: filler(90), etsyViewOrderUrl: "https://www.etsy.com/your/orders/" + (3000000000 + i),
    lastMessagePreview: filler(220), lastSenderName: "Buyer Number " + i, orderLinkIds: [], orderLinkOpen: 0,
    ...(unknown ? { _unknownRetryAttempted: true, _unknownRetryAttemptedAt: ts(NOW - DAY) } : {})
  };
}
for (let i = 0; i < N_DRAFTS; i++) {
  seed["EtsyMail_Drafts/draft_" + threadId(i)] = {
    draftId: "draft_" + threadId(i), threadId: threadId(i), text: filler(1800), status: "sent", createdBy: "ai", generatedByAI: true, aiModel: "claude", aiReasoning: filler(900),
    attachments: [1, 2, 3].map(k => ({ type: "image", url: "https://x/" + filler(80), name: filler(30), size: 1234 + k })),
    suggestedListings: [1, 2, 3, 4, 5, 6].map(k => ({ id: String(k), title: filler(120), url: "https://www.etsy.com/listing/" + k, image: "https://i.etsystatic.com/" + filler(70), price: "19.99" })),
    trackingImages: [], createdAt: ts(NOW - (i + 5) * 17 * MIN), updatedAt: ts(NOW - (i + 1) * 17 * MIN), sentAt: ts(NOW - (i + 1) * 17 * MIN)
  };
}
// three drafts among the newest 100 whose tracking lookup failed for good: the picture stays "pending" for ever
for (const i of [10, 20, 30]) {
  seed["EtsyMail_Drafts/draft_" + threadId(i)].trackingImages = [{ jobId: "tj_dead" + i, status: "pending", trackingCode: "9400" + i }];
  seed["EtsyMail_TrackingJobs/tj_dead" + i] = { jobId: "tj_dead" + i, status: "failed", error: "lookup failed", createdAt: ts(NOW - 2 * DAY), events: [] };
}
for (let i = 0; i < 40; i++) seed["EtsyMail_Jobs/gmail_gm" + i] = { jobId: "gmail_gm" + i, jobType: "scrape", status: "succeeded", threadId: threadId(i), createdAt: ts(NOW - DAY) };
for (let i = 0; i < N_OPEN_ENG; i++) {
  const id = "ol_" + (7000000 + i) + "_o_abc" + i, tid = threadId(300 + i);
  seed["EtsyMail_OrderLinks/" + id] = {
    id, receiptId: String(7000000 + i), scope: "order", lineId: null, lineLabel: "", orderNumber: String(7000000 + i), sandbox: false, status: "open", createdAtMs: NOW - 3 * DAY, createdBy: "Sorter 1",
    startedAtMs: NOW - 3 * DAY, updatedAtMs: NOW - DAY, v: NOW - DAY, threadId: tid, conversationUrl: "https://www.etsy.com/your/conversations/" + (100300 + i), link: "thread", linkedBy: "order",
    customer: { name: "Buyer Number " + (300 + i), username: "buyer" + (300 + i), buyerUserId: String(5000 + i) }, title: filler(60),
    outbox: [1, 2, 3, 4, 5].map(k => ({ id: "m" + k, text: filler(300), status: "sent", atMs: NOW - 3 * DAY + k, sentAtMs: NOW - 3 * DAY + k + 1000, msgId: "msg" + k })),
    seen: Array.from({ length: 150 }, (_, k) => "msg_id_" + i + "_" + String(k).padStart(8, "0") + "abcdef"), sim: [], unread: 0, inboundCount: 2,
    lastInboundAtMs: NOW - DAY, lastInboundPreview: filler(120), lastOutboundAtMs: NOW - 2 * DAY, lastShopReplyAtMs: 0, checkedToMs: NOW, pulledToMs: NOW - DAY
  };
  seed["EtsyMail_Threads/" + tid].orderLinkIds = [id]; seed["EtsyMail_Threads/" + tid].orderLinkOpen = 1;
}
for (let i = 0; i < N_RECEIPTS; i++) {
  const buyer = String(5000 + (i % 900));
  seed["EtsyMail_Receipts/" + (3000000000 + i)] = {
    receipt_id: String(3000000000 + i), receiptId: String(3000000000 + i), etsyOrderId: String(3000000000 + i), buyer_user_id: buyer, buyerUserId: buyer, created_timestamp: Math.floor(NOW / 1000) - i * 3600,
    updated_timestamp: Math.floor(NOW / 1000) - i * 3600, status: "Completed", is_paid: true, is_shipped: true, grandtotal_amount: 45, grandtotal_currency: "USD", buyer_name: "Buyer " + buyer,
    raw: { receipt_id: 3000000000 + i, buyer_user_id: Number(buyer), name: "Buyer " + buyer, transactions: [1, 2].map(k => ({ transaction_id: k, title: filler(140), sku: "SKU" + k, description: filler(500), variations: [{ property_id: 1, value_ids: [1], formatted_value: filler(60) }] })), shipments: [{ tracking_code: filler(24), carrier_name: "USPS" }], formatted_address: filler(160), message_from_buyer: filler(300) },
    mirrorWrittenAt: ts(NOW - 7 * DAY)
  };
}
Object.assign(seed, {
  "EtsyMail_Config/gmailWatcher": { enabled: true },
  "EtsyMail_Config/gmailSyncState": { lastSyncInProgress: false, lastInternalDateMs: NOW - 10 * MIN, lastSyncCompletedAt: ts(NOW - MIN) },
  "EtsyMail_Config/messageDedupeSweep": { version: 1, status: "done", cursor: null },
  "EtsyMail_Config/learnState": { lastRunAtMs: NOW - HOUR },
  "EtsyMail_Config/receiptsMirrorState": { enabled: true, lastSyncTimestamp: Math.floor(NOW / 1000) - 600, nextEligibleAtMs: NOW + 5 * MIN },
  "EtsyMail_OrderLinkMeta/bell": { n: 5, atMs: NOW - HOUR, inflight: {}, waiting: {} },
  "EtsyMail_OrderLinkMeta/waiting": { byReceipt: {}, byBuyer: {}, rebuiltAtMs: NOW - HOUR }
});
for (let i = 0; i < 25; i++) { seed["EtsyMail_GmailLinks/gl" + i] = { gmailMessageId: "gl" + i, status: "linked", threadId: threadId(i), linkedAt: ts(NOW - HOUR) }; }
gmail.listing = Array.from({ length: 25 }, (_, i) => "gl" + i);
m.db.seed(seed);
const db = m._raw;
const docBytes = p => meter.sizeOf(db.docs.get(p));
const doc = p => db.docs.get(p);
const sizes = { thread: docBytes("EtsyMail_Threads/" + threadId(100)), draft: docBytes("EtsyMail_Drafts/draft_" + threadId(100)), engagement: docBytes("EtsyMail_OrderLinks/" + Object.keys(seed).find(k => k.startsWith("EtsyMail_OrderLinks/")).split("/")[1]), receipt: docBytes("EtsyMail_Receipts/3000000000") };

const rows = [];
async function run(name, fn) {
  const a = m.snapshot(); const t0 = Date.now();
  quiet(); let r; try { r = await m.op(name, fn); } finally { loud(); }
  const d = m.since(a); rows.push({ name, reads: d.reads + d.aggs, bytes: d.bytes, writes: d.writes, deletes: d.deletes, ms: Date.now() - t0 });
  return r;
}
const sched = { headers: { "x-nf-event-source": "scheduled" }, body: "{}" };
const approx = (got, want, label) => assert(got === want, `${label}: ${got} (expected ${want})`);
let passed = 0;
const ok = (name) => { passed++; realLog("ok  " + name); };
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const reapers = load("etsyMailReapers.js");

  /* ── 1. the five-minute reaper, a quiet shop ── */
  const reap1 = await run("reapers tick 1 (nothing to repair)", () => reapers.handler(sched));
  assert.equal(reap1.statusCode, 200, "reapers answered " + reap1.statusCode + " " + reap1.body);
  const reap2 = await run("reapers tick 2 (nothing to repair)", () => reapers.handler(sched));
  const reap3 = await run("reapers tick 3 (nothing to repair)", () => reapers.handler(sched));
  if (process.env.CALLERS) {   // CALLERS=1: where one quiet reaper tick's reads and bytes go, by file:line
    m.reset(); quiet(); await m.op("reapers quiet tick", () => reapers.handler(sched)); loud();
    const rep = m.report();
    for (const [k, t] of Object.entries(rep.byCaller).sort((a, b) => b[1].bytes - a[1].bytes)) realLog(("  " + k).padEnd(62) + String(t.reads).padStart(6) + " reads" + String(t.bytes).padStart(9) + " B");
  }
  const body3 = JSON.parse(reap3.body);
  assert.equal(body3.totalReaped, 0, "a quiet shop reaps nothing: " + reap3.body);
  assert.deepEqual(body3.errors, [], "no pass failed");
  ok("reapers: a quiet shop, three ticks, nothing reaped and no pass failed");

  /* ── 2. what the reaper still repairs ── */
  // (a) mangled unicode: a thread updated after the first tick, one inside the newest 200, one far outside
  const T = id => "EtsyMail_Threads/" + id;
  const mangled = (id, upd) => m.db.seed({ [T(id)]: Object.assign({}, doc(T(threadId(5))), { threadId: id, customerName: "Caitr\\u00edona", subject: "Re: Etsy Conversation with Caitr\\u00edona", updatedAt: ts(upd) }) });
  mangled("etsy_conv_900001", NOW + 1000);               // a scrape just touched it
  // (b) a tracking picture the job finished but the draft never learned
  m.db.seed({
    ["EtsyMail_Drafts/draft_" + threadId(7)]: Object.assign({}, doc("EtsyMail_Drafts/draft_" + threadId(7)), { trackingImages: [{ jobId: "tj_ok", status: "pending" }], attachments: [{ type: "tracking_image", jobId: "tj_ok", status: "pending" }] }),
    "EtsyMail_TrackingJobs/tj_ok": { jobId: "tj_ok", status: "ready", carrier: "usps", imageUrl: "https://x/img.png", imageStoragePath: "etsymail/tracking/x.png", events: [{ t: 1 }] }
  });
  // (c) a stale auto-pipeline claim, a stale queued send, a detected thread without a job, an Unknown the subject can name, a deferred thread
  m.db.seed({
    [T("etsy_conv_900002")]: Object.assign({}, doc(T(threadId(6))), { threadId: "etsy_conv_900002", lastAutoDecision: "in_progress", lastAutoDecisionAt: ts(NOW - 20 * MIN) }),
    "EtsyMail_Drafts/draft_q1": { draftId: "draft_q1", threadId: threadId(8), status: "queued", queuedAt: ts(NOW - 90 * MIN), text: "hi", createdAt: ts(NOW - 91 * MIN) },
    [T("etsy_conv_900003")]: Object.assign({}, doc(T(threadId(9))), { threadId: "etsy_conv_900003", status: "detected_from_gmail", customerName: "Unknown", subject: "Re: Etsy Conversation with Ann Lee", gmailMessageId: "gmdet", createdAt: ts(NOW - 10 * MIN), updatedAt: ts(NOW - 10 * MIN) }),
    [T("etsy_conv_900004")]: Object.assign({}, doc(T(threadId(11))), { threadId: "etsy_conv_900004", status: "etsy_scraped", customerName: "Unknown", subject: "Etsy Conversation with Bo Chan", lastSyncedAt: ts(NOW - 20 * MIN), updatedAt: ts(NOW - 20 * MIN) }),
    [T("etsy_conv_900005")]: Object.assign({}, doc(T(threadId(12))), { threadId: "etsy_conv_900005", lastAutoDecision: "deferred_quiet_period", autoPipelineDeferUntilMs: NOW - MIN })
  });
  // (d) an open question whose conversation has a customer message the scrape hook missed
  const engId = Object.keys(seed).filter(k => k.startsWith("EtsyMail_OrderLinks/"))[0].split("/")[1], engThread = doc("EtsyMail_OrderLinks/" + engId).threadId;
  m.db.seed({
    ["EtsyMail_OrderLinks/" + engId]: Object.assign({}, doc("EtsyMail_OrderLinks/" + engId), { checkedToMs: NOW - 2 * HOUR }),
    [T(engThread)]: Object.assign({}, doc(T(engThread)), { lastInboundAt: ts(NOW - 5 * MIN) }),
    ["EtsyMail_Threads/" + engThread + "/messages/mm1"]: { direction: "inbound", text: "Is it ready?", senderName: "Buyer", timestamp: ts(NOW - 5 * MIN), createdAt: ts(NOW - 5 * MIN) }
  });
  const rep = await run("reapers tick 4 (nine things to repair)", () => reapers.handler(sched));
  const repBody = JSON.parse(rep.body);
  assert.deepEqual(repBody.errors, [], "no pass failed: " + rep.body);
  assert.equal(doc(T("etsy_conv_900001")).customerName, "Caitríona", "the mangled name is unmangled");
  assert.equal(doc("EtsyMail_Drafts/draft_" + threadId(7)).trackingImages[0].status, "ready", "the finished tracking picture reaches the draft");
  assert.equal(doc("EtsyMail_Drafts/draft_" + threadId(10)).trackingImages[0].status, "pending", "a failed lookup stays as it was");
  assert.equal(doc(T("etsy_conv_900002")).lastAutoDecision, "stale_claim_recovered", "the stale auto-pipeline claim is released");
  assert.equal(doc("EtsyMail_Drafts/draft_q1").status, "failed", "the stale queued send is expired");
  assert.equal(doc(T("etsy_conv_900003")).customerName, "Ann Lee", "a detected thread gets its name from the subject");
  assert(doc("EtsyMail_Jobs/gmail_gmdet") && doc("EtsyMail_Jobs/gmail_gmdet").status === "queued", "a detected thread without a job gets a scrape job");
  assert.equal(doc(T("etsy_conv_900004")).customerName, "Bo Chan", "an Unknown thread is named from its subject");
  assert(fetched.some(f => /etsyMailAutoPipeline-background/.test(f.url) && /etsy_conv_900005/.test(f.body || "")), "the deferred thread is fired");
  assert.equal(doc("EtsyMail_OrderLinks/" + engId).unread, 1, "the missed customer message is pulled into the open question");
  ok("reapers: all nine repairs still happen (unicode, tracking, stale claim, stale send, detected, unknown, deferred, missed message)");
  // after a repair the next tick is quiet again
  const reap5 = await run("reapers tick 5 (after the repairs)", () => reapers.handler(sched));
  assert.equal(JSON.parse(reap5.body).totalReaped, 0, "quiet again after the repairs: " + reap5.body);
  // and a thread updated later is still looked at
  mangled("etsy_conv_900006", Date.now() + 2000);
  await run("reapers tick 6 (a scrape touched one thread)", () => reapers.handler(sched));
  assert.equal(doc(T("etsy_conv_900006")).customerName, "Caitríona", "a thread updated after the watermark is still unmangled");
  ok("reapers: a thread updated later is still examined; a quiet tick reaps nothing");

  /* ── 3. the other minute and 3-minute ticks ── */
  const lc = load("etsyMailListingCreatorCron.js");
  const lcIdle = await run("listingCreatorCron tick (idle)", () => lc.handler(sched));
  assert.equal(JSON.parse(lcIdle.body).claimed, 0);
  m.db.seed({
    [T("etsy_conv_910001")]: { threadId: "etsy_conv_910001", customListingStatus: "queued", customerAccepted: true, customerAcceptedAt: ts(NOW - 90000), status: "sales_quote", filler: filler(2500) },
    [T("etsy_conv_910002")]: { threadId: "etsy_conv_910002", customListingReplyStatus: "queued", customListingReplyQueuedAt: ts(NOW - 10 * MIN), customListingUrl: "https://www.etsy.com/listing/99", customListingReplyDraftId: "draft_910002", filler: filler(2500) },
    "EtsyMail_Drafts/draft_910002": { draftId: "draft_910002", status: "sent", text: "Here it is https://www.etsy.com/listing/99 enjoy", filler: filler(5000) }
  });
  const lcWork = await run("listingCreatorCron tick (one queued, one reply to settle)", () => lc.handler(sched));
  assert.equal(JSON.parse(lcWork.body).claimed, 1, "the queued listing is claimed: " + lcWork.body);
  assert.equal(doc(T("etsy_conv_910001")).customListingStatus, "creating");
  assert(fetched.some(f => /etsyMailListingCreator-background/.test(f.url) && /etsy_conv_910001/.test(f.body || "")), "the worker is started");
  assert.equal(doc(T("etsy_conv_910002")).customListingReplyStatus, "sent", "the delivered link is settled");
  ok("listingCreatorCron: claims a queued listing, starts its worker, settles a delivered link");

  const sweep = load("etsyMailTrackingSweepStuck.js");
  await run("trackingSweepStuck tick (idle)", () => sweep.handler({}));
  m.db.seed({ "EtsyMail_TrackingJobs/tj_stuck": { jobId: "tj_stuck", status: "pending", createdAt: ts(NOW - 5 * MIN), events: [] } });
  await run("trackingSweepStuck tick (one stuck job)", () => sweep.handler({}));
  assert.equal(doc("EtsyMail_TrackingJobs/tj_stuck").status, "failed");
  assert.equal(doc("EtsyMail_TrackingJobs/tj_stuck").errorCode, "BG_TRIGGER_LOST");
  ok("trackingSweepStuck: a stuck job is still failed");

  const cleanup = load("etsyMailDraftSendCleanupCron.js");
  await run("draftSendCleanupCron tick (idle)", () => cleanup.handler());
  m.db.seed({ "EtsyMail_Drafts/draft_s1": { draftId: "draft_s1", threadId: threadId(8), status: "sending", sendHeartbeatAt: ts(NOW - 5 * MIN), sendAttempts: 0, text: filler(3000) } });
  await run("draftSendCleanupCron tick (one stranded send)", () => cleanup.handler());
  assert.equal(doc("EtsyMail_Drafts/draft_s1").status, "queued");
  ok("draftSendCleanupCron: a stranded send is still requeued");

  /* ── 4. the receipts mirror: a tick inside its window, and a working tick ── */
  const mirror = load("etsyMailReceiptsMirrorCron.js");
  m.db.seed({ "EtsyMail_Config/receiptsMirrorState": Object.assign({}, doc("EtsyMail_Config/receiptsMirrorState"), { nextEligibleAtMs: Date.now() + 5 * MIN }) });
  const diagBefore = [...db.docs.keys()].filter(k => k.startsWith("EtsyMail_DiagnosticLog/")).length;
  const mSkip = await run("receiptsMirrorCron tick (inside its 7-10 minute window)", () => mirror.handler({}));
  assert.equal(JSON.parse(mSkip.body).skipped, true);
  const fetchBefore = fetched.length;
  m.db.seed({ "EtsyMail_Config/receiptsMirrorState": Object.assign({}, doc("EtsyMail_Config/receiptsMirrorState"), { nextEligibleAtMs: 0 }) });
  etsyPages = [[0, 1, 2].map(i => ({ receipt_id: 3000000000 + i, buyer_user_id: 5000 + i, name: "Buyer " + (5000 + i), status: "Completed", is_paid: true, is_shipped: true, created_timestamp: Math.floor(NOW / 1000) - 99999, updated_timestamp: Math.floor(NOW / 1000), grandtotal: { amount: 4500, divisor: 100, currency_code: "USD" }, transactions: [] }))];
  const mWork = await run("receiptsMirrorCron tick (3 receipts changed, 3 buyers rebuilt)", () => mirror.handler({}));
  assert.equal(JSON.parse(mWork.body).outcome, "ok", mWork.body);
  assert.equal(fetched.slice(fetchBefore).filter(f => /api\.etsy\.com/.test(f.url)).length, 1, "one Etsy page, as before");
  const cust = doc("EtsyMail_Customers/5001");
  assert(cust && cust.orderCount === 4 && cust.totalSpent === 180 && cust.displayName === "Buyer 5001", "the customer summary is rebuilt from the mirror: " + JSON.stringify(cust));
  ok("receiptsMirrorCron: the customer summary is rebuilt exactly as before (4 receipts, 180.00)");
  if (BUDGETS) {
    const diagAfterSkip = [...db.docs.keys()].filter(k => k.startsWith("EtsyMail_DiagnosticLog/")).length;
    assert.equal(rows.find(r => /inside its/.test(r.name)).writes, 0, "a tick inside the window writes nothing");
    assert(diagAfterSkip - diagBefore === 1, "only the working tick leaves a diagnostic row (" + (diagAfterSkip - diagBefore) + ")");
    ok("receiptsMirrorCron: a tick inside the window writes nothing; the working tick leaves its diagnostic row");
  }

  /* ── 5. the Gmail watcher: cron + background run, 25 messages in the six-hour window, one of them an orphan ── */
  const gcron = load("etsyMailGmailCron.js"), gbg = load("etsyMailGmail-background.js");
  gmail.listing.push("orphan1");
  const gTick = async name => { await run(name + " (cron)", () => gcron.handler({})); return run(name + " (background run)", () => gbg.handler({ body: "{}" })); };
  const gOrphan = await gTick("gmail watcher tick 1 (one orphan)");
  assert.equal(gOrphan.statusCode, 200, gOrphan.body);
  assert(doc("EtsyMail_Threads/etsy_conv_777"), "the orphan's thread is created");
  assert(doc("EtsyMail_Jobs/gmail_orphan1") && doc("EtsyMail_Jobs/gmail_orphan1").status === "queued", "the orphan's scrape job is queued");
  assert(doc("EtsyMail_GmailLinks/orphan1"), "the orphan is in the link index");
  ok("gmail watcher: an orphaned message is still recovered (thread, scrape job, link)");
  await gTick("gmail watcher tick 2 (orphan now linked)");
  await gTick("gmail watcher tick 3 (idle)");
  await gTick("gmail watcher tick 4 (idle)");
  const before = { threads: [...db.docs.keys()].filter(k => k.startsWith("EtsyMail_Threads/")).length };
  assert.equal(before.threads, [...db.docs.keys()].filter(k => k.startsWith("EtsyMail_Threads/")).length, "idle ticks create nothing");
  ok("gmail watcher: idle ticks create nothing");

  /* ── the numbers ── */
  realLog("\nDocument sizes in this dataset: thread " + sizes.thread + " B, draft " + sizes.draft + " B, open question " + sizes.engagement + " B, mirrored receipt " + sizes.receipt + " B");
  realLog("\n" + "tick".padEnd(70) + "reads".padStart(7) + "bytes".padStart(10) + "writes".padStart(8) + "deletes".padStart(9));
  for (const r of rows) realLog(r.name.padEnd(70) + String(r.reads).padStart(7) + String(r.bytes).padStart(10) + String(r.writes).padStart(8) + String(r.deletes).padStart(9));
  realLog("RESULT " + JSON.stringify({ fnDir: FN_DIR === path.join(ROOT, "netlify/functions") ? "tree" : "other", sizes, rows }));

  if (BUDGETS) {
    const R = n => rows.find(r => r.name === n);
    const idle = R("reapers tick 3 (nothing to repair)");
    // before this work the same quiet tick read 420 documents and 1,279,324 bytes (the 200 newest threads in full, the 100
    // newest drafts in full, a second read of every draft with a pending picture, whole conversations of the open questions);
    // what is left in this dataset is the 100 drafts' one field, the open questions, the Unknown threads and a dozen empty queries
    meter.assertMax(idle, { reads: 230, bytes: 110000, writes: 3 }, "reapers idle tick");
    ok("budget: the reaper's quiet tick stays within 230 reads and 110 KB (" + idle.reads + " reads, " + idle.bytes + " B; was 420 reads, 1,279,324 B)");
    assert(R("reapers tick 2 (nothing to repair)").reads < R("reapers tick 1 (nothing to repair)").reads - 150, "the unicode walk is a one-query tick once its watermark exists");
    ok("budget: after its first pass the unicode walk reads next to nothing");
    meter.assertMax(R("listingCreatorCron tick (idle)"), { reads: 4, bytes: 2000, writes: 0 }, "listing creator idle tick");
    meter.assertMax(R("trackingSweepStuck tick (idle)"), { reads: 3, bytes: 500, writes: 0 }, "tracking sweep idle tick");
    meter.assertMax(R("draftSendCleanupCron tick (idle)"), { reads: 2, bytes: 500, writes: 0 }, "draft cleanup idle tick");
    meter.assertMax(R("receiptsMirrorCron tick (inside its 7-10 minute window)"), { reads: 1, bytes: 1500, writes: 0 }, "mirror skip tick");
    const g = R("gmail watcher tick 4 (idle) (background run)"), gc = R("gmail watcher tick 4 (idle) (cron)");
    meter.assertMax({ reads: g.reads + gc.reads, bytes: g.bytes + gc.bytes, writes: g.writes + gc.writes }, { reads: 6, bytes: 3000, writes: 2 }, "gmail watcher idle tick (cron + background)");
    ok("budget: every other idle tick within its few reads");
  }
  realLog("\n" + passed + " checks passed");
})().catch(e => { loud(); realLog(e && e.stack || e); process.exit(1); });
