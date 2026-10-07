// FC13 (Firebase cost): what etsyMailThreads reads for the two calls that used to read every thread, and proof that the answers
// did not change. Drives the REAL handler over the cost meter's in-memory Firestore (tests/cost/meter.cjs) filled with a
// shop-sized set of thread documents (about 12 KB each: two 6 KB search texts), once with the version before the change (taken
// from git, when git can give it) and once with the version in this tree. No network, no real data.
//
//   (1) plain ?counts=1 (the legacy Inbox layout, every 300 s per tab). Before: the composite index (status, <timestamp>) is
//       missing in production, so the call fell to a scan of EVERY thread. After: the same numbers from count() queries and from
//       reading only the threads that carry the three timestamps. The result must equal the full scan's result, for ordinary data
//       and for odd data (a status outside the list, "" / null status, a thread with no status at all, a number as status).
//   (2) ?search=1&q=...&limit=500 (both Inbox layouts, 200 ms after the last key). Before: 500 whole threads per query. After: a
//       field-masked scan plus whole reads of the matches only; a longer query typed within 15 s is answered from the shorter
//       one. The matches must be the same documents in the same order with the same fields, backfill and all.
//
//   node tests/etsy-mail/threads-cost.cjs            (BEFORE_COMMIT=<sha> picks the "before" version; default 8248616e)
"use strict";
const assert = require("assert"), path = require("path"), fs = require("fs"), Module = require("module"), cp = require("child_process");
const meter = require("../cost/meter.cjs");
const ROOT = path.join(__dirname, "../..");
const FN_DIR = path.join(ROOT, "netlify/functions");
const BEFORE = process.env.BEFORE_COMMIT || "8248616e";
process.env.ETSYMAIL_EXTENSION_SECRET = "s3cret-for-test";
const H = { "x-etsymail-secret": "s3cret-for-test" };

let oldSrc = null;
try { oldSrc = cp.execFileSync("git", ["show", BEFORE + ":netlify/functions/etsyMailThreads.js"], { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }); } catch (_) { /* no history: only the new version is run */ }

const m = meter.create();
m.install();
{ // etsyMailSync-background needs node-fetch and the Etsy call meter; here they are pretend (no network, no Etsy call is allowed)
  const prev = Module._load;
  Module._load = function (req, ...rest) {
    if (req === "node-fetch") return async url => { throw new Error("the test allows no network call: " + url); };
    if (/(^|[\\/])_etsyApiMeter(\.js)?$/.test(req)) return { bump: () => ({ failNet() {}, fromHttp() {} }), bumpSimple: () => {}, wrapHandler: fn => fn, flushNow: async () => {} };
    return prev.call(this, req, ...rest);
  };
}
function load(src) {                      // a fresh copy of the function (its remembered state starts empty)
  const file = path.join(FN_DIR, "etsyMailThreads.js");
  const mod = new Module(file, module); mod.filename = file; mod.paths = Module._nodeModulePaths(FN_DIR);
  mod._compile(src, file);
  return mod.exports;
}
const newSrc = fs.readFileSync(path.join(FN_DIR, "etsyMailThreads.js"), "utf8");

/* ───────────── fixture: a shop's threads ───────────── */
let seedN = 12345;
const rnd = () => { seedN = (seedN * 1103515245 + 12345) & 0x7fffffff; return seedN / 0x7fffffff; };
const WORDS = "hello thanks order mug engraved charm silver gold ring necklace shipping tracking arrived lovely photo proof approve change size chain clasp gift birthday wedding custom name date refund replace question".split(" ");
const text = n => { let s = ""; while (s.length < n) s += WORDS[Math.floor(rnd() * WORDS.length)] + " "; return s.slice(0, n).trim(); };
const STATUSES = ["archived", "archived", "archived", "archived", "sent", "sent", "draft_ready", "pending_human_review", "auto_replied", "sales_active", "sales_completed", "etsy_scraped", "ready_for_ai"];
const N = 1500;
function seedThreads(extra, noFalsyStamps) {
  m.db.docs.clear();
  seedN = 12345;                                  // the same fixture every time, so "before" and "after" read the same documents
  const seed = {};
  for (let i = 0; i < N; i++) {
    const status = STATUSES[i % STATUSES.length];
    const body = text(5800);
    const t = { customerName: "Buyer " + i, etsyUsername: "buyer" + i, subject: "Order question " + i, linkedOrderId: String(3000000000 + i), status, updatedAt: 2000000 - i, searchableText: ("buyer " + i + " order question " + i + " " + body), searchableMessageText: body, lastInboundAt: 1000 + i };
    if (i % 5 === 0) t.salesCompletedAt = 1500000 + i;
    // (production never stores these three as null or 0: they are set, or deleted; the odd values are here for the scan's truthiness test)
    if (!noFalsyStamps && i % 5 === 1 && i % 13 === 0) t.salesCompletedAt = null;
    if (!noFalsyStamps && i % 5 === 2 && i % 17 === 0) t.salesCompletedAt = 0;
    if (i % 70 === 0) t.refundFlaggedAt = 1600000 + i;
    if (i % 90 === 0) t.orderLinkOpenAt = 1700000 + i;
    if (!noFalsyStamps && i % 110 === 0 && i % 90 !== 0) t.orderLinkOpenAt = 0;
    seed["EtsyMail_Threads/t" + i] = t;
  }
  // planted words: in the newest 500 and beyond them
  for (const [i, w] of [[3, "zebraprint"], [17, "zebraprint"], [250, "zebraprint"], [480, "zebraprint"], [900, "zebraprint"], [1400, "zebraprint"], [60, "quartzite"], [300, "quartzite"]]) {
    const t = seed["EtsyMail_Threads/t" + i]; t.searchableText += " " + w; t.searchableMessageText += " " + w;
  }
  Object.assign(seed, extra || {});
  m.db.seed(seed);
}
const oddThreads = n => {
  const o = {};
  for (let i = 0; i < 40; i++) o["EtsyMail_Threads/u" + i] = { customerName: "Odd " + i, status: "unknown", updatedAt: 10 + i, searchableText: "odd unknown " + i };
  for (let i = 0; i < 3; i++) o["EtsyMail_Threads/w" + i] = { customerName: "Weird " + i, status: "needs_attention", updatedAt: 5 + i, salesCompletedAt: 99 + i, searchableText: "weird " + i };
  o["EtsyMail_Threads/e0"] = { customerName: "Empty", status: "", updatedAt: 3, searchableText: "empty" };
  o["EtsyMail_Threads/e1"] = { customerName: "Null", status: null, updatedAt: 2, salesCompletedAt: 7, searchableText: "null status" };
  if (n >= 2) o["EtsyMail_Threads/nf"] = { customerName: "No status field", updatedAt: 1, searchableText: "no status" };
  if (n >= 3) o["EtsyMail_Threads/num"] = { customerName: "Number status", status: 5, updatedAt: 1, searchableText: "number status" };
  return o;
};

const evt = q => ({ httpMethod: "GET", headers: H, queryStringParameters: q });
async function call(fn, label, q) {
  const a = m.snapshot();
  const r = await m.op(label, () => fn.handler(evt(q)));
  assert.strictEqual(r.statusCode, 200, label + ": " + String(r.body).slice(0, 200));
  return { body: JSON.parse(r.body), cost: m.since(a) };
}
const fails = [];
const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
const kb = d => (d.bytes / 1024).toFixed(0) + " KB";
const row = (what, d) => console.log("       " + what.padEnd(58) + String(d.reads + d.aggs).padStart(7) + " reads " + kb(d).padStart(9));
const prodGuard = meter.missingCompositeIndex([]);       // production today: no composite index on the threads
const withIndexes = meter.missingCompositeIndex(["EtsyMail_Threads|status,salesCompletedAt", "EtsyMail_Threads|status,refundFlaggedAt", "EtsyMail_Threads|status,orderLinkOpenAt"]);

(async () => {
  /* ───────────── (1) counts ───────────── */
  console.log("\n(1) plain ?counts=1 over " + N + " threads (production: no composite index)");
  m.queryGuard = prodGuard;
  seedThreads();
  const oracleOf = () => {            // what the full scan answers, computed straight from the stored threads
    const out = {}; let cs = 0, rf = 0, ol = 0;
    for (const [p, d] of Object.entries(m.db.dump())) {
      if (!p.startsWith("EtsyMail_Threads/")) continue;
      const s = d.status || "unknown"; out[s] = (out[s] || 0) + 1;
      if (s === "archived") continue;
      if (d.salesCompletedAt) cs++; if (d.refundFlaggedAt) rf++; if (d.orderLinkOpenAt) ol++;
    }
    out._completedSales = cs; out._refundFlagged = rf; out._orderLinkOpen = ol; return out;
  };
  const scenarios = [
    ["ordinary data", () => seedThreads()],
    ["a status outside the list, \"\" and null statuses", () => seedThreads(oddThreads(1))],
    ["... and a thread with no status field", () => seedThreads(oddThreads(2))],
    ["... and a number as a status", () => seedThreads(oddThreads(3))]
  ];
  for (const [name, seedIt] of scenarios) {
    seedIt();
    const want = oracleOf();
    let b0 = Infinity;
    if (oldSrc) {
      const b = await call(load(oldSrc), "counts.before", { counts: "1" });
      assert.deepStrictEqual(b.body.counts, want, "the version before the change must equal the oracle (" + name + ")");
      row(name + ": before", b.cost);
      b0 = b.cost.reads + b.cost.aggs;
    }
    const fn = load(newSrc);
    const a = await call(fn, "counts.after", { counts: "1" });
    check(JSON.stringify(Object.entries(a.body.counts).sort()) === JSON.stringify(Object.entries(want).sort()), name + ": the numbers are the full scan's numbers");
    row(name + ": after, first call", a.cost);
    const a2 = await call(fn, "counts.after2", { counts: "1" });
    check(JSON.stringify(a2.body.counts) === JSON.stringify(a.body.counts), name + ": the second call gives the same numbers");
    row(name + ": after, next call", a2.cost);
    if (name === "ordinary data") {
      // 33 count units + the threads that carry salesCompletedAt / refundFlaggedAt / orderLinkOpenAt (here 20 percent of the shop carry the first)
      check(a2.cost.reads + a2.cost.aggs < 500, "ordinary data: the call reads a quarter of the shop at most, not all " + N + " (" + (a2.cost.reads + a2.cost.aggs) + ")");
      check(a2.cost.bytes < 40 * 1024, "ordinary data: under 40 KB of documents (" + kb(a2.cost) + ")");
    }
    if (name.startsWith("a status outside")) check(a2.cost.reads + a2.cost.aggs < a.cost.reads + a.cost.aggs, "odd statuses: the second call is cheaper (the names are remembered)");
    if (name.startsWith("... and a thread") || name.startsWith("... and a number")) {
      check(a2.cost.reads + a2.cost.aggs >= N, name + ": the numbers could not be accounted cheaply, so the old full scan ran (" + (a2.cost.reads + a2.cost.aggs) + " reads)");
      check(a2.cost.reads + a2.cost.aggs <= b0 + 5, name + ": and the next call goes straight to the scan, no dearer than before the change (" + (a2.cost.reads + a2.cost.aggs) + " vs " + b0 + ")");
    }
  }
  // with the composite indexes in the console the folder counts are count() queries
  m.queryGuard = withIndexes;
  seedThreads(null, true);
  const fnI = load(newSrc);
  await call(fnI, "counts.indexed.first", { counts: "1" });
  const ai = await call(fnI, "counts.indexed", { counts: "1" });
  row("composite indexes present: after", ai.cost);
  check(JSON.stringify(Object.entries(ai.body.counts).sort()) === JSON.stringify(Object.entries(oracleOf()).sort()), "with the composite indexes: the same numbers");
  check(ai.cost.reads === 0 && ai.cost.bytes === 0, "with the composite indexes: no document is read at all (count() only: " + ai.cost.aggs + " read units)");
  m.queryGuard = prodGuard;

  /* ───────────── (2) search ───────────── */
  console.log("\n(2) ?search=1&limit=500 over " + N + " threads (the newest 500 are scanned)");
  const sq = (q, extra) => Object.assign({ search: "1", q, limit: "500" }, extra || {});
  const docsOf = b => b.docs.map(d => d.id);
  const plain = d => { const o = JSON.parse(JSON.stringify(d)); return o; };
  for (const variant of [["no thread lacks search text", 0], ["8 threads lack their search text (backfilled on the way)", 8]]) {
    console.log("   -- " + variant[0]);
    const mk = () => {
      seedThreads();
      for (let i = 0; i < variant[1]; i++) {
        const id = "t" + (10 + i * 30);
        const t = m.db.docs.get("EtsyMail_Threads/" + id); delete t.searchableText; delete t.searchableMessageText;
        for (let k = 0; k < 3; k++) m.db.docs.set("EtsyMail_Threads/" + id + "/messages/m" + k, { text: (k === 1 && i % 2 === 0 ? "the zebraprint arrived " : "hi ") + k, timestamp: k });
      }
    };
    for (const q of ["zebraprint", "quartzite", "zeb", "buyer 12", "mug engraved charm", "nomatchatall"]) {
      let before = null;
      if (oldSrc) { mk(); before = await call(load(oldSrc), "search.before", sq(q)); row("\"" + q + "\": before (" + before.body.count + " found)", before.cost); }
      mk();
      const after = await call(load(newSrc), "search.after", sq(q));
      row("\"" + q + "\": after (" + after.body.count + " found)", after.cost);
      if (before) {
        check(JSON.stringify(docsOf(after.body)) === JSON.stringify(docsOf(before.body)), "\"" + q + "\": the same threads in the same order");
        check(JSON.stringify(after.body.docs.map(plain)) === JSON.stringify(before.body.docs.map(plain)), "\"" + q + "\": every returned thread has the same fields");
        check(after.body.scanned === before.body.scanned && after.body.maxedOut === before.body.maxedOut && after.body.backfilled === before.body.backfilled, "\"" + q + "\": scanned " + after.body.scanned + ", maxedOut " + after.body.maxedOut + ", backfilled " + after.body.backfilled + " as before");
        if (q.length >= 3 && variant[1] === 0 && before.body.count < 20) check(after.cost.bytes < before.cost.bytes * 0.55, "\"" + q + "\": about half the bytes or less (" + kb(after.cost) + " vs " + kb(before.cost) + ")");
        if (q.length >= 3 && variant[1] === 0 && before.body.count >= 20) check(after.cost.bytes < before.cost.bytes * 0.8, "\"" + q + "\": a query with many matches still reads fewer bytes (" + kb(after.cost) + " vs " + kb(before.cost) + ")");
      }
    }
  }
  // typing a word: the 3rd, 4th ... letters are answered from the first scan
  console.log("   -- typing \"zebraprint\" letter by letter (200 ms between answers)");
  seedThreads();
  const fnT = load(newSrc);
  let tot = { reads: 0, aggs: 0, bytes: 0 }, totOld = { reads: 0, aggs: 0, bytes: 0 };
  let prevDocs = null;
  const word = "zebraprint";
  const oldFn = oldSrc ? load(oldSrc) : null;
  for (let n = 2; n <= word.length; n++) {
    const q = word.slice(0, n);
    const a = await call(fnT, "search.typing.after", sq(q));
    tot.reads += a.cost.reads; tot.aggs += a.cost.aggs; tot.bytes += a.cost.bytes;
    if (oldFn) {
      const saveDocs = JSON.parse(JSON.stringify(m.db.dump()));       // the old run must not see this run's backfills (none here)
      const b = await call(oldFn, "search.typing.before", sq(q));
      totOld.reads += b.cost.reads; totOld.aggs += b.cost.aggs; totOld.bytes += b.cost.bytes;
      check(JSON.stringify(docsOf(a.body)) === JSON.stringify(docsOf(b.body)) && JSON.stringify(a.body.docs.map(plain)) === JSON.stringify(b.body.docs.map(plain)),
        "typing \"" + q + "\": the same threads and fields as a fresh whole-thread search");
      void saveDocs;
    }
    prevDocs = a.body;
  }
  void prevDocs;
  if (oldFn) console.log("       the nine requests: before " + (totOld.reads + totOld.aggs) + " reads " + (totOld.bytes / 1024 / 1024).toFixed(1) + " MB; after " + (tot.reads + tot.aggs) + " reads " + (tot.bytes / 1024 / 1024).toFixed(1) + " MB");
  check(tot.reads + tot.aggs < 1300, "typing the word: under 1,300 reads in all (the first two scans, then the reads of the matches): " + (tot.reads + tot.aggs));
  // a limit larger than 500 is held at 500
  seedThreads();
  const big = await call(load(newSrc), "search.limit2000", sq("zebraprint", { limit: "2000" }));
  check(big.body.scanned === 500, "limit=2000 reads 500 threads, not 2,000 (scanned " + big.body.scanned + ")");
  // a status-scoped search still works and reads only that status
  const scoped = await call(load(newSrc), "search.status", sq("buyer", { status: "sent" }));
  check(scoped.body.docs.every(d => d.status === "sent") && scoped.body.count > 0, "a search scoped to one status returns only that status (" + scoped.body.count + ")");
  // an unchanged repeat inside 15 s costs nothing
  const fnC = load(newSrc);
  await call(fnC, "search.once", sq("zebraprint"));
  const again = await call(fnC, "search.twice", sq("zebraprint"));
  check(again.cost.reads === 0 && again.cost.aggs === 0, "the same search again within 15 s reads nothing");
  // time passes: the shorter answer is no longer fresh, so the longer query reads again
  const realNow = Date.now; let skew = 0; Date.now = () => realNow() + skew;
  const fnS = load(newSrc);
  await call(fnS, "search.ttl.a", sq("zebra"));
  skew = 16000;
  const stale = await call(fnS, "search.ttl.b", sq("zebraprint"));
  Date.now = realNow;
  check(stale.cost.reads + stale.cost.aggs > 400, "after 15 s a longer query reads fresh data (" + (stale.cost.reads + stale.cost.aggs) + " reads)");

  /* ───────────── (3) etsyMailSync-background, mode buyer ───────────── */
  console.log("\n(3) etsyMailSync-background mode=buyer: a buyer with 18 mirrored receipts (each carries the whole Etsy receipt)");
  m.queryGuard = meter.missingCompositeIndex(["EtsyMail_Receipts|buyer_user_id,created_timestamp"]);     // the index the buyer query already has
  let oldSync = null;
  try { oldSync = cp.execFileSync("git", ["show", BEFORE + ":netlify/functions/etsyMailSync-background.js"], { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }); } catch (_) { /* no history */ }
  const syncSrc = fs.readFileSync(path.join(FN_DIR, "etsyMailSync-background.js"), "utf8");
  const loadSync = src => { const file = path.join(FN_DIR, "etsyMailSync-background.js"); const mod = new Module(file, module); mod.filename = file; mod.paths = Module._nodeModulePaths(FN_DIR); mod._compile(src, file); return mod.exports; };
  const seedReceipts = () => {
    m.db.docs.clear();
    const seed = {};
    for (let i = 0; i < 18; i++) {
      const buyer = i < 18 ? "5001" : "5002";
      seed["EtsyMail_Receipts/" + (3000000000 + i)] = { receipt_id: String(3000000000 + i), buyer_user_id: buyer, buyer_name: "Buyer " + buyer, created_timestamp: 1700000000 - i * 3600, updated_timestamp: 1700000100 - i * 3600, status: i % 4 ? "Completed" : "Paid", is_paid: true, is_shipped: i % 2 === 0, grandtotal_amount: 40 + i, grandtotal_currency: "USD",
        raw: { receipt_id: 3000000000 + i, transactions: [1, 2].map(k => ({ transaction_id: k, title: text(140), description: text(500) })), formatted_address: text(160), message_from_buyer: text(300) }, mirroredAt: 1, ttl: 2 };
    }
    seed["EtsyMail_Receipts/3000000099"] = { receipt_id: "3000000099", buyer_user_id: "5002", buyer_name: "Other", created_timestamp: 1, grandtotal_amount: 5, grandtotal_currency: "USD", raw: { pad: text(2000) } };
    m.db.seed(seed);
  };
  const syncCall = async (fn, label) => {
    const a = m.snapshot();
    const r = await m.op(label, () => fn.handler({ httpMethod: "POST", headers: H, queryStringParameters: {}, body: JSON.stringify({ mode: "buyer", buyerUserId: "5001", receiptId: "3000000003" }) }));
    assert.ok(r.statusCode === 200 && JSON.parse(r.body).ok === true, label + ": " + String(r.body).slice(0, 200));
    const cust = JSON.parse(JSON.stringify(m.db.dump()["EtsyMail_Customers/5001"] || null));
    for (const k of ["updatedAt", "lastBuyerSyncAt", "nextBuyerSyncEligibleAtMs", "lastBuyerSyncJitterSeconds"]) if (cust) delete cust[k];
    return { cost: m.since(a), cust, out: JSON.parse(r.body) };
  };
  if (!oldSync) console.log("  (no git history: the version before the change is not run)");
  let before3 = null;
  if (oldSync) { seedReceipts(); before3 = await syncCall(loadSync(oldSync), "buyer.before"); row("buyer sync: before", before3.cost); }
  seedReceipts();
  const after3 = await syncCall(loadSync(syncSrc), "buyer.after");
  row("buyer sync: after", after3.cost);
  check(after3.cust && after3.cust.orderCount === 18 && after3.cust.recentReceipts.length > 0, "the customer record is written for the 18 receipts of the buyer");
  if (before3) {
    check(JSON.stringify(after3.cust) === JSON.stringify(before3.cust), "the customer record is the same as before the change (orders, totals, first/last order, recent receipts)");
    check(JSON.stringify(after3.out) === JSON.stringify(before3.out), "the answer is the same as before the change");
    check(after3.cost.bytes < before3.cost.bytes * 0.4, "a fraction of the bytes (" + kb(after3.cost) + " vs " + kb(before3.cost) + ")");
    check(after3.cost.reads + after3.cost.aggs <= before3.cost.reads + before3.cost.aggs, "no more reads than before");
  }

  m.uninstall();
  console.log("\n" + (fails.length ? "FAIL " + fails.length + "\n  " + fails.join("\n  ") : "PASS"));
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
