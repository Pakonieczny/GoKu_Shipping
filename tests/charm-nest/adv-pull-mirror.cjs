// Adversarial (ADVPULL, 15 Oct 2026): the receipts mirror (etsyMailReceiptsMirrorCron) against an Etsy that sends a page it cannot
// read, a body that never ends, or a long catch-up, in a function that lives 30 s and saves its watermark last.
// Drives the real mirror against an in-memory Firestore and a pretend Etsy (min_last_modified, sort updated asc, offset / limit).
// No network, no real services.
//   · an unreadable page (garbage, or no list in it) is a FAILED page: the run ends "error" with the pages before it kept and the
//     watermark moved up to them, never a thrown error that saves nothing, never an "end of the list" that looks like success;
//   · headers that came and a body that never ends are cut by the page's clock (it used to be cleared once the headers were in);
//   · a catch-up of 3,000 changed receipts does not run past the function's 30 s: it stops starting pages at 18 s, saves the watermark
//     and the next run goes on from it, until every receipt is mirrored exactly once, none lost, none skipped.
//   node tests/charm-nest/adv-pull-mirror.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const fnDir = path.join(__dirname, "../../netlify/functions");

/* ── a small in-memory Firestore: documents, batches, the buyer query of the customer rebuild ── */
const store = new Map(), SERVER_TS = { __ts: true };
const clone = v => JSON.parse(JSON.stringify(v, (k, x) => (x === SERVER_TS ? "__TS__" : x)), (k, x) => (x === "__TS__" ? Date.now() : x));
const docRef = (coll, id) => { const key = coll + "/" + id;
  return { id, path: key, coll,
    async get() { const d = store.get(key); return { exists: !!d, id, data: () => (d ? clone(d) : undefined) }; },
    async set(data, o) { store.set(key, Object.assign({}, (o && o.merge && store.get(key)) || {}, clone(data))); } }; };
const query = (coll, filters = []) => ({ where: (f, op, v) => query(coll, filters.concat([[f, v]])), orderBy: () => query(coll, filters), select: () => query(coll, filters), limit: () => query(coll, filters), doc: id => docRef(coll, id),
  async get() { const docs = [...store.entries()].filter(([k, d]) => k.startsWith(coll + "/") && filters.every(([f, v]) => d[f] === v)).map(([, d]) => ({ data: () => clone(d) })); return { docs, size: docs.length, empty: !docs.length }; } });
const db = { collection: c => query(c), doc: p => docRef(p.slice(0, p.lastIndexOf("/")), p.slice(p.lastIndexOf("/") + 1)),
  batch() { const ops = []; return { set: (r, d, o) => ops.push([r, d, o]), async commit() { for (const [r, d, o] of ops) await r.set(d, o); } }; } };
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, increment: n => n, delete: () => undefined }, Timestamp: { fromMillis: ms => ({ toMillis: () => ms }) } }) };

// a clock the test moves: a page "takes" seconds of the function's life without the test waiting them
const realNow = Date.now; let skew = 0; Date.now = () => realNow() + skew;
const realSetTimeout = global.setTimeout;

/* ── a pretend Etsy: the shop's receipts, asked as the mirror asks (updated asc from min_last_modified) ── */
let shop = [], mode = "ok", pageCost = 0; const asked = [];
const Module = require("module"), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === "firebase-admin" || /[\\/]firebaseAdmin(\.js)?$/.test(req) || req === "./firebaseAdmin") return fakeAdmin;
  if (req === "./_orderCancel") throw new Error("cancel records are off in this test");
  if (req === "./_etsyApiMeter") return { bump: () => ({ failNet() {}, fromHttp() {} }), wrapHandler: fn => fn };
  if (req === "./_etsyMailEtsy") return { getValidEtsyAccessToken: async () => "token" };
  if (req === "node-fetch") return async (url, init) => {
    const u = new URL(url), min = +u.searchParams.get("min_last_modified"), off = +u.searchParams.get("offset"), lim = +u.searchParams.get("limit");
    asked.push({ min, off }); skew += pageCost;
    const results = shop.filter(r => r.updated_timestamp >= min).sort((a, b) => a.updated_timestamp - b.updated_timestamp).slice(off, off + lim);
    const ok = body => ({ ok: true, status: 200, headers: { get: () => null }, json: body, text: async () => "" });
    if (mode === "garbage" && off === 100) return ok(async () => { throw new SyntaxError("Unexpected token < in JSON at position 0"); });
    if (mode === "nolist" && off === 100) return ok(async () => ({ error: "something else" }));
    if (mode === "stall" && off === 0) return ok(() => new Promise((_, no) => init.signal.addEventListener("abort", () => no(Object.assign(new Error("The user aborted a request."), { name: "AbortError" })))));
    return ok(async () => ({ count: shop.length, results }));
  };
  return realLoad.call(this, req, ...rest);
};
Object.assign(process.env, { SHOP_ID: "1", CLIENT_ID: "c", CLIENT_SECRET: "s" });
const mirror = require(path.join(fnDir, "etsyMailReceiptsMirrorCron.js"));
const realWarn = console.warn, realLog = console.log, realErr = console.error; console.warn = () => {}; console.log = () => {}; console.error = () => {};

const T0 = Math.floor(realNow() / 1000) - 86400;
const receipts = (n, from = 0) => Array.from({ length: n }, (_, i) => ({ receipt_id: 4300000000 + from + i, status: "Paid", is_paid: true, is_shipped: false, created_timestamp: T0, updated_timestamp: T0 + from + i + 1, transactions: [] }));
const state = () => store.get("EtsyMail_Config/receiptsMirrorState") || {};
const mirrored = () => [...store.keys()].filter(k => k.startsWith("EtsyMail_Receipts/")).length;
const reset = () => { store.clear(); asked.length = 0; skew = 0; pageCost = 0; mode = "ok"; store.set("EtsyMail_Config/receiptsMirrorState", { lastSyncTimestamp: T0 }); };
const run = async () => { store.set("EtsyMail_Config/receiptsMirrorState", Object.assign({}, state(), { nextEligibleAtMs: 0 })); return JSON.parse((await mirror.handler({})).body); };

let failed = 0, finished = false;
// (a call that never answers leaves nothing to wait for, and node then ends quietly with exit code 0: that is a failure, not a pass)
process.on("exit", () => { if (!finished) { realLog("FAIL  the run never reached its end: a call did not answer"); process.exitCode = 1; } });
async function check(name, fn) { try { await fn(); realLog("ok    " + name); } catch (e) { failed++; realLog("FAIL  " + name + "\n      " + String(e && e.stack || e).split("\n").slice(0, 3).join("\n      ")); } }

(async () => {
  await check("an unreadable page (garbage) is a failed page: the page before it is kept and the watermark moves up to it", async () => {
    reset(); shop = receipts(150); mode = "garbage";
    const out = await run();
    assert.equal(out.outcome, "error", JSON.stringify(out)); assert.equal(out.pagesFetched, 2); assert.equal(out.receiptsProcessed, 100);
    assert.equal(mirrored(), 100, "page one is kept"); assert.equal(state().lastSyncTimestamp, T0 + 100 + 1, "the watermark moved to page one's newest, plus one");
    assert.match(state().lastSyncErrorMsg, /^bad_body: unreadable page/);
    mode = "ok"; const again = await run();   // the next run goes on from there and finishes the list
    assert.equal(again.outcome, "ok"); assert.equal(mirrored(), 150, "nothing lost"); assert.equal(again.receiptsProcessed, 50);
  });

  await check("a 200 with no list in it is a failed page, not the end of the list", async () => {
    reset(); shop = receipts(150); mode = "nolist";
    const out = await run();
    assert.equal(out.outcome, "error"); assert.match(state().lastSyncErrorMsg, /^bad_body: the page has no list/); assert.equal(mirrored(), 100);
  });

  await check("headers that came and a body that never ends are cut by the page's clock, and the run says so", async () => {
    reset(); shop = receipts(40); mode = "stall";
    global.setTimeout = (fn, ms, ...a) => realSetTimeout(fn, ms >= 1000 ? 30 : ms, ...a);   // (the page's 25 s clock, in 30 ms)
    let out; try { out = await run(); } finally { global.setTimeout = realSetTimeout; }
    assert.equal(out.outcome, "error", JSON.stringify(out)); assert.equal(state().lastSyncErrorMsg, "timeout: Etsy body timeout"); assert.equal(mirrored(), 0);
    assert.equal(state().lastSyncTimestamp, T0, "nothing was read: the watermark stays, the same window is asked again");
  });

  await check("a catch-up of 3,000 changed receipts stops starting pages at 18 s, saves the watermark, and the next runs finish it with none lost", async () => {
    reset(); shop = receipts(3000); pageCost = 1500;   // (a slow Etsy day: 1.5 s a page)
    const first = await run();
    assert.equal(first.outcome, "ok", JSON.stringify(first)); assert(first.pagesFetched >= 10 && first.pagesFetched <= 14, "stopped by the clock, not by the page cap: " + first.pagesFetched + " pages");
    assert.equal(state().lastSyncCutByClock, true); assert.equal(state().lastSyncErrorMsg, null, "told as no error"); assert(state().lastSyncElapsedMs <= 25000, "inside the function's 30 s: " + state().lastSyncElapsedMs + " ms");
    assert.equal(mirrored(), first.receiptsProcessed); assert.equal(state().lastSyncTimestamp, T0 + first.receiptsProcessed + 1, "the watermark is saved at what was read");
    let runs = 1; while (mirrored() < 3000 && runs < 6) { skew = 0; await run(); runs++; }
    assert.equal(mirrored(), 3000, "every receipt mirrored after " + runs + " runs"); assert(runs <= 4, "a few runs, not a loop: " + runs);
    const ids = new Set([...store.keys()].filter(k => k.startsWith("EtsyMail_Receipts/"))); assert.equal(ids.size, 3000);
  });

  await check("the customer rebuild stops at 25 s and counts the rest as skipped; the state is still saved", async () => {
    reset(); shop = receipts(60).map((r, i) => Object.assign(r, { buyer_user_id: 7000 + i })); pageCost = 26000;   // (the one page "took" 26 s)
    const out = await run();
    assert.equal(out.outcome, "ok"); assert.equal(out.customerRebuildAttempted, 60, JSON.stringify(out)); assert.equal(out.customerRebuildUpdated, 0, "no time left for any");
    assert.equal(out.customerRebuildSkipped, 60); assert.equal(state().lastSyncTimestamp, T0 + 60 + 1, "and the watermark is saved");
  });

  Date.now = realNow; finished = true;
  if (failed) { realLog(failed + " check(s) failed"); process.exit(1); }
  realLog("mirror adversarial OK: an unreadable page and a stalled body are failed pages, a 3,000-receipt catch-up is cut by the clock and finished by the next runs");
})().catch(e => { realErr(e); process.exit(1); });
