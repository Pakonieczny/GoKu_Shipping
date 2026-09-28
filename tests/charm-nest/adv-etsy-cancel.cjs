// Adversarial checks of Etsy cancel detection (task G, 28 Sep): status spellings and missing fields, the same page twice,
// a restore landing inside the mirror's write, an order restored and then changed again on Etsy, a shipped-then-refunded
// order in the derived timeline, the hook's time budget across a whole mirror run, sandbox isolation and record size.
// Drives the real receipts mirror and charmNestLibrary ops against an in-memory Firestore; a pretend Etsy serves pages.
// No network, no real services.
//   node tests/charm-nest/adv-etsy-cancel.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const root = path.join(__dirname, "../.."), fnDir = path.join(root, "netlify/functions");

/* ── in-memory Firestore (as etsy-cancel.cjs): atomic batches, getAll, where ==/in, select; hooks to interleave ── */
const store = new Map(), cost = { getAll: 0, cancelGetAll: 0, commits: 0 };
const SERVER_TS = { __ts: true };
const clone = v => JSON.parse(JSON.stringify(v, (k, x) => (x === SERVER_TS ? "__TS__" : x)), (k, x) => (x === "__TS__" ? Date.now() : x));
let afterGetAll = null, slowCancelRead = 0, skew = 0;
function docRef(coll, id) {
  const key = coll + "/" + id;
  const snap = () => { const d = store.get(key); return { exists: !!d, id, ref: docRef(coll, id), data: () => (d ? clone(d) : undefined) }; };
  return { id, path: key, coll, _snap: snap,
    async get() { return snap(); },
    async set(data, opts) { store.set(key, Object.assign({}, (opts && opts.merge && store.get(key)) || {}, clone(data))); },
    async update(data) { if (!store.has(key)) throw new Error("NOT_FOUND: " + key); store.set(key, Object.assign({}, store.get(key), clone(data))); },
    async create(data) { if (store.has(key)) throw new Error("ALREADY_EXISTS: " + key); store.set(key, clone(data)); },
    async delete() { store.delete(key); } };
}
function query(coll, filters = [], order = null, lim = 0, after = null) {
  return {
    where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), order, lim, after),
    orderBy: (f, dir) => query(coll, filters, [f, dir || "asc"], lim, after),
    startAfter: v => query(coll, filters, order, lim, v),
    limit: n => query(coll, filters, order, n, after),
    select: () => query(coll, filters, order, lim, after),
    doc: id => docRef(coll, id),
    async get() {
      let rows = [...store.keys()].filter(k => k.startsWith(coll + "/")).map(k => docRef(coll, k.slice(coll.length + 1))._snap());
      for (const [f, op, v] of filters) {
        if (op === "in") assert(Array.isArray(v) && v.length >= 1 && v.length <= 30, "in takes 1 to 30 values");
        rows = rows.filter(r => { const x = r.data()[f]; return op === "==" ? x === v : op === "in" ? v.includes(x) : op === "array-contains" ? Array.isArray(x) && x.includes(v) : true; });
      }
      const val = (r, f) => (f === "__name__" ? r.id : r.data()[f]);
      if (order) rows.sort((a, b) => { const x = val(a, order[0]), y = val(b, order[0]); const c = x > y ? 1 : x < y ? -1 : 0; return order[1] === "desc" ? -c : c; });
      if (after != null) { assert(order, "startAfter needs an orderBy"); rows = rows.filter(r => val(r, order[0]) > after); }
      if (lim) rows = rows.slice(0, lim);
      return { size: rows.length, docs: rows, empty: !rows.length };
    }
  };
}
const db = {
  collection: c => query(c),
  doc: p => docRef(p.slice(0, p.lastIndexOf("/")), p.slice(p.lastIndexOf("/") + 1)),
  batch() {
    const ops = [];
    return { set(r, d, o) { ops.push(["set", r, d, o]); }, update(r, d) { ops.push(["update", r, d]); }, create(r, d) { ops.push(["create", r, d]); }, delete(r) { ops.push(["delete", r]); },
      async commit() {
        cost.commits++;
        assert(ops.length <= 500, "a batch holds at most 500 writes");
        for (const [k, r] of ops) { if (k === "create" && store.has(r.path)) throw new Error("ALREADY_EXISTS: " + r.path); if (k === "update" && !store.has(r.path)) throw new Error("NOT_FOUND: " + r.path); }
        for (const [k, r, d, o] of ops) await r[k](d, o);
      } };
  },
  async getAll(...args) {
    const refs = args.filter(r => r && r._snap);   // (a trailing { fieldMask } is read options)
    cost.getAll++;
    if (refs.some(r => /Charm_Nest_Cancelled/.test(r.coll))) { cost.cancelGetAll++; if (slowCancelRead) { skew += slowCancelRead; await new Promise(r => setTimeout(r, 2)); } }
    const out = refs.map(r => r._snap()); if (afterGetAll) { const f = afterGetAll; afterGetAll = null; await f(); } return out;
  },
  async runTransaction(fn) { return fn({ get: r => r.get(), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), create: (r, d) => r.create(d), delete: r => r.delete() }); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, increment: n => n, delete: () => undefined, arrayUnion: (...a) => a }, Timestamp: { fromMillis: ms => ({ toMillis: () => ms }) }, FieldPath: { documentId: () => "__name__" } }),
  storage: () => ({ bucket: () => ({ name: "test", file: () => ({ exists: async () => [false] }) }) }) };

// a clock the test can move on: a slow Firestore read "takes" seconds without the test waiting them
const realNow = Date.now; Date.now = () => realNow() + skew;

let etsyPages = [], etsyCalls = 0;
const Module = require("module"), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === "firebase-admin" || /[\\/]firebaseAdmin(\.js)?$/.test(req) || req === "./firebaseAdmin") return fakeAdmin;
  if (req === "node-fetch") return async url => {
    assert(/^https:\/\/api\.etsy\.com\//.test(String(url)), "the only fetch the mirror makes is Etsy's receipts page: " + url);
    etsyCalls++; const results = etsyPages.shift() || [];
    return { ok: true, status: 200, headers: { get: () => null }, json: async () => ({ count: results.length, results }), text: async () => "" };
  };
  if (req === "./_etsyApiMeter") return { bump: () => ({ failNet() {}, fromHttp() {} }), wrapHandler: fn => fn };
  if (req === "./_etsyMailEtsy") return { getValidEtsyAccessToken: async () => "token" };
  return realLoad.call(this, req, ...rest);
};
Object.assign(process.env, { SHOP_ID: "1", CLIENT_ID: "c", CLIENT_SECRET: "s" });
delete process.env.EDIT_PASSCODE;
const mirror = require(path.join(fnDir, "etsyMailReceiptsMirrorCron.js"));
const lib = require(path.join(fnDir, "charmNestLibrary.js"));
const OrderCancel = require(path.join(fnDir, "_orderCancel.js"));
const post = async body => { const r = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body || "{}") }; };
const realWarn = console.warn, realLog = console.log; console.warn = () => {}; console.log = () => {};

const T0 = Math.floor(realNow() / 1000) - 3600;
let nextTx = 6000000000;
const receipt = (id, status, extra = {}) => Object.assign({ receipt_id: +id, buyer_user_id: 88, name: "Buyer " + id, status, is_paid: true, is_shipped: false, created_timestamp: T0 - 86400, updated_timestamp: T0,
  transactions: [{ transaction_id: nextTx++, sku: "GF-HEART", title: "Heart charm", quantity: 1, expected_ship_date: T0 + 86400 }] }, extra);
const cancelDoc = (id, prefix = "") => store.get(prefix + "Charm_Nest_Cancelled/" + id);
const events = (id, prefix = "") => [...store.entries()].filter(([k, v]) => k.startsWith(prefix + "Order_Timeline/") && v.orderId === id).map(([, v]) => v);
const lastDiag = () => [...store.entries()].filter(([k]) => k.startsWith("EtsyMail_DiagnosticLog/")).map(([, v]) => v).filter(v => v.phase === "end" && v.etsyCancels).pop();
async function runMirror(pages) { etsyPages = pages.slice(); store.delete("EtsyMail_Config/receiptsMirrorState"); const r = await mirror.handler({}); return JSON.parse(r.body); }
let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; realLog("ok    " + name); }
  catch (e) { failed++; realLog("FAIL  " + name + "\n      " + String(e && e.message || e).split("\n")[0]); }
}

(async () => {
  await check("status spellings and missing status: only a cancel, or a full refund before shipping", () => {
    const yes = ["Canceled", "canceled", "Cancelled", "cancelled", "CANCELED", "Fully Refunded", "fully refunded"];
    for (const st of yes) assert.equal(OrderCancel.isCancelled({ status: st }), true, st);
    for (const st of ["Partially Refunded", "partially refunded", "Paid", "Completed", "Open", "", null, undefined]) assert.equal(OrderCancel.isCancelled({ status: st }), false, String(st));
    assert.equal(OrderCancel.isCancelled({ status: "Fully Refunded", raw: { is_shipped: true } }), false, "shipped (raw), then refunded: a return");
    assert.equal(OrderCancel.isCancelled({ status: null, raw: { status: "Canceled" } }), true, "the raw receipt's status when the top one is missing");
    assert.equal(OrderCancel.isCancelled(null), false); assert.equal(OrderCancel.isCancelled("Canceled"), false);
  });

  await check("missing fields: no id is skipped, no lines / dates / timestamps / null transactions do not throw", async () => {
    const out = await runMirror([[
      { status: "Canceled" },                                                         // no receipt_id
      { receipt_id: 4200000001, status: "Canceled" },                                 // nothing else at all
      { receipt_id: 4200000002, status: "Canceled", transactions: null, updated_timestamp: T0 },
      { receipt_id: 4200000003, status: "canceled", transactions: [null, { transaction_id: null, quantity: "x" }], updated_timestamp: T0 },
      { receipt_id: 4200000004, status: null, updated_timestamp: T0 }
    ]]);
    assert.equal(out.ok, true, JSON.stringify(out));
    assert(cancelDoc("4200000001") && cancelDoc("4200000002") && cancelDoc("4200000003"));
    assert.equal(cancelDoc("4200000003").lines.length, 2); assert.equal(cancelDoc("4200000003").lines[1].quantity, 1);
    assert(!cancelDoc("4200000004"));
  });

  await check("the same page twice, and one receipt twice in a page: one record, one event", async () => {
    const page = [receipt("4200000010", "Canceled", { updated_timestamp: T0 + 5 }), receipt("4200000010", "Canceled", { updated_timestamp: T0 + 5 })];
    await runMirror([page]); const snap = JSON.stringify(cancelDoc("4200000010"));
    await runMirror([page]);
    assert.equal(JSON.stringify(cancelDoc("4200000010")), snap); assert.equal(events("4200000010").length, 1);
    // Etsy's word changes (Canceled → Fully Refunded): noted, still one event
    await runMirror([[receipt("4200000010", "Fully Refunded", { updated_timestamp: T0 + 50 })]]);
    assert.equal(cancelDoc("4200000010").etsyStatus, "Fully Refunded"); assert.equal(events("4200000010").length, 1);
  });

  await check("a restore landing between the mirror's read and its batch is not undone", async () => {
    // (Paul's record already carries Etsy's word, so his restore knew it; a person's own cancel restored unaware of Etsy's
    //  is recorded as Etsy's: adv-a3-cancel.cjs)
    await post({ op: "cancelPut", orderId: "4200000020", by: "Paul", why: "asked", etsyStatus: "Canceled", record: {} });
    // the mirror reads Paul's record (and would add the receipt's lines to it); Paul restores the order before its batch lands
    afterGetAll = () => post({ op: "cancelRestore", orderId: "4200000020", by: "Paul" });
    const out = await runMirror([[receipt("4200000020", "Canceled", { updated_timestamp: T0 + 10 }), receipt("4200000021", "Canceled", { updated_timestamp: T0 + 10 })]]);
    assert.equal(out.ok, true);
    assert(cancelDoc("4200000021"), "the other order of the page is still recorded");
    assert(!cancelDoc("4200000020"), "the order Paul restored after Etsy's change must stay restored");
  });

  await check("restored, then changed again on Etsy with the same status: stays restored; a new status cancels it", async () => {
    await runMirror([[receipt("4200000030", "Fully Refunded", { updated_timestamp: T0 + 10 })]]);
    assert.equal(cancelDoc("4200000030").etsyStatus, "Fully Refunded");
    assert.equal((await post({ op: "cancelRestore", orderId: "4200000030", by: "Paul" })).body.ok, true);   // refunded, but it ships anyway
    await runMirror([[receipt("4200000030", "Fully Refunded", { updated_timestamp: Math.floor(Date.now() / 1000) + 30 })]]);   // the buyer's note changed
    assert(!cancelDoc("4200000030"), "nothing new from Etsy: the person's restore stands");
    await runMirror([[receipt("4200000030", "Canceled", { updated_timestamp: Math.floor(Date.now() / 1000) + 60 })]]);
    assert(cancelDoc("4200000030"), "Etsy now cancelled it: cancelled again");
    // a person's own cancel (Etsy never said) restored, then Etsy cancels: that is news
    await post({ op: "cancelPut", orderId: "4200000031", by: "Kim", why: "dup", record: {} });
    await post({ op: "cancelRestore", orderId: "4200000031", by: "Kim" });
    await runMirror([[receipt("4200000031", "Canceled", { updated_timestamp: Math.floor(Date.now() / 1000) + 30 })]]);
    assert(cancelDoc("4200000031"), "Etsy's cancel after a person's restore of their own cancel counts");
  });

  await check("shipped, then fully refunded: no record, and the order view does not call it cancelled", async () => {
    const r = receipt("4200000040", "Fully Refunded", { is_shipped: true, updated_timestamp: T0 + 900 });
    await runMirror([[r]]);
    assert(!cancelDoc("4200000040"), "a return is not a cancel");
    const tl = await post({ op: "timelineGet", orderId: "4200000040" });
    assert.equal(tl.status, 200);
    assert.notEqual(tl.body.where.stage, "cancelled", "where: " + JSON.stringify(tl.body.where && tl.body.where.label));
    assert(!tl.body.events.some(e => e.type === "etsyCancelled"), "no etsyCancelled event for a return");
    // unshipped and fully refunded still reads as cancelled
    await runMirror([[receipt("4200000041", "Fully Refunded", { updated_timestamp: T0 + 900 })]]);
    assert.equal((await post({ op: "timelineGet", orderId: "4200000041" })).body.where.stage, "cancelled");
  });

  await check("the hook's time is capped for the whole run, not per page (the mirror is never slowed past 8 s)", async () => {
    const full = base => Array.from({ length: 100 }, (_, i) => receipt(String(base + i), i === 0 ? "Canceled" : "Paid", { updated_timestamp: T0 + 1000 }));
    const pages = [full(4210000000), full(4210000100), full(4210000200), full(4210000300), full(4210000400), [receipt("4210000500", "Canceled")]];
    slowCancelRead = 3000; const g0 = cost.cancelGetAll;
    const out = await runMirror(pages); slowCancelRead = 0;
    assert.equal(out.ok, true); assert.equal(out.receiptsProcessed, 501, "every page is mirrored");
    const calls = cost.cancelGetAll - g0;
    assert(calls <= 3, `each page's hook "took" 3 s; ${calls} pages were given it (≤ 3 fit in 8 s)`);
    assert.equal(lastDiag().etsyCancels.off, true, "the run notes the hook was stopped");
  });

  await check("missed pages wait in a backlog; the next run records them from the mirror, with no Etsy call of its own", async () => {
    // the run above had no time left for its last three pages: their cancelled receipts were kept
    const bl = store.get("Charm_Nest_Cancelled_Backlog/pending");
    assert(bl, "a backlog was kept"); assert.deepEqual(bl.ids.slice().sort(), ["4210000300", "4210000400", "4210000500"]);
    assert(!cancelDoc("4210000500"));
    const e0 = etsyCalls; await runMirror([[]]);
    assert.equal(etsyCalls - e0, 1, "only the mirror's own page");
    for (const id of ["4210000300", "4210000400", "4210000500"]) assert.equal(cancelDoc(id).by, "Etsy", id);
    assert(!store.has("Charm_Nest_Cancelled_Backlog/pending"), "done: the backlog is gone");
    // capped at 200 ids, newest kept; a retry that fails keeps them all
    await OrderCancel.keepBacklog(db, Array.from({ length: 250 }, (_, i) => String(4220000000 + i)));
    const ids = store.get("Charm_Nest_Cancelled_Backlog/pending").ids; assert.equal(ids.length, 200); assert.equal(ids[199], "4220000249");
    const realGetAll = db.getAll; db.getAll = async () => { throw new Error("UNAVAILABLE"); };
    try { await assert.rejects(OrderCancel.retryBacklog(db, fakeAdmin.firestore.FieldValue)); } finally { db.getAll = realGetAll; }
    assert.equal(store.get("Charm_Nest_Cancelled_Backlog/pending").ids.length, 200, "a failed retry keeps them all");
    const r = await OrderCancel.retryBacklog(db, fakeAdmin.firestore.FieldValue); assert.equal(r.retried, 200);
    assert(!store.has("Charm_Nest_Cancelled_Backlog/pending"), "ids not in the mirror are done too");
  });

  await check("cancelSweep resumes where it stopped: each call makes progress within its budget", async () => {
    for (let i = 0; i < 450; i++) { const id = String(4230000000 + i); store.set("EtsyMail_Receipts/" + id, { receipt_id: id, status: "Canceled", is_shipped: false, updated_timestamp: T0, created_timestamp: T0 - 9, buyer_name: "B", raw: { receipt_id: id, status: "Canceled" } }); }
    slowCancelRead = 4000;   // each page's reads "take" 8 s: over the 7 s budget after one page
    let cursor, calls = 0, created = 0;
    try {
      do {
        const r = await post({ op: "cancelSweep", cursor }); calls++;
        assert.equal(r.status, 200, JSON.stringify(r.body)); created += r.body.created;
        assert(r.body.scanned > 0, "every call reads on"); cursor = r.body.more ? r.body.next : null;
        if (r.body.more) assert(r.body.next && Number.isInteger(r.body.next.s), "a resume point");
      } while (cursor && calls < 20);
    } finally { slowCancelRead = 0; }
    assert(calls >= 3 && calls < 20, "calls: " + calls);
    for (let i = 0; i < 450; i++) assert(cancelDoc(String(4230000000 + i)), "recorded: " + (4230000000 + i));
    assert(created >= 450);
    const again = await post({ op: "cancelSweep" }); assert.equal(again.body.created, 0); assert.equal(again.body.more, false);
  });

  await check("sandbox: the mirror and the sweep never write Sandbox_; sandbox ops never write production", async () => {
    const before = [...store.keys()].filter(k => k.startsWith("Sandbox_")).length;
    await runMirror([[receipt("4200000050", "Canceled")]]);
    assert.equal([...store.keys()].filter(k => k.startsWith("Sandbox_")).length, before);
    await post({ op: "cancelPut", orderId: "4200000051", by: "T", sandbox: true, record: {} });
    await post({ op: "sandboxCancel", orderId: "4200000052" });   // no sandbox flag: still the sandbox
    await post({ op: "cancelFates", orderId: "4200000050", sandbox: true, fates: [{ sheet: "GF Sheet 1", fate: "removed" }] });
    await post({ op: "cancelRestore", orderId: "4200000050", sandbox: true });
    assert(!cancelDoc("4200000051") && !cancelDoc("4200000052")); assert(cancelDoc("4200000051", "Sandbox_") && cancelDoc("4200000052", "Sandbox_"));
    assert(cancelDoc("4200000050") && !cancelDoc("4200000050").fates, "the production record is untouched by sandbox fates and restore");
    assert.equal(events("4200000051").length + events("4200000052").length, 0);
  });

  await check("record size: many lines and long titles stay small; the restore event keeps Etsy's word", async () => {
    const tx = Array.from({ length: 250 }, (_, i) => ({ transaction_id: 7000000000 + i, sku: "S".repeat(500), title: "T".repeat(5000), quantity: 1 }));
    await runMirror([[receipt("4200000060", "Canceled", { transactions: tx })]]);
    const c = cancelDoc("4200000060"); assert(JSON.stringify(c).length < 100000, "record " + JSON.stringify(c).length + " bytes");
    assert.equal(c.lines.length, 60); assert.equal(c.lines[0].title.length, 200);
    await post({ op: "cancelPut", orderId: "4200000060", by: "P".repeat(500), why: "W".repeat(5000), record: { sheets: Array.from({ length: 50 }, (_, i) => "GF Sheet " + i + "x".repeat(300)) } });
    assert.equal((await post({ op: "cancelRestore", orderId: "4200000060", by: "Paul" })).body.ok, true);
    const ev = events("4200000060").find(e => e.type === "cancelRestored");
    assert(ev && ev.data && ev.data.cancelled, "the restore event keeps the record"); assert(JSON.stringify(ev).length < 4096);
    assert.equal(ev.data.cancelled.etsyStatus, "Canceled");
  });

  console.warn = realWarn; console.log = realLog; Date.now = realNow;
  console.log(`adv-etsy-cancel: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.warn = realWarn; console.log = realLog; console.error(e); process.exit(1); });
