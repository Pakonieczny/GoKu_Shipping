// Adversarial wave 3, area 1 (Etsy cancel detection, server): a person who restores their own cancel, unaware that Etsy
// cancelled the order meanwhile, must not make Etsy's cancel disappear (mirror, backlog and the restore-mid-write race);
// the sweep's dry run and string cursor; a restored Fully Refunded order stays restored through the sweep and backlog;
// a page's hook error sends only that page to the backlog. Harness as adv-etsy-cancel.cjs: the real receipts mirror and
// charmNestLibrary ops against an in-memory Firestore; a pretend Etsy serves pages. No network, no real services.
//   node tests/charm-nest/adv-a3-cancel.cjs
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
  catch (e) { failed++; realLog("FAIL  " + name + "\n      " + String(e && e.stack || e).split("\n").slice(0, 3).join("\n      ")); }
}
(async () => {
  await check("a person restores their own cancel, unaware Etsy had cancelled it meanwhile: Etsy's cancel is still recorded", async () => {
    // Kim cancels the order in the sorter; the buyer cancels on Etsy; Kim, who never saw Etsy's word, restores her cancel
    // before the mirror's next run. The mirror then sees Etsy's cancel (changed before the restore).
    const id = "4300000200", tEtsy = Math.floor(Date.now() / 1000) - 5;
    await post({ op: "cancelPut", orderId: id, by: "Kim", why: "wrong order", record: {} });
    assert.equal((await post({ op: "cancelRestore", orderId: id, by: "Kim" })).body.ok, true); assert(!cancelDoc(id));
    await runMirror([[receipt(id, "Canceled", { updated_timestamp: tEtsy })]]);
    assert(cancelDoc(id), "Etsy cancelled it: the order must be cancelled (a restore of a person's own cancel says nothing of Etsy's)");
    assert.equal(cancelDoc(id).by, "Etsy");
    // the sweep and the backlog read the same rule
    const id2 = "4300000201";
    await post({ op: "cancelPut", orderId: id2, by: "Kim", record: {} }); await post({ op: "cancelRestore", orderId: id2, by: "Kim" });
    store.set("EtsyMail_Receipts/" + id2, { receipt_id: id2, status: "Fully Refunded", is_shipped: false, updated_timestamp: tEtsy, raw: { receipt_id: id2, status: "Fully Refunded" } });
    await OrderCancel.keepBacklog(db, [id2]); await OrderCancel.retryBacklog(db, fakeAdmin.firestore.FieldValue);
    assert(cancelDoc(id2), "the backlog records Etsy's cancel too");
    // an Etsy cancel a person restored knowing it (its record said Etsy) still stands restored
    await post({ op: "cancelRestore", orderId: id2, by: "Paul" });
    await runMirror([[receipt(id2, "Fully Refunded", { updated_timestamp: Math.floor(Date.now() / 1000) + 30 })]]);
    assert(!cancelDoc(id2), "restored knowing Etsy's word: left be");
  });

  await check("the same restore landing between the mirror's read and its batch: Etsy's cancel is recorded too", async () => {
    const id = "4300000210";
    await post({ op: "cancelPut", orderId: id, by: "Kim", why: "wrong order", record: {} });
    afterGetAll = () => post({ op: "cancelRestore", orderId: id, by: "Kim" });
    await runMirror([[receipt(id, "Canceled", { updated_timestamp: Math.floor(Date.now() / 1000) - 5 }), receipt("4300000211", "Canceled")]]);
    assert(cancelDoc("4300000211"));
    assert(cancelDoc(id) && cancelDoc(id).by === "Etsy", "Etsy cancelled it: " + JSON.stringify(cancelDoc(id)));
    assert.equal(events(id).filter(e => e.type === "etsyCancelled").length, 1);
  });

  await check("sweep dryRun writes nothing", async () => {
    for (let i = 0; i < 5; i++) { const id = String(4300000000 + i); store.set("EtsyMail_Receipts/" + id, { receipt_id: id, status: "Canceled", is_shipped: false, updated_timestamp: T0, raw: { receipt_id: id, status: "Canceled" } }); }
    const before = store.size;
    const r = await post({ op: "cancelSweep", dryRun: true });
    assert.equal(r.status, 200, JSON.stringify(r.body)); assert.equal(r.body.created, 5); assert.equal(store.size, before);
  });
  await check("sweep via GET with a string cursor", async () => {
    const r = await lib.handler({ httpMethod: "GET", headers: {}, queryStringParameters: { op: "cancelSweep", cursor: JSON.stringify({ s: 0, after: "4300000002" }) } });
    const b = JSON.parse(r.body); assert.equal(r.statusCode, 200, r.body); assert.equal(b.created, 2, r.body);
  });
  await check("restore an Etsy Fully Refunded record, then the sweep and the backlog: stays restored", async () => {
    const id = "4300000100"; store.set("EtsyMail_Receipts/" + id, { receipt_id: id, status: "Fully Refunded", is_shipped: false, updated_timestamp: T0, raw: { receipt_id: id, status: "Fully Refunded" } });
    await post({ op: "cancelSweep" }); assert(cancelDoc(id));
    await post({ op: "cancelRestore", orderId: id, by: "Paul" }); assert(!cancelDoc(id));
    await post({ op: "cancelSweep" }); assert(!cancelDoc(id), "sweep brought it back");
    await OrderCancel.keepBacklog(db, [id]); await OrderCancel.retryBacklog(db, fakeAdmin.firestore.FieldValue); assert(!cancelDoc(id), "backlog brought it back");
  });
  await check("a page's hook error (not a timeout) sends only that page to the backlog", async () => {
    store.delete("Charm_Nest_Cancelled_Backlog/pending");
    const realGetAll = db.getAll; let k = 0;
    db.getAll = async (...a) => { if (a.some(r => r && r.coll === "Charm_Nest_Cancelled") && k++ === 0) throw new Error("UNAVAILABLE"); return realGetAll.apply(db, a); };
    try {
      const full = base => Array.from({ length: 100 }, (_, i) => receipt(String(base + i), i === 0 ? "Canceled" : "Paid", { updated_timestamp: T0 + 2000 }));
      await runMirror([full(4310000000), [receipt("4310000100", "Canceled", { updated_timestamp: T0 + 2000 })]]);
    } finally { db.getAll = realGetAll; }
    const bl = store.get("Charm_Nest_Cancelled_Backlog/pending"); assert(bl, "backlog kept"); assert.deepEqual(bl.ids, ["4310000000"]);
    assert(cancelDoc("4310000100"));
  });
  console.warn = realWarn; console.log = realLog; Date.now = realNow;
  console.log(`adv-a3-cancel: ${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
})().catch(e => { console.warn = realWarn; console.log = realLog; console.error(e); process.exit(1); });
