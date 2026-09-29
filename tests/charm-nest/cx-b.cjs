// cx-b (29 Sep): a cancelled order's history and removals kept for good: the record's own removals list (who, where,
// when, outcome), a second press and a restore losing nothing (Charm_Nest_Cancelled_History), the order view's full read,
// the mirror's backlog bound and the timeline outbox's bound. The harness is adv-etsy-cancel.cjs's in-memory Firestore.
// No network, no real services.
//   node tests/charm-nest/cx-b.cjs
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


/* ── cx-b (29 Sep, part B): a cancelled order's history and removals are kept for good. The record keeps its own list
   of what was removed from where and how it went; a second press, a restore and the backlog lose nothing; the order
   view's read (cancelCheck full) gets the whole record. In-memory Firestore only: no network. ── */
(async () => {
  const RID = "4100000001", PID = `${RID}_7100000001_1`, T = realNow() - 3 * 86400000;
  await check("a cancel's take-off is a timed removal on its record (who, where, outcome)", async () => {
    await OrderCancel.put(db, fakeAdmin.firestore.FieldValue, OrderCancel.fromReceipt(receipt(RID, "Canceled")), { detectedBy: "mirror" });
    store.set("Charm_Pool/" + PID, { poolId: PID, orderId: RID, sheetId: "sh1", sheetName: "GF_Sep.16.26_Set-3_Sheet-2", lineKey: `${RID}_7100000001` });
    const r = await post({ op: "poolUpdate", poolIds: [PID], patch: { removedAt: T, removedBy: "Ann", removedReason: "cancelled" }, by: "Ann" });
    assert.equal(r.status, 200, JSON.stringify(r.body));
    const rem = cancelDoc(RID).removals || [];
    assert.equal(rem.length, 1, JSON.stringify(rem));
    assert.equal(rem[0].at, T); assert.equal(rem[0].by, "Ann"); assert.equal(rem[0].outcome, "removed"); assert.match(rem[0].where, /GF Sheet 2/);
    // the same take-off sent twice lands on the same entry
    await post({ op: "poolUpdate", poolIds: [PID], patch: { removedAt: T, removedBy: "Ann", removedReason: "cancelled" }, by: "Ann" });
    assert.equal(cancelDoc(RID).removals.length, 1);
  });
  await check("a piece still waiting turns removed, its waiting kept (was); never back", async () => {
    await post({ op: "cancelFates", orderId: RID, fates: [{ sheet: "SS Sheet 3", fate: "open", text: "on SS Sheet 3, not cut yet" }] });
    let w = cancelDoc(RID).removals.find(x => x.where === "SS Sheet 3"); assert.equal(w.outcome, "waiting");
    await post({ op: "cancelFates", orderId: RID, fates: [{ sheet: "SS Sheet 3", fate: "removed", text: "taken off SS Sheet 3" }] });
    w = cancelDoc(RID).removals.find(x => x.where === "SS Sheet 3"); assert.equal(w.outcome, "removed"); assert.equal(w.was.outcome, "waiting");
    await post({ op: "cancelFates", orderId: RID, fates: [{ sheet: "SS Sheet 3", fate: "open" }] });
    assert.equal(cancelDoc(RID).removals.find(x => x.where === "SS Sheet 3").outcome, "removed");
    await post({ op: "cancelFates", orderId: RID, removals: [{ id: "queue", where: "the queue", kind: "queue", outcome: "removed", by: "Bo" }, { id: "cut~GF Sheet 9", where: "GF Sheet 9", outcome: "setAside" }] });
    assert.equal(cancelDoc(RID).removals.length, 4);
  });
  await check("the order view's read (cancelCheck full) has the whole record; a station's check stays small", async () => {
    const full = (await post({ op: "cancelCheck", orderIds: [RID], full: true })).body.cancelled[RID];
    assert.equal(full.removals.length, 4); assert.equal(full.lines.length, 1); assert.equal(full.source, "etsy");
    const small = (await post({ op: "cancelCheck", orderIds: [RID] })).body.cancelled[RID];
    assert.equal(small.removals, undefined); assert.equal(small.lines, undefined);
  });
  const RID2 = "4100000002";
  await check("a person's second press keeps the removals; a restore keeps the whole record for good", async () => {
    await post({ op: "cancelPut", orderId: RID2, by: "Cy", why: "buyer asked" });
    await post({ op: "cancelFates", orderId: RID2, fates: [{ sheet: "GF Sheet 1", fate: "removed" }, { sheet: "GF Sheet 4", fate: "cut" }] });
    await post({ op: "cancelPut", orderId: RID2, by: "Cy", why: "buyer asked again" });
    assert.equal(cancelDoc(RID2).removals.length, 2); assert.equal(cancelDoc(RID2).why, "buyer asked again");
    const at = cancelDoc(RID2).at;
    const r = await post({ op: "cancelRestore", orderId: RID2, by: "Di" }); assert.equal(r.status, 200, JSON.stringify(r.body));
    assert.equal(cancelDoc(RID2), undefined);
    const h = store.get(`Charm_Nest_Cancelled_History/${RID2}~${at}`);
    assert(h, "the restored record is kept"); assert.equal(h.removals.length, 2); assert.equal(h.restoredBy, "Di"); assert.equal(h.by, "Cy");
    const ev = events(RID2).find(e => e.type === "cancelRestored"); assert(ev && ev.data.cancelled.removals.length === 2, "the restore event carries the removals");
  });
  await check("the sandbox keeps its own history (Sandbox_), production untouched", async () => {
    await post({ op: "cancelPut", orderId: "4100000003", by: "Ed", sandbox: true });
    await post({ op: "cancelRestore", orderId: "4100000003", by: "Ed", sandbox: true });
    assert([...store.keys()].some(k => k.startsWith("Sandbox_Charm_Nest_Cancelled_History/4100000003~")));
    assert(![...store.keys()].some(k => k.startsWith("Charm_Nest_Cancelled_History/4100000003~")));
  });
  await check("the mirror's backlog keeps 2000 ids (was 200); a run retries 200", async () => {
    const ids = Array.from({ length: 900 }, (_, i) => String(4200000000 + i));
    const k = await OrderCancel.keepBacklog(db, ids); assert.equal(k.kept, 900); assert.equal(k.dropped, 0);
    const r = await OrderCancel.retryBacklog(db, fakeAdmin.firestore.FieldValue); assert.equal(r.retried, 200);
    assert.equal(store.get(OrderCancel.BACKLOG + "/pending").ids.length, 700);
  });
  await check("the outbox bound drops other events first, never a cancel's (order-timeline.js)", async () => {
    const vm = require("node:vm"), fs = require("node:fs"), ls = new Map();
    const win = { addEventListener() {} }, ctx = { window: win, document: { addEventListener() {} }, location: { protocol: "https:" }, localStorage: { getItem: k => ls.get(k) || null, setItem: (k, v) => ls.set(k, v) }, setTimeout: () => 0, clearTimeout() {}, console, Blob, JSON, Date, Math };
    vm.runInNewContext(fs.readFileSync(path.join(root, "order-timeline.js"), "utf8"), ctx);
    const OT = win.OrderTimeline; OT.record({ orderId: RID, type: "cancelAlert", id: "ack-1" });
    for (let i = 0; i < 2100; i++) OT.record({ orderId: "4300000000", type: "scan", id: "s" + i });
    const disk = JSON.parse(ls.get("orderTimeline.outbox.v1"));
    assert.equal(disk.length, 500); assert(disk.some(e => e.type === "cancelAlert"), "the cancel alert stays on the disk");
    assert.equal(OT.pending(), 2000);
  });
  console.warn = realWarn; console.log = realLog;
  realLog(`\n${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
})().catch(e => { console.log = realLog; realLog("CRASH", e); process.exit(1); });
