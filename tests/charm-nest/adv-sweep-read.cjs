// Adversarial wave 4, item 3: op cancelSweep when a whole page read fails (the mirror's receipts page, or the cancel
// records that page needs). The call must answer 200 with what it did so far and a resume point (not 500 with none),
// try the page again only a bounded number of times, and a later call from that point must miss nothing.
// The real charmNestLibrary op against an in-memory Firestore (as adv-a3-cancel.cjs). No network, no real services.
//   node tests/charm-nest/adv-sweep-read.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const root = path.join(__dirname, "../.."), fnDir = path.join(root, "netlify/functions");

/* ── in-memory Firestore: batches, getAll, where ==/in, orderBy/startAfter/limit, select; faults to inject ── */
const store = new Map(), SERVER_TS = { __ts: true };
const clone = v => JSON.parse(JSON.stringify(v, (k, x) => (x === SERVER_TS ? "__TS__" : x)), (k, x) => (x === "__TS__" ? Date.now() : x));
let failGet = null, failGetAll = null, reads = { receipts: 0, records: 0 };
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
      if (coll === "EtsyMail_Receipts") reads.receipts++;
      if (failGet && failGet(coll, after)) throw new Error("DEADLINE_EXCEEDED: page read");
      let rows = [...store.keys()].filter(k => k.startsWith(coll + "/")).map(k => docRef(coll, k.slice(coll.length + 1))._snap());
      for (const [f, op, v] of filters) {
        if (op === "in") assert(Array.isArray(v) && v.length >= 1 && v.length <= 30, "in takes 1 to 30 values");
        rows = rows.filter(r => { const x = r.data()[f]; return op === "==" ? x === v : op === "in" ? v.includes(x) : true; });
      }
      const val = (r, f) => (f === "__name__" ? r.id : r.data()[f]);
      if (order) rows.sort((a, b) => { const x = val(a, order[0]), y = val(b, order[0]); const c = x > y ? 1 : x < y ? -1 : 0; return order[1] === "desc" ? -c : c; });
      if (after != null) rows = rows.filter(r => val(r, order[0]) > after);
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
        assert(ops.length <= 500, "a batch holds at most 500 writes");
        for (const [k, r] of ops) { if (k === "create" && store.has(r.path)) throw new Error("ALREADY_EXISTS: " + r.path); if (k === "update" && !store.has(r.path)) throw new Error("NOT_FOUND: " + r.path); }
        for (const [k, r, d, o] of ops) await r[k](d, o);
      } };
  },
  async getAll(...args) {
    const refs = args.filter(r => r && r._snap);
    if (refs.some(r => r.coll === "Charm_Nest_Cancelled")) { reads.records++; if (failGetAll && failGetAll()) throw new Error("UNAVAILABLE: records read"); }
    return refs.map(r => r._snap());
  },
  async runTransaction(fn) { return fn({ get: r => r.get(), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), create: (r, d) => r.create(d), delete: r => r.delete() }); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, increment: n => n, delete: () => undefined, arrayUnion: (...a) => a }, Timestamp: { fromMillis: ms => ({ toMillis: () => ms }) }, FieldPath: { documentId: () => "__name__" } }),
  storage: () => ({ bucket: () => ({ name: "test", file: () => ({ exists: async () => [false] }) }) }) };

let etsyCalls = 0;
const Module = require("module"), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === "firebase-admin" || /[\\/]firebaseAdmin(\.js)?$/.test(req) || req === "./firebaseAdmin") return fakeAdmin;
  if (req === "node-fetch") return async () => { etsyCalls++; throw new Error("no network in this test"); };
  return realLoad.call(this, req, ...rest);
};
delete process.env.EDIT_PASSCODE;
const lib = require(path.join(fnDir, "charmNestLibrary.js"));
const post = async body => { const r = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body || "{}") }; };
const realLog = console.log; console.warn = () => {}; console.log = () => {}; console.error = () => {};

const T0 = Math.floor(Date.now() / 1000) - 3600;
function seed(from, count, status = "Canceled") {
  for (let i = 0; i < count; i++) {
    const id = String(from + i);
    store.set("EtsyMail_Receipts/" + id, { receipt_id: id, status, is_shipped: false, updated_timestamp: T0, created_timestamp: T0 - 86400, buyer_name: "Buyer " + id,
      raw: { receipt_id: id, status, is_shipped: false, name: "Buyer " + id, transactions: [{ transaction_id: 1, sku: "GF-HEART", title: "Heart", quantity: 1 }] } });
  }
}
const records = () => [...store.keys()].filter(k => k.startsWith("Charm_Nest_Cancelled/")).length;
const reset = () => { store.clear(); failGet = failGetAll = null; reads = { receipts: 0, records: 0 }; };
async function finish(cursor) {   // go on from `cursor` until the sweep says there is no more
  let r, calls = 0, created = 0;
  do { r = await post(Object.assign({ op: "cancelSweep" }, cursor ? { cursor } : {})); calls++; assert.equal(r.status, 200); created += r.body.created; cursor = r.body.next; } while (r.body.more && calls < 20);
  assert.equal(r.body.more, false); return created;
}

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; realLog("ok    " + name); }
  catch (e) { failed++; realLog("FAIL  " + name + "\n      " + String(e && e.stack || e).split("\n").slice(0, 3).join("\n      ")); }
}
(async () => {
  await check("the second receipts page will not read: 200 with the first page's counts and a resume point, nothing missed after", async () => {
    reset(); seed(4400000000, 450);
    failGet = (coll, after) => coll === "EtsyMail_Receipts" && !!after;   // every page after the first fails
    const r = await post({ op: "cancelSweep" });
    assert.equal(r.status, 200, "a page that will not read answers calmly, not 500: " + JSON.stringify(r.body));
    assert.equal(r.body.created, 200, "the first page's work is reported");
    assert.equal(r.body.more, true); assert.deepEqual(r.body.next, { s: 0, after: "4400000199" }, "resumes at the page that failed");
    assert(r.body.readError && /DEADLINE_EXCEEDED/.test(r.body.readError), "says why it stopped");
    assert(reads.receipts <= 4, "bounded retries: 1 good read + at most 3 tries of the failing page, was " + reads.receipts);
    failGet = null;
    const rest = await finish(r.body.next);
    assert.equal(rest, 250, "the next run goes on from there"); assert.equal(records(), 450, "every cancel recorded");
  });

  await check("a read that fails once is tried again within the call: the sweep completes", async () => {
    reset(); seed(4400001000, 50);
    let left = 1; failGet = coll => coll === "EtsyMail_Receipts" && left-- > 0;
    const r = await post({ op: "cancelSweep" });
    assert.equal(r.status, 200, JSON.stringify(r.body));
    assert.equal(r.body.more, false); assert.equal(r.body.created, 50); assert(!r.body.readError);
    assert.equal(records(), 50);
  });

  await check("the page's cancel records will not read: 200, that page is not skipped, and the next run records it", async () => {
    reset(); seed(4400002000, 30);
    failGetAll = () => true;
    const r = await post({ op: "cancelSweep" });
    assert.equal(r.status, 200, JSON.stringify(r.body));
    assert.equal(r.body.created, 0); assert.equal(r.body.more, true);
    assert.deepEqual(r.body.next, { s: 0, after: "" }, "the page is read again next time, not skipped");
    assert(r.body.readError && /UNAVAILABLE/.test(r.body.readError));
    assert(reads.records <= 3, "bounded retries, was " + reads.records);
    assert.equal(records(), 0);
    failGetAll = null;
    assert.equal(await finish(r.body.next), 30); assert.equal(records(), 30);
  });

  await check("a string cursor from a failed run resumes too; zero Etsy calls", async () => {
    reset(); seed(4400003000, 250);
    failGet = (coll, after) => coll === "EtsyMail_Receipts" && !!after;
    const r = await post({ op: "cancelSweep" }); assert.equal(r.status, 200);
    failGet = null;
    assert.equal(await finish(JSON.stringify(r.body.next)), 50); assert.equal(records(), 250);
    assert.equal(etsyCalls, 0);
  });

  realLog(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
