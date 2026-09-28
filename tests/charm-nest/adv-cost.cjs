// Adversarial test (cost and limits): the real charmNestLibrary ops against an in-memory Firestore that, like Firestore,
// answers a query with no orderBy in document-id order. Checks and measures:
//   · cancelList idsOnly past 5000 records keeps the newest (a person's cancel of today), not the oldest
//   · poolUpdate's timeline pre-read: one read per piece, only for a change the timeline records
//   · timelineGet's derivation: how many queries and reads one order costs
// No network, no real services.   node tests/charm-nest/adv-cost.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const root = path.join(__dirname, "../.."), fnDir = path.join(root, "netlify/functions");

const store = new Map(), cost = { queries: 0, getAll: 0, docsRead: 0, writes: 0 };
const SERVER_TS = { __ts: true };
const clone = v => JSON.parse(JSON.stringify(v, (k, x) => (x === SERVER_TS ? "__TS__" : x)), (k, x) => (x === "__TS__" ? Date.now() : x));
function docRef(coll, id) {
  const key = coll + "/" + id;
  const snap = () => { const d = store.get(key); return { exists: !!d, id, ref: docRef(coll, id), data: () => (d ? clone(d) : undefined) }; };
  return { id, path: key, coll, _snap: snap, collection: n => query(key + "/" + n),
    async get() { cost.docsRead++; return snap(); },
    async set(data, opts) { cost.writes++; store.set(key, Object.assign({}, (opts && opts.merge && store.get(key)) || {}, clone(data))); },
    async update(data) { cost.writes++; if (!store.has(key)) throw new Error("NOT_FOUND"); store.set(key, Object.assign({}, store.get(key), clone(data))); },
    async create(data) { cost.writes++; if (store.has(key)) throw new Error("ALREADY_EXISTS"); store.set(key, clone(data)); },
    async delete() { cost.writes++; store.delete(key); } };
}
function query(coll, filters = [], order = null, lim = 0) {
  const q = {
    where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), order, lim),
    orderBy: (f, dir) => query(coll, filters, [f, dir || "asc"], lim),
    limit: n => query(coll, filters, order, n),
    select: () => q, doc: id => docRef(coll, id),
    count: () => ({ get: async () => ({ data: () => ({ count: 0 }) }) }),
    async get() {
      cost.queries++;
      let rows = [...store.keys()].filter(k => k.startsWith(coll + "/") && !k.slice(coll.length + 1).includes("/")).sort().map(k => docRef(coll, k.slice(coll.length + 1))._snap());
      for (const [f, op, v] of filters) {
        if (op === "in" || op === "array-contains-any") assert(Array.isArray(v) && v.length >= 1 && v.length <= 30, op + " takes 1 to 30 values");
        rows = rows.filter(r => { const x = r.data()[f]; return op === "==" ? x === v : op === "in" ? v.includes(x) : op === "array-contains-any" ? Array.isArray(x) && x.some(y => v.includes(y)) : op === ">=" ? x >= v : true; });
      }
      if (order) rows = rows.filter(r => r.data()[order[0]] !== undefined).sort((a, b) => { const x = a.data()[order[0]], y = b.data()[order[0]]; const c = x > y ? 1 : x < y ? -1 : 0; return order[1] === "desc" ? -c : c; });
      if (lim) rows = rows.slice(0, lim);
      cost.docsRead += Math.max(1, rows.length);   // Firestore bills an empty answer as one read
      return { size: rows.length, docs: rows, empty: !rows.length };
    }
  };
  return q;
}
const db = {
  collection: c => query(c),
  batch() { const ops = []; return { set(r, d, o) { ops.push(() => r.set(d, o)); }, update(r, d) { ops.push(() => r.update(d)); }, create(r, d) { ops.push(() => r.create(d)); }, delete(r) { ops.push(() => r.delete()); },
    async commit() { assert(ops.length <= 500, "a batch holds at most 500 writes"); for (const f of ops) await f(); } }; },
  async getAll(...refs) { refs = refs.filter(r => r && r._snap); cost.getAll++; cost.docsRead += refs.length; return refs.map(r => r._snap()); },
  async runTransaction(fn) { return fn({ get: r => r.get(), getAll: (...a) => db.getAll(...a), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), create: (r, d) => r.create(d), delete: r => r.delete() }); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, increment: n => n, delete: () => undefined, arrayUnion: (...a) => a }, Timestamp: { fromMillis: ms => ({ toMillis: () => ms }) }, FieldPath: { documentId: () => "__name__" } }),
  storage: () => ({ bucket: () => ({ name: "test", file: () => ({ exists: async () => [false] }) }) }) };
const Module = require("module"), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === "firebase-admin" || /[\\/]firebaseAdmin(\.js)?$/.test(req) || req === "./firebaseAdmin") return fakeAdmin;
  if (req === "node-fetch") return async () => { throw new Error("no network in this test"); };
  return realLoad.call(this, req, ...rest);
};
delete process.env.EDIT_PASSCODE;
const realLog = console.log; console.log = () => {}; console.warn = () => {};
const lib = require(path.join(fnDir, "charmNestLibrary.js"));
const post = async body => { const r = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body || "{}") }; };
const reset = () => Object.assign(cost, { queries: 0, getAll: 0, docsRead: 0, writes: 0 });
let passed = 0;
async function check(name, fn) { await fn(); passed++; realLog("ok  " + name); }

(async () => {
  await check("cancelList idsOnly past 5000 records still has today's cancel", async () => {
    // 5000 of Etsy's older cancels (the sweep and the mirror fill them in) under lower receipt numbers than today's
    const old = Date.now() - 400 * 86400e3;
    for (let i = 0; i < 5000; i++) { const id = String(3000000000 + i); store.set("Charm_Nest_Cancelled/" + id, { orderId: id, by: "Etsy", source: "etsy", why: "Cancelled on Etsy", at: old + i * 60e3, lines: [], sheets: [] }); }
    const put = await post({ op: "cancelPut", orderId: "4000000001", by: "Paul", why: "buyer asked", record: {} });
    assert.equal(put.status, 200, JSON.stringify(put.body));
    const r = await post({ op: "cancelList", idsOnly: true });
    assert.equal(r.body.ids.length, 5000);
    assert(r.body.ids.includes("4000000001"), "the order a person cancelled today is among the ids the orders check keeps out of the pull");
    for (const k of [...store.keys()]) if (k.startsWith("Charm_Nest_Cancelled/") || k.startsWith("Order_Timeline/")) store.delete(k);
  });

  await check("poolUpdate: a sheet save reads each piece once (a plain patch reads none)", async () => {
    const ids = Array.from({ length: 120 }, (_, i) => `41000000${String(10 + (i % 40)).padStart(2, "0")}_${70000 + i}_1`);
    for (const id of ids) store.set("Charm_Pool/" + id, { poolId: id, orderId: id.split("_")[0], state: "pooled" });
    reset(); await post({ op: "poolUpdate", poolIds: ids, patch: { engrave: true } });
    const plain = { ...cost };
    reset(); const r = await post({ op: "poolUpdate", poolIds: ids, patch: { sheetId: "sheet-A", setId: "set-1", state: "written", sheetName: "GF_Sep.28.26_Set-1_Sheet-1" } });
    assert.equal(r.status, 200);
    assert.equal(plain.docsRead, 0, "a patch the timeline does not record reads nothing");
    assert.equal(cost.docsRead, 120, "one read per piece");
    const placed = [...store.keys()].filter(k => k.startsWith("Order_Timeline/") && k.includes("~placed~")).length;
    assert.equal(placed, 40, "one placed event per order");
    reset(); await post({ op: "poolUpdate", poolIds: ids, patch: { sheetId: "sheet-A", setId: "set-1", state: "written", sheetName: "GF_Sep.28.26_Set-1_Sheet-1" } });
    assert.equal(cost.writes, 120, "the same save again writes the pieces and no event");
    realLog(`    measured: a save of 120 pieces (40 orders) = ${plain.writes} piece writes + 120 reads + 40 events; saved again = 120 reads, 0 events`);
  });

  await check("timelineGet: the derivation's cost for one order", async () => {
    const rid = "4200000001";
    for (let i = 0; i < 20; i++) store.set(`Order_Timeline/${rid}~scan~s${i}`, { orderId: rid, type: "scan", at: Date.now() - i * 60e3, by: "Tess", source: "station" });
    for (let i = 0; i < 3; i++) store.set(`Charm_Pool/${rid}_9${i}_1`, { poolId: `${rid}_9${i}_1`, orderId: rid, transactionId: "9" + i, sheetId: "sheet-B", createdAt: Date.now() - 3600e3 });
    store.set("Charm_Nest_Sheets/sheet-B", { id: "sheet-B", metal: "gold", sheetIndex: 2, orders: [rid], poolIds: [`${rid}_90_1`], setId: "set-2", createdAt: Date.now() - 3000e3 });
    reset(); const r = await post({ op: "timelineGet", orderId: rid });
    assert.equal(r.status, 200); assert(r.body.events.length >= 20);
    assert(cost.queries <= 8 && cost.getAll <= 7, `bounded round trips: ${cost.queries} queries, ${cost.getAll} getAll`);
    realLog(`    measured: timelineGet of an order with 20 events, 3 pieces, 1 sheet = ${cost.queries} queries + ${cost.getAll} getAll, ${cost.docsRead} reads`);
  });

  realLog(`adv-cost: ${passed} passed`);
})().catch(e => { realLog(e); process.exit(1); });
