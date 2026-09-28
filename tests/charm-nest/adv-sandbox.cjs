// Adversarial: sandbox versus production isolation across the cancel records, the order timeline and the stations'
// door (Paul's test wave, 28 Sep). The sandbox plays real Etsy order numbers, so a sandbox cancel or a sandbox timeline
// event must never reach the real order's timeline, its Cancelled tab or a station's cancel check, and production must
// never touch a Sandbox_ copy. Drives the real charmNestLibrary ops, firebaseOrders and _orderCancel against an in-memory
// Firestore that records which collection every read and write went to; and the real order-timeline.js outbox in a
// small fake page (a sandbox toggle between two events). No network.
//   node tests/charm-nest/adv-sandbox.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path"), fs = require("node:fs"), vm = require("node:vm");
const root = path.join(__dirname, "../.."), fnDir = path.join(root, "netlify/functions");

/* ── in-memory Firestore that logs the top-level collection of every read and write ── */
const store = new Map(), log = [];
const SERVER_TS = { __ts: true };
const clone = v => JSON.parse(JSON.stringify(v, (k, x) => (x === SERVER_TS ? "__TS__" : x)), (k, x) => (x === "__TS__" ? Date.now() : x));
const topOf = p => p.split("/")[0];
const touch = (kind, coll) => log.push([kind, topOf(coll)]);
function docRef(coll, id) {
  const key = coll + "/" + id;
  const snap = () => { const d = store.get(key); return { exists: !!d, id, ref: docRef(coll, id), data: () => (d ? clone(d) : undefined) }; };
  return { id, path: key, coll, _snap: snap,
    collection: sub => query(key + "/" + sub),
    async get() { touch("read", coll); return snap(); },
    async set(data, opts) { touch("write", coll); store.set(key, Object.assign({}, (opts && opts.merge && store.get(key)) || {}, clone(data))); },
    async update(data) { touch("write", coll); if (!store.has(key)) throw new Error("NOT_FOUND: " + key); store.set(key, Object.assign({}, store.get(key), clone(data))); },
    async create(data) { touch("write", coll); if (store.has(key)) throw new Error("ALREADY_EXISTS: " + key); store.set(key, clone(data)); },
    async delete() { touch("write", coll); store.delete(key); } };
}
const getPath = (o, f) => f.split(".").reduce((x, k) => (x == null ? undefined : x[k]), o);
function query(coll, filters = [], order = null, lim = 0) {
  return {
    where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), order, lim),
    orderBy: (f, dir) => query(coll, filters, [f, dir || "asc"], lim),
    limit: n => query(coll, filters, order, n),
    select: () => query(coll, filters, order, lim),
    doc: id => docRef(coll, id),
    count: () => ({ get: async () => { touch("read", coll); return { data: () => ({ count: [...store.keys()].filter(k => k.startsWith(coll + "/")).length }) }; } }),
    async get() {
      touch("read", coll);
      let rows = [...store.keys()].filter(k => k.startsWith(coll + "/") && !k.slice(coll.length + 1).includes("/")).map(k => docRef(coll, k.slice(coll.length + 1))._snap());
      for (const [f, op, v] of filters) rows = rows.filter(r => { const x = getPath(r.data(), f); return op === "==" ? x === v : op === "in" ? v.includes(x) : op === "array-contains-any" ? Array.isArray(x) && x.some(e => v.includes(e)) : op === "<" ? x < v : true; });
      if (order) rows.sort((a, b) => { const x = getPath(a.data(), order[0]), y = getPath(b.data(), order[0]); const c = x > y ? 1 : x < y ? -1 : 0; return order[1] === "desc" ? -c : c; });
      if (lim) rows = rows.slice(0, lim);
      return { size: rows.length, docs: rows, empty: !rows.length };
    }
  };
}
const db = {
  collection: c => query(c),
  batch() {
    const ops = [];
    return { set(r, d, o) { ops.push(["set", r, d, o]); }, update(r, d) { ops.push(["update", r, d]); }, create(r, d) { ops.push(["create", r, d]); }, delete(r) { ops.push(["delete", r]); },
      async commit() { for (const [k, r, d, o] of ops) await r[k](d, o); } };
  },
  async getAll(...refs) { if (refs.length && typeof refs[refs.length - 1].get !== "function") refs.pop(); for (const r of refs) touch("read", r.coll); return refs.map(r => r._snap()); },
  async runTransaction(fn) { return fn({ get: r => r.get(), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), create: (r, d) => r.create(d), delete: r => r.delete() }); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, increment: n => n, delete: () => undefined }, Timestamp: { fromMillis: ms => ({ toMillis: () => ms }) }, FieldPath: { documentId: () => "__name__" } }),
  storage: () => ({ bucket: () => ({ name: "test", getFiles: async () => [[], null], file: () => ({ exists: async () => [false] }) }) }) };
const Module = require("module"), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === "firebase-admin" || /[\\/]firebaseAdmin(\.js)?$/.test(req) || req === "./firebaseAdmin") return fakeAdmin;
  if (req === "node-fetch") return async () => { throw new Error("no network in this test"); };
  return realLoad.call(this, req, ...rest);
};
delete process.env.EDIT_PASSCODE;
const lib = require(path.join(fnDir, "charmNestLibrary.js"));
const orders = require(path.join(fnDir, "firebaseOrders.js"));
const OrderCancel = require(path.join(fnDir, "_orderCancel.js"));
const post = async body => { const r = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body || "{}") }; };
const station = async (method, qs, body) => { const r = await orders.handler({ httpMethod: method, headers: {}, queryStringParameters: qs, body: body ? JSON.stringify(body) : undefined }); return { status: r.statusCode, body: JSON.parse(r.body || "{}") }; };
const realWarn = console.warn, realLog = console.log; console.warn = () => {}; console.log = () => {};

/** What one call touched: { reads, writes } as sets of top-level collection names. */
async function watch(fn) {
  const from = log.length; const out = await fn();
  const seg = log.slice(from), reads = new Set(seg.filter(x => x[0] === "read").map(x => x[1])), writes = new Set(seg.filter(x => x[0] === "write").map(x => x[1]));
  return { out, reads, writes };
}
// (Charm_Sandbox is the sandbox's own control record: its snapshot and its order stream)
const prod = set => [...set].filter(c => !c.startsWith("Sandbox_") && c !== "Charm_Sandbox");
const sandboxed = set => [...set].filter(c => c.startsWith("Sandbox_"));

let passed = 0;
async function check(name, fn) { await fn(); passed++; realLog("ok  " + name); }

// a real order: in production it is on a sheet and open; the sandbox plays the same number from its snapshot
const RID = "4200000001", RID2 = "4200000002", NOW = Date.now();
store.set(`EtsyMail_Receipts/${RID}`, { receipt_id: RID, status: "Paid", is_shipped: false, buyer_name: "Real Buyer", created_timestamp: Math.floor(NOW / 1000) - 86400, updated_timestamp: Math.floor(NOW / 1000) - 3600,
  raw: { transactions: [{ transaction_id: 91, sku: "GF-HEART", title: "Heart", quantity: 1 }] } });
store.set(`Order_Timeline/${RID}~placed~p1`, { orderId: RID, type: "placed", at: NOW - 3600000, by: "System", source: "sorter", station: "sorter", sheetId: "gold-real", sheet: "GF Sheet 1", text: "", milestone: true });

(async () => {
  // ALLOWED production reads in the sandbox, each by design: the inbox's Etsy mirror (sandboxCancel copies what was
  // ordered from it) and the custom readings (shared, the decision kept per workspace)
  const SHARED_READS = new Set(["EtsyMail_Receipts", "Charm_Nest_CustomRead"]);

  await check("every sandbox cancel and timeline op reads and writes only Sandbox_ copies", async () => {
    const calls = [
      { op: "sandboxCancel", orderId: RID, by: "Tester" },
      { op: "cancelFates", orderId: RID, fates: [{ sheet: "GF Sheet 1", fate: "removed", text: "taken off" }] },
      { op: "cancelList", limit: 50 }, { op: "cancelList", idsOnly: true },
      { op: "cancelCheck", orderIds: [RID] },
      { op: "timelineAdd", events: [{ orderId: RID, type: "removed", at: NOW, by: "Tester", id: "sb-1", text: "Taken off GF Sheet 1 · cancelled on Etsy" }] },
      { op: "timelineGet", orderId: RID },
      { op: "cancelPut", orderId: RID2, by: "Tester", why: "rehearsal" },
      { op: "cancelRestore", orderId: RID2, by: "Tester" }
    ];
    for (const b of calls) {
      const { out, reads, writes } = await watch(() => post(Object.assign({ sandbox: true }, b)));
      assert.equal(out.status, 200, b.op + ": " + JSON.stringify(out.body));
      assert.deepEqual(prod(writes), [], `${b.op} in the sandbox wrote production: ${[...writes]}`);
      assert.deepEqual(prod(reads).filter(c => !SHARED_READS.has(c)), [], `${b.op} in the sandbox read production: ${[...reads]}`);
    }
    // sandboxCancel writes the sandbox whatever the request says
    const { writes } = await watch(() => post({ op: "sandboxCancel", orderId: RID2 }));
    assert.deepEqual(prod(writes), [], "sandboxCancel never writes production");
  });

  await check("the real order shows none of it: its timeline, Cancelled tab and a station's check", async () => {
    assert(store.get(`Sandbox_Charm_Nest_Cancelled/${RID}`), "the sandbox has its cancel");
    const tl = (await post({ op: "timelineGet", orderId: RID })).body;
    assert.equal(tl.cancelled, null, "no cancel record on the real order");
    assert(!tl.events.some(e => /cancel/i.test(e.type) || e.id && e.id.includes("sb-1")), "no sandbox event on the real timeline: " + JSON.stringify(tl.events.map(e => e.type)));
    assert.notEqual(tl.where.stage, "cancelled");
    const list = (await post({ op: "cancelList", limit: 200 })).body.list;
    assert(!list.some(c => c.orderId === RID), "not on the real Cancelled tab");
    assert(!(await post({ op: "cancelList", idsOnly: true })).body.ids.includes(RID), "not left out of the real pull");
    const st = await station("GET", { cancelCheck: RID });
    assert.deepEqual(st.body.cancelled, {}, "a real station's scan raises no alert");
    const sb = await station("GET", { cancelCheck: RID, sandbox: "1" });
    assert.equal(sb.body.cancelled[RID].by, "Etsy", "the sandbox's own check sees it");
  });

  await check("production ops never touch a Sandbox_ copy; cancelSweep refuses the sandbox", async () => {
    const calls = [
      { op: "cancelPut", orderId: RID2, by: "Paul", why: "customer asked" },
      { op: "cancelFates", orderId: RID2, fates: [{ sheet: "GF Sheet 2", fate: "cut", text: "already cut" }] },
      { op: "cancelList", limit: 50 }, { op: "cancelCheck", orderIds: [RID2] },
      { op: "timelineAdd", events: [{ orderId: RID2, type: "note", at: NOW, by: "Paul", id: "p-1", text: "real" }] },
      { op: "timelineGet", orderId: RID2 },
      { op: "cancelRestore", orderId: RID2, by: "Paul" },
      { op: "cancelSweep", dryRun: true }
    ];
    for (const b of calls) {
      const { out, reads, writes } = await watch(() => post(b));
      assert.equal(out.status, 200, b.op + ": " + JSON.stringify(out.body));
      assert.deepEqual(sandboxed(reads).concat(sandboxed(writes)), [], `${b.op} in production touched the sandbox: ${[...reads, ...writes]}`);
    }
    const { out, writes } = await watch(() => post({ op: "cancelSweep", sandbox: true }));
    assert.equal(out.status, 400); assert.equal(writes.size, 0, "the refused sweep writes nothing");
    // the mirror's hook fills production, whatever prefix a caller passes
    const receipts = [{ receipt_id: "4200000009", status: "Canceled", updated_timestamp: Math.floor(NOW / 1000), transactions: [] }];
    const m = await watch(() => OrderCancel.fromReceipts(db, fakeAdmin.firestore.FieldValue, receipts, { prefix: "Sandbox_" }));
    assert.deepEqual(sandboxed(m.writes), [], "the mirror never writes the sandbox"); assert(store.get("Charm_Nest_Cancelled/4200000009"));
  });

  await check("the stations' door: ?sandbox=1 keeps a station's events in the sandbox, and only then", async () => {
    const ev = { orderId: RID, type: "scan", at: NOW, by: "Ann", station: "welding", device: "weld-1", id: "weld-1-x" };
    const a = await watch(() => station("POST", { sandbox: "1" }, { timeline: [ev] }));
    assert.deepEqual([...a.writes], ["Sandbox_Order_Timeline"]);
    const b = await watch(() => station("POST", {}, { timeline: [Object.assign({}, ev, { id: "weld-1-y" })] }));
    assert.deepEqual([...b.writes], ["Order_Timeline"]);
    const c = await watch(() => station("GET", { cancelCheck: RID }));
    assert.deepEqual([...c.reads], ["Charm_Nest_Cancelled"]);
  });

  await check("a sandbox reset clears the sandbox's timeline (a replay of the same order numbers starts clean) and never production's", async () => {
    assert([...store.keys()].some(k => k.startsWith("Sandbox_Order_Timeline/")), "the rehearsal left events");
    const realBefore = [...store.keys()].filter(k => k.startsWith("Order_Timeline/")).length;
    let r; for (let i = 0; i < 20; i++) { r = await watch(() => post({ op: "sandboxReset", sandbox: true })); assert.equal(r.out.status, 200); if (!r.out.body.more) break; }
    assert.deepEqual(prod(r.writes), [], "the reset wrote production: " + [...r.writes]);
    assert.equal([...store.keys()].filter(k => k.startsWith("Order_Timeline/")).length, realBefore, "production's timeline untouched");
    assert(!store.get(`Sandbox_Charm_Nest_Cancelled/${RID}`), "the sandbox cancel went");
    assert.deepEqual([...store.keys()].filter(k => k.startsWith("Sandbox_Order_Timeline/")), [], "the sandbox's timeline events went with the reset");
    // the replay: the same real number in the sandbox reads as a fresh order, not cancelled by the last rehearsal
    const tl = (await post({ op: "timelineGet", orderId: RID, sandbox: true })).body;
    assert.equal(tl.events.length, 0, "nothing from the last rehearsal: " + JSON.stringify(tl.events.map(e => e.type)));
    assert.notEqual(tl.where.stage, "cancelled");
  });

  await check("order-timeline.js: across a sandbox toggle each event goes to the store it was recorded in", async () => {
    const posts = [], ls = new Map();
    const win = { location: { protocol: "https:" }, addEventListener() {}, localStorage: { getItem: k => (ls.has(k) ? ls.get(k) : null), setItem: (k, v) => ls.set(k, String(v)) } };
    const ctx = vm.createContext({ window: win, localStorage: win.localStorage, location: win.location, document: { addEventListener() {}, visibilityState: "visible" }, console, JSON, Math, Date, Promise, String, Number, Set, Array, Object, Error,
      setTimeout: () => 0, clearTimeout() {}, encodeURIComponent,
      fetch: async (url, o) => { posts.push({ url, body: o && o.body ? JSON.parse(o.body) : null }); return { ok: true, status: 200, json: async () => ({ ok: true }) }; } });
    vm.runInContext(fs.readFileSync(path.join(root, "order-timeline.js"), "utf8"), ctx);
    const T = win.OrderTimeline;
    // the sorter: a sandbox event, the switch, a production event, then the outbox is sent
    T.config({ mode: "sorter", sandbox: true }); T.record({ orderId: RID, type: "note", text: "sandbox" });
    T.config({ mode: "sorter", sandbox: false }); T.record({ orderId: RID, type: "note", text: "real" });
    await T.flush(); await T.flush();
    assert.equal(posts.length, 2);
    assert.equal(posts[0].body.sandbox, true); assert.deepEqual(posts[0].body.events.map(e => e.text), ["sandbox"]);
    assert.equal(posts[1].body.sandbox, false); assert.deepEqual(posts[1].body.events.map(e => e.text), ["real"]);
    // a station: the door's ?sandbox=1 only for a sandbox event
    posts.length = 0;
    T.config({ mode: "station", sandbox: true }); T.record({ orderId: RID, type: "scan" });
    T.config({ mode: "station", sandbox: false }); T.record({ orderId: RID, type: "scan" });
    await T.flush(); await T.flush();
    assert.deepEqual(posts.map(p => /\?sandbox=1$/.test(p.url)), [true, false]);
    // pending events of the other store are not shown on this store's timeline
    T.config({ mode: "sorter", sandbox: true }); T.record({ orderId: RID, type: "note", text: "sandbox pending" });
    T.config({ mode: "sorter", sandbox: false });
    ctx.fetch = async () => ({ ok: true, status: 200, json: async () => ({ events: [] }) });
    const got = await T.get(RID);
    assert(!got.events.some(e => e.text === "sandbox pending"), "a sandbox event still on its way is not on the real order's timeline");
  });

  console.warn = realWarn; console.log = realLog;
  console.log(`adv-sandbox: ${passed} checks passed`);
})().catch(e => { console.warn = realWarn; console.log = realLog; console.error(e); process.exit(1); });
