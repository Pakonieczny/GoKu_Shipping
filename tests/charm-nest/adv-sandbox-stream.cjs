// Adversarial (wave 3, area 14): the sandbox stream's reset against the shared custom readings and the real clock.
// Charm_Nest_CustomRead is shared by production and the sandbox: the readings Claude was paid for stay through a reset,
// and so does production's own decision (decided); the sandbox's decision (decidedSandbox) is the rehearsal's and goes
// with it, or the replay of the same real order number shows "Decided: …" on its timeline and the sorter meets the line
// already decided. A sandbox arrival is stamped on the real clock (simAt beside it), never in production, and a replay
// after the reset is a fresh arrival. Drives the real charmNestLibrary ops against an in-memory Firestore. No network.
//   node tests/charm-nest/adv-sandbox-stream.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const root = path.join(__dirname, "../.."), fnDir = path.join(root, "netlify/functions");

/* ── in-memory Firestore (FieldValue.delete honoured) that logs the top-level collection of every write ── */
const store = new Map(), log = [];
const SERVER_TS = { __ts: true }, DELETE = { __delete: true };
const clone = v => JSON.parse(JSON.stringify(v, (k, x) => (x === SERVER_TS ? "__TS__" : x === DELETE ? "__DEL__" : x)), (k, x) => (x === "__TS__" ? Date.now() : x));
const merge = (old, patch) => { const out = Object.assign({}, old || {}); for (const [k, v] of Object.entries(patch)) { if (v === "__DEL__") delete out[k]; else out[k] = v; } return out; };
const topOf = p => p.split("/")[0];
function docRef(coll, id) {
  const key = coll + "/" + id;
  const snap = () => { const d = store.get(key); return { exists: !!d, id, ref: docRef(coll, id), data: () => (d ? JSON.parse(JSON.stringify(d)) : undefined) }; };
  return { id, path: key, coll, _snap: snap,
    collection: sub => query(key + "/" + sub),
    async get() { return snap(); },
    async set(data, opts) { log.push(topOf(coll)); store.set(key, merge(opts && opts.merge ? store.get(key) : {}, clone(data))); },
    async update(data) { log.push(topOf(coll)); if (!store.has(key)) throw new Error("NOT_FOUND: " + key); store.set(key, merge(store.get(key), clone(data))); },
    async create(data) { log.push(topOf(coll)); if (store.has(key)) throw new Error("ALREADY_EXISTS: " + key); store.set(key, merge({}, clone(data))); },
    async delete() { log.push(topOf(coll)); store.delete(key); } };
}
const getPath = (o, f) => f.split(".").reduce((x, k) => (x == null ? undefined : x[k]), o);
function query(coll, filters = [], order = null, lim = 0) {
  return {
    where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), order, lim),
    orderBy: (f, dir) => query(coll, filters, [f, dir || "asc"], lim),
    limit: n => query(coll, filters, order, n),
    select: () => query(coll, filters, order, lim),
    doc: id => docRef(coll, id),
    count: () => ({ get: async () => ({ data: () => ({ count: [...store.keys()].filter(k => k.startsWith(coll + "/")).length }) }) }),
    async get() {
      let rows = [...store.keys()].filter(k => k.startsWith(coll + "/") && !k.slice(coll.length + 1).includes("/")).map(k => docRef(coll, k.slice(coll.length + 1))._snap());
      for (const [f, op, v] of filters) rows = rows.filter(r => { const x = getPath(r.data(), f); return op === "==" ? x === v : op === "in" ? v.includes(x) : op === "array-contains-any" ? Array.isArray(x) && x.some(e => v.includes(e)) : op === "<" ? x < v : op === ">=" ? x >= v : true; });
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
  async getAll(...refs) { if (refs.length && typeof refs[refs.length - 1].get !== "function") refs.pop(); return refs.map(r => r._snap()); },
  async runTransaction(fn) { return fn({ get: r => r.get(), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), create: (r, d) => r.create(d), delete: r => r.delete() }); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, increment: n => n, delete: () => DELETE }, Timestamp: { fromMillis: ms => ({ toMillis: () => ms }) }, FieldPath: { documentId: () => "__name__" } }),
  storage: () => ({ bucket: () => ({ name: "test", getFiles: async () => [[], null], file: () => ({ exists: async () => [false] }) }) }) };
const Module = require("module"), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === "firebase-admin" || /[\\/]firebaseAdmin(\.js)?$/.test(req) || req === "./firebaseAdmin") return fakeAdmin;
  if (req === "node-fetch") return async () => { throw new Error("no network in this test"); };
  return realLoad.call(this, req, ...rest);
};
delete process.env.EDIT_PASSCODE;
const lib = require(path.join(fnDir, "charmNestLibrary.js"));
const post = async body => { const r = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body || "{}") }; };
const realWarn = console.warn, realLog = console.log; console.warn = () => {}; console.log = () => {};

let passed = 0;
async function check(name, fn) { await fn(); passed++; realLog("ok  " + name); }
const resetAll = async () => { let r, writes = new Set(); for (let i = 0; i < 20; i++) { const from = log.length; r = await post({ op: "sandboxReset", sandbox: true }); log.slice(from).forEach(c => writes.add(c)); assert.equal(r.status, 200, JSON.stringify(r.body)); if (!r.body.more) break; } return { r, writes }; };

// a real order whose line Claude read once (shared), and which Paul decided in production
const RID = "4200000101", KEY = `${RID}_77`, NOW = Date.now();
store.set(`Charm_Nest_CustomRead/${KEY}`, { order: RID, latest: "h1", reads: { h1: { kind: "custom", confidence: 0.91, summary: "a drawing to cut", at: NOW - 86400000 } }, decided: { kind: "regular", by: "Paul", at: NOW - 3600000 } });

(async () => {
  await check("a sandbox decision on a shared reading is the sandbox's own: production's decision and timeline never see it", async () => {
    const d = await post({ op: "customDecide", sandbox: true, key: KEY, kind: "custom", by: "Tester" });
    assert.equal(d.status, 200, JSON.stringify(d.body));
    const doc = store.get(`Charm_Nest_CustomRead/${KEY}`);
    assert.equal(doc.decidedSandbox.kind, "custom"); assert.equal(doc.decided.kind, "regular", "production's decision stands");
    const sb = (await post({ op: "timelineGet", sandbox: true, orderId: RID })).body;
    assert(sb.events.some(e => e.type === "customDecided" && /custom/.test(e.text)), "the rehearsal's decision is on the sandbox timeline: " + JSON.stringify(sb.events.map(e => e.type + ":" + e.text)));
    const pr = (await post({ op: "timelineGet", orderId: RID })).body;
    assert(!pr.events.some(e => e.type === "customDecided" && /custom/.test(e.text)), "never on the real order's");
    assert.equal((await post({ op: "customReadGet", items: [{ key: KEY, hash: "h1" }] })).body.decided[KEY].kind, "regular");
  });

  await check("a sandbox arrival is stamped on the real clock (the stream's moment kept as simAt), in the sandbox only", async () => {
    const sim = NOW + 5 * 86400000, from = log.length;
    const r = await post({ op: "arrivalRecord", sandbox: true, now: sim, orders: [{ id: RID, createTs: NOW - 2 * 86400000 }] });
    assert.equal(r.status, 200, JSON.stringify(r.body));
    assert.deepEqual([...new Set(log.slice(from))].filter(c => !c.startsWith("Sandbox_")), [], "the sandbox arrival wrote production");
    const ev = [...store.entries()].find(([k, v]) => k.startsWith("Sandbox_Order_Timeline/") && v.type === "arrived")[1];
    assert(Math.abs(ev.at - Date.now()) < 60000, "the arrival is on the real clock"); assert.equal(ev.data.simAt, sim); assert.equal(ev.data.clock, "real");
    assert(!store.has(`Charm_Nest_Arrivals/${RID}`) && ![...store.keys()].some(k => k.startsWith("Order_Timeline/")), "production's ledger and timeline untouched");
  });

  await check("the reset clears the sandbox's decisions on the shared readings, and keeps the readings and production's decisions", async () => {
    const before = JSON.parse(JSON.stringify(store.get(`Charm_Nest_CustomRead/${KEY}`)));
    const { writes } = await resetAll();
    assert.deepEqual([...writes].filter(c => !c.startsWith("Sandbox_") && c !== "Charm_Sandbox" && c !== "Charm_Nest_CustomRead"), [], "the reset wrote production: " + [...writes]);
    const after = store.get(`Charm_Nest_CustomRead/${KEY}`);
    assert(after, "the shared reading is kept");
    assert.deepEqual(after.reads, before.reads, "the reading Claude was paid for is kept");
    assert.deepEqual(after.decided, before.decided, "production's decision is kept");
    assert.equal(after.decidedSandbox, undefined, "the rehearsal's decision went with the reset: " + JSON.stringify(after.decidedSandbox));
    // the replay: the same line is met undecided, and its sandbox timeline carries nothing the last rehearsal decided
    assert.deepEqual((await post({ op: "customReadGet", sandbox: true, items: [{ key: KEY, hash: "h1" }] })).body.decided, {}, "the replay meets the line undecided");
    const sb = (await post({ op: "timelineGet", sandbox: true, orderId: RID })).body;
    assert(!sb.events.some(e => e.type === "customDecided"), "no decision of the last rehearsal on the replay's timeline: " + JSON.stringify(sb.events.map(e => e.type + ":" + e.text)));
    assert(!sb.events.some(e => e.type === "arrived"), "the last rehearsal's arrival went too");
    // production still reads its own
    const pr = (await post({ op: "timelineGet", orderId: RID })).body;
    assert(pr.events.some(e => e.type === "customDecided" && /regular/.test(e.text)), "production's decision is still on the real timeline");
    assert.equal((await post({ op: "customReadGet", items: [{ key: KEY, hash: "h1" }] })).body.decided[KEY].kind, "regular");
    assert.equal((await post({ op: "customReadGet", sandbox: true, items: [{ key: KEY, hash: "h1" }] })).body.reads[KEY].kind, "custom", "the sandbox still reads the shared reading");
  });

  await check("after the reset the replay's arrival is new again: one arrival on the real clock", async () => {
    const r = await post({ op: "arrivalRecord", sandbox: true, now: NOW + 9 * 86400000, orders: [{ id: RID, createTs: NOW - 2 * 86400000 }] });
    assert.equal(r.status, 200);
    const sb = (await post({ op: "timelineGet", sandbox: true, orderId: RID })).body;
    assert.equal(sb.events.filter(e => e.type === "arrived").length, 1, "one arrival: " + JSON.stringify(sb.events.map(e => e.type)));
  });

  console.warn = realWarn; console.log = realLog;
  console.log(`adv-sandbox-stream: ${passed} checks passed`);
})().catch(e => { console.warn = realWarn; console.log = realLog; console.error(e); process.exit(1); });
