// Adversarial (wave 4, item 2: what the cancelled orders cost). The orders check read the whole Cancelled collection (up
// to 5000 records) each time, and AutoCancel its newest 50 every 2 minutes, a hidden tab too, and at every orders check.
//   1. cancelList `after`: only the records written since a read, in the order written (createdAt, then id), so a record
//      the sweep writes today for an old Etsy cancel (an old `at`) is read too, and a batch sharing one createdAt pages
//      through with nothing read twice or skipped.
//   2. Cancelled.load: after the first read, only what was written since; a restore made at another screen still drops out.
//   3. AutoCancel: its next reads are of what was written since; a hidden tab does not read on its timer, and the cancel that
//      came in meanwhile is taken up the moment it shows; a job that has to wait is taken up again at the next read.
// The real server op runs over an in-memory Firestore, the real client modules in a vm. No network, no Etsy.
//   node tests/charm-nest/adv-cancel-reads.cjs
"use strict";
const assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path"), vm = require("node:vm");
const root = path.join(__dirname, "../.."), fnDir = path.join(root, "netlify/functions");

/* ── an in-memory Firestore: orderBy (several, __name__ too), startAfter, select, limit, count; serverTimestamp is the
   commit's clock, one value for a whole batch, as Firestore's is ── */
class Timestamp {
  constructor(s, n) { this.seconds = s; this.nanoseconds = n; }
  toMillis() { return this.seconds * 1000 + Math.floor(this.nanoseconds / 1e6); }
}
const store = new Map(), cost = { docsRead: 0 };
let clock = 1_780_000_000_000;               // ms; each commit takes the next one
const SERVER_TS = { __ts: true };
const stampOf = () => { clock += 3; return new Timestamp(Math.floor(clock / 1000), (clock % 1000) * 1e6 + 123); };
function fill(data, ts, cur) { const o = {}; for (const [k, v] of Object.entries(data)) o[k] = v === SERVER_TS ? ts : v && v.__inc != null ? ((cur && +cur[k]) || 0) + v.__inc : v && typeof v === "object" && !(v instanceof Timestamp) ? JSON.parse(JSON.stringify(v)) : v; return o; }   // (__inc: FieldValue.increment, added to what the merged document holds: FC3b's cancel counter counts)
const cmp = (x, y) => {
  if (x instanceof Timestamp && y instanceof Timestamp) return x.seconds - y.seconds || x.nanoseconds - y.nanoseconds;
  return x > y ? 1 : x < y ? -1 : 0;
};
function docRef(coll, id) {
  const key = coll + "/" + id;
  const snap = fields => { const d = store.get(key); let data = d; if (d && fields) { data = {}; for (const f of fields) if (f in d) data[f] = d[f]; } return { exists: !!d, id, ref: docRef(coll, id), data: () => (data ? Object.assign({}, data) : undefined) }; };
  return { id, path: key, _snap: snap,
    async get() { cost.docsRead++; return snap(); },
    async set(data, o, ts) { const cur = (o && o.merge && store.get(key)) || {}; store.set(key, Object.assign({}, cur, fill(data, ts || stampOf(), cur))); },
    async update(data, ts) { if (!store.has(key)) throw new Error("NOT_FOUND"); store.set(key, Object.assign({}, store.get(key), fill(data, ts || stampOf()))); },
    async create(data, ts) { if (store.has(key)) throw new Error("ALREADY_EXISTS"); store.set(key, fill(data, ts || stampOf())); },
    async delete() { store.delete(key); } };
}
function query(coll, o = { where: [], order: [], after: null, lim: 0, fields: null }) {
  const w = x => query(coll, Object.assign({}, o, x));
  return {
    where: (f, op, v) => w({ where: o.where.concat([[f, op, v]]) }),
    orderBy: (f, dir) => w({ order: o.order.concat([[f, dir || "asc"]]) }),
    startAfter: (...v) => w({ after: v }), limit: n => w({ lim: n }), select: (...f) => w({ fields: f }), doc: id => docRef(coll, id),
    count: () => ({ get: async () => { const n = rowsOf().length; cost.docsRead += Math.max(1, Math.ceil(n / 1000)); return { data: () => ({ count: n }) }; } }),
    async get() { const rows = rowsOf(true); cost.docsRead += Math.max(1, rows.length); return { size: rows.length, docs: rows, empty: !rows.length, readTime: stampOf() }; }
  };
  function rowsOf(page) {
    const val = (r, f) => f === "__name__" ? r.id : store.get(coll + "/" + r.id)[f];
    let rows = [...store.keys()].filter(k => k.startsWith(coll + "/") && !k.slice(coll.length + 1).includes("/")).map(k => docRef(coll, k.slice(coll.length + 1))._snap());
    for (const [f, op, v] of o.where) rows = rows.filter(r => { const x = val(r, f); return op === "==" ? x === v : op === "in" ? v.includes(x) : op === ">" ? cmp(x, v) > 0 : op === ">=" ? cmp(x, v) >= 0 : true; });
    if (!page) return rows;
    for (const [f] of o.order) if (f !== "__name__") rows = rows.filter(r => val(r, f) !== undefined);   // Firestore leaves out a record without the field
    const order = o.order.length ? o.order : [["__name__", "asc"]];
    const by = (a, b) => { for (const [f, dir] of order) { const c = cmp(val(a, f), val(b, f)); if (c) return dir === "desc" ? -c : c; } return 0; };
    rows.sort(by);
    if (o.after) rows = rows.filter(r => { for (const [i, [f, dir]] of order.entries()) { if (i >= o.after.length) return false; const c = cmp(val(r, f), o.after[i]) * (dir === "desc" ? -1 : 1); if (c) return c > 0; } return false; });
    if (o.lim) rows = rows.slice(0, o.lim);
    return rows.map(r => docRef(coll, r.id)._snap(o.fields));
  }
}
const db = {
  collection: c => query(c),
  batch() { const ops = []; return { set(r, d, x) { ops.push(ts => r.set(d, x, ts)); }, update(r, d) { ops.push(ts => r.update(d, ts)); }, create(r, d) { ops.push(ts => r.create(d, ts)); }, delete(r) { ops.push(() => r.delete()); },
    async commit() { const ts = stampOf(); for (const f of ops) await f(ts); } }; },
  async getAll(...refs) { cost.docsRead += refs.length; return refs.map(r => r._snap()); },
  async runTransaction(fn) { const ts = stampOf(); return fn({ get: r => r.get(), getAll: (...a) => db.getAll(...a), set: (r, d, x) => r.set(d, x, ts), update: (r, d) => r.update(d, ts), create: (r, d) => r.create(d, ts), delete: r => r.delete() }); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, increment: n => ({ __inc: n }), delete: () => undefined, arrayUnion: (...a) => a }, Timestamp, FieldPath: { documentId: () => "__name__" } }),
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
const calls = [];
const post = async body => { calls.push(body); const r = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) }); return JSON.parse(r.body || "{}"); };
const COL = "Charm_Nest_Cancelled";
const put = (id, rec, ts) => docRef(COL, id).set(Object.assign({ orderId: id, by: "Etsy", source: "etsy", why: "Cancelled on Etsy", lines: [], sheets: [], createdAt: SERVER_TS }, rec), null, ts);
let passed = 0;
async function check(name, fn) { await fn(); passed++; realLog("ok  " + name); }

/* ── the client modules, as the page runs them ── */
const src = fs.readFileSync(path.join(root, "charm-nest-sheetwin.js"), "utf8");
const cut = (a, b) => { const i = src.indexOf(a), j = src.indexOf(b, i); assert(i >= 0 && j > i, "slice " + a); return src.slice(i, j); };
function page({ held = [] } = {}) {
  const ls = new Map(), timers = [], intervals = [], listeners = {}, said = [];
  let skew = 0; const RealDate = Date;                              // (the page's clock, which a test can move on: the deep read of the cancel counter is due after a minute)
  class PageDate extends RealDate { constructor(...a) { if (a.length) super(...a); else super(RealDate.now() + skew); } static now() { return RealDate.now() + skew; } }
  const document = { hidden: false, addEventListener: (t, f) => (listeners[t] = listeners[t] || []).push(f), querySelector: () => null, querySelectorAll: () => [], getElementById: () => null };
  const window = { Recall: { on: () => true } };   // (every job waits: what is queued is seen, nothing moves)
  const ctx = vm.createContext({
    window, document, console, Date: PageDate, CNListActivity: { key: () => "", state: () => ({}), touch() {} },   // (the Orders tab's list-activity module, which Cancelled.history reads: a stub, this test has no Orders tab)
    localStorage: { getItem: k => (ls.has(k) ? ls.get(k) : null), setItem: (k, v) => ls.set(k, String(v)), removeItem: k => ls.delete(k) },
    navigator: {}, setTimeout: (f, ms) => { timers.push({ f, ms }); return timers.length; }, clearTimeout() {}, setInterval: (f, ms) => { intervals.push({ f, ms }); return intervals.length; },
    api: async (fn, body) => { const r = await post(body); if (r.error) throw new Error(r.error); return r; },
    S: { cloud: { ok: true } }, B: { pool: { rows: new Map() } }, W: { dlg: {} },
    Orders: { rows: () => held.map(rid => ({ order: { receiptId: rid }, poolIds: [], state: "pulled" })), render() {} },
    allSheets: () => [], agent: (o, k, t) => said.push(t), whoAmI: () => "Tess", RunCtl: { renderBanner() {} }, ICON: {}, esc: s => s, h: () => ({})
  });
  vm.runInContext(cut("const Cancelled = window.Cancelled = (() => {", "\n  // the order's record is kept first") + cut("const AutoCancel = window.AutoCancel = (() => {", "\n  /* ── the freed room") +
    "\nwindow.Cancelled = Cancelled; window.AutoCancel = AutoCancel;", ctx);
  const show = hidden => { document.hidden = hidden; for (const f of listeners.visibilitychange || []) f(); };
  return { Cancelled: window.Cancelled, AutoCancel: window.AutoCancel, timers, intervals, said, show, document, later: ms => { skew += ms; } };
}
const wholeReads = from => calls.slice(from).filter(c => c.op === "cancelList" && c.idsOnly && c.after === undefined).length;
const cancelReads = () => calls.filter(c => c.op === "cancelList").length;

(async () => {
  await check("cancelList after: only what was written since, a late record with an old `at` too, a batch paged exactly", async () => {
    for (let i = 0; i < 20; i++) await put(String(3000000000 + i), { at: 1e12 + i });
    const top = await post({ op: "cancelList", idsOnly: true, track: true });
    assert.equal(top.ids.length, 20); assert(top.cursor, "a read that asks for it says where it stopped"); assert.equal(top.total, 20);
    // the sweep writes an Etsy cancel of long ago today; a person cancels an order; the mirror writes 150 in one batch
    await put("3100000001", { at: 5e11 });
    await post({ op: "cancelPut", orderId: "4000000001", by: "Paul", why: "buyer asked", record: {} });
    const b = db.batch(); for (let i = 0; i < 150; i++) b.set(docRef(COL, String(3200000000 + i)), { orderId: String(3200000000 + i), by: "Etsy", source: "etsy", at: 1e12, createdAt: SERVER_TS }); await b.commit();
    const seen = []; let c = top.cursor, pages = 0;
    for (;;) { const r = await post({ op: "cancelList", after: c, limit: 40 }); assert(!r.error, r.error); seen.push(...r.list.map(x => x.orderId)); c = r.cursor; pages++; if (!r.more) break; assert(pages < 10, "the pages end"); }
    assert.equal(seen.length, 152, "each new record once"); assert.equal(new Set(seen).size, 152);
    assert(seen.includes("3100000001") && seen.includes("4000000001"), "the late Etsy record and the person's cancel are read");
    const again = await post({ op: "cancelList", idsOnly: true, after: c });
    assert.deepEqual(again.ids, [], "nothing new: nothing read"); assert.equal(again.total, 172);
    for (const k of [...store.keys()]) store.delete(k);
  });

  await check("Cancelled.load: after the first read, only the records written since (a restore elsewhere still drops out)", async () => {
    for (let i = 0; i < 3000; i++) await put(String(3000000000 + i), { at: 1e12 + i });
    const p = page(); const { Cancelled } = p;
    await Cancelled.load(true); assert(Cancelled.has("3000000007"));
    await put("3100000001", { at: 5e11 });                       // an old Etsy cancel written now (the sweep)
    await post({ op: "cancelPut", orderId: "4000000001", by: "Paul", why: "", record: {} });
    let c0 = calls.length; await Cancelled.load(true);
    assert(Cancelled.has("3100000001") && Cancelled.has("4000000001"), "the new cancels are known");
    assert.equal(wholeReads(c0), 0, "a later read reads what changed, not the whole collection");
    assert.equal((await post({ op: "cancelRestore", orderId: "3000000007", by: "Tess" })).ok, true);   // restored at another screen (it raises the counter with its delete)
    await Cancelled.load(true);
    assert(!Cancelled.has("3000000007"), "an order restored elsewhere is pulled again");
    c0 = calls.length; cost.docsRead = 0; await Cancelled.load(true);
    assert.equal(wholeReads(c0), 0, "and the reads after that are of what changed again"); assert(cost.docsRead <= 5, `(read ${cost.docsRead})`);
    for (const k of [...store.keys()]) store.delete(k);
  });

  await check("AutoCancel: reads what was written since; a hidden tab does not read on its timer and catches up when it shows", async () => {
    for (let i = 0; i < 80; i++) await put(String(3000000000 + i), { at: Date.now() - 86400e3 + i });
    const p = page({ held: ["4000000002"] }); const { AutoCancel } = p;
    AutoCancel.start();
    const first = cancelReads(); await AutoCancel.poll();
    assert.equal(cancelReads() - first, 2, "the first read: the newest records, as before, and the list of cancelled orders the sheets are checked against (Cancelled.load, once)");
    cost.docsRead = 0; const q = await AutoCancel.poll();
    assert(cost.docsRead <= 3, `with nothing new, a read costs next to nothing (read ${cost.docsRead})`);
    assert(q.includes("3000000079"), "a record whose job had to wait is taken up again at the next read");
    // hidden: the 2-minute timer does not read; the cancel of an order held here comes in meanwhile
    p.show(true); const n0 = cancelReads();
    for (const t of p.intervals) t.f(); await new Promise(r => setImmediate(r));
    assert.equal(cancelReads(), n0, "a hidden tab does not read on its timer");
    await put("4000000002", { at: Date.now(), by: "Etsy" });
    p.show(false); await new Promise(r => setTimeout(r, 30)); await AutoCancel.idle();
    assert(cancelReads() > n0, "shown again: read at once");
    assert(p.said.some(t => /4000000002/.test(t) && /held here/.test(t)), "the cancel that came in while hidden is taken up: " + p.said.slice(-2).join(" | "));
  });

  await check("FC3b: the cancel counter (Charm_Nest_Rev/cancel): an idle forced read is ONE read, every writer raises it, a restore and a cancel show at once, an edit by hand shows at the deep read", async () => {
    const OrderCancel = require(path.join(fnDir, "_orderCancel.js")), FV = fakeAdmin.firestore.FieldValue;
    const counter = () => (store.get("Charm_Nest_Rev/cancel") || {}).n;
    for (let i = 0; i < 3000; i++) await put(String(3000000000 + i), { at: 1e12 + i });
    const p = page(); const { Cancelled } = p;
    const idle = async () => { const c0 = calls.length; cost.docsRead = 0; await Cancelled.load(true); return { reads: cost.docsRead, ask: calls.slice(c0).filter(c => c.op === "cancelList") }; };
    await Cancelled.load(true); assert(Cancelled.has("3000000007"));
    assert.equal(counter(), undefined, "no writer of the new code has run yet: the counter does not exist (it reads as 0)");
    let r = await idle();
    assert.equal(r.reads, 1, "nothing written: the whole forced read is one document, the counter (before: a query and a count())"); assert.equal(r.ask.length, 1); assert.equal(r.ask[0].ifGen, 0);
    for (let i = 0; i < 4; i++) assert.equal((await idle()).reads, 1);
    // 1 · a person's cancel (op cancelPut: a transaction that raises the counter with the record)
    await post({ op: "cancelPut", orderId: "4100000001", by: "Paul", why: "buyer asked", record: {} });
    assert.equal(counter(), 1, "raised in the same transaction as the record");
    r = await idle(); assert(Cancelled.has("4100000001"), "a cancel made anywhere is known at the very next read"); assert(r.ask[0].after && !r.ask.some(c => c.track), "the records written since were read, not the whole set"); assert(r.reads <= 210, "(the changed read, plus the 200 newest records the Orders tab list takes in when a cancel arrives: " + r.reads + ")");
    assert.equal((await idle()).reads, 1, "and cheap again");
    // 2 · the same cancel again, written anew by the person: still a record write, raised again
    await post({ op: "cancelPut", orderId: "4100000001", by: "Paul", why: "again", record: {} }); assert.equal(counter(), 2);
    // 3 · Etsy's cancels as the receipts mirror and the sweep write them: one batch for a whole page, one raise for the batch
    const rcpt = i => ({ receipt_id: 5100000000 + i, status: "Canceled", updated_timestamp: 1790000000 + i, created_timestamp: 1789000000, buyer_name: "B" + i, transactions: [{ transaction_id: 9000 + i, sku: "S", title: "T", quantity: 1 }] });
    const out = await OrderCancel.fromReceipts(db, FV, [rcpt(1), rcpt(2), rcpt(3)]); assert.equal(out.created, 3, JSON.stringify(out)); assert.equal(counter(), 3, "one raise for the whole batch, in it");
    r = await idle(); assert(Cancelled.has("5100000001") && Cancelled.has("5100000003"), "Etsy's cancels (written outside the handler) are known at the next read");
    const again = await OrderCancel.fromReceipts(db, FV, [rcpt(1)]); assert.equal(again.created, 0); assert.equal(counter(), 3, "a receipt already recorded writes nothing and raises nothing");
    assert.equal((await idle()).reads, 1);
    // 4 · a restore at another screen (op cancelRestore: the delete and the counter in one batch): the page drops it at the next read
    assert.equal((await post({ op: "cancelRestore", orderId: "5100000002", by: "Tess" })).ok, true); assert.equal(counter(), 4);
    await idle(); assert(!Cancelled.has("5100000002") && Cancelled.has("5100000001"), "restored: gone from the page's set, the others stay");
    assert.equal((await idle()).reads, 1);
    // 5 · what raises nothing: the fates of an order's pieces (no record appears or goes), a refused cancel
    await post({ op: "cancelFates", orderId: "4100000001", fates: [{ sheet: "GF Sheet 1", fate: "removed", text: "x" }], by: "Paul" });
    await post({ op: "cancelPut", by: "Paul" });
    assert.equal(counter(), 4, "noteFates and a refused cancel leave the counter alone");
    // 6 · an edit by hand that nothing raised the counter for: unseen until the deep read, which is due at the page's next read a minute on
    await docRef(COL, "3000000011").delete();
    r = await idle(); assert.equal(r.reads, 1); assert(Cancelled.has("3000000011"), "not known yet: the counter has not moved");
    p.later(61000); r = await idle();
    assert.equal(r.ask[0].ifGen, undefined, "a minute on the page asks in full (no ifGen)"); assert(!Cancelled.has("3000000011"), "the deep read counts the records and finds the deleted one"); assert(r.reads >= 2, "deep read: " + r.reads);
    assert.equal((await idle()).ask[0].ifGen, counter(), "and the cheap asks go on from the counter it read");
    // 7 · a server that answers no gen (older server, the sandbox, a counter that cannot be read) is read in full every time, as before
    const at0 = { s: 1780000000, n: 0, id: "3000000000" };
    const sb = await post({ op: "cancelList", idsOnly: true, after: at0, limit: 5, ifGen: 0, wantGen: true, sandbox: true });
    assert.equal(sb.unchanged, undefined); assert.equal(sb.gen, undefined); assert.equal([...store.keys()].filter(k => k.startsWith("Sandbox_Charm_Nest_Rev")).length, 0, "the sandbox keeps no counter");
    const old = await post({ op: "cancelList", idsOnly: true, after: at0, limit: 5 });
    assert.equal(old.gen, undefined, "an older page (no wantGen) is answered as before: no extra read, no gen"); assert(Array.isArray(old.ids) && old.total > 0);
    for (const k of [...store.keys()]) store.delete(k);
  });

  realLog(`\n${passed} passed`);
})().catch(e => { realLog("FAIL " + (e && e.stack || e)); process.exitCode = 1; });
