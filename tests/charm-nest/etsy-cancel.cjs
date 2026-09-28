// Etsy's cancels become cancel records (Paul A1 · A6, 28 Sep). Drives the real receipts mirror (etsyMailReceiptsMirrorCron)
// with pages of receipts from a pretend Etsy, and the real charmNestLibrary ops (cancelPut, cancelSweep, sandboxCancel),
// against an in-memory Firestore, and checks: a receipt Etsy cancelled gets one record (by "Etsy") and one etsyCancelled
// event; the same page again writes nothing; a person's record stays theirs and only gains etsyStatus; a page with no
// cancel costs no read; a failing cancel hook never breaks the mirror; an order restored after Etsy's change is left be;
// the sweep fills older ones idempotently; cancelPut keeps its callers and records source and the person's event; the
// sandbox cancel writes only Sandbox_ records. No network, no real services.
//   node tests/charm-nest/etsy-cancel.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const root = path.join(__dirname, "../.."), fnDir = path.join(root, "netlify/functions");

/* ── in-memory Firestore: atomic batches, transactions, getAll, where ==/in, select; counts what is read and written ── */
const store = new Map(), cost = { getAll: 0, queries: [], commits: 0 };
const SERVER_TS = { __ts: true };
const clone = v => JSON.parse(JSON.stringify(v, (k, x) => (x === SERVER_TS ? "__TS__" : x)), (k, x) => (x === "__TS__" ? Date.now() : x));
let failCancelReads = false, afterGetAll = null;
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
function query(coll, filters = [], order = null, lim = 0) {
  return {
    where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), order, lim),
    orderBy: (f, dir) => query(coll, filters, [f, dir || "asc"], lim),
    limit: n => query(coll, filters, order, n),
    select: () => query(coll, filters, order, lim),
    doc: id => docRef(coll, id),
    async get() {
      cost.queries.push(coll + " " + filters.map(f => f.join(" ")).join(", "));
      let rows = [...store.keys()].filter(k => k.startsWith(coll + "/")).map(k => docRef(coll, k.slice(coll.length + 1))._snap());
      for (const [f, op, v] of filters) {
        if (op === "in") assert(Array.isArray(v) && v.length >= 1 && v.length <= 30, "in takes 1 to 30 values");
        rows = rows.filter(r => { const x = r.data()[f]; return op === "==" ? x === v : op === "in" ? v.includes(x) : true; });
      }
      if (order) rows.sort((a, b) => { const x = a.data()[order[0]], y = b.data()[order[0]]; const c = x > y ? 1 : x < y ? -1 : 0; return order[1] === "desc" ? -c : c; });
      if (lim) rows = rows.slice(0, lim);
      return { size: rows.length, docs: rows, empty: !rows.length };
    }
  };
}
const db = {
  collection: c => query(c),
  doc: p => docRef(p.slice(0, p.lastIndexOf("/")), p.slice(p.lastIndexOf("/") + 1)),
  // a batch is all or nothing, as Firestore's: a create of a document there, or an update of one gone, fails every write
  batch() {
    const ops = [];
    return { set(r, d, o) { ops.push(["set", r, d, o]); }, update(r, d) { ops.push(["update", r, d]); }, create(r, d) { ops.push(["create", r, d]); }, delete(r) { ops.push(["delete", r]); },
      async commit() {
        cost.commits++;
        for (const [k, r] of ops) { if (k === "create" && store.has(r.path)) throw new Error("ALREADY_EXISTS: " + r.path); if (k === "update" && !store.has(r.path)) throw new Error("NOT_FOUND: " + r.path); }
        for (const [k, r, d, o] of ops) await r[k](d, o);
      } };
  },
  async getAll(...refs) {
    cost.getAll++;
    if (failCancelReads && refs.some(r => /Charm_Nest_Cancelled/.test(r.coll))) throw new Error("UNAVAILABLE: pretend Firestore outage");
    const out = refs.map(r => r._snap()); if (afterGetAll) { const f = afterGetAll; afterGetAll = null; f(); } return out;
  },
  async runTransaction(fn) { return fn({ get: r => r.get(), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), create: (r, d) => r.create(d), delete: r => r.delete() }); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, increment: n => n, delete: () => undefined }, Timestamp: { fromMillis: ms => ({ toMillis: () => ms }) }, FieldPath: { documentId: () => "__name__" } }),
  storage: () => ({ bucket: () => ({ name: "test", file: () => ({ exists: async () => [false] }) }) }) };

/* ── pretend Etsy: each call of the mirror's fetch serves the next page queued (and counts the calls) ── */
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
const warns = [], realWarn = console.warn; console.warn = (...a) => warns.push(a.join(" "));
const realLog = console.log; console.log = () => {};

const T0 = Math.floor(Date.now() / 1000) - 3600;
let nextTx = 5000000000;
const receipt = (id, status, extra = {}) => Object.assign({ receipt_id: +id, buyer_user_id: 77, name: "Ann Buyer " + id, status, is_paid: true, is_shipped: false, created_timestamp: T0 - 86400, updated_timestamp: T0,
  grandtotal: { amount: 4500, divisor: 100, currency_code: "USD" },
  transactions: [{ transaction_id: nextTx++, sku: "GF-HEART", title: "Heart charm", quantity: 2, expected_ship_date: T0 + 3 * 86400 }, { transaction_id: nextTx++, sku: "SS-STAR", title: "Star charm", quantity: 1, expected_ship_date: T0 + 2 * 86400 }] }, extra);
const cancelDoc = (id, prefix = "") => store.get(prefix + "Charm_Nest_Cancelled/" + id);
const events = (id, prefix = "") => [...store.entries()].filter(([k, v]) => k.startsWith(prefix + "Order_Timeline/") && v.orderId === id).map(([, v]) => v);
async function runMirror(pages) {
  etsyPages = pages.slice(); store.delete("EtsyMail_Config/receiptsMirrorState");   // (no jitter window: each run fetches)
  const r = await mirror.handler({}); return JSON.parse(r.body);
}
let passed = 0;
async function check(name, fn) { await fn(); passed++; realLog("ok  " + name); }

(async () => {
  await check("the rule: /cancel/i, or Fully Refunded when not shipped", () => {
    assert.equal(OrderCancel.isCancelled({ status: "Canceled" }), true);
    assert.equal(OrderCancel.isCancelled({ status: "cancelled" }), true);
    assert.equal(OrderCancel.isCancelled({ status: "Fully Refunded", is_shipped: false }), true);
    assert.equal(OrderCancel.isCancelled({ status: "Fully Refunded", is_shipped: true }), false);
    assert.equal(OrderCancel.isCancelled({ status: "Partially Refunded" }), false);
    assert.equal(OrderCancel.isCancelled({ status: "Paid" }), false);
    assert.equal(OrderCancel.isCancelled({ status: "Completed", raw: { status: "Completed" } }), false);
    assert.equal(OrderCancel.isCancelled({ status: "Canceled", raw: { status: "Canceled" } }), true, "a mirror document reads the same");
  });

  await check("mirror: a page with a cancelled receipt writes one record and one etsyCancelled event", async () => {
    const before = etsyCalls;
    const out = await runMirror([[receipt("4100000001", "Paid"), receipt("4100000002", "Canceled", { updated_timestamp: T0 + 60 }), receipt("4100000003", "Fully Refunded"), receipt("4100000004", "Fully Refunded", { is_shipped: true })]]);
    assert.equal(out.ok, true, "the mirror run is fine: " + JSON.stringify(out));
    assert.equal(out.receiptsProcessed, 4);
    assert.equal(etsyCalls - before, 1, "one Etsy call for the page, none for the cancels");
    assert(store.get("EtsyMail_Receipts/4100000002"), "the mirror wrote its receipts as always");
    const c = cancelDoc("4100000002");
    assert(c, "the cancelled receipt has its record");
    assert.equal(c.by, "Etsy"); assert.equal(c.source, "etsy"); assert.equal(c.etsyStatus, "Canceled"); assert.equal(c.why, "Cancelled on Etsy");
    assert.equal(c.at, (T0 + 60) * 1000, "at is the receipt's updated time");
    assert.equal(c.buyer, "Ann Buyer 4100000002"); assert.equal(c.placedAt, (T0 - 86400) * 1000); assert.equal(c.shipBy, (T0 + 2 * 86400) * 1000, "shipBy: the earliest line's");
    assert.equal(c.lines.length, 2); assert.deepEqual(Object.keys(c.lines[0]).sort(), ["material", "quantity", "sku", "title", "transactionId"]);
    assert.equal(c.lines[0].sku, "GF-HEART"); assert.equal(c.lines[0].quantity, 2); assert.match(c.lines[0].transactionId, /^\d+$/);
    assert.deepEqual(c.sheets, []);
    assert(cancelDoc("4100000003"), "Fully Refunded and not shipped counts"); assert.equal(cancelDoc("4100000003").etsyStatus, "Fully Refunded");
    assert(!cancelDoc("4100000001"), "a paid order is not cancelled"); assert(!cancelDoc("4100000004"), "refunded after shipping is a return, not a cancel");
    const ev = events("4100000002");
    assert.equal(ev.length, 1); assert.equal(ev[0].type, "etsyCancelled"); assert.equal(ev[0].by, "Etsy"); assert.equal(ev[0].source, "etsy"); assert.equal(ev[0].at, (T0 + 60) * 1000);
    assert(store.has("Order_Timeline/4100000002~etsyCancelled~4100000002"), "the event's key is the receipt id");
    const diag = [...store.entries()].filter(([k]) => k.startsWith("EtsyMail_DiagnosticLog/")).map(([, v]) => v).find(v => v.phase === "end" && v.etsyCancels);
    assert.deepEqual(diag.etsyCancels, { created: 2, noted: 0, errors: 0, off: false });
  });

  await check("mirror: the same page again writes nothing (idempotent)", async () => {
    const snapshot = JSON.stringify([...store.entries()].filter(([k]) => /^(Charm_Nest_Cancelled|Order_Timeline)\//.test(k)));
    const c0 = cost.commits;
    const out = await runMirror([[receipt("4100000002", "Canceled", { updated_timestamp: T0 + 60 }), receipt("4100000003", "Fully Refunded")]]);
    assert.equal(out.ok, true);
    assert.equal(JSON.stringify([...store.entries()].filter(([k]) => /^(Charm_Nest_Cancelled|Order_Timeline)\//.test(k))), snapshot, "no record or event changed");
    assert.equal(cost.commits - c0, 1, "only the mirror's own receipts batch was committed");
  });

  await check("mirror: a page with no cancel costs no read", async () => {
    const g0 = cost.getAll, q0 = cost.queries.length;
    await runMirror([[receipt("4100000010", "Paid"), receipt("4100000011", "Completed", { is_shipped: true })]]);
    assert.equal(cost.getAll, g0, "no getAll");
    assert(!cost.queries.slice(q0).some(q => /Charm_Nest_Cancelled|Order_Timeline/.test(q)), "no query of the cancel records or the timeline");
  });

  await check("a person's record is kept; Etsy only adds etsyStatus (and its event)", async () => {
    const r = await post({ op: "cancelPut", orderId: "4100000020", by: "Paul", why: "customer asked", record: { buyer: "Bea", placedAt: 1, shipBy: 2, sheets: ["GF Sheet 2"], lines: [{ transactionId: "9", sku: "GF-X", title: "X", quantity: 1, material: "gold" }] } });
    assert.equal(r.status, 200); assert.equal(r.body.ok, true); assert.equal(r.body.record.source, "sorter"); assert.equal(r.body.record.by, "Paul");
    const mine = cancelDoc("4100000020");
    const out = await runMirror([[receipt("4100000020", "Canceled", { updated_timestamp: T0 + 120 })]]);
    assert.equal(out.ok, true);
    const c = cancelDoc("4100000020");
    assert.equal(c.by, "Paul"); assert.equal(c.why, "customer asked"); assert.equal(c.source, "sorter"); assert.equal(c.at, mine.at); assert.deepEqual(c.sheets, ["GF Sheet 2"]);
    assert.equal(c.lines[0].material, "gold", "the person's lines stay");
    assert.equal(c.etsyStatus, "Canceled"); assert.equal(c.etsyAt, (T0 + 120) * 1000);
    const ev = events("4100000020").map(e => e.type + ":" + e.by).sort();
    assert.deepEqual(ev, ["cancelled:Paul", "etsyCancelled:Etsy"]);
    // and once more: nothing changes
    const snap = JSON.stringify(cancelDoc("4100000020")); await runMirror([[receipt("4100000020", "Canceled", { updated_timestamp: T0 + 120 })]]);
    assert.equal(JSON.stringify(cancelDoc("4100000020")), snap); assert.equal(events("4100000020").length, 2);
  });

  await check("a person who cancels one Etsy cancelled keeps Etsy as the canceller and adds the sorter's sheets and lines", async () => {
    const r = await post({ op: "cancelPut", orderId: "4100000002", by: "Kim", why: "saw it on Etsy", record: { sheets: ["SS Sheet 1"], lines: [{ transactionId: "1", sku: "GF-HEART", title: "Heart", quantity: 2, material: "gold" }] } });
    assert.equal(r.body.ok, true); assert.equal(r.body.kept, true);
    const c = cancelDoc("4100000002");
    assert.equal(c.by, "Etsy"); assert.equal(c.source, "etsy"); assert.equal(c.why, "Cancelled on Etsy"); assert.deepEqual(c.sheets, ["SS Sheet 1"]); assert.equal(c.lines[0].material, "gold");
    assert(events("4100000002").some(e => e.type === "cancelled" && e.by === "Kim"), "the person's own event is on the timeline");
  });

  await check("cancelPut: source etsy from the sorter, and a person's cancel written anew as before", async () => {
    const r = await post({ op: "cancelPut", orderId: "#4100000030", by: "Lee", source: "etsy", etsyStatus: "Canceled", record: { buyer: "Cy", sheets: ["RG Sheet 1"] } });
    assert.equal(r.body.ok, true);
    const c = cancelDoc("4100000030");
    assert.equal(c.by, "Etsy"); assert.equal(c.source, "etsy"); assert.equal(c.etsyStatus, "Canceled"); assert.equal(c.why, "Cancelled on Etsy"); assert.deepEqual(c.sheets, ["RG Sheet 1"]);
    const ev = events("4100000030"); assert.equal(ev.length, 1); assert.equal(ev[0].type, "etsyCancelled"); assert.equal(ev[0].data.notedBy, "Lee");
    // the mirror seeing it later adds nothing new and no second event
    await runMirror([[receipt("4100000030", "Canceled", { updated_timestamp: T0 + 300 })]]);
    assert.equal(cancelDoc("4100000030").by, "Etsy"); assert.equal(events("4100000030").length, 1);
    assert.equal(cancelDoc("4100000030").buyer, "Cy", "what the record had stays");
    // a person's cancel with no source is a person's (the old callers)
    await post({ op: "cancelPut", orderId: "4100000031", by: "Mo", why: "one", record: {} });
    await post({ op: "cancelPut", orderId: "4100000031", by: "Mo", why: "two", record: {} });
    assert.equal(cancelDoc("4100000031").why, "two", "a person's own record is written anew"); assert.equal(cancelDoc("4100000031").source, "sorter");
    const list = await post({ op: "cancelList", limit: 50 });
    assert(list.body.list.every(x => x.source === "etsy" || x.source === "sorter"), "cancelList gives every record a source");
    store.set("Charm_Nest_Cancelled/4100000032", { orderId: "4100000032", by: "Old", why: "", at: 5, lines: [], sheets: [] });   // a record from before `source`
    assert.equal((await post({ op: "cancelList", limit: 50 })).body.list.find(x => x.orderId === "4100000032").source, "sorter");
    const ids = await post({ op: "cancelList", idsOnly: true }); assert(ids.body.ids.includes("4100000030"));
    const chk = await post({ op: "cancelCheck", orderIds: ["4100000030", "4100000020"] });
    assert.equal(chk.body.cancelled["4100000030"].source, "etsy"); assert.equal(chk.body.cancelled["4100000020"].by, "Paul");
    assert.equal((await post({ op: "cancelPut", orderId: "abc" })).status, 400);
  });

  await check("an order a person restored after Etsy's change is not cancelled again; a later Etsy change does", async () => {
    store.set("Order_Timeline/4100000040~cancelRestored~x", { orderId: "4100000040", type: "cancelRestored", at: (T0 + 500) * 1000, by: "Paul" });
    await runMirror([[receipt("4100000040", "Canceled", { updated_timestamp: T0 + 400 })]]);
    assert(!cancelDoc("4100000040"), "restored after Etsy's last change: left be");
    await runMirror([[receipt("4100000040", "Canceled", { updated_timestamp: T0 + 600 })]]);
    assert(cancelDoc("4100000040"), "Etsy changed it again after the restore: cancelled");
  });

  await check("a failing cancel hook never breaks the mirror", async () => {
    failCancelReads = true;
    const w0 = warns.length;
    const out = await runMirror([[receipt("4100000050", "Canceled"), receipt("4100000051", "Paid")]]);
    failCancelReads = false;
    assert.equal(out.ok, true, "the mirror's run is fine"); assert.equal(out.receiptsProcessed, 2);
    assert(store.get("EtsyMail_Receipts/4100000050") && store.get("EtsyMail_Receipts/4100000051"), "its receipts are written");
    assert(!cancelDoc("4100000050"));
    assert(warns.slice(w0).some(w => /cancel records not written/.test(w)), "the miss is logged");
    const diag = [...store.entries()].filter(([k]) => k.startsWith("EtsyMail_DiagnosticLog/")).map(([, v]) => v).filter(v => v.phase === "end" && v.etsyCancels).pop();
    assert.equal(diag.etsyCancels.errors, 1);
  });

  await check("a person's cancel landing between the mirror's read and its batch is kept", async () => {
    afterGetAll = () => store.set("Charm_Nest_Cancelled/4100000055", { orderId: "4100000055", by: "Ada", source: "sorter", why: "just now", at: 7, lines: [], sheets: [] });
    const out = await runMirror([[receipt("4100000055", "Canceled"), receipt("4100000056", "Canceled")]]);
    assert.equal(out.ok, true);
    assert.equal(cancelDoc("4100000055").by, "Ada"); assert.equal(cancelDoc("4100000055").etsyStatus, "Canceled", "the batch was refused and each was done alone");
    assert.equal(cancelDoc("4100000056").by, "Etsy");
  });

  await check("cancelSweep fills older cancels from the mirror, idempotently, production only", async () => {
    // older receipts the mirror kept (as EtsyMail_Receipts documents), cancelled before this code ran
    for (const [id, st, shipped] of [["4100000060", "Canceled", false], ["4100000061", "Fully Refunded", false], ["4100000062", "Fully Refunded", true], ["4100000063", "Completed", true]]) {
      const raw = receipt(id, st, { is_shipped: shipped, updated_timestamp: T0 - 7 * 86400 });
      store.set("EtsyMail_Receipts/" + id, { receipt_id: id, status: st, is_shipped: shipped, is_paid: true, updated_timestamp: raw.updated_timestamp, created_timestamp: raw.created_timestamp, buyer_name: raw.name, raw });
    }
    const dry = await post({ op: "cancelSweep", dryRun: true });
    assert.equal(dry.status, 200); assert.equal(dry.body.dryRun, true); assert(dry.body.ids.includes("4100000060") && dry.body.ids.includes("4100000061"));
    assert(!cancelDoc("4100000060"), "a dry run writes nothing");
    const r = await post({ op: "cancelSweep" });
    assert.equal(r.body.ok, true); assert.equal(r.body.more, false);
    assert(r.body.ids.includes("4100000060") && r.body.ids.includes("4100000061")); assert(!r.body.ids.includes("4100000062"));
    assert.equal(cancelDoc("4100000060").by, "Etsy"); assert.equal(cancelDoc("4100000060").at, (T0 - 7 * 86400) * 1000); assert.equal(cancelDoc("4100000060").lines.length, 2);
    assert(!cancelDoc("4100000062") && !cancelDoc("4100000063"));
    assert.equal(cancelDoc("4100000020").by, "Paul", "the person's record the sweep also sees stays theirs");
    assert(cost.queries.some(q => /^EtsyMail_Receipts status == Canceled$/.test(q)), "single-field equality queries");
    const again = await post({ op: "cancelSweep" });
    assert.equal(again.body.created, 0); assert.equal(again.body.noted, 0, "the second sweep changes nothing");
    assert.equal(events("4100000060").length, 1);
    assert.equal((await post({ op: "cancelSweep", sandbox: true })).status, 400, "not in the sandbox");
  });

  await check("sandboxCancel: cancelled in the Sandbox_ records as Etsy would, production untouched", async () => {
    const r = await post({ op: "sandboxCancel", orderId: "4100000070", sandbox: true, by: "Tester" });
    assert.equal(r.body.ok, true); assert.equal(r.body.sandbox, true);
    const c = cancelDoc("4100000070", "Sandbox_");
    assert(c); assert.equal(c.by, "Etsy"); assert.equal(c.source, "etsy"); assert.equal(c.etsyStatus, "Canceled");
    assert(!cancelDoc("4100000070"), "no production record");
    assert.equal(events("4100000070", "Sandbox_")[0].type, "etsyCancelled"); assert.equal(events("4100000070").length, 0);
    // one the inbox's mirror knows: what was ordered comes from it
    await post({ op: "sandboxCancel", orderId: "4100000063" });
    const m = cancelDoc("4100000063", "Sandbox_"); assert.equal(m.lines.length, 2); assert.equal(m.buyer, "Ann Buyer 4100000063"); assert(!cancelDoc("4100000063"));
    const chk = await post({ op: "cancelCheck", orderIds: ["4100000070"], sandbox: true });
    assert.equal(chk.body.cancelled["4100000070"].source, "etsy", "a station's check in the sandbox sees it");
    const tl = await post({ op: "timelineGet", orderId: "4100000070", sandbox: true });
    assert.equal(tl.body.cancelled.by, "Etsy"); assert.equal(tl.body.events.length, 1);
  });

  console.warn = realWarn; console.log = realLog;
  console.log(`etsy-cancel: ${passed} checks passed`);
})().catch(e => { console.warn = realWarn; console.log = realLog; console.error(e); process.exit(1); });
