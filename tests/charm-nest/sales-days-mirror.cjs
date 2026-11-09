// SAL1 (Paul, 9 Nov 2026): the receipt mirror (etsyMailReceiptsMirrorCron) fills the Orders head's day counts from the pages it reads anyway, and the
// first run seeds them once from its own Firestore copy, so Yesterday and Last 7 days are whole from day one. Drives the real mirror against an
// in-memory Firestore and a pretend Etsy; counts the Etsy calls. No network.
//   node tests/charm-nest/sales-days-mirror.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const fnDir = path.join(__dirname, "../../netlify/functions"), D = require("../../charm-nest-order-days.js");

const store = new Map(), SERVER_TS = { __ts: true };
const clone = v => JSON.parse(JSON.stringify(v, (k, x) => (x === SERVER_TS ? "__TS__" : x)), (k, x) => (x === "__TS__" ? Date.now() : x));
const docRef = (coll, id) => { const key = coll + "/" + id;
  return { id, path: key, coll,
    async get() { const d = store.get(key); return { exists: !!d, id, data: () => (d ? clone(d) : undefined) }; },
    async set(data, o) { store.set(key, Object.assign({}, (o && o.merge && store.get(key)) || {}, clone(data))); } }; };
const OPS = { "==": (a, b) => a === b, ">=": (a, b) => a >= b, "<": (a, b) => a < b };
let queries = 0;
const query = (coll, filters = []) => ({ where: (f, op, v) => query(coll, filters.concat([[f, op, v]])), orderBy: () => query(coll, filters), select: () => query(coll, filters), limit: () => query(coll, filters), doc: id => docRef(coll, id),
  async get() { queries++; const docs = [...store.entries()].filter(([k, d]) => k.startsWith(coll + "/") && k.split("/").length === 2 && filters.every(([f, op, v]) => (OPS[op] || OPS["=="])(d[f], v))).map(([k, d]) => ({ id: k.split("/")[1], data: () => clone(d) })); return { docs, size: docs.length, empty: !docs.length }; } });
const db = { collection: c => query(c), doc: p => docRef(p.slice(0, p.lastIndexOf("/")), p.slice(p.lastIndexOf("/") + 1)),
  batch() { const ops = []; return { set: (r, d, o) => ops.push([r, d, o]), async commit() { for (const [r, d, o] of ops) await r.set(d, o); } }; },
  async runTransaction(fn) { const w = []; const out = await fn({ get: r => r.get(), set: (r, v) => w.push([r, v]) }); for (const [r, v] of w) await r.set(v); return out; },
  async getAll(...refs) { return Promise.all(refs.map(r => r.get())); } };
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, increment: n => n, delete: () => undefined }, Timestamp: { fromMillis: ms => ({ toMillis: () => ms }) } }) };

let shop = [], etsyCalls = 0;
const Module = require("module"), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === "firebase-admin" || /[\\/]firebaseAdmin(\.js)?$/.test(req) || req === "./firebaseAdmin") return fakeAdmin;
  if (req === "./_orderCancel") throw new Error("cancel records are off in this test");
  if (req === "./_etsyApiMeter") return { bump: () => ({ failNet() {}, fromHttp() {} }), wrapHandler: fn => fn };
  if (req === "./_etsyMailEtsy") return { getValidEtsyAccessToken: async () => "token" };
  if (req === "node-fetch") return async url => {
    etsyCalls++;
    const u = new URL(url), min = +u.searchParams.get("min_last_modified"), off = +u.searchParams.get("offset"), lim = +u.searchParams.get("limit");
    const results = shop.filter(r => r.updated_timestamp >= min).sort((a, b) => a.updated_timestamp - b.updated_timestamp).slice(off, off + lim);
    return { ok: true, status: 200, headers: { get: () => null }, json: async () => ({ count: shop.length, results }), text: async () => "" };
  };
  return realLoad.call(this, req, ...rest);
};
global.fetch = Module._load("node-fetch");
Object.assign(process.env, { SHOP_ID: "1", CLIENT_ID: "c", CLIENT_SECRET: "s" });
const mirror = require(path.join(fnDir, "etsyMailReceiptsMirrorCron.js"));
const realLog = console.log; console.warn = () => {}; console.log = () => {}; console.error = () => {};

(async () => {
  const now = Date.now(), nowS = Math.floor(now / 1000), today = D.dateOf(now), yday = D.shift(today, -1);
  const atDay = (date, h) => Math.floor(Date.parse(date + "T12:00:00Z") / 1000) + h * 60;   // (noon UTC: morning in Toronto, inside the day)
  // the mirror's Firestore copy already holds the last days' receipts (written by earlier runs): 3 yesterday (one cancelled), 2 four days ago, 1 old
  const put = r => store.set("EtsyMail_Receipts/" + r.receipt_id, r);
  [[1, yday, "Paid"], [2, yday, "Completed"], [3, yday, "Canceled"], [4, D.shift(today, -4), "Paid"], [5, D.shift(today, -4), "Paid"], [6, D.shift(today, -30), "Paid"]]
    .forEach(([i, date, status]) => put({ receipt_id: String(4400000000 + i), created_timestamp: atDay(date, i), updated_timestamp: atDay(date, i), status }));
  store.set("EtsyMail_Config/receiptsMirrorState", { lastSyncTimestamp: nowS - 600 });
  // Etsy: two orders placed in the last minutes (one shipped already: the pull rule would skip it; the count does not), and an old one updated now
  shop = [
    { receipt_id: 4400000101, status: "Completed", is_shipped: true, create_timestamp: nowS - 120, created_timestamp: nowS - 120, updated_timestamp: nowS - 100 },
    { receipt_id: 4400000102, status: "Paid", create_timestamp: nowS - 60, created_timestamp: nowS - 60, updated_timestamp: nowS - 50 },
    { receipt_id: 4400000001, status: "Completed", create_timestamp: atDay(yday, 1), created_timestamp: atDay(yday, 1), updated_timestamp: nowS - 40 }
  ];
  const r = await mirror.handler({});
  assert.equal(r.statusCode, 200, r.body);
  assert.equal(etsyCalls, 1, "the mirror's own page, and no other Etsy call: " + etsyCalls);
  const doc = store.get("Charm_Nest_Order_Days/recent");
  assert.ok(doc && doc.seededAt, "the counts were seeded from the mirror's copy");
  let sum = D.summarize(doc, Date.now());
  assert.equal(sum.today.n, D.dateOf((nowS - 120) * 1000) === today ? 2 : 1, "today: the two new receipts (the shipped one too): " + JSON.stringify(sum.today));
  assert.equal(sum.yesterday.n, 3); assert.equal(sum.yesterday.cx, 1, "yesterday: three, one cancelled, counted as placed");
  assert.equal(sum.week.n, sum.today.n + 5, "the week: today, yesterday and four days ago; the 30-day-old one is outside");
  assert.ok(store.get("EtsyMail_Config/receiptsMirrorState").orderDaysSeededAt, "the mirror remembers the seed");
  // the next run: no seed again (no query of the mirror's copy), the same receipts change nothing
  const q0 = queries; store.set("EtsyMail_Config/receiptsMirrorState", Object.assign(store.get("EtsyMail_Config/receiptsMirrorState"), { nextEligibleAtMs: 0, lastSyncTimestamp: nowS - 600 }));
  const was = JSON.stringify(store.get("Charm_Nest_Order_Days/recent").days);
  await mirror.handler({});
  assert.equal(JSON.stringify(store.get("Charm_Nest_Order_Days/recent").days), was, "the same receipts again: nothing moves");
  assert.ok(queries - q0 <= 2, "and the copy is not read again for a seed (" + (queries - q0) + " queries: the customer rebuild's own)");
  realLog(`sales-days-mirror: OK (Etsy calls ${etsyCalls}, both runs' own pages; today ${sum.today.n}, yesterday ${sum.yesterday.n}, week ${sum.week.n})`);
})().catch(e => { realLog(e); process.exit(1); });
