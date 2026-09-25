// Buyers' names and shipping addresses sit behind the operator passcode once it is set (EDIT_PASSCODE, the sorter's): the
// emulated Etsy (etsySandbox) and the Design Station's order archive (designArchive) refuse a call without it, answer one
// with it, let a browser send it (preflight), and stay open while no passcode is set. The customer conversation's
// in-process read of a sandbox order passes the door (serve).
//   node tests/charm-nest/passcode-lock.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const fnDir = path.join(__dirname, '../../netlify/functions');

/* ── a small firebase-admin: one sandbox snapshot of one order, an empty archive ── */
const snapPath = 'charmnest/sandbox/orders-lock.json';
const docs = new Map([['Charm_Sandbox/current', { path: snapPath, count: 1, at: Date.now() }]]);
const blobs = new Map([[snapPath, Buffer.from(JSON.stringify({ receipts: [{ receipt_id: 4170000001, is_paid: true, status: 'Paid', name: 'Test Buyer', transactions: [{ transaction_id: 41700000011, receipt_id: 4170000001 }] }] }))]]);
const docRef = (c, id) => ({ id, async get() { const d = docs.get(c + '/' + id); return { exists: !!d, id, data: () => d && { ...d }, get: f => d && d[f] }; } });
const query = c => { const q = { where: () => q, orderBy: () => q, limit: () => q, select: () => q, startAfter: () => q, async get() { return { empty: true, size: 0, docs: [], forEach() {} }; }, count: () => ({ get: async () => ({ data: () => ({ count: 0 }) }) }), doc: id => docRef(c, id) }; return q; };
const db = { collection: query, batch: () => ({ set() {}, commit: async () => {} }), async getAll(...refs) { return Promise.all(refs.map(r => r.get())); } };
const admin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => 0 }, Timestamp: { fromMillis: ms => ms } }), storage: () => ({ bucket: () => ({ file: p => ({ download: async () => [blobs.get(p)] }) }) }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return admin; return realLoad.call(this, req, ...rest); };
const etsy = require(path.join(fnDir, 'etsySandbox.js')), archive = require(path.join(fnDir, 'designArchive.js'));
const call = (h, q, headers = {}) => h.handler({ httpMethod: 'GET', headers, queryStringParameters: q }).then(r => ({ status: r.statusCode, headers: r.headers || {}, body: r.body ? JSON.parse(r.body) : null }));

(async () => {
  delete process.env.EDIT_PASSCODE;
  assert.strictEqual((await call(etsy, { fn: 'listOpenOrders' })).status, 200, 'no passcode set: the emulator answers, as before');
  assert.strictEqual((await call(archive, { op: 'have', ids: '4170000001' })).status, 200, 'no passcode set: the archive answers, as before');

  process.env.EDIT_PASSCODE = 'lock-test';
  const reads = [[etsy, { fn: 'listOpenOrders' }], [etsy, { fn: 'etsyOrderProxy', orderId: '4170000001' }], [etsy, { fn: 'status' }], [archive, { op: 'day', date: '2026-09-25' }], [archive, { op: 'have', ids: '4170000001' }], [archive, { op: 'index' }]];
  for (const [h, q] of reads) {
    const what = `${h === etsy ? 'etsySandbox' : 'designArchive'} ${JSON.stringify(q)}`;
    const r = await call(h, q);
    assert(r.status === 401 && r.body.error === 'unauthorized' && !/Test Buyer|4170000001/.test(JSON.stringify(r.body)), `${what}: refused without the passcode, and says nothing of the order`);
    assert(r.headers['Access-Control-Allow-Origin'] === '*', `${what}: the refusal reaches the page (CORS)`);
    assert.strictEqual((await call(h, q, { 'x-edit-passcode': 'wrong' })).status, 401, `${what}: a wrong passcode is refused`);
    assert.strictEqual((await call(h, q, { 'X-Edit-Passcode': 'lock-test' })).status, 200, `${what}: answered with the passcode`);
  }
  const put = await archive.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ op: 'put', orders: [{ receiptId: '4170000001' }] }) });
  assert.strictEqual(put.statusCode, 401, 'the archive takes no record without the passcode');
  for (const h of [etsy, archive]) { const pre = await h.handler({ httpMethod: 'OPTIONS', headers: {} }); assert(pre.statusCode < 300 && /x-edit-passcode/i.test(pre.headers['Access-Control-Allow-Headers']), 'a browser may send the passcode (preflight)'); }
  const own = await etsy.serve({ httpMethod: 'GET', queryStringParameters: { fn: 'etsyOrderProxy', orderId: '4170000001' } });
  assert.strictEqual(own.statusCode, 200, "the site's own functions read a sandbox order in process (the customer conversation's buyer)");
  delete process.env.EDIT_PASSCODE;
  console.log('passcode lock OK: the emulated Etsy and the order archive refuse a call without the passcode once it is set, answer one with it, stay open while none is set; the in-process read passes');
})().catch(e => { console.error(e); process.exitCode = 1; });
