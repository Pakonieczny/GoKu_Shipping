'use strict';
// Firestore cost (S8): the Data Manager half of the Ads console's conversion health (googleAdsDataManager.health, run by conversionHealth
// whenever its 15-minute copy is out of date: a dashboard load or a diagnostics refresh).
//   - "confirmed by Google" read up to 500 whole conversion rows to count the ones sent to this destination. It is now a count query (one
//     read per 1,000 matches, no rows moved), and exact above 500. A store without count(), or a refused count, reads the rows as before.
//   - the unsent-queue read (up to 500 rows) was done twice in one conversionHealth call; the second is handed the first.
//
//   node tests/cost/ru-S8-conversion-health.cjs        (prints the numbers; exits 1 when a ceiling or a behaviour check fails)
const path = require('path');
const assert = require('node:assert/strict');
const meter = require('./meter.cjs');
const { createDataManager, destination } = require(path.join(__dirname, '..', '..', 'netlify', 'functions', 'googleAdsDataManager.js'));

const say = (s) => process.stdout.write(s + '\n');
const env = { GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456', GADS_LOGIN_CUSTOMER_ID: '999', GADS_CLIENT_ID: 'c', GADS_CLIENT_SECRET: 's', GADS_DATAMANAGER_REFRESH_TOKEN: 'r' };
const target = destination(env.GADS_CONVERSION_ACTION, env.GADS_LOGIN_CUSTOMER_ID);
const other = destination('customers/123/conversionActions/777', '999');

function fixture(success, elsewhere, pending) {
  const m = meter.create();
  const seed = {};
  for (let i = 0; i < success; i++) seed['Brites_GAds_ConvQueue/ok' + i] = { orderId: 'o' + i, uploaded: true, dmState: 'success', dmRequestId: 'r' + i, dmDestination: target, value: 20, currency: 'USD', gclid: 'g' + i };
  for (let i = 0; i < elsewhere; i++) seed['Brites_GAds_ConvQueue/else' + i] = { orderId: 'e' + i, uploaded: true, dmState: 'success', dmRequestId: 'x' + i, dmDestination: other, value: 20, currency: 'USD', gclid: 'h' + i };
  for (let i = 0; i < pending; i++) seed['Brites_GAds_ConvQueue/wait' + i] = { orderId: 'w' + i, uploaded: false, dmState: 'processing', dmRequestId: 'p' + i, dmSubmittedAt: Date.now() - 3600000, dmDestination: target, value: 20, currency: 'USD', gclid: 'k' + i };
  m.db.seed(seed);
  return m;
}

// a store whose queries have no count() ('absent') or whose count() is refused ('refuse'), everything else being the real in-memory store
function guard(db, mode, hits) {
  const wrap = (o) => new Proxy(o, { get(t, p) {
    if (p === 'count') { if (mode === 'absent') return undefined; return () => { hits.n++; throw new Error('FAILED_PRECONDITION: the query requires an index'); }; }
    const v = t[p];
    if (typeof v !== 'function') return v;
    return (...a) => { const r = v.apply(t, a); return r && typeof r === 'object' && typeof r.then !== 'function' && ['collection', 'where', 'limit', 'orderBy', 'select', 'doc'].includes(p) ? wrap(r) : r; };
  } });
  return wrap(db);
}
const make = (m, db) => createDataManager({ env, fetch: async () => { throw new Error('no network in this test'); }, fb: () => ({ db: db || m.db, FV: { serverTimestamp: () => Date.now() } }),
  COL: { convQueue: 'Brites_GAds_ConvQueue' }, ledger: async () => {} });
const healthOf = async (m, svc, label, args) => { const before = m.snapshot(); const info = await m.op(label, () => svc.health(args || {})); return { info, d: m.since(before) }; };

(async () => {
  // (1) 700 sales confirmed for this destination, 100 for another action, 20 waiting
  const m = fixture(700, 100, 20);
  const counted = await healthOf(m, make(m), 'health (count)');
  say('health with 700 confirmed rows: reads ' + (counted.d.reads + counted.d.aggs) + ' (rows read ' + counted.d.reads + ', count reads ' + counted.d.aggs + '), confirmed ' + counted.info.confirmed + ', complete ' + counted.info.confirmedComplete);
  assert.equal(counted.info.confirmed, 700, 'the count is exact, even above the old 500-row cap');
  assert.equal(counted.info.confirmedComplete, true);
  assert.equal(counted.info.processing, 20, 'the unsent/processing figures are unchanged');
  assert.equal(counted.d.reads, 20, 'the only rows read are the 20 waiting ones');
  assert.equal(counted.d.aggs, 1, 'and the confirmed figure is one count read');

  // (2) a store with no count() (what the older fakes are): rows are read as before, capped at 500
  const m2 = fixture(700, 100, 20);
  const rows = await healthOf(m2, make(m2, guard(m2.db, 'absent', { n: 0 })), 'health (rows, no count)');
  say('same, store without count(): reads ' + (rows.d.reads + rows.d.aggs) + ', confirmed ' + rows.info.confirmed + ', complete ' + rows.info.confirmedComplete + ' (the old behaviour: capped at 500)');
  assert(rows.info.confirmed <= 500 && rows.info.confirmed < 700, 'without count the old figure stands: a lower bound from the first 500 rows (here 400, because 100 of them are another destination\'s)');
  assert.equal(rows.info.confirmedComplete, false);
  assert(rows.d.reads >= 500 + 20, 'and it reads the rows');

  // (3) a count that throws falls back and is not asked again by this instance
  const m3 = fixture(30, 5, 3);
  const hits = { n: 0 };
  const svc3 = make(m3, guard(m3.db, 'refuse', hits));
  const h1 = await healthOf(m3, svc3, 'health (count refused)');
  const h2 = await healthOf(m3, svc3, 'health (count refused, second)');
  assert.equal(h1.info.confirmed, 30, 'a refused count reads the rows and gets the same figure');
  assert.equal(h2.info.confirmed, 30);
  assert.equal(hits.n, 1, 'and a refusing store is asked once by an instance');

  // (4) the unsent rows handed over by the caller are not read again
  const m4 = fixture(10, 0, 300);
  const svc4 = make(m4);
  const own = await healthOf(m4, svc4, 'health (reads the unsent rows itself)');
  const handed = await m4.db.collection('Brites_GAds_ConvQueue').where('uploaded', '==', false).limit(500).get();
  const given = await healthOf(m4, svc4, 'health (unsent rows handed over)', { pending: handed });
  say('300 unsent rows: health alone reads ' + (own.d.reads + own.d.aggs) + ', given the rows conversionHealth had just read ' + (given.d.reads + given.d.aggs));
  assert.equal(own.d.reads, 300);
  assert.equal(given.d.reads, 0, 'nothing is read twice');
  assert.equal(given.info.processing, own.info.processing, 'the figures are the same');
  assert.equal(given.info.confirmed, own.info.confirmed);
  m.print();
})().catch((e) => { console.error(e); process.exit(1); });
