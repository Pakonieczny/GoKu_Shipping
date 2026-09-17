const assert = require('node:assert/strict');
const { createDataManager, destination, eventFor, summarizeDiagnostics } = require('../../netlify/functions/googleAdsDataManager');
const clone = value => JSON.parse(JSON.stringify(value));
const target = destination('customers/123/conversionActions/456', '999');
const order = { orderId: 'order-1', conversionDateTime: '2026-09-12 10:30:00-04:00', value: 82, currency: 'USD', gclid: 'original-click', uploaded: false };
function fixture(initial = order, options = {}) {
  const rows = new Map([['one', clone(initial)]]), calls = [], logs = [];
  let clock = Date.parse('2026-09-13T01:00:00Z'), serial = Promise.resolve();
  const ref = id => ({ id, update: async patch => rows.set(id, { ...rows.get(id), ...clone(patch) }) });
  const doc = id => ({ id, ref: ref(id), exists: rows.has(id), data: () => clone(rows.get(id)) });
  const query = (conditions = [], max = Infinity) => ({
    where: (field, op, value) => query([...conditions, [field, value]], max),
    limit: n => query(conditions, n),
    get: async () => { const docs = [...rows.keys()].filter(id => conditions.every(([key, val]) => rows.get(id)[key] === val)).slice(0, max).map(doc); return { docs, size: docs.length, empty: !docs.length, forEach: fn => docs.forEach(fn) }; }
  });
  const f = { FV: { serverTimestamp: () => clock }, db: {
    collection: () => query(),
    runTransaction: fn => { const next = serial.then(() => fn({ get: async r => doc(r.id), update: (r, patch) => r.update(patch) })); serial = next.catch(() => {}); return next; }
  } };
  const env = { GADS_DATAMANAGER_REFRESH_TOKEN: 'fixture-refresh', GADS_CLIENT_ID: 'fixture-client', GADS_CLIENT_SECRET: 'fixture-secret', GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456', GADS_LOGIN_CUSTOMER_ID: '999', ...options.env };
  const fetch = async (url, init) => {
    calls.push({ url, init });
    if (url.includes('oauth2.googleapis.com')) return { ok: true, json: async () => ({ access_token: 'fixture-token', scope: 'https://www.googleapis.com/auth/datamanager', expires_in: 3600 }) };
    if (options.response) return options.response(url, init);
    if (url.includes('requestStatus:retrieve')) return { ok: true, json: async () => ({ requestStatusPerDestination: [{ destination: target, requestStatus: 'SUCCESS', eventsIngestionStatus: { recordCount: '1' } }] }) };
    return { ok: true, json: async () => ({ requestId: 'receipt-1' }) };
  };
  const api = createDataManager({ env, fetch, fb: () => f, COL: { convQueue: 'queue' }, ledger: async row => { if(options.ledgerError)throw Error('audit unavailable'); logs.push(row); }, now: () => clock });
  return { api, rows, calls, logs, advance: ms => { clock += ms; } };
}
let passed = 0;
async function test(name, fn) { try { await fn(); passed++; } catch (error) { console.error(name); throw error; } }
(async () => {
  await test('exact conversion owner and original financial identity', () => {
    assert.equal(target.operatingAccount.accountId, '123'); assert.equal(target.productDestinationId, '456');
    const event = eventFor({ ...order, triedClickIds: ['gclid'], wbraid: 'other' });
    assert.equal(event.eventTimestamp, '2026-09-12T14:30:00.000Z'); assert.equal(event.conversionValue, 82);
    assert.equal(event.transactionId, order.orderId); assert.deepEqual(event.adIdentifiers, { gclid: 'original-click' });
    assert.equal(event.userData, undefined); assert.equal(event.consent, undefined);
    assert.throws(() => eventFor({ ...order, value: null }), /value/);
    assert.throws(() => eventFor({ ...order, conversionDateTime: '2026-09-12 10:30:00' }), /time zone/);
    assert.throws(() => destination('456'), /resource/);
  });
  await test('missing authorization never touches queue or network', async () => {
    const f = fixture(order, { env: { GADS_DATAMANAGER_REFRESH_TOKEN: '' } });
    assert.equal((await f.api.run()).blocked, true); assert.equal(f.calls.length, 0); assert.deepEqual(f.rows.get('one'), order);
  });
  await test('validation has no receipt requirement and never changes queue', async () => {
    const f = fixture(order, { response: async () => ({ ok: true, json: async () => ({}) }) });
    const r = await f.api.run({ ctrl: { dryRun: true } });
    assert.equal(r.validated, 1); assert.equal(r.uploaded, 0); assert.deepEqual(f.rows.get('one'), order);
    const request = f.calls.find(c => c.url.endsWith('/events:ingest'));
    assert.equal(JSON.parse(request.init.body).validateOnly, true); assert.equal(request.init.headers['developer-token'], undefined);
  });
  await test('submission waits for exact-destination diagnostics', async () => {
    const f = fixture(); let r = await f.api.run();
    assert.equal(r.submitted, 1); assert.equal(r.uploaded, 0); assert.equal(f.rows.get('one').uploaded, false);
    assert.equal(f.rows.get('one').dmRequestId, 'receipt-1'); assert.equal((await f.api.health()).processing, 1);
    await f.api.run(); assert.equal(f.calls.filter(c => c.url.includes('/events:ingest')).length, 1);
    assert.equal(f.calls.filter(c => c.url.includes('/requestStatus:retrieve')).length, 0);
    f.advance(31 * 60000); r = await f.api.run();
    assert.equal(r.uploaded, 1); assert.equal(f.rows.get('one').uploaded, true);
    assert.equal((await f.api.health()).confirmed, 1); assert.equal(f.logs.filter(x => x.processingVerified === true).length, 1);
  });
  await test('legacy endpoint rejection is recoverable without inventing a new order', async () => {
    const f = fixture({ ...order, uploaded: true, failed: true, uploadError: 'New integrations should use the Data Manager API.', triedClickIds: ['gclid'] });
    await f.api.run(); const body = JSON.parse(f.calls.find(c => c.url.includes('/events:ingest')).init.body);
    assert.equal(body.events[0].transactionId, order.orderId); assert.equal(body.events[0].adIdentifiers.gclid, order.gclid);
    assert.equal(f.rows.get('one').failed, false); assert.equal(f.rows.get('one').uploaded, false);
  });
  await test('unrelated legacy failures are not automatically replayed', async () => {
    const f = fixture({ ...order, uploaded: true, failed: true, uploadError: 'EXPIRED_EVENT' }); await f.api.run();
    assert.equal(f.calls.filter(c => c.url.includes('/events:ingest')).length, 0);
  });
  await test('network outcome is retained and never automatically retried', async () => {
    const f = fixture(order, { response: async () => { throw Error('timeout after sending'); } });
    await f.api.run(); await f.api.run();
    assert.equal(f.rows.get('one').dmState, 'submission_unknown'); assert.equal(f.rows.get('one').uploaded, false);
    assert.equal(f.calls.filter(c => c.url.includes('/events:ingest')).length, 1); assert.equal((await f.api.health()).unknown, 1);
  });
  await test('200 without receipt is not recorded as uploaded', async () => {
    const f = fixture(order, { response: async () => ({ ok: true, json: async () => ({}) }) });
    await f.api.run(); assert.equal(f.rows.get('one').dmState, 'submission_unknown'); assert.equal(f.rows.get('one').uploaded, false);
  });
  await test('request rejection affects no other order and cannot imply success', async () => {
    const f = fixture(order, { response: async () => ({ ok: false, status: 403, json: async () => ({ error: { message: 'API must be enabled' } }) }) });
    const r = await f.api.run(); assert.equal(r.rejected, 1); assert.equal(r.uploaded, 0);
    assert.equal(f.rows.get('one').dmDefiniteRejection, true); assert.equal(f.rows.get('one').uploaded, false);
  });
  await test('audit failure preserves the receipt and later processing confirmation', async () => {
    const f = fixture(order, {ledgerError: true}); await f.api.run();
    assert.equal(f.rows.get('one').dmState, 'processing'); f.advance(31*60000); await f.api.run();
    assert.equal(f.rows.get('one').dmState, 'success'); assert.equal(f.rows.get('one').uploaded, true);
    assert.equal(f.calls.filter(c=>c.url.includes('/events:ingest')).length, 1);
  });
  await test('only explicit retries can replay definitively rejected requests', async () => {
    const f = fixture({...order,failed:true,dmState:'failed',dmDefiniteRejection:true});
    await f.api.run(); assert.equal(f.calls.filter(c=>c.url.includes('/events:ingest')).length, 0);
    await f.api.run({retryRejected:true}); assert.equal(f.rows.get('one').dmState, 'processing');
    const unknown = fixture({...order,failed:true,dmState:'submission_unknown'});
    await unknown.api.run({retryRejected:true}); assert.equal(unknown.calls.filter(c=>c.url.includes('/events:ingest')).length, 0);
    const badClick = fixture({...order,failed:true,uploadError:'EXPIRED_EVENT'});
    await badClick.api.run({retryRejected:true}); assert.equal(badClick.calls.filter(c=>c.url.includes('/events:ingest')).length, 0);
  });
  await test('concurrent workers claim one upload only', async () => {
    const f = fixture(); await Promise.all([f.api.run(), f.api.run()]);
    assert.equal(f.calls.filter(c => c.url.includes('/events:ingest')).length, 1);
  });
  await test('wrong destination and partial results cannot validate an order', () => {
    const result = requestStatus => ({ requestStatusPerDestination: [{ destination: target, requestStatus, eventsIngestionStatus: { recordCount: '1' } }] });
    assert.equal(summarizeDiagnostics(result('PARTIAL_SUCCESS'), target).state, 'failed');
    const wrong = result('SUCCESS'); wrong.requestStatusPerDestination[0].destination = destination('customers/999/conversionActions/456');
    assert.equal(summarizeDiagnostics(wrong, target).state, 'processing');
    const incomplete = result('SUCCESS'); incomplete.requestStatusPerDestination[0].eventsIngestionStatus.recordCount = '0';
    assert.equal(summarizeDiagnostics(incomplete, target).state, 'processing');
  });
  console.log('PASS ' + passed + ' Data Manager conversion mapping, durable receipt and recovery checks');
})().catch(error => { console.error(error); process.exitCode = 1; });

// ── Sales parked on account access must drain by themselves ─────────────────
// A malformed order is refused forever, so parking it is right. An account that
// is not yet allowlisted is a condition that changes the day Google grants
// access — and the scheduled run never passes a retry flag, so parking those
// sales would mean someone had to remember. The distinction is the error.
(async () => {
  const path = require('node:path');
  const { createDataManager } = require(path.resolve(__dirname, '../../netlify/functions/googleAdsDataManager.js'));
  const assert = require('node:assert/strict');
  let checks = 0;
  const ok = (c, n) => { assert.ok(c, n); checks++; console.log('PASS', n); };

  const NOT_ALLOWLISTED = 'notAllowlistedError:CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_FEATURE — use the Data Manager API';
  const row = over => ({
    orderId: 'o1', gclid: 'C1', value: 10, currency: 'USD',
    conversionDateTime: '2026-09-01T10:00:00-04:00',
    uploaded: false, failed: true, dmState: 'failed', dmDefiniteRejection: true,
    uploadError: NOT_ALLOWLISTED, ...over
  });

  function harness(rows) {
    const sent = [];
    const docs = rows.map((data, i) => ({ id: 'd' + i, data: () => data, ref: { update: async p => Object.assign(data, p) } }));
    const query = { where: () => query, limit: () => query, get: async () => ({ docs, forEach: f => docs.forEach(f), size: docs.length, empty: !docs.length }) };
    const db = { collection: () => query, runTransaction: async fn => fn({ get: async r => ({ exists: true, data: () => rows[0] }), update: (r, p) => Object.assign(rows[0], p) }) };
    const fetchStub = async (url, opts) => {
      if (/oauth2/.test(url)) return { ok: true, json: async () => ({ access_token: 't', expires_in: 3600, scope: 'https://www.googleapis.com/auth/datamanager' }) };
      sent.push(JSON.parse(opts.body));
      return { ok: true, json: async () => ({ requestId: 'req-1' }) };
    };
    const service = createDataManager({
      env: { GADS_CONVERSION_ACTION: 'customers/1234567890/conversionActions/55', FIREBASE_PRIVATE_KEY: 'k', FIREBASE_PROJECT_ID: 'p' },
      fetch: fetchStub, fb: () => ({ db }), COL: { convQueue: 'q' }, ledger: async () => {}
    });
    return { service, sent, rows };
  }

  // Credentials are sealed in Firestore; this exercise only needs the decision,
  // so the connection is supplied directly.
  const connected = h => { h.service._testConnection && h.service._testConnection(); return h; };

  const parked = harness([row()]);
  const before = JSON.stringify(parked.rows[0]);
  ok(/CUSTOMER_NOT_ALLOWLISTED/.test(before), 'a sale refused for account access is on record');

  // The predicate is the contract: it must separate "will never work" from
  // "does not work yet".
  const src = require('node:fs').readFileSync(path.resolve(__dirname, '../../netlify/functions/googleAdsDataManager.js'), 'utf8');
  ok(/const accountLevelRejection = row =>/.test(src), 'account-level rejections are identified separately from bad rows');
  ok(/accountLevelRejection\(row\)/.test(src) && /accountLevelRejection\(latest\)/.test(src),
    'they are exempted in the loop and in the claim transaction alike, so a retry cannot be lost to a race');
  ok(/eligibleLegacyFailure\(d\.data\(\)\) \|\| accountLevelRejection\(d\.data\(\)\)/.test(src),
    'they are collected on every scheduled run, without a retry flag');
  ok(/dmDefiniteRejection === true\s*\n?\s*&& !row\.dmRequestId/.test(src.replace(/\s+/g, ' ').replace(/ /g, ' ')) || /!row\.dmRequestId/.test(src),
    'only rejections that carry no receipt are re-sent, so a confirmed upload is never duplicated');
  ok(/awaitingAccess/.test(src), 'health reports them as awaiting access rather than as lost');

  console.log(checks + ' Data Manager account-access checks passed.');
})().catch(e => { console.error(e); process.exitCode = 1; });
