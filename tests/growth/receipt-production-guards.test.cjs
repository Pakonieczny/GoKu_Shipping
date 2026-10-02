'use strict';

// Production-code behavior against an isolated in-memory Firestore fixture.
// Every account, receipt, sale and credential is synthetic. No live storage,
// provider requests, event ingestion or individual-purchase attribution occurs.
const assert = require('node:assert/strict');
const test = require('node:test');
const { createDataManager, destination } = require('../../netlify/functions/googleAdsDataManager');
const { rowFingerprint } = require('../../netlify/functions/_britesGrowthReceiptReconciliation');
const epoch = Date.parse('2026-10-02T06:00:00Z');
const target = destination('customers/123/conversionActions/456', '999');
class Timestamp {
  constructor(seconds, nanoseconds = 0) { this.seconds = seconds; this.nanoseconds = nanoseconds; }
  toMillis() { return this.seconds * 1000 + Math.floor(this.nanoseconds / 1000000); }
}
function copy(value, seen = new Map()) {
  if (!value || typeof value !== 'object') return value;
  if (seen.has(value)) return seen.get(value);
  if (value instanceof Date) return new Date(value.getTime());
  if (Buffer.isBuffer(value)) return Buffer.from(value);
  if (value instanceof Timestamp) return new Timestamp(value.seconds, value.nanoseconds);
  const out = Array.isArray(value) ? [] : {};
  seen.set(value, out);
  for (const key of Object.keys(value)) out[key] = copy(value[key], seen);
  return out;
}
const receipt = (extra = {}) => ({
  orderId: 'synthetic-order', value: 82, currency: 'USD', refundedTotal: 7,
  gclid: 'synthetic-click', gbraid: 'synthetic-gbraid', wbraid: 'synthetic-wbraid',
  conversionDateTime: '2026-09-26 10:30:00-04:00', buyerCountry: 'US',
  consent: { adUserData: 'GRANTED', adPersonalization: 'DENIED' },
  customer: { id: 'synthetic-customer', preferences: { occasion: 'birthday' } },
  refundIds: ['synthetic-refund-1'], dmValue: 75, originalTotal: 82,
  uploaded: false, failed: false, dmState: 'processing', dmRequestId: 'synthetic-receipt',
  dmDestination: copy(target), dmSubmittedAt: epoch - 3 * 86400000,
  dmChecks: 0, dmNextCheckAt: 0, createdAt: new Date(epoch - 4 * 86400000),
  updatedAt: new Timestamp(Math.floor(epoch / 1000) - 5, 123), ...extra
});
const diagnostics = (state = 'SUCCESS') => ({ requestStatusPerDestination: [{
  destination: copy(target), requestStatus: state, eventsIngestionStatus: { recordCount: '1' }
}] });
const response = (data, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => copy(data) });

function fixture(initial = [receipt()], options = {}) {
  const rows = new Map(initial.map((row, index) => ['row-' + index, copy(row)]));
  const versions = new Map([...rows.keys()].map(id => [id, 1]));
  const calls = [], audits = [], transactions = [], committed = [];
  let clock = epoch, revision = 1, ordinal = 0, serial = Promise.resolve();
  const put = (id, row) => { rows.set(id, copy(row)); versions.set(id, (versions.get(id) || 0) + 1); revision++; };
  const remove = id => { rows.delete(id); versions.set(id, (versions.get(id) || 0) + 1); revision++; };
  const ref = id => ({ id, path: 'synthetic-production-queue/' + id, kind: 'document', get: async () => snapshot(id) });
  const snapshot = id => {
    const value = copy(rows.get(id));
    return { id, ref: ref(id), exists: rows.has(id), data: () => copy(value) };
  };
  const query = (conditions = [], maximum = Infinity) => ({
    kind: 'query', conditions, maximum,
    where: (field, op, value) => { assert.equal(op, '=='); return query([...conditions, [field, value]], maximum); },
    limit: maximum => query(conditions, maximum),
    get: async () => {
      const docs = [...rows.keys()].filter(id => conditions.every(([field, value]) => rows.get(id)[field] === value))
        .slice(0, maximum).map(snapshot);
      return { docs, size: docs.length, empty: docs.length === 0 };
    }
  });
  const db = {
    collection: name => { assert.equal(name, 'synthetic-production-queue'); return query(); },
    runTransaction: work => {
      const number = ++ordinal;
      const result = serial.then(async () => {
        for (let attempt = 1; attempt <= 4; attempt++) {
          if (options.transactionError?.({ number, attempt })) throw Error('Synthetic transaction unavailable');
          const reads = new Map(), writes = [], events = [];
          let queryVersion = null;
          const trace = { number, attempt, events, committed: false };
          transactions.push(trace);
          await options.beforeTransaction?.({ number, attempt, put, remove, rows, advance: ms => { clock += ms; } });
          const tx = {
            get: async input => {
              assert.equal(writes.length, 0, 'every transaction read must precede every write');
              if (input.kind === 'query') {
                events.push({ kind: 'query', conditions: copy(input.conditions), maximum: input.maximum });
                if (options.queryError?.({ number, attempt })) throw Error('Synthetic uniqueness query unavailable');
                const normal = await input.get();
                queryVersion = revision;
                normal.docs.forEach(doc => reads.set(doc.id, versions.get(doc.id)));
                return options.queryResponse ? options.queryResponse({ number, attempt, normal }) : normal;
              }
              events.push({ kind: 'document', id: input.id });
              reads.set(input.id, versions.get(input.id));
              return snapshot(input.id);
            },
            update: (input, patch) => { events.push({ kind: 'update', id: input.id, fields: Object.keys(patch) }); writes.push([input.id, copy(patch)]); }
          };
          const value = await work(tx);
          await options.beforeCommit?.({ number, attempt, writes, put, remove, rows, advance: ms => { clock += ms; } });
          const changed = [...reads].some(([id, version]) => versions.get(id) !== version) || queryVersion !== null && queryVersion !== revision;
          if (changed) { trace.retried = true; continue; }
          for (const [id, patch] of writes) {
            assert.ok(rows.has(id), 'cannot update a missing row');
            put(id, { ...rows.get(id), ...patch }); committed.push({ number, attempt, id, patch });
          }
          trace.committed = true;
          return value;
        }
        throw Error('Synthetic transaction retry budget exhausted');
      });
      serial = result.catch(() => {}); return result;
    }
  };
  const controls = { rows, put, remove, advance: ms => { clock += ms; }, now: () => clock };
  const fetch = async (url, init) => {
    calls.push({ url, method: init.method, body: init.body });
    if (url === 'https://oauth2.googleapis.com/token') return response({ access_token: 'synthetic-token',
      expires_in: 3600, scope: 'https://www.googleapis.com/auth/datamanager' });
    assert.match(url, /^https:\/\/datamanager\.googleapis\.com\/v1\/requestStatus:retrieve\?requestId=/);
    assert.equal(init.method, 'GET'); assert.equal(init.body, undefined, 'receipt checks cannot ingest events');
    return options.diagnostic ? options.diagnostic(controls) : response(diagnostics());
  };
  const api = createDataManager({ env: {
    GADS_DATAMANAGER_REFRESH_TOKEN: 'synthetic-refresh', GADS_CLIENT_ID: 'synthetic-client',
    GADS_CLIENT_SECRET: 'synthetic-secret', GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456',
    GADS_LOGIN_CUSTOMER_ID: '999'
  }, fetch, fb: () => ({ db, FV: { serverTimestamp: () => clock } }), COL: { convQueue: 'synthetic-production-queue' },
  ledger: async entry => audits.push(copy(entry)), now: () => clock });
  return { api, ...controls, calls, audits, transactions, committed,
    statusCalls: () => calls.filter(call => call.url.includes('requestStatus:retrieve')) };
}
function unchangedPrivateFields(saved, original) {
  const status = new Set(['uploaded', 'failed', 'dmState', 'dmChecks', 'dmCheckedAt', 'dmNextCheckAt',
    'dmReconcileLease', 'dmReconcileLeaseUntil', 'dmWarnings', 'dmLastStatus', 'dmCheckError', 'uploadError', 'uploadedAt']);
  for (const key of Object.keys(original)) if (!status.has(key)) assert.deepEqual(saved[key], original[key], key + ' is preserved');
}

test('one unchanged saved receipt confirms once while every original sale field is preserved', async () => {
  const original = receipt(); const f = fixture([original]); const result = await f.api.reconcile();
  assert.equal(result.confirmed, 1); assert.equal(result.checked, 1); assert.equal(f.statusCalls().length, 1);
  assert.equal(f.audits.length, 1); assert.equal(f.rows.get('row-0').dmState, 'success');
  unchangedPrivateFields(f.rows.get('row-0'), original);
  assert.equal(f.rows.get('row-0').dmReconcileLease, null);
  assert.equal(f.rows.get('row-0').individualOrderConfirmed, undefined);
  assert.equal(f.rows.get('row-0').attributionConfirmed, undefined);
  for (const trace of f.transactions) {
    const queryRead = trace.events.find(event => event.kind === 'query');
    assert.deepEqual(queryRead.conditions, [['dmRequestId', 'synthetic-receipt']]);
    assert.equal(queryRead.maximum, 2, 'a bounded query checks even terminal duplicate rows');
    assert.equal(trace.events.at(-1).kind, 'update', 'all reads finish before the status write');
  }
  await f.api.reconcile({ force: true }); assert.equal(f.audits.length, 1); assert.equal(f.statusCalls().length, 1);
});

for (const [name, mutate] of [
  ['sale value', row => ({ ...row, value: 83 })],
  ['sale currency', row => ({ ...row, currency: 'CAD' })],
  ['refund total', row => ({ ...row, refundedTotal: 17 })],
  ['full refund', row => ({ ...row, refundedTotal: row.value })],
  ['refund identity list', row => ({ ...row, refundIds: [...row.refundIds, 'synthetic-refund-2'] })],
  ['original submitted monetary value', row => ({ ...row, dmValue: 74 })],
  ['original total', row => ({ ...row, originalTotal: 83 })],
  ['order identity', row => ({ ...row, orderId: 'synthetic-other-order' })],
  ['original conversion timestamp', row => ({ ...row, conversionDateTime: '2026-09-27 10:30:00-04:00' })],
  ['gclid', row => ({ ...row, gclid: 'synthetic-changed-click' })],
  ['gbraid', row => ({ ...row, gbraid: 'synthetic-changed-gbraid' })],
  ['wbraid', row => ({ ...row, wbraid: 'synthetic-changed-wbraid' })],
  ['click kind deletion', row => { const changed = { ...row }; delete changed.gclid; return changed; }],
  ['consent withdrawal', row => ({ ...row, consent: { ...row.consent, adUserData: 'DENIED' } })],
  ['country', row => ({ ...row, buyerCountry: 'GB' })],
  ['nested customer data', row => ({ ...row, customer: { ...row.customer, preferences: { occasion: 'anniversary' } } })],
  ['saved check count', row => ({ ...row, dmChecks: 4 })],
  ['another diagnostic check', row => ({ ...row, dmCheckedAt: epoch + 1 })],
  ['saved submission time', row => ({ ...row, dmSubmittedAt: row.dmSubmittedAt + 1 })],
  ['saved provider status', row => ({ ...row, dmLastStatus: 'FAILED' })],
  ['saved upload failure detail', row => ({ ...row, uploadError: 'Synthetic newer failure detail' })],
  ['Date millisecond', row => ({ ...row, createdAt: new Date(row.createdAt.getTime() + 1) })],
  ['Firestore Timestamp nanosecond', row => ({ ...row, updatedAt: new Timestamp(row.updatedAt.seconds, row.updatedAt.nanoseconds + 1) })],
  ['new monetary or refund field', row => ({ ...row, refundAwaitingAdjustment: true })],
  ['new unknown field', row => ({ ...row, sourceRevision: 2 })],
  ['undefined field presence', row => ({ ...row, extra: undefined })],
  ['same-value different financial type', row => ({ ...row, value: String(row.value) })],
  ['original destination account', row => ({ ...row, dmDestination: destination('customers/124/conversionActions/456', '999') })],
  ['original destination action', row => ({ ...row, dmDestination: destination('customers/123/conversionActions/457', '999') })],
  ['saved manager routing', row => ({ ...row, dmDestination: destination('customers/123/conversionActions/456', '998') })],
  ['replacement provider receipt', row => ({ ...row, dmRequestId: 'synthetic-replacement-receipt' })],
  ['independent confirmation', row => ({ ...row, uploaded: true, dmState: 'success' })],
  ['independent failure', row => ({ ...row, failed: true, dmState: 'failed' })],
  ['row expiration', row => ({ ...row, expired: true })]
]) test('in-flight ' + name + ' change invalidates receipt confirmation and is never overwritten', async () => {
  let changed;
  const f = fixture([receipt()], { diagnostic: async ({ rows, put }) => {
    changed = mutate(rows.get('row-0')); put('row-0', changed); return response(diagnostics());
  } });
  const result = await f.api.reconcile();
  assert.equal(result.confirmed, 0); assert.equal(result.checked, 0); assert.equal(f.audits.length, 0);
  assert.equal(f.statusCalls().length, 1); assert.deepEqual(f.rows.get('row-0'), changed);
  assert.equal(f.committed.length, 1, 'only the earlier owned lease was written');
  assert.deepEqual(Object.keys(f.committed[0].patch).sort(), ['dmReconcileLease', 'dmReconcileLeaseUntil']);
});

test('object key reordering does not change the original row identity', async () => {
  const f = fixture([receipt()], { diagnostic: async ({ rows, put }) => {
    const current = rows.get('row-0'), { dmDestination: d, consent, customer } = current;
    put('row-0', Object.fromEntries(Object.entries({ ...current,
      dmDestination: { productDestinationId: d.productDestinationId, loginAccount: { accountId: d.loginAccount.accountId,
        accountType: d.loginAccount.accountType }, operatingAccount: { accountId: d.operatingAccount.accountId, accountType: d.operatingAccount.accountType } },
      consent: { adPersonalization: consent.adPersonalization, adUserData: consent.adUserData },
      customer: { preferences: customer.preferences, id: customer.id }
    }).reverse()));
    return response(diagnostics());
  } });
  assert.equal((await f.api.reconcile()).confirmed, 1); assert.equal(f.audits.length, 1);
});

for (const [name, extra] of [
  ['pending', {}], ['confirmed', { uploaded: true, dmState: 'success' }],
  ['failed', { failed: true, dmState: 'failed' }], ['terminal archived', { uploaded: true, failed: true, dmState: 'archived' }]
]) test('a saved request shared with a ' + name + ' row is rejected before lease or provider lookup', async () => {
  const f = fixture([receipt(), receipt({ orderId: 'synthetic-other-order', ...extra })]);
  const before = copy([...f.rows]); const result = await f.api.reconcile({ force: true });
  assert.equal(result.confirmed, 0); assert.equal(result.checked, 0); assert.equal(f.statusCalls().length, 0);
  assert.equal(f.committed.length, 0); assert.equal(f.audits.length, 0); assert.deepEqual([...f.rows], before);
});

for (const [name, extra] of [
  ['pending', {}], ['confirmed', { uploaded: true, dmState: 'success' }], ['failed', { failed: true, dmState: 'failed' }]
]) test('a ' + name + ' duplicate introduced during GET cannot inherit the receipt status', async () => {
  let first, duplicate;
  const f = fixture([receipt()], { diagnostic: async ({ rows, put }) => {
    first = copy(rows.get('row-0')); duplicate = receipt({ orderId: 'synthetic-other-order', ...extra });
    put('other', duplicate); return response(diagnostics());
  } });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 0); assert.equal(result.checked, 0);
  assert.deepEqual(f.rows.get('row-0'), first); assert.deepEqual(f.rows.get('other'), duplicate);
  assert.equal(f.audits.length, 0); assert.equal(f.committed.length, 1);
});

for (const [name, returned] of [
  ['missing snapshot', undefined], ['missing docs', {}], ['non-array docs', { docs: {} }],
  ['no matching row', { docs: [] }], ['different row', { docs: [{ id: 'other', exists: true, data: () => receipt() }] }],
  ['nonexistent matching row', { docs: [{ id: 'row-0', exists: false, data: () => receipt() }] }],
  ['wrong request in matching row', { docs: [{ id: 'row-0', exists: true, data: () => receipt({ dmRequestId: 'other' }) }] }],
  ['missing data method', { docs: [{ id: 'row-0', exists: true }] }]
]) test('unconfirmed uniqueness result (' + name + ') fails closed without claiming or checking', async () => {
  const f = fixture([receipt()], { queryResponse: () => returned }); const before = copy([...f.rows]);
  const result = await f.api.reconcile(); assert.equal(result.checked, 0); assert.equal(result.confirmed, 0);
  assert.equal(f.committed.length, 0); assert.equal(f.statusCalls().length, 0); assert.equal(f.audits.length, 0);
  assert.deepEqual([...f.rows], before);
});

for (const number of [1, 2]) test('uniqueness query failure in transaction ' + number + ' preserves uncertainty', async () => {
  const original = receipt(); const f = fixture([original], { queryError: info => info.number === number });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 0); assert.equal(result.checked, 0);
  assert.equal(result.errors.length, 1); assert.match(result.errors[0].error, /uniqueness query unavailable/);
  assert.equal(f.rows.get('row-0').uploaded, false); assert.equal(f.rows.get('row-0').dmChecks, 0);
  assert.equal(f.audits.length, 0); assert.equal(f.statusCalls().length, number === 1 ? 0 : 1);
  assert.equal(f.committed.length, number === 1 ? 0 : 1); unchangedPrivateFields(f.rows.get('row-0'), original);
});

for (const [name, invalid] of [
  ['non-finite money', receipt({ value: NaN })],
  ['cyclic data', (() => { const row = receipt(); row.customer.circular = row; return row; })()],
  ['oversized data', receipt({ extra: 'x'.repeat(129000) })],
  ['unsupported function', receipt({ extra: () => 'synthetic' })],
  ['invalid Date', receipt({ createdAt: new Date(NaN) })]
]) test('unfingerprintable original row (' + name + ') is never claimed or sent', async () => {
  const f = fixture([invalid]); const before = copy([...f.rows]); const result = await f.api.reconcile();
  assert.equal(result.confirmed, 0); assert.equal(result.checked, 0); assert.equal(f.committed.length, 0);
  assert.equal(f.statusCalls().length, 0); assert.equal(f.audits.length, 0); assert.deepEqual([...f.rows], before);
});

test('new unsupported in-flight data cannot bypass the original fingerprint check', async () => {
  let changed;
  const f = fixture([receipt()], { diagnostic: async ({ rows, put }) => {
    changed = { ...rows.get('row-0'), extra: Infinity }; put('row-0', changed); return response(diagnostics());
  } });
  assert.equal((await f.api.reconcile()).confirmed, 0); assert.deepEqual(f.rows.get('row-0'), changed); assert.equal(f.audits.length, 0);
});

test('a removed row cannot be restored by a late provider answer', async () => {
  const f = fixture([receipt()], { diagnostic: async ({ remove }) => { remove('row-0'); return response(diagnostics()); } });
  assert.equal((await f.api.reconcile()).confirmed, 0); assert.equal(f.rows.has('row-0'), false); assert.equal(f.audits.length, 0);
});

test('an expired owned lease cannot confirm a late answer even without another worker', async () => {
  const f = fixture([receipt()], { diagnostic: async ({ advance }) => { advance(90000); return response(diagnostics()); } });
  const result = await f.api.reconcile({ maxMs: 120000 }); assert.equal(result.confirmed, 0); assert.equal(result.checked, 0);
  assert.equal(f.rows.get('row-0').uploaded, false); assert.equal(f.rows.get('row-0').dmChecks, 0); assert.equal(f.audits.length, 0);
});

test('an active foreign lease present before claim prevents every provider lookup and write', async () => {
  const f = fixture([receipt()], { beforeTransaction: ({ number, put, rows }) => {
    if (number === 1) put('row-0', { ...rows.get('row-0'), dmReconcileLease: 'synthetic-foreign', dmReconcileLeaseUntil: epoch + 90000 });
  } });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 0); assert.equal(f.statusCalls().length, 0);
  assert.equal(f.committed.length, 0); assert.equal(f.rows.get('row-0').dmReconcileLease, 'synthetic-foreign');
});

test('replacement of the owned lease during GET leaves the foreign owner unchanged', async () => {
  let changed;
  const f = fixture([receipt()], { diagnostic: async ({ put, rows }) => {
    changed = { ...rows.get('row-0'), dmReconcileLease: 'synthetic-foreign', dmReconcileLeaseUntil: epoch + 180000 };
    put('row-0', changed); return response(diagnostics());
  } });
  assert.equal((await f.api.reconcile()).confirmed, 0); assert.deepEqual(f.rows.get('row-0'), changed); assert.equal(f.audits.length, 0);
});

test('concurrent receipt refreshes preserve one status commit and one audit', { timeout: 3000 }, async () => {
  const f = fixture(); const result = await Promise.all(Array.from({ length: 20 }, () => f.api.reconcile({ force: true })));
  assert.equal(result.reduce((count, item) => count + item.confirmed, 0), 1); assert.equal(f.audits.length, 1);
  assert.equal(f.statusCalls().length, 1); assert.equal(f.rows.get('row-0').dmChecks, 1);
  assert.equal(f.committed.filter(write => write.patch.uploaded === true).length, 1);
});

test('a refund conflict after final reads retries the transaction and rejects the old provider answer', async () => {
  const f = fixture([receipt()], { beforeCommit: ({ number, attempt, rows, put }) => {
    if (number === 2 && attempt === 1) put('row-0', { ...rows.get('row-0'), refundedTotal: 27 });
  } });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 0); assert.equal(f.rows.get('row-0').refundedTotal, 27);
  assert.equal(f.rows.get('row-0').uploaded, false); assert.equal(f.rows.get('row-0').dmChecks, 0); assert.equal(f.audits.length, 0);
  assert.equal(f.transactions.some(trace => trace.number === 2 && trace.retried), true);
  assert.equal(f.committed.filter(write => write.patch.uploaded === true).length, 0);
});

test('a duplicate conflict after final uniqueness reads retries and rejects the proposed commit', async () => {
  const f = fixture([receipt()], { beforeCommit: ({ number, attempt, put }) => {
    if (number === 2 && attempt === 1) put('other', receipt({ orderId: 'synthetic-other-order', uploaded: true, dmState: 'success' }));
  } });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 0); assert.equal(f.rows.get('row-0').uploaded, false);
  assert.equal(f.rows.get('other').uploaded, true); assert.equal(f.audits.length, 0);
  assert.equal(f.transactions.some(trace => trace.number === 2 && trace.retried), true);
  assert.equal(f.committed.filter(write => write.patch.uploaded === true).length, 0);
});

test('a duplicate conflict before lease commit retries without sending a provider request', async () => {
  const f = fixture([receipt()], { beforeCommit: ({ number, attempt, put }) => {
    if (number === 1 && attempt === 1) put('other', receipt({ orderId: 'synthetic-other-order', uploaded: true, dmState: 'success' }));
  } });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 0); assert.equal(f.statusCalls().length, 0);
  assert.equal(f.committed.length, 0); assert.equal(f.rows.get('row-0').dmReconcileLease, undefined);
});

test('a pre-claim refund conflict uses the freshly retried original row and preserves it', async () => {
  const f = fixture([receipt()], { beforeCommit: ({ number, attempt, rows, put }) => {
    if (number === 1 && attempt === 1) put('row-0', { ...rows.get('row-0'), refundedTotal: 27 });
  } });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 1); assert.equal(f.rows.get('row-0').refundedTotal, 27);
  assert.equal(f.statusCalls().length, 1); assert.equal(f.audits.length, 1);
});

test('unchanged non-success provider status still records a check without touching original financial or click data', async () => {
  const original = receipt(); const f = fixture([original], { diagnostic: async () => response(diagnostics('PROCESSING')) });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 0); assert.equal(result.processing, 1);
  assert.equal(f.rows.get('row-0').uploaded, false); assert.equal(f.rows.get('row-0').dmChecks, 1);
  unchangedPrivateFields(f.rows.get('row-0'), original); assert.equal(f.audits.length, 0);
});

test('a changed refund also prevents a failed status from overwriting the concurrently changed row', async () => {
  let changed;
  const f = fixture([receipt()], { diagnostic: async ({ rows, put }) => {
    changed = { ...rows.get('row-0'), refundedTotal: 27 }; put('row-0', changed); return response(diagnostics('FAILED'));
  } });
  const result = await f.api.reconcile(); assert.equal(result.failed, 0); assert.equal(result.checked, 0);
  assert.deepEqual(f.rows.get('row-0'), changed); assert.equal(f.audits.length, 0);
});

test('the upload runner also uses guarded receipt checks and never rebuilds an existing receipt event', async () => {
  const f = fixture([receipt()], { diagnostic: async ({ rows, put }) => {
    put('other', receipt({ orderId: 'synthetic-other-order', uploaded: true, dmState: 'success' })); return response(diagnostics());
  } });
  const result = await f.api.run({ retryRejected: true }); assert.equal(result.uploaded, 0); assert.equal(result.submitted, 0);
  assert.equal(f.rows.get('row-0').uploaded, false); assert.equal(f.statusCalls().length, 1); assert.equal(f.audits.length, 0);
  assert.equal(f.calls.some(call => call.url.endsWith('/events:ingest')), false);
});

test('a status rejected for a transient duplicate can be checked again after uncertainty is resolved', async () => {
  let first = true;
  const f = fixture([receipt()], { diagnostic: async ({ put }) => {
    if (first) { first = false; put('other', receipt({ orderId: 'synthetic-other-order', uploaded: true, dmState: 'success' })); }
    return response(diagnostics());
  } });
  assert.equal((await f.api.reconcile()).confirmed, 0); f.remove('other'); f.advance(91000);
  assert.equal((await f.api.reconcile()).confirmed, 1); assert.equal(f.audits.length, 1); assert.equal(f.statusCalls().length, 2);
  assert.equal(f.calls.some(call => call.url.endsWith('/events:ingest')), false);
});

test('no full private row or fingerprint is added to the receipt audit', async () => {
  const f = fixture(); await f.api.reconcile(); const audit = f.audits[0];
  assert.equal(audit.orderId, undefined); assert.equal(audit.value, undefined); assert.equal(audit.gclid, undefined);
  assert.equal(audit.refundedTotal, undefined); assert.equal(audit.customer, undefined);
  assert.equal(audit.rowFingerprint, undefined); assert.equal(audit.individualOrderConfirmed, undefined);
  assert.equal(audit.processingVerified, true);
  assert.notEqual(rowFingerprint(f.rows.get('row-0')), rowFingerprint(receipt()), 'only status fields differ after confirmation');
});
