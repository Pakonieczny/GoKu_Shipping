'use strict';

// The fixtures are a transactional in-memory Firestore adapter. Real deployed
// storage acceptance is performed separately by the authenticated sandbox API.
const assert = require('node:assert/strict');
const test = require('node:test');
const {check, KIND} = require('../../netlify/functions/_britesGrowthReceiptSandboxCheck');
const {rowFingerprint, SANDBOX_NAMESPACE} = require('../../netlify/functions/_britesGrowthReceiptReconciliation');
const epoch = Date.parse('2026-10-02T05:30:00Z');
const clone = value => value instanceof Date ? new Date(value) : Array.isArray(value) ? value.map(clone)
  : value && typeof value === 'object' ? Object.fromEntries(Object.entries(value).map(([key, item]) => [key, clone(item)])) : value;

function fixture(options = {}) {
  const rows = new Map([['Brites_GAds_ConvQueue/synthetic-production-neighbor', {synthetic: true, untouched: 'synthetic-neighbor-private'}]]);
  const reads = [], writes = [], collections = [], transactions = [];
  const env = {BRITES_GROWTH_SANDBOX: '1', BRITES_GROWTH_NAMESPACE: SANDBOX_NAMESPACE,
    FIREBASE_PRIVATE_KEY: 'synthetic-secret-never-echo', ...options.env};
  let clock = epoch, serial = Promise.resolve();
  const snapshot = (path, view = rows) => ({id: path.split('/').at(-1), ref: ref(path), exists: view.has(path), data: () => clone(view.get(path))});
  const read = async (item, view = rows) => {
    reads.push(item.path);
    if (options.onRead) await options.onRead({item, rows, env});
    if (options.readError) throw Error('Synthetic database refusal includes synthetic-secret-never-echo');
    if (item.kind === 'document') return snapshot(item.path, view);
    return {docs: [...view.keys()].filter(path => path.startsWith(item.path + '/') && !path.slice(item.path.length + 1).includes('/')
      && item.conditions.every(([field, value]) => view.get(path)[field] === value)).slice(0, item.maximum).map(path => snapshot(path, view))};
  };
  const ref = path => ({kind: 'document', path: options.wrongDocPath ? 'Brites_GAds_ConvQueue/synthetic-wrong-ref' : path,
    get: () => read({kind: 'document', path})});
  const collection = (path, conditions = [], maximum = Infinity) => ({kind: 'query', path, conditions, maximum,
    doc: id => ref(path + '/' + id), get: () => read({kind: 'query', path, conditions, maximum}),
    where: (field, operator, value) => {assert.equal(operator, '=='); assert.equal(field, 'dmRequestId'); return collection(path, [...conditions, [field, value]], maximum);},
    limit: limit => collection(path, conditions, limit)});
  const db = {collection: path => {collections.push(path); return collection(path);},
    runTransaction: callback => {
      const result = serial.then(async () => {
        const view = new Map([...rows].map(([path, row]) => [path, clone(row)])), pending = []; transactions.push(transactions.length + 1);
        const answer = await callback({get: item => read(item, view),
          create: (item, row) => {
            if (options.createError) throw Error('Synthetic create failure');
            assert.ok(!view.has(item.path)); pending.push({kind: 'create', path: item.path, row: clone(row)});
          },
          update: (item, patch) => {
            if (options.updateError) throw Error('Synthetic update failure');
            assert.ok(view.has(item.path)); pending.push({kind: 'update', path: item.path, row: clone(patch)});
          }});
        if (options.commitError) throw Error('Synthetic commit failure');
        for (const write of pending) {
          assert.match(write.path, /^Brites_Growth_Sandbox_ReceiptQueueQA[A-Za-z0-9]+\//, 'every write is inside this synthetic sandbox receipt queue');
          rows.set(write.path, write.kind === 'create' ? write.row : {...rows.get(write.path), ...write.row}); writes.push(write);
        }
        return answer;
      });
      serial = result.catch(() => {}); return result;
    }};
  const service = {namespace: options.serviceNamespace || SANDBOX_NAMESPACE, col: suffix => {
    const path = options.wrongCollectionPath || SANDBOX_NAMESPACE + '_' + suffix; collections.push(path); return collection(path);
  }};
  return {rows, reads, writes, collections, transactions, env, db, service, now: () => clock, advance: ms => {clock += ms;},
    run: extra => check({service, env, db, now: () => clock, ...extra})};
}

test('complete synthetic check exercises storage dry-run/apply/idempotency/lease/concurrency and preserves artifacts', async () => {
  const f = fixture(), productionBefore = rowFingerprint(f.rows.get('Brites_GAds_ConvQueue/synthetic-production-neighbor'));
  const result = await f.run(); assert.equal(result.ok, true); assert.equal(result.passed, true);
  assert.equal(result.synthetic, true); assert.equal(result.sandboxOnly, true); assert.equal(result.noIngestion, true);
  assert.equal(result.providerCalls, 0); assert.equal(result.productionQueueAccessed, false); assert.equal(result.individualOrderAttributionConfirmed, false);
  assert.equal(result.preservedSyntheticArtifacts, true); assert.equal(result.syntheticReceiptDocs, 3); assert.equal(result.successfulStatusTransitions, 2);
  assert.equal(result.checks.dryRun.queueUnchanged, true); assert.equal(result.checks.dryRun.wouldApply, 1);
  assert.equal(result.checks.explicitApply.applied, 1); assert.equal(result.checks.explicitApply.originalFieldsPreserved, true);
  assert.equal(result.checks.idempotency.alreadyApplied, 1); assert.equal(result.checks.activeForeignLease.code, 'ACTIVE_FOREIGN_LEASE');
  assert.deepEqual(result.checks.concurrency, {passed: true, runs: 3, applied: 1, alreadyApplied: 2, checksStored: 1});
  assert.match(result.verificationLimit, /not Google receipt or purchase-attribution evidence/);
  const records = [...f.rows].filter(([path]) => path.startsWith(result.queueName + '/')); assert.equal(records.length, 4);
  for (const [path, row] of records) {assert.equal(row.synthetic, true); assert.equal(row.syntheticTestKind, KIND); assert.equal(row.doNotIngest, true); assert.equal(row.syntheticRunId, result.syntheticRunId);}
  assert.equal(f.rows.get(result.queueName + '/foreign-lease').dmState, 'processing');
  assert.equal(f.rows.get(result.queueName + '/foreign-lease').dmChecks, 0);
  assert.equal(f.rows.get(result.queueName + '/single').dmChecks, 1); assert.equal(f.rows.get(result.queueName + '/concurrent').dmChecks, 1);
  assert.equal(f.rows.get(result.queueName + '/check-result').passed, true);
  assert.equal(rowFingerprint(f.rows.get('Brites_GAds_ConvQueue/synthetic-production-neighbor')), productionBefore);
  assert.ok(f.reads.every(path => path.startsWith(result.queueName)), 'check never reads any production reference');
  assert.ok(f.writes.every(write => write.path.startsWith(result.queueName)), 'check never writes any production reference');
});

for (const [name, env, serviceNamespace] of [
  ['production environment namespace', {BRITES_GROWTH_NAMESPACE: 'Brites_Growth_Live'}, SANDBOX_NAMESPACE],
  ['missing namespace', {BRITES_GROWTH_NAMESPACE: undefined}, SANDBOX_NAMESPACE],
  ['sandbox flag off', {BRITES_GROWTH_SANDBOX: '0'}, SANDBOX_NAMESPACE],
  ['missing sandbox flag', {BRITES_GROWTH_SANDBOX: undefined}, SANDBOX_NAMESPACE],
  ['production service namespace', {}, 'Brites_Growth_Live']
]) test(name + ' fails before seeding or touching storage', async () => {
  const f = fixture({env, serviceNamespace}), result = await f.run();
  assert.equal(result.code, 'SANDBOX_NAMESPACE_REQUIRED'); assert.equal(result.passed, false);
  assert.equal(f.collections.length, 0); assert.equal(f.reads.length, 0); assert.equal(f.writes.length, 0); assert.equal(f.rows.size, 1);
});

test('caller-supplied receipts, queue names or provider diagnostics are not accepted', async () => {
  const f = fixture();
  for (const extra of [{rows: [{dmRequestId: 'caller-request'}]}, {queueName: 'Brites_GAds_ConvQueue'}, {diagnostics: {requestStatus: 'SUCCESS'}}, {requestId: 'caller-request'}]) {
    assert.equal((await f.run(extra)).code, 'INTERNAL_CHECK_PARAMETERS_ONLY');
  }
  assert.equal(f.collections.length, 0); assert.equal(f.reads.length, 0); assert.equal(f.writes.length, 0);
});

test('missing transaction or sandbox service storage methods fail before reading rows', async () => {
  const f = fixture();
  assert.equal((await f.run({db: {}})).code, 'SANDBOX_STORAGE_UNAVAILABLE');
  assert.equal((await f.run({service: {namespace: SANDBOX_NAMESPACE}})).code, 'SANDBOX_STORAGE_UNAVAILABLE');
  assert.equal(f.reads.length, 0); assert.equal(f.writes.length, 0);
});

test('sandbox collection and document references cannot redirect to a production queue', async () => {
  const wrong = fixture({wrongCollectionPath: 'Brites_GAds_ConvQueue'}), result = await wrong.run();
  assert.equal(result.code, 'SANDBOX_QUEUE_REFERENCE_UNCONFIRMED'); assert.equal(wrong.reads.length, 0); assert.equal(wrong.writes.length, 0);
  const doc = fixture({wrongDocPath: true}), badDoc = await doc.run();
  assert.equal(badDoc.passed, false); assert.equal(badDoc.phase, 'seed'); assert.equal(doc.reads.length, 0); assert.equal(doc.writes.length, 0);
});

test('namespace changes during seed reads abort before the atomic seed writes', async () => {
  const f = fixture({onRead: ({env}) => {env.BRITES_GROWTH_NAMESPACE = 'Brites_Growth_Live';}}), result = await f.run();
  assert.equal(result.code, 'SYNTHETIC_STORAGE_CHECK_FAILED'); assert.equal(result.phase, 'seed');
  assert.equal(f.writes.length, 0); assert.equal(f.rows.size, 1); assert.equal(result.preservedSyntheticArtifacts, false);
});

for (const option of ['readError', 'createError', 'commitError']) test(option + ' leaves no partially created fixture documents', async () => {
  const f = fixture({[option]: true}), result = await f.run();
  assert.equal(result.ok, false); assert.equal(result.phase, 'seed'); assert.equal(result.preservedSyntheticArtifacts, false);
  assert.equal(f.writes.length, 0); assert.equal(f.rows.size, 1);
  assert.ok(!JSON.stringify(result).includes('synthetic-secret-never-echo'));
});

test('apply failure retains tagged synthetic fixtures and reports the exact check phase', async () => {
  const f = fixture({updateError: true}), result = await f.run();
  assert.equal(result.ok, false); assert.equal(result.phase, 'apply'); assert.equal(result.preservedSyntheticArtifacts, true);
  const synthetic = [...f.rows].filter(([path]) => path.startsWith(result.queueName + '/'));
  assert.equal(synthetic.length, 3); assert.ok(synthetic.every(([, row]) => row.synthetic && row.doNotIngest && row.dmState === 'processing'));
  assert.equal(f.writes.filter(write => write.kind === 'update').length, 0);
});

test('separate check invocations use unique sandbox collections and retain both proof artifacts', async () => {
  const f = fixture(), first = await f.run(), second = await f.run();
  assert.equal(first.ok, true); assert.equal(second.ok, true); assert.notEqual(first.queueName, second.queueName); assert.notEqual(first.syntheticRunId, second.syntheticRunId);
  assert.equal([...f.rows.keys()].filter(path => path.startsWith(first.queueName + '/')).length, 4);
  assert.equal([...f.rows.keys()].filter(path => path.startsWith(second.queueName + '/')).length, 4);
});

test('bad check clock fails before any storage access', async () => {
  const f = fixture();
  for (const now of [null, () => NaN, () => -1]) assert.equal((await f.run({now})).code, 'CHECK_CLOCK_UNAVAILABLE');
  assert.equal(f.collections.length, 0); assert.equal(f.writes.length, 0);
});

test('sanitized check result excludes credentials and receipt payloads', async () => {
  const f = fixture(), result = await f.run(), encoded = JSON.stringify(result);
  for (const secret of ['synthetic-secret-never-echo', 'synthetic-neighbor-private', 'synthetic-not-a-google-click',
    '"gclid"', '"orderId"', '"dmRequestId"', '"accountId"', '"productDestinationId"']) assert.ok(!encoded.includes(secret), secret);
});

test('complete check never invokes any external provider API', async () => {
  const previous = globalThis.fetch; let calls = 0;
  globalThis.fetch = async () => {calls++; throw Error('Synthetic check must not call any provider');};
  try {const result = await fixture().run(); assert.equal(result.ok, true); assert.equal(result.noIngestion, true); assert.equal(calls, 0);}
  finally {globalThis.fetch = previous;}
});
