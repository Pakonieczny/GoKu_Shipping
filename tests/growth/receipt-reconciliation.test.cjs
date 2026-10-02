'use strict';

// Synthetic sandbox data only. This helper has no Google client or real writes.
const assert = require('node:assert/strict');
const test = require('node:test');
const {createReceiptReconciliation, rowFingerprint, DEFAULT_QUEUE, SANDBOX_NAMESPACE,
  MAX_BATCH, MAX_EVIDENCE_AGE_MS, MAX_PLAN_TTL_MS, STATUS_FIELDS} = require('../../netlify/functions/_britesGrowthReceiptReconciliation');
const epoch = Date.parse('2026-10-02T05:00:00Z');
const target = {operatingAccount: {accountType: 'GOOGLE_ADS', accountId: '123'}, productDestinationId: '456',
  loginAccount: {accountType: 'GOOGLE_ADS', accountId: '999'}};
const clone = value => value instanceof Date ? new Date(value) : Buffer.isBuffer(value) ? Buffer.from(value)
  : Array.isArray(value) ? value.map(clone) : value && typeof value === 'object'
    ? Object.fromEntries(Object.entries(value).map(([key, item]) => [key, clone(item)])) : value;
const receipt = over => ({orderId: 'synthetic-order-private', value: 54, refundedTotal: 0, currency: 'USD',
  gclid: 'synthetic-click-private', conversionDateTime: '2026-09-17 10:00:00-04:00', uploaded: false, failed: false,
  dmState: 'processing', dmRequestId: 'synthetic-request-private', dmDestination: clone(target),
  dmSubmittedAt: epoch - 15 * 86400000, dmChecks: 0, dmCheckedAt: 0, dmReconcileLease: null,
  dmReconcileLeaseUntil: 0, ...over});
const diagnostics = over => ({requestStatusPerDestination: [{destination: clone(target), requestStatus: 'SUCCESS',
  eventsIngestionStatus: {recordCount: '1'}, ...over}]});

function fixture(initial = [receipt()], options = {}) {
  const rows = new Map(initial.map((row, index) => ['row-' + index, clone(row)]));
  const reads = [], writes = [], transactions = [], collections = [];
  const env = {BRITES_GROWTH_SANDBOX: '1', BRITES_GROWTH_NAMESPACE: SANDBOX_NAMESPACE, ...options.env};
  const queueName = options.config?.queueName || DEFAULT_QUEUE;
  let clock = epoch, revision = 0, serial = Promise.resolve(), commits = Promise.resolve();
  const advance = value => {clock += value;};
  const change = (id, row) => {if (row === undefined) rows.delete(id); else rows.set(id, clone(row)); revision++;};
  const snapshot = (id, view = rows) => ({id, ref: ref(id), exists: view.has(id), data: () => clone(view.get(id))});
  const ref = id => ({kind: 'document', id, path: options.wrongDocPath ? 'Brites_GAds_ConvQueue/' + id : queueName + '/' + id});
  function query(path, conditions = [], maximum = Infinity) {
    return {kind: 'query', path, conditions, maximum,
      doc: id => ref(id),
      where: (field, operator, value) => {assert.equal(field, 'dmRequestId'); assert.equal(operator, '=='); return query(path, [...conditions, [field, value]], maximum);},
      limit: value => {assert.equal(value, 2); return query(path, conditions, value);}};
  }
  async function run(fn) {
    if (options.beforeTransaction) await options.beforeTransaction({rows, change, advance, env});
    for (let attempt = 1; attempt <= 8; attempt++) {
      const startedRevision = revision, view = new Map([...rows].map(([id, row]) => [id, clone(row)])), pending = [];
      const index = transactions.length + 1; transactions.push({index, attempt});
      const result = await fn({
        get: async item => {
          reads.push({kind: item.kind, path: item.path, id: item.id, conditions: clone(item.conditions)});
          if (options.onRead) await options.onRead({item, index, attempt, rows, change, advance, env});
          if (item.kind === 'document') return snapshot(item.id, view);
          const docs = [...view.keys()].filter(id => item.conditions.every(([field, value]) => view.get(id)[field] === value))
            .slice(0, item.maximum).map(id => snapshot(id, view));
          return {docs};
        },
        update: (item, patch) => {
          assert.ok(item.path.startsWith(SANDBOX_NAMESPACE + '_'), 'only isolated sandbox documents can be updated');
          assert.ok(Object.keys(patch).every(field => STATUS_FIELDS.includes(field)), 'only predetermined receipt status fields');
          pending.push([item.id, clone(patch)]);
        }
      });
      const commit = commits.then(async () => {
        if (options.beforeCommit) await options.beforeCommit({index, attempt, rows, pending, change, advance, env});
        if (options.failTransaction) throw Error('Synthetic transaction failure; no writes committed');
        if (pending.length && startedRevision !== revision) return {retry: true};
        for (const [id, patch] of pending) {rows.set(id, {...rows.get(id), ...patch}); writes.push({id, patch}); revision++;}
        return {result};
      });
      commits = commit.catch(() => {});
      const outcome = await commit;
      if (!outcome.retry) return outcome.result;
    }
    throw Error('Synthetic transaction conflict limit');
  }
  const db = {collection: path => {collections.push(path); return query(options.wrongCollectionPath || path);},
    runTransaction: fn => {
      if (options.concurrent) return run(fn);
      const current = serial.then(() => run(fn)); serial = current.catch(() => {}); return current;
    }};
  const service = options.service ? {namespace: options.serviceNamespace || SANDBOX_NAMESPACE,
    col: suffix => {collections.push(SANDBOX_NAMESPACE + '_' + suffix); return query(options.servicePath || SANDBOX_NAMESPACE + '_' + suffix);}} : undefined;
  const api = createReceiptReconciliation({env, db, service, now: () => clock, ...options.config});
  const proof = (id = 'row-0', over = {}) => ({rowId: id, requestId: rows.get(id)?.dmRequestId,
    observedAt: clock - 1000, destination: clone(rows.get(id)?.dmDestination), diagnostics: diagnostics(),
    individualOrderConfirmed: false, ...over});
  const input = () => ({rows: [...rows].map(([id, row]) => ({id, row: clone(row)})), evidence: [...rows.keys()].map(id => proof(id))});
  return {api, rows, reads, writes, transactions, collections, env, advance, change, proof, input};
}
const refusedCode = plan => plan.entries[0]?.code || plan.code;

test('planner does no I/O and dry-run rechecks saved evidence without any write', async () => {
  const f = fixture(), before = clone([...f.rows]), plan = f.api.plan(f.input());
  assert.equal(plan.ok, true); assert.equal(plan.eligible, 1); assert.equal(plan.dryRun, true);
  assert.equal(f.reads.length, 0); assert.equal(f.collections.length, 0); assert.equal(f.transactions.length, 0);
  const result = await f.api.apply(plan);
  assert.equal(result.ok, true); assert.equal(result.dryRun, true); assert.equal(result.wouldApply, 1);
  assert.equal(result.queueUpdated, false); assert.equal(result.applied, 0); assert.equal(f.writes.length, 0);
  assert.deepEqual([...f.rows], before); assert.equal(result.individualOrderAttributionConfirmed, false);
  assert.equal(result.individualOrdersUpdated, 0); assert.equal(result.eventsIngested, 0);
});

test('explicit sandbox apply updates receipt status and preserves original sale and destination', async () => {
  const f = fixture(), original = clone(f.rows.get('row-0')), plan = f.api.plan(f.input());
  const result = await f.api.apply(plan, {dryRun: false}), saved = f.rows.get('row-0');
  assert.equal(result.ok, true); assert.equal(result.applied, 1); assert.equal(result.queueUpdated, true);
  assert.equal(result.individualOrdersUpdated, 0); assert.equal(result.individualOrderAttributionConfirmed, false);
  assert.equal(saved.uploaded, true); assert.equal(saved.failed, false); assert.equal(saved.dmState, 'success');
  assert.equal(saved.dmChecks, 1); assert.equal(saved.dmCheckedAt, epoch - 1000);
  assert.equal(saved.dmProviderReceiptConfirmed, true); assert.equal(saved.dmLastStatus, 'SUCCESS');
  for (const field of Object.keys(original).filter(field => !STATUS_FIELDS.includes(field))) assert.deepEqual(saved[field], original[field], field);
  assert.equal(saved.uploadedAt, undefined, 'receipt observation cannot invent the original upload time');
  assert.equal(saved.individualOrderConfirmed, undefined);
  assert.equal(saved.dmIndividualOrderConfirmed, undefined);
});

test('public plans and results contain hashes rather than original order/account/request data', async () => {
  const f = fixture(), plan = f.api.plan(f.input()), result = await f.api.apply(plan);
  const encoded = JSON.stringify({plan, result});
  for (const value of ['synthetic-order-private', 'synthetic-click-private', 'synthetic-request-private', '"row-0"',
    '"accountId"', '"productDestinationId"', '"orderId"', '"gclid"', '"value"', '"currency"', '"requestId"']) assert.ok(!encoded.includes(value), value);
});

test('server-side evidence warnings remain visible without rejecting one-record success', async () => {
  const f = fixture(), input = f.input();
  input.evidence[0].diagnostics = diagnostics({warningInfo: {warningCounts: [{reason: 'PROCESSING_WARNING_REASON_UNSPECIFIED', recordCount: '1'}]}});
  const plan = f.api.plan(input); assert.equal(plan.ok, true);
  assert.equal((await f.api.apply(plan, {dryRun: false})).applied, 1);
  assert.deepEqual(f.rows.get('row-0').dmWarnings, ['PROCESSING_WARNING_REASON_UNSPECIFIED']);
});

test('legacy Google Ads destination product spelling remains exact and supported', async () => {
  const legacy = clone(target); legacy.operatingAccount.product = 'GOOGLE_ADS'; delete legacy.operatingAccount.accountType;
  const f = fixture([receipt({dmDestination: legacy})]), plan = f.api.plan(f.input());
  assert.equal(plan.ok, true); assert.equal((await f.api.apply(plan, {dryRun: false})).applied, 1);
  assert.deepEqual(f.rows.get('row-0').dmDestination, legacy);
});

for (const [name, env] of [
  ['sandbox flag missing', {BRITES_GROWTH_SANDBOX: undefined}],
  ['sandbox flag off', {BRITES_GROWTH_SANDBOX: '0'}],
  ['production namespace', {BRITES_GROWTH_NAMESPACE: 'Brites_Growth_Live'}],
  ['namespace absent', {BRITES_GROWTH_NAMESPACE: undefined}]
]) test(name + ' blocks planning and applying before storage access', async () => {
  const f = fixture(undefined, {env}), plan = f.api.plan(f.input()), result = await f.api.apply(plan, {dryRun: false});
  assert.equal(plan.code, 'SANDBOX_NAMESPACE_REQUIRED'); assert.equal(result.code, 'SANDBOX_NAMESPACE_REQUIRED');
  assert.equal(f.collections.length, 0); assert.equal(f.transactions.length, 0); assert.equal(f.writes.length, 0);
});

for (const queueName of ['Brites_GAds_ConvQueue', 'Brites_Growth_Live_ReceiptQueue', 'Brites_Growth_Sandbox_Products', 'Brites_Growth_Sandbox_State', 'Brites_Growth_Sandbox_ReceiptQueue/production',
  'Brites_Growth_Sandbox', 'Brites_Growth_Sandbox_../Brites_GAds_ConvQueue']) test('unsafe queue name is hard blocked: ' + queueName, async () => {
  const f = fixture(undefined, {config: {queueName}}), plan = f.api.plan(f.input());
  assert.equal(plan.code, 'SANDBOX_QUEUE_REQUIRED'); assert.equal((await f.api.apply(plan, {dryRun: false})).code, 'SANDBOX_QUEUE_REQUIRED');
  assert.equal(f.collections.length, 0); assert.equal(f.writes.length, 0);
});

test('sandbox service namespace and actual collection/document paths must agree', async () => {
  const unsafe = fixture(undefined, {service: true, serviceNamespace: 'Brites_Growth_Live'});
  assert.equal(unsafe.api.plan(unsafe.input()).code, 'SANDBOX_NAMESPACE_REQUIRED'); assert.equal(unsafe.collections.length, 0);
  for (const options of [{service: true, servicePath: 'Brites_GAds_ConvQueue'}, {wrongCollectionPath: 'Brites_GAds_ConvQueue'}, {wrongDocPath: true}]) {
    const f = fixture(undefined, options), plan = f.api.plan(f.input()), result = await f.api.apply(plan, {dryRun: false});
    assert.equal(result.ok, false); assert.match(result.code, /SANDBOX_(QUEUE|ROW)_REFERENCE_UNCONFIRMED/); assert.equal(f.writes.length, 0);
  }
  const safe = fixture(undefined, {service: true}), plan = safe.api.plan(safe.input());
  assert.equal((await safe.api.apply(plan, {dryRun: false})).applied, 1);
});

for (const [name, change, code] of [
  ['wrong saved request', input => {input.evidence[0].requestId = 'other-request';}, 'REQUEST_ID_CHANGED'],
  ['wrong saved destination', input => {input.evidence[0].destination.productDestinationId = '457';}, 'SAVED_DESTINATION_CHANGED'],
  ['changed saved manager', input => {input.evidence[0].destination.loginAccount.accountId = '998';}, 'SAVED_DESTINATION_CHANGED'],
  ['wrong provider account', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].destination.operatingAccount.accountId = '124';}, 'PROVIDER_DESTINATION_UNCONFIRMED'],
  ['wrong provider action', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].destination.productDestinationId = '457';}, 'PROVIDER_DESTINATION_UNCONFIRMED'],
  ['unsupported saved account type', input => {input.rows[0].row.dmDestination.operatingAccount.accountType = 'DISPLAY_VIDEO';}, 'SAVED_DESTINATION_UNSUPPORTED'],
  ['conflicting legacy account type', input => {input.rows[0].row.dmDestination.operatingAccount.product = 'DISPLAY_VIDEO';}, 'SAVED_DESTINATION_UNSUPPORTED'],
  ['missing provider destination', input => {delete input.evidence[0].diagnostics.requestStatusPerDestination[0].destination;}, 'PROVIDER_DESTINATION_UNCONFIRMED'],
  ['unsupported provider account type', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].destination.operatingAccount.accountType = 'DISPLAY_VIDEO';}, 'PROVIDER_DESTINATION_UNCONFIRMED'],
  ['zero events', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].eventsIngestionStatus.recordCount = '0';}, 'ONE_RECORD_REQUIRED'],
  ['bulk fifteen events', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].eventsIngestionStatus.recordCount = '15';}, 'ONE_RECORD_REQUIRED'],
  ['missing record count', input => {delete input.evidence[0].diagnostics.requestStatusPerDestination[0].eventsIngestionStatus.recordCount;}, 'ONE_RECORD_REQUIRED'],
  ['processing receipt', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].requestStatus = 'PROCESSING';}, 'PROVIDER_SUCCESS_REQUIRED'],
  ['partial success', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].requestStatus = 'PARTIAL_SUCCESS';}, 'PROVIDER_SUCCESS_REQUIRED'],
  ['provider failure', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].requestStatus = 'FAILURE';}, 'PROVIDER_SUCCESS_REQUIRED'],
  ['success with errors', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].errorInfo = {errorCounts: [{reason: 'PROCESSING_ERROR_REASON_INVALID_GCLID', recordCount: '1'}]};}, 'PROVIDER_ERRORS_PRESENT'],
  ['malformed errors', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].errorInfo = {errorCounts: {count: 1}};}, 'ERROR_DIAGNOSTICS_UNCONFIRMED'],
  ['unknown error summary shape', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].errorInfo = {failedRecords: 1};}, 'ERROR_DIAGNOSTICS_UNCONFIRMED'],
  ['audience receipt mixed with event status', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].audienceMembersIngestionStatus = {recordCount: '1'};}, 'UNSUPPORTED_RECEIPT_STATUS_KIND'],
  ['private text masquerading as warning', input => {input.evidence[0].diagnostics.requestStatusPerDestination[0].warningInfo = {warningCounts: [{reason: 'private buyer text'}]};}, 'WARNING_DIAGNOSTICS_UNCONFIRMED'],
  ['duplicate provider destination', input => {input.evidence[0].diagnostics.requestStatusPerDestination.push(clone(input.evidence[0].diagnostics.requestStatusPerDestination[0]));}, 'SINGLE_DESTINATION_REQUIRED'],
  ['additional provider destination', input => {input.evidence[0].diagnostics.requestStatusPerDestination.push({destination: {operatingAccount: {accountType: 'DISPLAY_VIDEO', accountId: '123'}}, requestStatus: 'SUCCESS'});}, 'SINGLE_DESTINATION_REQUIRED'],
  ['order confirmation claim', input => {input.evidence[0].individualOrderConfirmed = true;}, 'ATTRIBUTION_CLAIM_UNSUPPORTED'],
  ['attribution claim', input => {input.evidence[0].attributionConfirmed = true;}, 'ATTRIBUTION_CLAIM_UNSUPPORTED'],
  ['claim in provider payload', input => {input.evidence[0].diagnostics.individualOrderConfirmed = true;}, 'ATTRIBUTION_CLAIM_UNSUPPORTED'],
  ['claim in saved row', input => {input.rows[0].row.dmIndividualOrderConfirmed = true;}, 'ATTRIBUTION_CLAIM_UNSUPPORTED'],
  ['expired evidence', input => {input.evidence[0].observedAt = epoch - MAX_EVIDENCE_AGE_MS;}, 'EVIDENCE_EXPIRED'],
  ['future evidence', input => {input.evidence[0].observedAt = epoch + 1;}, 'EVIDENCE_TIME_UNCONFIRMED'],
  ['evidence before submission', input => {input.rows[0].row.dmSubmittedAt = epoch - 500;}, 'EVIDENCE_PRECEDES_SUBMISSION'],
  ['newer saved check', input => {input.rows[0].row.dmCheckedAt = epoch - 100;}, 'NEWER_ROW_CHECK'],
  ['expired numeric row', input => {input.rows[0].row.expiresAtMs = epoch;}, 'ROW_EXPIRED'],
  ['expired timestamp row', input => {input.rows[0].row.expireAt = {toMillis: () => epoch - 100, seconds: 1, nanoseconds: 0};}, 'ROW_EXPIRED'],
  ['invalid expiry', input => {input.rows[0].row.expiresAt = 'unknown';}, 'ROW_EXPIRY_UNCONFIRMED'],
  ['active receipt lease', input => {input.rows[0].row.dmReconcileLease = 'foreign-worker'; input.rows[0].row.dmReconcileLeaseUntil = epoch + 60000;}, 'ACTIVE_FOREIGN_LEASE'],
  ['active service lease', input => {input.rows[0].row.leaseOwner = 'foreign-worker'; input.rows[0].row.leaseUntil = epoch + 60000;}, 'ACTIVE_FOREIGN_LEASE'],
  ['malformed lease', input => {input.rows[0].row.dmReconcileLeaseUntil = 'unknown';}, 'LEASE_UNCONFIRMED'],
  ['terminal success', input => {input.rows[0].row.uploaded = true; input.rows[0].row.dmState = 'success';}, 'ROW_NOT_PENDING_RECEIPT'],
  ['terminal failure', input => {input.rows[0].row.failed = true; input.rows[0].row.dmState = 'failed';}, 'ROW_NOT_PENDING_RECEIPT'],
  ['unknown submission', input => {input.rows[0].row.dmState = 'submission_unknown';}, 'ROW_NOT_PENDING_RECEIPT'],
  ['unsent row', input => {delete input.rows[0].row.dmRequestId;}, 'SAVED_REQUEST_ID_INVALID'],
  ['production row marker', input => {input.rows[0].row.namespace = 'Brites_Growth_Live';}, 'ROW_NAMESPACE_UNSAFE'],
  ['invalid check counter', input => {input.rows[0].row.dmChecks = Number.MAX_SAFE_INTEGER;}, 'SAVED_CHECK_COUNT_INVALID']
]) test(name + ' refuses planning without writes', async () => {
  const f = fixture(), input = f.input(); change(input); const plan = f.api.plan(input);
  assert.equal(plan.ok, false); assert.equal(refusedCode(plan), code);
  const before = clone([...f.rows]); assert.equal((await f.api.apply(plan, {dryRun: false})).ok, false);
  assert.equal(f.writes.length, 0); assert.deepEqual([...f.rows], before); assert.equal(f.transactions.length, 0);
});

test('duplicate row, request and evidence IDs cannot form an applicable plan', async () => {
  const duplicateRows = fixture(), input = duplicateRows.input(); input.rows.push(clone(input.rows[0])); input.evidence.push({...clone(input.evidence[0]), rowId: 'row-other'});
  assert.equal(duplicateRows.api.plan(input).code, 'DUPLICATE_ROW_ID');
  const duplicateReceipts = fixture([receipt(), receipt({orderId: 'other-order'})]); assert.equal(duplicateReceipts.api.plan(duplicateReceipts.input()).code, 'DUPLICATE_REQUEST_ID');
  const f = fixture([receipt(), receipt({dmRequestId: 'other-request'})]), evidenceInput = f.input();
  evidenceInput.evidence[1].rowId = 'row-0'; assert.equal(f.api.plan(evidenceInput).code, 'DUPLICATE_EVIDENCE_ROW_ID');
  evidenceInput.evidence[1].rowId = 'row-1'; evidenceInput.evidence[1].requestId = evidenceInput.evidence[0].requestId;
  assert.equal(f.api.plan(evidenceInput).code, 'DUPLICATE_EVIDENCE_REQUEST_ID');
  assert.equal(f.writes.length, 0);
});

test('uniquely named sandbox receipt queues are supported without sharing other sandbox data', async () => {
  const queueName = DEFAULT_QUEUE + 'SyntheticQA20261002', f = fixture(undefined, {config: {queueName}});
  const plan = f.api.plan(f.input()); assert.equal(plan.queueName, queueName);
  assert.equal((await f.api.apply(plan, {dryRun: false})).applied, 1);
  assert.deepEqual(f.collections, [queueName]);
});

test('an expired foreign lease is not renewed or removed when receipt status is applied', async () => {
  const f = fixture([receipt({dmReconcileLease: 'expired-foreign-worker', dmReconcileLeaseUntil: epoch - 1,
    uploadError: 'Earlier unresolved diagnostic retained for review'})]), plan = f.api.plan(f.input());
  assert.equal((await f.api.apply(plan, {dryRun: false})).applied, 1);
  assert.equal(f.rows.get('row-0').dmReconcileLease, 'expired-foreign-worker');
  assert.equal(f.rows.get('row-0').dmReconcileLeaseUntil, epoch - 1);
  assert.equal(f.rows.get('row-0').uploadError, 'Earlier unresolved diagnostic retained for review');
});

test('plan validation bounds batch size and requires exact supplied row/evidence coverage', () => {
  const f = fixture(), input = f.input();
  assert.equal(f.api.plan({rows: input.rows, evidence: []}).code, 'EXACT_ROW_EVIDENCE_REQUIRED');
  assert.equal(f.api.plan({...input, force: true}).code, 'PLAN_INPUT_INVALID');
  const excess = Array.from({length: MAX_BATCH + 1}, (_, i) => ({id: 'row-' + i, row: receipt({dmRequestId: 'request-' + i})}));
  assert.equal(f.api.plan({rows: excess, evidence: excess.map(entry => f.proof(entry.id))}).code, 'PLAN_INPUT_INVALID');
  input.evidence[0].rowId = 'row-other'; assert.equal(f.api.plan(input).code, 'EXACT_ROW_EVIDENCE_REQUIRED');
  input.rows[0].id = '../production'; assert.equal(f.api.plan(input).code, 'SAVED_ROW_ID_INVALID');
});

test('opaque frozen plan cannot be recreated, edited, transplanted or given arbitrary apply fields', async () => {
  const f = fixture(), plan = f.api.plan(f.input()); assert.equal(Object.isFrozen(plan), true);
  assert.equal(Object.isFrozen(plan.entries[0]), true);
  assert.equal((await f.api.apply(JSON.parse(JSON.stringify(plan)), {dryRun: false})).code, 'PLAN_NOT_ISSUED_BY_THIS_INSTANCE');
  assert.equal((await fixture().api.apply(plan, {dryRun: false})).code, 'PLAN_NOT_ISSUED_BY_THIS_INSTANCE');
  assert.equal((await f.api.apply(plan, {dryRun: false, requestId: 'caller-request'})).code, 'APPLY_OPTIONS_INVALID');
  assert.equal((await f.api.apply(plan, {dryRun: 'false'})).code, 'APPLY_OPTIONS_INVALID');
  assert.equal(f.transactions.length, 0); assert.equal(f.writes.length, 0);
});

test('mutating input snapshots after planning cannot change the issued status patch', async () => {
  const f = fixture(), input = f.input(), plan = f.api.plan(input);
  input.rows[0].row.value = 9000; input.rows[0].row.dmRequestId = 'caller-replacement';
  input.evidence[0].diagnostics.requestStatusPerDestination[0].eventsIngestionStatus.recordCount = '15';
  assert.equal((await f.api.apply(plan, {dryRun: false})).applied, 1);
  assert.equal(f.rows.get('row-0').value, 54); assert.equal(f.rows.get('row-0').dmRequestId, 'synthetic-request-private');
});

for (const [name, patch, expected] of [
  ['request ID', {dmRequestId: 'replacement-request'}, 'ROW_CHANGED'],
  ['destination', {dmDestination: {...clone(target), productDestinationId: '457'}}, 'ROW_CHANGED'],
  ['sale value', {value: 99}, 'ROW_CHANGED'],
  ['refund', {refundedTotal: 54}, 'ROW_CHANGED'],
  ['extra row field', {independentChange: true}, 'ROW_CHANGED'],
  ['active lease', {dmReconcileLease: 'new-foreign-worker', dmReconcileLeaseUntil: epoch + 60000}, 'ACTIVE_FOREIGN_LEASE'],
  ['terminal failure', {failed: true, dmState: 'failed'}, 'ROW_NOT_PENDING_RECEIPT'],
  ['terminal success from another worker', {uploaded: true, dmState: 'success'}, 'ROW_NOT_PENDING_RECEIPT'],
  ['expired row', {expiresAtMs: epoch - 1}, 'ROW_EXPIRED'],
  ['claimed order attribution', {individualOrderConfirmed: true}, 'ATTRIBUTION_CLAIM_UNSUPPORTED']
]) test('transaction refuses a changed ' + name + ' and retains that independent update', async () => {
  const f = fixture(), plan = f.api.plan(f.input()); f.change('row-0', {...f.rows.get('row-0'), ...patch});
  const before = clone([...f.rows]), result = await f.api.apply(plan, {dryRun: false});
  assert.equal(result.ok, false); assert.equal(result.code, expected); assert.equal(f.writes.length, 0); assert.deepEqual([...f.rows], before);
});

test('removed row or duplicate receipt outside supplied snapshot blocks transactional apply', async () => {
  const gone = fixture(), plan = gone.api.plan(gone.input()); gone.change('row-0', undefined);
  assert.equal((await gone.api.apply(plan, {dryRun: false})).code, 'SAVED_ROW_MISSING');
  const f = fixture(), duplicatePlan = f.api.plan(f.input());
  f.change('hidden-terminal-row', receipt({uploaded: true, dmState: 'success', orderId: 'other-order'}));
  const before = clone([...f.rows]);
  assert.equal((await f.api.apply(duplicatePlan, {dryRun: false})).code, 'DUPLICATE_OR_UNCONFIRMED_SAVED_REQUEST');
  assert.equal(f.writes.length, 0); assert.deepEqual([...f.rows], before);
});

test('all planned rows are read before writing; one changed row aborts the whole batch', async () => {
  const f = fixture([receipt(), receipt({dmRequestId: 'other-request', orderId: 'other-order'})]), plan = f.api.plan(f.input());
  f.change('row-1', {...f.rows.get('row-1'), value: 100}); const before = clone([...f.rows]);
  const result = await f.api.apply(plan, {dryRun: false});
  assert.equal(result.ok, false); assert.equal(result.applied, 0); assert.equal(f.writes.length, 0); assert.deepEqual([...f.rows], before);
});

test('expired plan, evidence and row lifetime cannot be extended by caller options', async () => {
  const f = fixture(), plan = f.api.plan(f.input()); f.advance(MAX_PLAN_TTL_MS);
  assert.equal((await f.api.apply(plan, {dryRun: false})).code, 'PLAN_EXPIRED'); assert.equal(f.transactions.length, 0);
  const old = fixture(), input = old.input(); input.evidence[0].observedAt = epoch - MAX_EVIDENCE_AGE_MS + 50;
  const short = old.api.plan(input); assert.equal(short.expiresAt, epoch + 50); old.advance(50);
  assert.equal((await old.api.apply(short, {dryRun: false})).code, 'PLAN_EXPIRED');
  const expires = fixture([receipt({expiresAt: new Date(epoch + 100)})]), rowPlan = expires.api.plan(expires.input());
  assert.equal(rowPlan.expiresAt, epoch + 100); expires.advance(100);
  assert.equal((await expires.api.apply(rowPlan, {dryRun: false})).code, 'PLAN_EXPIRED');
});

test('read delays consume plan lifetime and cannot let a late transaction write', async () => {
  const f = fixture(undefined, {onRead: ({advance}) => advance(MAX_PLAN_TTL_MS)}), plan = f.api.plan(f.input()), before = clone([...f.rows]);
  assert.equal((await f.api.apply(plan, {dryRun: false})).code, 'PLAN_EXPIRED'); assert.equal(f.writes.length, 0); assert.deepEqual([...f.rows], before);
});

test('environment becoming live between planning and applying or during a transaction blocks writes', async () => {
  const f = fixture(), plan = f.api.plan(f.input()); f.env.BRITES_GROWTH_NAMESPACE = 'Brites_Growth_Live';
  assert.equal((await f.api.apply(plan, {dryRun: false})).code, 'SANDBOX_NAMESPACE_REQUIRED'); assert.equal(f.transactions.length, 0);
  const midway = fixture(undefined, {onRead: ({env}) => {env.BRITES_GROWTH_SANDBOX = '0';}}), latePlan = midway.api.plan(midway.input());
  assert.equal((await midway.api.apply(latePlan, {dryRun: false})).code, 'SANDBOX_NAMESPACE_REQUIRED'); assert.equal(midway.writes.length, 0);
});

test('applying the same plan twice is idempotent and does not increment status checks twice', async () => {
  const f = fixture(), plan = f.api.plan(f.input());
  assert.equal((await f.api.apply(plan, {dryRun: false})).applied, 1); const saved = clone(f.rows.get('row-0'));
  const repeat = await f.api.apply(plan, {dryRun: false});
  assert.equal(repeat.ok, true); assert.equal(repeat.applied, 0); assert.equal(repeat.alreadyApplied, 1); assert.equal(repeat.queueUpdated, false);
  assert.equal(f.writes.length, 1); assert.deepEqual(f.rows.get('row-0'), saved);
  const review = await f.api.apply(plan); assert.equal(review.alreadyApplied, 1); assert.equal(review.wouldApply, 0);
});

test('concurrent same-plan transactions retry safely and commit one status update', {timeout: 3000}, async () => {
  const f = fixture(undefined, {concurrent: true}), plan = f.api.plan(f.input());
  const outcomes = await Promise.all(Array.from({length: 6}, () => f.api.apply(plan, {dryRun: false})));
  assert.ok(outcomes.every(outcome => outcome.ok)); assert.equal(outcomes.reduce((sum, outcome) => sum + outcome.applied, 0), 1);
  assert.equal(outcomes.reduce((sum, outcome) => sum + outcome.alreadyApplied, 0), 5);
  assert.equal(f.writes.length, 1); assert.equal(f.rows.get('row-0').dmChecks, 1);
  assert.ok(f.transactions.some(transaction => transaction.attempt > 1), 'fixture exercised a transaction retry');
});

test('concurrent different plans cannot overwrite another plan successful result', {timeout: 3000}, async () => {
  const f = fixture(undefined, {concurrent: true}), first = f.api.plan(f.input()), second = f.api.plan(f.input());
  const outcomes = await Promise.all([f.api.apply(first, {dryRun: false}), f.api.apply(second, {dryRun: false})]);
  assert.equal(outcomes.filter(outcome => outcome.ok).length, 1); assert.equal(outcomes.reduce((sum, outcome) => sum + outcome.applied, 0), 1);
  assert.equal(f.writes.length, 1); assert.equal(f.rows.get('row-0').dmChecks, 1);
});

test('transaction conflict detects an in-flight sale change and leaves status pending', async () => {
  let changed = false;
  const f = fixture(undefined, {concurrent: true, beforeCommit: ({pending, rows, change}) => {
    if (!changed && pending.length) {changed = true; change('row-0', {...rows.get('row-0'), refundedTotal: 54});}
  }}), plan = f.api.plan(f.input());
  const result = await f.api.apply(plan, {dryRun: false});
  assert.equal(result.ok, false); assert.equal(result.code, 'ROW_CHANGED'); assert.equal(f.writes.length, 0);
  assert.equal(f.rows.get('row-0').refundedTotal, 54); assert.equal(f.rows.get('row-0').dmState, 'processing');
  assert.ok(f.transactions.some(transaction => transaction.attempt > 1));
});

test('an independently changed row cannot be hidden behind this plan idempotency marker', async () => {
  const f = fixture(), plan = f.api.plan(f.input()); await f.api.apply(plan, {dryRun: false});
  f.change('row-0', {...f.rows.get('row-0'), value: 500}); const before = clone([...f.rows]);
  assert.equal((await f.api.apply(plan, {dryRun: false})).ok, false); assert.equal(f.writes.length, 1); assert.deepEqual([...f.rows], before);
});

test('transaction failure commits no status patch and retains the saved receipt', async () => {
  const f = fixture(undefined, {failTransaction: true}), plan = f.api.plan(f.input()), before = clone([...f.rows]);
  const result = await f.api.apply(plan, {dryRun: false});
  assert.equal(result.code, 'SANDBOX_TRANSACTION_FAILED'); assert.equal(result.queueUpdated, false); assert.equal(f.writes.length, 0); assert.deepEqual([...f.rows], before);
});

test('fingerprinting ignores object key order but detects typed dates and sub-millisecond timestamp changes', () => {
  assert.equal(rowFingerprint({b: 2, a: 1}), rowFingerprint({a: 1, b: 2}));
  assert.notEqual(rowFingerprint({at: new Date(epoch)}), rowFingerprint({at: epoch}));
  const stamp = nanoseconds => ({seconds: 100, nanoseconds, toMillis: () => 100000});
  assert.notEqual(rowFingerprint({at: stamp(1)}), rowFingerprint({at: stamp(2)}));
});

test('cyclic or excessively deep rows fail closed without touching storage', () => {
  const f = fixture(), input = f.input(); input.rows[0].row.cycle = input.rows[0].row;
  assert.equal(refusedCode(f.api.plan(input)), 'ROW_FINGERPRINT_UNAVAILABLE');
  const another = f.input(); let item = another.rows[0].row;
  for (let i = 0; i < 30; i++) {item.child = {}; item = item.child;}
  assert.equal(refusedCode(f.api.plan(another)), 'ROW_FINGERPRINT_UNAVAILABLE'); assert.equal(f.transactions.length, 0);
});

test('reconciliation makes no provider call, even when applying a successful receipt', async () => {
  const previous = globalThis.fetch; let calls = 0;
  globalThis.fetch = async () => {calls++; throw Error('No provider operation is allowed from this helper');};
  try {const f = fixture(), plan = f.api.plan(f.input()); assert.equal((await f.api.apply(plan, {dryRun: false})).applied, 1); assert.equal(calls, 0);}
  finally {globalThis.fetch = previous;}
});
