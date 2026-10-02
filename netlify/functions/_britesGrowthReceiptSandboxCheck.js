'use strict';

// Protected internal check, not an endpoint. Every row and diagnostic below is
// generated here and tagged synthetic. No provider request is made. The parent
// caller may compare separate read-only production health before/after this check.
const crypto = require('node:crypto');
const {createReceiptReconciliation, rowFingerprint, SANDBOX_NAMESPACE, DEFAULT_QUEUE,
  STATUS_FIELDS} = require('./_britesGrowthReceiptReconciliation');
const KIND = 'receipt_status_reconciliation_storage_check';
const PARAMS = new Set(['service', 'env', 'db', 'now']);

async function check(options = {}) {
  const base = {schemaVersion: 1, synthetic: true, sandboxOnly: true, receiptOnly: true,
    evidenceKind: 'synthetic_firestore_transaction_integration', noIngestion: true,
    providerCalls: 0, productionQueueAccessed: false, individualOrderAttributionConfirmed: false,
    ok: false, passed: false, preservedSyntheticArtifacts: false};
  const denied = code => ({...base, blocked: true, code});
  if (!options || typeof options !== 'object' || Array.isArray(options) || Object.keys(options).some(key => !PARAMS.has(key))) return denied('INTERNAL_CHECK_PARAMETERS_ONLY');
  const {service, env = {}, db, now = Date.now} = options;
  const scopeSafe = () => env.BRITES_GROWTH_SANDBOX === '1' && env.BRITES_GROWTH_NAMESPACE === SANDBOX_NAMESPACE
    && service?.namespace === SANDBOX_NAMESPACE;
  if (!scopeSafe()) return denied('SANDBOX_NAMESPACE_REQUIRED');
  if (typeof service.col !== 'function' || typeof db?.runTransaction !== 'function') return denied('SANDBOX_STORAGE_UNAVAILABLE');
  if (typeof now !== 'function') return denied('CHECK_CLOCK_UNAVAILABLE');
  const at = now(); if (!Number.isFinite(at) || at <= 3600000) return denied('CHECK_CLOCK_UNAVAILABLE');
  const suffix = 'ReceiptQueueQA' + Math.floor(at).toString(36) + crypto.randomBytes(8).toString('hex');
  const queueName = SANDBOX_NAMESPACE + '_' + suffix, runId = crypto.randomUUID();
  let collection;
  try {collection = service.col(suffix);} catch {return denied('SANDBOX_QUEUE_REFERENCE_UNCONFIRMED');}
  if (!collection || collection.path !== queueName || typeof collection.doc !== 'function' || typeof collection.get !== 'function') return denied('SANDBOX_QUEUE_REFERENCE_UNCONFIRMED');
  const ids = ['single', 'foreign-lease', 'concurrent'];
  const reference = id => {
    const ref = collection.doc(id);
    if (!ref || ref.path !== queueName + '/' + id) throw Error('Unsafe synthetic document reference');
    return ref;
  };
  const target = {operatingAccount: {accountType: 'GOOGLE_ADS', accountId: '1000000001'}, productDestinationId: '2000000001'};
  const fixture = id => ({synthetic: true, syntheticTestKind: KIND, syntheticRunId: runId,
    origin: 'internally_generated_synthetic_fixture', doNotIngest: true,
    orderId: 'synthetic-not-an-order-' + runId + '-' + id, gclid: 'synthetic-not-a-google-click',
    value: 1, refundedTotal: 0, currency: 'USD', uploaded: false, failed: false,
    dmState: 'processing', dmRequestId: 'synthetic-no-provider-request-' + runId + '-' + id,
    dmDestination: target, dmSubmittedAt: at - 3600000, dmChecks: 0, dmCheckedAt: 0,
    dmReconcileLease: null, dmReconcileLeaseUntil: 0, syntheticCreatedAt: at});
  const api = createReceiptReconciliation({env, db, service, queueName, now});
  const planFor = (id, row) => api.plan({rows: [{id, row}], evidence: [{rowId: id, requestId: row.dmRequestId,
    destination: row.dmDestination, observedAt: now(), individualOrderConfirmed: false,
    diagnostics: {requestStatusPerDestination: [{destination: row.dmDestination, requestStatus: 'SUCCESS',
      eventsIngestionStatus: {recordCount: '1'}}]}}]});
  const readRow = async id => {
    if (!scopeSafe()) throw Error('Sandbox namespace changed');
    const saved = await reference(id).get(), row = saved.exists ? saved.data() : null;
    if (!row || row.synthetic !== true || row.syntheticRunId !== runId || row.syntheticTestKind !== KIND || row.doNotIngest !== true) throw Error('Synthetic row identity changed');
    return row;
  };
  let phase = 'seed';
  try {
    await db.runTransaction(async tx => {
      if (!scopeSafe()) throw Error('Sandbox namespace changed');
      const refs = ids.map(reference);
      for (const ref of refs) if ((await tx.get(ref)).exists) throw Error('Synthetic ID collision');
      if (!scopeSafe()) throw Error('Sandbox namespace changed');
      for (let index = 0; index < refs.length; index++) tx.create(refs[index], fixture(ids[index]));
    });
    base.preservedSyntheticArtifacts = true;
    phase = 'dry_run';
    const original = await readRow('single'), singlePlan = planFor('single', original);
    if (!singlePlan.ok) throw Error('Synthetic plan refused');
    const dryRun = await api.apply(singlePlan), afterDryRun = await readRow('single');
    const dryRunUnchanged = rowFingerprint(original) === rowFingerprint(afterDryRun);
    if (!dryRun.ok || dryRun.dryRun !== true || dryRun.wouldApply !== 1 || dryRun.applied !== 0 || dryRun.queueUpdated || !dryRunUnchanged) throw Error('Dry-run changed synthetic receipt');
    phase = 'apply';
    const applied = await api.apply(singlePlan, {dryRun: false}), afterApply = await readRow('single');
    if (!applied.ok || applied.applied !== 1 || afterApply.dmState !== 'success' || afterApply.dmChecks !== 1) throw Error('Synthetic apply not confirmed');
    const originalFieldsPreserved = Object.keys(original).filter(field => !STATUS_FIELDS.includes(field))
      .every(field => rowFingerprint(original[field]) === rowFingerprint(afterApply[field]));
    if (!originalFieldsPreserved) throw Error('Synthetic original data changed');
    phase = 'idempotency';
    const repeated = await api.apply(singlePlan, {dryRun: false}), afterRepeat = await readRow('single');
    const repeatUnchanged = rowFingerprint(afterApply) === rowFingerprint(afterRepeat);
    if (!repeated.ok || repeated.applied !== 0 || repeated.alreadyApplied !== 1 || repeated.queueUpdated || !repeatUnchanged) throw Error('Synthetic repeat not idempotent');
    phase = 'foreign_lease';
    const foreignOriginal = await readRow('foreign-lease'), foreignPlan = planFor('foreign-lease', foreignOriginal);
    if (!foreignPlan.ok) throw Error('Synthetic foreign-lease plan refused before lease');
    await db.runTransaction(async tx => {
      if (!scopeSafe()) throw Error('Sandbox namespace changed');
      const ref = reference('foreign-lease'), saved = await tx.get(ref);
      if (!saved.exists || rowFingerprint(saved.data()) !== rowFingerprint(foreignOriginal)) throw Error('Synthetic lease row changed');
      if (!scopeSafe()) throw Error('Sandbox namespace changed');
      tx.update(ref, {dmReconcileLease: 'synthetic-foreign-worker-' + runId, dmReconcileLeaseUntil: now() + 90000});
    });
    const foreignBefore = await readRow('foreign-lease'), foreign = await api.apply(foreignPlan, {dryRun: false}), foreignAfter = await readRow('foreign-lease');
    const foreignUnchanged = rowFingerprint(foreignBefore) === rowFingerprint(foreignAfter);
    if (foreign.ok || foreign.code !== 'ACTIVE_FOREIGN_LEASE' || foreign.applied !== 0 || !foreignUnchanged) throw Error('Synthetic active foreign lease not protected');
    phase = 'concurrency';
    const concurrentOriginal = await readRow('concurrent'), concurrentPlan = planFor('concurrent', concurrentOriginal);
    if (!concurrentPlan.ok) throw Error('Synthetic concurrent plan refused');
    const concurrent = await Promise.all(Array.from({length: 3}, () => api.apply(concurrentPlan, {dryRun: false}))), concurrentAfter = await readRow('concurrent');
    const appliedTotal = concurrent.reduce((sum, result) => sum + result.applied, 0), alreadyTotal = concurrent.reduce((sum, result) => sum + result.alreadyApplied, 0);
    if (concurrent.some(result => !result.ok) || appliedTotal !== 1 || alreadyTotal !== 2 || concurrentAfter.dmChecks !== 1) throw Error('Synthetic concurrent apply not idempotent');
    phase = 'record';
    if (!scopeSafe()) throw Error('Sandbox namespace changed');
    const result = {...base, at, completedAt: now(), ok: true, passed: true, blocked: false,
      queueName, syntheticRunId: runId, syntheticReceiptDocs: ids.length, successfulStatusTransitions: 2,
      checks: {dryRun: {passed: true, wouldApply: 1, queueUnchanged: dryRunUnchanged},
        explicitApply: {passed: true, applied: 1, originalFieldsPreserved},
        idempotency: {passed: true, alreadyApplied: 1, queueUnchanged: repeatUnchanged},
        activeForeignLease: {passed: true, code: foreign.code, queueUnchanged: foreignUnchanged},
        concurrency: {passed: true, runs: concurrent.length, applied: appliedTotal, alreadyApplied: alreadyTotal, checksStored: concurrentAfter.dmChecks}},
      verificationLimit: 'Synthetic diagnostics test storage integration only; they are not Google receipt or purchase-attribution evidence.'};
    await db.runTransaction(async tx => {
      if (!scopeSafe()) throw Error('Sandbox namespace changed');
      const ref = reference('check-result'); if ((await tx.get(ref)).exists) throw Error('Synthetic result collision');
      if (!scopeSafe()) throw Error('Sandbox namespace changed');
      tx.create(ref, {...result, syntheticTestKind: KIND, doNotIngest: true});
    });
    return result;
  } catch {return {...base, blocked: true, code: 'SYNTHETIC_STORAGE_CHECK_FAILED', phase, queueName, syntheticRunId: runId};}
}

module.exports = {check, KIND};
