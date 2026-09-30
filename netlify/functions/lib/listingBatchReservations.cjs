'use strict';
const batchCharmPaths = record => [...new Set((record.sets || []).flatMap(set =>
  (set.allTasks || set.tasks || []).map(task => task.input_charm_storage_path).filter(Boolean)))];
function pendingBatchWork(record) {
  if (record.retryBatchName || record.setComplete) return false;
  if (record.collectionPending || record.repairPending) return true;
  if (record.stallRestartBlocked || record.stallRestartClosed) return false;
  if (record.collected) return false;
  if (record.retryRequested && !['capacity_refused', 'attempts_exhausted', 'complete_or_protected'].includes(record.retryStatus)) return true;
  const state = String(record.state || '').replace(/^BATCH_STATE_/, 'JOB_STATE_');
  return ['JOB_STATE_PENDING', 'JOB_STATE_RUNNING', 'JOB_STATE_SUCCEEDED'].includes(state) ||
    !!record.stallCancelRequestedAt;
}
async function readReservedCharms(db, collection) {
  const fields = ['charmPaths', 'state', 'collected', 'setComplete', 'retryBatchName', 'retryRequested',
    'retryStatus', 'repairPending', 'collectionPending', 'stallCancelRequestedAt', 'stallRestartBlocked', 'stallRestartClosed'];
  const snapshots = await Promise.all([
    db.collection(collection).where('collected', '==', false).select(...fields).get(),
    db.collection(collection).where('repairPending', '==', true).select(...fields).get(),
  ]);
  const current = new Map();
  for (const snapshot of snapshots) snapshot.forEach(doc => {
    const data = doc.data();
    if (pendingBatchWork(data)) current.set(doc.id, {doc, data});
  });
  const reserved = new Set();
  const records = [...current.values()];
  for (let i = 0; i < records.length; i += 8) {
    const groups = await Promise.all(records.slice(i, i + 8).map(async ({doc, data}) => {
      // Only genuinely live legacy jobs need a full read. Finished history
      // neither reserves a charm nor loads its saved image prompts.
      return Array.isArray(data.charmPaths) ? data.charmPaths : batchCharmPaths((await doc.ref.get()).data() || {});
    }));
    for (const paths of groups) for (const path of paths) reserved.add(path);
  }
  return reserved;
}
module.exports = { batchCharmPaths, pendingBatchWork, readReservedCharms };
