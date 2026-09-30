'use strict';
const assert = require('node:assert/strict');
const { readReservedCharms, batchCharmPaths } = require('../../netlify/functions/lib/listingBatchReservations.cjs');
const charm = name => `listing-generator-1/Charm_Maker/New_Charms/${name}.png`;
const set = name => ({tasks: [{input_charm_storage_path: charm(name)}]});
const rows = [
  ...Array.from({length: 800}, (_, i) => ({id: `old_${i}`, state: 'JOB_STATE_FAILED', collected: false, sets: [set(`old_${i}`)]})),
  {id: 'parent', state: 'JOB_STATE_RUNNING', collected: false, retryBatchName: 'current', sets: [set('old')]},
  {id: 'current', state: 'JOB_STATE_RUNNING', collected: false, charmPaths: [charm('current')]},
  {id: 'queued', state: 'JOB_STATE_QUEUED', collected: false, retryRequested: true, charmPaths: [charm('queued')]},
  {id: 'repair', state: 'JOB_STATE_SUCCEEDED', collected: true, repairPending: true, charmPaths: [charm('repair')]},
  {id: 'saving', state: 'JOB_STATE_CANCELLED', collected: false, collectionPending: true, stallRestartClosed: true, charmPaths: [charm('saving')]},
  {id: 'complete', state: 'JOB_STATE_SUCCEEDED', collected: false, setComplete: true, sets: [set('complete')]},
  {id: 'legacy_live', state: 'JOB_STATE_RUNNING', collected: false, sets: [set('legacy')]},
];
const fullReads = [];
const db = {collection: () => ({where: (field, op, value) => ({select: (...fields) => {
  assert(!fields.includes('sets') && !fields.includes('preparedSubmission'), 'reservation scan excludes task payloads');
  return {get: async () => ({forEach: fn => rows.filter(r => r[field] === value).forEach(row => fn({id: row.id,
    data: () => Object.fromEntries(Object.entries(row).filter(([key]) => fields.includes(key))),
    ref: {get: async () => {fullReads.push(row.id); return {data: () => row};}},
  }))})};
}})})};
(async () => {
  assert.deepEqual([...await readReservedCharms(db, 'batches')].sort(),
    ['current', 'queued', 'repair', 'saving', 'legacy'].map(charm).sort());
  assert.deepEqual(fullReads, ['legacy_live'], 'only the live legacy job needs full metadata');
  assert.deepEqual(batchCharmPaths({sets: [set('same'), set('same')]}), [charm('same')]);
  const failingDb = {collection: () => ({where: () => ({select: () => ({get: async () => {throw new Error('unavailable');}})})})};
  await assert.rejects(readReservedCharms(failingDb, 'batches'), /unavailable/,
    'failed reservation verification cannot return an empty reservation set');
  console.log('Charm reservations: completed history excluded, live/queued/repair/saving protected, legacy fallback bounded, lookup failures stop selection');
})().catch(err => {console.error(err); process.exitCode = 1;});
