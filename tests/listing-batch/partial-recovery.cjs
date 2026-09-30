'use strict';
const assert = require('node:assert/strict');
const { reconcileSession, compliantModelTask, failureKind } = require('../../netlify/functions/lib/listingBatchRecovery.cjs');
const sessionId = 'sess_123456789_abcdef';
const category = 'Beady_Necklace';
const path = n => `listing-generator-1/${category}/Ready_To_List/Set_${n}`;
const job = (n, extra = {}) => ({ batchName: `batch_${n}`, sessionId, displayName: 'lg1-Beady_Necklace-6sets-date',
  state: 'JOB_STATE_SUCCEEDED', collected: true, createdAt: n, responsesFile: `file-${n}`,
  sets: [{ category, setN: n, outputBasePath: path(n), tasks: [0,1].map(slotIndex => ({slotIndex, type:'edits',
    input_storage_path:`listing-generator-1/Beady_Necklace/Primary_Models/Model_${n}.png`,prompt:'Preserve charm exactly.'})) }],
  results:{failedCount:1,failures:[{key:'s0_slot1',error:'Your request was rejected by the safety system'}]}, ...extra });
async function run() {
  const records = [job(1), job(2), job(3,{results:{failedCount:1,failures:[{key:'s0_slot1',error:'socket hang up'}]}}),
    job(4,{contentRepairAttempt:1}),job(5,{state:'JOB_STATE_RUNNING',collected:false}),job(6,{state:'JOB_STATE_QUEUED',locallyQueued:true,collected:false,retryRequested:true})];
  // An old pointer and duplicate provider job refer to set 1, not extra listings.
  records.push({...job(1),batchName:'batch_duplicate',createdAt:0,retryBatchName:'batch_1'});
  const files = new Set([`${path(1)}/manifest.json`,`${path(1)}/Slot_1.png`,`${path(1)}/Slot_2.png`,`${path(2)}/Slot_1.png`,`${path(3)}/Slot_1.png`,`${path(4)}/Slot_1.png`]);
  const summaryDocs = new Map();
  const db = { collection: name => name === 'batches' ? {where:()=>({limit:()=>({get:async()=>({size:records.length,docs:records.map(record=>({ref:record,data:()=>record}))})})})}
    : {doc:id=>({set:async value=>summaryDocs.set(id,value)})},
    batch:()=>({set:(ref,value)=>Object.assign(ref,value),commit:async()=>{}}) };
  const bucket = {getFiles:async({prefix})=>[[...files].filter(name=>name.startsWith(prefix)).map(name=>({name}))]};
  const summary = await reconcileSession({db,bucket,collection:'batches',sessionId,timestamp:()=>123,now:()=>123});
  assert.equal(summary.registered,6);assert.equal(summary.planned,6);
  assert.equal(summary.complete,1);assert.equal(summary.queued,2);assert.equal(summary.saving,1);assert.equal(summary.blocked,1);assert.equal(summary.active,1);
  assert.equal(records[2].collectionPending,true,'save errors recover existing output');
  assert.equal(records[2].collected,false,'a failed upload stays in the collector');
  assert.equal(records[1].repairPending,true,'partial moderation result gets one substantially clothed correction');
  assert.equal(records[3].retryRequested,false,'rejected correction cannot loop');
  files.add('listing-generator-1/Generated_Listing_Sets/Completed_Listing_Sets/Beady_Necklace_Set_2/Slot_1.png');
  const approved = await reconcileSession({db,bucket,collection:'batches',sessionId,timestamp:()=>124,now:()=>124});
  assert.equal(approved.approved,1);assert.equal(approved.complete,2);
  assert.equal(records[1].retryRequested,false,'approval takes precedence over missing Ready files');
  assert.equal(records[1].setComplete,true);
  assert.equal(files.size,7,'audit never rewrites images');
  const task={slotIndex:0,prompt:'Preserve charm exactly.',input_charm_storage_path:'original-charm.png'};
  const revised=compliantModelTask(task);
  assert.equal(revised.input_charm_storage_path,task.input_charm_storage_path);
  assert.match(revised.prompt,/opaque, fully covering crew-neck top/);
  assert.equal(failureKind('socket hang up'),'storage');
  assert.equal(failureKind('safety_violations=[sexual]'),'content');
  console.log('Partial recovery: unique sets, real files, approval protection, paid-result recovery, bounded clothing correction passed');
}
run().catch(err=>{console.error(err);process.exitCode=1;});
