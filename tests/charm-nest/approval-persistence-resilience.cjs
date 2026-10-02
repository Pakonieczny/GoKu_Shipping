// Controlled fixtures for the production engraving approval/recovery functions.
// No browser automation, network, credentials or production records are used.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const seals = require('../../charm-nest-engraving-seals.js');
const activity = require('../../charm-nest-activity.js');
const source = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const section = (start, end) => {
  const a = source.indexOf(start), b = source.indexOf(end, a);
  assert(a >= 0 && b > a, `Production section exists: ${start}`);
  return source.slice(a, b);
};
function approvalSection() {
  const markers = ['  async function prepareApproval(', '  function settleApproval(', '  async function settleApproval(', '  function recoverApprovals(', '  async function recoverApprovals('];
  const a = Math.min(...markers.map(m => source.indexOf(m)).filter(n => n >= 0));
  return source.slice(a, source.indexOf('  /* ── 7.6', a));
}
const settle = async (n = 30) => { for (let i = 0; i < n; i++) await Promise.resolve(); };
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }

function fixture({ holdPress = false, holdPool = false, writeFiles = false, realSession = false } = {}) {
  const press = deferred(), pool = deferred();
  const control = { presses: 0, checkpoints: [], backCheckpoints:[], writes: 0, builds:0, uploads:[], poolAttempts: 0, apiCalls: [], networkDown: false, failPoolOnce: false, loseBackReplyOnce:false, requireExactBackRetry:false, checkpointFailure: false, scheduled: 0, poolGate:holdPool ? pool.promise : null, idbPending:[], idbCommits:0, holdIDB:false };
  const context = vm.createContext({ assert, console, Blob, structuredClone, Promise, performance, setImmediate,
    setTimeout: () => 1, clearTimeout() {}, Date,
    CNListActivity: activity, CNEngravingSeals: { ...seals, press: async () => { control.presses++; if (holdPress) await press.promise; } }, control });
  const db = { createObjectStore() {}, transaction() {
    const tx = {commit(){control.idbCommits++;},objectStore:()=>({put(value,key){
      const captured=structuredClone(value);
      const complete=()=>setImmediate(()=>{control.checkpoints.push(captured);tx.oncomplete?.();});
      if(control.holdIDB)control.idbPending.push(complete);else complete();
      return {};
    }})};return tx;
  }};
  context.indexedDB={open(){const req={};setImmediate(()=>{req.result=db;req.onupgradeneeded?.();req.onsuccess?.();});return req;}};
  vm.runInContext(`
    const window = { addEventListener() {}, CharmNestOperations: null };
    const document = { querySelectorAll: () => [],addEventListener() {} };
    const navigator = { onLine: true };
    const METALS = [{key:'gold'}], WORKSPACE_SANDBOX = true;
    const S = {mode:'engrave',settings:{},cloud:{ok:true},packingCatalog:{},sources:[],poolSources:{},unassigned:[],sheets:{gold:{active:0}}};
    const row = {key:'4176116510/12',state:'written',poolIds:['4176116510_12_1'],order:{receiptId:'4176116510'},line:{transactionId:'12'},spec:{designSku:'BUNNY5'},engrave:{needed:true,state:'review',approved:false}};
    const job = {key:row.key,row,copies:row.poolIds.slice(),state:'review',text:'Love, DJ',lines:['Love, DJ'],lineInput:['Love, DJ'],fit:{size:6,capMm:1.6,weight:'Regular',centre:[0,0],angle:0,glyphs:[]},view:{cx:0,cy:0,cutMembers:[],checks:{}},verify:{geometry:{ok:true}},mask:{bits:new Uint8Array([1,1]),w:2,h:1,res:1},materialVersion:2,backs:[]};
    const jobs = new Map([[job.key,job]]);
    const fitTasks = new WeakMap();
    const sheet = {sheetId:'sheet-test',fileBase:'GF_Sheet_1',folderPath:'charmnest/test',metal:'gold',charms:[],placements:[],backPool:[]};
    const B = {run:null,carry:null,orders:{rows:[row],byKey:new Map([[row.key,row]])},pool:{rows:new Map([[row.poolIds[0],{copy:1}]])},sets:new Map(),engrave:{items:jobs},review:{items:[]}};
    const EG = {cardKey:null,card:null,drafts:{}};
    const PT = 72/25.4;
    const allSheets = () => [sheet], items = () => jobs;
    const charmFor = () => ({sourceId:'source-test'}), sheetFor = () => sheet, sourceOf = () => ({parsed:{}});
    const fitOpts = () => ({lineGap:.216});
    const P = {buildBackFile:async () => {control.builds++;return {bytes:new Uint8Array([1]),reference:{redrawn:false},wPt:20,hPt:20};}};
    const verifyBackFile = async () => ({ok:true});
    const renderBack = () => ({_sizePt:{w:20,h:20},toBlob:cb=>cb(new Blob(['preview']))});
    const uploadBytes = async (path,bytes,type) => {const upload={path,url:'https://example.invalid/'+path+'?token='+String(control.uploads.length+1)};control.uploads.push(upload);return upload;};
    const serverBacks = new Map();
    const api = async (name,body) => {
      control.apiCalls.push(JSON.parse(JSON.stringify(body)));
      if(control.networkDown)throw new Error('Failed to fetch');
      if(body.op==='backPut') {
        control.backCheckpoints.push(control.checkpoints.at(-1));
        const old=serverBacks.get(body.back.poolId);
        if(control.requireExactBackRetry && old && JSON.stringify(old)!==JSON.stringify(body.back))throw new Error('This back was edited elsewhere. Reopen it before saving your changes.');
        serverBacks.set(body.back.poolId,JSON.parse(JSON.stringify(body.back)));
        if(control.loseBackReplyOnce){control.loseBackReplyOnce=false;throw new Error('Failed to fetch after the server committed backPut');}
      }
      return {ok:true,sheet:{backPool:[...serverBacks.values()]}};
    };
    const Pool = {update:async (ids,patch) => {control.poolAttempts++;if(control.poolGate && control.poolAttempts===1)await control.poolGate;if(control.failPoolOnce){control.failPoolOnce=false;throw new Error('Network timeout');}for(const id of ids)Object.assign(B.pool.rows.get(id),patch);}};
    const employeeName = () => 'Paul', askEmployee = () => 'Paul';
    const toast = () => {}, agent = () => {}, render = () => {}, refreshBacks = () => {}, scheduleBackOutputs = () => {}, goes = () => {}, EG_TAB = () => '';
    const Review = {remove() {},add() {},render() {}};
    const Orders = {view:()=>({}),render() {},rows:()=>B.orders.rows};
    const Engrave = {view:()=>({tab:'place',focus:job.key,chosen:true,list:false,drafts:{}})};
    const Gate = {state:()=>({})}, Recall = {state:()=>({})}, LiveStrip = {rows:[]};
    const RunCtl = {backgroundSettled() {},poke() {},save:async () => {},renderBanner() {}};
    const syncEditedBack = async () => {};
    const loadFonts = async () => {}, restoreWritten = () => {}, hasPlacement = j => !!(j.fit&&j.view);
  `, context);
  // Browser modules share one realm. Loading this factory in the same context
  // keeps seal objects faithful to the real IndexedDB copier's prototype check.
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../../charm-nest-engraving-seals.js'),'utf8'),context);
  context.CNEngravingSeals=vm.runInContext('window.CNEngravingSeals',context);
  context.CNEngravingSeals.press=async()=>{control.presses++;if(holdPress)await press.promise;};
  // Use the real workspace copier and snapshot shape; the checkpoint adapter
  // commits those bytes immediately, or deliberately fails before the stamp.
  if(realSession)vm.runInContext(section('const Session = window.Session =', '/* ═══ 24g'),context);
  else {
    vm.runInContext(section('  const OMIT =', '  function open()') + section('  function poolSourcesInUse()', '  /** Before each checkpoint'), context);
    vm.runInContext(`
    const Session = {
      copy,capture,
      schedule(){control.scheduled++;},
      flushNow(){if(control.checkpointFailure)return Promise.resolve(false);control.checkpoints.push(structuredClone(capture()));return Promise.resolve(true);},
      flush(){return this.flushNow();}
    };
    `, context);
  }
  vm.runInContext(approvalSection(), context);
  if (writeFiles) {
    vm.runInContext(section('  let backQueue = Promise.resolve();', '  /* Paul, 24 Sep: "nothing may accumulate"'), context);
  } else {
    vm.runInContext(`
      async function writeBacks(j) {
        control.writes++;
        if(control.networkDown)throw new Error('Failed to fetch');
        j.backs=j.copies.map(poolId=>({poolId,approvedAt:j.approvedAt,approvedBy:j.approvedBy,engravingSeals:CNEngravingSeals.keep(j),outputs:{ai:{url:'https://example.invalid/back.ai'},png:{url:'https://example.invalid/back.png'}}}));
        j.state='written';j.row.engrave.state='written';
      }
    `, context);
  }
  return { c: context, control, press, pool, run: code => vm.runInContext(code, context) };
}

(async () => {
  const failures = [], checks = [];
  async function check(name, fn) { try { await fn(); checks.push(name); console.log('PASS:', name); } catch (e) { failures.push({name,error:e}); console.error('FAIL:', name, e.message); } }

  await check('verified approval survives a checkpoint in the middle of its stamp', async () => {
    const f = fixture({holdPress:true});
    const approval = f.run('approve(job, "Paul")');
    await settle();
    assert.equal(f.control.presses, 1);
    const snapshot = f.control.checkpoints.at(-1);
    assert(snapshot, 'verified intent is committed before the animated press starts');
    const saved = snapshot.jobs[0];
    assert.equal(saved.approvalIntent?.phase, 'verified');
    assert.equal(saved.approvalIntent.by, 'Paul');
    assert(saved.approvalIntent.at > 1e12);
    assert.equal(saved.approvalPreparing, undefined, 'transient preparation lock is never restored');
    assert.equal(saved.stamping, undefined, 'animation lock cannot strand a restored job');
    assert(saved.fit && saved.view && saved.mask, 'the exact placement remains in the checkpoint');

    const restored = fixture();
    restored.c.savedJSON = JSON.stringify(saved);
    restored.run(`const restoredJob=JSON.parse(savedJSON);if(restoredJob.mask?.bits)restoredJob.mask.bits=new Uint8Array(Object.values(restoredJob.mask.bits));Object.assign(job,restoredJob);job.row=row;jobs.set(job.key,job);control.networkDown=true;`);
    assert.equal(restored.run('typeof recoverApprovals'), 'function', 'recovery has an explicit decision settlement entry point');
    await restored.run('recoverApprovals()');
    const recovered = restored.run('job');
    assert.equal(recovered.state, 'approved');
    assert.equal(recovered.approvedAt, saved.approvalIntent.at);
    assert.equal(recovered.approvedBy, 'Paul');
    assert.equal(recovered.row.engrave.approved, true);
    assert.equal(seals.list(recovered).length, 1, 'recovering adds exactly the original approval seal');
    assert.equal(seals.list(recovered)[0].at, saved.approvalIntent.at);
    assert.equal(restored.control.presses, 0, 'restoring history never plays the wooden press again');
    await restored.run('resumeBacks()');
    await settle();
    assert.equal(recovered.state, 'approved', 'offline files do not revoke the recorded approval');
    assert(recovered.backPending);
    restored.control.networkDown = false;
    await restored.run('resumeBacks()');
    await settle();
    assert.equal(recovered.state, 'written');
    assert.equal(seals.list(recovered).length, 1);
    assert.equal(restored.control.presses, 0);
    f.press.resolve();await approval;
  });

  await check('failed intent checkpoint prevents a stamp and remains safely retryable', async () => {
    const f = fixture();f.control.checkpointFailure = true;
    await f.run('approve(job,"Paul")');
    assert.equal(f.control.presses, 0, 'the approval must be durable before it receives a visible stamp');
    assert.equal(f.run('job.state'), 'review');
    assert.equal(f.run('job.approvalPreparing'), false);
    assert.equal(seals.list(f.run('job')).length, 0);
    f.control.checkpointFailure = false;
    await f.run('approve(job,"Paul")');
    assert.equal(f.control.presses, 1);
    assert.equal(f.run('job.state'), 'written');
  });

  await check('a lost pool-update reply resumes a recorded back without another approval', async () => {
    const f = fixture({writeFiles:true});
    f.run(`job.state='approved';job.approvedAt=Date.now();job.approvedBy='Paul';row.engrave={needed:true,state:'approved',approved:true,approvedAt:job.approvedAt,approvedBy:'Paul'};CNEngravingSeals.add(job,'engraveApproved','Paul',job.approvedAt);`);
    f.control.failPoolOnce = true;
    await f.run('saveBacks(job)');
    assert.equal(f.run('job.backs.length'), 1, 'the exact copy is already recorded on its sheet');
    assert.equal(f.control.apiCalls.filter(b=>b.op==='backPut').length, 1);
    assert.equal(f.control.poolAttempts, 1);
    assert.equal(f.run('job.approvedBy'), 'Paul');
    assert(seals.list(f.run('job')).length === 1);
    await f.run('resumeBacks()');
    await settle(80);
    assert.equal(f.control.poolAttempts, 2, 'the incomplete pool stage must be retried');
    assert.equal(f.run('B.pool.rows.get(row.poolIds[0]).state'), 'engraved');
    assert.equal(f.run('job.state'), 'written');
    assert.equal(f.run('job.backPending'), undefined);
    assert.equal(seals.list(f.run('job')).length, 1, 'retrying the last stage never requires another seal');
    assert.equal(f.control.presses, 0);
  });

  await check('ordinary offline file saving retains the decision and retries exactly its copies', async () => {
    const f = fixture();
    f.run(`job.state='approved';job.approvedAt=Date.now();job.approvedBy='Paul';row.engrave={needed:true,state:'approved',approved:true,approvedAt:job.approvedAt,approvedBy:'Paul'};CNEngravingSeals.add(job,'engraveApproved','Paul',job.approvedAt);`);
    const at = f.run('job.approvedAt');f.control.networkDown = true;
    await f.run('saveBacks(job)');
    assert.equal(f.run('job.state'), 'approved');assert.equal(f.run('row.engrave.approved'), true);
    assert(f.run('job.backPending'));
    f.control.networkDown = false;
    await f.run('resumeBacks()');await settle();
    assert.equal(f.run('job.state'), 'written');assert.equal(f.run('job.approvedAt'), at);
    assert.equal(f.run('job.backs[0].poolId'), '4176116510_12_1');
    assert.equal(seals.list(f.run('job')).length, 1);
  });

  await check('closing during the final pool stage resumes that unfinished stage on restoration', async () => {
    const f = fixture({holdPool:true,writeFiles:true});
    f.run(`job.state='approved';job.approvedAt=Date.now();job.approvedBy='Paul';row.engrave={needed:true,state:'approved',approved:true,approvedAt:job.approvedAt,approvedBy:'Paul'};CNEngravingSeals.add(job,'engraveApproved','Paul',job.approvedAt);`);
    const saving = f.run('saveBacks(job)');await settle(80);
    assert.equal(f.control.poolAttempts, 1);
    const snapshot = structuredClone(f.run('Session.capture()'));
    assert.equal(snapshot.jobs[0].backs.length, 1);
    assert.equal(snapshot.jobs[0].backSaving, undefined, 'runtime save flag does not strand a reload');
    const restored = fixture({writeFiles:true});
    restored.c.snapshotJSON = JSON.stringify(snapshot);
    restored.run(`const restoredState=JSON.parse(snapshotJSON);Object.assign(row,restoredState.orders.rows[0]);Object.assign(job,restoredState.jobs[0]);job.row=row;jobs.set(job.key,job);`);
    await restored.run('resumeBacks()');await settle(80);
    assert.equal(restored.control.poolAttempts, 1, 'a restart must finish pool metadata already interrupted after backPut');
    assert.equal(restored.run('B.pool.rows.get(row.poolIds[0]).state'), 'engraved');
    assert.equal(restored.run('job.state'), 'written');
    assert.equal(seals.list(restored.run('job')).length, 1);
    assert.equal(restored.control.presses, 0);
    f.pool.resolve();await saving;
  });

  await check('recovery never applies an old verified intent to geometry that had to be repaired', async () => {
    const f = fixture();
    f.run(`job.approvalIntent={phase:'verified',at:Date.now(),by:'Paul',text:job.text};job.state='ready';job.fit=null;job.view=null;job.verify=null;row.engrave.state='ready';row.engrave.approved=false;`);
    await f.run('recoverApprovals()');
    assert.equal(f.run('job.state'), 'ready', 'changed material geometry must be refitted and reviewed');
    assert.equal(f.run('row.engrave.approved'), false);
    assert.equal(f.run('job.approvalIntent'), undefined, 'a superseded placement cannot replay later after a new fit');
    assert.equal(seals.list(f.run('job')).length, 0, 'unplaced geometry never receives a new approval seal');
    assert.equal(f.control.presses, 0);
  });

  await check('changed words discard a stale intent while preserving genuine earlier history', async () => {
    const f = fixture();
    f.run(`CNEngravingSeals.add(job,'engraveApproved','Seth',Date.now()-10000);job.approvalIntent={phase:'verified',at:Date.now(),by:'Paul',text:'Original words'};job.text='New words';job.lines=['New words'];`);
    await f.run('recoverApprovals()');
    assert.equal(f.run('job.state'), 'review');
    assert.equal(f.run('row.engrave.approved'), false);
    assert.equal(f.run('job.approvalIntent'), undefined, 'reverting the words later must not accidentally replay an obsolete decision');
    assert.deepEqual(seals.list(f.run('job')).map(s=>s.by), ['Seth']);
    assert.equal(f.control.presses, 0);
  });

  await check('the real immediate workspace checkpoint waits for its storage transaction to commit', async () => {
    const f = fixture({realSession:true});
    f.control.holdIDB=true;
    f.run('Session.listen();Session.schedule();');
    assert.equal(f.run('typeof Session.flushNow'), 'function', 'the immediate checkpoint is available to approval');
    const committed=f.run('Session.flushNow()');
    assert.equal(typeof committed?.then, 'function', 'approval can await durable completion');
    let finished=false;committed.then(()=>{finished=true;});
    await new Promise(setImmediate);await new Promise(setImmediate);
    assert.equal(f.control.idbPending.length, 1);
    assert.equal(finished, false, 'a merely issued write is not an approval checkpoint');
    assert.equal(f.control.checkpoints.length, 0);
    f.control.idbPending.shift()();
    assert.equal(await committed, true);
    assert.equal(f.control.checkpoints.length, 1);
    assert.equal(f.control.idbCommits, 1, 'the immediate transaction is explicitly committed during page close');
  });

  await check('an edited back with a lost save reply replays the exact durable upload payload', async () => {
    const f = fixture({writeFiles:true});
    f.run(`job.editingBack=true;job.editOriginal={copy:1};job.editSheet=sheet;job.expectedApprovedAt=Date.now()-10000;job.state='approved';job.approvedAt=Date.now();job.approvedBy='Paul';row.engrave={needed:true,state:'approved',approved:true,approvedAt:job.approvedAt,approvedBy:'Paul'};CNEngravingSeals.add(job,'engraveApproved','Paul',job.approvedAt);`);
    const prior=f.run('job.expectedApprovedAt'), approval=f.run('job.approvedAt');
    f.control.loseBackReplyOnce=true;f.control.requireExactBackRetry=true;
    await f.run('saveBacks(job)');
    const first=f.control.apiCalls.find(b=>b.op==='backPut');
    assert(first, 'the first exact copy was submitted and committed by the server');
    assert.equal(f.run('job.state'), 'approved');
    assert.equal(f.run('job.backs.length'), 0, 'a lost acknowledgement has not yet populated the local saved list');
    assert.equal(f.run('job.expectedApprovedAt'), prior);
    assert.equal(f.control.uploads.length, 2, 'one AI and one preview upload were created');
    assert(f.control.backCheckpoints[0]?.jobs[0]?.stagedBacks, 'the upload payload is durably checkpointed before backPut starts');
    const checkpoint=structuredClone(f.run('Session.capture()'));
    assert(checkpoint.jobs[0].stagedBacks, 'the exact unacknowledged payload survives closing the view or browser');
    const staged=JSON.stringify(checkpoint.jobs[0].stagedBacks);
    assert(staged.includes(first.back.outputs.ai.url));assert(staged.includes(first.back.outputs.png.url));
    await f.run('resumeBacks()');await settle(100);
    const attempts=f.control.apiCalls.filter(b=>b.op==='backPut');
    assert.equal(attempts.length, 2);
    assert.deepEqual(attempts[1].back, attempts[0].back, 'retry preserves all saved content, placement evidence, and upload URLs');
    assert.equal(attempts[1].expectedApprovedAt, prior, 'the original concurrency expectation is replayed until acknowledged');
    assert.equal(f.control.uploads.length, 2, 'retry must not produce replacement tokens by uploading again');
    assert.equal(f.control.builds, 1, 'retry must not rebuild the approved file');
    assert.equal(f.run('job.state'), 'written');
    assert.equal(f.run('job.expectedApprovedAt'), approval);
    assert.equal(f.run('job.backPending'), undefined);
    assert.equal(f.run('(job.stagedBacks || []).length'), 0, 'acknowledgement releases the persisted staged record');
    assert.equal(seals.list(f.run('job')).length, 1);
    assert.equal(f.control.presses, 0, 'file recovery never asks for another stamp');
  });

  await check('closing after a committed edited-back save restores its exact staged payload', async () => {
    const f = fixture({writeFiles:true});
    f.run(`job.editingBack=true;job.editOriginal={copy:1};job.editSheet=sheet;job.expectedApprovedAt=Date.now()-10000;job.state='approved';job.approvedAt=Date.now();job.approvedBy='Paul';row.engrave={needed:true,state:'approved',approved:true,approvedAt:job.approvedAt,approvedBy:'Paul'};CNEngravingSeals.add(job,'engraveApproved','Paul',job.approvedAt);`);
    f.control.loseBackReplyOnce=true;await f.run('saveBacks(job)');
    const original=f.control.apiCalls.find(b=>b.op==='backPut');
    const restored=fixture({writeFiles:true});
    restored.c.snapshotJSON=JSON.stringify(f.run('Session.capture()'));
    restored.c.serverJSON=f.run('JSON.stringify([...serverBacks])');
    restored.run(`const savedState=JSON.parse(snapshotJSON),savedJob=savedState.jobs[0];Object.assign(row,savedJob.editRow || savedState.orders.rows[0]);Object.assign(job,savedJob);job.row=row;jobs.set(job.key,job);for(const [id,back] of JSON.parse(serverJSON))serverBacks.set(id,back);control.requireExactBackRetry=true;`);
    await restored.run('resumeBacks()');await settle(100);
    const retried=restored.control.apiCalls.find(b=>b.op==='backPut');
    assert(retried);
    assert.deepEqual(JSON.parse(JSON.stringify(retried.back)), JSON.parse(JSON.stringify(original.back)), 'a new browser realm submits exactly the previously recorded JSON payload');
    assert.equal(retried.expectedApprovedAt, original.expectedApprovedAt);
    assert.equal(restored.control.uploads.length, 0, 'restoration uses the previously uploaded files');
    assert.equal(restored.control.builds, 0, 'restoration does not rebuild the already-verified back');
    assert.equal(restored.run('job.state'), 'written');
    assert.equal(restored.run('(job.stagedBacks || []).length'), 0);
    assert.equal(seals.list(restored.run('job')).length, 1);
    assert.equal(restored.control.presses, 0);
  });

  if (failures.length) {
    console.error(`${failures.length}/${checks.length + failures.length} approval persistence checks failed`);
    for (const {name,error} of failures) console.error(name + '\n' + error.stack);
    process.exitCode = 1;
  } else console.log(`${checks.length} approval persistence resilience checks passed`);
})().catch(e => { console.error(e);process.exitCode = 1; });
