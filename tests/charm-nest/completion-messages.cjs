// Real message handler + completion functions, isolated from production services.
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..');
const html = fs.readFileSync(path.join(root, 'design-1.html'), 'utf8');
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const O = require('../../charm-nest-orders.js');
const slice = (code, start, end) => code.slice(code.indexOf(start), code.indexOf(end, code.indexOf(start)));
const store = new Map();
let serial = 0, queue = Promise.resolve(), timestamp = 0;
const doc = key => ({
  get: async () => ({exists: store.has(key), data: () => store.get(key)}),
  set: async value => store.set(key, value),
  collection: name => collection(key + '/' + name)
});
const collection = key => ({doc: id => doc(key + '/' + id), add: async value => doc(key + '/auto-' + ++serial).set(value)});
const db = {collection, runTransaction(fn) {
  const task = queue.then(() => fn({get: ref => ref.get(), set: (ref, value) => ref.set(value)}));
  queue = task.catch(() => {}); return task;
}};
const admin = {firestore: Object.assign(() => db, {FieldValue: {serverTimestamp: () => ++timestamp}})};
const backend = {exports: {}, require: name => {assert.equal(name, './firebaseAdmin'); return admin;}, console};
vm.runInNewContext(fs.readFileSync(path.join(root, 'netlify/functions/firebaseOrders.js'), 'utf8'), backend);
const post = (body, sandbox = false) => backend.exports.handler({httpMethod: 'POST', body: JSON.stringify(body), queryStringParameters: sandbox ? {sandbox: '1'} : {}});
const messages = rid => [...store.entries()].filter(([key]) => key.startsWith('Brites_Orders/' + rid + '/messages/'));
let failures = new Set(), loseResponse = new Set(), ledger = [], archives = [], calls = [];
const context = vm.createContext({console, localStorage: {getItem: () => 'Paul'}, LS: {employee: 'employee'}, FN: '/fn', db: null,
  fetch: async (url, opts) => {
    const body = JSON.parse(opts.body); calls.push(body);
    if (!body.newMessage) return {ok: true};
    if (failures.has(body.orderNumber)) return {ok: false, status: 503};
    const result = await post(body);
    if (loseResponse.delete(body.orderNumber)) throw new Error('response lost');
    return {ok: result.statusCode === 200, status: result.statusCode, json: async () => JSON.parse(result.body)};
  },
  removePreviewBoxesForOrder() {}, $$: () => [], cssEsc: x => x,
  orderCache: {}, selectedOrders: new Set(['101','102']), completedOrders: new Set(),
  allOpenReceipts: [{receipt_id:'101'}, {receipt_id:'102'}], currentReceipts: [],
  persistCompleted: async ids => ledger.push(...ids), RT: {clientId: 'test'}, rtSelected: new Set(),
  persistSelection() {}, updateCounters() {}, applyMetalFilter() {}, refreshChrome() {},
  archiveOrders: async ids => archives.push(...ids), Bridge: {emit() {}}
});
vm.runInContext(slice(html, 'async function markBritesDesigned(', '/**\n * Load the print page') + '\n' +
  slice(html, 'let __commitInFlight = 0;', '/** Restore the most recent printed batch.'), context);

(async () => {
  failures.add('102');
  await assert.rejects(context.commitCompletion(['101','101','102'], {setId:'set-1', completedBy:'Charm Sorter (Paul)'}), /102.*Retry Commit set/);
  assert.equal(messages('101').length, 1);
  assert.equal(messages('102').length, 0);
  assert.deepEqual(ledger, []); assert.deepEqual(archives, []);
  assert.equal(context.completedOrders.size, 0, 'failed notification must leave orders open');
  assert.equal(context.allOpenReceipts.length, 2);
  const first = messages('101')[0][1];
  failures.clear(); loseResponse.add('102');
  await assert.rejects(context.commitCompletion(['101','102'], {setId:'set-1'}), /102/);
  assert.equal(messages('102').length, 1, 'server saved the message despite the lost response');
  await context.commitCompletion(['101','102','102'], {setId:'set-1'});
  assert.deepEqual(ledger, ['101','102']);
  assert.deepEqual(archives, ['101','102']);
  assert.equal(messages('101').length, 1); assert.equal(messages('102').length, 1);
  assert.equal(messages('101')[0][1], first, 'retry must not change message, sender or timestamp');
  assert.equal(first.text, 'DESIGNED :)'); assert.equal(first.senderName, 'Charm Sorter (Paul)');
  assert.equal(first.senderRole, 'staff'); assert.equal(first.setId, 'set-1');
  await Promise.all(Array.from({length: 5}, () => post({orderNumber:'103', newMessage:'DESIGNED :)', designSetId:'set-1'})));
  assert.equal(messages('103').length, 1, 'concurrent delivery is idempotent');
  await post({orderNumber:'103', newMessage:'DESIGNED :)', designSetId:'set-2'});
  assert.equal(messages('103').length, 2, 'a later set has its own completion event');
  await post({orderNumber:'103', newMessage:'DESIGNED :)', designSetId:'set-1'}, true);
  assert.equal(messages('103').length, 2, 'sandbox does not touch production');
  assert([...store.keys()].some(k => k.startsWith('Sandbox_Brites_Orders/103/messages/')));
  assert.equal((await post({orderNumber:'104', newMessage:'Something else', designSetId:'set-1'})).statusCode, 400);
  await context.markBritesDesigned(['105'], 'Paul');
  assert.equal(messages('105').length, 1, 'original QR print route remains supported');
  await post({orderNumber:'105', newMessage:'Normal chat', employeeName:'Paul'});
  assert.equal(messages('105').length, 2, 'ordinary chat remains append-only');

  // Use the actual QR membership check and release validator on per-colour fixtures.
  const set = {runId:'run-1', setId:'set-1', seq:2, sheetIds:['s1'], labelFiles:[]};
  const sh = {sheetId:'s1', setId:'set-1', metal:'gold', fileBase:'Set-1-gold', persistedDone:true, outputs:{}, releaseFull:true,
    verification:{ok:true}, placements:[{id:'c1'}], charms:[{id:'c1', order:'101/line', poolId:'p1'}], backPool:[]};
  let sheets = [sh], rows = [], jobs = new Map(), selected = {};
  const qr = () => {
    const f = {sheetId:'s1', sheet:sh.fileBase, path:'labels/s1.png', url:'https://fixture/s1.png', part:1, payload:O.encodeOrderList(['101'], O.CARD_TO_METAL[sh.metal] || sh.metal)};
    sh.label = {files:[f]}; set.labelFiles = [f];
  };
  qr();
  const gate = vm.createContext({O, Gate:{modern:()=>true, policy:(s,seq)=>O.sheetRelease({material:s.metal, verified:s.verification.ok, placed:s.placements.length, full:s.releaseFull, dirty:s.dirty}, {seq, selected})},
    sheetsOf:()=>sheets, Orders:{rows:()=>rows}, Engrave:{items:()=>jobs}, labelOf:x=>x});
  vm.runInContext(slice(bridge, '  function labelsReady(', '  async function onSheetSaved(') + '\n' + slice(bridge, '  function validateRelease(', '  async function finalize('), gate);
  const valid = () => gate.validateRelease(set);
  valid();
  for (const metal of ['gold','silver']) {
    sh.metal=metal; qr(); sh.releaseFull=false; assert.throws(valid); sh.releaseFull=true; valid();
  }
  sh.metal='rose'; qr(); set.seq=1; assert.throws(valid); set.seq=2; valid();
  for (const metal of ['gold10k','gold14k']) {
    sh.metal=metal; qr(); assert.throws(valid); selected[metal]=true; valid();
  }
  sh.metal='gold'; qr();
  sheets=[]; assert.throws(valid, /not all loaded/); sheets=[sh];
  sh.persistedDone=false; assert.throws(valid, /not saved/); sh.persistedDone=true;
  sh.label.files[0].payload='stale'; assert.throws(valid, /QR labels/); qr();
  set.labelFiles=[]; assert.throws(valid, /QR labels/); qr();
  rows=[{order:{receiptId:'101'}, poolIds:['p1'], engrave:{needed:true,approved:true,state:'approved'}}];
  assert.throws(valid, /back engraving/);
  rows[0].engrave.state='written'; assert.throws(valid, /back engraving/);
  sh.backPool=[{poolId:'p1', approvedAt:123, verified:{file:{ok:true}}, outputs:{ai:{path:'backs/p1.ai',url:'https://fixture/p1.ai'}}}];
  valid();
  jobs.set('edit', {copies:['p1'],state:'review'}); assert.throws(valid, /still needs/); jobs.clear();
  sh.backPool[0].verified.file.ok=false; assert.throws(valid, /back engraving/);
  rows[0].engrave={needed:false,approved:true,state:'skipped'}; valid();
  const orderRows = [{state:'written',spec:{}},{state:'pooled',spec:{}}];
  assert.equal(O.evaluateOrder(orderRows).committable,false, 'a mixed-material order waits for its other sheet');
  orderRows[1].state='written'; assert.equal(O.evaluateOrder(orderRows).committable,true);
  console.log('PASS: completion delivery, deduplication, retries, sandbox, print compatibility, all-colour sheet/QR/back gates');
})().catch(err => { console.error(err); process.exitCode=1; });
