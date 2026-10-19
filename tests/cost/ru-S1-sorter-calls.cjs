// RU-S1 (Firestore cost, 18 Oct 2026): what the server calls the Sorter's own page makes cost, measured on the REAL handlers over the cost meter's in-memory
// Firestore: the calls of a page load (charm-nest-bridge.js bootBridge, charm-nest-1.html cloudProbe), of the ten-minute order check (Arrivals.check), and the
// small timers that are left (the master admin's Alert read, an open order's Team thread, the Save keeper's repair beat). The Library's live read and the placement
// feed have their own tests (library-rev-list.cjs, fc3-orders-feed-cost.cjs); the numbers they print are the ones the report quotes.
// The SIZES are assumptions (3,000 + 2,000 + 300 learned-map documents, 7,000 master entries, 600 custom orders, 300 runs, 200 saved designs); the structure is the code's.
//   node tests/cost/ru-S1-sorter-calls.cjs
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const root = path.join(__dirname, '..', '..');
const m = meter.create(); m.install();
const realLog = console.log; console.log = () => {}; console.warn = () => {};
delete process.env.EDIT_PASSCODE; process.env.CHARM_NEST_HOLDERS_MS = '0';
const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js')), door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const call = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }); return JSON.parse(r.body || '{}'); };
const ago = i => meter.Timestamp.fromMillis(Date.now() - 3600e3 - i * 1000), seed = {};
for (let i = 0; i < 300; i++) seed['Charm_Nest_Calibration/cal' + i] = { sheetId: 's' + i, metal: 'gold', count: 25, cv: .3, largestFrac: .1, density: .6, placedAll: true, createdAt: ago(i) };
for (let i = 0; i < 3000; i++) seed['Charm_Nest_Library/h' + i] = { hash: 'h' + i, name: 'c' + i, updatedAt: ago(i) };
for (let i = 0; i < 300; i++) seed['Charm_Nest_Sheets/sheet' + i] = { id: 'sheet' + i, archived: false, status: 'complete', updatedAt: ago(i) };
for (let i = 0; i < 3000; i++) seed['Charm_Sku_Aliases/' + (4000000000 + i)] = { listingId: String(4000000000 + i), by: 'o', sku: 'SKU-' + i, v: 2, updatedAt: ago(i) };
for (let i = 0; i < 2000; i++) seed['Charm_Option_Map/' + (5000000000 + i)] = { listingId: String(5000000000 + i), map: { size: { small: { field: 'size', value: 'S', by: 'x', at: 1 } } }, updatedAt: ago(i) };
for (let i = 0; i < 300; i++) seed['Charm_Sku_NoDesign/nd' + i] = { sku: 'ND-' + i, by: 'o', createdAt: ago(i) };
for (let i = 0; i < 7000; i++) seed['Charm_Master_Index/SKU' + i] = { sku: 'SKU' + i, masterHash: 'aa', widthPt: 40, heightPt: 30, indexedAt: ago(i), updatedAt: ago(i) };
for (let f = 0; f < 6; f++) seed['Charm_Master_Files/ab' + f + 'cdef0123'] = { masterHash: 'ab' + f + 'cdef0123', name: 'M' + f, skus: Array.from({ length: 300 }, (_, i) => 'SKU-' + i), indexedAt: ago(f) };
for (let i = 0; i < 600; i++) seed['Charm_Custom_Orders/' + (4200000000 + i) + '_' + (900000 + i)] = { key: (4200000000 + i) + '_' + (900000 + i), state: 'done', updatedAtMs: Date.now() - i * 60000, stamps: [] };
for (let r = 0; r < 300; r++) seed['Charm_Nest_Runs/run-old-' + r] = { runId: 'run-old-' + r, day: '2026-09-01', step: 'done', status: 'complete', mode: 'auto', lines: { a: { state: 'written' } }, updatedAt: ago(r * 3600), createdAt: ago(0) };
for (let i = 0; i < 200; i++) seed['Charm_Custom_Saves/' + (4300000000 + i)] = { rid: String(4300000000 + i), listingId: String(1718000000 + i), sku: 'SKU' + i, phase: 'repo', alias: 'none', etsy: { state: 'linked' }, by: 'o', at: Date.now() - i * 1000, createdAt: ago(0), updatedAt: ago(0) };   // (whole saves: nothing to repair)
for (let i = 0; i < 40; i++) seed['Brites_Orders/4400000001/messages/m' + i] = { text: 'hello ' + i, senderName: 'A', senderRole: 'staff', timestamp: meter.Timestamp.fromMillis(Date.now() - (40 - i) * 600e3) };
seed['config/stationAdmins'] = { names: ['Paul K'] };
m.db.seed(seed);

(async () => {
  const rows = [];
  const run = async (name, fn, max) => {
    const a = m.snapshot(), out = await m.op(name, fn), d = m.since(a), reads = d.reads + d.aggs;
    rows.push({ name, reads, kb: Math.round(d.bytes / 102.4) / 10, writes: d.writes, max });
    if (max) meter.assertMax({ reads, bytes: d.bytes, writes: d.writes }, max, name);
    return out;
  };
  /* a page load */
  await run('ping (page load, with the 200 calibration rows)', () => call({ op: 'ping' }), { reads: 260 });
  await run('ping (again, within 10 min)', () => call({ op: 'ping' }), { reads: 8 });
  const sig = {};
  for (const [k, op] of [['al', 'aliasGet'], ['om', 'optionMapGet'], ['nd', 'noDesignGet']]) { const r = await run(op + ' WHOLE (a page that holds none: every load before RU-S1)', () => call({ op }), { reads: k === 'al' ? 3100 : k === 'om' ? 2100 : 350 }); sig[k] = { sig: r.sig, n: r.n }; }
  for (const [k, op] of [['al', 'aliasGet'], ['om', 'optionMapGet'], ['nd', 'noDesignGet']]) await run(op + ' with the signature of the kept copy (a load now)', () => call({ op, ifSig: sig[k].sig, since: sig[k] }), { reads: 6, bytes: 400 });
  const f1 = await run('masterListFiles, no signature', () => call({ op: 'masterListFiles' }), { reads: 25 });
  await run('masterListFiles ifFilesSig (unchanged: every check)', () => call({ op: 'masterListFiles', ifFilesSig: f1.filesSig }), { reads: 14, bytes: 1000 });
  await run('customGet list, a cold instance (a load, then every 30 min)', () => call({ op: 'customGet' }), { reads: 650 });
  await run('customGet list, a warm instance', () => call({ op: 'customGet' }), { reads: 3 });
  const lines = Array.from({ length: 300 }, (_, i) => (4500000000 + i) + '_' + (800000 + i)), cards = Array.from({ length: 10 }, (_, i) => 'custom:' + (4500000000 + i) + ':CUSTOM-N');
  const c1 = await run('customSheetGet, a page that holds no revision (300 lines, 10 cards)', () => call({ op: 'customSheetGet', keys: cards, lineKeys: lines, legacyLineKeys: [], wantRev: true }), { reads: 330 });
  await run('customSheetGet ifRev (unchanged: every check)', () => call({ op: 'customSheetGet', keys: [], lineKeys: [], legacyLineKeys: [], ifRev: c1.rev, heldKeys: cards, heldLines: lines }), { reads: 3 });
  await run('runList limit 10 (a load with no saved workspace)', () => call({ op: 'runList', limit: 10 }), { reads: 12 });
  await run('customSavePending (a load)', () => call({ op: 'customSavePending' }), { reads: 3 });
  /* the small timers */
  await run('customSaveRepair etsy:true, a full pass over 200 whole saves (the page beat and the cron, at most every 5 min)', () => call({ op: 'customSaveRepair', etsy: true }), { reads: 160, writes: 4 });
  await run('customSaveRepair etsy:true, inside the 5 minutes', () => call({ op: 'customSaveRepair', etsy: true }), { reads: 2 });
  const asks = q => door.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: q });
  await run('messagesFor since: nothing new (an open order, every 20 s / 30 s)', () => asks({ messagesFor: '4400000001', limit: '80', since: String(Date.now()) }), { reads: 2 });
  await run('messagesFor whole thread, 40 messages (on opening, then every 3 min)', () => asks({ messagesFor: '4400000001', limit: '80' }), { reads: 45 });
  await run('repoAlerts, master admin (every minute while the tab is shown)', () => call({ op: 'repoAlerts', by: 'Paul K' }), { reads: 4 });
  await run('repoAlerts, again', () => call({ op: 'repoAlerts', by: 'Paul K' }), { reads: 2 });
  /* the order check's own writes */
  await run('arrivalRecord, 1 new order (a check that found one)', () => call({ op: 'arrivalRecord', orders: [{ id: '4600000001', createTs: 1 }] }), { reads: 8, writes: 4 });

  console.log = realLog;
  realLog('\nthe Sorter page\'s own server calls (real handlers, meter)              reads     KB  writes');
  for (const r of rows) realLog(r.name.slice(0, 92).padEnd(94) + String(r.reads).padStart(5) + String(r.kb).padStart(8) + String(r.writes).padStart(5));
  const get = s => rows.find(r => r.name.startsWith(s));
  const whole = ['aliasGet WHOLE', 'optionMapGet WHOLE', 'noDesignGet WHOLE'].reduce((a, s) => a + get(s).reads, 0), held = ['aliasGet with', 'optionMapGet with', 'noDesignGet with'].reduce((a, s) => a + get(s).reads, 0);
  assert(whole > 5000 && held <= 12, `the learned maps: ${whole} reads whole, ${held} with the kept signature`);
  realLog(`\nlearned maps at a page load: ${whole.toLocaleString()} reads whole -> ${held} with the copy kept on the computer`);
  realLog('ru-S1-sorter-calls: within budget');
  process.exit(0);
})().catch(e => { console.log = realLog; console.error(e); process.exit(1); });
