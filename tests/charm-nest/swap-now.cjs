// SWAPNOW + READNOW (Paul, 23 Nov 2026, order 4245035194: "It took over 3 min to update the SKU", "You have a budget of 30 sec", "I don't understand why things are being put on removed put back ... Make this simple"):
//   1  the old piece never leaves its sheet: its entry is replaced by the new drawing in one write; every other piece stays byte for byte; a sheet that is nesting is not waited for, its result is refused (contentRev moved)
//   2  cut on the laser, a recorded cut, a green line, a set sent: the piece stays, as before
//   3  only the order's own line is read again, at once, with the saved link as it is now (no wait for every open order)
//   4  a piece already made right since the request is answered as it is: nothing is made again
//   NODE_PATH=<deps> node tests/charm-nest/swap-now.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const path = require('path'), T0 = Date.UTC(2026, 10, 23, 18, 0, 0), F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {}; let n = 0, failed = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 700))); if (!c) failed++; };
(async () => {
  const lib = F.load('charmNestLibrary.js'), Exec = F.load('_charmNestCmdExec.js'), R = 'run-swap', K = '4236768670_5297762073', O2 = '4231985849_5297762074', P1 = K + '_1', P2 = O2 + '_1';
  const place = (id, x, y) => ({ id, cxPt: x, cyPt: y, angle: 0, scale: 0.971 });
  const seed = o => { F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: ['pull', 'pool', 'nest', 'files'] }); lib._driveForget(); F.seed('Charm_Nest_Open/run', { runId: R, at: T0 });
    F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', releasePolicy: 2, lines: { [K]: { orderId: '4236768670', state: 'pooled', sku: 'LEMON', poolIds: [P1] } } });
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 } });
    F.seed('Charm_Nest_Drive/lease', { nonce: 'dn', state: 'running', heartbeat: true, startedAt: T0, beatAt: T0, at: T0 });
    for (const [p, k, sku] of [[P1, K, 'LEMON'], [P2, O2, 'MOON']]) F.seed('Charm_Pool/' + p, { poolId: p, orderId: k.split('_')[0], lineKey: k, sku, state: 'ready', sheetId: 'ss-1', material: 'silver', runId: R, quantity: 1 });
    F.seed('Charm_Nest_Sheets/ss-1', Object.assign({ id: 'ss-1', runId: R, metal: 'silver', fileBase: 'SS_working_ss-1', draft: true, archived: false, layoutRev: 9, contentRev: 7, poolIds: [P1, P2], orders: ['4236768670', '4231985849'],
      charms: [{ id: 'c-old', poolId: P1, order: '4236768670', sku: 'LEMON', hash: 'h1', name: 'LEMON' }, { id: 'c-2', poolId: P2, order: '4231985849', sku: 'MOON', hash: 'h2', name: 'MOON' }], placements: [place('c-old', 20, 20), place('c-2', 60, 20)], status: 'ready', nestJob: { id: 'nest-x', status: 'running', at: T0 } }, o || {})); };
  const fence = { path: 'Charm_Nest_Drive/lease', nonce: 'dn', runId: R }, call = (b, u) => lib.call(b, { unit: 'swap|' + u + '|' + Math.random(), fence });
  seed(); const was = F.doc('Charm_Nest_Sheets/ss-1');
  let r = await call({ op: 'cmdVacate', runId: R, lineKey: K, rid: '4236768670', sheetId: 'ss-1', poolIds: [P1], layoutBase: 1, contentBase: 1, by: 'Ana_M', why: 'Change SKU', swap: true }, 'v'), s = F.doc('Charm_Nest_Sheets/ss-1');
  ok('1 · the old piece stays on its sheet while the new one is made, though the sheet is nesting', r.ok === true && JSON.stringify(s.charms) === JSON.stringify(was.charms) && JSON.stringify(s.placements) === JSON.stringify(was.placements) && s.layoutRev === 9 && s.contentRev === 7, r);
  ok('1 · its record says it is the old one, so the new one can be made', F.doc('Charm_Pool/' + P1).state === 'superseded' && F.doc('Charm_Pool/' + P1).sheetId === null);
  F.seed('Charm_Pool/' + P1, { poolId: P1, orderId: '4236768670', lineKey: K, sku: 'LEMON_7271', state: 'ready', material: 'silver', runId: R, quantity: 1 });
  r = await call({ op: 'cmdPutBack', runId: R, lineKey: K, rid: '4236768670', sheetId: 'ss-1', poolIds: [P1], charms: [{ id: 'c-new', poolId: P1, order: '4236768670', sku: 'LEMON_7271', hash: 'h9', name: 'LEMON_7271', sourceId: 'x' }], keepBacks: true, layoutBase: 1, contentBase: 1, by: 'Ana_M', swap: true }, 'b'); s = F.doc('Charm_Nest_Sheets/ss-1');
  ok('1 · one write puts the new drawing in its entry: the old is gone, the new is listed', r.ok === true && !s.charms.some(c => c.id === 'c-old') && s.charms.some(c => c.id === 'c-new' && c.sku === 'LEMON_7271') && s.poolIds.filter(x => x === P1).length === 1, { r, c: s.charms });
  ok('1 · every other piece stays byte for byte, the place of the old one is free', JSON.stringify(s.placements) === JSON.stringify([was.placements[1]]) && JSON.stringify(s.charms.filter(c => c.id === 'c-2')) === JSON.stringify([was.charms[1]]), s.placements);
  ok('1 · a nest under way can no longer write over it; the sheet owes the new piece its place (placed pieces held)', s.contentRev === 8 && s.layoutRev === 10 && !!s.nestOwed && s.appendOnly === true && s.filesOwed > 0);
  ok('1 · the new piece\'s record is on the sheet', F.doc('Charm_Pool/' + P1).sheetId === 'ss-1');
  for (const [m, o] of [['cut on the laser', { laserDoneAt: T0 - 1 }], ['its cut recorded', { roseCutAt: T0 - 1 }], ['behind a green line', { rosePlanJson: '{}' }]]) { seed(o); r = await call({ op: 'cmdVacate', runId: R, lineKey: K, rid: '4236768670', sheetId: 'ss-1', poolIds: [P1], by: 'Ana_M', swap: true }, 'k' + m); ok('2 · ' + m + ': the piece stays, as before', r.keep === true && F.doc('Charm_Pool/' + P1).state === 'ready', r); }
  // 3 · the reading
  const World = F.load('_charmNestWorld.js'); seed();
  const line = { transactionId: '5297762073', listingId: '1816160635', sku: 'LEMON', title: 'Lemon charm necklace', quantity: 1, metalKey: 'silver', metalLabel: 'Sterling Silver', personalization: [], buyerMessage: '', variations: [{ name: 'Metal', value: 'Sterling Silver' }] };
  F.seed('Charm_World_Lines/' + K, { key: K, at: T0 - 60000, gone: false, data: JSON.stringify({ key: K, receiptId: '4236768670', order: { receiptId: '4236768670', lines: [line] }, line, spec: { designSku: 'LEMON', material: 'silver' }, problems: [], state: 'pooled', etsyOpen: true }) });
  const entries = ['LEMON', 'LEMON_7271'].map(sku => ({ sku, aiPath: 'charmnest/master/' + sku + '.ai', masterHash: 'm' + sku, widthPt: 28, heightPt: 28 })), asked = [];
  const ops = { masterList: async () => ({ ok: true, index: 'ix1', entries }), optionMapGet: async () => ({ maps: {} }), aliasGet: async () => ({ aliases: { '1816160635': { listingId: '1816160635', bySku: { LEMON: 'LEMON_7271' }, v: 2 } } }), noDesignGet: async () => ({ list: { patterns: [], skus: [], rows: [] } }), customGet: async () => ({ records: {} }), listingSkus: async b => { asked.push(b); return { tables: {} }; }, customSheetGet: async () => ({ records: {} }) };
  const got = await World.readNow({ db: F.db, ops, keys: [K], lines: { [K]: { state: 'pooled', poolIds: [P1] } } });
  ok('3 · the line reads LEMON_7271 at once, from the link saved a moment ago', got.size === 1 && got.get(K).spec && got.get(K).spec.designSku === 'LEMON_7271', got.get(K) && got.get(K).spec && { d: got.get(K).spec.designSku, p: got.get(K).problems });
  ok('3 · kept where the piece maker reads it', JSON.parse(F.doc('Charm_World_Lines/' + K).data).spec.designSku === 'LEMON_7271');
  // 4 · already made right
  seed(); F.seed('Charm_World_Lines/' + K, { key: K, at: T0, gone: false, data: JSON.stringify({ key: K, receiptId: '4236768670', order: { receiptId: '4236768670', lines: [line] }, line, spec: { designSku: 'LEMON', material: 'silver' }, problems: [], state: 'pooled', etsyOpen: true }) });
  F.seed('Charm_Pool/' + P1, { poolId: P1, orderId: '4236768670', lineKey: K, sku: 'LEMON_7271', state: 'ready', sheetId: 'ss-1', material: 'silver', runId: R, quantity: 1, createdAt: new Date(T0 + 40000) });
  const read = b => (ops[b.op] ? ops[b.op](b) : lib.call(b, {})), ctx = { db: F.db, run: { runId: R, lines: { [K]: { orderId: '4236768670', state: 'pooled', sku: 'LEMON', poolIds: [P1] } } }, runId: R, cloudSteps: ['nest', 'pool'], read, call: (b, u) => call(b, u || 'x'), say: () => {} };
  r = await Exec.EXEC.remakeline({ ctx, cmd: { id: 'c1', at: T0, by: 'Paul_K' }, a: { ids: [K] }, work: {} });
  ok('4 · made right 40 s after the request by the intake: answered as it is, nothing made again', !!(r.done && r.done.ok && /Made as it reads now/.test(r.done.text)) && F.doc('Charm_Pool/' + P1).state === 'ready', r);
  F.seed('Charm_Pool/' + P1, { poolId: P1, orderId: '4236768670', lineKey: K, sku: 'LEMON', state: 'ready', sheetId: 'ss-1', material: 'silver', runId: R, quantity: 1, createdAt: new Date(T0 - 86400e3) });
  r = await Exec.EXEC.remakeline({ ctx, cmd: { id: 'c2', at: T0, by: 'Paul_K' }, a: { ids: [K] }, work: {} });
  ok('4 · the old charm still on its sheet: it goes straight to the swap, no wait for every order', !!(r.wait && r.wait.work.stage === 'vacate'), r);
  log0(`${failed ? 'FAIL' : 'PASS'} swap-now (${n - failed} of ${n})`); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL swap-now: ' + (e && e.stack || e)); process.exit(1); });
