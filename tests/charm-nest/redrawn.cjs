// REDRAWN, KEEPNOW, NEWLINE (Paul, 22 Nov 2026). Offline: the real driver pass, the real requests unit and library handler on the in-memory cloud of _cloudFakes.cjs; no network, no Etsy.
//   1  REDRAWN: 9MM_DISC_8391 updated in the Charm Editor (its drawing 4840aa6f → 250728b9, the master file's hash unchanged): the open line whose 4 pieces hold the old drawing is found to make
//      again; a line already on the new drawing, a fitted size of the new drawing ("250728b9~10MM") and another SKU are not
//   2  KEEPNOW: the repository's "Make the open pieces again" (keepsync) never waits for the next reading of the orders: in ONE pass, with no new reading, it has picked the line and taken
//      its old pieces off their sheet to be made again
//   3  NEWLINE: a Change SKU on a new order (its pieces made, on no sheet yet) places them now and answers with them on a sheet, never "made again: 0 piece(s)", failed
//   NODE_PATH=<deps> node tests/charm-nest/redrawn.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const T0 = Date.UTC(2026, 10, 23, 1, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0; const ok = (n, c, d) => { log0((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const lib = F.load('charmNestLibrary.js'), spy = F.spy(lib), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), CmdUnit = F.load('_charmNestCmdUnit.js'), Exec = F.load('_charmNestCmdExec.js');
const R = 'run-2026-11-22-redrawn', SKU = '9MM_DISC_8391', MH = 'c5fdaf31', OLD = '4840aa6f', NEW = '250728b9';
const ITEMS = [CmdUnit.units.passItems[0]];
const STEPS = {}; for (const s of Drive.RUN_STEPS) STEPS[s] = [async () => ({ wait: 'test', why: 'the test holds the run at its step' })];

function world(steps) {
  F.reset(); F.clock.set(T0); spy.ops.length = 0;
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: steps || ['nest', 'pool', 'cmd:keepsync', 'cmd:remakeline'], flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-22', sheets: {}, holds: {}, errors: [], orders: [], lines: {} });
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  F.seed('Charm_World/summary', { at: T0 - 120000 });   // (the last reading of the orders: two minutes before every request here)
  F.seed('Charm_Master_Index/' + SKU, { sku: SKU, masterHash: MH, charmHash: NEW, widthPt: 28.022, heightPt: 28.022, drawnPt: [25.512, 25.512], edited: { at: T0 - 60000, by: 'Paul_K', from: OLD, to: NEW, size: null, n: 1 } });
  F.seed('Charm_Master_Index/MOON_1234', { sku: 'MOON_1234', masterHash: 'm-hash', charmHash: 'moon1' });
}
/** An open line of the run: n pieces of sku, each holding charmHash, on sheetId (or on no sheet). */
function line(key, sku, n, charmHash, sheetId) {
  const run = F.doc('Charm_Nest_Runs/' + R), ids = [];
  for (let k = 1; k <= n; k++) {
    const poolId = `${key}_${k}`; ids.push(poolId);
    F.seed('Charm_Pool/' + poolId, { poolId, state: 'ready', sheetId: sheetId || null, runId: R, orderId: key.split('_')[0], material: 'gold', lineKey: key, sku, charmHash, masterHash: sku === SKU ? MH : 'm-hash', areaPt2: 400, widthPt: 20, heightPt: 20, quantity: n, copy: k });
  }
  run.lines[key] = { orderId: key.split('_')[0], material: 'gold', state: 'pooled', poolIds: ids, pieceCount: n, sku };
  F.seed('Charm_Nest_Runs/' + R, run);
  F.seed('Charm_World_Lines/' + key, { at: T0 - 120000, data: JSON.stringify({ spec: { designSku: sku, material: 'gold' } }) });
  return ids;
}
function draft(id, page, ids) {
  const charms = ids.map((poolId, i) => ({ id: `pool:T:${poolId}`, poolId, order: poolId.split('_')[0], metal: 'gold', areaPt2: 400, widthPt: 20, heightPt: 20, name: poolId, sourceId: 'pool:T', excluded: false }));
  F.seed('Charm_Nest_Sheets/' + id, { runId: R, metal: 'gold', page, draft: true, setId: null, charms, poolIds: ids.slice(), placements: charms.map((c, i) => ({ id: c.id, cxPt: 30 + 25 * i, cyPt: 30, angle: 0 })),
    orders: [...new Set(charms.map(c => c.order))], layoutRev: 3, contentRev: 3, status: 'nested', cardStartedAt: T0 - 600000, createdAt: T0 - 600000, fileBase: null });
  for (const p of ids) F.seed('Charm_Pool/' + p, Object.assign({}, F.doc('Charm_Pool/' + p), { sheetId: id, state: 'nested' }));
}
async function pass() {
  const d = await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true });
  if (!d || !d.nonce) return { notDispatched: true };
  const out = await Drive.pass({ db: F.db, Lib: lib, nonce: d.nonce, steps: STEPS, lanes: Drive.defaultLanes(), passItems: ITEMS, sleep: async () => {}, renewMs: 0, budgetMs: 600000, log: () => {} }).catch(e => ({ error: String((e && e.stack) || e) }));
  F.clock.advance(61000);
  return out;
}
function putCmd(type, args) { const id = 'c' + type + Math.random().toString(36).slice(2, 6); const cur = F.doc('Charm_Nest_Run_Cmds/' + R) || { runId: R, cmds: {} }; cur.cmds[id] = { id, type, args, by: 'Paul_K', dev: 'dev-paul', at: Date.now(), state: 'waiting' }; F.seed('Charm_Nest_Run_Cmds/' + R, cur); return id; }
const cmdOf = id => ((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {})[id] || {};

(async () => {
  /* ═══ 1 · REDRAWN: the pieces of the replaced drawing are found ═══ */
  {
    world();
    line('4231992262_5290446926', SKU, 4, OLD, null);   // the order's 4 discs: the old drawing (29.8 mm)
    line('4239000001_5300000001', SKU, 1, NEW, null);   // already on the new drawing
    line('4239000002_5300000002', SKU, 1, NEW + '~10MM', null);   // a fitted size of the new drawing
    line('4239000003_5300000003', 'MOON_1234', 1, OLD, null);   // another SKU (the same hash by chance)
    const run = F.doc('Charm_Nest_Runs/' + R), found = await Exec.staleLines({ db: F.db, run: Object.assign({}, run, { runId: R }), runId: R }, [SKU]);
    ok('1 · REDRAWN: the order whose 4 discs hold the drawing the Charm Editor replaced is found to make again (the master file\'s hash is the same)', found.includes('4231992262_5290446926'), found);
    ok('1 · a line on the new drawing, a fitted size of it, and another SKU are not', found.length === 1, found);
    const before = Exec.staleLines.toString();
    ok('1 · (the rule is the drawing\'s hash the update replaced, edited.from)', /redrawn\(e, r\)/.test(before));
  }
  /* ═══ 2 · KEEPNOW: no wait for the next reading of the orders ═══ */
  {
    world();
    const ids = line('4231992262_5290446926', SKU, 4, OLD, null);
    draft('d-346bcfde', 4, ids.concat(line('4239000001_5300000001', SKU, 1, NEW, null)));
    const id = putCmd('keepsync', { ids: [SKU] });
    const p = await pass(), c = cmdOf(id), got = await lib.call({ op: 'runGet', runId: R }, { fence: null }).catch(() => null), L = ((got && got.run && got.run.lines) || {})['4231992262_5290446926'] || {};
    const vac = spy.ops.filter(o => o.op === 'cmdVacate'), D = F.doc('Charm_Nest_Sheets/d-346bcfde') || {};
    ok('2 · the pass ran', p && !p.error && !p.notDispatched, p);
    ok('2 · KEEPNOW: in ONE pass, with no new reading of the orders, the request is past "Waiting for the next reading of the orders"', !/next reading of the orders/.test(String(c.step || '') + String(c.text || '')), c);
    ok('2 · its old pieces were taken off their sheet to be made again (the line went back to the intake), the other line kept', vac.length >= 1 && ids.every(x => !(D.poolIds || []).includes(x)) && (D.poolIds || []).includes('4239000001_5300000001_1') && !!L.repoolChanged, { vac: vac.length, D: D.poolIds, L, step: c.step, text: c.text });
  }
  /* ═══ 3 · NEWLINE: a new order's pieces (on no sheet) are placed now, the request answers with them ═══ */
  {
    world();
    draft('d-room', 2, line('4239000009_5300000009', 'MOON_1234', 1, 'moon1', null));
    const ids = line('4244010642_5306465331', 'SHEEP 3', 2, 'sheep3', null);
    F.seed('Charm_Master_Index/SHEEP 3', { sku: 'SHEEP 3', masterHash: 'sh-hash', charmHash: 'sheep3', widthPt: 25.7, heightPt: 39.3, areaPt2: 577 });
    const id = putCmd('remakeline', { ids: ['4244010642_5306465331'], label: 'Change SKU: SHEEP3-CO → SHEEP 3' });
    F.seed(Exec.WORK + '/' + id, { stage: 'back', ready: ['4244010642_5306465331'], late: [], why: [], todo: ['4244010642_5306465331'], vac: { '4244010642_5306465331': { sheets: [] } }, at: Date.now(), state: 'working', type: 'remakeline', firstAt: Date.now() });
    const p = await pass(), c = cmdOf(id);
    const on = ids.map(x => (F.doc('Charm_Pool/' + x) || {}).sheetId || F.list('Charm_Nest_Sheets').find(s => (s.poolIds || []).includes(x)) && 'listed');
    ok('3 · the pass ran', p && !p.error && !p.notDispatched, p);
    ok('3 · NEWLINE: the 2 pieces of the new order are placed on a sheet now, never left on no sheet', on.every(Boolean), { on, step: c.step, text: c.text });
    ok('3 · and the request is never answered "made again: 0 piece(s)", failed', !(c.state === 'failed' && /0 piece/.test(c.text || '')) && (c.state === 'done' ? /2 piece/.test(c.text || '') : true), c);
  }
  ok('no Etsy call, no network outside our own functions', F.etsy.calls === 0 && !F.net.tripwire.length && !F.net.other.length);
  log0(failed ? `FAIL redrawn (${failed})` : 'PASS redrawn'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL redrawn: ' + (e && e.stack || e)); process.exit(1); });
