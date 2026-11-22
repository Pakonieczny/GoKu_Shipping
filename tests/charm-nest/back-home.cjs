// BACKHOME (Paul, 22 Nov 2026: order 4231992262's new discs "should've gone back to the original sheet"): an order line made again goes back to the sheet its pieces were taken from
// (their records' vacatedFrom), even a set's sheet (its own order's place), and nowhere else; a set sent to the station keeps what it has. Offline: the real driver pass, the real library
// handler and ops on the in-memory cloud of _cloudFakes.cjs; no network, no Etsy.
//   NODE_PATH=<deps> node tests/charm-nest/back-home.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const T0 = Date.UTC(2026, 10, 21, 18, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0; const ok = (n, c, d) => { log0((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const lib = F.load('charmNestLibrary.js'), spy = F.spy(lib), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Standby = F.load('_charmNestStandby.js');
const Combine = F.load('_charmNestCombine.js'), CmdUnit = F.load('_charmNestCmdUnit.js'), Steps = F.load('_charmNestSteps.js');
const R = 'run-2026-11-22-backhome';
const ITEMS = [CmdUnit.units.passItems[0]];
const STEPS = {}; for (const s of Drive.RUN_STEPS) STEPS[s] = [async () => ({ wait: 'test', why: 'the test holds the run at its step' })];
const logs = [];

function setDrive(mode, steps) { F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: steps || ['nest', 'cmd:remakeline', 'cmd:skuonce'], flippedAt: T0 - 3600e3 }); lib._driveForget(); }
function world(o = {}) {
  F.reset(); F.clock.set(T0); logs.length = 0; spy.ops.length = 0;
  setDrive(o.mode || 'on', o.steps);
  F.seed('Charm_Nest_Runs/' + R, Object.assign({ runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [], lines: {} }, o.run || {}));
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
}
/** A draft record of the run with its pieces (orders: [[orderId, n], ...]), every piece seated, its pool rows and its run lines. */
function sheet(id, metal, page, orders, extra = {}) {
  const charms = [], placements = [], lines = (F.doc('Charm_Nest_Runs/' + R) || {}).lines || {};
  for (const [rid, n] of orders) {
    const ids = [];
    for (let k = 1; k <= n; k++) {
      const poolId = `${rid}_${metal}_${k}`, c = { id: `pool:T_${metal}:${poolId}`, poolId, order: String(rid), metal, areaPt2: 400, widthPt: 20, heightPt: 20, name: `${rid} · T`, sourceId: `pool:T_${metal}`, excluded: false };
      if (extra.pin && extra.pin.includes(poolId)) { c.manual = { by: 'Ann', at: T0 - 3600e3 }; c.pinned = { cxPt: 40, cyPt: 40, angle: 0 }; }   // (a person's pin: sheetLayoutPatch's manual mark and place)
      charms.push(c); placements.push({ id: c.id, cxPt: 30 + 25 * placements.length, cyPt: 30, angle: 0 }); ids.push(poolId);
      F.seed('Charm_Pool/' + poolId, { poolId, state: 'pooled', sheetId: id, runId: R, orderId: String(rid), material: metal, lineKey: `${rid}_${metal}` });
    }
    lines[`${rid}_${metal}`] = { orderId: String(rid), material: metal, state: 'pooled', poolIds: ids, pieceCount: n };
  }
  F.seed('Charm_Nest_Runs/' + R, Object.assign(F.doc('Charm_Nest_Runs/' + R), { lines }));
  const rec = Object.assign({ runId: R, metal, page, draft: true, setId: null, charms, poolIds: charms.map(c => c.poolId), placements, orders: [...new Set(orders.map(o => String(o[0])))],
    layoutRev: 3, contentRev: 3, status: 'nested', cardStartedAt: T0 - (20 - page) * 60000, createdAt: T0 - (20 - page) * 60000, fileBase: null }, extra.rec || {});
  F.seed('Charm_Nest_Sheets/' + id, rec);
  return rec;
}
async function pass(items) {
  const d = await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true });
  if (!d || !d.nonce) return { notDispatched: true };
  const out = await Drive.pass({ db: F.db, Lib: lib, nonce: d.nonce, steps: STEPS, lanes: Drive.defaultLanes(), passItems: items || ITEMS, sleep: async () => {}, renewMs: 0, budgetMs: 600000, log: t => logs.push(t) }).catch(e => ({ error: String((e && e.stack) || e) }));
  F.clock.advance(61000);
  return out;
}
const S = id => F.doc('Charm_Nest_Sheets/' + id) || {};
const jobsOf = id => F.list('Charm_Nest_Jobs').filter(j => j.sheetId === id);
const moves = () => spy.ops.filter(o => o.op === 'cmdMovePieces');
function putCmd(type, args) { const id = 'c' + type + Math.random().toString(36).slice(2, 6); const cur = F.doc('Charm_Nest_Run_Cmds/' + R) || { runId: R, cmds: {} }; cur.cmds[id] = { id, type, args, by: 'Ann', dev: 'dev-ann', at: Date.now(), state: 'waiting' }; F.seed('Charm_Nest_Run_Cmds/' + R, cur); return id; }
const cmdOf = id => ((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {})[id] || {};

(async () => {
  const home = (ids, sid) => { for (const id of ids) F.seed('Charm_Pool/' + id, Object.assign({}, F.doc('Charm_Pool/' + id), { vacatedFrom: sid })); };
  /* ═══ 1 · the order's 4 new pieces on a new draft go back to the set's sheet they were taken from ═══ */
  {
    world();
    sheet('gold-h', 'gold', 1, [[8101, 2], [8102, 1]], { rec: { draft: false, setId: 'set-h', setSeq: 1, fileBase: 'GF_Nov.18.26_Set-1_Sheet-1' } }); F.seed('Charm_Nest_Sets/set-h', { setId: 'set-h', runId: R, sheetIds: ['gold-h'], status: 'awaiting review', seq: 1 });
    sheet('gold-d5', 'gold', 5, [[4231992262, 4], [8501, 1]]);
    const ids = [1, 2, 3, 4].map(k => `4231992262_gold_${k}`); home(ids, 'gold-h');
    const id = putCmd('backhome', { ids: ['4231992262_gold'] });
    const p = await pass();
    const H = S('gold-h'), D = S('gold-d5'), c = cmdOf(id);
    ok('1 · the pass ran', p && p.ok && !p.error, p);
    ok('1 · the 4 pieces are on the set\'s sheet they came from (listed and drawn), and no longer on the draft', ids.every(x => H.poolIds.includes(x) && H.charms.some(ch => ch.poolId === x)) && ids.every(x => !D.poolIds.includes(x)) && D.poolIds.includes('8501_gold_1'), { H: H.poolIds, D: D.poolIds });
    ok('1 · the pieces\' records name that sheet, and it owes a nest that only adds (its other pieces stay where they are)', ids.every(x => F.doc('Charm_Pool/' + x).sheetId === 'gold-h') && jobsOf('gold-h').length > 0 && jobsOf('gold-h').every(j => j.mode !== 'fresh'), jobsOf('gold-h'));
    ok('1 · the request is at its nest, then answered', /Nesting the order on its sheet|back on/.test(String(c.step || c.text || '')), c);
  }
  /* ═══ 2 · a set's sheet still takes no other order: pieces that were not taken from it are refused ═══ */
  {
    world();
    sheet('gold-h', 'gold', 1, [[8101, 2]], { rec: { draft: false, setId: 'set-h', setSeq: 1 } }); F.seed('Charm_Nest_Sets/set-h', { setId: 'set-h', runId: R, sheetIds: ['gold-h'], status: 'awaiting review', seq: 1 });
    sheet('gold-x', 'gold', 2, [[8201, 1]]);
    sheet('gold-d5', 'gold', 5, [[8301, 2]]);
    home(['8301_gold_1', '8301_gold_2'], 'gold-x');
    const before = JSON.stringify(S('gold-h'));
    const id = putCmd('backhome', { ids: ['8301_gold'] }); await pass();
    ok('2 · pieces taken from ANOTHER sheet go back there, never onto the set\'s sheet', JSON.stringify(S('gold-h')) === before && ['8301_gold_1', '8301_gold_2'].every(x => S('gold-x').poolIds.includes(x)), { h: S('gold-h').poolIds, x: S('gold-x').poolIds });
    const L = await lib.call({ op: 'cmdMovePieces', runId: R, from: { sheetId: 'gold-x' }, to: { sheetId: 'gold-h' }, poolIds: ['8201_gold_1'], by: 'test' }, { fence: null }).catch(e => ({ status: 500, error: String(e) }));
    ok('2 · the move op still refuses a new order onto a set\'s sheet (SETSHUT)', !L || L.status >= 400 || !L.ok, L);
  }
  /* ═══ 3 · a set that went to the station takes nothing back ═══ */
  {
    world();
    sheet('gold-h', 'gold', 1, [[8101, 2]], { rec: { draft: false, setId: 'set-h', setSeq: 1 } }); F.seed('Charm_Nest_Sets/set-h', { setId: 'set-h', runId: R, sheetIds: ['gold-h'], status: 'committed', committedAt: T0 - 60000, seq: 1 });
    sheet('gold-d5', 'gold', 5, [[8401, 2]]); home(['8401_gold_1', '8401_gold_2'], 'gold-h');
    const before = JSON.stringify(S('gold-h'));
    const id = putCmd('backhome', { ids: ['8401_gold'] }); await pass();
    ok('3 · a set sent to the station keeps what it has: refused in words, nothing moved', JSON.stringify(S('gold-h')) === before && cmdOf(id).state === 'failed' && /went to the station/.test(cmdOf(id).text || ''), cmdOf(id));
  }
  ok('no Etsy call, no network outside our own functions', F.etsy.calls === 0 && !F.net.tripwire.length && !F.net.other.length);
  log0(failed ? `FAIL back-home (${failed})` : 'PASS back-home'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL back-home: ' + (e && e.stack || e)); process.exit(1); });
