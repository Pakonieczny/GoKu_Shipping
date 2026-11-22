// COMBINEDRAFTS (Paul, 21 Nov 2026: "one sheet at a time"): underfilled Gold Filled / Sterling Silver drafts are combined by the cloud (pass item "combine", _charmNestCombine.js), and a
// person's Combine / Empty of two GF / SS drafts is done by the cloud (_charmNestCmdExec.js consolidate / empty). Offline: the real driver pass, the real library handler and ops
// (cmdMovePieces, archiveEmptySheet, nestStart) on the in-memory cloud of _cloudFakes.cjs; no network, no Etsy, no page code.
//   NODE_PATH=<deps> node tests/charm-nest/cloud-combine-drafts.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const T0 = Date.UTC(2026, 10, 21, 18, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0; const ok = (n, c, d) => { log0((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const lib = F.load('charmNestLibrary.js'), spy = F.spy(lib), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Standby = F.load('_charmNestStandby.js');
const Combine = F.load('_charmNestCombine.js'), CmdUnit = F.load('_charmNestCmdUnit.js'), Steps = F.load('_charmNestSteps.js');
// (GOLDONLY, Paul 21 Nov 2026: the cloud's own combine acts on Gold Filled only; parts 1-8 test the combine's rules on both colours, so Silver is let in for them; part 10 checks the live default)
Combine.AUTO_METALS.push('silver');
const R = 'run-2026-11-21-combine';
const ITEMS = [Combine.units.passItems[0], CmdUnit.units.passItems[0]];
const STEPS = {}; for (const s of Drive.RUN_STEPS) STEPS[s] = [async () => ({ wait: 'test', why: 'the test holds the run at its step' })];
const logs = [];

function setDrive(mode, steps) { F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: steps || ['nest', 'cmd:consolidate', 'cmd:empty'], flippedAt: T0 - 3600e3 }); lib._driveForget(); }
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
  ok('0 · NOAUTOCOMBINE: the combine is NOT a pass item of the cloud driver (only a person combines or empties sheets)', !Steps.passItems.some(i => i.name === 'combine') && !Steps.loaded.includes('./_charmNestCombine'), Steps.passItems.map(i => i.name));

  /* ═══ 1 · SS Draft 2 (6) + SS Draft 3 (5) → Draft 2 (11), nested fresh, Draft 3 archived; GF Draft 4 (12) onto the older GF draft with room in the NEXT pass (one a pass) ═══ */
  {
    world();
    sheet('silver-s1', 'silver', 1, [[5101, 2], [5102, 1]], { rec: { draft: false, setId: 'set-1', setSeq: 1 } }); F.seed('Charm_Nest_Sets/set-1', { setId: 'set-1', runId: R, sheetIds: ['silver-s1'], status: 'formed', seq: 1 });
    sheet('silver-d2', 'silver', 2, [[5201, 2], [5202, 1], [5203, 1], [5204, 1], [5205, 1]]);
    sheet('silver-d3', 'silver', 3, [[5301, 2], [5302, 1], [5303, 1], [5304, 1]]);
    sheet('gold-d1', 'gold', 1, [[6101, 1], [6102, 1], [6103, 1]]);
    sheet('gold-d4', 'gold', 4, [[6401, 3], [6402, 2], [6403, 2], [6404, 1], [6405, 1], [6406, 1], [6407, 1], [6408, 1]]);
    const before1 = JSON.stringify(S('silver-s1'));
    const p1 = await pass();
    ok('1 · the pass ran (the cloud carries the run, mode "on")', p1 && p1.ok && !p1.error, p1);
    const gfFirst = S('gold-d4').archived === true, ssFirst = S('silver-d3').archived === true;
    ok('1 · at most one combine a pass: the first pass combined ONE colour (Gold Filled first), the other waits', moves().length === 1 && gfFirst && !ssFirst && S('silver-d2').charms.length === 6, { moves: moves().length, gfFirst, ssFirst });
    ok('1 · GF Draft 4 (12 pieces) went onto the older GF draft with room (15), Draft 4 archived, a nest that only ADDS asked (NOWHOLE)', S('gold-d1').charms.length === 15 && jobsOf('gold-d1').length > 0 && jobsOf('gold-d1').every(j => j.mode !== 'fresh'), { n: S('gold-d1').charms.length });
    ok('1 · said in the run\'s log', logs.some(t => /\[drive\] combine: 12 pieces of GF Draft 4 moved onto GF Draft 1; GF Draft 4 was left empty and archived; GF Draft 1 is nested again from scratch/.test(t)), logs.filter(t => /combine/.test(t)));
    const p2 = await pass();
    const d2 = S('silver-d2'), d3 = S('silver-d3'), job = jobsOf('silver-d2')[0];
    ok('1 · the next pass ran', p2 && p2.ok && !p2.error, p2);
    ok('1 · SS Draft 3\'s 5 pieces are on SS Draft 2: 11 pieces, every pool id listed once', d2.charms.length === 11 && new Set(d2.poolIds).size === 11 && ['5301', '5302', '5303', '5304'].every(r => d2.charms.some(c => c.order === r)), { n: d2.charms && d2.charms.length });
    ok('1 · SS Draft 3 was left empty and archived (archiveEmptySheet)', d3.archived === true && !(d3.charms || []).length && spy.ops.some(o => o.op === 'archiveEmptySheet' && o.marked), { archived: d3.archived, left: (d3.charms || []).length });
    ok('1 · NOWHOLE: a nest of SS Draft 2 that only ADDS was asked (never fresh), its job queued', !!job && job.mode !== 'fresh' && job.status === 'queued' && (S('silver-d2').nestJob || {}).id === job.id, job);
    ok('1 · the moved pieces\' records name SS Draft 2 (sheetId), and the moved orders are whole on it', ['5301_silver_1', '5301_silver_2', '5304_silver_1'].every(id => F.doc('Charm_Pool/' + id).sheetId === 'silver-d2'));
    ok('1 · the SS sheet in a set was left alone (byte for byte)', JSON.stringify(S('silver-s1')) === before1);
    ok('1 · one move in this pass too (two in all)', moves().length === 2, moves());
    ok('1 · said in the run\'s log', logs.some(t => /\[drive\] combine: 5 pieces of SS Draft 3 moved onto SS Draft 2; SS Draft 3 was left empty and archived; SS Draft 2 is nested again from scratch · 4 order\(s\) moved whole/.test(t)), logs.filter(t => /combine/.test(t)));
    const p3 = await pass();
    ok('1 · a third pass changes nothing (no two drafts of a colour left)', p3.ok && moves().length === 2);
    ok('1 · no Etsy call, no network outside our own functions', F.etsy.calls === 0 && !F.net.tripwire.length && !F.net.other.length, { etsy: F.etsy.calls, other: F.net.other });
  }

  /* ═══ 2 · a draft in a set, a cut one, an approved one are left alone ═══ */
  {
    world();
    sheet('silver-a', 'silver', 1, [[7101, 1]], { rec: { setId: 'set-a', draft: false } }); F.seed('Charm_Nest_Sets/set-a', { setId: 'set-a', runId: R, sheetIds: ['silver-a'], status: 'formed', seq: 1 });
    sheet('silver-b', 'silver', 2, [[7201, 2]]);
    sheet('gold-a', 'gold', 1, [[7301, 1]], { rec: { laserDoneAt: T0 - 60000 } });
    sheet('gold-b', 'gold', 2, [[7401, 1]], { rec: { processReady: true } });
    sheet('gold-c', 'gold', 3, [[7501, 1]]);
    const snap = ['silver-a', 'silver-b', 'gold-a', 'gold-b', 'gold-c'].map(id => JSON.stringify(S(id)));
    await pass(); await pass();
    ok('2 · nothing moved: SS (the older sheet is in a set), GF (the older ones are cut and approved; one draft left)', moves().length === 0 && ['silver-a', 'silver-b', 'gold-a', 'gold-b', 'gold-c'].every((id, i) => JSON.stringify(S(id)) === snap[i]), moves());
    // a draft in a set as the NEWER one, two drafts older: they combine, the set's sheet stays
    sheet('silver-c', 'silver', 3, [[7601, 1]]);
    await pass();
    ok('2 · with a second SS draft, it goes onto the older draft (silver-b), the sheet in its set untouched', S('silver-b').charms.length === 3 && S('silver-c').archived === true && JSON.stringify(S('silver-a')) === snap[0]);
    // being nested, or held in Manual Control: left alone
    world();
    sheet('silver-m', 'silver', 1, [[7701, 1]], { rec: { manualEdit: { by: 'Ann', at: T0, tab: 't1' } } });
    sheet('silver-n', 'silver', 2, [[7702, 1]]);
    sheet('gold-m', 'gold', 1, [[7801, 1]]);
    sheet('gold-n', 'gold', 2, [[7802, 1]], { rec: { nestJob: { id: 'nest-x', status: 'running', at: T0 } } });
    await pass();
    ok('2 · a draft open in Manual Control, and one being nested, take no part', moves().length === 0 && !S('silver-n').archived && !S('gold-n').archived, moves());
  }

  /* ═══ 3 · pinned pieces stay: a source with a pinned piece is left whole; a pinned piece of the target keeps its pin through the combine ═══ */
  {
    world();
    sheet('silver-p1', 'silver', 1, [[8101, 2]]);
    sheet('silver-p2', 'silver', 2, [[8201, 2], [8202, 1]], { pin: ['8202_silver_1'] });
    sheet('gold-p1', 'gold', 1, [[8301, 2]], { pin: ['8301_gold_1'] });
    sheet('gold-p2', 'gold', 2, [[8401, 1]]);
    await pass();
    const g1 = S('gold-p1'), pin = (g1.charms || []).find(c => c.poolId === '8301_gold_1');
    ok('3 · SS: the newer draft holds a piece a person pinned: nothing of it moved, its pin kept', S('silver-p2').charms.length === 3 && !S('silver-p2').archived && S('silver-p2').charms.find(c => c.poolId === '8202_silver_1').pinned && S('silver-p1').charms.length === 2);
    ok('3 · GF: the newer draft went onto the older one, whose pinned piece kept its pin', g1.charms.length === 3 && S('gold-p2').archived === true && pin && pin.pinned && pin.pinned.cxPt === 40, { n: g1.charms && g1.charms.length, pin });
  }

  /* ═══ 4 · an order never splits: no room for ALL of the newer draft's pieces → nothing moves; an order also on a third sheet stays; the pairing rule is kept ═══ */
  {
    world();
    sheet('silver-r1', 'silver', 1, [[9101, 3], [9102, 1]], { rec: { usablePt2: 3000, freePt2: 1400 } });   // (room under the ceiling: 0.8 × 3000 − 1600 = 800 pt²)
    sheet('silver-r2', 'silver', 2, [[9201, 3]]);                                                         // (1200 pt²: does not fit whole)
    await pass();
    ok('4 · no room for all 3 pieces of order 9201: nothing moved, the order stays whole on its draft', moves().length === 0 && S('silver-r2').charms.length === 3 && S('silver-r1').charms.length === 4 && ['9201_silver_1', '9201_silver_2', '9201_silver_3'].every(id => F.doc('Charm_Pool/' + id).sheetId === 'silver-r2'));
    const L = await Combine.look({ db: F.db, runId: R, run: F.doc('Charm_Nest_Runs/' + R), read: b => lib.call(b, {}) });
    const j = Combine.judge(L, S('silver-r2') && Object.assign({ id: 'silver-r2' }, S('silver-r2')), Object.assign({ id: 'silver-r1' }, S('silver-r1')));
    ok('4 · the judgement says why, in words', !j.ok && /has no room for the 3 pieces/.test(j.why), j);
    // R16: an order of the newer draft also lies on a third sheet of its colour
    world();
    sheet('gold-h1', 'gold', 1, [[9301, 1]]);
    sheet('gold-h2', 'gold', 2, [[9401, 1]], { rec: { draft: false, setId: 'set-h' } }); F.seed('Charm_Nest_Sets/set-h', { setId: 'set-h', runId: R, sheetIds: ['gold-h2'], status: 'formed', seq: 1 });
    sheet('gold-h3', 'gold', 3, [[9501, 1], [9401, 1]].map(([r, n], i) => [r, n]));
    // (order 9401 has a piece on the set's sheet h2 and one on draft h3: its own pool id differs, the same order)
    { const h3 = S('gold-h3'); h3.charms[1].poolId = '9401_gold_9'; h3.charms[1].id = 'pool:T_gold:9401_gold_9'; h3.poolIds[1] = '9401_gold_9'; h3.placements[1].id = h3.charms[1].id; F.seed('Charm_Nest_Sheets/gold-h3', h3); F.seed('Charm_Pool/9401_gold_9', { poolId: '9401_gold_9', state: 'pooled', sheetId: 'gold-h3', runId: R, orderId: '9401', material: 'gold' }); }
    await pass();
    ok('4 · R16: order 9401 also lies on a third GF sheet: the draft holding it stays as it is', moves().length === 0 && S('gold-h3').charms.length === 2 && !S('gold-h3').archived);
    // PAIR2: the older GF draft already shares orders with two SS sheets; the newer GF draft's order is on a third SS sheet
    world();
    sheet('gold-q1', 'gold', 1, [[9601, 1], [9602, 1]]);
    sheet('gold-q2', 'gold', 2, [[9603, 1]]);
    sheet('silver-q1', 'silver', 1, [[9601, 1]], { rec: { draft: false, setId: 'set-q1' } });
    sheet('silver-q2', 'silver', 2, [[9602, 1]], { rec: { draft: false, setId: 'set-q2' } });
    sheet('silver-q3', 'silver', 3, [[9603, 1]], { rec: { draft: false, setId: 'set-q3' } });
    await pass();
    ok('4 · PAIR2: combining would give GF Draft 1 a third Sterling Silver partner: nothing moved', moves().length === 0 && S('gold-q2').charms.length === 1 && !S('gold-q2').archived);
    // onesheet off: nothing
    world({ run: { settings: { onesheet: 'off' } } });
    sheet('silver-o1', 'silver', 1, [[9701, 1]]); sheet('silver-o2', 'silver', 2, [[9702, 1]]);
    await pass();
    ok('4 · the sorter\'s onesheet "off": nothing is combined', moves().length === 0 && !S('silver-o2').archived);
  }

  /* ═══ 5 · a person's Combine / Empty of two GF / SS drafts, done by the cloud; refused in words when the rules say no ═══ */
  {
    world({ steps: ['cmd:consolidate', 'cmd:empty', 'nest'] });
    // the cloud's own combine must not get there first: the drafts' first look is the person's (the combine runs at order 55, the commands at 40)
    sheet('silver-x1', 'silver', 1, [[1101, 1]]);
    sheet('silver-x2', 'silver', 2, [[1102, 2]]);
    const id = putCmd('consolidate', { sourceId: 'silver-x2', targetId: 'silver-x1' });
    await pass();
    const c = cmdOf(id), x1 = S('silver-x1');
    ok('5 · NOMERGE: a person\'s Combine (consolidate) of two drafts is refused in words; nothing moved', c.state === 'failed' && /Combining sheets is a person's choice, for 10K and 14K Gold only/.test(c.text || ''), c);
    ok('5 · the request moved nothing itself (what moved is the cloud\'s own combine of two drafts, which only adds)', moves().length <= 1);
    // refused: the target is in a set
    world();
    sheet('silver-y1', 'silver', 1, [[1201, 1]], { rec: { draft: false, setId: 'set-y' } }); F.seed('Charm_Nest_Sets/set-y', { setId: 'set-y', runId: R, sheetIds: ['silver-y1'], status: 'formed', seq: 1 });
    sheet('silver-y2', 'silver', 2, [[1202, 1]]);
    const id2 = putCmd('consolidate', { sourceId: 'silver-y2', targetId: 'silver-y1' });
    await pass();
    ok('5 · Combine onto an SS sheet in a set: refused in words, nothing moved', cmdOf(id2).state === 'failed' && /Combining sheets is a person's choice|in a set/.test(cmdOf(id2).text || '') && moves().length === 0, cmdOf(id2));
    /* COMBINE10K14K (Paul, 21 Nov 2026): the only combine is a person's drag of a 10K / 14K run sheet onto another in the options window; never without it, never by the cloud, never GF / SS */
    for (const [label, args, by, moved] of [
      ['a person\'s 10K combine from the options window moves the pieces', { metal: 'gold10k', sourceId: 'gold10k-k2', targetId: 'gold10k-k1', via: 'options' }, 'Ann', true],
      ['a 10K combine not asked in the options window is refused', { metal: 'gold10k', sourceId: 'gold10k-k2', targetId: 'gold10k-k1' }, 'Ann', false],
      ['a 10K combine sent as the cloud is refused', { metal: 'gold10k', sourceId: 'gold10k-k2', targetId: 'gold10k-k1', via: 'options' }, 'Cloud', false],
      ['a Gold Filled combine, even from the options window, is refused', { metal: 'gold', sourceId: 'gold-k2', targetId: 'gold-k1', via: 'options' }, 'Ann', false]]) {
      world({ steps: ['cmd:consolidate', 'cmd:empty', 'nest'] });
      sheet('gold10k-k1', 'gold10k', 1, [[1401, 1]]); sheet('gold10k-k2', 'gold10k', 2, [[1402, 1]]);
      sheet('gold-k1', 'gold', 1, [[1501, 1]], { rec: { setId: 'set-k', draft: false } }); sheet('gold-k2', 'gold', 2, [[1502, 1]], { rec: { setId: 'set-k', draft: false } });
      const idk = putCmd('consolidate', args); { const cur = F.doc('Charm_Nest_Run_Cmds/' + R); cur.cmds[idk].by = by; F.seed('Charm_Nest_Run_Cmds/' + R, cur); }
      await pass();
      const ck = cmdOf(idk), srcIds = S(args.sourceId).poolIds || [], tgtIds = S(args.targetId).poolIds || [], piece = args.metal === 'gold10k' ? '1402_gold10k_1' : '1502_gold_1';
      ok('5k · ' + label, moved ? tgtIds.includes(piece) && !srcIds.includes(piece) : srcIds.includes(piece) && !tgtIds.includes(piece) && ck.state === 'failed' && /Combining sheets is a person's choice/.test(ck.text || ''), { ck: { state: ck.state, text: ck.text || ck.step }, srcIds, tgtIds });
    }
    // Empty of a GF draft: onto the oldest other draft that takes all its pieces (the full one is passed over)
    world();
    sheet('gold-e1', 'gold', 1, [[1301, 4]], { rec: { usablePt2: 2200, freePt2: 600 } });   // (full: 0.8 × 2200 − 1600 = 160 pt²)
    sheet('gold-e2', 'gold', 2, [[1302, 1]]);
    sheet('gold-e3', 'gold', 3, [[1303, 2]]);
    const id3 = putCmd('empty', { sheetId: 'gold-e3' });
    await pass();
    ok('5 · Empty of GF Draft 3: its pieces went onto Draft 2 (Draft 1 has no room), Draft 3 archived', S('gold-e2').charms.length === 3 && S('gold-e1').charms.length === 4 && S('gold-e3').archived === true && cmdOf(id3).state === 'running', { e2: S('gold-e2').charms.length, c: cmdOf(id3) });
    // a pinned piece on the draft to empty: refused, in words
    world();
    sheet('gold-z1', 'gold', 1, [[1401, 1]]);
    sheet('gold-z2', 'gold', 2, [[1402, 1]], { pin: ['1402_gold_1'] });
    const id4 = putCmd('empty', { sheetId: 'gold-z2' });
    await pass();
    ok('5 · Empty of a GF draft holding a pinned piece: refused in words, nothing moved', cmdOf(id4).state === 'failed' && /^Not emptied: GF Draft 2 holds a piece a person pinned/.test(cmdOf(id4).text || '') && moves().length === 0, cmdOf(id4));
  }

  /* ═══ 10 · GOLDONLY: by default the cloud's own combine never touches Sterling Silver drafts ═══ */
  {
    Combine.AUTO_METALS.splice(0, Combine.AUTO_METALS.length, 'gold');
    world();
    sheet('silver-g1', 'silver', 1, [[1601, 1]]); sheet('silver-g2', 'silver', 2, [[1602, 1]]);
    await pass();
    ok('10 · the default (Gold Filled only): two Sterling Silver drafts are left as they are', moves().length === 0 && !S('silver-g2').archived && S('silver-g1').charms.length === 1, moves());
    Combine.AUTO_METALS.push('silver');
  }
  /* ═══ 6 · mode "off" unchanged: the driver does not run, nothing is combined ═══ */
  {
    world({ mode: 'off' });
    sheet('silver-f1', 'silver', 1, [[1501, 1]]); sheet('silver-f2', 'silver', 2, [[1502, 1]]);
    const p = await pass();
    ok('6 · mode "off": no pass, nothing moved or archived', !(p && p.ok) && moves().length === 0 && !S('silver-f2').archived && S('silver-f1').charms.length === 1, p);
  }

  /* ═══ 7 · the drafts are numbered again once one is archived: Draft 1, 2, 3 with no gap ═══ */
  {
    world();
    sheet('silver-n2', 'silver', 2, [[2201, 2]], { rec: { draftOrder: 2 } });
    sheet('silver-n3', 'silver', 3, [[2301, 1]], { rec: { draftOrder: 3 } });
    sheet('silver-n4', 'silver', 4, [[2401, 1]], { pin: ['2401_silver_1'], rec: { draftOrder: 4 } });   // (a person's pin: it stays where it is)
    sheet('silver-n1', 'silver', 1, [[2101, 1]], { rec: { draft: false, setId: 'set-n', draftOrder: 1 } }); F.seed('Charm_Nest_Sets/set-n', { setId: 'set-n', runId: R, sheetIds: ['silver-n1'], status: 'formed', seq: 1 });
    await pass();
    ok('7 · SS Draft 3 went onto Draft 2 and was archived; the pinned Draft 4 stayed', S('silver-n3').archived === true && S('silver-n2').charms.length === 3 && S('silver-n4').charms.length === 1);
    ok('7 · the later draft moved up one place: page 3 (named "SS Draft 3"), its draftOrder 3; the set\'s sheet and Draft 2 keep theirs', S('silver-n4').page === 3 && S('silver-n4').draftOrder === 3 && S('silver-n2').page === 2 && S('silver-n1').page === 1
      && require('../../charm-nest-sheet-name.js').name(Object.assign({ id: 'silver-n4' }, S('silver-n4'))) === 'SS Draft 3', { n4: [S('silver-n4').page, S('silver-n4').draftOrder] });
    ok('7 · said in the run\'s log', logs.some(t => /SS Draft 4 is now SS Draft 3/.test(t)), logs.filter(t => /combine/.test(t)));
    // a person's Empty renumbers too
    world();
    sheet('gold-r1', 'gold', 1, [[2501, 1]]); sheet('gold-r2', 'gold', 2, [[2502, 1]]); sheet('gold-r3', 'gold', 3, [[2503, 1]], { pin: ['2503_gold_1'] });
    putCmd('empty', { sheetId: 'gold-r2' });
    await pass();
    ok('7 · a person\'s Empty of GF Draft 2: Draft 2 archived, Draft 3 is Draft 2 now', S('gold-r2').archived === true && S('gold-r1').charms.length === 2 && S('gold-r3').page === 2, { r3: S('gold-r3').page });
  }

  /* ═══ 8 · the gather sweep runs now (it is the combine's own unit) and the two never combine twice in one pass ═══ */
  {
    const Sweeps = F.load('_charmNestSweeps.js'), SW = Sweeps.units.passItems[0];
    world({ steps: ['nest', 'sweeps'] });
    sheet('silver-g1', 'silver', 1, [[3101, 1]]); sheet('silver-g2', 'silver', 2, [[3102, 1]]);
    sheet('gold-g1', 'gold', 1, [[3201, 1]]); sheet('gold-g2', 'gold', 2, [[3202, 1]]);
    await pass([Combine.units.passItems[0], SW]);
    ok('8 · combine item + gather sweep in one pass: ONE combine (the sweep saw the pass had combined)', moves().length === 1, moves());
    await pass([Combine.units.passItems[0], SW]);
    ok('8 · the next pass: the other colour, again one', moves().length === 2 && S('silver-g2').archived === true && S('gold-g2').archived === true, moves());
    world({ steps: ['nest', 'sweeps'] });
    sheet('silver-h1', 'silver', 1, [[3301, 1]]); sheet('silver-h2', 'silver', 2, [[3302, 1]]);
    sheet('gold-h1', 'gold', 1, [[3401, 1]]); sheet('gold-h2', 'gold', 2, [[3402, 1]]);
    await pass([SW]);
    ok('8 · NOAUTOCOMBINE: the gather sweep alone combines nothing', moves().length === 0 && S('silver-h2').archived !== true && S('gold-h2').archived !== true && !logs.some(t => /sweeps: gather: \d+ piece/.test(t)), { moves: moves().length, logs: logs.filter(t => /gather/.test(t)) });
    /* NOREOPEN (Paul, 21 Nov 2026: "None of the sheets should be reopened"): the sweeps never take a full mark off, whatever the sheet's fill */
    world({ steps: ['nest', 'sweeps'] });
    sheet('gold10k-m1', 'gold10k', 1, [[3501, 1]], { rec: { releaseFull: true, density: 0.21 } });
    await pass([SW]);
    ok('8 · NOREOPEN: a 21% draft marked full keeps its mark through the sweeps; no piece moved', S('gold10k-m1').releaseFull === true && moves().length === 0, { m1: S('gold10k-m1').releaseFull });
    /* GHOSTS: an entry of a sheet's piece list that its pool list does not name and another live sheet does is taken off; the piece stays where it lives, nothing else changes */
    world({ steps: ['nest', 'sweeps'] });
    sheet('gold-l1', 'gold', 1, [[3601, 1]], { rec: { draft: false, setId: 'set-l', releaseFull: true } }); F.seed('Charm_Nest_Sets/set-l', { setId: 'set-l', runId: R, sheetIds: ['gold-l1'], status: 'formed', seq: 1 });
    sheet('gold-l2', 'gold', 2, [[3602, 1]]);
    { const l1 = F.doc('Charm_Nest_Sheets/gold-l1'), l2 = F.doc('Charm_Nest_Sheets/gold-l2'); l1.charms = l1.charms.concat([Object.assign({}, l2.charms[0])]); F.seed('Charm_Nest_Sheets/gold-l1', l1); }
    await pass([SW]);
    ok('8 · GHOSTS: the leftover entry is taken off the set sheet; its own piece and placements stay; the piece stays on the draft that lists it', (S('gold-l1').charms || []).length === 1 && (S('gold-l1').placements || []).length === 1 && (S('gold-l2').charms || []).length === 1 && (S('gold-l2').placements || []).length === 1 && moves().length === 0, { l1: (S('gold-l1').charms || []).map(c => c.poolId), l2: (S('gold-l2').charms || []).map(c => c.poolId) });
    /* OLDMARKS: once, the old failure marks are cleared; no sheet's pieces or layout change */
    world({ steps: ['nest', 'sweeps'] });
    sheet('gold-o1', 'gold', 1, [[3701, 1]], { rec: { filesOwed: T0 - 5 * 86400e3 } });
    sheet('gold-o2', 'gold', 2, [[3702, 1]], { rec: { filesOwed: T0 - 3600e3 } });
    F.seed('Charm_Nest_Finish/gold-o1', { sheetId: 'gold-o1', runId: R, lastDone: { outcome: 'failed', why: '9 pieces of GF Sheet 1 could not be made again from the library: X · Y: its right body is no longer in the design', at: T0 - 5 * 86400e3 } });
    F.seed('Charm_Nest_Runs/' + R, Object.assign(F.doc('Charm_Nest_Runs/' + R), { errors: [{ t: 1, why: 'Failed to fetch' }, { t: 2, why: 'HTTP 429' }], sheetHolds: { 'd-gone': 'This sheet: its nest was stopped by Cloud', 'gold-o2': null } }));
    putCmd('consolidate', { sourceId: 'x', targetId: 'y' }); { const cur = F.doc('Charm_Nest_Run_Cmds/' + R); for (const k of Object.keys(cur.cmds)) { cur.cmds[k].state = 'failed'; cur.cmds[k].at = T0 - 3600e3; } F.seed('Charm_Nest_Run_Cmds/' + R, cur); }
    const p1 = JSON.stringify(S('gold-o1').placements), p2 = JSON.stringify(S('gold-o2').placements);
    await pass([SW]);
    const runAfter = F.doc('Charm_Nest_Runs/' + R) || {}, cmdsAfter = (F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {};
    ok('8 · OLDMARKS: the old "owing files" (failed redraw) is off and its task cleared; a sheet owed for another reason keeps its mark; old errors, a hold on a missing sheet and failed requests are gone; no layout changed',
      !S('gold-o1').filesOwed && (F.doc('Charm_Nest_Finish/gold-o1') || {}).lastDone.outcome === 'cleared' && !!S('gold-o2').filesOwed && (runAfter.errors || []).length === 0 && !(runAfter.sheetHolds || {})['d-gone'] && Object.keys(cmdsAfter).length === 0
      && JSON.stringify(S('gold-o1').placements) === p1 && JSON.stringify(S('gold-o2').placements) === p2,
      { o1: S('gold-o1').filesOwed, o2: S('gold-o2').filesOwed, errors: runAfter.errors, holds: runAfter.sheetHolds, cmds: Object.keys(cmdsAfter), logs: logs.filter(t => /oldmarks/.test(t)) });
    const Reg = F.load('_charmNestSteps.js');
    ok('8 · NOAUTOCOMBINE: the cloud\'s pass items hold no combine (nothing in the cloud combines drafts by itself)', Array.isArray(Reg.passItems) && Reg.passItems.length > 0 && !Reg.passItems.some(it => it && it.name === 'combine'), (Reg.passItems || []).map(it => it && it.name));
  }

  /* ═══ 9 · the page (charm-nest-bridge.js): Merge drafts for GF / SS on a viewer, sent to the cloud; never on a computer that carries a run ═══ */
  {
    const fs = require('fs'), vm = require('vm'), BR = fs.readFileSync(require('path').join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
    const a = BR.indexOf('  const s4Run = () =>'), b = BR.indexOf('  /** " (3 pairs, each kept whole)"');
    const mk = (viewer, pages) => {
      const w = { console, Object, Array, Set, Map, String, Number, JSON, Math };
      w.window = w; w.B = { run: viewer ? null : { runId: R } }; w.CNViewer = { on: () => viewer, nestOf: () => null }; w.CharmNestRunFollow = { state: () => ({ runId: R }) };
      w.CharmNestSheetName = require('../../charm-nest-sheet-name.js'); w.O = { rankDate: () => 0 };
      w.pagesOf = m => pages.filter(p => p.metal === m); w.inSetNow = p => !!p.setId && !p.draft; w.solid = m => ['gold10k', 'gold14k'].includes(m);
      w.modern = () => false; w.membershipEditable = () => true; w.holding = () => false;
      vm.runInContext(BR.slice(a, b) + '\nthis.mergePlan = mergePlan;', vm.createContext(w));
      return w;
    };
    const pg = (id, page, n, x) => Object.assign({ sheetId: id, id, metal: 'silver', page, draft: true, runId: R, charms: Array.from({ length: n }, (_, i) => ({ id: id + i })) }, x || {});
    const pages = [pg('s3', 3, 5), pg('s2', 2, 6), pg('s1', 1, 3, { draft: false, setId: 'set-1' }), pg('s9', 4, 2, { laserDoneAt: 5 })];
    const V = mk(true, pages).mergePlan('silver');
    ok('9 · a viewer: Merge drafts offered for the SS drafts of the followed run (Draft 2 first, then Draft 3); the set\'s sheet and a cut one left out', !!V && V.target.sheetId === 's2' && V.sources.map(p => p.sheetId).join() === 's3', V && { t: V.target.sheetId, s: V.sources.map(p => p.sheetId) });
    ok('9 · a computer that carries a run: nothing offered for GF / SS (mode "off" unchanged)', mk(false, pages).mergePlan('silver') === null);
    const m0 = BR.indexOf('/* S4-MERGE-BEGIN'), m1 = BR.indexOf('/* S4-MERGE-END */'), sent = [];
    const w = { console, Promise, Date, JSON, Map, Set, Object, String, setTimeout: () => 0 }; w.window = w;
    w.charmsWord = n => `${n} charms`; w.pw = p => p.name; w.sheetsWord = l => l.map(p => p.name).join(' and '); w.closeOptions = () => {}; w.repaintMerge = () => {}; w.mergeNote = () => {};
    w.CNRelayAsk = async cmd => { sent.push(cmd); return { ok: true, text: 'SS Draft 3: 5 piece(s) moved onto SS Draft 2; SS Draft 3 was left empty and archived' }; };
    vm.runInContext(BR.slice(m0, m1) + '\nthis.s4Merge = s4Merge; this.s4Words = s4Words;', vm.createContext(w));
    const T = { name: 'SS Draft 2', sheetId: 's2', metal: 'silver' }, S3 = { name: 'SS Draft 3', sheetId: 's3', metal: 'silver' }, plan = { target: T, sources: [S3], pages: [T, S3], moving: 5, total: 11 };
    let st = {}; await w.s4Merge('silver', 'move', plan, st);
    ok('9 · "Move all onto SS Draft 2" sends consolidate (Draft 3 onto Draft 2) and shows the cloud\'s answer', sent.length === 1 && sent[0].type === 'consolidate' && sent[0].sourceId === 's3' && sent[0].targetId === 's2' && /done by the cloud: .*archived/.test(st.done.text) && st.done.on === 's2', { sent, st });
    st = {}; await w.s4Merge('silver', 'empty', plan, st);
    ok('9 · "Empty SS Draft 3" sends empty for that draft', sent.length === 2 && sent[1].type === 'empty' && sent[1].sheetId === 's3');
    ok('9 · the words a person confirms say the cloud\'s rules for drafts', /when ALL its charms fit there/.test(w.s4Words(plan, 'move')) && /oldest other draft that takes them all/.test(w.s4Words(plan, 'empty')));
    ok('9 · the GF / SS card draws the panel on a viewer (renderFastMerge from renderRelease), with Empty', /if \(fastMergeHere\(sh\)\) return renderFastMerge\(sh, node\);/.test(BR) && /data-solid="merge-empty"/.test(BR));
    const HTML = fs.readFileSync(require('path').join(__dirname, '../../charm-nest-1.html'), 'utf8');
    ok('9 · the bridge loads under a new token (-cd)', /charm-nest-bridge\.js\?v=[^"]*-cd(?:-\w+)*"/.test(HTML));
  }

  log0(failed ? `FAIL cloud-combine-drafts (${failed})` : 'PASS cloud-combine-drafts'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL cloud-combine-drafts: ' + ((e && e.stack) || e)); process.exit(1); });
