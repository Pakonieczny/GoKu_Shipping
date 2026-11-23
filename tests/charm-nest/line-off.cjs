// LINEOFF (Paul, 23 Nov 2026, order 4235923653: a Gold Filled disc on a Rose Gold sheet whose cut line is recorded and that the laser has not cut: "Please also move this piece off the rose gold sheet", "Not cut: take it off",
// "Do not renest the RG"). The real library handler and writer over the in-memory cloud (_cloudFakes.cjs):
//   1  a person's take-off takes ONLY that piece off such a sheet: every other piece byte for byte, the recorded cut, its line, its physical sheet and its leftover unchanged, no nest asked
//   2  cut on the laser, lines still protected, its set sent or lasered, an automatic take-off (a cancel): kept, as before
//   3  its files are written again without that piece: the writer takes it only when every piece left is where the files have it; putSheet takes only the record's own layout from the cloud's writer
//   LINEOFF_ROOT=<another checkout> NODE_PATH=<deps> node tests/charm-nest/line-off.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), root = process.env.LINEOFF_ROOT ? path.resolve(process.env.LINEOFF_ROOT) : path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 23, 18, 30, 0), CUT = T0 - 5 * 86400e3, F = require(path.join(root, 'tests/charm-nest/_cloudFakes.cjs'))({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {}; let n = 0, failed = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 600))); if (!c) failed++; };
(async () => {
  const lib = F.load('charmNestLibrary.js'), Finish = F.load('_charmNestFinish.js'), R = 'run-lineoff', D = '4235923653', E = '4231985849', G = '4233700663', pid = o => `${o}_5295960022_1`;
  const place = (id, x, y, a) => ({ id, cxPt: x, cyPt: y, angle: a || 0, scale: 0.971, wPt: 30, hPt: 30 });
  function seed(o) {
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: ['pull', 'pool', 'nest', 'files', 'takeoff'] }); lib._driveForget();
    F.seed('Charm_Nest_Open/run', { runId: R, at: T0 }); F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', lines: {} }); F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 } });
    for (const x of [D, E, G]) F.seed('Charm_Pool/' + pid(x), { poolId: pid(x), orderId: x, lineKey: x + "_5295960022", state: 'written', sheetId: 'rg-1', setId: 'set-1', material: 'rose', runId: R, quantity: 1 });
    F.seed('Charm_Nest_Sets/set-1', Object.assign({ setId: 'set-1', runId: R, sheetIds: ['rg-1'], orders: {} }, (o && o.set) || {}));
    F.seed('Charm_Nest_Rose_Stock/rgs-1', { id: 'rgs-1', revision: 3, wPt: 283, hPt: 141, profileJson: '{"values":[1,2]}', lastCutSheetId: 'rg-9', available: true });
    F.seed('Charm_Nest_Sheets/rg-1', Object.assign({ id: 'rg-1', runId: R, metal: 'rose', fileBase: 'RG_Nov.18.26_Set-2_Sheet-1', draft: false, setId: 'set-1', archived: false, layoutRev: 36, contentRev: 30, stock: { wPt: 283, hPt: 141 },
      roseCutAt: CUT, roseCutRevision: 1, roseStockId: 'rgs-1', roseRevision: 1, rosePlanJson: '{"lines":[[0,0],[1,1]]}', rosePlanHash: 'ph', roseFingerprint: 'fp', verification: { ok: true }, status: 'complete',
      poolIds: [pid(D), pid(E), pid(G)], orders: [D, E, G], charms: [D, E, G].map(x => ({ id: 'c-' + x, poolId: pid(x), order: x, name: x, hash: 'h' + x })), placements: [place('c-' + D, 100, 110), place('c-' + E, 40, 20, 90), place('c-' + G, 70, 60, 45)],
      outputs: { ai: { path: 'charmnest/x/RG.ai', url: 'u' }, preview: { url: 'p' }, report: { path: 'charmnest/x/report.json' } } }, (o && o.sheet) || {}));
  }
  const off = (kind, u) => lib.call({ op: 'cloudTakeOff', orderId: D, sheetId: 'rg-1', ids: [pid(D)], kind, at: F.clock.now ? F.clock.now() : T0, who: 'Paul_K' }, { unit: 'test|' + u });
  seed(); const was = F.doc('Charm_Nest_Sheets/rg-1'), stock = JSON.stringify(F.doc('Charm_Nest_Rose_Stock/rgs-1'));
  let r = await off('sheetOff', 'a'); let s = F.doc('Charm_Nest_Sheets/rg-1');
  ok('1 · the disc comes off the sheet', (r.removed || []).includes(pid(D)) && !s.poolIds.includes(pid(D)) && !s.charms.some(c => c.poolId === pid(D)) && !s.placements.some(p => p.id === 'c-' + D), r);
  ok('1 · every other piece stays byte for byte', JSON.stringify(s.placements) === JSON.stringify(was.placements.slice(1)) && JSON.stringify(s.charms) === JSON.stringify(was.charms.slice(1)), s.placements);
  ok('1 · the recorded cut, its line, its physical sheet and the leftover are as they were', ['roseCutAt', 'roseCutRevision', 'roseStockId', 'roseRevision', 'rosePlanJson', 'rosePlanHash', 'roseFingerprint', 'setId', 'draft'].every(k => JSON.stringify(s[k]) === JSON.stringify(was[k])) && JSON.stringify(F.doc('Charm_Nest_Rose_Stock/rgs-1')) === stock, s);
  ok('1 · nothing is nested: no nest is asked, none is owed', !s.nestOwed && !s.nestJob && !F.list('Charm_Nest_Jobs').length);
  ok('1 · its files are owed, the piece waits for a person (held)', s.filesOwed > 0 && s.takenOffAt > CUT && F.doc('Charm_Pool/' + pid(D)).state === 'abandoned' && F.doc('Charm_Pool/' + pid(D)).sheetId === null);
  for (const [name, o, kind] of [['cut on the laser', { sheet: { laserDoneAt: T0 - 1000 } }, 'sheetOff'], ['its lines still protected', { sheet: { roseProtectedJson: '{"placements":[]}' } }, 'sheetOff'], ['its set cut on the laser', { set: { laserDoneAt: T0 - 1000 } }, 'sheetOff'], ['a cancelled order (no person took it off)', {}, 'cancel']]) {
    seed(o); r = await off(kind, 'b' + name); s = F.doc('Charm_Nest_Sheets/rg-1'); ok('2 · ' + name + ': the piece stays, as before', (r.kept || []).includes(pid(D)) && s.placements.length === 3 && s.poolIds.length === 3, r);
  }
  // 3 · the files
  seed(); r = await off('sheetOff', 'c'); s = F.doc('Charm_Nest_Sheets/rg-1');
  F.seed('Charm_Nest_Finish/rg-1', { sheetId: 'rg-1', runId: R, nonce: 'nn', state: 'running', heartbeat: true, startedAt: T0, beatAt: T0 });
  const fence = { path: 'Charm_Nest_Finish/rg-1', nonce: 'nn', runId: R }, put = (sheet, o, f) => lib.call(Object.assign({ op: 'putSheet', sheet: Object.assign({ id: 'rg-1' }, sheet), layoutBase: s.layoutRev, contentBase: s.contentRev }, o || {}), Object.assign({ unit: 'put|' + Math.random() }, f === false ? {} : { fence }));
  const err = x => x && (x.error || (x.status >= 400 ? 'status ' + x.status : ''));
  let p = await put({ placements: s.placements.map((q, i) => i ? q : Object.assign({}, q, { cxPt: q.cxPt + 3 })), stock: s.stock }, { lineOff: true }); ok('3 · a layout that moves a piece is refused', !!err(p) && /already cut/i.test(err(p)), p);
  p = await put({ placements: s.placements, stock: s.stock }, {}); ok('3 · the same layout without the writer\'s word is refused', !!err(p) && /already cut/i.test(err(p)), p);
  p = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify({ op: 'putSheet', sheet: { id: 'rg-1', placements: s.placements, stock: s.stock }, lineOff: true, layoutBase: s.layoutRev, contentBase: s.contentRev }) }).then(x => JSON.parse(x.body)); ok('3 · a page can never send it', !!err(p), p);
  p = await put({ placements: s.placements, stock: s.stock, verification: { ok: true } }, { lineOff: true }); const a = F.doc('Charm_Nest_Sheets/rg-1'); ok('3 · the record\'s own layout, from the cloud\'s writer, is taken; cut and line as they were', !err(p) && a.placements.length === 2 && a.roseCutAt === CUT && a.rosePlanJson === was.rosePlanJson && a.roseFingerprint === 'fp', p);
  // the writer's first step
  const S0 = async (rec, report) => { seed({ sheet: rec }); const bytes = Buffer.from(JSON.stringify(report)); F.seed('Charm_Nest_Finish/rg-1', { sheetId: 'rg-1', runId: R, nonce: 'nn', state: 'running', heartbeat: true, startedAt: T0, beatAt: T0 });
    const task = { runId: R, want: { at: T0, reason: 'owed' } }, X = { db: F.db, bucket: { file: () => ({ download: async () => [bytes] }) }, sheetId: 'rg-1', task, notes: [], log: () => {}, live: () => {}, saveTask: async q => Object.assign(task, q), call: (body, u) => lib.call(body, { unit: 'fin|' + u + Math.random(), fence }), read: b => lib.call(b, {}) };
    try { return { next: await Finish.STEP.S0(X), job: task.job }; } catch (e) { return { stop: e.kind, why: e.message }; } };
  const left = [place('c-' + E, 40, 20, 90), place('c-' + G, 70, 60, 45)], all = [place('c-' + D, 100, 110)].concat(left), taken = { placements: left, charms: was.charms.slice(1), poolIds: [pid(E), pid(G)], filesOwed: T0, takenOffAt: T0 };
  let w = await S0(taken, { placements: all }); ok('3 · the writer writes the files again without the piece', w.next === 'S1' && w.job && w.job.lineOff === true, w);
  w = await S0(Object.assign({}, taken, { placements: [left[0], place('c-' + G, 71, 60, 45)] }), { placements: all }); ok('3 · a piece not where the files have it: not written', w.stop === 'skipped', w);
  w = await S0(Object.assign({}, taken, { takenOffAt: CUT - 1000 }), { placements: all }); ok('3 · nothing taken off since its cut: not written, as before', w.stop === 'skipped' && /cut is recorded/.test(w.why), w);
  w = await S0(Object.assign({}, taken, { laserDoneAt: T0 - 5 }), { placements: all }); ok('3 · cut on the laser: never written', w.stop === 'skipped', w);
  { const src = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8'); ok('1 · the sheet window offers it (a computer that follows the cloud, never a cancel)', /lineOnly = s => \{ if \(!viewing \|\| cancelling/.test(src) && /ok\.push\(\{ id, sku, sh, where \}\);   \/\/ \(LINEOFF\)/.test(src)); }
  log0(`${failed ? 'FAIL' : 'PASS'} line-off (${n - failed} of ${n})`); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL line-off: ' + (e && e.stack || e)); process.exit(1); });
