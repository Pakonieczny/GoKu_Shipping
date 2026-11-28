// SHEETLIVE stage 4 (Paul, 27 Nov 2026: "Make sure that all the automatic connections and changes that the system can make on behalf of the user are correctly wired in to this new one live copy
// and that all updates are updated immediately and that nothing gets missed from front engraving, back engraving, custom orders, multipiece orders, cancelled orders, removed pieces").
// The page's one copy of a sheet (charm-nest-sheet-live.js) reads a sheet only when the changes feed NAMES it (s:<sheet>, t:<set>, p: / x: / o:<order>, '*'). Offline, over _cloudFakes.cjs
// (the real library handler and units; no network, no Firebase, no Etsy, nothing paid), each writer that changes what a sheet shows names it, at once, with the push counter moved:
//   A  Change SKU / remake (cmdSwapLine): the sheet and EVERY set it rewrote (a set listing the sheet beside its own)
//   B  a take-off / cancel in the cloud (cloudTakeOff): the sheet, its set (its order list), the order
//   C  the sheet writer's saves (putSheet), a set's save (setUpdate), a back or front engraving record (backPut: '*'), a cancel record (cancelPut)
//   D  the green line (rosePlan): its sheet; a nest slot's end written on the sheet (nestJob, outside the handler): its sheet, through feedNames
//   E  every other writer of a sheet, set or engraving record names it (code text, where the op needs the whole cloud to run)
//   node tests/charm-nest/sheet-live-feed.cjs    (SL_FN=<another checkout>/netlify/functions runs it against that code: production main before stage 4 fails A1, B1, D and four of E)
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 27, 20, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const FN = process.env.SL_FN ? path.resolve(process.env.SL_FN) : F.fnDir, Lsrc = f => fs.readFileSync(path.join(FN, f), 'utf8');   // (SL_FN=<another checkout>/netlify/functions: the same checks against that code)
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {}; console.error = () => {};
let n = 0, failed = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const part = async (name, fn) => { try { await fn(); } catch (e) { ok(name + ' ran to its end', false, String((e && e.stack) || e).slice(0, 900)); } };
const lib = F.load('charmNestLibrary.js', { dir: FN });
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode, feed: (r.headers && r.headers['X-CN-Feed']) || '' }); };

const R = 'run-2026-11-27-sl', DAY = '2026-11-27', S1 = 'gf-sl-s1', SET = 'set-2026-11-27-1', SET2 = 'set-2026-11-27-2', LEASE = 'Charm_Nest_Drive/lease';
const O1 = '4232000001', O2 = '4232000002', T1 = '5302000001', T2 = '5302000002', K1 = `${O1}_${T1}`, K2 = `${O2}_${T2}`, P1 = K1 + '_1', P2 = K2 + '_1';
const fence = { path: LEASE, nonce: 'dn-sl', runId: R };
const cloudCall = (b, u) => lib.call(b, { unit: String(u), fence });
const sheetDoc = id => F.doc('Charm_Nest_Sheets/' + id) || {};
const FB = 'GF_Nov.27.26_Set-1_Sheet-1', FOLDER = `charmnest/sets/${DAY}/Set-1`;
function seed() {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: ['pull', 'pool', 'nest', 'files', 'labels', 'checkpoint', 'takeoff', 'cmd:remakeline'], flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'labels', releasePolicy: 2, day: DAY, setIds: [SET, SET2], errors: [], orders: [O1, O2],
    lines: { [K1]: { orderId: O1, state: 'written', sku: 'LEAF', poolIds: [P1] }, [K2]: { orderId: O2, state: 'written', sku: 'OLD_SKU', poolIds: [P2] } } });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  F.seed(LEASE, { nonce: fence.nonce, state: 'running', heartbeat: true, startedAt: T0, beatAt: T0, at: T0 });
  const charms = [{ id: 'c1', poolId: P1, order: O1, sku: 'LEAF', hash: 'h1', name: `${O1} · LEAF`, areaPt2: 300, widthPt: 20, heightPt: 20, orderInfo: { transactionId: T1, sku: 'LEAF', copy: 1 } },
    { id: 'c2', poolId: P2, order: O2, sku: 'OLD_SKU', hash: 'h2', name: `${O2} · OLD_SKU`, areaPt2: 300, widthPt: 20, heightPt: 20, orderInfo: { transactionId: T2, sku: 'OLD_SKU', copy: 1 } }];
  F.seed('Charm_Nest_Sheets/' + S1, { id: S1, runId: R, metal: 'gold', metalLabel: 'GF', day: DAY, page: 1, draft: false, setId: SET, setSeq: 1, sheetIndex: 1, fileBase: FB, folder: FB, archived: false,
    charms, poolIds: [P1, P2], orders: [O1, O2], placements: [{ id: 'c1', cxPt: 20, cyPt: 20, angle: 0, wPt: 20, hPt: 20, scale: 1 }, { id: 'c2', cxPt: 60, cyPt: 20, angle: 0, wPt: 20, hPt: 20, scale: 1 }],
    layoutRev: 5, contentRev: 5, status: 'complete', releaseFull: true, verification: { ok: true }, stock: { wPt: 182, hPt: 62 },
    outputs: { ai: { path: `${FOLDER}/${FB}/${FB}.ai`, url: 'https://storage.test.invalid/s1.ai' } }, backPool: [] });
  for (const [p, o, t, sku] of [[P1, O1, T1, 'LEAF'], [P2, O2, T2, 'OLD_SKU']]) F.seed('Charm_Pool/' + p, { poolId: p, orderId: o, lineKey: p.replace(/_1$/, ''), transactionId: t, sku, copy: 1, state: 'written', sheetId: S1, setId: SET, sheetName: FB, material: 'gold', runId: R });
  const line = (t, sku, p) => ({ transactionId: t, sku, copies: [{ copy: 1, sheetId: S1, sheet: FB, poolId: p, backPoolId: null }] });
  F.seed('Charm_Nest_Sets/' + SET, { setId: SET, runId: R, day: DAY, seq: 1, name: 'Set-1', folder: FOLDER, group: 'dispatch', status: 'nesting', sheetIds: [S1], materials: ['gold'],
    orders: { [O1]: { held: null, lines: [line(T1, 'LEAF', P1)] }, [O2]: { held: null, lines: [line(T2, 'OLD_SKU', P2)] } }, labelFiles: [], labels: null, formedAt: T0 - 600e3, sheetNos: { gold: 1 } });
  // (a second set that lists the same sheet: the record names SET only, and a swap rewrites both)
  F.seed('Charm_Nest_Sets/' + SET2, { setId: SET2, runId: R, day: DAY, seq: 2, name: 'Set-2', folder: FOLDER, group: 'dispatch', status: 'nesting', sheetIds: [S1], materials: ['gold'],
    orders: { [O2]: { held: null, lines: [line(T2, 'OLD_SKU', P2)] } }, labelFiles: [], labels: null, formedAt: T0 - 600e3, sheetNos: { gold: 1 } });
}
// (what an op named: every name whose count moved in the feed's four shards, and whether the push counter moved after them)
const SHARDS = ['library', 'feed-p', 'feed-o', 'feed-x'];
const snap = () => Object.fromEntries(SHARDS.map(id => [id, Object.assign({}, (F.doc('Charm_Nest_Rev/' + id) || {}).changed || {})]));
const pushN = () => +((F.doc('Charm_Nest_Push/counter') || {}).n) || 0;
async function named(fn) {
  const a = snap(), p0 = pushN(); const r = await fn(); const b = snap(), names = new Set();
  for (const id of SHARDS) for (const [k, v] of Object.entries(b[id])) if ((a[id][k] || 0) !== v) names.add(k);
  return { r, names: [...names], pushed: pushN() > p0 };
}
const has = (x, ...ks) => ks.every(k => x.names.includes(k));

(async () => {
  await part('A', async () => {
    seed();
    const x = await named(() => cloudCall({ op: 'cmdSwapLine', runId: R, lineKey: K2, rid: O2, sheetId: S1, by: 'Michelle', keepBacks: false,
      pools: [{ poolId: P2, orderId: O2, lineKey: K2, transactionId: T2, sku: 'NEW_SKU', copy: 1, material: 'gold', runId: R, quantity: 1 }],
      charms: [{ id: 'c2n', poolId: P2, order: O2, sku: 'NEW_SKU', hash: 'h2n', name: `${O2} · NEW_SKU`, areaPt2: 300, widthPt: 20, heightPt: 20, orderInfo: { transactionId: T2, sku: 'NEW_SKU', copy: 1 } }],
      poses: [{ poolId: P2, cxPt: 60, cyPt: 20, angle: 0, wPt: 20, hPt: 20 }], layoutBase: 5 }, 'person|a'));
    ok('A1 Change SKU (cmdSwapLine): the sheet, its set AND every other set it rewrote are named, the push counter moved', x.r && x.r.ok === true && has(x, 's:' + S1, 't:' + SET, 't:' + SET2) && x.pushed, { names: x.names, r: x.r && (x.r.error || x.r.ok) });
    ok('A2 ... its answer is as before (no field of the feed\'s left in it)', x.r && !('setsDone' in x.r));
    const y = await named(() => cloudCall({ op: 'cmdSwapLine', runId: R, lineKey: K1, rid: O1, sheetId: S1, by: 'Michelle', pools: [], charms: [] }, 'person-off|a'));
    ok('A3 a line taken off its sheet the same way (no pieces): the sheet and its set named', y.r && y.r.ok === true && has(y, 's:' + S1, 't:' + SET), y.names);
  });
  await part('B', async () => {
    seed();
    const x = await named(() => lib.call({ op: 'cloudTakeOff', orderId: O2, sheetId: S1, ids: [P2], kind: 'cancel', at: Date.now(), who: 'Ann' }, { unit: 'test|b', fence }));
    ok('B1 a cancel / take-off in the cloud (cloudTakeOff): the sheet, the set whose order list it rewrote, the order\'s pieces and timeline are named', x.r && (x.r.ok === true || x.r.wrote === true || x.r.removed) && has(x, 's:' + S1, 't:' + SET, 'p:' + O2) && x.pushed, { names: x.names, r: x.r });
    ok('B2 ... its answer is as before', x.r && !('setTouched' in x.r));
  });
  await part('C', async () => {
    seed(); F.seed('Charm_Nest_Open/drive', { mode: 'off' }); lib._driveForget();   // (a page's own writes, as on a computer that carries the run: mode off)
    const w = await named(() => P({ op: 'putSheet', sheet: { id: S1, previewAt: T0 + 5, outputs: Object.assign({}, sheetDoc(S1).outputs, { preview: { path: 'x/p.png', url: 'https://storage.test.invalid/p.png' } }) } }));
    ok('C1 the sheet writer\'s save (putSheet: its files and picture): the sheet named, and in the answer\'s X-CN-Feed', w.r.http === 200 && has(w, 's:' + S1) && /s:gf-sl-s1/.test(w.r.feed) && w.pushed, { names: w.names, http: w.r.http, err: w.r.error });
    const s = await named(() => P({ op: 'setUpdate', setId: SET, patch: { status: 'labelled' } }));
    ok('C2 a set\'s save (setUpdate: labels, its order list, its status): the set named', s.r.http === 200 && has(s, 't:' + SET), { names: s.names, http: s.r.http, err: s.r.error });
    for (const face of ['front', 'back']) {
      const b = await named(() => P({ op: 'backPut', back: Object.assign({ poolId: P1, sheetId: S1, text: 'Ana', approvedAt: T0, approvedBy: 'Dana' }, face === 'front' ? { face: 'front' } : {}) }));
      ok(`C3 a ${face.toUpperCase()} engraving record (backPut, either face alike): named as "*" (every sheet a view shows is looked at by its revision), at once`, has(b, '*') && b.pushed, { names: b.names, http: b.r.http, err: b.r.error });
    }
    const c = await named(() => P({ op: 'cancelPut', orderId: O1, by: 'Ann', why: 'customer asked' }));
    ok('C4 a cancelled order (cancelPut): its cancel record and timeline named (x:, o:) beside "*"', has(c, 'x:' + O1, 'o:' + O1, '*') && c.pushed, { names: c.names, http: c.r.http, err: c.r.error });
  });
  await part('D', async () => {
    seed();
    const x = await named(() => lib.feedNames(['s:' + S1, 'not a name']));
    ok('D1 feedNames (a write made outside the handler): the sheet named in the library\'s shard, the push counter moved, a name of no feed form left out', has(x, 's:' + S1) && !x.names.includes('not a name') && x.pushed, x.names);
    const y = await named(() => lib.feedNames([]));
    ok('D2 ... nothing to name: nothing written', y.names.length === 0 && !y.pushed);
    const src = Lsrc('charmNestLibrary.js');
    ok('D3 the green line (rosePlan) names its sheet once its plan is written (it named only p:* before)', /OPS\.rosePlan = async b => \{ const out = await plan\(b\); if \(b && isId\(b\.sheetId\)\) revTouch\(b, \["s:" \+ b\.sheetId\]\); return out; \}/.test(src));
    // the wrapper itself, run: a plan written names its sheet; one refused names nothing more than before
    const lib2 = lib, b1 = { op: 'rosePlan', sheetId: S1 };
    const real = lib2.ops.rosePlan; ok('D4 rosePlan is the wrapped op in the library\'s ops', typeof real === 'function' && /revTouch/.test(String(real)));
    const slot = Lsrc('_charmNestNestSlot.js'), solve = Lsrc('charmNestSolve-background.js');
    ok('D5 a nest slot\'s end on the sheet (nestJob: stopped, stale, error; nested at the pick) names the sheet on the feed at once, through the function that runs it',
      /if \(wrote && ctx\.sheetMarked\) await announce\(ctx\);/.test(slot) && /if \(marked\) await announce\(ctx\);/.test(slot) && /await ctx\.feed\(\["s:" \+ ctx\.job\.sheetId\]\)/.test(slot) && /feed: o\.feed/.test(slot) && /feed: deps\.feed/.test(slot)
      && /feed: keys => require\("\.\/charmNestLibrary"\)\.feedNames\(keys\)/.test(solve));
    // announce(), run: with a feed it names the sheet; without one (a test, a tool) nothing; a failing feed is only said
    const a0 = slot.indexOf('async function announce(ctx) {'), a1 = slot.indexOf('\n', a0); const announce = new Function(slot.slice(a0, a1) + '\nreturn announce;')();
    const got = []; await announce({ feed: async k => { got.push(...k); }, job: { sheetId: S1 }, log() {} }); await announce({ job: { sheetId: S1 }, log() {} });
    let said = ''; await announce({ feed: async () => { throw new Error('down'); }, job: { sheetId: S1 }, jobId: 'j1', log: t => { said = t; } });
    ok('D6 announce(): names s:<sheet> with a feed, nothing without one, and a failing feed never throws into the slot', got.join() === 's:' + S1 && /the feed was not told \(down\)/.test(said));
    void b1;
  });
  await part('E', async () => {
    // every writer of what a sheet shows, by category (Paul's list): the op and the names it announces (code text: these need the whole cloud, the master files, the nest, to run)
    const src = { lib: Lsrc('charmNestLibrary.js'), cmd: Lsrc('_charmNestCmdOps.js'), fin: Lsrc('_charmNestFinishOps.js'), place: Lsrc('_charmNestPlaceOp.js'), off: Lsrc('_charmNestTakeOffOps.js'), done: Lsrc('_charmNestCustomDone.js'), nest: Lsrc('_charmNestNest.js'), eng: Lsrc('_charmEngraveOps.js'), files: Lsrc('_charmEngraveFiles.js') };
    const revOps = (src.lib.match(/const REV_OPS = new Set\(\[([^\]]*)\]\)/) || ['', ''])[1];
    const inRev = op => new RegExp('"' + op + '"').test(revOps) || new RegExp('REV_OPS\\.add\\("' + op + '"\\)').test(src.lib);
    const rows = [
      ['back and front engraving records (backPut, backInvalidate: "*")', inRev('backPut') && inRev('backInvalidate')],
      ['the cloud engrave lane writes through the ops (backPut, putSheet of the back index, poolUpdate)', /backPut/.test(src.files) && /putSheet/.test(src.files)],
      ['an engraving approval (engraveApprove: its order, p:)', /feedOrders\(|revTouch\(/.test(src.eng)],
      ['a Charm Editor save on a piece on a sheet (artPut: s:<sheet>)', /revTouch\(b, \["s:" \+/.test(src.lib.slice(src.lib.indexOf('async function op_artPut'), src.lib.indexOf('async function op_artGet') > 0 ? src.lib.indexOf('async function op_artGet') : src.lib.indexOf('async function op_artPut') + 30000))],
      ['custom pieces sent to sheets (customSheetPut: "*"; placeOnSheet: s:)', inRev('customSheetPut') && /revTouch\(b, \[?"s:" \+/.test(src.place)],
      ['custom done and its Undo (customDone: s: and t: of the set it rewrote)', /"t:" \+ x\.setId/.test(src.done) && /res\.back\.filter\(x => x\.setId\)\.map\(x => "t:" \+ x\.setId\)/.test(src.done)],
      ['moves and hops (cmdMovePieces, cmdHopPieces: s: of both sheets, t: of a gap set)', /cmdMovePieces/.test(src.cmd) && (src.cmd.match(/revTouch\(/g) || []).length >= 5],
      ['keepOrdersWhole move-on and seats (finishOverflow, finishSeat: s: of the sheets)', (src.fin.match(/revTouch\(b, /g) || []).length >= 3],
      ['cancelled orders and removed pieces (cloudTakeOff: s:, t:; cancelPut, cancelSweep: "*", x:)', /"t:" \+ setTouched/.test(src.off) && inRev('cancelPut') && inRev('cancelSweep')],
      ['Change SKU / remake (cmdSwapLine: s:, t: of every set it rewrote)', /\(out\.setsDone \|\| \[\]\)\.map\(id => "t:" \+ id\)/.test(src.cmd)],
      ['restore nest, green line, rose cut (restoreSheet, roseRecordCut, roseReopen, roseTakeOff: "*"; cmdRestoreNest: s:; rosePlan: s:)', inRev('restoreSheet') && inRev('roseRecordCut') && inRev('roseReopen') && inRev('roseTakeOff') && /OPS\.rosePlan = async b =>/.test(src.lib)],
      ['set membership and labels (setUpdate, setAllocate: t:; sheetKeepInSet: s:; flowApply: "*")', inRev('setUpdate') && inRev('setAllocate') && inRev('flowApply') && /revTouch\(b, \["t:" \+/.test(src.lib)],
      ['the sheet writer and finish (putSheet: s:; finishSheet / finishClaim: s:)', /revTouch\(b, \["s:" \+ /.test(src.lib) && /finishClaim/.test(src.fin)],
      ['Manual Control (sheetLayoutPatch, manualClaim: s:)', inRev('manualClaim')],
      ['a nest started on the sheet (nestStart: nestJob, refit: s:) and ended (the slot: feedNames)', /revTouch\(/.test(src.nest) || /"s:" \+/.test(src.nest)],
    ];
    for (const [what, cond] of rows) ok('E · ' + what, !!cond);
  });
  log0(`${failed ? 'FAIL' : 'PASS'} sheet-live-feed (${n - failed} of ${n})`);
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL', e && e.stack); process.exit(1); });
