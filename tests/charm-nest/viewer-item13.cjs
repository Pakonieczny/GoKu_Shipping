// ITEM 13 (Paul, 21 Nov 2026): a charm dragged onto the next / previous 10K sheet on a staff computer (a viewer) in mode "on" was refused ("The cloud carries every run now")
// and its fallback re-nested both sheets. Now the hop is ONE request (runCmdPut hop) and the cloud moves exactly those pieces where they were put. Offline (_cloudFakes.cjs):
//   A  mode on: a viewer's runCmdPut hop is taken; the page's old writes (putSheet with charms, poolUpdate with a sheet) are still refused
//   B  the driver's pass (commands unit only): the order's two pieces (a pair) are on B, pinned where they were put; every other piece of A and B keeps its pose; both owe their
//      files; no nest is owed (no dirty, no nestOwed); the piece records say B; the request is answered done in words
//   C  refused in words, nothing moved: half a pair, the target in a set with a new order (SETSHUT), the lane not on
//   D  the page (charm-nest-sheet-hop.js) sends ONE runCmdPut on a viewer and nothing else; mode off it never does (static)
//   NODE_PATH=<deps> node tests/charm-nest/viewer-item13.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1200))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Unit = F.load('_charmNestCmdUnit.js');
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const CV = 'cloud14-20261120', DEV = 'd0staff0001', R = 'run-2026-11-21-item13', A = 'g10-a-item13', B = 'g10-b-item13', C = 'g10-c-item13';
const pid = (o, l, n) => `${o}_${o}${l}_${n}`;
const charm = (id, poolId) => ({ id, poolId, order: poolId.split('_')[0], name: id, areaPt2: 300, widthPt: 20, heightPt: 20 });
function seed(steps) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: steps, flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [], processingComplete: true });
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  const sheet = (id, page, list, o) => {
    const charms = list.map(([cid, p]) => charm(cid, p));
    F.seed('Charm_Nest_Sheets/' + id, Object.assign({ id, runId: R, metal: 'gold10k', page, draft: true, status: 'complete', layoutRev: 4, contentRev: 4, fileBase: id, charms, poolIds: charms.map(c => c.poolId),
      placements: charms.map((c, i) => ({ id: c.id, cxPt: 20 + i * 30, cyPt: 30, angle: 0, wPt: 20, hPt: 20, scale: 1 })), orders: [...new Set(charms.map(c => c.order))] }, o || {}));
    for (const c of charms) F.seed('Charm_Pool/' + c.poolId, { poolId: c.poolId, runId: R, orderId: c.order, state: 'nested', sheetId: id });
  };
  // A: order 111 (a pair, two pieces of one line) and order 222; B: order 333; C: order 444, in a set
  sheet(A, 1, [['a1', pid('111', '1', 1)], ['a2', pid('111', '1', 2)], ['a3', pid('222', '1', 1)]]);
  sheet(B, 2, [['b1', pid('333', '1', 1)]]);
  sheet(C, 3, [['c1', pid('444', '1', 1)]], { draft: false, setId: 'set-i13' });
  F.seed('Charm_Nest_Sets/set-i13', { setId: 'set-i13', runId: R, status: 'awaiting review', sheetIds: [C], orders: {} });
}
async function pass() {
  F.net.kicks.length = 0;
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}
const hopCmd = (o) => Object.assign({ type: 'hop', sourceId: A, targetId: B, ids: [pid('111', '1', 1), pid('111', '1', 2)], poses: [{ poolId: pid('111', '1', 1), cxPt: 120, cyPt: 60, angle: 90 }, { poolId: pid('111', '1', 2), cxPt: 150, cyPt: 60, angle: 90 }], layoutBase: 4 }, o || {});
const cmdOf = id => Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}).find(c => c.id === id) || {};

(async () => {
  const STEPS = ['nest', 'cmd:hop'];
  /* A */
  seed(STEPS);
  let r = await P({ op: 'putSheet', sheet: { id: A, charms: [], placements: [], poolIds: [] }, dev: DEV, cv: CV });
  ok('A · mode on: the page\'s old hop write (putSheet with the pieces) is still refused', r.http === 409 && r.cloudCarries === true, r);
  const before = { A: JSON.parse(JSON.stringify(F.doc('Charm_Nest_Sheets/' + A))), B: JSON.parse(JSON.stringify(F.doc('Charm_Nest_Sheets/' + B))), C: JSON.parse(JSON.stringify(F.doc('Charm_Nest_Sheets/' + C))) };
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: hopCmd(), cv: CV });
  ok('A · the viewer\'s ONE request (runCmdPut hop) is taken, with the poses', r.http === 200 && r.ok && r.id && r.cmd.args.poses.length === 2 && r.cmd.args.layoutBase === 4, r);
  const id1 = r.id;
  /* B */
  const p = await pass();
  const a = F.doc('Charm_Nest_Sheets/' + A), b = F.doc('Charm_Nest_Sheets/' + B), c1 = cmdOf(id1);
  const poseOf = (s, cid) => (s.placements || []).find(q => q.id === cid);
  ok('B · the request is answered done, in words', c1.state === 'done' && /Order 111: 2 pieces moved from .* to .* where they were put/.test(c1.text || ''), { c1, p });
  ok('B · exactly the order\'s two pieces left A; order 222 stays on A where it was', !a.poolIds.includes(pid('111', '1', 1)) && !a.poolIds.includes(pid('111', '1', 2)) && a.charms.length === 1 && JSON.stringify(poseOf(a, 'a3')) === JSON.stringify(poseOf(before.A, 'a3')), a);
  ok('B · on B where they were put (pinned, angle 90, the manual mark), B\'s own piece where it was', poseOf(b, 'a1') && poseOf(b, 'a1').cxPt === 120 && poseOf(b, 'a1').angle === 90 && poseOf(b, 'a2').cxPt === 150 && b.charms.find(x => x.id === 'a1').pinned.cxPt === 120 && b.charms.find(x => x.id === 'a1').manual.by === 'Michelle' && JSON.stringify(poseOf(b, 'b1')) === JSON.stringify(poseOf(before.B, 'b1')), b);
  ok('B · both sheets owe their files; neither owes a nest (no dirty, no nestOwed, status as it was)', a.filesOwed > 0 && b.filesOwed > 0 && !a.dirty && !b.dirty && !a.nestOwed && !b.nestOwed && a.status === 'complete' && b.status === 'complete' && b.layoutRev === 5, { a: [a.filesOwed, a.dirty, a.nestOwed, a.status], b: [b.filesOwed, b.dirty, b.nestOwed, b.status, b.layoutRev] });
  ok('B · the piece records say B, moved by the person', [1, 2].every(n => { const x = F.doc('Charm_Pool/' + pid('111', '1', n)); return x.sheetId === B && x.movedBy === 'Michelle' && x.movedFrom === A; }));
  ok('B · nothing else moved (sheet C untouched)', JSON.stringify(F.doc('Charm_Nest_Sheets/' + C)) === JSON.stringify(before.C));
  ok('B · no nest was started (no nest job)', !F.list('Charm_Nest_Jobs').length, F.list('Charm_Nest_Jobs'));
  /* C */
  seed(STEPS);
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: hopCmd({ ids: [pid('111', '1', 1)], poses: [{ poolId: pid('111', '1', 1), cxPt: 120, cyPt: 60, angle: 0 }] }), cv: CV });
  await pass();
  let c = cmdOf(r.id);
  ok('C · half a pair: refused in words, nothing moved', c.state === 'failed' && /pair is never moved in halves/.test(c.text || '') && F.doc('Charm_Nest_Sheets/' + A).poolIds.length === 3, c);
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: hopCmd({ targetId: C, ids: [pid('222', '1', 1)], poses: [{ poolId: pid('222', '1', 1), cxPt: 120, cyPt: 60, angle: 0 }] }), cv: CV });
  await pass();
  c = cmdOf(r.id);
  ok('C · SETSHUT: a sheet in a set takes no new order: refused in words, nothing moved', c.state === 'failed' && /takes no new order/.test(c.text || '') && F.doc('Charm_Nest_Sheets/' + C).poolIds.length === 1, c);
  seed(['nest']);
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: hopCmd(), cv: CV });
  await pass();
  c = cmdOf(r.id);
  ok('C · lane "cmd:hop" not on: answered in words, nothing moved', c.state === 'failed' && /hop/.test(c.text || '') && F.doc('Charm_Nest_Sheets/' + A).poolIds.length === 3, c);
  /* D (static: the page) */
  const src = fs.readFileSync(path.join(__dirname, '../../charm-nest-sheet-hop.js'), 'utf8'), fn = src.slice(src.indexOf('async function hopCloud'), src.indexOf('/** The two saved records'));
  ok('D · the page: a viewer\'s hop goes to hopCloud before any save, line or nest (only when CNViewer.on())', /window\.CNViewer && CNViewer\.on\(\)\) return hopCloud/.test(src) && src.indexOf('return hopCloud') < src.indexOf('K.goldOpen([src, target])'));
  ok('D · hopCloud sends one runCmdPut and no putSheet / poolUpdate / nestStart; refuses in words while the lane is off', (fn.match(/op: "runCmdPut"/g) || []).length === 1 && !/putSheet|poolUpdate|nestStart|moveNow|moveIn/.test(fn) && /CNViewer\.lane\("cmd:hop"\)/.test(fn));
  ok('D · charm-nest-1.html loads the changed hop under a new token', /charm-nest-sheet-hop\.js\?v=[^"]*-i13[^"]*"/.test(fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8')));
  ok('rule · Etsy was never called', F.etsy.calls === 0);
  log0(failed ? `FAIL viewer-item13 (${failed})` : 'PASS viewer-item13');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL crashed: ' + (e && e.stack || e)); process.exit(1); });
