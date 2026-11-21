// GAP G2 (Paul, 21 Nov 2026): item 13 made a sheet hop a cloud request, but it needed a NEW lane "cmd:hop" in the live drive.cloudSteps that only the owner can add (so it was refused
// today), and a set already approved did not get its files written again after a hop (the page used Review.inPlaceRefile). Offline (_cloudFakes.cjs):
//   A  the LIVE cloudSteps (no "cmd:hop"; "cmd:seat" and "cmd:renest" on): a viewer's hop is done by the cloud, answered done in words, the pieces where they were put
//   B  "cmd:hop" alone is still accepted; "cmd:seat" alone or "cmd:renest" alone covers it; none of the three: answered in words (never silent), nothing moved
//   C  a hop that touches a sheet of an APPROVED set (labelled, its labels PDF made): the cloud asks itself for that set's files (labelsAsk setFiles, the labels pass item's
//      answer), and the answer says so; a hop that touches no approved set asks nothing
//   D  the page: CNViewer.lane("cmd:hop") (charm-nest-viewer.js in a bare context) follows the same rule; the hop's refusal words name the sheet commands; new ?v= tokens
//   NODE_PATH=<deps> node tests/charm-nest/viewer-g2.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), vm = require('vm');
const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1200))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Unit = F.load('_charmNestCmdUnit.js'), Standby = F.load('_charmNestStandby.js');
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const CV = 'cloud14-20261120', DEV = 'd0staff0001', R = 'run-2026-11-21-gapg2', A = 'g10-a-gapg2', B = 'g10-b-gapg2';
const LIVE = 'pull claim pool plan nest checkpoint engrave revalidate labels commit complete files takeoff rosecut sweeps cmd:reorder cmd:consolidate cmd:empty cmd:delete cmd:repicture cmd:seat cmd:renest cmd:remake cmd:unkeep cmd:placewaiting cmd:customsend cmd:setrebuild cmd:pairnow cmd:leaveset cmd:sendhome cmd:remakeline cmd:keepsync cmd:release cmd:skuonce'.split(' ');
const pid = (o, l, n) => `${o}_${o}${l}_${n}`;
const charm = (id, poolId) => ({ id, poolId, order: poolId.split('_')[0], name: id, areaPt2: 300, widthPt: 20, heightPt: 20 });
function seed(steps, o) {
  o = o || {};
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: steps, flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [], processingComplete: true });
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  const sheet = (id, page, list, extra) => {
    const charms = list.map(([cid, p]) => charm(cid, p));
    F.seed('Charm_Nest_Sheets/' + id, Object.assign({ id, runId: R, metal: 'gold10k', page, draft: true, status: 'complete', layoutRev: 4, contentRev: 4, fileBase: id, charms, poolIds: charms.map(c => c.poolId),
      placements: charms.map((c, i) => ({ id: c.id, cxPt: 20 + i * 30, cyPt: 30, angle: 0, wPt: 20, hPt: 20, scale: 1 })), orders: [...new Set(charms.map(c => c.order))] }, extra || {}));
    for (const c of charms) F.seed('Charm_Pool/' + c.poolId, { poolId: c.poolId, runId: R, orderId: c.order, state: 'nested', sheetId: id });
  };
  // A: order 111 (a pair) and order 222; B: order 333. With o.approved, A is in Set 7, approved (labelled, its labels PDF made, not sent to the station)
  sheet(A, 1, [['a1', pid('111', '1', 1)], ['a2', pid('111', '1', 2)], ['a3', pid('222', '1', 1)]], o.approved ? { draft: false, setId: 'set-g2', setSeq: 7 } : {});
  sheet(B, 2, [['b1', pid('333', '1', 1)]]);
  if (o.approved) F.seed('Charm_Nest_Sets/set-g2', { setId: 'set-g2', runId: R, seq: 7, name: 'Set 7', status: 'labelled', sheetIds: [A], orders: {}, labels: { pdf: { path: 'x/labels/Set 7_labels.pdf', url: 'https://example.invalid/l.pdf' } } });
}
async function pass() {
  F.net.kicks.length = 0;
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}
const hopCmd = o => Object.assign({ type: 'hop', sourceId: A, targetId: B, ids: [pid('111', '1', 1), pid('111', '1', 2)], poses: [{ poolId: pid('111', '1', 1), cxPt: 120, cyPt: 60, angle: 90 }, { poolId: pid('111', '1', 2), cxPt: 150, cyPt: 60, angle: 90 }], layoutBase: 4 }, o || {});
const cmdOf = id => Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}).find(c => c.id === id) || {};
const moved = () => { const b = F.doc('Charm_Nest_Sheets/' + B) || {}; return (b.poolIds || []).includes(pid('111', '1', 1)) && (b.poolIds || []).includes(pid('111', '1', 2)); };
async function hop(steps, o) {
  seed(steps, o);
  const r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: hopCmd(), cv: CV });
  if (!r.id) return { put: r, c: {} };
  await pass();
  return { put: r, c: cmdOf(r.id) };
}

(async () => {
  /* A · the live lanes, no owner step */
  ok('A · the live cloudSteps hold no "cmd:hop" (the owner step this gap removes)', !LIVE.includes('cmd:hop') && LIVE.includes('cmd:seat') && LIVE.includes('cmd:renest'));
  let h = await hop(LIVE);
  const b = F.doc('Charm_Nest_Sheets/' + B), pose = id => (b.placements || []).find(q => q.id === id);
  ok('A · the viewer\'s hop is taken and done by the cloud, answered done in words', h.put.http === 200 && h.c.state === 'done' && /Order 111: 2 pieces moved from .* to .* where they were put/.test(h.c.text || ''), h);
  ok('A · the pieces are on B where they were put; both sheets owe their files; no nest owed', moved() && pose('a1').cxPt === 120 && pose('a2').cxPt === 150 && b.filesOwed > 0 && F.doc('Charm_Nest_Sheets/' + A).filesOwed > 0 && !b.nestOwed && !b.dirty, b);
  ok('A · nothing was asked for a set (A and B are in none)', !F.list('Charm_Nest_Label_Asks').length, F.list('Charm_Nest_Label_Asks'));
  /* B · which lanes cover it */
  for (const [steps, word] of [[['nest', 'cmd:hop'], '"cmd:hop" itself'], [['nest', 'cmd:seat'], '"cmd:seat" alone'], [['nest', 'cmd:renest'], '"cmd:renest" alone']]) {
    h = await hop(steps);
    ok(`B · ${word}: the hop is done`, h.c.state === 'done' && moved(), h.c);
  }
  h = await hop(['nest', 'cmd:reorder']);
  ok('B · none of cmd:hop / cmd:seat / cmd:renest: answered in words, nothing moved', h.c.state === 'failed' && /hop/.test(h.c.text || '') && !moved() && F.doc('Charm_Nest_Sheets/' + A).poolIds.length === 3, h.c);
  ok('B · the one rule (Standby.cmdLaneOn): hop covered by seat / renest; other types keep their own lane', Standby.cmdLaneOn(['cmd:seat'], 'hop') && Standby.cmdLaneOn(['cmd:renest'], 'hop') && Standby.cmdLaneOn(['cmd:hop'], 'hop') && !Standby.cmdLaneOn(['cmd:reorder'], 'hop') && !Standby.cmdLaneOn(['cmd:seat'], 'renest') && Standby.cmdLaneOn(['cmd:seat'], 'seat') && !Standby.cmdLaneOn(null, 'hop'));
  /* C · an approved set is written again */
  h = await hop(LIVE, { approved: true });
  const ask = F.doc('Charm_Nest_Label_Asks/set-set-g2');
  ok('C · a hop off a sheet of an approved set is done', h.c.state === 'done' && moved(), h.c);
  ok('C · the cloud asked for that set\'s files again (labelsAsk setFiles, waiting for the labels pass item)', !!ask && ask.kind === 'setFiles' && ask.setId === 'set-g2' && ask.state === 'asked' && ask.runId === R, ask);
  ok('C · the answer says the approved set\'s files are written again', /Set 7 was already approved: its QR labels, labels PDF, manifest and set\.json are written again/.test(h.c.text || ''), h.c.text);
  const set7 = F.doc('Charm_Nest_Sets/set-g2') || {};
  log0(`info C · Set 7 after the pass: status ${JSON.stringify(set7.status)}, sheets ${JSON.stringify(set7.sheetIds)}, labels ${set7.labels ? Object.keys(set7.labels).join(',') : 'none'}`);
  ok('C · the set stays the set it was (still lists sheet A, not sent to the station, not superseded)', (set7.sheetIds || []).includes(A) && !set7.committedAt && !/^superseded|^complete/.test(String(set7.status || '')), set7);
  /* D · the page */
  const src = fs.readFileSync(path.join(__dirname, '../../charm-nest-viewer.js'), 'utf8');
  async function viewerLane(steps, name) {
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout: (f, ms) => setTimeout(f, Math.min(ms, 5)), clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, Date, JSON, performance: { now: () => 1e6 } };
    w.window = w; w.toast = () => {}; w.CharmNestRunFollow = { state: () => ({ runId: null }), dev: () => 'd0viewer01', now: () => {} };
    w.CN = { api: async (fn, body) => (body.op === 'driveGet' ? { ok: true, mode: 'on', drive: { mode: 'on', cloudSteps: steps } } : { ok: true }) };
    vm.runInContext(src, vm.createContext(w));
    await w.CNViewer.ready();
    return w.CNViewer.lane(name);
  }
  ok('D · the page: CNViewer.lane("cmd:hop") is on with the live lanes, with cmd:hop alone, off with none of the three', await viewerLane(LIVE, 'cmd:hop') && await viewerLane(['cmd:hop'], 'cmd:hop') && await viewerLane(['cmd:renest'], 'cmd:hop') && !(await viewerLane(['cmd:reorder'], 'cmd:hop')) && !(await viewerLane(LIVE, 'cmd:nope')) && await viewerLane(LIVE, 'cmd:seat'));
  const hopSrc = fs.readFileSync(path.join(__dirname, '../../charm-nest-sheet-hop.js'), 'utf8');
  ok('D · the hop\'s refusal (lanes off) names the sheet commands, not an owner step for "cmd:hop"', /CNViewer\.lane\("cmd:hop"\)\) return refuse\("Not moved: the cloud's sheet commands are not on/.test(hopSrc) && !/the owner turns on “cmd:hop”/.test(hopSrc));
  const html = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8');
  ok('D · charm-nest-1.html loads the changed viewer and hop under new tokens', /charm-nest-viewer\.js\?v=[^"]*-g2(?:-\w+)*"/.test(html) && /charm-nest-sheet-hop\.js\?v=[^"]*-g2(?:-\w+)*"/.test(html));
  ok('rule · Etsy was never called', F.etsy.calls === 0);
  log0(failed ? `FAIL viewer-g2 (${failed})` : 'PASS viewer-g2');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL crashed: ' + (e && e.stack || e)); process.exit(1); });
