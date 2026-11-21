// GAP G1 (Paul, 21 Nov 2026): in the sheet window, "Fill the freed room" / Place / Place all / by hand on a staff computer (a viewer) in mode "on" went moveIn -> rewritePage ->
// startNest (_byHand): the cloud was asked for a FRESH nest of both saved sheets, rearranging sheets the person never asked to change. Now it is the cloud's hop request
// (runCmdPut hop, fill: true; cmdHopPieces): the pieces go into the freed room where the window planned them, nothing else moves, nothing is nested. Offline (_cloudFakes.cjs):
//   A  mode on: a viewer's runCmdPut hop with fill (two sheets it comes from) is taken; the page's old writes (putSheet with charms) are still refused
//   B  the driver's pass: on Gold sheets (not only 10K / 14K: the freed room is filled on any metal but Rose Gold) exactly the planned pieces are on the target where planned (pinned),
//      every other piece of every sheet keeps its pose, the sheets owe their files, no nest is owed or started, the piece records say "fill"; answered done in words
//   C  refused in words, nothing moved: the same request without fill on Gold (the hop's 10K / 14K rule unchanged), fill on Rose Gold, the lane not on
//   D  the page (charm-nest-sheetwin.js, static): moveIn and placeAll on a viewer go to fillCloud before any flow, save or nest; mode off untouched; new cache token
//   E  the page (fillCloud run with stand-ins): ONE runCmdPut (fill, the sources, the target's layoutBase, each piece's pose) and nothing else; the cloud's answer shown in words
//      (done and refused); lane off refused in words with nothing sent; five source sheets go as two requests (the second without layoutBase)
//   NODE_PATH=<deps> node tests/charm-nest/viewer-g1.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 21, 17, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1200))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Unit = F.load('_charmNestCmdUnit.js');
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const CV = 'cloud14-20261120', DEV = 'd0staff0001', R = 'run-2026-11-21-g1', A = 'gf-a-g1', B = 'gf-b-g1', C = 'gf-c-g1';
const pid = (o, l, n) => `${o}_${o}${l}_${n}`;
const charm = (id, poolId) => ({ id, poolId, order: poolId.split('_')[0], name: id, areaPt2: 300, widthPt: 20, heightPt: 20 });
function seed(steps, metal) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: steps, flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [], processingComplete: true });
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  const sheet = (id, page, list) => {
    const charms = list.map(([cid, p]) => charm(cid, p));
    F.seed('Charm_Nest_Sheets/' + id, { id, runId: R, metal, page, draft: true, status: 'complete', layoutRev: 7, contentRev: 7, fileBase: id, charms, poolIds: charms.map(c => c.poolId),
      placements: charms.map((c, i) => ({ id: c.id, cxPt: 20 + i * 30, cyPt: 30, angle: 0, wPt: 20, hPt: 20, scale: 1 })), orders: [...new Set(charms.map(c => c.order))] });
    for (const c of charms) F.seed('Charm_Pool/' + c.poolId, { poolId: c.poolId, runId: R, orderId: c.order, state: 'nested', sheetId: id });
  };
  // A: order 111 (a pair) and order 222; B: the sheet with the freed room, order 333 left on it; C: orders 555 and 666
  sheet(A, 1, [['a1', pid('111', '1', 1)], ['a2', pid('111', '1', 2)], ['a3', pid('222', '1', 1)]]);
  sheet(B, 2, [['b1', pid('333', '1', 1)]]);
  sheet(C, 3, [['c1', pid('555', '1', 1)], ['c2', pid('666', '1', 1)]]);
}
async function pass() {
  F.net.kicks.length = 0;
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}
// the request as the page's fillCloud sends it: 222 from A and 555 from C into B's freed room
const fillCmd = (o) => Object.assign({ type: 'hop', fill: true, sourceId: A, sourceIds: [A, C], targetId: B, ids: [pid('222', '1', 1), pid('555', '1', 1)],
  poses: [{ poolId: pid('222', '1', 1), cxPt: 140, cyPt: 70, angle: 0 }, { poolId: pid('555', '1', 1), cxPt: 170, cyPt: 70, angle: 90 }], layoutBase: 7, label: 'Fill the freed room' }, o || {});
const cmdOf = id => Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}).find(c => c.id === id) || {};
const ROOT = path.join(__dirname, '../..');

async function partE() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-sheetwin.js'), 'utf8');
  const body = src.slice(src.indexOf('  const FILL_ASKED = new Set();'), src.indexOf('  // o.free: the nest itself picks the spots'));
  function kit(opt = {}) {
    const calls = [], puts = [], toasts = [], unlanded = [], freedSet = [], opened = [];
    const W = { fill: { state: 'ready' }, dlg: { open: true }, work: null, flow: null, id: opt.winId || 'none' };
    const FREED = new Map([[B, [{ x: { p: { cxPt: 140, cyPt: 70, wPt: 20, hPt: 20 } } }, { x: { p: { cxPt: 300, cyPt: 300, wPt: 20, hPt: 20 } } }]]]);
    const env = {
      window: { CharmNestRunFollow: { state: () => ({ runId: R }), dev: () => DEV, now: () => {} } },
      CNViewer: { on: () => true, lane: n => opt.lane !== false && n === 'cmd:hop' },
      api: async (fn, b) => {
        calls.push(b);
        if (b.op === 'runCmdPut') { const id = 'c' + (puts.length + 1); puts.push(Object.assign({ id }, b)); return { ok: true, id }; }
        if (b.op === 'runCmdList') return { cmds: puts.map(p => ({ id: p.id, state: opt.state || 'done', text: opt.text || 'Orders 222, 555: 2 pieces moved from A into the freed room on B' })) };
        return { error: 'unexpected ' + b.op };
      },
      sheetWord: (id) => 'Sheet ' + id, unland: rid => unlanded.push(rid), tryDo: f => { try { return f ? f() : null; } catch (_) { return null; } },
      stepOf: (text, state, detail) => ({ text, state: state || '', detail: detail || '' }),
      beginFlow: w => { if (W.flow) return null; W.flow = w; W.work = w; return w; }, endFlow: w => { if (W.flow === w) W.flow = null; },
      W, renderFill: () => {}, renderWork: () => {}, esc: s => String(s), toast: (m, k) => toasts.push([m, k]),
      setFreed: (id, list) => { freedSet.push([id, list.length]); FREED.set(id, list); }, FREED, open: (id, o) => opened.push([id, o]), pause: () => (opt.hang ? new Promise(() => {}) : new Promise(r => setImmediate(r)))
    };
    const mod = new Function(...Object.keys(env), body + '\nreturn { fillCloud, FILL_ASKED };')(...Object.values(env));
    return Object.assign(mod, { calls, puts, toasts, unlanded, freedSet, opened, W });
  }
  const settle = async k => { for (let i = 0; i < 200 && !k.toasts.length; i++) await new Promise(r => setImmediate(r)); };
  const sh = (id, extra) => Object.assign({ sheetId: id, fileBase: id, runId: R, metal: 'gold' }, extra || {});
  const sA = sh(A), sC = sh(C), tB = sh(B, { layoutRev: 7 });
  const spot = (poolId, src, cxPt, angle) => ({ c: { id: poolId, poolId }, src, cxPt, cyPt: 70, angle });
  const groups = () => [{ k: { rid: '222' }, spots: [spot(pid('222', '1', 1), sA, 140, 0)] }, { k: { rid: '555' }, spots: [spot(pid('555', '1', 1), sC, 170, 90)] }];
  // E1 · done
  let k = kit({ winId: B });
  const r = await k.fillCloud(groups(), tB, 'Michelle');
  const put = k.puts[0] || {};
  ok('E1 · ONE runCmdPut: hop with fill, both sheets it comes from, the target and its layoutBase, each piece where planned', r === true && k.puts.length === 1 && put.by === 'Michelle' && put.runId === R && put.dev === DEV && put.cmd.type === 'hop' && put.cmd.fill === true && put.cmd.sourceId === A && JSON.stringify(put.cmd.sourceIds) === JSON.stringify([A, C]) && put.cmd.targetId === B && put.cmd.layoutBase === 7 && JSON.stringify(put.cmd.poses) === JSON.stringify([{ poolId: pid('222', '1', 1), cxPt: 140, cyPt: 70, angle: 0 }, { poolId: pid('555', '1', 1), cxPt: 170, cyPt: 70, angle: 90 }]), k.puts);
  ok('E1 · the person sees it asked (the steps panel), and the window is free for the next change while the cloud works', k.W.work && /Asking the cloud: orders 222, 555 to Sheet/.test(k.W.work.title) && k.W.work.steps[0].state === 'ok' && k.W.work.steps[1].state === 'now' && !k.W.flow && k.FILL_ASKED.has(A) && k.FILL_ASKED.has(B) && k.FILL_ASKED.has(C), k.W.work);
  await settle(k);
  ok('E1 · the cloud\'s answer in words: a toast with its text, the panel done, the freed spot it took no longer free, the window read again', k.toasts.length === 1 && /into the freed room/.test(k.toasts[0][0]) && k.toasts[0][1] === 'ok' && k.W.work.state === 'done' && /moved to Sheet .* by the cloud/.test(k.W.work.title) && k.freedSet.some(([id, n]) => id === B && n === 1) && k.opened.length === 1 && k.opened[0][0] === B && !k.FILL_ASKED.size, { t: k.toasts, w: k.W.work, f: k.freedSet, o: k.opened });
  ok('E1 · nothing but runCmdPut and runCmdList was called (no putSheet, poolUpdate, nestStart)', k.calls.every(c => c.op === 'runCmdPut' || c.op === 'runCmdList'), k.calls.map(c => c.op));
  // E2 · refused by the cloud
  k = kit({ state: 'failed', text: 'Sheet B is in a set: it takes no new order (222)' });
  await k.fillCloud(groups(), tB, 'Michelle'); await settle(k);
  ok('E2 · refused: said in the cloud\'s words, the orders not drawn as landed, the panel failed', /not moved: Sheet B is in a set: it takes no new order \(222\)\. They stay where they are; no sheet was nested again\./.test(k.toasts[0] && k.toasts[0][0]) && k.toasts[0][1] === 'bad' && k.unlanded.includes('222') && k.unlanded.includes('555') && k.W.work.state === 'failed' && !k.freedSet.length, { t: k.toasts, w: k.W.work });
  // E3 · lane off
  k = kit({ lane: false });
  let err = null; try { await k.fillCloud(groups(), tB, 'Michelle'); } catch (e) { err = e; }
  ok('E3 · lane "cmd:hop" off: refused in words (the caller says "Not moved: ..."), nothing sent', err && /the cloud does not fill the freed room yet .*cmd:hop.*Nothing was changed: orders 222, 555 stay where they are and no sheet is nested again/.test(err.message) && !k.calls.length, err && err.message);
  // E4 · a second press while the cloud still works on these sheets
  k = kit({ hang: true });   // (the cloud still working: its answer never comes in this part)
  await k.fillCloud(groups(), tB, 'Michelle'); err = null;
  try { await k.fillCloud([groups()[0]], tB, 'Michelle'); } catch (e) { err = e; }
  ok('E4 · a second press on the same sheets while the cloud works: refused in words, nothing more sent', err && /still making the last move/.test(err.message) && k.puts.length === 1, err && err.message);
  // E5 · five sheets it comes from: two requests, layoutBase on the first only
  k = kit();
  const many = [1, 2, 3, 4, 5].map(i => ({ k: { rid: '70' + i }, spots: [spot(pid('70' + i, '1', 1), sh('gf-s' + i + '-g1'), 40 * i, 0)] }));
  await k.fillCloud(many, tB, 'Michelle'); await settle(k);
  ok('E5 · five sheets it comes from: two requests (four sheets each at most), layoutBase on the first only, both answers said', k.puts.length === 2 && k.puts[0].cmd.sourceIds.length === 4 && k.puts[1].cmd.sourceId === 'gf-s5-g1' && !k.puts[1].cmd.sourceIds && k.puts[0].cmd.layoutBase === 7 && !('layoutBase' in k.puts[1].cmd) && k.W.work.state === 'done', k.puts.map(p => p.cmd));
}

(async () => {
  const STEPS = ['nest', 'cmd:hop'];
  /* A */
  seed(STEPS, 'gold');
  let r = await P({ op: 'putSheet', sheet: { id: B, charms: [], placements: [], poolIds: [] }, dev: DEV, cv: CV });
  ok('A · mode on: the page\'s old write (putSheet with the pieces) is still refused', r.http === 409 && r.cloudCarries === true, r);
  const before = { A: JSON.parse(JSON.stringify(F.doc('Charm_Nest_Sheets/' + A))), B: JSON.parse(JSON.stringify(F.doc('Charm_Nest_Sheets/' + B))), C: JSON.parse(JSON.stringify(F.doc('Charm_Nest_Sheets/' + C))) };
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: fillCmd(), cv: CV });
  ok('A · the viewer\'s ONE request (runCmdPut hop, fill) is taken: fill, both sources, the poses, layoutBase', r.http === 200 && r.ok && r.id && r.cmd.args.fill === true && JSON.stringify(r.cmd.args.sourceIds) === JSON.stringify([A, C]) && r.cmd.args.poses.length === 2 && r.cmd.args.layoutBase === 7, r);
  const id1 = r.id;
  /* B */
  const p = await pass();
  const a = F.doc('Charm_Nest_Sheets/' + A), b = F.doc('Charm_Nest_Sheets/' + B), c = F.doc('Charm_Nest_Sheets/' + C), c1 = cmdOf(id1);
  const poseOf = (s, cid) => (s.placements || []).find(q => q.id === cid);
  ok('B · the request is answered done, in words (the freed room, nothing nested again)', c1.state === 'done' && /Orders 222, 555: 2 pieces moved from .* and .* into the freed room on .*, where they were planned; every other charm stayed where it was and no sheet was nested again/.test(c1.text || ''), { c1, p });
  ok('B · on B where planned (pinned, 555 turned 90), B\'s own piece where it was', poseOf(b, 'a3') && poseOf(b, 'a3').cxPt === 140 && poseOf(b, 'c1').cxPt === 170 && poseOf(b, 'c1').angle === 90 && b.charms.find(x => x.id === 'a3').pinned.cxPt === 140 && b.charms.find(x => x.id === 'c1').manual.by === 'Michelle' && JSON.stringify(poseOf(b, 'b1')) === JSON.stringify(poseOf(before.B, 'b1')), b);
  ok('B · A keeps the pair where it was, C keeps 666 where it was (nothing rearranged)', !a.poolIds.includes(pid('222', '1', 1)) && a.charms.length === 2 && ['a1', 'a2'].every(id => JSON.stringify(poseOf(a, id)) === JSON.stringify(poseOf(before.A, id))) && !c.poolIds.includes(pid('555', '1', 1)) && c.charms.length === 1 && JSON.stringify(poseOf(c, 'c2')) === JSON.stringify(poseOf(before.C, 'c2')), { a, c });
  ok('B · the three sheets owe their files; none owes a nest (no dirty, no nestOwed, status as it was)', [a, b, c].every(s => s.filesOwed > 0 && !s.dirty && !s.nestOwed && s.status === 'complete') && b.layoutRev === 8, [a, b, c].map(s => [s.filesOwed, s.dirty, s.nestOwed, s.status, s.layoutRev]));
  ok('B · the piece records say B, moved by the person into the freed room (movedHow fill)', [[pid('222', '1', 1), A], [pid('555', '1', 1), C]].every(([id, from]) => { const x = F.doc('Charm_Pool/' + id); return x.sheetId === B && x.movedBy === 'Michelle' && x.movedFrom === from && x.movedHow === 'fill'; }));
  ok('B · no nest was started (no nest job)', !F.list('Charm_Nest_Jobs').length, F.list('Charm_Nest_Jobs'));
  /* C */
  seed(STEPS, 'gold');
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: fillCmd({ fill: undefined }), cv: CV });
  await pass();
  let x = cmdOf(r.id);
  ok('C · without fill on Gold: the hop\'s own rule (10K / 14K only) unchanged, refused in words, nothing moved', x.state === 'failed' && /10K or 14K Gold, only/.test(x.text || '') && F.doc('Charm_Nest_Sheets/' + B).poolIds.length === 1, x);
  seed(STEPS, 'rose');
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: fillCmd(), cv: CV });
  await pass();
  x = cmdOf(r.id);
  ok('C · fill on Rose Gold: refused in words, nothing moved', x.state === 'failed' && /never Rose Gold/.test(x.text || '') && F.doc('Charm_Nest_Sheets/' + B).poolIds.length === 1, x);
  seed(['nest'], 'gold');
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: fillCmd(), cv: CV });
  await pass();
  x = cmdOf(r.id);
  ok('C · lane "cmd:hop" not on: answered in words, nothing moved', x.state === 'failed' && /hop/.test(x.text || '') && F.doc('Charm_Nest_Sheets/' + B).poolIds.length === 1, x);
  /* D (static: the page) */
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-sheetwin.js'), 'utf8');
  const mv = src.slice(src.indexOf('  async function moveIn(k, spots, target, who, o = {}) {'), src.indexOf('  // undo a move that did not complete'));
  const pa = src.slice(src.indexOf('  async function placeAll(list, target, who) {'), src.indexOf('  function renderFill()'));
  const fc = src.slice(src.indexOf('  async function fillCloud(groups, target, who) {'), src.indexOf('  // o.free: the nest itself picks the spots'));
  ok('D · moveIn: a viewer\'s Place / by hand / Place (add) goes to fillCloud first, before any flow, save or nest (only CNViewer.on(); the hop and the Hold engine untouched)', /if \(!o\.hop && !o\.headless && window\.CNViewer && CNViewer\.on\(\)\) return fillCloud\(\[\{ k, spots \}\], target, who\);/.test(mv) && mv.indexOf('return fillCloud(') < mv.indexOf('beginFlow(') && mv.indexOf('return fillCloud(') < mv.indexOf('rewritePage('));
  ok('D · placeAll: a viewer\'s Place all is one fillCloud of every planned order, before the first moveIn', /if \(window\.CNViewer && CNViewer\.on\(\)\) return fillCloud\(list\.map/.test(pa) && pa.indexOf('return fillCloud(') < pa.indexOf('await moveIn('));
  ok('D · fillCloud sends runCmdPut (hop, fill) and nothing that saves or nests; refuses in words while the lane is off', (fc.match(/op: "runCmdPut"/g) || []).length === 1 && /type: "hop", fill: true/.test(fc) && !/putSheet|poolUpdate|nestStart|startNest|rewritePage|moveNow|Pool\.update/.test(fc) && /CNViewer\.lane\("cmd:hop"\)/.test(fc));
  ok('D · charm-nest-1.html loads the changed sheet window under a new token', /charm-nest-sheetwin\.js\?v=[^"]*-g1(?:-\w+)*"/.test(fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8')));
  /* E (the page's fillCloud, run) */
  await partE();
  ok('rule · Etsy was never called', F.etsy.calls === 0);
  log0(failed ? `FAIL viewer-g1 (${failed})` : 'PASS viewer-g1');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL crashed: ' + (e && e.stack || e)); process.exit(1); });
