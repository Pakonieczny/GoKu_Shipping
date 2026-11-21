// G3 (Paul, 21 Nov 2026): in the Library, on a staff computer (a viewer) in mode "on", moving a DRAFT sheet of the open run into a set, out of one, or between sets was refused at once
// with "Its run is open on another screen" (charm-nest-flow.js): the old rule meant another COMPUTER carried the run, and since the cloud carries every run the press could never work.
// Now the move is the cloud's own request for the sheet's set (roseCutAsk kind include / exclude, lane "rosecut", _charmNestGreenLine.js member): the cloud applies it by the set rules
// (charm-nest-assembly-core.js, the set the person dropped it on as its hand set) and the person sees the cloud's answer in words. Mode off: the old plan, word for word.
//   S  OFFLINE, the real modules: charm-nest-viewer.js, charm-nest-set-edit.js and charm-nest-flow.js in a vm context (window = self) over the offline library (_cloudFakes.cjs) in mode "on"
//      with "rosecut" a cloud step; the cloud is the REAL ask unit (Green.askUnit), one pass each time the move waits for its answer:
//        a draft GF sheet dropped on Set 2 is put in Set 2 by the cloud; taken out again (drop on In progress); a GF sheet of Set 1 dropped on Set 2 goes over; a take-out the cloud refuses
//        (the owner's set) is said in the cloud's words and nothing is changed; mode off and the lane off; two drafts combined; the combine place; the server's own words for a page's
//        write to a set the cloud is making.
//   B  the page token is bumped for the changed page files.
// Offline only: no Etsy, no live cloud, no Firebase. Made-up names.   NODE_PATH=<deps> node tests/charm-nest/viewer-g3.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const T0 = Date.UTC(2026, 10, 21, 19, 0, 0), CV = 'cloud14-20261120', PAGE = 'd0michelleg3';
const R = 'run-2026-11-21-g3', SET1 = 'set-2026-11-21-g3a', SET2 = 'set-2026-11-21-g3b', DAY = '2026-11-21';

async function partS() {
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn, qe = console.error; console.log = () => {}; console.warn = () => {}; console.error = () => {};
  const out = [];
  try {
    const lib = F.load('charmNestLibrary.js');
    const Green = F.load('_charmNestGreenLine.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const pid = rid => `${rid}_${rid}1_1`;
    const sheet = (id, metal, rid, o) => {
      const p = pid(rid), c = { id: 'c-' + p, poolId: p, order: rid, areaPt2: 400, widthPt: 20, heightPt: 20 };
      F.seed('Charm_Pool/' + p, { poolId: p, runId: R, orderId: rid, state: 'placed', sheetId: id, material: metal });
      F.seed('Charm_Nest_Sheets/' + id, Object.assign({ id, runId: R, metal, page: 1, day: DAY, draft: true, setId: null, setSeq: null, sheetIndex: null, status: 'complete', archived: false,
        charms: [c], placements: [{ id: c.id, cxPt: 20, cyPt: 20, angle: 0 }], poolIds: [p], orders: [rid], outputs: { ai: { path: `x/${id}.ai` } }, verification: { ok: true }, releaseFull: true, placedCount: 1, charmCount: 1,
        persistedDone: true, fileBase: `${metal}_working_${id}`, layoutRev: 1, cardStartedAt: T0 - 3600e3 }, o || {}));
    };
    const inSetRec = (set, seq, n) => ({ draft: false, setId: set, setSeq: seq, sheetIndex: n });
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: ['rosecut', 'labels', 'cmd:leaveset'], flippedAt: T0 - 3600e3 }); lib._driveForget();
    F.seed('Charm_Nest_Runs/' + R, { runId: R, releasePolicy: 2, status: 'running', step: 'nest', mode: 'auto', day: DAY, solidIncluded: {}, lines: {}, sheets: {}, holds: {}, errors: [], orders: [] });
    F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 600e3 });
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
    const set = (id, seq, ids, metals) => F.seed('Charm_Nest_Sets/' + id, { setId: id, runId: R, day: DAY, seq, name: `Set-${seq}`, folder: `charmnest/sets/${DAY}/Set-${seq}`, group: 'dispatch', status: 'nesting',
      formedAt: T0 - 3600e3, sheetIds: ids, materials: metals, orders: {}, sheetNos: {}, labelFiles: [] });
    set(SET1, 1, ['gf1-g3', 'ss1-g3', 'gf3-g3', 'gf6-g3'], ['gold', 'silver']);
    set(SET2, 2, ['gf5-g3', 'ss5-g3'], ['gold', 'silver']);
    sheet('gf1-g3', 'gold', '4253000001', inSetRec(SET1, 1, 1));
    sheet('ss1-g3', 'silver', '4253000002', inSetRec(SET1, 1, 1));
    sheet('gf3-g3', 'gold', '4253000003', inSetRec(SET1, 1, 2));
    sheet('gf6-g3', 'gold', '4253000006', Object.assign(inSetRec(SET1, 1, 3), { ownerSet: { setId: SET1, by: 'Paul', at: T0 - 7200e3 } }));   // (the owner put it in Set 1)
    sheet('gf5-g3', 'gold', '4253000005', inSetRec(SET2, 2, 1));
    sheet('ss5-g3', 'silver', '4253000015', inSetRec(SET2, 2, 1));
    // the drafts: each taken out of Set 1 before (leftSet), so the cloud's look never puts them in a set by itself: only a person's move does
    sheet('gf2-g3', 'gold', '4253000020', { leftSet: SET1 });
    sheet('gf7-g3', 'gold', '4253000027', { leftSet: SET1 });
    sheet('gf8-g3', 'gold', '4253000028', { leftSet: SET1 });

    // the cloud: the REAL ask unit, one pass each time the page waits for its answer
    let pass = 0;
    const cloudPass = async () => { pass++; const ctx = { db: F.db, run: Object.assign({}, F.doc('Charm_Nest_Runs/' + R), { runId: R }), runId: R, budgetEnd: Date.now() + 600000, cloudSteps: ['rosecut'], say: () => {},
      call: (b, u) => lib.call(b, { unit: `g3|${u}|pass${pass}` }), read: b => lib.call(b, {}) }; return Green.askUnit(ctx); };

    // the page: viewer + set-edit + flow in a vm (window = self), its api the offline library
    const sent = [];
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout, clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, Date, JSON, Math, Map, Set, URL, TextEncoder, require: p => require(path.join(ROOT, p)) };
    w.window = w; w.self = w; w.globalThis = w;
    w.toast = () => {}; w.B = { run: null, employee: 'Michelle' }; w.CNEmployee = { name: () => 'Michelle' };
    w.CharmNestRunFollow = { state: () => ({ runId: R }), dev: () => PAGE, now: async () => ({}) };
    w.CN = { api: async (fn, body) => { sent.push(JSON.parse(JSON.stringify(body))); return P(Object.assign({ cv: CV, dev: PAGE }, body)); }, toast: () => {} };
    const ctxv = vm.createContext(w);
    for (const f of ['charm-nest-viewer.js', 'charm-nest-set-edit.js', 'charm-nest-flow.js']) vm.runInContext(fs.readFileSync(path.join(ROOT, f), 'utf8'), ctxv, { filename: f });
    await w.CNViewer.ready();
    let waits = 0;
    const LF = w.LibraryFlow;
    LF.configure({ employee: () => 'Michelle', rows: () => [], live: () => null, wait: async () => { if (++waits > 8) throw new Error('the cloud never answered'); await cloudPass(); } });
    const rec = id => F.doc('Charm_Nest_Sheets/' + id), listed = (s, id) => (F.doc('Charm_Nest_Sets/' + s).sheetIds || []).includes(id);
    const asksOf = id => sent.filter(b => b.op === 'roseCutAsk' && b.sheetId === id);
    const move = async (id, to) => { const plan = await LF.plan({ kind: 'sheet', id, to, by: 'Michelle' }); waits = 0; const res = await LF.commit(plan, { by: 'Michelle', confirmed: (plan.confirm || []).map(c => c.key) }); return { plan, res, lines: (res.applied || []).map(a => a.label) }; };

    out.push(['S0 the page is a viewer by its own read of the switch: mode "on", "rosecut" a cloud step, no run held here', w.CNViewer.on() === true && w.CharmNestCloudLanes('rosecut', { runId: R }) === true, w.CNViewer.state()]);

    // S1 a draft into an open set
    let x = await move('gf2-g3', { set: SET2 });
    out.push(['S1 a draft GF sheet dropped on Set 2: no "open on another screen", the plan is ONE ask of the cloud (include, Set 2)',
      !x.plan.needs.some(n => n.key === 'runElsewhere') && JSON.stringify(x.plan.steps.map(s => [s.type, s.kind, s.setId])) === JSON.stringify([['cloudSet', 'include', SET2]]), { needs: x.plan.needs, steps: x.plan.steps }]);
    out.push(['S2 the cloud put it in Set 2 by the set rules (record and set list); the press reached the cloud in Michelle\'s name, from the Library',
      x.res.ok === true && rec('gf2-g3').setId === SET2 && rec('gf2-g3').draft === false && !rec('gf2-g3').leftSet && listed(SET2, 'gf2-g3') && asksOf('gf2-g3').length === 1 && asksOf('gf2-g3')[0].kind === 'include' && asksOf('gf2-g3')[0].setId === SET2 && asksOf('gf2-g3')[0].by === 'Michelle' && F.doc('Charm_Nest_Cut_Asks/gf2-g3').via === 'library',
      { res: x.res, rec: { s: rec('gf2-g3').setId, d: rec('gf2-g3').draft, l: rec('gf2-g3').leftSet }, ask: F.doc('Charm_Nest_Cut_Asks/gf2-g3') }]);
    out.push(['S3 the person sees the cloud\'s answer in words ("... is in Set 2 now")', x.lines.some(l => /^The cloud: .*is in Set 2 now/.test(l)) && !(x.res.warnings || []).length, { lines: x.lines, warnings: x.res.warnings }]);

    // S4 out of an open set (drop on In progress)
    x = await move('gf2-g3', { area: 'progress' });
    out.push(['S4 taken out of Set 2 (drop on In progress): one exclude ask with the person\'s yes asked first; the cloud takes it out: a draft, marked taken out, Set 2 no longer lists it; its words said',
      JSON.stringify(x.plan.steps.map(s => [s.type, s.kind])) === JSON.stringify([['cloudSet', 'exclude']]) && x.plan.confirm.some(c => c.key === 'leaveSet') && x.res.ok === true && rec('gf2-g3').draft === true && rec('gf2-g3').leftSet === SET2 && !listed(SET2, 'gf2-g3') && x.lines.some(l => /^The cloud: .*taken out of Set 2/.test(l)),
      { plan: { needs: x.plan.needs, steps: x.plan.steps }, res: x.res, rec: { s: rec('gf2-g3').setId, d: rec('gf2-g3').draft, l: rec('gf2-g3').leftSet } }]);

    // S5 between sets
    x = await move('gf3-g3', { set: SET2 });
    out.push(['S5 a GF sheet of Set 1 dropped on Set 2: one include ask naming Set 2; the cloud takes it out of Set 1 and puts it in Set 2, and says both',
      JSON.stringify(x.plan.steps.map(s => [s.type, s.kind, s.setId])) === JSON.stringify([['cloudSet', 'include', SET2]]) && x.res.ok === true && rec('gf3-g3').setId === SET2 && rec('gf3-g3').draft === false && listed(SET2, 'gf3-g3') && !listed(SET1, 'gf3-g3') && x.lines.some(l => /^The cloud: .*Set 2.*taken out of Set 1/.test(l)),
      { plan: { needs: x.plan.needs, steps: x.plan.steps }, res: x.res, lines: x.lines, rec: { s: rec('gf3-g3').setId, d: rec('gf3-g3').draft } }]);

    // S6 the cloud refuses: never "done" when nothing happened
    x = await move('gf6-g3', { area: 'progress' });
    out.push(['S6 a take-out the cloud refuses (the owner put the sheet in Set 1): the move fails with the cloud\'s own words, and nothing changed',
      x.res.ok === false && /The cloud did not do it: .*owner/.test(String(x.res.error)) && rec('gf6-g3').setId === SET1 && rec('gf6-g3').draft === false && listed(SET1, 'gf6-g3'), { res: x.res }]);

    // S7 mode off: the old plan, word for word
    LF.configure({ cloud: () => false });
    let pl = await LF.plan({ kind: 'sheet', id: 'gf7-g3', to: { set: SET2 }, by: 'Michelle' });
    out.push(['S7 mode off (no cloud carrying): the plan is as before ("Its run is open on another screen"), no ask', pl.needs[0] && pl.needs[0].key === 'runElsewhere' && pl.needs[0].label === 'Its run is open on another screen' && !pl.steps.some(s => s.type === 'cloudSet'), pl.needs]);
    LF.configure({ cloud: null, cloudLane: () => false });
    const before = sent.length;
    pl = await LF.plan({ kind: 'sheet', id: 'gf7-g3', to: { set: SET2 }, by: 'Michelle' });
    const r8 = await LF.commit(pl, { by: 'Michelle', confirmed: [] });
    out.push(['S8 the cloud\'s set lane off: the plan says so in words, nothing is asked, nothing changes', pl.needs[0] && pl.needs[0].key === 'cloudLane' && r8.ok === false && /does not take set moves yet/.test(r8.error) && !sent.slice(before).some(b => b.op === 'roseCutAsk') && rec('gf7-g3').draft === true, { needs: pl.needs, r8 }]);
    LF.configure({ cloudLane: null });

    // S9 two drafts combined (one dropped on the other): both asked of the cloud
    pl = await LF.plan({ kind: 'sheet', id: 'gf7-g3', to: { sheet: 'gf8-g3' }, by: 'Michelle' });
    out.push(['S9 a draft dropped on another draft: no refusal; both are asked of the cloud (two include asks)', pl.needs.length === 0 && JSON.stringify(pl.steps.map(s => [s.type, s.kind, s.sheetId])) === JSON.stringify([['cloudSet', 'include', 'gf7-g3'], ['cloudSet', 'include', 'gf8-g3']]), { needs: pl.needs, steps: pl.steps }]);

    // S10 the combine place (sheetZone) on a viewer: offered from the records; mode off: as before
    const recs = { a: { id: 'a', metal: 'gold', draft: true, releaseFull: true }, b: { id: 'b', metal: 'gold', draft: true, releaseFull: true } };
    const zOn = LF.core.sheetZone({ kind: 'sheet', id: 'a' }, 'b', { live: () => null, rec: id => recs[id], cloud: () => true }, { self: 'GF Sheet A', name: 'GF Sheet B' });
    const zOff = LF.core.sheetZone({ kind: 'sheet', id: 'a' }, 'b', { live: () => null, rec: id => recs[id], cloud: () => false }, { self: 'GF Sheet A', name: 'GF Sheet B' });
    out.push(['S10 combining two drafts on a viewer is offered (the cloud puts both in a set); without the cloud it is refused as before', zOn && zOn.ok === true && zOff && zOff.ok === false && /Only sheets of the open run can be combined/.test(zOff.reason), { zOn, zOff }]);

    // S11 the server's words for a page that still writes the membership of a set the cloud is making
    const fa = await P({ op: 'flowApply', steps: [{ type: 'setMember', moves: [{ sheetId: 'gf1-g3', to: null }] }], by: 'Michelle', device: 'charm-nest-1', via: 'Library move', cv: CV, dev: PAGE });
    out.push(['S11 the server, mode on: a page\'s direct write to a set the cloud makes is refused with the cloud\'s words, never "open on another screen"; nothing changed',
      fa.http === 409 && /being made by the cloud/.test(String(fa.error)) && !/another screen/.test(String(fa.error)) && rec('gf1-g3').setId === SET1 && listed(SET1, 'gf1-g3'), fa]);
  } catch (e) { out.push(['S run', false, String(e && e.stack || e).split('\n').slice(0, 6).join(' | ')]); }
  finally { console.log = quiet; console.warn = qw; console.error = qe; }
  for (const [n, c, d] of out) ok(n, c, d);
}

function partB() {
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('B1 charm-nest-1.html loads the changed flow and set-edit under new tokens', /charm-nest-flow\.js\?v=[^"]*-g3(?:-\w+)*"/.test(html) && /charm-nest-set-edit\.js\?v=[^"]*-g3(?:-\w+)*"/.test(html));
}

(async () => {
  await partS();
  partB();
  log0(failed ? `FAIL viewer-g3 (${failed})` : 'PASS viewer-g3');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-g3: ' + (e && e.stack || e)); process.exit(1); });
