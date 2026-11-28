// S4 (Paul, 21 Nov 2026): four presses a person makes on a staff computer (a viewer) in cloud mode "on" were blocked or silent:
//   1  Stop on a sheet the cloud is nesting: the card never showed Stop (only a local "nesting" did) and Stop sent nothing. Now the card shows "Nesting in the cloud…" with Stop while the
//      cloud's nest job runs (CNViewer.nestOf: the Nest press's jobId, the record's nestJob, getJob), Stop sends stopJob { id, for: "person" }, Nest is not pressable twice.
//   2  Custom Send to Sheet refused when the cloud's beat was late ("not open right now"): in mode on it is put in the cloud regardless (as CNRelayAsk); with no run open, true words.
//   3  Merge sheets hidden on viewers: offered for the followed run's sheets and sent to the cloud (consolidate / renest).
//   4  artSheetWrite's putSheet { filesOwed: null } after the cloud took the files: refused every time, silently. Not sent when the cloud writes the files (its writer clears it).
//      (CESAVE, 23 Nov 2026: artSheetWrite is gone; after a Charm Editor save the page writes nothing: artPut marks the sheet and starts the cloud's writer, which seats the cut line and clears the marks)
//   V  charm-nest-viewer.js, the real file in a bare context (fake api and timers)       A  askCarrier, the real source      M  s4Merge, the real source
//   F  the art-edit host's changed(), the real source        C  the cloud (offline, _cloudFakes.cjs): the ops these presses send are taken from a viewer in mode on; the writer clears filesOwed itself
//   H  charm-nest-1.html: the card's S4 lines, stopNest's route, the inline scripts parse, the ?v= tokens
// Offline only: no Etsy, no live write. Made-up names.   NODE_PATH=<deps> node tests/charm-nest/viewer-s4.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
const log0 = console.log.bind(console);
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1500))); if (!cond) failed++; };
const BRIDGE = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8'), HTML = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
const VIEWER = fs.readFileSync(path.join(ROOT, 'charm-nest-viewer.js'), 'utf8');
const tick = () => new Promise(r => setImmediate(r));
const settle = async (n = 20) => { for (let i = 0; i < n; i++) await tick(); };

/* ═══ V · the viewer: the cloud's nest of a sheet, seen, refused twice, stopped ═══ */
function viewerWorld(mode, answer) {
  const calls = [], toasts = [], painted = [], timers = [];
  const w = { console: { log() {}, warn() {}, info() {}, error() {} }, Promise, Date, JSON, Map, Set, Object, Array, String, Number, Math, Error };
  w.globalThis = w; w.window = w;
  w.setTimeout = (f, ms) => { const t = { f, ms, unref() {} }; timers.push(t); return t; };
  w.clearTimeout = t => { const i = timers.indexOf(t); if (i >= 0) timers.splice(i, 1); };
  w.setInterval = () => ({ unref() {} });
  w.performance = { now: () => 120000 };
  w.document = { hidden: false, getElementById: () => null };
  w.location = { reload() {} };
  w.toast = (t, tone) => toasts.push({ t, tone });
  w.employeeName = () => 'Michelle';
  w.CharmNestRunFollow = { state: () => ({ runId: 'run-s4' }), dev: () => 'd0michelle1', now() {} };
  const sheets = [];
  w.CN = { api: async (fn, body) => { calls.push(body); if (body.op === 'driveGet') return { ok: true, mode, drive: { mode, cloudSteps: ['nest', 'files', 'cmd:consolidate', 'cmd:renest', 'cmd:customsend'] } }; return answer(body); },
    allSheets: () => sheets, renderCard: sh => painted.push(sh.sheetId) };
  vm.runInContext(VIEWER, vm.createContext(w));
  const run = async (maxMs = 6000) => { for (let k = 0; k < 4; k++) { const due = timers.splice(0).filter(t => (t.ms || 0) <= maxMs), later = []; for (const t of due) { try { t.f(); } catch (_) {} } void later; await settle(); } };
  return { w, V: w.CNViewer, calls, toasts, painted, sheets, run };
}
async function partV() {
  let job = { status: 'queued' };
  const X = viewerWorld('on', body => {
    if (body.op === 'nestStart') return { ok: true, jobId: 'nest-aaa111', created: true, status: 'queued', attempt: 1 };
    if (body.op === 'stopJob') return { ok: true };
    if (body.op === 'getJob') return { job: body.id === 'nest-aaa111' ? Object.assign({ id: body.id }, job) : null };
    return { ok: true };
  });
  await X.run(400); await settle();
  ok('V0 the switch reads "on": the page is a viewer', X.V.on() === true, X.V.state());
  const sh = { sheetId: 'sheet-gf-1', fileBase: 'GF Draft 1', metal: 'gold', runId: 'run-s4' }; X.sheets.push(sh);
  ok('V1 before any press, no cloud nest is shown for the sheet', X.V.nestOf(sh) === null);
  const p1 = X.V.nest(sh, true);
  ok('V1 while the Nest press is being asked, the card already reads "asking" (Nest cannot be pressed again meanwhile)', !!X.V.nestOf(sh) && X.V.nestOf(sh).asking === true);
  const r1 = await p1;
  const ns = X.calls.filter(c => c.op === 'nestStart');
  ok('V1 Nest sends nestStart once and the card knows the cloud\'s job', r1 === true && ns.length === 1 && (X.V.nestOf(sh) || {}).jobId === 'nest-aaa111', { r1, ns, n: X.V.nestOf(sh) });
  ok('V1 the person is told it nests in the cloud, and the card was drawn again', X.toasts.some(t => /is nesting in the cloud/.test(t.t)) && X.painted.includes('sheet-gf-1'), X.toasts);
  const r2 = await X.V.nest(sh, true);
  ok('V2 a second Nest while it runs is not sent: said in words', r2 === false && X.calls.filter(c => c.op === 'nestStart').length === 1 && X.toasts.some(t => /already nesting in the cloud: press Stop/.test(t.t)), X.toasts);
  const r2b = await X.V.nest(sh, false);
  ok('V2 Nest all passes a sheet already nesting by, in silence', r2b === false && X.calls.filter(c => c.op === 'nestStart').length === 1);
  job = { status: 'running', stage: 'searching' };
  await X.run(); await settle();
  ok('V3 the job is read (getJob) while it runs; the card still shows it', X.calls.some(c => c.op === 'getJob' && c.id === 'nest-aaa111') && (X.V.nestOf(sh) || {}).status === 'running');
  const s1 = await X.V.stopNest(sh);
  const sj = X.calls.filter(c => c.op === 'stopJob');
  ok('V4 Stop sends stopJob for THAT job, as a person\'s stop (for: person, by: the person)', s1 === true && sj.length === 1 && sj[0].id === 'nest-aaa111' && sj[0].for === 'person' && sj[0].by === 'Michelle', sj);
  ok('V4 until the job reads stopped the card says "Stopping" (not done), and the person is told the Stop was sent', !!(X.V.nestOf(sh) || {}).stopping && X.toasts.some(t => /^Stop sent: the cloud stops nesting/.test(t.t)), { n: X.V.nestOf(sh), toasts: X.toasts.slice(-2) });
  await X.V.stopNest(sh);
  ok('V4 a second Stop while it stops sends nothing more', X.calls.filter(c => c.op === 'stopJob').length === 1);
  job = { status: 'stopped', stopBy: 'Michelle', stopFor: 'person' };
  await X.run(); await settle();
  ok('V5 the job reads stopped: the card no longer shows the cloud nest, and the person is told it stopped', X.V.nestOf(sh) === null && X.toasts.some(t => /the cloud stopped its nest \(Stop pressed by Michelle\)/.test(t.t)), X.toasts.slice(-2));
  // the cloud's own nest of a sheet (its driver asked it): learned from the record the change feed read
  const sh2 = { sheetId: 'sheet-ss-2', fileBase: 'SS Draft 2', metal: 'silver', runId: 'run-s4' }; X.sheets.push(sh2);
  X.V.sawSheet(sh2, { id: 'sheet-ss-2', nestJob: { id: 'nest-bbb222', status: 'queued', at: Date.now(), attempt: 1 } });
  ok('V6 a nest the cloud started by itself (the record\'s nestJob) shows on the card with Stop', (X.V.nestOf(sh2) || {}).jobId === 'nest-bbb222' && X.painted.includes('sheet-ss-2'));
  X.V.sawSheet(sh2, { id: 'sheet-ss-2', nestJob: { id: 'nest-bbb222', status: 'nested', at: Date.now(), attempt: 1 } });
  ok('V6 the record says it ended: the card shows no cloud nest', X.V.nestOf(sh2) === null);
  const sh3 = { sheetId: 'sheet-old', metal: 'gold', recalled: { nestJob: { id: 'nest-ccc', status: 'queued', at: Date.now() - 20 * 60000 } } };
  ok('V6 a record\'s mark older than a nest\'s lease, never confirmed by its job, is not shown as running', X.V.nestOf(sh3) === null);
  const shNo = { metal: 'gold' }; const rNo = await X.V.nest(shNo, true);
  ok('V7 a sheet not saved in the cloud: refused in words, nothing sent', rNo === false && X.toasts.some(t => /not saved in the cloud yet/.test(t.t)));
  // mode off: nothing of this runs
  const O = viewerWorld('off', () => ({ ok: true }));
  await O.run(400); await settle();
  const shO = { sheetId: 'sheet-x', metal: 'gold', recalled: { nestJob: { id: 'nest-x', status: 'queued', at: Date.now() } } };
  O.V.sawSheet(shO, { id: 'sheet-x', nestJob: { id: 'nest-x', status: 'queued', at: Date.now() } });
  ok('V8 mode off: no cloud nest is ever shown (the card and Stop are as before)', O.V.on() === false && O.V.nestOf(shO) === null && !O.calls.some(c => c.op === 'getJob' || c.op === 'stopJob'));
}

/* ═══ A · Send to Sheet's askCarrier (the real source) ═══ */
function carrierWorld(o) {
  const a = BRIDGE.indexOf('const carrierAsks=new Map();'), b = BRIDGE.indexOf('/* REMOTESEND · applyRemote(ck', a);
  const src = BRIDGE.slice(a, b);
  const calls = [], cmds = {};
  const w = { console, Promise, Date, JSON, Map, Object, String, Error, setTimeout: f => setTimeout(f, 0) };
  w.window = w; w.B = { run: null }; w.employeeName = () => 'Michelle'; w.ver = 0; w.Session = { schedule() {} };
  w.CNViewer = { on: () => !!o.on };
  w.CharmNestRunFollow = { state: () => ({ runId: o.runId === undefined ? 'run-s4' : o.runId }), dev: () => 'd0michelle1' };
  w.decisionLines = () => ({ '4250000001_42500000011': [] });
  w.api = async (fn, body) => {
    calls.push(body);
    if (body.op === 'runCarrier') { if (o.carrierThrows) throw new Error('timeout'); return { ok: true, mine: false, alive: !!o.alive, carrier: o.on ? { dev: 'cloud', by: 'Cloud', at: 0 } : { dev: 'd0paulpc01', by: 'Paul', at: 0 } }; }
    if (body.op === 'runCmdPut') { cmds.c1 = { id: 'c1', state: 'waiting', at: Date.now(), dev: body.dev, type: body.cmd.type, args: { ck: body.cmd.ck } }; return { ok: true, id: 'c1' }; }
    if (body.op === 'runCmdList') { if (cmds.c1 && o.answer && (o.after == null || calls.filter(c => c.op === 'runCmdList').length > o.after)) Object.assign(cmds.c1, o.answer); return { cmds: Object.values(cmds) }; }
    return { ok: true };
  };
  const ctx = vm.createContext(w);
  vm.runInContext(`const viewerSend = () => { try { return !B.run && !!(window.CNViewer && typeof CNViewer.on === "function" && CNViewer.on()); } catch (_) { return false; } };\nconst all = () => ({});\n${src}\nthis.askCarrier = askCarrier;`, ctx);
  return { calls, w };
}
async function partA() {
  const run = async o => { const X = carrierWorld(o), steps = []; let out = null, err = null; try { out = await X.w.askCarrier({ ck: 'card:4250000001_42500000011', rid: '4250000001' }, t => steps.push(t)); } catch (e) { err = e.message; } return { out, err, steps, put: X.calls.find(c => c.op === 'runCmdPut'), calls: X.calls }; };
  let r = await run({ on: true, alive: false, after: 2, answer: { state: 'done', text: 'its new piece is on GF Draft 1' } });
  ok('A1 mode on, the cloud\'s beat late (not alive): the send is still put in the cloud (customsend) and its answer shown', !r.err && !!r.put && r.put.cmd.type === 'customsend' && r.out && r.out.ok === true && /GF Draft 1/.test(r.out.text), r);
  ok('A1 the words name the cloud, never a computer', r.steps.length > 0 && r.steps.every(t => /the cloud/i.test(t) && !/computer/.test(t)), r.steps);
  r = await run({ on: true, alive: false, carrierThrows: true, after: 1, answer: { state: 'done', text: 'its new piece is on SS Draft 1' } });
  ok('A2 mode on, the carrier could not even be read: still put in the cloud and answered', !r.err && !!r.put && r.out && r.out.ok === true, r);
  r = await run({ on: true, runId: null });
  ok('A3 mode on, no run open: nothing is put, and the person is told what to do in true words', !r.put && /No run is open in the cloud/.test(r.err || '') && /Start the run/.test(r.err || '') && !/another computer/.test(r.err || ''), r);
  r = await run({ on: false, alive: false });
  ok('A4 mode off, no carrier open: as before (not put; "not open right now")', !r.put && /not open right now/.test(r.err || ''), r);
  r = await run({ on: false, runId: null });
  ok('A4 mode off, no run: as before ("No run is open on another computer")', !r.put && /No run is open on another computer/.test(r.err || ''), r);
  const send = BRIDGE.slice(BRIDGE.indexOf('const receipt=await saveCloud(e,"pending")'), BRIDGE.indexOf('const touring=!how.recover && how.tour!==false && tourOk()'));
  ok('A5 Send to Sheet on a viewer with no run open places nothing here and says what to do (no local placement, no "sent")', /if\(viewerSend\(\) && !window\.CharmNestRunFollow\?\.state\?\.\(\)\.runId\)\{e\.sendError="No run is open in the cloud\.";if\(!how\.recover\)toast\(`Order \$\{e\.rid\}: not on a sheet\. No run is open in the cloud[^`]*Start the run[^`]*`,"warn",14000\);return null;\}/.test(send), send.slice(-700));
}

/* ═══ M · Merge sheets on a viewer (the real s4Merge / s4Words) ═══ */
async function partM() {
  const a = BRIDGE.indexOf('/* S4-MERGE-BEGIN'), b = BRIDGE.indexOf('/* S4-MERGE-END */');
  ok('M0 the viewer\'s merge is in the bridge', a > 0 && b > a);
  const src = BRIDGE.slice(a, b);
  const world = answers => {
    const sent = [], notes = [], paints = [];
    const w = { console, Promise, Date, JSON, Map, Set, Object, String, setTimeout: () => 0 };
    w.window = w;
    w.charmsWord = n => `${n} charm${n === 1 ? '' : 's'}`; w.pw = p => p.name; w.sheetsWord = l => l.map(p => p.name).join(' and ');
    w.closeOptions = () => {}; w.repaintMerge = () => paints.push(1); w.mergeNote = (m, d) => notes.push(d);
    w.CNRelayAsk = async (cmd, onStep, maxMs, o) => { sent.push({ cmd, o }); onStep('the cloud: Moving pieces…'); return answers(cmd, sent.length); };
    vm.runInContext(src + '\nthis.s4Merge = s4Merge; this.s4Words = s4Words;', vm.createContext(w));
    return { w, sent, notes, paints };
  };
  const T = { name: 'Sheet 1', sheetId: 'sh-1', charms: [1, 2] }, S2 = { name: 'Sheet 2', sheetId: 'sh-2', charms: [3] }, S3 = { name: 'Sheet 3', sheetId: 'sh-3', charms: [4, 5] };
  const plan = { target: T, sources: [S2, S3], pages: [T, S2, S3], moving: 3, total: 5 };
  let X = world(cmd => ({ ok: true, text: `${cmd.sourceId || cmd.sheetId} done by the cloud` })), st = {};
  let r = await X.w.s4Merge('gold14k', 'move', plan, st);
  ok('M1 "Move all onto Sheet 1": one consolidate per later sheet onto Sheet 1, for 14K, always put in the cloud', r === true && X.sent.length === 2 && X.sent.every(s => s.cmd.type === 'consolidate' && s.cmd.targetId === 'sh-1' && s.cmd.metal === 'gold14k' && s.o && s.o.always === true) && X.sent.map(s => s.cmd.sourceId).join() === 'sh-2,sh-3', X.sent);
  ok('M1 what the cloud answered is said once, after it answered; nothing is left busy', st.busy === null && st.done && !st.done.bad && /done by the cloud/.test(st.done.text) && X.notes.length === 1, st);
  X = world(cmd => ({ ok: true, text: `${cmd.sheetId} nested again` })); st = {};
  r = await X.w.s4Merge('gold10k', 'renest', plan, st);
  ok('M2 "Re-nest": one renest per sheet, in order', r === true && X.sent.map(s => s.cmd.type + ':' + s.cmd.sheetId).join() === 'renest:sh-1,renest:sh-2,renest:sh-3', X.sent);
  X = world((cmd, n) => n === 1 ? { ok: false, text: 'That sheet is being nested right now.' } : { ok: true }); st = {};
  r = await X.w.s4Merge('gold14k', 'move', plan, st);
  ok('M3 a refusal is said in the cloud\'s words, the rest is not asked, never "done"', r === false && X.sent.length === 1 && st.done.bad === true && /being nested right now/.test(st.done.text) && /The rest was not asked/.test(st.done.text), st);
  X = world(() => ({ ok: false, queued: true, cloud: true, text: 'The cloud has not started it yet: it does it at its next pass (every minute or so). Nothing more is needed here.' })); st = {};
  r = await X.w.s4Merge('gold14k', 'move', { target: T, sources: [S2], pages: [T, S2], moving: 1, total: 3 }, st);
  ok('M4 not started yet: said as sent (not done), the cloud does it at its next pass', r === true && !st.done.bad && /sent to the cloud/.test(st.done.text) && !/done by the cloud/.test(st.done.text), st);
  ok('M5 the ask names what the cloud does', /The cloud moves 3 charms from Sheet 2 and Sheet 3 into Sheet 1's free room/.test(X.w.s4Words(plan, 'move')) && /The cloud nests Sheet 1 and Sheet 2 and Sheet 3 again from scratch/.test(X.w.s4Words(plan, 'renest')));
  const gate = BRIDGE.slice(BRIDGE.indexOf('const MERGE = {};'), BRIDGE.indexOf('function paintMerge(sh, node)') + 3000);
  ok('M6 the followed run\'s sheets are mergeable on a viewer (s4Sheet), a cloud nest counts as busy, the press goes to the cloud, the ask uses the cloud\'s words',
    /const s4Run = \(\) => \{ try \{ return !B\.run && window\.CNViewer && typeof CNViewer\.on === "function" && CNViewer\.on\(\) \? \(window\.CharmNestRunFollow\?\.state\?\.\(\)\.runId \|\| null\)/.test(gate)
    && /const mergeable = p => solid\(p\.metal\) && p\.charms\.length > 0 && \(modern\(p\.runId\) \|\| s4Sheet\(p\)\)/.test(gate) && /CNViewer\.nestOf\(p\)\);   \/\/ \(S4/.test(gate)
    && /if \(!spec && s4Run\(\)\) return s4Merge\(m, kind, plan, st\);/.test(gate) && /s4Run\(\) \? s4Words\(plan, st\.ask\) : mergeWords\(plan, st\.ask\); q\("go"\)/.test(gate));
}

/* ═══ F · after a Charm Editor save the page writes no sheet (the art-edit host, the real source) ═══
   (CESAVE, Paul 23 Nov 2026: "those changes should be reflective on the original charm on the original sheet": artPut itself marks the sheet (filesOwed, artOwed, cutCheck) and starts the cloud's
    writer, which seats a changed cut line and clears the marks (charmNestLibrary.js op_artPut, _charmNestFinish.js). The page's artSheetWrite, with its putSheet { filesOwed: null, cutCheck: null },
    is gone: the art-edit host's changed() only fits the engraving again, says it and draws the card, in every mode) */
async function partF() {
  const a = BRIDGE.indexOf('const SAVED_CHARMS = window.CharmNestSavedCharms = new Map();'), b = BRIDGE.indexOf('\n/* ── THUMBSYNC', a);
  ok('F0 the art-edit host is in the bridge, and artSheetWrite is gone from it (CESAVE)', a > 0 && b > a && !/artSheetWrite|ART_SHEET_WRITES/.test(BRIDGE), { a, b });
  const run = async o => {
    const calls = [], logs = [], cards = [], writes = [];
    const sheet = { sheetId: 'sh-art', metal: 'gold', fileBase: 'GF Sheet 3', outputs: { ai: { url: 'x' } } };
    const w = { console: { warn() {}, log() {} }, Promise, Date, JSON, Map, WeakMap, Object, Set };
    w.window = w; w.allSheets = () => [sheet]; w.B = { pool: { rows: new Map() } }; w.Session = { schedule() {} }; w.employeeName = () => 'Michelle';
    w.api = async (fn, body) => { calls.push(body); return { ok: true }; };
    w.agent = (...x) => logs.push(x.slice(2).join(' ')); w.renderCard = sh => cards.push(sh.sheetId);
    w.writeAsPlaced = async sh => { writes.push(sh.sheetId); return true; };
    w.Engrave = { artChanged: () => 1 }; w.Pool = { sheetOf: () => (o.onSheet ? sheet : null), charmOf: () => null };
    w.CNViewer = o.mode ? { mode: () => o.mode, on: () => o.mode === 'on' } : undefined;
    let host = null; w.CharmNestArtEdit = { setHost: h => { host = h; } };
    vm.runInContext(BRIDGE.slice(a, b), vm.createContext(w));
    await host.changed('4250000001_42500000011_1', 'front', { charm: {}, record: { by: 'Michelle', at: 1 }, sheetId: o.onSheet ? 'sh-art' : null, filesOwed: o.onSheet ? 5 : null, cutChanged: !!o.cut, cut: !!o.cut });
    await settle();
    return { calls, writes, cards, logs };
  };
  for (const [name, o] of [['F1 a viewer in mode on', { mode: 'on', onSheet: true }], ['F2 standby', { mode: 'standby', onSheet: true }], ['F3 mode on, its cut line changed', { mode: 'on', onSheet: true, cut: true }], ['F4 mode off', { mode: 'off', onSheet: true, cut: true }], ['F4 no viewer module at all', { onSheet: true }]]) {
    const r = await run(o);
    ok(`${name}: the page sends no putSheet (no filesOwed, no cutCheck) and writes no files itself; the card is drawn again and the log says the cloud writes the sheet`, !r.calls.some(c => c && c.op === 'putSheet') && !r.writes.length && r.cards.join() === 'sh-art' && r.logs.some(t => /the cloud writes GF Sheet 3 again/.test(t)), r);
  }
  const r = await run({ mode: 'on', onSheet: false });
  ok('F5 a piece on no sheet: nothing is sent, no card drawn', !r.calls.length && !r.writes.length && !r.cards.length, r);
}

/* ═══ C · the cloud takes these presses from a viewer in mode on; its writer clears filesOwed ═══ */
async function partC() {
  const T0 = Date.UTC(2026, 10, 21, 17, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = [console.log, console.warn]; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const CV = 'cloud14-20261120', DEV = 'd0michelle1', R = 'run-s4-cloud';
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: ['nest', 'files', 'cmd:consolidate', 'cmd:renest', 'cmd:customsend'], flippedAt: T0 - 3600e3 }); lib._driveForget();
    F.seed('Charm_Nest_Jobs/nest-s4job0001', { id: 'nest-s4job0001', kind: 'nest', cloud: true, status: 'running', state: 'running', sheetId: 'sh-s4', runId: R, metal: 'gold', createdMs: T0 - 60e3 });
    F.seed('Charm_Nest_Sheets/sh-s4', { id: 'sh-s4', runId: R, metal: 'gold14k', status: 'complete', layoutRev: 2, contentRev: 2, filesOwed: T0 - 5000, cutCheck: { p1: { at: T0 - 5000, by: 'Michelle' } }, nestJob: { id: 'nest-s4job0001', status: 'queued', at: T0 - 60e3 } });
    const g = await P({ op: 'getJob', id: 'nest-s4job0001', cv: CV, dev: DEV });
    ok('C1 a viewer reads the cloud\'s nest job (getJob, a read) in mode on', g.http === 200 && g.job && g.job.status === 'running', g);
    const s = await P({ op: 'stopJob', id: 'nest-s4job0001', for: 'person', by: 'Michelle', why: 'Stop pressed by Michelle', cv: CV, dev: DEV });
    const j = F.doc('Charm_Nest_Jobs/nest-s4job0001') || {};
    ok('C1 a viewer\'s Stop (stopJob) is taken in mode on: the job is asked to stop, for a person (the cloud then waits for its Nest press)', s.http === 200 && s.ok === true && j.stopRequested === true && j.stopFor === 'person' && j.stopBy === 'Michelle', { s, j });
    const old = await P({ op: 'putSheet', sheet: { id: 'sh-s4', filesOwed: null, cutCheck: null }, cv: CV, dev: DEV });
    ok('C2 (why the page no longer sends it) putSheet { filesOwed: null } from a viewer is refused in mode on', old.http === 409 && old.cloudCarries === true && (old.fields || []).includes('filesOwed') && (F.doc('Charm_Nest_Sheets/sh-s4') || {}).filesOwed === T0 - 5000, old);
    const cc = await P({ op: 'putSheet', sheet: { id: 'sh-s4', cutCheck: null }, cv: CV, dev: DEV });
    const rec = F.doc('Charm_Nest_Sheets/sh-s4') || {};
    // (CESAVE, Paul 23 Nov 2026: a changed cut line is seated by the cloud's writer, which clears cutCheck itself (_charmNestFinishOps.js finishSeat): the page no longer sends it, and a viewer's is refused with the rest)
    ok('C2 the cut lines\' check alone (putSheet { cutCheck: null }) from a viewer is refused in mode on too; cutCheck and filesOwed stay for the cloud\'s writer', cc.http === 409 && cc.cloudCarries === true && (cc.fields || []).includes('cutCheck') && rec.cutCheck && rec.cutCheck.p1 && rec.filesOwed === T0 - 5000, { cc, cutCheck: rec.cutCheck, filesOwed: rec.filesOwed });
    const fo = fs.readFileSync(path.join(ROOT, 'netlify/functions/_charmNestFinishOps.js'), 'utf8');
    ok('C2 the writer clears the cut lines\' check itself once it has seated them (finishSeat: compare-and-clear on each piece\'s mark)', /left = Object\.assign\(\{\}, s\.cutCheck\); for \(const \[k, at\] of Object\.entries\(checked\)\) if \(left\[k\] && num\(left\[k\]\.at\) === num\(at\)\) delete left\[k\];/.test(fo) && /cutCheck: Object\.keys\(left\)\.length \? left : null/.test(fo));
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0 - 600e3, since: T0 - 3600e3 } });
    for (const cmd of [{ type: 'consolidate', metal: 'gold14k', sourceId: 'sh-b', targetId: 'sh-a', label: 'Merge sheets: Sheet 2 onto Sheet 1' }, { type: 'renest', metal: 'gold14k', sheetId: 'sh-a', label: 'Re-nest Sheet 1' }, { type: 'customsend', ck: 'card:4250000001_42500000011', ids: ['4250000001_42500000011'] }]) {
      const p = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd, cv: CV });
      ok(`C3 a viewer's ${cmd.type} request is put in the cloud in mode on, whatever the cloud's beat`, p.http === 200 && !!p.id && p.cmd.type === cmd.type && (cmd.type !== 'consolidate' || (p.cmd.args.sourceId === 'sh-b' && p.cmd.args.targetId === 'sh-a')), p);
    }
    const fin = fs.readFileSync(path.join(ROOT, 'netlify/functions/_charmNestFinish.js'), 'utf8');
    ok('C4 the cloud\'s sheet writer clears filesOwed (and, CESAVE, artOwed) itself once the files are written (its final putSheet, compare-and-clear on the owed mark)',
      /op: "putSheet", sheet: \{ id: X\.sheetId, saving: false, filesOwed: null,(?: artOwed: null,)? filesJob: null \}, contentBase: job\.written\.contentRev, filesOwedBase: job\.owedAt/.test(fin));
  } finally { console.log = quiet[0]; console.warn = quiet[1]; }
}

/* ═══ H · the page ═══ */
function partH() {
  const scripts = [...HTML.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]).filter(s => s.trim() && !/^\s*\{/.test(s));
  let bad = null; for (const s of scripts) { try { new vm.Script(s); } catch (e) { if (!/import|export/.test(e.message)) { bad = e.message; break; } } }
  ok('H0 the page\'s inline scripts parse', !bad, bad);
  const rc = HTML.slice(HTML.indexOf('function renderCard(sh) {'), HTML.indexOf('function renderProgress(sh) {'));
  ok('H1 the card asks CNViewer.nestOf: "Nesting in the cloud…" / "Stopping in the cloud…", Nest hidden while it runs, Stop shown for the cloud\'s job',
    /const cnv = window\.CNViewer && typeof CNViewer\.nestOf === "function" \? CNViewer\.nestOf\(sh\) : null;/.test(rc) && /Nesting in the cloud…/.test(rc) && /Stopping in the cloud…/.test(rc)
    && /q\("nest"\)\.classList\.toggle\("hidden", \["nesting", "finishing", "queued"\]\.includes\(sh\.status\) \|\| !!cnv\)/.test(rc) && /q\("stop"\)\.classList\.toggle\("hidden", sh\.status !== "nesting" && !\(cnv && cnv\.jobId\)\)/.test(rc));
  // stopNest's route, run: a cloud nest goes to CNViewer.stopNest; a local one as before
  const line = HTML.slice(HTML.indexOf('function stopNest(sh) {'), HTML.indexOf('\n// One replacement gate for live updates'));
  const w = { window: {}, S: { settings: {} }, renderCard() {}, agent() {}, api: async () => ({}), called: [] };
  w.window.CNViewer = w.CNViewer = { nestOf: sh => sh.cloud ? { jobId: 'j1' } : null, stopNest: sh => w.called.push(['cloud', sh.sheetId]) };
  vm.runInContext(line + '\nthis.stopNest = stopNest;', vm.createContext(w));
  w.stopNest({ sheetId: 'c1', status: 'complete', cloud: true }); w.stopNest({ sheetId: 'c2', status: 'complete' });
  const loc = { sheetId: 'l1', status: 'nesting', pool: {}, nestFlow: 'learned', workers: [{ postMessage: m => w.called.push(['worker', m.type]) }], jobId: 'J' }; w.stopNest(loc);
  ok('H2 Stop on a viewer\'s card stops the cloud\'s job; a sheet nesting on this computer is stopped as before; a sheet with neither is left alone', JSON.stringify(w.called) === JSON.stringify([['cloud', 'c1'], ['worker', 'stop']]) && loc.pool.stopAsked === true, w.called);
  ok('H3 the changed files load under new tokens (-s4)', /charm-nest-viewer\.js\?v=[^"]*-s4(?:-\w+)*"/.test(HTML) && /charm-nest-bridge\.js\?v=[^"]*-s4(?:-\w+)*"/.test(HTML) && /charm-nest-cloud-sheets\.js\?v=[^"]*-s4(?:-\w+)*"/.test(HTML));
  const cs = fs.readFileSync(path.join(ROOT, 'charm-nest-cloud-sheets.js'), 'utf8');
  ok('H4 the change feed and a reload tell the viewer each record\'s nestJob (and SHEETLIVE\'s cloud-mode cards, liveChanged, 27 Nov 2026)', (cs.match(/root\.CNViewer\.sawSheet\(p, rec\)/g) || []).length === 3 && /async function liveChanged\(p, rec, L\) \{[\s\S]{0,1200}root\.CNViewer\.sawSheet\(p, rec\)/.test(cs));
}

(async () => {
  for (const [name, f] of [['V', partV], ['A', partA], ['M', partM], ['F', partF], ['C', partC], ['H', partH]]) {
    try { await f(); } catch (e) { ok(`${name} ran without an exception`, false, String(e && e.stack || e)); }
  }
  log0(failed ? `\n${failed} FAILED` : '\nall passed');
  process.exit(failed ? 1 : 0);
})();
