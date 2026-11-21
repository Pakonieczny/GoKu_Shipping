// G6 (Paul, 21 Nov 2026): the 10K / 14K / Rose Gold "In current set" switch on a staff computer (a viewer, mode "on") refused, in words, a take-out of a sheet of a COMMITTED set whenever
// the page had not read that set's record: the page could not tell the set was committed, so the press went to the cloud's member() (cmdLeaveSet refuses a committed set), and a
// sheet taken out of a committed set could not be put back from the switch. Now (charm-nest-bridge.js, G6 BEGIN … G6 END) the viewer reads the sheet's record and its set's record
// from the cloud (getSheet, setGet: READ ops) when the page cannot tell, and a sheet of a committed set goes by the Library's own path (editCommitted → LibraryFlow plan / commit:
// op flowApply step setMember, which the server takes from a page in mode "on"; the set's files then asked of the cloud, labelsAsk); the line next to the switch says what happened.
//   S  OFFLINE, the real modules: charm-nest-viewer.js, charm-nest-set-edit.js, charm-nest-flow.js in a vm context over the offline library (_cloudFakes.cjs) in mode "on", and the
//      bridge's own switch code (its In current set section, cut from charm-nest-bridge.js as it is) run over it: take-out from a committed set the page has no record of, put back
//      after a reload (the take-out read from the record's history), the page knowing the set, a sheet of an OPEN set (the cloud's), Rose Gold's include (the cloud's), a refusal.
//   B  the bridge: the viewer branch of the switch goes through g6ViewerRoute before cloudInclude; mode off unchanged; the page token is bumped
// Offline only: no Etsy, no live cloud. Made-up names.   NODE_PATH=<deps> node tests/charm-nest/viewer-g6.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelleg6';
const R = 'run-2026-11-21-g6', R2 = 'run-2026-11-21-g6b', SETID = 'set-2026-11-21-2', OPEN = 'set-2026-11-21-3', DAY = '2026-11-21';
const A = '4252000001', K = '4252000002', T = '4252000003', RG = '4252000004';
const pid = rid => `${rid}_${rid}1_1`;
const SRC = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');

/** The bridge's own switch code, cut from the file as it is: the committed-set tests (committedSheet … leftSetOf) and the switch's section (SETEDIT … G6 END). */
function switchCode() {
  const cut = (a, b) => { const i = SRC.indexOf(a), j = SRC.indexOf(b, i); if (i < 0 || j < 0) throw new Error('not found: ' + a); return SRC.slice(i, j); };
  return cut('  const committedSheet = sh =>', '  function projectLibraryRecords(rows)') + cut('  const SETEDIT = { busy: null };', '  /* ── G6 END ── */');
}

function records(T0) {
  const sheet = (id, rid, metal, extra) => Object.assign({ id, runId: R, metal, day: DAY, status: 'complete', page: 1, placedCount: 1, charmCount: 1, density: 0.41, stock: { wIn: 6, hIn: 4.5 },
    poolIds: [pid(rid)], orders: [rid], charms: [{ id: 'c-' + rid, poolId: pid(rid), order: rid, name: 'LEAF_CHARM', sku: 'LEAF_CHARM' }], placements: [{ id: 'c-' + rid, cxPt: 40, cyPt: 40, angle: 0, scale: 1 }],
    verification: { ok: true }, persistedDone: true, releaseFull: true, archived: false, updatedAt: T0 - 60000, createdAt: T0 - 3600e3 }, extra);
  const folder = `charmnest/sets/${DAY}/Set-2`;
  const a = sheet('gf-g6-a', A, 'gold', { fileBase: `GF_${DAY}_Set-2_Sheet-1`, draft: false, setId: SETID, setSeq: 2, sheetIndex: 1, outputs: { ai: { path: `${folder}/a/GF.ai`, url: 'https://storage.test.invalid/a.ai' } } });
  const k = sheet('k14-g6-k', K, 'gold14k', { fileBase: `14K_${DAY}_Set-2_Sheet-1`, draft: false, setId: SETID, setSeq: 2, sheetIndex: 1, solidIncluded: true, outputs: { ai: { path: `${folder}/k/14K.ai`, url: 'https://storage.test.invalid/k.ai' } } });
  const t = sheet('k10-g6-t', T, 'gold10k', { runId: R2, fileBase: `10K_${DAY}_Set-3_Sheet-1`, draft: false, setId: OPEN, setSeq: 3, sheetIndex: 1, solidIncluded: true, outputs: { ai: { path: 'o/t.ai', url: 'https://storage.test.invalid/t.ai' } } });
  const rg = sheet('rg-g6-r', RG, 'rose', { runId: R2, fileBase: `RG_${DAY}_Draft-1`, draft: true, setId: null, outputs: { ai: { path: 'o/r.ai', url: 'https://storage.test.invalid/r.ai' } } });
  const line = (rid, sh) => ({ held: null, lines: [{ transactionId: rid + '1', sku: 'LEAF_CHARM', copies: [{ copy: 1, sheetId: sh.id, sheet: sh.fileBase, poolId: pid(rid) }] }] });
  const set = { setId: SETID, runId: R, day: DAY, seq: 2, name: 'Set-2', folder, group: 'dispatch', status: 'complete', committedAt: T0 - 40000, committed: [A, K], sheetIds: [a.id, k.id], materials: ['gold', 'gold14k'],
    orders: { [A]: line(A, a), [K]: line(K, k) }, labelFiles: [], processSeals: [{ how: 'approved', by: 'Ann', at: T0 - 30000 }], updatedAt: T0 - 40000, createdAt: T0 - 3600e3 };
  const open = { setId: OPEN, runId: R2, day: DAY, seq: 3, name: 'Set-3', folder: `charmnest/sets/${DAY}/Set-3`, group: 'dispatch', status: 'nesting', formedAt: T0 - 3600e3, sheetIds: [t.id], materials: ['gold10k'], orders: { [T]: line(T, t) }, labelFiles: [] };
  const pool = (rid, sh, setId, run) => ({ poolId: pid(rid), orderId: rid, transactionId: rid + '1', lineKey: `${rid}_${rid}1`, sku: 'LEAF_CHARM', copy: 1, state: 'placed', sheetId: sh.id, setId: setId || null, runId: run, material: sh.metal, quantity: 1 });
  const ln = rid => ({ orderId: rid, transactionId: rid + '1', state: 'written', poolIds: [pid(rid)], sku: 'LEAF_CHARM', quantity: 1 });
  const run = { runId: R, status: 'complete', step: 'complete', finishedAt: T0 - 20000, mode: 'auto', day: DAY, releasePolicy: 2, setIds: [SETID], setId: SETID, errors: [], orders: [A, K],
    lines: { [`${A}_${A}1`]: Object.assign(ln(A), { material: 'gold' }), [`${K}_${K}1`]: Object.assign(ln(K), { material: 'gold14k' }) } };
  const run2 = { runId: R2, status: 'running', step: 'nest', mode: 'auto', day: DAY, releasePolicy: 2, setIds: [OPEN], errors: [], orders: [T, RG], solidIncluded: {}, sheets: {}, holds: {},
    lines: { [`${T}_${T}1`]: Object.assign(ln(T), { material: 'gold10k' }), [`${RG}_${RG}1`]: Object.assign(ln(RG), { material: 'rose' }) } };
  return { sheets: [a, k, t, rg], sets: [set, open], runs: [run, run2], pools: [pool(A, a, SETID, R), pool(K, k, SETID, R), pool(T, t, OPEN, R2), pool(RG, rg, null, R2)] };
}

async function partS() {
  const T0 = Date.UTC(2026, 10, 21, 18, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn, qe = console.error; console.log = () => {}; console.warn = () => {}; console.error = () => {};
  const out = [];
  try {
    const lib = F.load('charmNestLibrary.js');
    const answers = [];
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} answers.push({ op: body.op, http: r.statusCode, error: o.error || null, steps: (body.steps || []).map(s => s && s.type) }); return Object.assign({}, o, { http: r.statusCode }); };
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: ['rosecut', 'labels', 'files', 'cmd:leaveset'], flippedAt: T0 - 3600e3 }); lib._driveForget();
    F.seed('Charm_Nest_Open/run', { runId: R2, at: T0 - 600e3 });
    F.seed('Charm_Nest_Run_Carrier/' + R2, { runId: R2, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
    const rec = records(T0);
    for (const r of rec.runs) F.seed('Charm_Nest_Runs/' + r.runId, r);
    for (const s of rec.sheets) F.seed('Charm_Nest_Sheets/' + s.id, s);
    for (const s of rec.sets) F.seed('Charm_Nest_Sets/' + s.setId, s);
    for (const p of rec.pools) F.seed('Charm_Pool/' + p.poolId, p);
    F.seed('Charm_World/summary', { at: T0 - 60000, live: 0, mirrorBefore: T0 - 60000 });
    F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: T0 - 60000 });

    // the page's modules (window = self), as viewer-item5.cjs runs them, over the offline library in mode "on"
    const said = [], sent = [];
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout, clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, Date, JSON, Math, Map, Set, URL, TextEncoder, String, Number, Array, Object, RegExp, Error,
      require: p => require(path.join(ROOT, p)) };
    w.window = w; w.self = w; w.globalThis = w;
    w.toast = (t, tone) => { said.push({ t: String(t), tone }); };
    w.CNEmployee = { name: () => 'Michelle' };
    w.CharmNestRunFollow = { state: () => ({ runId: R2 }), dev: () => PAGE, now: async () => ({}) };
    w.CN = { api: async (fn, body) => { sent.push(JSON.parse(JSON.stringify(body))); return P(Object.assign({ cv: CV, dev: PAGE }, body)); }, toast: (t, tone) => said.push({ t: String(t), tone }) };
    const ctx = vm.createContext(w);
    for (const f of ['charm-nest-viewer.js', 'charm-nest-set-edit.js', 'charm-nest-flow.js']) vm.runInContext(fs.readFileSync(path.join(ROOT, f), 'utf8'), ctx, { filename: f });
    w.LibraryFlow.configure({ employee: () => 'Michelle', rows: () => [], live: () => null, applyMembership: async () => {} });
    await w.CNViewer.ready();

    // the bridge's switch code over it: this page holds no run (a viewer) and, unless a check says so, has read NO set record (Sets.ofRun → [])
    let pageSets = [];
    const busy = [], ops = () => sent.map(b => b.op);
    const make = vm.runInContext(`(function (setTimeout, api, toast, employeeName, needEmployee, B, S, Sets, Session, refreshMembership, renderRelease, goldFamily, solid, inSetNow, sheetNameOf, labelOf) {
${switchCode()}
  return { g6ViewerRoute, g6SetOf, cloudPress, includeRoute };
})`, ctx, { filename: 'charm-nest-bridge.js (In current set)' });
    const solid = m => m === 'gold10k' || m === 'gold14k';
    const G = make(fn => setTimeout(fn, 0), (fn, body) => w.CN.api(fn, body), w.toast, () => 'Michelle', async () => 'Michelle', { run: null }, { cloud: { ok: true } },
      { ofRun: () => pageSets }, { schedule() {} }, () => {}, sh => busy.push(sh._incBusy ? sh._incBusy.text : null), m => m === 'rose' || solid(m), solid, sh => !!sh.setId && !sh.draft,
      sh => `${sh.metal} sheet`, m => m);
    const node = {};   // (no line drawn on this test page: the answer is said by toast, never silent)
    const press = async (sh, want) => { said.length = 0; busy.length = 0; sent.length = 0; const a0 = answers.length; const handled = await G.g6ViewerRoute(sh, node, want); return { handled, said: said.slice(), busy: busy.slice(), ops: ops(), answers: answers.slice(a0) }; };
    const R_ = id => F.doc('Charm_Nest_Sheets/' + id), inSet = (set, id) => (F.doc('Charm_Nest_Sets/' + set).sheetIds || []).includes(id);

    out.push(['S0 the page is a viewer by its own read of the switch (mode "on"), and the switch is the cloud\'s on this 14K sheet (cloudPress)', w.CNViewer.on() === true && G.cloudPress({ runId: R, metal: 'gold14k', sheetId: 'k14-g6-k' }) === true]);

    /* ═══ the take-out of a sheet of a COMMITTED set, the page holding no record of that set ═══ */
    const k = { sheetId: 'k14-g6-k', runId: R, metal: 'gold14k', page: 1, setId: SETID, draft: false };
    out.push(['S1 the page alone cannot tell: no set read, so its own rule would send the press to the cloud\'s member() (the gap)', JSON.stringify(G.includeRoute(k, false)) === '{}']);
    let x = await press(k, false);
    const leave = x.answers.find(a => a.op === 'flowApply');
    out.push(['S2 the take-out is answered here, by the Library\'s path: the sheet and its set read from the cloud (getSheet, setGet), then the Library\'s plan and its flowApply setMember',
      x.handled === true && x.ops.indexOf('getSheet') >= 0 && x.ops.indexOf('setGet') > x.ops.indexOf('getSheet') && !!leave && leave.steps.includes('setMember') && !x.ops.includes('roseCutAsk'), x]);
    out.push(['S3 nothing the page sent was refused (mode "on" takes the Library\'s setMember from a page)', x.answers.every(a => a.http < 400), x.answers]);
    out.push(['S4 the records: the 14K sheet is out of committed Set 2 (a draft, in no set; Set 2 lists only the GF sheet); the GF sheet stays',
      R_('k14-g6-k').draft === true && !R_('k14-g6-k').setId && !inSet(SETID, 'k14-g6-k') && inSet(SETID, 'gf-g6-a') && R_('gf-g6-a').setId === SETID, { k: { d: R_('k14-g6-k').draft, s: R_('k14-g6-k').setId }, set: F.doc('Charm_Nest_Sets/' + SETID).sheetIds }]);
    const asks = F.list('Charm_Nest_Label_Asks');
    out.push(['S5 Set 2\'s files are asked of the cloud (labelsAsk setFiles, in Michelle\'s name), not made on this computer', asks.some(a => a.kind === 'setFiles' && a.setId === SETID && a.by === 'Michelle') && x.ops.includes('labelsAsk'), asks]);
    out.push(['S6 the person sees it: the switch busy while it reads and saves, then the line "Taken out of Set 2." with the labels asked of the cloud, as done (never silent, never a refusal)',
      x.busy.includes('Reading its set…') && x.busy.includes('Taking out…') && x.said.length === 1 && x.said[0].tone === 'ok' && /^Taken out of Set 2\. Its labels are asked of the cloud\.$/.test(x.said[0].t) && k.leftSet === SETID, x]);

    /* ═══ put back, after a reload: the page's copy is a draft with no take-out mark; the record's history names the set ═══ */
    const k2 = { sheetId: 'k14-g6-k', runId: R, metal: 'gold14k', page: 1, setId: null, draft: true };
    x = await press(k2, true);
    const backOk = x.handled === true && inSet(SETID, 'k14-g6-k') && R_('k14-g6-k').setId === SETID && R_('k14-g6-k').draft === false;
    const backSaid = x.said[0] || {};
    out.push(['S7 the include of a sheet taken out of a committed set (the page holding neither the set nor the mark): read from the cloud, the Library\'s path, and it is answered: back in Set 2, or the Library\'s own words',
      x.handled === true && x.ops.includes('getSheet') && x.ops.includes('setGet') && !x.ops.includes('roseCutAsk') && x.said.length === 1 && (backOk ? backSaid.tone === 'ok' && /^Back in Set 2\./.test(backSaid.t) : backSaid.tone === 'bad' && !!backSaid.t),
      { x, back: backOk, rec: { s: R_('k14-g6-k').setId, d: R_('k14-g6-k').draft, hold: R_('k14-g6-k').laserHold, hist: (R_('k14-g6-k').flowHistory || []).slice(-2) } }]);
    log0('  (S7: ' + (backOk ? 'put back in Set 2: ' : 'refused by the Library: ') + JSON.stringify(backSaid.t) + ')');

    /* ═══ the page knows the committed set: no read, the Library's path at once (as before) ═══ */
    if (!backOk) { F.seed('Charm_Nest_Sheets/k14-g6-k', Object.assign(R_('k14-g6-k'), { draft: false, setId: SETID, setSeq: 2, laserHold: null })); F.seed('Charm_Nest_Sets/' + SETID, Object.assign(F.doc('Charm_Nest_Sets/' + SETID), { sheetIds: ['gf-g6-a', 'k14-g6-k'] })); }
    pageSets = [F.doc('Charm_Nest_Sets/' + SETID)];
    const k3 = { sheetId: 'k14-g6-k', runId: R, metal: 'gold14k', page: 1, setId: SETID, draft: false };
    x = await press(k3, false);
    out.push(['S8 a page that holds the committed set: the Library\'s path at once (no getSheet read), the same done line', x.handled === true && !x.ops.includes('getSheet') && x.ops.includes('flowApply') && x.said.length === 1 && x.said[0].tone === 'ok' && /^Taken out of Set 2\./.test(x.said[0].t) && !inSet(SETID, 'k14-g6-k'), x]);
    pageSets = [];

    /* ═══ the last sheet of a committed set: the Library refuses it, in its words; nothing changed ═══ */
    const gfSet = F.doc('Charm_Nest_Sets/' + SETID).sheetIds.slice();
    x = await press({ sheetId: 'gf-g6-a', runId: R, metal: 'gold14k', page: 1, setId: SETID, draft: false }, false);
    out.push(['S9 a take-out the Library refuses (the last sheet of the set) is answered in its words, the switch shaken; nothing written', x.handled === true && x.said.length === 1 && x.said[0].tone === 'bad' && !!x.said[0].t && JSON.stringify(F.doc('Charm_Nest_Sets/' + SETID).sheetIds) === JSON.stringify(gfSet) && !x.ops.includes('flowApply'), x]);

    /* ═══ the cloud's presses stay the cloud's ═══ */
    x = await press({ sheetId: 'k10-g6-t', runId: R2, metal: 'gold10k', page: 1, setId: OPEN, draft: false }, false);
    out.push(['S10 a sheet of an OPEN set: read, not committed, so the press is the cloud\'s (cloudInclude: roseCutAsk), nothing written here', x.handled === false && x.ops.includes('setGet') && !x.ops.includes('flowApply') && !x.said.length && inSet(OPEN, 'k10-g6-t'), x]);
    x = await press({ sheetId: 'rg-g6-r', runId: R2, metal: 'rose', page: 1, setId: null, draft: true, leftSet: SETID }, true);
    out.push(['S11 Rose Gold\'s include is the cloud\'s as before (it joins a committed set only by its Cut Sheet press): no read', x.handled === false && !x.ops.length, x]);
    x = await press({ sheetId: 'k10-g6-t', runId: R2, metal: 'gold10k', page: 1, setId: null, draft: true }, true);
    out.push(['S12 an include of a sheet never taken out of a committed set: read, the cloud\'s', x.handled === false && x.ops.includes('getSheet') && !x.ops.includes('flowApply'), x]);
  } catch (e) { out.push(['S run', false, String(e && e.stack || e).split('\n').slice(0, 5).join(' | ')]); }
  finally { console.log = quiet; console.warn = qw; console.error = qe; }
  for (const [n, c, d] of out) ok(n, c, d);
}

function partB() {
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  const on = SRC.indexOf('include.onchange=async e=>{'), seg = SRC.slice(on, SRC.indexOf('changeMembership(m,want,list)', on) + 40);
  ok('B1 the switch on a viewer: g6ViewerRoute first (a committed set: the Library\'s path), then the cloud (cloudInclude); mode off keeps its guard and the run\'s own Include',
    /if\(cloudPress\(sh\)\)\{const want=e\.target\.checked;if\(await g6ViewerRoute\(sh,node,want\)\)return;.*?return cloudInclude\(sh,node,want\);\}/.test(seg) && seg.indexOf('Changed on the computer that carries the run') > seg.indexOf('g6ViewerRoute') && /if\(route\.edit\)return editCommitted\(sh,node,want\);/.test(seg) && /changeMembership\(m,want,list\)/.test(seg));
  ok('B2 the reads are READ ops (getSheet, setGet) and the done line is only a viewer\'s (o.viewer); mode off\'s runEdit words unchanged',
    /op: "getSheet", id: sh\.sheetId/.test(SRC) && /op: "setGet", setId: id/.test(SRC) && /if \(o && o\.viewer\) \{/.test(SRC) && /return res\.warnings && res\.warnings\.length \? "Saved\. The set's labels are not remade yet\." : "";/.test(SRC));
  ok('B3 charm-nest-1.html loads the changed bridge under a new token (-g6)', /charm-nest-bridge\.js\?v=[^"]*-g6(?:-\w+)*"/.test(html));
}

(async () => {
  await partS();
  partB();
  log0(failed ? `FAIL viewer-g6 (${failed})` : 'PASS viewer-g6');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-g6: ' + (e && e.stack || e)); process.exit(1); });
