// G4 (Paul, 21 Nov 2026): a Rose Gold (or 10K / 14K) sheet whose green line is CUT ALREADY and that is in no set could not be put into its set from a staff computer (a viewer) in mode "on":
// the cloud's cut request had no "join only" path (roseCutAsk refused a cut sheet as "recorded already"), and the page's own include writes nothing on a viewer. Now every such press is ONE
// ask the cloud acts on, roseCutAsk kind "join" (an Include on a cut sheet in no set is the same), joined by the set rules, never cut again (_charmNestGreenLine.js joinCut).
//   S  offline (_cloudFakes.cjs, mode on): a cut Rose Gold sheet taken out of its set joins Set 1 again (its yes, its take-out mark and "automatic cut off" cleared, its cut untouched, no rose op);
//      a cut 14K sheet's Include joins too; one already in its set (Gate.rejoin's case too) is said so; a draft a committed set lists is never "done" (where it is put back, in words); the
//      owner's set wins; an uncut sheet's join is refused in words; a Cut Sheet ask on a cut sheet is refused as before; no Etsy call
//   P  charm-nest-rose-ui.js in a vm: on a viewer, Cut Sheet / Approve on a cut sheet in no set sends ONE ask kind "join" (no cut); a copy that had not seen its cut turns the "cut already" answer
//      into that join; RoseStock.join passes the set dropped on and says the cloud's refusal; cloudJoins only on a viewer; mode off asks nothing
//   F  charm-nest-flow.js: the Library's drag of a cut sheet into a set on a viewer plans ONE cloudJoin step (its yes asked), commit asks the cloud and says its answer, a refusal stops it in
//      the cloud's words; mode off plans as before; makeRoseJoin's joinOnly on a viewer asks the cloud instead of Gate.changeMembership
//   R  charm-nest-set-repair.js: Approve on a cut sheet on a viewer is its join (a window, then env.joinCut), never a cut; mode off unchanged
//   H  charm-nest-bridge.js (the switch of a cut sheet a committed set lost) and charm-nest-1.html tokens
// Offline only: no Etsy, no live cloud. Made-up names.   NODE_PATH=<deps> node tests/charm-nest/viewer-g4.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const T0 = Date.UTC(2026, 10, 21, 15, 0, 0), CV = 'cloud14-20261120', PAGE = 'd0michelle1';
const R = 'run-2026-11-21-g4', SET = 'set-2026-11-21-1', SET0 = 'set-2026-11-21-0', DAY = '2026-11-21';
const outline = { subpaths: [[['m', [0, 0]], ['l', [10, 0]], ['l', [10, 10]], ['l', [0, 10]], ['h']]] };

/* ── S: the cloud ── */
async function partS() {
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  const out = [];
  try {
    const lib = F.load('charmNestLibrary.js'), Green = F.load('_charmNestGreenLine.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const pid = (rid, n) => `${rid}_${rid}${n}_1`;
    const sheet = (id, metal, rid, n, o) => {
      const p = pid(rid, n), c = { id: 'c-' + p, poolId: p, order: rid, areaPt2: 400, widthPt: 20, heightPt: 20 };
      F.seed('Charm_Pool/' + p, { poolId: p, runId: R, orderId: rid, state: 'placed', sheetId: id, material: metal });
      F.seed('Charm_Nest_Sheets/' + id, Object.assign({ id, runId: R, metal, page: 1, day: DAY, draft: true, setId: null, setSeq: null, sheetIndex: null, status: 'complete', archived: false,
        charms: [c], placements: [{ id: c.id, cxPt: 20, cyPt: 20, angle: 0 }], poolIds: [p], orders: [rid], outputs: { ai: { path: `x/${id}.ai` } }, verification: { ok: true }, releaseFull: false,
        fileBase: `${metal}_working_${id}`, layoutRev: 1, cardStartedAt: T0 - 3600e3 }, o || {}));
    };
    const CUT = T0 - 600e3, cutOf = (stock, o) => Object.assign({ roseCutAt: CUT, roseStockId: stock, rosePlanHash: 'h-' + stock, roseRevision: 1 }, o || {});
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: ['rosecut', 'labels', 'cmd:leaveset'], flippedAt: T0 - 3600e3 }); lib._driveForget();
    F.seed('Charm_Nest_Runs/' + R, { runId: R, releasePolicy: 2, status: 'running', step: 'nest', mode: 'auto', day: DAY, solidIncluded: {}, lines: {}, sheets: {}, holds: {}, errors: [], orders: [] });
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
    F.seed('Charm_Nest_Sets/' + SET0, { setId: SET0, runId: R, day: DAY, seq: 1, name: 'Set-1', folder: `charmnest/sets/${DAY}/Set-1`, group: 'dispatch', status: 'complete', committedAt: T0 - 7200e3, formedAt: T0 - 9000e3, sheetIds: ['gf-s0-g4', 'ss-s0-g4', 'rg-c0-g4', 'rg-c1-g4'], materials: ['gold', 'silver', 'rose'], orders: {}, sheetNos: {}, labelFiles: [] });
    F.seed('Charm_Nest_Sets/' + SET, { setId: SET, runId: R, day: DAY, seq: 2, name: 'Set-2', folder: `charmnest/sets/${DAY}/Set-2`, group: 'dispatch', status: 'nesting', formedAt: T0 - 3600e3, sheetIds: ['gf-s1-g4', 'ss-s1-g4'], materials: ['gold', 'silver'], orders: {}, sheetNos: {}, labelFiles: [] });
    sheet('gf-s0-g4', 'gold', '4250000700', 1, { draft: false, setId: SET0, setSeq: 1, sheetIndex: 1, releaseFull: true, fileBase: 'GF_Nov.21.26_Set-1_Sheet-1' });
    sheet('ss-s0-g4', 'silver', '4250000701', 1, { draft: false, setId: SET0, setSeq: 1, sheetIndex: 1, releaseFull: true, fileBase: 'SS_Nov.21.26_Set-1_Sheet-1' });
    sheet('rg-c0-g4', 'rose', '4250000700', 2, cutOf('rgs-c0', { setId: SET0, setSeq: 1, sheetIndex: 1, draft: false, solidIncluded: true }));   // (committed Set 1 lists it and so does its record: only a page's copy lost its place)
    sheet('rg-c1-g4', 'rose', '4250000701', 2, cutOf('rgs-c1', { setId: null, draft: true }));                                      // (committed Set 1 lists it; its record is a draft)
    sheet('gf-s1-g4', 'gold', '4250000751', 1, { draft: false, setId: SET, setSeq: 2, sheetIndex: 1, releaseFull: true, fileBase: 'GF_Nov.21.26_Set-2_Sheet-1' });
    sheet('ss-s1-g4', 'silver', '4250000752', 1, { draft: false, setId: SET, setSeq: 2, sheetIndex: 1, releaseFull: true, fileBase: 'SS_Nov.21.26_Set-2_Sheet-1' });
    sheet('rg-g4', 'rose', '4250000751', 2, cutOf('rgs-1', { solidIncluded: false, leftSet: SET, autoCutOff: true }));          // cut, then taken out of Set 2
    sheet('k14-g4', 'gold14k', '4250000795', 1, cutOf('rgs-2'));                                                                  // cut 14K with an order of its own, in no set
    sheet('k10-un-g4', 'gold10k', '4250000790', 1);                                                                               // not cut
    sheet('k10-own-g4', 'gold10k', '4250000791', 1, cutOf('rgs-3', { ownerSet: { setId: 'set-elsewhere', by: 'Paul', at: T0 - 1000 } }));
    const ops = [];
    let pass = 0;
    const cloud = async () => { pass++; const ctx = { db: F.db, run: Object.assign({}, F.doc('Charm_Nest_Runs/' + R), { runId: R }), runId: R, budgetEnd: Date.now() + 600000, cloudSteps: ['rosecut'], say: () => {},
      call: (b, u) => { ops.push(b.op); return lib.call(b, { unit: `g4|${u}|pass${pass}` }); }, read: b => lib.call(b, {}) }; return Green.askUnit(ctx); };
    const press = async (id, kind, extra) => { const r = await P(Object.assign({ op: 'roseCutAsk', sheetId: id, kind, by: 'Michelle', dev: PAGE, cv: CV }, extra || {})); ops.length = 0; await cloud(); return { r, ask: F.doc('Charm_Nest_Cut_Asks/' + id), ops: ops.slice() }; };
    const rec = id => F.doc('Charm_Nest_Sheets/' + id), inSet = (id, s) => (F.doc('Charm_Nest_Sets/' + (s || SET)).sheetIds || []).includes(id);
    const noRose = list => !list.some(op => /^rose(Plan|RecordCut|Claim|Reopen)$/.test(op));
    const cuts = id => { const s = rec(id).roseStockId; return s ? F.list(`Charm_Nest_Rose_Stock/${s}/cuts`).length : 0; };

    let x = await press('rg-g4', 'join', { setId: SET });
    const rg = rec('rg-g4');
    out.push(['S1 a viewer\'s join of a cut Rose Gold sheet taken out of its set is taken (a person\'s press) and the cloud puts it in Set 2: in the set\'s list, its yes, the take-out mark and "automatic cut off" cleared',
      x.r.http === 200 && x.r.ok && x.r.ask.kind === 'join' && x.r.ask.setId === SET && x.ask.state === 'done' && rg.setId === SET && rg.draft === false && rg.solidIncluded === true && !rg.leftSet && rg.autoCutOff === false && inSet('rg-g4'),
      { r: x.r, ask: x.ask, rg: { setId: rg.setId, draft: rg.draft, si: rg.solidIncluded, left: rg.leftSet, off: rg.autoCutOff }, list: F.doc('Charm_Nest_Sets/' + SET).sheetIds }]);
    out.push(['S2 ... never cut again: its cut and stock as they were, no rosePlan / roseClaim / roseRecordCut / roseReopen, and the answer says so in words',
      rg.roseCutAt === CUT && rg.roseStockId === 'rgs-1' && rg.rosePlanHash === 'h-rgs-1' && cuts('rg-g4') === 0 && noRose(x.ops) && /is in Set 2 now/.test(x.ask.why) && /nothing was cut again/.test(x.ask.why), { why: x.ask.why, ops: x.ops }]);
    x = await press('k14-g4', 'include');
    out.push(['S3 the In current set switch of a cut 14K sheet in no set (kind include) is its join: in Set 2, nothing cut, said in words', x.ask.state === 'done' && inSet('k14-g4') && rec('k14-g4').draft === false && rec('k14-g4').roseCutAt === CUT && noRose(x.ops) && /in Set 2 now/.test(x.ask.why) && /nothing was cut again/.test(x.ask.why), { ask: x.ask, ops: x.ops }]);
    x = await press('k14-g4', 'join');
    out.push(['S4 a join of a sheet already in its set: done, said so, nothing changed', x.ask.state === 'done' && /is in Set 2 already/.test(x.ask.why) && inSet('k14-g4') && noRose(x.ops), x.ask]);
    x = await press('rg-c0-g4', 'join');
    out.push(['S5 Gate.rejoin\'s case (only a page\'s copy lost its place in COMMITTED Set 1; the record kept it): the cloud says it is in Set 1 already (the page follows the record), nothing written or cut',
      x.ask.state === 'done' && /is in Set 1 already/.test(x.ask.why) && rec('rg-c0-g4').draft === false && rec('rg-c0-g4').setId === SET0 && noRose(x.ops) && !x.ops.includes('putSheet'), { ask: x.ask, ops: x.ops }]);
    x = await press('rg-c1-g4', 'join');
    out.push(['S5b a draft record that committed Set 1 still lists: never "done" (only the set\'s own edit changes a committed set): refused in words that say where it is put back; nothing written',
      x.ask.state === 'refused' && /sent to the station/.test(x.ask.why) && /drop it on Set 1 in the Library/.test(x.ask.why) && rec('rg-c1-g4').draft === true && !x.ops.includes('putSheet'), { ask: x.ask, ops: x.ops }]);
    x = await press('k10-own-g4', 'join');
    out.push(['S6 a cut sheet whose set the owner decided goes into no other set: refused in words, nothing changed', x.ask.state === 'refused' && /owner/.test(x.ask.why) && rec('k10-own-g4').draft === true && !rec('k10-own-g4').solidIncluded && !inSet('k10-own-g4'), x.ask]);
    const un = await P({ op: 'roseCutAsk', sheetId: 'k10-un-g4', kind: 'join', by: 'Michelle', dev: PAGE, cv: CV });
    out.push(['S7 a join of a sheet whose green line is NOT cut is refused at once in words (press Cut Sheet), no ask written', !!un.error && /not cut yet/.test(un.error) && !F.doc('Charm_Nest_Cut_Asks/k10-un-g4'), un]);
    F.seed('Charm_Nest_Sheets/k10-own-g4', Object.assign(rec('k10-own-g4'), { ownerSet: null }));
    const again = await P({ op: 'roseCutAsk', sheetId: 'k10-own-g4', kind: 'cut', by: 'Michelle', dev: PAGE, cv: CV });
    out.push(['S8 a Cut Sheet ask on a cut sheet is refused as before ("recorded already", cut: true: the page turns it into its join)', !!again.error && again.cut === true && /recorded already/.test(again.error), again]);
    const nobody = await P({ op: 'roseCutAsk', sheetId: 'k10-own-g4', kind: 'join', dev: PAGE, cv: CV });
    out.push(['S9 a join says who pressed it, or is refused', !!nobody.error, nobody]);
    out.push(['S10 Etsy never called', F.etsy.calls === 0, F.etsy.calls]);
  } catch (e) { out.push(['S run', false, String(e && e.stack || e).split('\n').slice(0, 5).join(' | ')]); }
  finally { console.log = quiet; console.warn = qw; }
  for (const [n, c, d] of out) ok(n, c, d);
}

/* ── P: charm-nest-rose-ui.js on a page, its io faked ── */
async function partP() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-rose-ui.js'), 'utf8');
  function page(o) {
    const calls = [], toasts = [], asks = o.asks || [];
    const sh = Object.assign({ metal: 'rose', sheetId: 'rg-p1', runId: R, el: null, charms: [{ id: 'c', outline, centerPt: [5, 5], members: [] }], placements: [{ id: 'c', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }],
      persistedDone: true, verification: { ok: true }, status: 'complete', dirty: false, setId: null, draft: true, roseCutAt: CUTP }, o.sheet || {});
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout, clearTimeout, Promise, Date, JSON, Math, Object, Array, String, Number, Error, Set, Map, WeakMap, queueMicrotask, structuredClone };
    w.window = w; w.CharmNestRose = require(path.join(ROOT, 'charm-nest-rose.js'));
    w.CNEmployee = { name: () => 'Michelle' };
    if (o.viewer) { w.CNViewer = { on: () => true }; w.CharmNestCloudLanes = name => name === 'rosecut'; }
    w.Session = { schedule() {} };
    w.CN = { S: { cloud: { ok: true }, settings: {}, mode: 'nest' }, esc: s => String(s), toast: (t, k) => toasts.push(String(t)), allSheets: () => [], renderCard() {}, drawPreview() {}, stockFor: () => ({ wPt: 300, hPt: 150 }), uid: () => 'u',
      api: async (fn, body) => {
        calls.push(body);
        if (body.op === 'roseCutAsk') { if (o.cutFirst && body.kind !== 'join') return { error: "This sheet's green line cut is recorded already", status: 409, cut: true }; return { ok: true, ask: { sheetId: body.sheetId, kind: body.kind, by: body.by, at: Date.now(), seq: 1, state: 'asked', setId: body.setId || null } }; }
        if (body.op === 'roseCutAskGet') return { ok: true, asks: asks.length > 1 ? [asks.shift()] : asks.slice() };
        if (body.op === 'getSheet') return { sheet: Object.assign({ id: body.id, metal: sh.metal, roseCutAt: CUTP, rosePlanHash: 'h', draft: true, setId: null }, o.after || {}) };
        if (body.op === 'roseGet') return { stock: { id: 'rgs-p', revision: 1 }, cuts: [] };
        return { error: 'not in this test' };
      } };
    vm.createContext(w);
    vm.runInContext(src, w);
    w.RoseStock.ASK.pollMs = 5; w.RoseStock.ASK.waitMs = 2000;
    return { w, sh, calls, toasts };
  }
  const CUTP = 1790000000000, rose = c => c.filter(x => /^rose(Plan|RecordCut|Claim|Reopen)$/.test(x.op));
  { const { w, sh, calls, toasts } = page({ viewer: true, asks: [{ sheetId: 'rg-p1', seq: 1, state: 'working' }, { sheetId: 'rg-p1', seq: 1, state: 'done', why: 'RG Draft 1 is in Set 2 now. Its green line was cut already, so nothing was cut again' }], after: { setId: 'set-2', draft: false, setSeq: 2 } });
    let res = null, err = null; try { res = await w.RoseStock.record(sh, {}); } catch (e) { err = e.message; }
    const asks = calls.filter(c => c.op === 'roseCutAsk');
    ok('P1 viewer: Cut Sheet / Approve on a cut sheet in no set sends ONE roseCutAsk kind "join" (never "cut"), no rose op', asks.length === 1 && asks[0].kind === 'join' && asks[0].by === 'Michelle' && !rose(calls).length, calls.map(c => c.op + ':' + (c.kind || '')));
    ok('P2 ... the card says it was asked, then the cloud\'s answer (its words) and the sheet in its set as the record has it', !err && res && /in Set 2 now/.test(res.why) && sh.setId === 'set-2' && sh.draft === false && toasts.some(t => /asked/.test(t) && /no second cut/.test(t)) && toasts.some(t => /in Set 2 now/.test(t)), { err, res, toasts, set: sh.setId }); }
  { const { w, sh, calls } = page({ viewer: true, cutFirst: true, sheet: { roseCutAt: null }, asks: [{ sheetId: 'rg-p1', seq: 1, state: 'done', why: 'RG Draft 1 is in Set 2 now' }], after: { setId: null, draft: true } });
    let err = null; try { await w.RoseStock.record(sh, {}); } catch (e) { err = e.message; }
    const kinds = calls.filter(c => c.op === 'roseCutAsk').map(c => c.kind);
    ok('P3 a copy that had not seen its cut: the "cut already" answer is read again, and the press becomes its join (cut, then join), nothing cut here', !err && kinds.join() === 'cut,join' && sh.roseCutAt === CUTP && !rose(calls).length, { err, kinds }); }
  { const { w, calls } = page({ viewer: true, asks: [{ sheetId: 'rg-lib', seq: 1, state: 'refused', why: 'RG Draft 4 is not in a set yet (Nest and verify first). Your yes is kept: it joins its set by itself once the set rules let it.' }] });
    const recd = { sheetId: 'rg-lib', metal: 'rose', runId: R, roseCutAt: CUTP, setId: null, draft: true };
    let err = null; try { await w.RoseStock.join(recd, { by: 'Michelle', setId: 'set-2' }); } catch (e) { err = e.message; }
    const a = calls.find(c => c.op === 'roseCutAsk');
    ok('P4 RoseStock.join on a Library record (no page copy): the set it was dropped on goes with the ask; a refusal comes back in the cloud\'s own words', a && a.kind === 'join' && a.setId === 'set-2' && /not in a set yet/.test(err || '') && /Your yes is kept/.test(err || '') && !calls.some(c => c.op === 'getSheet'), { a, err }); }
  { const v = page({ viewer: true }).w, off = page({ viewer: false }).w, r0 = { sheetId: 'x', metal: 'gold14k', runId: R, roseCutAt: CUTP, setId: null, draft: true };
    ok('P5 cloudJoins: a cut green-line sheet in no set on a viewer only (not in a set, not uncut, not GF, not laser cut, never in mode off)',
      v.RoseStock.cloudJoins(r0) && !v.RoseStock.cloudJoins(Object.assign({}, r0, { setId: 's', draft: false })) && !v.RoseStock.cloudJoins(Object.assign({}, r0, { roseCutAt: null })) && !v.RoseStock.cloudJoins(Object.assign({}, r0, { metal: 'gold' })) && !v.RoseStock.cloudJoins(Object.assign({}, r0, { laserDoneAt: 5 })) && !off.RoseStock.cloudJoins(r0)); }
  { const { w, sh, calls } = page({ viewer: false });
    try { await w.RoseStock.record(sh, {}); } catch (_) { /* no Gate here: only the calls matter */ }
    ok('P6 mode off: the press runs on this page as before, no ask', !calls.some(c => c.op === 'roseCutAsk'), calls.map(c => c.op)); }
}

/* ── F: the Library's drag (charm-nest-flow.js) ── */
async function partF() {
  const LF = require(path.join(ROOT, 'charm-nest-flow.js'));
  const CUTF = 1790000000000, joins = [];
  const recs = {
    'rg-f': { id: 'rg-f', runId: R, metal: 'rose', draft: true, setId: null, roseCutAt: CUTF, roseStockId: 'rgs-f', rosePlanHash: 'h', status: 'complete', verification: { ok: true }, orders: ['4250000751'], placedCount: 1, charmCount: 1 },
    'rg-new': { id: 'rg-new', runId: R, metal: 'rose', draft: true, setId: null, roseStockId: 'rgs-n', status: 'complete', verification: { ok: true }, orders: ['4250000753'], placedCount: 1, charmCount: 1 }
  };
  const setDoc = { setId: 'set-2', seq: 2, runId: R, day: DAY, sheetIds: ['gf-1', 'ss-1'], status: 'nesting' };
  let answer = { ok: true, why: 'RG Draft 1 is in Set 2 now. Its green line was cut already, so nothing was cut again' };
  const inS = r => !!(r && r.setId && !r.draft && r.solidIncluded !== false);
  LF.configure({
    api: async b => b.op === 'flowState' ? { sheets: (b.sheetIds || []).map(i => recs[i]).filter(Boolean), sets: (b.setIds || []).includes('set-2') ? [setDoc] : [], runs: { [R]: { open: true } } } : { error: 'not in this test' },
    employee: () => 'Michelle', rows: () => [], cuts: m => ['rose', 'gold10k', 'gold14k'].includes(m), rose: () => null, live: () => null, shared: { between: () => [] }, areaOf: null, sets: null, setOf: null,
    cloudJoins: r => !!(r && r.roseCutAt && !inS(r)),
    cloudJoin: async (id, o) => { joins.push({ id, setId: o.setId, by: o.by }); return answer; }
  });
  let p = await LF.plan({ kind: 'sheet', id: 'rg-f', to: { set: 'set-2' } });
  ok('F1 viewer: a cut Rose Gold sheet dropped on Set 2 plans ONE cloudJoin step and asks its yes; nothing refused ("open on another screen" is gone for it)',
    p.needs.length === 0 && p.steps.map(s => s.type).join() === 'cloudJoin' && p.steps[0].setId === 'set-2' && p.confirm.map(c => c.key).join() === 'roseSet' && /no cut is recorded/.test(p.confirm[0].detail), { needs: p.needs, steps: p.steps, confirm: p.confirm });
  let r = await LF.commit(p, { by: 'Michelle', confirmed: [] });
  ok('F2 ... without its yes nothing is asked', r.ok === false && !joins.length, r.error);
  r = await LF.commit(p, { by: 'Michelle', confirmed: ['roseSet'] });
  ok('F3 ... with its yes the cloud is asked (the set dropped on, who) and the move says the cloud\'s answer', r.ok === true && joins.length === 1 && joins[0].id === 'rg-f' && joins[0].setId === 'set-2' && joins[0].by === 'Michelle' && (r.applied || []).some(a => /in Set 2 now/.test(a.label)), { r: { ok: r.ok, error: r.error, applied: r.applied }, joins });
  answer = { ok: false, error: 'RG Draft 1 is not in a set yet (Taken out of its set). Your yes is kept: it joins its set by itself once the set rules let it' };
  r = await LF.commit(p, { by: 'Michelle', confirmed: ['roseSet'] });
  ok('F4 ... a refusal stops the move in the cloud\'s own words (never "done")', r.ok === false && /not in a set yet/.test(r.error || ''), r.error);
  p = await LF.plan({ kind: 'sheet', id: 'rg-new', to: { set: 'set-2' } });
  ok('F5 an UNCUT sheet is not a cloud join (its Cut Sheet press is item 4\'s path): planned as before', !p.steps.some(s => s.type === 'cloudJoin'), { needs: p.needs.map(n => n.key), steps: p.steps.map(s => s.type) });
  LF.configure({ cloudJoins: null, cloudJoin: null }); LF.hooks.cloudJoins = null; LF.hooks.cloudJoin = null;
  p = await LF.plan({ kind: 'sheet', id: 'rg-f', to: { set: 'set-2' } });
  ok('F6 mode off (no cloud join): planned exactly as before (the run open on another screen)', !p.steps.some(s => s.type === 'cloudJoin') && p.needs.some(n => n.key === 'runElsewhere'), { needs: p.needs.map(n => n.key), steps: p.steps.map(s => s.type) });
  // makeRoseJoin (the roseJoin hook) with joinOnly on a viewer: the cloud's join, not Gate.changeMembership
  { const log = [], sh = { sheetId: 'rg-m', metal: 'rose', runId: R, roseCutAt: CUTF, setId: null, draft: true };
    const RS = { cloudJoins: s => !!s.roseCutAt, join: async (s, o) => { log.push('join:' + o.by); return { ok: true, why: 'RG Draft 2 is in Set 2 now' }; }, record: async () => { log.push('record'); }, render() {} };
    const G = { changeMembership: async () => { log.push('changeMembership'); } };
    const h = await LF.core.makeRoseJoin(() => ({ RoseStock: RS, Gate: G, CN: { allSheets: () => [sh] } }))('rg-m', { by: 'Michelle', joinOnly: true });
    ok('F7 makeRoseJoin joinOnly on a viewer: the cloud\'s join (RoseStock.join), never Gate.changeMembership, no cut claimed', h.ok === true && h.cloud === true && log.join() === 'join:Michelle' && h.sheets[0].cut === false && /in Set 2 now/.test(h.why), { h, log });
    const RSoff = Object.assign({}, RS, { cloudJoins: () => false }), log2 = log.length;
    await LF.core.makeRoseJoin(() => ({ RoseStock: RSoff, Gate: G, CN: { allSheets: () => [sh] } }))('rg-m', { by: 'Michelle', joinOnly: true });
    ok('F8 ... mode off: Gate.changeMembership as before', log.slice(log2).join() === 'changeMembership', log); }
}

/* ── R: the set card's Approve (charm-nest-set-repair.js) ── */
async function partR() {
  const SRp = require(path.join(ROOT, 'charm-nest-set-repair.js'));
  const row = (id, metal, o) => Object.assign({ id, sheetId: id, metal, runId: R, draft: true, setId: null, releaseFull: false, page: 1 }, o || {});
  const st = { setId: 'set-2', seq: 2, name: 'Set-2', goldPending: [row('k14-r', 'gold14k')] };
  const page = { sheetId: 'k14-r', metal: 'gold14k', runId: R, draft: true, setId: null, roseCutAt: 1790000000000 };
  const said = [], asked = [], joined = [], cut = [];
  const env = (yes, viewer) => ({ by: () => 'Michelle', cloud: () => true, pages: () => [page], holding: () => false, say: (t, k) => said.push([t, k || 'bad']), refresh: () => {}, confirmCut: async w => { asked.push(w); return yes; },
    cut: async ids => { cut.push(ids.slice()); return ids.map(id => ({ id, cut: true })); }, full: () => false,
    cloudJoins: () => !!viewer, joinCut: async (pg, by, setId) => { joined.push({ id: pg.sheetId, by, setId }); return { ok: true, why: '14K Draft 1 is in Set 2 now. Its green line was cut already, so nothing was cut again' }; } });
  let r = await SRp.cutPending(st, 'k14-r', { env: env(false, true) });
  ok('R1 viewer: Approve on a cut sheet opens its own window (join, nothing cut again); Cancel changes nothing', r.cancelled === true && !joined.length && !cut.length && /^Approve /.test(asked[0].title) && asked[0].lines.some(l => /Nothing is cut again/.test(l.text)), { r, w: asked[0] });
  r = await SRp.cutPending(st, 'k14-r', { env: env(true, true) });
  ok('R2 ... with the yes: the cloud\'s join (who, the set), never the Cut Sheet press; the card says the cloud\'s answer', r.ok === true && r.joined === true && joined.length === 1 && joined[0].by === 'Michelle' && joined[0].setId === 'set-2' && !cut.length && /in Set 2 now/.test(r.words) && said.some(s => /in Set 2 now/.test(s[0]) && s[1] === 'ok'), { r, joined, cut, said });
  cut.length = 0; joined.length = 0;
  r = await SRp.cutPending(st, 'k14-r', { env: env(true, false) });
  ok('R3 mode off: Approve is the Cut Sheet press as before (no cloud join)', !joined.length && cut.length === 1, { r, joined, cut });
}

/* ── H: the switch and the page tokens ── */
function partH() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  const on = src.indexOf('include.onchange=async e=>{'), seg = src.slice(on, src.indexOf('changeMembership(m,want,list)', on));
  ok('H1 the switch on a viewer: a cut sheet a committed set lost is asked of the cloud (its place back), before the route that said "Its set is fixed."; the press of every other sheet as before',
    /if\(cloudPress\(sh\)&&e\.target\.checked&&sh\.roseCutAt&&!inSetNow\(sh\)&&!sh\.recalled&&committedSheet\(sh\)\)return cloudInclude\(sh,node,true\);/.test(seg) && seg.indexOf('committedSheet(sh))return cloudInclude') < seg.indexOf('if(cloudPress(sh)){') && seg.indexOf('if(cloudPress(sh)){') > 0);
  ok('H2 charm-nest-1.html loads the changed page files under new tokens (-g4)', ['charm-nest-rose-ui.js', 'charm-nest-flow.js', 'charm-nest-set-repair.js', 'charm-nest-bridge.js'].every(f => new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-g4(?:-\\w+)*"').test(html)));
}

(async () => {
  await partS();
  await partP();
  await partF();
  await partR();
  partH();
  log0(failed ? `FAIL viewer-g4 (${failed})` : 'PASS viewer-g4');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-g4: ' + (e && e.stack || e)); process.exit(1); });
