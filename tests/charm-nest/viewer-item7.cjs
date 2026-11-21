// VIEWINCLUDE (item 7, Paul 21 Nov 2026): the 10K / 14K "In current set" switch pressed on a staff computer (a viewer) in mode "on" answered "Changed on the computer that carries the run",
// and no computer carries a run since the cloud does: the switch could never be used. Now the press is ONE ask to the cloud (roseCutAsk kind include / exclude, lane "rosecut") and the cloud
// does it by the set rules (_charmNestGreenLine.js member: the sheet's own yes, then a look at the sets by charm-nest-assembly-core.js; a take-out by cmdLeaveSet).
//   S  offline (_cloudFakes.cjs, mode on): the page's press is taken; the cloud includes a 14K / 10K sheet into its set, refuses a take-out the order rule forbids (in words, nothing changed),
//      takes a sheet out, puts it back, keeps an un-ready sheet's yes with the rule's words; a sheet nobody approved stays out
//   B  the bridge: a viewer's press goes to cloudInclude; mode off keeps the old path (the guard and changeMembership); the page token is bumped
// Offline only: no Etsy, no live cloud. Made-up names.   NODE_PATH=<deps> node tests/charm-nest/viewer-item7.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1500))); if (!cond) failed++; };
const T0 = Date.UTC(2026, 10, 21, 15, 0, 0), CV = 'cloud14-20261120', PAGE = 'd0michelle1';
const R = 'run-2026-11-21-item7', SET = 'set-2026-11-21-1', DAY = '2026-11-21';

async function partS() {
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  const out = [];
  try {
    const lib = F.load('charmNestLibrary.js');
    const Green = F.load('_charmNestGreenLine.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const pid = (rid, n) => `${rid}_${rid}${n}_1`;
    const sheet = (id, metal, rid, o) => {
      const p = pid(rid, 1), c = { id: 'c-' + p, poolId: p, order: rid, areaPt2: 400, widthPt: 20, heightPt: 20 };
      F.seed('Charm_Pool/' + p, { poolId: p, runId: R, orderId: rid, state: 'placed', sheetId: id, material: metal });
      F.seed('Charm_Nest_Sheets/' + id, Object.assign({ id, runId: R, metal, page: 1, day: DAY, draft: true, setId: null, setSeq: null, sheetIndex: null, status: 'complete', archived: false,
        charms: [c], placements: [{ id: c.id, cxPt: 20, cyPt: 20, angle: 0 }], poolIds: [p], orders: [rid], outputs: { ai: { path: `x/${id}.ai` } }, verification: { ok: true }, releaseFull: false,
        fileBase: `${metal}_working_${id}`, layoutRev: 1, cardStartedAt: T0 - 3600e3 }, o || {}));
    };
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: ['rosecut', 'labels', 'cmd:leaveset'], flippedAt: T0 - 3600e3 }); lib._driveForget();
    const run = { runId: R, releasePolicy: 2, status: 'running', step: 'nest', mode: 'auto', day: DAY, solidIncluded: {}, lines: {}, sheets: {}, holds: {}, errors: [], orders: [] };
    F.seed('Charm_Nest_Runs/' + R, run);
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
    F.seed('Charm_Nest_Sets/' + SET, { setId: SET, runId: R, day: DAY, seq: 1, name: 'Set-1', folder: `charmnest/sets/${DAY}/Set-1`, group: 'dispatch', status: 'nesting', formedAt: T0 - 3600e3, sheetIds: ['gf-s1-it7', 'ss-s1-it7'], materials: ['gold', 'silver'], orders: {}, sheetNos: {}, labelFiles: [] });
    sheet('gf-s1-it7', 'gold', '4250000751', { draft: false, setId: SET, setSeq: 1, sheetIndex: 1, releaseFull: true, fileBase: 'GF_Nov.21.26_Set-1_Sheet-1' });
    sheet('ss-s1-it7', 'silver', '4250000752', { draft: false, setId: SET, setSeq: 1, sheetIndex: 1, releaseFull: true, fileBase: 'SS_Nov.21.26_Set-1_Sheet-1' });
    sheet('k14-s1-it7', 'gold14k', '4250000751');                 // (shares order 751 with GF1: its ONE set is Set 1)
    sheet('k14-s2-it7', 'gold14k', '4250000752');                 // (nobody approves it: it stays out)
    F.seed('Charm_Pool/' + pid('4250000751', 2), { poolId: pid('4250000751', 2), runId: R, orderId: '4250000751', state: 'placed', sheetId: 'k14-s1-it7', material: 'gold14k' });
    { const k1 = F.doc('Charm_Nest_Sheets/k14-s1-it7'); k1.charms[0].poolId = pid('4250000751', 2); k1.charms[0].id = 'c-' + k1.charms[0].poolId; k1.placements[0].id = k1.charms[0].id; k1.poolIds = [k1.charms[0].poolId]; F.seed('Charm_Nest_Sheets/k14-s1-it7', k1); }
    { const k2 = F.doc('Charm_Nest_Sheets/k14-s2-it7'); k2.charms[0].poolId = pid('4250000752', 2); k2.charms[0].id = 'c-' + k2.charms[0].poolId; k2.placements[0].id = k2.charms[0].id; k2.poolIds = [k2.charms[0].poolId]; F.seed('Charm_Nest_Sheets/k14-s2-it7', k2); F.seed('Charm_Pool/' + k2.poolIds[0], { poolId: k2.poolIds[0], runId: R, orderId: '4250000752', state: 'placed', sheetId: 'k14-s2-it7', material: 'gold14k' }); }
    sheet('k10-s1-it7', 'gold10k', '4250000790');                 // (an order of its own)
    sheet('k10-s2-it7', 'gold10k', '4250000791', { verification: { ok: false } });   // (not verified yet: its yes is kept, it waits)
    let pass = 0;
    const cloud = async () => { pass++; const ctx = { db: F.db, run: Object.assign({}, F.doc('Charm_Nest_Runs/' + R), { runId: R }), runId: R, budgetEnd: Date.now() + 600000, cloudSteps: ['rosecut'], say: () => {},
      call: (b, u) => lib.call(b, { unit: `item7|${u}|pass${pass}` }), read: b => lib.call(b, {}) }; return Green.askUnit(ctx); };
    const press = async (id, kind) => { const r = await P({ op: 'roseCutAsk', sheetId: id, kind, by: 'Michelle', dev: PAGE, cv: CV }); await cloud(); return { r, ask: F.doc('Charm_Nest_Cut_Asks/' + id) }; };
    const rec = id => F.doc('Charm_Nest_Sheets/' + id), inSet = id => (F.doc('Charm_Nest_Sets/' + SET).sheetIds || []).includes(id);

    let x = await press('k14-s1-it7', 'include');
    out.push(['S1 a viewer\'s Include on a 14K sheet is taken in mode on (a person\'s press) and the cloud puts it in its ONE set (Set 1), with its yes; the answer says so',
      x.r.http === 200 && x.r.ok && x.ask.state === 'done' && /in Set 1 now/.test(x.ask.why) && rec('k14-s1-it7').setId === SET && rec('k14-s1-it7').draft === false && rec('k14-s1-it7').solidIncluded === true && inSet('k14-s1-it7'), { r: x.r, ask: x.ask, k1: { setId: rec('k14-s1-it7').setId, draft: rec('k14-s1-it7').draft, si: rec('k14-s1-it7').solidIncluded } }]);
    out.push(['S2 a 14K sheet nobody approved stays out of the set (greyed until a person approves it)', !inSet('k14-s2-it7') && rec('k14-s2-it7').draft === true, { k2: rec('k14-s2-it7').setId }]);
    x = await press('k14-s1-it7', 'exclude');
    out.push(['S3 taking K1 out is refused by the order rule (it shares an order with GF1, which stays), in words; nothing changed', x.ask.state === 'refused' && /shared with another sheet/.test(x.ask.why) && inSet('k14-s1-it7') && rec('k14-s1-it7').draft === false, x.ask]);
    x = await press('k10-s1-it7', 'include');
    out.push(['S4 a 10K sheet with an order of its own joins the open set on its Include', x.ask.state === 'done' && inSet('k10-s1-it7') && rec('k10-s1-it7').setId === SET && rec('k10-s1-it7').solidIncluded === true, { ask: x.ask, t1: rec('k10-s1-it7').setId }]);
    x = await press('k10-s1-it7', 'exclude');
    out.push(['S5 its take-out: out of Set 1, a draft, marked taken out, its yes gone; the answer says so', x.ask.state === 'done' && /taken out of Set 1/.test(x.ask.why) && !inSet('k10-s1-it7') && rec('k10-s1-it7').draft === true && rec('k10-s1-it7').leftSet === SET && rec('k10-s1-it7').solidIncluded === false, { ask: x.ask, t1: rec('k10-s1-it7') && { d: rec('k10-s1-it7').draft, l: rec('k10-s1-it7').leftSet, s: rec('k10-s1-it7').solidIncluded } }]);
    x = await press('k10-s1-it7', 'include');
    out.push(['S6 switched on again: the take-out mark cleared, it is back in Set 1', x.ask.state === 'done' && inSet('k10-s1-it7') && rec('k10-s1-it7').setId === SET && !rec('k10-s1-it7').leftSet, { ask: x.ask, l: rec('k10-s1-it7').leftSet }]);
    x = await press('k10-s2-it7', 'include');
    out.push(['S7 an un-verified 10K sheet: its yes is kept and the answer gives the rule\'s words; it is in no set', x.ask.state === 'done' && /included: it joins its set once/.test(x.ask.why) && /Nest and verify first/.test(x.ask.why) && rec('k10-s2-it7').solidIncluded === true && !inSet('k10-s2-it7') && rec('k10-s2-it7').draft === true, x.ask]);
    x = await press('k10-s2-it7', 'exclude');
    out.push(['S8 ... and switched off: its yes is gone, said in words', x.ask.state === 'done' && /is not included/.test(x.ask.why) && rec('k10-s2-it7').solidIncluded === false, x.ask]);
    const bad = await P({ op: 'roseCutAsk', sheetId: 'k14-s2-it7', kind: 'move', by: 'Michelle', dev: PAGE, cv: CV });
    out.push(['S9 an unknown kind is refused', bad.http >= 400 || !!bad.error, bad]);
    F.seed('Charm_Nest_Sheets/k14-s2-it7', Object.assign(rec('k14-s2-it7'), { roseCutAt: T0 }));
    const cut = await P({ op: 'roseCutAsk', sheetId: 'k14-s2-it7', kind: 'cut', by: 'Michelle', dev: PAGE, cv: CV });
    out.push(['S10 a Cut Sheet ask on a cut sheet is refused as before (unchanged)', !!cut.error && cut.cut === true, cut]);
  } catch (e) { out.push(['S run', false, String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')]); }
  finally { console.log = quiet; console.warn = qw; }
  for (const [n, c, d] of out) ok(n, c, d);
}

function partB() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  const on = src.indexOf('include.onchange=async e=>{'), seg = src.slice(on, src.indexOf('changeMembership(m,want,list)', on) + 40);
  ok('B1 the switch: a viewer\'s press (cloudPress) is asked of the cloud before the old guard; mode off keeps the guard and the run\'s own Include', seg.indexOf('if(cloudPress(sh))') > 0 && seg.indexOf('return cloudInclude(sh,node,want)') > seg.indexOf('if(cloudPress(sh))') && seg.indexOf('Changed on the computer that carries the run') > seg.indexOf('return cloudInclude') && /changeMembership\(m,want,list\)/.test(seg));
  ok('B2 cloudPress is a viewer only: no run held, the sheet of a run, Rose Gold / 10K / 14K, CNViewer.on()', /const cloudPress = sh => !B\.run && !!sh\.runId && goldFamily\(sh\.metal\) && !!\(window\.CNViewer && typeof CNViewer\.on === "function" && CNViewer\.on\(\)\);/.test(src));
  ok('B3 cloudInclude sends roseCutAsk include / exclude and shows the cloud\'s answer (done or refused), never a silent done', /op: "roseCutAsk", sheetId: sh\.sheetId, kind: want \? "include" : "exclude"/.test(src) && /a\.state === "refused"\) return done\(/.test(src) && /The cloud has not answered yet/.test(src));
  ok('B4 charm-nest-1.html loads the changed bridge under a new token', /charm-nest-bridge\.js\?v=[^"]*-vi7(?:-\w+)*"/.test(html));
}

(async () => {
  await partS();
  partB();
  console.log(failed ? `FAIL viewer-item7 (${failed})` : 'PASS viewer-item7');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL viewer-item7: ' + (e && e.stack || e)); process.exit(1); });
