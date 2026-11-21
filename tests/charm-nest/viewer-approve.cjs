// VIEWAPPROVE (Paul, 21 Nov 2026, the morning after the cloud took every run: Michelle's Approve on Design & Engrave said "Not approved: the approval could not be saved on this browser"
// five times, and her None / Front / Back choice said "Your choice is saved, but the placements could not all be updated: The cloud carries every run now: poolUpdate ...". "Why is this happening?")
//   Why: a viewer keeps no workspace (Session.freeze) and the cloud refuses a page's production writes. The engraving lane (charm-nest-engrave-cloud.js) looked for the run only on the
//   computer that HOLDS it (B.run), which no computer does once the cloud carries every run, so Approve never went to the cloud and fell to the browser's own save; and a person's choice
//   on a face (its engrave flag, its approved back withdrawn) was refused as if it were the run's work.
//   S  the library in mode "on" (offline, _cloudFakes.cjs): a current page's poolUpdate carrying ONLY a face's engrave flag and who decided it, and its backInvalidate, are taken; every
//      other poolUpdate (a state, a sheet, a flag with anything else, a flag of the wrong kind) is still refused in words; an old page still gets the reload words; mode off unchanged
//   P  the lane on a viewer (charm-nest-engrave-cloud.js in a bare context): no B.run, the run followed (CharmNestRunFollow) carried by the cloud with "engrave" on: lane() and carries()
//      say the cloud; a viewer that has not looked at the run yet looks once; a run a computer carries, or the drive off, is not the cloud's
//   E  the REAL page over the fake site (_engraveRealWorld.cjs), made a viewer after its orders are in (CNViewer on, Session frozen, no run held, the run followed, the cloud carrying it,
//      mode "on" on the fake site): Approve is sent to the cloud (a real engraveApprove task in the fake cloud), never "could not be saved on this browser"; None on another face is taken
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-approve.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1200))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle1';

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';   // (the offline library's door; never set while the real page of part E talks to its fake site)
  const T0 = Date.UTC(2026, 10, 21, 13, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const R = 'run-2026-11-21-viewap', P1 = '4241400132_42414001321_1', P2 = '4241071527_42410715271_1', SH = 'gold-s40-viewap';
    const seed = mode => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: ['engrave', 'files'], flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [] });
      F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
      F.seed('Charm_Pool/' + P1, { poolId: P1, runId: R, orderId: '4241400132', state: 'placed', sheetId: SH, engrave: true });
      F.seed('Charm_Pool/' + P2, { poolId: P2, runId: R, orderId: '4241071527', state: 'placed', sheetId: SH, engrave: true });
      F.seed('Charm_Pool_Back/' + P2, { poolId: P2, sheetId: SH, runId: R });
      F.seed('Charm_Nest_Sheets/' + SH, { id: SH, runId: R, metal: 'gold', backPool: [{ poolId: P2 }, { poolId: '4241000000_42410000001_1' }], placements: [], charms: [] });
    };
    seed('on');
    const carries = r => r.http === 409 && r.cloudCarries === true && /The cloud carries every run now/.test(r.error || '') && !r.reload;
    let r = await P({ op: 'poolUpdate', poolIds: [P1], patch: { engrave: false, engraveSkippedBy: 'Michelle' }, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S1 mode on: a viewer\'s cut-plain (engrave false, who decided) is taken, and the piece says so', r.http === 200 && r.ok && F.doc('Charm_Pool/' + P1).engrave === false && F.doc('Charm_Pool/' + P1).engraveSkippedBy === 'Michelle' && F.doc('Charm_Pool/' + P1).state === 'placed', { r, row: F.doc('Charm_Pool/' + P1) });
    r = await P({ op: 'poolUpdate', poolIds: [P1], patch: { engraveFront: true }, dev: PAGE, cv: CV });
    ok('S2 ... a face opened again (engraveFront true) is taken', r.http === 200 && F.doc('Charm_Pool/' + P1).engraveFront === true, r);
    r = await P({ op: 'poolUpdate', poolIds: [P1], patch: { engrave: false, state: 'abandoned', sheetId: null }, dev: PAGE, cv: CV });
    ok('S3 ... a flag sent with a state or a sheet is the run\'s work: refused in words, nothing written', carries(r) && F.doc('Charm_Pool/' + P1).state === 'placed' && F.doc('Charm_Pool/' + P1).sheetId === SH, { r, row: F.doc('Charm_Pool/' + P1) });
    r = await P({ op: 'poolUpdate', poolIds: [P1], patch: { state: 'engraved', engraveApprovedBy: 'Michelle' }, dev: PAGE, cv: CV });
    ok('S4 ... an approval\'s own write (state engraved, approved by) stays the cloud\'s: refused', carries(r) && F.doc('Charm_Pool/' + P1).state === 'placed', r);
    r = await P({ op: 'poolUpdate', poolIds: [P1], patch: { engrave: 'no' }, dev: PAGE, cv: CV });
    ok('S5 ... a flag of the wrong kind is refused', carries(r), r);
    r = await P({ op: 'poolUpdate', poolIds: [P1], patch: {}, dev: PAGE, cv: CV });
    ok('S6 ... an empty patch is not a decision: refused', carries(r), r);
    r = await P({ op: 'backInvalidate', poolIds: [P2], dev: PAGE, cv: CV });
    const sh = F.doc('Charm_Nest_Sheets/' + SH);
    ok('S7 ... a face taken away (None) withdraws its approved back: the back record says invalidated and the sheet\'s back list drops that piece only', r.http === 200 && r.ok && F.doc('Charm_Pool_Back/' + P2).invalidated === true && sh.backPool.length === 1 && sh.backPool[0].poolId === '4241000000_42410000001_1', { r, back: F.doc('Charm_Pool_Back/' + P2), backPool: sh.backPool });
    r = await P({ op: 'poolUpdate', poolIds: [P1], patch: { engrave: true }, dev: PAGE });
    ok('S8 ... an old page (no cv) still gets the reload words, even for a flag', r.http === 409 && r.reload === true, r);
    r = await P({ op: 'putSheet', sheet: { id: SH, runId: R, metal: 'gold', placements: [] }, dev: PAGE, cv: CV });
    ok('S9 ... a sheet\'s pieces stay the cloud\'s (putSheet refused as before)', carries(r), r);
    r = await P({ op: 'timelineAdd', events: [{ id: 'va-note-1', orderId: '4241400132', type: 'note', at: T0, by: 'Michelle', station: 'sorter', text: 'Back engraving approved' }], dev: PAGE, cv: CV });
    const tl = await P({ op: 'timelineGet', orderId: '4241400132' });
    ok('S11 mode on: a person\'s step on an order\'s timeline (from a viewer) is taken and kept', r.http === 200 && !r.cloudCarries && (tl.events || []).some(e => e.id === 'va-note-1' || (e.by === 'Michelle' && e.at === T0)), { r, events: (tl.events || []).map(e => [e.id, e.type, e.by]) });
    r = await P({ op: 'timelineAdd', events: [{ id: 'va-note-2', orderId: '4241400132', type: 'note', at: T0, by: 'Michelle' }], dev: PAGE });
    ok('S12 ... from an old page (no cv) it still gets the reload words', r.http === 409 && r.reload === true, r);
    seed('off');
    r = await P({ op: 'poolUpdate', poolIds: [P1], patch: { engrave: false, state: 'abandoned', sheetId: null }, dev: PAGE, cv: CV });
    ok('S10 drive off: nothing is refused (every page works as before the cloud)', r.http === 200 && F.doc('Charm_Pool/' + P1).state === 'abandoned', r);
  } finally { console.log = quiet; console.warn = qw; }
}

async function partP() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-engrave-cloud.js'), 'utf8');
  const R = 'run-2026-11-21-viewap';
  function page(o) {
    const calls = [];
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout, clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, Date, JSON };
    w.window = w;
    w.B = o.held ? { run: { runId: o.held } } : {};
    let followed = o.followed || null;
    w.CharmNestRunFollow = { state: () => ({ runId: followed }), dev: () => PAGE, now: async why => { calls.push({ op: 'follow.now', why }); followed = o.later || followed; return {}; } };
    if (o.viewer) w.CNViewer = { on: () => true };
    w.CN = { api: async (fn, body) => {
      calls.push(body);
      if (body.op === 'runCarrier') return { ok: true, mine: false, alive: true, carrier: { dev: o.carrier || 'cloud', by: 'Cloud', at: Date.now() } };
      if (body.op === 'driveGet') return { ok: true, mode: o.mode || 'on', drive: { mode: o.mode || 'on', cloudSteps: o.steps || ['engrave', 'files'] } };
      return { ok: true };
    } };
    vm.runInContext(src, vm.createContext(w));
    return { w, calls };
  }
  { const { w, calls } = page({ viewer: true, followed: R });
    const lane = await w.CharmNestEngraveCloud.lane();
    const rc = calls.find(c => c.op === 'runCarrier');
    ok('P1 a viewer (no run held, the run followed, the cloud carrying it, "engrave" on): the lane is the cloud\'s', lane === true && w.CharmNestEngraveCloud.carries(R) === true && rc && rc.runId === R && rc.check === true && rc.dev === PAGE, { lane, calls }); }
  { const { w, calls } = page({ viewer: true, followed: null, later: R });
    const lane = await w.CharmNestEngraveCloud.lane();
    ok('P2 a viewer that has not looked at the run yet looks once, then the lane is the cloud\'s', lane === true && calls.some(c => c.op === 'follow.now' && c.why === 'approve'), { lane, calls }); }
  { const { w } = page({ viewer: false, followed: R, carrier: 'd0paulpc01', mode: 'off' });
    ok('P3 drive off, a computer carrying the run: not the cloud\'s (the page approves as before)', (await w.CharmNestEngraveCloud.lane()) === false && w.CharmNestEngraveCloud.carries(R) === false); }
  { const { w } = page({ viewer: true, followed: R, steps: ['files'] });
    ok('P4 the cloud carrying the run but "engrave" not one of its lanes: not the cloud\'s', (await w.CharmNestEngraveCloud.lane()) === false); }
  { const { w } = page({ held: R });
    ok('P5 the computer that holds the run (B.run) finds it as before', (await w.CharmNestEngraveCloud.lane()) === true); }
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('P6 charm-nest-1.html loads the changed lane and bridge under new tokens', /charm-nest-engrave-cloud\.js\?v=1121-cloud11-va1[-\w]*"/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-va1[-\w]*"/.test(html));
}

async function partE() {
  let Rw; try { Rw = require('./_engraveRealWorld.cjs'); } catch (e) { ok('E world', false, String(e)); return; }
  const w = await Rw.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { log0('E: no playwright-core: the real page was not run'); return; }
  const page = w.page, st = w.srv.st;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
  const Q = '#egQueue .rvItem[data-kind=placement]';
  try {
    const made = await Rw.build(w, ['back', 'both']);
    const key = await Rw.openEngraveDetail(w, 'back', { face: 'back' }); await Rw.settle(page, 600);
    const R = 'run-2026-11-21-viewap';
    // the fake site: the cloud carries this run, mode "on", "engrave" one of its lanes
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'manual', cloudSteps: ['engrave', 'files', 'takeoff'], flippedAt: Date.now() - 3600e3 });
    st.put('Charm_Nest_Runs', R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [] });
    st.put('Charm_Nest_Run_Carrier', R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now() + 3600e3, since: Date.now() - 3600e3 } });
    for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {}
    // the page is a viewer now: no run held, the run followed, its workspace frozen
    const before = await page.evaluate(R => {
      window.__heldRun = B.run ? B.run.runId : null; B.run = null;
      const F = window.CharmNestRunFollow, st0 = F.state; F.state = () => Object.assign(st0(), { runId: R });
      window.CNViewer = window.CNViewer || {}; window.__viewerOn = CNViewer.on; CNViewer.on = () => true;
      Session.freeze();
      window.__said = []; const t0 = window.toast; window.toast = function (t) { window.__said.push(String(t)); return t0 && t0.apply(this, arguments); };
      return { frozen: Session.frozen(), held: window.__heldRun };
    }, R);
    ok('E0 the page is a viewer: its workspace frozen, no run held', before.frozen === true, before);
    const press = await page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approve"]'); if (!b) return 'none'; if (b.disabled) return 'dimmed'; b.click(); return 'pressed'; }, Q);
    const task = await until(async () => { const snap = await st.db.collection('Charm_Engrave_Files').where('jobKey', '==', key).get(); return snap.docs.length ? snap.docs[0].data() : null; }, 20000);
    const facts = await page.evaluate(({ key, Q }) => { const j = Engrave.items().get(key), c = document.querySelector(Q), why = c && c.querySelector('.egApproveWhy');
      return { state: j && j.state, approving: !!(j && j.approving && j.approving.cloud), why: why && !why.hidden ? why.textContent.trim() : '', said: window.__said.slice() }; }, { key, Q });
    const browserSave = facts.said.concat([facts.why]).some(t => /could not be saved on this browser/.test(t));
    ok('E1 Approve on the viewer is sent to the cloud: a real engraveApprove task for this face, the face waits on it, never "could not be saved on this browser"',
      press === 'pressed' && !!task && task.runId === R && task.by && facts.approving && !browserSave, { press, task: task && { runId: task.runId, by: task.by, state: task.state }, facts });
    // a person's choice on the other order's back face (cut plain): the page's own Pool.update reaches the fake cloud in mode "on" and is taken; the run's own write is still refused there
    const other = (made.both && made.both.jobs || []).find(j => j.face === 'back') || null;
    const res = await page.evaluate(async k => {
      const j = Engrave.items().get(k), ids = (j && j.copies) || []; if (!ids.length) return { none: 'no pieces' };
      const out = { ids };
      try { await Pool.update(ids, { engrave: false, engraveSkippedBy: 'Test Operator' }); out.flag = 'taken'; } catch (e) { out.flag = String(e && e.message || e); }
      try { await Pool.update(ids, { state: 'abandoned' }); out.state = 'taken'; } catch (e) { out.state = String(e && e.message || e); }
      return out;
    }, other ? other.key : '');
    const rows = res.ids ? await Promise.all(res.ids.map(async id => { const d = await st.db.collection('Charm_Pool').doc(id).get(); return d.exists ? d.data() : null; })) : [];
    ok('E2 the viewer\'s cut-plain reaches the cloud in mode on (the pieces say engrave false, by whom), and the run\'s own write from the page is still refused in words',
      res.flag === 'taken' && rows.length > 0 && rows.every(r => r && r.engrave === false && r.engraveSkippedBy === 'Test Operator') && /The cloud carries every run now/.test(String(res.state)), { res, rows: rows.map(r => r && { engrave: r.engrave, by: r.engraveSkippedBy, state: r.state }) });
    await page.evaluate(() => { CNViewer.on = window.__viewerOn; });
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  ok('E no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
  await w.close();
}

(async () => {
  // (parts S and P in a process of their own: the fake site of part E and the offline cloud of part S each load the library with their own database, and one process keeps one copy of it)
  if (process.env.VA_PART === 'offline') { await partS(); await partP(); }
  else {
    if (!process.env.SKIP_PAGE) await partE();
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { VA_PART: 'offline' }), encoding: 'utf8', timeout: 180000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) /.test(l)) log0(l);
    if (r.status !== 0) failed++;
  }
  log0(failed ? `FAIL viewer-approve (${failed})` : 'PASS viewer-approve');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-approve: ' + (e && e.stack || e)); process.exit(1); });
