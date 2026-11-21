// ITEM 1 (Paul, 21 Nov 2026): Cancel Order on an order with pieces already on a sheet, pressed on a staff computer (a viewer) while the cloud carries every run (mode "on"), only said
// "A cancelled order is taken off its sheets by the cloud (its cancel is recorded)" and recorded nothing: CNViewer.takeOff returned ok for mode "cancel" without sending cancelPut, so the
// order stayed open and its pieces stayed on the sheet. Now the viewer records the cancel with exactly the record the page's own path keeps (the sheet window's cancelRecOf: who, why, the
// moment pressed, the order), and the cloud's take-off lane, which reads every cancel written since its cursor, takes the pieces off.
//   E  the REAL page over the fake site (bridge-server.cjs: the real library over an in-memory Firestore). First with the drive off (mode off: the page's own path, unchanged, and CNViewer
//      never asked), then the same page booted again with Charm_Nest_Open/drive mode "on" and "takeoff" one of the cloud's lanes (a real viewer: CNViewer.on(), Session frozen, no run held):
//      SheetWin.takeOffOrder({ mode: "cancel" }) (what Cancel Order calls) records the cancel in the fake cloud with the same record as mode off, says so in words, and the page's
//      Cancelled list has it; the lane not on: refused in words, nothing recorded. The cancelPut body the real page sent is handed to part S.
//   P  charm-nest-viewer.js in a bare context: the record is sent as given; a refusal from the cloud is said and answered ok:false; no name / lane off: refused, nothing sent; the cloud's
//      take-off journal is followed to its end and said (done with the sheets; held with the reason).
//   S  the offline cloud (_cloudFakes.cjs): mode "on", the run carried by the cloud, the order's pieces on a filling sheet: the EXACT cancelPut body the real page sent is taken from a
//      current page, wakes the driver, and the take-off lane takes the pieces off (the sheet, the pool rows, the run line gone "cancelled by <who>", its journal job done, fates on the record).
// Offline only: no Etsy, no live write, made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item1.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1200))); if (!cond) failed++; };
const CV = 'cloud14-20261120';
const RID = '4195000011', RID_OFF = '4195000016', RID_LANE = '4195000017';
const shape = b => b && { keys: Object.keys(b).filter(k => !['op', 'cv', 'dev'].includes(k)).sort(), record: Object.keys(b.record || {}).sort(), line: Object.keys(((b.record || {}).lines || [])[0] || {}).sort() };

/* ═══ E · the real page ═══ */
async function partE() {
  const OF = require('./_orderFactsWorld.cjs'), { start } = require('./bridge-server.cjs');
  let chromium; try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(ROOT, 'node_modules'), 'playwright-core'))); } catch (_) { log0('E: no playwright-core: the real page was not run'); return null; }
  const srv = await start({ receipts: [] }), st = srv.st;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await ctx.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; window.confirm = () => true; });
  const page = await ctx.newPage(), errors = [];
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
  const forget = () => { for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {} };
  const puts = rid => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === 'cancelPut' && String(c.body.orderId) === rid);
  const boot = async () => {
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.SheetWin && window.Cancelled && window.CNViewer && CNViewer.known() && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await OF.seed(page, ['single', 'late', 'message']);
    await page.evaluate(() => { window.__said = []; const t0 = window.toast; window.toast = function (t) { window.__said.push(String(t)); return t0 && t0.apply(this, arguments); }; });
  };
  let body = null;
  try {
    /* mode off: the page's own path, as before */
    await boot();
    const off = await page.evaluate(async rid => {
      let asked = 0; const t0 = CNViewer.takeOff; CNViewer.takeOff = function () { asked++; return t0.apply(this, arguments); };
      const res = await SheetWin.takeOffOrder({ orderId: rid, mode: 'cancel', scope: 'order', by: 'Test Operator', note: 'buyer asked' });
      CNViewer.takeOff = t0;
      return { mode: CNViewer.mode(), on: CNViewer.on(), asked, res, has: Cancelled.has(rid), left: Orders.rows().filter(r => String(r.order.receiptId) === rid).length };
    }, RID_OFF);
    const offPut = puts(RID_OFF), offRec = st.doc('Charm_Nest_Cancelled', RID_OFF);
    ok('E1 mode off: Cancel Order takes the page\'s own path (CNViewer never asked), records the cancel once and takes the order off every list, as before',
      off.mode === 'off' && !off.on && off.asked === 0 && off.res && off.res.ok && offPut.length === 1 && !!offRec && offRec.by === 'Test Operator' && offRec.why === 'buyer asked' && off.has && off.left === 0, { off, puts: offPut.length, rec: offRec && { by: offRec.by, why: offRec.why } });

    /* mode on: the same page booted as a viewer */
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'manual', cloudSteps: ['pull', 'claim', 'pool', 'plan', 'nest', 'checkpoint', 'engrave', 'revalidate', 'labels', 'commit', 'complete', 'files', 'takeoff', 'rosecut', 'sweeps'], flippedAt: Date.now() - 3600e3 });
    forget();
    await boot();
    const v = await page.evaluate(() => ({ mode: CNViewer.mode(), on: CNViewer.on(), frozen: !!(window.Session && Session.frozen && Session.frozen()), run: !!(window.B && B.run), lane: CNViewer.lane('takeoff') }));
    ok('E2 the page boots as a viewer in mode on (its workspace frozen, no run held, "takeoff" the cloud\'s)', v.mode === 'on' && v.on && v.frozen && !v.run && v.lane, v);
    const pressedAt = Date.now();
    const on = await page.evaluate(async rid => { window.__said.length = 0; const res = await SheetWin.takeOffOrder({ orderId: rid, mode: 'cancel', scope: 'order', by: 'Test Operator', note: 'buyer asked' }); return { res, said: window.__said.slice(), has: Cancelled.has(rid) }; }, RID);
    const onPut = puts(RID), rec = st.doc('Charm_Nest_Cancelled', RID);
    body = onPut.length ? onPut[0].body : null;
    ok('E3 mode on: Cancel Order on the viewer RECORDS the cancel in the cloud (one cancelPut from a current page): who, why, the moment pressed, the order\'s lines',
      on.res && on.res.ok === true && on.res.via === 'cloud' && onPut.length === 1 && body.cv === CV && !!rec && rec.by === 'Test Operator' && rec.why === 'buyer asked' && Math.abs(+rec.at - pressedAt) < 15000 && Array.isArray(rec.lines) && rec.lines.length === 1 && rec.lines[0].transactionId === RID + '1',
      { res: on.res, puts: onPut.length, rec });
    ok('E4 ... with exactly the record the page\'s own path keeps in mode off (the same fields, the same line fields)', JSON.stringify(shape(body)) === JSON.stringify(shape(offPut[0] && offPut[0].body)) && body.record.buyer === 'Alder Fixture', { on: shape(body), off: shape(offPut[0] && offPut[0].body) });
    ok('E5 ... and the person is told the truth: cancelled, in Orders › Cancelled, the cloud takes its pieces off (never "nothing more is needed")', on.has && on.said.some(t => /Order 4195000011 cancelled: its record is under Orders › Cancelled, and the cloud takes/.test(t)) && !on.said.some(t => /nothing more is needed/.test(t)), on.said);
    // the lane not on: refused in words, nothing recorded
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'manual', cloudSteps: ['pull', 'files'], flippedAt: Date.now() - 3600e3 }); forget();
    const no = await page.evaluate(async rid => { await CNViewer.read('test'); window.__said.length = 0; const res = await SheetWin.takeOffOrder({ orderId: rid, mode: 'cancel', scope: 'order', by: 'Test Operator', note: '' }); return { res, said: window.__said.slice(), has: Cancelled.has(rid), lane: CNViewer.lane('takeoff') }; }, RID_LANE);
    ok('E6 mode on, "takeoff" not the cloud\'s: refused in words, nothing recorded (never "done" when nothing happened)', !no.lane && no.res && no.res.ok === false && puts(RID_LANE).length === 0 && !st.doc('Charm_Nest_Cancelled', RID_LANE) && no.said.some(t => /was not cancelled: the cloud does not take orders off sheets/.test(t)), no);
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  ok('E no page error', errors.length === 0, errors.slice(0, 2).join(' | '));
  await browser.close(); await srv.close();
  return body;
}

/* ═══ P · the viewer module ═══ */
async function partP() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-viewer.js'), 'utf8');
  async function viewer(o) {
    const calls = [], said = [];
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout: (f, ms) => setTimeout(f, Math.min(ms, 5)), clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, Date, JSON, performance: { now: () => 1e6 } };
    w.window = w; w.toast = (t, tone) => { said.push([String(t), tone]); };
    w.CharmNestRunFollow = { state: () => ({ runId: null }), dev: () => 'd0viewer01', now: () => {} };
    let looks = 0;
    w.CN = { api: async (fn, body) => {
      calls.push(body);
      if (body.op === 'driveGet') return { ok: true, mode: 'on', drive: { mode: 'on', cloudSteps: o.steps || ['takeoff', 'files'] } };
      if (body.op === 'cancelPut') return o.refuse ? Promise.reject(Object.assign(new Error(o.refuse), { status: 409, data: { error: o.refuse } })) : { ok: true, record: { orderId: body.orderId, at: body.at, by: body.by } };
      if (body.op === 'takeOffGet') { looks++; const job = (o.jobs || [])[Math.min(looks - 1, (o.jobs || []).length - 1)]; return { journals: { [body.orderIds[0]]: job ? { jobs: { [`cancel-${o.at}`]: job } } : null } }; }
      return { ok: true };
    } };
    vm.runInContext(src, vm.createContext(w));
    await w.CNViewer.ready();
    return { w, calls, said };
  }
  const at = Date.UTC(2026, 10, 21, 14, 0, 0), rec = { orderId: RID, by: 'Test Operator', why: 'buyer asked', at, record: { buyer: 'Alder Fixture', placedAt: at - 864e5, shipBy: at + 864e5, sheets: ['GF Sheet 2'], lines: [{ transactionId: RID + '1', sku: 'LEAF_CHARM', title: 't', quantity: 1, material: 'gold' }] } };
  const wait = async (fn, ms) => { const t0 = Date.now(); while (!fn() && Date.now() - t0 < (ms || 3000)) await new Promise(r => setTimeout(r, 10)); return fn(); };
  { const { w, calls, said } = await viewer({ at, jobs: [{ status: 'working' }, { status: 'done', taken: [{ where: 'GF Sheet 2' }], left: [] }] });
    const r = await w.CNViewer.takeOff({ orderId: RID, mode: 'cancel', by: 'Test Operator', note: 'buyer asked', cancelRec: rec });
    const put = calls.find(c => c.op === 'cancelPut');
    ok('P1 the cancel is sent as the record given (cancelPut: who, why, when pressed, the order), and answered ok in takeOffOrder\'s shape', r.ok === true && r.mode === 'cancel' && r.at === at && !!put && JSON.stringify(Object.assign({}, put, { op: undefined })) === JSON.stringify(Object.assign({}, rec, { op: undefined })), { r, put });
    await wait(() => said.some(s => /the cloud took its pieces off GF Sheet 2/.test(s[0])));
    ok('P2 ... then the cloud\'s take-off is followed (takeOffGet, job cancel-<at>) and said when done, naming the sheet', said.some(s => /Order 4195000011 cancelled/.test(s[0])) && said.some(s => /Order 4195000011: the cloud took its pieces off GF Sheet 2/.test(s[0]) && s[1] === 'ok'), said); }
  { const { w, said } = await viewer({ at, jobs: [{ status: 'held', why: 'RG Sheet 1: its green line keeps a group' }] });
    await w.CNViewer.takeOff({ orderId: RID, mode: 'cancel', by: 'Test Operator', cancelRec: rec });
    await wait(() => said.some(s => /could not take its pieces off/.test(s[0])));
    ok('P3 a take-off the cloud holds is said with its reason', said.some(s => /Order 4195000011 is cancelled, but the cloud could not take its pieces off: RG Sheet 1: its green line keeps a group/.test(s[0]) && s[1] === 'bad'), said); }
  { const { w, calls, said } = await viewer({ refuse: 'This page is an old version: it reloads, then you can do this again. Nothing was changed' });
    const r = await w.CNViewer.takeOff({ orderId: RID, mode: 'cancel', by: 'Test Operator', cancelRec: rec });
    ok('P4 the cloud refuses: said in words, ok false, nothing followed', r.ok === false && said.some(s => /was not cancelled: This page is an old version/.test(s[0]) && s[1] === 'bad') && !calls.some(c => c.op === 'takeOffGet'), { r, said }); }
  { const { w, calls, said } = await viewer({ steps: ['files'] });
    const r = await w.CNViewer.takeOff({ orderId: RID, mode: 'cancel', by: 'Test Operator', cancelRec: rec });
    ok('P5 "takeoff" not one of the cloud\'s lanes: refused in words, nothing sent', r.ok === false && !calls.some(c => c.op === 'cancelPut') && said.some(s => /was not cancelled: the cloud does not take orders off sheets/.test(s[0])), { r, said }); }
  { const { w, calls, said } = await viewer({});
    const r = await w.CNViewer.takeOff({ orderId: RID, mode: 'cancel', by: '', cancelRec: null });
    ok('P6 nobody signed in: refused in words, nothing sent', r.ok === false && !calls.some(c => c.op === 'cancelPut') && said.some(s => /sign in first/.test(s[0])), { r, said }); }
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('P7 charm-nest-1.html loads the changed viewer and sheet window under new tokens', /charm-nest-viewer\.js\?v=[^"]*-it1(?:-\w+)*"/.test(html) && /charm-nest-sheetwin\.js\?v=[^"]*-it1(?:-\w+)*"/.test(html));
}

/* ═══ S · the cloud's take-off lane acts on the record written this way ═══ */
async function partS(body) {
  process.env.EDIT_PASSCODE = 'test-pass';   // (the offline library's door; never set while the real page of part E talks to its fake site)
  const at = Math.round(+body.at), T0 = at + 2000;
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  const out = [];
  try {
    const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js');
    const P = async b => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(b) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const R = 'run-2026-11-21-item1', rid = String(body.orderId), O2 = '4195009901', lk = `${rid}_${(body.record.lines[0] || {}).transactionId}`, p1 = lk + '_1', q1 = `${O2}_${O2}1_1`;
    const runPass = async nonce => Drive.pass({ db: F.db, Lib: lib, nonce, sleep: async () => {}, renewMs: 0, budgetMs: 3600e3 });
    const force = async () => { const g = await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true }); return g && g.nonce ? runPass(g.nonce) : null; };
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: Drive.RUN_STEPS.concat(['takeoff']), flippedAt: T0 - 3600e3 }); lib._driveForget();
    F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 600e3 });
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
    F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'manual', day: '2026-11-21', errors: [], orders: [rid, O2],
      lines: { [lk]: { orderId: rid, state: 'written', poolIds: [p1] }, [`${O2}_${O2}1`]: { orderId: O2, state: 'written', poolIds: [q1] } } });
    for (const [pid, o] of [[p1, rid], [q1, O2]]) F.seed('Charm_Pool/' + pid, { poolId: pid, orderId: o, lineKey: pid.replace(/_\d+$/, ''), state: 'written', sheetId: 'gf-i1', material: 'gold', runId: R, quantity: 1 });
    const charm = (cid, pid, o) => ({ id: cid, poolId: pid, order: o, name: cid, hash: 'h-' + cid }), place = (cid, x) => ({ id: cid, cxPt: x, cyPt: 20, angle: 0, scale: 1 });
    F.seed('Charm_Nest_Sheets/gf-i1', { id: 'gf-i1', runId: R, metal: 'gold', fileBase: 'GF_working_gf-i1', draft: true, archived: false, layoutRev: 3, contentRev: 1, poolIds: [p1, q1], orders: [rid, O2],
      charms: [charm('c-1', p1, rid), charm('c-2', q1, O2)], placements: [place('c-1', 10), place('c-2', 60)] });
    // the lane's first look (its cursor set; nothing cancelled yet)
    await force();
    const before = F.doc('Charm_Nest_Sheets/gf-i1');
    ok('S0 before the press: the order\'s piece is on the filling sheet, the lane has looked once (its cursor is set)', before.poolIds.includes(p1) && !F.doc('Charm_Nest_Cancelled/' + rid), before.poolIds);
    // the press, EXACTLY as the real page sent it
    F.net.kicks.length = 0;
    const put = await P(body);
    ok('S1 mode on: the cancelPut the real viewer sent is taken from a current page (a person\'s press, not refused), and kept as sent', put.http === 200 && put.ok && !put.cloudCarries && F.doc('Charm_Nest_Cancelled/' + rid).by === body.by && F.doc('Charm_Nest_Cancelled/' + rid).why === body.why, put);
    const kick = F.net.kicks.filter(k => k.name === 'charmNestDrive-background').pop(), nonce = kick ? JSON.parse(kick.body).nonce : null;
    ok('S2 ... it wakes the cloud driver (driveSoon)', !!nonce, F.net.kicks.map(k => k.name));
    const pass = nonce ? await runPass(nonce) : await force();
    const sh = F.doc('Charm_Nest_Sheets/gf-i1'), row = F.doc('Charm_Pool/' + p1), job = ((F.doc('Charm_Nest_TakeOff/' + rid) || {}).jobs || {})[`cancel-${Math.round(+F.doc('Charm_Nest_Cancelled/' + rid).at)}`];
    ok('S3 the take-off lane found the cancel and took the piece off the sheet; the other order\'s piece keeps its pose', pass && pass.takeoff && pass.takeoff.detected === 1 && !sh.poolIds.includes(p1) && sh.poolIds.includes(q1) && JSON.stringify(sh.placements) === JSON.stringify([place('c-2', 60)]) && sh.filesOwed > 0,
      { takeoff: pass && pass.takeoff, poolIds: sh.poolIds });
    ok('S4 ... the piece record says it was taken off by the person who pressed, cancelled', row.state === 'abandoned' && row.sheetId === null && row.removedBy === body.by && /^cancelled/.test(row.removedReason || ''), row);
    const R2 = F.doc('Charm_Nest_Runs/' + R) || {}; let line = R2.lines && R2.lines[lk]; if (!line) for (const p of F.list('Charm_Nest_Run_Live').filter(x => x.runId === R)) { const j = JSON.parse(p.json || '{}'); if (j[lk]) line = j[lk]; }
    ok('S5 ... its run line is gone, "cancelled by <who>"', line && line.state === 'gone' && line.reason === `cancelled by ${body.by}`, line);
    const crec = F.doc('Charm_Nest_Cancelled/' + rid);
    ok('S6 ... its journal job is done and the cancel record says what became of it on the sheet', job && job.status === 'done' && Array.isArray(crec.fates) && crec.fates.some(f => f.fate === 'removed'), { job, fates: crec.fates });
  } catch (e) { out.push(String(e && e.stack || e)); ok('S run', false, String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  finally { console.log = quiet; console.warn = qw; }
}

(async () => {
  // (part S in a process of its own: the fake site of part E and the offline cloud of part S each load the library with their own database)
  if (process.env.IT1_PART === 'offline') {
    let body = null; try { body = JSON.parse(process.env.IT1_BODY || 'null'); } catch (_) { body = null; }
    if (!body) {   // (no page run: the body the page's cancelRecOf makes, written out)
      const at = Date.UTC(2026, 10, 21, 14, 0, 0);
      body = { op: 'cancelPut', orderId: RID, by: 'Test Operator', why: 'buyer asked', at, record: { buyer: 'Alder Fixture', placedAt: at - 864e5, shipBy: at + 6 * 864e5, sheets: [], lines: [{ transactionId: RID + '1', sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', quantity: 1, material: 'gold' }] }, dev: 'd0viewer01', cv: CV };
      log0('(S: no body from the real page: the written-out one is used)');
    }
    await partS(body); await partP();
  } else {
    const body = process.env.SKIP_PAGE ? null : await partE();
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { IT1_PART: 'offline', IT1_BODY: body ? JSON.stringify(body) : '' }), encoding: 'utf8', timeout: 240000 });
    const outp = String(r.stdout || '') + String(r.stderr || ''); for (const l of outp.split('\n')) if (/^(OK|FAIL|\(S:)/.test(l)) log0(l);
    if (r.status !== 0) { failed++; if (!/^(OK|FAIL) /m.test(outp)) log0(outp.slice(-2000)); }
  }
  log0(failed ? `FAIL viewer-item1 (${failed})` : 'PASS viewer-item1');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item1: ' + (e && e.stack || e)); process.exit(1); });
