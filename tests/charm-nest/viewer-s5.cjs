// S5 (Paul, 21 Nov 2026): three take-off presses on a staff computer (a viewer) while the cloud carries every run (mode "on").
//   1 · the sheet window's "Take off the sheet" → "Take off and hold" ("Only this charm" or "The whole order") was refused on every viewer: no sheet went with it (the server: "which
//       sheet the order comes off is required"), and the pieces chosen were dropped, so "Only this charm" would have taken the whole order off. Now the sheet, the pieces chosen (only)
//       and the scope go with the press, the cloud keeps them on the ask and takes only those pieces off (the rest of the order stays), and the cloud's answer is said.
//   2 · "Set aside" on a cancelled order's notice in the run pill wrote its steps with production ops the server refuses in mode "on", silently. Now it sends takeOffAck: the notice
//       leaves every pill, the cancel record's removals say "set aside by <who>", and the take-off job's noticeAck is set.
//   3 · a take-off the cloud gave up on said "press Try again, or take the pieces off on a computer": there was no Try again, and "on a computer" is impossible in mode "on". Now
//       the words say the truth in mode "on", and a Try again button beside them sends takeOffAsk { kind: retry }, which the lane takes (the job starts again and ends).
//   E  the REAL page over the fake site (bridge-server.cjs: the real library over an in-memory Firestore): mode off first (Set aside takes the page's own path, no takeOffAck), then the
//      page booted again as a viewer (mode "on", "takeoff" the cloud's): the sheet window's take-off (SheetWin.holdKit.takeOff, the same takeOff the panel's button calls) sends the
//      sheet, the pieces and the scope, and the library keeps them on the ask; Set aside sends takeOffAck and the notice goes.
//   P  charm-nest-viewer.js in a bare context: what is sent and what is said (taken off, refused, held with Try again, Try again sent, Set aside).
//   S  the offline cloud (_cloudFakes.cjs): the asks as the page sends them, through the real handler and the real driver pass: only the chosen pieces come off; the whole order across
//      two sheets; the old shape refused in words; Try again ends a stuck job; Set aside through takeOffAck; the stuck words in mode "on".
// Offline only: no Etsy, no live write, made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-s5.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1200))); if (!cond) failed++; };
const CV = 'cloud14-20261120';
const X = '4195000021', pid = (o, l, n) => `${o}_${o}${l}_${n || 1}`;
const ON_STEPS = ['pull', 'claim', 'pool', 'plan', 'nest', 'checkpoint', 'engrave', 'revalidate', 'labels', 'commit', 'complete', 'files', 'takeoff', 'rosecut', 'sweeps'];

/* ═══ E · the real page ═══ */
async function partE() {
  const { start } = require('./bridge-server.cjs');
  let chromium; try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(ROOT, 'node_modules'), 'playwright-core'))); } catch (_) { log0('E: no playwright-core: the real page was not run'); return; }
  const srv = await start({ receipts: [] }), st = srv.st;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await ctx.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; window.confirm = () => true; });
  const page = await ctx.newPage(), errors = [];
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
  const forget = () => { for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {} };
  const callsOf = (op, rid) => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === op && (!rid || String(c.body.orderId) === rid));
  const boot = async () => {
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.SheetWin && window.AutoCancel && window.CNViewer && CNViewer.known() && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(() => { window.__said = []; const t0 = window.toast; window.toast = function (t) { window.__said.push(String(t)); return t0 && t0.apply(this, arguments); }; });
  };
  // a set-aside notice on this page, as the shared store brings it (cn.autoCancel.v1:production), then AutoCancel reads it again
  const notice = (rid, at) => page.evaluate(([rid, at]) => { const k = 'cn.autoCancel.v1:production'; let s = {}; try { s = JSON.parse(localStorage.getItem(k) || '{}') || {}; } catch (_) {} s.notices = (s.notices || []).filter(n => n.rid !== rid).concat([{ rid, at, text: `Order ${rid} was cancelled. Its pieces are already cut on GF Sheet 2: set them aside.`, where: 'GF Sheet 2', t: Date.now(), sheets: ['GF Sheet 2'] }]); localStorage.setItem(k, JSON.stringify(s)); AutoCancel.reload(); return AutoCancel.notices().map(n => n.rid); }, [rid, at]);
  try {
    /* mode off: Set aside takes the page's own path (no takeOffAck) */
    await boot();
    const R0 = '4195000041', at0 = Date.now() - 60000;
    await notice(R0, at0);
    const off = await page.evaluate(async rid => { const r = AutoCancel.ack(rid, null); await new Promise(z => setTimeout(z, 900)); return { mode: CNViewer.mode(), on: CNViewer.on(), sync: r === undefined, left: AutoCancel.notices().map(n => n.rid) }; }, R0);
    ok('E1 mode off: Set aside takes the page\'s own path, as before (no takeOffAck sent; the notice goes here)', off.mode === 'off' && !off.on && !off.left.includes(R0) && callsOf('takeOffAck').length === 0, { off, acks: callsOf('takeOffAck').length });

    /* mode on: the same page booted as a viewer */
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'manual', cloudSteps: ON_STEPS, flippedAt: Date.now() - 3600e3 });
    forget();
    await boot();
    const v = await page.evaluate(() => ({ mode: CNViewer.mode(), on: CNViewer.on(), frozen: !!(window.Session && Session.frozen && Session.frozen()), run: !!(window.B && B.run), lane: CNViewer.lane('takeoff') }));
    ok('E2 the page boots as a viewer in mode on (workspace frozen, no run held, "takeoff" the cloud\'s)', v.mode === 'on' && v.on && v.frozen && !v.run && v.lane, v);
    // 1 · "Take off and hold", "Only this charm": the sheet window's take-off with one piece of a two-piece order, on sheet gf-s5
    const a = pid(X, 1), b = pid(X, 2);
    const t1 = await page.evaluate(async ([rid, a]) => { window.__said.length = 0; await SheetWin.holdKit.takeOff({ rid, ok: [{ id: a, sku: 'LEAF_CHARM', sh: { sheetId: 'gf-s5', fileBase: 'GF_working_gf-s5' }, where: 'GF Sheet 2' }], stay: [] }, 'Test Operator', 'customer asked'); return { said: window.__said.slice() }; }, [X, a]);
    const ask = callsOf('takeOffAsk', X).pop(), body = ask && ask.body, jd = st.doc('Charm_Nest_TakeOff', X) || {}, stored = Object.values(jd.asks || {})[0] || {};
    ok('E3 "Only this charm" on a viewer sends the sheet, the piece chosen and the scope (takeOffAsk sheetOff: sheetId, only, scope "order"), from a current page',
      !!body && body.kind === 'sheetOff' && body.sheetId === 'gf-s5' && JSON.stringify(body.only) === JSON.stringify([a]) && body.scope === 'order' && body.by === 'Test Operator' && body.cv === CV, body);
    ok('E4 ... the cloud TOOK the press (no refusal) and keeps the piece chosen and the scope on the ask, for its take-off lane',
      stored.kind === 'sheetOff' && stored.state === 'waiting' && stored.sheetId === 'gf-s5' && JSON.stringify(stored.only) === JSON.stringify([a]) && !(stored.only || []).includes(b) && stored.scope === 'order', jd);
    ok('E5 ... and the person is told: this piece comes off and waits on hold, the rest of the order stays (never "which sheet ... is required")',
      t1.said.some(t => /Order 4195000021: the cloud takes this piece off and puts it on hold; the rest of the order stays where it is/.test(t)) && !t1.said.some(t => /which sheet|was not taken off/.test(t)), t1.said);
    // 2 · Set aside on a cancelled order's notice
    const R2 = '4195000042', at2 = Date.now() - 120000;
    st.put('Charm_Nest_Cancelled', R2, { orderId: R2, at: at2, by: 'Etsy', source: 'etsy', why: '' });
    st.put('Charm_Nest_TakeOff', R2, { rid: R2, due: false, updatedAt: at2, jobs: { [`cancel-${at2}`]: { kind: 'cancel', at: at2, by: 'Etsy', status: 'done', noticed: true } } });
    const sp = await page.evaluate(([rid, at]) => CN.api('charmNestLibrary', { op: 'sharePut', name: 'autocancel-production', set: { [`n:${rid}:${at}`]: { rid, at, text: `Order ${rid} was cancelled. Its pieces are already cut on GF Sheet 2: set them aside.`, where: 'GF Sheet 2', t: Date.now(), sheets: ['GF Sheet 2'] } }, by: 'System' }, { quiet: true }).catch(e => ({ error: e.message })), [R2, at2]);
    await notice(R2, at2);
    const s2 = await page.evaluate(async rid => { window.__said.length = 0; const r = await AutoCancel.ack(rid, null); return { r, left: AutoCancel.notices().map(n => n.rid), said: window.__said.slice() }; }, R2);
    const ack = callsOf('takeOffAck', R2).pop(), share = (st.doc('Charm_Nest_Shared', 'autocancel-production') || {}).f || {}, field = share[`n:${R2}:${at2}`];
    const rec = st.doc('Charm_Nest_Cancelled', R2) || {}, job = ((st.doc('Charm_Nest_TakeOff', R2) || {}).jobs || {})[`cancel-${at2}`] || {};
    ok('E6 "Set aside" on a viewer sends takeOffAck (who, the notice\'s moment) and the cloud takes it (no silent refusal)', sp && sp.ok && !!ack && ack.body.by === 'Test Operator' && +ack.body.at === at2 && s2.r && s2.r.ok === true && s2.r.removed === 1, { sp, ack: ack && ack.body, r: s2.r });
    ok('E7 ... the notice leaves the shared store (every page\'s pill) and this page; the person is told who set it aside', !!field && field.x === 1 && !s2.left.includes(R2) && s2.said.some(t => /Order 4195000042: its pieces are set aside, by Test Operator/.test(t)), { field, left: s2.left, said: s2.said });
    ok('E8 ... the cancel record\'s removals say "set aside by Test Operator", and the take-off job\'s noticeAck is set', (rec.removals || []).some(x => x.outcome === 'setAside' && x.by === 'Test Operator' && /set aside by Test Operator/.test(x.text)) && job.noticeAck && job.noticeAck.by === 'Test Operator', { removals: rec.removals, noticeAck: job.noticeAck });
    ok('E9 nothing on the viewer was refused by the server as production work (cancelFates / timelineAdd from this page in mode on)', !st.calls.some(c => c.name === 'charmNestLibrary' && ['cancelFates', 'timelineAdd'].includes(c.op) && [X, R2].includes(String(c.body.orderId || ((c.body.events || [])[0] || {}).orderId))), st.calls.filter(c => ['cancelFates', 'timelineAdd'].includes(c.op)).map(c => c.op));
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  ok('E no page error', errors.length === 0, errors.slice(0, 2).join(' | '));
  await browser.close(); await srv.close();
}

/* ═══ P · the viewer module ═══ */
async function partP() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-viewer.js'), 'utf8');
  async function viewer(o) {
    const calls = [], said = [];
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout: (f, ms) => setTimeout(f, Math.min(ms, 5)), clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, Date, JSON, performance: { now: () => 1e6 } };
    w.window = w;
    const els = [];
    w.toast = (t, tone) => { said.push([String(t), tone]); if (!o.dom) return undefined; const el = { kids: [], gone: false, appendChild(c) { this.kids.push(c); }, querySelector(sel) { return sel === '[data-cn-act]' ? this.kids.find(k => k.attrs && k.attrs['data-cn-act']) || null : null; }, _go() { this.gone = true; } }; els.push(el); return el; };
    w.CharmNestRunFollow = { state: () => ({ runId: null }), dev: () => 'd0viewer01', now: () => {} };
    let looks = 0;
    w.CN = { api: async (fn, body) => {
      calls.push(body);
      if (body.op === 'driveGet') return { ok: true, mode: 'on', drive: { mode: 'on', cloudSteps: o.steps || ['takeoff', 'files'] } };
      if (body.op === 'takeOffAsk') return o.refuse ? { error: o.refuse, status: 409 } : { ok: true, orderId: body.orderId, askId: 'a1', kind: body.kind, at: 777, job: body.kind === 'retry' ? null : `${body.kind}-777` };
      if (body.op === 'takeOffAck') return o.refuseAck ? Promise.reject(Object.assign(new Error(o.refuseAck), { status: 409, data: { error: o.refuseAck } })) : { ok: true, orderId: body.orderId, removed: o.ackRemoved == null ? 1 : o.ackRemoved };
      if (body.op === 'cancelPut') return { ok: true, record: { orderId: body.orderId, at: body.at, by: body.by } };
      if (body.op === 'takeOffGet') { looks++; const job = o.job ? o.job(looks, calls) : null; return { journals: { [body.orderIds[0]]: job ? { jobs: { [o.key]: job } } : null } }; }
      return { ok: true };
    } };
    vm.runInContext(src, vm.createContext(w));
    await w.CNViewer.ready();
    if (o.dom) w.document = { createElement: () => ({ attrs: {}, style: {}, setAttribute(k, v) { this.attrs[k] = v; } }) };
    return { w, calls, said, els };
  }
  // (the real clock: part S froze Date.now in this process)
  const { performance: perf } = require('perf_hooks');
  const wait = async (fn, ms) => { const t0 = perf.now(); while (!fn() && perf.now() - t0 < (ms || 3000)) await new Promise(r => setTimeout(r, 10)); return fn(); };
  const a = pid(X, 1);
  { const { w, calls, said } = await viewer({ key: 'sheetOff-777', job: n => (n < 2 ? { status: 'working' } : { status: 'done', taken: [{ where: 'GF Sheet 2', n: 1 }], left: [] }) });
    const r = await w.CNViewer.takeOff({ orderId: X, mode: 'hold', by: 'Test Operator', note: 'customer asked', only: [a], scope: 'order', sheetId: 'gf-s5' });
    const ask = calls.find(c => c.op === 'takeOffAsk');
    ok('P1 a take-off sends the sheet, the piece chosen and the scope (sheetOff), and answers the job it becomes', r.ok === true && r.job === 'sheetOff-777' && ask && ask.kind === 'sheetOff' && ask.sheetId === 'gf-s5' && JSON.stringify(ask.only) === JSON.stringify([a]) && ask.scope === 'order' && ask.by === 'Test Operator' && ask.note === 'customer asked', { r, ask });
    await wait(() => said.some(s => /the cloud took 1 piece off GF Sheet 2/.test(s[0])));
    ok('P2 ... said when pressed (this piece comes off, the rest stays), then the cloud\'s answer when its job is done (takeOffGet, job sheetOff-<at>)', said.some(s => /the cloud takes this piece off and puts it on hold; the rest of the order stays/.test(s[0])) && said.some(s => /Order 4195000021: the cloud took 1 piece off GF Sheet 2 and put it on hold$/.test(s[0]) && s[1] === 'ok'), said); }
  { const { w, calls, said } = await viewer({});
    const r = await w.CNViewer.takeOff({ orderId: X, mode: 'hold', by: 'Test Operator' });
    ok('P3 no sheet and no piece known: refused in words, nothing sent', r.ok === false && !calls.some(c => c.op === 'takeOffAsk') && said.some(s => /was not taken off: the sheet or the pieces to take off were not known\. Nothing was changed/.test(s[0]) && s[1] === 'bad'), said); }
  { const { w, said } = await viewer({ refuse: 'which sheet the order comes off, or which of its pieces, is required' });
    const r = await w.CNViewer.takeOff({ orderId: X, mode: 'hold', by: 'Test Operator', only: [a], sheetId: 'gf-s5' });
    ok('P4 the cloud refuses: said with its reason, ok false (never "done")', r.ok === false && said.some(s => /Order 4195000021 was not taken off: which sheet .* Nothing was changed/.test(s[0]) && s[1] === 'bad') && !said.some(s => /the cloud takes/.test(s[0])), said); }
  // 3 · held / stuck → Try again
  { let retried = 0;
    const stuck = { status: 'stuck', updatedAt: 111, why: 'The cloud could not finish this take-off (boom): press Try again and the cloud starts it again' };
    const { w, calls, said, els } = await viewer({ dom: true, key: 'sheetOff-777', job: (n, cs) => { const asked = cs.some(c => c.op === 'takeOffAsk' && c.kind === 'retry'); if (asked) retried++; return !asked ? stuck : retried < 3 ? stuck : { status: 'done', updatedAt: 222, taken: [{ where: 'GF Sheet 2', n: 1 }], left: [] }; } });
    await w.CNViewer.takeOff({ orderId: X, mode: 'hold', by: 'Test Operator', only: [a], sheetId: 'gf-s5', scope: 'order' });
    await wait(() => said.some(s => /was not taken off: The cloud could not finish/.test(s[0])));
    const el = els.find(e => e.kids.length), btn = el && el.kids[0], act = w.CNViewer.lastAct();
    ok('P5 a take-off the cloud gave up on is said with its reason and a Try again button beside it (the words say Try again, never "on a computer")',
      said.some(s => /Order 4195000021 was not taken off: The cloud could not finish this take-off \(boom\): press Try again and the cloud starts it again$/.test(s[0]) && s[1] === 'bad') && !!btn && btn.textContent === 'Try again' && btn.attrs['data-cn-act'] === 'Try again' && act && act.label === 'Try again', { said, btn: btn && btn.textContent });
    btn.onclick({ stopPropagation() {} });
    await wait(() => said.some(s => /took 1 piece off/.test(s[0])));
    const rt = calls.filter(c => c.op === 'takeOffAsk' && c.kind === 'retry');
    ok('P6 Try again sends takeOffAsk { kind: retry } for the order, under the person\'s name; the toast goes', rt.length === 1 && rt[0].orderId === X && rt[0].by === 'Test Operator' && el.gone === true, rt);
    ok('P7 ... it is said, the stuck job as it was is not said again, and the cloud\'s answer is said when the job ends',
      said.some(s => /Order 4195000021: the cloud tries its take-off again/.test(s[0])) && said.filter(s => /The cloud could not finish/.test(s[0])).length === 1 && said.some(s => /the cloud took 1 piece off GF Sheet 2 and put it on hold/.test(s[0])), said); }
  { const at = Date.UTC(2026, 10, 21, 14, 0, 0);
    const { w, said } = await viewer({ key: `cancel-${at}`, job: () => ({ status: 'stuck', updatedAt: 5, why: 'The cloud could not finish this take-off (boom): press Try again and the cloud starts it again' }) });
    await w.CNViewer.takeOff({ orderId: X, mode: 'cancel', by: 'Test Operator', cancelRec: { orderId: X, by: 'Test Operator', why: '', at, record: { lines: [] } } });
    await wait(() => said.some(s => /could not take its pieces off/.test(s[0])));
    const act = w.CNViewer.lastAct();
    ok('P8 a cancel\'s take-off the cloud gave up on offers Try again too', act && act.label === 'Try again' && /Order 4195000021 is cancelled, but the cloud could not take its pieces off: The cloud could not finish/.test(act.text), act && act.text); }
  { const { w, calls } = await viewer({ steps: ['files'] });
    const r = await w.CNViewer.retry(X, { by: 'Test Operator' });
    ok('P9 Try again while "takeoff" is not the cloud\'s: refused in words, nothing sent', r.ok === false && !calls.some(c => c.op === 'takeOffAsk'), r); }
  // 2 · Set aside
  { const { w, calls, said } = await viewer({});
    const r = await w.CNViewer.ack(X, 1763733600000, { by: 'Test Operator' });
    const b = calls.find(c => c.op === 'takeOffAck');
    ok('P10 Set aside sends takeOffAck { orderId, at, by } and says who set it aside', r.ok === true && b && b.orderId === X && b.at === 1763733600000 && b.by === 'Test Operator' && said.some(s => /Order 4195000021: its pieces are set aside, by Test Operator/.test(s[0])), { r, b, said }); }
  { const { w, said } = await viewer({ refuseAck: 'the cloud is busy' });
    const r = await w.CNViewer.ack(X, 1, { by: 'Test Operator' });
    ok('P11 a refused Set aside is said and answered ok false (the notice stays)', r.ok === false && said.some(s => /Order 4195000021: not set aside: the cloud is busy\. Nothing was changed/.test(s[0]) && s[1] === 'bad'), said); }
  { const { w, calls } = await viewer({});
    const r = await w.CNViewer.ack(X, 1, { by: '' });
    ok('P12 nobody signed in: Set aside refused in words, nothing sent', r.ok === false && !calls.some(c => c.op === 'takeOffAck'), r); }
  { const { w, said } = await viewer({ ackRemoved: 0 });
    const r = await w.CNViewer.ack(X, 1, { by: 'Test Operator' });
    ok('P12b the cloud had no such notice (set aside on another screen): said so, never "set aside by" when nothing was recorded', r.ok === true && r.already === true && said.some(s => /this notice was already set aside on another screen; nothing more was recorded/.test(s[0])) && !said.some(s => /set aside, by/.test(s[0])), said); }
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8'), sw = fs.readFileSync(path.join(ROOT, 'charm-nest-sheetwin.js'), 'utf8'), drv = fs.readFileSync(path.join(ROOT, 'netlify/functions/_charmNestDrive.js'), 'utf8');
  ok('P13 charm-nest-1.html loads the changed viewer and sheet window under new tokens (-s5)', /charm-nest-viewer\.js\?v=[^"]*-s5(?:-\w+)*"/.test(html) && /charm-nest-sheetwin\.js\?v=[^"]*-s5(?:-\w+)*"/.test(html));
  ok('P14 the sheet window\'s viewer take-off sends the sheet and the scope; its Set aside sends CNViewer.ack in mode on; the driver gives the lane its mode',
    /CNViewer\.takeOff\(\{ orderId: plan\.rid, mode: cx \? "cancel" : "hold"[^\n]*scope: "order", \.\.\.\(sheetId \? \{ sheetId \} : \{\}\)/.test(sw) && /if \(window\.CNViewer && CNViewer\.on\(\)\) \{\s*st = read\(\); const n = st\.notices\.find[^\n]*\n[^\n]*\n\s*return CNViewer\.ack\(rid, n\.at/.test(sw) && /TakeOff\.lane\(\{[^)]*mode: info\.mode/.test(drv));
}

/* ═══ S · the cloud takes the presses as the page sends them ═══ */
async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), TakeOff = F.load('_charmNestTakeOff.js');
    const P = async b => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(Object.assign({ dev: 'd0viewer01', cv: CV }, b)) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const R = 'run-2026-11-21-s5', O2 = '4195009902', Y = '4195000031';
    const a = pid(X, 1), b = pid(X, 2), c = pid(X, 3), q = pid(O2, 1), y = pid(Y, 1);
    const runPass = async nonce => Drive.pass({ db: F.db, Lib: lib, nonce, sleep: async () => {}, renewMs: 0, budgetMs: 3600e3 });
    const pass = async () => { F.clock.advance(5000); const kick = F.net.kicks.filter(k => k.name === 'charmNestDrive-background').pop(); F.net.kicks.length = 0; const n = kick ? JSON.parse(kick.body).nonce : null; if (n) { const r = await runPass(n); if (r && r.ok && r.takeoff) return r; } const g = await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true }); return g && g.nonce ? runPass(g.nonce) : null; };
    const lineOf = k => { const r = F.doc('Charm_Nest_Runs/' + R) || {}; if (r.lines && r.lines[k]) return r.lines[k]; for (const p of F.list('Charm_Nest_Run_Live').filter(x => x.runId === R)) { const j = JSON.parse(p.json || '{}'); if (j[k]) return j[k]; } return null; };
    const charm = (cid, p, o) => ({ id: cid, poolId: p, order: o, name: cid, hash: 'h-' + cid }), place = (cid, x) => ({ id: cid, cxPt: x, cyPt: 20, angle: 0, scale: 1 });
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: Drive.RUN_STEPS.concat(['takeoff']), flippedAt: T0 - 3600e3 }); lib._driveForget();
    F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 600e3 });
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
    const L = p => [p.replace(/_\d+$/, ''), { orderId: p.split('_')[0], state: 'written', poolIds: [p] }];
    F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'manual', day: '2026-11-21', errors: [], orders: [X, O2, Y], lines: Object.fromEntries([a, b, c, q, y].map(L)) });
    for (const [p, sh, m] of [[a, 'gf-s5', 'gold'], [b, 'gf-s5', 'gold'], [q, 'gf-s5', 'gold'], [y, 'gf-s5', 'gold'], [c, 'ss-s5', 'silver']]) F.seed('Charm_Pool/' + p, { poolId: p, orderId: p.split('_')[0], lineKey: p.replace(/_\d+$/, ''), state: 'written', sheetId: sh, material: m, runId: R, quantity: 1 });
    F.seed('Charm_Nest_Sheets/gf-s5', { id: 'gf-s5', runId: R, metal: 'gold', fileBase: 'GF_working_gf-s5', draft: true, archived: false, layoutRev: 3, contentRev: 1, poolIds: [a, b, q, y], orders: [X, O2, Y],
      charms: [charm('c-a', a, X), charm('c-b', b, X), charm('c-q', q, O2), charm('c-y', y, Y)], placements: [place('c-a', 10), place('c-b', 40), place('c-q', 70), place('c-y', 100)] });
    F.seed('Charm_Nest_Sheets/ss-s5', { id: 'ss-s5', runId: R, metal: 'silver', fileBase: 'SS_working_ss-s5', draft: true, archived: false, layoutRev: 2, contentRev: 1, poolIds: [c], orders: [X], charms: [charm('c-c', c, X)], placements: [place('c-c', 10)] });
    // (Y: a take-off the cloud gave up on before: stuck, after eight tries)
    F.seed('Charm_Nest_TakeOff/' + Y, { rid: Y, due: false, updatedAt: T0 - 60000, jobs: { 'sheetOff-1763736000000': { kind: 'sheetOff', at: 1763736000000, by: 'Ann', sheetId: 'gf-s5', scope: 'order', only: [y], status: 'stuck', tries: 8, why: 'The cloud could not finish this take-off (boom): press Try again and the cloud starts it again' } } });
    await pass();   // (the lane's first look: its cursor)

    /* 1 · "Only this charm": the ask exactly as the viewer sends it */
    const old = await P({ op: 'takeOffAsk', orderId: X, kind: 'sheetOff', scope: 'order', note: '', by: 'Test Operator' });
    ok('S1 the old viewer shape (no sheet, no piece) is refused in words, nothing kept', !!old.error && /which sheet the order comes off, or which of its pieces, is required/.test(old.error) && !Object.keys((F.doc('Charm_Nest_TakeOff/' + X) || {}).asks || {}).length, old);
    const wrong = await P({ op: 'takeOffAsk', orderId: X, kind: 'sheetOff', sheetId: 'gf-s5', scope: 'order', only: [q], by: 'Test Operator' });
    ok('S2 pieces of another order named: refused in words, nothing kept', !!wrong.error && /None of the pieces named is a piece of order 4195000021/.test(wrong.error), wrong);
    F.net.kicks.length = 0;
    const one = await P({ op: 'takeOffAsk', orderId: X, kind: 'sheetOff', sheetId: 'gf-s5', scope: 'order', only: [a], note: 'customer asked', by: 'Test Operator' });
    const asked = Object.values((F.doc('Charm_Nest_TakeOff/' + X) || {}).asks || {})[0] || {};
    ok('S3 "Only this charm": taken in mode on (a person\'s press), the piece chosen and the scope kept on the ask, the job it becomes answered', one.http === 200 && one.ok && !one.cloudCarries && one.job === `sheetOff-${one.at}` && JSON.stringify(asked.only) === JSON.stringify([a]) && asked.scope === 'order' && asked.sheetId === 'gf-s5', { one, asked });
    const p1 = await pass();
    const gf1 = F.doc('Charm_Nest_Sheets/gf-s5'), ra = F.doc('Charm_Pool/' + a), rb = F.doc('Charm_Pool/' + b), j1 = ((F.doc('Charm_Nest_TakeOff/' + X) || {}).jobs || {})[one.job] || {};
    ok('S4 ... the cloud took ONLY that piece off: the order\'s other piece stays on the sheet, and every other pose is byte for byte', p1 && p1.takeoff && !gf1.poolIds.includes(a) && gf1.poolIds.includes(b) && gf1.poolIds.includes(q) && JSON.stringify(gf1.placements) === JSON.stringify([place('c-b', 40), place('c-q', 70), place('c-y', 100)]) && rb.state === 'written' && rb.sheetId === 'gf-s5' && F.doc('Charm_Nest_Sheets/ss-s5').poolIds.includes(c),
      { takeoff: p1 && p1.takeoff, poolIds: gf1.poolIds });
    ok('S5 ... its piece is held by the person (heldBy, the note), its line held in the sheet window\'s words; the order\'s other lines stay as they were',
      ra.state === 'abandoned' && ra.heldBy === 'Test Operator' && ra.heldReason === 'customer asked' && (lineOf(a.replace(/_1$/, '')) || {}).state === 'held' && /^Taken off .+ by Test Operator: customer asked$/.test((lineOf(a.replace(/_1$/, '')) || {}).hold || '') && (lineOf(b.replace(/_1$/, '')) || {}).state === 'written' && (lineOf(c.replace(/_1$/, '')) || {}).state === 'written',
      { a: ra, la: lineOf(a.replace(/_1$/, '')), lb: lineOf(b.replace(/_1$/, '')) });
    ok('S6 ... its job is done and says what was taken (the page says it)', j1.status === 'done' && (j1.taken || []).length === 1 && j1.taken[0].n === 1 && JSON.stringify(j1.only) === JSON.stringify([a]), j1);

    /* 1 · "The whole order": its pieces on two sheets */
    F.clock.advance(2000);
    const all = await P({ op: 'takeOffAsk', orderId: X, kind: 'sheetOff', sheetId: 'gf-s5', scope: 'order', only: [b, c], note: '', by: 'Test Operator' });
    await pass();
    const gf2 = F.doc('Charm_Nest_Sheets/gf-s5'), ss2 = F.doc('Charm_Nest_Sheets/ss-s5'), j2 = ((F.doc('Charm_Nest_TakeOff/' + X) || {}).jobs || {})[all.job] || {};
    ok('S7 "The whole order": its pieces come off both sheets (the one pressed and the other); the other orders\' pieces stay', all.ok && !gf2.poolIds.includes(b) && !ss2.poolIds.includes(c) && gf2.poolIds.includes(q) && gf2.poolIds.includes(y) && j2.status === 'done' && (j2.taken || []).length === 2 && F.doc('Charm_Pool/' + c).heldBy === 'Test Operator', { all, gf: gf2.poolIds, ss: ss2.poolIds, j2 });

    /* 3 · Try again: a stuck take-off starts again and ends */
    F.clock.advance(2000);
    const rt = await P({ op: 'takeOffAsk', orderId: Y, kind: 'retry', note: 'Try again', by: 'Test Operator' });
    ok('S8 Try again (takeOffAsk retry) is taken in mode on', rt.http === 200 && rt.ok && rt.kind === 'retry' && rt.job === null, rt);
    await pass();
    const jy = ((F.doc('Charm_Nest_TakeOff/' + Y) || {}).jobs || {})['sheetOff-1763736000000'] || {};
    ok('S9 ... the lane starts the stuck job again and it ends: done, its piece off the sheet', jy.status === 'done' && !F.doc('Charm_Nest_Sheets/gf-s5').poolIds.includes(y) && F.doc('Charm_Pool/' + y).state === 'abandoned', jy);

    /* 2 · Set aside: takeOffAck from a viewer */
    const C2 = '4195000051', AT = T0 - 7200e3;
    F.seed('Charm_Nest_Cancelled/' + C2, { orderId: C2, at: AT, by: 'Etsy', source: 'etsy', createdAt: F.Timestamp.fromMillis(AT) });
    F.seed('Charm_Nest_TakeOff/' + C2, { rid: C2, due: false, updatedAt: AT, jobs: { [`cancel-${AT}`]: { kind: 'cancel', at: AT, by: 'Etsy', status: 'done', noticed: true } } });
    await P({ op: 'sharePut', name: 'autocancel-production', set: { [`n:${C2}:${AT}`]: { rid: C2, at: AT, text: `Order ${C2} was cancelled.`, where: 'GF Sheet 2', t: AT, sheets: ['GF Sheet 2'] } }, by: 'Cloud' });
    const ack = await P({ op: 'takeOffAck', orderId: C2, at: AT, by: 'Test Operator' });
    const f = ((F.doc('Charm_Nest_Shared/autocancel-production') || {}).f || {})[`n:${C2}:${AT}`], rec = F.doc('Charm_Nest_Cancelled/' + C2) || {}, jc = ((F.doc('Charm_Nest_TakeOff/' + C2) || {}).jobs || {})[`cancel-${AT}`] || {};
    ok('S10 Set aside (takeOffAck) from a viewer in mode on: taken; the notice leaves the shared store; the record says set aside by <who>; the job\'s noticeAck is set',
      ack.http === 200 && ack.ok && ack.removed === 1 && f && f.x === 1 && (rec.removals || []).some(r => r.outcome === 'setAside' && r.by === 'Test Operator') && jc.noticeAck && jc.noticeAck.by === 'Test Operator', { ack, f, removals: rec.removals, noticeAck: jc.noticeAck });

    /* 3 · the words of a take-off the cloud gives up on, by mode */
    const stuckOf = async mode => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_TakeOff/4195000061', { rid: '4195000061', due: true, updatedAt: T0 - 1000, jobs: { 'sheetOff-5': { kind: 'sheetOff', at: 5, by: 'Ann', sheetId: 'gf-s5', status: 'waiting', tries: 7 } } });
      const sent = [];
      await TakeOff.lane({ db: F.db, run: { lines: {} }, runId: R, mode, budgetEnd: T0 + 60000, state: { takeoff: { cancelCursor: { at: 1 }, cancelGen: 1, lookAt: T0 } }, saveState: async () => {},
        call: async body => { sent.push(body); return { ok: true }; },
        read: async body => (body.op === 'getOrderPieces' ? { error: 'boom' } : body.op === 'cancelList' ? { unchanged: true } : body.op === 'takeOffGet' ? { journals: {} } : {}) });
      const s = sent.find(x => x.op === 'takeOffJournal' && x.set && x.set.status === 'stuck');
      return s ? s.set.why : null;
    };
    const wOn = await stuckOf('on'), wStandby = await stuckOf('standby');
    ok('S11 mode on: a take-off the cloud gives up on says the truth (press Try again; never "take the pieces off on a computer")', /^The cloud could not finish this take-off \(where the order's pieces are could not be read: boom\): press Try again and the cloud starts it again$/.test(wOn || '') && !/on a computer/.test(wOn), wOn);
    ok('S12 standby (computers still do production): the words are as before', /press Try again, or take the pieces off on a computer$/.test(wStandby || ''), wStandby);
  } catch (e) { ok('S run', false, String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  finally { console.log = quiet; console.warn = qw; }
}

(async () => {
  // (parts S and P in a process of their own: the fake site of part E and the offline cloud of part S each load the library with their own database)
  if (process.env.S5_PART === 'offline') { await partS(); await partP(); }
  else {
    if (!process.env.SKIP_PAGE) await partE();
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { S5_PART: 'offline' }), encoding: 'utf8', timeout: 240000 });
    const outp = String(r.stdout || '') + String(r.stderr || ''); for (const l of outp.split('\n')) if (/^(OK|FAIL) /.test(l) && !/^FAIL viewer-s5/.test(l)) log0(l);
    if (r.status !== 0) { failed++; if (!/^(OK|FAIL) /m.test(outp)) log0(outp.slice(-2000)); }
  }
  log0(failed ? `FAIL viewer-s5 (${failed})` : 'PASS viewer-s5');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-s5: ' + (e && e.stack || e)); process.exit(1); });
