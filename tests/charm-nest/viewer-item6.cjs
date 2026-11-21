// ITEM 6 (Paul, 21 Nov 2026, the cloud carrying every run since 20 Nov 23:41): "Make QR label" in the sheet window, pressed on a staff computer, said "No QR label made: The cloud carries
// every run now…". Why: no computer holds the run in mode "on" (B.run is null on every page), so the window offered only a sheet's own label (never Release as it stands), and its upload
// (charmnest/sheets/…) and its putSheet { label } are the cloud's work: refused. Now on a viewer the press is a person's ask, labelsAsk, and the person sees the cloud's answer.
//   S  offline (_cloudFakes.cjs, the library in mode "on", the REAL labels unit _charmNestLabels.js): a viewer's labelsAsk is taken where its old putSheet { label } is refused; the labels
//      pass item answers it and makes THAT sheet's label (a sheet of no open set: its own label beside its files; a sheet in its set with none: relabel; a sheet of a committed set: its own
//      label, never "done" with nothing made); a sheet with no order on it is refused in words; a Gold sheet of the open run released as it stands joins its set with its label, or is
//      refused with the reason and put back as it was
//   E  the REAL page (charm-nest-1.html over the fake site, bridge-server.cjs) booted as a viewer (the switch "on" before it loads: Session frozen, no run held): the sheet window offers Make
//      QR label, the press sends ONE labelsAsk (nothing uploaded, no putSheet from the page), the cloud's labels unit (run here over the fake site's records) makes the label, and the window
//      shows it ("done — the cloud made its QR label"). Then the switch "off" and a fresh page: the press makes the label on this computer as before (no labelsAsk)
// Offline only: no Etsy, no paid call, no live write. Made-up names. The QR picture is drawn by @resvg/resvg-js when installed, else a stand-in PNG (the codes on it are the unit's own).
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item6.cjs
'use strict';
const path = require('path');
const ROOT = path.join(__dirname, '../..'), FN = path.join(ROOT, 'netlify/functions');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle1';
const R = 'run-2026-11-21-qrsix', R_OLD = 'run-2026-11-14-qrold', DAY = '2026-11-21', OLD_DAY = '2026-11-14';
const O1 = '4241400132', O2 = '4241071527', O3 = '4241555501';
const pid = (rid, n) => `${rid}_${rid}${n}_1`;
/** a sheet's record: its placed pieces, files and check (no label) */
function sheetRec(id, o) {
  const name = o.fileBase, dir = o.dir || `charmnest/sheets/${o.day}/${name}`;
  const charms = o.orders.map((rid, i) => ({ id: `${id}-c${i}`, poolId: o.noPool ? undefined : pid(rid, i + 1), order: rid, name: `${rid} · charm`, areaPt2: 300, widthPt: 28.35, heightPt: 28.35 }));
  return Object.assign({ id, metal: o.metal, metalLabel: { gold: 'GF', silver: 'SS' }[o.metal], day: o.day, runId: o.runId, page: o.page || 1, draft: !o.setId, setId: o.setId || null, setSeq: o.setId ? 1 : null, sheetIndex: o.setId ? 1 : null,
    fileBase: name, folder: name, charms, placements: charms.map((c, i) => ({ n: i + 1, id: c.id, cxPt: 20 + i * 32, cyPt: 20, angle: 0, scale: 0.971, wPt: 28.35, hPt: 28.35 })), poolIds: charms.map(c => c.poolId).filter(Boolean),
    orders: o.orders.slice(), placedCount: charms.length, charmCount: charms.length, density: o.density || 0.4, releaseFull: !!o.full, status: 'complete', endedBy: 'complete', verification: { ok: true },
    stock: { wIn: 182 / 72, hIn: 62 / 72, wPt: 182, hPt: 62 }, outputs: { ai: { path: `${dir}/${name}.ai`, url: o.aiUrl || 'https://storage.test.invalid/x.ai' }, ...(o.preview ? { preview: { path: `${dir}/preview.png`, url: o.preview } } : {}) },
    backPool: [], engraving: {}, listings: [], names: '', archived: false }, o.extra || {});
}
/** the QR picture: the real renderer when @resvg/resvg-js is here, else a stand-in PNG that carries the unit's own codes */
function qrPicture() {
  try { require.resolve('@resvg/resvg-js'); return 'resvg'; } catch (_) { /* not installed here */ }
  const Q = require(path.join(FN, '_charmNestQrLabel.js'));
  Q.renderPng = ({ payload }) => ({ png: Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), Buffer.from('stand-in QR ' + payload)]), ecc: 'M' });
  return 'stand-in';
}

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const T0 = Date.UTC(2026, 10, 21, 18, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js'), how = qrPicture(), Labels = F.load('_charmNestLabels.js'), O = require(path.join(ROOT, 'charm-nest-orders.js'));
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const OWN = 'gf-qrsix-own', REL = 'ss-qrsix-set', DONE = 'gf-qrsix-done', EMPTY = 'gf-qrsix-empty', GFD = 'gf-qrsix-draft';
    const seed = mode => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: ['labels', 'files', 'takeoff', 'engrave', 'rosecut', 'checkpoint'], flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'manual', day: DAY, releasePolicy: 2, solidIncluded: {}, sheets: {}, holds: {}, errors: [], orders: [], lines: {} });
      F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
      F.seed('Charm_Nest_Runs/' + R_OLD, { runId: R_OLD, status: 'complete', day: OLD_DAY, releasePolicy: 2, lines: {} });
      F.seed('Charm_Nest_Sheets/' + OWN, sheetRec(OWN, { metal: 'gold', day: OLD_DAY, runId: R_OLD, fileBase: 'GF_Nov.14.26_Draft-2', orders: [O1, O2] }));
    };
    const runOf = id => Object.assign({ runId: id }, F.doc('Charm_Nest_Runs/' + id));
    const ctxOf = () => ({ db: F.db, run: runOf(R), runId: R, call: (b, u) => lib.call(b, { unit: u }), read: b => lib.call(b, {}), bucket: F.bucket, budgetEnd: Date.now() + 600000, gate: async () => ({ ok: true, why: [] }), say: () => {} });
    const ask = (sheetId, kind) => P({ op: 'labelsAsk', kind, sheetId, by: 'Michelle', dev: PAGE, cv: CV });
    const askDoc = id => F.doc('Charm_Nest_Label_Asks/sheet-' + id) || {};
    const rec = id => F.doc('Charm_Nest_Sheets/' + id) || {};
    seed('on');
    let r = await P({ op: 'putSheet', sheet: { id: OWN, label: { files: [{ path: 'x.png' }], own: true } }, dev: PAGE, cv: CV });
    ok('S0 mode on: what the window used to send (putSheet { label } from the page) is refused: the label is the cloud\'s work', r.http === 409 && r.cloudCarries === true && (r.fields || []).includes('label') && !rec(OWN).label, r);
    r = await ask(OWN, 'complete');
    ok('S1 mode on: a viewer\'s Make QR label (labelsAsk complete, its sheet, who pressed) is taken: one ask, "asked"', r.http === 200 && r.ok && r.ask && r.ask.key === 'sheet-' + OWN && askDoc(OWN).state === 'asked' && askDoc(OWN).by === 'Michelle' && askDoc(OWN).runId === R_OLD, { r, ask: askDoc(OWN) });
    const again = await ask(OWN, 'complete');
    ok('S1b ... pressed again before the cloud answers: the same ask (no second one)', again.http === 200 && again.again === true && askDoc(OWN).seq === 1, again);
    const p1 = await Labels.passUnit(ctxOf());
    const L = rec(OWN).label || {}, f0 = (L.files || [])[0] || {};
    const want = O.encodeOrderList([O1, O2], O.CARD_TO_METAL.gold || 'gold');
    ok(`S2 the cloud's labels pass item answers it and makes THAT sheet's label (its own, beside its files; QR: ${how}): done`, askDoc(OWN).state === 'done' && L.own === true && (L.orders || []).join() === [O1, O2].join() && f0.payload === want && /^charmnest\/sheets\/2026-11-14\/GF_Nov\.14\.26_Draft-2\/GF_Nov\.14\.26_Draft-2_label\.png$/.test(f0.path) && F.blobs.has(f0.path) && /token=/.test(f0.url || ''), { p1, ask: askDoc(OWN), label: L });
    const got = await P({ op: 'labelsAskGet', sheetId: OWN, dev: PAGE, cv: CV });
    ok('S3 what the window reads back (labelsAskGet) says done', got.http === 200 && (got.asks || []).some(a => a.key === 'sheet-' + OWN && a.state === 'done' && a.seq === 1), got);
    // a sheet in its open set with no label: relabel
    F.seed('Charm_Nest_Sets/set-qrsix-1', { setId: 'set-qrsix-1', seq: 1, day: DAY, runId: R, group: 'dispatch', name: 'Set-1', folder: `charmnest/sets/${DAY}/Set-1`, sheetIds: [REL], materials: ['silver'], orders: {}, labelFiles: [], labels: null, status: 'nesting', sheetNos: { silver: 1 } });
    F.seed('Charm_Nest_Sheets/' + REL, sheetRec(REL, { metal: 'silver', day: DAY, runId: R, setId: 'set-qrsix-1', fileBase: 'SS_Nov.21.26_Set-1_Sheet-1', dir: `charmnest/sets/${DAY}/Set-1/SS_Nov.21.26_Set-1_Sheet-1`, orders: [O3], full: true }));
    r = await ask(REL, 'relabel');
    const rr = await Labels.doAsk(ctxOf(), askDoc(REL));
    const L2 = rec(REL).label || {}, set1 = F.doc('Charm_Nest_Sets/set-qrsix-1') || {};
    ok('S4 a sheet in its set with no label (labelsAsk relabel): the cloud draws it, its record and its set\'s list say so, done', r.http === 200 && askDoc(REL).state === 'done' && (L2.files || []).length === 1 && (L2.orders || []).join() === O3 && (set1.labelFiles || []).some(f => f.sheetId === REL && f.path === L2.files[0].path) && F.blobs.has(L2.files[0].path), { r, rr, ask: askDoc(REL), label: L2, labelFiles: set1.labelFiles });
    // a sheet of a COMMITTED set (an older run) that has no label: its own label, never "done" with nothing made
    F.seed('Charm_Nest_Sets/set-qrsix-old', { setId: 'set-qrsix-old', seq: 3, day: OLD_DAY, runId: R_OLD, group: 'dispatch', name: 'Set-3', folder: `charmnest/sets/${OLD_DAY}/Set-3`, sheetIds: [DONE], materials: ['gold'], orders: {}, labelFiles: [], status: 'committed', committedAt: T0 - 86400e3 * 6 });
    F.seed('Charm_Nest_Sheets/' + DONE, sheetRec(DONE, { metal: 'gold', day: OLD_DAY, runId: R_OLD, setId: 'set-qrsix-old', fileBase: 'GF_Nov.14.26_Set-3_Sheet-1', dir: `charmnest/sets/${OLD_DAY}/Set-3/GF_Nov.14.26_Set-3_Sheet-1`, orders: [O2] }));
    await ask(DONE, 'complete'); await Labels.doAsk(ctxOf(), askDoc(DONE));
    const L3 = rec(DONE).label || {};
    ok('S5 a sheet of a committed set with no label: the cloud makes its own label (it used to answer "done" with nothing made)', askDoc(DONE).state === 'done' && L3.own === true && (L3.orders || []).join() === O2 && F.blobs.has(((L3.files || [])[0] || {}).path), { ask: askDoc(DONE), label: L3 });
    // a sheet with no order on it: refused in words
    F.seed('Charm_Nest_Sheets/' + EMPTY, sheetRec(EMPTY, { metal: 'gold', day: OLD_DAY, runId: R_OLD, fileBase: 'GF_Nov.14.26_Draft-5', orders: [O1], noPool: true }));
    await ask(EMPTY, 'complete'); await Labels.doAsk(ctxOf(), askDoc(EMPTY));
    ok('S6 a sheet with no order placed on it: refused, with the cloud\'s words (what the window shows under its button)', askDoc(EMPTY).state === 'refused' && /no order is placed on this sheet/.test(askDoc(EMPTY).why || '') && !rec(EMPTY).label, askDoc(EMPTY));
    // a Gold sheet of the open run, released as it stands: it joins its set with its label, or is refused with the reason and put back as it was (never left released, never silent)
    F.seed('Charm_Nest_Sheets/' + GFD, sheetRec(GFD, { metal: 'gold', day: DAY, runId: R, fileBase: 'GF_Nov.21.26_Draft-1', orders: [O1], density: 0.31, extra: { topup: { startedAt: T0 - 600000, tried: [] } } }));
    await ask(GFD, 'complete');
    let rel = null; try { rel = await Labels.doAsk(ctxOf(), askDoc(GFD)); } catch (e) { rel = { threw: String(e && e.message || e) }; }
    const a7 = askDoc(GFD), g7 = rec(GFD);
    const joined = a7.state === 'done' && !g7.draft && !!g7.setId && ((g7.label && g7.label.files) || []).length > 0;
    const refused = a7.state === 'refused' && String(a7.why || '').length > 3 && !g7.releaseFull && !((g7.label && g7.label.files) || []).length;
    ok('S7 Release as it stands (a Gold sheet of the open run, labelsAsk complete): it joins its set with its label, or is refused with the reason and its release put back', joined || refused, { rel, ask: a7, sheet: { draft: g7.draft, setId: g7.setId, releaseFull: g7.releaseFull, label: !!g7.label } });
    seed('off');
    r = await P({ op: 'putSheet', sheet: { id: OWN, label: { files: [{ path: 'x.png' }], own: true } }, dev: PAGE, cv: CV });
    ok('S8 drive off: nothing is refused (the page makes its own label as before)', r.http === 200 && !!rec(OWN).label, r);
    ok('S9 Etsy never called, nothing else fetched', F.etsy.calls === 0 && F.net.tripwire.length === 0, { etsy: F.etsy.calls, trip: F.net.tripwire });
  } finally { console.log = quiet; console.warn = qw; }
}

async function partE() {
  const pwDir = process.env.PW_DIR || path.join(ROOT, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { log0('E: no playwright-core: the real page was not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), st = srv.st;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) return null; await sleep(150); } };
  const OWN = 'gf-qrsix-page', OFF = 'gf-qrsix-off';
  const image = srv.blobUrl('preview.png');
  st.blobs.set('preview.png', { buf: Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), meta: { contentType: 'image/png', metadata: {} } });
  const drive = mode => { st.put('Charm_Nest_Open', 'drive', { mode, runMode: 'manual', cloudSteps: ['labels', 'files', 'takeoff', 'engrave', 'rosecut'], flippedAt: Date.now() - 3600e3 }, false); for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {} };
  async function newPage() {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {}
      window.prompt = () => 'Test Operator'; window.confirm = () => true;
      window.__said = []; const iv = setInterval(() => { if (typeof window.toast !== 'function' || window.toast.__spy) return; const t0 = window.toast; window.toast = function (t) { window.__said.push(String(t)); return t0.apply(this, arguments); }; window.toast.__spy = true; }, 50); });
    const page = await context.newPage(); page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push('page: ' + String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.CNViewer && CNViewer.known(), null, { timeout: 60000 });
    return { context, page };
  }
  const foot = page => page.evaluate(() => { const W = SheetWin.hopKit.W, f = W.el && W.el.foot, b = f && f.querySelector('[data-r2=label]'); return { open: SheetWin.isOpen(), id: W.id, rec: !!W.rec, text: f ? f.textContent.replace(/\s+/g, ' ').trim() : '', btn: b ? { title: b.title, disabled: b.disabled } : null, img: !!(f && f.querySelector('img[data-label-sheet]')), remaking: !!(f && f.querySelector('.remaking')) }; });
  const openSheet = async (page, id) => { await page.evaluate(id => SheetWin.open(id), id); return until(async () => { const f = await foot(page); return f.open && f.id === id && f.rec && (f.btn || f.img) ? f : null; }, 30000); };
  const labelCalls = from => st.calls.slice(from).filter(c => (c.op === 'putSheet' && c.body && c.body.sheet && 'label' in c.body.sheet) || (c.name === 'charmNestOutput' && /_label/.test(JSON.stringify(c.body || {}).slice(0, 4000))) || c.op === 'labelsAsk');
  try {
    // the fake site: the cloud carries every run (mode "on", lane "labels"), a run open in the cloud, a sheet of an earlier run with no label
    drive('on');
    st.put('Charm_Nest_Runs', R, { runId: R, status: 'running', step: 'nest', mode: 'manual', day: DAY, releasePolicy: 2, solidIncluded: {}, sheets: {}, holds: {}, errors: [], orders: [], lines: {} });
    st.put('Charm_Nest_Run_Carrier', R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now() + 3600e3, since: Date.now() - 3600e3 } });
    st.put('Charm_Nest_Runs', R_OLD, { runId: R_OLD, status: 'complete', day: OLD_DAY, releasePolicy: 2, lines: {} });
    st.put('Charm_Nest_Sheets', OWN, sheetRec(OWN, { metal: 'gold', day: OLD_DAY, runId: R_OLD, fileBase: 'GF_Nov.14.26_Draft-7', orders: [O1, O2], preview: image }));
    const { context, page } = await newPage();
    const v = await page.evaluate(R => {
      const F = window.CharmNestRunFollow, st0 = F.state; F.state = () => Object.assign(st0(), { runId: R, run: { status: 'running' } });
      return { on: CNViewer.on(), held: !!(window.B && B.run), frozen: !!(window.Session && Session.frozen && Session.frozen()), lane: !!(window.CharmNestCloudLanes && CharmNestCloudLanes('labels', { runId: 'x' })) };
    }, R);
    ok('E0 the page boots as a viewer (switch "on"): no run held, the labels lane the cloud\'s', v.on && !v.held && v.lane, v);
    const f0 = await openSheet(page, OWN);
    ok('E1 the sheet window offers Make QR label for a sheet of no open set, as the cloud\'s ask', !!f0 && !!f0.btn && /Ask the cloud/.test(f0.btn.title) && !/No QR label made/.test(f0.text), f0);
    const c0 = st.calls.length;
    await page.click('.sheetWin [data-r2=label]');
    const asked = await until(() => { const a = st.doc('Charm_Nest_Label_Asks', 'sheet-' + OWN); return a && a.state === 'asked' ? a : null; }, 15000);
    const mid = await foot(page);
    const sent = labelCalls(c0);
    ok('E2 the press sends ONE labelsAsk (its sheet, who pressed), nothing uploaded and no putSheet { label } from the page; the window says it is making it, never refused', !!asked && asked.kind === 'complete' && asked.by === 'Test Operator' && sent.length >= 1 && sent.every(c => c.op === 'labelsAsk') && sent.filter(c => c.op === 'labelsAsk').length === 1 && !/No QR label made|cloud carries every run/i.test(mid.text), { asked, sent: sent.map(c => c.op || c.name), mid });
    // the cloud's labels unit, over the fake site's records (as the driver's pass item "labels" runs it)
    qrPicture();
    const Labels = require(path.join(FN, '_charmNestLabels.js')), lib = st.handlers.charmNestLibrary;
    const run = Object.assign({ runId: R }, st.doc('Charm_Nest_Runs', R));
    const pu = await Labels.passUnit({ db: st.db, run, runId: R, call: (b, u) => lib.call(b, { unit: u }), read: b => lib.call(b, {}), bucket: st.bucket, budgetEnd: Date.now() + 120000, gate: async () => ({ ok: true, why: [] }), say: () => {} }).catch(e => ({ threw: String(e && e.stack || e).split('\n').slice(0, 3).join(' | ') }));
    const recL = (st.doc('Charm_Nest_Sheets', OWN) || {}).label || {};
    ok('E3 the cloud\'s labels unit answers the ask and makes that sheet\'s label (its record says so)', (st.doc('Charm_Nest_Label_Asks', 'sheet-' + OWN) || {}).state === 'done' && recL.own === true && (recL.files || []).length === 1, { pu, ask: st.doc('Charm_Nest_Label_Asks', 'sheet-' + OWN), recL });
    const shown = await until(async () => { const f = await foot(page); const said = await page.evaluate(() => window.__said.slice()); return f.img && said.some(t => /done — the cloud made its QR label/.test(t)) ? { f, said } : null; }, 30000);
    const after = shown || { f: await foot(page), said: await page.evaluate(() => window.__said.slice()) };
    ok('E4 the person sees the cloud\'s answer: "done — the cloud made its QR label", and the window shows the label', !!shown && !after.said.some(t => /No QR label made|cloud carries every run/i.test(t)), after);
    await context.close();
    // the switch "off", a fresh page holding no run: Make QR label makes the label on this computer, as before (no labelsAsk)
    drive('off');
    st.put('Charm_Nest_Sheets', OFF, sheetRec(OFF, { metal: 'gold', day: OLD_DAY, runId: R_OLD, fileBase: 'GF_Nov.14.26_Draft-8', orders: [O3], preview: image }));
    const p2 = await newPage();
    const v2 = await p2.page.evaluate(() => ({ on: CNViewer.on(), lane: !!(window.CharmNestCloudLanes && CharmNestCloudLanes('labels', { runId: 'x' })) }));
    const g0 = await openSheet(p2.page, OFF);
    const c1 = st.calls.length;
    if (g0 && g0.btn) await p2.page.click('.sheetWin [data-r2=label]');
    const made = await until(() => { const l = (st.doc('Charm_Nest_Sheets', OFF) || {}).label; return l && (l.files || []).length ? l : null; }, 30000);
    const sent2 = labelCalls(c1);
    ok('E5 drive off: the press makes the label on this computer as before (its own upload and putSheet { label }), no labelsAsk', !v2.on && !v2.lane && !!g0 && !!g0.btn && !/Ask the cloud/.test(g0.btn.title) && !!made && made.own === true && !sent2.some(c => c.op === 'labelsAsk') && sent2.some(c => c.op === 'putSheet'), { v2, g0, made: !!made, sent2: sent2.map(c => c.op || c.name) });
    await p2.context.close();
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  ok('E no page error', errors.length === 0, errors.slice(0, 3).join(' | '));
  await browser.close(); srv.close();
}

(async () => {
  // (part S in a process of its own: the fake site of part E and the offline cloud of part S each load the library with their own database)
  if (process.env.Q6_PART === 'offline') await partS();
  else {
    if (!process.env.SKIP_PAGE) await partE();
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { Q6_PART: 'offline' }), encoding: 'utf8', timeout: 180000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) /.test(l)) log0(l);
    if (r.status !== 0) failed++;
  }
  log0(failed ? `FAIL viewer-item6 (${failed})` : 'PASS viewer-item6');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item6: ' + (e && e.stack || e)); process.exit(1); });
