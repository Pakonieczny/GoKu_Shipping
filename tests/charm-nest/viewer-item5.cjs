// ITEM 5 (Paul, 21 Nov 2026): a sheet moved into a committed set in the Library, on a staff computer, while the cloud carries every run (Charm_Nest_Open/drive mode "on").
//   Before: the membership was saved, but the set's QR label and set files stayed stale, with a warning: the page made them itself (charm-nest-set-edit.js relabel / remakeFiles: Storage
//   uploads, putSheet {label}, flowApply setFiles) and the cloud refuses every one of those from a page in mode "on"; and where the move did ask the cloud (labelsAsk, charm-nest-flow.js), the
//   set's setFiles ask took the place of its relabel ask (one key a set), so the cloud remade the set's files without the new sheet's label, and the person was told nothing.
//   Now the press is an ask of the cloud's labels unit (SetEdit.askCloud): one relabel ask for each sheet put in, the set's setFiles ask, said as asked; the cloud's answer is said when it comes.
//   S  OFFLINE, the real modules: charm-nest-viewer.js (its own lane from the offline library's driveGet), charm-nest-set-edit.js and charm-nest-flow.js in a vm context (window = self), over
//      the offline library (_cloudFakes.cjs) in mode "on" with "labels" a cloud step: the Library move (LibraryFlow.plan / commit) saves the membership, asks the cloud (no refused write,
//      no warning, the step said as asked), and the REAL driver (tick, charmNestDrive-background pass, the labels unit) makes the new sheet's QR label in its set and the set's labels PDF,
//      manifest and set.json with it; the page's follow says "made by the cloud". The sheet window's Make QR label and the approve's QR label step ask the same way.
//   O  the same move with the drive "off": nothing is asked of the cloud; the page's own relabel and set files run exactly as before.
//   E  the REAL page in headless Chrome over the fake site (bridge-server.cjs), booted while the fake site says mode "on" (so it is a viewer by its own boot): LibraryFlow's move of a draft sheet
//      into a committed set saves the membership and leaves the two asks in the fake cloud, with no refused write and no warning.
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item5.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle5';
const R = 'run-2026-11-21-item5', R2 = 'run-2026-11-21-item5b', SETID = 'set-2026-11-21-2', DAY = '2026-11-21';
const A = '4251000001', N = '4251000002', TA = A + '1', TN = N + '1', PA = `${A}_${TA}_1`, PN = `${N}_${TN}_1`;

/** The records: a committed set (Set 2) of the open run with one GF sheet and its label, and a full GF draft sheet of the same run, in no set. */
function records(T0, labelOf) {
  const sheet = (id, rid, pid, extra) => Object.assign({ id, runId: R, metal: 'gold', day: DAY, status: 'complete', page: 1, placedCount: 1, charmCount: 1, density: 0.41, stock: { wIn: 6, hIn: 4.5 },
    poolIds: [pid], orders: [rid], charms: [{ id: 'c-' + rid, poolId: pid, order: rid, name: 'LEAF_CHARM', sku: 'LEAF_CHARM' }], placements: [{ id: 'c-' + rid, cxPt: 40, cyPt: 40, angle: 0, scale: 1 }],
    verification: { ok: true }, persistedDone: true, releaseFull: true, archived: false, updatedAt: T0 - 60000, createdAt: T0 - 3600e3 }, extra);
  const folder = `charmnest/sets/${DAY}/Set-2`, aPath = `${folder}/GF_${DAY}_Sheet-1`;
  const a = sheet('gf-i5-a', A, PA, { fileBase: `GF_${DAY}_Sheet-1`, draft: false, setId: SETID, setSeq: 2, sheetIndex: 1, outputs: { ai: { path: aPath + '/GF.ai', url: 'https://storage.test.invalid/a.ai' }, pdf: { path: aPath + '/GF.pdf', url: 'https://storage.test.invalid/a.pdf' } } });
  const n = sheet('gf-i5-new', N, PN, { fileBase: `GF_${DAY}_Draft-2`, draft: true, setId: null, outputs: { ai: { path: `charmnest/sheets/${DAY}/GF_${DAY}_Draft-2/GF.ai`, url: 'https://storage.test.invalid/n.ai' }, preview: { path: 'n.png', url: 'https://storage.test.invalid/n.png' } } });
  const la = labelOf(a, folder);
  a.label = { files: [la], orders: [A], by: 'cloud', at: T0 - 50000 };
  const set = { setId: SETID, runId: R, day: DAY, seq: 2, name: 'Set-2', folder, group: 'dispatch', status: 'complete', committedAt: T0 - 40000, committed: [A], sheetIds: ['gf-i5-a'], materials: ['gold'],
    orders: { [A]: { held: null, lines: [{ transactionId: TA, sku: 'LEAF_CHARM', copies: [{ copy: 1, sheetId: 'gf-i5-a', sheet: a.fileBase, poolId: PA }] }] } },
    labelFiles: [Object.assign({ sheetId: 'gf-i5-a', metal: 'gold' }, la)], labels: { pdf: { path: `${folder}/labels/Set-2_labels.pdf`, url: 'u' } }, processSeals: [{ how: 'approved', by: 'Ann', at: T0 - 30000 }], updatedAt: T0 - 40000, createdAt: T0 - 3600e3 };
  const pool = (pid, rid, tid, sheetId, setId) => ({ poolId: pid, orderId: rid, transactionId: tid, lineKey: `${rid}_${tid}`, sku: 'LEAF_CHARM', copy: 1, state: 'placed', sheetId, setId: setId || null, runId: R, material: 'gold', quantity: 1 });
  const run = { runId: R, status: 'complete', step: 'complete', finishedAt: T0 - 20000, mode: 'auto', day: DAY, releasePolicy: 2, setIds: [SETID], setId: SETID, errors: [], orders: [A, N],
    lines: { [`${A}_${TA}`]: { orderId: A, transactionId: TA, state: 'written', poolIds: [PA], sku: 'LEAF_CHARM', material: 'gold', quantity: 1 }, [`${N}_${TN}`]: { orderId: N, transactionId: TN, state: 'written', poolIds: [PN], sku: 'LEAF_CHARM', material: 'gold', quantity: 1 } } };
  return { a, n, set, run, pools: [pool(PA, A, TA, 'gf-i5-a', SETID), pool(PN, N, TN, 'gf-i5-new')] };
}

/** The page's modules in a vm context (window = self), over `call` (the library handler as CN.api sends it: this build's cv, this computer's dev). */
function pageModules(call, o) {
  const said = [], sent = [];
  const w = { console: { log() {}, warn() {}, error() {} }, setTimeout, clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, Date, JSON, Math, Map, Set, URL, TextEncoder,
    require: p => require(path.join(ROOT, p)) };
  w.window = w; w.self = w; w.globalThis = w;
  w.toast = (t, tone) => { said.push({ t: String(t), tone }); };
  w.B = { run: null, employee: 'Michelle' };
  w.CNEmployee = { name: () => 'Michelle' };
  w.CharmNestRunFollow = { state: () => ({ runId: R2 }), dev: () => PAGE, now: async () => ({}) };
  w.CN = { api: async (fn, body) => { sent.push(JSON.parse(JSON.stringify(body))); return call(Object.assign({ cv: CV, dev: PAGE }, body)); }, toast: (t, tone) => said.push({ t: String(t), tone }) };
  const ctx = vm.createContext(w);
  for (const f of ['charm-nest-viewer.js', 'charm-nest-set-edit.js', 'charm-nest-flow.js']) vm.runInContext(fs.readFileSync(path.join(ROOT, f), 'utf8'), ctx, { filename: f });
  const hooksSeen = { relabel: [], files: [], applied: 0 };
  w.LibraryFlow.configure({ employee: () => 'Michelle', rows: () => [], live: () => null,
    relabelSet: async (s, x) => { hooksSeen.relabel.push(s); return o && o.ownPath ? undefined : w.SetEdit.relabel(s, x); },
    setFiles: async (s, x) => { hooksSeen.files.push(s.setId); return o && o.ownPath ? undefined : w.SetEdit.remakeFiles(s, x); },
    applyMembership: async () => { hooksSeen.applied++; } });
  return { w, said, sent, hooksSeen };
}

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';   // (the offline library's door; never set while the real page of part E talks to its fake site)
  const T0 = Date.UTC(2026, 10, 21, 17, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn, qe = console.error; console.log = () => {}; console.warn = () => {}; console.error = () => {};
  try {
    const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), QrLabel = F.load('_charmNestQrLabel.js'), O = require(path.join(ROOT, 'charm-nest-orders.js'));
    // (the label's drawing itself, @resvg/resvg-js, is not installed where this test runs: when it is missing, a fixed 1x1 PNG stands for each drawn label; every other part is the real unit)
    try { QrLabel.renderPng({ payload: 'x', label: 'x', notes: [] }); } catch (_) { const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==', 'base64'); QrLabel.renderPng = () => ({ png, ecc: 'M' }); }
    const answers = [];
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} const out = Object.assign({}, o, { http: r.statusCode }); answers.push({ op: body.op, http: r.statusCode, error: o.error || null, cloudCarries: !!o.cloudCarries }); return out; };
    // a sheet's QR label as the cloud drew it, its PNG in the bucket (the set's labels PDF is made from the bucket's copies)
    const labelOf = (sh, folder) => {
      const payload = O.encodeOrderList(sh.orders, 'GF'), label = `GF · Set-2 · Sheet 1 · 1 order`, img = QrLabel.renderPng({ payload, label, notes: [] }), p = `${folder}/${sh.fileBase}/${sh.fileBase}_label.png`;
      F.bucket.file(p).save(img.png, { contentType: 'image/png' });
      return { path: p, url: 'https://storage.test.invalid/' + encodeURIComponent(p), sheet: sh.fileBase, part: 1, parts: 1, orders: sh.orders, payload, ecc: img.ecc, label };
    };
    const seed = mode => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: mode === 'off' ? [] : ['labels', 'files', 'engrave', 'takeoff'], flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Open/run', { runId: R2, at: T0 - 600e3 }); F.seed('Charm_Nest_Runs/' + R2, { runId: R2, status: 'running', step: 'nest', mode: 'auto', day: DAY, releasePolicy: 2, errors: [], orders: [], lines: {} });
      F.seed('Charm_Nest_Run_Carrier/' + R2, { runId: R2, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
      const rec = records(T0, labelOf);
      F.seed('Charm_Nest_Runs/' + R, rec.run);
      F.seed('Charm_Nest_Sheets/' + rec.a.id, rec.a); F.seed('Charm_Nest_Sheets/' + rec.n.id, rec.n); F.seed('Charm_Nest_Sets/' + SETID, rec.set);
      for (const p of rec.pools) F.seed('Charm_Pool/' + p.poolId, p);
      F.seed('Charm_World/summary', { at: T0 - 60000, live: 0, mirrorBefore: T0 - 60000 });
      F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: T0 - 60000 });
      return rec;
    };

    /* ═══ S · mode "on": the viewer's Library move asks the cloud, and the cloud makes the label and the set's files ═══ */
    seed('on');
    const pg = pageModules(P);
    await pg.w.CNViewer.ready();
    ok('S0 the page reads the switch itself (charm-nest-viewer.js over the offline library): mode "on", "labels" a cloud step, CharmNestCloudLanes("labels") true',
      pg.w.CNViewer.on() === true && pg.w.CharmNestCloudLanes('labels', { type: 'relabel' }) === true && pg.w.SetEdit.cloudDraws({ type: 'setFiles', setId: SETID }) === true, pg.w.CNViewer.state());
    const LF = pg.w.LibraryFlow;
    const plan = await LF.plan({ kind: 'sheet', id: 'gf-i5-new', to: { set: SETID }, by: 'Michelle' });
    ok('S1 the plan for the drop of GF Draft 2 on committed Set 2: the membership, a relabel of the sheet put in, the set\'s files', plan.ok !== false && JSON.stringify(plan.steps.map(s => s.type)) === JSON.stringify(['setMember', 'relabel', 'setFiles']), { needs: plan.needs, steps: plan.steps.map(s => s.type), error: plan.error });
    const a0 = answers.length;
    const res = await LF.commit(plan, { by: 'Michelle', confirmed: (plan.confirm || []).map(c => c.key) });
    const pageOps = answers.slice(a0), refused = pageOps.filter(x => x.http >= 400);
    const asks = F.list('Charm_Nest_Label_Asks'), relabelAsk = asks.find(x => x.kind === 'relabel'), filesAsk = asks.find(x => x.kind === 'setFiles');
    ok('S2 the move is saved: GF Draft 2 is in Set 2 (the sheet\'s record and the set\'s list)', res.ok === true && F.doc('Charm_Nest_Sheets/gf-i5-new').setId === SETID && F.doc('Charm_Nest_Sheets/gf-i5-new').draft === false && F.doc('Charm_Nest_Sets/' + SETID).sheetIds.includes('gf-i5-new'), { res, sheet: F.doc('Charm_Nest_Sheets/gf-i5-new') });
    ok('S3 nothing the page sent was refused (no upload, no putSheet label, no flowApply setFiles from the page): only the membership and the asks',
      !refused.length && !pageOps.some(x => x.op === 'putSheet') && pg.sent.filter(b => b.op === 'flowApply').every(b => (b.steps || []).every(s => s.type === 'setMember')), { refused, ops: pageOps.map(x => x.op) });
    ok('S4 the cloud is asked twice, in Michelle\'s name, and keeps both asks: the QR label of the sheet put in (relabel, by sheet) and Set 2\'s files (setFiles, by set)',
      asks.length === 2 && relabelAsk && relabelAsk.sheetId === 'gf-i5-new' && relabelAsk.state === 'asked' && relabelAsk.by === 'Michelle' && filesAsk && filesAsk.setId === SETID && filesAsk.state === 'asked' && filesAsk.by === 'Michelle', asks);
    const lines = (res.applied || []).map(a => a.label);
    ok('S5 the person is told: each step said as asked of the cloud (never "made"), no warning', !(res.warnings || []).length && lines.some(l => /QR label for Set 2: asked of the cloud/.test(l)) && lines.some(l => /Set 2's labels PDF and manifest: asked of the cloud/.test(l)) && !lines.some(l => /QR label made/.test(l)),
      { lines, warnings: res.warnings });
    ok('S6 the page\'s own label path never ran (the flow asked the cloud itself; set-edit wrote nothing)', !pg.hooksSeen.relabel.length && !pg.hooksSeen.files.length && pg.hooksSeen.applied === 1, pg.hooksSeen);

    // the cloud: the mail system's copy and the world are fresh since the press (the setFiles freshness gate), then the real driver: its tick, its pass (charmNestDrive-background), the labels unit
    F.clock.advance(30000);
    F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: Date.now() - 1000 });
    F.seed('Charm_World/summary', { at: Date.now(), live: 0, mirrorBefore: Date.now() - 500 });
    let passes = 0;
    for (let i = 0; i < 6 && F.list('Charm_Nest_Label_Asks').some(x => x.state !== 'done' && x.state !== 'refused'); i++) {
      await Drive.tick({ db: F.db, kick: Drive.kick });
      passes += await F.drain(4);
      F.clock.advance(61000);
    }
    const asks2 = F.list('Charm_Nest_Label_Asks'), sn = F.doc('Charm_Nest_Sheets/gf-i5-new'), set2 = F.doc('Charm_Nest_Sets/' + SETID);
    const files = (sn.label && sn.label.files) || [], f0 = files[0] || {};
    ok('S7 the cloud answered both asks: done (the real driver\'s pass, the labels unit)', passes > 0 && asks2.length === 2 && asks2.every(x => x.state === 'done'), { passes, asks: asks2.map(x => ({ key: x.key, kind: x.kind, state: x.state, why: x.why })) });
    ok('S8 GF Draft 2\'s QR label is made by the cloud in Set 2: its PNG in the bucket, its code the orders on it, its title names Set-2', files.length === 1 && F.blobs.has(f0.path) && f0.payload === O.encodeOrderList([N], O.CARD_TO_METAL.gold || 'gold') && /Set-2/.test(f0.label || '') && sn.label.by === 'cloud',
      { label: sn.label, blobs: [...F.blobs.keys()] });
    const json = (() => { try { return JSON.parse(F.blobs.get(`${set2.folder}/set.json`).buf.toString('utf8')); } catch (_) { return null; } })();
    ok('S9 Set 2\'s label list carries both sheets\' labels', (set2.labelFiles || []).some(f => f.sheetId === 'gf-i5-new' && f.path === f0.path) && (set2.labelFiles || []).some(f => f.sheetId === 'gf-i5-a'), set2.labelFiles);
    ok('S10 Set 2\'s files are made again by the cloud WITH the sheet put in: labels PDF, manifest and set.json in the bucket and on the set, set.json listing GF Draft 2 and its order',
      set2.labels && set2.labels.pdf && F.blobs.has(set2.labels.pdf.path) && set2.labels.manifest && F.blobs.has(set2.labels.manifest.path) && json && json.sheets.some(s => s.sheetId === 'gf-i5-new') && json.labels.some(l => l.path === f0.path) && !!json.orders[N] && /^complete/.test(set2.status) && set2.committedAt,
      { labels: set2.labels, json: json && { sheets: json.sheets.map(s => s.sheetId), labels: json.labels.map(l => l.path), orders: Object.keys(json.orders) }, status: set2.status });
    // the person's follow (the page polls the asks): what it says once the cloud answered
    const said = [];
    const end = await pg.w.SetEdit.followAsks([relabelAsk, filesAsk].map(x => Object.assign({ what: x.kind === 'relabel' ? 'The QR label for Set-2' : "Set-2's labels PDF and manifest" }, x)), { api: b => P(Object.assign({ cv: CV, dev: PAGE }, b)), wait: async () => {}, every: 1, ms: 60000, say: (t, tone) => said.push([tone, t]) });
    ok('S11 the page says the cloud\'s answer when it comes: "made by the cloud", once each', Object.values(end).every(x => x.state === 'done') && said.length === 2 && said.every(([tone, t]) => tone === 'ok' && /made by the cloud$/.test(t)), said);

    /* the other presses that make a sheet's label from a viewer: the approve's QR label step and the sheet window's Make QR label, each one ask of the cloud */
    F.clock.advance(5000);
    const q0 = answers.length;
    const qr = await pg.w.SetEdit.askCloud({ type: 'qrLabel', sheetId: 'gf-i5-a', inSet: true }, { by: 'Michelle', api: b => P(Object.assign({ cv: CV, dev: PAGE }, b)) });
    const own = await pg.w.SetEdit.askCloud({ type: 'qrLabel', sheetId: 'gf-i5-new', complete: true }, { by: 'Michelle', api: b => P(Object.assign({ cv: CV, dev: PAGE }, b)) });
    ok('S12 a sheet\'s QR label from a viewer (approve step: relabel of a sheet in its set; Make QR label: the cloud\'s "complete"): one ask each, nothing refused, said as asked',
      qr.asks[0] && qr.asks[0].kind === 'relabel' && qr.asks[0].sheetId === 'gf-i5-a' && own.asks[0] && own.asks[0].kind === 'complete' && answers.slice(q0).every(x => x.http < 400) && /asked of the cloud/.test(qr.label) && pg.said.some(s => /asked of the cloud/.test(s.t)), { qr, own, ops: answers.slice(q0) });
    let threw = ''; try { await pg.w.SetEdit.askCloud({ type: 'qrLabel', sheetId: 'gf-i5-new' }, { by: 'Michelle', api: P }); } catch (e) { threw = e.message; }
    ok('S13 a QR label step for a sheet in no set that is not a Make QR label press is never turned into a release: it says where to press', /open the sheet and press Make QR label/.test(threw), threw);
    // a direct call of the page's own relabel and set files (charm-nest-set-edit.js) on a viewer: an ask, never an upload
    const d0 = answers.length;
    const dr = await pg.w.SetEdit.relabel({ type: 'relabel', sheetIds: ['gf-i5-new'], setId: SETID }, { by: 'Michelle', toast: false });
    const df = await pg.w.SetEdit.remakeFiles({ setId: SETID }, { by: 'Michelle', toast: false });
    ok('S14 SetEdit.relabel / remakeFiles on a viewer ask the cloud (labelsAsk) and write nothing themselves', dr.asks[0].kind === 'relabel' && df.asks[0].kind === 'setFiles' && JSON.stringify(answers.slice(d0).map(x => x.op)) === JSON.stringify(['labelsAsk', 'labelsAsk']) && answers.slice(d0).every(x => x.http < 400), answers.slice(d0));

    /* ═══ O · drive "off": the same move; nothing asked of the cloud, the page's own label and files as before ═══ */
    seed('off');
    const po = pageModules(P, { ownPath: true });
    await po.w.CNViewer.ready();
    const o0 = answers.length;
    const planO = await po.w.LibraryFlow.plan({ kind: 'sheet', id: 'gf-i5-new', to: { set: SETID }, by: 'Michelle' });
    const resO = await po.w.LibraryFlow.commit(planO, { by: 'Michelle', confirmed: (planO.confirm || []).map(c => c.key) });
    ok('O1 drive off: the move is saved, the page\'s own relabel and set files run (as before), nothing is asked of the cloud, nothing refused, and the steps say what they always said',
      po.w.CNViewer.on() === false && po.w.SetEdit.cloudDraws({ type: 'relabel' }) === false && resO.ok === true && F.doc('Charm_Nest_Sheets/gf-i5-new').setId === SETID
      && po.hooksSeen.relabel.length === 1 && po.hooksSeen.relabel[0].sheetIds.includes('gf-i5-new') && JSON.stringify(po.hooksSeen.files) === JSON.stringify([SETID])
      && !answers.slice(o0).some(x => x.op === 'labelsAsk') && !F.list('Charm_Nest_Label_Asks').length && answers.slice(o0).every(x => x.http < 400) && (resO.applied || []).some(a => a.key === 'qrLabel' && /QR label made for Set 2/.test(a.label)),
      { ok: resO.ok, err: resO.error, hooks: po.hooksSeen, ops: answers.slice(o0).map(x => x.op), applied: (resO.applied || []).map(a => a.label) });
  } finally { console.log = quiet; console.warn = qw; console.error = qe; }
}

async function partE() {
  let Base; try { Base = require('./_orderFactsWorld.cjs'); } catch (e) { ok('E world', false, String(e)); return; }
  const w = await Base.world({ viewport: { width: 1440, height: 900 }, facts: false });
  if (!w) { log0('E: no playwright-core: the real page was not run'); return; }
  const page = w.page, st = w.srv.st;
  try {
    const T0 = Date.now(), rec = records(T0, (sh, folder) => ({ path: `${folder}/${sh.fileBase}/${sh.fileBase}_label.png`, url: 'data:image/png;base64,', sheet: sh.fileBase, part: 1, parts: 1, orders: sh.orders, payload: 'p-a', label: 'GF · Set-2 · Sheet 1 · 1 order' }));
    const ts = { toMillis: () => T0 };
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'manual', cloudSteps: ['labels', 'files', 'engrave', 'takeoff'], flippedAt: T0 - 3600e3 });
    st.put('Charm_Nest_Open', 'run', { runId: R, at: T0 - 3600e3 });
    st.put('Charm_Nest_Run_Carrier', R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0 + 3600e3, since: T0 - 3600e3 } });
    st.put('Charm_Nest_Runs', R, rec.run);
    st.put('Charm_Nest_Sheets', rec.a.id, Object.assign({}, rec.a, { updatedAt: ts, createdAt: ts })); st.put('Charm_Nest_Sheets', rec.n.id, Object.assign({}, rec.n, { updatedAt: ts, createdAt: ts }));
    st.put('Charm_Nest_Sets', SETID, Object.assign({}, rec.set, { updatedAt: ts, createdAt: ts }));
    for (const p of rec.pools) st.put('Charm_Pool', p.poolId, p);
    for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {}
    // the page boots again while the fake site says "on": a viewer by its own boot (CNViewer reads the switch first)
    await page.reload({ waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.LibraryFlow && window.CNViewer && CNViewer.known() && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const v = await page.evaluate(R => { const F = window.CharmNestRunFollow, st0 = F.state; F.state = () => Object.assign(st0(), { runId: R }); window.__said = []; const t0 = window.toast; window.toast = function (t) { window.__said.push(String(t)); return t0 && t0.apply(this, arguments); };
      return { on: CNViewer.on(), lanes: CharmNestCloudLanes('labels', { type: 'relabel' }), frozen: !!(window.Session && Session.frozen && Session.frozen()), run: !!(window.B && B.run) }; }, R);
    ok('E0 the real page booted as a viewer (mode "on" read at its boot, workspace frozen, no run held), the labels lane the cloud\'s', v.on === true && v.lanes === true && v.frozen === true && v.run === false, v);
    const out = await page.evaluate(async SETID => {
      const plan = await LibraryFlow.plan({ kind: 'sheet', id: 'gf-i5-new', to: { set: SETID }, by: 'Test Operator' });
      const res = await LibraryFlow.commit(plan, { by: 'Test Operator', confirmed: (plan.confirm || []).map(c => c.key) });
      return { steps: plan.steps.map(s => s.type), needs: plan.needs, ok: res.ok, error: res.error || null, warnings: res.warnings || [], applied: (res.applied || []).map(a => a.label), said: window.__said.slice() };
    }, SETID);
    const asks = [...st.docs.keys()].filter(k => /^Charm_Nest_Label_Asks\//.test(k)).map(k => st.docs.get(k));
    const sheet = st.docs.get('Charm_Nest_Sheets/gf-i5-new') || {};
    const refused = (st.calls || []).filter(c => c && c.status >= 400 && /cloud carries/i.test(String(c.error || '')));
    ok('E1 the real page\'s Library move on the viewer: the membership saved, the cloud asked for the sheet\'s QR label and the set\'s files, said as asked, no warning',
      out.ok === true && sheet.setId === SETID && asks.some(a => a.kind === 'relabel' && a.sheetId === 'gf-i5-new' && a.state === 'asked') && asks.some(a => a.kind === 'setFiles' && a.setId === SETID && a.state === 'asked') && !out.warnings.length && out.applied.some(l => /asked of the cloud/.test(l)) && !refused.length,
      { out, asks: asks.map(a => a && { kind: a.kind, setId: a.setId, sheetId: a.sheetId, state: a.state, by: a.by }), setId: sheet.setId, refused: refused.slice(0, 3) });
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  ok('E no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
  await w.close();
}

(async () => {
  // (part S in a process of its own: the fake site of part E and the offline cloud of part S each load the library with their own database, and one process keeps one copy of it)
  if (process.env.I5_PART === 'offline') { await partS(); }
  else {
    if (process.env.I5_PAGE) await partE();   // (part E, the real page, is opt-in: I5_PAGE=1; not yet run green)
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { I5_PART: 'offline' }), encoding: 'utf8', timeout: 300000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) /.test(l)) log0(l);
    if (r.status !== 0) { failed++; if (!/^(OK|FAIL) /m.test(out)) log0(out.slice(-2000)); }
  }
  log0(failed ? `FAIL viewer-item5 (${failed})` : 'PASS viewer-item5');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item5: ' + (e && e.stack || e)); process.exit(1); });
