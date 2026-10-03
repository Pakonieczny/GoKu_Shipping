// The sheet window's own Make QR label and Include paths, reached without the window by the Library's moves
// (SheetWin.joinInfo / joinSet / remakeLabel, used by charm-nest-flow.js): the real functions of charm-nest-sheetwin.js run in
// node's vm over stand-ins for the page (the open run, its sheets, Gate, Sets, the cloud). Nothing leaves the process.
//   node tests/charm-nest/library-flow-sheetwin.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const src = fs.readFileSync(path.join(__dirname, '../../charm-nest-sheetwin.js'), 'utf8');
const part = (a, b) => { const i = src.indexOf(a); assert(i >= 0, 'not found: ' + a); const j = src.indexOf(b, i); assert(j > i, 'not found: ' + b); return src.slice(i, j); };

const plain = x => JSON.parse(JSON.stringify(x));   // (objects made inside the vm are of another realm)
(async () => {
  const calls = [], toasts = [];
  let sheets = [], records = {}, assembleMode = 'join', changed = null;
  const run = { runId: 'run-1', status: 'processed' };
  const api = async (fn, body) => {
    calls.push(body.op + (body.sheet ? ':' + body.sheet.id : body.id ? ':' + body.id : ''));
    if (body.op === 'getSheet') return { sheet: records[body.id] || null };
    if (body.op === 'putSheet') { const sh = sheets.find(p => p.sheetId === body.sheet.id); if (sh && 'releaseFull' in body.sheet) sh.putRelease = body.sheet.releaseFull; return { ok: true }; }
    return { ok: true };
  };
  const Gate = {
    modern: id => id === 'run-1',
    splitWith: (sh, inc) => sheets.filter(p => p !== sh && p.metal.startsWith('gold1') && p.split).map(p => ({ sheet: p, orders: ['5001'] })).filter(() => sh.split),
    policy: () => ({ reason: 'the run keeps it out' }),
    assemble: async r => {
      calls.push('assemble');
      for (const sh of sheets) {
        if (assembleMode === 'join' && sh.draft && sh.releaseFull) Object.assign(sh, { draft: false, setId: 'set-9', label: { files: [{ path: 'l.png' }] } });
        else if (assembleMode === 'label' && sh.setId && !sh.draft) sh.label = { files: [{ path: 'again.png' }] };
      }
    },
    changeMembership: async (m, inc, list) => { changed = { m, inc, ids: list.map(p => p.sheetId) }; for (const sh of list) Object.assign(sh, { draft: false, setId: 'set-9', label: { files: [{ path: 'l.png' }] } }); }
  };
  const Sets = { ofRun: () => [{ group: 'dispatch', setId: 'set-9', seq: 9, committedAt: null }], renderLabelPng: async () => ({ blob: 'png', ecc: 'M' }) };
  const CN = { METAL_TAG: { gold: 'GF' }, uploadBytes: async p => ({ path: p, url: 'https://x/' + p }) };
  const O = { CARD_TO_METAL: {}, safeChunks: ids => [ids], encodeOrderList: ids => 'code:' + ids.join(',') };
  const c = { console, Set, Map, Math, JSON, Object, Array, String, Number, Promise, Error, Date, api, Gate, Sets, CN, O, calls, toasts, setSheets: l => { sheets = l; }, getSheets: () => sheets, getChanged: () => changed };
  vm.createContext(c);
  vm.runInContext(`
    const CODE = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
    const W = { rec: null, id: null, flow: null, el: {}, dlg: { open: false } };
    const S = { mode: 'nest' }, fmt = { pct: v => Math.round(v * 100) + '%' }, nowT = () => 1;
    let __sheets = []; const allSheets = () => getSheets();
    const BUSY = ['nesting', 'finishing', 'queued'], busy = sh => BUSY.includes(sh.status) || !!(sh.persisted && !sh.persistedDone && !sh.problem);
    const sentToStation = sh => false;
    const sheetName = sh => sh.metal + ' · sheet ' + sh.page, ridOf = c => c.order, sheetNoOf = r => r.sheetIndex || 1, whoAmI = () => 'Paul';
    const agent = () => {}, toast = (m) => toasts.push(m), open2 = () => {}, askSplit = async () => null;
    const window = { B: { run: ${JSON.stringify(run)} }, Gate, Sets, CN, CharmNestOrders: O, Session: { schedule() {} }, SheetEvents: { qrLabel() {} } };
    const B = window.B, Session = window.Session;
  `, c);
  vm.runInContext(part('function labelPlan()', 'function renderMenu()') + '\n' + part('const savedSheet = async', 'window.SheetWin = { remakeLabel') + '\nthis.X = { joinInfo, joinSet, remakeLabel, labelPlanOf };', c);
  const { joinInfo, joinSet, remakeLabel } = c.X;

  const sheet = over => Object.assign({ metal: 'gold', page: 1, sheetId: 'gold-1', runId: 'run-1', status: 'complete', dirty: false, draft: true, persistedDone: true, verification: { ok: true }, density: .7,
    charms: [{ id: 'a', order: '4001' }], placements: [{ id: 'a' }], rejects: [] }, over);
  const reset = list => { calls.length = 0; toasts.length = 0; assembleMode = 'join'; vm.runInContext('0', c); c.setSheets(list); };

  // ── what the open run can do for a sheet now ──
  reset([sheet(), sheet({ sheetId: 'rose-1', metal: 'rose', page: 2 }), sheet({ sheetId: 'in-set', draft: false, setId: 'set-9', label: { files: [] } }), sheet({ sheetId: 'busy-1', status: 'nesting' }), sheet({ sheetId: 'other-run', runId: 'run-0' })]);
  let i = joinInfo('gold-1'); assert.deepEqual(plain(i), { runHere: true, draft: true, dispatchSetId: 'set-9', can: { ok: true, byHand: true, reason: '' }, split: [] });
  i = joinInfo('rose-1'); assert.equal(i.can.ok, false); assert(/Cut Sheet/.test(i.can.reason), i.can.reason);
  i = joinInfo('in-set'); assert.equal(i.can.ok, false); assert.equal(i.draft, false); assert(/already in a set/.test(i.can.reason), i.can.reason);
  i = joinInfo('busy-1'); assert.equal(i.can.ok, false); assert(/nested or saved/.test(i.can.reason), i.can.reason);
  i = joinInfo('other-run'); assert.equal(i.runHere, false); assert.equal(i.can.ok, false);
  assert.equal(joinInfo('nope'), null);
  assert(!calls.length, 'reading changes nothing');

  // ── a sheet joins its set with its label (the button's own path), and a failure puts back what it changed ──
  await joinSet('gold-1'); const g = c.getSheets()[0]; assert.equal(g.draft, false); assert.equal(g.setId, 'set-9'); assert(g.label.files.length);
  assert.deepEqual(calls.filter(x => /^(putSheet|assemble)/.test(x)), ['putSheet:gold-1', 'assemble'], calls.join());
  reset([sheet()]); assembleMode = 'none';
  await assert.rejects(joinSet('gold-1'), /the run keeps it out/); assert.equal(c.getSheets()[0].releaseFull, false, 'the release is put back'); assert.equal(c.getSheets()[0].draft, true);
  await assert.rejects(joinSet('rose-1'), /cannot join a set from here/);
  await assert.rejects(joinSet('in-set'), /cannot join a set from here/);

  // ── a 10K/14K sheet: Include, and an order on two sheets needs the person's choice ──
  reset([sheet({ sheetId: 'k10-1', metal: 'gold10k', split: true }), sheet({ sheetId: 'k10-2', metal: 'gold10k', page: 2, split: true })]);
  i = joinInfo('k10-1'); assert.equal(i.can.ok, true); assert.equal(i.can.byHand, false); assert.deepEqual(plain(i.split.map(x => x.label + ':' + x.orders)), ['10K Sheet 2:5001']);
  await assert.rejects(joinSet('k10-1'), /needs your choice/); assert.equal(c.getChanged(), null);
  await joinSet('k10-1', { split: 'this' }); assert.deepEqual(plain(c.getChanged()), { m: 'gold10k', inc: true, ids: ['k10-1'] }); assert.equal(c.getSheets()[1].draft, true, 'the other sheet stays out');

  // ── the label of a sheet in its set, or of its own, made again ──
  reset([sheet({ sheetId: 'in-set', draft: false, setId: 'set-9', label: null })]); records['in-set'] = { id: 'in-set' }; assembleMode = 'label';
  await remakeLabel('in-set'); assert(c.getSheets()[0].label.files.length); assert(calls.includes('assemble'));
  reset([sheet({ sheetId: 'in-set', draft: false, setId: 'set-9', label: null })]); assembleMode = 'none';
  await assert.rejects(remakeLabel('in-set'), /the run keeps it out/);
  records.own = { id: 'own', metal: 'gold', day: '2026-10-03', sheetIndex: 1, runId: 'run-0', fileBase: 'GF_Sheet-1', placements: [{ id: 'a' }], charms: [{ id: 'a', order: '4001' }], verification: { ok: true }, outputs: { ai: { path: 'charmnest/sheets/x/own.ai' } } };
  reset([sheet({ sheetId: 'own', runId: 'run-0', draft: false, setId: 'set-1', label: null })]);
  await remakeLabel('own'); assert(calls.includes('putSheet:own'), calls.join()); assert(!calls.includes('assemble'), 'a sheet outside the open run gets a label of its own');
  reset([sheet()]); await assert.rejects(remakeLabel('nope'), /no longer in the Library/);
  reset([sheet({ sheetId: 'gold-1' })]); records['gold-1'] = { id: 'gold-1' };
  await assert.rejects(remakeLabel('gold-1'), /cannot be made from here/, 'a sheet that has not joined a set is joined, not relabelled');
  assert(!toasts.length, 'no pop-up for a move');
  console.log('PASS: sheet window paths for the Library moves: join info, join a set with its label, Include with its split choice, label made again, failures put back what they changed');
})().catch(e => { console.error(e); process.exitCode = 1; });
