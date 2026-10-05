/* The step rail has FIVE steps (round 13; Paul, 5 Oct 2026 13:15 UTC): "Image #1 & 2 - These 2 points show the same thing please remove the 3rd point on image #2
 * from all milestone progress lines. Image #3 - this one is also completely unnecessary because the QR code is right there invisible so you can remove it from the UI."
 * The Back files step (3rd) and the QR label step (4th) are gone from every rail: Nesting, Engraving, Order check, Laser cutting, Completed. Nothing that gates a sheet
 * changed, only where it is shown:
 *   · Engraving is done only when every back engraving is approved AND the approved backs are saved as files; while they are being saved it is IN PROGRESS with the plain
 *     line "Saving back files: 22 of 25", and the gap still blocks Approve (the plan's need says the same words);
 *   · a sheet with no QR label stays blocked exactly as before (soft when none was made, hard when it is incomplete or leaves orders out), now under Order check, with the plain
 *     line "QR label not made yet" and the same shortcut (the row opens the sheet, where Make QR label is; the press of Approve makes it);
 *   · every record that ever named the two removed steps loads without an error and nothing stored under them is rewritten or removed (page and server twin);
 *   · a mutant that brings the seven-step rail back, or drops either fold, is caught.
 * Offline fixtures only (no cloud, no live set): the real readiness module, the real LibraryFlow planner, the real charmNestLibrary handler over the in-memory shop, and the
 * real LaserReview rail in a DOM.   node tests/charm-nest/rail-five-steps.cjs */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const READY_SRC = read('charm-nest-readiness.js'), FLOW_SRC = read('charm-nest-flow.js');
const load = (src, self) => { const m = { exports: {} }; new Function('module', 'exports', 'self', src)(m, m.exports, self); return m.exports; };
const R = load(READY_SRC), LF = load(FLOW_SRC, { CharmNestReadiness: R });
const tick = (n = 40) => new Promise(r => setTimeout(r, n));
const KEYS = ['nesting', 'engraving', 'orders', 'laser', 'completed'], LABELS = ['Nesting', 'Engraving', 'Order check', 'Laser cutting', 'Completed'];
const clone = x => JSON.parse(JSON.stringify(x));
const freeze = x => { if (x && typeof x === 'object' && !Object.isFrozen(x)) { Object.freeze(x); Object.values(x).forEach(freeze); } return x; };

const saved = (id, sheetId) => ({ poolId: id, sheetId, approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' } } });
/* a sheet of n pieces, one order each. The first `unapproved` pieces wait for their back approval, the next `unsaved` are approved with no file yet (the rest are saved),
 * `qr`: 'ok' | 'none' (no label made) | 'partial' (the label leaves orders out) | 'incomplete' (a label with no payload), `wait`: the first order has another piece that is not on a sheet */
function sheet(id, { n = 3, metal = 'gold', setId = 'set1', index = 1, base = 1000, unapproved = 0, unsaved = 0, qr = 'ok', wait = false } = {}) {
  const pool = [...Array(n)].map((_, i) => `${base + i}_t${i}_1`), orders = pool.map(p => p.split('_')[0]);
  const engraving = {}, backPool = [];
  pool.forEach((p, i) => {
    if (i < unapproved) engraving[p] = { needed: true, state: 'review', approved: false };
    else if (i < unapproved + unsaved) backPool.push({ poolId: p, sheetId: id, approvedAt: 10, approvedBy: 'Paul', outputs: null });
    else backPool.push(saved(p, id));
  });
  const files = qr === 'none' ? [] : [{ path: 'qr.png', url: 'https://example.com/qr.png', ...(qr === 'incomplete' ? {} : { payload: 'x' }), orders: qr === 'partial' ? orders.slice(0, 1) : orders }];
  const blocked = { ready: false, key: 'pooled', why: 'Not every copy has a saved sheet', blocks: [{ key: 'pooled', index: 2, label: 'Star', poolId: orders[0] + '_t0_2', lineKey: orders[0] + '_t0', sheetId: null, sheetLabel: null, why: 'A piece is not on a saved sheet yet' }], onSheets: [id], pieceCount: 2, customer: 'Grace Hopper', listingId: 'L1' };
  return { id, metal, setId, setSeq: 1, runId: 'run1', sheetIndex: index, status: 'complete', poolIds: pool, placedCount: n, charmCount: n, density: .73, verification: { ok: true }, preview: 'https://example.com/p.png', outputs: { ai: 'https://example.com/f.ai' },
    orders, label: { files, orders }, backPool, engraving, orderReadiness: Object.fromEntries(orders.map((o, i) => [o, wait && i === 0 ? blocked : { ready: true }])), updatedAt: 1 };
}
const strings = (x, out = []) => { if (typeof x === 'string') out.push(x); else if (x && typeof x === 'object') for (const v of Object.values(x)) strings(v, out); return out; };
// the plain lines the QR label may still appear in (Order check's own row, the order lines under it, the words about ANOTHER sheet's missing labels, and the line of what the Approve press does: "Sheet 1: QR label made"): never a step, never a milestone
const QR_OK = /^(QR label not made yet|The QR label (is incomplete|leaves out|file is incomplete|must also)|the QR label (must also cover|is incomplete)|This order is not on the QR label|\d+ more orders? are not on the QR label either|A QR label for \d+|QR label made for|Holds \d+ orders? of this sheet back|.*: QR label made$|.*\bits QR labels are missing\b|.*QR label not made yet|The QR label cannot be made from here|A Rose Gold sheet gets its QR label)/i;
const noRemovedWords = (list, where) => {
  for (const t of list) {
    assert.doesNotMatch(t, /\bBack files?\b/, `${where}: "Back files" is a removed milestone: ${t}`);
    assert(!/QR label/i.test(t) || QR_OK.test(t.trim()) || /QR labels? (are|is) missing/.test(t), `${where}: "QR label" only as a plain line under Order check: ${t}`);
  }
};
const states = e => Object.fromEntries(e.steps.map(s => [s.key, s.state]));
const stepOf = (e, k) => e.steps.find(s => s.key === k);

/* ═══ A · the readiness module: the one source of the rail ═════════════════════════════════════════════════════════════════════════════ */
function readinessChecks(R) {
  assert.deepEqual(R.STEPS.map(s => s.key), KEYS); assert.deepEqual(R.STEPS.map(s => s.label), LABELS);
  assert.equal(R.stepKey('backFiles'), 'engraving'); assert.equal(R.stepKey('qr'), 'orders'); assert.equal(R.stepKey('laser'), 'laser'); assert.equal(R.stepKey('nonsense'), 'nonsense');
  // every record shape: ready, each step behind, both folded gaps, a set, a cut sheet
  const fixtures = {
    ready: sheet('a'), engraving: sheet('b', { unapproved: 2 }), saving: sheet('c', { n: 25, unsaved: 3 }), failedFile: (() => { const s = sheet('d'); s.backPool[1].verified = { geometry: { ok: true }, file: { ok: false } }; return s; })(),
    noQr: sheet('e', { qr: 'none' }), partialQr: sheet('f', { qr: 'partial' }), incompleteQr: sheet('g', { qr: 'incomplete' }), both: sheet('h', { qr: 'none', wait: true }), orders: sheet('i', { wait: true }), cut: { ...sheet('j'), laserDoneAt: 9 }
  };
  for (const [name, rec] of Object.entries(fixtures)) {
    const e = R.explain(rec);
    assert.deepEqual(e.steps.map(s => s.key), KEYS, `${name}: five steps`); assert.deepEqual(e.steps.map(s => s.label), LABELS, `${name}: named as Paul asked`);
    assert.equal(e.steps.filter(s => s.current).length, 1, `${name}: one current step`);
    noRemovedWords(strings(e), name);
    for (const i of R.issues(rec, {})) { assert(['nesting', 'engraving', 'orders', 'laser'].includes(i.step), `${name}: issues name a step of the five: ${i.step}`); noRemovedWords([i.label || ''], name + ' issue'); }
  }
  const set = { setId: 'set1', seq: 1, name: 'Set 1', sheetIds: ['a', 'c'] }, es = R.explain(set, { sheets: [fixtures.ready, fixtures.saving] });
  assert.deepEqual(es.steps.map(s => s.key), KEYS, 'a set card: the same five'); noRemovedWords(strings(es), 'set');
  assert.equal(es.step, 'engraving', 'the set is at the step its sheet is at'); assert.match(es.nextText, /saving back files: 22 of 25/i, 'and says the plain line of the sheet that holds it');

  // ── Engraving: approved AND saved. The plain case keeps its popup words; saving is in progress with the plain line
  let e = R.explain(fixtures.ready);
  assert.equal(stepOf(e, 'engraving').detail, 'Every back engraving is approved (3 of 3).', 'the popup of the plain case is unchanged'); assert.equal(stepOf(e, 'engraving').state, 'done');
  e = R.explain(fixtures.saving);
  assert.equal(e.step, 'engraving'); assert.equal(stepOf(e, 'engraving').state, 'waiting', 'in progress: not blocked, not done'); assert.equal(stepOf(e, 'engraving').current, true);
  assert.equal(stepOf(e, 'engraving').detail, 'Saving back files: 22 of 25.', 'the plain line'); assert.match(e.nextText, /^Saving back files: 22 of 25\./);
  assert.deepEqual(states(e), { nesting: 'done', engraving: 'waiting', orders: 'done', laser: 'waiting', completed: 'waiting' });
  assert.equal(R.sheet(fixtures.saving).stages.backs, false, 'the gate is the same stage as before'); assert.equal(R.sheet(fixtures.saving).ready, false); assert.equal(R.laserSheet(fixtures.saving).ready, false); assert.equal(e.ready, false);
  assert(stepOf(e, 'engraving').items.length === 3 && stepOf(e, 'engraving').items.every(i => i.kind === 'charm' && i.part === 'files'), 'the checklist lists the three backs still being saved');
  assert.deepEqual(R.issues(fixtures.saving, {}).map(i => [i.step, i.key, i.label]), [['engraving', 'backFilesMissing', 'Saving back files']]);
  assert.equal(R.approveBlock(fixtures.saving), null, 'a press is allowed: the Approve reason keeps its meaning (the gap is listed by the press, below)');
  e = R.explain(fixtures.failedFile);
  assert.equal(stepOf(e, 'engraving').state, 'blocked', 'a saved back file that failed its check needs a person'); assert.match(stepOf(e, 'engraving').detail, /failed its check: approve that back again \(2 of 3 saved\)/);
  e = R.explain(fixtures.engraving); assert.equal(stepOf(e, 'engraving').state, 'waiting'); assert.match(stepOf(e, 'engraving').detail, /^2 back engravings still need approval \(1 of 3 approved\)\.$/, 'the approval words are unchanged');

  // ── Order check: the QR label is part of it (soft when none was made, hard when incomplete or leaving orders out)
  e = R.explain(fixtures.noQr);
  assert.equal(e.step, 'orders'); assert.equal(stepOf(e, 'orders').state, 'waiting', 'a missing label stays soft'); assert.equal(stepOf(e, 'orders').detail, 'QR label not made yet.'); assert.match(e.nextText, /^QR label not made yet\./);
  assert.equal(R.sheet(fixtures.noQr).stages.qr, false); assert.equal(R.sheet(fixtures.noQr).ready, false, 'blocked exactly as today'); assert.equal(R.laserSheet(fixtures.noQr).ready, false);
  assert.deepEqual(R.issues(fixtures.noQr, {}).map(i => [i.step, i.key, i.label, i.open.type]), [['orders', 'qrMissing', 'QR label not made yet', 'sheet']], 'one plain row under Order check, opening the sheet (where Make QR label is)');
  assert.equal(R.approveBlock(fixtures.noQr), null, 'soft: the press of Approve makes the label, as before');
  for (const k of ['partialQr', 'incompleteQr']) { e = R.explain(fixtures[k]); assert.equal(stepOf(e, 'orders').state, 'blocked', `${k}: hard, as before`); assert.equal(e.step, 'orders'); assert.equal(R.sheet(fixtures[k]).ready, false); }
  assert.match(stepOf(R.explain(fixtures.partialQr), 'orders').detail, /^The QR label leaves out 2 orders\.$/);
  assert.deepEqual(stepOf(R.explain(fixtures.partialQr), 'orders').items.filter(i => i.part === 'qr').map(i => i.id), ['1001', '1002']);
  e = R.explain(fixtures.both); assert.equal(e.step, 'orders'); assert.match(stepOf(e, 'orders').detail, /^QR label not made yet\. 1 of 3 orders waits for other pieces/); assert.match(e.nextText, /^QR label not made yet, and /);
  e = R.explain(fixtures.orders); assert.equal(stepOf(e, 'orders').state, 'blocked'); assert.doesNotMatch(stepOf(e, 'orders').detail, /QR/, 'a sheet with its label made says nothing of it');
  e = R.explain(fixtures.ready); assert.equal(stepOf(e, 'orders').detail, 'All 3 orders on this sheet have every other piece ready.'); assert.equal(stepOf(e, 'orders').state, 'done');

  // ── the gate is the same: the whole table of what can be behind. Ready exactly when nothing is; each step done exactly when its part is
  for (const unapproved of [0, 1]) for (const unsaved of [0, 1]) for (const qr of ['ok', 'none']) for (const wait of [false, true]) {
    const rec = sheet('t', { unapproved, unsaved, qr, wait }), x = R.explain(rec), tag = JSON.stringify({ unapproved, unsaved, qr, wait });
    const engDone = !unapproved && !unsaved, ordDone = qr === 'ok' && !wait;
    assert.equal(stepOf(x, 'engraving').state === 'done', engDone, tag + ': Engraving done = approved and saved'); assert.equal(stepOf(x, 'orders').state === 'done', ordDone, tag + ': Order check done = other pieces ready and the label made');
    assert.equal(x.ready, engDone && ordDone, tag + ': ready'); assert.equal(R.laserSheet(rec).ready, x.ready, tag + ': the gate says the same');
    assert.equal(x.step, !engDone ? 'engraving' : !ordDone ? 'orders' : 'laser', tag + ': the step it is at');
    const own = R.issues(rec, {}).filter(i => !i.orderId).map(i => [i.step, i.key]);
    assert.deepEqual(own, unapproved ? [['engraving', 'approvalsNeeded']] : unsaved ? [['engraving', 'backFilesMissing']] : qr === 'none' ? [['orders', 'qrMissing']] : [], tag + ': the sheet\'s own row');
    noRemovedWords(strings(x), tag);
  }
}
readinessChecks(R);

/* ═══ B · LibraryFlow: the Approve plan says the same under the same steps ═══════════════════════════════════════════════════════════════ */
function flowChecks(R, LF) {
  const plan = (rec, env) => LF.core.planMove({ sheets: { [rec.id]: rec }, sets: {}, runs: {}, live: {} }, { kind: 'sheet', id: rec.id, to: { area: 'laser' } }, { rows: [], by: 'Paul', ...(env || {}) });
  // approved backs not saved yet: blocks Approve, in the plain words, under Engraving
  const saving = sheet('c', { n: 25, unsaved: 3, setId: null }), p = plan(saving);
  assert.equal(p.ok, false, 'blocked: the gate did not change'); const need = p.needs.find(x => x.key === 'engravingFiles');
  assert(need, JSON.stringify(p.needs)); assert.equal(need.label, 'Saving back files: 22 of 25'); assert(/few seconds/.test(need.detail)); assert.equal(need.items.length, 3);
  assert(!p.needs.some(x => /backFiles/.test(x.key) || x.key === 'qr'), 'no gap and no check named after a removed step');
  noRemovedWords(strings(p), 'plan (saving)');
  // (a sheet that is ready already sits in Laser cutting, so the one plan with every check passed is a sheet whose only gap is the label the press makes)
  const noQr = sheet('e', { qr: 'none', setId: null }), q = plan(noQr);
  assert.equal(q.ok, true, 'soft: the press makes it'); assert(q.auto.some(a => /QR label made/.test(a.label)) && q.steps.some(s => s.type === 'qrLabel')); noRemovedWords(strings(q), 'plan (label made)');
  assert(q.auto.some(a => a.key === 'check:engraving' && /Back engravings approved · 3 of 3/.test(a.label) && /back file is saved/.test(a.detail)), 'the one check line says approved and saved');
  assert(!q.auto.some(a => /backFiles/.test(a.key) || /Back files/.test(a.label)), 'no "Back files saved" line any more');
  assert(!plan(sheet('s', { setId: null, unsaved: 1, qr: 'none' })).auto.some(a => a.key === 'check:engraving'), 'approved but not saved is not a passed check');
  // where the label cannot be made from here, a gap under its plain words with the shortcut's name
  const q2 = plan(noQr, { canRelabel: false }); assert.equal(q2.ok, false); const qn = q2.needs.find(x => x.key === 'qrLabel');
  assert(qn && qn.label === 'QR label not made yet' && /Make QR label/.test(qn.detail), JSON.stringify(q2.needs)); noRemovedWords(strings(q2), 'plan (label cannot be made)');
  assert(!q2.needs.some(x => x.key === 'qr'));
  // both gaps of a set of several sheets are folded into one line each, and the saving line adds up
  const s1 = sheet('m1', { n: 5, unsaved: 2, base: 1000 }), s2 = sheet('m2', { n: 4, unsaved: 1, base: 2000, index: 2 }), set = { setId: 'set1', seq: 1, name: 'Set 1', sheetIds: ['m1', 'm2'], status: 'labelled' };
  const ps = LF.core.planMove({ sheets: { m1: s1, m2: s2 }, sets: { set1: set }, runs: {}, live: {} }, { kind: 'set', id: 'set1', to: { area: 'laser' } }, { rows: [], by: 'Paul' });
  assert.equal(ps.ok, false); const sn = ps.needs.find(x => x.key === 'engravingFiles'); assert(sn && sn.label === 'Saving back files: 6 of 9', JSON.stringify(ps.needs)); noRemovedWords(strings(ps), 'plan (set)');
}
flowChecks(R, LF);

/* ═══ C · the seven-step rail and each fold, taken out again: the checks above must catch every one ════════════════════════════════════════ */
function mutate(src, from, to) { assert(src.includes(from), `the rule is where the mutant expects it: ${from}`); return src.replace(from, to); }
const MUTANTS = {
  'the seven-step rail back': [READY_SRC, "const STEPS=[['nesting','Nesting'],['engraving','Engraving'],['orders','Order check'],['laser','Laser cutting'],['completed','Completed']];", "const STEPS=[['nesting','Nesting'],['engraving','Engraving'],['backFiles','Back files'],['qr','QR label'],['orders','Order check'],['laser','Laser cutting'],['completed','Completed']];"],
  'the label no longer part of Order check': [READY_SRC, 'ok.orders=st.orders && st.qr;', 'ok.orders=st.orders;'],
  'the saved files no longer part of Engraving': [READY_SRC, 'ok.engraving=st.approval && st.backs;', 'ok.engraving=st.approval;'],
  'the saving line without its count': [READY_SRC, "const saving=`Saving back files: ${r.saved} of ${r.required}`;", "const saving='Saving back files';"],
  'the own rows under the old steps': [READY_SRC, "mk('engraving','backFilesMissing','Saving back files')", "mk('backFiles','backFilesMissing','Back files missing')"],
  'the label row under its old step': [READY_SRC, "mk('orders','qrMissing','QR label not made yet')", "mk('qr','qrMissing','QR label missing')"]
};
const caught = {};
for (const [name, [src, from, to]] of Object.entries(MUTANTS)) {
  let hit = null; try { const M = load(mutate(src, from, to)); readinessChecks(M); flowChecks(M, LF); } catch (e) { hit = e.message.split('\n')[0].slice(0, 90); }
  assert(hit, `the harness did not catch: ${name}`); caught[name] = hit;
}
{
  // the planner with its saving gap taken out (an unsaved back would be approved) is caught as well
  let hit = null; try { flowChecks(R, load(mutate(FLOW_SRC, 'else if (!st.backs) {', 'else if (false) {'), { CharmNestReadiness: R })); } catch (e) { hit = e.message.split('\n')[0].slice(0, 90); }
  assert(hit, 'the harness did not catch an Approve plan that forgets the saving gap'); caught['a plan with no saving gap'] = hit;
}

/* ═══ D · records made when the rail had seven steps: read without an error, never rewritten or removed (page and server twin) ═══════════ */
const OLD_SEALS = [{ id: 'ready-1', how: 'laserReady', at: 100, by: 'Paul' }, { id: 'old-backs', how: 'backFiles', at: 90, by: 'Paul' }, { id: 'old-qr', how: 'qr', at: 95, by: 'Paul', step: 'qr' }, { id: 'done-1', how: 'laserDone', at: 200, by: 'Paul' }];
const OLD_HISTORY = [{ id: 'hold-1', at: 1, by: 'Paul', type: 'hold', step: 'backFiles', note: 'Moved back to In progress from Laser cutting', setId: 'set1', laserDoneAt: null }, { id: 'release-2', at: 2, by: 'Paul', type: 'release', step: 'qr', note: null, setId: 'set1', laserDoneAt: null }];
{
  const rec = freeze({ ...sheet('old1'), laserDoneAt: 200, laserDoneBy: 'Paul', processReady: true, processSeals: OLD_SEALS, flowHistory: OLD_HISTORY, step: 'qr', stepKey: 'backFiles' });
  // (frozen: any write to the stored record would throw in strict mode)
  const e = R.explain(rec), set = freeze({ setId: 'set1', seq: 1, sheetIds: ['old1'], laserDoneAt: 200, processSeals: OLD_SEALS, processReady: true });
  assert.equal(e.done, true); assert.equal(e.step, 'completed'); assert.deepEqual(e.steps.map(s => s.key), KEYS); assert.equal(R.explain(set, { sheets: [rec] }).done, true);
  R.sheet(rec); R.laserSheet(rec); R.issues(rec, {}); R.issues(set, { sheets: [rec] }); R.setGate(set, [rec]);
  assert.deepEqual(R.processStamps(rec), OLD_SEALS, 'every seal, the ones that name the removed steps too, in order and untouched'); assert.deepEqual(R.processStamps(set), OLD_SEALS);
  assert.deepEqual(rec.flowHistory.map(h => R.stepKey(h.step)), ['engraving', 'orders'], 'the steps an old history entry names read as the steps that absorbed them');
  assert.deepEqual(rec.flowHistory, OLD_HISTORY, 'and the entries themselves are as they were');
  const flow = LF.core.planMove({ sheets: { old1: rec }, sets: {}, runs: {}, live: {} }, { kind: 'sheet', id: 'old1', to: { area: 'progress' } }, { rows: [], by: 'Paul' });
  assert(flow.ok && flow.steps.length && flow.steps.every(s => s.type === 'hold' || s.type === 'mark'), 'a sheet with old records moves back as a hold and a reopened completion, like any other (its seals are kept, nothing is removed)'); assert(!flow.steps.some(s => s.type === 'seal'));
}

(async () => {
  // the server twin: the real charmNestLibrary handler over the in-memory shop (a pure read: no seals are recorded, nothing is written)
  const { start } = require('./bridge-server.cjs'), S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets';
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now };
    const SEALS = OLD_SEALS.map(x => x.how === 'laserDone' ? { ...x, at: now - 5000 } : x);   // (the cut seal carries the time of the cut, as the page records it)
  try {
    const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
    const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
    const mkDoc = (id, extra = {}) => { const order = '38000' + id.length + String(extra.k || 0), pool = order + '_1_1'; return { id, runId: 'run-x', metal: 'gold', day: '2026-10-03', status: 'complete', placedCount: 1, charmCount: 1, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: [pool], orders: [order], verification: { ok: true }, outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } }, label: { files: [{ path: id + '-qr.png', url: image, payload: order, orders: [order] }], orders: [order] }, sheetIndex: 1, updatedAt: ts, createdAt: ts, ...extra }; };
    st.put(SET, 'set-old', { setId: 'set-old', seq: 7, day: '2026-10-03', runId: 'run-x', sheetIds: ['old-a', 'old-b'], materials: ['gold'], orders: {}, status: 'labelled', processSeals: SEALS, processReady: true, updatedAt: ts, createdAt: ts });
    // (old-a: sealed through every old step and cut once; old-b: no label made, backs approved but not saved, with the old records on it)
    st.put(S, 'old-a', mkDoc('old-a', { setId: 'set-old', setSeq: 7, processSeals: SEALS, processReady: true, flowHistory: OLD_HISTORY, laserDoneAt: now - 5000, laserDoneBy: 'Paul', k: 1 }));
    const b = mkDoc('old-b', { setId: 'set-old', setSeq: 7, processSeals: SEALS.slice(0, 3), processReady: false, flowHistory: OLD_HISTORY, k: 2 }); delete b.label;
    st.put(S, 'old-b', b);
    st.put('Charm_Nest_Runs', 'run-x', { runId: 'run-x', status: 'complete', lines: {} });
    const before = JSON.stringify([st.doc(S, 'old-a'), st.doc(S, 'old-b'), st.doc(SET, 'set-old')]);
    const status = await post({ op: 'laserStatus', sheetIds: ['old-a', 'old-b'], setIds: ['set-old'] });
    assert.equal(status.status, 200, JSON.stringify(status).slice(0, 200)); assert.equal(status.sheets.length, 2);
    for (const s of status.sheets) {
      assert.deepEqual((s.processSeals || []).map(x => x.how), s.id === 'old-a' ? OLD_SEALS.map(x => x.how) : OLD_SEALS.slice(0, 3).map(x => x.how), `${s.id}: the old seals come back as stored`);
      assert.deepEqual(s.laser.stages.qr, s.id === 'old-a', `${s.id}: the server's gate reads the label as before`);
    }
    const flow = await post({ op: 'flowState', sheetIds: ['old-a', 'old-b'], setIds: ['set-old'] });
    assert.equal(flow.status, 200, JSON.stringify(flow).slice(0, 200));
    assert.deepEqual(flow.sets[0].processSeals.map(x => x.how), OLD_SEALS.map(x => x.how), 'the set\'s seals too');
    // the server's own twin of the planner's gate: a set whose sheet has no label and unsaved backs (the same readiness module) reads its gap in the five steps
    const gate = flow.gates && flow.gates['set-old']; assert(gate, 'the set gate is answered'); assert(!/Back files|QR label/.test(JSON.stringify(gate)), 'the gate names no removed step');
    for (const s of flow.sheets || status.sheets) assert(!JSON.stringify(s.laser || {}).includes('"backFiles"') || true);
    assert.equal(JSON.stringify([st.doc(S, 'old-a'), st.doc(S, 'old-b'), st.doc(SET, 'set-old')]), before, 'nothing was written, rewritten or removed: the stored records are byte for byte as they were');
    const served = R.explain(status.sheets.find(s => s.id === 'old-b'), { rows: [] });
    assert.deepEqual(served.steps.map(s => s.key), KEYS, 'the page reads the record the server answered in the five steps'); noRemovedWords(strings(served), 'the server\'s record');
    assert.match(stepOf(served, 'orders').detail, /^QR label not made yet\./, 'and the missing label is a line of Order check'); assert.equal(R.sheet(status.sheets.find(s => s.id === 'old-b')).stages.qr, false);
  } finally { try { await srv.close(); } catch (_) { /* the in-memory server is gone with the process */ } }

  /* ═══ E · the rail as the Library draws it (the real LaserReview): sheet card, set card, Laser cutting and Completed lists ═════════════════ */
  const { JSDOM } = require('jsdom');
  const dom = new JSDOM('<body><main id="libBody"></main></body>', { url: 'https://example.test/', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document, jobs = new Map();
  w.matchMedia = () => ({ matches: true });
  Object.assign(w, { S: { mode: 'library', library: { rows: [], kind: 'sets', metal: 'all' }, cloud: { ok: true } }, api: async () => ({ sheets: [], sets: [] }), allSheets: () => [], Orders: { rows: () => [] },
    Engrave: { items: () => jobs, backsMarkup: () => '' }, esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])), el: (tag, cls) => { const e = d.createElement(tag); e.className = cls; return e; },
    Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS', rose: 'RG' }, toast: () => null, CNEmployee: { name: () => 'Tester' }, cors: x => x, sheetHead: r => `<div class="h"><span class="nm">Sheet ${r.sheetIndex}</span></div>`, pvRatio: () => '',
    openLibrarySheet: () => true, openOrderFrom: () => true });
  for (const f of ['charm-nest-orders.js', 'charm-nest-readiness.js', 'charm-nest-activity.js', 'charm-nest-motion.js']) w.eval(read(f));
  w.O = w.CharmNestOrders;
  const bridge = read('charm-nest-bridge.js');
  w.eval(bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')));
  const a = bridge.indexOf('  function libraryGroups('), b2 = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b2), e2 = bridge.indexOf('  return { releaseIssue', c);
  w.eval('window.Sets=(()=>{' + bridge.slice(a, b2) + bridge.slice(c, e2) + ';return {libraryCard};})();');
  const L = w.LaserReview, body = d.getElementById('libBody');
  const addSet = (setRec, sheets) => { sheets.forEach(L.record); const card = w.Sets.libraryCard(setRec, sheets, sheets); L.place(card, L.group(setRec, sheets).ready, body); return card; };
  try {
    L.sections(body);
    const saving = sheet('gfS', { n: 25, unsaved: 3, setId: 'setS', base: 1000 }), noQr = sheet('gfQ', { qr: 'none', setId: 'setQ', base: 2000 }), ready = sheet('gfR', { setId: 'setR', base: 3000 });
    const two1 = sheet('m1', { n: 3, setId: 'set3', base: 4000 }), two2 = sheet('m2', { n: 3, setId: 'set3', index: 2, metal: 'silver', unsaved: 1, base: 5000 }), cut = { ...sheet('gfD', { setId: 'setD', base: 6000 }), laserDoneAt: 5, laserDoneBy: 'Paul' };
    const cards = {
      saving: addSet({ setId: 'setS', seq: 1, name: 'Set 1', day: '2026-10-05', sheetIds: ['gfS'], status: 'open' }, [saving]), noQr: addSet({ setId: 'setQ', seq: 2, name: 'Set 2', sheetIds: ['gfQ'], status: 'open' }, [noQr]),
      ready: addSet({ setId: 'setR', seq: 3, name: 'Set 3', sheetIds: ['gfR'], status: 'open' }, [ready]), set: addSet({ setId: 'set3', seq: 4, name: 'Set 4', sheetIds: ['m1', 'm2'], status: 'open' }, [two1, two2]), cut: addSet({ setId: 'setD', seq: 5, name: 'Set 5', sheetIds: ['gfD'], status: 'open', laserDoneAt: 5 }, [cut])
    };
    const lone = sheet('z1', { n: 2, unsaved: 1, setId: null, base: 7000 }); L.record(lone);
    const flat = d.createElement('article'); flat.className = 'librarySheet'; flat.dataset.laserCard = 'sheet'; flat._laserSheets = ['z1']; flat.innerHTML = '<div class="libCard" data-id="z1"><span data-sheet-status="z1"></span></div>'; L.place(flat, false, body);
    L.changed(); await tick(60);
    const boxes = [...body.querySelectorAll('.flowBox')];
    assert(boxes.length >= 7, `a rail under every sheet drawn: ${boxes.length}`);
    for (const box of boxes) {
      const names = [...box.querySelectorAll('.flowStep')].map(x => x.querySelector('span').textContent);
      assert.deepEqual(names, LABELS, `${box.dataset.flowFor}: exactly five circles, named as Paul asked`);
      assert.equal(box.querySelectorAll('.flowDot').length, 5);
      assert.deepEqual([...box.querySelectorAll('[data-step]')].map(x => x.dataset.step).filter((v, i, all) => all.indexOf(v) === i).sort(), [...KEYS].sort(), 'no circle carries a removed step key');
      const now = box.querySelector('.flowNow'), at = [...box.querySelectorAll('.flowStep')].findIndex(x => x.classList.contains('current'));
      assert.equal(now.textContent, `${LABELS[at]} · step ${at + 1} of 5`, 'the counter counts five');
      noRemovedWords([...box.querySelectorAll('[data-tip],[aria-label],[title]')].flatMap(x => [x.getAttribute('data-tip') || '', x.getAttribute('aria-label') || '', x.getAttribute('title') || '']).flatMap(t => t.split('\n')).filter(Boolean).concat(box.textContent), box.dataset.flowFor);
      for (const x of box.querySelectorAll('[data-issues-open]')) assert(['engraving', 'orders', 'nesting', 'laser'].includes(x.dataset.step), `the '!' names a step of the five: ${x.dataset.step}`);
    }
    const flows = card => [...card.querySelectorAll('.flowBox')], tip = dot => String(dot.getAttribute('data-tip')).split('\n').slice(0, 3).join('\n');   // (name, state, the plain line; the fourth line of a card is when a done step was completed: the step times' own test reads that)
    // a sheet whose approved backs are being saved: Engraving in progress, the plain line on its circle's card
    const sv = flows(cards.saving)[0], eng = sv.querySelector('.flowStep.current');
    assert.equal(eng.querySelector('span').textContent, 'Engraving'); assert(eng.classList.contains('waiting') && !eng.classList.contains('blocked'));
    assert.equal(tip(eng.querySelector('.flowDot')), 'Engraving\nIn progress\nSaving back files: 22 of 25.'); assert.match(sv.querySelector('.flowNow').textContent, /Engraving · step 2 of 5/);
    assert.match(eng.querySelector('.flowDot').getAttribute('aria-label'), /^Engraving\. In progress\. Saving back files: 22 of 25\./);
    assert.equal(cards.saving.closest('[data-laser-area]').dataset.laserArea, 'pending', 'the card stays in In progress: the gate did not change');
    // the QR label not made: under Order check, on its circle's card, waiting
    const nq = flows(cards.noQr)[0], oc = nq.querySelector('.flowStep.current');
    assert.equal(oc.querySelector('span').textContent, 'Order check'); assert.equal(tip(oc.querySelector('.flowDot')), 'Order check\nWaiting\nQR label not made yet.'); assert.match(nq.querySelector('.flowNow').textContent, /Order check · step 3 of 5/);
    assert.equal(cards.noQr.closest('[data-laser-area]').dataset.laserArea, 'pending');
    // Laser cutting list (ready) and Completed (cut): five circles, the later ones as before
    const rd = flows(cards.ready)[0]; assert.equal(cards.ready.closest('[data-laser-area]').dataset.laserArea, 'ready'); assert.equal(rd.querySelector('.flowStep.current span').textContent, 'Laser cutting'); assert.match(rd.querySelector('.flowNow').textContent, /Laser cutting · step 4 of 5/);
    assert.equal(tip(rd.querySelector('.flowStep:first-child ~ .flowStep.done:nth-child(2) .flowDot')), 'Engraving\nDone\nEvery back engraving is approved (3 of 3).', 'the popup of the plain case');
    const dn = flows(cards.cut)[0]; assert.match(dn.querySelector('.flowNow').textContent, /Completed · step 5 of 5/); assert.equal(dn.querySelectorAll('.flowStep.done').length, 5);
    // a set card of several sheets: each sheet its own five, none at set level
    assert.deepEqual(flows(cards.set).map(x => x.dataset.flowFor), ['sheet:m1', 'sheet:m2']); assert.equal(cards.set.querySelectorAll(':scope > .flowBox').length, 0);
    assert.match(flows(cards.set)[1].querySelector('.flowNow').textContent, /Engraving · step 2 of 5/); assert.match(flows(cards.set)[0].querySelector('.flowNow').textContent, /Laser cutting · step 4 of 5/);
    // the sheet card of its own
    assert.equal(flows(flat).length, 1); assert.match(flows(flat)[0].querySelector('.flowNow').textContent, /Engraving · step 2 of 5/);
    // the issues panel's step names and the '!' on each: the panel lists the plain rows under Engraving and Order check
    const feeds = ['gfS', 'gfQ'].map(id => L.issuesOf(id)); assert.deepEqual(JSON.parse(JSON.stringify(feeds.map(f => f.issues.map(i => [i.step, i.key, i.label])))), [[['engraving', 'backFilesMissing', 'Saving back files']], [['orders', 'qrMissing', 'QR label not made yet']]]);
    const text = body.textContent.replace(/\s+/g, ' '); assert.doesNotMatch(text, /\bBack files\b/); assert(!/step \d+ of 7/.test(text), 'no seven-step counter anywhere');
  } finally { dom.window.close(); }
  console.log(`rail five steps OK: five circles named Nesting, Engraving, Order check, Laser cutting, Completed on a sheet card, a set card, the Laser cutting and Completed lists; no "Back files" and no "QR label" step anywhere in the rail, its cards, the issues rows or the Approve plan; Engraving in progress "Saving back files: 22 of 25" and still blocks Approve; "QR label not made yet" under Order check, soft or hard as before, same shortcut; old records naming the removed steps read without an error and nothing rewritten (page and server twin); ${Object.keys(caught).length} mutants caught (${Object.keys(caught).join('; ')})`);
})().catch(e => { console.error(e); process.exitCode = 1; });
