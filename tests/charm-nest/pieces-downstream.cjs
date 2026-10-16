// PIECES, DOWNSTREAM (PL): everything that follows the engraving once a line bought twice is TWO pieces, each with its own words, Approve, seals and back file.
//
// Paul, 16 Oct 2026, order 4195059483 ("CORGI 34728 · SS · ×2"): "ALL multipiece orders must always show each individual piece with respect to all Engravings and Monograms."  The model (one job per piece, slots
// C1..CN, charm-nest-engrave-sides.js) is PC's; the windows are PV's. What this proves is what comes AFTER the engraving of a piece, offline, on fakes:
//   A  the order's milestone rail (charm-nest-timeline-ui.js): ENGRAVED is done only when EVERY piece of a line has its approval (pool ids the host names, line.engravePools); per piece seals for a line cut by unit; the
//      rail counts the pieces of the line already approved; an old approval event that names no piece stands for all of them (never asked twice); quantity 1, ears and discs without units read as they always did
//   B  the server's wording of an approval's event (charmNestLibrary.js earWords/earCaption/pieceData) reads exactly like the page's (Sides.earOf) for every slot, and the slot is not clipped
//   C  Send to Sheet / laser gating (charm-nest-readiness.js on Sides.summary): a half-approved line is ONE calm "waiting 1", not ready; both approved is ready; an old whole-line approval migrated to C1/C2 is as approved as before
//   D  the production Engrave code (charm-nest-bridge.js, run in a vm over fakes of the sheet, the cloud and the file builder): a qty-2 charm with DIFFERENT words per piece goes through approve -> back file per piece:
//      each piece's file and record carry ITS words, its pool id, its slot and its copy number; a piece not approved yet writes nothing; the timeline gets one event per piece; ears/discs unchanged
//   E  the real page (Chromium, offline fake site, _engraveRealWorld.cjs): two pieces with different words, approve each: the Order window's rail waits for the second; readiness waits for the second; ears and discs jobs as before
//   node tests/charm-nest/pieces-downstream.cjs        (E needs PW_DIR=<dir with playwright-core>; without it E says SKIP)
// What a pass does NOT prove: a live order, a live Firestore/Storage write, the live laser; only that the page's own code does this on the offline fakes.
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));
const Readiness = require(path.join(root, 'charm-nest-readiness.js'));

let passed = 0, failed = 0; const fails = [];
const t = async (id, name, fn) => { try { await fn(); passed++; console.log('  PASS ' + id + ' ' + name); } catch (e) { failed++; const msg = String(e && e.message || e).split('\n').slice(0, 12).join(' | '); fails.push(id + ' ' + name + ' :: ' + msg); console.log('  FAIL ' + id + ' ' + name + '  [' + msg + ']'); } };

const RID = '4195059483', TX = '3711111', LINE = RID + '_' + TX, ID1 = LINE + '_1', ID2 = LINE + '_2', ID3 = LINE + '_3';

/* ═══ A · the milestone rail ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
const win = { document: { getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } } };
win.window = win; vm.createContext(win); vm.runInContext(read('charm-nest-timeline-ui.js'), win);
const UI = win.OrderTimelineUI, ENG = UI.STAGES.findIndex(s => s.k === 'engraved');
const T0 = 1.76e12;
const ev = (type, min, o) => Object.assign({ id: type + '~' + min + '~' + ((o && o.data && (o.data.poolId || '')) || ''), key: type + '~' + min + '~' + ((o && o.data && (o.data.poolId || '')) || ''), type, at: T0 + min * 6e4, by: 'Paul', source: 'sorter', lineKey: LINE, transactionId: TX }, o || {});
const arrived = ev('arrived', 0, { lineKey: undefined, transactionId: undefined, by: 'Etsy', source: 'etsy' });
const placed = ev('placed', 5, { sheetId: 'shA', sheet: 'SS Sheet 1' });
const approved = (id, min, slot, extra) => ev('engraveApproved', min, { data: Object.assign({ poolId: id, signedIn: true }, slot ? { slot } : {}, extra || {}) });
const plain = x => JSON.parse(JSON.stringify(x));
const stepOf = (events, pools, faces) => UI.derive(events.map(e => Object.assign({}, e)), null, null, null, null, faces || null, pools || null);

(async () => {
  await t('A1', 'two pieces, only piece 1 approved: the Engraved step stays open (before: done at the first approval)', () => {
    const evs = [arrived, placed, approved(ID1, 10, 'C1')];
    const old = stepOf(evs), mine = stepOf(evs, [ID1, ID2]);
    assert.equal(old.step >= ENG, true, 'control: the old rule is done at the first approval');
    assert.ok(mine.step < ENG, 'Engraved must not be reached: step ' + mine.step);
    assert.ok(mine.stages[ENG].partial && mine.stages[ENG].partial.includes(ID2), 'it says which piece is missing');
    assert.equal(UI.stepDone(mine, ENG), false);
    assert.deepEqual(JSON.parse(JSON.stringify(mine.stages[ENG].pieces)), { of: 2, done: 1 });
  });
  await t('A2', 'both pieces approved: Engraved is done, with the first approval as its stamp', () => {
    const evs = [arrived, placed, approved(ID1, 10, 'C1'), approved(ID2, 12, 'C2')];
    const d = stepOf(evs, [ID1, ID2]);
    assert.equal(d.step, ENG); assert.equal(UI.stepDone(d, ENG), true); assert.equal(d.stages[ENG].partial, undefined);
    assert.equal(d.stages[ENG].first.at, T0 + 10 * 6e4);
  });
  await t('A3', 'an old approval event that names no piece stands for every piece of its line: nothing already approved is asked again', () => {
    const old = ev('engraveApproved', 10, { data: { text: 'Mom', pieces: 2 } });
    const d = stepOf([arrived, placed, old], [ID1, ID2]);
    assert.equal(d.step, ENG); assert.equal(d.stages[ENG].partial, undefined);
    // and the page's own events of a migrated line (one per pool id, no slot, same instant) cover both too
    const d2 = stepOf([arrived, placed, approved(ID1, 10), approved(ID2, 10)], [ID1, ID2]);
    assert.equal(d2.step, ENG);
  });
  await t('A4', 'a line with one piece (or no word from the host) reads exactly as it always did', () => {
    const evs = [arrived, placed, approved(ID1, 10)];
    for (const pools of [null, undefined, [], [ID1]]) { const d = stepOf(evs, pools); assert.equal(d.step, ENG); assert.equal(d.stages[ENG].partial, undefined); assert.equal(d.stages[ENG].pieces, undefined); }
  });
  await t('A5', 'a piece engraved on both faces: every piece needs every face', () => {
    const front = id => ev('engraveApproved', 20, { data: { poolId: id, face: 'front' } });
    const evs = [arrived, placed, approved(ID1, 10), front(ID1), approved(ID2, 12)];
    const d = stepOf(evs, [ID1, ID2], ['front', 'back']);
    assert.ok(d.step < ENG && d.stages[ENG].partial.length >= 1, 'piece 2 front is missing');
    const e = stepOf(evs.concat([front(ID2)]), [ID1, ID2], ['front', 'back']);
    assert.equal(e.step, ENG); assert.equal(e.stages[ENG].partial, undefined);
  });
  await t('A6', 'later steps already happened (laser cut): the rail is never pulled back', () => {
    const d = stepOf([arrived, placed, approved(ID1, 10), ev('laserDone', 60, { sheetId: 'shA' })], [ID1, ID2]);
    assert.ok(d.step >= ENG + 1, 'laser cut stays: ' + d.step);
  });
  await t('A7', 'summary: a line of qty 2 counts the pieces already approved on the rail ("Engraved 1 of 2"), then both', () => {
    const line = { engrave: { state: 'review', needed: true }, engravePools: [ID1, ID2] };
    const piece = { key: LINE, tid: TX, qty: 2, pools: [ID1, ID2], sheets: ['shA'], line };
    const one = UI.summary([arrived, placed, approved(ID1, 10, 'C1')], [piece], null), r1 = one.rail.find(r => r.s.k === 'engraved');
    assert.deepEqual([r1.n, r1.of], [1, 2]); assert.ok(one.step < ENG);
    const two = UI.summary([arrived, placed, approved(ID1, 10, 'C1'), approved(ID2, 12, 'C2')], [piece], null), r2 = two.rail.find(r => r.s.k === 'engraved');
    assert.deepEqual([r2.n, r2.of], [2, 2]); assert.equal(two.step, ENG);
    // without the host's pools it is the old "done at the first approval" (control)
    const ctl = UI.summary([arrived, placed, approved(ID1, 10)], [Object.assign({}, piece, { line: { engrave: { state: 'review', needed: true } } })], null);
    assert.equal(ctl.step, ENG);
  });
  await t('A8', 'sub-pieces (a piece per slot): each sees only its own approval; the order waits on the slowest', () => {
    const line = { engrave: { state: 'review', needed: true }, engravePools: [ID1, ID2] };
    const subs = [ID1, ID2].map((id, i) => ({ key: LINE + '#C' + (i + 1), lineKey: LINE, slot: 'C' + (i + 1), tid: TX, qty: 1, pools: [id], sheets: ['shA'], line }));
    const evs = [arrived, placed, approved(ID1, 10, 'C1')];
    const s = UI.summary(evs, subs, null);
    assert.equal(UI.stepDone(s.each[0].D, ENG), true); assert.equal(UI.stepDone(s.each[1].D, ENG), false);
    const r = s.rail.find(x => x.s.k === 'engraved'); assert.deepEqual([r.n, r.of], [1, 2]);
    assert.ok(s.step < ENG, 'the order is where its slowest piece is');
    assert.equal(UI.ofPiece(approved(ID2, 12, 'C2'), subs[0], subs), false); assert.equal(UI.ofPiece(approved(ID2, 12, 'C2'), subs[1], subs), true);
    assert.equal(UI.ofPiece(arrived, subs[1], subs), true, 'the order\'s own events count for every piece');
  });
  await t('A9', 'seals: a line cut by unit draws an Engraved seal per piece; slotless events, ears and discs group by line as before', () => {
    const norm = list => UI.sealsOf(list.map(e => Object.assign({}, e)));
    const engraved = list => norm(list).filter(e => e.type === 'engraveApproved');
    assert.equal(engraved([arrived, placed, approved(ID1, 10, 'C1'), approved(ID2, 12, 'C2')]).length, 2, 'one seal per piece');
    assert.equal(engraved([arrived, placed, approved(ID1, 10), approved(ID2, 10)]).length, 1, 'old events with no slot: one seal for the line, as before');
    assert.equal(engraved([arrived, placed, approved(ID1, 10, 'L'), approved(ID2, 10, 'R')]).length, 1, 'ears: unchanged');
    assert.equal(engraved([arrived, placed, approved(ID1, 10, 'D1'), approved(ID2, 11, 'D2'), approved(ID3, 12, 'D3')]).length, 1, 'discs: unchanged');
    const two = engraved([approved(ID1, 10, 'L'), approved(ID2, 10, 'R'), approved(LINE + '_3', 20, 'L.2'), approved(LINE + '_4', 20, 'R.2')]);
    assert.equal(two.length, 4, 'two pairs of ears: each ear its own seal once the line is cut by unit');
  });

  await t('A10', 'the step card names the piece still to approve with its own words ("Piece 2"), and the step is "now", not done', () => {
    const lines = [{ sku: 'CORGI', engrave: { needed: true, state: 'review', approved: false, text: 'Back: Anna · Ben' }, engraveCandidate: true, onSheet: true, engravePieces: [{ label: 'Piece 1', text: 'Anna', done: true }, { label: 'Piece 2', text: 'Ben', done: false }] }];
    const r = UI.requirementsOf('engraved', { events: [arrived, placed, approved(ID1, 10, 'C1')], context: { lines, sheets: [] }, stages: UI.STAGES, pools: [ID1, ID2] });
    assert.equal(r.state, 'now'); assert.deepEqual(plain(r.need.map(n => n.t)), ['Approve the back engraving “Ben” of SKU CORGI · Piece 2 in Review']);
    const ok = UI.requirementsOf('engraved', { events: [arrived, placed, approved(ID1, 10, 'C1'), approved(ID2, 12, 'C2')], context: { lines: [Object.assign({}, lines[0], { engrave: { needed: true, state: 'approved', approved: true } })], sheets: [] }, stages: UI.STAGES, pools: [ID1, ID2] });
    assert.equal(ok.state, 'done'); assert.deepEqual(plain(ok.need), []);
  });

  /* ═══ B · the server's words ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  const lib = read('netlify/functions/charmNestLibrary.js');
  const block = lib.slice(lib.indexOf('const SLOT_WORDS'), lib.indexOf('async function op_backPut'));
  const srv = new Function('str', block + '; return { earWords, earCaption, pieceData };')((v, n) => String(v == null ? '' : v).slice(0, n));
  await t('B1', 'the server words every slot like the page (Sides.earOf), and does not clip a long one', () => {
    for (const s of ['L', 'R', 'D1', 'D2', 'D12', 'C1', 'C2', 'C12', 'C400', 'L.2', 'R.3', 'D1.2', 'D2.3', 'D10.12', 'L.99']) assert.equal(srv.earWords(s), Sides.earOf(s), s);
    for (const s of ['', null, 'X', 'C', 'C1.2', 'D', 'Q9', 'L2']) assert.equal(srv.earWords(s), '', String(s));
    assert.equal(srv.earCaption({ slot: 'D10.12' }, true), ' · ' + Sides.earOf('D10.12'));
    assert.equal(srv.earCaption({ slot: 'C2' }, false), 'Piece 2');
    assert.deepEqual(srv.pieceData({ slot: 'C2' }), { slot: 'C2' }); assert.deepEqual(srv.pieceData({ slot: 'L.2', side: 'L' }), { slot: 'L.2', side: 'L' });
    assert.deepEqual(srv.pieceData({}), {}); assert.deepEqual(srv.pieceData({ slot: 'nope' }), {});
  });
  await t('B2', 'the page words its approval event with the same slot words (timelineApproved uses Sides.slotWord; the server\'s caption equals it)', () => {
    for (const s of ['C1', 'C2', 'L.2', 'D3.2']) assert.equal(Sides.slotWord({ slot: s }), srv.earWords(s));
    assert.ok(/SIDES\(\)\.slotWord\(job\)/.test(read('charm-nest-bridge.js')), 'timelineApproved names the piece');
  });

  /* ═══ C · Send to Sheet / laser gating ═════════════════════════════════════════════════════════════════════════════════════════════════════ */
  const rec = (state, extra) => Object.assign({ needed: state !== 'skipped', state, approved: state === 'approved' || state === 'written' || state === 'skipped', approvedBy: state === 'approved' ? 'Paul' : '', approvedAt: state === 'approved' ? T0 : 0, text: 'x' }, extra || {});
  const jobsOf = states => states.map((st, i) => ({ key: LINE + '#C' + (i + 1), slot: 'C' + (i + 1), copies: [LINE + '_' + (i + 1)], state: st, text: 'W' + (i + 1), engraveRec: rec(st) }));
  const lineRow = jobs => { const row = { key: LINE, state: 'pooled', poolIds: jobs.flatMap(j => j.copies), spec: { engraveCandidate: true } }; row.engrave = Sides.summary({ mergeSeals: (...r) => Seals.merge(...r) }, undefined, jobs); return row; };
  const sheetOf = (row) => { const eng = Readiness.decisions([row]); return { eng, rep: Readiness.sheet({ id: 'shA', poolIds: row.poolIds, engraving: eng, placedCount: row.poolIds.length, layoutOk: true }) }; };
  await t('C1', 'half-approved line: the gate counts ONE piece waiting (the other is approved), not ready', () => {
    const row = lineRow(jobsOf(['approved', 'review'])), { eng, rep } = sheetOf(row);
    assert.equal(row.engrave.approved, false); assert.equal(row.engrave.state, 'review');
    assert.equal(eng[ID1].approved, true); assert.equal(eng[ID2].approved, false);
    assert.equal(rep.waiting, 1); assert.equal(rep.approved, 1); assert.equal(rep.stages.approval, false); assert.equal(rep.ready, false);
    assert.deepEqual(Object.keys(row.engrave.pieces), [ID1, ID2]);
  });
  await t('C2', 'both approved: the line is approved and the sheet\'s engraving gate opens; one more still open keeps it shut', () => {
    const both = lineRow(jobsOf(['approved', 'approved'])), r = sheetOf(both).rep;
    assert.equal(both.engrave.approved, true); assert.equal(r.waiting, 0); assert.equal(r.stages.approval, true);
    const three = lineRow(jobsOf(['approved', 'approved', 'words'])); assert.equal(three.engrave.approved, false); assert.equal(sheetOf(three).rep.waiting, 1);
  });
  await t('C3', 'a piece cut plain is not waited for; an old whole-line approval read as piece 1 and piece 2 is as approved as before', () => {
    const plain = lineRow(jobsOf(['approved', 'skipped'])); assert.equal(sheetOf(plain).rep.waiting, 0);
    const whole = [{ key: LINE, copies: [ID1, ID2], state: 'approved', text: 'Mom', engraveRec: rec('approved') }];
    const old = lineRow(whole), mig = lineRow(jobsOf(['approved', 'approved']));
    assert.deepEqual(Object.values(sheetOf(old).eng).map(d => d.approved), Object.values(sheetOf(mig).eng).map(d => d.approved));
    assert.equal(sheetOf(old).rep.waiting, 0);
  });

  /* ═══ D · the production Engrave code over fakes: approve -> back file per piece ══════════════════════════════════════════════════════════════ */
  const source = read('charm-nest-bridge.js');
  const begin = source.indexOf('const Engrave = window.Engrave = (() => {'), end = source.indexOf('/* ═══ 22 · Sets', begin);
  assert(begin >= 0 && end > begin, 'the production engraving subsystem is in the bridge');
  const returnAt = source.lastIndexOf('  return {', end);
  const instrumentation = `
  loadFonts=async()=>{F_.ok=true;return F_;};
  verifyBackFile=(bytes,job)=>approvalFixture.verify(bytes,job);
  renderBack=(job,px)=>approvalFixture.preview(job,px);
  refreshBacks=()=>Session.schedule();
  render=()=>approvalFixture.paint();
  scheduleBackOutputs=sh=>approvalFixture.index(sh);
`;
  const script = source.slice(begin, returnAt) + instrumentation + source.slice(returnAt, end);
  const sessionCopy = source.slice(source.indexOf('  const OMIT = new Set('), source.indexOf('  function open() {', source.indexOf('  const OMIT = new Set(')));
  const btn = () => ({ disabled: false, textContent: 'Approved', attrs: new Map(), setAttribute(k, v) { this.attrs.set(k, v); }, getAttribute(k) { return this.attrs.get(k) ?? null; }, removeAttribute(k) { this.attrs.delete(k); } });
  const until = async p => { for (let n = 0; n < 200; n++) { if (p()) return; await new Promise(setImmediate); } assert.fail('the controlled pipeline did not reach its boundary'); };

  /** A line with `slots` (name, copies, words) as ONE job each, on one sheet, with the production Engrave code. */
  function fixture(slots, o) {
    o = o || {};
    const copies = slots.flatMap(s => s.copies);
    const row = { key: LINE, state: 'pooled', poolIds: copies.slice(), order: { receiptId: RID }, line: { transactionId: TX, sku: 'CORGI_34728', quantity: slots.length }, spec: { designSku: 'CORGI_34728', quantity: slots.length, personalization: ['x'], engraveCandidate: true } };
    const charm = { sourceId: 'source-1', outline: {}, members: [] };
    const sheet = { sheetId: 'sheet-1', runId: 'run-1', fileBase: 'SS_1', folderPath: 'fixture/SS_1', backPool: [] };
    const calls = { built: [], uploaded: [], recorded: [], events: [], pool: [], reviews: [], presses: 0, schedule: 0 };
    const jobs = new Map();
    const ctx = { poolRow: id => ({ copy: +String(id).split('_').pop() || 1 }), charmOf: () => charm, entryFor: () => ({}), pair: null, mergeSeals: (...r) => Seals.merge(...r) };
    const made = slots.map(s => {
      const job = { key: s.slot ? LINE + '#' + s.slot : LINE, row, state: s.state || 'review', copies: s.copies.slice(), text: s.words, lines: [s.words], lineInput: [s.words], lineMode: 'preserve', lineGap: .18,
        fit: { size: 2, capMm: 1, weight: 'Regular', glyphs: [], centre: [0, 0], angle: 0 }, view: { cx: 0, cy: 0, cutMembers: [], members: [] }, verify: { geometry: { ok: true } }, confidence: 1, questions: [], backs: [], t: 1 };
      if (s.slot) { Object.assign(job, { slot: s.slot, rowKey: LINE, groupKey: RID + ':' + TX }); job.row = Sides.sideRow(ctx, row, s.slot, job); job.engraveRec = { needed: true, state: job.state }; }
      else row.engrave = { needed: true, state: job.state, approved: false };
      jobs.set(job.key, job); return job;
    });
    if (slots.some(s => s.slot)) Sides.linkParent(ctx, row, made);
    const window = { addEventListener() {}, Seal: { whenIdle: async () => {}, defer: () => false }, CharmNestInteraction: { idle: async () => {}, defer: () => false }, CharmNestEngraveSides: Sides, CharmNestEngraveFaces: require(path.join(root, 'charm-nest-engrave-faces.js')) };
    let timer = 0; const timers = new Map();
    const context = { Date, Promise, Map, Set, WeakMap, WeakSet, Blob, ArrayBuffer, DataView, Uint8Array, TextEncoder, performance, O: require(path.join(root, 'charm-nest-orders.js')),
      window, document: { getElementById: () => null, querySelectorAll: () => [], activeElement: null }, localStorage: { getItem: () => null },
      setTimeout(fn, ms) { const id = ++timer; timers.set(id, { fn, ms }); return id; }, clearTimeout: id => timers.delete(id),
      B: { engrave: { fonts: { ok: true }, items: jobs }, pool: { rows: new Map(copies.map(id => [id, { copy: +String(id).split('_').pop() || 1 }])) }, run: { runId: 'run-1' }, orders: { rows: [row], byKey: new Map([[LINE, row]]) } },
      S: { settings: {}, cloud: { ok: true }, library: { rows: [] } }, PT: 72 / 25.4, MM: 25.4 / 72, Pool: { charmOf: () => charm, sheetOf: () => sheet, update: async (ids, patch) => { calls.pool.push({ ids: ids.slice(), patch }); }, previewIds: () => [], previewCharmOf: () => null },
      sourceOf: () => ({ parsed: {} }), allSheets: () => [sheet], Master: { entryFor: () => ({}) }, CNEngravingSeals: Object.assign({}, Seals, { press: async () => { calls.presses++; } }), CNListActivity: require(path.join(root, 'charm-nest-activity.js')),
      P: { buildBackFile: async options => { calls.built.push(options.meta); return { bytes: new Uint8Array([1]), reference: { redrawn: true }, wPt: 20, hPt: 20 }; } },
      employeeName: () => 'Paul', askEmployee: () => 'Paul', toast() {}, agent() {}, esc: String,
      Review: { remove() {}, add: r => calls.reviews.push(r), items: () => [], render() {} }, Orders: { rows: () => [row], render() {} },
      RunCtl: { poke() {}, backgroundSettled() {}, stopIfRunning() { throw Error('one back must not stop the run'); } },
      SheetEvents: { label: () => 'SS Sheet 1' },
      TL: { rec: (e) => { calls.events.push(e); return true; }, line() { return true; }, hash: s => String(s).length.toString(36) },
      Session: { schedule() { calls.schedule++; }, async flush() { this.flushNow(); }, flushNow() {}, copy: x => context.copy(x) },
      api: async (name, payload) => { if (payload.op === 'backPut') { calls.recorded.push(payload.back); return { ok: true }; } if (payload.op === 'getSheet') return { sheet: { id: sheet.sheetId, backPool: sheet.backPool } }; return { ok: true }; },
      uploadBytes: async p => { calls.uploaded.push(p); return { path: p, url: 'https://fixture.invalid/' + p }; },
      approvalFixture: { verify: async () => ({ ok: true }), preview: () => ({ _sizePt: { w: 20, h: 20 }, toBlob: fn => fn(new Blob(['png'])) }), paint() {}, index() {} } };
    vm.createContext(context); vm.runInContext(sessionCopy, context); vm.runInContext(script, context);
    Object.defineProperty(row, '_jobSlots', { value: new (vm.runInContext('Set', context))(slots.map(s => s.slot).filter(Boolean)), enumerable: false, configurable: true, writable: true });   // (the registry the page keeps on the line, made in the page's own realm)
    return { context, E: window.Engrave, row, jobs, made, calls, sheet, ctx };
  }
  const recordOf = (f, id) => f.calls.recorded.find(b => b.poolId === id);
  const builtOf = f => { const seen = new Set(); return f.calls.built.map(m => [m.poolId, m.text, m.copy]).filter(x => { const k = x.join('|'); if (seen.has(k)) return false; seen.add(k); return true; }); };   // (the approval builds the file once to verify it, the save builds it again: the distinct files)

  await t('D1', 'qty-2 charm, different words: approving piece 1 writes ITS back file only (its words, pool id, slot, copy); piece 2 writes nothing yet', async () => {
    const f = fixture([{ slot: 'C1', copies: [ID1], words: 'Anna' }, { slot: 'C2', copies: [ID2], words: 'Ben' }]);
    await f.E.approve(f.made[0], 'Paul', btn());
    await until(() => f.made[0].state === 'written');
    assert.deepEqual(builtOf(f), [[ID1, 'Anna', 1]]);
    assert.equal(f.calls.recorded.length, 1); const r = recordOf(f, ID1);
    assert.equal(r.text, 'Anna'); assert.equal(r.slot, 'C1'); assert.equal(r.copy, 1); assert.equal(r.sheetId, 'sheet-1');
    assert.ok(f.calls.uploaded.every(p => p.includes(ID1)), 'the files are named for piece 1: ' + f.calls.uploaded);
    assert.equal(f.made[1].state, 'review'); assert.equal(f.made[1].approvedAt || null, null); assert.equal(f.row.engrave.approved, false);
    assert.deepEqual(plain(f.calls.pool.map(c => c.ids)), [[ID1]], 'only piece 1 is marked engraved in the pool');
    assert.deepEqual(plain(Object.keys(f.row.engrave.pieces)), [ID1, ID2]); assert.equal(f.row.engrave.pieces[ID1].approved, true); assert.equal(f.row.engrave.pieces[ID2].approved, false);
  });
  await t('D2', 'then piece 2: its own file with Ben; the two files differ; the line is approved only now; one timeline event per piece with its slot', async () => {
    const f = fixture([{ slot: 'C1', copies: [ID1], words: 'Anna' }, { slot: 'C2', copies: [ID2], words: 'Ben' }]);
    await f.E.approve(f.made[0], 'Paul', btn()); await until(() => f.made[0].state === 'written');
    await f.E.approve(f.made[1], 'Paul', btn()); await until(() => f.made[1].state === 'written');
    assert.deepEqual(builtOf(f), [[ID1, 'Anna', 1], [ID2, 'Ben', 2]]);
    assert.equal(recordOf(f, ID2).text, 'Ben'); assert.equal(recordOf(f, ID2).slot, 'C2'); assert.notEqual(f.calls.uploaded.filter(p => p.endsWith('.ai'))[0], f.calls.uploaded.filter(p => p.endsWith('.ai'))[1]);
    assert.equal(f.row.engrave.approved, true);
    const evs = f.calls.events.filter(e => e.type === 'engraveApproved');
    assert.deepEqual(evs.map(e => [e.data.poolId, e.data.slot]), [[ID1, 'C1'], [ID2, 'C2']]);
    assert.ok(/Piece 1/.test(evs[0].text) && /Piece 2/.test(evs[1].text) && /Anna/.test(evs[0].text) && /Ben/.test(evs[1].text), evs.map(e => e.text).join(' | '));
    // each piece keeps its OWN seals: approving piece 2 did not add a seal to piece 1
    assert.equal(Seals.list(f.made[0]).filter(s => s.how === 'engraveApproved').length, 1); assert.equal(Seals.list(f.made[1]).filter(s => s.how === 'engraveApproved').length, 1);
  });
  await t('D3', 'the timeline is told which pieces to wait for: both while two are open, none for one piece, none for a piece cut plain', async () => {
    const f = fixture([{ slot: 'C1', copies: [ID1], words: 'Anna' }, { slot: 'C2', copies: [ID2], words: 'Ben' }]);
    assert.deepEqual(plain(f.E.engravePoolsOf(f.row)), [ID1, ID2]);
    assert.deepEqual(plain(f.E.engravePiecesOf(f.row).map(p => [p.label, p.text, p.done])), [['Piece 1', 'Anna', false], ['Piece 2', 'Ben', false]]);
    f.made[1].state = 'skipped'; assert.equal(f.E.engravePoolsOf(f.row), null, 'a piece cut plain is not waited for');
    const one = fixture([{ slot: null, copies: [ID1], words: 'Anna' }]); assert.equal(one.E.engravePoolsOf(one.row), null, 'a line with one job: as before');
  });
  await t('D4', 'a line bought once and a whole-line job (an old record): one job, one file per copy, as before', async () => {
    const f = fixture([{ slot: null, copies: [ID1, ID2], words: 'Mom' }]);
    await f.E.approve(f.made[0], 'Paul', btn()); await until(() => f.made[0].state === 'written');
    assert.deepEqual(builtOf(f).map(x => x.slice(0, 2)), [[ID1, 'Mom'], [ID2, 'Mom']]);
    assert.equal(f.calls.recorded.length, 2); assert.ok(f.calls.recorded.every(r => r.slot === undefined), 'a whole-line record carries no slot');
  });
  await t('D5', 'discs D1..D3 (and a later set D1.2): each disc its own file, slot on the record; the font per disc reads disc 2 of the second set too', async () => {
    const ids = [1, 2, 3, 4].map(n => LINE + '_' + n);
    const f = fixture([{ slot: 'D1', copies: [ids[0]], words: 'A' }, { slot: 'D2', copies: [ids[1]], words: 'B' }, { slot: 'D1.2', copies: [ids[2]], words: 'C' }, { slot: 'D2.2', copies: [ids[3]], words: 'D' }]);
    for (const j of f.made) { await f.E.approve(j, 'Paul', btn()); await until(() => j.state === 'written'); }
    assert.deepEqual(f.calls.recorded.map(r => [r.poolId, r.slot, r.text]), ids.map((id, i) => [id, ['D1', 'D2', 'D1.2', 'D2.2'][i], 'ABCD'[i]]));
    assert.ok(/pieceIndexOf = job => \{ const m = \/\^D\(\\d\+\)\(\?:\\\.\\d\+\)\?\$\//.test(source) || /D\(\\d\+\)\(\?:\\\.\\d\+\)\?\$\/\.exec\(String\(job && job\.slot/.test(source), 'the font index reads D2.2');
  });
  await t('D6', 'the seals module reads the font of a later set\'s disc as disc 2', () => {
    assert.ok(/D\(\\d\+\)\(\?:\\\.\\d\+\)\?\$\/\.exec\(String\(e\.job\?\.slot/.test(read('charm-nest-engraving-seals.js')));
  });

  /* ═══ E · the real page ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  let R = null; try { R = require('./_engraveRealWorld.cjs'); } catch (_) { R = null; }
  if (!R) console.log('  SKIP E (no real-world harness)');
  else await require('./_piecesDownstreamPage.cjs')({ R, t, assert, RID, Readiness });

  console.log(`\npieces-downstream: ${passed} passed, ${failed} failed`);
  if (failed) { for (const f of fails) console.log('  FAILED ' + f); process.exitCode = 1; }
})().catch(e => { console.error(e); process.exitCode = 1; });
