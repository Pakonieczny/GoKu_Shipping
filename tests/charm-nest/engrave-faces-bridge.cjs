// ENGMAP (front-back-1011): the bridge's ENGRAVE pipeline reads and keeps a front beside the back. Offline: the real page code (charm-nest-bridge.js, sliced) runs in a
// vm over stand-ins for the page's modules. No browser, no network, no paid call (the stand-in reader FAILS the test if it is asked for a line with a front), nothing live.
//
//   1  identity: a line that engraves the back only (no reading, mode back, a pair, discs) has the same job keys, the same job fields and the same side rows with and without the face model
//   2  a single charm with a front: two jobs (front first), the back converted to a side row, the line's engrave is their summary
//   3  the words come from the setup, never from a paid reading; each face has its own card, record and timeline line; a face with no words goes to a person
//   4  a back already decided on the line is kept as it is (same job, same record) when a front is added; only the front is read
//   5  ears and discs: a job for each (piece, face); the piece count counts pieces; jobOf / jobForPool / jobForRecord / jobsOfPiece find the right face
//   6  a person's setup (applyEngraving): add a front, drop an undecided face, cut a decided one plain (never delete), no engraving, words changed
//   7  a line whose setup now asks for a front that no job has is looked at again (wantsFront); a back nobody touched is read again from the setup
//   8  the other edited pieces (approve, write, revoke, skip, open, sheet backs, laser review) pass the face on, checked in the page source
//   node tests/charm-nest/engrave-faces-bridge.cjs
'use strict';
const path = require('path'), fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const transport = require.resolve(path.join(root, 'netlify/functions/_etsyMailAnthropic.js'));
require.cache[transport] = { id: transport, filename: transport, loaded: true, exports: {} };
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const Export = require(path.join(root, 'charm-nest-export.js'));
const Orders = require(path.join(root, 'charm-nest-orders.js'));
const IntakeWorld = require('./_engrave-intake-world.cjs');
const F = require('./pairs-fixtures.cjs');

const J = x => (x === undefined ? undefined : JSON.parse(JSON.stringify(x))), deq = (a, b, m) => assert.deepEqual(J(a), J(b), m);   // (values made inside the vm have its own Array and Object: compare them as data)
const results = [];
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  ' + name); } catch (e) { results.push([name, e]); console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 5).join('\n      ')); } };

const W = F.world({ orders: [
  { rid: F.rid(1), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }] },
  { rid: F.rid(3), lines: [{ n: 30, kind: 'discs', discs: 3, on: 'sh-gf1' }] },
  { rid: F.rid(4), lines: [{ n: 40, kind: 'single', on: 'sh-gf1' }] },
  { rid: F.rid(5), lines: [{ n: 50, kind: 'single', on: 'sh-gf1' }] },
] });
const lineRow = (w, ord, n, extra) => {
  const o = w.orders.find(x => x.rid === F.rid(ord)), l = o.lines.find(x => x.n === n);
  return Object.assign({ key: F.lineKey(o.rid, F.tx(n)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: o.rid }, line: { transactionId: F.tx(n), sku: l.sku, title: 'Mini Tag' }, spec: { designSku: l.sku, form: l.form, quantity: l.qty, personalization: ['front - W', 'back - 1. 27. 24'], engraveCandidate: true }, state: 'written' }, extra || {});
};
const BASE = { pair: lineRow(W, 1, 10), discs: lineRow(W, 3, 30), one: lineRow(W, 4, 40), two: lineRow(W, 5, 50) };
const BOTH = { v: 1, mode: 'both', source: 'labelled', front: { text: 'W' }, back: { text: '1. 27. 24' }, textSource: 'personalization', needsPerson: false, confidence: 0.95, why: 'The buyer labelled front and back' };
const mk = (base, engraving, extra) => { const r = Object.assign({}, base, { spec: Object.assign({}, base.spec, engraving === undefined ? {} : { engraving }) }, extra || {}); delete r.engrave; r.poolIds = base.poolIds.slice(); return r; };

/* ── the page's own code (SIDES … setNone, setupOf … applyEngraving, wantsFront), run over stand-ins ── */
const bridgeSrc = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const cut = (from, to) => { const a = bridgeSrc.indexOf(from), b = bridgeSrc.indexOf(to, a); assert.ok(a > 0 && b > a, 'slice ' + from.trim().slice(0, 30)); return bridgeSrc.slice(a, b); };
const SLICE4 = cut('  function revokeBacks(job)', '  /* ── 7.5 · the review controls'), SLICE5 = cut('  async function skip(job, by)', '  function sendBack(job');
const SLICE6 = cut('  async function sheetBackOutputs(sh)', '  /* ── the Engraving tab ── */');
const SLICE1 = cut('  const SIDES = ', '  function setNone(job'), SLICE2 = cut("  /** The line's effective engraving setup", '  let previewRecovery'), SLICE3 = cut("  /** A line whose setup asks for a front that has no front job yet", '  let classifyPass');
function harness(rows, o = {}) {
  const jobs = new Map(), asked = [], skipped = [], events = [], apiCalls = [], acts = [], poolUpdates = [], pool = new Map(W.pool.map(p => [p.poolId, p]));
  const win = { CharmNestEngraveSides: Sides, CharmNestPair: Pair }; if (o.faces !== false) win.CharmNestEngraveFaces = Faces;
  const byKey = new Map(rows.map(r => [r.key, r]));
  const ctx = {
    window: win, items: () => jobs, B: { pool: { rows: pool }, orders: { byKey }, run: null },
    Pool: { charmOf: id => { const r = pool.get(id); return r ? F.charmOf(r.sku) : null; }, update: async (ids, patch) => { poolUpdates.push({ ids, patch }); } }, Master: { entryFor: sku => F.entryOf(sku) }, CNEngravingSeals: Seals, DECIDED: ['approved', 'written', 'skipped'],
    O: { FINISHED_LINE: new Set(['committed', 'gone', 'cancelled']), engravingNote: null }, S: { settings: { engraveConfidence: .65 } },
    render() {}, agent() {}, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, JSON, Math, RegExp, Error,
    Review: { add() {}, remove() {}, render() {} }, RunCtl: { poke() {}, save: async () => {} }, Session: { schedule() {} }, Orders: { render() {}, rows: () => rows, lineRecord: r => [r.key, { key: r.key }] },
    TL: { line: (row, type, d) => { if (d && d.once && events.some(e => e.type === type && e.id === d.id)) return; events.push({ row, type, id: d && d.id, text: d && d.text, data: d && d.data }); }, hash: s => String(s).length },
    sheets: o.sheets || [], allSheets: () => (o.sheets || []), refreshBacks() {}, backQueue: Promise.resolve(), toast() {}, api: async (fn, body) => { apiCalls.push(body); return {}; }, goes() {}, EG_TAB: x => x, showDecided() {}, CNListActivity: { touch() {} }, humanAct: (kind, d) => acts.push(Object.assign({ kind }, d)), syncEditedBack: async () => {},
    employeeName: () => 'Paul', needEmployee: async () => 'Paul', charmFor: () => null,
    setReady(job) { job.state = 'ready'; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'ready', text: job.text, approved: false }); return job; },
    toWords(job, why) { job.state = 'words'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'words', text: job.text, approved: false, reason: why }); return job; },
    setNone(job, why) { job.state = 'none'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: false, state: 'none', reason: why, approved: true }); return job; },
    ...(o.realSkip ? {} : { skip: async (job, by) => { skipped.push(job.key); job.state = 'skipped'; job.row.engrave = Object.assign({}, job.row.engrave, { needed: false, state: 'skipped', approved: true, decidedBy: by }); return job; } }),
    agentCall: async (mode, ask) => { asked.push(ask); if (o.reading === undefined) throw new Error('a paid reading was asked for'); return o.reading; },
  };
  vm.createContext(ctx); vm.runInContext(SLICE1, ctx); vm.runInContext(SLICE2, ctx); vm.runInContext(SLICE3, ctx); vm.runInContext(SLICE4, ctx); if (o.realSkip) vm.runInContext(SLICE5, ctx);
  if (o.outputs) { const up = o.outputs; Export.backIndexPdf = async a => { up.pdfs.push(a.title + (a.face ? ':' + a.face : '')); return new Uint8Array(1); }; win.CharmNestExport = Export; Object.assign(ctx, { S: Object.assign(ctx.S, { cloud: { ok: true } }), CharmNestAssets: { bytes: async () => null }, CharmNestExport: Export, uploadBytes: async (p2) => { up.paths.push(p2); return { path: p2, url: 'u:' + p2 }; }, TextEncoder, Uint8Array }); vm.runInContext(SLICE6, ctx); }
  vm.runInContext('this.pieceJobsOf = pieceJobsOf; this.pieceCountOf = pieceCountOf; this.faceJobs = faceJobs; this.jobOf = jobOf; this.jobsOfPiece = jobsOfPiece; this.pieceFields = pieceFields; this.setupOf = setupOf; this.untouched = untouched;', ctx);   // (the page\'s consts, reachable by the test)
  return { ctx, jobs, asked, skipped, events, apiCalls, acts, poolUpdates, byKey, keys: () => [...jobs.keys()] };
}
const decide = (j, state, text) => { j.state = state; if (text !== undefined) { j.text = text; j.lines = text.split('\n'); } j.approvedAt = Date.now(); j.approvedBy = 'Paul'; j.row.engrave = Object.assign({}, j.row.engrave, { needed: true, state, approved: true, text: j.text, approvedBy: 'Paul', approvedAt: j.approvedAt }); };
const shape = j => { const o = {}; for (const k of Object.keys(j).sort()) if (k !== 'row' && k !== 't' && typeof j[k] !== 'function') o[k] = j[k]; return JSON.stringify(o); };

(async () => {
  console.log('ENGMAP bridge');

  /* 1 ── identity */
  await t('1 a line that engraves the back only has the same jobs with and without the face model (keys, fields, side rows)', async () => {
    for (const [name, base] of [['one', BASE.one], ['pair', BASE.pair], ['discs', BASE.discs]]) for (const eng of [undefined, { v: 1, mode: 'back', source: 'default', back: { text: 'Anna' }, needsPerson: false, textSource: 'personalization', confidence: .9 }]) {
      const a = harness([mk(base, eng)]), b = harness([mk(base, eng)], { faces: false });
      const ja = a.ctx.ensureJobs(a.byKey.values().next().value), jb = b.ctx.ensureJobs(b.byKey.values().next().value);
      deq(ja.map(j => j.key), jb.map(j => j.key), name + ' keys'); deq(ja.map(shape), jb.map(shape), name + ' fields');
      assert.ok(ja.every(j => !('face' in j) && !j.linked), name + ' no face, not linked'); deq(ja.map(j => !!j.row.parentRow), jb.map(j => !!j.row.parentRow));
      if (name === 'one') assert.equal(ja[0].row, a.byKey.values().next().value, 'a single charm\'s job still works on the line\'s own row');
    }
  });

  /* 2 ── a single charm with a front */
  await t('2 a single charm that reads "both": two jobs, the front first, each on its own side row; the line\'s engrave is their summary', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]), js = h.ctx.ensureJobs(row);
    deq(js.map(j => j.key), [row.key + '~front', row.key]);
    const [fr, bk] = js; assert.equal(fr.face, 'front'); assert.equal('face' in bk, false); assert.equal(Faces.faceOf(fr), 'front'); assert.equal(Faces.faceOf(bk), 'back');
    assert.equal(fr.row.parentRow, row); assert.equal(bk.row.parentRow, row); assert.notEqual(fr.row, bk.row); assert.equal(fr.row.key, row.key + '~front'); assert.equal(bk.row.key, row.key); assert.equal(bk.linked, true);
    deq(fr.copies, row.poolIds); deq(bk.copies, row.poolIds);
    assert.equal(h.ctx.pieceCountOf(row), 1, 'one piece with two faces'); assert.equal(h.ctx.jobOf(row, null, 'front'), fr); assert.equal(h.ctx.jobOf(row, null, 'back'), bk); assert.equal(h.ctx.jobOf(row), fr, 'the face still to settle: the front first');
    deq(h.ctx.jobsOf(row, { face: 'back' }), [bk]); deq(h.ctx.pieceJobsOf(row, 'front'), [fr]); const fj = h.ctx.faceJobs(row); assert.equal(fj.front, fr); assert.equal(fj.back, bk);
    const pp = h.ctx.jobsOfPiece(row.poolIds[0]); assert.equal(pp.front, fr); assert.equal(pp.back, bk);
    // the summary reads through the line's own engrave
    fr.row.engrave = { needed: true, state: 'ready', approved: false, text: 'W' }; bk.row.engrave = { needed: true, state: 'approved', approved: true, text: '1. 27. 24', approvedAt: 5 };
    assert.equal(row.engrave.approved, false, 'approved only when every face is'); assert.equal(row.engrave.state, 'ready'); assert.match(row.engrave.text, /Front: W/); assert.match(row.engrave.text, /Back: 1\. 27\. 24/);
    const pid = row.poolIds[0]; assert.equal(row.engrave.pieces[pid].state, 'ready'); assert.equal(row.engrave.pieces[pid].approved, false); assert.equal(row.engrave.pieces[pid].faces.front.state, 'ready'); assert.equal(row.engrave.pieces[pid].faces.back.state, 'approved');
    fr.row.engrave = { needed: true, state: 'approved', approved: true, text: 'W', approvedAt: 6 };
    assert.equal(row.engrave.approved, true); assert.equal(row.engrave.state, 'approved'); assert.equal(row.engrave.pieces[pid].approved, true);
    // a front only (no back) line is a faced line too
    const r2 = mk(BASE.two, Object.assign({}, BOTH, { mode: 'front', back: null })), h2 = harness([r2]), j2 = h2.ctx.ensureJobs(r2);
    deq(j2.map(j => j.key), [r2.key + '~front']); assert.equal(j2[0].row.parentRow, r2); assert.equal(h2.ctx.jobOf(r2, null, 'back'), null);
  });

  /* 3 ── the words come from the setup, not from a paid reading */
  await t('3 classify: front W and back 1. 27. 24 from the setup, no paid reading, a card and a timeline line for each face', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]); await h.ctx.classify(row);
    assert.equal(h.asked.length, 0, 'no paid reading'); const fr = h.jobs.get(row.key + '~front'), bk = h.jobs.get(row.key);
    assert.equal(fr.text, 'W'); assert.equal(bk.text, '1. 27. 24'); assert.equal(fr.state, 'ready'); assert.equal(bk.state, 'ready');
    assert.equal(fr.requests.side, 'front'); assert.equal(bk.requests.side, 'back'); assert.equal(fr.wordsSource, 'spec.engraving'); assert.notEqual(fr.row.engrave, bk.row.engrave, 'never one shared record');
    const ev = h.events.filter(e => e.type === 'engraveNeeded'); assert.equal(ev.length, 2);
    const ef = ev.find(e => e.data.face === 'front'), eb = ev.find(e => !e.data.face); assert.match(ef.text, /^Front engraving: “W”/); assert.match(eb.text, /^Back engraving: “1\. 27\. 24”/); assert.notEqual(ef.id, eb.id);
    assert.equal(row.engrave.state, 'ready'); assert.equal(row.engrave.approved, false);
  });
  await t('3b a line the note leaves open ("both" with no words) goes to a person for each face, with no guess and no paid reading', async () => {
    const row = mk(BASE.one, { v: 1, mode: 'both', source: 'unlabelled', needsPerson: true, textSource: 'personalization', confidence: .3 }), h = harness([row]); await h.ctx.classify(row);
    assert.equal(h.asked.length, 0); for (const j of [...h.jobs.values()]) { assert.equal(j.state, 'words'); assert.equal(j.approvedAt, null); assert.ok(j.questions.length); assert.equal(j.text, ''); }
    assert.match(h.jobs.get(row.key + '~front').questions[0], /does not say which words go on the front/, 'with no reason from the intake the card says what is missing');
    const row2 = mk(BASE.one, { v: 1, mode: 'both', source: 'note', needsPerson: true, reviewKind: 'texts', why: 'front and back engraving, but the words do not say which go on the front and which on the back: a person decides', textSource: 'unlabelled', front: null, back: null }), h2 = harness([row2]); await h2.ctx.classify(row2);
    assert.match(h2.jobs.get(row2.key).questions[0], /^front and back engraving, but the words do not say/, 'the intake\'s own reason is the question');
  });
  await t('3d a reading marked for a person (a conflict) keeps its words as the proposal, in the words card, approved by nobody; a blank option with words is asked, not read; a person\'s "no engraving" stays', async () => {
    const row = mk(BASE.one, { v: 1, mode: 'both', source: 'option', front: { text: 'W' }, back: { text: 'B' }, needsPerson: true, reviewKind: 'conflict', why: 'two options disagree', textSource: 'labelled' }), h = harness([row]); await h.ctx.classify(row);
    for (const [k, w] of [[row.key + '~front', 'W'], [row.key, 'B']]) { const j = h.jobs.get(k); assert.equal(j.state, 'words'); assert.equal(j.text, w); assert.equal(j.questions[0], 'two options disagree'); assert.equal(j.approvedAt, null); }
    assert.equal(h.asked.length, 0);
    const blank = mk(BASE.two, { v: 1, mode: 'none', source: 'option', needsPerson: true, reviewKind: 'conflict', why: 'the option says blank but the buyer wrote words: a person decides' }), hb = harness([blank]); await hb.ctx.classify(blank);
    const bj = hb.jobs.get(blank.key); assert.equal(hb.asked.length, 0); assert.equal(bj.state, 'words'); assert.equal(bj.text, 'front - W\nback - 1. 27. 24', 'the buyer\'s words are the proposal'); assert.match(bj.questions[0], /option says blank/);
    const no = mk(BASE.one, BOTH, { engravingOverride: { mode: 'none', by: 'Rita', at: 5 } }), hn = harness([no]); await hn.ctx.classify(no);
    assert.equal(hn.asked.length, 0); assert.equal(hn.jobs.get(no.key).state, 'none'); assert.match(hn.jobs.get(no.key).reason, /decided by Rita/); assert.equal(no.engrave.approved, true);
  });
  await t('3e words for each ear: the ear\'s own words are the proposal on its front and its back (a person confirms each), never one ear\'s words on the other', async () => {
    const eng = { v: 1, mode: 'both', source: 'note', needsPerson: true, reviewKind: 'pieces', why: 'each ear has its own words', textSource: 'labelled', pieces: { L: { front: { text: 'A' }, back: { text: '1' } }, R: { front: { text: 'B' }, back: { text: '2' } } } };
    const row = mk(BASE.pair, eng), h = harness([row]); await h.ctx.classify(row); const txt = k => h.jobs.get(row.key + k).text;
    assert.equal(txt('#L~front'), 'A'); assert.equal(txt('#L'), '1'); assert.equal(txt('#R~front'), 'B'); assert.equal(txt('#R'), '2'); assert.ok([...h.jobs.values()].every(j => j.state === 'words'));
  });
  await t('3c "back" read by the intake with nothing a person said is the old reader (one paid reading, the same question); a person\'s own back words need no reading', async () => {
    const reading = { engrave: true, text: 'Anna', source: 'personalization', confidence: .95, questions: [], requests: { side: 'back' } };
    const row = mk(BASE.one, { v: 1, mode: 'back', source: 'default', back: { text: 'Anna' }, needsPerson: false, textSource: 'personalization', confidence: .9 }), h = harness([row], { reading }); await h.ctx.classify(row);
    assert.equal(h.asked.length, 1); assert.equal('slots' in h.asked[0], false); deq([...h.jobs.keys()], [row.key]); assert.equal(h.jobs.get(row.key).text, 'Anna');
    const mine = mk(BASE.one, undefined, { engravingOverride: { mode: 'back', back: { text: 'Anna K' }, by: 'Paul', at: 1 } }), h2 = harness([mine]); await h2.ctx.classify(mine);
    assert.equal(h2.asked.length, 0); deq([...h2.jobs.keys()], [mine.key]); assert.equal(h2.jobs.get(mine.key).text, 'Anna K'); assert.equal(h2.jobs.get(mine.key).state, 'ready'); assert.equal('face' in h2.jobs.get(mine.key), false);
  });

  await t('3f a front / both line with NO words (engraveCandidate false, reviewKind "nowords") is raised and asked about, not cut plain; lines with nothing to engrave, add-ons and no-design lines stay plain', async () => {
    const nowords = { v: 1, mode: 'both', source: 'option', needsPerson: true, reviewKind: 'nowords', textSource: 'none', front: null, back: null, why: 'front and back engraving was chosen but no words were written', confidence: .3 };
    const row = mk(BASE.one, nowords); row.spec.engraveCandidate = false; row.spec.personalization = [];
    const h = harness([row]); await h.ctx.classify(row); assert.equal(h.asked.length, 0); assert.equal(row.spec.engraveCandidate, true, 'raised'); assert.equal(h.jobs.get(row.key + '~front').state, 'words'); assert.equal(h.jobs.get(row.key).state, 'words');
    assert.match(h.jobs.get(row.key).questions[0], /no words were written/); assert.equal(Faces.review(Faces.setupOf(row)).kind, 'nowords'); assert.equal(Faces.review(Faces.setupOf(row)).mode, 'both');
    for (const [name, patch] of [['no design', { noDesign: true }], ['add-on', { special: { own: true } }], ['no sku', { designSku: null }]]) {
      const r = mk(BASE.two, nowords); Object.assign(r.spec, { engraveCandidate: false, personalization: [] }, patch); const hp = harness([r]); await hp.ctx.classify(r);
      assert.equal(hp.jobs.get(r.key).state, 'none', name); assert.equal(r.spec.engraveCandidate, false, name);
    }
    const plain = mk(BASE.two, undefined); plain.spec.engraveCandidate = false; plain.spec.personalization = []; const hq = harness([plain]); await hq.ctx.classify(plain); assert.equal(hq.jobs.get(plain.key).state, 'none'); assert.equal(plain.spec.engraveCandidate, false);
  });
  await t('3g a person\'s setup on a plain line makes it a line to engrave; a plain (none) job of a face the setup now asks for is opened again, a decided one is not', async () => {
    const row = mk(BASE.one, undefined); row.spec.engraveCandidate = false; row.spec.personalization = [];
    const h = harness([row]); await h.ctx.classify(row); assert.equal(h.jobs.get(row.key).state, 'none');
    const out = await h.ctx.applyEngraving(row, { mode: 'both', front: { text: 'W' }, back: { text: 'Z' } }, 'Paul'); assert.equal(out.ok, true, JSON.stringify(out));
    assert.equal(row.spec.engraveCandidate, true); assert.equal(h.jobs.get(row.key + '~front').state, 'ready'); assert.equal(h.jobs.get(row.key).state, 'ready'); assert.equal(h.jobs.get(row.key).text, 'Z'); assert.equal(h.asked.length, 0);
    // a decided back stays when the person only changes the front's words
    decide(h.jobs.get(row.key), 'approved'); const out2 = await h.ctx.applyEngraving(row, { front: { text: 'Y' } }, 'Paul'); assert.equal(h.jobs.get(row.key).state, 'approved'); assert.equal(h.jobs.get(row.key + '~front').text, 'Y'); deq(out2.kept, []);
  });
  /* 4 ── a decided back is kept */
  await t('4 a back already approved on a single charm is kept (same job, same record, same state) when the setup now asks for a front; only the front is read', async () => {
    const row = mk(BASE.one, undefined), h = harness([row]), old = h.ctx.ensureJobs(row)[0];
    decide(old, 'approved', 'Anna'); const rec = Object.assign({}, row.engrave); assert.equal(rec.approved, true);
    row.spec.engraving = BOTH;
    const js = h.ctx.ensureJobs(row); assert.equal(js.length, 2); const bk = js.find(j => !Faces.isFront(j)), fr = js.find(Faces.isFront);
    assert.equal(bk, old, 'the very same job'); assert.equal(bk.state, 'approved'); assert.equal(bk.text, 'Anna'); deq(bk.engraveRec, rec, 'its record went with it to its own row'); assert.equal(bk.row.parentRow, row);
    assert.equal(fr.state, 'classify'); assert.equal(row.engrave.approved, false, 'the line is not approved until the front is');
    await h.ctx.classify(row); assert.equal(h.asked.length, 0); assert.equal(bk.text, 'Anna', 'the approved back was not read again'); assert.equal(bk.state, 'approved'); assert.equal(fr.text, 'W'); assert.equal(fr.state, 'ready');
  });

  /* 5 ── ears and discs */
  await t('5 a pair with a front: a job for each ear and face (L front, L back, R front, R back); the piece count counts ears; the lookups find the right one', async () => {
    const row = mk(BASE.pair, BOTH), h = harness([row]), js = h.ctx.ensureJobs(row);
    deq(js.map(j => j.key), [row.key + '#L~front', row.key + '#L', row.key + '#R~front', row.key + '#R']);
    assert.equal(h.ctx.pieceCountOf(row), 2); const [L, R] = row.poolIds;
    assert.equal(h.ctx.jobForPool(row, R, 'front').key, row.key + '#R~front'); assert.equal(h.ctx.jobForPool(row, R, 'back').key, row.key + '#R'); assert.equal(h.ctx.jobForPool(row, L).key, row.key + '#L~front', 'the face still to settle');
    assert.equal(h.ctx.jobOf(row, 'R', 'back').key, row.key + '#R'); assert.equal(h.ctx.jobOf(row, L, 'front').key, row.key + '#L~front');
    assert.equal(h.ctx.jobsOfPiece(L).front.key, row.key + '#L~front'); assert.equal(h.ctx.jobsOfPiece(R).back.key, row.key + '#R');
    const pcs = h.ctx.piecesOf(row); assert.equal(pcs.length, 4); deq(pcs.map(p => p.face), ['front', 'back', 'front', 'back']); assert.ok(pcs.every(p => p.label));
    const fj = h.ctx.faceJobs(row, 'R'); assert.equal(fj.front.key, row.key + '#R~front'); assert.equal(fj.back.key, row.key + '#R');
    // a record of the piece says its face; the back's says nothing new
    assert.equal(h.ctx.pieceFields(js[0]).face, 'front'); assert.equal('face' in h.ctx.pieceFields(js[1]), false); assert.equal(h.ctx.pieceFields(js[1]).slot, 'L');
    await h.ctx.classify(row); assert.equal(h.asked.length, 0); assert.ok(js.every(j => j.state === 'ready')); assert.equal(js[0].text, 'W'); assert.equal(js[1].text, '1. 27. 24');
  });
  await t('5b discs with a front only: a front job on each disc, no back jobs', async () => {
    const row = mk(BASE.discs, Object.assign({}, BOTH, { mode: 'front', back: null })), h = harness([row]), js = h.ctx.ensureJobs(row);
    deq(js.map(j => j.key), ['D1', 'D2', 'D3'].map(s => row.key + '#' + s + '~front')); assert.equal(h.ctx.pieceCountOf(row), 3); deq(h.ctx.jobsOf(row, { face: 'back' }), []);
    assert.equal(h.ctx.jobsOf(row, { face: 'front' }).length, 3);
  });
  await t('5c jobForRecord: a back record finds the back, a front record makes the front job (a restored set), neither mixes with the other', async () => {
    const row = mk(BASE.one, undefined), h = harness([row]); h.ctx.ensureJobs(row); const pid = row.poolIds[0];
    const bk = h.ctx.jobForRecord(row, { poolId: pid }); assert.equal(bk.key, row.key); assert.equal(h.keys().length, 1);
    const fr = h.ctx.jobForRecord(row, { poolId: pid, face: 'front' }); assert.equal(fr.key, row.key + '~front'); assert.equal(fr.face, 'front'); assert.equal(h.keys().length, 2); fr.state = 'written';   // (the caller of a restore settles it at once)
     assert.equal(h.ctx.jobForRecord(row, { poolId: pid, face: 'front' }), fr, 'the same job the next time');
    assert.equal(h.ctx.jobForRecord(row, { poolId: pid }), bk, 'a back record still finds the back'); assert.equal(bk.row.parentRow, row, 'the back is linked to the line');
    assert.equal(h.ctx.jobOf(row.constructor === Object ? row : row, null, 'front'), fr);
  });
  await t('5d dropLine takes every face job with it', async () => {
    const row = mk(BASE.pair, BOTH), h = harness([row]); h.ctx.ensureJobs(row); assert.equal(h.ctx.dropLine(row), 4); assert.equal(h.keys().length, 0);
  });

  /* 6 ── a person's setup */
  await t('6a applyEngraving: add a front to a back-only line, with the person\'s words: a front job, no paid reading, a seal-free timeline line by the person', async () => {
    const row = mk(BASE.one, undefined), h = harness([row]), bk0 = h.ctx.ensureJobs(row)[0]; bk0.text = 'Anna'; bk0.lines = ['Anna'];
    const out = await h.ctx.applyEngraving(row, { mode: 'both', front: { text: 'W' }, back: { text: 'Anna' } });
    assert.equal(out.ok, true, JSON.stringify(out)); assert.equal(h.asked.length, 0); assert.equal(row.engravingOverride.by, 'Paul'); assert.equal(row.engravingOverride.mode, 'both');
    deq(out.added, [row.key + '~front']); const fr = h.jobs.get(row.key + '~front'), bk = h.jobs.get(row.key); assert.equal(fr.text, 'W'); assert.equal(fr.state, 'ready'); assert.equal(bk.text, 'Anna'); assert.equal(bk.state, 'ready');
    const e = h.events.find(x => x.type === 'engraveChanged' && x.data.how === 'setup'); assert.ok(e); assert.match(e.text, /^Engraving set by Paul: Front: W · Back: Anna/);
  });
  await t('6b applyEngraving: back only again: the undecided front is dropped, the line is summarised by the back alone', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]); await h.ctx.classify(row); assert.equal(h.keys().length, 2);
    const out = await h.ctx.applyEngraving(row, { mode: 'back' }, 'Paul'); deq(out.dropped, [row.key + '~front']); deq(h.keys(), [row.key]);
    assert.equal(row.engrave.pieces[row.poolIds[0]].faces, undefined, 'no front, no faces map'); assert.equal(h.ctx.jobsOf(row).length, 1);
  });
  await t('6c a decided face the setup no longer asks for is cut plain (skipped by the person), never deleted', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]); await h.ctx.classify(row); const fr = h.jobs.get(row.key + '~front'); decide(fr, 'approved');
    const out = await h.ctx.applyEngraving(row, { mode: 'back' }, 'Paul'); deq(out.dropped, []); deq(out.skipped, [row.key + '~front']); deq(h.skipped, [row.key + '~front']);
    assert.equal(h.jobs.get(row.key + '~front'), fr, 'the job and its record stay'); assert.equal(fr.state, 'skipped');
  });
  await t('6d "no engraving": undecided faces are cut to none, an approved one is skipped by the person; no front is made', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]); await h.ctx.classify(row); decide(h.jobs.get(row.key), 'approved');
    const out = await h.ctx.applyEngraving(row, { mode: 'none' }, 'Paul'); assert.equal(out.ok, true);
    assert.equal(h.jobs.get(row.key).state, 'skipped'); assert.equal(h.jobs.get(row.key + '~front'), undefined, 'the undecided front went'); assert.equal(h.asked.length, 0);
    const r2 = mk(BASE.two, undefined), h2 = harness([r2]); await h2.ctx.classify.call(null, r2).catch(() => {}); // (no reading offered: the back falls to the words card)
    h2.ctx.ensureJobs(r2); const out2 = await h2.ctx.applyEngraving(r2, { mode: 'none' }, 'Paul'); assert.equal(out2.ok, true); assert.equal(h2.jobs.get(r2.key).state, 'none'); assert.equal(h2.jobs.get(r2.key).decision.none, true);
  });
  await t('6e words changed: an undecided face is read from the setup again; a decided face keeps its decision (it is sent back from its own card)', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]); await h.ctx.classify(row); decide(h.jobs.get(row.key), 'approved');
    const out = await h.ctx.applyEngraving(row, { mode: 'both', front: { text: 'Z' }, back: { text: 'NEW BACK' } }, 'Paul');
    assert.equal(h.jobs.get(row.key + '~front').text, 'Z'); assert.equal(h.jobs.get(row.key + '~front').state, 'ready'); deq(out.kept, [row.key]); assert.equal(h.jobs.get(row.key).text, '1. 27. 24'); assert.equal(h.jobs.get(row.key).state, 'approved'); assert.equal(h.asked.length, 0);
  });
  await t('6f a line past engraving, or not read yet, is refused with a reason; nothing changes', async () => {
    const row = mk(BASE.one, BOTH, { state: 'committed' }), h = harness([row]); const out = await h.ctx.applyEngraving(row, { mode: 'back' }, 'Paul'); assert.equal(out.ok, false); assert.match(out.error, /past engraving/); assert.equal(row.engravingOverride, undefined);
    const r2 = { key: 'x', poolIds: [], spec: null }; assert.equal((await harness([r2]).ctx.applyEngraving(r2, { mode: 'back' }, 'P')).ok, false);
  });

  const tick = () => new Promise(r => setTimeout(r, 5));
  await t('6g revoke: a front takes only its own record and file, the back keeps its own (and the other way round); a back-only line asks the server exactly as before', async () => {
    const row = mk(BASE.one, BOTH), pid = row.poolIds[0], sheets = [{ sheetId: 's1', backPool: [{ poolId: pid, text: '1. 27. 24' }, { poolId: pid, face: 'front', text: 'W' }, { poolId: 'other', text: 'x' }] }];
    const h = harness([row], { sheets }); const [fr, bk] = h.ctx.ensureJobs(row); for (const j of [fr, bk]) { j.state = 'approved'; j.approvedAt = 1; j.copies = [pid]; }
    h.ctx.revokeBacks(fr); await tick(); deq(sheets[0].backPool.map(b => b.poolId + (b.face === 'front' ? '~front' : '')), [pid, 'other']); deq(h.apiCalls, [{ op: 'backInvalidate', poolIds: [pid], face: 'front' }]);
    sheets[0].backPool.push({ poolId: pid, face: 'front', text: 'W' }); h.apiCalls.length = 0;
    h.ctx.revokeBacks(bk); await tick(); deq(sheets[0].backPool.map(b => b.poolId + (b.face === 'front' ? '~front' : '')), ['other', pid + '~front']); deq(h.apiCalls, [{ op: 'backInvalidate', poolIds: [pid] }]);
    const one = mk(BASE.two, undefined), sh2 = [{ sheetId: 's2', backPool: [{ poolId: one.poolIds[0], text: 'Anna' }] }], h2 = harness([one], { sheets: sh2 }), j2 = h2.ctx.ensureJobs(one)[0]; j2.copies = one.poolIds.slice(); j2.approvedAt = 1;
    h2.ctx.revokeBacks(j2); await tick(); deq(h2.apiCalls, [{ op: 'backInvalidate', poolIds: one.poolIds }]); assert.equal(sh2[0].backPool.length, 0);
  });
  await t('6h invalidate (words changed on the order): every face of the line goes back to be read, each record marked', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row], { sheets: [] }); const [fr, bk] = h.ctx.ensureJobs(row); for (const j of [fr, bk]) { j.state = 'approved'; j.approvedAt = 1; j.engraveRec = { approved: true, state: 'approved' }; }
    h.ctx.invalidate(row, 'the words changed'); await tick(); assert.equal(fr.state, 'classify'); assert.equal(bk.state, 'classify'); assert.equal(fr.engraveRec.approved, false); assert.equal(bk.engraveRec.state, 'reclassify'); assert.equal(row.engrave.approved, false);
  });
  await t('6i skip: a skipped front is cut plain on its own (front flags only, a front event, the back untouched); a skipped back keeps the flags it always set', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row], { sheets: [], realSkip: true }); await h.ctx.classify(row); const fr = h.jobs.get(row.key + '~front'), bk = h.jobs.get(row.key);
    await h.ctx.skip(fr, 'Paul'); assert.equal(fr.state, 'skipped'); assert.equal(bk.state, 'ready'); deq(h.poolUpdates[0].patch, { engraveFront: false, engraveFrontSkippedBy: 'Paul' });
    const ev = h.events.find(e => e.type === 'engraveChanged' && e.data.how === 'skipped'); assert.match(ev.text, /^Front engraving skipped by Paul/); assert.equal(ev.data.face, 'front'); assert.match(h.acts[0].detail, /^front engraving skipped/);
    assert.equal(row.engrave.approved, false, 'the back is still to be approved: the line is not'); assert.equal(row.engrave.pieces[row.poolIds[0]].faces.front.state, 'skipped');
    await h.ctx.skip(bk, 'Paul'); deq(h.poolUpdates[1].patch, { engrave: false, engraveSkippedBy: 'Paul' }); assert.equal(row.engrave.approved, true); assert.equal(row.engrave.state, 'skipped');
    const plain = mk(BASE.two, undefined), hp = harness([plain], { sheets: [], realSkip: true }), pj = hp.ctx.ensureJobs(plain)[0]; pj.text = 'Anna'; pj.lines = ['Anna']; await hp.ctx.skip(pj, 'Paul');
    deq(hp.poolUpdates[0].patch, { engrave: false, engraveSkippedBy: 'Paul' }); const pe = hp.events.find(e => e.type === 'engraveChanged'); assert.match(pe.text, /^Engraving skipped by Paul/); assert.equal('face' in pe.data, false);
  });

  await t('6j the sheet\'s back index: a back-only sheet writes the same two files at the same paths with the same record; a sheet with fronts also writes front/front-index.pdf and front/front-report.json -> sh.frontOutputs', async () => {
    const mkSheet = list => ({ sheetId: 'sx', fileBase: 'GOLD_1', folderPath: 'Sheets/GOLD_1', metal: 'gold', backPool: list });
    const back = { poolId: 'p1', order: '1', sku: 'S', copy: 1, text: 'Anna', font: 'Ace', weight: 400, sizePt: 5, capMm: 1.2, approvedBy: 'Paul', approvedAt: 5, outputs: { ai: { path: 'a.ai' }, png: { url: 'x' } } }, front = Object.assign({}, back, { face: 'front', text: 'W' });
    const up1 = { paths: [], pdfs: [] }, h1 = harness([], { outputs: up1 }), sh1 = mkSheet([back]); await h1.ctx.sheetBackOutputs(sh1);
    deq(up1.paths, ['Sheets/GOLD_1/back/back-index.pdf', 'Sheets/GOLD_1/back/back-report.json']); deq(up1.pdfs, ['GOLD_1']); deq(sh1.backOutputs, { index: { path: 'Sheets/GOLD_1/back/back-index.pdf', url: 'u:Sheets/GOLD_1/back/back-index.pdf' }, report: { path: 'Sheets/GOLD_1/back/back-report.json', url: 'u:Sheets/GOLD_1/back/back-report.json' }, count: 1 }); assert.equal(sh1.frontOutputs, undefined);
    deq(h1.apiCalls, [{ op: 'putSheet', sheet: { id: 'sx', backOutputs: sh1.backOutputs } }]);
    const up2 = { paths: [], pdfs: [] }, h2 = harness([], { outputs: up2 }), sh2 = mkSheet([back, front]); await h2.ctx.sheetBackOutputs(sh2);
    deq(up2.paths.sort(), ['Sheets/GOLD_1/back/back-index.pdf', 'Sheets/GOLD_1/back/back-report.json', 'Sheets/GOLD_1/front/front-index.pdf', 'Sheets/GOLD_1/front/front-report.json']); deq(up2.pdfs.sort(), ['GOLD_1', 'GOLD_1:front']);
    assert.equal(sh2.backOutputs.count, 1); assert.equal(sh2.frontOutputs.count, 1); assert.match(sh2.frontOutputs.index.path, /\/front\/front-index\.pdf$/); assert.deepEqual(Object.keys(h2.apiCalls[0].sheet).sort(), ['backOutputs', 'frontOutputs', 'id']);
  });

  await t('6k a checkpoint restore: the jobs of a line with a front come back with their own side rows and records, the line\'s summary reads as before, and the back-only line comes back as it was', async () => {
    const row = mk(BASE.pair, BOTH), h = harness([row]); await h.ctx.classify(row); const js = h.ctx.jobsOf(row); decide(js[1], 'approved'); decide(js[0], 'approved'); const before = J(row.engrave);
    // what the session saves is the job without its row; the restore puts the line row back in and relinks (Session: `j.row = byKey.get(j.rowKey || j.key)`, then Engrave.relinkAll())
    const saved = js.map(j => { const c = Object.assign({}, j); delete c.row; return JSON.parse(JSON.stringify(c)); });
    const row2 = mk(BASE.pair, BOTH), h2 = harness([row2]); for (const c of saved) { c.row = h2.byKey.get(c.rowKey || c.key); h2.jobs.set(c.key, c); }
    assert.equal(h2.ctx.relinkAll(), 1); const re = h2.ctx.jobsOf(row2); deq(re.map(j => j.key), js.map(j => j.key));
    for (const j of re) { assert.equal(j.row.parentRow, row2); assert.equal(j.row.key, j.key); assert.equal(Faces.faceOf(j.row), Faces.faceOf(j)); }
    assert.equal(row2.engrave.approved, before.approved); assert.equal(row2.engrave.state, before.state); assert.equal(row2.engrave.text, before.text); deq(Object.keys(row2.engrave.pieces).sort(), Object.keys(before.pieces).sort());
    // a back-only line: nothing to relink
    const one = mk(BASE.two, undefined), h3 = harness([one]); h3.ctx.ensureJobs(one); const c3 = Object.assign({}, h3.jobs.get(one.key)); delete c3.row;
    const one2 = mk(BASE.two, undefined), h4 = harness([one2]); const j4 = JSON.parse(JSON.stringify(c3)); j4.row = one2; h4.jobs.set(j4.key, j4); assert.equal(h4.ctx.relinkAll(), 0); assert.equal(j4.row, one2);
  });
  await t('6l EngraveLink.resolve: a piece with a front opens the face asked for; without a face, the one still to settle, else the back; a key ending ~front names the front; a back-only piece resolves as it did', async () => {
    const run = (jobsList, q, rows) => { const items = new Map(jobsList.map(j => [j.key, j])), win = { Engrave: { items: () => items }, Orders: { rows: () => rows || [] } }; vm.createContext(win); win.window = win; vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-engrave-link.js'), 'utf8'), win); return win.EngraveLink.resolve(q); };
    const row = mk(BASE.one, BOTH), h = harness([row]); const [fr, bk] = h.ctx.ensureJobs(row); const rid = row.order.receiptId, pid = row.poolIds[0];
    for (const j of [fr, bk]) { j.row.order = row.order; }
    fr.state = 'review'; bk.state = 'approved';
    assert.equal(run([fr, bk], { rid, poolId: pid }).job, fr, 'the face still to settle'); assert.equal(run([fr, bk], { rid, poolId: pid, face: 'back' }).job, bk); assert.equal(run([fr, bk], { rid, poolId: pid, face: 'front' }).job, fr);
    assert.equal(run([fr, bk], { rid, poolId: pid, face: 'back' }).tab, 'done'); assert.equal(run([fr, bk], { rid, poolId: pid, face: 'front' }).tab, 'place');
    fr.state = 'approved'; assert.equal(run([fr, bk], { rid, poolId: pid }).job, bk, 'both decided: the back'); assert.equal(run([fr, bk], { rid, key: row.key + '~front' }).job, fr); assert.equal(run([fr, bk], { rid, key: row.key, face: 'front' }).job, fr); assert.equal(run([fr, bk], { rid, key: row.key }).job, bk);
    assert.equal(run([bk], { rid, poolId: pid, face: 'front' }).job, null, 'no front job: the back is never put in its place');
    const one = mk(BASE.two, undefined), h2 = harness([one]), j2 = h2.ctx.ensureJobs(one)[0]; j2.row.order = one.order; j2.state = 'review'; assert.equal(run([j2], { rid: one.order.receiptId, poolId: one.poolIds[0] }).job, j2); assert.equal(run([j2], { rid: one.order.receiptId, key: one.key }).job, j2);
  });
  /* 7 ── looked at again */
  await t('7 wantsFront: a line read before the front was known is looked at again; an untouched back is read again from the setup, a touched one is not', async () => {
    const row = mk(BASE.one, undefined), h = harness([row]); const bk = h.ctx.ensureJobs(row)[0]; row.engrave = { needed: true, state: 'ready', approved: false }; bk.state = 'ready'; bk.text = 'old reading';
    assert.equal(h.ctx.wantsFront(row), false, 'nothing asks for a front');
    row.spec.engraving = BOTH; assert.equal(h.ctx.wantsFront(row), true); assert.equal(bk.state, 'classify', 'the untouched back is read again from the setup');
    const r2 = mk(BASE.two, undefined), h2 = harness([r2]); const b2 = h2.ctx.ensureJobs(r2)[0]; r2.engrave = { needed: true, state: 'ready', approved: false }; b2.state = 'ready'; b2.decision = { by: 'Paul' };
    r2.spec.engraving = BOTH; assert.equal(h2.ctx.wantsFront(r2), true); assert.equal(b2.state, 'ready', 'a back a person touched stays');
    h2.ctx.ensureJobs(r2); assert.equal(h2.ctx.wantsFront(r2), false, 'once the front job exists the line is not looked at again');
    const done = mk(BASE.one, BOTH, { state: 'committed' }), h3 = harness([done]); done.engrave = { needed: true, state: 'approved' }; assert.equal(h3.ctx.wantsFront(done), false, 'a finished line is left alone');
  });

  /* 8 ── the rest of the pipeline passes the face on (page source) */
  await t('8 approve, write, revoke, skip, open, sheet backs and the laser review all carry the face (source checks)', () => {
    const has = (re, why) => assert.ok(re.test(bridgeSrc), why);
    has(/const recKeyFor = \(poolId, x\) => \{ const F = FACES\(\); return F \? F\.recKey\(poolId, faceOf\(x\)\) : poolId; \};/, 'record keys by face');
    has(/face:\s*"front"/, 'a front record says so');
    has(/jobsOfPiece, pieceCountOf|pieceCountOf, jobForRecord|jobForRecord/, 'the new lookups are exported');
    for (const name of ['pieceJobsOf', 'faceJobs', 'jobsOfPiece', 'pieceCountOf', 'jobForRecord', 'faceOf', 'isFront', 'setupOf', 'applyEngraving', 'setFaces']) has(new RegExp('\\b' + name + '\\b[^\\n]*\\n?[^\\n]*'), 'export ' + name);
    const exp = bridgeSrc.slice(bridgeSrc.lastIndexOf('Engrave'), bridgeSrc.length);
    assert.ok(/applyEngraving/.test(bridgeSrc.slice(bridgeSrc.indexOf('const Engrave'))), 'the Engrave module exports applyEngraving');
    // no remaining raw "jobsOf(job.row).length" piece count
    assert.equal((bridgeSrc.match(/jobsOf\(job\.row\)\.length/g) || []).length, 0, 'piece counts use pieceCountOf');
  });

  /* 9 ── the real intake (ENGINTAKE's spec.engraving) through the real pipeline */
  await t('9 order 4195966039 as the intake reads it: two jobs, W on the front and 1. 27. 24 on the back, no paid reading', async () => {
    const order = { receiptId: '4195966039', updateTs: 0 }, line = { transactionId: 't1', listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm', quantity: 1, variations: [{ name: 'Metal Choice', value: 'Gold Filled' }, { name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font Choice', value: 'Ace' }], personalization: ['front - W', 'back - 1. 27. 24'], metalKey: 'gold', metalLabel: 'Gold Filled' };
    const spec = Orders.interpretLine(order, line, IntakeWorld.ctx()); assert.ok(spec.engraving, 'the intake read it'); assert.equal(spec.engraving.mode, 'both');
    const row = { key: '4195966039_t1', poolIds: ['4195966039_t1_1'], order: { receiptId: '4195966039' }, line: { transactionId: 't1', sku: 'MINI_0445', title: line.title }, spec, state: 'pooled' }, h = harness([row]); await h.ctx.classify(row);
    assert.equal(h.asked.length, 0); const fr = h.jobs.get(row.key + '~front'), bk = h.jobs.get(row.key); assert.equal(fr.text, 'W'); assert.equal(bk.text, '1. 27. 24'); assert.equal(fr.state, 'ready'); assert.equal(bk.state, 'ready'); assert.equal(row.engrave.approved, false);
  });
  await t('9b every line of the intake\'s offline order data with a face reading goes through the pipeline: nothing paid for a front, a line marked for a person only asks, nothing is approved', async () => {
    const ctx = IntakeWorld.ctx(); let faced = 0, asked = 0, backOld = 0, raised = 0, total = 0;
    for (const { key, order, line } of IntakeWorld.lines('words').concat(IntakeWorld.lines('plain'))) {
      total++; const spec = Orders.interpretLine(order, line, ctx); if (!spec || !spec.engraving) continue;
      const row = { key: 'K' + total, poolIds: ['K' + total + '_1'], order: { receiptId: order.receiptId }, line: { transactionId: line.transactionId, sku: line.sku, title: line.title }, spec, state: 'pooled' };
      const e = spec.engraving, h = harness([row], { reading: { engrave: true, text: 'ANNA', source: 'personalization', confidence: .95, questions: [], requests: { side: 'back' } } });
      await h.ctx.classify(row).catch(err => { throw new Error(key + ': ' + err.message); });
      const jobs = [...h.jobs.values()];
      if (!spec.engraveCandidate && !(e.needsPerson && e.mode !== 'none' && spec.designSku && !spec.noDesign && !spec.ownDesign)) { assert.ok(jobs.every(j => j.state === 'none'), key + ' stays plain'); continue; }
      if (!e.needsPerson && e.mode === 'back') { backOld++; assert.ok(jobs.length === 1 && !('face' in jobs[0]), key + ' back: the old job'); assert.ok(h.asked.length <= 1, key); continue; }
      if (spec.engraveRaised) raised++;
      assert.equal(h.asked.length, 0, key + ' no paid reading'); asked += 0; faced++;
      const want = e.mode === 'both' ? 2 : e.mode === 'none' ? 1 : 1; assert.equal(jobs.length, want, key + ' jobs for ' + e.mode);
      for (const j of jobs) { assert.ok(!['approved', 'written'].includes(j.state), key); assert.equal(j.approvedAt, null, key); assert.ok(e.needsPerson ? j.state === 'words' : j.state === 'ready', key + ' ' + j.state + ' for ' + e.mode + (e.needsPerson ? ' review' : '')); }
      assert.equal(row.engrave.approved, false, key);
    }
    assert.ok(total > 300 && faced > 0, 'swept ' + total + ' lines, ' + faced + ' faced, ' + backOld + ' back, ' + raised + ' raised'); console.log('      (swept ' + total + ' lines: ' + faced + ' with a front / a person to ask, ' + backOld + ' back-only readings through the old reader, ' + raised + ' raised from "no words")');
  });
  const failed = results.filter(([, e]) => e);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  if (failed.length) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
