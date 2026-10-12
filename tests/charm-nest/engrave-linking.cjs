// ENGLINK (engraving UI, 12 Oct 2026; Paul point 6: "everything has to be linked to its engraving, whether it be front or back on the same charm or multiple charms in the same order"):
// an engraving belongs to ONE (piece, face): the job key <lineKey>[#slot][~front] (CharmNestEngraveFaces.keyOf). Offline: the real page code (charm-nest-bridge.js, sliced) runs in a vm over stand-ins,
// the real faces / setup / sides / seals / link / order-engraving modules run as they are. No browser, no network, no paid call (the stand-in reader FAILS the test when it is asked), nothing live.
//   1  one key: keyOf / pieceKeyOf / facesOfPiece name the same job from a job, a side row, a piece record, a key and a pool id; Front first; an earring has no Back slot; copies of a quantity-N line share one job
//   2  front and back of one charm: an edit of the front leaves the back's words, state and seals alone, and the reverse (line-level save and the bridge's applyEngraving)
//   3  a three-disc necklace: an edit of disc 2 (engravingOverride.pieces) never reaches disc 1 or 3; the old line-level override still reads for every disc; nothing is migrated
//   4  two identical lines of one order (and a quantity-2 line) never share an override, a draft key or a job key across lines
//   5  approval and seals are per face: approving the front does not approve the back; a front's record never makes a back seal; the line is approved only when every face is
//   6  the lookups: a recalled record goes to the line that holds its pool id (two lines of one SKU); EngraveLink and the order card never put a piece's other face in the place of the one asked for
//   7  a pair (legacy rows): the line's words reach both ears as before; an ear's own words stay on that ear; an earring never gets a Back
//   node tests/charm-nest/engrave-linking.cjs
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
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  ' + name); } catch (e) { results.push([name, e]); console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n      ')); } };

const W = F.world({ orders: [
  { rid: F.rid(1), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }] },
  { rid: F.rid(3), lines: [{ n: 30, kind: 'discs', discs: 3, on: 'sh-gf1' }] },
  { rid: F.rid(4), lines: [{ n: 40, kind: 'single', on: 'sh-gf1' }] },
  { rid: F.rid(5), lines: [{ n: 50, kind: 'single', qty: 2, on: 'sh-gf1' }, { n: 51, kind: 'single', on: 'sh-gf1' }, { n: 52, kind: 'single', on: 'sh-gf1' }] },
  { rid: F.rid(6), lines: [{ n: 60, kind: 'discs', discs: 3, qty: 2, on: 'sh-gf1' }] },
] });
const lineRow = (w, ord, n, extra) => {
  const o = w.orders.find(x => x.rid === F.rid(ord)), l = o.lines.find(x => x.n === n);
  return Object.assign({ key: F.lineKey(o.rid, F.tx(n)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: o.rid }, line: { transactionId: F.tx(n), sku: l.sku, title: 'Mini Tag' }, spec: { designSku: l.sku, form: l.form, quantity: l.qty, personalization: ['front - Name One', 'back - Name Two'], engraveCandidate: true }, state: 'written' }, extra || {});
};
const BASE = { pair: lineRow(W, 1, 10), discs: lineRow(W, 3, 30), one: lineRow(W, 4, 40), q2: lineRow(W, 5, 50), twinA: lineRow(W, 5, 51), twinB: lineRow(W, 5, 52), discs2: lineRow(W, 6, 60) };
const BOTH = { v: 1, mode: 'both', source: 'labelled', front: { text: 'Name One' }, back: { text: 'Name Two' }, textSource: 'personalization', needsPerson: false, confidence: 0.95, why: 'labelled' };
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
    O: { FINISHED_LINE: new Set(['committed', 'gone', 'cancelled']), engravingNote: null, fontOfRecord: () => 'f1', fontById: () => ({ name: 'F' }), DEFAULT_FONT: 'f1' }, loadFontSet: async () => null, S: { settings: { engraveConfidence: .65 } },
    render() {}, agent() {}, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, JSON, Math, RegExp, Error,
    Review: { add() {}, remove() {}, render() {} }, RunCtl: { poke() {}, save: async () => {} }, Session: { schedule() {} }, Orders: { render() {}, rows: () => rows, lineRecord: r => [r.key, { key: r.key }] },
    TL: { line: (row, type, d) => { if (d && d.once && events.some(e => e.type === type && e.id === d.id)) return; events.push({ row, type, id: d && d.id, text: d && d.text, data: d && d.data }); }, hash: s => String(s).length },
    sheets: o.sheets || [], allSheets: () => (o.sheets || []), refreshBacks() {}, backQueue: Promise.resolve(), toast() {}, api: async (fn, body) => { apiCalls.push(body); return {}; }, goes() {}, EG_TAB: x => x, showDecided() {}, CNListActivity: { touch() {} }, humanAct: (kind, d) => acts.push(Object.assign({ kind }, d)), syncEditedBack: async () => {},
    faceWord: j => (j && j.face === 'front' ? 'front' : 'back'),
    employeeName: () => 'Paul', needEmployee: async () => 'Paul', charmFor: () => null,
    setReady(job) { job.state = 'ready'; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'ready', text: job.text, approved: false }); return job; },
    toWords(job, why) { job.state = 'words'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'words', text: job.text, approved: false, reason: why }); return job; },
    setNone(job, why) { job.state = 'none'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: false, state: 'none', reason: why, approved: true }); return job; },
    ...(o.realSkip ? {} : { skip: async (job, by) => { skipped.push(job.key); job.state = 'skipped'; job.row.engrave = Object.assign({}, job.row.engrave, { needed: false, state: 'skipped', approved: true, decidedBy: by }); return job; } }),
    agentCall: async (mode, ask) => { asked.push(ask); if (o.reading === undefined) throw new Error('a paid reading was asked for'); return o.reading; },
  };
  vm.createContext(ctx); vm.runInContext(SLICE1, ctx); vm.runInContext(SLICE2, ctx); vm.runInContext(SLICE3, ctx); vm.runInContext(SLICE4, ctx); if (o.realSkip) vm.runInContext(SLICE5, ctx);
  if (o.outputs) { const up = o.outputs; Export.backIndexPdf = async a => { up.pdfs.push(a.title + (a.face ? ':' + a.face : '')); return new Uint8Array(1); }; win.CharmNestExport = Export; Object.assign(ctx, { S: Object.assign(ctx.S, { cloud: { ok: true } }), CharmNestAssets: { bytes: async () => null }, CharmNestExport: Export, uploadBytes: async (p2) => { up.paths.push(p2); return { path: p2, url: 'u:' + p2 }; }, TextEncoder, Uint8Array }); vm.runInContext(SLICE6, ctx); }
  vm.runInContext('this.pieceJobsOf = pieceJobsOf; this.pieceCountOf = pieceCountOf; this.faceJobs = faceJobs; this.jobOf = jobOf; this.jobsOfPiece = jobsOfPiece; this.pieceFields = pieceFields; this.setupOf = setupOf; this.untouched = untouched; this.jobsOf = jobsOf; this.fromRecall = fromRecall;', ctx);   // (the page\'s consts, reachable by the test)
  return { ctx, jobs, asked, skipped, events, apiCalls, acts, poolUpdates, byKey, keys: () => [...jobs.keys()] };
}
const decide = (j, state, text) => { j.state = state; if (text !== undefined) { j.text = text; j.lines = text.split('\n'); } j.approvedAt = Date.now(); j.approvedBy = 'Paul'; j.row.engrave = Object.assign({}, j.row.engrave, { needed: true, state, approved: true, text: j.text, approvedBy: 'Paul', approvedAt: j.approvedAt }); };
const shape = j => { const o = {}; for (const k of Object.keys(j).sort()) if (k !== 'row' && k !== 't' && typeof j[k] !== 'function') o[k] = j[k]; return JSON.stringify(o); };

const Setup = require(path.join(root, 'charm-nest-engrave-setup.js'));
const linkSrc = fs.readFileSync(path.join(root, 'charm-nest-engrave-link.js'), 'utf8');
const NEVER = { rule: 'earring', form: 'huggie', why: "Earrings are never engraved (the shop's rule)", note: [] };
const rid = ord => F.rid(ord);
const K = Faces.keyOf;
const slotsOf = h => line => [...new Set(h.ctx.jobsOf(line).map(j => j.slot || null))];
/** The setup editor's save path over the harness's jobs (what the bridge's setupEnv does, minus the page). `log` collects every job the save re-seeded. */
function env(h, log) {
  return { who: () => 'Paul', now: () => 7, store: (l, ov) => { if (ov) l.engravingOverride = ov; else delete l.engravingOverride; }, jobsOf: l => h.ctx.jobsOf(l), ensureJobs: l => h.ctx.ensureJobs(l), slots: l => slotsOf(h)(l),
    remove: j => { h.jobs.delete(j.key); }, skip: async j => { j.state = 'skipped'; }, setNone: j => { j.state = 'none'; }, reopen: j => j,
    seed: async (j, seed) => { log.push(j.key); j.text = seed.text; j.lines = seed.lines.slice(); j.fontAsked = seed.fontWord || null; }, changed() {} };
}
const words = (h, row) => Object.fromEntries(h.ctx.jobsOf(row).map(j => [j.key.slice(row.key.length) || '(line)', j.text]));
const PERSON_BACK = (text, extra) => Object.assign({ mode: 'back', back: { text }, by: 'Paul', at: 1 }, extra || {});   // (an OLD record: line-level only, no `pieces`)

(async () => {
  console.log('ENGLINK linking');

  /* 1 ── one key */
  await t('1 one key: keyOf / pieceKeyOf name the same (piece, face) from a job, a side row, a line row, a piece record, a key; the other face of a piece is keyOf(x, face)', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]), [fr, bk] = h.ctx.ensureJobs(row), base = row.key;
    assert.equal(K(bk), base); assert.equal(K(fr), base + '~front'); assert.equal(K(bk, 'front'), fr.key); assert.equal(K(fr, 'back'), bk.key); assert.equal(K(fr.row), fr.key); assert.equal(K(bk.row), bk.key);
    assert.equal(K(row, 'front'), fr.key); assert.equal(K(row), bk.key); assert.equal(K(base + '~front', 'back'), base); assert.equal(K(base, 'front'), base + '~front');
    for (const x of [fr, fr.row, base + '~front']) assert.equal(Faces.pieceKeyOf(x), base);
    const rec = Sides.pieceRecord(fr); assert.equal(K(rec), fr.key); assert.equal(rec.pieceKey, Faces.pieceKeyOf(fr));
    const jobs = h.ctx.jobsOf(row);
    for (const handle of [base, row.poolIds[0], fr, bk, fr.row, rec]) {
      const p = Faces.facesOfPiece(handle, jobs); deq(p.faces.map(f => f.face), ['front', 'back'], 'Front first'); deq(p.faces.map(f => f.key), [fr.key, bk.key]); assert.equal(p.faces[0].job, fr); assert.equal(p.faces[1].job, bk); assert.equal(p.never, false); assert.equal(p.pieceKey, base);
    }
    // a back-only charm: the front is a quiet empty slot with its key ready, nothing expected on it
    const only = mk(BASE.one, undefined, { engravingOverride: PERSON_BACK('Name One') }), h2 = harness([only]); h2.ctx.ensureJobs(only);
    const q = Faces.facesOfPiece(only.key, h2.ctx.jobsOf(only), Faces.setupOf(only)); deq(q.faces.map(f => [f.face, f.expected, !!f.job]), [['front', false, false], ['back', true, true]]); assert.equal(q.faces[0].key, only.key + '~front');
  });
  await t('1b ears, discs and copies: every (piece, face) has its own key; a quantity-N line is one job holding its copies; every copy\'s saved record has its own key', async () => {
    const pair = mk(BASE.pair, BOTH), hp = harness([pair]), pj = hp.ctx.ensureJobs(pair);
    assert.equal(new Set(pj.map(j => K(j))).size, 4, 'L/R x front/back'); deq(pj.map(j => K(j)), [pair.key + '#L~front', pair.key + '#L', pair.key + '#R~front', pair.key + '#R']);
    assert.notEqual(Faces.facesOfPiece(pair.poolIds[0], pj).pieceKey, Faces.facesOfPiece(pair.poolIds[1], pj).pieceKey);
    // a necklace of two units with three discs each: disc n of both necklaces is ONE job (the words are the buyer's, one personalisation for the line); its copies are the two pool ids
    const two = mk(BASE.discs2, { v: 1, mode: 'back', source: 'note', back: { text: 'Name One' }, textSource: 'labelled', confidence: .9 }); two.spec.pieceCount = 6; two.spec.pair = { kind: 'multi' };
    const hd = harness([two]), dj = hd.ctx.ensureJobs(two); deq(dj.map(j => j.key.slice(two.key.length)), ['#D1', '#D2', '#D3']); deq(dj.map(j => j.copies.map(c => +c.split('_').pop())), [[1, 4], [2, 5], [3, 6]]);
    for (const [i, want] of [[0, '#D1'], [1, '#D2'], [2, '#D3'], [3, '#D1'], [4, '#D2'], [5, '#D3']]) assert.equal(Faces.facesOfPiece(two.poolIds[i], dj).pieceKey, two.key + want, 'copy ' + (i + 1));
    const recKeys = two.poolIds.flatMap(id => ['front', 'back'].map(f => Faces.recKey(id, f))); assert.equal(new Set(recKeys).size, 12, 'each copy x face has its own saved-record key');
    // a plain quantity-2 line: one job, both copies
    const q2 = mk(BASE.q2, { v: 1, mode: 'back', source: 'note', back: { text: 'Name One' }, textSource: 'labelled', confidence: .9 }), hq = harness([q2]), qj = hq.ctx.ensureJobs(q2); assert.equal(qj.length, 1); deq(qj[0].copies, q2.poolIds);
    for (const id of q2.poolIds) assert.equal(Faces.facesOfPiece(id, qj).faces.find(f => f.face === 'back').job, qj[0]);
    // two lines of one SKU in one order: different keys, different jobs
    const a = mk(BASE.twinA, BOTH), b = mk(BASE.twinB, BOTH), ht = harness([a, b]); ht.ctx.ensureJobs(a); ht.ctx.ensureJobs(b);
    assert.notEqual(a.key, b.key); assert.equal(a.spec.designSku, b.spec.designSku); assert.equal(new Set([...ht.jobs.keys()]).size, 4); assert.notEqual(Faces.facesOfPiece(a.poolIds[0], [...ht.jobs.values()]).faces[1].job, Faces.facesOfPiece(b.poolIds[0], [...ht.jobs.values()]).faces[1].job);
  });
  await t('1c an earring never has a Back slot: no expected face, a back job it still holds is listed apart, a decided front stays the one slot', async () => {
    const ear = mk(BASE.pair, BOTH); ear.spec.engraveNever = NEVER; const h = harness([ear]); await h.ctx.classify(ear);
    const jobs = h.ctx.jobsOf(ear), p = Faces.facesOfPiece(ear.key + '#L', jobs, Faces.setupOf(ear));
    assert.equal(p.never, true); deq(p.faces, [], 'no slot at all while nothing is decided'); assert.ok(p.faces.every(f => f.face !== 'back'));
    assert.equal(Faces.setupOf(ear).faces.length, 0);
    const legacy = [{ key: ear.key + '#L~front', face: 'front', slot: 'L', copies: [ear.poolIds[0]], state: 'approved', row: ear }, { key: ear.key + '#L', slot: 'L', copies: [ear.poolIds[0]], state: 'approved', row: ear }];
    const q = Faces.facesOfPiece(ear.key + '#L', legacy); deq(q.faces.map(f => f.face), ['front']); assert.equal(q.faces[0].expected, false); deq(q.legacy.map(j => j.key), [ear.key + '#L']);
  });

  /* 2 ── front and back of one charm */
  await t('2 front and back of one charm: the front\'s edit leaves the back alone (words, state, seals), and the reverse; a decided face is never re-seeded', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]); await h.ctx.classify(row); const [fr, bk] = h.ctx.jobsOf(row); assert.equal(fr.text, 'Name One'); assert.equal(bk.text, 'Name Two');
    let log = []; let res = await Setup.apply(row, { front: { text: 'Name Three' } }, env(h, log)); assert.equal(res.ok, true); deq(log, [fr.key]); assert.equal(fr.text, 'Name Three'); assert.equal(bk.text, 'Name Two');
    log = []; res = await Setup.apply(row, { back: { text: 'Name Four' } }, env(h, log)); deq(log, [bk.key]); assert.equal(fr.text, 'Name Three'); assert.equal(bk.text, 'Name Four');
    decide(bk, 'approved'); const seals = Seals.list(bk).map(s => s.id);
    log = []; await Setup.apply(row, { front: { text: 'Name One' } }, env(h, log)); deq(log, [fr.key]); assert.equal(bk.state, 'approved'); assert.equal(bk.text, 'Name Four'); deq(Seals.list(bk).map(s => s.id), seals);
    log = []; await Setup.apply(row, { back: { text: 'Name Five' } }, env(h, log)); deq(log, [], 'an approved back is not re-seeded'); assert.equal(bk.text, 'Name Four');
    // the bridge\'s own path: a front-only patch reads again only the front
    const r2 = mk(BASE.one, BOTH), h2 = harness([r2]); await h2.ctx.classify(r2); const [f2, b2] = h2.ctx.jobsOf(r2); f2.fit = {}; b2.fit = {};
    await h2.ctx.applyEngraving(r2, { front: { text: 'Name Three' } }, 'Paul'); assert.equal(f2.text, 'Name Three'); assert.equal(b2.text, 'Name Two'); assert.equal(b2.state, 'ready'); assert.ok(b2.fit, 'the back kept its placement');
  });

  /* 3 ── a necklace of discs */
  await t('3 discs: the old line-level override reads for every disc; an edit of disc 2 (pieces[D2]) never reaches disc 1 or 3, on the editor\'s path and on the bridge\'s; nothing is migrated', async () => {
    const old = PERSON_BACK('Name One'), row = mk(BASE.discs, undefined, { engravingOverride: JSON.parse(JSON.stringify(old)) }), h = harness([row]); await h.ctx.classify(row);
    deq(words(h, row), { '#D1': 'Name One', '#D2': 'Name One', '#D3': 'Name One' });
    for (const s of ['D1', 'D2', 'D3']) { const st = Faces.setupOf(row, undefined, s); assert.equal(st.back.text, 'Name One', s + ' reads the line-level word'); assert.equal(st.pieceEdited, false); }
    deq(row.engravingOverride, old, 'reading an old record changes nothing in it');
    const log = []; const e = env(h, log);
    let res = await Setup.apply(row, { slot: 'D2', back: { text: 'Name Two', fontWord: 'Ace' } }, e); assert.equal(res.ok, true, JSON.stringify(res)); deq(log, [row.key + '#D2']);
    deq(words(h, row), { '#D1': 'Name One', '#D2': 'Name Two', '#D3': 'Name One' }); assert.equal(h.ctx.jobsOf(row)[1].fontAsked, 'Ace', 'the piece\'s font word reaches its job'); assert.ok(!h.ctx.jobsOf(row)[0].fontAsked, 'disc 1 got no font word');
    assert.equal(row.engravingOverride.back.text, 'Name One', 'the line-level words are untouched'); deq(Object.keys(row.engravingOverride.pieces), ['D2']); assert.equal(row.engravingOverride.pieces.D2.back.fontWord, 'Ace');
    // the view of one piece
    const jobs = h.ctx.jobsOf(row), v2 = Setup.read(row, { slot: 'D2', jobs }), v1 = Setup.read(row, { slot: 'D1', jobs });
    assert.equal(v2.rows[0].text, 'Name Two'); assert.equal(v2.rows[0].fontWord, 'Ace'); assert.equal(v1.rows[0].text, 'Name One'); assert.equal(v1.rows[0].fontWord, ''); assert.equal(v2.pieceEdited, true); assert.equal(v1.pieceEdited, false);
    assert.equal(v2.jobKeys.back, row.key + '#D2'); assert.equal(v2.jobKeys.front, row.key + '#D2~front'); assert.equal(v2.rows[0].key, row.key + '#D2'); assert.notEqual(v1.sig, v2.sig); assert.equal(v2.rows[0].perPiece, false);
    // a patch built from the piece's view carries the piece
    const d = { mode: v2.mode, faces: { back: { text: 'Name Three', font: 'Ace' }, front: { text: '', font: '' } } }; deq(Setup.patchOf(v2, d), { slot: 'D2', back: { text: 'Name Three', fontWord: 'Ace' } });
    log.length = 0; await Setup.apply(row, { slot: 'D3', back: { text: 'Name Three' } }, e); deq(log, [row.key + '#D3']); deq(words(h, row), { '#D1': 'Name One', '#D2': 'Name Two', '#D3': 'Name Three' });
    // a line-level save reaches only the pieces without words of their own
    log.length = 0; await Setup.apply(row, { back: { text: 'Name Five' } }, e); deq(log, [row.key + '#D1']); deq(words(h, row), { '#D1': 'Name Five', '#D2': 'Name Two', '#D3': 'Name Three' });
    // no words for a piece: it goes back to the line's
    log.length = 0; await Setup.apply(row, { slot: 'D2', back: { text: '' } }, e); deq(log, [row.key + '#D2']); assert.equal(words(h, row)['#D2'], 'Name Five'); assert.equal(row.engravingOverride.pieces.D2, undefined);
    // a decided disc is never re-seeded, and a sibling\'s edit never touches it
    decide(h.ctx.jobsOf(row)[0], 'approved'); log.length = 0; await Setup.apply(row, { slot: 'D1', back: { text: 'Name Six' } }, e); deq(log, [], 'approved: kept'); await Setup.apply(row, { slot: 'D2', back: { text: 'Name Seven' } }, e); assert.equal(h.ctx.jobsOf(row)[0].text, 'Name Five'); assert.equal(h.ctx.jobsOf(row)[1].text, 'Name Seven');
    // the bridge\'s path
    const r2 = mk(BASE.discs, undefined, { engravingOverride: PERSON_BACK('Name One') }), h2 = harness([r2]); await h2.ctx.classify(r2); const out = await h2.ctx.applyEngraving(r2, { slot: 'D2', back: { text: 'Name Two' } }, 'Paul');
    assert.equal(out.ok, true); deq(words(h2, r2), { '#D1': 'Name One', '#D2': 'Name Two', '#D3': 'Name One' }); assert.equal(r2.engravingOverride.back.text, 'Name One'); assert.equal(r2.engravingOverride.pieces.D2.back.text, 'Name Two');
    await h2.ctx.applyEngraving(r2, { slot: 'D3', front: { text: 'Name Three' }, mode: 'both' }, 'Paul'); assert.equal(h2.jobs.get(r2.key + '#D3~front').text, 'Name Three'); assert.equal(h2.jobs.get(r2.key + '#D1~front').text, '', 'a sibling disc\'s front got no words from disc 3\'s edit');
  });
  await t('3b buyer words for each disc and a person\'s word for one: the others keep the buyer\'s; a person\'s piece words clear that piece\'s "say which" but not its sibling\'s', async () => {
    const eng = { v: 1, mode: 'back', source: 'note', needsPerson: true, reviewKind: 'pieces', why: 'each disc has its own words', textSource: 'labelled', pieces: { D1: { back: { text: 'Name One' } }, D2: { back: { text: 'Name Two' } }, D3: { back: { text: 'Name Three' } } } };
    const row = mk(BASE.discs, eng), h = harness([row]); await h.ctx.classify(row); assert.ok(h.ctx.jobsOf(row).every(j => j.state === 'words'), 'the buyer\'s words are proposals for a person');
    await Setup.apply(row, { slot: 'D2', back: { text: 'Name Four' } }, env(h, [])); const st = s => Faces.setupOf(row, undefined, s);
    assert.equal(st('D2').back.text, 'Name Four'); assert.equal(st('D1').back.text, 'Name One'); assert.equal(st('D3').back.text, 'Name Three'); assert.equal(st('D2').needsPerson, false); assert.equal(st('D1').needsPerson, true);
    assert.equal(Faces.seedFor(Faces.setupOf(row), 'back', 'D2').text, 'Name Four'); assert.equal(Faces.seedFor(Faces.setupOf(row), 'back', 'D3').text, 'Name Three');
  });

  /* 4 ── lines never share */
  await t('4 two identical lines of one order never share an override, a draft key or a job; one quantity-2 line is ONE job for both copies (one personalisation), each copy with its own record key', async () => {
    const a = mk(BASE.twinA, undefined, { engravingOverride: PERSON_BACK('Name One') }), b = mk(BASE.twinB, undefined, { engravingOverride: PERSON_BACK('Name Two') }), h = harness([a, b]); await h.ctx.classify(a); await h.ctx.classify(b);
    assert.equal(h.jobs.get(a.key).text, 'Name One'); assert.equal(h.jobs.get(b.key).text, 'Name Two');
    const log = []; await Setup.apply(a, { back: { text: 'Name Three' } }, env(h, log)); deq(log, [a.key]); assert.equal(h.jobs.get(b.key).text, 'Name Two'); assert.equal(b.engravingOverride.back.text, 'Name Two'); assert.notEqual(a.engravingOverride, b.engravingOverride);
    const va = Setup.read(a, { jobs: h.ctx.jobsOf(a) }), vb = Setup.read(b, { jobs: h.ctx.jobsOf(b) }); assert.notEqual(va.key, vb.key); assert.notEqual(va.jobKeys.back, vb.jobKeys.back); assert.equal(va.jobKeys.back, a.key); assert.equal(vb.jobKeys.front, b.key + '~front');
    const q = mk(BASE.q2, undefined, { engravingOverride: PERSON_BACK('Name One') }), hq = harness([q]); await hq.ctx.classify(q); assert.equal(hq.ctx.jobsOf(q).length, 1); deq(hq.ctx.jobsOf(q)[0].copies, q.poolIds);
    assert.notEqual(Faces.recKey(q.poolIds[0], 'back'), Faces.recKey(q.poolIds[1], 'back'));
  });

  /* 5 ── approval and seals are per face */
  await t('5 approving the front does not approve the back: seals stay per face, the line is approved only when every face is, and a front\'s record never makes a back seal', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]); await h.ctx.classify(row); const [fr, bk] = h.ctx.jobsOf(row), ctxS = { mergeSeals: (...r) => Seals.merge(...r) }, sum = () => Sides.summary(ctxS, undefined, h.ctx.jobsOf(row));
    deq(Seals.list(fr), []); deq(Seals.list(bk), []);
    decide(fr, 'approved');
    assert.equal(fr.state, 'approved'); assert.notEqual(bk.state, 'approved', 'the back is still to settle'); assert.equal(bk.row.engrave.approved, false);
    const sf = Seals.list(fr); assert.ok(sf.length >= 1, 'the front has its seal'); assert.ok(sf.every(s => s.face === 'front' && /:front:/.test(s.id)), 'a front\'s seals say front: ' + JSON.stringify(sf));
    deq(Seals.list(bk), [], 'the back has no seal because the front was approved');
    let s = sum(); assert.equal(s.approved, false, 'the line is not approved while the back is open'); assert.equal(s.pieces[row.poolIds[0]].approved, false); assert.equal(s.pieces[row.poolIds[0]].faces.front.approved, true); assert.equal(s.pieces[row.poolIds[0]].faces.back.approved, false);
    assert.ok(s.seals.length >= 1 && s.seals.every(x => x.face === 'front'), 'the line\'s seal list holds the front\'s seal only, as a front seal: ' + JSON.stringify(s.seals));
    // the same instant, the same person: two faces are two seals
    const a = Seals.add(fr, 'engraveApproved', 'Paul', 5), b = Seals.add(bk, 'engraveApproved', 'Paul', 5); assert.notEqual(a.id, b.id); assert.equal(a.face, 'front'); assert.equal(b.face, undefined);
    assert.equal(Seals.merge(fr, bk).filter(x => x.at === 5).length, 2, 'merge never folds a front into a back'); assert.equal(Seals.merge(bk, bk).filter(x => x.at === 5).length, 1, 'the same face merges with itself');
    assert.ok(Seals.list(fr).every(x => x.face === 'front') && Seals.list(bk).every(x => x.face !== 'front'), 'each face lists its own');
    // the saved record of a front is a front record
    const rec = Seals.record({ job: fr, kind: 'approved', at: 9, by: 'Paul' }); assert.equal(rec.key, fr.key); assert.ok(rec.engravingSeals.every(x => x.face === 'front'), JSON.stringify(rec.engravingSeals));
    // events: a face's own row reads only its own face's approvals
    const ev = [{ type: 'engraveApproved', id: 'e1', at: 3, by: 'Paul', lineKey: row.key, orderId: rid(4), data: { face: 'front', poolId: row.poolIds[0] } }, { type: 'engraveApproved', id: 'e2', at: 4, by: 'Paul', lineKey: row.key, orderId: rid(4), data: { poolId: row.poolIds[0] } }];
    deq(Seals.fromEvents(ev, fr.row).map(x => x.id), ['e1']); deq(Seals.fromEvents(ev, bk.row).map(x => x.id), ['e2']);
    // the back is approved last: now the line is
    decide(bk, 'approved'); s = sum(); assert.equal(s.approved, true); assert.equal(s.pieces[row.poolIds[0]].approved, true); assert.ok(s.seals.some(x => x.face === 'front') && s.seals.some(x => x.face !== 'front'), 'both faces\' seals are in the line');
    // and the reverse: only the back approved, the front still open
    const r2 = mk(BASE.one, BOTH), h2 = harness([r2]); await h2.ctx.classify(r2); const [f2, b2] = h2.ctx.jobsOf(r2); decide(b2, 'approved');
    assert.notEqual(f2.state, 'approved'); deq(Seals.list(f2), []); assert.ok(Seals.list(b2).length >= 1 && Seals.list(b2).every(x => x.face !== 'front')); assert.equal(Sides.summary(ctxS, undefined, h2.ctx.jobsOf(r2)).approved, false);
    // a piece with two discs: approving disc 1's back approves nothing of disc 2
    const d = mk(BASE.discs, undefined, { engravingOverride: PERSON_BACK('Name One') }), hd = harness([d]); await hd.ctx.classify(d); const dj = hd.ctx.jobsOf(d); decide(dj[0], 'approved');
    deq(dj.map(j => j.state === 'approved'), [true, false, false]); deq(dj.slice(1).map(j => Seals.list(j).length), [0, 0]); assert.equal(Sides.summary(ctxS, undefined, dj).approved, false);
  });

  /* 6 ── the lookups */
  await t('6 a recalled back goes to the line that holds its pool id (two lines of one SKU in one order); a copy of one line is never read as the other', async () => {
    const a = mk(BASE.twinA, undefined, { engravingOverride: PERSON_BACK('Name One') }), b = mk(BASE.twinB, undefined, { engravingOverride: PERSON_BACK('Name Two') });
    const rec = (row, text, extra) => Object.assign({ order: rid(5), sku: row.spec.designSku, poolId: row.poolIds[0], transactionId: row.line.transactionId, text, lines: [text], approvedBy: 'Paul', approvedAt: 4 }, extra || {});
    // the record names b's piece but carries a's transaction id (a stale or merged record): the pool id wins
    const sheets = [{ recalled: true, backPool: [rec(b, 'Name Four', { transactionId: a.line.transactionId })] }], h = harness([a, b], { sheets }); h.ctx.fromRecall();
    assert.equal(h.jobs.has(a.key), false, 'a got nothing'); assert.equal(h.jobs.get(b.key).text, 'Name Four'); assert.equal(h.jobs.get(b.key).state, 'written');
    // no pool id: the transaction id names the line; no pool id and no transaction id with two lines of the SKU: no line is guessed
    const h2 = harness([a, b], { sheets: [{ recalled: true, backPool: [rec(a, 'Name Five', { poolId: undefined })] }] }); h2.ctx.fromRecall(); assert.equal(h2.jobs.get(a.key).text, 'Name Five'); assert.equal(h2.jobs.has(b.key), false);
    const h3 = harness([a, b], { sheets: [{ recalled: true, backPool: [rec(a, 'Name Six', { poolId: undefined, transactionId: undefined })] }] }); h3.ctx.fromRecall(); assert.equal(h3.jobs.size, 0, 'two lines fit: nothing is guessed');
    // a recalled front is the front of the piece it names
    const h4 = harness([a, b], { sheets: [{ recalled: true, backPool: [rec(b, 'Name Seven', { face: 'front' }), rec(b, 'Name Eight')] }] }); h4.ctx.fromRecall();
    assert.equal(h4.jobs.get(b.key + '~front').text, 'Name Seven'); assert.equal(h4.jobs.get(b.key).text, 'Name Eight'); assert.equal(h4.jobs.has(a.key) || h4.jobs.has(a.key + '~front'), false);
  });
  await t('6b EngraveLink and the order card ask for a (piece, face) and are given that or nothing: a back card is never shown the piece\'s front, and a face with no job opens the piece\'s own job, never another piece\'s', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]); await h.ctx.classify(row); const [fr, bk] = h.ctx.jobsOf(row);
    const win = { Engrave: { items: () => h.jobs }, CharmNestEngraveFaces: Faces }, run = new Function('window', 'document', linkSrc); run(win, {}); const L = win.EngraveLink; assert.ok(L && L.resolve);
    const rs = x => (L.resolve(Object.assign({ rid: rid(4) }, x)).job || {}).key;
    assert.equal(rs({ key: row.key }), bk.key); assert.equal(rs({ key: row.key, face: 'front' }), fr.key); assert.equal(rs({ piece: fr.key }), fr.key); assert.equal(rs({ piece: fr.key, face: 'back' }), bk.key);
    assert.equal(rs({ poolId: row.poolIds[0], face: 'front' }), fr.key); assert.equal(rs({ poolId: row.poolIds[0], face: 'back' }), bk.key);
    // the front is the one still to settle: a request that names the back does not land on it
    fr.state = 'review'; bk.state = 'approved'; assert.equal(rs({ key: row.key }), bk.key); assert.equal(rs({ poolId: row.poolIds[0], piece: bk.key }), bk.key);
    // a back-only charm asked for its front (no front job yet): the piece's own job opens, the same piece (the existing rule), never another piece's
    const only = mk(BASE.twinA, undefined, { engravingOverride: PERSON_BACK('Name One') }), h2 = harness([only]); await h2.ctx.classify(only); const win2 = { Engrave: { items: () => h2.jobs }, CharmNestEngraveFaces: Faces }; run(win2, {});
    assert.equal((win2.EngraveLink.resolve({ rid: rid(5), key: only.key, face: 'front' }).job || {}).key, only.key); assert.equal((win2.EngraveLink.resolve({ rid: rid(5), key: only.key, face: 'back' }).job || {}).key, only.key);
    // the order card: a key that names a job is that job, the face a key names is the face shown
    const keep = { Engrave: globalThis.Engrave, CNEngravingSeals: globalThis.CNEngravingSeals, CharmNestEngraveFaces: globalThis.CharmNestEngraveFaces, Orders: globalThis.Orders };
    try {
      globalThis.Engrave = { items: () => h.jobs, jobOf: (r, id) => (id ? [...h.jobs.values()].find(j => (j.copies || []).includes(id)) : h.jobs.get(r.key)) || null, jobsOf: r => [...h.jobs.values()] }; globalThis.CNEngravingSeals = Seals; globalThis.CharmNestEngraveFaces = Faces; globalThis.Orders = { rows: () => [row] };
      delete require.cache[require.resolve(path.join(root, 'charm-nest-order-engraving.js'))]; require(path.join(root, 'charm-nest-order-engraving.js')); const OE = globalThis.OrderEngraving; assert.ok(OE && OE.engOf);
      const eng = key => (OE.engOf({ row, key, poolId: row.poolIds[0] }).job || {}).key;
      assert.equal(eng(bk.key), bk.key, 'a back card keeps the back though the front is the one to settle'); assert.equal(eng(fr.key), fr.key); assert.equal(eng(undefined), bk.key.length ? eng(undefined) : '');
      const kinds = key => OE.engOf({ row, key, poolId: row.poolIds[0] }).eng.kind; assert.equal(kinds(bk.key), 'approved'); assert.equal(kinds(fr.key), 'approve', 'each card says its own face\'s state');
    } finally { for (const k of Object.keys(keep)) { if (keep[k] === undefined) delete globalThis[k]; else globalThis[k] = keep[k]; } delete globalThis.OrderEngraving; }
  });
  await t('6c the words asked for are about the face decided: a front\'s decision records the front side, a back\'s the back side', async () => {
    const row = mk(BASE.one, BOTH), h = harness([row]); await h.ctx.classify(row); const [fr, bk] = h.ctx.jobsOf(row);
    // the page's own decideWords, run over the stand-ins (its few helpers that are not in the sliced code are stubbed: nothing is fitted, written or sent)
    Object.assign(h.ctx, { wordsEvent() {}, sheetFor: () => null, fitJob: async () => {}, DesignLink: { call: async () => ({}) } }); vm.runInContext(cut('  async function decideWords(', "  /** The line's effective engraving setup: the intake's reading"), h.ctx);
    const dw = vm.runInContext('decideWords', h.ctx); assert.equal(typeof dw, 'function', 'decideWords is in the page code');
    await dw(fr, { text: 'Name Nine', by: 'Paul' }); assert.equal(fr.requests.side, 'front'); assert.equal(fr.text, 'Name Nine'); assert.equal(bk.text, 'Name Two', 'the back keeps its words');
    await dw(bk, { text: 'Name Ten', by: 'Paul' }); assert.equal(bk.requests.side, 'back'); assert.equal(fr.text, 'Name Nine', 'the front keeps its words');
  });

  /* 7 ── pairs are legacy rows */
  await t('7 a legacy pair row: the line\'s words reach both ears as before; an ear\'s own words stay on that ear; an earring that is never engraved has no face, no store and no back', async () => {
    const pair = mk(BASE.pair, undefined, { engravingOverride: PERSON_BACK('Name One') }), h = harness([pair]); await h.ctx.classify(pair);
    deq(words(h, pair), { '#L': 'Name One', '#R': 'Name One' }); let log = [];
    await Setup.apply(pair, { slot: 'L', back: { text: 'Name Two' } }, env(h, log)); deq(log, [pair.key + '#L']); deq(words(h, pair), { '#L': 'Name Two', '#R': 'Name One' });
    log = []; await Setup.apply(pair, { back: { text: 'Name Five' } }, env(h, log)); deq(log, [pair.key + '#R']); deq(words(h, pair), { '#L': 'Name Two', '#R': 'Name Five' });
    // the ears keep their own records and keys
    const [l, r] = [h.jobs.get(pair.key + '#L'), h.jobs.get(pair.key + '#R')]; assert.notEqual(l.copies[0], r.copies[0]); assert.notEqual(Faces.recKey(l.copies[0], 'back'), Faces.recKey(r.copies[0], 'back'));
    // never engraved: no faces, nothing stored, no back job is made
    const ear = mk(BASE.pair, BOTH); ear.spec.engraveNever = NEVER; const he = harness([ear]); await he.ctx.classify(ear); const log2 = [];
    assert.equal(Faces.neverOf(ear) ? true : false, true); deq(Faces.setupOf(ear, { mode: 'both', front: { text: 'Name One' }, back: { text: 'Name Two' } }).faces, []); assert.equal(Setup.read(ear), null);
    const res = await Setup.apply(ear, { back: { text: 'Name Three' } }, env(he, log2)); assert.equal(res.never, true); deq(log2, []); assert.equal(ear.engravingOverride, undefined, 'nothing was stored');
    const res2 = await Setup.apply(ear, { slot: 'L', back: { text: 'Name Three' } }, env(he, log2)); assert.equal(res2.never, true); assert.equal(ear.engravingOverride, undefined);
    assert.ok(he.ctx.jobsOf(ear).every(j => j.state === 'none' || j.state === 'skipped'), 'no job of an earring waits for a back: ' + he.ctx.jobsOf(ear).map(j => j.key + ':' + j.state));
  });

  const bad = results.filter(r => r[1]);
  console.log(results.length - bad.length + ' of ' + results.length + ' passed');
  process.exit(bad.length ? 1 : 0);
})();
