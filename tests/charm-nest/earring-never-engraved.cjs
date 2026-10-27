// ZODNOBACK (Paul, 12 Oct 2026, a pair of zodiac huggie hoops whose buyer typed a sign word in the personalisation box; the page showed a "BACK ENGRAVING" card for it):
// "for some reason that was interpreted as an engraving, No earrings are ever allowed to have back engraving make that a rule forever."
// THE RULE (charm-nest-orders.js section 3d, one helper: earringNeverBackEngraves; the verdict is spec.engraveNever; the faces model reads it through CharmNestEngraveFaces.neverOf):
// no earring (huggie, hoop, stud, a zodiac pair) is ever engraved on either face, whatever the buyer typed, whichever option says "2 sides" or "front and back", and whatever a person picks later (a later pick is dropped quietly).
// The buyer's words stay a plain note (spec.personalization: the sign word is the sign resolver's to read). Necklaces, bracelets and keychains keep their back engraving exactly as they had it.
// Offline: no Firestore, no network, no paid reading (the stand-in reader FAILS the test when it is asked), nothing live. The bridge side runs the page's own code (charm-nest-bridge.js, sliced) over stand-ins.
//   1  the Capricorn order: no engraving, no candidate, no card, no seal, no gate; the sign word stays the buyer's note
//   2  every earring kind x every way of asking for an engraving: still none
//   3  a person's later pick: the faces model ignores it, the setup control draws nothing and stores nothing
//   4  necklaces, discs, bracelets and keychains are unchanged (2 sides still gets its back engraving); a necklace-and-earrings bundle is not vetoed by the rule
//   5  the readers of the verdict: the readiness decisions, the timeline's Engraved step, the sheet approval step and the set gate
//   6  the Engrave tab (bridge): the classifier, the words card, ready, send back, approve, a person's pick, and a job already open from an earlier reading; an approved or written job is never touched
//   7  the paid reader answers "no engraving" for an earring without a model call
//   node tests/charm-nest/earring-never-engraved.cjs
'use strict';
const path = require('path'), fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const transport = require.resolve(path.join(root, 'netlify/functions/_etsyMailAnthropic.js'));
require.cache[transport] = { id: transport, filename: transport, loaded: true, exports: {} };
const O = require(path.join(root, 'charm-nest-orders.js'));
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const Setup = require(path.join(root, 'charm-nest-engrave-setup.js'));
const Readiness = require(path.join(root, 'charm-nest-readiness.js'));
const SetEdit = require(path.join(root, 'charm-nest-set-edit.js'));
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));
const Agent = require(path.join(root, 'netlify/functions/_charmNestAgent.js'));
const PF = require('./pairs-fixtures.cjs');
const noNested = require('./_noNestedArrays.cjs');

let passed = 0, failed = 0;
const t = async (name, fn) => { try { await fn(); passed++; console.log('  ok  ' + name); } catch (e) { failed++; console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n      ')); } };
const V = (name, value) => ({ name, value });
const J = x => (x === undefined ? undefined : JSON.parse(JSON.stringify(x)));

// the library's entries for the designs used here (a huggie zodiac drawing, a studs drawing, the marigold disc, plain discs)
const ENT = {
  'HUGGIE HOOPS-ZODIAC': { sku: 'HUGGIE HOOPS-ZODIAC', members: 3, holes: 1, backKeepOut: [], widthPt: 23, heightPt: 28 },
  'ZODIAC_EARRING_1': { sku: 'ZODIAC_EARRING_1', members: 3, holes: 0, backKeepOut: [], widthPt: 14, heightPt: 14 },
  'STUD_STAR': { sku: 'STUD_STAR', members: 2, holes: 0, backKeepOut: [], widthPt: 12, heightPt: 12 },
  'HOOP_PLAIN': { sku: 'HOOP_PLAIN', members: 2, holes: 0, backKeepOut: [], widthPt: 20, heightPt: 20 },
  'BIRTHFLOWER_NECKLACE_DISC-9': { sku: 'BIRTHFLOWER_NECKLACE_DISC-9', members: 2, holes: 0, backKeepOut: [], widthPt: 48.354, heightPt: 56.287 },
  'INITIAL_DISC_4571': { sku: 'INITIAL_DISC_4571', members: 2, holes: 1, backKeepOut: [], widthPt: 36.516, heightPt: 36.516 },
  'CURB': { sku: 'CURB', members: 5, holes: 2, backKeepOut: [], widthPt: 58.03, heightPt: 20.4 },
  'KEYTAG_1': { sku: 'KEYTAG_1', members: 3, holes: 1, backKeepOut: [], widthPt: 30, heightPt: 30 },
};
const entryFor = s => ENT[String(s || '').toUpperCase()] || null;
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: () => '' }, extra || {});
const read = (line, c) => O.interpretLine({ receiptId: '4200256896', updateTs: 0 }, Object.assign({ transactionId: 't1', listingId: '1234', sku: '', title: '', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [] }, line), c || ctx());
const plain1 = r => typeof r === 'string' && !!r && !/\n/.test(r) && r.length <= 260;

/* ── the lines ── */
const HUGGIE_TITLE = 'Huggie Hoop Earrings with Astrological Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold | Handcrafted Horoscope Jewelry';
const CAPRICORN = { sku: 'HUGGIE HOOPS-ZODIAC', title: HUGGIE_TITLE, variations: [V('Metal Choice', '14k Gold Filled'), V('Hoop Size', '8mm'), V('Personalization', 'Capricorn')], personalization: ['Capricorn'], metalLabel: '14k Gold Filled' };
// every kind of earring line the shop sells, as a line an option or a note could try to engrave
const EAR = {
  huggie:  { sku: 'HUGGIE HOOPS-ZODIAC', title: HUGGIE_TITLE },
  hoop:    { sku: 'HOOP_PLAIN', title: 'Gold Hoop Earrings, Minimalist Hoops with Charm' },
  stud:    { sku: 'STUD_STAR', title: 'Stud Earrings, Personalized Charms' },
  zodiac:  { sku: 'ZODIAC_EARRING_1', title: 'Zodiac Earrings, Star Sign Stud Earrings, Astrology Gift' },
};
// every way a line or a person can ask for an engraving
const ASKS = [
  { name: 'the sign word alone', opt: [], pers: ['Capricorn'] },
  { name: 'option "2 sides"', opt: [V('Engrave', 'Gold - 2 sides')], pers: ['Capricorn'] },
  { name: 'option "front and back"', opt: [V('Engraving', 'Front&Back')], pers: ['Capricorn'] },
  { name: 'option "Back engraving", a name', opt: [V('Engraving', 'Back engraving')], pers: ['Anna'] },
  { name: 'labelled front and back words', opt: [V('Engraving', 'Front&Back')], pers: ['front: A\nback: B'] },
  { name: 'a back line alone', opt: [], pers: ['back: Anna'] },
  { name: 'a note with a date', opt: [V('Engraving', 'Back engraving')], pers: ['Anna\nOctober 26 2020'] },
];
const earLine = (kind, ask) => Object.assign({}, EAR[kind], { variations: [V('Metal Choice', 'Gold Filled')].concat(ask.opt), personalization: ask.pers });
const overrides = [
  { v: 1, mode: 'back', back: { text: 'Capricorn' }, by: 'Ana_M', at: 1 }, { v: 1, mode: 'front', front: { text: 'Capricorn' }, by: 'Ana_M', at: 1 },
  { v: 1, mode: 'both', front: { text: 'A' }, back: { text: 'B' }, by: 'Ana_M', at: 1 }, { v: 1, mode: 'none', by: 'Ana_M', at: 1 },
];

(async () => {
console.log('ZODNOBACK · no earring is ever engraved');

/* ═══ 1 · the Capricorn order ═══ */
await t('1a ORDER 4200256896 (zodiac huggie hoops, "Capricorn" typed in the personalisation box): no engraving, not a candidate, the verdict says why, the buyer\'s word stays', () => {
  const spec = read(CAPRICORN);
  assert.equal(spec.engraving, undefined, 'no reading of an engraving at all'); assert.equal(spec.engraveCandidate, false);
  assert.ok(spec.engraveNever, 'the verdict'); noNested(spec.engraveNever, 'engraveNever');
  assert.equal(spec.engraveNever.rule, 'earring'); assert.equal(spec.engraveNever.form, 'huggie'); assert.ok(plain1(spec.engraveNever.why) && /never engraved/i.test(spec.engraveNever.why), spec.engraveNever.why);
  assert.deepEqual(spec.engraveNever.note, ['Capricorn'], 'the buyer\'s word is kept as a note');
  assert.deepEqual(spec.personalization, ['Capricorn'], 'the sign resolver still reads the word where it always did');
  assert.deepEqual(spec.problems.filter(p => /engrav|sign|zodiac/i.test(p.kind)), [], 'no question about an engraving'); assert.equal(spec.problems.length, 0, JSON.stringify(spec.problems.map(p => p.kind)));
  assert.equal(spec.lineForm, 'huggie'); assert.equal(spec.designSku, 'HUGGIE HOOPS-ZODIAC', 'the design is the intake\'s other job, untouched');
});

await t('1b the faces model: mode none, no face, nothing for a person, the rule is the source; the helper and the verdict agree', () => {
  const spec = read(CAPRICORN), st = Faces.setupOf({ spec });
  assert.equal(st.mode, 'none'); assert.deepEqual(st.faces, []); assert.equal(st.front, null); assert.equal(st.back, null); assert.equal(st.needsPerson, false); assert.equal(st.source, 'rule'); assert.equal(st.never, true); assert.ok(plain1(st.why));
  assert.equal(Faces.neverOf({ spec }).rule, 'earring'); assert.equal(Faces.neverOf({ spec: { engraveCandidate: true } }), null); assert.equal(Faces.neverOf(null), null); assert.equal(Faces.neverOf({}), null);
  // the one helper: a raw line, a spec, and a row all answer the same
  const line = Object.assign({ transactionId: 't1', listingId: '1234', quantity: 1, variations: [] }, CAPRICORN);
  assert.equal(O.earringNeverBackEngraves({ line }).rule, 'earring'); assert.equal(O.earringNeverBackEngraves(spec).rule, 'earring');
  assert.equal(O.earringNeverBackEngraves(null), null);
  assert.equal(O.earringNeverBackEngraves({ line: { title: 'Birth Flower Necklace', sku: 'X', variations: [] } }), null);
});

/* ═══ 2 · the matrix ═══ */
await t('2 every earring kind x every way of asking (a sign, "2 sides", "front and back", labelled words, a date): no engraving, no candidate, the words stay the buyer\'s', () => {
  let n = 0;
  for (const kind of Object.keys(EAR)) for (const ask of ASKS) {
    const sp = read(earLine(kind, ask)), why = kind + ' / ' + ask.name;
    assert.equal(sp.engraving, undefined, why + ': engraving'); assert.equal(sp.engraveCandidate, false, why + ': candidate'); assert.ok(sp.engraveNever && sp.engraveNever.rule === 'earring', why + ': verdict');
    assert.deepEqual(sp.personalization, ask.pers, why + ': the words stay'); assert.deepEqual(sp.engraveNever.note, ask.pers.map(x => x.trim()).filter(Boolean).slice(0, 4).map(x => x.slice(0, 120)), why + ': the note');
    const st = Faces.setupOf({ spec: sp }); assert.deepEqual([st.mode, st.faces.length, st.needsPerson, st.never], ['none', 0, false, true], why + ': faces');
    for (const ov of overrides) { const o2 = Faces.setupOf({ spec: sp, engravingOverride: ov }); assert.deepEqual([o2.mode, o2.faces.length, o2.needsPerson], ['none', 0, false], why + ': a person\'s ' + ov.mode); o2.never || assert.fail(why + ': never'); }
    n++;
  }
  assert.equal(n, 28);
});

await t('2b a pair with quantity 2, and a line with no personalisation at all: the same', () => {
  for (const kind of Object.keys(EAR)) for (const q of [1, 2]) for (const pers of [[], ['']]) {
    const sp = read(Object.assign(earLine(kind, ASKS[2]), { quantity: q, personalization: pers }));
    assert.equal(sp.engraving, undefined, kind); assert.equal(sp.engraveCandidate, false); assert.equal(sp.engraveNever.rule, 'earring'); assert.deepEqual(sp.engraveNever.note, []);
  }
});

/* ═══ 3 · a person's later pick ═══ */
await t('3 a person\'s pick on an earring is dropped quietly: the setup control is not drawn, apply() stores nothing, touches nothing, asks nobody; a pick saved before the rule is kept as saved and not in force (there is no undo path any more)', async () => {
  const spec = read(CAPRICORN), line = { key: '4200256896_t1', spec, state: 'pulled', poolIds: ['p1', 'p2'] };
  assert.equal(Setup.read(line), null, 'no control');
  const touched = []; const env = new Proxy({}, { get: (_, k) => { touched.push(String(k)); return () => { throw new Error('env.' + String(k) + ' was called'); }; } });
  for (const patch of [{ mode: 'back', back: { text: 'Capricorn' } }, { mode: 'front', front: { text: 'A' } }, { mode: 'both', front: { text: 'A' }, back: { text: 'B' } }, { mode: 'none' }]) {
    const res = await Setup.apply(line, patch, env); assert.equal(res.ok, true); assert.equal(res.never, true); assert.deepEqual([res.removed, res.added, res.reopened, res.seeded], [[], [], [], []]); assert.ok(!res.cancelled && !res.pending && !res.message);
  }
  assert.deepEqual(touched, [], 'env (the signed-in name, the store, the jobs) was never touched'); assert.equal(line.engravingOverride, undefined, 'nothing stored');
  // a pick somebody saved before the rule is kept exactly as it was saved (never rewritten, not in force). The "Undo my edit" path is gone (ENGUNDO, point 8): apply() drops every call for an
  // earring, an old { undo: true } included, and touches nothing
  line.engravingOverride = overrides[0]; assert.equal(Faces.setupOf(line).mode, 'none', 'a pick saved earlier is not in force');
  const kept = JSON.stringify(line.engravingOverride);
  const res = await Setup.apply(line, null, env, { undo: true }); assert.equal(res.ok, true); assert.equal(res.never, true, 'an earring is never touched, an old undo option included');
  assert.equal(JSON.stringify(line.engravingOverride), kept, 'the old pick is kept exactly as it was saved'); assert.deepEqual(touched, [], 'env was never touched');
});

/* ═══ 4 · everything else is unchanged ═══ */
const TITLE_BF = 'Birth Flower Necklace, Gold Birth Flower Necklace, Silver Birth Flower Necklace, Gold Flower Necklace, Birth Month Necklace, Bridesmaid Gift';
const BF = (value, extra) => read(Object.assign({ sku: 'BIRTHFLOWER_NECKLACE_DISC-9', listingId: '870782383', title: TITLE_BF, personalization: ['Typewriter font\n\nGeorgia Andy Margot Jaarsma\nOctober 26 2020'], variations: [V('Birth Flower', 'Marigold'), V('Length/Engrave:1 side/2 sides?', value)] }, extra || {}));
await t('4a the Birth Flower necklace "2 sides" still gets its back engraving, as BFSIDES made it (words, source "option", a candidate, a real setup, no verdict)', () => {
  const sp = BF('Gold - 16" - 2 sides'), e = sp.engraving;
  assert.ok(e && e.mode === 'back' && e.source === 'option', JSON.stringify(e)); assert.deepEqual(e.back.lines, ['Georgia Andy Margot Jaarsma', 'October 26 2020']); assert.equal(sp.engraveCandidate, true); assert.equal(sp.engraveNever, undefined);
  const st = Faces.setupOf({ spec: sp }); assert.equal(st.mode, 'back'); assert.deepEqual(st.faces, ['back']); assert.equal(st.never, undefined); assert.equal(Faces.neverOf({ spec: sp }), null);
  assert.equal(BF('Gold - 16" - 1 side').engraving.mode, 'none', '"1 side" is still the art only'); assert.equal(BF('Gold - 16" - 1 side').engraveNever, undefined);
  // a person's pick on the necklace still wins (the rule is for earrings only)
  assert.equal(Faces.setupOf({ spec: sp, engravingOverride: overrides[2] }).mode, 'both'); assert.equal(Faces.setupOf({ spec: sp, engravingOverride: overrides[3] }).mode, 'none');
  assert.ok(Setup.read({ key: 'k', spec: sp, state: 'pulled' }), 'the necklace still has its setup control');
});

await t('4b a disc necklace with front and back, a name bracelet and a keychain: engraved as before, no verdict', () => {
  const disc = read({ sku: 'INITIAL_DISC_4571', listingId: '888', title: 'Initial Disc Necklace', variations: [V('Metal Choice', 'Gold'), V('Discs', '2 Disc'), V('Engraving', 'Front&Back')], personalization: ['front: A\nback: B'] });
  assert.equal(disc.engraving.mode, 'both'); assert.equal(disc.engraveCandidate, true); assert.equal(disc.engraveNever, undefined);
  const brace = read({ sku: 'CURB', listingId: '777', title: 'Personalized Name Bracelet, Engraved Bracelet', variations: [V('Metal Choice', 'Gold'), V('Engraving', 'Back engraving')], personalization: ['Anna'] });
  assert.ok(brace.engraving && brace.engraving.mode !== 'none', JSON.stringify(brace.engraving)); assert.equal(brace.engraveCandidate, true); assert.equal(brace.engraveNever, undefined);
  const key = read({ sku: 'KEYTAG_1', listingId: '666', title: 'Personalized Keychain, Engraved Key Ring Gift', variations: [V('Metal Choice', 'Gold'), V('Engraving', 'Back engraving')], personalization: ['Anna'] });
  assert.ok(key.engraving && key.engraving.mode !== 'none', JSON.stringify(key.engraving)); assert.equal(key.engraveCandidate, true); assert.equal(key.engraveNever, undefined);
  for (const sp of [disc, brace, key]) assert.notEqual(Faces.setupOf({ spec: sp }).mode, 'none');
});

await t('4c a bundle that sells a necklace AND earrings (the earring words are only part of it) is not vetoed; the necklace of the set keeps its engraving reading', () => {
  const sp = BF('Gold - 16" - 2 sides', { title: 'Birth Flower Necklace and Earrings Set, Gift Set for Her' });
  assert.equal(sp.engraveNever, undefined, JSON.stringify(sp.engraveNever)); assert.ok(sp.engraving && sp.engraving.mode === 'back');
  assert.equal(O.earringNeverBackEngraves({ line: { title: 'Earrings and Necklace Gift Set', sku: 'X', variations: [], quantity: 1 }, lineForm: { form: 'earring', by: 'its earring words' } }), null, 'earring words alone under a title that also names a necklace: not sure, so the rule stays out');
  assert.equal(O.earringNeverBackEngraves({ line: { title: 'Earrings and Necklace Gift Set', sku: 'X', variations: [], quantity: 1 }, lineForm: { form: 'earring', by: 'its option says earrings' } }).rule, 'earring', 'an option that says earrings is sure');
  assert.equal(O.earringNeverBackEngraves({ lineForm: 'earring' }).rule, 'earring', 'an earring form with no title to doubt it is an earring');
});

/* ═══ 5 · the readers of the verdict: readiness, the Engraved step, the sheet, the set gate ═══ */
await t('5 readiness says "no engraving, nothing to approve" for the earring and still waits for a necklace; the Engraved step is not shown; the sheet approval step and the set gate do not block on an earring', () => {
  const ear = { key: '4200256896_t1', poolIds: ['e1', 'e2'], state: 'pooled', spec: read(CAPRICORN) }, neck = { key: '4200828980_t1', poolIds: ['n1'], state: 'pooled', spec: BF('Gold - 16" - 2 sides') };
  const d = Readiness.decisions([ear, neck]);
  for (const id of ['e1', 'e2']) assert.deepEqual(d[id], { needed: false, state: 'none', approved: true }, 'the earring piece ' + id);
  assert.deepEqual(d.n1, { needed: true, state: 'unknown', approved: false }, 'the necklace still waits for its decision');
  const win = { document: { getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } } }; win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8'), win);
  assert.equal(win.OrderTimelineUI.engraveOf(ear), false, 'the Engraved step is not part of an earring\'s timeline'); assert.notEqual(win.OrderTimelineUI.engraveOf(neck), false);
  // a laser sheet of rose gold
  const sheet = (ids, engraving) => ({ id: 'sh', sheetId: 'sh', metal: 'rose', poolIds: ids, placedCount: ids.length, engraving });
  const earSheet = Readiness.sheet(sheet(['e1', 'e2'], { e1: d.e1, e2: d.e2 })), neckSheet = Readiness.sheet(sheet(['n1'], { n1: d.n1 }));
  assert.equal(earSheet.stages.approval, true, 'an earrings sheet has nothing to approve'); assert.equal(neckSheet.stages.approval, false);
  const cuts = () => true;
  assert.equal(SetEdit.engravingGap({ id: 'sh', metal: 'rose', laser: earSheet }, { cuts }), null, 'no gate for the earrings');
  // (Paul, 27 Oct 2026: a green-line sheet joins its set by its approval and the set waits for its engravings: no join gate for the necklace either; its sheet still waits at its approval stage)
  assert.equal(SetEdit.engravingGap({ id: 'sh', metal: 'rose', laser: neckSheet }, { cuts }), null, 'no join gate for the necklace (the set waits for it)');
});

/* ═══ 6 · the Engrave tab (bridge) ═══ */
const bridgeSrc = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const cut = (from, to) => { const a = bridgeSrc.indexOf(from), b = bridgeSrc.indexOf(to, a); assert.ok(a > 0 && b > a, 'slice ' + from.trim().slice(0, 40)); return bridgeSrc.slice(a, b); };
const SLICE_A = cut('  const SIDES = ', '  function setNone(job'), SLICE_B = cut('  function setNone(job', '  async function decideWords(job'), SLICE_C = cut("  /** The line's effective engraving setup", '  let previewRecovery');
const SLICE_D = cut('  function sendBack(job', "  /* ── the line's engraving SETUP"), SLICE_E = cut('  function reopenJob(job)', '  /** A person\'s words for an existing, undecided job'), SLICE_F = cut('  async function approveOnce(job, by, button) {', '  async function saveBacks(job)');
const W = PF.world({ orders: [{ rid: PF.rid(1), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }] }, { rid: PF.rid(2), lines: [{ n: 20, kind: 'single', on: 'sh-gf1' }] }] });
// a row of the fixture world carrying a spec the real intake made (its design, count and pair stay the fixture's, so the piece plan is the world's)
const rowOf = (ord, n, line, c) => {
  const o = W.orders.find(x => x.rid === PF.rid(ord)), l = o.lines.find(x => x.n === n), sp = O.interpretLine({ receiptId: o.rid, updateTs: 0 }, Object.assign({ transactionId: PF.tx(n), listingId: '1234', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [] }, line), c || ctx());
  return { key: PF.lineKey(o.rid, PF.tx(n)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: o.rid }, line: { transactionId: PF.tx(n), sku: l.sku, title: line.title || '' }, spec: Object.assign({}, sp, { designSku: l.sku, form: l.form, quantity: l.qty, pieceCount: l.pieces.length, pair: Object.assign({}, l.line.pair) }), state: 'pooled' };
};
function harness(rows) {
  const jobs = new Map(), asked = [], reviewAdds = [], reviewRemoves = [], events = [], pool = new Map(W.pool.map(p => [p.poolId, p])), byKey = new Map(rows.map(r => [r.key, r])), api = [];
  const win = { CharmNestEngraveSides: Sides, CharmNestPair: Pair, CharmNestEngraveFaces: Faces };
  const c = {
    window: win, items: () => jobs, B: { pool: { rows: pool }, orders: { byKey }, run: null }, fitTasks: new Map(), toast() {},
    Pool: { charmOf: id => { const r = pool.get(id); return r ? PF.charmOf(r.sku) : null; }, update: async () => { } }, Master: { entryFor: sku => PF.entryOf(sku) }, CNEngravingSeals: Seals, DECIDED: ['approved', 'written', 'skipped'],
    O: { FINISHED_LINE: new Set(['committed', 'gone', 'cancelled']), engravingNote: null }, S: { settings: { engraveConfidence: .65 } },
    render() {}, agent() {}, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, JSON, Math, RegExp, Error,
    Review: { add: x => reviewAdds.push(x), remove: k => reviewRemoves.push(k), render() {} }, RunCtl: { poke() {}, save: async () => {} }, Session: { schedule() {} }, Orders: { render() {}, rows: () => rows, lineRecord: r => [r.key, { key: r.key }] },
    TL: { line: (row, type, d) => events.push({ type, id: d && d.id }), hash: s => String(s).length },
    sheets: [], allSheets: () => [], refreshBacks() {}, backQueue: Promise.resolve(), api: async (fn, body) => { api.push(body); return {}; }, goes() {}, EG_TAB: x => x, showDecided() {}, CNListActivity: { touch() {} }, humanAct() {}, syncEditedBack: async () => {},
    faceWord: j => (j && j.face === 'front' ? 'front' : 'back'), employeeName: () => 'Paul', needEmployee: async () => 'Paul', charmFor: () => null, loadFonts: async () => {}, revokeBacks() {}, recoverApprovals() {}, saveBacks: async () => {},
    agentCall: async (mode, ask) => { asked.push(ask); throw new Error('a paid reading was asked for'); },
  };
  vm.createContext(c); for (const s of [SLICE_A, SLICE_B, SLICE_C, SLICE_D, SLICE_E, SLICE_F]) vm.runInContext(s, c);
  vm.runInContext('this.neverEngraves = neverEngraves; this.putAwayNever = putAwayNever; this.classifyOnce = classifyOnce; this.applyEngraving = applyEngraving; this.toWords = toWords; this.setReady = setReady; this.sendBack = sendBack; this.reopenJob = reopenJob; this.approveOnce = approveOnce; this.raiseCandidate = raiseCandidate; this.ensureJobs = ensureJobs; this.jobsOf = jobsOf;', c);
  return { ctx: c, jobs, asked, reviewAdds, reviewRemoves, events, api, byKey };
}
const earRow = () => rowOf(1, 10, Object.assign({}, EAR.huggie, { variations: [V('Metal Choice', '14k Gold Filled'), V('Engraving', 'Front&Back')], personalization: ['Capricorn'] }));
const neckRow = () => rowOf(2, 20, { sku: 'BIRTHFLOWER_NECKLACE_DISC-9', listingId: '870782383', title: TITLE_BF, personalization: ['Typewriter font\n\nGeorgia Andy Margot Jaarsma\nOctober 26 2020'], variations: [V('Birth Flower', 'Marigold'), V('Length/Engrave:1 side/2 sides?', 'Gold - 16" - 2 sides')] });
const open = (h, row, state, extra) => { const js = h.ctx.ensureJobs(row); for (const j of js) Object.assign(j, { state, text: 'Capricorn', lines: ['Capricorn'] }, extra || {}); for (const j of js) j.row.engrave = Object.assign({}, j.row.engrave, { needed: true, state, approved: state === 'approved', text: 'Capricorn' }); return js; };

await t('6a the fixture rows are what they claim: the pair row carries the verdict, the necklace row a back engraving', () => {
  const e = earRow(), n = neckRow(); assert.equal(e.spec.engraveNever.rule, 'earring'); assert.equal(e.poolIds.length, 2); assert.equal(n.spec.engraving.mode, 'back'); assert.equal(n.poolIds.length, 1);
  const h = harness([e, n]); assert.equal(h.ctx.neverEngraves(e).rule, 'earring'); assert.equal(h.ctx.neverEngraves(n), null); assert.equal(h.ctx.neverEngraves({ parentRow: e }).rule, 'earring', 'a side row / job row answers for its line'); assert.equal(h.ctx.neverEngraves(null), null);
});

await t('6b the classifier on an earring: every job goes to "no engraving", nothing is read or paid for, no card, no question; the necklace beside it is raised and read as before', async () => {
  const e = earRow(), n = neckRow(), h = harness([e, n]);
  await h.ctx.classifyOnce(e);
  const js = h.ctx.jobsOf(e); assert.ok(js.length >= 1); for (const j of js) assert.equal(j.state, 'none', j.key);
  assert.equal(e.engrave.state, 'none'); assert.equal(e.engrave.needed, false); assert.equal(e.engrave.approved, true); assert.deepEqual(h.asked, [], 'no paid reading'); assert.deepEqual(h.reviewAdds, [], 'no card'); assert.deepEqual(h.api, []);
  assert.equal(h.ctx.raiseCandidate(e), false, 'an earring is never raised as a line to engrave'); assert.notEqual(e.spec.engraveCandidate, true);
  assert.equal(h.ctx.raiseCandidate(n), true, 'the necklace is a candidate as before');
});

await t('6c a card or a job that is already open from an earlier reading (a words card, a placement to check, "ready") is put away as "no engraving" when the line is read again; its Review card goes', async () => {
  for (const state of ['words', 'review', 'ready', 'classify', 'blocked']) {
    const e = earRow(), h = harness([e]); const js = open(h, e, state);
    assert.equal(h.ctx.putAwayNever(e), true, state); for (const j of h.ctx.jobsOf(e)) { assert.equal(j.state, 'none', state); assert.ok(h.reviewRemoves.includes('eng:' + j.key), 'its card is taken off Review'); }
    assert.equal(h.ctx.putAwayNever(e), false, 'the second time: nothing left to do'); assert.deepEqual(h.reviewAdds, []); assert.ok(js.length >= 1);
  }
  // an open summary with no job of its own (a restored line)
  const e = earRow(), h = harness([e]); e.engrave = { needed: true, state: 'words', approved: false, text: 'Capricorn' };
  assert.equal(h.ctx.putAwayNever(e), true); assert.deepEqual([e.engrave.needed, e.engrave.state, e.engrave.approved], [false, 'none', true]);
  const n = neckRow(), h2 = harness([n]); open(h2, n, 'words'); assert.equal(h2.ctx.putAwayNever(n), false, 'a necklace\'s open card stays open'); for (const j of h2.ctx.jobsOf(n)) assert.equal(j.state, 'words');
});

await t('6d the words card, "ready", "send back", "ask again" and the approval can no longer open an engraving on an earring', async () => {
  const e = earRow(), h = harness([e]); const [j] = open(h, e, 'classify');
  h.ctx.toWords(j, 'say what to engrave'); assert.equal(j.state, 'none'); assert.deepEqual(h.reviewAdds, [], 'no words card');
  Object.assign(j, { state: 'words' }); await h.ctx.setReady(j); assert.equal(j.state, 'none', 'ready -> put away');
  Object.assign(j, { state: 'review' }); h.ctx.sendBack(j, 'x'); assert.equal(j.state, 'none', 'send back -> put away');
  Object.assign(j, { state: 'none' }); h.ctx.reopenJob(j); assert.equal(j.state, 'none', 'ask again does not reopen it'); assert.deepEqual(h.reviewAdds, []);
  Object.assign(j, { state: 'review' }); await h.ctx.approveOnce(j, 'Paul'); assert.equal(j.state, 'none', 'the approval button: put away, not approved'); assert.ok(!j.approvedAt && !j.approvedBy && !(j.row.engrave.seals || []).length, 'no approval seal');
  assert.deepEqual(h.asked, []);
  // the same calls on a necklace job are the page\'s own (the words card opens, ready goes ahead as far as the stand-in fonts allow)
  const n = neckRow(), h2 = harness([n]); const [nj] = open(h2, n, 'classify'); h2.ctx.toWords(nj, 'say what to engrave'); assert.equal(nj.state, 'words'); assert.equal(h2.reviewAdds.length, 1, 'the necklace gets its words card');
});

await t('6e a job already APPROVED or WRITTEN on an earring keeps everything: state, text, seals; the rule never rewrites a decision', async () => {
  for (const state of ['approved', 'written']) {
    const e = earRow(), h = harness([e]); const js = open(h, e, state, { approvedBy: 'Paul', approvedAt: 1760000000000 }); for (const j of js) j.row.engrave.seals = [{ id: 's1', how: 'engraveApproved', at: 1760000000000, by: 'Paul' }];
    const before = J(js.map(j => [j.state, j.text, j.approvedAt, j.row.engrave.seals]));
    assert.equal(h.ctx.putAwayNever(e), false, state); await h.ctx.classifyOnce(e); await h.ctx.setReady(js[0]); await h.ctx.approveOnce(js[0], 'Paul'); h.ctx.reopenJob(js[0]); await h.ctx.applyEngraving(e, { mode: 'none' }, 'Paul');
    assert.deepEqual(J(h.ctx.jobsOf(e).map(j => [j.state, j.text, j.approvedAt, j.row.engrave.seals])), before, state + ': untouched');
  }
});

await t('6f a person\'s pick on an earring (applyEngraving, the setup control): no job is made, nothing stored, nobody asked, no name needed; the same pick on a necklace goes through its own path', async () => {
  const e = earRow(), h = harness([e]);
  for (const patch of [{ mode: 'back', back: { text: 'Capricorn' } }, { mode: 'front', front: { text: 'A' } }, { mode: 'both', front: { text: 'A' }, back: { text: 'B' } }]) {
    const res = await h.ctx.applyEngraving(e, patch, undefined); assert.equal(res.ok, true); assert.equal(res.never, true); assert.deepEqual(J([res.added, res.dropped, res.skipped, res.kept]), [[], [], [], []]); assert.equal(res.setup.mode, 'none');
  }
  assert.equal(e.engravingOverride, undefined, 'nothing stored'); assert.equal(h.jobs.size, 0, 'no job made'); assert.deepEqual(h.asked, []); assert.deepEqual(h.reviewAdds, []);
  const n = neckRow(), h2 = harness([n]); h2.ctx.setReady = async j => j;   // (the fitting and the fonts are the page\'s; this test is about whether the rule swallows the pick)
  const res = await h2.ctx.applyEngraving(n, { mode: 'back' }, undefined); assert.notEqual(res.never, true, 'a necklace is not swallowed by the rule'); assert.equal(res.ok, true);
});

await t('6g the order window says it calmly: one "No engraving" fact that names the buyer\'s word as a note (only when the buyer wrote something); a necklace keeps its own fact', () => {
  const SL = cut('  function engReadingOf(r) {', '  /** The order\'s facts, gathered for OrderFacts');
  const c = { window: { CharmNestEngraveFaces: Faces }, O, esc: x => String(x).replace(/[&<>]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' })[ch]), tryDo: f => { try { return f(); } catch (_) { return undefined; } }, String, Number };
  vm.createContext(c); vm.runInContext(SL + '\nthis.engReadingOf = engReadingOf;', c);
  const say = c.engReadingOf(earRow()); assert.equal(say.chip, 'No engraving'); assert.match(say.html, /Earrings are never engraved/); assert.match(say.html, /Capricorn/); assert.match(say.html, /not an engraving/); assert.doesNotMatch(say.html, /approve|Engrave tab|\?/i, 'no question, no card to approve');
  const quiet = rowOf(1, 10, Object.assign({}, EAR.huggie, { variations: [V('Metal Choice', '14k Gold Filled')], personalization: [] })); assert.equal(c.engReadingOf(quiet), null, 'nothing written, nothing said');
  const nk = c.engReadingOf(neckRow()); assert.equal(nk.chip, 'Back engraving'); assert.doesNotMatch(nk.html, /never engraved/);
});

/* ═══ 7 · the paid reader ═══ */
await t('7 the server reader answers "no engraving" for an earring without a model call and without a key; a necklace is read as before', async () => {
  const had = process.env.ANTHROPIC_API_KEY; delete process.env.ANTHROPIC_API_KEY;
  try {
    for (const body of [{ form: 'huggie' }, { form: 'earring' }, { form: 'stud' }, { form: 'earrings' }, { engravable: false }, { engravable: false, form: 'necklace' }]) {
      assert.equal(Agent.neverEngraves(body), true, JSON.stringify(body)); const a = await Agent.run('engraveIntent', Object.assign({ line: { title: HUGGIE_TITLE }, personalization: ['Capricorn'] }, body));
      assert.equal(a.engrave, false); assert.equal(a.text, ''); assert.equal(a.never, true); assert.deepEqual(a.questions, []); assert.equal(a.model, null); assert.equal(a.usage, null); assert.equal(a.skipped, undefined);
    }
    for (const body of [{ form: 'necklace' }, {}, { engravable: true }, null]) assert.equal(Agent.neverEngraves(body), false, JSON.stringify(body));
    const n = await Agent.run('engraveIntent', { form: 'necklace', line: { title: TITLE_BF }, personalization: ['Anna'] }); assert.ok(n.skipped && /ANTHROPIC_API_KEY/.test(n.skipped), 'a necklace still goes the paid way (here: no key, so it is skipped)');
    const o = await Agent.run('name', { form: 'huggie' }); assert.ok(!o.never, 'only the engraving reader is short-circuited');
  } finally { if (had !== undefined) process.env.ANTHROPIC_API_KEY = had; }
});

console.log('\n' + passed + ' passed, ' + failed + ' failed');
process.exit(failed ? 1 : 0);
})();
