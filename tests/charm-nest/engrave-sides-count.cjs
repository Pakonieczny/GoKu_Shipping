// BFSIDES (Paul, 12 Oct 2026, order 4200828980, Birth Flower necklace "Gold - 16\" - 2 sides", personalisation "Typewriter font / Georgia Andy Margot Jaarsma / October 26 2020"):
// "if 2 sided is chosen, that means there's also backside engraving option."
// A side count in an option VALUE ("1 side", "2 sides", "double sided", with metal and length words around it) on a design that carries its own front art (a flower, a zodiac sign, a picture)
// means: 2 = the buyer's words on the BACK (the front is the art), 1 = nothing of the buyer's is engraved. On a design with no art of its own (a bar, a tag, a plain disc) 2 sides stays both faces of buyer text.
// Offline: no Firestore, no network, no paid reading. Intake (charm-nest-orders.js) and the faces model (charm-nest-engrave-faces.js); the bridge side is in engrave-faces-bridge.cjs (section 10).
//   node tests/charm-nest/engrave-sides-count.cjs
'use strict';
const assert = require('assert/strict'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const noNested = require('./_noNestedArrays.cjs');

let passed = 0, failed = 0; const t = async (name, fn) => { try { await fn(); passed++; console.log('  ok  ' + name); } catch (e) { failed++; console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n      ')); } };
const V = (name, value) => ({ name, value });
// the library's real index entries (backups/catalog-1009 index-all.json, 9 Oct 2026): members, holes, size
const ENT = {
  'BIRTHFLOWER_NECKLACE_DISC-9': { sku: 'BIRTHFLOWER_NECKLACE_DISC-9', members: 2, holes: 0, backKeepOut: [], widthPt: 48.354, heightPt: 56.287 },       // the marigold: outline + one hatched flower
  'ZODIAC_NECKLACE_DISC-6': { sku: 'ZODIAC_NECKLACE_DISC-6', members: 3, holes: 1, backKeepOut: [], widthPt: 29.6, heightPt: 35 },                       // a sign on a round disc
  'ZODIAC_NECKLACE_DISC-4': { sku: 'ZODIAC_NECKLACE_DISC-4', members: 4, holes: 1, backKeepOut: [], widthPt: 29.6, heightPt: 49.7 },                     // the same kind with its loop drawn: 1.68 times as tall as wide, counted as a carrier of words (a miss: read as before)
  'BIRTHFLOWER_NECKLACE_OVAL-0': { sku: 'BIRTHFLOWER_NECKLACE_OVAL-0', members: 4, holes: 0, backKeepOut: [], widthPt: 37, heightPt: 59.7 },          // also long: a miss
  'INITIAL_DISC_4571': { sku: 'INITIAL_DISC_4571', members: 2, holes: 1, backKeepOut: [], widthPt: 36.516, heightPt: 36.516 },                       // a plain disc: outline + hole, no art
  'CURB': { sku: 'CURB', members: 5, holes: 2, backKeepOut: [], widthPt: 58.03, heightPt: 20.4 },                                                     // a bar: two cut strokes, long
  'MINI_0445': { sku: 'MINI_0445', members: 4, holes: 1, backKeepOut: [], widthPt: 16.894, heightPt: 28.036 },                                       // a tag with an engraved border, long
  'MEMORIAL_0437': { sku: 'MEMORIAL_0437', members: 5, holes: 4, backKeepOut: [], widthPt: 47.854, heightPt: 47.854 },                                       // the handwriting listing's design: counts no art
  'FLAGGED_ART': { sku: 'FLAGGED_ART', members: 1, holes: 0, backKeepOut: [], frontArt: true, widthPt: 30, heightPt: 30 },
  'FLAGGED_PLAIN': { sku: 'FLAGGED_PLAIN', members: 9, holes: 0, backKeepOut: [], frontArt: false, widthPt: 30, heightPt: 30 },
};
const entryFor = s => ENT[String(s || '').toUpperCase()] || null;
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: () => '' }, extra || {});
const TITLE = 'Birth Flower Necklace, Gold Birth Flower Necklace, Silver Birth Flower Necklace, Gold Flower Necklace, Birth Month Necklace, Bridesmaid Gift';
const OPT = 'Length/Engrave:1 side/2 sides?';
const PERS = ['Typewriter font\n\nGeorgia Andy Margot Jaarsma\nOctober 26 2020'];
const read = (extra, c) => O.interpretLine({ receiptId: '4200828980', updateTs: 0 }, Object.assign({ transactionId: 't1', listingId: '870782383', sku: 'BIRTHFLOWER_NECKLACE_DISC-9', title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: PERS, variations: [V('Birth Flower', 'Marigold'), V(OPT, 'Gold - 16" - 2 sides')] }, extra || {}), c || ctx());
const withOpt = (value, extra, c) => read(Object.assign({ variations: [V('Birth Flower', 'Marigold'), V(OPT, value)] }, extra || {}), c);
const plain1 = r => typeof r === 'string' && !!r && !/\n/.test(r) && r.length <= 220;

(async () => {
console.log('BFSIDES · side counts in option values');

await t('1 the side count of a value: every spelling, with metal and length words around it, any case, any option; face words and look-alikes are no count', () => {
  for (const v of ['Gold - 16" - 2 sides', 'Silver- 16"- 2 sides', 'Rose- 18"- 2 sides', 'GOLD·DOUBLE SIDED', 'ROSE GOLD·DBL SIDED', 'two sided', '2-sided', '2 sided', 'Double-Sided', 'double sided', 'both sides', 'Both Faces', 'dual sided', 'Gold - 16&quot; - 2 sides', '2SIDES', 'Gold 17" 2 SIDES ($39.50)']) assert.equal(O.sidesOfValue(v), 2, v);
  for (const v of ['Gold - 16" - 1 side', 'Silver - 17"- 1 side', 'Rose- 18"- 1 side', 'one-sided', 'One Sided', 'single sided', 'single side', '1 sided', '1-side', 'Gold 16" 1 SIDE ($33.50)']) assert.equal(O.sidesOfValue(v), 1, v);
  for (const v of ['', 'Gold - 16"', 'Gold Filled', 'Double Heart', 'Double Disc', 'Gold Double Hoop', '3 sides', '12 sides', 'Tag1 (front+back)', 'Tag1 (front+back) 2 sided', 'Front & Back 2 sides', 'Gold - FRONT & BACK', 'Writing-FRONT', 'No engraving 1 side', 'both charms', 'one side of each']) assert.equal(O.sidesOfValue(v), v === 'one side of each' ? 1 : 0, v);
});

await t('2 which designs carry their own front art: the index counts (members - outline - holes - keep-outs), a long design is a carrier of words, an explicit frontArt wins, an unknown entry says null', () => {
  const f = k => O.frontArtOf(ENT[k]);
  assert.equal(f('BIRTHFLOWER_NECKLACE_DISC-9').front, true); assert.equal(f('BIRTHFLOWER_NECKLACE_DISC-9').art, 1); assert.equal(f('ZODIAC_NECKLACE_DISC-6').front, true); assert.equal(f('ZODIAC_NECKLACE_DISC-4').front, false, 'a miss, safe'); assert.equal(f('BIRTHFLOWER_NECKLACE_OVAL-0').front, false);
  assert.equal(f('INITIAL_DISC_4571').front, false, 'a plain disc: outline and hole only'); assert.equal(f('MEMORIAL_0437').front, false);
  assert.equal(f('CURB').front, false, 'a bar is long'); assert.equal(f('CURB').long, true); assert.equal(f('MINI_0445').front, false, 'a tag with an engraved border is a long carrier'); assert.equal(f('MINI_0445').long, true);
  assert.equal(f('FLAGGED_ART').front, true); assert.equal(f('FLAGGED_ART').how, 'flag'); assert.equal(f('FLAGGED_PLAIN').front, false);
  for (const e of [null, undefined, {}, { sku: 'X' }, { sku: 'X', members: 'many' }, 'text']) assert.equal(O.frontArtOf(e).front, null, JSON.stringify(e));
  assert.equal(O.frontArtOf({ members: 3, holes: 1, backKeepOut: [{ layer: 'BACK KEEP-OUT' }], widthPt: 20, heightPt: 20 }).front, false, 'a keep-out layer is not art');
});

await t('3 ORDER 4200828980: "Gold - 16\\" - 2 sides" on the marigold disc reads BACK engraving, the buyer\'s name and date, source "option", the font word, nothing held for a person', () => {
  const spec = read(); const e = spec.engraving;
  assert.ok(e, 'a reading'); noNested(e, 'engraving');
  assert.equal(e.mode, 'back'); assert.equal(e.source, 'option'); assert.equal(e.needsPerson, false); assert.equal(e.front, null);
  assert.deepEqual(e.back.lines, ['Georgia Andy Margot Jaarsma', 'October 26 2020'], 'the font line is not words, the blank line is gone');
  assert.equal(e.back.text, 'Georgia Andy Margot Jaarsma\nOctober 26 2020'); assert.equal(e.back.fontWord, 'Typewriter');
  assert.equal(e.textSource, 'single'); assert.deepEqual(e.option, { name: OPT, value: 'Gold - 16" - 2 sides' }); assert.deepEqual(e.sides, { n: 2, art: 'index' });
  assert.ok(plain1(e.why) && /2 sides/.test(e.why) && /back/.test(e.why) && /Georgia/.test(e.why), e.why); assert.ok(e.confidence >= 0.85 && e.confidence <= 0.95);
  assert.equal(spec.engraveCandidate, true);
  // the faces model reads it as a back engraving with those words, and the job is seeded from it
  const st = Faces.setupOf({ spec }); assert.equal(st.mode, 'back'); assert.deepEqual(st.faces, ['back']); assert.equal(st.needsPerson, false); assert.equal(st.front, null);
  assert.deepEqual(st.back.lines, ['Georgia Andy Margot Jaarsma', 'October 26 2020']); assert.equal(st.source, 'option'); assert.deepEqual(st.sides, { n: 2, art: 'index' });
  const seed = Faces.seedFor(st, 'back', null); assert.ok(seed); assert.equal(seed.text, 'Georgia Andy Margot Jaarsma\nOctober 26 2020'); assert.equal(seed.fontWord, 'Typewriter'); assert.equal(seed.source, 'option'); assert.equal(seed.how, 'single');
  assert.equal(Faces.seedFor(st, 'front', null), null, 'the front is the design\'s art: no words for it');
});

await t('4 the BFWHY hold (an unmapped Birth Flower option, a table not loaded) changes nothing of the reading; neither do the metal, the length or the other options', () => {
  const held = read(), clear = read({}, ctx({ optionMaps: { '870782383': { 'Birth Flower': { Marigold: { field: 'design', value: 'BIRTHFLOWER_NECKLACE_DISC-9' } } } } }));
  assert.ok(held.problems.length >= 0);
  assert.deepEqual(held.engraving, clear.engraving, 'the same reading with the option mapped or not');
  // everything else of the spec is what it was before the side count was read (the same line on a design nobody knows the art of)
  const unknown = read({}, ctx({ masterEntry: s => (String(s).toUpperCase() === 'BIRTHFLOWER_NECKLACE_DISC-9' ? { sku: s } : null) }));
  for (const k of Object.keys(unknown)) if (!['engraving', 'engraveCandidate'].includes(k)) assert.deepEqual(held[k], unknown[k], 'spec.' + k);
  for (const [v, metal] of [['Gold - 16" - 2 sides', 'gold'], ['Silver- 17"- 2 sides', 'silver'], ['Rose- 18"- 1 side', 'rose']]) {
    const a = withOpt(v, { metalKey: metal, metalLabel: metal }), b = withOpt(v, { metalKey: metal, metalLabel: metal }, ctx({ masterEntry: s => (String(s).toUpperCase() === 'BIRTHFLOWER_NECKLACE_DISC-9' ? { sku: s } : null) }));
    for (const k of ['material', 'materialKey', 'materialLabel', 'size', 'chain', 'form', 'quantity', 'options', 'problems', 'designSku', 'personalization', 'pair', 'pieceCount']) assert.deepEqual(a[k], b[k], v + ' spec.' + k);
  }
});

await t('5 "1 side": only the flower, the buyer\'s words (written or not) are not engraved, nothing is held', () => {
  for (const pers of [PERS, [], ['ANNA']]) {
    const spec = withOpt('Gold - 16" - 1 side', { personalization: pers }), e = spec.engraving;
    assert.ok(e); assert.equal(e.mode, 'none'); assert.equal(e.source, 'option'); assert.equal(e.needsPerson, false); assert.equal(e.front, null); assert.equal(e.back, null); assert.equal(e.textSource, 'none');
    assert.deepEqual(e.sides, { n: 1, art: 'index' }); assert.ok(plain1(e.why) && /1 side/.test(e.why), e.why);
    if (pers.length) assert.equal(e.why, 'Buyer chose 1 side; words not engraved: \u201c' + pers[0].replace(/^Typewriter font\n\n/, '').replace(/\n/g, ' / ') + '\u201d', 'ONE calm line for a person');
    else assert.match(e.why, /^1 side from the option .*: only the design's own art is engraved$/); assert.equal(spec.engraveCandidate, pers.length > 0, 'the line is read as it always was; the engraving pass cuts it plain with the reason');
    assert.equal(e.reviewKind, undefined); assert.equal(e.raw, undefined); noNested(e, 'one side');
    const st = Faces.setupOf({ spec }); assert.equal(st.mode, 'none'); assert.deepEqual(st.faces, []); assert.equal(st.needsPerson, false);
  }
  // "single sided" and "one-sided" read the same
  for (const v of ['one-sided', 'Silver - 17"- 1 side', 'single sided']) assert.equal(withOpt(v).engraving.mode, 'none', v);
});

await t('6 2 sides with other words: labelled "back:" is the back; labelled "front:" is a conflict for a person; no words asks; a request or a long note is left to the engraving reader; a quoted word is read', () => {
  const lab = withOpt('Silver- 16"- 2 sides', { personalization: ['font: Pristina\nback: NAME1 and ...'] }).engraving;
  assert.equal(lab.mode, 'back'); assert.equal(lab.needsPerson, false); assert.equal(lab.back.text, 'NAME1 and ...'); assert.equal(lab.back.fontWord, 'Pristina'); assert.equal(lab.textSource, 'labelled');
  const both = withOpt('Gold - 16" - 2 sides', { personalization: ['front: A\nback: B'] }).engraving;
  assert.equal(both.needsPerson, true); assert.equal(both.reviewKind, 'conflict'); assert.match(both.why, /labelled front/);
  const none = withOpt('Gold - 16" - 2 sides', { personalization: [] }).engraving;
  assert.deepEqual([none.mode, none.needsPerson, none.reviewKind, none.back], ['back', true, 'nowords', null]); assert.match(none.why, /2 sides/); assert.ok(plain1(none.why));
  const note = withOpt('Gold - 16" - 2 sides', { personalization: [], buyerMessage: 'Please engrave Anna on the back' }).engraving;
  assert.deepEqual([note.mode, note.needsPerson, note.back], ['back', false, null], 'the words are in a note: the reader reads them as it always did for a back');
  const ask = withOpt('Gold - 16" - 2 sides', { personalization: ['Please engrave my name Anna and the date 10.26.20 in the centre of the back, thank you so much'] }).engraving;
  assert.deepEqual([ask.mode, ask.needsPerson, ask.back], ['back', false, null], 'not a plain inscription: the reader decides');
  const quoted = withOpt('Gold - 16" - 2 sides', { personalization: ['"JFC" is the engraving, centered please'] }).engraving;
  assert.equal(quoted.back && quoted.back.text, 'JFC'); assert.equal(quoted.textSource, 'single'); assert.ok(quoted.confidence <= 0.8);
  for (const e of [lab, both, none, note, ask, quoted]) noNested(e, 'reading');
});

await t('7 a design with NO art of its own keeps today\'s reading: 2 sides = both faces of buyer text (a person says which words go where), 1 side = the default back; an unknown design too', () => {
  for (const sku of ['INITIAL_DISC_4571', 'CURB', 'MINI_0445', 'MEMORIAL_0437', 'FLAGGED_PLAIN', 'NOT_IN_MASTER']) {
    const two = read({ sku }), one = withOpt('Gold - 16" - 1 side', { sku });
    assert.equal(two.engraving.mode, 'both', sku); assert.equal(two.engraving.needsPerson, true, sku); assert.equal(two.engraving.reviewKind, 'texts', sku); assert.equal(two.engraving.sides, undefined, sku); assert.equal(two.engraving.back, null, sku);
    assert.equal(one.engraving, undefined, sku + ': "1 side" says no face: the default back, read by the engraving reader as before'); assert.equal(one.engraveCandidate, true, sku);
    const lab = read({ sku, personalization: ['front: A\nback: B'] }).engraving; assert.deepEqual([lab.mode, lab.front.text, lab.back.text, lab.needsPerson], ['both', 'A', 'B', false], sku);
  }
  // a design whose own entry says it has art wins over a count
  assert.equal(read({ sku: 'FLAGGED_ART' }).engraving.mode, 'back');
});

await t('8 a value that names its faces, a person\'s saved answer for the value, a conflicting option and the title all stay above a side count', () => {
  const tag = withOpt('Tag1 (front+back)'); assert.equal(tag.engraving.mode, 'both'); assert.equal(tag.engraving.sides, undefined);
  const answered = O.interpretLine({ receiptId: '4200828980', updateTs: 0 }, { transactionId: 't1', listingId: '870782383', sku: 'BIRTHFLOWER_NECKLACE_DISC-9', title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['front: A\nback: B'], variations: [V('Birth Flower', 'Marigold'), V(OPT, 'Gold - 16" - 2 sides')] },
    ctx({ optionMaps: { '870782383': { [OPT]: { 'Gold - 16" - 2 sides': { field: 'ignore', value: null, source: 'answer', engrave: 'both' } } } } }));
  const ans = answered.options.find(o => o.name === OPT);
  assert.equal(ans && ans.mapped && ans.mapped.engrave, 'both', 'the saved answer reaches the option');
  assert.equal(answered.engraving.mode, 'both', 'a saved answer that says both is both, whatever the art'); assert.equal(answered.engraving.front.text, 'A'); assert.equal(answered.engraving.back.text, 'B'); assert.equal(answered.engraving.sides, undefined);
  const conflict = read({ variations: [V('Birth Flower', 'Marigold'), V(OPT, 'Gold - 16" - 2 sides'), V('Charm Type', 'Tag1 (front engrave)')] }).engraving;
  assert.equal(conflict.needsPerson, true); assert.equal(conflict.reviewKind, 'conflict');
});

await t('9 a person\'s saved edit always wins over the side count', () => {
  const spec = read(); const base = { spec };
  const mine = Faces.setupOf(base, { mode: 'both', front: { text: 'A' }, back: { text: 'B' }, by: 'Rita', at: 5 });
  assert.deepEqual([mine.mode, mine.edited, mine.front.text, mine.back.text, mine.needsPerson], ['both', true, 'A', 'B', false]);
  const none = Faces.setupOf(base, { mode: 'none', by: 'Rita', at: 5 }); assert.deepEqual([none.mode, none.faces.length, none.edited], ['none', 0, true]);
  const one = Faces.setupOf({ spec: withOpt('Gold - 16" - 1 side') }, { mode: 'back', back: { text: 'ANNA' }, by: 'Rita', at: 5 });
  assert.deepEqual([one.mode, one.faces, one.back.text, one.edited], ['back', ['back'], 'ANNA', true], 'a person can still add words to a 1 side line');
});

await t('10 the whole intake: 1 side, 2 sides on every real art / plain design of the sample, every spelling: always a well-formed reading, never throws, never fills the front', () => {
  let n = 0;
  for (const sku of Object.keys(ENT)) for (const v of ['Gold - 16" - 2 sides', 'Silver- 16"- 1 side', 'GOLD·DOUBLE SIDED', 'single sided', 'Rose- 18"- 2 sides']) for (const pers of [PERS, [], ['A'], ['front: A\nback: B'], ['please make it nice'], ['X\nY\nZ\nW']]) {
    const spec = withOpt(v, { sku, personalization: pers }); n++;
    const e = spec.engraving; if (!e) continue;
    assert.ok(['none', 'front', 'back', 'both'].includes(e.mode)); assert.equal(typeof e.needsPerson, 'boolean'); assert.ok(plain1(e.why), e.why); noNested(e, v);
    if (e.sides) { assert.equal(e.front, null, 'a side count never fills the front'); assert.ok(e.mode === (e.sides.n === 2 ? 'back' : 'none') || e.needsPerson, v + ' ' + e.mode); }
    Faces.setupOf({ spec });
  }
  assert.ok(n > 150);
});

await t('11 the extra spellings: "2 side", "2x sided", "two sides", "2-sided", "double-sided", "1 sided", "single side", "1side", "one side only", "flower only" (and a non-breaking space); a value that says both counts, or a face, says nothing', () => {
  for (const v of ['2 side', '2x sided', '2X Sided', 'two sides', '2-sided', 'double-sided', 'Double Sided', '2x sides', 'Gold - 16" - 2 side', 'Rose- 17"- 2x sided ($39.50)', 'Gold&nbsp;-&nbsp;16&quot;&nbsp;-&nbsp;2&nbsp;sides', 'Gold - 16" - 2 sides', '2&#160;sides']) assert.equal(O.sidesOfValue(v), 2, v);
  for (const v of ['1 sided', 'single side', '1side', '1 side', 'one side only', 'One Side Only', 'single sided only', 'flower only', 'Flower Only', 'Gold - 16" - flower only', 'Silver- 17"- design only', 'art only', 'Gold - 16" - 1 side engraved', '1x sided']) assert.equal(O.sidesOfValue(v), 1, v);
  for (const v of ['1 side / 2 sides', 'flower only 2 sides', 'front only', 'back only', 'Gold - 16" - engraving only', 'flowers', 'only gold', 'Gold Filled - Only', 'Flower only, no engraving', 'Writing-FRONT only 2 sides']) assert.equal(O.sidesOfValue(v), 0, v);
  // end to end, on the marigold disc, each spelling reads like its number
  for (const v of ['Gold - 16" - 2 side', 'Silver- 17"- 2x sided', 'Rose- 18"- two sides', 'Gold - 2-sided', 'Gold - double-sided']) { const e = withOpt(v).engraving; assert.deepEqual([e.mode, e.needsPerson, e.back && e.back.text, e.sides.n], ['back', false, 'Georgia Andy Margot Jaarsma\nOctober 26 2020', 2], v); }
  for (const v of ['Gold - 16" - 1 sided', 'Silver- 17"- single side', 'Rose- 18"- 1side', 'Gold - one side only', 'Gold - 16" - flower only']) { const e = withOpt(v).engraving; assert.deepEqual([e.mode, e.needsPerson, e.back, e.sides.n], ['none', false, null, 1], v); }
});

await t('12 the policy (flippable in one line): 1 side + words = nothing engraved and ONE calm line, never held; "ask" asks a person; "back" engraves the words anyway; 2 sides + unlabelled words = back, "ask" confirms', () => {
  const P = O.SIDES_POLICY; assert.deepEqual(Object.assign({}, P), { oneSideWithWords: 'skip', twoSidesUnlabelled: 'back' }, 'the defaults');
  try {
    const one = () => withOpt('Gold - 16" - 1 side').engraving, two = () => read().engraving;
    let e = one(); assert.deepEqual([e.mode, e.needsPerson, e.back, e.reviewKind], ['none', false, null, undefined]); assert.match(e.why, /^Buyer chose 1 side; words not engraved: “Georgia/);
    P.oneSideWithWords = 'ask'; e = one(); assert.deepEqual([e.mode, e.needsPerson, e.reviewKind, e.back], ['none', true, 'conflict', null]); assert.ok(plain1(e.why) && /1 side/.test(e.why) && /Georgia/.test(e.why), e.why); assert.equal(Faces.setupOf({ spec: withOpt('Gold - 16" - 1 side') }).needsPerson, true);
    assert.equal(withOpt('Gold - 16" - 1 side', { personalization: [] }).engraving.needsPerson, false, 'no words: nothing to ask');
    P.oneSideWithWords = 'back'; e = one(); assert.deepEqual([e.mode, e.needsPerson, e.back && e.back.text, e.sides.n], ['back', false, 'Georgia Andy Margot Jaarsma\nOctober 26 2020', 1]); assert.ok(plain1(e.why), e.why);
    assert.equal(withOpt('Gold - 16" - 1 side', { personalization: [] }).engraving.mode, 'none', 'back policy without words: still nothing to engrave');
    P.oneSideWithWords = 'skip'; P.twoSidesUnlabelled = 'ask'; e = two(); assert.deepEqual([e.mode, e.needsPerson, e.reviewKind, e.back && e.back.text], ['back', true, 'texts', 'Georgia Andy Margot Jaarsma\nOctober 26 2020'], 'suggested for the back, a person confirms');
    e = withOpt('Gold - 16" - 2 sides', { personalization: ['back: Anna'] }).engraving; assert.deepEqual([e.mode, e.needsPerson, e.back.text], ['back', false, 'Anna'], 'a labelled back is no question');
    P.twoSidesUnlabelled = 'back'; e = two(); assert.deepEqual([e.mode, e.needsPerson], ['back', false]);
    for (const k of Object.keys(P)) assert.ok(['skip', 'ask', 'back'].includes(P[k]));
  } finally { P.oneSideWithWords = 'skip'; P.twoSidesUnlabelled = 'back'; }
});

await t('13 the design-front test comes from the listing and the master only: art in the index, not long, not a handwriting / photo / picture listing; the option words never decide it; an explicit frontArt flag wins over the title', () => {
  const E = ENT['BIRTHFLOWER_NECKLACE_DISC-9'];
  assert.equal(O.frontArtOf(E, 'Birth Flower Necklace').front, true); assert.equal(O.frontArtOf(E, '').front, true); assert.equal(O.frontArtOf(E).front, true);
  for (const title of ['Actual Handwriting Necklace', 'Handwritten Memorial Necklace', 'Photo Engraved Necklace', 'Custom Picture Disc', 'Drawing Necklace', 'Logo Charm', 'Fingerprint Necklace']) { const a = O.frontArtOf(E, title); assert.deepEqual([a.front, a.how], [false, 'kind'], title); }
  assert.equal(O.frontArtOf(E, 'Birth Flower Necklace, Gold Birth Flower Necklace, Bridesmaid Gift').front, true);
  assert.equal(O.frontArtOf(ENT.FLAGGED_ART, 'Handwriting Necklace').front, true, 'a person flag wins');
  // the same value on the same design: the title of the listing decides, never the option words
  for (const title of ['Actual Handwriting Birth Flower Necklace', 'Photo Birth Flower Necklace']) { const sp = read({ title }); assert.equal(sp.engraving.mode, 'both', title); assert.equal(sp.engraving.sides, undefined, title); assert.equal(sp.engraving.needsPerson, true, title); }
  assert.equal(read({ title: 'Birth Flower Necklace' }).engraving.mode, 'back');
  // option words that look like a face or an art do not make a design front: a plain design with "flower only" or "2 sides" is read as before
  for (const v of ['Gold - flower only', 'Gold - 2 sides', 'Gold - 1 side']) { const sp = withOpt(v, { sku: 'INITIAL_DISC_4571' }); assert.equal(sp.engraving && sp.engraving.sides, undefined, v); }
});

await t('14 the plain-face listings are read byte for byte as before: every real design of the audit\'s twelve "Front&Back" / "DOUBLE SIDED" listings (79 orders) never takes a side count', () => {
  // (the library index entries of 9 Oct 2026 and the option values BFAUDIT found on those listings)
  const D = { CURB: [4, 2, 45.02, 11.008], HEART_3750: [1, 0, 41.318, 39.577], BEADY: [3, 2, 91.921, 17.248], GOLD_7478: [1, 0, 87.54, 16.673], MINI_0445: [4, 1, 16.894, 28.036], SKINNY_0191: [3, 2, 73.366, 11.004], MEMORIAL_0437: [5, 4, 47.854, 47.854], SKINNY_2322: [4, 2, 78.719, 26.912] };
  const VALUES = ['SILVER · Front&Back', 'GOLD · Front&Back', 'ROSE + FRONT&BACK', 'Gold Writing-FRONT', 'Silver Writing-FRONT', 'Gold - FRONT & BACK', 'Tag1 (front engrave)', 'Tag2 (front+back)', 'Tag4 (front + back)', 'SILVER-Writing FRONT', 'ROSE Front&Back', 'GOLD·DOUBLE SIDED', 'ROSE GOLD·DBL SIDED', 'SILVER·DOUBLE SIDED', 'SILVER·ENGRAVED', 'GOLD·Front', 'GOLD·6-10·Characters'];
  for (const [sku, [members, holes, widthPt, heightPt]] of Object.entries(D)) {
    const fa = O.frontArtOf({ sku, members, holes, backKeepOut: [], widthPt, heightPt }); assert.notEqual(fa.front, true, sku + ' has no art of its own in the index');
    for (const v of VALUES) assert.equal(O.sidesOfValue(v) === 0 || fa.front !== true, true, sku + ' ' + v);
  }
  for (const v of VALUES.filter(x => !/DOUBLE|DBL/.test(x))) assert.equal(O.sidesOfValue(v), 0, v + ' names faces, not a side count');
});

console.log(`\n${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
})();
