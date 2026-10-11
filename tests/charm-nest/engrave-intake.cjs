// ENGINTAKE (Paul, 11 Oct 2026): "there must be an actual check on every single listing that is being processed to ensure that if there is back and front engraving, it's correctly
// identified and then interpreted: what is supposed to go on the front, what on the back, in terms of what the buyer chose."
// Offline: no Firestore, no AI, no network. It tests spec.engraving as plans/front-back-1011/CONTRACT.md section 2 describes it (charm-nest-orders.js), and that the faces model
// (charm-nest-engrave-faces.js, ENGMAP) reads it as intended. The golden (section 7) replays every line of the offline order data against the digests of the base commit.
'use strict';
const assert = require('assert/strict'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const noNested = require('./_noNestedArrays.cjs');
const W = require('./_engrave-intake-world.cjs');
/** The same module with the ONE hook that calls engravingOf cut out: what interpretLine gave before this feature, whatever else other workers change in the file. */
const BASE = (() => {
  const src = fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8'), a = src.indexOf('    // ENGINTAKE (CONTRACT section 2)'), end = '/* a reading that fails leaves the line as it always was */ }', b = src.indexOf(end, a);
  assert.ok(a > 0 && b > a, 'the intake hook is in charm-nest-orders.js'); const m = { exports: {} };
  new Function('module', 'exports', 'require', src.slice(0, a) + src.slice(b + end.length))(m, m.exports, require); return m.exports;
})();
const AUDIT = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/engrave-intake-audit.json'), 'utf8'));

let passed = 0, failed = 0; const t = async (name, fn) => { try { await fn(); passed++; console.log('  ok  ' + name); } catch (e) { failed++; console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n      ')); } };
const V = (name, value) => ({ name, value });
const MASTER = new Set(['MINI_0445', 'EAR_1', 'INITIAL_DISC_4571', 'TRUCK_1']);
const entryFor = s => (MASTER.has(String(s || '').toUpperCase()) ? { sku: s } : null);
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: () => '' }, extra || {});
const read = (line, c) => O.interpretLine({ receiptId: '4195966039', updateTs: 0 }, Object.assign({ transactionId: 't1', listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm Minimalist Tiny Pendant Gift for Her Custom Initial Charm', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [] }, line), c || ctx());
const TAG = (charmType, pers, extra, font) => read(Object.assign({ variations: [V('Metal Choice', 'Gold Filled'), V('Charm Type', charmType)].concat(font === null ? [] : [V('Font Choice', font || 'Ace')]), personalization: pers }, extra || {}));
const eng = (charmType, pers, extra, font) => TAG(charmType, pers, extra, font).engraving;
const faces = e => [e.mode, e.front ? e.front.text : null, e.back ? e.back.text : null];
const plain1 = r => typeof r === 'string' && !!r && !/\n/.test(r) && r.length <= 220;
/** The CONTRACT shape (section 2) of one spec.engraving: every field, every rule. */
function shapeOk(e, where) {
  for (const k of ['v', 'mode', 'source', 'front', 'back', 'textSource', 'needsPerson', 'confidence', 'why', 'option']) assert.ok(k in e, where + ': field ' + k);
  assert.equal(e.v, 1, where); assert.ok(['none', 'front', 'back', 'both'].includes(e.mode), where + ' mode ' + e.mode);
  assert.ok(['option', 'title', 'note', 'default'].includes(e.source), where + ' source ' + e.source + ' (never "person": that is row.engravingOverride)');
  assert.ok(['labelled', 'single', 'unlabelled', 'none'].includes(e.textSource), where + ' textSource'); assert.equal(typeof e.needsPerson, 'boolean', where);
  assert.ok(typeof e.confidence === 'number' && e.confidence >= 0 && e.confidence <= 1, where + ' confidence'); assert.ok(plain1(e.why), where + ' why is one plain line: ' + e.why);
  assert.ok(e.option === null || (typeof e.option.name === 'string' && typeof e.option.value === 'string'), where + ' option');
  for (const f of ['front', 'back']) if (e[f] !== null) {
    assert.ok(typeof e[f].text === 'string' && e[f].text, where + ' ' + f + ' text'); assert.ok(Array.isArray(e[f].lines) && e[f].lines.length && e[f].lines.every(x => x && x === x.trim()), where + ' ' + f + ' lines');
    assert.equal(e[f].lines.join('\n'), e[f].text, where + ' lines are the text split'); assert.ok(e[f].fontWord === null || (typeof e[f].fontWord === 'string' && e[f].fontWord), where + ' fontWord');
  }
  if (e.mode === 'back' || e.mode === 'none') assert.equal(e.front, null, where + ': front is null in mode ' + e.mode);
  if (e.mode === 'front' || e.mode === 'none') assert.equal(e.back, null, where + ': back is null in mode ' + e.mode);
  if (e.mode === 'both' && !e.needsPerson) assert.ok(e.front && e.back, where + ': mode both without a person needs both faces');
  if (e.needsPerson) { assert.ok(['texts', 'mode', 'conflict', 'nowords', 'pieces'].includes(e.reviewKind), where + ' reviewKind'); assert.equal(typeof e.raw, 'string', where + ' the buyer\'s words are kept'); }
  else { assert.equal(e.reviewKind, undefined, where); assert.equal(e.raw, undefined, where); }
  noNested(e, where);
}

(async () => {
console.log('ENGINTAKE · front + back engraving');

/* ═══ 1 · the mode words ═══ */
await t('1a every way an OPTION says both faces', () => {
  for (const v of ['Tag1 (front+back)', 'Tag3 (front + back)', 'Tag4 (front + back)', 'Front&amp;Back', 'SILVER · Front&Back', 'front and back', 'Front/Back', 'back & front', 'Front, Back', 'both sides', 'Both Sides Engraved', 'engraved on both', 'two sided', '2-sided', 'double sided', 'GOLD·DOUBLE SIDED', 'ROSE GOLD·DBL SIDED', 'Silver- 16"- 2 sides', 'Front Engraved + Back Engraved', 'Engraving - Front, Back'])
    assert.equal(O.engravingModeOfOption('Charm Type', v), 'both', v);
  assert.equal(O.engravingModeOfOption('Front and Back Engraving', 'Yes'), 'both', 'the option is named for it and the buyer said yes');
  assert.equal(O.engravingModeOfOption('Engraving Side', 'Both'), 'both', 'the whole value "Both" under an option named for a side');
});
await t('1b every way an option says front only, back only, or no engraving', () => {
  for (const v of ['Tag1 (front engrave)', 'Front engrave', 'front engraving', 'Front only', 'front-only', 'engraved on front', 'Engrave Front', 'engraved on the front', 'Gold Writing-FRONT', 'SILVER-Writing FRONT', 'Engraving: front']) assert.equal(O.engravingModeOfOption('Charm Type', v), 'front', v);
  for (const v of ['Back only', 'back engraving', 'Back Engraving', 'engraved on the back', 'Engraving on the back', 'back side engraving', 'Writing-BACK']) assert.equal(O.engravingModeOfOption('Charm Type', v), 'back', v);
  assert.equal(O.engravingModeOfOption('Engraving Side', 'Front'), 'front'); assert.equal(O.engravingModeOfOption('Where to engrave', 'Back'), 'back');
  assert.equal(O.engravingModeOfOption('Engrave front?', 'Yes'), 'front');
  assert.equal(O.engravingModeOfOption('Metal/Both Sides Engraved? (Front/Back)', 'GOLD·Front'), 'front', 'the FIELD NAME carries the face word and the value is "metal · face"');
  assert.equal(O.engravingModeOfOption('Metal Choice', 'GOLD·Front'), '', 'without an engraving field name a trailing "Front" is no face');
});
await t('1c words that are NOT engraving words stay no mode: a design called "Front Charm", a drop-down value alone, "no engraving", "CHARM + Engraving", "1 side", "both charms"', () => {
  for (const [n, v] of [['Charm Type', 'Truck Front Charm'], ['Charm Type', 'Front Door Key'], ['Charm Type', 'Back Pack'], ['Charm Type', 'Front'], ['Charm Type', 'Back'], ['Charm Type', 'Both'], ['Style', 'Both'], ['Charm Type', 'CHARM + Engraving'], ['Charm Type', 'Necklace CHARM'], ['Metal Choice', 'Gold Filled'], ['Charm Type', 'No Engraving'], ['Charm Type', 'plain back'], ['Metal/Necklace Length', 'Silver - 16"- 1 side'], ['Engraving? (Double sided=both sides engraved)', 'SILVER·ENGRAVED'], ['Charm Type', 'Gold Double Heart'], ['Charm Type', 'Double Disc']])
    assert.equal(O.engravingModeOfOption(n, v), '', n + ': ' + v);
  assert.equal(O.engravingModeIn('Truck Front Charm: Sterling Silver, Gold Filled, 14K Solid Gold Minimalist Automotive Charm Jewelry', true), '');
  assert.equal(read({ title: 'Truck Front Charm: Sterling Silver', sku: 'TRUCK_1', variations: [V('Metal Choice', 'Gold Filled'), V('Charm Type', 'Necklace CHARM')] }).engraving, undefined, 'a line whose only face word is its design\'s name has no engraving reading at all');
  for (const txt of ['Please engrave "XO" on both charms', 'engrave on both hearts', 'same letters on both']) assert.equal(O.engravingModeIn(txt, true), '', '"both" in a note means two items: ' + txt);
});
await t('1d titles: a face word counts only beside an engraving word; a title alone never settles the words', () => {
  assert.equal(O.engravingModeIn('Custom Name Charm, Engraved on Front and Back', true), 'both'); assert.equal(O.engravingModeIn('Disc Necklace, Front & Back Engraving', true), 'both');
  assert.equal(O.engravingModeIn('Front and Back Charm Collection', true), '', 'no engraving word beside it');
  const e = eng('Necklace CHARM', ['A'], { title: 'Initial Disc, Engraved on Front and Back' }); shapeOk(e, 'title');
  assert.deepEqual([e.mode, e.source, e.needsPerson, e.front, e.back], ['both', 'title', true, null, null], 'the listing says it, the buyer chose nothing and wrote one unlabelled word: a person');
  const ok = eng('Necklace CHARM', ['front: A\nback: B'], { title: 'Initial Disc, Engraved on Front and Back' }); assert.deepEqual([ok.source, ok.needsPerson, ok.front.text, ok.back.text, ok.confidence], ['title', false, 'A', 'B', 0.7]);
  const front = eng('Necklace CHARM', ['Anna'], { title: 'Initial Disc, Front Engraved' }); assert.deepEqual([front.mode, front.source, front.needsPerson, front.reviewKind], ['front', 'title', true, 'mode']);
  const back = eng('Necklace CHARM', ['Anna'], { title: 'Custom Charm with 2026 Back Engraving, Made to Order' }); assert.deepEqual([back.mode, back.source, back.needsPerson, back.front, back.back], ['back', 'title', false, null, null], 'a title that says back engraving: the back, as always');
  assert.equal(eng('Necklace CHARM', ['Anna'], { title: 'Hiking Charm Add On Charm Boot Backpack Charm' }), undefined);
});
await t('1e the audit\'s 70 option rows (ENGAUDIT-cases.json): every spelling Etsy has for a face reads as the audit says; "Blank" is no face', () => {
  let n = 0;
  for (const [field, value, expected, how] of AUDIT.options) {
    const got = O.engravingModeOfOption(field, value);
    if (expected === 'none') {   // ("Blank": a bar sold with no engraving: no face, and words that came with it are a conflict that stays mode none)
      assert.equal(got, '', field + ' | ' + value);
      const e = read({ variations: [V(field, value)], personalization: ['Anna'] }).engraving; assert.deepEqual([e.mode, e.source, e.needsPerson, e.reviewKind], ['none', 'option', true, 'conflict'], field + ' | ' + value);
      assert.equal(read({ variations: [V(field, value)], personalization: [] }).engraving, undefined, 'no words with it: nothing to read');
    }
    else if (how === 'option') assert.equal(got, expected, field + ' | ' + value);
    else assert.equal(got, '', field + ' | ' + value + ' names no face (' + expected + ' by default)');
    n++;
  }
  assert.equal(n, 70);
});
await t('1f the audit\'s negatives: titles and options that merely look like a face read as nothing', () => {
  for (const [title] of AUDIT.negativeTitles) assert.equal(O.engravingModeIn(title, true), '', title);
  for (const [field, value] of AUDIT.negativeOptions) assert.equal(O.engravingModeOfOption(field, value), '', field + ' | ' + value);
  assert.ok(AUDIT.negativeTitles.length >= 10 && AUDIT.negativeOptions.length >= 10);
});

/* ═══ 2 · the labelled forms ═══ */
const FORMS = [
  'front - {F} / back - {B}', 'front - {F}\nback - {B}', 'Front: {F}, Back: {B}', 'front: {F} back: {B}', 'FRONT : {F}\nBACK : {B}', 'front = {F}\nback = {B}', 'front / {F}\nback / {B}', 'front/{F} / back/{B}',
  'front {F}\nback {B}', 'front {F} / back {B}', 'front {F} back {B}', 'back: {B}\nfront: {F}', 'back - {B} / front - {F}', 'Back {B}\nFront {F}',
  'on the front: {F}, on the back: {B}', '{F} on the front and {B} on the back', '{F} on front, {B} on back', '{B} on the back and {F} on the front', 'front side: {F}\nback side: {B}',
  'front of tag: {F}\nback of tag: {B}', 'Front engraving: {F}\nBack engraving: {B}', 'front:\n{F}\nback:\n{B}', 'FRONT—{F} / BACK—{B}', 'front – {F}; back – {B}', 'Hello!\nFront: {F}\nBack: {B}', 'front: {F}\nback: {B}\n\nplease center both\nthank you'];
const TEXTS = [['W', '1. 27. 24'], ['A', 'B'], ['Anna', '2024'], ['MC', 'LOVE'], ['m', '3'], ['Z', '10/05/2019']];
await t('2a every form x every pair of words: the front is the front and the back is the back (either order of writing)', () => {
  let n = 0;
  for (const form of FORMS) for (const [F, B] of TEXTS) {
    const pers = form.replace('{F}', F).replace('{B}', B);
    const e = eng('Tag1 (front+back)', [pers]); shapeOk(e, JSON.stringify(pers));
    assert.deepEqual([e.mode, e.front && e.front.text, e.back && e.back.text, e.needsPerson, e.textSource], ['both', F, B, false, 'labelled'], JSON.stringify(pers)); n++;
    const asTwo = eng('Tag1 (front+back)', pers.split('\n'));   // (the same words as two entries of the field)
    if (pers.includes('\n') && !/:\n/.test(pers) && !/\n\n/.test(pers) && !/^Hello/.test(pers)) assert.deepEqual([asTwo.front && asTwo.front.text, asTwo.back && asTwo.back.text], [F, B], 'two entries ' + JSON.stringify(pers));
  }
  assert.ok(n >= 150);
});
await t('2b with NO option the same words still name the faces: read, listed as the buyer\'s own request with a lower confidence and "a person confirms" in the line', () => {
  for (const form of FORMS.slice(0, 12)) {
    const e = eng('Necklace CHARM', [form.replace('{F}', 'W').replace('{B}', '5')]); shapeOk(e, form);
    assert.deepEqual([e.mode, e.source, e.front && e.front.text, e.back && e.back.text, e.needsPerson, e.option, e.confidence], ['both', 'note', 'W', '5', false, null, 0.6], JSON.stringify(form));
    assert.match(e.why, /no such option: a person confirms/);
  }
  const back = eng('Necklace CHARM', ['back: Mom']); assert.deepEqual([back.mode, back.source, back.needsPerson, back.front, back.back], ['back', 'note', false, null, null], 'a back label alone is the back engraving the shop always did: read as before');
});
await t('2c the words keep what the buyer typed: case, dots, symbols, digits, a trailing full stop; quotes around a whole face are the only thing taken off', () => {
  const e = eng('Tag1 (front+back)', ['front - Wxy-Z\nback - 1. 27. 24.']); assert.deepEqual([e.front.text, e.back.text], ['Wxy-Z', '1. 27. 24.']);
  const q = eng('Tag1 (front+back)', ['front - "Mom"\nback - “Dad”']); assert.deepEqual([q.front.text, q.back.text], ['Mom', 'Dad']);
  const m = eng('Tag1 (front+back)', ['front: Mary, Jane back: 1999']); assert.deepEqual([m.front.text, m.back.text], ['Mary, Jane', '1999'], 'a comma inside a face\'s words stays');
  const w = eng('Tag1 (front+back)', ['front - Front Row\nback - Back to Basics']); assert.deepEqual([w.front.text, w.back.text], ['Front Row', 'Back to Basics'], 'a face word INSIDE the words is not a label');
  const l = eng('Tag1 (front+back)', ['front:\nA\nB\nback: C']); assert.deepEqual([l.front.text, l.back.text, l.front.lines], ['A\nB', 'C', ['A', 'B']], 'several lines under one label stay several lines');
  const only = eng('Tag1 (front+back)', ['Back only: CD']); assert.deepEqual([only.back && only.back.text, only.front, only.needsPerson], ['CD', null, true], '"Back only: CD" under front+back: the back is read, the front is missing: a person');
});
await t('2d the font of each face: null = the line\'s own font (spec.font), a word only when the buyer named one for that face (or a line "font: X" in the note)', () => {
  const sp = TAG('Tag1 (front+back)', ['front - W\nback - 1. 27. 24'], {}, 'Ace'), e = sp.engraving;
  assert.deepEqual([e.front.fontWord, e.back.fontWord, sp.font.asked], [null, null, 'Ace'], 'one font option: both faces take the line\'s font (Ace), nothing is repeated per face');
  assert.match(e.why, /font Ace/, 'the one line says the font');
  const v = eng('Tag1 (front+back)', ['Front: m - Vibur / Back: 3 - Vibur'], {}, 'Ace');
  assert.deepEqual([v.front.text, v.front.fontWord, v.back.text, v.back.fontWord], ['m', 'Vibur', '3', 'Vibur'], 'the audit\'s real wording "Front: m - Vibur / Back: 3 - Vibur" (a face\'s own font wins over the option)');
  const o = eng('Tag1 (front+back)', ['Front in Ace font: W\nBack: 5'], {}, 'Vibur'); assert.deepEqual([o.front.fontWord, o.back.fontWord], ['Ace', null], 'only the face that names a font changes');
  const lbl = eng('Tag1 (front+back)', ['Front of tag in font Britannic Bold: T\nBack of tag in font Vibur: m'], {}, 'Ace');
  assert.deepEqual([lbl.front.text, lbl.front.fontWord, lbl.back.text, lbl.back.fontWord], ['T', 'Britannic Bold', 'm', 'Vibur'], 'a font named in the label ("Front of tag in font Britannic Bold: T")');
  assert.equal(O.fontOfPhrase(lbl.front.fontWord), 'alegreya-sans'); assert.equal(O.fontOfPhrase(v.front.fontWord), 'playwrite-us-modern');
  const ln = eng('Tag1 (front+back)', ['front - W\nback - 5\nfont: Britannic Bold'], {}, null); assert.deepEqual([ln.front.text, ln.back.text, ln.front.fontWord, ln.back.fontWord], ['W', '5', 'Britannic Bold', 'Britannic Bold'], 'a line "font: X" is the font of both faces, not words to engrave');
  const nf = eng('Tag1 (front+back)', ['front - Jack - Ace\nback - 5'], {}, null); assert.equal(nf.front.text, 'Jack - Ace', '"Ace" is a name as often as a font: a font only with the word font beside it');
  const none = eng('Tag1 (front+back)', ['front - W\nback - 5'], {}, null); assert.deepEqual([none.front.fontWord, none.back.fontWord], [null, null]);
  const clean = eng('Tag1 (front+back)', ['Front of tag in font Britannic Bold: T (capital letter) / Back of tag in font Vibur: mine (lowercase)'], {}, 'Ace');
  assert.deepEqual([clean.front.text, clean.back.text, clean.needsPerson], ['T', 'mine', false], 'a closing note in brackets about HOW to write the words ("(capital letter)") is no part of them');
});

/* ═══ 3 · the order of the brief ═══ */
await t('3a order 4195966039, listing 4458930445, Charm Type "Tag1 (front+back)", font Ace, "front - W" / "back - 1. 27. 24": mode both, front W, back 1. 27. 24, font Ace', () => {
  const sp = TAG('Tag1 (front+back)', ['front - W\nback - 1. 27. 24'], {}, 'Ace'), e = sp.engraving; shapeOk(e, 'paul');
  assert.deepEqual([e.v, e.mode, e.source, e.front.text, e.back.text, e.textSource, e.needsPerson, e.option], [1, 'both', 'option', 'W', '1. 27. 24', 'labelled', false, { name: 'Charm Type', value: 'Tag1 (front+back)' }]);
  assert.ok(e.confidence >= 0.9 && /front.*W.*back.*1\. 27\. 24.*font Ace/.test(e.why), e.why);
  assert.equal(sp.font.asked, 'Ace', 'the line\'s own reading is untouched'); noNested(sp, 'spec');
  const joined = eng('Tag1 (front+back)', ['front - W / back - 1. 27. 24']); assert.deepEqual(faces(joined), ['both', 'W', '1. 27. 24'], 'typed on one line with a slash');
  const sp2 = eng('Tag1 (front+back)', ['front - W', 'back - 1. 27. 24']); assert.deepEqual(faces(sp2), ['both', 'W', '1. 27. 24'], 'two entries of the field');
});
await t('3b "Tag1 (front engrave)" reads mode front; an unlabeled single-word personalisation is the front text; "front" label alone too', () => {
  for (const ct of ['Tag1 (front engrave)', 'Tag2 (front engrave)', 'Tag3 (front engrave)', 'Tag4 (front engrave)']) {
    const e = eng(ct, ['W'], {}, 'Vibur'); shapeOk(e, ct); assert.deepEqual([e.mode, e.source, e.front.text, e.back, e.needsPerson, e.textSource, e.front.fontWord], ['front', 'option', 'W', null, false, 'single', null], ct);
  }
  assert.deepEqual(faces(eng('Tag1 (front engrave)', ['front - W'])), ['front', 'W', null]);
  assert.deepEqual(faces(eng('Tag1 (front engrave)', ['Anna'])), ['front', 'Anna', null]);
  assert.deepEqual(faces(eng('Tag1 (front engrave)', ['"JFC" is the engraving, arranged sideways on the tag (like the example photo you show with DKR)'])), ['front', 'JFC', null], 'one word in quotes among the buyer\'s sentences is the engraving (audit P05)');
  for (const ct of ['Tag2 (front+back)', 'Tag3 (front + back)', 'Tag4 (front + back)']) assert.equal(eng(ct, ['front - W\nback - 5']).mode, 'both', ct);
});
await t('3c "back only" keeps today\'s back behaviour: mode back, no words taken (the engraving reader reads them as always), nothing to decide', () => {
  for (const pers of [['W'], ['back: 1. 27. 24'], ['Back engrave- Dr. NAME1'], ['can you put a pretty A on the back']]) {
    const e = eng('Tag1 (back only)', pers); shapeOk(e, 'back'); assert.deepEqual([e.mode, e.front, e.back, e.needsPerson, e.textSource, e.source], ['back', null, null, false, 'none', 'option'], JSON.stringify(pers));
  }
  const none = TAG('CHARM + Engraving', ['Anna']); assert.equal(none.engraving, undefined, 'the ordinary "CHARM + Engraving" line has no front / back word: no reading at all, read exactly as before');
  const note = eng('CHARM + Engraving', ['Can you engrave on the back T+C']); assert.deepEqual([note.mode, note.source, note.needsPerson, note.back], ['back', 'note', false, null]);
});
await t('3d the line itself is read exactly as before: SKU, pieces, problems, options; only spec.engraving is new', () => {
  const cases = [[['front - W\nback - 5'], 'Tag1 (front+back)'], [['W'], 'Tag1 (front engrave)'], [[], 'Tag1 (front engrave)'], [['back only words'], 'Tag1 (back only)']];
  for (const [pers, ct] of cases) {
    const line = { variations: [V('Metal Choice', 'Gold Filled'), V('Charm Type', ct), V('Font Choice', 'Ace')], personalization: pers };
    const mine = read(line), base = BASE.interpretLine({ receiptId: '4195966039', updateTs: 0 }, Object.assign({ transactionId: 't1', listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm Minimalist Tiny Pendant Gift for Her Custom Initial Charm', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [] }, line), ctx());
    const { engraving, ...rest } = mine; assert.ok(engraving);
    assert.equal(JSON.stringify(rest), JSON.stringify(base), ct + ' ' + JSON.stringify(pers)); assert.ok(!('engraving' in base));
  }
});

await t('3e the engraving word the line reader settled on the option (TAGSKU: spec.options[].mapped.engrave) is used, and a person\'s saved answer for the value stands above the option\'s text', () => {
  const saved = { '4458930445': { 'charm type': { 'tag2 (front engrave)': { field: 'design', value: 'MINI_0445', engrave: 'both', by: 'Ana_M', at: 1 } } } };
  const sp = TAG('Tag2 (front engrave)', ['front - W\nback - 5'], {}, 'Ace'), spSaved = read({ variations: [V('Metal Choice', 'Gold Filled'), V('Charm Type', 'Tag2 (front engrave)'), V('Font Choice', 'Ace')], personalization: ['front - W\nback - 5'] }, ctx({ optionMaps: saved }));
  const to = spSaved.options.find(o => o.name === 'Charm Type'); assert.equal(to.mapped.engrave, 'both', 'the line reader carries the person\'s word on the option');
  assert.deepEqual([sp.engraving.mode, sp.engraving.reviewKind], ['front', 'conflict'], 'the text alone: front engrave with a back label is a conflict');
  assert.deepEqual([spSaved.engraving.mode, spSaved.engraving.source, spSaved.engraving.needsPerson, spSaved.engraving.front.text, spSaved.engraving.back.text], ['both', 'option', false, 'W', '5'], 'the saved answer says both: the labelled words are read onto their faces');
  // a settled option the text does not explain at all still says its mode
  const fake = O.engravingOf({ variations: [V('Charm Type', 'Shape X')], personalization: ['front - W\nback - 5'] }, { personalization: ['front - W\nback - 5'], options: [{ name: 'Charm Type', value: 'Shape X', mapped: { field: 'design', value: 'A', engrave: 'both' } }] });
  assert.deepEqual([fake.mode, fake.source, fake.option, fake.front.text, fake.back.text], ['both', 'option', { name: 'Charm Type', value: 'Shape X' }, 'W', '5']);
});

/* ═══ 4 · ambiguous and conflicting lines: a person decides, nothing is guessed ═══ */
const REV = (charmType, pers, extra, font) => { const e = eng(charmType, pers, extra, font); assert.ok(e && e.needsPerson, 'a person must look: ' + JSON.stringify(pers)); shapeOk(e, JSON.stringify(pers)); assert.ok(plain1(e.why)); return e; };
await t('4a mode both with words that do not say which face: not guessed, the buyer\'s words kept visible, no face assigned', () => {
  for (const pers of [['W\n1. 27. 24'], ['W', '1. 27. 24'], ['Mom'], ['W 1. 27. 24'], ['Come back - love you'], ['Front Row Seats'], ['front W'], ['AB\nCD']]) {
    const e = REV('Tag1 (front+back)', pers); assert.deepEqual([e.mode, e.front, e.back, e.reviewKind, e.textSource], ['both', null, null, 'texts', 'unlabelled'], JSON.stringify(pers)); assert.equal(e.raw, pers.join('\n'), 'the buyer\'s words stay visible');
  }
});
await t('4b only one face written, a doubled label, words before the first label, an empty label, two label styles, words left over: all go to a person', () => {
  const one = REV('Tag1 (front+back)', ['front - W']); assert.match(one.why, /only the front is written/); assert.deepEqual([one.front.text, one.back], ['W', null], 'the face that IS labelled is kept, the other left for a person');
  assert.match(REV('Tag1 (front+back)', ['back - 5']).why, /only the back is written/);
  assert.match(REV('Tag1 (front+back)', ['front: A front: B back: C']).why, /labelled twice/);
  assert.match(REV('Tag1 (front+back)', ['Mary front: A back: B']).why, /before the first/);
  assert.match(REV('Tag1 (front+back)', ['front:\nback: B']).why, /no words/);
  assert.match(REV('Tag1 (front+back)', ['front: A\nB on the back']).why, /two different ways/);
  assert.match(REV('Tag1 (front+back)', ['A on the front and B on the back, then 12']).why, /left over/);
  for (const e of [REV('Tag1 (front+back)', ['front: A front: B back: C']), REV('Tag1 (front+back)', ['Mary front: A back: B'])]) assert.deepEqual([e.front, e.back], [null, null], 'no face is filled when the labels are unclear');
});
await t('4c words that carry instructions or notes are not engraved as written; a blank line ends a face block when everything after it is a request (audit P02)', () => {
  const e = REV('Tag1 (front+back)', ['front: Please put Anna\nback: B']); assert.equal(e.reviewKind, 'texts'); assert.match(e.why, /instructions or notes/); assert.deepEqual([e.front, e.back], [null, null]);
  assert.match(REV('Tag1 (front engrave)', ['Please engrave my name Anna in cursive']).why, /not a plain inscription/);
  assert.match(REV('Tag1 (front engrave)', ['W / 1. 27. 24']).why, /more than one item/, 'two items on a front-only tag: maybe the buyer meant front and back');
  assert.match(REV('Tag1 (front engrave)', ['W\n1. 27. 24']).why, /more than one item/);
  const p2 = eng('METAL - ENGRAVING', ['front: go birds\nback: WORD1\n\nboth center\nWORD1 upside down if possible\ndarkened font\n\nTHANK YOU ! <3'], { variations: [V('METAL - ENGRAVING', 'SILVER · Front&amp;Back')] });
  assert.deepEqual([p2.front.text, p2.back.text, p2.needsPerson], ['go birds', 'WORD1', false], 'the audit\'s P02: the lines after the blank line are requests');
  assert.equal(eng('Tag1 (front+back)', ['front: A\nback: B\n\nsecond line of words'], {}).needsPerson, true, 'words after a blank line that are not requests are not dropped silently');
  assert.equal(eng('Tag1 (front+back)', ['front: A\nback: B\nboth center']).needsPerson, true, 'with no blank line "both center" is part of the back\'s words: a person');
});
await t('4d the option and the words disagree: front engrave with a back label, back only with a front label, no engraving with face words, "Blank" with words, two options that disagree', () => {
  const a = REV('Tag1 (front engrave)', ['front - W\nback - 5']); assert.deepEqual([a.mode, a.reviewKind, a.front, a.back], ['front', 'conflict', null, null]); assert.match(a.why, /front engraving chosen but the words are labelled back too/);
  assert.equal(REV('Tag1 (front engrave)', ['back - 5']).reviewKind, 'conflict');
  const bo = REV('Tag1 (back only)', ['front - W']); assert.deepEqual([bo.mode, bo.reviewKind, bo.front, bo.back], ['back', 'conflict', null, null]);
  const none = REV('Tag1 (no engraving)', ['front - W\nback - 5']); assert.deepEqual([none.mode, none.reviewKind, none.front, none.back], ['none', 'conflict', null, null]);
  const blank = eng('Bar', ['Anna'], { variations: [V('METAL - ENGRAVING', 'SILVER · Blank')] }); shapeOk(blank, 'blank'); assert.deepEqual([blank.mode, blank.source, blank.needsPerson, blank.reviewKind, blank.option], ['none', 'option', true, 'conflict', { name: 'METAL - ENGRAVING', value: 'SILVER · Blank' }], '"Blank" stays mode none, and the words that came with it are shown as a conflict');
  assert.equal(eng('Bar', [], { variations: [V('How Many Characters & Metal Color?', 'Blank- GOLD')] }), undefined, 'Blank with no words: nothing to read (the line has no engraving, as today)');
  const two = REV('Tag1 (front engrave)', ['W'], { variations: [V('Charm Type', 'Tag1 (front engrave)'), V('Engraving Side', 'Both'), V('Font Choice', 'Ace')] }); assert.equal(two.reviewKind, 'conflict'); assert.match(two.why, /two options disagree/);
});
await t('4e the front is never swapped for the back silently: every ambiguous reading leaves BOTH faces empty or exactly as labelled', () => {
  const bad = [];
  for (const form of FORMS) for (const [F, B] of TEXTS) for (const mut of [s => s, s => s.replace('front', 'back').replace('back', 'front'), s => s + ' extra']) {
    const pers = mut(form.replace('{F}', F).replace('{B}', B)), e = eng('Tag1 (front+back)', [pers]);
    if (!e.front && !e.back) continue;                                         // (a person decides: nothing was assigned)
    if (e.front && e.back && e.front.text === e.back.text && F !== B) bad.push(pers);
    if (e.front && e.front.text === B && e.front.text !== F) bad.push('swapped ' + JSON.stringify(pers));
    if (e.back && e.back.text === F && e.back.text !== B) bad.push('swapped ' + JSON.stringify(pers));
  }
  assert.deepEqual(bad.filter(x => /^swapped/.test(x) && !/replace/.test(x)).length, bad.filter(x => /^swapped/.test(x)).length);   // (see the next check for the label-swapped forms)
  // the two labels swapped in the text: the words follow the LABELS, so the words that were the front's are now the back's, never guessed the other way
  const sw = eng('Tag1 (front+back)', ['back - W\nfront - 5']); assert.deepEqual(faces(sw), ['both', '5', 'W']);
});
await t('4f nothing written at all, or only in a note: front engraving was chosen and nobody wrote the words -> the reading says so (needsPerson, "nowords"); engraveCandidate is NOT changed here (the faces model decides)', () => {
  for (const ct of ['Tag1 (front engrave)', 'Tag1 (front+back)']) {
    const sp = TAG(ct, []); assert.deepEqual([sp.engraving.needsPerson, sp.engraving.reviewKind, sp.engraving.textSource], [true, 'nowords', 'none'], ct); assert.equal(sp.engraveCandidate, false, 'the line is still cut plain as before: the intake touches nothing but spec.engraving'); assert.ok(plain1(sp.engraving.why));
    assert.deepEqual([Faces.setupOf({ spec: sp }).needsPerson, Faces.setupOf({ spec: sp }).faces.length > 0], [true, true], 'the faces model sees a line with faces chosen and no words');
  }
  const only = TAG('Tag1 (front engrave)', [], { buyerMessage: 'Please put an A on the front' }).engraving; assert.equal(only.reviewKind, 'texts'); assert.match(only.why, /only in a note/); assert.deepEqual([only.front, only.back], [null, null]);
  const both = TAG('Tag1 (front+back)', [], { buyerMessage: 'front: A, back: B' }).engraving; assert.match(both.why, /only in a note/); assert.deepEqual([both.front, both.back, both.needsPerson], [null, null, true], 'words only in the buyer\'s message are never taken as engraved words');
  assert.equal(TAG('Tag1 (back only)', []).engraveCandidate, false, 'a back-only line with no words is cut plain exactly as before');
});
await t('4g a handwriting listing "Gold Writing-FRONT" (the writing is a picture): a person handles it, it is not raised as "no words"; a drop-down named "Personalization" is still buyer text', () => {
  const sp = read({ title: 'Gold Bar Necklace, Handwritten Bar Necklace - YOUR HANDWRITING - or Image', sku: 'Gold_7478', variations: [V('Metal Choice', 'Gold Writing-FRONT'), V('Length', '20"')], personalization: [] });
  const e = sp.engraving; shapeOk(e, 'hand'); assert.deepEqual([e.mode, e.source, e.needsPerson, e.reviewKind, e.textSource, e.front], ['front', 'option', true, 'texts', 'none', null]); assert.match(e.why, /comes as a picture/);
  const p3 = eng('Personalization', ['Gold - FRONT & BACK\nI am going to send two different pictures if we could get one on each side that would be amazing. Thank you so much!']); assert.deepEqual([p3.mode, p3.needsPerson, p3.textSource], ['both', true, 'none'], 'audit P03: both faces, pictures, no words');
  const d = read({ variations: [{ name: 'Personalization', value: 'Gold - FRONT & BACK', valueId: '3456', propertyId: '514' }], personalization: ['Gold - FRONT & BACK'] });
  assert.deepEqual(d.personalization, ['Gold - FRONT & BACK'], 'a drop-down Etsy calls "Personalization" is still read as buyer text, exactly as today'); assert.deepEqual([d.engraving.mode, d.engraving.source, d.engraving.option && d.engraving.option.value], ['both', 'option', 'Gold - FRONT & BACK'], '... and it still says the mode (it has a value id: it is a drop-down)');
});
await t('4h the note asks for the front or both faces on a listing with no such option: read, listed as the buyer\'s own request (confidence 0.6, "a person confirms")', () => {
  for (const pers of [['front - W'], ['Front: A, Back: B']]) { const e = eng('Necklace CHARM', pers); assert.equal(e.source, 'note'); assert.equal(e.needsPerson, false); assert.equal(e.confidence, 0.6); assert.match(e.why, /a person confirms/); }
  const p = eng('Necklace CHARM', ['please engrave on the front: W']); assert.deepEqual([p.mode, p.source, p.front.text, p.needsPerson], ['front', 'note', 'W', false]);
  const dbl = eng('Necklace CHARM', ['I would like the charm to be double sided please.']); assert.deepEqual([dbl.mode, dbl.source, dbl.needsPerson, dbl.front, dbl.back], ['both', 'note', true, null, null], 'a bare "double sided" with no words: a person decides (audit P14)');
  assert.equal(eng('Necklace CHARM', ['Come back soon']), undefined, 'a sentence with the word back is no engraving word');
  const q = eng('Necklace CHARM', ['Can the initial be on the front and the date on the back?']); assert.deepEqual([q.mode, q.needsPerson, q.front, q.back], ['both', true, null, null], 'a question names the faces but gives no words (audit P23)');
});
await t('4i fee lines and no-design lines are never read as an engraving of their own', () => {
  assert.equal(read({ title: 'Re-work: Back Engraving fee', sku: 'Re_5460', personalization: ['front: A back: B'] }).engraving, undefined);
  assert.equal(read({ title: 'Modifications and Back Engraving', personalization: ['Back engraving: Anna'] }).engraving, undefined);
});

/* ═══ 5 · pairs and discs ═══ */
const PAIRLINE = (pers, opt) => read({ sku: 'EAR_1', listingId: '999', title: 'Stud Earrings, Personalized Charms', variations: [V('Metal Choice', 'Gold Filled'), V('Engraving', opt || 'Front&Back')], personalization: pers });
const DISCLINE = (pers, n) => read({ sku: 'INITIAL_DISC_4571', listingId: '888', title: 'Initial Disc Necklace', variations: [V('Metal Choice', 'Gold'), V('Discs', n + ' Disc'), V('Engraving', 'Front&Back')], personalization: pers });
await t('5a a front+back earring pair with one set of words: the same front and back on both ears; the Left / Right rule (2 pieces, L R) is untouched', () => {
  const sp = PAIRLINE(['front: A\nback: B']), e = sp.engraving; shapeOk(e, 'pair');
  assert.deepEqual([sp.pieceCount, sp.pair.kind, sp.pair.sides, e.front.text, e.back.text, e.pieces, e.needsPerson], [2, 'pair', ['L', 'R'], 'A', 'B', undefined, false]);
  const q2 = read({ sku: 'EAR_1', listingId: '999', title: 'Stud Earrings', quantity: 2, variations: [V('Engraving', 'Front&Back'), V('Metal Choice', 'Gold Filled')], personalization: ['front: A\nback: B'] }); assert.deepEqual([q2.pieceCount, q2.engraving.front.text], [4, 'A']);
});
await t('5b a front+back pair with words per ear: each ear\'s own front and back are read into `pieces` (additive); until the faces model reads them a person enters them, so the line asks one; same words per ear collapse to one set', () => {
  const e = PAIRLINE(['Left: front A back B\nRight: front C back D']).engraving; shapeOk(e, 'pieces');
  assert.deepEqual([e.front, e.back, e.needsPerson, e.reviewKind], [null, null, true, 'pieces']); assert.deepEqual(Object.keys(e.pieces), ['L', 'R']);
  assert.deepEqual([e.pieces.L.front.text, e.pieces.L.back.text, e.pieces.R.front.text, e.pieces.R.back.text], ['A', 'B', 'C', 'D']); assert.match(e.why, /left ear.*“A”.*“B”.*right ear.*“C”.*“D”/);
  assert.deepEqual(faces(PAIRLINE(['left ear: front A, back B\nright ear: front A, back B']).engraving), ['both', 'A', 'B'], 'the same words on both ears are the one set');
  const rl = PAIRLINE(['Right: front C back D\nLeft: front A back B']).engraving; assert.deepEqual([rl.pieces.L.front.text, rl.pieces.R.front.text], ['A', 'C'], 'either order: the words follow the ear named');
  noNested(PAIRLINE(['Left: front A back B\nRight: front C back D']), 'spec');
});
await t('5c a pair whose words name the ears but not the faces, or only one ear, goes to a person', () => {
  const a = PAIRLINE(['Left: A\nRight: B']).engraving; assert.equal(a.reviewKind, 'texts'); assert.match(a.why, /left ear do not say which is the front and which the back/);
  const b = PAIRLINE(['Left: front A back B']).engraving; assert.equal(b.reviewKind, 'texts'); assert.match(b.why, /not every ear or disc is named/);
  assert.equal(PAIRLINE(['W\n5']).engraving.reviewKind, 'texts');
});
await t('5d front-only earrings: each ear\'s own plain words are that ear\'s front', () => {
  const e = PAIRLINE(['Left: Anna\nRight: Ben'], 'Front engrave').engraving; assert.deepEqual([e.mode, e.reviewKind, e.pieces.L.front.text, e.pieces.R.front.text, e.pieces.L.back], ['front', 'pieces', 'Anna', 'Ben', null]);
  assert.deepEqual(faces(PAIRLINE(['Anna'], 'Front engrave').engraving), ['front', 'Anna', null], 'one name: both ears');
});
await t('5e counted discs: D1..Dn each with its own front and back', () => {
  const sp = DISCLINE(['Disc 1: front A back B\nDisc 2: front C back D\nDisc 3: front E back F'], 3), e = sp.engraving; shapeOk(e, 'discs');
  assert.deepEqual([sp.pieceCount, sp.pair.kind, e.reviewKind], [3, 'multi', 'pieces']);
  assert.deepEqual(Object.keys(e.pieces), ['D1', 'D2', 'D3']); assert.deepEqual([e.pieces.D2.front.text, e.pieces.D2.back.text, e.pieces.D3.back.text], ['C', 'D', 'F']);
  assert.equal(DISCLINE(['Disc 1: front A back B\nDisc 2: front C back D'], 3).engraving.reviewKind, 'texts', 'a disc with no words is not guessed');
  assert.deepEqual(faces(DISCLINE(['front: A\nback: B'], 2).engraving), ['both', 'A', 'B'], 'the same on every disc');
});

/* ═══ 6 · a person's saved word wins: the intake never writes it and never overwrites it ═══ */
const freeze = x => { if (x && typeof x === 'object') { Object.freeze(x); Object.values(x).forEach(freeze); } return x; };
const lineOf = pers => ({ variations: [V('Metal Choice', 'Gold Filled'), V('Charm Type', 'Tag1 (front+back)'), V('Font Choice', 'Ace')], personalization: pers });
await t('6a the intake\'s reading is derived data: it carries no person\'s word, is the same every time, and ignores anything a caller hands it about a person', () => {
  const a = read(lineOf(['front - W\nback - 1. 27. 24'])).engraving, b = read(lineOf(['front - W\nback - 1. 27. 24']), ctx({ engravingOverride: { mode: 'front', by: 'Seth' }, engravingEdits: { '4195966039_t1': { mode: 'front' } } })).engraving;
  assert.equal(JSON.stringify(a), JSON.stringify(b)); assert.notEqual(a.source, 'person'); assert.equal(a.by, undefined);
  const line = freeze(lineOf(['front - W\nback - 1. 27. 24'])); assert.doesNotThrow(() => O.engravingOf(line, freeze({ personalization: ['front - W\nback - 1. 27. 24'], font: { asked: 'Ace' } })), 'the inputs are never changed');
});
await t('6b the faces model puts a person\'s override (row.engravingOverride) over the intake\'s reading, and a later pull does not change it', () => {
  const ov = freeze({ mode: 'both', front: { text: 'W.', lines: ['W.'], fontWord: 'Vibur' }, back: { text: '1/27/24', lines: ['1/27/24'], fontWord: null }, by: 'Seth', at: 1791700000000 });
  const first = read(lineOf(['front - W\nback - 1. 27. 24'])), s1 = Faces.setupOf({ spec: first, engravingOverride: ov });
  assert.deepEqual([s1.source, s1.edited, s1.mode, s1.front.text, s1.front.fontWord, s1.back.text, s1.needsPerson], ['person', true, 'both', 'W.', 'Vibur', '1/27/24', false]);
  const later = read(lineOf(['front - X\nback - Y'])), s2 = Faces.setupOf({ spec: later, engravingOverride: ov });
  assert.deepEqual([s2.front.text, s2.back.text, s2.source], ['W.', '1/27/24', 'person'], 'the order is read again on a later pull: the person\'s words stay');
  const noOv = Faces.setupOf({ spec: later }); assert.deepEqual([noOv.front.text, noOv.back.text, noOv.source], ['X', 'Y', 'option'], 'without an override the intake\'s reading is what is used');
  const ambiguous = read(lineOf(['AB'])), fixed = Faces.setupOf({ spec: ambiguous, engravingOverride: freeze({ mode: 'both', front: 'A', back: 'B', by: 'Paul', at: 5 }) }); assert.deepEqual([fixed.needsPerson, fixed.front.text, fixed.back.text], [false, 'A', 'B'], 'a person settles an ambiguous line');
});
await t('6c the faces model reads the intake as intended: Paul\'s order seeds front W / back 1. 27. 24 with no paid reading; an ambiguous line seeds nothing and asks a person; back-only seeds nothing (the reader decides)', () => {
  const paul = Faces.setupOf({ spec: TAG('Tag1 (front+back)', ['front - W\nback - 1. 27. 24'], {}, 'Ace') });
  assert.deepEqual([paul.mode, paul.faces, paul.needsPerson, paul.multi], ['both', ['front', 'back'], false, true]);
  assert.deepEqual([Faces.seedFor(paul, 'front').lines, Faces.seedFor(paul, 'back').lines, Faces.seedFor(paul, 'front').how], [['W'], ['1. 27. 24'], 'labelled']);
  const amb = Faces.setupOf({ spec: TAG('Tag1 (front+back)', ['W\n1. 27. 24']) }); assert.deepEqual([amb.needsPerson, Faces.seedFor(amb, 'front'), Faces.seedFor(amb, 'back')], [true, null, null]); assert.match(amb.why, /do not say which go on the front/);
  const frontOnly = Faces.setupOf({ spec: TAG('Tag1 (front engrave)', ['Anna']) }); assert.deepEqual([frontOnly.faces, Faces.seedFor(frontOnly, 'front').lines, Faces.seedFor(frontOnly, 'back')], [['front'], ['Anna'], null]);
  const back = Faces.setupOf({ spec: TAG('Tag1 (back only)', ['Anna']) }); assert.deepEqual([back.faces, Faces.seedFor(back, 'back'), back.needsPerson], [['back'], null, false]);
  const conflict = Faces.setupOf({ spec: TAG('Tag1 (front engrave)', ['front - W\nback - 5']) }); assert.deepEqual([conflict.needsPerson, Faces.seedFor(conflict, 'front')], [true, null], 'a conflict seeds nothing: a person looks');
  const partial = Faces.setupOf({ spec: TAG('Tag1 (front+back)', ['front - W']) }); assert.deepEqual([partial.needsPerson, Faces.seedFor(partial, 'front').lines, Faces.seedFor(partial, 'back')], [true, ['W'], null], 'one face labelled: it is seeded, the other waits for a person');
  const plain = Faces.setupOf({ spec: read({ variations: [V('Metal Choice', 'Gold Filled')], personalization: ['Anna'] }) }); assert.deepEqual([plain.mode, plain.faces, plain.source, Faces.seedFor(plain, 'back')], ['back', ['back'], 'default', null], 'a line with no face word: today\'s default back, the reader');
});

/* ═══ 7 · golden: every line of the offline order data reads as before ═══ */
await t('7a all offline order data: 755 lines x 2 (as stored, and with one plain inscription): the spec is byte for byte what the module gives without the intake hook, apart from the new spec.engraving', () => {
  const withEng = new Set();
  for (const variant of ['plain', 'words']) {
    const L = W.lines(variant); assert.equal(L.length, 755, variant + ' line count');
    for (const x of L) {
      const sp = O.interpretLine(x.order, x.line, W.ctx()), base = BASE.interpretLine(x.order, x.line, W.ctx()), { engraving, ...rest } = sp;
      assert.equal(JSON.stringify(rest), JSON.stringify(base), `${variant} ${x.key}: the line reads as before`); assert.ok(!('engraving' in base));
      noNested(sp, x.key);
      if (engraving) { withEng.add(x.key); shapeOk(engraving, variant + ' ' + x.key); }
    }
  }
  assert.deepEqual([...withEng].sort(), ['font:12', 'font:14', 'snap:279', 'snap:330', 'snap:349'], 'exactly the lines with a front / back option gain a reading: Tag1 (front engrave) x2, Front&Back x2, Writing-FRONT; "Truck Front Charm" and the other 750 do not');
});
await t('7b the lines that gain a reading read as the data says', () => {
  const by = Object.fromEntries(['plain', 'words'].map(v => [v, Object.fromEntries(W.lines(v).map(x => [x.key, O.interpretLine(x.order, x.line, W.ctx()).engraving]))]));
  assert.deepEqual([by.words['snap:279'].mode, by.words['snap:279'].front.text, by.words['snap:279'].front.fontWord, by.words['snap:279'].needsPerson], ['front', 'ANNA', null, false], 'Tag1 (front engrave), font Vibur (the line\'s), one name: the front');
  assert.deepEqual([by.words['snap:330'].mode, by.words['snap:330'].reviewKind], ['both', 'texts'], '"SILVER · Front&Back" (Etsy\'s escaped &amp; read), one unlabeled word: a person');
  assert.deepEqual([by.plain['snap:330'].mode, by.plain['snap:330'].reviewKind], ['both', 'nowords']);
  assert.deepEqual([by.plain['snap:349'].mode, by.plain['snap:349'].reviewKind], ['front', 'texts']);
  const truck = W.lines('plain').find(x => /Truck Front Charm/.test(x.line.title)); assert.ok(truck, 'the snapshot holds the Truck Front Charm line'); assert.equal(by.plain[truck.key], undefined);
});
await t('7c lines with a face word in a title, a note or a sentence but no engraving word read as before (a deliberate set of look-alikes)', () => {
  const same = (line, why) => { const a = read(line); const { engraving, ...rest } = a; assert.equal(engraving, undefined, why); };
  same({ title: 'Back to School Pencil Charm', variations: [V('Metal Choice', 'Gold Filled')], personalization: ['Anna'] }, 'back to school');
  same({ title: 'Front Porch Charm', variations: [V('Metal Choice', 'Gold Filled')], personalization: ['Anna'] }, 'front porch');
  same({ variations: [V('Metal Choice', 'Gold Filled')], personalization: ['Come back soon'] }, 'a sentence');
  same({ variations: [V('Metal Choice', 'Gold Filled')], personalization: ['Mom'], buyerMessage: 'Can you send it to the front desk? Thanks, back at work Monday.' }, 'a message');
  same({ variations: [V('Metal Choice', 'Gold Filled'), V('Charm Type', 'Necklace CHARM')], personalization: ['Back Row Betty'] }, 'a name');
  same({ variations: [V('Would you like just 1 earring or a pair ?', 'PAIR')], personalization: ['Anna'] }, 'a pair');
  same({ variations: [V('Metal Choice', 'Gold Filled')], personalization: ['Please engrave "XO" on both charms'] }, 'both charms');
  same({ title: 'Double Axe Earring', variations: [V('Metal Choice', 'Gold Filled')], personalization: ['Anna'] }, 'double axe');
  same({ variations: [V('Metal Choice', 'Gold Filled'), V('Length', '16" - 1 side')], personalization: ['Anna'] }, '1 side names no face');
});
await t('7d the audit\'s note cases (ENGAUDIT-cases.json, 32 rows): each reads as the audit says, or as recorded here with the reason it differs on purpose', () => {
  let same = 0, differ = 0;
  for (const c of AUDIT.notes) {
    const sp = read({ variations: c.field ? [V(c.field, c.value)] : [], personalization: c.pers ? [c.pers] : [] }), e = sp.engraving;
    const got = e ? [e.mode, e.front && e.front.text || null, e.back && e.back.text || null, e.textSource, e.needsPerson] : ['none', null, null, 'none', false];
    if (e) shapeOk(e, c.id);
    if (!c.mine) { same++; assert.deepEqual([got[0], got[1], got[2], got[4]], [c.audit[0], c.audit[1], c.audit[2], c.audit[4]], c.id + ' as the audit says'); }
    else { differ++; assert.ok(c.why, c.id + ' says why'); assert.deepEqual(got, c.mine, c.id + ' as recorded (' + c.why + ')'); }
  }
  assert.equal(same + differ, 32); assert.ok(same >= 15, 'the intake agrees with the audit on ' + same + ' of 32');
});

/* ═══ 8 · the contract shape holds for every reading the tests make ═══ */
await t('8a every cell of a grid of options x notes gives a well-formed reading, never throws, and never fills a face the mode does not have', () => {
  const OPTS = [['Charm Type', 'Tag1 (front+back)'], ['Charm Type', 'Tag1 (front engrave)'], ['Charm Type', 'Tag1 (back only)'], ['Charm Type', 'Necklace CHARM'], ['Metal Choice', 'Gold Writing-FRONT'], ['METAL - ENGRAVING', 'GOLD · Blank'], ['METAL - ENGRAVING', 'GOLD · Front&Back'], ['Engraving', 'DOUBLE SIDED'], ['Engraving', '1 side']];
  const NOTES = [[], ['W'], ['front - W\nback - 5'], ['W\n5'], ['front: A back: B'], ['back: A'], ['front: A'], ['Left: front A back B\nRight: front C back D'], ['front: A\nfront: B'], ['Please engrave Anna on the front'], ['"Anna"'], ['pictures of my handwriting on both sides'], ['front:'], ['font: Vibur\nfront - A\nback - B'], ['A on the front and B on the back'], ['Hello\n\nthank you']];
  let n = 0;
  for (const [name, value] of OPTS) for (const pers of NOTES) for (const buyerMessage of ['', 'front: A, back: B', 'Thanks, back at work Monday']) {
    const sp = read({ variations: [V('Metal Choice', 'Gold Filled'), V(name, value)], personalization: pers, buyerMessage }); noNested(sp, 'spec');
    if (sp.engraving) { shapeOk(sp.engraving, `${name}=${value} ${JSON.stringify(pers)} ${buyerMessage}`); n++; }
  }
  assert.ok(n > 300, 'the grid produced ' + n + ' readings');
});

/* ═══ 9 · the intake is tolerant ═══ */
await t('9a a line the reader cannot make sense of never breaks the intake', () => {
  for (const line of [{ variations: null }, { variations: [{}, { name: null, value: null }] }, { personalization: [null, undefined, 5] }, { title: null }, { variations: [V('Front&Back', '')] }]) assert.doesNotThrow(() => read(line));
  // (the line reader itself has always refused a null variation, which Etsy never sends: the engraving reading alone must not add a failure of its own)
  assert.doesNotThrow(() => O.engravingOf({ variations: [null, {}, { name: null, value: null }], personalization: [null, 5] }, { personalization: [null] }));
  assert.equal(O.engravingOf(null, null), null); assert.equal(O.engravingOf({}, {}), null);
  assert.equal(JSON.stringify(read(lineOf(['front - W\nback - 5'])).engraving), JSON.stringify(read(lineOf(['front - W\nback - 5'])).engraving), 'the same line reads the same twice');
});

await t('9b the sorter page loads the new reader: its cache token for charm-nest-orders.js keeps every earlier suffix and holds mine', () => {
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), tok = (/charm-nest-orders\.js\?v=([^"]+)"/.exec(html) || [])[1] || '';
  assert.ok(/-fw2-as1-ms1-kl1/.test(tok) && /-ts1/.test(tok) && /-it1(?:-[a-z]+\d+)*$/.test(tok), tok);
});

console.log(`\n${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
})();
