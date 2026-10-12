// BFFONT (Paul, 12 Oct 2026, order 4200828980, personalisation "Typewriter font" / "Georgia Andy Margot Jaarsma" / "October 26 2020"): "Sometimes users will stipulate the font choice in the
// message, so the message has to be parsed for font choices, and that has to have some flexibility for wrongly spelled fonts or similar fonts to what we have in our repository that
// can be substituted in case of not exact match."
//   node tests/charm-nest/font-request.cjs        (no network, no Etsy, no AI, nothing live, no buyer data: every phrase below is made up in the style of the shop's real messages)
//  A · the table: one ordered preference table, every id installed, never the capitals-only font, the nearest of a style is the first installed
//  B · order 4200828980: the font line is a font (Typewriter -> Crimson Text, "from the message"), not words; the name Georgia is a name
//  C · 30 messy phrases that ARE font requests (spelling, case, spaces, plurals, other languages, kinds of font, fonts that are not ours)
//  D · 30 phrases that are NOT (names that look like fonts, places, finishes, colours, negations, questions, a bare name) - the words stay
//  E · taking the request out of the words: whole lines, edges of a short line, blank lines, a sentence stays whole
//  F · who wins: a person's face font, a font named for a face, the drop-down, the message, the listing's usual font
//  G · the engraved words of a front / back line, and the bridge (ensureJobFont, the reader's result)
//  I · the audit's cases: the words beside a font are never eaten, misspellings, traps, who wins
//  H · nothing else changes: all 1,510 offline lines read exactly as before (no new key on any), the page loads the new files
'use strict';
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const W = require('./_engrave-intake-world.cjs');
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; }, eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
const read = f => fs.readFileSync(path.join(root, f), 'utf8');

// ── A · the table ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
{
  const installed = new Set(O.ENGRAVING_FONTS.map(f => f.id));
  eq(O.FONT_STYLES.map(r => r.style), ['typewriter', 'brush', 'script', 'cursive', 'handwriting', 'blackletter', 'slab', 'serif', 'rounded', 'geometric', 'comic', 'bold', 'sans', 'vague'], 'one table, in priority order');
  for (const r of O.FONT_STYLES) {
    for (const id of r.prefer) { ok(installed.has(id), `${r.style}: ${id} is an installed font`); ok(!O.fontById(id).caps, `${r.style}: ${id} is not the capitals-only font (it would change the buyer's letters)`); }
    if (r.style !== 'vague') { ok(r.prefer.length >= 1 && r.label, `${r.style} has a label and at least one font`); eq(O.nearestFont(r.style), r.prefer[0], `${r.style}: the nearest is the first one installed`); }
    else eq(r.prefer, [], 'a taste (nice, pretty) picks nothing');
  }
  eq(['typewriter', 'script', 'cursive', 'handwriting', 'serif', 'sans', 'bold', 'rounded', 'comic', 'brush', 'slab', 'geometric', 'blackletter'].map(O.nearestFont),
    ['crimson-text', 'marck-script', 'playwrite-us-trad', 'mynerve', 'crimson-text', 'source-sans-3', 'libre-franklin', 'jost', 'source-sans-3', 'vibur', 'crimson-text', 'jost', 'crimson-text'], 'the nearest font of each kind');
  // the first font of a list that is not installed is skipped (a font that is removed or added is one edit of the table)
  const i = O.ENGRAVING_FONTS.findIndex(f => f.id === 'marck-script'), gone = O.ENGRAVING_FONTS.splice(i, 1)[0];
  try { eq([O.nearestFont('script'), O.nearestFont('blackletter')], ['playwrite-us-trad', 'crimson-text'], 'Marck Script not installed: the next one of the list'); } finally { O.ENGRAVING_FONTS.splice(i, 0, gone); }
  eq(O.nearestFont('nothing-like-it'), O.DEFAULT_FONT, 'an unknown kind is the usual font');
  // edit distance
  eq([['typewriter', 'typewritter', 1], ['typewriter', 'tipewriter', 1], ['pristina', 'pristine', 1], ['angelina', 'angelia', 1], ['stylish', 'stylsih', 1], ['abc', 'abd', 1], ['viber', 'vibur', 1], ['ace', 'ice', 1]].map(([a, b, d]) => O.damerau(a, b, 3) === d), Array(8).fill(true), 'Damerau-Levenshtein, a swap is one edit');
  ok(O.damerau('christina', 'pristina', 1) > 1 && O.damerau('typewriter', 'xyz', 2) > 2, 'far words stop early');
}

// ── B · order 4200828980 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
const PERS = 'Typewriter font\n\nGeorgia Andy Margot Jaarsma\nOctober 26 2020';
const MASTER = new Set(['BIRTHFLOWER_NECKLACE_DISC-9', 'MINI_0445', 'CURB']);
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: s => (MASTER.has(String(s || '').toUpperCase()) ? { sku: s } : null), masterLoose: () => '' }, extra || {});
const V = (name, value) => ({ name, value });
const spec = (o = {}) => O.interpretLine({ receiptId: o.rid || '4200828980', updateTs: 0 }, Object.assign({ transactionId: 't1', listingId: o.listingId || '1800000001', sku: o.sku || 'BIRTHFLOWER_NECKLACE_DISC-9', title: o.title || 'Birth Flower Necklace Bridesmaid Gift', quantity: 1, metalKey: 'gold', metalLabel: 'Gold',
  personalization: o.pers == null ? [PERS] : o.pers, buyerMessage: o.msg || '', staffNote: o.staff || '', variations: (o.vars || [['Birth Flower', 'Marigold'], ['Metal, Length & Sides', 'Gold - 16" - 2 sides']]).map(([a, b]) => V(a, b)) }), ctx(o.ctx));
{
  const r = O.fontReadText(PERS);
  eq(r.text, 'Georgia Andy Margot Jaarsma\nOctober 26 2020', 'the font line is gone, the blank line after it with it, the buyer\'s name and date are the words');
  eq(r.lines, ['Georgia Andy Margot Jaarsma', 'October 26 2020']);
  eq(r.requests.map(q => [q.asked, q.id, q.how, q.cue]), [['Typewriter', 'crimson-text', 'exact', 'font']], 'one request: Typewriter, the shop\'s own word');
  eq(r.requests.map(q => q.id), ['crimson-text'], 'Georgia is not read as a font');
  const sp = spec();
  eq([sp.fontText.asked, sp.fontText.id, sp.fontText.name, sp.fontText.how, sp.fontText.from], ['Typewriter', 'crimson-text', 'Crimson Text', 'exact', 'personalization'], 'spec.fontText');
  eq(sp.font, undefined, 'no drop-down font: spec.font is untouched');
  const pf = O.pieceFont(sp, null);
  eq([pf.id, pf.name, pf.asked, pf.mapped, pf.source, pf.via], ['crimson-text', 'Crimson Text', 'Typewriter', true, 'message', 'message'], 'the piece is engraved in the shop\'s Typewriter font (the same id as the drop-down word), source: message');
  eq(O.fontLine(sp, null), 'Font: Typewriter → Crimson Text (from the message)', 'the one plain line');
  ok(/font Typewriter → Crimson Text \(from the message\)/.test(sp.engraving.why) || !/font/.test(sp.engraving.why), 'the faces reading says it too when it has a font to say: ' + sp.engraving.why);
  eq(sp.engraving.raw, PERS, 'a person who must look sees the buyer\'s own words, unchanged');
  const same = O.pieceFont(spec({ pers: ['Anna'], vars: [['Font', 'Typewriter']] }), null);
  eq([same.id, same.source], ['crimson-text', 'rule:font'], 'the drop-down word Typewriter is the same font id (read as before)');
}

// ── C · phrases that ARE font requests: [text, installed font id, how, what the buyer asked] ───────────────────────────────────────────────
const YES = [
  ['Typewriter font', 'crimson-text', 'exact'], ['font: Pristina', 'marck-script', 'exact'], ['in Comic Sans please', 'source-sans-3', 'spelling'], ['use the cursive font', 'playwrite-us-trad', 'class'],
  ['Angelina script', 'mynerve', 'exact'], ['script font', 'marck-script', 'class'], ['font style: Stylish', 'playwrite-us-trad', 'exact'], ['Please use Vibur', 'vibur', 'exact'],
  ['Typewritter font', 'crimson-text', 'spelling'], ['Type writer font', 'crimson-text', 'exact'], ['Tipewriter font', 'crimson-text', 'spelling'], ['TYPEWRITE font', 'crimson-text', 'exact'], ['typwriter font', 'crimson-text', 'spelling'],
  ['Pristine font', 'marck-script', 'spelling'], ['Angelia font', 'mynerve', 'spelling'], ['Comic Sans MS font', 'source-sans-3', 'spelling'], ['font Comic Sans MS', 'source-sans-3', 'spelling'], ['Britanic Bold font', 'libre-franklin', 'spelling'],
  ['Viber font', 'vibur', 'spelling'], ['letra typewriter', 'crimson-text', 'exact'], ['Fuente: Pristina', 'marck-script', 'exact'], ['FONT - ANGELINA', 'mynerve', 'exact'], ['3. Typewriter Font', 'crimson-text', 'exact'],
  ['font: Courier New', 'crimson-text', 'near'], ['Georgia font', 'crimson-text', 'near'], ['in Times New Roman please', 'crimson-text', 'near'], ['Arial font', 'source-sans-3', 'near'], ['Impact font', 'libre-franklin', 'near'],
  ['Special Elite font', 'crimson-text', 'near'], ['in Brush Script', 'vibur', 'near'], ['Cooper Black font', 'libre-franklin', 'near'], ['handwriting style', 'mynerve', 'class'], ['Rounded font', 'jost', 'class'], ['a serif font', 'crimson-text', 'class'],
  ['clean, all-caps block font', 'libre-franklin', 'class'], ['calligraphic font', 'marck-script', 'class'], ['Cursive text', 'playwrite-us-trad', 'class'], ['the Pristina font please', 'marck-script', 'exact'], ['use font Vibur', 'vibur', 'exact'], ['Stylish font, centered', 'playwrite-us-trad', 'exact']
];
for (const [t, id, how] of YES) {
  const r = O.fontReadText(t), q = r.requests[0];
  ok(q && q.id === id && q.how === how, `“${t}” -> ${id} (${how}); got ${q ? q.id + ' ' + q.how : 'nothing'}`);
}
{
  eq(O.fontResolve('Typewriter font').id, 'crimson-text'); eq(O.fontResolve('Courier New').how, 'near'); eq(O.fontResolve('cursive').id, 'playwrite-us-trad'); eq(O.fontResolve('cursive').how, 'class');
  eq(O.fontResolve('Pristine').how, 'spelling'); eq(O.fontResolve('Pristina').how, 'exact'); eq(O.fontResolve('zzz'), { id: '', how: 'none', style: '', canon: '' }); eq(O.fontResolve('').id, ''); eq(O.fontResolve('nice').how, 'vague');
  // the seven real spellings of Typewriter, in any case, keep their stem reading
  for (const w of ['Typewriter', 'TYPEWRITE', 'TYPEWRITER', 'TypeWrit', 'Typewritr', 'TypeWrt', 'Typewrite', 'typewriter', 'Type-Writer']) eq(O.fontResolve(w).id, 'crimson-text', w);
  // every spelling of every shop word that FONTAUDIT saw
  for (const [w, id] of Object.entries({ Stylish: 'playwrite-us-trad', STYLISH: 'playwrite-us-trad', PRISTINA: 'marck-script', angelina: 'mynerve', COMIC: 'source-sans-3', Vibur: 'vibur', Ace: 'jost-ace', 'Britannic Bold': 'libre-franklin' })) eq(O.fontResolve(w).id, id, w);
  // a cursive class is not an exact match: the buyer's word stays beside the stand-in
  const sp = spec({ pers: ['Anna\nCursive text - Izzy'] });
  eq([sp.fontText.id, sp.fontText.how, sp.fontText.style], ['playwrite-us-trad', 'class', 'cursive']);
  eq(O.fontLine(sp, null), 'Requested font: Cursive (engraved in Playwrite US Trad, the nearest cursive font we have; from the message)', 'a stand-in is a request to look at, said with the buyer\'s own word');
  const co = spec({ pers: ['Emma\nfont: Courier New'], vars: [['Metal', 'Gold']] });
  eq(O.fontLine(co, null), 'Requested font: Courier New (engraved in Crimson Text, the nearest typewriter-style font we have; from the message)');
  const pf = O.pieceFont(co, null); eq([pf.id, pf.mapped, pf.substituted, pf.asked], ['crimson-text', false, true, 'Courier New'], 'not mapped (a person looks), but engraved in the stand-in; it never holds the order');
  const vg = spec({ pers: ['Lila\nin a nice pretty font'], vars: [['Metal', 'Gold']] });
  eq([vg.fontText.id, vg.fontText.how], ['', 'vague'], 'a taste picks no font'); eq(O.fontLine(vg, null), 'Requested font: nice pretty (engraved in Source Sans 3, the usual font; from the message)');
  const pl = spec({ pers: ['Lila\nPlain font please'], vars: [['Metal', 'Gold']] });
  eq(O.fontLine(pl, null), 'Font: Source Sans 3 (the usual font; the buyer asked for “Plain”, from the message)', 'a plain / normal font is the usual font, no flag');
}

// ── D · phrases that are NOT font requests ───────────────────────────────────────────────────────────────────────────────────────────────
const NO = [
  'Georgia Andy Margot Jaarsma', 'Courier', 'My name is Courier', 'Courier Smith', 'Ace', 'Ace Ventura', 'Angelina Jolie', 'in Georgia', 'Atlanta in Georgia', 'in Georgia, USA', 'in pristine condition',
  'in Typewriter City', 'in Comic Con', 'Comic Con 2020', '5 times', 'Times Square', 'Style: Anna', 'Fonts of Love', 'font size 12', 'darkened font', 'non-darkened font', 'black font', 'gold font',
  'No cursive pls', "don't use script", 'love script', 'Hollywood Script', "Anna's font", 'Grace font', 'Christina font', 'in bold', 'Courier New please', 'Old English', 'calligraphy', 'Elegant script',
  'Pristina', 'Typewriter', 'handwriting - will send image!', 'I will send photos for handwriting engraving', 'the writing "love papa" in his handwriting', 'my mother\'s handwriting for a necklace',
  'do not darken the font', 'What font is used in the video?', 'so i can approve the font, etc', 'I DONT WANT ANY INSCRIPTION', 'Georgia and Florida', 'Jack Ace'
];
for (const t of NO) { const r = O.fontReadText(t); ok(!r.requests.length && !r.changed && r.text === t, `“${t}” is no font request: ${JSON.stringify(r.requests)}`); }
{
  // asked of the shop or of a person, never decided
  for (const t of ['Mom in cursive?', 'Can you engrave a custom font?', 'Please use font #3 in all caps', 'font #2 for Korean']) { const r = O.fontReadText(t); ok(!r.changed && r.asks.length && r.text === t, `“${t}” is a question for a person`); }
  eq(O.fontReadText('Emma\nAngelina\n2020').asks.length, 1, 'Angelina alone on a line among other lines: a name or the font? ONE plain line asks, the words stay');
  eq(O.fontReadText('Emma\nAngelina\n2020').text, 'Emma\nAngelina\n2020'); eq(O.fontReadText('Emma\nAce\n2020').asks.length, 1);
  eq(O.fontReadText('Angelina').asks.length, 0, 'the only line is a name');
  const sp = spec({ pers: ['Emma\nAngelina\n2020'], vars: [['Metal', 'Gold']] }); eq([sp.fontText.id, sp.fontText.how, sp.fontText.asks.length], ['', 'ask', 1], 'carried on the line as a question, no font chosen'); eq(O.pieceFont(sp, null).id, 'source-sans-3');
  ok(spec({ pers: ['Georgia Andy Margot Jaarsma\nOctober 26 2020'] }).fontText === undefined, 'no font in the words: nothing is added to the line');
}

// ── E · taking the request out of the words ──────────────────────────────────────────────────────────────────────────────────────────────
{
  const t = (text, want, m) => eq(O.fontReadText(text).text, want, m || text);
  t('D font Stylish', 'D'); t('N Angelina font', 'N'); t('A; Angelina font', 'A', 'an initial before the font word stays'); t('A Angelina font', 'A'); t('I font Stylish', 'I'); t('D+R  in the Ace font', 'D+R');
  t('Evie in cursive please!', 'Evie'); t('Cursive text - Izzy', 'Izzy'); t('Anna & Tom in Pristina', 'Anna & Tom'); t('Sarah in cursive font', 'Sarah'); t('My Love in Typewriter font', 'My Love');
  t('Name: Emma\nFont: Angelina\nDate: 2020', 'Name: Emma\nDate: 2020'); t('Emma\nPristina\n2020', 'Emma\n2020', 'a font word alone on its own line, other lines carry the words');
  t('Typewriter font\n\nGeorgia Andy\n\nOctober 26 2020', 'Georgia Andy\n\nOctober 26 2020', 'the blank after the removed first line goes, the blank between the words stays');
  t('Georgia Andy\nTypewriter font\n\nOctober 26 2020', 'Georgia Andy\n\nOctober 26 2020'); t('Georgia Andy\n\nTypewriter font', 'Georgia Andy', 'a request at the end leaves no trailing blank');
  t('Pristina font\nFront: A\nBack: B', 'Front: A\nBack: B'); t('1. Butterfly symbol\n2. Centered\n3. Typewriter Font', '1. Butterfly symbol\n2. Centered', 'the list number goes with its line');
  t('Please use Vibur', ''); t('I want it in Typewriter font', ''); t('Font: As discussed in messages, Slab', '');
  t('a nice font', '', 'the article goes with a taste word');
  // inside a sentence the request is read and the sentence stays whole (the paid reader / a person settles it)
  for (const s of ['Please engrave Jane & Tom in Typewriter font', 'The name Alice in script', 'Please use a clean, all-caps block font so the text is sharp', 'I would like this engraved in a script font but I was not sure']) { const r = O.fontReadText(s); ok(r.requests.length && !r.changed && r.text === s, s); }
  // text with no request comes back unchanged, byte for byte (carriage returns too)
  for (const s of ['Anna\r\nTom\r\n', '', '  ', 'front - W\nback - 1. 27. 24', 'Hello!\n\nHappy birthday']) eq(O.fontReadText(s).text, s, JSON.stringify(s));
  eq(O.fontReadText(null).text, ''); eq(O.fontReadText(undefined).requests, []);
  eq(O.fontReadText('Ace font\nFront in Ace font: W\nBack: 5', { keepFaceLines: true }).text, 'Front in Ace font: W\nBack: 5', 'a face line that names its own font is left to the face reader');
  eq(O.fontReadText('Emma\nPristina\n2020', { cueOnly: true }).text, 'Emma\nPristina\n2020', 'the paid reader\'s own words: a bare font word is not removed');
  ok(O.fontReadText('x '.repeat(2000) + 'font').lines.length === 1, 'a very long line is read and ends');
}

// ── F · who wins ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
{
  // the drop-down's own font wins over the message; the message's other font is said, not applied
  const opt = spec({ pers: ['Anna\nPristina font'], vars: [['Font', 'Typewriter']] });
  eq([opt.font.id, opt.fontText.id, opt.fontText.differs], ['crimson-text', 'marck-script', 'Typewriter'], 'the option chose Typewriter, the message asks for Pristina: both on the line');
  eq([O.pieceFont(opt, null).id, O.pieceFont(opt, null).source], ['crimson-text', 'rule:font'], 'the drop-down wins; a person is told (differs)'); eq(O.fontLine(opt, null), 'Font: Typewriter → Crimson Text', 'an option font reads exactly as before');
  const agree = spec({ pers: ['Anna\nTypewriter font'], vars: [['Font', 'Typewriter']] }); eq(agree.fontText.differs, undefined, 'when they agree nothing is said');
  // the message wins over the listing's usual font (the Mini Tag listings: Vibur) - the buyer's word always wins
  const tag = spec({ rid: '4195966039', listingId: '4458930445', sku: 'MINI_0445', pers: ['W\nBritannic Bold font'], vars: [['Metal Choice', 'Gold Filled'], ['Charm Type', 'Tag1 (front engrave)']] });
  eq([tag.font.id, tag.font.listing, O.pieceFont(tag, null).id, O.pieceFont(tag, null).source], ['vibur', true, 'libre-franklin', 'message'], 'the listing\'s usual font (Vibur) is only the fallback');
  const tag2 = spec({ rid: '4195966039', listingId: '4458930445', sku: 'MINI_0445', pers: ['W'], vars: [['Metal Choice', 'Gold Filled'], ['Charm Type', 'Tag1 (front engrave)']] }); eq(O.pieceFont(tag2, null).id, 'vibur', 'no message font: the usual font as before');
  // a font named for a face (the person's setup or "Front: m - Vibur") outranks everything
  const both = spec({ pers: ['Anna\nTypewriter font'] }); both.engraving = { mode: 'both', front: { text: 'A', lines: ['A'], fontWord: 'Vibur' }, back: { text: 'B', lines: ['B'], fontWord: null } };
  eq(['front', 'back'].map(f => O.pieceFont(both, null, f).id), ['vibur', 'crimson-text'], 'the face with its own font word keeps it, the other takes the line\'s (the message)');
  // a person's saved word for a face (row.engravingOverride -> Faces.setupOf) wins
  const set = Faces.setupOf({ spec: both }, { mode: 'both', front: { text: 'A', lines: ['A'], fontWord: 'Pristina' }, back: { text: 'B', lines: ['B'], fontWord: 'Angelina' }, by: 'Ana', at: 1 });
  eq([set.front.fontWord, set.back.fontWord], ['Pristina', 'Angelina'], 'the person\'s words'); const sp2 = Object.assign({}, both, { engraving: { mode: 'both', front: set.front, back: set.back } });
  eq(['front', 'back'].map(f => O.pieceFont(sp2, null, f).id), ['marck-script', 'mynerve'], 'a person\'s saved choice always wins');
  // a piece with its own font (a font per disc) beats the line's message font
  const disc = spec({ pers: ['A\nPristina font'], vars: [['Metal', 'Gold']] }); disc.fontPieces = [{ id: 'vibur', asked: 'Vibur', source: 'rule:font' }];
  eq([O.pieceFont(disc, 0).id, O.pieceFont(disc, 1).id], ['vibur', 'marck-script'], 'a disc\'s own font, then the message');
  // order of the readings: the staff note, the personalisation box, the buyer's message
  eq(spec({ pers: ['Anna'], msg: 'Could you use Pristina?', staff: 'font: Vibur' }).fontText.id, 'vibur', 'the shop\'s own note first');
  const two = spec({ pers: ['Anna\nPristina font'], msg: 'Typewriter font please' }); eq([two.fontText.id, two.fontText.also], ['marck-script', ['Typewriter']], 'the box before the message; the other font is listed, not applied');
  eq(spec({ pers: ['Anna'], msg: 'Hello, I would like it in Typewriter font please. Thank you!' }).fontText.id, 'crimson-text', 'the buyer\'s message is read too');
  ok(spec({ pers: ['Anna'], vars: [['Font', 'Stylish']] }).fontText === undefined, 'an option font and no word in the text: no fontText at all');
}

// ── G · the faces reading and the bridge ─────────────────────────────────────────────────────────────────────────────────────────────────
{
  const tagv = [['Metal Choice', 'Gold Filled'], ['Charm Type', 'Tag1 (front+back)']], at = (pers, extra) => spec(Object.assign({ rid: '4195966039', listingId: '4458930445', sku: 'MINI_0445', pers: [pers], vars: tagv }, extra || {}));
  const a = at('front - W\nback - 5\nPristine font');
  eq([a.engraving.mode, a.engraving.front.text, a.engraving.back.text, a.engraving.needsPerson], ['both', 'W', '5', false], 'the font line is not an unlabelled word: the faces read cleanly');
  eq([a.engraving.front.fontWord, a.engraving.back.fontWord], [null, null], 'the font is the line\'s (spec.fontText), not repeated per face'); eq(O.pieceFont(a, null, 'front').id, 'marck-script');
  ok(/font Pristine → Marck Script \(from the message\)/.test(a.engraving.why), a.engraving.why);
  const b = at('front - W\nback - 5\nfont: Britannic Bold'); eq([b.engraving.front.fontWord, b.engraving.back.fontWord], ['Britannic Bold', 'Britannic Bold'], 'an exact "font: X" line is still the font of both faces, exactly as before');
  const c = at('front - W\nback - 5\nCourier New font'); eq([c.engraving.front.text, c.engraving.back.text, c.engraving.needsPerson, O.fontLine(c, null).slice(0, 38)], ['W', '5', false, 'Requested font: Courier New (engraved '], 'a font that is not ours on a front + back line');
  const d = at('Front in Ace font: W\nBack of tag in font Vibur: m'); eq([d.engraving.front.text, d.engraving.front.fontWord, d.engraving.back.text, d.engraving.back.fontWord], ['W', 'Ace', 'm', 'Vibur'], 'a face that names its own font is read as before');
}
{
  const src = read('charm-nest-bridge.js'), a = src.indexOf('  async function ensureJobFont(job) {'), b = src.indexOf('    return setOf(job);\n  }', a);
  ok(a > 0 && b > a, 'ensureJobFont is in the bridge');
  const run = async (job, loaded) => { const env = vm.createContext({ O, pieceIndexOf: () => null, loadFontSet: async () => ({ ok: loaded !== false }), setOf: () => ({}), Array }); vm.runInContext(src.slice(a, b) + '    return setOf(job);\n  }\nthis.ensureJobFont = ensureJobFont;', env); await env.ensureJobFont(job); return job; };
  (async () => {
    let j = await run({ row: { spec: spec() }, text: 'x', lines: ['x'] });
    eq([j.fontKey, j.fontName, j.fontMapped, j.fontFromNote, j.fontSubstituted, j.font.asked], ['crimson-text', 'Crimson Text', true, true, false, 'Typewriter'], 'order 4200828980: the piece is fitted in the Typewriter font');
    j = await run({ row: { spec: spec({ pers: ['Emma\nCourier New font'], vars: [['Metal', 'Gold']] }) }, text: 'x', lines: ['x'] });
    eq([j.fontKey, j.fontMapped, j.fontSubstituted, j.fontStyle, j.font.asked], ['crimson-text', false, true, 'typewriter', 'Courier New'], 'a font that is not ours: the nearest, flagged');
    j = await run({ row: { spec: {} }, text: 'x', lines: ['x'], requests: { side: 'back', font: 'Typewritter', handwriting: false, image: false } });
    eq([j.fontKey, j.fontMapped, j.fontSubstituted], ['crimson-text', true, false], 'a spelling the paid reader reported is the shop\'s font');
    j = await run({ row: { spec: {} }, text: 'x', lines: ['x'], requests: { side: 'back', font: 'cursive', handwriting: false, image: false } });
    eq([j.fontKey, j.fontMapped, j.fontSubstituted, j.fontStyle], ['playwrite-us-trad', false, true, 'cursive'], 'a kind of font the paid reader reported: the nearest, flagged');
    j = await run({ row: { spec: {} }, text: 'x', lines: ['x'], requests: { side: 'back', font: 'Papyrus', handwriting: false, image: false } });
    eq([j.fontKey, j.fontMapped, j.fontSubstituted], ['source-sans-3', false, false], 'a font nobody can place: the usual font, still "Requested font", never a hold');
    j = await run({ row: { spec: spec({ pers: ['Anna\nPristina font'], vars: [['Font', 'Typewriter']] }) }, text: 'x', lines: ['x'], requests: { side: 'back', font: 'Pristina', handwriting: false, image: false } });
    eq([j.fontKey, j.fontMapped], ['crimson-text', true], 'the drop-down still wins in the page');
    j = await run({ row: { spec: spec() }, text: 'x', lines: ['x'], fontKey: 'vibur', fontLocked: true, fit: {} });
    eq(j.fontKey, 'vibur', 'a saved back reopened keeps the font it has');
    j = await run({ row: { spec: spec() }, text: 'x', lines: ['x'], faceFontWord: 'Angelina' }); eq(j.fontKey, 'mynerve', 'a font named for the face outranks the message');
    // the reader's result and the fallback take a font line out of the words (source shape)
    ok(/O\.fontReadText\(job\.lines\.join\("\\n"\), \{ cueOnly: true \}\)/.test(src) && /O\.fontReadText\(sp\.personalization\.join\("\\n"\), \{ cueOnly: true \}\)/.test(src), 'the paid reader\'s result and the "reader unavailable" proposal drop a font line');
    ok(/O\.fontResolve\(job\.requests\.font\)/.test(src) && /p\.via !== "message"/.test(src), 'ensureJobFont reads a requested font with the tolerant reader and never over a message font');
    console.log(`font-request G (bridge): done`);
  })().catch(e => { console.error(e); process.exitCode = 1; });
}

// ── I · the audit's cases (BFAUDIT, 12 Oct 2026): the words next to a font are never eaten; every plain listing reads a font; misspellings; traps; who wins ─────────────────────────────────
{
  const front = (pers, extra) => spec(Object.assign({ rid: '4070410000', listingId: '4458930445', sku: 'MINI_0445', pers: [pers], vars: [['Metal Choice', 'Gold Filled'], ['Charm Type', 'Tag1 (front only)']] }, extra || {}));
  // the real bug: "D+R in the Ace font" took the engraving words with it (front left empty, nothing held)
  const a = front('D+R  in the Ace font');
  eq([a.engraving.mode, a.engraving.front && a.engraving.front.text, a.engraving.needsPerson, a.fontText.id, a.fontText.asked], ['front', 'D+R', false, 'jost-ace', 'Ace'], 'D+R stays the front words; Ace is the font');
  eq(O.pieceFont(a, null).id, 'jost-ace');
  const m = front('please engrave “MC” in the uppercase Ace font');
  eq([m.engraving.front && m.engraving.front.text, m.engraving.needsPerson, m.fontText.id], ['MC', false, 'jost-ace'], 'the quoted letters are the words, the font is the font');
  const sentence = front('please write Anna Marie in typewriter font');
  ok(sentence.engraving.needsPerson || !/typewriter font/i.test(String(sentence.engraving.front && sentence.engraving.front.text)), 'a sentence is never engraved with its font words: a person writes exactly what is engraved');
  for (const t of ['D+R in the Ace font', 'W  in Ace font', 'MC - Ace font', 'Anna & Tom in the Pristina font']) { const e = front(t).engraving; ok(e.front && e.front.text && !/font|\bace\b|pristina/i.test(e.front.text) && !e.needsPerson, `“${t}”: the words stay, the font goes: ${JSON.stringify(e.front)}`); }
  // "Font Angelina" on its own line, no colon, among labelled faces: not fused into the back words
  const fa = spec({ rid: '3882129810', listingId: '4458930445', sku: 'MINI_0445', pers: ['Front I love you\nBack Mom\nFont Angelina'], vars: [['Metal Choice', 'Gold Filled'], ['Charm Type', 'Tag1 (front+back)']] });
  eq([fa.engraving.front.text, fa.engraving.back.text, fa.fontText.id, fa.engraving.needsPerson], ['I love you', 'Mom', 'mynerve', false], '"Font Angelina": a font line, not back words');
  // a plain listing with no face mode at all reads the font as well (these lines used to be left to the paid reader)
  const plain = t => spec({ rid: '3885340903', listingId: '1838555907', sku: 'CURB', pers: [t], vars: [['Metal', 'Gold']] });
  for (const [t, id] of [['D font Stylish', 'playwrite-us-trad'], ['N Angelina font', 'mynerve'], ['A; Angelina font', 'mynerve'], ['AB\nFont: ACE', 'jost-ace'], ['Angelina Style', 'mynerve'], ['Emma\nStylish font', 'playwrite-us-trad']]) eq([t, plain(t).fontText && plain(t).fontText.id], [t, id]);
  eq(O.fontReadText('D font Stylish').text, 'D'); eq(O.fontReadText('N Angelina font').text, 'N'); eq(O.fontReadText('A; Angelina font').text, 'A'); eq(O.fontReadText('Angelina Style').text, '');
  // misspellings of every shop font seen or plausible: <x> font, and the bare phrase of the paid reader
  const MIS = { 'Typwriter': 'crimson-text', 'Typwritter': 'crimson-text', 'Typerwriter': 'crimson-text', 'Type writer': 'crimson-text', 'Tipewriter': 'crimson-text', 'Angelena': 'mynerve', 'Anjelina': 'mynerve', 'Angelia': 'mynerve', 'Prestina': 'marck-script', 'Pristna': 'marck-script',
    'Pristine': 'marck-script', 'Stylsh': 'playwrite-us-trad', 'Stilish': 'playwrite-us-trad', 'Comick': 'source-sans-3', 'Comic Sans': 'source-sans-3', 'Brittanic Bold': 'libre-franklin', 'Britanic Bold': 'libre-franklin', 'Viber': 'vibur', 'Vibbur': 'vibur',
    'Sarif': 'crimson-text', 'Serrif': 'crimson-text', 'Cursiv': 'playwrite-us-trad', 'Calligrafy': 'marck-script', 'Scipt': 'marck-script' };
  for (const [w, id] of Object.entries(MIS)) { const q = O.fontReadText(`${w} font`).requests[0]; ok(q && q.id === id, `“${w} font” -> ${id}; got ${q ? q.id : 'nothing'}`); ok(O.fontResolve(w).id === id, `fontResolve(“${w}”) -> ${id}`); }
  // traps from the audit: a first name, a place, a clasp, a finish, an instruction, a word that only looks like a font
  const TRAP = ['Georgia Andy Margot Jaarsma\nOctober 26 2020', 'Georgia', 'Angelina Mae', 'Angelina Mae\nOctober 26 2020', 'Grace in Ace', 'Jack Ace', 'ANGELINA LOWER CASE g', 'lobster clasp please', 'lobster charm', 'I DONT WANT ANY INSCRIPTION', 'darkened font', 'non-darkened font',
    'do not darken the font', 'please DO NOT darken the font', 'No cursive pls!', 'her handwriting', 'handwritten heart for back of the disc', 'in the handwriting I’ll email', 'I will send message with handwriting photo', 'Stylish', 'a stylish gift', 'Comic book fan',
    'two times', 'paw print', 'fingerprint', 'so I can approve the font', '+engrave', 'What font is used in the video with the love mom', 'Is there any way you can engrave a custom font?'];
  for (const t of TRAP) { const r = O.fontReadText(t); ok(!r.changed && r.text === t && !r.requests.filter(q => q.id).length, `“${t}” is not a font: ${JSON.stringify(r.requests)}`); }
  eq(O.fontReadText('Normal clean font. No cursive pls!').requests.map(q => [q.id, q.how]), [['source-sans-3', 'usual']], 'the plain font is asked for, the cursive is not');
  ok(spec({ pers: ['Georgia Andy Margot Jaarsma\nOctober 26 2020'] }).fontText === undefined, 'Georgia is a name here');
  eq(spec({ pers: ['Anna\nGeorgia font'], vars: [['Metal', 'Gold']] }).fontText.id, 'crimson-text', 'and a font there (Georgia font: the nearest serif)');
  // who wins: the drop-down (what the buyer bought) over the message; the message says so, never a hold
  const dd = spec({ pers: ['Front: A\nBack: B', 'in a script font please'], rid: '4192839059', listingId: '4458930445', sku: 'MINI_0445', vars: [['Metal Choice', 'Gold Filled'], ['Charm Type', 'Tag1 (front+back)'], ['Font', 'Typewriter']] });
  eq([O.pieceFont(dd, null).id, dd.engraving.needsPerson], ['crimson-text', false], 'the drop-down font is engraved, nothing is held');
  ok(/Buyer's message also asks for script \(the font option wins\)/.test(dd.engraving.why), dd.engraving.why);
  eq(O.fontDisagree(dd), "Buyer's message also asks for script; the font option chose Typewriter and that is what is engraved");
  eq(O.fontDisagree(spec({ pers: ['Anna\nTypewriter font'], vars: [['Font', 'Typewriter']] })), '', 'the same font twice is no disagreement');
  const same = spec({ pers: ['Anna\nStylish font'], vars: [['Metal', 'Gold'], ['Font', 'Stylish']] }); eq([O.pieceFont(same, null).id, same.fontText.differs], ['playwrite-us-trad', undefined]);
  const src = read('charm-nest-orders.js'); ok(/const FONT_MESSAGE_OVER_OPTION = false;/.test(src), 'the one place that decides who wins, default: the drop-down');
}

// ── H · nothing else changes ────────────────────────────────────────────────────────────────────────────────────────────────────────────
{
  let lines = 0, gained = 0, withFont = 0;
  for (const variant of ['plain', 'words']) for (const x of W.lines(variant)) {
    const sp = O.interpretLine(x.order, x.line, W.ctx()); lines++;
    if (sp.fontText) gained++;
    if (sp.font) withFont++;
  }
  eq([lines, gained], [1510, 0], 'no line of the offline order data gains a font reading: they read exactly as before'); ok(withFont > 100, 'and the lines with a font option are among them: ' + withFont);
  const html = read('charm-nest-1.html'), tok = f => (new RegExp(f.replace('.', '\\.') + '\\?v=([^"]+)"').exec(html) || [])[1] || '';
  ok(/-ef1(?:-[a-z]+\d+)*-bf1(?:-[a-z]+\d+)*$/.test(tok('charm-nest-orders.js')) || /-ef1-bf1/.test(tok('charm-nest-orders.js')), 'the orders token keeps its history and holds -bf1: ' + tok('charm-nest-orders.js').slice(-30));
  ok(/-su1(?:-[a-z]+\d+)*-bf1/.test(tok('charm-nest-engrave-setup.js')) || /-su1-bf1/.test(tok('charm-nest-engrave-setup.js')), 'the setup token holds -bf1');
  ok(/-lrt1(?:-[a-z]+\d+)*-bf1|-bf1/.test(tok('charm-nest-bridge.js')) && /-sui1/.test(tok('charm-nest-bridge.js')), 'the bridge token keeps its history and holds -bf1');
  ok(['fontReadText', 'fontScanLine', 'fontTextOf', 'fontResolve', 'nearestFont', 'FONT_STYLES'].every(k => k in O), 'exports');
}
process.on('exit', () => { if (!process.exitCode) console.log(`font-request: ${n} checks passed`); });
