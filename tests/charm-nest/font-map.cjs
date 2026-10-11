// FONTMAP / VIBURFONT (Paul, 10 Oct 2026, 16:03-16:13 UTC): "find similar versions that are open source to the options we offer and then map those options. Anytime you see font
// choices, chosen by the user from a drop-down on all listings this has to be a check for every single purchase listing." and, for the three Mini Tag listings (4458930445,
// 1714117116 "Tiny Initial Tag 1" and "Tiny Initial Tag 1-CO"): "ignore the Font choice and just find and install a similar looking font".
//   node tests/charm-nest/font-map.cjs        (no network, no Etsy, no AI, nothing live; one small worker thread)
// Real code, real data: the real modules, the 17 real order lines of the Sep 17 snapshot (fixtures/font-option-lines.json), EVERY font-bearing option value of the 49 listings with a font
// drop-down (fixtures/font-audit-values.json, read from 8,500 real orders by FONTAUDIT: names and values only), and the real master design TINY INITIAL TAG 1 (fixtures/TINY_INITIAL_TAG_1.ai).
//  A · the fonts: files, licences, glyphs, the registry
//  B · the words: all 8 font words, every spelling seen, the option name is not needed, "No Font" is no font, an unknown word never holds
//  C · the real lines: the three Mini Tag lines read with no font question, every other listing keeps its own font
//  E · ENGFONT (11 Oct 2026, Paul: "double check that you've correctly identified the font choices ... identify open source versions of each offered font"): the Mini Tag family's three words
//      are each matched to the shop's own chart (Vibur -> Vibur itself, Ace -> Jost with the slashed zero + capitals only, Britannic Bold -> Libre Franklin), the BUYER'S word always wins
//      (the FONT_LISTINGS one-font rule is only the fallback for a line with no font word), every spelling, both faces of a front + back line, the capitals rule, the slashed zero
//  D · the engraving: "C." (and the other tag words) typeset in the tag font on the real tag, inside it; the weight rule; the worker; the saved back reopens in its font
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), vm = require('vm');
const { Worker } = require('worker_threads');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js')), G = require(path.join(root, 'charm-nest-geom.js')), Fit = require(path.join(root, 'charm-nest-engrave-fit.js'));
const T = require(path.join(root, 'charm-nest-text.js')), Backs = require(path.join(root, 'charm-nest-backs.js'));
const ot = require(path.join(root, 'vendor/opentype-1.3.4.min.js'));
const { CharmNestPDF: P } = require(path.join(root, 'netlify/functions/_charmNestPdf.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const bytes = f => { const b = fs.readFileSync(path.join(root, f)); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };

const WORDS = { Stylish: 'playwrite-us-trad', Pristina: 'marck-script', Angelina: 'mynerve', Typewriter: 'crimson-text', Comic: 'source-sans-3', Vibur: 'vibur', Ace: 'jost-ace', 'Britannic Bold': 'libre-franklin' };
const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/font-option-lines.json'), 'utf8')).lines;
const AUDIT = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/font-audit-values.json'), 'utf8'));

// ── A · the fonts ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
const emoji = ot.parse(bytes('vendor/fonts/NotoEmoji-Regular.ttf')), emojiMap = require(path.join(root, 'vendor/fonts/emoji-sequences.json'));
const sets = {};   // id → { Regular, Semibold? } as the page wraps them
for (const f of O.ENGRAVING_FONTS) {
  sets[f.id] = {};
  for (const [weight, file] of Object.entries(f.files)) {
    ok(fs.existsSync(path.join(root, file)), `${f.name}: ${file} is in the repository`);
    const font = ot.parse(bytes(file));
    for (const ch of 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.&-\'') ok(font.charToGlyphIndex(ch) > 0, `${f.name} ${weight} has "${ch}"`);
    sets[f.id][weight] = T.withEmoji(font, emoji, emojiMap, ot.Path);
  }
  if (f.id !== 'source-sans-3') {
    const lic = path.join(root, 'vendor/fonts', path.basename(f.files.Regular).replace(/-[A-Za-z]+\.ttf$/, '-OFL.txt'));
    ok(fs.existsSync(lic) && /SIL OPEN FONT LICENSE Version 1\.1/.test(fs.readFileSync(lic, 'utf8')), `${f.name}: its SIL Open Font License text sits beside the file`);
    ok(/Copyright/.test(fs.readFileSync(lic, 'utf8')), `${f.name}: and names its authors`);
  }
}
{ const build = read('scripts/build-public.cjs');
  for (const f of O.ENGRAVING_FONTS) if (f.id !== 'source-sans-3') ok(build.includes('vendor/fonts/' + path.basename(f.files.Regular).replace(/-[A-Za-z]+\.ttf$/, '-OFL.txt')), `${f.name}: the licence is served with the font`);
  const files = new Set(fs.readdirSync(path.join(root, 'vendor/fonts')));
  for (const f of O.ENGRAVING_FONTS) for (const file of Object.values(f.files)) ok(files.has(path.basename(file)), `${file} is copied by the build (every .otf/.ttf of vendor/fonts)`); }
eq(O.ENGRAVING_FONTS.map(f => f.id), ['source-sans-3', 'playwrite-us-trad', 'marck-script', 'mynerve', 'crimson-text', 'playwrite-us-modern', 'jost', 'alegreya-sans', 'vibur', 'jost-ace', 'libre-franklin'], 'Source Sans 3 and ten others (the first match of Vibur / Ace / Britannic Bold stay installed: a saved back may name them)');
eq(O.ENGRAVING_FONTS.filter(f => !f.files.Semibold).map(f => f.id), ['playwrite-us-trad', 'marck-script', 'mynerve', 'playwrite-us-modern', 'vibur'], 'the five scripts have ONE weight; the others have a heavier cut for the small sizes');
eq(O.ENGRAVING_FONTS.filter(f => f.caps).map(f => f.id), ['jost-ace'], 'only the Ace font is a capitals-only font');
for (const g of O.FONT_WORDS) ok(O.fontById(g.id), `${g.words[0]} maps to an installed font`);

// ── B · the words ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
{ // 1. every font-bearing option value of the 49 listings, read by VALUE whatever the option is called (22 option names carry a font)
  let withFont = 0, none = 0; const names = new Set();
  for (const r of AUDIT.rows) {
    const fr = O.fontRead(r.name, r.value), fh = fr ? null : O.fontHidden(r.name, r.value), fm = O.fontInMetal(r.name, r.value), got = (fr && fr.id) || (fh && fh.id) || (fm && fm.id) || '';
    if (r.word) { eq(got, WORDS[r.word], `listing ${r.listing} "${r.name}": ${r.value} -> ${r.word}`); withFont++; names.add(r.name); }
    else { eq(got, '', `listing ${r.listing} "${r.name}": ${r.value} carries no font`); none++; }
  }
  eq([withFont, none], [183, 8], '183 font values (all 8 words) and 8 values with no font (No Font, Silver • Custom)');
  ok(names.size >= 20, `${names.size} option names carry a font: all read`);
  eq([...new Set(AUDIT.rows.filter(r => r.word).map(r => r.word))].sort(), Object.keys(WORDS).sort(), 'the 8 words of the shop'); }
{ // 2. the seven spellings of Typewriter, case aside, and the other words in any case
  for (const w of ['Typewriter', 'TYPEWRITE', 'TYPEWRITER', 'TypeWrit', 'Typewrite', 'Typewritr', 'TypeWrt', 'typewriter', 'Type Writer', 'type-writer']) eq(O.fontOfWord(w), 'crimson-text', `Typewriter: ${w}`);
  for (const [w, id] of [['STYLISH', 'playwrite-us-trad'], ['stylish', 'playwrite-us-trad'], ['PRISTINA', 'marck-script'], ['ANGELINA', 'mynerve'], ['angelina', 'mynerve'], ['COMIC', 'source-sans-3'], ['Vibur', 'vibur'], ['Ace', 'jost-ace'], ['Britannic Bold', 'libre-franklin']]) eq(O.fontOfWord(w), id, w);
  eq([O.fontOfWord('Typed'), O.fontOfWord('Type'), O.fontOfWord('Wrist'), O.fontOfWord('Pace'), O.fontOfWord('Place'), O.fontOfWord('Face')], ['', '', '', '', '', ''], 'a word that merely looks like a font word is none');
  eq(O.fontOfPhrase('Typewriter font'), 'crimson-text', 'the word inside a phrase'); eq(O.fontOfPhrase('Ace of hearts font'), 'jost-ace', 'Ace in a font option reads as Ace'); }
{ // 3. "No Font" and "Silver • Custom" are not fonts: nothing asked, nothing held, the line engraves in the app's own font
  for (const [name, value] of [['Necklace Length', '18" ·NO FONT·'], ['Chain Length', '18"·No Font'], ['Bracelet Length/Font Choice', '9"/No Font'], ['Necklace Length and Font', '14" ·NO FONT·']]) {
    eq([O.fontRead(name, value), O.fontHidden(name, value)], [null, null], `${name}: ${value} carries no font (and stays the length it was)`);
    const sp = O.interpretLine({ receiptId: '1', updateTs: 0 }, { transactionId: '2', listingId: '1017622821', sku: 'CURB', title: 't', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [{ name, value }] }, { optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {} });
    ok(!sp.font && !sp.problems.some(p => p.kind === 'needsMapping'), `${value}: no font, no question`);
  }
  const named = O.fontRead('Font Choice', 'No Font'); eq([named.asked, named.id, named.none], ['', '', true], 'an option NAMED for a font whose value is No Font asks for none'); eq(O.fontLine({ font: { asked: '', id: '', name: '' } }), '', 'and the card says nothing about it');
  eq(O.fontInMetal('Metal? Font Choice?', 'Silver • Custom'), null, 'Silver • Custom is a custom font the buyer writes in a note: nothing to map');
  eq(O.fontInMetal('Metal? Font Choice?', 'Gold • Typewriter').id, 'crimson-text', 'Metal? Font Choice?: Gold • Typewriter'); }
{ // 4. a word the shop does not offer never holds a line: Source Sans 3, "Requested font: X"
  const sp = O.interpretLine({ receiptId: '1', updateTs: 0 }, { transactionId: '2', listingId: '1', sku: 'A', title: 't', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [{ name: 'Font', value: 'Cooper Black' }] }, { optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {} });
  eq(sp.problems.filter(p => p.kind === 'needsMapping'), [], 'Cooper Black: no question');
  eq([sp.font.asked, sp.font.id, O.pieceFont(sp).name, O.pieceFont(sp).mapped], ['Cooper Black', '', 'Source Sans 3', false], 'engraved in Source Sans 3');
  eq(O.fontLine(sp), 'Requested font: Cooper Black (engraved in Source Sans 3)', 'and the card says so'); }

// ── C · the real lines ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
const lineOf = x => ({ transactionId: x.tid || '1', listingId: x.listingId, sku: x.sku, title: x.title || 't', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: x.vars.map(([name, value]) => ({ name, value })) });
const MASTER = new Set(['TINY INITIAL TAG 1', 'MINI_0445', 'INITIAL_8391', 'INITIAL_DISC_4571', 'CURB', 'BEADY']);
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: s => MASTER.has(String(s).toUpperCase()) ? {} : null, masterLoose: () => '' }, extra);
const interpret = (x, c) => O.interpretLine({ receiptId: x.rid, updateTs: 0 }, lineOf(x), c || ctx());
const asks = sp => sp.problems.filter(p => p.kind === 'needsMapping');
{ const tagLines = FIX.filter(x => ['4458930445', '1714117116'].includes(x.listingId));
  eq(tagLines.map(x => x.sku), ['Tiny Initial Tag 1-CO', 'Tiny Initial Tag 1', ''], 'the three real lines: "Tiny Initial Tag 1-CO", "Tiny Initial Tag 1" and the listing with no SKU');
  // ENGFONT: the buyer's own word reaches the engraving (these three lines all say Vibur) and the font is Vibur itself; no flag turns it into something else
  for (const hold of [false, true]) { const keep = O.FONT_RULES.unknownHolds; O.FONT_RULES.unknownHolds = hold; try {
    for (const x of tagLines) {
      const sp = interpret(x), fontOpt = sp.options.find(o => /font/i.test(o.name));
      eq([sp.font.id, sp.font.name, sp.font.source, !!sp.font.listing, sp.font.asked], ['vibur', 'Vibur', 'rule:font', false, 'Vibur'], `${x.rid} ${x.sku || '(no SKU)'}: the buyer's Vibur is Vibur (the switch ${hold ? 'on' : 'off'})`);
      eq([fontOpt.mapped.field, fontOpt.mapped.value, fontOpt.mapped.source], ['font', 'vibur', 'rule:font'], 'the Font Choice option answers itself with the buyer\'s word');
      eq(asks(sp).filter(p => /font/i.test(p.optionName) || p.font), [], 'no question about the font');
    } } finally { O.FONT_RULES.unknownHolds = keep; } }
  // the buyer's pick is followed on every listing of the family, whatever the font
  for (const [w, id] of [['Ace', 'jost-ace'], ['Britannic Bold', 'libre-franklin'], ['Vibur', 'vibur']]) for (const [lid, sku] of [['4458930445', ''], ['4458930445', 'Mini_0445'], ['1714117116', 'Tiny Initial Tag 1'], ['1714117116', 'Tiny Initial Tag 1-CO'], ['1714112618', 'Tiny Initial Tag 3'], ['1714117068', 'Tiny Initial Tag 4']]) {
    const sp = interpret({ rid: '1', listingId: lid, sku, vars: [['Metal Choice', 'Gold Filled'], ['Font Choice', w]] });
    eq([sp.font.id, !!sp.font.listing, asks(sp).filter(p => /font/i.test(p.optionName)).length], [id, false, 0], `${lid} ${sku || '(no SKU)'}: ${w} -> ${id} (the buyer's choice, never the listing's)`);
  }
  // the listing's usual font (Vibur, FONT_LISTINGS) is ONLY the fallback: no font option at all, "No Font", or a word the shop does not offer
  for (const [label, vars] of [['no font option', [['Metal Choice', 'Gold Filled']]], ['No Font', [['Font Choice', 'No Font']]], ['a word the shop does not offer', [['Font Choice', 'Brush Script']]]]) for (const [lid, sku] of [['4458930445', ''], ['1714117116', 'Tiny Initial Tag 1-CO']]) {
    const sp = interpret({ rid: '1', listingId: lid, sku, vars });
    eq([sp.font.id, !!sp.font.listing, sp.font.source, asks(sp).filter(p => /font/i.test(p.optionName)).length], ['vibur', true, 'rule:font-listing', 0], `${lid}: ${label} -> the listing's usual font, no question, never a hold`);
  }
  { const none = interpret({ rid: '1', listingId: '4458930445', sku: '', vars: [['Metal Choice', 'Gold Filled']] }); eq(O.fontLine(none), 'Font: Vibur (the usual font of this listing: the buyer named none)', 'the card says it is the listing\'s usual font'); }
  { const sp = interpret({ rid: '1', listingId: '4458930445', sku: '', vars: [['Font Choice', 'Brush Script']] }); eq([O.pieceFont(sp).mapped, O.fontLine(sp)], [false, 'Requested font: Brush Script (engraved in Vibur, the usual font of this listing)'], 'a font the shop does not offer is flagged "Requested font", engraved in the listing\'s usual font'); }
  // a listing that is not in the family has no usual font: no font word, no font (Source Sans 3)
  eq(interpret({ rid: '1', listingId: '999', sku: 'A', vars: [['Metal Choice', 'Gold Filled']] }).font, undefined, 'another listing with no font word has no font at all');
  // by SKU alone as well (any spelling of case): the SKU names the listing for the fallback; the word still wins
  eq(interpret({ rid: '1', listingId: '999', sku: 'tiny initial tag 1-co', vars: [['Font Choice', 'Ace']] }).font.id, 'jost-ace', 'the SKU names the listing; the buyer\'s Ace still wins');
  eq(interpret({ rid: '1', listingId: '999', sku: 'tiny initial tag 1-co', vars: [['Metal Choice', 'Gold']] }).font.id, 'vibur', 'and with no word the SKU\'s listing falls back to Vibur');
  eq(interpret({ rid: '1', listingId: '1714112618', sku: 'Tiny Initial Tag 3', vars: [['Font Choice (see listing photo for choices)', 'Ace']] }).font.id, 'jost-ace', 'Tiny Initial Tag 3 (another listing of the family): Ace is the Ace font'); }
{ // every other listing with a Font option: mapped per word, never the tag font
  for (const x of FIX.filter(l => !['4458930445', '1714117116'].includes(l.listingId))) {
    const sp = interpret(x), word = x.vars.map(v => O.fontParts(v[1]).words.map(w => O.fontOfWord(w)).find(Boolean)).find(Boolean);
    ok(word && sp.font && sp.font.id === word && !sp.font.listing, `${x.rid} ${x.listingId}: ${JSON.stringify(x.vars.at(-1))} -> ${sp.font && sp.font.name}`);
    eq(asks(sp).filter(p => p.font || /font/i.test(p.optionName)), [], 'and no question about the font');
  }
  eq(interpret(FIX.find(l => l.rid === '4172791262')).font, { asked: 'Stylish', id: 'playwrite-us-trad', name: 'Playwrite US Trad', source: 'rule:font' }, 'order 4172791262 INITIAL_8391 "Font: Stylish" resolves to Playwrite US Trad (it used to wait as "not mapped")');
  eq(interpret(FIX.find(l => l.rid === '4175370240')).font.id, 'crimson-text', 'order 4175370240 "Fonts: 16"/ Typewriter" resolves to Crimson Text'); }
{ // item 3: "Charm Type: Tag1 (front engrave)" on 4171852053: Etsy keeps ONE SKU (Mini_0445) for all 32 products of the listing (read once, 1 Etsy call), so the option cannot name a master charm
  const x = FIX.find(l => l.rid === '4171852053'), table = { 4458930445: { at: 1791650616248, n: 32, uni: 'Mini_0445' } };
  // (the receipt's own variation ids, as the Sep 17 snapshot has them: Charm Type = property 513, value 1481636059101)
  const withIds = c => O.interpretLine({ receiptId: x.rid, updateTs: 0 }, Object.assign(lineOf(x), { productId: '31726160304', variations: lineOf(x).variations.map(v => v.name === 'Charm Type' ? Object.assign(v, { property_id: 513, value_id: 1481636059101 }) : v) }), c);
  const sp = withIds(ctx({ listingSkus: table })), q = asks(sp);
  eq(q.map(p => [p.optionName, p.optionValue]), [['Charm Type', 'Tag1 (front engrave)']], 'the Tag1 option still waits for a person (and only it)');
  // (TAGSKU 1 of 11 Oct: the Tag number is now read as the tag SHAPE, so the plain reason names the shape, not the shared SKU; the option still waits, and never picks a look-alike by name)
  ok(/no design for the Tag1 shape/.test(q[0].why), 'with the plain reason: ' + q[0].why);
  ok(sp.designSku !== 'TINY INITIAL TAG 1', 'TINY INITIAL TAG 1 (a look-alike by name) is never picked');
  // a person's saved answer for the option settles it (not written here)
  const saved = { 4458930445: { 'charm type': { 'tag1 (front engrave)': { field: 'design', value: 'TINY INITIAL TAG 1', by: 'Paul', at: 1 } } } };
  const sp2 = withIds(ctx({ listingSkus: table, optionMaps: saved }));
  eq([asks(sp2).length, sp2.designSku], [0, 'TINY INITIAL TAG 1'], 'once a person says which design Tag1 is, the line goes on, in the tag font'); }

// ── E · ENGFONT: the three Mini Tag words, every spelling, both faces, the capitals rule, the slashed zero ───────────────────────────────────────────────
{ // 1. every option name and spelling of the family reads to the same font (the value says it, the option's name only has to be a font's)
  const SPELL = { 'jost-ace': ['Ace', 'ACE', 'ace', 'Ace (no lowercase)', 'ACE (NO LOWERCASE)', 'Ace - no lowercase', 'Ace no lowercase', 'ace(no lowercase)', 'Ace (caps only)', 'Ace (uppercase)'],
    'libre-franklin': ['Britannic Bold', 'BRITANNIC BOLD', 'britannic bold', 'Britannic-Bold', 'Britannic', 'Britanic Bold', 'BritannicBold'], vibur: ['Vibur', 'VIBUR', 'vibur', 'Viber'] };
  const NAMES = ['Font Choice', 'Font', 'Font Style', 'Font Choice (see listing photo for choices)', 'Fonts', 'Font Selection', 'Font Choice:', 'FONT CHOICE', 'Typeface', 'Lettering Style'];
  let n0 = 0;
  for (const [id, values] of Object.entries(SPELL)) for (const value of values) {
    eq(O.fontOfWord(value), id, `"${value}" is the word of ${id}`);
    for (const name of NAMES) { eq((O.fontRead(name, value) || {}).id, id, `"${name}": ${value}`); n0++; }
    eq((O.fontHidden('Necklace Length /Font /Alignment', `16"·${value}·Center`) || {}).id, id, `a length option that carries ${value}`);
    eq((O.fontInMetal('Metal? Font Choice?', `Gold • ${value}`) || {}).id, id, `a metal option that carries ${value}`);
    const sp = interpret({ rid: '1', listingId: '4458930445', sku: '', vars: [['Font Choice', value]] });
    eq([sp.font.id, !!sp.font.listing], [id, false], `${value} on listing 4458930445 reaches the engraving as ${id}`);
  }
  ok(n0 >= 200, `${n0} (option name x spelling) pairs read`);
  eq([O.fontOfWord('Pace'), O.fontOfWord('Space'), O.fontOfWord('Acer'), O.fontOfWord('Brit'), O.fontOfWord('Britain'), O.fontOfWord('Vibrant'), O.fontOfWord('Vibe')], ['', '', '', '', '', '', ''], 'words that merely look like them are none');
  // the other word families are untouched
  for (const [w, id] of [['Stylish', 'playwrite-us-trad'], ['Pristina', 'marck-script'], ['Angelina', 'mynerve'], ['Typewriter', 'crimson-text'], ['Comic', 'source-sans-3']]) eq(O.fontOfWord(w), id, `${w} keeps ${id}`); }
{ // 2. order 4195966039 as Paul's screenshot shows it: Mini Tag add-on, Charm Type "Tag1 (front+back)", Font Choice "Ace": the engraving is Ace -> Jost Slashed Zero, capitals only
  const sp = interpret({ rid: '4195966039', listingId: '4458930445', sku: 'MINI_0445', vars: [['Charm Type', 'Tag1 (front+back)'], ['Font Choice', 'Ace'], ['Metal Choice', 'Gold Filled']] });
  eq([sp.font.asked, sp.font.id, sp.font.name, sp.font.source, !!sp.font.listing], ['Ace', 'jost-ace', 'Jost Slashed Zero', 'rule:font', false], 'order 4195966039: "Font Choice: Ace" is the Ace font');
  eq(asks(sp).filter(p => /font/i.test(p.optionName) || p.font), [], 'no question about the font');
  eq(O.fontLine(sp), 'Font: Ace → Jost Slashed Zero · capitals only', 'the card line');
  // both faces: the line's one font serves the front and the back, unless a face names its own word
  for (const face of [undefined, 'front', 'back']) { const pf = O.pieceFont(sp, null, face); eq([pf.id, pf.caps, pf.mapped], ['jost-ace', true, true], `face ${face || '(none)'}: Ace`); }
  const both = Object.assign({}, sp, { engraving: { mode: 'both', front: { text: 'W', lines: ['W'], fontWord: null }, back: { text: '1. 27. 24', lines: ['1. 27. 24'], fontWord: null } } });
  eq(['front', 'back'].map(f => O.pieceFont(both, null, f).id), ['jost-ace', 'jost-ace'], 'a front + back line with no per-face word: Ace on both faces');
  both.engraving.back.fontWord = 'Vibur';
  eq(['front', 'back'].map(f => O.pieceFont(both, null, f).id), ['jost-ace', 'vibur'], 'a face that names its own font word (Back: ... - Vibur) gets that font; the other face keeps the line\'s');
  eq(O.pieceFont(both, null, 'back').caps, false, 'and the capitals rule follows the face\'s own font');
  both.engraving.back.fontWord = 'Britannic Bold'; eq(O.pieceFont(both, null, 'back').id, 'libre-franklin', 'Britannic Bold on a face');
  both.engraving.back.fontWord = 'Papyrus'; { const pf = O.pieceFont(both, null, 'back'); eq([pf.id, pf.mapped, pf.asked], ['jost-ace', false, 'Papyrus'], 'a face word the shop does not offer: the line\'s font, flagged as requested'); eq(O.fontLine(both, null, 'back'), 'Requested font: Papyrus (engraved in Jost Slashed Zero)', 'and the card says so'); }
  eq(O.faceFontWord(both, 'front'), '', 'a face with no word names none'); }
{ // 3. the capitals rule and the engraving of the page (the bridge's own ensureJobFont, run against fakes)
  eq([O.capsText('w', 'jost-ace'), O.capsText('Anna & Joe 1. 27. 24', 'jost-ace'), O.capsText('w', 'jost'), O.capsText('w', 'vibur'), O.capsText('w', 'libre-franklin'), O.capsText('w', 'source-sans-3'), O.capsText(null, 'jost-ace')], ['W', 'ANNA & JOE 1. 27. 24', 'w', 'w', 'w', 'w', ''], 'Ace text is capitals; every other font keeps the text as typed');
  const src = read('charm-nest-bridge.js'), a = src.indexOf('  async function ensureJobFont(job) {'), b = src.indexOf('    return setOf(job);\n  }', a);
  ok(a > 0 && b > a, 'ensureJobFont is in the bridge');
  const run = async (job, loaded) => { const env = vm.createContext({ O, pieceIndexOf: () => null, loadFontSet: async () => ({ ok: loaded !== false }), setOf: () => ({}), Array }); vm.runInContext(src.slice(a, b) + '    return setOf(job);\n  }\nthis.ensureJobFont = ensureJobFont;', env); await env.ensureJobFont(job); return job; };
  const sp = interpret({ rid: '4195966039', listingId: '4458930445', sku: 'MINI_0445', vars: [['Charm Type', 'Tag1 (front+back)'], ['Font Choice', 'Ace']] });
  (async () => {
    let j = await run({ row: { spec: sp }, text: 'w\n1. 27. 24', lines: ['w', '1. 27. 24'], lineInput: ['w', '1. 27. 24'] });
    eq([j.fontKey, j.fontName, j.lines, j.text, j.lineInput, j.fontCapsApplied], ['jost-ace', 'Jost Slashed Zero', ['W', '1. 27. 24'], 'W\n1. 27. 24', ['W', '1. 27. 24'], true], 'the piece of order 4195966039: "w" is engraved as "W" (lines, text and the line input), in Jost Slashed Zero');
    j = await run({ row: { spec: sp }, text: 'w', lines: ['w'], lineInput: ['w'], fontKey: 'jost-ace', fontLocked: true, fit: {} });
    eq([j.fontKey, j.lines], ['jost-ace', ['W']], 'a saved back reopened in Engrave (its font is locked) and edited with a lowercase letter: capitals still');
    j = await run({ row: { spec: sp }, text: 'w', lines: ['w'], lineInput: ['w'] }, false);
    eq([j.fontKey, j.lines, /could not be loaded/.test(j.fontFallback)], ['source-sans-3', ['W'], true], 'if the Ace font will not load the piece falls back to Source Sans 3 (said on the card) and its words are still capitals');
    for (const [word, id] of [['Vibur', 'vibur'], ['Britannic Bold', 'libre-franklin']]) {
      const sp2 = interpret({ rid: '1', listingId: '4458930445', sku: '', vars: [['Font Choice', word]] });
      j = await run({ row: { spec: sp2 }, text: 'w', lines: ['w'], lineInput: ['w'] });
      eq([j.fontKey, j.lines, !!j.fontCapsApplied], [id, ['w'], false], `${word}: the text is left exactly as typed`);
    }
    j = await run({ row: { spec: {} }, text: 'w', lines: ['w'], lineInput: ['w'] }); eq([j.fontKey, j.lines], ['source-sans-3', ['w']], 'a line with no font at all: Source Sans 3, text untouched');
    console.log(`font-map E (ensureJobFont): done`);
  })().catch(e => { console.error(e); process.exitCode = 1; }); }
{ // 4. the slashed zero: the Ace font's zero carries a slash (one more contour than plain Jost: the ring, and the slash splits its counter in two), nothing else changed
  const moves = (font, ch) => font.getPath(ch, 0, 0, 100).commands.filter(c => c.type === 'M').length;
  for (const w of ['Regular', 'Semibold']) {
    eq([moves(sets['jost-ace'][w], '0'), moves(sets.jost[w], '0')], [3, 2], `${w}: Jost Slashed Zero draws the zero with a slash (3 contours), plain Jost without (2)`);
    for (const ch of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789.&-\'') eq(JSON.stringify(sets['jost-ace'][w].getPath(ch, 0, 0, 100).commands), JSON.stringify(sets.jost[w].getPath(ch, 0, 0, 100).commands), `${w}: "${ch}" is Jost's own drawing, unchanged`);
    eq(sets['jost-ace'][w].getAdvanceWidth('0', 100), sets.jost[w].getAdvanceWidth('0', 100), `${w}: the slashed zero has the zero's own width`); }
  ok(/Modified Version|slash/i.test(fs.readFileSync(path.join(root, 'vendor/fonts/JostSlashedZero-OFL.txt'), 'utf8')) && /Jost Project Authors/.test(fs.readFileSync(path.join(root, 'vendor/fonts/JostSlashedZero-OFL.txt'), 'utf8')), 'the licence file says what was changed and keeps Jost\'s copyright');
  // Vibur is the font of the shop's chart: the app's own layout of it has no stray advance (the font's "rs" ligature carries a bad advance in the file; the app lays out sanely)
  const vib = sets.vibur.Regular; for (const t of ['Doris', 'Mars', 'Ross', 'Pearson', 'rs', 'Marsha']) ok(vib.getAdvanceWidth(t, 100) > 20 && vib.getAdvanceWidth(t, 100) < 100 * t.length * 1.1, `Vibur lays out "${t}" in sane width (${vib.getAdvanceWidth(t, 100).toFixed(1)})`); }
{ // 5. records: a back saved in any of the fonts reopens in it, old and new
  const rec = (font, fontKey) => Object.assign({ font }, fontKey ? { fontKey } : {});
  eq([rec('Jost Slashed Zero'), rec('Libre Franklin'), rec('Vibur'), rec('Jost'), rec('Alegreya Sans'), rec('Playwrite US Modern'), rec('Source Sans 3'), rec('x', 'jost-ace'), rec('x', 'vibur')].map(r => O.fontOfRecord(r)),
    ['jost-ace', 'libre-franklin', 'vibur', 'jost', 'alegreya-sans', 'playwrite-us-modern', 'source-sans-3', 'jost-ace', 'vibur'], 'a saved back names its font by name or key; the first matches of Vibur, Ace and Britannic Bold still open in themselves'); }

// ── D · the engraving ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
(async () => {
  const parsed = await P.parseSource(new Uint8Array(fs.readFileSync(path.join(__dirname, 'fixtures/TINY_INITIAL_TAG_1.ai'))), 'TINY_INITIAL_TAG_1.ai');
  const c = P.groupCharms(parsed, { minPt: 6 }).charms[0];
  const entry = { upAngle: 270, upSource: 'long' };   // the master index's own facts for TINY INITIAL TAG 1 (45.02 x 16.67 pt, upAngle 270, engravable)
  const charm = { outline: c.outline, members: c.members, bbox: c.bbox, widthPt: 45.02, heightPt: 16.673, upAngle: 270 };
  const input = (text, over) => Object.assign({ charm, lines: [text], lineMode: 'auto', viewOptions: G.viewOptionsFor({ editingBack: false, charmUp: 270, nudged: false, entry }), maskOptions: { marginMm: .8, keepOut: [] },
    opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: true } }, over || {});
  const fitted = {};
  for (const f of O.ENGRAVING_FONTS) for (const text of ['C.', 'JFC', 'm', '3', 'Ab']) {
    const res = Fit.calculate(input(text), sets[f.id], G);
    ok(res.fit && res.check.ok, `${f.name}: "${text}" fits inside the real TINY INITIAL TAG 1 (${res.fit ? res.fit.size.toFixed(2) + ' pt, cap ' + res.fit.capMm.toFixed(2) + ' mm, ' + res.fit.weight : res.reason})`);
    ok(G.verifyInk(res.fit.cmds, res.mask).ok, `${f.name} "${text}": zero ink outside the tag's cut line and in no hole`);
    ok(res.fit.capMm >= 1.0, `${f.name} "${text}": legible (cap ${res.fit.capMm.toFixed(2)} mm)`);
    fitted[f.id + '|' + text] = res;
  }
  // ENGFONT: the words of order 4195966039 ("W" on the front, "1. 27. 24" on the back, the zero too) in the three Mini Tag fonts on the real tag, inside it
  for (const id of ['jost-ace', 'vibur', 'libre-franklin']) for (const text of ['W', '1. 27. 24', '10 0', 'MC']) {
    const res = Fit.calculate(input(O.capsText(text, id)), sets[id], G);
    ok(res.fit && res.check.ok && G.verifyInk(res.fit.cmds, res.mask).ok, `${id}: "${O.capsText(text, id)}" fits the real tag with no ink outside it`);
  }
  // the tag font's own letters, not Source Sans 3's: "C." is drawn from Playwrite US Modern's glyphs
  const tag = fitted['playwrite-us-modern|C.'], plain = fitted['source-sans-3|C.'];
  ok(JSON.stringify(tag.fit.cmds) !== JSON.stringify(plain.fit.cmds), '"C." in the tag font is a different drawing from Source Sans 3');
  { const want = sets['playwrite-us-modern'].Regular.getPath('C.', 0, 0, tag.fit.size).commands.length; ok(tag.fit.cmds.length >= want * 0.9 && tag.fit.cmds.length <= want * 1.1 + 2, `the drawing has the font's own outline (${tag.fit.cmds.length} segments for ${want})`); }

  // the weight rule (Semibold under 2.2 mm cap height; a one-weight font cuts its Regular at every size and says so)
  for (const f of O.ENGRAVING_FONTS) {
    const small = fitted[f.id + '|C.'].fit;   // the tag is 5 mm tall: cap under 2.2 mm
    ok(small.capMm < 2.2, `${f.name}: the tag's cap height is under 2.2 mm`);
    eq(small.weight, f.files.Semibold ? 'Semibold' : 'Regular', `${f.name}: weight ${f.files.Semibold ? 'Semibold under 2.2 mm' : 'Regular (one weight)'}`);
    const big = { outline: { kind: 'path', layer: 'CUT', closed: true, stroke: true, strokeRGB: [1, 0, 0], subpaths: [[['m', [0, 0]], ['l', [120, 0]], ['l', [120, 60]], ['l', [0, 60]], ['h']]], bbox: [0, 0, 120, 60] } };
    big.members = [big.outline]; big.bbox = big.outline.bbox;
    const large = Fit.calculate(input('Anna', { charm: big, viewOptions: { res: 6, upAngle: 90 } }), sets[f.id], G).fit;
    ok(large.capMm >= 2.2, `${f.name}: a 42 mm plate lets "Anna" past 2.2 mm (${large.capMm.toFixed(2)})`);
    eq(large.weight, 'Regular', `${f.name}: Regular at a large size`);
  }
  eq(Fit.calculate(input('C.'), { Regular: sets['source-sans-3'].Regular, Semibold: sets['source-sans-3'].Semibold }, G).fit.weight, 'Semibold', 'Source Sans 3 itself is unchanged: Semibold on the tag');

  // the worker fits with a keyed font set (the page sends each non-default font once) and agrees with the page
  { const fb = f => Object.fromEntries(Object.entries(f.files).map(([w, file]) => [w, bytes(file)])), fontBytes = Object.assign(fb(O.fontById('source-sans-3')), { emoji: bytes('vendor/fonts/NotoEmoji-Regular.ttf'), emojiMap });
    class Adapter { constructor() { this.w = new Worker(`
      const {parentPort}=require('node:worker_threads'),fs=require('node:fs'),vm=require('node:vm');
      const context=vm.createContext({console,Intl,TextEncoder,TextDecoder,setTimeout,clearTimeout});
      context.self=context; context.postMessage=data=>parentPort.postMessage(data);
      context.importScripts=(...files)=>files.forEach(file=>vm.runInContext(fs.readFileSync(file.split('?')[0],'utf8'),context,{filename:file}));
      context.importScripts('charm-nest-engrave-worker.js');
      parentPort.on('message',data=>context.onmessage({data}));`, { eval: true, cwd: root });
      this.w.on('message', data => this.onmessage && this.onmessage({ data })); this.w.on('error', e => this.onerror && this.onerror(e)); keep.push(this); }
      postMessage(d) { this.w.postMessage(d); } terminate() { this.w.terminate(); } }
    const keep = [];
    try {
      const client = Fit.createClient({ WorkerClass: Adapter, url: 'charm-nest-engrave-worker.js', fonts: fontBytes, fontsFor: id => { const f = O.fontById(id); return f ? fb(f) : null; } });
      const viaWorker = await client.run(Object.assign(input('C.'), { fontKey: 'playwrite-us-modern' })), direct = fitted['playwrite-us-modern|C.'];
      eq(JSON.parse(JSON.stringify(viaWorker.fit)), JSON.parse(JSON.stringify(direct.fit)), 'the background worker, given the tag font once, fits "C." exactly as the page does');
      for (const over of [{}, { fontKey: 'source-sans-3' }]) eq(JSON.parse(JSON.stringify((await client.run(Object.assign(input('C.'), over))).fit)), JSON.parse(JSON.stringify(plain.fit)), `and a piece with ${over.fontKey ? 'the default font key' : 'no font key'} still fits in Source Sans 3`);
      await assert.rejects(client.run(Object.assign(input('C.'), { fontKey: 'no-such-font' })), /unavailable/); n++;
    } finally { keep.forEach(w => w.terminate()); } }

  // the saved back records the font and reopens in it
  { const fit = tag.fit, mk = extra => Object.assign({ poolId: 'p1', text: 'C.', lines: ['C.'], sizePt: fit.size, capMm: fit.capMm, weight: fit.weight, centre: fit.centre, angle: fit.angle || 0, lineGap: fit.layout.lineGap, upAngle: tag.view.upAngle, approvedAt: 5 }, extra);
    eq([O.fontOfRecord(mk({ font: 'Playwrite US Modern', fontKey: 'playwrite-us-modern' })), O.fontOfRecord(mk({ font: 'Playwrite US Modern' })), O.fontOfRecord(mk({ font: 'Source Sans 3' })), O.fontOfRecord(mk({})), O.fontOfRecord(null)],
      ['playwrite-us-modern', 'playwrite-us-modern', 'source-sans-3', 'source-sans-3', 'source-sans-3'], 'a record names its font by key, by the name the server keeps, or (every back saved before) it is Source Sans 3');
    const drawn = (rec, font) => JSON.stringify(Backs.engraveOn(rec, charm, { font, geom: G }).glyphs);
    ok(drawn(mk({}), sets['playwrite-us-modern'].Regular) !== drawn(mk({}), sets['source-sans-3'].Regular), 'the same record drawn in two fonts is two different drawings (so the font must follow the record)');
    // the viewers' own code (charm-nest-sheetwin.js backFont / otherFonts), run against a fake Engrave holding some fonts
    const src = read('charm-nest-sheetwin.js'), a = src.indexOf('  const backFontId = rec =>'), b = src.indexOf('  /** An engraving approved in Engrave but not written yet');
    ok(a > 0 && b > a, 'the viewers\' font code is in charm-nest-sheetwin.js');
    const loaded = new Set(), asked = [], Engrave = { fonts: sets['source-sans-3'], fontSetFor: id => id === 'source-sans-3' ? Engrave.fonts : loaded.has(id) ? sets[id] : null, loadFontSet: id => { asked.push(id); loaded.add(id); return Promise.resolve(sets[id]); } };
    const win = { CharmNestOrders: O, Engrave }, env = vm.createContext({ window: win, tryDo: f => { try { return f(); } catch (_) { return undefined; } }, Promise, Map, Set, Array });
    vm.runInContext(src.slice(a, b) + '\nthis.backFont = backFont; this.otherFonts = otherFonts; this.backFontId = backFontId;', env);
    const tagRec = mk({ font: 'Playwrite US Modern', fontKey: 'playwrite-us-modern' }), oldRec = mk({ font: 'Source Sans 3' });
    eq(env.backFont('Regular', tagRec), null, 'a record in the tag font draws nothing until that font is here (never in the wrong font)');
    eq(env.backFont('Semibold', oldRec), sets['source-sans-3'].Semibold, 'a record saved before the other fonts is drawn in Source Sans 3, Semibold when it says so');
    await Promise.all(env.otherFonts(new Map([['p1', tagRec], ['p2', oldRec]]), []));
    eq(asked, ['playwrite-us-modern'], 'only the other font is asked for, once');
    eq(env.otherFonts(new Map([['p1', tagRec]]), []).length, 0, 'and not again when it is here');
    eq(env.backFont('Semibold', tagRec), sets['playwrite-us-modern'].Regular, 'a one-weight font is cut in Regular at every size, whatever the record says');
    eq(env.backFont('Regular', mk({ font: 'Crimson Text', fontKey: 'crimson-text' })), null, 'Crimson Text is not loaded yet'); await Promise.all(env.otherFonts([mk({ font: 'Crimson Text' })], [])); eq(env.backFont('Semibold', mk({ font: 'Crimson Text' })), sets['crimson-text'].Semibold, 'a two-weight font keeps its Semibold');
    // the bridge writes the font on every record it saves, and the server's short copy keeps the name
    const bridge = read('charm-nest-bridge.js');
    ok(/font:job\.fontName \|\| "Source Sans 3",fontKey:fontIdOf\(job\)/.test(bridge), 'the back file meta says the font name and key');
    ok(/font: job\.fontName \|\| "Source Sans 3", fontKey: fontIdOf\(job\)/.test(bridge) || /fontKey: fontIdOf\(job\)/.test(bridge), 'and so does the saved record');
    ok(/fontKey:O\.fontOfRecord\(saved\)/.test(bridge), 'a back reopened in Engrave takes its font from the record');
    ok(/SHEET_BACK_FIELDS[\s\S]{0,400}"font"/.test(read('netlify/functions/charmNestLibrary.js')), 'the server keeps the font name of each saved back (viewers derive the font from it)');
    // the cache tokens carry the change
    const html = read('charm-nest-1.html');
    for (const f of ['charm-nest-orders.js', 'charm-nest-bridge.js', 'charm-nest-sheetwin.js', 'charm-nest-engraving-seals.js']) ok(new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-vf1(?:-[a-z0-9]+)*"').test(html), `${f}: the ?v= token carries -vf1`);
    ok(/charm-nest-engrave-worker\.js\?v=[^"']*-vf1/.test(bridge), 'and the worker URL');
    for (const f of ['charm-nest-orders.js', 'charm-nest-bridge.js']) ok(new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-ef1(?:-[a-z0-9]+)*"').test(html), `${f}: the ?v= token carries -ef1 (ENGFONT)`); }

  console.log(`font-map: ${n} checks passed`);
})().catch(e => { console.error(e); process.exitCode = 1; });
