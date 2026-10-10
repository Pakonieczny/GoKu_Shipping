// OPTFONT: a font option (Font: Stylish, Fonts: 16"/ Typewriter) is read, not held as "not mapped" (Paul, 10 Oct 2026, orders 4175370240 and 4172791262:
// "I thought you were supposed to check all of the drop down menu SKU links"). Offline: no Etsy, no paid call, nothing live.
//   node tests/charm-nest/options-font.cjs
// What it proves (charm-nest-orders.js fontRead / fontParts / interpretLine, FONT_RULES; the engraving review's "Requested font"):
//   · the app engraves in the fonts of ENGRAVING_FONTS (FONTMAP, 10 Oct: an open-source lookalike for each of the shop's drop-down fonts, Source Sans 3 for the rest): the font words of the
//     real snapshot (Typewriter, Stylish, Pristina, Vibur, Comic) each map to one of them, in any spelling (the full word list and the new fonts: tests/charm-nest/font-map.cjs)
//   · the real lines of the Sep 17 snapshot that carry a font (fixtures/font-option-lines.json: 17 lines, 15 distinct option/value pairs, 6 distinct font words):
//     an option NAMED for a font (5 lines) is read and no longer held; a length option that carries a font in its value (12 lines) reads as the chain length it always did
//   · a compound value is split: "16"/ Typewriter" is the length 16" and the font Typewriter, "16"·PRISTINA·Center" adds the alignment
//   · a font the shop offers (or an installed font's own name) maps by itself, no question; a word the shop does not offer is never guessed
//   · any other font is mapped as the font ASKED for (never a look-alike), spec.font keeps it, the line is not held and it engraves in Source Sans 3;
//     FONT_RULES.unknownHolds = true holds it instead with ONE plain line naming the font and the app's fonts
//   · a person's saved answer, for the whole value or for the font alone, is read first and never changed; other options read as before
//   · the engraving review says "Requested font: Script (engraved in Source Sans 3)" beside the words (the shop decides), and "Font: Typewriter → Crimson Text" for a font it has
const assert = require('assert');
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require('../../charm-nest-orders.js');
const SEALS = require('../../charm-nest-engraving-seals.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/font-option-lines.json'), 'utf8')).lines;
const order = rid => ({ receiptId: String(rid), updateTs: 0 });
const lineOf = x => ({ transactionId: x.tid, listingId: x.listingId, sku: x.sku, title: x.title, quantity: x.q || 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: x.vars.map(([name, value]) => ({ name, value })) });
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {} }, extra);
const read = (x, c) => O.interpretLine(order(x.rid), lineOf(x), c || ctx());
const mk = (vars, over) => Object.assign({ rid: '4170000001', tid: '5200000001', listingId: '1718', sku: 'A', title: 'Initial Disc Necklace', q: 1, vars }, over || {});
const opt = (sp, name) => sp.options.find(o => o.name === name);
const asks = sp => sp.problems.filter(p => p.kind === 'needsMapping');
const withRule = (v, fn) => { const keep = O.FONT_RULES.unknownHolds; O.FONT_RULES.unknownHolds = v; try { return fn(); } finally { O.FONT_RULES.unknownHolds = keep; } };

// ── 1 · the app's fonts ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
eq(O.ENGRAVING_FONTS.map(f => [f.id, f.name]), [['source-sans-3', 'Source Sans 3'], ['playwrite-us-trad', 'Playwrite US Trad'], ['marck-script', 'Marck Script'], ['mynerve', 'Mynerve'], ['crimson-text', 'Crimson Text'], ['playwrite-us-modern', 'Playwrite US Modern'], ['jost', 'Jost'], ['alegreya-sans', 'Alegreya Sans']], 'the engraving fonts: Source Sans 3 and the seven open-source lookalikes (weight picked by size)');
eq(O.FONT_RULES, { unknownHolds: false }, 'the rule: a font the app does not have is carried to the engraving, it does not hold the line');
{ const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  ok(/font: ?job\.fontName \|\| "Source Sans 3"/.test(bridge), 'every saved back says the name of the font it was engraved in (Source Sans 3 when the job has none)');
  const files = fs.readdirSync(path.join(root, 'vendor/fonts')).filter(f => /\.(otf|ttf)$/i.test(f)).sort();
  eq(files, ['AlegreyaSans-Bold.ttf', 'AlegreyaSans-ExtraBold.ttf', 'CrimsonText-Regular.ttf', 'CrimsonText-SemiBold.ttf', 'Jost-Regular.ttf', 'Jost-SemiBold.ttf', 'MarckScript-Regular.ttf', 'Mynerve-Regular.ttf', 'NotoEmoji-Regular.ttf', 'PlaywriteUSModern-Regular.ttf', 'PlaywriteUSTrad-Regular.ttf', 'SourceSans3-Regular.otf', 'SourceSans3-Semibold.otf'], 'the shipped font files'); }

// ── 2 · reading one option ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
{ const R = (name, value) => O.fontRead(name, value), parts = (name, value) => { const r = R(name, value); return r && [r.asked, r.chain, r.align, r.id]; };
  eq(parts('Font', 'Stylish'), ['Stylish', '', '', 'playwrite-us-trad'], 'a plain font word');
  eq(parts('Fonts', '16"/ Typewriter'), ['Typewriter', '16"', '', 'crimson-text'], 'the length is split off: 16" and Typewriter');
  eq(parts('Font Choice', 'Vibur'), ['Vibur', '', '', 'playwrite-us-modern'], 'Font Choice');
  eq(parts('Font Choice (see listing photo for choices)', 'Vibur'), ['Vibur', '', '', 'playwrite-us-modern'], 'Font Choice (see listing photo for choices)');
  eq(parts('Typeface', 'Script'), ['Script', '', '', ''], 'Typeface is a font option');
  eq(parts('Lettering', '18 inch / Comic'), ['Comic', '18 inch', '', 'source-sans-3'], 'Lettering, a length written in words');
  eq(parts('Font', '16"·PRISTINA·Center'), ['PRISTINA', '16"', 'Center', 'marck-script'], 'length, font and alignment');
  eq(parts('Font', 'Typewriter · Right'), ['Typewriter', '', 'Right', 'crimson-text'], 'font and alignment');
  eq(parts('Font', '14" | Comic | Centered'), ['Comic', '14"', 'Centered', 'source-sans-3'], 'any of the usual separators');
  eq(parts('Font', 'Dancing Script'), ['Dancing Script', '', '', ''], 'a font of two words stays whole');
  eq(R('Font', '16"'), null, 'a value with no font word is no font');
  eq(R('Font', '   '), null, 'an empty value is no font');
  for (const nm of ['Necklace Length', 'Necklace Length /Font /Alignment', 'LENGTH / FONT / ALIGNMENT', 'Necklace Length & Font Choice:', 'Font Size', 'Font Color', 'Metal', 'Charm Type', 'Style', 'Type', 'Personalization']) eq(R(nm, 'Typewriter'), null, `"${nm}" is not a font option: read as it always was`); }

// ── 3 · exact is automatic, a look-alike never is ──────────────────────────────────────────────────────────────────────────────────────────
for (const v of ['Source Sans 3', 'source sans 3', 'SOURCE SANS 3', '  Source   Sans  3 ', 'Source-Sans 3', 'source_sans_3', 'Source Sans 3.', 'SourceSans3', '16"/ Source Sans 3', '16" · SOURCE SANS 3 · Center']) {
  const sp = read(mk([['Font', v]]));
  eq([asks(sp).length, opt(sp, 'Font').mapped.field, opt(sp, 'Font').mapped.value, opt(sp, 'Font').mapped.source, sp.font.id, sp.font.name], [0, 'font', 'source-sans-3', 'rule:font', 'source-sans-3', 'Source Sans 3'], `${JSON.stringify(v)}: maps by itself to the app font, no question`);
}
eq(read(mk([['Font', '16"/ Source Sans 3']])).chain, '16"', 'its length goes to the line as the chain, the font is read apart');
eq(read(mk([['Font', 'Source Sans 3']])).chain, null, 'no length, no chain');
for (const v of ['Source Sans', 'Source Sans Pro', 'Source Sans 3 Bold', 'Source Sans 2', 'Sans 3', 'Sourcesans', 'Dancing Script', 'Cursive', 'Slab Serif']) {
  const fr = O.fontRead('Font', v); eq([fr.id, fr.name], ['', ''], `${JSON.stringify(v)} is not a font the shop offers: never guessed`);
}
eq([O.fontRead('Font', 'Typewriter').asked, O.fontRead('Font', 'Typewrite').asked, O.fontRead('Font', 'Typewriter').id, O.fontRead('Font', 'Typewrite').id], ['Typewriter', 'Typewrite', 'crimson-text', 'crimson-text'], 'Typewriter and Typewrite are written two ways (kept as written, the card shows the buyer\'s word) and are one font');

// ── 4 · the real lines of the Sep 17 snapshot ──────────────────────────────────────────────────────────────────────────────────────────────
eq(FIX.length, 17, '17 lines of the snapshot carry a font in an option');
const FONT_OPT = /\bfonts?\b/i, isFontNamed = name => O.isFontOption(name);
const fontLines = [], lengthLines = [];
for (const x of FIX) for (const [name, value] of x.vars) { const p = O.fontParts(value); if (!p.words.length || !(FONT_OPT.test(name) || p.align)) continue; (isFontNamed(name) ? fontLines : lengthLines).push({ x, name, value, p }); }
eq([fontLines.length, lengthLines.length], [5, 12], '5 lines whose option is named for a font (held as "not mapped" until now), 12 whose length option carries a font in its value (read as a chain length)');
eq(new Set(fontLines.concat(lengthLines).map(l => l.name + ' || ' + l.value)).size, 15, '15 distinct option/value pairs');
const words = new Map(); for (const l of fontLines.concat(lengthLines)) { const k = l.p.words.join(' / ').toLowerCase(); words.set(k, (words.get(k) || 0) + 1); }
eq([...words].sort(), [['comic', 1], ['pristina', 3], ['stylish', 4], ['typewrite', 4], ['typewriter', 2], ['vibur', 3]], '6 distinct font words and how many lines each has (case and punctuation folded)');
eq([...words.keys()].map(w => O.fontRead('Font', w).id).sort(), ['crimson-text', 'crimson-text', 'marck-script', 'playwrite-us-modern', 'playwrite-us-trad', 'source-sans-3'], 'every one of the 6 is an installed font now');
for (const l of fontLines) {
  const sp = read(l.x), o = opt(sp, l.name);
  const tag = !!O.fontListing(lineOf(l.x), l.x.sku), id = tag ? 'playwrite-us-modern' : O.fontOfPhrase(l.p.words.join(' / '));
  eq([asks(sp).filter(p => p.optionName === l.name).length, o.mapped.field, o.mapped.value, o.mapped.source, sp.font.asked, sp.font.id], [0, 'font', id, tag ? 'rule:font-listing' : 'rule:font', l.p.words.join(' / '), id], `${l.x.rid} ${l.name}: ${l.value}: read as the font asked for and the installed font it is, nothing held`);
  if (!sp.options.some(q => q.name !== l.name && q.mapped && q.mapped.field === 'chain')) eq(sp.chain, l.p.chain || null, `${l.x.rid}: the length it came with is the chain`);   // (a line with a length option of its own keeps that one: the first read)
}
for (const l of lengthLines) {
  const sp = read(l.x), o = opt(sp, l.name);
  eq([o.mapped.field, o.mapped.value, o.mapped.source, sp.font && sp.font.id], ['chain', l.value, 'rule:length', O.fontOfPhrase(l.p.words.join(' / '))], `${l.x.rid} ${l.name}: ${l.value}: read as the chain length it always was, whole, and its font is read too`);
}
{ // the lines that waited keep every other answer: the same problems but the font's
  const stylish = FIX.find(x => x.rid === '4172791262'), sp = read(stylish);
  eq(sp.problems.map(p => p.kind), [], 'Font: Stylish, the disc necklace: nothing else to ask (no master given here)');
  eq([sp.designSku, sp.pieceCount], ['INITIAL_8391', 3], '4172791262 INITIAL_8391: still the same design, and 3 discs');
  const typ = FIX.find(x => x.rid === '4175370240'), st = read(typ);
  eq(st.problems.map(p => p.kind), [], 'Fonts: 16"/ Typewriter, the 2 disc necklace: nothing to ask');
  eq([st.pieceCount, st.chain, st.font.asked, st.font.id, opt(st, 'Necklace Options').mapped.field], [2, '16"', 'Typewriter', 'crimson-text', 'count'], '4175370240: 2 discs (the Necklace Options count is as before), chain 16", the font asked for is Typewriter, engraved in Crimson Text'); }

// ── 5 · the hold, when it is switched on: ONE plain line, for a word the shop does not offer ──────────────────────────────────────────────────
withRule(true, () => {
  for (const [name, value, asked, chain] of [['Font', 'Dancing Script', 'Dancing Script', ''], ['Fonts', '16"/ Slab Serif', 'Slab Serif', '16"'], ['Font Choice', 'Cursive · Center', 'Cursive', '']]) {
    const sp = read(mk([[name, value]])), a = asks(sp).filter(p => p.font);
    eq(a.length, 1, `${value}: one question for the font option, not two`);
    eq(asks(sp).filter(p => !p.font && p.optionName === name).length, 0, 'and no generic "not mapped" for it');
    const p = a[0]; eq([p.optionName, p.optionValue, p.font.asked, p.font.chain, p.font.raw], [name, asked, asked, chain, value], 'it names the font alone, the length is read apart');
    ok(p.font.why.includes('“' + p.font.asked + '”') && p.font.why.includes('Source Sans 3') && p.font.why.includes('Crimson Text') && !/\n/.test(p.font.why), 'ONE plain line naming the font and the installed fonts: ' + p.font.why);
    eq(p.font.pick.length, 8, 'the installed fonts to pick from');
    eq(opt(sp, name).mapped, null, 'the option is unmapped');
  }
  // the shop's own words never hold, whatever the switch says (they are installed fonts)
  for (const l of fontLines) eq(asks(read(l.x)).filter(p => p.optionName === l.name).length, 0, `${l.x.rid} ${l.name}: ${l.value}: the shop's own font word never asks`);
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  ok(/p\.pairSecond \? `\$\{p\.pairSecond\.why\}` : p\.font \? p\.font\.why :/.test(bridge), 'the Review row says that one line (problemText reads p.font.why)');
  // a saved answer for the whole value, or for the font alone, settles it (the person's, read first)
  const x = FIX.find(l => l.rid === '4175370240'), key = 'fonts';
  const whole = { '1008014571': { [key]: { '16"/ typewriter': { field: 'font', value: 'jost', by: 'Paul', at: 1 } } } };
  let sp = read(x, ctx({ optionMaps: whole })); eq([asks(sp).length, opt(sp, 'Fonts').mapped.field, opt(sp, 'Fonts').mapped.source, sp.font.id, sp.chain], [0, 'font', 'listing', 'jost', '16"'], 'a saved answer for the whole value settles it, over the shop word\'s font');
  const alone = { '1008014571': { [key]: { typewriter: { field: 'font', value: 'jost', by: 'Paul', at: 1 } } } };
  sp = read(x, ctx({ optionMaps: alone })); eq([asks(sp).length, sp.font.id, sp.chain], [0, 'jost', '16"'], 'a saved answer for the font alone settles it');
  sp = read(Object.assign({}, x, { vars: [['Necklace Options', 'ROSEGOLD - 2 Disc'], ['Fonts', '18"/Typewriter']] }), ctx({ optionMaps: alone })); eq([asks(sp).length, sp.font.id, sp.chain], [0, 'jost', '18"'], 'and serves every length that font comes with');
  sp = read(Object.assign({}, x, { listingId: '1999' }), ctx({ optionMaps: alone })); eq([asks(sp).length, sp.font.id], [0, 'crimson-text'], 'but only the listing it was saved for (another listing reads the shop word)');
  sp = read(Object.assign({}, x, { listingId: '1999' }), ctx({ optionMaps: { '*': { [key]: { typewriter: { field: 'ignore', value: null } } } } })); eq([asks(sp).length, opt(sp, 'Fonts').mapped.field], [0, 'ignore'], 'a shop-wide "changes nothing" settles it too');
  const mine = JSON.parse(JSON.stringify(whole)); read(x, ctx({ optionMaps: mine })); read(x, ctx({ optionMaps: mine })); eq(mine, whole, 'a saved answer is read, never written or changed');
  eq(asks(read(mk([['Font', 'Source Sans 3']]))).length, 0, 'an exact app font never asks');
});
// the switch off again: nothing in the module kept the hold
eq(asks(read(FIX.find(l => l.rid === '4172791262'))).length, 0, 'with the switch off the font does not hold');
{ const sp = read(mk([['Font', 'Dancing Script']])); eq([asks(sp).length, sp.font.asked, sp.font.id, opt(sp, 'Font').mapped.source], [0, 'Dancing Script', '', 'rule:font-asked'], 'and a word the shop does not offer is carried as asked, engraved in Source Sans 3'); }

// ── 6 · other options read as before ───────────────────────────────────────────────────────────────────────────────────────────────────────
{ const m = (name, value) => opt(read(mk([[name, value]])), name).mapped;
  eq(m('Font Size', '12'), { field: 'size', value: '12', source: 'rule:size' }, 'Font Size is a size option still');
  eq(m('Necklace Length', '16"'), { field: 'chain', value: '16"', source: 'rule:length' }, 'Necklace Length 16" is the chain still');
  eq(m('Style', 'Typewriter'), null, 'a plain Style option holding a font word is still the generic question (it is not named for a font)');
  eq(asks(read(mk([['Style', 'Typewriter']])))[0].font, undefined, 'and carries no font problem');
  eq(opt(read(mk([['Metal Choice', 'Gold']])), 'Metal Choice'), undefined, 'metal options are the station classifier\'s, as before'); }
{ // the shop's own default maps are untouched
  eq(Object.keys(O.DEFAULT_OPTION_MAP['*']).filter(k => /font|typeface|lettering/i.test(k)), [], 'no built-in "*" map for fonts: an exact app font is read by the rule, nothing is stored'); }

// ── 7 · the engraving review says it ───────────────────────────────────────────────────────────────────────────────────────────────�
{ const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  ok(/fontText = O\.fontLine\(sp, pIx\)/.test(bridge) && /if \(fontText && !fontMapped && /.test(bridge) && /wants\.push\(fontText\)/.test(bridge), 'the placement card lists a font the shop does not offer under To check with the buyer, and a mapped one under From the order');
  const sp = read(FIX.find(l => l.rid === '4175370240')), unk = read(mk([['Font', 'Dancing Script']]));
  eq(O.fontLine(sp), 'Font: Typewriter → Crimson Text', 'the card line for a font the app has: "Font: Typewriter → Crimson Text"');
  eq(O.fontLine(unk), 'Requested font: Dancing Script (engraved in Source Sans 3)', 'and for a word the shop does not offer: "Requested font: Dancing Script (engraved in Source Sans 3)"');
  const panel = (spec, requests) => SEALS.panel({ kind: 'approve', text: 'J', job: { key: 'k', row: { spec }, requests, state: 'review' }, pieceLabel: '' });
  ok(/Font: Typewriter → Crimson Text/.test(panel(sp)), 'the Engraving card says "Font: Typewriter → Crimson Text" under the words');
  ok(!/Asked for the font/.test(panel(sp)) && !/Requested font/.test(panel(sp)), 'and asks nothing about it');
  ok(/Requested font: Dancing Script \(a drop-down option, engraved in Source Sans 3\)/.test(panel(unk)), 'a word the shop does not offer is "Requested font: Dancing Script (a drop-down option, engraved in Source Sans 3)"');
  ok(!/Font: /.test(panel({})) && !/Requested font/.test(panel({})), 'nothing for a line with no font option');
  eq((panel(sp, { font: 'typewriter' }).match(/Asked for the font/g) || []).length, 1, 'a note that asks for a font is still said (once)');
  eq((panel(unk, { font: 'Comic Sans' }).match(/(Asked for the font|Requested font)/g) || []).length, 2, 'both when the note and the option ask for different fonts'); }

console.log(`options-font: ${n} checks passed`);
