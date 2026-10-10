// FONTMAP / VIBURFONT (Paul, 10 Oct 2026, 16:03-16:13 UTC): "find similar versions that are open source to the options we offer and then map those options. Anytime you see font
// choices, chosen by the user from a drop-down on all listings this has to be a check for every single purchase listing." and, for the three Mini Tag listings (4458930445,
// 1714117116 "Tiny Initial Tag 1" and "Tiny Initial Tag 1-CO"): "ignore the Font choice and just find and install a similar looking font".
//   node tests/charm-nest/font-map.cjs        (no network, no Etsy, no AI, nothing live; one small worker thread)
// Real code, real data: the real modules, the 17 real order lines of the Sep 17 snapshot (fixtures/font-option-lines.json), EVERY font-bearing option value of the 49 listings with a font
// drop-down (fixtures/font-audit-values.json, read from 8,500 real orders by FONTAUDIT: names and values only), and the real master design TINY INITIAL TAG 1 (fixtures/TINY_INITIAL_TAG_1.ai).
//  A · the fonts: files, licences, glyphs, the registry
//  B · the words: all 8 font words, every spelling seen, the option name is not needed, "No Font" is no font, an unknown word never holds
//  C · the real lines: the three Mini Tag lines read with no font question, every other listing keeps its own font
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

const WORDS = { Stylish: 'playwrite-us-trad', Pristina: 'marck-script', Angelina: 'mynerve', Typewriter: 'crimson-text', Comic: 'source-sans-3', Vibur: 'playwrite-us-modern', Ace: 'jost', 'Britannic Bold': 'alegreya-sans' };
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
eq(O.ENGRAVING_FONTS.map(f => f.id), ['source-sans-3', 'playwrite-us-trad', 'marck-script', 'mynerve', 'crimson-text', 'playwrite-us-modern', 'jost', 'alegreya-sans'], 'Source Sans 3 and seven lookalikes');
eq(O.ENGRAVING_FONTS.filter(f => !f.files.Semibold).map(f => f.id), ['playwrite-us-trad', 'marck-script', 'mynerve', 'playwrite-us-modern'], 'the four scripts have ONE weight; the others have a heavier cut for the small sizes');
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
  for (const [w, id] of [['STYLISH', 'playwrite-us-trad'], ['stylish', 'playwrite-us-trad'], ['PRISTINA', 'marck-script'], ['ANGELINA', 'mynerve'], ['angelina', 'mynerve'], ['COMIC', 'source-sans-3'], ['Vibur', 'playwrite-us-modern'], ['Ace', 'jost'], ['Britannic Bold', 'alegreya-sans']]) eq(O.fontOfWord(w), id, w);
  eq([O.fontOfWord('Typed'), O.fontOfWord('Type'), O.fontOfWord('Wrist'), O.fontOfWord('Pace'), O.fontOfWord('Place'), O.fontOfWord('Face')], ['', '', '', '', '', ''], 'a word that merely looks like a font word is none');
  eq(O.fontOfPhrase('Typewriter font'), 'crimson-text', 'the word inside a phrase'); eq(O.fontOfPhrase('Ace of hearts font'), 'jost', 'Ace in a font option reads as Ace'); }
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
  for (const hold of [false, true]) { const keep = O.FONT_RULES.unknownHolds; O.FONT_RULES.unknownHolds = hold; try {
    for (const x of tagLines) {
      const sp = interpret(x), fontOpt = sp.options.find(o => /font/i.test(o.name));
      eq([sp.font.id, sp.font.name, sp.font.source, sp.font.listing, sp.font.asked], ['playwrite-us-modern', 'Playwrite US Modern', 'rule:font-listing', true, 'Vibur'], `${x.rid} ${x.sku || '(no SKU)'}: the listing's one font, whatever the buyer picked (the switch ${hold ? 'on' : 'off'})`);
      eq([fontOpt.mapped.field, fontOpt.mapped.value, fontOpt.mapped.source], ['font', 'playwrite-us-modern', 'rule:font-listing'], 'the Font Choice option is settled by the listing rule');
      eq(asks(sp).filter(p => /font/i.test(p.optionName) || p.font), [], 'no question about the font');
    } } finally { O.FONT_RULES.unknownHolds = keep; } }
  // whatever the buyer picks on these listings is ignored, never held
  for (const w of ['Ace', 'Britannic Bold', 'Vibur', 'Brush Script', 'Papyrus']) for (const [lid, sku] of [['4458930445', ''], ['1714117116', 'Tiny Initial Tag 1'], ['1714117116', 'Tiny Initial Tag 1-CO']]) {
    const sp = interpret({ rid: '1', listingId: lid, sku, vars: [['Metal Choice', 'Gold Filled'], ['Font Choice', w]] });
    eq([sp.font.id, asks(sp).filter(p => /font/i.test(p.optionName)).length], ['playwrite-us-modern', 0], `${lid} ${sku || '(no SKU)'}: ${w} is ignored`);
  }
  // by SKU alone as well, and a listing that is not one of them keeps the buyer's font
  eq(interpret({ rid: '1', listingId: '999', sku: 'tiny initial tag 1-co', vars: [['Font Choice', 'Ace']] }).font.id, 'playwrite-us-modern', 'the SKU names the listing too (any spelling of case)');
  eq(interpret({ rid: '1', listingId: '1714112618', sku: 'Tiny Initial Tag 3', vars: [['Font Choice (see listing photo for choices)', 'Ace']] }).font.id, 'jost', 'Tiny Initial Tag 3 (another listing) keeps its own font word: Ace is Jost'); }
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
  ok(/one SKU \(MINI_0445\) for every choice of this listing/.test(q[0].why), 'with the plain reason: ' + q[0].why);
  ok(sp.designSku === 'MINI_0445' && sp.designSku !== 'TINY INITIAL TAG 1', 'the only SKU Etsy gives is MINI_0445; TINY INITIAL TAG 1 (a look-alike by name) is never picked');
  // a person's saved answer for the option settles it (not written here)
  const saved = { 4458930445: { 'charm type': { 'tag1 (front engrave)': { field: 'design', value: 'TINY INITIAL TAG 1', by: 'Paul', at: 1 } } } };
  const sp2 = withIds(ctx({ listingSkus: table, optionMaps: saved }));
  eq([asks(sp2).length, sp2.designSku], [0, 'TINY INITIAL TAG 1'], 'once a person says which design Tag1 is, the line goes on, in the tag font'); }

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
    ok(/charm-nest-engrave-worker\.js\?v=[^"']*-vf1/.test(bridge), 'and the worker URL'); }

  console.log(`font-map: ${n} checks passed`);
})().catch(e => { console.error(e); process.exitCode = 1; });
