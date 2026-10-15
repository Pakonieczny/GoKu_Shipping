// SIZEFIT hardening (Paul, 15 Oct 2026): the ways a hostile real shop could make the size check cut a disc at the WRONG size without a word, or leave a line nobody can release.
// Pure side only (node): how a drop-down value is read as a diameter, and what the "ask" hold offers a person.
//   node tests/charm-nest/size-fit-hard.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, why) => { assert(c, why); n++; };
const PT = 72 / 25.4, PAD = O.SIZE_PAD_PT;
const disc = (mm, o = {}) => { const w = mm * PT; return Object.assign({ sku: 'WOLF_1234', widthPt: w + PAD, heightPt: w * 1.15 + PAD, areaPt2: Math.PI * w * w / 4 * 1.03, aiPath: 'charmnest/master/WOLF_1234.ai', charmHash: 'h14' }, o); };
const d14 = disc(14);
const order = { receiptId: '4199999999', updateTs: 1, staffNote: '', buyerMessage: '' };
const ctxOf = e => ({ optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => (String(s).toUpperCase() === 'WOLF_1234' ? e : null) });
const lineOf = (name, value) => ({ transactionId: '1', listingId: '1111', sku: 'WOLF_1234', title: 'Wolf Charm Only Disc Necklace', quantity: 1, metalKey: 'ss', metalLabel: 'Sterling Silver', personalization: [], variations: [{ name, value }] });
const read = (name, value, e = d14) => O.interpretLine(order, lineOf(name, value), ctxOf(e));
const NAME = 'Size of Necklace Charm';

// ── 1 · a number written the way a buyer in another country writes it is that number: "1,2 cm" is 12 mm (it was read as 2 cm = 20 mm, and the disc was cut 20 mm), "10,5mm" is 10.5 (it was read as 5 mm)
for (const [v, want] of [['1,2 cm', '12MM'], ['1,2cm', '12MM'], ['10,5mm', '10.5MM'], ['12,0 mm', '12MM'], ['1,25 cm', '12.5MM'], ['0,9 cm', '9MM']]) {
  const sp = read(NAME, v); ok(sp.size === want, `"${v}" is ${want}, read ${sp.size}`);
  ok(!sp.sizeFit || sp.sizeFit.wantMm === (want === '12.5MM' ? 12.5 : parseFloat(want)), `"${v}": the check is run for ${want}`);
}
// ── 2 · a range or a list of sizes is no size: it is never read as its last number ("10-12mm" was 12 mm)
for (const v of ['10-12mm', '10 - 12 mm', '10–12mm', '10 to 12mm', '10 or 12mm', '10 / 12mm', '10, 12mm', '10 & 12mm', '10 and 12mm', '10~12mm', '10mm - 12mm', '10mm or 12mm', '1 cm / 1.2 cm']) {
  const sp = read(NAME, v); ok(!sp.size && !sp.sizeFit, `"${v}" names two sizes: not read as one (size ${sp.size})`);
}
// ... while the ordinary ways of writing ONE size still read as before
for (const [v, want] of [['12mm', '12MM'], ['12 mm', '12MM'], ['12MM + Engraving', '12MM'], ['Large (14mm)', '14MM'], ['1.2 cm', '12MM'], ['Option 2 - 12mm', '12MM'], ['Style 1: 10mm', '10MM'], ['12mm / 1/2 in', '12MM'], ['12 mm (engraved)', '12MM'], ['Ø 12 mm', '12MM'], ['12 millimeters', '12MM']]) {
  const sp = read(NAME, v); ok(sp.size === want, `"${v}" is still ${want}, read ${sp.size}`);
}
// ── 3 · full-width digits and letters (an Asian keyboard), and an underscore joining the words ("12mm_engraved")
for (const [v, want] of [['１２ｍｍ', '12MM'], ['１０ mm + engraving', '10MM'], ['12mm_engraved', '12MM'], ['12MM_ENGRAVING', '12MM'], ['10mm_', '10MM']]) {
  const sp = read(NAME, v); ok(sp.size === want && !sp.problems.some(p => p.kind === 'needsMapping'), `"${v}" is ${want}, read ${sp.size}: ${JSON.stringify(sp.problems.map(p => p.kind))}`);
}
// ── 4 · the unit may be in the option's NAME and the choices bare numbers ("Disc size (mm)": 10 / 12 / 14)
for (const [nm, v, want] of [['Disc size (mm)', '12', '12MM'], ['Charm Diameter (mm)', '10', '10MM'], ['Size in mm', '14', '14MM'], ['Size (mm)', '12.5', '12.5MM'], ['Charm size (cm)', '1.2', '12MM'], ['Diameter, mm', '10', '10MM'], ['Size of Necklace Charm (mm)', '12', '12MM']]) {
  const sp = read(nm, v); ok(sp.size === want && !sp.problems.some(p => p.kind === 'needsMapping'), `"${nm}: ${v}" is ${want}, read ${sp.size}: ${JSON.stringify(sp.problems.map(p => p.kind))}`);
  ok(sp.sizeFit && sp.sizeFit.wantMm === parseFloat(want), `"${nm}: ${v}" is checked against the design`);
}
// the names that carry a unit but are not about the size are never a size ("Chain length (cm)": 45)
for (const [nm, v] of [['Chain length (cm)', '45'], ['Necklace length (cm)', '45'], ['Chain (cm)', '45'], ['Engraving font height (mm)', '3']]) {
  const sp = read(nm, v); ok(!sp.sizeFit && (!sp.size || nm === 'Engraving font height (mm)'), `"${nm}: ${v}" is no diameter: size ${sp.size}, check ${JSON.stringify(sp.sizeFit)}`);
}
// and a bare number under a name WITHOUT a unit keeps meaning what it always meant (a ring size)
{ const r = read('Ring Size', '7'); ok(r.size === '7' && !r.sizeFit, 'Ring Size 7 as before'); const s = read('Disc size', '12'); ok(!s.sizeFit, 'a bare "12" under "Disc size" (no unit anywhere) is not guessed to be millimetres: ' + JSON.stringify(s.sizeFit)); }

// ── 5 · an impossible size must be RELEASABLE: the Review card offers the sizes in `available` (the person picks one: "Use this size"), so a design that holds no sizes of its own offers the size it is drawn at
{ const big = read(NAME, '40mm'); const p = big.problems.find(x => x.kind === 'missingSize');
  ok(p && big.sizeFit.action === 'ask', 'the 40 mm line is held');
  ok(Array.isArray(p.available) && p.available.length === 1 && /^\d+(\.\d)?MM$/.test(p.available[0]), 'the hold offers the size the design is drawn at: ' + JSON.stringify(p.available));
  const again = read(NAME, p.available[0]); ok(again.size === p.available[0] && !again.problems.some(x => x.kind === 'missingSize') && again.sizeFit && again.sizeFit.action === 'same', 'the offered size releases the line, drawn as it is: ' + JSON.stringify(again.sizeFit));
  const tiny = read(NAME, '4mm'); const pt = tiny.problems.find(x => x.kind === 'missingSize'); ok(pt && pt.available.length === 1, 'a size too small is offered the same way');
  // a design that holds sizes offers those, as it always did
  const sized = { sku: 'WOLF_1234', sizes: { '12MM': disc(12, { aiPath: 'a12' }), '14MM': disc(14, { aiPath: 'a14' }) } };
  const sp = read(NAME, '70mm', sized); const ps = sp.problems.find(x => x.kind === 'missingSize'); ok(ps && ps.available.join() === '12MM,14MM', 'a sized design offers its own sizes: ' + JSON.stringify(ps && ps.available)); }
// ── 6 · a person's OLD answer for the option must not switch the check off. Before this check a "Size of Necklace Charm: 12mm" option asked Review once and was answered "changes nothing" (ignore), saved for the listing:
//       the standard check has to read the size anyway, or every listing a person ever answered cuts the repo's size whatever the buyer chose
{ const withMap = (name, value, ans, e = d14) => O.interpretLine(order, lineOf(name, value), Object.assign(ctxOf(e), { optionMaps: { '1111': { [name.toLowerCase()]: { [value.toLowerCase()]: ans } } } }));
  for (const [nm, v, want] of [[NAME, '10mm', '10MM'], [NAME, '12mm + engraving', '12MM'], ['Charm Size (NO Huggie option)', '14mm', '14MM'], ['Disc size (mm)', '10', '10MM']]) {
    const sp = withMap(nm, v, { field: 'ignore', value: null, by: 'Test', at: 1 });
    ok(sp.size === want && sp.sizeFit && sp.sizeFit.wantMm === parseFloat(want) && !sp.problems.some(p => p.kind === 'needsMapping'), `"${nm}: ${v}" answered "changes nothing" is still the size ${want}: size ${sp.size}, ${JSON.stringify(sp.sizeFit && sp.sizeFit.action)}`);
  }
  // the person's other answers stand: a size they typed, a charm they picked
  const typed = withMap(NAME, '10mm', { field: 'size', value: 'M' }); ok(typed.size === 'M' && !typed.sizeFit, 'an answer that names a size stands: ' + typed.size);
  const other = withMap('Gift Wrap', 'yes', { field: 'ignore' }); ok(!other.size && !other.sizeFit, 'an ignored option that is no size stays ignored');
  const notmm = withMap('Style', 'Plain', { field: 'ignore' }); ok(!notmm.size, 'an ignored option with no size in it stays ignored'); }

// ── 7 · a design that carries no area is not known to be round: it is never resized on the shape of its box alone (a square, a heart and a star all have a box of 1 : 1)
{ const noArea = { sku: 'WOLF_1234', widthPt: 14 * PT + PAD, heightPt: 14 * PT + PAD, aiPath: 'x.ai' };
  ok(O.sizeFit(noArea, '12MM').action === 'unchecked', 'no area, a 1 : 1 box: unchecked, not scaled: ' + JSON.stringify(O.sizeFit(noArea, '12MM')));
  ok(O.sizeFit(Object.assign({}, noArea, { areaPt2: 0 }), '12MM').action === 'unchecked', 'an area of 0 is no area');
  ok(O.sizeFit(Object.assign({}, noArea, { areaPt2: Math.PI * Math.pow(14 * PT / 2, 2) }), '12MM').action === 'scale', 'with its area, the same disc is scaled'); }
// ── 8 · a size that is NOT the charm's: HOOP SIZE 8.5mm / 11mm is the shop's real option on every huggie listing, and a Ring Size or a Bracelet Size in mm is a ring's or a bracelet's. The charm of that line must never be drawn at it
//       (a round huggie charm of 6 mm was scaled to the 8.5 mm hoop). The size is still read as before (the design may hold sizes of its own), only the resize is off, for every page that draws the design.
{ const huggie = (sku, title, vars, e) => O.interpretLine(order, { transactionId: '1', listingId: '1816178147', sku, title, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: vars }, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: () => e });
  const V = (name, value) => ({ name, value });
  const round = mm => Object.assign({ sku: 'COMPASS (HUGGIE)' }, disc(mm, { aiPath: 'c.ai' }));
  for (const [nm, v, want] of [['HOOP SIZE', '8.5mm', '8.5MM'], ['Hoop Size', '11mm', '11MM'], ['Huggie Size', '8mm', '8MM'], ['Size of the Hoop', '10mm', '10MM'], ['Earring Size', '12mm', '12MM'], ['Ring Size', '17mm', '17MM'], ['Bracelet Size', '7mm', '7MM'], ['Charm Bracelet Size', '7mm', '7MM'], ['Chain size', '3mm', '3MM']]) {
    const e = round(6), sp = huggie('HUGGIE HOOPS-ZODIAC', 'Zodiac Huggie Hoops', [V('METAL CHOICE', 'Gold'), V(nm, v)], e);
    ok(sp.size === want, `"${nm}: ${v}" is still read as the size ${want} (read ${sp.size})`);
    ok(!sp.sizeFit || sp.sizeFit.action === 'unchecked', `"${nm}: ${v}" never resizes the charm: ${JSON.stringify(sp.sizeFit)}`);
    ok(O.sizeFit(e, want).action === 'unchecked', `...nor does any page that draws the design (the pool asks sizeFit(entry, "${want}")): ${JSON.stringify(O.sizeFit(e, want))}`);
  }
  // a plain "Size" on a huggie listing is the hoop's as well
  { const e = round(6), sp = huggie('TENNIS BALL (HUGGIE)', 'Tennis Ball Huggie Hoop Earrings', [V('Size', '8.5mm')], e); ok(sp.size === '8.5MM' && (!sp.sizeFit || sp.sizeFit.action === 'unchecked') && O.sizeFit(e, '8.5MM').action === 'unchecked', 'Size: 8.5mm on a huggie listing is the hoop\'s: ' + JSON.stringify(sp.sizeFit)); }
  // ... and the options that ARE the charm's still resize it, on any listing
  for (const [nm, v, title] of [['Size of the Necklace Charm (NO Huggie option)', '10mm', 'Wolf Charm Only Disc Necklace'], ['Size of the Necklace Charm (NO Huggie option)', '10mm', 'Wolf Disc Necklace or Huggie Hoops'], ['Huggie Charm Size', '10mm', 'Wolf Huggie Hoops'], ['Charm Size', '10mm', 'Zodiac Huggie Hoops'], ['Disc size', '10mm', 'Wolf Disc Necklace'], ['Size', '10mm', 'Wolf Charm Only Disc Necklace'], ['Pendant Size', '10mm', 'Wolf Pendant'], ['Diameter', '10mm', 'Wolf Disc Necklace']]) {
    const e = Object.assign({ sku: 'WOLF' }, disc(14, { aiPath: 'w.ai' })), sp = huggie('WOLF', title, [V('METAL CHOICE', 'Gold'), V(nm, v)], e);
    ok(sp.size === '10MM' && sp.sizeFit && sp.sizeFit.action === 'scale' && O.sizeFit(e, '10MM').action === 'scale', `"${nm}: ${v}" on "${title}" resizes the charm: ${JSON.stringify(sp.sizeFit)}`);
  } }
// the real Tennis Ball + Tennis Racket huggie hoops (order 4171010675, HOOP SIZE 8.5mm): BOTH designs of the pair are the line's, and the Right ear's own design is not resized to the hoop either
{ const NAMES = ['TENNIS BALL (HUGGIE)', 'TENNIS RACKET (HUGGIE)', 'TENNIS RACKET'], ents = new Map(NAMES.map(k => [k, Object.assign({ sku: k }, disc(6, { aiPath: k + '.ai' }))])), loose = new Map(NAMES.map(k => [O.looseKey(k), k]));
  const TITLE = 'Mismatched Tennis Ball and Raquet Huggie Hoops Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold | Handcrafted Sports Jewelry';
  const line = { transactionId: '5213678588', listingId: '1744372161', sku: 'Huggie Hoops-Tennis Ball/Racket3', title: TITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '', variations: [{ name: 'METAL CHOICE', value: 'Silver' }, { name: 'HOOP SIZE', value: '8.5mm' }] };
  const sp = O.interpretLine({ receiptId: '4171010675', updateTs: 1 }, line, { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: s => ents.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose.get(O.looseKey(s)) || '' });
  const sides = (sp.pair.members || []).map(m => m.side + ':' + m.sku).join();
  ok(sides === 'L:TENNIS BALL (HUGGIE),R:TENNIS RACKET (HUGGIE)' && sp.size === '8.5MM', 'the pair is read as in the real order: ' + sides + ' ' + sp.size);
  for (const k of ['TENNIS BALL (HUGGIE)', 'TENNIS RACKET (HUGGIE)']) ok(O.sizeFit(ents.get(k), '8.5MM').action === 'unchecked', `${k}: not resized to the 8.5 mm hoop: ` + JSON.stringify(O.sizeFit(ents.get(k), '8.5MM')));
  ok(O.sizeFit(ents.get('TENNIS RACKET'), '8.5MM').action === 'scale', 'a design this line does not use is not touched by it'); }
console.log('size fit hard OK: ' + n + ' checks');
