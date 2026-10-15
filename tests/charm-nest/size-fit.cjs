// SIZEFIT (Paul, 15 Oct 2026): "All 'Charm Only Disc' listings have an option for the size of the Necklace Charm ... The system needs to incorporate a new check that checks the size of the charm chosen by the user and
// automatically adapts the charm size from the repo to match the buyer chosen size/diameter from the dropdown menu. This must be robust and a standard check when analyzing / triaging order listings."
// The pure side: how a size option is read (any name that speaks of the size, "12mm + engraving" is 12 mm), what the check decides for a line (same / scale / unchecked / ask), and the file scaler the page draws it with.
//   node tests/charm-nest/size-fit.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
const { CharmNestPDF: P, PDFLib } = require(path.join(__dirname, '../../netlify/functions/_charmNestPdf.js'));
let n = 0; const ok = (c, why) => { assert(c, why); n++; };
const near = (a, b, tol, why) => ok(Math.abs(a - b) <= tol, `${why}: ${a} vs ${b}`);
const PT = 72 / 25.4;

// ── 1 · sizeMmOf: only a size in millimetres (or centimetres) is a diameter
ok(O.sizeMmOf('12MM') === 12 && O.sizeMmOf('12mm') === 12 && O.sizeMmOf('12.5MM') === 12.5 && O.sizeMmOf('1.2CM') === 12 && O.sizeMmOf(' 14 MM ') === 14, 'mm and cm tokens');
for (const bad of ['M', 'S', 'XL', '7', '', null, undefined, '12', 'MM', '12 inch', '12MMX']) ok(O.sizeMmOf(bad) === null, 'not a diameter: ' + JSON.stringify(bad));

// ── 2 · sizeFit: the repo holds a round 14 mm disc (a jump ring lengthens it a little; its record carries the margin of the outer box): what is drawn for each size asked
const PAD = O.SIZE_PAD_PT;   // (an index record keeps the OUTER box of a drawing: its margin is not part of the disc)
const disc = (mm, o = {}) => { const w = mm * PT; return Object.assign({ sku: 'WOLF_1234', widthPt: w + PAD, heightPt: w * 1.15 + PAD, areaPt2: Math.PI * w * w / 4 * 1.03, aiPath: 'charmnest/master/WOLF_1234.ai', charmHash: 'h14' }, o); };
const d14 = disc(14);
{ const f10 = O.sizeFit(d14, '10MM'); ok(f10.action === 'scale' && Math.abs(f10.scale - 10 / 14) < 1e-9 && f10.haveMm === 14 && f10.wantMm === 10, '14 mm -> 10 mm: scale 0.714: ' + JSON.stringify(f10));
  const f12 = O.sizeFit(d14, '12MM'); ok(f12.action === 'scale' && Math.abs(f12.scale - 12 / 14) < 1e-9, '14 -> 12');
  const f14 = O.sizeFit(d14, '14MM'); ok(f14.action === 'same', 'the very size drawn: same, nothing is scaled');
  ok(O.sizeFit({ sku: 'D', widthPt: 37, heightPt: 42.2, areaPt2: 973.8, aiPath: 'd' }, '12MM').action === 'same', 'the real 12 mm disc (record: 37 x 42.2 pt with its margin) is 12 mm, not 13: nothing is scaled');
  const f141 = O.sizeFit(d14, '14.1MM'); ok(f141.action === 'same', 'within 0.3 mm: the same');
  const f16 = O.sizeFit(d14, '16MM'); ok(f16.action === 'scale' && f16.scale > 1.14 && f16.scale < 1.15, 'a bigger one is scaled up too'); }
// a design that holds sizes: the very size is drawn as it is (null), else the NEAREST drawn size is scaled
{ const sized = { sku: 'W', sizes: { '12MM': disc(12, { aiPath: 'a12' }), '14MM': disc(14, { aiPath: 'a14' }) } };
  ok(O.sizeFit(sized, '12MM') === null && O.sizeFit(sized, '14MM') === null, 'a size the library holds is read as it is (null)');
  const f10 = O.sizeFit(sized, '10MM'); ok(f10.action === 'scale' && f10.base === '12MM' && Math.abs(f10.scale - 10 / 12) < 1e-9, '10 mm from the nearest (12 mm): ' + JSON.stringify(f10));
  const f13 = O.sizeFit(sized, '13MM'); ok(f13.action === 'scale' && ['12MM', '14MM'].includes(f13.base), '13 mm from a neighbour: ' + f13.base); }
// not a round disc: never resized on a guess
{ const sq = { sku: 'SQ', widthPt: 14 * PT, heightPt: 14 * PT, areaPt2: 14 * PT * 14 * PT, aiPath: 'sq' };    // a square-ish piece: its area is not a circle's
  const f = O.sizeFit(sq, '12MM'); ok(f.action === 'unchecked' && /not a round disc/.test(f.why), 'a square is left alone: ' + JSON.stringify(f));
  const bar = { sku: 'BAR', widthPt: 30 * PT, heightPt: 6 * PT, areaPt2: 30 * PT * 6 * PT * .9, aiPath: 'bar' }; ok(O.sizeFit(bar, '12MM').action === 'unchecked', 'a bar is left alone');
  ok(O.sizeFit({ sku: 'X', aiPath: 'x' }, '12MM').action === 'unchecked', 'a design with no measurements is left alone, never guessed'); }
// a size that is not an adaptation but a mistake of the listing or the drawing
{ const f = O.sizeFit(d14, '40MM'); ok(f.action === 'ask' && /more than 2\.5 times bigger/.test(f.why), '40 mm from 14 mm asks: ' + JSON.stringify(f));
  ok(O.sizeFit(d14, '4MM').action === 'ask', '4 mm from 14 mm asks (too small to be a rescale)'); ok(O.sizeFit(d14, '2MM') === null, 'under 3 mm is no diameter at all'); }
// no diameter word, nothing to check
for (const s of ['M', 'S', '7', null, '']) ok(O.sizeFit(d14, s) === null, 'size ' + JSON.stringify(s) + ' is not checked');
ok(O.sizeFit(null, '12MM') === null, 'no entry');

// ── 3 · reading the option: any name that speaks of the size, with the buyer's own words after the number
const order = { receiptId: '4199999999', updateTs: 1, staffNote: '', buyerMessage: '' };
const ctxOf = e => ({ optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => (String(s).toUpperCase() === 'WOLF_1234' ? e : null) });
const lineOf = (name, value, extra) => Object.assign({ transactionId: '1', listingId: '1111', sku: 'WOLF_1234', title: 'Wolf Charm Only Disc Necklace', quantity: 1, metalKey: 'ss', metalLabel: 'Sterling Silver', personalization: [], variations: [{ name, value }] }, extra || {});
const NAMES = ['Size', 'Charm Size', 'Necklace Charm Size', 'Size of Necklace Charm', 'Size of the Necklace Charm (NO Huggie option)', 'Charm size (necklace)', 'Pendant Size', 'Disc Size', 'Diameter', 'Charm Diameter'];
const VALUES = [['10mm', '10MM'], ['10mm + engraving', '10MM'], ['10 mm + Engraving', '10MM'], ['12mm + engraving', '12MM'], ['12MM', '12MM'], ['14mm', '14MM'], ['14 mm (with engraving)', '14MM'], ['Large (14mm)', '14MM'], ['1.2 cm', '12MM']];
for (const name of NAMES) for (const [v, want] of VALUES) {
  const sp = O.interpretLine(order, lineOf(name, v), ctxOf(d14));
  ok(sp.size === want, `"${name}: ${v}" is size ${want}, read ${sp.size}`);
  ok(!sp.problems.some(p => p.kind === 'needsMapping'), `"${name}: ${v}" asks nothing about the option: ${JSON.stringify(sp.problems.map(p => p.kind))}`);
}
{ const sp = O.interpretLine(order, lineOf('Size of Necklace Charm', '10mm + engraving'), ctxOf(d14));
  ok(sp.sizeFit && sp.sizeFit.action === 'scale' && Math.abs(sp.sizeFit.scale - 10 / 14) < 1e-9 && !sp.problems.some(p => p.kind === 'missingSize'), 'the line carries the check: spec.sizeFit scale 0.714, nothing held for the size');
  const same = O.interpretLine(order, lineOf('Size of Necklace Charm', '14mm'), ctxOf(d14)); ok(same.sizeFit && same.sizeFit.action === 'same', 'the size drawn: same');
  const big = O.interpretLine(order, lineOf('Size of Necklace Charm', '40mm'), ctxOf(d14));
  ok(big.sizeFit.action === 'ask' && big.problems.some(p => p.kind === 'missingSize' && /2\.5 times/.test(p.why || '')), 'an impossible size holds the line with the reason: ' + JSON.stringify(big.problems.map(p => p.kind)));
  // a sized design that holds 12 and 14 only: 10 mm is scaled from 12, not held
  const sized = { sku: 'WOLF_1234', sizes: { '12MM': disc(12, { aiPath: 'a12' }), '14MM': disc(14, { aiPath: 'a14' }) }, aiPath: null };
  const s10 = O.interpretLine(order, lineOf('Charm Size', '10mm'), ctxOf(sized));
  ok(s10.sizeFit && s10.sizeFit.action === 'scale' && s10.sizeFit.base === '12MM' && !s10.problems.some(p => p.kind === 'missingSize'), 'a size the library lacks, scaled from the nearest it holds: ' + JSON.stringify(s10.sizeFit));
  const s12 = O.interpretLine(order, lineOf('Charm Size', '12mm'), ctxOf(sized)); ok(s12.sizeFit === undefined && !s12.problems.some(p => p.kind === 'missingSize'), 'a size the library holds: no check needed');
  // a sized design that is not a disc still holds the line for a size it lacks, as it always did
  const sq = { sku: 'WOLF_1234', sizes: { '12MM': { widthPt: 40, heightPt: 40, areaPt2: 1000, aiPath: 'q' } } };
  const sqq = O.interpretLine(order, lineOf('Charm Size', '10mm'), ctxOf(sq)); ok(sqq.problems.some(p => p.kind === 'missingSize'), 'not a disc: a missing size still holds the line as before'); }
// the older words are read as they always were: Ring Size 7, S/M/L, the option that is not a size at all
{ const r = O.interpretLine(order, lineOf('Ring Size', '7'), ctxOf(d14)); ok(r.size === '7' && !r.sizeFit, 'Ring Size 7 as before, no diameter check');
  const m = O.interpretLine(order, lineOf('Size', 'Medium'), ctxOf(d14)); ok(m.size === 'M' && !m.sizeFit, 'Medium is M as before');
  const ch = O.interpretLine(order, lineOf('Necklace Length', '16 inch'), ctxOf(d14)); ok(!ch.size, 'a necklace length is no size');
  const two = O.interpretLine(order, lineOf('Size of Necklace Charm', '10mm or 12mm'), ctxOf(d14)); ok(!two.size, 'two sizes in one value are never guessed');
  const chain = O.interpretLine(order, lineOf('Chain length', '16mm'), ctxOf(d14)); ok(!chain.size, 'a chain option never becomes the size'); }

// ── 4 · scaleSource: the drawing at the diameter chosen, read back by the app's own reader
(async () => {
  const { PDFDocument, PDFName } = PDFLib;
  const f = x => (Math.round(x * 1000) / 1000).toString();
  const circle = (cx, cy, r) => { const k = 0.5522847498 * r; return `${f(cx + r)} ${f(cy)} m ${f(cx + r)} ${f(cy + k)} ${f(cx + k)} ${f(cy + r)} ${f(cx)} ${f(cy + r)} c ${f(cx - k)} ${f(cy + r)} ${f(cx - r)} ${f(cy + k)} ${f(cx - r)} ${f(cy)} c ${f(cx - r)} ${f(cy - k)} ${f(cx - k)} ${f(cy - r)} ${f(cx)} ${f(cy - r)} c ${f(cx + k)} ${f(cy - r)} ${f(cx + r)} ${f(cy - k)} ${f(cx + r)} ${f(cy)} c h`; };
  // a 14 mm disc with a hanging hole, its page 80 x 80 pt, content in TWO streams (as Illustrator splits it), a crop box too
  const R = 14 * PT / 2;
  const doc = await PDFDocument.create(), page = doc.addPage([80, 80]);
  page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(`q 0 0 0 RG 0.5 w ${circle(40, 40, R)} S\n`)), doc.context.register(doc.context.flateStream(`${circle(40, 40 + R - 3, 1.5)} S Q\n`))]));
  page.setCropBox(5, 5, 70, 70);
  const bytes = new Uint8Array(await doc.save({ useObjectStreams: false }));
  const read = async b => { const parsed = await P.parseSource(b, 'd.ai'), g = P.groupCharms(parsed, { minPt: 6 }); return { parsed, g, charm: g.charms[0] }; };
  const base = await read(bytes);
  ok(base.g.charms.length === 1 && P.cutLinesOf(base.charm).length === 1, 'the test disc reads as one charm with one hole');
  const w0 = base.charm.bbox[2] - base.charm.bbox[0];
  near(w0 / PT, 14, 0.05, 'the test disc is 14 mm');
  for (const k of [10 / 14, 12 / 14, 1, 1.25, 0.4, 2.5]) {
    const out = await P.scaleSource(bytes, k), got = await read(out), w = got.charm.bbox[2] - got.charm.bbox[0], h = got.charm.bbox[3] - got.charm.bbox[1];
    ok(got.g.charms.length === 1 && P.cutLinesOf(got.charm).length === 1, `k=${k.toFixed(3)}: still one charm, one hole`);
    near(w / PT, 14 * k, 0.03, `k=${k.toFixed(3)}: diameter in mm`); near(h / PT, 14 * k, 0.03, `k=${k.toFixed(3)}: height in mm`);
    near(got.parsed.pageW, 80 * k, 0.01, `k=${k.toFixed(3)}: the page is scaled with it`);
    near(got.charm.outline.lwPt, 0.5 * k, 0.001, `k=${k.toFixed(3)}: strokes scale with the drawing`);
    const cb = got.parsed.page.node.lookup(PDFName.of('CropBox')); ok(cb && cb.asArray().map(x => x.asNumber())[2] > 0, 'the crop box is kept');
    near(got.parsed.page.node.lookup(PDFName.of('CropBox')).asArray()[2].asNumber(), 75 * k, 0.01, `k=${k.toFixed(3)}: the crop box is scaled`);
  }
  // twice is the same as once (10/14 then 14/10 is the very drawing again)
  { const twice = await read(await P.scaleSource(await P.scaleSource(bytes, 10 / 14), 14 / 10)); near(twice.charm.bbox[2] - twice.charm.bbox[0], w0, 0.01, 'scaled down and back up: the same disc'); }
  // refusals
  for (const [why, fn] of [['not a PDF', () => P.scaleSource(new Uint8Array([1, 2, 3, 4]), 1.2)], ['no bytes', () => P.scaleSource(null, 1.2)], ['zero', () => P.scaleSource(bytes, 0)], ['NaN', () => P.scaleSource(bytes, NaN)], ['negative', () => P.scaleSource(bytes, -1)], ['huge', () => P.scaleSource(bytes, 100)], ['tiny', () => P.scaleSource(bytes, 0.001)]]) {
    let threw = false; try { await fn(); } catch (_) { threw = true; } ok(threw, 'scaleSource refuses ' + why);
  }
  // the input is never changed
  const copy = Uint8Array.from(bytes); await P.scaleSource(bytes, 0.8); ok(Buffer.compare(Buffer.from(copy), Buffer.from(bytes)) === 0, 'the bytes given are not changed');
  console.log('size fit OK: ' + n + ' checks');
})().catch(e => { console.error(e); process.exit(1); });
