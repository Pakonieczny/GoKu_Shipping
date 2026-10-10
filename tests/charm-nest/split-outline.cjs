// LIVING #721 (ARMY_83276, ENGRAVED_71405, GRENADE_45535, SOLDIER_63845, TACTICAL_27863, TACTICAL_76428 are six SKU names under ONE
// grenade): above the finished grenade the master holds a second, unlabelled drawing of it whose cut line is two OPEN pieces, the top
// (lever, handle and the top of the body, with the handle's closed hole as a second subpath of the same path) and the bottom (half the
// body). They end 1.13 pt from each other on both sides of a red stripe band. The first chaining pass (1 pt, single-subpath strokes) skipped
// them, the bottom piece fell to the grenade as a loose "detail" and the card showed a half circle floating above it (10.4 x 18.5 mm).
// The second chaining pass (charm-nest-pdf.js "an outline drawn in two open pieces") makes the two pieces one outline of their own.
// The structure is the master's: layer markers, one `q ... S Q` per object, the stripe band a closed red rectangle on ENGRAVE.
//   node tests/charm-nest/split-outline.cjs                      (READER=/path/to/charm-nest-pdf.js runs it against another copy, e.g. the old one)
const assert = require('node:assert/strict');
global.self = global;
global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js');
require(process.env.READER || '../../charm-nest-pdf.js');
const P = global.CharmNestPDF;
const { PDFDocument, PDFName, PDFString, StandardFonts } = PDFLib;

const K = 0.5523, n3 = v => (+v).toFixed(3), pt = (x, y) => `${n3(x)} ${n3(y)}`;
// the top half of an ellipse (centre cx, y0 on its diameter), drawn right to left: open, chord 2a long
const top = (cx, y0, a, b) => `${pt(cx + a, y0)} m ${pt(cx + a, y0 + K * b)} ${pt(cx + K * a, y0 + b)} ${pt(cx, y0 + b)} c ${pt(cx - K * a, y0 + b)} ${pt(cx - a, y0 + K * b)} ${pt(cx - a, y0)} c`;
// the bottom half, drawn left to right
const bottom = (cx, y0, a, b) => `${pt(cx - a, y0)} m ${pt(cx - a, y0 - K * b)} ${pt(cx - K * a, y0 - b)} ${pt(cx, y0 - b)} c ${pt(cx + K * a, y0 - b)} ${pt(cx + a, y0 - K * b)} ${pt(cx + a, y0)} c`;
const circle = (cx, cy, r) => `${pt(cx + r, cy)} m ${pt(cx + r, cy + K * r)} ${pt(cx + K * r, cy + r)} ${pt(cx, cy + r)} c ${pt(cx - K * r, cy + r)} ${pt(cx - r, cy + K * r)} ${pt(cx - r, cy)} c ${pt(cx - r, cy - K * r)} ${pt(cx - K * r, cy - r)} ${pt(cx, cy - r)} c ${pt(cx + K * r, cy - r)} ${pt(cx + r, cy - K * r)} ${pt(cx + r, cy)} c h`;
const blob = (cx, cy, w, h) => { const a = w / 2, b = h / 2; return `${pt(cx + a, cy)} m ${pt(cx + a, cy + K * b)} ${pt(cx + K * a, cy + b)} ${pt(cx, cy + b)} c ${pt(cx - K * a, cy + b)} ${pt(cx - a, cy + K * b)} ${pt(cx - a, cy)} c ${pt(cx - a, cy - K * b)} ${pt(cx - K * a, cy - b)} ${pt(cx, cy - b)} c ${pt(cx + K * a, cy - b)} ${pt(cx + a, cy - K * b)} ${pt(cx + a, cy)} c h`; };

// A: the finished grenade (closed) with its label; X: the same drawing split in two open pieces 1.14 pt apart (the band is the gap), above it.
// Controls, each a split body that must NOT be chained: Y 1.6 pt apart (too far), Z two pen widths, W tiny, V a figure of eight (the ends mate crosswise).
async function master() {
  const doc = await PDFDocument.create();
  const page = doc.addPage([400, 200]);
  const font = await doc.embedFont(StandardFonts.Helvetica);
  const fkey = page.node.newFontDictionary(font.name, font.ref).toString();
  const ocg = n => doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFString.of(n) }));
  page.node.Resources().set(PDFName.of('Properties'), doc.context.obj({ MC1: ocg('ENGRAVE'), MC3: ocg('CUT') }));
  const ops = [], gapY = 1.14, Y0 = 80;
  ops.push('/OC /MC1 BDC', 'q 1 0.114 0.145 RG 0.283 w');
  ops.push(`${pt(50 - 10, Y0 - gapY / 2)} ${n3(20)} ${n3(gapY)} re S`);                  // the red stripe band between the two pieces of X (a closed rectangle)
  ops.push(`${pt(40, 40)} m ${pt(60, 50)} l S`);                                         // a red stripe across A
  ops.push('Q', 'EMC');
  ops.push('/OC /MC3 BDC', 'q 0 0 0 RG 0.283 w');
  ops.push(`${blob(50, 45, 24, 24)} S`);                                                  // A: the finished grenade body, closed
  ops.push(`${top(50, Y0 + gapY / 2, 10, 14)} ${circle(50, Y0 + 8, 2.5)} S`);                // X top: an open run + the closed hole of the handle, ONE path
  ops.push(`${bottom(50, Y0 - gapY / 2, 10, 10)} S`);                                    // X bottom: the other open run, 1.14 pt below the top's ends
  ops.push(`${top(130, Y0 + 0.8, 10, 14)} S`, `${bottom(130, Y0 - 0.8, 10, 10)} S`);      // Y: ends 1.6 pt apart
  ops.push(`${top(190, Y0 + gapY / 2, 10, 14)} S`, '0.6 w', `${bottom(190, Y0 - gapY / 2, 10, 10)} S`, '0.283 w');   // Z: the lower piece is drawn with another pen
  ops.push(`${top(250, Y0 + gapY / 2, 2.5, 2.5)} S`, `${bottom(250, Y0 - gapY / 2, 2.5, 2.5)} S`);                     // W: a split body 5 pt wide (less than a charm)
  // V: a top piece and a bottom piece whose ends mate (1.14 pt) but the bottom is a looped curve that crosses itself: the loop is not simple
  ops.push(`${top(310, Y0 + gapY / 2, 10, 14)} S`, `${pt(320, Y0 - gapY / 2)} m ${pt(285, Y0 + 35)} ${pt(335, Y0 + 35)} ${pt(300, Y0 - gapY / 2)} c S`);
  ops.push('Q', 'EMC');
  for (const [t, x, y] of [['GRENADE_TEST', 33, 28]]) ops.push(`BT ${fkey} 3 Tf 1 0 0 1 ${x} ${y} Tm (${t}) Tj ET`);
  page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(ops.join('\n') + '\n'))]));
  return new Uint8Array(await doc.save({ useObjectStreams: false }));
}

const nearC = (c, x, y, tol = 4) => { const b = c.outline.bbox; return Math.abs((b[0] + b[2]) / 2 - x) <= tol && Math.abs((b[1] + b[3]) / 2 - y) <= tol; };
(async () => {
  const bytes = await master();
  const parsed = await P.parseSource(bytes, 'master.ai');
  const cut = parsed.segments.concat(parsed.nested).filter(s => s.kind === 'path' && s.layer === 'CUT');
  const xTop = cut.find(s => s.bbox[0] > 38 && s.bbox[2] < 62 && s.bbox[1] > 78 && s.subpaths.length === 2), xBot = cut.find(s => s.bbox[0] > 38 && s.bbox[2] < 62 && s.bbox[3] < 81 && s.bbox[1] > 65 && s.subpaths.length === 1);
  assert(xTop && !xTop.closed && xBot && !xBot.closed, 'X: the master gives two open cut paths, the top with two subpaths (the open run and the closed hole)');
  const g = P.groupCharms(parsed, { minPt: 6 });
  const labels = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / (25.4 / 72), widen: 0.25 });
  const A = g.charms.find(c => nearC(c, 50, 45, 3));
  assert(A && labels.labels.has(A.index), 'A: the finished grenade is a charm with its label');
  // the grenade keeps only its own drawing: the bottom piece of X is no longer its "detail"
  assert(!A.members.some(m => m === xBot || m === xTop), 'A: none of the open pieces of X is a member of the finished grenade (' + A.members.map(m => m.layer + (m.closed ? '' : ' open') + ' ' + m.bbox.map(Math.round)).join(' | ') + ')');
  assert(A.bbox[3] < 60, 'A: the grenade card is one body tall (top edge ' + A.bbox[3].toFixed(1) + ' pt), no loose arc above it');
  // X is one outline of its own: a synthetic closed chain whose real parts are its members, made by the second pass
  const X = g.charms.find(c => c.outline.synthetic && c.outline.parts.includes(xBot));
  assert(X && X.outline.splitOutline === true && X.outline.parts.length === 2 && X.outline.parts.includes(xTop), 'X: the two open pieces are chained into one outline by the second pass');
  assert(X.outline.closed && X.members.includes(xTop) && X.members.includes(xBot), 'X: the chain is closed and both pieces are its members');
  assert(!labels.labels.has(X.index), 'X: it has no label of its own (the SKUs stay on the finished grenade)');
  assert(X.members.some(m => m.layer === 'ENGRAVE'), 'X: its red stripe band belongs to it');
  // the controls are not chained
  const chains = g.charms.filter(c => c.outline.synthetic);
  assert.equal(chains.length, 1, 'only X is chained: Y (1.6 pt apart), Z (two pens), W (5 pt wide) and V (a loop that would cross itself) are not; got ' + chains.map(c => c.outline.bbox.map(Math.round)).join(' | '));
  // the first pass is as it was: a chain it makes carries no splitOutline mark (the bar of the older masters: a U-shape plus a line)
  const p2 = await P.parseSource(await (async () => {
    const doc = await PDFDocument.create(); const page = doc.addPage([100, 60]);
    page.node.Resources().set(PDFName.of('Properties'), doc.context.obj({ MC3: doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFString.of('CUT') })) }));
    const ops = ['/OC /MC3 BDC', 'q 0 0 0 RG 0.283 w', '20 20 m 20 40 l 70 40 l 70 20 l S', '20 20 m 70 20 l S', 'Q', 'EMC'];   // a U-shape and the line that closes it
    page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(ops.join('\n') + '\n'))]));
    return new Uint8Array(await doc.save({ useObjectStreams: false }));
  })(), 'bar.ai');
  const g2 = P.groupCharms(p2, { minPt: 6 });
  assert(g2.charms.length === 1 && g2.charms[0].outline.synthetic && !g2.charms[0].outline.splitOutline, 'the first pass still chains a U-shape and its line, and does not mark it as a second-pass chain');
  console.log('split-outline OK: two open pieces of one outline (1.13 pt apart, one with a hole) are one outline, the finished grenade keeps no loose arc, four controls are not chained');
})().catch(e => { console.error(e); process.exit(1); });
