// Sample text and callouts in a master (the "blurred writing" on library cards, Paul 8 Oct 2026, image #11).
// A master is also the artists' workbook: a customer's example words are typed as live text, one text object per letter
// along a curve, then outlined, and the live copy is kept; a grey "11.1 mm" callout stands under the charm. Both used to
// be read as the charm's own detail (grey placeholder boxes, "22 holes", the hanging-hole search taking a letter, a wrong
// size, the text written into the per-SKU file) and the callout was read as the SKU ("11.1 MM").
// This builds those cases on a page and checks the grouping, the labels and the per-SKU file; it also checks that words
// that exist only as outlined artwork keep their text, and that a hoop ring is never taken for a letter.
//   node tests/charm-nest/sample-text.cjs
const assert = require('node:assert/strict');
const path = require('node:path');
const { PDFDocument, PDFName, rgb, StandardFonts, degrees } = require(path.join(__dirname, '../../vendor/pdf-lib-1.17.1.min.js'));
const { CharmNestPDF: P, Geom: G } = require('../../netlify/functions/_charmNestPdf.js');
const MM = 72 / 25.4;

const num = n => (+n).toFixed(3);
const circle = (cx, cy, r) => { const k = 0.5523 * r; return `${num(cx + r)} ${num(cy)} m ${num(cx + r)} ${num(cy + k)} ${num(cx + k)} ${num(cy + r)} ${num(cx)} ${num(cy + r)} c ${num(cx - k)} ${num(cy + r)} ${num(cx - r)} ${num(cy + k)} ${num(cx - r)} ${num(cy)} c ${num(cx - r)} ${num(cy - k)} ${num(cx - k)} ${num(cy - r)} ${num(cx)} ${num(cy - r)} c ${num(cx + k)} ${num(cy - r)} ${num(cx + r)} ${num(cy - k)} ${num(cx + r)} ${num(cy)} c h`; };
const box = (x, y, w, h) => `${num(x)} ${num(y)} m ${num(x + w)} ${num(y)} l ${num(x + w)} ${num(y + h)} l ${num(x)} ${num(y + h)} l h`;
const stroke = p => `q 0 0 0 RG 0.4 w ${p} S Q`;

/** One page: charm A (sample text on it, callout + real label under it), charm B (outlined words only), charm C (only a callout under it). */
async function build() {
  const doc = await PDFDocument.create();
  const W = 220 * MM, H = 120 * MM;
  const page = doc.addPage([W, H]);
  const font = await doc.embedFont(StandardFonts.Helvetica);
  const ops = [];
  const cy = H - 40 * MM, sA = 32 * MM, sB = 32 * MM, xA = 40 * MM, xB = 110 * MM, xC = 180 * MM;
  const outline = (cx, w, h) => stroke(box(cx - w / 2, cy - h / 2, w, h));
  // A: a 32 x 28 mm body with a hanging hole at the top right, as a charm of the library
  ops.push(outline(xA, sA, 28 * MM)); ops.push(stroke(circle(xA + 11 * MM, cy + 9 * MM, 1.3 * MM)));
  // B: the same body with a script word that exists only as outlined artwork (no live copy), blue engraving
  ops.push(outline(xB, sB, 28 * MM)); ops.push(stroke(circle(xB + 11 * MM, cy + 9 * MM, 1.3 * MM)));
  // C: a body whose only text under it is a measurement
  ops.push(outline(xC, sB, 28 * MM)); ops.push(stroke(circle(xC + 11 * MM, cy + 9 * MM, 1.3 * MM)));
  // the sample word on A: live text letter by letter along a slope, each letter with its outlined twin (a small closed square)
  const word = 'TAEKWONDO', letters = [];
  for (let i = 0; i < word.length; i++) {
    const x = xA - 12 * MM + i * 2.6 * MM, y = cy - 6 * MM + i * 0.9 * MM;
    letters.push({ ch: word[i], x, y });
    ops.push(stroke(box(x + 0.3, y + 0.2, 2.2, 2.4)));
  }
  // B's outlined script: the same kind of small closed shapes, nothing live behind them
  for (let i = 0; i < 8; i++) ops.push(`q 0.1 0.2 1 RG 0.4 w ${box(xB - 10 * MM + i * 2.4 * MM, cy - 6 * MM, 2.2, 2.4)} S Q`);
  const stream = doc.context.flateStream(ops.join('\n') + '\n');
  page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(stream)]));
  for (const l of letters) page.drawText(l.ch, { x: l.x, y: l.y, size: 3, font, color: rgb(0, 0, 0), rotate: degrees(20) });
  const label = (cx, text, color, dy) => { const w = font.widthOfTextAtSize(text, 5); page.drawText(text, { x: cx - w / 2, y: cy - 14 * MM - (dy || 2) * MM - 3.75, size: 5, font, color }); };
  label(xA, '11.1 mm', rgb(0.47, 0.47, 0.47), 2);             // the callout stands first under the charm: it was its "SKU"
  label(xA, 'TAEKWONDO BELT', rgb(0.1, 0.1, 0.1), 4.5);       // the stacked second line is the real SKU
  label(xB, 'SCRIPT_100', rgb(0.1, 0.1, 0.1), 2);
  label(xC, 'OPTION B', rgb(0.47, 0.47, 0.47), 2);
  return { bytes: await doc.save({ useObjectStreams: false }), xA, xB, xC };
}

(async () => {
  // ── the label rule ──
  for (const s of ['11.1 mm', '11.4 MM', '14MM', '8 MM', '1 in', '0.32 in', '12.5 cm', 'FRONT', 'BACK', 'Front Back', 'FRONT/BACK', 'OPTION', 'OPTION B', 'Original Size', 'ORIG SIZE'])
    assert.equal(P.parseSkuLabel(s), null, `${s} is a callout, not a SKU`);
  for (const s of ['TAEKWONDO BELT', 'Hedgehog_88074', 'HUGGIE HOOPS- CROCODILE', 'LETTER_EARRING-0', 'Cheer 1 - Megaphone(CHEER)', 'MM-CHARM', 'BACKPACK_12', 'FRONTIER_1', 'OPTIONS_5', '19'])
    assert.ok(P.parseSkuLabel(s), `${s} stays a SKU`);

  const { bytes } = await build();
  const parsed = await P.parseSource(new Uint8Array(bytes), 'sample-text.ai');
  const old = P.groupCharms(parsed, { minPt: 6, keepSampleText: true });
  const g = P.groupCharms(parsed, { minPt: 6 });
  assert.equal(g.charms.length, 3, 'three charms');
  const at = (grp, x) => grp.charms.find(c => x >= c.outline.bbox[0] && x <= c.outline.bbox[2]);
  const A0 = at(old, 40 * MM), A = at(g, 40 * MM), B = at(g, 110 * MM), Cc = at(g, 180 * MM);

  // before (keepSampleText = the old behaviour): the live letters are members, and the twins count as holes
  assert.equal(A0.members.filter(m => m.kind === 'text' && /^[A-Z]$/.test(m.str || '')).length >= 9, true, 'old behaviour: the nine live letters were members');
  assert.ok(P.cutLinesOf(A0).length >= 10, 'old behaviour: every outlined letter counted as a hole: ' + P.cutLinesOf(A0).length);

  // the labels are read the way the indexer reads them: the label text then leaves every charm
  const lab = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 8 * MM, widen: 0.25 });
  assert.equal(lab.labels.get(A.index).sku, 'TAEKWONDO BELT', 'A is TAEKWONDO BELT, not 11.1 MM');
  assert.equal(lab.labels.get(A.index).extra.length, 0, 'the callout is not a second SKU');
  assert.equal(lab.labels.get(B.index).sku, 'SCRIPT_100');
  assert.ok(!lab.labels.has(Cc.index) && lab.unlabelled.includes(Cc.index), 'C had only a callout under it: it is unlabelled, so it is not indexed');


  // after: A keeps its body and its one real hole, nothing else
  const ob = A.outline.bbox, onPiece = m => m.bbox[0] < ob[2] && m.bbox[2] > ob[0] && m.bbox[1] < ob[3] && m.bbox[3] > ob[1];
  assert.equal(A.members.filter(m => m.kind === 'text' && onPiece(m)).length, 0, 'no live text is left ON the charm (a callout beside it is the markers\' business)');
  assert.equal(P.cutLinesOf(A).length, 1, 'only the hanging hole is a cut-out: ' + P.cutLinesOf(A).length);
  const own = A.members.filter(m => !(m.kind === 'text' && !onPiece(m)));   // (a callout beside the charm may still be a member where markers are not taken out yet)
  assert.equal(own.length, 2, 'outline and hole: ' + own.length);
  assert.equal(own.every(m => m.kind === 'path'), true);
  const taken = g.sampleText.filter(t => t.charm === A.index);
  assert.equal(taken.filter(t => t.why === 'live text').length, 9, 'nine live letters taken');
  assert.equal(taken.filter(t => t.why !== 'live text').length, 9, 'nine outlined letters taken');
  // the hanging-hole search finds the real hole (upper right of the body), not a letter
  const up = G.upAngleOf(A);
  assert.ok(up.angle > 15 && up.angle < 60, 'up angle points at the real hanging hole: ' + up.angle);
  const upOld = G.upAngleOf(A0);
  assert.ok(Math.abs(upOld.angle - up.angle) > 5 || P.cutLinesOf(A0).length > 1, 'the letters used to compete for the hanging hole');

  // B: words that exist only as outlined artwork are the design and keep their text
  assert.equal(B.members.length, 10, 'B keeps its outline, hole and eight outlined script shapes: ' + B.members.length);
  assert.equal(g.sampleText.filter(t => t.charm === B.index).length, 0, 'nothing taken from B');

  // the per-SKU file: no text objects at all, no outlined letters
  const ai = await P.buildSingleCharm(A, parsed);
  const again = await P.parseSource(new Uint8Array(ai), 'A.ai');
  assert.equal(again.counts.text, 0, 'the per-SKU file carries no text');
  const g2 = P.groupCharms(again, { minPt: 6 });
  assert.equal(g2.charms.length, 1);
  assert.equal(P.cutLinesOf(g2.charms[0]).length, 1, 'the per-SKU file has the one hole');
  assert.equal(g2.charms[0].members.filter(m => m.kind !== 'text').length, 2, 'and nothing else');

  // fake segments: the layer rule and the hoop ring
  const seg = (o) => Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], paintOp: 'S', subpaths: [[['m', [0, 0]], ['h']]], lwPt: 0.2 }, o);
  const outline2 = seg({ bbox: [0, 0, 40, 40], layer: 'CUT' });
  const live = { kind: 'text', str: 'Jo', bbox: [10, 10, 16, 14], layer: 'HATCH', fillRGB: [0, 0, 1] };
  const sameLayer = seg({ bbox: [10, 10, 12, 12], layer: 'HATCH' }), otherLayer = seg({ bbox: [12, 10, 14, 12], layer: 'CUT' }), big = seg({ bbox: [10, 10, 30, 30], layer: 'HATCH' });
  const cutText = { kind: 'text', str: 'Jo', bbox: [10, 10, 16, 14], layer: 'CUT', fillRGB: [0, 0, 0] };
  const ring = seg({ bbox: [11, 9, 16, 14], layer: 'CUT', subpaths: [[['m', [0, 0]], ['c', [0, 1], [1, 1], [1, 0]], ['h']]] });   // a hoop ring on the cut layer, sitting where the sample text is
  assert.ok(P.ringLike(ring), 'the fake ring is a hoop ring');
  const fake = { outline: outline2, members: [outline2, live, cutText, sameLayer, otherLayer, big, ring] };
  const why = P.sampleTextOf(fake);
  assert.ok(why.has(live) && why.has(sameLayer), 'live text and its letter on the same layer are sample text');
  assert.ok(why.has(cutText), 'text on the cut layer is sample text too');
  assert.ok(!why.has(otherLayer) || why.get(otherLayer) === 'outlined letter of live text', 'the CUT-layer shape is a twin only of the CUT-layer text');
  assert.ok(!P.sampleTextOf({ outline: outline2, members: [outline2, live, otherLayer] }).has(otherLayer), 'a shape on another layer than the text is not its twin');
  assert.ok(!why.has(big), 'a shape far bigger than a letter is not its twin');
  assert.ok(!why.has(ring), 'a hoop ring is never a letter');
  assert.ok(!why.has(outline2), 'the outline is never taken');

  console.log('sample-text OK · live text and its outlined letters leave the charm, callouts are no SKU, outlined-only words stay');
})().catch(e => { console.error(e); process.exit(1); });
