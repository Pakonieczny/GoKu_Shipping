// Blue hatching must stay blue in every per-SKU file. Illustrator opens the HATCH layer with a text run that carries
// `/CS0 cs 0 0 1 scn`; every hatch object after it only inherits that fill. The per-SKU writer blanks the text run of
// every charm that does not own it, so the colour was lost and the hatching was read back as the default black
// (ANGEL_93539, BASEBALL_91540, SPORTS9-RUNNING SHOE, FIRE DEPT BADGE and every other charm whose hatching inherits the
// layer's fill). The structure here is the master's: layer markers, the opening text run, `q cm path f Q` per object.
//   node tests/charm-nest/hatch-colour-state.cjs
const assert = require('node:assert/strict');
global.self = global;
global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js');
require('../../charm-nest-pdf.js');
const P = global.CharmNestPDF;
const { PDFDocument, PDFName, PDFString, StandardFonts } = PDFLib;

const SKUS = ['ANGEL_93539', 'BASEBALL_91540', 'SPORTS9-RUNNING SHOE', 'FIRE DEPT BADGE'];
const num = n => (+n).toFixed(3);
const box = (x, y, w, h) => `${num(x)} ${num(y)} m ${num(x + w)} ${num(y)} l ${num(x + w)} ${num(y + h)} l ${num(x)} ${num(y + h)} l h`;
const wing = (x, y) => `${box(x + 8, y + 8, 6, 3)} ${box(x + 8, y + 14, 6, 3)}`;   // a compound path: the hatching of one design

async function master() {
  const doc = await PDFDocument.create();
  const page = doc.addPage([400, 140]);
  const font = await doc.embedFont(StandardFonts.Helvetica);
  const fkey = page.node.newFontDictionary(font.name, font.ref).toString();
  const ocg = n => doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFString.of(n) }));
  const res = page.node.Resources();
  res.set(PDFName.of('Properties'), doc.context.obj({ MC1: ocg('ENGRAVE'), MC2: ocg('HATCH'), MC3: ocg('CUT') }));
  res.set(PDFName.of('ColorSpace'), doc.context.obj({ CS0: doc.context.obj([PDFName.of('CalRGB'), doc.context.obj({ WhitePoint: doc.context.obj([0.95, 1, 1.09]) })]) }));
  const at = i => [20 + i * 90, 40];
  const ops = [];
  // layer 1: red engraving strokes, each object sets its own colour (they were always read right)
  ops.push('/OC /MC1 BDC', 'q');
  SKUS.forEach((s, i) => { const [x, y] = at(i); ops.push(`q /CS0 CS 1 0 0 SCN 0.25 w ${box(x + 2, y + 2, 4, 4)} S Q`); });
  ops.push('Q', 'EMC');
  // layer 2: HATCH. The opening text run sets the layer's fill once; the objects only inherit it.
  ops.push('/OC /MC2 BDC', 'q', `BT /CS0 cs 0 0 1 scn ${fkey} 1 Tf 1 0 0 1 5 5 Tm (SPARE) Tj ET`);
  SKUS.forEach((s, i) => { const [x, y] = at(i); ops.push(`q 1 0 0 1 0 0 cm ${wing(x, y)} f Q`); });
  ops.push('Q', 'EMC');
  // layer 3: CUT, black outlines
  ops.push('/OC /MC3 BDC', 'q');
  SKUS.forEach((s, i) => { const [x, y] = at(i); ops.push(`q 0 0 0 RG 0.283 w ${box(x, y, 22, 28)} S Q`); });
  ops.push('Q', 'EMC');
  page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(ops.join('\n') + '\n'))]));
  return new Uint8Array(await doc.save({ useObjectStreams: false }));
}

const blue = c => c && c.length === 3 && c[0] < 0.05 && c[1] < 0.05 && c[2] > 0.95;
const hatchOf = c => c.members.filter(m => m.kind === 'path' && m.layer === 'HATCH');

(async () => {
  const bytes = await master();
  const parsed = await P.parseSource(bytes, 'master.ai');
  const g = P.groupCharms(parsed, { minPt: 6 });
  assert.equal(g.charms.length, 4, 'four charms found in the master');
  assert(parsed.segments.some(s => s.kind === 'text' && s.held && s.held.length), 'the opening text run records the colour operators it carries');
  const order = g.charms.slice().sort((a, b) => a.outline.bbox[0] - b.outline.bbox[0]);
  for (const [i, c] of order.entries()) {
    assert(hatchOf(c).length === 1 && blue(hatchOf(c)[0].fillRGB), SKUS[i] + ': the master reads its hatching blue');
    c.name = SKUS[i];
    // the per-SKU file the library stores (what index-master.cjs writes), read back as the page reads it
    const ai = await P.buildSingleCharm(c, parsed);
    const p2 = await P.parseSource(new Uint8Array(ai), 'one.ai');
    const g2 = P.groupCharms(p2, { minPt: 6 });
    assert.equal(g2.charms.length, 1, SKUS[i] + ': one charm in its own file');
    const hatch = hatchOf(g2.charms[0]);
    assert.equal(hatch.length, 1, SKUS[i] + ': its hatching is in the file');
    assert(blue(hatch[0].fillRGB), SKUS[i] + ': the per-SKU file keeps the hatching blue, not the default black: ' + JSON.stringify(hatch[0].fillRGB));
    assert(!p2.segments.some(s => s.kind === 'text'), SKUS[i] + ': the other charms\' text is still taken out');
    const paints = [];
    const ctx = new Proxy({}, { get: (o, k) => (k in o ? o[k] : () => {}), set: (o, k, v) => { if (k === 'fillStyle' || k === 'strokeStyle') paints.push(v); o[k] = v; return true; } });
    P.drawCharm(ctx, g2.charms[0], (x, y) => [x, y], 5);
    assert(paints.includes('rgb(0,0,255)'), SKUS[i] + ': drawn in blue hatching');
    assert(g2.charms[0].members.filter(m => m.layer === 'ENGRAVE').every(m => m.strokeRGB[0] === 1 && m.strokeRGB[2] === 0), SKUS[i] + ': engraving stays red');
  }
  // isolate on its own: only state operators come back, never another charm's drawing
  const keep = order[1].topIndices;
  const bytes2 = P.isolate(parsed.content, parsed.segments, keep);
  const text = Buffer.from(bytes2).toString('latin1');
  assert(/0 0 1\s+scn/.test(text), 'the layer fill operator survives isolate');
  assert(!/\(SPARE\)/.test(text), 'the text itself does not');
  // The second defect: hatching left on the CUT layer. A blue fill is engraving at any layer; strokes and dark fills on CUT stay cut.
  const G = require('../../charm-nest-geom.js');
  const on = (layer, o) => ({ kind: 'path', closed: true, layer, stroke: false, fill: false, ...o });
  const blueFill = on('CUT', { fill: true, fillRGB: [0, 0, 1] });
  assert.equal(G.pathRole(blueFill), 'artwork', 'a blue fill on CUT is hatching');
  assert(!G.isCutLine(blueFill) && !P.isCutLine(blueFill), 'and never a cut line, a hole or a body');
  assert(!G.isCutLine(on(null, { fill: true, fillRGB: [0.02, 0, 1] })) && !G.isCutLine(on('Layer 1', { fill: true, fillRGB: [0.21, 0.39, 1] })), 'wherever it was drawn');
  assert(G.isCutLine(on('CUT', { stroke: true, strokeRGB: [0, 0, 1] })), 'a blue STROKE on CUT is still a cut line');
  assert(G.isCutLine(on('CUT', { stroke: true, strokeRGB: [0.31, 0.82, 0.85] })), 'the cyan hoop stroke on CUT is still a cut line');
  assert(G.isCutLine(on('CUT', { fill: true, fillRGB: [0, 0, 0] })) && G.isCutLine(on('CUT', { fill: true, fillRGB: [0.5, 0, 0] })), 'black and dark-red body fills on CUT stay cut');
  assert.equal(G.pathRole({ ...blueFill, manufacturingRole: 'cut' }), 'cut', 'an explicit manufacturing role still wins');
  console.log('hatch-colour-state OK: ' + SKUS.join(', ') + ' keep their blue hatching in the per-SKU file');
})().catch(e => { console.error(e); process.exit(1); });
