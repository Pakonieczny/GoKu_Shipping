/* CLIPS + EXPORTFACES (Paul, 11 Nov 2026: "The .ai and .dxf download buttons are not working ... the files must exactly represent the sheets and the set including the back
 * engraving"). The Library's .dxf stopped on "This artwork uses a clipping mask" for every full Gold Filled sheet of the day: each charm is drawn under the frame of its
 * own file (a big rectangle, turned with the charm) and the old check wanted every clip to hold the corners of the WHOLE sheet. Now the parser says which clips are in
 * force over each path and the export cuts the art to them: a path inside its clips is exported exactly as before, a path crossing one is cut to what shows, a path
 * wholly hidden is not exported. And the worker's compose now receives the front engravings and each record's ear, which the page's hand-off dropped.
 * Offline: real charm-nest-pdf.js / -export.js / -vector.js with pdf-lib and Clipper; the background hand-off is sliced out of charm-nest-background.js. No network.
 *   node tests/charm-nest/export-clips.cjs                                                                                                                            */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const ROOT = path.join(__dirname, '../..');
global.self = global; global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js')); global.ClipperLib = require(path.join(ROOT, 'vendor/clipper-6.4.2.js'));
require(path.join(ROOT, 'charm-nest-pdf.js')); require(path.join(ROOT, 'charm-nest-vector.js'));
const E = require(path.join(ROOT, 'charm-nest-export.js')), V = global.CharmNestVector;

/** A one-page PDF whose content is `ops`, under the layer `name`. */
async function pdf(name, w, h, ops) {
  const L = PDFLib, d = await L.PDFDocument.create(), p = d.addPage([w, h]); p.node.normalize();
  const ref = d.context.register(d.context.obj({ Type: 'OCG', Name: L.PDFString.of(name) }));
  p.node.Resources().set(L.PDFName.of('Properties'), d.context.obj({ art: ref }));
  p.node.addContentStream(d.context.register(d.context.flateStream('/OC /art BDC ' + ops + ' EMC')));
  d.catalog.set(L.PDFName.of('OCProperties'), d.context.obj({ OCGs: [ref], D: { Order: [ref], ON: [ref] } }));
  return d.save({ useObjectStreams: false });
}
const area = p => V.filled(p).reduce((s, l) => { let a = 0; const q = l.points; for (let i = 0; i < q.length; i++) { const u = q[i], v = q[(i + 1) % q.length]; a += u[0] * v[1] - v[0] * u[1]; } return s + Math.abs(a) / 2 * (l.hole ? -1 : 1); }, 0);

(async () => {
  // 1. the 11 Nov sheets: two charms far apart, each under a long thin frame turned with it. The frame holds its own charm, not the corners of the sheet:
  //    the old check stopped the export; now both charms are exported exactly as drawn.
  {
    const frame = (cx, cy) => `${cx - 300} ${cy - 290} m ${cx + 290} ${cy + 300} l ${cx + 300} ${cy + 290} l ${cx - 290} ${cy - 300} l h W n`;
    const bytes = await pdf('CUT', 283, 142, `q ${frame(30, 30)} 1 0 0 RG .5 w 25 25 10 10 re S Q q ${frame(250, 110)} 0 0 1 rg 245 105 10 10 re f Q`);
    const parsed = await CharmNestPDF.parseSource(bytes, 'frames');
    const paths = E.leaves(parsed);
    assert.equal(paths.length, 2, 'both charms are exported');
    assert(paths.every(p => !p.clipped && !p.clips), 'art inside its frame is exported exactly as drawn (and carries no clip data into the DXF)');
    assert.deepEqual(paths.map(p => p.bbox.map(Math.round)), [[25, 25, 35, 35], [245, 105, 255, 115]]);
    const d = E.dxf(paths); assert.equal(d.entityCount, 2);
  }
  // 2. a picture traced in the Charm Editor, cropped (layer IMG): the filled art and its line are cut to the crop; what lies wholly outside it is not exported
  {
    const bytes = await pdf('IMG', 100, 100, 'q 10 10 20 20 re W n 0 0 0 rg 0 0 20 20 re f 1 0 0 RG .5 w 0 15 m 40 15 l S 0 0 0 rg 50 50 5 5 re f Q 0 0 0 rg 60 60 5 5 re f');
    const parsed = await CharmNestPDF.parseSource(bytes, 'crop');
    const paths = E.leaves(parsed);
    const fills = paths.filter(p => p.fill), lines = paths.filter(p => p.stroke);
    assert.equal(fills.length, 2, 'the cropped square and the square drawn after Q (no clip) are exported; the one hidden by the crop is not');
    const cut = fills.find(p => p.clipped);
    assert(cut && Math.abs(area(cut) - 100) < 1e-6, 'the 20 x 20 square shows as its 10 x 10 part inside the crop');
    assert.deepEqual(cut.bbox.map(v => +v.toFixed(6)), [10, 10, 20, 20]);
    assert(fills.some(p => !p.clipped && p.bbox[0] === 60), 'Q ends the crop: the next square is whole');
    assert.equal(lines.length, 1); const ln = lines[0].subpaths[0];
    assert.equal(ln.length, 2, 'the line becomes the one piece of it inside the crop');
    assert.deepEqual([ln[0][1], ln[1][1]].map(p => p.map(v => +v.toFixed(6))).sort((a, b) => a[0] - b[0]), [[10, 15], [30, 15]]);
    const d = E.dxf(paths); assert.equal(d.entityCount, 3, 'two hatches and one polyline');
  }
  // 3. a clip set inside a form (a charm's own file on the sheet) governs only that form's art, at the place the sheet draws it; even-odd clips are honoured
  {
    const L = PDFLib, d = await L.PDFDocument.create(), p = d.addPage([200, 100]); p.node.normalize();
    const form = d.context.register(d.context.flateStream('0 0 20 20 re 5 5 10 10 re W* n 0 0 0 rg 0 0 20 20 re f', { Type: 'XObject', Subtype: 'Form', BBox: [0, 0, 20, 20] }));
    p.node.Resources().set(L.PDFName.of('XObject'), d.context.obj({ C1: form }));
    p.node.addContentStream(d.context.register(d.context.flateStream('q 1 0 0 1 100 50 cm /C1 Do Q 0 0 0 rg 0 0 5 5 re f')));
    const parsed = await CharmNestPDF.parseSource(await d.save({ useObjectStreams: false }), 'form');
    const paths = E.leaves(parsed), ring = paths.find(p => p.clipped), plain = paths.find(p => !p.clipped);
    assert(ring && Math.abs(area(ring) - 300) < 1e-6, 'the even-odd clip leaves the 20 x 20 square with its 10 x 10 middle out');
    assert.deepEqual(ring.bbox.map(v => +v.toFixed(6)), [100, 50, 120, 70], 'at the place the sheet draws the form');
    assert(plain && Math.abs(area(plain) - 25) < 1e-6, 'art drawn after the form is not clipped');
  }
  // 4. a raster picture still stops the DXF with words that say so (it cannot be cut by a laser line)
  {
    const L = PDFLib, d = await L.PDFDocument.create(), p = d.addPage([50, 50]); p.node.normalize();
    const img = d.context.register(d.context.flateStream(new Uint8Array(3), { Type: 'XObject', Subtype: 'Image', Width: 1, Height: 1, ColorSpace: 'DeviceRGB', BitsPerComponent: 8 }));
    p.node.Resources().set(L.PDFName.of('XObject'), d.context.obj({ I1: img }));
    p.node.addContentStream(d.context.register(d.context.flateStream('q 10 0 0 10 5 5 cm /I1 Do Q')));
    const parsed = await CharmNestPDF.parseSource(await d.save({ useObjectStreams: false }), 'raster');
    assert.throws(() => E.leaves(parsed), /raster image/);
  }
  // 5. the worker's compose receives the front engravings and each record's ear (EXPORTFACES)
  {
    const src = fs.readFileSync(path.join(ROOT, 'charm-nest-background.js'), 'utf8');
    const a = src.indexOf('  const rec=b=>'), b = src.indexOf('  E.productionDxf=', a);
    assert(a > 0 && b > a, 'the compose hand-off is in charm-nest-background.js');
    const calls = [], ctx = { E: {}, geometry: { run: (type, input) => (calls.push({ type, input }), Promise.resolve({})) } };
    vm.createContext(ctx); vm.runInContext(src.slice(a, b), ctx);
    const sheet = { id: 's1', fileBase: 'GF', placements: [{ id: 'a' }], charms: [{ id: 'a', poolId: 'p1', side: 'L', bits: new Uint8Array(4) }] };
    await ctx.E.compose(new Uint8Array(1), sheet, [{ poolId: 'p1', bytes: new Uint8Array(2), side: 'L', groupKey: '1:2', outputs: {} }], [{ poolId: 'p1', bytes: new Uint8Array(3), face: 'front', side: 'L' }]);
    const input = calls[0].input;
    assert.equal(calls[0].type, 'compose');
    assert.equal(input.fronts.length, 1, 'the front engraving goes to the worker'); assert.equal(input.fronts[0].face, 'front');
    assert.deepEqual(Object.keys(input.backs[0]).sort(), ['bytes', 'groupKey', 'poolId', 'side'], 'a back keeps its ear (Left / Right layers) and nothing heavy');
    assert.equal(JSON.stringify(input.sheet.charms), JSON.stringify([{ id: 'a', poolId: 'p1', side: 'L' }]), 'the charm keeps its ear, not its pictures');
    await ctx.E.compose(new Uint8Array(1), sheet, []);
    assert(!('fronts' in calls[1].input), 'a sheet with no front engraving is handed over as before');
    const worker = fs.readFileSync(path.join(ROOT, 'charm-nest-compute-worker.js'), 'utf8');
    assert(/compose\(a\.front,a\.sheet,a\.backs,a\.fronts\)/.test(worker), 'the worker composes with the fronts');
  }
  console.log('Export clips OK: art inside its clips is exported as drawn, art crossing a clip is cut to what shows, hidden art is left out, no export stops for a clip; the worker composes with the front engravings and the ears');
})().catch(e => { console.error(e); process.exit(1); });
