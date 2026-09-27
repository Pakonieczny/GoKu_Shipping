// A custom order's .dxf design reads as an .ai does (charm-nest-dxf.js → CharmNestPDF): outlines, holes, engraving, units
const assert = require('node:assert/strict'), path = require('node:path');
global.window = global; global.PDFLib = require(path.join(__dirname, '../../vendor/pdf-lib-1.17.1.min.js'));
require(path.join(__dirname, '../../charm-nest-pdf.js'));
const D = require(path.join(__dirname, '../../charm-nest-dxf.js')), P = global.CharmNestPDF, MM = 72 / 25.4;
const g = (...kv) => { let s = ''; for (let i = 0; i < kv.length; i += 2) s += `${kv[i]}\n${kv[i + 1]}\n`; return s; };
function dxf(ents, { units = 4, layers = '', blocks = '' } = {}) {
  return g(0, 'SECTION', 2, 'HEADER') + (units == null ? '' : g(9, '$INSUNITS', 70, units)) + g(0, 'ENDSEC') +
    g(0, 'SECTION', 2, 'TABLES', 0, 'TABLE', 2, 'LAYER') + layers + g(0, 'ENDTAB', 0, 'ENDSEC') +
    g(0, 'SECTION', 2, 'BLOCKS') + blocks + g(0, 'ENDSEC') +
    g(0, 'SECTION', 2, 'ENTITIES') + ents + g(0, 'ENDSEC', 0, 'EOF');
}
const line = (x1, y1, x2, y2, extra = '') => g(0, 'LINE', 8, '0') + extra + g(10, x1, 20, y1, 11, x2, 21, y2);
const square = (x, y, s, extra) => line(x, y, x + s, y, extra) + line(x + s, y + s, x + s, y, extra) + line(x + s, y + s, x, y + s, extra) + line(x, y, x, y + s, extra);   // one drawn backwards
const circle = (x, y, r, extra = '') => g(0, 'CIRCLE', 8, '0') + extra + g(10, x, 20, y, 40, r);
const mm = v => v / MM;
async function read(text, name = 't.dxf') {
  const out = await D.toPdf(text, name), parsed = await P.parseSource(out.bytes, name);
  return { out, parsed, grouped: P.groupCharms(parsed, { minPt: 6 }) };
}
(async () => {
  // 1 · a square drawn as four loose lines, a hanging hole and a red engraving: one charm, 20 mm, the hole inside it
  {
    const { out, grouped } = await read(dxf(square(0, 0, 20) + circle(10, 16, 1.2) + g(0, 'LWPOLYLINE', 8, '0', 62, 1, 90, 3, 70, 0, 10, 5, 20, 5, 10, 10, 20, 10, 10, 15, 20, 5)));
    assert.equal(out.meta.units, 'mm'); assert(Math.abs(out.meta.widthMm - 20) < 0.01, out.meta.widthMm);
    assert.equal(grouped.charms.length, 1, 'one charm');
    const c = grouped.charms[0], b = c.outline.bbox;
    assert(Math.abs(mm(b[2] - b[0]) - 20) < 0.3 && Math.abs(mm(b[3] - b[1]) - 20) < 0.3, 'the four lines joined into its 20 mm outline');
    assert(c.members.length >= 3, 'hole and engraving belong to it');
  }
  // 2 · a closed LWPOLYLINE with bulges (a rounded 30 × 12 mm tag) and two ARCs making one 8 mm circle beside it
  {
    const tag = g(0, 'LWPOLYLINE', 8, '0', 90, 4, 70, 1, 10, 0, 20, 0, 10, 24, 20, 0, 42, 1, 10, 24, 20, 12, 10, 0, 20, 12, 42, 1);
    const arcs = g(0, 'ARC', 8, '0', 10, 50, 20, 6, 40, 4, 50, 0, 51, 180) + g(0, 'ARC', 8, '0', 10, 50, 20, 6, 40, 4, 50, 180, 51, 360);
    const { grouped } = await read(dxf(tag + arcs));
    assert.equal(grouped.charms.length, 2, 'the tag and the circle');
    const w = grouped.charms.map(c => mm(c.outline.bbox[2] - c.outline.bbox[0])).sort((a, b) => a - b);
    assert(Math.abs(w[0] - 8) < 0.2, 'two arcs joined into an 8 mm circle: ' + w[0]);
    assert(Math.abs(w[1] - 36) < 0.2, 'bulges round the tag\'s ends (24 + 6 + 6 mm): ' + w[1]);
  }
  // 3 · a block inserted twice, one of them scaled and rotated; a hidden layer is left out
  {
    const blocks = g(0, 'BLOCK', 8, '0', 2, 'HEART', 70, 0, 10, 0, 20, 0) + circle(0, 0, 5) + g(0, 'ENDBLK');
    const ins = g(0, 'INSERT', 8, '0', 2, 'HEART', 10, 0, 20, 0) + g(0, 'INSERT', 8, '0', 2, 'HEART', 10, 30, 20, 0, 41, 2, 42, 2, 50, 90);
    const layers = g(0, 'LAYER', 2, 'GUIDES', 70, 0, 62, -7);
    const { out, grouped } = await read(dxf(ins + square(-50, -50, 5, g(8, 'GUIDES')).replace(/8\n0\n8\nGUIDES/g, '8\nGUIDES'), { blocks, layers }));
    assert.equal(grouped.charms.length, 2, 'two inserts, hidden layer left out');
    const w = grouped.charms.map(c => mm(c.outline.bbox[2] - c.outline.bbox[0])).sort((a, b) => a - b);
    assert(Math.abs(w[0] - 10) < 0.2 && Math.abs(w[1] - 20) < 0.2, 'insert scale: ' + w);
    assert(out.meta.skipped['hidden layer'] >= 1);
  }
  // 4 · no units in the header and 0.8 across: inches (20.32 mm); a closed spline reads as an outline
  {
    const { out } = await read(dxf(square(0, 0, 0.8), { units: null }));
    assert.equal(out.meta.units, 'in (assumed)'); assert(Math.abs(out.meta.widthMm - 20.32) < 0.01);
    const sp = g(0, 'SPLINE', 8, '0', 70, 1, 71, 3, 72, 11, 73, 7, 40, 0, 40, 0, 40, 0, 40, 0, 40, 1, 40, 2, 40, 3, 40, 4, 40, 4, 40, 4, 40, 4,
      10, 0, 20, 0, 10, 10, 20, -5, 10, 20, 20, 0, 10, 25, 20, 10, 10, 20, 20, 20, 10, 5, 20, 18, 10, 0, 20, 0);
    const { grouped } = await read(dxf(sp));
    assert.equal(grouped.charms.length, 1, 'a closed spline is an outline');
  }
  // 5 · an engraving layer is engraving whatever its colour; a CUT layer in colour is still a cut line
  {
    const { grouped } = await read(dxf(square(0, 0, 20, g(62, 5)).replace(/8\n0\n62\n5/g, '8\nCUT\n62\n5') + square(5, 5, 4).replace(/8\n0/g, '8\nEngrave')));
    assert.equal(grouped.charms.length, 1, 'the blue CUT square is the outline, the black Engrave square its detail');
    assert(Math.abs(mm(grouped.charms[0].outline.bbox[2] - grouped.charms[0].outline.bbox[0]) - 20) < 0.3);
  }
  // 6 · what is not a drawing says so
  await assert.rejects(D.toPdf('hello', 'x.dxf'), /not a DXF/);
  await assert.rejects(D.toPdf(new TextEncoder().encode('AutoCAD Binary DXF\r\n\x1a\x00'), 'b.dxf'), /binary DXF/);
  await assert.rejects(D.toPdf(dxf(g(0, 'TEXT', 8, '0', 10, 0, 20, 0, 40, 3, 1, 'hi')), 'e.dxf'), /nothing drawn/);
  console.log('custom-dxf: ok');
})().catch(e => { console.error(e); process.exit(1); });
