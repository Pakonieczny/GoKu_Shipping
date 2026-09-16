// A placement with a scale (the optimization pass) is written smaller by exactly that factor and read back as such.
global.window = global; const L = global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js'); require('../../charm-nest-pdf.js');
const fs = require('fs'), path = require('path'), os = require('os'), assert = require('assert');
const { build } = require('./fixture.cjs');
(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-scale-')); const file = path.join(tmp, 'fixture.ai'); await build(file, 6);
  const parsed = await CharmNestPDF.parseSource(new Uint8Array(fs.readFileSync(file)), 'fixture'); const g = CharmNestPDF.groupCharms(parsed, { minPt: 6 });
  const c = g.charms[0]; c.centerPt = [(c.bbox[0] + c.bbox[2]) / 2, (c.bbox[1] + c.bbox[3]) / 2]; c.name = 'a'; c.sourceId = 's';
  const w0 = c.outline.bbox[2] - c.outline.bbox[0];
  const sources = new Map([['s', parsed]]);
  const bytes = await CharmNestPDF.buildSheet({ sheet: { wPt: 300, hPt: 300, strokeRGB: [1, 0, 0], strokePt: 0.5 }, placements: [{ charm: c, angle: 0, scale: 0.97, cxPt: 150, cyPt: 150 }, { charm: c, angle: 90, scale: 1, cxPt: 60, cyPt: 60 }], sources, title: 't', meta: {} });
  const out = await CharmNestPDF.parseSource(new Uint8Array(bytes), 'out'); const go = CharmNestPDF.groupCharms(out, { minPt: 6 });
  const widths = go.charms.map(x => +(x.outline.bbox[2] - x.outline.bbox[0]).toFixed(2)).sort((a, b) => a - b);
  const expect = [+(w0 * 0.97).toFixed(2), +w0.toFixed(2)];
  // the rotated copy's width is its height; compare the scaled one against 0.97 × the unrotated width
  const scaled = widths.find(w => Math.abs(w - expect[0]) < 0.05);
  assert(scaled != null, `scaled copy written at 97 %: widths ${widths} expected one ≈ ${expect[0]}`);
  console.log('scale OK ·', w0.toFixed(2), 'pt →', scaled, 'pt at 97 %');
})().catch(e => { console.error(e); process.exit(1); });
