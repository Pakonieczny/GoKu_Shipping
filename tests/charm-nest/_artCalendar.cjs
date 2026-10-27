/* A calendar charm for the ART EDIT tests (Paul, 27 Oct 2026: "add a date inside a given calendar day square and update the month name located on top of the calendar charm").
   No calendar design is among the real masters in fixtures/, so one is drawn here the way the shop draws its masters and then written as a per-SKU master is (CharmNestPDF.buildSingleCharm:
   the whole charm in ONE form), so it is read back exactly as Pool.masterCharm reads charmnest/master/<SKU>.ai:
     CUT      the outline, a closed black hairline (10,10)-(70,80)
     ENGRAVE  the month name "MAY" on top, three letters as black FILLS (the black-fill rule reads them as blue hatching), y 68..76
              the day grid below it, 5 x 4 squares of 10 pt as red STROKES, x 15..65, y 15..55 (day n = row, col from the top left)
   → { bytes (the per-SKU file), parsed, charm, month (the letters' member indices), day(row, col) → the square's member index } */
'use strict';
const path = require('path');
module.exports = async function calendar(ROOT) {
  ROOT = ROOT || path.join(__dirname, '..', '..');
  const L = global.PDFLib, P = global.CharmNestPDF;
  const d = await L.PDFDocument.create(), p = d.addPage([80, 90]); p.node.normalize();
  const ocg = name => d.context.register(d.context.obj({ Type: 'OCG', Name: L.PDFString.of(name) }));
  const cut = ocg('CUT'), engrave = ocg('ENGRAVE');
  p.node.Resources().set(L.PDFName.of('Properties'), d.context.obj({ ocCut: cut, ocEng: engrave }));
  const rect = (x, y, w, h) => `${x} ${y} m ${x + w} ${y} l ${x + w} ${y + h} l ${x} ${y + h} l h`;
  const poly = pts => pts.map((q, i) => `${q[0]} ${q[1]} ${i ? 'l' : 'm'}`).join(' ') + ' h';
  // M, A (with its counter), Y: each letter one path, painted f
  const M = poly([[20, 68], [22, 68], [22, 73], [24, 70], [26, 73], [26, 68], [28, 68], [28, 76], [26, 76], [24, 73], [22, 76], [20, 76]]);
  const A = poly([[34, 68], [36, 68], [37, 71], [39, 71], [40, 68], [42, 68], [39, 76], [37, 76]]) + ' ' + poly([[37.5, 72.5], [38.5, 72.5], [38, 74]]);
  const Y = poly([[49, 68], [51, 68], [51, 72], [54, 76], [52, 76], [50, 73.5], [48, 76], [46, 76], [49, 72]]);
  let ops = `/OC /ocCut BDC q 0 0 0 RG 0.25 w ${rect(10, 10, 60, 70)} S Q EMC `;
  ops += `/OC /ocEng BDC q 0 0 0 rg ${M} f Q q 0 0 0 rg ${A} f* Q q 0 0 0 rg ${Y} f Q `;
  for (let r = 0; r < 4; r++) for (let c = 0; c < 5; c++) ops += `q 1 0 0 RG 0.3 w ${rect(15 + 10 * c, 45 - 10 * r, 10, 10)} S Q `;
  ops += 'EMC';
  p.node.addContentStream(d.context.register(d.context.flateStream(ops)));
  d.catalog.set(L.PDFName.of('OCProperties'), d.context.obj({ OCGs: [cut, engrave], D: { Order: [cut, engrave], ON: [cut, engrave] } }));
  const drawn = await d.save({ useObjectStreams: false });
  // the per-SKU master, as the Library writes it: the charm alone, in one form
  const page = await P.parseSource(drawn, 'CALENDAR-drawn.ai'), g0 = P.groupCharms(page);
  if (g0.charms.length !== 1) throw new Error('the drawn calendar groups as ' + g0.charms.length + ' charms');
  const bytes = await P.buildSingleCharm(Object.assign(g0.charms[0], { name: 'CALENDAR', strokePt: 0.25 }), page);
  const parsed = await P.parseSource(bytes, 'CALENDAR.ai'), g = P.groupCharms(parsed);
  if (g.charms.length !== 1) throw new Error('the per-SKU calendar groups as ' + g.charms.length + ' charms');
  const charm = g.charms[0];
  Object.assign(charm, { id: 'cal', sourceId: 's', name: 'CALENDAR', sku: 'CALENDAR', centerPt: [(charm.bbox[0] + charm.bbox[2]) / 2, (charm.bbox[1] + charm.bbox[3]) / 2], strokePt: 0.25, hash: 'calhash01' });
  // (the per-SKU file sets the charm on a page of its own: its points are the drawing's moved by the outline's new corner)
  const ox = charm.outline.bbox[0] - 10, oy = charm.outline.bbox[1] - 10, at = (x, y) => [x + ox, y + oy];
  const mid = m => [(m.bbox[0] + m.bbox[2]) / 2, (m.bbox[1] + m.bbox[3]) / 2];
  const month = charm.members.map((m, i) => (m.fill && m.bbox[1] >= oy + 67 ? i : -1)).filter(i => i >= 0);
  const dayMid = (r, c) => at(20 + 10 * c, 50 - 10 * r);
  const day = (r, c) => charm.members.findIndex(m => m.stroke && !m.fill && Math.abs(mid(m)[0] - dayMid(r, c)[0]) < 0.5 && Math.abs(mid(m)[1] - dayMid(r, c)[1]) < 0.5);
  return { bytes, parsed, charm, month, day, dayMid, mid, at };
};
