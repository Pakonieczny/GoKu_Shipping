// DACHSHUND_88528 (HUGGIE) read as "a ring and a few red strokes, no body, 0 holes". Its black body on CUT is a path the
// artist never closed: the end lies 0.09 pt (0.03 mm) from the start and there is no `h`, and the reader only called a
// stroke closed when its ends met within 0.05 pt. No closed stroke, no cut line, no outline candidate: the charm was the
// cyan jump ring alone, the body (more than three times the ring's box) fell out as an orphan and the engraving hung
// loose around the ring. The plain Dachshund (no ring) had no outline at all and its SKU was never indexed.
// The structure here is the master's: layer markers, `q cm path S Q` per object, the body open by 0.09 pt.
//   node tests/charm-nest/broken-charm.cjs
const assert = require('node:assert/strict');
global.self = global;
global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js');
require('../../charm-nest-pdf.js');
const P = global.CharmNestPDF;
const { PDFDocument, PDFName, PDFString } = PDFLib;

const K = 0.5523, num = n => (+n).toFixed(3);
// an ellipse of four Béziers from (cx+a, cy) round to (cx+a, cy+gap): open by `gap` when gap > 0, with `h` when `close`
const blob = (cx, cy, w, h, gap, close) => { const a = w / 2, b = h / 2, p = (x, y) => `${num(x)} ${num(y)}`;
  return `${p(cx + a, cy)} m ${p(cx + a, cy + K * b)} ${p(cx + K * a, cy + b)} ${p(cx, cy + b)} c ${p(cx - K * a, cy + b)} ${p(cx - a, cy + K * b)} ${p(cx - a, cy)} c ${p(cx - a, cy - K * b)} ${p(cx - K * a, cy - b)} ${p(cx, cy - b)} c ${p(cx + K * a, cy - b)} ${p(cx + a, cy - K * b + gap)} ${p(cx + a, cy + gap)} c${close ? ' h' : ''}`; };
const stroke = (x1, y1, x2, y2) => `${num(x1)} ${num(y1)} m ${num(x1 + (x2 - x1) / 2)} ${num(y1 + 3)} ${num(x2 - 1)} ${num(y2 + 2)} ${num(x2)} ${num(y2)} c`;

async function master() {
  const doc = await PDFDocument.create();
  const page = doc.addPage([300, 200]);
  const ocg = n => doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFString.of(n) }));
  const res = page.node.Resources();
  res.set(PDFName.of('Properties'), doc.context.obj({ MC1: ocg('ENGRAVE'), MC2: ocg('HATCH'), MC3: ocg('CUT') }));
  const ops = [];
  // engraving (red, open strokes) and hatching (a blue dot) of the huggie dog (A), the plain dog (B) and a control (C)
  ops.push('/OC /MC1 BDC', 'q 1 0.114 0.145 RG 0.283 w');
  for (const [x, y] of [[40, 130], [40, 60], [200, 60]]) ops.push(stroke(x + 4, y + 2, x + 14, y + 9) + ' S', stroke(x + 18, y + 1, x + 22, y + 8) + ' S');
  ops.push(`0 0 0 RG ${blob(250, 150, 14, 14, 0.1, false)} S`);                      // F: a black stroke on the ENGRAVE layer, open by 0.1 pt: engraving, never closed
  ops.push('Q', 'EMC');
  ops.push('/OC /MC2 BDC', 'q 0 0 1 rg', `${blob(46, 138, 1, 1, 0, true)} f`, 'Q', 'EMC');
  // CUT: A = body open by 0.09 pt + a cyan jump ring (two circles, compound), B = body open by 0.11 pt, C = a closed body
  ops.push('/OC /MC3 BDC', 'q');
  ops.push(`0 0 0 RG 0.283 w ${blob(55, 135, 30, 14, 0.09, false)} S`);               // A body, ends 0.09 pt apart
  ops.push(`0.31 0.82 0.851 RG 0.25 w ${blob(63, 148, 9.4, 9.4, 0, true)} ${blob(63, 148, 5.8, 5.8, 0, true)} S`);   // A jump ring (outer + inner circle)
  ops.push(`0 0 0 RG 0.283 w ${blob(55, 65, 30, 14, 0.11, false)} S`);                // B body, ends 0.11 pt apart
  ops.push(`0 0 0 RG 0.283 w ${blob(215, 65, 30, 14, 0, true)} S`);                   // C closed body
  ops.push(`0 0 0 RG 0.283 w ${blob(130, 30, 30, 14, 4, false)} S`);                  // D: open by 4 pt, a real opening
  ops.push(`0 0 0 RG 0.283 w ${blob(180, 30, 8, 8, 0.3, false)} S`);                  // E: a small hole open by 0.3 pt (a 6 mm-class eye hole)
  ops.push('Q', 'EMC');
  page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(ops.join('\n') + '\n'))]));
  return new Uint8Array(await doc.save({ useObjectStreams: false }));
}

const near = (c, x, y, tol = 3) => { const b = c.outline.bbox; return Math.abs((b[0] + b[2]) / 2 - x) <= tol && Math.abs((b[1] + b[3]) / 2 - y) <= tol; };
(async () => {
  const bytes = await master();
  const parsed = await P.parseSource(bytes, 'master.ai');
  const all = parsed.segments.concat(parsed.nested);
  const cutPaths = all.filter(s => s.kind === 'path' && s.layer === 'CUT');
  const bodyA = cutPaths.find(s => near({ outline: s }, 55, 135, 1) && !s.fill);
  assert(bodyA && bodyA.closed && bodyA.nearClosed > 0.05 && bodyA.nearClosed < 0.2, 'the body ends 0.09 pt from its start: read as closed, the gap recorded (' + (bodyA && bodyA.nearClosed) + ')');
  assert(bodyA.subpaths[0][bodyA.subpaths[0].length - 1][0] === 'h', 'its subpath got the closing operator');
  const bodyD = cutPaths.find(s => near({ outline: s }, 130, 30, 1) && (s.bbox[2] - s.bbox[0]) > 20);
  assert(bodyD && !bodyD.closed && bodyD.nearClosed == null, 'a real 4 pt opening stays open (the indexer reports it as an open outline)');
  const eng = all.filter(s => s.kind === 'path' && s.layer === 'ENGRAVE');
  assert(eng.length === 7 && eng.every(s => !s.closed), 'engraving strokes (even a black one open by 0.1 pt) are never closed by this rule');

  const g = P.groupCharms(parsed, { minPt: 6 });
  const A = g.charms.find(c => near(c, 55, 135, 4)), B = g.charms.find(c => near(c, 55, 65, 4)), C = g.charms.find(c => near(c, 215, 65, 4));
  assert(A && B && C, 'the three dogs are charms: ' + g.charms.map(c => c.outline.bbox.map(Math.round)).join(' | '));
  assert(A.outline === bodyA, 'A: the body, not the ring, is the outline');
  const w = c => c.outline.bbox[2] - c.outline.bbox[0];
  assert(w(A) > 29 && w(B) > 29, 'both dogs are body sized (' + w(A).toFixed(1) + ', ' + w(B).toFixed(1) + ' pt), not a 9 pt ring');
  assert.equal(A.members.filter(m => m.layer === 'ENGRAVE').length, 2, 'A keeps its engraving');
  assert.equal(B.members.filter(m => m.layer === 'ENGRAVE').length, 2, 'B (no ring) keeps its engraving');
  assert.equal(C.members.filter(m => m.layer === 'ENGRAVE').length, 2, 'the control keeps its engraving');
  const orphanSized = g.orphans.filter(s => s.kind === 'path' && (s.bbox[2] - s.bbox[0]) >= 6 && s.layer === 'CUT');
  assert(orphanSized.length === 1 && orphanSized[0] === bodyD, 'the only body-sized cut line left over is D, the body with a real 4 pt opening: ' + orphanSized.map(s => s.bbox.map(Math.round)));
  const r = P.integrateRings(A);
  assert(r.left.length === 0, 'the jump ring joins the body as it does on any huggie: ' + JSON.stringify(r.left));
  assert(A.outline.bbox[3] - A.outline.bbox[1] > 14, 'the hoop sits on top of the body');

  // the per-SKU file the library stores, read back as the page reads it: still one body, closed, with its engraving
  for (const [name, c, engr] of [['A', A, 2], ['B', B, 2]]) {
    const ai = await P.buildSingleCharm(c, parsed);
    const p2 = await P.parseSource(new Uint8Array(ai), 'one.ai');
    const g2 = P.groupCharms(p2, { minPt: 6 });
    assert.equal(g2.charms.length, 1, name + ': one charm in its own file');
    assert(g2.charms[0].outline.closed && (g2.charms[0].outline.bbox[2] - g2.charms[0].outline.bbox[0]) > 25, name + ': the file reads back with the body as its outline');
    assert.equal(g2.charms[0].members.filter(m => m.layer === 'ENGRAVE').length, engr, name + ': engraving kept in the file');
  }
  // a small cut-out open by 0.3 pt is a hole once closed
  const holeE = cutPaths.find(s => near({ outline: s }, 180, 30, 1) && (s.bbox[2] - s.bbox[0]) < 10);
  assert(holeE && holeE.closed && holeE.nearClosed > 0.2, 'a small cut-out open by 0.3 pt is closed too');

  console.log('broken-charm OK: a body open by 0.09 pt is closed, the dachshund huggie is a body with its ring and engraving, a 4 pt opening stays open');
})().catch(e => { console.error(e); process.exit(1); });
