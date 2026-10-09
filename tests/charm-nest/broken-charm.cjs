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
const { PDFDocument, PDFName, PDFString, StandardFonts } = PDFLib;
const fs = require('fs'), os = require('os'), path = require('path');

const K = 0.5523, num = n => (+n).toFixed(3);
// an ellipse of four Béziers from (cx+a, cy) round to (cx+a, cy+gap): open by `gap` when gap > 0, with `h` when `close`
const blob = (cx, cy, w, h, gap, close) => { const a = w / 2, b = h / 2, p = (x, y) => `${num(x)} ${num(y)}`;
  return `${p(cx + a, cy)} m ${p(cx + a, cy + K * b)} ${p(cx + K * a, cy + b)} ${p(cx, cy + b)} c ${p(cx - K * a, cy + b)} ${p(cx - a, cy + K * b)} ${p(cx - a, cy)} c ${p(cx - a, cy - K * b)} ${p(cx - K * a, cy - b)} ${p(cx, cy - b)} c ${p(cx + K * a, cy - b)} ${p(cx + a, cy - K * b + gap)} ${p(cx + a, cy + gap)} c${close ? ' h' : ''}`; };
const stroke = (x1, y1, x2, y2) => `${num(x1)} ${num(y1)} m ${num(x1 + (x2 - x1) / 2)} ${num(y1 + 3)} ${num(x2 - 1)} ${num(y2 + 2)} ${num(x2)} ${num(y2)} c`;

async function master() {
  const doc = await PDFDocument.create();
  const page = doc.addPage([300, 200]);
  const font = await doc.embedFont(StandardFonts.Helvetica);
  const fkey = page.node.newFontDictionary(font.name, font.ref).toString();
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
  // G: ONE compound path: the body is an open run (4 pt short of closing) and its closing chord is the NEXT subpath (drawn the other
  // way round), plus a closed hole. Nothing in it is closed, the loop is (the chicken of RUNNING_73848, the customs bars).
  ops.push(`0 0 0 RG 0.283 w 270 100 m 270 108 262 112 255 112 c 245 112 238 108 238 100 c 238 92 245 88 255 88 c 262 88 268 92 270 96 c 270 100 m 270 99 270 97 270 96 c ${blob(255, 100, 4, 4, 0, true)} S`);
  ops.push('Q', 'EMC');
  // a lone ring with engraving drawn round it (a ring-only design): R at (100, 175)
  ops.push('/OC /MC3 BDC', 'q', `0.31 0.82 0.851 RG 0.25 w ${blob(100, 175, 9.4, 9.4, 0, true)} S`, 'Q', 'EMC');
  ops.push('/OC /MC1 BDC', 'q', '1 0.114 0.145 RG 0.283 w', stroke(85, 160, 100, 168) + ' S', stroke(104, 160, 118, 168) + ' S', 'Q', 'EMC');
  // the SKU under each design (3 pt text, 3 pt under the body)
  for (const [t, x, y] of [['DOG_88528 (HUGGIE)', 40, 118], ['DOG_88528', 40, 48], ['DOG_CONTROL', 200, 48], ['DOG_OPEN_44711', 115, 14], ['RING_12345', 90, 166]]) ops.push(`BT ${fkey} 3 Tf 1 0 0 1 ${x} ${y} Tm (${t}) Tj ET`);
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
  assert(eng.length === 9 && eng.every(s => !s.closed), 'engraving strokes (even a black one open by 0.1 pt) are never closed by this rule');

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
  // the compound body: its open run and its chord are one closed loop, the hole stays its own subpath
  const bodyG = cutPaths.find(s => near({ outline: s }, 254, 100, 2) && (s.bbox[2] - s.bbox[0]) > 25);
  assert(bodyG && bodyG.closed && bodyG.subpaths.length === 2, 'G: the open run and its closing chord join into one closed loop (' + (bodyG && bodyG.subpaths.length) + ' subpaths)');
  assert(bodyG.subpaths.every(sub => sub[sub.length - 1][0] === 'h'), 'G: both subpaths are closed now');
  const shoelace = poly => Math.abs(poly.reduce((v, p, i) => v + p[0] * poly[(i + 1) % poly.length][1] - p[1] * poly[(i + 1) % poly.length][0], 0) / 2);
  const loopArea = shoelace(P.flatten({ subpaths: [bodyG.subpaths[0]] }, 12)[0]);
  assert(loopArea > 560 && loopArea < 660, 'G: the joined loop has the body\'s own area, the chord bridged without a jump (' + loopArea.toFixed(0) + ' pt²)');
  const gg = g.charms.find(c => c.outline === bodyG);
  assert(gg && P.cutLinesOf(gg).length === 0, 'G: the compound body is a charm outline of its own (its hole is part of the path)');
  // a small cut-out open by 0.3 pt is a hole once closed
  const holeE = cutPaths.find(s => near({ outline: s }, 180, 30, 1) && (s.bbox[2] - s.bbox[0]) < 10);
  assert(holeE && holeE.closed && holeE.nearClosed > 0.2, 'a small cut-out open by 0.3 pt is closed too');

  // the scan tool (scripts/broken-charm-scan.cjs) on the same master: the dogs are clean, the ring-only design and the dog with a
  // real opening are found, with the reason
  const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'bc-')), 'MASTER-BC.ai'); fs.writeFileSync(file, bytes);
  const S = require('../../scripts/broken-charm-scan.cjs');
  const res = await S.scan(file, {});
  const row = sku => res.rows.find(r => r.skus.includes(sku));
  const rules = sku => row(sku) ? row(sku).flags.map(f => f.rule) : null;
  assert.deepEqual(rules('DOG_88528 (HUGGIE)'), [], 'the huggie dog is a clean design now: ' + JSON.stringify(row('DOG_88528 (HUGGIE)') && row('DOG_88528 (HUGGIE)').flags));
  assert.deepEqual(rules('DOG_88528'), [], 'the plain dog is a clean design now');
  assert(row('DOG_88528 (HUGGIE)').areaPt2 > 300 && row('DOG_88528 (HUGGIE)').nearClosed, 'its silhouette is the body (' + row('DOG_88528 (HUGGIE)').areaPt2 + ' pt²), recorded as read from a near-closed line');
  assert(rules('RING_12345') && rules('RING_12345').includes('lone-ring'), 'a ring-only design is a lone ring: ' + JSON.stringify(rules('RING_12345')));
  assert(rules('RING_12345').includes('engraving-outside'), 'and its engraving has no cut outline around it');
  const open4 = res.orphanLabels.find(o => o.sku === 'DOG_OPEN_44711');
  assert(open4 && open4.cutBodies >= 1 && open4.openCut >= 1 && open4.gaps[0] > 3 && open4.gaps[0] < 5, 'the dog open by 4 pt: its label finds no charm, and the report names the open body and its gap: ' + JSON.stringify(open4));
  // the report: reasons per SKU, the open body for the artist
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bc-rep-')), scanFile = path.join(dir, 'scan.json'); fs.writeFileSync(scanFile, JSON.stringify(res));
  const list = S.report(['--scan', scanFile, '--out', dir]);
  const rw = JSON.parse(fs.readFileSync(path.join(dir, 'BROKENCHARM-rewrite-skus.json'), 'utf8'));
  assert.deepEqual(rw.skus, ['DOG_88528', 'DOG_88528 (HUGGIE)'], 'the re-index list holds the two dogs the reader now reads right, not the ring or the open dog');
  assert.equal(list.find(e => e.sku === 'DOG_OPEN_44711').fix, 'master', 'the open dog is for the artist');
  assert.equal(list.find(e => e.sku === 'RING_12345').fix, 'review', 'a lone ring is for a person to look at');
  const dog = list.find(e => e.sku === 'DOG_88528 (HUGGIE)');
  assert(dog && dog.fix === 'reader' && dog.reasons.some(r => r.rule === 'cut-line-not-closed' && /0\.0\d+ pt/.test(r.why)), 'the repaired dog is listed as a design the reader fixes, with the size of its gap: ' + JSON.stringify(dog));
  assert(list.some(e => e.sku === 'RING_12345') && list.some(e => e.sku === 'DOG_OPEN_44711') && !list.some(e => e.sku === 'DOG_OPEN_44711' && e.fix === 'reader'), 'the report lists the ring and the open dog (for a person and the artist): ' + list.map(e => e.sku + ':' + e.fix).join(','));
  // a SKU on two drawings of two masters is held live once: the one the library holds from another master is not re-indexed
  const two = JSON.parse(JSON.stringify(res)), baseRow = two.rows.find(x => x.sku === 'DOG_88528 (HUGGIE)'), mk = (master, sku, r, nc) => Object.assign({}, baseRow, { master, sku, skus: [sku], charm: r, nearClosed: nc || undefined, flags: [] });
  const X = JSON.parse(JSON.stringify(two)); X.master = 'MASTER SKU_X'; X.rows = [mk('MASTER SKU_X', 'SHARED_1', 1, true)]; X.orphanLabels = [];
  const Y = JSON.parse(JSON.stringify(X)); Y.master = 'MASTER SKU_Y'; Y.rows = [mk('MASTER SKU_Y', 'SHARED_1', 2, false)];
  const fx = path.join(dir, 'x.json'), fy = path.join(dir, 'y.json'); fs.writeFileSync(fx, JSON.stringify(X)); fs.writeFileSync(fy, JSON.stringify(Y));
  const heldBy = m => { const f = path.join(dir, 'index-' + m + '.json'); fs.writeFileSync(f, JSON.stringify({ entries: [{ sku: 'SHARED_1', masterName: 'MASTER SKU_' + m + '.ai', widthPt: 10, heightPt: 10, areaPt2: 50, holes: 0 }] })); return f; };
  const run = (m) => { const d = fs.mkdtempSync(path.join(os.tmpdir(), 'bc-rep2-')); S.report(['--scan', fx, fy, '--index', heldBy(m), '--out', d]); return JSON.parse(fs.readFileSync(path.join(d, 'BROKENCHARM-rewrite-skus.json'), 'utf8')); };
  // a SKU on two drawings of two masters is held live by one of them: the other master's design is not re-indexed under it
  const heldByX = run('X'); assert.deepEqual(heldByX.skus, ['SHARED_1'], 'the master that holds the SKU live is re-indexed'); assert.deepEqual(heldByX.notReindexedHeldByAnotherMaster, {});
  assert(heldByX.skuInSeveralMasters.SHARED_1 && heldByX.skuInSeveralMasters.SHARED_1.length === 2, 'the SKU in two masters is named in the list: ' + JSON.stringify(heldByX.skuInSeveralMasters));
  const heldByY = run('Y'); assert.deepEqual(heldByY.skus, [], 'a SKU the library holds from another master is not re-indexed under this design'); assert(heldByY.notReindexedHeldByAnotherMaster.SHARED_1, 'and the list says why');
  console.log('broken-charm OK: a body open by 0.09 pt is closed, the dachshund huggie is a body with its ring and engraving, a 4 pt opening stays open');
})().catch(e => { console.error(e); process.exit(1); });
