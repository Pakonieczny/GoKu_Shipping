// MONOHOOPS (Paul, 19 Oct 2026): "1. The Monograms need 2 hoops per each monogram. 2. The hoops must be placed on the left and right side of each Monogram. 3. The hoops must be aligned on the same horizontal axis to ensure it
// hangs correctly. 4. Do NOT place the hoops right at the edge of the monogram, each hoop should be near the center of the Left and Right letters of each Monogram. The spacing can vary slightly within reason, but must be pretty
// closely aligned." The real engine over Paul's REAL 78-letter alphabet file, offline (charm-nest-monogram.js section 3b; the service's upgrade of designs approved before the hoops, charm-nest-monogram-ui.js):
//   1  for a spread of words (ABC MJW LOA IAI WMW OXO; a narrow I on the left, a wide W on the right, letters with peaks A / V and flat tops T / E, script loops): exactly two hoop holes are added to the letters' own holes;
//      both hoop centres on ONE line (the same y); each hoop's x in the middle band of its own letter and never within EDGE_MIN_MM of the piece's side; every ring welded (its overlap with the piece measured here, independently, with
//      Clipper) and every hole clear of the ink with the full metal wall round it (no inner opening within the wall); symmetric within a tolerance where the letters are alike; the piece still ONE body; the judged rules (metrics.violations)
//      exactly those of the same word built without hoops
//   2  the file: the PDF reads back as one piece with the two hoop holes, the size of the piece with its hoops
//   3  the size: a piece fitted to a width keeps its hoops at their real size (1.30 mm hole) inside the width; a height envelope holds the letters AND the hoops; autoFit's piece carries them
//   4  designs approved before the hoops (no hoops tag on the design) and not on a sheet: replaced by the same piece WITH its hoops, the approval and the seals kept, when the piece made now is the approved one; refused (with words)
//      when it is not; a design on a sheet is never touched; a new approval is tagged { hoops: 2 }
//   MONOHOOPS_PNG=<dir> (with PW_DIR / CHROMIUM) also draws the pieces to PNG.     node tests/charm-nest/monogram-hoops.cjs
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
global.ClipperLib = require(path.join(root, 'vendor/clipper-6.4.2.js'));
const C = global.ClipperLib;
const M = require(path.join(root, 'charm-nest-monogram.js')), UI = require(path.join(root, 'charm-nest-monogram-ui.js'));
const { CharmNestPDF: P, Geom } = require(path.join(root, 'netlify/functions/_charmNestPdf.js'));
const R = require('./_monogramReal.cjs');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); if (!ok || process.env.VERBOSE) console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const section = t => console.log('\n── ' + t);
const MM = 25.4 / 72, SC = 1000, H = M.HOOPS, f2 = v => (+v).toFixed(2);

// independent geometry (Clipper, 0.001 pt): the piece's ink, its inner openings, a disc
const ints = pts => pts.map(p => ({ X: Math.round(p[0] * SC), Y: Math.round(p[1] * SC) }));
const run = (type, a, b, fa, fb) => { const c = new C.Clipper(), out = new C.Paths(); c.AddPaths(a, C.PolyType.ptSubject, true); if (b && b.length) c.AddPaths(b, C.PolyType.ptClip, true); c.Execute(type, out, fa, fb); return out; };
const EO = C.PolyFillType.pftEvenOdd, NZ = C.PolyFillType.pftNonZero;
const inkOf = r => run(C.ClipType.ctUnion, r.rings.map(g => ints(g.pts)), null, EO, EO);
const countersOf = r => r.rings.filter(g => g.hole).map(g => ints(g.pts).reverse());
const meetArea = (a, b) => run(C.ClipType.ctIntersection, a, b, NZ, NZ).reduce((s, p) => s + C.Clipper.Area(p), 0) / SC / SC * MM * MM;
const disc = (xPt, yPt, rMm) => { const r = rMm / MM * SC, o = []; for (let i = 0; i < 128; i++) { const t = i / 128 * 2 * Math.PI; o.push({ X: Math.round(xPt * SC + r * Math.cos(t)), Y: Math.round(yPt * SC + r * Math.sin(t)) }); } return o; };
// a hoop hole of the piece: a ring whose every point lies HOLE_D_MM / 2 from (x, y)
const isHoopHole = (g, h) => g.hole && g.pts.every(p => Math.abs(Math.hypot(p[0] - h.xPt, p[1] - h.yPt) * MM - H.HOLE_D_MM / 2) < 0.01);

(async () => {
  const T0 = Date.now(), L = await R.load();
  const WORDS = ['ABC', 'MJW', 'LOA', 'IAI', 'WMW', 'OXO', 'IMW', 'AHA', 'VAV', 'TET', 'EOE', 'CKC', 'HPH', 'ESG', 'QYQ', 'APV'];
  const ALIKE = ['OXO', 'IAI', 'VAV', 'EOE', 'ESG'];          // (the two side letters alike enough that the hoops stand symmetric within SYM_ALIKE_MM)
  const SYM_ALIKE_MM = 1.0;
  const built = {};

  // ═══ 1 · two hoops, level, near the side letters' centres, welded, clear ═══
  section('1 · two hoops per monogram, on one line, near the centres of the left and right letters');
  for (const w of WORDS) {
    const trio = await L.word(w), base = M.build(trio, { hoops: false }), r = M.build(trio);
    built[w] = r;
    if (!base.ok || !r.ok) { check(false, `${w}: built (${base.why || r.why})`); continue; }
    const hp = r.report.hoops, W = r.bboxPt[2];
    check(!!hp && hp.ok === true && hp.version === H.VERSION, `${w}: report.hoops says two hoops were placed (version ${hp && hp.version})${hp && !hp.ok ? ': ' + hp.why : ''}`);
    if (!hp || !hp.ok) continue;
    const L0 = hp.left, R0 = hp.right, hoopHoles = r.rings.filter(g => isHoopHole(g, L0) || isHoopHole(g, R0));
    check(r.rings.filter(g => g.hole).length === base.rings.filter(g => g.hole).length + 2 && hoopHoles.length === 2 && hoopHoles.filter(g => isHoopHole(g, L0)).length === 1, `${w}: exactly 2 hoop holes are added (${base.rings.length - 1} letter holes, now ${r.rings.length - 1})`);
    check(r.rings.filter(g => !g.hole).length === 1 && !r.rings[0].hole && R.shoelace(r.rings[0].pts) > 0, `${w}: still ONE body (one outer ring, counter-clockwise)`);
    check(L0.yPt === R0.yPt && L0.yPt === hp.yPt && hp.levelMm === 0, `${w}: both hoop centres on one horizontal line (y ${L0.yPt} = ${R0.yPt} pt)`);
    const holeYs = hoopHoles.map(g => { const ys = g.pts.map(p => p[1]); return (Math.min(...ys) + Math.max(...ys)) / 2; });
    check(Math.abs(holeYs[0] - holeYs[1]) < 2e-3, `${w}: and the two holes drawn in the piece are level too (${holeYs.map(v => v.toFixed(3)).join(' / ')} pt)`);
    // each x in the middle band of its own letter (the band: BAND of the letter's width, round the centre of its body), never at the piece's edge
    const own = [r.placed[0], r.placed[2]];
    for (const [h, p, nm] of [[L0, own[0], 'left'], [R0, own[1], 'right']]) {
      const bw = p.bboxPt[2] - p.bboxPt[0], bc = (p.bboxPt[0] + p.bboxPt[2]) / 2, band = h.band;
      check(h.xPt >= band[0] - 1e-6 && h.xPt <= band[1] + 1e-6 && band[1] - band[0] <= H.BAND * bw + 1e-3 && Math.abs((band[0] + band[1]) / 2 - bc) <= 0.25 * bw, `${w}: the ${nm} hoop (x ${f2(h.xMm)} mm) is in the middle band of its letter ${h.letter} (${f2(band[0] * MM)} to ${f2(band[1] * MM)} mm; the letter ${f2(p.bboxPt[0] * MM)} to ${f2(p.bboxPt[2] * MM)} mm), ${f2(Math.abs(h.offsetMm))} mm from its centre`);
      check(h.xPt * MM >= H.EDGE_MIN_MM - 1e-6 && (W - h.xPt) * MM >= H.EDGE_MIN_MM - 1e-6 && (h.xPt - H.OUTER_D_MM / 2 / MM) > 0 && (h.xPt + H.OUTER_D_MM / 2 / MM) < W, `${w}: the ${nm} hoop is not at the piece's edge (${f2(Math.min(h.xPt, W - h.xPt) * MM)} mm in, limit ${H.EDGE_MIN_MM})`);
    }
    // welded and clear, measured here on the piece without hoops
    const ink = inkOf(base), counters = countersOf(base);
    for (const [h, nm] of [[L0, 'left'], [R0, 'right']]) {
      const weld = meetArea([disc(h.xPt, h.yPt, H.OUTER_D_MM / 2)], ink), inHole = meetArea([disc(h.xPt, h.yPt, H.HOLE_D_MM / 2)], ink);
      const wall = counters.length ? meetArea([disc(h.xPt, h.yPt, H.OUTER_D_MM / 2)], counters) : 0;
      check(h.neckMm > 0 ? h.weldMm2 >= H.NECK_W_MM * H.NECK_PEN_MM * 0.6 - 1e-6 : weld >= H.WELD_MIN_MM2 - 0.01, `${w}: the ${nm} ring is welded to the piece (overlap ${f2(weld)} mm2, limit ${H.WELD_MIN_MM2}${h.neckMm ? `; a ${f2(h.neckMm)} mm neck` : ''})`);
      check(inHole < 1e-4 && h.inkClearMm >= H.INK_CLEAR_MM - 0.03, `${w}: the ${nm} hole is clear of every ink (${f2(inHole)} mm2 of ink in it, ${f2(h.inkClearMm)} mm to the nearest)`);
      check(wall < 1e-4, `${w}: the ${nm} ring touches no inner opening, so the hole keeps its full ${f2((H.OUTER_D_MM - H.HOLE_D_MM) / 2)} mm wall all round`);
    }
    // the piece with its hoops covers the piece without them (nothing of the letters is lost), and grows only upward
    const lost = run(C.ClipType.ctDifference, ink, inkOf(r), NZ, NZ).reduce((s, p) => s + C.Clipper.Area(p), 0) / SC / SC * MM * MM;
    const hoopHoleArea = 2 * Math.PI * (H.HOLE_D_MM / 2) ** 2;
    check(lost < 0.01, `${w}: no ink of the letters is lost (${f2(lost)} mm2; the hoop holes are ${f2(hoopHoleArea)} mm2 of air over no ink)`);
    check(r.bboxPt[0] === 0 && r.bboxPt[1] === 0 && Math.abs(r.bboxPt[2] - base.bboxPt[2]) < 1e-3 && r.bboxPt[3] >= base.bboxPt[3] - 1e-3 && Math.abs(r.hMm - r.bboxPt[3] * MM) < 1e-3 && Math.abs(hp.lettersMm[1] - base.hMm) < 2e-3,
      `${w}: the box is the whole piece (${f2(r.wMm)} x ${f2(r.hMm)} mm; the letters ${f2(base.wMm)} x ${f2(base.hMm)} mm, the hoops add ${f2(hp.addedHeightMm)} mm)`);
    // the judged rules are the letters', exactly as without hoops
    const keys = x => x.report.metrics.violations.map(v => v.key).sort().join();
    check(keys(r) === keys(base) && !keys(r).includes('hoops'), `${w}: no rule regresses (${keys(base) || 'none'} without hoops, ${keys(r) || 'none'} with)`);
    if (ALIKE.includes(w)) check(hp.asymmetryMm <= SYM_ALIKE_MM, `${w}: alike side letters, the hoops stand symmetric about the piece's centre within ${SYM_ALIKE_MM} mm (${f2(hp.asymmetryMm)} mm)`);
    console.log(`    ${w}: ${f2(r.wMm)} x ${f2(r.hMm)} mm, hoops at y ${f2(hp.yMm)} mm, x ${f2(L0.xMm)} / ${f2(R0.xMm)} mm (letter centres ${f2(L0.centreMm)} / ${f2(R0.centreMm)}), welds ${f2(L0.weldMm2)} / ${f2(R0.weldMm2)} mm2, necks ${f2(L0.neckMm)} / ${f2(R0.neckMm)} mm, asymmetry ${f2(hp.asymmetryMm)} mm${hp.warnings.length ? ' · ' + hp.warnings.join(' | ') : ''}`);
  }
  check(H.OUTER_D_MM === 2.3 && H.HOLE_D_MM === 1.3 && Math.abs(H.OUTER_D_MM / 2 / MM - 3.26) < 0.01 && Math.abs(H.HOLE_D_MM / 2 / MM - 1.84) < 0.01, `the hoop is the shop's necklace hoop: ${H.OUTER_D_MM} mm across (r ${f2(H.OUTER_D_MM / 2 / MM)} pt), hole ${H.HOLE_D_MM} mm (r ${f2(H.HOLE_D_MM / 2 / MM)} pt)`);
  check(Object.values(built).filter(r => r && r.ok && r.report.hoops && r.report.hoops.ok).length === WORDS.length, `every word of the spread has its hoops (${WORDS.length})`);
  const sound = Object.entries(built).map(([w, r]) => [w, R.sound(r)]).filter(([, b]) => b.length);
  check(!sound.length, 'every piece is sound by the pipeline\'s own measure (one piece, counters inside, box, the letters 14.8 mm tall): ' + sound.map(([w, b]) => w + ': ' + b.join('; ')).join(' | '));
  const off = M.build(await L.word('ABC'), { hoops: false });
  check(off.ok && !off.report.hoops && off.rings.length === built.ABC.rings.length - 2, 'opts.hoops: false builds the letters alone (no report.hoops, two holes fewer)');

  // ═══ 2 · the file ═══
  section('2 · the file is the piece with its hoops');
  for (const w of ['APV', 'OXO']) {
    const r = built[w], bytes = await M.toPdf(r, { title: 'Monogram ' + w }), pf = await R.preflight(bytes, 'Monogram ' + w + '.ai');
    const cuts = P.cutLinesOf(pf.charms[0]);
    check(pf.pieces === 1 && Math.abs(pf.wMm - r.wMm) < 0.05 && Math.abs(pf.hMm - r.hMm) < 0.05, `${w}: the cut-file preflight reads ONE piece of ${f2(pf.wMm)} x ${f2(pf.hMm)} mm (the piece with its hoops: ${f2(r.wMm)} x ${f2(r.hMm)})`);
    check(1 + cuts.length === r.rings.length, `${w}: its outline + ${cuts.length} cut lines = the ${r.rings.length} rings (the letters' holes and the two hoop holes; no hoop of the file is welded again)`);
  }

  // ═══ 3 · the size ═══
  section('3 · a fitted piece keeps its hoops at their real size');
  {
    const trio = await L.word('APV'), plain = built.APV, mm = 16, r = M.build(trio, { fit: { maxWidthMm: mm } }), hp = r.report.hoops;
    const hole = r.rings.find(g => isHoopHole(g, hp.left));
    check(r.ok && hp.ok && r.wMm <= mm + 1e-3 && r.report.fit.applied && !!hole, `fitted to ${mm} mm wide: ${f2(r.wMm)} x ${f2(r.hMm)} mm, scale ${r.report.fit.scale}, the hoops on it`);
    check(!!hole && Math.abs(Math.max(...hole.pts.map(p => p[0])) - Math.min(...hole.pts.map(p => p[0])) - H.HOLE_D_MM / MM) < 0.02, `and the hoop hole is still ${H.HOLE_D_MM} mm across (a jump ring fits whatever the piece's size)`);
    check(Math.abs(hp.lettersMm[0] - plain.report.hoops.lettersMm[0] * r.report.fit.scale) < 0.01, 'the letters are scaled as before (the width envelope is the letters\' width: the hoops stand inside it)');
    const mh = plain.hMm - 2, t = M.build(trio, { fit: { maxWidthMm: 40, maxHeightMm: mh } });
    check(t.ok && t.report.hoops.ok && t.hMm <= mh + 2e-3 && t.hMm >= mh - 0.3, `a height envelope of ${f2(mh)} mm holds the letters AND the hoops: ${f2(t.hMm)} mm tall (letters ${f2(t.report.hoops.lettersMm[1])} mm + hoops ${f2(t.report.hoops.addedHeightMm)} mm), scale ${t.report.fit.scale}`);
    const af = M.autoFit(await L.word('MJW'), { autoFitMs: 1500 });
    check(af.ok && af.report.hoops && af.report.hoops.ok && af.rings.filter(g => isHoopHole(g, af.report.hoops.left) || isHoopHole(g, af.report.hoops.right)).length === 2 && !!af.report.autoFit, `autoFit's piece carries its two hoops (${af.report.autoFit && af.report.autoFit.tried} builds tried)`);
    // a piece that already has its hoops, scaled afterwards (CharmNestMonogramFit.applyFit on a finished result): the hoops are scaled with it and report.hoops follows, and says so
    const F = require(path.join(root, 'charm-nest-monogram-fit.js')), cp = JSON.parse(JSON.stringify(built.APV)); F.applyFit(cp, { maxWidthMm: 16 });
    const ks = cp.report.fit.scale;
    check(R.sound(cp, { scale: ks }).length === 0 && Math.abs(cp.report.hoops.sizeMm.hole - H.HOLE_D_MM * ks) < 2e-3 && cp.report.warnings.some(t => /hoops were scaled/.test(t)), `a finished piece scaled afterwards keeps a consistent report.hoops (hole ${cp.report.hoops.sizeMm.hole} mm, said in the warnings): ${R.sound(cp, { scale: ks }).join('; ') || 'sound'}`);
  }

  // ═══ 4 · designs approved before the hoops ═══
  section('4 · a design approved before the hoops gets them, its letters and approval kept');
  {
    const lineOf = (rid, tx, qty, words) => ({ key: `${rid}_${tx}`, state: 'pulled', order: { receiptId: rid }, line: { transactionId: tx, quantity: qty, sku: 'MONOGRAM_5351' }, poolIds: [], spec: { quantity: qty, personalization: [words], special: { monogram: true, words }, material: 'gold' } });
    function world(rows) {
      const store = {}, entries = new Map(L.skus.map(s => [s, { sku: s, aiPath: 'charmnest/master/' + s + '.ai', indexedAt: 1 }])); let ids = 0;
      const CS = {
        entries: () => store, cardOf: it => (it ? { ck: it.key } : null),
        add(it, files, opts = {}) { const e = store[it.key] || (store[it.key] = { ck: it.key, rid: it.rid, files: [], sent: null }); if (e.sent) return; for (const f of files) e.files.push(Object.assign({ id: 'f' + (++ids), name: f.name, state: 'ready', metal: opts.metal || 'gold', qty: 1, pieces: 1, wMm: 0, hMm: 0, file: f }, opts.mono ? { mono: Object.assign({ line: String(opts.mono.line), slot: opts.mono.slot || null }, opts.mono.hoops ? { hoops: opts.mono.hoops } : {}) } : {})); },
        remove(ck, id) { const e = store[ck]; if (!e || e.sent) return; e.files = e.files.filter(f => f.id !== id); }, open() {}
      };
      const d = { rows: () => rows, skus: () => L.skus, settings: () => (d._s || (d._s = {})), saveSettings() {}, toast() {}, changed() {}, CNProgress: null, Engine: M, Geom, P, itemOf: row => ({ key: 'ord:custom:' + row.order.receiptId + ':X', rid: row.order.receiptId, rows: [row], row }), CustomSheet: CS,
        Master: { entryFor: s => entries.get(s) || null, fetchEntry: async s => entries.get(s) || null }, Pool: { masterCharm: async e => L.masterCharm(e) }, pictureOf: async () => 'data:image/png;base64,AA' };
      const ui = UI.make(d);
      return { ui, store, card: (row, n) => { ui.status(row, n); return ui.cards.get(ui.pieceKey(row, n)); } };
    }
    // the size an approved file is read at (CustomSheet.ingest: the cut outline's box) — here from the PDF the engine wrote
    const readSize = async F => { const pf = await R.preflight(new Uint8Array(await F.file.arrayBuffer()), F.name); F.wMm = pf.wMm; F.hMm = pf.hMm; return pf; };
    const ck = 'ord:custom:4210000999:X';
    // a design made before the hoops: the same letters, built without them, written as the page wrote it then (no hoops in its tag)
    // (the page's own piece of these letters: the card's build, autoFit included; the pre-hoops design is the same letters, the same adjustments, without hoops)
    const w0 = world([lineOf('4210000990', '900', 1, 'APV')]), c0 = w0.card(w0.ui.rows()[0]); await w0.ui.rebuild(c0);
    const row = lineOf('4210000999', '901', 1, 'APV'), w = world([row]), plainBytes = await M.toPdf(M.build(await L.word('APV'), { hoops: false, adjust: c0.result.report.adjust }), { title: 'Monogram APV' });
    w.store[ck] = { ck, rid: '4210000999', sent: null, files: [{ id: 'old1', name: 'Monogram APV.ai', qty: 1, metal: 'rose', state: 'ready', pieces: 1, wMm: 0, hMm: 0, mono: { line: row.key, slot: null }, file: { arrayBuffer: async () => plainBytes.buffer.slice(plainBytes.byteOffset, plainBytes.byteOffset + plainBytes.length) } }] };
    await readSize(w.store[ck].files[0]);
    const seals0 = w.ui.sealsOf ? (w.ui.sealsOf(row) || []).length : 0;
    check(w.ui.status(row).approved === true, 'the old design reads as approved');
    const c = w.card(row); await w.ui.rebuild(c); if (c.hoopsBusy) await c.hoopsBusy;
    const F = w.store[ck].files[0];
    check(w.store[ck].files.length === 1 && F.id !== 'old1' && F.name === 'Monogram APV.ai' && F.metal === 'rose' && F.mono && F.mono.hoops === H.VERSION && F.mono.line === row.key, `once the card has made its piece the design is replaced by the same piece with its hoops: one design, same name and metal, tagged ${JSON.stringify(F.mono)}`);
    const pf = await readSize(F);
    check(pf.pieces === 1 && Math.abs(pf.wMm - built.APV.wMm) < 0.05 && Math.abs(pf.hMm - built.APV.hMm) < 0.05, `the new design is the piece with its hoops (${f2(pf.wMm)} x ${f2(pf.hMm)} mm)`);
    check(w.ui.status(row).approved === true && w.ui.status(row).state === 'approved' && (w.ui.sealsOf ? (w.ui.sealsOf(row) || []).length : 0) === seals0, 'the approval stands: still approved, no new seal, no second approval asked');
    const up = await w.ui.upgradeCard({ key: ck }); check(up.ok && up.n === 0, 'the send then finds nothing left to do (upgradeCard: ok, 0)');
    // the letters cannot be made the same any more (here: a design read at another size): not changed, not sent, asked again
    const row2 = lineOf('4210000998', '902', 1, 'APV'), w2 = world([row2]), ck2 = 'ord:custom:4210000998:X';
    w2.store[ck2] = { ck: ck2, rid: '4210000998', sent: null, files: [{ id: 'old2', name: 'Monogram APV.ai', qty: 1, metal: 'gold', state: 'ready', pieces: 1, wMm: 21.4, hMm: 14.1, mono: { line: row2.key, slot: null } }] };
    const c2 = w2.card(row2); await w2.ui.rebuild(c2);
    const up2 = await w2.ui.upgradeCard({ key: ck2 });
    check(!up2.ok && /approve it again/.test(up2.why) && w2.store[ck2].files.length === 1 && w2.store[ck2].files[0].id === 'old2', 'a design whose letters now make another piece is NOT changed silently and not sent: ' + up2.why);
    const q2 = w2.ui.describe(row2).queue;
    check(!q2 || q2.kind === 'hoops', 'its card asks for a new approval' + (q2 ? ': ' + q2.text : ' (no send in this world)'));
    await w2.ui.approve(c2); if (w2.store[ck2].files[0].id === 'old2') await w2.ui.approve(c2);      // (a word that still breaks a rule asks a second press)
    check(w2.store[ck2].files.length === 1 && w2.store[ck2].files[0].id !== 'old2' && w2.store[ck2].files[0].mono.hoops === H.VERSION, 'Approve then approves it again: the new design carries its hoops');
    // a design on a sheet is never touched
    const row3 = lineOf('4210000997', '903', 1, 'APV'), w3 = world([row3]), ck3 = 'ord:custom:4210000997:X';
    w3.store[ck3] = { ck: ck3, rid: '4210000997', sent: { at: 1, by: 'x', lines: {} }, files: [{ id: 'old3', name: 'Monogram APV.ai', qty: 1, metal: 'gold', state: 'ready', pieces: 1, wMm: 18, hMm: 14.8, mono: { line: row3.key, slot: null } }] };
    const c3 = w3.card(row3); await w3.ui.rebuild(c3); if (c3.hoopsBusy) await c3.hoopsBusy;
    check(w3.store[ck3].files.length === 1 && w3.store[ck3].files[0].id === 'old3' && !w3.store[ck3].files[0].mono.hoops && w3.ui.status(row3).sent === true, 'a design already on a sheet is never touched');
    // a fresh approval is tagged with the hoops
    const row4 = lineOf('4210000996', '904', 1, 'OXO'), w4 = world([row4]), ck4 = 'ord:custom:4210000996:X';
    const c4 = w4.card(row4); await w4.ui.rebuild(c4); await w4.ui.approve(c4); if (!w4.store[ck4]) await w4.ui.approve(c4);
    const F4 = (w4.store[ck4] || { files: [] }).files[0];
    check(!!F4 && F4.mono && F4.mono.hoops === H.VERSION && c4.result.report.hoops.ok, 'a new approval writes the piece with its hoops, its design tagged { hoops: ' + H.VERSION + ' }');
  }

  // ═══ pictures ═══
  const dir = process.env.MONOHOOPS_PNG, pw = process.env.PW_DIR;
  if (dir && pw) {
    fs.mkdirSync(dir, { recursive: true });
    const { chromium } = require(path.join(pw, 'playwright-core'));
    const exe = process.env.CHROMIUM && fs.statSync(process.env.CHROMIUM).isFile() ? process.env.CHROMIUM : undefined;
    const br = await chromium.launch(exe ? { executablePath: exe } : {}), pg = await br.newPage({ viewport: { width: 900, height: 700 }, deviceScaleFactor: 2 });
    for (const w of WORDS) {
      const r = built[w]; if (!r || !r.ok) continue;
      const b = r.bboxPt, pad = Math.max(b[2], b[3]) * 0.06, hp = r.report.hoops;
      const line = hp && hp.ok ? `<line x1="${-pad}" x2="${b[2] + pad}" y1="${b[3] - hp.yPt}" y2="${b[3] - hp.yPt}" stroke="#d33" stroke-width="0.12" stroke-dasharray="0.6 0.4"/>` : '';
      const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${-pad} ${-pad} ${b[2] + 2 * pad} ${b[3] + 2 * pad}" width="860" height="${Math.round(860 * (b[3] + 2 * pad) / (b[2] + 2 * pad))}"><rect x="${-pad}" y="${-pad}" width="${b[2] + 2 * pad}" height="${b[3] + 2 * pad}" fill="#fff"/><path d="${M.svgPath(r)}" fill="#d9bd78" fill-rule="evenodd" stroke="#5a4520" stroke-width="0.15" stroke-linejoin="round"/>${line}</svg>`;
      await pg.setContent(`<html><body style="margin:0;background:#fff;font:14px sans-serif"><div style="padding:6px">${w} · ${f2(r.wMm)} x ${f2(r.hMm)} mm · hoops ${hp && hp.ok ? `at ${f2(hp.yMm)} mm, asymmetry ${f2(hp.asymmetryMm)} mm` : 'none'}</div>${svg}</body></html>`);
      await pg.screenshot({ path: path.join(dir, `monogram-hoops-${w}.png`), fullPage: true });
    }
    await br.close(); console.log(`\n  pictures: ${dir}/monogram-hoops-<WORD>.png`);
  }

  console.log(`\nmonogram-hoops: ${fails.length ? fails.length + ' FAILED of ' : 'all '}${nCheck} checks${fails.length ? '' : ' passed'} in ${Math.round((Date.now() - T0) / 1000)} s`);
  if (fails.length) { for (const f of fails) console.log(' - ' + f); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
