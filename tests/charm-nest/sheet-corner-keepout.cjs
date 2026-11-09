// CORNERS (Paul, 9 Nov 2026): a 1.5 x 1.5 mm right-triangle keep-out at each physical sheet corner. `node tests/charm-nest/sheet-corner-keepout.cjs`
const S = require('../../charm-nest-solver.js'), assert = require('assert');
const MM = 72 / 25.4;
const rect = (id, wPt, hPt, sc = 6) => { const w = Math.round(wPt * sc), h = Math.round(hPt * sc); return { id, order: id, w, h, scale: sc, bits: new Uint8Array(w * h).fill(1), areaPt2: wPt * hPt }; };
(async () => {
  assert(Math.abs(S.CORNER_DEFAULT_PT - 1.5 * MM) < 1e-9);
  // packed sheets of several sizes: nothing (with clearance) in any corner triangle
  for (const [wMm, hMm] of [[100, 50], [100, 100], [60, 40]]) {
    const pieces = Array.from({ length: 30 }, (_, i) => rect('p' + i, 14 + i % 3 * 4, 12 + i % 4 * 3));
    const job = { sheet: { wPt: wMm * MM, hPt: hMm * MM, insetPt: 1.5, cornerPt: S.CORNER_DEFAULT_PT }, clearancePt: -0.5, angles: [0, 90], timeBudgetMs: 2500, seed: 1, pieces, maxFill: .8 };
    const r = await S.solve(job, {}), v = S.verify(job, r.placements, 6);
    assert(r.placements.length > 0 && v.ok && v.cornerPx === 0, `${wMm}x${hMm}: ${JSON.stringify({ n: r.placements.length, ok: v.ok, c: v.cornerPx })}`);
  }
  // a charm at the corner is refused by the grid and flagged by the verifier; at 2 mm the reach grows
  const sheet = { wPt: 100 * MM, hPt: 50 * MM, insetPt: 0, cornerPt: S.CORNER_DEFAULT_PT }, p = rect('a', 6, 6);
  const at = (cx, cy, sh = sheet) => { const g = S.makeSheetGrid(sh, 0, 2), v = S.prepareVariant(p, 0, 0, 2); return g.fits(v.fine.pm, Math.round(cx * 2 - v.solid.cx), Math.round(cy * 2 - v.solid.cy)); };
  assert(!at(3.2, 3.2), 'a drop into the top-left corner triangle is refused');
  assert(at(6, 6), 'beside the triangle it fits');
  assert(at(4, 7.5) && !at(4, 7.5, { ...sheet, cornerPt: 2 * MM }), 'a setting of 2 mm is honoured');
  const flagged = S.verify({ sheet, clearancePt: 0, pieces: [p] }, [{ id: 'a', cxPt: sheet.wPt - 3.05, cyPt: sheet.hPt - 3.05, angle: 0 }], 6);
  assert(!flagged.ok && flagged.cornerPx > 0 && flagged.cornerIds[0] === 'a', 'an old placement in a corner is flagged');
  assert(S.verify({ sheet: { ...sheet, cornerPt: undefined }, clearancePt: 0, pieces: [p] }, [{ id: 'a', cxPt: 3.05, cyPt: 3.05, angle: 0 }], 6).ok, 'a job made before the rule has none');
  // a remnant keeps it only where the stock corner is still metal
  const remnant = { version: 1, axis: 'x', step: .5, wPt: sheet.wPt, hPt: sheet.hPt, values: Array(Math.ceil(sheet.hPt / .5)).fill(40) };
  const tris = S.cornerTriangles({ ...sheet, remnant });
  assert.deepEqual(tris.map(t => t.sx), [-1, -1], 'only the two right-hand corners are still metal');
  console.log('Sheet corner keep-out OK: packed sheets clear, hand drop refused, 2 mm setting, old placement flagged, remnant corners');
})().catch(e => { console.error(e); process.exitCode = 1; });
