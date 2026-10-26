/* OVERLAPFIX (Paul, 25 Oct 2026): "a tiny readjustment of the position should resolve the tiny overlap, but if that is not possible, then shrinking the charm by the smallest needed amount
 * so that the overlap disappears and the charm can be repositioned very slightly." The planner (charm-nest-overlap-fix.js) on small made-up sheets of render-verifier areas:
 *   1 two discs overlapping by 1 pt with room around: a shift of about 1 pt straight away from the other, no shrink
 *   2 a charm boxed in by neighbours, 1 px overlap, no room to shift: the smallest shrink (and one step less does not clear it)
 *   3 a chain A–B–C: B overlaps both, the way straight away from its overlaps is blocked by C; one fix of B clears both and touches nothing
 *   4 a charm that is not movable is never touched (its movable partner moves; two locked charms stay unresolved)
 *   5 near the sheet edge the inset is respected (the charm does not go out over it)
 * Offline, deterministic.        node tests/charm-nest/overlap-fix.cjs                                                                                                                  */
'use strict';
const assert = require('node:assert/strict');
const F = require('../../charm-nest-overlap-fix.js');
const RES = 6;

const disc = (cx, cy, r) => {
  const X0 = Math.floor(cx - r) - 1, Y0 = Math.floor(cy - r) - 1, bw = Math.ceil(2 * r) + 3, bh = bw, core = new Uint8Array(bw * bh);
  for (let y = 0; y < bh; y++) for (let x = 0; x < bw; x++) if ((X0 + x + 0.5 - cx) ** 2 + (Y0 + y + 0.5 - cy) ** 2 <= r * r) core[y * bw + x] = 1;
  return { X0, Y0, bw, bh, core, c: { x: cx, y: cy } };
};
const rect = (x0, y0, w, h) => ({ X0: x0, Y0: y0, bw: w, bh: h, core: new Uint8Array(w * h).fill(1), c: { x: x0 + w / 2, y: y0 + h / 2 } });
const sheet = (shapes) => ({ areas: new Map(Object.entries(shapes)), centers: new Map(Object.entries(shapes).map(([k, a]) => [k, a.c])) });
// the sheet after the fixes, every core at its new place
const after = (s, fixes) => new Map([...s.areas].map(([k, a]) => { const f = fixes.find(x => x.name === k); return [k, f ? F.transformArea(a, s.centers.get(k), f) : a]; }));
// independent check: pixels two cores share, with the moved ones grown by `grow` px (the margin), and pixels outside the inset
function overlapPx(areas, W, H, grow = 0, grown = new Set()) {
  const g = new Uint8Array(W * H); let n = 0;
  for (const [k, a] of areas) {
    const m = grown.has(k) ? grow : 0, mine = new Set();
    for (let y = 0; y < a.bh; y++) for (let x = 0; x < a.bw; x++) {
      if (!a.core[y * a.bw + x]) continue;
      for (let yy = -m; yy <= m; yy++) for (let xx = -m; xx <= m; xx++) { const X = a.X0 + x + xx, Y = a.Y0 + y + yy; if (X >= 0 && Y >= 0 && X < W && Y < H) mine.add(Y * W + X); }
    }
    for (const i of mine) { if (g[i]) n++; g[i] = 1; }
  }
  return n;
}
const outsidePx = (a, W, H, inset) => { let n = 0; for (let y = 0; y < a.bh; y++) for (let x = 0; x < a.bw; x++) { if (!a.core[y * a.bw + x]) continue; const X = a.X0 + x, Y = a.Y0 + y; if (X < inset || Y < inset || X >= W - inset || Y >= H - inset) n++; } return n; };
const run = (name, fn) => { const t = Date.now(); fn(); console.log(`  ok  ${name}  (${Date.now() - t} ms)`); };

run('1 · two discs 1 pt into each other: shifted about 1 pt away, no shrink', () => {
  const W = 600, H = 400, inset = 12;
  const s = sheet({ A: disc(250, 200, 60), B: disc(250 + 114, 200, 60) });   // 6 px = 1 pt deep
  assert.ok(F.findOverlaps(s.areas).length === 1);
  const r = F.plan({ W, H, inset, res: RES, areas: s.areas, centers: s.centers, movable: new Set(['A', 'B']), prefer: new Map([['A', 1], ['B', 0]]) });
  assert.equal(r.ok, true, r.why);
  assert.equal(r.fixes.length, 1);
  const f = r.fixes[0];
  assert.equal(f.name, 'B');
  assert.equal(f.scale, 1);
  assert.ok(f.dx >= 7 && f.dx <= 8 && Math.abs(f.dy) <= 1, `straight away from A: ${f.dx},${f.dy}`);
  assert.ok(f.movedPt >= 1 && f.movedPt <= 1.5, `${f.movedPt} pt`);
  assert.equal(overlapPx(after(s, r.fixes), W, H, 1, new Set(['B'])), 0);
});

run('2 · boxed in, 1 px overlap, no room: the smallest shrink', () => {
  const W = 600, H = 400, inset = 12;
  const s = sheet({
    B: rect(240, 140, 120, 120),        // x 240..359, y 140..259
    L: rect(200, 100, 41, 201),         // to x 240: 1 px into B
    R: rect(361, 100, 40, 201),         // 1 px gap
    T: rect(242, 100, 118, 39),         // to y 138: 1 px gap
    D: rect(242, 261, 118, 40),         // from y 261: 1 px gap
  });
  const args = { W, H, inset, res: RES, areas: s.areas, centers: s.centers, movable: new Set(['B']) };
  const r = F.plan(args);
  assert.equal(r.ok, true, r.why);
  assert.equal(r.fixes.length, 1);
  const f = r.fixes[0];
  assert.equal(f.name, 'B');
  assert.ok(f.scale < 1 && f.scale >= 0.97, `scale ${f.scale}`);
  assert.ok(f.movedPt <= 0.5, `moved ${f.movedPt} pt`);
  assert.equal(overlapPx(after(s, r.fixes), W, H), 0, 'scaled mask overlaps');
  assert.equal(overlapPx(after(s, r.fixes), W, H, 1, new Set(['B'])), 0, 'the margin is kept');
  // the smallest: one shrink step less does not clear it with the margin (only the margin-free fallback can: B pressed against L and R, sharing no pixel)
  const less = F.plan(Object.assign({}, args, { maxShrink: +(1 - f.scale - 0.005).toFixed(4) }));
  assert.ok(less.fixes.every(x => x.marginPx === 0), JSON.stringify(less.fixes));
});

run('3 · chain A–B–C: B clears both, not by going into C', () => {
  const W = 600, H = 500, inset = 12;
  const s = sheet({ A: disc(200, 200, 30), B: disc(240, 240, 30), C: disc(299, 240, 30) });   // A 3 px into B (upper left), C 1 px into B (right)
  const pairs = F.findOverlaps(s.areas);
  assert.deepEqual(pairs.map(p => p.a + p.b), ['AB', 'BC']);
  const r = F.plan({ W, H, inset, res: RES, areas: s.areas, centers: s.centers, movable: new Set(['A', 'B', 'C']), prefer: new Map([['B', 0], ['A', 1], ['C', 1]]) });
  assert.equal(r.ok, true, r.why);
  assert.deepEqual(r.fixes.map(f => f.name), ['B'], 'one fix of B clears both');
  const f = r.fixes[0];
  assert.equal(f.scale, 1);
  assert.ok(f.dx <= 0 && f.dy > 0, `down, not towards C: ${f.dx},${f.dy}`);
  assert.ok(f.movedPt <= 3);
  assert.equal(overlapPx(after(s, r.fixes), W, H, 1, new Set(['B'])), 0);
});

run('4 · a charm that is not movable is never touched', () => {
  const W = 700, H = 400, inset = 12;
  const s = sheet({ big: disc(200, 200, 60), small: disc(200 + 85, 200, 30), D: disc(500, 200, 40), E: disc(500 + 76, 200, 40) });
  // small would go first (lower prefer, smaller) but is locked; D and E are both locked
  const r = F.plan({ W, H, inset, res: RES, areas: s.areas, centers: s.centers, movable: new Set(['big']), prefer: new Map([['small', 0], ['big', 1]]) });
  assert.deepEqual(r.fixes.map(f => f.name), ['big']);
  assert.ok(r.fixes[0].dx < 0, 'big moves away from small');
  assert.equal(r.ok, false);
  assert.deepEqual(r.unresolved.map(u => u.pair), ['D ↔ E']);
  const moved = after(s, r.fixes);
  assert.equal(overlapPx(new Map([['big', moved.get('big')], ['small', moved.get('small')]]), W, H), 0);
});

run('5 · at the sheet edge the inset is kept', () => {
  const W = 400, H = 400, inset = 12;
  const s = sheet({ B: disc(42, 200, 30), A: disc(82, 240, 30) });   // B's core starts exactly at the inset; A 3 px into it from the lower right
  assert.equal(outsidePx(s.areas.get('B'), W, H, inset), 0);
  const r = F.plan({ W, H, inset, res: RES, areas: s.areas, centers: s.centers, movable: new Set(['A', 'B']), prefer: new Map([['B', 0], ['A', 1]]) });
  assert.equal(r.ok, true, r.why);
  const f = r.fixes[0];
  assert.equal(f.name, 'B');
  assert.ok(f.dx >= 0 && f.dy < 0, `up, not out over the edge: ${f.dx},${f.dy}`);
  const moved = after(s, r.fixes);
  assert.equal(outsidePx(moved.get('B'), W, H, inset), 0);
  assert.equal(overlapPx(moved, W, H, 1, new Set(['B'])), 0);
});

console.log('overlap-fix: all passed');
