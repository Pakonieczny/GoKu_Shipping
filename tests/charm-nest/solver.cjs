// Solver unit test: synthetic shapes, real densities. `node tests/charm-nest/solver.cjs`
const S = require('../../charm-nest-solver.js');
const assert = require('assert');
function shape(id, kind, wPt, hPt, scale = 6) {
  const w = Math.round(wPt * scale), h = Math.round(hPt * scale), bits = new Uint8Array(w * h);
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
    const nx = (x + .5) / w * 2 - 1, ny = (y + .5) / h * 2 - 1;
    let on = kind === 'rect' ? 1 : kind === 'ellipse' ? (nx * nx + ny * ny <= 1) : kind === 'ring' ? (nx * nx + ny * ny <= 1 && nx * nx + ny * ny >= .35) : (Math.abs(nx) + Math.abs(ny) <= 1);
    if (on) bits[y * w + x] = 1;
  }
  let a = 0; for (const b of bits) a += b;
  return { id, w, h, scale, bits, areaPt2: a / (scale * scale) };
}
(async () => {
  const kinds = ['rect', 'ellipse', 'ring', 'diamond'];
  const pieces = [];
  let r = S.rng(7);
  for (let i = 0; i < 21; i++) pieces.push(shape('c' + i, kinds[i % 4], 55 + r() * 95, 55 + r() * 95));
  const job = { sheet: { wPt: 513.03, hPt: 434.19, insetPt: 1.5 }, clearancePt: 0.5, angles: [0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330], timeBudgetMs: 25000, seed: 3, pieces };
  const tot = pieces.reduce((s, p) => s + p.areaPt2, 0);
  console.log('total charm area', tot.toFixed(0), 'pt² of', (513 * 434).toFixed(0), '=', (tot / (513 * 434) * 100).toFixed(1) + '% density needed');
  let placedEvents = 0, trials = 0; const t = Date.now();
  const res = await S.solve(job, { onPlaced: () => placedEvents++, onTrial: s => { trials++; if (trials <= 3 || s.better) console.log(' trial', s.trial, s.placed + '/' + s.total, 'density', (s.density * 100).toFixed(1) + '%', 'better=' + s.better, (s.elapsedMs / 1000).toFixed(1) + 's'); } });
  console.log('result', res.placements.length + '/' + pieces.length, 'endedBy', res.endedBy, 'trials', res.trials, 'density', (res.density * 100).toFixed(1) + '%', 'pocket', res.pocket, 'in', ((Date.now() - t) / 1000).toFixed(1) + 's');
  const v = S.verify(job, res.placements, 6);
  console.log('verify', v);
  assert(v.ok, 'verifier found overlap/outside');
  assert(v.minGapPt >= job.clearancePt - 0.2, 'gap below clearance: ' + v.minGapPt);
  assert(res.placements.length >= 15, 'too few placed');
  console.log('OK');
})().catch(e => { console.error(e); process.exit(1); });
