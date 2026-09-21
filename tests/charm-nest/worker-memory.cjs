// Exercise the real Worker boundary, including structured cloning of live bests.
const assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const { serialize } = require('node:v8');
const Solver = require('../../charm-nest-solver.js');
const root = path.join(__dirname, '../..');
function bytes(value, seen = new Set()) {
  if (!value || typeof value !== 'object' || seen.has(value)) return 0;
  seen.add(value);
  if (ArrayBuffer.isView(value)) return bytes(value.buffer, seen);
  if (value instanceof ArrayBuffer) return value.byteLength;
  return [...(value instanceof Map ? value.values() : Object.values(value))].reduce((sum, v) => sum + bytes(v, seen), 0);
}
(async () => {
  const messages = []; let internalBytes = 0, bestBytes = 0;
  const job = {
    sheet: { wPt: 100, hPt: 50, insetPt: 1 }, clearancePt: 0,
    angles: Array.from({ length: 180 }, (_, i) => i * 2), fineRes: 2, coarseRes: .5,
    timeBudgetMs: 10000, maxTrials: 1, maxFill: .74, seed: 2,
    pieces: Array.from({ length: 3 }, (_, i) => ({ id: String(i), w: 18, h: 12, scale: 1,
      bits: Uint8Array.from({ length: 216 }, (_, j) => +(j % 18 < 12 || j < 72)), order: String(i), orderDate: i + 1 }))
  };
  const c = vm.createContext({ importScripts() {}, self: { postMessage(message) {
    const cloned = structuredClone(message);
    if (cloned.type === 'best') {
      assert.equal(cloned.best.rec, undefined); assert.equal(cloned.best.grids, undefined);
      assert.equal(bytes(cloned.best), 0, 'no raster masks cross the worker boundary');
      assert(Number.isFinite(cloned.best.contactQuality), 'ranking evidence is preserved');
      bestBytes = Math.max(bestBytes, serialize(cloned).byteLength);
    }
    messages.push(cloned);
  } }, CharmNestSolver: { ...Solver, solve: (job, cb) => Solver.solve(job, { ...cb, yield: () => Promise.resolve(), onBest(best, summary) {
    internalBytes = Math.max(internalBytes, bytes(best));
    cb.onBest(best, summary);
    assert(best.rec && best.grids, 'publishing must not strip the running solver state');
  } }) } });
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-worker.js'), 'utf8'), c);
  await c.self.onmessage({ data: { type: 'solve', jobId: 'dense', job } });
  assert(!messages.some(m => m.type === 'error'));
  assert(internalBytes > 1000000); assert(bestBytes > 0 && bestBytes < 16000);
  const result = messages.find(m => m.type === 'done').result;
  assert.equal(result.placements.length, 3); assert(Solver.verify(job, result.placements, 6).ok);
  assert.equal(result.params.angles.length, 180);

  // Learned/challenger results use the same boundary, including the final result.
  c.self.CharmNestLearned = c.CharmNestLearned = { solve: async (job, cb) => {
    const best = { ...result, rec: { uncloneable() {} }, grids: { uncloneable() {} } };
    cb.onBest(best, { total: 3 }); return best;
  } };
  await c.self.onmessage({ data: { type: 'solve', jobId: 'learned', job: { learned: {} } } });
  assert.equal(messages.at(-1).type, 'done');
  console.log(`Worker memory OK: ${internalBytes} internal mask bytes; largest best message ${bestBytes} bytes; all 180 angles retained`);
})().catch(e => { console.error(e); process.exitCode = 1; });
