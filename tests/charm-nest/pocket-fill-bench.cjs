/* Measured benchmark for the pocket fill (see pocket-fill-lib.cjs). One scenario per run, JSON on stdout, so scenarios
 * run side by side:  node tests/charm-nest/pocket-fill-bench.cjs <scenario> [--root DIR] [--orders N] [--seed N] [--step 2|1]
 * `--root` is a directory holding an older charm-nest-solver.js, charm-nest-rose.js and charm-nest-1.html (the BEFORE).
 * scenarios: leftover-50x46-14k  full-50x46-14k  leftover-100x50-14k  full-100x50-14k  gold-100x50  gold-50x46
 *            full-50x46-10k  full-100x50-10k  silver-100x50  silver-50x46                                              */
'use strict';
const L = require('./pocket-fill-lib.cjs');
const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? process.argv[i + 1] : d; };
const name = process.argv[2], root = arg('root', L.REPO), orders = +arg('orders', 26), seed = +arg('seed', 7), step = +arg('step', 2);
const unjam = arg('unjam', '1') !== '0', unjamMs = +arg('unjamMs', 0);   // the unjam (9 Oct): on by default; --unjam 0 for the same run without it, --unjamMs the time ceiling of one search's unjam
const dump = arg('dump', ''); let n = 0;   // --dump DIR keeps the job of every search that turned an order away (v8-serialized), to replay one search alone
const SC = {
  'leftover-50x46-14k': { metal: 'gold14k', w: 50, h: 46, left: true },
  'full-50x46-14k': { metal: 'gold14k', w: 50, h: 46 },
  'leftover-100x50-14k': { metal: 'gold14k', w: 100, h: 50, left: true, grow: 1.35 },
  'full-100x50-14k': { metal: 'gold14k', w: 100, h: 50, grow: 1.35 },
  'gold-100x50': { metal: 'gold', w: 100, h: 50, grow: 1.35 },
  'gold-50x46': { metal: 'gold', w: 50, h: 46 },
  'full-50x46-10k': { metal: 'gold10k', w: 50, h: 46 },
  'full-100x50-10k': { metal: 'gold10k', w: 100, h: 50, grow: 1.35 },
  'silver-100x50': { metal: 'silver', w: 100, h: 50, grow: 1.35 },
  'silver-50x46': { metal: 'silver', w: 50, h: 46 },
};
const sc = SC[name]; if (!sc) { console.error('scenario?', Object.keys(SC)); process.exit(2); }
const S = L.solverOf(root), wPt = L.PT(sc.w), hPt = L.PT(sc.h);
const stock = { wPt, hPt, ...(sc.left ? { remnant: L.leftover(wPt, hPt, sc.w === 50 ? L.SCREENSHOT_STEPS : [70, 60, 50, 40, 30, 22]) } : {}) };
(async () => {
  const stream = L.makeOrders(S, orders, seed, { small: .35, mid: .40, large: .25, two: .15, grow: sc.grow || 1 });
  const t0 = Date.now();
  const run = await L.runStream({ root, metal: sc.metal, sheet: stock, orders: stream, batch: 3, seed: 1, checkRoom: true, unjam, unjamMs, onSearch: dump ? (job, result) => { if (result.rejects.length) { n++; require('fs').writeFileSync(require('path').join(dump, `${name}-${n}.v8`), require('v8').serialize(job)); } } : null });
  const m = L.measure(run, stock, { step, cut: ['gold10k', 'gold14k', 'rose'].includes(sc.metal) });
  const closed = m.sheets.filter(s => s.closed), fills = closed.map(s => s.fill);
  const all = m.sheets.map(s => s.fill);
  console.log(JSON.stringify({
    scenario: name, root: root === L.REPO ? 'repo' : root, orders, seed, sheets: m.sheets.length, closedSheets: closed.length,
    closedFill: fills.length ? +(fills.reduce((a, b) => a + b, 0) / fills.length).toFixed(3) : null, fills: m.sheets.map(s => `${s.page}:${s.fill}${s.envFill != null ? '/env' + s.envFill : ''}${s.closed ? 'c' : ''}`),
    anywhere: { leftOut: m.sheets.reduce((n, s) => n + s.leftOutAnywhere, 0), potential: m.sheets.reduce((n, s) => n + s.potentialAnywhere, 0) },
    placed: m.placedTotal, smallRoomWrong: run.roomWrong.length, leftOutWhileFits: m.leftOut.length, potentialLater: m.potential.length, leftOut: m.leftOut, potential: m.potential,
    pocketPass: { searchesWithMiss: run.searches.filter(s => s.rejects).length, filled: run.searches.reduce((n, s) => n + (s.pocket ? s.pocket.filled : 0), 0), ms: run.searches.reduce((n, s) => n + (s.pocket ? s.pocket.ms : 0), 0), worstMs: Math.max(0, ...run.searches.map(s => s.pocket ? s.pocket.ms : 0)),
      stages: ['0', '1'].map(k => { const st = run.searches.flatMap(s => ((s.pocket && s.pocket.stages) || []).filter(x => String(x.step) === k)); return { step: k === '0' ? 'whole steps' : 'half steps', runs: st.length, added: st.reduce((n, x) => n + x.added, 0), ms: st.reduce((n, x) => n + x.ms, 0) }; }) },
    unjam: (() => { const all = run.searches.map(s => s.unjam).filter(Boolean), u = all.filter(x => x.attempts); return { on: unjam, searchesWithMiss: run.searches.filter(s => s.rejects).length, searchesTried: u.length, notTried: all.filter(x => !x.attempts).reduce((o, x) => (o[x.reason || 'not needed'] = (o[x.reason || 'not needed'] || 0) + 1, o), {}), attempts: u.reduce((n, x) => n + x.attempts, 0), committed: u.filter(x => x.committed).length, rounds: u.reduce((n, x) => n + x.rounds, 0), movedSaved: u.reduce((n, x) => n + x.moved.length, 0), seatedMore: u.reduce((n, x) => n + x.filled.length, 0), ms: u.reduce((n, x) => n + x.ms, 0), worstMs: Math.max(0, ...u.map(x => x.ms)), why: u.flatMap(x => x.tries.map(t => t.ok ? 'ok' : t.why)).reduce((o, w) => (o[w] = (o[w] || 0) + 1, o), {}), tries: u.flatMap(x => x.tries) }; })(),
    searchMs: { total: run.ms, perSheet: m.sheets.map(s => Math.round(s.ms)), worstSearch: Math.max(...run.searches.map(s => s.ms)) }, wallMs: Date.now() - t0,
  }, null, 1));
})().catch(e => { console.error(e); process.exit(1); });
