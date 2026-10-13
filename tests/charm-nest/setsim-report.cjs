/* SETSIM report: the before/after table of two sets of setsim-run.cjs results (same seeds on both sides).
 *   node tests/charm-nest/setsim-report.cjs --before a.json,b.json --after c.json,d.json [--md out.md]
 * Counts are summed over the seeds, densities and fronts are averaged over the seeds' sheets (weighted by sheet count).                                           */
'use strict';
const fs = require('node:fs');
const argv = process.argv.slice(2), opt = k => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : ''; };
const load = list => String(list || '').split(',').filter(Boolean).map(f => JSON.parse(fs.readFileSync(f, 'utf8')));
const sum = (rs, f) => rs.reduce((n, r) => n + (+f(r) || 0), 0);
const wmean = (rs, f, w) => { let a = 0, b = 0; for (const r of rs) { const v = f(r), k = w(r); if (v != null && k > 0) { a += v * k; b += k; } } return b ? a / b : null; };
const pct = x => x == null ? 'n/a' : (100 * x).toFixed(1) + '%';
const num = (x, d = 1) => x == null ? 'n/a' : (+x).toFixed(d);

function digest(rs) {
  const pieces = sum(rs, r => r.pieces), orders = sum(rs, r => r.orders);
  return {
    seeds: rs.map(r => r.seed).join(','), commit: [...new Set(rs.map(r => r.commit))].join(','), orders, pieces, multi: sum(rs, r => r.ordersMultiPiece), cross: sum(rs, r => r.ordersCrossMetal),
    split: sum(rs, r => r.split.sameMetal), pairsSplit: sum(rs, r => r.pairsSplit),
    sheets: sum(rs, r => r.sheets.total), completed: sum(rs, r => r.sheets.completed), gfss: sum(rs, r => r.sheets.gfss), gfssCompleted: sum(rs, r => r.sheets.gfssCompleted),
    densityClosed: wmean(rs, r => r.density.gfssClosed, r => r.sheets.gfssCompleted), densityAll: wmean(rs, r => r.density.gfssAll, r => r.sheets.gfss),
    byMetal: Object.fromEntries(['gold', 'silver', 'rose', 'gold14k', 'gold10k'].map(m => [m, { closed: wmean(rs, r => r.density.byMetal[m] && r.density.byMetal[m].densityClosed, r => r.density.byMetal[m] ? r.density.byMetal[m].completed : 0), n: sum(rs, r => r.density.byMetal[m] ? r.density.byMetal[m].completed : 0) }])),
    frontStd: wmean(rs, r => r.front.frontStd, r => r.sheets.gfssCompleted), cavern: wmean(rs, r => r.front.cavernMm, r => r.sheets.gfssCompleted),
    sets: sum(rs, r => r.sets.formed), invalid: sum(rs, r => r.sets.invalid), partialIn: sum(rs, r => r.sets.partialGfssInSets), everPartialIn: sum(rs, r => r.sets.everPartialGfssInSets), inSets: sum(rs, r => r.sets.sheetsInSets), waitingOutside: sum(rs, r => r.sets.completedWaitingOutside),
    queued: sum(rs, r => r.queue.ordersQueued), passed: sum(rs, r => r.queue.passedOrders || 0), maxHops: Math.max(0, ...rs.map(r => r.queue.maxHops || 0)), meanWait: wmean(rs, r => r.queue.meanWaitTicks, r => r.orders), maxWait: Math.max(0, ...rs.map(r => r.queue.maxWaitTicks)), neverPlaced: sum(rs, r => r.queue.neverPlaced),
    step: rs.map(r => r.step).join(','), wallS: sum(rs, r => r.wallS),
  };
}
function table(b, a) {
  const row = (name, f, fmt = x => String(x)) => `| ${name} | ${b ? fmt(f(b)) : ''} | ${a ? fmt(f(a)) : ''} |`;
  const of = (n, d) => d ? `${n} (${(100 * n / d).toFixed(1)}%)` : String(n);
  return [
    '| measure (fake orders only) | before | after |', '|---|---|---|',
    row('seeds', d => d.seeds), row('commit', d => d.commit), row('orders / pieces', d => `${d.orders} / ${d.pieces}`), row('multi-piece orders; orders with lines of two metals', d => `${d.multi}; ${d.cross}`),
    row('orders split across sheets of the same metal', d => of(d.split, d.orders)), row('earring / huggie pairs with the two pieces on two sheets', d => d.pairsSplit),
    row('sheets / completed', d => `${d.sheets} / ${d.completed}`), row('GF + SS sheets / completed', d => `${d.gfss} / ${d.gfssCompleted}`),
    row('whole-sheet density, completed GF + SS (mean)', d => pct(d.densityClosed)), row('whole-sheet density, all GF + SS sheets incl. still filling', d => pct(d.densityAll)),
    row('density by metal, completed (GF / SS / RG / 14K / 10K)', d => ['gold', 'silver', 'rose', 'gold14k', 'gold10k'].map(m => d.byMetal[m].n ? pct(d.byMetal[m].closed) : '-').join(' / ')),
    row('front raggedness, completed GF + SS (mm, 0 = straight)', d => num(d.frontStd, 2)), row('widest empty gap behind the front (mm)', d => num(d.cavern, 1)),
    row('sets formed', d => d.sets), row('sets without a completed GF and a completed SS', d => d.invalid), row('partial GF/SS sheets inside a set (at the end)', d => d.partialIn), row('partial GF/SS sheets that were in a set at any update', d => d.everPartialIn),
    row('sheets inside sets / completed GF+SS waiting outside any set', d => `${d.inSets} / ${d.waitingOutside}`),
    row('orders passed on whole to a later sheet (distinct) / most hops of one order', d => `${d.passed} / ${d.maxHops}`), row('orders queued (placed after the update they arrived in)', d => d.queued), row('mean wait of all orders / longest wait (updates of 10 min)', d => `${num(d.meanWait, 2)} / ${d.maxWait}`), row('orders never placed by the end', d => d.neverPlaced),
    row('angle step (degrees); wall seconds', d => `${d.step}; ${d.wallS}`),
  ].join('\n');
}
if (require.main === module) {
  const b = load(opt('before')), a = load(opt('after')), out = table(b.length ? digest(b) : null, a.length ? digest(a) : null);
  console.log(out); if (opt('md')) fs.writeFileSync(opt('md'), out + '\n');
}
module.exports = { digest, table };
