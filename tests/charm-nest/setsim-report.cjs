/* SETSIM report: the before/after table of two sets of setsim-run.cjs results (same seeds on both sides).
 *   node tests/charm-nest/setsim-report.cjs --before a.json,b.json --after c.json,d.json [--md out.md]
 * Counts are summed over the seeds, densities and fronts are averaged over the seeds' sheets (weighted by sheet count).                                           */
'use strict';
const fs = require('node:fs');
const argv = process.argv.slice(2), opt = k => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : ''; };
/** A result written before the runner counted only the metals it nests (no `ordersAll`) counted orders of 10K / 14K, which no sheet of such a run takes, as orders never placed:
    the stream is made again from its seed (it is deterministic) and those orders are left out. An order with a line of a nested metal AND a line of an un-nested one is kept in the
    never-placed list (its placement cannot be told from the file): the report marks it. */
function fixOld(r, late) {
  if (r.ordersAll != null) return r;
  const Lib = require('./setsim-lib.cjs'), S = Lib.L.solverOf(Lib.REPO), st = Lib.makeStream(S, { seed: r.seed, orders: r.orders, mix: { late } }), by = new Map(st.map(o => [o.id, o]));
  const mine = o => Lib.piecesOf(o).filter(c => r.metals.includes(c.metal)), out = JSON.parse(JSON.stringify(r));
  out.ordersAll = r.orders; out.orders = st.filter(o => mine(o).length).length; out.pieces = st.reduce((n, o) => n + mine(o).length, 0); out.ordersMultiPiece = st.filter(o => mine(o).length > 1).length;
  const listed = (r.queue.neverPlacedList || []).filter(x => by.get(x.order) && mine(by.get(x.order)).length);
  out.queue.neverPlaced = listed.length; out.queue.neverPlacedList = listed; out.queue.mixedUnclear = listed.filter(x => Lib.piecesOf(by.get(x.order)).some(c => !r.metals.includes(c.metal))).length;
  return out;
}
const load = list => String(list || '').split(',').filter(Boolean).map(f => fixOld(JSON.parse(fs.readFileSync(f, 'utf8')), +(opt('late') || 0.3)));
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
    queued: sum(rs, r => r.queue.ordersQueued), queuedLast: rs.every(r => r.queue.queuedFromLastLine != null) ? sum(rs, r => r.queue.queuedFromLastLine) : null, meanWaitLast: rs.every(r => r.queue.meanWaitFromLastLine != null) ? wmean(rs, r => r.queue.meanWaitFromLastLine, r => r.orders) : null, passed: sum(rs, r => r.queue.passedOrders || 0), maxHops: Math.max(0, ...rs.map(r => r.queue.maxHops || 0)), meanWait: wmean(rs, r => r.queue.meanWaitTicks, r => r.orders), maxWait: Math.max(0, ...rs.map(r => r.queue.maxWaitTicks)), neverPlaced: sum(rs, r => r.queue.neverPlaced), mixedUnclear: sum(rs, r => r.queue.mixedUnclear || 0),
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
    row('orders passed on whole to a later sheet (distinct) / most hops of one order', d => `${d.passed} / ${d.maxHops}`), row('orders queued (placed after the update they arrived in)', d => d.queued), row('orders held back after their last line came in (queued) / mean wait from the last line (new runs only)', d => d.queuedLast == null ? 'n/a' : `${d.queuedLast} / ${num(d.meanWaitLast, 2)}`), row('mean wait of all orders / longest wait, counted from the first line (updates of 10 min)', d => `${num(d.meanWait, 2)} / ${d.maxWait}`), row('orders never placed by the end (nested metals only)', d => d.neverPlaced + (d.mixedUnclear ? ` (${d.mixedUnclear} of them also has a line of a metal this run does not nest)` : '')),
    row('angle step (degrees); wall seconds', d => `${d.step}; ${d.wallS}`),
  ].join('\n');
}
/** The sets side (setsim-sets.cjs --json): each file is the array of per-seed results. */
function setsDigest(files) {
  const rs = files.flatMap(f => JSON.parse(fs.readFileSync(f, 'utf8')));
  const S = f => sum(rs, f), n = rs.length, rules = {};
  for (const r of rs) for (const x of r.failRules || []) rules[x] = (rules[x] || 0) + 1;
  const lim = {}; for (const r of rs) for (const x of r.limitRules || []) lim[x] = (lim[x] || 0) + 1;
  return { runs: n, orders: S(r => r.orders), sheets: S(r => r.gfss), sets: S(r => r.sets), partialEver: S(r => r.partialInSetsEver), partialPeak: Math.max(0, ...rs.map(r => r.maxPartialInSets)),
    brokenRuns: rs.filter(r => (r.failRules || []).length).length, rules: Object.keys(rules).map(k => k + ' x' + rules[k]).join(', ') || 'none', limits: Object.keys(lim).map(k => k + ' x' + lim[k]).join(', ') || 'none',
    left: S(r => r.leftOutside), held: S(r => r.blockedSheets), heldUpdates: S(r => r.blockedSheetUpdates), waitMean: wmean(rs, r => r.completedWaitMean, r => r.gfss), waitMax: Math.max(0, ...rs.map(r => r.completedWaitMax)), forced: S(r => r.forcedSplits || 0), lifted: S(r => r.liftedOrders || 0) };
}
function setsTable(b, a) {
  const row = (name, f, fmt = x => String(x)) => `| ${name} | ${b ? fmt(f(b)) : ''} | ${a ? fmt(f(a)) : ''} |`;
  return ['| sets side (page Gate.assemble over a toy sheet fill; fake orders only) | before | after |', '|---|---|---|',
    row('seeds run; orders in all', d => `${d.runs}; ${d.orders}`), row('GF + SS sheets made', d => d.sheets), row('sets formed', d => d.sets),
    row('partial GF/SS sheets that were inside a set at some update (all seeds); most at one update', d => `${d.partialEver}; ${d.partialPeak}`),
    row('seeds with a broken rule (S1 partial in a set, S2 set without GF+SS, S3, S4, S5, S6, S7)', d => `${d.brokenRuns} of ${d.runs}: ${d.rules}`),
    row('consequences of a split by force, measured (S6* sets tied together, S7* sheets stuck at the end)', d => d.limits),
    row('completed GF/SS sheets outside every set at the end of the stream', d => d.left), row('completed sheets held out because a sheet sharing an order was still filling / sheet-updates', d => `${d.held} / ${d.heldUpdates}`),
    row('wait of a completed sheet before it joined a set (updates of 10 min): mean / longest', d => `${num(d.waitMean, 2)} / ${d.waitMax}`), row('forced splits (a later line, its mates\' sheet already released) / orders moved whole to the next sheet', d => `${d.forced} / ${d.lifted}`)].join('\n');
}
if (require.main === module) {
  const b = load(opt('before')), a = load(opt('after'));
  let out = (b.length || a.length) ? table(b.length ? digest(b) : null, a.length ? digest(a) : null) : '';
  const sb = String(opt('sets-before') || '').split(',').filter(Boolean), sa = String(opt('sets-after') || '').split(',').filter(Boolean);
  if (sb.length || sa.length) out += (out ? '\n\n' : '') + setsTable(sb.length ? setsDigest(sb) : null, sa.length ? setsDigest(sa) : null);
  console.log(out); if (opt('md')) fs.writeFileSync(opt('md'), out + '\n');
}
module.exports = { digest, table, setsDigest, setsTable };
