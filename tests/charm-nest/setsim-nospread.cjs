/* NOSPREAD, the day (Paul, 17 Oct 2026): how many multi-piece and earring orders still end on two sheets of the same colour, how big the groups of sheets that share an order are (the units that
 * make a Set of Sheets of 14 sheets: the cardinal rule), how full the sheets get, and whether any piece is left unplaced. Fake orders only; offline; deterministic.
 *
 *   node tests/charm-nest/setsim-nospread.cjs [--root DIR] [--seed 1,2,3] [--orders 300] [--mix day|heavy] [--solver sim|real] [--per-sheet 45] [--late 0.1] [--mixed 0.08]
 *                                             [--big 1] [--big-pieces 120] [--leak 0] [--max-order 0.5] [--batches 2,3,5,4] [--off] [--wait-lines] [--margin 0.15] [--json FILE] [--label NAME] [--quiet] [--splits]
 * `--root` is a checkout of the code to judge (default: this repository). Run it once on the commit BEFORE the change (git worktree of the starting commit) and once after, with the same seeds:
 *   node tests/charm-nest/setsim-nospread.cjs --root /path/to/before --json before.json ; node tests/charm-nest/setsim-nospread.cjs --json after.json
 *   node tests/charm-nest/setsim-nospread.cjs --compare before.json after.json
 * `--late` is the share of the extra lines of an order that reach the sorter 1-3 updates after its first line (a line held for review, read late, released from hold).
 * `--mixed` is the share of extra lines that are of the other metal (a Gold + Silver order ties a GF sheet to an SS sheet; no placement of one colour can undo that).
 * `--big N` puts N orders of `--big-pieces` pieces of one design (about 1.3 sheets of area per 100 pieces at the default sheet) into the day: each is bigger than a sheet and must fill consecutive sheets.
 * `--wait-lines` is not what the page does: it holds every order back until its last line has come, to show what waiting for late lines would buy and cost.
 * `--solver real` runs the shop's solver (hours for a day; 60 x 40 mm sheets, 60 degree steps); the default stand-in decides only what fits, every decision about where an order goes is the page's own code.                                                                                           */
'use strict';
const fs = require('node:fs');
const Lib = require('./setsim-nospread-lib.cjs');
const argv = process.argv.slice(2), opt = (k, d) => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : d; }, flag = k => argv.includes('--' + k);

const pct = x => x == null ? 'n/a' : (100 * x).toFixed(1) + '%';
function line(r) {
  const a = r.all, s = r.same;
  return [`${r.solver} ${r.mix} seed ${r.seed}: ${r.orders} orders, ${r.pieces} pieces, ${r.partsMulti} multi-piece (per colour), ${r.sheets} sheets (${r.closed} released)`,
    `  SPLIT: ${r.multiOrdersSplit} multi-piece orders on 2+ sheets of one colour (${r.multiPct}%${r.multiOrdersSplit !== r.multiOrdersSplitNoBig ? `; ${r.multiOrdersSplitNoBig} not counting orders bigger than a sheet` : ''}), earring/huggie pairs with the 2 pieces on 2 sheets: ${r.pairLinesSplit} of ${r.pairLines}`,
    `  UNITS (sheets that share an order, any colour): ${a.linked} linked groups, mean ${a.meanLinked} sheets, max ${a.max}, mean unit of a sheet ${a.perSheetMean}; same-colour splits only: ${s.linked} groups, mean ${s.meanLinked}, max ${s.max}, per sheet ${s.perSheetMean}`,
    ...(r.held ? [`  WAIT-LINES (not what the page does): ${r.held.orders} orders held until their last line, ${r.held.meanUpdates} updates on average, ${r.held.maxUpdates} at most`] : []),
    ...(r.setsEnd && !r.setsEnd.error ? [`  SETS (every sheet completed at the end of the day): ${r.setsEnd.formed} sets, largest ${r.setsEnd.largest} sheets, sizes ${r.setsEnd.sizes.join(',')}; ${r.setsEnd.oversized} sets of more than 2 sheets`] : []),
    `  FILL: released sheets ${pct(r.fillClosed)} (GF+SS ${pct(r.fillClosedGfss)}, ${pct(r.over75)} of them at 75% or more), all sheets ${pct(r.fillAll)}; ${r.pieces - r.unplaced} of ${r.pieces} pieces placed, UNPLACED ${r.unplaced}${r.problems && r.problems.length ? `, PROBLEMS ${r.problems.length}: ${r.problems.slice(0, 2).map(p => p.problem).join(' | ')}` : ''}`,
    ...(r.big.length ? [`  BIG: ${r.big.map(b => `order ${b.order} ${b.pieces} pieces (${b.share} sheets of area) on ${b.sheets} sheets, fewest possible ${b.fewest}${b.miss ? ` (${b.miss} UNPLACED)` : ''}`).join('; ')}`] : [])].join('\n');
}
function sum(rs, f) { return rs.reduce((n, r) => n + (+f(r) || 0), 0); }
function wm(rs, f, w) { let a = 0, b = 0; for (const r of rs) { const v = f(r), k = w(r); if (v != null && k > 0) { a += v * k; b += k; } } return b ? a / b : null; }
function digest(rs) {
  return { seeds: rs.map(r => r.seed).join(','), orders: sum(rs, r => r.orders), pieces: sum(rs, r => r.pieces), multi: sum(rs, r => r.partsMulti), split: sum(rs, r => r.multiOrdersSplit), splitNoBig: sum(rs, r => r.multiOrdersSplitNoBig),
    pairLines: sum(rs, r => r.pairLines), pairsSplit: sum(rs, r => r.pairLinesSplit), sheets: sum(rs, r => r.sheets), closed: sum(rs, r => r.closed),
    unitAllMean: wm(rs, r => r.all.meanLinked, r => r.all.linked), unitAllMax: Math.max(...rs.map(r => r.all.max)), unitAllPerSheet: wm(rs, r => r.all.perSheetMean, r => r.sheets), groupsAll: sum(rs, r => r.all.linked),
    unitSameMean: wm(rs, r => r.same.meanLinked, r => r.same.linked), unitSameMax: Math.max(...rs.map(r => r.same.max)), unitSamePerSheet: wm(rs, r => r.same.perSheetMean, r => r.sheets), groupsSame: sum(rs, r => r.same.linked),
    setsFormed: sum(rs, r => r.setsEnd ? r.setsEnd.formed : 0), setsLargest: Math.max(0, ...rs.map(r => r.setsEnd ? r.setsEnd.largest : 0)), setsOversized: sum(rs, r => r.setsEnd ? r.setsEnd.oversized : 0), setsSheets: sum(rs, r => r.setsEnd ? r.setsEnd.sheetsInSets : 0),
    fillClosed: wm(rs, r => r.fillClosedGfss, r => r.closed), fillAll: wm(rs, r => r.fillAll, r => r.sheets), over75: wm(rs, r => r.over75, r => r.closed), unplaced: sum(rs, r => r.unplaced), problems: sum(rs, r => (r.problems || []).length) };
}
function compare(before, after) {
  const b = digest(before), a = digest(after), n = (x, d = 1) => x == null ? 'n/a' : (+x).toFixed(d), p = x => x == null ? 'n/a' : (100 * x).toFixed(1) + '%', of = (k, t) => t ? `${k} (${(100 * k / t).toFixed(1)}%)` : String(k);
  const row = (name, f) => `| ${name} | ${f(b)} | ${f(a)} |`;
  return ['| fake day, same seeds | before | after |', '|---|---|---|',
    row('seeds; orders; pieces', d => `${d.seeds}; ${d.orders}; ${d.pieces}`),
    row('multi-piece orders (per colour)', d => d.multi),
    row('... split across 2+ sheets of ONE colour', d => of(d.split, d.multi)),
    row('... of which not counting orders bigger than a sheet', d => of(d.splitNoBig, d.multi)),
    row('earring/huggie pairs on 2 sheets', d => of(d.pairsSplit, d.pairLines)),
    row('groups of sheets that share an order, any colour (count; mean sheets; max)', d => `${d.groupsAll}; ${n(d.unitAllMean, 2)}; ${d.unitAllMax}`),
    row('... mean size of the unit a sheet belongs to', d => n(d.unitAllPerSheet, 2)),
    row('groups made by splits of ONE colour only (count; mean; max)', d => `${d.groupsSame}; ${n(d.unitSameMean, 2)}; ${d.unitSameMax}`),
    row('... mean size of the unit a sheet belongs to (one colour)', d => n(d.unitSamePerSheet, 2)),
    row('sheets; released', d => `${d.sheets}; ${d.closed}`),
    row('sets formed when every sheet is completed; largest set (sheets); sets of more than 2 sheets', d => `${d.setsFormed}; ${d.setsLargest}; ${d.setsOversized}`),
    row('fill of released GF+SS sheets (mean); share at 75%+', d => `${p(d.fillClosed)}; ${p(d.over75)}`),
    row('fill of all sheets (mean)', d => p(d.fillAll)),
    row('pieces unplaced; sheets stuck on a problem', d => `${d.unplaced}; ${d.problems}`)].join('\n');
}

async function main() {
  if (flag('compare')) { const i = argv.indexOf('--compare'), [bf, af] = [argv[i + 1], argv[i + 2]], rd = f => JSON.parse(fs.readFileSync(f, 'utf8')); console.log(compare(rd(bf), rd(af))); return; }
  const seeds = String(opt('seed', '1,2,3')).split(',').map(Number), out = [], nb = +opt('big', 0), bp = +opt('big-pieces', 120);
  for (const seed of seeds) {
    const r = await Lib.runDay({ root: opt('root'), seed, orders: +opt('orders', 300), mix: opt('mix', 'day'), solver: opt('solver', 'sim'), perSheet: +opt('per-sheet', 45), late: +opt('late', 0.1), mixed: +opt('mixed', 0.08), leak: +opt('leak', 0), maxOrder: +opt('max-order', 0), waitLines: flag('wait-lines'), batches: String(opt('batches', '2,3,5,4')).split(',').map(Number),
      settings: Object.assign({}, flag('off') ? { nospread: 'off' } : {}, opt('margin') != null ? { nospreadMargin: +opt('margin') } : {}), big: Array.from({ length: nb }, (_, i) => ({ pieces: bp, metal: i % 2 ? 'silver' : 'gold', design: [8 + (i * 3) % 9, 1] })), step: +opt('step', 60), log: flag('quiet') || !flag('verbose') ? undefined : console.log });
    delete r.sim; out.push(r); console.log(line(r));
    if (flag('splits')) for (const s of r.splitList) console.log(`     split ${s.order} ${s.metal} ${s.n} pieces (${s.kinds}) on ${s.sheets}`);
  }
  console.log('\n' + compare(out, out).split('\n').map(l => l.replace(/\| ([^|]*) \| ([^|]*) \| [^|]*\|$/, '| $1 | $2 |')).join('\n'));
  if (opt('json')) fs.writeFileSync(opt('json'), JSON.stringify(out, null, 1));
}
module.exports = { digest, compare, line };
if (require.main === module) main().catch(e => { console.error(e); process.exit(1); });
