/* SETSIM, whole days (SETRULE, Paul, 15 Oct 2026): how many sets does a production-sized day of FAKE orders make, and what is in them.
 *
 * Paul, 15 Oct 2026: "how is it that this one set has 4 GF and 2 SS sheets. This should be literally impossible ... orders with multiple pieces or earrings with left and right
 * charms should stay on a given sheet, so that sheets of the same colour do not create an endless chain of connections".
 *
 *   node tests/charm-nest/setsim-days.cjs [--root DIR] [--seed 1,2,3] [--orders 400] [--scale 0.27] [--mixed 0.10] [--mode stream,end,lag] [--late 0] [--json FILE]
 *
 * The same toy sheets and the page's own set assembly (Gate.assemble of `root`) as setsim-sets.cjs; run it once with `--root` a checkout of the code BEFORE a change and once after.
 *   stream   the assembly looks after every update (a day in the shop: sheets complete one by one)
 *   lag      the assembly looks every 6th update only (a slow cloud, a busy page: several sheets completed since the last look)
 *   end      the assembly looks once, after every sheet is completed (the run's end: settleTopups marks every sheet released at once; or a page reloaded at the end of the day)
 * Measured at the end of the day: the sheets, the groups of sheets that share an order (the cardinal rule: ONE unit), the sets and what each holds, and the sets that are not minimal
 * (a set from which a whole unit could leave and still hold a completed GF and a completed SS sheet: Paul's "as soon as the minimum criteria is met ... the set should be formed").
 * `scale` shrinks every charm so that a toy sheet holds about 45 orders, as the sheets of Paul's day did (49, 37, 50, 38, 48, 51). All orders are made up. Offline, deterministic.   */
'use strict';
const path = require('node:path');
const Lib = require('./setsim-lib.cjs'), { toySheets } = require('./setsim-sets.cjs');
const { METALS, CODE, DEFAULT_MIX, makeStream, partOf, schedule, setWorld, L } = Lib;
const argv = process.argv.slice(2), opt = (k, d) => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : d; };

/** A day as Paul describes it: mostly single charms, 10-20 % of the orders with several pieces (earring pairs Left + Right, mismatched pairs, discs, quantity lines), some with a line of the other metal. */
const dayMix = (mixed, late) => ({
  lines: [.90, .08, .02],
  kinds: { single: .90, pair: .06, mismatched: .005, discs: .005, qty: .015, huggie: .01, trio: .005 },
  size: DEFAULT_MIX.size, metals: { gold: .50, silver: .38, rose: .06, gold14k: .03, gold10k: .03 }, otherMetal: mixed, late: late || 0,
});

const classOf = p => ({ gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' })[p.metal] || '?';

async function oneDay({ root, seed, orders, scale, mixed, mode, late }) {
  const S = L.solverOf(root), stream = makeStream(S, { seed, orders, mix: dayMix(mixed, late) }), toy = toySheets(scale), world = setWorld(root), SR = world.SR, r = L.rng(seed * 31 + 5);
  const metalsOf = o => new Set(o.lines.map(l => l.metal));
  const mixedOrders = stream.filter(o => metalsOf(o).has('gold') && metalsOf(o).has('silver')).length;
  const multiPiece = stream.filter(o => METALS.some(m => o.lines.filter(l => l.metal === m).reduce((n, l) => n + l.pieces.length, 0) > 1)).length;
  const lookAt = tick => mode === 'stream' || (mode === 'lag' && tick % 6 === 0);
  let tick = 0;
  for (const ev of schedule(stream, [2, 3, 5, 4])) {
    tick = ev.tick;
    for (const a of ev.arrivals) for (const m of METALS) { const part = partOf(a.order, m, a.lines); if (part) toy.add(part, tick, 0, r, true); }
    if (lookAt(tick)) { world.sync(toy.asNest()); await world.assemble(); }
  }
  // the end: every sheet is completed at once (what the stream's end does)
  for (const s of toy.all()) if (!s.releaseFull) { s.releaseFull = true; s.releasedAt = ++tick; }
  world.sync(toy.asNest()); await world.assemble();
  const pg = world.gatePages(), sets = world.sets(), byId = new Map(pg.map(p => [p.sheetId, p]));
  const core = pg.map(p => world.SO.fromPage(p)).filter(Boolean), groups = world.SO.core.groups(core), inGroup = new Set(groups.flatMap(g => g.ids));
  const comps = groups.map(g => g.ids.map(id => byId.get(id))).concat(pg.filter(p => !inGroup.has(p.sheetId)).map(p => [p]));
  const tally = list => { const t = { GF: 0, SS: 0, other: 0 }; for (const p of list) { const c = classOf(p); if (c === 'GF') t.GF++; else if (c === 'SS') t.SS++; else t.other++; } return t; };
  const done = p => SR.isCompleted(p);
  const bigGroup = groups.slice().sort((a, b) => b.ids.length - a.ids.length)[0];
  // the sets: what each holds; a set is NOT minimal when a whole unit (the sheets of it that share orders) could leave and a completed GF + a completed SS stay
  const setRows = sets.map(s => {
    const members = s.ids.map(id => byId.get(id)).filter(Boolean), t = tally(members), gs = new Map(); let n = 0;
    const idsIn = new Set(members.map(p => p.sheetId)), units = [];
    for (const g of groups) { const inn = g.ids.filter(id => idsIn.has(id)); if (inn.length) { units.push(inn); inn.forEach(id => gs.set(id, 1)); } }
    for (const p of members) if (!gs.has(p.sheetId)) units.push([p.sheetId]);
    const removable = units.filter(u => u.some(id => ['GF', 'SS'].includes(classOf(byId.get(id)))) && SR.validSet(members.filter(p => !u.includes(p.sheetId))).ok);   // (a unit of GF/SS sheets: 10K, 14K and Rose Gold sheets are added by hand and never count)
    return { name: s.name, sheets: members.length, ...t, units: units.length, removableUnits: removable.length, minimal: !removable.length, valid: SR.validSet(members).ok };
  });
  const gfOnly = comps.filter(c => tally(c.filter(done)).GF && !tally(c.filter(done)).SS).length, ssOnly = comps.filter(c => tally(c.filter(done)).SS && !tally(c.filter(done)).GF).length, both = comps.filter(c => { const t = tally(c.filter(done)); return t.GF && t.SS; }).length;
  const t = tally(pg);
  return { seed, mode, orders, mixedGfSsOrders: mixedOrders, multiPieceOrders: multiPiece, sheets: pg.length, GF: t.GF, SS: t.SS, other: t.other, ordersPerGfSsSheet: +(orders * .88 / Math.max(1, t.GF + t.SS)).toFixed(1),
    forcedSplits: toy.forced, groups: groups.length, components: comps.length, largestGroup: bigGroup ? { sheets: bigGroup.ids.length, ...tally(bigGroup.ids.map(id => byId.get(id))), orders: bigGroup.orders.length } : null,
    sets: sets.length, idealSets: both + Math.min(gfOnly, ssOnly), setsNotMinimal: setRows.filter(s => s.valid && !s.minimal).length, largestSet: setRows.reduce((m, s) => Math.max(m, s.sheets), 0),
    setsWithManyGF: setRows.filter(s => s.GF > 1).length, setsWithManySS: setRows.filter(s => s.SS > 1).length, setsInvalid: setRows.filter(s => !s.valid).length,
    outsideCompleted: pg.filter(p => !(!p.draft && p.setId) && ['gold', 'silver'].includes(p.metal) && done(p)).length, setRows };
}

async function main() {
  const root = path.resolve(opt('root') || Lib.REPO), seeds = String(opt('seed', '1,2,3')).split(',').map(Number), modes = String(opt('mode', 'stream,end')).split(','), orders = +opt('orders', 400), scale = +opt('scale', .27), mixed = +opt('mixed', .10), late = +opt('late', 0), out = [];
  for (const mode of modes) for (const seed of seeds) out.push(await oneDay({ root, seed, orders, scale, mixed, mode, late }));
  for (const d of out) {
    const lg = d.largestGroup;
    console.log(`${d.mode.padEnd(6)} seed ${d.seed}: ${d.orders} orders (${d.multiPieceOrders} multi-piece, ${d.mixedGfSsOrders} GF+SS mixed) -> ${d.sheets} sheets (${d.GF} GF, ${d.SS} SS, ${d.other} other, ~${d.ordersPerGfSsSheet} orders/sheet) | shared-order groups ${d.groups}, components ${d.components}, largest ${lg ? `${lg.sheets} sheets (${lg.GF} GF + ${lg.SS} SS + ${lg.other} other, ${lg.orders} orders)` : 'none'} | sets ${d.sets} (ideal <= ${d.idealSets}), not minimal ${d.setsNotMinimal}, with >1 GF ${d.setsWithManyGF}, with >1 SS ${d.setsWithManySS}, largest ${d.largestSet} sheets, completed outside ${d.outsideCompleted}, invalid ${d.setsInvalid}`);
    if (argv.includes('--sets')) for (const s of d.setRows) console.log(`      ${s.name}: ${s.sheets} sheets = ${s.GF} GF + ${s.SS} SS + ${s.other} other, ${s.units} unit(s), ${s.minimal ? 'minimal' : `${s.removableUnits} unit(s) could leave`}`);
  }
  if (opt('json')) require('node:fs').writeFileSync(opt('json'), JSON.stringify(out, null, 1));
}
module.exports = { oneDay, dayMix };
if (require.main === module) main().catch(e => { console.error(e); process.exit(1); });
