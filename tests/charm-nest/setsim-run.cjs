/* SETSIM runner: a seeded synthetic order stream through the sorter's own nesting and set rules, measured (see setsim-lib.cjs for what is real and what is stand-in).
 *   node tests/charm-nest/setsim-run.cjs [--root DIR] [--label NAME] [--orders N] [--seed N] [--step DEG] [--batches 2,3,5,4] [--metals gold,silver,rose,gold14k,gold10k] [--late 0.3] [--sheet 60x40] [--out FILE.json] [--quiet]
 * `--root` is a checkout of the code to judge (default: this repository); run it once on the commit before a change and once after, with the same seed, then
 *   node tests/charm-nest/setsim-report.cjs before.json after.json
 * Every simulated 10 minutes brings 2-5 orders (the sorter's trickle); each metal's sheets are fed as the page feeds them; after each update the page's own set assembly
 * (Gate.assemble) runs over the sheets. All orders are made up here (no real order, set or buyer). Offline: no network, no Firestore, no Etsy, no paid call.            */
'use strict';
const fs = require('node:fs'), path = require('node:path');
const Lib = require('./setsim-lib.cjs');
const { METALS, CODE, makeStream, partOf, piecesOf, schedule, Sim, setWorld, fillOf, frontOf, placementOf, mean, r1, L } = Lib;

const argv = process.argv.slice(2), opt = (k, d) => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : d; };

async function run(o = {}) {
  const root = path.resolve(o.root || REPO_DEFAULT), orders = +o.orders || 240, seed = +o.seed || 1, step = +o.step || 24, label = o.label || path.basename(root);
  const batches = String(o.batches || '2,3,5,4').split(',').map(Number), metals = String(o.metals || METALS.join(',')).split(',').filter(m => METALS.includes(m));
  const sim = new Sim({ root, step, seed, sheet: o.sheet || '60x40', job: o.job ? JSON.parse(o.job) : undefined, extra: o.extra ? String(o.extra).split(',') : [] }), S = L.solverOf(root);
  const stream = makeStream(S, { seed, orders, mix: Object.assign({}, o.mix, o.late ? { late: +o.late } : {}) }), world = setWorld(root);
  const log = o.quiet ? () => {} : (...a) => console.log(...a);
  const byId = new Map(stream.map(x => [x.id, x]));
  const arrival = new Map(), lastArrival = new Map(), done = new Map(), firstSeen = new Map(), t00 = Date.now(), ticks = [];
  const everPartialInSet = new Set(), everInSet = new Set();
  let tick = 0;
  const mine = o => piecesOf(o).filter(c => metals.includes(c.metal));   // (an order's pieces of the metals this run nests; an order of 10K or 14K only is not on any sheet here)
  const placedIds = () => new Set(sim.pages.flatMap(p => p.placements.map(q => q.id)));
  const plan = schedule(stream, batches, +o.drain || 0);
  const tickOnce = async ev => {
    tick = ev.tick; const t0 = Date.now();
    for (const x of ev.orders) arrival.set(x.id, tick);
    for (const a of ev.arrivals) lastArrival.set(a.order.id, tick);   // (the update the order's LAST line came in)
    for (const m of metals) { const parts = ev.arrivals.map(a => partOf(a.order, m, a.lines)).filter(Boolean); if (parts.length || (sim.metals[m] && sim.metals[m].work.length)) await sim.metal(m).step(parts, tick); }
    // where every order's pieces are now: an order is placed when each piece is on a sheet
    const placed = placedIds();
    for (const [id, at] of arrival) if (!done.has(id) && mine(byId.get(id)).length && mine(byId.get(id)).every(c => placed.has(c.id))) done.set(id, tick);
    world.sync(sim.pages); await world.assemble();
    const sets = world.sets(), pg = world.gatePages();
    const inSet = pg.filter(p => !p.draft && p.setId);
    const partial = inSet.filter(p => ['gold', 'silver'].includes(p.metal) && !world.SR.isCompleted(p));
    inSet.forEach(p => everInSet.add(p.sheetId)); partial.forEach(p => everPartialInSet.add(p.sheetId));
    ticks.push({ tick, orders: ev.orders.length, sheets: sim.pages.filter(p => p.placements.length).length, sets: sets.length, partialInSets: partial.length, ms: Date.now() - t0 });
    log(`t${String(tick).padStart(3)} +${ev.orders.length} sheets ${ticks.at(-1).sheets} (done ${[...sim.pages].filter(p => p.releaseFull).length}) sets ${sets.length} partial-in-sets ${partial.length} unplaced-orders ${[...arrival.keys()].filter(id => mine(byId.get(id)).length && !done.has(id)).length} ${Math.round((Date.now() - t0) / 1000)}s`);
  };
  for (const ev of plan) await tickOnce(ev);

  /* ── the measures ── */
  const pl = placementOf(sim, stream, metals), pg = world.gatePages(), sets = world.sets();
  const nested = stream.filter(x => mine(x).length), waits = nested.filter(x => done.has(x.id)).map(x => done.get(x.id) - arrival.get(x.id));
  const queued = nested.filter(x => done.has(x.id) && done.get(x.id) > arrival.get(x.id)), neverDone = nested.filter(x => arrival.has(x.id) && !done.has(x.id));
  const sheetRows = sim.pages.filter(p => p.placements.length).map(p => {
    const full = fillOf(p), closed = !!p.releaseFull, f = ['gold', 'silver', 'rose'].includes(p.metal) && p.placements.length ? frontOf(S, sim.metal(p.metal).stock, p) : null;
    return { metal: p.metal, label: `${CODE[p.metal]} ${p.page}`, page: p.page, placed: p.placements.length, orders: new Set(p.placements.map(q => (p.charms.find(c => c.id === q.id) || {}).order)).size, fill: r1(full, 3), closed, ...(f || {}) };
  });
  const byMetal = {};
  for (const m of metals) {
    const rows = sheetRows.filter(r => r.metal === m), closed = rows.filter(r => r.closed);
    byMetal[m] = { sheets: rows.length, completed: closed.length, densityClosed: r1(mean(closed.map(r => r.fill)), 3), densityAll: r1(mean(rows.map(r => r.fill)), 3), minClosed: closed.length ? Math.min(...closed.map(r => r.fill)) : null,
      frontStd: r1(mean(closed.filter(r => r.frontStd != null).map(r => r.frontStd)), 2), cavernMm: r1(mean(closed.filter(r => r.cavernMm != null).map(r => r.cavernMm)), 1), maxCavernMm: closed.filter(r => r.cavernMm != null).length ? Math.max(...closed.map(r => r.cavernMm || 0)) : null };
  }
  const gs = sheetRows.filter(r => ['gold', 'silver'].includes(r.metal)), gsClosed = gs.filter(r => r.closed);
  const setRows = sets.map(s => ({ name: s.name, sheets: s.ids.map(id => { const p = pg.find(x => x.sheetId === id); return { id, label: p ? `${CODE[p.metal]} ${p.page}` : id, completed: !!p && world.SR.isCompleted(p), fill: p ? r1(p.density, 2) : null, metal: p && p.metal }; }) }));
  const setValid = setRows.map(s => world.SR.validSet(s.ids ? [] : []));   // (placeholder: replaced below)
  const inSetPages = pg.filter(p => !p.draft && p.setId);
  const partialIn = inSetPages.filter(p => ['gold', 'silver'].includes(p.metal) && !world.SR.isCompleted(p));
  const out = {
    label, root: root === path.resolve(Lib.REPO) ? 'repo' : root, commit: gitHead(root), seed, orders, step, batches: batches.join(','), metals, ticks: tick, wallS: Math.round((Date.now() - t00) / 1000),
    ordersAll: stream.length, orders: stream.filter(x => mine(x).length).length, pieces: stream.reduce((n, x) => n + mine(x).length, 0), piecesAll: stream.reduce((n, x) => n + piecesOf(x).length, 0), nested: 'metals counted: ' + metals.join(','), ordersMultiPiece: pl.ordersMultiPiece, ordersCrossMetal: pl.spreadMetals, pairsSplit: pl.pairsSplit,
    split: { sameMetal: pl.splitSameMetal.length, list: pl.splitSameMetal.slice(0, 40) },
    sheets: { total: sheetRows.length, completed: sheetRows.filter(r => r.closed).length, gfss: gs.length, gfssCompleted: gsClosed.length },
    density: { gfssClosed: r1(mean(gsClosed.map(r => r.fill)), 3), gfssAll: r1(mean(gs.map(r => r.fill)), 3), byMetal },
    front: { frontStd: r1(mean(gsClosed.filter(r => r.frontStd != null).map(r => r.frontStd)), 2), cavernMm: r1(mean(gsClosed.filter(r => r.cavernMm != null).map(r => r.cavernMm)), 1) },
    sets: { formed: sets.length, partialGfssInSets: partialIn.length, everPartialGfssInSets: everPartialInSet.size, sheetsInSets: inSetPages.length, everInSets: everInSet.size, completedWaitingOutside: pg.filter(p => p.draft && ['gold', 'silver'].includes(p.metal) && world.SR.isCompleted(p)).length,
      invalid: setRows.filter(s => { const v = world.SR.validSet(s.sheets.map(x => pg.find(p => p.sheetId === x.id)).filter(Boolean)); return !v.ok; }).length, list: setRows },
    queue: { passedOrders: new Set(sim.ctl.passes.flatMap(x => x.orders)).size, passes: sim.ctl.passes.length, maxHops: Math.max(0, ...[...sim.ctl.passes.flatMap(x => x.orders).reduce((m, id) => m.set(id, (m.get(id) || 0) + 1), new Map()).values()]), ordersQueued: queued.length, meanWaitTicks: r1(mean(waits), 2), maxWaitTicks: waits.length ? Math.max(...waits) : 0, meanWaitFromLastLine: r1(mean(nested.filter(x => done.has(x.id)).map(x => done.get(x.id) - (lastArrival.get(x.id) || arrival.get(x.id)))), 2), maxWaitFromLastLine: Math.max(0, ...nested.filter(x => done.has(x.id)).map(x => done.get(x.id) - (lastArrival.get(x.id) || arrival.get(x.id)))), queuedFromLastLine: nested.filter(x => done.has(x.id) && done.get(x.id) > (lastArrival.get(x.id) || arrival.get(x.id))).length, neverPlaced: neverDone.length, neverPlacedList: neverDone.slice(0, 20).map(x => ({ order: x.id, pieces: piecesOf(x).length, arrivedTick: arrival.get(x.id) })), unplacedAtEnd: pl.unplaced.length },
    searches: Object.fromEntries(metals.map(m => [m, sim.metals[m] ? { n: sim.metals[m].searches.length, ms: sim.metals[m].ms } : null])),
    sheetRows,
  };
  void setValid;
  return out;
}
const REPO_DEFAULT = Lib.REPO;
function gitHead(root) { try { return require('node:child_process').execSync('git -C ' + JSON.stringify(root) + ' rev-parse --short HEAD', { encoding: 'utf8' }).trim(); } catch (_) { return ''; } }

module.exports = { run };
if (require.main === module) {
  run({ root: opt('root'), label: opt('label'), orders: opt('orders'), seed: opt('seed'), step: opt('step'), sheet: opt('sheet'), late: opt('late'), batches: opt('batches'), metals: opt('metals'), drain: opt('drain'), extra: opt('extra'), job: opt('job'), quiet: argv.includes('--quiet') }).then(r => {
    const f = opt('out'); if (f) fs.writeFileSync(f, JSON.stringify(r, null, 1));
    const { sheetRows, split, sets, ...head } = r; console.log(JSON.stringify({ ...head, split: { sameMetal: split.sameMetal }, sets: { ...sets, list: undefined } }, null, 1));
  }).catch(e => { console.error(e); process.exit(1); });
}
