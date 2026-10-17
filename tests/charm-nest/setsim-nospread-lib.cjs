/* NOSPREAD simulation library (Paul, 17 Oct 2026): a day of FAKE orders through the sorter's OWN intake and nesting rules (the real functions of charm-nest-1.html and
 * charm-nest-bridge.js sliced by tests/charm-nest/setsim-lib.cjs and run in a vm), measured for how many multi-piece and earring orders end up on two sheets of the same colour,
 * how big the groups of sheets that share an order are (the units a Set of Sheets must hold whole: the cardinal rule), how full the sheets get, and whether any piece is left unplaced.
 *
 * Two solvers behind the same page code (`solver`):
 *   'sim'   a stand-in for the packer, so that a production-sized day (hundreds of orders, 40-50 orders to a sheet) runs in seconds on every seed: pieces of an order are seated
 *           whole-or-nothing, oldest first (singles first on a sheet that is filling its gaps), against the sheet's usable area and the fill ceiling, and the bigger a piece is
 *           the earlier the free space stops being one piece's pocket (POCKET below). It decides only WHAT FITS; every decision about WHERE an order goes is the page's own code.
 *   'real'  the shop's solver (charm-nest-solver.js, as setsim-run.cjs runs it): slow (a day takes an hour on a busy machine); used to confirm what the stand-in shows on a small day.
 * Offline and deterministic (seeded); no network, no Firestore, no Etsy, no paid call; every order is made up here.                                                              */
'use strict';
const path = require('node:path');
const Lib = require('./setsim-lib.cjs');
const { METALS, makeStream, orderOf, partOf, piecesOf, schedule, Sim, fillOf, mean, r1, L } = Lib;

/** The made-up day. `day` is Paul's description (mostly single charms, 10-20 % of the orders with several pieces); `heavy` is a stress day (about 45 % multi-piece). */
const MIXES = {
  day: { lines: [.82, .14, .04], kinds: { single: .80, pair: .10, mismatched: .005, discs: .025, qty: .025, huggie: .015, trio: .03 }, size: { small: .35, mid: .40, large: .25 }, metals: { gold: .55, silver: .45 } },
  pairs: { lines: [1, 0, 0], kinds: { pair: 1 }, size: { small: .35, mid: .40, large: .25 }, metals: { gold: 1 } },   // an earring day (or the last orders of a day): only multi-piece orders come to close the sheets
  heavy: { lines: [.60, .28, .12], kinds: { single: .40, pair: .28, mismatched: .04, discs: .08, qty: .08, huggie: .05, trio: .07 }, size: { small: .35, mid: .40, large: .25 }, metals: { gold: .55, silver: .45 } },
};

/** The stand-in packer. `cfg`: { smallPt2 (the area of one of the shop's smallest charms), leak (0..1: the share of searches that leave a half-seated order for the page to lift), maxOrder (a share of the ceiling: a poor packer that never seats a larger order) }. */
const POCKET = { from: .45, shrink: .64 };   // beyond 45 % full the largest pocket is a shrinking share of the room left: 100 % of it at 45 % full, 36 % of it at the fill ceiling
function standIn(real, cfg = {}) {
  const inflated = (p, clearance) => { const g = (+clearance || 0) / 2, w = p.w / (p.scale || 1), h = p.h / (p.scale || 1); return p.areaPt2 + g * 2 * (w + h) + Math.PI * g * g; };
  const allowed = (used, usable, cap) => { const room = Math.max(0, cap * usable - used), frag = Math.min(1, Math.max(0, (used / usable - POCKET.from) / Math.max(.01, cap - POCKET.from))); return room * (1 - POCKET.shrink * frag); };
  const S = Object.create(real);
  S.solve = async function (job) {
    const ins = (+job.sheet.insetPt || 0) + Math.max(0, +job.clearancePt || 0) / 2, usable = Math.max(0, job.sheet.wPt - 2 * ins) * Math.max(0, job.sheet.hPt - 2 * ins), cap = +job.maxFill || .8;
    const area = p => inflated(p, job.clearancePt), byId = new Map(job.pieces.map(p => [p.id, p]));
    const locked = new Map((job.lockedPlacements || []).filter(l => byId.has(l.id)).map(l => [l.id, l]));
    const placements = [...locked.values()].map(l => ({ ...l })); let used = placements.reduce((n, l) => n + area(byId.get(l.id)), 0);
    const todo = job.pieces.filter(p => !locked.has(p.id)), fifo = todo.some(p => p.orderDate > 0);
    const groups = new Map(); todo.forEach((p, i) => { const g = groups.get(p.order) || { order: p.order, pieces: [], date: Infinity, idx: i }; g.pieces.push(p); g.date = Math.min(g.date, p.orderDate || 0); groups.set(p.order, g); });
    const mates = new Map(); for (const p of job.pieces) mates.set(p.order, (mates.get(p.order) || 0) + 1);   // (the order's pieces on this sheet, placed or not: a "single" is an order of one)
    let seq = [...groups.values()];
    seq.sort(fifo ? (a, b) => a.date - b.date || a.idx - b.idx : job.singlesFirst ? (a, b) => ((mates.get(a.order) > 1) - (mates.get(b.order) > 1)) || a.idx - b.idx : (a, b) => a.idx - b.idx);
    const rejects = [], capped = [], noRoom = [], pocketFilled = []; let missed = false;
    for (const g of seq) {
      const take = []; let u = used, ok = true, why = '';
      // (cfg.maxOrder: a poor packer, one that never seats a whole order asking for more than that share of what the sheet takes, however empty the sheet is)
      if (cfg.maxOrder && g.pieces.reduce((n, p) => n + area(p), 0) > cfg.maxOrder * cap * usable) { ok = false; why = 'room'; }
      else for (const p of g.pieces) { const a = area(p); if (u + a > cap * usable + 1e-6) { ok = false; why = 'cap'; break; } if (a > allowed(u, usable, cap) + 1e-6) { ok = false; why = 'room'; break; } take.push(p); u += a; }
      const leak = cfg.leak && !ok && g.pieces.length > 1 && take.length && cfg.rng && cfg.rng() < cfg.leak;   // (a search that leaves half an order seated: the page's keepOrdersWhole lifts it)
      if (ok || leak) {
        for (const p of take) { placements.push({ id: p.id, angle: 0, cxPt: 0, cyPt: 0, wPt: p.w / p.scale, hPt: p.h / p.scale }); used += area(p); if (missed && !leak) pocketFilled.push(p.id); }
        if (leak) { for (const p of g.pieces.slice(take.length)) { rejects.push(p.id); (why === 'cap' ? capped : noRoom).push(p.id); } missed = true; }
      } else { for (const p of g.pieces) { rejects.push(p.id); (why === 'cap' ? capped : noRoom).push(p.id); } missed = true; }
    }
    const room = allowed(used, usable, cap);
    return { placements, rejects, capped, noRoom, pocketFilled, density: used / usable, usablePt2: usable, freePt2: usable - used, placedPt2: used, pocket: { wPt: 0, hPt: 0, xPt: 0, yPt: 0 }, trials: 10, elapsedMs: 0, timedOut: false,
      endedBy: !rejects.length ? 'complete' : noRoom.length ? 'no-room' : 'cap', smallRoom: job.roomCheck ? room >= (cfg.smallPt2 || 0) : undefined, params: { maxFill: cap }, unjam: undefined };
  };
  S.verify = () => ({ ok: true });
  return S;
}

/** A stream with an explicit list of BIG orders put in (an order of `pieces` copies of one design: a wedding, a school, a shop): { at: 0..1 position in the day, pieces, metal, design:[idx,f], kind:'qty'|'discs' } */
function withBig(S, stream, big = [], seed = 1) {
  const out = stream.slice();
  big.forEach((b, i) => {
    const at = Math.min(out.length - 1, Math.max(0, Math.floor((b.at == null ? (i + 1) / (big.length + 1) : b.at) * out.length))), old = out[at];
    const [idx, f] = b.design || [10, 1];
    const o = orderOf(S, old.id, old.n, [{ kind: b.kind || 'discs', k: b.pieces, idx, f, metal: b.metal || 'gold' }], 1000 + old.n);
    o.big = b.pieces; out[at] = o;
  });
  return out;
}

/** The sheet (mm) that holds about `perSheet` of this stream's order-parts of a metal at the fill ceiling: the page's own areas stay unscaled, the sheet is sized to them. */
function sheetFor(S, stream, metals, perSheet, cap = .8, ratio = 1.4) {
  let area = 0, parts = 0;
  for (const o of stream) for (const m of metals) { const p = partOf(o, m); if (p && !o.big) { area += p.pieces.reduce((n, c) => n + c.areaPt2, 0); parts++; } }
  const need = (area / Math.max(1, parts)) * perSheet / (cap * .93);   // (about 93 % of the ceiling is reached in practice)
  const wPt = Math.sqrt(need * ratio), hPt = need / wPt, mm = pt => Math.ceil(pt * 25.4 / 72 + 2.2);
  return `${mm(wPt)}x${mm(hPt)}`;
}

/** The pieces in the stream, ascending by area: the area of one of the smallest (the 7th percentile), for the stand-in's "room for a small charm". */
function smallArea(stream, q = .07) { const a = stream.flatMap(piecesOf).map(c => c.areaPt2).sort((x, y) => x - y); return a.length ? a[Math.floor(q * (a.length - 1))] : 0; }

/** One day: build, run, measure. */
async function runDay(o = {}) {
  const root = path.resolve(o.root || Lib.REPO), seed = +o.seed || 1, orders = +o.orders || 300, metals = (o.metals || ['gold', 'silver']).filter(m => METALS.includes(m));
  const perSheet = +o.perSheet || 45, solver = o.solver || 'sim', step = +o.step || 60, late = +o.late || 0, mixed = o.mixed != null ? +o.mixed : .08, batches = o.batches || [2, 3, 5, 4];
  const S = L.solverOf(root), mixName = o.mix || 'day', mix = Object.assign({}, MIXES[mixName] || MIXES.day, { late, otherMetal: mixed });
  let stream = makeStream(S, { seed, orders, mix });
  stream = withBig(S, stream, o.big || [], seed);
  const sheet = o.sheet || (solver === 'real' ? '60x40' : sheetFor(S, stream, metals, perSheet));
  const sim = new Sim({ root, step, seed, sheet, extra: o.extra || [] }), cfg = { smallPt2: smallArea(stream) * 1.0, leak: +o.leak || 0, maxOrder: +o.maxOrder || 0, rng: L.rng(seed * 17 + 3) }, fake = solver === 'sim' ? standIn(L.solverOf(root), cfg) : null;
  Object.assign(sim.ctx.S.settings, o.settings || {});   // (the page's settings for this run: { nospread: 'off' } is the page without the policy, { nospreadMargin: 0.1 } the one setting)
  const metalSim = m => { const ms = sim.metal(m); if (fake) ms.S = fake; return ms; };
  const expectedOf = (ord, m) => ord.lines.filter(l => l.metal === m).reduce((n, l) => n + l.pieces.length, 0);
  const log = o.log || (() => {}), t0 = Date.now(), plan = schedule(stream, batches, +o.drain || 2), seen = new Map();
  // (what waiting would buy, not something the page does: every line of an order is put on a sheet when its LAST line has come, so no line is ever late; `held` is how many orders that holds back and for how many updates)
  let held = null;
  if (o.waitLines) {
    const first = new Map(), last = new Map(); for (const ev of plan) for (const a of ev.arrivals) { if (!first.has(a.order.id)) first.set(a.order.id, ev.tick); last.set(a.order.id, ev.tick); }
    for (const ev of plan) ev.arrivals = ev.arrivals.filter(a => last.get(a.order.id) === ev.tick).map(a => ({ order: a.order, lines: a.order.lines }));
    const waits = [...last].map(([id, t]) => t - first.get(id)).filter(w => w > 0); held = { orders: waits.length, meanUpdates: waits.length ? r1(mean(waits), 2) : 0, maxUpdates: Math.max(0, ...waits) };
  }
  // (seen: order|metal -> the pieces that came in before this update, so that the intake is told how many the order has placed already)
  for (const ev of plan) {
    for (const m of metals) {
      const parts = ev.arrivals.map(a => { const p = partOf(a.order, m, a.lines); if (p) { const k = a.order.id + '|' + m, before = seen.get(k) || 0; seen.set(k, before + p.pieces.length); p.expected = expectedOf(a.order, m); p.order = { pieces: p.pieces, expected: p.expected, placed: before }; } return p; }).filter(Boolean);
      if (parts.length || (sim.metals[m] && sim.metals[m].work.length)) await metalSim(m).step(parts, ev.tick);
    }
    log(`t${ev.tick} +${ev.orders.length} sheets ${sim.pages.filter(p => p.placements.length).length} ${Math.round((Date.now() - t0) / 1000)}s`);
    if (o.watch) {   // (where the pieces of one order are after this update, and what its sheets are doing: a trace to explain a split)
      const w = stream.find(q => q.id === String(o.watch)), ids = new Set(w ? piecesOf(w).map(c => c.id) : []);
      const at = sim.pages.filter(p => p.charms.some(c => ids.has(c.id))).map(p => `${Lib.CODE[p.metal]}${p.page}[${Math.round(100 * fillOf(p))}%${p.releaseFull ? ' released' : ''}${p.topup ? ' gapfill' + (p.topup.closedAt ? '-closed' : '') : ''}: ${p.placements.filter(q => ids.has(q.id)).length} placed, ${p.charms.filter(c => ids.has(c.id)).length - p.placements.filter(q => ids.has(q.id)).length} waiting]`);
      if (at.length) (o.trace || (o.trace = [])).push(`t${ev.tick}: ${at.join('  ')}   arrivals of it: ${ev.arrivals.filter(a => a.order.id === String(o.watch)).map(a => a.lines.map(l => l.kind).join('+')).join('; ') || '-'}`);
    }
  }
  const res = measure(sim, stream, metals, { sheet, solver, seed, mix: mixName, late, orders, perSheet, root, wallS: Math.round((Date.now() - t0) / 1000) });
  if (o.sets !== false) {   // the sets the page's own assembly (Gate.assemble of `root`) makes of the day's sheets when every sheet is completed at the end of the day (what the sets are worth: one set held 14 sheets)
    try {
      const world = Lib.setWorld(root); let tick = 0; for (const p of sim.pages) if (p.placements.length && !p.releaseFull) { p.releaseFull = true; p.releasedAt = ++tick; }
      world.sync(sim.pages); await world.assemble();
      const sets = world.sets(), pg = world.gatePages(), inSet = new Set(sets.flatMap(x => x.ids)), sizes = sets.map(x => x.ids.length).sort((a, b) => b - a);
      res.setsEnd = { formed: sets.length, largest: sizes[0] || 0, sizes, sheetsInSets: inSet.size, sheets: pg.length, outside: pg.filter(x => !inSet.has(x.sheetId) && ['gold', 'silver'].includes(x.metal)).length, oversized: sizes.filter(n => n > 2).length };
    } catch (e) { res.setsEnd = { error: String(e && e.message || e) }; }
  }
  res.problems = sim.pages.filter(p => p.problem).map(p => ({ metal: p.metal, page: p.page, problem: p.problem }));
  res.passes = (sim.ctl.passes || []).length; if (o.trace) res.trace = o.trace; if (held) res.held = held;
  res.sim = sim;
  return res;
}

/** The measures of a finished (or running) simulation. */
function measure(sim, stream, metals, head = {}) {
  const pages = sim.pages.filter(p => metals.includes(p.metal) && p.placements.length), where = new Map();
  for (const p of pages) for (const q of p.placements) where.set(q.id, p);
  const label = p => `${Lib.CODE[p.metal]}${p.page}`;
  // parts: one order's pieces of one metal (what a sheet of that metal holds of it)
  const parts = []; let unplaced = 0, pieces = 0;
  for (const o of stream) for (const m of metals) {
    const part = partOf(o, m); if (!part) continue;
    const sheets = [...new Set(part.pieces.map(c => where.get(c.id)).filter(Boolean))], miss = part.pieces.filter(c => !where.has(c.id)).length;
    pieces += part.pieces.length; unplaced += miss;
    parts.push({ order: o.id, metal: m, n: part.pieces.length, kinds: part.lines.map(l => l.kind), sheets, miss, big: !!o.big, pair: part.lines.some(l => ['pair', 'huggie', 'mismatched'].includes(l.kind)) });
  }
  const multi = parts.filter(x => x.n > 1), split = multi.filter(x => x.sheets.length > 1), splitNoBig = split.filter(x => !x.big);
  const pairLines = []; for (const o of stream) for (const l of o.lines) if (['pair', 'huggie', 'mismatched'].includes(l.kind) && metals.includes(l.metal)) pairLines.push(new Set(l.pieces.map(c => where.get(c.id)).filter(Boolean)).size);
  // the groups of sheets that share an order (the cardinal rule: one unit), over all the sheets, and over splits of ONE colour only
  const unit = (sameOnly) => {
    const parent = new Map(pages.map(p => [p, p])), find = x => { while (parent.get(x) !== x) { parent.set(x, parent.get(parent.get(x))); x = parent.get(x); } return x; };
    const byOrder = new Map();
    for (const p of pages) for (const q of p.placements) { const c = p.charms.find(x => x.id === q.id); if (!c) continue; const k = sameOnly ? c.order + '|' + p.metal : c.order; let s = byOrder.get(k); if (!s) byOrder.set(k, s = new Map()); s.set(c.id, p); }
    for (const [, pcs] of byOrder) { if (pcs.size < 2) continue; const ps = [...new Set(pcs.values())]; for (let i = 1; i < ps.length; i++) { const a = find(ps[0]), b = find(ps[i]); if (a !== b) parent.set(a, b); } }
    const comp = new Map(); for (const p of pages) { const r = find(p); (comp.get(r) || comp.set(r, []).get(r)).push(p); }
    const sizes = [...comp.values()].map(c => c.length), linked = sizes.filter(n => n > 1), N = pages.length;
    return { components: sizes.length, linked: linked.length, meanLinked: linked.length ? r1(mean(linked), 2) : 0, max: Math.max(0, ...sizes), perSheetMean: N ? r1(sizes.reduce((s, n) => s + n * n, 0) / N, 2) : 0, sheetsInLinked: linked.reduce((a, b) => a + b, 0), hist: sizes.reduce((h, n) => { h[n] = (h[n] || 0) + 1; return h; }, {}), biggest: [...comp.values()].sort((a, b) => b.length - a.length)[0].map(label) };
  };
  const closed = pages.filter(p => p.releaseFull), fills = ps => ps.length ? r1(mean(ps.map(fillOf)), 4) : null;
  const byMetal = Object.fromEntries(metals.map(m => [m, { sheets: pages.filter(p => p.metal === m).length, closed: closed.filter(p => p.metal === m).length, fillClosed: fills(closed.filter(p => p.metal === m)), fillAll: fills(pages.filter(p => p.metal === m)) }]));
  // an order bigger than a sheet: the sheets it fills against the fewest it could (its area over what a sheet takes under the fill ceiling)
  const usable = pages.length ? sim.ctx.usableArea(pages[0]) : 0, capacity = usable * (+sim.ctx.S.settings.maxFill || .8);
  const bigRows = parts.filter(x => x.big).map(x => { const o = stream.find(q => q.id === x.order), area = partOf(o, x.metal).pieces.reduce((n, c) => n + sim.ctx.inflatedArea(c), 0); return { order: x.order, pieces: x.n, sheets: x.sheets.length, miss: x.miss, fewest: capacity ? Math.ceil(area / capacity - 1e-9) : null, share: capacity ? r1(area / capacity, 2) : null }; });
  return Object.assign({}, head, {
    orders: stream.length, partsMulti: multi.length, pieces, unplaced, multiOrdersSplit: split.length, multiOrdersSplitNoBig: splitNoBig.length, multiPct: multi.length ? r1(100 * split.length / multi.length, 2) : 0,
    pairLines: pairLines.length, pairLinesSplit: pairLines.filter(n => n > 1).length, splitList: split.slice(0, 30).map(x => ({ order: x.order, metal: x.metal, n: x.n, kinds: x.kinds.join('+'), sheets: x.sheets.map(label).join(',') })),
    all: unit(false), same: unit(true),
    sheets: pages.length, closed: closed.length, fillClosed: fills(closed), fillAll: fills(pages), fillClosedGfss: fills(closed.filter(p => ['gold', 'silver'].includes(p.metal))), over75: closed.length ? r1(closed.filter(p => fillOf(p) >= .75 - 1e-9).length / closed.length, 3) : null,
    byMetal, big: bigRows, ordersMulti: multi.length,
  });
}

module.exports = { MIXES, POCKET, standIn, withBig, sheetFor, smallArea, runDay, measure };
