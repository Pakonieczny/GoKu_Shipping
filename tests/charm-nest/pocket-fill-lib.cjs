/* Shared pieces of the pocket-fill suite and its benchmark (tests/charm-nest/pocket-fill.cjs, pocket-fill-bench.cjs).
 *
 * Paul, 7 Oct 2026: "The nesting process did not do a good job of filling this sheet before it moved onto the next sheet.
 * It needs to be more methodical to fit in the small charms into tight spaces."
 *
 *   · charms drawn from the shop's own library silhouettes (the solver's PROBES: real charms with their rings)
 *   · a stand-in for the sorter's intake that runs the PAGE'S OWN functions (feedTurn, feedOn, keepOrdersWhole, sheetFull,
 *     topupSettle, topupRoom: sliced from charm-nest-1.html and run in a vm) around the real solver, so a sheet closes,
 *     gap-fills and hands charms on exactly as the page decides it, and the layouts are the real solver's
 *   · an exhaustive check, with nothing from the solver's search in it: every position of the sheet grid at every angle
 *     (2° steps, or 1°), the one collision predicate (Grid.fits) the solver's own placements are tested with
 * Everything is offline, deterministic (seeded) and touches no network, Firestore or paid call.
 * `root` is the directory holding the solver and the page: the repo (default) or a copy of an older version of it.      */
'use strict';
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const REPO = path.join(__dirname, '../..');
const MM = 25.4 / 72, PT = mm => mm / MM;
const rng = seed => { let a = seed >>> 0 || 1; return () => { a |= 0; a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; };

function solverOf(root) { return require(path.join(root || REPO, 'charm-nest-solver.js')); }

/** A charm of the shop's library (PROBES[index], 1 px/pt) drawn at 4 px/pt and enlarged by `f`. */
function charm(S, id, index, f, order, orderDate) {
  const [w, h, b] = S.PROBES[index], src = S.bitsFromBase64(b, w * h), K = 4, W = Math.round(w * f * K), H = Math.round(h * f * K), bits = new Uint8Array(W * H);
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) bits[y * W + x] = src[Math.min(h - 1, Math.floor(y / (f * K))) * w + Math.min(w - 1, Math.floor(x / (f * K)))];
  let n = 0; for (let i = 0; i < bits.length; i++) n += bits[i];
  return { id, name: id, w: W, h: H, scale: K, bits, areaPt2: n / (K * K), widthPt: W / K, heightPt: H / K, order: order || id, orderDate: orderDate || 0, hash: id };
}
/** A square test charm (side in pt), the shape the other nest suites use. */
function square(id, side, order, orderDate) {
  const K = 4, n = Math.round(side * K), bits = new Uint8Array(n * n).fill(1);
  return { id, name: id, w: n, h: n, scale: K, bits, areaPt2: side * side, widthPt: side, heightPt: side, order: order || id, orderDate: orderDate || 0, hash: id };
}
const mm2 = pt2 => pt2 * MM * MM;

/** The green-line leftover of a cut sheet: the stock cut away from the left, in steps (mm removed from x = 0 per band of rows). */
function leftover(wPt, hPt, bandsMm) {
  const step = .5, n = Math.ceil(hPt / step), values = [];
  for (let i = 0; i < n; i++) { const band = Math.min(bandsMm.length - 1, Math.floor(i * step / hPt * bandsMm.length)); values.push(+PT(bandsMm[band]).toFixed(3)); }
  return { version: 1, wPt, hPt, axis: 'x', step, values };
}
/** The 50 × 46 mm leftover of the 7 Oct screenshot: a staircase cut away at the left, deepest at the top. */
const SCREENSHOT_STEPS = [34, 30, 26, 22, 17, 13];

/** The orders of a stream: a mix of small, middle and large charms of the library, a few of them two-piece. */
function makeOrders(S, n, seed, mix) {
  const r = rng(seed), orders = [];
  mix = mix || { small: .35, mid: .40, large: .25, two: .15, grow: 1 };
  for (let i = 0; i < n; i++) {
    const u = r(), kind = u < mix.small ? 'small' : u < mix.small + mix.mid ? 'mid' : 'large';
    const pick = k => k === 'small' ? [Math.floor(r() * 8), 1 + r() * .1] : k === 'mid' ? [8 + Math.floor(r() * 10), 1 + r() * .2] : [15 + Math.floor(r() * 9), 1.2 + r() * .3];
    const pieces = [], count = r() < mix.two ? 2 : 1, id = 'o' + i;
    for (let j = 0; j < count; j++) { const [pi, f] = pick(j ? 'small' : kind); pieces.push(charm(S, count > 1 ? `${id}.${j}` : id, pi, f * mix.grow, id, 1000 + i)); }
    orders.push({ id, kind, pieces });
  }
  return orders;
}

/* ── the page's own rules, run as the page has them ─────────────────────────────────────────────────────────────── */
function pageRules(root, ctl) {
  const html = fs.readFileSync(path.join(root || REPO, 'charm-nest-1.html'), 'utf8');
  const slice = (from, to) => { const a = html.indexOf(from), b = html.indexOf(to, a + 1); if (a < 0 || b < 0) throw new Error('slice ' + from); return html.slice(a, b); };
  const ctx = {
    window: { B: { run: { runId: 'run-1', status: 'running' } } }, S: { settings: { maxFill: .8, clearancePt: -.5, runMode: 'auto' } },
    Set, Map, Math, JSON, Object, Array, String, Number, Date, console,
    CharmNestRose: require(path.join(root || REPO, 'charm-nest-rose.js')),   // (the page's own test for the metals with a green line: feedTurn asks it)
    log() {}, agent: () => null, fmt: { pct: x => Math.round(100 * x) + '%' },
    activeCharms: sh => sh.charms.filter(c => !c.excluded), carefulNest: () => true, allSheets: () => ctl.pages,
    overflowToNextSheet: sh => ctl.overflow(sh), startNest: sh => ctl.work.push(sh),
  };
  vm.createContext(ctx);
  vm.runInContext(slice('function keepOrdersWhole(', 'function orderSummary('), ctx);
  vm.runInContext(slice('function inflatedArea(', 'function usableArea('), ctx);
  vm.runInContext(slice('const FEED_ORDERS', '/* A stopped run starts none of its sheets'), ctx);
  return ctx;
}

/* ── one sheet's search, as the page builds the job (buildJob) and finishes it (finishNest) ───────────────────────── */
const CAREFUL_ANGLES = Array.from({ length: 180 }, (_, i) => i * 2);
const CUT_METALS = ['rose', 'gold10k', 'gold14k'];

/**
 * A stream of orders onto sheets of one metal, the way the sorter does it: each update brings `batch` orders to the sheet
 * the intake chooses; a search places the oldest three and is judged by the page's own rules; a sheet that is full, or
 * whose gap fill is over, passes what is left to the next open sheet.
 *   opts: { root, metal, sheet:{wPt,hPt,remnant?}, orders, batch, seed, quiet, onSheet }
 */
async function runStream(opts) {
  const root = opts.root || REPO, S = solverOf(root), metal = opts.metal || 'gold14k', cut = CUT_METALS.includes(metal), fast = ['gold', 'silver'].includes(metal);
  const ctl = { pages: [], work: [], moved: [], overflow: null, ms: 0, searches: [], roomWrong: [] };
  const ctx = pageRules(root, ctl);
  const stock = opts.sheet, newPage = () => ({ metal, runId: 'run-1', page: ctl.pages.length + 1, charms: [], placements: [], rejects: [], status: 'ready', intakePhase: 'fill', log: [], movedFrom: [], searches: 0, ms: 0 });
  const closed = p => !!p.releaseFull;
  const pageIndex = p => ctl.pages.indexOf(p);
  ctl.overflow = sh => {
    const ids = new Set(sh.rejects), moving = sh.charms.filter(c => ids.has(c.id));
    if (!moving.length) return null;
    if (!sh.placements.length) { sh.problem = 'cannot fit an empty sheet'; return null; }
    sh.charms = sh.charms.filter(c => !ids.has(c.id)); sh.rejects = []; sh.movedFrom.push(moving.map(c => c.id));
    const i = pageIndex(sh);
    let next = fast ? ctl.pages.slice(i + 1).find(p => !closed(p)) || null : ctl.pages.length - 1 > i ? ctl.pages[ctl.pages.length - 1] : null;
    if (!next || closed(next)) { next = newPage(); ctl.pages.push(next); }
    next.appendOnly = next.placements.length > 0;
    for (const c of moving) { c.pinned = null; next.charms.push(c); }
    if (!ctl.work.includes(next)) ctl.work.push(next);
    return next;
  };
  const room = p => ctx.topupRoom(p) > 0;
  const intakePage = () => {
    const newest = ctl.pages[ctl.pages.length - 1];
    if (!fast) return closed(newest) ? null : newest;
    const open = p => p.charms.length && !closed(p), pick = ctl.pages.find(p => p !== newest && open(p) && room(p)) || newest;
    return pick === newest && open(newest) && !room(newest) ? null : closed(pick) ? null : pick;
  };
  const buildJob = (sh, items) => ({
    sheet: { wPt: stock.wPt, hPt: stock.hPt, insetPt: 1.5, ...(stock.remnant ? { remnant: stock.remnant } : {}) },
    clearancePt: -.5, angles: CAREFUL_ANGLES, fineRes: 2, coarseRes: .5, timeBudgetMs: 180000, fullBudget: false, stallMs: 60000, maxTrials: 2000, seed: opts.seed || 1,
    lockedPlacements: sh.appendOnly ? sh.nestInitial || sh.placements : sh.nestInitial?.length ? sh.nestInitial : null,
    careful: true, roomCheck: fast, block: cut, initialLayout: sh.nestInitial || null, maxFill: .8, nearFullContact: !cut,
    pieces: items.map(c => ({ id: c.id, w: c.w, h: c.h, scale: c.scale, bits: c.bits, areaPt2: c.areaPt2, order: c.order || c.id, orderDate: sh.topup && !sh.topup.closedAt && sh.appendOnly ? 0 : (+c.orderDate || 0), pinned: c.pinned ? { ...c.pinned } : null })),
    ...(opts.job || {}),
  });
  async function nest(sh) {
    let items = ctx.activeCharms(sh);
    if (!items.length) return;
    items = ctx.feedTurn(sh, items);
    const onSheet = new Set(items.map(c => c.id)), kept = (sh.placements || []).filter(p => onSheet.has(p.id));
    sh.nestInitial = kept.length ? kept.map(p => ({ ...p })) : null;
    for (const c of items) { const p = kept.find(q => q.id === c.id); c.pinned = p ? { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle } : null; }
    sh.placements = kept.map(p => ({ ...p })); sh.rejects = [];
    const job = buildJob(sh, items), t0 = Date.now();
    const result = await S.solve(job, {});
    const ms = Date.now() - t0; sh.ms += ms; sh.searches++; ctl.searches.push({ page: sh.page, ms, n: items.length - kept.length, placed: result.placements.length - kept.length, pocket: result.careful && result.careful.pocket, rejects: result.rejects.length });
    sh.status = 'finishing'; sh.endedBy = result.endedBy; sh.placements = result.placements.map(p => ({ ...p })); sh.rejects = result.rejects.slice();
    const byId = new Map(items.map(c => [c.id, c]));
    ctx.keepOrdersWhole(sh, byId, new Set(result.pocketFilled || []));
    sh.verification = { ok: S.verify(job, sh.placements, 4).ok };
    if (!sh.verification.ok) throw new Error('layout failed verification on page ' + sh.page);
    sh.result = result; sh.density = result.density; sh.usablePt2 = result.usablePt2; sh.freePt2 = result.freePt2; sh.placedPt2 = result.placedPt2;
    sh.releaseFull = ctx.topupSettle(sh, result, ctx.sheetFull(sh, result, items)) || !!(sh.keepRelease && sh.keepRelease.full);
    // the room check (Gold and Silver): "no room for even the smallest charms" releases the sheet at once, so it must never be wrong
    if (fast && result.smallRoom === false && opts.checkRoom) {
      const grid = gridOf(S, stock, sh.placements, sh.charms, -.5);
      for (const k of [0, 1]) { const spot = fitsAnywhere(S, grid, charm(S, 'probe' + k, k, 1), -.5, 2); if (spot) ctl.roomWrong.push({ page: sh.page, probe: k, ...spot }); }
    }
    sh.status = sh.rejects.length ? 'partial' : 'complete';
    if (sh.rejects.length && sh.endedBy !== 'stopped') ctl.overflow(sh);
    ctx.feedOn(sh);
  }
  ctl.pages.push(newPage());
  const t00 = Date.now();
  for (let i = 0; i < opts.orders.length; i += opts.batch || 3) {
    const batch = opts.orders.slice(i, i + (opts.batch || 3));
    let page = intakePage();
    if (!page) { page = newPage(); ctl.pages.push(page); }
    page.appendOnly = page.placements.length > 0;
    for (const o of batch) for (const c of o.pieces) page.charms.push({ ...c, pinned: null });
    ctl.work.push(page);
    while (ctl.work.length) { const sh = ctl.work.shift(); await nest(sh); }
    if (opts.progress) opts.progress(i + batch.length, opts.orders.length, ctl);
  }
  ctl.ms = Date.now() - t00;
  return Object.assign(ctl, { S, ctx, closed });
}

/* ── the exhaustive check ────────────────────────────────────────────────────────────────────────────────────────── */
/** The layout of a sheet as a collision grid, every placed charm stamped as the solver stamps a saved one. */
function gridOf(S, stock, placements, charms, clearancePt) {
  const byId = new Map(charms.map(c => [c.id, c]));
  return S.makeSheetGrid({ wPt: stock.wPt, hPt: stock.hPt, insetPt: 1.5, ...(stock.remnant ? { remnant: stock.remnant } : {}), fixedPieces: placements.map(p => ({ piece: byId.get(p.id), placement: p })) }, clearancePt, 2);
}
/** First legal position of a charm anywhere on the grid at any angle of the step: { angle, x, y } or null. Nothing of the
    solver's search is used: every cell of the grid, every angle, the one collision test. */
function fitsAnywhere(S, grid, piece, clearancePt, step = 2, from = 0) {
  for (let a = from; a < 360; a += step) {
    const v = S.prepareVariant(piece, a, clearancePt, 2); if (!v) continue;
    const pm = v.fine.pm, PW = grid.W - pm.w, PH = grid.H - pm.h;
    for (let y = 0; y <= PH; y++) for (let x = 0; x <= PW; x++) if (grid.fits(pm, x, y)) return { angle: a, x, y };
  }
  return null;
}

/** The stock a green line consumes (Rose Gold, 10K, 14K): on each row of the sheet, everything from the cut-away stock to the farthest
    charm on that row. A pocket inside it goes with the cut; stock beyond it is the leftover the next sheet is seated on.
    Rows are fine cells (2 per pt); the answer is { far[row] (first free cell of the row, or the farthest charm's end), base[row], cells }. */
function envelopeOf(S, stock, placements, charms) {
  const byId = new Map(charms.map(c => [c.id, c])), FW = Math.round(stock.wPt * 2), FH = Math.round(stock.hPt * 2), base = new Int32Array(FH), far = new Int32Array(FH);
  const g = S.makeSheetGrid({ wPt: stock.wPt, hPt: stock.hPt, insetPt: 1.5, ...(stock.remnant ? { remnant: stock.remnant } : {}) }, -.5, 2);
  for (let y = 0; y < FH; y++) { let d = 0; while (d < FW && g.get(d, y)) d++; base[y] = far[y] = d; }
  for (const p of placements) {
    const v = S.prepareVariant(byId.get(p.id), p.angle, -.5, 2), x0 = Math.round(p.cxPt * 2 - v.solid.cx), y0 = Math.round(p.cyPt * 2 - v.solid.cy), b = v.fine.bits;
    for (let r = 0; r < v.fine.h; r++) for (let c = v.fine.w - 1; c >= 0; c--) if (b[r * v.fine.w + c]) { const y = y0 + r; if (y >= 0 && y < FH) far[y] = Math.max(far[y], x0 + c + 1); break; }
  }
  let cells = 0; for (let y = 0; y < FH; y++) cells += far[y] - base[y];
  return { far, base, cells, FW, FH };
}
/** fitsAnywhere, but only where the whole charm lies inside the envelope (every row of it ends at or before the farthest charm of that row). */
function fitsInside(S, grid, piece, env, clearancePt, step = 2) {
  for (let a = 0; a < 360; a += step) {
    const v = S.prepareVariant(piece, a, clearancePt, 2); if (!v) continue;
    const pm = v.fine.pm, w = v.fine.w, h = v.fine.h, b = v.fine.bits, last = new Int32Array(h).fill(-1);
    for (let r = 0; r < h; r++) for (let c = w - 1; c >= 0; c--) if (b[r * w + c]) { last[r] = c; break; }
    const PW = grid.W - pm.w, PH = grid.H - pm.h;
    for (let y = 0; y <= PH; y++) positions: for (let x = 0; x <= PW; x++) {
      for (let r = 0; r < h; r++) if (last[r] >= 0 && x + last[r] + 1 > env.far[y + r]) continue positions;
      if (grid.fits(pm, x, y)) return { angle: a, x, y };
    }
  }
  return null;
}

/** What a finished stream shows: the fill of each closed sheet, and the charms that moved on from a sheet, or came to a
    later one, although a pocket of the closed sheet still took them. */
function measure(run, stock, opts = {}) {
  const S = run.S, clearancePt = -.5, out = { sheets: [], leftOut: [], potential: [], placedTotal: 0, ms: run.ms };
  const every = run.pages.flatMap(p => p.charms);
  for (const [i, p] of run.pages.entries()) {
    if (!p.placements.length) continue;
    const fill = p.usablePt2 ? 1 - p.freePt2 / p.usablePt2 : p.density, closedNow = run.closed(p);
    out.placedTotal += p.placements.length;
    const row = { page: p.page, placed: p.placements.length, fill: +fill.toFixed(3), closed: closedNow, searches: p.searches, ms: p.ms, leftOut: [], potential: [], leftOutAnywhere: 0, potentialAnywhere: 0 };
    // a green-line sheet loses only the pockets inside the stock the line consumes: its fill is the charms' share of that stock
    const cut = !!stock.remnant || opts.cut, env = cut ? envelopeOf(S, stock, p.placements, p.charms) : null;
    if (env) { let placedCells = 0; const byId = new Map(p.charms.map(c => [c.id, c])); for (const q of p.placements) placedCells += S.prepareVariant(byId.get(q.id), q.angle, clearancePt, 2).cells; row.envFill = +(placedCells / Math.max(1, env.cells)).toFixed(3); row.envMm2 = Math.round(mm2(env.cells / 4)); }
    if (closedNow || opts.allSheets) {
      const grid = gridOf(S, stock, p.placements, p.charms, clearancePt), movedIds = new Set(p.movedFrom.flat());
      const single = c => every.filter(x => x.order === c.order).length === 1;
      for (const c of every) {
        if (p.placements.some(q => q.id === c.id) || !single(c)) continue;
        const later = run.pages.slice(i + 1).some(q => q.placements.some(r => r.id === c.id));
        if (!movedIds.has(c.id) && !later) continue;
        const any = fitsAnywhere(S, grid, c, clearancePt, opts.step || 2), spot = env ? (any && fitsInside(S, grid, c, env, clearancePt, opts.step || 2)) : any;
        if (any) row[movedIds.has(c.id) ? 'leftOutAnywhere' : 'potentialAnywhere']++;
        if (spot) (movedIds.has(c.id) ? row.leftOut : row.potential).push({ id: c.id, mm2: +mm2(c.areaPt2).toFixed(0), ...spot });
      }
    }
    out.sheets.push(row); out.leftOut.push(...row.leftOut); out.potential.push(...row.potential);
  }
  return out;
}

module.exports = { envelopeOf, fitsInside, REPO, MM, PT, rng, solverOf, charm, square, mm2, leftover, SCREENSHOT_STEPS, makeOrders, pageRules, runStream, gridOf, fitsAnywhere, measure, CAREFUL_ANGLES };
