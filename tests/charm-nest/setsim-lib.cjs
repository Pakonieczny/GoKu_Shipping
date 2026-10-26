/* SETSIM: the shared pieces of the sets-and-whole-orders simulation (nospread.cjs, setsim-scenes.cjs, setsim-sets.cjs and their libraries).
 *
 * Paul, 13 Oct 2026: "A partial GF and SS sheet should never be part of a Set of sheets ... keep orders of the same colour on one sheet ... the priority
 * should be to maintain all of the pieces for each given order on that one sheet ... keep that [multi-piece order] in queue and then add it to the next sheet
 * and wait for an order that has a single charm to fill in small remaining gaps on all nearly finished sheets."
 *
 * What this measures, on a SYNTHETIC order stream only (never a real order, set or buyer; every number is made up here):
 *   · makeStream      a seeded stream of fake orders: singles, matching and mismatched earring pairs (Left + exact mirror Right), huggie pairs, n-disc necklaces,
 *                     quantity lines, multi-line orders, a share of orders whose lines are of two metals (that is what ties sheets of two metals into one group)
 *   · MetalSim        the sorter's own intake and nesting rules for one metal, run as the PAGE has them: the real functions feedTurn, feedOn, keepOrdersWhole,
 *                     overflowToNextSheet, sheetFull, topupSettle, topupRoom (charm-nest-1.html) and LiveNest.intakePage (charm-nest-bridge.js) are sliced from the
 *                     files of `root` and run in a vm around the real solver, so a change in those functions shows here without touching this file. The solver job
 *                     is built like buildJob (pocket-fill-lib.cjs). 12 degree angle steps by default (`step`) to keep a day of orders affordable; production uses 2.
 *   · SetWorld        the page's own set assembly (Gate.assemble, charm-nest-bridge.js) with the page's set rules (charm-nest-set-rules.js) and the cardinal rule
 *                     (charm-nest-shared-orders.js), run after every simulated 10 minutes over the sheets the nesting made, exactly as tests/charm-nest/sets-follow.cjs does
 *   · measure         per run: orders split over several sheets, partial sheets inside sets, sets formed, whole-sheet density, front raggedness, queued orders and waits
 * Offline and deterministic (seeded). No network, no Firestore, no Etsy, no paid call; nothing is written anywhere but the files a runner is told to write.
 * `root` is the repository (default) or a copy of an older commit of it (git worktree), so the same stream can be run BEFORE and AFTER a change.            */
'use strict';
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const L = require('./pocket-fill-lib.cjs');
const REPO = L.REPO, MM = L.MM, PT = L.PT;

const METALS = ['gold', 'silver', 'rose', 'gold14k', 'gold10k'];
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold14k: '14K', gold10k: '10K' };
const CUT = new Set(['rose', 'gold14k', 'gold10k']);

/* ═══════════════════════════ the synthetic stream ═══════════════════════════ */
const flip = c => { const W = c.w, H = c.h, bits = new Uint8Array(W * H); for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) bits[y * W + (W - 1 - x)] = c.bits[y * W + x]; return Object.assign({}, c, { bits }); };
const DEFAULT_MIX = {
  lines: [.60, .28, .12],                                              // an order has 1, 2 or 3 lines
  kinds: { single: .40, pair: .28, mismatched: .07, discs: .06, qty: .08, huggie: .05, trio: .06 },   // one line is a single charm, a matching pair, a mismatched pair, an n-disc necklace, a quantity-n line, a huggie pair, or three different charms
  size: { small: .35, mid: .40, large: .25 },
  metals: { gold: .45, silver: .32, rose: .12, gold14k: .06, gold10k: .05 },
  otherMetal: .10,                                                     // the share of extra lines that are of another metal than the order's first line
  late: 0,                                                             // the share of extra lines that reach the sorter 1-3 updates after the order's first line (a line held for review, read late, released from hold)
};
const rng = L.rng;
const pickW = (r, table) => { let u = r() * Object.values(table).reduce((a, b) => a + b, 0); for (const [k, w] of Object.entries(table)) { u -= w; if (u < 0) return k; } return Object.keys(table).pop(); };

/** One fake order line of a kind, from a spec { idx, f } (a design of the library's silhouettes and its scale), { idx2, f2 } for a mismatched pair, { k } the number of discs / copies. Pool ids are `order_line_copy`, the form the app uses everywhere. */
function lineOf(S, oid, n, kind, spec, metal, date) {
  const mk = (copy, idx, f, extra) => { const id = `${oid}_${n}_${copy}`; return Object.assign(L.charm(S, id, idx, f, oid, date), { poolId: id, metal }, extra || {}); };
  const key = `${oid}:${n}`, info = form => ({ receiptId: oid, transactionId: String(n), form }), pieces = [];
  if (kind === 'single') pieces.push(mk(1, spec.idx, spec.f));
  else if (kind === 'pair' || kind === 'huggie') {   // a Left and its exact mirror, the Right (the nester may turn a piece and never reflect it)
    const base = { groupKey: key, groupSize: 2, orderInfo: info('earring') };
    pieces.push(mk(1, spec.idx, spec.f, Object.assign({ side: 'L', mirror: false, bodyIndex: 0 }, base)), flip(mk(2, spec.idx, spec.f, Object.assign({ side: 'R', mirror: true, bodyIndex: 1 }, base))));
  } else if (kind === 'mismatched') {
    const base = { groupKey: key, groupSize: 2, orderInfo: info('earring') };
    pieces.push(mk(1, spec.idx, spec.f, Object.assign({ side: 'L', mirror: false, bodyIndex: 0 }, base)), mk(2, spec.idx2, spec.f2, Object.assign({ side: 'R', mirror: true, bodyIndex: 1 }, base)));
  } else if (kind === 'discs') for (let c = 1; c <= spec.k; c++) pieces.push(mk(c, spec.idx, spec.f, { groupKey: key, groupSize: spec.k, bodyIndex: c - 1, orderInfo: info('necklace') }));
  else if (kind === 'qty') for (let c = 1; c <= spec.k; c++) pieces.push(mk(c, spec.idx, spec.f));
  else if (kind === 'trio') spec.designs.forEach(([idx, f], c) => pieces.push(mk(c + 1, idx, f, { groupKey: key, groupSize: spec.designs.length, bodyIndex: c, orderInfo: info('charm') })));
  else throw new Error('setsim: unknown line kind ' + kind);
  return { n, kind, metal, pieces, delay: 0 };
}
/** An order from explicit lines: [{ kind, metal, ...spec, delay }]. */
const orderOf = (S, oid, n, specs, date) => ({ id: String(oid), n, lines: specs.map((sp, i) => Object.assign(lineOf(S, String(oid), i + 1, sp.kind, sp, sp.metal || 'gold', date || 1000 + n), { delay: sp.delay || 0 })) });

/** A seeded stream of fake orders: [{ id, n, lines:[{ n, kind, metal, pieces:[charm], delay }] }]. */
function makeStream(S, { seed = 1, orders = 200, mix = {} } = {}) {
  const M = Object.assign({}, DEFAULT_MIX, mix), r = rng(seed * 7919 + 13), out = [];
  const designOf = size => size === 'small' ? [Math.floor(r() * 8), 1 + r() * .1] : size === 'mid' ? [8 + Math.floor(r() * 10), 1 + r() * .15] : [15 + Math.floor(r() * 9), 1.1 + r() * .2];
  for (let i = 0; i < orders; i++) {
    const oid = String(4210000000 + i * 7), nLines = 1 + pickW(r, { 0: M.lines[0], 1: M.lines[1], 2: M.lines[2] }) * 1, date = 1000 + i;
    const first = pickW(r, M.metals), lines = [];
    for (let n = 1; n <= nLines; n++) {
      const kind = pickW(r, M.kinds), metal = n === 1 || r() >= M.otherMetal ? first : pickW(r, { gold: 1, silver: 1, rose: .5 });
      let spec;
      if (kind === 'single' || kind === 'qty') { const [idx, f] = designOf(pickW(r, M.size)); spec = { idx, f, k: 2 + Math.floor(r() * 2) }; }
      else if (kind === 'pair') { const [idx, f] = designOf(r() < .6 ? 'small' : 'mid'); spec = { idx, f }; }
      else if (kind === 'huggie') spec = { idx: 15 + Math.floor(r() * 9), f: 1 + r() * .15 };
      else if (kind === 'mismatched') { const [idx, f] = designOf(r() < .6 ? 'small' : 'mid'), [idx2, f2] = designOf(r() < .6 ? 'small' : 'mid'); spec = { idx, f, idx2, f2 }; }
      else if (kind === 'discs') spec = { k: 3 + Math.floor(r() * 4), idx: [8, 10, 12, 13][Math.floor(r() * 4)], f: .9 + r() * .1 };
      else spec = { designs: [0, 1, 2].map(() => designOf(pickW(r, { small: .5, mid: .4, large: .1 }))) };
      const line = lineOf(S, oid, n, kind, spec, metal, date);
      line.delay = n > 1 && r() < M.late ? 1 + Math.floor(r() * 3) : 0;
      lines.push(line);
    }
    out.push({ id: oid, n: i, lines });
  }
  return out;
}
const piecesOf = o => o.lines.flatMap(l => l.pieces);
/** The order's lines of one metal as the part of it that metal's sheets get. */
const partOf = (o, metal, lines = o.lines) => { const mine = lines.filter(l => l.metal === metal); return mine.length ? { id: o.id, n: o.n, metal, lines: mine, pieces: mine.flatMap(l => l.pieces) } : null; };
/** The updates of a stream: each brings 2-5 orders (batches) with the lines that are not late, plus the late lines of earlier orders that are due. [{ tick, orders:[order], arrivals:[{ order, lines }] }] */
function schedule(stream, batches = [2, 3, 5, 4], drain = 0) {
  const out = [], due = new Map(); let i = 0, tick = 0;
  const push = (t, order, lines) => { if (!lines.length) return; if (!due.has(t)) due.set(t, []); due.get(t).push({ order, lines }); };
  for (let k = 0; i < stream.length; k++) {
    const batch = stream.slice(i, i + batches[k % batches.length]); i += batch.length; tick++;
    for (const o of batch) { push(tick, o, o.lines.filter(l => !l.delay)); for (const d of new Set(o.lines.filter(l => l.delay).map(l => l.delay))) push(tick + d, o, o.lines.filter(l => l.delay === d)); }
    out.push({ tick, orders: batch, arrivals: due.get(tick) || [] });
  }
  const last = Math.max(tick, ...due.keys());
  for (tick++; tick <= last + drain; tick++) out.push({ tick, orders: [], arrivals: due.get(tick) || [] });
  return out;
}

/* ═══════════════════════════ the page's own rules, sliced from the files of `root` ═══════════════════════════ */
const read = (root, f) => fs.readFileSync(path.join(root || REPO, f), 'utf8');
const sliceBetween = (src, from, to, what) => { const a = src.indexOf(from), b = src.indexOf(to, a + 1); if (a < 0 || b < 0) throw new Error('setsim: cannot find ' + (what || from) + ' in the page code (the page was reorganised: update tests/charm-nest/setsim-lib.cjs)'); return src.slice(a, b); };

/** The page functions of nesting and feeding, in a vm with stubs for everything that draws, saves or talks to the cloud. `extra`: more files (relative to root) whose text is run in the same vm, e.g. a module a worker adds. */
function pageWorld(root, ctl, extra = []) {
  const html = read(root, 'charm-nest-1.html'), bridge = read(root, 'charm-nest-bridge.js');
  const O = require(path.join(root || REPO, 'charm-nest-orders.js'));
  const sheets = Object.fromEntries(METALS.map(m => [m, { pages: [], runId: 'run-1', active: 0 }]));
  const ctx = {
    window: { B: { run: { runId: 'run-1', status: 'running' } }, Gate: { holding: () => false }, CN: {} }, S: { settings: { maxFill: .8, clearancePt: -.5, insetPt: 1.5, budgetS: 180, runMode: 'auto', engine: 'solver' }, sheets },
    Set, Map, WeakMap, Math, JSON, Object, Array, String, Number, Date, console, Promise,
    CharmNestRose: require(path.join(root || REPO, 'charm-nest-rose.js')), CharmNestSolver: L.solverOf(root), O, Sets: { ofRun: () => [] },
    log() {}, agent: () => null, toast() {}, renderCard() {}, renderRail() {}, updateTopSub() {}, computeSaturation() {}, labelOf: m => m, fmt: { pct: x => Math.round(100 * x) + '%', s: x => x + ' ms' },
    activeCharms: sh => sh.charms.filter(c => !c.excluded), allSheets: () => METALS.flatMap(m => sheets[m].pages), manualSheetClosed: () => false, pagesOf: m => sheets[m].pages,
    addPage: metal => ctl.addPage(metal), startNest: sh => ctl.startNest(sh),
  };
  ctx.window.SimClock = { now: () => (ctl.tick || 0) * 600000 };   // (NOSPREAD, Paul 17 Oct 2026: an update is 10 simulated minutes: a sheet that turned a whole order away is nested once more after 30 quiet minutes, nospreadSettle)
  ctx.window.CN.topupRoom = sh => ctx.topupRoom(sh);
  ctx.window.CN.topupTakes = (sh, pieces, order) => typeof ctx.topupTakes === 'function' ? ctx.topupTakes(sh, pieces, order) : true;   // (NESTQUEUE: a multi-piece order that cannot fit a gap-filling sheet whole goes on; NOSPREAD: asked with the whole order)
  // (NOSPREAD, Paul 17 Oct 2026: the pure policy module, when the root has it; an older root has none, and the page code then does what it did)
  try { const NS = require(path.join(root || REPO, 'charm-nest-nospread.js')); ctx.CharmNestNoSpread = NS; ctx.window.CharmNestNoSpread = NS; } catch (e) { if (e.code !== 'MODULE_NOT_FOUND') throw e; }
  ctx.stockFor = () => ctl.stock; ctx.angleSet = () => ctl.angles();
  vm.createContext(ctx);
  // pairs and groups, keepOrdersWhole, overflowToNextSheet, then sheetFull / the gap fill (TOPUP, topupSettle, topupRoom ...), then the feed (feedTurn, nestItems, feedOn)
  const ran = [], run = text => { ran.push(text); vm.runInContext(text, ctx); };
  run(sliceBetween(html, 'function groupOf(', 'function inflatedArea(', 'groupOf .. overflowToNextSheet'));
  run(sliceBetween(html, 'function inflatedArea(', 'function usableArea(', 'inflatedArea .. topupRoom'));
  run(sliceBetween(html, 'function usableArea(', '/** ρ_target', 'usableArea'));   // (the sheet's usable area: keepOrdersWhole reads it)
  run(sliceBetween(html, 'const FEED_ORDERS', '/* A stopped run starts none of its sheets', 'the feed'));
  // the solver job exactly as the page builds it (buildJob and what it reads); the runner only replaces the angles and the budget
  run(sliceBetween(html, 'const CAREFUL_ANGLES', 'function packingKey(', 'buildJob'));
  // the intake: which sheet an order's pieces go on (LiveNest.intakePage and the closed test it uses)
  run(sliceBetween(bridge, 'function prepareSheet(sh) {', 'async function add(run) {', 'LiveNest.prepareSheet .. intakePage'));   // (prepareSheet pins what a sheet already holds, as saved pieces: arrivalPin)
  for (const f of extra) run(read(root, f));
  // a page function the sliced code asks for with `typeof name === "function"` and that is not in this world is silently skipped by the page code: say so, so that a skipped
  // rule is never read as a failing one (a worker who adds such a helper outside the slices tells SETSIM, or the slice list here grows)
  ctl.missing = [...new Set(ran.flatMap(t => [...t.matchAll(/typeof\s+([A-Za-z_$][\w$]*)\s*(?:===|!==|==|!=)\s*["']function["']/g)].map(m => m[1])))].filter(n => { try { return vm.runInContext('typeof ' + n, ctx) !== 'function'; } catch (_) { return true; } });
  if (ctl.missing.length && !process.env.SETSIM_QUIET) console.error('setsim: page functions asked for with typeof but not in the simulated world (their rules are skipped here): ' + ctl.missing.join(', '));
  // every time a sheet passes orders on to the next one, who and from where (the runner reports it: orders passed over, hops)
  const passOn = ctx.overflowToNextSheet; ctl.passes = [];
  ctx.overflowToNextSheet = (sh, announce) => { const ids = new Set(sh.rejects || []); const orders = [...new Set((sh.charms || []).filter(c => ids.has(c.id)).map(c => c.order))]; const next = passOn(sh, announce); if (orders.length) ctl.passes.push({ tick: ctl.tick || 0, metal: sh.metal, from: sh.page, to: next && next.page, orders }); return next; };
  // (the page's `const`s are not properties of the vm: the two the runner calls are handed out here)
  ctx.closed = vm.runInContext('closed', ctx);
  return ctx;
}

const stockOf = sheet => { const [w, h] = String(sheet || '60x40').split('x').map(Number); return { wPt: PT(w), hPt: PT(h) }; };
const CAREFUL_ANGLES = step => Array.from({ length: Math.round(360 / step) }, (_, i) => i * step);

/** One metal's sheets, fed the way the sorter feeds them: each simulated update brings the metal's part of 2-5 orders; the intake puts every order's pieces on a sheet whole
    (LiveNest.intakePage), a search places the oldest three orders (feedTurn) and is judged by the page's rules; a sheet that is full, or whose gap fill is over, passes what is left to the next sheet. */
class MetalSim {
  constructor(root, metal, sim) {
    this.root = root; this.metal = metal; this.sim = sim; this.S = L.solverOf(root); this.cut = CUT.has(metal); this.fast = !this.cut;
    this.stock = stockOf(sim.opts.sheet); this.work = []; this.searches = []; this.ms = 0; this.tick = 0;
  }
  get pages() { return this.sim.ctx.S.sheets[this.metal].pages; }
  newPage() { return { metal: this.metal, runId: 'run-1', page: Math.max(0, ...this.pages.map(p => p.page)) + 1, charms: [], placements: [], rejects: [], status: 'ready', intakePhase: 'fill', log: [], movedFrom: [], searches: 0, ms: 0, born: this.tick }; }
  addPage() { const p = this.newPage(); this.pages.push(p); return p; }
  startNest(sh) { if (!this.work.includes(sh)) this.work.push(sh); }
  /** The page's own buildJob, with the angles and budget of this simulation. */
  buildJob(sh) {
    const o = this.sim.opts, job = this.sim.ctx.buildJob(sh);
    return Object.assign(job, { angles: CAREFUL_ANGLES(o.step), timeBudgetMs: 180000, seed: o.seed || 1 }, o.job || {});
  }
  async nest(sh) {
    const ctx = this.sim.ctx, S = this.S;
    let items = ctx.activeCharms(sh);
    if (!items.length) return;
    ctx.prepareSheet(sh); sh.dirty = false;   // (startNest does this for a sheet of a run: fill phase, and what is already placed is pinned as a saved piece, arrivalPin; the sheet is no longer waiting to be nested)
    items = ctx.feedTurn(sh, items);
    const onSheet = new Set(items.map(c => c.id)), kept = (sh.placements || []).filter(p => onSheet.has(p.id));
    sh.nestInitial = kept.length ? kept.map(p => ({ ...p })) : null;
    sh.placements = kept.map(p => ({ ...p })); sh.rejects = [];
    sh.runSeed = this.sim.opts.seed || 1; const job = this.buildJob(sh), t0 = Date.now();
    const result = await S.solve(job, {});
    const ms = Date.now() - t0; sh.ms += ms; sh.searches++; this.searches.push({ page: sh.page, ms, n: items.length - kept.length, placed: result.placements.length - kept.length, rejects: result.rejects.length });
    sh.status = 'finishing'; sh.endedBy = result.endedBy; sh.placements = result.placements.map(p => ({ ...p })); sh.rejects = result.rejects.slice();
    const byId = new Map(items.map(c => [c.id, c]));
    ctx.keepOrdersWhole(sh, byId, new Set(result.pocketFilled || []));
    // (finishNest: pieces the making-room pass moved are saved at their new spots, and the check builds the job again from the sheet as it now stands)
    if (result.unjam && result.unjam.moved && result.unjam.moved.length && sh.nestInitial) { const moved = new Set(result.unjam.moved), now = new Map(sh.placements.map(p => [p.id, p])); sh.nestInitial = sh.nestInitial.map(p => moved.has(p.id) && now.has(p.id) ? { ...now.get(p.id), scale: p.scale } : p); }
    sh.verification = { ok: S.verify(this.buildJob(sh), sh.placements, 4).ok };
    if (!sh.verification.ok) throw new Error('layout failed verification on ' + this.metal + ' page ' + sh.page);
    sh.liveInfo = { freePt2: result.freePt2, usablePt2: result.usablePt2, placedPt2: result.placedPt2, pocket: result.pocket, placed: result.placements.length, total: items.length };
    sh.result = result; sh.density = result.density; sh.usablePt2 = result.usablePt2; sh.freePt2 = result.freePt2; sh.placedPt2 = result.placedPt2;
    sh.releaseFull = ctx.topupSettle(sh, result, ctx.sheetFull(sh, result, items)) || !!(sh.keepRelease && sh.keepRelease.full);
    if (sh.releaseFull && !sh.releasedAt) sh.releasedAt = this.tick;
    sh.status = sh.rejects.length ? 'partial' : 'complete';
    // (NOSPREAD, Paul 17 Oct 2026: the sheet is written and saved, as finishNest leaves it: the one-off nest of a sheet that turned a whole order away needs a sheet at rest, nospreadSettle)
    sh.persistedDone = true; sh.sheetId = sh.sheetId || 'sim-' + sh.metal + sh.page; sh.fileBase = sh.fileBase || 'sim-' + sh.page; sh._written = { key: JSON.stringify([sh.page, sh.placements.map(p => [p.id, p.cxPt, p.cyPt, p.angle].join()).sort()]) }; sh.nests = (sh.nests || 0) + 1;
    if (sh.rejects.length && sh.endedBy !== 'stopped') ctx.overflowToNextSheet(sh, false);
    ctx.feedOn(sh);
  }
  /** One simulated update: the metal's parts of the orders that arrived come in, and every sheet that took pieces is searched until nothing is left to place. */
  async step(parts, tick) {
    const ctx = this.sim.ctx, t0 = Date.now();
    this.intake(parts, tick);
    // (the sheet the intake chose first was empty at the start of a stream: the page nests the sheet the batch landed on)
    while (this.work.length) { const sh = this.work.shift(); await this.nest(sh); }
    // (NOSPREAD, Paul 17 Oct 2026: the page's tick, settleTopups, brings a sheet that turned a whole order away to one more nest, once; a page without the policy has no such function)
    for (let k = 0; k < 3 && typeof ctx.nospreadSettle === 'function'; k++) { ctx.nospreadSettle(); if (!this.work.length) break; while (this.work.length) { const sh = this.work.shift(); await this.nest(sh); } }
    this.ms += Date.now() - t0;
  }
  /** The intake of one update (attachPool, as the page has it): each line of the parts goes onto the sheet the page picks, and that sheet waits to be nested (work). Nothing is nested here: step() nests,
      and a driver that plays the live timing (tests/charm-nest/nospread-hard.cjs: the next update arrives while a search is out) calls it on its own. */
  intake(parts, tick) {
    this.tick = tick; this.sim.ctl.tick = tick; const ctx = this.sim.ctx, run = { runId: 'run-1' };
    if (!this.pages.length) this.pages.push(this.newPage());
    // (NOSPREAD, Paul 17 Oct 2026: an order with a line still to come is put on its sheet after the others of the update, as the bridge's addRows does)
    const behind = part => !!(part.order && part.order.expected > part.order.pieces.length + (part.order.placed || 0) && typeof ctx.nospread === 'function' && ctx.nospread());
    for (const part of parts.map((x, i) => [x, i]).sort((a, b) => behind(a[0]) - behind(b[0]) || a[1] - b[1]).map(x => x[0])) for (const line of part.lines) {
      // (NOSPREAD, Paul 17 Oct 2026: the intake is told the order the line belongs to, as attachPool tells it: the order's pieces of this metal that came in with this update, and the number the order will have in all, late lines included)
      const info = part.order || { pieces: part.pieces, expected: part.expected || part.pieces.length };
      if (typeof ctx.nospreadChunk === 'function' && !info.pieces.some(c => c.chunk)) ctx.nospreadChunk(this.metal, info.pieces, info);   // (an order bigger than a sheet is cut into units that each fit one: attachPool does the same)
      let page = ctx.intakePage(this.metal, run, part.id, line.pieces, info);
      // (R16 HARD, Paul 26 Oct 2026: as attachPool keeps it, the order's own sheet of this colour although it was released, LiveNest.orderHome, kept released while it nests them, reopenHome)
      const back = typeof ctx.orderHome === 'function' ? ctx.orderHome(this.metal, run, part.id, { skip: line.pieces }) : null;
      if (back) page = back;
      if (!page || (ctx.closed(page) && page !== back)) page = this.addPage();      // (attachPool: a closed sheet is never joined, but for the order's own)
      if (back && page === back && typeof ctx.reopenHome === 'function') ctx.reopenHome(page, part.id);
      for (const c of line.pieces) page.charms.push(Object.assign({}, c, { pinned: null }));
      page.appendOnly = page.placements.length > 0; page.intakeAppend = page.appendOnly;
      this.startNest(page);
    }
  }
}

/** The metals of a simulation, in lockstep: the page world is shared (one vm), each metal has its own sheets. */
class Sim {
  constructor(opts = {}) {
    this.opts = Object.assign({ root: REPO, step: 24, seed: 1, sheet: '60x40', extra: [] }, opts);
    // (the solver's pocket pass and making-room caps are wall-clock seconds: on a busy machine they would cut the searches short and make runs differ; generous caps keep a run what an idle machine would give)
    this.opts.job = Object.assign({ pocketMs: 120000, unjamMs: 90000 }, this.opts.job || {});
    this.metals = {};
    this.ctl = { addPage: m => this.metals[m].addPage(), startNest: sh => this.metals[sh.metal].startNest(sh), get stock() { return stockOf(self.opts.sheet); }, angles: () => CAREFUL_ANGLES(self.opts.step) };
    const self = this;
    this.ctx = pageWorld(this.opts.root, this.ctl, this.opts.extra);
  }
  metal(m) { return this.metals[m] || (this.metals[m] = new MetalSim(this.opts.root, m, this)); }
  get pages() { return METALS.flatMap(m => (this.metals[m] ? this.metals[m].pages : [])); }
}

/* ═══════════════════════════ the sets: the page's own assembly over the sheets the nesting made ═══════════════════════════ */
function setWorld(root, extraStubs = '') {
  const O = require(path.join(root || REPO, 'charm-nest-orders.js')), SO = require(path.join(root || REPO, 'charm-nest-shared-orders.js')), SR = require(path.join(root || REPO, 'charm-nest-set-rules.js')), SN = require(path.join(root || REPO, 'charm-nest-sheet-name.js'));
  const code = read(root, 'charm-nest-bridge.js'), gateAt = code.indexOf('const Gate = window.Gate ='), GATE = code.slice(gateAt, code.indexOf('/* ═══ 21', gateAt));
  if (gateAt < 0 || !GATE) throw new Error('setsim: cannot find the Gate in the bridge');
  const RUN = 'run-1', writes = [];
  // (SETFIX, Paul 17 Oct 2026: the owner's rules as a checker, charm-nest-set-invariants.js, run by the Gate itself after every assembly; a checkout from before it has no checker and simply has no verdict)
  let II = null; try { II = require(path.join(root || REPO, 'charm-nest-set-invariants.js')); } catch (_) { II = null; }
  const c = vm.createContext({ O, SO, SR, SN, II, console, RUN, allocations: 0, writes });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSetRules:SR,CharmNestSheetName:SN,CharmNestSetInvariants:II}, S={cloud:{ok:true},settings:{}}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
const CharmNestSheetName=SN;
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[]; let seq=1;
const allSheets=()=>pages, labelOf=m=>m, today=()=> '2026-10-13', refreshAllCards=()=>{};
const api=async(n,b)=>{writes.push({n,b});return {}};
const Orders={rows:()=>B.orders.rows}, Engrave={items:()=>{throw new Error('forming a set must not read the back engravings')},writeBacks:async()=>{},saveSheetBacks:async()=>{}};
const Pool={update:async(ids,p)=>writes.push({ids,p})};
const RunCtl={onSheetDone(){}};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,persistSheet:()=>{throw new Error('must not re-render the artwork')},loadLibrary:async()=>{}};
const Sets={onSheetSaved:async(sh,_c,_u,o)=>{const set=o&&o.setOverride;if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);},labelsReady:()=>true,ofRun:()=>[...B.sets.values()].sort((a,b)=>a.seq-b.seq),save:async()=>{},
  ensure:async(runId,group,o)=>{allocations++;const s={runId,group,seq:seq++,day:today(),setId:'set-'+allocations,name:'Set-'+(seq-1),sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{}};B.sets.set(s.setId,s);return s}};
${extraStubs}
`, c);
  vm.runInContext(GATE, c);
  const byKey = new Map();   // sheet key (metal + page) -> the page object the Gate holds
  return {
    c, SO, SR,
    /** The sheets the nesting holds now, as the Gate sees saved sheets (a sheet with nothing placed is not one). */
    sync(simPages, isPartial) {
      for (const p of simPages) {
        if (!p.placements.length) continue;
        const key = p.metal + '#' + p.page, onSheet = p.placements.map(q => p.charms.find(x => x.id === q.id)).filter(Boolean);
        let g = byKey.get(key);
        if (!g) { g = { metal: p.metal, page: p.page, runId: RUN, sheetId: key, draft: true, outputs: { ai: 'a' }, persistedDone: true, verification: { ok: true } }; byKey.set(key, g); c.__p = [g]; vm.runInContext('pages.push(...__p)', c); }
        Object.assign(g, { status: 'complete', density: p.usablePt2 ? 1 - p.freePt2 / p.usablePt2 : p.density, placements: p.placements.map(q => ({ id: q.id })), charms: onSheet.map(x => ({ id: x.id, poolId: x.poolId, order: x.order, side: x.side })), releaseFull: !!p.releaseFull });
      }
    },
    gatePages: () => vm.runInContext('pages', c),
    /** a page the Gate holds already built (the incremental scenario adds sheets one by one) */
    addPage(pg) { c.__p = [pg]; vm.runInContext('pages.push(...__p)', c); return pg; },
    /** the Gate's own self-check (the owner's rules, rule by rule), or null when this checkout has none */
    selfCheck: () => II ? JSON.parse(vm.runInContext('JSON.stringify(Gate.selfCheck ? Gate.selfCheck(B.run) : null)', c)) : null,
    /** the sheets as the checker reads them (Gate.invariantInput), or null */
    checkerInput: () => II ? JSON.parse(vm.runInContext('JSON.stringify(Gate.invariantInput ? Gate.invariantInput(B.run) : null)', c)) : null, II,
    assemble: () => vm.runInContext('Gate.assemble(B.run)', c),
    sets: () => JSON.parse(vm.runInContext('JSON.stringify([...B.sets.values()].map(s=>({name:s.name,seq:s.seq,setId:s.setId,ids:s.sheetIds.slice()})))', c)),
    allocations: () => c.allocations, writes,
  };
}

/* ═══════════════════════════ what is measured ═══════════════════════════ */
/** Fill of a sheet as its card shows it (charms' share of the usable stock). */
const fillOf = p => p.usablePt2 ? 1 - p.freePt2 / p.usablePt2 : (p.density || 0);

/** How the front of a sheet lies (nest-front-bench.cjs): the standard deviation of the rows' reach (mm; 0 = straight) and the widest empty disc behind the front (mm). */
function frontOf(S, stock, page) {
  const FW = Math.round(stock.wPt * 2), FH = Math.round(stock.hPt * 2), occ = new Uint8Array(FW * FH), far = new Int32Array(FH), byId = new Map(page.charms.map(c => [c.id, c])), MMC = MM / 2, band = 3;
  for (const p of page.placements) {
    const v = S.prepareVariant(byId.get(p.id), p.angle, -.5, 2), x0 = Math.round(p.cxPt * 2 - v.solid.cx), y0 = Math.round(p.cyPt * 2 - v.solid.cy);
    for (let r = 0; r < v.fine.h; r++) for (let q = 0; q < v.fine.w; q++) if (v.fine.bits[r * v.fine.w + q]) { const x = x0 + q, y = y0 + r; if (x >= 0 && y >= 0 && x < FW && y < FH) { occ[y * FW + x] = 1; if (x + 1 > far[y]) far[y] = x + 1; } }
  }
  const rows = []; for (let y = band; y < FH - band; y++) rows.push(far[y]);
  const mean = rows.reduce((a, b) => a + b, 0) / rows.length, std = Math.sqrt(rows.reduce((a, b) => a + (b - mean) ** 2, 0) / rows.length), sorted = rows.slice().sort((a, b) => a - b), xf = Math.max(band + 1, sorted[Math.floor(.9 * (sorted.length - 1))]);
  const blocked = new Uint8Array(FW * FH);
  for (let y = 0; y < FH; y++) for (let x = 0; x < FW; x++) { const usable = x >= band && x < FW - band && y >= band && y < FH - band; if (!usable || x >= xf || occ[y * FW + x]) blocked[y * FW + x] = 1; }
  const d = new Int32Array(FW * FH), INF = 1e9; for (let i = 0; i < d.length; i++) d[i] = blocked[i] ? 0 : INF;
  for (let y = 0; y < FH; y++) for (let x = 0; x < FW; x++) { const i = y * FW + x; if (!d[i]) continue; let m = d[i]; if (x > 0) m = Math.min(m, d[i - 1] + 3); if (y > 0) { m = Math.min(m, d[i - FW] + 3); if (x > 0) m = Math.min(m, d[i - FW - 1] + 4); if (x < FW - 1) m = Math.min(m, d[i - FW + 1] + 4); } d[i] = m; }
  for (let y = FH - 1; y >= 0; y--) for (let x = FW - 1; x >= 0; x--) { const i = y * FW + x; if (!d[i]) continue; let m = d[i]; if (x < FW - 1) m = Math.min(m, d[i + 1] + 3); if (y < FH - 1) { m = Math.min(m, d[i + FW] + 3); if (x < FW - 1) m = Math.min(m, d[i + FW + 1] + 4); if (x > 0) m = Math.min(m, d[i + FW - 1] + 4); } d[i] = m; }
  let mx = 0; for (let i = 0; i < d.length; i++) if (d[i] > mx) mx = d[i];
  return { frontStd: +(std * MMC).toFixed(2), cavernMm: +(2 * (mx / 3) * MMC).toFixed(1) };
}

const mean = a => a.length ? a.reduce((s, x) => s + x, 0) / a.length : null;
const r1 = (x, k = 1) => x == null ? null : +(+x).toFixed(k);

/** The nesting side of a finished (or running) simulation: where every order's pieces are. */
function placementOf(sim, stream, metals) {   // (`metals`: the metals the run nests; pieces of other metals are not on any sheet of the run and are not counted)
  const mine = o => piecesOf(o).filter(c => !metals || metals.includes(c.metal));
  const where = new Map();   // order id -> Map(metal -> Set(page)), and pieces seen
  for (const p of sim.pages) for (const q of p.placements) {
    const c = p.charms.find(x => x.id === q.id); if (!c) continue;
    let m = where.get(c.order); if (!m) where.set(c.order, m = new Map());
    let s = m.get(p.metal); if (!s) m.set(p.metal, s = new Set()); s.add(p);
  }
  const placed = new Set(sim.pages.flatMap(p => p.placements.map(q => q.id)));
  const out = { orders: stream.filter(o => piecesOf(o).some(c => !metals || metals.includes(c.metal))).length, splitSameMetal: [], spreadMetals: 0, ordersMultiPiece: 0, unplaced: [], pairsSplit: 0 };
  for (const o of stream) {
    const n = mine(o).length; if (!n) continue; if (n > 1) out.ordersMultiPiece++;
    const m = where.get(o.id) || new Map();
    if (m.size > 1) out.spreadMetals++;
    for (const [metal, pages] of m) if (pages.size > 1) out.splitSameMetal.push({ order: o.id, metal, pieces: n, pages: [...pages].map(p => p.page), kinds: o.lines.filter(l => l.metal === metal).map(l => l.kind) });
    const missing = mine(o).filter(c => !placed.has(c.id)); if (missing.length) out.unplaced.push({ order: o.id, missing: missing.length, of: n });
    for (const l of o.lines) if ((l.kind === 'pair' || l.kind === 'mismatched' || l.kind === 'huggie') && l.pieces.every(c => placed.has(c.id))) { const pg = new Set(sim.pages.filter(p => p.placements.some(q => l.pieces.some(c => c.id === q.id)))); if (pg.size > 1) out.pairsSplit++; }
  }
  return out;
}

module.exports = { METALS, CODE, CUT, DEFAULT_MIX, makeStream, lineOf, orderOf, piecesOf, partOf, schedule, pageWorld, MetalSim, Sim, setWorld, fillOf, frontOf, placementOf, mean, r1, REPO, L };
