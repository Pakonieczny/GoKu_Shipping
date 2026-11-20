/* NestPlan: the pure helpers of a sheet's nest, one copy for the page and the cloud (Charm Nest cloud architecture v2, build step 6: moved out of charm-nest-1.html, 20 Nov 2026).
 *
 * Why. The owner's goal is that no production work runs on any computer: the cloud nests the sheets, and a page only shows the cloud and sends a person's presses. The cloud
 * can only nest a sheet the way the page does if it builds the SAME solver job, feeds the SAME orders per turn, keeps the SAME orders whole and runs and picks the SAME
 * searches. Those rules lived inside charm-nest-1.html. They live here now, and the page calls them (charm-nest-1.html keeps a thin function of the same name for each, so
 * every caller, test and other script that knew the page's name still finds it). Nothing in the page's behaviour changes.
 *
 * What this file is. Pure: no DOM, no network, no page state, no clock. Numbers and plain objects in, a job or a decision out; the few helpers that change a sheet (feedTurn
 * sets sh.feedWait, keepOrdersWhole lifts placements, the KEEPSEARCH bookkeeping moves its counters) change only what they are handed. Everything the page used to read from its
 * own globals comes in `env`:
 *   env.settings              the Settings (the page's S.settings): clearancePt, insetPt, budgetS, seed, maxFill, engine
 *   env.stockFor(metal, sh)   the sheet's stock { wPt, hPt }                 env.cornerPt(sh)   the corner keep-out of the sheet (the page's cornerPtSetting)
 *   env.angleSet()            the angles of the Settings' step              env.activeCharms(sh)   the charms on the sheet less those taken out (default: not excluded)
 *   env.cores                 the processors a search may use (the page: navigator.hardwareConcurrency)
 *   env.Solver, env.Rose, env.Orders    CharmNestSolver, CharmNestRose, CharmNestOrders (node: require('./charm-nest-solver.js') and the others)
 *   env.groupOf, env.nospread, env.nospreadAdmitted, env.topupRoom, env.usableArea, env.inflatedArea    the page's own rules a helper reads (absent: that rule is left out, as the page did)
 *   env.agent(scope, kind, text)   the activity log
 * and, from the page only, its own binding of each helper kept here (orderKey, orderRank, gapFilling, carefulNest, carefulAngles, nestItems, turnOrder, orderSizes, manualSize,
 * handLocks, withHandLocks, orderMatesOf). The page passes them so that a test that stands in for one of the page's functions, as the page's tests do, is still heard; the
 * cloud passes none and the ones below are used.
 * Tests: tests/charm-nest/nest-plan.cjs (the module builds the same job as the page for fixtures, in node and against the real page in Chromium); the page's own suites run
 * the page's functions, which now call this file.                                                                                                                             */
(function (root, factory) { const api = factory(); if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestNestPlan = api; })(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  /** After nesting, every charm is written and previewed 2.9 % smaller than drawn, scaled about its own centre: the layout is solved at full size and the shrink afterwards makes minor overlaps disappear.
      (Paul, 25 Oct 2026: "Increase the 2.5% to 2.9%"; was 0.975.) Every place that writes or draws a placed charm reads this one number. */
  const CHARM_SCALE = 0.971;
  /** One charm at a time on every sheet (Paul, 24 Sep): Gold, Silver, Rose Gold past its green line, a sheet's first charms as much as later ones. Each is graded spot by spot
      at 2° steps with the others looked ahead, nudged into place, and never moved again; a sheet is never re-arranged, except that an order turned away from a sheet with
      room left lifts the few newest charms around the bottleneck and seats them again with it (9 Oct, "making room"). */
  const CAREFUL_ANGLES = Array.from({ length: 180 }, (_, i) => i * 2);
  /** A big batch goes on a Gold or Silver sheet of a run three orders at a time (feedTurn). */
  const FEED_ORDERS = 3;
  /** keepOrdersWhole lifts the saved pieces of an order that missed only when the whole order fits one empty sheet: its area under this share of the sheet's usable area at the fill ceiling (orderMatesOf). */
  const ORDER_ROOM = 0.7;
  /** (KEEPSEARCH / SMALLSEARCH, Paul 14 Nov 2026) at most this many finished layouts per fresh 10K, 14K or Rose Gold nest. */
  const KEEP_MAX = 36;
  /** The metals of solid gold (GOLDOPT: the least stock wins) and the metals whose searches are judged tight (TIGHT: each layout carries its dead space). */
  const GOLD_METALS = ["gold10k", "gold14k"], TIGHT_METALS = ["gold10k", "gold14k", "rose"];

  const fnOf = (env, name, own) => (env && typeof env[name] === "function" ? env[name] : own);

  /** The size a person gave a piece by hand in the sheet window (Manual Control, 10K / 14K; Paul, 18 Oct 2026): 1 = its design size, bounded 0.8 to 1.2;
      the piece is nested, checked and written at that size (buildJob, finishNest). (CUSTOMPOOL, 28 Oct 2026: a custom design sent at its exact size has no other: c.frozen) */
  function manualSize(c) { const v = c && c.manual && !c.frozen ? +c.manual.size : 0; return v >= 0.8 && v <= 1.2 ? v : 1; }
  /** A sheet nests one charm at a time unless the learned flow runs it. */
  const carefulNest = sh => sh.nestFlow !== "learned";
  /** The unit the nest keeps whole: an order, or one chunk of an order bigger than a sheet (NOSPREAD, Paul 17 Oct 2026: `chunk`, 1.. in the order they are seated; the order itself, `c.order`, stays what the sets share). */
  const orderKey = c => String(c.order || c.id) + (c.chunk ? "~" + String(c.chunk).padStart(3, "0") : "");
  /** The date the nest's oldest-first rule reads for a piece: its order's date, and for a piece of an order released from hold (frontAt) a time ahead of every real order date,
      so it is placed first, in the order the holds were released (Paul, 5 Oct). `Orders`: CharmNestOrders (its rankDate); without it, the order's own date. */
  function orderRank(c, Orders) { return Orders && Orders.rankDate ? Orders.rankDate(c) : +(c && c.orderDate) || 0; }
  /** A Gold or Silver sheet filling its gaps (see the page's topupSettle). */
  const gapFilling = sh => !!sh && !!sh.topup && !sh.topup.closedAt && ["gold", "silver"].includes(sh.metal);
  /** How many pieces each order of `charms` has: Map order -> count. */
  function orderSizes(charms, env) { const key = fnOf(env, "orderKey", orderKey), by = new Map(); for (const c of charms) { const k = key(c); by.set(k, (by.get(k) || 0) + 1); } return by; }
  /** The order the orders of a sheet take their turn in: oldest first. A sheet filling its gaps takes the single-charm orders first (oldest
      first among them) and the multi-piece orders after, so its few tries left go to the orders that fit a small gap (Paul, 13 Oct 2026). */
  function turnOrder(sh, items, env) {
    const rank = fnOf(env, "orderRank", c => orderRank(c, env && env.Orders)), key = fnOf(env, "orderKey", orderKey), gap = fnOf(env, "gapFilling", gapFilling), sizes = fnOf(env, "orderSizes", ch => orderSizes(ch, env));
    const age = (a, b) => rank(a) - rank(b) || key(a).localeCompare(key(b));
    if (!gap(sh)) return age;
    const size = sizes(items);
    return (a, b) => ((size.get(key(a)) > 1) - (size.get(key(b)) > 1)) || age(a, b);
  }
  /** The charms on the sheet less those taken out (the page's activeCharms). */
  const activeCharms = sh => sh.charms.filter(c => !c.excluded);
  /** The charms on the sheet less those waiting for their turn: what the search places, is judged by and writes. */
  function nestItems(sh, env) {
    const items = fnOf(env, "activeCharms", activeCharms)(sh); if (!sh.feedWait?.length) return items;
    const later = new Set(sh.feedWait); return items.filter(c => !later.has(c.id));
  }

  /* A big batch goes onto a sheet a few orders at a time, oldest first, the way orders come in from Etsy (Paul, 25 Sep:
     "the charms were coming in a few at a time and then the system was placing them"). A run that took every open order
     put 138 charms on one sheet, and its one-by-one fill weighed them all together, drawn as masks, before the sheet was
     written. Now the oldest few orders are placed and the sheet written, then the next few come in (the page's feedOn); the charms of
     the later orders wait on the sheet for their turn, as later orders wait at Etsy, and each few is placed exactly as an
     update's new orders are, gap fill included. Gold and Silver of a run. Rose Gold, 10K and 14K (the metals with a green
     line) keep their own flow: the whole batch is placed in one search and built as one block from the sheet's start, as
     Rose Gold is (Paul, 8 Oct: "the 14K gold sheet is a mess because the sheet was not initially forced to be aligned like
     the RG 14/20"). Three orders at a time never saw the rest of the batch, and each little group was seated against the last one. */
  /** The charms this search places, and those it leaves waiting for their turn (sh.feedWait). */
  function feedTurn(sh, items, env) {
    env = env || {};
    const careful = fnOf(env, "carefulNest", carefulNest), key = fnOf(env, "orderKey", orderKey), gap = fnOf(env, "gapFilling", gapFilling), turnOf = fnOf(env, "turnOrder", (s, it) => turnOrder(s, it, env));
    sh.feedWait = null;
    if (!careful(sh) || sh.metal === "rose" || (env.Rose ? env.Rose.cuts(sh.metal) : false) || !sh.runId || (env.settings.engine || "solver") === "ai") return items;
    const placed = new Set(sh.placements.map(p => p.id));
    const fresh = items.filter(c => !placed.has(c.id)).sort(turnOf(sh, items));
    // a sheet filling its gaps: the orders it still owes a try try it together, in one search (topupRoom)
    // (NOSPREAD, Paul 17 Oct 2026, B: a multi-piece order that cannot fit whole is no try of a sheet filling its gaps: it waits here only to be passed on whole to the next sheet, feedOn)
    const names = [...new Set(fresh.map(c => key(c)))], mine = new Map(); for (const c of fresh) { const k = key(c); if (!mine.has(k)) mine.set(k, []); mine.get(k).push(c); }
    const stays = gap(sh) && typeof env.nospread === "function" && env.nospread() ? env.nospreadAdmitted(sh, new Map(names.map(k => [k, mine.get(k)]))) : null, tryable = stays ? names.filter(k => stays.has(k)) : names;
    const turn = new Set(tryable.slice(0, sh.topup && !sh.topup.closedAt ? Math.max(1, env.topupRoom(sh, false)) : FEED_ORDERS));
    // a pair (a line of pieces) with a piece already placed on this sheet has its other pieces come in this turn, whatever their order's rank:
    // they join their mate's sheet and are never left waiting to be passed on alone (rule R2; a line with nothing placed is unchanged)
    // The same for any piece of an ORDER with a piece already placed on this sheet (another line of the order, a copy that came in after its mates were saved;
    // Paul, 13 Oct 2026: all the pieces of an order on one sheet): it takes this turn with them, not the order's place in line.
    const gid = typeof env.groupOf === "function" ? env.groupOf : () => null, placedLines = new Set(items.filter(c => placed.has(c.id)).map(gid).filter(Boolean)), placedOrders = new Set(items.filter(c => placed.has(c.id)).map(c => key(c)));
    if (placedLines.size || placedOrders.size) for (const c of fresh) if (!turn.has(key(c)) && (placedOrders.has(key(c)) || placedLines.has(gid(c)))) turn.add(key(c));
    const wait = fresh.filter(c => !turn.has(key(c)));
    if (!wait.length) return items;
    sh.feedWait = wait.map(c => c.id);
    const later = new Set(sh.feedWait);
    return items.filter(c => !later.has(c.id));
  }

  /* ZEROGAP (Paul, 27 Oct 2026: a charm placed by hand in Manual Control may sit right against its neighbour or the sheet's edge, at any pose a 0.05 mm nudge gives): a nest that starts the
     sheet afresh holds each such charm EXACTLY where the person put it, as a locked placement (the solver's append keeps those as they are and nests the rest round them), never seated again
     on the nest's own half-point grid with its edge margin, where it could be moved a hair or turned away. Only charms Manual Control placed (c.manual with the person's pin); null when none. */
  function handLocks(sh, items, env) {
    const Sv = (env && env.Solver) || null, sizeOf = fnOf(env, "manualSize", manualSize), out = [];
    for (const c of items || []) {
      const pin = c && c.manual && c.pinned && !c.arrivalPin ? c.pinned : null; if (!pin || ![pin.cxPt, pin.cyPt].every(Number.isFinite)) continue;
      const size = sizeOf(c), angle = ((+pin.angle || 0) % 360 + 360) % 360, scale = c.frozen ? 1 : size !== 1 ? +(CHARM_SCALE * size).toFixed(5) : CHARM_SCALE;   // (CUSTOMPOOL: a custom design sent at its exact size is held at 100 %)
      let wPt = (+c.widthPt || 20) * size, hPt = (+c.heightPt || 20) * size;
      try { const v = Sv && c.bits && Sv.prepareVariant({ bits: c.bits, w: c.w, h: c.h, scale: size === 1 ? c.scale : c.scale / size }, angle, 0, 2); if (v) { wPt = v.solid.w / 2; hPt = v.solid.h / 2; } } catch (_) { /* its own size stands */ }
      out.push({ id: c.id, angle, cxPt: +pin.cxPt, cyPt: +pin.cyPt, xPt: +pin.cxPt - wPt / 2, yPt: +pin.cyPt - hPt / 2, wPt, hPt, contact: 0, scale });
    }
    return out.length ? out : null;
  }
  /** The placements a nest holds still: the saved layout it was given (`base`), and every charm a person placed in Manual Control that it does not list (handLocks); null when none. */
  function withHandLocks(sh, items, base, env) {
    const list = (base || []).slice(), have = new Set(list.map(p => p.id));
    for (const p of fnOf(env, "handLocks", (s, it) => handLocks(s, it, env))(sh, items) || []) if (!have.has(p.id)) list.push(p);
    return list.length ? list : null;
  }
  /** (GOLDOPT, Paul 12 Nov 2026) how many careful searches a fresh 10K, 14K or Rose Gold nest runs side by side: one fewer than the processors, three to six. */
  const multiStartFor = cores => Math.min(6, Math.max(3, (cores || 4) - 1));
  /** The solver job of a sheet: its stock and settings, the pieces it places (nestItems), the placements it holds still, and the way it searches. */
  function buildJob(sh, env) {
    env = env || {};
    const cfg = () => env.settings, sizeOf = fnOf(env, "manualSize", manualSize), Solver = env.Solver || null, Rose = env.Rose || null;
    const ANGLES = () => { const a = env.carefulAngles; return Array.isArray(a) ? a : CAREFUL_ANGLES; }, lockWith = fnOf(env, "withHandLocks", (s, it, base) => withHandLocks(s, it, base, env));
    const key = fnOf(env, "orderKey", orderKey), rank = fnOf(env, "orderRank", c => orderRank(c, env.Orders)), gap = fnOf(env, "gapFilling", gapFilling);
    const st = env.stockFor(sh.metal, sh);
    const items = fnOf(env, "nestItems", s => nestItems(s, env))(sh), careful = fnOf(env, "carefulNest", carefulNest)(sh);
    return {
      sheet: { wPt: st.wPt, hPt: st.hPt, insetPt: +cfg().insetPt || 0, cornerPt: env.cornerPt(sh), ...(sh.roseStock?.profileJson ? { remnant: JSON.parse(sh.roseStock.profileJson) } : {}) },
      clearancePt: +cfg().clearancePt || 0, writeScale: 1 /* (25 Oct 2026: nesting at the written size, CHARM_SCALE, made the append turns of a big file place 11 of 14 pieces on an 11 % sheet; off until that is fixed. The overlap the shrink can cause is cleared by fixOverlaps) */, angles: careful && env.angleSet().length < ANGLES().length ? ANGLES() : env.angleSet(), fineRes: 2, coarseRes: 0.5,
      timeBudgetMs: sh.intakeBudgetMs || (+cfg().budgetS || 180) * 1000, fullBudget: sh.intakePhase !== "fill", stallMs: 60 * 1000, maxTrials: sh.intakePhase === "fill" ? 2000 : 1000000, seed: sh.runSeed || +cfg().seed || 1,
      // charms already on the sheet stay exactly where they are
      protectedRose: sh.roseProtected || null, lockedPlacements: lockWith(sh, items, sh.appendOnly ? sh.nestInitial || sh.placements : careful && sh.nestInitial?.length ? sh.nestInitial : null),
      // a Gold or Silver sheet also reports whether one of the shop's smallest charms could still go in (see the page's topupSettle)
      careful, roomCheck: careful && ["gold", "silver"].includes(sh.metal),
      // (GOLDOPT: solid 10K / 14K gold, small sheets, costly stock: up to six searches side by side, each its own placing order)
      // (TIGHT, Paul 14 Nov 2026: Rose Gold too, and each layout carries its dead space so the least wasteful one is kept; GOLDLOCK (searchCount) still nests a sheet with held pieces the single way)
      ...(TIGHT_METALS.includes(sh.metal) ? { tight: true } : {}),
      ...(careful && TIGHT_METALS.includes(sh.metal) && !sh.appendOnly ? { multiStart: multiStartFor(env.cores) } : {}),
      // an order turned away from a sheet with room left lifts the few newest charms near the bottleneck and seats them again beside it ("making room"; Rose Gold never, see the solver's unjam)
      // (a sheet whose attempts found no arrangement tries less each time, and not at all after three)
      // (INPLACE, Paul 20 Oct 2026: a sheet that takes a replaced charm that did not fit its old spot never moves a charm already on it to make room: sh._keepPlaced)
      unjam: careful && sh.metal !== "rose" && !sh.roseCutAt && !sh._keepPlaced && (sh._unjamFails || 0) < 3, ...(sh._unjamFails ? { unjamMs: 15000 } : {}),
      // every metal goes on as one tight block from the sheet's start, never spread along its edges (see BLOCK_WEIGHTS): Rose Gold, 10K and 14K
      // (the metals with a green line) since September, Gold-filled and Sterling Silver since 11 Oct (Paul: "nest the pieces in a somewhat straight
      // vertical line front exactly like we already do with the RG 14/20 sheet"); a sheet already saved keeps its charms, and its holes are filled first.
      /* GFEASE (Paul, 11 Nov 2026: "the GF and SS sheets should be maintaining the same style of alignment" as Rose Gold): every careful sheet pays
         the block's full price for growing the front, from the first charm to the last, so Gold and Silver keep a straight vertical front too.
         (From 11 Oct Gold and Silver had job.blockEase { from: .45, to: .7, scale: .25 }: the price eased off over the second half of the sheet, for
         a little more density on a sheet nothing is cut from, and the front slanted there. Removed 11 Nov for alignment; the solver still reads
         job.blockEase, nothing asks for it.) */
      block: careful,
      initialLayout: sh.nestInitial || null, maxFill: +cfg().maxFill || 0.80, nearFullContact: !(sh.metal === "rose" || (Rose ? Rose.cuts(sh.metal) : false)),
      // filling its gaps, a sheet takes any later order that fits, not only the oldest; the pockets go to single-charm orders before a multi-piece order that was turned away (solver: singlesFirst)
      ...(gap(sh) ? { singlesFirst: true } : {}),
      // (a piece a person resized by hand, Manual Control: its bitmap is read at that size, so the nest, its check and the written file all see the same piece)
      // (hold: pinned by a person, never lifted; the run's own pin on every placed charm is arrivalPin; pairFields: the group, side and "seat near its mate" of a pair's pieces, nothing for any other charm)
      pieces: items.map(c => ({ id: c.id, w: c.w, h: c.h, scale: sizeOf(c) === 1 ? c.scale : c.scale / sizeOf(c), bits: c.bits, areaPt2: sizeOf(c) === 1 ? c.areaPt2 : c.areaPt2 * sizeOf(c) ** 2, order: key(c), orderDate: sh.topup && !sh.topup.closedAt && sh.appendOnly ? 0 : rank(c), pinned: c.pinned ? { cxPt: c.pinned.cxPt, cyPt: c.pinned.cyPt, angle: c.pinned.angle } : null, hold: !!(c.pinned && !c.arrivalPin), ...(Solver && Solver.pairFields ? Solver.pairFields(c) : {}) }))
    };
  }

  /* How many searches a sheet's nest runs side by side, and how (GOLDOPT, Paul 12 Nov 2026: 10K and 14K are solid gold on small sheets, "nest the pieces the most efficient possible way":
     several careful searches at once, each placing the pieces in its own order (job.orderVariant), and the tightest layout wins, betterLayout). */
  /** The number of searches for `job` (its multiStart as buildJob set it, `count` pieces), with `parallel(angles)` the page's own count for a single search (parallelCount).
      (GOLDLOCK, Paul 12 Nov 2026: 14K Draft 1 and 2 were flagged "Verification flagged": a sheet with pieces held where they are (lockedPlacements: a moved-in order, the saved layout; protectedRose) is
       nested the single way it always was: the side-by-side searches are for a fresh nest only, where nothing is held)
      (SMALLSEARCH, Paul 14 Nov 2026: a 3-piece sheet ran six searches for a minute): a sheet of few pieces runs no more searches side by side than it has pieces. Changes job.multiStart to match. */
  function searchCount(job, count, parallel) {
    if (job.multiStart && (job.lockedPlacements?.length || job.protectedRose)) delete job.multiStart;
    if (job.multiStart && job.tight) job.multiStart = Math.max(1, Math.min(job.multiStart, count));
    return job.multiStart ? (job.tight ? job.multiStart : Math.max(parallel(job.angles.length), job.multiStart)) : parallel(job.angles.length);
  }
  /** The job search `i` of a sheet's first round runs: its own seed, every other one turning more broadly, its own placing order when several run side by side, and (KEEPSEARCH)
      every other one laying the bars flat along the edges (LONGFIRST). o.keep: the nest keeps searching (keepPlan); o.useGPU: this search runs on the GPU. */
  function slotJob(job, i, o) {
    o = o || {};
    return Object.assign({}, job, { seed: job.seed + i, exploreRotations: i % 2 === 1, ...(job.multiStart ? { orderVariant: i } : {}), ...(o.keep && i % 2 === 1 ? { longFirstForce: true } : {}), useGPU: !!o.useGPU });
  }
  /* KEEPSEARCH (Paul 14 Nov 2026: "Why is the system finding good solutions and then scrapping them?" -> "Yes, do it"): a 10K, 14K or Rose Gold fresh nest keeps searching while its time lasts:
     a search that finishes starts again in a new placing order (orderVariant: the next number, shaken by its own seed), up to KEEP_MAX finished layouts; the least wasteful one is kept (betterLayout, TIGHT). */
  /** The bookkeeping of a nest that keeps searching (null when it does not): `n` searches started at `t`, `count` pieces. (SMALLSEARCH: at most two finished layouts per piece) */
  function keepPlan(job, n, count, t) {
    if (!(job.multiStart && job.tight)) return null;
    return { job, t0: t, next: n, done: 0, started: new Array(n).fill(t), max: Math.min(KEEP_MAX, Math.max(n, 2 * count)) };
  }
  /** How a search's layouts are weighed while it keeps searching (the search's own best), and how the nest's finished layouts are weighed at its end (the pick). */
  const keepOpts = metal => ({ reachFirst: GOLD_METALS.includes(metal), tight: true });
  const pickOpts = metal => ({ reachFirst: GOLD_METALS.includes(metal), tight: TIGHT_METALS.includes(metal) });
  /** A search `idx` finished with `result`: counted, and kept as that search's own best when it beats it (Solver.betterLayout; stockOf() the sheet's stock). */
  function keepResult(K, results, idx, result, metal, stockOf, Solver) {
    if (!result) return false;
    K.done++;
    if (!results[idx] || Solver.betterLayout(result, results[idx], stockOf(), keepOpts(metal))) results[idx] = result;
    return true;
  }
  /** Whether search `idx` starts once more at `now`: it finished with a layout nothing stopped, the sheet still nests, fewer than K.max were started, and this pass fits again in what is left of the time. */
  function keepAgain(K, idx, result, now, o) {
    o = o || {};
    const pass = now - (K.started[idx] || K.t0);
    return !!(result && !o.stopAsked && result.endedBy !== "stopped" && o.nesting && K.next < K.max && now - K.t0 + pass <= (K.job.timeBudgetMs || 180000));
  }
  /** The job a search runs when it starts again in the next placing order `v` (every other one lays the bars flat along the edges, LONGFIRST; never on the GPU). */
  function keepJob(job, v) { return Object.assign({}, job, { seed: job.seed + v, exploreRotations: v % 2 === 1, orderVariant: v, useGPU: false, ...(v % 2 === 1 ? { longFirstForce: true } : {}) }); }
  /** Search `idx` starts again at `now`: the next placing order is taken, and its job is given back. */
  function keepNext(K, idx, now) { const v = K.next++; K.started[idx] = now; return keepJob(K.job, v); }
  /** The best of the searches' layouts (GOLDOPT: the least stock wins for solid gold): null when none. */
  function pickLayout(results, metal, stockOf, Solver) { return results.reduce((a, b) => Solver.betterLayout(b, a, stockOf(), pickOpts(metal)) ? b : a, null); }
  /** The layout a nest ends with: the best of its searches' layouts (pickLayout); completion is a candidate, not permission to replace the best layout already received, so the
      leading search's own layout (workerBest, when there is one) is weighed against the best so far (best) and the better kept, with its parameters and the reason it ended. */
  function finalPick(results, workerBest, best, metal, stockOf, Solver) {
    const pick = pickLayout(results, metal, stockOf, Solver);
    return Solver.bestResult(workerBest || pick, best, stockOf(), pickOpts(metal));
  }

  /** Whatever did not fit moves on to the next sheet of the same metal, which then nests on its own.
      A multi-piece order is never split across sheets: if any of its pieces was held back, the whole order goes to the next sheet.
      A pair (any line) is never left half on a sheet either: when a piece of a line did not fit and its saved mates sit locked on
      the sheet, they are lifted with it, so the line moves on whole (never split unless it cannot fit one sheet). Rose Gold's
      protected pieces and a piece a person pinned stay where they are; the line is then split, and the page's overflowToNextSheet says so.
      The page then runs its net (homeward, R16 HARD) on the sheet, whatever was lifted. → { lifted, mates, wholeMates, behind } */
  function keepOrdersWhole(sh, byId, pocket, env) {
    env = env || {};
    const key = fnOf(env, "orderKey", orderKey);
    // (NOSPREAD, Paul 17 Oct 2026: a unit is an order, or one chunk of an order bigger than a sheet, orderKey; a line's group is kept whole within its chunk)
    const gid = typeof env.groupOf === "function" ? c => { const g = env.groupOf(c); return g && c && c.chunk ? g + "~" + c.chunk : g; } : () => null;
    const orderOf = id => { const c = byId.get(id); return c ? key(c) : id; };
    const rank = fnOf(env, "orderRank", c => orderRank(c, env.Orders));   // (the date the nest ranks an order by; one released from hold is ahead of every date)
    const bad = new Set(sh.rejects.map(orderOf));
    // a topping-up sheet takes any later order that fits its gaps, so a younger one is not lifted for an older miss
    const dated = !(sh.topup && !sh.topup.closedAt) && [...byId.values()].some(c => rank(c) > 0);
    const cutoff = dated && sh.rejects.length ? Math.min(...sh.rejects.map(id => rank(byId.get(id)))) : Infinity;
    const locked = new Set([...(sh.roseProtected?.placements || []), ...(sh.appendOnly ? sh.nestInitial || [] : [])].map(p => p.id));
    // (an order the search seated in a pocket that every older order that missed does not fit is not lifted for being younger: result.pocketFilled, the solver's pocket pass)
    const lifted = sh.placements.filter(p => !locked.has(p.id) && (bad.has(orderOf(p.id)) || (dated && !(pocket && pocket.has(p.id)) && rank(byId.get(p.id)) > cutoff)));
    // the saved mates of a line that missed: lifted with it, except protected Rose Gold pieces and pieces a person pinned (those stay: a split);
    // a Merge sheets rerun (_mergeNext) never lifts the target's saved charms: they stay where they are, as its design promises
    const missed = new Set(sh.rejects.map(id => gid(byId.get(id))).filter(Boolean)), fixed = new Set((sh.roseProtected?.placements || []).map(p => p.id));
    const mates = missed.size && !sh._mergeNext ? sh.placements.filter(p => locked.has(p.id) && !lifted.includes(p) && !fixed.has(p.id) && missed.has(gid(byId.get(p.id))) && !(byId.get(p.id)?.pinned && !byId.get(p.id)?.arrivalPin)) : [];
    lifted.push(...mates);
    // The ORDER is the unit (Paul, 13 Oct 2026: "the priority should be to maintain all of the pieces for each given order on that one sheet"): a piece of an
    // order that did not fit takes the order's other SAVED pieces off this sheet with it (another line of the order, a copy of a plain quantity-N line),
    // so the whole order starts the next sheet, and the small room they leave is filled by single charms (the gap fill). Only when the whole order fits one
    // empty sheet (otherwise lifting gains nothing), when other pieces stay on this sheet (an emptied sheet cannot pass anything on), and never a protected
    // Rose Gold piece or one a person pinned; the same exceptions as for a pair's mates.
    const wholeMates = fnOf(env, "orderMatesOf", (...a) => orderMatesOf(...a, env))(sh, byId, bad, orderOf, locked, fixed, lifted);
    lifted.push(...wholeMates);
    // (R16 HARD, Paul 26 Oct 2026: a piece of an order that moves on, which came onto this sheet while it was searched (a later line of the order: neither seated nor turned away by this search), goes
    //  with it, never left behind to be passed on alone later)
    const unitOf = c => String(key(c)), going = new Set([...sh.rejects.map(orderOf), ...lifted.map(p => orderOf(p.id))].map(String));
    const seatedNow = new Set(sh.placements.map(p => p.id)), waitTurn = new Set(sh.feedWait || []), turned = new Set(sh.rejects);
    const behind = going.size ? (sh.charms || []).filter(c => !c.excluded && !seatedNow.has(c.id) && !turned.has(c.id) && !waitTurn.has(c.id) && going.has(unitOf(c))).map(c => c.id) : [];
    if (behind.length) sh.rejects.push(...behind);
    if (!lifted.length) return { lifted, mates, wholeMates, behind };
    sh.placements = sh.placements.filter(p => !lifted.includes(p));
    sh.rejects.push(...lifted.map(p => p.id));
    // a saved piece lifted off is no longer a saved placement of this sheet: the check that follows holds the saved layout in place (buildJob lockedPlacements) and would
    // flag the sheet for a "moved" piece that was taken off on purpose
    if (sh.nestInitial && lifted.some(p => sh.nestInitial.some(q => q.id === p.id))) sh.nestInitial = sh.nestInitial.filter(q => !lifted.some(p => p.id === q.id));
    fnOf(env, "agent", () => null)({ metal: sh.metal }, "nest", `Orders kept whole and oldest first: ${lifted.length} placed piece(s) of ${new Set(lifted.map(p => orderOf(p.id))).size} order(s) lifted off — the order moves to the next sheet as one${mates.length ? `; ${mates.length} saved piece(s) of ${new Set(mates.map(p => gid(byId.get(p.id)))).size} pair/group(s) whose other piece did not fit come with it, so none is left half on this sheet` : ""}${wholeMates.length ? `; ${wholeMates.length} saved piece(s) of ${new Set(wholeMates.map(p => String(orderOf(p.id)))).size} order(s) whose other piece did not fit come with it, so the order stays on one sheet` : ""}`);
    return { lifted, mates, wholeMates, behind };
  }
  /** The saved pieces keepOrdersWhole lifts with an order one of whose pieces did not fit (see there): placements of `sh` that are locked, not lifted yet, not protected, not
      pinned by a person, of an order in `bad`, when that whole order fits one empty sheet (its area, grown by half the clearance, under ORDER_ROOM of the sheet's
      usable area at the fill ceiling) and the sheet keeps some other piece. [] when the sheet's usable area is not known (no env.usableArea) or a Merge sheets rerun. */
  function orderMatesOf(sh, byId, bad, orderOf, locked, fixed, lifted, env) {
    env = env || {};
    if (!bad.size || sh._mergeNext || typeof env.usableArea !== "function") return [];
    const key = id => String(orderOf(id)), badKeys = new Set([...bad].map(String)), unit = fnOf(env, "orderKey", orderKey);
    // (NOSPREAD, Paul 17 Oct 2026: an order that asks for up to bigShare of what a sheet takes is taken to fit a fresh one, the same assumption the intake makes of a big order; ORDER_ROOM alone left an order of 70% to 90% of a sheet split by a late line)
    const NS = typeof env.nospread === "function" ? env.nospread() : null, share = NS ? Math.max(ORDER_ROOM, NS.NOSPREAD.bigShare) : ORDER_ROOM;
    const settings = env.settings, cap = +(settings && settings.maxFill) || .80, room = env.usableArea(sh) * cap * share;
    if (!(room > 0)) return [];
    const area = new Map();
    for (const c of sh.charms || []) if (!c.excluded) { const k = unit(c); area.set(k, (area.get(k) || 0) + (typeof env.inflatedArea === "function" ? env.inflatedArea(c) : +c.areaPt2 || 0)); }
    const mates = sh.placements.filter(p => locked.has(p.id) && !lifted.includes(p) && !fixed.has(p.id) && badKeys.has(key(p.id)) && !(byId.get(p.id)?.pinned && !byId.get(p.id)?.arrivalPin) && (area.get(key(p.id)) || 0) <= room);
    return mates.length && sh.placements.length - lifted.length - mates.length > 0 ? mates : [];
  }

  return {
    CHARM_SCALE, CAREFUL_ANGLES, FEED_ORDERS, ORDER_ROOM, KEEP_MAX, GOLD_METALS, TIGHT_METALS,
    manualSize, carefulNest, orderKey, orderRank, gapFilling, orderSizes, turnOrder, activeCharms, nestItems, feedTurn,
    handLocks, withHandLocks, multiStartFor, buildJob,
    searchCount, slotJob, keepPlan, keepOpts, pickOpts, keepResult, keepAgain, keepJob, keepNext, pickLayout, finalPick,
    keepOrdersWhole, orderMatesOf
  };
});
