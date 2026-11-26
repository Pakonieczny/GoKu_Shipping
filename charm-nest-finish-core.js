/* charm-nest-finish-core.js — FINISHCORE (the cloud carries every run, architecture v2, build step 7): the pure parts of a sheet's finish (charm-nest-1.html finishNest) and of its save
 * (persistSheet), in ONE file that the page and node both load, so that the cloud's one sheet writer (a later step) writes a sheet with the page's own rules and not a copy of them.
 * Each function below was a function of charm-nest-1.html; the page now calls these with its own helpers, and does exactly what it did.
 *
 *   layoutKey(sh, byId)                          what a sheet's written files depend on (finishNest's "nothing new went in")
 *   buildJob(sh, host)                           the solver's job for a sheet
 *   sheetSpec(sh, byId, st, result, host)        the written sheet's spec (CharmNestPDF.buildSheet's input)
 *   buildReport(sh, result, items, host)         the sheet's nest report
 *   exchangeHeld, unionArea, overlapOnly, overlapRound, applyOverlapFixes, overlapMoved
 *                                                the OVERLAPFIX planner: the page writes the file and runs both checks (fixOverlaps), this plans each round and applies it
 *   exactOverlaps, exactFix (exactArea, exactShared, exactMoved)
 *                                                EXACTOVERLAP: the written file's own cut lines read, and the crossings cleared
 *   membershipOf(sh, host)                       NOPARTIALSET, with SETSHUT's "held" rule (a partial sheet the set already lists stays in it) and KEEPINSET (CharmNestSetRules.isPartial)
 *   record(sh, o, outputs)                       the Library record a save writes (persistSheet's record())
 *   sheetFull, topupSettle, intakeMarks, sheetFileBase, overflowPlan (rechunkStalled), feedNext
 *                                                build step 7b: finishNest's after-nest decisions, for the cloud's sheet writer (the page keeps its own copies, unchanged)
 *
 * Nothing here reads the DOM, the network, Firestore or a page global. What a function needs of where it runs comes in `host` (the page hands its own functions; node hands its own):
 *   buildJob     host { stockFor(metal, sh), nestItems(sh), carefulNest(sh), angleSet(), cornerPtSetting(sh), gapFilling(sh), orderRank(c), settings, CAREFUL_ANGLES?,
 *                       manualSize?(c), withHandLocks?(sh, items, base), orderKey?(c), CharmNestSolver?, CharmNestRose?, navigator? }
 *   sheetSpec    host { sourceOf(id), labelOf(metal), charmScale? }
 *   buildReport  host { stockFor(metal, sh), labelOf(metal), shapeSignature(items), now?() }
 *   exactFix     host { buildJob(sh), insetFor(sh), isFrozen?(c), Vector, Export, PDF, sheetSpec(sh, byId, st, result), buildSheet(spec), progress?(sh), log(sh, msg), agent(who, kind, text), now?() }
 *   membershipOf host { SetRules, setsOfRun?(runId), modern?(runId) }
 *   record       o    { items, byId, day, name, member, shapes, sheetName(sh), sourceOf(id), solidSelected?(metal, sh), pairDescriptor?(c), listingsOf?(charms) }
 *   node: const FC = require('./charm-nest-finish-core.js');   the page: window.CharmNestFinishCore */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.CharmNestFinishCore = api;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  /** After nesting, every charm is written and previewed 2.9 % smaller than drawn (charm-nest-1.html CHARM_SCALE, Paul 25 Oct 2026); the page hands its own, this is the same number for node. */
  const CHARM_SCALE_DEFAULT = 0.971;
  /** One charm at a time on every sheet, graded at 2° steps (charm-nest-1.html CAREFUL_ANGLES); the page hands its own, this is the same list for node. */
  const CAREFUL_ANGLES_DEFAULT = Array.from({ length: 180 }, (_, i) => i * 2);

  /** What a sheet's written files depend on: each placed charm, where and how it lies, and its artwork and name. */
  const layoutKey = (sh, byId) => JSON.stringify([sh.sheetId, (sh.placements || []).map(p => { const c = byId.get(p.id); return [String(p.id), +(+p.cxPt).toFixed(3), +(+p.cyPt).toFixed(3), p.angle, p.scale || 1, c ? c.hash : null, c ? c.name : null]; }).sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)]);

  /** The solver's job for a sheet (charm-nest-1.html buildJob, moved as it was). The page's own names are bound first to what the host hands: its helpers, its settings
   *  (read when the job reads them), CAREFUL_ANGLES, and the solver, Rose Gold and navigator objects it has (undefined when it has none: then the job does without, as the page did). */
  function buildJob(sh, host) {
    const { stockFor, nestItems, carefulNest, angleSet, cornerPtSetting, gapFilling, orderRank, manualSize, withHandLocks, orderKey } = host;
    const S = { get settings() { return host.settings; } }, CAREFUL_ANGLES = host.CAREFUL_ANGLES || CAREFUL_ANGLES_DEFAULT, CharmNestSolver = host.CharmNestSolver, CharmNestRose = host.CharmNestRose, navigator = host.navigator;
    const st = stockFor(sh.metal, sh), sizeOf = c => (typeof manualSize === "function" ? manualSize(c) : 1);   // (a host with no manualSize: every piece at its design size)
    const items = nestItems(sh), careful = carefulNest(sh);
    return {
      sheet: { wPt: st.wPt, hPt: st.hPt, insetPt: +S.settings.insetPt || 0, cornerPt: cornerPtSetting(sh), ...(sh.roseStock?.profileJson ? {remnant:JSON.parse(sh.roseStock.profileJson)} : {}) },
      clearancePt: +S.settings.clearancePt || 0, writeScale: 1 /* (25 Oct 2026: nesting at the written size, CHARM_SCALE, made the append turns of a big file place 11 of 14 pieces on an 11 % sheet; off until that is fixed. The overlap the shrink can cause is cleared by fixOverlaps) */, angles: careful && angleSet().length < CAREFUL_ANGLES.length ? CAREFUL_ANGLES : angleSet(), fineRes: 2, coarseRes: 0.5,
      timeBudgetMs: sh.intakeBudgetMs || (+S.settings.budgetS || 180) * 1000, fullBudget: sh.intakePhase !== "fill", stallMs: 60 * 1000, maxTrials: sh.intakePhase === "fill" ? 2000 : 1000000, seed: sh.runSeed || +S.settings.seed || 1,
      // charms already on the sheet stay exactly where they are
      protectedRose:sh.roseProtected||null,lockedPlacements:(typeof withHandLocks==="function"?withHandLocks:(sh,items,base)=>base)(sh,items,sh.appendOnly?sh.nestInitial||sh.placements:careful&&sh.nestInitial?.length?sh.nestInitial:null),   // (a host with no withHandLocks: the saved layout alone)
      // a Gold or Silver sheet also reports whether one of the shop's smallest charms could still go in (see topupSettle)
      careful, roomCheck: careful && ["gold", "silver"].includes(sh.metal),
      // (GOLDOPT: solid 10K / 14K gold, small sheets, costly stock: up to six searches side by side, each its own placing order)
      // (TIGHT, Paul 14 Nov 2026: Rose Gold too, and each layout carries its dead space so the least wasteful one is kept; GOLDLOCK below still nests a sheet with held pieces the single way)
      ...(["gold10k", "gold14k", "rose"].includes(sh.metal) ? { tight: true } : {}),
      ...(careful && ["gold10k", "gold14k", "rose"].includes(sh.metal) && !sh.appendOnly ? { multiStart: Math.min(6, Math.max(3, ((typeof navigator !== "undefined" && navigator.hardwareConcurrency) || 4) - 1)) } : {}),
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
      initialLayout: sh.nestInitial || null, maxFill: +S.settings.maxFill || 0.80, nearFullContact: !(sh.metal==="rose"||(typeof CharmNestRose!=="undefined"&&CharmNestRose.cuts(sh.metal))),
      // filling its gaps, a sheet takes any later order that fits, not only the oldest; the pockets go to single-charm orders before a multi-piece order that was turned away (solver: singlesFirst)
      ...(gapFilling(sh) ? { singlesFirst: true } : {}),
      // (a piece a person resized by hand, Manual Control: its bitmap is read at that size, so the nest, its check and the written file all see the same piece)
      pieces: items.map(c => ({ id: c.id, w: c.w, h: c.h, scale: sizeOf(c) === 1 ? c.scale : c.scale / sizeOf(c), bits: c.bits, areaPt2: sizeOf(c) === 1 ? c.areaPt2 : c.areaPt2 * sizeOf(c) ** 2, order: typeof orderKey === "function" ? orderKey(c) : c.order || c.id, orderDate: sh.topup && !sh.topup.closedAt && sh.appendOnly ? 0 : orderRank(c), pinned: c.pinned ? { cxPt: c.pinned.cxPt, cyPt: c.pinned.cyPt, angle: c.pinned.angle } : null, hold: !!(c.pinned && !c.arrivalPin), ...(typeof CharmNestSolver !== "undefined" && CharmNestSolver.pairFields ? CharmNestSolver.pairFields(c) : {}) }))   // (hold: pinned by a person, never lifted; the run's own pin on every placed charm is arrivalPin; pairFields: the group, side and "seat near its mate" of a pair's pieces, nothing for any other charm)
    };
  }

  /** The written sheet's spec from its placements (as finishNest writes it). */
  function sheetSpec(sh, byId, st, result, host) {
    const sources = new Map(); for (const p of sh.placements) { const c = byId.get(p.id); if (c && !sources.has(c.sourceId)) sources.set(c.sourceId, host.sourceOf(c.sourceId).parsed); }
    return { sheet: { wPt: st.wPt, hPt: st.hPt, strokeRGB: [1, 0, 0], strokePt: 0.5 }, placements: sh.placements.map(p => ({ charm: byId.get(p.id), angle: p.angle, cxPt: p.cxPt, cyPt: p.cyPt, scale: p.scale || 1 })), sources, title: `${host.labelOf(sh.metal)} sheet ${sh.sheetId}`, meta: { sheetId: sh.sheetId, metal: sh.metal, seed: result.params.seed, clearancePt: result.params.clearancePt, charmScale: host.charmScale != null ? host.charmScale : CHARM_SCALE_DEFAULT } };
  }

  /** The sheet's nest report (written beside its files as <name>_nest-report.json, and read back by the record: stock, placements, rejects, calibration). */
  function buildReport(sh, result, items, host) {
    const byId = new Map(items.map(c => [c.id, c])); const st = host.stockFor(sh.metal, sh);
    return {
      version: 1, frontInSheet: 1, bestRevision: sh.bestRevision || 0,   // (FRONTINSHEET, 26 Nov 2026: files written since then draw the Charm Editor's front parts on their charms; _charmNestFinish.js filesNow reads it)
      bestHistory: sh.bestHistory || [], sheetId: sh.sheetId, metal: sh.metal, metalLabel: host.labelOf(sh.metal), createdAt: (typeof host.now === "function" ? new Date(host.now()) : new Date()).toISOString(),
      stock: { wIn: st.wIn, hIn: st.hIn, wPt: st.wPt, hPt: st.hPt }, params: result.params, endedBy: result.endedBy, trials: result.trials, elapsedMs: Math.round(result.elapsedMs),
      density: +result.density.toFixed(4), usablePt2: Math.round(result.usablePt2), freePt2: Math.round(result.freePt2), placedPt2: Math.round(result.placedPt2), pocket: result.pocket,
      placements: sh.placements.map((p, i) => ({ n: i + 1, id: p.id, layer: p.layerName, name: byId.get(p.id)?.name, hash: byId.get(p.id)?.hash, source: byId.get(p.id)?.sourceName, index: byId.get(p.id)?.index, angle: p.angle, scale: p.scale || 1, cxPt: +p.cxPt.toFixed(3), cyPt: +p.cyPt.toFixed(3), wPt: +p.wPt.toFixed(2), hPt: +p.hPt.toFixed(2), areaPt2: +(byId.get(p.id)?.areaPt2 || 0).toFixed(1) })),
      rejects: sh.rejects.map(id => ({ id, name: byId.get(id)?.name, source: byId.get(id)?.sourceName, index: byId.get(id)?.index, areaPt2: +(byId.get(id)?.areaPt2 || 0).toFixed(1), widthPt: +(byId.get(id)?.widthPt || 0).toFixed(1), heightPt: +(byId.get(id)?.heightPt || 0).toFixed(1) })),
      excluded: sh.charms.filter(c => c.excluded).map(c => ({ id: c.id, name: c.name, reason: c.open ? "open path" : "excluded by operator" })),
      verification: sh.verification, charms: items.map(c => ({ id: c.id, name: c.name, hash: c.hash, qty: c.qty, order: c.order || c.id, layer: c.layer || null, areaPt2: +c.areaPt2.toFixed(1), widthPt: +c.widthPt.toFixed(2), heightPt: +c.heightPt.toFixed(2), namedBy: c.namedBy, confidence: c.confidence })),
      calibration: Object.assign({ density: +result.density.toFixed(4), placedAll: sh.rejects.length === 0 }, host.shapeSignature(items)),
      log: sh.log.slice()
    };
  }

  /* SHRINKBOTH (Paul, 12 Nov 2026): the check after a sheet is nested shrinks BOTH charms of an overlapping pair, each by up to 2.5 % about its own centre (charm-nest-overlap-fix.js), never more than
     2.5 % in all for a charm; only when that cannot clear it is the nudge tried. Before: one charm of the pair, a nudge first, a shrink of up to 6 %. */
  const OVF_MIN_SCALE = 0.975;
  /* INPLACEHOLD (Paul, 17 Nov 2026: "never re-nest or rebuild sheets; do not make things denser or sparser"; designs-window-e2e.cjs scenarios B and F: after a piece was exchanged in place, two of its neighbours kept their places and were made 2.5 % smaller,
     0.971 to 0.9467, by the exact overlap pass below). A sheet written AS IT STANDS after a piece was exchanged in place (writeAsPlaced: the result ended "kept", the new piece carries c.inPlace) is the old sheet with one new piece in it: the overlap passes
     (fixOverlaps, exactFix) may shrink or move ONLY that new piece, never a neighbour. → the placement ids they may not touch (empty for every other write). A crossing the new piece cannot clear alone stays what it always was, a warning on the sheet
     (sh.exactOverlap.ok false, "not cleared"): the guard is not weakened, nothing is saved silently, and no neighbour is touched. */
  function exchangeHeld(sh, byId, result) {
    if (!result || result.endedBy !== "kept") return new Set();
    const fresh = new Set((sh.placements || []).filter(p => { const c = byId.get(p.id); return c && c.inPlace && !c.inPlace.seen; }).map(p => p.id));   // (seen: its exchange was written already, by an earlier write: it is a neighbour like any other now)
    return fresh.size ? new Set((sh.placements || []).filter(p => !fresh.has(p.id)).map(p => p.id)) : new Set();
  }

  /* OVERLAPFIX (Paul, 25 Oct 2026): "minor overlap between some charms on a sheet … cannot hold the sheet and the sheet must be auto fixed by the system by shrinking a given charm that is
     overlapping by the smallest possible amount and moving it over just slightly enough that the offending overlap gets resolved … The primary approach should be that a tiny readjustment
     of the position should resolve the tiny overlap, but if that is not possible, then shrinking the charm by the smallest needed amount."
     The re-render's own pixels (what each charm covers, verifyRendered `areas`) are what the plan reads (charm-nest-overlap-fix.js): for each overlapping pair the charm to touch (never a
     locked one or a protected Rose Gold one; the newer and smaller first) gets the nearest whole-pixel nudge that clears every
     other charm and the margin; only when none within 3 pt does, the smallest shrink about its own centre, with its nearest nudge. A whole-pixel nudge renders exactly as the shifted pixels,
     so the plan is the result; the file is written and BOTH checks run again, at most three rounds (the page's fixOverlaps). Anything still flagged after that is flagged as before. */
  /** The two checks flag an overlap and nothing the fix cannot touch (no error, nothing outside the sheet, no empty layer, nothing removed or protected moved): a round is worth planning. */
  const overlapOnly = (r, g) => !!r && !!g && !(r.ok && g.ok) && !r.error && !g.error && !r.outsidePx && !(r.emptyLayers || []).length && !g.outsidePx && !g.removedPx && !g.protectedMoved && (r.overlapPx > 0 || g.overlapPx > 0);
  /** One round of the plan. o { F: CharmNestOverlapFix, render (its areas and page), gm (the geometry check with its masks), res, byId, locked: Set of ids never touched, kept: Set of ids already
   *  on the sheet (touched after the newer ones), isFrozen?(charm) }. → { byLayer, plan } or { byLayer, error } when the planner itself threw. */
  function overlapRound(sh, o) {
    const { F, render, res, byId, locked, kept } = o, gmById = new Map((o.gm.masks || []).map(m => [m.id, m]));
    // what each charm takes up for BOTH checks: the re-rendered ink (render.areas) united with the outline the geometry check reads (its masks), page pixels at res, y down
    const byLayer = new Map(sh.placements.map(p => [p.layerName, p])), areas = new Map(), centers = new Map(), movable = new Set(), prefer = new Map(), shrinkLeft = new Map();
    for (const p of sh.placements) {
      if (!p.layerName) continue;
      const a = render.areas.get(p.layerName), g = gmById.get(p.id), u = unionArea(a, g); if (!u) continue;
      areas.set(p.layerName, u);
      centers.set(p.layerName, { x: p.cxPt * res, y: p.cyPt * res });
      if (!locked.has(p.id) && !(typeof o.isFrozen === "function" && o.isFrozen(byId.get(p.id)))) movable.add(p.layerName);   // (CUSTOMPOOL: a custom design sent at its exact size is never shrunk; the other charm of the pair moves)
      prefer.set(p.layerName, kept.has(p.id) ? 1 : 0);
      shrinkLeft.set(p.layerName, Math.max(0, 1 - OVF_MIN_SCALE / (p.fixScale || 1)));   // (SHRINKBOTH: 2.5 % in all, whatever the rounds)
    }
    let plan; try { plan = F.plan({ W: render.page.W, H: render.page.H, inset: render.page.inset, res, areas, centers, movable, prefer, maxShrink: 1 - OVF_MIN_SCALE, shrinkLeft }); } catch (e) { return { byLayer, error: e }; }
    return { byLayer, plan };
  }
  /** The round's fixes onto the placements (and onto the saved layout the search holds still, nestInitial, so the geometry check and every later turn hold each where the fix put it); `done` collects the charms touched. */
  function applyOverlapFixes(plan, byLayer, saved, done, res) {
    for (const f of plan.fixes) {
      const p = byLayer.get(f.name); if (!p) continue;
      if (f.scale && f.scale < 1) { p.scale = +((p.scale || 1) * f.scale).toFixed(5); p.fixScale = +((p.fixScale || 1) * f.scale).toFixed(5); }   // (fixScale: the fix's own shrink, which the geometry check reads too)
      p.cxPt += (f.dx || 0) / res; p.cyPt += (f.dy || 0) / res;
      const k = saved.get(p.id); if (k && k !== p) Object.assign(k, { cxPt: p.cxPt, cyPt: p.cyPt, scale: p.scale, ...(p.fixScale ? { fixScale: p.fixScale } : {}) });
      done.set(p.id, p);
    }
  }
  /** What the fix did to each charm it touched: how far it moved and how much smaller it is than before the first round. */
  const overlapMoved = (done, before, byId) => [...done.values()].map(p => { const b = before.get(p.id) || {}, d = Math.hypot(p.cxPt - b.cxPt, p.cyPt - b.cyPt), k = (p.scale || 1) / (b.scale || 1); return { id: p.id, name: p.name || (byId.get(p.id) || {}).name || p.id, movedPt: +d.toFixed(2), shrinkPct: +((1 - k) * 100).toFixed(2) }; });
  /** One footprint from the render's area ({X0, Y0, bw, bh, core}) and the geometry check's mask ({x0, y0, w, h, bits}), page pixels; either may be missing. */
  function unionArea(a, g) {
    if (a && a.empty) a = null;
    if (!a && !g) return null;
    const X0 = Math.min(a ? a.X0 : Infinity, g ? g.x0 : Infinity), Y0 = Math.min(a ? a.Y0 : Infinity, g ? g.y0 : Infinity);
    const X1 = Math.max(a ? a.X0 + a.bw : -Infinity, g ? g.x0 + g.w : -Infinity), Y1 = Math.max(a ? a.Y0 + a.bh : -Infinity, g ? g.y0 + g.h : -Infinity);
    const bw = X1 - X0, bh = Y1 - Y0, core = new Uint8Array(bw * bh);
    if (a) for (let y = 0; y < a.bh; y++) for (let x = 0; x < a.bw; x++) if (a.core[y * a.bw + x]) core[(a.Y0 - Y0 + y) * bw + (a.X0 - X0 + x)] = 1;
    if (g) for (let y = 0; y < g.h; y++) for (let x = 0; x < g.w; x++) if (g.bits[y * g.w + x]) core[(g.y0 - Y0 + y) * bw + (g.x0 - X0 + x)] = 1;
    return { X0, Y0, bw, bh, core };
  }

  /* EXACTOVERLAP (Paul, 12 Nov 2026: "I know there's overlap, I've tested it [on the laser] ... you're not catching it"): the two checks above read pixels and, with the aggressive clearance
     (a negative clearance: neighbours share a stroke), allow a sliver of overlap; the laser cuts the real lines, so GF Set-1 Sheet 1 had 6 overlapping pairs, RG 7 (one of 2.07 mm²), SS 9.
     This check reads the written file itself: every charm's cut outline (charm-nest-export.js productionPaths, the same lines the .ai and the .dxf carry), and every pair of charms whose
     outlines share ANY area (over EXACT_EPS_MM2) is an overlap, whatever the clearance. The fix (SHRINKBOTH): both charms shrink together about their own centres, 0.5 % at a time, never past
     2.5 % in all (OVF_MIN_SCALE); when that cannot clear it, the smaller movable one is nudged straight away from the other (up to 1 mm), never into a third charm or past the inset. The file
     is written again and read again, at most three rounds. (MORESHRINK, Paul 12 Nov 2026: "just shrink them a little more") when that cannot clear it, both shrink a little more, up to 6 % in all,
     each moved up to 1 mm away from the other. What is still left is a warning on the sheet (sh.exactOverlap.ok false), never a hold: it still joins its set (OVNOHOLD). */
  const EXACT_EPS_MM2 = 0.0005, EXACT_MM = 25.4 / 72, EXACT_MORE_SCALE = 0.94;
  /** Every charm's cut outline in the written file (host { Vector, Export, PDF }: CharmNestVector, CharmNestExport, CharmNestPDF), and every pair of charms whose outlines cross; null when a part is missing. */
  async function exactOverlaps(aiBytes, st, host) {
    const h = host || {}, V = h.Vector, E = h.Export, P = h.PDF;
    if (!V || !E || !P || !aiBytes) return null;
    const parsed = await P.parseSource(aiBytes, "Production sheet"), paths = E.productionPaths(parsed);
    const grey = c => !c || Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2]) <= 0.15;
    const loopsBy = new Map();
    for (const p of paths) {
      if (!p.charm) continue;
      const cut = (p.cutSilhouette && p.fill && !p.stroke) || (p.stroke && grey(p.strokeRGB) && !p.fill);
      if (!cut) continue;
      const loops = (p.subpaths || []).map(s => V.flatten(s)).filter(f => f.points.length >= 3).map(f => f.points);
      if (!loopsBy.has(p.charm)) loopsBy.set(p.charm, []);
      loopsBy.get(p.charm).push(...loops);
    }
    const shapes = new Map();
    for (const [name, loops] of loopsBy) {
      const outer = V.boolean(loops, [], "union", "nonzero").filter(r => !r.hole).map(r => r.points);
      if (!outer.length) continue;
      let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
      for (const l of outer) for (const q of l) { if (q[0] < x0) x0 = q[0]; if (q[1] < y0) y0 = q[1]; if (q[0] > x1) x1 = q[0]; if (q[1] > y1) y1 = q[1]; }
      shapes.set(name, { name, polys: outer, box: [x0, y0, x1, y1] });
    }
    const pairs = [], list = [...shapes.values()];
    for (let i = 0; i < list.length; i++) for (let j = i + 1; j < list.length; j++) {
      const mm2 = exactShared(list[i], list[j], h.Vector); if (mm2 > EXACT_EPS_MM2) pairs.push({ a: list[i].name, b: list[j].name, mm2: +mm2.toFixed(4) });
    }
    pairs.sort((p, q) => q.mm2 - p.mm2);
    return { shapes, pairs };
  }
  const exactArea = ps => ps.reduce((n, p) => { let a = 0; for (let i = 0; i < p.length; i++) { const q = p[i], r = p[(i + 1) % p.length]; a += q[0] * r[1] - r[0] * q[1]; } return n + Math.abs(a) / 2; }, 0);
  /** The area two charms' outlines share, in mm² (0 when their boxes do not meet). V: CharmNestVector. */
  function exactShared(a, b, V) {
    if (a.box[2] < b.box[0] || b.box[2] < a.box[0] || a.box[3] < b.box[1] || b.box[3] < a.box[1]) return 0;
    const x = V.boolean(a.polys, b.polys, "intersection", "nonzero", "nonzero");
    return exactArea(x.map(r => r.points)) * EXACT_MM * EXACT_MM;
  }
  /** A charm's outline scaled by s about (px, py) and moved by (dx, dy), all in the file's points. */
  function exactMoved(sh0, s, px, py, dx, dy) {
    const polys = sh0.polys.map(l => l.map(q => [px + (q[0] - px) * s + dx, py + (q[1] - py) * s + dy]));
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const l of polys) for (const q of l) { if (q[0] < x0) x0 = q[0]; if (q[1] < y0) y0 = q[1]; if (q[0] > x1) x1 = q[0]; if (q[1] > y1) y1 = q[1]; }
    return { name: sh0.name, polys, box: [x0, y0, x1, y1] };
  }
  /** The written file read for crossing cut lines and cleared (SHRINKBOTH, the nudge, MORESHRINK), written again each round; sets sh.exactOverlap. → { aiBytes, labelledBytes, specBase, changed } or null. */
  async function exactFix(sh, o, host) {
    const h = host, { byId, st, result } = o; let { aiBytes, labelledBytes } = o, specBase = null, changed = false;
    const shared = (a, b) => exactShared(a, b, h.Vector);
    const job = h.buildJob(sh), locked = new Set((job.protectedRose?.placements || []).map(p => p.id)), saved = new Map((sh.nestInitial || []).map(p => [p.id, p]));
    if (typeof h.lockIds === "function") for (const id of h.lockIds(sh) || []) locked.add(id);   // (NOREWRITE, the cloud: a piece already placed is never moved or shrunk)
    for (const id of exchangeHeld(sh, byId, result)) locked.add(id);   // (INPLACEHOLD: after a piece was exchanged in place only that piece may be shrunk or moved, never a neighbour)
    const inset = h.insetFor(sh), H = st.hPt, W = st.wPt;
    const fixes = [];
    let ex = null;
    for (let round = 0; round < 3; round++) {
      ex = await exactOverlaps(aiBytes, st, h); if (!ex || !ex.pairs.length) break;
      const byLayer = new Map(sh.placements.map(p => [p.layerName, p])), touched = new Set(), planned = [];
      const cur = new Map(ex.shapes);   // (each charm as it will stand after this round's fixes)
      for (const pr of ex.pairs) {
        if (touched.has(pr.a) || touched.has(pr.b)) continue;
        const A = byLayer.get(pr.a), B = byLayer.get(pr.b); if (!A || !B) continue;
        if (locked.has(A.id) && locked.has(B.id)) continue;   // (INPLACEHOLD: two pieces neither of which may be touched: it stays in `pairs`, a warning as before)
        const may = p => !locked.has(p.id) && !(typeof h.isFrozen === "function" && h.isFrozen(byId.get(p.id)));
        const left = p => may(p) ? Math.max(0, 1 - OVF_MIN_SCALE / (p.fixScale || 1)) : 0;
        const piv = p => [p.cxPt, H - p.cyPt];
        const others = n => [...cur.values()].filter(x => x.name !== pr.a && x.name !== pr.b && x.name !== n);
        const clearOfOthers = (sx, skip) => others(skip).every(x => shared(sx, x) <= EXACT_EPS_MM2);
        const inside = sx => sx.box[0] >= inset - 1e-6 && sx.box[1] >= inset - 1e-6 && sx.box[2] <= W - inset + 1e-6 && sx.box[3] <= H - inset + 1e-6;
        const sa0 = cur.get(pr.a), sb0 = cur.get(pr.b); let got = null;
        // 1 · both shrink together (each never past what it has left)
        for (let k = 1; k <= 5 && !got; k++) {
          const s = 1 - 0.005 * k, sa = Math.max(1 - left(A), s), sb = Math.max(1 - left(B), s); if (sa >= 1 && sb >= 1) break;
          const ma = exactMoved(sa0, sa, ...piv(A), 0, 0), mb = exactMoved(sb0, sb, ...piv(B), 0, 0);
          if (shared(ma, mb) <= EXACT_EPS_MM2) got = [{ p: A, s: sa, dx: 0, dy: 0, shape: ma }, { p: B, s: sb, dy: 0, dx: 0, shape: mb }];
        }
        // 2 · the smaller movable one nudged straight away from the other, with what shrink each has left
        if (!got) {
          const area = x => exactArea(x.polys), cands = [A, B].filter(p => !locked.has(p.id)).sort((p, q) => area(cur.get(p.layerName)) - area(cur.get(q.layerName)));
          search: for (const M of cands) {
            const N = M === A ? B : A, m0 = cur.get(M.layerName), n0 = cur.get(N.layerName);
            const sm = 1 - left(M), sn = 1 - left(N), mN = exactMoved(n0, sn, ...piv(N), 0, 0);
            const [mx, my] = piv(M), [nx, ny] = piv(N); let ux = mx - nx, uy = my - ny; const ul = Math.hypot(ux, uy) || 1; ux /= ul; uy /= ul;
            // (straight away from the other first, then 24 directions round: the shortest move that clears it, in 0.05 mm steps up to 1 mm)
            const dirs = [[ux, uy]]; for (let k = 0; k < 24; k++) { const a = k * Math.PI / 12; dirs.push([Math.cos(a), Math.sin(a)]); }
            for (let d = 0.05 / EXACT_MM; d <= 1 / EXACT_MM + 1e-9; d += 0.05 / EXACT_MM) for (const [vx, vy] of dirs) {
              const mM = exactMoved(m0, sm, mx, my, vx * d, vy * d);
              if (shared(mM, mN) <= EXACT_EPS_MM2 && clearOfOthers(mM, N.layerName) && inside(mM)) { got = [{ p: M, s: sm, dx: vx * d, dy: vy * d, shape: mM }, { p: N, s: sn, dx: 0, dy: 0, shape: mN }]; break search; }
            }
          }
        }
        // 3 · (MORESHRINK) both shrink a little more, up to 6 % in all, each moved up to 1 mm apart along the line between them (±30°)
        if (!got) {
          const more = p => may(p) ? Math.max(0, 1 - EXACT_MORE_SCALE / (p.fixScale || 1)) : 0;
          const [ax, ay] = piv(A), [bx, by] = piv(B), base = Math.atan2(ay - by, ax - bx), step = 0.1 / EXACT_MM;
          more: for (let k = 1; k <= 12; k++) {
            const s = 1 - 0.005 * k, sa = Math.max(1 - more(A), s), sb = Math.max(1 - more(B), s);
            for (let t = 0; t <= 6; t++) {
              const th = base + (t % 2 ? 1 : -1) * Math.ceil(t / 2) * Math.PI / 18, vx = Math.cos(th), vy = Math.sin(th);
              for (let da = 0; da <= 10; da++) for (let db = 0; db <= 10; db++) {
                const dA = locked.has(A.id) ? 0 : da * step, dB = locked.has(B.id) ? 0 : db * step;
                const ma = exactMoved(sa0, sa, ax, ay, vx * dA, vy * dA), mb = exactMoved(sb0, sb, bx, by, -vx * dB, -vy * dB);
                if (shared(ma, mb) <= EXACT_EPS_MM2 && inside(ma) && inside(mb) && clearOfOthers(ma, pr.b) && clearOfOthers(mb, pr.a)) { got = [{ p: A, s: sa, dx: vx * dA, dy: vy * dA, shape: ma }, { p: B, s: sb, dx: -vx * dB, dy: -vy * dB, shape: mb }]; break more; }
              }
            }
            if (sa === 1 - more(A) && sb === 1 - more(B)) break;
          }
        }
        if (!got) continue;
        for (const g of got) { touched.add(g.p.layerName); cur.set(g.p.layerName, g.shape); if (g.s < 1 || g.dx || g.dy) planned.push(g); }
      }
      if (!planned.length) break;
      for (const g of planned) {
        const p = g.p;
        if (g.s < 1) { p.scale = +((p.scale || 1) * g.s).toFixed(5); p.fixScale = +((p.fixScale || 1) * g.s).toFixed(5); }
        p.cxPt += g.dx; p.cyPt -= g.dy;   // (the file's y runs up, the sheet's down)
        const k = saved.get(p.id); if (k && k !== p) Object.assign(k, { cxPt: p.cxPt, cyPt: p.cyPt, scale: p.scale, ...(p.fixScale ? { fixScale: p.fixScale } : {}) });
        fixes.push({ name: p.layerName, shrinkPct: +((1 - g.s) * 100).toFixed(2), movedMm: +(Math.hypot(g.dx, g.dy) * EXACT_MM).toFixed(3) });
      }
      sh.stage = `Clearing ${planned.length} overlapping charm${planned.length === 1 ? "" : "s"} · writing again…`; if (typeof h.progress === "function") h.progress(sh);
      specBase = h.sheetSpec(sh, byId, st, result);
      aiBytes = await h.buildSheet(specBase);
      sh.placements.forEach((p, i) => { p.layerName = specBase.placements[i].layerName; });
      labelledBytes = await h.buildSheet(Object.assign({}, specBase, { labelled: true, placements: specBase.placements.map(p => Object.assign({}, p)) }));
      changed = true; ex = null;
    }
    if (!ex) ex = await exactOverlaps(aiBytes, st, h);
    if (!ex) return null;
    sh.exactOverlap = { at: typeof h.now === "function" ? h.now() : Date.now(), ok: !ex.pairs.length, pairs: ex.pairs.slice(0, 20), fixes };
    if (fixes.length) { const w = fixes.map(f => `${f.name} ${f.shrinkPct ? f.shrinkPct + "% smaller" : ""}${f.shrinkPct && f.movedMm ? ", " : ""}${f.movedMm ? "moved " + f.movedMm + " mm" : ""}`).join("; "); h.log(sh, `exact overlap check: cleared by ${w}`); h.agent({ metal: sh.metal }, ex.pairs.length ? "warn" : "ok", `Overlap (exact cut lines): ${w}${ex.pairs.length ? ` · still overlapping: ${ex.pairs.map(p => p.a + " ↔ " + p.b).join("; ")}` : ""}`); }
    else if (ex.pairs.length) h.agent({ metal: sh.metal }, "warn", `Overlap (exact cut lines) not cleared within 6 % or 1 mm (it does not hold the sheet): ${ex.pairs.map(p => `${p.a} ↔ ${p.b} ${p.mm2} mm²`).join("; ")}`);
    return { aiBytes, labelledBytes, specBase, changed };
  }

  /** (NOPARTIALSET) The membership a save of this sheet may carry: its own, except that a partial GF or SS sheet of a modern run (not full, not cut: CharmNestSetRules.isPartial) is a draft outside every set.
   *  host { SetRules: CharmNestSetRules, setsOfRun(runId): the run's sets (each { setId, sheetIds }), modern(runId): the run is a modern one (Gate.modern) }.
   *  (KEEPINSET, Paul 20 Nov 2026: a partial sheet the owner put back in its set, keepInSet, is not partial to CharmNestSetRules.isPartial while it is in that set: it keeps its set here too) */
  function membershipOf(sh, host) {
    const h = host || {}, own = { draft: !!sh.draft, setId: sh.setId || null, setSeq: sh.seq || null, sheetIndex: sh.sheetIndex || null };
    try {
      const R = h.SetRules;
      // (SETSHUT, Paul 20 Nov 2026: "how the fuck did it get removed from a set?"): a partial sheet the set ALREADY holds (the set lists it) is kept in it. NOPARTIALSET keeps a partial sheet from
      //  joining a set; it wrote SS_Nov.12.26_Set-1_Sheet-3, a member of the Nov 12 Set 1 since Nov 13, as a draft at its next save, half out of its set (the set and its pieces still named it)
      const held = (() => { try { const sets = sh.setId && sh.sheetId && typeof h.setsOfRun === "function" ? h.setsOfRun(sh.runId) : null; return !!(sets && sets.some(s => s && s.setId === sh.setId && (s.sheetIds || []).includes(sh.sheetId))); } catch (_) { return false; } })();
      if (!held && R && typeof R.isPartial === "function" && (sh.setId || !sh.draft) && R.isPartial(sh) && typeof h.modern === "function" && h.modern(sh.runId)) return { draft: true, setId: null, setSeq: null, sheetIndex: null };
      // (RELEASEMARK, Paul 20 Nov 2026: GF Draft 1 and 2 were saved as drafts that named Set 5, a set that never listed them): a draft names no set, its number or index, whatever its copy on this
      //  computer says; only a person's take-out of that very set, under way (leftSet), keeps the name until the run lets it go
      if (!held && own.draft && own.setId && String(sh.leftSet || "") !== String(own.setId)) return { draft: true, setId: null, setSeq: null, sheetIndex: null };
    } catch (_) { /* its own, as before */ }
    return own;
  }

  /** The Library record a save of this sheet writes (persistSheet's record(): the first write without its files, the second with them, and the charm links later).
   *  o { items: the sheet's charms, byId, day, name: its file base, member: membershipOf(sh), shapes: Map id -> the shape fields (shapeFieldOf), sheetName(sh), sourceOf(id),
   *      solidSelected?(metal, sh), pairDescriptor?(charm), listingsOf?(charms) }. outputs: the files' { path, url } (null for the first write). */
  function record(sh, o, outputs) {
    const { items, byId, day, name, member, shapes } = o;
    return {
      id: sh.sheetId, cardStartedAt: sh.cardStartedAt || null, metal: sh.metal, metalLabel: o.sheetName(sh), day, folder: name, fileBase: name, seq: sh.seq || sh.page, runId: sh.runId || null, status: sh.status, endedBy: sh.endedBy, trials: sh.trials, searchMetrics:sh.searchMetrics||null, gpu:sh.gpuProgress||null, elapsedMs: Math.round(sh.elapsedMs || 0), totalMs: Math.round(sh.totalMs || 0),
      stock: sh.outputs.report.stock, params: sh.solveParams, density: sh.density, freePt2: Math.round(sh.liveInfo.freePt2), usablePt2: Math.round(sh.liveInfo.usablePt2), pocket: sh.liveInfo.pocket,
      charmCount: items.length, placedCount: sh.placements.length, rejectCount: (sh.outputs.report.rejects || []).length, page: sh.page, ...(Number.isFinite(sh.draftOrder) ? { draftOrder: sh.draftOrder } : {}), verification: { ok: !!sh.verification.ok, minGapPt: sh.verification.minGapPt, minEdgePt: sh.verification.minEdgePt, render: sh.verification.render && { ok: sh.verification.render.ok, overlapPx: sh.verification.render.overlapPx, outsidePx: sh.verification.render.outsidePx } },
      outputs,
      sources: [...new Set(items.map(c => c.sourceId))].map(id => { const s = o.sourceOf(id); return s ? { id: s.id, name: s.name, zipName: s.zipName, hash: s.hash, bytes: s.bytes ? s.bytes.length : 0, path: s.cloud && s.cloud.path, url: s.cloud && s.cloud.url, pool: !!s.pool, custom: !!s.custom, sku: s.sku || null } : null; }).filter(Boolean),
      dirty:!!sh.dirty, draft: member.draft, solidIncluded:["gold10k","gold14k"].includes(sh.metal) ? !!(typeof o.solidSelected === "function" && o.solidSelected(sh.metal, sh)) : sh.metal === "rose" && sh.solidPick === true ? true : null, releaseFull: !!sh.releaseFull,   /* (SETFIX: a Rose Gold sheet keeps its own tick across a reload, true or nothing) */ topup: sh.topup || null, intakeFinalized: !!sh.intakeFinalized, intakeOptimized:!!sh.intakeOptimized, intakeOptimizedCount:sh.intakeOptimizedCount||0, missRearranged:!!sh.missRearranged, setId: member.setId, setSeq: member.setSeq, sheetIndex: member.sheetIndex, runId: sh.runId || null,
      orders: [...new Set(sh.placements.map(p => { const c = byId.get(p.id); return c && c.order ? String(c.order).split("/")[0] : null; }).filter(Boolean))],
      ...(listings => listings ? { listings } : {})(typeof o.listingsOf === "function" ? o.listingsOf(sh.placements.map(p => byId.get(p.id))) : null),
      poolIds: sh.placements.map(p => byId.get(p.id) && byId.get(p.id).poolId).filter(Boolean), backPool: sh.backPool || [], backOutputs: sh.backOutputs || null, label: sh.label || null,
      charms: items.map(c => ({ id: c.id, hash: c.hash, name: c.name, sourceId: c.sourceId, sourceName: c.sourceName, index: c.index, qty: c.qty, order: c.order || c.id, layer: c.layer || null, poolId: c.poolId || null, custom: !!c.custom, sku: c.sku || null, areaPt2: +c.areaPt2.toFixed(1), widthPt: +c.widthPt.toFixed(2), heightPt: +c.heightPt.toFixed(2), thumbUrl: c.cloud && c.cloud.png || null, aiUrl: c.cloud && c.cloud.ai || null, pngPath: c.cloud && c.cloud.pngPath || null, aiPath: c.cloud && c.cloud.aiPath || null, excluded: !!c.excluded, open: !!c.open, ...(typeof o.pairDescriptor === "function" ? o.pairDescriptor(c) : {}), ...(c.manual ? { manual: c.manual } : {}), ...(c.frozen ? { frozen: c.frozen } : {}), ...(shapes.get(c.id) || {}) })),
      placements: sh.outputs.report.placements, rejects: sh.outputs.report.rejects,
      names: items.map(c => c.name).filter(Boolean).join(" ")
    };
  }

  /* ═══ THE AFTER-NEST DECISIONS (cloud plan v2, build step 7b, C4): what finishNest decides once a sheet is written, ported for the cloud's one sheet writer (_charmNestFinish.js),
     each as charm-nest-1.html has it (sheetFull, topupSettle, the intakeOptimized / intakeFinalized rules, sheetFileBase, overflowToNextSheet with rechunkStalled, feedOn). The page keeps
     its own copies and is unchanged; these take what the page reads of its globals in `o`:
       o { maxFill, orderKey(c), orderRank(c), inflatedArea(c), usablePt2, live, manual, topupOff, now(), log(sh, words), NS (the NOSPREAD policy, null when off), groupOf(c),
           topupRoom(sh) (the gap fill's tries left), tag(metal), sheetName(metal, day, seq, index), today() } ═══ */
  const TOPUP = { orders: 35, target: 0.75 };
  const FINISHED_ENDS = ["cap", "stalled", "trials", "claude-full", "plateau", "no-room"];
  const FULL_CEILING = 0.80;   // (the fill line: a sheet whose pieces cover this share of its usable area is full; the sorter's maxFill when it is set)
  const FULL_AFTER_ORDERS = 5;   // (FULLAFTER5, Paul 21 Nov 2026: "the same rule for all percentage sheets": a sheet is full once this many different orders in a row had no room on it, at any fill)
  /** FULLAFTER5: the run of different orders in a row a sheet had no room for, after this nest (sh: the sheet as written, with its rejects; before: the placements it held before the
      nest; items: its pieces; o.orderKey). A nest that seats a new piece starts the run again. → [order keys] */
  function noRoomRunOf(sh, result, before, items, prevRun, o) {
    o = o || {};
    const key = keyOf(o), had = new Set((before || []).map(p => String(p && p.id))), placedNew = (sh.placements || []).some(p => p && !had.has(String(p.id)));
    if (!result || result.endedBy === "stopped" || result.timedOut) return placedNew ? [] : (Array.isArray(prevRun) ? prevRun.map(String) : []);
    const untried = untriedIds(result), byId = new Map((items || []).map(c => [String(c.id), c])), area = areaOf(o), room = (+result.usablePt2 || 0) * (+o.maxFill || FULL_CEILING);
    // (an order counts only when the whole order would fit an empty sheet: one bigger than a sheet says nothing about this sheet's room)
    const fits = k => !(room > 0) || (items || []).filter(c => String(key(c)) === k).reduce((n, c) => n + area(c), 0) <= room;
    /* ONCEWHOLE (Paul, 22 Nov 2026: "One charm does not equal 11%... this is not the right answer"; RG Sheet 1 released with "12 different orders in a row had no room on it", 9 of them
       seated on it): a piece taken off to keep its order whole and oldest first (keepOrdersWhole: result.lifted, result.ordersWhole) FITTED; only what the search turned away counts */
    const fitted = new Set([...(Array.isArray(result.lifted) ? result.lifted : []), ...["lifted", "mates", "wholeMates", "behind"].flatMap(k => (result.ordersWhole && Array.isArray(result.ordersWhole[k]) ? result.ordersWhole[k] : []))].map(String));
    const missed = (sh.rejects || []).map(idOfReject).filter(id => !untried.has(id) && !fitted.has(id)).map(id => (byId.get(id) ? String(key(byId.get(id))) : id)).filter(fits);
    const run = placedNew ? [] : (Array.isArray(prevRun) ? prevRun.map(String) : []);
    for (const k of missed) if (!run.includes(k)) run.push(k);
    return run.slice(-20);
  }
  const keyOf = o => (o && typeof o.orderKey === "function" ? o.orderKey : c => String(c.order || c.id));
  const rankOf = o => (o && typeof o.orderRank === "function" ? o.orderRank : c => +(c && c.orderDate) || 0);
  const areaOf = o => (o && typeof o.inflatedArea === "function" ? o.inflatedArea : c => +(c && c.areaPt2) || 0);
  const idOfReject = r => String(r && typeof r === "object" ? r.id : r);
  const verified = sh => !!(sh && sh.verification && sh.verification.ok);
  const pctWord = v => `${Math.round((+v || 0) * 100)}%`;
  /** The charms a one-by-one fill that ran out of time never reached (charm-nest-1.html untriedIds). */
  const untriedIds = result => new Set(result && result.timedOut ? (result.rejects || []).map(idOfReject).filter(id => !(result.noRoom || []).map(String).includes(id) && !(result.capped || []).map(String).includes(id)) : []);
  /** The fill the sheet card shows, rounded as it shows it (charm-nest-1.html shownFull). */
  const shownFull = result => Math.round(100 * (+result.usablePt2 ? 1 - (+result.freePt2 || 0) / +result.usablePt2 : +result.density || 0)) / 100;
  /** The charms on the sheet less those waiting for their turn (charm-nest-nest-plan.js nestItems). */
  const nestItemsOf = sh => { const later = new Set((sh.feedWait || []).map(String)); return (sh.charms || []).filter(c => c && !c.excluded && !later.has(String(c.id))); };

  /** Full, for release (charm-nest-1.html sheetFull): the ceiling is reached, or a whole order that would fit an empty sheet overflows a search that finished. */
  function sheetFull(sh, result, items, o) {
    o = o || {};
    const unitKey = keyOf(o), area = areaOf(o), cap = +o.maxFill || 0.8, byId = new Map(items.map(c => [c.id, c]));
    const finished = FINISHED_ENDS.includes(result.endedBy) || (result.endedBy === "budget" && sh.intakePhase !== "fill" && result.trials !== 0);
    const untried = untriedIds(result), missed = (sh.rejects || []).map(idOfReject).filter(id => !untried.has(id));
    /* FULLAFTER5 (Paul, 21 Nov 2026: "the same rule for all percentage sheets … all partial draft sheets only and moving forward"): on a DRAFT a piece a finished search turned away no
       longer makes the sheet full by itself, at any fill: the draft is full once FULL_AFTER_ORDERS different orders in a row had no room on it (the writer's afterNestMarks, noRoomRunOf), or when
       its pieces reach the fill line (below). A sheet in a set keeps the rule it had */
    const overflow = !sh.draft && missed.length > 0 && ((+result.density || 0) + 1e-6 >= Math.min(0.74, cap) || finished) &&
      [...new Set(missed.map(id => (byId.get(id) ? unitKey(byId.get(id)) : id)))].some(order => items.filter(c => unitKey(c) === order).reduce((n, c) => n + area(c), 0) <= (+result.usablePt2 || 0) * cap);
    const passedAtDoor = !!sh._nsCheck && !!sh._nsPassed && ((+result.density || 0) + 1e-6 >= Math.min(0.74, cap - 0.004) || result.smallRoom === false); sh._nsCheck = false; sh._nsMiss = passedAtDoor;
    return result.endedBy !== "stopped" && verified(sh) && (sh.placements || []).length > 0 && (overflow || passedAtDoor || (+result.placedPt2 || 0) / Math.max(1, +result.usablePt2 || 0) >= cap - 1e-6);
  }
  /** The multi-piece orders a gap fill turned away this update (charm-nest-1.html notePassed: sh.topup.passed). */
  function notePassed(sh, o) {
    const t = sh.topup; if (!t) return;
    const unitKey = keyOf(o), items = nestItemsOf(sh), byId = new Map(items.map(c => [String(c.id), c])), size = new Map(); for (const c of items) size.set(unitKey(c), (size.get(unitKey(c)) || 0) + 1);
    const seen = new Set((t.passed || []).map(String)); let fresh = 0;
    for (const id of (sh.rejects || []).map(idOfReject)) { const c = byId.get(id); if (c && size.get(unitKey(c)) > 1 && !seen.has(unitKey(c))) { seen.add(unitKey(c)); fresh++; } }
    if (fresh) t.passed = [...seen];
  }
  /** The gap fill (charm-nest-1.html topupSettle): a Gold Filled / Sterling Silver sheet whose next order missed stays first in line for the next 35 orders, until 75 % full, the ceiling, no room, or
   *  the run stops bringing orders (o.live false). → whether the sheet is released full now; sh.topup / sh.releaseWhy are kept on the sheet. */
  function topupSettle(sh, result, full, o) {
    o = o || {};
    const cap = +o.maxFill || 0.8, t = sh.topup, now = typeof o.now === "function" ? o.now() : Date.now(), log = typeof o.log === "function" ? o.log : () => {};
    if (!["gold", "silver"].includes(sh.metal) || sh.roseProtected || result.endedBy === "stopped" || !verified(sh) || o.topupOff) return full;
    const atCeiling = (+result.placedPt2 || 0) / Math.max(1, +result.usablePt2 || 0) >= cap - 0.004;
    const reached = shownFull(result) + 1e-9 >= Math.min(TOPUP.target, cap);
    const noRoom = result.smallRoom === false, live = !!o.live, rejects = sh.rejects || [];
    if (!t) {
      if (sh.keepRelease && sh.keepRelease.full) return full;
      if (!full || atCeiling || reached || noRoom || (!rejects.length && !sh._nsMiss) || !live) {
        if (full && !atCeiling && !reached) {
          const why = noRoom ? "no room left for even the smallest charms" : !rejects.length ? "no order is waiting to try its gaps" : o.manual ? "Manual mode brings no later orders to try its gaps" : "it is not a sheet of the live Auto run, so no later orders come to try its gaps";
          if (sh.releaseWhy !== why) { sh.releaseWhy = why; log(sh, `Released at ${pctWord(shownFull(result))} without a gap fill: ${why}`); }
        }
        return full;
      }
      delete sh.releaseWhy;
      sh.topup = { at: now, base: +(+result.density || 0).toFixed(4), tried: [], placed: (sh.placements || []).length };
      log(sh, `Filling its gaps at ${pctWord(result.density)}: the order that missed moves to the next sheet, and the next ${TOPUP.orders} orders try this sheet first`);
      notePassed(sh, o);
      return false;
    }
    if (t.closedAt) return full;
    const unitKey = keyOf(o), before = new Set((sh.nestInitial || []).map(p => p.id)), tried = new Set(t.tried || []), untried = untriedIds(result), items = nestItemsOf(sh);
    const skipped = new Set(items.filter(c => untried.has(String(c.id))).map(unitKey));
    for (const c of items) if (!before.has(c.id) && !skipped.has(unitKey(c))) tried.add(unitKey(c));
    t.tried = [...tried]; t.placed = (sh.placements || []).length; notePassed(sh, o);
    if (!live || atCeiling || reached || noRoom || t.tried.length >= TOPUP.orders) {
      t.closedAt = now; t.density = +(+result.density || 0).toFixed(4);
      t.why = reached ? `${pctWord(shownFull(result))} full` : atCeiling ? "at the fill ceiling" : noRoom ? "no room left for even the smallest charms" : !live ? "the run stopped bringing orders" : `${TOPUP.orders} later orders tried`;
      log(sh, `Gap fill done, ${t.why}: ${pctWord(t.base)} → ${pctWord(result.density)}, sheet released`);
      return true;
    }
    return false;
  }
  /** The intake marks a write leaves (charm-nest-1.html finishNest: intakeOptimized once a search that is not the quick gap fill ended verified; intakeFinalized for the final phase only). */
  function intakeMarks(sh, result, items) {
    const out = {}, ok = result.endedBy !== "stopped" && verified(sh);
    if (sh.intakePhase !== "fill" && ok) { out.intakeOptimized = true; out.intakeOptimizedCount = items.length; }
    out.intakeFinalized = sh.intakePhase === "final" && ok && (sh.placements || []).length > 0;
    return out;
  }
  /** A sheet's file name (charm-nest-1.html sheetFileBase): a draft, or one owed its number, is `${TAG}_working_${id}`; a sheet of a set is named by its set's day, number and place. */
  function sheetFileBase(sh, o) {
    o = o || {};
    const tag = typeof o.tag === "function" ? o.tag(sh.metal) : String(sh.metal || "").toUpperCase();
    if (sh.draft || sh.numberOwed) return `${tag}_working_${sh.sheetId || sh.id}`;
    return o.sheetName(sh.metal, sh.setDay || (typeof o.today === "function" ? o.today() : new Date().toISOString().slice(0, 10)), sh.seq || 1, sh.sheetIndex || sh.page || 1);
  }
  /** rechunkStalled (charm-nest-1.html): the oldest unit turned away from an EMPTY sheet is cut in two (the NOSPREAD policy's halve) and the sheet nests again. → { chunks: { charmId: n }, words } | null */
  function rechunkStalled(sh, moving, o) {
    o = o || {};
    const NS = o.NS; if (!NS || typeof NS.halve !== "function" || sh._mergeNext) return null;
    const key = keyOf(o), rank = rankOf(o), by = new Map(); for (const c of moving) { const k = key(c); if (!by.has(k)) by.set(k, []); by.get(k).push(c); }
    const first = [...by.values()].sort((a, b) => rank(a[0]) - rank(b[0]) || key(a[0]).localeCompare(key(b[0])))[0];
    const halves = first ? NS.halve(first, areaOf(o), typeof o.groupOf === "function" ? o.groupOf : null, 0.5 * (+o.usablePt2 || 0) * (+o.maxFill || 0.8)) : [];
    if (halves.length < 2) return null;
    const base = String(first[0].order || first[0].id), units = new Map();
    for (const c of sh.charms || []) if (c && !c.excluded && String(c.order || c.id) === base) { const k = key(c); if (!units.has(k)) units.set(k, []); units.get(k).push(c); }
    const seq = []; for (const k of [...units.keys()].sort()) { if (k === key(first[0])) seq.push(halves[0], halves[1]); else seq.push(units.get(k)); }
    const chunks = {}; seq.forEach((u, i) => u.forEach(c => { chunks[String(c.id)] = i + 1; }));
    return { chunks, words: `Order ${base.split("/")[0]}: ${first.length} piece(s) did not pack into an empty sheet whole; cut in two (${halves[0].length} + ${halves[1].length}) and nested again, in order` };
  }
  /** overflowToNextSheet's decision (charm-nest-1.html): what the sheet turned away (sh.rejects, not placed) and what becomes of it.
   *  → { kind: "none" } | { kind: "move", moving } (to the next sheet) | { kind: "rechunk", moving, chunks, words } | { kind: "stop", moving, why } (the oldest order cannot fit on an empty sheet) */
  function overflowPlan(sh, o) {
    o = o || {};
    const ids = new Set((sh.rejects || []).map(idOfReject)), placed = new Set((sh.placements || []).map(p => String(p.id)));
    const moving = (sh.charms || []).filter(c => c && !c.excluded && ids.has(String(c.id)) && !placed.has(String(c.id)));
    if (!moving.length) return { kind: "none", moving };
    if (!(sh.placements || []).length) {
      const re = rechunkStalled(sh, moving, o); if (re) return Object.assign({ kind: "rechunk", moving }, re);
      const gid = typeof o.groupOf === "function" ? o.groupOf : () => null, lines = new Map();
      for (const c of moving) { const g = gid(c); if (g) { if (!lines.has(g)) lines.set(g, []); lines.get(g).push(c); } }
      const big = [...lines.values()].sort((a, b) => b.length - a.length)[0] || [];
      const what = big.length < 2 ? "" : big.length === 2 ? "a pair" : `a group of ${big.length} pieces`;
      return { kind: "stop", moving, why: `The oldest order cannot fit on an empty sheet at these settings.${what ? ` It holds ${what} that must stay on one sheet, so it is not split.` : ""} Review its size or the fill ceiling before continuing.` };
    }
    return { kind: "move", moving };
  }
  /** feedOn's decision (charm-nest-1.html feedOn) for a sheet just written whose later orders waited for their turn (sh.feedWait).
   *  → { act: "none" } | { act: "pass", pass: [charm ids] } (they go on to the next sheet) | { act: "nest" } (its next turn) | { act: "pass+nest", pass } */
  function feedNext(sh, o) {
    o = o || {};
    const later = new Set((sh.feedWait || []).map(String)); if (!later.size) return { act: "none", left: [] };
    const placed = new Set((sh.placements || []).map(p => String(p.id))), left = (sh.charms || []).filter(c => c && !c.excluded && later.has(String(c.id)) && !placed.has(String(c.id)));
    if (!left.length || sh.problem || sh.runHold || (sh.endedBy === "stopped" && !sh.resumeWait)) return { act: "none", left };
    /* NOFLEE (Paul, 21 Nov 2026: 77 Gold Filled pieces left the merged draft for a new one): a sheet whose layout missed the gap check (verification not ok) no longer sends its waiting
       orders on: it nests again, as a sheet that passed would; only a sheet released full passes them */
    if (sh.releaseFull && (sh.placements || []).length && sh.endedBy !== "stopped") return { act: "pass", pass: left.map(c => c.id), left };
    if (sh.topup && !sh.topup.closedAt && (sh.placements || []).length && sh.endedBy !== "stopped") {
      const key = keyOf(o), rank = rankOf(o), turn = (a, b) => rank(a) - rank(b) || key(a).localeCompare(key(b));
      const order = [...new Set(left.slice().sort(turn).map(key))], room = typeof o.topupRoom === "function" ? o.topupRoom(sh) : Infinity;
      const keep = new Set(order.slice(0, Math.max(0, room))), pass = left.filter(c => !keep.has(key(c))).map(c => c.id);
      if (pass.length === left.length) return { act: "pass", pass, left };
      if (pass.length) return { act: "pass+nest", pass, left };
    }
    return { act: "nest", left };
  }

  return {
    TOPUP, FULL_CEILING, FULL_AFTER_ORDERS, noRoomRunOf, untriedIds, shownFull, sheetFull, topupSettle, intakeMarks, sheetFileBase, rechunkStalled, overflowPlan, feedNext,
    CHARM_SCALE: CHARM_SCALE_DEFAULT, CAREFUL_ANGLES: CAREFUL_ANGLES_DEFAULT, OVF_MIN_SCALE, EXACT_EPS_MM2, EXACT_MM, EXACT_MORE_SCALE,
    layoutKey, buildJob, sheetSpec, buildReport,
    exchangeHeld, overlapOnly, overlapRound, applyOverlapFixes, overlapMoved, unionArea,
    exactOverlaps, exactFix, exactArea, exactShared, exactMoved,
    membershipOf, record
  };
});
