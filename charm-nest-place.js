/*  charm-nest-place.js — CHOOSING EACH PIECE'S SHEET: the one copy the sorter page and the cloud both run.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Cloud plan v2, build step 5 (P4, P5, C4; Paul, 20 Nov 2026: no production work runs on any computer, the cloud does it all). The sheet a line's pieces go on was
 *  chosen inside charm-nest-bridge.js (Pool.attachPool, LiveNest.intakePage / pairFits / lineFits / orderHome / reopensHome, Pool.greenFits / partnerSheets /
 *  draftsMeet / greenMustWait) and charm-nest-1.html (topupRoom, topupTakes, nospreadSheet, nospreadOrder, nospreadChunk), so only a page could choose it. The
 *  rules are here now, unchanged, and the page calls them; the cloud runs the same rules over the sheet RECORDS of a run (recordHost below).
 *
 *  Loads two ways, one source:  window.CharmNestPlace (the sorter page, before charm-nest-bridge.js)
 *                               module.exports        (node: require("../../charm-nest-place.js"); the cloud driver's placement unit, _charmNestPlaceUnit.js)
 *
 *  The owner's rules it keeps, each where it always was:
 *    R2        a line's late piece goes to the sheet that holds its other pieces (chooseSheet: matePages)
 *    R16       every later piece of a Gold Filled / Sterling Silver order goes to the order's own sheet of its colour, open or released full and not cut, approved or sent
 *              (orderHome / reopensHome; the caller keeps that sheet's release while it nests them: reopenHome)
 *    PAIR      a GF sheet shares orders with one SS sheet and an SS sheet with one GF sheet (pairFits)
 *    PAIR2     a GF or SS sheet shares orders with at most TWO sheets of the other metal, never chaining (pairFits); a Rose Gold / 10K / 14K sheet's two GF or two SS partners
 *              are in one set or one unit (greenFits)
 *    Decision 2  a Rose Gold / 10K / 14K sheet mixes orders with one GF and/or one SS sheet of ONE set (greenFits, draftsMeet): RG / 10K / 14K sheets mix only within one set
 *    RGC1 / NOWAIT  a 10K / 14K line the open layout cannot take waits for its green line; a Rose Gold line goes on another layout or a new draft (chooseSheet: the green-line wait)
 *    NOSPREAD  a multi-piece order goes onto a sheet near completion only if all of it fits; an order bigger than a sheet is cut into units (topupTakes, nospreadChunk)
 *    ONEFILL / PAIRFIX  one Gold Filled / Sterling Silver sheet fills at a time; a line the sheet still filling cannot pair waits for it (the host's addPage / fillingPage)
 *    SETSHUT   a sheet in any set takes no new orders, its own order's later pieces excepted (closed; closedRec on a record; placeOnSheet checks it again in its transaction)
 *    SPLITWAIT no set forms while an order has GF / SS pieces waiting to be placed (splitWaiting, splitHolds: the set assembly's, page and cloud)
 *    KEEPINSET / OWNEROVERRIDE / NOPARTIALSET / MINFLOOR  placement never changes a sheet's set: nothing here writes membership
 *
 *  create(host) → the rules for one host. Everything they read of the world comes through the host; nothing here reaches a page global, the network or Etsy.
 *    sheets()              every sheet (the page: allSheets(); the cloud: the run's records as views)
 *    pagesOf(metal)        the sheets of a metal in line order, oldest first (the newest is the last)
 *    closed(p)             the sheet takes no new order (the page: LiveNest.closed; the cloud: closedRec)
 *    holding(p)            kept for a change of its own (the page: Gate.holding; records keep no such mark)
 *    fixedSet(p)           in a set approved for laser cutting, sent or cut
 *    rows()                the run's order lines as { order: { receiptId }, spec: { material }, state, poolIds } (null: none known)
 *    run()                 the run (pairFree: the orders a person sent on outside the pairing rule)
 *    settings()            the sorter's settings (onesheet, nospread)
 *    r16On(), fast(m), cuts(m)   R16 on; a Gold Filled / Sterling Silver metal; a metal with a green line (Rose Gold, 10K, 14K)
 *    addPage(m, o), fillingPage(m), greenPage(m, run), reopenHome(p, rid)   the sheet still filling or a new one (ONEFILL / PAIRFIX: o.ok, o.wait), the next
 *                          green-line sheet, the own-order keep
 *    topupRoom(p), topupTakes(p, pieces, info)   the gap fill's room and NOSPREAD's admission (absent: every sheet has room)
 *  topupRoom / topupTakes / nospreadSheet / nospreadOrder / nospreadChunk (sheet, ..., env): the page's fill model handed in as env { settings(), NS() (the NOSPREAD policy,
 *  null when it is off), cfg(), inflatedArea(c), usableArea(sh), activeCharms(sh), orderKey(c), gapFilling(sh), groupOf(c), topup(), gapPack(), say(), passed(), chunked() }.
 *
 *  ON RECORDS (the cloud). A sheet record is read as the page reads a page (recordView): its pieces are the charms its poolIds lists (Charm_Nest_Sheets is the one truth of
 *  "on a sheet", _charmNestPlacement.js), held while a person moves its pieces by hand (manualEdit, alive 90 s) or a heavy task works on it (nestJob / filesJob), its run's
 *  holds from run.sheetHolds. recordHost(...) makes the host over them, with NEW DRAFTS as views not yet written: the placement unit writes one (placeOnSheet, one transaction)
 *  only when a piece goes on it. nextSheet(...) is the next sheet in line for pieces a nest passed on (the cloud writer's overflow, C4).
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestPlace = api;
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";

  const PAIR_OF = { gold: "silver", silver: "gold" };
  const FAST = ["gold", "silver"];
  const CUT_METALS = ["rose", "gold10k", "gold14k"];
  const METALS = ["gold", "silver", "rose", "gold10k", "gold14k"];
  /** charm-nest-1.html TOPUP and GAP_PACK (the page passes its own through env; these are the cloud's, the same numbers). */
  const TOPUP = { orders: 35, target: 0.75 }, GAP_PACK = 0.5;
  /** charmNestLibrary.js MANUAL_STALE_MS: a Manual Control claim not kept alive this long is a closed tab's. */
  const MANUAL_STALE_MS = 90000;
  /** A heavy task's mark on a sheet record (nestJob, filesJob: { jobId, at }) older than this is a task that died (the lease's 14 minutes from its start, and a margin). */
  const JOB_STALE_MS = 15 * 60000;
  const LINE_DONE = ["gone", "noDesign", "skipped", "committed", "abandoned"];
  const orderOf = c => String(c && c.order != null ? c.order : "").split("/")[0];
  const num = v => (Number.isFinite(+v) ? +v : 0);
  const ms = v => (v && typeof v.toMillis === "function" ? v.toMillis() : v && typeof v === "object" && (v._seconds || v.seconds) ? (v._seconds || v.seconds) * 1000 : typeof v === "string" ? Date.parse(v) || 0 : num(v));
  /* the pure modules this one reads (the page loads them before it; node requires them, and Netlify's bundler follows these names) */
  const NoSpread = () => (root && root.CharmNestNoSpread) || (typeof require === "function" ? require("./charm-nest-nospread.js") : null);
  const NestPlan = () => (root && root.CharmNestNestPlan) || (typeof require === "function" ? require("./charm-nest-nest-plan.js") : null);
  const PairLib = () => (root && root.CharmNestPair) || (typeof require === "function" ? require("./charm-nest-pair.js") : null);

  /* ═══ the gap fill and NOSPREAD (charm-nest-1.html: topupRoom, topupTakes, nospreadSheet, nospreadOrder, nospreadChunk), over the fill model a host hands in ═══ */
  const topupOf = env => (env && typeof env.topup === "function" ? env.topup() : TOPUP);
  const gapPackOf = env => (env && typeof env.gapPack === "function" ? env.gapPack() : GAP_PACK);
  /** NOSPREAD's settings as the page reads them (nospreadCfg): the release target, the gap-fill share and the one setting, nospreadMargin. */
  function nospreadCfg(settings) {
    const s = settings || {};
    return { target: TOPUP.target, fitAtTarget: GAP_PACK, ...(s.nospreadMargin != null && s.nospreadMargin !== "" && Number.isFinite(+s.nospreadMargin) ? { nearMargin: +s.nospreadMargin } : {}) };
  }
  const cfgOf = env => (env && typeof env.cfg === "function" ? env.cfg() : nospreadCfg(env && env.settings ? env.settings() : {}));
  /** The orders a sheet filling its gaps can still take for a try: 35 less those that have tried it and (waiting) less those already on it waiting for their try; any other sheet has no such limit. */
  function topupRoom(sh, waiting, env) {
    if (waiting === undefined) waiting = true;
    const t = sh.topup; if (!t || t.closedAt || !FAST.includes(sh.metal)) return Infinity;
    const tried = new Set((t.tried || []).map(String));
    let room = topupOf(env).orders - tried.size;
    if (waiting) { const placed = new Set((sh.placements || []).map(p => p.id)); room -= new Set(env.activeCharms(sh).filter(c => !placed.has(c.id)).map(c => env.orderKey(c)).filter(o => !tried.has(o))).size; }
    return room;
  }
  /** The sheet as the NOSPREAD policy reads it: what is seated, what is attached and waiting (reserved: it will take room), `skip` (a Set of charm ids) not counted. */
  function nospreadSheet(sh, skip, o, env) {
    const placed = new Set((sh.placements || []).map(p => p.id)); let placedPt2 = 0, pendingPt2 = 0;
    for (const c of env.activeCharms(sh)) { if (skip && skip.has(c.id)) continue; const a = env.inflatedArea(c); if (placed.has(c.id)) placedPt2 += a; else if (!(o && o.nestedOnly)) pendingPt2 += a; }   // (ONEATATIME: nestedOnly, see topupTakes)
    return { usablePt2: env.usableArea(sh), placedPt2, pendingPt2, maxFill: +env.settings().maxFill || .8, gapFilling: env.gapFilling(sh) };
  }
  /** The order as the policy reads it. `info` (what the intake knows of the order): { pieces, expected, placed }. */
  function nospreadOrder(pieces, info, env) {
    const ps = info && info.pieces && info.pieces.length ? info.pieces : pieces || [], placed = Math.max(0, Math.floor(+(info && info.placed) || 0)), expected = Math.max(0, Math.floor(+(info && info.expected) || 0));
    return env.NS().orderOf({ pieces: ps, placed, more: Math.max(0, expected - ps.length - placed) }, c => env.inflatedArea(c));
  }
  /** Whether an order (its `pieces`) should try `sh`: NOSPREAD's rules A and B over the WHOLE order on every Gold / Silver sheet; with the policy off, a multi-piece order that asks for
      more than GAP_PACK of the room a gap-filling sheet showed last goes on to the next sheet. */
  function topupTakes(sh, pieces, order, env) {
    const NS = env.NS();
    if (NS && FAST.includes(sh.metal)) {
      if (!pieces || !pieces.length) return true;
      // (ONEATATIME, Paul 11 Nov 2026: a sheet still filling is judged by what is nested on it; a sheet filling its gaps keeps the waiting pieces counted)
      const o = nospreadOrder(pieces, order, env), a = NS.admits(nospreadSheet(sh, new Set(pieces.map(c => c.id)), { nestedOnly: !env.gapFilling(sh) }, env), o, cfgOf(env));
      if (!a.take) { if (typeof env.say === "function") env.say(sh, pieces, o, a); if (typeof env.passed === "function") env.passed(sh, a); }
      return a.take;
    }
    if (!env.gapFilling(sh) || !pieces || pieces.length < 2) return true;
    const info = sh.liveInfo; if (!info || !(+info.usablePt2 > 0)) return true;
    const room = Math.min(+info.freePt2 || 0, (+env.settings().maxFill || .8) * info.usablePt2 - (+info.placedPt2 || 0));
    return pieces.reduce((n, c) => n + env.inflatedArea(c), 0) <= Math.max(0, room) * gapPackOf(env);
  }
  /** An order bigger than one sheet, cut into units for the nest (`chunk` 1, 2, ...: consecutive units each seated whole on a sheet in turn, a pair or a necklace's discs kept in one
      unit). Every metal. Returns the units ([] when the order is not big, or the policy is off). env.chunked(metal, pieces, order, capacity, units): the page's words. */
  function nospreadChunk(metal, pieces, info, env) {
    const NS = env.NS(); if (!NS || !pieces || pieces.length < 2) return [];
    const capacity = env.usableArea({ metal }) * (+env.settings().maxFill || .8), order = nospreadOrder(pieces, info, env), cfg = cfgOf(env);   // (a big order starts a fresh sheet, which has the metal's own stock)
    if (!NS.isBig(order, capacity, cfg)) return [];
    const units = NS.chunksOf(pieces, c => env.inflatedArea(c), typeof env.groupOf === "function" ? env.groupOf : null, capacity, cfg);
    if (units.length > 1) {
      units.forEach((u, i) => u.forEach(c => { c.chunk = i + 1; }));
      if (typeof env.chunked === "function") env.chunked(metal, pieces, order, capacity, units);
    }
    return units;
  }

  /* ═══ SPLITWAIT (Paul, 20 Nov 2026: "no set can be formed when there are mixed orders between coloured sheets"), the set assembly's: the orders whose Gold Filled or Sterling Silver
     piece still waits to be placed (on no sheet yet), and the free sheets that wait with every sheet they share an order with ═══ */
  /** rows: the run's order lines ({ order: { receiptId }, spec: { material }, state, poolIds }); sheets: every sheet. → Map orderId → "gold" | "silver" (the metal that waits). */
  function splitWaiting(rows, sheets) {
    const onSheet = new Set((sheets || []).flatMap(p => (p.charms || []).map(c => c.poolId)).filter(Boolean)), out = new Map();
    for (const r of rows || []) {
      if (!r || !r.order || !["pulled", "waiting", "pooled"].includes(r.state) || (r.spec && !["gold", "silver"].includes(r.spec.material))) continue;
      const ids = r.poolIds || []; if (ids.length && ids.every(id => onSheet.has(id))) continue;
      const k = String(r.order.receiptId); if (!out.has(k)) out.set(k, r.spec && r.spec.material === "silver" ? "silver" : "gold");
    }
    return out;
  }
  /** The free sheets that hold such an order, and every free sheet tied to them by shared orders (links: [{ ids: [sheet] }]). → Map sheet → [orderId]. */
  function splitHolds(free, waitingOf, links) {
    const holds = new Map((free || []).map(p => [p, [...new Set((p.charms || []).map(orderOf).filter(k => waitingOf.has(k)))]]).filter(([, ks]) => ks.length));
    for (const g of links || []) if ((g.ids || []).some(x => holds.has(x))) { const ks = [...new Set(g.ids.flatMap(x => holds.get(x) || []))]; for (const x of g.ids) if (free.includes(x) && !holds.has(x)) holds.set(x, ks); }
    return holds;
  }

  /* ═══ NOSPREAD's turn-away, read from the sheets (RELEASEMARK, Paul 20 Nov 2026: GF Draft 1 sat at 78% and never released while four later Gold Filled sheets took the orders). The page counted
     a whole order turned away from a sheet near completion in its own memory only (_nsPassed): a reload, or another computer carrying the run, lost it and the sheet was never brought to the one nest
     that releases it. ONEFILL fills one Gold Filled / Sterling Silver sheet at a time, so a LATER sheet of the same metal and run that holds pieces is the record's own proof that this sheet was passed
     over (refused or shut); every computer and the cloud read the same answer from the same records. ═══ */
  /** passedOver(sheet, sheets): a Gold Filled / Sterling Silver sheet with pieces on it, and a sheet of its metal and run started after it (cardStartedAt, createdAt; else a higher page) holds pieces. */
  function passedOver(sh, sheets) {
    if (!sh || !FAST.includes(sh.metal) || !(sh.placements || []).length || sh.recalled || sh.archived) return false;
    const born = s => ms(s && (s.cardStartedAt || s.createdAt)) || 0, idOf = s => (s && (s.sheetId || s.id)) || null, mine = born(sh), me = idOf(sh);
    return (sheets || []).some(q => q && q !== sh && (!me || idOf(q) !== me) && q.metal === sh.metal && (!sh.runId || q.runId === sh.runId) && !q.recalled && !q.archived && (q.placements || []).length > 0
      && (mine && born(q) ? born(q) > mine : num(q.page) > num(sh.page)));
  }

  /* ═══ the rules for one host ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  function create(H) {
    const sheets = () => H.sheets() || [];
    const pagesOf = m => H.pagesOf(m) || [];
    const closed = p => !!H.closed(p);
    const lineCuts = m => !!H.cuts(m);
    const r16On = () => !!H.r16On();
    const hasRoom = p => !H.topupRoom || H.topupRoom(p) > 0;
    const settingsOf = () => (H.settings ? H.settings() || {} : {});

    /* ── PAIR · one Gold Filled sheet and one Sterling Silver sheet share orders (Paul, 26 Oct 2026), PAIR2 (20 Nov 2026) ── */
    /** The run's sheets of metal `m` holding a piece of one of the orders `ids` (a Set). */
    function sheetsHolding(m, ids, run, skip) {
      const out = new Set(); if (!ids.size) return out;
      for (const p of sheets()) { if (p.metal !== m || p.recalled || (run && p.runId && p.runId !== run.runId)) continue; if ((p.charms || []).some(c => !(skip && skip.has(c)) && ids.has(orderOf(c)))) out.add(p); }
      return out;
    }
    const ordersHeld = (p, skip) => new Set((p.charms || []).filter(c => !(skip && skip.has(c))).map(orderOf).filter(Boolean));
    /** Lines of these orders of metal `m` that are not on any sheet yet (the order rows; a world without them knows of none): they come later, to the sheet's partner. */
    function linesToCome(ids, m) {
      try {
        const rows = H.rows ? H.rows() : null; if (!rows) return false;
        const on = new Set(sheets().flatMap(p => (p.charms || []).map(c => c.poolId)).filter(Boolean));
        return rows.some(r => r && r.order && ids.has(String(r.order.receiptId)) && r.spec && r.spec.material === m && !LINE_DONE.includes(r.state) && !(r.poolIds || []).some(id => on.has(id)));
      } catch (_) { return false; }
    }
    /* PAIRFREE (Paul, 16 Nov 2026): an order a PERSON sent on although its GF and SS pieces are not on one pair of sheets (run.pairFree) is not judged by the pairing rule any more. */
    const pairFreeOf = run => { try { const r = run || (H.run ? H.run() : null) || null; return new Set(((r && r.pairFree) || []).map(String)); } catch (_) { return new Set(); } };
    /** May this GF / SS sheet take pieces of these orders (skip: pieces on their way elsewhere, not counted). */
    function pairFits(page, orderIds, run, o = {}) {
      const other = page && PAIR_OF[page.metal]; if (!other) return true;
      const free = pairFreeOf(run);
      const ids = new Set([].concat(orderIds == null ? [] : orderIds).map(String).filter(x => x && !free.has(x))); if (!ids.size) return true;
      const skip = o.skip || null, heldHere = ordersHeld(page, skip); for (const x of free) heldHere.delete(x);
      const mine = sheetsHolding(other, heldHere, run, skip), theirs = sheetsHolding(other, ids, run, skip);
      // (PAIR2, Paul 20 Nov 2026: sets of 2 GF + 1 SS are normal; "I'm afraid this new rule is gonna … create endless chains of sheets in one set"): a GF or SS sheet shares orders with at most TWO
      //  sheets of the other metal, and only when neither of them shares orders with another sheet of this one's metal, so a unit is at most 2 + 1 sheets and never chains on
      const partners = new Set([...mine, ...theirs]);
      if (partners.size > 2) return false;
      if (partners.size === 2) for (const q of partners) { const qs = sheetsHolding(page.metal, ordersHeld(q, skip), run, skip); qs.delete(page); if (qs.size) return false; }
      // (will go to: the order's lines of the other metal not on a sheet yet go to this sheet's one partner; when that partner takes nothing any more, they could not, so this sheet does not take the order)
      if (mine.size === 1 && !theirs.size && closed([...mine][0]) && linesToCome(ids, other)) return false;
      return true;
    }
    /** The same for any metal: pairFits for GF / SS, greenFits for Rose Gold / 10K / 14K. */
    function lineFits(page, orderIds, run, o = {}) {
      if (!page) return true;
      if (PAIR_OF[page.metal]) return pairFits(page, orderIds, run, o);
      return greenFits(page, orderIds, run, o);
    }

    /* ── R16 HARD · all the pieces of a multi-piece order on ONE sheet of its colour (Paul, 26 Oct 2026) ── */
    /** A released sheet takes its own order's late pieces: not cut, not recalled, not in a set approved for laser cutting, sent or cut, not held by a person, by a replaced piece,
        by a problem, or for ANOTHER order's change. */
    function reopensHome(p, run, rid) {
      if (!p || !PAIR_OF[p.metal] || p.recalled || p.roseCutAt || +p.laserDoneAt > 0 || p.processReady || p._manualHold || p.problem) return false;   // (HOLDTAKES; NOBUSY: a sheet being nested or written is never closed for that)
      if (run && p.runId && p.runId !== run.runId) return false;
      if (H.holding && H.holding(p) && !(rid != null && p.keepRelease && p.keepRelease.rid === String(rid).split("/")[0])) return false;   // (kept for another order's change)
      return !(H.fixedSet && H.fixedSet(p));
    }
    /** The sheet of metal m holding pieces of the order that can take more: open, else released full and REOPENABLE. null: a new order; an order bigger than one sheet (its units
        fill consecutive sheets by design); one already on several sheets or whose sheet is cut, approved or sent (a split of necessity). */
    function orderHome(m, run, orderId, o = {}) {
      if (!PAIR_OF[m] || orderId == null || orderId === "" || !r16On()) return null;
      const oid = String(orderId).split("/")[0], skip = o.skip ? new Set(o.skip) : null;
      const mine = p => (p.charms || []).filter(c => !(skip && skip.has(c)) && !c.excluded && orderOf(c) === oid);
      const pages = pagesOf(m).filter(p => !p.recalled && (!run || !p.runId || p.runId === run.runId) && mine(p).length);
      if (!pages.length || pages.some(p => mine(p).some(c => c.chunk))) return null;   // (an order bigger than one sheet: its units fill consecutive sheets by design)
      return pages.find(p => !closed(p)) || pages.find(p => reopensHome(p, run, oid)) || null;
    }
    /* Gold and Silver arrivals go to the run's earliest open sheet first; Rose Gold, 10K and 14K keep the newest sheet, where its green line is. An order goes onto one sheet whole;
       a sheet filling its gaps takes only the orders it still owes a try (topupRoom); a multi-piece order goes onto a sheet near completion only if all of it fits (NOSPREAD,
       topupTakes); only a sheet that pairs (PAIR); the sheet still filling takes the order only when it pairs (PAIRFIX: o.wait, the caller decides that the line waits). */
    function intakePage(m, run, orderId, pieces, order, o = {}) {
      const pages = pagesOf(m), newest = pages[pages.length - 1], fast = !!H.fast(m);
      const open = p => p.charms.length && run && p.runId === run.runId && !closed(p);
      const home = orderId != null && pages.find(p => open(p) && (fast || !(p.rosePlan || p.roseProtected)) && p.charms.some(c => String(c.order) === String(orderId)));
      if (home) return home;
      if (fast && orderId != null) { const back = orderHome(m, run, orderId, { skip: pieces }); if (back) return back; }
      /* CUTDRAFTFILL (Paul, 21 Nov 2026: a 10K order opened 10K Draft 7 for one piece while 10K Draft 4 held three): a Rose Gold / 10K / 14K order tries every open draft of its metal,
         oldest first, before the newest; the green-line rules that follow (greenFits, RGC1) still decide, and a sheet planned or protected for its cut is passed over */
      if (!fast) return pages.find(p => open(p) && !p.recalled && !(p.rosePlan || p.roseProtected)) || newest;
      const room = p => hasRoom(p) && (!pieces || !H.topupTakes || H.topupTakes(p, pieces, order));
      const pairs = p => orderId == null || pairFits(p, [orderId], run);
      const pick = pages.find(p => p !== newest && open(p) && pairs(p) && room(p)) || newest;
      if (pick === newest && open(newest) && (!pairs(newest) || !room(newest))) { const f = H.addPage(m, { ok: pairs, wait: !!o.wait }); return f || newest; }
      return pick;
    }

    /* ── Decision 2 · a Rose Gold / 10K / 14K sheet shares orders with 1 GF and/or 1 SS sheet of ONE set (Paul, 26 Oct 2026); RGONE (9 Nov); PAIR2 (20 Nov) ── */
    function partnerSheets(orderIds, run, skip) {
      const ids = new Set([...orderIds].map(String).filter(Boolean)), out = { gold: new Set(), silver: new Set() };
      if (!ids.size) return out;
      for (const p of sheets()) {
        if (!out[p.metal] || (run && p.runId && p.runId !== run.runId)) continue;
        if (p.charms.some(c => !(skip && skip.has(c)) && ids.has(orderOf(c)))) out[p.metal].add(p);
      }
      return out;
    }
    /** Two drafts outside every set, each still UNPAIRED and the same place in line (the n-th GF and the n-th SS sheet of the run in no set), form the same set (RGONE). */
    function draftsMeet(g, s, run) {
      const inRun = p => !(run && p.runId && p.runId !== run.runId) && !p.archived && !p.recalled, loose = p => !(p.setId && !p.draft);
      const all = sheets().filter(inRun), ordersOf = p => new Set((p.charms || []).map(orderOf).filter(Boolean));
      const paired = (p, otherMetal) => { const mine = ordersOf(p); return all.some(q => q.metal === otherMetal && (q.charms || []).some(c => mine.has(orderOf(c)))); };
      if (paired(g, "silver") || paired(s, "gold")) return false;
      const rank = p => all.filter(q => q.metal === p.metal && loose(q)).indexOf(p);
      const rg = rank(g); return rg >= 0 && rg === rank(s);
    }
    /** greenFits(page, orderId | [orderId], run, {skip}): may this green-line sheet take pieces of these orders (skip: pieces on their way elsewhere, not counted). */
    function greenFits(page, orderId, run, o) {
      const skip = (o && o.skip) || null, add = partnerSheets([].concat(orderId), run, skip); if (!add.gold.size && !add.silver.size) return true;      // (an order with no GF / SS piece ties the sheet to nothing)
      const mine = partnerSheets(page.charms.filter(c => !(skip && skip.has(c))).map(orderOf), run, skip);
      const gf = new Set([...mine.gold, ...add.gold]), ss = new Set([...mine.silver, ...add.silver]);
      if (gf.size > 2 || ss.size > 2) return false;
      // (PAIR2, Paul 20 Nov 2026: "the rose gold and a 10 K and a 14 K should be allowed to match to such a set and be able to have mixed orders with all the sheets and the given set"): with two GF or two SS
      //  partners, every partner is in ONE set, or none is in a set and they are one unit (tied together by shared orders): the sheet still goes with exactly one set
      if (gf.size > 1 || ss.size > 1) {
        const parts = [...gf, ...ss], setOf = p => (p.setId && !p.draft ? p.setId : null), sets = new Set(parts.map(setOf));
        if (sets.size > 1) return false;
        if (!sets.has(null)) return true;
        const ord = p => new Set((p.charms || []).filter(c => !(skip && skip.has(c))).map(orderOf).filter(Boolean)), seen = new Set([parts[0]]), todo = [parts[0]];
        while (todo.length) { const a = todo.pop(), oa = ord(a); for (const b of parts) if (!seen.has(b) && [...ord(b)].some(x => oa.has(x))) { seen.add(b); todo.push(b); } }
        return seen.size === parts.length;
      }
      // (both partners in one set; or neither in a set yet and each the other's one partner, PAIR: they share an order, so they are one unit and one set; or both still unpaired and the same place in
      //  line, RGONE above. Two drafts that share nothing and are paired elsewhere could end in two sets)
      if (gf.size && ss.size) { const setOf = p => (p.setId && !p.draft ? p.setId : null), g = [...gf][0], s = [...ss][0], a = setOf(g), b = setOf(s); if (a !== b) return false; if (!a) { const og = new Set(g.charms.map(orderOf).filter(Boolean)); if (!s.charms.some(c => og.has(orderOf(c))) && !draftsMeet(g, s, run)) return false; } }
      return true;
    }
    /** The open (uncut) automatic layout of a green-line metal: it has pieces, it is of the run, not cut, not recalled, not released full, not marked cut by the laser. */
    const greenOpen = (p, run) => !!p && p.charms.length > 0 && (!run || !p.runId || p.runId === run.runId) && !p.roseCutAt && !p.recalled && !p.releaseFull && !(+p.laserDoneAt > 0);
    /** RGC1: should this line wait for the open layout `was` instead of opening a page? (greenFits refused it there) */
    function greenMustWait(was, rid, run, metal, list) {
      if (!greenOpen(was, run)) return false;
      // (3: a formed set never waits for it: its GF / SS sheets are in a formed set the open layout's are not in)
      const formed = p => p.setId && !p.draft ? String(p.setId) : null, mine = partnerSheets([rid], run), theirs = partnerSheets(was.charms.map(orderOf), run);
      const mySets = new Set([...mine.gold, ...mine.silver].map(formed).filter(Boolean)), theirSets = new Set([...theirs.gold, ...theirs.silver].map(formed).filter(Boolean));
      if ([...mySets].some(s => !theirSets.has(s))) return false;
      // (the order's own sheet of this colour elsewhere, R16: it goes there, as before)
      try { const home = orderHome(metal, run, rid, { skip: list }); if (home && home !== was) return false; } catch (_) { /* no home: it waits */ }
      return true;
    }

    /* ── Pool.attachPool's choice of the sheet for one line's pieces of one metal ── */
    /** o: { metal, rid (the order), run, list (the line's pieces of this metal), info (orderInfo), matePages (the sheets holding the line's other pieces, R2), byMetalSize (how many metals
        the line has), shut(p) (closed, with the caller's own exceptions; default closed), ip ({ page }: INPLACE's own sheet, the page only), relinkHomes (LISTPICK's home sheets, the
        page only) and relinkPrep(metal) (PICKFULL's reopening of them, called first) }.
        → { page, back (the order's own sheet, R16, null when none), wait: null | { kind: "green" | "pair", sheet }, events: [{ kind: "green" | "pair", alt, page } | { kind: "split", apart }] }
        The host's addPage / greenPage / reopenHome are called on the way exactly as the page called them. */
    function chooseSheet(o) {
      const metal = o.metal, run = o.run || null, list = o.list || [], info = o.info, rid = String(o.rid), ip = o.ip || null, matePages = o.matePages || [], byMetalSize = o.byMetalSize || 1, events = [];
      const shut = typeof o.shut === "function" ? o.shut : closed, inRun = p => !run || !p.runId || p.runId === run.runId, gs = metal === "gold" || metal === "silver";
      let page = ip ? ip.page : intakePage(metal, run, o.rid, list, info, { wait: gs && byMetalSize === 1 });
      // (the line's own sheet first, when it still takes pieces: the order's home sheet can be an earlier one, and a pair is never put half on each: R2)
      if (!ip) { const open = matePages.filter(p => p.metal === metal && !shut(p) && inRun(p)); if (open.length && !open.includes(page)) page = open[0]; }
      // (LISTPICK, Paul 18 Oct 2026: a line made up again because its listing's charm was picked goes first into the vacant space its old pieces left; the page only)
      if (!ip && !matePages.length && Array.isArray(o.relinkHomes)) {
        if (typeof o.relinkPrep === "function") o.relinkPrep(metal);
        const home = o.relinkHomes.find(p => p && p.metal === metal && pagesOf(metal).includes(p) && !shut(p) && inRun(p)); if (home) page = home;
      }
      // (Decision 2: one GF and/or one SS partner sheet, in one set. NOWAIT, Paul 14 Nov 2026: a Rose Gold line is never left waiting for the open layout's green line; it goes on another open
      //  layout that keeps the rule, else a new draft. 10K / 14K lines wait as before: RGC1, one physical sheet at a time)
      if (!ip && lineCuts(metal) && !matePages.includes(page) && !greenFits(page, rid, run)) {
        const was = page, alt = pagesOf(metal).find(p => p !== was && !shut(p) && inRun(p) && greenFits(p, rid, run));
        if (metal !== "rose" && !alt && byMetalSize === 1 && greenMustWait(was, rid, run, metal, list)) return { page: was, back: null, wait: { kind: "green", sheet: was }, events };
        page = alt || H.greenPage(metal, run);
        events.push({ kind: "green", alt: !!alt, page });
      }
      // (PAIR, Paul 26 Oct 2026: the mirror for Gold Filled and Sterling Silver, for the sheets chosen around intakePage; an order with pieces of this colour on the sheet already keeps them there, R16.
      //  PAIRFIX, Paul 12 Nov 2026: the sheet still filling is used only when it pairs; when it does not, a one-colour line WAITS for it to fill, then goes on the next sheet)
      if (!ip && gs && !matePages.includes(page) && !page.charms.some(c => orderOf(c) === rid) && !pairFits(page, [rid], run)) {
        const was = page, room = p => hasRoom(p) && (!H.topupTakes || H.topupTakes(p, list, info));
        const alt = pagesOf(metal).find(p => p !== was && p.charms.length && !shut(p) && inRun(p) && pairFits(p, [rid], run) && room(p));
        const home16 = orderHome(metal, run, rid, { skip: list });
        const nx = alt || home16 || H.addPage(metal, { ok: p => pairFits(p, [rid], run), wait: byMetalSize === 1 && settingsOf().onesheet !== "off" });
        if (!nx) { const f = (H.fillingPage ? H.fillingPage(metal) : null) || was; return { page: was, back: null, wait: { kind: "pair", sheet: f }, events }; }
        page = nx;
        events.push({ kind: "pair", alt: !!alt, page });
      }
      // (R16 HARD, Paul 26 Oct 2026: the order's own sheet of this colour wins over every choice above, open or released full and not cut, approved or sent; it keeps its release and its set while
      //  it nests them, reopenHome. A sheet that cannot take them any more leaves a split of necessity, said: the "split" event)
      const back = !ip ? orderHome(metal, run, rid, { skip: list }) : null;
      if (back && back !== page) page = back;
      if (!ip && !back && gs && r16On()) {
        const apart = pagesOf(metal).filter(p => p !== page && inRun(p) && !p.recalled && (p.charms || []).some(c => !list.includes(c) && !c.excluded && !c.chunk && orderOf(c) === rid));
        if (apart.length) events.push({ kind: "split", apart });
      }
      // (a sheet of another run, or one that is shut and is not the order's own: the next sheet. PAIRFIX: the sheet still filling only when it pairs. RGC1: a green-line metal's next sheet
      //  continues on its newest cut's leftover)
      if (!ip && ((run && page.runId && page.runId !== run.runId) || shut(page) && page !== back)) page = lineCuts(metal) ? H.greenPage(metal, run) : H.addPage(metal, gs ? { ok: p => pairFits(p, [rid], run) } : {});
      if (back && page === back && H.reopenHome) H.reopenHome(page, rid);
      return { page, back: back || null, wait: null, events };
    }

    /* ── the next sheet in line for pieces a nest passed on (charm-nest-1.html overflowToNextSheet's choice; the cloud writer's overflow, C4) ──
       sh: the sheet that passed them on; moving: its pieces that did not fit. Gold / Silver: the first sheet after it in line that takes every multi-piece order moving on whole (NOSPREAD)
       and keeps the pairing rule (lineFits), else the first in that line; Rose Gold / 10K / 14K: the newest sheet when it keeps the rule. None, or one shut or busy: the sheet still filling
       that keeps the rule (ONEFILL / PAIRFIX), else a new one. o.admitted(p, units) → Set of unit keys a sheet admits (NOSPREAD; absent: all); o.busy(p): the sheet is being written. */
    function nextSheet(sh, moving, o = {}) {
      const metal = sh.metal, line = pagesOf(metal), i = line.indexOf(sh), inLine = FAST.includes(metal), run = sh.runId ? { runId: sh.runId } : null;
      const shutOf = typeof o.shut === "function" ? o.shut : closed, busy = typeof o.busy === "function" ? o.busy : () => false;
      const units = new Map(); const key = typeof o.orderKey === "function" ? o.orderKey : (c => (NestPlan() ? NestPlan().orderKey(c) : String(c.order || c.id)));
      for (const c of moving) { const k = key(c); if (!units.has(k)) units.set(k, []); units.get(k).push(c); }
      const takes = p => { if (typeof o.admitted !== "function") return true; const ok = o.admitted(p, units); return [...units.keys()].every(k => ok.has(k)); };
      const oids = [...new Set(moving.map(orderOf).filter(Boolean))], keeps = p => lineFits(p, oids, run, { skip: new Set(moving) });
      const inLineNext = () => { const after = line.slice(i + 1).filter(p => !shutOf(p) && takes(p)); return after.find(keeps) || after[0] || null; };
      /* (CUTDRAFTFILL: what a Rose Gold / 10K / 14K nest passes on goes to the oldest open draft of its metal that keeps the rule, before the newest and before a new draft) */
      const cutOpen = () => line.find(p => p !== sh && p.charms.length && (!run || !p.runId || p.runId === run.runId) && !shutOf(p) && !busy(p) && !p.recalled && !(p.rosePlan || p.roseProtected) && keeps(p) && takes(p)) || null;
      let next = inLine ? inLineNext() : (cutOpen() || (() => { const last = line.length - 1 > i ? line[line.length - 1] : null; return last && keeps(last) ? last : null; })());
      if (!next || shutOf(next) || (!inLine && busy(next))) next = H.addPage(metal, { except: sh, ok: keeps });
      return next;
    }

    return { sheetsHolding, ordersHeld, linesToCome, pairFreeOf, pairFits, lineFits, r16On, reopensHome, orderHome, intakePage, partnerSheets, draftsMeet, greenFits, greenOpen, greenMustWait,
      chooseSheet, nextSheet, lineCuts };
  }

  /* ═══ ON RECORDS (the cloud) ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  const manualAlive = (m, now) => !!(m && typeof m === "object" && m.tab && now - num(m.at) < MANUAL_STALE_MS);
  /* (NESTBUSY: the cloud's nest marks nestJob { id, status, at } ("queued" until its slots end it "nested", "error", "stopped" or "stale"); the sheet writer's filesJob { jobId, at }.
     Either id counts; a mark that says it ended is no task.) */
  const TASK_ENDED = ["nested", "applied", "done", "error", "stopped", "stale", "superseded", "failed", "skipped"];
  const jobRunning = (j, now) => !!(j && typeof j === "object" && (j.jobId || j.id) && !(num(j.endedAt) > 0) && !TASK_ENDED.includes(String(j.status || j.state || "")) && !(num(j.at) > 0 && now - num(j.at) > JOB_STALE_MS));
  const idOf = s => String((s && (s.id || s.sheetId)) || "");
  const greenPlanned = s => CUT_METALS.includes(s.metal) && !!(s.rosePlanJson || s.rosePlanHash || s.roseProtectedJson || s.rosePlan || s.roseProtected);
  const fixedSetOf = (s, sets) => { const id = idOf(s); return !!id && (sets || []).some(x => x && (x.committedAt || x.processReady || x.laserSetPending || num(x.laserDoneAt) > 0) && (x.sheetIds || []).includes(id)); };
  /** closedRec(sheet, setsOfRun, run, now): LiveNest.closed on a sheet RECORD (P4). True when the sheet takes no new order: its green line was cut, it was released full, the laser marked it
      cut, the set assembly pulls it in with a partner, the run holds it, its intake was finalized (a Rose Gold / 10K / 14K sheet with a planned or protected line excepted, as on the
      page), a person is moving its pieces by hand (manualEdit, alive), a heavy task works on it (nestJob / filesJob), or it is in a set (SETSHUT: its setId, or any set of the run listing it).
      The own-order exception is the caller's (orderHome / reopensHome; placeOnSheet's `home`). */
  function closedRec(s, sets, run, now) {
    if (!s) return true;
    now = now || Date.now();
    if (s.archived) return true;
    if (s.roseCutAt || s.releaseFull || num(s.laserDoneAt) > 0 || s.cardinalPull) return true;
    /* HOLDTAKES (Paul, 21 Nov 2026: a held sheet split CAMP_0903 onto a new 14K draft): a hold stops a sheet's NEST only; it still takes pieces, and the new pieces lift the hold
       (_charmNestNestStep.js) */
    if (!greenPlanned(s) && s.intakeFinalized) return true;
    /* NOBUSY (Paul, 21 Nov 2026: "there's no sheet that should ever be blocked just because it's busy"): a sheet whose nest or files are running takes its pieces all the same (they wait
       for that nest and go on with the next one); it no longer sends them to a new draft. A person's Manual Control still holds it. */
    if (manualAlive(s.manualEdit, now)) return true;
    if (s.setId && !s.draft) return true;
    const id = idOf(s);
    return !!id && (sets || []).some(x => x && (x.sheetIds || []).includes(id));
  }
  /** reopensHome on a RECORD (placeOnSheet's check of `home`, inside its transaction): a GF / SS sheet that takes its own order's late pieces although it is closed. */
  function reopensHomeRec(s, sets, run, now) {
    now = now || Date.now();
    if (!s || !PAIR_OF[s.metal] || s.archived || s.roseCutAt || num(s.laserDoneAt) > 0 || s.processReady || manualAlive(s.manualEdit, now) || s.problem) return false;   // (HOLDTAKES; NOBUSY)
    if (run && run.runId && s.runId && s.runId !== run.runId) return false;
    return !fixedSetOf(s, sets);
  }
  /** The pool ids a record lists (its pieces, _charmNestPlacement.js: "on a sheet" is a saved, not archived record listing the id). */
  const listedOf = rec => new Set((Array.isArray(rec && rec.poolIds) ? rec.poolIds : []).map(String));
  /** A sheet RECORD as the rules read a page: its pieces (the charms its poolIds lists, and a charm with no pool id), its marks under the page's names (runHold from the run's holds,
      _manualHold for a person's Manual Control, _inplaceHold for a heavy task, rosePlan / roseProtected for a planned or protected green line) and its fill (liveInfo). */
  function recordView(rec, o = {}) {
    const now = o.now || Date.now(), run = o.run || null, listed = listedOf(rec);
    const v = Object.assign({}, rec);
    v.sheetId = rec.id; v.id = rec.id; v.recalled = null; v._rec = rec;
    v.charms = (Array.isArray(rec.charms) ? rec.charms : []).filter(c => c && (!c.poolId || listed.has(String(c.poolId))));
    v.placements = Array.isArray(rec.placements) ? rec.placements : [];
    v.rosePlan = !!(rec.rosePlanJson || rec.rosePlanHash); v.roseProtected = !!rec.roseProtectedJson;
    v.runHold = (run && run.sheetHolds && run.sheetHolds[rec.id]) || null;
    v._manualHold = manualAlive(rec.manualEdit, now); v._inplaceHold = jobRunning(rec.nestJob, now) || jobRunning(rec.filesJob, now);
    v.liveInfo = +rec.usablePt2 > 0 ? { usablePt2: +rec.usablePt2, freePt2: +rec.freePt2 || 0, placedPt2: Math.max(0, +rec.usablePt2 - (+rec.freePt2 || 0)) } : null;
    v.layoutRev = num(rec.layoutRev); v.contentRev = num(rec.contentRev);
    return v;
  }
  /** The run's sheets of each metal in line order, as a page restores them (restoreRunSheets): oldest first by when the sheet was started; the sheets of a metal a person ordered by hand
      (draftOrder) first, in that order. */
  function lineOrder(views) {
    const startedOf = x => num(x.cardStartedAt) || ms(x.createdAt) || 0, has = x => x.draftOrder != null && Number.isFinite(+x.draftOrder);
    const out = views.slice().sort((a, b) => startedOf(a) - startedOf(b));
    const seats = new Map(); out.forEach((x, i) => { if (!seats.has(x.metal)) seats.set(x.metal, []); seats.get(x.metal).push(i); });
    const res = out.slice();
    for (const at of seats.values()) { const sub = at.map(i => out[i]); if (!sub.some(has)) continue; sub.sort((a, b) => (has(a) ? 0 : 1) - (has(b) ? 0 : 1) || (has(a) ? +a.draftOrder - +b.draftOrder : 0)); at.forEach((i, k) => { res[i] = sub[k]; }); }
    return res;
  }
  /* The plates (charm-nest-1.html DEFAULTS.stock and STOCK_PRESETS, in inches) and the few settings the sheet choice reads; the shared settings (Charm_Nest_Shared/sorter-settings,
     sorter-stock) are laid over them by the caller, as every page lays them over its own. */
  const STOCK_PRESETS = { A: [100 / 25.4, 50 / 25.4], B: [100 / 25.4, 100 / 25.4] };
  const RECORD_DEFAULTS = { maxFill: 0.80, insetPt: 1.5, clearancePt: -0.5, stock: { gold: [100 / 25.4, 50 / 25.4], silver: [100 / 25.4, 50 / 25.4], rose: [100 / 25.4, 50 / 25.4], gold10k: [100 / 25.4, 50 / 25.4], gold14k: [100 / 25.4, 50 / 25.4] } };
  /** stockFor on a record: the size it was nested at (its stock, or a kept or new size), else the shared preset (Gold Filled and Sterling Silver), else the metal's own size. */
  function stockRec(metal, sh, settings, preset) {
    const saved = sh && (sh.newStock || sh.keptStock || sh.stock);
    if (saved && typeof saved === "object") { const w = +saved.wPt || +saved.wIn * 72, h = +saved.hPt || +saved.hIn * 72; if (w > 0 && h > 0) return { wPt: w, hPt: h }; }
    const p = STOCK_PRESETS[preset || "A"], own = (settings.stock || {})[metal] || RECORD_DEFAULTS.stock[metal] || RECORD_DEFAULTS.stock.gold;
    const inches = p && !CUT_METALS.includes(metal) ? p : own;
    return { wPt: inches[0] * 72, hPt: inches[1] * 72 };
  }
  /** The fill model of the cloud (the page's inflatedArea, usableArea, activeCharms, orderKey, gapFilling, groupOf) over records: env for topupRoom / topupTakes / nospreadChunk. */
  function recordEnv(settings, preset) {
    const s = Object.assign({}, RECORD_DEFAULTS, settings || {});
    return {
      settings: () => s, NS: () => (s.nospread !== "off" ? NoSpread() : null), cfg: () => nospreadCfg(s),
      inflatedArea: c => { const g = (+s.clearancePt || 0) / 2; return (+(c && c.areaPt2) || 0) + g * 2 * ((+(c && c.widthPt) || 0) + (+(c && c.heightPt) || 0)) + Math.PI * g * g; },   // (silhouette grown by half the clearance)
      usableArea: sh => {
        if (sh && sh.sheetId && sh.liveInfo && +sh.liveInfo.usablePt2 > 0) return +sh.liveInfo.usablePt2;   // (the record's own, from its last nest: a remnant's shape included)
        const st = stockRec(sh.metal, sh, s, preset), inset = +s.insetPt || 0, clearance = +s.clearancePt || 0, ins = inset + Math.max(0, clearance) / 2;
        return Math.max(0, st.wPt - 2 * ins) * Math.max(0, st.hPt - 2 * ins);
      },
      activeCharms: sh => (sh.charms || []).filter(c => !c.excluded),
      orderKey: c => NestPlan().orderKey(c), gapFilling: sh => NestPlan().gapFilling(sh),
      groupOf: c => { const P = PairLib(); if (!P || typeof P.inGroup !== "function" || !P.inGroup(c)) return null; const k = P.groupKey(c); return k && k.length > 1 && !/^:/.test(k) ? k : null; }
    };
  }
  /** What the intake knows of an order's pieces of one metal (the page's pieceTotals / orderInfo), from the run's lines and the sheets: { expected, placed } Maps by "order|metal". */
  function pieceTotalsRec(lines, views) {
    const expected = new Map(), placed = new Map(), add = (m, k, n) => m.set(k, (m.get(k) || 0) + n);
    for (const l of Object.values(lines || {})) { if (!l || ["gone", "skipped", "noDesign", "committed"].includes(l.state) || l.noDesign) continue; add(expected, String(l.orderId) + "|" + (l.material || ""), Math.max(0, Math.floor(+l.pieceCount || +l.quantity || 1))); }
    for (const p of views || []) for (const c of p.charms || []) if (!c.excluded) add(placed, orderOf(c) + "|" + p.metal, 1);
    return { expected, placed };
  }
  /** { pieces (in hand: this line's and the order's other lines' unplaced pieces of this metal), expected, placed }. batch: Map "order|metal" → pieces in hand. */
  function orderInfoRec(rid, metal, list, batch, totals) {
    const key = String(rid), mine = (batch && batch.get(key + "|" + metal)) || list;
    const placed = totals.placed.get(key + "|" + metal) || 0, expected = (totals.expected.get(key + "|" + metal) || 0) + (totals.expected.get(key + "|") || 0);
    return { pieces: mine.length ? mine : list, expected: Math.max(expected, mine.length + placed), placed };
  }
  /** The charm a piece's pool row puts on a sheet record (finish-core's record() charm, as a page would write it: the same id, name, source and pair fields), with what is known of
      its size (`geo`: { widthPt, heightPt, areaPt2 } from the row or the library's index entry). The nest makes its outline from the design (the pool row names it). */
  function charmOfRow(row, geo, at) {
    const src = "pool:" + String(row.aiPath || row.sku || "").replace(/[^\w]+/g, "_"), q = Math.max(1, Math.floor(+row.quantity || 1)), g = geo || {};
    const c = { id: `${src}:${row.poolId}`, hash: row.charmHash || null, name: `${row.orderId} · ${row.sku || ""}${q > 1 ? ` · ${+row.copy || 1}/${q}` : ""}`, sourceId: src, sourceName: `${row.sku || ""}${row.size ? " · " + row.size : ""} (master)`, index: 0,
      order: String(row.orderId), layer: null, poolId: row.poolId, custom: !!row.custom, sku: row.sku || null, metal: row.material || null, lineKey: row.lineKey || null,
      areaPt2: Math.round((+g.areaPt2 || 0) * 10) / 10, widthPt: Math.round((+g.widthPt || 0) * 100) / 100, heightPt: Math.round((+g.heightPt || 0) * 100) / 100,
      aiPath: row.aiPath || null, orderDate: +row.orderDate || 0, arrivedAt: +row.arrivedAt || 0, excluded: false, open: false, placedBy: "Cloud", placedAt: at || Date.now() };
    if (+row.frontAt > 0) c.frontAt = +row.frontAt;
    for (const k of ["side", "mirror", "bodyIndex", "groupKey", "groupSize", "oneBody"]) if (row[k] !== undefined && row[k] !== null) c[k] = row[k];
    return c;
  }
  /** The leftover the newest green line of a metal made, still free (no uncut sheet of the run holds it): { stockId, revision, partialId, from } | null. A hint for the nest's claim
      (the stock's own record decides: roseClaim). */
  function greenLeftoverRec(views, metal, except) {
    const held = new Set(views.filter(p => p !== except && !p.roseCutAt).map(p => p.roseStockId || (p.greenLeftover && p.greenLeftover.stockId)).filter(Boolean));
    const last = new Map(); for (const p of views) if (p.metal === metal && p.roseCutAt && p.roseStockId) { const q = last.get(p.roseStockId), rv = x => num(x.roseCutRevision) || num(x.roseRevision); if (!q || rv(p) > rv(q)) last.set(p.roseStockId, p); }
    const rv = x => num(x.roseCutRevision) || num(x.roseRevision);
    const cut = [...last.values()].filter(p => rv(p) >= 1 && !held.has(p.roseStockId)).sort((a, b) => num(b.roseCutAt) - num(a.roseCutAt))[0];
    return cut ? { stockId: cut.roseStockId, revision: rv(cut), partialId: `${cut.roseStockId}-${rv(cut)}`, from: cut.sheetId || null } : null;
  }
  /** The host of the rules over a run's sheet RECORDS. o: { run (with lines and sheetHolds), records (the run's sheet records), sets (the run's set records), settings, preset (the shared
      plate), partial ({ metal: "new" | "auto" }: Options → Partial Sheet), now }. → { host, rules, env, views(), pagesOf, fillingPage, addPage, viewOf(rec), shut(view) }. NEW DRAFTS are views
      with _new: true (no sheetId), written by placeOnSheet only when a piece goes on one. */
  function recordHost(o = {}) {
    const now = o.now || Date.now(), run = o.run || {}, sets = o.sets || [], settings = Object.assign({}, RECORD_DEFAULTS, o.settings || {}), env = recordEnv(settings, o.preset), partial = o.partial || {};
    let views = lineOrder((o.records || []).filter(r => r && !r.archived && (!run.runId || r.runId === run.runId)).map(r => recordView(r, { run, now })));
    const fresh = [];
    const all = () => views.concat(fresh);
    /* NOROOMFULL (Paul, 21 Nov 2026: "why is there stuff being put on a full sheet"): a sheet whose last nest ended with no room is full for the automatic placement: no new order goes
       on it (its own order's later pieces still may: orderHome / reopensHome). A person's move is not refused: closedRec, which placeOnSheet and finishOverflow check, is unchanged. */
    // (FULLAFTER5, Paul 21 Nov 2026: "the same rule for all percentage sheets": no room counts as full only after 5 different orders in a row had no room on the sheet, at any fill)
    const noRoom = r => !!r && (r.placements || []).length > 0 && (Array.isArray(r.noRoomRun) ? r.noRoomRun.length : 0) >= 5;
    const closedV = v => !!v && (v._shut || (!v._new && (closedRec(v._rec, sets, run, now) || noRoom(v._rec))));
    function nextPage(m) { const mine = all().filter(v => v.metal === m); return Math.max(mine.length, ...mine.map(v => num(v.page))) + 1; }
    function newDraft(m) {
      const unused = fresh.find(v => v.metal === m && !v.charms.length && !v.greenLeftover); if (unused) return unused;
      const v = { _new: true, sheetId: null, id: null, metal: m, runId: run.runId || null, page: nextPage(m), charms: [], placements: [], poolIds: [], draft: true, setId: null, status: "idle", cardStartedAt: now, liveInfo: null, layoutRev: 0, contentRev: 0 };
      fresh.push(v); return v;
    }
    const pagesOf = m => { if (!all().some(v => v.metal === m)) newDraft(m); return all().filter(v => v.metal === m); };
    /* ONEFILL (Paul, 12 Nov 2026: "one sheet at a time for everything"): a Gold Filled or Sterling Silver sheet of the run still filling, or null */
    const stillFilling = p => !!p && (p.charms || []).length > 0 && !(p.setId && !p.draft) && !p.releaseFull && !p.topup && !p.roseCutAt && !(+p.laserDoneAt > 0) && !p.recalled && !p.problem && !p.cardinalPull && !p.intakeFinalized && !p._ownLine && !(p.label && p.label.own);
    function fillingPage(metal, x = {}) {
      if (!FAST.includes(metal) || settings.onesheet === "off") return null;
      const rid = run.runId || null, held = p => !!p._manualHold || closedV(p);   // (NOBUSY: a draft being nested is still the draft filling)
      return pagesOf(metal).find(p => p !== x.except && (!rid || !p.runId || p.runId === rid) && stillFilling(p) && !held(p)) || null;
    }
    /* PAIRFIX: the sheet still filling only when it keeps the rule the caller needs (x.ok); else x.wait → null (the caller's pieces wait for it), and without x.wait a new draft */
    /* ONEDRAFTFILL (Paul, 21 Nov 2026: "these draft sheets only have one or two pieces and they're going onto a next draft sheet … this breaks my rules"): every draft still filling is
       tried, oldest first, before a new draft is opened. Only the first was offered: when it could not take the pieces (full in fact, never marked full while the cloud's nests failed), a
       new draft was made, and the second draft, with its room, was passed over. */
    function fillingPages(metal, x = {}) {
      if (!FAST.includes(metal) || settings.onesheet === "off") return [];
      const rid = run.runId || null, held = p => !!p._manualHold || closedV(p);   // (NOBUSY: a draft being nested is still the draft filling)
      return pagesOf(metal).filter(p => p !== x.except && (!rid || !p.runId || p.runId === rid) && stillFilling(p) && !held(p)).sort((a, b) => num(a.page) - num(b.page));
    }
    function addPage(metal, x = {}) {
      if (!x.fresh) {
        const fs = fillingPages(metal, x);
        for (const f of fs) if (!x.ok || x.ok(f)) return f;
        if (fs.length && x.wait) return null;
      }
      return newDraft(metal);
    }
    /* RGC1: the next sheet of a green-line metal continues on the leftover its newest cut made (the nest claims it), unless Options → Partial Sheet says always a new sheet */
    function greenPage(metal) {
      const pg = newDraft(metal);
      if (partial[metal] === "new") return pg;
      const left = greenLeftoverRec(all(), metal, pg); if (left) pg.greenLeftover = left;
      return pg;
    }
    const host = {
      sheets: () => all(), pagesOf, closed: closedV, holding: () => false, fixedSet: v => !v._new && fixedSetOf(v, sets),
      rows: () => Object.entries(run.lines || {}).filter(([, l]) => l && typeof l === "object").map(([key, l]) => ({ key, order: { receiptId: l.orderId }, spec: { material: l.material }, state: l.state, poolIds: l.poolIds || [] })),
      run: () => run, settings: () => settings, r16On: () => settings.nospread !== "off", fast: m => FAST.includes(m), cuts: m => CUT_METALS.includes(m),
      addPage, fillingPage: m => fillingPage(m), greenPage: (m) => greenPage(m), reopenHome: (v, rid) => { v._home = String(rid).split("/")[0]; return true; },
      topupRoom: v => topupRoom(v, true, env), topupTakes: (v, pieces, info) => topupTakes(v, pieces, info, env)
    };
    const rules = create(host);
    return {
      host, rules, env, settings, closed: closedV, pagesOf, fillingPage, addPage, newDraft, views: all,
      /** A record read again (after a refusal or a write): its view takes the place of the old one. */
      refresh(rec) { const v = recordView(rec, { run, now: Date.now() }), i = views.findIndex(x => x.sheetId === rec.id); if (i >= 0) views[i] = v; else views.push(v); return v; },
      /** A new draft now written (placeOnSheet created it): it becomes a sheet like the others. */
      made(v, rec) { const i = fresh.indexOf(v); if (i >= 0) fresh.splice(i, 1); const w = recordView(rec, { run, now: Date.now() }); views.push(w); return w; }
    };
  }
  /** SPLITWAIT over records: the run's lines and the sheet views (splitWaiting's rows from the lines). */
  const splitWaitingRec = (lines, views) => splitWaiting(Object.values(lines || {}).filter(l => l && typeof l === "object").map(l => ({ order: { receiptId: l.orderId }, spec: { material: l.material }, state: l.state, poolIds: l.poolIds || [] })), views);

  return { create, PAIR_OF, FAST, CUT_METALS, METALS, TOPUP, GAP_PACK, MANUAL_STALE_MS, JOB_STALE_MS,
    topupRoom, topupTakes, nospreadSheet, nospreadOrder, nospreadChunk, nospreadCfg, splitWaiting, splitHolds, splitWaitingRec, passedOver,
    closedRec, reopensHomeRec, recordView, lineOrder, recordEnv, recordHost, stockRec, pieceTotalsRec, orderInfoRec, charmOfRow, greenLeftoverRec, listedOf, manualAlive, jobRunning };
});
