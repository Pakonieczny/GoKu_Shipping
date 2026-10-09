/* Release hold: an order goes back in the queue AHEAD of the orders coming in from Etsy and is placed at once on the next
 * available placement (Paul, 5 Oct 2026: "When an order gets released from hold it must go back in queue and be placed on the
 * next available placement on the available sheet ahead of the incoming orders from Etsy").
 *
 * ADDS to window.OrderHold (the object is made here when the hold engine's file has not loaded yet: load order does not matter):
 *
 *   OrderHold.releasePlan(rid)                  read only, nothing is written or made. A Promise that also carries its fields
 *       (so it is read either way, `await`ed or not):
 *       { rid, label, held, canRelease, blockedWhy|null, front:true, running, resumed, needsNewSheet,
 *         pieces:[{ lineKey, label, metal, qty }],
 *         targets:[{ metal, metalLabel, sheetId|null, label, page, newSheet, partial, density, inSet, pieces }],  target = targets[0],
 *         stays:[{ label, sheetLabel }]  (pieces of the order that stay on a sheet already cut),
 *         effects:[plain sentences, ready to show] }
 *   OrderHold.release(rid, { name, onStep })    -> Promise<{ ok, released, rid, placed, sheets:[{sheetId,label,metal,page,newSheet}], steps, error? }>
 *   OrderHold.releaseStatus(rid)                -> { running:boolean, step }   (for resume after a reload)
 *   OrderHold.resumeReleases()                  -> Promise: finishes any release a reload cut short (the page does this by itself)
 *
 * Steps (every step has `at`, ms; onStep(step) is called as each real step completes):
 *   { type:'start', rid, resumed? }
 *   { type:'queued', rid, front:true, frontAt }                 the order carries its place at the front of the queue (saved)
 *   { type:'target', sheetId|null, label, spot:null, newSheet, metal, page, density }      one per metal: where it will go
 *   { type:'flight', toSheetId|null, poolIds, metal, page, label }                         its pieces leave On hold for that sheet
 *   { type:'placed', sheetId|null, poolIds, label, metal, page, placements:[{ poolId, cxPt, cyPt, angle, wPt, hPt }] }
 *   { type:'qr', sheetId|null, label, made, orders, why? }      the QR label of the sheet that changed: made again, naming the order, where the
 *                                                               sheet is in its set (a sheet leaves its set while its nest is written and rejoins when it
 *                                                               is saved; the step waits for that); a sheet still filling (a partial GF/SS) has none yet: made:false, why
 *   { type:'released', rid, sheets:[...], placed }              the hold is lifted, permanent on the order's timeline
 *   { type:'done', placed, why? }  |  { type:'error', message, rid, stage }
 *
 * How the order goes first. Its lines, pieces and pool rows carry `frontAt` (the release time, ms). Every placement path sorts
 * what carries it first, the order released first first (CharmNestOrders.byQueue / rankDate): the run's pull order
 * (Pool.addAll), LiveNest's intake, the nest's oldest-first rank (feedTurn, feedOn, topping up, keepOrdersWhole and the job
 * the solver reads, which the server's solver reads too), the Merge order, the sheet window's waiting candidates.
 * How it is placed at once. The hold is lifted through the existing path (Review.repool: the line's hold, the Review card,
 * the timeline's "restored"), which makes the pieces up and puts them on the sheet the intake would use (LiveNest.intakePage:
 * the earliest open sheet of the metal with room, a partial one first; a new sheet when none; never one that is cut, recalled,
 * released full, held, or in a committed set). A sheet already in its set keeps its set from the moment the order goes onto it
 * (Gate.keep, set in attachPool when the line carries `releasing`). The sheet is then nested THAT MOMENT, one piece at a time,
 * nothing already on it moved (the nest's own append rules: tight packing, density 75% toward 80%, careful grading; Rose Gold a
 * block packed from the left with no green line), the engine waits for the pieces to be placed and saved (the Library's own copy
 * of the sheet lists them), and the sheet's QR label is made again.
 * Restart-safe: the order's lines carry `releasing` (saved with the workspace and the run record) until every piece is placed;
 * a page that loads with it finishes the release once (the pieces already on a sheet are never put on again): a check every 5 s
 * (OrderHold._scan) takes up what a reload cut short, at most 3 times per order per load. Nothing is deleted anywhere.
 */
(function () {
  'use strict';
  const G = typeof window !== 'undefined' ? window : globalThis;
  const OH = G.OrderHold = G.OrderHold || {};
  const RUNNING = new Map(), LAST = new Map(), TRIED = new Map();
  const PLACE_MS = 6 * 60000, SAVE_MS = 90000, QR_MS = 60000;
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const str = v => (v == null ? '' : String(v));

  /* ── readers: everything is looked for when it is needed, so the file works whichever script loaded first ── */
  const CNx = () => G.CN || {};
  const allSheets = () => { try { return CNx().allSheets ? CNx().allSheets() : []; } catch (_) { return []; } };
  const pagesOf = m => { try { return CNx().pagesOf ? CNx().pagesOf(m) : allSheets().filter(p => p.metal === m); } catch (_) { return []; } };
  const rowsOfOrder = rid => (G.Orders && G.Orders.rows ? G.Orders.rows() : []).filter(r => r && r.order && str(r.order.receiptId) === str(rid) && r.state !== 'gone');
  const metalOf = r => { try { return (G.CustomSheet && G.CustomSheet.metalOf && G.CustomSheet.metalOf(r)) || (r.spec && r.spec.material) || r.material || null; } catch (_) { return (r.spec && r.spec.material) || r.material || null; } };
  const theRun = () => (G.B && G.B.run) || null;
  const stepAt = s => (G.CharmNestOrders && G.CharmNestOrders.stepIndex ? G.CharmNestOrders.stepIndex(s) : -1);
  const runOpen = () => { const r = theRun(); return !!r && !['complete', 'abandoned'].includes(r.status) && stepAt(r.step) >= stepAt('pool'); };
  const nameNow = () => { try { const n = G.CNEmployee && G.CNEmployee.name ? G.CNEmployee.name() : (G.B && G.B.employee) || ''; return str(n).trim(); } catch (_) { return ''; } };
  const metalWord = m => { try { return CNx().labelOf ? CNx().labelOf(m) : str(m); } catch (_) { return str(m); } };
  const ridOfCharm = c => str(c && c.order).split('/')[0];
  const pct = d => Math.round(Math.max(0, Math.min(1, +d || 0)) * 100);
  const word = (n, one, many) => `${n} ${n === 1 ? one : many}`;

  /* ── the sheets ── */
  const committed = sh => { try { return (G.Sets && G.Sets.ofRun ? G.Sets.ofRun(sh.runId) : []).some(s => s.committedAt && (s.sheetIds || []).includes(sh.sheetId)); } catch (_) { return false; } };
  /** Cut, recalled, or in a committed set: never filled, never taken from. */
  const cutSheet = sh => !!(sh.roseCutAt || sh.laserDoneAt || sh.recalled || committed(sh));
  const closed = sh => cutSheet(sh) || !!(G.LiveNest && G.LiveNest.closed && G.LiveNest.closed(sh));
  const busy = sh => ['nesting', 'finishing', 'queued'].includes(sh.status) || !!sh._operationStarting || !!sh._learnedStarting || !!(sh.persisted && !sh.persistedDone && !sh.problem);
  const label = sh => { try { if (G.SheetEvents && G.SheetEvents.label) return G.SheetEvents.label(sh); } catch (_) { /* below */ } return `${(CNx().METAL_TAG || {})[sh.metal] || sh.metal} Sheet ${sh.sheetIndex || sh.page || 1}`; };
  const inSet = sh => !!sh.setId && !sh.draft;
  const newLabel = metal => { const pages = pagesOf(metal); return `${(CNx().METAL_TAG || {})[metal] || metal} Sheet ${Math.max(pages.length, ...pages.map(p => +p.page || 0)) + 1}`; };

  /** The sheet the order goes on: the one LiveNest.intakePage and attachPool would choose, without making one (null: a new sheet). */
  function pick(metal, rid) {
    const run = theRun(), pages = pagesOf(metal), newest = pages[pages.length - 1] || null;
    const FAST = (G.CharmNestOrders && G.CharmNestOrders.FAST_MATERIALS) || new Set(['silver', 'gold']);
    let page = newest;
    if (newest && FAST.has(metal)) {
      const open = p => p.charms.length && run && p.runId === run.runId && !closed(p);
      const room = p => !CNx().topupRoom || CNx().topupRoom(p) > 0;
      // an order goes onto one sheet whole: its other pieces already waiting on a sheet in line come first
      const home = pages.find(p => open(p) && p.charms.some(c => ridOfCharm(c) === str(rid)));
      page = home || pages.find(p => p !== newest && open(p) && room(p)) || newest;
      if (page === newest && open(newest) && !room(newest)) page = null;
    }
    if (page && ((run && page.runId && page.runId !== run.runId) || closed(page))) page = null;
    return page;
  }
  function describe(metal, page, n) {
    const fresh = !page || (!page.charms.length && !(page.placements || []).length);
    return { metal, metalLabel: metalWord(metal), sheetId: page ? page.sheetId || null : null, label: page ? label(page) : newLabel(metal), page: page ? page.page : null,
      newSheet: fresh, partial: !fresh, density: fresh ? 0 : +(page.density || 0), inSet: !!page && inSet(page), pieces: n };
  }
  /** Where each piece of the order is now: poolId → { sh, c, placed }. */
  function locate(ids) {
    const want = ids instanceof Set ? ids : new Set(ids), out = new Map();
    for (const sh of allSheets()) {
      let placed = null;
      for (const c of sh.charms || []) {
        if (!c.poolId || !want.has(c.poolId) || out.has(c.poolId)) continue;
        if (!placed) placed = new Set((sh.placements || []).map(p => p.id));
        out.set(c.poolId, { sh, c, placed: placed.has(c.id) });
      }
    }
    return out;
  }
  const piecesOnSheets = rid => { const out = []; for (const sh of allSheets()) for (const c of sh.charms || []) if (c.poolId && ridOfCharm(c) === str(rid)) out.push({ sh, c }); return out; };

  /** How many pieces a held line comes back as: the pieces the cloud still lists for it (a pair of earrings is two, a mismatched pair a left and a right, n discs n),
   *  else the line's quantity as before. */
  function pieceCount(r, rid) {
    const q = Math.max(1, +(r.spec && r.spec.quantity) || +(r.line && r.line.quantity) || 1);
    try { const OP = G.OrderPieces; if (OP && OP.of) { const n = OP.of(rid).filter(p => p && p.lineKey === r.key && p.poolId).length; if (n > 0) return n; } } catch (_) { /* the quantity below */ }
    return q;
  }
  /** Sentences for a pair (or n discs) whose pieces went on more than one sheet: "Its pair is on two sheets: the left earring on GF Sheet 1 and the right earring on GF Sheet 2."; [] when every group is whole on one sheet. */
  function landedWords(ids) {
    try {
      const P = G.PairRemove; if (!P) return [];
      const items = [...locate(new Set(ids))].map(([id, x]) => { const pr = (G.B && G.B.pool && G.B.pool.rows.get(id)) || { poolId: id }; return { id, groupKey: P.keyOf(pr) || P.keyOf(x.c) || P.keyOf(id), side: P.sideOfPiece(pr, P.isMismatchedSku(pr.sku)), form: str(pr.form), where: label(x.sh) }; });
      return P.groupsOf(items).filter(g => g.sheets.length > 1).map(g => ((g.kind === 'pair' || g.kind === 'mismatched') && g.size === 2 ? `Its pair is on two sheets: ${g.phrase}.` : g.text));
    } catch (_) { return []; }
  }
  /* ── pairs (Paul, 9 Oct 2026, amendment 2): every earring pair line makes TWO pieces for each unit, one Left and one Right (matching or
     mismatched, mirror images), and a necklace with a count (discs, letters, charms) makes that many pieces of one group. A line is one
     GROUP (charm-nest-pair.js: receipt + transaction) and goes back on a sheet whole; one whose other pieces stay on a sheet already cut
     is split (rule R3), and the preview and the order's timeline say so in plain words. The intake's own reading of the line (spec.pair,
     spec.pieceCount, CharmNestOrders.pieceCountOf) is the one source; a line it does not call a pair reads and writes exactly what it did. ── */
  const PAIR = () => G.CharmNestPair || null;
  /** What the intake says a line is: { pair: an earring pair line, mismatched, pieces }. */
  function lineKind(r) {
    const sp = (r && r.spec) || {}, qty = Math.max(1, +sp.quantity || +(r && r.line && r.line.quantity) || 1), pr = sp.pair && sp.pair.kind ? sp.pair : null;
    let n = 0; try { const O = G.CharmNestOrders; n = O && O.pieceCountOf ? +O.pieceCountOf(r) || 0 : 0; } catch (_) { n = 0; }
    n = n || +sp.pieceCount || qty;
    return { pair: !!pr && (pr.kind === 'pair' || pr.kind === 'mismatched'), mismatched: !!pr && (pr.kind === 'mismatched' || !!pr.mismatched), pieces: n };
  }
  /** The lines of the order whose pieces are partly on a sheet already cut and partly to be placed now: [{ lineKey, groupKey, label, stays:[sheet label], sides:[Left|Right], fresh, stayed }]. */
  function splitLines(rid, rows, stayIds, freshIds) {
    const fresh = new Set(freshIds), on = piecesOnSheets(rid), out = [];
    for (const r of rows) {
      const ids = (r.poolIds || []).map(String); if (ids.length < 2) continue;
      const stayHere = ids.filter(id => stayIds.has(id)), go = ids.filter(id => fresh.has(id));
      if (!stayHere.length || !go.length) continue;
      const here = on.filter(x => stayHere.includes(String(x.c.poolId))), where = [...new Set(here.map(x => label(x.sh)))];
      const sides = here.map(x => (x.c.side === 'L' ? 'Left' : x.c.side === 'R' ? 'Right' : '')).filter(Boolean);
      const P = PAIR(); let key = ''; try { key = P && P.groupKey ? P.groupKey(r) : ''; } catch (_) { key = ''; }
      out.push({ lineKey: r.key, groupKey: key || `${rid}:${str(r.line && r.line.transactionId)}`, label: str((r.spec && r.spec.designSku) || (r.line && r.line.sku) || 'piece'), stays: where, sides: sides.length === stayHere.length ? sides : [], fresh: go.length, stayed: stayHere.length });
    }
    return out;
  }
  const splitWords = x => `${x.sides.length ? `its ${x.sides.join(' and ')} ${x.sides.length === 1 ? 'piece' : 'pieces'}` : word(x.stayed, 'piece', 'pieces')} of ${x.label} stay${x.stayed === 1 ? 's' : ''} on ${x.stays.join(' and ') || 'a sheet already cut'}, so ${x.stayed + x.fresh === 2 ? 'this pair is' : 'these pieces are'} on two sheets (both sheets must go in one set)`;

  /* ── the plan: read only ── */
  function planOf(rid) {
    rid = str(rid);
    const rows = rowsOfOrder(rid), held = rows.filter(r => r.hold), resumed = rows.some(r => r.releasing);
    const base = { rid, label: `Order ${rid}`, held: held.length > 0, canRelease: false, blockedWhy: null, front: true, running: RUNNING.has(rid), resumed, needsNewSheet: false, pieces: [], targets: [], target: null, stays: [], effects: [] };
    if (!rows.length) { base.blockedWhy = `Order ${rid} is not in this sorter.`; return base; }
    if (G.Cancelled && G.Cancelled.has && G.Cancelled.has(rid)) { base.blockedWhy = `Order ${rid} was cancelled.`; return base; }
    if (!held.length && !resumed) { base.blockedWhy = 'This order is not on hold.'; return base; }
    base.canRelease = true;
    const mine = rows.filter(r => r.hold || r.releasing), byMetal = new Map();
    const pairs = [];
    for (const r of mine) {
      const m = metalOf(r), qty = Math.max(1, +(r.spec && r.spec.quantity) || +(r.line && r.line.quantity) || 1);
      const lk = lineKind(r), pc = { lineKey: r.key, label: str((r.spec && r.spec.designSku) || (r.line && r.line.sku) || 'piece'), metal: m, qty };
      // (an earring pair line makes a Left and a Right for each unit, so it is counted by its pieces; every other line keeps the count it had)
      if (lk.pair) { pc.kind = lk.mismatched ? 'mismatched' : 'pair'; pc.pieces = lk.pieces; pc.sides = ['Left', 'Right']; pairs.push(pc); }
      base.pieces.push(pc);
      if (m) byMetal.set(m, (byMetal.get(m) || 0) + (lk.pair ? lk.pieces : pieceCount(r, rid)));   // (any other line: the pieces the cloud lists for it, n discs, else its quantity)
    }
    const open = runOpen(), fx = base.effects;
    fx.push(`Order ${rid} goes to the front of the queue, ahead of the orders coming in from Etsy.`);
    if (!open) fx.push('No run is open, so the order waits at the front and is placed first when the next run starts.');
    else for (const [m, n] of byMetal) {
      const page = pick(m, rid), t = describe(m, page, n);
      base.targets.push(t);
      if (t.newSheet) base.needsNewSheet = true;
      if (m === 'rose') fx.push(t.newSheet ? `${word(n, 'piece', 'pieces')} start${n === 1 ? 's' : ''} ${t.label}, packed from the left. No green line is drawn.` : `${word(n, 'piece', 'pieces')} go${n === 1 ? 'es' : ''} on ${t.label}${t.density ? ` (${pct(t.density)}% full)` : ''} next to the pieces already there, packed from the left. No green line is drawn and nothing is moved.`);
      else if (t.newSheet) fx.push(`No ${t.metalLabel} sheet has room, so ${word(n, 'piece', 'pieces')} start${n === 1 ? 's' : ''} a new sheet, ${t.label}.`);
      else fx.push(`${word(n, 'piece', 'pieces')} go${n === 1 ? 'es' : ''} on ${t.label}${t.density ? ` (${pct(t.density)}% full)` : ''}, the next ${t.metalLabel} sheet with room, one piece at a time. Nothing already on it moves.`);
      fx.push(t.inSet ? `The QR label of ${t.label} is made again.` : `${t.label} is still filling, so its QR label is made when it is released to its set.`);
    }
    base.target = base.targets[0] || null;
    for (const pc of pairs) fx.push(`${pc.label}: ${pc.pieces > 2 && pc.pieces % 2 === 0 ? `its ${pc.pieces / 2} pairs (each a Left and a Right) go` : 'the pair (Left and Right) goes'} on the same sheet, together.`);
    // pieces of the order that stay where they are: on a sheet already cut
    const had = new Map(), stayIds = new Set();
    for (const { sh, c } of piecesOnSheets(rid)) if (cutSheet(sh)) { had.set(label(sh), (had.get(label(sh)) || 0) + 1); stayIds.add(String(c.poolId)); }
    for (const [sheetLabel, n] of had) { base.stays.push({ label: word(n, 'piece', 'pieces'), sheetLabel }); fx.push(`${word(n, 'piece', 'pieces')} stay${n === 1 ? 's' : ''} on ${sheetLabel}, already cut.`); }
    // a pair or line with pieces on a sheet already cut and pieces released now is split (R3): said plainly, and kept on the plan
    const cutIds = new Set([...stayIds]), freshIds = mine.flatMap(r => (r.poolIds || []).map(String)).filter(id => !cutIds.has(id));
    const split = splitLines(rid, mine, cutIds, freshIds);
    if (split.length) { base.split = split; for (const x of split) fx.push(`${splitWords(x)}.`); }
    if (mine.some(r => (r.problems || []).length)) fx.push('A piece needs a look in Review before it can be placed; it stays at the front of the queue.');
    return base;
  }
  OH.releasePlan = function releasePlan(rid) {
    let plan; try { plan = planOf(rid); } catch (e) { plan = { rid: str(rid), label: `Order ${rid}`, held: false, canRelease: false, blockedWhy: str(e && e.message || e), front: true, targets: [], target: null, pieces: [], stays: [], effects: [], needsNewSheet: false }; }
    return Object.assign(Promise.resolve(plan), plan);
  };

  /* ── saving: the workspace and the run record (what makes the release restart-safe) ── */
  async function persist() {
    try { if (G.Session) { G.Session.schedule && G.Session.schedule(); if (G.Session.flushNow) await G.Session.flushNow(); } } catch (_) { /* the page's own checkpoint carries it */ }
    try { const r = theRun(); if (r && G.Orders && G.Orders.lineRecord && G.RunCtl && G.RunCtl.save) { r.lines = Object.fromEntries(G.Orders.rows().map(G.Orders.lineRecord)); await G.RunCtl.save(r); } } catch (_) { /* tried again at the next save */ }
  }
  const refresh = () => { try { G.Review && G.Review.syncOrderItems && G.Review.syncOrderItems(); G.Orders && G.Orders.render && G.Orders.render(); G.RunCtl && G.RunCtl.poke && G.RunCtl.poke(); } catch (_) { /* a repaint never stops a release */ } };

  /* ── placing at once ── */
  const startNow = sh => {
    const CN = CNx();
    // a stopped run starts none of its sheets by itself; a person's release is the person's own press
    try { if (CN.heldForResume && CN.heldForResume(sh)) sh._byHand = true; } catch (_) { /* below */ }
    if (!['nesting', 'finishing'].includes(sh.status)) sh.status = 'ready';
    const go = typeof G.startNest === 'function' ? G.startNest : CN.startNest;   // (the page's own nest; the global is what the page's code calls too)
    if (go) go(sh);
  };
  /** Waits until every fresh piece is placed on a sheet (its sheet nests now; a piece that did not fit goes whole to the next
      sheet, which nests too). Announces each sheet once its pieces are placed. Throws when a sheet reports a problem or the
      time runs out: the order stays at the front of the queue and the next load finishes it. */
  async function placeNow(fresh, emit, ms) {
    const want = new Set(fresh), told = new Set(), starts = new Map(), t0 = Date.now(), lostSince = { t: 0 };
    for (;;) {
      const loc = locate(want), lost = [...want].filter(id => !loc.has(id));
      if (lost.length) { if (!lostSince.t) lostSince.t = Date.now(); if (Date.now() - lostSince.t > 20000) throw new Error(`${word(lost.length, 'piece is', 'pieces are')} not on a sheet yet`); } else lostSince.t = 0;
      const by = new Map();
      for (const x of loc.values()) { if (!by.has(x.sh)) by.set(x.sh, []); by.get(x.sh).push(x); }
      for (const [sh, xs] of by) {
        if (xs.every(x => x.placed) && !told.has(sh)) {
          told.add(sh);
          const at = new Map((sh.placements || []).map(p => [p.id, p]));
          emit({ type: 'placed', sheetId: sh.sheetId || null, poolIds: xs.map(x => x.c.poolId), label: label(sh), metal: sh.metal, page: sh.page,
            placements: xs.map(x => { const p = at.get(x.c.id) || {}; return { poolId: x.c.poolId, cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle || 0, wPt: p.wPt || x.c.widthPt || 0, hPt: p.hPt || x.c.heightPt || 0 }; }) });
        }
      }
      const pending = [...loc.values()].filter(x => !x.placed);
      if (!pending.length && !lost.length) return [...by.keys()];
      for (const sh of new Set(pending.map(x => x.sh))) {
        // (a problem from a nest before this release is old news: the search is started first, and clears it; only the one it reports counts)
        if (sh.problem && (starts.get(sh) || 0) > 0) throw new Error(`${label(sh)}: ${sh.problem}`);
        if (busy(sh) || sh.runHold) continue;
        const n = starts.get(sh) || 0;
        if (n < 6) { starts.set(sh, n + 1); startNow(sh); }
      }
      if (Date.now() - t0 > ms) throw new Error('the sheet is still being filled; the release carries on by itself');
      await sleep(300);
    }
  }
  const cloudOk = () => { const S = CNx().S; return !S || !S.cloud || !!S.cloud.ok; };
  /** The sheet's saved record (the Library's own copy) lists every one of these pieces as placed. Offline: nothing to wait for. */
  async function cloudSaved(sh, ids) {
    const CN = CNx(); if (!CN.api || !sh.sheetId || !cloudOk()) return true;
    try {
      const r = await CN.api('charmNestLibrary', { op: 'getSheet', id: sh.sheetId }, { quiet: true, timeoutMs: 12000 }), rec = r && r.sheet; if (!rec) return false;
      const placed = new Set((rec.placements || []).map(p => p.id)), on = new Set((rec.charms || []).filter(c => placed.has(c.id)).map(c => c.poolId));
      return ids.every(id => on.has(id));
    } catch (_) { return false; }
  }
  /** Waits until each sheet's saved record holds the order's pieces, and (for a sheet that was in its set) until its own nest has
      finished, so the label is made from the saved sheet. A sheet still filling goes on with the other orders after the release. */
  async function savedSheets(pages, ids, wasSet) {
    const loc = locate(new Set(ids));
    for (const sh of pages) {
      const mine = [...loc.values()].filter(x => x.sh === sh).map(x => x.c.poolId);
      for (let t = Date.now(); Date.now() - t < SAVE_MS && !(await cloudSaved(sh, mine)); await sleep(1000));
      if (wasSet(sh) || (sh.label && sh.label.own)) for (let t = Date.now(); Date.now() - t < SAVE_MS && busy(sh); await sleep(300));
    }
  }
  /** The QR label of a sheet that changed, made again where the sheet has one. A sheet that was in its set leaves it while its
      nest is written (a nest makes every sheet a draft) and rejoins when it is saved (Gate.assemble); it is given its turn,
      and asked once in a while, so the label is made from the sheet as it stands. */
  async function remakeQr(sh, rid, emit, wasSet) {
    const run = theRun(), orders = () => ((sh.label && sh.label.orders) || []).map(str), has = () => inSet(sh) || !!(sh.label && sh.label.own);
    const modern = () => !!(run && G.Gate && G.Gate.modern && G.Gate.modern(run.runId) && G.Gate.assemble);
    let made = false, why = '';
    if (wasSet && !has()) for (let t = Date.now(), nudged = 0; Date.now() - t < QR_MS && !(has() && !busy(sh)); await sleep(500)) {
      if (!busy(sh) && !has() && Date.now() - nudged > 5000 && modern()) { nudged = Date.now(); try { await G.Gate.assemble(run); } catch (_) { /* tried again */ } }
    }
    if (has()) {
      for (let t = Date.now(); Date.now() - t < 15000 && !orders().includes(rid) && busy(sh); await sleep(300));
      if (!orders().includes(rid) && modern()) { try { await G.Gate.assemble(run); } catch (e) { why = str(e && e.message); } }
      if (!orders().includes(rid) && sh.label && sh.label.own && sh.sheetId && G.SheetWin && G.SheetWin.remakeLabel) { try { await G.SheetWin.remakeLabel(sh.sheetId); } catch (e) { why = str(e && e.message); } }
      made = orders().includes(rid);
      if (!made && !why) why = 'the label is made when the sheet is saved again';
    } else why = wasSet ? 'the sheet rejoins its set when it is saved again; its label is made then' : 'still filling: its QR label is made when it is released to its set';
    emit(Object.assign({ type: 'qr', sheetId: sh.sheetId || null, label: label(sh), made, orders: orders().length }, why ? { why } : {}));
  }

  /* ── the release ── */
  /** A hold is a mark in the cloud (the pool rows a take-off left: abandoned, held, on no sheet) and every page reads it as "on hold" until the pieces are made up again (poolPut stamps repooledAt).
   *  With a run open that follows at once; with none (or a piece the run could not make up) it would be hours, and every other computer, the Library and the stations' timeline would say "held" for
   *  an order this page has just released. The release says so itself: the order's still-held pool rows get the same repooledAt stamp, written over the rows only (nothing is made up here, no sheet is
   *  touched). Never throws: the later poolPut stamps them anyway. */
  async function liftInCloud(rid) {
    try {
      const OP = G.OrderPieces, CN = G.CN; if (!OP || !OP.of || !OP.load || !CN || !CN.api) return;
      await OP.load([rid], { force: true });
      const ids = [...new Set(OP.of(rid).filter(p => p && p.hold && p.poolId).map(p => String(p.poolId)))];
      if (!ids.length) return;
      await CN.api('charmNestLibrary', { op: 'poolUpdate', poolIds: ids, patch: { repooledAt: Date.now() } }, { quiet: true, timeoutMs: 15000 });
      await OP.load([rid], { force: true });   // (this page's own view of the order is the cloud's again before the release is over: nothing re-reads the old marks into the rows)
      if (G.PlacementFeed && G.PlacementFeed.nudge) G.PlacementFeed.nudge();
    } catch (e) { try { console.warn('[OrderHold.release] the hold marks in the cloud were not cleared', e && e.message || e); } catch (_) { /* none */ } }
  }
  async function doRelease(rid, opts) {
    const steps = [], emit = s => { s.at = Date.now(); steps.push(s); LAST.set(rid, { step: s }); try { if (opts.onStep) opts.onStep(s); } catch (e) { try { console.warn('[OrderHold.release] onStep', e); } catch (_) { /* none */ } } return s; };
    const out = (ok, extra) => Object.assign({ ok, released: false, rid, placed: false, sheets: [], steps }, extra);
    const fail = (message, stage, extra) => { emit({ type: 'error', message, rid, stage }); return out(false, Object.assign({ error: message, stage }, extra)); };
    const rows0 = rowsOfOrder(rid), resumed = rows0.some(r => r.releasing);
    emit(Object.assign({ type: 'start', rid }, resumed ? { resumed: true } : {}));
    if (!G.Orders || !G.Review || !G.B) return fail('The sorter is not ready yet; try again in a moment.', 'start');
    // (a Hold still taking this order off its sheets: its first lines are under On hold already, and a release now would put them back while the rest comes off)
    if (OH.status && (OH.status(rid) || {}).running) return fail(`Order ${rid} is being put on hold right now, so it cannot be released yet. It is still on hold: press Release hold again when that has finished.`, 'start');
    if (!rows0.length) return fail(`Order ${rid} is not in this sorter.`, 'start');
    if (G.Cancelled && G.Cancelled.has && G.Cancelled.has(rid)) return fail(`Order ${rid} was cancelled.`, 'start');
    if (!rows0.some(r => r.hold || r.releasing)) return fail('This order is not on hold.', 'start');
    const who = str(opts.name || (rows0.find(r => r.releasing) || { releasing: {} }).releasing.by || nameNow()).trim();
    if (!who) return fail('A name is needed for the record.', 'start');

    /* 1 · the place at the front of the queue, saved before anything else changes (a reload from here on finishes it) */
    const frontAt = resumed ? +rows0.find(r => r.releasing).releasing.at || Date.now() : Date.now();
    for (const r of rows0) if (r.hold || r.releasing) { r.frontAt = frontAt; r.releasing = { at: frontAt, by: who, stage: (r.releasing && r.releasing.stage) || 'queued' }; }
    await persist();
    emit({ type: 'queued', rid, front: true, frontAt });
    const open = runOpen(), mine = rowsOfOrder(rid).filter(r => r.releasing);

    /* 2 · where it goes (read from the sheets as they are now) */
    const byMetal = new Map(); for (const r of mine) { const m = metalOf(r); if (m) byMetal.set(m, (byMetal.get(m) || 0) + 1); }
    const targets = [], kept = new Set();
    const keep = sh => { if (sh && inSet(sh) && !kept.has(sh) && G.Gate && G.Gate.keep) { try { G.Gate.keep(sh); if (sh.keepRelease) sh.keepRelease.rid = rid; kept.add(sh); } catch (_) { /* below */ } } };
    // (the sheet is let go as soon as its own nest is through; a keep that is another change's is left alone)
    const unkeep = sh => { const t0 = Date.now(), tick = () => { if (!sh.keepRelease || sh.keepRelease.rid !== rid) return; if (!busy(sh) || Date.now() - t0 > 600000) { delete sh.keepRelease; return; } setTimeout(tick, 2000); }; tick(); };
    const wasSet = sh => kept.has(sh) || targets.some(t => t.inSet && t.sheetId && t.sheetId === sh.sheetId);   // (in its set when the order went on it)
    if (open) for (const [m, n] of byMetal) {
      const page = pick(m, rid), t = describe(m, page, n);
      targets.push(t);
      emit({ type: 'target', sheetId: t.sheetId, label: t.label, spot: null, newSheet: t.newSheet, metal: m, page: t.page, density: t.density });
    }
    // pieces on a sheet already cut stay where they are: never touched, never put on again
    const stay = new Set(piecesOnSheets(rid).filter(x => cutSheet(x.sh)).map(x => x.c.poolId));

    /* 3 · the hold is lifted through the existing path: the pieces are made up and put on the sheet the intake would use */
    try {
      for (const r of rowsOfOrder(rid).filter(r => r.hold)) { r.heldAt = null; await G.Review.repool(r); }
      if (resumed && open) for (const r of rowsOfOrder(rid).filter(r => r.releasing && !r.hold && ['pulled', 'held'].includes(r.state) && !(r.problems || []).length && !(G.Pool && G.Pool.onSheets && G.Pool.onSheets(r)))) await G.Pool.poolAdd(r, theRun());
    } catch (e) { refresh(); return fail(`The hold could not be lifted: ${str(e && e.message || e)}`, 'hold', { released: rowsOfOrder(rid).every(r => !r.hold) }); }
    for (const r of rowsOfOrder(rid)) if (r.releasing) r.releasing.stage = 'attached';
    // a sheet already in its set keeps its set while it takes the order, and gets its new label (as a piece taken off does).
    // (Only now: a sheet kept is closed to the intake, so the order is put on first and the sheet kept right after)
    for (const x of piecesOnSheets(rid)) keep(x.sh);
    refresh();
    const after = rowsOfOrder(rid), still = after.filter(r => r.hold);
    if (still.length) { for (const r of after) delete r.releasing; await persist(); return fail(str(still[0].hold), 'hold'); }
    await liftInCloud(rid);   // (the hold is lifted on this page: the cloud's take-off marks say so too, whether or not a run is open to make the pieces up again)
    const stuck =after.filter(r => (r.problems || []).length);
    if (stuck.length) { for (const r of after) delete r.releasing; await persist(); return fail(`Order ${rid} is released and at the front of the queue, but it needs a look in Review: ${str(stuck[0].reason || (stuck[0].problems[0] && stuck[0].problems[0].reason) || 'a piece has no design yet')}`, 'review', { released: true }); }
    const ids = [...new Set(after.filter(r => r.releasing).flatMap(r => r.poolIds || []))], fresh = ids.filter(id => !stay.has(id));
    const split = splitLines(rid, after.filter(r => r.releasing), new Set([...stay].map(String)), fresh);   // (a pair or line with pieces on a sheet already cut: split, R3)
    const done = ['pooled', 'written', 'labelled', 'committed'];
    // (a line that is never cut, a chain only line or one completed by hand, has nothing to put on a sheet: it is "noDesign" with no pieces, and no reason to stop the release)
    const unplaced = after.filter(r => !done.includes(r.state) && !(r.state === 'noDesign' && !(r.poolIds || []).length));
    if (unplaced.length && open) { const r = unplaced[0]; for (const x of after) delete x.releasing; await persist(); return fail(`Order ${rid} is released and at the front of the queue, but ${str(r.reason || r.state)}`, 'place', { released: true }); }

    const finish = async (pages, placed, why, landed) => {
      const first = pages[0], where = pages.map(label).join(', '), said = landed && landed.length ? ` · ${landed.join(' ')}` : '';
      if (G.SheetEvents && G.SheetEvents.order) G.SheetEvents.order({ type: 'released', orderId: rid, id: `oh-rel-${rid}-${frontAt}`, by: who, at: Date.now(), sheetId: first ? first.sheetId || '' : '', sheet: where, setId: first && inSet(first) ? first.setId : '',
        text: (placed && where ? `Released from hold by ${who} · placed on ${where}` : `Released from hold by ${who} · front of the queue`) + (split.length ? ` · pair split: ${split.map(splitWords).join('; ')}` : '') + said,
        data: Object.assign({ frontAt, sheets: pages.map(label), newSheet: targets.some(t => t.newSheet), pieces: fresh.length }, Object.assign(split.length ? { split: split.map(x => x.groupKey) } : {}, said ? { landed: true } : null)) });
      for (const r of rowsOfOrder(rid)) delete r.releasing;
      for (const sh of kept) unkeep(sh);
      refresh(); await persist();
      try { if (G.agent) G.agent({ bridge: true }, 'DS', `Order ${rid} released from hold by ${who}${where ? ' and placed on ' + where : ''}, at the front of the queue`); } catch (_) { /* a log line only */ }
      const sheets = pages.map(sh => ({ sheetId: sh.sheetId || null, label: label(sh), metal: sh.metal, page: sh.page, newSheet: !!targets.find(t => t.metal === sh.metal && t.newSheet) }));
      emit({ type: 'released', rid, sheets, placed });
      emit(Object.assign({ type: 'done', placed }, why ? { why } : {}));
      return out(true, { released: true, placed, sheets });
    };
    if (!open) return finish([], false, 'no run is open: the order waits at the front of the queue');
    if (!fresh.length) return finish([], true);   // every piece was already on a sheet (one that is cut): nothing to place

    /* 4 · its pieces leave On hold for their sheet, the sheet nests now, and the engine waits until they are placed and saved */
    const to = new Map(); for (const [id, x] of locate(fresh)) { if (!to.has(x.sh)) to.set(x.sh, []); to.get(x.sh).push(id); }
    for (const [sh, list] of to) emit({ type: 'flight', toSheetId: sh.sheetId || null, poolIds: list, metal: sh.metal, page: sh.page, label: label(sh) });
    let pages;
    try { pages = await placeNow(fresh, emit, PLACE_MS); await savedSheets(pages, fresh, wasSet); }
    catch (e) { refresh(); return fail(`Order ${rid} is released and at the front of the queue. ${str(e && e.message || e)}`, 'place', { released: true, pending: true }); }
    for (const r of rowsOfOrder(rid)) if (r.releasing) r.releasing.stage = 'placed';

    /* 5 · the QR label of each sheet that changed, then the timeline's permanent step with the sheet and the time */
    for (const sh of pages) await remakeQr(sh, rid, emit, wasSet(sh));
    // a pair (or n discs) that could not go on one sheet is said, never "placed" as if whole (the sets and the order window read the same fact)
    const landed = landedWords(fresh);
    if (landed.length) emit({ type: 'pairSplit', rid, text: landed.join(' ') });
    return finish(pages, true, undefined, landed);
  }

  OH.release = function release(rid, opts) {
    rid = str(rid); opts = opts || {};
    if (RUNNING.has(rid)) return RUNNING.get(rid);
    const p = doRelease(rid, opts).catch(e => { const s = { type: 'error', message: str(e && e.message || e), rid, stage: 'run', at: Date.now() }; try { if (opts.onStep) opts.onStep(s); } catch (_) { /* none */ } return { ok: false, released: false, rid, placed: false, sheets: [], steps: [s], error: s.message }; })
      .finally(() => { RUNNING.delete(rid); const l = LAST.get(rid); if (l) l.running = false; });
    RUNNING.set(rid, p); return p;
  };
  OH.releaseStatus = rid => { const l = LAST.get(str(rid)); return { running: RUNNING.has(str(rid)), step: l ? l.step : null }; };
  /** A release a reload (or an error) cut short: its lines still carry `releasing`, so the release is finished here, once at a time. */
  OH.resumeReleases = async function resumeReleases() {
    if (!G.Orders || !G.Orders.rows || !G.B || !G.B.run) return [];
    const rids = [...new Set(G.Orders.rows().filter(r => r && r.order && r.releasing && r.state !== 'gone').map(r => str(r.order.receiptId)))], out = [];
    for (const rid of rids) {
      if (RUNNING.has(rid)) continue;
      const t = TRIED.get(rid) || { n: 0, at: 0 };
      if (t.n >= 3 || Date.now() - t.at < 30000) continue;
      TRIED.set(rid, { n: t.n + 1, at: Date.now() });
      out.push(await OH.release(rid, { name: ((G.Orders.rows().find(r => r.releasing && str(r.order.receiptId) === rid) || {}).releasing || {}).by }));
    }
    return out;
  };
  // after a load, once the workspace is back and the run is known (checked now and then: a workspace restored late is found too)
  if (typeof setInterval === 'function' && G.document) {
    const scan = () => { try { if (G.Session && G.Session.ready && !G.Session.ready()) return; OH.resumeReleases().catch(() => {}); } catch (_) { /* tried again */ } };
    setInterval(scan, 5000);
    OH._scan = scan;
  }
  OH._internals = { pick, describe, locate, planOf };
})();
