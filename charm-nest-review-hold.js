/* Charm Nest · a sheet held by an order in Review (Paul, 12 Oct 2026: "If an order goes for review and is already on a sheet then keep that order on that sheet and just hold the
   sheet until the user resolves the issue within the Review tab, only then the sheet can proceed to the next step").

   This file has no screen and no Firebase of its own. The hold is DERIVED, every time it is asked, from two things the page already holds:
     · the order's review record (charm-nest-review-mark.js, CharmNestReviewMark.isMarked(rid): true only while the order is in Review and not resolved), which every computer reads from
       the cloud, so it is right after a reload and on a second computer;
     · where the order's pieces sit (OrderPieces, the one answer the rows, chips and rail read).
   Nothing is remembered here, nothing is written, and the pieces are never lifted off their sheet or nested again.

   How it stops a sheet: charm-nest-readiness.js reads an order in Review as a HELD piece (reason(rid) below is its source: the 'held' block, key 'held', worded "In Review ..."),
   so the sheet's Order check step is not done, the rail's '!' lists the order, Approve for laser cutting is grey with the order named, a set with the sheet waits as one (setGate says it
   once), the set release and the laser cut are refused, and the sheet is never ready to start or complete. The records the cloud answers (the Library's live read, the flow's state) cannot
   know the Review, so overlay(record) puts the stop on them; a sheet cut once before is never touched.

     CharmNestReviewHold.heldBy(sheetId)   -> [{ rid, name, reason, piece, poolIds, sheetId, pieces:[{poolId, piece}] }]   the orders in Review that sit on that sheet ([] = not held)
     .isHeld(sheetId) -> boolean           .reasonFor(sheetId) -> '' | one plain line naming every order that holds it
     .sheetsHeldBy(rid) -> [{ sheetId, name, setId }]                                the sheets an unresolved Review on that order holds
     .canHold(rid) -> { ok:true, sheets:[{sheetId,name,setId}] } | { ok:false, why }   why: ONE plain line (the piece was released / the sheet is already cut)
     .onChange(fn) -> unsubscribe          fn({ rids, sheetIds }) after the held set changed (a mark, a resolve, a piece moved, a reload)
     .inReview(rid) -> boolean            the order is in Review and not resolved (sync); automatic movers (Hold's fill) leave such an order where it is
     .keepsPlace(rid) -> boolean           in Review, or its Review was just answered by a pick: a saved link / an "Order changed" card never lifts its piece off a sheet (bridge Pool.keepsPlace)
     .openInReview(rid) -> boolean         opens that order's card in the Review tab
     .reason(rid) -> '' | 'In Review: ...' (readiness' source)      .overlay(record) -> record with the stop on its orderReadiness      .overlayFlow(record) -> the same, and its laser report
   Pure of its page: every lookup is made when asked (root.CharmNestReviewMark, root.OrderPieces, root.CharmNestReadiness), so the script order does not matter and a page without them is
   simply not held. */
(function (root, factory) { const api = factory(root); if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestReviewHold = api; })(typeof self !== 'undefined' ? self : globalThis, function (root) {
  'use strict';
  const safe = (fn, dflt) => { try { const v = fn(); return v === undefined ? dflt : v; } catch (_) { return dflt; } };
  const MARK = () => root.CharmNestReviewMark || null;
  const RD = () => root.CharmNestReadiness || (typeof require === 'function' ? safe(() => require('./charm-nest-readiness.js'), null) : null);
  const OP = () => root.OrderPieces || null;
  const ridOf = x => String(x == null ? '' : x).replace(/\D/g, '');
  const uniq = list => [...new Set(list.filter(Boolean))];
  const cutSheet = s => !!(s && (+s.laserDoneAt > 0 || +s.roseCutAt > 0));

  /* ── what is in Review: the review record, nothing else ── */
  const marked = () => {
    const M = MARK(); if (!M) return [];
    const list = safe(() => (typeof M.markedRids === 'function' ? M.markedRids() : typeof M.openRids === 'function' ? M.openRids() : []), []);
    return uniq((list || []).map(ridOf));
  };
  const isMarked = rid => { const M = MARK(); rid = ridOf(rid); return !!(rid && M && typeof M.isMarked === 'function' && safe(() => M.isMarked(rid), false)); };
  const markWhy = rid => { const M = MARK(); const w = safe(() => (M && typeof M.reasonOf === 'function' ? String(M.reasonOf(rid) || '') : ''), ''); return w.replace(/\s+/g, ' ').trim().slice(0, 120); };
  /** The order's pieces keep their place on their sheet: it is in Review, or a person just answered its Review by picking the charm (the saved link that pick makes must not lift the piece off its sheet to make it up again). Derived from the review record, as isMarked is. */
  function keepsPlace(rid) {
    rid = ridOf(rid); if (!rid) return false; if (isMarked(rid)) return true;
    const M = MARK(); const i = M && typeof M.info === 'function' ? safe(() => M.info(rid), null) : null;
    return !!(i && i.state === 'done' && i.how === 'pick');
  }
  /** The line readiness reads an order in Review by ('' when it is not): always starts "In Review". */
  function reason(rid) { rid = ridOf(rid); if (!rid || !isMarked(rid)) return ''; const w = markWhy(rid); return w ? `In Review: ${w}` : 'In Review'; }

  /* ── where the order's pieces are ── */
  const piecesOf = rid => safe(() => { const O = OP(); return O && typeof O.of === 'function' ? (O.of(rid) || []) : []; }, []);
  const EAR = { L: 'left earring', R: 'right earring' };
  /** A piece in plain words: "left earring", "piece 2 of 3", or '' for the one piece of a one-piece order. */
  function pieceWords(p, total) {
    if (!p) return '';
    if (EAR[p.side]) return EAR[p.side];
    return total > 1 ? `piece ${p.index || 1} of ${total}` : '';
  }
  const sheetName = (p, fallback) => String(p.sheetLabel || fallback || 'a sheet');
  /** The order's live pieces (cancelled and completed-by-hand ones are no pieces) with where each stands. */
  function livePieces(rid) {
    const all = piecesOf(rid).filter(p => p && !p.hand && p.problem !== 'noDesign');
    return all;
  }

  /* ── the answers ── */
  function heldBy(sheetId) {
    sheetId = String(sheetId || ''); if (!sheetId) return [];
    const rids = marked(); if (!rids.length) return [];
    const out = [];
    for (const rid of rids) {
      const all = livePieces(rid), mine = all.filter(p => p.sheetId === sheetId && p.state !== 'cut');
      if (!mine.length) continue;
      const words = mine.map(p => pieceWords(p, all.length)).filter(Boolean);
      const piece = words.length === 0 ? '' : words.length === 1 ? words[0] : words.length === 2 ? `${words[0]} and ${words[1]}` : `${words.length} pieces`;
      const r = reason(rid) || 'In Review';
      out.push({ rid, name: String((mine[0].label || mine[0].sku || '')).replace(/\s+/g, ' ').trim().slice(0, 48), reason: r, piece, poolIds: mine.map(p => p.poolId).filter(Boolean), sheetId,
        pieces: mine.map(p => ({ poolId: p.poolId, piece: pieceWords(p, all.length) })) });
    }
    return out;
  }
  const isHeld = sheetId => heldBy(sheetId).length > 0;
  function reasonFor(sheetId) {
    const h = heldBy(sheetId); if (!h.length) return '';
    const one = x => `Order ${x.rid}${x.piece ? ` (${x.piece})` : ''}`;
    return h.length === 1 ? `${one(h[0])} is in Review` : `${h.length} orders are in Review: ${h.slice(0, 2).map(one).join(', ')}${h.length > 2 ? ', and more' : ''}`;
  }
  function sheetsHeldBy(rid) {
    rid = ridOf(rid); if (!rid || !isMarked(rid)) return [];
    const seen = new Map();
    for (const p of livePieces(rid)) if (p.sheetId && p.state !== 'cut' && !seen.has(p.sheetId)) seen.set(p.sheetId, { sheetId: p.sheetId, name: sheetName(p), setId: p.setId || null });
    return [...seen.values()];
  }
  /** Whether Send to Review can hold this order's sheet(s): a piece already released (its QR label printed / Complete Order) or a sheet already cut cannot be held. */
  function canHold(rid) {
    rid = ridOf(rid);
    const raw = piecesOf(rid).filter(p => p && p.problem !== 'noDesign');
    const onSheet = raw.filter(p => !p.hand && p.sheetId), open = onSheet.filter(p => p.state !== 'cut'), cut = onSheet.filter(p => p.state === 'cut');
    if (open.length) {
      const seen = new Map();
      for (const p of open) if (!seen.has(p.sheetId)) seen.set(p.sheetId, { sheetId: p.sheetId, name: sheetName(p), setId: p.setId || null });
      return { ok: true, sheets: [...seen.values()] };
    }
    if (cut.length) { const names = uniq(cut.map(p => sheetName(p))); return { ok: false, why: `${names.length === 1 ? names[0] : 'Its sheets'} ${names.length === 1 ? 'is' : 'are'} already cut, so ${names.length === 1 ? 'it' : 'they'} cannot be held` }; }
    if (raw.some(p => p.hand)) return { ok: false, why: 'Its piece was already released (the QR label is printed), so no sheet can be held' };
    return { ok: true, sheets: [] };
  }
  function openInReview(rid) {
    const M = MARK(); rid = ridOf(rid);
    return safe(() => {
      if (M && typeof M.open === 'function') { M.open(rid); return true; }
      if (M && typeof M.show === 'function') { M.show(rid); return true; }
      const R = root.Review; if (R && typeof R.showOrder === 'function') { R.showOrder(rid); return true; }
      if (R && typeof R.showCard === 'function') { R.showCard('mark:' + rid); return true; }
      return false;
    }, false);
  }

  /* ── the stop on a record the cloud answered ── */
  /** The record with its orders in Review read as held pieces (orderReadiness only; the record is not changed and nothing is written). A sheet cut, archived or cut once before is returned as it came. */
  function overlay(rec) {
    if (!rec || typeof rec !== 'object') return rec;
    const rids = marked(); if (!rids.length) return rec;
    const R = RD(); if (!R || typeof R.reviewReport !== 'function' || typeof R.orderIds !== 'function') return rec;
    if (rec.archived || +rec.laserDoneAt > 0 || (typeof R.completedBefore === 'function' && R.completedBefore(rec))) return rec;
    const sid = rec.id || rec.sheetId, here = new Set(R.orderIds(rec)), want = rids.filter(r => here.has(r));
    if (!want.length) return rec;
    const reps = { ...(rec.orderReadiness || {}) }, ids = R.idsOf ? R.idsOf(rec) : (rec.poolIds || []);
    for (const rid of want) {
      const all = livePieces(rid), byId = new Map(all.map(p => [p.poolId, p]));
      const list = ids.filter(id => String(id).split('_')[0] === rid && !(byId.get(id) && (byId.get(id).hand || byId.get(id).state === 'cut'))).map((id, i) => {
        const p = byId.get(id);
        return { poolId: id, lineKey: (p && p.lineKey) || String(id).replace(/_\d+$/, ''), label: (p && p.label) || '', side: (p && (p.side === 'L' || p.side === 'R') ? p.side : null), index: (p && p.index) || i + 1, sheetId: sid, sheetLabel: safe(() => R.sheetLabel(rec), '') };
      });
      if (!list.length) continue;
      const total = Math.max(all.length, list.length);
      reps[rid] = R.reviewReport(reps[rid], rid, rec, reason(rid) || 'In Review', list);
      if (reps[rid] && total > (reps[rid].pieceCount || 0)) reps[rid] = { ...reps[rid], pieceCount: total };
    }
    return { ...rec, orderReadiness: reps };
  }
  /** overlay() for a record the flow plans from (op flowState): its laser report, read from the cloud's own copy, is told the Order check is not done. */
  function overlayFlow(rec) {
    const o = overlay(rec); if (o === rec) return rec;
    const L = rec.laser;
    return L && typeof L === 'object' && L.stages ? { ...o, laser: { ...L, stages: { ...L.stages, orders: false }, ready: false } } : o;
  }

  /* ── changes: the held set is re-read when the review record or a piece moved; callers hear only a real change ── */
  const subs = new Set();
  let sig = null, queued = false, wired = false;
  const held = () => { const m = new Map(); for (const rid of marked()) m.set(rid, sheetsHeldBy(rid).map(s => s.sheetId).sort().join(',')); return m; };
  const signature = h => JSON.stringify([...h.entries()].sort());
  let prev = new Map();
  function check() {
    queued = false;
    const now = held(), s = signature(now); if (s === sig) return; const was = prev; sig = s; prev = now;
    const rids = uniq([...now.keys(), ...was.keys()].filter(r => now.get(r) !== was.get(r)));
    if (!rids.length) return;   // (the first look at a page with nothing in Review: nobody is told)
    const sheetIds = uniq(rids.flatMap(r => String(now.get(r) || '').split(',').concat(String(was.get(r) || '').split(','))));
    const released = rids.some(r => !now.has(r) && was.has(r));
    for (const fn of [...subs]) safe(() => fn({ rids, sheetIds }), null);
    // the cards follow at once; a sheet that was held and is now free lets its run carry on (sets look again), with no press
    safe(() => root.LaserReview && root.LaserReview.changed && root.LaserReview.changed(), null);
    if (released) safe(() => root.RunCtl && root.RunCtl.poke && root.RunCtl.poke(true), null);
  }
  function later() { if (queued) return; queued = true; (typeof Promise !== 'undefined' ? Promise.resolve() : { then: f => f() }).then(check); }
  function wire() {
    if (wired) return; const M = MARK(); if (!M || typeof M.onChange !== 'function') return;
    wired = true;
    safe(() => M.onChange(() => { loadPieces(); later(); }), null);
    safe(() => { const O = OP(); if (O && typeof O.subscribe === 'function') O.subscribe(later); }, null);
    loadPieces(); later();
  }
  // the pieces of an order in Review are read once (OrderPieces.load: its own small read, cached) so a sheet in the Library names it after a reload
  const asked = new Set();
  function loadPieces() { safe(() => { const O = OP(); const want = marked().filter(r => !asked.has(r)); if (!want.length || !O || typeof O.load !== 'function') return; want.forEach(r => asked.add(r)); Promise.resolve(O.load(want)).then(later, () => { want.forEach(r => asked.delete(r)); }); }, null); }
  function onChange(fn) { if (typeof fn !== 'function') return () => {}; subs.add(fn); wire(); return () => { subs.delete(fn); }; }
  if (root && root.document && typeof root.addEventListener === 'function') {
    // wire as soon as the review module is there (it may load after this file)
    let tries = 0; const t = setInterval(() => { wire(); if (wired || ++tries > 40) clearInterval(t); }, 500);
  }

  return { inReview: isMarked, keepsPlace, heldBy, isHeld, reasonFor, sheetsHeldBy, canHold, onChange, openInReview, reason, overlay, overlayFlow, _check: check, _reset: () => { sig = null; prev = new Map(); asked.clear(); subs.clear(); wired = false; } };
});
