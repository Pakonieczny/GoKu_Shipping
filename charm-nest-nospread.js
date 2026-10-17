/* NoSpread: keep the pieces of a multi-piece or earring order on ONE sheet of its colour (NOSPREAD, Paul, 17 Oct 2026).
 *
 * Why. A Set of Sheets must hold every sheet that shares a multi-piece order (the cardinal rule, charm-nest-shared-orders.js). Each order spread over two sheets of one colour ties
 * them into one unit, and units chain: one set came out with 14 sheets. The owner ruled out relaxing the cardinal rule; his remedy, verbatim:
 *   "A. When practicable try to avoid spreading multi-piece and earring orders of the same colour between sheets. This approach is most important/effective at the transition point
 *    between sheets.
 *    B. When a sheet of a given colour is near completion the system should avoid adding orders of earrings and multi-piece orders to the almost complete sheet (Unless they all fit)
 *    and instead add them to the next sheet in-line. This will greatly reduce the cross pollination of orders between sheets of the same colour. There could be a circumstance where
 *    a given order is so big that it fills several sheets and the system must be smart enough and robust enough to deal with such a scenario."
 *
 * What this file is. The POLICY, pure (no DOM, no network, no page state): numbers in, a decision out. The page asks it two questions and keeps every other rule it has
 * (charm-nest-1.html: topupTakes, feedTurn, feedOn, keepOrdersWhole, overflowToNextSheet, sheetFull; charm-nest-bridge.js: LiveNest.intakePage, Pool.attachPool):
 *   1. admits(sheet, order)     may this order go onto this sheet?  (B: a sheet near completion takes a multi-piece order only if ALL its pieces fit; A: outside the transition
 *                                point the order goes where all its pieces can stay together; a BIG order starts on a fresh sheet)
 *   2. isBig / chunksOf / halve an order bigger than one sheet is cut into consecutive units that each fit a sheet and seated one unit after the other: consecutive sheets, the fewest possible
 *   (which sheet of the line takes an order is the first one that admits it; none: a new sheet; the page's intake does that with its own tests of a sheet being open, closed, cut or in a set)
 * The sheet and the order are plain objects the page builds from its own fill model (usablePt2, the fill ceiling maxFill, what is placed, what waits on the sheet) and its own piece
 * counting (charm-nest-orders.js pieceCountOf: `more` is the number of pieces the order will still bring, from the order's other lines: held, late, not pooled yet).
 *
 *   sheet = { usablePt2, placedPt2, pendingPt2, maxFill, gapFilling }
 *             placedPt2   the area seated on the sheet          pendingPt2   the area attached to it that no search has seated yet (reserved: it will take room)
 *             gapFilling  the sheet is filling its gaps (topupSettle opened it: the app's own mark of a nearly finished Gold or Silver sheet)
 *   order = orderOf({ pieces:[charm], more, placed }, area)  ->  { known, more, count, footprint, multi }
 *
 * THE SETTING. NOSPREAD.nearMargin: a Gold or Silver sheet is NEAR COMPLETION from (the 75 % of topupSettle's TOPUP.target) - nearMargin of its usable area filled, counting what is attached and waiting, and
 * always while it fills its gaps. 0.15 = from 60 % filled. Why 0.15 (tests/charm-nest/setsim-nospread.cjs, made-up days of 600 orders, a sheet of 45 orders, 3 seeds, 50 orders of 20 pieces among them):
 * margin 0 (only the gap fill and the room check) leaves 4 of 710 multi-piece orders on two sheets, 0.05 to 0.15 leave 0 to 1, and a margin of 0.25 or more starts one more sheet at the end of the day and
 * lowers the day's mean fill for no gain; the fill of released sheets is 77 % to 78 % at every margin. Page override: S.settings.nospreadMargin; S.settings.nospread = "off" puts the page back to what it
 * did before this file.                                                                                                                                                                         */
(function (root, factory) { const api = factory(); if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestNoSpread = api; })(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  /** The one setting (see above) and the constants that come with it, each from the app's own fill model. */
  const NOSPREAD = {
    nearMargin: 0.15,    // THE setting: near completion = filled to (target - nearMargin) or more, or filling its gaps
    target: 0.75,        // topupSettle's TOPUP.target: the fill at which Gold and Silver sheets are released (the page passes its own)
    fitAtNear: 1,        // the share of the room left that a whole multi-piece order may ask for on a sheet just entering the near zone
    fitAtTarget: 0.5,    // ... at the target, and on any sheet filling its gaps: charm-nest-1.html GAP_PACK (measured: of 40 multi-piece orders that tried a gap fill, 2 fitted, asking 20 % and 23 % of the room)
    bigShare: 0.9,       // an order asking for more than this share of what ONE sheet takes under the fill ceiling cannot be counted on to fit one sheet (a packing never reaches the whole ceiling)
    chunkShare: 0.9,     // a BIG order is cut into consecutive units ("chunks") of at most this share of what one sheet takes (the same assumption as bigShare: what asks for no more fits one sheet; the real solver seated 30 identical charms,
                         // 95 % of the ceiling, on a 60 x 40 mm sheet); each unit is seated whole like any order (the solver never seats part of an order); a unit that does not pack is cut in two (halve) and nested again
  };
  const num = (v, d) => (v == null || v === '' || !Number.isFinite(+v)) ? d : +v;
  const cfgOf = c => { const o = Object.assign({}, NOSPREAD, c || {}); for (const k of Object.keys(NOSPREAD)) o[k] = num(o[k], NOSPREAD[k]); return o; };

  /** An order, from its pieces: { known (pieces in hand), more (pieces still to come from its other lines), placed (pieces already on a sheet), count (all of them), footprint (area of what has to be placed, the pieces to come at the mean of those in hand), multi (2 or more pieces) }. */
  function orderOf(spec, area) {
    const ps = (spec && spec.pieces) || [], a = typeof area === 'function' ? area : c => +(c && c.areaPt2) || 0;
    const more = Math.max(0, Math.floor(num(spec && spec.more, 0))), placed = Math.max(0, Math.floor(num(spec && spec.placed, 0))), known = ps.length;
    const sum = ps.reduce((n, c) => n + a(c), 0), mean = known ? sum / known : 0;
    return { known, more, placed, count: known + more + placed, footprint: sum + more * mean, multi: known + more + placed >= 2 };
  }
  /** What the sheet has: fill (placed and waiting, share of its usable area), room (area left under the fill ceiling after what waits), fresh (nothing seated or waiting). */
  function view(s, cfg) {
    const c = cfgOf(cfg), usable = Math.max(1, num(s && s.usablePt2, 1)), cap = num(s && s.maxFill, 0.8), placed = Math.max(0, num(s && s.placedPt2, 0)), pending = Math.max(0, num(s && s.pendingPt2, 0));
    const fill = (placed + pending) / usable, nearFrom = c.target - c.nearMargin;
    return { usable, cap, capacity: usable * cap, fill, room: Math.max(0, usable * cap - placed - pending), fresh: placed + pending <= 1e-9, gapFilling: !!(s && s.gapFilling), nearFrom, near: !!(s && s.gapFilling) || fill + 1e-12 >= nearFrom };
  }
  /** The share of the room left a whole multi-piece order may ask for: all of it where the near zone starts, the measured gap-fill share (fitAtTarget) at the target and beyond, and on every sheet that fills its gaps. */
  function fitShare(fill, gapFilling, cfg) {
    const c = cfgOf(cfg), from = c.target - c.nearMargin;
    if (gapFilling || c.nearMargin <= 0 || fill >= c.target) return c.fitAtTarget;
    const t = Math.min(1, Math.max(0, (fill - from) / (c.target - from)));
    return c.fitAtNear + (c.fitAtTarget - c.fitAtNear) * t;
  }
  /** An order that asks for more than `bigShare` of what one sheet takes: it fills consecutive sheets, cut into units (chunksOf), the fewest possible. */
  const isBig = (order, capacityPt2, cfg) => !!order && capacityPt2 > 0 && order.footprint > cfgOf(cfg).bigShare * capacityPt2;

  /** A BIG order cut into consecutive units for the nest (the solver, like the page, never seats part of an order, so an order bigger than a sheet could never be seated at all): the pieces
   *  in the order they come, next-fit, into units of at most chunkShare of `capacityPt2`; the pieces of one line that make a group (`group(c)`: an earring pair, the discs of a necklace) stay in
   *  one unit when the group fits one. One unit (the order itself) when it fits. -> [[piece]] */
  function chunksOf(pieces, area, group, capacityPt2, cfg) {
    const c = cfgOf(cfg), a = typeof area === 'function' ? area : x => +(x && x.areaPt2) || 0, g = typeof group === 'function' ? group : () => null, limit = c.chunkShare * capacityPt2;
    const ps = pieces || []; if (!(limit > 0) || ps.reduce((n, x) => n + a(x), 0) <= limit) return ps.length ? [ps.slice()] : [];
    const atoms = []; const byGroup = new Map();
    for (const x of ps) { const k = g(x); if (k == null) { atoms.push([x]); continue; } let at = byGroup.get(k); if (!at) { at = []; byGroup.set(k, at); atoms.push(at); } at.push(x); }
    const out = []; let cur = [], used = 0;
    const put = list => { for (const x of list) { const ax = a(x); if (cur.length && used + ax > limit) { out.push(cur); cur = []; used = 0; } cur.push(x); used += ax; } };
    for (const at of atoms) {
      const need = at.reduce((n, x) => n + a(x), 0);
      if (at.length > 1 && need <= limit) { if (cur.length && used + need > limit) { out.push(cur); cur = []; used = 0; } cur.push(...at); used += need; }
      else put(at);   // (a group bigger than a unit is cut piece by piece)
    }
    if (cur.length) out.push(cur);
    return out;
  }
  /** A unit cut in two of about half its area each, in order -- the safety net for a unit that does not pack into an EMPTY sheet. The pieces of one group (`group(c)`: an earring pair, the discs of a
   *  necklace) are never parted unless the group alone asks for more than `limitPt2` (half of what a sheet takes): a pair that does not pack an empty sheet is a real problem for a person, not a thing to cut.
   *  [] when there is nothing to cut (one piece, or one group that is not bigger than the limit). */
  function halve(pieces, area, group, limitPt2) {
    const a = typeof area === 'function' ? area : x => +(x && x.areaPt2) || 0, g = typeof group === 'function' ? group : () => null, ps = pieces || []; if (ps.length < 2) return [];
    const atoms = [], byGroup = new Map();
    for (const x of ps) { const k = g(x); if (k == null) { atoms.push([x]); continue; } let at = byGroup.get(k); if (!at) { at = []; byGroup.set(k, at); atoms.push(at); } at.push(x); }
    const total = ps.reduce((n, x) => n + a(x), 0), half = total / 2;
    if (atoms.length < 2) { const only = atoms[0]; return only && total > (+limitPt2 || 0) ? halvePieces(only, a) : []; }   // (one group: cut only when it is bigger than a half sheet by itself)
    let acc = 0, cut = 1;
    for (let i = 0; i < atoms.length - 1; i++) { acc += atoms[i].reduce((n, x) => n + a(x), 0); cut = i + 1; if (acc >= half - 1e-9 * (half + 1)) break; }
    return [atoms.slice(0, cut).flat(), atoms.slice(cut).flat()];
  }
  function halvePieces(ps, a) {
    const half = ps.reduce((n, x) => n + a(x), 0) / 2; let acc = 0, cut = 1;
    for (let i = 0; i < ps.length - 1; i++) { acc += a(ps[i]); cut = i + 1; if (acc >= half - 1e-9 * (half + 1)) break; }
    return [ps.slice(0, cut), ps.slice(cut)];
  }

  /** May `order` go onto `sheet`? { take, why, room, need, share, near }.
   *  - a single piece: yes (the page's own try limits apply to it)
   *  - a BIG order: only a fresh sheet (it starts one; it never shares the room of a sheet with other orders' spreading)
   *  - more than the room left under the fill ceiling: no, whatever the sheet is (it cannot fit; it is not tried, nothing younger is lifted for it)
   *  - a sheet not near completion: yes (A: its pieces stay together on it; the search seats them or lifts the whole order)
   *  - a sheet near completion or filling its gaps (B): only if all its pieces fit: the footprint within `share` of the room left, and no line of the order still to come (the room kept for it would be
   *    taken by other orders, and the sheet is released soon: the late line would find it closed) */
  function admits(sheet, order, cfg) {
    const c = cfgOf(cfg), v = view(sheet, c);
    if (!order || !order.multi) return { take: true, why: 'single', room: v.room, need: order ? order.footprint : 0, share: 1, near: v.near };
    const need = order.footprint, share = fitShare(v.fill, v.gapFilling, c), out = (take, why) => ({ take, why, room: v.room, need, share, near: v.near });
    if (isBig(order, v.capacity, c)) return v.fresh ? out(true, 'big-starts-a-fresh-sheet') : out(false, 'big-needs-a-fresh-sheet');
    if (need > v.room + 1e-9) return out(false, 'no-room-for-all');
    if (!v.near) return out(true, 'filling');
    // a line still to come (held for a question, read late, not pooled yet): the room it needs is free now but other orders will take it, and a sheet near completion is released soon; the order waits for a sheet that is not
    if (order.more > 0) return out(false, 'lines-still-to-come');
    return need <= v.room * share + 1e-9 ? out(true, 'fits-whole') : out(false, 'near-completion');
  }
  return { NOSPREAD, orderOf, view, fitShare, isBig, chunksOf, halve, admits };
});
