/* Charm Nest · the production flow of the rail (window.CNFlow).
   FLW1 (Paul, 9 Nov 2026: "Also, this list does not match the actual process flow so make sure it matches."): the rail's list was the old nesting run's
   own steps (Pull, Pool, Nest, Check, Engrave, Labels, Commit). It is now the flow an order goes through, in the order Timeline's lanes and words
   (charm-nest-timeline-ui.js LANES, STAGE_LABEL): Order in · In review / On hold · Design & engrave · On a sheet · Laser cut · Sorted · Welded ·
   Assembled · Shipped · Completed, each with the number of orders standing at it now.

   Where an order stands is where its SLOWEST piece is (the order window's rule, OrderTimelineUI.summary): each open line is put on one step, the
   order on the lowest of its lines. A line completed by hand waits on no step (it is skipped); an order made only of those is Completed.
     line, from what the page already holds (the bridge passes these in, nothing here reads anything):
       cancelled (its placement)            → not counted
       held (Orders.heldNow)                → In review / On hold
       on a sheet now (PiecePlacement)      → Laser cut when that sheet is cut (laserDoneAt, roseCutAt, its set's laserDoneAt), else On a sheet
       a Review card open on it             → In review / On hold
       no design yet, engraving to settle   → Design & engrave
       anything else                        → Order in (pulled, waiting for a sheet)
   The steps after the laser (sorted, welded, assembled, packed / label printed at Shipping) are scanned at the stations and live in each order's
   timeline, which this page does not read for every order. They are LEARNED, at no cost, from the timeline reads the page already makes (the order
   window, its Timeline, the Engrave tab's history: OrderTimeline.get) and from the events this page records itself (OrderTimeline.onRecord), the
   way the Completed pile learns a shipped order (charm-nest-completed.js). What was learned is kept on this computer (localStorage, one small
   list per workspace). An order whose later scans were never seen here stays at Laser cut; a step with nothing learned shows "—", never a guess.
   Shipped and Completed are the Completed pile's own orders (CNCompleted: shipped by a station or a label, and marked Completed on Etsy).

   API
     STAGES                    [{ id, label, lane, words }] in the flow's order
     lineRank(row, c)          pure: the line's place in STAGES (0-9), "hand", or null (not counted)
     orderRank(rows, c, rid)   pure: the order's place (its slowest line; the steps after the laser learned for it)
     count(rows, c)            pure: { counts: { id: n }, byRid: Map(rid → id), learned: { id: n } }
     learnEvents(rid, events)  keeps the furthest step after the laser the events show; true when it moved on
     learned(rid)              the step learned for an order (5-9), else 0
     world(name)               'production' | 'sandbox'
     hook()                    the OrderTimeline reads and records, learned from (done once, by itself)
   c (the page's answers): { place(row) → PiecePlacement | null, held(row), review(row), cut(row, place), nested(row), learned(rid), shipped(rid) → "shipped" | "etsyCompleted" | "" }
*/
(function (root) {
  'use strict';
  if (root.CNFlow) return;
  const doc = root.document;

  // the Timeline's lanes (charm-nest-timeline-ui.js LANES: Etsy, Office, Sheet & laser, Sorting, Welding, Assembly, Shipping) and its words
  const STAGES = [
    { id: 'order', label: 'Order in', lane: 'etsy', words: 'Received from Etsy, waiting for a sheet' },
    { id: 'review', label: 'In review / On hold', lane: 'office', words: 'A person has to decide (Review), or the order is on hold' },
    { id: 'design', label: 'Design & engrave', lane: 'office', words: 'Its design or its engraving is still being settled' },
    { id: 'sheet', label: 'On a sheet', lane: 'sheet', words: 'Placed on a sheet, not cut yet' },
    { id: 'cut', label: 'Laser cut', lane: 'sheet', words: 'Its sheet is cut on the laser (Rose Gold / 10K / 14K: Rose cut)' },
    { id: 'sorted', label: 'Sorted', lane: 'sorting', words: 'Scanned at Sorting', station: true },
    { id: 'welded', label: 'Welded', lane: 'welding', words: 'Scanned at Welding (stud earrings)', station: true },
    { id: 'assembled', label: 'Assembled', lane: 'assembly', words: 'Scanned at Assembly', station: true },
    { id: 'shipped', label: 'Shipped', lane: 'shipping', words: 'Packed, label printed and shipped', station: true },
    { id: 'completed', label: 'Completed', lane: 'etsy', words: 'Completed on Etsy' }
  ];
  const AT = Object.fromEntries(STAGES.map((s, i) => [s.id, i]));
  const SORTED = AT.sorted, CUT = AT.cut, REVIEW = AT.review;
  // the station events after the laser, as the Timeline ranks them (whereOf's RANK: sorted 4, welded 5, assembled 6, packed / label printed 7, shipped 8, completed on Etsy 9)
  const LATE = { sorted: AT.sorted, welded: AT.welded, assembled: AT.assembled, packed: AT.shipped, shipped: AT.shipped, etsyCompleted: AT.completed };
  const late = e => { if (!e || !e.type) return 0; if (e.type === 'labelPrinted') return String(e.station || '') === 'shipping' ? AT.shipped : 0; return LATE[e.type] || 0; };

  const digits = v => { const s = String(v == null ? '' : v).trim(); return /^CUSTOM-/i.test(s) ? s : s.replace(/\D/g, ''); };
  const ask = (f, ...a) => { try { return typeof f === 'function' ? f(...a) : undefined; } catch (_) { return undefined; } };

  /* ═══ one line, one order ═══ */
  function lineRank(r, c) {
    if (!r || r.state === 'gone') return null;
    const pl = ask(c.place, r) || null;
    if (pl && pl.state === 'cancelled') return null;
    if (pl && pl.state === 'hand') return 'hand';
    if (ask(c.held, r)) return REVIEW;
    // (some copies on a sheet and some not: the slowest copy is not on one yet)
    const onSheet = pl && pl.state !== 'loading' ? (pl.state === 'sheet' || !!pl.onSheet) && !pl.partial : !!ask(c.nested, r);
    if (onSheet) return ask(c.cut, r, pl) ? CUT : AT.sheet;
    if (ask(c.review, r) || r.state === 'unmatched') return REVIEW;
    if (r.state === 'noDesign' || (r.engrave && r.engrave.needed && !r.engrave.approved)) return AT.design;
    return AT.order;
  }
  function orderRank(rows, c, rid) {
    const ranks = (rows || []).map(r => lineRank(r, c)).filter(x => x != null);
    if (!ranks.length) return null;
    const live = ranks.filter(x => x !== 'hand');
    if (!live.length) return AT.completed;                                     // (every piece completed by hand)
    const slow = Math.min(...live), was = +ask(c.learned, rid) || 0;
    // (a station's scan is the order's, its QR label: once seen, the order is there, unless a piece of it is held or back in Review)
    return was >= SORTED && slow !== REVIEW && was > slow ? was : slow;
  }
  /** The open orders on each step, and the shipped ones (the Completed pile) on Shipped and Completed. */
  function count(rows, c, shippedList) {
    const counts = Object.fromEntries(STAGES.map(s => [s.id, 0])), byRid = new Map(), groups = new Map();
    for (const r of rows || []) { if (!r || !r.order) continue; const rid = String(r.order.receiptId); if (!groups.has(rid)) groups.set(rid, []); groups.get(rid).push(r); }
    for (const [rid, rs] of groups) {
      const k = orderRank(rs, c, rid); if (k == null) continue;
      const id = STAGES[k].id; counts[id]++; byRid.set(rid, id);
    }
    for (const e of shippedList || []) { if (!e || !e.rid || groups.has(String(e.rid))) continue; counts[e.type === 'etsyCompleted' ? 'completed' : 'shipped']++; }
    return { counts, byRid };
  }

  /* ═══ what was learned of the steps after the laser ═══ */
  const KEY = 'cn.flow.v1', MAX = 3000;
  let worldName = 'production', store = null, saveT = 0;
  const listeners = new Set();
  const keyOf = () => KEY + ':' + worldName;
  function load() {
    if (store) return store;
    store = new Map();
    try { const j = JSON.parse(root.localStorage.getItem(keyOf()) || '[]'); if (Array.isArray(j)) for (const e of j) if (Array.isArray(e) && e[0]) store.set(String(e[0]), [+e[1] || 0, +e[2] || 0]); } catch (_) {}
    return store;
  }
  function save() {
    clearTimeout(saveT);
    saveT = setTimeout(() => {
      try {
        let all = [...load().entries()];
        if (all.length > MAX) { all.sort((a, b) => b[1][1] - a[1][1]); all = all.slice(0, MAX); store = new Map(all); }
        root.localStorage.setItem(keyOf(), JSON.stringify(all.map(([rid, v]) => [rid, v[0], v[1]])));
      } catch (_) { /* a full storage keeps it in memory for this visit */ }
    }, 400);
  }
  function world(name) { const w = name === 'sandbox' ? 'sandbox' : 'production'; if (w === worldName && store) return; worldName = w; store = null; load(); }
  function learnEvents(rid, events) {
    rid = digits(rid); if (!rid || !Array.isArray(events)) return false;
    let best = 0, at = 0;
    for (const e of events) { const k = late(e); if (k > best || (k === best && k && +e.at > at)) { best = k; at = +e.at || 0; } }
    if (!best) return false;
    const m = load(), was = m.get(rid);
    if (was && was[0] >= best) return false;
    m.set(rid, [best, at || Date.now()]); save();
    for (const f of listeners) { try { f(rid, best); } catch (_) {} }
    return true;
  }
  const learned = rid => { const v = load().get(digits(rid)); return v ? v[0] : 0; };
  const learnedCount = () => load().size;
  const onChange = f => { listeners.add(f); return () => listeners.delete(f); };

  function hook() {
    const T = root.OrderTimeline;
    if (!T || T._flowHooked) return !!T;
    T._flowHooked = true;
    if (typeof T.get === 'function') {
      const get = T.get;
      T.get = function (orderId) {
        const p = get.apply(this, arguments);
        try { Promise.resolve(p).then(tl => { if (tl && tl.events) learnEvents(orderId, tl.events); }, () => {}); } catch (_) {}
        return p;
      };
    }
    if (typeof T.onRecord === 'function') try { T.onRecord(ev => { if (ev && ev.orderId && late(ev)) learnEvents(ev.orderId, [ev]); }); } catch (_) {}
    return true;
  }

  load(); hook();
  if (!root.OrderTimeline && doc) doc.addEventListener('DOMContentLoaded', hook, { once: true });
  root.CNFlow = { STAGES, AT, lineRank, orderRank, count, learnEvents, learned, learnedCount, onChange, world, hook };
})(typeof self !== 'undefined' ? self : this);
