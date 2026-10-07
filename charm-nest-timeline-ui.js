/*  charm-nest-timeline-ui.js — the order timeline, drawn (Paul, 28 Sep 19:21: C1-C3, C8, E2, E3).
 *  Design: plans/design/order-view-spec.md §5-6 and its prototype; the stamps are siblings of the Seal family in
 *  charm-nest-motion.js (same 120-unit geometry, ink texture, ring words, date/time/name in the middle).
 *
 *    const tl = OrderTimelineUI.mount(el, { orderId, highlight, live: true, compact: false, onSheet(sheetId, poolId),
 *                                           onOpen(event), onEvents(events), onNow({ text, where, step, next, … }),
 *                                           stages: OrderTimelineUI.stagesFor(lines) | () => its steps })
 *    tl.refresh() → Promise   tl.focus(eventId | event | { stage }) → true when its seal is ringed on the chart   tl.destroy()
 *    onEvents/onNow tell the host what is drawn after every change; onOpen is compact's "open this on the Timeline".
 *    stagesFor(line | lines) → a piece's own steps (Welded only for a stud earring, Engraved only with a back engraving);
 *    isStud(line) → whether it is a stud; engraveOf(line) → true/false, null when not known yet.
 *
 *  Reads OrderTimeline.get(orderId) → { events, cancelled, where }. Draws, top to bottom:
 *    Now + milestone rail  where the order is right now (the server's `where`), and one stamp per step of its own
 *                          rail (Order in → Nested → [Engraved] → Laser cut → Sorted → [Welded] → Assembled → Shipped;
 *                          Welded only when a piece is a stud earring, Engraved only when one has a back engraving): passed steps stamped, the next one pulsing, the ones to come
 *                          faint outlines; a cancelled order gets a red CANCELLED stamp across the rail
 *    lanes                 one lane per place, one column per day (idle days collapse), one stamp per event, a gold
 *                          path from event to event, the NOW line and the milestones to come as dashed stamps
 *  Nothing is drawn under the chart (Paul, 5 Oct 2026: "remove this entire UI and functionality from the Timeline tab": the detail pane,
 *  its Next for this order / All that … needs / Around this step, and the selection and the keys that served them are gone): a stamp
 *  is never "selected" and a click on one opens nothing. tl.focus(…), opts.highlight and a click on a rail step only put a thin ring in
 *  the accent round that stamp on the chart (and bring it into view).
 *  A stamp rested on, reached with Tab or tapped grows where it stands (Seal.zoom in charm-nest-motion.js: one adaptive zoom for every
 *  seal, small ones more, never a second copy), with the step's explainer card under it.
 *  compact: true draws the rail alone, sized to its host (the order view's header); a rail stamp asks the host to open
 *  it on the Timeline: opts.onOpen(event), or else a bubbling "timeline:focus" event, detail { eventId }.
 *  nowStamps()/wireNow(): the order view's "Where it is now" seal and latest stamps (see the end of this file).
 *  Live: OrderTimeline.onRecord for this order, plus a refresh while the tab is visible: every 2.5 s for the order view that
 *  is open (its shared feed, below; a change made on this page is read again at once and 1.5 s later), every 20 s for a
 *  mount of its own; a new stamp comes down on its lane, the NOW line glides and the rail moves on. Motion is transform and opacity only; none under
 *  prefers-reduced-motion. destroy() clears every timer and listener it set.
 *  The chart fills its room (5 Oct, "the chart fits its room" below): its grid takes all the space its host gives it, and every part (lanes,
 *  seals up to the 84 px cap, text, strokes, day heads, the NOW pill) is sized from that room again whenever it changes (a ResizeObserver). */
(function (root) {
  "use strict";
  if (root.OrderTimelineUI) return;
  const doc = root.document;
  const E = "cubic-bezier(.2,.8,.2,1)", SLIDE = "cubic-bezier(.3,.1,.2,1)";
  const POLL = 20000;
  // the order view that is open (Paul, 3 Oct 04:03: "within two or three seconds maximum to reflect whatever the user is
  // doing"): its feed reads every POLL_OPEN while the tab is visible, and again at once and POLL_AGAIN after a change made
  // on this page; reads are never closer than POLL_GAP, fail with a back-off up to POLL_FAIL, and a seal this page stamped
  // is kept for LOCAL_TTL until the server's answer has it
  const POLL_OPEN = 2500, POLL_AGAIN = 1500, POLL_GAP = 600, POLL_FAIL = 30000, LOCAL_TTL = 180000;
  /* Firebase cost (7 Oct 2026): a poll of an open order view read the whole timeline (dozens of documents and their bytes) every 2.5 s, 1,440 times an
     hour, answered or not. Now the poll carries the revision of the answer the view holds (OrderTimeline.get ifRev: the server compares the update times of
     the documents the answer is made of, no field read back) and gets { unchanged } when none moved, which is nearly always: the view is redrawn from a
     whole answer only when something changed, and a whole answer is read at least every FULL_EVERY (a record rewritten in place leaves no trace in the
     digest). The pace follows the person: 2.5 s while anyone touches the page or something moved in the last 3 minutes, then 5 s, after 10 minutes 10 s,
     after an hour 30 s (a window left open overnight); a touch of the page, a change made on it, or the tab shown again is back at 2.5 s at once. */
  const FULL_EVERY = 60000, IDLE_STEPS = [[3 * 60000, 1], [10 * 60000, 2], [60 * 60000, 4], [Infinity, 12]];
  let lastTouch = Date.now(), touchAt = 0; const wakers = new Set();
  const touch = () => {
    const t = Date.now(); lastTouch = t;
    if (t - touchAt < 1000) return; touchAt = t;
    for (const w of [...wakers]) { try { w(); } catch (_) {} }
  };
  try { for (const ev of ["pointerdown", "pointermove", "keydown", "wheel", "touchstart"]) doc.addEventListener(ev, touch, { capture: true, passive: true }); } catch (_) {}
  const reduced = () => { try { return !!root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (_) { return false; } };
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
  const str = (v, n) => String(v == null ? "" : v).slice(0, n);
  const hash = s => { let h = 2166136261; for (const c of String(s)) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); } return h >>> 0; };
  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  /** One Web Animation, or none when motion is reduced (the end state is what the page already shows). */
  function anim(el, frames, ms, o) {
    if (!el || typeof el.animate !== "function" || reduced()) return null;
    try { const a = el.animate(frames, Object.assign({ duration: ms, easing: E }, o || {})); a.finished.catch(() => {}); return a; } catch (_) { return null; }
  }

  /* ── the zoomed seal (Paul, 2 Oct 2026: "get rid of the hover states on all the seals and just add a compelling zooming
     animation where the seal grows in size in its current position"): the stamp itself grows from its own centre, by the one
     adaptive curve of Seal.zoom (charm-nest-motion.js), and goes back the same way. There is no second seal. The room under
     the grown seal belongs to the step's explainer card, which is placed below the seal's grown rectangle. ── */
  const SEAL_SIZE = 84;
  /* ── the Timeline page's own seal sizes (Paul, 3 Oct 2026, on an order's Timeline: "All of these seals are too big ... they can be
     much smaller, and then they don't have to zoom in so large"): about 40% under the shared 84 px, on this page only (every other
     page keeps the shared size). The header rail's steps keep one size of their own, and the rail's rows are cut to fit them. A seal still shows
     everything it showed; the zoom (Seal.zoom) is what reads it.
     (Paul, 5 Oct 2026, with the detail pane gone: "expand the chart and enlarge the seals and all other components of the chart to fit the empty
     space below": the chart's own seals are no longer one fixed 26 px but the size its room allows, up to FIT_CAP, the shared 84 px: see "the
     chart fits its room" below.) ── */
  const TL_RAIL_SEAL = 20;
  const zoomApi = () => (root.Seal && root.Seal.zoom) || null;
  const zoomOn = (el, kb) => { const z = zoomApi(); return !!(z && el && z.show(el, { keyboard: !!kb, managed: true })); };
  const zoomOff = (el, now) => { const z = zoomApi(); if (z && el) z.hide(!!now, el); };
  /** The page rectangle the zoomed seal fills (null when it is not zoomed): what cards stay clear of. */
  const zoomRect = el => { const z = zoomApi(); return (z && el && z.rectOf(el)) || null; };
  /** `b`'s own rectangle widened to the zoomed seal's, for a card that must clear both. */
  const withZoom = (b, el) => {
    const zr = zoomRect(el); if (!zr) return null;
    return { getBoundingClientRect: () => { const r = b.getBoundingClientRect(), L = Math.min(r.left, zr.left), R = Math.max(r.right, zr.right), T = Math.min(r.top, zr.top), B = Math.max(r.bottom, zr.bottom); return { left: L, right: R, width: R - L, top: T, height: B - T, bottom: B }; } };
  };

  // Every timeline/overview seal uses the same rest and departure rules. The original seal alone owns the hover;
  // the grown seal and explanatory cards cannot keep it alive, and moving to a neighbour starts a new full delay.
  // (Paul, 2 Oct: 750 ms, then 500 ms, so a pointer running across the screen zooms nothing.)
  // The rest delay is the engine's one named constant (Seal.zoom.DELAY, 500); 500 here only covers a page without the engine.
  const sealHoverDelay = () => { const z = zoomApi(); return z && z.DELAY > 0 ? z.DELAY : 500; };
  // A mouse click on a seal zooms it at once (the rest is skipped) and the zoom stays until the pointer leaves it, Esc, a scroll or a press
  // elsewhere; a second click on a seal that is only to look at (not a button) puts it back; a seal whose click goes elsewhere (o.click(b)
  // false) leaves the zoom to that. A zoom that came by resting is "second" only when it was already grown when the press began.
  function restOnSeal(host, pick, show, hide, o) {
    let wanted = null, shown = null, timer = 0, point = null, pre = null, held = null, clickAt = 0;
    const lit = b => { const z = zoomApi(); return !z || !!z.rectOf(b); };   // still grown (a scroll or a redraw may have put it back)
    const at = node => { const b = node && pick(node); return b && host.contains(b) ? b : null; };
    const underPointer = b => !point || typeof doc.elementFromPoint !== "function" || at(doc.elementFromPoint(point.x, point.y)) === b;
    const cancel = now => { clearTimeout(timer); timer = 0; wanted = shown = null; hide(now); };
    const open = (b, kb, click) => { clearTimeout(timer); timer = 0; wanted = null; shown = b; show(b, !!kb, !!click); };
    const over = ev => {
      if (ev.pointerType === "touch") return;
      const b = at(ev.target); if (!b || b === wanted || b === shown || b === held) return;
      point = { x: ev.clientX, y: ev.clientY }; cancel(true); wanted = b;
      timer = setTimeout(() => { timer = 0; if (wanted === b && host.isConnected && b.isConnected && underPointer(b)) open(b); else cancel(true); }, sealHoverDelay());
    };
    const out = ev => {
      if (ev.pointerType === "touch") return;
      const b = at(ev.target);
      if (b && b === held && !b.contains(ev.relatedTarget)) held = null;
      if (b && (b === wanted || b === shown) && !b.contains(ev.relatedTarget)) cancel();
    };
    const move = ev => {
      if (ev.pointerType === "touch") return;
      point = { x: ev.clientX, y: ev.clientY };
      if ((wanted || shown) && at(ev.target) !== (wanted || shown)) cancel();
    };
    // a finger: a tap on a seal zooms it at once and stays (another tap on it, or a press elsewhere, puts it back); a mouse press
    // elsewhere lets it go too
    const down = ev => {
      const b = at(ev.target);
      if (ev.pointerType === "touch" && b) { if (b === shown) cancel(); else { cancel(true); point = null; open(b); } return; }
      if ((wanted || shown) && b !== (wanted || shown)) cancel();
      pre = b && b === shown && lit(b) ? b : null; if (held && b !== held) held = null;
    };
    const click = ev => {
      if (ev.pointerType === "touch") return;   // a finger zoomed it on its press
      const b = at(ev.target), was = pre; pre = null;
      if (!b || (o && o.click && !o.click(b))) return;
      if (b === shown && lit(b)) { if (was === b && ev.detail && !b.closest("button, a")) { held = b; cancel(); } return; }
      cancel(true); open(b, !ev.detail, true); clickAt = Date.now(); if (ev.detail) point = { x: ev.clientX, y: ev.clientY };
    };
    const enter = ev => {   // Enter or Space on a seal that has the focus (not a button: that has its own press) grows it or puts it back
      const b = (ev.key === "Enter" || ev.key === " ") && at(ev.target); if (!b || b !== ev.target || b.closest("button, a")) return;
      ev.preventDefault(); if (b === shown && lit(b)) cancel(); else { cancel(true); open(b, true); }
    };
    const blur = () => cancel(true), vis = () => { if (doc.visibilityState === "hidden") cancel(true); };
    const key = ev => { if (ev.key === "Escape" && (wanted || shown)) { ev.preventDefault(); ev.stopPropagation(); cancel(true); } };
    const check = () => {
      const cur = wanted || shown; if (!cur) return;
      // (a click the page answers by drawing its seals again puts the zoom back: the seal now under the pointer grows in its place)
      const redrawn = shown === cur && Date.now() - clickAt < 1200 && (!cur.isConnected || !lit(cur));
      if (cur.isConnected && !redrawn) return;
      const idOf = b => { const n = b.closest("[data-key], [data-stage]"); return n ? (n.dataset.key || "") + "|" + (n.dataset.stage || "") : ""; };
      const found = redrawn && point && typeof doc.elementFromPoint === "function" ? at(doc.elementFromPoint(point.x, point.y)) : null, again = found && idOf(found) !== "|" && idOf(found) === idOf(cur) ? found : null;   // (the very seal that was clicked, not a neighbour that has moved under the pointer)
      cancel(true); if (again) open(again, false, true);
    };
    const observer = new root.MutationObserver(check); observer.observe(host, { childList: true, subtree: true });
    host.addEventListener("pointerover", over); host.addEventListener("pointerout", out); host.addEventListener("click", click); host.addEventListener("keydown", enter);
    doc.addEventListener("pointermove", move, true); doc.addEventListener("pointerdown", down, true); doc.addEventListener("visibilitychange", vis); doc.addEventListener("keydown", key, true);
    root.addEventListener("blur", blur); root.addEventListener("scroll", blur, true); root.addEventListener("resize", blur);
    return { open, cancel, check, get current() { return wanted || shown; }, destroy() {
      cancel(true); observer.disconnect(); host.removeEventListener("pointerover", over); host.removeEventListener("pointerout", out); host.removeEventListener("click", click); host.removeEventListener("keydown", enter);
      doc.removeEventListener("pointermove", move, true); doc.removeEventListener("pointerdown", down, true); doc.removeEventListener("visibilitychange", vis); doc.removeEventListener("keydown", key, true);
      root.removeEventListener("blur", blur); root.removeEventListener("scroll", blur, true); root.removeEventListener("resize", blur);
    } };
  }

  /* ════ Stamp family: siblings of the two existing seals (Seal in charm-nest-motion.js) ════
     m = milestone: scalloped, like "Order completed"; e = event: double-ring postmark, like "QR label printed";
     a = alert: a thick notched ring. Small face (the lanes): heavy edge, ink wash, big icon. Full face (loupe, detail):
     the kind on the top ring, where on the bottom ring, date, time and name in the middle. */
  const INK = { blue: "#22408f", green: "#19663f", gold: "#8b6420", rose: "#9a4f45", slate: "#2f5563", clay: "#a8321e", velvet: "#3b362f", note: "#5b554c" };
  const RED = "#b42318";
  const ICON = {
    arrived: "M3 13h5l1.5 3h5L16 13h5M5 5h14l2 8v6H3v-6z",
    pull: "M12 3v12M7 10l5 5 5-5M4 19h16",
    eye: "M2 12c3-5 6.5-7.5 10-7.5S19 7 22 12c-3 5-6.5 7.5-10 7.5S5 17 2 12zM12 9a3 3 0 1 1 0 6a3 3 0 1 1 0-6",
    question: "M8.5 9a3.5 3.5 0 1 1 5 3.2c-1 .5-1.5 1.2-1.5 2.3v.5M12 18.5v.5",
    skip: "M5 6l7 6-7 6zM13 6l7 6-7 6z",
    check: "M5 12.5l4.5 4.5L19 7.5",
    pen: "M12 3l6 7-6 11-6-11zM12 10v5",
    sheet: "M3 6h18v12H3zM14 10h4v4h-4zM9 9.8a2.2 2.2 0 1 1 0 4.4a2.2 2.2 0 1 1 0-4.4",
    stack: "M12 3l9 5-9 5-9-5zM3 13l9 5 9-5",
    moved: "M4 8h14l-3-3M20 16H6l3 3",
    out: "M4 4h10v16H4zM10 12h11M17 8l4 4-4 4",
    renest: "M20 11a8 8 0 1 0-2.3 5.7M20 4v7h-7",
    pause: "M9 6v12M15 6v12",
    play: "M8 5l11 7-11 7z",
    undo: "M9 14L4 9l5-5M4 9h10a6 6 0 0 1 0 12h-3",
    x: "M6 6l12 12M18 6L6 18",
    etsyx: "M15 6H8v12h7M8 12h5M3.5 20.5l17-17",
    resize: "M4 14v6h6M20 10V4h-6M4 20l7-7M20 4l-7 7",
    merge: "M6 4v5a6 6 0 0 0 6 6v5M18 4v5a6 6 0 0 1-6 6",
    set: "M4 8h11v11H4zM8 4h12v12",
    greenline: "M3 17L21 7",
    scissors: "M6 3.4a2.6 2.6 0 1 1 0 5.2a2.6 2.6 0 1 1 0-5.2M6 15.4a2.6 2.6 0 1 1 0 5.2a2.6 2.6 0 1 1 0-5.2M8.2 16.6L20 6M8.2 7.4L20 18",
    qr: "M3 3h7v7H3zM14 3h7v7h-7zM3 14h7v7H3zM14 14h3v3h-3zM18 18h3v3h-3zM18 14h3M14 18v3",
    lock: "M6 11h12v9H6zM8.5 11V8a3.5 3.5 0 0 1 7 0v3",
    laser: "M12 2v9M8 14.5l-3 2M16 14.5l3 2M12 15v5M6 21h12",
    grid: "M3 3h8v8H3zM13 3h8v8h-8zM3 13h8v8H3zM13 13h8v8h-8z",
    spark: "M12 2v6M12 16v6M2 12h6M16 12h6M5 5l4 4M15 15l4 4M19 5l-4 4M9 15l-4 4",
    chain: "M9.5 14.5l-2 2a3.5 3.5 0 0 1-5-5l3-3a3.5 3.5 0 0 1 5 0M14.5 9.5l2-2a3.5 3.5 0 0 1 5 5l-3 3a3.5 3.5 0 0 1-5 0M9 15l6-6",
    box: "M3 8l9-5 9 5v9l-9 5-9-5zM3 8l9 5 9-5M12 13v9",
    tag: "M3 12V4h8l10 10-8 8zM7.5 6.2a1.3 1.3 0 1 1 0 2.6a1.3 1.3 0 1 1 0-2.6",
    plane: "M22 2L11 13M22 2l-7 20-4-9-9-4z",
    bag: "M4 8h16l-1.5 13h-13zM9 8V6a3 3 0 0 1 6 0v2M9 14l2 2 4-4",
    alert: "M12 3l10 18H2zM12 10v5M12 18v.5",
    bubble: "M4 5h16v11H9l-5 4z",
    mail: "M3 6h18v12H3zM3 6l9 7 9-7",
    barcode: "M4 5v14M7 5v14M9.5 5v14M13 5v14M15.5 5v14M18 5v14M20.5 5v14",
    dot: "M12 9a3 3 0 1 1 0 6a3 3 0 1 1 0-6"
  };
  // per event type: ink, edge (m/e/a), icon, its lane when no station says otherwise, and its filter groups
  const K = (ink, sh, ic, lane, grp) => ({ ink, sh, ic, lane, grp: (grp || "").split(" ").filter(Boolean) });
  const KIND = {
    arrived: K("velvet", "m", "arrived", "etsy"), pulled: K("velvet", "e", "pull", "office"), interpreted: K("velvet", "e", "eye", "office"),
    needsDecision: K("gold", "a", "question", "office", "hc"), decided: K("green", "m", "check", "office"), skipped: K("note", "e", "skip", "office"),
    customRead: K("velvet", "e", "eye", "office"), customDecided: K("green", "e", "check", "office"),
    designSent: K("gold", "e", "sheet", "office", "sh"), designDropped: K("gold", "e", "pen", "office", "sh"),
    engraveNeeded: K("gold", "e", "pen", "office"), engraveApproved: K("green", "e", "pen", "office"), engraveChanged: K("gold", "e", "pen", "office"),
    pooled: K("gold", "e", "stack", "sheet", "sh"), placed: K("gold", "m", "sheet", "sheet", "sh"), moved: K("gold", "e", "moved", "sheet", "sh"),
    removed: K("clay", "e", "out", "sheet", "sh hc"), renested: K("gold", "e", "renest", "sheet", "sh"),
    held: K("clay", "a", "pause", "office", "hc"), released: K("green", "e", "play", "office", "hc"), restored: K("green", "e", "undo", "office", "hc"),
    cancelled: K("clay", "a", "x", "office", "hc"), etsyCancelled: K("clay", "a", "etsyx", "etsy", "hc"), cancelRestored: K("green", "e", "undo", "office", "hc"),
    cancelStep: K("clay", "e", "out", "sheet", "sh hc"),
    sizeChanged: K("gold", "e", "resize", "sheet", "sh"), included: K("gold", "e", "set", "sheet", "sh"), excluded: K("note", "e", "set", "sheet", "sh"),
    merged: K("gold", "e", "merge", "sheet", "sh"), roseLine: K("rose", "e", "greenline", "sheet", "sh"), roseCut: K("rose", "e", "scissors", "sheet", "sh"),
    qrLabel: K("blue", "e", "qr", "sheet", "sh"), setCommitted: K("gold", "e", "lock", "sheet", "sh"), laserDone: K("gold", "m", "laser", "sheet", "sh"),
    recalled: K("clay", "e", "undo", "sheet", "sh"), sealPrinted: K("blue", "e", "qr", "sheet"), sealCompleted: K("green", "m", "check", "sheet"),
    scan: K("slate", "e", "barcode", "sorting", "stn"), sorted: K("slate", "m", "grid", "sorting", "stn"), welded: K("slate", "m", "spark", "welding", "stn"),
    assembled: K("slate", "m", "chain", "assembly", "stn"), packed: K("slate", "e", "box", "shipping", "stn"), labelPrinted: K("blue", "e", "tag", "shipping", "stn"),
    shipped: K("green", "m", "plane", "shipping", "stn"), etsyCompleted: K("green", "m", "bag", "etsy", "stn"), cancelAlert: K("clay", "a", "alert", "sorting", "stn hc"),
    note: K("note", "e", "bubble", "office", "msg"), teamMessage: K("note", "e", "bubble", "office", "msg"), customerMessage: K("note", "e", "mail", "etsy", "msg"),
    other: K("note", "e", "dot", "office")
  };
  const own = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
  const kindOf = t => own(KIND, t) ? KIND[t] : KIND.other;
  /* ── which of those kinds gets a seal (Paul, 28 Sep: "I only want actual real milestones to be recorded with seals and
     skip all the noise") ──
     MILESTONE_SEAL: the steps of the rail, the ones a person would call a step of the work.
     PERSON_SEAL: what a person did that changes the order — cancelled, held, taken off a sheet, put back.
     Everything else (read, interpreted, needs a decision, pulled, pooled, moved, QR label, set committed, included …)
     stays in the record and in memory — the step explainer reads it as plain text — but draws no stamp anywhere: not on
     the lanes, not in the "recent stamps" row, not on the Overview's "where it is now" card. */
  // (engraveApproved is the Engraved step; roseCut is a rose sheet's Laser cut; etsyCompleted folds into Shipped)
  const MILESTONE_SEAL = new Set(["arrived", "placed", "engraveApproved", "laserDone", "roseCut", "sorted", "welded", "assembled", "shipped", "etsyCompleted"]);
  const PERSON_SEAL = new Set(["cancelled", "etsyCancelled", "cancelRestored", "removed", "cancelStep", "held", "released", "restored", "cancelAlert", "designSent"]);
  const isPlainDecision = e => !!e && e.type === "engraveChanged" && !!e.data && e.data.how === "skipped";
  const sealed = e => !!e && (isPlainDecision(e) || MILESTONE_SEAL.has(e.type) || PERSON_SEAL.has(e.type) || !!opStepOf(e));
  /** The seals to draw, oldest first: one per rail step per piece. A step recorded again for the same line (a second
   *  sorting scan, a rose sheet's cut after its laser mark) keeps the first seal and hangs the rest on it as `same`, so
   *  nothing is lost — the explainer and the detail can still read them. Etsy's completion folds into the order's
   *  Shipped seal (its own seal only when nothing was shipped at a station). A person's action never collapses. */
  function sealsOf(events) {
    const out = [], first = new Map();
    let ship = null;
    for (const e of events || []) {
      if (!sealed(e)) continue;
      e.same = null;
      if (!MILESTONE_SEAL.has(e.type)) { out.push(e); continue; }
      const st = own(STOP_OF, e.type) ? STAGES[STOP_OF[e.type]].k : e.type;
      const g = st + "|" + (e.lineKey || e.transactionId || "");
      const kept = first.get(g) || (e.type === "etsyCompleted" && ship) || null;
      if (kept) { (kept.same = kept.same || []).push(e); continue; }
      first.set(g, e); out.push(e);
      if (st === "shipped" && !ship) ship = e;
    }
    return out;
  }
  /* ── every QR label printed is a seal of its own (Paul, 29 Sep 02:08: "Any time a label is printed whether it be
     through this software or through the charm sorting software that must be recorded that the label was printed for
     this order and the time it was printed. We already have the seals for the manual printing. Please use those to show
     in the timeline."). A print is the Charm Sorter's Print QR label / Print again (a Review card or the order window:
     the server's sealPrinted, one per line, read from its record's stamps for an older one, and the page's own
     labelPrinted), the Sorting station's or the Design Station's (labelPrinted). One press is one seal, however many
     events say it, numbered as the card numbers it (Print Nº n); a reprint is a seal of its own, and none ever goes. A
     shipping label stays with Shipped. The Charm Sorter's lie on the Office lane, the Sorting station's on Sorting.
     Drawn with the card's own seal (charm-nest-motion.js Seal.svg) where the page has it. ── */
  const PRINT_MS = 3 * 60 * 1000;
  const PRINT_WHERE = { charm: "Charm Sorter", sorting: "Sorting station", qr: "QR station", design: "Design Station" };
  const isPrint = e => !!e && (e.type === "sealPrinted" || (e.type === "labelPrinted" && labelStepOf(e) === "sorted"));
  const printPlace = e => e.type === "sealPrinted" || e.station === "sorter" || e.device === "charm-nest-1" || (!!e.data && e.data.label === "custom") ? "charm" : e.station || "other";
  const sameHand = (a, b) => !a.by || !b.by || String(a.by).trim().toLowerCase() === String(b.by).trim().toLowerCase();
  const lineOf = e => e.lineKey || e.transactionId || "";
  /** The print seals, oldest first: one per press, each the event it is drawn as, with print { n, where } and the other
   *  events of its press hung on it as `same` (so a click on any of them finds it). */
  function printSeals(events) {
    const press = [];
    for (const e of events || []) {
      if (!isPrint(e)) continue;
      e.same = null; e.print = null;
      const place = printPlace(e);
      if (place !== "charm") { press.push({ place, s: [], l: e }); continue; }
      if (e.type !== "sealPrinted") continue;
      // each line's seal of one press (customPut, a line at a time): the press just before, when it has none of this line
      const p = press.filter(x => x.place === "charm" && x.s.length).pop();
      if (p && !p.s.some(x => lineOf(x) === lineOf(e)) && e.at - p.s[0].at <= PRINT_MS && sameHand(p.s[0], e)) p.s.push(e);
      else press.push({ place, s: [e], l: null });
    }
    // the page's own word of a press (labelPrinted): the nearest press of the same person that has none yet
    for (const e of events || []) {
      if (!isPrint(e) || e.type !== "labelPrinted" || printPlace(e) !== "charm") continue;
      let best = null;
      for (const p of press) if (p.place === "charm" && !p.l && p.s.length && sameHand(p.s[0], e) && Math.abs(p.s[0].at - e.at) <= PRINT_MS && (!best || Math.abs(p.s[0].at - e.at) < Math.abs(best.s[0].at - e.at))) best = p;
      if (best) best.l = e; else press.push({ place: "charm", s: [], l: e });
    }
    const count = {}, out = [];
    for (const p of press.map(p => Object.assign(p, { rep: p.l || p.s[0] })).sort((a, b) => byAt(a.rep, b.rep))) {
      // its number: the record's count at that press (the card's Print Nº), else the one the page stamped, else the next
      const had = p.s.map(x => +(x.data && x.data.prints)).find(v => v > 0) || +(p.l && p.l.data && p.l.data.print) || 0;
      const n = had || (count[p.place] || 0) + 1; count[p.place] = Math.max(count[p.place] || 0, n);
      const all = (p.l ? [p.l] : []).concat(p.s), rep = p.rep;
      if (p.place === "charm") for (const x of all) x.lane = "office";
      rep.print = { n, where: PRINT_WHERE[p.place] || placeOf(rep) || "a station" };
      rep.same = all.length > 1 ? all.filter(x => x !== rep) : null;
      out.push(rep);
    }
    return out;
  }
  /** The seals drawn: the milestones and what a person did (sealsOf), and every label printed (printSeals). */
  const withPrints = events => sealsOf(events).concat(printSeals(events)).sort(byAt);
  /** A print's seal: on its lane the QR stamp; its full face the card's own (Seal.svg), else this file's with its words. */
  function printSvg(e, full, opts) {
    const n = +e.print.n || 1, plain = Object.assign({}, e, { type: "sealPrinted", print: null });
    if (!full) return stampSvg(plain, false, opts);
    const Sl = root.Seal;
    if (Sl && typeof Sl.svg === "function") {
      try {
        const svg = Sl.svg({ how: "print", at: +e.at || 0, by: personOf(e), n }), id = opts && opts.uid ? String(opts.uid).replace(/[^\w-]/g, "_") : "";
        return id ? svg.replace(/\bsl\d+([ftb])\b/g, id + "$1") : svg;   // (a uid: the same markup every time, as stampSvg's)
      } catch (_) {}
    }
    plain.data = Object.assign({}, e.data, { ring: "QR label printed", foot: "PRINT Nº " + n });
    return stampSvg(plain, true, opts);
  }
  const printTitle = e => `QR label printed · Print Nº ${e.print.n} · ${e.print.where}`;
  /** What is holding this order up, in plain words: a hold nobody released, or a question nobody answered. */
  function blockerOf(events) {
    let hold = null; const need = new Map(), byHand = new Map();
    for (const e of events || []) {
      // a line completed with Complete Order, or with its QR label printed, answers its question (a person finished it by hand); a Reopen asks it again
      const op = handStepOf(e), l = e.lineKey;
      if (op && l) { const [a, b] = op === "complete" ? [need, byHand] : [byHand, need]; if (a.has(l)) { b.set(l, a.get(l)); a.delete(l); } continue; }
      if (e.type === "held") hold = e;
      else if (e.type === "released" || e.type === "restored" || e.type === "cancelRestored") hold = null;
      else if (e.type === "needsDecision") need.set(e.lineKey || e.id, e);
      else if (e.type === "decided" || e.type === "customDecided" || e.type === "skipped") {
        if (e.lineKey && need.has(e.lineKey)) need.delete(e.lineKey); else need.clear();
      }
    }
    if (hold) return { label: "On hold", text: str((hold.data && (hold.data.reason || hold.data.why)) || hold.text || "Waiting to be let go", 220), at: hold.at, by: whoOf(hold) };
    const q = [...need.values()].pop();
    if (q) return { label: "Needs a decision", text: str(q.text || (q.data && (q.data.why || q.data.reason)) || "Someone has to decide before this order can go on", 220), at: q.at, by: whoOf(q) };
    return null;
  }
  const typeInfo = t => { try { const T = root.OrderTimeline && root.OrderTimeline.TYPES; return (T && T[t]) || null; } catch (_) { return null; } };
  const labelOf = t => (typeInfo(t) || {}).label || String(t || "Event").replace(/([a-z])([A-Z])/g, "$1 $2").replace(/^./, c => c.toUpperCase());
  const LANES = [
    { k: "etsy", l: "Etsy", ic: "bag" }, { k: "office", l: "Office", ic: "pen" }, { k: "sheet", l: "Sheet & laser", ic: "sheet" },
    { k: "sorting", l: "Sorting", ic: "grid", st: 1 }, { k: "welding", l: "Welding", ic: "spark", st: 1 }, { k: "assembly", l: "Assembly", ic: "chain", st: 1 }, { k: "shipping", l: "Shipping", ic: "box", st: 1 }
  ];
  const LANE = Object.fromEntries(LANES.map((L, i) => [L.k, Object.assign({ i }, L)]));
  const STATION_LANE = { sorting: "sorting", qr: "sorting", welding: "welding", assembly: "assembly", shipping: "shipping", laser: "sheet", design: "office", inbox: "office" };
  const STATION_NAME = { sorting: "Sorting", qr: "QR station", welding: "Welding", assembly: "Assembly", shipping: "Shipping", laser: "Laser", design: "Design Station", sorter: "Sorter", inbox: "Inbox" };
  const ETSY_SIDE = new Set(["arrived", "etsyCancelled", "etsyCompleted", "customerMessage"]);
  const laneOf = e => ETSY_SIDE.has(e.type) ? "etsy" : STATION_LANE[e.station] || (e.source === "etsy" ? "etsy" : kindOf(e.type).lane);
  /* who did it (Paul, 28 Sep 23:51): the person signed in on that station's own login, as the station recorded it. A step
     done with nobody signed in says so: data.signedIn false, or a completion recorded with no name (never a guess) */
  const COMPLETION = new Set(["placed", "renested", "setCommitted", "engraveApproved", "laserDone", "roseCut", "sorted", "welded", "assembled", "packed", "labelPrinted", "shipped", "sealPrinted", "sealCompleted"]);
  const unsigned = e => !!e && !String(e.by || "").trim() && e.source !== "etsy" && e.source !== "system" && !e.derived && ((!!e.data && e.data.signedIn === false) || COMPLETION.has(e.type));
  const personOf = e => String((e && e.by) || "").trim() || (unsigned(e) ? "Not signed in" : "");
  const whoOf = e => personOf(e) || (e.source === "etsy" ? "Etsy" : e.source === "station" ? "Station" : "Automatic");
  const stationName = e => STATION_NAME[e.station] || (LANE[e.lane] || LANE.office).l;
  // where it happened, as the seals and the step card say it ("Welded · Marco R. · Welding", "…at the Design Station")
  const placeOf = e => (e && own(STATION_NAME, e.station) ? STATION_NAME[e.station] : "");
  const AT_PLACE = { sorting: "the Sorting station", qr: "the QR station", welding: "the Welding station", assembly: "the Assembly station", shipping: "the Shipping station", laser: "the laser", design: "the Design Station", sorter: "the sorter", inbox: "the inbox" };
  /* A label printed: an order's QR label printed at the Sorting station or the Design Station (its Review tab) is a detail
     of the Sorted step; a shipping label is the Shipped step's. Never a step of its own. (_orderTimeline.js labelStepOf,
     keep the two alike.) data.label: sheetQR | orderQR | custom | shipping, else by the station. */
  const SORT_LABELS = new Set(["sheetQR", "orderQR", "custom"]), SORT_SIDE = new Set(["sorting", "design", "qr", "sorter"]);
  const labelStepOf = e => { const k = e && e.data && e.data.label; return k === "shipping" ? "shipped" : SORT_LABELS.has(k) || SORT_SIDE.has(e && e.station) ? "sorted" : "shipped"; };
  const LABEL_WORD = { sheetQR: "sheet QR", orderQR: "order QR", custom: "custom QR", shipping: "shipping" };
  // a scan only says where the order was seen: "Seen at Sorting", never a step
  const seenAt = e => "Seen at " + (placeOf(e) || "a station");
  // the milestone rail (Paul, 28 Sep 21:18; the server's `where.rail`/`where.step`): the real steps a piece goes through,
  // in process order. Each step: the events that stand for it (the stamp it shows, the event a click opens) and the lane
  // its dashed stamp waits on. `only`: a step only some pieces take ("stud": a stud earring; "engrave": a piece with a
  // back engraving); for any other piece it reads `none` and stagesFor leaves it out.
  // Approving (a sheet committed, a SKU decided) is not a step: what it needs is said under the step it blocks. Etsy's
  // completion folds into Shipped. Engraved: the piece's back engraving approved in Engrave, which writes it into its
  // sheet's back file for the laser (backPut records engraveApproved, who and when; older orders: Charm_Pool_Back).
  const STAGES = [
    { k: "arrived", l: "Order in", types: ["arrived"], kind: "arrived", lane: "etsy" },
    { k: "sheet", l: "Nested", types: ["placed", "renested"], kind: "placed", lane: "sheet" },
    { k: "engraved", l: "Engraved", types: ["engraveApproved"], kind: "engraveApproved", lane: kindOf("engraveApproved").lane, only: "engrave", none: "No engraving" },
    { k: "laser", l: "Laser cut", types: ["laserDone", "roseCut"], kind: "laserDone", lane: "sheet" },
    { k: "sorted", l: "Sorted", types: ["sorted"], kind: "sorted", lane: "sorting" },
    { k: "welded", l: "Welded", types: ["welded"], kind: "welded", lane: "welding", only: "stud", none: "No welding" },
    { k: "assembled", l: "Assembled", types: ["assembled"], kind: "assembled", lane: "assembly" },
    { k: "shipped", l: "Shipped", types: ["shipped", "etsyCompleted"], kind: "shipped", lane: "shipping" }
  ];
  /* Whether a piece is a stud earring, read as the sorter reads a line's product type: CharmNestOrders.purchaseDetails,
     the Style/Type option the buyer chose, else the listing's title ("Stud earrings", "Huggie hoop earrings", "Necklace",
     "Charm only · …"). Takes the sorter's row ({ line, spec }), an Etsy line ({ title, variations }), or anything that
     names its type (productType, type, kind, form). A huggie, a hoop or a charm only is never a stud. null: nothing
     about the piece is known yet (a line still being read from the records), so it is not ruled out. */
  const STUD = /\bstuds?\b/i, NOT_STUD = /\b(?:huggies?|hoops?|necklaces?|bracelets?|anklets?|key\s*(?:chains?|rings?))\b|\bcharms?\s+only\b/i;
  function studOf(x) {
    if (!x || typeof x !== "object") return null;
    const line = x.line && typeof x.line === "object" ? x.line : x.title != null || Array.isArray(x.variations) ? x : null;
    const spec = (x.spec && typeof x.spec === "object" && x.spec) || {}, O = root.CharmNestOrders;
    if (line && O && typeof O.purchaseDetails === "function") {
      try { const t = O.purchaseDetails(line, spec).type; if (t && t !== "Type not specified") return t === "Stud earrings"; } catch (_) {}
    }
    const said = [x.productType, x.type, x.kind, x.form, spec.form].filter(v => typeof v === "string" && v).join(" ");
    const text = (said + " " + String((line && line.title) || "") + " " + (line && Array.isArray(line.variations) ? line.variations.map(v => v && v.value).join(" ") : "")).trim();
    return text ? STUD.test(text) && !NOT_STUD.test(text) : null;
  }
  const isStud = x => studOf(x) === true;
  /* Whether a piece carries a back engraving, read as the sorter reads it (Paul, 28 Sep: a step a piece never takes is not
     shown). The line's Engrave state first (row.engrave): "none" (nothing to engrave, or a person said so) and "skipped"
     (cut plain) rule it out; words, ready, blocked or written mean it has one; classify/reclassify are still reading. Then
     its spec (CharmNestOrders.interpretLine): engraveCandidate false (no personalisation, buyer message, note, nor a Team
     message that says more than a workflow stamp like "DESIGNED :)" or "QA1", engravingNote) or noDesign (never cut)
     rule it out. Takes the sorter's row ({ spec, engrave }) or a piece's line carrying the same fields. null: not known
     yet (an Etsy line alone: its order's message may still ask for one), which keeps the step. */
  const ENGRAVES = new Set(["words", "ready", "blocked", "written"]);
  function engraveOf(x) {
    if (!x || typeof x !== "object") return null;
    const g = x.engrave && typeof x.engrave === "object" ? x.engrave : null;
    if (g && (g.state === "none" || g.state === "skipped")) return false;
    if (g && (ENGRAVES.has(g.state) || (g.needed === true && !g.state))) return true;
    const spec = x.spec && typeof x.spec === "object" ? x.spec : x;
    if (spec.noDesign === true || spec.engraveCandidate === false) return false;
    return null;
  }
  /** A piece's own steps: STAGES, with Welded only when the piece is a stud earring and Engraved only when it carries a
      back engraving. Every piece of an order (an array): the order's steps, a step shown when any piece takes it (or is
      not read yet). Nothing given: every step. */
  function stagesFor(line) {
    const list = (Array.isArray(line) ? line : [line]).filter(x => x && typeof x === "object");
    if (!list.length) return STAGES;
    const weld = list.some(x => studOf(x) !== false), engrave = list.some(x => engraveOf(x) !== false);
    return STAGES.filter(s => (s.only !== "stud" || weld) && (s.only !== "engrave" || engrave));
  }
  const ALL_STAGES = STAGES;   // (mount's own `STAGES` is the order's steps)
  const STOP_OF = {}; STAGES.forEach((s, i) => s.types.forEach(t => { STOP_OF[t] = i; }));
  /* a piece's (or order's) own steps, plus any step it was given all the same: a step with an event of its own is always
     drawn (a necklace that was welded, a plain piece whose back engraving was approved) */
  function keepDone(r, evs) {
    r = Array.isArray(r) && r.length ? ALL_STAGES.filter(s => r.some(x => x === s || (x && x.k === s.k))) : ALL_STAGES;
    if (r.length < ALL_STAGES.length && (evs || []).some(e => e && own(STOP_OF, e.type) && !r.includes(ALL_STAGES[STOP_OF[e.type]]))) r = ALL_STAGES.filter(s => r.includes(s) || (evs || []).some(e => e && s.types.includes(e.type)));
    return r.length ? r : ALL_STAGES;
  }
  // what a person would miss from a cut-short read: the rail's steps and what a person did (the sealed kinds)
  const MISSED = new Set(STAGES.flatMap(s => s.types).concat(["cancelled", "etsyCancelled", "cancelRestored", "removed", "cancelStep", "held", "released", "restored", "cancelAlert"]));

  /* where the order is now: a copy of whereOf in netlify/functions/_orderTimeline.js (keep the two alike). The server's
     `where` is used as it comes; this one counts the steps this page recorded that the server has not seen yet. */
  const RANK = {
    arrived: 0, pulled: 0, interpreted: 0, pooled: 0, decided: 0, skipped: 0, customDecided: 0, designSent: 0, designDropped: 0, released: 0, restored: 0,
    needsDecision: 1, engraveNeeded: 1, held: 1, customRead: 1,
    placed: 2, moved: 2, renested: 2, qrLabel: 2, roseLine: 2, included: 2, merged: 2, sizeChanged: 2, setCommitted: 2, sealPrinted: 2, sealCompleted: 2, recalled: 2,
    laserDone: 3, roseCut: 3, sorted: 4, welded: 5, assembled: 6, packed: 7, labelPrinted: 7, shipped: 8, etsyCompleted: 9
  };
  const STAGE_BY_RANK = ["waiting", "review", "sheet", "cut", "sorted", "welded", "assembled", "packed", "shipped", "completed"];
  const STAGE_LABEL = { waiting: "Waiting", review: "In review", held: "On hold", designed: "Design complete", sheet: "On a sheet", cut: "Cut on the laser", sorted: "Sorted", welded: "Welded", assembled: "Assembled", packed: "Packed", shipped: "Shipped", completed: "Completed on Etsy", cancelled: "Cancelled" };
  const PEOPLE_OUT = new Set(["", "system", "etsy", "operator", "someone"]);
  // the furthest step of STAGES (the full rail, 0-7) an event shows the order has reached
  const STEP = { arrived: 0, placed: 1, moved: 1, renested: 1, qrLabel: 1, roseLine: 1, included: 1, merged: 1, sizeChanged: 1, setCommitted: 1, sealCompleted: 1,
    engraveApproved: 2, laserDone: 3, roseCut: 3, sorted: 4, welded: 5, assembled: 6, packed: 6, labelPrinted: 6, shipped: 7, etsyCompleted: 7 };
  // (a label printed at Sorting or the Design Station is the Sorted step's detail: it moves the order on nowhere)
  const sortLabel = e => !!e && e.type === "labelPrinted" && labelStepOf(e) === "sorted";
  /** The furthest step of the full rail (0-7) one event shows the order reached; null: none. */
  const stepOf = e => (!e || sortLabel(e) || STEP[e.type] == null ? null : STEP[e.type]);
  const isDesigned = e => e.type === "note" && e.data && (e.data.stamp === "DESIGNED :)" || e.data.stamp === "designComplete");
  function whereOf(events, cancelled, hint) {
    hint = hint || {};
    const list = (events || []).filter(e => e && own(KIND, e.type)).slice().sort(byAt);
    let rank = 0, stage = "waiting", since = 0, sheet = "", sheetId = "", setId = "", station = "", device = "", by = "", at = 0, designed = false, cancel = null, step = list.length ? 0 : -1, seen = false;
    const enter = (st, e) => { if (st !== stage) since = +e.at || 0; stage = st; };
    for (const e of list) {
      at = Math.max(at, +e.at || 0);
      // a Complete Order press or a Reopen moves the order on no step (derive's `hand`: completed by hand)
      if (opStepOf(e)) { if (!PEOPLE_OUT.has(String(e.by || "").trim().toLowerCase())) by = e.by; continue; }
      if (stepOf(e) != null) step = Math.max(step, stepOf(e)); else if (isDesigned(e)) step = Math.max(step, 1);
      if (e.station && e.type !== "arrived") { station = e.station; device = e.device || ""; seen = e.type === "scan"; }
      if (!PEOPLE_OUT.has(String(e.by || "").trim().toLowerCase())) by = e.by;
      if (e.setId) setId = e.setId;
      if (CANCEL_TYPES.has(e.type)) { cancel = e; continue; }
      if (e.type === "cancelRestored") { cancel = null; continue; }
      if (isDesigned(e)) { designed = true; if (rank <= 2 && !sheetId) { rank = 2; enter("designed", e); } continue; }
      if (e.type === "setCommitted" || e.type === "sealCompleted") designed = true;
      if (e.type === "removed") {
        if (rank <= 2 && (!e.sheetId || !sheetId || e.sheetId === sheetId)) { rank = 0; sheet = ""; sheetId = ""; enter(/hold/i.test((e.data && e.data.reason) || e.text || "") ? "held" : "waiting", e); }
        continue;
      }
      const r = sortLabel(e) ? null : RANK[e.type]; if (r == null) continue;
      if (r >= 3) { if (r > rank) { rank = r; enter(STAGE_BY_RANK[r], e); } if (r === 3 && e.sheetId) { sheetId = e.sheetId; sheet = e.sheet || sheet; } continue; }
      if (rank > 2) continue;
      if (r === 2) { rank = 2; if (e.sheetId) { sheetId = e.sheetId; sheet = e.sheet || sheet; } enter(sheetId ? "sheet" : designed ? "designed" : "sheet", e); continue; }
      if (sheetId && e.type !== "held") continue;
      rank = 0; if (e.type === "held") { sheet = ""; sheetId = ""; }
      enter(r === 1 ? (e.type === "held" ? "held" : "review") : "waiting", e);
    }
    if (stage === "sheet" && !sheetId && designed) stage = "designed";
    const isCancelled = hint.record ? !!cancelled : !!(cancelled || cancel);
    if (isCancelled) {
      stage = "cancelled"; since = (cancel && +cancel.at) || (cancelled && +cancelled.at) || since;
      if (!cancel && cancelled && +cancelled.at >= at) { at = +cancelled.at; if (!PEOPLE_OUT.has(String(cancelled.by || "").trim().toLowerCase())) by = cancelled.by; }
    }
    const label = stage === "sheet" && sheet ? `On ${sheet}` : STAGE_LABEL[stage] || stage;
    const bits = [label];
    if (isCancelled && sheet) bits.push(`pieces on ${sheet}`);
    if (station && !["sheet", "waiting", "review", "held"].includes(stage)) bits.push(`${seen ? "seen at" : "at"} ${station}${device ? " (" + device + ")" : ""}`);
    if (by) bits.push(`by ${by}`);
    return { stage, label, text: bits.join(" · ").slice(0, 200), sheet, sheetId, setId, station, device, by, at, since, designed, cancelled: isCancelled, step, rail: STAGES.map(s => s.l) };
  }
  // (the All · Milestones · Stations · Sheets · Holds & cancels · Messages chips are gone: only milestones are drawn
  //  now, so there is nothing left to filter. The bar keeps one quiet "Stamps" chip — the legend of the seals.)
  const CANCEL_TYPES = new Set(["cancelled", "etsyCancelled"]);

  /* ── Complete Order and Reopen (Paul, 29 Sep 02:08: "The timeline also doesn't have the seals or the points for when an
     order was manually completed by pressing the completed button in the Review tab") ──
     Each press of Complete Order (a Review card, a Custom Orders card, the order window: charmNestLibrary customPut, a
     sealCompleted whose data.how is "button") is a point of its own on the Office lane ("Operator"), with the green
     scalloped "Order completed" seal the card is stamped with (Seal in charm-nest-motion.js: its ink, edge and check).
     A Reopen or an Undo (customReopen: a note with data.reopened) is a point of its own, with its own seal: it never
     takes the Complete point away (seals never disappear), and a later Complete adds another. The seal's rim says where
     it was pressed (data.pressedIn; an older press: the sorter), its middle who and when. norm() dresses the event for
     drawing and the host gets the record back as it came (pubOf). A completion by printing the label is the label's. */
  KIND.sealCompleted = K("green", "m", "check", "office");   // (the Office lane's, not the sheet's)
  KIND.reopened = K("velvet", "e", "undo", "office");
  function opStepOf(x) {
    if (!x) return "";
    if (x.type === "sealCompleted") return x.data && x.data.how === "print" ? "" : "complete";
    return x.type === "reopened" || (x.type === "note" && !!x.data && !!x.data.reopened) ? "reopen" : "";
  }
  /** A Complete press or a Reopen as it is drawn: its type, its words and where it was pressed; null for any other. */
  function opDress(x) {
    const k = opStepOf(x); if (!k) return null;
    const d = x.data && typeof x.data === "object" && !Array.isArray(x.data) ? x.data : {}, where = str(d.pressedIn, 40);
    const text = k === "complete" ? "Completed with Complete Order" : d.reopened === "undo" ? "Completion undone: back to Open" : "Reopened: back to Open";
    return { type: k === "reopen" ? "reopened" : x.type, text: text + (where ? " · " + where : ""), data: Object.assign({}, d, where ? { foot: where } : {}) };
  }
  // A press that completes a piece by hand: a Complete Order press, or the QR label printed from Custom Orders (the server's sealPrinted: customPut writes the
  // record's state 'completed' for either button, so either one, or both in any order with any number of reprints, releases the piece; Paul, 5 Oct)
  const handStepOf = x => (x && x.type === "sealPrinted" ? "complete" : opStepOf(x));
  /** Completed by hand (Paul, 29 Sep: a Complete Order press reads as completed everywhere; 5 Oct: and so does a Print QR label press): the latest press
   *  when every line pressed has no Reopen after its last press, else null. The steps it had not reached are skipped, not "next";
   *  a Reopen puts the order back where it was (its seals stay), a later press of either button completes it again. */
  function handOf(events) {
    const last = new Map();
    for (const e of events || []) { const k = handStepOf(e); if (k) last.set(e.lineKey || "", k === "complete" ? e : null); }
    const v = [...last.values()];
    return v.length && v.every(Boolean) ? v.reduce((a, b) => (+b.at >= +a.at ? b : a)) : null;
  }
  /** Every press that completes a piece by hand and still stands (no Reopen of its line after it): the pieces' own completions. A Set of the
   *  very events given. While another piece of the order is not done, none of them is the ORDER's event (handDoneOf). */
  function handLive(events) {
    const by = new Map();
    for (const e of events || []) { const k = handStepOf(e); if (!k) continue; const l = e.lineKey || ""; if (k === "complete") { if (!by.has(l)) by.set(l, []); by.get(l).push(e); } else by.delete(l); }
    return new Set([...by.values()].flat());
  }
  /** The event that COMPLETED the order by hand (what the "Where it is now" card says and stamps; Paul, 5 Oct: the line said 12:41 PM and the seal
   *  10:57 AM): null unless handOf says every piece pressed stands completed; else the latest of the presses that completed each piece (the first press
   *  after its last Reopen: a QR label printed again later is a reprint, never the completion, as the record's completedAt keeps it). One event: the card's
   *  line takes its time and person, its seal (handSealOf) the same. */
  function handDoneOf(events) {
    if (!handOf(events)) return null;
    const first = new Map();
    for (const e of events || []) { const k = handStepOf(e); if (!k) continue; const l = e.lineKey || ""; if (k === "complete") { if (!first.has(l)) first.set(l, e); } else first.delete(l); }
    const v = [...first.values()];
    return v.length ? v.reduce((a, b) => (+b.at >= +a.at ? b : a)) : null;
  }
  /** The order's ORDER COMPLETE seal for the press that completed it (handDoneOf): a Complete Order press is its own seal as it is; a QR label that
   *  completed the order is the order's completion at that same moment, by that same person (the label's own QR seal stays on its piece and the Timeline). */
  const handSealOf = h => (h && h.type === "sealPrinted" ? Object.assign({}, h, { type: "sealCompleted", print: null, lane: "office", milestone: true, data: Object.assign({}, h.data, { how: "order" }) }) : h || null);

  const pt = (r, a) => [60 + r * Math.cos(a * Math.PI / 180), 60 + r * Math.sin(a * Math.PI / 180)];
  const arc = (r, a0, a1, sw) => { const [x0, y0] = pt(r, a0), [x1, y1] = pt(r, a1), span = sw ? (a1 - a0 + 360) % 360 : (a0 - a1 + 360) % 360; return `M${x0.toFixed(2)} ${y0.toFixed(2)}A${r} ${r} 0 ${span > 180 ? 1 : 0} ${sw} ${x1.toFixed(2)} ${y1.toFixed(2)}`; };
  function scallops(n, rv, rc, sw) { let d = ""; const step = 360 / n; for (let i = 0; i < n; i++) { const a0 = i * step, [x0, y0] = pt(rv, a0), [xm, ym] = pt(rc, a0 + step / 2), [x1, y1] = pt(rv, a0 + step); d += (i ? "" : `M${x0.toFixed(2)} ${y0.toFixed(2)}`) + `Q${xm.toFixed(2)} ${ym.toFixed(2)} ${x1.toFixed(2)} ${y1.toFixed(2)}`; } return `<path d="${d}Z" stroke-width="${sw || 2.4}"/>`; }
  function star(a, r, s) { const [cx, cy] = pt(r, a); let d = ""; for (let i = 0; i < 10; i++) { const rr = i % 2 ? s * .42 : s, t = (i * 36 - 90) * Math.PI / 180; d += (i ? "L" : "M") + (cx + rr * Math.cos(t)).toFixed(2) + " " + (cy + rr * Math.sin(t)).toFixed(2); } return `<path d="${d}Z" stroke="none"/>`; }
  const edgeOf = sh => sh === "m" ? scallops(30, 54.2, 58.4) + `<circle cx="60" cy="60" r="51.6" stroke-width="1"/>`
    : sh === "a" ? `<circle cx="60" cy="60" r="55.4" stroke-width="4.2" stroke-dasharray="9 3.2"/><circle cx="60" cy="60" r="50.6" stroke-width="1"/>`
    : `<circle cx="60" cy="60" r="55.6" stroke-width="3.2"/><circle cx="60" cy="60" r="52" stroke-width=".9"/>`;
  let UID = 0;
  const MON = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], DAYN = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
  // one formatter each, made once: toLocale*String builds a new one per call (~0.3 ms), and a redraw formats every stamp
  const fmtOf = o => { let f = null; return d => { try { return (f = f || new Intl.DateTimeFormat("en-US", o)).format(d); } catch (_) { return ""; } }; };
  // The same recorded achievement must show the same shop-local date/time on cards and the timeline.
  const SEAL_ZONE = "America/Toronto", DATE_PARTS = new Intl.DateTimeFormat("en-US", { timeZone: SEAL_ZONE, day: "2-digit", month: "short", year: "numeric" });
  const dateOf = t => { const parts = DATE_PARTS.formatToParts(new Date(+t || Date.now())), get = type => parts.find(p => p.type === type).value; return `${get("day")} ${get("month").toUpperCase()} ${get("year")}`; };
  const TIME = fmtOf({ timeZone: SEAL_ZONE, hour: "numeric", minute: "2-digit" }), LONG = fmtOf({ timeZone: SEAL_ZONE, weekday: "long", month: "short", day: "numeric", year: "numeric" }), SHORT_DAY = fmtOf({ timeZone: SEAL_ZONE, weekday: "short" });
  const timeOf = t => TIME(new Date(+t || Date.now()));
  const longWhen = t => LONG(new Date(+t)) + " · " + timeOf(t);
  const shortWhen = t => `${SHORT_DAY(new Date(+t)).toUpperCase()} ${timeOf(t)}`;
  /** "Mon 12:41 PM" in the shop's zone, the one every seal says its time in (a line beside a seal never reads another zone: Paul, 5 Oct 2026). */
  const whenOf = t => `${SHORT_DAY(new Date(+t))} ${timeOf(t)}`;
  function ago(t) {
    const s = (Date.now() - t) / 1000; if (!(t > 0)) return ""; if (s < 45) return "just now";
    const m = s / 60; if (m < 60) return Math.round(m) + " min ago";
    const h = m / 60; if (h < 24) return Math.round(h) + " h ago";
    const d = h / 24; return d < 2 ? "yesterday" : d < 14 ? Math.round(d) + " days ago" : new Date(t).toLocaleDateString("en-US", { month: "short", day: "numeric" });
  }
  const rotOf = e => { const h = hash(String(e.key || e.id || "") + e.type); return kindOf(e.type).sh === "m" ? 4 + (h % 8) : -(4 + (h % 9)); };
  // One size per group: the kind changes the silhouette, never the space occupied by its seal.
  const sealAttrs = e => `data-tl-face="${esc(JSON.stringify({ key: e.key, type: e.type, at: e.at, by: e.by, source: e.source, station: e.station, lane: e.lane, sheet: e.sheet, data: e.data, print: e.print }))}"`;
  function storedFace(b) { try { return b && b.dataset.tlFace ? JSON.parse(b.dataset.tlFace) : null; } catch (_) { return null; } }
  // (read once per host and again after a resize: a computed-style read in the middle of a redraw makes the page work out every style
  //  the redraw has just changed, twice a step, and a live step over a hundred events then no longer fits a frame)
  const sealBase = new WeakMap(); let sealBaseGen = 0;
  try { root.addEventListener("resize", () => { sealBaseGen++; }); } catch (_) { /* no window to resize */ }
  function baseSealSize(host) {
    const hit = sealBase.get(host); if (hit && hit.gen === sealBaseGen) return hit.size;
    let size = SEAL_SIZE; try { size = Math.max(1, parseFloat(root.getComputedStyle(host).getPropertyValue("--seal-size")) || SEAL_SIZE); } catch (_) { /* the default */ }
    sealBase.set(host, { gen: sealBaseGen, size }); return size;
  }
  // m: what measure() read before the redraw wrote anything (without it, read now)
  function fitRail(rail, m) {
    if (!rail) return;
    const wrap = rail.querySelector(".tlStops"), n = wrap && wrap.children.length; if (!n) return;
    const base = baseSealSize(rail), width = m ? m.stops : wrap.clientWidth || rail.clientWidth, compactHost = rail.closest(".tlUI.compact"), hostH = m ? m.host : compactHost ? compactHost.clientHeight : 0;
    let fit = width > 0 ? Math.min(base, Math.max(24, width / n - 8)) : base;
    // A compact header is already sized by its host. Its seals fit that space without making a taller toolbar.
    if (compactHost && hostH) fit = Math.min(fit, Math.max(TL_RAIL_SEAL - 2, hostH - (44 - TL_RAIL_SEAL)));
    rail.style.setProperty("--seal-fit", Math.round(fit * 100) / 100 + "px");
  }
  function iconG(ic, x, y, s, ink, sw) {
    const green = ic === "greenline";
    return `<g transform="translate(${x - 12 * s} ${y - 12 * s}) scale(${s})" fill="none" stroke="${green ? "#2f7d3a" : ink}" stroke-width="${green ? 3 : sw || 2.2}" stroke-linecap="round" stroke-linejoin="round"><path d="${ICON[ic] || ICON.dot}"/>${green ? `<circle cx="3" cy="17" r="1.8" fill="#2f7d3a"/><circle cx="21" cy="7" r="1.8" fill="#2f7d3a"/>` : ""}</g>`;
  }
  const iconSvg = ic => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="${ICON[ic] || ICON.dot}"/></svg>`;
  // the ink texture of the seals: a slightly rough edge and a few pinholes where the stamp did not take
  const texOf = (id, seed) => `<filter id="${id}f" x="-6%" y="-6%" width="112%" height="112%" color-interpolation-filters="sRGB"><feTurbulence type="fractalNoise" baseFrequency=".05" numOctaves="2" seed="${seed}" result="lo"/><feDisplacementMap in="SourceGraphic" in2="lo" scale="1.8" xChannelSelector="R" yChannelSelector="G" result="rough"/><feTurbulence type="fractalNoise" baseFrequency=".95" numOctaves="1" seed="${seed + 11}" result="hi"/><feColorMatrix in="hi" type="matrix" values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 3.6 0 0 0 -.72" result="holes"/><feComposite in="rough" in2="holes" operator="in"/></filter>`;
  function footOf(e) {
    const d = e.data || {}, L = LANE[e.lane] || LANE.office;
    if (d.foot) return String(d.foot);
    if (e.type === "sealPrinted" && d.n) return "PRINT Nº " + d.n;
    // where it happened (Paul, 28 Sep 23:51): the station, with the sheet when it names one ("Laser · 14K Sheet 4")
    const at = placeOf(e);
    if (at && e.sheet) return `${at} · ${e.sheet}`;
    if (e.sheet) return e.sheet;
    if (at) return /station$/i.test(at) || !(LANE[STATION_LANE[e.station]] || {}).st ? at : at + " station";
    return L.st ? L.l + " station" : L.k === "etsy" ? "From Etsy" : L.l;
  }
  // Eight visual families share the same action/date/time layout. Event wording stays truthful within its family.
  const FAMILY_BY_TYPE = {
    arrived: "received", pulled: "received", interpreted: "received", decided: "received", customRead: "received", customDecided: "received",
    designSent: "prepared", designDropped: "prepared", pooled: "prepared", placed: "prepared", moved: "prepared", renested: "prepared",
    sizeChanged: "prepared", included: "prepared", excluded: "prepared", merged: "prepared", qrLabel: "prepared", setCommitted: "prepared", sealPrinted: "prepared", roseLine: "prepared",
    engraveNeeded: "engraving", engraveApproved: "engraving", engraveChanged: "engraving",
    laserDone: "laser", roseCut: "laser", laserReady: "laser", sheetCompleted: "laser", setCompleted: "laser",
    scan: "finishing", sorted: "finishing", welded: "finishing", assembled: "finishing",
    packed: "fulfilment", labelPrinted: "fulfilment", shipped: "fulfilment", etsyCompleted: "fulfilment", sealCompleted: "fulfilment",
    cancelled: "cancelled", etsyCancelled: "cancelled", cancelStep: "cancelled", cancelAlert: "cancelled"
  };
  const FACE_ACTION = { arrived: "ORDER RECEIVED", placed: "ON SHEET", designSent: "SENT TO SHEET", engraveApproved: "BACK ENGRAVING", laserDone: "LASER CUT", roseCut: "PARTIAL CUT", laserReady: "LASER READY", sheetCompleted: "SHEET COMPLETE", setCompleted: "SET COMPLETE", sealCompleted: "ORDER COMPLETE", etsyCompleted: "ETSY COMPLETE", cancelled: "CANCELLED", etsyCancelled: "ETSY CANCELLED", sealPrinted: "QR LABEL PRINTED" };
  const CANONICAL_ICON = { arrived: "received", placed: "prepared", designSent: "prepared", engraveApproved: "engraving", laserDone: "laser", assembled: "finishing", shipped: "fulfilment", held: "exceptions", cancelled: "cancelled" };
  function faceModel(e) {
    const printed = !!e.print || isPrint(e), plain = isPlainDecision(e), at = +(plain ? e.data.decidedAt ?? e.at : e.at) || 0, family = printed ? "prepared" : FAMILY_BY_TYPE[e.type] || "exceptions";
    const action = plain ? "CUT PLAIN" : printed ? "QR LABEL PRINTED" : FACE_ACTION[e.type] || String((e.data && e.data.ring) || labelOf(e.type)).toUpperCase();
    return { family, action, icon: plain ? "plain" : CANONICAL_ICON[e.type], path: plain || CANONICAL_ICON[e.type] ? undefined : ICON[printed ? "qr" : kindOf(e.type).ic] || ICON.dot, date: at ? dateOf(at) : "", time: at ? timeOf(at) : "", by: e.ghost || (!at && !String(e.by || "").trim()) ? "" : personOf(e), at };
  }
  /** One stamp as SVG. full: the face that reads (ring words, icon, date, time, name); otherwise edge + big icon.
   *  opts.ghost draws a step still to come (dashed, no ink); opts.tex:false leaves out the ink texture; opts.uid names
   *  its inner ids (the same stamp then draws the same markup; unique in the page, as the ids are). */
  function stampSvg(e, full, opts) {
    opts = opts || {};
    if (root.Seal && typeof root.Seal.face === "function") {
      try { return root.Seal.face(faceModel(e), { signer: !!opts.hover, ghost: !!opts.ghost }); } catch (err) { warn("seal face", err); }
    }
    if (e && e.print) return printSvg(e, full, opts);
    opts = opts || {};
    const Kd = kindOf(e.type), ink = INK[Kd.ink], id = opts.uid ? String(opts.uid).replace(/[^\w-]/g, "_") : "tls" + (++UID), seed = (hash(String(e.key || e.id || e.type) + e.at) % 997) + 1;
    const useTex = opts.tex !== false && !opts.ghost, tex = useTex ? texOf(id, seed) : "", g = useTex ? ` filter="url(#${id}f)"` : "";
    if (!full) {
      const wash = opts.ghost ? "none" : Kd.sh === "m" ? ink + "24" : ink + "12";
      const edge = Kd.sh === "m" ? scallops(26, 52, 58, 6) : Kd.sh === "a" ? `<circle cx="60" cy="60" r="54" stroke-width="9" stroke-dasharray="14 6"/>` : `<circle cx="60" cy="60" r="54" stroke-width="7"/><circle cx="60" cy="60" r="43" stroke-width="2.4" fill="none"/>`;
      return `<svg viewBox="0 0 120 120" aria-hidden="true" focusable="false"><defs>${tex}</defs><g${g} fill="${ink}" stroke="${ink}"><g fill="rgba(255,254,251,.08)"${opts.ghost ? ' stroke-dasharray="8 7"' : ""}>${edge}</g><circle cx="60" cy="60" r="${Kd.sh === "e" ? 41 : 48}" fill="${wash}" stroke="none"/>${iconG(Kd.ic, 60, 60, 2.1, ink, 2.8)}</g></svg>`;
    }
    const top = String((e.data && e.data.ring) || labelOf(e.type)).toUpperCase(), foot = footOf(e).toUpperCase().slice(0, 26);
    let nm = whoOf(e).toUpperCase().replace(/\s+/g, " ");
    if (nm.length > 13) { const w = nm.split(" "); nm = w.length > 1 ? `${w[0]} ${w[w.length - 1][0]}.` : nm; }
    if (nm.length > 14) nm = nm.slice(0, 13) + "…";
    const nfs = Math.min(9.4, (64 / Math.max(1, nm.length) - .5) / .68), tfs = top.length > 16 ? 7.4 : 8.6, ffs = foot.length > 18 ? 5.8 : 6.6;
    const sans = `font-family="-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif"`, mono = `font-family="ui-monospace,Menlo,Consolas,monospace"`;
    return `<svg viewBox="0 0 120 120" aria-hidden="true" focusable="false"><defs>${tex}<path id="${id}t" d="${arc(44.2, 158, 22, 1)}"/><path id="${id}b" d="${arc(50.6, 143, 37, 0)}"/></defs>` +
      `<g${g} fill="${ink}" stroke="${ink}"><g fill="rgba(255,254,251,.08)">${edgeOf(Kd.sh)}</g><circle cx="60" cy="60" r="41.4" stroke-width="1.3" fill="none"/>` +
      `<text stroke="none" ${sans} font-size="${tfs}" font-weight="800" letter-spacing="1.2"><textPath href="#${id}t" startOffset="50%" text-anchor="middle">${esc(top)}</textPath></text>` +
      `<text stroke="none" ${sans} font-size="${ffs}" font-weight="800" letter-spacing="1.05"><textPath href="#${id}b" startOffset="50%" text-anchor="middle">${esc(foot)}</textPath></text>` +
      star(150, 47.4, 2.6) + star(30, 47.4, 2.6) + iconG(Kd.ic, 60, 31.5, .72, ink) +
      `<path d="M25 44.5h70M22 72.5h76" stroke-width="1" fill="none"/>` +
      `<text x="60" y="56.4" text-anchor="middle" stroke="none" ${mono} font-size="10.4" font-weight="800">${esc(dateOf(e.at))}</text>` +
      `<text x="60" y="68.2" text-anchor="middle" stroke="none" ${mono} font-size="9.8" font-weight="700">${esc(timeOf(e.at))}</text>` +
      `<text x="60" y="84" text-anchor="middle" stroke="none" ${sans} font-size="${Math.max(7.4, nfs).toFixed(2)}" font-weight="800" letter-spacing=".5">${esc(nm)}</text></g></svg>`;
  }
  /** The cancellation seal uses the same regular footprint as every other event, with its warning in the record. */
  function cancellationFace(c) {
    return { key: "cancel-" + c.at, type: c.source === "etsy" || /^etsy$/i.test(c.by || "") ? "etsyCancelled" : "cancelled", at: +c.at || 0, by: c.by || "", source: c.source || "", lane: "office", data: { ring: "CANCELLED ORDER", foot: "DO NOT PROCEED" } };
  }
  function cancelSvg(c) { return stampSvg(cancellationFace(c), true, { uid: "tlx" + (++UID) }); }

  /* ════ one event, as the timeline reads it ════ */
  function norm(x) {
    if (!x || typeof x !== "object" || !x.type) return null;
    const was = x, op = opDress(x); if (op) x = Object.assign({}, x, op);   // (a Complete press or a Reopen, drawn)
    const type = String(x.type), at = Number(x.at) || 0, rawId = String(x.id || `${at}-${type}`);
    const e = {
      // the server keeps `${orderId}~${type}~${key}`; the page's own record (live, or still in the outbox) is `key`
      key: type + "~" + rawId.split("~").pop().replace(/[^\w.:-]/g, "_"), id: rawId, type, at,
      by: str(x.by, 80), source: str(x.source, 20), station: str(x.station, 20), device: str(x.device, 40),
      lineKey: str(x.lineKey, 80), transactionId: str(x.transactionId, 30), sheetId: str(x.sheetId, 100), sheet: str(x.sheet, 80), setId: str(x.setId, 100),
      text: str(x.text, 200), data: x.data && typeof x.data === "object" && !Array.isArray(x.data) ? x.data : null,
      milestone: x.milestone != null ? !!x.milestone : !!(typeInfo(type) || {}).milestone, pending: !!x.pending, derived: !!x.derived
    };
    e.lane = laneOf(e);
    if (op) e.orig = Object.fromEntries(["type", "text", "data"].filter(k => was[k] != null).map(k => [k, was[k]]));
    return e;
  }
  // the same event again (norm of the same record): nothing in what it says has changed
  const FACTS = ["key", "id", "type", "at", "by", "source", "station", "device", "lineKey", "transactionId", "sheetId", "sheet", "setId", "text", "milestone", "pending", "derived"];
  const sameFacts = (a, b) => FACTS.every(k => a[k] === b[k]) && (a.data === b.data || JSON.stringify([a.data, a.orig]) === JSON.stringify([b.data, b.orig]));
  // an event as the host gets it (onOpen, onEvents, onNow): the record's own fields, without the drawing's
  const PUB = ["id", "key", "type", "at", "by", "source", "station", "device", "lineKey", "transactionId", "sheetId", "sheet", "setId", "text", "data", "milestone", "pending", "derived"];
  const pubOf = (e, orderId) => { const o = { orderId }; for (const k of PUB) if (e[k] != null && e[k] !== "" && e[k] !== false) o[k] = e[k]; return e.orig ? Object.assign(o, e.orig) : o; };
  const warn = (what, err) => { try { console.warn("[OrderTimelineUI] " + what + ":", err); } catch (_) {} };
  // oldest first, as the server's byTime: at the same moment the order's arrival leads (a step the server drew at the
  // arrival, from before it, keeps its own order after it: data.recordedAt)
  const recAt = e => (e.data && +e.data.recordedAt) || e.at;
  const byAt = (a, b) => a.at - b.at || (a.type === "arrived" ? -1 : b.type === "arrived" ? 1 : 0) || recAt(a) - recAt(b) || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0);
  const reasonOf = e => { const d = e.data || {}; return str(d.reason || d.why || d.removedReason || d.cancelReason || "", 400); };
  const poolOf = e => { const d = e.data || {}; return String(d.poolId || (Array.isArray(d.poolIds) && d.poolIds[0]) || (e.lineKey ? `${e.lineKey}_${d.copy || 1}` : "")); };
  const titleOf = (e, max) => {
    if (e.print) return printTitle(e);
    if (e.type === "scan") return seenAt(e);
    const t = e.text && e.text.length <= (max || 90) ? e.text : "";
    return t || labelOf(e.type) + (e.sheet ? " — " + e.sheet : "");
  };
  const hay = e => [e.text, e.sheet, e.sheetId, e.setId, e.lineKey, e.transactionId, e.by, e.device, (() => { try { return JSON.stringify(e.data || ""); } catch (_) { return ""; } })()].join(" ").toLowerCase();

  /** Where an order stands, for the rail and the Now line: its events (oldest first), its cancel record and the
   *  server's `where` (worked out here when there is none). Pure: the order view may use it too.
   *  rail: the order's (or a piece's) own steps, stagesFor(lines); every step when left out.
   *  → { W, rail, stages[{first,last}], step, cur, stop, cancelled, hold, hand, last } — all indexes into `rail`. step:
   *  the furthest step reached (W.step counts the full rail: a step this order does not take is passed over), cur: the
   *  step being worked towards (-1 when all are done, or it was completed by hand), stop: where a cancelled order
   *  stopped, hand: the Complete Order press that completed it by hand (handOf; the steps after `step` are skipped). */
  const PLACE_STATES = new Set(["sheet", "waiting", "hold", "hand", "cancelled", "loading"]);
  function derive(events, cancelRec, where, rail, place) {
    events = events || [];
    rail = Array.isArray(rail) && rail.length ? rail : STAGES;
    let W = where && typeof where === "object" ? where : Object.assign(whereOf(events, cancelRec), typeof where === "string" && where ? { label: where, text: where } : {});
    const stages = rail.map(() => ({ first: null, last: null }));
    let lastHold = null, lastCancel = null;
    for (const e of events) {
      const i = own(STOP_OF, e.type) ? rail.indexOf(STAGES[STOP_OF[e.type]]) : -1;
      if (i >= 0) { const st = stages[i]; if (!st.first) st.first = e; st.last = e; }
      if (e.type === "held" || e.type === "released" || e.type === "restored") lastHold = e;
      if (CANCEL_TYPES.has(e.type) || e.type === "cancelRestored") lastCancel = e;
    }
    let full = clamp(Number.isFinite(+W.step) ? Math.round(+W.step) : -1, -1, STAGES.length - 1);
    let cancelled = null;
    if (cancelRec && typeof cancelRec === "object") cancelled = { at: +cancelRec.at || (lastCancel && lastCancel.at) || 0, by: str(cancelRec.by, 80), why: str(cancelRec.why, 400), source: cancelRec.source || (cancelRec.by === "Etsy" ? "etsy" : "sorter") };
    else if (lastCancel && lastCancel.type !== "cancelRestored" && W.cancelled !== false) cancelled = { at: lastCancel.at, by: whoOf(lastCancel), why: reasonOf(lastCancel) || lastCancel.text, source: lastCancel.type === "etsyCancelled" ? "etsy" : lastCancel.source };
    else if (W.cancelled) cancelled = { at: +W.since || +W.at || 0, by: str(W.by, 80), why: "", source: "" };
    let hold = !cancelled && W.stage === "held" ? (lastHold && lastHold.type === "held" ? lastHold : { type: "held", at: +W.since || 0, text: "", data: null }) : null;
    const hand = cancelled ? null : handOf(events);
    /* Where the piece IS now (the page's PiecePlacement, opts.placement): history is what happened, and `step` is its high-water mark: no event lowers it, so
       a piece placed once and taken off since (a hold, a remove, its sheet deleted, its set undone) kept Nested and a step after it: "On hold" under dots that
       said "Nested, next: Laser cut" (Paul, 5 Oct 2026, image 3). With a placement the steps follow the CURRENT state: a piece on no sheet (place.fence) has
       Nested and every later step hollow, unless the history already shows it cut (a piece cannot un-cut: `beyond`); a piece on a sheet has Nested done at least
       (`raised`: the sheet's record may be a moment ahead of the timeline). W (the words "On a sheet" / "Waiting") and the hold follow it too. The events, and
       every seal on the Timeline, are untouched: this only decides what is DONE, never what happened. */
    const pl = place && typeof place === "object" && PLACE_STATES.has(place.state) ? place : null;
    let fenced = false, raised = false, beyond = false;
    if (pl && !cancelled && !hand && pl.state !== "cancelled" && pl.state !== "hand" && pl.state !== "loading") {
      if (pl.fence && full >= 1) { if (full < 3) { full = 0; fenced = true; } else beyond = true; }
      else if (pl.floor >= 1 && full < 1) { full = 1; raised = true; }
      const st = W.stage, said = (stage, label) => ({ stage, label, text: label });
      if (pl.state === "hold") {
        hold = (lastHold && lastHold.type === "held" ? lastHold : null) || { type: "held", at: +pl.since || +W.since || 0, text: pl.reason || "", data: null };
        if (!beyond && st !== "held") W = Object.assign({}, W, said("held", "On hold"), pl.onSheet ? {} : { sheet: "", sheetId: "" });
      } else if (pl.state === "waiting" || pl.state === "sheet") {
        hold = null;
        if (pl.fence && !beyond && (st === "sheet" || st === "held")) W = Object.assign({}, W, said("waiting", pl.text || "Waiting for a sheet"), { sheet: "", sheetId: "" });
        else if (pl.state === "sheet" && pl.sheets && pl.sheets.length) {
          const a = pl.sheets[0], names = pl.sheets.map(x => x.label).filter(Boolean).join(" + ") || "a sheet", mine = W.sheetId && pl.sheets.some(x => x.id === W.sheetId);
          if (st === "waiting" || st === "review" || st === "designed" || st === "held") W = Object.assign({}, W, said("sheet", "On " + names), { sheet: a.label || "", sheetId: a.id || "" });
          else if (st === "sheet" && !mine && a.id) W = Object.assign({}, W, { sheet: a.label || W.sheet, sheetId: a.id });
        }
      }
    }
    const step = full < 0 ? -1 : rail.filter(s => STAGES.indexOf(s) <= full).length - 1;
    const cur = step + 1 < rail.length ? step + 1 : -1;
    return { W, rail, stages, step, cur: hand ? -1 : cur, stop: cancelled ? Math.min(step + 1, rail.length - 1) : -1, cancelled, hold, hand, last: events[events.length - 1] || null, place: pl, fenced, raised, beyond };
  }
  /** Whether step i (an index into STAGES) counts as done for a derived piece or order: reached, or stamped, and (placement) not a step after Nested that a
   *  piece taken off its sheet no longer stands on. ONE rule for the row's dots, the rail's counts and the Timeline (never `D.step` or `first` alone). */
  const stepDone = (D, i) => !!D && (D.step >= i || (!D.fenced && !!(D.stages && D.stages[i] && D.stages[i].first)));

  /* ════ orders of several pieces (Paul, 28 Sep: "very convoluted and confusing especially on multipiece orders") ════
     A piece is one line of the order: { key (its lineKey), tid, qty, line, pools [poolIds], sheets [sheetIds] }. An event
     is a piece's when it names it (its line, its transaction, one of its pool pieces); one that names no line belongs to
     the whole order (arrived, a cancel, a note), or, when it names a sheet, to the pieces on that sheet. */
  function ofPiece(e, p, pieces) {
    if (!e || !p) return false;
    if (e.lineKey) return e.lineKey === p.key;
    if (e.transactionId) return !p.tid || e.transactionId === String(p.tid);
    const d = e.data || {}, ids = [].concat(d.poolId || [], Array.isArray(d.poolIds) ? d.poolIds : []).map(String);
    if (ids.length) return ids.some(id => (p.pools || []).includes(id) || id.startsWith(p.key + "_"));
    if (e.sheetId && (pieces || []).some(q => (q.sheets || []).length)) return (p.sheets || []).includes(e.sheetId);
    return true;
  }
  /** The order across its pieces: each piece where it stands (derive over its own events, its own steps), each rail step
   *  with how many pieces reached it (a line of 2 counts 2) out of those that take it, and the step of the slowest piece,
   *  which is where the order is. → { each: [{ p, D, steps, events }], rail: [{ s, i, n, of }], step } */
  function summary(events, pieces, cancelRec, placeOf) {
    const evs = (events || []).map(x => (x && x.lane ? x : norm(x))).filter(Boolean).sort(byAt), ps = pieces || [];
    // (placeOf(key), or a piece's own .place: where the piece IS now, the PiecePlacement; see derive)
    const placed = p => { try { return typeof placeOf === "function" ? placeOf(p.key) || null : p.place || null; } catch (_) { return null; } };
    const each = ps.map(p => { const list = evs.filter(e => ofPiece(e, p, ps)); return { p, D: derive(list, cancelRec, null, null, placed(p)), steps: keepDone(stagesFor(p.line), list), events: list }; });
    const rail = STAGES.map((s, i) => ({ s, i, n: 0, of: 0 }));
    for (const x of each) for (const s of x.steps) {
      const i = STAGES.indexOf(s); if (i < 0) continue;
      const q = Math.max(1, Math.round(+x.p.qty || 1)); rail[i].of += q;
      if (stepDone(x.D, i) || x.D.hand) rail[i].n += q;
    }
    // (a piece completed by hand waits on no step: the order is where its slowest other piece is)
    return { each, rail: rail.filter(r => r.of), step: each.length ? Math.min(...each.map(x => (x.D.hand ? STAGES.length - 1 : x.D.step))) : -1 };
  }
  /** The rail a view draws (list: its steps, oldest first), with the step being worked towards and, when cancelled,
   *  where it stopped, both on that rail. */
  function railed(D, list, sum) {
    D.rail = (list && list.length ? list : STAGES).map(s => ({ s, i: STAGES.indexOf(s) })).filter(r => r.i >= 0);
    const nx = D.rail.find(r => r.i > D.step);
    D.cur = nx && !D.hand ? nx.i : -1;
    D.stop = D.cancelled ? (nx ? nx.i : D.rail[D.rail.length - 1].i) : -1;
    D.sum = sum || null;
    return D;
  }

  /* ════ what each step needs (Paul, 28 Sep 21:18, point 5): every step, done or to come, says what is there and what is
     still missing to move on, in the shop's words. requirementsOf(step, data) is pure: it reads the order's events and
     `where` (this component's), and what the host already holds of the order (opts.context(): its lines' states, holds,
     waits and engraving, and the readiness of the sheets they sit on). No Etsy calls, no server calls.
       step  a STAGES entry, its key ("laser") or its index      data { events, where, cancelled, D?, context?, stages? }
       →     { k, label, i, n, of, state: done|now|later|stopped|gone|none, done:[{t,sub}], need:[{kind:wait|person|next|after|stop, t}], facts:[t] }
     The steps are the order's (or a piece's) own rail: D.rail when D is given (the component's), else stagesFor's list in
     data.stages (keepDone: a step with an event of its own stays). i is the step's place in STAGES; n and of count it on
     that rail. A step the rail leaves out (Welded for a necklace, Engraved for a plain piece) is "none": nothing to do.
     How the sorter moves an order on: the Gate makes a line up onto a sheet (fast metals once a sheet's worth waits,
     sooner for a piece due within two days; slow metals every few days); a person answers Review (an unknown SKU, a
     custom order, a hold); the engraving is read, fitted and approved, and its back file saved; the set is committed
     once every sheet in it is ready (layout checked, front file, approvals, backs, QR labels: CharmNestReadiness); the
     laser marks the sheet cut; then each station scans the order's QR label as it sorts, welds, assembles and ships. */
  const QUIET = {   // the in-between events a step lists as quiet facts (never sealed)
    arrived: ["pulled", "interpreted", "needsDecision", "decided", "customRead", "customDecided", "skipped"],
    sheet: ["pooled", "moved", "renested", "included", "merged", "sizeChanged", "designSent", "designDropped"],
    engraved: ["engraveNeeded", "engraveChanged", "engraveApproved"],
    laser: ["qrLabel", "setCommitted", "sealPrinted", "sealCompleted", "recalled", "roseLine"],
    sorted: ["cancelAlert"], welded: [], assembled: [], shipped: ["packed", "etsyCompleted"]
  };
  // a scan is a quiet fact of its own station's step ("Seen at Welding" under Welded), never of every station's
  const SCAN_STEP = { sorting: "sorted", qr: "sorted", welding: "welded", assembly: "assembled", shipping: "shipped" };
  const HOW = {     // how a step gets done, when nothing more precise is known
    arrived: "Etsy's order record is read every few minutes",
    sheet: "the sorter nests it on a sheet at its next pass",
    engraved: "its back engraving is read, fitted and approved in Review, then its back file is saved",
    laser: "its set is committed and the sheet is cut on the laser",
    sorted: "scan its QR label at the Sorting station",
    welded: "weld the studs, then scan at the Welding station",
    assembled: "assemble it, then scan at the Assembly station",
    shipped: "pack it, print the label and scan at the Shipping station",
    approved: "its sheet's set is committed", completed: "Etsy marks the order complete"
  };
  const metalWord = m => ({ gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" }[m] || String(m || ""));
  const dayWord = d => { const t = Date.parse(String(d || "") + "T12:00:00"); return t ? new Date(t).toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" }) : String(d || ""); };
  function requirementsOf(step, data) {
    data = data || {};
    const i = typeof step === "number" ? step : STAGES.findIndex(s => s.k === (step && typeof step === "object" ? step.k : step));
    const s = STAGES[i]; if (!s) return null;
    const evs = (data.events || []).map(e => e && e.lane && e.key ? e : norm(e)).filter(Boolean).sort(byAt);
    let D = data.D || derive(evs, data.cancelled || null, data.where || null, null, data.place || null);
    // (derive's own rail is the steps themselves; the rail a view draws is railed's { s, i })
    if (!Array.isArray(D.rail) || !D.rail.length || !D.rail[0].s) D = railed(D, keepDone(data.stages, evs));
    const R = D.rail, pos = R.findIndex(r => r.i === i);
    const cx = data.context && typeof data.context === "object" ? data.context : {};
    const lines = Array.isArray(cx.lines) ? cx.lines : [], sheets = Array.isArray(cx.sheets) ? cx.sheets.filter(x => x && x.name) : [];
    const state = pos < 0 ? "none" : D.cancelled ? (i < D.stop ? "done" : i === D.stop ? "stopped" : "gone") : i <= D.step ? "done" : D.hand ? "skipped" : i === D.cur ? "now" : "later";
    const done = [], need = [], facts = [];
    // who did it, where and when (Paul, 28 Sep 23:51): "Welded · Marco R. · Welding", then its time and the station's words
    const doneOf = e => {
      const t = [e.type === s.types[0] ? s.l : labelOf(e.type), whoOf(e)].concat(placeOf(e) ? [placeOf(e)] : []).join(" · ");
      const said = e.text && e.text.toLowerCase() !== t.toLowerCase() ? str(e.text, 80) : "";
      return { t, sub: shortWhen(e.at) + (said ? " · " + said : ""), key: e.key };
    };
    for (const e of evs) if (STOP_OF[e.type] === i) done.push(doneOf(e));
    if (state === "done" && !done.length) done.push({ t: s.l, sub: "done before the timeline was kept" });
    /* A piece that is on no sheet NOW (the placement: D.fenced) has Nested and the steps after it still to do, whatever happened before. What happened stays
       on the Timeline, every seal of it; here it is only said in the past tense ("Was on SS Sheet 1 until Paul took it off, 5 Oct 12:44 AM"), never as a
       step done (Paul, 5 Oct 2026, image 3). */
    const fenced = !!(D.fenced && state !== "done" && state !== "none");
    let wasNote = "";
    if (fenced) {
      const pl = data.place || D.place || null, earlier = done.splice(0);
      if (s.k === "sheet") {
        const was = (pl && pl.wasOn) || (root.PiecePlacement && root.PiecePlacement.history ? root.PiecePlacement.history(evs).wasOn : null);
        wasNote = was && was.until ? `Was on ${was.label || "a sheet"} until ${was.by ? was.by + " took it off" : "it was taken off"}, ${shortWhen(was.until)}` : was && was.label ? `Was on ${was.label} earlier` : earlier.length ? "Was on a sheet earlier" : "";
      } else for (const d of earlier) facts.push(`Earlier: ${d.t} · ${d.sub}`);
    }
    /* the order's labels (Paul, 28 Sep 23:51): its QR label printed at the Sorting station or the Design Station is said
       under Sorted, "Label printed at the Sorting station by Ana P." (the latest print per place), or "Label not printed
       yet"; a shipping label under Shipped */
    const labels = s.k === "sorted" || s.k === "shipped" ? evs.filter(e => e.type === "labelPrinted" && labelStepOf(e) === s.k) : [];
    const lastAt = new Map(); for (const e of labels) lastAt.set(e.station || "", e);
    for (const e of lastAt.values()) {
      const who = personOf(e), n = labels.filter(x => (x.station || "") === (e.station || "")).length, kind = LABEL_WORD[e.data && e.data.label] || "";
      done.push({ t: `${s.k === "shipped" ? "Shipping label" : "Label"} printed at ${AT_PLACE[e.station] || "a station"}${who === "Not signed in" ? ", not signed in" : who ? " by " + who : ""}`,
        sub: [shortWhen(e.at)].concat(kind && s.k !== "shipped" ? [kind + " label"] : [], n > 1 ? [`printed ${n} times`] : []).join(" · "), key: e.key, label: true });
    }
    const quiet = QUIET[s.k] || [];
    for (const e of evs) {
      if (e.type === "scan" ? SCAN_STEP[e.station] === s.k : quiet.includes(e.type) && STOP_OF[e.type] == null)
        facts.push(`${titleOf(e, 70)}${e.type === "scan" ? " · " + whoOf(e) : ""} · ${shortWhen(e.at)}`);
    }
    const out = () => ({ k: s.k, label: s.l, i, n: pos + 1, of: R.length, state, done, need, facts: facts.slice(-6), fenced, was: wasNote });
    const add = (kind, t) => { if (t && !need.some(n => n.t === t)) need.push({ kind, t: String(t).slice(0, 220) }); };
    if (state === "none") { facts.push(`${s.none || "Not a step"} for ${lines.length === 1 ? "this piece" : "this order"}`); return out(); }
    if (state === "gone" || state === "stopped") { add("stop", D.cancelled && D.cancelled.source === "etsy" ? "Cancelled on Etsy: this step will not happen" : "Cancelled: this step will not happen"); return out(); }
    // completed by hand (Complete Order): nothing is owed, and who completed it and when is said
    if (state === "skipped") { done.push({ t: "Not needed: the order was completed by hand", sub: [shortWhen(D.hand.at)].concat(personOf(D.hand) ? [personOf(D.hand)] : []).join(" · "), key: D.hand.key }); return out(); }
    const noLabel = () => { if (s.k === "sorted" && !labels.length) add("label", "Label not printed yet"); };
    const name = l => [l.sku ? "SKU " + l.sku : "", l.form ? `(${l.form})` : ""].filter(Boolean).join(" ") || l.title || "a piece";
    // the pieces not on a sheet yet: an order travels whole, so they hold back the step being worked on too, and they show
    // under Nested even once another piece of the order is nested
    const loose = () => {
      for (const l of lines) {
        if (l.onSheet || l.hand || l.state === "gone") continue;   // (hand: completed by hand, a piece that needs no sheet)
        if (l.hold) add("person", `${name(l)} is held${l.reason ? ": " + l.reason : ""}. Release it in Review`);
        else if (l.state === "unmatched") add("person", `${name(l)} has no charm${l.reason ? " (" + l.reason + ")" : ""}; pick it in Review`);
        else if (l.problem) add("person", `${name(l)}: ${l.problem}; decide it in Review`);
        else if (l.state === "noDesign") add("person", `${name(l)} has no design${l.special ? " (" + l.special + ")" : ""}; finish it under Custom Orders`);
        else if (l.state === "oversize") add("person", `${name(l)} is too big for the plate; resize it in Review`);
        else if (l.wait && l.wait.kind === "slow") add("wait", `${metalWord(l.wait.material)} goes to the laser ${dayWord(l.wait.until)}; slow metals go every few days`);
        else if (l.wait) add("wait", `the ${metalWord(l.wait.material)} pieces waiting fill ${Math.round(+l.wait.pct || 0)}% of a sheet; a sheet is made when it is full, or sooner for a piece due to ship within 2 days`);
        else if (l.reason) add("wait", `${name(l)}: ${l.reason}`);
      }
    };
    if (state === "done") { if (s.k === "sheet") loose(); noLabel(); return out(); }
    if (state === "later") { const cp = R.findIndex(r => r.i === D.cur); add("after", `${STAGES[Math.max(0, D.cur)].l}${cp >= 0 && cp < pos - 1 ? " and the steps between" : ""}`); }
    if (fenced && s.k === "sheet") { add(D.hold ? "person" : "wait", "Not on a sheet now"); if (wasNote) add("was", wasNote); }
    if (D.hold) { const r = reasonOf(D.hold) || D.hold.text || ""; add("person", `On hold${r ? ": " + r.slice(0, 120) : ""}. Release it in Review`); }
    if (state === "now" && s.k !== "sheet") loose();
    if (s.k === "arrived") add("wait", HOW.arrived);
    else if (s.k === "sheet") {
      loose();
      add("next", HOW.sheet);
    } else if (s.k === "engraved") {
      const eng = lines.filter(l => l.engrave && l.engrave.needed);
      if (lines.length && !eng.length && lines.every(l => l.engrave || l.engraveCandidate === false)) facts.push("Nothing to engrave on this order");
      for (const l of eng) if (!l.engrave.approved) add("person", `Approve the back engraving${l.engrave.text ? ` “${String(l.engrave.text).slice(0, 60)}”` : ""} of ${name(l)} in Review`);
      for (const x of sheets) if (x.required > 0 && x.saved < x.required) add("wait", `${x.name} has ${x.saved} of ${x.required} back files saved`);
      if (!need.some(n => n.kind !== "after")) add("next", HOW.engraved);
    } else if (s.k === "laser" || s.k === "approved") {
      if (!sheets.length && D.step < 1) add("after", "It is nested on a sheet first");
      for (const x of sheets) {
        if (x.cut) continue;
        const g = x.stages || {};
        if (x.placed != null) facts.push(`${x.name} holds ${x.placed} piece${x.placed === 1 ? "" : "s"}${x.pct ? ` · ${Math.round(x.pct)}% full` : ""}`);
        // (a Rose Gold sheet's newest charms wait for its Cut Sheet press: said by name, with what to press)
        if (x.uncut > 0) add("person", `${x.name} has ${x.uncut} charm${x.uncut === 1 ? "" : "s"} not cut yet: press Cut Sheet`);
        else if (g.layout === false) add("wait", `${x.name}'s layout is checked and saved`);
        if (g.front === false) add("wait", `${x.name}'s front cut file is saved`);
        if (g.approval === false) add("person", `${x.name}: ${x.waiting || "some"} engraving${x.waiting === 1 ? "" : "s"} still to approve in Review`);
        if (g.backs === false && g.approval !== false) add("wait", `${x.name} has ${x.saved || 0} of ${x.required || 0} back files saved`);
        if (g.qr === false) add("person", `Print the QR labels for ${x.name}`);
        if (x.ready) add("next", `${x.name} is ready: commit its set and cut it`);
      }
      if (!evs.some(e => e.type === "setCommitted")) add("next", "its set is committed once every sheet in it is ready");
      add("next", "the laser cuts the sheet and marks it cut");
    } else add("next", HOW[s.k] || `${s.l.toLowerCase()} at its station`);
    noLabel();
    return out();
  }
  const REQ_WORD = { wait: "Waiting", person: "Needs a person", next: "Next", after: "After", stop: "Stopped", label: "", was: "" };   // (was: what the history says, in the past tense: never a step to do)
  // (a label not printed yet is said, but a step that is done stays done: only a real need makes it "Part done")
  const partDone = q => q.state === "done" && q.need.some(n => n.kind !== "label");
  const STATE_WORD = { done: "Done", now: "Next", later: "To come", stopped: "Stopped here", gone: "Won't happen", none: "Not needed", skipped: "Skipped" };
  const CHECK = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12.5l4.5 4.5L19 7.5"/></svg>`;
  const cap1 = t => { t = String(t || ""); return t.charAt(0).toUpperCase() + t.slice(1); };
  /** A step's lines: what is done with a check, what is missing with an open circle (the last two done, the first four missing). */
  function reqLines(q) {
    // (the step's own lines, then its labels: a label line never pushes out who did the step)
    const mine = q.done.filter(d => !d.label), labels = q.done.filter(d => d.label);
    const done = mine.slice(-2).concat(labels.slice(-2)).map(d => `<li class="rq ok"><i>${CHECK}</i><span>${esc(d.t)}<small>${esc(d.sub)}</small></span></li>`);
    const need = q.need.slice(0, 4).map(n => `<li class="rq ${n.kind}"><i aria-hidden="true"></i><span>${REQ_WORD[n.kind] ? `<em>${esc(REQ_WORD[n.kind])}</em>` : ""}${esc(cap1(n.t))}</span></li>`);
    const more = q.need.length > 4 ? `<li class="rq more"><span>${q.need.length - 4} more</span></li>` : "";
    if (q.state === "none") return `<ul class="tlReq"><li class="rq ok"><i>${CHECK}</i><span>${esc(q.facts[0] || "Not needed")}</span></li></ul>`;
    return `<ul class="tlReq">${done.join("")}${need.join("")}${more}</ul>`;
  }
  /** The small card under a hovered step; open: what a click on it does ("click to open it on the Timeline" on the header rail), none when it does nothing. */
  const reqCard = (q, open) => `<div class="xh"><b>${esc(q.label)}</b><span class="xs ${q.state}${partDone(q) ? " now" : ""}">${esc(partDone(q) ? "Part done" : STATE_WORD[q.state] || "")}</span></div>${reqLines(q)}` +
    `<div class="xf">${q.n ? `Step ${q.n} of ${q.of}` : "Not a step of this order"}${open ? " · " + esc(open) : ""}</div>`;
  /** Shows the card (a fixed layer) under dot, never over it: below when there is room, else beside it (and beside seal,
   *  the zoomed seal's rect in the view, when one is open). → the animation. */
  function placeExp(card, html, dot, whole, seal) {
    card.innerHTML = html;
    card.style.display = "block"; card.style.transform = "none"; card.style.opacity = "0"; card.className = "tlExp";
    // a transformed ancestor (the order view's dialog) moves a fixed layer's origin: measure where it really is
    const o = card.getBoundingClientRect(), d = dot.getBoundingClientRect(), wr = (whole || dot).getBoundingClientRect();
    const vw = root.innerWidth || 1200, vh = root.innerHeight || 800, w = card.offsetWidth, h = card.offsetHeight, cx = d.left + d.width / 2;
    let x = clamp(cx - w / 2, 8, vw - w - 8), y = wr.bottom + 10;
    if (y + h > vh - 8) {   // no room below: beside the dot (the zoomed seal takes the room above it)
      y = clamp(d.top + d.height / 2 - 24, 8, vh - h - 8);
      const L = seal ? Math.min(d.left, seal.left) : d.left, R = seal ? Math.max(d.right, seal.right) : d.right;
      x = R + 14 + w <= vw - 8 ? R + 14 : Math.max(8, L - 14 - w);
      card.classList.add("side");
    } else card.style.setProperty("--ax", clamp(cx - x, 14, w - 14) + "px");
    const to = `translate(${Math.round(x - o.left)}px,${Math.round(y - o.top)}px)`;
    card.style.transform = to; card.style.opacity = "1";
    return anim(card, [{ opacity: 0, transform: to + " translateY(-5px) scale(.98)" }, { opacity: 1, transform: to }], 200);
  }
  function fadeExp(card, a) {
    if (a) { try { a.cancel(); } catch (_) {} }
    const b = anim(card, [{ opacity: 1 }, { opacity: 0 }], 130, { easing: "ease-in" });
    card.style.opacity = "0";
    const done = () => { if (card.style.opacity === "0") { card.style.display = "none"; card.innerHTML = ""; } };
    if (b) b.finished.then(done, () => {}); else done();
    return b;
  }
  /** The order view's "Where it is now" card (and any host): hovering its seal or a stamp in it (.tlNowSeal, .tlMini,
   *  [data-tl-step]) shows the step card under it after a rest; a click opens that step on the Timeline via onPin({ stage }).
   *  Leaving the original seal dismisses the card. get() → { events, cancelled, where, context, stages }.
   *  Wired once per host (it survives the host's innerHTML being written again); a later call only swaps get/onPin. */
  function explainOn(host, get, onPin) {
    if (!host) return;
    host._tlExpGet = get; host._tlExpPin = onPin;
    if (host._tlExp) return;
    css();
    const card = doc.createElement("div"); card.className = "tlExp"; card.setAttribute("role", "tooltip");
    (host.closest && host.closest("dialog") || doc.body).appendChild(card);
    let on = null, a = null, cur = null;
    const SEL = ".tlNowSeal, .tlMini, [data-tl-step]";
    const stepOf = b => {
      const g = (typeof host._tlExpGet === "function" && host._tlExpGet()) || {};
      // (g.D: the rail's own derived state, mount().state(), so the card and the rail can never disagree; g.place: where the piece IS now, see derive)
      const evs = (g.events || []).map(norm).filter(Boolean).sort(byAt), D = g.D && Array.isArray(g.D.rail) && g.D.rail[0] && g.D.rail[0].s ? g.D : railed(derive(evs, g.cancelled || null, g.where || null, null, g.place || null), keepDone(g.stages, evs));
      let i = b.dataset.tlStep ? STAGES.findIndex(s => s.k === b.dataset.tlStep) : -1;
      if (i < 0 && b.dataset.tlEv) { const e = evs.find(x => x.id === b.dataset.tlEv); if (e && own(STOP_OF, e.type)) i = STOP_OF[e.type]; }
      if (i < 0) i = D.cancelled ? D.stop : D.cur >= 0 ? D.cur : D.step;
      return i >= 0 ? requirementsOf(i, { events: evs, D, context: g.context, place: g.place || null }) : null;
    };
    const foot = () => { const f = card.querySelector(".xf"); if (f && cur) f.textContent = `${cur.n ? `Step ${cur.n} of ${cur.of}` : "Not a step of this order"}${typeof host._tlExpPin === "function" ? " · click seal to open on the Timeline" : ""}`; };
    const show = (b, kb) => {
      const q = stepOf(b); if (!q) return false;
      if (a) { try { a.cancel(); } catch (_) {} }
      on = b; cur = q;
      // the seal grows where it stands; the card goes under what it has grown to, never over it
      if (b.matches(".tlNowSeal, .tlMini")) zoomOn(b, kb);
      const zr = zoomRect(b), whole = zr ? withZoom(b, b) : b;
      a = placeExp(card, reqCard(q), b.querySelector(".s") || b, whole, zr && { left: zr.left, right: zr.right }); card.classList.add("on"); foot();
      return true;
    };
    const hide = now => {
      if (!on) return; zoomOff(on, now); on = null; card.classList.remove("on");
      if (now) { if (a) { try { a.cancel(); } catch (_) {} } a = null; card.style.display = "none"; }
      else a = fadeExp(card, a);
    };
    const hover = restOnSeal(host, node => node.closest?.(SEL), show, hide, { click: () => typeof host._tlExpPin !== "function" });   // (a click that opens the step on the Timeline takes the Overview away: nothing to zoom)
    host.addEventListener("focusin", ev => { const b = ev.target.closest?.(SEL); if (b && b.matches(":focus-visible")) hover.open(b, true); });
    host.addEventListener("focusout", ev => { if (ev.target.closest?.(SEL) === hover.current) hover.cancel(); });
    host.addEventListener("click", ev => {
      const b = ev.target.closest && ev.target.closest(".tlNowSeal, [data-tl-step]"); if (!b) return;
      const q = stepOf(b); hover.cancel(true);
      if (q && typeof host._tlExpPin === "function") { try { host._tlExpPin({ stage: q.k }); } catch (err) { warn("onPin", err); } }
    });
    const dlg = host.closest && host.closest("dialog"); if (dlg) dlg.addEventListener("close", () => hover.cancel(true));
    host._tlExp = card;
  }

  /* ════ the component's look (once per page) ════ */
  const CSS = `
.tlUI{position:relative;min-width:0;min-height:0;display:flex;flex-direction:column;color:var(--ink,#1c1a17);font:13px/1.45 var(--sans,system-ui,sans-serif);--tlE:cubic-bezier(.2,.8,.2,1);--tlSpring:cubic-bezier(.3,1.7,.5,1);--tlSlate:#2f5563;--tl-t:1}
.tlUI *{box-sizing:border-box}
.tlUI button{font:inherit;color:inherit;cursor:pointer}
.tlUI [hidden]{display:none!important}
.tlBar.inTools{border:0;padding:0;min-height:0;flex-wrap:nowrap;gap:10px;min-width:0;font:13px/1.45 var(--sans,system-ui,sans-serif);--tlE:cubic-bezier(.2,.8,.2,1)}
.tlBar.inTools *{box-sizing:border-box}
.tlBar.inTools button{font:inherit;cursor:pointer}
.tlBar.inTools [hidden]{display:none!important}
@media (max-width:1599px){.tlBar.inTools .tlSum:not(:has(.err)){display:none}}
.tlUI .tlLbl{display:block;font:700 9.5px var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--ink45)}
.tlTop{display:flex;align-items:center;gap:14px 26px;padding:10px 18px 9px;border-bottom:1px solid var(--line2);flex-wrap:wrap;flex:none}
.tlNow{flex:1 1 300px;min-width:240px;max-width:460px}
.tlNowT{font:500 17px/1.25 var(--serif);margin:3px 0 5px;color:var(--ink)}
.tlNow.cx .tlNowT{color:#8a3a26}.tlNow.hold .tlNowT{color:#7a5a1d}.tlNow.done .tlNowT{color:#19663f}
.tlNowS{display:flex;align-items:center;flex-wrap:wrap;gap:5px 8px;font:10.5px var(--mono);color:var(--ink45);min-height:22px}
.tlNowS .why{flex-basis:100%;font:12px/1.4 var(--sans);color:#8a3a26;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tlLink{border:0;background:none;padding:0;font:600 11px var(--sans);color:var(--gold);text-decoration:underline;text-underline-offset:2px;text-decoration-color:var(--goldLine)}
.tlLink:hover{text-decoration-color:currentColor}
.tlOpenSheet[aria-disabled="true"]{opacity:.45;cursor:not-allowed;color:var(--ink45);text-decoration-color:transparent;transform:none;box-shadow:none}
.tlOpenSheet[aria-disabled="true"]:hover{opacity:.45;background:transparent;text-decoration-color:transparent;transform:none;box-shadow:none}
.tlRail{position:relative;flex:3 1 520px;min-width:0;max-width:860px;margin-left:auto}
.tlStops{position:relative;display:grid;grid-template-columns:repeat(var(--n,9),minmax(0,1fr));margin:0;padding:0}
.tlTrack,.tlFill{position:absolute;top:calc(var(--seal-fit,var(--seal-size,84px)) / 2);height:2px;border-radius:2px;left:calc(100% / (2 * var(--n,9)));right:calc(100% / (2 * var(--n,9)))}
.tlTrack{background:repeating-linear-gradient(90deg,var(--ink25) 0 4px,transparent 4px 8px)}
.tlFill{background:var(--sage);transform-origin:0 50%;transform:scaleX(0);transition:transform .9s cubic-bezier(.3,.1,.2,1)}
.tlRail.cx .tlFill{background:linear-gradient(90deg,var(--sage) 75%,var(--clay))}
.tlStop{position:relative;z-index:1;display:flex;flex-direction:column;align-items:center;gap:4px;min-width:0;border:0;background:none;padding:0 1px;color:var(--ink45)}
.tlStop .tlSeal{position:relative;display:block;width:var(--seal-fit,var(--seal-size,84px));height:var(--seal-fit,var(--seal-size,84px));transform:rotate(var(--rot,0deg));transition:transform .22s var(--tlE),opacity .24s}
.tlStop .tlSeal svg{width:100%;height:100%;display:block;overflow:visible}
.tlStop>span{font:700 8.5px/1.2 var(--mono);letter-spacing:.07em;text-transform:uppercase;text-align:center;max-width:100%;overflow-wrap:anywhere}
.tlStop.d>span{color:var(--ink70)}
.tlStop.c>span{color:#7a5a1d}
.tlStop.c .tlSeal::after{content:"";position:absolute;inset:-4px;border-radius:50%;border:2px solid var(--gold2);opacity:0;animation:tlRing 2s ease-out infinite}
.tlStop.c.paused .tlSeal::after{animation:none;opacity:.9;border-color:#c79a3a;border-style:dashed}
.tlStop.f .tlSeal{opacity:.45}
.tlStop.x>span{color:#8a3a26}
.tlStop.gone{opacity:.45}.tlStop.gone>span{text-decoration:line-through}
.tlStop .tlCnt{position:absolute;top:-5px;left:calc(50% + 9px);font:700 8px/1 var(--mono);letter-spacing:.02em;font-style:normal;padding:2px 5px;border-radius:999px;background:var(--goldSoft,#f6eedc);color:#7a5a1d;box-shadow:0 0 0 1px var(--goldLine,#e3cf9f);white-space:nowrap;pointer-events:none;z-index:2;animation:tlCntIn .36s var(--tlSpring) both}
@keyframes tlCntIn{from{opacity:0;transform:scale(.6)}}
@keyframes tlRing{0%{transform:scale(.85);opacity:.85}70%,100%{transform:scale(1.4);opacity:0}}
.tlCxStamp{position:absolute;left:50%;top:50%;width:var(--seal-fit,var(--seal-size,84px));height:var(--seal-fit,var(--seal-size,84px));cursor:pointer;z-index:3;transform:translate(-50%,-50%) rotate(-6deg)}
.tlCxStamp svg{display:block;width:100%;height:100%;mix-blend-mode:multiply;opacity:.93}
.tlBar{display:flex;align-items:center;gap:6px;padding:7px 18px;border-bottom:1px solid var(--line);flex-wrap:wrap;min-height:40px;flex:none}
.tlBarR{margin-left:auto;display:flex;align-items:center;gap:10px;font:10.5px var(--mono);color:var(--ink45);min-width:0}
.tlBarR .err{color:#8a3a26;display:inline-flex;gap:6px;align-items:center}
.tlLive{display:inline-flex;align-items:center;gap:6px;font:700 9.5px var(--mono);letter-spacing:.1em;color:#3c5a39;background:var(--sageSoft);border-radius:999px;padding:3px 9px}
.tlLive i{width:7px;height:7px;border-radius:50%;background:currentColor;animation:tlBreathe 1.8s ease-in-out infinite}
@keyframes tlBreathe{50%{opacity:.3}}
.tlBusy{display:inline-flex;align-items:center;gap:6px}
.tlSpin{display:inline-block;width:11px;height:11px;flex:none;border:2px solid rgba(0,0,0,.12);border-top-color:var(--gold);border-radius:50%;animation:tlSpin .7s linear infinite}
@keyframes tlSpin{to{transform:rotate(360deg)}}
/* the chart fills the room its host gives it (fit(), below): the grid is one row as tall as that room, whatever it holds, so its size never depends on what is drawn in it */
.tlGrid{display:grid;grid-template-columns:var(--tl-label-w,140px) minmax(0,1fr);grid-template-rows:minmax(0,1fr);position:relative;flex:1 1 360px;min-height:0;overflow:hidden}
.tlLanes{border-right:1px solid var(--line);background:var(--card);padding-top:var(--tl-top,40px);padding-bottom:var(--tl-axis,26px);overflow:hidden;min-width:0}
.tlLane{position:relative;height:var(--tl-lane-height,42px);display:flex;flex-direction:column;justify-content:center;padding:0 var(--tl-padx,14px);border-bottom:1px solid var(--line2);min-width:0}
.tlLane::before{content:"";position:absolute;inset:0;background:linear-gradient(90deg,rgba(202,168,97,.16),transparent);opacity:0;transition:opacity .24s}
.tlLane.on::before{opacity:1}
.tlLane b{position:relative;font:700 var(--tl-fs-lane,9.5px) var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--ink70);display:flex;align-items:center;gap:calc(6px * var(--tl-t,1))}
.tlLane b svg{width:var(--tl-ic,12px);height:var(--tl-ic,12px);color:var(--ink45);flex:none}
.tlLane span{position:relative;font-size:var(--tl-fs-sub,11px);color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tlUI.tlNoSub .tlLane span{display:none}
.tlLane.stn b{color:var(--tlSlate)}
.tlScroll{overflow:hidden;position:relative;min-width:0;min-height:0}
.tlCanvas{position:relative;height:100%;min-width:100%}
.tlPath{position:absolute;left:0;top:0;overflow:visible;pointer-events:none}
.tlDay{position:absolute;top:0;bottom:0;border-right:1px dashed var(--line)}
.tlDay.alt{background:rgba(250,247,241,.7)}
.tlDay .dh{position:absolute;left:var(--tl-dhx,14px);top:var(--tl-dhy,10px);font:700 var(--tl-fs-day,10px) var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--ink70);white-space:nowrap}
.tlDay .dh small{display:block;font:400 var(--tl-fs-small,9.5px) var(--mono);letter-spacing:.02em;color:var(--ink45);text-transform:none;margin-top:1px;overflow:hidden;text-overflow:ellipsis}
.tlUI.tlNoSmall .tlDay .dh small{display:none}
.tlDay:not(.idle) .dh{right:calc(var(--tl-dhx,14px) * .57);overflow:hidden;text-overflow:ellipsis}
.tlDay.idle{background:repeating-linear-gradient(135deg,transparent 0 6px,rgba(196,189,176,.22) 6px 7px)}
.tlDay.idle .dh{left:50%;transform:translateX(-50%);text-align:center}
.tlLaneLine{position:absolute;left:0;right:0;height:1px;background:var(--line2)}
.tlSt{position:absolute;width:var(--s);height:var(--s);margin:calc(var(--s) / -2) 0 0 calc(var(--s) / -2);border:0;padding:0;background:transparent;border-radius:50%;transform:rotate(var(--rot));transition:opacity .24s,transform .22s var(--tlE);z-index:2}
.tlSt svg{width:100%;height:100%;display:block;overflow:visible;mix-blend-mode:multiply}
.tlSt.sel::before{content:"";position:absolute;inset:calc(-6px * var(--tl-t,1));border-radius:50%;border:calc(1.5px * var(--tl-t,1)) solid var(--gold);box-shadow:0 0 0 calc(4px * var(--tl-t,1)) rgba(202,168,97,.18);animation:tlSelIn .32s var(--tlSpring) both}
@keyframes tlSelIn{from{transform:scale(.6);opacity:0}}
.tlSt.hl::after{content:"";position:absolute;inset:calc(-10px * var(--tl-t,1));border-radius:50%;background:radial-gradient(rgba(202,168,97,.38),transparent 70%);z-index:-1}
.tlSt.dim{opacity:.13}.tlSt.dim svg{filter:grayscale(1)}
.tlSt.pend svg{opacity:.65}
.tlSt.pending svg,.tlStop .tlSeal.pending svg{visibility:hidden}
.tlSt.ghost{opacity:.42;cursor:default}
.tlSt.ghost.sel{opacity:.85}
.tlSt.ghost svg{mix-blend-mode:normal}
.tlInkRing{position:absolute;border-radius:50%;border:2px solid;pointer-events:none;z-index:1;opacity:0}
.tlNowLine{position:absolute;top:calc(var(--tl-top,40px) - 6px * var(--tl-t,1));bottom:calc(11px * var(--tl-t,1));width:0;border-left:calc(1.5px * var(--tl-t,1)) solid var(--gold);z-index:1}
.tlNowLine::before,.tlNowLine::after{content:"";position:absolute;left:calc(-5px * var(--tl-t,1));bottom:calc(-5px * var(--tl-t,1));width:calc(9px * var(--tl-t,1));height:calc(9px * var(--tl-t,1));border-radius:50%;background:var(--gold)}
.tlNowLine::after{background:none;border:calc(2px * var(--tl-t,1)) solid var(--gold2);left:calc(-7px * var(--tl-t,1));bottom:calc(-7px * var(--tl-t,1));width:calc(13px * var(--tl-t,1));height:calc(13px * var(--tl-t,1));animation:tlRing 2s ease-out infinite}
.tlNowLine span{position:absolute;right:calc(8px * var(--tl-t,1));bottom:calc(-3px * var(--tl-t,1));font:700 var(--tl-fs-pill,9.5px) var(--mono);letter-spacing:.1em;color:#7a5a1d;white-space:nowrap;background:var(--goldSoft);padding:calc(2px * var(--tl-t,1)) calc(6px * var(--tl-t,1));border-radius:calc(5px * var(--tl-t,1))}
.tlNowLine.cx{border-color:var(--clay)}.tlNowLine.cx::before{background:var(--clay)}.tlNowLine.cx::after{display:none}
.tlNowLine.cx span{background:var(--claySoft);color:#8a3a26}
.tlAfterCx{position:absolute;top:calc(var(--tl-top,40px) - 6px * var(--tl-t,1));bottom:0;right:0;background:repeating-linear-gradient(135deg,transparent 0 7px,rgba(176,86,63,.09) 7px 8px)}
.tlMsg{position:absolute;left:calc(var(--tl-label-w,140px) + 18px);top:50%;transform:translateY(-50%);display:flex;align-items:center;gap:9px;font-size:12.5px;color:var(--ink70);background:var(--card);border:1px solid var(--line);border-radius:10px;padding:9px 13px;box-shadow:var(--sh);z-index:4;max-width:calc(100% - var(--tl-label-w,140px) - 36px)}
.tlMsg.err{color:#8a3a26;background:var(--claySoft);border-color:#e7b9aa}
.tlBadge{display:inline-flex;align-items:center;gap:7px;border:1px solid var(--line);border-radius:999px;padding:3px 10px 3px 4px;background:var(--card);font:12px var(--sans);color:var(--ink);white-space:nowrap}
.tlBadge i{width:20px;height:20px;border-radius:50%;display:grid;place-items:center;background:var(--slateSoft);color:var(--tlSlate);flex:none}
.tlBadge i svg{width:11px;height:11px}
.tlBadge em{font:700 9.5px var(--mono);letter-spacing:.1em;color:var(--tlSlate);font-style:normal;text-transform:uppercase}
.tlBadge.sm{font-size:11px;padding:1px 8px 1px 2px;gap:5px}.tlBadge.sm i{width:17px;height:17px}.tlBadge.sm em{font-size:8.5px}
.tlGrid>.tlEmpty{position:absolute;left:158px;right:18px;top:50%;transform:translateY(-50%);margin:0;color:var(--ink45);font-size:12.5px;pointer-events:none}
.tlUI.compact .tlBar,.tlUI.compact .tlGrid,.tlUI.compact .tlNow{display:none}
.tlUI.compact{height:100%;justify-content:center}
.tlUI.compact .tlTop{border-bottom:0;padding:0;flex:1 1 auto;align-items:center;flex-wrap:nowrap}
.tlUI.compact .tlRail{flex:1 1 auto;max-width:none;margin:0}
.tlUI.compact .tlStops{grid-template-columns:repeat(var(--n,9),minmax(0,1fr))!important;row-gap:0}
.tlUI.compact .tlStop .tlCnt{top:3px;left:calc(50% + 13px);font-size:7.5px;padding:1.5px 4px}
.tlUI.compact .tlTrack,.tlUI.compact .tlFill{display:block;top:calc(var(--seal-fit,var(--seal-size,84px)) / 2)}
.tlUI.compact .tlStop{gap:2px}
.tlUI.compact .tlStop .tlSeal{width:var(--seal-fit,var(--seal-size,84px));height:var(--seal-fit,var(--seal-size,84px))}
.tlUI.compact .tlStop>span{font-size:7.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tlUI.compact .tlCxStamp{width:var(--seal-fit,var(--seal-size,84px));height:var(--seal-fit,var(--seal-size,84px))}
.tlUI.compact .tlMsg{left:50%;top:50%;transform:translate(-50%,-50%);max-width:100%;padding:3px 10px;font-size:11px;gap:6px;box-shadow:none;white-space:nowrap}
@media (max-width:900px){.tlRail{flex-basis:100%}.tlStops{grid-template-columns:repeat(6,minmax(0,1fr));row-gap:10px}.tlTrack,.tlFill{display:none}}
.tlNowSeal{position:relative;width:var(--seal-fit,var(--seal-size,84px));height:var(--seal-fit,var(--seal-size,84px));flex:none;transform:rotate(var(--rot,0deg))}
.tlNowSeal svg,.tlMini svg{display:block;width:100%;height:100%;overflow:visible}
.tlNowSeal.cx{margin:0;mix-blend-mode:multiply;transform:rotate(-11deg)}
.tlBlock{display:inline-flex;align-items:baseline;gap:9px;min-width:0;max-width:100%;font:13px/1.45 var(--sans);color:#7a5a1d;background:var(--goldSoft);border:1px solid var(--goldLine);border-radius:10px;padding:5px 12px}
.tlBlock b{font:700 9.5px var(--mono);letter-spacing:.1em;text-transform:uppercase;color:#8a6a24;flex:none}
.tlCxList{flex:1 1 100%;display:flex;flex-direction:column;gap:3px;min-width:0;margin:2px 0 6px;font:12.5px/1.4 var(--sans)}
.tlCxList .h{font-weight:700;color:var(--clay,#b0563f)}
.tlCxList .s{display:flex;align-items:baseline;gap:7px;min-width:0}
.tlCxList .s i{font-style:normal;font-weight:800;width:1em;flex:none;text-align:center}
.tlCxList .s.ok i{color:var(--sage,#4f7a5a)}.tlCxList .s.wait i,.tlCxList .s.wait{color:#8a6a24}.tlCxList .s.bad i,.tlCxList .s.bad{color:var(--clay,#b0563f)}.tlCxList .s.back i{color:var(--sage,#4f7a5a)}
.tlCxList small{opacity:.7;font-size:11px;white-space:nowrap}
.tlMini{position:relative;width:var(--seal-fit,var(--seal-size,84px));height:var(--seal-fit,var(--seal-size,84px));margin:0 1px;padding:0;border:0;background:none;flex:none;cursor:pointer;vertical-align:middle}
/* The same regular seal and delayed 168px hover face are used in compact and full views. */
.tlMini>span{position:absolute;inset:0;border-radius:50%;transform:rotate(var(--rot,0deg));transition:transform .22s cubic-bezier(.2,.8,.2,1);pointer-events:none}
.tlMini .f{visibility:hidden}
.tlMini:focus-visible{outline:0}
.tlExp{position:fixed;z-index:2147483001;left:0;top:0;width:272px;pointer-events:none;background:var(--card,#fffefb);color:var(--ink,#1c1a17);border:1px solid var(--line,#e7e1d6);border-radius:12px;padding:11px 14px 9px;box-shadow:0 1px 0 rgba(255,255,255,.6) inset,0 14px 34px rgba(30,26,20,.16),0 2px 6px rgba(30,26,20,.06);font:12px/1.4 var(--sans,system-ui,sans-serif);opacity:0;display:none}
.tlExp::before{content:"";position:absolute;left:var(--ax,50%);top:-6px;width:10px;height:10px;margin-left:-5px;background:inherit;border-left:1px solid var(--line,#e7e1d6);border-top:1px solid var(--line,#e7e1d6);transform:rotate(45deg)}
.tlExp.up::before{top:auto;bottom:-6px;transform:rotate(225deg)}
.tlExp .xh{display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin-bottom:7px}
.tlExp .xh b{font:500 15px/1.2 var(--serif,Georgia,serif);color:var(--ink)}
.tlExp .xs{flex:none;font:700 8.5px var(--mono,monospace);letter-spacing:.1em;text-transform:uppercase;color:var(--ink45);border-radius:999px;padding:2px 7px;background:var(--card2,#f6f2ea)}
.tlExp .xs.done{color:#3c5a39;background:var(--sageSoft,#e8efe3)}
.tlExp .xs.now{color:#7a5a1d;background:var(--goldSoft,#f6eedc)}
.tlExp .xs.stopped,.tlExp .xs.gone{color:#8a3a26;background:var(--claySoft,#f4e3dc)}
.tlExp .xf{margin-top:8px;padding-top:6px;border-top:1px solid var(--line2,#efe9df);font:9.5px var(--mono,monospace);letter-spacing:.04em;color:var(--ink45)}
.tlReq{list-style:none;margin:0;padding:0;display:grid;gap:6px}
.tlReq .rq{display:grid;grid-template-columns:16px minmax(0,1fr);gap:8px;align-items:start;color:var(--ink70)}
.tlReq .rq>i{width:14px;height:14px;margin-top:1px;border-radius:50%;border:1.5px solid var(--ink25,#c4bdb0);display:grid;place-items:center}
.tlReq .rq.ok>i{border:0;background:var(--sage,#6f8d6a);color:#fff}.tlReq .rq.ok>i svg{width:9px;height:9px}
.tlReq .rq.person>i{border-color:#c79a3a}.tlReq .rq.stop>i{border-color:var(--clay,#b0563f)}
.tlReq .rq.after>i,.tlReq .rq.next>i{border-style:dashed}.tlReq .rq.was>i{border-style:dotted}.tlReq .rq.was span{color:var(--ink45)}
.tlReq .rq span{min-width:0;overflow-wrap:anywhere}
.tlReq .rq.ok span{color:var(--ink)}
.tlReq .rq small{display:block;font:10px var(--mono,monospace);color:var(--ink45);margin-top:1px}
.tlReq .rq em{font:700 8.5px var(--mono,monospace);letter-spacing:.09em;text-transform:uppercase;font-style:normal;color:var(--ink45);margin-right:6px}
.tlReq .rq.person em{color:#7a5a1d}.tlReq .rq.stop em{color:#8a3a26}
.tlReq .rq.more{grid-template-columns:1fr;padding-left:24px;font:10px var(--mono,monospace);color:var(--ink45)}
.tlSt.ghost[data-stage]{cursor:pointer}
.tlExp.side::before{display:none}
/* a grown seal's own ring or glow stays a thin ring round it (Paul, 3 Oct: the ring that came with the zoom was bigger than the seal's own
   growing): Seal.zoom puts the grown seal's scale in --zk, so these are drawn at 1 / --zk of their size and come out the same thin ring once grown */
.tlSt.sel[data-seal-zoom]::before{inset:calc(-3px / var(--zk, 1));border-width:calc(1.5px / var(--zk, 1));box-shadow:0 0 0 calc(2px / var(--zk, 1)) rgba(202,168,97,.18)}
.tlSt.hl[data-seal-zoom]::after{inset:calc(-4px / var(--zk, 1))}
.tlStop.c .tlSeal[data-seal-zoom]::after{inset:calc(-3px / var(--zk, 1));border-width:calc(1.5px / var(--zk, 1));animation:none;opacity:.9}
@media (prefers-reduced-motion:reduce){.tlUI *,.tlUI *::before,.tlUI *::after,.tlMini>span{animation-duration:.001s!important;animation-iteration-count:1!important;transition-duration:.001s!important}.tlUI .tlSpin{animation:tlSpin 1.4s linear infinite!important}}
`;
  function css() {
    if (doc.getElementById("tlUiCss")) return;
    const s = doc.createElement("style"); s.id = "tlUiCss"; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }
  const badge = (e, sm) => `<span class="tlBadge${sm ? " sm" : ""}"><i>${iconSvg((LANE[e.lane] || LANE.office).ic)}</i><em>${esc(e.print ? e.print.where : stationName(e))}</em>${esc(whoOf(e))}</span>`;
  /* ════ the chart fits its room (Paul, 5 Oct 2026, on the Timeline tab: "expand the chart and enlarge the seals and all other components of
     the chart to fit the empty space below ... the full chart should always be visible on one screen without scrolling, this needs to be
     dynamic in nature") ════
     The grid takes all the room its host gives it, and one measure of that room (its height and width, read before anything is written)
     sets every part of the chart together:
       fitVertical  the height → one scale for the text and the strokes, the lane height, the band for the day heads and the one for the
                    NOW pill, and the seal size: the lane height less a little air, never more than FIT_CAP. A taller screen spreads
                    the lanes instead of growing the seals; a shorter one first takes the air out, then the day heads' second line
                    and the lanes' who-lines go (fewer labels, never clipped ones), and the seals only go below FIT_MIN last.
       fitChart     the width → how far apart the seals sit, how wide the days are. The seals are laid out one after the other in time,
                    each as near its neighbour as a seal that must not touch it can be (placeRun: the seal's own circle against every
                    seal it could touch, so two seals on different lanes may stand one above the other, and two on one lane never
                    meet). Too wide a chart first closes the spacing up, then the seals themselves get smaller (down to FIT_FLOOR);
                    a roomy one opens the spacing out (up to FIT_STRETCH times), so the route spans the screen.
     There is no sideways scroll unless even the smallest seals of a very long history cannot fit. */
  const FIT_CAP = 84;        // the widest a seal rests on the chart: the app's one shared seal size (SEAL_SIZE); what a taller screen has over goes to the lanes
  const FIT_MIN = 20;        // under this the labels give way first
  const FIT_FLOOR = 8;       // only a long history on a narrow screen goes under FIT_MIN
  const FIT_STRETCH = 2.6;   // the most a roomy chart opens its spacing out
  const FIT_CLOSE = .2;      // the nearest two seals on different lanes stand, as a part of a seal and its air, when the chart is at its tightest
  const dayKey = t => { const d = new Date(t); return d.getFullYear() * 10000 + (d.getMonth() + 1) * 100 + d.getDate(); };
  const midnight = t => { const d = new Date(t); d.setHours(0, 0, 0, 0); return +d; };
  const r2 = n => Math.round(n * 100) / 100;
  /** What the height and the width of the room decide: the scale, the sizes of the text, the bands and the lanes, and the seal size. */
  function fitVertical(h, w, cap, minTier) {
    h = Math.max(120, +h || 360); w = Math.max(200, +w || 800);
    // one scale for the text and the strokes: it grows with the room (the slower of its height and its width, so a phone keeps phone-sized words), within a floor and a ceiling
    const t = clamp(Math.min(Math.pow(h / 360, .62), w / 700), .82, 1.7);
    const fs = { lane: clamp(9.5 * t, 8.5, 16), sub: clamp(11 * t, 9, 14.5), day: clamp(10 * t, 9, 17), small: clamp(9.5 * t, 8.5, 15), pill: clamp(9.5 * t, 8.5, 15) };
    const L = LANES.length;
    let top = 0, axis = 0, laneH = 0, s = 0, tier = minTier || 0;
    for (; tier < 3; tier++) {
      // tier 0: the day heads of two lines; tier 1: one line; tier 2: one line, closer, and the pill's band as thin as it can be
      top = Math.round(tier === 0 ? 14 * t + fs.day * 1.25 + fs.small * 1.25 + 2 : tier === 1 ? 14 * t + fs.day * 1.25 : 5 + fs.day * 1.2 + 4);
      axis = Math.round(tier < 2 ? fs.pill * 1.2 + 18 * t : fs.pill * 1.2 + 12);
      laneH = Math.max(12, Math.floor((h - top - axis) / L));
      s = Math.min(cap, laneH - clamp(laneH * .18, 4, 22));
      if (s >= FIT_MIN) break;
    }
    if (tier === 3) tier = 2;
    if (s < FIT_MIN) s = Math.max(FIT_FLOOR, Math.min(cap, laneH - 3));   // (spacing first, then the seal)
    axis += Math.max(0, h - top - axis - laneH * L);   // (the pixels that do not divide into lanes go to the foot, so the lanes end exactly where the room does)
    const padx = Math.round(clamp(14 * Math.pow(t, .8), 7, 22) * (w < 520 ? .65 : 1)), ic = clamp(12 * t, 10, 19);
    // the label column: a lane's name on one line, but never wider than a quarter of the room; narrower than that the name wraps at its spaces, and the column is never
    // narrower than the longest word (ASSEMBLY, SHIPPING) with its icon
    const word = 8 * .7 * fs.lane + ic + 5 * t + 2 * padx + 4;
    const labelW = Math.round(clamp(fs.lane * 10.6 + 2 * padx + 6, word, Math.max(word, w * .26)));
    return { h, w, t, fs, top, axis, laneH, s: Math.floor(s * 2) / 2, cap, labelW, padx, ic, dhx: padx, dhy: Math.round(tier === 2 ? 4 : 9 * t),
      showSmall: tier === 0, showSub: laneH >= fs.lane * 1.3 + fs.sub * 1.3 + 8, tier, route: r2(clamp(1.6 * t, 1.3, 3.6)), future: r2(clamp(1.4 * t, 1.2, 3.2)) };
  }
  /** The days of the drawn seals, oldest first: each with its seals' lanes, the words of its head and the idle days before it. */
  function dayModel(evs) {
    const days = []; let cur = null;
    for (const e of evs) { const k = dayKey(e.at); if (!cur || cur.k !== k) { cur = { k, at: e.at, evs: [], lanes: [], gap: 0, prev: 0 }; days.push(cur); } cur.evs.push(e); cur.lanes.push((LANE[e.lane] || LANE.office).i); }
    days.forEach((d, i) => {
      // its head's second line ("8:52 AM – 3:44 PM · 9", one time when there is one) never runs into the next day
      const t0 = timeOf(d.evs[0].at), t1 = timeOf(d.evs[d.evs.length - 1].at), dt = new Date(d.at);
      d.span = (t0 === t1 ? t0 : t0 + " – " + t1) + " · " + d.evs.length;
      d.chars = (DAYN[dt.getDay()] + " · " + MON[dt.getMonth()] + " " + dt.getDate()).length + (dt.getFullYear() !== new Date().getFullYear() ? 5 : 0);
      d.short = (DAYN[dt.getDay()] + " " + dt.getDate()).length;   // (a very tight chart names a day "THU 1")
      if (i) { d.prev = days[i - 1].at; d.gap = Math.round((midnight(d.at) - midnight(d.prev)) / 864e5) - 1; }
    });
    return days;
  }
  /** x of each seal of a run (their centres, from the run's start): each as near the one before as it may stand (`c` of a seal and its
   *  air), and clear of every seal its own circle could touch, which lies on a lane near enough. */
  function placeRun(lanes, s, g, c, first, laneH) {
    const pitch = s + g, xs = [];
    for (let j = 0; j < lanes.length; j++) {
      let x = j ? xs[j - 1] + c * pitch : first;
      for (let i = j - 1; i >= 0 && xs[i] + pitch > x; i--) {
        const dy = Math.abs(lanes[j] - lanes[i]) * laneH;
        if (dy < pitch) x = Math.max(x, xs[i] + Math.sqrt(pitch * pitch - dy * dy));
      }
      xs.push(x);
    }
    return xs;
  }
  /** The spacing for a chart `lam` wide: 0 the tightest, 1 as the chart was drawn, up to FIT_STRETCH roomier. */
  function fitKnobs(lam, s, V) {
    const lo = Math.min(lam, 1), up = Math.max(lam, 1), mix = (a, b) => a + (b - a) * lo;
    const pad1 = Math.max(14, s * .55), day1 = 150 * V.t, idle1 = Math.max(26, 34 * V.t);
    return { s, lo, up, g: mix(2, clamp(s * .14, 5, 12)), c: mix(FIT_CLOSE, 1) * up, pad: mix(Math.max(3, pad1 * .25), pad1) * up, dayMin: mix(day1 * .3, day1) * up, idle: mix(idle1 * .6, idle1) * Math.sqrt(up) };
  }
  /** The day columns and every seal's x, for one spacing: cols (the days, the idle ones between), xs (the seals), nowX, gx (the steps to come), width. */
  function buildChart(days, ghostLanes, V, k) {
    const { s, g, c, pad } = k, step = c * (s + g), cols = [], xs = [];
    let x = 0, alt = 0;
    days.forEach(d => {
      // (an idle day's name always fits its own column: it is centred in it, and never runs over the day beside it)
      const idle = (label, w0) => { const w = Math.ceil(Math.max(w0, label.length * .74 * V.fs.day + 10)); cols.push({ idle: 1, x, w, label }); x += w; };
      const tight = k.lo < .45;   // (a chart closed up this far names its days and its idle stretches short: THU 1, 3d)
      if (d.gap > 2) idle(tight ? d.gap + "d" : d.gap + " days", k.idle + 22 * k.lo);
      else for (let q = 1; q <= d.gap; q++) idle(DAYN[new Date(midnight(d.prev) + q * 864e5 + 36e5 * 12).getDay()], k.idle);
      const rel = placeRun(d.lanes, s, g, c, pad + s / 2, V.laneH);
      const named = (tight ? d.short : d.chars) * .74 * V.fs.day, said = d.span.length * .62 * V.fs.small, head = V.dhx * 1.57 + (V.showSmall ? Math.max(named, said) : named);
      const w = Math.ceil(Math.max(k.dayMin, head, (rel.length ? rel[rel.length - 1] + s / 2 : 0) + pad * .8));
      cols.push({ x, w, at: d.at, evs: d.evs, alt: alt++ % 2, span: d.span, short: tight });
      for (const r of rel) xs.push(Math.round(x + r));
      x += w;
    });
    const nowX = Math.round(xs.length ? xs[xs.length - 1] + Math.max(s / 2 + 5, .75 * step) : pad + s / 2);
    const gr = placeRun(ghostLanes, s, g, c, s / 2 + 8, V.laneH), gx = gr.map(v => Math.round(nowX + v));
    const end = (gx.length ? gx[gx.length - 1] + s / 2 : nowX) + Math.max(10, pad * .6);
    return { cols, xs, nowX, gx, step, half: Math.max(s / 2 + 3, step / 2), width: Math.ceil(Math.max(x, end)) };
  }
  /** The widest spacing, then the largest seals, that fit `availW`: → buildChart's answer with the seal size (s), how open the spacing is (lam) and `over` when even the smallest seals do not fit. */
  function fitChart(days, ghostLanes, V, availW) {
    const need = (lam, s) => buildChart(days, ghostLanes, V, fitKnobs(lam, s, V)).width;
    let s = V.s, lam = 0;
    if (need(0, s) > availW) {   // closed up as far as it goes and still too wide: smaller seals
      let lo = FIT_FLOOR, hi = s;
      if (need(0, lo) <= availW) for (let i = 0; i < 12; i++) { const mid = (lo + hi) / 2; if (need(0, mid) <= availW) lo = mid; else hi = mid; }
      s = lo;
    } else if (need(FIT_STRETCH, s) <= availW) lam = FIT_STRETCH;
    else { let lo = 0, hi = FIT_STRETCH; for (let i = 0; i < 16; i++) { const mid = (lo + hi) / 2; if (need(mid, s) <= availW) lo = mid; else hi = mid; } lam = lo; }
    s = Math.floor(s * 2) / 2;
    const B = buildChart(days, ghostLanes, V, fitKnobs(lam, s, V));
    return Object.assign(B, { s, lam, over: B.width > availW + 1 });
  }
  const laneY = (k, V) => V.top + (LANE[k] || LANE.office).i * V.laneH + V.laneH / 2;
  const pathD = pts => { let d = ""; pts.forEach((e, i) => { if (!i) { d = `M${e.x} ${e.y}`; return; } const p = pts[i - 1], mx = (p.x + e.x) / 2; d += ` C${mx} ${p.y} ${mx} ${e.y} ${e.x} ${e.y}`; }); return d; };

  /* ════ one order's timeline, read once and shared (adversarial wave 2) ════
     feed(orderId, { pollMs }) → { orderId, answer, error, loading, refresh({ force }), nudge(), subscribe(fn(kind)) → off, destroy() }
     The order view's Overview, header rail and Timeline tab each read the order's timeline for themselves (three
     timelineGet an open, a fourth at every return to the tab, and two polls): they now take one feed (mount's
     opts.feed). A refresh while one is on its way joins it; one not forced reuses an answer younger than the poll (the
     feed is live), so a return to the tab reads nothing; Retry forces. While anyone listens it reads again every pollMs
     (POLL_OPEN, 2.5 s: only an open order view has a feed) with the tab visible; kinds: "wait", "data", "error".
     Paul, 3 Oct 04:03 (an engraving approved took 30 s to reach the open view: its own record had to be saved first, and
     the view then waited out a 20 s poll): what the view shows now follows what is done, within 2-3 s.
       · nudge() says something changed here: read at once, and again POLL_AGAIN later for the server's own copy. A read
         already on its way may be older than the change, so another follows it (never two at once, never closer than
         POLL_GAP). Every event this page records for the order nudges it (OrderTimeline.onRecord), and is kept in the
         answer (`pending`) until the server's answer has the same key, so a read that raced its delivery never takes a
         seal away. The page's writes nudge it too (OrderWin.nudge).
       · polls and nudges read quietly (no "wait" once something is drawn: no spinner every 2.5 s); a failed read backs the
         poll off (x2 up to POLL_FAIL) and is said (kind "error") only when nothing is drawn yet or it failed 3 times.
       · an answer whose derived steps are missing (its derivation timed out, a query failed) is not drawn over the one
         before it: it would take their seals away until the next read (twice at most; then it is the answer).
     destroy() stops it, and an answer still on its way is dropped. */
  const keyOfRaw = x => { if (!x || typeof x !== "object" || !x.type) return ""; const at = Number(x.at) || 0, type = String(x.type); return type + "~" + String(x.id || `${at}-${type}`).split("~").pop().replace(/[^\w.:-]/g, "_"); };
  const partial = j => !!(j && j.derived && (j.derived.timedOut || (Array.isArray(j.derived.errors) && j.derived.errors.length)));
  const openPoll = () => { const v = +(root.OrderTimelineUI && root.OrderTimelineUI.pollOpenMs); return v >= 250 ? v : POLL_OPEN; };   // (pollOpenMs: tests only)
  function feed(orderId, o) {
    o = o || {};
    const id = digits(orderId), pollMs = Math.max(250, +o.pollMs || openPoll()), gap = Math.min(POLL_GAP, pollMs / 2), subs = new Set(), local = new Map();
    const F = { orderId: id, answer: null, error: "", loading: null, at: 0, tried: 0, began: 0, fails: 0, skips: 0, dirty: false, dead: false, rev: null, fullAt: 0, moved: Date.now(), fullNext: false };
    let pollT = 0, againT = 0, seq = 0, unrec = null, slow = 1;
    const fullEvery = () => { const v = +(root.OrderTimelineUI && root.OrderTimelineUI.fullEveryMs); return v >= 250 ? v : FULL_EVERY; };   // (fullEveryMs: tests only)
    /** How many polls apart the next read is: 1 while the page is in use or something moved lately, more as it sits unused. */
    const pace = () => { const idle = Date.now() - Math.max(lastTouch, F.moved); for (const [ms, k] of IDLE_STEPS) if (idle < ms) return k; return 1; };
    const emit = kind => { for (const fn of [...subs]) { try { fn(kind, F); } catch (err) { warn("feed", err); } } };
    const seen = () => doc.visibilityState !== "hidden";
    /** The next read in `ms` (by default pollMs after the last one began, and a longer wait after failures). */
    function arm(ms) {
      clearTimeout(pollT); pollT = 0;
      if (F.dead || !subs.size) return;
      slow = ms != null || F.fails ? 1 : pace();
      const every = pollMs * slow, wait = ms != null ? ms : F.fails ? Math.min(POLL_FAIL, pollMs * 2 ** Math.min(F.fails, 5)) : F.began ? Math.max(gap, every - (Date.now() - F.began)) : every;
      // (a poll asks the cheap question; one that follows a change on this page, or comes while this page holds events the server has not answered for, reads in full)
      pollT = setTimeout(() => { pollT = 0; if (!F.dead && seen()) { if (F.dirty) F.fullNext = true; F.dirty = false; F.refresh({ force: true, quiet: true, probe: true }); } }, wait);
    }
    function done(my, fn) {
      if (F.dead || my !== seq) return F.answer;
      F.loading = null; F.tried = Date.now(); fn();
      if (F.dirty && !F.dead && seen()) { F.dirty = false; F.fullNext = true; arm(Math.max(0, F.began + gap - Date.now())); } else arm();   // (a change came while it was reading: the next read follows at once, in full)
      return F.answer;
    }
    /** The answer with this page's own events the server has not answered for (a read can pass them in flight). */
    function withLocal(j) {
      if (!local.size) return j;
      const evs = Array.isArray(j.events) ? j.events : [], have = new Set(evs.map(keyOfRaw)), now = Date.now(), extra = [];
      for (const [k, x] of local) { if (have.has(k) || now - x.live > LOCAL_TTL) local.delete(k); else extra.push(x.ev); }
      return extra.length ? Object.assign({}, j, { events: evs.concat(extra).sort((a, b) => (+a.at || 0) - (+b.at || 0)) }) : j;
    }
    F.refresh = r => {
      if (F.dead) return Promise.resolve(F.answer);
      if (F.loading) return F.loading;
      if (!(r && r.force) && F.answer && Date.now() - F.at < pollMs) return Promise.resolve(F.answer);
      const my = ++seq, api = root.OrderTimeline;
      F.began = Date.now();
      // the cheap question: only for a poll, with a revision in hand, an answer drawn, nothing of this page's own waiting for the server's copy, and a whole read not due
      const ask = r && r.probe && !F.fullNext && F.rev && F.answer && !local.size && F.began - F.fullAt < fullEvery() * Math.min(4, slow || 1) ? { ifRev: F.rev } : { wantRev: true };
      F.fullNext = false;
      // (deferred: a throw before the read still reaches the subscribers after "wait", never before it)
      const p = F.loading = Promise.resolve().then(() => {
        if (!id) throw new Error("no order number");
        if (!api || typeof api.get !== "function") throw new Error("the timeline is not loaded on this page");
        return api.get(id, ask);
      }).then(j => done(my, () => {
        F.fails = 0; F.error = "";
        if (j && j.unchanged && ask.ifRev) { F.skips = 0; F.at = F.tried; return; }   // (nothing it was made of has moved: the view keeps what it draws, nothing is redrawn)
        if (partial(j) && F.answer && F.skips < 2) { F.skips++; F.rev = null; return; }   // (what is drawn keeps its seals; the next read tries again)
        const rev = !partial(j) && j && typeof j.rev === "string" ? j.rev : null;
        if (rev !== F.rev || !F.rev) F.moved = Date.now();
        F.rev = rev; F.fullAt = F.began;
        F.skips = 0; F.answer = withLocal(j || {}); F.at = F.tried; emit("data");
      }), e => done(my, () => { F.fails++; F.error = String((e && e.message) || e || "failed"); if (!F.answer || F.fails >= 3) emit("error"); }));
      if (!(r && r.quiet) || !F.answer) emit("wait");
      return p;
    };
    /** A read now, if the view is seen and none is on its way (one on its way is followed by another). */
    function want() {
      if (F.dead || !subs.size) return;
      F.fullNext = true;   // (something changed on this page: the read that follows is a whole one, never the cheap question)
      if (!seen() || F.loading) { F.dirty = true; return; }
      const wait = F.began + gap - Date.now();
      if (wait > 0) { arm(wait); return; }
      F.refresh({ force: true, quiet: true });
    }
    /** Something changed on this page (an event recorded, a write to the cloud): read now, and again for the server's copy. */
    F.nudge = () => {
      if (F.dead || !subs.size) return;
      F.moved = Date.now();
      want();
      clearTimeout(againT); againT = setTimeout(() => { againT = 0; want(); }, POLL_AGAIN);
    };
    F.subscribe = fn => { subs.add(fn); if (!pollT && !F.loading) arm(); return () => { subs.delete(fn); if (!subs.size) { clearTimeout(pollT); pollT = 0; clearTimeout(againT); againT = 0; } }; };
    const onVis = () => {
      if (F.dead || !subs.size) return;
      if (!seen()) { clearTimeout(pollT); pollT = 0; return; }
      if (!F.loading && (F.dirty || Date.now() - F.tried >= pollMs)) { if (F.dirty) F.fullNext = true; F.dirty = false; F.refresh({ force: true, quiet: true, probe: true }); } else if (!F.loading && !pollT) arm();   // (the tab shown again asks the cheap question: whatever moved while it was hidden moved the digest)
    };
    const onNet = () => { F.fails = 0; want(); };   // (the network is back: the poll waits no longer)
    // the page is touched while the poll is slowed: the next read is due at the normal pace, not at the slow one
    const wake = () => {
      if (F.dead || !subs.size || slow <= 1 || F.loading || !pollT || !seen()) return;
      arm();
    };
    wakers.add(wake);
    const onRec = x => {
      if (F.dead || !x || digits(x.orderId) !== id) return;
      const k = keyOfRaw(x); if (!k) return;
      local.set(k, { live: Date.now(), ev: Object.assign({ pending: true }, x) });
      if (local.size > 200) local.delete(local.keys().next().value);
      F.nudge();
    };
    doc.addEventListener("visibilitychange", onVis); root.addEventListener("online", onNet);
    try { if (root.OrderTimeline && typeof root.OrderTimeline.onRecord === "function") unrec = root.OrderTimeline.onRecord(onRec); } catch (_) {}
    F.destroy = () => {
      if (F.dead) return; F.dead = true; clearTimeout(pollT); pollT = 0; clearTimeout(againT); againT = 0; subs.clear(); local.clear(); wakers.delete(wake);
      doc.removeEventListener("visibilitychange", onVis); root.removeEventListener("online", onNet);
      try { if (typeof unrec === "function") unrec(); } catch (_) {}
      unrec = null;
    };
    return F;
  }

  /* ════ mount ════ */
  function mount(el, opts) {
    opts = opts || {};
    if (!el || typeof el.appendChild !== "function") throw new Error("OrderTimelineUI.mount needs an element");
    css();
    const orderId = digits(opts.orderId), live = opts.live !== false, compact = !!opts.compact;
    // a shared feed (the order view): its reads, its poll; this mount keeps its own live stamps (onRecord) alone
    const src = opts.feed && typeof opts.feed.subscribe === "function" && opts.feed.orderId === orderId ? opts.feed : null;
    const pollMs = Math.max(250, +opts.pollMs || POLL);   // pollMs: tests only
    /* this order's own steps (opts.stages: an array, or a function asked at every redraw, e.g. stagesFor(its lines));
       inside the component they stand in for the full rail, so an order with no stud earring has no Welded step (one
       that was welded all the same keeps it: what happened is always drawn) */
    const withDone = keepDone;
    const railNow = evs => { let r = null; try { r = typeof opts.stages === "function" ? opts.stages() : opts.stages; } catch (err) { warn("stages", err); } return withDone(r, evs); };
    // events/byKey: what is drawn (one piece's, or all); every/allKeys: all the order's (opts.pieces, opts.piece: agent F)
    const S = { events: [], shown: [], shownKeys: new Set(), byKey: new Map(), every: [], allKeys: new Map(), pieces: Array.isArray(opts.pieces) ? opts.pieces : [], piece: opts.piece || null, cancelled: null, where: null, D: null, mark: null, markStage: null, hl: new Set(), sig: "",
      stamping: null, deferredPaint: null, loaded: false, loading: null, error: "", dead: false, lastLoad: 0, seq: 0, nowX: 0, pendingFocus: null, hlDone: false };
    // where the piece IS now (opts.placement(key|null) -> the page's PiecePlacement; null asks for the order's: one piece's, or the roll-up of all); see derive
    const plaOf = k => { try { return typeof opts.placement === "function" ? opts.placement(k == null ? null : k) || null : null; } catch (err) { warn("placement", err); return null; } };
    const pieceSig = x => JSON.stringify(x.map(p => [p.key, p.qty, p.pools, p.sheets])) + "#" + plaSig(x);
    const plaSig = ps => [plaOf(null)].concat((ps || []).map(p => plaOf(p.key))).map(q => (q && q.sig) || "").join("|");
    const timers = new Set();
    const later = (fn, ms) => { const t = setTimeout(() => { timers.delete(t); if (!S.dead) fn(); }, ms); timers.add(t); return t; };
    const cancelT = t => { if (t) { clearTimeout(t); timers.delete(t); } return 0; };
    const box = doc.createElement("div");
    box.className = "tlUI" + (compact ? " compact" : "");
    box.setAttribute("data-order", orderId);
    box.innerHTML = `<div class="tlTop"><div class="tlNow"><span class="tlLbl">Now</span><div class="tlNowT">Finding where this order is…</div><div class="tlNowS"></div></div>` +
      `<div class="tlRail" role="group" aria-label="Main steps"><span class="tlTrack"></span><span class="tlFill"></span><div class="tlStops"></div></div></div>` +
      `<div class="tlBar"><div class="tlBarR"><span class="tlBusy" hidden><i class="tlSpin"></i><span>Checking for new steps</span></span><span class="tlLive" hidden><i></i>LIVE</span><span class="tlSum"></span></div></div>` +
      `<div class="tlGrid"><div class="tlLanes"></div><div class="tlScroll"><div class="tlCanvas"></div></div><div class="tlMsg" hidden></div></div>` +
      `<div class="tlExp" role="tooltip"></div>`;
    el.appendChild(box);
    // opts.toolbar: the host's own bar (the order view's tab row, spec §1) takes the live badge and the summary, so the lanes keep the height
    const tb = !compact && opts.toolbar && typeof opts.toolbar.appendChild === "function" ? opts.toolbar : null, bar = box.querySelector(".tlBar");
    if (tb) { bar.classList.add("inTools"); tb.appendChild(bar); }
    const $ = s => box.querySelector(s) || (tb ? bar.querySelector(s) : null), $$ = s => [...box.querySelectorAll(s)];
    if (compact) $(".tlRail").appendChild($(".tlMsg"));   // the rail alone: its wait and error lines sit on it
    const scroller = $(".tlScroll"), exp = $(".tlExp"), gridEl = $(".tlGrid");
    let fitObserver = null;
    let unsub = null, unfeed = null, pollT = 0, busyT = 0, zoomFor = null;
    // what the lanes' names show now: a redraw that would write the same leaves them (and their layout) alone
    let lanesHtml = "";
    const pub = e => pubOf(e, orderId);

    /* ── loading ── */
    function message(kind, text) {
      const m = $(".tlMsg");
      if (!kind) { m.hidden = true; m.innerHTML = ""; return; }
      m.hidden = false; m.className = "tlMsg" + (kind === "err" ? " err" : "");
      m.innerHTML = kind === "wait" ? `<i class="tlSpin"></i><span>${esc(text)}</span>` : kind === "err" ? `<span>${esc(text)}</span><button type="button" class="btn ghost sm tlRetry">Retry</button>` : `<span>${esc(text)}</span>`;
    }
    function busy(on) { const b = $(".tlBusy"); if (b) b.hidden = !on; }
    function waiting() {
      if (!S.loaded) {
        message("wait", compact ? "Loading the steps…" : `Loading the timeline of order ${orderId || "—"}…`);
        const ns = $(".tlNowS"); ns._h = null; ns.innerHTML = `<i class="tlSpin"></i><span>Loading the timeline</span>`;
      } else if (!busyT) busyT = later(() => busy(true), 350);
    }
    /** The shared feed's news: its wait, its answer (merged as this mount's own would be), its failure. */
    function onFeed(kind) {
      if (S.dead) return;
      if (kind === "wait") { waiting(); return; }
      busyT = cancelT(busyT); busy(false); S.lastLoad = Date.now();
      if (kind === "data") { S.error = ""; apply(src.answer || {}); } else if (kind === "error") { S.error = src.error || "failed"; failed(); }
    }
    function load(force) {
      if (S.dead) return Promise.resolve();
      if (src) return src.refresh({ force: force !== false }).then(() => undefined, () => undefined);
      if (S.loading) return S.loading;
      const my = ++S.seq;
      waiting();
      const api = root.OrderTimeline;
      S.loading = (async () => {
        try {
          if (!orderId) throw new Error("no order number");
          if (!api || typeof api.get !== "function") throw new Error("the timeline is not loaded on this page");
          const j = await api.get(orderId);
          if (S.dead || my !== S.seq) return;
          S.error = "";
          apply(j || {});
        } catch (e) {
          if (S.dead) return;
          S.error = String((e && e.message) || e || "failed");
          failed();
        } finally {
          busyT = cancelT(busyT); if (!S.dead) busy(false);
          S.loading = null; S.lastLoad = Date.now();
        }
      })();
      return S.loading;
    }
    function failed() {
      if (!S.loaded) {
        message("err", compact ? "Couldn't load the steps" : `Couldn't load the timeline: ${S.error}.`);
        $(".tlNowT").textContent = "Couldn't load the timeline";
        const ns = $(".tlNowS"); ns._h = null; ns.innerHTML = `<span>${esc(S.error)}</span><button type="button" class="tlLink tlRetry">Retry</button>`;
      } else {
        const r = $(".tlSum"); if (r) r.innerHTML = `<span class="err">Couldn't check for new steps <button type="button" class="tlLink tlRetry">Retry</button></span>`;
      }
    }
    /** The server's answer, merged with what this page recorded meanwhile (still on its way to the server). */
    function apply(j) {
      const next = new Map();
      // (an event the answer repeats as it was keeps its own object: the open view's answer comes every 2.5 s, and what was
      // derived from the events at the last redraw (S.D, the explainer's "completed by hand" one) must still be them)
      for (const x of Array.isArray(j.events) ? j.events : []) { const e = norm(x); if (e) { const o = S.allKeys.get(e.key); next.set(e.key, o && sameFacts(o, e) ? o : e); } }
      for (const [k, e] of S.allKeys) if (e.live && !next.has(k) && Date.now() - e.live < 180000) next.set(k, e);
      const first = !S.loaded, fresh = first ? [] : [...next.keys()].filter(k => !S.allKeys.has(k));
      S.allKeys = next; S.every = [...next.values()].sort(byAt); narrow();
      S.cancelled = j.cancelled || null; S.where = j.where || null; S.loaded = true; S.truncated = !!j.truncated; S.leftOut = j.leftOut || null;
      const sig = sigOf();
      if (!first && sig === S.sig) { paintNowSub(); paintSum(); return; }
      S.sig = sig;
      message(null);
      repaint({ first, fresh });
      if (first) afterFirst();
    }
    /** What is drawn, in one string: an answer that says the same repaints nothing (a stamp still dropping keeps going). */
    function sigOf() {
      return S.every.map(e => e.key + (e.pending ? "*" : "")).join("|") + "#" + (S.cancelled ? S.cancelled.at || 1 : 0) + "#" + JSON.stringify(S.where || "") + "#" + plaSig(S.pieces);
    }
    function afterFirst() {
      if (S.pendingFocus != null) { const id = S.pendingFocus; S.pendingFocus = null; if (focus(id)) return; }
      if (S.hlDone) return; S.hlDone = true;
      // (a search opens an order with highlight: true, "light its number": no event is asked for, so no seal is ringed)
      const h = typeof opts.highlight === "string" || typeof opts.highlight === "number" ? String(opts.highlight).trim() : "";
      if (!h || digits(h) === orderId) return;
      const k = findKey(h);
      if (k) { mark(k, { scroll: true }); return; }
      const q = h.toLowerCase(), hits = S.events.filter(e => hay(e).includes(q));
      if (!hits.length) return;
      S.hl = new Set(hits.map(e => e.key));
      for (const b of $$(".tlSt[data-key]")) b.classList.toggle("hl", S.hl.has(b.dataset.key));
      mark(hits[hits.length - 1].key, { scroll: true });
    }

    /* ── painting ── */
    /** The widths a redraw fits its seals to, read once before it writes anything: a read between its writes makes the page
     *  work out every style those writes have changed, again for each read, and a live step then no longer fits a frame. */
    function measure() {
      const rail = $(".tlRail"), wrap = rail && rail.querySelector(".tlStops"), host = rail && rail.closest(".tlUI.compact");
      S.M = { stops: wrap ? wrap.clientWidth || rail.clientWidth : 0, host: host ? host.clientHeight : 0, scroll: scroller.clientWidth, grid: compact ? { w: 0, h: 0 } : gridSize() };
    }
    /** The room the chart has: its grid's inner size, which its host decides (never what is drawn in it). */
    const gridSize = () => ({ w: gridEl.clientWidth, h: gridEl.clientHeight });
    /** Writes what one measure of the room decided (fitVertical) as the sizes the chart's styles read: the scale, the bands, the lanes, the text. */
    let fitSig = "";
    function applyFit(V) {
      const sig = [V.h, V.w, V.t, V.labelW, V.top, V.axis, V.laneH, V.showSub, V.showSmall].join("|");
      if (sig === fitSig) return; fitSig = sig;
      const st = box.style, set = (k, v) => st.setProperty(k, v), px = n => r2(n) + "px";
      set("--tl-t", String(r2(V.t))); set("--tl-label-w", px(V.labelW)); set("--tl-top", px(V.top)); set("--tl-axis", px(V.axis)); set("--tl-lane-height", px(V.laneH));
      set("--tl-fs-lane", px(V.fs.lane)); set("--tl-fs-sub", px(V.fs.sub)); set("--tl-fs-day", px(V.fs.day)); set("--tl-fs-small", px(V.fs.small)); set("--tl-fs-pill", px(V.fs.pill));
      set("--tl-ic", px(V.ic)); set("--tl-padx", px(V.padx)); set("--tl-dhx", px(V.dhx)); set("--tl-dhy", px(V.dhy));
      box.classList.toggle("tlNoSub", !V.showSub); box.classList.toggle("tlNoSmall", !V.showSmall);
    }
    function repaint(o) {
      o = o || {};
      if (S.stamping) {
        const prev = S.deferredPaint || {}; S.deferredPaint = Object.assign({}, prev, o, { fresh: [...new Set((prev.fresh || []).concat(o.fresh || []))] });
        return;
      }
      measure();
      sealHover.cancel(true); // a redraw cannot preserve hover just because a larger parent still matches :hover
      const D = S.D = deriveNow(); S.psig = pieceSig(S.pieces);
      // every event stays in S.events (the host, and the step explainer, read them all); only the seals are drawn
      S.shown = withPrints(S.events); S.shownKeys = new Set(S.shown.map(e => e.key));
      const now = Date.now(), pressKeys = (o.fresh || []).filter(k => { const e = S.byKey.get(k); return e && (e.live || e.pending || (!e.derived && +e.at >= now - 300000 && +e.at <= now + 60000)); });
      // A newly discovered old record gains its historical face silently; only a fresh action gets a physical press.
      const paint = Object.assign({}, o, { pressKeys });
      paintNow(D, paint); const railPresses = paintRail(D, paint);
      tell();
      if (!compact) { paintCanvas(D, paint); paintSum(); paintEmpty(); }
      const fresh = (o.fresh || []).filter(k => S.shownKeys.has(k));
      const presses = compact ? railPresses : pressKeys.filter(k => S.shownKeys.has(k)).map(k => $(".tlCanvas").querySelector(`.tlSt[data-key="${cssEsc(k)}"]`)).filter(Boolean);
      // a new stamp past the right edge comes into view once its press is done, when the reader was looking at the end of the chart
      // (the stamp before it was in view); nothing is selected or opened
      const follow = () => {
        if (S.dead || compact || !fresh.length) return;
        const newest = fresh.map(k => S.byKey.get(k)).filter(Boolean).sort(byAt).pop(), prev = S.shown.filter(e => !fresh.includes(e.key)).pop();
        if (newest && prev && prev.x != null && prev.x >= scroller.scrollLeft && prev.x <= scroller.scrollLeft + scroller.clientWidth) scrollToEv(newest);
      };
      if (presses.length && root.Seal && typeof root.Seal.press === "function") {
        for (const target of presses) target.classList.add("pending");
        S.stamping = Promise.resolve().then(async () => {
          for (const target of presses) { if (S.dead) break; if (target.isConnected) { try { await root.Seal.press(target); } finally { target.classList.remove("pending"); } } }
        }).catch(err => warn("seal press", err)).finally(() => {
          S.stamping = null;
          if (S.dead) return;
          const deferred = S.deferredPaint; S.deferredPaint = null;
          follow();
          if (deferred) repaint(deferred);
        });
      } else follow();
    }
    /** An order with no seal yet says so over the chart, in one quiet line (its lanes and the dashed steps stay). */
    function paintEmpty() {
      const g = $(".tlGrid"), had = g && g.querySelector(":scope > .tlEmpty"); if (!g) return;
      if (S.shown.length) { if (had) had.remove(); return; }
      const t = S.events.length ? "No milestone yet. The first seal lands here the moment this order reaches one." : "Nothing is recorded for this order yet. Each step shows here the moment it happens.";
      if (had) { if (had.textContent !== t) had.textContent = t; return; }
      const p = doc.createElement("p"); p.className = "tlEmpty"; p.textContent = t; g.appendChild(p);
    }
    /** One piece: its own events and steps. All pieces of an order of several: the order is where its slowest piece is,
     *  on the steps any of its pieces takes, each counted. One piece only: as it always was. */
    const pieceNow = () => (S.piece && S.pieces.find(p => p.key === S.piece)) || null;
    function narrow() {
      const p = pieceNow();
      if (!p) { S.piece = null; S.events = S.every; S.byKey = S.allKeys; return; }
      S.events = S.every.filter(e => ofPiece(e, p, S.pieces)); S.byKey = new Map(S.events.map(e => [e.key, e]));
    }
    function deriveNow() {
      const p = pieceNow();
      if (p) return railed(derive(S.events, S.cancelled, null, null, plaOf(p.key)), withDone(stagesFor(p.line), S.events));
      const D = derive(S.events, S.cancelled, whereNow(), null, plaOf(null));
      if (S.pieces.length < 2) return railed(D, railNow(S.events));
      const sum = summary(S.every, S.pieces, S.cancelled, plaOf), far = D.step;
      if (D.hand && !sum.each.every(x => x.D.hand)) D.hand = null;   // (completed by hand: every piece of it)
      D.step = Math.min(D.step, sum.step);
      // a step that every piece taking it has passed is not what the order waits on (a stud's Welded, done, while the
      // necklaces wait on Assembled): the next step is the first one some piece still has to reach
      for (const r of sum.rail) { if (r.i <= D.step) continue; if (r.of && r.n >= r.of && r.i <= far) D.step = r.i; else break; }
      return railed(D, sum.rail.map(r => r.s), sum);
    }
    /** The host's piece switcher: pieces (as opts.pieces; omitted keeps them) and the piece shown (null: all of them).
     *  The rail and the lanes cross over to it, from the side it lies on (dir). */
    function setPieces(list, key, dir) {
      if (S.dead) return;
      const ps = Array.isArray(list) ? list : S.pieces;
      key = key || null;
      // (the pieces AND where each is now: a piece taken off its sheet changes the rail with no change of pieces; S.psig is what was drawn last)
      const moved = key !== S.piece, changed = pieceSig(ps) !== S.psig;
      if (!moved && !changed) return;
      S.pieces = ps; S.piece = key; narrow();
      if (moved || (S.mark && !S.byKey.has(S.mark))) { S.mark = null; S.markStage = null; }   // (the ring was round another piece's stamp)
      if (!S.loaded) return;
      hideZoom(true);
      repaint({ first: moved });
      if (moved) for (const x of [$(".tlRail"), compact ? null : $(".tlGrid")]) if (x) anim(x, [{ opacity: 0, transform: `translateX(${(dir || 0) * 14}px)` }, { opacity: 1, transform: "none" }], 340, { easing: E });
    }
    /** The server's `where`; while this page has steps the server has not answered for yet (live, or still in the
     *  outbox) it is worked out here from every step, never behind the server's own. */
    function whereNow() {
      const srv = S.where && typeof S.where === "object" ? S.where : null;
      if (srv && !S.events.some(e => e.pending || e.live)) return srv;
      // as the server's: with its answer in, the cancel record says whether the order is cancelled (a restore whose
      // cancelRestored event was not written leaves a cancel in the history); a cancel recorded here since counts too
      const lastCx = S.events.filter(e => CANCEL_TYPES.has(e.type) || e.type === "cancelRestored").pop();
      const liveCx = lastCx && CANCEL_TYPES.has(lastCx.type) && (lastCx.live || lastCx.pending) ? lastCx : null;
      const local = whereOf(S.events, S.cancelled || liveCx, { record: !!srv });
      if (!srv) return typeof S.where === "string" && S.where ? Object.assign(local, { label: S.where, text: S.where }) : local;
      return Object.assign({}, srv, local, { step: Math.max(Number.isFinite(+srv.step) ? +srv.step : -1, local.step) });
    }
    function nowText(D) {
      const W = D.W;
      if (D.cancelled) return { cls: "cx", t: "Cancelled — do not proceed" };
      if (D.hand) return { cls: "done", t: "Order completed by hand" };
      if (!S.events.length) return { cls: "", t: "Nothing recorded yet" };
      if (D.hold) { const r = reasonOf(D.hold) || D.hold.text; return { cls: "hold", t: "On hold" + (r ? " — " + r.slice(0, 80) : "") }; }
      return { cls: W.stage === "completed" ? "done" : "", t: String(W.label || W.text || "—").slice(0, 140) };
    }
    function paintNow(D, o) {
      const nt = nowText(D), n = $(".tlNow"), T = $(".tlNowT");
      n.className = "tlNow" + (nt.cls ? " " + nt.cls : "");
      if (T.textContent !== nt.t) { T.textContent = nt.t; if (!o.first) anim(T, [{ opacity: 0, transform: "translateY(4px)" }, { opacity: 1, transform: "none" }], 320); }
      paintNowSub();
    }
    /** The host's say on a sheet link (opts.sheetLink(sheetId, poolId) → { piece, off } or nothing): which piece it is about (named in the
     *  strip when the order has several), and, when that piece is not on that sheet, why: the link is greyed and inert and the host says the reason. */
    const linkSay = (sheetId, pool) => { try { return typeof opts.sheetLink === "function" ? opts.sheetLink(String(sheetId || ""), pool || null) || null : null; } catch (_) { return null; } };
    const openBtn = (cls, sheetId, pool, h) => `<button type="button" class="${cls}${h && h.off ? " off" : ""}" data-sheet="${esc(sheetId)}" data-pool="${esc(pool)}"${h && h.off ? ` aria-disabled="true" data-why="${esc(h.off)}" title="${esc(h.off)}"` : ""}>Open sheet</button>`;
    function paintNowSub() {
      const D = S.D; if (!D) return;
      const s = $(".tlNowS");
      // (written only when it changed: an answer that repeats every 2.5 s must not rebuild "Open sheet" under a click)
      const put = h => { if (s._h !== h) { s._h = h; s.innerHTML = h; } };
      if (D.cancelled) {
        const c = D.cancelled, who = c.source === "etsy" || c.by === "Etsy" ? "on Etsy" : "by " + (c.by || "a person");
        put(`<span>Cancelled ${esc(who)} · ${esc(shortWhen(c.at))} · ${esc(ago(c.at))}</span>${c.why ? `<span class="why" title="${esc(c.why)}">${esc(c.why)}</span>` : ""}`);
        return;
      }
      const W = D.W, e = D.last; if (!e) { put(""); return; }
      const st = W.station || e.station || "", at = +W.at || e.at;
      const who = { station: st, by: W.by || whoOf(e), lane: STATION_LANE[st] || e.lane, source: e.source };
      const pool = W.sheetId ? S.events.filter(x => x.sheetId === W.sheetId).map(poolOf).filter(Boolean).pop() || "" : "";
      const hl = W.sheetId ? linkSay(W.sheetId, pool) : null;
      put(`${badge(who, true)}<span>${esc(shortWhen(at))} · ${esc(ago(at))}</span>` +
        (W.sheetId ? `<span>${hl && hl.piece ? esc(hl.piece) + " " : ""}on ${esc(W.sheet || W.sheetId)}</span>${opts.onSheet ? openBtn("tlLink tlOpenSheet", W.sheetId, pool, hl) : ""}` : ""));
    }
    function paintRail(D, o) {
      const wrap = $(".tlStops"), rail = $(".tlRail"), R = D.rail || STAGES.map((s, i) => ({ s, i }));
      // (the steps of the piece shown, or of all of them: drawn again when they change)
      const keys = R.map(r => r.s.k).join(" ");
      if (wrap.dataset.keys !== keys) { wrap.dataset.keys = keys; rail.style.setProperty("--n", R.length); wrap.innerHTML = R.map(r => `<button type="button" class="tlStop f" data-stage="${r.s.k}"><i class="tlSeal"></i><span>${esc(r.s.l)}</span><em class="tlCnt" hidden></em></button>`).join(""); }
      fitRail(rail, S.M);
      const nodes = [...wrap.children], presses = [];
      // all pieces (Paul, 5 Oct: "only show it above the next unfinished milestone"): the count of a step some pieces reached and others
      // not yet ("2 of 6") is drawn on the next unfinished step alone, the first one, in rail order, that not every piece has reached;
      // a later step, a done step and a cancelled order show none (the step explainer and the aria-label still say how many are there)
      const sumOf = i => D.sum && D.sum.rail.find(r => r.i === i);
      const next = !S.loaded || D.cancelled || D.hand ? -1 : R.findIndex(r => { const q = sumOf(r.i); return r.i > D.step && !(q && q.of > 0 && q.n >= q.of); });
      R.forEach(({ s, i }, j) => {
        const n = nodes[j], st = D.stages[i];
        const sr = sumOf(i), part = j === next && sr && sr.n > 0 && sr.n < sr.of ? `${sr.n} of ${sr.of}` : "";
        let c;
        if (!S.loaded) c = "f";
        else if (D.cancelled) c = i < D.stop ? "d" : i === D.stop ? "x" : "f gone";
        else c = i <= D.step ? "d" : i === D.cur ? "c" : D.hand ? "f gone" : "f";   // (completed by hand: the rest skipped)
        // a step passed with no event of its own (an older order, or one done off the record) still shows as done
        const ev = c === "d" ? st.first : null;
        const sig = c + "|" + (ev ? ev.key : "") + (c === "c" && D.hold ? "|h" : "") + (c === "x" ? "|" + D.cancelled.at : "") + "|" + part + "|" + (sr ? sr.n + "/" + sr.of : "");
        if (n.dataset.sig === sig) return;
        n.dataset.sig = sig;
        n.className = "tlStop " + c + (c === "c" && D.hold ? " paused" : "");
        const seal = n.querySelector(".tlSeal"), rot = ev ? rotOf(ev) : 0;
        n.style.setProperty("--rot", rot + "deg");
        seal.innerHTML = c === "d" ? stampSvg(ev || { key: "d-" + s.k, type: s.kind, at: 0 }, !!ev) : c === "x" ? stampSvg({ key: "x-" + s.k, type: D.cancelled.source === "etsy" ? "etsyCancelled" : "cancelled", at: D.cancelled.at, by: D.cancelled.by }, true)
          : stampSvg({ key: "g-" + s.k, type: s.kind, at: 0 }, false, { ghost: 1 });
        n.querySelector("span").textContent = c === "x" ? "Cancelled" : s.l;
        const cnt = n.querySelector(".tlCnt"); if (cnt) { cnt.hidden = !part; cnt.textContent = part; }
        // (who did it and where: "Welded · Marco R. · Welding · done Tuesday, Sep 29, 2026 · 10:15 AM")
        const say = (c === "d" ? (ev ? `${[s.l, whoOf(ev)].concat(placeOf(ev) ? [placeOf(ev)] : []).join(" · ")} · done ${longWhen(ev.at)}` : `${s.l}: done`) : c === "c" ? `${s.l}: ${D.hold ? "on hold" : "next"}` : c === "x" ? `Cancelled here, ${longWhen(D.cancelled.at)}` : D.hand ? `${s.l}: skipped, the order was completed by hand` : `${s.l}: still to come`) + (sr && D.sum ? ` · ${sr.n} of ${sr.of} piece${sr.of === 1 ? "" : "s"}` : "");
        n.setAttribute("aria-label", say); n.removeAttribute("title");   // the step explainer (below) replaces the dark tooltip
        const pressEvent = ev || (c === "x" && S.events.filter(e => CANCEL_TYPES.has(e.type)).pop());
        if (compact && pressEvent && (o.pressKeys || []).includes(pressEvent.key)) presses.push(seal);
      });
      const at = D.cancelled ? D.stop : D.cur < 0 ? R[R.length - 1].i : Math.max(0, D.cur), idx = Math.max(0, R.findIndex(r => r.i === at));
      $(".tlFill").style.transform = `scaleX(${(idx / Math.max(1, R.length - 1)).toFixed(4)})`;
      rail.classList.toggle("cx", !!D.cancelled);
      let cx = rail.querySelector(".tlCxStamp:not(.out)");
      const cxK = D.cancelled ? [D.cancelled.at, D.cancelled.by, D.cancelled.source, D.stop].join("|") : "";
      const cxAt = () => {
        cx.dataset.k = cxK; cx.dataset.tlFace = JSON.stringify(cancellationFace(D.cancelled)); cx.innerHTML = cancelSvg(D.cancelled);
        cx.setAttribute("role", "img"); cx.setAttribute("aria-label", "Cancelled order · " + longWhen(D.cancelled.at) + (D.cancelled.by ? " · " + D.cancelled.by : ""));   // (who signed it: for assistive technology, with no caption)
        // over the steps it will not reach, so the ✕ where it stopped stays readable
        const from = Math.min(Math.max(0, R.findIndex(r => r.i === D.stop)) + 1, R.length - 1), mid = ((from + R.length - 1) / 2 + .5) / R.length;
        cx.style.left = `clamp(calc(var(--seal-fit,var(--seal-size,84px)) / 2), ${(mid * 100).toFixed(2)}%, calc(100% - var(--seal-fit,var(--seal-size,84px)) / 2))`;
      };
      if (D.cancelled && cx && cx.dataset.k !== cxK) cxAt();
      if (D.cancelled && !cx) {
        cx = doc.createElement("div"); cx.className = "tlCxStamp"; rail.appendChild(cx); cxAt();

      } else if (!D.cancelled && cx) {
        cx.classList.add("out");
        const a = anim(cx, [{ opacity: 1 }, { opacity: 0 }], 240);
        if (a) a.finished.then(() => cx.remove(), () => cx.remove()); else cx.remove();
      }
      return presses;
    }
    function paintSum() {
      const r = $(".tlSum"), lv = $(".tlLive"); if (!r) return;
      if (lv) lv.hidden = !live || !S.loaded;
      const e = S.shown[S.shown.length - 1] || S.events[S.events.length - 1];
      const n = S.shown.length;
      const t = e ? `${n} milestone${n === 1 ? "" : "s"}${leftOutNote()} · last ${shortWhen(e.at)} · ${whoOf(e)}` : "";
      if (r.textContent !== t) r.textContent = t;
    }
    /* What a cut-short read left out, said plainly (the server's leftOut: a type past its first 500 is not read further,
       and at most 2000 recorded events are read). Only repeats nobody would miss (scans, reads, moves) cut: nothing said. */
    function leftOutNote() {
      if (!S.truncated) return "";
      const L = S.leftOut;
      if (!L || !Array.isArray(L.types)) return " (not every recorded step could be read)";
      const seen = [...new Set(L.types.filter(t => MISSED.has(t)).map(labelOf))];
      const bits = seen.length ? [`only ${L.kept || 500} ${seen.map(l => `“${l}”`).join(", ")} steps shown, the rest left out`] : [];
      if (L.capped) bits.push("stopped at 2000 recorded steps: later ones may be missing");
      return bits.length ? ` (${bits.join("; ")})` : "";
    }
    function paintLanes() {
      const who = {}; for (const e of S.events) (who[e.lane] = who[e.lane] || new Set()).add(whoOf(e));
      const selLane = S.mark && S.byKey.get(S.mark) ? S.byKey.get(S.mark).lane : S.markStage ? (STAGES.find(x => x.k === S.markStage) || {}).lane || "" : "";   // (the lane of the ringed stamp is lit)
      const html = LANES.map(L => { const w = !who[L.k] || !who[L.k].size ? "—" : L.k === "office" ? "Operator" : [...who[L.k]].join(", "); /* (the Office lane names no one: Paul, 29 Sep, "it just should say Operator") */ return `<div class="tlLane${L.st ? " stn" : ""}${selLane === L.k ? " on" : ""}" data-lane="${L.k}"><b>${iconSvg(L.ic)}${esc(L.l)}</b><span title="${esc(w)}">${esc(w)}</span></div>`; }).join("");
      if (html !== lanesHtml) { lanesHtml = html; $(".tlLanes").innerHTML = html; }
    }
    function paintCanvas(D, o) {
      hideZoom(true);
      const cv = $(".tlCanvas"), evs = S.shown, oldNow = S.nowX;
      paintLanes();
      const pending = D.cancelled || D.hand ? [] : (D.rail || STAGES.map((s, i) => ({ s, i }))).filter(g => g.i > D.step);
      // the room, read once before anything is written (measure): every part of the chart is sized to it together
      // (measure() has read it already, before this redraw wrote anything: never read again here, a layout read in the middle of the writes
      //  makes the page work out every style they changed. A chart in a hidden tab is drawn for the last room it had; the observer redraws it when it is shown)
      const seen = S.M && S.M.grid ? S.M.grid : gridSize();
      if (seen.h) S.room = seen;
      const grid = seen.h ? seen : S.room || { w: 800, h: 360 }, resized = !S.fit || S.fit.w !== seen.w || S.fit.h !== seen.h;
      const cap = Math.min(FIT_CAP, baseSealSize(cv));
      let V = fitVertical(grid.h, grid.w, cap);
      const availW = Math.max(60, grid.w - V.labelW), days = dayModel(evs), ghostLanes = pending.map(g => (LANE[g.s.lane] || LANE.office).i);
      let P = fitChart(days, ghostLanes, V, availW);
      if (V.showSmall && P.s < V.s - .5) {   // (the day heads' second line costs the seals some size: the heads keep one line, and the room that gives back goes to the lanes, when that is the better chart)
        const V1 = fitVertical(grid.h, grid.w, cap, 1), P1 = fitChart(days, ghostLanes, V1, availW);
        if (P1.s > P.s + .5) { V = V1; P = P1; }
      }
      applyFit(V);
      const fit = P.s, last = evs[evs.length - 1], H = V.h, nowX = P.nowX;
      cv.style.setProperty("--seal-fit", fit + "px");
      evs.forEach((e, i) => { e.x = P.xs[i]; e.y = laneY(e.lane, V); });
      const ghosts = pending.map((g, j) => ({ key: "ghost-" + g.s.k, type: g.s.kind, s: g.s, x: P.gx[j], y: laneY(g.s.lane, V) }));
      const W = Math.max(availW, P.width), cols = P.cols;
      scroller.style.overflowX = P.over ? "auto" : "";   // (hidden, unless even the smallest seals of a very long history cannot fit)
      const thisYear = new Date().getFullYear();
      const dayCols = cols.map(c => { const d = c.idle ? null : new Date(c.at); return `<div class="tlDay${c.alt ? " alt" : ""}${c.idle ? " idle" : ""}" style="left:${c.x}px;width:${c.w}px"><div class="dh">${c.idle ? esc(c.label) : `${c.short ? `${DAYN[d.getDay()]} ${d.getDate()}` : `${DAYN[d.getDay()]} · ${MON[d.getMonth()]} ${d.getDate()}${d.getFullYear() !== thisYear ? " " + d.getFullYear() : ""}`}<small>${esc(c.span)}</small>`}</div></div>`; }).join("");
      const lines = LANES.map((L, i) => `<div class="tlLaneLine" style="top:${V.top + (i + 1) * V.laneH}px"></div>`).join("");
      const future = ghosts.length && last ? pathD([last].concat(ghosts)) : "";
      // the cancel line: at the cancel stamp (or, with only the record, where its time falls)
      let cxX = 0;
      if (D.cancelled) { const ce = evs.filter(e => CANCEL_TYPES.has(e.type)).pop(); cxX = ce ? ce.x + P.half : ((evs.filter(e => e.at <= D.cancelled.at).pop() || { x: nowX - P.step * .75 }).x + P.half); }
      const Wh = D.W, stn = Wh.station && !["sheet", "waiting", "review", "held"].includes(Wh.stage) ? STATION_NAME[Wh.station] || Wh.station : "";
      const nowLbl = D.hand ? ["COMPLETED", shortWhen(D.hand.at)].concat(personOf(D.hand) ? [personOf(D.hand).toUpperCase()] : []).join(" · ") : D.hold ? "NOW · ON HOLD" : Wh.stage === "completed" || (D.cur < 0 && last) ? "COMPLETED" : stn ? "NOW · AT " + stn.toUpperCase() : last ? "NOW · " + String(Wh.label || "").toUpperCase() : "NOW";
      cv.style.width = W + "px"; cv.style.height = H + "px";
      const back = dayCols + lines +
        `<svg class="tlPath" width="${W}" height="${H}" aria-hidden="true">${evs.length > 1 ? `<path d="${pathD(evs)}" fill="none" stroke="var(--gold)" stroke-width="${V.route}" stroke-opacity=".55" stroke-linecap="round"/>` : ""}${future ? `<path d="${future}" fill="none" stroke="var(--ink25)" stroke-width="${V.future}" stroke-dasharray="${r2(3 * V.t)} ${r2(5 * V.t)}"/>` : ""}</svg>` +
        (D.cancelled ? `<div class="tlAfterCx" style="left:${cxX}px"></div><div class="tlNowLine cx" style="left:${cxX}px"><span>CANCELLED · ${esc(shortWhen(D.cancelled.at))}</span></div>` : `<div class="tlNowLine" style="left:${nowX}px"><span>${esc(nowLbl)}</span></div>`);
      const clsOf = e => `tlSt${S.mark === e.key ? " sel" : ""}${e.pending ? " pend" : ""}${S.hl.has(e.key) ? " hl" : ""}`;
      const posOf = e => `left:${e.x}px;top:${e.y}px;--s:var(--seal-fit,var(--seal-size,84px));--rot:${rotOf(e)}deg`, sayOf = e => `${labelOf(e.type)} · ${titleOf(e)} · ${longWhen(e.at)} · ${whoOf(e)}${placeOf(e) ? " · " + placeOf(e) : ""}`;
      const ghostHtml = ghosts.map(g => `<span class="tlSt ghost${S.markStage === g.s.k ? " sel" : ""}" data-stage="${esc(g.s.k)}" style="left:${g.x}px;top:${g.y}px;--s:var(--seal-fit,var(--seal-size,84px));--rot:0deg" aria-label="${esc("To come: " + g.s.l)}">${stampSvg(g, false, { ghost: 1 })}</span>`).join("");
      // the stamps already drawn are kept (a live step parses one stamp, not every one: a redraw of 100 stays in a frame);
      // the days, lines, path, NOW line and ghosts are drawn again
      const kept = new Map();
      if (!o.first) for (const b of [...cv.children]) { if (b.tagName === "BUTTON" && b.dataset.key && S.shownKeys.has(b.dataset.key) && !kept.has(b.dataset.key)) kept.set(b.dataset.key, b); else b.remove(); }
      if (!kept.size) {
        cv.innerHTML = back + evs.map(e => `<button type="button" class="${clsOf(e)}" data-key="${esc(e.key)}" data-sv="${esc(e.type + "|" + e.at)}" style="${posOf(e)}" aria-label="${esc(sayOf(e))}">${stampSvg(e, true)}</button>`).join("") + ghostHtml;
      } else {
        cv.insertAdjacentHTML("afterbegin", back);
        let prev = cv.querySelector(".tlNowLine");
        for (const e of evs) {
          let b = kept.get(e.key);
          if (!b) { b = doc.createElement("button"); b.type = "button"; b.dataset.key = e.key; }
          const sv = e.type + "|" + e.at, cls = clsOf(e), pos = posOf(e), say = sayOf(e);
          if (b.dataset.sv !== sv) { b.dataset.sv = sv; b.innerHTML = stampSvg(e, true); }
          if (b.className !== cls) b.className = cls;
          if (b.getAttribute("style") !== pos) b.setAttribute("style", pos);
          if (b.getAttribute("aria-label") !== say) b.setAttribute("aria-label", say);
          if (prev.nextSibling !== b) cv.insertBefore(b, prev.nextSibling);
          prev = b;
        }
        cv.insertAdjacentHTML("beforeend", ghostHtml);
      }
      S.nowX = nowX;
      // (a NOW label longer than the room before its line, "COMPLETED · TUE 3:08 AM · PAUL" early on, reads after it)
      const nl0 = cv.querySelector(".tlNowLine:not(.cx) span"); if (nl0 && (nl0.offsetWidth || nowLbl.length * V.fs.pill * .72 + 12) + 8 > nowX) { nl0.style.right = "auto"; nl0.style.left = "calc(8px * var(--tl-t, 1))"; }
      // a re-fit (the room changed, or a seal arrived and the chart took it in) glides: each seal from where it stood to where it stands, by transform alone,
      // the rest of the chart fading in. Never on the first drawing (the window opening has its own motion), never while it is being dragged to a size
      // (a re-fit a moment after the last), never under reduced motion (anim answers none)
      const pos = new Map();
      for (const e of evs) pos.set("k:" + e.key, { x: e.x, y: e.y, s: fit });
      for (const g of ghosts) pos.set("g:" + g.s.k, { x: g.x, y: g.y, s: fit });
      const before = S.pos, was = S.fit, calm = Date.now() - (S.fitAt || 0) > 140;
      S.pos = pos; S.fit = { w: seen.w, h: seen.h }; S.fitAt = Date.now();
      if (!o.first && before && was && was.w > 0 && was.h > 0 && seen.w > 0 && calm && !reduced() && typeof cv.animate === "function") {
        // (only the seals that move, and not more than a few dozen: a very long history just takes its new places, a frame is worth more than the glide)
        const moving = [];
        for (const b of cv.querySelectorAll(".tlSt")) {
          if (b._tlGlide) { try { b._tlGlide.cancel(); } catch (_) {} b._tlGlide = null; }
          const id = b.dataset.key ? "k:" + b.dataset.key : b.dataset.stage ? "g:" + b.dataset.stage : "", from = before.get(id), to = pos.get(id);
          if (!from || !to || b.classList.contains("pending")) continue;
          const dx = from.x - to.x, dy = from.y - to.y, k = from.s / to.s;
          if (Math.abs(dx) >= .5 || Math.abs(dy) >= .5 || Math.abs(k - 1) >= .01) moving.push([b, dx, dy, k]);
        }
        if (moving.length <= 40) for (const [b, dx, dy, k] of moving) {
          try { b._tlGlide = b.animate([{ transform: `translate(${dx}px, ${dy}px) scale(${k}) rotate(var(--rot))` }, { transform: "rotate(var(--rot))" }], { duration: 260, easing: E }); } catch (_) { /* no glide */ }
        }
        if (resized) for (const n of cv.children) if (n.tagName !== "BUTTON" && !n.classList.contains("tlSt")) { try { n.animate([{ opacity: .3 }, { opacity: 1 }], { duration: 220, easing: E }); } catch (_) { /* no fade */ } }
      }
      if (o.first) {
        const p = cv.querySelector(".tlPath"); anim(p, [{ opacity: 0 }, { opacity: 1 }], 900, { easing: SLIDE });
        // opens at "now"
        if (scroller.clientWidth && W > scroller.clientWidth) scroller.scrollLeft = Math.max(0, nowX - scroller.clientWidth * .6);
      } else {
        // (the canvas keeps its width while it is redrawn, so the scroll stays where the reader left it)
        const nl = cv.querySelector(".tlNowLine:not(.cx)");
        if (nl && oldNow && !resized && Math.abs(oldNow - nowX) > 1) anim(nl, [{ transform: `translateX(${oldNow - nowX}px)` }, { transform: "none" }], 760, { easing: SLIDE });
      }
    }
    const cssEsc = s => (root.CSS && root.CSS.escape ? root.CSS.escape(s) : String(s).replace(/["\\]/g, "\\$&"));
    /** The seal that stands for an event: itself when it is drawn, the seal that collapsed it, else the milestone it
     *  happened under (the last seal at or before its time). "" when this order has no seal yet. */
    function sealKey(key) {
      if (!key) return "";
      if (S.shownKeys.has(key)) return key;
      const e = S.byKey.get(key); if (!e) return "";
      for (const s of S.shown) if (s.same && s.same.some(x => x.key === key)) return s.key;
      let at = "";
      for (const s of S.shown) { if (s.at <= e.at) at = s.key; else if (!at) { at = s.key; break; } }
      return at;
    }
    /** Rings one seal of the chart (a thin ring in the accent, its lane lit) and, with o.scroll, brings it into view. Nothing opens: the
     *  chart has no detail pane, and a click on a seal only grows it where it stands (Seal.zoom).
     *  what: an event key or id (the seal that stands for it is ringed), or { stage } for a step of the rail (the seal of that step when it is
     *  done, else its dashed stamp). The ring stays until the next one. → true when a seal was ringed. */
    function mark(what, o) {
      o = o || {};
      let key = "", stage = "";
      if (what && typeof what === "object" && what.stage) {
        const i = STAGES.findIndex(s => s.k === what.stage); if (i < 0) return false;
        const first = S.D && S.D.stages[i] && S.D.stages[i].first;
        if (first) key = sealKey(first.key); else stage = STAGES[i].k;
      } else key = sealKey(what);
      let ghost = null;
      if (stage) { ghost = $$(".tlSt.ghost[data-stage]").find(b => b.dataset.stage === stage) || null; if (!ghost) return false; }
      else if (!key) return false;
      S.mark = key || null; S.markStage = stage || null;
      for (const b of $$(".tlSt[data-key]")) b.classList.toggle("sel", b.dataset.key === key);
      for (const b of $$(".tlSt.ghost[data-stage]")) b.classList.toggle("sel", b.dataset.stage === stage);
      const e = key ? S.byKey.get(key) : null, lane = e ? e.lane : (STAGES.find(s => s.k === stage) || {}).lane;
      for (const l of $$(".tlLane")) l.classList.toggle("on", l.dataset.lane === lane);
      if (o.scroll) scrollToEv(e || { x: parseFloat(ghost.style.left) });
      return true;
    }
    function scrollToEv(e) {
      if (!e || !scroller.clientWidth || !(e.x >= 0)) return;
      const sl = scroller.scrollLeft, cw = scroller.clientWidth;
      if (e.x < sl + 40 || e.x > sl + cw - 150) {
        const left = Math.max(0, e.x - cw * .6);
        try { scroller.scrollTo({ left, behavior: reduced() ? "auto" : "smooth" }); } catch (_) { scroller.scrollLeft = left; }
      }
    }
    function findKey(id) {
      if (id == null) return null;
      const s = String(id); if (S.byKey.has(s)) return s;
      const tail = s.split("~").pop().replace(/[^\w.:-]/g, "_");
      for (const [k, e] of S.byKey) if (e.id === s || k.split("~").slice(1).join("~") === tail) return k;
      return null;
    }

    /* ── the zoom: a rested-on dot's seal grows where it stands (Seal.zoom: small ones more, large ones little), so nothing is
       clipped or covered and the dot stays under the pointer ── */
    function evOfEl(b) {
      const stored = storedFace(b); if (stored) return stored;
      if (b.dataset.key) return S.byKey.get(b.dataset.key) || null;
      const i = STAGES.findIndex(s => s.k === b.dataset.stage); if (i < 0 || !S.D) return null;
      if ((b.classList.contains("x") || b.classList.contains("tlCxStamp")) && S.D.cancelled) return S.events.filter(e => CANCEL_TYPES.has(e.type)).pop() || { key: "x", type: "cancelled", at: S.D.cancelled.at, by: S.D.cancelled.by, lane: "office", data: null };
      // (a step the piece no longer stands on, taken off its sheet, is drawn as a ghost: its zoom is the ghost too, the real seal stays on the Timeline)
      return (stepDone(S.D, i) && S.D.stages[i].first) || { key: "future-" + STAGES[i].k, type: STAGES[i].kind, at: 0, ghost: true, by: "" };
    }
    const liftOf = b => b.classList.contains("tlStop") ? b.querySelector(".tlSeal") : b;
    function showZoom(b, kb) {
      if (!evOfEl(b)) return false;
      const el = liftOf(b); if (!el) return false;
      if (zoomFor && zoomFor !== el) zoomOff(zoomFor, true);
      zoomFor = el; return zoomOn(el, kb);
    }
    function hideZoom(now) { const el = zoomFor; zoomFor = null; if (el) zoomOff(el, now); }
    function onScroll() { sealHover.cancel(true); }

    /* ── the step explainer (Paul, 28 Sep, point 5): hovering a step (a rail's stop, a lane stamp, a dashed stamp to come)
       shows a small card BELOW its dot, what is done and what is still missing (the zoomed seal sits above: the dot stays
       seen); a click on a lane stamp only grows it where it stands, a click on a header rail's step opens the Timeline on it.
       Escape or a click elsewhere lets it go. opts.context() is what the host knows of the order (requirementsOf). ── */
    let expFor = null, expA = null;
    const ctxOf = () => { try { return (typeof opts.context === "function" ? opts.context() : opts.context) || null; } catch (err) { warn("context", err); return null; } };
    // (S.D is the rail drawn: the piece shown's own steps, or the order's)
    const reqOf = i => requirementsOf(i, { events: S.events, D: S.D || deriveNow(), context: ctxOf() });
    function stageOfEl(b) {
      if (b.dataset.stage) return STAGES.findIndex(s => s.k === b.dataset.stage);
      const e = b.dataset.key && S.byKey.get(b.dataset.key); if (!e) return -1;
      // an event that is not a step of its own (a hold, a message …): the step the order is working towards
      return own(STOP_OF, e.type) ? STOP_OF[e.type] : S.D ? (S.D.cur >= 0 ? S.D.cur : S.D.step) : -1;
    }
    function showExp(b) {
      if (!S.loaded || !S.D || !exp) return;
      const i = stageOfEl(b), q = i >= 0 ? reqOf(i) : null; if (!q) return;
      if (expA) { try { expA.cancel(); } catch (_) {} expA = null; }
      // an event that is not a step of its own says what it is first, then what the order needs to move on
      const e = b.dataset.key && S.byKey.get(b.dataset.key), lone = e && STOP_OF[e.type] == null, hand = lone && S.D.hand;
      const head = lone ? `<ul class="tlReq"><li class="rq ok"><i>${CHECK}</i><span>${esc(titleOf(e, 80))}<small>${esc(shortWhen(e.at) + " · " + whoOf(e))}</small></span></li></ul><div class="xf" style="margin:8px 0 ${hand ? 0 : 9}px">${hand ? (hand === e ? "Order completed by hand · nothing more to do" : `Order completed by hand · ${esc(shortWhen(hand.at))}${personOf(hand) ? " · " + esc(personOf(hand)) : ""}`) : "Then, for the order to move on"}</div>` : "";
      expFor = b;
      // the seal has grown where it stands: the card goes under what it has grown to (and, in a short view, beside it), never over it
      const sealEl = liftOf(b), zr = zoomRect(sealEl), whole = zr ? withZoom(b, sealEl) : b;
      expA = placeExp(exp, hand ? head : head + reqCard(q, compact ? "click to open it on the Timeline" : ""), b.classList.contains("tlStop") ? b.querySelector(".tlSeal") || b : b, whole, zr && { left: zr.left, right: zr.right });
      exp.classList.add("on");
    }
    function hideExp(now) {
      if (!expFor || !exp) return;
      expFor = null; exp.classList.remove("on");
      if (now) { if (expA) { try { expA.cancel(); } catch (_) {} } expA = null; exp.style.display = "none"; exp.innerHTML = ""; return; }
      expA = fadeExp(exp, expA);
    }
    const sealSelector = ".tlStop .tlSeal:not(.pending), .tlSt[data-key]:not(.pending), .tlSt.ghost[data-stage], .tlCxStamp";
    const sealHover = restOnSeal(box, node => node.closest?.(sealSelector), (seal, kb, click) => {
      const b = seal.closest(".tlStop") || seal;
      showZoom(b, kb);
      if (!click && b.matches(".tlSt, .tlStop")) showExp(b);   // (a click only grows the seal; on the header rail it also opens the step on the Timeline)
    }, now => { hideZoom(now); hideExp(now); });
    function focusedSeal(ev) {
      const b = ev.target.closest?.(".tlStop, .tlSt[data-key], .tlCxStamp");
      return b && b.matches(":focus-visible") ? liftOf(b) : null;
    }
    function expFocus(ev) { const b = focusedSeal(ev); if (b && !b.classList.contains("pending")) sealHover.open(b, true); }
    function expBlur(ev) { const b = ev.target.closest?.(".tlStop, .tlSt[data-key], .tlCxStamp"); if (b && liftOf(b) === sealHover.current) sealHover.cancel(); }

    /* ── clicks ── */
    function onClick(ev) {
      const t = ev.target; if (!t || !t.closest) return;
      const os = t.closest(".tlOpenSheet"); if (os && os.getAttribute("aria-disabled") === "true") { ev.preventDefault(); return; }
      if (os) { ev.preventDefault(); hideZoom(true); try { if (typeof opts.onSheet === "function") opts.onSheet(os.dataset.sheet, os.dataset.pool || null); } catch (e) { try { console.warn("[OrderTimelineUI] onSheet:", e); } catch (_) {} } return; }
      const stop = t.closest(".tlStop"); if (stop) { stageClick(stop, ev); return; }
      if (t.closest(".tlRetry")) { load(true); return; }
    }
    /** A step of the rail: compact hands it to the host (which opens its Timeline on it); on the chart's own rail it rings that step's seal
     *  (or, for a step not reached, its dashed stamp). */
    function stageClick(stop, click) {
      const e = evOfEl(stop);
      const i = STAGES.findIndex(s => s.k === stop.dataset.stage), last = S.D && i >= 0 && (!S.D.fenced || stepDone(S.D, i)) && S.D.stages[i].last;
      const target = stop.classList.contains("x") ? e : last || e;
      if (!target || !target.key || !S.byKey.has(target.key)) {
        if (i >= 0 && S.loaded) {
          if (compact) { if (click) click.preventDefault(); handOver({ stage: STAGES[i].k }); return; }
          if (mark({ stage: STAGES[i].k }, { scroll: true })) return;
        }
        anim(stop.querySelector(".tlSeal"), [{ transform: "translateX(0)" }, { transform: "translateX(-3px)" }, { transform: "translateX(3px)" }, { transform: "translateX(0)" }], 300);
        return;
      }
      if (compact) { if (click) click.preventDefault(); handOver(target); return; }
      mark(target.key, { scroll: true });
    }

    /* ── live ── */
    function onRecord(x) {
      if (S.dead || !x || digits(x.orderId) !== orderId) return;
      const e = norm(x); if (!e || S.allKeys.has(e.key)) return;
      e.live = Date.now(); e.pending = true;
      S.allKeys.set(e.key, e); S.every = [...S.allKeys.values()].sort(byAt); narrow();
      if (!S.loaded) return;   // the answer on its way merges it
      S.sig = sigOf();
      repaint({ fresh: [e.key] });
    }
    function arm() {
      pollT = cancelT(pollT);
      if (!live || S.dead) return;
      pollT = later(() => { pollT = 0; if (doc.visibilityState === "hidden") return; Promise.resolve(load()).then(arm, arm); }, pollMs);
    }
    function onVis() {
      if (S.dead || !live) return;
      if (doc.visibilityState === "hidden") { pollT = cancelT(pollT); return; }
      if (Date.now() - S.lastLoad >= pollMs) Promise.resolve(load()).then(arm, arm); else arm();
    }

    /* ── the handle ── */
    function refresh() { return Promise.resolve(load(false)).then(() => undefined); }
    /** compact: the host opens the event on its Timeline — opts.onOpen(event), else a bubbling "timeline:focus". */
    function handOver(e) {
      const x = e.stage && !e.type ? { orderId, stage: e.stage } : pub(e);
      if (typeof opts.onOpen === "function") { try { opts.onOpen(x); } catch (err) { warn("onOpen", err); } return; }
      box.dispatchEvent(new CustomEvent("timeline:focus", { bubbles: true, detail: e.stage && !e.type ? { stage: e.stage, orderId } : { eventId: e.id, key: e.key, orderId } }));
    }
    /** Tells the host what is drawn: opts.onEvents(events, oldest first) and opts.onNow(where it is now). */
    function tell() {
      const D = S.D; if (S.dead || !D) return;
      if (typeof opts.onEvents === "function") { try { opts.onEvents(S.every.map(pub)); } catch (err) { warn("onEvents", err); } }
      if (typeof opts.onNow === "function") {
        const nt = nowText(D);
        try { opts.onNow({ text: nt.t, tone: nt.cls, where: D.W, step: D.step, next: D.cur >= 0 ? STAGES[D.cur].l : "", cancelled: D.cancelled, held: !!D.hold, last: D.last ? pub(D.last) : null }); } catch (err) { warn("onNow", err); }
      }
    }
    function focus(ev) {
      if (S.dead) return false;
      // an event id (the server's or this page's), a key, or an event as onOpen/onEvents hand it out
      if (ev && typeof ev === "object" && ev.stage && !ev.type && !ev.id) {
        const i = STAGES.findIndex(s => s.k === ev.stage); if (i < 0) return false;
        if (compact) { handOver({ stage: ev.stage }); return true; }
        if (!S.loaded) { S.pendingFocus = ev; return false; }
        return mark({ stage: ev.stage }, { scroll: true });
      }
      const id = ev && typeof ev === "object" ? (ev.key && S.byKey.has(ev.key) ? ev.key : ev.id || ev.eventId || ev.key) : ev;
      if (!S.loaded) { S.pendingFocus = id; return false; }
      const k = findKey(id); if (!k) return false;
      const e = S.byKey.get(k);
      if (compact) { handOver(e); return true; }
      return mark(k, { scroll: true });
    }
    function destroy() {
      if (fitObserver) fitObserver.disconnect();
      root.removeEventListener("resize", fitAll);
      if (S.dead) return;
      S.dead = true;
      for (const t of timers) clearTimeout(t);
      timers.clear(); pollT = busyT = 0;
      try { if (typeof unsub === "function") unsub(); } catch (_) {}
      try { if (typeof unfeed === "function") unfeed(); } catch (_) {}
      unsub = unfeed = null;
      doc.removeEventListener("visibilitychange", onVis);
      sealHover.destroy();
      box.removeEventListener("click", onClick);
      box.removeEventListener("focusin", expFocus); box.removeEventListener("focusout", expBlur);
      scroller.removeEventListener("scroll", onScroll);
      try { for (const a of box.getAnimations({ subtree: true })) a.cancel(); } catch (_) {}
      if (tb) { bar.removeEventListener("click", onClick); bar.remove(); }
      box.remove();
    }

    /** The room changed (the window, the order window, the browser's zoom, a taller header ...): the chart is fitted to it again, in the frame the
     *  change was seen in. The room is read from its host (el) and from the grid itself, so nothing the chart writes can change what it observes. */
    function fitAll() {
      if (S.dead) return;
      if (S.stamping) { S.deferredPaint = S.deferredPaint || {}; return; }
      measure();
      fitRail($(".tlRail"), S.M);
      if (compact) return;
      const g = S.M.grid;
      if (!g.w || !g.h) return;   // (hidden: the observer says when it is shown)
      if (S.fit && S.fit.w === g.w && S.fit.h === g.h) return;   // (the same room: it fits it already)
      if (S.loaded && S.D) paintCanvas(S.D, {});
      else { applyFit(fitVertical(g.h, g.w, Math.min(FIT_CAP, baseSealSize($(".tlCanvas"))))); S.fit = { w: g.w, h: g.h }; }   // (nothing to draw yet: the lanes' names and bands already stand at their size)
    }
    root.addEventListener("resize", fitAll);
    if (typeof root.ResizeObserver === "function") { fitObserver = new root.ResizeObserver(fitAll); fitObserver.observe(el); if (!compact) fitObserver.observe(gridEl); }
    box.addEventListener("click", onClick);
    box.addEventListener("focusin", expFocus); box.addEventListener("focusout", expBlur);
    if (tb) bar.addEventListener("click", onClick);
    scroller.addEventListener("scroll", onScroll, { passive: true });
    paintRail(railed(derive([], null), pieceNow() ? withDone(stagesFor(pieceNow().line), []) : railNow([])), { first: true });
    if (!compact) paintLanes();
    if (src) {
      // what the feed has already: its answer drawn at once, its read on the way waited for; else it reads now
      unfeed = src.subscribe(onFeed);
      if (src.answer) { onFeed("data"); if (src.loading) waiting(); }
      else if (src.loading) waiting();
      else if (src.error) onFeed("error");
      else src.refresh();
    } else load();
    if (live) {
      try { if (root.OrderTimeline && typeof root.OrderTimeline.onRecord === "function") unsub = root.OrderTimeline.onRecord(onRecord); } catch (_) {}
      if (!src) { doc.addEventListener("visibilitychange", onVis); arm(); }   // (a feed polls for every mount)
    }
    // state(): the derived state last drawn (D: steps, hold, placement), for a host's step card to read the very same answer as the rail
    return { refresh, destroy, focus, setPieces, state: () => S.D };
  }

  /* ════ the order view's "Where it is now" card (spec §3, §8) ════
     nowStamps(events, { ev, cancelled, hand }) → { seal, recent } HTML: the latest step's seal at the shared 84px size, including cancelled orders (hand: handDoneOf, the press
     that completed the order: its ORDER COMPLETE seal, the same event as the card's line); all compact groups
     fit the same seal uniformly, and every full face grows to 168px on hover.
     wireNow(card, onOpen) once that HTML is in the page: a stamp click → onOpen({ id }); historical rendering is silent. */
  function nowStamps(events, o) {
    css(); o = o || {};
    const evs = (events || []).map(norm).filter(Boolean).sort(byAt), c = o.cancelled;
    // the seal of the milestone it is at now — never a "read" or a "?" (Paul, 28 Sep) — and, when something is holding
    // it up, that in plain words in place of the old row of stamps
    // (the order completed by hand, o.hand = handDoneOf: the card's line and its seal are that one event, the order's ORDER COMPLETE; any other card
    //  never wears a piece's completion: while another piece is not done, the order is where its slowest piece is, and the pieces' own seals stand on
    //  their rows, their boxes and the Timeline)
    const seals = sealsOf(evs), hand = o.hand ? handSealOf(norm(o.hand)) : null, asked = hand || (o.ev && norm(o.ev));
    const liveKeys = hand ? null : new Set([...handLive(evs)].map(e => e.key)), pool = liveKeys && liveKeys.size ? seals.filter(e => !liveKeys.has(e.key)) : seals;
    const last = (hand || (asked && sealed(asked) && !(liveKeys && liveKeys.has(asked.key)) ? asked : pool[pool.length - 1])) || null;
    const blocker = c ? null : blockerOf(evs);
    let seal = "";
    if (c) {
      const etsy = c.type === "etsyCancelled" || c.source === "etsy" || /^etsy$/i.test(c.by || "");
      seal = `<div class="tlNowSeal cx" tabindex="0" aria-label="${esc("Cancelled order · " + longWhen(c.at) + (c.by ? " · " + c.by : ""))}" data-at="${+c.at || 0}" ${sealAttrs(cancellationFace(Object.assign({}, c, { source: etsy ? "etsy" : c.source })))}>${stampSvg(cancellationFace(Object.assign({}, c, { source: etsy ? "etsy" : c.source })), true, { uid: "tlNowCx" })}</div>`;
    } else if (last) seal = `<div class="tlNowSeal" tabindex="0" aria-label="${esc(labelOf(last.type) + " · " + longWhen(last.at) + " · " + whoOf(last))}" ${sealAttrs(last)} data-key="${esc(last.key)}" style="--rot:${rotOf(last)}deg">${stampSvg(last, true, { uid: "tlNowSeal" })}</div>`;
    // (`recent`, the old row of six stamps, is gone: one seal says where it is, and the words below say what it waits on)
    const recent = blocker ? `<span class="tlBlock"><b>${esc(blocker.label)}</b>${esc(blocker.text)}</span>` : c ? cxList(evs, c) : "";
    return { seal, recent, blocker };
  }
  /* A cancelled order's story in plain words (Paul, 29 Sep 00:26): the exact moment it was cancelled (Etsy's own, with
     when the sorter saw it), then every place it was taken out of, each with its time and how it went ("Still on SS
     Sheet 3: not taken off yet" when it is not done), a station's "Understood" with who and where, and a restore. */
  const CX_OK = new Set(["removed", "setAside"]);
  function cxSteps(evs, c) {
    const cx = evs.filter(e => CANCEL_TYPES.has(e.type)).pop() || null, at = +(c && c.at) || (cx && +cx.at) || 0;
    const steps = [];
    for (const e of evs) {
      if (+e.at < at - 60000) continue;
      const d = e.data || {}, who = personOf(e), place = placeOf(e);
      if (e.type === "removed" && (d.cancel || /^cancel/i.test(reasonOf(e)))) steps.push({ e, st: "ok", text: e.text && /^Removed from/.test(e.text) ? e.text : `Removed from ${e.sheet || "its sheet"}` });
      else if (e.type === "cancelStep") steps.push({ e, st: d.outcome === "failed" ? "bad" : CX_OK.has(d.outcome) && d.done !== false ? "ok" : "wait", text: e.text || `${d.outcome || "Step"}: ${e.sheet || ""}` });
      else if (e.type === "cancelAlert") steps.push({ e, st: "ok", text: `Cancel alert understood${place ? " at " + place : ""}${who ? " by " + who : ""}` });
      else if (e.type === "cancelRestored" && +e.at >= at) steps.push({ e, st: "back", text: `Restored${who ? " by " + who : ""}` });
    }
    // one line per place: the latest word on it (a step said "still on" and the pieces came off later, a set aside)
    const last = new Map();
    steps.forEach((s, i) => { const p = s.e.type === "removed" || s.e.type === "cancelStep" ? String(s.e.sheet || "").trim().toLowerCase() : ""; if (p) last.set(p, i); });
    return { cx, at, steps: steps.filter((s, i) => { const p = s.e.type === "removed" || s.e.type === "cancelStep" ? String(s.e.sheet || "").trim().toLowerCase() : ""; return !p || last.get(p) === i; }) };
  }
  function cxList(evs, c) {
    const { cx, at, steps } = cxSteps(evs, c), d = (cx && cx.data) || {};
    const etsy = (cx && cx.type === "etsyCancelled") || c.source === "etsy" || /^etsy$/i.test(c.by || "");
    const seen = +d.seenAt > at + 60000 ? ` · seen by the sorter ${shortWhen(d.seenAt)}` : "";
    const who = etsy ? "on Etsy" : `by ${(cx && personOf(cx)) || c.by || "a person"}`;
    const head = at ? `<span class="h">Cancelled ${esc(who)} ${esc(shortWhen(at))}${esc(seen)}</span>` : "";
    const mark = { ok: "✓", wait: "…", bad: "✗", back: "↺" };
    const rows = steps.slice(-12).map(s => `<span class="s ${s.st}"><i>${mark[s.st]}</i>${esc(s.text)}<small>${esc(shortWhen(s.e.at))}${s.e.type !== "cancelAlert" && personOf(s.e) && !/ by /.test(s.text) ? " · " + esc(personOf(s.e)) : ""}</small></span>`).join("");
    return head || rows ? `<span class="tlCxList">${head}${rows}</span>` : "";
  }
  function wireNow(card, onOpen) {
    if (!card) return;
    if (!card._tlHover) {
      const sel = ".tlMini[data-tl-ev], .tlNowSeal";
      card._tlHover = restOnSeal(card, node => node.closest?.(sel), (b, kb) => nowZoom(card, b, false, kb), now => nowZoom(card, null, now), { click: b => typeof card._tlExpPin !== "function" && !b.matches(".tlMini") });
      card.addEventListener("focusin", ev => { const b = ev.target.closest?.(sel); if (b && b.matches(":focus-visible")) card._tlHover.open(b, true); });
      card.addEventListener("focusout", ev => { if (ev.target.closest?.(sel) === card._tlHover.current) card._tlHover.cancel(); });
      card.addEventListener("click", ev => { const b = ev.target.closest?.(sel); if (b && (b.matches(".tlMini") || typeof card._tlExpPin === "function")) card._tlHover.cancel(true); });   // (a click that opens something takes the zoom with it; one that does not has just grown the seal)
      card.closest("dialog")?.addEventListener("close", () => card._tlHover.cancel(true));
    }
    card._tlHover.check();
    card.querySelectorAll(".tlMini[data-tl-ev]").forEach(b => {
      b.onclick = ev => { ev.preventDefault(); card._tlHover.cancel(true); if (typeof onOpen === "function") { try { onOpen({ id: b.dataset.tlEv }); } catch (err) { warn("onOpen", err); } } };
    });
    if (card._tlZoomFor && !card._tlZoomFor.isConnected) nowZoom(card, null, true);   // repainted under the pointer
    const cx = card.querySelector(".tlNowSeal.cx"), s = card.querySelector(".tlNowSeal:not(.cx)");
    card._tlCx = cx ? cx.dataset.at : null; card._tlLast = s ? s.dataset.key : null;
  }

  /* the card's stamps zoom like the timeline's: b grows where it stands (Seal.zoom); null puts it back. */
  function nowZoom(card, b, now, kb) {
    const was = card._tlZoomFor;
    if (b) { if (was && was !== b) zoomOff(was, true); card._tlZoomFor = b; zoomOn(b, kb); return; }
    card._tlZoomFor = null; if (was) zoomOff(was, now);
  }

  /** The icon of the lane an event belongs to (the station badge's disc), as SVG markup; "" for no event. */
  function iconOf(x) { const e = x && norm(x); return e ? iconSvg((LANE[e.lane] || LANE.office).ic) : ""; }

  root.OrderTimelineUI = { mount, feed, stampSvg, derive, stepDone, STAGES, stagesFor, ofPiece, summary, isStud, engraveOf, KIND, labelOf, nowStamps, wireNow, iconOf, sealed, sealsOf, blockerOf, requirementsOf, explainOn,
    stepOf, labelStepOf, personOf, placeOf, opStepOf, whenOf, timeOf, handStepOf, handOf, handDoneOf, handLive, handSealOf, faceModel };
  root.OrderTimelineUI.pollOpenMs = POLL_OPEN;   // how often the open order view's feed reads (tests may set another before it opens)
})(typeof window !== "undefined" ? window : globalThis);
