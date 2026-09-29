/*  charm-nest-timeline-ui.js — the order timeline, drawn (Paul, 28 Sep 19:21: C1-C3, C8, E2, E3).
 *  Design: plans/design/order-view-spec.md §5-6 and its prototype; the stamps are siblings of the Seal family in
 *  charm-nest-motion.js (same 120-unit geometry, ink texture, ring words, date/time/name in the middle).
 *
 *    const tl = OrderTimelineUI.mount(el, { orderId, highlight, live: true, compact: false, onSheet(sheetId, poolId),
 *                                           onOpen(event), onEvents(events), onNow({ text, where, step, next, … }),
 *                                           stages: OrderTimelineUI.stagesFor(lines) | () => its steps })
 *    tl.refresh() → Promise   tl.focus(eventId | event) → true when shown   tl.destroy()
 *    onEvents/onNow tell the host what is drawn after every change; onOpen is compact's "open this on the Timeline".
 *    stagesFor(line | lines) → a piece's own steps (Welded only for a stud earring, Engraved only with a back engraving);
 *    isStud(line) → whether it is a stud; engraveOf(line) → true/false, null when not known yet.
 *
 *  Reads OrderTimeline.get(orderId) → { events, cancelled, where }. Draws, top to bottom:
 *    Now + milestone rail  where the order is right now (the server's `where`), and one stamp per step of its own
 *                          rail (Order in → Nested → [Engraved] → Laser cut → Sorted → [Welded] → Assembled → Shipped;
 *                          Welded only when a piece is a stud earring, Engraved only when one has a back engraving): passed steps stamped, the next one pulsing, the ones to come
 *                          faint outlines; a cancelled order gets a red CANCELLED stamp across the rail
 *    filters               All · Milestones · Stations · Sheets · Holds & cancels · Messages, with counts, and Stamps
 *                          (the legend); non-matches dim, nothing moves
 *    lanes                 one lane per place, one column per day (idle days collapse), one stamp per event, a gold
 *                          path from event to event, the NOW line and the milestones to come as dashed stamps
 *    detail                the chosen event inline, never a pop-up: its seal, who, where, when, the sheet (Open sheet),
 *                          before → after, the reason, its data, Earlier/Later (← →) and the steps around it
 *  Hovering a stamp lifts it onto a loupe (its own fixed layer, never clipped) at 136px with its full face.
 *  compact: true draws the rail alone, sized to its host (the order view's header); a rail stamp asks the host to open
 *  it on the Timeline: opts.onOpen(event), or else a bubbling "timeline:focus" event, detail { eventId }.
 *  nowStamps()/wireNow(): the order view's "Where it is now" seal and latest stamps (see the end of this file).
 *  Live: OrderTimeline.onRecord for this order plus a refresh every 20 s while the tab is visible; a new stamp comes
 *  down on its lane, the NOW line glides and the rail moves on. Motion is transform and opacity only; none under
 *  prefers-reduced-motion. destroy() clears every timer and listener it set. */
(function (root) {
  "use strict";
  if (root.OrderTimelineUI) return;
  const doc = root.document;
  const E = "cubic-bezier(.2,.8,.2,1)", SLIDE = "cubic-bezier(.3,.1,.2,1)", SPRING = "cubic-bezier(.3,1.7,.5,1)", DROP = "cubic-bezier(.5,0,.3,1)";
  const POLL = 20000;
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

  /* ── the zoomed seal (Paul, 2026-09-28): a hovered dot's seal opens directly ABOVE it, its bottom edge ZGAP over the
     dot's top, at 90% of the old 136px loupe. It never covers the dot, which stays in sight and keeps the hover (the
     layer takes no pointer). It flips below only when the view has no room above, and stays inside the view sideways.
     The room under the dot belongs to the step's explainer card. ── */
  const ZSZ = 122, ZGAP = 8, ZM = 8;
  function zoomSpot(r, vw, vh) {
    const up = r.top - ZGAP - ZSZ >= ZM || r.bottom + ZGAP + ZSZ > vh - ZM && r.top > vh - r.bottom;
    return { x: clamp(r.left + r.width / 2 - ZSZ / 2, ZM, Math.max(ZM, vw - ZSZ - ZM)), y: up ? r.top - ZGAP - ZSZ : r.bottom + ZGAP, up };
  }
  /** Lays a fixed layer at the dot's spot and eases it up out of the dot's edge; quiet puts it there at once. */
  function zoomIn(layer, r, quiet) {
    layer.style.display = "block"; layer.style.transform = "none";
    // a transformed ancestor moves a fixed layer's origin: measure where it really sits
    const o = layer.getBoundingClientRect(), p = zoomSpot(r, root.innerWidth || 1200, root.innerHeight || 800);
    const to = `translate(${Math.round(p.x - o.left)}px,${Math.round(p.y - o.top)}px)`;
    layer.dataset.side = p.up ? "above" : "below"; layer.style.transformOrigin = p.up ? "50% 100%" : "50% 0";
    layer.style.transform = to;
    if (!quiet) anim(layer, [{ transform: `${to} translateY(${p.up ? 6 : -6}px) scale(.84)`, opacity: 0 }, { transform: to, opacity: 1 }], 240, { easing: "cubic-bezier(.2,.8,.2,1)" });
    return p;
  }
  /** Eases the layer back down toward its dot: the animation, or null when motion is reduced. */
  function zoomOut(layer) {
    const t = layer.style.transform;
    return anim(layer, [{ transform: t, opacity: 1 }, { transform: `${t} translateY(${layer.dataset.side === "below" ? -5 : 5}px) scale(.88)`, opacity: 0 }], 150, { easing: "ease-in" });
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
  const PERSON_SEAL = new Set(["cancelled", "etsyCancelled", "cancelRestored", "removed", "cancelStep", "held", "released", "restored", "cancelAlert"]);
  const sealed = e => !!e && (MILESTONE_SEAL.has(e.type) || PERSON_SEAL.has(e.type) || !!opStepOf(e));
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
  /** The print seal in the Stamps legend. */
  const printLegend = t => { const e = { key: "legend-print", type: "sealPrinted", at: t, by: "Name", lane: "office", data: null, print: { n: 1, where: PRINT_WHERE.charm } }; return `<figure><span class="sv" style="transform:rotate(${rotOf(e)}deg)">${stampSvg(e, true, { tex: false })}</span><figcaption>QR label printed<small>every print</small></figcaption></figure>`; };
  /** What is holding this order up, in plain words: a hold nobody released, or a question nobody answered. */
  function blockerOf(events) {
    let hold = null; const need = new Map(), byHand = new Map();
    for (const e of events || []) {
      // a line completed with Complete Order answers its question (a person finished it by hand); a Reopen asks it again
      const op = opStepOf(e), l = e.lineKey;
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
  /** Completed by hand (Paul, 29 Sep: a Complete Order press reads as completed everywhere): the latest press when every
   *  line pressed has no Reopen after its last press, else null. The steps it had not reached are skipped, not "next";
   *  a Reopen puts the order back where it was (its Complete and Reopen seals stay), a later press completes it again. */
  function handOf(events) {
    const last = new Map();
    for (const e of events || []) { const k = opStepOf(e); if (k) last.set(e.lineKey || "", k === "complete" ? e : null); }
    const v = [...last.values()];
    return v.length && v.every(Boolean) ? v.reduce((a, b) => (+b.at >= +a.at ? b : a)) : null;
  }

  const pt = (r, a) => [60 + r * Math.cos(a * Math.PI / 180), 60 + r * Math.sin(a * Math.PI / 180)];
  const arc = (r, a0, a1, sw) => { const [x0, y0] = pt(r, a0), [x1, y1] = pt(r, a1), span = sw ? (a1 - a0 + 360) % 360 : (a0 - a1 + 360) % 360; return `M${x0.toFixed(2)} ${y0.toFixed(2)}A${r} ${r} 0 ${span > 180 ? 1 : 0} ${sw} ${x1.toFixed(2)} ${y1.toFixed(2)}`; };
  function scallops(n, rv, rc, sw) { let d = ""; const step = 360 / n; for (let i = 0; i < n; i++) { const a0 = i * step, [x0, y0] = pt(rv, a0), [xm, ym] = pt(rc, a0 + step / 2), [x1, y1] = pt(rv, a0 + step); d += (i ? "" : `M${x0.toFixed(2)} ${y0.toFixed(2)}`) + `Q${xm.toFixed(2)} ${ym.toFixed(2)} ${x1.toFixed(2)} ${y1.toFixed(2)}`; } return `<path d="${d}Z" stroke-width="${sw || 2.4}"/>`; }
  function star(a, r, s) { const [cx, cy] = pt(r, a); let d = ""; for (let i = 0; i < 10; i++) { const rr = i % 2 ? s * .42 : s, t = (i * 36 - 90) * Math.PI / 180; d += (i ? "L" : "M") + (cx + rr * Math.cos(t)).toFixed(2) + " " + (cy + rr * Math.sin(t)).toFixed(2); } return `<path d="${d}Z" stroke="none"/>`; }
  const edgeOf = sh => sh === "m" ? scallops(30, 54.2, 58.4) + `<circle cx="60" cy="60" r="51.6" stroke-width="1"/>`
    : sh === "a" ? `<circle cx="60" cy="60" r="55.4" stroke-width="4.2" stroke-dasharray="9 3.2"/><circle cx="60" cy="60" r="50.6" stroke-width="1"/>`
    : `<circle cx="60" cy="60" r="55.6" stroke-width="3.2"/><circle cx="60" cy="60" r="52" stroke-width=".9"/>`;
  let UID = 0;
  const MON = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], DAYN = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
  const dateOf = t => { const d = new Date(+t || Date.now()); return `${String(d.getDate()).padStart(2, "0")} ${MON[d.getMonth()]} ${d.getFullYear()}`; };
  // one formatter each, made once: toLocale*String builds a new one per call (~0.3 ms), and a redraw formats every stamp
  const fmtOf = o => { let f = null; return d => { try { return (f = f || new Intl.DateTimeFormat("en-US", o)).format(d); } catch (_) { return ""; } }; };
  const TIME = fmtOf({ hour: "numeric", minute: "2-digit" }), LONG = fmtOf({ weekday: "long", month: "short", day: "numeric", year: "numeric" });
  const timeOf = t => TIME(new Date(+t || Date.now()));
  const longWhen = t => LONG(new Date(+t)) + " · " + timeOf(t);
  const shortWhen = t => { const d = new Date(+t); return `${DAYN[d.getDay()]} ${timeOf(t)}`; };
  function ago(t) {
    const s = (Date.now() - t) / 1000; if (!(t > 0)) return ""; if (s < 45) return "just now";
    const m = s / 60; if (m < 60) return Math.round(m) + " min ago";
    const h = m / 60; if (h < 24) return Math.round(h) + " h ago";
    const d = h / 24; return d < 2 ? "yesterday" : d < 14 ? Math.round(d) + " days ago" : new Date(t).toLocaleDateString("en-US", { month: "short", day: "numeric" });
  }
  const rotOf = e => { const h = hash(String(e.key || e.id || "") + e.type); return kindOf(e.type).sh === "m" ? 4 + (h % 8) : -(4 + (h % 9)); };
  const sizeOf = e => { const s = kindOf(e.type).sh; return s === "m" ? 36 : s === "a" ? 32 : 28; };
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
  /** One stamp as SVG. full: the face that reads (ring words, icon, date, time, name); otherwise edge + big icon.
   *  opts.ghost draws a step still to come (dashed, no ink); opts.tex:false leaves out the ink texture; opts.uid names
   *  its inner ids (the same stamp then draws the same markup; unique in the page, as the ids are). */
  function stampSvg(e, full, opts) {
    if (e && e.print) return printSvg(e, full, opts);
    opts = opts || {};
    const Kd = kindOf(e.type), ink = INK[Kd.ink], id = opts.uid ? String(opts.uid).replace(/[^\w-]/g, "_") : "tls" + (++UID), seed = (hash(String(e.key || e.id || e.type) + e.at) % 997) + 1;
    const useTex = opts.tex !== false && !opts.ghost, tex = useTex ? texOf(id, seed) : "", g = useTex ? ` filter="url(#${id}f)"` : "";
    if (!full) {
      const wash = opts.ghost ? "none" : Kd.sh === "m" ? ink + "24" : ink + "12";
      const edge = Kd.sh === "m" ? scallops(26, 52, 58, 6) : Kd.sh === "a" ? `<circle cx="60" cy="60" r="54" stroke-width="9" stroke-dasharray="14 6"/>` : `<circle cx="60" cy="60" r="54" stroke-width="7"/><circle cx="60" cy="60" r="43" stroke-width="2.4" fill="none"/>`;
      return `<svg viewBox="0 0 120 120" aria-hidden="true" focusable="false"><defs>${tex}</defs><g${g} fill="${ink}" stroke="${ink}"><g fill="${opts.ghost ? "rgba(255,254,251,.9)" : "#fffefb"}"${opts.ghost ? ' stroke-dasharray="8 7"' : ""}>${edge}</g><circle cx="60" cy="60" r="${Kd.sh === "e" ? 41 : 48}" fill="${wash}" stroke="none"/>${iconG(Kd.ic, 60, 60, 2.1, ink, 2.8)}</g></svg>`;
    }
    const top = String((e.data && e.data.ring) || labelOf(e.type)).toUpperCase(), foot = footOf(e).toUpperCase().slice(0, 26);
    let nm = whoOf(e).toUpperCase().replace(/\s+/g, " ");
    if (nm.length > 13) { const w = nm.split(" "); nm = w.length > 1 ? `${w[0]} ${w[w.length - 1][0]}.` : nm; }
    if (nm.length > 14) nm = nm.slice(0, 13) + "…";
    const nfs = Math.min(9.4, (64 / Math.max(1, nm.length) - .5) / .68), tfs = top.length > 16 ? 7.4 : 8.6, ffs = foot.length > 18 ? 5.8 : 6.6;
    const sans = `font-family="-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif"`, mono = `font-family="ui-monospace,Menlo,Consolas,monospace"`;
    return `<svg viewBox="0 0 120 120" aria-hidden="true" focusable="false"><defs>${tex}<path id="${id}t" d="${arc(44.2, 158, 22, 1)}"/><path id="${id}b" d="${arc(50.6, 143, 37, 0)}"/></defs>` +
      `<g${g} fill="${ink}" stroke="${ink}"><g fill="rgba(255,254,251,.92)">${edgeOf(Kd.sh)}</g><circle cx="60" cy="60" r="41.4" stroke-width="1.3" fill="none"/>` +
      `<text stroke="none" ${sans} font-size="${tfs}" font-weight="800" letter-spacing="1.2"><textPath href="#${id}t" startOffset="50%" text-anchor="middle">${esc(top)}</textPath></text>` +
      `<text stroke="none" ${sans} font-size="${ffs}" font-weight="800" letter-spacing="1.05"><textPath href="#${id}b" startOffset="50%" text-anchor="middle">${esc(foot)}</textPath></text>` +
      star(150, 47.4, 2.6) + star(30, 47.4, 2.6) + iconG(Kd.ic, 60, 31.5, .72, ink) +
      `<path d="M25 44.5h70M22 72.5h76" stroke-width="1" fill="none"/>` +
      `<text x="60" y="56.4" text-anchor="middle" stroke="none" ${mono} font-size="10.4" font-weight="800">${esc(dateOf(e.at))}</text>` +
      `<text x="60" y="68.2" text-anchor="middle" stroke="none" ${mono} font-size="9.8" font-weight="700">${esc(timeOf(e.at))}</text>` +
      `<text x="60" y="84" text-anchor="middle" stroke="none" ${sans} font-size="${Math.max(7.4, nfs).toFixed(2)}" font-weight="800" letter-spacing=".5">${esc(nm)}</text></g></svg>`;
  }
  /** The red rubber stamp laid across the rail of a cancelled order. */
  function cancelSvg(c) {
    const id = "tlx" + (++UID), by = c.source === "etsy" || /^etsy$/i.test(c.by || "") ? "ON ETSY" : "BY " + String(c.by || "a person").toUpperCase().slice(0, 24);
    const mono = `font-family="ui-monospace,Menlo,Consolas,monospace"`;
    return `<svg viewBox="0 0 420 118" aria-hidden="true" focusable="false"><defs>${texOf(id, (hash(c.at) % 997) + 1)}</defs><g filter="url(#${id}f)" fill="${RED}" stroke="${RED}">` +
      `<rect x="5" y="5" width="410" height="108" rx="12" fill="none" stroke-width="6"/><rect x="15" y="15" width="390" height="88" rx="7" fill="none" stroke-width="1.8"/>` +
      `<text x="210" y="60" text-anchor="middle" stroke="none" font-family="-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif" font-size="44" font-weight="900" letter-spacing="7">CANCELLED</text>` +
      `<text x="210" y="80" text-anchor="middle" stroke="none" ${mono} font-size="12.5" font-weight="800" letter-spacing="3">DO NOT PROCEED</text>` +
      `<text x="210" y="96" text-anchor="middle" stroke="none" ${mono} font-size="10" font-weight="700" letter-spacing="1.4">${esc(`${by} · ${dateOf(c.at)} · ${timeOf(c.at)}`.toUpperCase())}</text></g></svg>`;
  }

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
  const humanKey = k => String(k).replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/[_-]+/g, " ").toLowerCase().replace(/^./, c => c.toUpperCase());
  function fmt(v) {
    if (v == null || v === "") return "—";
    if (Array.isArray(v)) return v.map(fmt).join(", ").slice(0, 160);
    if (typeof v === "object") {
      if (v.wIn != null && v.hIn != null) return `${v.wIn} × ${v.hIn} in`;
      if (v.w != null && v.h != null) return `${v.w} × ${v.h}`;
      if (v.label || v.name) return String(v.label || v.name);
      try { return JSON.stringify(v).slice(0, 160); } catch (_) { return "…"; }
    }
    if (typeof v === "number" && v > 1e12 && v < 4e12) return longWhen(v);
    if (typeof v === "boolean") return v ? "yes" : "no";
    return String(v).slice(0, 200);
  }
  /** before → after pairs in an event's data (a move's sheets, a size change, an engraving's words …). */
  function pairsOf(d) {
    const out = [], used = new Set();
    if (!d) return { out, used };
    const add = (what, a, b) => { if (!used.has(a) && !used.has(b) && (d[a] != null || d[b] != null)) { out.push({ what, a: d[a], b: d[b] }); used.add(a); used.add(b); } };
    for (const [a, b] of [["from", "to"], ["before", "after"], ["old", "new"], ["was", "now"], ["prev", "next"]]) if (a in d || b in d) add("", a, b);
    for (const k of Object.keys(d)) {
      let m = /^(from|old|prev|before)([A-Z]\w*)$/.exec(k);
      if (m) { const p = { from: "to", old: "new", prev: "next", before: "after" }[m[1]] + m[2]; if (p in d) add(humanKey(m[2]), k, p); continue; }
      m = /^(\w+?)(From|Before|Old)$/.exec(k);
      if (m) { const p = m[1] + { From: "To", Before: "After", Old: "New" }[m[2]]; if (p in d) add(humanKey(m[1]), k, p); }
    }
    return { out, used };
  }
  const REASON_KEYS = new Set(["reason", "why", "removedReason", "cancelReason", "ring", "foot"]);
  function factsOf(e, used) {
    const f = [];
    if (e.sheetId || e.sheet) f.push(["Sheet", e.sheet || e.sheetId]);
    if (e.sheetId && e.sheet && e.sheet !== e.sheetId) f.push(["Sheet id", e.sheetId]);
    if (e.setId) f.push(["Set", e.setId]);
    if (e.lineKey) f.push(["Line", e.lineKey]); else if (e.transactionId) f.push(["Transaction", e.transactionId]);
    if (e.device) f.push(["Device", e.device]);
    if (e.source) f.push(["Recorded by", e.source === "etsy" ? "Etsy check" : e.source === "station" ? "Station" : e.source === "system" ? "Automatic" : humanKey(e.source)]);
    if (e.pending) f.push(["Status", "Saving — on its way"]);
    if (e.derived) f.push(["From", "The order's older records (before the timeline)"]);
    const d = e.data || {};
    let n = 0;
    for (const [k, v] of Object.entries(d)) { if (used.has(k) || REASON_KEYS.has(k) || v == null || v === "" || ++n > 14) continue; f.push([humanKey(k), fmt(v)]); }
    return f;
  }
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
  function derive(events, cancelRec, where, rail) {
    events = events || [];
    rail = Array.isArray(rail) && rail.length ? rail : STAGES;
    const W = where && typeof where === "object" ? where : Object.assign(whereOf(events, cancelRec), typeof where === "string" && where ? { label: where, text: where } : {});
    const stages = rail.map(() => ({ first: null, last: null }));
    let lastHold = null, lastCancel = null;
    for (const e of events) {
      const i = own(STOP_OF, e.type) ? rail.indexOf(STAGES[STOP_OF[e.type]]) : -1;
      if (i >= 0) { const st = stages[i]; if (!st.first) st.first = e; st.last = e; }
      if (e.type === "held" || e.type === "released" || e.type === "restored") lastHold = e;
      if (CANCEL_TYPES.has(e.type) || e.type === "cancelRestored") lastCancel = e;
    }
    const full = clamp(Number.isFinite(+W.step) ? Math.round(+W.step) : -1, -1, STAGES.length - 1);
    const step = full < 0 ? -1 : rail.filter(s => STAGES.indexOf(s) <= full).length - 1;
    const cur = step + 1 < rail.length ? step + 1 : -1;
    let cancelled = null;
    if (cancelRec && typeof cancelRec === "object") cancelled = { at: +cancelRec.at || (lastCancel && lastCancel.at) || 0, by: str(cancelRec.by, 80), why: str(cancelRec.why, 400), source: cancelRec.source || (cancelRec.by === "Etsy" ? "etsy" : "sorter") };
    else if (lastCancel && lastCancel.type !== "cancelRestored" && W.cancelled !== false) cancelled = { at: lastCancel.at, by: whoOf(lastCancel), why: reasonOf(lastCancel) || lastCancel.text, source: lastCancel.type === "etsyCancelled" ? "etsy" : lastCancel.source };
    else if (W.cancelled) cancelled = { at: +W.since || +W.at || 0, by: str(W.by, 80), why: "", source: "" };
    const hold = !cancelled && W.stage === "held" ? (lastHold && lastHold.type === "held" ? lastHold : { type: "held", at: +W.since || 0, text: "", data: null }) : null;
    const hand = cancelled ? null : handOf(events);
    return { W, rail, stages, step, cur: hand ? -1 : cur, stop: cancelled ? Math.min(step + 1, rail.length - 1) : -1, cancelled, hold, hand, last: events[events.length - 1] || null };
  }

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
  function summary(events, pieces, cancelRec) {
    const evs = (events || []).map(x => (x && x.lane ? x : norm(x))).filter(Boolean).sort(byAt), ps = pieces || [];
    const each = ps.map(p => { const list = evs.filter(e => ofPiece(e, p, ps)); return { p, D: derive(list, cancelRec), steps: keepDone(stagesFor(p.line), list), events: list }; });
    const rail = STAGES.map((s, i) => ({ s, i, n: 0, of: 0 }));
    for (const x of each) for (const s of x.steps) {
      const i = STAGES.indexOf(s); if (i < 0) continue;
      const q = Math.max(1, Math.round(+x.p.qty || 1)); rail[i].of += q;
      if (x.D.step >= i || x.D.stages[i].first || x.D.hand) rail[i].n += q;
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
    let D = data.D || derive(evs, data.cancelled || null, data.where || null);
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
    const out = () => ({ k: s.k, label: s.l, i, n: pos + 1, of: R.length, state, done, need, facts: facts.slice(-6) });
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
        if (l.onSheet || l.state === "gone") continue;
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
  const REQ_WORD = { wait: "Waiting", person: "Needs a person", next: "Next", after: "After", stop: "Stopped", label: "" };
  // (a label not printed yet is said, but a step that is done stays done: only a real need makes it "Part done")
  const partDone = q => q.state === "done" && q.need.some(n => n.kind !== "label");
  const STATE_WORD = { done: "Done", now: "Next", later: "To come", stopped: "Stopped here", gone: "Won't happen", none: "Not needed", skipped: "Skipped" };
  const CHECK = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12.5l4.5 4.5L19 7.5"/></svg>`;
  const cap1 = t => { t = String(t || ""); return t.charAt(0).toUpperCase() + t.slice(1); };
  /** A step's lines: what is done with a check, what is missing with an open circle (full: every one, and the quiet facts). */
  function reqLines(q, full) {
    // (the step's own lines, then its labels: a label line never pushes out who did the step)
    const mine = q.done.filter(d => !d.label), labels = q.done.filter(d => d.label);
    const done = mine.slice(full ? -6 : -2).concat(labels.slice(-2)).map(d => `<li class="rq ok"><i>${CHECK}</i><span>${esc(d.t)}<small>${esc(d.sub)}</small></span></li>`);
    const need = (full ? q.need : q.need.slice(0, 4)).map(n => `<li class="rq ${n.kind}"><i aria-hidden="true"></i><span>${REQ_WORD[n.kind] ? `<em>${esc(REQ_WORD[n.kind])}</em>` : ""}${esc(cap1(n.t))}</span></li>`);
    const more = !full && q.need.length > 4 ? `<li class="rq more"><span>${q.need.length - 4} more · click to see them</span></li>` : "";
    if (q.state === "none") return `<ul class="tlReq"><li class="rq ok"><i>${CHECK}</i><span>${esc(q.facts[0] || "Not needed")}</span></li></ul>`;
    const facts = full && q.facts.length ? `<div class="tlReqF"><span class="tlLbl">Also recorded</span>${q.facts.map(f => `<span>${esc(f)}</span>`).join("")}</div>` : "";
    return `<ul class="tlReq">${done.join("")}${need.join("")}${more}</ul>${facts}`;
  }
  /** The small card under a hovered step. */
  const reqCard = q => `<div class="xh"><b>${esc(q.label)}</b><span class="xs ${q.state}${partDone(q) ? " now" : ""}">${esc(partDone(q) ? "Part done" : STATE_WORD[q.state] || "")}</span></div>${reqLines(q, false)}` +
    `<div class="xf">${q.n ? `Step ${q.n} of ${q.of}` : "Not a step of this order"}${q.state === "done" ? " · click to open it" : " · click to pin"}</div>`;
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
   *  [data-tl-step]) shows the step card under it; a click on the seal (or a [data-tl-step]) pins the card, whose link
   *  (or a second click) calls onPin({ stage }), which the host turns into the Timeline with that step pinned. get() → { events, cancelled, where, context, stages }.
   *  Wired once per host (it survives the host's innerHTML being written again); a later call only swaps get/onPin. */
  function explainOn(host, get, onPin) {
    if (!host) return;
    host._tlExpGet = get; host._tlExpPin = onPin;
    if (host._tlExp) return;
    css();
    const card = doc.createElement("div"); card.className = "tlExp"; card.setAttribute("role", "tooltip");
    (host.closest && host.closest("dialog") || doc.body).appendChild(card);
    let on = null, a = null, t = 0, pinned = null, cur = null;
    const SEL = ".tlNowSeal, .tlMini, [data-tl-step]";
    const stepOf = b => {
      const g = (typeof host._tlExpGet === "function" && host._tlExpGet()) || {};
      const evs = (g.events || []).map(norm).filter(Boolean).sort(byAt), D = railed(derive(evs, g.cancelled || null, g.where || null), keepDone(g.stages, evs));
      let i = b.dataset.tlStep ? STAGES.findIndex(s => s.k === b.dataset.tlStep) : -1;
      if (i < 0 && b.dataset.tlEv) { const e = evs.find(x => x.id === b.dataset.tlEv); if (e && own(STOP_OF, e.type)) i = STOP_OF[e.type]; }
      if (i < 0) i = D.cancelled ? D.stop : D.cur >= 0 ? D.cur : D.step;
      return i >= 0 ? requirementsOf(i, { events: evs, D, context: g.context }) : null;
    };
    // as the Timeline's card: it takes the pointer, so moving from the seal onto it keeps it; leaving both lets it go
    // 160 ms later. A click pins it (Esc or a click elsewhere lets it go); pinned, it offers the step on the Timeline.
    const foot = pin => { const f = card.querySelector(".xf"); if (f && cur) f.innerHTML = `${cur.n ? `Step ${cur.n} of ${cur.of}` : "Not a step of this order"} · ${pin && typeof host._tlExpPin === "function" ? `<button type="button" class="tlLink" data-tl-open>Open on the Timeline</button>` : "click to pin"}`; };
    const show = b => {
      const q = stepOf(b); if (!q) return false;
      if (a) { try { a.cancel(); } catch (_) {} }
      clearTimeout(t); on = b; cur = q; pinned = null;
      a = placeExp(card, reqCard(q), b.querySelector(".s") || b, b); card.classList.add("on"); foot(false);
      return true;
    };
    const hide = () => { clearTimeout(t); t = 0; pinned = null; if (!on) return; on = null; card.classList.remove("on"); a = fadeExp(card, a); };
    const later = () => { clearTimeout(t); t = setTimeout(() => { t = 0; if (on && !pinned && !card.matches(":hover") && !(on.isConnected && on.matches(":hover"))) hide(); }, 160); };
    host.addEventListener("pointerover", ev => {
      const b = ev.target.closest && ev.target.closest(SEL); if (!b || !host.contains(b)) return;
      if (b === on) { clearTimeout(t); return; }
      show(b);
    });
    host.addEventListener("pointerout", ev => { const b = ev.target.closest && ev.target.closest(SEL); if (b && b === on && !b.contains(ev.relatedTarget) && !(ev.relatedTarget && card.contains(ev.relatedTarget))) later(); });
    card.addEventListener("pointerleave", ev => { if (on && !(ev.relatedTarget && on.contains(ev.relatedTarget))) later(); });
    host.addEventListener("click", ev => {
      const b = ev.target.closest && ev.target.closest(".tlNowSeal, [data-tl-step]"); if (!b) return;
      if (pinned === b) { openIt(); return; }   // a second click: the step on the Timeline
      if (b !== on && !show(b)) return;
      pinned = b; foot(true);
    });
    function openIt() {
      const q = cur; hide();
      if (q && typeof host._tlExpPin === "function") { try { host._tlExpPin({ stage: q.k }); } catch (err) { warn("onPin", err); } }
    }
    card.addEventListener("click", ev => { if (ev.target.closest && ev.target.closest("[data-tl-open]")) openIt(); });
    doc.addEventListener("pointerdown", ev => { if (pinned && !card.contains(ev.target) && !(on && on.contains(ev.target))) hide(); }, true);
    // (Esc lets the pin go first; the order view stays open)
    doc.addEventListener("keydown", ev => { if (ev.key === "Escape" && pinned) { ev.preventDefault(); ev.stopPropagation(); hide(); } }, true);
    const dlg = host.closest && host.closest("dialog"); if (dlg) dlg.addEventListener("close", hide);
    host._tlExp = card;
  }

  /* ════ the component's look (once per page) ════ */
  const CSS = `
.tlUI{position:relative;min-width:0;min-height:0;display:flex;flex-direction:column;color:var(--ink,#1c1a17);font:13px/1.45 var(--sans,system-ui,sans-serif);--tlE:cubic-bezier(.2,.8,.2,1);--tlSpring:cubic-bezier(.3,1.7,.5,1);--tlSlate:#2f5563}
.tlUI *{box-sizing:border-box}
.tlUI button{font:inherit;color:inherit;cursor:pointer}
.tlUI [hidden]{display:none!important}
.tlBar.inTools{border:0;padding:0;min-height:0;flex-wrap:nowrap;gap:10px;min-width:0;font:13px/1.45 var(--sans,system-ui,sans-serif);--tlE:cubic-bezier(.2,.8,.2,1)}
.tlBar.inTools *{box-sizing:border-box}
.tlBar.inTools button{font:inherit;cursor:pointer}
.tlBar.inTools [hidden]{display:none!important}
.tlBar.inTools .tlChips{flex-wrap:nowrap;min-width:0;overflow-x:auto;scrollbar-width:none}
.tlBar.inTools .tlChip{flex:none}
.tlBar button.tlChip{font-size:11px}
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
.tlRail{position:relative;flex:3 1 520px;min-width:0;max-width:860px;margin-left:auto}
.tlStops{position:relative;display:grid;grid-template-columns:repeat(var(--n,9),minmax(0,1fr));margin:0;padding:0}
.tlTrack,.tlFill{position:absolute;top:17px;height:2px;border-radius:2px;left:calc(100% / (2 * var(--n,9)));right:calc(100% / (2 * var(--n,9)))}
.tlTrack{background:repeating-linear-gradient(90deg,var(--ink25) 0 4px,transparent 4px 8px)}
.tlFill{background:var(--sage);transform-origin:0 50%;transform:scaleX(0);transition:transform .9s cubic-bezier(.3,.1,.2,1)}
.tlRail.cx .tlFill{background:linear-gradient(90deg,var(--sage) 75%,var(--clay))}
.tlStop{position:relative;z-index:1;display:flex;flex-direction:column;align-items:center;gap:4px;min-width:0;border:0;background:none;padding:0 1px;color:var(--ink45)}
.tlStop .tlSeal{position:relative;display:block;width:34px;height:34px;transform:rotate(var(--rot,0deg));transition:transform .22s var(--tlE),opacity .24s}
.tlStop .tlSeal svg{width:100%;height:100%;display:block;overflow:visible}
.tlStop:hover .tlSeal{transform:rotate(var(--rot,0deg)) scale(1.12)}
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
.tlCxStamp{position:absolute;left:50%;top:50%;width:min(270px,40%);pointer-events:none;z-index:3;transform:translate(-50%,-50%) rotate(-6deg)}
.tlCxStamp svg{display:block;width:100%;height:auto;mix-blend-mode:multiply;opacity:.93}
.tlBar{display:flex;align-items:center;gap:6px;padding:7px 18px;border-bottom:1px solid var(--line);flex-wrap:wrap;min-height:40px;flex:none}
.tlChips{display:flex;flex-wrap:wrap;gap:6px}
.tlChip{border:1px solid var(--line);border-radius:999px;padding:3px 10px;background:var(--card);font-size:11px;color:var(--ink70);display:inline-flex;align-items:center;gap:6px;transition:transform .12s}
.tlChip:hover{border-color:var(--ink25)}
.tlChip:active{transform:scale(.96)}
.tlChip b{font:600 9.5px var(--mono);color:var(--ink45)}
.tlChip.on{background:var(--velvet);border-color:var(--velvet);color:#fff}.tlChip.on b{color:var(--rTxt3,#ada393)}
.tlChip.zero:not(.on){opacity:.55}
.tlBarR{margin-left:auto;display:flex;align-items:center;gap:10px;font:10.5px var(--mono);color:var(--ink45);min-width:0}
.tlBarR .err{color:#8a3a26;display:inline-flex;gap:6px;align-items:center}
.tlLive{display:inline-flex;align-items:center;gap:6px;font:700 9.5px var(--mono);letter-spacing:.1em;color:#3c5a39;background:var(--sageSoft);border-radius:999px;padding:3px 9px}
.tlLive i{width:7px;height:7px;border-radius:50%;background:currentColor;animation:tlBreathe 1.8s ease-in-out infinite}
@keyframes tlBreathe{50%{opacity:.3}}
.tlBusy{display:inline-flex;align-items:center;gap:6px}
.tlSpin{display:inline-block;width:11px;height:11px;flex:none;border:2px solid rgba(0,0,0,.12);border-top-color:var(--gold);border-radius:50%;animation:tlSpin .7s linear infinite}
@keyframes tlSpin{to{transform:rotate(360deg)}}
.tlGrid{display:grid;grid-template-columns:140px minmax(0,1fr);border-bottom:1px solid var(--line);position:relative;flex:none}
.tlLanes{border-right:1px solid var(--line);background:var(--card);padding-top:40px;padding-bottom:26px}
.tlLane{position:relative;height:58px;display:flex;flex-direction:column;justify-content:center;padding:0 14px;border-bottom:1px solid var(--line2);min-width:0}
.tlLane::before{content:"";position:absolute;inset:0;background:linear-gradient(90deg,rgba(202,168,97,.16),transparent);opacity:0;transition:opacity .24s}
.tlLane.on::before{opacity:1}
.tlLane b{position:relative;font:700 9.5px var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--ink70);display:flex;align-items:center;gap:6px}
.tlLane b svg{width:12px;height:12px;color:var(--ink45);flex:none}
.tlLane span{position:relative;font-size:11px;color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tlLane.stn b{color:var(--tlSlate)}
.tlScroll{overflow-x:auto;overflow-y:hidden;position:relative;min-width:0}
.tlCanvas{position:relative;height:472px;min-width:100%}
.tlPath{position:absolute;left:0;top:0;overflow:visible;pointer-events:none}
.tlDay{position:absolute;top:0;bottom:0;border-right:1px dashed var(--line)}
.tlDay.alt{background:rgba(250,247,241,.7)}
.tlDay .dh{position:absolute;left:14px;top:10px;font:700 10px var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--ink70);white-space:nowrap}
.tlDay .dh small{display:block;font:400 9.5px var(--mono);letter-spacing:.02em;color:var(--ink45);text-transform:none;margin-top:1px;overflow:hidden;text-overflow:ellipsis}
.tlDay:not(.idle) .dh{right:8px;overflow:hidden;text-overflow:ellipsis}
.tlDay.idle{background:repeating-linear-gradient(135deg,transparent 0 6px,rgba(196,189,176,.22) 6px 7px)}
.tlDay.idle .dh{left:50%;transform:translateX(-50%);text-align:center}
.tlLaneLine{position:absolute;left:0;right:0;height:1px;background:var(--line2)}
.tlSt{position:absolute;width:var(--s);height:var(--s);margin:calc(var(--s) / -2) 0 0 calc(var(--s) / -2);border:0;padding:0;background:transparent;border-radius:50%;transform:rotate(var(--rot));transition:opacity .24s,transform .22s var(--tlE);z-index:2}
.tlSt svg{width:100%;height:100%;display:block;overflow:visible;mix-blend-mode:multiply}
.tlSt:hover{transform:rotate(var(--rot)) scale(1.12)}
.tlSt.sel::before{content:"";position:absolute;inset:-6px;border-radius:50%;border:1.5px solid var(--gold);box-shadow:0 0 0 4px rgba(202,168,97,.18);animation:tlSelIn .32s var(--tlSpring) both}
@keyframes tlSelIn{from{transform:scale(.6);opacity:0}}
.tlSt.hl::after{content:"";position:absolute;inset:-10px;border-radius:50%;background:radial-gradient(rgba(202,168,97,.38),transparent 70%);z-index:-1}
.tlSt.dim{opacity:.13}.tlSt.dim svg{filter:grayscale(1)}
.tlSt.pend svg{opacity:.65}
.tlSt.ghost{opacity:.42;cursor:default}.tlSt.ghost:hover{transform:rotate(var(--rot))}
.tlSt.ghost svg{mix-blend-mode:normal}
.tlInkRing{position:absolute;border-radius:50%;border:2px solid;pointer-events:none;z-index:1;opacity:0}
.tlNowLine{position:absolute;top:34px;bottom:4px;width:0;border-left:1.5px solid var(--gold);z-index:1}
.tlNowLine::before,.tlNowLine::after{content:"";position:absolute;left:-5px;bottom:-5px;width:9px;height:9px;border-radius:50%;background:var(--gold)}
.tlNowLine::after{background:none;border:2px solid var(--gold2);left:-7px;bottom:-7px;width:13px;height:13px;animation:tlRing 2s ease-out infinite}
.tlNowLine span{position:absolute;right:8px;bottom:-3px;font:700 9.5px var(--mono);letter-spacing:.1em;color:#7a5a1d;white-space:nowrap;background:var(--goldSoft);padding:2px 6px;border-radius:5px}
.tlNowLine.cx{border-color:var(--clay)}.tlNowLine.cx::before{background:var(--clay)}.tlNowLine.cx::after{display:none}
.tlNowLine.cx span{background:var(--claySoft);color:#8a3a26}
.tlAfterCx{position:absolute;top:34px;bottom:0;right:0;background:repeating-linear-gradient(135deg,transparent 0 7px,rgba(176,86,63,.09) 7px 8px)}
.tlMsg{position:absolute;left:158px;top:50%;transform:translateY(-50%);display:flex;align-items:center;gap:9px;font-size:12.5px;color:var(--ink70);background:var(--card);border:1px solid var(--line);border-radius:10px;padding:9px 13px;box-shadow:var(--sh);z-index:4;max-width:calc(100% - 176px)}
.tlMsg.err{color:#8a3a26;background:var(--claySoft);border-color:#e7b9aa}
/* the zoomed seal (zoomSpot): 122px, 90% of the old 136px, above its dot and taking no pointer, so the dot keeps the hover */
.tlLoupe{position:fixed;z-index:2147483000;left:0;top:0;width:122px;height:122px;pointer-events:none;border-radius:50%;background:var(--card,#fffefb);box-shadow:0 0 0 1px rgba(30,26,20,.06),0 16px 40px rgba(30,26,20,.22);display:none}
.tlLoupe .lf,.tlLoupe .lf svg{width:100%;height:100%;display:block}
.tlDetail{position:relative;flex:1 1 auto;min-height:0;overflow:auto;padding:22px 28px 26px}
.tlDetIn{display:grid;grid-template-columns:150px minmax(0,1fr) 290px;gap:32px;align-content:start}
.tlBig{width:150px;height:150px;transform:rotate(var(--rot,0deg))}
.tlBig svg{width:100%;height:100%;display:block;overflow:visible}
.tlDetail h3{font:500 24px/1.2 var(--serif);margin:6px 0 4px}
.tlWhen{font:11.5px var(--mono);color:var(--ink45)}
.tlBadgeRow{margin-top:12px;display:flex;flex-wrap:wrap;gap:6px}
.tlDetail p{margin:14px 0 0;color:var(--ink70);max-width:62ch}
.tlWhy{margin:14px 0 0;border-left:3px solid var(--clay);background:var(--claySoft);padding:8px 12px;border-radius:0 10px 10px 0;font-size:12.5px;color:#5c2a1c;max-width:62ch}
.tlWhy b{display:block;font:700 9.5px var(--mono);letter-spacing:.1em;text-transform:uppercase;color:#8a3a26;margin-bottom:2px}
.tlBA{display:flex;align-items:stretch;gap:8px;margin-top:14px;max-width:760px}
.tlBA .m{flex:1}
.tlBA .arr{align-self:center;color:var(--ink45);font:15px var(--mono)}
.tlMeta{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:9px;max-width:760px;margin-top:14px}
.tlUI .m{border:1px solid var(--line);border-radius:10px;padding:7px 10px;background:var(--card);min-width:0}
.tlUI .m.after{border-color:var(--goldLine);background:var(--goldSoft)}
.tlUI .m i{display:block;font:9.5px var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--ink45);font-style:normal;margin-bottom:2px}
.tlUI .m span{font:12.5px var(--mono);overflow-wrap:anywhere}
.tlActs{display:flex;flex-wrap:wrap;gap:8px;margin-top:16px}
.tlBadge{display:inline-flex;align-items:center;gap:7px;border:1px solid var(--line);border-radius:999px;padding:3px 10px 3px 4px;background:var(--card);font:12px var(--sans);color:var(--ink);white-space:nowrap}
.tlBadge i{width:20px;height:20px;border-radius:50%;display:grid;place-items:center;background:var(--slateSoft);color:var(--tlSlate);flex:none}
.tlBadge i svg{width:11px;height:11px}
.tlBadge em{font:700 9.5px var(--mono);letter-spacing:.1em;color:var(--tlSlate);font-style:normal;text-transform:uppercase}
.tlBadge.sm{font-size:11px;padding:1px 8px 1px 2px;gap:5px}.tlBadge.sm i{width:17px;height:17px}.tlBadge.sm em{font-size:8.5px}
.tlAround{border-left:1px solid var(--line);padding-left:24px;display:grid;gap:4px;align-content:start}
.tlAround .tlLbl{margin-bottom:6px}
.tlArw{position:relative;display:grid;grid-template-columns:34px minmax(0,1fr);gap:10px;align-items:center;border:0;background:transparent;text-align:left;padding:6px 8px;border-radius:10px;transition:transform .18s var(--tlE)}
.tlArw::before{content:"";position:absolute;inset:0;border-radius:inherit;background:var(--card2);opacity:0;transition:opacity .18s}
.tlArw:hover::before{opacity:1}.tlArw:hover{transform:translateX(2px)}
.tlArw.cur::before{opacity:1;background:var(--goldSoft)}
.tlArw .sv{position:relative;display:block;width:34px;height:34px}
.tlArw .sv svg{width:100%;height:100%;display:block}
.tlArw div{position:relative;min-width:0}
.tlArw b{display:block;font:600 12px var(--sans);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tlArw span{font:10.5px var(--mono);color:var(--ink45)}
.tlEmpty{color:var(--ink45);font-size:12.5px}
.tlLegend{display:grid;grid-template-columns:repeat(auto-fill,minmax(112px,1fr));gap:14px 8px}
.tlLegend figure{margin:0;display:grid;justify-items:center;gap:6px;text-align:center}
.tlLegend .sv{width:96px;height:96px}.tlLegend .sv svg{width:100%;height:100%;display:block}
.tlLegend figcaption{font:600 10.5px var(--sans);color:var(--ink70)}
.tlLegend figcaption small{display:block;font:9px var(--mono);color:var(--ink45);letter-spacing:.06em;text-transform:uppercase;font-weight:400}
.tlUI.compact .tlBar,.tlUI.compact .tlGrid,.tlUI.compact .tlDetail,.tlUI.compact .tlNow{display:none}
.tlUI.compact{height:100%;justify-content:center}
.tlUI.compact .tlTop{border-bottom:0;padding:0;flex:1 1 auto;align-items:center;flex-wrap:nowrap}
.tlUI.compact .tlRail{flex:1 1 auto;max-width:none;margin:0}
.tlUI.compact .tlStops{grid-template-columns:repeat(var(--n,9),minmax(0,1fr))!important;row-gap:0}
.tlUI.compact .tlStop .tlCnt{top:3px;left:calc(50% + 15px);font-size:7.5px;padding:1.5px 4px}
.tlUI.compact .tlTrack,.tlUI.compact .tlFill{display:block;top:11px}
.tlUI.compact .tlStop{gap:2px}
.tlUI.compact .tlStop .tlSeal{width:24px;height:24px}
.tlUI.compact .tlStop>span{font-size:7.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tlUI.compact .tlCxStamp{width:min(150px,30%)}
.tlUI.compact .tlMsg{left:50%;top:50%;transform:translate(-50%,-50%);max-width:100%;padding:3px 10px;font-size:11px;gap:6px;box-shadow:none;white-space:nowrap}
@media (max-width:1100px){.tlDetIn{grid-template-columns:120px minmax(0,1fr)}.tlBig{width:120px;height:120px}.tlAround{grid-column:1/-1;border-left:0;padding-left:0;border-top:1px solid var(--line);padding-top:14px}}
@media (max-width:900px){.tlRail{flex-basis:100%}.tlStops{grid-template-columns:repeat(6,minmax(0,1fr));row-gap:10px}.tlTrack,.tlFill{display:none}}
.tlNowSeal{width:92px;height:92px;flex:none;transform:rotate(var(--rot,0deg))}
.tlNowSeal svg,.tlMini svg{display:block;width:100%;height:100%;overflow:visible}
.tlNowSeal.cx{width:118px;height:118px;margin:-8px 0;pointer-events:none;mix-blend-mode:multiply;transform:rotate(-11deg)}
.tlBlock{display:inline-flex;align-items:baseline;gap:9px;min-width:0;max-width:100%;font:13px/1.45 var(--sans);color:#7a5a1d;background:var(--goldSoft);border:1px solid var(--goldLine);border-radius:10px;padding:5px 12px}
.tlBlock b{font:700 9.5px var(--mono);letter-spacing:.1em;text-transform:uppercase;color:#8a6a24;flex:none}
.tlCxList{flex:1 1 100%;display:flex;flex-direction:column;gap:3px;min-width:0;margin:2px 0 6px;font:12.5px/1.4 var(--sans)}
.tlCxList .h{font-weight:700;color:var(--clay,#b0563f)}
.tlCxList .s{display:flex;align-items:baseline;gap:7px;min-width:0}
.tlCxList .s i{font-style:normal;font-weight:800;width:1em;flex:none;text-align:center}
.tlCxList .s.ok i{color:var(--sage,#4f7a5a)}.tlCxList .s.wait i,.tlCxList .s.wait{color:#8a6a24}.tlCxList .s.bad i,.tlCxList .s.bad{color:var(--clay,#b0563f)}.tlCxList .s.back i{color:var(--sage,#4f7a5a)}
.tlCxList small{opacity:.7;font-size:11px;white-space:nowrap}
.tlMini{position:relative;width:26px;height:26px;margin:0 1px;padding:0;border:0;background:none;flex:none;cursor:pointer;vertical-align:middle}
/* a stamp stays put under the pointer (a touch larger); wireNow zooms its full face .f onto a 122px layer above it */
.tlMini>span{position:absolute;inset:0;border-radius:50%;transform:rotate(var(--rot,0deg));transition:transform .22s cubic-bezier(.2,.8,.2,1);pointer-events:none}
.tlMini .f{visibility:hidden}
.tlMini:hover,.tlMini:focus-visible{z-index:6;outline:0}
.tlMini:hover .s,.tlMini:focus-visible .s{transform:rotate(var(--rot,0deg)) scale(1.12)}
.tlExp{position:fixed;z-index:2147483001;left:0;top:0;width:272px;pointer-events:none;background:var(--card,#fffefb);color:var(--ink,#1c1a17);border:1px solid var(--line,#e7e1d6);border-radius:12px;padding:11px 14px 9px;box-shadow:0 1px 0 rgba(255,255,255,.6) inset,0 14px 34px rgba(30,26,20,.16),0 2px 6px rgba(30,26,20,.06);font:12px/1.4 var(--sans,system-ui,sans-serif);opacity:0;display:none}
.tlExp::before{content:"";position:absolute;left:var(--ax,50%);top:-6px;width:10px;height:10px;margin-left:-5px;background:inherit;border-left:1px solid var(--line,#e7e1d6);border-top:1px solid var(--line,#e7e1d6);transform:rotate(45deg)}
.tlExp.up::before{top:auto;bottom:-6px;transform:rotate(225deg)}
.tlExp .xh,.tlPinH{display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin-bottom:7px}
.tlExp .xh b{font:500 15px/1.2 var(--serif,Georgia,serif);color:var(--ink)}
.tlExp .xs,.tlPinH .xs{flex:none;font:700 8.5px var(--mono,monospace);letter-spacing:.1em;text-transform:uppercase;color:var(--ink45);border-radius:999px;padding:2px 7px;background:var(--card2,#f6f2ea)}
.tlExp .xs.done,.tlPinH .xs.done{color:#3c5a39;background:var(--sageSoft,#e8efe3)}
.tlExp .xs.now,.tlPinH .xs.now{color:#7a5a1d;background:var(--goldSoft,#f6eedc)}
.tlExp .xs.stopped,.tlExp .xs.gone,.tlPinH .xs.stopped,.tlPinH .xs.gone{color:#8a3a26;background:var(--claySoft,#f4e3dc)}
.tlExp .xf{margin-top:8px;padding-top:6px;border-top:1px solid var(--line2,#efe9df);font:9.5px var(--mono,monospace);letter-spacing:.04em;color:var(--ink45)}
.tlReq{list-style:none;margin:0;padding:0;display:grid;gap:6px}
.tlReq .rq{display:grid;grid-template-columns:16px minmax(0,1fr);gap:8px;align-items:start;color:var(--ink70)}
.tlReq .rq>i{width:14px;height:14px;margin-top:1px;border-radius:50%;border:1.5px solid var(--ink25,#c4bdb0);display:grid;place-items:center}
.tlReq .rq.ok>i{border:0;background:var(--sage,#6f8d6a);color:#fff}.tlReq .rq.ok>i svg{width:9px;height:9px}
.tlReq .rq.person>i{border-color:#c79a3a}.tlReq .rq.stop>i{border-color:var(--clay,#b0563f)}
.tlReq .rq.after>i,.tlReq .rq.next>i{border-style:dashed}
.tlReq .rq span{min-width:0;overflow-wrap:anywhere}
.tlReq .rq.ok span{color:var(--ink)}
.tlReq .rq small{display:block;font:10px var(--mono,monospace);color:var(--ink45);margin-top:1px}
.tlReq .rq em{font:700 8.5px var(--mono,monospace);letter-spacing:.09em;text-transform:uppercase;font-style:normal;color:var(--ink45);margin-right:6px}
.tlReq .rq.person em{color:#7a5a1d}.tlReq .rq.stop em{color:#8a3a26}
.tlReq .rq.more{grid-template-columns:1fr;padding-left:24px;font:10px var(--mono,monospace);color:var(--ink45)}
.tlReqF{display:flex;flex-wrap:wrap;gap:4px 12px;margin-top:12px;font:10.5px var(--mono,monospace);color:var(--ink45)}
.tlReqF .tlLbl{flex-basis:100%;margin-bottom:1px}
.tlStepReq{margin-top:16px;max-width:620px;border-top:1px solid var(--line2);padding-top:12px}
.tlStepReq .tlPinH b{font:500 15px var(--serif)}
.tlPin .tlReq{max-width:620px;margin-top:14px;gap:9px}.tlPin .tlReq .rq{font-size:13px}
.tlPath2{display:grid;gap:2px}
.tlPath2 button{display:grid;grid-template-columns:26px minmax(0,1fr) auto;gap:10px;align-items:center;border:0;background:transparent;text-align:left;padding:5px 8px;border-radius:9px;transition:background .18s,transform .18s var(--tlE)}
.tlPath2 button:hover{background:var(--card2)}.tlPath2 button.cur{background:var(--goldSoft)}
.tlPath2 .sv{width:26px;height:26px}.tlPath2 .sv svg{width:100%;height:100%;display:block}
.tlPath2 b{font:600 12px var(--sans)}.tlPath2 span{font:9.5px var(--mono);color:var(--ink45);letter-spacing:.04em;text-transform:uppercase}
.tlPath2 button.later .sv,.tlPath2 button.gone .sv{opacity:.45}
.tlSt.ghost[data-stage]{cursor:pointer}
.tlExp.side::before{display:none}
.tlExp.on{pointer-events:auto}
.tlStop.pinned .tlSeal::before{content:"";position:absolute;inset:-5px;border-radius:50%;border:1.5px solid var(--gold);box-shadow:0 0 0 4px rgba(202,168,97,.18);animation:tlSelIn .32s var(--tlSpring) both}
.tlStop.pinned>span{color:#7a5a1d}
@media (prefers-reduced-motion:reduce){.tlUI *,.tlUI *::before,.tlUI *::after,.tlMini>span{animation-duration:.001s!important;animation-iteration-count:1!important;transition-duration:.001s!important}.tlUI .tlSpin{animation:tlSpin 1.4s linear infinite!important}}
`;
  function css() {
    if (doc.getElementById("tlUiCss")) return;
    const s = doc.createElement("style"); s.id = "tlUiCss"; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }
  const badge = (e, sm) => `<span class="tlBadge${sm ? " sm" : ""}"><i>${iconSvg((LANE[e.lane] || LANE.office).ic)}</i><em>${esc(e.print ? e.print.where : stationName(e))}</em>${esc(whoOf(e))}</span>`;
  // roomier than it was (Paul, 28 Sep: "this entire section is way too crowded"): the seals sit further apart on a
  //  taller lane, and a day is wider, so nothing crowds even when a day holds three or four of them
  const COL = 50, LANE_H = 58, TOP = 40, PAD = 18, IDLE = 34, DAYMIN = 150, AXIS = 26, H = TOP + LANES.length * LANE_H + AXIS;
  const laneY = k => TOP + (LANE[k] || LANE.office).i * LANE_H + LANE_H / 2;
  const dayKey = t => { const d = new Date(t); return d.getFullYear() * 10000 + (d.getMonth() + 1) * 100 + d.getDate(); };
  const midnight = t => { const d = new Date(t); d.setHours(0, 0, 0, 0); return +d; };
  /** Day columns and each event's place (x in its day, y on its lane). */
  function layout(evs) {
    const days = []; let cur = null;
    for (const e of evs) { const k = dayKey(e.at); if (!cur || cur.k !== k) { cur = { k, at: e.at, evs: [] }; days.push(cur); } cur.evs.push(e); }
    let x = 0, alt = 0; const cols = [];
    days.forEach((d, i) => {
      if (i) {
        const gap = Math.round((midnight(d.at) - midnight(days[i - 1].at)) / 864e5) - 1;
        if (gap > 2) { cols.push({ idle: 1, x, w: IDLE + 22, label: gap + " days" }); x += IDLE + 22; }
        else for (let g = 1; g <= gap; g++) { const dd = new Date(midnight(days[i - 1].at) + g * 864e5 + 36e5 * 12); cols.push({ idle: 1, x, w: IDLE, label: DAYN[dd.getDay()] }); x += IDLE; }
      }
      // its head's second line ("8:52 AM – 3:44 PM · 9", one time when there is one) never runs into the next day
      const t0 = timeOf(d.evs[0].at), t1 = timeOf(d.evs[d.evs.length - 1].at), span = (t0 === t1 ? t0 : t0 + " – " + t1) + " · " + d.evs.length;
      const w = Math.max(DAYMIN, PAD * 2 + d.evs.length * COL, Math.ceil(26 + span.length * 6.1));
      cols.push({ x, w, at: d.at, evs: d.evs, alt: alt++ % 2, span });
      d.evs.forEach((e, j) => { e.x = x + PAD + j * COL + COL / 2; e.y = laneY(e.lane); });
      x += w;
    });
    return { cols, w: x };
  }
  const pathD = pts => { let d = ""; pts.forEach((e, i) => { if (!i) { d = `M${e.x} ${e.y}`; return; } const p = pts[i - 1], mx = (p.x + e.x) / 2; d += ` C${mx} ${p.y} ${mx} ${e.y} ${e.x} ${e.y}`; }); return d; };

  /* ════ one order's timeline, read once and shared (adversarial wave 2) ════
     feed(orderId, { pollMs }) → { orderId, answer, error, loading, refresh({ force }), subscribe(fn(kind)) → off, destroy() }
     The order view's Overview, header rail and Timeline tab each read the order's timeline for themselves (three
     timelineGet an open, a fourth at every return to the tab, and two polls): they now take one feed (mount's
     opts.feed). A refresh while one is on its way joins it; one not forced reuses an answer younger than the poll (the
     feed is live), so a return to the tab reads nothing; Retry and the poll force. While anyone listens it reads again
     every pollMs with the tab visible; kinds: "wait", "data", "error". destroy() stops it, and an answer still on its
     way is dropped. */
  function feed(orderId, o) {
    o = o || {};
    const id = digits(orderId), pollMs = Math.max(250, +o.pollMs || POLL), subs = new Set();
    const F = { orderId: id, answer: null, error: "", loading: null, at: 0, tried: 0, dead: false };
    let pollT = 0, seq = 0;
    const emit = kind => { for (const fn of [...subs]) { try { fn(kind, F); } catch (err) { warn("feed", err); } } };
    function arm() {
      clearTimeout(pollT); pollT = 0;
      if (F.dead || !subs.size) return;
      pollT = setTimeout(() => { pollT = 0; if (!F.dead && doc.visibilityState !== "hidden") F.refresh({ force: true }); }, pollMs);
    }
    function done(my, fn) { if (F.dead || my !== seq) return F.answer; F.loading = null; F.tried = Date.now(); fn(); arm(); return F.answer; }
    F.refresh = r => {
      if (F.dead) return Promise.resolve(F.answer);
      if (F.loading) return F.loading;
      if (!(r && r.force) && F.answer && Date.now() - F.at < pollMs) return Promise.resolve(F.answer);
      const my = ++seq, api = root.OrderTimeline;
      // (deferred: a throw before the read still reaches the subscribers after "wait", never before it)
      const p = F.loading = Promise.resolve().then(() => {
        if (!id) throw new Error("no order number");
        if (!api || typeof api.get !== "function") throw new Error("the timeline is not loaded on this page");
        return api.get(id);
      }).then(j => done(my, () => { F.answer = j || {}; F.error = ""; F.at = F.tried; emit("data"); }),
        e => done(my, () => { F.error = String((e && e.message) || e || "failed"); emit("error"); }));
      emit("wait");
      return p;
    };
    F.subscribe = fn => { subs.add(fn); if (!pollT && !F.loading) arm(); return () => { subs.delete(fn); if (!subs.size) { clearTimeout(pollT); pollT = 0; } }; };
    const onVis = () => {
      if (F.dead || !subs.size) return;
      if (doc.visibilityState === "hidden") { clearTimeout(pollT); pollT = 0; return; }
      if (!F.loading && Date.now() - F.tried >= pollMs) F.refresh({ force: true }); else if (!F.loading && !pollT) arm();
    };
    doc.addEventListener("visibilitychange", onVis);
    F.destroy = () => { if (F.dead) return; F.dead = true; clearTimeout(pollT); pollT = 0; subs.clear(); doc.removeEventListener("visibilitychange", onVis); };
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
    const S = { events: [], shown: [], shownKeys: new Set(), byKey: new Map(), every: [], allKeys: new Map(), pieces: Array.isArray(opts.pieces) ? opts.pieces : [], piece: opts.piece || null, cancelled: null, where: null, D: null, sel: null, legend: false, hl: new Set(), sig: "",
      loaded: false, loading: null, error: "", dead: false, lastLoad: 0, seq: 0, nowX: 0, pendingFocus: null, hlDone: false };
    const timers = new Set();
    const later = (fn, ms) => { const t = setTimeout(() => { timers.delete(t); if (!S.dead) fn(); }, ms); timers.add(t); return t; };
    const cancelT = t => { if (t) { clearTimeout(t); timers.delete(t); } return 0; };
    const box = doc.createElement("div");
    box.className = "tlUI" + (compact ? " compact" : "");
    box.setAttribute("data-order", orderId);
    box.innerHTML = `<div class="tlTop"><div class="tlNow"><span class="tlLbl">Now</span><div class="tlNowT">Finding where this order is…</div><div class="tlNowS"></div></div>` +
      `<div class="tlRail" role="group" aria-label="Main steps"><span class="tlTrack"></span><span class="tlFill"></span><div class="tlStops"></div></div></div>` +
      `<div class="tlBar"><div class="tlChips" role="toolbar" aria-label="Show"></div><div class="tlBarR"><span class="tlBusy" hidden><i class="tlSpin"></i><span>Checking for new steps</span></span><span class="tlLive" hidden><i></i>LIVE</span><span class="tlSum"></span></div></div>` +
      `<div class="tlGrid"><div class="tlLanes"></div><div class="tlScroll"><div class="tlCanvas"></div></div><div class="tlMsg" hidden></div></div>` +
      `<div class="tlDetail" aria-live="polite"></div><div class="tlLoupe" aria-hidden="true"></div><div class="tlExp" role="tooltip"></div>`;
    el.appendChild(box);
    // opts.toolbar: the host's own bar (the order view's tab row, spec §1) takes the filters, so the lanes keep the height
    const tb = !compact && opts.toolbar && typeof opts.toolbar.appendChild === "function" ? opts.toolbar : null, bar = box.querySelector(".tlBar");
    if (tb) { bar.classList.add("inTools"); tb.appendChild(bar); }
    const $ = s => box.querySelector(s) || (tb ? bar.querySelector(s) : null), $$ = s => [...box.querySelectorAll(s)];
    if (compact) $(".tlRail").appendChild($(".tlMsg"));   // the rail alone: its wait and error lines sit on it
    const loupe = $(".tlLoupe"), scroller = $(".tlScroll"), exp = $(".tlExp");
    let unsub = null, unfeed = null, pollT = 0, busyT = 0, loupeFor = null, loupeAt = null, hideA = null;
    // what the lanes' names and the detail show now: a redraw that would write the same leaves them (and their layout) alone
    let lanesHtml = "", detHtml = "";
    const detUid = "tlDet" + (++UID);
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
        $(".tlNowS").innerHTML = `<i class="tlSpin"></i><span>Loading the timeline</span>`;
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
        $(".tlNowS").innerHTML = `<span>${esc(S.error)}</span><button type="button" class="tlLink tlRetry">Retry</button>`;
      } else {
        const r = $(".tlSum"); if (r) r.innerHTML = `<span class="err">Couldn't check for new steps <button type="button" class="tlLink tlRetry">Retry</button></span>`;
      }
    }
    /** The server's answer, merged with what this page recorded meanwhile (still on its way to the server). */
    function apply(j) {
      const next = new Map();
      for (const x of Array.isArray(j.events) ? j.events : []) { const e = norm(x); if (e) next.set(e.key, e); }
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
      return S.every.map(e => e.key + (e.pending ? "*" : "")).join("|") + "#" + (S.cancelled ? S.cancelled.at || 1 : 0) + "#" + JSON.stringify(S.where || "");
    }
    function afterFirst() {
      if (S.pendingFocus != null) { const id = S.pendingFocus; S.pendingFocus = null; if (focus(id)) return; }
      if (S.hlDone) return; S.hlDone = true;
      const h = opts.highlight == null ? "" : String(opts.highlight).trim();
      if (!h || digits(h) === orderId) return;
      const k = findKey(h);
      if (k) { select(k, 0, { scroll: true }); return; }
      const q = h.toLowerCase(), hits = S.events.filter(e => hay(e).includes(q));
      if (!hits.length) return;
      S.hl = new Set(hits.map(e => e.key));
      for (const b of $$(".tlSt[data-key]")) b.classList.toggle("hl", S.hl.has(b.dataset.key));
      select(hits[hits.length - 1].key, 0, { scroll: true });
    }

    /* ── painting ── */
    function repaint(o) {
      o = o || {};
      const D = S.D = deriveNow();
      // every event stays in S.events (the host, and the step explainer, read them all); only the seals are drawn
      S.shown = withPrints(S.events); S.shownKeys = new Set(S.shown.map(e => e.key));
      paintNow(D, o); paintRail(D, o);
      tell();
      if (compact) { if (loupeFor) { const b = loupeFor; hideLoupe(true); if (b.isConnected && b.matches(HOVER) && b.matches(":hover")) showLoupe(b, true); } return; }
      paintChips(); paintCanvas(D, o); paintSum();
      const fresh = (o.fresh || []).filter(k => S.shownKeys.has(k));
      if (S.legend) return;
      if (S.pin && renderPin(true)) return;
      if (fresh.length) {
        // a new step: follow it when the reader was on the latest one, otherwise leave their reading alone
        const newest = fresh.map(k => S.byKey.get(k)).sort(byAt).pop(), prevLast = S.shown.filter(e => !fresh.includes(e.key)).pop();
        if (!S.sel || (prevLast && S.sel === prevLast.key)) select(newest.key, 1, { scroll: true, quiet: false });
        else if (S.shownKeys.has(S.sel)) renderDetail(S.sel, 0, true);
        else select(newest.key, 1, { scroll: true });
      } else if (S.sel && S.shownKeys.has(S.sel)) renderDetail(S.sel, 0, true);
      else if (S.shown.length) select(S.shown[S.shown.length - 1].key, 0, { scroll: o.first, quiet: true });
      else $(".tlDetail").innerHTML = `<p class="tlEmpty">${esc(S.events.length ? "No milestone yet. The first seal lands here the moment this order reaches one." : "Nothing is recorded for this order yet. Each step shows here the moment it happens.")}</p>`;
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
      if (p) return railed(derive(S.events, S.cancelled), withDone(stagesFor(p.line), S.events));
      const D = derive(S.events, S.cancelled, whereNow());
      if (S.pieces.length < 2) return railed(D, railNow(S.events));
      const sum = summary(S.every, S.pieces, S.cancelled), far = D.step;
      if (D.hand && !sum.each.every(x => x.D.hand)) D.hand = null;   // (completed by hand: every piece of it)
      D.step = Math.min(D.step, sum.step);
      // a step that every piece taking it has passed is not what the order waits on (a stud's Welded, done, while the
      // necklaces wait on Assembled): the next step is the first one some piece still has to reach
      for (const r of sum.rail) { if (r.i <= D.step) continue; if (r.of && r.n >= r.of && r.i <= far) D.step = r.i; else break; }
      return railed(D, sum.rail.map(r => r.s), sum);
    }
    /** The host's piece switcher: pieces (as opts.pieces; omitted keeps them) and the piece shown (null: all of them).
     *  The rail, the lanes and the detail cross over to it, from the side it lies on (dir). */
    function setPieces(list, key, dir) {
      if (S.dead) return;
      const ps = Array.isArray(list) ? list : S.pieces, sigP = x => JSON.stringify(x.map(p => [p.key, p.qty, p.pools, p.sheets]));
      key = key || null;
      const moved = key !== S.piece, changed = sigP(ps) !== sigP(S.pieces);
      if (!moved && !changed) return;
      S.pieces = ps; S.piece = key; narrow();
      if (S.sel && !S.byKey.has(S.sel)) S.sel = null;
      if (!S.loaded) return;
      hideLoupe(true);
      repaint({ first: moved });
      if (moved) for (const x of [$(".tlRail"), compact ? null : $(".tlGrid"), compact ? null : $(".tlDetail")]) if (x) anim(x, [{ opacity: 0, transform: `translateX(${(dir || 0) * 14}px)` }, { opacity: 1, transform: "none" }], 340, { easing: E });
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
    function paintNowSub() {
      const D = S.D; if (!D) return;
      const s = $(".tlNowS");
      if (D.cancelled) {
        const c = D.cancelled, who = c.source === "etsy" || c.by === "Etsy" ? "on Etsy" : "by " + (c.by || "a person");
        s.innerHTML = `<span>Cancelled ${esc(who)} · ${esc(shortWhen(c.at))} · ${esc(ago(c.at))}</span>${c.why ? `<span class="why" title="${esc(c.why)}">${esc(c.why)}</span>` : ""}`;
        return;
      }
      const W = D.W, e = D.last; if (!e) { s.innerHTML = ""; return; }
      const st = W.station || e.station || "", at = +W.at || e.at;
      const who = { station: st, by: W.by || whoOf(e), lane: STATION_LANE[st] || e.lane, source: e.source };
      const next = D.cur >= 0 && !D.hold ? STAGES[D.cur].l : "", pool = W.sheetId ? S.events.filter(x => x.sheetId === W.sheetId).map(poolOf).filter(Boolean).pop() || "" : "";
      s.innerHTML = `${badge(who, true)}<span>${esc(shortWhen(at))} · ${esc(ago(at))}</span>${next ? `<span>next: ${esc(next)}</span>` : ""}` +
        (W.sheetId ? `<span>on ${esc(W.sheet || W.sheetId)}</span>${opts.onSheet ? `<button type="button" class="tlLink tlOpenSheet" data-sheet="${esc(W.sheetId)}" data-pool="${esc(pool)}">Open sheet</button>` : ""}` : "");
    }
    function paintRail(D, o) {
      const wrap = $(".tlStops"), rail = $(".tlRail"), R = D.rail || STAGES.map((s, i) => ({ s, i }));
      // (the steps of the piece shown, or of all of them: drawn again when they change)
      const keys = R.map(r => r.s.k).join(" ");
      if (wrap.dataset.keys !== keys) { wrap.dataset.keys = keys; rail.style.setProperty("--n", R.length); wrap.innerHTML = R.map(r => `<button type="button" class="tlStop f" data-stage="${r.s.k}"><i class="tlSeal"></i><span>${esc(r.s.l)}</span><em class="tlCnt" hidden></em></button>`).join(""); }
      const nodes = [...wrap.children];
      R.forEach(({ s, i }, j) => {
        const n = nodes[j], st = D.stages[i];
        // all pieces: a step some pieces reached and others not yet says how many ("2 of 3")
        const sr = D.sum && D.sum.rail.find(r => r.i === i), part = sr && sr.n > 0 && sr.n < sr.of ? `${sr.n} of ${sr.of}` : "";
        let c;
        if (!S.loaded) c = "f";
        else if (D.cancelled) c = i < D.stop ? "d" : i === D.stop ? "x" : "f gone";
        else c = i <= D.step ? "d" : i === D.cur ? "c" : D.hand ? "f gone" : "f";   // (completed by hand: the rest skipped)
        // a step passed with no event of its own (an older order, or one done off the record) still shows as done
        const ev = c === "d" ? st.first : null;
        const sig = c + "|" + (ev ? ev.key : "") + (c === "c" && D.hold ? "|h" : "") + (c === "x" ? "|" + D.cancelled.at : "") + "|" + part;
        if (n.dataset.sig === sig) return;
        const was = n.dataset.sig || "";
        n.dataset.sig = sig;
        n.className = "tlStop " + c + (c === "c" && D.hold ? " paused" : "");
        const seal = n.querySelector(".tlSeal"), rot = ev ? rotOf(ev) : 0;
        n.style.setProperty("--rot", rot + "deg");
        seal.innerHTML = c === "d" ? stampSvg(ev || { key: "d-" + s.k, type: s.kind, at: 0 }, false) : c === "x" ? stampSvg({ key: "x-" + s.k, type: D.cancelled.source === "etsy" ? "etsyCancelled" : "cancelled", at: D.cancelled.at }, false)
          : stampSvg({ key: "g-" + s.k, type: s.kind, at: 0 }, false, { ghost: 1 });
        n.querySelector("span").textContent = c === "x" ? "Cancelled" : s.l;
        const cnt = n.querySelector(".tlCnt"); if (cnt) { cnt.hidden = !part; cnt.textContent = part; }
        // (who did it and where: "Welded · Marco R. · Welding · done Tuesday, Sep 29, 2026 · 10:15 AM")
        const say = (c === "d" ? (ev ? `${[s.l, whoOf(ev)].concat(placeOf(ev) ? [placeOf(ev)] : []).join(" · ")} · done ${longWhen(ev.at)}` : `${s.l}: done`) : c === "c" ? `${s.l}: ${D.hold ? "on hold" : "next"}` : c === "x" ? `Cancelled here, ${longWhen(D.cancelled.at)}` : D.hand ? `${s.l}: skipped, the order was completed by hand` : `${s.l}: still to come`) + (sr && D.sum ? ` · ${sr.n} of ${sr.of} piece${sr.of === 1 ? "" : "s"}` : "");
        n.setAttribute("aria-label", say); n.removeAttribute("title");   // the step explainer (below) replaces the dark tooltip
        if ((c === "d" || c === "x") && !was.startsWith(c)) {
          if (o.first) anim(seal, [{ opacity: 0, transform: `rotate(${rot}deg) scale(.6)` }, { opacity: 1, transform: `rotate(${rot}deg) scale(1)` }], 380, { delay: 120 + i * 45, easing: SPRING, fill: "backwards" });
          else anim(seal, [{ transform: `rotate(${rot}deg) scale(.6)` }, { transform: `rotate(${rot}deg) scale(1.5)`, offset: .45 }, { transform: `rotate(${rot}deg) scale(1)` }], 700, { easing: SPRING });
        }
      });
      const at = D.cancelled ? D.stop : D.cur < 0 ? R[R.length - 1].i : Math.max(0, D.cur), idx = Math.max(0, R.findIndex(r => r.i === at));
      $(".tlFill").style.transform = `scaleX(${(idx / Math.max(1, R.length - 1)).toFixed(4)})`;
      rail.classList.toggle("cx", !!D.cancelled);
      let cx = rail.querySelector(".tlCxStamp:not(.out)");
      const cxK = D.cancelled ? [D.cancelled.at, D.cancelled.by, D.cancelled.source, D.stop].join("|") : "";
      const cxAt = () => {
        cx.dataset.k = cxK; cx.innerHTML = cancelSvg(D.cancelled);
        // over the steps it will not reach, so the ✕ where it stopped stays readable
        const from = Math.min(Math.max(0, R.findIndex(r => r.i === D.stop)) + 1, R.length - 1), mid = ((from + R.length - 1) / 2 + .5) / R.length;
        cx.style.left = `clamp(135px, ${(mid * 100).toFixed(2)}%, calc(100% - 135px))`;
      };
      if (D.cancelled && cx && cx.dataset.k !== cxK) cxAt();
      if (D.cancelled && !cx) {
        cx = doc.createElement("div"); cx.className = "tlCxStamp"; rail.appendChild(cx); cxAt();
        anim(cx, [{ transform: "translate(-50%,-50%) rotate(-2deg) scale(1.9) translateY(-20px)", opacity: 0 }, { transform: "translate(-50%,-50%) rotate(-8deg) scale(.96)", opacity: 1, offset: .62 }, { transform: "translate(-50%,-50%) rotate(-6deg) scale(1)", opacity: 1 }], 700, { delay: o.first ? 450 : 0, easing: DROP, fill: "backwards" });
      } else if (!D.cancelled && cx) {
        cx.classList.add("out");
        const a = anim(cx, [{ opacity: 1 }, { opacity: 0 }], 240);
        if (a) a.finished.then(() => cx.remove(), () => cx.remove()); else cx.remove();
      }
    }
    /** One quiet chip: the legend of the seals. (The filters are gone — only milestones are drawn.) */
    function paintChips() {
      const html = `<button type="button" class="tlChip${S.legend ? " on" : ""}" data-legend aria-pressed="${S.legend}">Stamps</button>`;
      const c = $(".tlChips"); if (c.innerHTML !== html) c.innerHTML = html;
    }
    function paintSum() {
      const r = $(".tlSum"), lv = $(".tlLive"); if (!r) return;
      if (lv) lv.hidden = !live || !S.loaded;
      const e = S.shown[S.shown.length - 1] || S.events[S.events.length - 1];
      const n = S.shown.length;
      r.textContent = e ? `${n} milestone${n === 1 ? "" : "s"}${leftOutNote()} · last ${shortWhen(e.at)} · ${whoOf(e)}` : "";
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
      const selLane = S.sel && S.byKey.get(S.sel) ? S.byKey.get(S.sel).lane : "";
      const html = LANES.map(L => { const w = !who[L.k] || !who[L.k].size ? "—" : L.k === "office" ? "Operator" : [...who[L.k]].join(", "); /* (the Office lane names no one: Paul, 29 Sep, "it just should say Operator") */ return `<div class="tlLane${L.st ? " stn" : ""}${selLane === L.k ? " on" : ""}" data-lane="${L.k}"><b>${iconSvg(L.ic)}${esc(L.l)}</b><span title="${esc(w)}">${esc(w)}</span></div>`; }).join("");
      if (html !== lanesHtml) { lanesHtml = html; $(".tlLanes").innerHTML = html; }
    }
    function paintCanvas(D, o) {
      const hov = loupeFor; hideLoupe(true);
      const cv = $(".tlCanvas"), evs = S.shown, oldNow = S.nowX;
      paintLanes();
      const { cols, w } = layout(evs), last = evs[evs.length - 1];
      const nowX = last ? last.x + COL * .75 : PAD + COL / 2;
      const ghosts = D.cancelled || D.hand ? [] : (D.rail || STAGES.map((s, i) => ({ s, i }))).filter(g => g.i > D.step).map((g, j) => ({ key: "ghost-" + g.s.k, type: g.s.kind, s: g.s, x: nowX + COL * (j + .9), y: laneY(g.s.lane) }));
      const W = Math.ceil(Math.max(w, nowX + COL * (ghosts.length + .6) + 16));
      const thisYear = new Date().getFullYear();
      const dayCols = cols.map(c => { const d = c.idle ? null : new Date(c.at); return `<div class="tlDay${c.alt ? " alt" : ""}${c.idle ? " idle" : ""}" style="left:${c.x}px;width:${c.w}px"><div class="dh">${c.idle ? esc(c.label) : `${DAYN[d.getDay()]} · ${MON[d.getMonth()]} ${d.getDate()}${d.getFullYear() !== thisYear ? " " + d.getFullYear() : ""}<small>${esc(c.span)}</small>`}</div></div>`; }).join("");
      const lines = LANES.map((L, i) => `<div class="tlLaneLine" style="top:${TOP + (i + 1) * LANE_H}px"></div>`).join("");
      const future = ghosts.length && last ? pathD([last].concat(ghosts)) : "";
      // the cancel line: at the cancel stamp (or, with only the record, where its time falls)
      let cxX = 0;
      if (D.cancelled) { const ce = evs.filter(e => CANCEL_TYPES.has(e.type)).pop(); cxX = ce ? ce.x + COL / 2 : ((evs.filter(e => e.at <= D.cancelled.at).pop() || { x: nowX - COL * .75 }).x + COL / 2); }
      const Wh = D.W, stn = Wh.station && !["sheet", "waiting", "review", "held"].includes(Wh.stage) ? STATION_NAME[Wh.station] || Wh.station : "";
      const nowLbl = D.hand ? ["COMPLETED", shortWhen(D.hand.at)].concat(personOf(D.hand) ? [personOf(D.hand).toUpperCase()] : []).join(" · ") : D.hold ? "NOW · ON HOLD" : Wh.stage === "completed" || (D.cur < 0 && last) ? "COMPLETED" : stn ? "NOW · AT " + stn.toUpperCase() : last ? "NOW · " + String(Wh.label || "").toUpperCase() : "NOW";
      cv.style.width = W + "px"; cv.style.height = H + "px";
      const back = dayCols + lines +
        `<svg class="tlPath" width="${W}" height="${H}" aria-hidden="true">${evs.length > 1 ? `<path d="${pathD(evs)}" fill="none" stroke="var(--gold)" stroke-width="1.6" stroke-opacity=".55" stroke-linecap="round"/>` : ""}${future ? `<path d="${future}" fill="none" stroke="var(--ink25)" stroke-width="1.4" stroke-dasharray="3 5"/>` : ""}</svg>` +
        (D.cancelled ? `<div class="tlAfterCx" style="left:${cxX}px"></div><div class="tlNowLine cx" style="left:${cxX}px"><span>CANCELLED · ${esc(shortWhen(D.cancelled.at))}</span></div>` : `<div class="tlNowLine" style="left:${nowX}px"><span>${esc(nowLbl)}</span></div>`);
      const clsOf = e => `tlSt${S.sel === e.key ? " sel" : ""}${e.pending ? " pend" : ""}${S.hl.has(e.key) ? " hl" : ""}`;
      const posOf = e => `left:${e.x}px;top:${e.y}px;--s:${sizeOf(e)}px;--rot:${rotOf(e)}deg`, sayOf = e => `${labelOf(e.type)} · ${titleOf(e)} · ${longWhen(e.at)} · ${whoOf(e)}${placeOf(e) ? " · " + placeOf(e) : ""}`;
      const ghostHtml = ghosts.map(g => `<span class="tlSt ghost" data-stage="${esc(g.s.k)}" style="left:${g.x}px;top:${g.y}px;--s:${sizeOf(g)}px;--rot:0deg" aria-label="${esc("To come: " + g.s.l)}">${stampSvg(g, false, { ghost: 1 })}</span>`).join("");
      // the stamps already drawn are kept (a live step parses one stamp, not every one: a redraw of 100 stays in a frame);
      // the days, lines, path, NOW line and ghosts are drawn again
      const kept = new Map();
      if (!o.first) for (const b of [...cv.children]) { if (b.tagName === "BUTTON" && b.dataset.key && S.shownKeys.has(b.dataset.key) && !kept.has(b.dataset.key)) kept.set(b.dataset.key, b); else b.remove(); }
      if (!kept.size) {
        cv.innerHTML = back + evs.map(e => `<button type="button" class="${clsOf(e)}" data-key="${esc(e.key)}" data-sv="${esc(e.type + "|" + e.at)}" style="${posOf(e)}" aria-label="${esc(sayOf(e))}">${stampSvg(e, false)}</button>`).join("") + ghostHtml;
      } else {
        cv.insertAdjacentHTML("afterbegin", back);
        let prev = cv.querySelector(".tlNowLine");
        for (const e of evs) {
          let b = kept.get(e.key);
          if (!b) { b = doc.createElement("button"); b.type = "button"; b.dataset.key = e.key; }
          const sv = e.type + "|" + e.at, cls = clsOf(e), pos = posOf(e), say = sayOf(e);
          if (b.dataset.sv !== sv) { b.dataset.sv = sv; b.innerHTML = stampSvg(e, false); }
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
      const nl0 = cv.querySelector(".tlNowLine:not(.cx) span"); if (nl0 && (nl0.offsetWidth || nowLbl.length * 6.7 + 12) + 8 > nowX) { nl0.style.right = "auto"; nl0.style.left = "8px"; }
      if (o.first) {
        const p = cv.querySelector(".tlPath"); anim(p, [{ opacity: 0 }, { opacity: 1 }], 900, { easing: SLIDE });
        [...cv.querySelectorAll(".tlSt")].forEach((b, i) => { const r = b.style.getPropertyValue("--rot"); anim(b, [{ transform: `rotate(${r}) scale(.3)`, opacity: 0 }, { transform: `rotate(${r}) scale(1)`, opacity: b.classList.contains("ghost") ? .42 : b.classList.contains("dim") ? .13 : 1 }], 420, { delay: Math.min(i * 18, 540), easing: SPRING, fill: "backwards" }); });
        // opens at "now"
        if (scroller.clientWidth && W > scroller.clientWidth) scroller.scrollLeft = Math.max(0, nowX - scroller.clientWidth * .6);
      } else {
        // (the canvas keeps its width while it is redrawn, so the scroll stays where the reader left it)
        const nl = cv.querySelector(".tlNowLine:not(.cx)");
        if (nl && oldNow && Math.abs(oldNow - nowX) > 1) anim(nl, [{ transform: `translateX(${oldNow - nowX}px)` }, { transform: "none" }], 760, { easing: SLIDE });
        for (const k of o.fresh || []) {
          const e = S.byKey.get(k), b = e && cv.querySelector(`.tlSt[data-key="${cssEsc(k)}"]`); if (!b) continue;
          const r = rotOf(e), s = sizeOf(e);
          anim(b, [{ transform: `rotate(${r - 8}deg) scale(2.4)`, opacity: 0 }, { transform: `rotate(${r + 2}deg) scale(.9)`, opacity: 1, offset: .55 }, { transform: `rotate(${r}deg) scale(1)`, opacity: 1 }], 900, { easing: DROP });
          if (!reduced()) {
            const ring = doc.createElement("span"); ring.className = "tlInkRing";
            ring.style.cssText = `left:${e.x - s / 2}px;top:${e.y - s / 2}px;width:${s}px;height:${s}px;border-color:${INK[kindOf(e.type).ink]}`;
            cv.appendChild(ring);
            const a = anim(ring, [{ transform: "scale(.8)", opacity: 0 }, { transform: "scale(.9)", opacity: .9, offset: .5 }, { transform: "scale(2.2)", opacity: 0 }], 1100, { delay: 350, easing: "ease-out", fill: "both" });
            if (a) a.finished.then(() => ring.remove(), () => ring.remove()); else ring.remove();
          }
        }
      }
      // a live step while a stamp is hovered: its loupe stays up (the stamp was kept)
      if (hov && hov.isConnected && hov.matches(HOVER) && hov.matches(":hover")) showLoupe(hov, true);
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
    function toggleLegend() {
      S.legend = !S.legend; paintChips();
      if (!S.legend) { renderDetail(S.sel || (S.shown[S.shown.length - 1] || {}).key, 0); return; }
      // the legend shows the seals that are actually drawn, nothing else
      const t = Date.now(), types = Object.keys(KIND).filter(k => sealed({ type: k }));
      const det = $(".tlDetail");
      det.innerHTML = `<div class="tlLegend">${types.map(k => { const e = { key: "legend-" + k, type: k, at: t, by: "Name", lane: kindOf(k).lane, data: null }; const sh = kindOf(k).sh; return `<figure><span class="sv" style="transform:rotate(${rotOf(e)}deg)">${stampSvg(e, true, { tex: false })}</span><figcaption>${esc(labelOf(k))}<small>${sh === "m" ? "milestone" : sh === "a" ? "alert" : "event"}</small></figcaption></figure>`; }).join("")}${printLegend(t)}</div>`;
      anim(det.firstChild, [{ opacity: 0, transform: "translateY(6px)" }, { opacity: 1, transform: "none" }], 280);
    }
    /** Chooses one event: its stamp gets the gold ring, its lane lights, its detail opens below. */
    function select(key, dir, o) {
      o = o || {};
      key = sealKey(key);
      if (!key) return false;
      S.sel = key; if (S.legend) { S.legend = false; paintChips(); }
      if (S.pin) { S.pin = null; for (const b of $$(".tlStop.pinned")) b.classList.remove("pinned"); }
      for (const b of $$(".tlSt[data-key]")) b.classList.toggle("sel", b.dataset.key === key);
      const e = S.byKey.get(key);
      for (const l of $$(".tlLane")) l.classList.toggle("on", l.dataset.lane === e.lane);
      renderDetail(key, dir, !!o.quiet);
      if (o.scroll) scrollToEv(e);
      return true;
    }
    function scrollToEv(e) {
      if (!e || !scroller.clientWidth || e.x == null) return;
      const sl = scroller.scrollLeft, cw = scroller.clientWidth;
      if (e.x < sl + 40 || e.x > sl + cw - 150) {
        const left = Math.max(0, e.x - cw * .6);
        try { scroller.scrollTo({ left, behavior: reduced() ? "auto" : "smooth" }); } catch (_) { scroller.scrollLeft = left; }
      }
    }
    function renderDetail(key, dir, quiet) {
      const det = $(".tlDetail"), evs = S.shown, i = evs.findIndex(x => x.key === key);
      if (i < 0) return;
      const e = evs[i], D = S.D || derive(S.events, S.cancelled);
      const ae = doc.activeElement, focused = ae && ae !== det && det.contains(ae) ? (ae.dataset && ae.dataset.step) || (ae.classList.contains("tlArw") ? "arw" : "*") : null;
      const around = evs.slice(Math.max(0, i - 2), i + 3);
      const { out: ba, used } = pairsOf(e.data);
      const facts = factsOf(e, used);
      let why = reasonOf(e);
      if (!why && CANCEL_TYPES.has(e.type) && D.cancelled) why = D.cancelled.why;
      const desc = e.text && e.text.length > 90 ? e.text : "";
      const whyLbl = CANCEL_TYPES.has(e.type) ? "Why it was cancelled" : e.type === "removed" ? "Why it was taken off" : e.type === "held" ? "Why it was held" : "Reason";
      const html = `<div class="tlDetIn">` +
        `<span class="tlBig" style="--rot:${rotOf(e)}deg">${stampSvg(e, true, { uid: detUid })}</span>` +
        `<div class="tlDetMain"><span class="tlLbl">${esc(labelOf(e.type))} · milestone ${i + 1} of ${evs.length}${e.pending ? " · saving…" : ""}</span>` +
        `<h3>${esc(titleOf(e))}</h3><div class="tlWhen">${esc(longWhen(e.at))} · ${esc(ago(e.at))}</div>` +
        `<div class="tlBadgeRow">${badge(e)}</div>` +
        (desc ? `<p>${esc(desc)}</p>` : "") +
        (why ? `<div class="tlWhy"><b>${esc(whyLbl)}</b>${esc(why)}</div>` : "") +
        ba.map(p => `<div class="tlBA"><div class="m"><i>${esc(p.what ? p.what + " · before" : "Before")}</i><span>${esc(fmt(p.a))}</span></div><span class="arr" aria-hidden="true">→</span><div class="m after"><i>${esc(p.what ? p.what + " · after" : "After")}</i><span>${esc(fmt(p.b))}</span></div></div>`).join("") +
        (facts.length ? `<div class="tlMeta">${facts.map(([k, v]) => `<div class="m"><i>${esc(k)}</i><span>${esc(v)}</span></div>`).join("")}</div>` : "") +
        nextHtml(D) +
        `<div class="tlActs"><button type="button" class="btn ghost sm" data-step="-1"${i ? "" : " disabled"}>‹ Earlier</button><button type="button" class="btn ghost sm" data-step="1"${i < evs.length - 1 ? "" : " disabled"}>Later ›</button>` +
        (e.sheetId && opts.onSheet ? `<button type="button" class="btn sm tlOpenSheet" data-sheet="${esc(e.sheetId)}" data-pool="${esc(poolOf(e))}">Open sheet</button>` : "") + `</div></div>` +
        `<div class="tlAround"><span class="tlLbl">Around this step</span>${around.map(a => `<button type="button" class="tlArw${a.key === e.key ? " cur" : ""}" data-key="${esc(a.key)}"><span class="sv" style="transform:rotate(${rotOf(a)}deg)">${stampSvg(a, false, { tex: false })}</span><div><b>${esc(titleOf(a, 60))}</b><span>${esc(shortWhen(a.at))} · ${esc(whoOf(a))}</span></div></button>`).join("")}</div></div>`;
      if (quiet && html === detHtml && det.firstChild && det.firstChild.classList.contains("tlDetIn")) return;
      det.innerHTML = detHtml = html;
      if (focused && !det.contains(doc.activeElement)) { const b = (focused === "arw" && det.querySelector(".tlArw.cur")) || det.querySelector(`[data-step="${focused}"]:not(:disabled)`) || det.querySelector("[data-step]:not(:disabled)"); if (b) b.focus({ preventScroll: true }); }
      if (quiet) return;
      const inn = det.firstChild;
      anim(inn, [{ opacity: 0, transform: `translateX(${(dir || 0) * 14}px)${dir ? "" : " translateY(6px)"}` }, { opacity: 1, transform: "none" }], 280);
      const big = inn.querySelector(".tlBig"), r = rotOf(e);
      anim(big, [{ transform: `scale(1.35) rotate(${r - 6}deg)`, opacity: 0 }, { opacity: 1, offset: .55 }, { transform: `scale(1) rotate(${r}deg)`, opacity: 1 }], 500, { easing: SPRING });
    }
    function stepBy(d) {
      const i = S.shown.findIndex(e => e.key === S.sel), j = clamp((i < 0 ? S.shown.length - 1 : i) + d, 0, S.shown.length - 1);
      if (!S.shown[j] || S.shown[j].key === S.sel) return;
      const onStamp = doc.activeElement && doc.activeElement.matches && doc.activeElement.matches(".tlSt[data-key]") && box.contains(doc.activeElement);
      select(S.shown[j].key, d, { scroll: true });
      if (onStamp) { const b = box.querySelector(`.tlSt[data-key="${cssEsc(S.shown[j].key)}"]`); if (b) b.focus({ preventScroll: true }); }
    }
    function findKey(id) {
      if (id == null) return null;
      const s = String(id); if (S.byKey.has(s)) return s;
      const tail = s.split("~").pop().replace(/[^\w.:-]/g, "_");
      for (const [k, e] of S.byKey) if (e.id === s || k.split("~").slice(1).join("~") === tail) return k;
      return null;
    }

    /* ── the loupe: a hovered dot's seal zooms to 122px ABOVE it on its own layer (zoomSpot), so nothing clips it and
       the dot stays in sight under the pointer ── */
    function evOfEl(b) {
      if (b.dataset.key) return S.byKey.get(b.dataset.key) || null;
      const i = STAGES.findIndex(s => s.k === b.dataset.stage); if (i < 0 || !S.D) return null;
      if (b.classList.contains("x") && S.D.cancelled) return S.events.filter(e => CANCEL_TYPES.has(e.type)).pop() || { key: "x", type: "cancelled", at: S.D.cancelled.at, by: S.D.cancelled.by, lane: "office", data: null };
      return S.D.stages[i].first;
    }
    const liftOf = b => b.classList.contains("tlStop") ? b.querySelector(".tlSeal") : b;
    function showLoupe(b, quiet) {
      const e = evOfEl(b); if (!e) return;
      if (hideA) { try { hideA.cancel(); } catch (_) {} hideA = null; }
      if (loupeFor && loupeFor !== b) liftOf(loupeFor).classList.remove("lifted");
      // above the dot itself; a flip goes under the whole stop, so a rail step keeps its name in sight
      const lift = liftOf(b), d = lift.getBoundingClientRect(), bb = b.getBoundingClientRect(), rot = rotOf(e);
      const r = { left: d.left, width: d.width, top: d.top, bottom: Math.max(d.bottom, bb.bottom) };
      // the seal alone: what the step is and still needs is the explainer card under the dot (it replaced the dark caption)
      loupe.innerHTML = `<div class="lf" style="transform:rotate(${rot}deg)">${stampSvg(e, true)}</div>`;
      loupeAt = zoomIn(loupe, r, quiet);
      loupeFor = b; lift.classList.add("lifted");
    }
    function hideLoupe(now) {
      const b = loupeFor; if (!b) return;
      loupeFor = null;
      const lift = liftOf(b);
      const done = () => { hideA = null; lift.classList.remove("lifted"); if (!loupeFor) loupe.style.display = "none"; };
      if (now || !lift.isConnected) return done();
      hideA = zoomOut(loupe);
      if (hideA) hideA.finished.then(done, () => { lift.classList.remove("lifted"); }); else done();
    }
    const HOVER = ".tlSt[data-key], .tlStop.d, .tlStop.x";
    function onOver(ev) { const b = ev.target.closest && ev.target.closest(HOVER); if (b && b !== loupeFor && box.contains(b)) showLoupe(b); }
    function onOut(ev) { const b = ev.target.closest && ev.target.closest(HOVER); if (b && b === loupeFor && !b.contains(ev.relatedTarget)) hideLoupe(); }
    function onScroll() { hideLoupe(true); hideExp(true); }

    /* ── the step explainer (Paul, 28 Sep, point 5): hovering a step (a rail's stop, a lane stamp, a dashed stamp to come)
       shows a small card BELOW its dot, what is done and what is still missing (the zoomed seal sits above: the dot stays
       seen); a click on a step not done yet pins the fuller version in the detail below, never a pop-up on a pop-up.
       Escape or a click elsewhere lets it go. opts.context() is what the host knows of the order (requirementsOf). ── */
    let expFor = null, expA = null, expT = 0;
    const ctxOf = () => { try { return (typeof opts.context === "function" ? opts.context() : opts.context) || null; } catch (err) { warn("context", err); return null; } };
    // (S.D is the rail drawn: the piece shown's own steps, or the order's)
    const reqOf = i => requirementsOf(i, { events: S.events, D: S.D || deriveNow(), context: ctxOf() });
    const EXP = ".tlStop, .tlSt[data-key], .tlSt.ghost[data-stage]";
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
      expFor = b; expT = cancelT(expT);
      // a seal that had no room above its dot (a rail at the top of the view) opened under it: the card goes under the seal
      const z = loupeFor === b && loupeAt ? loupeAt : null, lp = z && !z.up ? z : null;
      const whole = lp ? { getBoundingClientRect: () => { const r = b.getBoundingClientRect(); return { left: r.left, right: r.right, width: r.width, top: r.top, height: r.height, bottom: Math.max(r.bottom, lp.y + ZSZ) }; } } : b;
      // (a short view puts the card beside the dot: beside its seal too, never over it)
      expA = placeExp(exp, hand ? head : head + reqCard(q), b.classList.contains("tlStop") ? b.querySelector(".tlSeal") || b : b, whole, z && { left: z.x, right: z.x + ZSZ });
      exp.classList.add("on");   // (the card takes the pointer: moving onto it keeps it)
    }
    function hideExp(now) {
      expT = cancelT(expT);
      if (!expFor || !exp) return;
      expFor = null; exp.classList.remove("on");
      if (now) { if (expA) { try { expA.cancel(); } catch (_) {} } expA = null; exp.style.display = "none"; exp.innerHTML = ""; return; }
      expA = fadeExp(exp, expA);
    }
    function nextHtml(D) {
      if (!D || D.cancelled || D.cur < 0) return "";
      const q = reqOf(D.cur); if (!q || !q.need.length) return "";
      return `<div class="tlStepReq"><div class="tlPinH" style="justify-content:flex-start"><span class="tlLbl">Next for this order</span><span class="xs now">${esc(q.label)}</span></div>${reqLines(q, false)}<button type="button" class="tlLink" data-pin="${esc(q.k)}" style="margin-top:9px">All that ${esc(q.label)} needs</button></div>`;
    }
    function expOver(ev) { const b = ev.target.closest && ev.target.closest(EXP); if (b && b !== expFor && box.contains(b)) showExp(b); }
    // the pointer leaving the step for its card (across the gap between them) keeps the card; leaving both lets it go
    const expLater = () => { expT = cancelT(expT); expT = later(() => { expT = 0; if (expFor && !exp.matches(":hover") && !expFor.matches(":hover")) hideExp(); }, 160); };
    function expOut(ev) { const b = ev.target.closest && ev.target.closest(EXP); if (b && b === expFor && !b.contains(ev.relatedTarget)) { if (ev.relatedTarget && exp.contains(ev.relatedTarget)) return; expLater(); } }
    function expLeave(ev) { if (expFor && !(ev.relatedTarget && expFor.contains(ev.relatedTarget))) expLater(); }
    function expFocus(ev) { const b = ev.target.closest && ev.target.closest(".tlStop"); if (b && b.matches(":focus-visible")) showExp(b); }
    function expBlur(ev) { const b = ev.target.closest && ev.target.closest(".tlStop"); if (b && b === expFor) hideExp(); }
    /** Pins a step's fuller explainer in the detail below; → true when shown. */
    function pin(i) {
      const s = STAGES[i]; if (!s || compact || !S.loaded) return false;
      const was = S.pin; S.pin = s.k; hideExp(true);
      renderPin(was === s.k);
      for (const b of $$(".tlStop")) b.classList.toggle("pinned", b.dataset.stage === s.k);
      return true;
    }
    function unpin() {
      if (!S.pin) return;
      S.pin = null; for (const b of $$(".tlStop.pinned")) b.classList.remove("pinned");
      const k = S.sel && S.shownKeys.has(S.sel) ? S.sel : (S.shown[S.shown.length - 1] || {}).key;
      if (k) renderDetail(k, 0); else $(".tlDetail").innerHTML = detHtml = "";
    }
    function renderPin(quiet) {
      const i = STAGES.findIndex(s => s.k === S.pin), q = i >= 0 ? reqOf(i) : null, det = $(".tlDetail");
      if (!q) { S.pin = null; return false; }
      const D = S.D, ev = q.state === "done" ? D.stages[i].first : null, s = STAGES[i];
      const seal = ev ? stampSvg(ev, true, { uid: detUid }) : stampSvg({ key: "pin-" + s.k, type: s.kind, at: 0 }, false, { ghost: 1 });
      const sheet = (ev && ev.sheetId && ev) || S.events.filter(e => e.sheetId).pop();
      const path = (D.rail || STAGES.map((x, j) => ({ s: x, i: j }))).map(({ s: x, i: j }) => { const r = reqOf(j), f = D.stages[j].first; return `<button type="button" class="${r.state}${j === i ? " cur" : ""}" data-pin="${x.k}"><span class="sv">${f && r.state === "done" ? stampSvg(f, false, { tex: false }) : stampSvg({ key: "p-" + x.k, type: x.kind, at: 0 }, false, { ghost: 1 })}</span><b>${esc(x.l)}</b><span>${esc(r.state === "done" && f ? shortWhen(f.at) : STATE_WORD[r.state] || "")}</span></button>`; }).join("");
      const html = `<div class="tlDetIn tlPin"><span class="tlBig" style="--rot:${ev ? rotOf(ev) : 0}deg">${seal}</span>` +
        `<div class="tlDetMain"><div class="tlPinH" style="justify-content:flex-start"><span class="tlLbl">${q.n ? `Step ${q.n} of ${q.of}` : "Not a step of this order"}</span><span class="xs ${q.state}${partDone(q) ? " now" : ""}">${esc(partDone(q) ? "Part done" : STATE_WORD[q.state] || "")}</span></div>` +
        `<h3>${esc(s.l)}</h3><div class="tlWhen">${q.state === "done" || q.state === "skipped" ? "What was done" : q.need.some(n => n.kind === "person") ? "Waiting on a person" : "What is still missing"}</div>` +
        reqLines(q, true) +
        `<div class="tlActs">${ev ? `<button type="button" class="btn ghost sm" data-open-ev="${esc(ev.key)}">Show the step</button>` : ""}${sheet && opts.onSheet ? `<button type="button" class="btn ghost sm tlOpenSheet" data-sheet="${esc(sheet.sheetId)}" data-pool="${esc(poolOf(sheet))}">Open sheet</button>` : ""}<button type="button" class="btn ghost sm" data-unpin>Close</button></div></div>` +
        `<div class="tlAround"><span class="tlLbl">The path</span><div class="tlPath2">${path}</div></div></div>`;
      if (quiet && html === detHtml) return true;
      det.innerHTML = detHtml = html;
      if (!quiet) anim(det.firstChild, [{ opacity: 0, transform: "translateY(6px)" }, { opacity: 1, transform: "none" }], 280);
      return true;
    }
    /** A click anywhere but the pinned step, its detail and the rails lets the pin go. */
    function onDocDown(ev) {
      if (!S.pin) return;
      const t = ev.target;
      if (t && t.closest && (t.closest(".tlDetail") || t.closest(".tlStop") || t.closest(".tlSt"))) return;
      unpin();
    }
    function onDocKey(ev) { if (ev.key === "Escape" && S.pin) { unpin(); } }

    /* ── clicks and keys ── */
    function onClick(ev) {
      const t = ev.target; if (!t || !t.closest) return;
      const pk = t.closest("[data-pin]"); if (pk) { pin(STAGES.findIndex(s => s.k === pk.dataset.pin)); return; }
      if (t.closest("[data-unpin]")) { unpin(); return; }
      const oe = t.closest("[data-open-ev]"); if (oe) { S.pin = null; for (const b of $$(".tlStop.pinned")) b.classList.remove("pinned"); select(oe.dataset.openEv, 0, { scroll: true }); return; }
      const gh = t.closest(".tlSt.ghost[data-stage]"); if (gh) { pin(STAGES.findIndex(s => s.k === gh.dataset.stage)); return; }
      const chip = t.closest(".tlChip"); if (chip) { toggleLegend(); return; }
      const os = t.closest(".tlOpenSheet"); if (os) { ev.preventDefault(); hideLoupe(true); try { if (typeof opts.onSheet === "function") opts.onSheet(os.dataset.sheet, os.dataset.pool || null); } catch (e) { try { console.warn("[OrderTimelineUI] onSheet:", e); } catch (_) {} } return; }
      const st = t.closest(".tlSt[data-key], .tlArw[data-key]");
      if (st) { const k = st.dataset.key, a = S.shown.findIndex(e => e.key === S.sel), b = S.shown.findIndex(e => e.key === k); select(k, a < 0 || a === b ? 0 : b > a ? 1 : -1, { scroll: st.classList.contains("tlArw") }); return; }
      const step = t.closest("[data-step]"); if (step) { stepBy(+step.dataset.step || 0); return; }
      const stop = t.closest(".tlStop"); if (stop) { stageClick(stop, ev); return; }
      if (t.closest(".tlRetry")) { load(true); return; }
    }
    function stageClick(stop, click) {
      const e = evOfEl(stop);
      const i = STAGES.findIndex(s => s.k === stop.dataset.stage), last = S.D && i >= 0 && S.D.stages[i].last;
      const target = stop.classList.contains("x") ? e : last || e;
      if (!target || !target.key || !S.byKey.has(target.key)) {
        if (i >= 0 && S.loaded) {
          if (compact) { if (click) click.preventDefault(); handOver({ stage: STAGES[i].k }); return; }
          if (pin(i)) return;
        }
        anim(stop.querySelector(".tlSeal"), [{ transform: "translateX(0)" }, { transform: "translateX(-3px)" }, { transform: "translateX(3px)" }, { transform: "translateX(0)" }], 300);
        return;
      }
      if (compact) { if (click) click.preventDefault(); handOver(target); return; }
      const a = S.shown.findIndex(x => x.key === S.sel), b = S.shown.findIndex(x => x.key === sealKey(target.key));
      select(target.key, a < 0 || a === b ? 0 : b > a ? 1 : -1, { scroll: true });
    }
    function onKey(ev) {
      if (ev.key !== "ArrowLeft" && ev.key !== "ArrowRight") return;
      const t = ev.target; if (!t || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName) || t.isContentEditable) return;
      if (!t.closest || !t.closest(".tlGrid, .tlDetail")) return;
      ev.preventDefault(); stepBy(ev.key === "ArrowLeft" ? -1 : 1);
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
        return pin(i);
      }
      const id = ev && typeof ev === "object" ? (ev.key && S.byKey.has(ev.key) ? ev.key : ev.id || ev.eventId || ev.key) : ev;
      if (!S.loaded) { S.pendingFocus = id; return false; }
      const k = findKey(id); if (!k) return false;
      const e = S.byKey.get(k);
      if (compact) { handOver(e); return true; }
      if (!select(k, 0, { scroll: true })) return false;
      const b = box.querySelector(`.tlSt[data-key="${cssEsc(sealKey(k))}"]`);
      if (b) { try { b.scrollIntoView({ block: "nearest", inline: "nearest", behavior: reduced() ? "auto" : "smooth" }); } catch (_) {} }
      return true;
    }
    function destroy() {
      if (S.dead) return;
      S.dead = true;
      for (const t of timers) clearTimeout(t);
      timers.clear(); pollT = busyT = 0;
      try { if (typeof unsub === "function") unsub(); } catch (_) {}
      try { if (typeof unfeed === "function") unfeed(); } catch (_) {}
      unsub = unfeed = null;
      doc.removeEventListener("visibilitychange", onVis);
      box.removeEventListener("click", onClick); box.removeEventListener("pointerover", onOver); box.removeEventListener("pointerout", onOut); box.removeEventListener("keydown", onKey);
      box.removeEventListener("pointerover", expOver); box.removeEventListener("pointerout", expOut); box.removeEventListener("focusin", expFocus); box.removeEventListener("focusout", expBlur); exp.removeEventListener("pointerleave", expLeave);
      doc.removeEventListener("pointerdown", onDocDown, true); doc.removeEventListener("keydown", onDocKey);
      scroller.removeEventListener("scroll", onScroll);
      try { for (const a of box.getAnimations({ subtree: true })) a.cancel(); } catch (_) {}
      if (tb) { bar.removeEventListener("click", onClick); bar.remove(); }
      box.remove();
    }

    box.addEventListener("click", onClick); box.addEventListener("pointerover", onOver); box.addEventListener("pointerout", onOut); box.addEventListener("keydown", onKey);
    box.addEventListener("pointerover", expOver); box.addEventListener("pointerout", expOut); box.addEventListener("focusin", expFocus); box.addEventListener("focusout", expBlur); exp.addEventListener("pointerleave", expLeave);
    if (!compact) { doc.addEventListener("pointerdown", onDocDown, true); doc.addEventListener("keydown", onDocKey); }
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
    return { refresh, destroy, focus, setPieces };
  }

  /* ════ the order view's "Where it is now" card (spec §3, §8) ════
     nowStamps(events, { ev, cancelled }) → { seal, recent } HTML: the latest step's seal at 92px (a cancelled order: the
     118px CANCELLED ORDER / DO NOT PROCEED seal) and the last six stamps at 26px, whose full face grows out on hover.
     wireNow(card, onOpen) once that HTML is in the page: a stamp click → onOpen({ id }); the cancel seal drops in once. */
  function nowStamps(events, o) {
    css(); o = o || {};
    const evs = (events || []).map(norm).filter(Boolean).sort(byAt), c = o.cancelled;
    // the seal of the milestone it is at now — never a "read" or a "?" (Paul, 28 Sep) — and, when something is holding
    // it up, that in plain words in place of the old row of stamps
    const seals = sealsOf(evs), asked = o.ev && norm(o.ev);
    const last = (asked && sealed(asked) ? asked : seals[seals.length - 1]) || null;
    const blocker = c ? null : blockerOf(evs);
    let seal = "";
    if (c) {
      const etsy = c.type === "etsyCancelled" || c.source === "etsy" || /^etsy$/i.test(c.by || "");
      seal = `<div class="tlNowSeal cx" data-at="${+c.at || 0}">${stampSvg({ key: "now-cx", type: etsy ? "etsyCancelled" : "cancelled", at: +c.at || 0, by: c.by || (etsy ? "Etsy" : ""), source: etsy ? "etsy" : "", data: { ring: "CANCELLED ORDER", foot: "DO NOT PROCEED" } }, true, { uid: "tlNowCx" })}</div>`;
    } else if (last) seal = `<div class="tlNowSeal" data-key="${esc(last.key)}" style="--rot:${rotOf(last)}deg">${stampSvg(last, true, { uid: "tlNowSeal" })}</div>`;
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
    card.querySelectorAll(".tlMini[data-tl-ev]").forEach(b => {
      b.onclick = ev => { ev.preventDefault(); nowZoom(card, null); if (typeof onOpen === "function") { try { onOpen({ id: b.dataset.tlEv }); } catch (err) { warn("onOpen", err); } } };
      b.onpointerenter = b.onfocus = () => nowZoom(card, b);
      b.onpointerleave = b.onblur = () => { if (card._tlZoomFor === b) nowZoom(card, null); };
    });
    if (card._tlZoomFor && !card._tlZoomFor.isConnected) nowZoom(card, null, true);   // repainted under the pointer
    const cx = card.querySelector(".tlNowSeal.cx"), s = card.querySelector(".tlNowSeal:not(.cx)");
    if (cx && card._tlCx !== cx.dataset.at) {
      card._tlCx = cx.dataset.at;
      anim(cx, [{ transform: "rotate(-4deg) scale(1.9) translateY(-30px)", opacity: 0 }, { transform: "rotate(-13deg) scale(.96)", opacity: 1, offset: .62 }, { transform: "rotate(-11deg) scale(1)", opacity: 1 }], 700, { easing: DROP, delay: 200, fill: "backwards" });
    }
    if (!cx) card._tlCx = null;
    if (s && card._tlLast && card._tlLast !== s.dataset.key) { const r = s.style.getPropertyValue("--rot"); anim(s, [{ transform: `rotate(${r}) scale(1.6)`, opacity: 0 }, { transform: `rotate(${r}) scale(.94)`, opacity: 1, offset: .55 }, { transform: `rotate(${r}) scale(1)`, opacity: 1 }], 600, { easing: DROP }); }
    card._tlLast = s ? s.dataset.key : null;
  }

  /* the card's stamps zoom like the timeline's: b's full face on the card's own 122px layer above it (zoomSpot); null
     eases it away. The face's ids are renamed in the copy so the page never holds two of one id. */
  function nowZoom(card, b, now) {
    let L = card._tlZoom;
    if (b) {
      const f = b.querySelector(".f"); if (!f) return;
      if (!L || !L.isConnected) { L = card._tlZoom = doc.createElement("div"); L.className = "tlLoupe tlNowZoom"; L.setAttribute("aria-hidden", "true"); card.appendChild(L); }
      if (card._tlZoomA) { try { card._tlZoomA.cancel(); } catch (_) {} card._tlZoomA = null; }
      let h = f.innerHTML; const ids = [...new Set([...h.matchAll(/\bid="([^"]+)"/g)].map(m => m[1]))];
      for (const id of ids) h = h.split(`id="${id}"`).join(`id="${id}-z"`).split(`#${id})`).join(`#${id}-z)`).split(`#${id}"`).join(`#${id}-z"`);
      L.innerHTML = `<div class="lf" style="transform:rotate(${b.style.getPropertyValue("--rot") || "0deg"})">${h}</div>`;
      const quiet = !!card._tlZoomFor; card._tlZoomFor = b;
      zoomIn(L, b.getBoundingClientRect(), quiet);
      return;
    }
    card._tlZoomFor = null; if (!L || L.style.display === "none") return;
    const done = () => { card._tlZoomA = null; if (!card._tlZoomFor) L.style.display = "none"; };
    const a = now || !L.isConnected ? null : zoomOut(L);
    card._tlZoomA = a; if (a) a.finished.then(done, () => {}); else done();
  }

  /** The icon of the lane an event belongs to (the station badge's disc), as SVG markup; "" for no event. */
  function iconOf(x) { const e = x && norm(x); return e ? iconSvg((LANE[e.lane] || LANE.office).ic) : ""; }

  root.OrderTimelineUI = { mount, feed, stampSvg, derive, STAGES, stagesFor, ofPiece, summary, isStud, engraveOf, KIND, labelOf, nowStamps, wireNow, iconOf, sealed, sealsOf, blockerOf, requirementsOf, explainOn,
    stepOf, labelStepOf, personOf, placeOf, opStepOf, handOf };
})(typeof window !== "undefined" ? window : globalThis);
