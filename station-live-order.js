/*  station-live-order.js — the order each station person has in hand RIGHT NOW (Paul, 5 Oct 2026: "the current order each station is
 *  processing, the thumbnails, the QR code, the person who is working on it, how long since it was scanned"). The station pages tell it
 *  to the console through the live layer of station-activity.js (StationActivity.working / idle / touch, plans/employee-hr/api.md);
 *  this file is the small page-side half they share, so every page says it the same way:
 *
 *    StationLiveOrder.start(rid, transactions, { note, title, customer, holdMs })   an order was scanned or opened: it is in hand
 *    StationLiveOrder.batch(ids, transactions, { note, holdMs, title, key, sync, keepTime })   a batch of orders on one sheet (one order = start)
 *    StationLiveOrder.sheet(key, title, { transactions, note, customer, holdMs })   something in hand that names no order (a titled sheet)
 *    StationLiveOrder.note(text)                                                    the same order, a new short line (scan time kept)
 *    (sync: the same thing told again is a refresh, not a new scan; keepTime: what replaces what is in hand keeps its scan time)
 *    StationLiveOrder.end(rid?)                                                     it was completed, printed or closed: nothing in hand
 *    StationLiveOrder.pieces(transactions) → { pieces, pieceCount }                 the pieces of every Etsy line (CharmNestOrders.pieceCountOf, the quantity as always); an EARRING PAIR (matching or mismatched)
 *                                                                                    is a Left then a Right piece per unit (side "L" | "R", the line's group key, its size and number), a mismatched line the pool still makes as one piece per unit is one glued piece (both: true)
 *    StationLiveOrder.units(transactions, filter?) → number                         the pieces (efficiency "parts") of a list of lines, the one count every station page uses
 *    StationLiveOrder.kind(line) → { kind, mismatched, sided, pieces }              single | pair | mismatched | multi, read from the line alone (a station never holds the master)
 *    StationLiveOrder.tag(line) → "" | "Pair: Left + Right"                         the short words a cell adds for an earring pair line ("" for every other line: nothing changes on screen)
 *    StationLiveOrder.sides(line, rid?, max?) → [{ side, grp, of, n, both }]        the pair fields of the pieces one line makes, in order ([] for a line with no sides and no group)
 *    StationLiveOrder.ears(lines) → [{ side, n, of }]                               the stickers an order prints (QR Printer.html dataObj.pieces): a Left then a Right per pair, [] for an order with no earring pair
 *
 *  `transactions` are the Etsy lines the page already holds (quantity, listing_id, sku, title, variations): the thumbnails, the QR and
 *  the customer are looked up by the server from what the app already stores; nothing here asks Etsy for anything. The call carries
 *  the order id, the pieces (id, label, sku, listing id, size) and short plain words: never a PIN, address or message text.
 *  Best effort: without the live layer, with nobody signed in or when anything fails it does nothing, never throws, never waits.
 *  While an order is in hand a click or key on the page keeps it so (the library ends it after a quiet stretch, a sign-out, or when
 *  the page closes). */
(function () {
  "use strict";
  if (window.StationLiveOrder) return;
  const MAX_PIECES = 24, HOLD_MS = 15 * 60000, TOUCH_EVERY = 15000;
  const clean = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
  const digits = (v, n) => String(v == null ? "" : v).replace(/\D/g, "").slice(0, n);
  const lib = () => { try { return window.StationActivity || null; } catch (_) { return null; } };
  let cur = null, watching = false, lastTouch = 0;

  /** "S" | "M" | "L" ... from an Etsy line's size variation, or "" */
  function sizeOf(t) {
    try {
      for (const v of (Array.isArray(t && t.variations) ? t.variations : [])) {
        if (!/size/i.test(String((v && v.formatted_name) || ""))) continue;
        const x = clean(v.formatted_value, 30).toLowerCase();
        if (/^(extra[- ]?small|xs)\b/.test(x)) return "XS";
        if (/^(small|s)\b/.test(x)) return "S";
        if (/^(medium|m)\b/.test(x)) return "M";
        if (/^(large|l)\b/.test(x)) return "L";
        if (/^(extra[- ]?large|xl)\b/.test(x)) return "XL";
        return /^[A-Za-z0-9]{1,6}$/.test(x) ? x.toUpperCase() : "";
      }
    } catch (_) {}
    return "";
  }
  /* ── pairs (Paul, 9 and 10 Oct 2026): an earring pair (stud, hoop, huggie hoops, huggie charm set, "pair of earrings"; matching OR
     mismatched) is one LEFT and one RIGHT piece per unit of quantity (q units = q Left + q Right); a mismatched pair is the same with
     two different designs. Necklace discs, letters and singles are never sided. Station pages hold raw Etsy lines and never the master,
     so the count and the words come from ONE place, charm-nest-orders.js (CharmNestOrders.pieceCountOf, lineSignals, lineMismatched and
     PIECE_RULES, loaded by the pages before this file): the parts a station is credited with always agree with the pieces the pool
     makes, and when Paul's rules change the count changes with them. Without it (not loaded, or older than that) the rule is the one in
     force before: one piece per unit of every line, and a line is mismatched when its own words say so: the SKU (MISMATCHED,
     MISMATCHED_7134) or a variation value the buyer chose ("Mismatched pair"). The listing TITLE is not read for that: a title such as
     "Mismatched or matching studs" is on every line of the listing, whichever the buyer picked. The server's live layer
     (_stationLive.js dress) knows a mismatched design from the master and shows Left and Right on a piece the page did not split.
     A line that is not an earring pair and not mismatched is counted and told exactly as before. */
  const SAYS = /(?:^|[^A-Za-z])mis-?matched(?![A-Za-z])/i;
  const ordersLib = () => { try { const O = window.CharmNestOrders; return O && typeof O === "object" ? O : null; } catch (_) { return null; } };
  const skuOf = t => String((t && (t.sku || t.__sku)) || "");
  /* A line is a mismatched pair only when it is a pair of EARRINGS (ADVCOUNT, 9 Oct: a necklace charm whose SKU is "CAT(+FISH) - Cat Only", a necklace with the option "Silver • 2 symbols"
     or a note "not mismatched" was told "Pair: Left + Right" and printed a LEFT and a RIGHT sticker). What the buyer's words say (a two-designs option, a Left / Right option name, a
     note) counts only on a line sold as a pair; a SKU is never split here to guess two designs (a station holds no master, and a mismatched pair has the same count and sides as a
     matching one); one ear alone (Single) is never a pair of anything. */
  const personal = v => /personali[sz]ation|engraving text|custom text/i.test(String((v && (v.formatted_name != null ? v.formatted_name : v.name)) || ""));
  function saysMismatched(t) {
    try {
      if (!t || typeof t !== "object") return false;
      const O = ordersLib(), sig = O && typeof O.lineSignals === "function" ? O.lineSignals(t) : null;
      if (sig && sig.soldAs === "single") return false;
      if (t.__pair === "mismatched" || (t.__pair && t.__pair.mismatched === true)) return true;
      if (SAYS.test(skuOf(t))) return true;                                                // a design named MISMATCHED, MISMATCHED_7134
      const pair = sig ? sig.soldAs === "pair" : true;                                     // (without the intake file the old rule: the line's own words)
      for (const v of (Array.isArray(t.variations) ? t.variations : [])) {
        if (!pair && personal(v)) continue;                                                // a note on a line that is not an earring pair says nothing about pairs
        if (SAYS.test(String((v && (v.formatted_value != null ? v.formatted_value : v.value)) || ""))) return true;
      }
      if (pair && sig && O.lineSignals(Object.assign({}, t, { title: "" })).says === true) return true;   // (the listing title is on every line of the listing, whichever the buyer picked: not read)
    } catch (_) {}
    return false;
  }
  /** is this line sold as a PAIR of earrings (the line's own words: stud, hoop, huggie hoops, earrings; not "single earring")? */
  function soldAsPair(t) {
    try { const O = ordersLib(); return !!(O && typeof O.lineSignals === "function" && t && typeof t === "object" && O.lineSignals(t).soldAs === "pair"); } catch (_) { return false; }
  }
  const qtyOf = t => Math.max(1, Math.floor(Number(t && (t.quantity != null ? t.quantity : t.__qty))) || 1);
  /** how many pieces one line makes: CharmNestOrders.pieceCountOf (the Etsy quantity times what the rules say), else the Etsy quantity as always */
  function countOf(t) {
    let n = qtyOf(t);
    try { const O = ordersLib(); if (O && typeof O.pieceCountOf === "function") { const c = Math.floor(Number(O.pieceCountOf(t))); if (c >= 1) n = c; } } catch (_) {}
    return n;
  }
  /** what a line is as pieces: n pieces for q units; sided = the pieces are Left / Right ears (an earring pair made as a Left and a Right piece per unit, or a
   *  single earring whose line names its side); glued = a mismatched line the pool still makes as ONE piece per unit (left and right drawn together).
   *  When the intake file can list the pieces itself (CharmNestOrders.piecesOf: side per piece) its list is the word; else the line's words decide. */
  function describe(t) {
    const q = qtyOf(t), n = countOf(t), mis = saysMismatched(t);
    let sided = n === 2 * q && (mis || soldAsPair(t)), list = null;
    try { const O = ordersLib(); if (O && typeof O.piecesOf === "function") { const l = O.piecesOf(t); if (Array.isArray(l) && l.length === n) { list = l; sided = l.some(p => p && (p.side === "L" || p.side === "R")); } } } catch (_) {}
    return { q, n, mis, sided, list, glued: mis && !sided && n === q };
  }
  /** single | pair | mismatched | multi, from the line alone */
  function kind(t) {
    const d = describe(t);
    return { kind: d.mis ? (d.q === 1 && d.n <= 2 ? "mismatched" : "multi") : d.sided && d.n >= 2 ? (d.n === 2 ? "pair" : "multi") : d.n <= 1 ? "single" : "multi", mismatched: d.mis, sided: d.sided, pieces: d.n };
  }
  /** the short words a cell adds: "" for a line with no sides (nothing on screen changes) */
  const tag = t => { try { const d = describe(t); if (!d.mis && !(d.sided && d.n >= 2)) return ""; return d.q > 1 ? d.q + " pairs: Left + Right each" : "Pair: Left + Right"; } catch (_) { return ""; } };
  /** the pieces (efficiency "parts") of a list of lines; filter picks the lines that count (the Welding station counts its stud lines) */
  function units(list, filter) {
    let n = 0;
    try { for (const t of (Array.isArray(list) ? list : [])) { if (!t || typeof t !== "object") continue; if (typeof filter === "function" && !filter(t)) continue; n += countOf(t); } } catch (_) {}
    return n;
  }
  /** receipt:transaction, the group key of one order line (CharmNestPair.groupKey's own shape) */
  const groupOf = (t, rid) => {
    const r0 = t && (t.receipt_id != null ? t.receipt_id : t.receiptId != null ? t.receiptId : t.typedOrderNumber);
    const r = digits(r0, 20) || digits(rid, 20), x = digits(t && (t.transaction_id != null ? t.transaction_id : t.transactionId), 20);
    return r ? r + ":" + x : "";
  };
  /** The pair fields of the pieces one line makes, in order (at most `max`): [{ side, grp, of, n, both }]; [] for a line that has no sides and no group.
   *  A sided line alternates Left, Right (a Left is always followed by its own Right); a glued line is `both`; a line the rules make into several
   *  pieces (n discs) is one group with no sides. A line the rules do not touch gets []. */
  function sides(t, rid, max) {
    const out = [];
    try {
      if (!t || typeof t !== "object") return out;
      const d = describe(t); if (!d.sided && !d.glued && d.n <= d.q) return out;
      const grp = groupOf(t, rid), cap = Math.max(0, Math.min(max == null ? MAX_PIECES : Number(max) || 0, 500));
      for (let k = 1; k <= d.n && out.length < cap; k++) {
        if (d.sided && d.n >= 2 && k % 2 === 1 && out.length + 2 > cap) break;           // (a left and a right go in together or not at all)
        const p = {};
        if (d.sided) { const w = d.list && d.list[k - 1] && d.list[k - 1].side; p.side = w === "L" || w === "R" ? w : d.list ? undefined : k % 2 === 1 ? "L" : "R"; if (p.side === undefined) delete p.side; } else if (d.glued) p.both = true;
        if (grp) { p.grp = grp; if (!d.glued) { p.of = d.n; p.n = k; } }
        out.push(p);
      }
    } catch (_) {}
    return out;
  }

  /** The ears an order's sticker prints (QR Printer.html, dataObj.pieces): [{ side, n, of }], a Left then a Right for every unit of every earring pair line
   *  (matching or mismatched; the words of the line decide, whatever the count rules say: a sticker is a thing in the hand), n = the pair's number in the order
   *  and of = how many pairs the order has. [] when the order has no earring pair: the one order sticker it always was. */
  function ears(list) {
    const out = [];
    try {
      const pairs = [];
      for (const t of (Array.isArray(list) ? list : [])) {
        if (!t || typeof t !== "object") continue;
        if (!(saysMismatched(t) || soldAsPair(t))) continue;
        for (let u = 0; u < Math.min(qtyOf(t), 20); u++) pairs.push(1);
      }
      for (let i = 0; i < pairs.length && out.length + 2 <= 40; i++) { out.push({ side: "L", n: i + 1, of: pairs.length }, { side: "R", n: i + 1, of: pairs.length }); }
    } catch (_) {}
    return out;
  }

  /** One piece per unit of every line (a line with quantity 2 is two pieces), at most 24 told; pieceCount is the true total.
   *  An earring pair is a Left then a Right piece per unit, with the line's group key, the group's size and the piece's number
   *  (a mismatched line the pool still makes as one piece per unit is one glued piece, `both`); every other line is exactly as before.
   *  A pair is never cut in half by the limit of 24. */
  function pieces(list, rid) {
    const out = []; let total = 0, i = 0;
    try {
      for (const t of (Array.isArray(list) ? list : [])) {
        i++;
        if (!t || typeof t !== "object") continue;
        const d = describe(t), n = d.n, sd = sides(t, rid, MAX_PIECES - out.length);
        total += n;
        if (!sd.length && (d.sided || d.glued || n > d.q)) continue;           // (no room left for the pieces of a group: all of them or none)
        const base = String(t.transaction_id || t.listing_id || i).replace(/[^\w.:-]/g, "_").slice(0, 30);
        const label = clean(t.title, 60), sku = clean(t.sku || t.__sku, 60), listingId = digits(t.listing_id, 20), size = sizeOf(t);
        for (let k = 1; k <= n && out.length < MAX_PIECES; k++) {
          if (sd.length && k > sd.length) break;                              // (the pair fields stopped: the limit would cut a pair in half)
          const p = { id: base + "-" + k, label };
          if (sku) p.sku = sku;
          if (listingId) p.listingId = listingId;
          if (size) p.size = size;
          if (sd.length) Object.assign(p, sd[k - 1]);
          out.push(p);
        }
      }
    } catch (_) {}
    return { pieces: out, pieceCount: total };
  }

  /** a click or a key on the page while an order is in hand: it is still in hand (the library ends it after a quiet stretch) */
  function watch() {
    if (watching) return; watching = true;
    try {
      const bump = () => {
        try {
          if (!cur) return;
          const now = Date.now(); if (now - lastTouch < TOUCH_EVERY) return; lastTouch = now;
          const a = lib(); if (a && typeof a.touch === "function") a.touch();
        } catch (_) {}
      };
      for (const ev of ["pointerdown", "keydown"]) document.addEventListener(ev, bump, { capture: true, passive: true });
    } catch (_) {}
  }

  /** tell the library. A scan (fresh) is a new scan: the library keeps the old time only for the same order told again inside 15 s.
   *  A new line for the order in hand (not fresh) keeps its scan time and its pieces.
   *  o.sync: the same order or batch told again is a refresh, not a scan (the time stays). o.keepTime: whatever replaces what is in
   *  hand (a selection that grows or shrinks) keeps the time of what was in hand. */
  function tell(o, fresh) {
    try {
      const a = lib(); if (!a || typeof a.working !== "function") return false;
      const kind = o.kind === "sheet" ? "sheet" : "order", rid = kind === "order" ? digits(o.rid, 30) : "", title = clean(o.title, 80);
      if (kind === "order" ? !rid : !title) return false;
      const key = String(o.key != null ? o.key : kind + "|" + rid + "|" + title);
      let who = ""; try { const w = typeof a.who === "function" ? a.who() : null; who = w && w.person ? String(w.person) : ""; } catch (_) {}
      const same = !!cur && cur.key === key;
      if (!fresh && !same) return false;                       // a new line only for the order in hand
      if (!fresh && cur.person !== who) fresh = true;          // the order in hand was somebody else's scan (they signed out): this person's first word about it is a scan of their own
      if (fresh && o.sync === true && same && cur.person === who) fresh = false;     // told again: a refresh
      const keep = !!cur && cur.person === who && (!fresh || o.keepTime === true);
      const next = Object.assign({}, same ? cur : {}, o, { kind, rid, title, key, person: who });
      const ok = !!a.working({ kind, rid, title, orderNumber: kind === "order" ? (clean(o.orderNumber, 40) || rid) : clean(o.orderNumber, 40),
        customer: o.customer != null ? clean(o.customer, 60) : (same ? cur.customer : undefined),
        pieces: next.pieces, pieceCount: next.pieceCount, note: o.note != null ? clean(o.note, 80) : next.note,
        scannedAt: keep ? cur.scannedAt : undefined, holdMs: Math.max(5000, Math.min(1800000, Number(next.holdMs) || HOLD_MS)) });
      if (ok) {
        let at = 0;                                            // the scan time the library holds (it decides whether this is the same scan)
        try { for (const r of (a.current() || [])) if (r && r.kind === kind && String(r.rid || "") === rid && r.scannedAt) at = r.scannedAt; } catch (_) {}
        next.scannedAt = at || (same && cur.scannedAt) || Date.now();
        cur = next; watch();
      }
      return ok;
    } catch (_) { return false; }
  }

  function start(rid, transactions, o) {
    try {
      o = o || {};
      const p = pieces(transactions, rid);
      return tell({ kind: "order", rid, title: o.title, orderNumber: o.orderNumber, customer: o.customer, note: o.note != null ? o.note : "",
        pieces: Array.isArray(transactions) ? p.pieces : (o.pieces || []), pieceCount: Array.isArray(transactions) ? p.pieceCount : (o.pieceCount || 0), holdMs: o.holdMs,
        sync: o.sync, keepTime: o.keepTime }, true);
    } catch (_) { return false; }
  }
  /** a batch of orders worked on together (the sorting sheet): one order is that order; several are one sheet with all their pieces */
  function batch(ids, transactions, o) {
    try {
      o = o || {};
      const list = [...new Set((ids || []).map(x => digits(x, 30)).filter(Boolean))];
      if (!list.length) return false;
      if (list.length === 1) {
        const mine = (Array.isArray(transactions) ? transactions : []).filter(t => { const r = digits(t && (t.typedOrderNumber || t.receipt_id || t.receiptId), 30); return !r || r === list[0]; });
        return start(list[0], mine, Object.assign({}, o, { title: "" }));
      }
      const p = pieces(transactions, list[0]);
      return tell({ kind: "sheet", key: o.key != null ? "sheet|" + o.key : "sheet|" + list.join(","), title: o.title || "Batch of " + list.length + " orders", orderNumber: list[0],
        note: o.note != null ? o.note : "", pieces: p.pieces, pieceCount: p.pieceCount, holdMs: o.holdMs, sync: o.sync, keepTime: o.keepTime }, true);
    } catch (_) { return false; }
  }
  /** something in hand that names no order (an inbox conversation without an order): a titled sheet with no QR; `key` tells one from another */
  function sheet(key, title, o) {
    try {
      o = o || {};
      const p = pieces(o.transactions);
      return tell({ kind: "sheet", key: "sheet|" + clean(key, 80), title, orderNumber: o.orderNumber, customer: o.customer, note: o.note != null ? o.note : "",
        pieces: p.pieces, pieceCount: p.pieceCount, holdMs: o.holdMs, sync: o.sync, keepTime: o.keepTime }, true);
    } catch (_) { return false; }
  }
  /** the same order in hand, a new short line (the scan time and the pieces stay) */
  function note(text) {
    try { if (!cur) return false; return tell({ kind: cur.kind, key: cur.key, rid: cur.rid, title: cur.title, orderNumber: cur.orderNumber, note: text == null ? "" : text }, false); } catch (_) { return false; }
  }
  /** done: completed, printed or closed. With an id, only when that order is the one in hand. */
  function end(rid) {
    try {
      const want = rid == null || rid === "" ? "" : digits(rid, 30);
      if (want && cur && cur.rid && cur.rid !== want) return false;
      const a = lib();
      const r = a && typeof a.idle === "function" ? (cur && cur.kind === "order" && cur.rid ? !!a.idle(cur.rid) : !!a.idle()) : false;
      cur = null;
      return r;
    } catch (_) { cur = null; return false; }
  }
  window.StationLiveOrder = { start, batch, sheet, note, end, pieces, units, kind, tag, sides, ears, current: () => (cur ? { kind: cur.kind, rid: cur.rid, title: cur.title, scannedAt: cur.scannedAt } : null) };
})();
