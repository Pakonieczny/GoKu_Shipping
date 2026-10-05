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
 *    StationLiveOrder.pieces(transactions) → { pieces, pieceCount }                 one piece per unit of every Etsy line
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
  /** One piece per unit of every line (a line with quantity 2 is two pieces), at most 24 told; pieceCount is the true total. */
  function pieces(list) {
    const out = []; let total = 0, i = 0;
    try {
      for (const t of (Array.isArray(list) ? list : [])) {
        i++;
        if (!t || typeof t !== "object") continue;
        const q = Math.max(1, Math.floor(Number(t.quantity != null ? t.quantity : t.__qty)) || 1);
        total += q;
        const base = String(t.transaction_id || t.listing_id || i).replace(/[^\w.:-]/g, "_").slice(0, 30);
        const label = clean(t.title, 60), sku = clean(t.sku || t.__sku, 60), listingId = digits(t.listing_id, 20), size = sizeOf(t);
        for (let k = 1; k <= q && out.length < MAX_PIECES; k++) {
          const p = { id: base + "-" + k, label };
          if (sku) p.sku = sku;
          if (listingId) p.listingId = listingId;
          if (size) p.size = size;
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
      const p = pieces(transactions);
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
      const p = pieces(transactions);
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
  window.StationLiveOrder = { start, batch, sheet, note, end, pieces, current: () => (cur ? { kind: cur.kind, rid: cur.rid, title: cur.title, scannedAt: cur.scannedAt } : null) };
})();
