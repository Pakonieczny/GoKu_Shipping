/*  charm-nest-etsy-shape.js — the Design Station's Etsy order shaping, outside the station.
 *  ═══════════════════════════════════════════════════════════════════════
 *  design-1.html turns each Etsy receipt (listOpenOrders / etsyOrderProxy) into the "order" and "line" objects the Charm
 *  Nest sorter reads over the bridge (orders.snapshot, orders.detail). This is that shaping, line for line, so a server
 *  reading Etsy with its own token answers exactly what the station answers. Pure: no DOM, no fetch, no storage, and no
 *  argument is ever changed (the station decorates its receipts in place; here a copy is decorated).
 *  Each part names the station code it follows; tests/charm-nest/etsy-shape.cjs runs those station functions, cut out of
 *  design-1.html, against this module on the same receipts. A change to the station's shaping belongs here too.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.CharmNestEtsyShape = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  /* ═══ 1 · metal classification (design-1.html §4: METALS … SOLID_UNKNOWN ~1232-1263, normMetalText … countsAreEmpty
         ~1696-1937). Precedence: 14k solid → 10k solid → rose → silver → gold. A bare karat is never solid gold; "solid"
         and the karat must share one source phrase. Exception (Paul, 13 Nov 2026): the exact selections "14K Gold + Engrave"
         and "10K Gold + Engrave" are 14K / 10K solid. ═══ */
  const METALS = [
    { key:"14k",    label:"14K",    short:"14", match:[/\b14\s*k(?:t|arat)?\s*(?:solid(?:\s+gold)?|gold\s+solid)\b/, /\bsolid(?:\s+gold)?\s*14\s*k(?:t|arat)?\b/, /^14\s*k\s+gold\s*\+\s*engrave$/] },
    { key:"10k",    label:"10K",    short:"10", match:[/\b10\s*k(?:t|arat)?\s*(?:solid(?:\s+gold)?|gold\s+solid)\b/, /\bsolid(?:\s+gold)?\s*10\s*k(?:t|arat)?\b/, /^10\s*k\s+gold\s*\+\s*engrave$/] },
    { key:"rose",   label:"Rose",   short:"R",  match:[/rose/] },
    { key:"silver", label:"Silver", short:"S",  match:[/silv(?:er|r)/, /sterling/] },
    { key:"gold",   label:"Gold",   short:"G",  match:[/gold/, /yellow/] },
  ];
  const METAL_ORDER = ["gold","silver","rose","10k","14k"];   // display order (light → heavy), not precedence
  const METAL_BY_KEY = Object.fromEntries(METALS.map(m => [m.key, m]));
  const METAL_KEYS   = METALS.map(m => m.key);

  const SOLID_RE       = /\bsolid\b/;
  const KARAT_RE       = /\b(?:10|14)\s*k(?:t|arat)?\b/;
  const GOLD_FILLED_RE = /\bgold\s*fill(?:ed)?\b|\bgf\b|\b14\s*20\b/;
  const SOLID_UNKNOWN  = "__solid_unknown__";

  /** The station's list page size (listOpenOrders asks Etsy for 100; a shorter page is the last). */
  const PAGE_SIZE = 100;

  function normMetalText(s) {
    return String(s ?? "")
      .toLowerCase()
      .replace(/&quot;/g, '"')
      .replace(/[\/_-]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  /** Text → metal key, or "". solidStrict: "solid" with no karat is SOLID_UNKNOWN, never guessed onto a bench. */
  function metalKeyFromText(raw, { solidStrict = false } = {}) {
    const t = normMetalText(raw);
    if (!t) return "";

    // An explicit Gold Filled wins over "solid" elsewhere in the text; Rose Gold Filled stays rose.
    if (GOLD_FILLED_RE.test(t)) return /\brose\b/.test(t) ? "rose" : "gold";

    if (solidStrict && SOLID_RE.test(t) && !KARAT_RE.test(t)) return SOLID_UNKNOWN;

    for (const m of METALS) {                 // METALS order == precedence order
      for (const re of m.match) if (re.test(t)) return m.key;
    }
    return "";
  }

  /* LISTMETAL (Paul, 23 Nov 2026, order 4235923653: "When buyer chooses this Gold Filled option ... our system interprets it as Rose Gold ... for that specific choice from the drop-down menu to be recognized as
     Gold Filled"; and "Fix and move both" for 4230787700 and 4238855149): listings 581466464 and 522898333 name their metal in a drop-down called "Personalization" ("GOLD·Engraved", "GOLD·Front", "SILVER·Front"),
     which the reading below leaves out as the buyer's own words, and their other choice is a length: the metal was the title's last resort ("Sterling Silver, Gold or Rose Gold" → Rose). A seller's drop-down
     (never the buyer's typed personalization, property 54) whose choice is a metal and an engraving word is read before the title. No other choice, and no option that names a metal itself, is changed. */
  const CHOICE_METAL = /^(rose\s*gold|gold|(?:sterling\s*)?silver)(?:\s*filled)?\s*[·•∙⋅|\/:,\-–—]\s*(?:engraved|engraving|blank|front|back|both)\b/i;
  function choiceMetal(vars) {
    for (const v of vars) {
      if (!v || !/^personali[sz]ation$/i.test(String(v.formatted_name || "").trim()) || String(v.property_id == null ? "" : v.property_id) === "54") continue;
      const m = CHOICE_METAL.exec(String(v.formatted_value || "").trim()); if (m) return metalKeyFromText(m[1]);
    }
    return "";
  }

  /**
   * Metal for one transaction: 1 · the Metal / Color / Colour variation; 2 · any other variation value, each on its own
   * (14k, then 10k, then the first); 3 · wide only: title and personalisation, each on its own, never concatenated.
   */
  function metalKeyForTx(tx, { wide = false } = {}) {
    if (!tx) return "";
    const vars = Array.isArray(tx.variations) ? tx.variations : [];
    const S = { solidStrict: true };
    let sawSolidUnknown = false;
    const note = k => { if (k === SOLID_UNKNOWN) sawSolidUnknown = true; return k; };

    for (const v of vars) {
      if (!v || !/metal|colou?r/i.test(String(v.formatted_name || ""))) continue;
      const k = note(metalKeyFromText(v.formatted_value, S));
      if (k && k !== SOLID_UNKNOWN) return k;
    }

    const seen = [];
    for (const v of vars) {
      if (!v || /personalization/i.test(String(v.formatted_name || ""))) continue;
      const k = note(metalKeyFromText(v.formatted_value, S));
      if (k && k !== SOLID_UNKNOWN) seen.push(k);
    }
    if (seen.includes("14k")) return "14k";
    if (seen.includes("10k")) return "10k";
    if (seen.length) return seen[0];

    { const k = choiceMetal(vars); if (k) return k; }   // (LISTMETAL: before the title)

    if (wide) {
      const sources = [
        tx.title || "",
        ...(Array.isArray(tx.personalization) ? tx.personalization : [tx.personalization || ""]),
      ];
      const wideSeen = [];
      for (const source of sources) {
        const k = note(metalKeyFromText(source, S));
        if (k && k !== SOLID_UNKNOWN) wideSeen.push(k);
      }
      if (wideSeen.includes("14k")) return "14k";
      if (wideSeen.includes("10k")) return "10k";
      if (wideSeen.length) return wideSeen[0];
    }

    if (sawSolidUnknown) return SOLID_UNKNOWN;
    return "";   // a stored _metalKey is never evidence on its own
  }

  /** True only for a key that names a real bench. SOLID_UNKNOWN and "" are not. */
  const isRealMetal = k => !!k && k !== SOLID_UNKNOWN && METAL_BY_KEY[k] != null;

  /* The station caches the key on the transaction (tx._metalKey); metalLabelFor reads it back. These three write it, as the
     station's do, and run only on this module's own copies. */
  function resolveTxMetal(tx, { wide = false } = {}) {
    if (!tx) return "";
    const k = metalKeyForTx(tx, { wide });
    tx._metalKey = k;
    return k;
  }

  /** Narrow, then wide; SOLID_UNKNOWN only when neither pass finds a real bench. */
  function txMetalResolved(tx) {
    const narrow = resolveTxMetal(tx);
    if (isRealMetal(narrow)) return narrow;
    const wide = resolveTxMetal(tx, { wide:true });
    if (isRealMetal(wide)) return wide;
    return (narrow === SOLID_UNKNOWN || wide === SOLID_UNKNOWN) ? SOLID_UNKNOWN : "";
  }

  /** Per-metal quantity totals → {14k, 10k, rose, silver, gold}. */
  function metalCountsFor(txs) {
    const out = {};
    for (const k of METAL_KEYS) out[k] = 0;
    (txs || []).forEach(tx => {
      const k = txMetalResolved(tx);
      if (isRealMetal(k)) out[k] += (Number(tx.quantity) || 1);
    });
    return out;
  }

  /** The single metal tag for a whole order, using METALS precedence. */
  function rowMetalFor(counts) {
    for (const m of METALS) if (counts && counts[m.key]) return m.key;
    return "";
  }

  function countsAreEmpty(counts) {
    return !counts || METAL_KEYS.every(k => !counts[k]);
  }

  /** design-1.html metalLabelFor (~4229): the Metal/Color variation's own words, else a metal-looking value, else the key's label. */
  function metalLabelFor(tx) {
    if (Array.isArray(tx.variations)) {
      for (const v of tx.variations) {
        const name = String(v?.formatted_name || "");
        const val  = String(v?.formatted_value || "").replace(/&quot;/g, '"').trim();
        if (!val) continue;
        if (/personalization/i.test(name)) continue;
        if (/metal|colou?r/i.test(name)) return val;
      }
      for (const v of tx.variations) {
        const val = String(v?.formatted_value || "").replace(/&quot;/g, '"').trim();
        if (val && metalKeyFromText(val)) return val;
      }
    }
    const k = tx._metalKey || "";
    if (k === SOLID_UNKNOWN) return "Solid — karat unknown";
    return k ? (METAL_BY_KEY[k]?.label || k) : "";
  }

  /** design-1.html txNeedsAttention (~4261): personalisation, the personalised flag, or a buyer message. */
  function txNeedsAttention(tx) {
    const p = tx.personalization;
    const hasPers = (Array.isArray(p) && p.length) ||
                    (typeof p === "string" && p.trim().length) ||
                    tx.is_personalized === true;
    return !!(hasPers || (tx.message_from_buyer && tx.message_from_buyer.trim().length));
  }

  /* ═══ 2 · reading a receipt (design-1.html §11) ═══ */

  /** design-1.html receiptStamp (~3388): Etsy's update stamp under any of its names. */
  const receiptStamp = r => Number(r?.update_timestamp || r?.updated_timestamp || r?.last_modified_tsz || 0);

  // The row's ship-by text. Local time of whoever runs it: the station is a browser, a server is usually UTC.
  const shipStr = ts => new Date(ts * 1000).toLocaleDateString("en-US", { month:"short", day:"2-digit" });

  // Every receipt here crossed HTTP as JSON (listOpenOrders, etsyOrderProxy), so a JSON copy is the station's own copy.
  const clone = v => (v == null ? v : JSON.parse(JSON.stringify(v)));

  /** fetchOpenOrdersSequential (~3423): the pages of a list sweep, in order, as one list. Etsy lists newest first by offset,
      so an order arriving mid-sweep repeats the last of one page as the first of the next: a receipt is listed once, at its
      first place, as its later (fresher) copy. A page is Etsy's `results` array or the answer holding it. */
  function collectReceiptPages(pages) {
    const all = [], seenAt = new Map();
    for (const page of pages || []) {
      const receipts = Array.isArray(page) ? page : (page && page.results) || [];
      for (const r of receipts) { const id = r && r.receipt_id != null ? String(r.receipt_id) : "", at = id ? seenAt.get(id) : undefined; if (at != null) all[at] = r; else { if (id) seenAt.set(id, all.length); all.push(r); } }
    }
    return all;
  }

  /** etsyOrderProxy.js (~45-52): Etsy's receipt (or the proxy's own answer) as the { receipt, transactions } envelope the
      station reads, as a fresh JSON copy. An answer with no receipt in it is returned as it is. */
  function detailEnvelope(payload) {
    const receipt = payload?.receipt || (payload?.receipt_id ? payload : null);
    const normalized = receipt ? { ...payload, receipt, transactions: payload.transactions || receipt.transactions || [] } : payload;
    return clone(normalized);
  }

  /* pullEtsyOrderDetails (~2236): every item of a detail read takes the receipt's buyer message, and its personalisation is
     lifted out of the "Personalization" variation (&quot; decoded, "Not requested…" dropped, the receipt's own
     personalization as the fallback). Writes into `data`, a copy. */
  function liftDetailTransactions(data) {
    const buyerMsg = data?.receipt?.message_from_buyer || "";
    const receiptPers = data?.receipt?.personalization || "";
    (data.transactions || []).forEach(t => {
      t.message_from_buyer = buyerMsg;
      let engrave = (t.variations || [])
        .find(v => /personalization/i.test(v?.formatted_name || ""))?.formatted_value || "";
      engrave = String(engrave).replace(/&quot;/g, '"').trim();
      if (engrave && /^not requested/i.test(engrave)) engrave = "";
      if (!engrave && receiptPers) engrave = String(receiptPers).trim();
      t.personalization = engrave ? [engrave] : [];
      resolveTxMetal(t);
    });
    return data;
  }

  /* decorateReceipt (~3447), less its fallback to the station's in-memory item cache. Writes into `r`, a copy. */
  function decorateReceipt(r) {
    const ts = (Array.isArray(r.transactions) && r.transactions[0]?.expected_ship_date)
      || r.expected_ship_date || r.dispatch_date || r.ship_by_date || 0;
    r._shipTS  = ts;
    r._shipStr = ts ? shipStr(ts) : "N/A";

    const buyerMsg = r.message_from_buyer || "";
    (r.transactions || []).forEach(t => {
      if (!t.message_from_buyer) t.message_from_buyer = buyerMsg;
      if (!Array.isArray(t.personalization)) {
        const pv = (t.variations || []).find(v => /personalization/i.test(v?.formatted_name || ""));
        let val = String(pv?.formatted_value || "").replace(/&quot;/g, '"').trim();
        if (/^not requested/i.test(val)) val = "";
        t.personalization = val ? [val] : [];
      }
      resolveTxMetal(t);
    });

    r._stamp       = receiptStamp(r);
    r._metalCounts = metalCountsFor(r.transactions || []);
    r._rowMetal    = rowMetalFor(r._metalCounts);
    r._hydrated    = Array.isArray(r.transactions) && r.transactions.length > 0;
    r._attn        = (r.transactions || []).some(txNeedsAttention);
    return r;
  }

  /* applyHydration (~3510): a listed receipt's items arriving from a detail read. Writes into `rec`, a copy. */
  function applyHydration(rec, txs) {
    rec.transactions  = txs;
    rec._metalCounts  = metalCountsFor(txs);
    rec._rowMetal     = rowMetalFor(rec._metalCounts);
    rec._hydrated     = txs.length > 0;
    rec._attn         = txs.some(txNeedsAttention);
    if (!rec._shipTS && txs[0]?.expected_ship_date) {
      rec._shipTS  = txs[0].expected_ship_date;
      rec._shipStr = shipStr(rec._shipTS);
    }
    return rec;
  }

  /**
   * A raw Etsy receipt → the receipt as the station holds it when orderForBridge / lineForBridge read it: _shipTS, _shipStr,
   * _stamp, _metalCounts, _rowMetal, _hydrated, _attn on the receipt; message_from_buyer, personalization and _metalKey on
   * each item. Returns a new object; `raw` is not changed. opts.via picks the station's path:
   *  · "list"    (default for a bare receipt) — a receipt from getShopReceipts / listOpenOrders: decorateReceipt, as
   *              buildNewOrderList and orders.check do.
   *  · "detail"  (default for etsyOrderProxy's { receipt, transactions } answer) — getShopReceipt with
   *              includes=Transactions,Transactions.personalization,Transactions.variations (_etsyMailEtsy getShopReceiptFull),
   *              bare or through etsyOrderProxy: pullEtsyOrderDetails, then decorateReceipt, as orders.detail (fresh), the
   *              order search and undo do. opts.receiptId fills a missing receipt_id; opts.listed, the receipt as the list
   *              sweep held it, is folded in the way orders.detail folds a fresh read into a listed order.
   *  · "hydrate" (default when opts.detail is given) — raw is the listed receipt, opts.detail its detail read (null: the
   *              read failed): decorateReceipt, then hydrateOrder + applyHydration, as orders.snapshot / orders.detail do
   *              for a listed order that came without its items.
   */
  function prepareReceipt(raw, opts = {}) {
    if (!raw || typeof raw !== "object") throw new TypeError("prepareReceipt: an Etsy receipt is required");
    const via = opts.via || (opts.detail !== undefined ? "hydrate" : (raw.receipt && typeof raw.receipt === "object" ? "detail" : "list"));
    if (via === "list") return decorateReceipt(clone(raw));
    if (via === "hydrate") {
      const r = decorateReceipt(clone(raw));
      const data = opts.detail == null ? null : detailEnvelope(opts.detail);
      if (data) liftDetailTransactions(data);
      return applyHydration(r, data?.transactions || []);
    }
    if (via !== "detail") throw new TypeError(`prepareReceipt: unknown via "${via}"`);
    const data = detailEnvelope(raw);
    if (!data?.receipt || !Array.isArray(data.transactions)) throw new Error("Etsy returned an incomplete receipt");
    liftDetailTransactions(data);
    const rc = data.receipt;
    rc.receipt_id = rc.receipt_id || String(opts.receiptId || opts.listed?.receipt_id || "");
    rc.transactions = data.transactions || [];
    decorateReceipt(rc);
    if (opts.listed == null) return rc;
    const r = decorateReceipt(clone(opts.listed));
    Object.assign(r, rc);
    return decorateReceipt(r);
  }

  /** orders.detail's verdict on a fresh read (~8275): shipped, cancelled, unpaid or fully refunded is gone ("Partially
      Refunded" is not). Takes the detail receipt, bare or in etsyOrderProxy's envelope. */
  function detailStatus(raw) {
    const rc = raw && raw.receipt && typeof raw.receipt === "object" ? raw.receipt : (raw || {});
    const gone = !!rc.is_shipped || !!rc.was_shipped || !!rc.is_canceled || !!rc.was_canceled || rc.is_paid === false || rc.was_paid === false || !!(rc.status && /cancel|fully\s*refund/i.test(String(rc.status)));
    return { gone, status: String(rc.status || ""), isShipped: !!rc.is_shipped };
  }

  /* ═══ 3 · the bridge's shapes (design-1.html Bridge: metalOf ~7936, lineForBridge ~7937, orderForBridge ~7989) ═══ */

  /** A real bench key, "solid_unknown", or "". Reads a copy: the station's writes tx._metalKey. */
  const metalOf = tx => { const k = txMetalResolved(Object.assign({}, tx)); return isRealMetal(k) ? k : (k === SOLID_UNKNOWN ? "solid_unknown" : ""); };

  /** lineForBridge: one item as the sorter reads it. productId and the variations' property/value ids let the sorter find the
      SKU Etsy keeps for the purchased option (charm-nest-orders.js inventorySku). */
  function shapeLine(tx, r) {
    tx = Object.assign({}, tx);   // the metal key the station caches on the item (metalOf) is what metalLabelFor reads back
    r = r || {};
    const key = txMetalResolved(tx);
    return { transactionId: String(tx.transaction_id), listingId: String(tx.listing_id || ""), productId: String(tx.product_id || ""), sku: String(tx.sku || "").trim(), title: String(tx.title || ""), quantity: Number(tx.quantity) || 1,
      metalKey: isRealMetal(key) ? key : (key === SOLID_UNKNOWN ? "solid_unknown" : ""), metalLabel: metalLabelFor(tx) || "", personalization: Array.isArray(tx.personalization) ? tx.personalization.map(String) : (tx.personalization ? [String(tx.personalization)] : []),
      buyerMessage: String(tx.message_from_buyer || r.message_from_buyer || ""), expectedShipDate: Number(tx.expected_ship_date) || 0, isPersonalized: !!tx.is_personalized,
      variations: (tx.variations || []).map(v => ({ name: String(v?.formatted_name || ""), value: String(v?.formatted_value || "").replace(/&quot;/g, '"'), propertyId: v?.property_id != null ? String(v.property_id) : null, valueId: v?.value_id != null ? String(v.value_id) : null })) };
  }

  /**
   * orderForBridge: a prepared receipt (prepareReceipt) as the sorter reads it. extras is what the station reads from
   * Firestore: { staffNote, hasNote, messages, hasMessages, completed, withNotes }.
   *  · hasNote — the order is in the staff-note index (default: a non-blank staffNote); staffNote is "" without it.
   *  · messages — the internal thread's rows as the station reads them (messageRowsFromDocs); hasMessages — the order's
   *    message flag, also true whenever rows are given.
   *  · withNotes: false — the snapshot's withNotes:false: no note text and no rows (the flags still answer).
   * lockedBy / selectedHere / claimed belong to one station's screen: null / false / null.
   */
  function shapeOrder(r, extras = {}) {
    extras = extras || {};
    const rid = String(r.receipt_id);
    const txs = Array.isArray(r.transactions) && r.transactions.length ? r.transactions : [];
    const withNotes = extras.withNotes !== false;
    const hasNote = extras.hasNote !== undefined ? !!extras.hasNote : !!String(extras.staffNote || "").trim();
    const staffNote = withNotes && hasNote ? String(extras.staffNote || "") : "";
    const messages = withNotes && Array.isArray(extras.messages) ? extras.messages : [];
    return { receiptId: rid, orderNumber: String(r.order_number || rid), shipBy: Number(r._shipTS) || 0, updateTs: Number(r._stamp || receiptStamp(r)) || 0, createTs: Number(r.create_timestamp || r.created_timestamp || r.creation_tsz || 0) || 0,
      buyer: { name: String(r.name || ""), country: String(r.country_iso || r.country || ""), city: String(r.city || "") }, buyerMessage: String(r.message_from_buyer || ""), isGift: !!r.is_gift, giftMessage: String(r.gift_message || ""),
      attention: !!r._attn, hydrated: !!r._hydrated, staffNote, hasNote, hasMessages: !!extras.hasMessages || messages.length > 0, messages,
      lockedBy: null, selectedHere: false, claimed: null, completed: !!extras.completed,
      metals: r._metalCounts || {}, lines: txs.map(tx => shapeLine(tx, r)) };
  }

  /** prepareReceipt then shapeOrder. */
  const shapeReceipt = (raw, extras, opts) => shapeOrder(prepareReceipt(raw, opts), extras);

  /** orders.snapshot's order (~8240): soonest ship-by first (none = 0), then receipt id. */
  const compareOrders = (x, y) => (x.shipBy || 0) - (y.shipBy || 0) || x.receiptId.localeCompare(y.receiptId);

  /** The bridge's messagesOf (~7977): Brites_Orders/{rid}/messages ordered by timestamp, newest first, the newest `limit`
      (5) → rows oldest first. Takes Firestore snapshots (with .data()) or their plain data. */
  function messageRowsFromDocs(docs, limit = 5) {
    return (docs || []).slice(0, limit).map(d => { const m = (d && typeof d.data === "function" ? d.data() : d) || {}; const t = m.timestamp; return { senderName: String(m.senderName || "Staff"), text: String(m.text || ""), at: t && t.toMillis ? t.toMillis() : (t && t.seconds ? t.seconds * 1000 : null), imageUrl: m.imageUrl || null }; }).reverse();
  }

  return {
    // constants
    METALS, METAL_ORDER, METAL_KEYS, METAL_BY_KEY, SOLID_UNKNOWN, PAGE_SIZE,
    // metal helpers (none changes its argument)
    normMetalText, metalKeyFromText, metalKeyForTx, isRealMetal,
    txMetalResolved: tx => txMetalResolved(Object.assign({}, tx)),
    metalCountsFor: txs => metalCountsFor((txs || []).map(tx => Object.assign({}, tx))),
    rowMetalFor, countsAreEmpty, metalLabelFor, metalOf, txNeedsAttention,
    // receipts
    receiptStamp, collectReceiptPages, detailEnvelope, prepareReceipt, detailStatus,
    // the sorter's shapes
    shapeLine, shapeOrder, shapeReceipt, compareOrders, messageRowsFromDocs,
  };
});
