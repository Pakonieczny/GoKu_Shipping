/*  charm-nest-sku.js — the SKU a custom design gets when it is saved to the charm repository (SKUGEN, 11 Oct 2026).
 *  ═══════════════════════════════════════════════════════════════════════
 *  Paul, 11 Oct 2026: "save that to Etsy for that particular listing AND into our charm repository so that next time that particular
 *  listing is purchased, it's already linked with the same SKU". This file is the ONE rule for that SKU, shared by the sorter page and
 *  by the cloud (netlify/functions/_charmNestSkuCheck.js requires this very file by a literal path, as _stationLive.js does with
 *  charm-nest-orders.js), so the page's preview and the server's check can never drift apart. Pure and deterministic: no DOM, no
 *  network, no clock, no writes, no Etsy. Plan: plans/save-to-repo-1011/CONTRACT.md section 3.
 *
 *  THE SHAPE  NAME_TAIL, like the shop's own ("Heart_9491": SKU_List_V1.html makeSkuFrom, the first word of the title and the last 4
 *  digits of the listing id; 866 of the 6,856 master SKUs end in "_" and 4 digits).
 *    · NAME comes from the design FILE's name without its extension: entities decoded, accents folded, UPPERCASE, "&" is AND,
 *      apostrophes are dropped (ACORN'S is ACORNS), every other run of characters outside A-Z 0-9 is ONE "_", ends trimmed. Trailing
 *      file-version words are dropped (V1 V2…, FINAL, COPY, NEW, EDIT, EDITED, OUTLINE, OUTLINED, CUT, LASER, FILE, ARTBOARD, AI,
 *      DXF and counters such as "(2)"), never the first word. A name that is empty, only digits or an Illustrator default (Untitled-1,
 *      Artboard 1) is replaced by the first phrase of the listing's title (up to the first comma, dash or pipe), cleaned the same way,
 *      and failing that by CHARM. At most 18 characters (cut at a "_" when one lies in the last 8 of them, else hard).
 *    · TAIL is the last 4 digits of the Etsy listing id. Candidates, in order: TAIL4, the last 5 digits, the last 6 digits, then the
 *      last 6 digits + "_A" … "_F". plan() drops what the page already knows is taken and hands at most 6 of the rest to the cloud.
 *    · Only A-Z 0-9 "_" "-" (a SKU starts with a letter or digit), at most 32 characters (Etsy's limit). Never a space or / + & ; ,
 *      (charm-nest-orders.js splitSkus would read two designs).
 *  RESERVED (never produced; tested with the REAL code of charm-nest-orders.js, not a copy of its lists): anything specialOf reads as a
 *  special order for a line with that SKU and nothing else (CUSTOM_…, RE_…, CHAIN_…, ADD_…, CARD_…, ORDER_…: every SKU_RULES family),
 *  a SKU variationBase reads as charm-only (…-CO / …_CO), one with "(HUGGIE)", one that ends "-" or "_" and 1 to 3 digits (the numbered
 *  families, INDEXED_SKU / skuIndexFamily), and a SKU that is exactly a known Etsy typo. A name that makes a reserved SKU gets
 *  "CHARM_" in front (CUSTOM_LEAF becomes CHARM_CUSTOM_LEAF); if that is still reserved, the next candidate.
 *  UNIQUE means free in ALL of: the master index (exact SKU, and a twin that reads the same without spacing and punctuation:
 *  looseKey, "FOXHEAD4821" and "FOX_HEAD_4821"), the saved aliases (any alias value, any bySku key, any listing), and the listing's
 *  own inventory SKUs (the cached table: every product, those outside the group too).
 *  GROUP: the products of the listing the SKU would go on: those that carry the SKU the bought product carries. The mode never
 *  changes the SKU text. ALIASWORKS: false when the SKU Etsy carries keeps the line a custom order whatever an alias says.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory(function () { return require("./charm-nest-orders.js"); });
  else root.CharmNestSku = factory(function () { return root.CharmNestOrders; });
})(typeof self !== "undefined" ? self : this, function (ordersOf) {
  "use strict";

  let loaded = null;
  /** The orders module (charm-nest-orders.js), found when first needed so a page may load the two scripts in either order. */
  const O = () => loaded || (loaded = ordersOf());
  const MAX_LEN = 32, NAME_MAX = 18, SEND_MAX = 6;
  const FORMAT = /^[A-Z0-9][A-Z0-9_-]*$/;

  const decode = s => { const t = s == null ? "" : String(s); const o = O(); return o && o.decodeEntities ? o.decodeEntities(t) : t; };
  const up = s => decode(s).trim().toUpperCase();   // (the way charm-nest-orders.js upSku reads a SKU: decoded before the capitals)
  const digits = s => String(s == null ? "" : s).replace(/\D/g, "");

  /* ── NAME ─────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const APOSTROPHES = /['‘’‚‛′ʼ´`]/g;
  const LETTERS = { "ß": "SS", "Æ": "AE", "æ": "AE", "Œ": "OE", "œ": "OE", "Ø": "O", "ø": "O", "Đ": "D", "đ": "D", "Ð": "D", "ð": "D", "Ł": "L", "ł": "L", "ı": "I", "Þ": "TH", "þ": "TH" };
  const FILE_EXT = /\.(?:ai|dxf|pdf|eps|svg|cdr|dwg|png|jpe?g)$/i;
  const SEP = "[\\s_\\-.]";
  /* the words a design file's name ends with that say "which copy of the file" and not "which design"; each needs a separator before it, so the FIRST word is never dropped */
  const JUNK = ["V\\d{1,3}(?:[._]\\d{1,3})*", "FINAL", "COPY(?:" + SEP + "*\\d{1,3})?", "NEW", "EDIT", "EDITED", "OUTLINE", "OUTLINED", "CUT", "LASER", "FILE", "ARTBOARD(?:" + SEP + "*\\d{1,3})?", "AI", "DXF"];
  const TAIL_JUNK = new RegExp(SEP + "+(?:" + JUNK.join("|") + ")\\s*$");
  const TAIL_COUNTER = new RegExp(SEP + "*\\(\\s*\\d{1,3}\\s*\\)\\s*$");
  const DEFAULT_NAME = /^(?:UNTITLED|ARTBOARD|DOCUMENT|DRAWING|LAYER)(?:_\d{1,3})*$/;   // (what Illustrator calls a file nobody named)

  const fold = s => String(s).replace(APOSTROPHES, "").normalize("NFKD").replace(/[̀-ͯ]/g, "").replace(/[ßÆæŒœØøĐđÐðŁłıÞþ]/g, m => LETTERS[m]).toUpperCase();
  /** A text as NAME words: A-Z 0-9 joined by single "_", the file-version words and counters at its end dropped. */
  function squash(text) {
    let s = fold(text).replace(/&/g, " AND ").replace(/\s+/g, " ").trim();
    for (let i = 0; i < 12; i++) { const was = s; s = s.replace(TAIL_COUNTER, "").replace(TAIL_JUNK, "").trim(); if (s === was) break; }
    return s.replace(/[^A-Z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  }
  const usable = n => !!n && !/^\d+$/.test(n) && !DEFAULT_NAME.test(n);
  /** The file's name without its folder and extension (an Illustrator or DXF file; a .ai.ai is read once more). */
  function baseName(fileName) {
    let s = decode(fileName).split(/[\\/]/).pop();
    for (let i = 0; i < 2; i++) s = s.replace(FILE_EXT, "");
    return s.trim();
  }
  /** The first phrase of a listing's title: up to the first comma, dash, pipe… (the rule of charm-nest-orders.js titleLead), "Add a" and the like left off. */
  const titleLead = title => decode(title).toLowerCase().split(/\s*[,|•·:;–—(\[]\s*|\s+-\s+|\s+\/\s+/)[0].replace(/^\s*(?:add(?:\s+an?)?|optional|\+)\s+/, "").trim();
  /** NAME is at most 18 characters: the whole words that fit when a "_" lies in the last 8 of them, else cut hard. */
  function cut(name) {
    if (name.length <= NAME_MAX) return name;
    if (name.charAt(NAME_MAX) === "_") return name.slice(0, NAME_MAX);
    const at = name.lastIndexOf("_", NAME_MAX - 1);
    return (at >= NAME_MAX - 8 ? name.slice(0, at) : name.slice(0, NAME_MAX)).replace(/_+$/, "");
  }
  const fileNameOf = f => (f && typeof f === "object" ? f.name : f);
  /** NAME for a design file (a name or an object with .name) bought on a listing with this title: "Test_Charm.ai" gives TEST_CHARM. Never empty. */
  function nameOf(fileName, title) {
    const a = squash(baseName(fileNameOf(fileName)));
    if (usable(a)) return cut(a);
    const b = squash(titleLead(title));
    if (usable(b)) return cut(b);
    return "CHARM";
  }

  /* ── reserved ─────────────────────────────────────────────────────────────────────────────────────────────────────── */
  /** Why a SKU must never be given to a saved design ("" when it may): its characters or length, or one of the shapes the sorter already reads as something else. */
  function reservedWhy(sku) {
    const s = up(sku), o = O();
    if (!s) return "empty";
    if (/\(HUGGIE\)/.test(s)) return "the huggie mark (HUGGIE)";
    if (!FORMAT.test(s)) return "only A-Z, 0-9, _ and - are allowed";
    if (s.length > MAX_LEN) return "longer than " + MAX_LEN + " characters";
    if (/[-_]\d{1,3}$/.test(s) || o.skuIndexFamily(s, function () { return true; })) return "it ends like a numbered family (NAME-12)";
    if (o.variationBase(s)) return "it ends in the charm-only mark -CO";
    const sp = o.specialOf({ sku: s, title: "", variations: [] }, {});
    if (sp) return "the sorter reads this SKU as a " + String(sp.label || sp.kind || "special order").toLowerCase();
    if (o.knownTypo && o.knownTypo(s)) return "a SKU Etsy carries misspelled on purpose";
    return "";
  }
  const reserved = sku => !!reservedWhy(sku);

  /* ── candidates ───────────────────────────────────────────────────────────────────────────────────────────────────── */
  /** NAME + "_" + TAIL, the name shortened when the whole would pass 32 characters. */
  function join(name, tail) {
    const room = MAX_LEN - tail.length - 1;
    const n = name.length > room ? name.slice(0, room).replace(/_+$/, "") : name;
    return n ? n + "_" + tail : "";
  }
  /** Every SKU worth trying for this listing and NAME, in the order they are tried: TAIL4, 5 digits, 6 digits, then 6 digits + _A … _F. None is reserved; a reserved NAME gets CHARM_ in front. */
  function candidates(listingId, name) {
    const d = digits(listingId); if (!d) return [];
    const nm = nameOf(name, ""), t4 = d.slice(-4), t5 = d.slice(-5), t6 = d.slice(-6);
    const tails = [t4, t5, t6].concat("ABCDEF".split("").map(L => t6 + "_" + L)), seen = new Set(), out = [];
    // a NAME that makes a reserved SKU is moved aside once, for every candidate (CHARM_CHAIN_4821, CHARM_CHAIN_64821…), not candidate by candidate
    const front = reservedWhy(join(nm, t4)) ? "CHARM_" : "";
    for (const tail of tails) {
      let sku = join(front + nm, tail);
      if (!sku || reservedWhy(sku)) { sku = front ? "" : join("CHARM_" + nm, tail); if (!sku || reservedWhy(sku)) continue; }
      if (!seen.has(sku)) { seen.add(sku); out.push(sku); }
    }
    return out;
  }

  /* ── what is already taken ────────────────────────────────────────────────────────────────────────────────────────── */
  /** The saved aliases as a Map: looseKey → what holds it. Every alias value (sku, huggie, bySku values, a saved pair's L and R), every bySku and pairBySku key, of every listing. */
  function aliasIndex(aliases) {
    const m = new Map(), loose = O().looseKey;
    const put = (s, detail) => { const k = s == null || s === "" ? "" : loose(s); if (k && !m.has(k)) m.set(k, detail); };
    if (!aliases || typeof aliases !== "object") return m;
    for (const id of Object.keys(aliases)) {
      const a = aliases[id]; if (!a || typeof a !== "object") continue;
      const of = "listing " + String(id).replace(/~.*$/, "");
      put(a.sku, "an answer saved for " + of); put(a.huggie, "an answer saved for " + of);
      if (a.keep && typeof a.keep === "object") put(a.keep.sku, "a design saved for " + of);
      if (a.bySku && typeof a.bySku === "object") for (const k of Object.keys(a.bySku)) { put(k, "the SKU Etsy sent for " + of); put(a.bySku[k], "an answer saved for " + of); }
      if (a.pairBySku && typeof a.pairBySku === "object") for (const k of Object.keys(a.pairBySku)) { const p = a.pairBySku[k]; put(k, "the SKU Etsy sent for " + of); if (p && typeof p === "object") { put(p.L, "a pair saved for " + of); put(p.R, "a pair saved for " + of); } }
    }
    return m;
  }
  /** The SKUs a listing's cached inventory table holds, every product (deleted ones too), as a Map: looseKey → what holds it. */
  function tableSkus(table) {
    const m = new Map(), loose = O().looseKey;
    const put = (s, detail) => { const k = s == null || s === "" ? "" : loose(s); if (k && !m.has(k)) m.set(k, detail); };
    if (!table || typeof table !== "object") return m;
    if (table.uni !== undefined && table.uni !== null) put(table.uni, "the SKU of every product of this listing");
    if (Array.isArray(table.products)) for (const p of table.products) if (p && typeof p === "object") put(p.sku, "the SKU of a product of this listing");
    return m;
  }

  /* ── group ────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const idOf = x => (x == null || x === "" ? "" : String(x));
  const pairKey = p => idOf(p[0]) + ":" + idOf(p[1]);
  // the (property, value) ids Etsy puts on each variation of a transaction; a question with no value id (Personalization) has none
  const varIds = line => (Array.isArray(line && line.variations) ? line.variations : []).map(v => [idOf(v && (v.propertyId != null ? v.propertyId : v.property_id)), idOf(v && (v.valueId != null ? v.valueId : v.value_id))]).filter(p => p[0] && p[1] && p[1] !== "0");
  /**
   * The products of the listing the new SKU would go on. table: the listing's cached inventory table (charm-nest-orders.js inventorySku:
   * { uni } or { products: [{ id, sku, d, pv }] }), line: the order line ({ productId, variations, listingId }).
   * The bought product is the one whose id is line.productId, else the one live product whose option pairs equal the line's; none or two is
   * "unknown" (the Etsy step decides from the live inventory). The group is every live product whose SKU equals the bought product's SKU
   * (an empty SKU equals an empty SKU). → { mode: "all" | "some" | "unknown", productIds, boughtId, sku (what the group carries now), live }.
   * "all": the group is every live product. A table that says one SKU for the whole listing ({ uni }) is "all" with no ids.
   */
  function group(table, line) {
    const none = { mode: "unknown", productIds: [], boughtId: "", sku: "", live: 0 };
    if (!table || typeof table !== "object" || table.gone) return none;
    if (!Array.isArray(table.products)) return table.uni === undefined || table.uni === null ? none : { mode: "all", productIds: [], boughtId: "", sku: up(table.uni), live: 0 };
    const all = table.products.filter(p => p && typeof p === "object"), live = all.filter(p => !p.d), pid = idOf(line && (line.productId != null ? line.productId : line.product_id));
    let bought = null;
    if (pid) { const p = all.find(x => idOf(x.id) === pid); if (p) bought = p.d ? false : p; }
    if (bought === null) {
      const want = varIds(line).map(pairKey).sort().join("|");
      if (want) { const hits = live.filter(p => (Array.isArray(p.pv) ? p.pv : []).map(a => pairKey(Array.isArray(a) ? a : String(a).split(":"))).sort().join("|") === want); if (hits.length === 1) bought = hits[0]; }
    }
    if (!bought) return Object.assign({}, none, { live: live.length });
    const sku = up(bought.sku), ids = live.filter(p => up(p.sku) === sku).map(p => idOf(p.id));
    return { mode: ids.length === live.length ? "all" : "some", productIds: ids, boughtId: idOf(bought.id), sku, live: live.length };
  }

  /**
   * Does an alias on this listing still answer the order that carries this SKU once the design is saved under `sku`? False when the SKU
   * Etsy carries keeps the line a custom order whatever an alias says (specialOf tests the raw SKU first: CUSTOM_6673), so only Etsy's
   * own new SKU makes the next order link. Asked of the real specialOf with a master that holds the new SKU.
   */
  function aliasWorks(rawSku, sku) {
    const raw = up(rawSku); if (!raw) return true;
    const mine = up(sku);
    return !O().specialOf({ sku: raw, title: "", variations: [] }, { sku: mine, masterEntry: function (s) { return s === mine ? {} : null; } });
  }

  /**
   * The preview (zero reads): { ok, sku, candidates, group, aliasWorks, why, name, taken }.
   *   file: the design file (a name, or an object with .name); title: the listing's title; listingId; line: the order line
   *   ({ sku, productId, variations, listingId }); table: the listing's cached inventory table (null when not loaded);
   *   hasMaster(sku): is it an exact master SKU; hasLoose(sku): does a master SKU read the same without spacing and punctuation;
   *   aliases: the saved aliases ({ listingId: { sku, bySku, … } }).
   * sku: the first candidate nothing known holds; candidates: up to 6 such SKUs, for the cloud's check (customSkuCheck), which re-checks
   * the master and the listing's own records before anything is written. ok is false when no listing id or no SKU is free (`why` says it).
   */
  function plan(o) {
    o = o || {};
    const line = o.line && typeof o.line === "object" ? o.line : {};
    const lid = digits(o.listingId != null && o.listingId !== "" ? o.listingId : line.listingId);
    const grp = group(o.table, line), out = { ok: false, sku: "", candidates: [], group: grp, aliasWorks: true, why: "", name: "", taken: [] };
    if (!lid) { out.why = "This listing has no id to make a SKU from"; return out; }
    out.name = nameOf(o.file, o.title);
    const all = candidates(lid, out.name), al = aliasIndex(o.aliases), own = tableSkus(o.table), loose = O().looseKey;
    const exact = typeof o.hasMaster === "function" ? o.hasMaster : function () { return false; }, twin = typeof o.hasLoose === "function" ? o.hasLoose : function () { return false; };
    const free = [];
    for (const sku of all) {
      const k = loose(sku), t = twin(sku);
      if (exact(sku)) out.taken.push({ sku, by: "master", detail: "already a master design" });
      else if (t) out.taken.push({ sku, by: "master", detail: "reads like the master SKU " + (typeof t === "string" ? t : "") });
      else if (al.has(k)) out.taken.push({ sku, by: "alias", detail: al.get(k) });
      else if (own.has(k)) out.taken.push({ sku, by: "listing", detail: own.get(k) });
      else free.push(sku);
    }
    out.candidates = free.slice(0, SEND_MAX);
    if (!free.length) { out.why = "No free SKU for this design"; out.aliasWorks = aliasWorks(line.sku, "CHARM_" + lid.slice(-4)); return out; }
    out.ok = true; out.sku = free[0];
    out.aliasWorks = aliasWorks(line.sku, out.sku);
    return out;
  }

  return { nameOf, candidates, reserved, reservedWhy, group, aliasWorks, plan, aliasIndex, tableSkus, looseKey: function (s) { return O().looseKey(s); }, baseName, titleLead, MAX_LEN, NAME_MAX, SEND_MAX, FORMAT };
});
