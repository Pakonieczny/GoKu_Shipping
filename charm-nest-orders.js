/*  charm-nest-orders.js — reading an order line, naming a set, holding an order.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Pure, deterministic, shared by the sorter page and the node tests. Nothing
 *  in here asks a model anything: material comes from the Design Station's
 *  classifier, form / size / chain from the listing's options through the
 *  option map, the SKU from the transaction (or a learned alias). Free text
 *  (personalisation, buyer message, staff note, messages) is only carried
 *  along for the engraving classifier (design §5.2, §7.1).
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.CharmNestOrders = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  /* ═══ 1 · material: the station's metal key → the sorter's card ═══════ */
  const METAL_TO_CARD = { gold: "gold", silver: "silver", rose: "rose", "10k": "gold10k", "14k": "gold14k" };
  const CARD_TAG = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
  const CARD_LABEL = { gold: "GF 14/20", silver: "SS", rose: "RG 14/20", gold10k: "10K Gold", gold14k: "14K Gold" };

  /* ═══ 2 · option maps ══════════════════════════════════════════════════
     Charm_Option_Map/{listingId or "*"} = { optionName: { value: { field, value } } }. Names and values are matched
     after normalisation (lower-case, trimmed, single spaces). The built-in "*" map below covers the plain literal
     values the shop's listings use; anything else is a "Needs mapping" review item until a person maps it once. */
  const norm = s => String(s == null ? "" : s).replace(/&quot;/g, "\"").toLowerCase().replace(/\s+/g, " ").trim();
  const FORM_VALUES = {
    "necklace": "necklace", "pendant necklace": "necklace", "charm necklace": "necklace", "necklace (with chain)": "necklace", "with chain": "necklace", "with necklace": "necklace", "necklace & charm": "necklace", "charm + chain": "necklace", "charm and chain": "necklace", "charm with chain": "necklace",
    "earrings": "earrings", "earring": "earrings", "pair of earrings": "earrings", "earrings (pair)": "earrings", "single earring": "earring-single",
    "charm only": "charm", "charm": "charm", "pendant only": "charm", "pendant": "charm", "charm only (no chain)": "charm", "no chain": "charm", "charm without chain": "charm", "loose charm": "charm",
    "huggie": "huggie", "huggie charm": "huggie", "huggie hoop": "huggie", "huggie hoops": "huggie", "huggie earrings": "huggie", "huggies": "huggie",
    "bracelet": "bracelet", "charm bracelet": "bracelet", "anklet": "anklet", "keychain": "keychain", "key chain": "keychain", "keyring": "keychain"
  };
  const SIZE_VALUES = { "xs": "XS", "extra small": "XS", "s": "S", "small": "S", "m": "M", "medium": "M", "l": "L", "large": "L", "xl": "XL", "extra large": "XL", "mini": "XS", "regular": "M", "standard": "M" };
  const DEFAULT_OPTION_MAP = { "*": {} };
  for (const nm of ["style", "type", "product", "option", "options", "select", "item", "product type", "jewelry type", "jewellery type", "choose", "choice", "charm type", "charm style", "jewelry style", "jewellery style", "necklace or charm", "charm or necklace"]) { DEFAULT_OPTION_MAP["*"][nm] = {}; for (const [v, f] of Object.entries(FORM_VALUES)) DEFAULT_OPTION_MAP["*"][nm][v] = { field: "form", value: f }; }
  for (const nm of ["size", "charm size", "pendant size"]) { DEFAULT_OPTION_MAP["*"][nm] = {}; for (const [v, s] of Object.entries(SIZE_VALUES)) DEFAULT_OPTION_MAP["*"][nm][v] = { field: "size", value: s }; }
  /* A shop writes the same choice in its own word order — "Necklace CHARM" for what this vocabulary calls "charm
     necklace" — and adds a word that is not part of the choice: "CHARM + Engraving" is a charm, engraved. A value is read
     as a form when its words, with those fillers removed, are exactly the words of one entry in FORM_VALUES. Anything
     else stays unmapped for a person to decide once: nothing here guesses what a buyer ordered. */
  const FORM_FILLER = new Set(["engraving", "engraved", "engrave", "personalised", "personalized", "with", "and", "plus", "only", "the", "a", "an", "&", "+", "-"]);
  const wordSet = v => new Set(String(v || "").toLowerCase().replace(/[^\w\s+&-]+/g, " ").split(/[\s+&-]+/).filter(w => w && !FORM_FILLER.has(w)));
  const sameWords = (a, b) => a.size === b.size && [...a].every(w => b.has(w));
  const FORM_WORDS = Object.entries(FORM_VALUES).map(([k, f]) => ({ words: wordSet(k), form: f }));
  const formByWords = value => { const w = wordSet(value); if (!w.size) return null; const hit = FORM_WORDS.filter(x => sameWords(w, x.words)); const forms = [...new Set(hit.map(x => x.form))]; return forms.length === 1 ? forms[0] : null; };
  const isFormOption = name => /^\s*(charm |pendant |jewel+ery |jewelry )?(type|style|form)\s*$/i.test(String(name || ""));
  const isMetalOption = name => /metal|colou?r|finish|plating/i.test(String(name || ""));
  const isPersonalisation = name => /personali[sz]ation|engraving text|custom text/i.test(String(name || ""));
  const isSizeOption = name => /(^|\s)size\s*$/i.test(String(name || ""));
  const isChainOption = name => /chain|necklace length|length|extender/i.test(String(name || ""));
  // 16" is how a shop writes a necklace length. The old rule ended in \b, which cannot follow a quote mark, so every
  // length written that way fell through as an unmapped option and held the line.
  const looksLikeLength = v => /^\s*\d+(\.\d+)?\s*("|''|\u201d|\u2033|in\b|inch|cm\b|mm\b)/i.test(String(v || "")) || /^\s*\d{1,2}(\.\d)?\s*$/.test(String(v || ""));
  const bareValue = v => norm(String(v || "").replace(/[^\w\s.+&-]+/g, " ")).split(/[\s+&]+/).filter(w => w && !FORM_FILLER.has(w)).join(" ");
  const looksLikeSize = v => /^(xs|s|m|l|xl|xxl|\d{1,2}(\.\d)?\s*(mm|cm)?( us)?)$/i.test(bareValue(v));

  /** { field, value, source } for one option, or null when nothing deterministic applies. */
  function optionLookup(maps, listingId, name, value) {
    const n = norm(name), v = norm(value);
    const tryMap = (m, src) => { if (!m) return null; const byName = m[n] || m[Object.keys(m).find(k => norm(k) === n)]; if (!byName) return null; const hit = byName[v] || byName[Object.keys(byName).find(k => norm(k) === v)]; return hit ? Object.assign({ source: src }, hit) : null; };
    return tryMap(maps && maps[String(listingId)], "listing") || tryMap(maps && maps["*"], "shop") || tryMap(DEFAULT_OPTION_MAP["*"], "default");
  }

  /* ═══ 3 · SKUs that are not charms ═════════════════════════════════════ */
  /** noDesign = { patterns: [regex source strings], skus: [strings] } — Charm_Sku_NoDesign flattened. */
  function isNoDesign(sku, noDesign) {
    const s = String(sku || "").trim().toUpperCase(); if (!s) return false;
    if ((noDesign && noDesign.skus || []).some(x => String(x).trim().toUpperCase() === s)) return true;
    for (const p of (noDesign && noDesign.patterns) || []) { try { if (new RegExp(p, "i").test(s)) return true; } catch (_) { /* a bad pattern never matches */ } }
    return false;
  }
  /** The transaction's SKU, or the alias learned for its listing. */
  function resolveSku(line, aliases) {
    const raw = String(line.sku || "").trim().toUpperCase();
    if (raw) return { sku: raw, source: "transaction" };
    const a = aliases && aliases[String(line.listingId)];
    if (a && a.sku) return { sku: String(a.sku).trim().toUpperCase(), source: "alias" };
    return { sku: "", source: null };
  }

  /* ═══ 4 · the line spec ════════════════════════════════════════════════ */
  /**
   * order: the bridge order (design §4.4); line: one of its lines
   * ctx: { optionMaps, aliases, noDesign, masterEntry?: (sku) => entry|null }
   * → { designSku, skuSource, material, materialKey, materialLabel, form, size, chain, quantity, personalization[],
   *     buyerMessage, staffNote, messages[], updateTs, options[], problems[], noDesign, engraveCandidate, sources }
   */
  function interpretLine(order, line, ctx) {
    ctx = ctx || {};
    const problems = [];
    const { sku, source: skuSource } = resolveSku(line, ctx.aliases);
    const noDesign = isNoDesign(sku, ctx.noDesign) || (!sku && isNoDesign(line.title, ctx.noDesign));
    const metalKey = String(line.metalKey || "");
    const material = METAL_TO_CARD[metalKey] || null;
    if (!noDesign && !material) problems.push({ kind: "needsMaterial", metalKey: metalKey || null, metalLabel: line.metalLabel || "", listingId: String(line.listingId || ""), options: (line.variations || []).map(v => `${v.name}: ${v.value}`), title: line.title || "" });
    const spec = { designSku: sku || null, skuSource, material, materialKey: metalKey || null, materialLabel: line.metalLabel || (material ? CARD_LABEL[material] : ""), form: null, size: null, chain: null, quantity: Math.max(1, Math.round(+line.quantity || 1)), personalization: (line.personalization || []).map(s => String(s)).filter(s => s.trim()), buyerMessage: String(line.buyerMessage || order.buyerMessage || ""), staffNote: String(line.staffNote || order.staffNote || ""), messages: (line.messages || order.messages || []).slice(-5), updateTs: +order.updateTs || 0, options: [], problems, noDesign, sources: { material: "station classifier (Metal/Colour option first)", sku: skuSource } };
    for (const v of line.variations || []) {
      const name = v.name || v.formatted_name, value = String(v.value != null ? v.value : v.formatted_value || "").replace(/&quot;/g, "\"").trim();
      if (!name || !value || isMetalOption(name) || isPersonalisation(name)) continue;
      const hit = optionLookup(ctx.optionMaps, line.listingId, name, value);
      let mapped = hit ? { field: hit.field, value: hit.value, source: hit.source } : null;
      if (!mapped) {                                                            // deterministic name rules, no free-text reading
        const asForm = formByWords(value);
        if (isSizeOption(name) && looksLikeSize(value)) mapped = { field: "size", value: SIZE_VALUES[bareValue(value)] || bareValue(value).toUpperCase().replace(/\s+/g, ""), source: "rule:size" };
        else if (isChainOption(name) && looksLikeLength(value)) mapped = { field: "chain", value, source: "rule:length" };
        else if (isChainOption(name) && asForm) mapped = { field: "form", value: asForm, source: "rule:form" };
        else if (isFormOption(name) && looksLikeLength(value)) mapped = { field: "chain", value, source: "rule:length" };
        else if (isFormOption(name) && asForm) mapped = { field: "form", value: asForm, source: "rule:form" };
      }
      spec.options.push({ name, value, mapped });
      if (!mapped) { if (!noDesign) problems.push({ kind: "needsMapping", listingId: String(line.listingId || ""), optionName: name, optionValue: value, title: line.title || "" }); continue; }
      if (mapped.field === "ignore") continue;
      if (mapped.field === "form" && !spec.form) spec.form = mapped.value;
      else if (mapped.field === "size" && !spec.size) spec.size = mapped.value;
      else if (mapped.field === "chain" && !spec.chain) spec.chain = mapped.value;
    }
    if (!noDesign && !sku) problems.push({ kind: "unmatchedSku", reason: "no SKU on the transaction and no alias for the listing", listingId: String(line.listingId || ""), title: line.title || "" });
    if (ctx.masterEntry && sku && !noDesign) {
      const entry = ctx.masterEntry(sku);
      if (!entry) problems.push({ kind: "unmatchedSku", reason: "not in any master file", sku, listingId: String(line.listingId || ""), title: line.title || "" });
      else if (entry.blocked) problems.push({ kind: "blockedSku", reason: entry.blocked, sku });
      else if (entry.sizes && Object.keys(entry.sizes).length) { if (!spec.size || !entry.sizes[spec.size]) problems.push({ kind: "missingSize", sku, size: spec.size, available: Object.keys(entry.sizes) }); }
    }
    spec.engraveCandidate = !noDesign && (spec.personalization.length > 0 || !!spec.buyerMessage.trim() || !!spec.staffNote.trim() || spec.messages.length > 0);
    return spec;
  }
  const lineKey = (order, line) => `${order.receiptId}_${line.transactionId}`;
  const poolId = (order, line, copy) => `${order.receiptId}_${line.transactionId}_${copy}`;

  /* ═══ 5 · sets, dates, names ═══════════════════════════════════════════ */
  const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  /** One local clock: the calendar day of the machine, never UTC. */
  const localDay = (d = new Date()) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  /** "Sep.16.26" for the same local day. */
  const dateTag = (d = new Date()) => `${MON[d.getMonth()]}.${String(d.getDate()).padStart(2, "0")}.${String(d.getFullYear()).slice(-2)}`;
  const dateTagOfDay = day => { const [y, m, dd] = String(day).split("-").map(Number); return dateTag(new Date(y, m - 1, dd, 12)); };
  const setId = (day, seq) => `set-${day}-${seq}`;
  const setLabel = seq => `Set-${seq}`;
  const setFolder = (day, seq) => `charmnest/sets/${day}/Set-${seq}`;
  const sheetName = (card, day, seq, n) => `${CARD_TAG[card] || card}_${dateTagOfDay(day)}_Set-${seq}_Sheet-${n}`;
  const sheetFolder = (card, day, seq, n) => `${setFolder(day, seq)}/${sheetName(card, day, seq, n)}`;

  /* ═══ 6 · labels: byte-identical to the Design Station's encoder ═══════ */
  function toB36(numStr) {
    const clean = String(numStr).trim();
    if (!/^\d+$/.test(clean)) return clean;
    let n = BigInt(clean);
    if (n === 0n) return "0";
    const digits = "0123456789abcdefghijklmnopqrstuvwxyz";
    let out = "";
    while (n > 0n) { out = digits[Number(n % 36n)] + out; n = n / 36n; }
    return out;
  }
  function encodeOrderList(ids, metal) {
    const b36 = (ids || []).map(x => { const d = String(x).replace(/\D/g, ""); return d ? toB36(d) : String(x); });
    return `B36|${metal}|` + b36.join(".");
  }
  function safeChunks(arr, metal, maxChars = 1000, startSize = 50, minSize = 8) {
    const out = []; let i = 0, size = startSize;
    while (i < arr.length) {
      const slice = arr.slice(i, i + size);
      if (encodeOrderList(slice, metal).length > maxChars && size > minSize) { size = Math.max(minSize, Math.floor(size * 0.75)); continue; }
      out.push(slice); i += slice.length;
    }
    return out;
  }
  /** The station's metal key for a sorter card (labels carry the station's vocabulary: gold, silver, rose, 10k, 14k). */
  const CARD_TO_METAL = Object.fromEntries(Object.entries(METAL_TO_CARD).map(([k, v]) => [v, k]));

  /* ═══ 7 · holding an order whole ═══════════════════════════════════════ */
  /**
   * rows: this order's line rows { spec, state, reason, engrave: { needed, approved }, poolIds }
   * An order is committable only when every line is: nested and written; nested, written and its engraving approved;
   * or on the no-design list. Anything else holds the whole order with the first unresolved line's reason.
   */
  const DONE_STATES = new Set(["written", "labelled", "committed"]);
  function evaluateOrder(rows) {
    const lines = rows.map(r => {
      if (r.spec && r.spec.noDesign) return { key: r.key, ok: true, why: "no design (chain/packaging)" };
      if (r.state === "gone") return { key: r.key, ok: false, why: "order gone from Etsy" };
      if (r.problems && r.problems.length) return { key: r.key, ok: false, why: r.problems[0].kind === "needsMaterial" ? "needs material" : r.problems[0].kind === "needsMapping" ? "needs an option mapped" : r.problems[0].kind === "unmatchedSku" ? "SKU not in a master" : r.problems[0].kind === "missingSize" ? "no design for that size" : r.problems[0].kind };
      if (!DONE_STATES.has(r.state)) return { key: r.key, ok: false, why: r.reason || `line is ${r.state || "not nested"}` };
      if (r.engrave && r.engrave.needed && !r.engrave.approved) return { key: r.key, ok: false, why: r.engrave.state === "skipped" ? null : (r.engrave.state === "review" ? "engraving awaiting review" : r.engrave.state === "words" ? "engraving words need a decision" : r.engrave.reason || "engraving not approved") };
      return { key: r.key, ok: true, why: null };
    }).map(l => (l.why === null && !l.ok ? Object.assign(l, { ok: true }) : l));
    const first = lines.find(l => !l.ok);
    return { committable: !first, held: first ? { line: first.key, why: first.why } : null, lines };
  }

  /* ═══ 7b · what goes to the laser today, and what waits ═══════════════════════════════════════════════════════════
     A sheet costs the same to cut whether it carries ninety charms or nine, so a partial sheet is money and machine
     time thrown away. Two of the materials sell fast enough to fill a sheet most days; three do not, and could wait a
     week for a full one, which the customers who ordered them would not thank us for. So there are two regimes:

       fast (SS, GF 14/20)   — only full sheets go. A partial remainder waits for the next pull to top it up, unless a
                               piece on it is due or belongs to an order that is going anyway (below), in which case
                               the sheet is cut partial and filled as far as it can be.
       slow (RG, 10K, 14K)   — whatever there is goes, dates are not looked at, but only every `cadenceDays` days, so
                               the laser is not started for two pieces. A person can release a material early.

     And one rule over both: an order is never split across sets. An order with a 14K piece and a GF piece travels as
     one — so on a day 14K is closed, its GF piece waits too, and on the day 14K opens, its GF piece rides along even
     if that makes the GF sheet partial. Sets are then simply the groups of materials tied together by such orders:
     a material no order ties to another is a set of its own.

     This is a pure function of what it is given and is tested as one. It touches nothing. */
  const FAST_MATERIALS = new Set(["silver", "gold"]);
  const SLOW_MATERIALS = new Set(["rose", "gold10k", "gold14k"]);
  const dayDiff = (a, b) => Math.round((Date.parse(b + "T12:00:00Z") - Date.parse(a + "T12:00:00Z")) / 86400000);
  const addDays = (day, n) => new Date(Date.parse(day + "T12:00:00Z") + n * 86400000).toISOString().slice(0, 10);
  /**
   * lines: [{ key, orderId, material, areaPt2 (footprint incl. clearance), shipBy (unix s, 0 = unknown) }] — only lines
   *        that are otherwise ready to be pooled.
   * opts:  { today: "YYYY-MM-DD", capacity: { material: usablePt2 × ceiling }, lastReleased: { material: "YYYY-MM-DD" },
   *          released: { material: "YYYY-MM-DD" } (a person opened it that day), forceFill: { material: true } (a person
   *          said cut the partial), cadenceDays (2), lateDays (2) }
   * →      { take: Set(key), wait: Map(key → { kind, why, material, until, pct }), materials: { material → summary } }
   */
  function planRelease(lines, opts) {
    const today = opts.today, cadence = Math.max(1, +opts.cadenceDays || 2), lateDays = Math.max(0, +opts.lateDays == null ? 2 : +opts.lateDays);
    const lastRel = opts.lastReleased || {}, released = opts.released || {}, forceFill = opts.forceFill || {}, cap = opts.capacity || {};
    const take = new Set(), wait = new Map(), materials = {};
    const dueSoon = l => l.shipBy > 0 && (l.shipBy * 1000 - Date.parse(today + "T00:00:00")) <= lateDays * 86400000;
    // 1 · is a slow material open today?
    const openSlow = {};
    for (const m of SLOW_MATERIALS) {
      const forced = released[m] === today;
      const last = lastRel[m] || null;
      const open = forced || !last || dayDiff(last, today) >= cadence;
      openSlow[m] = open;
      materials[m] = { regime: "slow", open, forced, lastReleased: last, next: open ? today : addDays(last, cadence), pieces: 0, taken: 0 };
    }
    for (const m of FAST_MATERIALS) materials[m] = { regime: "fast", pieces: 0, taken: 0, full: 0, pct: 0, partial: false, forcedBy: [] };
    // 2 · an order travels whole: it goes only if every slow material it touches is open
    const byOrder = new Map();
    for (const l of lines) { if (!byOrder.has(l.orderId)) byOrder.set(l.orderId, []); byOrder.get(l.orderId).push(l); }
    const gated = [];
    for (const [oid, ls] of byOrder) {
      const mats = new Set(ls.map(l => l.material));
      const closed = [...mats].filter(m => SLOW_MATERIALS.has(m) && !openSlow[m]);
      if (closed.length) { for (const l of ls) wait.set(l.key, { kind: "slow", material: closed[0], until: materials[closed[0]].next, why: `${closed[0]} opens ${materials[closed[0]].next}` }); continue; }
      for (const l of ls) gated.push(Object.assign({ multi: mats.size > 1, urgent: dueSoon(l) }, l));
    }
    for (const l of lines) if (materials[l.material]) materials[l.material].pieces++;
    // 3 · slow materials that are open: everything goes
    for (const l of gated) if (SLOW_MATERIALS.has(l.material)) { take.add(l.key); materials[l.material].taken++; }
    // 4 · fast materials: full sheets go; the remainder waits unless something on it has to travel
    for (const m of FAST_MATERIALS) {
      const cand = gated.filter(l => l.material === m).sort((a, b) => (b.multi - a.multi) || (b.urgent - a.urgent) || ((a.shipBy || 1e12) - (b.shipBy || 1e12)));
      const usable = +cap[m] || 0, sum = materials[m];
      if (!cand.length) continue;
      if (!(usable > 0)) { cand.forEach(l => take.add(l.key)); sum.taken = cand.length; continue; }   // no plate known: never hold on a guess
      const total = cand.reduce((s, l) => s + (+l.areaPt2 || 0), 0);
      const full = Math.floor(total / usable); sum.full = full;
      const fullCap = full * usable;
      let acc = 0; const inFull = [], rest = [];
      for (const l of cand) { if (acc + (+l.areaPt2 || 0) <= fullCap + 1e-6) { acc += +l.areaPt2 || 0; inFull.push(l); } else rest.push(l); }
      inFull.forEach(l => take.add(l.key));
      const forced = rest.filter(l => l.multi || l.urgent);
      const restArea = rest.reduce((s, l) => s + (+l.areaPt2 || 0), 0);
      sum.pct = Math.round(Math.min(1, restArea / usable) * 100);
      if (forced.length || forceFill[m]) {
        // a sheet that has to be cut is filled as far as it will go: the whole remainder rides, up to one more sheet
        let acc2 = 0; for (const l of rest) { if (acc2 + (+l.areaPt2 || 0) <= usable + 1e-6) { acc2 += +l.areaPt2 || 0; take.add(l.key); } else wait.set(l.key, { kind: "fill", material: m, pct: sum.pct, why: `waits for a full ${m} sheet` }); }
        sum.partial = true; sum.forcedBy = forceFill[m] ? ["operator"] : [...new Set(forced.map(l => l.multi ? `order ${l.orderId} travels with another material` : `order ${l.orderId} is due`))];
      } else for (const l of rest) wait.set(l.key, { kind: "fill", material: m, pct: sum.pct, why: `waits for a full ${m} sheet · ${sum.pct}% so far` });
      sum.taken = cand.filter(l => take.has(l.key)).length;
    }
    return { take, wait, materials };
  }
  /** Which materials must travel together: those tied by an order with pieces in more than one. Each group is a set.
   *  → { material → groupKey }, groupKey = the group's materials sorted and joined with "+". */
  function kinGroups(lines) {
    const parent = {}; const find = x => { while (parent[x] !== x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; };
    const union = (a, b) => { const ra = find(a), rb = find(b); if (ra !== rb) parent[ra] = rb; };
    for (const l of lines) if (l.material) parent[l.material] = parent[l.material] || l.material;
    const byOrder = new Map();
    for (const l of lines) { if (!l.material) continue; if (!byOrder.has(l.orderId)) byOrder.set(l.orderId, new Set()); byOrder.get(l.orderId).add(l.material); }
    for (const mats of byOrder.values()) { const arr = [...mats]; for (let i = 1; i < arr.length; i++) union(arr[0], arr[i]); }
    const groups = {}; for (const m of Object.keys(parent)) { const r = find(m); (groups[r] = groups[r] || []).push(m); }
    const out = {}; for (const ms of Object.values(groups)) { const key = ms.slice().sort().join("+"); for (const m of ms) out[m] = key; }
    return out;
  }

  /* ═══ 8 · the run ══════════════════════════════════════════════════════ */
  const RUN_STEPS = ["pull", "claim", "pool", "plan", "nest", "checkpoint", "engrave", "revalidate", "labels", "commit", "complete"];
  const HALF = { pull: "A", claim: "A", pool: "A", plan: "A", nest: "A", checkpoint: "A", engrave: "B", revalidate: "B", labels: "B", commit: "B", complete: "B" };
  const nextStep = s => { const i = RUN_STEPS.indexOf(s); return i < 0 || i === RUN_STEPS.length - 1 ? null : RUN_STEPS[i + 1]; };
  const stepIndex = s => RUN_STEPS.indexOf(s);

  return { METAL_TO_CARD, CARD_TO_METAL, CARD_TAG, CARD_LABEL, DEFAULT_OPTION_MAP, FORM_VALUES, SIZE_VALUES, norm, optionLookup, isNoDesign, resolveSku, interpretLine, lineKey, poolId,
    localDay, dateTag, dateTagOfDay, setId, setLabel, setFolder, sheetName, sheetFolder, toB36, encodeOrderList, safeChunks, evaluateOrder, planRelease, kinGroups, FAST_MATERIALS, SLOW_MATERIALS, RUN_STEPS, HALF, nextStep, stepIndex, DONE_STATES };
});
