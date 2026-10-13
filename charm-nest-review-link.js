/* charm-nest-review-link.js — REVLINK (Paul, 12 Oct 2026): when a person picks a vector file (a master charm) for a listing in Review, that connection is
   recorded for good and the NEXT order of the same listing is matched by it with no Review.

   Paul: "once the user chooses a given vector file to match to a given listing then that connection has to be automatically made forever and that has to be
   recorded so next time a buyer purchases the same listing the system automatically knows which vector to pull ... even in sandbox mode this works like in real life
   and if I make choices those choices should be saved moving forward whether I'm doing it in sandbox or in live mode."

   HOW A PICK IS STORED (the one mechanism the matcher already reads; nothing new is invented):
     · alias    Charm_Sku_Aliases/{listingId}.bySku[the SKU the matcher looked the line up by] = the charm      (aliasPut; no SKU on the line: the listing's own `sku`, v 2; a huggie set: `huggie`)
                The key is the SKU the line was MATCHED with (spec.boughtSku: the transaction's SKU, or the SKU Etsy's inventory gives the variation bought), so a listing whose variations
                each have a SKU keeps ONE link per variation. charm-nest-orders.js resolveSku reads it before an exact master match ("saved link beats exact match", DACHMAP).
     · option   Charm_Option_Map/{listingId}.map[option name][option value] = { field: "design", value: the charm }   (optionMapPut)
                For a line whose design comes from an OPTION (a charm an option picks, a letter's member of its family, a zodiac sign read from an option, a Mini Tag shape): the matcher
                reads the option's answer BEFORE the SKU's, so a link under the SKU would never be read. The pick is kept on the option value, for this listing only.
   Both live in the SHARED store, in the sandbox too (charmNestLibrary aliasPut / optionMapPut: written once, READ BACK by the server before it answers verified:true), so a sandbox Reset or
   Purge never reaches them and the sandbox and production read the same links.

   WHAT THIS FILE ADDS (pure functions, no Firestore, no Etsy, no AI):
     planOf(row, sku)       → { ok, via: "alias" | "option", body, key, why } or { ok:false, why }: which write makes the NEXT order of this line's listing read as `sku`
     proof(row, sku, ctx)   → { ok, designSku, skuSource, problems, why }: a NEW order of the same listing (same listing, SKU and options, new numbers) read with the maps as they are NOW
     linkOf(row, maps)      → { sku, via, key } | null: the link saved for this line's listing and SKU now (what a new order would be matched by)
     save(row, sku, {by})   → the write, read back by the server, the maps read again, the proof; { ok, via, key, sku, verified, proof }
   A caller that resolves a Review item by a pick (REVMARK's resolve, REVSUGGEST's choices) calls save(); the Review card's "Use this charm" keeps its own routine and uses proof() to say so when
   a link would not hold. Nothing here applies a charm to anything by itself: a person picks, always.

   UMD: window.CharmNestReviewLink in the page, module.exports in node tests. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestReviewLink = api;
})(typeof self !== "undefined" ? self : globalThis, function (root) {
  "use strict";
  const Orders = () => root.CharmNestOrders || (typeof require === "function" ? require("./charm-nest-orders.js") : null);
  const up = s => String(s == null ? "" : s).trim().toUpperCase();
  const lid = line => String((line && line.listingId) == null ? "" : line.listingId).replace(/\D/g, "");
  /** The SKU the matcher looked the line up by: spec.boughtSku (the transaction's SKU, or the inventory's for the variation bought), else the line's own. */
  const fromOf = (line, spec) => (spec && spec.boughtSku != null ? up(spec.boughtSku) : up(Orders() ? Orders().decodeEntities(line && line.sku) : line && line.sku));
  const SOURCES_BY_OPTION = new Set(["option", "letter", "zodiac"]);   // (skuSource values read from an option's value, ahead of the SKU)

  /** The option value that decided the line's design, when one did: { name, value, how } or null. The matcher reads an option's answer BEFORE the SKU's, so a line whose charm came from
      an option (a person's answer for the value, a letter's place in its family, a zodiac sign or a Mini Tag shape read from the option) can only be re-pointed on that option value. */
  function optionOf(spec) {
    const sp = spec || {}, opts = Array.isArray(sp.options) ? sp.options : [];
    if (sp.viaLetter && sp.viaLetter.name && sp.viaLetter.value) return { name: String(sp.viaLetter.name), value: String(sp.viaLetter.value), how: "letter" };
    if (!SOURCES_BY_OPTION.has(sp.skuSource)) return null;
    const mine = up(sp.designSku);
    const hit = opts.find(o => o && o.mapped && o.mapped.field === "design" && up(o.mapped.value) === mine && (o.mapped.source === "listing" || o.mapped.source === "shop" || /^rule:(zodiac-sign|tag-shape)/.test(String(o.mapped.source || ""))));
    return hit ? { name: String(hit.name), value: String(hit.value), how: String(hit.mapped.source) } : null;
  }

  /** Which write makes the next order of this line's listing read as `sku`. */
  function planOf(row, sku) {
    const line = row && row.line, spec = row && row.spec;
    sku = up(sku);
    if (!line || !sku) return { ok: false, why: "no line or no charm to link" };
    const listingId = lid(line);
    if (!listingId) return { ok: false, why: "this line has no listing number, so a link cannot be kept for its listing" };
    if (spec && spec.noDesign && !spec.ownDesign) return { ok: false, why: "this line is not cut (no design), so there is no charm to link" };
    // a pair of two designs is read from the pair's own link or the words that name both: one charm is not its answer
    if (spec && spec.skuSource === "pair") return { ok: false, why: "this line is two designs; link the pair from the Unmatched SKU card (Use both)" };
    const opt = optionOf(spec);
    if (opt) return { ok: true, via: "option", key: `${listingId}|${opt.name}=${opt.value}`, body: { op: "optionMapPut", listingId, optionName: opt.name, optionValue: opt.value, map: { field: "design", value: sku } }, option: opt,
      why: `the charm comes from the option “${opt.name}: ${opt.value}”, so the link is kept on that option value for this listing` };
    // a SHARED zodiac SKU whose sign came from the buyer's words (not an option) has no stable key: the next buyer may name another sign
    if (spec && spec.skuSource === "zodiac") return { ok: false, why: "the charm is read from the buyer's own words (the sign), which differ per order, so no link can be kept for the listing" };
    const from = fromOf(line, spec), huggie = !from && !!(spec && spec.huggieSet);
    return { ok: true, via: "alias", key: `${listingId}|${from || (huggie ? "(huggie)" : "(listing)")}`,
      body: Object.assign({ op: "aliasPut", listingId, sku, title: String(line.title || "").slice(0, 200) }, from ? { fromSku: from } : {}, huggie ? { huggie: true } : {}),
      why: from ? `kept for this listing and the SKU “${from}”` : "kept for this listing (the line has no SKU)" };
  }

  /** A new order of the same listing, read with the maps as they are now. `ctx` is the page's reading context (Orders.ctx()), or a test's.
      When the line was matched by the SKU Etsy's inventory table gives its variation (spec.viaInventory), a page WITHOUT that table (another computer that never asked Etsy, a table older than
      the page keeps) reads the line again: `withoutTable` says what it would read, so the one case a link depends on the table is told, not hidden. */
  function proof(row, sku, ctx) {
    const O = Orders(); sku = up(sku);
    if (!O || !row || !row.line) return { ok: false, why: "nothing to read" };
    const read = c => {
      const line = JSON.parse(JSON.stringify(row.line)), order = Object.assign({}, JSON.parse(JSON.stringify(row.order || {})), { receiptId: "NEXT" + String(row.order && row.order.receiptId || "") });
      line.transactionId = "NEXT" + String(line.transactionId || "");
      const sp = O.interpretLine(order, line, c);
      const got = up(sp.designSku), members = (O.setDesignsOf ? O.setDesignsOf(sp) : null) || [];
      const same = got === sku || (members.length === 1 && up(members[0]) === sku) || (typeof O.sameDesign === "function" && !!got && typeof c.masterEntry === "function" && O.sameDesign(got, sku, k => c.masterEntry(k)));
      const asks = (sp.problems || []).filter(p => p.kind === "unmatchedSku" || p.kind === "needsMapping").map(p => p.kind);
      return { ok: same && !asks.length, designOk: same, designSku: sp.designSku || null, skuSource: sp.skuSource || null, problems: (sp.problems || []).map(p => p.kind),
        why: same && !asks.length ? "" : same ? "the next order would still ask a question about this line" : `a new order of this listing would still read as ${sp.designSku || "no charm"} (${sp.skuSource || "no source"})` };
    };
    try {
      const c = typeof ctx === "function" ? ctx() : ctx, out = read(c);
      if (row.spec && row.spec.viaInventory && c && c.listingSkus && Object.keys(c.listingSkus).length) out.withoutTable = read(Object.assign({}, c, { listingSkus: {} }));
      return out;
    } catch (e) { return { ok: false, why: "the next order could not be read: " + ((e && e.message) || e) }; }
  }

  /** The link saved now for this line's listing and SKU (what a new order is matched by), read from the maps: { sku, via, key } or null. */
  function linkOf(row, maps) {
    const O = Orders(), line = row && row.line, spec = row && row.spec; if (!O || !line) return null;
    const m = maps || (root.B && root.B.maps) || {}, listingId = lid(line); if (!listingId) return null;
    const opt = optionOf(spec);
    if (opt) { const hit = O.optionLookup(m.optionMaps, listingId, opt.name, opt.value); if (hit && hit.field === "design" && hit.value && hit.source !== "default") return { sku: up(hit.value), via: "option", key: `${listingId}|${opt.name}=${opt.value}` }; }
    const a = m.aliases && m.aliases[listingId]; if (!a) return null;
    const from = fromOf(line, spec);
    if (from && a.bySku) { const v = O.bySkuOf(a.bySku, from); if (v) return { sku: up(v), via: "alias", key: `${listingId}|${from}` }; }
    if (!from && spec && spec.huggieSet && a.huggie) return { sku: up(a.huggie), via: "alias", key: `${listingId}|(huggie)` };
    if (!from && !(spec && spec.huggieSet) && a.sku) return { sku: up(a.sku), via: "alias", key: `${listingId}|(listing)` };
    return null;
  }

  /** The pick, kept: ONE write (aliasPut / optionMapPut), read back by the server before it says verified:true, then the page's maps are read again and a new order of the listing is read
      as proof. Throws when the link could not be saved or read back (nothing is counted as saved); resolves { ok:false, why } when the pick has no key to be kept under. `deps` is for tests. */
  async function save(row, sku, opts) {
    opts = opts || {};
    const plan = planOf(row, sku); if (!plan.ok) return plan;
    const call = opts.api || root.api; if (typeof call !== "function") throw new Error("no way to reach the cloud from here");
    const who = String(opts.by || (root.B && root.B.employee) || "").trim(); if (!who) throw new Error("the link needs the signed-in name: sign in first");
    const r = await call("charmNestLibrary", Object.assign({}, plan.body, { by: who }));
    if (!r || r.verified !== true || up(r.sku) !== up(sku)) throw new Error((r && r.error) || "the link was not read back from the cloud, so it is not counted as saved");
    const O = opts.orders || root.Orders;
    if (O && typeof O.loadMaps === "function") await O.loadMaps(true);
    const p = proof(row, sku, opts.ctx || (O && O.ctx));
    return { ok: true, via: plan.via, key: plan.key, sku: up(sku), verified: true, readBack: { kind: r.kind, key: r.key, sku: r.sku }, proof: p, why: plan.why };
  }

  /** The pick a person makes in Review for a line: save() above, then (when the order is in Review by a person's word: REVMARK) its mark is resolved with how:"pick", so ONE call does what a Review choice means.
      A pick that cannot be kept for the listing (no key: two designs, a sign from the buyer's words) is NOT resolved by this call: `{ ok:false, why }`, the caller says why. */
  async function pick(row, sku, opts) {
    opts = opts || {};
    const saved = await save(row, sku, opts); if (!saved.ok) return saved;
    const M = opts.mark || root.CharmNestReviewMark, rid = row && row.order && row.order.receiptId != null ? String(row.order.receiptId) : "";
    let resolved = null;
    if (M && rid && typeof M.isMarked === "function" && M.isMarked(rid) && typeof M.resolve === "function") resolved = await M.resolve(rid, { by: opts.by || (root.B && root.B.employee) || undefined, how: "pick", sku: up(sku), rowKey: row && row.key });
    return Object.assign({}, saved, { resolved });
  }

  return { planOf, proof, linkOf, save, pick, optionOf, fromOf };
});
