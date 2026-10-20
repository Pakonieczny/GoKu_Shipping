/* charm-nest-charm-fix.js — WRONGCHARM (Paul, 18 Oct 2026; order 4193072538, Etsy listing 1819191541 "Tiny Moon Charm Celestial Charm Moon Jewelry Add On Charm Minimalist Charm Gold Accent
   Birthday Gift", GF, a necklace add-on): "this order/listing is actually incorrectly matched to the wrong SKU from our repo. It should be matched with" MOON_3288 (a 9.1 x 14.9 mm moon).
   The listing's Etsy SKU is "TINY MOON & STARS 2", an exact master SKU (a star and a moon, two bodies under one label), so the line was matched, pooled and nested on a GF sheet with no question
   asked. A charm could be picked for a listing only from a Review card (the similar-charm chips, pickSimilar; an option's pickCharm), and an order whose SKU matched never reaches Review.

   THE CORRECTION ("Wrong charm?" beside the Vector design of the order window, and on the order's card in the Orders tab, which opens that window on it): a small picker (the Review card's own
   similar charms, and the master index search field) where a person picks the right library charm. Nothing new is stored and nothing new re-makes an order:
     · the pick is the SAME saved link a Review pick writes (charm-nest-review-link.js pick: aliasPut under Charm_Sku_Aliases/{listing}.bySku[the SKU the line was matched by], or optionMapPut on the
       option value that gives the charm), keyed exactly as the matcher reads it (CharmNestReviewLink.planOf / linkOf: listing + SKU, or listing + option value);
     · the matcher already reads that link BEFORE an exact master match of the same SKU (DACHMAP, charm-nest-orders.js resolveSku: "saved link beats exact match", saved-link-wins.cjs), and only for THIS
       listing: the same SKU on another listing is read exactly as before;
     · every open order of the listing is then re-made with the pick by LISTPICK (Review.relinkNow, charm-nest-listing-relink.js): a line not made yet reads it, a piece on an open sheet is swapped (its
       old room first, else the next sheet), a cut / committed / released / protected one is held and told for a person; each order gets "Charm updated from listing pick by <name>" on its timeline.
   Before anything is written the person confirms ONE line: "Use MOON_3288 for every order of listing 1819191541?" with the count of the open orders it changes. The Etsy listing (its SKU) is never changed
   here: the link lives in the sorter's saved links only.

   WHAT THIS FILE IS (pure functions: no Firestore, no Etsy, no page; the bridge's Review.fixCharm does the work):
     offerOf(row, h)                → { ok, why, listingId, cur, from, key, via }   whether a line's charm can be corrected for its listing (the button is shown only when ok)
     affectedOf(rows, row, sku, h)  → { key, rows, orders, made }                   the open lines of the listing that read under the SAME link key (the matcher's granularity)
     askText(sku, listingId, aff)   → the one confirming line
     mapsWith(maps, plan, O)        → copies of { aliases, optionMaps } with the planned link in them (nothing is changed): a new order of the listing is read with them BEFORE the write, so a link
                                      that would not take precedence is refused with its reason instead of being saved
   h = { RL: CharmNestReviewLink, O: CharmNestOrders (setDesignsOf), cancelled(row) }

   UMD: window.CharmNestCharmFix in the page, module.exports in node tests. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestCharmFix = api;
})(typeof self !== "undefined" ? self : globalThis, function (root) {
  "use strict";
  const up = s => String(s == null ? "" : s).trim().toUpperCase();
  const lidOf = line => String((line && line.listingId) == null ? "" : line.listingId).replace(/\D/g, "");
  const FINISHED = new Set(["gone", "skipped", "noDesign"]);
  const ASKS = new Set(["unmatchedSku", "needsMapping", "blockedSku"]);   // (a line that still asks which charm it is is answered on its Review card, as before)
  const no = why => ({ ok: false, why });
  const RLof = h => (h && h.RL) || root.CharmNestReviewLink || (typeof require === "function" ? require("./charm-nest-review-link.js") : null);

  /** Whether the charm a line is matched to can be corrected for its listing here. */
  function offerOf(row, h) {
    h = h || {}; const RL = RLof(h);
    if (!row || !row.line || !row.spec || row.loading) return no("the line is not read yet");
    if (row.fromRecord) return no("this order is read from the records, not the open orders");
    if (FINISHED.has(row.state)) return no("the line is finished, skipped or gone");
    if (h.cancelled && h.cancelled(row)) return no("the order is cancelled");
    const sp = row.spec;
    if (sp.noDesign && !sp.ownDesign) return no("this line is not cut (no design)");
    if (sp.ownDesign || sp.customDone) return no("this line is made from its own artwork, not a library charm");
    if ((sp.problems || []).some(p => p && ASKS.has(p.kind))) return no("this line still asks which charm it is: its Review card answers that");
    const cur = up(sp.designSku); if (!cur) return no("no charm is matched to this line yet");
    let set = []; try { set = (h.O && h.O.setDesignsOf ? h.O.setDesignsOf(sp) : null) || []; } catch (_) { set = []; }
    if (set.length > 1) return no("this line is a set of several charms");
    if (!RL || !RL.planOf) return no("the saved links did not load");
    const plan = RL.planOf(row, cur); if (!plan.ok) return no(plan.why);
    return { ok: true, why: "", listingId: lidOf(row.line), cur, from: RL.fromOf ? RL.fromOf(row.line, sp) : "", key: plan.key, via: plan.via, option: plan.option || null };
  }

  /** The open lines of the line's listing that the pick reaches: those read under the SAME link key (listing + SKU, or listing + option value), never another SKU or option value of the listing. */
  function affectedOf(rows, row, sku, h) {
    h = h || {}; const RL = RLof(h), lid = lidOf(row && row.line), want = up(sku);
    const plan = RL && row ? RL.planOf(row, want) : null; if (!plan || !plan.ok) return { key: "", rows: [], orders: [], made: 0 };
    const out = [];
    for (const rr of rows || []) {
      if (!rr || !rr.line || !rr.spec || lidOf(rr.line) !== lid || FINISHED.has(rr.state)) continue;
      if (h.cancelled && h.cancelled(rr)) continue;
      let p = null; try { p = RL.planOf(rr, want); } catch (_) { p = null; }
      if (p && p.ok && p.key === plan.key) out.push(rr);
    }
    if (row && !out.includes(row)) out.unshift(row);
    return { key: plan.key, rows: out, orders: [...new Set(out.map(r => String(r.order && r.order.receiptId)))], made: out.filter(r => (r.poolIds || []).length).length };
  }

  /** The one line a person confirms before anything is written. (INPLACE, Paul 20 Oct 2026: a made piece goes into its old spot, or onto the latest unfinished sheet; only a laser-cut one is held) */
  function askText(sku, listingId, aff) {
    const n = aff && aff.orders ? aff.orders.length : 0, made = (aff && aff.made) || 0;
    return `Use ${up(sku)} for every order of listing ${listingId}? ${n} open order${n === 1 ? "" : "s"} of it ${n === 1 ? "changes" : "change"}${made ? ` (${made} already made: each new piece goes into the old one's spot, or onto the latest unfinished sheet when it does not fit; a laser-cut piece is held for a person)` : ""}; the same SKU on other listings stays as it is.`;
  }

  /** { aliases, optionMaps } with the planned link written into copies (the maps themselves are never changed). */
  function mapsWith(maps, plan, O) {
    const m = maps || {}, aliases = Object.assign({}, m.aliases || {}), optionMaps = Object.assign({}, m.optionMaps || {});
    if (!plan || !plan.ok || !plan.body) return { aliases, optionMaps };
    const b = plan.body, id = String(b.listingId);
    if (plan.via === "alias") {
      const a = Object.assign({}, aliases[id] || {});
      if (b.fromSku) { a.v = 2; a.bySku = Object.assign({}, a.bySku || {}, { [up(b.fromSku)]: up(b.sku) }); }
      else if (b.huggie) a.huggie = up(b.sku);
      else { a.v = 2; a.sku = up(b.sku); }
      aliases[id] = a;
    } else if (plan.via === "option") {
      const norm = O && O.norm ? O.norm : (s => String(s || "").toLowerCase().replace(/\s+/g, " ").trim());
      const lm = Object.assign({}, optionMaps[id] || {}), nk = Object.keys(lm).find(k => norm(k) === norm(b.optionName)) || b.optionName;
      const byName = Object.assign({}, lm[nk] || {}), vk = Object.keys(byName).find(k => norm(k) === norm(b.optionValue)) || b.optionValue;
      byName[vk] = Object.assign({}, byName[vk] || {}, b.map); lm[nk] = byName; optionMaps[id] = lm;
    }
    return { aliases, optionMaps };
  }

  return { offerOf, affectedOf, askText, mapsWith, up };
});
