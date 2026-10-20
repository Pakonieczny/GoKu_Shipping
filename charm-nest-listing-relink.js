/* charm-nest-listing-relink.js — LISTPICK (Paul, 18 Oct 2026; order 4201900041 "Charm picked: FEATHER_4102" while order 4204208130 of the SAME listing stayed on its old feather):
   "Moving forward if a given listing is manually corrected and released by the user then immediately all already purchased orders using the exact same listing must be identified and
   updated accordingly ... in the background with no user intervention. If the charms have already been nested onto a sheet ... those already nested charms must be removed and replaced
   with the updated user selected charm for each affected purchased listing/order. If the updated charm/charms do not fit into the vacant space left behind on the sheet then that
   charm/charms must move onto the next available sheet and everything must be updated concurrently to reflect the change."

   A person's pick for a listing is the saved link REVLINK keeps (charm-nest-review-link.js): Charm_Sku_Aliases/{listing}.bySku[SKU] (or the listing's own sku / huggie), or
   Charm_Option_Map/{listing}.map[option][value] = { field: "design" }. The SAME LISTING is matched at exactly the granularity the matcher reads a link at: CharmNestReviewLink.linkOf(row, maps)
   (listing + the SKU the line was matched by, or listing + the option value that decides its charm). A line whose own link did not move (another SKU of the listing, another option value) is
   never touched.

   WHAT THIS FILE IS (pure functions: no Firestore, no Etsy, no page; the bridge's Review.relink does the work with the page's own take-off and intake):
     changedListings(prev, next)                  → Set of listing ids whose alias record or option answers differ between two copies of the maps
     snapshot(rows, prev, next, linkOf)           → [{ key, row, from, was }] the live lines of those listings as they read BEFORE the new maps (from: the design; was: the link it was read by)
     classify(entry, h)                           → { action, why, to, from, link, fixed?, hold? } one line, as it reads NOW (after Orders.interpretAll with the new maps):
         "skip"    not this pick's business: no link, the link did not move for it, not read by the link, a question open, a person's own pick for this order, finished, held
         "none"    already right (its pieces already carry the picked charm, under any of its names: another computer swapped them, or this one did)
         "design"  nothing made yet (or its pieces taken off): the line simply reads the new charm; no write but its timeline event
         "swap"    pieces pooled or on a sheet not cut (a draft, a formed, released or committed set): the old pieces come off in one commit and the line is made up again with the picked charm (Review.repool, repoolChanged)
         "kept"    the order is in Review right now (REVHOLD keeps its piece on its sheet): left until the Review is resolved, then swapped
         "flag"    a piece is laser cut (INPLACE, Paul 20 Oct 2026: only cut; fixedOf), or inside a cut Rose Gold / 10K / 14K green line: nothing is changed physically; a person is told (hold: the line is held for them)
     h = { maps, linkOf(row, maps), made(row) → [sku of each live piece] | null (not known), fixedOf(row) → [{ name, why, cut, local }], keepsPlace(row), ownPick(row) → sku | "",
           sameDesign(a, b), cancelled(row) }
   Idempotent by construction: the decision is read from what the line and its pieces ARE, never from a memory of what was done, so a second run over the same state answers "none".

   UMD: window.CharmNestListingRelink in the page, module.exports in node tests. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestListingRelink = api;
})(typeof self !== "undefined" ? self : globalThis, function () {
  "use strict";
  const up = s => String(s == null ? "" : s).trim().toUpperCase();
  const lidOf = line => String((line && line.listingId) == null ? "" : line.listingId).replace(/\D/g, "");
  const FINISHED = new Set(["gone", "skipped", "noDesign"]);
  const BY_LINK = new Set(["alias", "option"]);   // (the skuSource of a line read by a person's saved link: a SKU answer, or an option value's)
  const same = (a, b) => a === b || (() => { try { return JSON.stringify(a) === JSON.stringify(b); } catch (_) { return false; } })();

  /** The listings whose answers differ between two copies of the maps ({ aliases, optionMaps }). The shop-wide option answers ("*") never pick a charm (the server keeps a design per listing only). */
  function changedListings(prev, next) {
    const out = new Set(), p = prev || {}, n = next || {};
    for (const field of ["aliases", "optionMaps"]) {
      const a = p[field] || {}, b = n[field] || {};
      if (a === b) continue;
      for (const id of new Set([...Object.keys(a), ...Object.keys(b)])) if (id !== "*" && !same(a[id], b[id])) out.add(String(id).replace(/\D/g, ""));
    }
    out.delete("");
    return out;
  }
  /** The live lines of the listings that changed, as they read before the new maps: what each was (its design) and the link it was read by (linkOf with the OLD maps). */
  function snapshot(rows, prev, next, linkOf) {
    const lids = changedListings(prev, next); if (!lids.size) return [];
    const out = [];
    for (const row of rows || []) {
      if (!row || !row.line || !row.key || row.state === "gone" || !lids.has(lidOf(row.line))) continue;
      let was = null; try { was = row.spec ? linkOf(row, prev) : null; } catch (_) { was = null; }
      out.push({ key: row.key, row, from: row.spec && !row.spec.noDesign ? up(row.spec.designSku) : "", was: was ? { sku: up(was.sku), key: was.key, via: was.via } : null, at: Date.now() });
    }
    return out;
  }

  const res = (action, why, more) => Object.assign({ action, why }, more || {});
  /** One line, read NOW (the new maps, its new spec), against the link it was read by before. See the header. */
  function classify(e, h) {
    const row0 = e && e.row;
    if (!row0 || !row0.spec || !row0.line) return res("skip", "the line is not read");
    if (FINISHED.has(row0.state) || (h.cancelled && h.cancelled(row0))) return res("skip", "the line is finished, cancelled or gone");
    // a line cut and committed is never read again by the page (Orders.interpretAll): it is read here, on the side, only to TELL a person; nothing of it is changed
    let row = row0; if (row0.state === "committed" && h.readNow) { let s = null; try { s = h.readNow(row0); } catch (_) { s = null; } if (s) row = Object.assign({}, row0, { spec: s }); }
    const sp = row.spec;
    let now = null; try { now = h.linkOf(row, h.maps); } catch (_) { now = null; }
    if (!now || !now.sku) return res("skip", "no saved link reads this line");
    const to = up(now.sku), link = { sku: to, key: now.key, via: now.via };
    if (e.was && up(e.was.sku) === to) return res("skip", "the link this line is read by did not change", { to, link });
    const sd = (a, b) => !!a && !!b && (up(a) === up(b) || (h.sameDesign ? !!h.sameDesign(a, b) : false));
    // the line must be READ by that link now (a person's per-line override, a pair, a set or a sign from the buyer's words is something else)
    if (!BY_LINK.has(sp.skuSource) || !sd(sp.designSku, to) || sp.noDesign || sp.ownDesign || sp.customDone) return res("skip", "the line is not read by the saved link", { to, link });
    if ((sp.problems || []).length) return res("skip", "the line still asks a question", { to, link });
    // a person's own pick for this very order (its Review resolved by a DIFFERENT charm) wins over the listing's
    const own = up(h.ownPick ? h.ownPick(row0) : ""); if (own && !sd(own, to)) return res("skip", `a person picked ${own} for this order`, { to, link });
    const from = up(e.from), made = h.made ? h.made(row0) : [];
    if (made == null) return res("skip", "its pieces are not read yet", { to, link, from, retry: true });
    if (!made.length) return from && !sd(from, to) ? res("design", "nothing is made yet: the line reads the new charm", { to, link, from }) : res("none", "the line already reads the picked charm", { to, link, from });
    if (made.some(s => !s)) return res("skip", "a piece's record names no design", { to, link, from });
    const was = [...new Set(made.map(up))].join(" + ");
    if (made.every(s => sd(s, to))) return res("none", "its pieces already carry the picked charm", { to, link, from: from || was });
    const fixed = (h.fixedOf ? h.fixedOf(row0) : []) || [];
    // (INPLACE, Paul 20 Oct 2026: a line in a committed set is no longer fixed by that alone: only a sheet the laser has CUT keeps its piece, as fixedOf says; the page's fixedOf also names a piece
    //  whose sheet this page does not hold. Without a fixedOf a committed line is told, as before.)
    if ((row0.state === "committed" && !h.fixedOf) || fixed.length) return res("flag", fixed.length ? fixed.map(f => `${f.why} on ${f.name}`).join(", ") : "its set is committed", { to, link, from: was, fixed, hold: row0.state !== "committed" && !row0.hold && fixed.some(f => f.local) });
    if (h.keepsPlace && h.keepsPlace(row0)) return res("kept", "the order is in Review: its piece stays on its sheet until the Review is resolved", { to, link, from: was });
    if (row0.hold) return res("skip", "the line is held by a person", { to, link, from: was });
    return res("swap", "its pieces are pooled or on an open sheet", { to, link, from: was });
  }

  return { changedListings, snapshot, classify, up };
});
