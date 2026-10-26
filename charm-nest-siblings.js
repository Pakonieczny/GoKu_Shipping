/*  charm-nest-siblings.js — "this listing's SKU names no design, but the library has its numbered pair" (SIBLINGS, 12 Oct 2026).
 *  ═══════════════════════════════════════════════════════════════════════
 *  Paul, 12 Oct 2026, order 4198409468 (Etsy SKU "SCHRODINGER CAT", a necklace whose listing photo shows TWO cats, one plain and one with a skull; the master
 *  holds "SCHRODINGER CAT 1" and "SCHRODINGER CAT 2" and a huggie-sized "SCHRODINGER CAT"): "This should be an obvious one ... Associate this etsy listing
 *  with both of the charms ... in the most effective and long lasting approach."
 *
 *  WHAT THIS IS  A pure, deterministic reader (no DOM, no network, no clock, no Etsy, no writes). Given a line whose own SKU has no usable design and the
 *  master's SKU list, it says whether the Review row should offer ONE quiet tap, "Use both: X 1 + X 2", and which single designs to offer beside it.
 *  It only SUGGESTS. A number after a name is often an ALTERNATIVE, not a member of a set (DRAGON 1 to DRAGON 11 are eleven dragons; a necklace never
 *  wears all of them), so nothing here ever picks a design for a line: a person taps, and the answer is saved for good by the page's own saved pair
 *  link (charmNestLibrary aliasPut with pair, kept per listing and per transaction SKU under Charm_Sku_Aliases/{listingId}.pairBySku).
 *
 *  WHERE THE CANDIDATES COME FROM  The form guard (charm-nest-orders.js, HUGGIEGUARD, plans/schro-1012/GUARD-API.md) already finds the numbered designs of a SKU and keeps only those
 *  that fit the line's form: spec.siblings / problem.siblings ([{ sku, n, form, why }], a necklace line never lists a "(HUGGIE)" design). This module reads THAT list and does not decide forms
 *  again. Only a caller that has no list (an older read) gets the plain scan below (scan: the master's SKUs named like the line's SKU plus a number, "(HUGGIE)" designs left out).
 *  WHEN IT ANSWERS (all must hold, else null):
 *   · the line has a SKU, and there are at least TWO numbered designs (whole numbers from 1, one per number) of it.
 *   · the line is a NECKLACE (the guard's spec.lineForm "necklace"; else what the Review row's "Jewellery" fact says, a Necklace Length option, or the line's own words) and nothing else: a bracelet, an anklet or a
 *     charm sold alone is not offered it (coordinator, 12 Oct 2026: "only on a necklace-form line"). An earring or huggie line is never offered it (a pair of earrings has its own Left and Right
 *     answers, "Which design goes on the other ear?").
 *   · the numbered designs are all of ONE form: never a "(HUGGIE)" or earring design beside a plain one (the guard's own list already holds only the line's form; a design it marks huggie or earring is left out here as well).
 *     "GRAD CAP 1" whose master has only "GRAD CAP 1 (HUGGIE)" and "GRAD CAP 2" names no numbered family of its own (GRAD CAP 1 1, GRAD CAP 1 2 do not exist): no offer, the hold stands.
 *  WHAT IT OFFERS: the two lowest numbers as the pair (the Left is the lower number only because a saved pair keeps two names in order: a necklace has no Left and Right; "X-1" and "X_3"
 *  are the shop's INDEXED families, where the option picks the member, and never make a pair), every numbered design as a single choice, and plain words. Three or more numbered
 *  designs: the saved link holds exactly TWO distinct designs per listing and SKU, so the pair is offered and the rest are listed as single choices (never "all N").
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory(function () { return require("./charm-nest-orders.js"); });
  else root.CharmNestSiblings = factory(function () { return root.CharmNestOrders; });
})(typeof self !== "undefined" ? self : this, function (ordersOf) {
  "use strict";

  let loaded = null;
  /** The orders module (charm-nest-orders.js), found when first needed so a page may load the two scripts in either order. */
  const O = () => loaded || (loaded = ordersOf());
  const decode = s => { const t = s == null ? "" : String(s), o = O(); return o && o.decodeEntities ? o.decodeEntities(t) : t; };
  const up = s => decode(s).replace(/\s+/g, " ").trim().toUpperCase();   // (the way charm-nest-orders.js upSku reads a SKU, with its spacing made single)
  const loose = s => String(s).replace(/[^A-Z0-9]+/g, "");
  const NUMBERED = /^(.*\S)\s+([1-9]\d?)$/;                              // "SCHRODINGER CAT 1": a name, one space or more, a whole number 1 to 99
  const HUGGIE_TAIL = /\s*\(\s*HUGGIE\s*\)\s*$/;
  const TWO_CHARMS = /\b(?:dual|double|duo|twin|two|2)[\s-]*(?:piece\s*)?charms?\b|\bcharms?\s*(?:duo|pair|set\s+of\s+(?:two|2))\b|\b(?:set|pair)\s+of\s+(?:two|2)\s+charms?\b|\b2\s*x\s*charms?\b/i;
  const CHAIN_OPTION = /\b(?:necklace|chain)\s*(?:length|size)\b/i;
  const HUGGIE_WORDS = /\bhuggies?\b|\bhuggie\s+hoops?\b/i;

  /** The words a line says about its own kind, from what the buyer chose and its title. */
  function wordsOf(line) {
    const o = O(), opts = o && o.purchaseOptions ? o.purchaseOptions(line || {}, {}).map(v => v.name + " " + v.value) : (Array.isArray(line && line.variations) ? line.variations.map(v => (v && v.name || "") + " " + (v && v.value || "")) : []);
    return [String(line && line.title || "")].concat(opts).join(" · ");
  }
  /** What kind of line this is for the suggestion: { ok, family, why }. ok is true for a necklace (what the Review row's "Jewellery" fact says, else the line's form, else its own words),
   *  or for a bracelet, an anklet or a charm sold alone whose own words say two charms. spec: the line's read (charm-nest-orders.js interpretLine), when the caller has it. */
  function lineKind(line, spec) {
    const o = O(), words = wordsOf(line), fam = o && o.lineFamily ? o.lineFamily(line || {}) : "", form = String(spec && spec.form || "").toLowerCase();
    let det = ""; try { det = o && o.purchaseDetails ? String(o.purchaseDetails(line || {}, spec || {}).type || "") : ""; } catch (_) { det = ""; }
    const lf = String(spec && spec.lineForm || "").toLowerCase();   // (the form guard's reading of the line: huggie | earring | necklace | unknown; it is not worked out again here)
    if (lf === "huggie" || lf === "earring") return { ok: false, family: "earring", why: "an earring piece has its own Left and Right answers" };
    if (lf === "necklace" && !/^charm only/i.test(det)) return { ok: true, family: "necklace", says: TWO_CHARMS.test(words), why: "a necklace" };
    const earrings = /earring|hoop|huggie|stud/i.test(det) || form === "earrings" || form === "huggie" || form === "earring-single" || fam === "earring" || (HUGGIE_WORDS.test(words) && fam !== "necklace" && det !== "Necklace");
    if (earrings) return { ok: false, family: "earring", why: "an earring piece has its own Left and Right answers" };
    const says = TWO_CHARMS.test(words), chain = (Array.isArray(line && line.variations) ? line.variations : []).some(v => v && CHAIN_OPTION.test(String(v.name != null ? v.name : v.formatted_name || "")) && String(v.value != null ? v.value : v.formatted_value || "").trim());   // (a Necklace Length option is a necklace: the buyer chose a chain)
    if (chain && !/^charm only/i.test(det)) return { ok: true, family: "necklace", says, why: "a necklace (it has a chain length)" };
    if (det === "Necklace" || (!/^charm only/i.test(det) && (form === "necklace" || fam === "necklace"))) return { ok: true, family: "necklace", says, why: "a necklace" };
    const other = /bracelet/i.test(det) || form === "bracelet" || fam === "bracelet" ? "bracelet" : /anklet/i.test(det) || form === "anklet" || fam === "anklet" ? "anklet" : "";
    return { ok: false, family: other || fam || "", why: other ? "a " + other + " is not a necklace" : "a piece that is not known to be a necklace" };
  }

  /** The numbered designs of `base` found in a list of master SKUs: [{ sku, n }] in number order, one per number. A "(HUGGIE)" design (unless `o.huggie`), a design of another family
   *  and a design the caller says is blocked are left out. base: the SKU as a line carries it. Only the fallback: the guard's own list is read first (suggest). */
  function scan(base, skus, o) {
    o = o || {}; const b = loose(up(base).replace(HUGGIE_TAIL, "")), out = new Map();
    if (!b) return [];
    const oo = O();
    // (with `o.index` the master is not walked: the numbered designs of every name were listed once, index(); a page with thousands of SKUs asks for each of its unknown-SKU lines)
    const cand = o.index ? (o.index.get(b) || []).map(x => x.sku) : skus || [];
    for (const k of cand) {
      const sku = up(k); if (!sku || (!o.huggie && HUGGIE_TAIL.test(sku))) continue;
      const m = NUMBERED.exec(sku.replace(HUGGIE_TAIL, "")); if (!m || loose(m[1]) !== b) continue;
      if (o.blocked && o.blocked(sku)) continue;
      if (o.line && oo && oo.skuFamilyConflict && oo.skuFamilyConflict(sku, o.line)) continue;
      const n = +m[2]; if (!out.has(n)) out.set(n, { sku, n });   // (the master keeps every SKU in capitals with single spaces: the SKU as the master spells it)
    }
    return [...out.values()].sort((a, c) => a.n - c.n);
  }
  /** The master's numbered designs by name: Map(name without spaces or marks → [{ sku, n }]), "(HUGGIE)" designs left out. One walk of the list; scan() then looks a name up. */
  function index(skus) {
    const m = new Map();
    for (const k of skus || []) {
      const sku = up(k); if (!sku || HUGGIE_TAIL.test(sku)) continue;
      const x = NUMBERED.exec(sku); if (!x) continue;
      const b = loose(x[1]), a = m.get(b); if (a) a.push({ sku, n: +x[2] }); else m.set(b, [{ sku, n: +x[2] }]);
    }
    return m;
  }
  const INDEXED = /[-_]\d{1,3}\s*$/;
  /** The list the guard (or the scan) gave, as [{ sku, n }]: one per whole number from 1, in number order. An entry may be a SKU string or { sku, n }. */
  function numbered(list) {
    const out = new Map();
    for (const x of list || []) {
      const sku = up(x && typeof x === "object" ? x.sku : x); if (!sku) continue;
      let n = x && typeof x === "object" && Number.isFinite(+x.n) ? Math.floor(+x.n) : NaN;
      if (!(n >= 1)) { const m = /(\d{1,3})\s*$/.exec(sku.replace(HUGGIE_TAIL, "")); n = m ? +m[1] : NaN; }
      if (!(n >= 1) || out.has(n)) continue;
      out.set(n, { sku, n });
    }
    return [...out.values()].sort((a, c) => a.n - c.n);
  }

  /** The suggestion for a line, or null. o: { spec?: the line's read (charm-nest-orders.js interpretLine), siblings?: the guard's list (spec.siblings / problem.siblings), skus?: the master's
   *  SKUs (any iterable, for the scan when no list was given), index?: index(skus), the same, listed once, blocked?: sku → bool, usable?: sku → bool (the line's own SKU has a design the page accepts: nothing is suggested) }.
   *  → { base, form, noun, siblings: [{ sku, n }], pair: { L, R }, rest: [{ sku, n }], numbers: [n], ask, why } */
  function suggest(line, o) {
    o = o || {}; if (!line) return null;
    const spec = o.spec || null, base = up(o.base || line.sku); if (!base) return null;
    if (typeof o.usable === "function" && o.usable(base)) return null;
    const kind = lineKind(line, spec); if (!kind.ok) return null;
    const given = Array.isArray(o.siblings) ? o.siblings : Array.isArray(spec && spec.siblings) ? spec.siblings : null;
    // (one form only: a design the guard marks huggie or earring, or one named "(HUGGIE)", is never offered to a necklace, beside a plain one or alone)
    const sameForm = x => !(x && typeof x === "object" && /^(?:huggie|earring)$/i.test(String(x.form || ""))) && !HUGGIE_TAIL.test(up(x && typeof x === "object" ? x.sku : x));
    const all = numbered(given ? given.filter(sameForm) : scan(base, o.index ? null : Array.isArray(o.skus) ? o.skus : [...(o.skus || [])], { line, blocked: o.blocked, index: o.index }));
    const siblings = all.filter(x => !(o.blocked && o.blocked(x.sku)));
    const pool = siblings.filter(x => !INDEXED.test(x.sku)); if (pool.length < 2) return null;
    const pair = { L: pool[0].sku, R: pool[1].sku }, rest = siblings.filter(x => x.sku !== pair.L && x.sku !== pair.R), numbers = siblings.map(s => s.n);
    const noun = "necklace";
    const nums = numbers.length === 2 ? `a ${numbers[0]} and a ${numbers[1]}` : `numbers ${numbers.slice(0, -1).join(", ")} and ${numbers[numbers.length - 1]}`;
    const ask = numbers.length === 2 ? `${base} has ${nums} in the library. Is this ${noun} both charms?` : `${base} has ${nums} in the library. Is this ${noun} two of them (${pool[0].n} and ${pool[1].n})?`;
    return { base, form: kind.family, noun, siblings, pair, rest, numbers, ask, why: `the SKU names no design, and the library holds ${base} ${numbers.join(", ")}`, says: !!kind.says };
  }

  /* ═══ SIMILAR CHARMS for ANY order in Review (REVSUGGEST, 12 Oct 2026) ═══════════════════════════════════════════════════
     Paul, 12 Oct 2026: "every time an order goes into the review tab ... the system should identify and present to the user multiple options that match the current SKU ... If there are no similar matches
     for charm SKU then don't show any suggestive charms." The numbered designs above only answer a necklace whose SKU is the base of "X 1", "X 2". This is the same kind of list for every other line, matched or not
     (an order a person sends to Review because its charm looks wrong is matched already): the library's charms that read like the line's own SKU, composed from what the page already has and nothing new:
       · the master SKUs that read the same with spacing and punctuation set aside (Master.likeFor, LOOKALIKE: "CELESTIAL3-COMET" and "CELESTIAL 3 - COMET"),
       · the master SKUs named exactly like the SKU, or whose name holds ALL its words (CharmNestOrders.suggestCharms, the list Review's option cards already offer),
       · not a blocked design, not another FORM's design (the form guard, formOk: a necklace never sees a huggie design), one entry for a drawing that has several names (a person has nothing to choose),
       · at most `max` (8) are listed, the rest only counted ("and N more"); a name that fits more than 12 designs is a word, not a design: only the exact and the same-reading ones stay.
     It only SUGGESTS: nothing here picks a design, writes, reads the cloud, calls Etsy or an AI; the caller's tap is the person's pick (the saved link, aliasPut). A line with no SKU, or one whose SKU is the shop's
     shared charm-only placeholder (genericSku), has nothing to be similar to: no list (never guessed from the title: Paul, 12 Oct 2026, "no exact match = no match").
     o: { skus: the master's SKUs (any iterable), like?: sku → master SKUs that read the same, blocked?: sku → bool, formOk?: sku → bool, entryFor?: sku → master entry (one entry per drawing),
          current?: the master SKUs the line is matched to now (marked, listed first), exclude?: SKUs another offer of the same row already lists, max?, base?: the SKU to read (default the line's) }
     → null (nothing similar to choose from) | { base, choices: [{ sku, why, current }], extra: n (similar ones not listed) } */
  const SIMILAR_MAX = 8, WORD_FITS = 12;
  function similar(line, o) {
    o = o || {}; if (!line) return null;
    const oo = O(); if (!oo || typeof oo.suggestCharms !== "function") return null;
    const raw = up(o.base != null ? o.base : line.sku); if (!raw) return null;
    if (typeof oo.genericSku === "function" && oo.genericSku(raw)) return null;
    const base = (typeof oo.variationBase === "function" && oo.variationBase(raw)) || raw, max = o.max > 0 ? Math.floor(o.max) : SIMILAR_MAX;
    const cur = [...new Set((o.current || []).map(up).filter(Boolean))], gone = new Set((o.exclude || []).map(up));
    const ok = k => !(typeof o.blocked === "function" && o.blocked(k)) && !(typeof o.formOk === "function" && !o.formOk(k));
    const same = (a, b) => { try { return typeof o.entryFor === "function" && oo.sameDesign ? oo.sameDesign(a, b, o.entryFor) : a === b; } catch (_) { return a === b; } };
    const found = [], seen = new Set();
    const add = (sku, why, rank) => { const k = up(sku); if (!k || seen.has(k) || gone.has(k)) return; seen.add(k); if (cur.includes(k) || ok(k)) found.push({ sku: k, why, rank }); };
    for (const k of typeof o.like === "function" ? o.like(base) || [] : []) add(k, "reads the same without its spacing and punctuation", 0);
    let named = []; try { named = oo.suggestCharms({ value: base, line, skus: o.skus || [], max: 400, lead: false }) || []; } catch (_) { named = []; }
    const words = named.filter(x => !/^named exactly/.test(x.why || ""));
    const keepWords = words.length <= WORD_FITS;
    for (const x of named) { const exact = /^named exactly/.test(x.why || ""); if (exact || keepWords) add(x.sku, exact ? "named like the SKU" : "its name holds all the words of the SKU", exact ? 1 : 2); }
    // one entry for a drawing that carries several names: the line's own current name first, else the best ranked
    const out = [];
    for (const x of found) { const twin = out.findIndex(y => same(x.sku, y.sku)); if (twin < 0) out.push(x); else if (cur.includes(x.sku) && !cur.includes(out[twin].sku)) out[twin] = x; }
    const others = out.filter(x => !cur.includes(x.sku));
    if (!others.length) return null;
    const mine = cur.map(k => out.find(x => x.sku === k) || { sku: k, why: "matched now", rank: -1 }).filter(x => !others.includes(x) && !gone.has(x.sku));
    const shown = others.slice(0, max);
    return { base, choices: mine.map(x => ({ sku: x.sku, why: x.why, current: true })).concat(shown.map(x => ({ sku: x.sku, why: x.why, current: false }))), extra: others.length - shown.length };
  }

  return { suggest, scan, index, numbered, lineKind, similar, NUMBERED, TWO_CHARMS };
});
