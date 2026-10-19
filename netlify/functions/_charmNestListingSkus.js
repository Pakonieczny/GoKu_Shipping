/*  netlify/functions/_charmNestListingSkus.js
 *  The SKU Etsy keeps for each product of a listing (Paul, 9 Oct: "a listing has SKUs attributed … the choice the user made in
 *  the drop-down menu will directly correlate to the charm", which Etsy stores as the SKU of that option). The receipt
 *  transaction carries that SKU already, and the sorter reads it first. This table is for the lines whose transaction came
 *  without it or with only the listing's own SKU, and for telling a SKU that belongs to one value of an option from one every
 *  value shares (charm-nest-orders.js inventorySku, tiesToOption).
 *
 *  Cost, exactly:
 *    · Etsy: one GET /listings/{id}/inventory for a listing the sorter asks about whose table is not stored or is older than
 *      7 days; never per order, never for a listing no line waits on. At most 6 a request and 60 a day for the whole shop
 *      (Charm_Listing_Skus/_budget). A rate-limited or failed call is not stored and not repeated in the same request.
 *      The sandbox asks Etsy the same way, for the same listings, from the same stored tables and the same 60 a day, and at most
 *      30 of those 60 (FLAGLOAD, 11 Oct 2026): it used to read the cache only, and nothing but a production page ever filled it,
 *      so a listing only the sandbox's own newest orders carried stayed "not loaded yet" for ever (Review rows: Zodiac Sign
 *      Scorpio, Cancer, Letter T). A table the sandbox stores is the shop's table: production finds it and asks Etsy nothing.
 *    · Firestore: one read per listing asked about (plus one for the day's budget), one write per listing fetched (plus one
 *      for the budget). The page asks only for listings with a line that waits on its SKU and keeps the answers for 7 days.
 *  A table is { at, n, uni } when every product of the listing has the one SKU (or none), else { at, n, products: [{ id,
 *  sku, d, pv: [[propertyId, valueId]…] }], names: { "propertyId:valueId": "Etsy's text for that value" }, props: { propertyId:
 *  "Etsy's name of the option" } }; { gone: true } for a listing Etsy no longer has. (Stored with each pair as one
 *  "propertyId:valueId" string, read back as pairs: Firestore refuses an array in an array. The names are stored as a list of
 *  "propertyId:valueId:text" strings and read back as the map.)
 *  The value names (ZODIACTWO, 10 Oct 2026) are what lets a SIGN the buyer wrote in a note ("balance et lion") be turned into the
 *  charm that value of the drop-down decides on this listing: only the values the orders bought were ever named before. They come
 *  with the same one GET that gives the SKUs, so a table stored before has none (`names` absent) and is asked again, once, only when
 *  the page says a line needs the names (nameIds); a table Etsy gave no names for is stored with names: {} and not asked again. */
"use strict";

const COLL = "Charm_Listing_Skus", BUDGET_ID = "_budget";
const REFRESH_MIN_MS = 10 * 60000;   // FALSEHOLD: a table the page found stale is asked again only when it was stored more than ten minutes ago (never in a loop)
const TTL_MS = 7 * 86400000, DAY_CAP = Infinity, CALL_CAP = 6, MAX_ASK = 25, MAX_PRODUCTS = 1500;   // NOLIMIT (Paul, 13 Oct 2026): no daily limit on listing look-ups (it was 60); CALL_CAP is only how many one request asks
const SANDBOX_CAP = DAY_CAP;   // SBCAP (Paul, 13 Oct 2026: "remove this limit for now"): the sandbox no longer has a share of its own (it was 30 of the day's 60, so production kept half); it may spend up to the whole day's DAY_CAP. To bring the share back, put 30 here.

const sku = v => String(v == null ? "" : v).trim().slice(0, 80);
const dayOf = ms => new Date(ms).toISOString().slice(0, 10);

/* Firestore refuses an array directly inside an array (the project's rule: "Nested arrays are not allowed"), and pv is a list of
   pairs. A table is therefore STORED with each pair as one "propertyId:valueId" string and READ BACK as pairs, so the page and
   every reader see the one shape the table always had; a table stored before (pairs, which Firestore happened to take) reads the
   same. Property and value ids are digits, so the colon never occurs inside one. */
const packPv = pv => (Array.isArray(pv) ? pv : []).map(a => (Array.isArray(a) ? String(a[0]) + ":" + String(a[1]) : String(a)));
const unpackPv = pv => (Array.isArray(pv) ? pv : []).map(a => { if (Array.isArray(a)) return [String(a[0]), String(a[1])]; const i = String(a).indexOf(":"); return [String(a).slice(0, i), String(a).slice(i + 1)]; });
const MAX_NAMES = 400, NAME_LEN = 60;
const nameText = v => String(v == null ? "" : v).replace(/\s+/g, " ").trim().slice(0, NAME_LEN);
const packNames = m => Object.keys(m || {}).map(k => k + ":" + nameText(m[k]));
const unpackNames = a => { const out = {}; for (const x of Array.isArray(a) ? a : []) { const s = String(x), i = s.indexOf(":"), j = i < 0 ? -1 : s.indexOf(":", i + 1); if (j > 0) out[s.slice(0, j)] = s.slice(j + 1); } return out; };
const packProps = m => Object.keys(m || {}).map(k => k + ":" + nameText(m[k]));
const unpackProps = a => { const out = {}; for (const x of Array.isArray(a) ? a : []) { const s = String(x), i = s.indexOf(":"); if (i > 0) out[s.slice(0, i)] = s.slice(i + 1); } return out; };
const toStored = t => {
  if (!(t && Array.isArray(t.products))) return t;
  const o = Object.assign({}, t, { products: t.products.map(p => Object.assign({}, p, { pv: packPv(p.pv) })) });
  delete o.names; delete o.props;
  if (t.names && typeof t.names === "object") { o.nm = packNames(t.names); o.pn = packProps(t.props); }
  return o;
};
const fromStored = t => {
  if (!(t && Array.isArray(t.products))) return t;
  const o = Object.assign({}, t, { products: t.products.map(p => Object.assign({}, p, { pv: unpackPv(p.pv) })) });
  if (Array.isArray(t.nm)) { o.names = unpackNames(t.nm); o.props = unpackProps(t.pn); }
  delete o.nm; delete o.pn;
  return o;
};

/** Etsy's inventory of one listing → the compact table above. */
function compact(inv, now) {
  const list = Array.isArray(inv && inv.products) ? inv.products : [];
  const products = list.slice(0, MAX_PRODUCTS).map(p => ({
    id: String(p && p.product_id != null ? p.product_id : ""),
    sku: sku(p && p.sku),
    d: p && p.is_deleted ? 1 : 0,
    pv: (Array.isArray(p && p.property_values) ? p.property_values : []).flatMap(x => (Array.isArray(x && x.value_ids) ? x.value_ids : []).map(v => [String(x.property_id), String(v)]))
  }));
  const live = products.filter(p => !p.d), skus = new Set(live.map(p => p.sku.toUpperCase()));
  const base = { at: now, n: products.length };
  // one SKU for every live product, or none at all: nothing in the table tells one option's value from another's
  if (skus.size <= 1) return Object.assign(base, { uni: live.length ? live[0].sku : "" });
  // Etsy's own text for each value of each option (values[i] goes with value_ids[i]) and its name of the option: what a sign named in words is looked up by
  const names = {}, props = {};
  for (const p of list.slice(0, MAX_PRODUCTS)) for (const x of (Array.isArray(p && p.property_values) ? p.property_values : [])) {
    const pid = String(x && x.property_id != null ? x.property_id : ""), ids = Array.isArray(x && x.value_ids) ? x.value_ids : [], texts = Array.isArray(x && x.values) ? x.values : [];
    if (!pid) continue;
    if (x.property_name && !props[pid]) props[pid] = nameText(x.property_name);
    ids.forEach((v, i) => { const k = pid + ":" + String(v), t = nameText(texts[i]); if (t && !names[k] && Object.keys(names).length < MAX_NAMES) names[k] = t; });
  }
  return Object.assign(base, { products, names, props });
}

/**
 * ids: listing ids asked for, most wanted first. env: { db, fetchInventory(id) → Etsy's inventory, now?, cacheOnly?, sandbox? }.
 * → { tables: { id: table }, pending: [id], why, etsyCalls, cap: { used, max }, sandboxCap?: { used, max } }. A listing with a
 * stored table older than the ttl is answered with that table if Etsy cannot be asked now (it is also listed in `pending`).
 * Etsy is asked in this order: listings with no table at all, then those whose table lacks the value names a line needs, then
 * those whose table is a week old (inside each group, the order the page gave). why says why some are pending:
 * "batch" (this request's 6 are used: ask again soon), "day" (the day's 60 are used), "sandbox" (the sandbox's 30 of the
 * day's 60 are used), "error" (Etsy refused or failed), "cache" (a request that asked for no Etsy call).
 */
async function lookup(ids, env) {
  const db = env.db, now = env.now || Date.now(), coll = db.collection(COLL);
  const want = [...new Set((ids || []).map(x => String(x == null ? "" : x).trim()).filter(x => /^\d{3,20}$/.test(x)))].slice(0, MAX_ASK);
  // listings the page needs Etsy's value names for (nameIds): a stored table of options with no names is asked again for them, once
  const needNames = new Set((env.nameIds || []).map(x => String(x == null ? "" : x).trim()));
  // FALSEHOLD (Paul, 19 Oct 2026, order 4207131438): listings the page found STALE (refreshIds: a line's own SKU names a design while the stored table says one other SKU for every choice; Etsy's
  // inventory changed after it was stored): a stored table older than REFRESH_MIN_MS is asked of Etsy again, once (the page asks once per listing per page), within the same caps
  const refresh = new Set((env.refreshIds || []).map(x => String(x == null ? "" : x).trim()));
  const out = { tables: {}, pending: [], why: null, etsyCalls: 0, cap: { used: 0, max: DAY_CAP } };
  if (!want.length) return out;
  const snaps = await db.getAll(...want.map(id => coll.doc(id)), coll.doc(BUDGET_ID));
  const bd = snaps[want.length].exists ? snaps[want.length].data() || {} : {};
  const today = bd.day === dayOf(now), sandbox = !!env.sandbox;
  let used = today ? Math.max(0, +bd.n || 0) : 0, usedSb = today ? Math.max(0, +bd.sb || 0) : 0;   // (sb: how many of the day's calls the sandbox made)
  const stale = [];
  want.forEach((id, i) => {
    const s = snaps[i], t = s.exists ? s.data() : null;
    const noNames = !!t && needNames.has(id) && Array.isArray(t.products) && !Array.isArray(t.nm);   // (stored before the names were kept)
    const again = !!t && refresh.has(id) && now - (+t.at || 0) >= REFRESH_MIN_MS;   // (FALSEHOLD: a stale table the page asks to refresh)
    if (t && now - (+t.at || 0) < TTL_MS && !noNames && !again) out.tables[id] = fromStored(t);
    else { if (t) out.tables[id] = fromStored(t); stale.push({ id, rank: !t ? 0 : noNames || again ? 1 : 2 }); }
  });
  stale.sort((a, b) => a.rank - b.rank);   // (stable: inside a group, the page's order, oldest waiting order first)
  let attempts = 0, stop = !!env.cacheOnly;
  if (stop && stale.length) out.why = "cache";
  for (const { id } of stale) {
    const dayFull = used >= DAY_CAP, sbFull = sandbox && usedSb >= SANDBOX_CAP;
    if (stop || attempts >= CALL_CAP || dayFull || sbFull) { out.pending.push(id); if (!out.why) out.why = dayFull ? "day" : sbFull ? "sandbox" : "batch"; continue; }
    attempts++; used++; if (sandbox) usedSb++;
    try {
      const table = compact(await env.fetchInventory(id), now);
      await coll.doc(id).set(toStored(table));
      out.tables[id] = table;
    } catch (e) {
      const st = +(e && e.status) || 0;
      // a listing Etsy no longer has is kept as such, so it is not asked for again for a week; anything else (a limit,
      // the network, a server fault) is not stored and ends this request's asking
      if (st === 404 || st === 410) { const table = { at: now, n: 0, gone: true }; await coll.doc(id).set(table); out.tables[id] = table; }
      else { stop = true; out.pending.push(id); out.why = "error"; }
    }
  }
  out.etsyCalls = attempts;
  out.cap = { used, max: DAY_CAP };
  if (sandbox) out.sandboxCap = { used: usedSb, max: SANDBOX_CAP };
  if (attempts) await coll.doc(BUDGET_ID).set({ day: dayOf(now), n: used, sb: usedSb, at: now });
  return out;
}

module.exports = { lookup, compact, toStored, fromStored, packNames, unpackNames, COLL, TTL_MS, REFRESH_MIN_MS, DAY_CAP, CALL_CAP, SANDBOX_CAP, MAX_ASK };
