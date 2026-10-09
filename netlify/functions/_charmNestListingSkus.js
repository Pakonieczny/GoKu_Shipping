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
 *      (Charm_Listing_Skus/_budget). A rate-limited or failed call is not stored and not repeated in the same request. The
 *      sandbox never calls Etsy: it reads what production stored.
 *    · Firestore: one read per listing asked about (plus one for the day's budget), one write per listing fetched (plus one
 *      for the budget). The page asks only for listings with a line that waits on its SKU and keeps the answers for 7 days.
 *  A table is { at, n, uni } when every product of the listing has the one SKU (or none), else { at, n, products: [{ id,
 *  sku, d, pv: [[propertyId, valueId]…] }] }; { gone: true } for a listing Etsy no longer has. */
"use strict";

const COLL = "Charm_Listing_Skus", BUDGET_ID = "_budget";
const TTL_MS = 7 * 86400000, DAY_CAP = 60, CALL_CAP = 6, MAX_ASK = 25, MAX_PRODUCTS = 1500;

const sku = v => String(v == null ? "" : v).trim().slice(0, 80);
const dayOf = ms => new Date(ms).toISOString().slice(0, 10);

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
  return Object.assign(base, { products });
}

/**
 * ids: listing ids asked for. env: { db, fetchInventory(id) → Etsy's inventory, now?, cacheOnly? }.
 * → { tables: { id: table }, pending: [id], why, etsyCalls, cap: { used, max } }. A listing with a stored table older than the
 * ttl is answered with that table if Etsy cannot be asked now (it is also listed in `pending`). why says why some are pending:
 * "batch" (this request's 6 are used: ask again soon), "day" (the day's 60 are used), "error" (Etsy refused or failed),
 * "cache" (the sandbox, or a request that asked for no Etsy call).
 */
async function lookup(ids, env) {
  const db = env.db, now = env.now || Date.now(), coll = db.collection(COLL);
  const want = [...new Set((ids || []).map(x => String(x == null ? "" : x).trim()).filter(x => /^\d{3,20}$/.test(x)))].slice(0, MAX_ASK);
  const out = { tables: {}, pending: [], why: null, etsyCalls: 0, cap: { used: 0, max: DAY_CAP } };
  if (!want.length) return out;
  const snaps = await db.getAll(...want.map(id => coll.doc(id)), coll.doc(BUDGET_ID));
  const bd = snaps[want.length].exists ? snaps[want.length].data() || {} : {};
  let used = bd.day === dayOf(now) ? Math.max(0, +bd.n || 0) : 0;
  const stale = [];
  want.forEach((id, i) => {
    const s = snaps[i], t = s.exists ? s.data() : null;
    if (t && now - (+t.at || 0) < TTL_MS) out.tables[id] = t;
    else { if (t) out.tables[id] = t; stale.push(id); }
  });
  let attempts = 0, stop = !!env.cacheOnly;
  if (stop && stale.length) out.why = "cache";
  for (const id of stale) {
    if (stop || attempts >= CALL_CAP || used >= DAY_CAP) { out.pending.push(id); if (!out.why) out.why = used >= DAY_CAP ? "day" : "batch"; continue; }
    attempts++; used++;
    try {
      const table = compact(await env.fetchInventory(id), now);
      await coll.doc(id).set(table);
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
  if (attempts) await coll.doc(BUDGET_ID).set({ day: dayOf(now), n: used, at: now });
  return out;
}

module.exports = { lookup, compact, COLL, TTL_MS, DAY_CAP, CALL_CAP, MAX_ASK };
