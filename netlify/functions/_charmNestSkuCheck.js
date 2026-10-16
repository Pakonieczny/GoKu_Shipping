/*  netlify/functions/_charmNestSkuCheck.js
 *  customSkuCheck (SKUGEN, 11 Oct 2026; plans/save-to-repo-1011/CONTRACT.md sections 3 and 7): of the SKUs the sorter's preview
 *  proposes for a custom design saved to the charm repository (charm-nest-sku.js), which is the first one nobody holds?
 *
 *  READS ONLY. No write, no Etsy call, no scan: for at most 9 candidates it reads (getAll, by document id) the candidates' own master
 *  index documents (field mask: sku only), the listing's alias document (Charm_Sku_Aliases/{listingId}; in the sandbox its own copy
 *  too) and the listing's cached inventory table (Charm_Listing_Skus/{listingId}, written by listingSkus; this op never asks Etsy).
 *  That is at most 12 small reads. The look-alike twins of the master (FOXHEAD4821 and FOX_HEAD_4821) can only be found in the page,
 *  which holds the whole index; the master document ids are what the cloud can check without reading the index.
 *
 *  The rule is the page's: this file requires charm-nest-sku.js by a literal path (as _stationLive.js does with charm-nest-orders.js),
 *  so a candidate the page would never make (a reserved or malformed SKU) is not free here either.
 *
 *  body: { listingId: digits, candidates: ["ACORN_LEAF_4821", …], sandbox? }
 *  → { ok: true, free: "ACORN_LEAF_4821" | "", taken: [{ sku, by: "master" | "alias" | "listing" | "rule", detail }],
 *      checked: { master: true, alias: true, listing: "cache" | "none" } }
 *  A malformed request (no listing id, no candidates, more than 9) is { error } and HTTP 400, as every op of charmNestLibrary answers
 *  it; anything the request is allowed to ask is HTTP 200, a taken candidate included. `by: "rule"` is a candidate that is reserved or
 *  has characters the rule never makes (the page does not send one; a page older than this rule might).
 */
"use strict";
const Sku = require("../../charm-nest-sku.js");

const INDEX = "Charm_Master_Index", ALIASES = "Charm_Sku_Aliases", LISTING_SKUS = "Charm_Listing_Skus", SANDBOX = "Sandbox_";
const MAX_CANDIDATES = 9;

async function check(db, body) {
  const b = body && typeof body === "object" ? body : {};
  const lid = String(b.listingId == null ? "" : b.listingId).trim();
  if (!/^\d{3,20}$/.test(lid)) return { error: "listingId must be the listing's digits" };
  if (!Array.isArray(b.candidates) || !b.candidates.length) return { error: "candidates required: the SKUs to check, first choice first" };
  if (b.candidates.length > MAX_CANDIDATES) return { error: `at most ${MAX_CANDIDATES} candidates` };
  const sandbox = b.sandbox === true || b.sandbox === 1 || b.sandbox === "1";

  // the candidates, in the order given, upper case, each once
  const list = [], seen = new Set();
  for (const c of b.candidates) { const s = typeof c === "string" ? c.trim().toUpperCase() : ""; if (!seen.has(s)) { seen.add(s); list.push(s); } }
  const taken = [], refused = {};   // (candidate → why the rule refuses it; candidates are upper case, so no key meets one of Object's own)
  const ask = [];   // the candidates the rule allows: only these become a document id
  for (const s of list) { const why = Sku.reservedWhy(s); if (why) refused[s] = { sku: s, by: "rule", detail: why }; else ask.push(s); }

  const aliasRefs = [db.collection(ALIASES).doc(lid)];
  if (sandbox) aliasRefs.push(db.collection(SANDBOX + ALIASES).doc(lid));
  const [masters, rest] = await Promise.all([
    ask.length ? db.getAll(...ask.map(s => db.collection(INDEX).doc(s)), { fieldMask: ["sku"] }) : [],
    db.getAll(...aliasRefs, db.collection(LISTING_SKUS).doc(lid))
  ]);
  const heldByMaster = new Set(); ask.forEach((s, i) => { if (masters[i] && masters[i].exists) heldByMaster.add(s); });
  const aliasDocs = {}; aliasRefs.forEach((r, i) => { if (rest[i] && rest[i].exists) aliasDocs[i ? lid + "~sandbox" : lid] = rest[i].data() || {}; });
  const aliasKeys = Sku.aliasIndex(aliasDocs);
  const table = rest[aliasRefs.length] && rest[aliasRefs.length].exists ? rest[aliasRefs.length].data() || null : null;
  const tableKeys = Sku.tableSkus(table), loose = Sku.looseKey;

  let free = "";
  const judge = (s, heldMaster) => {
    let hit = refused[s];
    if (!hit && heldMaster.has(s)) hit = { sku: s, by: "master", detail: "already a master design" };
    if (!hit && s && aliasKeys.has(loose(s))) hit = { sku: s, by: "alias", detail: aliasKeys.get(loose(s)) };
    if (!hit && s && tableKeys.has(loose(s))) hit = { sku: s, by: "listing", detail: tableKeys.get(loose(s)) };
    return hit;
  };
  for (const s of list) { const hit = judge(s, heldByMaster); if (hit) taken.push(hit); else if (!free) free = s; }
  // KEEP B2 (K8): "taken by another design" always ends: when every candidate the page knew is taken, the order goes on with the next letters (NAME_TAIL6_G ... _Z), checked the same way
  let extended = false;
  if (!free) {
    const more = extension(list).filter(s => !seen.has(s));
    const rule = {}; const ask2 = [];
    for (const s of more) { const why = Sku.reservedWhy(s); if (why) rule[s] = { sku: s, by: "rule", detail: why }; else ask2.push(s); }
    const docs2 = ask2.length ? await db.getAll(...ask2.map(s => db.collection(INDEX).doc(s)), { fieldMask: ["sku"] }) : [];
    const held2 = new Set(); ask2.forEach((s, i) => { if (docs2[i] && docs2[i].exists) held2.add(s); });
    for (const s of more) {
      const hit = rule[s] || judge(s, held2);
      if (hit) taken.push(hit); else { free = s; extended = true; break; }
    }
  }
  return Object.assign({ ok: true, free, taken, checked: { master: true, alias: true, listing: table && !table.gone ? "cache" : "none" } }, extended ? { extended: true } : {});
}
/** The letters after the last candidate given: HEART_364821_C gives HEART_364821_D ... _Z; a candidate with no letter gives _A ... _Z. At most 32 characters. */
function extension(list) {
  const last = list.length ? list[list.length - 1] : "";
  if (!last) return [];
  const m = /^(.*\d)_([A-Z])$/.exec(last), stem = m ? m[1] : last, from = m ? m[2].charCodeAt(0) + 1 : 65, out = [];
  for (let c = from; c <= 90; c++) { const s = stem + "_" + String.fromCharCode(c); if (s.length <= 32) out.push(s); }
  return out;
}

module.exports = { check, Sku, MAX_CANDIDATES };
