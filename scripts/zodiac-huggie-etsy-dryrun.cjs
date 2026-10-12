#!/usr/bin/env node
/* DRY RUN ONLY (ZODETSY, 12 Oct 2026). Builds, offline, the inventory the huggie hoop "Astrological Charm" listing would carry once it has a
 * ZODIAC SIGN drop-down, checks it, and builds the matching restore payload. It reads JSON files and prints or writes local files.
 * It has NO network code at all (no fetch, no Etsy, no Firestore, no token), and the app has no button or operation that calls it.
 *
 *   node scripts/zodiac-huggie-etsy-dryrun.cjs --selftest
 *   node scripts/zodiac-huggie-etsy-dryrun.cjs --print-fixture                       the documented fixture (Etsy's getListingInventory shape)
 *   node scripts/zodiac-huggie-etsy-dryrun.cjs --inventory <get.json> [--structure three|two] [--master <skus.json>] [--out <dir>]
 *   node scripts/zodiac-huggie-etsy-dryrun.cjs --restore-from <backup-get.json> [--out <dir>]
 *
 *   --inventory     the answer of GET /v3/application/listings/{id}/inventory, kept in a file (the backup taken before any write is exactly this)
 *   --structure     three (default): a third drop-down ZODIAC SIGN next to METAL CHOICE and HOOP SIZE (Etsy's third variation, PUT needs
 *                   ?max_variations_supported=3); two: the fallback if Etsy refuses a third variation: metal and hoop size merged into ONE drop-down
 *   --master        a JSON list of master SKUs (strings or {sku}), to prove the seven signs exist in the repo and the five new SKUs exist nowhere
 *   --out           a directory to write payload.json, restore.json and report.txt into (nothing is written without it)
 *
 * What a payload is: { query, body } = the query string and JSON body of ONE `PUT /v3/application/listings/{listing_id}/inventory`. This file never sends it.
 * The money rules, the HTML-unescaping and the product shape are the app's own (netlify/functions/_charmNestEtsySku.js sanitizeProduct, the same code the ETSYSKU
 * write was proven with on fakes; _etsyInventoryCanonical.js toDecimalPrice), so the numbers here are the numbers a write would send. */
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert');
const Sku = require('../netlify/functions/_charmNestEtsySku.js');      // pure at load: Firebase is only built on first real use, which this script never triggers
const Canon = require('../netlify/functions/_etsyInventoryCanonical.js');
const { sanitizeProduct, decodeOnce } = Sku.internals;

/* ── the twelve signs ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   Labels are the plain sign names (Etsy refuses "(" and ")" in option labels, and the app reads a sign by its exact name). Order = the listing photo's.
   sku: the SKU each option carries on Etsy. The seven "repo" ones are the master's own huggie SKUs, spelled exactly as the master holds them (looked at on the
   master cards and in the 11 Oct master copy: same size in pt as the cards' mm). The five "new" ones exist nowhere yet (Paul adds the designs later and names
   each master entry like its Etsy SKU): the repo's huggie convention is "<design name> (HUGGIE)", and the stem "ZODIAC <SIGN>" is the one that fits Etsy's
   32-character SKU limit for every sign ("ZODIAC ICONS SAGITTARIUS (HUGGIE)" is 33). */
const SIGNS = Object.freeze([
  { label: 'Aquarius',    sku: 'ZODIAC AQUARIUS (HUGGIE)',      from: 'new' },
  { label: 'Pisces',      sku: 'ZODIAC DISCS PISCES (HUGGIE)',  from: 'repo' },
  { label: 'Aries',       sku: 'ZODIAC ICONS ARIES (HUGGIE)',   from: 'repo' },
  { label: 'Taurus',      sku: 'ZODIAC DISCS TAURUS (HUGGIE)',  from: 'repo' },
  { label: 'Gemini',      sku: 'ZODIAC GEMINI (HUGGIE)',        from: 'new' },
  { label: 'Cancer',      sku: 'ZODIAC CANCER (HUGGIE)',        from: 'new' },
  { label: 'Leo',         sku: 'ZODIAC ICONS LEO (HUGGIE)',     from: 'repo' },
  { label: 'Virgo',       sku: 'ZODIAC VIRGO (HUGGIE)',         from: 'repo' },
  { label: 'Libra',       sku: 'ZODIAC ICONS LIBRA (HUGGIE)',   from: 'repo' },
  { label: 'Scorpio',     sku: 'ZODIAC DISCS SCORPIO (HUGGIE)', from: 'repo' },
  { label: 'Sagittarius', sku: 'ZODIAC SAGITTARIUS (HUGGIE)',   from: 'new' },
  { label: 'Capricorn',   sku: 'ZODIAC CAPRICORN (HUGGIE)',     from: 'new' }
].map(Object.freeze));
const OLD_LISTING_SKU = 'HUGGIE HOOPS-ZODIAC';          // the one SKU every product of the listing carries today (the master reads it as Libra: the bug of order 4200256896)
const SIGN_PROPERTY_NAME = 'ZODIAC SIGN';               // upper case like METAL CHOICE and HOOP SIZE, which Etsy keeps as typed
const CUSTOM_IDS = Object.freeze([513, 514, 516]);      // Etsy's custom-variation property ids ("at most 3 custom variations", Etsy Listings Tutorial)
const MAX_SKU = 32, MAX_ALL = 400, MAX_THREE = 2500, VALUE_LIMIT_SEEN = 20;
const LISTS = ['price_on_property', 'quantity_on_property', 'sku_on_property'];

/* ── small helpers ── */
const clone = x => JSON.parse(JSON.stringify(x));
const live = inv => (Array.isArray(inv && inv.products) ? inv.products : []).filter(p => p && typeof p === 'object' && !p.is_deleted);
const ids = x => (Array.isArray(x) ? x : []).map(Number).filter(Number.isFinite);
const propOrder = inv => Canon.productPropertyOrder({ products: live(inv) });
const looseKey = s => String(s == null ? '' : s).toUpperCase().replace(/[^A-Z0-9]+/g, '');
const setOf = x => JSON.stringify([...new Set(ids(x))].sort((a, b) => a - b));
const sameList = (a, b) => JSON.stringify(ids(a)) === JSON.stringify(ids(b));

/* ── the builder ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   inv: Etsy's getListingInventory answer. opts: { structure: 'three' | 'two', thirdPropertyId?, signs?, labelMap?: { "<metal>|<size>": "label" } }
   → { query, body, plan } . Every source product becomes 12 products (one per sign); the existing option values keep their ids; nothing else is touched. */
function build(inv, opts) {
  opts = opts || {};
  const structure = opts.structure === 'two' ? 'two' : 'three';
  const signs = opts.signs || SIGNS;
  const src = live(inv), props = propOrder(inv);
  if (!src.length) throw new Error('the inventory has no live products');
  if (props.length !== 2) throw new Error('expected a listing with exactly two variation properties today, found ' + props.length + ' (' + props.join(', ') + ')');
  const out = { products: [], price_on_property: [], quantity_on_property: [], sku_on_property: [] };
  let thirdId = null, all;
  if (structure === 'three') {
    thirdId = opts.thirdPropertyId != null ? Number(opts.thirdPropertyId) : CUSTOM_IDS.find(i => !props.includes(i));
    if (!thirdId) throw new Error('no free custom property id among ' + CUSTOM_IDS.join('/') + ' (the listing uses ' + props.join(', ') + ')');
    if (props.includes(thirdId)) throw new Error('property id ' + thirdId + ' is already a variation of this listing');
    all = [...props, thirdId];
  } else all = [...props];
  const sanitized = src.map(p => sanitizeProduct(p, ''));   // the app's own product shape: decoded text, decimal price, one offering, readiness kept
  sanitized.forEach((base, i) => {
    for (const s of signs) {
      const prod = clone(base);
      prod.sku = s.sku;
      if (structure === 'three') {
        prod.property_values.push({ property_id: thirdId, property_name: SIGN_PROPERTY_NAME, scale_id: null, value_ids: [], values: [s.label] });
      } else {
        // the fallback: the two existing drop-downs become one ("<metal> - <size>"), the second slot becomes the sign
        const [a, b] = prod.property_values;
        const key = a.values.join('/') + '|' + b.values.join('/');
        const label = (opts.labelMap && opts.labelMap[key]) || (a.values.join('/') + ' - ' + b.values.join('/'));
        prod.property_values = [
          { property_id: a.property_id, property_name: opts.combinedName || 'METAL & HOOP SIZE', scale_id: null, value_ids: [], values: [label] },
          { property_id: b.property_id, property_name: SIGN_PROPERTY_NAME, scale_id: null, value_ids: [], values: [s.label] }
        ];
      }
      out.products.push(prod);
    }
  });
  // *_on_property: Etsy takes zero, one or ALL of the properties (two of three is a 400), and once one list holds them all every other list is zero or all.
  // Price varied before, so it is "all" now (every sign has the same price: the validator proves it); the SKU varies by sign, so it is "all" too.
  const was = f => ids(inv[f]).length > 0;
  out.price_on_property = was('price_on_property') ? [...all] : [];
  out.quantity_on_property = was('quantity_on_property') ? [...all] : [];
  out.sku_on_property = [...all];
  if (Array.isArray(inv.readiness_state_on_property)) out.readiness_state_on_property = ids(inv.readiness_state_on_property).length ? [...all] : [];
  return {
    query: structure === 'three' ? { max_variations_supported: 3 } : {},
    body: out,
    plan: { structure, sourceProducts: src.length, signs: signs.length, products: out.products.length, existingProperties: props, thirdPropertyId: thirdId, propertyOrder: all,
      table: signs.map(s => ({ sign: s.label, sku: s.sku, from: s.from, products: src.length })) }
  };
}

/* ── the validator ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   Written separately from build() (it reads the source inventory directly, not through sanitizeProduct) so one bug cannot hide in both.
   → { ok, failures: [string], warnings: [string], stats } */
function validate(inv, built, opts) {
  opts = opts || {};
  const f = [], w = [];
  const structure = (built.plan && built.plan.structure) || 'three';
  const signs = opts.signs || SIGNS;
  const src = live(inv), props = propOrder(inv), body = built.body || {}, products = Array.isArray(body.products) ? body.products : [];
  const nS = signs.length, expected = src.length * nS;
  const thirdId = built.plan && built.plan.thirdPropertyId;
  const wantIds = structure === 'three' ? [...props, thirdId] : [...props];

  // 1 · the query and the top-level shape
  if (structure === 'three') { if (!built.query || built.query.max_variations_supported !== 3) f.push('the PUT must carry max_variations_supported=3 (without it Etsy answers 409 for a third variation)'); }
  else if (built.query && built.query.max_variations_supported != null) f.push('the two-variation fallback must not carry max_variations_supported');
  const topAllowed = ['products', ...LISTS, 'readiness_state_on_property'];
  for (const k of Object.keys(body)) if (!topAllowed.includes(k)) f.push('unexpected body field ' + k);
  if (nS !== 12) f.push('there must be 12 signs, found ' + nS);

  // 2 · counts and Etsy's product caps
  if (products.length !== expected) f.push('product count is ' + products.length + ', expected ' + src.length + ' x ' + nS + ' = ' + expected);
  const anyAll = LISTS.concat(['readiness_state_on_property']).some(k => Array.isArray(body[k]) && body[k].length === wantIds.length && wantIds.length > 0);
  if (anyAll && products.length > MAX_ALL) f.push('more than ' + MAX_ALL + ' products (' + products.length + '): Etsy refuses a listing that varies by all properties above ' + MAX_ALL);
  if (products.length > MAX_THREE) f.push('more than ' + MAX_THREE + ' products');

  // 3 · every product: shape, SKU, option text
  const bySign = new Map(), combos = new Set(), perSignCount = new Map();
  products.forEach((p, i) => {
    const k = 'product ' + (i + 1);
    for (const key of Object.keys(p)) if (!['sku', 'property_values', 'offerings'].includes(key)) f.push(k + ' has field ' + key + ' (ids and flags must not be sent)');
    if (typeof p.sku !== 'string' || !p.sku.trim()) f.push(k + ' has an empty SKU');
    else {
      if (p.sku.length > MAX_SKU) f.push(k + ' SKU is ' + p.sku.length + ' characters (Etsy allows 32): ' + p.sku);
      if (p.sku !== p.sku.trim()) f.push(k + ' SKU has leading or trailing space');
      if (/[,/\\]/.test(p.sku)) w.push(k + ' SKU has a comma or slash (Etsy advises against symbols): ' + p.sku);
    }
    const pv = Array.isArray(p.property_values) ? p.property_values : [];
    if (pv.length !== wantIds.length) f.push(k + ' has ' + pv.length + ' property values, expected ' + wantIds.length);
    for (const v of pv) {
      if (!Number.isInteger(v.property_id) || !Array.isArray(v.value_ids) || !Array.isArray(v.values) || !v.values.length) f.push(k + ' has a malformed property value');
      for (const t of v.values || []) {
        if (typeof t !== 'string' || !t.trim()) f.push(k + ' has an empty option label');
        else {
          if (/[()]/.test(t)) f.push(k + ' option label has parentheses, which Etsy refuses: ' + t);
          if (t.length > VALUE_LIMIT_SEEN && !w.some(x => x.includes('"' + t + '"'))) w.push('option label "' + t + '" is ' + t.length + ' characters (the longest value seen on the shop is 20; Etsy\'s limit is not documented)');
        }
      }
    }
    if (JSON.stringify(pv.map(v => v.property_id)) !== JSON.stringify(wantIds)) f.push(k + ' property ids are ' + JSON.stringify(pv.map(v => v.property_id)) + ', expected ' + JSON.stringify(wantIds));
    const ck = JSON.stringify(pv.map(v => [v.property_id, v.values]));
    if (combos.has(ck)) f.push(k + ' repeats an option combination: ' + ck); combos.add(ck);
    const signLabel = pv.length ? (pv[pv.length - 1].values || [])[0] : '';
    const sign = signs.find(s => s.label === signLabel);
    if (!sign) f.push(k + ' carries a sign that is not one of the twelve: ' + signLabel);
    else {
      perSignCount.set(sign.label, (perSignCount.get(sign.label) || 0) + 1);
      if (p.sku !== sign.sku) f.push(k + ' (' + sign.label + ') SKU is ' + JSON.stringify(p.sku) + ', expected ' + JSON.stringify(sign.sku));
      if (!bySign.has(sign.label)) bySign.set(sign.label, new Set()); bySign.get(sign.label).add(p.sku);
    }
  });
  for (const s of signs) {
    if ((perSignCount.get(s.label) || 0) !== src.length) f.push(s.label + ' appears on ' + (perSignCount.get(s.label) || 0) + ' products, expected ' + src.length);
    if ((bySign.get(s.label) || new Set()).size > 1) f.push(s.label + ' has more than one SKU');
  }
  const skuSet = new Set(signs.map(s => s.sku));
  if (skuSet.size !== nS) f.push('the 12 signs do not have 12 different SKUs');
  if (new Set(signs.map(s => looseKey(s.sku))).size !== nS) f.push('two signs have SKUs that differ only in spacing or punctuation (the app reads those as one SKU)');
  for (const s of signs) if (looseKey(s.sku) === looseKey(OLD_LISTING_SKU)) f.push(s.label + ' SKU is the listing\'s old shared SKU');

  // 4 · the existing options, prices, stock and processing profile are exactly what was read (a second, independent comparison per source product and sign)
  src.forEach((g, i) => {
    const go = Array.isArray(g.offerings) ? g.offerings[0] : null;
    if (!go) { f.push('source product ' + (i + 1) + ' has no offering'); return; }
    const enabled = go.is_enabled !== false, price = Canon.toDecimalPrice(go.price), q = enabled ? Number(go.quantity != null ? go.quantity : go.available_quantity != null ? go.available_quantity : 0) : 0;
    if (!(Number.isFinite(price) && price > 0)) f.push('source product ' + (i + 1) + ' price unreadable');
    signs.forEach((s, j) => {
      const b = products[i * nS + j], k = 'source product ' + (i + 1) + ' x ' + s.label;
      if (!b) return;
      const o = Array.isArray(b.offerings) && b.offerings.length === 1 ? b.offerings[0] : null;
      if (!o) { f.push(k + ': offerings are not exactly one'); return; }
      if (o.price !== price) f.push(k + ' price is ' + o.price + ', was ' + price);
      if (o.quantity !== q) f.push(k + ' quantity is ' + o.quantity + ', was ' + q);
      if (o.is_enabled !== enabled) f.push(k + ' enabled is ' + o.is_enabled + ', was ' + enabled);
      if (go.readiness_state_id != null ? o.readiness_state_id !== Number(go.readiness_state_id) : Object.prototype.hasOwnProperty.call(o, 'readiness_state_id')) f.push(k + ' processing profile (readiness_state_id) changed');
      for (const key of Object.keys(o)) if (!['price', 'quantity', 'is_enabled', 'readiness_state_id'].includes(key)) f.push(k + ' offering has field ' + key);
      const keep = structure === 'three' ? (g.property_values || []) : [];
      keep.forEach((v, n) => {
        const w2 = (b.property_values || [])[n] || {}, vals = (v.values || []).map(x => decodeOnce(String(x)));
        if (w2.property_id !== Number(v.property_id)) f.push(k + ' option id changed');
        if (JSON.stringify(w2.values) !== JSON.stringify(vals)) f.push(k + ' option text changed: ' + JSON.stringify(w2.values) + ' was ' + JSON.stringify(vals));
        if (JSON.stringify(w2.value_ids) !== JSON.stringify(Array.isArray(v.value_ids) && v.value_ids.length === vals.length ? v.value_ids : [])) f.push(k + ' option value ids changed');
        if ((w2.scale_id == null ? null : w2.scale_id) !== (v.scale_id == null ? null : v.scale_id)) f.push(k + ' scale changed');
        if (v.property_name != null && w2.property_name !== decodeOnce(String(v.property_name))) f.push(k + ' option name changed');
      });
      if (structure === 'two') {
        const [pa, pb] = g.property_values || [], mk = (pa.values || []).map(x => decodeOnce(String(x))).join('/') + '|' + (pb.values || []).map(x => decodeOnce(String(x))).join('/');
        const want = (opts.labelMap && opts.labelMap[mk]) || mk.replace('|', ' - '), got = b.property_values || [];
        if (!got[0] || got[0].property_id !== Number(pa.property_id) || JSON.stringify(got[0].values) !== JSON.stringify([want]) || JSON.stringify(got[0].value_ids) !== '[]') f.push(k + ' merged option is not "' + want + '"');
        if (!got[1] || got[1].property_id !== Number(pb.property_id) || got[1].property_name !== SIGN_PROPERTY_NAME || JSON.stringify(got[1].values) !== JSON.stringify([s.label])) f.push(k + ' second option is not the sign ' + s.label);
      }
      if (structure === 'three') {
        const t = (b.property_values || [])[keep.length] || {};
        if (t.property_id !== thirdId || t.property_name !== SIGN_PROPERTY_NAME || t.scale_id !== null || JSON.stringify(t.value_ids) !== '[]' || JSON.stringify(t.values) !== JSON.stringify([s.label])) f.push(k + ' third property is not the ' + SIGN_PROPERTY_NAME + ' custom option for ' + s.label);
      }
    });
  });
  const sum = a => Math.round(a.reduce((x, y) => x + y, 0) * 100) / 100;
  const oldSum = sum(src.map(g => Canon.toDecimalPrice(g.offerings[0].price))), newSum = sum(products.map(p => p.offerings && p.offerings[0] && p.offerings[0].price));
  if (Math.round(oldSum * nS * 100) / 100 !== newSum) f.push('the price total is ' + newSum + ', expected ' + nS + ' x ' + oldSum);

  // 5 · property ids and the *_on_property lists
  if (structure === 'three') {
    if (!CUSTOM_IDS.includes(thirdId)) w.push('the third property id ' + thirdId + ' is not one of Etsy\'s custom ids ' + CUSTOM_IDS.join('/') + ' (a taxonomy property needs a real value id list)');
    if (props.includes(thirdId)) f.push('the third property id is already used by this listing');
  }
  const total = wantIds.length;
  for (const key of LISTS.concat(['readiness_state_on_property'])) {
    if (!(key in body)) { if (key !== 'readiness_state_on_property') f.push(key + ' is missing'); else if (Array.isArray(inv.readiness_state_on_property)) f.push(key + ' was read and must be sent back'); continue; }
    const l = body[key];
    if (!Array.isArray(l)) { f.push(key + ' is not a list'); continue; }
    if (l.some(x => !wantIds.includes(x))) f.push(key + ' names a property the listing does not have');
    if (new Set(l).size !== l.length) f.push(key + ' repeats a property');
    if (total === 3 && l.length === 2) f.push(key + ' has exactly two of three properties (Etsy: "supports only zero, one, or all 3 variation properties")');
    if (l.length && JSON.stringify(l) !== JSON.stringify(wantIds.filter(x => l.includes(x)))) f.push(key + ' is not in the order the properties appear in the products');
  }
  const lens = LISTS.concat(Array.isArray(body.readiness_state_on_property) ? ['readiness_state_on_property'] : []).map(k => (body[k] || []).length);
  if (lens.some(n => n === total) && lens.some(n => n > 0 && n < total)) f.push('one *_on_property list holds every property while another holds only some (Etsy refuses it)');
  if (ids(inv.price_on_property).length && !sameList(body.price_on_property, wantIds)) f.push('price varied by property before; it must now be listed on all ' + total);
  if (!sameList(body.sku_on_property, wantIds)) f.push('sku_on_property must list all ' + total + ' properties (the SKU varies by sign)');
  // the declared lists must be true of the products: a field is constant across products that share their values on the declared properties
  for (const key of ['price', 'quantity']) {
    const declared = ids(body[key + '_on_property']), seen = new Map();
    for (const p of products) {
      const gk = JSON.stringify((p.property_values || []).filter(v => declared.includes(v.property_id)).map(v => [v.property_id, v.values]));
      const val = key === 'price' ? p.offerings[0].price : p.offerings[0].quantity;
      if (seen.has(gk) && seen.get(gk) !== val) { f.push(key + '_on_property does not cover how ' + key + ' really varies'); break; }
      seen.set(gk, val);
    }
  }
  return { ok: f.length === 0, failures: f, warnings: w, stats: { sourceProducts: src.length, products: products.length, signs: nS, distinctSkus: skuSet.size, priceTotal: newSum } };
}

/* ── the SKU table against the master (--master) ── */
function checkMaster(masterList, signs) {
  signs = signs || SIGNS;
  const f = [], names = new Set((masterList || []).map(x => String(x && x.sku != null ? x.sku : x).toUpperCase())), loose = new Map();
  for (const n of names) { const l = looseKey(n); loose.set(l, (loose.get(l) || []).concat(n)); }
  for (const s of signs) {
    const hit = names.has(s.sku.toUpperCase()), lk = loose.get(looseKey(s.sku)) || [];
    if (s.from === 'repo' && !hit) f.push(s.label + ': ' + s.sku + ' is not in the master (the Etsy SKU would not match the repo)');
    if (s.from === 'new' && (hit || lk.length)) f.push(s.label + ': ' + s.sku + ' already reads as master SKU ' + (hit ? s.sku : lk[0]) + ' (a "missing" sign would resolve to another design)');
  }
  return f;
}

/* ── the restore ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   backup: the GET answer saved before the write. The restored listing has the SAME options, prices, quantities, readiness and SKUs (the shared SKU again); Etsy gives every
   product a new id on any PUT, so "restore" recreates the inventory, it cannot bring the old product ids back. The listing has three variations when it is needed,
   so it MUST carry max_variations_supported=3 (without it Etsy answers 409 and nothing is restored). */
function buildRestore(backup, opts) {
  const src = live(backup), props = propOrder(backup);
  if (!src.length) throw new Error('the backup has no live products');
  const body = { products: src.map(p => sanitizeProduct(p, decodeOnce(p.sku == null ? '' : p.sku))) };
  const norm = Sku.internals.normalizeOnProperty(backup, props);
  for (const k of LISTS) body[k] = norm[k];
  if (Array.isArray(backup.readiness_state_on_property)) body.readiness_state_on_property = ids(backup.readiness_state_on_property);
  return { query: { max_variations_supported: 3 }, body, plan: { products: body.products.length, properties: props } };
}
function validateRestore(backup, restore) {
  const f = [], src = live(backup), props = propOrder(backup), products = (restore.body && restore.body.products) || [];
  if (!restore.query || restore.query.max_variations_supported !== 3) f.push('the restore PUT must carry max_variations_supported=3 (the listing has three variations then)');
  if (products.length !== src.length) f.push('restore has ' + products.length + ' products, the backup ' + src.length);
  const key = ps => JSON.stringify(ps.map(p => [Number(p.property_id), (p.values || []).map(x => decodeOnce(String(x)))]));
  src.forEach((g, i) => {
    const b = products[i], k = 'product ' + (i + 1);
    if (!b) return;
    if (b.sku !== decodeOnce(g.sku == null ? '' : g.sku)) f.push(k + ' SKU differs from the backup');
    if (key(b.property_values || []) !== key(g.property_values || [])) f.push(k + ' options differ from the backup');
    if ((b.property_values || []).length !== props.length) f.push(k + ' has ' + (b.property_values || []).length + ' options, the backup ' + props.length);
    if (JSON.stringify((b.property_values || []).map(v => v.value_ids)) !== JSON.stringify((g.property_values || []).map(v => (Array.isArray(v.value_ids) && v.value_ids.length === (v.values || []).length ? v.value_ids : [])))) f.push(k + ' option value ids differ from the backup');
    const go = g.offerings[0], o = (b.offerings || [])[0] || {}, enabled = go.is_enabled !== false;
    if (o.price !== Canon.toDecimalPrice(go.price)) f.push(k + ' price differs from the backup');
    if (o.quantity !== (enabled ? Number(go.quantity != null ? go.quantity : go.available_quantity != null ? go.available_quantity : 0) : 0)) f.push(k + ' quantity differs from the backup');
    if (o.is_enabled !== enabled) f.push(k + ' enabled differs from the backup');
    if ((go.readiness_state_id != null ? Number(go.readiness_state_id) : null) !== (o.readiness_state_id != null ? o.readiness_state_id : null)) f.push(k + ' processing profile differs from the backup');
  });
  for (const k of LISTS) if (setOf(restore.body[k]) !== setOf(Sku.internals.normalizeOnProperty(backup, props)[k])) f.push(k + ' differs from the backup');
  if (Array.isArray(backup.readiness_state_on_property) && setOf(restore.body.readiness_state_on_property) !== setOf(backup.readiness_state_on_property)) f.push('readiness_state_on_property differs from the backup');
  return { ok: f.length === 0, failures: f };
}

/* ── the documented fixture: Etsy's getListingInventory shape for a huggie hoop listing ─────────────────────────────────────────────────────────────────────
   Real parts (from the shop's other huggie hoop listings in the 17 Sep order snapshot): property 514 = METAL CHOICE (Silver 55196991045, Gold 54296040664, 14k Solid Gold
   148615583071), property 513 = HOOP SIZE (8.5mm 110638330075, 11mm 65252824563), custom values (scale null), one SKU shared by every product. INVENTED parts: the prices
   (only the $32.00 floor is the listing page's struck-through price), quantities, offering/product ids and the processing profile id. The real listing is read once, supervised. */
function fixture(extra) {
  extra = extra || {};
  const metals = extra.metals || [['Silver', 55196991045, 3200], ['Gold', 54296040664, 3800], ['14k Solid Gold', 148615583071, 28900]];
  const sizes = extra.sizes || [['8.5mm', 110638330075, 0], ['11mm', 65252824563, 200]];
  const products = []; let n = 0;
  for (const [mn, mid, mp] of metals) for (const [sn, sid, sp] of sizes) {
    n++;
    products.push({
      product_id: 28100000000 + n, sku: OLD_LISTING_SHARED(), is_deleted: false,
      offerings: [{ offering_id: 34500000000 + n, quantity: extra.quantity != null ? extra.quantity : 999, is_enabled: true, is_deleted: false, price: { amount: mp + sp, divisor: 100, currency_code: 'USD' }, readiness_state_id: 1020304051823 }],
      property_values: [
        { property_id: 514, property_name: 'METAL CHOICE', scale_id: null, scale_name: null, value_ids: [mid], values: [mn] },
        { property_id: 513, property_name: 'HOOP SIZE', scale_id: null, scale_name: null, value_ids: [sid], values: [sn] }
      ]
    });
  }
  const all = [514, 513];
  return { products, price_on_property: all, quantity_on_property: [], sku_on_property: [], readiness_state_on_property: [] };
}
function OLD_LISTING_SHARED() { return OLD_LISTING_SKU; }

/* ── the human report ── */
function report(inv, built, v, rb, rv) {
  const L = [];
  L.push('DRY RUN: nothing was sent anywhere. This is the PUT that would be sent, checked.');
  L.push('structure: ' + built.plan.structure + '   source products: ' + built.plan.sourceProducts + '   signs: ' + built.plan.signs + '   products to send: ' + built.plan.products);
  L.push('existing property ids: ' + built.plan.existingProperties.join(', ') + (built.plan.thirdPropertyId ? '   new property id: ' + built.plan.thirdPropertyId : ''));
  L.push('PUT query: ' + (Object.keys(built.query).length ? Object.entries(built.query).map(([k, x]) => k + '=' + x).join('&') : '(none)'));
  L.push('price_on_property ' + JSON.stringify(built.body.price_on_property) + '   quantity_on_property ' + JSON.stringify(built.body.quantity_on_property) + '   sku_on_property ' + JSON.stringify(built.body.sku_on_property) + (built.body.readiness_state_on_property ? '   readiness_state_on_property ' + JSON.stringify(built.body.readiness_state_on_property) : ''));
  L.push('', 'SKU per sign:'); for (const r of built.plan.table) L.push('  ' + r.sign.padEnd(12) + r.sku.padEnd(34) + (r.from === 'repo' ? 'repo master SKU' : 'NEW (no master entry yet)') + '   x ' + r.products + ' products');
  L.push('', 'validation: ' + (v.ok ? 'PASSED' : 'FAILED') + '  (' + v.failures.length + ' failures, ' + v.warnings.length + ' warnings)');
  v.failures.slice(0, 40).forEach(x => L.push('  FAIL ' + x)); v.warnings.slice(0, 20).forEach(x => L.push('  warn ' + x));
  if (rb) { L.push('', 'restore PUT (from the backup): ' + rb.plan.products + ' products, query max_variations_supported=3, validation: ' + (rv.ok ? 'PASSED' : 'FAILED')); rv.failures.slice(0, 20).forEach(x => L.push('  FAIL ' + x)); }
  return L.join('\n');
}

/* ── the self-test: the fixture, mutations the validator must catch, the restore, the fallback ──────────────────────────────────────────────────────────── */
function selfTest() {
  let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
  const inv = fixture();
  // 1 · the fixture builds 72 products and passes
  const b = build(inv), v = validate(inv, b);
  ok(v.ok, 'the fixture payload validates: ' + v.failures.join(' | '));
  eq([b.plan.sourceProducts, b.body.products.length, b.plan.signs], [6, 72, 12], '6 x 12 = 72 products');
  eq(b.query, { max_variations_supported: 3 }, 'the third variation needs max_variations_supported=3');
  eq(b.plan.thirdPropertyId, 516, 'the free custom id is 516 when 513 and 514 are taken');
  eq(b.body.price_on_property, [514, 513, 516], 'price varies by all three (two of three is a 400)'); eq(b.body.sku_on_property, [514, 513, 516], 'sku list holds all three too'); eq(b.body.quantity_on_property, [], 'an empty list stays empty');
  eq(b.body.readiness_state_on_property, [], 'the readiness list that was read is sent back');
  eq(new Set(b.body.products.map(p => p.sku)).size, 12, 'twelve distinct SKUs'); ok(b.body.products.every(p => p.sku && p.sku.length <= 32), 'no empty SKU, none over 32 characters');
  ok(b.body.products.every(p => p.property_values.length === 3 && p.property_values[2].property_id === 516 && p.property_values[2].value_ids.length === 0), 'every product has the sign as its third option, a custom value with no id');
  ok(b.body.products.every(p => !('product_id' in p) && !('is_deleted' in p) && !('offering_id' in p.offerings[0])), 'no ids are sent');
  eq(b.body.products.filter(p => p.sku === 'ZODIAC ICONS LIBRA (HUGGIE)').length, 6, 'Libra is on 6 products (3 metals x 2 sizes)');
  eq(SIGNS.filter(s => s.from === 'repo').map(s => s.label).sort(), ['Aries', 'Leo', 'Libra', 'Pisces', 'Scorpio', 'Taurus', 'Virgo'], 'seven signs are the repo\'s');
  eq(SIGNS.filter(s => s.from === 'new').map(s => s.label).sort(), ['Aquarius', 'Cancer', 'Capricorn', 'Gemini', 'Sagittarius'], 'five are new');
  ok(SIGNS.every(s => s.sku.length <= 32 && !/[()]/.test(s.label)), 'SKUs fit, labels have no parentheses');
  // every price is the source's (31.99 style rounding, the cents survive)
  const inv2 = clone(inv); inv2.products[1].offerings[0].price = { amount: 3399, divisor: 100, currency_code: 'USD' };
  const b2 = build(inv2); ok(validate(inv2, b2).ok, 'a 33.99 price validates'); eq(b2.body.products.filter(p => p.offerings[0].price === 33.99).length, 12, '33.99 is kept exactly on the 12 signs of that product');
  // 2 · the validator catches every wrong payload
  const bad = (name, mutate, expectText) => { const c = clone(b); mutate(c); const r = validate(inv, c); ok(!r.ok && (!expectText || r.failures.some(x => x.includes(expectText))), name + ' is caught: ' + r.failures.slice(0, 3).join(' | ')); };
  bad('a changed price', c => { c.body.products[5].offerings[0].price = 31.99; }, 'price is');
  bad('a changed quantity', c => { c.body.products[7].offerings[0].quantity = 1; }, 'quantity is');
  bad('a disabled offering', c => { c.body.products[3].offerings[0].is_enabled = false; }, 'enabled is');
  bad('a lost processing profile', c => { delete c.body.products[9].offerings[0].readiness_state_id; }, 'processing profile');
  bad('an empty SKU', c => { c.body.products[4].sku = ''; }, 'empty SKU');
  bad('a duplicate SKU across two signs', c => { for (const p of c.body.products) if (p.sku === 'ZODIAC CANCER (HUGGIE)') p.sku = 'ZODIAC GEMINI (HUGGIE)'; }, 'expected');
  bad('a SKU over 32 characters', c => { c.body.products[0].sku = 'ZODIAC ICONS SAGITTARIUS (HUGGIE) X'; }, 'characters');
  bad('a missing product', c => { c.body.products.pop(); }, 'product count');
  bad('an extra product', c => { c.body.products.push(clone(c.body.products[0])); }, 'product count');
  bad('a repeated combination', c => { c.body.products[1] = clone(c.body.products[0]); }, 'repeats');
  bad('a product id left in', c => { c.body.products[2].product_id = 1; }, 'ids and flags');
  bad('a parenthesis in a label', c => { c.body.products[0].property_values[2].values = ['Aquarius (Jan 20)']; }, 'parentheses');
  bad('a sign that is not one of the twelve', c => { c.body.products[0].property_values[2].values = ['Ophiuchus']; }, 'not one of the twelve');
  bad('the wrong third property id', c => { c.body.products.forEach(p => { p.property_values[2].property_id = 514; }); }, 'property ids');
  bad('a changed option value id', c => { c.body.products[0].property_values[0].value_ids = [1]; }, 'value ids changed');
  bad('two of three properties in a list', c => { c.body.price_on_property = [514, 513]; }, 'exactly two');
  bad('sku list left empty', c => { c.body.sku_on_property = []; }, 'sku_on_property must');
  bad('a partial list next to an all list', c => { c.body.quantity_on_property = [516]; }, 'holds every property');
  bad('the query parameter dropped', c => { c.query = {}; }, 'max_variations_supported=3');
  bad('an unexpected body field', c => { c.body.legacy = false; }, 'unexpected body field');
  bad('readiness list dropped', c => { delete c.body.readiness_state_on_property; }, 'must be sent back');
  // 3 · the 400-product cap
  const big = fixture({ sizes: Array.from({ length: 12 }, (_, i) => [(8 + i) + 'mm', 1000 + i, 0]) });
  ok(!validate(big, build(big)).ok && validate(big, build(big)).failures.some(x => x.includes('400')), '3 metals x 12 sizes x 12 signs = 432 is over Etsy\'s 400 for a listing that varies by all properties');
  const eleven = fixture({ sizes: Array.from({ length: 11 }, (_, i) => [(8 + i) + 'mm', 1000 + i, 0]) }); ok(validate(eleven, build(eleven)).ok, '11 sizes = 396 products is allowed');
  // 4 · other shapes of the listing
  const off = fixture(); off.quantity_on_property = [514, 513]; off.products[0].offerings[0].is_enabled = false; off.products[0].offerings[0].quantity = 5;   // (stock that differs by product means the listing varies quantity by property)
  const bo = build(off); ok(validate(off, bo).ok, 'a disabled product validates'); ok(bo.body.products.slice(0, 12).every(p => p.offerings[0].is_enabled === false && p.offerings[0].quantity === 0), 'a disabled offering keeps quantity 0 on all 12 signs (Etsy\'s unavailable form)');
  const esc = fixture(); esc.products[0].property_values[1].values = ['8.5&quot;mm &amp; up']; const be = build(esc); ok(validate(esc, be).ok, 'escaped text validates'); eq(be.body.products[0].property_values[1].values, ['8.5"mm & up'], 'Etsy\'s HTML-escaped text is sent back unescaped once');
  const nr = fixture(); delete nr.readiness_state_on_property; ok(!('readiness_state_on_property' in build(nr).body), 'no readiness list read, none sent');
  const twoP = fixture(); twoP.price_on_property = [514]; const bt = build(twoP); ok(validate(twoP, bt).ok && sameList(bt.body.price_on_property, [514, 513, 516]), 'a price list of one property is widened to all (the other lists are all)');
  assert.throws(() => build({ products: [{ sku: 'X', offerings: [{ price: 1, quantity: 1, is_enabled: true }], property_values: [] }] }), /exactly two/); n++;
  const three = fixture(); three.products.forEach(p => p.property_values.push({ property_id: 516, property_name: 'X', scale_id: null, value_ids: [], values: ['y'] })); assert.throws(() => build(three), /exactly two/); n++;
  const usedUp = fixture(); usedUp.products.forEach(p => { p.property_values[0].property_id = 516; }); eq(build(usedUp).plan.thirdPropertyId, 514, 'the free custom id is chosen (514 when 516 and 513 are taken)');
  // 5 · the SKU table against a master
  const master = ['ZODIAC ICONS LIBRA (HUGGIE)', 'ZODIAC DISCS PISCES (HUGGIE)', 'ZODIAC DISCS SCORPIO (HUGGIE)', 'ZODIAC DISCS TAURUS (HUGGIE)', 'ZODIAC VIRGO (HUGGIE)', 'ZODIAC ICONS LEO (HUGGIE)', 'ZODIAC ICONS ARIES (HUGGIE)',
    'HUGGIE HOOPS- ZODIAC', 'HUGGIE HOOPS- ZODIAC AVATAR', 'ZODIAC DISCS AQUARIUS', 'ZODIAC ICONS LIBRA', 'ZODIAC_EARRINGS-9'];
  eq(checkMaster(master), [], 'the seven are in the master, the five are in none');
  ok(checkMaster(master.slice(1)).some(x => x.includes('Libra')), 'a repo SKU missing from the master is reported');
  ok(checkMaster(master.concat(['ZODIAC GEMINI (HUGGIE)'])).some(x => x.includes('Gemini')), 'a "missing" sign that already exists is reported');
  ok(checkMaster(master.concat(['ZODIAC  CANCER  HUGGIE'])).some(x => x.includes('Cancer')), 'a spacing/punctuation twin of a new SKU is reported (the app reads it as the same SKU)');
  // 6 · the restore
  const rb = buildRestore(inv), rv = validateRestore(inv, rb);
  ok(rv.ok, 'the restore validates: ' + rv.failures.join(' | ')); eq(rb.query, { max_variations_supported: 3 }, 'the restore needs max_variations_supported=3');
  eq(rb.body.products.length, 6, 'the restore has the original 6 products'); ok(rb.body.products.every(p => p.sku === OLD_LISTING_SKU && p.property_values.length === 2), 'the shared SKU is back, two options each');
  eq(rb.body.price_on_property, [514, 513], 'the original lists come back'); eq(rb.body.sku_on_property, [], 'including the empty ones');
  const brokenRb = clone(rb); brokenRb.body.products[0].offerings[0].price = 1; ok(!validateRestore(inv, brokenRb).ok, 'a restore with a changed price is caught');
  const noParam = clone(rb); noParam.query = {}; ok(!validateRestore(inv, noParam).ok, 'a restore without the query parameter is caught');
  // restoring what the new payload came from gives back the original products exactly (modulo ids)
  eq(rb.body.products.map(p => p.property_values.map(x => [x.property_id, x.values, x.value_ids])), b.body.products.filter((_, i) => i % 12 === 0).map(p => p.property_values.slice(0, 2).map(x => [x.property_id, x.values, x.value_ids])), 'restore options = the options the new payload kept');
  // 7 · the two-drop-down fallback
  const t2 = build(inv, { structure: 'two' }), tv = validate(inv, t2);
  ok(tv.ok, 'the fallback validates: ' + tv.failures.join(' | ')); eq([t2.body.products.length, Object.keys(t2.query).length], [72, 0], '72 products, no third-variation parameter');
  eq(t2.body.price_on_property, [514, 513], 'two properties: both listed'); ok(t2.body.products.every(p => p.property_values.length === 2 && p.property_values[0].property_id === 514 && p.property_values[1].values[0] && p.property_values[1].property_name === SIGN_PROPERTY_NAME), 'the sign is the second drop-down, metal and size are merged in the first');
  ok(tv.warnings.some(x => x.includes('"14k Solid Gold - 11mm"')), 'a merged label over 20 characters is flagged');
  const LM = { '14k Solid Gold|8.5mm': '14k Solid 8.5mm', '14k Solid Gold|11mm': '14k Solid 11mm' }, t2b = build(inv, { structure: 'two', labelMap: LM }); ok(!validate(inv, t2b, { labelMap: LM }).warnings.length && validate(inv, t2b, { labelMap: LM }).ok, 'short labels clear the warning');
  const t2c = clone(t2); t2c.body.products[0].property_values[0].values = ['Gold - 11mm']; ok(!validate(inv, t2c).ok, 'a merged label that is not metal + size is caught');
  console.log('  ' + n + ' checks passed');
}

/* ── command line ── */
function main(argv) {
  const arg = (k, d) => { const i = argv.indexOf(k); return i >= 0 ? argv[i + 1] : d; };
  const has = k => argv.includes(k);
  const read = f => JSON.parse(fs.readFileSync(path.resolve(f), 'utf8'));
  if (has('--selftest')) { selfTest(); return 0; }
  if (has('--print-fixture')) { process.stdout.write(JSON.stringify(fixture(), null, 2) + '\n'); return 0; }
  const outDir = arg('--out');
  const write = (name, text) => { if (outDir) { fs.mkdirSync(path.resolve(outDir), { recursive: true }); fs.writeFileSync(path.join(path.resolve(outDir), name), text); } };
  if (has('--restore-from')) {
    const backup = read(arg('--restore-from')), rb = buildRestore(backup), rv = validateRestore(backup, rb);
    const text = 'RESTORE DRY RUN: nothing was sent. products ' + rb.plan.products + ', query max_variations_supported=3, validation ' + (rv.ok ? 'PASSED' : 'FAILED') + '\n' + rv.failures.map(x => '  FAIL ' + x).join('\n');
    console.log(text); write('restore.json', JSON.stringify({ query: rb.query, body: rb.body }, null, 2)); write('restore-report.txt', text + '\n');
    return rv.ok ? 0 : 1;
  }
  if (has('--inventory')) {
    const inv = read(arg('--inventory')), built = build(inv, { structure: arg('--structure', 'three') }), v = validate(inv, built);
    const rb = buildRestore(inv), rv = validateRestore(inv, rb);
    let mf = [];
    if (arg('--master')) mf = checkMaster(read(arg('--master')));
    mf.forEach(x => v.failures.push('master: ' + x)); v.ok = v.failures.length === 0;
    const text = report(inv, built, v, rb, rv); console.log(text);
    write('payload.json', JSON.stringify({ query: built.query, body: built.body }, null, 2)); write('restore.json', JSON.stringify({ query: rb.query, body: rb.body }, null, 2)); write('report.txt', text + '\n');
    return v.ok && rv.ok ? 0 : 1;
  }
  console.error('usage: node scripts/zodiac-huggie-etsy-dryrun.cjs --selftest | --print-fixture | --inventory <get.json> [--structure three|two] [--master <skus.json>] [--out <dir>] | --restore-from <backup.json> [--out <dir>]');
  return 2;
}

if (require.main === module) process.exit(main(process.argv.slice(2)));
module.exports = { SIGNS, OLD_LISTING_SKU, SIGN_PROPERTY_NAME, CUSTOM_IDS, build, validate, buildRestore, validateRestore, checkMaster, fixture };
