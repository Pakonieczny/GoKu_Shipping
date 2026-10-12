/*  netlify/functions/_charmNestEtsyZodiac.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  DORMANT, GATED write path for Paul's ask of 12 Oct 2026 (order 4200256896, listing "Huggie Hoop Earrings with Astrological Charm ..."):
 *  add a THIRD drop-down, ZODIAC SIGN, with the twelve signs to that Etsy listing, every sign on its own SKU, no price change.
 *  Design and research: plans/zodiac-1012/ZODETSY-findings.md (sections 3 to 7); the offline builder this file copies is
 *  scripts/zodiac-huggie-etsy-dryrun.cjs (a test proves the two build the same payload). Nothing calls this by itself: it runs only when a person
 *  presses the two buttons in Settings (charm-nest-zodiac-etsy.js), through the Charm repo password door (op need "modify"), and the second
 *  button also needs the typed words "ADD ZODIAC DROPDOWN <listing number>".
 *
 *  THE DANGER (same as _charmNestEtsySku.js): Etsy's inventory PUT REPLACES THE WHOLE INVENTORY of the listing, and Etsy gives every product a new
 *  id. A wrong body changes what buyers pay or can buy, so nothing is sent until every step below has passed, and nothing is ever undone by itself.
 *
 *  ONE op, four stages (charmNestLibrary: zodiacListingPrepare, zodiacListingWrite, zodiacListingReconcile are gated "modify"; zodiacListingStatus
 *  reads only):
 *    prepare    reads Etsy (at most 2 calls: POST /scopes, GET inventory), writes NOTHING to Etsy. It builds the whole 72-product (real count) body with the
 *               builder and validators, keeps the before-copy in Storage (never overwritten), the body and a restore body beside it, and one small record
 *               (config/zodiacEtsy_{listingId}, written only by this file) with the hash of the prepared body. It answers a plain summary.
 *    write      refuses unless: not the sandbox; the typed words are exact; a prepared record for THIS listing and THIS hash exists, is ready and is not
 *               older than 12 h; nothing was sent before; the listing is active (the inbox mirror, 0 calls). It claims the record in ONE transaction (a
 *               lease: a second press is refused), reads the inventory once more (call 1) and goes on only if it still equals the before-copy and
 *               builds the very same body (same hash), and the before-copy, body and restore files are really in Storage. Then it hands the PUT to a
 *               BACKGROUND function (a 72-product PUT may take longer than a normal function lives) and answers "writing". Nothing else.
 *    execute    (charmNestZodiacEtsy-background) the ONE PUT, with ?max_variations_supported=3, after its own transaction writing -> putting (a second
 *               kick finds that done and sends nothing). Then Etsy's echo against what was sent. No second PUT, no undo, whatever it says.
 *    reconcile  one GET (call 1 of that press): what does Etsy hold now? After a write whose answer was lost, this settles it: the new inventory
 *               (written), the old one (not applied), or something else (mismatch, kept for a person).
 *  Refusals by Etsy: 401/403 = "needs listings_w" (nothing changed, no retry loop); 409 on the third variation = "Etsy has not enabled a third
 *  drop-down for this shop key" (nothing changed, no retry loop); 400 = Etsy's own words. A refusal never re-sends anything: a new check is needed.
 *
 *  The record's `state`: blocked | prepared | writing (claimed, PUT not started) | putting (PUT started) | written | mismatch (Etsy holds something
 *  that is not what was sent) | unknown (the PUT may or may not have landed) | refused (Etsy said no, nothing changed) | stale (the listing moved
 *  since the check, nothing sent). Only "prepared" can be written.
 *
 *  Dependencies are injected so a test runs it on fakes with no token:
 *    make({ db, FV, bucket, etsy: { get(path, query), put(path, query, body), scopes() }, kick({listingId, writeId}), meter, now, timeouts? })
 *  etsy.get/put/scopes resolve with Etsy's JSON and throw an Error with .status (and .data) on an HTTP error, exactly as etsyFetch does.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const crypto = require("crypto");
const Canon = require("./_etsyInventoryCanonical");
const Sku = require("./_charmNestEtsySku");
const ListingSkus = require("./_charmNestListingSkus");
const { decodeOnce, sanitizeProduct, compareInventories } = Sku.internals;

const CONFIG = "config", REC_PREFIX = "zodiacEtsy_", MIRROR = "EtsyMail_Listings", MASTER = "Charm_Master_Index";
const STORE = "charmnest/zodiac-1012/", SITE = "zodiac.listing", KICK_FN = "charmNestZodiacEtsy-background";
const LISTING_RE = /^\d{3,20}$/, HASH_RE = /^[0-9a-f]{64}$/;
const OLD_SKU = "HUGGIE HOOPS-ZODIAC";                    // the one SKU every product of the listing carries today (also how the listing is recognised)
const SIGN_PROPERTY_NAME = "ZODIAC SIGN";                 // upper case like METAL CHOICE and HOOP SIZE
const CUSTOM_IDS = [513, 514, 516];                       // Etsy's custom-variation property ids
const MAX_SKU = 32, MAX_ALL = 400, MAX_THREE = 2500, VALUE_LIMIT_SEEN = 20;
const LISTS = ["price_on_property", "quantity_on_property", "sku_on_property"];
const GET_MS = 15000, SCOPES_MS = 15000, PUT_MS = 100000;
const CLAIM_MS = 3 * 60000;           // the background function must have started the PUT within this of the press; else the claim is void (nothing was sent)
const PREPARED_MAX_MS = 12 * 3600000;  // a check older than this is not written from
const SETTLE_MS = 10 * 60000;         // a PUT that started less than this ago may still be in flight: "not applied" is not concluded before
const MAX_RECONCILES = 3;             // reads of "what does Etsy hold" per listing: a button, never a loop
const PREPARE_LEASE_MS = 60000;
const QUERY = Object.freeze({ max_variations_supported: 3 });

/* The twelve signs, in the order of the listing photo. `sku` is what each option carries on Etsy: the seven "repo" ones are the master's own huggie SKUs (checked
   against the live master on 12 Oct 2026), the five "new" ones exist nowhere yet (Paul adds their designs later and names each master entry like its Etsy SKU).
   Real Etsy SKUs of this shop already hold spaces and "(" ")" (e.g. "Squirrel 1 (eating)") and reach 32 characters, so these spellings are as safe as theirs. */
const SIGNS = Object.freeze([
  { label: "Aquarius", sku: "ZODIAC AQUARIUS (HUGGIE)", from: "new" },
  { label: "Pisces", sku: "ZODIAC DISCS PISCES (HUGGIE)", from: "repo" },
  { label: "Aries", sku: "ZODIAC ICONS ARIES (HUGGIE)", from: "repo" },
  { label: "Taurus", sku: "ZODIAC DISCS TAURUS (HUGGIE)", from: "repo" },
  { label: "Gemini", sku: "ZODIAC GEMINI (HUGGIE)", from: "new" },
  { label: "Cancer", sku: "ZODIAC CANCER (HUGGIE)", from: "new" },
  { label: "Leo", sku: "ZODIAC ICONS LEO (HUGGIE)", from: "repo" },
  { label: "Virgo", sku: "ZODIAC VIRGO (HUGGIE)", from: "repo" },
  { label: "Libra", sku: "ZODIAC ICONS LIBRA (HUGGIE)", from: "repo" },
  { label: "Scorpio", sku: "ZODIAC DISCS SCORPIO (HUGGIE)", from: "repo" },
  { label: "Sagittarius", sku: "ZODIAC SAGITTARIUS (HUGGIE)", from: "new" },
  { label: "Capricorn", sku: "ZODIAC CAPRICORN (HUGGIE)", from: "new" }
].map(Object.freeze));

/* ── every refusal in plain words (the Settings row shows `text`); `state` is the record's state afterwards when the answer is about a stored record ── */
const REASONS = {
  sandbox: "nothing is written to Etsy from the sandbox",
  "bad-listing": "that is not a listing number: paste the listing's number or its Etsy link",
  confirm: o => "the typed words are not exactly: " + (o.phrase || "ADD ZODIAC DROPDOWN <listing number>"),
  "not-prepared": "this listing has not been checked yet (or the check is no longer usable): press Check the listing first",
  "not-ready": "the check found problems, so nothing can be written (see the check)",
  hash: "the page holds an older check than the one stored: press Check the listing again",
  old: "the check is more than 12 hours old: press Check the listing again",
  already: "the zodiac drop-down was already written to this listing; nothing more is sent",
  "sent-before": "a change was already sent to Etsy for this listing and its result is not settled: press Check what Etsy holds",
  running: "a write for this listing is already running",
  "listing-state": o => "the listing is " + (o.listingState || "not active") + " on Etsy",
  changed: "the listing on Etsy is not what it was when it was checked (somebody changed it, or the drop-down is already there). Nothing was sent: press Check the listing again",
  "backup-failed": "the safety copy could not be confirmed in storage. Nothing was sent",
  scope: "Etsy refused: this connection needs the listings_w permission. Paul needs to re-authorise Etsy with listings_w. Nothing changed",
  "third-variation": "Etsy has not enabled a third drop-down for this shop's key. Nothing changed",
  conflict: "Etsy answered with a conflict and kept the listing as it was. Nothing changed",
  rejected: "Etsy did not accept the change and kept the listing as it was. Nothing changed",
  busy: "Etsy is busy: nothing was changed, try again in a minute",
  "etsy-down": "Etsy did not answer in time",
  offline: "could not reach Etsy",
  "listing-gone": "Etsy no longer has this listing",
  kick: "the Etsy step could not be started. Nothing was sent",
  unconfirmed: "Etsy answered, but not with the whole inventory: press Check what Etsy holds",
  "not-applied": "Etsy still holds the old inventory: the change did not land. Nothing was changed",
  "in-flight": "the change may still be on its way to Etsy: wait a few minutes, then press Check what Etsy holds",
  "no-more": "Etsy was asked three times already: a person should look at the listing",
  nothing: "there is nothing to check for this listing yet",
  prepare: "another check of this listing is running",
  internal: "the step was stopped by a safety check"
};
function answer(reason, o) {
  o = o || {};
  const r = REASONS[reason] || REASONS.internal;
  const out = { ok: false, reason: REASONS[reason] ? reason : "internal", text: typeof r === "function" ? r(o) : r, calls: o.calls | 0 };
  if (o.state) out.state = o.state;
  if (reason === "scope") out.needs = "listings_w";
  if (o.details && o.details.length) out.details = o.details.slice(0, 12).map(x => String(x).slice(0, 220));
  if (o.phrase) out.phrase = o.phrase;
  return out;
}
const isSandbox = b => !!b && (b.sandbox === true || b.sandbox === 1 || b.sandbox === "1" || b.sandbox === "true");

/* ── small helpers ── */
const sha256 = t => crypto.createHash("sha256").update(t).digest("hex");
const clone = x => JSON.parse(JSON.stringify(x));
const canonText = v => JSON.stringify(sortKeys(v));
function sortKeys(v) { if (Array.isArray(v)) return v.map(sortKeys); if (v && typeof v === "object") { const o = {}; for (const k of Object.keys(v).sort()) o[k] = sortKeys(v[k]); return o; } return v; }
const clean = v => {   // Firestore refuses undefined
  if (Array.isArray(v)) return v.map(clean);
  if (v && typeof v === "object") { const o = {}; for (const k of Object.keys(v)) if (v[k] !== undefined) o[k] = clean(v[k]); return o; }
  return v;
};
const ids = x => (Array.isArray(x) ? x : []).map(Number).filter(Number.isFinite);
const sameList = (a, b) => JSON.stringify(ids(a)) === JSON.stringify(ids(b));
const liveOf = inv => (Array.isArray(inv && inv.products) ? inv.products : []).filter(p => p && typeof p === "object" && !p.is_deleted);
const propOrder = inv => Canon.productPropertyOrder({ products: liveOf(inv) });
const looseKey = s => String(s == null ? "" : s).toUpperCase().replace(/[^A-Z0-9]+/g, "");
const skuNorm = s => decodeOnce(s == null ? "" : s).trim().toUpperCase();
const phraseFor = id => "ADD ZODIAC DROPDOWN " + id;
const recName = id => REC_PREFIX + id;
function withTimeout(fn, ms) {
  let t;
  const timer = new Promise((_, rej) => { t = setTimeout(() => rej(Object.assign(new Error("timeout"), { timeout: true })), ms); });
  return Promise.race([Promise.resolve().then(fn), timer]).finally(() => clearTimeout(t));
}
/** A listing number from what was typed or pasted: the digits, or the number inside an Etsy link ".../listing/123456789/...". */
function parseListingId(x) {
  const s = String(x == null ? "" : x).trim();
  const m = /\/listing\/(\d{3,20})(?:[/?#]|$)/.exec(s);
  const id = m ? m[1] : s;
  return LISTING_RE.test(id) ? id : "";
}

/* ═══ the builder (copied from scripts/zodiac-huggie-etsy-dryrun.cjs, "three" structure only; tests/charm-nest/zodiac-etsy-write.cjs proves the two agree) ═══ */
function build(inv) {
  const src = liveOf(inv), props = propOrder(inv);
  if (!src.length) throw new Error("the inventory has no live products");
  if (props.length !== 2) throw new Error("expected a listing with exactly two variation properties today, found " + props.length + " (" + props.join(", ") + ")");
  const thirdId = CUSTOM_IDS.find(i => !props.includes(i));
  if (!thirdId) throw new Error("no free custom property id among " + CUSTOM_IDS.join("/") + " (the listing uses " + props.join(", ") + ")");
  const all = [...props, thirdId], products = [];
  const sanitized = src.map(p => sanitizeProduct(p, ""));   // the app's own product shape: decoded text, decimal price, one offering, readiness kept
  for (const base of sanitized) for (const s of SIGNS) {
    const prod = clone(base);
    prod.sku = s.sku;
    prod.property_values.push({ property_id: thirdId, property_name: SIGN_PROPERTY_NAME, scale_id: null, value_ids: [], values: [s.label] });
    products.push(prod);
  }
  // *_on_property: Etsy takes zero, one or ALL of the properties (two of three is a 400) and, once one list holds them all, every other list is zero or all.
  const was = f => ids(inv[f]).length > 0;
  const body = { products, price_on_property: was("price_on_property") ? [...all] : [], quantity_on_property: was("quantity_on_property") ? [...all] : [], sku_on_property: [...all] };
  if (Array.isArray(inv.readiness_state_on_property)) body.readiness_state_on_property = ids(inv.readiness_state_on_property).length ? [...all] : [];
  return { query: { max_variations_supported: 3 }, body, plan: { sourceProducts: src.length, products: products.length, existingProperties: props, thirdPropertyId: thirdId } };
}

/* The validator, written apart from build() (it reads the source inventory directly) so one bug cannot hide in both. → { ok, failures, warnings } */
function validate(inv, built) {
  const f = [], w = [];
  const src = liveOf(inv), props = propOrder(inv), body = built.body || {}, products = Array.isArray(body.products) ? body.products : [];
  const nS = SIGNS.length, expected = src.length * nS, thirdId = built.plan && built.plan.thirdPropertyId, wantIds = [...props, thirdId];
  if (!built.query || built.query.max_variations_supported !== 3) f.push("the PUT must carry max_variations_supported=3 (without it Etsy answers 409 for a third variation)");
  for (const k of Object.keys(body)) if (![...["products"], ...LISTS, "readiness_state_on_property"].includes(k)) f.push("unexpected body field " + k);
  if (products.length !== expected) f.push("product count is " + products.length + ", expected " + src.length + " x " + nS + " = " + expected);
  const anyAll = LISTS.concat(["readiness_state_on_property"]).some(k => Array.isArray(body[k]) && body[k].length === wantIds.length);
  if (anyAll && products.length > MAX_ALL) f.push("more than " + MAX_ALL + " products (" + products.length + "): Etsy refuses a listing that varies by all properties above " + MAX_ALL);
  if (products.length > MAX_THREE) f.push("more than " + MAX_THREE + " products");
  const bySign = new Map(), combos = new Set(), perSign = new Map();
  products.forEach((p, i) => {
    const k = "product " + (i + 1);
    for (const key of Object.keys(p)) if (!["sku", "property_values", "offerings"].includes(key)) f.push(k + " has field " + key + " (ids and flags must not be sent)");
    if (typeof p.sku !== "string" || !p.sku.trim()) f.push(k + " has an empty SKU");
    else {
      if (p.sku.length > MAX_SKU) f.push(k + " SKU is " + p.sku.length + " characters (Etsy allows 32): " + p.sku);
      if (p.sku !== p.sku.trim()) f.push(k + " SKU has leading or trailing space");
    }
    const pv = Array.isArray(p.property_values) ? p.property_values : [];
    if (pv.length !== wantIds.length) f.push(k + " has " + pv.length + " property values, expected " + wantIds.length);
    for (const v of pv) {
      if (!Number.isInteger(v.property_id) || !Array.isArray(v.value_ids) || !Array.isArray(v.values) || !v.values.length) f.push(k + " has a malformed property value");
      for (const t of v.values || []) {
        if (typeof t !== "string" || !t.trim()) f.push(k + " has an empty option label");
        else {
          if (/[()]/.test(t)) f.push(k + " option label has parentheses, which Etsy refuses: " + t);
          if (t.length > VALUE_LIMIT_SEEN && !w.some(x => x.includes('"' + t + '"'))) w.push('option label "' + t + '" is ' + t.length + " characters (the longest value seen on the shop is 20; Etsy's limit is not documented)");
        }
      }
    }
    if (JSON.stringify(pv.map(v => v.property_id)) !== JSON.stringify(wantIds)) f.push(k + " property ids are " + JSON.stringify(pv.map(v => v.property_id)) + ", expected " + JSON.stringify(wantIds));
    const ck = JSON.stringify(pv.map(v => [v.property_id, v.values]));
    if (combos.has(ck)) f.push(k + " repeats an option combination: " + ck);
    combos.add(ck);
    const signLabel = pv.length ? (pv[pv.length - 1].values || [])[0] : "";
    const sign = SIGNS.find(s => s.label === signLabel);
    if (!sign) f.push(k + " carries a sign that is not one of the twelve: " + signLabel);
    else {
      perSign.set(sign.label, (perSign.get(sign.label) || 0) + 1);
      if (p.sku !== sign.sku) f.push(k + " (" + sign.label + ") SKU is " + JSON.stringify(p.sku) + ", expected " + JSON.stringify(sign.sku));
      if (!bySign.has(sign.label)) bySign.set(sign.label, new Set());
      bySign.get(sign.label).add(p.sku);
    }
  });
  for (const s of SIGNS) {
    if ((perSign.get(s.label) || 0) !== src.length) f.push(s.label + " appears on " + (perSign.get(s.label) || 0) + " products, expected " + src.length);
    if ((bySign.get(s.label) || new Set()).size > 1) f.push(s.label + " has more than one SKU");
  }
  if (new Set(SIGNS.map(s => s.sku)).size !== nS) f.push("the 12 signs do not have 12 different SKUs");
  if (new Set(SIGNS.map(s => looseKey(s.sku))).size !== nS) f.push("two signs have SKUs that differ only in spacing or punctuation (the app reads those as one SKU)");
  for (const s of SIGNS) if (looseKey(s.sku) === looseKey(OLD_SKU)) f.push(s.label + " SKU is the listing's old shared SKU");
  // the existing options, prices, stock and processing profile are exactly what was read (a second comparison per source product and sign)
  src.forEach((g, i) => {
    const go = Array.isArray(g.offerings) ? g.offerings[0] : null;
    if (!go) { f.push("source product " + (i + 1) + " has no offering"); return; }
    const enabled = go.is_enabled !== false, price = Canon.toDecimalPrice(go.price), q = enabled ? Number(go.quantity != null ? go.quantity : go.available_quantity != null ? go.available_quantity : 0) : 0;
    if (!(Number.isFinite(price) && price > 0)) f.push("source product " + (i + 1) + " price unreadable");
    SIGNS.forEach((s, j) => {
      const b = products[i * nS + j], k = "source product " + (i + 1) + " x " + s.label;
      if (!b) return;
      const o = Array.isArray(b.offerings) && b.offerings.length === 1 ? b.offerings[0] : null;
      if (!o) { f.push(k + ": offerings are not exactly one"); return; }
      if (o.price !== price) f.push(k + " price is " + o.price + ", was " + price);
      if (o.quantity !== q) f.push(k + " quantity is " + o.quantity + ", was " + q);
      if (o.is_enabled !== enabled) f.push(k + " enabled is " + o.is_enabled + ", was " + enabled);
      if (go.readiness_state_id != null ? o.readiness_state_id !== Number(go.readiness_state_id) : Object.prototype.hasOwnProperty.call(o, "readiness_state_id")) f.push(k + " processing profile (readiness_state_id) changed");
      for (const key of Object.keys(o)) if (!["price", "quantity", "is_enabled", "readiness_state_id"].includes(key)) f.push(k + " offering has field " + key);
      (g.property_values || []).forEach((v, n) => {
        const w2 = (b.property_values || [])[n] || {}, vals = (v.values || []).map(x => decodeOnce(String(x)));
        if (w2.property_id !== Number(v.property_id)) f.push(k + " option id changed");
        if (JSON.stringify(w2.values) !== JSON.stringify(vals)) f.push(k + " option text changed: " + JSON.stringify(w2.values) + " was " + JSON.stringify(vals));
        if (JSON.stringify(w2.value_ids) !== JSON.stringify(Array.isArray(v.value_ids) && v.value_ids.length === vals.length ? v.value_ids : [])) f.push(k + " option value ids changed");
        if ((w2.scale_id == null ? null : w2.scale_id) !== (v.scale_id == null ? null : v.scale_id)) f.push(k + " scale changed");
        if (v.property_name != null && w2.property_name !== decodeOnce(String(v.property_name))) f.push(k + " option name changed");
      });
      const t = (b.property_values || [])[(g.property_values || []).length] || {};
      if (t.property_id !== thirdId || t.property_name !== SIGN_PROPERTY_NAME || t.scale_id !== null || JSON.stringify(t.value_ids) !== "[]" || JSON.stringify(t.values) !== JSON.stringify([s.label])) f.push(k + " third property is not the " + SIGN_PROPERTY_NAME + " custom option for " + s.label);
    });
  });
  const sum = a => Math.round(a.reduce((x, y) => x + y, 0) * 100) / 100;
  const oldSum = sum(src.map(g => Canon.toDecimalPrice(g.offerings[0].price))), newSum = sum(products.map(p => p.offerings && p.offerings[0] && p.offerings[0].price));
  if (Math.round(oldSum * nS * 100) / 100 !== newSum) f.push("the price total is " + newSum + ", expected " + nS + " x " + oldSum);
  if (!CUSTOM_IDS.includes(thirdId)) w.push("the third property id " + thirdId + " is not one of Etsy's custom ids " + CUSTOM_IDS.join("/"));
  if (props.includes(thirdId)) f.push("the third property id is already used by this listing");
  const total = wantIds.length;
  for (const key of LISTS.concat(["readiness_state_on_property"])) {
    if (!(key in body)) { if (key !== "readiness_state_on_property") f.push(key + " is missing"); else if (Array.isArray(inv.readiness_state_on_property)) f.push(key + " was read and must be sent back"); continue; }
    const l = body[key];
    if (!Array.isArray(l)) { f.push(key + " is not a list"); continue; }
    if (l.some(x => !wantIds.includes(x))) f.push(key + " names a property the listing does not have");
    if (new Set(l).size !== l.length) f.push(key + " repeats a property");
    if (total === 3 && l.length === 2) f.push(key + ' has exactly two of three properties (Etsy: "supports only zero, one, or all 3 variation properties")');
    if (l.length && JSON.stringify(l) !== JSON.stringify(wantIds.filter(x => l.includes(x)))) f.push(key + " is not in the order the properties appear in the products");
  }
  const lens = LISTS.concat(Array.isArray(body.readiness_state_on_property) ? ["readiness_state_on_property"] : []).map(k => (body[k] || []).length);
  if (lens.some(n => n === total) && lens.some(n => n > 0 && n < total)) f.push("one *_on_property list holds every property while another holds only some (Etsy refuses it)");
  if (ids(inv.price_on_property).length && !sameList(body.price_on_property, wantIds)) f.push("price varied by property before; it must now be listed on all " + total);
  if (!sameList(body.sku_on_property, wantIds)) f.push("sku_on_property must list all " + total + " properties (the SKU varies by sign)");
  for (const key of ["price", "quantity"]) {   // the declared lists must be true of the products
    const declared = ids(body[key + "_on_property"]), seen = new Map();
    for (const p of products) {
      const gk = JSON.stringify((p.property_values || []).filter(v => declared.includes(v.property_id)).map(v => [v.property_id, v.values]));
      const val = key === "price" ? p.offerings[0].price : p.offerings[0].quantity;
      if (seen.has(gk) && seen.get(gk) !== val) { f.push(key + "_on_property does not cover how " + key + " really varies"); break; }
      seen.set(gk, val);
    }
  }
  return { ok: f.length === 0, failures: f, warnings: w };
}

/* What this particular listing must also be before anything is built from it (the identity of the listing and what Etsy needs on every offering). → [reasons] */
function listingProblems(inv) {
  const bad = [], live = liveOf(inv);
  if (!live.length) return ["Etsy sent no products for this listing"];
  const props = propOrder(inv);
  if (props.length === 3) bad.push("this listing already has three drop-downs");
  else if (props.length !== 2) bad.push("this listing has " + props.length + " drop-down(s); the change is made only for a listing with exactly two");
  const odd = live.filter(p => skuNorm(p.sku) !== OLD_SKU);
  if (odd.length) bad.push(odd.length + " of " + live.length + " products do not carry the SKU " + OLD_SKU + " (found: " + [...new Set(odd.map(p => decodeOnce(p.sku == null ? "" : p.sku) || "(none)"))].slice(0, 4).join(", ") + "): this does not look like the zodiac huggie hoop listing");
  const noReady = live.filter(p => !(p.offerings && p.offerings[0] && p.offerings[0].readiness_state_id != null));
  if (noReady.length) bad.push(noReady.length + " of " + live.length + " products came without their processing profile, which Etsy wants on every offering");
  if (live.length * SIGNS.length > MAX_ALL) bad.push(live.length + " products x 12 signs is over Etsy's limit of " + MAX_ALL);
  return bad;
}

/* the restore body: the listing as the before-copy had it (same options, prices, stock, processing profile, the shared SKU again, the original lists).
   It needs ?max_variations_supported=3 (the listing has three variations by then). Never sent by this file. */
function restoreOf(before) {
  const src = liveOf(before), props = propOrder(before);
  const norm = Sku.internals.normalizeOnProperty(before, props);
  const body = { products: src.map(p => sanitizeProduct(p, decodeOnce(p.sku == null ? "" : p.sku))) };
  for (const k of LISTS) body[k] = norm[k];
  if (Array.isArray(before.readiness_state_on_property)) body.readiness_state_on_property = ids(before.readiness_state_on_property);
  return { query: { max_variations_supported: 3 }, body };
}

/* ── the plain summary of an inventory and what would be written ── */
const label = p => (Array.isArray(p.property_values) ? p.property_values : []).map(v => (v.values || []).map(x => decodeOnce(String(x))).join("/")).join(" / ");
function describe(inv, built, v, extra) {
  const live = liveOf(inv), props = propOrder(inv);
  const options = props.map(id => {
    const vals = [];
    let name = "";
    for (const p of live) for (const pv of p.property_values || []) if (Number(pv.property_id) === id) {
      if (!name && pv.property_name) name = decodeOnce(String(pv.property_name));
      for (const t of pv.values || []) { const d = decodeOnce(String(t)); if (!vals.includes(d)) vals.push(d); }
    }
    return { id, name, values: vals };
  });
  const rows = live.map(p => ({ label: label(p), price: Canon.toDecimalPrice(p.offerings && p.offerings[0] && p.offerings[0].price), enabled: !(p.offerings && p.offerings[0] && p.offerings[0].is_enabled === false), quantity: (p.offerings && p.offerings[0] && (p.offerings[0].quantity != null ? p.offerings[0].quantity : p.offerings[0].available_quantity)) | 0 }));
  const prices = rows.map(r => r.price).filter(Number.isFinite);
  return Object.assign({
    options, productsBefore: live.length, productsAfter: built ? built.body.products.length : 0,
    rows: rows.slice(0, 60), prices: { min: prices.length ? Math.min(...prices) : null, max: prices.length ? Math.max(...prices) : null },
    pricesEqual: v ? !v.failures.some(x => /price/i.test(x)) : false,
    thirdPropertyId: built ? built.plan.thirdPropertyId : null, query: "max_variations_supported=3",
    onProperty: built ? { price: built.body.price_on_property, quantity: built.body.quantity_on_property, sku: built.body.sku_on_property } : null,
    signs: SIGNS.map(s => ({ sign: s.label, sku: s.sku, from: s.from }))
  }, extra || {});
}

/* ═══ the module ═══ */
function make(deps) {
  const { db, FV, bucket, etsy, meter } = deps;
  const clock = typeof deps.now === "function" ? deps.now : Date.now;
  const T = Object.assign({ get: GET_MS, scopes: SCOPES_MS, put: PUT_MS }, deps.timeouts || {});
  const ref = id => db.collection(CONFIG).doc(recName(id));
  void FV;

  /* one Etsy call, counted: `run` is a per-stage counter { n, max } so no stage can ever make more calls than it promises */
  function caller(max) {
    const run = { n: 0, max };
    run.call = (fn, ms) => {
      if (run.n >= run.max) return Promise.reject(Object.assign(new Error("call cap"), { cap: true }));
      run.n++;
      try { meter && meter.bumpSimple(SITE); } catch (_) { /* a counter never stops the work */ }
      return withTimeout(fn, ms);
    };
    return run;
  }
  const flush = async () => { if (meter && typeof meter.flushNow === "function") { try { await meter.flushNow(); } catch (_) { /* best effort */ } } };

  /* an Etsy error → its reason (before a PUT, "nothing changed" is true of every one; after a PUT, a timeout or 5xx is ambiguous) */
  function classify(e) {
    if (e && e.cap) return { reason: "internal" };
    if (e && e.timeout) return { reason: "etsy-down", ambiguous: true };
    const msg = String((e && e.message) || "");
    let st = +(e && e.status) || 0;
    if (!st) { const m = /refresh failed:\s*(\d{3})/i.exec(msg); if (m) st = +m[1]; else if (/not seeded|No refresh_token/i.test(msg)) st = 401; }
    const text = e && e.data && typeof e.data.error === "string" ? e.data.error : "";
    const details = text ? [text] : [];
    if (st === 401 || st === 403) return { reason: "scope", details };
    if (st === 404 || st === 410) return { reason: "listing-gone", details };
    if (st === 409) return { reason: /variation/i.test(text + " " + msg) ? "third-variation" : "conflict", details };
    if (st === 429) return { reason: "busy", details };
    if (st >= 500) return { reason: "etsy-down", ambiguous: true, details };
    if (st >= 400) return { reason: "rejected", details };
    return { reason: "offline", ambiguous: true, details };
  }

  async function readRec(listingId) {
    const s = await ref(listingId).get();
    return s.exists ? s.data() || {} : null;
  }
  async function mirror(listingId) {
    try {
      const s = await db.collection(MIRROR).doc(listingId).get();
      const d = s.exists ? s.data() || {} : {};
      return { state: typeof d.state === "string" && d.state.trim() ? d.state.trim().toLowerCase() : null, title: typeof d.title === "string" ? decodeOnce(d.title).slice(0, 160) : "" };
    } catch (_) { return { state: null, title: "" }; }
  }
  /* the twelve SKUs against the live master (Firestore reads, no Etsy call): exact document ids */
  async function masterCheck() {
    try {
      const snaps = await db.getAll(...SIGNS.map(s => db.collection(MASTER).doc(s.sku.toUpperCase())));
      return SIGNS.map((s, i) => ({ sign: s.label, sku: s.sku, from: s.from, inMaster: !!snaps[i].exists }));
    } catch (e) { console.warn("[etsyZodiac] master not read:", e && e.message); return null; }
  }

  /* Storage: nothing is ever overwritten. A path that exists must hold the same text. */
  async function saveOnce(path, text) {
    const f = bucket.file(path);
    if ((await f.exists())[0]) {
      const have = (await f.download())[0].toString("utf8");
      if (have !== text) throw new Error("a different file is already stored at " + path);
      return { path, existed: true };
    }
    await f.save(Buffer.from(text, "utf8"), { resumable: false, contentType: "application/json", metadata: { cacheControl: "no-store" } });
    if (!(await f.exists())[0]) throw new Error("the file is not in Storage after the save: " + path);
    return { path, existed: false };
  }
  /* the before-copy: the first free name; a name already holding THIS copy is reused (a repeated check of an unchanged listing adds no file) */
  async function saveBefore(listingId, text) {
    for (let i = 1; i <= 8; i++) {
      const path = STORE + listingId + "-before" + (i > 1 ? "-" + i : "") + ".json", f = bucket.file(path);
      if (!(await f.exists())[0]) { await saveOnce(path, text); return path; }
      if ((await f.download())[0].toString("utf8") === text) return path;
    }
    throw new Error("no free name for the safety copy");
  }
  async function readFileText(path) { return (await bucket.file(path).download())[0].toString("utf8"); }

  /* ═══ STAGE 1 · prepare ═══ */
  async function prepare(body) {
    body = body && typeof body === "object" ? body : {};
    if (isSandbox(body)) return answer("sandbox");
    const listingId = parseListingId(body.listingId);
    if (!listingId) return answer("bad-listing");
    const by = String(body.by == null ? "" : body.by).trim().slice(0, 80), phrase = phraseFor(listingId), now = clock(), r = ref(listingId);
    // a lease, so two presses cannot run two checks at once; a record whose write was sent is never overwritten by a check
    let got;
    try {
      got = await db.runTransaction(async tx => {
        const s = await tx.get(r), d = s.exists ? s.data() || {} : null;
        if (d) {
          if (d.state === "written" || d.state === "mismatch") return { refusal: "already" };
          if (d.state === "putting" || d.state === "unknown" || (d.state === "writing" && d.write && d.write.putAt)) return { refusal: "sent-before" };
          if (d.state === "writing" && d.write && +d.write.runningUntil > now) return { refusal: "running" };
          if (+d.prepLease > now) return { refusal: "prepare" };
          tx.update(r, { prepLease: now + PREPARE_LEASE_MS });
        } else tx.set(r, { v: 1, listingId, state: "none", prepLease: now + PREPARE_LEASE_MS, createdAt: now });
        return { d };
      });
    } catch (e) { got = { error: e }; }
    if (got.error) { console.error("[etsyZodiac] prepare claim", listingId, got.error && got.error.message); return answer("internal", { details: [got.error && got.error.message] }); }
    if (got.refusal) return answer(got.refusal, { phrase });
    const run = caller(2);
    const done = async (a, patch) => {   // every exit: the lease is released and (when something was learned) the record says it
      try { await r.update(clean(Object.assign({ prepLease: 0 }, patch || {}))); } catch (e) { try { await r.set(clean(Object.assign({ v: 1, listingId, prepLease: 0 }, patch || {})), { merge: true }); } catch (e2) { console.warn("[etsyZodiac] record not written", listingId, e2 && e2.message); } }
      await flush();
      return Object.assign(a, { calls: run.n });
    };
    try {
      const mir = await mirror(listingId);
      const reasons = [], warnings = [];
      if (mir.state && mir.state !== "active") reasons.push("the listing is " + mir.state.replace(/[^a-z_ ]/g, "").slice(0, 20) + " on Etsy (only an active listing is written)");
      const master = await masterCheck();
      if (!master) warnings.push("the repo's master list could not be read, so the twelve SKUs were not compared with it");
      else {
        const gone = master.filter(m => m.from === "repo" && !m.inMaster).map(m => m.sku);
        if (gone.length) reasons.push("these SKUs are meant to match the repo but are not in the master: " + gone.join(", "));
      }

      // Etsy call 1 of 2: what the connection may do
      let scopes = { checked: false, listings_w: null, list: [] };
      try {
        const sc = await run.call(() => etsy.scopes(), T.scopes);
        const list = Array.isArray(sc && sc.scopes) ? sc.scopes.map(String) : typeof (sc && sc.scope) === "string" ? sc.scope.split(/\s+/).filter(Boolean) : null;
        if (list) { scopes = { checked: true, listings_w: list.includes("listings_w"), list: list.slice(0, 30) }; if (!scopes.listings_w) reasons.push("the Etsy connection does not have the listings_w permission: Paul needs to re-authorise Etsy with listings_w"); }
        else warnings.push("Etsy's answer about the connection's permissions was not understood, so listings_w was not confirmed");
      } catch (e) {
        const c = classify(e);
        if (c.reason === "scope") { reasons.push("Etsy refused the connection (401/403): it needs re-authorising with listings_w"); scopes = { checked: true, listings_w: false, list: [], refused: true }; }
        else warnings.push("the connection's permissions could not be checked (" + c.reason + ")");
      }

      // Etsy call 2 of 2: the inventory (skipped when Etsy already refused the connection: the same refusal would come back)
      let inv = null;
      if (!(scopes.refused)) {
        try { inv = await run.call(() => etsy.get("/listings/" + listingId + "/inventory", {}), T.get); }
        catch (e) { const c = classify(e); reasons.push(c.reason === "listing-gone" ? "Etsy has no such listing" : c.reason === "scope" ? "Etsy refused the connection (401/403)" : "the inventory could not be read (" + c.reason + ")"); }
      }
      if (inv && !Array.isArray(inv.products)) { reasons.push("Etsy answered without an inventory"); inv = null; }
      if (!inv) {
        const out = answer("not-ready", { state: "blocked", details: reasons });
        out.listingId = listingId; out.phrase = phrase; out.reasons = reasons.slice(0, 12); out.warnings = warnings;
        return await done(out, { state: "blocked", ready: false, reasons: reasons.slice(0, 12), warnings, preparedAt: now, preparedBy: by, summary: { scopes, listingState: mir.state, title: mir.title } });
      }

      // the facts of this listing, then the body
      reasons.push(...listingProblems(inv));
      let built = null, v = null;
      if (!reasons.some(x => /drop-down|no products/.test(x))) {
        try { built = build(inv); v = validate(inv, built); } catch (e) { reasons.push(String((e && e.message) || e).slice(0, 220)); }
        if (v) { reasons.push(...v.failures.slice(0, 12)); warnings.push(...v.warnings.slice(0, 8)); }
      }
      const beforeText = JSON.stringify(inv), beforeHash = sha256(canonText(inv));
      let paths = {}, payloadHash = "";
      if (built) {
        const payloadText = JSON.stringify({ query: built.query, body: built.body });
        payloadHash = sha256(payloadText);
        if (reasons.length === 0) {   // files are kept only for a listing that passed every check
          try {
            paths.before = await saveBefore(listingId, beforeText);
            paths.payload = (await saveOnce(STORE + listingId + "-payload-" + payloadHash.slice(0, 12) + ".json", payloadText)).path;
            paths.restore = (await saveOnce(STORE + listingId + "-restore-" + beforeHash.slice(0, 12) + ".json", JSON.stringify(restoreOf(inv)))).path;
          } catch (e) {
            console.warn("[etsyZodiac] files not saved for", listingId, e && e.message);
            reasons.push("the safety copy could not be saved in storage (" + String((e && e.message) || e).slice(0, 120) + ")");
          }
        }
      }
      const finalReady = !!built && reasons.length === 0 && !!paths.before && !!paths.payload && !!paths.restore && reasons.length === 0;
      const summary = describe(inv, built, v, { scopes, listingState: mir.state, title: mir.title, master, identity: { sharedSku: OLD_SKU }, restoreChecked: !!paths.restore });
      const state = finalReady ? "prepared" : "blocked";
      const rec = { v: 1, listingId, state, ready: finalReady, reasons: reasons.slice(0, 14), warnings: warnings.slice(0, 10), payloadHash, beforeHash, paths, summary, preparedAt: now, preparedBy: by, calls: run.n, write: null, result: null };
      const out = { ok: true, state, ready: finalReady, listingId, phrase, payloadHash, summary, reasons: rec.reasons, warnings: rec.warnings, paths, calls: run.n };
      return await done(out, rec);
    } catch (e) {
      console.error("[etsyZodiac] prepare", listingId, e && e.message);
      return await done(answer("internal", { details: [e && e.message] }), {});
    }
  }

  /* ═══ STAGE 2 · write (everything up to the hand-off to the background function) ═══ */
  async function write(body) {
    body = body && typeof body === "object" ? body : {};
    if (isSandbox(body)) return answer("sandbox");
    const listingId = parseListingId(body.listingId);
    if (!listingId) return answer("bad-listing");
    const phrase = phraseFor(listingId);
    if (String(body.confirm == null ? "" : body.confirm).trim() !== phrase) return answer("confirm", { phrase });
    const hash = String(body.payloadHash == null ? "" : body.payloadHash);
    if (!HASH_RE.test(hash)) return answer("hash");
    const by = String(body.by == null ? "" : body.by).trim().slice(0, 80), now = clock(), r = ref(listingId), writeId = crypto.randomBytes(16).toString("hex");
    let got;
    try {
      got = await db.runTransaction(async tx => {
        const s = await tx.get(r);
        if (!s.exists) return { refusal: "not-prepared" };
        const d = s.data() || {};
        if (d.state === "written" || d.state === "mismatch") return { refusal: "already" };
        if (d.state === "putting" || d.state === "unknown") return { refusal: "sent-before" };
        if (d.state === "writing") {
          if (d.write && d.write.putAt) return { refusal: "sent-before" };
          if (d.write && +d.write.runningUntil > now) return { refusal: "running" };
          // a claim that expired without a PUT ever starting: nothing was sent, the check stands
        } else if (d.state === "blocked") return { refusal: "not-ready" };
        else if (d.state !== "prepared") return { refusal: "not-prepared" };
        if (d.ready !== true) return { refusal: "not-ready" };
        if (d.payloadHash !== hash) return { refusal: "hash" };
        if (!(now - (+d.preparedAt || 0) <= PREPARED_MAX_MS)) return { refusal: "old" };
        tx.update(r, clean({ state: "writing", write: { id: writeId, by, claimedAt: now, runningUntil: now + CLAIM_MS, putAt: null, kickedAt: null }, result: null }));
        return { d };
      });
    } catch (e) { console.error("[etsyZodiac] claim", listingId, e && e.message); return answer("internal", { details: [e && e.message] }); }
    if (got.refusal) return answer(got.refusal, { phrase });
    const d = got.d, run = caller(1);

    /* nothing has been sent from here to the kick; any stop puts the record where it belongs (conditionally: only while this claim is still the live one) */
    const stop = async (reason, o, newState, patch) => {
      await settleClaim(listingId, writeId, newState, Object.assign({ lastRefusal: { reason, at: clock() } }, patch || {}));
      await flush();
      return answer(reason, Object.assign({ calls: run.n, state: newState }, o));
    };
    try {
      const mir = await mirror(listingId);
      if (mir.state && mir.state !== "active") return await stop("listing-state", { listingState: mir.state.replace(/[^a-z_ ]/g, "").slice(0, 20) }, "prepared");

      // Etsy call 1: the inventory as it is now
      let fresh;
      try { fresh = await run.call(() => etsy.get("/listings/" + listingId + "/inventory", {}), T.get); }
      catch (e) { const c = classify(e); return await stop(c.reason, { details: c.details }, c.reason === "scope" ? "refused" : "prepared"); }
      if (!fresh || !Array.isArray(fresh.products)) return await stop("changed", {}, "stale");
      if (sha256(canonText(fresh)) !== d.beforeHash) return await stop("changed", {}, "stale");

      // the body built from it is the body that was checked
      let built, v;
      try { built = build(fresh); v = validate(fresh, built); } catch (e) { return await stop("internal", { details: [e && e.message] }, "stale"); }
      if (!v.ok || listingProblems(fresh).length) return await stop("internal", { details: v.failures.concat(listingProblems(fresh)) }, "stale");
      if (sha256(JSON.stringify({ query: built.query, body: built.body })) !== d.payloadHash) return await stop("changed", {}, "stale");

      // the safety copy, the body and the restore body are really in Storage, and the copy is the very listing just read
      try {
        const p = d.paths || {};
        if (!p.before || !p.payload || !p.restore) throw new Error("a file path is missing on the record");
        const before = await readFileText(p.before);
        if (sha256(canonText(JSON.parse(before))) !== d.beforeHash) throw new Error("the stored before-copy is not the checked listing");
        if (sha256(await readFileText(p.payload)) !== d.payloadHash) throw new Error("the stored body is not the checked body");
        const rb = JSON.parse(await readFileText(p.restore));
        if (!rb || !rb.body || !Array.isArray(rb.body.products) || rb.body.products.length !== liveOf(fresh).length) throw new Error("the stored restore body is not usable");
      } catch (e) {
        console.warn("[etsyZodiac] backup not confirmed for", listingId, e && e.message);
        return await stop("backup-failed", { details: [e && e.message] }, "stale");
      }

      // hand the PUT to the background function. The claim already says writing; the PUT cannot start unless the background function wins ITS transaction.
      await patchWrite(listingId, writeId, { kickedAt: clock(), getCalls: run.n });
      try { await deps.kick({ listingId, writeId }); }
      catch (e) {
        console.warn("[etsyZodiac] kick failed for", listingId, e && e.message);
        return await stop("kick", { details: [e && e.message] }, "prepared");
      }
      await flush();
      return { ok: true, state: "writing", listingId, calls: run.n };
    } catch (e) {
      console.error("[etsyZodiac] write", listingId, e && e.message);
      return await stop("internal", { details: [e && e.message] }, "prepared");
    }
  }

  /* a note on the live claim's `write` map (status line only); never changes the state */
  async function patchWrite(listingId, writeId, patch) {
    try {
      await db.runTransaction(async tx => {
        const r = ref(listingId), s = await tx.get(r), d = s.exists ? s.data() || {} : null;
        if (!d || d.state !== "writing" || !d.write || d.write.id !== writeId || d.write.putAt) return;
        tx.update(r, clean({ write: Object.assign({}, d.write, patch) }));
      });
    } catch (e) { console.warn("[etsyZodiac] note not written for", listingId, e && e.message); }
  }

  /* put the record where a stop belongs, but only while this claim is still the live one and no PUT has started */
  async function settleClaim(listingId, writeId, newState, patch) {
    try {
      await db.runTransaction(async tx => {
        const r = ref(listingId), s = await tx.get(r), d = s.exists ? s.data() || {} : null;
        if (!d || d.state !== "writing" || !d.write || d.write.id !== writeId || d.write.putAt) return;
        tx.update(r, clean(Object.assign({ state: newState, write: null }, patch || {})));
      });
    } catch (e) { console.warn("[etsyZodiac] claim not released for", listingId, e && e.message); }
  }

  /* ═══ the ONE PUT · run by charmNestZodiacEtsy-background ═══ */
  async function execute(body) {
    body = body && typeof body === "object" ? body : {};
    const listingId = parseListingId(body.listingId), writeId = String(body.writeId == null ? "" : body.writeId);
    if (!listingId || !/^[0-9a-f]{32}$/.test(writeId)) return { ok: false, skipped: "bad-request" };
    const r = ref(listingId), run = caller(1);
    let d;
    try { d = await readRec(listingId); } catch (e) { return { ok: false, skipped: "unreadable" }; }
    if (!d || d.state !== "writing" || !d.write || d.write.id !== writeId || d.write.putAt) return { ok: false, skipped: "not-claimed" };
    const fail = async (reason, o, newState) => { await settleClaim(listingId, writeId, newState || "refused", { result: { ok: false, reason, text: answer(reason, o).text, at: clock(), details: (o && o.details) || [] } }); return { ok: false, reason, state: newState || "refused" }; };
    // the body to send is the stored one, and it must be the checked one
    let payload;
    try {
      const text = await readFileText((d.paths || {}).payload);
      if (sha256(text) !== d.payloadHash) throw new Error("the stored body is not the checked body");
      payload = JSON.parse(text);
      if (!payload || !payload.body || !Array.isArray(payload.body.products) || payload.body.products.length !== (d.summary || {}).productsAfter || !payload.query || payload.query.max_variations_supported !== 3) throw new Error("the stored body is not complete");
    } catch (e) { return await fail("internal", { details: [e && e.message] }, "stale"); }
    // the transaction that decides who may send: writing -> putting, once
    const t0 = clock();
    let claimed = false;
    try {
      claimed = await db.runTransaction(async tx => {
        const s = await tx.get(r), x = s.exists ? s.data() || {} : null;
        if (!x || x.state !== "writing" || !x.write || x.write.id !== writeId || x.write.putAt || !(+x.write.runningUntil > t0)) return false;
        tx.update(r, clean({ state: "putting", write: Object.assign({}, x.write, { putAt: t0, runningUntil: 0 }) }));
        return true;
      });
    } catch (e) { return { ok: false, skipped: "claim-failed" }; }
    if (!claimed) return { ok: false, skipped: "not-claimed" };

    // the one PUT
    let echo = null, err = null;
    try { echo = await run.call(() => etsy.put("/listings/" + listingId + "/inventory", payload.query, payload.body), T.put); } catch (e) { err = e; }
    const finish = async (state, result, extra) => {
      try { await r.update(clean(Object.assign({ state, result: Object.assign({ at: clock() }, result) }, extra || {}))); } catch (e) { console.error("[etsyZodiac] RESULT NOT RECORDED", listingId, state, e && e.message); }
      await flush();
    };
    if (err) {
      const c = classify(err);
      if (c.ambiguous) { await finish("unknown", { ok: null, reason: c.reason, text: answer(c.reason).text + ". The change may or may not have landed: press Check what Etsy holds", details: c.details || [] }); return { ok: false, state: "unknown", reason: c.reason }; }
      await finish("refused", { ok: false, reason: c.reason, text: answer(c.reason).text, details: c.details || [] });
      return { ok: false, state: "refused", reason: c.reason };
    }
    // Etsy's own answer against what was sent. No second PUT and no undo, whatever it says.
    if (!wholeInventory(echo, payload.body.products.length)) {
      await finish("unknown", { ok: null, reason: "unconfirmed", text: answer("unconfirmed").text, details: [] });
      return { ok: false, state: "unknown", reason: "unconfirmed" };
    }
    const cmp = compareInventories(payload.body, echo);
    if (!cmp.ok) {
      await finish("mismatch", { ok: false, verified: false, reason: "mismatch", text: "Etsy accepted the change but its listing is not exactly what was sent. The safety copy and the restore body are kept; nothing was undone", details: cmp.differences.slice(0, 12) });
      return { ok: false, state: "mismatch", details: cmp.differences };
    }
    const afterHash = sha256(canonText(echo));
    await finish("written", { ok: true, verified: true, products: payload.body.products.length, afterHash, text: "Written and checked against Etsy's own answer" });
    await afterWrite(listingId, echo, writeId);
    return { ok: true, state: "written", verified: true, products: payload.body.products.length, calls: run.n };
  }
  function wholeInventory(inv, n) {
    const live = liveOf(inv);
    return live.length > 0 && live.every(p => p && Array.isArray(p.offerings) && p.offerings.length > 0 && p.offerings[0].price != null && Object.prototype.hasOwnProperty.call(p.offerings[0], "is_enabled"));
  }
  /* what is kept after a verified write: the listing's SKU table refreshed from Etsy's own answer (so the sorter sees the new SKUs and option names at once, no extra Etsy call), and a copy of the answer */
  async function afterWrite(listingId, echo, writeId) {
    try { await db.collection(ListingSkus.COLL).doc(listingId).set(clean(ListingSkus.toStored(ListingSkus.compact(echo, clock())))); }
    catch (e) { console.warn("[etsyZodiac] SKU table not refreshed for", listingId, e && e.message); }
    try { await saveOnce(STORE + listingId + "-after-" + String(writeId).slice(0, 8) + ".json", JSON.stringify(echo)); }
    catch (e) { console.warn("[etsyZodiac] after-copy not saved for", listingId, e && e.message); }
  }

  /* ═══ reconcile · one read of what Etsy holds now, for a write whose answer was lost ═══ */
  async function reconcile(body) {
    body = body && typeof body === "object" ? body : {};
    if (isSandbox(body)) return answer("sandbox");
    const listingId = parseListingId(body.listingId);
    if (!listingId) return answer("bad-listing");
    const now = clock(), r = ref(listingId);
    let d;
    try { d = await readRec(listingId); } catch (e) { return answer("internal", { details: [e && e.message] }); }
    if (!d) return answer("nothing");
    if (d.state === "writing" && !(d.write && d.write.putAt)) {
      if (d.write && +d.write.runningUntil > now) return answer("running", { state: "writing" });
      await settleClaim(listingId, d.write && d.write.id, "prepared", {});   // an expired claim whose PUT never started: nothing was sent
      return { ok: true, state: "prepared", calls: 0, text: "Nothing had been sent to Etsy: the check stands." };
    }
    if (!(d.state === "putting" || d.state === "unknown")) return answer(d.state === "written" || d.state === "mismatch" ? "already" : "nothing", { state: d.state });
    if ((+d.reconciles || 0) >= MAX_RECONCILES) return answer("no-more", { state: d.state });
    const putAt = +(d.write && d.write.putAt) || 0, run = caller(1);
    try {
      await r.update(clean({ reconciles: (+d.reconciles || 0) + 1 }));
      let live;
      try { live = await run.call(() => etsy.get("/listings/" + listingId + "/inventory", {}), T.get); }
      catch (e) { const c = classify(e); await flush(); return answer(c.reason, { calls: run.n, state: d.state, details: c.details }); }
      await flush();
      if (!live || !Array.isArray(live.products)) return answer("unconfirmed", { calls: run.n, state: d.state });
      const payload = JSON.parse(await readFileText((d.paths || {}).payload));
      const cmp = compareInventories(payload.body, live);
      if (cmp.ok) {
        await r.update(clean({ state: "written", result: { at: clock(), ok: true, verified: true, by: "check", products: payload.body.products.length, afterHash: sha256(canonText(live)), text: "Written: Etsy now holds exactly what was sent" } }));
        await afterWrite(listingId, live, (d.write && d.write.id) || "check");
        return { ok: true, state: "written", verified: true, calls: run.n };
      }
      if (sha256(canonText(live)) === d.beforeHash) {
        if (now - putAt < SETTLE_MS) return answer("in-flight", { calls: run.n, state: d.state });
        await r.update(clean({ state: "refused", result: { at: clock(), ok: false, reason: "not-applied", text: answer("not-applied").text, details: [] } }));
        return Object.assign(answer("not-applied", { calls: run.n, state: "refused" }), { settled: true });
      }
      await r.update(clean({ state: "mismatch", result: { at: clock(), ok: false, verified: false, reason: "mismatch", text: "Etsy holds something that is neither the old listing nor what was sent. The safety copy and the restore body are kept; nothing was undone", details: cmp.differences.slice(0, 12) } }));
      return { ok: false, state: "mismatch", reason: "mismatch", details: cmp.differences.slice(0, 12), calls: run.n };
    } catch (e) {
      console.error("[etsyZodiac] reconcile", listingId, e && e.message);
      return answer("internal", { calls: run.n, state: d.state, details: [e && e.message] });
    }
  }

  /* ═══ status · the record, read only, no Etsy call ═══ */
  async function status(body) {
    body = body && typeof body === "object" ? body : {};
    const listingId = parseListingId(body.listingId);
    if (!listingId) return answer("bad-listing");
    let d;
    try { d = await readRec(listingId); } catch (e) { return answer("internal", { details: [e && e.message] }); }
    const phrase = phraseFor(listingId);
    if (!d) return { ok: true, found: false, state: "none", listingId, phrase };
    const w = d.write || {};
    return {
      ok: true, found: true, listingId, phrase, state: d.state || "none", ready: d.ready === true, payloadHash: d.payloadHash || "", summary: d.summary || null,
      reasons: d.reasons || [], warnings: d.warnings || [], paths: d.paths || {}, preparedAt: +d.preparedAt || 0, preparedBy: d.preparedBy || "",
      write: d.write ? { by: w.by || "", claimedAt: +w.claimedAt || 0, kickedAt: +w.kickedAt || 0, putAt: +w.putAt || 0 } : null,
      result: d.result || null, reconciles: +d.reconciles || 0, lastRefusal: d.lastRefusal || null, now: clock()
    };
  }

  return { prepare, write, execute, reconcile, status };
}

/* ── the real wiring (charmNestLibrary and the background function): built on first use, so loading this file touches neither Firebase nor Etsy ── */
let shared = null;
function real() {
  if (!shared) {
    const admin = require("./firebaseAdmin"), E = require("./_etsyMailEtsy"), meter = require("./_etsyApiMeter");
    shared = make({
      db: admin.firestore(), FV: admin.firestore.FieldValue, bucket: admin.storage().bucket(), meter, now: Date.now,
      etsy: {
        get: (path, query) => E.etsyFetch(path, { query }),
        put: (path, query, body) => E.etsyFetch(path, { method: "PUT", query, body }),
        scopes: async () => {   // POST /v3/application/scopes (form body token=<the access token>): what the connection may do. The token goes to Etsy only, never into an answer or a log.
          const fetch = require("node-fetch"), token = await E.getValidEtsyAccessToken(), secret = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;
          const t = meter.bump(SITE + ".scopes");
          let res;
          try { res = await fetch("https://api.etsy.com/v3/application/scopes", { method: "POST", headers: { "x-api-key": process.env.CLIENT_ID + ":" + secret, "Content-Type": "application/x-www-form-urlencoded" }, body: new URLSearchParams({ token }).toString(), timeout: SCOPES_MS }); }
          catch (e) { t.failNet(); throw e; }
          t.fromHttp(res.status);
          const text = await res.text(); let data = {}; try { data = text ? JSON.parse(text) : {}; } catch (_) { data = {}; }
          if (!res.ok) { const e = new Error("Etsy scopes: " + res.status); e.status = res.status; e.data = data; throw e; }
          return data;
        }
      },
      kick: async ({ listingId, writeId }) => {   // the background function gets 15 minutes; it answers 202 at once. It sends nothing unless its own transaction (writing -> putting, this writeId) wins.
        const fetch = require("node-fetch"), base = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
        const res = await fetch(base + "/.netlify/functions/" + KICK_FN, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ listingId, writeId }), timeout: 10000 });
        if (!(res.status === 202 || res.status === 200)) throw new Error("the background function answered " + res.status);
      }
    });
  }
  return shared;
}
const ops = {
  zodiacListingPrepare: async body => (isSandbox(body) ? answer("sandbox") : real().prepare(body)),
  zodiacListingWrite: async body => (isSandbox(body) ? answer("sandbox") : real().write(body)),
  zodiacListingReconcile: async body => (isSandbox(body) ? answer("sandbox") : real().reconcile(body)),
  zodiacListingStatus: async body => real().status(body)
};

module.exports = { make, ops, real, REASONS, SIGNS, OLD_SKU, QUERY, STORE, parseListingId, phraseFor, internals: { build, validate, listingProblems, restoreOf, describe, canonText, sha256 } };
