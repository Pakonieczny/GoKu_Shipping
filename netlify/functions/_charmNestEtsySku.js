/*  netlify/functions/_charmNestEtsySku.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  The LAST step of "save a custom design to the charm repository, give it a SKU, link it on Etsy"
 *  (plans/save-to-repo-1011/CONTRACT.md section 5): put the new SKU on the listing's product(s) on Etsy, so the next order of
 *  that listing already carries the SKU the repository knows.
 *
 *  THE DANGER. Etsy's inventory PUT REPLACES THE WHOLE INVENTORY of a listing (every product, price, quantity, enabled state,
 *  readiness profile, the three *_on_property lists). A SKU change is therefore a full write of everything else, and a body that is
 *  wrong in one field changes what buyers pay or can buy. So this module:
 *    · takes ONLY {rid}: everything it writes comes from Charm_Custom_Saves/{rid}, which the repository step (under the upload
 *      password) saved first; it refuses without a saved record that shows the repository step finished, and it never runs in the
 *      sandbox or for a rehearsal;
 *    · reads the listing's live inventory FIRST and changes nothing unless the SKU on Etsy is still the one the order was read with;
 *    · keeps the whole answer it read as a before-copy (Storage charmnest/custom-saves/{listingId}/{rid}-before.json) and notes it on
 *      the record BEFORE the PUT; if either cannot be written, NOTHING is sent;
 *    · sends back everything unchanged except the `sku` of the products that shared the bought product's SKU (the group); the body
 *      is built with the rules netlify/functions/etsyUpdateListingInventoryProxy.js proved (copied, not edited), then checked
 *      field by field against what was read (selfCheck) before it leaves, and compared with Etsy's own echo after;
 *    · NEVER undoes anything by itself and never sends a second PUT in one run: a difference is recorded (state "failed", reason
 *      "verify", the details and the path of the before-copy) for a person to look at;
 *    · spends at most 3 Etsy calls a run (listing state when the mirror has none, inventory, PUT), each with an 8 s timeout, each
 *      counted through meter.bumpSimple("custom.skuWrite") and returned as `calls`.
 *
 *  ENTITIES. Etsy HTML-escapes text it serves (a 16-inch value arrives as 16&quot;) and stores what it is sent as it is: written
 *  back escaped, the value shows buyers a literal "16&quot;" (etsy-pricing.html, decodeProducts). So values, option names and the
 *  SKUs of products this module does not change are sent back through ONE pass of the exact inverse of that escaping (the five
 *  characters & < > " ' and nothing else, never a second pass: a stored "&quot;" that Etsy serves as "&amp;quot;" goes back as
 *  "&quot;"). Everything else in the body is the very value Etsy served.
 *
 *  Dependencies are injected, so a test runs it on fakes and no test needs (or has) an Etsy token:
 *    make({ db, FV, bucket, etsy: { get(path, query), put(path, query, body) }, meter, now, timeoutMs?, maxCalls? })   (the last two can only LOWER the 8 s and the 3)
 *    the real wiring (`ops`, used by charmNestLibrary) builds them on first use from firebaseAdmin and _etsyMailEtsy.etsyFetch, which
 *    reads and refreshes config/etsyOauth itself. etsy.get/put resolve with Etsy's JSON and throw an Error with .status (and .data)
 *    on an HTTP error, exactly as etsyFetch does.
 *
 *  The save record's `etsy` field says where it stands: pending (nothing tried) | writing (the before-copy is safe, the PUT is leaving or has left; the record holds a
 *  60 s lease so a second press is refused) | linked | failed (reason, text; a clear refusal, nothing applied unless reason "verify") | unverified (the PUT may have landed:
 *  Retry reads the listing first and only writes again if the SKU is not there and Etsy never held a different answer). A Netlify function killed mid-run leaves "writing".
 *
 *  Answer (always HTTP 200 for an expected refusal; the key is `reason`, never `error`, which the Library turns into HTTP 400):
 *    { ok:true,  state:"linked", calls, productIds, sku, verified }
 *    { ok:false, state:"failed"|"unverified", reason, text, retry, calls, needs?, details?, beforePath? }
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const crypto = require("crypto");
const Canon = require("./_etsyInventoryCanonical");
const ListingSkus = require("./_charmNestListingSkus");

const SAVES = "Charm_Custom_Saves", ALIASES = "Charm_Sku_Aliases", MIRROR = "EtsyMail_Listings", LEASES = "Charm_Custom_EtsyLeases";
const SITE = "custom.skuWrite";
const MAX_CALLS = 3, TIMEOUT_MS = 8000, LEASE_MS = 60000;
const RID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$/, LISTING_RE = /^\d{3,20}$/, SKU_RE = /^[A-Z0-9_-]{1,32}$/;
const LISTS = ["price_on_property", "quantity_on_property", "sku_on_property"];
const CARRY = ["attempts", "backups", "beforePath", "beforeHash", "groupBefore", "products", "boughtKey"];   // what an `etsy` record keeps from one write to the next

/* every reason, in plain words (the strip shows "Etsy not updated: {text}"); retry says whether pressing Retry can ever help */
const REASONS = {
  scope: { text: "Etsy refused the change. Paul needs to re-authorise Etsy with listings_w", retry: true, needs: "listings_w" },
  busy: { text: "Etsy is busy, try again in a minute", retry: true },
  "etsy-down": { text: "Etsy did not answer", retry: true },
  offline: { text: "could not reach Etsy", retry: true },
  "listing-state": { text: o => "the listing is " + (o.listingState || "not active") + " on Etsy", retry: true },
  "listing-gone": { text: "Etsy no longer has this listing", retry: false },
  changed: { text: "the SKU on Etsy changed since the order was read", retry: false },
  "product-missing": { text: "the product bought is no longer on the listing", retry: false },
  "sku-in-use": { text: "that SKU is already on another product of the listing", retry: false },
  verify: { text: o => o.skuMissing ? "Etsy did not keep the new SKU" : "Etsy's listing is not what was sent; the safety copy is kept", retry: false },
  "backup-failed": { text: "the safety copy could not be saved", retry: true },
  rejected: { text: "Etsy did not accept the change", retry: false },
  unverified: { text: "Etsy accepted the change but did not confirm it; press Retry to check", retry: true, state: "unverified" },
  running: { text: "the Etsy step is already running", retry: true },
  interrupted: { text: "the last try was cut off; the next try checks Etsy first", retry: true, state: "unverified" },   // (KEEP R6: a run that died after its note "writing"; never shown as working)
  "not-connected": { text: "Etsy is not connected on the server. Paul needs to connect it", retry: true, needs: "etsy-connection" },
  sandbox: { text: "nothing is written to Etsy from the sandbox", retry: false },
  rehearsal: { text: "this was only a rehearsal; nothing was saved", retry: false },
  "no-save": { text: "nothing is saved for this order yet", retry: false },
  "not-saved": { text: "the repository step has not finished", retry: false },
  "bad-save": { text: "the saved record is not complete", retry: false },
  "bad-request": { text: "the request is not complete", retry: false },
  internal: { text: "the change was stopped by a safety check", retry: false }
};
function answer(reason, o) {
  o = o || {};
  const r = REASONS[reason] || REASONS.internal;
  const out = { ok: false, state: o.state || r.state || "failed", reason: REASONS[reason] ? reason : "internal", text: typeof r.text === "function" ? r.text(o) : r.text, retry: r.retry !== false, calls: o.calls | 0 };
  if (r.needs) out.needs = r.needs;
  if (o.details && o.details.length) out.details = o.details.slice(0, 10).map(x => String(x).slice(0, 200));
  if (o.beforePath) out.beforePath = o.beforePath;
  return out;
}
const isSandbox = b => !!b && (b.sandbox === true || b.sandbox === 1 || b.sandbox === "1" || b.sandbox === "true");

/* ── text helpers ── */
/* The exact inverse of HTML-escaping, in ONE pass: &amp; &lt; &gt; &quot; &apos; and the numeric forms of those five characters. */
const SPECIAL = { quot: '"', apos: "'", amp: "&", lt: "<", gt: ">" };
const SPECIAL_CODE = { 34: '"', 38: "&", 39: "'", 60: "<", 62: ">" };
function decodeOnce(v) {
  const s = String(v == null ? "" : v);
  if (s.indexOf("&") < 0) return s;
  return s.replace(/&(?:#x([0-9a-f]+)|#(\d+)|([a-z][a-z0-9]*));/gi, (m, h, d, n) => {
    if (h || d) { const c = SPECIAL_CODE[h ? parseInt(h, 16) : parseInt(d, 10)]; return c === undefined ? m : c; }
    return Object.prototype.hasOwnProperty.call(SPECIAL, n) ? SPECIAL[n] : m;
  });
}
const skuNorm = s => decodeOnce(s).trim().toUpperCase();
const sha256 = t => crypto.createHash("sha256").update(t).digest("hex");
const clean = v => {   // Firestore refuses undefined
  if (Array.isArray(v)) return v.map(clean);
  if (v && typeof v === "object") { const o = {}; for (const k of Object.keys(v)) if (v[k] !== undefined) o[k] = clean(v[k]); return o; }
  return v;
};
const idList = x => (Array.isArray(x) ? x : []).map(Number).filter(Number.isFinite);
const setKey = x => JSON.stringify([...new Set(idList(x))].sort((a, b) => a - b));
function withTimeout(fn, ms) {
  let t;
  const timer = new Promise((_, rej) => { t = setTimeout(() => rej(Object.assign(new Error("timeout"), { timeout: true })), ms); });
  return Promise.race([Promise.resolve().then(fn), timer]).finally(() => clearTimeout(t));
}

/* ── which Etsy product was bought: the id, else the option pairs (propertyId:valueId) the order carried ── */
function pairList(x) {   // the record's optionPairs: [["513","1234"]] | ["513:1234"] | [{property_id, value_id}]
  const out = [];
  for (const a of Array.isArray(x) ? x : []) {
    let p = null;
    if (Array.isArray(a) && a.length >= 2) p = [a[0], a[1]];
    else if (typeof a === "string" && a.indexOf(":") > 0) p = [a.slice(0, a.indexOf(":")), a.slice(a.indexOf(":") + 1)];
    else if (a && typeof a === "object") p = [a.property_id != null ? a.property_id : a.propertyId, a.value_id != null ? a.value_id : a.valueId];
    if (p && /^\d+$/.test(String(p[0])) && /^\d+$/.test(String(p[1]))) out.push(String(p[0]) + ":" + String(p[1]));
  }
  return [...new Set(out)].sort();
}
const productPairs = p => [...new Set((Array.isArray(p && p.property_values) ? p.property_values : []).flatMap(v => (Array.isArray(v && v.value_ids) ? v.value_ids : []).map(id => String(v.property_id) + ":" + String(id))))].sort();
function findBought(live, doc, key) {
  const pid = String(doc.productId == null ? "" : doc.productId).trim();
  if (pid) { const hit = live.filter(p => String(p.product_id) === pid); if (hit.length === 1) return hit[0]; }
  const pairs = pairList(doc.optionPairs);
  if (pairs.length) { const hit = live.filter(p => JSON.stringify(productPairs(p)) === JSON.stringify(pairs)); if (hit.length === 1) return hit[0]; }
  // after a write of ours Etsy may have given the product (and its option values) new ids: the option text, noted when the before-copy was made, still names it
  if (typeof key === "string") { const hit = live.filter(p => Canon.comboKey(p) === key); if (hit.length === 1) return hit[0]; }
  return !pid && !pairs.length && typeof key !== "string" && live.length === 1 ? live[0] : null;   // a listing with one product and no options
}

/* ── the PUT body: the proxy's proven rules (etsyUpdateListingInventoryProxy.js sanitizeForPut / normalizeOnProperty, copied, not edited) ── */
/* Etsy rejects a PUT whose *_on_property fields reference a partial set of the listing's variation properties once ANY of them references
   them all; widening to the full list changes no value (every product carries its own price, quantity and SKU). Fields at zero stay. */
function normalizeOnProperty(inv, propIds) {
  const total = propIds.length;
  const current = {};
  for (const f of LISTS) current[f] = (Array.isArray(inv[f]) ? inv[f] : []).map(Number).filter(Number.isFinite);
  if (!total) return current;
  if (!LISTS.some(f => current[f].length === total)) return current;
  for (const f of LISTS) { const len = current[f].length; if (len > 0 && len < total) current[f] = [...propIds]; }
  return current;
}
function sanitizeProduct(p, sku) {
  const out = {
    sku,
    property_values: (Array.isArray(p.property_values) ? p.property_values : []).map(v => {
      const values = (Array.isArray(v.values) ? v.values : []).map(x => decodeOnce(String(x)));
      const o = { property_id: Number(v.property_id) };
      if (v.property_name != null) o.property_name = decodeOnce(String(v.property_name));
      o.scale_id = v.scale_id ?? null;
      o.value_ids = Array.isArray(v.value_ids) && v.value_ids.length === values.length ? v.value_ids : [];
      o.values = values;
      return o;
    }),
    offerings: (Array.isArray(p.offerings) ? p.offerings : []).slice(0, 1).map(o => {
      const enabled = o.is_enabled !== false;
      // Etsy's canonical "unavailable" is quantity 0 + disabled; stock on a disabled offering would make Etsy enable it again
      const r = { price: Canon.toDecimalPrice(o.price), quantity: enabled ? Number(o.quantity ?? o.available_quantity ?? 0) : 0, is_enabled: enabled };
      if (o.readiness_state_id != null) r.readiness_state_id = Number(o.readiness_state_id);
      return r;
    })
  };
  return out;
}
function buildPutBody(inv, live, groupSet, newSku) {
  const onp = normalizeOnProperty(inv, Canon.productPropertyOrder({ products: live }));
  const body = {
    products: live.map(p => sanitizeProduct(p, groupSet.has(p) ? newSku : decodeOnce(p.sku == null ? "" : p.sku))),
    price_on_property: onp.price_on_property, quantity_on_property: onp.quantity_on_property, sku_on_property: onp.sku_on_property
  };
  // the newer readiness-by-property list, when Etsy sends one: kept as it is (leaving it out would reset it)
  if (Array.isArray(inv.readiness_state_on_property)) body.readiness_state_on_property = idList(inv.readiness_state_on_property);
  return body;
}
/* The body against what was READ, field by field, by code that does not share sanitizeProduct's structure. Anything unexpected = [reasons]
   and NOTHING is sent. Allowed differences: the group's sku; HTML-escaping undone; a disabled offering's stock zeroed; price as a decimal; the
   on-property lists widened to all of the listing's properties. */
function selfCheck(inv, live, body, groupSet, newSku) {
  const bad = [];
  if (!body || !Array.isArray(body.products) || body.products.length !== live.length) return ["the product count"];
  const allowed = ["products", ...LISTS, "readiness_state_on_property"];
  for (const k of Object.keys(body)) if (!allowed.includes(k)) bad.push("unexpected field " + k);
  live.forEach((g, i) => {
    const b = body.products[i], k = "product " + (i + 1);
    if (!b) { bad.push(k + " missing"); return; }
    if (b.sku !== (groupSet.has(g) ? newSku : decodeOnce(g.sku == null ? "" : g.sku))) bad.push(k + " sku");
    if (typeof b.sku !== "string" || b.sku.length > 32 && groupSet.has(g)) bad.push(k + " sku length");
    const gp = Array.isArray(g.property_values) ? g.property_values : [];
    if (!Array.isArray(b.property_values) || b.property_values.length !== gp.length) bad.push(k + " options");
    else gp.forEach((v, j) => {
      const w = b.property_values[j], vals = (Array.isArray(v.values) ? v.values : []).map(x => decodeOnce(String(x)));
      if (w.property_id !== Number(v.property_id)) bad.push(k + " option id");
      if (JSON.stringify(w.values) !== JSON.stringify(vals)) bad.push(k + " option value");
      if (JSON.stringify(w.value_ids) !== JSON.stringify(Array.isArray(v.value_ids) && v.value_ids.length === vals.length ? v.value_ids : [])) bad.push(k + " option value ids");
      if ((w.scale_id ?? null) !== (v.scale_id ?? null)) bad.push(k + " scale");
      if ((v.property_name != null) !== Object.prototype.hasOwnProperty.call(w, "property_name") || (v.property_name != null && w.property_name !== decodeOnce(String(v.property_name)))) bad.push(k + " option name");
    });
    const go = Array.isArray(g.offerings) ? g.offerings[0] : null;
    if (!go) { bad.push(k + " has no offering"); return; }
    if (!Array.isArray(b.offerings) || b.offerings.length !== 1) { bad.push(k + " offerings"); return; }
    const o = b.offerings[0], enabled = go.is_enabled !== false, price = Canon.toDecimalPrice(go.price), q = enabled ? Number(go.quantity ?? go.available_quantity ?? 0) : 0;
    if (!(typeof price === "number" && Number.isFinite(price) && price > 0)) bad.push(k + " price unreadable");
    else if (o.price !== price) bad.push(k + " price");
    if (!Number.isInteger(q) || q < 0) bad.push(k + " quantity unreadable");
    else if (o.quantity !== q) bad.push(k + " quantity");
    if (o.is_enabled !== enabled) bad.push(k + " enabled");
    if (go.readiness_state_id != null ? o.readiness_state_id !== Number(go.readiness_state_id) : Object.prototype.hasOwnProperty.call(o, "readiness_state_id")) bad.push(k + " readiness");
  });
  const order = Canon.productPropertyOrder({ products: live });
  for (const f of LISTS) {
    const orig = setKey(inv[f]), got = setKey(body[f]), widened = setKey(order);
    if (got !== orig && !(got === widened && idList(inv[f]).length > 0 && idList(inv[f]).length < order.length)) bad.push(f);
  }
  if (Array.isArray(inv.readiness_state_on_property) ? setKey(body.readiness_state_on_property) !== setKey(inv.readiness_state_on_property) : Object.prototype.hasOwnProperty.call(body, "readiness_state_on_property")) bad.push("readiness_state_on_property");
  return bad;
}

/* ── Etsy's answer to the PUT (or a later GET) against what was sent ── */
/* the proxy's test of an echo that is a whole inventory (same test; nothing weaker) */
function looksLikeFreshInventory(payload, expectedProductCount) {
  return !!payload && Array.isArray(payload.products) && payload.products.length === expectedProductCount &&
    payload.products.every(p => p && Array.isArray(p.offerings) && p.offerings.length > 0 && p.offerings[0].price != null && Object.prototype.hasOwnProperty.call(p.offerings[0], "is_enabled"));
}
function compareInventories(sent, echo) {
  const prep = inv => ({
    products: (Array.isArray(inv && inv.products) ? inv.products : []).filter(p => p && !p.is_deleted).map(p => Object.assign({}, p, { sku: decodeOnce(p.sku == null ? "" : p.sku) })),
    price_on_property: inv && inv.price_on_property, quantity_on_property: inv && inv.quantity_on_property, sku_on_property: inv && inv.sku_on_property
  });
  const a = prep(sent), b = prep(echo), diffs = [];
  const v = Canon.verifyAgainst(a, b);   // combination keys, price, quantity, SKU, enabled state (HTML-escaping compared as text)
  diffs.push(...v.differences);
  for (const f of LISTS) if (Array.isArray(sent && sent[f]) && Array.isArray(echo && echo[f]) && setKey(sent[f]) !== setKey(echo[f])) diffs.push(f + " is " + setKey(echo[f]) + ", sent " + setKey(sent[f]));
  if (Array.isArray(sent && sent.readiness_state_on_property) && Array.isArray(echo && echo.readiness_state_on_property) && setKey(sent.readiness_state_on_property) !== setKey(echo.readiness_state_on_property)) diffs.push("readiness_state_on_property changed");
  const byKey = new Map(b.products.map(p => [Canon.comboKey(p), p]));
  for (const p of a.products) {
    const g = byKey.get(Canon.comboKey(p)), key = Canon.comboKey(p) || "base product";
    if (!g) continue;   // (already reported as missing)
    const sr = p.offerings && p.offerings[0] && p.offerings[0].readiness_state_id, gr = g.offerings && g.offerings[0] && g.offerings[0].readiness_state_id;
    if (sr != null && gr != null && Number(sr) !== Number(gr)) diffs.push(key + " readiness is " + gr + ", sent " + sr);
    const sv = (p.property_values || []).map(x => [Number(x.property_id), (x.values || []).map(String)]).sort((x, y) => x[0] - y[0]);
    const gv = (g.property_values || []).map(x => [Number(x.property_id), (x.values || []).map(t => decodeOnce(String(t)))]).sort((x, y) => x[0] - y[0]);
    if (JSON.stringify(sv) !== JSON.stringify(gv)) diffs.push(key + " option text is not what was sent");
  }
  return { ok: diffs.length === 0, differences: diffs };
}

/* ── errors → a reason ── */
function classify(e) {
  if (e && e.cap) return { reason: "internal" };
  if (e && e.timeout) return { reason: "etsy-down", ambiguous: true };
  const msg = String((e && e.message) || "");
  let st = +(e && e.status) || 0;
  if (!st) {
    const m = /refresh failed:\s*(\d{3})/i.exec(msg);
    // KEEP R8: a failed TOKEN REFRESH is about the connection, not about this write: 400 (invalid_grant: the refresh token is dead), 401, 403 = Paul must re-authorise (scope, retried on a later load);
    // 429 and 5xx are Etsy's own weather. It must never read as "Etsy did not accept the change" (rejected, final).
    if (m) { const r = +m[1]; st = r === 429 || r >= 500 ? r : 401; }
    else if (/not seeded|No refresh_token/i.test(msg)) st = 401;
    else if (/Missing SHOP_ID|CLIENT_ID|CLIENT_SECRET/i.test(msg)) return { reason: "not-connected" };
  }
  const detail = e && e.data && typeof e.data.error === "string" ? [e.data.error] : [];
  if (st === 401 || st === 403) return { reason: "scope" };
  if (st === 404 || st === 410) return { reason: "listing-gone" };
  if (st === 429) return { reason: "busy" };
  if (st >= 500) return { reason: "etsy-down", ambiguous: st === 502 || st === 503 || st === 504 };
  if (st >= 400) return { reason: "rejected", details: detail };
  return { reason: "offline", ambiguous: true };
}

/* ── the module ── */
function make(deps) {
  const { db, FV, bucket, etsy, meter } = deps;
  const clock = typeof deps.now === "function" ? deps.now : Date.now;
  const timeout = deps.timeoutMs > 0 ? deps.timeoutMs : TIMEOUT_MS;
  const maxCalls = deps.maxCalls > 0 ? Math.min(deps.maxCalls, MAX_CALLS) : MAX_CALLS;   // (a test may only LOWER the cap)

  /* the record is read, checked and the run claimed in ONE transaction: a second press while a run is in flight is refused (a lease of 60 s) */
  async function claim(ref) {
    const t = clock();
    return db.runTransaction(async tx => {
      const snap = await tx.get(ref);
      if (!snap.exists) return { refusal: "no-save" };
      const d = snap.data() || {};
      if (d.rehearsal === true) return { refusal: "rehearsal" };
      if (d.phase !== "repo") return { refusal: "not-saved" };
      const oldSku = d.oldSku == null ? "" : d.oldSku;
      if (!LISTING_RE.test(String(d.listingId == null ? "" : d.listingId)) || !SKU_RE.test(String(d.sku == null ? "" : d.sku)) || typeof oldSku !== "string" || oldSku.length > 120) return { refusal: "bad-save" };
      const prev = d.etsy && typeof d.etsy === "object" ? d.etsy : { state: "pending" };
      if (prev.state === "linked") return { linked: { d, prev } };
      // KEEP B5: the line already carried the design's own SKU (the shop's SKUs have the very shape the plan makes: HEART_9491): Etsy has nothing to change, the listing IS linked, no call is made
      if (skuNorm(oldSku) === String(d.sku)) {
        const done = { state: "linked", at: t, calls: 0, noop: true, verified: false, productIds: [], sku: String(d.sku) };
        tx.update(ref, { etsy: clean(done) });
        return { linked: { d, prev: done } };
      }
      if (+prev.runningUntil > t) return { refusal: "running" };
      tx.update(ref, { etsy: clean(Object.assign({}, prev, { runningUntil: t + LEASE_MS })) });
      return { d, prev, lease: t + LEASE_MS };
    });
  }

  async function mirrorState(listingId) {
    try {
      const s = await db.collection(MIRROR).doc(listingId).get();
      const v = s.exists ? (s.data() || {}).state : null;
      return typeof v === "string" && v.trim() ? v.trim().toLowerCase() : null;
    } catch (_) { return null; }
  }

  /* the order's record: {rid}, or {rid}-{listingId} when the order holds more than one listing's save (an optional listingId says which; a caller may also pass that id as the rid) */
  async function recordRef(rid, listingId) {
    const base = db.collection(SAVES).doc(rid);
    if (!LISTING_RE.test(listingId)) return base;
    const list = db.collection(SAVES).doc(rid + "-" + listingId), [a, c] = await db.getAll(base, list);
    if (a.exists && String((a.data() || {}).listingId || listingId) !== listingId) return list;
    if (!a.exists && c.exists) return list;
    return base;
  }

  async function customEtsySkuWrite(body) {
    body = body && typeof body === "object" ? body : {};
    if (isSandbox(body)) return answer("sandbox");
    const rid = String(body.rid == null ? "" : body.rid).trim();
    if (!RID_RE.test(rid)) return answer("bad-request");
    let ref;
    try { ref = await recordRef(rid, String(body.listingId == null ? "" : body.listingId).trim()); } catch (e) { console.error("[etsySku] record", rid, e && e.message); return answer("internal", { details: [e && e.message] }); }
    let got;
    try { got = await claim(ref); } catch (e) { console.error("[etsySku] claim", rid, e && e.message); return answer("internal", { details: [e && e.message] }); }
    if (got.refusal) return answer(got.refusal);
    if (got.linked) return linkedAnswer(got.linked.d, got.linked.prev, 0);
    const out = await leased(ref, got, () => run(ref, rid, got.d, got.prev, got.lease));
    if (out.calls && meter && typeof meter.flushNow === "function") { try { await meter.flushNow(); } catch (_) { /* the counters are best effort */ } }
    return out;
  }

  const linkedAnswer = (d, etsyDoc, calls) => Object.assign({ ok: true, state: "linked", calls, productIds: (Array.isArray(etsyDoc.productIds) ? etsyDoc.productIds : []).map(String), sku: String(d.sku), verified: etsyDoc.verified !== false }, etsyDoc.noop === true ? { noop: true } : {});

  async function run(ref, rid, d, prev, lease) {
    const listingId = String(d.listingId), sku = String(d.sku), oldSku = skuNorm(d.oldSku);
    let calls = 0, putSent = false;
    const noPut = prev.state === "failed" && prev.reason === "verify";   // Etsy did not hold what was sent once: this record never sends a PUT again
    let cur = {};   // the `etsy` record as this run writes it
    for (const k of CARRY) if (prev[k] !== undefined) cur[k] = prev[k];

    const call = fn => {
      if (calls >= maxCalls) return Promise.reject(Object.assign(new Error("call cap"), { cap: true }));
      calls++;
      try { meter && meter.bumpSimple(SITE); } catch (_) { /* a counter never stops the work */ }
      return withTimeout(fn, timeout);
    };
    /* write the `etsy` record; the lease is released with the run's last write only (the "writing" note keeps it, so a second press while the PUT is in flight is refused);
       a failure here is reported to the log, the answer still tells the truth */
    const settle = async (state, patch, midRun) => {
      const keep = {}; for (const k of CARRY) if (cur[k] !== undefined) keep[k] = cur[k];
      cur = clean(Object.assign(keep, patch || {}));
      try { await ref.update({ etsy: Object.assign({}, cur, { state, at: clock(), calls }, midRun ? { runningUntil: lease } : {}) }); return true; }
      catch (e) { console.warn("[etsySku] could not update the save record for", rid, e && e.message); return false; }
    };
    const stop = async (reason, o) => {
      const a = answer(reason, Object.assign({ calls }, o));
      await settle(a.state, { reason: a.reason, text: a.text, retry: a.retry, details: a.details });
      return a;
    };
    /* an Etsy error → its reason; after the PUT, an answer that may still have been applied (timeout, 502/503/504, no connection) is "unverified", not "failed" */
    const stopOn = (e, extra) => { const c = classify(e); return stop(c.reason, Object.assign({}, extra, { details: c.details, state: c.ambiguous && extra && extra.afterPut ? "unverified" : undefined })); };

    try {
      /* 2 · the listing's state: the inbox mirror (no Etsy call), else one GET; only an active listing is written. Skipped when an earlier run already wrote or tried to. */
      const checked = prev.state === "writing" || prev.state === "unverified" || (prev.state === "failed" && prev.reason === "verify");
      if (!checked) {
        let state = await mirrorState(listingId);
        if (!state) {
          let l;
          try { l = await call(() => etsy.get("/listings/" + listingId)); } catch (e) { return await stopOn(e); }
          state = l && typeof l.state === "string" ? l.state.trim().toLowerCase() : "";
        }
        if (state !== "active") return await stop("listing-state", { listingState: String(state || "unknown").replace(/[^a-z_ ]/g, "").slice(0, 20) || "unknown" });
      }

      /* 3 · the live inventory, then the checks that nothing moved since the order was read */
      let inv;
      try { inv = await call(() => etsy.get("/listings/" + listingId + "/inventory", { legacy: false })); } catch (e) { return await stopOn(e); }
      const live = (Array.isArray(inv && inv.products) ? inv.products : []).filter(p => p && typeof p === "object" && !p.is_deleted);
      if (!live.length) return await stop("product-missing");
      const bought = findBought(live, d, cur.boughtKey);
      if (!bought) {
        // KEEP R11: Etsy gives every product a NEW id on every write, so an order whose product id was read before ANOTHER order of the listing wrote the same SKU can no longer find its product by id.
        // When that other order's write renamed it (its record's groupBefore holds this id) and the listing carries the SKU now, this order is linked by that write: nothing to write, nothing missing.
        // (or, with no sibling record to say so: the listing carries the SKU and NO product still carries the old one, so everything that shared the bought product's SKU has been renamed)
        const carried = live.filter(p => skuNorm(p.sku) === sku);
        if (carried.length && (!live.some(p => skuNorm(p.sku) === oldSku) || await renamedBySibling(ref, d))) return await linked(ref, rid, d, inv, carried, calls, false, settle);
        return await stop("product-missing");
      }
      const bsku = skuNorm(bought.sku);

      if (bsku === sku) {   // already on Etsy (an earlier run's PUT landed, or a person set it): no write
        if (live.some(p => skuNorm(p.sku) === oldSku && p !== bought)) return await stop("changed");
        const group = live.filter(p => skuNorm(p.sku) === sku);
        let verified = false;
        if (cur.beforePath) {   // we wrote it: the live listing must be the before-copy with only the group's SKU changed
          let before = null;
          try { before = JSON.parse((await bucket.file(cur.beforePath).download())[0].toString("utf8")); } catch (_) { before = null; }
          const bl = before && Array.isArray(before.products) ? before.products.filter(p => p && !p.is_deleted) : null;
          if (bl && bl.length) {
            const gs = new Set(bl.filter(p => skuNorm(p.sku) === oldSku));
            const cmp = compareInventories(buildPutBody(before, bl, gs, sku), inv);
            if (!cmp.ok) return await stop("verify", { details: cmp.differences, beforePath: cur.beforePath });
            verified = true;
          }
        }
        return await linked(ref, rid, d, inv, group, calls, verified, settle);
      }
      if (bsku !== oldSku) return await stop("changed");
      const group = live.filter(p => skuNorm(p.sku) === oldSku);
      if (live.some(p => skuNorm(p.sku) === sku)) return await stop("sku-in-use");
      const mode = group.length === live.length ? "all" : "some";
      if (d.group && (d.group.mode === "all" || d.group.mode === "some") && d.group.mode !== mode) return await stop("changed");

      if (noPut) return await stop("verify", { skuMissing: true, beforePath: cur.beforePath });

      /* the body, checked against what was read before it can leave */
      const groupSet = new Set(group), putBody = buildPutBody(inv, live, groupSet, sku);
      const problems = selfCheck(inv, live, putBody, groupSet, sku);
      if (problems.length) return await stop("internal", { details: problems });

      /* 4 · the before-copy, in Storage and on the record, BEFORE the PUT; if either fails, nothing is sent */
      let attempt = (+cur.attempts || 0) + 1, beforePath = "";
      const beforeText = JSON.stringify(inv), beforeHash = sha256(beforeText);
      try {
        // a before-copy is never overwritten: a name already in Storage is skipped (the first one, -before.json, is the listing as it was before any write)
        for (let i = 0; i < 6; i++, attempt++) {
          beforePath = "charmnest/custom-saves/" + listingId + "/" + rid + "-before" + (attempt > 1 ? "-" + attempt : "") + ".json";
          if (!(await bucket.file(beforePath).exists())[0]) break;
          beforePath = "";
        }
        if (!beforePath) throw new Error("no free name for the safety copy");
        await bucket.file(beforePath).save(Buffer.from(beforeText, "utf8"), { resumable: false, contentType: "application/json", metadata: { cacheControl: "no-store" } });
        const [there] = await bucket.file(beforePath).exists();
        if (!there) throw new Error("the safety copy is not in Storage after the save");
        cur = Object.assign({}, cur, { attempts: attempt, backups: [...(Array.isArray(cur.backups) ? cur.backups : []), beforePath], beforePath, beforeHash, products: live.length, boughtKey: Canon.comboKey(bought),
          groupBefore: group.slice(0, 100).map(p => ({ id: String(p.product_id == null ? "" : p.product_id), sku: String(p.sku == null ? "" : p.sku) })) });
        if (!(await settle("writing", {}, true))) throw new Error("the safety copy is not noted on the save record");
      } catch (e) {
        console.warn("[etsySku] backup failed for", rid, e && e.message);
        return await stop("backup-failed");
      }

      /* 5 · the one PUT */
      let echo;
      putSent = true;
      try { echo = await call(() => etsy.put("/listings/" + listingId + "/inventory", { legacy: false }, putBody)); }
      catch (e) { return await stopOn(e, { afterPut: true, beforePath }); }

      /* 6 · Etsy's own answer against what was sent. No second PUT, no undo, whatever it says. */
      if (!looksLikeFreshInventory(echo, putBody.products.length)) return await stop("unverified", { beforePath });
      const cmp = compareInventories(putBody, echo);
      if (!cmp.ok) return await stop("verify", { details: cmp.differences, skuMissing: cmp.differences.some(x => /SKU/.test(x)), beforePath });
      return await linked(ref, rid, d, echo, group, calls, true, settle);
    } catch (e) {
      console.error("[etsySku]", rid, e && e.message);
      return await stop("internal", { state: putSent ? "unverified" : undefined, details: [e && e.message] });
    }
  }

  /* 7 · linked: the record, the alias note, and the listing's SKU table refreshed from Etsy's own answer (so the page sees the new SKU at once, no extra Etsy call) */
  async function linked(ref, rid, d, readBack, group, calls, verified, settle) {
    const sku = String(d.sku), listingId = String(d.listingId);
    const idsNow = (Array.isArray(readBack && readBack.products) ? readBack.products : []).filter(p => p && !p.is_deleted && skuNorm(p.sku) === sku).map(p => String(p.product_id == null ? "" : p.product_id)).filter(Boolean);
    const productIds = idsNow.length ? idsNow : group.map(p => String(p.product_id == null ? "" : p.product_id));
    const at = clock();
    const recorded = await settle("linked", { productIds, verified, sku });
    try {
      const aliasRef = db.collection(ALIASES).doc(listingId), snap = await aliasRef.get();
      if (snap.exists) await aliasRef.update({ keep: Object.assign({}, (snap.data() || {}).keep || {}, { etsy: { state: "linked", at, rid, sku } }) });
    } catch (e) { console.warn("[etsySku] alias note not written for", listingId, e && e.message); }
    try { await db.collection(ListingSkus.COLL).doc(listingId).set(clean(ListingSkus.toStored(ListingSkus.compact(readBack, at)))); }
    catch (e) { console.warn("[etsySku] SKU table not refreshed for", listingId, e && e.message); }
    const out = { ok: true, state: "linked", calls, productIds, sku, verified };
    if (!recorded) out.recorded = false;   // Etsy has the SKU; only the save record could not be updated (a Retry makes one GET and writes it)
    return out;
  }

  /* KEEP R7: ONE Etsy write per LISTING at a time. Two orders of a listing (a race lost in the repository step, a second tab, a resume) each hold their own record and lease; without this both read the
     inventory with the old SKU and both PUT the whole inventory. The listing's lease is a document of its own (Charm_Custom_EtsyLeases/{listing}, 60 s); the loser answers "running" (retry), reads the
     winner's result on its next try (the SKU is on Etsy: linked, no second write). A function that dies leaves a lease that simply runs out. */
  async function leaseListing(listingId, owner) {
    const lref = db.collection(LEASES).doc(listingId), t = clock();
    return db.runTransaction(async tx => {
      const s = await tx.get(lref), c = s.exists ? s.data() || {} : null;
      if (c && c.owner && c.owner !== owner && +c.until > t) return { ok: false, owner: c.owner };
      tx.set(lref, { owner, until: t + LEASE_MS, at: t });
      return { ok: true };
    });
  }
  async function unleaseListing(listingId, owner) {
    const lref = db.collection(LEASES).doc(listingId);
    try { await db.runTransaction(async tx => { const s = await tx.get(lref); if (s.exists && (s.data() || {}).owner === owner) tx.delete(lref); }); }
    catch (e) { console.warn("[etsySku] listing lease not released for", listingId, e && e.message); }   // (it runs out by itself)
  }
  async function leased(ref, got, work) {
    const listingId = String(got.d.listingId), owner = String(ref.id || got.d.rid || "");
    let l; try { l = await leaseListing(listingId, owner); } catch (e) { console.warn("[etsySku] listing lease not taken for", listingId, e && e.message); l = { ok: true, unchecked: true }; }   // (the rid lease still holds)
    if (!l.ok) {
      // give back this record's own lease (nothing was tried), so the next try is not refused for a minute
      const back = Object.assign({}, got.prev); delete back.runningUntil;
      try { await ref.update({ etsy: clean(back) }); } catch (_) { /* it runs out */ }
      return answer("running", { calls: 0 });
    }
    try { return await work(); } finally { if (!l.unchecked) await unleaseListing(listingId, owner); }
  }

  async function renamedBySibling(ref, d) {
    const pid = String(d.productId == null ? "" : d.productId).trim();
    if (!pid) return false;
    try {
      const snap = await db.collection(SAVES).where("listingId", "==", String(d.listingId)).limit(50).get();
      return snap.docs.some(x => { const o = x.data() || {}, e = o.etsy || {}; return x.id !== ref.id && String(o.sku) === String(d.sku) && e.state === "linked" && (Array.isArray(e.groupBefore) ? e.groupBefore : []).some(g => g && String(g.id) === pid); });
    } catch (e) { console.warn("[etsySku] sibling saves not read for", d.listingId, e && e.message); return false; }
  }

  return { customEtsySkuWrite };
}

/* ── the real wiring (charmNestLibrary): built on first use, so loading this file touches neither Firebase nor Etsy ── */
let shared = null;
function real() {
  if (!shared) {
    const admin = require("./firebaseAdmin"), E = require("./_etsyMailEtsy");
    shared = make({
      db: admin.firestore(), FV: admin.firestore.FieldValue, bucket: admin.storage().bucket(), meter: require("./_etsyApiMeter"), now: Date.now,
      etsy: { get: (path, query) => E.etsyFetch(path, { query }), put: (path, query, body) => E.etsyFetch(path, { method: "PUT", query, body }) }
    });
  }
  return shared;
}
const ops = {
  customEtsySkuWrite: async body => (isSandbox(body) ? answer("sandbox") : real().customEtsySkuWrite(body))
};

/** KEEP R6: the `etsy` object of a save record as a reader should see it NOW. A run notes "writing" (with a 60 s lease) from its before-copy to its last write; when the function is killed in between
    that note would stay for ever. A "writing" past its lease is not working: it is "unverified" (the next try reads Etsy first and writes only if the SKU is not there). Pure; the store is not touched. */
function effective(etsyDoc, now) {
  const e = etsyDoc && typeof etsyDoc === "object" ? etsyDoc : { state: "pending" };
  if (e.state !== "writing" || +e.runningUntil > (now == null ? Date.now() : now)) return e;
  const r = REASONS.interrupted;
  return Object.assign({}, e, { state: "unverified", reason: "interrupted", text: r.text, retry: true });
}

module.exports = { make, ops, REASONS, effective, LEASES, internals: { decodeOnce, normalizeOnProperty, sanitizeProduct, buildPutBody, selfCheck, compareInventories, looksLikeFreshInventory, classify, findBought, pairList } };
