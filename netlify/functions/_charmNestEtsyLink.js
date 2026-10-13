/*  netlify/functions/_charmNestEtsyLink.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  "Put this master charm's SKU on that Etsy listing" (SWANSKU, 12 Oct 2026; Paul: "Update this etsy listing SKU to match this Charm in our repo").
 *  The listing already exists on Etsy and the charm already exists in the master repository: this op only makes the listing's products carry the
 *  charm's own master SKU, so the next order of that listing resolves to the charm by its transaction SKU (charm-nest-orders.js resolveSku).
 *  It is the sibling of _charmNestEtsySku.js (custom designs saved to the repo, authority: a saved record) and reuses its proven internals
 *  (the PUT body, the field by field self-check, the echo comparison, the error classes). Here the authority is the repository password
 *  (op listingEtsySkuWrite is a "modify" op: _charmNestRepoAuth.js LIBRARY), because nothing saved says which listing may take which SKU.
 *
 *  Takes { listingId (a number or an Etsy listing link), sku, by }. THE DANGER is the same as there: Etsy's inventory PUT REPLACES THE WHOLE
 *  INVENTORY of the listing, so a body wrong in one field changes what buyers pay or can buy. So:
 *    · it refuses unless the master index holds EXACTLY that SKU (the entry's own spelling) and the entry is not blocked; it never runs in the
 *      sandbox; the SKU must be one Etsy keeps as written (printable text, single spaces, at most 32 characters: "BEADY SWAN" is fine);
 *    · it reads the listing's live inventory FIRST (the only way to know what is there); a listing whose products all carry the SKU already is
 *      "already", no write. Otherwise EVERY live product takes the SKU, whatever the products carry now (one SKU, several different ones such as
 *      X and X-CO, blanks, the shop's placeholder SKUs): Paul, 13 Oct 2026, "Replace each set of etsy SKU's with the provided charm repo SKU".
 *      It no longer stops as "mixed-skus"; when the products carried two or more different SKUs (blank counts as one value) the answer says what was
 *      replaced (`replaced`: [{ sku, n }]) and the before-copy below keeps every old SKU, so they can be put back;
 *    · every live product then takes the SKU and EVERYTHING ELSE goes back unchanged (price, stock, enabled state, readiness, options, the
 *      *_on_property lists), built by the rules etsyUpdateListingInventoryProxy.js proved and checked field by field against what was read
 *      (selfCheck) before it leaves; Etsy's own echo is compared with what was sent after;
 *    · the whole answer it read is kept as a before-copy in Storage (charmnest/custom-saves/{listingId}/link-{SKU}-before.json) and noted on
 *      the record BEFORE the PUT; if either cannot be written, NOTHING is sent;
 *    · ONE PUT per attempt, never a second PUT in one run, and it NEVER undoes anything by itself: a difference is recorded (reason "verify")
 *      with the before-copy's path for a person to look at, and a refused body ("rejected") is recorded and not sent again unchanged;
 *    · at most 3 Etsy calls a run (listing state when the inbox mirror has none, inventory, PUT), 8 s each, each counted through
 *      meter.bumpSimple("custom.skuLink"); 401/403 = "needs listings_w", no retry loop;
 *    · a lease on the record Charm_Custom_Saves/link-{listingId} (60 s, taken in one transaction) and the record of a PUT that left make a
 *      double press send once: while one run is in flight a second is refused; after "linked" a second makes no Etsy call.
 *  Dependencies are injected (make({ db, FV, bucket, etsy:{get,put}, meter, now, timeoutMs?, maxCalls? })), so a test runs on fakes and needs no token;
 *  `ops` builds the real ones on first use exactly as _charmNestEtsySku.js does.
 *
 *  The record (Charm_Custom_Saves/link-{listingId}; no buyer data): { kind, listingId, sku, by, at, earlier[], link:{ state, at, calls, ... } }
 *  link.state: pending | writing (the before-copy is safe, the PUT is leaving or has left) | linked | failed | unverified (the PUT may have landed: a later press
 *  reads the listing first; it links when the SKU is there, sends once more ONLY if the listing is still exactly as the before-copy was, and otherwise stops).
 *
 *  Answer (always HTTP 200 for an expected refusal; the key is `reason`, never `error`):
 *    { ok:true,  state:"linked", calls, listingId, sku, productIds, products, verified, already?, beforePath?, replaced?: [{ sku, n }] (only when the products carried 2+ different SKUs; blank is sku ""), replacedMore? }
 *    { ok:false, state:"failed"|"unverified", reason, text, retry, calls, needs?, details?, beforePath? }
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const crypto = require("crypto");
const Canon = require("./_etsyInventoryCanonical");
const ListingSkus = require("./_charmNestListingSkus");
const Master = require("./_charmNestMaster");
const Etsy = require("./_charmNestEtsySku");
const { decodeOnce, buildPutBody, selfCheck, compareInventories, looksLikeFreshInventory, classify } = Etsy.internals;

const SAVES = "Charm_Custom_Saves", MIRROR = "EtsyMail_Listings", SITE = "custom.skuLink";
const MAX_CALLS = 3, TIMEOUT_MS = 8000, LEASE_MS = 60000, MAX_ATTEMPTS = 3, ETSY_SKU_MAX = 32;
const LISTING_RE = /^\d{3,20}$/, PLAIN_SKU = /^[\x21-\x7e]+(?: [\x21-\x7e]+)*$/;   // printable ASCII words, one space between them, none at either end
const CARRY = ["attempts", "backups", "beforePath", "beforeHash", "beforeCanon", "products", "putSent", "replaced", "replacedMore"];   // what a `link` record keeps from one run to the next

const REASONS = {
  scope: { text: "Etsy refused the change. Paul needs to re-authorise Etsy with listings_w", retry: true, needs: "listings_w" },
  busy: { text: "Etsy is busy, try again in a minute", retry: true },
  "etsy-down": { text: "Etsy did not answer", retry: true },
  offline: { text: "could not reach Etsy", retry: true },
  "listing-state": { text: o => "the listing is " + (o.listingState || "not active") + " on Etsy", retry: true },
  "listing-gone": { text: "Etsy no longer has this listing", retry: false },
  "no-products": { text: "the listing has no products on Etsy", retry: false },
  changed: { text: "the listing on Etsy is neither as it was before the first try nor as wanted: nothing was changed", retry: false },
  verify: { text: o => o.skuMissing ? "Etsy did not keep the new SKU as it was sent; the safety copy is kept" : "Etsy's listing is not what was sent; the safety copy is kept", retry: false },
  "backup-failed": { text: "the safety copy could not be saved", retry: true },
  rejected: { text: "Etsy did not accept the change", retry: false },
  unverified: { text: "Etsy accepted the change but did not confirm it; press again to check", retry: true, state: "unverified" },
  running: { text: "the Etsy step is already running", retry: true },
  attempts: { text: "three tries were already made on this listing; a person must look at the safety copies", retry: false },
  sandbox: { text: "nothing is written to Etsy from the sandbox", retry: false },
  "sku-not-in-master": { text: "that SKU is not in the master repository", retry: false },
  "sku-blocked": { text: "that master design is blocked until its conflict is settled", retry: false },
  "sku-shape": { text: o => "Etsy would not keep that SKU as it is" + (o.details && o.details[0] ? " (" + o.details[0] + ")" : ""), retry: false },
  "bad-request": { text: "the request is not complete: a listing (number or link) and a master SKU", retry: false },
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
const sha256 = t => crypto.createHash("sha256").update(t).digest("hex");
const MAX_REPLACED = 12;
/** What the live products carry now, one group per SKU (HTML-escaping undone, spaces at the ends and capitals set aside; a blank SKU is its own group "") → [{ sku (the first spelling Etsy showed), n }], biggest first. */
function skuGroups(live) {
  const by = new Map();
  for (const p of live) { const t = decodeOnce(p.sku == null ? "" : p.sku).trim(), k = t.toUpperCase(), g = by.get(k); if (g) g.n++; else by.set(k, { sku: t.slice(0, 40), n: 1 }); }
  return [...by.values()].sort((a, b) => b.n - a.n || (a.sku < b.sku ? -1 : a.sku > b.sku ? 1 : 0));
}
/** The groups to report: only when the products carried two or more different SKUs; at most MAX_REPLACED groups, the products of the others counted in `replacedMore`. → { replaced?, replacedMore? } */
function replacedOf(groups) {
  if (groups.length < 2) return {};
  const out = { replaced: groups.slice(0, MAX_REPLACED).map(g => ({ sku: g.sku, n: g.n })) };
  const more = groups.slice(MAX_REPLACED).reduce((n, g) => n + g.n, 0);
  if (more) out.replacedMore = more;
  return out;
}
const clean = v => {   // Firestore refuses undefined
  if (Array.isArray(v)) return v.map(clean);
  if (v && typeof v === "object") { const o = {}; for (const k of Object.keys(v)) if (v[k] !== undefined) o[k] = clean(v[k]); return o; }
  return v;
};
function withTimeout(fn, ms) {
  let t;
  const timer = new Promise((_, rej) => { t = setTimeout(() => rej(Object.assign(new Error("timeout"), { timeout: true })), ms); });
  return Promise.race([Promise.resolve().then(fn), timer]).finally(() => clearTimeout(t));
}
/** "1700763608" or any Etsy listing link ("…/listing/1700763608/swan-charm-necklace…?transaction_id=…") → the listing id, else "". */
function listingIdOf(v) {
  const s = String(v == null ? "" : v).trim();
  if (LISTING_RE.test(s)) return s;
  const m = /\/listing\/(\d{3,20})(?:[/?#]|$)/.exec(s);
  return m ? m[1] : "";
}
/** Why Etsy might not keep this SKU exactly as written ("" = it will): too long, a space at either end or twice in a row, a character that is not plain printable text. */
function skuProblem(sku) {
  if (typeof sku !== "string" || !sku) return "no SKU";
  if (sku.length > ETSY_SKU_MAX) return "longer than Etsy's " + ETSY_SKU_MAX + " characters";
  if (!PLAIN_SKU.test(sku)) return "it has a space at an end, two spaces in a row or a character Etsy may trim";
  if (!Master.isSku(sku)) return "it is not a master SKU";
  return "";
}
const linkId = listingId => "link-" + listingId;
const slug = sku => sku.replace(/[^A-Z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 40) || "SKU";

function make(deps) {
  const { db, bucket, etsy, meter } = deps;
  const clock = typeof deps.now === "function" ? deps.now : Date.now;
  const timeout = deps.timeoutMs > 0 ? deps.timeoutMs : TIMEOUT_MS;
  const maxCalls = deps.maxCalls > 0 ? Math.min(deps.maxCalls, MAX_CALLS) : MAX_CALLS;   // (a test may only LOWER the cap)

  /* the record is read, checked and the run claimed in ONE transaction: a second press while a run is in flight is refused (a lease of 60 s) */
  async function claim(ref, listingId, sku, by) {
    const t = clock();
    return db.runTransaction(async tx => {
      const snap = await tx.get(ref), d = snap.exists ? snap.data() || {} : null;
      let prev = d && d.link && typeof d.link === "object" ? d.link : { state: "pending" }, earlier = d && Array.isArray(d.earlier) ? d.earlier : [];
      if (d && d.sku !== sku) {   // a different SKU for the same listing: a new record that keeps a note of the old one
        if (+prev.runningUntil > t) return { refusal: "running" };
        earlier = earlier.concat([{ sku: d.sku, state: prev.state, beforePath: prev.beforePath || null, at: prev.at || d.at || null }]).slice(-5);
        prev = { state: "pending" };
      }
      if (prev.state === "linked") return { linked: { prev } };
      if (+prev.runningUntil > t) return { refusal: "running" };
      if (prev.state === "failed" && (prev.reason === "rejected" || prev.reason === "verify")) return { blocked: prev };   // never sent again unchanged: a person looks first
      if ((+prev.attempts || 0) >= MAX_ATTEMPTS) return { refusal: "attempts" };
      const link = clean(Object.assign({}, prev, { runningUntil: t + LEASE_MS }));
      tx.set(ref, clean({ kind: "listingSkuLink", listingId, sku, by, at: t, earlier, link }));
      return { prev, lease: t + LEASE_MS };
    });
  }
  async function mirrorState(listingId) {
    try {
      const s = await db.collection(MIRROR).doc(listingId).get();
      const v = s.exists ? (s.data() || {}).state : null;
      return typeof v === "string" && v.trim() ? v.trim().toLowerCase() : null;
    } catch (_) { return null; }
  }
  const linkedAnswer = (listingId, sku, prev) => ({ ok: true, state: "linked", calls: 0, listingId, sku, productIds: (Array.isArray(prev.productIds) ? prev.productIds : []).map(String), products: +prev.products || 0, verified: prev.verified !== false, ...(prev.already ? { already: true } : {}), ...(prev.beforePath ? { beforePath: prev.beforePath } : {}), ...(Array.isArray(prev.replaced) && prev.replaced.length ? { replaced: prev.replaced.map(g => ({ sku: String(g.sku == null ? "" : g.sku), n: +g.n || 0 })), ...(+prev.replacedMore > 0 ? { replacedMore: +prev.replacedMore } : {}) } : {}) });

  async function listingEtsySkuWrite(body) {
    body = body && typeof body === "object" ? body : {};
    if (isSandbox(body)) return answer("sandbox");
    const listingId = listingIdOf(body.listingId), sku = typeof body.sku === "string" ? body.sku : "";
    if (!listingId || !sku) return answer("bad-request");
    const shape = skuProblem(sku); if (shape) return answer("sku-shape", { details: [shape] });
    const by = String(body.by == null ? "" : body.by).replace(/[^\w .'-]/g, "").trim().slice(0, 80) || "operator";
    try {   // the master holds EXACTLY this SKU (the entry's own spelling), and it is not blocked
      const s = await db.collection(Master.INDEX).doc(sku).get(), e = s.exists ? s.data() || {} : null;
      if (!e || e.sku !== sku) return answer("sku-not-in-master");
      if (e.blocked) return answer("sku-blocked");
    } catch (e) { console.error("[etsyLink] master read", listingId, e && e.message); return answer("internal", { details: [e && e.message] }); }
    const ref = db.collection(SAVES).doc(linkId(listingId));
    let got;
    try { got = await claim(ref, listingId, sku, by); } catch (e) { console.error("[etsyLink] claim", listingId, e && e.message); return answer("internal", { details: [e && e.message] }); }
    if (got.refusal) return answer(got.refusal);
    if (got.blocked) return answer(got.blocked.reason, { details: got.blocked.details, beforePath: got.blocked.beforePath, skuMissing: got.blocked.skuMissing });
    if (got.linked) return linkedAnswer(listingId, sku, got.linked.prev);
    const out = await run(ref, listingId, sku, got.prev, got.lease);
    if (out.calls && meter && typeof meter.flushNow === "function") { try { await meter.flushNow(); } catch (_) { /* the counters are best effort */ } }
    return out;
  }

  async function run(ref, listingId, sku, prev, lease) {
    let calls = 0, putSent = false;
    let cur = {};   // the `link` record as this run writes it
    for (const k of CARRY) if (prev[k] !== undefined) cur[k] = prev[k];
    const priorPut = prev.putSent === true;   // a PUT of an earlier try may have left: this run reads first and decides

    const call = fn => {
      if (calls >= maxCalls) return Promise.reject(Object.assign(new Error("call cap"), { cap: true }));
      calls++;
      try { meter && meter.bumpSimple(SITE); } catch (_) { /* a counter never stops the work */ }
      return withTimeout(fn, timeout);
    };
    /* write the `link` record; the lease is released with the run's last write only (the "writing" note keeps it) */
    const settle = async (state, patch, midRun) => {
      const keep = {}; for (const k of CARRY) if (cur[k] !== undefined) keep[k] = cur[k];
      cur = clean(Object.assign(keep, patch || {}));
      try { await ref.update({ link: Object.assign({}, cur, { state, at: clock(), calls }, midRun ? { runningUntil: lease } : {}) }); return true; }
      catch (e) { console.warn("[etsyLink] could not update the record for", listingId, e && e.message); return false; }
    };
    const stop = async (reason, o) => {
      const a = answer(reason, Object.assign({ calls }, o));
      await settle(a.state, { reason: a.reason, text: a.text, retry: a.retry, details: a.details, skuMissing: o && o.skuMissing ? true : undefined, ...(o && o.putSent !== undefined ? { putSent: o.putSent } : {}) });
      return a;
    };
    /* an Etsy error → its reason. After the PUT, an error that may still have been applied (timeout, 502-504, no connection) is "unverified"; one that cannot have been (401/403/429, a refused body, 404) says the PUT did not land */
    const stopOn = (e, extra) => {
      const c = classify(e), after = extra && extra.afterPut;
      return stop(c.reason, Object.assign({}, extra, { details: c.details, state: c.ambiguous && after ? "unverified" : undefined, putSent: after ? !!c.ambiguous : undefined }));
    };

    try {
      /* 1 · the listing's state: the inbox mirror (no Etsy call), else one GET; only an active listing is written. Skipped when an earlier try already put. */
      if (!priorPut) {
        let state = await mirrorState(listingId);
        if (!state) {
          let l;
          try { l = await call(() => etsy.get("/listings/" + listingId)); } catch (e) { return await stopOn(e); }
          state = l && typeof l.state === "string" ? l.state.trim().toLowerCase() : "";
        }
        if (state !== "active") return await stop("listing-state", { listingState: String(state || "unknown").replace(/[^a-z_ ]/g, "").slice(0, 20) || "unknown" });
      }

      /* 2 · the live inventory, and what is on it */
      let inv;
      try { inv = await call(() => etsy.get("/listings/" + listingId + "/inventory", { legacy: false })); } catch (e) { return await stopOn(e); }
      const live = (Array.isArray(inv && inv.products) ? inv.products : []).filter(p => p && typeof p === "object" && !p.is_deleted);
      if (!live.length) return await stop("no-products");
      const groups = skuGroups(live);   // what the products carry now (for the answer and the record when it is more than one SKU)

      if (live.every(p => decodeOnce(p.sku == null ? "" : p.sku).trim() === sku)) {   // already on Etsy (an earlier try landed, or a person set it): no write
        let verified = false;
        if (cur.beforePath) {   // we wrote it: the live listing must be the before-copy with only the SKU changed
          let before = null;
          try { before = JSON.parse((await bucket.file(cur.beforePath).download())[0].toString("utf8")); } catch (_) { before = null; }
          const bl = before && Array.isArray(before.products) ? before.products.filter(p => p && !p.is_deleted) : null;
          if (bl && bl.length) {
            const cmp = compareInventories(buildPutBody(before, bl, new Set(bl), sku), inv);
            if (!cmp.ok) return await stop("verify", { details: cmp.differences, beforePath: cur.beforePath });
            verified = true;
          }
        }
        return await linked(listingId, sku, inv, live.length, calls, verified, settle, { already: !cur.beforePath || undefined, beforePath: cur.beforePath, replaced: cur.replaced, replacedMore: cur.replacedMore });
      }
      // an earlier try may have put: the listing must still be exactly as the before-copy was (Etsy did not apply it) before another PUT; anything else is for a person
      const canonNow = Canon.snapshotHash(inv);
      if (priorPut && cur.beforeCanon && canonNow !== cur.beforeCanon) return await stop("changed", { beforePath: cur.beforePath });

      /* 3 · the body, checked against what was read before it can leave */
      const all = new Set(live), putBody = buildPutBody(inv, live, all, sku);
      const problems = selfCheck(inv, live, putBody, all, sku);
      if (problems.length) return await stop("internal", { details: problems });

      /* 4 · the before-copy, in Storage and on the record, BEFORE the PUT; if either fails, nothing is sent */
      let attempt = (+cur.attempts || 0) + 1, beforePath = "";
      const beforeText = JSON.stringify(inv), beforeHash = sha256(beforeText);
      try {
        // a before-copy is never overwritten: a name already in Storage is skipped (the first one, -before.json, is the listing as it was before any write)
        for (let i = 0; i < 6; i++, attempt++) {
          beforePath = "charmnest/custom-saves/" + listingId + "/link-" + slug(sku) + "-before" + (attempt > 1 ? "-" + attempt : "") + ".json";
          if (!(await bucket.file(beforePath).exists())[0]) break;
          beforePath = "";
        }
        if (!beforePath) throw new Error("no free name for the safety copy");
        await bucket.file(beforePath).save(Buffer.from(beforeText, "utf8"), { resumable: false, contentType: "application/json", metadata: { cacheControl: "no-store" } });
        const [there] = await bucket.file(beforePath).exists();
        if (!there) throw new Error("the safety copy is not in Storage after the save");
        cur = Object.assign({}, cur, { attempts: attempt, backups: [...(Array.isArray(cur.backups) ? cur.backups : []), beforePath], beforePath, beforeHash, beforeCanon: canonNow, products: live.length, putSent: true }, replacedOf(groups));   // (the old SKUs are in the before-copy; the record says which and how many)
        if (!(await settle("writing", {}, true))) throw new Error("the safety copy is not noted on the record");
      } catch (e) {
        console.warn("[etsyLink] backup failed for", listingId, e && e.message);
        return await stop("backup-failed", { putSent: priorPut });
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
      return await linked(listingId, sku, echo, live.length, calls, true, settle, { beforePath, replaced: cur.replaced, replacedMore: cur.replacedMore });
    } catch (e) {
      console.error("[etsyLink]", listingId, e && e.message);
      return await stop("internal", { state: putSent ? "unverified" : undefined, details: [e && e.message] });
    }
  }

  /* 7 · linked: the record, and the listing's SKU table refreshed from Etsy's own answer (so the page sees the new SKU at once, no extra Etsy call) */
  async function linked(listingId, sku, readBack, n, calls, verified, settle, o) {
    const productIds = (Array.isArray(readBack && readBack.products) ? readBack.products : []).filter(p => p && !p.is_deleted).map(p => String(p.product_id == null ? "" : p.product_id)).filter(Boolean);
    const at = clock();
    const recorded = await settle("linked", { productIds, verified, sku, products: n, already: o.already, putSent: undefined });
    try { await db.collection(ListingSkus.COLL).doc(listingId).set(clean(ListingSkus.toStored(ListingSkus.compact(readBack, at)))); }
    catch (e) { console.warn("[etsyLink] SKU table not refreshed for", listingId, e && e.message); }
    const out = { ok: true, state: "linked", calls, listingId, sku, productIds, products: n, verified };
    if (o.already) out.already = true;
    if (o.beforePath) out.beforePath = o.beforePath;
    if (Array.isArray(o.replaced) && o.replaced.length) { out.replaced = o.replaced; if (+o.replacedMore > 0) out.replacedMore = +o.replacedMore; }
    if (!recorded) out.recorded = false;   // Etsy has the SKU; only the record could not be updated (a press makes one GET and writes it)
    return out;
  }

  return { listingEtsySkuWrite };
}

/* ── the real wiring (charmNestLibrary): built on first use, so loading this file touches neither Firebase nor Etsy ── */
let shared = null;
function real() {
  if (!shared) {
    const admin = require("./firebaseAdmin"), E = require("./_etsyMailEtsy");
    shared = make({
      db: admin.firestore(), bucket: admin.storage().bucket(), meter: require("./_etsyApiMeter"), now: Date.now,
      etsy: { get: (path, query) => E.etsyFetch(path, { query }), put: (path, query, body) => E.etsyFetch(path, { method: "PUT", query, body }) }
    });
  }
  return shared;
}
const ops = {
  listingEtsySkuWrite: async body => (isSandbox(body) ? answer("sandbox") : real().listingEtsySkuWrite(body))
};

module.exports = { make, ops, REASONS, internals: { listingIdOf, skuProblem, linkId } };
