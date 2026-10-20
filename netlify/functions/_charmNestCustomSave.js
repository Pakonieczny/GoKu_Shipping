/*  netlify/functions/_charmNestCustomSave.js
 *  Save a custom design to the master charm repository (Paul, 11 Oct 2026: the Designs window should "save the uploaded vector file
 *  to the master charm repository", give it a SKU, and link that SKU to the Etsy listing so the next purchase of it is already
 *  linked; 16 Oct 2026, KEEP: "super robust and completely self contained ... All eventualities must be fully robust").
 *  Rules for the page and the server: /tmp/kp/CONTRACT-KEEP.md sections B1-B8 (also the head of tests/charm-nest/keep-server-chaos.cjs).
 *
 *  customSaveCommit  (repo upload token; the HTTP gate is _charmNestRepoAuth.js LIBRARY.customSaveCommit)
 *      The browser has already put two files in Storage with the upload token: charmnest/master/custom/{SKU}.ai and {SKU}.png. This op
 *      checks they are there (and, when the page declared it, that they are the very bytes it built: file.aiSha256), then writes ALL the
 *      records in ONE transaction, create-only. The commit is CONVERGENT and keyed by the DESIGN (its charmHash), not by the SKU asked for:
 *        · the drawing is in the repository already (found here by charmHash, not by what a page cached)  -> that SKU is used, nothing new
 *          is stored (how "same-design"); a SKU asked for that belongs to ANOTHER drawing is refused (sku-taken: the page asks for the next);
 *        · the LISTING'S ANSWER (Charm_Sku_Aliases/{listing}.bySku[the line's SKU], or .sku for a line with no SKU) is never replaced:
 *          the same drawing already answering it is "already linked" (how "listing-same", only the record is written), ANOTHER design
 *          answering it is "listing-linked" (first link wins, nothing written); a dangling answer (its entry is gone) is replaced;
 *        · every part that is missing is completed by the next call (the index entry, the file record, the alias, the save record);
 *        · the same order with the same drawing again answers the record (how "again"); with ANOTHER drawing: already-saved sameFile:false.
 *      Records written (all create-only or merges, one transaction):
 *        Charm_Master_Index/{SKU}        created (ref.create: an existing SKU is never overwritten, never marked blocked: not masterPutIndex)
 *        Charm_Master_Files/{masterHash} created, or only its `skus` list grown when it is a record this op made (a real master's record is never touched)
 *        Charm_Sku_Aliases/{listingId}   merged: bySku[oldSku] = SKU (or sku + v:2 when the order had no SKU), and keep {rid, sku, oldSku, was, at, by}
 *        Charm_Custom_Saves/{rid}        created: the truth after a reload (one save per order and listing: {rid}, or {rid}-{listingId} when the
 *                                        order already holds another listing's save); the Etsy step reads ONLY this
 *        Charm_Custom_Intents/{rid}      only when the page recorded an intent: set to "done"
 *      reuse:true (a design the page found in its library): the same, writing only the alias and the save record.
 *      Refuses sandbox:true on the way in, before any read. Expected refusals are HTTP 200 with ok:false (status:200), so the page's
 *      api() does not throw and the engine reads the answer. After a success the op best-effort removes ORPHAN files (see sweep).
 *
 *  KEEPOVERRIDE (Paul, 20 Oct 2026): "If a user engages the toggle to update the charm repository, then that's exactly what needs to happen ... If there is an existing charm that is incorrectly
 *  matched to a given Etsy listing/order then the system must remove the SKU from the offending charm and attach it to the newly uploaded charm in the Charm repo." A commit that carries
 *  override:{ via, tag, from } is that explicit act (the designs window's switch on a line that already reads a library charm X). The toggle wins over "first link wins", for THIS listing only:
 *    via "own"   the line's own SKU S is X's (Charm_Master_Index/{S} exists with another drawing): the entry S is REPLACED by the new design (its files go to {S}~{tag}.ai|png, so X's
 *                files are never written over), X keeps its other SKUs (the entries that share its file); S was X's only SKU: X is retired. Other save records of S are marked superseded.
 *    via "link"  the line reads X through the listing's link (an alias, a spelling): the design is saved under its own SKU as before and the listing's answer is REPLACED (X keeps its SKUs).
 *    either way  this order's own record of another drawing is replaced, and ONE audit record is written in the same transaction (Charm_Keep_Overrides/{rid}-{listing}-{at}: who, when,
 *                which order, the whole old entry, the old answer, the replaced record), so the act can be read back and undone by the same switch with the old design.
 *  Without override nothing here changes: first link wins, a SKU of another drawing is sku-taken.
 *  customSaveGet     (read only) { rid, listingId? } -> { ok, save: null | { id, rid, sku, phase, reused, etsy, at, by, ... }, intent? }, one or two reads.
 *  customSavePending (read only) {} -> { ok, pending: [{ rid, listingId, sku, etsy, at, linkNeedsEtsy }] }: saved designs whose Etsy step is not done.
 *  customSaveIntent  (no token; writes only Charm_Custom_Intents/{rid}, never the repository) { rid, listingId, hash, name, by, state }.
 *
 *  Nothing here calls Etsy. The SKU rule (what a SKU may be, what is reserved) is the page's own module, charm-nest-sku.js, so the
 *  server and the page cannot drift. Dependencies are passed in ({ db, FV, tokenUrl, bucket, now }) so a test runs on fakes.            */
"use strict";
const { str, num } = require("./_charmNestAuth");
const Master = require("./_charmNestMaster");

const COLL = "Charm_Custom_Saves", INTENTS = "Charm_Custom_Intents", ALIASES = "Charm_Sku_Aliases", INDEX = Master.INDEX, FILES = Master.FILES;
const OVERRIDES = "Charm_Keep_Overrides";   // KEEPOVERRIDE (Paul, 20 Oct 2026): one audit record per explicit "the toggle wins" commit (who, when, which order, what was replaced)
const TAG_RE = /^[0-9a-z]{6,16}$/;
const DIR = "charmnest/master/custom/";
const ORPHAN_MS = 24 * 3600 * 1000, SWEEP_SKUS = 20, SWEEP_LIST = 200;   // (KEEP B8: files with no index entry go after a day, at most 20 SKUs a sweep)
const PENDING_MAX = 25, COOLDOWN_MS = 60 * 1000, SCOPE_COOLDOWN_MS = 30 * 60 * 1000;
const OLD_MAX = 120, KEY_MAX = 120;                                  // (the line's SKU as kept on the record / as an alias key: the length aliasPut keeps)
const SHA_RE = /^[0-9a-f]{64}$/i, MAX_HASHED = 40 * 1024 * 1024;
/** KEEP B9: a commit is checked against the hash of the .ai the page built when it sends one (file.aiSha256). requireFileHash makes the hash mandatory (a call without it is a 400): false until every page sends it. */
const config = { requireFileHash: false };
const SKU_RE = /^[A-Z0-9][A-Z0-9_-]{1,31}$/;                 // what a custom SKU may be: letters, digits, _ and - only, at most 32 (Etsy's limit)
const RID_RE = /^\d{5,20}$/, LISTING_RE = /^\d{3,20}$/, ID_RE = /^\d{1,20}$/, HASH_RE = /^[0-9a-f]{8,64}$/i;
const MIN_AI_BYTES = 200;
const pathsOf = sku => ({ aiPath: DIR + sku + ".ai", thumbPath: DIR + sku + ".png" });
/** KEEPOVERRIDE (Paul, 20 Oct 2026): the stem of a file under custom/: {SKU}.ai, or {SKU in file letters}__k{tag}.ai (charm-nest-sku.js fileKey). A tagged stem belongs to the entry whose file it is. */
const stemOfFile = name => { const m = /^charmnest\/master\/custom\/([^\/]+)\.(?:ai|png)$/.exec(String(name || "")); return m ? m[1] : ""; };
const TAGGED_RE = /__k[0-9a-z]{6,16}$/;

/* ── answers ── */
const refuse = (error, extra) => Object.assign({ ok: false, error, status: 200 }, extra || {});   // (status 200: an expected refusal is not an HTTP error)
const bad = (field, why) => ({ error: "bad-request", field, why });                                // (a malformed call: HTTP 400)
const isSandbox = b => b && (b.sandbox === true || b.sandbox === 1 || b.sandbox === "1" || b.sandbox === "true");
const alreadyExists = e => !!e && (e.code === 6 || e.code === "already-exists" || /ALREADY_EXISTS/i.test(String(e.message || "")));
const warn = (what, e) => { try { console.warn("[customSave] " + what + ":", String((e && (e.message || e.code)) || e).slice(0, 160)); } catch (_) {} };

/* The page's SKU module (reserved families, -CO, (HUGGIE), indexed endings): required where it is used, with a literal path, so the
   bundle holds it and the server answers by the very code the page ran. A test may pass its own in deps.Sku. */
function skuRules(deps) { return (deps && deps.Sku) || require("../../charm-nest-sku.js"); }

/* ── what a call must say ── */
function optionPairsOf(v) {
  const out = [];
  for (const p of Array.isArray(v) ? v.slice(0, 10) : []) {
    const a = Array.isArray(p) ? p[0] : p && (p.propertyId != null ? p.propertyId : p.property_id != null ? p.property_id : p.p);
    const b = Array.isArray(p) ? p[1] : p && (p.valueId != null ? p.valueId : p.value_id != null ? p.value_id : p.v);
    const x = String(a == null ? "" : a), y = String(b == null ? "" : b);
    if (ID_RE.test(x) && ID_RE.test(y)) out.push({ propertyId: x, valueId: y });   // (a list of maps: Firestore refuses an array directly inside an array)
  }
  return out;
}
function groupOf(g) {
  const mode = g && ["all", "some", "unknown"].includes(g.mode) ? g.mode : "unknown";
  const ids = [...new Set((g && Array.isArray(g.productIds) ? g.productIds : []).map(x => String(x == null ? "" : x)).filter(x => ID_RE.test(x)))].slice(0, 100);
  return { mode, productIds: ids };
}
/** KEEP B4, the line's SKU as an alias key: one that charm-nest-orders.js bySkuOf can look up and Firestore can hold as a map key. Anything else is NOT an error: the repository
    step still saves, the listing is then linked by the Etsy step alone (alias "none"). */
const usableKey = s => !!s && s.length <= KEY_MAX && !/^__.*__$/.test(s) && !/[\u0000-\u001f\u007f]/.test(s);   // (a "/" is fine: charmNestLibrary aliasPut keys the same SKUs the same way)
/** The line's SKU as the record keeps it: trimmed, upper case (spaces inside kept: Etsy keeps them), control characters turned into spaces, at most 120. */
const oldSkuOf = v => str(v, 400).replace(/[\u0000-\u001f\u007f]+/g, " ").trim().toUpperCase().slice(0, OLD_MAX).trim();
/** A SKU longer than the record keeps would be cut, and a cut SKU is no key any order carries: it is not an alias key (the Etsy step links it). */
const wasCut = v => str(v, 400).replace(/[\u0000-\u001f\u007f]+/g, " ").trim().length > OLD_MAX;
/** Reads and checks a commit body: { ok:true, v } with clean values, or { ok:false, answer } (HTTP 400 for a malformed call). */
function read(b, deps) {
  const rid = str(b.rid, 30).trim(), listingId = str(b.listingId, 30).trim(), sku = str(b.sku, 80).trim().toUpperCase();
  if (!RID_RE.test(rid)) return { answer: bad("rid", "the order number") };
  if (!LISTING_RE.test(listingId)) return { answer: bad("listingId", "the listing number") };
  let rules; try { rules = skuRules(deps); } catch (e) { warn("sku rules not loaded", e); return { answer: { error: "unavailable", status: 503 } }; }
  const oldSku = oldSkuOf(b.oldSku);
  // KEEPOVERRIDE / KEEPOWNSKU (Paul, 20 Oct 2026): the design may be saved under the order's OWN Etsy SKU (etsyOwn: exactly the line's SKU as the library keys it), or under the library SKU the line
  // already reads (override via "own"): those SKUs are the shop's, any name the library can hold; every other SKU is one the save makes (letters, digits, _ and -, 32 at most)
  const o = b.override && typeof b.override === "object" ? b.override : null, via = o && (o.via === "own" || o.via === "link") ? o.via : "";
  if (o && !via) return { answer: bad("override.via", "own or link") };
  const R = rules.ownWhy && rules.fileKey ? rules : require("../../charm-nest-sku.js");
  const etsyOwn = b.etsyOwn === true && !!oldSku && sku === oldSku.replace(/\//g, "-");
  const shop = (etsyOwn || via === "own") && Master.isSku(sku) && !R.ownWhy(sku);
  if (!(SKU_RE.test(sku) || shop) || !Master.isSku(sku)) return { answer: bad("sku", "letters, digits, _ and - only, at most 32") };
  if (!shop && rules.reserved(sku)) return { answer: refuse("reserved", { sku }) };
  const productId = ID_RE.test(String(b.productId == null ? "" : b.productId)) ? String(b.productId) : "";
  const d = b.design && typeof b.design === "object" ? b.design : {}, f = b.file && typeof b.file === "object" ? b.file : {}, e = b.entry && typeof b.entry === "object" ? b.entry : {};
  const name = str(d.name || f.name, 200).trim() || sku + ".ai";
  const masterHash = str(f.masterHash || d.hash, 80).trim().toLowerCase();
  const reuse = b.reuse === true;
  // (the files: {SKU}.ai, or {SKU in file letters}__k{tag}.ai for a SKU taken from another drawing (its files stay apart, for the audit) or one with other characters: Sku.fileKey, as the page uploads them)
  const tag = str(b.fileTag || (o && o.tag), 20).trim().toLowerCase(), forced = via === "own" && !reuse, fileKey = R.fileKey(sku, tag, forced);
  if (!reuse && fileKey !== sku && !TAG_RE.test(tag)) return { answer: bad("fileTag", "6 to 16 letters or digits") };
  const override = via ? { via, from: str(o.from, 80).trim().toUpperCase() } : null;
  const declared = { aiSha256: SHA_RE.test(str(f.aiSha256, 80).trim()) ? str(f.aiSha256, 80).trim().toLowerCase() : "", aiBytes: Math.max(0, Math.floor(num(f.aiBytes))), pngSha256: SHA_RE.test(str(f.pngSha256, 80).trim()) ? str(f.pngSha256, 80).trim().toLowerCase() : "" };
  const v = { rid, listingId, sku, oldSku, oldCut: wasCut(b.oldSku), productId, optionPairs: optionPairsOf(b.optionPairs), group: groupOf(b.group), title: str(b.title, 200), by: str(b.by, 80) || "operator", reuse, declared, override, etsyOwn, fileKey: reuse ? sku : fileKey,
    design: { name, kind: d.kind === "dxf" ? "dxf" : "ai", hash: str(d.hash || masterHash, 80).toLowerCase(), bytes: Math.max(0, Math.floor(num(d.bytes))) } };
  const charmHash = str(e.charmHash, 80).trim();
  if (!reuse && config.requireFileHash && !declared.aiSha256) return { answer: bad("file.aiSha256", "the hash of the .ai the page built") };
  if (reuse) {
    if (!charmHash) return { answer: bad("entry.charmHash", "the design to reuse") };
    v.entry = { charmHash };
    return { ok: true, v };
  }
  if (!HASH_RE.test(masterHash)) return { answer: bad("file.masterHash", "the uploaded file's hash") };
  if (!charmHash) return { answer: bad("entry.charmHash", "the design's hash") };
  const w = num(e.widthPt), h = num(e.heightPt), area = num(e.areaPt2), members = Math.floor(num(e.members)), holes = Math.floor(num(e.holes));
  if (!(w > 0 && w < 20000 && h > 0 && h < 20000)) return { answer: bad("entry.size", "width and height") };
  if (!(area >= 0 && members >= 1 && members <= 5000 && holes >= 0 && holes <= 5000)) return { answer: bad("entry.shape", "area, members and holes") };
  v.masterHash = masterHash;
  v.entry = { charmHash, widthPt: w, heightPt: h, areaPt2: area, members, holes, engravable: e.engravable !== false, upAngle: e.upAngle == null || e.upAngle === "" ? null : num(e.upAngle), upSource: str(e.upSource || "drawn", 20) };
  return { ok: true, v };
}

/* ── the order's save record: {rid}, or {rid}-{listingId} when the order already holds ANOTHER listing's save (an order with two custom lines) ── */
const sameListing = (d, listingId) => !d || !d.listingId || String(d.listingId) === String(listingId);
/** Which of the two documents is this listing's: { ref, snap } (the snap may not exist yet: the id a new record would take). */
function pick(ridRef, ridSnap, listRef, listSnap, listingId) {
  if (ridSnap.exists && !sameListing(ridSnap.data(), listingId)) return { ref: listRef, snap: listSnap };
  if (!ridSnap.exists && listSnap && listSnap.exists) return { ref: listRef, snap: listSnap };
  return { ref: ridRef, snap: ridSnap };
}
/** The record of an order (and listing, when the caller knows it) outside a transaction: { ref, snap }. Without a listing id it is {rid}. */
async function recordFor(db, rid, listingId) {
  const ridRef = db.collection(COLL).doc(rid);
  if (!LISTING_RE.test(String(listingId == null ? "" : listingId))) return { ref: ridRef, snap: await ridRef.get() };
  const listRef = db.collection(COLL).doc(rid + "-" + listingId);
  const [a, c] = await db.getAll(ridRef, listRef);
  return pick(ridRef, a, listRef, c, String(listingId));
}
/** Is the drawing a record holds the drawing this call carries? By charm hash when both have one, else by the dropped file's hash, else by the SKU (a record from before the hash was kept). */
function sameDrawing(d, v) {
  const dc = str(d.charmHash, 80), vc = v.entry && v.entry.charmHash ? String(v.entry.charmHash) : "";
  if (dc && vc) return dc === vc;
  const dh = str((d.design && d.design.hash) || d.masterHash, 80).toLowerCase(), vh = v.reuse ? "" : str(v.design.hash, 80).toLowerCase();
  if (dh && vh) return dh === vh;
  return String(d.sku || "").toUpperCase() === v.sku;
}

/* ── Storage: both files are there, the .ai is a real file, and (when the page said what it built) it is that file ── */
const sha256 = buf => require("crypto").createHash("sha256").update(buf).digest("hex");
async function checkFiles(bucket, v) {
  const { aiPath, thumbPath } = pathsOf(v.fileKey || v.sku), ai = bucket.file(aiPath), png = bucket.file(thumbPath), dec = v.declared || {};
  let which = "ai";
  try {
    const [aiThere] = await ai.exists(); if (!aiThere) return refuse("not-uploaded", { which: "ai", why: "missing" });
    which = "thumb"; const [pngThere] = await png.exists(); if (!pngThere) return refuse("not-uploaded", { which: "thumb", why: "missing" });
    which = "ai";
    const [meta] = await ai.getMetadata(), size = Number(meta && meta.size);
    if (!(size > MIN_AI_BYTES)) return refuse("bad-file", { which: "ai", why: "too-small" });
    // another save that chose the same SKU a moment ago uploads to the very same path: bytes that are not the ones this page built are NOT this design's file (KEEP K4)
    const other = () => refuse("sku-taken", { sku: v.sku, by: "upload", why: "changed" });
    if (dec.aiBytes && size !== dec.aiBytes) return other();
    let head;
    if (dec.aiSha256 && size <= MAX_HASHED) {
      const [all] = await ai.download(); head = Buffer.from(all).slice(0, 8);
      if (sha256(Buffer.from(all)) !== dec.aiSha256) return other();
    } else { const [h] = await ai.download({ start: 0, end: 7 }); head = h; }
    if (!head || Buffer.from(head).slice(0, 4).toString("latin1") !== "%PDF") return refuse("bad-file", { which: "ai", why: "not-pdf" });
    if (dec.pngSha256) {
      which = "thumb";
      const [pm] = await png.getMetadata();
      if (Number(pm && pm.size) <= MAX_HASHED) { const [pb] = await png.download(); if (sha256(Buffer.from(pb)) !== dec.pngSha256) return other(); }
    }
    return null;
  } catch (e) {
    // a file that went between the look and the read (a zero-age repair, a Library delete, the day-old sweep) is a file that is not there: the page uploads it again, once
    if (e && (e.code === 404 || e.code === "404" || e.code === "ENOENT" || /no blob|not found|no such object/i.test(String(e.message || "")))) return refuse("not-uploaded", { which, why: "missing" });
    throw e;
  }
}

/* ── the sweep (KEEP B8, K5): files under charmnest/master/custom/ whose SKU has no index entry and that are a day old are the leftovers of an upload whose commit never came ── */
async function sweep(deps) {
  const out = { listed: 0, orphans: [], deleted: 0 };
  try {
    const { db, bucket } = deps, now = (typeof deps.now === "function" ? deps.now : Date.now)();
    if (!bucket || typeof bucket.getFiles !== "function") return out;
    const res = await bucket.getFiles({ prefix: DIR, autoPaginate: false, maxResults: SWEEP_LIST }), files = (res && res[0]) || [];
    out.listed = files.length;
    const bySku = new Map();
    for (const f of files) { const k = stemOfFile(f && f.name); if (k) { if (!bySku.has(k)) bySku.set(k, []); bySku.get(k).push(f); } }
    const skus = [...bySku.keys()].slice(0, 100);
    if (!skus.length) return out;
    const docs = await db.getAll(...skus.map(s => db.collection(INDEX).doc(s)), { fieldMask: ["sku"] });
    for (let i = 0; i < skus.length && out.orphans.length < SWEEP_SKUS; i++) {
      if (docs[i] && docs[i].exists) continue;
      if (TAGGED_RE.test(skus[i])) { const q = await db.collection(INDEX).where("aiPath", "==", DIR + skus[i] + ".ai").limit(1).get(); if (!q.empty) continue; }   // (KEEPOVERRIDE: a tagged file is kept while an entry draws from it)
      let old = true;
      for (const f of bySku.get(skus[i])) {
        let t = NaN; try { const [m] = await f.getMetadata(); t = Date.parse((m && (m.updated || m.timeCreated)) || ""); } catch (_) { t = NaN; }
        if (!Number.isFinite(t) || now - t < ORPHAN_MS) { old = false; break; }
      }
      if (!old) continue;
      out.orphans.push(skus[i]);
      for (const f of bySku.get(skus[i])) { try { await f.delete({ ignoreNotFound: true }); out.deleted++; } catch (e) { warn("orphan not removed " + f.name, e); } }
    }
  } catch (e) { warn("sweep", e); }
  return out;
}

/** The two files a call uploaded under its candidate SKU, removed when nothing owns them: no index entry has that SKU (re-read right before each delete). Best effort, never an error. */
async function dropCandidate(deps, v) {
  try {
    if (v.reuse || !deps.bucket) return;
    const held = await deps.db.collection(INDEX).doc(v.sku).get(), { aiPath, thumbPath } = pathsOf(v.fileKey || v.sku);
    // (KEEPOVERRIDE: an override's own files ({SKU}~{tag}) are litter when the entry does not use them: the SKU is held, by another file)
    if (held.exists && !(v.fileKey && v.fileKey !== v.sku && str((held.data() || {}).aiPath, 400) !== aiPath)) return;
    for (const p of [aiPath, thumbPath]) { try { await deps.bucket.file(p).delete({ ignoreNotFound: true }); } catch (e) { warn("candidate file not removed " + p, e); } }
  } catch (e) { warn("candidate files not dropped", e); }
}

/* ── the commit ── */
async function commit(deps, b) {
  b = b || {};
  if (isSandbox(b)) return refuse("sandbox");   // the sandbox is a rehearsal: it never writes the repository, an alias or Etsy; refused before anything is read
  const { db, FV } = deps, got = read(b, deps);
  if (!got.ok) return got.answer;
  const v = got.v, now = (typeof deps.now === "function" ? deps.now : Date.now)(), col = n => db.collection(n);
  const ridRef = col(COLL).doc(v.rid), listRef = col(COLL).doc(v.rid + "-" + v.listingId), indexRef = col(INDEX).doc(v.sku), aliasRef = col(ALIASES).doc(v.listingId), intentRef = col(INTENTS).doc(v.rid);
  const fileRef = v.reuse ? null : col(FILES).doc(v.masterHash);
  const linksOf = async e => { if (!e) return e; const [a, t] = await Promise.all([e.aiPath ? deps.tokenUrl(e.aiPath) : null, e.thumbPath ? deps.tokenUrl(e.thumbPath) : null]); if (a) e.aiUrl = a; if (t) e.thumbUrl = t; return e; };
  const up = s => String(s == null ? "" : s).trim().toUpperCase();
  const dataOf = s => (s && s.exists ? s.data() || {} : null);
  // a design an order may be given by the server: a real entry, not blocked, whose SKU is one the Etsy step can write and the page's rules allow (a master design named "FOO BAR" is the Library's, not this save's)
  const rules = skuRules(deps), usable = x => !!x && !!x.sku && !!x.aiPath && !x.blocked && SKU_RE.test(String(x.sku)) && !rules.reserved(String(x.sku));
  const mode = v.oldSku === "" ? "whole" : usableKey(v.oldSku) && !v.oldCut ? "bySku" : "none";   // KEEP B4: how the listing is answered (none: only the Etsy step links it)

  const ov = v.override, own = !!ov && ov.via === "own", auditId = v.rid + "-" + v.listingId + "-" + now;   // KEEPOVERRIDE (Paul, 20 Oct 2026): the switch's explicit "the toggle wins"
  /* Everything is decided from what the transaction reads, so a retry (a concurrent writer, a lost reply, a record cut half way) starts from the truth. `made` holds the Storage links once the files were checked. */
  const decide = async (tx, made) => {
    const reads = [ridRef, listRef, indexRef, aliasRef, intentRef].concat(fileRef ? [fileRef] : []);
    const snaps = await tx.getAll(...reads), [ridSnap, listSnap, indexSnap, aliasSnap, intentSnap] = snaps, fileSnap = fileRef ? snaps[5] : null;
    const rec = pick(ridRef, ridSnap, listRef, listSnap, v.listingId);

    // 1 · this order's record: the same drawing again answers it, another drawing is refused (the first design of an order stays)
    let stale = null, replaced = null;   // a record whose entry is GONE (a Library delete, a torn store): it promises a design that does not exist, so this call makes the design again and replaces the record
    if (rec.snap.exists) {
      const d = rec.snap.data() || {}, e = up(d.sku);
      if (!sameDrawing(d, v)) {
        if (!ov) return { refusal: refuse("already-saved", { sku: d.sku || "", sameFile: false, listingId: String(d.listingId || "") }) };
        stale = d; replaced = d;   // KEEPOVERRIDE: the toggle wins: this order's record of ANOTHER drawing is replaced (it is kept whole in the audit record)
      }
      const entry = replaced ? null : e === v.sku ? dataOf(indexSnap) : dataOf(await tx.get(col(INDEX).doc(e)));
      if (replaced) { /* (decided below, as a new save) */ }
      else if (!entry || !entry.sku) stale = d;
      else {
        // the record is the truth: what it promised and is missing is put back (the listing's answer, when NOTHING answers it now; the intent, when it is still open)
        const old = dataOf(aliasSnap) || {}, was = d.alias === "bySku" && d.oldSku ? up(old.bySku && old.bySku[d.oldSku]) : d.alias === "whole" ? up(old.sku) : "x";
        if (!was && String(d.listingId || v.listingId) === v.listingId) {
          const alias = { listingId: v.listingId, by: str(d.by, 80) || v.by, title: str(d.title, 200), updatedAt: FV.serverTimestamp(), keep: { rid: v.rid, sku: e, oldSku: str(d.oldSku, 120), was: "", at: now, by: str(d.by, 80) || v.by } };
          if (d.alias === "bySku") alias.bySku = { [d.oldSku]: e }; else { alias.sku = e; alias.v = 2; }
          await tx.set(aliasRef, alias, { merge: true });
        }
        if (intentSnap.exists && (intentSnap.data() || {}).state !== "done") await tx.set(intentRef, { state: "done", sku: e, updatedAt: FV.serverTimestamp() }, { merge: true });
        return { done: "again", how: "again", sku: e, id: rec.ref.id, save: d, entry, created: false };
      }
    }

    // 2 · which entry carries the drawing: the design is looked for by its hash HERE, not in what a page cached
    const ex = dataOf(indexSnap);
    let target, creating = false, copyOf = null, overriding = null;
    if (v.reuse) {
      // an identical design the repository already holds: its SKU must be there, usable, and the very same drawing
      if (!ex || !ex.sku || !ex.aiPath) return { refusal: refuse("reuse-missing", { sku: v.sku }) };
      if (ex.blocked) return { refusal: refuse("reuse-blocked", { sku: v.sku }) };
      if (str(ex.charmHash, 80) !== v.entry.charmHash) return { refusal: refuse("reuse-mismatch", { sku: v.sku }) };
      target = { sku: v.sku, data: ex };
    } else {
      const q = await tx.get(col(INDEX).where("charmHash", "==", v.entry.charmHash).limit(8));
      const hits = q.docs.map(x => x.data() || {}).filter(x => usable(x) && String(x.charmHash || "") === v.entry.charmHash)
        .sort((a, c) => (c.sku === v.sku) - (a.sku === v.sku) || String(a.sku).localeCompare(String(c.sku)));
      if (own || v.etsyOwn) {
        // KEEPOVERRIDE via "own": the line's own SKU takes the new design, whatever drawing holds it now (that drawing keeps its other SKUs; it is kept whole in the audit record)
        // KEEPOWNSKU: the order's own Etsy SKU takes it (a SKU no drawing holds: the listing had no match); the same drawing under another SKU is not stored twice (S joins it)
        if (ex && ex.sku && str(ex.charmHash, 80) === v.entry.charmHash && ex.aiPath && !ex.blocked) target = { sku: v.sku, data: ex };
        else if (indexSnap.exists && ex && ex.sku && !own) return { refusal: refuse("sku-taken", { sku: v.sku, by: "master" }) };
        else {
          const hit = hits.find(x => x.sku !== v.sku) || null;   // (the new design is in the repository already under another SKU: S joins that drawing, nothing is stored twice)
          target = { sku: v.sku, data: null }; if (indexSnap.exists) overriding = ex || {};
          if (hit) copyOf = hit; else creating = true;
        }
      }
      else if (hits.length) target = { sku: hits[0].sku, data: hits[0] };
      else if (ex) return { refusal: refuse("sku-taken", { sku: v.sku, by: "master" }) };   // (another drawing holds the SKU: the page asks for the next candidate)
      else { creating = true; target = { sku: v.sku, data: null }; }
    }

    // 3 · the listing's answer is never replaced: the same drawing there is "already linked", another design there is "listing-linked" (first link wins)
    const old = dataOf(aliasSnap) || {};
    const rawPrior = mode === "bySku" ? up(old.bySku && old.bySku[v.oldSku]) : mode === "whole" ? up(old.sku) : "";
    let prior = rawPrior, aliasWas = "";
    if (mode === "bySku" && v.oldSku === target.sku) prior = "";
    let linked = false;
    if (prior && prior !== target.sku) {
      const pe = dataOf(await tx.get(col(INDEX).doc(prior)));
      if (pe && pe.sku && !pe.blocked) {
        if (!own && str(pe.charmHash, 80) === v.entry.charmHash && usable(pe)) { target = { sku: pe.sku, data: pe }; creating = false; copyOf = null; linked = true; }   // the same drawing under another SKU already answers: that SKU stays
        else if (ov) aliasWas = prior;   // KEEPOVERRIDE: the toggle wins over "first link wins": this listing's answer is replaced below (the old one is kept in keep.was and the audit record)
        else return { refusal: refuse("listing-linked", { sku: pe.sku, by: pe.hashSource === "custom-save" ? "save" : "repository" }) };
      }   // (an answer whose entry is gone is dangling: it is replaced below, and kept in keep.was)
    } else if (prior && prior === target.sku && !creating) linked = true;
    const aliasOwn = mode === "bySku" && v.oldSku === target.sku;   // the line already carries the design's own SKU: nothing to alias
    const fixOwn = !!ov && aliasOwn && !!rawPrior && rawPrior !== target.sku;   // (KEEPOVERRIDE: ... unless the listing answers that SKU with another one: the answer is put right, to the SKU itself)

    // 4 · a new entry needs its files, checked outside the transaction
    if (creating && !made) return { need: "files" };

    // KEEPOVERRIDE: what an override replaces is read before anything is written: the SKUs that stay with the old drawing (its card), the other save records of that SKU (superseded)
    let others = [], supersede = [];
    if (overriding) {
      const by = overriding.aiPath ? ["aiPath", str(overriding.aiPath, 400)] : overriding.charmHash ? ["charmHash", str(overriding.charmHash, 80)] : null;
      if (by) { const q = await tx.get(col(INDEX).where(by[0], "==", by[1]).limit(40)); others = q.docs.map(x => x.id).filter(id => id && id !== v.sku).sort(); }
      const ss = await tx.get(col(COLL).where("sku", "==", v.sku).limit(20));
      supersede = ss.docs.filter(x => x.id !== rec.ref.id && !(x.data() || {}).superseded && str((x.data() || {}).charmHash, 80) !== v.entry.charmHash).map(x => x.ref);
    }
    const acted = !!ov && !!(overriding || aliasWas || replaced || fixOwn);
    const stamp = acted ? { audit: auditId, via: ov.via, at: now, by: v.by, rid: v.rid, listingId: v.listingId } : null;
    const fromOf = () => (overriding ? { sku: v.sku, skus: others.slice(0, 20), retired: !others.length, charmHash: str(overriding.charmHash, 80), aiPath: str(overriding.aiPath, 400), masterHash: str(overriding.masterHash, 80) } : null);

    // 5 · the writes
    let entry = target.data, masterHash = entry ? entry.masterHash || "" : v.masterHash, aiPath = entry ? entry.aiPath : "", thumbPath = entry ? entry.thumbPath || "" : "";
    if (creating) {
      ({ aiPath, thumbPath } = pathsOf(v.sku));
      const e = v.entry;
      entry = { sku: v.sku, masterHash, masterPath: aiPath, masterName: v.design.name, charmHash: e.charmHash, widthPt: e.widthPt, heightPt: e.heightPt, areaPt2: e.areaPt2, members: e.members, holes: e.holes,
        engravable: e.engravable, engravableBy: "index", upAngle: e.upAngle, upSource: e.upSource, aiPath, thumbPath, aiUrl: str(made.aiUrl, 900), thumbUrl: str(made.thumbUrl, 900),
        open: false, labelSource: "text", confidence: null, indexedAt: FV.serverTimestamp(), indexedAtMs: now, firstIndexedAt: FV.serverTimestamp(), hashSource: "custom-save" };
      if (overriding) entry.override = Object.assign({}, stamp, { from: fromOf() });
      if (indexSnap.exists) await tx.set(indexRef, entry); else await tx.create(indexRef, entry);   // (KEEPOVERRIDE: the entry of the line's own SKU is replaced whole; X's files stay where they are)
      if (!fileSnap.exists) await tx.set(fileRef, { masterHash, path: aiPath, url: str(made.aiUrl, 900), name: v.design.name, charms: 1, labelled: 1, unlabelled: [], orphans: [], duplicates: [], undecodable: 0, blocked: [],
        unlabelledCount: 0, orphanCount: 0, duplicateCount: 0, blockedCount: 0, skus: [v.sku], indexedAt: FV.serverTimestamp(), indexedAtMs: now, indexedBy: "custom-save" });
      else if ((fileSnap.data() || {}).indexedBy === "custom-save") { const skus = Array.isArray((fileSnap.data() || {}).skus) ? fileSnap.data().skus : []; if (!skus.includes(v.sku)) await tx.set(fileRef, { skus: skus.concat([v.sku]).slice(0, 2000) }, { merge: true }); }
      // (a record of a real master file that happens to hold the same bytes is left exactly as it is)
    } else if (copyOf) {
      // KEEPOVERRIDE: the new design is in the repository already under another SKU: the line's SKU joins that drawing (its card lists it), marked a design saved from an order so a master file never writes it back
      entry = Object.assign({}, copyOf, { sku: v.sku, masterHash: v.masterHash || copyOf.masterHash || "", hashSource: "custom-save", attachedFrom: copyOf.sku, indexedAt: FV.serverTimestamp(), indexedAtMs: now });
      delete entry.blocked; delete entry.conflict;
      if (overriding) entry.override = Object.assign({}, stamp, { from: fromOf() });
      masterHash = entry.masterHash; aiPath = entry.aiPath || ""; thumbPath = entry.thumbPath || "";
      if (indexSnap.exists) await tx.set(indexRef, entry); else await tx.create(indexRef, entry);
    }
    const sku = target.sku;
    if (mode !== "none" && ((!aliasOwn && !linked && prior !== sku) || fixOwn)) {
      // the listing's alias: the old SKU answers with the new one (or, with no old SKU, the listing does); a dangling answer that stood there is kept in `keep.was` (KEEPOVERRIDE: and the one the toggle replaced)
      const alias = { listingId: v.listingId, by: v.by, title: v.title, updatedAt: FV.serverTimestamp(), keep: { rid: v.rid, sku, oldSku: v.oldSku, was: aliasWas || (fixOwn ? rawPrior : prior), at: now, by: v.by } };
      if (stamp) alias.keep.override = { audit: auditId, via: ov.via };
      if (mode === "bySku") alias.bySku = { [v.oldSku]: sku }; else { alias.sku = sku; alias.v = 2; }
      await tx.set(aliasRef, alias, { merge: true });
    }
    if (acted) {
      // KEEPOVERRIDE: the other save records of the moved SKU now promise a drawing it no longer has: superseded (the repair pass and the Etsy queue leave them), and ONE audit record of the act
      for (const r of supersede) await tx.set(r, { superseded: { at: now, by: v.by, rid: v.rid, audit: auditId } }, { merge: true });
      await tx.set(col(OVERRIDES).doc(auditId), { at: now, by: v.by, rid: v.rid, listingId: v.listingId, sku, oldSku: v.oldSku, via: ov.via, fromSku: ov.from || "", title: v.title,
        newCharmHash: v.entry.charmHash, newAiPath: aiPath, newMasterHash: masterHash || "", attachedFrom: copyOf ? copyOf.sku : "",
        removed: overriding ? Object.assign(fromOf(), { entry: overriding, thumbPath: str(overriding.thumbPath, 400) }) : null,
        alias: aliasWas || fixOwn ? { listingId: v.listingId, key: mode === "bySku" ? v.oldSku : "", was: aliasWas || rawPrior, now: sku } : null,
        replacedSave: replaced ? { id: rec.ref.id, sku: str(replaced.sku, 80), charmHash: str(replaced.charmHash, 80), at: num(replaced.at) || null, by: str(replaced.by, 80) } : null,
        superseded: supersede.map(r => r.id), undo: "drop the old design in the designs window of an order of this listing and switch it on: the same act moves the SKU back" });
    }
    const aliasMode = mode === "bySku" && aliasOwn ? "none" : mode;
    const save = { rid: v.rid, listingId: v.listingId, sku, oldSku: v.oldSku, productId: v.productId, optionPairs: v.optionPairs, group: v.group, title: v.title, design: v.design, charmHash: v.entry.charmHash,
      masterHash, aiPath, thumbPath, by: v.by, at: now, reused: !creating, phase: "repo", etsy: stale && up(stale.sku) === sku && stale.etsy && stale.etsy.state === "linked" ? stale.etsy : { state: "pending" }, rehearsal: false, alias: aliasMode };
    if (stamp) save.override = Object.assign({}, stamp, { fromSku: ov.from || "", from: fromOf(), alias: aliasWas || fixOwn ? { was: aliasWas || rawPrior, now: sku } : null, replaced: replaced ? str(replaced.sku, 80) : "" });
    if (stale) await tx.set(rec.ref, save); else await tx.create(rec.ref, save);   // (a stale record is replaced; any other record was refused or answered above)
    if (intentSnap.exists && (intentSnap.data() || {}).state !== "done") await tx.set(intentRef, { state: "done", sku, updatedAt: FV.serverTimestamp() }, { merge: true });
    return { done: creating ? "saved" : "linked", how: stamp ? "override" : creating ? "created" : linked ? "listing-same" : "same-design", sku, id: rec.ref.id, save, entry, created: creating };
  };

  let made = null;
  for (let round = 0; round < 5; round++) {
    let out;
    try { out = await db.runTransaction(tx => decide(tx, made)); }
    catch (e) { if (!alreadyExists(e)) throw e; continue; }   // a concurrent writer created a document first: decide again from what is there
    if (out.refusal) {
      // nothing was made for this call: the files it uploaded under its candidate SKU are litter now (unless that SKU has an entry, or the call is a reply the page may still need to repeat)
      if (["listing-linked", "already-saved"].includes(out.refusal.error)) await dropCandidate(deps, v);
      return out.refusal;
    }
    if (out.need === "files") {
      const no = await checkFiles(deps.bucket, v);
      if (no) return no;
      const { aiPath, thumbPath } = pathsOf(v.sku), [aiUrl, thumbUrl] = await Promise.all([deps.tokenUrl(aiPath), deps.tokenUrl(thumbPath)]);
      made = { aiUrl: aiUrl || "", thumbUrl: thumbUrl || "" };
      continue;
    }
    if (out.how === "again" && out.entry && out.entry.hashSource === "custom-save" && out.entry.aiPath && deps.bucket) {
      // the record and its entry are there; the files of a design this save made must be too (a Storage delete, an orphan sweep that was too eager): the page uploads them again, once, and commits again
      try {
        const [aiThere] = await deps.bucket.file(out.entry.aiPath).exists(); if (!aiThere) return refuse("not-uploaded", { which: "ai", why: "missing" });
        if (out.entry.thumbPath) { const [pngThere] = await deps.bucket.file(out.entry.thumbPath).exists(); if (!pngThere) return refuse("not-uploaded", { which: "thumb", why: "missing" }); }
      } catch (e) { warn("files of a saved design not checked", e); }
    }
    const answer = { ok: true, rid: v.rid, id: out.id, listingId: v.listingId, sku: out.sku, created: out.created, how: out.how, entry: await linksOf(out.entry ? Master.slimEntry(out.entry) : null), alias: out.save.alias || (out.save.oldSku ? "bySku" : "whole") };
    if (out.save && out.save.override) answer.override = out.save.override;   // (KEEPOVERRIDE: what the toggle replaced, for the page's progress lines; a repeated call answers it again from the record)
    if (out.sku !== v.sku || v.fileKey !== v.sku) await dropCandidate(deps, v);   // (the design lives under another SKU: the files uploaded under this call's candidate are litter)
    if (out.created || out.how !== "again") await sweep(deps);   // (best effort, never changes the answer)
    return answer;
  }
  return { error: "unavailable", status: 503 };
}

/* ── the read: what a reload asks, one or two documents ── */
/** KR 2: an Etsy step noted "writing" whose lease has run out is not running (the function that held it died): it is "unverified" (the retry reads Etsy first, so it is safe), reason "interrupted". */
const etsyNow = (e, now) => (e && e.state === "writing" && !(+e.runningUntil > now) ? Object.assign({}, e, { state: "unverified", reason: "interrupted", retry: true }) : e);
const saveView = (id, d, now) => {
  const e = etsyNow(d.etsy && typeof d.etsy === "object" ? d.etsy : {}, now == null ? Date.now() : now);
  return { id, rid: str(d.rid, 30), listingId: str(d.listingId, 30), sku: str(d.sku, 80), oldSku: str(d.oldSku, 120), phase: str(d.phase, 20), reused: d.reused === true, alias: str(d.alias, 10), charmHash: str(d.charmHash, 80),   // (KEEPOVERRIDE: the drawing it holds: another drawing dropped later is a correction)
    ...(d.superseded ? { superseded: true } : {}),
    etsy: Object.assign({ state: str(e.state, 20) || "pending" }, e.reason ? { reason: str(e.reason, 40) } : {}, e.text ? { text: str(e.text, 200) } : {}, e.retry === false ? { retry: false } : {}), at: num(d.at) || null, by: str(d.by, 80) };
};
const intentView = d => ({ rid: str(d.rid, 30), listingId: str(d.listingId, 30), hash: str(d.hash, 80), name: str(d.name, 200), by: str(d.by, 80), state: str(d.state, 12), at: num(d.at) || null, updatedAt: num(d.updatedAtMs) || null });
async function get(deps, b) {
  b = b || {};
  const rid = str(b.rid, 30).trim();
  if (!RID_RE.test(rid)) return bad("rid", "the order number");
  if (isSandbox(b)) return { ok: true, save: null, sandbox: true };   // (a rehearsal saves nothing, so it is never "already saved"; a real order of the same number is not its business)
  const listingId = str(b.listingId, 30).trim(), rec = await recordFor(deps.db, rid, listingId);
  if (rec.snap.exists) return { ok: true, save: saveView(rec.ref.id, rec.snap.data() || {}) };
  let intent = null; try { const s = await deps.db.collection(INTENTS).doc(rid).get(); if (s.exists) intent = intentView(s.data() || {}); } catch (e) { warn("intent not read", e); }
  return intent ? { ok: true, save: null, intent } : { ok: true, save: null };
}

/* ── the saves that wait for their Etsy step (KEEP B6): the server remembers, any page that loads can resume ── */
const TERMINAL = new Set(["listing-gone", "product-missing", "changed", "sku-in-use", "verify", "rejected", "bad-save", "no-save", "not-saved", "rehearsal", "sandbox", "bad-request"]);
async function pending(deps, b) {
  b = b || {};
  if (isSandbox(b)) return { ok: true, pending: [], sandbox: true };
  const now = (typeof deps.now === "function" ? deps.now : Date.now)(), rules = (() => { try { return skuRules(deps); } catch (_) { return null; } })();
  const snap = await deps.db.collection(COLL).where("etsy.state", "in", ["pending", "failed", "unverified", "writing"]).limit(200).get();
  const rows = [];
  for (const s of snap.docs) {
    const d = s.data() || {}, e = etsyNow(d.etsy && typeof d.etsy === "object" ? d.etsy : {}, now);
    if (d.phase !== "repo" || d.rehearsal === true || e.state === "linked" || d.superseded) continue;   // (KEEPOVERRIDE: a record whose SKU was moved to another design by the toggle is not finished on Etsy)
    if (e.state === "failed" && (e.retry === false || TERMINAL.has(e.reason))) continue;   // a person's matter (or nothing to retry): not listed
    if (+e.runningUntil > now) continue;                                                      // somebody is running it
    const cool = e.reason === "scope" ? SCOPE_COOLDOWN_MS : COOLDOWN_MS;
    if (+e.at && now - +e.at < cool) continue;                                                // tried a moment ago
    let needs = false; try { needs = !rules || d.alias === "none" || !rules.aliasWorks(String(d.oldSku || ""), String(d.sku || "")); } catch (_) { needs = true; }
    rows.push({ id: s.id, rid: str(d.rid, 30), listingId: str(d.listingId, 30), sku: str(d.sku, 80), etsy: Object.assign({ state: str(e.state, 20) || "pending" }, e.reason ? { reason: str(e.reason, 40) } : {}), at: num(d.at) || 0, linkNeedsEtsy: needs });
  }
  rows.sort((a, c) => (c.linkNeedsEtsy - a.linkNeedsEtsy) || (a.at - c.at));   // (the saves only Etsy can link first, then the oldest)
  const cap = Math.min(100, Math.max(1, Math.floor(num(b.limit)) || PENDING_MAX));
  return { ok: true, pending: rows.slice(0, cap) };
}

/* ── the server's copy of an intent (KEEP B7): who wants what saved. Writes only Charm_Custom_Intents; best effort for the page, never required ── */
const INTENT_STATES = ["wanted", "cancelled", "done"];
async function intent(deps, b) {
  b = b || {};
  const rid = str(b.rid, 30).trim(), listingId = str(b.listingId, 30).trim(), state = str(b.state, 12).trim();
  if (!RID_RE.test(rid)) return bad("rid", "the order number");
  if (!LISTING_RE.test(listingId)) return bad("listingId", "the listing number");
  if (!INTENT_STATES.includes(state)) return bad("state", "wanted, cancelled or done");
  if (isSandbox(b)) return { ok: true, sandbox: true, intent: null };   // (a rehearsal leaves no trace)
  const { db, FV } = deps, now = (typeof deps.now === "function" ? deps.now : Date.now)(), ref = db.collection(INTENTS).doc(rid), hash = str(b.hash, 80).trim().toLowerCase();
  const doc = { rid, listingId, hash, name: str(b.name, 200), by: str(b.by, 80) || "operator", state, at: now, updatedAt: FV.serverTimestamp(), updatedAtMs: now };
  const out = await db.runTransaction(async tx => {
    const [cur, rec] = await tx.getAll(ref, db.collection(COLL).doc(rid));
    const c = cur.exists ? cur.data() || {} : null;
    if (rec.exists && state !== "done") return { intent: Object.assign({}, intentView(Object.assign({}, c || doc, { state: "done" }))) };   // (a saved order stays saved: a late "wanted" or "cancelled" changes nothing)
    if (c && c.state === "done" && state !== "done") return { intent: intentView(c) };
    const next = Object.assign({}, c && c.at ? { at: c.at } : {}, doc);
    if (c && c.at) next.at = c.at;
    await tx.set(ref, next, { merge: true });
    return { intent: intentView(next) };
  });
  return { ok: true, intent: out.intent };
}

module.exports = { config, COLL, INTENTS, OVERRIDES, DIR, SKU_RE, pathsOf, stemOfFile, commit, get, pending, intent, sweep, read, recordFor, pick, optionPairsOf, groupOf, usableKey, oldSkuOf, ORPHAN_MS };
