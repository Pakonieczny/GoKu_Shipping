/*  netlify/functions/_charmNestCustomSave.js
 *  Save a custom design to the master charm repository (Paul, 11 Oct 2026: the Designs window should "save the uploaded vector file
 *  to the master charm repository", give it a SKU, and link that SKU to the Etsy listing so the next purchase of it is already
 *  linked). Contract: plans/save-to-repo-1011/CONTRACT.md sections 2, 4 and 7.
 *
 *  customSaveCommit  (repo upload token; the HTTP gate is _charmNestRepoAuth.js LIBRARY.customSaveCommit)
 *      The browser has already put two files in Storage with the upload token: charmnest/master/custom/{SKU}.ai and {SKU}.png (a path
 *      nothing else uses, so no existing file can be overwritten). This op checks they are there, then writes ALL the records in ONE
 *      transaction, create-only:
 *        Charm_Master_Index/{SKU}        created (ref.create: an existing SKU is never overwritten, never marked blocked: not masterPutIndex)
 *        Charm_Master_Files/{masterHash} created, or only its `skus` list grown when it is a record this op made (a real master's record is never touched)
 *        Charm_Sku_Aliases/{listingId}   merged: bySku[oldSku] = SKU (or sku + v:2 when the order had no SKU), and keep {rid, sku, oldSku, was, at, by}
 *        Charm_Custom_Saves/{rid}        created: the truth after a reload (one save per order); the Etsy step reads ONLY this
 *      Idempotent: the same rid with the same SKU answers the saved record; another SKU for a saved rid is refused (already-saved).
 *      reuse:true (an identical design already in the repository, exact charmHash): writes only the alias and the save record.
 *      Refuses sandbox:true on the way in, before any read. Expected refusals are HTTP 200 with ok:false (status:200), so the page's
 *      api() does not throw and the engine reads the answer.
 *  customSaveGet     (read only) { rid } -> { ok, save: null | { rid, sku, phase, reused, etsy, at, by, ... } }, one read.
 *
 *  Nothing here calls Etsy. The SKU rule (what a SKU may be, what is reserved) is the page's own module, charm-nest-sku.js, so the
 *  server and the page cannot drift. Dependencies are passed in ({ db, FV, tokenUrl, bucket }) so a test runs on fakes.            */
"use strict";
const { str, num } = require("./_charmNestAuth");
const Master = require("./_charmNestMaster");

const COLL = "Charm_Custom_Saves", ALIASES = "Charm_Sku_Aliases", INDEX = Master.INDEX, FILES = Master.FILES;
const DIR = "charmnest/master/custom/";
const SKU_RE = /^[A-Z0-9][A-Z0-9_-]{1,31}$/;                 // what a custom SKU may be: letters, digits, _ and - only, at most 32 (Etsy's limit)
const RID_RE = /^\d{5,20}$/, LISTING_RE = /^\d{3,20}$/, ID_RE = /^\d{1,20}$/, HASH_RE = /^[0-9a-f]{8,64}$/i;
const MIN_AI_BYTES = 200;
const pathsOf = sku => ({ aiPath: DIR + sku + ".ai", thumbPath: DIR + sku + ".png" });

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
/** Reads and checks a commit body: { ok:true, v } with clean values, or { ok:false, answer } (HTTP 400 for a malformed call). */
function read(b, deps) {
  const rid = str(b.rid, 30).trim(), listingId = str(b.listingId, 30).trim(), sku = str(b.sku, 80).trim().toUpperCase();
  if (!RID_RE.test(rid)) return { answer: bad("rid", "the order number") };
  if (!LISTING_RE.test(listingId)) return { answer: bad("listingId", "the listing number") };
  if (!SKU_RE.test(sku) || !Master.isSku(sku)) return { answer: bad("sku", "letters, digits, _ and - only, at most 32") };
  let rules; try { rules = skuRules(deps); } catch (e) { warn("sku rules not loaded", e); return { answer: { error: "unavailable", status: 503 } }; }
  if (rules.reserved(sku)) return { answer: refuse("reserved", { sku }) };
  const oldSku = str(b.oldSku, 120).trim().toUpperCase();
  if (/^__.*__$/.test(oldSku) || oldSku.indexOf("/") >= 0) return { answer: bad("oldSku", "not a usable SKU") };
  const productId = ID_RE.test(String(b.productId == null ? "" : b.productId)) ? String(b.productId) : "";
  const d = b.design && typeof b.design === "object" ? b.design : {}, f = b.file && typeof b.file === "object" ? b.file : {}, e = b.entry && typeof b.entry === "object" ? b.entry : {};
  const name = str(d.name || f.name, 200).trim() || sku + ".ai";
  const masterHash = str(f.masterHash || d.hash, 80).trim().toLowerCase();
  const reuse = b.reuse === true;
  const v = { rid, listingId, sku, oldSku, productId, optionPairs: optionPairsOf(b.optionPairs), group: groupOf(b.group), title: str(b.title, 200), by: str(b.by, 80) || "operator", reuse,
    design: { name, kind: d.kind === "dxf" ? "dxf" : "ai", hash: str(d.hash || masterHash, 80).toLowerCase(), bytes: Math.max(0, Math.floor(num(d.bytes))) } };
  const charmHash = str(e.charmHash, 80).trim();
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

/* ── Storage: both files are there, the .ai is a real file ── */
async function checkFiles(bucket, aiPath, thumbPath) {
  const ai = bucket.file(aiPath), png = bucket.file(thumbPath);
  const [aiThere] = await ai.exists(); if (!aiThere) return { which: "ai", why: "missing" };
  const [pngThere] = await png.exists(); if (!pngThere) return { which: "thumb", why: "missing" };
  const [meta] = await ai.getMetadata();
  if (!(Number(meta && meta.size) > MIN_AI_BYTES)) return { which: "ai", why: "too-small", bad: true };
  const [head] = await ai.download({ start: 0, end: 7 });
  if (!head || Buffer.from(head).slice(0, 4).toString("latin1") !== "%PDF") return { which: "ai", why: "not-pdf", bad: true };
  return null;
}

/* ── the commit ── */
async function commit(deps, b) {
  b = b || {};
  if (isSandbox(b)) return refuse("sandbox");   // the sandbox is a rehearsal: it never writes the repository, an alias or Etsy; refused before anything is read
  const { db, FV } = deps, got = read(b, deps);
  if (!got.ok) return got.answer;
  const v = got.v, saveRef = db.collection(COLL).doc(v.rid), indexRef = db.collection(INDEX).doc(v.sku), aliasRef = db.collection(ALIASES).doc(v.listingId);
  const fileRef = v.reuse ? null : db.collection(FILES).doc(v.masterHash);
  const linksOf = async e => { if (!e) return e; const [a, t] = await Promise.all([e.aiPath ? deps.tokenUrl(e.aiPath) : null, e.thumbPath ? deps.tokenUrl(e.thumbPath) : null]); if (a) e.aiUrl = a; if (t) e.thumbUrl = t; return e; };

  // 1 · already saved? (the same SKU answers the saved record again; another SKU for this order is refused)
  const [was, held] = await db.getAll(saveRef, indexRef);
  if (was.exists) {
    const d = was.data() || {};
    if (String(d.sku || "").toUpperCase() !== v.sku) return refuse("already-saved", { sku: d.sku || "" });
    return { ok: true, rid: v.rid, sku: v.sku, created: false, entry: await linksOf(held.exists ? Master.slimEntry(held.data()) : null), alias: d.alias || (d.oldSku ? "bySku" : "whole") };
  }

  // 2 · the files (a new design only; a reused one adds none)
  let aiPath = "", thumbPath = "", aiUrl = "", thumbUrl = "";
  if (!v.reuse) {
    if (held.exists) return refuse("sku-taken", { sku: v.sku, by: "master" });   // (nothing is checked or written for a SKU that is already there)
    ({ aiPath, thumbPath } = pathsOf(v.sku));
    const no = await checkFiles(deps.bucket, aiPath, thumbPath);
    if (no) return refuse(no.bad ? "bad-file" : "not-uploaded", { which: no.which, why: no.why });
    [aiUrl, thumbUrl] = await Promise.all([deps.tokenUrl(aiPath), deps.tokenUrl(thumbPath)]);
  }

  // 3 · every record in one transaction, create-only
  const now = Date.now(); let out;
  try {
    out = await db.runTransaction(async tx => {
      const [saveSnap, indexSnap, aliasSnap, fileSnap] = await tx.getAll(...[saveRef, indexRef, aliasRef].concat(fileRef ? [fileRef] : []));
      if (saveSnap.exists) { const d = saveSnap.data() || {}; return String(d.sku || "").toUpperCase() === v.sku ? { done: "again", save: d, index: indexSnap.exists ? indexSnap.data() : null } : refuse("already-saved", { sku: d.sku || "" }); }
      const ex = indexSnap.exists ? indexSnap.data() || {} : null, old = aliasSnap.exists ? aliasSnap.data() || {} : {};
      let index, masterHash = v.masterHash, entryDoc;
      if (v.reuse) {
        // an identical design the repository already holds: its SKU must be there, usable, and the very same drawing
        if (!ex || !ex.sku || !ex.aiPath) return refuse("reuse-missing", { sku: v.sku });
        if (ex.blocked) return refuse("reuse-blocked", { sku: v.sku });
        if (str(ex.charmHash, 80) !== v.entry.charmHash) return refuse("reuse-mismatch", { sku: v.sku });
        masterHash = ex.masterHash || ""; aiPath = ex.aiPath; thumbPath = ex.thumbPath || ""; entryDoc = ex;
      } else {
        if (ex) return refuse("sku-taken", { sku: v.sku, by: "master" });   // (create() would fail the same way; this says so before anything is written)
        const e = v.entry;
        index = { sku: v.sku, masterHash, masterPath: aiPath, masterName: v.design.name, charmHash: e.charmHash, widthPt: e.widthPt, heightPt: e.heightPt, areaPt2: e.areaPt2, members: e.members, holes: e.holes,
          engravable: e.engravable, engravableBy: "index", upAngle: e.upAngle, upSource: e.upSource, aiPath, thumbPath, aiUrl: str(aiUrl, 900), thumbUrl: str(thumbUrl, 900),
          open: false, labelSource: "text", confidence: null, indexedAt: FV.serverTimestamp(), indexedAtMs: now, firstIndexedAt: FV.serverTimestamp(), hashSource: "custom-save" };
        entryDoc = index;
      }
      // the listing's alias: the old SKU answers with the new one (or, with no old SKU, the listing does); what stood there is kept in `keep.was`
      const prior = v.oldSku ? str(old.bySku && old.bySku[v.oldSku], 80) : (old.sku ? str(old.sku, 80) : "");
      const alias = { listingId: v.listingId, by: v.by, title: v.title, updatedAt: FV.serverTimestamp(), keep: { rid: v.rid, sku: v.sku, oldSku: v.oldSku, was: prior === v.sku ? "" : prior, at: now, by: v.by } };
      if (v.oldSku) alias.bySku = { [v.oldSku]: v.sku }; else { alias.sku = v.sku; alias.v = 2; }
      const save = { rid: v.rid, listingId: v.listingId, sku: v.sku, oldSku: v.oldSku, productId: v.productId, optionPairs: v.optionPairs, group: v.group, title: v.title, design: v.design,
        masterHash, aiPath, thumbPath, by: v.by, at: now, reused: v.reuse, phase: "repo", etsy: { state: "pending" }, rehearsal: false, alias: v.oldSku ? "bySku" : "whole" };
      if (!v.reuse) {
        await tx.create(indexRef, index);
        if (!fileSnap.exists) await tx.set(fileRef, { masterHash, path: aiPath, url: str(aiUrl, 900), name: v.design.name, charms: 1, labelled: 1, unlabelled: [], orphans: [], duplicates: [], undecodable: 0, blocked: [],
          unlabelledCount: 0, orphanCount: 0, duplicateCount: 0, blockedCount: 0, skus: [v.sku], indexedAt: FV.serverTimestamp(), indexedAtMs: now, indexedBy: "custom-save" });
        else if ((fileSnap.data() || {}).indexedBy === "custom-save") { const skus = Array.isArray((fileSnap.data() || {}).skus) ? fileSnap.data().skus : []; if (!skus.includes(v.sku)) await tx.set(fileRef, { skus: skus.concat([v.sku]).slice(0, 2000) }, { merge: true }); }
        // (a record of a real master file that happens to hold the same bytes is left exactly as it is)
      }
      await tx.set(aliasRef, alias, { merge: true });
      await tx.create(saveRef, save);
      return { done: "saved", entry: entryDoc, alias: save.alias, created: !v.reuse };
    });
  } catch (e) {
    if (!alreadyExists(e)) throw e;
    // a second press at the same moment (or a SKU made since the read): say which
    const [again, there] = await db.getAll(saveRef, indexRef);
    if (again.exists) { const d = again.data() || {}; return String(d.sku || "").toUpperCase() === v.sku ? { ok: true, rid: v.rid, sku: v.sku, created: false, entry: await linksOf(there.exists ? Master.slimEntry(there.data()) : null), alias: d.alias || (d.oldSku ? "bySku" : "whole") } : refuse("already-saved", { sku: d.sku || "" }); }
    return refuse("sku-taken", { sku: v.sku, by: "master" });
  }
  if (out && out.ok === false) return out;
  if (out && out.error) return out;
  if (out.done === "again") return { ok: true, rid: v.rid, sku: v.sku, created: false, entry: await linksOf(out.index ? Master.slimEntry(out.index) : null), alias: out.save.alias || (out.save.oldSku ? "bySku" : "whole") };
  return { ok: true, rid: v.rid, sku: v.sku, created: out.created, entry: await linksOf(Master.slimEntry(out.entry)), alias: out.alias };
}

/* ── the read: what a reload asks, one document ── */
async function get(deps, b) {
  b = b || {};
  const rid = str(b.rid, 30).trim();
  if (!RID_RE.test(rid)) return bad("rid", "the order number");
  if (isSandbox(b)) return { ok: true, save: null, sandbox: true };   // (a rehearsal saves nothing, so it is never "already saved"; a real order of the same number is not its business)
  const s = await deps.db.collection(COLL).doc(rid).get();
  if (!s.exists) return { ok: true, save: null };
  const d = s.data() || {}, e = d.etsy && typeof d.etsy === "object" ? d.etsy : {};
  return { ok: true, save: { rid, listingId: str(d.listingId, 30), sku: str(d.sku, 80), oldSku: str(d.oldSku, 120), phase: str(d.phase, 20), reused: d.reused === true, alias: str(d.alias, 10),
    etsy: Object.assign({ state: str(e.state, 20) || "pending" }, e.reason ? { reason: str(e.reason, 40) } : {}, e.text ? { text: str(e.text, 200) } : {}), at: num(d.at) || null, by: str(d.by, 80) } };
}

module.exports = { COLL, DIR, SKU_RE, pathsOf, commit, get, read, optionPairsOf, groupOf };
