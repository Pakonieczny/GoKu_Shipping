/*  netlify/functions/_charmNestMaster.js
 *  The master index write, shared by charmNestLibrary (browser-side indexing of a small master) and
 *  charmMaster-background (server-side indexing of a large one), so both paths apply the same rules:
 *    · Charm_Master_Index is keyed by SKU alone — a SKU is one design whatever colour it is ordered in;
 *    · a SKU present in two different master files is a duplicate: BOTH entries are blocked until fixed;
 *    · re-indexing the same master (same hash, or a file that `replaces` an older hash) simply updates;
 *    · a size that moved more than 5 % on re-index is flagged, never silently accepted;
 *    · operator overrides (engravable, upAngle, backKeepOut, confirmed vision labels) survive a re-index.        */
"use strict";
const { str, num } = require("./_charmNestAuth");

const INDEX = "Charm_Master_Index", FILES = "Charm_Master_Files";
const isSku = s => /^[A-Z0-9][A-Z0-9\-]{2,40}$/.test(String(s || ""));

function slimEntry(d) {
  if (!d) return null;
  return { sku: d.sku, masterHash: d.masterHash || null, masterPath: d.masterPath || null, masterName: d.masterName || null, charmHash: d.charmHash || null, widthPt: num(d.widthPt), heightPt: num(d.heightPt), areaPt2: num(d.areaPt2), members: num(d.members), holes: num(d.holes),
    engravable: d.engravable !== false, engravableBy: d.engravableBy || null, upAngle: d.upAngle == null ? null : num(d.upAngle), upSource: d.upSource || null, backKeepOut: d.backKeepOut || [], aiPath: d.aiPath || null, thumbPath: d.thumbPath || null, aiUrl: d.aiUrl || null, thumbUrl: d.thumbUrl || null,
    sizes: d.sizes || null, blocked: d.blocked || null, conflict: d.conflict || null, sizeMoved: d.sizeMoved || null, labelSource: d.labelSource || "text", confirmedBy: d.confirmedBy || null, open: !!d.open, indexedAt: d.indexedAt && d.indexedAt.toMillis ? d.indexedAt.toMillis() : (num(d.indexedAtMs) || null), hashSource: d.hashSource || "browser" };
}

/**
 * entries: [{ sku, size?, masterHash, masterPath, masterName, charmHash, widthPt, heightPt, areaPt2, members, holes, engravable, upAngle, upSource, aiPath, thumbPath, aiUrl, thumbUrl, open, labelSource, confidence, blocked? }]
 * opts: { replaces: [masterHash…] } — hashes of earlier versions of this same master file.
 */
async function putIndex(db, FV, body) {
  const entries = (body.entries || []).filter(e => e && isSku(String(e.sku || "").toUpperCase()));
  const masterHash = str(body.masterHash, 80), replaces = new Set((body.replaces || []).map(String).concat([masterHash]));
  const out = { written: 0, blocked: [], sizeMoved: [] };
  // group sized designs under one SKU
  const bySku = new Map();
  for (const e of entries) { const sku = String(e.sku).toUpperCase(); if (!bySku.has(sku)) bySku.set(sku, []); bySku.get(sku).push(e); }
  for (const [sku, list] of bySku) {
    const ref = db.collection(INDEX).doc(sku); const snap = await ref.get(); const ex = snap.exists ? snap.data() : null;
    const base = list.find(e => !e.size) || list[0];
    const doc = { sku, masterHash, masterPath: str(body.masterPath, 600), masterName: str(body.masterName, 200), indexedAt: FV.serverTimestamp(), indexedAtMs: Date.now(), hashSource: str(body.hashSource || "browser", 20) };
    const geom = e => ({ charmHash: str(e.charmHash, 80), widthPt: num(e.widthPt), heightPt: num(e.heightPt), areaPt2: num(e.areaPt2), members: num(e.members), holes: num(e.holes), aiPath: str(e.aiPath, 600), thumbPath: str(e.thumbPath, 600), aiUrl: str(e.aiUrl, 900), thumbUrl: str(e.thumbUrl, 900), open: !!e.open, labelSource: str(e.labelSource || "text", 20), confidence: e.confidence == null ? null : num(e.confidence) });
    Object.assign(doc, geom(base));
    const sized = list.filter(e => e.size);
    if (sized.length) { doc.sizes = {}; for (const e of sized) doc.sizes[String(e.size).toUpperCase()] = geom(e); if (!list.find(e => !e.size)) { doc.charmHash = null; doc.aiPath = ""; doc.thumbPath = ""; doc.aiUrl = ""; doc.thumbUrl = ""; } }
    else if (ex && ex.sizes) doc.sizes = FV.delete();
    // derived defaults, kept when an operator already overrode them
    if (!(ex && ex.engravableBy === "operator")) { doc.engravable = base.engravable !== false; doc.engravableBy = "index"; }
    if (!(ex && ex.upSource === "operator")) { doc.upAngle = base.upAngle == null ? null : num(base.upAngle); doc.upSource = str(base.upSource || "index", 20); }
    if (base.backKeepOut) doc.backKeepOut = base.backKeepOut;
    // duplicates across masters
    let blocked = base.blocked ? str(base.blocked, 300) : null;
    if (ex && ex.masterHash && !replaces.has(ex.masterHash) && ex.masterHash !== masterHash) {
      blocked = `SKU present in two master files: ${ex.masterName || ex.masterHash} and ${doc.masterName || masterHash}`;
      doc.conflict = { with: ex.masterHash, withName: ex.masterName || null, at: Date.now() };
      out.blocked.push({ sku, reason: blocked, with: ex.masterHash });
    } else doc.conflict = FV.delete();
    // size moved more than 5 % on re-index
    if (ex && num(ex.widthPt) && num(doc.widthPt)) {
      const dw = Math.abs(doc.widthPt - ex.widthPt) / ex.widthPt, dh = Math.abs(doc.heightPt - ex.heightPt) / (ex.heightPt || 1);
      if (dw > 0.05 || dh > 0.05) { doc.sizeMoved = { from: [ex.widthPt, ex.heightPt], to: [doc.widthPt, doc.heightPt], at: Date.now() }; out.sizeMoved.push({ sku, from: [ex.widthPt, ex.heightPt], to: [doc.widthPt, doc.heightPt] }); }
      else doc.sizeMoved = FV.delete();
    }
    doc.blocked = blocked || FV.delete();
    if (!ex) doc.firstIndexedAt = FV.serverTimestamp();
    await ref.set(doc, { merge: true }); out.written++;
  }
  return out;
}

async function putFile(db, FV, body) {
  const f = body.file || {}; const hash = str(f.masterHash, 80); if (!/^[0-9a-f]{8,64}$/i.test(hash)) return { error: "bad master hash" };
  const doc = { masterHash: hash, path: str(f.path, 600), url: str(f.url, 900), name: str(f.name, 200), charms: num(f.charms), labelled: num(f.labelled), unlabelled: (f.unlabelled || []).slice(0, 500), orphans: (f.orphans || []).slice(0, 500), duplicates: (f.duplicates || []).slice(0, 500), undecodable: num(f.undecodable), blocked: (f.blocked || []).slice(0, 500), skus: (f.skus || []).map(s => String(s).toUpperCase()).slice(0, 2000), indexedAt: FV.serverTimestamp(), indexedAtMs: Date.now(), indexedBy: str(f.indexedBy || "browser", 20), pageW: num(f.pageW), pageH: num(f.pageH) };
  if (Array.isArray(f.visionReads)) doc.visionReads = f.visionReads.slice(0, 1000);
  if (f.replaces) doc.replaces = (f.replaces || []).map(String).slice(0, 20);
  await db.collection(FILES).doc(hash).set(doc, { merge: true });
  return { ok: true, masterHash: hash };
}

module.exports = { INDEX, FILES, isSku, slimEntry, putIndex, putFile };
