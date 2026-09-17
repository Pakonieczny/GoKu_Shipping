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
const isSku = s => /^[A-Z0-9][A-Z0-9 _.,'&()+\-]{1,60}$/.test(String(s || ""));   // free text, as the shop's SKUs are ("T-REX_84495", "HUGGIE HOOPS- UMBRELLA")

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
  // One read for every SKU, then one batched write: a document at a time was 300 sequential round trips for a batch of
  // 150 and ran into the function's time limit on a real master, losing records with no error to show for it.
  const skus = [...bySku.keys()], existing = new Map();
  for (let i = 0; i < skus.length; i += 200) {
    const part = skus.slice(i, i + 200);
    const snaps = await db.getAll(...part.map(s => db.collection(INDEX).doc(s)));
    snaps.forEach((sn, j) => { if (sn.exists) existing.set(part[j], sn.data()); });
  }
  let batch = db.batch(), pending = 0;
  const flush = async () => { if (pending) { await batch.commit(); batch = db.batch(); pending = 0; } };
  for (const [sku, list] of bySku) {
    const ref = db.collection(INDEX).doc(sku); const ex = existing.get(sku) || null;
    // a stable base whatever order the entries arrived in: the unsized entry, else the smallest size
    const LADDER = ["XS", "S", "M", "L", "XL"];
    const base = list.find(e => !e.size) || list.slice().sort((a, b) => {
      const ia = LADDER.indexOf(String(a.size).toUpperCase()), ib = LADDER.indexOf(String(b.size).toUpperCase());
      return (ia < 0 ? 9 : ia) - (ib < 0 ? 9 : ib) || String(a.size).localeCompare(String(b.size));
    })[0];
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
    // size moved more than 5 % on re-index — only against a different master file; re-writing the same one is not a change
    if (ex && ex.masterHash && ex.masterHash !== masterHash && num(ex.widthPt) && num(doc.widthPt)) {
      const dw = Math.abs(doc.widthPt - ex.widthPt) / ex.widthPt, dh = Math.abs(doc.heightPt - ex.heightPt) / (ex.heightPt || 1);
      if (dw > 0.05 || dh > 0.05) { doc.sizeMoved = { from: [ex.widthPt, ex.heightPt], to: [doc.widthPt, doc.heightPt], at: Date.now() }; out.sizeMoved.push({ sku, from: [ex.widthPt, ex.heightPt], to: [doc.widthPt, doc.heightPt] }); }
      else doc.sizeMoved = FV.delete();
    } else doc.sizeMoved = FV.delete();
    doc.blocked = blocked || FV.delete();
    if (!ex) doc.firstIndexedAt = FV.serverTimestamp();
    batch.set(ref, doc, { merge: true }); out.written++;
    if (++pending >= 400) await flush();
  }
  await flush();
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
