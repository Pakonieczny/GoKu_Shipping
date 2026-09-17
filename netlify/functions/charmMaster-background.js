/*  netlify/functions/charmMaster-background.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  Index a large master Illustrator file server-side (design §6.3): parse,
 *  group, read the SKU label under every charm, write one .ai per SKU (the
 *  original bytes, every non-member segment blanked — nothing redrawn) plus
 *  a thumbnail, and record Charm_Master_Index/{SKU} and
 *  Charm_Master_Files/{masterHash}. Progress and the result go to
 *  Charm_Nest_Jobs/{id}, which the Master tab polls with op=getJob.
 *
 *  Same parser as the browser (charm-nest-pdf.js), same index writer as the
 *  browser path (_charmNestMaster.js). Silhouettes come from the pure
 *  rasteriser in charm-nest-geom.js, so their hash carries hashSource
 *  "server"; the sorter re-derives its own silhouette when it fetches the
 *  per-SKU file, so the two never have to agree bit for bit.
 *
 *  Kicked by charmNestLibrary op=startMaster { path, name, opts } after the
 *  page uploaded the master with a signed URL (any size).
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const crypto = require("crypto");
const admin = require("./firebaseAdmin");
const Master = require("./_charmNestMaster");
const { parseBody, safePath } = require("./_charmNestAuth");
const db = admin.firestore();
const FV = admin.firestore.FieldValue;
const JOBS = "Charm_Nest_Jobs";
const MM = 25.4 / 72;

function tokenUrl(bucket, path, token) { return "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucket.name) + "/o/" + encodeURIComponent(path) + "?alt=media&token=" + encodeURIComponent(token); }
async function save(bucket, path, buf, contentType) {
  const file = bucket.file(path); let token = null;
  try { const [meta] = await file.getMetadata(); token = meta && meta.metadata && meta.metadata.firebaseStorageDownloadTokens; if (token) token = String(token).split(",")[0]; } catch (_) { token = null; }
  token = token || (crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex"));
  await file.save(buf, { resumable: false, contentType, metadata: { cacheControl: "public, max-age=31536000", metadata: { firebaseStorageDownloadTokens: token, uploadedBy: "charmMaster-background" } } });
  return { path, url: tokenUrl(bucket, path, token) };
}
/** A charm's members as an SVG (y flipped), rendered to PNG by resvg when it is available. */
function thumbnailPng(Geom, charm, size) {
  let Resvg = null; try { ({ Resvg } = require("@resvg/resvg-js")); } catch (_) { return null; }
  const b = charm.bbox, pad = 2, w = b[2] - b[0] + 2 * pad, h = b[3] - b[1] + 2 * pad, s = size / Math.max(w, h);
  const css = c => `rgb(${Math.round((c[0] || 0) * 255)},${Math.round((c[1] || 0) * 255)},${Math.round((c[2] || 0) * 255)})`;
  const parts = [];
  for (const m of charm.members) {
    if (m.kind !== "path") continue;
    const d = Geom.svgPathOf(m); if (!d) continue;
    const st = m.stroke ? (Math.min(m.strokeRGB[0], m.strokeRGB[1], m.strokeRGB[2]) >= 0.92 ? "#2a2724" : css(m.strokeRGB)) : "none";
    parts.push(`<path d="${d}" fill="${m.fill ? css(m.fillRGB) : "none"}" fill-rule="${m.paintOp && m.paintOp.endsWith("*") ? "evenodd" : "nonzero"}" stroke="${st}" stroke-width="${Math.max(0.6 / s, m.lwPt || 0.5)}"/>`);
  }
  parts.push(`<path d="${Geom.svgPathOf(charm.outline)}" fill="none" stroke="rgba(190,40,40,.9)" stroke-width="${Math.max(1 / s, 0.6)}"/>`);
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${Math.max(8, Math.round(w * s))}" height="${Math.max(8, Math.round(h * s))}" viewBox="0 0 ${w} ${h}"><rect width="100%" height="100%" fill="#ece7dc"/><g transform="translate(${pad - b[0]} ${b[3] + pad}) scale(1 -1)">${parts.join("")}</g></svg>`;
  try { return new Resvg(svg, { fitTo: { mode: "width", value: Math.max(8, Math.round(w * s)) } }).render().asPng(); } catch (e) { console.warn("[charmMaster] thumbnail", e.message); return null; }
}

exports.handler = async (event) => {
  const body = parseBody(event);
  const id = String(body.id || "").replace(/[^\w\-]/g, "").slice(0, 80);
  if (!id) return { statusCode: 400, body: "bad payload" };
  const ref = db.collection(JOBS).doc(id);
  const snap = await ref.get();
  if (!snap.exists) return { statusCode: 404, body: "unknown job" };
  const job = snap.data();
  if (job.status !== "pending") return { statusCode: 200, body: "already handled" };
  await ref.set({ status: "running", stage: "downloading", startedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
  const progress = (stage, done, total) => ref.set({ stage, done: done || 0, total: total || 0, updatedAt: FV.serverTimestamp() }, { merge: true }).catch(() => {});
  try {
    const { CharmNestPDF, Geom } = require("./_charmNestPdf");
    const bucket = admin.storage().bucket();
    const [buf] = await bucket.file(safePath(job.path)).download();
    const bytes = new Uint8Array(buf);
    const masterHash = crypto.createHash("sha256").update(buf).digest("hex");
    const opts = job.opts || {};
    const pattern = opts.skuPattern ? new RegExp(opts.skuPattern) : CharmNestPDF.SKU_PATTERN_DEFAULT;
    const gapPt = (opts.labelGapMm || 6.4) / MM;
    await progress("parsing", 0, 0);
    const parsed = await CharmNestPDF.parseSource(bytes, job.name || "master.ai");
    const g = CharmNestPDF.groupCharms(parsed, { minPt: +opts.minPt || 6 });
    const lab = CharmNestPDF.labelCharms(parsed, g.charms, { pattern, gapPt, widen: 0.25 });
    const entries = [], blocked = [], skus = [];
    const total = lab.labels.size; let done = 0;
    for (const [index, l] of lab.labels) {
      const c = g.charms.find(x => x.index === index); if (!c) continue;
      const sil = Geom.silhouetteBits(c, 6, {});
      const charmHash = CharmNestPDF.fnv(CharmNestPDF.signature(sil.bits, sil.w, sil.h) + "|" + Math.round((sil.bboxOuter[2] - sil.bboxOuter[0]) * 2) + "x" + Math.round((sil.bboxOuter[3] - sil.bboxOuter[1]) * 2) + "|" + c.members.length);
      const open = (() => { const polys = Geom.flatten(c.outline, 12); return !polys.length || polys.some(p => Math.hypot(p[0][0] - p[p.length - 1][0], p[0][1] - p[p.length - 1][1]) > 1.5 && !c.outline.closed); })();
      let engravable = true, upAngle = null, upSource = "drawn", flipOk = true, flipWhy = null;
      try { const up = Geom.upAngleOf(c); upAngle = up.angle; upSource = up.source; const view = Geom.backView(c, { res: 6, upAngle }); const mask = Geom.engraveMask(view, { marginMm: opts.engraveMarginMm || 0.8 }); const r = Geom.largestRectangles(mask, 1)[0]; engravable = !!r && ((r.wPt * MM >= 6 && r.hPt * MM >= 3) || (r.wPt * MM >= 3 && r.hPt * MM >= 6)); }
      catch (e) { flipOk = false; flipWhy = e.message; engravable = false; }
      const key = l.size ? `${l.sku}__${l.size}` : l.sku;
      const ai = await CharmNestPDF.buildSingleCharm(c, parsed);
      const aiUp = await save(bucket, `charmnest/master/${key}.ai`, Buffer.from(ai), "application/illustrator");
      let thumb = null; const png = thumbnailPng(Geom, c, 168); if (png) thumb = await save(bucket, `charmnest/master/${key}.png`, Buffer.from(png), "image/png");
      const reasons = [];
      if (open) reasons.push("open outline");
      if (!flipOk) reasons.push(flipWhy);
      entries.push({ sku: l.sku, size: l.size, charmHash, widthPt: sil.bboxOuter[2] - sil.bboxOuter[0], heightPt: sil.bboxOuter[3] - sil.bboxOuter[1], areaPt2: sil.areaPt2, members: c.members.length, holes: CharmNestPDF.cutLinesOf(c).length, engravable, upAngle, upSource, aiPath: aiUp.path, aiUrl: aiUp.url, thumbPath: thumb && thumb.path, thumbUrl: thumb && thumb.url, open, labelSource: "text", blocked: reasons.length ? reasons.join("; ") : null });
      if (reasons.length) blocked.push({ sku: l.sku, reason: reasons.join("; ") });
      skus.push(l.sku);
      for (const x of l.extra || []) { entries.push(Object.assign({}, entries[entries.length - 1], { sku: x.sku, size: x.size })); skus.push(x.sku); if (reasons.length) blocked.push({ sku: x.sku, reason: reasons.join("; ") }); }   // every further line under the charm: the same design under another SKU
      done++; if (done % 5 === 0) await progress("indexing", done, total);
    }
    // a small ring left loose beside a charm (not merged by grouping) blocks that charm: it would be cut without its ring
    for (const o of g.orphans || []) {
      const b = o.bbox; if (!b) continue; const maxDim = Math.max(b[2] - b[0], b[3] - b[1]); if (maxDim > 13 || o.kind !== "path" || !o.closed) continue;
      const cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2;
      for (const c of g.charms) { if (!c.sku) continue; const ob = c.outline.bbox; if (cx < ob[0] - 4 / MM || cx > ob[2] + 4 / MM || cy < ob[1] - 4 / MM || cy > ob[3] + 4 / MM) continue; const d = Geom.distToPolys(cx, cy, Geom.flatten(c.outline, 8)); if (d <= 3 / MM) { const e = entries.find(x => x.sku === c.sku); if (e && !/detached ring/.test(e.blocked || "")) { e.blocked = (e.blocked ? e.blocked + "; " : "") + "detached ring not merged"; blocked.push({ sku: c.sku, reason: "detached ring not merged" }); } } }
    }
    const idx = await Master.putIndex(db, FV, { entries, masterHash, masterPath: job.path, masterName: job.name, hashSource: "server", replaces: opts.replaces || [] });
    await Master.putFile(db, FV, { file: { masterHash, path: job.path, name: job.name, charms: g.charms.length, labelled: lab.labels.size, unlabelled: lab.unlabelled, orphans: lab.orphans, duplicates: lab.duplicates, undecodable: lab.undecodable.length, blocked: blocked.concat(idx.blocked.map(b => ({ sku: b.sku, reason: b.reason }))), skus, indexedBy: "server", pageW: parsed.pageW, pageH: parsed.pageH, replaces: opts.replaces || [] } });
    const result = { masterHash, charms: g.charms.length, labelled: lab.labels.size, unlabelled: lab.unlabelled, orphans: lab.orphans, duplicates: lab.duplicates, undecodable: lab.undecodable.length, blocked, conflicts: idx.blocked, sizeMoved: idx.sizeMoved, skus };
    await ref.set({ status: "done", stage: "done", done, total, result, finishedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
    console.log(`[charmMaster] ${id}: ${lab.labels.size} labelled of ${g.charms.length}, ${lab.unlabelled.length} unlabelled, ${lab.orphans.length} orphan labels, ${blocked.length} blocked`);
  } catch (e) {
    console.error("[charmMaster]", id, e);
    await ref.set({ status: "error", error: String(e && e.message || e), updatedAt: FV.serverTimestamp() }, { merge: true });
  }
  return { statusCode: 200, body: "ok" };
};
