#!/usr/bin/env node
/*  scripts/index-master.cjs — index a master Illustrator file from this PC.
 *  ═══════════════════════════════════════════════════════════════════════
 *  The same work the Master tab and the server route do (parse, find every
 *  charm, read the SKU under it, extract it with all its layers, flip-check
 *  it, write one .ai per SKU and the index), but on this machine: no 1 GB
 *  memory cap, no 15-minute limit, so a master of any size indexes.
 *
 *    node --max-old-space-size=8192 scripts/index-master.cjs "C:\path\MASTER.ai" --origin https://brites-charm-sorter.goldenspike.app
 *
 *  Options
 *    --origin URL        the sorter site whose functions receive the files and the index (required unless --dry)
 *    --passcode CODE     EDIT_PASSCODE, if the functions are locked
 *    --dry               parse and report only: nothing is uploaded or written
 *    --pattern REGEX     SKU pattern (default: the sorter's)        --gap-mm N   label gap below the charm (6.4)
 *    --min-pt N          smallest charm side in points (6)         --concurrency N  parallel uploads (4)
 *    --upload-master     also store the master file itself under charmnest/master/files/
 *    --replaces HASH     an earlier version of this master to supersede (repeatable)
 *    --resume            continue an interrupted run (progress is kept in <file>.index-progress.json)
 *  The master library (index and per-SKU files) is one library, read by production and the sandbox alike; it holds
 *  designs, not orders, so there is no sandbox copy of it.
 *
 *  Output: a report of labelled, unlabelled, orphan and duplicate labels, blocked SKUs, and a JSON next to the file.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const fs = require("fs"), path = require("path"), crypto = require("crypto");
const root = path.join(__dirname, "..");
const MM = 25.4 / 72;

function args(argv) {
  const o = { file: null, origin: "", passcode: "", dry: false, pattern: "", gapMm: 6.4, minPt: 6, concurrency: 4, uploadMaster: false, replaces: [], resume: false, engraveMarginMm: 0.8 };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i], v = argv[i + 1];
    if (a === "--origin") { o.origin = String(v || "").replace(/\/+$/, ""); i++; }
    else if (a === "--passcode") { o.passcode = v || ""; i++; }
    else if (a === "--dry") o.dry = true;
    else if (a === "--pattern") { o.pattern = v || ""; i++; }
    else if (a === "--gap-mm") { o.gapMm = +v || 6.4; i++; }
    else if (a === "--min-pt") { o.minPt = +v || 6; i++; }
    else if (a === "--concurrency") { o.concurrency = Math.max(1, Math.min(16, +v || 4)); i++; }
    else if (a === "--upload-master") o.uploadMaster = true;
    else if (a === "--replaces") { o.replaces.push(String(v || "")); i++; }
    else if (a === "--resume") o.resume = true;
    else if (a === "--engrave-margin-mm") { o.engraveMarginMm = +v || 0.8; i++; }
    else if (!a.startsWith("--") && !o.file) o.file = a;
  }
  return o;
}

async function api(origin, passcode, fn, body) {
  const headers = { "Content-Type": "application/json" }; if (passcode) headers["X-Edit-Passcode"] = passcode;
  const res = await fetch(`${origin}/.netlify/functions/${fn}`, { method: "POST", headers, body: JSON.stringify(body || {}) });
  const txt = await res.text(); let data; try { data = JSON.parse(txt); } catch (_) { data = { error: txt.slice(0, 200) }; }
  if (!res.ok) throw new Error(`${fn} ${body && body.op}: HTTP ${res.status} ${data && data.error || ""}`);
  return data;
}
async function upload(origin, passcode, p, buf, contentType) {
  if (buf.length <= 4.5 * 1024 * 1024) { const r = await api(origin, passcode, "charmNestOutput", { op: "put", path: p, contentType, base64: buf.toString("base64") }); return { path: r.path, url: r.url }; }
  const signed = await api(origin, passcode, "charmNestOutput", { op: "sign", path: p, contentType });
  const put = await fetch(signed.uploadUrl, { method: "PUT", headers: signed.headers || { "Content-Type": contentType }, body: buf });
  if (!put.ok) throw new Error(`signed PUT ${put.status}`);
  const fin = await api(origin, passcode, "charmNestOutput", { op: "finalize", path: signed.path, token: signed.token, contentType });
  return { path: fin.path, url: fin.url };
}
function thumbnailPng(Geom, charm, size) {
  let Resvg = null; try { ({ Resvg } = require("@resvg/resvg-js")); } catch (_) { return null; }
  const b = charm.bbox, pad = 2, w = b[2] - b[0] + 2 * pad, h = b[3] - b[1] + 2 * pad, s = size / Math.max(w, h);
  const css = c => `rgb(${Math.round((c[0] || 0) * 255)},${Math.round((c[1] || 0) * 255)},${Math.round((c[2] || 0) * 255)})`;
  const parts = [];
  for (const m of charm.members) { if (m.kind !== "path") continue; const d = Geom.svgPathOf(m); if (!d) continue; const st = m.stroke ? (Math.min(m.strokeRGB[0], m.strokeRGB[1], m.strokeRGB[2]) >= 0.92 ? "#2a2724" : css(m.strokeRGB)) : "none"; parts.push(`<path d="${d}" fill="${m.fill ? css(m.fillRGB) : "none"}" fill-rule="${m.paintOp && m.paintOp.endsWith("*") ? "evenodd" : "nonzero"}" stroke="${st}" stroke-width="${Math.max(0.6 / s, m.lwPt || 0.5)}"/>`); }
  parts.push(`<path d="${Geom.svgPathOf(charm.outline)}" fill="none" stroke="rgba(190,40,40,.9)" stroke-width="${Math.max(1 / s, 0.6)}"/>`);
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${Math.max(8, Math.round(w * s))}" height="${Math.max(8, Math.round(h * s))}" viewBox="0 0 ${w} ${h}"><rect width="100%" height="100%" fill="#ece7dc"/><g transform="translate(${pad - b[0]} ${b[3] + pad}) scale(1 -1)">${parts.join("")}</g></svg>`;
  try { return new Resvg(svg, { fitTo: { mode: "width", value: Math.max(8, Math.round(w * s)) } }).render().asPng(); } catch (_) { return null; }
}

async function main(argv, log = console.log) {
  const o = args(argv);
  if (!o.file) throw new Error('usage: node scripts/index-master.cjs "<master.ai>" --origin <sorter site> [--dry] [--passcode …]');
  if (!o.dry && !o.origin) throw new Error("--origin is required (or use --dry to only parse and report)");
  const { CharmNestPDF: P, Geom: G } = require(path.join(root, "netlify/functions/_charmNestPdf.js"));
  const buf = fs.readFileSync(o.file); const name = path.basename(o.file);
  const masterHash = crypto.createHash("sha256").update(buf).digest("hex");
  log(`${name}: ${(buf.length / 1048576).toFixed(1)} MB · sha256 ${masterHash.slice(0, 12)} · ${o.dry ? "DRY RUN (nothing written)" : "→ " + o.origin}`);
  const progressPath = o.file + ".index-progress.json";
  let progress = { masterHash, done: {} };
  if (o.resume && fs.existsSync(progressPath)) { try { const p = JSON.parse(fs.readFileSync(progressPath, "utf8")); if (p.masterHash === masterHash) progress = p; } catch (_) {} }
  const saveProgress = () => { try { fs.writeFileSync(progressPath, JSON.stringify(progress)); } catch (_) {} };

  const t0 = Date.now();
  const parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), name);
  log(`parsed in ${((Date.now() - t0) / 1000).toFixed(1)} s · ${parsed.segments.length} top-level segment(s) · ${parsed.counts.text} text run(s) · page ${(parsed.pageW * MM).toFixed(0)} × ${(parsed.pageH * MM).toFixed(0)} mm`);
  const g = P.groupCharms(parsed, { minPt: o.minPt });
  const pattern = o.pattern ? new RegExp(o.pattern) : P.SKU_PATTERN_DEFAULT;
  const lab = P.labelCharms(parsed, g.charms, { pattern, gapPt: o.gapMm / MM, widen: 0.25 });
  log(`${g.charms.length} charm outline(s) · ${lab.labels.size} labelled (${lab.skuCount} SKU line(s)) · ${lab.unlabelled.length} unlabelled · ${lab.orphans.length} orphan label(s) · ${lab.duplicates.length} duplicate(s)${lab.undecodable.length ? ` · ${lab.undecodable.length} text run(s) unreadable (outlined or CID font without ToUnicode)` : ""}`);
  if (lab.orphans.length) log(`  orphan labels (no charm within ${o.gapMm} mm above): ${lab.orphans.slice(0, 40).map(x => x.sku).join(", ")}${lab.orphans.length > 40 ? " …" : ""}`);
  if (lab.unlabelled.length) log(`  unlabelled charms (by index): ${lab.unlabelled.slice(0, 40).join(", ")}${lab.unlabelled.length > 40 ? " …" : ""}`);
  if (lab.duplicates.length) log(`  duplicates: ${lab.duplicates.slice(0, 40).map(d => `${d.sku}/${d.also}`).join(", ")}`);

  let masterUp = null;
  if (!o.dry && o.uploadMaster) { masterUp = await upload(o.origin, o.passcode, `charmnest/master/files/${masterHash.slice(0, 12)}-${name.replace(/[^\w.\-]+/g, "_")}`, buf, "application/pdf"); log(`master stored at ${masterUp.path}`); }

  const items = [...lab.labels].map(([index, l]) => ({ index, l, c: g.charms.find(x => x.index === index) })).filter(x => x.c);
  const entries = [], blocked = [], skus = [];
  let done = 0, skipped = 0; const total = items.length; const queue = items.slice();
  const one = async ({ index, l, c }) => {
    const key = l.size ? `${l.sku}__${l.size}` : l.sku;
    if (progress.done[key]) { const d = progress.done[key]; entries.push(d.entry); if (d.blocked) blocked.push(d.blocked); skus.push(l.sku); for (const x of l.extra || []) { entries.push(Object.assign({}, d.entry, { sku: x.sku, size: x.size })); skus.push(x.sku); if (d.blocked) blocked.push({ sku: x.sku, reason: d.blocked.reason }); } skipped++; return; }
    const sil = G.silhouetteBits(c, 6, {});
    const charmHash = P.fnv(P.signature(sil.bits, sil.w, sil.h) + "|" + Math.round((sil.bboxOuter[2] - sil.bboxOuter[0]) * 2) + "x" + Math.round((sil.bboxOuter[3] - sil.bboxOuter[1]) * 2) + "|" + c.members.length);
    const open = (() => { const polys = G.flatten(c.outline, 12); return !polys.length || polys.some(p => Math.hypot(p[0][0] - p[p.length - 1][0], p[0][1] - p[p.length - 1][1]) > 1.5 && !c.outline.closed); })();
    let engravable = true, upAngle = null, upSource = "drawn", flipOk = true, flipWhy = null;
    try { const up = G.upAngleOf(c); upAngle = up.angle; upSource = up.source; const view = G.backView(c, { res: 6, upAngle }); const mask = G.engraveMask(view, { marginMm: o.engraveMarginMm }); const r = G.largestRectangles(mask, 1)[0]; engravable = !!r && ((r.wPt * MM >= 6 && r.hPt * MM >= 3) || (r.wPt * MM >= 3 && r.hPt * MM >= 6)); }
    catch (e) { flipOk = false; flipWhy = e.message; engravable = false; }
    let aiUp = { path: `charmnest/master/${key}.ai`, url: "" }, thumb = null;
    if (!o.dry) {
      const ai = await P.buildSingleCharm(c, parsed);
      aiUp = await upload(o.origin, o.passcode, `charmnest/master/${key}.ai`, Buffer.from(ai), "application/illustrator");
      const png = thumbnailPng(G, c, 168); if (png) thumb = await upload(o.origin, o.passcode, `charmnest/master/${key}.png`, Buffer.from(png), "image/png");
    }
    const reasons = []; if (open) reasons.push("open outline"); if (!flipOk) reasons.push("flip check failed: " + flipWhy);
    const entry = { sku: l.sku, size: l.size, charmHash, widthPt: sil.bboxOuter[2] - sil.bboxOuter[0], heightPt: sil.bboxOuter[3] - sil.bboxOuter[1], areaPt2: sil.areaPt2, members: c.members.length, holes: P.cutLinesOf(c).length, engravable, upAngle, upSource, aiPath: aiUp.path, aiUrl: aiUp.url, thumbPath: thumb && thumb.path, thumbUrl: thumb && thumb.url, open, labelSource: "text", confidence: 1, blocked: reasons.length ? reasons.join("; ") : null };
    entries.push(entry); skus.push(l.sku); const blk = reasons.length ? { sku: l.sku, reason: reasons.join("; ") } : null; if (blk) blocked.push(blk);
    for (const x of l.extra || []) { entries.push(Object.assign({}, entry, { sku: x.sku, size: x.size })); skus.push(x.sku); if (blk) blocked.push({ sku: x.sku, reason: blk.reason }); }   // every further line under the charm: the same design under another SKU
    progress.done[key] = { entry, blocked: blk, extra: l.extra || [] }; done++;
    if (done % 25 === 0) { saveProgress(); log(`  ${done + skipped}/${total} · ${((Date.now() - t0) / 1000).toFixed(0)} s`); }
  };
  await Promise.all(Array.from({ length: o.dry ? 8 : o.concurrency }, async () => { while (queue.length) { const it = queue.shift(); try { await one(it); } catch (e) { log(`  ! ${it.l.sku}: ${e.message}`); blocked.push({ sku: it.l.sku, reason: "not written: " + e.message }); } } }));
  saveProgress();
  // a small ring left loose beside a charm blocks it, as the other routes do
  for (const orp of g.orphans || []) { const b = orp.bbox; if (!b || orp.kind !== "path" || !orp.closed) continue; if (Math.max(b[2] - b[0], b[3] - b[1]) > 13) continue; const cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2; for (const c of g.charms) { if (!c.sku) continue; if (G.distToPolys(cx, cy, G.flatten(c.outline, 8)) <= 3 / MM) { const e = entries.find(x => x.sku === c.sku); if (e && !/detached ring/.test(e.blocked || "")) { e.blocked = (e.blocked ? e.blocked + "; " : "") + "detached ring not merged"; blocked.push({ sku: c.sku, reason: "detached ring not merged" }); } } } }
  log(`${entries.length} SKU entr${entries.length === 1 ? "y" : "ies"} ready (${skipped} from the previous run) · ${blocked.length} blocked · ${((Date.now() - t0) / 1000).toFixed(0)} s`);

  let conflicts = [], sizeMoved = [], written = 0;
  if (!o.dry) {
    written = new Set(entries.map(e => String(e.sku).toUpperCase())).size;   // one record per SKU, its sizes inside it
    for (let i = 0; i < entries.length; i += 150) {
      const r = await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterPutIndex", entries: entries.slice(i, i + 150), masterHash, masterPath: masterUp ? masterUp.path : null, masterName: name, hashSource: "local", replaces: i === 0 ? o.replaces : [] });
      conflicts = conflicts.concat(r.blocked || []); sizeMoved = sizeMoved.concat(r.sizeMoved || []);
      log(`  index ${Math.min(i + 150, entries.length)}/${entries.length}`);
    }
    await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterPutFile", file: { masterHash, path: masterUp ? masterUp.path : null, url: masterUp ? masterUp.url : null, name, charms: g.charms.length, labelled: lab.labels.size, unlabelled: lab.unlabelled, orphans: lab.orphans, duplicates: lab.duplicates, undecodable: lab.undecodable.length, blocked: blocked.concat(conflicts.map(b => ({ sku: b.sku, reason: b.reason }))), skus, replaces: o.replaces, indexedBy: "local-indexer" } });
    if (conflicts.length) log(`  ${conflicts.length} SKU(s) also live in another master file — blocked until fixed: ${conflicts.slice(0, 30).map(b => b.sku).join(", ")}`);
    if (sizeMoved.length) log(`  ${sizeMoved.length} SKU(s) changed size by more than 5 % since the last index: ${sizeMoved.slice(0, 30).map(b => b.sku).join(", ")}`);
    // read the index back: a batch that times out can answer without having stored everything
  {
    const want = [...new Set(entries.map(e => String(e.sku).toUpperCase()))];
    for (let pass = 1; pass <= 3; pass++) {
      const have = new Set();
      for (let i = 0; i < want.length; i += 300) { const r = await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterGetMany", skus: want.slice(i, i + 300) }); for (const k of Object.keys(r.entries || {})) have.add(k); }
      const missing = want.filter(s => !have.has(s));
      if (!missing.length) { log(`verified: all ${want.length} SKU(s) are in the index`); break; }
      log(`  ${missing.length} SKU(s) did not land — writing them again (pass ${pass})`);
      if (pass === 3) { log(`  still missing: ${missing.slice(0, 40).join(", ")}${missing.length > 40 ? " …" : ""}`); break; }
      const again = entries.filter(e => missing.includes(String(e.sku).toUpperCase()));
      for (let i = 0; i < again.length; i += 100) await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterPutIndex", entries: again.slice(i, i + 100), masterHash, masterPath: masterUp ? masterUp.path : null, masterName: name, hashSource: "local", replaces: [] });
    }
  }
  log(`index written: ${written} SKU(s) on ${o.origin} — the Master tab shows them after Reload index`);
  }
  const report = { file: name, bytes: buf.length, masterHash, charms: g.charms.length, labelled: lab.labels.size, unlabelled: lab.unlabelled, orphans: lab.orphans, duplicates: lab.duplicates, undecodable: lab.undecodable.length, blocked, conflicts, sizeMoved, written, dry: o.dry, seconds: Math.round((Date.now() - t0) / 1000), at: new Date().toISOString() };
  try { fs.writeFileSync(o.file + ".index-report.json", JSON.stringify(report, null, 1)); log(`report: ${o.file}.index-report.json`); } catch (_) {}
  return report;
}
module.exports = { main };
if (require.main === module) main(process.argv).catch(e => { console.error("index-master:", e.message); process.exit(1); });
