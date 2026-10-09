#!/usr/bin/env node
/*  scripts/broken-charm-scan.cjs — OFFLINE finder of broken charms in the three masters (and, read-only, in what the library stores).
 *  ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
 *  Born from DACHSHUND_88528 (HUGGIE): the Master tab card showed a ring and a few loose red strokes, no body, "0 holes".
 *  The body was a black CUT path whose ends lay 0.09 pt apart with no closepath: not a closed stroke for the reader, so no
 *  outline, so the charm was its jump ring alone (cause and fix: closeNearlyClosed in charm-nest-pdf.js). This tool looks for
 *  every design that shows the same symptoms, whatever the cause. It writes nothing to the live site and calls no AI.
 *
 *    node --max-old-space-size=4096 scripts/broken-charm-scan.cjs scan "<MASTER.ai>" --out <scan.json> [--grouped <cache.v8>]
 *        parse + group + label with the repo's own reader (the same code the app and the indexer use), then flag, per labelled
 *        design:   lone-ring            its only cut line is a hoop-sized circle
 *                  ink-dropped          drawing next to the design that the reader left out of every charm (an orphan)
 *                  engraving-outside    engraving or hatching that mostly lies outside the cut outline (no cut around it)
 *                  open-outline         the cut outline is not closed (the master draws it open: for the artist)
 *        and per label that found no charm:
 *                  label-without-charm  a SKU-like label with cut or engraving ink right above it that no charm holds
 *        Every design also gets its measures (silhouette area and size after the hoop weld, holes, members) for the
 *        sibling comparison. --grouped reads/writes a v8 cache of the grouped master (parsing + grouping takes minutes).
 *
 *    node scripts/broken-charm-scan.cjs stored --index <index.json> --skus <skus.json|ALL> --out <stored.json> [--concurrency 3]
 *        READ-ONLY GETs of the stored card pictures (thumbUrl of the index records you hand it; the index is the answer of ONE
 *        masterList read), at most a few at a time: per picture the ink share, the share of the largest connected piece of ink,
 *        and whether it has any black (cut) ink at all.
 *
 *    node scripts/broken-charm-scan.cjs files --index <index.json> --skus <list.json> --out <files.json>
 *        READ-ONLY GETs of the stored per-SKU .ai files of the SKUs in the list (the affected list is fine): what each file holds
 *        (outline size, members, cut lines), read back with the repo's reader. Use it for the handful of flagged designs, not for all.
 *
 *    node scripts/broken-charm-scan.cjs report --scan a.json b.json c.json [--stored stored.json] [--index index.json] [--files files.json] --out <dir>
 *        merges the scans, compares each design with its siblings (same name before the number, same variant), with the live
 *        record when the index is given, and writes BROKENCHARM-affected.json and BROKENCHARM-rewrite-skus.json.
 *  ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
"use strict";
const fs = require("fs"), path = require("path"), v8 = require("v8");
const root = path.join(__dirname, "..");
const MM = 25.4 / 72;
const r1 = v => Math.round(v * 10) / 10, r2 = v => Math.round(v * 100) / 100;
const arg = (a, k, d) => { const i = a.indexOf(k); return i >= 0 ? a[i + 1] : d; };
const area = b => Math.max(0, b[2] - b[0]) * Math.max(0, b[3] - b[1]);
const inter = (a, b) => !(a[2] < b[0] || a[0] > b[2] || a[3] < b[1] || a[1] > b[3]);
const grow = (b, g) => [b[0] - g, b[1] - g, b[2] + g, b[3] + g];

// ── the family of a design: its name before the number, and its variant ((HUGGIE), (NECKLACE) …) ─────────────────────────
function familyOf(sku) {
  const s = String(sku || "").toUpperCase().trim();
  const v = /\(([A-Z ]+)\)\s*$/.exec(s), variant = v ? v[1].trim() : "";
  let base = s.replace(/\([A-Z ]+\)\s*$/, "").trim();
  base = base.replace(/[\s_\-]*\d{3,6}\s*$/, "").replace(/[\s_\-]+\d{1,2}\s*$/, "").trim();   // DACHSHUND_88528 → DACHSHUND ; TRICERATOPS 2 → TRICERATOPS
  return { base, variant, name: s.replace(/\([A-Z ]+\)\s*$/, "").trim() };
}

// ═══ scan ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
async function scan(file, opts) {
  const { CharmNestPDF: P, Geom: G } = require(path.join(root, "netlify/functions/_charmNestPdf.js"));
  let parsed, g, lab;
  if (opts.grouped && fs.existsSync(opts.grouped)) { ({ parsed, g, lab } = v8.deserialize(fs.readFileSync(opts.grouped))); }
  else {
    const buf = fs.readFileSync(file);
    parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), path.basename(file));
    g = P.groupCharms(parsed, { minPt: 6 });
    const l = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / MM, widen: 0.25 });
    lab = { labels: l.labels, unlabelled: l.unlabelled, orphans: l.orphans, duplicates: l.duplicates };
    if (opts.saveGrouped) fs.writeFileSync(opts.saveGrouped, v8.serialize({ parsed, g, lab }));
  }
  const master = path.basename(file || opts.grouped || "").replace(/\.(ai|v8)$/i, "");
  const all = parsed.segments.concat(parsed.nested);
  const atMm = b => b ? [r1((b[0] + b[2]) / 2 * MM), r1((parsed.pageH - (b[1] + b[3]) / 2) * MM)] : null;   // centre of a box, in mm from the artboard's top left corner (where Illustrator measures)
  const inkSegs = all.filter(s => s.kind === "path" && s.bbox && /^(cut|engrave|hatch|front detail)/i.test(String(s.layer || "")));
  const orphanSet = new Set(g.orphans || []);
  const inPoly = (x, y, polys) => G.pointInPolys(x, y, polys);
  const rows = [];
  for (const [index, l] of lab.labels) {
    const c = g.charms.find(x => x.index === index); if (!c || !c.outline) continue;
    const skus = [l.sku, ...(l.extra || []).map(x => x.sku)].filter(Boolean).map(x => String(x).toUpperCase());
    const ob = c.outline.bbox, ow = ob[2] - ob[0], oh = ob[3] - ob[1];
    const flags = [];
    // 1 · a lone ring
    const cuts = P.cutLinesOf(c);
    if (Math.max(ow, oh) <= 13 && P.ringLike(c.outline) && !cuts.some(m => Math.max(m.bbox[2] - m.bbox[0], m.bbox[3] - m.bbox[1]) > Math.max(ow, oh))) {
      flags.push({ rule: "lone-ring", why: `the only cut line is a ${r1(Math.max(ow, oh) * MM)} mm circle (a hoop): the body is missing` });
    }
    // 2 · drawing next to the design that no charm holds
    const near = grow(c.bbox, 6), dropped = [];
    for (const s of inkSegs) { if (!orphanSet.has(s) || !inter(s.bbox, near)) continue; const w = s.bbox[2] - s.bbox[0], h = s.bbox[3] - s.bbox[1]; if (Math.max(w, h) < 2.5) continue; dropped.push(s); }
    if (dropped.length) {
      const big = dropped.filter(s => (s.bbox[2] - s.bbox[0]) >= 6 && (s.bbox[3] - s.bbox[1]) >= 6 && /^cut/i.test(String(s.layer || "")));
      const dropArea = dropped.reduce((a, s) => a + area(s.bbox), 0);
      if (big.length || dropArea >= 0.15 * area(c.bbox)) flags.push({ rule: "ink-dropped", openBodies: big.filter(s => !s.closed).length, why: `${dropped.length} drawing(s) beside it belong to no charm (${[...new Set(dropped.map(s => s.layer))].join("/")}${big.length ? `, ${big.length} body-sized cut line(s), ${big.map(s => (s.closed ? "closed" : "open")).join("/")}` : ""})`, detail: dropped.slice(0, 6).map(s => ({ layer: s.layer, closed: !!s.closed, bbox: s.bbox.map(r1) })) });
    }
    // 3 · engraving or hatching with no cut outline around it
    const art = c.members.filter(m => m !== c.outline && m.kind === "path" && !G.isCutLine(m) && G.pathRole(m) === "artwork" && m.bbox);
    if (art.length) {
      const all0 = G.flatten(c.outline, 8), ringArea = pl => Math.abs(pl.reduce((v, p, i) => v + p[0] * pl[(i + 1) % pl.length][1] - p[1] * pl[(i + 1) % pl.length][0], 0) / 2);
      const polys = all0.length ? [all0.reduce((m, pl) => ringArea(pl) > ringArea(m) ? pl : m)] : all0;   // the outermost polygon: a thick outline drawn as a filled ring has a counter that is inside the charm
      let inside = 0, total = 0;
      for (const m of art) { const pts = G.flatten(m, 6).flat(), step = Math.max(1, Math.ceil(pts.length / 60)); for (const p of pts.filter((_, i) => i % step === 0)) { total++; if (inPoly(p[0], p[1], polys) || G.distToPolys(p[0], p[1], polys) <= 1.5) inside++; } }
      if (total >= 6 && inside / total < 0.5) flags.push({ rule: "engraving-outside", why: `${Math.round(100 * (1 - inside / total))} % of its engraving lies outside the cut outline`, detail: { members: art.length } });
    }
    // 4 · an outline that is not closed
    const polys0 = G.flatten(c.outline, 12);
    const opened = !c.outline.closed || polys0.some(p => Math.hypot(p[0][0] - p[p.length - 1][0], p[0][1] - p[p.length - 1][1]) > 1.5 && !c.outline.closed);
    if (opened) flags.push({ rule: "open-outline", why: "the master draws the cut outline open (ends further apart than 1.5 pt): fix in Illustrator" });
    // measures, the way the indexer takes them (hoops welded first)
    let sil = null, holes = cuts.length;
    try { P.integrateRings(c); holes = P.cutLinesOf(c).length; sil = G.silhouetteBits(c, 6, {}); } catch (_) {}
    rows.push({
      master, charm: index, sku: skus[0], skus, name: l.str || null, label: l.bbox ? l.bbox.map(r1) : null, atMm: atMm(c.bbox),
      outlineMm: [r1(ow * MM), r1(oh * MM)], bbox: ob.map(r1),
      widthPt: sil ? r2(sil.bboxOuter[2] - sil.bboxOuter[0]) : null, heightPt: sil ? r2(sil.bboxOuter[3] - sil.bboxOuter[1]) : null, areaPt2: sil ? r2(sil.areaPt2) : null,
      holes, members: c.members.length, nearClosed: c.members.concat([c.outline]).some(m => m.nearClosed != null) || undefined, flags
    });
  }
  // 5 · a SKU-like label that found no charm, with cut or engraving ink right above it
  const orphanLabels = [];
  const lastPoint = s => { let l = null; for (const o of s.subpaths[0]) { if (o[0] === "m" || o[0] === "l") l = o[1]; else if (o[0] === "c") l = o[3]; } return l; };
  const skuLike = s => /(?:[_ ]\d{3,6}|\(HUGGIE\)|\(NECKLACE\)|\(EARRING\))\s*$/i.test(String(s || ""));
  const taken = new Set(); for (const c of g.charms) for (const m of c.members) taken.add(m);
  for (const o of lab.orphans || []) {
    if (!skuLike(o.str) || !o.bbox) continue;
    const lh = o.bbox[3] - o.bbox[1]; if (lh > 30) continue;                         // a heading, not a SKU line
    const cx = (o.bbox[0] + o.bbox[2]) / 2, zone = [o.bbox[0] - 30, o.bbox[3] - 1, o.bbox[2] + 30, o.bbox[3] + 6.4 / MM * 3];
    const above = inkSegs.filter(s => inter(s.bbox, zone) && (s.bbox[0] + s.bbox[2]) / 2 > o.bbox[0] - 20 && (s.bbox[0] + s.bbox[2]) / 2 < o.bbox[2] + 20 && Math.max(s.bbox[2] - s.bbox[0], s.bbox[3] - s.bbox[1]) >= 2.5);
    if (!above.length) continue;
    const loose = above.filter(s => orphanSet.has(s)), cutIn = loose.filter(s => /^cut/i.test(String(s.layer || "")) && Math.max(s.bbox[2] - s.bbox[0], s.bbox[3] - s.bbox[1]) >= 6);
    orphanLabels.push({ master, sku: String(o.sku).toUpperCase(), name: o.str, label: o.bbox.map(r1), atMm: atMm(o.bbox), ink: above.length, looseInk: loose.length, cutBodies: cutIn.length, openCut: cutIn.filter(s => !s.closed).length, gaps: cutIn.filter(s => !s.closed).map(s => r2(Math.hypot(...[0, 1].map(k => s.subpaths[0][0][1][k] - lastPoint(s)[k])))), layers: [...new Set(above.map(s => s.layer))], heldByCharm: above.length - loose.length });
  }
  return { master, at: new Date().toISOString(), counts: { charms: g.charms.length, labelled: lab.labels.size, orphanSegments: (g.orphans || []).length, orphanLabels: (lab.orphans || []).length }, rows, orphanLabels };
}

// ═══ stored pictures (read-only GETs) ═══════════════════════════════════════════════════════════════════════════════════════
function pngStats(buf) {
  const zlib = require("zlib");
  if (buf.readUInt32BE(0) !== 0x89504e47) throw new Error("not a PNG");
  let p = 8, w = 0, h = 0, bd = 0, ct = 0, il = 0; const idat = [];
  while (p < buf.length) { const len = buf.readUInt32BE(p), type = buf.toString("latin1", p + 4, p + 8), d = buf.subarray(p + 8, p + 8 + len); p += 12 + len; if (type === "IHDR") { w = d.readUInt32BE(0); h = d.readUInt32BE(4); bd = d[8]; ct = d[9]; il = d[12]; } else if (type === "IDAT") idat.push(d); else if (type === "IEND") break; }
  if (bd !== 8 || il !== 0 || ![2, 6].includes(ct)) throw new Error(`unsupported PNG (depth ${bd}, type ${ct}, interlace ${il})`);
  const ch = ct === 6 ? 4 : 3, raw = zlib.inflateSync(Buffer.concat(idat)), stride = w * ch, out = Buffer.alloc(h * stride);
  for (let y = 0; y < h; y++) {
    const f = raw[y * (stride + 1)], src = raw.subarray(y * (stride + 1) + 1, (y + 1) * (stride + 1)), dst = out.subarray(y * stride, (y + 1) * stride), up = y ? out.subarray((y - 1) * stride, y * stride) : null;
    for (let x = 0; x < stride; x++) { const a = x >= ch ? dst[x - ch] : 0, b = up ? up[x] : 0, c = up && x >= ch ? up[x - ch] : 0; let v = src[x]; if (f === 1) v += a; else if (f === 2) v += b; else if (f === 3) v += (a + b) >> 1; else if (f === 4) { const pp = a + b - c, pa = Math.abs(pp - a), pb = Math.abs(pp - b), pc = Math.abs(pp - c); v += pa <= pb && pa <= pc ? a : pb <= pc ? b : c; } dst[x] = v & 255; }
  }
  const N = w * h, ink = new Uint8Array(N); let n = 0, red = 0, dark = 0;
  for (let i = 0; i < N; i++) {
    const r = out[i * ch], gg = out[i * ch + 1], b = out[i * ch + 2], a = ch === 4 ? out[i * 4 + 3] : 255;
    if (a > 20 && Math.abs(r - 0xec) + Math.abs(gg - 0xe7) + Math.abs(b - 0xdc) > 60) { ink[i] = 1; n++; if (r > 150 && gg < 100 && b < 100) red++; else if (r < 80 && gg < 80 && b < 80) dark++; }
  }
  const dil = new Uint8Array(N);                                                      // one pixel wider, so a thin stroke stays one piece
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { if (!ink[y * w + x]) continue; for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) { const X = x + dx, Y = y + dy; if (X >= 0 && Y >= 0 && X < w && Y < h) dil[Y * w + X] = 1; } }
  const lab = new Int32Array(N); let best = { n: 0 }, pieces = 0, widest = 0;
  for (let s = 0; s < N; s++) {
    if (!dil[s] || lab[s]) continue; pieces++; const st = [s]; lab[s] = pieces; let x0 = w, y0 = h, x1 = 0, y1 = 0, m = 0;
    while (st.length) { const i = st.pop(), x = i % w, y = (i - x) / w; if (ink[i]) { m++; if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y; } for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) { const X = x + dx, Y = y + dy; if (X < 0 || Y < 0 || X >= w || Y >= h) continue; const j = Y * w + X; if (dil[j] && !lab[j]) { lab[j] = pieces; st.push(j); } } }
    if (m > best.n) best = { n: m, x0, y0, x1, y1 };
    if (m >= 6) widest = Math.max(widest, Math.max((x1 - x0 + 1) / w, (y1 - y0 + 1) / h));
  }
  return { w, h, inkShare: r2(n / N * 100) / 100, redShare: r2(red / N * 100) / 100, blackShare: r2(dark / N * 100) / 100, pieces, largestPieceShare: n ? r2(best.n / n * 100) / 100 : 0, largestPieceSpan: best.n ? r2(Math.max((best.x1 - best.x0 + 1) / w, (best.y1 - best.y0 + 1) / h) * 100) / 100 : 0, widestPieceSpan: r2(widest * 100) / 100 };
}
async function stored(a) {
  const idx = JSON.parse(fs.readFileSync(arg(a, "--index"), "utf8")), entries = idx.entries || idx;
  const spec = arg(a, "--skus", "ALL"); let want = null;
  if (spec !== "ALL") { const j = JSON.parse(fs.readFileSync(spec, "utf8")); want = new Set((Array.isArray(j) ? j : j.skus || []).map(x => String(x && x.sku || x).toUpperCase())); }
  const conc = Math.max(1, Math.min(4, +arg(a, "--concurrency", 3) || 3)), keep = arg(a, "--save-dir"); if (keep) fs.mkdirSync(keep, { recursive: true });
  const byFile = new Map();                                                           // one GET per picture, however many SKUs share it
  for (const e of entries) { if (!e.thumbUrl || (want && !want.has(String(e.sku).toUpperCase()))) continue; if (!byFile.has(e.thumbPath || e.thumbUrl)) byFile.set(e.thumbPath || e.thumbUrl, { url: e.thumbUrl, skus: [] }); byFile.get(e.thumbPath || e.thumbUrl).skus.push(e.sku); }
  const queue = [...byFile.entries()], out = {}, errors = []; let done = 0;
  await Promise.all(Array.from({ length: conc }, async () => {
    while (queue.length) {
      const [k, v] = queue.shift();
      try { const res = await fetch(v.url); if (!res.ok) throw new Error("HTTP " + res.status); const body = Buffer.from(await res.arrayBuffer()); if (keep) { const f = path.join(keep, String(k).replace(/[^\w.\-]+/g, "_")); fs.writeFileSync(f, body); } const st = pngStats(body); for (const s of v.skus) out[s] = Object.assign({ file: k }, st); }
      catch (e) { errors.push({ file: k, error: e.message }); }
      if (++done % 200 === 0) console.log(`  ${done}/${byFile.size}`);
      await new Promise(r => setTimeout(r, 120));
    }
  }));
  fs.writeFileSync(arg(a, "--out"), JSON.stringify({ at: new Date().toISOString(), files: byFile.size, errors, pictures: out }));
  console.log(`stored pictures read: ${byFile.size} file(s), ${errors.length} error(s)`);
}


// ═══ stored per-SKU files (read-only GETs) ══════════════════════════════════════════════════════════════════════════════════
async function files(a) {
  const { CharmNestPDF: P, Geom: G } = require(path.join(root, "netlify/functions/_charmNestPdf.js"));
  const idx = JSON.parse(fs.readFileSync(arg(a, "--index"), "utf8")), entries = idx.entries || idx, by = new Map(entries.map(e => [String(e.sku).toUpperCase(), e]));
  const j = JSON.parse(fs.readFileSync(arg(a, "--skus"), "utf8")), skus = (Array.isArray(j) ? j : j.skus || j.affected || []).map(x => String(x && x.sku || x).toUpperCase());
  const out = {}, done = new Set();
  for (const sku of skus) {
    const e = by.get(sku); if (!e || !e.aiUrl || done.has(e.aiPath)) { if (e && done.has(e.aiPath)) out[sku] = out[[...by.values()].find(x => x.aiPath === e.aiPath && out[String(x.sku).toUpperCase()]).sku.toUpperCase()]; continue; }
    done.add(e.aiPath);
    try {
      const res = await fetch(e.aiUrl); if (!res.ok) throw new Error("HTTP " + res.status);
      const buf = Buffer.from(await res.arrayBuffer()), parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), "stored.ai"), g = P.groupCharms(parsed, { minPt: 2 });
      const c = g.charms.slice().sort((x, y) => area(y.bbox) - area(x.bbox))[0];
      const layers = {}; for (const m of c ? c.members : []) layers[m.layer || "-"] = (layers[m.layer || "-"] || 0) + 1;
      out[sku] = { file: e.aiPath, bytes: buf.length, charms: g.charms.length, outlineMm: c ? [r1((c.outline.bbox[2] - c.outline.bbox[0]) * MM), r1((c.outline.bbox[3] - c.outline.bbox[1]) * MM)] : null, extentMm: c ? [r1((c.bbox[2] - c.bbox[0]) * MM), r1((c.bbox[3] - c.bbox[1]) * MM)] : null, members: c ? c.members.length : 0, cutLines: c ? P.cutLinesOf(c).length : 0, layers, orphans: g.orphans.length };
    } catch (err) { out[sku] = { file: e.aiPath, error: err.message }; }
    await new Promise(r => setTimeout(r, 150));
  }
  fs.writeFileSync(arg(a, "--out"), JSON.stringify({ at: new Date().toISOString(), files: out }));
  console.log(`stored files read: ${done.size}`);
}

// ═══ report ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
function median(v) { const s = v.slice().sort((x, y) => x - y); return s.length ? (s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2) : 0; }
function report(a) {
  const scans = (a.slice(a.indexOf("--scan") + 1).filter((x, i, arr) => !x.startsWith("--") && arr.slice(0, i).every(y => !y.startsWith("--")))).map(f => JSON.parse(fs.readFileSync(f, "utf8")));
  const idx = arg(a, "--index") ? JSON.parse(fs.readFileSync(arg(a, "--index"), "utf8")) : null, live = new Map();
  for (const e of (idx && (idx.entries || idx)) || []) live.set(String(e.sku).toUpperCase(), e);
  const pics = arg(a, "--stored") ? JSON.parse(fs.readFileSync(arg(a, "--stored"), "utf8")).pictures : {};
  const outDir = arg(a, "--out", "."); fs.mkdirSync(outDir, { recursive: true });
  const rows = scans.flatMap(s => s.rows);
  // siblings: same name before the number and the same variant; the comparison uses the design's own silhouette area
  const fam = new Map();
  for (const r of rows) for (const sku of r.skus) { const f = familyOf(sku); const k = f.base + "|" + f.variant; if (!fam.has(k)) fam.set(k, []); fam.get(k).push({ sku, r }); }
  const affected = new Map();
  const add = (sku, reason, extra) => { const k = String(sku).toUpperCase(); if (!affected.has(k)) affected.set(k, { sku: k, reasons: [], ...extra }); const e = affected.get(k); if (!e.reasons.some(x => x.rule === reason.rule)) e.reasons.push(reason); Object.assign(e, extra || {}); };
  for (const r of rows) {
    for (const sku of r.skus) {
      const base = { master: r.master, charm: r.charm, atMm: r.atMm, widthMm: r.widthPt != null ? r1(r.widthPt * MM) : null, heightMm: r.heightPt != null ? r1(r.heightPt * MM) : null, holes: r.holes, members: r.members };
      for (const f of r.flags) add(sku, f, base);
      const f = familyOf(sku), sibs = (fam.get(f.base + "|" + f.variant) || []).filter(x => x.r !== r && x.r.areaPt2 > 0);
      if (sibs.length >= 3 && r.areaPt2 > 0) { const med = median(sibs.map(x => x.r.areaPt2)); if (r.areaPt2 < 0.3 * med) add(sku, { rule: "small-vs-siblings", why: `silhouette ${r1(r.areaPt2 * MM * MM)} mm² against ${r1(med * MM * MM)} mm² (median of ${sibs.length} ${f.base}${f.variant ? " (" + f.variant + ")" : ""} designs)` }, base); }
      const twin = rows.filter(x => x !== r && x.skus.some(s2 => { const g = familyOf(s2); return g.name === f.name && g.variant !== f.variant; }));
      // (a huggie is smaller than the full-size piece of the same design, so only an extreme ratio counts)
      if (f.variant === "HUGGIE" && twin.length && r.areaPt2 > 0) { const t = twin.find(x => x.areaPt2 > 0); if (t && r.areaPt2 < 0.08 * t.areaPt2) add(sku, { rule: "small-vs-twin", why: `silhouette ${r1(r.areaPt2 * MM * MM)} mm² against ${r1(t.areaPt2 * MM * MM)} mm² for the same design without (HUGGIE)` }, base); }
      // the stored record and picture
      const e = live.get(sku.toUpperCase());
      if (e && r.areaPt2 > 0 && e.areaPt2 > 0 && (e.areaPt2 < 0.5 * r.areaPt2 || e.areaPt2 > 2 * r.areaPt2)) add(sku, { rule: "live-record-differs", why: `the library holds ${r1(e.areaPt2 * MM * MM)} mm² (${r1(e.widthPt * MM)} x ${r1(e.heightPt * MM)} mm, ${e.holes} holes); the reader now gives ${r1(r.areaPt2 * MM * MM)} mm² (${r1(r.widthPt * MM)} x ${r1(r.heightPt * MM)} mm, ${r.holes} holes)` }, base);
      const pic = pics[e ? e.sku : sku];
      if (pic && (pic.widestPieceSpan < 0.7 || pic.inkShare < 0.02)) add(sku, { rule: "stored-picture", why: `the stored card picture is ${pic.inkShare < 0.02 ? "almost empty" : "no body, only loose pieces"} (ink ${Math.round(pic.inkShare * 1000) / 10} %, the widest connected piece spans ${Math.round(pic.widestPieceSpan * 100)} % of the picture, ${pic.pieces} pieces)` }, base);
    }
  }
  for (const s of scans) for (const o of s.orphanLabels) {
    if (!o.looseInk) continue;                                                       // the ink above it is held by charms: another matter (the label sits too far from them)
    add(o.sku, { rule: "label-without-charm", openBodies: o.openCut, gaps: o.gaps, why: `its label has ${o.looseInk} drawing(s) right above (${o.layers.join("/")}${o.cutBodies ? `, ${o.cutBodies} body-sized cut line(s), ${o.openCut} open${o.gaps && o.gaps.length ? ", gap " + o.gaps.join("/") + " pt" : ""}` : ""}) that no charm holds: the design is missing from the library` }, { master: o.master, charm: null, atMm: o.atMm });
  }
  // what each design needs: the reader now reads it right (a re-index writes it), the stored data is stale, the artist must fix the master, or a person looks
  const stored1 = arg(a, "--files") ? JSON.parse(fs.readFileSync(arg(a, "--files"), "utf8")).files : {};
  for (const e of affected.values()) { const sf = stored1[e.sku]; if (sf) e.storedFile = sf; const lv = live.get(e.sku); if (lv) e.library = { widthMm: r1(lv.widthPt * MM), heightMm: r1(lv.heightPt * MM), areaMm2: r1(lv.areaPt2 * MM * MM), holes: lv.holes, members: lv.members, file: lv.aiPath, picture: lv.thumbPath }; }
  const rowBySku = new Map(); for (const r of rows) for (const sku of r.skus) rowBySku.set(sku.toUpperCase(), r);
  const rewrite = new Map(), addRewrite = (sku, why) => { const k = String(sku).toUpperCase(); if (!rewrite.has(k)) rewrite.set(k, why); };
  for (const e of affected.values()) {
    const r = rowBySku.get(e.sku), rules = e.reasons.map(x => x.rule), own = r ? r.flags.length : 1;
    const masterSide = e.reasons.some(x => (x.rule === "open-outline") || (x.openBodies > 0));
    e.inLibrary = live.size ? live.has(e.sku) : undefined;
    e.fix = r && r.nearClosed && !own ? "reader" : masterSide ? "master" : r && !own && rules.every(x => x === "live-record-differs" || x === "stored-picture") ? "reindex" : "review";
    e.needs = { reader: "re-index: the reader now reads it right (closeNearlyClosed in charm-nest-pdf.js)", master: "the artist closes or completes the cut line in Illustrator, then re-index", reindex: "re-index: the stored record or picture is stale against what the reader gives now", review: "a person looks at the drawing (not a certain defect)" }[e.fix];
  }
  for (const r of rows) if (r.nearClosed) for (const sku of r.skus) addRewrite(sku, "reader");                  // every line under a design whose cut line the reader now closes (they share one file)
  for (const e of affected.values()) if (e.fix === "reindex" || e.fix === "reader") for (const sku of (rowBySku.get(e.sku) || { skus: [e.sku] }).skus) addRewrite(sku, e.fix);
  const list = [...affected.values()].sort((x, y) => x.sku < y.sku ? -1 : 1);
  const fixBy = {}; for (const e of list) fixBy[e.fix] = (fixBy[e.fix] || 0) + 1;
  fs.writeFileSync(path.join(outDir, "BROKENCHARM-affected.json"), JSON.stringify({ at: new Date().toISOString(), masters: scans.map(s => ({ master: s.master, at: s.at, counts: s.counts })), count: list.length, byFix: fixBy, affected: list }, null, 1));
  const rw = [...rewrite.keys()].sort();
  fs.writeFileSync(path.join(outDir, "BROKENCHARM-rewrite-skus.json"), JSON.stringify({ at: new Date().toISOString(), note: "SKUs to re-index (index-master.cjs --only <this file>): the reader now reads these right, or their stored record or picture is stale. A SKU under a design with other SKUs travels with it. Designs broken in the master itself are NOT here: they are fix=master in BROKENCHARM-affected.json.", count: rw.length, newInLibrary: live.size ? rw.filter(k => !live.has(k)) : undefined, skus: rw, why: Object.fromEntries(rw.map(k => [k, rewrite.get(k)])) }, null, 1));
  console.log(`${list.length} SKU(s) flagged → ${path.join(outDir, "BROKENCHARM-affected.json")} ${JSON.stringify(fixBy)} · ${rw.length} to re-index → BROKENCHARM-rewrite-skus.json`);
  const by = {}; for (const e of list) for (const r of e.reasons) by[r.rule] = (by[r.rule] || 0) + 1; console.log(JSON.stringify(by));
  return list;
}

module.exports = { scan, stored, files, report, pngStats, familyOf };
if (require.main === module) {
  const a = process.argv.slice(2), cmd = a[0];
  (async () => {
    if (cmd === "scan") {
      const out = arg(a, "--out"); if (!a[1] || !out) throw new Error("usage: scan <master.ai> --out <scan.json> [--grouped cache.v8]");
      const res = await scan(a[1], { grouped: arg(a, "--grouped"), saveGrouped: arg(a, "--save-grouped") });
      fs.writeFileSync(out, JSON.stringify(res)); const by = {}; for (const r of res.rows) for (const f of r.flags) by[f.rule] = (by[f.rule] || 0) + 1;
      console.log(`${res.master}: ${res.rows.length} designs, ${res.orphanLabels.length} label(s) without a charm but with ink above · flags ${JSON.stringify(by)} → ${out}`);
    } else if (cmd === "stored") await stored(a);
    else if (cmd === "files") await files(a);
    else if (cmd === "report") report(a);
    else throw new Error("usage: scan | stored | report (see the top of this file)");
  })().catch(e => { console.error("broken-charm-scan:", e.message); process.exit(1); });
}
