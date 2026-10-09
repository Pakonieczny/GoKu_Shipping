#!/usr/bin/env node
/*  scripts/blackfill-audit.cjs
 *  ═══════════════════════════════════════════════════════════════════════
 *  Offline audit of the black-fill rule (charm-nest-pdf.js "a black FILL is blue hatching"): a black FILLED area inside a
 *  charm is blue hatching, never a hole, a cut line or a black body.
 *
 *  For every design of a staged master (a folder written by `index-master.cjs --out-dir`: records.json and files/) it reads the
 *  per-SKU file the way the Master tab does (parse, group, take the largest charm, weld the hoop), twice: with the rule OFF
 *  (opts.blackFill === false: the reader before this rule) and ON. It compares the two (holes, members, size, area, what is
 *  painted) and measures how dark each stored picture is: the share of dark pixels (all three channels under 80, so blue
 *  hatching is not dark) in the charm's own body, for the picture the stored-PNG route draws (scripts/index-master.cjs
 *  thumbnailPng) and for the picture the Master tab draws on a canvas (drawCharm). No design may exceed 90 %.
 *
 *  A design whose black art is a round disc (rule 2) cannot be re-read from its stored file: the weld that joined the hoop
 *  replaced the compound fill by a cut outline and one cut-out per white strip, so the fill itself is no longer in the file.
 *  Those are found in the master (`--master NAME=FILE`): every black fill that is one perfect circle with at least two inner
 *  holes, read again with a few neighbours around it and labelled by the SKU text written beside it.
 *
 *    node scripts/blackfill-audit.cjs --stage DIR [--stage DIR …] --out DIR [--master NAME=FILE …] [--limit N] [--only FILE]
 *      --stage DIR   a staged master (records.json + files/)
 *      --master N=F  the master file itself, named as the stage folder's records say (masterName contains N), for rule 2
 *      --out DIR     BLACKFILL-affected.json, BLACKFILL-metrics.json, BLACKFILL-rewrite-skus.json, BLACKFILL-summary.json
 *      --only FILE   restrict to the aiPath names (or SKUs) listed in FILE (JSON list or one per line)
 *  Reads only. Writes nothing but its own result files.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const fs = require("fs"), path = require("path"), zlib = require("zlib");
const root = path.join(__dirname, "..");
const MM = 25.4 / 72;
const { CharmNestPDF: P, Geom: G } = require(path.join(root, "netlify/functions/_charmNestPdf.js"));
// resvg loads every system font on each render unless told not to; these pictures hold no text (speed only)
try { const rj = require("@resvg/resvg-js"), Orig = rj.Resvg; rj.Resvg = class extends Orig { constructor(svg, opts = {}) { super(svg, { ...opts, font: { loadSystemFonts: false, ...(opts.font || {}) } }); } }; } catch (_) { /* no resvg: pictures are not rendered */ }
const IM = require("./index-master.cjs");

function args(argv) {
  const o = { stages: [], masters: {}, out: null, limit: 0, only: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i], v = argv[i + 1];
    if (a === "--stage") { o.stages.push(v); i++; }
    else if (a === "--master") { const k = String(v).indexOf("="); o.masters[String(v).slice(0, k).toUpperCase()] = String(v).slice(k + 1); i++; }
    else if (a === "--out") { o.out = v; i++; }
    else if (a === "--limit") { o.limit = +v || 0; i++; }
    else if (a === "--only") { o.only = v; i++; }
  }
  if (!o.stages.length || !o.out) throw new Error("usage: node scripts/blackfill-audit.cjs --stage DIR [--stage DIR …] --out DIR [--master NAME=FILE …] [--limit N] [--only FILE]");
  return o;
}

/* ── a minimal PNG reader (resvg writes 8-bit RGBA, not interlaced) ─────────────────────────────────────────────── */
function decodePng(buf) {
  let p = 8, W = 0, H = 0, ct = 6; const idat = [];
  while (p < buf.length) { const len = buf.readUInt32BE(p), type = buf.toString("latin1", p + 4, p + 8), data = buf.subarray(p + 8, p + 8 + len); if (type === "IHDR") { W = data.readUInt32BE(0); H = data.readUInt32BE(4); ct = data[9]; } else if (type === "IDAT") idat.push(data); p += 12 + len; }
  const bpp = ct === 6 ? 4 : ct === 2 ? 3 : 4, raw = zlib.inflateSync(Buffer.concat(idat)), stride = W * bpp, out = Buffer.alloc(W * H * 4);
  let prev = Buffer.alloc(stride);
  for (let y = 0; y < H; y++) {
    const f = raw[y * (stride + 1)], line = Buffer.from(raw.subarray(y * (stride + 1) + 1, (y + 1) * (stride + 1)));
    for (let x = 0; x < stride; x++) {
      const a = x >= bpp ? line[x - bpp] : 0, b = prev[x], c = x >= bpp ? prev[x - bpp] : 0;
      line[x] = (line[x] + (f === 0 ? 0 : f === 1 ? a : f === 2 ? b : f === 3 ? ((a + b) >> 1) : (() => { const pp = a + b - c, pa = Math.abs(pp - a), pb = Math.abs(pp - b), pc = Math.abs(pp - c); return pa <= pb && pa <= pc ? a : pb <= pc ? b : c; })())) & 255;
    }
    for (let x = 0; x < W; x++) { out[(y * W + x) * 4] = line[x * bpp]; out[(y * W + x) * 4 + 1] = line[x * bpp + 1]; out[(y * W + x) * 4 + 2] = line[x * bpp + 2]; out[(y * W + x) * 4 + 3] = bpp === 4 ? line[x * bpp + 3] : 255; }
    prev = line;
  }
  return { W, H, px: out };
}

/* ── a Canvas2D subset that records into SVG, so drawCharm itself can be run and rasterised ─────────────────────── */
class SvgCtx {
  constructor(w, h) { this.w = w; this.h = h; this.out = []; this.d = ""; this.fillStyle = "#000"; this.strokeStyle = "#000"; this.lineWidth = 1; this.fills = []; this.strokes = []; this.stack = []; }
  save() { this.stack.push([this.fillStyle, this.strokeStyle, this.lineWidth]); } restore() { const s = this.stack.pop(); if (s) [this.fillStyle, this.strokeStyle, this.lineWidth] = s; }
  beginPath() { this.d = ""; } moveTo(x, y) { this.d += `M${x.toFixed(2)} ${y.toFixed(2)}`; } lineTo(x, y) { this.d += `L${x.toFixed(2)} ${y.toFixed(2)}`; }
  bezierCurveTo(a, b, c, d, e, f) { this.d += `C${[a, b, c, d, e, f].map(v => v.toFixed(2)).join(" ")}`; } closePath() { this.d += "Z"; }
  fillRect(x, y, w, h) { this.out.push(`<rect x="${x}" y="${y}" width="${w}" height="${h}" fill="${this.fillStyle}"/>`); }
  fill(rule) { if (!this.d) return; this.fills.push(this.fillStyle); this.out.push(`<path d="${this.d}" fill="${this.fillStyle}" fill-rule="${rule === "evenodd" ? "evenodd" : "nonzero"}"/>`); }
  stroke() { if (!this.d) return; this.strokes.push(this.strokeStyle); this.out.push(`<path d="${this.d}" fill="none" stroke="${this.strokeStyle}" stroke-width="${this.lineWidth}"/>`); }
  setLineDash() {} clip() {} arc() {} rect() {}
  svg() { return `<svg xmlns="http://www.w3.org/2000/svg" width="${this.w}" height="${this.h}" viewBox="0 0 ${this.w} ${this.h}">${this.out.join("")}</svg>`; }
}
let Resvg = null; try { ({ Resvg } = require("@resvg/resvg-js")); } catch (_) { Resvg = null; }
const raster = svg => { const r = new Resvg(svg, { fitTo: { mode: "original" } }).render(); return { W: r.width, H: r.height, px: Buffer.from(r.pixels) }; };

/* ── one design: read it the way the Master tab does ────────────────────────────────────────────────────────────── */
async function load(bytes, name, blackFill) {
  const parsed = await P.parseSource(new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength), name);
  const g = P.groupCharms(parsed, { minPt: 6, blackFill });
  if (!g.charms.length) return null;
  const charm = g.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
  for (const c of g.charms) if (c !== charm) { for (const m of c.members) if (!charm.members.includes(m)) charm.members.push(m); charm.bbox = [Math.min(charm.bbox[0], c.bbox[0]), Math.min(charm.bbox[1], c.bbox[1]), Math.max(charm.bbox[2], c.bbox[2]), Math.max(charm.bbox[3], c.bbox[3])]; }
  const weld = P.integrateRings(charm);
  return { charm, weld };
}
/** The numbers a record carries, as index-master.cjs measures them. */
function facts(charm) {
  const sil = G.silhouetteBits(charm, 6, {});
  return { members: charm.members.length, holes: P.cutLinesOf(charm).length, widthPt: sil.bboxOuter[2] - sil.bboxOuter[0], heightPt: sil.bboxOuter[3] - sil.bboxOuter[1], areaPt2: sil.areaPt2,
    hatched: charm.members.filter(m => m.hatchBlue).length, disc: charm.members.some(m => m.discOf) };
}
const near = (a, b) => Math.abs(a - b) <= Math.max(0.05, 0.002 * Math.max(Math.abs(a), Math.abs(b)));
const isDark = (px, i) => px[i + 3] > 200 && px[i] < 80 && px[i + 1] < 80 && px[i + 2] < 80;
/** Share of dark pixels inside the charm's own body, and over the whole picture, for a picture drawn at `size` with the app's 2 pt padding.
 *  The body is the filled area of the charm's outline with its edge band (2 px) taken off: a charm drawn as a thin line (an outline filled
 *  only 0.25 pt wide, the rainbow arcs, an elephant's silhouette) has no interior to be black, and its two hairline edges would otherwise
 *  count as a "100 % black body". `bodyEdge` is the share without that band, `thin` says the body is mostly edge band. */
function darkShare(img, charm, size) {
  const b = charm.bbox, pad = 2, w = b[2] - b[0] + 2 * pad, h = b[3] - b[1] + 2 * pad, s = size / Math.max(w, h), W = Math.max(8, Math.round(w * s)), H = Math.max(8, Math.round(h * s));
  const mask = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${w} ${h}"><rect width="100%" height="100%" fill="#fff"/><g transform="translate(${pad - b[0]} ${b[3] + pad}) scale(1 -1)"><path d="${G.svgPathOf(charm.outline)}" fill="#000" fill-rule="${String(charm.outline.paintOp || "").endsWith("*") ? "evenodd" : "nonzero"}"/></g></svg>`;
  const m = raster(mask), R = 2, inB = new Uint8Array(m.W * m.H); for (let i = 0; i < inB.length; i++) inB[i] = m.px[i * 4] < 128 ? 1 : 0;
  const tmp = new Uint8Array(inB.length), inner = new Uint8Array(inB.length);                       // erosion by R px (square element, two passes)
  for (let y = 0; y < m.H; y++) for (let x = 0; x < m.W; x++) { let ok = 1; for (let d = -R; d <= R && ok; d++) { const xx = x + d; if (xx < 0 || xx >= m.W || !inB[y * m.W + xx]) ok = 0; } tmp[y * m.W + x] = ok; }
  for (let y = 0; y < m.H; y++) for (let x = 0; x < m.W; x++) { let ok = 1; for (let d = -R; d <= R && ok; d++) { const yy = y + d; if (yy < 0 || yy >= m.H || !tmp[yy * m.W + x]) ok = 0; } inner[y * m.W + x] = ok; }
  let all = 0, total = img.W * img.H, bodyEdge = 0, darkEdge = 0, body = 0, dark = 0;
  for (let y = 0; y < img.H; y++) for (let x = 0; x < img.W; x++) {
    const i = (y * img.W + x) * 4, d = isDark(img.px, i); if (d) all++;
    const mx = Math.min(m.W - 1, Math.round(x * m.W / img.W)), my = Math.min(m.H - 1, Math.round(y * m.H / img.H)), k = my * m.W + mx;
    if (inB[k]) { bodyEdge++; if (d) darkEdge++; }
    if (inner[k]) { body++; if (d) dark++; }
  }
  return { body: body ? dark / body : 0, bodyEdge: bodyEdge ? darkEdge / bodyEdge : 0, thin: bodyEdge > 0 && body < 0.25 * bodyEdge, picture: all / total };
}
/** What the Master tab paints for the charm: drawCharm on a canvas, and the stored-PNG route. */
function pictures(charm) {
  const b = charm.bbox, pad = 2, w = b[2] - b[0] + 2 * pad, h = b[3] - b[1] + 2 * pad, s = 168 / Math.max(w, h), W = Math.max(8, Math.round(w * s)), H = Math.max(8, Math.round(h * s));
  const ctx = new SvgCtx(W, H); ctx.fillStyle = "#ece7dc"; ctx.fillRect(0, 0, W, H);
  P.drawCharm(ctx, charm, (x, y) => [(x - b[0] + pad) * s, (b[3] + pad - y) * s], s);
  const canvas = raster(ctx.svg());
  const png = IM.thumbnailPng(G, charm, 168, P); const stored = png ? decodePng(png) : null;
  return { canvas, stored, fills: [...new Set(ctx.fills)], strokes: [...new Set(ctx.strokes)].length, key: require("crypto").createHash("sha1").update(ctx.svg()).digest("hex").slice(0, 12), storedKey: png ? require("crypto").createHash("sha1").update(png).digest("hex").slice(0, 12) : null };
}
function measure(charm) {
  const f = facts(charm), pic = pictures(charm);
  return { ...f, fills: pic.fills, picture: pic.key, storedPicture: pic.storedKey,
    darkCanvas: darkShare(pic.canvas, charm, 168), darkStored: pic.stored ? darkShare(pic.stored, charm, 168) : null };
}
const pct = x => Math.round(x * 1000) / 10;

async function design(bytes, name) {
  const now = await load(bytes, name, true); if (!now) return { error: "no outline" };
  const after = measure(now.charm), stamped = after.hatched > 0 || after.disc;
  let before = after;
  if (stamped) { const old = await load(bytes, name, false); before = measure(old.charm); }
  return { before, after, changed: stamped };
}

/* ── rule 2 in the master: round black fills with art in them ───────────────────────────────────────────────────── */
async function discsOf(file, log) {
  const buf = fs.readFileSync(file); const parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), path.basename(file));
  const all = parsed.segments.concat(parsed.nested), found = [];
  for (const s of all) {
    if (!P.isBlackFill(s) || !s.bbox || (s.subpaths || []).length < 3) continue;
    const w = s.bbox[2] - s.bbox[0], h = s.bbox[3] - s.bbox[1]; if (w < 6 || h < 6 || w > 200) continue;
    if (P.engravedDiscOf({ outline: s }) >= 0) found.push(s);
  }
  log(`${path.basename(file)}: ${found.length} round black fill(s) with art inside`);
  const out = [];
  for (const s of found) {
    const reg = [s.bbox[0] - 40, s.bbox[1] - 25, s.bbox[2] + 40, s.bbox[3] + 40];
    const segs = all.filter(x => x.bbox && x.kind !== "clip" && x.bbox[0] < reg[2] && x.bbox[2] > reg[0] && x.bbox[1] < reg[3] && x.bbox[3] > reg[1] && (x.kind === "text" || x.bbox[2] - x.bbox[0] < 200));
    const result = {};
    for (const on of [false, true]) {
      // every run re-reads the segments (a stamped segment would carry its answer into the other run)
      const mini = () => ({ segments: segs.map(x => ({ ...x })), nested: [], pageW: parsed.pageW, pageH: parsed.pageH });
      const parsedMini = mini(); const g = P.groupCharms(parsedMini, { minPt: 6, blackFill: on });
      const c = g.charms.find(k => k.bbox[0] <= s.bbox[0] + 1 && k.bbox[2] >= s.bbox[2] - 1 && k.bbox[1] <= s.bbox[1] + 1 && k.bbox[3] >= s.bbox[3] - 1); if (!c) { result.error = "charm not found"; break; }
      let skus = [];
      try { const lab = P.labelCharms(parsedMini, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / MM, widen: 0.25 }); const l = lab.labels.get(c.index); if (l) skus = [l.sku, ...(l.extra || []).map(x => x.sku)].filter(Boolean); } catch (_) {}
      P.integrateRings(c); result[on ? "after" : "before"] = measure(c); result.skus = skus.length ? skus : result.skus || [];
    }
    out.push({ at: s.bbox.map(v => Math.round(v * 10) / 10), ...result });
  }
  return out;
}

async function main(argv, log = console.log) {
  const o = args(argv); fs.mkdirSync(o.out, { recursive: true });
  const only = o.only ? new Set(fs.readFileSync(o.only, "utf8").split(/[\r\n,]+/).map(x => x.replace(/^[\s"\[]+|[\s"\],]+$/g, "")).filter(Boolean)) : null;
  const affected = [], metrics = {}, errors = [], summary = { stages: {}, over90: [], designs: 0 };
  for (const dir of o.stages) {
    const rec = JSON.parse(fs.readFileSync(path.join(dir, "records.json"), "utf8")), master = String(rec.masterName || path.basename(dir));
    const byFile = new Map(); for (const e of rec.entries) { const k = e.aiPath; if (!byFile.has(k)) byFile.set(k, []); byFile.get(k).push(e); }
    let list = [...byFile.entries()]; if (only) list = list.filter(([k, es]) => only.has(k) || es.some(e => only.has(e.sku))); if (o.limit) list = list.slice(0, o.limit);
    log(`${master}: ${list.length} design file(s)`); let n = 0, changed = 0;
    for (const [aiPath, entries] of list) {
      const file = path.join(dir, "files", aiPath); if (!fs.existsSync(file)) { errors.push({ aiPath, why: "file missing" }); continue; }
      let r; try { r = await design(fs.readFileSync(file), path.basename(aiPath)); } catch (e) { errors.push({ aiPath, why: e.message }); continue; }
      if (r.error) { errors.push({ aiPath, why: r.error }); continue; }
      n++; summary.designs++; if (n % 100 === 0) log(`  ${master}: ${n} read, ${changed} changed so far`);
      const skus = entries.map(e => e.sku + (e.size ? ` · ${e.size}` : ""));
      for (const sku of skus) metrics[sku] = { master, thin: r.after.darkCanvas.thin, darkCanvasBodyWithEdge: pct(r.after.darkCanvas.bodyEdge), darkStoredBody: r.after.darkStored ? pct(r.after.darkStored.body) : null, darkCanvasBody: pct(r.after.darkCanvas.body), darkStoredPicture: r.after.darkStored ? pct(r.after.darkStored.picture) : null, darkCanvasPicture: pct(r.after.darkCanvas.picture),
        before: r.changed ? { darkStoredBody: r.before.darkStored ? pct(r.before.darkStored.body) : null, darkCanvasBody: pct(r.before.darkCanvas.body) } : undefined };
      const worst = Math.max(r.after.darkStored ? r.after.darkStored.body : 0, r.after.darkCanvas.body);
      if (worst > 0.9) summary.over90.push({ skus, master, darkStoredBody: pct(r.after.darkStored ? r.after.darkStored.body : 0), darkCanvasBody: pct(r.after.darkCanvas.body), fills: r.after.fills });
      if (!r.changed) continue;
      changed++;
      const a = r.after, b = r.before, holesChanged = a.holes !== b.holes, sizeChanged = !near(a.widthPt, b.widthPt) || !near(a.heightPt, b.heightPt) || !near(a.areaPt2, b.areaPt2), pictureChanged = a.picture !== b.picture || a.storedPicture !== b.storedPicture;
      affected.push({ master, skus, aiPath, category: a.disc ? "round disc with black art: circle is the cut line, the fill is hatching" : "black fill inside the charm: hatching",
        hatchedMembers: a.hatched, holes: [b.holes, a.holes], members: [b.members, a.members], sizePt: [[+b.widthPt.toFixed(2), +b.heightPt.toFixed(2), +b.areaPt2.toFixed(1)], [+a.widthPt.toFixed(2), +a.heightPt.toFixed(2), +a.areaPt2.toFixed(1)]],
        painted: [b.fills, a.fills], darkBody: [pct(Math.max(b.darkStored ? b.darkStored.body : 0, b.darkCanvas.body)), pct(Math.max(a.darkStored ? a.darkStored.body : 0, a.darkCanvas.body))], holesChanged, sizeChanged, pictureChanged });
    }
    summary.stages[master] = { designs: n, changed };
    const name = Object.keys(o.masters).find(k => master.toUpperCase().includes(k));
    if (name && Resvg) {
      const discs = await discsOf(o.masters[name], log);
      for (const d of discs) {
        if (!d.before || !d.after) { errors.push({ master, at: d.at, why: d.error || "disc not read" }); continue; }
        const a = d.after, b = d.before; summary.stages[master].discs = (summary.stages[master].discs || 0) + 1;
        affected.push({ master, skus: d.skus, aiPath: null, category: "round disc with black art: circle is the cut line, the fill is hatching", hatchedMembers: a.hatched, holes: [b.holes, a.holes], members: [b.members, a.members],
          sizePt: [[+b.widthPt.toFixed(2), +b.heightPt.toFixed(2), +b.areaPt2.toFixed(1)], [+a.widthPt.toFixed(2), +a.heightPt.toFixed(2), +a.areaPt2.toFixed(1)]], painted: [b.fills, a.fills],
          darkBody: [pct(Math.max(b.darkStored ? b.darkStored.body : 0, b.darkCanvas.body)), pct(Math.max(a.darkStored ? a.darkStored.body : 0, a.darkCanvas.body))], holesChanged: a.holes !== b.holes, sizeChanged: !near(a.widthPt, b.widthPt) || !near(a.heightPt, b.heightPt) || !near(a.areaPt2, b.areaPt2), pictureChanged: true, masterAt: d.at });
      }
    }
  }
  const rewrite = [...new Set(affected.flatMap(x => x.skus.map(s => s.replace(/ · .*$/, ""))))].filter(Boolean).sort();
  fs.writeFileSync(path.join(o.out, "BLACKFILL-affected.json"), JSON.stringify({ rule: "a black FILL inside a charm is blue hatching (charm-nest-pdf.js, classifyBlackFills)", count: affected.length, designs: affected }, null, 1));
  fs.writeFileSync(path.join(o.out, "BLACKFILL-metrics.json"), JSON.stringify({ definition: "share of dark pixels (all channels under 80) inside the charm's own body, picture 168 px; Stored = scripts/index-master.cjs thumbnailPng, Canvas = drawCharm as the Master tab paints it", metrics }, null, 1));
  fs.writeFileSync(path.join(o.out, "BLACKFILL-rewrite-skus.json"), JSON.stringify({ skus: rewrite }, null, 1));
  summary.affected = affected.length; summary.rewriteSkus = rewrite.length; summary.errors = errors;
  fs.writeFileSync(path.join(o.out, "BLACKFILL-summary.json"), JSON.stringify(summary, null, 1));
  log(`designs ${summary.designs} · affected ${affected.length} · rewrite SKUs ${rewrite.length} · over 90 % dark: ${summary.over90.length} · errors ${errors.length}`);
  return { affected, summary };
}
module.exports = { main, load, facts, measure, darkShare, decodePng, SvgCtx, raster };
if (require.main === module) main(process.argv).catch(e => { console.error("blackfill-audit:", e.message); process.exit(1); });
