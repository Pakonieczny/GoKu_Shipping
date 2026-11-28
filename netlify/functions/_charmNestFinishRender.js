/*  netlify/functions/_charmNestFinishRender.js — the sheet writer's two pictures of a written sheet, drawn in node (cloud plan v2 build step 7a; designs.json "Finishing a
 *  sheet", gaps 1 and 3). Nothing here reads or writes the cloud.
 *
 *  renderCheck(aiBytes, spec)   the page's render check (charm-nest-pdf.js verifyRendered, :2894) with node as the rasteriser in place of pdf.js: the written .ai is read back
 *                               (CharmNestPDF.parseSource), each charm's layer (its optional-content group: the form the sheet draws it with, and everything in it) is drawn
 *                               ALONE, cropped to its own box, on the node canvas (_charmNestCanvas.js: resvg, or its own JS rasteriser), on white; then exactly the page's
 *                               counting: the ink (r+g+b < 720) and its box grown by a pixel, the flood from that box's border, the erosion by the negative clearance (the
 *                               same two-run square test), the core; then overlap, outside the inset, in a corner keep-out and empty layers, with the page's answer shape
 *                               ({ ok, overlapPx, outsidePx, cornerPx, cornerLayers, emptyLayers, overlappingPairs, overlapDetail, layers, rendered, res, erodePx } and the
 *                               hidden areas / page the overlap fixer plans on). spec as the page's: { wPt, hPt, insetPt, res, erodePt, corners, layerSigs, cache }.
 *                               What it draws as pdf.js does: every path with its own fill and stroke colour, fill rule and width (at least one device pixel, as pdf.js
 *                               draws a hairline), in content order, so a white knock-out still clears the ink under it. What it draws more coarsely: live text, an image
 *                               and a shading are drawn as their box (more ink, never less: a check that errs flags, it never passes what pdf.js would flag); line caps and
 *                               joins are the PDF defaults. The exact cut-line check (finish-core exactOverlaps) stays the authority on overlap, as on the page.
 *  previewPng(sh, st)           the page's previewPng / paintPreview(clean) (charm-nest-1.html :6941, :7906): the sheet alone, 1026 px wide, every placed charm drawn by
 *                               CharmNestPDF.drawCharm where it lies, with its saved FRONT engraving on it (FRONTPIC: sh.backPool, as the .ai has it); a Rose Gold / 10K / 14K sheet
 *                               on its stock's history (RGHISTORY: the grey cut-away, earlier cuts' grey pieces and green lines, as the page's RoseStock.paint drew them). → a PNG Buffer.
 *  previewFonts(pool)           (FRONTPIC) the fonts those front engravings are drawn in, read before the picture (and the script fonts their words need). Never throws.               */
"use strict";
const { CharmNestPDF: P, Geom, withCanvas } = require("./_charmNestPdf");
const Canvas = require("./_charmNestCanvas");

const canvas = (w, h) => { withCanvas({ backend: process.env.CHARM_NEST_CANVAS || "auto" }); return new Canvas.NodeCanvas(w, h); };
const css = c => `rgb(${Math.round(((c && c[0]) || 0) * 255)},${Math.round(((c && c[1]) || 0) * 255)},${Math.round(((c && c[2]) || 0) * 255)})`;
/** Flood from the border over `open` cells (1 = passable): charm-nest-pdf.js floodFromBorder, the same cells reached. */
function floodFromBorder(open, w, h) {
  const seen = new Uint8Array(w * h), stack = new Int32Array(w * h);
  let sp = 0;
  const push = (i) => { if (!seen[i] && open[i]) { seen[i] = 1; stack[sp++] = i; } };
  for (let x = 0; x < w; x++) { push(x); push((h - 1) * w + x); }
  for (let y = 0; y < h; y++) { push(y * w); push(y * w + w - 1); }
  while (sp) { const i = stack[--sp]; const x = i % w, y = (i - x) / w; if (x > 0) push(i - 1); if (x < w - 1) push(i + 1); if (y > 0) push(i - w); if (y < h - 1) push(i + w); }
  return seen;
}
const bbUnion = (a, b) => (!a ? (b ? b.slice() : null) : !b ? a : [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);
/** The segments each optional-content layer of the written sheet draws (a charm is one form the sheet draws inside its layer; the form's own inner layers are its own). → Map name → segs, in content order. */
function layersOf(parsed) {
  const by = new Map(), add = (name, seg) => { if (!by.has(name)) by.set(name, []); by.get(name).push(seg); };
  for (const s of parsed.segments || []) {
    const L = s.layer; if (!L) continue;
    if (s.kind === "xobj") { for (const k of s.children || []) if (k.kind !== "xobj") add(L, k); }
    else add(L, s);
  }
  return by;
}
/** One layer alone, cropped to its box (page pixels, y down), on white: the RGBA pixels of the crop and where it lies. */
function drawLayer(segs, page, res, W, H) {
  let bb = null, lw = 0;
  for (const s of segs) { if (s.kind === "clip" || s.kind === "noop" || !s.bbox) continue; bb = bbUnion(bb, s.bbox); if (s.kind === "path" && s.stroke) lw = Math.max(lw, +s.lwPt || 0); }
  if (!bb) return null;
  const pad = 3 + Math.ceil(Math.max(lw * res, 1) / 2);
  const X0 = Math.max(0, Math.floor((bb[0] - page.x0) * res) - pad), X1 = Math.min(W, Math.ceil((bb[2] - page.x0) * res) + pad);
  const Y0 = Math.max(0, Math.floor((page.y1 - bb[3]) * res) - pad), Y1 = Math.min(H, Math.ceil((page.y1 - bb[1]) * res) + pad);
  const cw = X1 - X0, ch = Y1 - Y0; if (cw <= 0 || ch <= 0) return null;
  const cv = canvas(cw, ch), ctx = cv.getContext("2d");
  ctx.fillStyle = "#ffffff"; ctx.fillRect(0, 0, cw, ch);
  const tx = (x, y) => [(x - page.x0) * res - X0, (page.y1 - y) * res - Y0];
  for (const s of segs) {
    if (s.kind === "path") {
      if (!s.fill && !s.stroke) continue;
      ctx.beginPath(); P.pathToCanvas(ctx, s, tx);
      if (s.fill) { ctx.fillStyle = css(s.fillRGB); ctx.fill(String(s.paintOp || "").endsWith("*") ? "evenodd" : "nonzero"); }
      if (s.stroke) { ctx.strokeStyle = css(s.strokeRGB); ctx.lineWidth = Math.max(1, (+s.lwPt || 0) * res); ctx.lineJoin = "miter"; ctx.lineCap = "butt"; ctx.stroke(); }
    } else if ((s.kind === "text" || s.kind === "image" || s.kind === "shading") && s.bbox && !s.outlined) {
      const a = tx(s.bbox[0], s.bbox[3]), b = tx(s.bbox[2], s.bbox[1]);
      ctx.fillStyle = css(s.fillRGB || [0.2, 0.2, 0.2]); ctx.fillRect(a[0], a[1], Math.max(1, b[0] - a[0]), Math.max(1, b[1] - a[1]));
    }
  }
  return { X0, Y0, w: cw, h: ch, img: ctx.getImageData(0, 0, cw, ch).data };
}
/** What one layer covers (the page's covered(), on a crop at X0, Y0 of the page): { X0, Y0, bw, bh, core } in page pixels, or { empty }. */
function coveredOf(d, erodePx) {
  if (!d) return { empty: true };
  const { img, w: W, h: H } = d;
  let ink = 0, bx0 = W, by0 = H, bx1 = -1, by1 = -1;
  for (let y = 0, j = 0; y < H; y++) for (let x = 0; x < W; x++, j += 4) {
    if (img[j] + img[j + 1] + img[j + 2] >= 720) continue;
    ink++; if (x < bx0) bx0 = x; if (x > bx1) bx1 = x; if (y < by0) by0 = y; if (y > by1) by1 = y;
  }
  if (!ink) return { empty: true };
  const X0 = Math.max(0, bx0 - 1), Y0 = Math.max(0, by0 - 1), X1 = Math.min(W - 1, bx1 + 1), Y1 = Math.min(H - 1, by1 + 1), bw = X1 - X0 + 1, bh = Y1 - Y0 + 1;
  const open = new Uint8Array(bw * bh);
  for (let y = 0, b = 0; y < bh; y++) for (let x = 0, j = ((Y0 + y) * W + X0) * 4; x < bw; x++, b++, j += 4) if (img[j] + img[j + 1] + img[j + 2] >= 720) open[b] = 1;
  const reached = floodFromBorder(open, bw, bh);
  let solid = reached;
  if (erodePx) {
    const span = 2 * erodePx + 1, rowOk = new Uint8Array(bw * bh), colRun = new Int32Array(bw);
    for (let y = 0; y < bh; y++) { const row = y * bw; let run = 0; for (let x = 0; x < bw; x++) { run = reached[row + x] ? 0 : run + 1; if (run >= span) rowOk[row + x - erodePx] = 1; } }
    solid = new Uint8Array(bw * bh).fill(1);
    for (let y = 0; y < bh; y++) { const row = y * bw; for (let x = 0; x < bw; x++) { const run = colRun[x] = rowOk[row + x] ? colRun[x] + 1 : 0; if (run >= span) solid[row - erodePx * bw + x] = 0; } }
  }
  const core = new Uint8Array(bw * bh);
  for (let b = 0; b < core.length; b++) if (!reached[b] && !solid[b]) core[b] = 1;
  return { X0: X0 + d.X0, Y0: Y0 + d.Y0, bw, bh, core };
}
/** The render check of a written sheet (see the head of this file). spec { wPt, hPt, insetPt, res, erodePt, corners, layerSigs (Map), cache (Map), onProgress?, pace? } */
async function renderCheck(bytes, spec) {
  const res = spec.res || 6, parsed = await P.parseSource(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes), "sheet.ai");
  const mb = parsed.mediaBox || [0, 0, spec.wPt, spec.hPt], page = { x0: mb[0], y1: mb[3] };
  const W = Math.ceil((mb[2] - mb[0]) * res), H = Math.ceil((mb[3] - mb[1]) * res);
  const by = layersOf(parsed), charmLayers = [...by.keys()].filter(n => !/^SHEET|^LABELS/.test(n)).map(name => ({ name }));
  const idGrid = new Int16Array(W * H).fill(-1);
  const inset = Math.floor((spec.insetPt || 0) * res * 0.999);
  const erodePx = spec.erodePt > 0 ? Math.ceil(spec.erodePt * res) + 1 : 0;
  const sigs = spec.layerSigs instanceof Map ? spec.layerSigs : null, cache = spec.cache instanceof Map ? spec.cache : null;
  const keyOf = name => { const sig = sigs && cache ? sigs.get(name) : null; return sig ? [sig, W, H, res, inset, erodePx].join("|") : null; };
  const used = new Set(); let reused = 0;
  let overlapPx = 0, outsidePx = 0, cornerPx = 0; const pairs = new Set(), empty = [], detail = {}, byName = new Map(), cornerLayers = new Set();
  const tris = Array.isArray(spec.corners) ? spec.corners.filter(t => t && t.c > 0) : [];
  const inCorner = (X, Y) => { for (const t of tris) { const u = ((t.sx > 0 ? X : X + 1) / res - t.x) * t.sx, v = ((t.sy > 0 ? Y : Y + 1) / res - t.y) * t.sy; if (u >= 0 && v >= 0 && u + v < t.c - 1e-9) return true; } return false; };
  for (let li = 0; li < charmLayers.length; li++) {
    const name = charmLayers[li].name, key = keyOf(name);
    let area = key ? cache.get(key) : null;
    if (area) reused++;
    else { area = coveredOf(drawLayer(by.get(name), page, res, W, H), erodePx); if (key) cache.set(key, area); if (spec.pace) await spec.pace(); }
    if (key) used.add(key);
    if (area.empty) { empty.push(name); if (spec.onProgress) spec.onProgress(li + 1, charmLayers.length); continue; }
    byName.set(name, area);
    const { X0, Y0, bw, bh, core } = area;
    for (let y = 0, b = 0; y < bh; y++) for (let x = 0; x < bw; x++, b++) {
      if (!core[b]) continue;
      const X = X0 + x, Y = Y0 + y; if (X < 0 || Y < 0 || X >= W || Y >= H) { outsidePx++; continue; }
      const i = Y * W + X;
      if (X < inset || Y < inset || X >= W - inset || Y >= H - inset) outsidePx++;
      if (tris.length && inCorner(X, Y)) { cornerPx++; cornerLayers.add(name); }
      if (idGrid[i] >= 0) { overlapPx++; const pair = charmLayers[idGrid[i]].name + " ↔ " + name; pairs.add(pair); const d = detail[pair] || (detail[pair] = { px: 0, x0: 1e9, y0: 1e9, x1: -1, y1: -1 }); d.px++; if (X < d.x0) d.x0 = X; if (Y < d.y0) d.y0 = Y; if (X > d.x1) d.x1 = X; if (Y > d.y1) d.y1 = Y; }
      idGrid[i] = li;
    }
    if (spec.onProgress) spec.onProgress(li + 1, charmLayers.length);
  }
  if (cache) for (const k of [...cache.keys()]) if (!used.has(k)) cache.delete(k);
  const overlapDetail = Object.fromEntries(Object.entries(detail).map(([k, d]) => [k, { px: d.px, boxPt: [d.x0, d.y0, d.x1, d.y1].map(v => +(v / res).toFixed(1)) }]));
  const out = { ok: overlapPx === 0 && outsidePx === 0 && cornerPx === 0 && empty.length === 0, overlapPx, outsidePx, cornerPx, cornerLayers: [...cornerLayers], emptyLayers: empty, overlappingPairs: [...pairs], overlapDetail, layers: charmLayers.length, rendered: charmLayers.length - reused, res, erodePx, rasteriser: "node" };
  Object.defineProperty(out, "areas", { value: byName, enumerable: false }); Object.defineProperty(out, "page", { value: { W, H, inset }, enumerable: false });
  return out;
}

/** An area as text and back, for the work folder (the core packed eight cells a byte). */
function packArea(a) {
  if (!a || a.empty) return { empty: true };
  const n = a.core.length, bytes = new Uint8Array((n + 7) >> 3); for (let i = 0; i < n; i++) if (a.core[i]) bytes[i >> 3] |= 1 << (i & 7);
  return { X0: a.X0, Y0: a.Y0, bw: a.bw, bh: a.bh, core: Buffer.from(bytes).toString("base64") };
}
function unpackArea(o) {
  if (!o || o.empty) return { empty: true };
  const n = o.bw * o.bh, bytes = Buffer.from(String(o.core || ""), "base64"), core = new Uint8Array(n); for (let i = 0; i < n; i++) core[i] = (bytes[i >> 3] >> (i & 7)) & 1;
  return { X0: o.X0, Y0: o.Y0, bw: o.bw, bh: o.bh, core };
}

/** The sheet's preview picture (the page's previewPng with paintPreview(clean)): 1026 px wide, the sheet alone. → PNG Buffer, or null. */
/* RGHISTORY (Paul, 22 Nov 2026: "all RG sheets in Library are not displaying the Greyed out charms prior to the Green cut line"): a Rose Gold / 10K / 14K sheet is drawn on its physical
   sheet as the page drew it (charm-nest-rose-ui.js RoseStock.paint, 'history' then 'lines'): the stock already cut away in light grey, every earlier cut's pieces in grey, every cut's
   green line; a sheet whose own line is cut shows grey only what was used BEFORE its cut, its own pieces as they are. hist: { stock: { profileJson }, cuts: [{ sheetId, revision, setAsideAt,
   planJson }] } (the stock's record and its cuts, newest first), null for a sheet of no such stock. */
const parseJ = v => { if (!v) return null; if (typeof v === "object") return v; try { return JSON.parse(v); } catch (e) { return null; } };
function roseCurrent(cuts) { const seen = new Set(); return (cuts || []).filter(c => c && !c.setAsideAt).map(c => Object.assign({}, c, { plan: c.plan || parseJ(c.planJson) })).sort((x, y) => (+y.revision || 0) - (+x.revision || 0)).filter(c => { const k = c.sheetId; if (!k) return true; if (seen.has(k)) return false; seen.add(k); return true; }); }
function roseStroke(ctx, paths, k, color, width, dashed) { ctx.strokeStyle = color; ctx.lineWidth = width; ctx.setLineDash(dashed ? [4, 3] : []); for (const path of paths || []) { ctx.beginPath(); path.forEach(([x, y], i) => (i ? ctx.lineTo(x * k, y * k) : ctx.moveTo(x * k, y * k))); ctx.stroke(); } ctx.setLineDash([]); }
function roseCutsOf(sh, hist) {
  if (!hist) return { cuts: [], own: null, profile: null };
  let cuts = roseCurrent(hist.cuts);
  const ownAny = sh.roseCutAt ? cuts.find(c => c.sheetId === (sh.id || sh.sheetId)) : null;
  if (ownAny) cuts = cuts.filter(c => (+c.revision || 0) <= (+ownAny.revision || 0));
  const own = sh.roseCutAt && !(+sh.laserDoneAt > 0) ? ownAny : null;
  const prior = own ? cuts.filter(c => c !== own && (+c.revision || 0) < (+own.revision || 0)).sort((a, b) => (+b.revision || 0) - (+a.revision || 0))[0] : null;
  const profile = own ? parseJ(prior && prior.plan && prior.plan.profile) : parseJ(hist.stock && hist.stock.profileJson);
  return { cuts, own, profile };
}
function roseHistory(ctx, sh, hist, k) {
  const { cuts, own, profile } = roseCutsOf(sh, hist);
  if (profile && Array.isArray(profile.values)) { ctx.fillStyle = "#f0eeeb"; profile.values.forEach((v, i) => { if (profile.axis === "x") ctx.fillRect(0, i * profile.step * k, v * k, profile.step * k); else ctx.fillRect(i * profile.step * k, 0, profile.step * k, v * k); }); }
  for (const c of cuts) if (c !== own) for (const shape of (c.plan && c.plan.shapes) || []) {
    ctx.fillStyle = "#d8d5d0"; for (const path of shape.paths || []) { ctx.beginPath(); path.forEach(([x, y], i) => (i ? ctx.lineTo(x * k, y * k) : ctx.moveTo(x * k, y * k))); ctx.closePath(); ctx.fill(); }
    roseStroke(ctx, shape.paths, k, "#b9b4ad", 1, false);
  }
}
function roseLines(ctx, sh, hist, k) {
  const { cuts } = roseCutsOf(sh, hist);
  for (const c of cuts) roseStroke(ctx, c.plan && c.plan.lines, k, "#249c8a", Math.max(2, 0.2 * k), false);
  if (!sh.roseCutAt && sh.rosePlanJson) { const pl = parseJ(sh.rosePlanJson); if (pl) roseStroke(ctx, pl.lines, k, "#008974", Math.max(2.5, 0.2 * k), true); }
}
/* FRONTPIC (Paul, 28 Nov 2026: every picture of a sheet must show exactly what the downloaded .ai shows; the Library picture of GF_Nov.24.26_Set-1_Sheet-1 had no "Waldo" on
   4244028423_5307575879_1 (SKINNY_0191), GF_Nov.23.26_Set-1_Sheet-2 no "MSW", both in the sheet window and in the .ai). The .ai puts each SAVED front engraving ON its charm
   (charm-nest-export-ui.js build, charm-nest-export.js planFronts); a back goes above the sheet, never on it. This picture drew the charms alone. Each placed charm's saved front is now
   drawn on it as the sheet window draws it (charm-nest-sheetwin.js paintBase: CharmNestBacks.engraveOn, then drawEngrave in CharmNestBacks.INK): a record of sh.backPool with face
   "front", not invalidated, not pending, its file saved (outputs.ai or ai), the newest approval per piece; an artOnly front's parts are already in the charm's drawing. Its font is the
   window's backFont: the record's own (CharmNestOrders.fontOfRecord: its fontKey, else its font's name), Semibold when its weight says so and the font has one (_charmEngraveFonts.js).
   A font that cannot be read leaves those words out: the picture is still made, and why is said once. The page's picture (charm-nest-1.html paintPreview, clean) draws them the same way. */
const savedFront = r => !!r && r.face === "front" && !r.invalidated && !r.pending && !r.artOnly && !!((r.outputs && r.outputs.ai) || r.ai);
/** The saved front engraving of each piece, the newest approval: Map poolId → record. */
function savedFronts(pool) {
  const by = new Map();
  for (const r of Array.isArray(pool) ? pool : []) { if (!savedFront(r) || r.poolId == null) continue; const k = String(r.poolId), was = by.get(k); if (!was || (+r.approvedAt || 0) >= (+was.approvedAt || 0)) by.set(k, r); }
  return by;
}
const said = new Set();
const sayOnce = why => { if (said.has(why)) return; said.add(why); console.warn("[sheet picture] " + why); };
let Fonts = null;
/** The font a saved front is drawn in (the window's backFont); null, said once, when it cannot be read. */
function frontFont(rec) {
  try {
    const F = Fonts || (Fonts = require("./_charmEngraveFonts")), O = require("../../charm-nest-orders.js");
    const id = O.fontOfRecord(rec) || O.DEFAULT_FONT, set = F.setOf(id);
    if (!set) { sayOnce(`the engraving font ${(O.fontById(id) || {}).name || id} could not be read: front engravings in it are left out of the sheet pictures`); return null; }
    return (rec.weight === "Semibold" && set.Semibold) || set.Regular || null;
  } catch (e) { sayOnce(`the engraving fonts could not be read (${(e && e.message) || e}): front engravings are left out of the sheet pictures`); return null; }
}
/** One saved front's words on its charm: the context at the charm's place, tx the charm's own points → the picture (the window's tx0). → true when drawn. */
function drawFront(ctx, rec, c, tx) {
  const font = frontFont(rec); if (!font) return false;
  try {
    const Backs = require("../../charm-nest-backs.js"), geo = Backs.engraveOn(rec, c, { font, geom: Geom }); if (!geo) return false;
    if (!geo.glyphs) { sayOnce(`the words of the front engraving of ${rec.poolId} could not be laid out: they are left out of its sheet's picture`); return false; }
    return Backs.drawEngrave(ctx, geo, tx, { fill: Backs.INK });
  } catch (e) { sayOnce(`a front engraving could not be drawn on its charm (${(e && e.message) || e})`); return false; }
}
/** Before the picture: the fonts of the saved fronts read, and the script fonts their words need (as the page's ensureScriptFonts). pool: a backPool, or a sheet holding one. Never throws. */
async function previewFonts(pool) {
  for (const rec of savedFronts(Array.isArray(pool) ? pool : pool && pool.backPool).values()) {
    const font = frontFont(rec); if (!font) continue;
    try { await Fonts.ensureScripts((Array.isArray(rec.lines) ? rec.lines : String(rec.text == null ? "" : rec.text).split(/\r?\n/)).join("\n"), font); }
    catch (e) { sayOnce(`the script fonts of a front engraving could not be read (${(e && e.message) || e})`); }
  }
}
function previewPng(sh, st, hist) {
  try {
    const Wc = 1026, Hc = Math.max(8, Math.round(1026 * st.hPt / st.wPt)), cv = canvas(Wc, Hc), ctx = cv.getContext("2d"), k = Wc / st.wPt;
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.fillStyle = "#fffefb"; ctx.fillRect(0, 0, Wc, Hc);
    if (hist) { try { roseHistory(ctx, sh, hist, k); } catch (e) { /* the sheet is drawn without its history */ } }   // (RGHISTORY)
    ctx.strokeStyle = "rgba(176,86,63,.9)"; ctx.lineWidth = Math.max(1, 0.5 * k); ctx.strokeRect(0.5, 0.5, Wc - 1, Hc - 1);
    const byId = new Map((sh.charms || []).map(c => [c.id, c])), fronts = savedFronts(sh.backPool);
    for (const p of sh.roseCutAt && +sh.laserDoneAt > 0 ? [] : sh.placements || []) {
      const c = byId.get(p.id); if (!c || !c.centerPt) continue;
      ctx.save(); ctx.translate(p.cxPt * k, p.cyPt * k); ctx.rotate((+p.angle || 0) * Math.PI / 180); if (p.scale) ctx.scale(p.scale, p.scale);
      const cx = c.centerPt[0], cy = c.centerPt[1], tx = (x, y) => [(x - cx) * k, (cy - y) * k];
      P.drawCharm(ctx, c, tx, k);
      if (c.poolId != null && fronts.has(String(c.poolId))) drawFront(ctx, fronts.get(String(c.poolId)), c, tx);   // (FRONTPIC)
      if (sh.shippedOut && c.poolId && sh.shippedOut[c.poolId]) P.shippedMark(ctx, c, tx, Math.max(2.2, 0.9 * k) / (p.scale || 1));   // (SHIPPEDSTAYS)
      ctx.restore();
    }
    if (hist) { try { ctx.setTransform(1, 0, 0, 1, 0, 0); roseLines(ctx, sh, hist, k); } catch (e) { /* drawn without its lines */ } }   // (RGHISTORY)
    return cv.toBuffer();
  } catch (e) { return null; }
}

module.exports = { renderCheck, previewPng, previewFonts, savedFronts, roseCutsOf, layersOf, coveredOf, drawLayer, floodFromBorder, packArea, unpackArea };
