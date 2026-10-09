#!/usr/bin/env node
/*  scripts/audit-catalog.cjs — OFFLINE audit of the three master Illustrator files (nothing is read from or written to
 *  the live site, Firestore or Storage; no network, no AI).
 *  ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
 *  Why: the catalogue (Charm_Master_Index + one .ai per SKU in Storage) is built from the masters by the indexer
 *  (scripts/index-master.cjs, the Master tab, charmMaster-background). A defect in how a master is read (blue hatching
 *  read as black engraving, grey boxes, ghost text, an extra hoop circle, a detached hoop, a solid black charm) is
 *  baked into the per-SKU files of every charm that has it. This tool fingerprints every SKU of every master, flags
 *  the SKUs whose structure is inconsistent with the family conventions and writes the list the repair is run from.
 *
 *  Two steps (the parse + grouping of a 60 MB master takes 4-5 minutes, so it is done once and kept):
 *
 *    node --max-old-space-size=6000 scripts/audit-catalog.cjs dump "<MASTER.ai>" <dump.json> [--rt REGEX | --rt-all] [--rings]
 *        --rt / --rt-all also write each matching charm's per-SKU file in memory and read it back the way the app does
 *        (the record then carries `rt`: file size, what the app sees after integrateRings), so a defect the writer
 *        or the reader adds, as against one the master already has, shows up. --rt-all is slow (minutes per master).
 *        parse + group + label with the repo's own code (netlify/functions/_charmNestPdf.js: the parser the app and the
 *        indexer use) and write one compact fingerprint per charm. Run it once per master.
 *
 *        (report --fixed: mark a run on the fixed code, where what remains of a ring or label defect is for a person to look at,
 *        because a re-index cannot change it.)
 *        --rings applies the hoop weld (integrateRings) to a copy of each charm that has a ring-sized circle and records what the
 *        app sees afterwards (`rings`), so the report judges hoops on the welded charm. Use it for a run on the fixed code.
 *
 *    node scripts/audit-catalog.cjs leak "<MASTER.ai>" <dump.json>
 *        (dump already runs this) adds the paint-lost-by-the-writer check to a dump made without it, in about 30 s: cuts every
 *        non-path segment out of the page the way the per-SKU writer (isolate) does, reads the page again and compares the paint
 *        of each path. After a writer fix it must find 0.
 *
 *    node scripts/audit-catalog.cjs report <dump.json> [<dump.json> ...] --out <dir> [--findings <dir>]
 *        classify every charm, flag the inconsistent ones and write CATALOG-offending.json and CATALOG-report.md
 *        into <dir>. --findings reads the five workers' <NAME>-affected.json files (if present) to confirm categories.
 *
 *    node scripts/audit-catalog.cjs compare --old <dump.json>... --new <dump.json>... --out <dir> [--offending <CATALOG-offending.json>]
 *        two dumps of the same masters (the code the catalogue was built with, and the fixed code; both with `dump --rings`): per SKU
 *        line what changes in the size of the welded outline, the through-cuts, the members and the paint. Writes CATALOG-changes.json.
 *        An offline stand-in for step B (diff against the live site) of CATALOG-runbook.md; it cannot know what the live files hold.
 *
 *  Categories (the `defects[]` of an SKU; the exact rules are in audit-catalog-report.cjs classify(), thresholds in T):
 *    colour-lost-in-file   the per-SKU writer cuts out a text object that set the layer's colour, so blue HATCH reads back black
 *                          (found by `leak`; baked into the file, needs a re-index after the writer fix)
 *    grey-box / image-member   shading, image or grey box members: drawn as translucent boxes, carried in the per-SKU file
 *    text-member / label-text-in-charm / label-ink / text-undecodable   text or label ink inside a charm
 *    hatch-not-blue / engrave-not-red / cut-chromatic / outline-not-cut-layer   a layer and its colour disagree
 *    solid-black           the cut outline is drawn as a black fill (read-time: drawCharm shows it as a cut line)
 *    extra-ring / detached-ring / ring-orphan   hoop rings that are doubled, loose or left out of the charm
 *    sku-is-dimension / size-outlier / family-outlier / outline-noop   labelling and family conventions
 *    writer-changed-members / rings-error   (only with --rt) the per-SKU file does not read back as the master draws it
 *
 *  The thresholds are constants at the top of classify(); the report prints how many SKUs each rule flagged so a
 *  threshold that is wrong shows up as a count that is implausible.
 *  ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
"use strict";
const fs = require("fs"), path = require("path");
/** The commit of the working tree that parsed the master (so a dump says what code it came from). */
function codeCommit() { try { return require("child_process").execSync("git rev-parse --short=8 HEAD", { cwd: __dirname, stdio: ["ignore", "pipe", "ignore"] }).toString().trim(); } catch (e) { return null; } }
const root = path.join(__dirname, "..");
const MM = 25.4 / 72;
const r = (v, n) => { const k = Math.pow(10, n == null ? 2 : n); return Math.round(v * k) / k; };

/* ─────────────────────────────── step 1 · dump ─────────────────────────────── */
function polyStats(G, seg) {
  const polys = G.flatten(seg, 8); let area = 0, per = 0; const circles = []; let rect = false;
  for (const poly of polys) {
    if (poly.length < 3) continue;
    let a = 0, p = 0;
    for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { a += poly[j][0] * poly[i][1] - poly[i][0] * poly[j][1]; p += Math.hypot(poly[i][0] - poly[j][0], poly[i][1] - poly[j][1]); }
    a = Math.abs(a) / 2; area += a; per += p;
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const q of poly) { if (q[0] < x0) x0 = q[0]; if (q[0] > x1) x1 = q[0]; if (q[1] < y0) y0 = q[1]; if (q[1] > y1) y1 = q[1]; }
    const w = x1 - x0, h = y1 - y0, cx = (x0 + x1) / 2, cy = (y0 + y1) / 2;
    if (w > 0.5 && Math.abs(w - h) <= 0.05 * Math.max(w, h)) {
      const rad = (w + h) / 4; let dev = 0; for (const q of poly) dev = Math.max(dev, Math.abs(Math.hypot(q[0] - cx, q[1] - cy) - rad));
      if (dev <= 0.04 * rad) circles.push({ cx: r(cx), cy: r(cy), r: r(rad) });
    }
  }
  // an axis-aligned rectangle: one subpath, 4 or 5 straight points, every edge horizontal or vertical
  const sps = seg.subpaths || [];
  if (sps.length === 1) {
    const pts = sps[0].filter(o => o[0] === "m" || o[0] === "l").map(o => o[1]);
    if (sps[0].every(o => o[0] !== "c") && pts.length >= 4 && pts.length <= 5) {
      let ok = true; for (let i = 0; i < pts.length; i++) { const a = pts[i], b = pts[(i + 1) % pts.length]; if (Math.abs(a[0] - b[0]) > 0.05 && Math.abs(a[1] - b[1]) > 0.05) { ok = false; break; } }
      rect = ok;
    }
  }
  return { area, per, circles: circles.slice(0, 6), rect, subs: sps.length, cmds: sps.reduce((n, s) => n + s.length, 0) };
}

/** Cut out every non-path segment of the page (what the per-SKU writer does to the segments that are not a charm's own), read the
 *  page again and compare the paint of each path with the original. Returns Map(segment index → { what: [from, to] }). */
async function leakScan(P, parsed) {
  const { PDFDocument, PDFName } = require(path.join(root, "vendor/pdf-lib-1.17.1.min.js"));
  const keep = parsed.segments.filter(x => x.kind === "path").map(x => x.index);
  const out = new Map();
  const doc = await PDFDocument.load(parsed.bytes, { ignoreEncryption: true, updateMetadata: false });
  const ref = doc.context.register(doc.context.flateStream(P.isolate(parsed.content, parsed.segments, keep)));
  doc.getPage(0).node.set(PDFName.of("Contents"), doc.context.obj([ref]));
  const p2 = await P.parseSource(new Uint8Array(await doc.save({ useObjectStreams: false })), "leak.ai");
  const a = parsed.segments.filter(x => x.kind === "path"), b = p2.segments.filter(x => x.kind === "path");
  if (a.length !== b.length) { out.error = `the cut page has ${b.length} paths, the master ${a.length}: not compared`; return out; }
  const col = (on, c) => (on && c ? c.map(v => r(v, 2)).join(",") : "-");
  for (let i = 0; i < a.length; i++) {
    const x = a[i], y = b[i], d = {};
    if (x.fill !== y.fill || col(x.fill, x.fillRGB) !== col(y.fill, y.fillRGB)) d.fill = [col(x.fill, x.fillRGB), col(y.fill, y.fillRGB)];
    if (x.stroke !== y.stroke || col(x.stroke, x.strokeRGB) !== col(y.stroke, y.strokeRGB)) d.stroke = [col(x.stroke, x.strokeRGB), col(y.stroke, y.strokeRGB)];
    if (x.stroke && Math.abs((x.lwPt || 0) - (y.lwPt || 0)) > 0.02) d.lw = [r(x.lwPt || 0, 2), r(y.lwPt || 0, 2)];
    if (Object.keys(d).length) out.set(x.index, d);
  }
  return out;
}

async function dump(file, outFile, opts) {
  opts = opts || {};
  const { CharmNestPDF: P, Geom: G } = require(path.join(root, "netlify/functions/_charmNestPdf.js"));
  const buf = fs.readFileSync(file), name = path.basename(file), t0 = Date.now();
  const log = m => console.log(`[${((Date.now() - t0) / 1000).toFixed(0)}s] ${m}`);
  const parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), name);
  log(`parsed: ${parsed.segments.length} top-level segments, ${parsed.nested.length} nested`);
  // The per-SKU writer (isolate) cuts out every segment that is not the charm's own, and a segment's byte range can hold a
  // graphics-state operator that the segments after it rely on (a text object that sets the layer's fill colour: BT /CS0 cs
  // 0 0 1 scn …). Cutting it changes the colour of the paths that follow. Done here for every non-path segment at once, which
  // is the writer's effect on any charm: the paths whose paint changes, by segment index (see `leak` of each charm).
  const leak = opts.noLeak ? null : await leakScan(P, parsed);
  if (leak) log(`leak scan: ${leak.size} path segment(s) change paint when the segments around them are cut out${leak.error ? " · " + leak.error : ""}`);
  const g = P.groupCharms(parsed, { minPt: 6 });
  log(`grouped: ${g.charms.length} charm outlines, ${g.orphans.length} orphans (rule ${g.rule})`);
  const lab = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / MM, widen: 0.25 });
  log(`labelled: ${lab.labels.size} charms (${lab.skuCount} SKU lines), ${lab.unlabelled.length} unlabelled, ${lab.orphans.length} orphan labels, ${lab.duplicates.length} duplicates`);
  const kindCount = {}; for (const s of parsed.segments.concat(parsed.nested)) kindCount[s.kind] = (kindCount[s.kind] || 0) + 1;
  const layerCount = {}; for (const s of parsed.segments.concat(parsed.nested)) layerCount[s.layer || "(none)"] = (layerCount[s.layer || "(none)"] || 0) + 1;

  const member = (m, outlinePolys, ob) => {
    const o = { i: m.index != null ? m.index : null, p: m.parent != null ? m.parent : null, k: m.kind, L: m.layer || null, bb: m.bbox ? m.bbox.map(v => r(v)) : null };
    if (m.manufacturingRole) o.role = m.manufacturingRole;
    if (m.synthetic) o.syn = 1;
    if (m.kind === "path") {
      o.op = m.paintOp; o.s = m.stroke ? 1 : 0; o.f = m.fill ? 1 : 0; if (m.stroke) o.sc = m.strokeRGB.map(v => r(v, 3)); if (m.fill) o.fc = m.fillRGB.map(v => r(v, 3));
      o.lw = r(m.lwPt || 0, 2); o.cl = m.closed ? 1 : 0; o.d = m.depth || 0;
      const st = polyStats(G, m); o.A = r(st.area); o.P = r(st.per); o.ns = st.subs; o.nc = st.cmds; if (st.circles.length) o.circ = st.circles; if (st.rect) o.rect = 1;
      if (m.paintOp) o.eo = /\*$/.test(m.paintOp) ? 1 : 0;
      // where it sits against the outline
      if (outlinePolys && m.bbox && m.closed !== undefined) {
        const polys = G.flatten(m, 4), pts = polys.flat(); const step = Math.max(1, Math.ceil(pts.length / 40)); let inside = 0, n = 0, dmin = Infinity;
        for (let i = 0; i < pts.length; i += step) { n++; if (G.pointInPolys(pts[i][0], pts[i][1], outlinePolys)) inside++; const d = G.distToPolys(pts[i][0], pts[i][1], outlinePolys); if (d < dmin) dmin = d; }
        if (n) { o.in = r(inside / n); o.dO = r(dmin); }
      }
    } else if (m.kind === "text") { o.str = (m.str || "").slice(0, 60); o.chars = m.chars; if (m.fillRGB) o.fc = m.fillRGB.map(v => r(v, 3)); if (m.undecodable) o.undec = 1; }
    else if (m.kind === "xobj") { o.name = m.name; o.kids = (m.children || []).length; }
    return o;
  };

  /** What the app sees of one charm once its per-SKU file is written and read back (the app's own route: buildSingleCharm →
   *  parseSource → groupCharms → biggest charm + the loose ink inside its box → integrateRings). Returns a record like a master's. */
  const readBack = async (c) => {
    const ai = await P.buildSingleCharm(c, parsed);
    const p2 = await P.parseSource(new Uint8Array(ai), "one.ai"), g2 = P.groupCharms(p2, { minPt: 6 });
    const out = { bytes: ai.length, sha: require("crypto").createHash("sha256").update(ai).digest("hex").slice(0, 16), charms: g2.charms.length, orphans: g2.orphans.length };
    if (!g2.charms.length) return out;
    const big = g2.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
    for (const c2 of g2.charms) if (c2 !== big) for (const m of c2.members) if (!big.members.includes(m)) big.members.push(m);
    for (let grew = true; grew;) { grew = false; for (const m of g2.orphans) { if (!m.bbox || big.members.includes(m) || !big.topIndices.includes(m.parent != null ? m.parent : m.index)) continue; const pad = Math.max(big.strokePt || 0.5, m.lwPt || 0) / 2 + 1, b = big.bbox; if (m.bbox[2] < b[0] - pad || m.bbox[0] > b[2] + pad || m.bbox[3] < b[1] - pad || m.bbox[1] > b[3] + pad) continue; big.members.push(m); big.bbox = [Math.min(b[0], m.bbox[0]), Math.min(b[1], m.bbox[1]), Math.max(b[2], m.bbox[2]), Math.max(b[3], m.bbox[3])]; grew = true; } }
    const rec = (ch) => ({ outline: member(ch.outline, null, null), ob: ch.outline.bbox.map(v => r(v)), members: ch.members.filter(m => m !== ch.outline).map(m => member(m, G.flatten(ch.outline, 8), ch.outline.bbox)) });
    out.read = rec(big); out.readOrphans = g2.orphans.length;
    try { const ir = P.integrateRings(big); out.rings = { welded: ir.welded, left: ir.left }; out.final = rec(big); } catch (e) { out.rings = { error: String(e.message || e).slice(0, 200) }; }
    return out;
  };
  const rtRe = opts.rt ? new RegExp(opts.rt, "i") : null;
  const charms = [];
  for (const c of g.charms) {
    const l = lab.labels.get(c.index) || null;
    const op = c.outline ? G.flatten(c.outline, 8) : null;
    const ob = c.outline ? c.outline.bbox : null;
    const rec = {
      index: c.index, sku: c.sku || null, size: c.skuSize || null, extra: (c.extraSkus || []).map(x => x.sku + (x.size ? "·" + x.size : "")),
      label: l ? (l.str || null) : null, bbox: c.bbox.map(v => r(v)), ob: ob ? ob.map(v => r(v)) : null, layer: c.layer || null, stroke: r(c.strokePt || 0),
      outline: c.outline ? member(c.outline, null, null) : null, extras: (c.extras || []).length, merged: c.mergedInto != null ? 1 : 0,
      members: c.members.filter(m => m !== c.outline).map(m => member(m, op, ob))
    };
    if (leak && leak.size) { const hit = c.members.filter(m => m.index != null && m.parent == null && m.kind === "path" && leak.has(m.index)); if (hit.length) rec.leak = { n: hit.length, of: c.members.length, first: hit.slice(0, 4).map(m => Object.assign({ i: m.index, L: m.layer || null }, leak.get(m.index))) }; }
    // --rings: the hoop weld the app and the indexer run before they measure a charm (integrateRings), applied to a copy of the charm
    // (no file is written: the weld works on the charm's own members, the same ones the per-SKU file carries). The classifier then
    // judges the hoops on what the app sees, not on the raw drawing.
    if (opts.rings && l && P.integrateRings && rec.members.some(m => m.circ)) {
      try {
        const cc = Object.assign({}, c, { members: c.members.slice() }), ir = P.integrateRings(cc), op2 = G.flatten(cc.outline, 8);
        rec.rings = { welded: ir.welded, left: ir.left, final: { outline: member(cc.outline, null, null), ob: cc.outline.bbox.map(v => r(v)), members: cc.members.filter(m => m !== cc.outline).map(m => member(m, op2, cc.outline.bbox)) } };
      } catch (e) { rec.rings = { error: String(e.message || e).slice(0, 200) }; }
    }
    if ((opts.rtAll || rtRe) && l && (opts.rtAll || [c.sku, ...(c.extraSkus || []).map(x => x.sku)].some(x => rtRe && rtRe.test(x)))) { try { rec.rt = await readBack(c); } catch (e) { rec.rt = { error: String(e.message || e).slice(0, 200) }; } }
    charms.push(rec);
  }
  log(`members described for ${charms.length} charms${charms.some(c => c.rt) ? ` (${charms.filter(c => c.rt).length} written and read back)` : ""}`);
  // orphans (ink the grouping left out of every charm): where each sits against the nearest charm
  const polysCache = new Map(); const polysOf = c => { let p = polysCache.get(c.index); if (!p && c.outline) { p = G.flatten(c.outline, 6); polysCache.set(c.index, p); } return p; };
  const orphans = [];
  for (const m of g.orphans) {
    if (!m.bbox) continue;
    const cx = (m.bbox[0] + m.bbox[2]) / 2, cy = (m.bbox[1] + m.bbox[3]) / 2; let best = null, bd = 40;
    for (const c of g.charms) { const b = c.outline && c.outline.bbox; if (!b || cx < b[0] - 40 || cx > b[2] + 40 || cy < b[1] - 40 || cy > b[3] + 40) continue; const d = G.distToPolys(cx, cy, polysOf(c)); if (d < bd) { bd = d; best = c; } }
    const o = member(m, null, null); if (m.kind === "path") { /* shape metrics already added */ }
    o.near = best ? best.index : null; o.nearSku = best ? best.sku || null : null; o.dNear = best ? r(bd) : null;
    orphans.push(o);
  }
  log(`orphans described: ${orphans.length}`);
  const out = { master: name, bytes: buf.length, pageW: parsed.pageW, pageH: parsed.pageH, at: new Date().toISOString(), code: codeCommit(), rule: g.rule, kindCount, layerCount,
    lab: { labelled: lab.labels.size, skuCount: lab.skuCount, unlabelled: lab.unlabelled, orphans: lab.orphans.map(x => ({ sku: x.sku, size: x.size, str: x.str, bbox: x.bbox && x.bbox.map(v => r(v)) })), duplicates: lab.duplicates, undecodable: lab.undecodable.length },
    charms, orphans };
  fs.writeFileSync(outFile, JSON.stringify(out));
  log(`dump written: ${outFile} (${(fs.statSync(outFile).size / 1048576).toFixed(1)} MB)`);
}

/** Add the leak scan to a dump made without it (a 30 s parse of the master instead of the 5 minute grouping): every charm gets
 *  `leak` = { n, of, first[] } when members of it lose their paint in the per-SKU file, and the dump gets `leakTotal`. */
async function addLeak(file, dumpFile) {
  const { CharmNestPDF: P } = require(path.join(root, "netlify/functions/_charmNestPdf.js"));
  const buf = fs.readFileSync(file), t0 = Date.now();
  const parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), path.basename(file));
  const leak = await leakScan(P, parsed), d = JSON.parse(fs.readFileSync(dumpFile, "utf8"));
  if (d.master !== path.basename(file)) throw new Error(`the dump is of ${d.master}, not ${path.basename(file)}`);
  let hit = 0;
  for (const c of d.charms) {
    delete c.leak;
    const all = (c.outline ? [c.outline] : []).concat(c.members), ms = all.filter(m => m.i != null && m.p == null && m.k === "path" && leak.has(m.i));
    if (ms.length) { hit++; c.leak = { n: ms.length, of: all.length, first: ms.slice(0, 4).map(m => Object.assign({ i: m.i, L: m.L }, leak.get(m.i))) }; }
  }
  d.leakTotal = { paths: leak.size, charms: hit, error: leak.error || null, at: new Date().toISOString() };
  fs.writeFileSync(dumpFile, JSON.stringify(d));
  console.log(`${path.basename(file)}: ${leak.size} path(s) change paint in the per-SKU file · ${hit} charm(s) hold some · ${((Date.now() - t0) / 1000).toFixed(0)} s`);
  return d.leakTotal;
}

/** A fingerprint of one per-SKU .ai (or any small .ai) that does not depend on where on its page the charm sits: every drawn
 *  segment as kind, layer, paint, colours, line width and a box relative to the drawing's own corner, sorted and hashed. Two files
 *  with the same print draw the same thing; the differences list says what changed (colour, members added or dropped, shapes moved). */
async function filePrint(bytes) {
  const { CharmNestPDF: P } = require(path.join(root, "netlify/functions/_charmNestPdf.js"));
  const parsed = await P.parseSource(new Uint8Array(bytes), "one.ai");
  const pageArea = parsed.pageW * parsed.pageH;
  const segs = parsed.segments.concat(parsed.nested).filter(x => x.bbox && x.kind !== "clip" && x.kind !== "noop" && !(x.kind === "xobj" && x.children && x.children.length)
    && !(x.kind === "path" && (x.bbox[2] - x.bbox[0]) * (x.bbox[3] - x.bbox[1]) >= 0.8 * pageArea));          // (the artboard frame is not the charm)
  let x0 = Infinity, y0 = Infinity; for (const x of segs) { if (x.bbox[0] < x0) x0 = x.bbox[0]; if (x.bbox[1] < y0) y0 = x.bbox[1]; }
  const col = c => c ? c.map(v => r(v, 2)).join(",") : "";
  const items = segs.map(x => [x.kind, x.layer || "", x.paintOp || "", x.stroke ? col(x.strokeRGB) : "", x.fill ? col(x.fillRGB) : "", x.kind === "path" ? r(x.lwPt || 0, 2) : "", r(x.bbox[0] - x0, 1), r(x.bbox[1] - y0, 1), r(x.bbox[2] - x0, 1), r(x.bbox[3] - y0, 1), x.kind === "text" ? (x.str || "") : ""].join("|")).sort();
  const hash = require("crypto").createHash("sha256").update(items.join("\n")).digest("hex").slice(0, 16);
  return { n: items.length, hash, items, page: [r(parsed.pageW, 1), r(parsed.pageH, 1)] };
}
/** What differs between two prints, in words: [] when they are the same drawing. */
function printDiff(a, b) {
  if (a.hash === b.hash) return [];
  const why = [], count = arr => { const m = new Map(); for (const k of arr) m.set(k, (m.get(k) || 0) + 1); return m; };
  const ca = count(a.items), cb = count(b.items); let only = [], gone = [];
  for (const [k, v] of ca) { const w = cb.get(k) || 0; for (let i = 0; i < v - w; i++) only.push(k); }
  for (const [k, v] of cb) { const w = ca.get(k) || 0; for (let i = 0; i < v - w; i++) gone.push(k); }
  const f = k => k.split("|"); const kinds = l => [...new Set(l.map(k => f(k)[0]))];
  if (a.n !== b.n) why.push(`${a.n} drawn parts vs ${b.n}`);
  const colourOnly = only.length && only.length === gone.length && only.every(k => gone.some(g => f(g).slice(0, 2).concat(f(g).slice(6, 10)).join("|") === f(k).slice(0, 2).concat(f(k).slice(6, 10)).join("|")));
  if (colourOnly) why.push("same shapes, different paint or line width");
  else { if (only.length) why.push(`${only.length} part(s) only in the first (${kinds(only).join("/")})`); if (gone.length) why.push(`${gone.length} part(s) only in the second (${kinds(gone).join("/")})`); }
  return why.length ? why : ["different"];
}

module.exports = { dump, polyStats, filePrint, printDiff, leakScan, addLeak };

if (require.main === module) {
  const [cmd, ...rest] = process.argv.slice(2);
  if (cmd === "dump") {
    if (rest.length < 2) { console.error('usage: node scripts/audit-catalog.cjs dump "<MASTER.ai>" <dump.json>'); process.exit(2); }
    const opts = {}; for (let i = 2; i < rest.length; i++) { if (rest[i] === "--rt") opts.rt = rest[++i]; else if (rest[i] === "--rt-all") opts.rtAll = true; else if (rest[i] === "--no-leak") opts.noLeak = true; else if (rest[i] === "--rings") opts.rings = true; }
    dump(rest[0], rest[1], opts).catch(e => { console.error("audit-catalog dump:", e.stack || e.message); process.exit(1); });
  } else if (cmd === "leak") {
    if (rest.length < 2) { console.error('usage: node scripts/audit-catalog.cjs leak "<MASTER.ai>" <dump.json>'); process.exit(2); }
    addLeak(rest[0], rest[1]).catch(e => { console.error("audit-catalog leak:", e.stack || e.message); process.exit(1); });
  } else if (cmd === "report") {
    require("./audit-catalog-report.cjs").main(rest);
  } else if (cmd === "compare") {
    require("./audit-catalog-report.cjs").compare(rest);
  } else { console.error("usage: audit-catalog.cjs dump <master.ai> <dump.json> | report <dump.json>... --out <dir> | compare --old <dump>... --new <dump>... --out <dir>"); process.exit(2); }
}
