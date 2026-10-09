/*  scripts/audit-catalog-report.cjs — the classifier of scripts/audit-catalog.cjs (step 2: dump files → defects).
 *  Offline: reads only the dump JSON files that `audit-catalog.cjs dump` wrote. See audit-catalog.cjs for the categories.
 *
 *    node scripts/audit-catalog.cjs report <LIVING.json> <OBJECTS.json> <CUSTOMS-MISC.json> --out <dir> [--findings <dir>]
 *
 *  Writes <dir>/CATALOG-offending.json (one row per SKU: sku, name, master, defects[], detail{}, and the other SKUs that share
 *  its design file, which are rewritten with it) and <dir>/CATALOG-report.md (counts per category, Paul's examples, and the
 *  cross-check against the five workers' <NAME>-affected.json files when --findings is given).                            */
"use strict";
const fs = require("fs"), path = require("path");
const MM = 25.4 / 72;

/* ───────── thresholds (every rule's count is printed in the report, so a threshold that is wrong shows up) ───────── */
const T = {
  blackMax: 0.2, whiteMin: 0.9, achroSat: 0.15,
  ringRmin: 1.2, ringRmax: 6,            // pt: a hoop circle (inner hole 1.84, outer 2.9 to 4.7)
  concentricTol: 0.35,                    // centres within this fraction of the smaller radius are one hoop
  sameRadiusTol: 0.06,
  detachedD: 0.6,                         // pt: a ring this far from the outline's edge (and outside it) is loose
  solidFill: 0.5,                         // a black-filled CUT member covering this share of the outline area = solid black body
  famMin: 6, famShare: 0.8,               // a family needs 6 members; a feature held by 80 % is its convention
  sizeMaxMm: 60, sizeMinMm: 3,
  rareShare: 0.004, rareMin: 3            // a (layer, paint, colour) combination under 0.4 % of a master's artwork members
};

const cls = c => {
  if (!c) return "none"; const mx = Math.max(c[0], c[1], c[2]), mn = Math.min(c[0], c[1], c[2]);
  if (mx - mn <= T.achroSat) return mx < T.blackMax ? "black" : mx > T.whiteMin ? "white" : "grey";
  if (c[1] >= 0.5 && c[2] >= 0.5 && c[0] < 0.5) return "cyan";
  if (c[2] >= 0.6 && c[0] < 0.45 && c[1] < 0.55) return "blue";
  if (c[0] >= 0.6 && c[1] < 0.45 && c[2] < 0.45) return "red";
  if (c[1] >= 0.5 && c[0] < 0.5 && c[2] < 0.5) return "green";
  return "other";
};
const layerKey = L => (L == null ? "(none)" : /^cut/i.test(L) ? "CUT" : /^engrav/i.test(L) ? "ENGRAVE" : /^hatch/i.test(L) ? "HATCH" : /^labels?$/i.test(L) ? "LABELS" : /^back engrav/i.test(L) ? "BACK" : "OTHER:" + L);
const paintKey = m => (m.f ? "F" : "") + (m.s ? "S" : "");
const colOf = m => (m.f ? cls(m.fc) : m.s ? cls(m.sc) : "none");
const isRingSize = r => r >= T.ringRmin && r <= T.ringRmax;
const familyOf = sku => String(sku || "").toUpperCase().replace(/[-_ ]?\d+$/, "").replace(/\s*\(.*\)\s*$/, "").trim();

/** Fingerprint + defects of one charm record (a `charms[]` row of a dump, or a read-back `rt.read` / `rt.final`). */
function fingerprint(c) {
  const out = c.outline || null, mem = c.members || [], all = (out ? [out] : []).concat(mem);
  const fp = { layers: {}, kinds: {}, art: {}, text: [], shading: 0, images: 0, circles: [], outlineFill: false, outlineLayer: out ? layerKey(out.L) : null, outlineOp: out ? out.op || null : null, outlineA: out ? out.A || 0 : 0, nArt: 0, nHatch: 0, nEngrave: 0 };
  for (const m of all) {
    const lk = layerKey(m.L); fp.layers[lk] = (fp.layers[lk] || 0) + 1; fp.kinds[m.k] = (fp.kinds[m.k] || 0) + 1;
    if (m.k === "path") {
      const key = lk + ":" + paintKey(m) + ":" + colOf(m); fp.art[key] = (fp.art[key] || 0) + 1;
      if (lk === "HATCH") fp.nHatch++; if (lk === "ENGRAVE") fp.nEngrave++;
      for (const q of m.circ || []) fp.circles.push(Object.assign({ L: lk, own: m === out ? "O" : "M", d: m.dO, inside: m.in, col: colOf(m), op: m.op }, q));
    } else if (m.k === "text") fp.text.push({ L: lk, str: m.str || "", undec: !!m.undec });
    else if (m.k === "shading") fp.shading++;
    else if (m.k === "image") fp.images++;
    else if (m.k === "xobj") fp.images += m.kids ? 0 : 1;
  }
  fp.outlineFill = !!(out && out.f);
  return fp;
}
/** Distinct concentric circle groups (hoops): [{cx, cy, radii: [..distinct..], members}] */
function hoops(fp) {
  const cl = [];
  for (const q of fp.circles) {
    let f = cl.find(x => Math.hypot(x.cx - q.cx, x.cy - q.cy) < Math.max(0.6, T.concentricTol * Math.min(x.rmin, q.r)));
    if (f) { f.rs.push(q); f.rmin = Math.min(f.rmin, q.r); } else cl.push({ cx: q.cx, cy: q.cy, rs: [q], rmin: q.r });
  }
  return cl.map(x => { const radii = []; for (const q of x.rs.slice().sort((a, b) => a.r - b.r)) if (!radii.length || Math.abs(q.r - radii[radii.length - 1]) > T.sameRadiusTol * q.r) radii.push(q.r); return { cx: x.cx, cy: x.cy, radii, circles: x.rs }; });
}

function classify(c, ctx) {
  const fp = c.fp || (c.fp = fingerprint(c)), defects = [], detail = {};
  const add = (code, why) => { if (!defects.includes(code)) defects.push(code); detail[code] = detail[code] ? (detail[code].length < 220 ? detail[code] + "; " + why : detail[code]) : why; };
  const mem = c.members || [];
  // 1 · grey boxes: a shading / image / form with no vector content is drawn as a translucent box by every preview and is in the per-SKU file
  const sh = mem.filter(m => m.k === "shading"), im = mem.filter(m => m.k === "image" || (m.k === "xobj" && !m.kids));
  if (sh.length) add("grey-box", `${sh.length} shading member(s) on ${[...new Set(sh.map(m => layerKey(m.L)))].join("/")}`);
  if (im.length) add("image-member", `${im.length} image/form member(s) on ${[...new Set(im.map(m => layerKey(m.L)))].join("/")}`);
  // grey fills: a filled neutral-grey rectangle-like member
  const gb = mem.filter(m => m.k === "path" && m.f && cls(m.fc) === "grey" && (m.rect || (m.ns === 1 && m.nc <= 6)));
  if (gb.length) add("grey-box", `${gb.length} grey filled box path(s)`);
  // 2 · text objects inside the charm (a label is removed from its charm's members; whatever else is text stays and is drawn as a grey box)
  if (fp.text.length) {
    const byL = {}; for (const t of fp.text) byL[t.L] = (byL[t.L] || 0) + 1;
    add("text-member", `${fp.text.length} text object(s) (${Object.entries(byL).map(([k, v]) => k + "×" + v).join(", ")}): "${fp.text.slice(0, 3).map(t => String(t.str).slice(0, 24)).join('", "')}"`);
    if (fp.text.some(t => t.L === "LABELS")) add("label-text-in-charm", "a LABELS-layer text is a member of this charm");
    if (fp.text.some(t => t.undec)) add("text-undecodable", "text that cannot be read back (no ToUnicode)");
  }
  // 3 · ink on the LABELS layer: an outlined label (or a box behind it) the grouping attached to this charm
  const li = mem.filter(m => m.k === "path" && layerKey(m.L) === "LABELS");
  if (li.length) add("label-ink", `${li.length} path(s) on the LABELS layer inside the charm`);
  // 4 · colour against layer: the convention of these masters is CUT black stroke, ENGRAVE red, HATCH blue fill, BACK ENGRAVING black
  const hb = mem.filter(m => m.k === "path" && layerKey(m.L) === "HATCH" && colOf(m) !== "blue" && colOf(m) !== "white" && colOf(m) !== "none" && (m.A == null || m.A > 0.05));
  if (hb.length) add("hatch-not-blue", `${hb.length} HATCH path(s) painted ${[...new Set(hb.map(m => paintKey(m) + " " + colOf(m)))].join(", ")}`);
  const eb = mem.filter(m => m.k === "path" && layerKey(m.L) === "ENGRAVE" && colOf(m) !== "red" && colOf(m) !== "white" && colOf(m) !== "none");
  if (eb.length) add("engrave-not-red", `${eb.length} ENGRAVE path(s) painted ${[...new Set(eb.map(m => paintKey(m) + " " + colOf(m)))].join(", ")}`);
  const cc = mem.filter(m => m.k === "path" && layerKey(m.L) === "CUT" && ["blue", "red", "green", "other"].includes(colOf(m)));
  if (cc.length) add("cut-chromatic", `${cc.length} CUT path(s) painted ${[...new Set(cc.map(m => paintKey(m) + " " + colOf(m)))].join(", ")}`);
  const ol = c.outline;
  if (ol && ol.k === "path" && !ol.syn && fp.outlineLayer !== "CUT") add("outline-not-cut-layer", `the outline is on ${ol.L || "no layer"}`);
  // 5 · solid black: the cut outline (or a CUT member covering the body) is a black FILL, so every preview paints the charm solid
  if (ol && ol.f && cls(ol.fc) === "black" && !ol.s) add("solid-black", `the outline path is a black fill (${ol.op}), not a stroke`);
  else if (ol && ol.f && cls(ol.fc) === "black") add("solid-black", `the outline path is a black fill and stroke (${ol.op})`);
  const body = mem.filter(m => m.k === "path" && m.f && layerKey(m.L) === "CUT" && cls(m.fc) === "black" && fp.outlineA > 0 && (m.A || 0) >= T.solidFill * fp.outlineA && !(m.circ && m.circ.length && (m.A || 0) < 80));
  if (body.length) add("solid-black", `${body.length} black-filled CUT member(s) cover ${Math.round(100 * Math.max(...body.map(m => m.A)) / fp.outlineA)} % of the outline area`);
  // 6 · hoops
  const hp = hoops(fp);
  for (const h of hp) {
    if (!h.circles.some(q => isRingSize(q.r)) && !h.radii.some(isRingSize)) continue;
    if (h.radii.length >= 3) add("extra-ring", `${h.radii.length} concentric circles at (${h.cx.toFixed(1)}, ${h.cy.toFixed(1)}), radii ${h.radii.map(v => v.toFixed(2)).join(" / ")}`);
  }
  for (const q of fp.circles) {
    if (!isRingSize(q.r) || q.own === "O") continue;
    if (q.d != null && q.d > T.detachedD && (q.inside == null || q.inside < 0.5)) add("detached-ring", `a ${(q.r * 2 * MM).toFixed(1)} mm ring ${q.d.toFixed(1)} pt from the outline edge`);
  }
  // 7 · ring-sized loose ink the grouping left out of the charm (the app only folds what lies in the charm's box back in)
  const lo = (ctx.orphansNear.get(c.index) || []).filter(o => o.k === "path" && o.circ && o.circ.some(q => isRingSize(q.r)) && o.dNear != null && o.dNear <= 12);
  if (lo.length) add("ring-orphan", `${lo.length} hoop-sized circle(s) ${Math.min(...lo.map(o => o.dNear)).toFixed(1)} pt away that belong to no charm`);
  // 8 · plausibility
  if (c.ob) { const w = (c.ob[2] - c.ob[0]) * MM, h = (c.ob[3] - c.ob[1]) * MM, mx = Math.max(w, h); if (mx > T.sizeMaxMm) add("size-outlier", `${w.toFixed(1)} × ${h.toFixed(1)} mm`); if (mx < T.sizeMinMm) add("size-outlier", `${w.toFixed(1)} × ${h.toFixed(1)} mm`); }
  const dim = [c.sku, ...(c.extra || [])].filter(s => /^\s*\d+(\.\d+)?\s*(MM|CM|IN|")\s*$/i.test(String(s || "")));
  if (dim.length) add("sku-is-dimension", `"${dim[0]}" is a size annotation read as a SKU`);
  if (ol && ol.k === "path" && ol.op === "n") add("outline-noop", "outline paint is a no-op");
  // 9 · read back through the per-SKU file (only when the dump was made with --rt)
  if (c.rt && c.rt.read) {
    const a = fingerprint(c.rt.read), b = c.rt.final ? fingerprint(c.rt.final) : null;
    const dropped = (a.kinds.path || 0) - (fingerprint({ outline: c.outline, members: mem }).kinds.path || 0);
    if (dropped) add("writer-changed-members", `the per-SKU file reads back with ${dropped > 0 ? "+" : ""}${dropped} path member(s)`);
    if (b) { const hf = hoops(b), n = Math.max(0, ...hf.map(h => h.radii.length)); if (n >= 3) add("extra-ring", `after integrateRings the app sees ${n} concentric circles at a hoop`); }
    if (c.rt.rings && c.rt.rings.error) add("rings-error", c.rt.rings.error); else if (c.rt.rings && c.rt.rings.left && c.rt.rings.left.length) add("rings-error", c.rt.rings.left[0]);
  }
  return { defects, detail };
}

/* ───────── driver ───────── */
function load(file) { const d = JSON.parse(fs.readFileSync(file, "utf8")); d.file = path.basename(file); return d; }
function short(m) { return /LIVING/i.test(m) ? "LIVING" : /OBJECTS/i.test(m) ? "OBJECTS" : /CUSTOMS/i.test(m) ? "CUSTOMS-MISC" : m; }

function readFindings(dir) {
  const out = {}; if (!dir || !fs.existsSync(dir)) return out;
  for (const f of fs.readdirSync(dir)) {
    const m = /^([A-Za-z]+)-affected\.json$/.exec(f); if (!m) continue;
    try {
      const j = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8")); const set = new Set();
      const take = v => { if (typeof v === "string") set.add(v.toUpperCase()); else if (v && typeof v === "object") { if (v.sku) set.add(String(v.sku).toUpperCase()); for (const k of ["skus", "affected", "offending", "list", "items"]) if (Array.isArray(v[k])) v[k].forEach(take); } };
      Array.isArray(j) ? j.forEach(take) : take(j); out[m[1].toUpperCase()] = set;
    } catch (e) { out[m[1].toUpperCase()] = { error: e.message }; }
  }
  return out;
}

const EXAMPLES = [
  ["running shoe", /RUNNING SHOE/i], ["fire dept badge", /FIRE (DEPT )?BADGE/i], ["baseball", /BASEBALL/i], ["angel", /^ANGEL|\bANGEL\b/i], ["saxophone (huggie)", /SAXOPHONE/i], ["curb", /^CURB\b/i],
  ["rubber duck (huggie)", /RUBBER DUCK/i], ["taekwondo belt", /TAEKWON/i], ["golf cart", /GOLF CART/i], ["Aun asi nos levantamos / 11.x MM blobs", /LEVANTAMOS|^11\.\d+ MM$/i], ["UCO seal", /\bUCO\b|CENTRAL OKLA/i], ["Summerfest", /SUMMERFEST/i],
  ["CELESTIAL huggie cards", /CELESTIAL/i], ["CHEER huggie cards", /^CHEER/i], ["CHARGING huggie cards", /CHARGING/i], ["LETTER_EARRING", /^LETTER_EARRING/i], ["INITIAL_LETTER", /^INITIAL_LETTER/i], ["LE", /^LE$/i]
];

function main(argv) {
  const files = [], o = { out: ".", findings: "" };
  for (let i = 0; i < argv.length; i++) { if (argv[i] === "--out") o.out = argv[++i]; else if (argv[i] === "--findings") o.findings = argv[++i]; else if (!argv[i].startsWith("--")) files.push(argv[i]); }
  if (!files.length) throw new Error("usage: audit-catalog.cjs report <dump.json>... --out <dir> [--findings <dir>]");
  fs.mkdirSync(o.out, { recursive: true });
  const rows = [], stats = {}, byDesign = [];
  for (const f of files) {
    const d = load(f), master = short(d.master), orphansNear = new Map();
    for (const x of d.orphans || []) if (x.near != null) { if (!orphansNear.has(x.near)) orphansNear.set(x.near, []); orphansNear.get(x.near).push(x); }
    const s = stats[master] = { master: d.master, charms: d.charms.length, labelled: 0, skuLines: 0, unlabelled: d.charms.filter(c => !c.sku).length, orphanLabels: (d.lab.orphans || []).length, duplicates: (d.lab.duplicates || []).length, undecodable: d.lab.undecodable, orphanInk: (d.orphans || []).length, flaggedDesigns: 0, flaggedSkus: 0 };
    // the master's own convention table: how common is each (layer, paint, colour) among labelled charms' artwork members
    const combo = {}; let tot = 0;
    for (const c of d.charms) if (c.sku) { c.fp = fingerprint(c); for (const [k, v] of Object.entries(c.fp.art)) { combo[k] = (combo[k] || 0) + v; tot += v; } }
    // family convention: the share of each feature among a family's designs
    const fam = new Map();
    for (const c of d.charms) if (c.sku) { const k = familyOf(c.sku); if (!fam.has(k)) fam.set(k, []); fam.get(k).push(c); }
    const famFeat = c => ({ outlineFill: c.fp.outlineFill ? "filled" : "stroked", hatch: c.fp.nHatch ? "hatch" : "nohatch", rings: hoops(c.fp).filter(h => h.circles.some(q => isRingSize(q.r))).length + "ring" });
    const famMajor = new Map();
    for (const [k, list] of fam) { if (list.length < T.famMin) continue; const m = {}; for (const c of list) { const ft = famFeat(c); for (const [a, b] of Object.entries(ft)) { m[a] = m[a] || {}; m[a][b] = (m[a][b] || 0) + 1; } } const maj = {}; for (const [a, vs] of Object.entries(m)) { const [bv, bn] = Object.entries(vs).sort((x, y) => y[1] - x[1])[0]; if (bn / list.length >= T.famShare) maj[a] = bv; } famMajor.set(k, { n: list.length, maj }); }
    for (const c of d.charms) {
      if (!c.sku) continue; s.labelled++; const names = [c.sku + (c.size ? " · " + c.size : ""), ...(c.extra || [])]; s.skuLines += names.length;
      const r = classify(c, { orphansNear });
      // rare combos (information only: they are listed, they do not make a SKU an offender on their own)
      const rare = Object.keys(c.fp.art).filter(k => combo[k] / tot < T.rareShare && combo[k] >= T.rareMin);
      const fm = famMajor.get(familyOf(c.sku)); const famDev = [];
      if (fm) { const ft = famFeat(c); for (const [a, v] of Object.entries(fm.maj)) if (ft[a] !== v) famDev.push(`${a}: ${ft[a]} (family ${fm.n} designs: ${v})`); }
      if (famDev.length) { r.defects.push("family-outlier"); r.detail["family-outlier"] = `${familyOf(c.sku)}: ${famDev.join("; ")}`; }
      byDesign.push({ master, index: c.index, names, label: c.label, defects: r.defects, detail: r.detail, rare, ob: c.ob, nMembers: (c.members || []).length, layers: c.fp.layers, art: c.fp.art, rtBytes: c.rt && c.rt.bytes || null });
      if (r.defects.length) s.flaggedDesigns++;
      names.forEach((nm, i) => {
        const sku = nm.replace(/ · [A-Z0-9]{1,3}$/, "");
        rows.push({ sku: sku.toUpperCase(), name: i === 0 ? (c.label || sku) : sku, master, charmIndex: c.index, defects: r.defects.slice(), detail: r.detail, sharesFileWith: names.filter((_, j) => j !== i).map(x => x.replace(/ · [A-Z0-9]{1,3}$/, "").toUpperCase()), rare: rare.length ? rare : undefined });
        if (r.defects.length) s.flaggedSkus++;
      });
    }
  }
  // the same SKU can appear in two masters: it is one record in the catalogue (flag both rows, keep both)
  const offending = rows.filter(r => r.defects.length);
  // counts per category (SKUs, and designs = distinct per-SKU files)
  const cat = {}; for (const r of rows) for (const dft of r.defects) { cat[dft] = cat[dft] || { skus: 0, designs: new Set() }; cat[dft].skus++; cat[dft].designs.add(r.master + "#" + r.charmIndex); }
  const findings = readFindings(o.findings), flaggedSet = new Set(offending.map(r => r.sku));
  const cross = {};
  for (const [name, set] of Object.entries(findings)) {
    if (set.error) { cross[name] = { error: set.error }; continue; }
    const allSkus = new Set(rows.map(r => r.sku)), mine = new Set(offending.map(r => r.sku));
    const inCatalogue = [...set].filter(s => allSkus.has(s)), both = inCatalogue.filter(s => mine.has(s));
    cross[name] = { workerCount: set.size, inMasters: inCatalogue.length, alsoFlaggedHere: both.length, notFlaggedHere: inCatalogue.filter(s => !mine.has(s)).slice(0, 25) };
  }
  // Paul's examples
  const ex = EXAMPLES.map(([label, re]) => { const hit = rows.filter(r => re.test(r.sku) || re.test(r.name)); return { label, count: hit.length, rows: hit.slice(0, 12).map(r => ({ sku: r.sku, master: r.master, defects: r.defects })) }; });
  // write
  const sorted = offending.sort((a, b) => a.master.localeCompare(b.master) || a.sku.localeCompare(b.sku));
  fs.writeFileSync(path.join(o.out, "CATALOG-offending.json"), JSON.stringify({ generatedAt: new Date().toISOString(), tool: "scripts/audit-catalog.cjs report", note: "offline audit of the three masters (current main code); one row per SKU; sharesFileWith lists the SKUs rewritten with it", thresholds: T, counts: Object.fromEntries(Object.entries(cat).map(([k, v]) => [k, { skus: v.skus, designs: v.designs.size }])), offending: sorted.map(({ sku, name, master, charmIndex, defects, detail, sharesFileWith }) => ({ sku, name, master, charmIndex, defects, detail, sharesFileWith })) }, null, 1));
  const L = [];
  L.push("# Catalogue audit (offline, from the three master files)", "", `Generated ${new Date().toISOString()} by scripts/audit-catalog.cjs on the current main code. Nothing was read from the live site.`, "");
  L.push("## Masters", "", "| master | charm outlines | labelled designs | SKU lines | unlabelled outlines | labels with no charm | duplicate labels | loose ink | designs flagged | SKU lines flagged |", "|---|---|---|---|---|---|---|---|---|---|");
  for (const s of Object.values(stats)) L.push(`| ${s.master} | ${s.charms} | ${s.labelled} | ${s.skuLines} | ${s.unlabelled} | ${s.orphanLabels} | ${s.duplicates} | ${s.orphanInk} | ${s.flaggedDesigns} | ${s.flaggedSkus} |`);
  const tl = Object.values(stats).reduce((a, s) => ({ l: a.l + s.labelled, k: a.k + s.skuLines, f: a.f + s.flaggedSkus, d: a.d + s.flaggedDesigns }), { l: 0, k: 0, f: 0, d: 0 });
  L.push("", `Total: ${tl.l} labelled designs, ${tl.k} SKU lines (${new Set(rows.map(r => r.sku)).size} distinct SKUs); ${tl.d} designs and ${flaggedSet.size} distinct SKUs are flagged (${tl.f} SKU lines).`, "");
  L.push("## Defects per category", "", "| category | SKU lines | designs (files to rewrite) |", "|---|---|---|");
  for (const [k, v] of Object.entries(cat).sort((a, b) => b[1].skus - a[1].skus)) L.push(`| ${k} | ${v.skus} | ${v.designs.size} |`);
  L.push("", "## Paul's examples", "");
  for (const e of ex) { L.push(`- **${e.label}**: ${e.count} SKU line(s)`); for (const r of e.rows.slice(0, 8)) L.push(`  - ${r.sku} (${r.master}): ${r.defects.length ? r.defects.join(", ") : "nothing flagged"}`); }
  if (Object.keys(cross).length) { L.push("", "## Cross-check with the five workers", ""); for (const [k, v] of Object.entries(cross)) L.push(v.error ? `- ${k}: unreadable (${v.error})` : `- ${k}: ${v.workerCount} SKUs listed, ${v.inMasters} of them in the masters, ${v.alsoFlaggedHere} also flagged here${v.notFlaggedHere.length ? `; not flagged here: ${v.notFlaggedHere.join(", ")}` : ""}`); }
  fs.writeFileSync(path.join(o.out, "CATALOG-report.md"), L.join("\n") + "\n");
  console.log(`designs ${tl.l}, SKU lines ${tl.k}, flagged designs ${tl.d}, flagged distinct SKUs ${flaggedSet.size}`);
  for (const [k, v] of Object.entries(cat).sort((a, b) => b[1].skus - a[1].skus)) console.log(String(v.skus).padStart(6), String(v.designs.size).padStart(6), k);
  return { rows, stats, cat, ex, cross };
}
module.exports = { main, classify, fingerprint, hoops, cls, T };
