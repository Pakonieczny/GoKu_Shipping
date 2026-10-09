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
/** Mirrors NOT_SKU in charm-nest-pdf.js (parseSkuLabel): a measurement or a view word typed beside a charm is a callout, never a SKU. */
const NOT_SKU = /^(?:[\d.,]+\s*(?:mm|cm|in|inch|inches|")|(?:front|back)(?:\s*(?:\/|&|AND)?\s*(?:front|back))?|option(?:\s+[a-z])?|orig(?:inal)?\s*size)$/i;

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

/** A fill the app paints as ink: black, or a dark neutral (the app's own test: no tint, luma <= 0.35). */
const darkFill = c => { if (!c) return false; const mx = Math.max(c[0], c[1], c[2]), mn = Math.min(c[0], c[1], c[2]); return mx - mn <= 0.15 && 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2] <= 0.35; };
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

/** What a defect needs once the code fixes are on main. rewrite: the per-SKU file or the index record holds it (re-index the SKU);
 *  read-time: the app decides it every time a file is read (the code fix is enough; thumbnails are the only thing baked);
 *  prune: a record that is not a catalogue SKU; review: a hint for a person, no automatic fix. */
const FIX = {
  "colour-lost-in-file": ["rewrite", "the per-SKU writer dropped the colour the layer's first text object set (isolate)"],
  "grey-box": ["rewrite", "shading / box members are in the file and in the record's size and silhouette"], "image-member": ["rewrite", "an image is in the file"],
  "text-member": ["rewrite", "sample or note text is in the file and counted in holes, size and up angle"], "label-text-in-charm": ["rewrite", "label text is a member"], "label-ink": ["rewrite", "outlined label ink is a member"],
  "detached-ring": ["rewrite", "the hoop is separate from the body in the file and the record"], "extra-ring": ["rewrite", "a doubled hoop circle is in the file"], "ring-orphan": ["rewrite", "a hoop circle was left out of the charm"],
  "writer-changed-members": ["rewrite", "the file does not read back as the master draws it"], "rings-error": ["review", "ring welding failed on read-back"],
  "solid-black": ["read-time", "drawn as a cut line now; only the stored thumbnail PNG is still a black body"], "cut-chromatic": ["read-time", "colour role is decided when the file is read"], "hatch-not-blue": ["read-time", "colour role is decided when the file is read"],
  "engrave-not-red": ["read-time", "colour role is decided when the file is read"], "outline-not-cut-layer": ["read-time", "outline role is decided when the file is read"],
  "sku-is-dimension": ["prune", "a size note or view word (11.4 MM, OPTION B, ORIGINAL SIZE) was read as a SKU: remove the record (catalog-repair prune)"],
  "text-undecodable": ["review", "text the parser cannot read"], "size-outlier": ["review", "far from the family's size"], "family-outlier": ["review", "differs from the family's convention"], "outline-noop": ["review", "the outline draws nothing"]
};
const fixOf = defects => { const f = defects.map(d => (FIX[d] || ["review"])[0]); return f.includes("rewrite") ? "rewrite" : f.includes("prune") ? "prune" : f.includes("read-time") ? "read-time" : f.length ? "review" : null; };
function classify(c, ctx) {
  const fp = c.fp || (c.fp = fingerprint(c)), defects = [], detail = {};
  const add = (code, why) => { if (!defects.includes(code)) defects.push(code); detail[code] = detail[code] ? (detail[code].length < 220 ? detail[code] + "; " + why : detail[code]) : why; };
  const mem = c.members || [];
  // 0 · paint lost by the per-SKU writer: the master draws these paths in a colour that a cut-out segment (a text object, say) set,
  //     and the file written for the SKU no longer sets it (HATCH blue read back as black). Baked into the file: needs a re-index.
  if (c.leak) { const w = (c.leak.first || []).map(x => `${x.L || "no layer"} ${x.fill ? "fill " + x.fill[0] + " -> " + x.fill[1] : ""}${x.stroke ? " stroke " + x.stroke[0] + " -> " + x.stroke[1] : ""}${x.lw ? " width " + x.lw[0] + " -> " + x.lw[1] : ""}`.trim()); add("colour-lost-in-file", `${c.leak.n} of ${c.leak.of} member(s) are painted differently in the per-SKU file: ${[...new Set(w)].slice(0, 3).join("; ")}`); }
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
  if (ol && ol.f && darkFill(ol.fc) && !ol.s) add("solid-black", `the outline path is a black fill (${ol.op}), not a stroke`);
  else if (ol && ol.f && darkFill(ol.fc)) add("solid-black", `the outline path is a black fill and stroke (${ol.op})`);
  const body = mem.filter(m => m.k === "path" && m.f && layerKey(m.L) === "CUT" && darkFill(m.fc) && fp.outlineA > 0 && (m.A || 0) >= T.solidFill * fp.outlineA && !(m.circ && m.circ.length && (m.A || 0) < 80));
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
  const dim = [c.sku, ...(c.extra || [])].filter(s => NOT_SKU.test(String(s || "").trim()));
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
      const take = v => { if (typeof v === "string") set.add(v.toUpperCase()); else if (v && typeof v === "object") { if (v.sku) set.add(String(v.sku).toUpperCase()); for (const k of ["skus", "skusBefore", "affected", "offending", "list", "items", "entries", "charms"]) if (Array.isArray(v[k])) v[k].forEach(take); } };
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
  const files = [], o = { out: ".", findings: "", basis: "" };
  for (let i = 0; i < argv.length; i++) { if (argv[i] === "--out") o.out = argv[++i]; else if (argv[i] === "--findings") o.findings = argv[++i]; else if (argv[i] === "--basis") o.basis = argv[++i]; else if (!argv[i].startsWith("--")) files.push(argv[i]); }
  if (!files.length) throw new Error("usage: audit-catalog.cjs report <dump.json>... --out <dir> [--findings <dir>] [--basis <text>]");
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
        rows.push({ sku: sku.toUpperCase(), name: i === 0 ? (c.label || sku) : sku, master, charmIndex: c.index, defects: r.defects.slice(), fix: fixOf(r.defects), detail: r.detail, sharesFileWith: names.filter((_, j) => j !== i).map(x => x.replace(/ · [A-Z0-9]{1,3}$/, "").toUpperCase()), rare: rare.length ? rare : undefined });
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
  const ex = EXAMPLES.map(([label, re]) => { const hit = rows.filter(r => re.test(r.sku) || re.test(r.name)).sort((a, b) => (b.defects.length > 0) - (a.defects.length > 0)); return { label, count: hit.length, flagged: hit.filter(r => r.defects.length).length, rows: hit.slice(0, 12).map(r => ({ sku: r.sku, master: r.master, defects: r.defects })) }; });
  // write
  // a SKU read from two masters is one record in the catalogue: whichever master is indexed last owns it (the other is blocked or replaced)
  const mastersOf = new Map(); for (const r of rows) { if (!mastersOf.has(r.sku)) mastersOf.set(r.sku, new Set()); mastersOf.get(r.sku).add(r.master); }
  const multiMaster = [...mastersOf].filter(([, m]) => m.size > 1).map(([sku, m]) => ({ sku, masters: [...m], flagged: flaggedSet.has(sku) })).sort((a, b) => a.sku.localeCompare(b.sku));
  const byFix = {}; for (const r of offending) { const k = r.fix || "none"; byFix[k] = byFix[k] || { skus: new Set(), lines: 0, designs: new Set() }; byFix[k].skus.add(r.sku); byFix[k].lines++; byFix[k].designs.add(r.master + "#" + r.charmIndex); }
  const fixCounts = Object.fromEntries(Object.entries(byFix).map(([k, v]) => [k, { skus: v.skus.size, skuLines: v.lines, designs: v.designs.size }]));
  const sorted = offending.sort((a, b) => a.master.localeCompare(b.master) || a.sku.localeCompare(b.sku));
  fs.writeFileSync(path.join(o.out, "CATALOG-offending.json"), JSON.stringify({ generatedAt: new Date().toISOString(), tool: "scripts/audit-catalog.cjs report", note: "offline audit of the three masters (current main code); one row per SKU; sharesFileWith lists the SKUs rewritten with it", thresholds: T, fixCounts, fixes: FIX, multiMaster, counts: Object.fromEntries(Object.entries(cat).map(([k, v]) => [k, { skus: v.skus, designs: v.designs.size }])), offending: sorted.map(({ sku, name, master, charmIndex, defects, fix, detail, sharesFileWith }) => ({ sku, name, master, charmIndex, defects, fix, detail, sharesFileWith })) }, null, 1));
  const L = [];
  L.push("# Catalogue audit (offline, from the three master files)", "", `Generated ${new Date().toISOString()} by scripts/audit-catalog.cjs from dump files made ${o.basis || "with the code that was checked out when `audit-catalog.cjs dump` ran (the dump header names its commit)"}. Nothing was read from the live site.`, "");
  L.push("## Masters", "", "| master | charm outlines | labelled designs | SKU lines | unlabelled outlines | labels with no charm | duplicate labels | loose ink | designs flagged | SKU lines flagged |", "|---|---|---|---|---|---|---|---|---|---|");
  for (const s of Object.values(stats)) L.push(`| ${s.master} | ${s.charms} | ${s.labelled} | ${s.skuLines} | ${s.unlabelled} | ${s.orphanLabels} | ${s.duplicates} | ${s.orphanInk} | ${s.flaggedDesigns} | ${s.flaggedSkus} |`);
  const tl = Object.values(stats).reduce((a, s) => ({ l: a.l + s.labelled, k: a.k + s.skuLines, f: a.f + s.flaggedSkus, d: a.d + s.flaggedDesigns }), { l: 0, k: 0, f: 0, d: 0 });
  L.push("", `Total: ${tl.l} labelled designs, ${tl.k} SKU lines (${new Set(rows.map(r => r.sku)).size} distinct SKUs); ${tl.d} designs and ${flaggedSet.size} distinct SKUs are flagged (${tl.f} SKU lines).`, "");
  L.push("## Defects per category", "", "| category | SKU lines | designs | what it needs | why |", "|---|---|---|---|---|");
  for (const [k, v] of Object.entries(cat).sort((a, b) => b[1].skus - a[1].skus)) L.push(`| ${k} | ${v.skus} | ${v.designs.size} | ${(FIX[k] || ["review", ""])[0]} | ${(FIX[k] || ["", ""])[1]} |`);
  L.push("", "By what the repair has to do (a design with several defects counts once, under the strongest):", "");
  for (const k of ["rewrite", "prune", "read-time", "review"]) if (fixCounts[k]) L.push(`- **${k}**: ${fixCounts[k].designs} design(s), ${fixCounts[k].skuLines} SKU line(s), ${fixCounts[k].skus} distinct SKU(s)`);
  L.push("", "## Paul's examples", "");
  for (const e of ex) { L.push(`- **${e.label}**: ${e.count} SKU line(s), ${e.flagged} flagged`); for (const r of e.rows.slice(0, 8)) L.push(`  - ${r.sku} (${r.master}): ${r.defects.length ? r.defects.join(", ") : "nothing flagged"}`); }
  L.push("", "## SKUs that sit in more than one master", "", `${multiMaster.length} SKU(s) are labelled in two masters. They are ONE record in the catalogue, and the master indexed last owns it, so a repair run per master must not rewrite them from the wrong one: ${multiMaster.slice(0, 40).map(x => `${x.sku} (${x.masters.map(m => m.replace(/^MASTER SKU_|_MV.*$/g, "")).join(" + ")}${x.flagged ? ", flagged" : ""})`).join(", ")}${multiMaster.length > 40 ? " …" : ""}`);
  if (Object.keys(cross).length) { L.push("", "## Cross-check with the five workers", "", "A worker's SKU that is not flagged here has no defect baked into its per-SKU file or record (its fix is read-time, or it changes nothing for that SKU), so it needs no rewrite.", ""); for (const [k, v] of Object.entries(cross)) L.push(v.error ? `- ${k}: unreadable (${v.error})` : `- ${k}: ${v.workerCount} SKUs listed, ${v.inMasters} of them in the masters, ${v.alsoFlaggedHere} also flagged here${v.notFlaggedHere.length ? `; not flagged here: ${v.notFlaggedHere.join(", ")}` : ""}`); }
  fs.writeFileSync(path.join(o.out, "CATALOG-report.md"), L.join("\n") + "\n");
  console.log(`designs ${tl.l}, SKU lines ${tl.k}, flagged designs ${tl.d}, flagged distinct SKUs ${flaggedSet.size}`);
  for (const [k, v] of Object.entries(cat).sort((a, b) => b[1].skus - a[1].skus)) console.log(String(v.skus).padStart(6), String(v.designs.size).padStart(6), k);
  return { rows, stats, cat, ex, cross };
}
module.exports = { main, classify, fingerprint, hoops, cls, T };
