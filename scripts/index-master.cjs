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
 *    --all               rebuild every charm on the sheet, including SKUs the library already holds (default: skip those)
 *    --only LIST         rebuild ONLY the charms carrying one of these SKUs (a JSON file: an array of SKUs, of {sku} rows, or
 *                        an object with .skus / .offending; or SKUs separated by commas). Implies --all for those charms and
 *                        reads the library for just those SKUs (masterGetMany) instead of the whole index (masterList).
 *                        A charm's other SKUs travel with it (they share one file), so they are rewritten too.
 *    --new-hash          with --only: write the charm hash the new file computes. By default a rebuilt SKU whose size, area and holes
 *                        are unchanged keeps the hash it has (the hash counts the members, so dropping a grey box would change it, and
 *                        a hash that moves makes the protected placements of a cut Rose sheet refuse to save)
 *    --pin FILE          which charm owns a SKU that the sheet labels under more than one charm ("twins"). A JSON object
 *                        { "SKU": { "at": [x, y] } } (a point on the owning charm, in page points: its outline box centre) or
 *                        { "SKU": charmIndex }; "SKU__S" pins a sized line. { "SKU": "elsewhere" } leaves the SKU to
 *                        another master: every charm of this sheet loses it (a SKU read from two masters has one home). Without a pin the first charm of the page keeps the SKU
 *                        (the one labelCharms reports as kept: the label highest on the page), the same on every run.
 *    --out-dir DIR       STAGE instead of upload: write every per-SKU .ai / .png and records.json (the exact entries that
 *                        would be sent to masterPutIndex) under DIR and touch no server (no --origin needed). Review the
 *                        count, the sizes and a few files, then run again without --out-dir to write the same thing.
 *    --work-dir DIR      keep the progress and report files in DIR instead of next to the master file
 *    --no-pairs          do not look for designs that draw two bodies under one label (mismatched pairs: see pairLayer). With it the run is exactly what it was before the pair layer.
 *    --pair-also LIST    also fold these doubtful rows into one design (a JSON list or SKUs separated by commas, as --only): a row the layer found but was not sure of
 *                        (see "pairs" in the report). A row with a label of its own on a second body cannot be folded.
 *    (the passcode is read from the EDIT_PASSCODE environment variable when --passcode is not given: it is never printed)
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
    else if (a === "--all") o.all = true;
    else if (a === "--only") { o.only = String(v || ""); i++; }
    else if (a === "--new-hash") o.newHash = true;
    else if (a === "--pin") { o.pin = String(v || ""); i++; }
    else if (a === "--out-dir") { o.outDir = String(v || ""); i++; }
    else if (a === "--work-dir") { o.workDir = String(v || ""); i++; }
    else if (a === "--no-pairs") o.noPairs = true;
    else if (a === "--pair-also") { o.pairAlso = String(v || ""); i++; }
    else if (a === "--engrave-margin-mm") { o.engraveMarginMm = +v || 0.8; i++; }
    else if (!a.startsWith("--") && !o.file) o.file = a;
  }
  if (!o.passcode && process.env.EDIT_PASSCODE) o.passcode = process.env.EDIT_PASSCODE;
  return o;
}

/** --only: a list of SKUs from a file or a comma list → Set of upper-case SKUs (or null when not given). */
function onlySet(spec) {
  if (!spec) return null;
  let list;
  if (fs.existsSync(spec)) {
    const txt = fs.readFileSync(spec, "utf8"); let j; try { j = JSON.parse(txt); } catch (_) { j = txt.split(/[\r\n,]+/); }   // JSON (a list, {skus}, {offending}) or a plain list, one SKU a line
    list = Array.isArray(j) ? j : Array.isArray(j.skus) ? j.skus : Array.isArray(j.offending) ? j.offending : [];
  } else list = spec.split(",");
  const set = new Set(list.map(x => String(x && typeof x === "object" ? x.sku : x || "").trim().toUpperCase()).filter(Boolean));
  if (!set.size) throw new Error("--only names no SKU");
  return set;
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
function thumbnailPng(Geom, charm, size, PDF) {
  let Resvg = null; try { ({ Resvg } = require("@resvg/resvg-js")); } catch (_) { return null; }
  // a mismatched pair design (two different bodies under one SKU) is stored as ONE picture of both bodies side by side at one scale with a
  // Left and a Right chip under them (charm-nest-pair-thumb.js, the same picture the app draws); every other design keeps today's picture exactly
  let PT = null; try { PT = require("../charm-nest-pair-thumb.js"); } catch (_) {}
  const pair = PT ? PT.plan(charm) : null;
  const b = charm.bbox, pad = 2, w = b[2] - b[0] + 2 * pad, h = b[3] - b[1] + 2 * pad, s = pair ? PT.layout(pair, b, { size, padPt: pad }).s : size / Math.max(w, h);
  const css = c => `rgb(${Math.round((c[0] || 0) * 255)},${Math.round((c[1] || 0) * 255)},${Math.round((c[2] || 0) * 255)})`;
  const parts = [], partOf = new Map();   // (partOf: what each member drew, kept only so a pair's bodies can be grouped when one has to be turned over)
  let cur = null; const emit = x => { parts.push(x); if (pair) { const l = partOf.get(cur) || []; l.push(x); partOf.set(cur, l); } };
  for (const m of charm.members) { cur = m; if (m.kind !== "path") continue; const d = Geom.svgPathOf(m); if (!d) continue;
    // a cut silhouette the master drew as a black fill is the cut line, drawn as an outline: a solid body would read as a solid engraving.
    // A black fill INSIDE the charm (m.hatchBlue, stamped by the grouping: charm-nest-pdf.js "a black FILL is blue hatching") is hatching: filled blue below.
    if (PDF && PDF.isCutSilhouetteFill(charm, m)) { emit(`<path d="${d}" fill="none" stroke="#000" stroke-width="${Math.max(0.6 / s, 0.25)}"/>`); continue; }
    // the red edge of thin strips that run across the charm (BASKETBALL_9338's seams, charm-nest-pdf.js "thin strips outlined in red are hatching") is the hatched strip itself: filled blue, no line
    if (m.hatchStrip && m.hatchBlue && m.stroke && !m.fill) { emit(`<path d="${d}" fill="rgb(0,0,255)" fill-rule="evenodd" stroke="none"/>`); continue; }
    // black line art (charm-nest-pdf.js "black LINE ART is blue hatching": open lines, the closed shapes on them, black ink on an engraving layer) is the same line in blue
    if (m.hatchLine && m.hatchBlue && m.stroke && !m.fill) { emit(`<path d="${d}" fill="none" stroke="rgb(0,0,255)" stroke-width="${Math.max(0.6 / s, m.lwPt || 0.5)}"/>`); continue; }
    // a cut line is drawn black, as the app's canvas does (charm-nest-pdf.js drawCharm: the outline and every cut-line member get a black pen whatever colour the master gave them)
    const cutPen = m === charm.outline || (Geom.isCutLine && Geom.isCutLine(m));
    const st = m.stroke ? (cutPen ? "#000" : Math.min(m.strokeRGB[0], m.strokeRGB[1], m.strokeRGB[2]) >= 0.92 ? "#2a2724" : css(m.strokeRGB)) : "none"; emit(`<path d="${d}" fill="${m.fill ? (m.hatchBlue && !m.stroke ? "rgb(0,0,255)" : css(m.fillRGB)) : "none"}" fill-rule="${m.paintOp && m.paintOp.endsWith("*") ? "evenodd" : "nonzero"}" stroke="${st}" stroke-width="${Math.max(0.6 / s, m.lwPt || 0.5)}"/>`); }
  // (the cut outline in black, as the app's canvas draws it (charm-nest-pdf.js drawCharm: pen #000, the outline's own width, the cut hairline for a silhouette the master filled): both bodies'
  //  outlines for a pair, the charm's own for every other design. It used to be a red overlay, so a stored picture showed a thick red ring that the Library's own drawing never had.)
  const blackOutline = o => `<path d="${Geom.svgPathOf(o)}" fill="none" stroke="#000" stroke-width="${Math.max(0.6 / s, PDF && PDF.isCutSilhouetteFill && PDF.isCutSilhouetteFill(charm, o) ? 0.25 : o.lwPt || 0.25)}"/>`;
  for (const o of pair ? pair.bodies.map(x => x.outline).filter(Boolean) : [charm.outline]) parts.push(blackOutline(o));
  if (pair) {
    // each ear faces its own side: a body the master drew facing the wrong way is turned over left to right about its own centre (the same as the app's pictures)
    let inner = parts.join("");
    if (pair.bodies.some(x => x.mirror)) {
      const used = new Set();
      inner = pair.bodies.map(bd => { const g = (bd.members || []).map(m => { used.add(m); return (partOf.get(m) || []).join(""); }).join("") + (bd.outline ? blackOutline(bd.outline) : ""); return `<g${bd.mirror ? ` transform="${PT.mirrorSvg(bd.bbox)}"` : ""}>${g}</g>`; }).join("")
        + [...partOf.entries()].filter(([m]) => !used.has(m)).map(([, l]) => l.join("")).join("");
    }
    const pic = PT.svgPicture(pair, { bbox: b, padPt: pad, size, bg: "#ece7dc", inner });
    // the chip lettering is the repo's own Source Sans 3 Semibold, so it does not depend on a font this PC happens to have
    try { return new Resvg(pic.svg, { fitTo: { mode: "width", value: pic.layout.W }, font: { fontFiles: [path.join(root, "vendor", "fonts", "SourceSans3-Semibold.otf")], loadSystemFonts: false, defaultFontFamily: "Source Sans 3 Semibold" } }).render().asPng(); } catch (_) { return null; }
  }
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${Math.max(8, Math.round(w * s))}" height="${Math.max(8, Math.round(h * s))}" viewBox="0 0 ${w} ${h}"><rect width="100%" height="100%" fill="#ece7dc"/><g transform="translate(${pad - b[0]} ${b[3] + pad}) scale(1 -1)">${parts.join("")}</g></svg>`;
  try { return new Resvg(svg, { fitTo: { mode: "width", value: Math.max(8, Math.round(w * s)) } }).render().asPng(); } catch (_) { return null; }
}

/** --pin: SKU (or SKU__SIZE) → { at: [x, y] } | { index } | charm index. Returns a Map keyed by the upper-case SKU key, or null. */
function pinMap(spec) {
  if (!spec) return null;
  const j = JSON.parse(fs.readFileSync(spec, "utf8")), src = j && j.pins && typeof j.pins === "object" ? j.pins : j, m = new Map();
  for (const [k, v] of Object.entries(src || {})) m.set(String(k).trim().toUpperCase().replace(/\s*[·•]\s*([A-Z0-9]{1,3})$/, "__$1"), v);
  return m;
}
/** A SKU read under more than one charm of the sheet ("twins") belongs to ONE of them. labelCharms reports every later charm in
 *  `duplicates` and means to drop the SKU from it, but its drop removes only the first line it finds: a charm whose label text
 *  repeats the SKU on a second line (or a stack that lists it twice) keeps it, so both charms were built under one file name and one
 *  index record, and which of them reached the library depended on which build finished last (four run at once) and which entry the
 *  index took first. Here the owner is settled before anything is built, the same on every run: the charm a pin names, else the first
 *  charm of the page (the one labelCharms kept). Every other charm loses every line of that SKU (it keeps its other SKUs, or becomes
 *  unlabelled), and a SKU written twice under one charm is kept once. Returns the decisions, for the report. */
function settleTwins(lab, charms, pins) {
  const keyOf = x => { const sk = String(x.sku || "").toUpperCase(); return x.size ? `${sk}__${String(x.size).toUpperCase()}` : sk; };
  const linesOf = l => [l].concat(l.extra || []);
  const cOf = i => charms.find(c => c.index === i);
  const setLines = (i, lines) => {
    const l = lab.labels.get(i), c = cOf(i); if (!l) return;
    if (!lines.length) { lab.labels.delete(i); if (c) { c.sku = null; c.skuSize = null; c.label = null; c.extraSkus = []; } lab.unlabelled.push(i); return; }
    const f = lines[0]; if (f !== l) { l.sku = f.sku; l.size = f.size; l.str = f.str; l.bbox = f.bbox; }
    l.extra = lines.slice(1); if (c) { c.sku = l.sku; c.skuSize = l.size; c.extraSkus = l.extra; }
  };
  const giveLine = (i, line) => {
    const l = lab.labels.get(i), c = cOf(i), x = { sku: line.sku, size: line.size, str: line.str, bbox: line.bbox };
    if (l) { l.extra = (l.extra || []).concat([x]); if (c) c.extraSkus = l.extra; return; }
    lab.labels.set(i, Object.assign({ seg: null, gap: 0, extra: [] }, x));
    const u = lab.unlabelled.indexOf(i); if (u >= 0) lab.unlabelled.splice(u, 1);
    if (c) { c.sku = x.sku; c.skuSize = x.size; c.extraSkus = []; }
  };
  for (const [i, l] of [...lab.labels]) {                              // a SKU written twice under one charm is one line
    const seen = new Set(), lines = linesOf(l).filter(x => { const k = keyOf(x); if (seen.has(k)) return false; seen.add(k); return true; });
    if (lines.length !== 1 + (l.extra || []).length) setLines(i, lines);
  }
  const claims = new Map(), kept = new Map(), add = (k, i) => { if (!claims.has(k)) claims.set(k, new Set()); claims.get(k).add(i); };
  for (const [i, l] of lab.labels) for (const x of linesOf(l)) add(keyOf(x), i);
  for (const d of lab.duplicates || []) { const k = keyOf(d); add(k, d.charmIndex); add(k, d.firstIndex); if (!kept.has(k)) kept.set(k, d.firstIndex); }
  const out = [];
  for (const [k, set] of claims) {
    const pin = pins && pins.get(k), away = pin === "elsewhere" || (pin && pin.elsewhere === true);   // "elsewhere": another master owns this SKU
    if (set.size < 2 && !away) continue;
    const idx = [...set].sort((a, b) => a - b); let owner = kept.has(k) && set.has(kept.get(k)) ? kept.get(k) : idx[0], rule = "first on the page";
    if (away) { for (const i of idx) { const l = lab.labels.get(i); if (l) setLines(i, linesOf(l).filter(x => keyOf(x) !== k)); } out.push({ key: k, owner: null, others: idx, rule: "elsewhere" }); continue; }
    if (pin != null) {
      let want = typeof pin === "number" ? pin : pin && pin.index != null ? +pin.index : null;
      if (want == null && pin && Array.isArray(pin.at)) {
        const [px, py] = pin.at.map(Number); let best = null, bd = Infinity;
        for (const i of idx) { const b = cOf(i).outline.bbox; if (px < b[0] - 1 || px > b[2] + 1 || py < b[1] - 1 || py > b[3] + 1) continue; const d = Math.hypot(px - (b[0] + b[2]) / 2, py - (b[1] + b[3]) / 2); if (d < bd) { bd = d; best = i; } }
        want = best;
      }
      if (want == null || !set.has(want)) throw new Error(`--pin ${k}: no charm carrying it is where the pin says (${JSON.stringify(pin)}); its charms are ${idx.map(i => { const b = cOf(i).outline.bbox; return `#${i} at ${((b[0] + b[2]) / 2).toFixed(1)},${((b[1] + b[3]) / 2).toFixed(1)}`; }).join("; ")}`);
      owner = want; rule = "pinned";
    }
    // an owner that lost the line to labelCharms' duplicate rule (another charm was first on the page, so this one's line was dropped) gets it back
    if (!(lab.labels.get(owner) && linesOf(lab.labels.get(owner)).some(x => keyOf(x) === k))) {
      const proto = idx.map(i => lab.labels.get(i)).filter(Boolean).flatMap(linesOf).find(x => keyOf(x) === k);
      if (proto) giveLine(owner, proto);
    }
    for (const i of idx) if (i !== owner) { const l = lab.labels.get(i); if (l) setLines(i, linesOf(l).filter(x => keyOf(x) !== k)); }
    out.push({ key: k, owner, others: idx.filter(i => i !== owner), rule });
  }
  let n = 0; for (const l of lab.labels.values()) n += 1 + (l.extra || []).length; lab.skuCount = n;
  return out;
}

/** The storage name of each charm's file (charmnest/master/<key>.ai). The server turns every run of characters other than
 *  letters, digits, _ . - into one "_" (safePath), so two different SKUs can name one file ("BOWLING_PIN+BALL" and "BOWLING PIN + BALL"
 *  both become BOWLING_PIN_BALL.ai) and the second upload replaced the first. The first charm in drawing order keeps the plain
 *  name; a later charm whose name collides gets "__<charm index>" after it. Returns Map(charm index → key). */
function fileKeys(items) {
  const safe = k => String(k).replace(/[^\w.\-\/]+/g, "_").replace(/\.\.+/g, ".").toLowerCase(), taken = new Set(), out = new Map();
  for (const { index, l } of items.slice().sort((a, b) => a.index - b.index)) {
    const base = l.size ? `${l.sku}__${l.size}` : l.sku; let key = base;
    if (taken.has(safe(key))) key = `${base}__${index}`;
    taken.add(safe(key)); out.set(index, key);
  }
  return out;
}

/** Whether a design looks the same in a mirror (charm-nest-pair.js symmetryOf on its first body): "symmetric" | "slight" | "directional", or undefined when the module cannot say.
 *  Stored as the index field `sym` so the Master tab can show its "faces" box only for designs that need it. */
function symLevel(Pair, c) {
  try { const b = Pair && Pair.bodiesOf ? Pair.bodiesOf(c)[0] : null; return b && Pair.symmetryOf ? Pair.symmetryOf(b).level : undefined; } catch (_) { return undefined; }
}

/** The pair layer (pairs, 9 Oct): designs a master draws as a ROW of two bodies under ONE label are built as one design. The code lives in charm-nest-pair.js
 *  (CharmNestPair.pairLayer), so this script and the server indexer (netlify/functions/charmMaster-background.js) read the rows the same way.
 *  --no-pairs leaves it out; --pair-also (a list or a file, like --only) folds the doubtful one-label rows of those SKUs too. */
function pairLayer(P, G, Pair, g, lab, items, o, log) {
  if (!Pair || !Pair.pairLayer || o.noPairs) return { rows: [], fold: new Map(), refused: new Map() };
  return Pair.pairLayer(P, G, g, lab, items, Object.assign({}, o, { pairAlso: o.pairAlso ? onlySet(o.pairAlso) : null }), log);
}

async function main(argv, log = console.log) {
  const o = args(argv);
  if (!o.file) throw new Error('usage: node scripts/index-master.cjs "<master.ai>" --origin <sorter site> [--dry] [--passcode …]');
  const stage = !!o.outDir, net = !o.dry && !stage;                      // stage: files and records go to a folder, no server is touched
  if (net && !o.origin) throw new Error("--origin is required (or use --dry to only parse and report, or --out-dir to stage the files)");
  const only = onlySet(o.only);
  const { CharmNestPDF: P, Geom: G } = require(path.join(root, "netlify/functions/_charmNestPdf.js"));
  let Pair = null; try { Pair = require(path.join(root, "charm-nest-pair.js")); } catch (_) {}   // (the pair layer; without the module the run is what it was before it)
  const buf = fs.readFileSync(o.file); const name = path.basename(o.file);
  const masterHash = crypto.createHash("sha256").update(buf).digest("hex");
  log(`${name}: ${(buf.length / 1048576).toFixed(1)} MB · sha256 ${masterHash.slice(0, 12)} · ${o.dry ? "DRY RUN (nothing written)" : stage ? "STAGING into " + o.outDir + " (no server is touched)" : "→ " + o.origin}${only ? " · only " + only.size + " SKU(s)" : ""}`);
  const workBase = o.workDir ? path.join(o.workDir, name) : stage ? path.join(o.outDir, name) : o.file;   // where .index-progress.json / .index-report.json go
  for (const d of [o.workDir, o.outDir]) if (d) fs.mkdirSync(d, { recursive: true });
  const progressPath = workBase + ".index-progress.json";
  let progress = { masterHash, done: {} };
  if (o.resume && fs.existsSync(progressPath)) { try { const p = JSON.parse(fs.readFileSync(progressPath, "utf8")); if (p.masterHash === masterHash) progress = p; } catch (_) {} }
  const saveProgress = () => { try { fs.writeFileSync(progressPath, JSON.stringify(progress)); } catch (_) {} };

  const t0 = Date.now();
  const parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), name);
  log(`parsed in ${((Date.now() - t0) / 1000).toFixed(1)} s · ${parsed.segments.length} top-level segment(s) · ${parsed.counts.text} text run(s) · page ${(parsed.pageW * MM).toFixed(0)} × ${(parsed.pageH * MM).toFixed(0)} mm`);
  const g = P.groupCharms(parsed, { minPt: o.minPt });
  const pattern = o.pattern ? new RegExp(o.pattern) : P.SKU_PATTERN_DEFAULT;
  const lab = P.labelCharms(parsed, g.charms, { pattern, gapPt: o.gapMm / MM, widen: 0.25 });
  const twins = settleTwins(lab, g.charms, pinMap(o.pin));
  log(`${g.charms.length} charm outline(s) · ${lab.labels.size} labelled (${lab.skuCount} SKU line(s)) · ${lab.unlabelled.length} unlabelled · ${lab.orphans.length} orphan label(s) · ${lab.duplicates.length} duplicate(s)${lab.undecodable.length ? ` · ${lab.undecodable.length} text run(s) unreadable (outlined or CID font without ToUnicode)` : ""}`);
  if ((g.markers || []).length) { const by = {}; for (const m of g.markers) by[m.why] = (by[m.why] || 0) + 1; log(`  ${g.markers.length} marker object(s) beside charms are left out of them (${Object.entries(by).map(([k, v]) => `${v} ${k}`).join(", ")})`); }
  if ((g.stray || []).length) { const by = {}; for (const m of g.stray) by[m.why] = (by[m.why] || 0) + 1; log(`  ${g.stray.length} piece(s) of loose ink outside a charm's cut outline are left out of it (${Object.entries(by).map(([k, v]) => `${v} ${k}`).join(", ")})`); }
  if (lab.orphans.length) log(`  orphan labels (no charm within ${o.gapMm} mm above): ${lab.orphans.slice(0, 40).map(x => x.sku).join(", ")}${lab.orphans.length > 40 ? " …" : ""}`);
  if (lab.unlabelled.length) log(`  unlabelled charms (by index): ${lab.unlabelled.slice(0, 40).join(", ")}${lab.unlabelled.length > 40 ? " …" : ""}`);
  if (lab.duplicates.length) log(`  duplicates: ${lab.duplicates.slice(0, 40).map(d => `${d.sku}/${d.also}`).join(", ")}`);
  if (twins.length) log(`  ${twins.length} SKU(s) settled: read under more than one charm and kept on one, or left to another master (${twins.filter(t => t.rule === "pinned").length} pinned, ${twins.filter(t => t.rule === "elsewhere").length} elsewhere, the rest the first on the page): ${twins.slice(0, 30).map(t => `${t.key} -> ${t.owner == null ? "elsewhere" : "#" + t.owner}`).join(", ")}${twins.length > 30 ? " …" : ""}`);

  let masterUp = null;
  if (net && o.uploadMaster) { masterUp = await upload(o.origin, o.passcode, `charmnest/master/files/${masterHash.slice(0, 12)}-${name.replace(/[^\w.\-]+/g, "_")}`, buf, "application/pdf"); log(`master stored at ${masterUp.path}`); }

  let items = [...lab.labels].map(([index, l]) => ({ index, l, c: g.charms.find(x => x.index === index) })).filter(x => x.c);
  items.sort((x, y) => x.index - y.index);                                 // drawing order: the same queue, and the same records.json, on every run
  const skusOf = l => [l.sku, ...(l.extra || []).map(x => x.sku)].filter(Boolean).map(x => String(x).toUpperCase());
  if (only) {
    const found = new Set(); items = items.filter(({ l }) => { const mine = skusOf(l); const hit = mine.some(sk => only.has(sk)); if (hit) for (const sk of mine) found.add(sk); return hit; });
    const absent = [...only].filter(sk => !found.has(sk));
    log(`--only: ${items.length} charm(s) on this sheet carry ${[...only].filter(sk => found.has(sk)).length} of the ${only.size} SKU(s) asked for${absent.length ? ` (${absent.length} not on this sheet: ${absent.slice(0, 20).join(", ")}${absent.length > 20 ? " …" : ""})` : ""}`);
  }
  const held = new Map();                                                // the library's entries for these SKUs, when it was read
  // The SKUs are read from the sheet as text, so the library is consulted before any charm is built: one whose SKUs are
  // all held already is left alone, and one with even a single new SKU is rebuilt whole so they keep sharing a file.
  const supersede = new Set(); let heldCount = 0, fileRecs = [];
  if (net) {
    if (only) {
      // only the SKUs being rebuilt are read (one read each), not the whole index
      const want = [...new Set(items.flatMap(({ l }) => skusOf(l)))];
      for (let i = 0; i < want.length; i += 300) { const r = await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterGetMany", skus: want.slice(i, i + 300) }); for (const [k, e] of Object.entries(r.entries || {})) if (e && e.sku) held.set(String(k).toUpperCase(), e); }
    } else
    // the index comes in parts (each answer says where the next starts): every part is read, or a library past the
    // first part would be taken for one without the rest
    for (let cursor = null, part = 0; ; part++) {
      if (part === 500) throw new Error("the SKU index did not end after 500 parts");
      const list = await api(o.origin, o.passcode, "charmNestLibrary", Object.assign({ op: "masterList", limit: 3000 }, cursor ? { cursor } : {}));
      for (const e of list.entries || []) if (e && e.sku) held.set(String(e.sku).toUpperCase(), e);
      if (!(cursor = list.next || null)) break;
    }
    // Whichever master a SKU belongs to now is superseded by this one, or the two would block each other as the same SKU
    // in two files. This is read even when every charm is being rebuilt, because the clash does not depend on skipping.
    const files = await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterListFiles" }); fileRecs = files.files || [];
    for (const f of files.files || []) if (f && f.masterHash && f.masterHash !== masterHash && String(f.name || "") === name) supersede.add(f.masterHash);
    const before = items.length;
    items = items.filter(({ l }) => {
      const mine = [l.sku, ...(l.extra || []).map(x => x.sku)].filter(Boolean).map(x => String(x).toUpperCase());
      for (const sk of mine) { const e = held.get(sk); if (e && e.masterHash && e.masterHash !== masterHash) supersede.add(e.masterHash); }
      if (!o.all && !only && mine.every(sk => held.has(sk))) return false;
      return true;
    });
    heldCount = before - items.length;
    if (heldCount) log(`${heldCount} charm(s) are already in the library and are left as they are · ${items.length} to index (--all rebuilds every one)`);
    if (!items.length) { log("nothing new on this sheet"); return { file: name, masterHash, charms: g.charms.length, written: 0, held: before, dry: false, at: new Date().toISOString() }; }
  }
  const entries = [], blocked = [], skus = [], kept = [];                 // kept: pair designs left as they were (see one())
  const keyOf = fileKeys(items);                                          // one file name per charm, whatever the server does to the characters
  // a row of bodies under one label that is surely a mismatched pair is built as ONE design (both bodies in its file); every other charm is the object it was
  const pairs = pairLayer(P, G, Pair, g, lab, items, o, log);
  if (pairs.fold.size) items = items.map(it => pairs.fold.has(it.index) ? Object.assign({}, it, { c: pairs.fold.get(it.index).charm }) : it);
  const rank = new WeakMap(), addEntry = (e, index) => { entries.push(e); rank.set(e, index); };   // (builds finish in any order; the records are written in drawing order)
  let done = 0, skipped = 0, hashesKept = 0; const total = items.length; const queue = items.slice();
  const one = async ({ index, l, c }) => {
    const key = keyOf.get(index), pm = pairs.fold.get(index);          // pm: set only for a design folded from a pair; every other design takes the lines it always took
    // A pair design is never written back as one body: a row that could not be folded, or a design the library holds as a pair that this run drew as one body (--no-pairs, or a changed drawing), is left as it was.
    if (!pm) {
      const why = pairs.refused.get(index) || (Pair && Pair.heldPair && [l].concat(l.extra || []).some(x => Pair.heldPair(held.get(String(x.sku).toUpperCase()), x.size)) ? "the library holds it as a pair and this run drew one body" : null);
      if (why) { log(`  ! ${l.sku}: left as it was, not written as one body (${why})`); kept.push({ sku: l.sku, why }); return; }
    }
    if (progress.done[key] && !!progress.done[key].entry.pair === !!pm) { const d = progress.done[key]; addEntry(d.entry, index); if (d.blocked) blocked.push(d.blocked); skus.push(l.sku); for (const x of l.extra || []) { addEntry(Object.assign({}, d.entry, { sku: x.sku, size: x.size }), index); skus.push(x.sku); if (d.blocked) blocked.push({ sku: x.sku, reason: d.blocked.reason }); } skipped++; return; }
    // a hoop drawn beside the body is welded into the cut line before the charm is measured or written, as the Master tab and the server route do
    if (!pm) { const r = P.integrateRings(c); if (r.left.length) log(`  ! ${l.sku}: a hoop could not join its charm: ${r.left[0]}`); }   // (a pair's bodies were welded before they were folded)
    const sil = G.silhouetteBits(pm ? pm.view : c, 6, {});
    const charmHash = P.fnv(P.signature(sil.bits, sil.w, sil.h) + "|" + Math.round((sil.bboxOuter[2] - sil.bboxOuter[0]) * 2) + "x" + Math.round((sil.bboxOuter[3] - sil.bboxOuter[1]) * 2) + "|" + c.members.length);
    const open = pm ? pm.open : (() => { const polys = G.flatten(c.outline, 12); return !polys.length || polys.some(p => Math.hypot(p[0][0] - p[p.length - 1][0], p[0][1] - p[p.length - 1][1]) > 1.5 && !c.outline.closed); })();
    let engravable = true, upAngle = null, upSource = "drawn", flipOk = true, flipWhy = null;
    if (pm) ({ engravable, upAngle, upSource, flipOk, flipWhy } = pm.engrave); else
    try { const up = G.upAngleOf(c); upAngle = up.angle; upSource = up.source; const view = G.backView(c, { res: 6, upAngle }); const mask = G.engraveMask(view, { marginMm: o.engraveMarginMm }); const r = G.largestRectangles(mask, 1)[0]; engravable = !!r && ((r.wPt * MM >= 6 && r.hPt * MM >= 3) || (r.wPt * MM >= 3 && r.hPt * MM >= 6)); }
    catch (e) { flipOk = false; flipWhy = e.message; engravable = false; }
    let aiUp = { path: `charmnest/master/${key}.ai`, url: "" }, thumb = null;
    if (!o.dry) {
      const ai = await P.buildSingleCharm(c, parsed, pm && pm.bodies ? { bodies: pm.bodies } : undefined), png = thumbnailPng(G, c, 168, P);
      if (stage) {
        const put = (rel, bytes) => { const f = path.join(o.outDir, "files", rel); fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(f, Buffer.from(bytes)); return { path: rel, url: "" }; };
        aiUp = put(`charmnest/master/${key}.ai`, ai); if (png) thumb = put(`charmnest/master/${key}.png`, png);
      } else {
        aiUp = await upload(o.origin, o.passcode, `charmnest/master/${key}.ai`, Buffer.from(ai), "application/illustrator");
        if (png) thumb = await upload(o.origin, o.passcode, `charmnest/master/${key}.png`, Buffer.from(png), "image/png");
      }
      // No thumbnail could be drawn (@resvg/resvg-js is not installed here): the entry keeps the picture it already has, because a
      // record written without one would blank thumbPath/thumbUrl (masterPutIndex merges what it is sent).
      if (!thumb) { const e = held.get(String(l.sku).toUpperCase()), t = e && (l.size ? e.sizes && e.sizes[String(l.size).toUpperCase()] : e); if (t && t.thumbPath) thumb = { path: t.thumbPath, url: t.thumbUrl || "" }; }
    }
    const reasons = []; if (open) reasons.push("open outline"); if (!flipOk) reasons.push(flipWhy);
    let hashKept = false;
    const entry = { sku: l.sku, size: l.size, charmHash, widthPt: sil.bboxOuter[2] - sil.bboxOuter[0], heightPt: sil.bboxOuter[3] - sil.bboxOuter[1], areaPt2: sil.areaPt2, members: c.members.length, holes: pm ? pm.holes : P.cutLinesOf(c).length, engravable, upAngle, upSource, aiPath: aiUp.path, aiUrl: aiUp.url, thumbPath: thumb && thumb.path, thumbUrl: thumb && thumb.url, open, labelSource: "text", confidence: 1, blocked: reasons.length ? reasons.join("; ") : null };
    if (pm) entry.pair = pm.field;                                       // { v: 1, bodies, mismatched }: only a folded pair carries it (a record without it leaves a stored one alone)
    if (Pair && Pair.entryFields) Object.assign(entry, Pair.entryFields(c, !!pm));   // facings (a folded pair drawn with the right body as the mirror image of the left: the order window reads it without the drawing; a person's words stand over it) and sym (does the design look the same in a mirror: the Master tab asks a person which way a directional one faces), the words the server indexer writes too
    if (net && only && !o.newHash) {                                   // the held record's hash stands while the drawing's geometry is the same
      const h = held.get(String(l.sku).toUpperCase()), t = h && (l.size ? (h.sizes || {})[String(l.size).toUpperCase()] : h), near = (a, b) => Math.abs(a - b) <= Math.max(0.01, 0.001 * Math.max(Math.abs(a), Math.abs(b)));
      if (t && t.charmHash && near(t.widthPt, entry.widthPt) && near(t.heightPt, entry.heightPt) && near(t.areaPt2, entry.areaPt2) && (t.holes || 0) === (entry.holes || 0)) { if (t.charmHash !== entry.charmHash) hashKept = true; entry.charmHash = t.charmHash; }
    }
    if (hashKept) hashesKept++;
    addEntry(entry, index); skus.push(l.sku); const blk = reasons.length ? { sku: l.sku, reason: reasons.join("; ") } : null; if (blk) blocked.push(blk);
    for (const x of l.extra || []) { addEntry(Object.assign({}, entry, { sku: x.sku, size: x.size }), index); skus.push(x.sku); if (blk) blocked.push({ sku: x.sku, reason: blk.reason }); }   // every further line under the charm: the same design under another SKU
    progress.done[key] = { entry, blocked: blk, extra: l.extra || [] }; done++;
    if (done % 25 === 0) { saveProgress(); log(`  ${done + skipped}/${total} · ${((Date.now() - t0) / 1000).toFixed(0)} s`); }
  };
  await Promise.all(Array.from({ length: o.dry ? 8 : o.concurrency }, async () => { while (queue.length) { const it = queue.shift(); try { await one(it); } catch (e) { log(`  ! ${it.l.sku}: ${e.message}`); blocked.push({ sku: it.l.sku, reason: "not written: " + e.message }); } } }));
  entries.sort((x, y) => (rank.get(x) || 0) - (rank.get(y) || 0)); skus.sort();
  saveProgress();
  // a small ring left loose beside a charm blocks it, as the other routes do
  for (const orp of g.orphans || []) { const b = orp.bbox; if (!b || orp.kind !== "path" || !orp.closed) continue; if (Math.max(b[2] - b[0], b[3] - b[1]) > 13) continue; const cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2; for (const c of g.charms) { if (!c.sku) continue; const ob = c.outline.bbox; if (cx < ob[0] - 4 / MM || cx > ob[2] + 4 / MM || cy < ob[1] - 4 / MM || cy > ob[3] + 4 / MM) continue; if (G.distToPolys(cx, cy, G.flatten(c.outline, 8)) <= 3 / MM) { const e = entries.find(x => x.sku === c.sku); if (e && !/detached ring/.test(e.blocked || "")) { e.blocked = (e.blocked ? e.blocked + "; " : "") + "detached ring not merged"; blocked.push({ sku: c.sku, reason: "detached ring not merged" }); } } } }
  if (hashesKept) log(`${hashesKept} design(s) keep the charm hash they have (same size, area and holes; --new-hash writes the new one)`);
  if (kept.length) log(`${kept.length} pair design(s) were NOT written, because this run drew them as one body and the library holds them as a pair: ${kept.slice(0, 40).map(k => k.sku).join(", ")}${kept.length > 40 ? " …" : ""}`);
  log(`${entries.length} SKU entr${entries.length === 1 ? "y" : "ies"} ready (${skipped} from the previous run) · ${blocked.length} blocked · ${((Date.now() - t0) / 1000).toFixed(0)} s`);

  let conflicts = [], sizeMoved = [], keptAtServer = [], written = 0;
  if (stage) {
    written = new Set(entries.map(e => String(e.sku).toUpperCase())).size;
    fs.writeFileSync(path.join(o.outDir, "records.json"), JSON.stringify({ masterHash, masterName: name, hashSource: "local", replaces: [...new Set(o.replaces)], only: only ? [...only] : null, entries, blocked }, null, 1));
    log(`staged ${written} SKU record(s) and ${new Set(entries.map(e => e.aiPath)).size} design file(s) in ${o.outDir} (records.json, files/) — nothing was sent anywhere`);
  }
  if (net) {
    written = new Set(entries.map(e => String(e.sku).toUpperCase())).size;   // one record per SKU, its sizes inside it
    // the master's stored path (the entries already name it) is kept when this run does not upload the master again
    const keepPath = masterUp ? masterUp.path : ([...held.values()].find(e => e.masterHash === masterHash && e.masterPath) || {}).masterPath || null;
    for (let i = 0; i < entries.length; i += 150) {
      const r = await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterPutIndex", entries: entries.slice(i, i + 150), masterHash, masterPath: keepPath, masterName: name, hashSource: "local", replaces: i === 0 ? [...new Set(o.replaces.concat([...supersede]))] : [] });
      conflicts = conflicts.concat(r.blocked || []); sizeMoved = sizeMoved.concat(r.sizeMoved || []); keptAtServer = keptAtServer.concat(r.pairKept || []);
      log(`  index ${Math.min(i + 150, entries.length)}/${entries.length}`);
    }
    // --only rewrites a few SKUs of a master whose file record is already there: that record (its counts and lists) is left as it is
    // when it exists, and written whole (every SKU the sheet carries) when it does not
    if (!(only && fileRecs.some(f => f && f.masterHash === masterHash))) {
      const fileSkus = only ? [...lab.labels.values()].flatMap(l => [l.sku, ...(l.extra || []).map(x => x.sku)]) : skus;
      await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterPutFile", file: { masterHash, path: masterUp ? masterUp.path : null, url: masterUp ? masterUp.url : null, name, charms: g.charms.length, labelled: lab.labels.size, unlabelled: lab.unlabelled, orphans: lab.orphans, duplicates: lab.duplicates, undecodable: lab.undecodable.length, blocked: blocked.concat(conflicts.map(b => ({ sku: b.sku, reason: b.reason }))), skus: fileSkus, replaces: o.replaces, indexedBy: "local-indexer" } });
    }
    if (keptAtServer.length) log(`  ${keptAtServer.length} SKU(s) the library holds as a pair were NOT overwritten with one body (the server refused): ${keptAtServer.slice(0, 30).map(b => b.sku).join(", ")}`);
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
      for (let i = 0; i < again.length; i += 100) await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterPutIndex", entries: again.slice(i, i + 100), masterHash, masterPath: keepPath, masterName: name, hashSource: "local", replaces: [] });
    }
  }
  log(`index written: ${written} SKU(s) on ${o.origin} — the Master tab shows them after Reload index`);
  }
  const report = { file: name, bytes: buf.length, masterHash, held: heldCount, charms: g.charms.length, labelled: lab.labels.size, unlabelled: lab.unlabelled, orphans: lab.orphans, duplicates: lab.duplicates, twins, pairs: pairs.rows, pairsKept: kept, undecodable: lab.undecodable.length, blocked, conflicts, sizeMoved, pairsKeptAtServer: keptAtServer, written, dry: o.dry, seconds: Math.round((Date.now() - t0) / 1000), at: new Date().toISOString() };
  try { fs.writeFileSync(workBase + ".index-report.json", JSON.stringify(report, null, 1)); log(`report: ${workBase}.index-report.json`); } catch (_) {}
  return report;
}
module.exports = { main, api, upload, onlySet, settleTwins, pinMap, fileKeys, thumbnailPng, pairLayer, symLevel };
if (require.main === module) main(process.argv).catch(e => { console.error("index-master:", e.message); process.exit(1); });
