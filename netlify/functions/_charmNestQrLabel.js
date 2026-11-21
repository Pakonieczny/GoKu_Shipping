/*  netlify/functions/_charmNestQrLabel.js — a sheet's QR label (145 × 145 pt) drawn in node, as the page's Sets.renderLabelPng (charm-nest-bridge.js ~15840) draws it on a canvas
 *  (cloud plan v2 step 10a, designs.json "QR labels" gaps 1-2).
 *
 *    the QR       lib/qrcode.min.js, THE PAGE'S OWN ENCODER, run in a node vm with a two-property shim (document.documentElement.tagName "svg", navigator.userAgent): a QRCode is
 *                 made with Object.create(QRCode.prototype) and a drawing that draws nothing, makeCode runs the library's own model, and the modules are read with
 *                 _oQRCode.getModuleCount() / isDark(r, c). ECC M, and L when the text is too long for M: exactly the page's try/catch. Same version, same modules.
 *    the words    the label in 9 pt bold at (1, 93) and "Notes:" at (92, 0.5), the notes in 6.5 pt at (1, 104 + 9i), at most four: drawn as outlines from
 *                 netlify/functions/fonts/OpenSans-Bold.ttf / OpenSans-Regular.ttf (opentype.js, the page's own vendored copy), so nothing depends on a font being installed.
 *                 A line wider than 143 pt is squeezed to 143 pt (canvas fillText's maxWidth). textBaseline "top": the em box's top at the given y.
 *    the PNG      the SVG rasterised by @resvg/resvg-js at 8× (1160 × 1160 px), the page's scale.
 *  Pure apart from reading its three files; deterministic: the same payload, label and notes give the same bytes.                                                              */
"use strict";
const fs = require("fs"), path = require("path"), vm = require("vm");

const W = 145, QR_AT = 3, QR_PT = 85, SCALE = 8;
const firstFile = list => list.find(f => { try { return fs.statSync(f).isFile(); } catch (_) { return false; } }) || null;
const near = rel => [path.join(__dirname, rel), path.join(__dirname, "..", "..", rel), path.join(process.cwd(), rel), path.join(process.env.LAMBDA_TASK_ROOT || "/var/task", rel)];

let QR = null;
/** The page's QRCode, loaded once into a vm context with the shim it reads at load (it picks its SVG drawer, which is never asked to draw). */
function qrLib() {
  if (QR) return QR;
  const file = firstFile(near("lib/qrcode.min.js").concat([path.join(__dirname, "lib", "qrcode.min.js")]));
  if (!file) throw new Error("The QR encoder (lib/qrcode.min.js) is not in this function's files: the label cannot be made");
  const stub = () => ({ style: {}, setAttribute() {}, setAttributeNS() {}, appendChild() {}, removeChild() {}, hasChildNodes: () => false, getContext: () => null });
  const box = { document: { documentElement: { tagName: "svg" }, createElementNS: stub, createElement: stub, getElementById: () => null }, navigator: { userAgent: "node" }, console };
  vm.createContext(box);
  vm.runInContext(fs.readFileSync(file, "utf8") + "\n;this.QRCode = QRCode;", box, { filename: "qrcode.min.js" });
  if (typeof box.QRCode !== "function" || !box.QRCode.CorrectLevel) throw new Error("The QR encoder did not load");
  QR = box.QRCode;
  return QR;
}
/** The QR modules of a text at a level ("M" | "L"): { n, dark: [[0|1]] }. Throws as the library throws (too long for the level). */
function modules(text, level) {
  const Q = qrLib(), q = Object.create(Q.prototype);
  q._htOption = { width: 256, height: 256, typeNumber: 4, colorDark: "#000000", colorLight: "#ffffff", correctLevel: Q.CorrectLevel[level] };
  q._el = {}; q._android = false; q._oQRCode = null; q._oDrawing = { draw() {}, clear() {} };
  q.makeCode(String(text));
  const m = q._oQRCode, n = m.getModuleCount(), dark = [];
  for (let r = 0; r < n; r++) { const row = new Array(n); for (let c = 0; c < n; c++) row[c] = m.isDark(r, c) ? 1 : 0; dark.push(row); }
  return { n, dark };
}
/** The page's try: ECC M, else L. → { n, dark, ecc } */
function qr(text) { try { return Object.assign(modules(text, "M"), { ecc: "M" }); } catch (_) { return Object.assign(modules(text, "L"), { ecc: "L" }); } }

let FONTS = null;
function fonts() {
  if (FONTS) return FONTS;
  const opentype = require("../../vendor/opentype-1.3.4.min.js");
  const load = name => { const f = firstFile(near(`netlify/functions/fonts/${name}`).concat(near(`fonts/${name}`), [path.join(__dirname, "fonts", name), path.join(__dirname, "..", "production-functions", "fonts", name)])); if (!f) throw new Error(`The label's font (${name}) is not in this function's files`); const b = fs.readFileSync(f); return opentype.parse(b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength)); };
  FONTS = { bold: load("OpenSans-Bold.ttf"), regular: load("OpenSans-Regular.ttf") };
  return FONTS;
}
const esc = s => String(s).replace(/[<>&"]/g, c => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;", '"': "&quot;" }[c]));
/** One line of words as an outline at (x, top) in `size` pt, squeezed to maxWidth like canvas fillText. → an SVG element */
function words(font, text, x, top, size, maxWidth) {
  const t = String(text == null ? "" : text); if (!t) return "";
  const em = font.ascender - font.descender, base = top + size * (em > 0 ? font.ascender / em : 0.8);
  const width = font.getAdvanceWidth(t, size), squeeze = maxWidth && width > maxWidth ? maxWidth / width : 1;
  const d = font.getPath(t, 0, 0, size).toPathData(3);
  if (!d) return "";
  return `<path fill="#000" transform="translate(${x} ${+base.toFixed(4)})${squeeze < 1 ? ` scale(${+squeeze.toFixed(6)} 1)` : ""}" d="${esc(d)}"/>`;
}
/** The label as SVG (145 × 145 user units, one per point). → { svg, ecc } */
function labelSvg(payload, label, notes) {
  const code = qr(payload), F = fonts(), k = QR_PT / code.n;
  let qrPath = "";
  for (let r = 0; r < code.n; r++) for (let c = 0; c < code.n; c++) if (code.dark[r][c]) qrPath += `M${c} ${r}h1v1h-1z`;
  const parts = [`<rect width="${W}" height="${W}" fill="#fff"/>`, `<path fill="#000" shape-rendering="crispEdges" transform="translate(${QR_AT} ${QR_AT}) scale(${+k.toFixed(8)})" d="${qrPath}"/>`,
    words(F.bold, label, 1, 93, 9, 143), words(F.bold, "Notes:", 92, 0.5, 9, 0)];
  (Array.isArray(notes) ? notes : []).slice(0, 4).forEach((t, i) => parts.push(words(F.regular, t, 1, 104 + i * 9, 6.5, 143)));
  return { svg: `<svg xmlns="http://www.w3.org/2000/svg" width="${W * SCALE}" height="${W * SCALE}" viewBox="0 0 ${W} ${W}">${parts.join("")}</svg>`, ecc: code.ecc, modules: code.n };
}
let RESVG = null;
/** The label as a PNG at the page's scale (1160 px). → { png: Buffer, ecc } */
function renderPng(o) {
  const { svg, ecc, modules: n } = labelSvg(o.payload, o.label, o.notes);
  if (!RESVG) { try { RESVG = require("@resvg/resvg-js"); } catch (e) { throw new Error("The label renderer (@resvg/resvg-js) did not load: " + ((e && e.message) || e)); } }
  const png = new RESVG.Resvg(svg, { fitTo: { mode: "width", value: W * SCALE }, font: { loadSystemFonts: false }, background: "#ffffff" }).render().asPng();
  return { png: Buffer.from(png), ecc, modules: n, svg };
}

module.exports = { qr, modules, labelSvg, renderPng, qrLib, W, SCALE };
