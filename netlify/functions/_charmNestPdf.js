/*  netlify/functions/_charmNestPdf.js
 *  The browser's parser/writer (charm-nest-pdf.js) and geometry (charm-nest-geom.js), loaded once for node. The
 *  parser expects `window.PDFLib`; here that is the same vendored pdf-lib the page uses, so a master file indexed on
 *  the server is parsed by byte-identical code. No canvas: silhouettes and thumbnails come from charm-nest-geom.
 *  withCanvas() (cloud plan v2, step 3) is the exception, asked for by name: it puts the page's canvas in place for node
 *  (_charmNestCanvas.js, as OffscreenCanvas; charm-nest-pdf.js looks for it each time it draws), so the cloud's piece maker
 *  (charm-nest-pool-make.js) traces silhouettes with the page's own buildSilhouettes. Nothing that runs today calls it.  */
"use strict";
const g = typeof globalThis !== "undefined" ? globalThis : global;
if (!g.window) g.window = g;
if (!g.PDFLib) g.PDFLib = require("../../vendor/pdf-lib-1.17.1.min.js");
if (!g.CharmNestPDF) require("../../charm-nest-pdf.js");
const Geom = require("../../charm-nest-geom.js");
/** The page's canvas for node, put in place (once) for every later draw; o.backend: "resvg" | "js" | "auto" (_charmNestCanvas.js). → this module. */
function withCanvas(o) { require("./_charmNestCanvas.js").install(g, o); return module.exports; }
module.exports = { CharmNestPDF: g.CharmNestPDF, Geom, PDFLib: g.PDFLib, withCanvas };
