/*  netlify/functions/_charmNestPdf.js
 *  The browser's parser/writer (charm-nest-pdf.js) and geometry (charm-nest-geom.js), loaded once for node. The
 *  parser expects `window.PDFLib`; here that is the same vendored pdf-lib the page uses, so a master file indexed on
 *  the server is parsed by byte-identical code. No canvas: silhouettes and thumbnails come from charm-nest-geom.    */
"use strict";
const g = typeof globalThis !== "undefined" ? globalThis : global;
if (!g.window) g.window = g;
if (!g.PDFLib) g.PDFLib = require("../../vendor/pdf-lib-1.17.1.min.js");
if (!g.CharmNestPDF) require("../../charm-nest-pdf.js");
const Geom = require("../../charm-nest-geom.js");
module.exports = { CharmNestPDF: g.CharmNestPDF, Geom, PDFLib: g.PDFLib };
