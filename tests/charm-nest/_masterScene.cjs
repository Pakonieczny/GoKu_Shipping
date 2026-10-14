// A synthetic master page for the indexing tests (MASTER2): bodies, rings and labels placed by number, so a test can lay out exactly the sheet it needs
// (a pair under one label, a label that pokes into its charm, a ring beside a body, a label split in two runs ...). Offline: pdf-lib only.
//   const { scene } = require('./_masterScene.cjs'); const bytes = await scene({ page: [420, 300], bodies: [...], rings: [...], labels: [...] });
// bodies: { shape: 'house' | 'notch' | 'bar' | 'blob', x, y, w, h, ink: 'r g b' (a coloured detail, so two bodies can differ), hole: true (a hanging hole), id }
// rings:  { cx, cy, r, inner: r2, third: r3 }   (a hoop drawn as two or three concentric circles)
// tj: [{ parts: ['Hello', -400, 'World_1'], x, y, size }]   extraPages: n blank pages after the first
// labels: { text, cx, top, size, runs: [text, ...] (a label written in several runs, left to right) , font: 'Helvetica' | 'Helvetica-Bold' | 'Courier' }   top = the text box's top edge (pt)
'use strict';
const path = require('path');
const { PDFDocument, PDFName, rgb, StandardFonts } = require(path.join(__dirname, '../../vendor/pdf-lib-1.17.1.min.js'));
const f = v => (+v).toFixed(3);
const poly = pts => pts.map((p, i) => `${f(p[0])} ${f(p[1])} ${i ? 'l' : 'm'}`).join(' ') + ' h';
const circle = (cx, cy, r) => { const k = 0.5523 * r; return `${f(cx + r)} ${f(cy)} m ${f(cx + r)} ${f(cy + k)} ${f(cx + k)} ${f(cy + r)} ${f(cx)} ${f(cy + r)} c ${f(cx - k)} ${f(cy + r)} ${f(cx - r)} ${f(cy + k)} ${f(cx - r)} ${f(cy)} c ${f(cx - r)} ${f(cy - k)} ${f(cx - k)} ${f(cy - r)} ${f(cx)} ${f(cy - r)} c ${f(cx + k)} ${f(cy - r)} ${f(cx + r)} ${f(cy - k)} ${f(cx + r)} ${f(cy)} c h`; };
const SHAPES = {
  house: (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + .7 * h], [x + w / 2, y + h], [x, y + .7 * h]],
  notch: (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + h], [x + .3 * w, y + h], [x, y + .6 * h]],
  bar: (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + h], [x, y + h]],
  blob: (x, y, w, h) => [[x + .2 * w, y], [x + .8 * w, y], [x + w, y + .5 * h], [x + .6 * w, y + h], [x + .1 * w, y + .8 * h], [x, y + .3 * h]]
};
async function scene(spec) {
  const doc = await PDFDocument.create(), [W, H] = spec.page || [420, 300], page = doc.addPage([W, H]);
  const fonts = {}; for (const n of ['Helvetica', 'Helvetica-Bold', 'Courier']) fonts[n] = await doc.embedFont(StandardFonts[n === 'Helvetica-Bold' ? 'HelveticaBold' : n]);
  const ops = [];
  for (const b of spec.bodies || []) {
    const pts = (SHAPES[b.shape || 'house'])(b.x, b.y, b.w, b.h);
    ops.push(`q 0.05 0.05 0.05 RG 0.5 w ${poly(pts)} S Q`);
    ops.push(`q ${b.ink || '0.2 0.35 0.85'} rg ${f(b.x + b.w * .3)} ${f(b.y + b.h * .2)} ${f(b.w * .3)} ${f(b.h * .2)} re f Q`);
    if (b.hole !== false) ops.push(`q 0 0 0 RG 0.4 w ${circle(b.x + b.w * .7, b.y + b.h * .75 - (b.shape === 'bar' ? b.h * .25 : 0), Math.min(2.4, b.h * .12))} S Q`);
  }
  for (const r of spec.rings || []) { ops.push(`q 0 0 0 RG 0.4 w ${circle(r.cx, r.cy, r.r)} ${r.inner ? circle(r.cx, r.cy, r.inner) : ''} ${r.third ? circle(r.cx, r.cy, r.third) : ''} S Q`); }
  for (const x of spec.extraOps || []) ops.push(x);
  // tj: [{ parts: ['Hello', -400, 'World_1'], x, y, size }] a text-array operator (kerning and word gaps are numbers between the strings)
  for (const t of spec.tj || []) { page.setFont(fonts.Helvetica); const fk = page.fontKey.toString(); ops.push(`BT ${fk} ${t.size || 6} Tf ${t.x} ${t.y} Td [${t.parts.map(p => typeof p === 'number' ? String(p) : '(' + p + ')').join(' ')}] TJ ET`); }
  for (let i = 0; i < (spec.extraPages || 0); i++) doc.addPage([W, H]);
  page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(ops.join('\n') + '\n'))]));
  for (const l of spec.labels || []) {
    const size = l.size || 6, font = fonts[l.font || 'Helvetica'], runs = l.runs || [l.text], gapW = font.widthOfTextAtSize(' ', size);
    const total = runs.reduce((n, t) => n + font.widthOfTextAtSize(t, size), 0) + gapW * (runs.length - 1);
    let x = l.cx - total / 2; const y = l.top - size * .75;
    for (const t of runs) { page.drawText(t, { x, y, size, font, color: rgb(.1, .1, .1) }); x += font.widthOfTextAtSize(t, size) + gapW + (l.runGap || 0); }
  }
  return Buffer.from(await doc.save({ useObjectStreams: false }));
}
module.exports = { scene, SHAPES };
