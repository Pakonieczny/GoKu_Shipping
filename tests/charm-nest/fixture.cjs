// Builds a synthetic "flat Illustrator page" fixture: N charm outlines (closed,
// near-black, stroked) with coloured inner detail, engraved text, one form
// XObject holding a red stroke, and a page-sized red frame. Mirrors the
// structure of the reference file in the design document.
//   node tests/charm-nest/fixture.cjs <out.ai> [count]
const fs = require('fs');
const path = require('path');
const { PDFDocument, PDFName, rgb, StandardFonts } = require(path.join(__dirname, '../../vendor/pdf-lib-1.17.1.min.js'));

async function build(outPath, count = 18) {
  const doc = await PDFDocument.create();
  const W = 513.03, H = 434.19;
  const page = doc.addPage([W, H]);
  const font = await doc.embedFont(StandardFonts.Helvetica);
  const ops = [];
  const num = n => (+n).toFixed(3);
  const rnd = (() => { let a = 12345; return () => { a = (a * 1103515245 + 12345) & 0x7fffffff; return a / 0x7fffffff; }; })();
  // page frame (red, like a sheet outline drawn into the artwork)
  ops.push(`q 1 0 0 RG 0.5 w 0 0 ${num(W)} ${num(H)} re S Q`);
  const kinds = ['heart', 'circle', 'tag', 'star', 'moon', 'shield'];
  const charms = [];
  let placedBoxes = [];
  for (let i = 0; i < count; i++) {
    const kind = kinds[i % kinds.length];
    const size = 34 + rnd() * 62;
    let cx, cy, tries = 0;
    do { cx = 30 + rnd() * (W - 60); cy = 30 + rnd() * (H - 60); tries++; }
    while (tries < 300 && placedBoxes.some(b => Math.abs(b[0] - cx) < (b[2] + size) / 2 + 6 && Math.abs(b[1] - cy) < (b[2] + size) / 2 + 6));
    placedBoxes.push([cx, cy, size]);
    charms.push({ kind, cx, cy, size });
    ops.push(outline(kind, cx, cy, size, num));
    // inner detail: coloured fill, a tiny hole ring, engraved text
    ops.push(`q 0.85 0.72 0.42 rg ${num(cx - size * 0.12)} ${num(cy - size * 0.1)} ${num(size * 0.24)} ${num(size * 0.2)} re f Q`);
    // jump ring: a small closed black circle sitting on the body's top edge, like real artwork
    const top = { heart: 0.39, circle: 0.5, tag: 0.5, star: 0.5, moon: 0.5, shield: 0.4 }[kind] * size;
    const rx = kind === 'moon' ? cx + size * 0.15 : kind === 'heart' ? cx - size * 0.31 : cx;   // heart: on a lobe, not in the notch
    const ry = kind === 'heart' ? cy + size * 0.283 + size * 0.05 : cy + top + size * 0.05;
    ops.push(`q 0 0 0 RG 0.5 w ${circle(rx, ry, size * 0.06, num)} S Q`);
  }
  // one Form XObject with a red stroke — the reference file had exactly this
  const c0 = charms[0];
  const formContent = `q 1 0 0 RG 0.6 w ${num(c0.cx - c0.size * 0.2)} ${num(c0.cy)} m ${num(c0.cx + c0.size * 0.2)} ${num(c0.cy)} l S Q`;
  const form = doc.context.flateStream(formContent, { Type: 'XObject', Subtype: 'Form', BBox: [0, 0, W, H], Matrix: [1, 0, 0, 1, 0, 0] });
  const formRef = doc.context.register(form);
  page.node.normalize();
  const key = page.node.newXObject('Fm0', formRef);
  ops.push(`q /${key.encodedName.slice(1)} Do Q`);
  // write the raw ops as the page content, followed by text drawn via pdf-lib
  const raw = ops.join('\n') + '\n';
  const stream = doc.context.flateStream(raw);
  const ref = doc.context.register(stream);
  page.node.set(PDFName.of('Contents'), doc.context.obj([ref]));
  for (const c of charms.slice(0, 6)) page.drawText('9.26.25', { x: c.cx - 9, y: c.cy - c.size * 0.32, size: 4.5, font, color: rgb(0.1, 0.1, 0.1) });
  const bytes = await doc.save({ useObjectStreams: false });
  fs.writeFileSync(outPath, bytes);
  return { charms, bytes: bytes.length };
}
function circle(cx, cy, r, num) {
  const k = 0.5523 * r;
  return `${num(cx + r)} ${num(cy)} m ${num(cx + r)} ${num(cy + k)} ${num(cx + k)} ${num(cy + r)} ${num(cx)} ${num(cy + r)} c ` +
    `${num(cx - k)} ${num(cy + r)} ${num(cx - r)} ${num(cy + k)} ${num(cx - r)} ${num(cy)} c ` +
    `${num(cx - r)} ${num(cy - k)} ${num(cx - k)} ${num(cy - r)} ${num(cx)} ${num(cy - r)} c ` +
    `${num(cx + k)} ${num(cy - r)} ${num(cx + r)} ${num(cy - k)} ${num(cx + r)} ${num(cy)} c h`;
}
function outline(kind, cx, cy, s, num) {
  const r = s / 2; let p = '';
  if (kind === 'circle') p = circle(cx, cy, r, num);
  else if (kind === 'heart') p = `${num(cx)} ${num(cy - r)} m ${num(cx - r * 1.4)} ${num(cy + r * 0.2)} ${num(cx - r * 0.5)} ${num(cy + r * 1.1)} ${num(cx)} ${num(cy + r * 0.45)} c ${num(cx + r * 0.5)} ${num(cy + r * 1.1)} ${num(cx + r * 1.4)} ${num(cy + r * 0.2)} ${num(cx)} ${num(cy - r)} c h`;
  else if (kind === 'tag') p = `${num(cx - r * 0.6)} ${num(cy - r)} m ${num(cx + r * 0.6)} ${num(cy - r)} l ${num(cx + r * 0.6)} ${num(cy + r * 0.5)} l ${num(cx)} ${num(cy + r)} l ${num(cx - r * 0.6)} ${num(cy + r * 0.5)} l h`;
  else if (kind === 'star') { const pts = []; for (let i = 0; i < 10; i++) { const a = Math.PI / 2 + i * Math.PI / 5, rr = i % 2 ? r * 0.45 : r; pts.push([cx + Math.cos(a) * rr, cy + Math.sin(a) * rr]); } p = pts.map((q, i) => `${num(q[0])} ${num(q[1])} ${i ? 'l' : 'm'}`).join(' ') + ' h'; }
  else if (kind === 'moon') p = `${num(cx + r * 0.3)} ${num(cy + r)} m ${num(cx - r * 1.2)} ${num(cy + r * 0.6)} ${num(cx - r * 1.2)} ${num(cy - r * 0.6)} ${num(cx + r * 0.3)} ${num(cy - r)} c ${num(cx - r * 0.4)} ${num(cy - r * 0.5)} ${num(cx - r * 0.4)} ${num(cy + r * 0.5)} ${num(cx + r * 0.3)} ${num(cy + r)} c h`;
  else p = `${num(cx - r)} ${num(cy + r * 0.8)} m ${num(cx + r)} ${num(cy + r * 0.8)} l ${num(cx + r)} ${num(cy - r * 0.2)} ${num(cx)} ${num(cy - r)} ${num(cx)} ${num(cy - r)} c ${num(cx)} ${num(cy - r)} ${num(cx - r)} ${num(cy - r * 0.2)} ${num(cx - r)} ${num(cy + r * 0.8)} c h`;
  return `q 0.05 0.05 0.05 RG 0.5 w ${p} S Q`;
}
module.exports = { build };
if (require.main === module) build(process.argv[2] || 'TEST-sheet.ai', +process.argv[3] || 18).then(r => console.log('wrote', r.charms.length, 'charms', r.bytes, 'bytes'));
