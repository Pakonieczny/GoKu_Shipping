// A synthetic MASTER Illustrator page: N charm outlines (closed, near-black, stroked), each with a hanging hole (a small
// closed black circle inside the outline near its top edge), coloured front detail, and its SKU written as text directly
// under it — plus, optionally, the edge cases §15 of the design document asks for: a label 8 mm below (too far), a
// label shared between two outlines, an unlabelled charm, a duplicate label, and a size suffix.
//   node tests/charm-nest/fixture-master.cjs <out.ai> [count]
const fs = require('fs');
const path = require('path');
const { PDFDocument, PDFName, rgb, StandardFonts } = require(path.join(__dirname, '../../vendor/pdf-lib-1.17.1.min.js'));
const MM = 72 / 25.4;

function circle(cx, cy, r, num) { const k = 0.5523 * r; return `${num(cx + r)} ${num(cy)} m ${num(cx + r)} ${num(cy + k)} ${num(cx + k)} ${num(cy + r)} ${num(cx)} ${num(cy + r)} c ${num(cx - k)} ${num(cy + r)} ${num(cx - r)} ${num(cy + k)} ${num(cx - r)} ${num(cy)} c ${num(cx - r)} ${num(cy - k)} ${num(cx - k)} ${num(cy - r)} ${num(cx)} ${num(cy - r)} c ${num(cx + k)} ${num(cy - r)} ${num(cx + r)} ${num(cy - k)} ${num(cx + r)} ${num(cy)} c h`; }
/** Asymmetric outlines, so a wrong flip is visible: a tag, a crescent-ish blob, a shield with a notch, a house. */
function outline(kind, cx, cy, s, num) {
  const r = s / 2; let p = '';
  if (kind === 'tag') p = `${num(cx - r * 0.7)} ${num(cy - r)} m ${num(cx + r * 0.5)} ${num(cy - r)} l ${num(cx + r)} ${num(cy - r * 0.3)} l ${num(cx + r * 0.5)} ${num(cy + r)} l ${num(cx - r * 0.7)} ${num(cy + r)} l h`;
  else if (kind === 'blob') p = `${num(cx - r)} ${num(cy)} m ${num(cx - r)} ${num(cy + r * 1.2)} ${num(cx + r * 0.2)} ${num(cy + r * 1.1)} ${num(cx + r * 0.6)} ${num(cy + r * 0.4)} c ${num(cx + r * 1.1)} ${num(cy - r * 0.2)} ${num(cx + r * 0.3)} ${num(cy - r)} ${num(cx - r * 0.2)} ${num(cy - r * 0.8)} c ${num(cx - r * 0.9)} ${num(cy - r * 0.6)} ${num(cx - r)} ${num(cy - r * 0.4)} ${num(cx - r)} ${num(cy)} c h`;
  else if (kind === 'shield') p = `${num(cx - r)} ${num(cy + r * 0.8)} m ${num(cx + r)} ${num(cy + r * 0.8)} l ${num(cx + r)} ${num(cy - r * 0.2)} ${num(cx + r * 0.3)} ${num(cy - r)} ${num(cx)} ${num(cy - r)} c ${num(cx - r * 0.2)} ${num(cy - r)} ${num(cx - r)} ${num(cy - r * 0.2)} ${num(cx - r)} ${num(cy + r * 0.3)} c ${num(cx - r * 0.6)} ${num(cy + r * 0.3)} l ${num(cx - r * 0.6)} ${num(cy + r * 0.8)} l h`;
  else p = `${num(cx - r)} ${num(cy - r)} m ${num(cx + r)} ${num(cy - r)} l ${num(cx + r)} ${num(cy + r * 0.3)} l ${num(cx + r * 0.2)} ${num(cy + r)} l ${num(cx - r)} ${num(cy + r * 0.2)} l h`;
  return `q 0.05 0.05 0.05 RG 0.5 w ${p} S Q`;
}

/**
 * opts: { count, labelled: true, edge: true, scale: 1 } → { charms:[{sku, size, kind, cx, cy, s, labelled}], bytes }
 * The SKUs are BR-TST-01 … and the page is 300 × 250 mm-ish so the charms have room; sizes 10–18 mm, times scale.
 */
async function buildMaster(outPath, opts = {}) {
  const layered = !!opts.layered;   // bodies first, rings after — how a master drawn in Illustrator layers reaches the parser
  const count = opts.count || 8, edge = opts.edge !== false;
  const doc = await PDFDocument.create();
  const W = 300 * MM, H = 220 * MM;
  const page = doc.addPage([W, H]);
  const font = await doc.embedFont(StandardFonts.Helvetica);
  const ops = []; const num = n => (+n).toFixed(3);
  const kinds = ['tag', 'blob', 'shield', 'house'];
  const charms = []; const later = [];
  const cols = 5, gapX = W / cols, gapY = 55 * MM;
  for (let i = 0; i < count; i++) {
    const kind = kinds[i % kinds.length]; const s = (10 + (i % 5) * 2) * MM * (+opts.scale || 1);
    const cx = gapX * (i % cols) + gapX / 2, cy = H - 40 * MM - Math.floor(i / cols) * gapY;
    const sku = `BR-TST-${String(i + 1).padStart(2, '0')}`;
    ops.push(outline(kind, cx, cy, s, num));
    // front detail: a coloured fill (must never reach the back) and a red engraving-style stroke
    ops.push(`q 0.2 0.35 0.85 rg ${num(cx - s * 0.15)} ${num(cy - s * 0.12)} ${num(s * 0.25)} ${num(s * 0.18)} re f Q`);
    ops.push(`q 0.9 0.1 0.1 RG 0.4 w ${num(cx - s * 0.2)} ${num(cy - s * 0.3)} m ${num(cx + s * 0.15)} ${num(cy - s * 0.3)} l S Q`);
    // the hanging hole: a closed black circle inside the outline near its top, off-centre so the flip is visible
    const hx = cx + s * 0.18, hy = cy + s * 0.3;
    const holeOp = `q 0 0 0 RG 0.4 w ${circle(hx, hy, s * 0.06, num)} S Q`;
    // a layered master writes every body first and every ring after (Illustrator layers): the ring's stream neighbours are
    // other rings, never its own body. It also hangs a jump ring off the top of the outline, touching it from outside.
    const jumpOp = `q 0 0 0 RG 0.4 w ${circle(cx, cy + s / 2 + s * 0.05, s * 0.055, num)} S Q`;
    if (layered) later.push(holeOp, jumpOp); else ops.push(holeOp);
    charms.push({ sku, size: null, kind, cx, cy, s, hole: [hx, hy], labelled: true });
  }
  if (layered) ops.push(...later);
  const raw = ops.join('\n') + '\n';
  const stream = doc.context.flateStream(raw); const ref = doc.context.register(stream);
  page.node.set(PDFName.of('Contents'), doc.context.obj([ref]));
  // labels: SKU text directly under each charm (top edge ≈ 2 mm below the outline's bottom edge)
  const labelAt = (c, text, dyMm) => { const size = 6; const w = font.widthOfTextAtSize(text, size); page.drawText(text, { x: c.cx - w / 2, y: c.cy - c.s / 2 - (dyMm || 2) * MM - size * 0.75, size, font, color: rgb(0.1, 0.1, 0.1) }); };
  charms.forEach((c, i) => {
    if (!edge) return labelAt(c, c.sku);
    if (i === 1) { c.size = 'S'; return labelAt(c, `${c.sku} · S`); }          // size suffix
    if (i === 2) { c.labelled = false; c.tooFar = true; return labelAt(c, c.sku, 8); } // 8 mm below: too far, becomes an orphan label
    if (i === 3) { c.labelled = false; return; }                                   // unlabelled
    if (i === 4) { labelAt(c, c.sku); c.duplicate = true; return labelAt(c, 'BR-DUP-05', 4); } // two labels under one charm
    return labelAt(c, c.sku);
  });
  const bytes = await doc.save({ useObjectStreams: false });
  if (outPath) fs.writeFileSync(outPath, bytes);
  return { charms, bytes, W, H };
}
module.exports = { buildMaster };
if (require.main === module) buildMaster(process.argv[2] || 'TEST-master.ai', { count: +process.argv[3] || 8 }).then(r => console.log('wrote', r.charms.length, 'charms', r.bytes.length, 'bytes'));
