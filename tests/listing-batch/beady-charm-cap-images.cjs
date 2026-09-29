"use strict";

// The picture step of the Beady Necklace charm size (see beady-charm-cap.cjs and
// netlify/functions/_beadyCharmCap.js), on made-up pictures: no network, no real
// listing images. A sizing-guide graphic (slot 3) is drawn with a charm that is
// larger than the placeholder in its template, then shrunk to the factor.
// Checked: the charm becomes `factor` of its size, everything the charm does not
// touch (title, pencil, pointer line, caption) stays byte-identical, the vacated
// area is the plain background, and anything doubtful is returned unchanged
// without throwing.
// Needs sharp (a dependency of the site); skipped where it is not installed.
// Usage: node tests/listing-batch/beady-charm-cap-images.cjs

const assert = require("node:assert/strict");
const path = require("node:path");

let sharp;
try { sharp = require("sharp"); }
catch { console.log("beady charm cap pictures: sharp is not installed here, skipped"); process.exit(0); }
const { capFlatSlot, decode, estimateBg, measure } = require(path.resolve("netlify/functions/_beadyCharmCapFlat.js"));

const W = 2048;
const BG = [252, 251, 249];

function canvas() {
  const px = Buffer.alloc(W * W * 3);
  for (let i = 0; i < W * W; i++) { const n = (i * 2654435761 >>> 0) % 3 - 1; px[i * 3] = BG[0] + n; px[i * 3 + 1] = BG[1] + n; px[i * 3 + 2] = BG[2] + n; }
  return px;
}
const set = (px, x, y, c) => { if (x >= 0 && y >= 0 && x < W && y < W) { const o = (y * W + x) * 3; px[o] = c[0]; px[o + 1] = c[1]; px[o + 2] = c[2]; } };
const rect = (px, x0, y0, x1, y1, c) => { for (let y = y0; y <= y1; y++) for (let x = x0; x <= x1; x++) set(px, x, y, c); };
const ellipse = (px, cx, cy, rx, ry, c, alpha = 1) => {
  for (let y = Math.floor(cy - ry); y <= Math.ceil(cy + ry); y++) for (let x = Math.floor(cx - rx); x <= Math.ceil(cx + rx); x++) {
    if (((x - cx) / rx) ** 2 + ((y - cy) / ry) ** 2 <= 1 && x >= 0 && y >= 0 && x < W && y < W) {
      const o = (y * W + x) * 3;
      for (let k = 0; k < 3; k++) px[o + k] = Math.round(px[o + k] * (1 - alpha) + c[k] * alpha);
    }
  }
};
// the parts of the graphic the charm never touches: title, pencil, pointer line, caption
function fixedParts(px) {
  rect(px, 700, 200, 1350, 300, [40, 40, 40]);        // title
  rect(px, 0, 640, 690, 780, [240, 190, 30]);          // pencil
  rect(px, 1022, 1000, 1025, 1200, [50, 50, 50]);      // pointer line
  rect(px, 600, 1320, 1450, 1400, [60, 60, 60]);       // caption
}
const png = (px) => sharp(px, { raw: { width: W, height: W, channels: 3 } }).png().toBuffer();
async function guide(charm) {
  const px = canvas();
  fixedParts(px);
  charm(px);
  return png(px);
}
const gold = [226, 184, 104];
const template = () => guide((px) => ellipse(px, 1024, 700, 150, 150, gold));            // the placeholder charm
const output = () => guide((px) => {                                                      // the model's larger charm
  ellipse(px, 1024, 700, 110, 140, gold);
  ellipse(px, 1024, 548, 18, 18, gold);                                                   // hoop
});

const raw = async (buf) => { const d = await sharp(buf).raw().toBuffer({ resolveWithObject: true }); return d; };

(async () => {
  const tpl = await template();
  const out = await output();
  const factor = 0.87;

  const r = await capFlatSlot(out, tpl, { slotIndex: 2, factor });
  assert.equal(r.changed, true, `shrunk (${r.reason})`);
  assert.notEqual(r.buf, out);

  // size: the charm's longest side is `factor` of what it was
  const size = async (buf) => {
    const ctx = await decode(buf); ctx.bg = estimateBg(ctx).bg;
    const m = measure(ctx, 2); assert(m.ok, m.reason);
    const b = m.boxes[0].bbox; return { w: b.x1 - b.x0 + 1, h: b.y1 - b.y0 + 1, b };
  };
  const before = await size(out), after = await size(r.buf);
  const ratio = Math.max(after.w, after.h) / Math.max(before.w, before.h);
  assert(Math.abs(ratio - factor) < 0.03, `charm ${before.w}x${before.h} -> ${after.w}x${after.h} (ratio ${ratio.toFixed(3)}, wanted ${factor})`);
  // centred on the same point, as the sizing guide requires
  const cx = (b) => (b.x0 + b.x1) / 2, cy = (b) => (b.y0 + b.y1) / 2;
  assert(Math.abs(cx(after.b) - cx(before.b)) <= 2 && Math.abs(cy(after.b) - cy(before.b)) <= 2, "same centre");

  // nothing else changes: title, pencil, pointer line, caption are byte-identical
  const a = await raw(out), b2 = await raw(r.buf);
  assert.equal(b2.info.width, W); assert.equal(b2.info.height, W);
  const same = (x0, y0, x1, y1) => { for (let y = y0; y <= y1; y++) for (let x = x0; x <= x1; x++) { const o = (y * W + x) * b2.info.channels; for (let k = 0; k < 3; k++) if (a.data[o + k] !== b2.data[o + k]) return false; } return true; };
  assert(same(0, 0, W - 1, 480), "title area untouched");
  assert(same(0, 620, 690, 800), "pencil untouched");
  assert(same(1010, 990, 1037, 1210), "pointer line untouched");
  assert(same(0, 1260, W - 1, W - 1), "caption untouched");
  // the area the old charm left is the plain background (nothing of the old outline)
  const px = (x, y) => { const o = (y * W + x) * b2.info.channels; return [b2.data[o], b2.data[o + 1], b2.data[o + 2]]; };
  for (const [x, y] of [[920, 700], [1130, 700], [1024, 850], [1034, 860]]) {
    assert(px(x, y).every((v, k) => Math.abs(v - BG[k]) <= 3), `old charm area at ${x},${y} is background: ${px(x, y)}`);
  }
  // and the shrunk charm is drawn where it should be
  assert(px(1024, 700).every((v, k) => Math.abs(v - gold[k]) <= 6), "charm body colour kept");

  // doubtful input is returned unchanged, never thrown
  const unchanged = async (label, res, input) => { assert.equal(res.changed, false, label); assert.equal(res.buf, input, `${label}: same buffer`); assert(res.reason, label); };
  await unchanged("not a flat slot", await capFlatSlot(out, tpl, { slotIndex: 0, factor }), out);
  await unchanged("factor 1.2", await capFlatSlot(out, tpl, { slotIndex: 2, factor: 1.2 }), out);
  await unchanged("factor 1", await capFlatSlot(out, tpl, { slotIndex: 2, factor: 1 }), out);
  const junk = Buffer.from("not a picture at all");
  await unchanged("garbage picture", await capFlatSlot(junk, tpl, { slotIndex: 2, factor }), junk);
  await unchanged("garbage template", await capFlatSlot(out, junk, { slotIndex: 2, factor }), out);
  await unchanged("no template", await capFlatSlot(out, null, { slotIndex: 2, factor }), out);
  await unchanged("no picture", await capFlatSlot(null, tpl, { slotIndex: 2, factor }), null);
  const blank = await guide(() => {});
  await unchanged("no charm drawn", await capFlatSlot(blank, tpl, { slotIndex: 2, factor }), blank);
  const noPointer = await (async () => { const p = canvas(); rect(p, 700, 200, 1350, 300, [40, 40, 40]); rect(p, 0, 640, 690, 780, [240, 190, 30]); rect(p, 600, 1320, 1450, 1400, [60, 60, 60]); ellipse(p, 1024, 700, 110, 140, gold); return png(p); })();
  await unchanged("no pointer line", await capFlatSlot(noPointer, tpl, { slotIndex: 2, factor }), noPointer);
  const wrongLayout = await guide((p) => { ellipse(p, 1024, 700, 110, 140, gold); rect(p, 0, 0, 600, 480, [10, 10, 200]); });
  await unchanged("title area changed (not this template)", await capFlatSlot(wrongLayout, tpl, { slotIndex: 2, factor }), wrongLayout);
  const tooBig = await guide((p) => { ellipse(p, 1024, 700, 400, 400, gold); });
  await unchanged("charm far too large for the frame", await capFlatSlot(tooBig, tpl, { slotIndex: 2, factor }), tooBig);
  // the sizing-guide picture is not a back-engraving graphic
  await unchanged("wrong graphic for slot 4", await capFlatSlot(out, tpl, { slotIndex: 3, factor }), out);
  const small = await sharp(out).resize(400, 400).png().toBuffer();
  await unchanged("small picture", await capFlatSlot(small, tpl, { slotIndex: 2, factor }), small);
})().then(() => {
  console.log("beady charm cap pictures: charm shrinks to the factor, the rest is byte-identical, doubtful input is returned unchanged");
}).catch((e) => { console.error(e); process.exit(1); });
