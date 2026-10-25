"use strict";

// The byte check every listing reference image passes before it is sent to OpenAI
// (netlify/functions/lib/listingBatchImages.cjs), on made-up pictures: no network, no real files.
// 2026-10-23/24: 505 of 506 Regular Necklace jobs came back 6/6 "Invalid input image file. Please
// check the uploaded image." The bytes went out as data:<Storage contentType>;base64 and nothing
// looked at them, so a JPEG or WebP saved as .png, a 16-bit/CMYK/palette/animated file or a cut-off
// upload all reached OpenAI as "image/png".
// Checked: JPEG/WebP bytes labelled image/png are sent under their real type; palette, 16-bit, CMYK,
// greyscale, interlaced, APNG and GIF files are re-encoded as 8-bit sRGB PNG; HTML, JSON, empty and
// truncated files throw InputImageError (permanent) before any paid request; canonical:true (the
// charm) always re-encodes. Without sharp only formats OpenAI reads directly pass.
// Needs sharp (a dependency of the site); skipped where it is not installed.
// Usage: node tests/listing-batch/input-images.cjs   (from the repository root)

const assert = require("node:assert/strict");
const path = require("node:path");
const zlib = require("node:zlib");

let sharp;
try { sharp = require("sharp"); }
catch { console.log("listing batch input images: sharp is not installed here, skipped"); process.exit(0); }
const { normalizeBatchInputImage, sniffImageType, InputImageError } =
  require(path.resolve("netlify/functions/lib/listingBatchImages.cjs"));

const W = 96, H = 64;
const rgb = Buffer.alloc(W * H * 3);
for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
  const o = (y * W + x) * 3; rgb[o] = x * 2; rgb[o + 1] = y * 3; rgb[o + 2] = (x * 7 + y * 5) % 256;
}
const base = () => sharp(rgb, { raw: { width: W, height: H, channels: 3 } });

// PNG chunks, for the APNG made below (sharp writes still PNGs only).
const chunk = (type, data) => {
  const len = Buffer.alloc(4); len.writeUInt32BE(data.length);
  const body = Buffer.concat([Buffer.from(type, "latin1"), data]);
  const crc = Buffer.alloc(4); crc.writeUInt32BE(zlib.crc32(body) >>> 0);
  return Buffer.concat([len, body, crc]);
};
function chunksOf(png) {
  const out = [];
  for (let at = 8; at < png.length;) {
    const len = png.readUInt32BE(at);
    out.push({ type: png.toString("latin1", at + 4, at + 8), data: png.subarray(at + 8, at + 8 + len) });
    at += 12 + len;
  }
  return out;
}
// A two-frame APNG: acTL + fcTL before the IDAT, a second frame as fcTL + fdAT. A decoder that does not
// know APNG shows the first frame and reports one page, which is why the lib looks for acTL itself.
function apngOf(png) {
  const cs = chunksOf(png);
  const idat = Buffer.concat(cs.filter((c) => c.type === "IDAT").map((c) => c.data));
  const actl = Buffer.alloc(8); actl.writeUInt32BE(2, 0); actl.writeUInt32BE(0, 4);
  const fctl = (seq) => { const b = Buffer.alloc(26); b.writeUInt32BE(seq, 0); b.writeUInt32BE(W, 4); b.writeUInt32BE(H, 8);
    b.writeUInt16BE(1, 20); b.writeUInt16BE(10, 22); return b; };
  const seq2 = Buffer.alloc(4); seq2.writeUInt32BE(2);
  return Buffer.concat([png.subarray(0, 8), chunk("IHDR", cs.find((c) => c.type === "IHDR").data), chunk("acTL", actl),
    chunk("fcTL", fctl(0)), chunk("IDAT", idat), chunk("fcTL", fctl(1)), chunk("fdAT", Buffer.concat([seq2, idat])),
    chunk("IEND", Buffer.alloc(0))]);
}
// A two-frame GIF: the one frame sharp writes, twice, after a NETSCAPE loop block.
function animatedGifOf(gif) {
  let at = 13; if (gif[10] & 0x80) at += 3 * 2 ** ((gif[10] & 7) + 1);
  const head = gif.subarray(0, at), frames = []; let ext = [];
  const skip = (i) => { while (gif[i] !== 0) i += gif[i] + 1; return i + 1; };
  while (at < gif.length && gif[at] !== 0x3b) {
    if (gif[at] === 0x21) { const s = at, label = gif[at + 1]; at = skip(at + 2); if (label === 0xf9) ext.push(gif.subarray(s, at)); continue; }
    assert.equal(gif[at], 0x2c, "GIF image descriptor");
    const s = at; let i = at + 10; if (gif[at + 9] & 0x80) i += 3 * 2 ** ((gif[at + 9] & 7) + 1);
    i = skip(i + 1); frames.push(Buffer.concat([...ext, gif.subarray(s, i)])); ext = []; at = i;
  }
  const loop = Buffer.from([0x21, 0xff, 0x0b, ...Buffer.from("NETSCAPE2.0"), 0x03, 0x01, 0x00, 0x00, 0x00]);
  return Buffer.concat([head, loop, frames[0], frames[0], Buffer.from([0x3b])]);
}

const meta = (buf) => sharp(buf).metadata();
let checks = 0;
// What OpenAI is sent must be a clean 8-bit sRGB PNG/JPEG/WebP whose bytes say what its label says.
async function assertClean(out, wantMime, label) {
  assert.equal(out.mime, wantMime, `${label}: sent as ${wantMime}`);
  assert.equal(`image/${sniffImageType(out.buffer)}`, out.mime, `${label}: the bytes are what the label says`);
  const m = await meta(out.buffer);
  assert.equal(m.depth, "uchar", `${label}: 8-bit`);
  assert.equal(m.space, "srgb", `${label}: sRGB`);
  assert(!(Number(m.pages || 1) > 1), `${label}: one frame`);
  assert(!m.isProgressive, `${label}: not interlaced`);
  assert(!m.paletteBitDepth && !m.isPalette, `${label}: no palette`);
  assert.equal(m.width, W, `${label}: width kept`);
  assert.equal(m.height, H, `${label}: height kept`);
  // and it decodes completely
  await sharp(out.buffer, { failOn: "warning" }).raw().toBuffer();
  checks++;
}
async function assertRejected(buf, pattern, label, opts = {}) {
  await assert.rejects(normalizeBatchInputImage(buf, { contentType: "image/png", path: "listing-generator-1/New_Charms/Charm_7.png",
    role: "Charm image", ...opts }), (err) => {
    assert(err instanceof InputImageError, `${label}: InputImageError (got ${err && err.name}: ${err && err.message})`);
    assert.equal(err.permanent, true, `${label}: permanent, so the set stops instead of retrying`);
    assert.match(err.message, /Charm_7\.png/, `${label}: the message names the file`);
    assert.match(err.message, pattern, `${label}: ${err.message}`);
    return true;
  }, `${label}: rejected before any paid request`);
  checks++;
}

(async () => {
  const png = await base().png({ compressionLevel: 9 }).toBuffer();
  const jpeg = await base().jpeg({ quality: 90 }).toBuffer();
  const webp = await base().webp({ quality: 90 }).toBuffer();
  const opts = { contentType: "image/png", path: "listing-generator-1/New_Charms/Charm_7.png", role: "Template image" };

  // A clean PNG is sent as it is, the same bytes.
  const asIs = await normalizeBatchInputImage(png, opts);
  assert.equal(asIs.action, "as-is");
  assert.equal(asIs.buffer, png, "a clean PNG is not touched");
  await assertClean(asIs, "image/png", "clean PNG");

  // JPEG / WebP bytes saved under a .png name: the real type is sent, the bytes are not re-encoded.
  for (const [buf, mime, name] of [[jpeg, "image/jpeg", "JPEG"], [webp, "image/webp", "WebP"]]) {
    const out = await normalizeBatchInputImage(buf, opts);
    assert.equal(out.action, "relabelled", `${name} labelled image/png is relabelled`);
    assert.equal(out.from, "image/png");
    assert.equal(out.buffer, buf, `${name}: the same bytes`);
    await assertClean(out, mime, `${name} labelled image/png`);
  }
  // Without the decoder the real type is still sent; a format OpenAI does not read is refused.
  const noDecoder = await normalizeBatchInputImage(jpeg, { ...opts, sharp: null });
  assert.equal(noDecoder.mime, "image/jpeg", "without sharp a JPEG labelled png is still sent as image/jpeg");
  await assertRejected(await base().gif().toBuffer(), /GIF/, "a GIF without sharp", { sharp: null });

  // Files OpenAI may refuse although a browser shows them: re-encoded once as an 8-bit sRGB PNG.
  const variants = [
    ["palette PNG", await base().png({ palette: true }).toBuffer(), /palette/],
    ["16-bit PNG", await base().toColourspace("rgb16").png().toBuffer(), /16-bit/],
    ["CMYK JPEG", await base().toColourspace("cmyk").jpeg().toBuffer(), /cmyk/],
    ["greyscale PNG", await base().toColourspace("b-w").png().toBuffer(), /greyscale/],
    ["interlaced PNG", await base().png({ progressive: true }).toBuffer(), /interlaced/],
    ["APNG", apngOf(await base().ensureAlpha().png().toBuffer()), /animated/],
    ["GIF", await base().gif().toBuffer(), /GIF/],
    ["animated GIF", animatedGifOf(await base().gif().toBuffer()), /animated/],
  ];
  // the made-up files really are what they are named
  assert.equal((await meta(variants[1][1])).depth, "ushort");
  assert.equal((await meta(variants[2][1])).space, "cmyk");
  assert.equal((await meta(variants[3][1])).space, "b-w");
  assert.equal((await meta(variants[4][1])).isProgressive, true);
  assert.equal((await meta(variants[7][1])).pages, 2);
  for (const [name, buf, why] of variants) {
    const out = await normalizeBatchInputImage(buf, opts);
    assert.equal(out.action, "converted", `${name} is converted (got ${out.action}: ${out.detail})`);
    assert.match(out.detail, why, `${name}: the reason is kept (${out.detail})`);
    assert.notEqual(out.buffer, buf);
    await assertClean(out, "image/png", name);
  }

  // canonical (the charm of a set): always re-encoded, even a file that looks clean.
  for (const [buf, name] of [[png, "clean PNG"], [jpeg, "JPEG"], [webp, "WebP"]]) {
    const out = await normalizeBatchInputImage(buf, { ...opts, role: "Charm image", canonical: true });
    assert.equal(out.action, "converted", `canonical ${name} is re-encoded`);
    assert.notEqual(out.buffer, buf, `canonical ${name}: new bytes`);
    await assertClean(out, "image/png", `canonical ${name}`);
  }
  const canonicalPng = await normalizeBatchInputImage(png, { ...opts, canonical: true });
  assert(!canonicalPng.buffer.equals(png), "the canonical charm is written by the encoder, not passed through");

  // Not images at all, or not whole: refused with InputImageError before anything is paid for.
  await assertRejected(Buffer.from("<!DOCTYPE html><html><head><title>404</title></head><body>Not found</body></html>"), /HTML/, "an HTML error page");
  await assertRejected(Buffer.from('{"error":{"code":404,"message":"No such object: Charm_7.png"}}'), /JSON/, "a JSON error answer");
  await assertRejected(Buffer.alloc(0), /empty/, "an empty file");
  await assertRejected(png.subarray(0, Math.floor(png.length * 0.6)), /damaged or incomplete/, "a truncated PNG");
  await assertRejected(jpeg.subarray(0, Math.floor(jpeg.length * 0.6)), /damaged or incomplete/, "a truncated JPEG");
  await assertRejected(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13]), /cannot be read|no readable size|damaged/, "a PNG header only");
  await assertRejected(null, /empty/, "no bytes at all");

  console.log(`Listing batch input images: real type sent, 8 kinds of awkward file converted, canonical charm re-encoded, 7 broken files refused (${checks} checks) passed`);
})().catch((err) => { console.error(err); process.exitCode = 1; });
