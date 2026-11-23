// RGHISTORY (Paul, 22 Nov 2026: "all RG sheets in Library are not displaying the Greyed out charms prior to the Green cut line"): the cloud's sheet picture (_charmNestFinishRender.js
// previewPng) of a sheet on a Rose Gold stock draws the stock's history as the page's RoseStock.paint did: the cut-away in light grey, the earlier cuts' pieces grey, every cut's green line.
// The fixture is the real stock of RG Nov 22 Set-2 Sheet 1 (cut 18 Nov at ~48–57 mm, then 22 Nov at ~77–88 mm). No network.
//   1  the 22 Nov sheet (its own line cut): the 18 Nov pieces are grey left of its line; nothing grey right of 18 Nov's line where its own pieces are; both green lines drawn
//   2  a sheet of no stock (no history) is drawn as before: no grey, no green
//   3  the writer reads the stock and its cuts and hands them to the picture
//   NODE_PATH=<deps> node tests/charm-nest/rg-history-picture.cjs  (OUT=<png> to keep the picture)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 600))); if (!c) failed++; };
const R = require(path.join(root, 'netlify/functions/_charmNestFinishRender.js'));
const hist = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/rose-stock-history.json'), 'utf8'));
const st = { wPt: hist.stock.wPt, hPt: hist.stock.hPt }, MM = 25.4 / 72;
const sheet = { id: 'rose-mwmr139j-cralro139j', metal: 'rose', roseCutAt: 1795400303000, laserDoneAt: 0, charms: [], placements: [] };
const png = R.previewPng(sheet, st, hist);
ok('1 · a picture is drawn', !!png && png.length > 1000, png && png.length);
if (process.env.OUT && png) fs.writeFileSync(process.env.OUT, png);
let img = null; try { const { loadImage, createCanvas } = (() => { try { return require('@napi-rs/canvas'); } catch (_) { return require('canvas'); } })(); img = { loadImage, createCanvas }; } catch (_) { img = null; }
(async () => {
  // read the pixels back with the cloud's own canvas when it can decode; else with pngjs-free counting through the renderer's canvas
  let px = null; try { px = decode(png, require('zlib')); } catch (e) { px = null; }
  ok('1 · the picture can be read back', !!px, 'no decoder');
  if (px) {
    const W = px.width, H = px.height, k = W / st.wPt, at = (xmm, ymm) => { const x = Math.round(xmm / MM * k), y = Math.round(ymm / MM * k), i = (y * W + x) * 4; return [px.data[i], px.data[i + 1], px.data[i + 2]]; };
    const isGrey = c => Math.abs(c[0] - c[1]) < 12 && Math.abs(c[1] - c[2]) < 12 && c[0] < 245 && c[0] > 150, isGreen = c => c[1] > c[0] + 40 && c[1] > c[2] - 10;
    let greyLeft = 0, greyRight = 0, green = 0;
    for (let x = 2; x < 98; x += 0.5) for (let y = 2; y < 48; y += 0.5) { const c = at(x, y); if (isGrey(c)) { if (x < 47) greyLeft++; else if (x > 60) greyRight++; } if (isGreen(c)) green++; }
    ok('1 · the 18 Nov cut is grey left of its green line (the stock used before this sheet)', greyLeft > 2000, { greyLeft });
    ok('1 · nothing grey where this sheet\'s own pieces stand (right of 18 Nov\'s line)', greyRight < 50, { greyRight });
    ok('1 · the green lines are drawn', green > 100, { green });
    const plain = R.previewPng(Object.assign({}, sheet, { roseCutAt: 0 }), st, null), p2 = decode(plain, require('zlib'));
    let g2 = 0, gr2 = 0; for (let i = 0; i < p2.data.length; i += 4) { const c = [p2.data[i], p2.data[i + 1], p2.data[i + 2]]; if (isGrey(c)) g2++; if (isGreen(c)) gr2++; }
    ok('2 · a sheet of no stock is drawn as before: no grey, no green', g2 < 50 && gr2 === 0, { g2, gr2 });
  }
  const fin = fs.readFileSync(path.join(root, 'netlify/functions/_charmNestFinish.js'), 'utf8');
  ok('3 · the writer reads the stock and its cuts and hands them to the picture', /collection\("Charm_Nest_Rose_Stock"\)\.doc\(String\(X\.rec\.roseStockId\)\)[\s\S]{0,200}collection\("cuts"\)\.orderBy\("revision", "desc"\)/.test(fin) && /X\.Render\.previewPng\([\s\S]{0,300}X\.st, hist\)/.test(fin));
  console.log(failed ? `FAIL rg-history-picture (${failed})` : 'PASS rg-history-picture'); process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL rg-history-picture: ' + (e && e.stack || e)); process.exit(1); });
/** A plain PNG decoder (8-bit RGBA or RGB, filter types 0–4), enough to read the renderer's own picture back. */
function decode(buf, zlib) {
  let p = 8, W = 0, H = 0, ct = 0; const idat = [];
  while (p < buf.length) { const len = buf.readUInt32BE(p), type = buf.toString('ascii', p + 4, p + 8), data = buf.slice(p + 8, p + 8 + len); if (type === 'IHDR') { W = data.readUInt32BE(0); H = data.readUInt32BE(4); ct = data[9]; } else if (type === 'IDAT') idat.push(data); p += 12 + len; }
  const raw = zlib.inflateSync(Buffer.concat(idat)), bpp = ct === 6 ? 4 : 3, stride = W * bpp, out = Buffer.alloc(W * H * 4); let prev = Buffer.alloc(stride);
  for (let y = 0; y < H; y++) { const f = raw[y * (stride + 1)], line = Buffer.from(raw.slice(y * (stride + 1) + 1, (y + 1) * (stride + 1)));
    for (let x = 0; x < stride; x++) { const a = x >= bpp ? line[x - bpp] : 0, b = prev[x], c = x >= bpp ? prev[x - bpp] : 0; let v = line[x];
      if (f === 1) v += a; else if (f === 2) v += b; else if (f === 3) v += (a + b) >> 1; else if (f === 4) { const pp = a + b - c, pa = Math.abs(pp - a), pb = Math.abs(pp - b), pc = Math.abs(pp - c); v += pa <= pb && pa <= pc ? a : pb <= pc ? b : c; } line[x] = v & 255; }
    for (let x = 0; x < W; x++) { out[(y * W + x) * 4] = line[x * bpp]; out[(y * W + x) * 4 + 1] = line[x * bpp + 1]; out[(y * W + x) * 4 + 2] = line[x * bpp + 2]; out[(y * W + x) * 4 + 3] = bpp === 4 ? line[x * bpp + 3] : 255; }
    prev = line; }
  return { width: W, height: H, data: out };
}
