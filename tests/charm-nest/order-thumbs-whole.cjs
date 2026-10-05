// The order window's two pictures show the WHOLE picture (Paul, 5 Oct, round 3: "the thumbnail is too zoomed in and it
// cut off so do not zoom in this thumbnail in this view"; image 2 showed the middle-finger Vector design cut off at the
// bottom). Cause: .owPhoto and .owVector are square boxes made by aspect-ratio and laid out as grids, and the picture
// inside them was sized "height:100%". A percentage height does not resolve against a height that only comes from
// aspect-ratio, so a TALL picture took its natural height at the box's width, ran past the bottom of the box and was
// clipped (every portrait Etsy photo and every upright charm; wide ones never showed it). Second cause: the box was
// handed the list rows' saved click-zoom for the same SKU (ListZoom, one key for every view; a click near a thumbnail's
// edge zooms it ×5), so one click in a list left the order window zoomed in and cut off for good (R3-E's data-viewer on the
// two boxes keeps ListZoom off them; this test keeps it that way). Third: in the one-column layout (≤ 900 px) the Overview's
// two grid rows shared the view's height, so on a phone the pictures lay across the conversation, or under it.
// Real artwork (the repo's middle-finger charm), upright, wide, tiny, huge and off-centre drawings, portrait, landscape,
// square, tiny and huge photos, and SVGs with no viewBox / no size are put in the order window at 320, 480, 900 and 1440 px
// wide, after a click-zoom has been saved for each in its list row, and the picture's pixels (a screenshot of the box)
// are measured: every corner marker of a photo and the whole drawing of a vector lies inside the box with air all round,
// centred, in its own proportions, with no zoom transform. The list rows' thumbnails and the sheet window's charm picture,
// which reuse the same loader, must not be cut either, and the lists' own click-zoom must still work.
//   node tests/charm-nest/order-thumbs-whole.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>,
//        CN_SHOTS=dir keeps screenshots, THUMBS_PREFIX=thumbs-before|thumbs-after names them)
const path = require('path'), fs = require('fs'), zlib = require('zlib'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const hand = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/middle_5903-paths.json'), 'utf8'));

/* ── photos: a PNG with a coloured square in each corner, so a cropped or zoomed photo loses a corner ── */
const crcTable = (() => { const t = []; for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; t[n] = c >>> 0; } return t; })();
const crc = b => { let c = 0xffffffff; for (const x of b) c = crcTable[(c ^ x) & 255] ^ (c >>> 8); return (c ^ 0xffffffff) >>> 0; };
const chunk = (type, data) => { const len = Buffer.alloc(4); len.writeUInt32BE(data.length); const td = Buffer.concat([Buffer.from(type), data]); const c = Buffer.alloc(4); c.writeUInt32BE(crc(td)); return Buffer.concat([len, td, c]); };
const MARK = [[220, 30, 30], [30, 170, 60], [40, 70, 220], [210, 30, 200]];   // top-left, top-right, bottom-left, bottom-right
function photo(w, h) {
  const m = Math.max(1, Math.round(Math.min(w, h) * 0.12)), bg = [244, 242, 246];
  const row = (top, bottom) => { const r = Buffer.alloc(1 + 3 * w); for (let x = 0; x < w; x++) { let c = bg; if (top || bottom) { const left = x < m, right = x >= w - m; if (left || right) c = MARK[(bottom ? 2 : 0) + (right ? 1 : 0)]; } r[1 + 3 * x] = c[0]; r[2 + 3 * x] = c[1]; r[3 + 3 * x] = c[2]; } return r; };
  const plain = row(false, false), top = row(true, false), bottom = row(false, true), rows = [];
  for (let y = 0; y < h; y++) rows.push(y < m ? top : y >= h - m ? bottom : plain);
  const head = Buffer.alloc(13); head.writeUInt32BE(w, 0); head.writeUInt32BE(h, 4); head[8] = 8; head[9] = 2;
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', head), chunk('IDAT', zlib.deflateSync(Buffer.concat(rows), { level: 1 })), chunk('IEND', Buffer.alloc(0))]);
}
const PHOTOS = { portrait: [300, 500], landscape: [520, 300], square: [400, 400], tiny: [8, 8], huge: [1800, 2600] };
const png = {}; for (const [k, [w, h]] of Object.entries(PHOTOS)) png[k] = photo(w, h);

/* ── the rows: one order each, with its own charm drawing and its own kind of photo ── */
const CASES = [
  { id: 'hand', sku: 'MIDDLE_9935', photo: 'square', note: 'the real middle-finger charm (upright, a ring above it), as image 2' },
  { id: 'tall', sku: 'THUMB_TALL', photo: 'landscape', note: 'upright: 10 × 70 pt' },
  { id: 'wide', sku: 'THUMB_WIDE', photo: 'portrait', note: 'wide: 90 × 9 pt' },
  { id: 'tiny', sku: 'THUMB_TINY', photo: 'tiny', note: 'tiny: 1.6 × 1.6 pt' },
  { id: 'huge', sku: 'THUMB_HUGE', photo: 'huge', note: 'huge: 4000 × 5000 pt' },
  { id: 'off', sku: 'THUMB_OFF', photo: 'portrait', note: 'off-centre: far from the origin, lopsided, a ring in one corner' },
].map((c, i) => Object.assign(c, { rid: '41708372' + String(40 + i).padStart(2, '0'), tid: '4170837' + String(2400 + i) + '1', lid: '19037399' + String(10 + i) }));
const orderOf = c => ({ receiptId: c.rid, orderNumber: c.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Leslie Suhr' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: c.tid, listingId: c.lid, sku: c.sku, title: c.sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });

const SVGS = {
  'svg-size-no-viewbox': `<svg xmlns="http://www.w3.org/2000/svg" width="100" height="300"><rect x="12" y="12" width="76" height="276" rx="14" fill="#1b4fe0"/><circle cx="50" cy="40" r="14" fill="#fff"/></svg>`,
  'svg-nothing': `<svg xmlns="http://www.w3.org/2000/svg"><rect x="20" y="20" width="260" height="110" rx="14" fill="#1b4fe0"/></svg>`,
  'svg-viewbox-only': `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 50 200"><rect x="6" y="6" width="38" height="188" rx="8" fill="#1b4fe0"/></svg>`,
};

/** In the page: the box, its picture, and where the picture really lies (object-fit: contain), in viewport pixels. */
function probe(sel) {
  const box = document.querySelector(sel); if (!box) return null;
  const im = box.querySelector('img,canvas'), r = box.getBoundingClientRect(), cs = getComputedStyle(box);
  const bl = box.clientLeft, bt = box.clientTop, inner = { x: r.x + bl, y: r.y + bt, w: box.clientWidth, h: box.clientHeight };
  if (!im) return { inner, none: true, text: box.textContent.trim().slice(0, 60) };
  const ir = im.getBoundingClientRect(), ic = getComputedStyle(im);
  const nw = im.tagName === 'IMG' ? im.naturalWidth : im.width, nh = im.tagName === 'IMG' ? im.naturalHeight : im.height;
  let content = { x: ir.x, y: ir.y, w: ir.width, h: ir.height };
  if (nw && nh && ic.objectFit === 'contain') { const s = Math.min(ir.width / nw, ir.height / nh), w = nw * s, h = nh * s; content = { x: ir.x + (ir.width - w) / 2, y: ir.y + (ir.height - h) / 2, w, h }; }
  const m = /matrix\(([^)]+)\)/.exec(ic.transform), t = m ? m[1].split(',').map(Number) : [1, 0, 0, 1, 0, 0];
  // what lies on top of the picture at its middle, top and bottom: nothing but the picture and its own box
  const over = [[.5, .5], [.5, .06], [.5, .94], [.06, .5], [.94, .5]].map(([fx, fy]) => { const e = document.elementFromPoint(inner.x + inner.w * fx, inner.y + inner.h * fy); return !e || box.contains(e) || e.contains(box) && e.id !== 'orderWin' && !/owBody|owVInfo|owMain|owView/.test(e.className) ? null : (e.id ? '#' + e.id : '') + '.' + String(e.className).slice(0, 40); }).filter(Boolean);
  return { inner, over, el: { x: ir.x, y: ir.y, w: ir.width, h: ir.height }, content, nat: [nw, nh], fit: ic.objectFit, zoomed: Math.abs(t[0] - 1) > 1e-3 || Math.abs(t[3] - 1) > 1e-3 || Math.abs(t[4]) > .5 || Math.abs(t[5]) > .5, scale: t[0], zoomReady: box.classList.contains('zoomReady'), display: cs.display, sw: im.complete !== false };
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.CN_SHOTS || null, prefix = process.env.THUMBS_PREFIX || 'thumbs-after';
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const problems = [], seen = [];
  const check = (cond, msg) => { if (!cond) problems.push(msg); }, T0 = Date.now(), say = m => { if (process.env.THUMBS_LOG) console.log(((Date.now() - T0) / 1000).toFixed(1) + 's ' + m); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    // the photos the lists and the window ask the proxy for: a different shape for each listing
    await context.route(/\/\.netlify\/functions\/imageProxy/, r => { const k = new URL(r.request().url()).searchParams.get('url'); return r.fulfill({ status: 200, contentType: 'image/png', headers: { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: png[k] || png.square }); });
    await context.addInitScript(photos => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator');
      localStorage.setItem('cn.listingPhotos.v1', JSON.stringify(photos)); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; }, CASES.map(c => [c.lid, '/.netlify/functions/imageProxy?url=' + c.photo]));
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // a second, blank page reads the pixels of screenshots
    const lab = await context.newPage(); await lab.goto('about:blank');
    /** Pixels of a screenshot of one box: where the drawing / the corner markers are. */
    const scan = async (buf, kind) => lab.evaluate(async ({ b64, kind, MARK }) => {
      const bmp = await createImageBitmap(await (await fetch('data:image/png;base64,' + b64)).blob()), W = bmp.width, H = bmp.height;
      const cv = new OffscreenCanvas(W, H), ctx = cv.getContext('2d'); ctx.drawImage(bmp, 0, 0); const d = ctx.getImageData(0, 0, W, H).data;
      const R = 11, inCorner = (x, y) => (x < R || x >= W - R) && (y < R || y >= H - R);   // the box's rounded corners show what is behind it
      const out = { W, H, art: null, edge: 0, marks: MARK.map(() => ({ n: 0, box: null })) };
      const grow = (b, x, y) => b ? [Math.min(b[0], x), Math.min(b[1], y), Math.max(b[2], x), Math.max(b[3], y)] : [x, y, x, y];
      for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
        if (inCorner(x, y)) continue; const o = (y * W + x) * 4, r = d[o], g = d[o + 1], b = d[o + 2];
        if (kind === 'vector') {
          if (r < 225 || g < 225 || b < 225) { out.art = grow(out.art, x, y); const nearEdge = x < 2 || y < 2 || x >= W - 2 || y >= H - 2; if (nearEdge) out.edge++; }
        } else MARK.forEach((c, i) => { if (Math.hypot(r - c[0], g - c[1], b - c[2]) < 70) { out.marks[i].n++; out.marks[i].box = grow(out.marks[i].box, x, y); } });
      }
      return out;
    }, { b64: buf.toString('base64'), kind, MARK });

    /** One box measured: geometry, then pixels. kind 'vector' or 'photo'. */
    const measure = async (label, sel, kind, opts = {}) => {
      await page.evaluate(sel => { const b = document.querySelector(sel); if (b) b.scrollIntoView({ block: 'center', inline: 'nearest' }); }, sel);
      const g = await page.evaluate(`(${probe})(${JSON.stringify(sel)})`);
      if (!g || g.none) { check(false, `${label}: no picture in ${sel} (${g && g.text})`); return null; }
      const I = g.inner, inner = I.w;
      // the pixels inside the frame's border, as the eye sees them
      const buf = await page.screenshot({ clip: { x: Math.round(I.x), y: Math.round(I.y), width: Math.round(I.w), height: Math.round(I.h) } }), px = await scan(buf, kind);
      const air = Math.min(g.content.x - I.x, g.content.y - I.y, I.x + I.w - (g.content.x + g.content.w), I.y + I.h - (g.content.y + g.content.h));
      if (g.over && g.over.length) check(false, `${label}: something lies over the picture: ${g.over.join(', ')}`);
      check(!g.zoomed, `${label}: the picture is zoomed (scale ${g.scale}) — a saved or default zoom was applied to the thumbnail`);
      if (!opts.list) check(!g.zoomReady, `${label}: the thumbnail was given the lists' zoom/pan (zoomReady)`);
      check(g.content.x >= I.x - .5 && g.content.y >= I.y - .5 && g.content.x + g.content.w <= I.x + I.w + .5 && g.content.y + g.content.h <= I.y + I.h + .5, `${label}: the picture runs past its frame: picture ${JSON.stringify(g.content)} frame ${JSON.stringify(I)}`);
      if (!opts.list) check(air >= 3, `${label}: no breathing room, the picture comes within ${air.toFixed(1)} px of the frame`);
      if (kind === 'photo') {
        const names = ['top-left', 'top-right', 'bottom-left', 'bottom-right'], all = px.marks.every(m => m.n >= 3);
        check(all, `${label}: a corner of the photo is not visible: ${px.marks.map((m, i) => names[i] + '=' + m.n).join(' ')}`);
        if (all) {
          const u = px.marks.reduce((b, m) => [Math.min(b[0], m.box[0]), Math.min(b[1], m.box[1]), Math.max(b[2], m.box[2]), Math.max(b[3], m.box[3])], [1e9, 1e9, -1, -1]), w = u[2] - u[0] + 1, h = u[3] - u[1] + 1;
          check(Math.abs(w / h - g.nat[0] / g.nat[1]) < .05 * (g.nat[0] / g.nat[1]) + .06, `${label}: the photo is stretched: shown ${w}×${h}, photo is ${g.nat[0]}×${g.nat[1]}`);
          const cx = (u[0] + u[2]) / 2, cy = (u[1] + u[3]) / 2; check(Math.abs(cx - px.W / 2) <= 3 && Math.abs(cy - px.H / 2) <= 3, `${label}: the photo is off-centre: its middle is at ${cx.toFixed(0)},${cy.toFixed(0)} in a ${px.W}×${px.H} frame`);
          check(Math.min(u[0], u[1], px.W - 1 - u[2], px.H - 1 - u[3]) >= 2, `${label}: the photo touches the frame edge: ${JSON.stringify(u)} in ${px.W}×${px.H}`);
        }
      } else if (opts.list) {
        check(!!px.art, `${label}: nothing is drawn`);   // (a list row's box is drawn edge to edge by design; its picture is checked above to lie inside it, unzoomed)
      } else {
        check(!!px.art, `${label}: nothing is drawn`);
        if (px.art) {
          const a = px.art, cx = (a[0] + a[2]) / 2, cy = (a[1] + a[3]) / 2, tol = Math.max(3, .04 * inner);
          check(px.edge === 0, `${label}: the drawing is cut by the frame (${px.edge} drawn pixels on the frame's edge); drawn box ${JSON.stringify(a)} in ${px.W}×${px.H}`);
          check(Math.min(a[0], a[1], px.W - 1 - a[2], px.H - 1 - a[3]) >= 2, `${label}: the drawing touches the frame edge: ${JSON.stringify(a)} in ${px.W}×${px.H}`);
          if (!opts.offcentreOK) check(Math.abs(cx - px.W / 2) <= tol && Math.abs(cy - px.H / 2) <= tol, `${label}: the drawing is off-centre: its middle is at ${cx.toFixed(0)},${cy.toFixed(0)} in a ${px.W}×${px.H} frame`);
        }
      }
      seen.push(label); say('measured ' + label); return { g, px, buf };
    };

    // ── the rows: each order's charm is drawn the way the pool holds it ──
    await page.evaluate(async ({ cases, orders, hand }) => {
      await Orders.loadMaps(true);
      const poly = pts => [pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['h']])];
      const rect = (x0, y0, x1, y1) => poly([[x0, y0], [x1, y0], [x1, y1], [x0, y1]]);
      const bb = ps => { const xs = ps.flat().map(p => p[0]), ys = ps.flat().map(p => p[1]); return [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)]; };
      const circle = (cx, cy, r) => { const k = .5523 * r; return [[['m', [cx + r, cy]], ['c', [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ['c', [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ['c', [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ['c', [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]], ['h']]]; };
      const stroke = (subpaths, lw) => ({ kind: 'path', subpaths, stroke: true, fill: false, closed: true, paintOp: 'S', strokeRGB: [0, 0, 0], lwPt: lw || .25 });
      const fill = (subpaths, rgb) => ({ kind: 'path', subpaths, stroke: false, fill: true, closed: true, paintOp: 'f', fillRGB: rgb });
      const shape = (id, outlinePts, inner, extra) => {
        const o = stroke(poly(outlinePts)); o.bbox = bb([outlinePts]); o.layer = 'CUT';
        const members = [o]; for (const m of inner) members.push(m);
        const b = o.bbox.slice(); for (const e of (extra || [])) { b[0] = Math.min(b[0], e[0]); b[1] = Math.min(b[1], e[1]); b[2] = Math.max(b[2], e[2]); b[3] = Math.max(b[3], e[3]); }
        members.forEach((m, index) => { m.index = index; m.bbox = m.bbox || b; });
        return { outline: o, members, bbox: b, strokePt: .25 };
      };
      const built = {
        tall: shape('tall', [[0, 0], [10, 0], [10, 70], [0, 70]], [fill(rect(2, 8, 8, 60), [0, 0, 1])]),
        wide: shape('wide', [[0, 0], [90, 0], [90, 9], [0, 9]], [fill(rect(6, 2, 84, 7), [0, 0, 1])]),
        tiny: shape('tiny', [[0, 0], [1.6, 0], [1.6, 1.6], [0, 1.6]], [fill(rect(.4, .4, 1.2, 1.2), [0, 0, 1])]),
        huge: shape('huge', [[0, 0], [4000, 0], [4000, 5000], [0, 5000]], [fill(rect(500, 600, 3500, 4400), [0, 0, 1])]),
        // an L, far from the origin, with its ring in the top-right corner
        off: shape('off', [[5000, -3000], [5040, -3000], [5040, -2970], [5020, -2970], [5020, -2900], [5000, -2900]], [fill(rect(5004, -2996, 5016, -2905), [0, 0, 1]), stroke(circle(5034, -2976, 3))], [[5031, -2979, 5037, -2973]]),
      };
      for (const c of cases) {
        const order = orders[c.id], line = order.lines[0], key = CharmNestOrders.lineKey(order, line), poolId = `${c.rid}_${c.tid}_1`;
        const row = { key, order, line, arrivedAt: Date.now() - cases.indexOf(c) * 1000, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [poolId], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row);
        let charm;
        if (c.id === 'hand') {
          const segs = JSON.parse(JSON.stringify(hand)).map((m, index) => Object.assign(m, { index })), area = b => (b[2] - b[0]) * (b[3] - b[1]);
          const outline = segs.filter(m => m.layer === 'CUT').sort((a, b) => area(b.bbox) - area(a.bbox))[0];
          charm = { outline, members: segs, bbox: [5, 5, 26.4, 44.6], strokePt: .25 }; CharmNestPDF.integrateRings(charm);
        } else charm = built[c.id];
        const b = charm.outline.bbox; Object.assign(charm, { id: 'ch-' + c.id, name: c.sku, poolId, order: c.rid, centerPt: [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2], widthPt: b[2] - b[0], heightPt: b[3] - b[1] });
        (window.__charms = window.__charms || {})[poolId] = charm;
      }
      const was = Pool.charmOf; Pool.charmOf = id => window.__charms[id] || was(id);
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { cases: CASES, orders: Object.fromEntries(CASES.map(c => [c.id, orderOf(c)])), hand });

    say('rows built');
    // ── 1 · the lists: every row's two thumbnails are drawn, the drawing whole (the same loader and the same boxes as the Engraving, Review and Library cards) ──
    const rowSel = c => `#ordItems [data-key="${c.rid}_${c.tid}"]`;
    for (const c of CASES) await page.waitForSelector(rowSel(c));
    for (const c of CASES) {
      await page.locator(rowSel(c)).scrollIntoViewIfNeeded();
      await page.waitForFunction(s => { const r = document.querySelector(s); return r && r.querySelector('[data-vector] img, [data-vector] canvas') && r.querySelector('[data-listing] img'); }, rowSel(c), { timeout: 40000 });
      await page.waitForFunction(s => { const i = document.querySelector(s + ' [data-listing] img'); return i && i.complete && i.naturalWidth > 0; }, rowSel(c), { timeout: 20000 });
      await measure(`list row ${c.id} (88 px) vector`, `${rowSel(c)} [data-vector]`, 'vector', { list: true });
    }
    // the lists' own zoom is a feature and stays: one click on a vector zooms it, and the listing starts at the reference's 2×
    if (!process.env.THUMBS_NO_LIST_CLICK) for (const c of CASES) {   // (THUMBS_NO_LIST_CLICK: for the before/after pictures of the box layout alone)
      const s0 = await page.evaluate(s => parseFloat(document.querySelector(s + ' [data-listing] img').dataset.scale), rowSel(c));
      check(s0 === 2, `list row ${c.id}: the listing thumbnail keeps the lists' own starting zoom (2), got ${s0}`);
      await page.locator(`${rowSel(c)} [data-vector]`).click({ position: { x: 44, y: 8 } });   // near the top of the drawing, as a hand lands
      const s1 = await page.evaluate(s => parseFloat(document.querySelector(s + ' [data-vector] img, ' + s + ' [data-vector] canvas').dataset.scale), rowSel(c));
      check(s1 > 1.2, `list row ${c.id}: a click in a list thumbnail still zooms it (scale ${s1})`);
    }
    await page.evaluate(() => new Promise(r => setTimeout(r, 400)));   // the saved frames are written (150 ms)

    // ── 2 · the order window at four widths, each order opened after its list zoom was saved ──
    const settled = () => page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 8000 });
    const WIDTHS = [[320, 1000], [480, 1300], [900, 2300], [1440, 900]];
    for (const c of CASES) {
      const key = `${c.rid}_${c.tid}`; await page.setViewportSize({ width: 1440, height: 900 }); say('opening ' + c.id);
      await page.evaluate(k => OrderWin.open(k), key);
      await page.waitForFunction(() => { const v = document.querySelector('#owVector img, #owVector canvas'), p = document.querySelector('#owPhoto img'); return v && p && p.complete && p.naturalWidth > 0 && (v.tagName === 'CANVAS' || (v.complete && v.naturalWidth > 0)); }, null, { timeout: 40000 });
      say('pictures there'); await settled(); say('settled');
      for (const [w, h] of (c.id === 'hand' || c.id === 'off' ? WIDTHS : WIDTHS.slice(0, 3))) {   // the window is resized, not reopened: the layout answers the width
        await page.setViewportSize({ width: w, height: h }); await page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
        const tag = `order window ${w} px, ${c.id} (${c.note})`;
        await measure(tag + ' · Vector design', '#owVector', 'vector');
        await measure(tag + ' · Etsy listing photo ' + c.photo, '#owPhoto', 'photo');
        const spill = await page.evaluate(() => { const p = document.querySelector('.owPics'); return p.scrollWidth - p.clientWidth; });
        check(spill <= 1, `${tag}: the pictures column spills sideways by ${spill} px`);
        if (shots && c.id === 'hand') { fs.mkdirSync(shots, { recursive: true }); await page.locator('.owPics').screenshot({ path: path.join(shots, `${prefix}-${w}.png`) }); }
      }
      await page.setViewportSize({ width: 1440, height: 900 }); await page.evaluate(() => OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 });
    }

    // ── 3 · SVGs with no viewBox / no size (the vector loader hands the box whatever picture the render gives it), and the
    //       sheet window's charm picture (.owCharm .pic, 92 px), which is laid out by its own rule ──
    await page.setViewportSize({ width: 480, height: 1300 });
    const c0 = CASES[0], k0 = `${c0.rid}_${c0.tid}`;
    await page.evaluate(k => OrderWin.open(k), k0); await settled();
    for (const [name, svg] of Object.entries(SVGS)) {
      await page.evaluate(([name, svg, k]) => { CharmNestPDF.frontPreview = async () => 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg); const row = Orders.rows().find(r => r.key === k); row.spec.size = name; OrderWin.paint(); }, [name, svg, k0]);
      await page.waitForFunction(() => { const i = document.querySelector('#owVector img'); return i && i.complete && !document.querySelector('#owVector .thumbLoading'); }, null, { timeout: 15000 });
      // (an SVG with neither viewBox nor size is painted 1:1 from the box's corner by the browser itself: nothing in CSS can scale or
      // centre it, so it is only held to lying whole inside the frame)
      await measure(`order window 480 px, ${name}`, '#owVector', 'vector', { offcentreOK: name === 'svg-nothing' });
    }
    await page.evaluate(() => OrderWin.close());
    // a list row's drawn picture, laid into the sheet window's picture box as that panel does
    for (const c of CASES) {
      const src = await page.evaluate(s => { const i = document.querySelector(s + ' [data-vector] img'); return i ? i.src : null; }, rowSel(c));
      if (!src) { check(false, `sheet window charm picture, ${c.id}: the list row has no picture to copy`); continue; }
      await page.evaluate(src => { document.querySelectorAll('.t3Pic').forEach(n => n.remove());
        const s = document.createElement('section'); s.className = 'owCharm t3Pic'; s.style.cssText = 'position:fixed;left:20px;top:20px;z-index:99999;background:#fff;padding:8px'; s.innerHTML = '<div class="pic" data-pic><img alt="" src="' + src + '"></div>'; document.body.appendChild(s); }, src);
      await page.waitForFunction(() => { const i = document.querySelector('.t3Pic img'); return i && i.complete && i.naturalWidth > 0; });
      await measure(`sheet window charm picture (92 px), ${c.id}`, '.t3Pic [data-pic]', 'vector');
    }
    await page.evaluate(() => document.querySelectorAll('.t3Pic').forEach(n => n.remove()));
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (problems.length) { console.error(problems.map(p => '  ✗ ' + p).join('\n')); console.error(`\n  ${problems.length} problems in ${seen.length} measured pictures`); process.exitCode = 1; return; }
  console.log(`Order thumbnails OK: ${seen.length} pictures measured whole, centred, with air, unzoomed — real hand charm, upright / wide / tiny / huge / off-centre drawings, portrait / landscape / square / tiny / huge photos, SVGs without viewBox or size; order window at 320, 480, 900 and 1440 px, list rows and the sheet window charm picture too`);
}
main().catch(e => { console.error(e); process.exit(1); });
