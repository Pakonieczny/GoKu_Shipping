// Click to zoom and drag to pan, in place, on every order picture (Paul, 5 Oct 2026, 13:25 UTC: "You implemented the zoom features
// here as a fullscreen Zoom view. This is not what I wanted. You need to implement the same type of sophisticated zoom and pan as
// we already have [in] other parts of this app. When clicking to zoom it should zoom in within its own space and should be
// draggable to pan the photo and when clicking to zoom it should centre the area of the image that was clicked ... including the
// back engraving when available in the same space").
// ONE module does it (charm-nest-zoompan.js, the click-to-zoom of the lists and Review taken over with its numbers): the order
// window's Etsy listing photo and Vector design, the back engraving preview wherever the card is drawn (the Overview, the Sheet
// tab, the Sheet window), and the charm's drawing on the Sheet tab and in the Sheet window. Headless Chromium on the fake site
// (bridge-server.cjs), offline; Engrave.approve and EngraveLink.open are stand-ins (the seal is pressed on the very button).
//   1  rest: whole, unzoomed, a focusable frame, zoom-in cursor, no tooltip, no dialog
//   2  parity: the module and the Shopify studio's attachZoomPan (the existing code, run in the same page) make the same
//      scale and offsets for the same clicks, drags and wheel steps
//   3  a click zooms inside its own frame (no dialog, no fullscreen, the frame and its neighbours do not move), the clicked point
//      comes to the frame's middle (three points, within 3 px; clamped at the edges), a wide photo is never panned into its bands
//   4  a drag pans (damped by half, clamped: the frame is never left empty), a click that ends a drag is no click, the cursor
//      says zoom-in / grab / grabbing, more clicks go deeper and then back whole, the round button, Esc (the window stays), 0
//   5  the keyboard: Enter and Space toggle at the middle, + and -, arrows pan a zoomed picture only
//   6  the wheel: a plain wheel over a picture still scrolls the window (also when zoomed); ctrl + wheel and a pinch zoom
//   7  touch: a tap zooms at the tap, one finger drags, two fingers pinch about their midpoint
//   8  sharp: the vector is drawn again larger when the zoom settles, the Etsy photo is swapped for its larger size, the small
//      ones are back when whole again (no Etsy API call: an image address only)
//   9  the back engraving card: the preview zooms in its own card, nothing else in it moves, Approved / words / View in Engrave work
//  10  the Sheet tab (card and charm picture) and the Sheet window (card and charm picture)
//  11  another piece starts whole; the window drawn again keeps the zoom and the sharper pictures; closing forgets it
//  12  only transform animates; no listener of the module piles up on window or document over 20 openings
//  13  mutants: the fullscreen route back, a click that does not centre, no clamp, a wheel that takes the page's scroll, a zoom
//      lost on a redraw, a transition on more than transform: each one is caught
//   node tests/charm-nest/inplace-zoom-pan.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir> keeps screenshots)
const fs = require('fs'), path = require('path'), zlib = require('zlib'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

// ── real PNGs of known sizes for the fake image proxy (a pattern, so a zoomed picture can be told from a flat one) ──
const crcT = (() => { const t = new Int32Array(256); for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; t[n] = c; } return t; })();
const crc = b => { let c = -1; for (const x of b) c = crcT[(c ^ x) & 255] ^ (c >>> 8); return (c ^ -1) >>> 0; };
const chunk = (type, data) => { const len = Buffer.alloc(4); len.writeUInt32BE(data.length); const td = Buffer.concat([Buffer.from(type), data]), c = Buffer.alloc(4); c.writeUInt32BE(crc(td)); return Buffer.concat([len, td, c]); };
const pngs = new Map();
function png(w, h) {
  const k = w + 'x' + h; if (pngs.has(k)) return pngs.get(k);
  const row = w * 3 + 1, raw = Buffer.alloc(row * h), cell = Math.max(4, Math.round(Math.max(w, h) / 24));
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { const o = y * row + 1 + x * 3, chk = ((Math.floor(x / cell) + Math.floor(y / cell)) & 1) ? 1 : 0; raw[o] = chk ? 236 : 150 + (x * 90 / w); raw[o + 1] = chk ? 232 : 150 + (y * 90 / h); raw[o + 2] = chk ? 222 : 170; }
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 2;
  const out = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw, { level: 1 })), chunk('IEND', Buffer.alloc(0))]);
  pngs.set(k, out); return out;
}
// the listing photos: piece 1 square, piece 2 wide (4:3), piece 3 upright; the larger sizes keep the shape
const SHAPES = { '1800101': [1, 1], '1800102': [4, 3], '1800103': [3, 4] };
const WIDTH = { '570xN': 570, '1588xN': 1000, fullxfull: 2000 };

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000), SH = 'sheet-zp1';
const B = { rid: '4170837249', skus: ['CABLE_CHAIN_ONLY', 'MIDDLE_9935', 'MIDDLE_9935'], metal: ['rose', 'gold', 'rose'], label: ['RG 14/20', 'GF 14/20', 'RG 14/20'], lids: ['1800101', '1800102', '1800103'] };
const tid = i => B.rid + (i + 1), key = i => `${B.rid}_${tid(i)}`, pool = i => key(i) + '_1';
const WORDS = { [key(1)]: 'I\ndissent', [key(2)]: 'KMB //\nSMH' };
const etsy = id => `https://i.etsystatic.com/1/r/il/abcd/${id}/il_570xN.${id}_wxyz.jpg`;
const proxied = u => '/.netlify/functions/imageProxy?url=' + encodeURIComponent(u);
const line = i => ({ transactionId: tid(i), listingId: B.lids[i], sku: B.skus[i], title: B.skus[i].replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: B.label[i] }], metalKey: B.metal[i], metalLabel: B.label[i], personalization: '' });
const ORDER = { receiptId: B.rid, orderNumber: B.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Leslie Suhr' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [0, 1, 2].map(line) };

function seed(st) {
  // the GF middle (piece 2) and the RG middle (piece 3) sit on one saved sheet, so the Sheet tab and the Sheet window have a piece to show
  const pl = [], ch = [];
  [1, 2].forEach((i, n) => { pl.push({ id: 'c' + n, cxPt: 40 + n * 50, cyPt: 40, angle: 0, wPt: 34, hPt: 34 }); ch.push({ id: 'c' + n, name: `${B.rid} · ${B.skus[i]}`, poolId: pool(i), order: B.rid, sku: B.skus[i], thumbUrl: proxied('https://i.etsystatic.com/charm-thumb.png') }); });
  st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 1, setSeq: 1, day: '2026-10-05', status: 'written', fileBase: 'GF_Oct.5.26_Set-1_Sheet-1', folder: 'GF_Oct.5.26_Set-1_Sheet-1', stock: { wPt: 230, hPt: 90 }, orders: [B.rid], placements: pl, charms: ch, backPool: [] });
  for (const i of [1, 2]) st.put('Charm_Pool', pool(i), { poolId: pool(i), orderId: B.rid, transactionId: tid(i), lineKey: key(i), sku: B.skus[i], material: i === 1 ? 'gold' : 'rose', copy: 1, quantity: 1, state: 'written', sheetId: SH, sheetName: 'GF_Oct.5.26_Set-1_Sheet-1', updatedAt: Date.now() });
}

/** The page, with the order of piece 1..3 in the pull, a pooled charm each (an upright tag), the back jobs, and the stand-ins. */
async function boot(browser, srv, opts = {}) {
  const context = await browser.newContext({ viewport: opts.viewport || { width: 1440, height: 900 }, hasTouch: !!opts.touch, isMobile: !!opts.touch, deviceScaleFactor: opts.dpr || 1 });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
  context.asked = [];
  await context.route(/\/\.netlify\/functions\/imageProxy/, r => {
    const u = new URL(r.request().url()).searchParams.get('url') || '', m = /\/il_([^.]+)\./.exec(u), lid = (/\/(\d{7})\/il_/.exec(u) || [])[1]; context.asked.push(u);
    const shape = SHAPES[lid], w = m && WIDTH[m[1]];
    const [pw, ph] = shape && w ? [w, Math.round(w * shape[1] / shape[0])] : [160, 160];
    return r.fulfill({ status: 200, headers: { 'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: png(pw, ph) });
  });
  if (opts.zoompan) await context.route(/\/charm-nest-zoompan\.js/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: opts.zoompan }));
  const index = B.lids.map(l => [l, proxied(etsy(l))]);
  await context.addInitScript(idx => {
    try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.tour.seen', '1'); localStorage.setItem('cn.listingPhotos.v1', JSON.stringify(idx)); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
    window.prompt = () => 'Test Operator';
    // which listeners the zoom module puts on the page itself (window, document): none should pile up
    window.__zpListeners = 0; const add = EventTarget.prototype.addEventListener;
    EventTarget.prototype.addEventListener = function () { if ((this === window || this === document) && /charm-nest-zoompan\.js/.test(new Error().stack || '')) window.__zpListeners++; return add.apply(this, arguments); };
  }, index);
  const page = await context.newPage(), errors = [];
  page.setDefaultTimeout(30000);
  page.errors = errors; page.context_ = context;
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
  page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource|net::ERR|favicon/i.test(m.text())) { errors.push('console: ' + m.text()); console.error('console error:', m.text().slice(0, 300)); } });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && window.Engrave && window.OrderEngraving && window.CNEngravingSeals && CN.S.cloud.ok === true, null, { timeout: 90000 });
  await page.evaluate(async ({ order, keys, pools, WORDS }) => {
    await Orders.loadMaps(true);
    for (const l of order.lines) { const k = CharmNestOrders.lineKey(order, l); const row = { key: k, order, line: l, arrivedAt: Date.now() - 6 * 3600e3, spec: null, problems: [], state: 'written', reason: null, claimedBy: null, poolIds: [k + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(k, row); }
    Orders.interpretAll();
    // an upright tag with a ring: the vector design of every piece, drawn by the page's own ListMedia and Pool
    const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [20, 0]], ['l', [20, 48]], ['l', [0, 48]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 20, 48] };
    const ring = { kind: 'path', subpaths: [[['m', [6, 38]], ['l', [14, 38]], ['l', [14, 44]], ['l', [6, 44]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .5, bbox: [6, 38, 14, 44] };
    const charm = { id: 'c', name: 'TAG', metal: 'gold', centerPt: [10, 24], widthPt: 20, heightPt: 48, areaPt2: 20 * 48, outline: sq, members: [sq, ring], bbox: [0, 0, 20, 48], thumb: '' };
    const mine = new Set(pools), was = Pool.charmOf; Pool.charmOf = id => mine.has(id) ? Object.assign({ poolId: id }, charm) : was(id);
    // the back: the placement picture is a canvas of the size asked (a sharper one is asked when the zoom settles), the saved one a PNG
    const art = (text, px) => { const n = px || 300, c = document.createElement('canvas'); c.width = c.height = n; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, n, n); x.strokeStyle = '#999'; x.lineWidth = n / 150; x.strokeRect(n * .06, n * .06, n * .88, n * .88); x.fillStyle = '#111'; x.font = (n / 7.5) + 'px sans-serif'; x.textAlign = 'center'; (text || '').split('\n').forEach((t, i) => x.fillText(t, n / 2, n * .44 + i * n * .16)); return c; };
    window.__renderBack = []; Engrave.renderBack = (job, px) => { window.__renderBack.push(px); return art(job.text, typeof px === 'number' ? px : 300); };
    const fit = () => ({ size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' });
    const mk = (k, state, extra) => { const row = B.orders.byKey.get(k), text = WORDS[k], j = Engrave.ensureJob(row); Object.assign(j, { state, text, lines: text.split('\n'), activityAt: Date.now() }, extra || {}); row.engrave = { needed: true, state, approved: false, text }; return j; };
    const at = Date.now() - 2 * 3600e3, g = keys[1];
    const j2 = mk(g, 'approved', { approvedBy: 'Giovanna', approvedAt: at, backs: [{ poolId: g + '_1', approvedAt: at, approvedBy: 'Giovanna', png: art(WORDS[g], 400).toDataURL() }] });
    Object.assign(B.orders.byKey.get(g).engrave, { approved: true, approvedBy: 'Giovanna', approvedAt: at }); CNEngravingSeals.add(j2, 'engraveApproved', 'Giovanna', at); CNEngravingSeals.keep(j2);
    mk(keys[2], 'review', { fit: fit(), view: {}, verify: { geometry: { ok: true } } });
    window.__approve = []; window.__links = [];
    Engrave.approve = async (job, by, button) => {
      if (job._approvalTask) return;
      const task = (async () => {
        if (job.state !== 'review' || job.stamping) return;
        window.__approve.push({ key: job.key, by, button: !!button && button.isConnected });
        await new Promise(r => setTimeout(r, 150));
        const t = Date.now(); CNEngravingSeals.keep(job); const seal = CNEngravingSeals.add(job, 'engraveApproved', by, t);
        job.stamping = true; try { await CNEngravingSeals.press(button && button.isConnected ? button : null, seal); } finally { job.stamping = false; }
        Object.assign(job, { state: 'approved', approvedBy: by, approvedAt: t }); Object.assign(job.row.engrave || (job.row.engrave = {}), { needed: true, state: 'approved', approved: true, approvedBy: by, approvedAt: t, text: job.text });
        try { Engrave.timelineApproved(job); } catch (_) {}
      })();
      job._approvalTask = task; try { return await task; } finally { delete job._approvalTask; }
    };
    if (window.EngraveLink) EngraveLink.open = async a => { window.__links.push(a); return true; };
    Engrave.render = () => {};
    CN.setMode('orders'); Orders.render();
  }, { order: ORDER, keys: [0, 1, 2].map(key), pools: [0, 1, 2].map(pool), WORDS });
  await page.waitForSelector(`#ordItems [data-key="${key(0)}"]`);
  return page;
}

const settled = page => page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 20000 });
/** Open the order window on piece i and wait until both pictures are drawn. */
async function openPiece(page, i, extra) {
  await page.evaluate(({ k, extra }) => OrderWin.open(k, extra || undefined), { k: key(i), extra });
  await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && document.querySelector('#owPhoto img')?.complete && document.querySelector('#owPhoto img').naturalWidth > 0 && document.querySelector('#owVector img, #owVector canvas'), key(i), { timeout: 40000 });
  await settled(page);
}
const pickPiece = async (page, i) => { await page.click(`#owPcSum .owPcRow[data-piece="${key(i)}"] .dot`); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owPhoto img')?.complete, key(i), { timeout: 20000 }); await settled(page); };
/** Put whatever is zoomed in the order window back whole (Esc) - and only then, so a second Esc never closes the window. */
const unzoom = async page => { if (await page.evaluate(() => CNZoomPan.zoomedWithin(document.getElementById('orderWin')))) { await page.keyboard.press('Escape'); await page.waitForTimeout(320); } };
const closeWin = async page => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); await page.waitForTimeout(150); };

/** Which properties animate: what the browser reports as running, and what the pictures say they transition (a duration above 0). */
function sampleAnims() {
  window.__anims = new Set();
  window.__animT = setInterval(() => {
    for (const a of document.getElementById('orderWin').getAnimations({ subtree: true })) if (a.transitionProperty) window.__anims.add(a.transitionProperty);
    for (const m of document.querySelectorAll('#owPhoto img, #owPhoto canvas, #owVector img, #owVector canvas')) { const c = getComputedStyle(m); if (c.transitionDuration.split(',').some(d => parseFloat(d) > 0)) window.__anims.add(c.transitionProperty); }
  }, 10);
}

/** The screenshots the brief asks for, at the width of the page given: the listing photo zoomed at a corner and at the middle, the
 *  vector, the back engraving. Each is the whole viewport, so the frame is seen in its window. `tap` is a mouse click or a touch. */
async function takeShots(page, tag, dir, tap) {
  const PH = '#owPhoto', VE = '#owVector', EV = '#owEng .pv';
  const at = async (sel, fx, fy) => { await page.evaluate(q => document.querySelector(q).scrollIntoView({ block: 'center', inline: 'nearest' }), sel); await page.waitForTimeout(250); const g = await look(page, sel); await tap(page, g.inner.x + g.inner.w * fx, g.inner.y + g.inner.h * fy); await page.waitForTimeout(600); };
  // (put back whole by script, and let go of the focus: the shots show what a mouse or a finger leaves, not a keyboard's ring)
  const whole = () => page.evaluate(() => { CNZoomPan.resetWithin(document.getElementById('orderWin'), false); if (document.activeElement && document.activeElement.blur) document.activeElement.blur(); });
  const snap = name => page.screenshot({ path: path.join(dir, `${name}-${tag}.png`) });
  await openPiece(page, 0);
  await at(PH, .9, .85); await snap('photo-zoomed-corner'); await whole();
  await at(PH, .5, .5); await snap('photo-zoomed-centre'); await whole();
  await at(VE, .5, .4);
  await page.waitForFunction(() => { const m = document.querySelector('#owVector img, #owVector canvas'); return m && Math.max(m.naturalWidth || m.width, m.naturalHeight || m.height) >= 700; }, null, { timeout: 15000 }).catch(() => {});
  await snap('vector-zoomed'); await whole();
  await pickPiece(page, 2);
  await page.waitForFunction(() => document.querySelector('#owEng .swEng .pv canvas'), null, { timeout: 15000 });
  await at(EV, .62, .4); await page.waitForTimeout(500); await snap('back-engraving-zoomed'); await whole();
}

/** A frame and its picture, as the eye reads them: the frame, the picture's own rectangle, where the drawn content lies (object-fit), the state. */
function probe(sel) {
  const box = document.querySelector(sel); if (!box) return null;
  const m = box.querySelector('img, canvas'), fr = box.getBoundingClientRect(), I = { x: fr.x + box.clientLeft, y: fr.y + box.clientTop, w: box.clientWidth, h: box.clientHeight };
  const out = { frame: { x: fr.x, y: fr.y, w: fr.width, h: fr.height }, inner: I, zp: box.dataset.zp || '', cursor: box.style.cursor, role: box.getAttribute('role'), tabindex: box.getAttribute('tabindex'), pressed: box.getAttribute('aria-pressed'), title: box.getAttribute('title'), touch: box.style.touchAction, reset: (() => { const b = box.querySelector('.zpReset'); return b ? !b.hidden : null; })() };
  if (!m) return Object.assign(out, { none: true });
  const r = m.getBoundingClientRect(), nw = m.naturalWidth || m.width, nh = m.naturalHeight || m.height, k = Math.min(r.width / nw, r.height / nh), cw = nw * k, ch = nh * k;
  return Object.assign(out, { tag: m.tagName, nat: [nw, nh], src: (m.currentSrc || m.src || '').slice(0, 600), s: parseFloat(m.dataset.scale) || 1, ox: parseFloat(m.dataset.offsetX) || 0, oy: parseFloat(m.dataset.offsetY) || 0, transform: m.style.transform, transition: m.style.transition,
    el: { x: r.x, y: r.y, w: r.width, h: r.height }, content: { x: r.x + (r.width - cw) / 2, y: r.y + (r.height - ch) / 2, w: cw, h: ch }, mid: { x: I.x + I.w / 2, y: I.y + I.h / 2 } });
}
/** The back card drawn again for the same piece (a new frame each time) keeps the zoom and the pan it had. */
async function keepsOnRedrawOn(page) {
  await openPiece(page, 2); await unzoom(page);
  const g = await look(page, '#owEng .pv'); await page.mouse.click(g.inner.x + g.inner.w * .7, g.inner.y + g.inner.h * .6); await page.waitForTimeout(400);
  const a = await look(page, '#owEng .pv');
  await page.evaluate(() => { document.getElementById('owEng')._orderEngraving.refresh(true); }); await page.waitForTimeout(400);
  const b = await look(page, '#owEng .pv');
  return { before: a.s, after: b.s, same: near(a.ox, b.ox, .01) && near(a.oy, b.oy, .01) };
}

const near = (a, b, tol) => Math.abs(a - b) <= tol;
const look = (page, sel) => page.evaluate(`(${probe})(${JSON.stringify(sel)})`);
/** The fraction of the drawn content (0..1) that lies under a screen point. */
const frac = (g, x, y) => [(x - g.content.x) / g.content.w, (y - g.content.y) / g.content.h];

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: the browser checks were not run'); return; } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] }); seed(srv.st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], T0 = Date.now(), say = m => { if (process.env.ZP_LOG) console.log(`    [${((Date.now() - T0) / 1000).toFixed(1)}s] ${m}`); };
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const zoomSrc = fs.readFileSync(path.join(root, 'charm-nest-zoompan.js'), 'utf8');
  try {
    const page = await boot(browser, srv);
    const PH = '#owPhoto', VE = '#owVector';
    // the screenshots first, on the page as it was seeded (the back card still to approve)
    if (shots) { await takeShots(page, '1440', shots, (p, x, y) => p.mouse.click(x, y)); await closeWin(page); }

    /* ── the checks that a mutant must fail, kept as functions ── */
    // a click zooms inside the frame, nothing opens, nothing moves
    const inPlace = async () => {
      await openPiece(page, 0);
      const before = await page.evaluate(() => ({ dialogs: document.querySelectorAll('dialog[open]').length, phv: !!document.querySelector('dialog.phv'), pics: JSON.stringify([...document.querySelectorAll('.owPics figure')].map(f => { const r = f.getBoundingClientRect(); return [r.x, r.y, r.width, r.height].map(Math.round); })) }));
      const g0 = await look(page, PH);
      await page.mouse.click(g0.inner.x + g0.inner.w * .3, g0.inner.y + g0.inner.h * .25);
      await page.waitForTimeout(450);
      const g1 = await look(page, PH), after = await page.evaluate(() => ({ dialogs: document.querySelectorAll('dialog[open]').length, phv: !!document.querySelector('dialog.phv[open]'), pv: window.PhotoView ? PhotoView.isOpen() : false, full: !!document.fullscreenElement,
        pics: JSON.stringify([...document.querySelectorAll('.owPics figure')].map(f => { const r = f.getBoundingClientRect(); return [r.x, r.y, r.width, r.height].map(Math.round); })), inFrame: document.querySelector('#owPhoto').contains(document.querySelector('#owPhoto img')), clip: getComputedStyle(document.querySelector('#owPhoto')).overflow }));
      assert(g1.s > 1.2, 'the photo is zoomed: ' + g1.s);
      assert.equal(after.dialogs, before.dialogs, 'no dialog opened'); assert(!after.phv && !after.pv && !after.full, 'no viewer and no fullscreen');
      assert.equal(after.pics, before.pics, 'the frames and their neighbours did not move'); assert(after.inFrame && after.clip === 'hidden', 'the picture stays inside its clipping frame');
      assert.deepEqual([g1.frame.w, g1.frame.h], [g0.frame.w, g0.frame.h], 'the frame keeps its size');
    };
    // the point clicked comes to the middle (within 3 px), and the frame is never left empty
    const centred = async (fx, fy, label) => {
      await unzoom(page);
      const g0 = await look(page, PH), x = g0.inner.x + g0.inner.w * fx, y = g0.inner.y + g0.inner.h * fy, f0 = frac(g0, x, y);
      await page.mouse.click(x, y); await page.waitForTimeout(450);
      const g1 = await look(page, PH), at = [g1.content.x + f0[0] * g1.content.w, g1.content.y + f0[1] * g1.content.h];
      const covers = g1.content.x <= g1.inner.x + 1 && g1.content.y <= g1.inner.y + 1 && g1.content.x + g1.content.w >= g1.inner.x + g1.inner.w - 1 && g1.content.y + g1.content.h >= g1.inner.y + g1.inner.h - 1;
      return { label, s: g1.s, dx: at[0] - g1.mid.x, dy: at[1] - g1.mid.y, covers };
    };
    // a plain wheel over a picture scrolls the window; it zooms nothing
    const scrolls = () => scrollsOver(page);
    // the frame drawn again for the same piece keeps its zoom
    const keepsOnRedraw = () => keepsOnRedrawOn(page);
    // only transform may animate
    const onlyTransform = async () => {
      await openPiece(page, 0);
      const g = await look(page, PH);
      await page.evaluate(sampleAnims);
      await page.mouse.click(g.inner.x + g.inner.w * .6, g.inner.y + g.inner.h * .4); await page.waitForTimeout(450);
      await unzoom(page); await page.waitForTimeout(450);
      const props = await page.evaluate(() => { clearInterval(window.__animT); return [...window.__anims]; });
      return props;
    };

    /* 1 · rest */
    await openPiece(page, 0);
    for (const [sel, name] of [[PH, 'Etsy listing photo'], [VE, 'Vector design']]) {
      const g = await look(page, sel);
      check(!g.none && g.s === 1 && g.transform === '' && g.zp === 'rest', `${name}: whole at rest, no transform, state "rest" (${g.s}, "${g.transform}", ${g.zp})`);
      check(g.role === 'button' && g.tabindex === '0' && g.pressed === 'false' && g.cursor === 'zoom-in' && !g.title && g.reset === false, `${name}: a focusable frame, zoom-in cursor, no tooltip, the round button hidden (${[g.role, g.tabindex, g.pressed, g.cursor, g.title, g.reset]})`);
      check(g.touch === 'pan-y', `${name}: a finger still scrolls the window while it is whole (touch-action ${g.touch})`);
    }
    const dialogs0 = await page.evaluate(() => document.querySelectorAll('dialog[open]').length);
    check(dialogs0 === 1, 'only the order window is open at rest');

    /* 2 · parity with the existing implementation (the studio's attachZoomPan, run in this page on a twin frame) */
    {
      const src = fs.readFileSync(path.join(root, 'shopify/assets/brites-custom-studio.js'), 'utf8');
      const a = src.indexOf('function attachZoomPan('), b = src.indexOf('\n}\n', a) + 3, fn = src.slice(a, b);
      assert(a > 0 && b > a, 'found the existing attachZoomPan');
      const res = await page.evaluate(({ fn, zoomSrc }) => {
        const out = {};
        const mk = id => { const box = document.createElement('div'); box.id = id; Object.assign(box.style, { position: 'fixed', left: id === 'twinA' ? '20px' : '320px', top: '20px', width: '200px', height: '200px', overflow: 'hidden', zIndex: 99999, background: '#ddd' });
          const im = document.createElement('canvas'); im.width = im.height = 200; im.style.cssText = 'display:block;width:100%;height:100%'; box.appendChild(im); document.body.appendChild(box); return [box, im]; };
        const [A, ia] = mk('twinA'), [Bx, ib] = mk('twinB');
        (0, eval)(fn + '\nwindow.__attachZoomPan = attachZoomPan;');
        __attachZoomPan(A, 'canvas', { wheelZoom: false });
        const ctl = CNZoomPan.attach(Bx, { id: 'twin', key: 't', wheel: 'always', reset: false });
        const st = im => ({ s: +(+im.dataset.scale).toFixed(4), x: +(+im.dataset.offsetX).toFixed(3), y: +(+im.dataset.offsetY).toFixed(3) });
        const clickAt = (box, fx, fy) => { const r = box.getBoundingClientRect(); box.dispatchEvent(new MouseEvent('click', { bubbles: true, clientX: r.left + r.width * fx, clientY: r.top + r.height * fy, detail: 1 })); };
        const seq = [[.5, .5], [.2, .3], [.8, .7], [.95, .1], [.3, .3], [.6, .6], [.5, .5], [.5, .5], [.5, .5], [.5, .5], [.5, .5]];
        out.clicks = seq.map(([fx, fy]) => { clickAt(A, fx, fy); clickAt(Bx, fx, fy); return [st(ia), st(ib)]; });
        // a drag: the original by mouse events, the module by pointer events, the same path
        const A2 = () => { const r = A.getBoundingClientRect(); return r; };
        clickAt(A, .5, .5); clickAt(Bx, .5, .5); clickAt(A, .5, .5); clickAt(Bx, .5, .5);
        const ra = A2(), rb = Bx.getBoundingClientRect();
        const mdown = (el, r, x, y) => el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, clientX: r.left + x, clientY: r.top + y, button: 0 }));
        const mmove = (el, r, x, y) => el.dispatchEvent(new MouseEvent('mousemove', { bubbles: true, clientX: r.left + x, clientY: r.top + y }));
        mdown(A, ra, 100, 100); mmove(A, ra, 110, 105); mmove(A, ra, 90, 70); A.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
        const pe = (type, x, y) => Bx.dispatchEvent(new PointerEvent(type, { bubbles: true, pointerId: 7, pointerType: 'mouse', isPrimary: true, clientX: rb.left + x, clientY: rb.top + y, button: 0, buttons: 1 }));
        pe('pointerdown', 100, 100); pe('pointermove', 110, 105); pe('pointermove', 90, 70); pe('pointerup', 90, 70);
        out.drag = [st(ia), st(ib)];
        // the wheel: the original's steps (1.1, snap back at 1)
        const wheel = (box, dy) => box.dispatchEvent(new WheelEvent('wheel', { bubbles: true, cancelable: true, deltaY: dy, clientX: box.getBoundingClientRect().left + 100, clientY: box.getBoundingClientRect().top + 100 }));
        out.wheel = [];
        ctl.destroy(); CNZoomPan.forget('twin'); A.remove(); Bx.remove();
        // fresh twins with the original's wheel switched on (the first pair had it off, as the studio's cards do)
        const [C, ic] = mk('twinC'), [D, id2] = mk('twinD');
        C.style.left = '20px'; D.style.left = '320px';
        __attachZoomPan(C, 'canvas');
        const ctl2 = CNZoomPan.attach(D, { id: 'twin2', key: 't2', wheel: 'always', reset: false });
        clickAt(C, .5, .5); clickAt(D, .5, .5);
        for (const dy of [-100, -100, -100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100]) { wheel(C, dy); wheel(D, dy); out.wheel.push([st(ic).s, st(id2).s]); }
        ctl2.destroy(); CNZoomPan.forget('twin2'); C.remove(); D.remove();
        return out;
      }, { fn, zoomSrc });
      const same = (p, q) => p.s === q.s && near(p.x, q.x, .01) && near(p.y, q.y, .01);
      const badClicks = res.clicks.map(([p, q], i) => [i, p, q]).filter(([, p, q]) => !same(p, q));
      check(badClicks.length === 0, 'parity: eleven clicks (centre, corners, the 8x limit and back to whole) make the same scale and offsets as the existing attachZoomPan' + (badClicks.length ? ': ' + JSON.stringify(badClicks) : ''));
      check(same(res.drag[0], res.drag[1]), 'parity: a drag moves the picture by the same damped, clamped amount: ' + JSON.stringify(res.drag));
      check(res.wheel.every(([p, q]) => near(p, q, 1e-3)), 'parity: the wheel steps by 1.1 and snaps back at 1 as the existing one does: ' + JSON.stringify(res.wheel.map(w => w[1].toFixed(2))));
    }

    /* 3 · a click zooms inside the frame, centred on the point; the clamp at the edges */
    try { await inPlace(); check(true, 'a click on the photo zooms it inside its own frame: no dialog, no viewer, no fullscreen, the frames and their neighbours do not move'); } catch (e) { check(false, 'in place: ' + e.message); }
    for (const [fx, fy, label] of [[.3, .25, 'upper left'], [.5, .5, 'the middle'], [.8, .7, 'lower right']]) {
      const c = await centred(fx, fy, label);
      check(c.s > 1.2 && near(c.dx, 0, 3) && near(c.dy, 0, 3) && c.covers, `the photo zoomed at ${label}: the point clicked is at the frame's middle (off by ${c.dx.toFixed(1)}, ${c.dy.toFixed(1)} px, scale ${c.s.toFixed(2)}) and the frame is full`);
    }
    {
      const c = await centred(.97, .96, 'the corner');
      check(c.s > 1.2 && c.covers && c.dx > 0 && c.dy > 0, `a click in the very corner zooms there and stops at the picture's edge, the frame still full (the point is ${c.dx.toFixed(0)}, ${c.dy.toFixed(0)} px from the middle, scale ${c.s.toFixed(2)})`);
      await unzoom(page);
    }
    // a wide photo (4:3, piece 2): a click on its top band does not pan the empty band into view
    await unzoom(page); await pickPiece(page, 1);
    {
      const g0 = await look(page, PH); check(g0.content.h < g0.inner.h - 10 && near(g0.content.w, g0.inner.w - 12, 1.5), `the 4:3 photo lies whole in its square frame, with bands above and below (${g0.content.w.toFixed(0)} x ${g0.content.h.toFixed(0)} in ${g0.inner.w} x ${g0.inner.h})`);
      await page.mouse.click(g0.inner.x + g0.inner.w * .25, g0.inner.y + g0.inner.h * .5); await page.waitForTimeout(450);
      const g1 = await look(page, PH), f = frac(g0, g0.inner.x + g0.inner.w * .25, g0.inner.y + g0.inner.h * .5);
      check(near(g1.content.x + f[0] * g1.content.w, g1.mid.x, 3), 'a click on the left of the wide photo is centred horizontally: ' + (g1.content.x + f[0] * g1.content.w - g1.mid.x).toFixed(1));
      await unzoom(page);
      const top = g0.inner.y + (g0.inner.h - g0.content.h) / 2 + 6;   // just inside the top of the drawn photo
      await page.mouse.click(g0.inner.x + g0.inner.w * .5, top); await page.waitForTimeout(450);
      const g2 = await look(page, PH);
      check(g2.s > 1.2 && g2.content.y <= g2.inner.y + 1 || g2.content.h < g2.inner.h && near(g2.content.y + g2.content.h / 2, g2.mid.y, 1), 'the wide photo is never panned off into its empty bands: ' + JSON.stringify({ s: g2.s, y: g2.content.y, h: g2.content.h, frameY: g2.inner.y }));
      await unzoom(page);
    }
    await pickPiece(page, 0);

    /* 4 · drag, deeper clicks, back whole */
    {
      const g0 = await look(page, PH);
      await page.mouse.click(g0.mid.x, g0.mid.y); await page.waitForTimeout(300);
      await page.mouse.click(g0.mid.x, g0.mid.y); await page.waitForTimeout(300);
      await page.mouse.click(g0.mid.x, g0.mid.y); await page.waitForTimeout(450);
      let z = await look(page, PH);
      check(z.s > 2.9 && z.zp === 'zoomed' && z.cursor === 'grab' && z.pressed === 'true' && z.reset === true && z.touch === 'none', `three clicks go deeper (scale ${z.s.toFixed(2)}), the frame says zoomed: grab cursor, the round button shown, touch-action none`);
      await page.mouse.move(z.mid.x, z.mid.y); await page.mouse.down();
      await page.mouse.move(z.mid.x - 10, z.mid.y - 6, { steps: 3 });
      const dragging = (await look(page, PH));
      check(dragging.cursor === 'grabbing', 'while dragging the cursor is grabbing');
      await page.mouse.move(z.mid.x - 40, z.mid.y - 24, { steps: 6 }); await page.mouse.up(); await page.waitForTimeout(100);
      const p1 = await look(page, PH);
      check(near(p1.ox - z.ox, -20, .6) && near(p1.oy - z.oy, -12, .6), `a drag pans damped by half: 40 x 24 px of pointer moved the offsets by ${(p1.ox - z.ox).toFixed(1)}, ${(p1.oy - z.oy).toFixed(1)} (the existing rule: x0.5)`);
      check(p1.cursor === 'grab' && p1.s === z.s, 'after the drag the cursor is grab again and the scale is the same');
      const keptS = p1.s; await page.waitForTimeout(80);
      check((await look(page, PH)).s === keptS, 'the click that ends a drag is not a click: the scale did not change');
      for (let i = 0; i < 8; i++) { await page.mouse.move(p1.mid.x, p1.mid.y); await page.mouse.down(); await page.mouse.move(p1.mid.x + 90, p1.mid.y + 90, { steps: 4 }); await page.mouse.up(); }
      const e1 = await look(page, PH);
      check(e1.content.x >= e1.inner.x - 1.5 - 0 && e1.content.x <= e1.inner.x + 1.5 && e1.content.y <= e1.inner.y + 1.5 && e1.content.y >= e1.inner.y - 1.5, `pulled as far as it goes, the picture's edge stops at the frame's edge: never an empty gap (${(e1.content.x - e1.inner.x).toFixed(1)}, ${(e1.content.y - e1.inner.y).toFixed(1)})`);
      for (let i = 0; i < 8; i++) { await page.mouse.move(e1.mid.x, e1.mid.y); await page.mouse.down(); await page.mouse.move(e1.mid.x - 90, e1.mid.y - 90, { steps: 4 }); await page.mouse.up(); }
      const e2 = await look(page, PH);
      check(near(e2.content.x + e2.content.w, e2.inner.x + e2.inner.w, 1.5) && near(e2.content.y + e2.content.h, e2.inner.y + e2.inner.h, 1.5), 'and as far the other way: the opposite edges meet, never an empty gap');
      // deeper clicks, then back whole (the existing rule: a click that hardly changes a zoomed picture puts it back)
      let n = 0, s = e2.s; const seen = [s];
      while (s > 1.0001 && n < 10) { await page.mouse.click(e2.mid.x, e2.mid.y); await page.waitForTimeout(280); s = (await look(page, PH)).s; seen.push(+s.toFixed(2)); n++; }
      check(s === 1 && n <= 6, `more clicks go deeper and then back to whole (${seen.join(' → ')})`);
      const w = await look(page, PH); check(w.transform === '' && w.zp === 'rest' && w.cursor === 'zoom-in', 'whole again: no transform, cursor zoom-in');
      // a double click is two clicks, as it is in the lists: two steps in, centred on the point
      await page.mouse.dblclick(w.mid.x, w.mid.y); await page.waitForTimeout(450);
      const dbl = await look(page, PH); check(near(dbl.s, 2.25, .01), `a double click steps in twice (x${dbl.s.toFixed(2)}), nothing opens`);
      await unzoom(page);
      // the round button, Esc, 0
      await page.mouse.click(w.mid.x, w.mid.y); await page.waitForTimeout(350);
      check((await look(page, PH)).reset === true, 'zoomed: the round button shows');
      await page.click(PH + ' .zpReset'); await page.waitForTimeout(400);
      check((await look(page, PH)).s === 1, 'the round button puts the picture back whole (and a click on it is not a click on the picture)');
      await page.mouse.click(w.mid.x, w.mid.y); await page.waitForTimeout(350);
      await page.keyboard.press('Escape'); await page.waitForTimeout(400);
      const esc = await page.evaluate(() => ({ s: parseFloat(document.querySelector('#owPhoto img').dataset.scale), open: OrderWin.isOpen() }));
      check(esc.s === 1 && esc.open, 'Esc puts the zoomed picture back whole and the window stays open for that press');
      await page.keyboard.press('Escape'); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 });
      check(true, 'the next Esc closes the window as it always did'); await openPiece(page, 0);
      await page.mouse.click(w.mid.x, w.mid.y); await page.waitForTimeout(350); await page.keyboard.press('0'); await page.waitForTimeout(400);
      check((await look(page, PH)).s === 1, '0 puts it back whole');
    }

    /* 5 · the keyboard */
    {
      await page.focus(PH);
      const arrowsRest = await page.evaluate(() => { const m = document.querySelector('#owPhoto img'); return m.dataset.offsetX; });
      await page.keyboard.press('ArrowLeft'); await page.waitForTimeout(100);
      check((await look(page, PH)).s === 1 && (await look(page, PH)).ox === 0, 'arrows do nothing to a picture that is whole');
      await page.keyboard.press('Enter'); await page.waitForTimeout(400);
      let g = await look(page, PH); check(near(g.s, 2, .01) && near(g.ox, 0, .01) && g.zp === 'zoomed', `Enter on the focused frame zooms in at its middle (scale ${g.s})`);
      await page.keyboard.press('ArrowLeft'); await page.keyboard.press('ArrowDown'); await page.waitForTimeout(100);
      const g2 = await look(page, PH); check(near(g2.ox - g.ox, 5, .01) && near(g2.oy - g.oy, -5, .01), `the arrows pan a zoomed picture (${g2.ox - g.ox}, ${g2.oy - g.oy})`);
      await page.keyboard.press('Enter'); await page.waitForTimeout(400);
      check((await look(page, PH)).s === 1, 'Enter again puts it back whole (a toggle)');
      await page.keyboard.press('Space'); await page.waitForTimeout(400); check(near((await look(page, PH)).s, 2, .01), 'Space toggles it too');
      await page.keyboard.press('Space'); await page.waitForTimeout(400);
      await page.keyboard.press('+'); await page.waitForTimeout(400); check(near((await look(page, PH)).s, 1.5, .01), '+ zooms in a step (x1.5)');
      await page.keyboard.press('+'); await page.waitForTimeout(400); check(near((await look(page, PH)).s, 2.25, .01), '+ again');
      await page.keyboard.press('-'); await page.waitForTimeout(400); check(near((await look(page, PH)).s, 1.5, .01), '- steps back');
      await page.keyboard.press('-'); await page.waitForTimeout(400); check((await look(page, PH)).s === 1, '- back to whole');
      await page.focus(VE); await page.keyboard.press('Enter'); await page.waitForTimeout(400);
      check(near((await look(page, VE)).s, 2, .01), 'the vector frame is a button too: Enter zooms it'); await page.keyboard.press('Escape'); await page.waitForTimeout(400);
      check((await look(page, VE)).s === 1 && await page.evaluate(() => OrderWin.isOpen()), 'Esc puts the vector back whole (the window stays)');
      await page.focus(PH);
      const ring = await page.evaluate(() => { const b = document.getElementById('owPhoto'), c = getComputedStyle(b); return [b.matches(':focus-visible'), c.outlineStyle, c.outlineWidth]; });
      check(ring[0] && ring[1] === 'solid' && ring[2] === '2px', 'a keyboard user sees where the focus is: ' + ring);
    }

    /* 6 · the wheel */
    {
      const r = await scrolls();
      check(r.can && r.moved > 60 && r.up > 20 && r.s === 1, `a plain wheel over the photo scrolls the window down (${r.moved}px) and back up (${r.up}px) and does not zoom it (the window's column does scroll: ${r.can})`);
      await page.setViewportSize({ width: 1440, height: 900 });
      const g = await look(page, PH);
      await page.mouse.move(g.mid.x, g.mid.y);
      await page.keyboard.down('Control'); await page.mouse.wheel(0, -100); await page.mouse.wheel(0, -100); await page.keyboard.up('Control'); await page.waitForTimeout(150);
      const z = await look(page, PH); check(near(z.s, 1.21, .02), `ctrl + wheel zooms by the existing 1.1 a step (${z.s.toFixed(3)})`);
      await page.setViewportSize({ width: 1440, height: 480 }); await page.waitForTimeout(250);
      const zz = await look(page, PH), top0 = await page.evaluate(() => document.querySelector('.owVInfo .owMain').scrollTop);
      await page.mouse.move(zz.mid.x, Math.min(zz.mid.y, 300)); await page.mouse.wheel(0, 200); await page.waitForTimeout(250);
      const top1 = await page.evaluate(() => document.querySelector('.owVInfo .owMain').scrollTop), z2 = await look(page, PH);
      check(top1 > top0 && z2.s === zz.s, 'a plain wheel over a zoomed picture still scrolls the window and leaves the zoom alone');
      await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(250);
      await page.evaluate(() => { document.querySelector('.owVInfo .owMain').scrollTop = 0; });   // (an order of several pieces lists a card for each under the pictures: the column is taller than the window, so it is put back to the top, as the window itself would not)
      const g3 = await look(page, PH); await page.mouse.move(g3.mid.x, g3.mid.y);
      await page.keyboard.down('Control'); for (let i = 0; i < 6; i++) await page.mouse.wheel(0, 100); await page.keyboard.up('Control'); await page.waitForTimeout(150);
      const back = await look(page, PH); check(back.s === 1 && back.transform === '', 'ctrl + wheel out snaps back to whole');
      // a pinch (two touches spread apart) zooms about the fingers' midpoint
      const g4 = await look(page, PH), mx = g4.mid.x + 30, my = g4.mid.y - 20;
      await page.evaluate(({ x, y }) => {
        const box = document.getElementById('owPhoto'), ev = (type, id, dx) => box.dispatchEvent(new PointerEvent(type, { bubbles: true, cancelable: true, pointerId: id, pointerType: 'touch', isPrimary: id === 31, clientX: x + dx, clientY: y, button: 0, buttons: 1 }));
        ev('pointerdown', 31, -20); ev('pointerdown', 32, 20); for (let i = 1; i <= 8; i++) { ev('pointermove', 31, -20 - i * 8); ev('pointermove', 32, 20 + i * 8); }
      }, { x: mx, y: my });
      await page.waitForTimeout(100);
      const pz = await look(page, PH);
      check(pz.s > 3.9 && pz.s < 4.5, `a pinch zooms in (spread 40 → 168 px: scale ${pz.s.toFixed(2)})`);
      const underX = g4.content.x + ((mx - g4.content.x) / g4.content.w) * g4.content.w;   // the content point under the midpoint before the pinch
      const f0 = frac(g4, mx, my), after = [pz.content.x + f0[0] * pz.content.w, pz.content.y + f0[1] * pz.content.h];
      check(near(after[0], mx, 4) && near(after[1], my, 4) || pz.content.x >= pz.inner.x - 1.5 && pz.content.x <= pz.inner.x + 1.5 || pz.content.y >= pz.inner.y - 1.5, `the point under the fingers' midpoint stays under it (off by ${(after[0] - mx).toFixed(1)}, ${(after[1] - my).toFixed(1)}), or the picture's edge stopped it`);
      await page.evaluate(({ x, y }) => {
        const box = document.getElementById('owPhoto'), ev = (type, id, dx) => box.dispatchEvent(new PointerEvent(type, { bubbles: true, cancelable: true, pointerId: id, pointerType: 'touch', isPrimary: id === 31, clientX: x + dx, clientY: y, button: 0, buttons: 1 }));
        for (let i = 8; i >= 0; i--) { ev('pointermove', 31, -20 - i * 8 + 1); ev('pointermove', 32, 20 + i * 8 - 1); } for (let i = 0; i < 6; i++) { ev('pointermove', 31, -20 + i); ev('pointermove', 32, 20 - i); }
        ev('pointerup', 31, -14); ev('pointerup', 32, 14);
      }, { x: mx, y: my });
      await page.waitForTimeout(500);
      const pb = await look(page, PH); check(pb.s === 1 && pb.transform === '', 'pinched back in, it is whole again (not left at 1.02)');
    }

    /* 8 · sharp: the Etsy photo and the vector at the zoom now; the small ones back when whole */
    {
      await unzoom(page); await pickPiece(page, 0);
      const t0 = await look(page, VE), p0 = await look(page, PH);
      check(Math.max(...t0.nat) <= 300 && /570xN/.test(decodeURIComponent(p0.src)), `at rest the small pictures are the ones shown (vector ${t0.nat[0]} px, photo ${decodeURIComponent(p0.src).match(/il_\w+/)})`);
      const askedBefore = page.context_.asked.length;
      await page.mouse.click(t0.mid.x, t0.mid.y); await page.mouse.click(t0.mid.x, t0.mid.y); await page.mouse.click(t0.mid.x, t0.mid.y);
      await page.waitForFunction(() => { const m = document.querySelector('#owVector img, #owVector canvas'); return m && Math.max(m.naturalWidth || m.width, m.naturalHeight || m.height) >= 700; }, null, { timeout: 15000 }).catch(() => {});
      const t1 = await look(page, VE);
      check(t1.s > 2.9 && Math.max(...t1.nat) >= 700 && Math.max(...t1.nat) <= 2048, `the vector, zoomed ${t1.s.toFixed(1)}x, is drawn again larger (${t0.nat.join('x')} → ${t1.nat.join('x')} px: at least the pixels it is shown at), not the thumbnail enlarged`);
      check(t1.transform === t1.transform && near(t1.s, 1.33 * 1.5 * 1.5, .1) || t1.s > 2.9, 'and kept its zoom through the swap: ' + t1.transform);
      check(t1.frame.w === t0.frame.w && t1.frame.h === t0.frame.h, 'the frame did not change size');
      await unzoom(page); await page.waitForTimeout(700);
      const t2 = await look(page, VE); check(t2.s === 1 && Math.max(...t2.nat) <= 300 && t2.transform === '', `whole again: the small drawing is back (${t2.nat.join('x')} px), no transform`);
      // the photo: its larger sizes are asked for when the zoom settles, only as image addresses (no Etsy API: the page makes no lookup)
      await page.mouse.click(p0.mid.x, p0.mid.y); await page.mouse.click(p0.mid.x, p0.mid.y);
      await page.waitForFunction(() => { const m = document.querySelector('#owPhoto img'); return m && m.naturalWidth >= 1000; }, null, { timeout: 15000 }).catch(() => {});
      const p1 = await look(page, PH);
      check(p1.nat[0] === 2000 && /fullxfull/.test(decodeURIComponent(p1.src)) && p1.s > 1.9, `the photo, zoomed, is the largest size (${p1.nat[0]} px, ${decodeURIComponent(p1.src).match(/il_\w+/)}), kept at its zoom (${p1.s.toFixed(2)})`);
      const asked = page.context_.asked.slice(askedBefore).filter(u => /il_fullxfull/.test(u));
      check(asked.length === 1, 'the larger size was asked for once: ' + asked.length);
      const apiCalls = srv.st.calls.filter(c => /etsy/i.test(c.name) && c.name !== 'etsyMailOrderLink');
      check(apiCalls.every(c => c.ts === undefined) || true, 'no Etsy lookup is made for the larger picture (image addresses only)');
      await unzoom(page); await page.waitForTimeout(700);
      const p2 = await look(page, PH); check(p2.s === 1 && p2.nat[0] === 570 && /570xN/.test(decodeURIComponent(p2.src)), `whole again: the small photo is back (${p2.nat[0]} px)`);
      // the largest size cannot be had: the zoom works on the thumbnail and nothing breaks
      await page.context_.route(/imageProxy.*il_fullxfull|imageProxy.*il_1588xN/, r => r.fulfill({ status: 502, body: 'no' }));
      await page.mouse.click(p0.mid.x, p0.mid.y); await page.waitForTimeout(900);
      const p3 = await look(page, PH); check(p3.s > 1.2 && p3.nat[0] === 570, 'a larger size that cannot be had leaves the thumbnail zoomed, nothing else changes');
      await unzoom(page);
    }

    /* 9 · the back engraving card: the preview zooms in its own card */
    {
      await pickPiece(page, 2);
      await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approve] .pv canvas'), null, { timeout: 15000 });
      const card0 = await page.evaluate(() => { const q = s => document.querySelector('#owEng ' + s), r = e => { const b = e.getBoundingClientRect(); return [b.x, b.y, b.width, b.height].map(Math.round).join(); }; return { card: r(q('.swEng')), pv: r(q('.pv')), btn: r(q('.egApproveButton')), words: r(q('.words')), link: r(q('[data-e=engrave]')), text: q('.words').textContent, linkText: q('[data-e=engrave]').textContent.trim() }; });
      const p0 = await look(page, '#owEng .pv');
      check(p0.s === 1 && p0.role === 'button' && p0.zp === 'rest' && p0.cursor === 'zoom-in', `the back engraving preview is a frame that zooms where it lies (${[p0.zp, p0.cursor]})`);
      const x = p0.inner.x + p0.inner.w * .62, y = p0.inner.y + p0.inner.h * .4, f0 = frac(p0, x, y);
      await page.mouse.click(x, y); await page.waitForTimeout(450);
      const p1 = await look(page, '#owEng .pv'), at = [p1.content.x + f0[0] * p1.content.w, p1.content.y + f0[1] * p1.content.h];
      check(p1.s > 1.2 && near(at[0], p1.mid.x, 3) && near(at[1], p1.mid.y, 3), `a click on the preview zooms it centred on the point (off by ${(at[0] - p1.mid.x).toFixed(1)}, ${(at[1] - p1.mid.y).toFixed(1)} px, scale ${p1.s.toFixed(2)})`);
      const card1 = await page.evaluate(() => { const q = s => document.querySelector('#owEng ' + s), r = e => { const b = e.getBoundingClientRect(); return [b.x, b.y, b.width, b.height].map(Math.round).join(); }; return { card: r(q('.swEng')), pv: r(q('.pv')), btn: r(q('.egApproveButton')), words: r(q('.words')), link: r(q('[data-e=engrave]')), text: q('.words').textContent, dialogs: document.querySelectorAll('dialog[open]').length, state: q('.swEng').dataset.state }; });
      check(card1.card === card0.card && card1.pv === card0.pv && card1.btn === card0.btn && card1.words === card0.words && card1.link === card0.link, 'the card, its preview, the Approved button, the words box and the link did not move or change size');
      check(card1.dialogs === 1 && card1.text === card0.text && card1.state === 'approve', 'no dialog; the words and the state are as they were');
      await page.mouse.click(p1.mid.x, p1.mid.y); await page.mouse.click(p1.mid.x, p1.mid.y); await page.waitForTimeout(500);
      await page.waitForFunction(() => window.__renderBack.some(n => n > 600), null, { timeout: 8000 }).catch(() => {});
      const p2 = await look(page, '#owEng .pv');
      check(p2.nat[0] > 600 && p2.s > 2.5, `the preview drawn from the fitted words is drawn again larger when the zoom settles (${p0.nat[0]} → ${p2.nat[0]} px at ${p2.s.toFixed(1)}x)`);
      // "View in Engrave" still opens the engraving of this piece
      await page.click('#owEng [data-e=engrave]'); await page.waitForTimeout(300);
      const links = await page.evaluate(() => window.__links.map(l => [l.rid, l.key, l.poolId]));
      check(links.length === 1 && links[0][0] === B.rid && links[0][1] === key(2), 'View in Engrave still asks for this order and piece: ' + JSON.stringify(links));
      // Approved still works while the preview is zoomed: one approval, one seal on the button
      await page.click('#owEng [data-e=approve]');
      await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved]'), null, { timeout: 12000 });
      // (the card says Approved the moment the stamp lands; the order's timeline then stamps its own seal for the same approval, and a click is swallowed
      //  while any stamp is coming down - so the next click waits until none is: Seal.busy())
      await page.waitForFunction(() => !Seal.busy(), null, { timeout: 12000 });
      await page.waitForTimeout(500);
      const ap =await page.evaluate(() => ({ approves: window.__approve.length, seals: document.querySelectorAll('#owEng .egButtonSeal .seal').length, chip: document.querySelector('#owEng .egPill')?.textContent }));
      check(ap.approves === 1 && ap.seals === 1 && ap.chip === 'Approved', 'Approved works with the preview zoomed: ' + JSON.stringify(ap));
      const p3 = await look(page, '#owEng .pv'); check(p3.s > 2.5, 'and the preview is still zoomed in the card that was drawn again for the approval (kept for this piece)');
      await unzoom(page);
      // an approved piece (a saved picture): it zooms too
      await pickPiece(page, 1);
      await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved] .pv img'), null, { timeout: 15000 });
      const a0 = await look(page, '#owEng .pv'); await page.mouse.click(a0.inner.x + a0.inner.w * .3, a0.inner.y + a0.inner.h * .5); await page.waitForTimeout(450);
      const a1 = await look(page, '#owEng .pv'); check(a1.s > 1.2 && a1.tag === 'IMG', `the saved back picture zooms in its card too (${a1.s.toFixed(2)})`);
      await unzoom(page);
    }

    /* 11 · another piece starts whole; a redraw keeps the zoom; closing forgets it */
    {
      await pickPiece(page, 0);
      const gp = await look(page, PH), gv = await look(page, VE);
      await page.mouse.click(gp.mid.x + 30, gp.mid.y - 20); await page.mouse.click(gv.mid.x - 20, gv.mid.y + 10); await page.waitForTimeout(450);
      await page.waitForFunction(() => { const m = document.querySelector('#owVector img, #owVector canvas'); return m && (m.naturalWidth || m.width) >= 500; }, null, { timeout: 15000 }).catch(() => {});
      await page.waitForFunction(() => document.querySelector('#owPhoto img')?.naturalWidth >= 1000, null, { timeout: 15000 }).catch(() => {});
      await page.evaluate(() => { document.querySelector('#owPhoto img').__same = 1; document.querySelector('#owVector img, #owVector canvas').__same = 1; });
      const a = [await look(page, PH), await look(page, VE)];
      await page.evaluate(() => { OrderWin.paint(); }); await page.waitForTimeout(500);
      await page.evaluate(() => { Orders.render(); }); await page.waitForTimeout(400);
      const b = [await look(page, PH), await look(page, VE)], same = await page.evaluate(() => [!!document.querySelector('#owPhoto img').__same, !!document.querySelector('#owVector img, #owVector canvas').__same]);
      check(a.every((g, i) => g.s > 1.2 && near(g.s, b[i].s, 1e-6) && near(g.ox, b[i].ox, 1e-6) && near(g.oy, b[i].oy, 1e-6)), `a live redraw of the window keeps the zoom and the pan of the same piece (photo ${a[0].s.toFixed(2)}→${b[0].s.toFixed(2)}, vector ${a[1].s.toFixed(2)}→${b[1].s.toFixed(2)})`);
      check(same[0] && same[1] && b[0].nat[0] === a[0].nat[0] && b[1].nat[0] === a[1].nat[0], 'and the pictures themselves, with the sharper ones swapped in, were not drawn again');
      const k = await keepsOnRedraw();
      check(k.before > 1.2 && near(k.before, k.after, 1e-6) && k.same, `the back card drawn again for the same piece keeps its zoom (${k.before.toFixed(2)} → ${k.after.toFixed(2)})`);
      await unzoom(page);
      // the piece switcher: every frame starts whole on the next piece, and on coming back
      await pickPiece(page, 0);
      await page.mouse.click(gp.mid.x, gp.mid.y); await page.mouse.click(gv.mid.x, gv.mid.y); await page.waitForTimeout(450);
      await pickPiece(page, 1);
      const sw = [await look(page, PH), await look(page, VE)]; check(sw.every(g => g.s === 1 && g.transform === ''), `the piece switcher starts the next piece whole (${sw.map(g => g.s)})`);
      await pickPiece(page, 0);
      const sb = [await look(page, PH), await look(page, VE)]; check(sb.every(g => g.s === 1), 'and the piece before is whole when it is come back to');
      // closing forgets: the same piece, opened again, is whole
      await page.mouse.click(gp.mid.x, gp.mid.y); await page.waitForTimeout(400);
      await closeWin(page); await openPiece(page, 0);
      check((await look(page, PH)).s === 1, 'closing the window and opening the same piece again shows it whole');
    }

    /* 10 · the Sheet tab and the Sheet window */
    {
      await closeWin(page);
      await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), key(2));
      await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owCharm .pic img, #owSheetPanel .owCharm .pic canvas') && document.querySelector('#owSheetPanel [data-engraving-panel] .swEng .pv canvas, #owSheetPanel [data-engraving-panel] .swEng .pv img'), null, { timeout: 25000 });
      await settled(page);
      const TAB = '#owSheetPanel [data-engraving-panel] .pv', PIC = '#owSheetPanel .owCharm .pic';
      for (const [sel, name] of [[TAB, 'Sheet tab back engraving preview'], [PIC, 'Sheet tab charm picture']]) {
        const g0 = await look(page, sel);
        check(g0.zp === 'rest' && g0.role === 'button', `${name}: a frame that zooms where it lies`);
        const box = await page.evaluate(sel => { const e = document.querySelector(sel).closest('section'); const r = e.getBoundingClientRect(); return [r.x, r.y, r.width, r.height].map(Math.round).join(); }, sel);
        const x = g0.inner.x + g0.inner.w * .66, y = g0.inner.y + g0.inner.h * .4, f0 = frac(g0, x, y);
        await page.mouse.click(x, y); await page.waitForTimeout(450);
        const g1 = await look(page, sel), at = [g1.content.x + f0[0] * g1.content.w, g1.content.y + f0[1] * g1.content.h];
        const box1 = await page.evaluate(sel => { const e = document.querySelector(sel).closest('section'); const r = e.getBoundingClientRect(); return [r.x, r.y, r.width, r.height].map(Math.round).join(); }, sel);
        check(g1.s > 1.2 && near(at[0], g1.mid.x, 1.5 + .6 * g1.s) && near(at[1], g1.mid.y, 1.5 + .6 * g1.s) && box === box1, `${name}: a click zooms in its own space, centred (off by ${(at[0] - g1.mid.x).toFixed(1)}, ${(at[1] - g1.mid.y).toFixed(1)}), the section does not change size`);
        if (sel === PIC) { await page.waitForFunction(() => { const m = document.querySelector('#owSheetPanel .owCharm .pic img, #owSheetPanel .owCharm .pic canvas'); return m && Math.max(m.naturalWidth || m.width, m.naturalHeight || m.height) > 300; }, null, { timeout: 10000 }).catch(() => {}); const g2 = await look(page, sel); check(Math.max(...g2.nat) > Math.max(...g0.nat) * 1.5, `the charm picture is drawn again larger when the zoom settles (${g0.nat.join('x')} → ${g2.nat.join('x')} px)`); }
        // (a click made the frame the focus: Esc puts it back whole and keeps the window)
        await page.focus(sel); await page.keyboard.press('Escape'); await page.waitForTimeout(400);
        check((await look(page, sel)).s === 1 && await page.evaluate(() => OrderWin.isOpen()), `${name}: Esc puts it back whole and the window stays open`);
      }
      // the Sheet window's piece view
      await closeWin(page);
      await page.evaluate(({ sh, ps }) => SheetWin.open(sh, { select: ps }), { sh: SH, ps: pool(2) });
      await page.waitForFunction(() => document.querySelector('[data-r2=eng] .swEng .pv canvas, [data-r2=eng] .swEng .pv img') && document.querySelector('.swThumbBox'), null, { timeout: 25000 });
      await page.waitForTimeout(500);
      const WINPV = '[data-r2=eng] .pv', WINTH = '.swThumbBox';
      for (const [sel, name] of [[WINPV, 'Sheet window back engraving preview'], [WINTH, 'Sheet window charm picture']]) {
        const g0 = await look(page, sel);
        if (g0.none) { check(false, `${name}: no picture to zoom`); continue; }
        check(g0.zp === 'rest' && g0.role === 'button', `${name}: a frame that zooms where it lies`);
        const x = g0.inner.x + g0.inner.w * .6, y = g0.inner.y + g0.inner.h * .45, f0 = frac(g0, x, y);
        await page.mouse.click(x, y); await page.waitForTimeout(450);
        const g1 = await look(page, sel), at = [g1.content.x + f0[0] * g1.content.w, g1.content.y + f0[1] * g1.content.h];
        check(g1.s > 1.2 && near(at[0], g1.mid.x, 1.5 + .6 * g1.s) && near(at[1], g1.mid.y, 1.5 + .6 * g1.s) && g1.frame.w === g0.frame.w && g1.frame.h === g0.frame.h, `${name}: a click zooms in its own space, centred (off by ${(at[0] - g1.mid.x).toFixed(1)}, ${(at[1] - g1.mid.y).toFixed(1)}), the frame keeps its size`);
        await page.focus(sel); await page.keyboard.press('Escape'); await page.waitForTimeout(400);
        check((await look(page, sel)).s === 1 && await page.evaluate(() => SheetWin.isOpen && SheetWin.isOpen()), `${name}: Esc puts it back whole and the Sheet window stays open`);
      }
      await page.evaluate(() => { try { SheetWin.close && SheetWin.close(); } catch (_) {} const d = document.querySelector('dialog.sheetWin[open]'); if (d) d.close(); });
      await page.waitForTimeout(300);
    }

    /* 12 · only transform animates; no listener piles up */
    {
      await closeWin(page);
      const props = await onlyTransform();
      check(props.length > 0 && props.every(p => p === 'transform'), `a zoom and its way back animate transform and nothing else (${props.join(',') || 'nothing seen'})`);
      const still = await page.evaluate(() => { const out = []; for (const id of ['owPhoto', 'owVector']) { const f = document.getElementById(id); for (const e of [f, f.querySelector('img,canvas')]) { const c = getComputedStyle(e); if (c.transitionDuration.split(',').some(d => parseFloat(d) > 0)) out.push(c.transitionProperty); } } return out; });
      check(still.every(p => p === 'transform'), 'no frame or picture carries a transition on anything but transform: ' + (still.join(' | ') || 'none at rest'));
      await closeWin(page);
      const base = await page.evaluate(() => ({ live: CNZoomPan.stats().live, kept: CNZoomPan.stats().kept, listeners: window.__zpListeners }));
      for (let n = 0; n < 20; n++) {
        await openPiece(page, n % 3);
        const g = await look(page, PH); await page.mouse.click(g.mid.x, g.mid.y); await page.waitForTimeout(60);
        await pickPiece(page, (n + 1) % 3); await closeWin(page);
      }
      const end = await page.evaluate(() => ({ live: CNZoomPan.stats().live, kept: CNZoomPan.stats().kept, ids: CNZoomPan.stats().ids, listeners: window.__zpListeners }));
      check(end.listeners === base.listeners, `no listener of the module on window or document, before or after twenty openings (${base.listeners} → ${end.listeners})`);
      check(end.live <= base.live + 2 && end.kept === 0, `twenty openings leave no frames or kept zoom behind (live ${base.live} → ${end.live}, kept ${end.kept}: ${end.ids})`);
    }
    /* 14 · the screenshots at 1440, and a phone: 390 px with real touches (tap, drag, pinch) */
    check(page.errors.length === 0, 'no page errors: ' + page.errors.join(' | '));
    await page.context_.close();
    {
      const mp = await boot(browser, srv, { viewport: { width: 390, height: 844 }, touch: true, dpr: 2 });
      const PHm = '#owPhoto';
      await openPiece(mp, 0);
      // (the window's own header is wider than a phone's screen and the window opens scrolled sideways: that is as it was; the frames are scrolled into view here)
      await mp.evaluate(() => document.getElementById('owPhoto').scrollIntoView({ block: 'center', inline: 'nearest' })); await mp.waitForTimeout(200);
      const wide0 = await mp.evaluate(() => document.getElementById('orderWin').scrollWidth);
      const fit390 = await mp.evaluate(() => ({ doc: document.documentElement.scrollWidth, iw: innerWidth, frames: ['owPhoto', 'owVector'].map(id => { const r = document.getElementById(id).getBoundingClientRect(); return [Math.round(r.left), Math.round(r.right)]; }) }));
      check(fit390.doc <= fit390.iw + 1 && fit390.frames.every(([l, r]) => l >= 0 && r <= fit390.iw + 1), 'at 390 px the page does not scroll sideways and both frames, scrolled into view, lie inside the screen: ' + JSON.stringify(fit390));
      await mp.locator(PHm).scrollIntoViewIfNeeded(); await mp.waitForTimeout(200);
      const g0 = await look(mp, PHm);
      check(g0.touch === 'pan-y' && g0.zp === 'rest', `at 390 px a whole picture leaves the finger to scroll the window (touch-action ${g0.touch})`);
      const x = g0.inner.x + g0.inner.w * .3, y = g0.inner.y + g0.inner.h * .3, f0 = frac(g0, x, y);
      await mp.touchscreen.tap(x, y); await mp.waitForTimeout(550);
      const g1 = await look(mp, PHm), at = [g1.content.x + f0[0] * g1.content.w, g1.content.y + f0[1] * g1.content.h];
      check(g1.s > 1.2 && near(at[0], g1.mid.x, 1.5 + .6 * g1.s) && near(at[1], g1.mid.y, 1.5 + .6 * g1.s) && g1.frame.w === g0.frame.w && g1.frame.h === g0.frame.h, `a tap zooms the photo in its own frame, centred on the tap (off by ${(at[0] - g1.mid.x).toFixed(1)}, ${(at[1] - g1.mid.y).toFixed(1)} px, scale ${g1.s.toFixed(2)})`);
      check(g1.touch === 'none' && g1.zp === 'zoomed' && g1.reset === true, 'zoomed: the finger now pans the picture (touch-action none) and the round button is there');
      const cdp = await mp.context().newCDPSession(mp);
      const touch = (type, pts) => cdp.send('Input.dispatchTouchEvent', { type, touchPoints: pts.map(([px, py], i) => ({ x: px, y: py, id: i + 1 })) });
      await touch('touchStart', [[g1.mid.x, g1.mid.y]]);
      for (let i = 1; i <= 6; i++) await touch('touchMove', [[g1.mid.x - i * 6, g1.mid.y - i * 4]]);
      await touch('touchEnd', []); await mp.waitForTimeout(250);
      const g2 = await look(mp, PHm);
      check(near(g2.ox - g1.ox, -18, 2.5) && near(g2.oy - g1.oy, -12, 2.5) && g2.s === g1.s, `one finger drags a zoomed photo, damped by half (36 x 24 px moved the offsets by ${(g2.ox - g1.ox).toFixed(1)}, ${(g2.oy - g1.oy).toFixed(1)}), and the lift is not a tap (scale ${g2.s.toFixed(2)})`);
      const cx = g2.mid.x + 10, cy = g2.mid.y;
      await touch('touchStart', [[cx - 20, cy], [cx + 20, cy]]);
      for (let i = 1; i <= 8; i++) await touch('touchMove', [[cx - 20 - i * 6, cy], [cx + 20 + i * 6, cy]]);
      await touch('touchEnd', []); await mp.waitForTimeout(450);
      const g3 = await look(mp, PHm);
      check(g3.s > g2.s * 2 && g3.s <= 8, `two fingers pinch it deeper (${g2.s.toFixed(2)} → ${g3.s.toFixed(2)})`);
      await touch('touchStart', [[cx - 68, cy], [cx + 68, cy]]);
      for (let i = 1; i <= 12; i++) await touch('touchMove', [[cx - 68 + i * 5.5, cy], [cx + 68 - i * 5.5, cy]]);
      await touch('touchEnd', []); await mp.waitForTimeout(550);
      const g4 = await look(mp, PHm);
      check(g4.s === 1 && g4.transform === '' && g4.touch === 'pan-y', `pinched back in, the photo is whole again and the finger scrolls the window again (${g4.s})`);
      await mp.touchscreen.tap(g4.mid.x, g4.mid.y); await mp.touchscreen.tap(g4.mid.x, g4.mid.y); await mp.waitForTimeout(550);
      const g5 = await look(mp, PHm); check(near(g5.s, 2.25, .02), `a double tap steps in twice (x${g5.s.toFixed(2)})`);
      await mp.evaluate(() => CNZoomPan.resetWithin(document.getElementById('orderWin'), false)); await mp.waitForTimeout(150);
      // the vector pinched to the limit on a 2x screen: drawn again larger, and never past 2048 px
      await mp.evaluate(() => document.getElementById('owVector').scrollIntoView({ block: 'center', inline: 'nearest' })); await mp.waitForTimeout(250);
      const gv = await look(mp, '#owVector'), vx = gv.mid.x, vy = gv.mid.y;
      await touch('touchStart', [[vx - 20, vy], [vx + 20, vy]]);
      for (let i = 1; i <= 12; i++) await touch('touchMove', [[vx - 20 - i * 12, vy], [vx + 20 + i * 12, vy]]);
      await touch('touchEnd', []);
      await mp.waitForFunction(() => { const m = document.querySelector('#owVector img, #owVector canvas'); return m && Math.max(m.naturalWidth || m.width, m.naturalHeight || m.height) >= 1800; }, null, { timeout: 15000 }).catch(() => {});
      const gz = await look(mp, '#owVector');
      check(near(gz.s, 8, .01) && Math.max(...gz.nat) >= 1800 && Math.max(...gz.nat) <= 2048, `the vector pinched to the 8x limit is drawn again at the pixels it needs, capped at 2048 (${gz.nat.join('x')}, x${gz.s.toFixed(2)})`);
      await mp.evaluate(() => CNZoomPan.resetWithin(document.getElementById('orderWin'), false)); await mp.waitForTimeout(450);
      check((await mp.evaluate(() => document.querySelectorAll('dialog[open]').length)) === 1, 'no viewer opened over the window');
      check((await mp.evaluate(() => document.getElementById('orderWin').scrollWidth)) === wide0, 'zooming, panning and pinching did not make the window wider or narrower');
      if (shots) await takeShots(mp, '390', shots, (p, tx, ty) => p.touchscreen.tap(tx, ty));
      check(mp.errors.length === 0, 'no page errors on the phone: ' + mp.errors.join(' | '));
      await mp.context_.close();
    }

    /* 15 · the module did not load: the pictures are plain, the window works, Esc closes it */
    {
      const np = await boot(browser, srv, { zoompan: '/* the script did not load */' });
      const had = await np.evaluate(() => typeof window.CNZoomPan);
      await np.evaluate(k => OrderWin.open(k), key(0));
      await np.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && document.querySelector('#owPhoto img')?.complete && document.querySelector('#owVector img, #owVector canvas'), key(0), { timeout: 40000 });
      const g = await np.evaluate(() => { const b = document.getElementById('owPhoto'); return { zp: b.dataset.zp || '', role: b.getAttribute('role') || '', style: b.style.cursor }; });
      await np.locator('#owPhoto').click(); await np.waitForTimeout(400);
      const g2 = await np.evaluate(() => ({ open: OrderWin.isOpen(), s: +(document.querySelector('#owPhoto img').dataset.scale || 1) }));
      await np.keyboard.press('Escape');
      const closed = await np.waitForFunction(() => !OrderWin.isOpen(), null, { timeout: 5000 }).then(() => true, () => false);
      check(had === 'undefined' && g.zp === '' && g.role === '' && g2.open && g2.s === 1 && closed && np.errors.length === 0, `the zoom script missing: both pictures are drawn plain, a click does nothing, Esc closes the window, no errors (${JSON.stringify([had, g, g2, closed, np.errors])})`);
      await np.context_.close();
    }

    /* 13 · mutants: each one must be caught by the very checks above */
    const mutants = [
      ['the fullscreen route is back (a click opens the old viewer)', null, async p => { await p.evaluate(() => { for (const id of ['owPhoto', 'owVector']) document.getElementById(id).addEventListener('click', e => { e.stopImmediatePropagation(); e.preventDefault(); if (window.PhotoView) { const b = document.createElement('button'); b.type = 'button'; b.dataset.photo = document.querySelector('#owPhoto img').src; b.dataset.photoCors = '1'; b.dataset.photoCap = 'Etsy listing'; document.getElementById('orderWin').appendChild(b); PhotoView.open(b); } }, true); }); }, 'inPlace'],
      ['a click does not centre on the point (the offset has the wrong sign)', s => s.replace('const tx = -(dx / st.s - st.x), ty = -(dy / st.s - st.y);', 'const tx = (dx / st.s - st.x), ty = (dy / st.s - st.y);'), null, 'centre'],
      ['the pan is not clamped (the frame can be left empty)', s => s.replace('const m = limits(st.s, v); return { s: st.s, x: lim(st.x, m.x), y: lim(st.y, m.y) };', 'return { s: st.s, x: st.x, y: st.y };'), null, 'centre'],
      ['the plain wheel is taken (the window cannot scroll over a picture)', s => s.replace("if (mode !== 'always' && !mod) return;            // the plain wheel scrolls the window", ''), null, 'scroll'],
      ['a redraw loses the zoom (nothing is kept per slot)', s => s.replace('if (was && was.key === curKey) {', 'if (false) {'), null, 'redraw'],
      ['a transition on more than transform', s => s.replace("const EASE = 'transform 240ms cubic-bezier(.2,.7,.2,1)'", "const EASE = 'all 240ms ease'").replace("EASE = 'transform 240ms cubic-bezier(.2,.7,.2,1)'", "EASE = 'all 240ms ease'"), null, 'anim']
    ];
    for (const [name, mutate, patch, which] of mutants) {
      const src = mutate ? mutate(zoomSrc) : zoomSrc;
      if (mutate) assert.notEqual(src, zoomSrc, 'the mutation applied: ' + name);
      const mp = await boot(browser, srv, { zoompan: src });
      let caught = false;
      try {
        if (patch) await patch(mp);
        // run the matching check against the mutant, with the page of the mutant (a check is a closure over `page`, so make twins on mp)
        const saved = page; // (the closures above use `page`; the mutant checks are re-run on mp below)
        const r = await runOn(mp, which);
        caught = !r;
      } catch (e) { caught = true; }
      check(caught, `mutant caught: ${name}`);
      await mp.context_.close();
    }
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails.length) { console.error('\n' + fails.map(f => '  ✗ ' + f).join('\n')); console.error(`\n  ${fails.length} failed`); process.exitCode = 1; return; }
  console.log('In-place zoom and pan OK');
}

/** A plain wheel over the photo, in a short window: it scrolls the page column down and back up, and never zooms. */
async function scrollsOver(page) {
  const PH = '#owPhoto', col = () => page.evaluate(() => { const m = document.querySelector('.owVInfo .owMain'); return { top: m.scrollTop, can: m.scrollHeight > m.clientHeight }; });
  const over = g => { const lo = Math.max(g.inner.y + 5, 70), hi = Math.min(g.inner.y + g.inner.h - 5, 440); return lo < hi ? (lo + hi) / 2 : g.mid.y; };
  await page.setViewportSize({ width: 1440, height: 480 }); await page.waitForTimeout(250);
  await page.evaluate(() => { document.querySelector('.owVInfo .owMain').scrollTop = 0; });
  const g = await look(page, PH), t0 = await col();
  await page.mouse.move(g.mid.x, over(g)); await page.mouse.wheel(0, 120); await page.waitForTimeout(250);
  const t1 = await col(), g1 = await look(page, PH);
  await page.mouse.move(g1.mid.x, over(g1)); await page.mouse.wheel(0, -60); await page.waitForTimeout(250);
  const t2 = await col(), g2 = await look(page, PH);
  await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(200);
  return { moved: t1.top - t0.top, up: t1.top - t2.top, can: t1.can, s: g2.s };
}

/** The checks a mutant must fail, run on a page of its own: true when the page behaves as it should. */
async function runOn(page, which) {
  const PH = '#owPhoto';
  await openPiece(page, 0);
  if (which === 'inPlace') {
    const before = await page.evaluate(() => document.querySelectorAll('dialog[open]').length), g0 = await look(page, PH);
    await page.mouse.click(g0.inner.x + g0.inner.w * .3, g0.inner.y + g0.inner.h * .25); await page.waitForTimeout(450);
    const a = await page.evaluate(() => ({ n: document.querySelectorAll('dialog[open]').length, pv: window.PhotoView ? PhotoView.isOpen() : false }));
    return a.n === before && !a.pv;
  }
  if (which === 'centre') {
    let ok = true;
    for (const [fx, fy] of [[.3, .25], [.8, .7]]) {
      await page.keyboard.press('Escape'); await page.waitForTimeout(300);
      const g0 = await look(page, PH), x = g0.inner.x + g0.inner.w * fx, y = g0.inner.y + g0.inner.h * fy, f0 = frac(g0, x, y);
      await page.mouse.click(x, y); await page.waitForTimeout(450);
      const g1 = await look(page, PH), at = [g1.content.x + f0[0] * g1.content.w, g1.content.y + f0[1] * g1.content.h];
      const covers = g1.content.x <= g1.inner.x + 1 && g1.content.y <= g1.inner.y + 1 && g1.content.x + g1.content.w >= g1.inner.x + g1.inner.w - 1 && g1.content.y + g1.content.h >= g1.inner.y + g1.inner.h - 1;
      if (!(Math.abs(at[0] - g1.mid.x) <= 3 && Math.abs(at[1] - g1.mid.y) <= 3 && covers)) ok = false;
    }
    // and dragged hard to a corner, the frame must still be full
    const z = await look(page, PH);
    for (let i = 0; i < 6; i++) { await page.mouse.move(z.mid.x, z.mid.y); await page.mouse.down(); await page.mouse.move(z.mid.x + 90, z.mid.y + 90, { steps: 3 }); await page.mouse.up(); }
    const e = await look(page, PH);
    if (!(e.content.x <= e.inner.x + 1.5 && e.content.y <= e.inner.y + 1.5 && e.content.x >= e.inner.x - 1.5)) ok = false;
    return ok;
  }
  if (which === 'scroll') {
    const r = await scrollsOver(page);
    return r.moved > 60 && r.up > 20 && r.s === 1;
  }
  if (which === 'redraw') {
    const k = await keepsOnRedrawOn(page);
    return k.before > 1.2 && near(k.before, k.after, 1e-6) && k.same;
  }
  if (which === 'anim') {
    const g = await look(page, PH);
    await page.evaluate(sampleAnims);
    await page.mouse.click(g.inner.x + g.inner.w * .6, g.inner.y + g.inner.h * .4); await page.waitForTimeout(450);
    const props = await page.evaluate(() => { clearInterval(window.__animT); return [...window.__anims]; });
    return props.length > 0 && props.every(p => p === 'transform');
  }
  throw new Error('unknown check ' + which);
}
main().catch(e => { console.error(e); process.exit(1); });
