// Change SKU window (Paul, 23 Nov 2026): "1. The charm thumbnail are much too small need to make them twice as big cause they're barely visible. 2. ... two thumbnails did not load successfully. 3. allow users to
// click to zoom on the listing image." The page in headless Chromium over the repo's fake site (bridge-server.cjs), at the owner's own window (1352 x 593, dpr 2), then a taller one and a phone's:
//   1  every result's picture is at least 100 px (it was 42), the name under it; only the list scrolls; the toggle, the chips and the two side pictures are in sight without scrolling
//   2  a stored picture whose link is dead (403) is drawn from the design's own file; one that cannot be had says "no picture", shows no broken image and its card is still chosen by a tap on it
//   3  a click on the listing photo zooms it in its own box, a drag pans it, Esc puts it back and the window stays open; the next Esc closes the window; opened again it is whole
//   SHOT=<path prefix> keeps pictures of the window.   node tests/charm-nest/change-sku-window.cjs   (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { (process.env.SKIP_BROWSER === '1' ? console.log : m => { console.error('FAIL: ' + m + ' (no browser here: set PW_DIR and CHROMIUM, or SKIP_BROWSER=1 to skip it on purpose)'); process.exit(1); })('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { (process.env.SKIP_BROWSER === '1' ? console.log : m => { console.error('FAIL: ' + m + ' (no browser here: set PW_DIR and CHROMIUM, or SKIP_BROWSER=1 to skip it on purpose)'); process.exit(1); })('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const RUN = 'run-skuwin-1', LID = '1819191541', OLD = 'HOWLING_WOLF_AND_MOON_38473', DEAD = 'WOLF_HOWLING_29638', GONE = 'WOLF_HOWLING_GONE_1', A = '4193172538', TID = '5100000001';
const WOLVES = Array.from({ length: 44 }, (_, i) => ['WOLF_' + (3000 + i), 'WOLF_PACK_CELESTIAL_LARGE_' + (40000 + i), 'WOLF_HEAD_' + (500 + i)][i % 3]);
const SKUS = [...new Set([OLD, DEAD, GONE, ...WOLVES])];
const SHIP = Math.floor(Date.UTC(2026, 9, 24, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 9, 16, 12) / 1000);
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const srv = await start({ receipts: [] }), st = srv.st;
  for (const sku of SKUS) st.put('Charm_Master_Index', sku, { sku, aiPath: 'test/' + sku + '.ai', widthPt: 28.35, heightPt: 28.35, holes: 0 }, false);
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  const errors = [], asked = new Map();
  try {
    const ctx = await browser.newContext({ viewport: { width: 1352, height: 593 }, deviceScaleFactor: 2 });
    await ctx.route(u => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await ctx.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', 'Paul'); } catch (_) {} window.confirm = () => true; window.alert = () => {}; });
    const page = await ctx.newPage(); page.setDefaultTimeout(20000);
    page.on('pageerror', e => errors.push(String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && window.Review && window.Pool && window.OrderWin && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    const pics = await page.evaluate(async ({ SKUS, RUN, LID, OLD, DEAD, GONE, A, TID, BASE_TS, SHIP }) => {
      await Orders.loadMaps(true);
      const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, pid = `${A}_${TID}_1`;
      const sq = (x0, y0, x1, y1) => ({ kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], lwPt: .5, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]] });
      const out = {};
      for (const [i, sku] of SKUS.entries()) {
        const key = 'test/' + sku + '.ai', k = .15 + (i % 7) * .05, outline = sq(0, 0, D, D), c = { id: 'pool:' + sku + ':0', name: sku, sku, sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline, members: [outline, sq(D * k, D * k, D * (1 - k), D * (1 - k))], rMm: R, w: 16, h: 16, scale: 2, bits: new Uint8Array(256).fill(1), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2, hash: 'h-' + sku };
        c.thumb = await CharmNestPDF.thumbnail(c, 168); out[sku] = c.thumb;
        B.master.entries.set(sku, { sku, updatedAt: 1, indexedAt: 1, aiPath: sku === GONE ? '' : key, widthPt: 2 * R * MM, heightPt: 2 * R * MM, holes: 0, upAngle: null, labelSource: 'text', charmHash: 'h-' + sku, thumbPath: 'charmnest/master/' + sku + '.png', thumbUrl: 'https://firebasestorage.googleapis.com/v0/b/test-bucket/o/charmnest%2Fmaster%2F' + sku + '.png?alt=media&token=t' });
        if (sku !== GONE) B.pool.sources.set(key, { id: 'pool:' + sku, pool: true, name: sku, sku, charms: [c], usedAt: Date.now() });
      }
      return out;
    }, { SKUS, RUN, LID, OLD, DEAD, GONE, A, TID, BASE_TS, SHIP });
    // the asset function: the stored picture, and 403 "Artwork link expired" for the two whose link died
    await ctx.route(u => /charmNestAsset/.test(u.pathname), r => {
      const inner = new URL(r.request().url()).searchParams.get('url') || '', sku = (/master(?:%2F|\/)([A-Z0-9_]+)\.png/i.exec(inner) || [])[1]; if (!sku) return r.fulfill({ status: 404, body: '' });
      asked.set(sku, (asked.get(sku) || 0) + 1);
      if (sku === DEAD || sku === GONE) return r.fulfill({ status: 403, contentType: 'application/json', body: '{"error":"Artwork link expired"}' });
      return r.fulfill({ status: 200, contentType: 'image/png', headers: { 'Cross-Origin-Resource-Policy': 'same-origin' }, body: Buffer.from(pics[sku].split(',')[1], 'base64') });
    });
    await page.evaluate(({ RUN, LID, OLD, A, TID, BASE_TS, SHIP }) => {
      const line = { transactionId: TID, listingId: LID, sku: OLD, title: 'Howling Wolf Charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' };
      const order = { receiptId: A, orderNumber: A, createTs: BASE_TS, updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Buyer A' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
      const row = { key: CharmNestOrders.lineKey(order, line), order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: 'gold' };
      B.orders.rows.length = 0; B.orders.byKey.clear(); B.orders.rows.push(row); B.orders.byKey.set(row.key, row); B.sets.clear();
      B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [A] };
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render(); if (!B.employee) B.employee = 'Paul';
      OrderWin.open(row.key);
    }, { RUN, LID, OLD, A, TID, BASE_TS, SHIP });
    const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + (typeof what === 'function' ? await what() : what)); await sleep(100); } };
    await until(() => page.evaluate(() => { const b = document.getElementById('owEditSku'); return b && !b.disabled && b.dataset.offer === '1'; }), 15000, () => page.evaluate(() => 'Change SKU is offered: ' + (document.getElementById('owEditSku') || {}).title));
    const openIt = async () => {   // (the order window's own photo, a wide one: a room with a red square in its middle)
      await page.evaluate(() => { const c = document.createElement('canvas'); c.width = 640; c.height = 480; const x = c.getContext('2d'); x.fillStyle = '#d8c7a4'; x.fillRect(0, 0, 640, 480); x.fillStyle = '#7a6a4c'; x.fillRect(0, 340, 640, 140); x.fillStyle = '#b5432f'; x.fillRect(280, 180, 80, 80); document.getElementById('owPhoto').innerHTML = `<img alt="" src="${c.toDataURL('image/jpeg')}">`; document.getElementById('owEditSku').click(); });
      await until(() => page.evaluate(() => document.getElementById('owSkuDlg').open), 5000, 'the Change SKU window opens');
    };
    const type = async t => { await page.fill('#owSkuInput', t); await sleep(250); };
    const drawn = () => until(() => page.evaluate(() => { const l = document.getElementById('owSkuList').getBoundingClientRect(), seen = [...document.querySelectorAll('#owSkuList .seHit .skPic')].filter(p => { const r = p.getBoundingClientRect(); return r.top < l.bottom && r.bottom > l.top; }); return seen.length > 3 && seen.every(p => { const i = p.querySelector('img'); return i && (i.naturalWidth > 0 || i.dataset.assetFailed); }); }), 15000, 'the pictures in sight are drawn');
    const look = () => page.evaluate(() => {
      const q = s => document.querySelector(s), box = e => { const r = e.getBoundingClientRect(); return { x: r.left, y: r.top, w: r.width, h: r.height, b: r.bottom, r: r.right }; }, d = box(q('#owSkuDlg')), foot = box(q('#owSkuDlg .dlgFoot')), left = q('.skuDlg .seLeft'), list = q('#owSkuList'), body = q('.skuDlg .dlgBody');
      const hits = [...list.querySelectorAll('.seHit')].map(h => ({ sku: h.dataset.sku, c: box(h), p: box(h.querySelector('.skPic')), i: h.querySelector('.skPic img') ? box(h.querySelector('.skPic img')) : null, nm: box(h.querySelector('.nm')) }));
      return { d, foot, hits, cols: new Set(hits.map(h => Math.round(h.c.x))).size, list: Object.assign(box(list), { over: list.scrollHeight - list.clientHeight, overX: list.scrollWidth - list.clientWidth }), left: Object.assign(box(left), { over: left.scrollHeight - left.clientHeight, overX: left.scrollWidth - left.clientWidth }), bodyOver: body.scrollHeight - body.clientHeight, once: box(q('#owSkuOnceRow')), from: box(q('#owSkuFromPic')), to: box(q('#owSkuToPic')) };
    });
    const tidy = (v, what, min) => {
      assert(v.hits.length > 20, what + ': results: ' + v.hits.length);
      for (const h of v.hits) { assert(h.p.w >= min && Math.abs(h.p.w - h.p.h) < 1.5, `${what}: ${h.sku} picture ${h.p.w.toFixed(1)} x ${h.p.h.toFixed(1)} (want ${min} and square)`); assert(h.nm.y >= h.p.b - .5 && h.nm.b <= h.c.b + .5 && h.nm.r <= h.c.r + .5 && h.nm.x >= h.c.x - .5, `${what}: ${h.sku}: its name is under its picture, inside its card`); if (h.i) assert(h.i.w / h.p.w > .9 && h.i.w / h.p.w < .95, `${what}: the picture is inset about 4%: ${(h.i.w / h.p.w).toFixed(3)}`); }
      for (let i = 0; i < v.hits.length; i++) for (let j = i + 1; j < v.hits.length; j++) { const a = v.hits[i].c, b = v.hits[j].c; assert(a.r <= b.x + .5 || b.r <= a.x + .5 || a.b <= b.y + .5 || b.b <= a.y + .5, `${what}: ${v.hits[i].sku} and ${v.hits[j].sku} overlap`); }
      assert(v.list.over > 0 && v.list.overX <= 0, what + ': the list is what scrolls, downwards: ' + JSON.stringify(v.list));
    };
    /* 1 · the owner's window */
    await openIt(); await type('wolf'); await drawn(); await sleep(300);
    let v = await look(); tidy(v, '1352 x 593', 100);
    const leftFits = (v, w) => assert(v.left.over <= 0 && v.bodyOver <= 0 && v.once.b <= v.foot.y + .5 && v.once.b <= v.left.b + .5 && v.once.h > 20, w + ': the left column and the toggle fit without scrolling: ' + JSON.stringify({ left: v.left, body: v.bodyOver, once: v.once, foot: v.foot }));
    leftFits(v, 'off'); assert(v.from.w >= 125 && Math.abs(v.from.w - v.to.w) < 1, 'the side pictures use their width: ' + v.from.w);
    const px = v.hits[0].p.w;
    if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + '.png' });
    await page.click('#owSkuOnce'); await sleep(250); leftFits(await look(), 'toggle on'); if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + '-on.png' }); await page.click('#owSkuOnce');
    pass(`1352 x 593: ${v.cols} across, each picture ${px.toFixed(0)} px (it was 42), the name under it, nothing overlaps; only the list scrolls; the toggle is in sight, off and on; the side pictures are ${v.from.w.toFixed(0)} px`);
    /* 2 · the two lost pictures */
    await type('wolf howling');
    const at = sku => page.evaluate(sku => { const p = document.querySelector(`#owSkuList [data-sku="${sku}"] .skPic`), i = p && p.querySelector('img'); if (!i) return null; const c = getComputedStyle(i); return { src: i.getAttribute('src') || '', w: i.naturalWidth, failed: !!i.dataset.assetFailed, opacity: c.opacity, pe: c.pointerEvents, word: getComputedStyle(p, '::after').content }; }, sku);
    await until(async () => { const a = await at(DEAD), g = await at(GONE); return a && a.w > 0 && g && g.failed; }, 15000, async () => 'the dead link is drawn and the other one is given up: ' + JSON.stringify([await at(DEAD), await at(GONE)]));
    const dead = await at(DEAD), gone = await at(GONE);
    assert.equal(dead.src, pics[DEAD], 'the dead link\'s picture is the one drawn from its design'); assert(asked.get(DEAD) === 1, 'the dead link was asked for once: ' + asked.get(DEAD));
    assert(gone.opacity === '0' && gone.pe === 'none' && /no picture/.test(gone.word) && dead.word === 'none', 'the picture that cannot be had is hidden, lets the tap through and says so: ' + JSON.stringify([gone, dead.word]));
    if (process.env.SHOT) await page.locator('#owSkuDlg').screenshot({ path: process.env.SHOT + '-lost.png' });
    await page.click(`#owSkuList [data-sku="${GONE}"] .skPic`, { position: { x: 30, y: 30 } }); await sleep(150);
    assert.equal(await page.evaluate(() => document.getElementById('owSkuInput').value), GONE, 'a tap on the lost picture chooses its charm');
    pass('a stored picture whose link is dead (403) is drawn from its design; one that cannot be had says "no picture" and its card is chosen by a tap on it');
    /* 3 · the listing photo */
    const z = () => page.evaluate(() => { const b = document.getElementById('owSkuFromPic'), i = b.querySelector('img.photo'), r = b.getBoundingClientRect(); return { s: +(i.dataset.scale || 1), x: +(i.dataset.offsetX || 0), zp: b.dataset.zp, fit: getComputedStyle(i).objectFit, cut: getComputedStyle(b).overflow, box: [r.left, r.top, r.width, r.height].map(Math.round).join(), open: document.getElementById('owSkuDlg').open, order: document.getElementById('orderWin').open }; });
    const z0 = await z(); assert(z0.s === 1 && z0.zp === 'rest' && z0.fit === 'cover', 'whole at rest: ' + JSON.stringify(z0));
    const fb = v.from; await page.mouse.click(fb.x + fb.w / 2 + 20, fb.y + fb.h / 2 + 10); await sleep(450);
    const z1 = await z(); assert(z1.s > 1.3 && z1.zp === 'zoomed' && z1.box === z0.box && z1.cut === 'hidden', 'a click zooms in its own box: ' + JSON.stringify([z0, z1]));
    if (process.env.SHOT) await page.locator('#owSkuDlg').screenshot({ path: process.env.SHOT + '-zoom.png' });
    await page.mouse.move(fb.x + fb.w / 2, fb.y + fb.h / 2); await page.mouse.down(); await page.mouse.move(fb.x + fb.w / 2 + 40, fb.y + fb.h / 2, { steps: 5 }); await page.mouse.up(); await sleep(80);
    const z2 = await z(); assert(z2.s === z1.s && z2.x > z1.x, 'a drag pans it: ' + JSON.stringify([z1, z2]));
    await page.focus('#owSkuInput'); await page.keyboard.press('Escape'); await sleep(400);   // (Esc from the search field: the dialog's own cancel)
    const z3 = await z(); assert(z3.s === 1 && z3.open && z3.order, 'Esc puts it back whole, the window stays: ' + JSON.stringify(z3));
    await page.mouse.click(fb.x + fb.w / 2, fb.y + fb.h / 2); await sleep(350); assert((await z()).s > 1.3); await page.keyboard.press('Escape'); await sleep(400);   // (Esc from the photo itself)
    const z4 = await z(); assert(z4.s === 1 && z4.open, 'Esc from the photo too: ' + JSON.stringify(z4));
    await page.mouse.click(fb.x + fb.w / 2, fb.y + fb.h / 2); await sleep(350); await page.click('#owSkuCancel'); await sleep(200); assert(!(await z()).open, 'Cancel closes it, zoomed or not');
    await openIt(); const z5 = await z(); assert(z5.s === 1 && z5.zp === 'rest', 'opened again it is whole: ' + JSON.stringify(z5));
    await page.focus('#owSkuInput'); await page.keyboard.press('Escape'); await sleep(300); const z6 = await z(); assert(!z6.open && z6.order, 'Esc with nothing zoomed closes the Change SKU window only: ' + JSON.stringify(z6));
    await page.evaluate(() => { document.getElementById('owPhoto').innerHTML = ''; document.getElementById('owEditSku').click(); }); await sleep(300);
    assert(await page.evaluate(() => { const b = document.getElementById('owSkuFromPic'); return document.getElementById('owSkuDlg').open && b.dataset.zp === 'none' && !b.hasAttribute('role') && !b.hasAttribute('tabindex') && !!b.querySelector('svg.q'); }), 'without a photo the box is no button');
    await page.click('#owSkuCancel'); await sleep(150);
    pass(`the listing photo zooms in its own box on a click (x${z1.s.toFixed(2)}), is dragged to pan, Esc puts it back and the window stays; Cancel and a second Esc close it as before; whole when opened again; no photo, no zoom`);
    /* a taller window, and a phone */
    await page.setViewportSize({ width: 1500, height: 950 }); await openIt(); await type('wolf'); await drawn(); v = await look(); tidy(v, '1500 x 950', 136); leftFits(v, '1500 x 950'); const tall = v.hits[0].p.w; if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + '-tall.png' });
    await page.setViewportSize({ width: 375, height: 812 }); await sleep(300); const ph = await look();
    assert(ph.cols === 2 && ph.hits[0].p.w >= 100 && ph.list.h >= 200 && ph.list.overX <= 0 && ph.list.r <= 375 && Math.abs(ph.from.w - 96) < 1.5, 'a phone: two across, the list tall enough to use: ' + JSON.stringify({ cols: ph.cols, p: ph.hits[0].p.w, list: ph.list, from: ph.from.w }));
    if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + '-phone.png' });
    pass(`1500 x 950: ${tall.toFixed(0)} px pictures; a phone: two across, ${ph.hits[0].p.w.toFixed(0)} px, the list ${ph.list.h.toFixed(0)} px tall`);
    assert.deepEqual(errors, [], 'no page error');
    console.log(`change-sku-window: ${n} steps passed`);
  } finally { await browser.close().catch(() => {}); await (srv.close ? srv.close() : 0); }
  process.exit(0);
})().catch(e => { console.error('FAIL', e && e.message || e); process.exit(1); });
