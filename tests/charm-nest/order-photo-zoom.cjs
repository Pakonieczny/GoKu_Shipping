// Click to zoom and pan on the order window's two pictures (Paul, 5 Oct 2026, round 3: "add a full-fledged click to zoom
// and pan functionality for both the charm listing image and the vector thumbnail"). Paul's rule is that it looks exactly
// the same, so the one viewer the inbox photos use (PhotoView, charm-nest-mail.js) is what opens:
//   · a click, Enter or Space on the Etsy listing photo or on the Vector design opens it, as a layer of the order window;
//   · the Etsy photo opens at the largest size Etsy keeps (and falls back to the next size, then to the thumbnail's own,
//     when a size cannot be had); the vector is drawn again large (never the 220 px thumbnail enlarged), for a pooled
//     charm and for a master's drawing alike;
//   · the wheel and a pinch zoom (with limits), a drag pans (never past the picture's edges), a double-click or 0 resets;
//   · ← → and the side buttons step Etsy listing ↔ Vector design of the same piece, and the caption says which;
//   · Esc closes the viewer only: the window under it, its tab and its piece are as they were, and the focus is back on
//     the thumbnail that was used;
//   · the thumbnails themselves never zoom (no in-place zoom, no tooltip, no reset button), and each is a focusable button;
//   · the inbox's photo viewer (data-photo in a conversation) still behaves exactly as before, on the page and in a layer.
//   node tests/charm-nest/order-photo-zoom.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOTS=dir keeps screenshots)
const path = require('path'), zlib = require('zlib'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

// ── pictures for the fake image proxy: real PNGs of known sizes, so which size the viewer loaded can be read ──
const crcT = (() => { const t = new Int32Array(256); for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; t[n] = c; } return t; })();
const crc = b => { let c = -1; for (const x of b) c = crcT[(c ^ x) & 255] ^ (c >>> 8); return (c ^ -1) >>> 0; };
const chunk = (type, data) => { const len = Buffer.alloc(4); len.writeUInt32BE(data.length); const td = Buffer.concat([Buffer.from(type), data]), c = Buffer.alloc(4); c.writeUInt32BE(crc(td)); return Buffer.concat([len, td, c]); };
const pngs = new Map();
function png(w, h) {
  const k = w + 'x' + h; if (pngs.has(k)) return pngs.get(k);
  const row = w * 3 + 1, raw = Buffer.alloc(row * h);
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { const o = y * row + 1 + x * 3; raw[o] = x * 255 / w; raw[o + 1] = y * 255 / h; raw[o + 2] = ((x >> 5) + (y >> 5)) & 1 ? 215 : 45; }
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 2;
  const out = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw, { level: 1 })), chunk('IEND', Buffer.alloc(0))]);
  pngs.set(k, out); return out;
}
const SIZES = { fullxfull: [2000, 1500], '1588xN': [1000, 750], '570xN': [570, 428] };   // (the middle one is smaller than Etsy's real 1588: only the order matters)

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 8, 17) / 1000);
const RID = '4170837249', TID = n => `${RID}${n}`, SKUS = ['CARDINAL_A', 'CARDINAL_B', 'CARDINAL_C'], LID = ['1800100', '1800200', '1800300'];
const etsy = (id, size = '570xN') => `https://i.etsystatic.com/1/r/il/abcd/${id}/il_${size}.${id}_wxyz.jpg`;
const proxied = u => '/.netlify/functions/imageProxy?url=' + encodeURIComponent(u);

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.CN_SHOTS || null;
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, hasTouch: true });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    // the image proxy: a photo's size is in its Etsy address (il_<size>.), the second piece's two largest sizes cannot be had,
    // and so the third piece's; the inbox's own test photos (…url=a, b) are small
    const asked = [];
    await context.route(/\/\.netlify\/functions\/imageProxy/, r => {
      const u = new URL(r.request().url()).searchParams.get('url') || '', m = /\/il_([^.]+)\./.exec(u); asked.push(u);
      const headers = { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' };
      const tid = /\/(\d{7})\/il_/.exec(u);
      const lid = tid && tid[1];
      if (m && lid === '1800200' && m[1] === 'fullxfull') return r.fulfill({ status: 502, headers, body: 'too large' });
      if (m && lid === '1800300' && m[1] !== '570xN') return r.fulfill({ status: 502, headers, body: 'too large' });
      const [w, h] = m && SIZES[m[1]] || [300, 200];
      return r.fulfill({ status: 200, headers: Object.assign({ 'Content-Type': 'image/png' }, headers), body: png(w, h) });
    });
    // the three pieces' photos are already known (the local index), so no lookup is made
    const index = LID.map(l => [l, proxied(etsy(l))]);
    await context.addInitScript(idx => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator');
      localStorage.setItem('cn.listingPhotos.v1', JSON.stringify(idx)); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; }, index);
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.PhotoView && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // the order: three pieces, each with its listing photo; the first two have a pooled charm (a 34 × 34 square), the third has
    // none and is drawn from its master's design (its copy in the pool's sources, so nothing is fetched)
    await page.evaluate(({ RID, TIDS, SKUS, LID, SHIP }) => {
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      const charm = { id: 'c', name: 'TAG', metal: 'gold', centerPt: [17, 17], widthPt: 34, heightPt: 34, areaPt2: 34 * 34, outline: sq, members: [sq], bbox: [0, 0, 34, 34], thumb: '' };
      const was = Pool.charmOf; Pool.charmOf = id => /^zz-/.test(id) ? Object.assign({ poolId: id }, charm) : was(id);
      const wasEntry = Master.entryFor; Master.entryFor = sku => sku === 'CARDINAL_C' ? { sku, aiPath: 'zz/CARDINAL_C.ai', aiUrl: 'x' } : wasEntry(sku);
      B.pool.sources.set('zz/CARDINAL_C.ai', { charms: [charm] });
      const order = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Hannah Whitford' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
        lines: SKUS.map((sku, i) => ({ transactionId: TIDS[i], listingId: LID[i], sku, title: sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' })) };
      for (const [i, line] of order.lines.entries()) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: i < 2 ? ['zz-' + i] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { RID, TIDS: SKUS.map((_, i) => TID(i + 1)), SKUS, LID, SHIP });
    const keys = SKUS.map((_, i) => `${RID}_${TID(i + 1)}`);
    await page.waitForSelector(`#ordItems [data-key="${keys[0]}"]`);
    const settled = () => page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 8000 });
    const viewer = () => page.evaluate(() => {
      const d = document.querySelector('dialog.phv'); if (!d || !d.open) return null;
      const i = d.querySelector('.phvImg'), st = d.querySelector('.phvStage'), m = /translate\(([-\d.e]+)px,\s*([-\d.e]+)px\) scale\(([-\d.e]+)\)/.exec(i.style.transform || ''), n = d.querySelector('.phvNote');
      return { cap: d.querySelector('.phvCap').textContent, n: d.querySelector('.phvN').textContent, pct: d.querySelector('.phvPct').textContent, nat: [i.naturalWidth, i.naturalHeight], src: (i.currentSrc || i.src || '').slice(0, 60),
        tx: m && +m[1], ty: m && +m[2], s: m && +m[3], sw: st.clientWidth, sh: st.clientHeight, layer: d.classList.contains('layer'), inWin: !!d.closest('#orderWin'), note: n.hidden ? '' : n.textContent,
        shown: i.style.visibility !== 'hidden', orig: !d.querySelector('.phvOrig').hidden, nextTitle: d.querySelector('.phvNav.next').title, nextLabel: d.querySelector('.phvNav.next').getAttribute('aria-label'), label: d.getAttribute('aria-label'),
        navHidden: d.querySelector('.phvNav.next').hidden };
    });
    const loaded = (w) => page.waitForFunction(w => { const i = document.querySelector('dialog.phv[open] .phvImg'); return i && i.complete && i.naturalWidth === w && i.style.visibility !== 'hidden'; }, w, { timeout: 20000 });
    const closed = () => page.waitForFunction(() => !document.querySelector('dialog.phv[open]'), null, { timeout: 4000 });
    const winState = () => page.evaluate(() => ({ open: document.getElementById('orderWin').open, title: document.getElementById('owTitle').textContent, view: document.querySelector('.owTabsV [aria-selected=true]').dataset.owView, key: OrderWin.key(),
      scroll: document.querySelector('.owVInfo .owMain').scrollTop, dialogs: [...document.querySelectorAll('dialog[open]')].filter(d => !d.classList.contains('phv')).length, active: document.activeElement && document.activeElement.id }));
    const centre = { x: 720, y: 450 };

    // 1 · open the order (piece 1): both thumbnails drawn, each a focusable button, neither zooms where it lies
    await page.click(`#ordItems [data-key="${keys[0]}"]`);
    await settled();
    await page.waitForFunction(() => document.querySelector('#owPhoto img')?.complete && document.querySelector('#owVector img, #owVector canvas'), null, { timeout: 20000 });
    const t1 = await page.evaluate(() => {
      const b = id => document.getElementById(id), ph = b('owPhoto'), vh = b('owVector');
      return { photoBtn: [b('owPhotoBox').getAttribute('role'), b('owPhotoBox').tabIndex, b('owPhotoBox').getAttribute('aria-label')], vecBtn: [b('owVectorBox').getAttribute('role'), b('owVectorBox').tabIndex, b('owVectorBox').getAttribute('aria-label')],
        titles: [ph.title, vh.title, ph.closest('figure').querySelector('[title]')?.title || '', vh.closest('figure').querySelector('.thumbReset') ? 'reset' : ''], bound: [ph.classList.contains('zoomReady'), vh.classList.contains('zoomReady')],
        cursor: [getComputedStyle(ph).cursor, getComputedStyle(vh).cursor], vecNat: (vh.querySelector('img') || vh.querySelector('canvas')).tagName, vecSize: (() => { const e = vh.querySelector('img') || vh.querySelector('canvas'); return [e.naturalWidth || e.width, e.naturalHeight || e.height]; })() };
    });
    assert.deepEqual(t1.photoBtn.slice(0, 2), ['button', 0]); assert.deepEqual(t1.vecBtn.slice(0, 2), ['button', 0]);
    assert.match(t1.photoBtn[2], /Etsy listing photo/); assert.match(t1.vecBtn[2], /Vector design/);
    assert.deepEqual(t1.titles, ['', '', '', ''], 'no tooltip on either picture, no reset button: ' + JSON.stringify(t1.titles));
    assert.deepEqual(t1.bound, [false, false], 'neither thumbnail is bound to the in-place zoom of the lists');
    assert.deepEqual(t1.cursor, ['zoom-in', 'zoom-in']);
    assert(t1.vecSize[0] <= 260, 'the vector thumbnail is the small one: ' + t1.vecSize);
    // the wheel over a thumbnail scrolls the page as it did: it does not zoom it
    await page.hover('#owPhoto'); await page.mouse.wheel(0, -300);
    await page.waitForTimeout(150);
    assert.equal(await page.evaluate(() => ['owPhoto', 'owVector'].map(id => { const e = document.querySelector('#' + id + ' img, #' + id + ' canvas'); return e.style.transform || 'none'; }).join()), 'none,none', 'a wheel does not zoom a thumbnail in place');
    if (shots) await page.screenshot({ path: path.join(shots, 'order-photo-zoom-thumbs.png') });

    // 2 · a click on the Etsy photo opens the viewer on it, as a layer of the window, at the largest size
    const before = await winState();
    await page.click('#owPhoto');
    await loaded(2000);
    let v = await viewer();
    assert(v.layer && v.inWin, 'the viewer is a layer of the order window');
    assert.match(v.cap, /^Etsy listing · Piece 1 of 3 · CARDINAL_A$/); assert.equal(v.n, '1 of 2'); assert(!v.navHidden, 'side buttons');
    assert.deepEqual(v.nat, [2000, 1500], 'the largest size was loaded: ' + v.src); assert(v.orig, 'the original is one click away');
    assert.equal(v.nextTitle, 'Next picture (→)'); assert.equal(v.label, 'Picture');
    assert(asked.some(u => /il_fullxfull\./.test(u)), 'the largest size was asked for'); assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 2, 'the window and its viewer, nothing else');
    const fit = v.s; assert(fit > 0 && fit < 1 && v.pct === Math.round(fit * 100) + '%', 'fitted to the screen: ' + v.pct);
    if (shots) await page.screenshot({ path: path.join(shots, 'order-photo-zoom-photo.png') });

    // 3 · wheel zoom with limits, drag pan limited to the picture's edges, double-click resets
    await page.mouse.move(centre.x, centre.y);
    await page.mouse.wheel(0, -400); await page.waitForTimeout(80);
    let v2 = await viewer(); assert(v2.s > fit * 1.5, `the wheel zooms in: ${fit} → ${v2.s}`);
    for (let i = 0; i < 12; i++) { await page.mouse.wheel(0, -600); }
    await page.waitForTimeout(80);
    v2 = await viewer(); const max = Math.max(fit * 6, 4); assert(Math.abs(v2.s - max) < 1e-6, `zoom stops at its limit (${v2.s} vs ${max})`);
    await page.mouse.wheel(0, 100000); await page.waitForTimeout(80);
    v2 = await viewer(); assert(v2.s < max && v2.s >= fit - 1e-9, 'zooming out steps down, never below the fit');
    for (let i = 0; i < 6; i++) await page.mouse.wheel(0, 600);
    await page.waitForTimeout(80);
    v2 = await viewer(); assert(Math.abs(v2.s - fit) < 1e-6, 'zoomed all the way out it is the fit again: ' + v2.s);
    for (let i = 0; i < 3; i++) await page.mouse.wheel(0, -500);
    await page.waitForTimeout(80);
    const z0 = await viewer(); assert(z0.s > fit * 2, 'zoomed in again to pan');
    const drag = async (dx, dy) => { await page.mouse.move(centre.x, centre.y); await page.mouse.down(); const N = 8; for (let i = 1; i <= N; i++) await page.mouse.move(centre.x + dx * i / N, centre.y + dy * i / N); await page.mouse.up(); await page.waitForTimeout(60); };
    await drag(-150, -90);
    const p1 = await viewer(); assert(Math.abs((p1.tx - z0.tx) - (-150)) < 2 && Math.abs((p1.ty - z0.ty) - (-90)) < 2, `a drag pans the picture with the pointer: dx ${p1.tx - z0.tx}, dy ${p1.ty - z0.ty}`);
    for (let i = 0; i < 25; i++) await drag(400, 400);   // as far as it goes towards the top left of the picture: its edge stops at the screen's
    const pe = await viewer(); assert.equal(pe.tx, 0, 'the left edge stops at the screen: ' + pe.tx); assert.equal(pe.ty, 0, 'the top edge stops at the screen: ' + pe.ty);
    for (let i = 0; i < 25; i++) await drag(-400, -400);
    const pf = await viewer(); const W = 2000 * pf.s, H = 1500 * pf.s;
    assert(Math.abs(pf.tx - (pf.sw - W)) < 1e-6, `the right edge stops at the screen: ${pf.tx} vs ${pf.sw - W}`); assert(Math.abs(pf.ty - (pf.sh - H)) < 1e-6, 'the bottom edge stops at the screen');
    assert(await page.evaluate(() => !!document.querySelector('dialog.phv .phvStage.zoomed')), 'a zoomed picture shows the grab cursor');
    await page.mouse.dblclick(centre.x, centre.y);
    await page.waitForTimeout(60);
    const reset = await viewer(); assert(Math.abs(reset.s - fit) < 1e-6, 'a double-click resets to the fit: ' + reset.s);
    await page.mouse.dblclick(centre.x, centre.y); await page.waitForTimeout(60);
    const dz = await viewer(); assert(dz.s > fit * 2, 'and zooms in on the spot when it is fitted: ' + dz.s);
    await page.keyboard.press('0'); await page.waitForTimeout(60);
    assert(Math.abs((await viewer()).s - fit) < 1e-6, '0 resets');
    await page.keyboard.press('+'); await page.waitForTimeout(60); const kz = await viewer(); assert(kz.s > fit * 1.5, '+ zooms');
    await page.keyboard.press('-'); await page.waitForTimeout(60); assert(Math.abs((await viewer()).s - fit) < 1e-6, '− zooms back out');
    await page.click('dialog.phv [data-phv=in]'); await page.waitForTimeout(60); assert((await viewer()).s > fit * 1.5, 'the + button zooms'); await page.click('dialog.phv .phvPct'); await page.waitForTimeout(60);
    assert(Math.abs((await viewer()).s - fit) < 1e-6, 'the percentage button fits it');

    // 3b · a pinch (two touches spread apart) zooms about the fingers' midpoint
    await page.evaluate(({ x, y }) => {
      const st = document.querySelector('dialog.phv .phvStage'), ev = (type, id, dx) => st.dispatchEvent(new PointerEvent(type, { bubbles: true, pointerId: id, pointerType: 'touch', isPrimary: id === 21, clientX: x + dx, clientY: y, button: 0, buttons: 1 }));
      ev('pointerdown', 21, -40); ev('pointerdown', 22, 40); for (let i = 1; i <= 8; i++) { ev('pointermove', 21, -40 - i * 25); ev('pointermove', 22, 40 + i * 25); }
      ev('pointerup', 21, -240); ev('pointerup', 22, 240);
    }, centre);
    await page.waitForTimeout(80);
    const pz = await viewer(); assert(pz.s > fit * 2.5, `a pinch zooms in: ${fit} → ${pz.s}`);
    await page.keyboard.press('0');

    // 4 · ← → and the side buttons step Etsy listing ↔ Vector design; the vector is drawn large
    await page.keyboard.press('ArrowRight');
    await loaded(1600);
    v = await viewer();
    assert.match(v.cap, /^Vector design · Piece 1 of 3 · CARDINAL_A$/); assert.equal(v.n, '2 of 2'); assert(v.src.startsWith('data:image/png'), 'the vector is a picture drawn for the viewer: ' + v.src);
    assert.deepEqual(v.nat, [1600, 1600], 'drawn at 1600 px, not the thumbnail enlarged'); assert.equal(v.orig, false, 'no "Original" link for a drawing');
    assert(v.s < 1 && v.s > 0, 'fitted'); assert.equal(v.note, '');
    if (shots) await page.screenshot({ path: path.join(shots, 'order-photo-zoom-vector.png') });
    await page.mouse.move(centre.x, centre.y); await page.mouse.wheel(0, -500); await page.waitForTimeout(60);
    assert((await viewer()).s > v.s * 1.5, 'the vector zooms too');
    await page.keyboard.press('ArrowLeft'); await loaded(2000);
    assert.match((await viewer()).cap, /^Etsy listing/);
    await page.click('dialog.phv [data-phv=next]'); await loaded(1600);
    assert.match((await viewer()).cap, /^Vector design/);
    await page.click('dialog.phv [data-phv=prev]'); await loaded(2000);
    assert.equal((await viewer()).n, '1 of 2');

    // 5 · Esc closes the viewer only: the same window, the same piece, the focus back on the thumbnail used
    await page.keyboard.press('Escape'); await closed();
    const after = await winState();
    assert.deepEqual(after, Object.assign({}, before, { active: 'owPhotoBox' }), 'the window is as it was, the focus on the Etsy photo');
    assert.equal(after.dialogs, 1, 'one window'); assert.equal(await page.evaluate(() => OrderWin.isOpen()), true);

    // 6 · from the keyboard: Enter, then Space, on the vector
    await page.focus('#owVectorBox'); await page.keyboard.press('Enter');
    await loaded(1600);
    v = await viewer(); assert.match(v.cap, /^Vector design/); assert.equal(v.n, '2 of 2', 'it opens on the one that was used');
    await page.keyboard.press('Escape'); await closed();
    assert.equal((await winState()).active, 'owVectorBox', 'Esc gives the focus back to the vector');
    assert.deepEqual(await page.evaluate(() => { const b = document.getElementById('owVectorBox'), c = getComputedStyle(b); return [b.matches(':focus-visible'), c.outlineStyle, c.outlineWidth]; }), [true, 'solid', '2px'], 'a keyboard user sees where the focus is');
    await page.keyboard.press('Space'); await loaded(1600);
    assert.match((await viewer()).cap, /^Vector design/, 'Space opens it too');
    await page.keyboard.press('Escape'); await closed();
    await page.focus('#owPhotoBox'); await page.keyboard.press('Enter'); await loaded(2000);
    assert.match((await viewer()).cap, /^Etsy listing/, 'Enter on the photo'); await page.keyboard.press('Escape'); await closed();
    // a click on the vector box (the pointer, not the keys)
    await page.click('#owVector'); await loaded(1600); assert.match((await viewer()).cap, /^Vector design/); await page.keyboard.press('Escape'); await closed();
    assert.equal((await winState()).active, 'owVectorBox');

    // 7 · another piece: its own pictures and its place in the caption; a size that cannot be had falls back to the next
    await page.click(`#owPieceSw [data-piece="${keys[1]}"]`);
    await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owPhoto img')?.getAttribute('src')?.includes('1800200'), keys[1], { timeout: 10000 });
    await settled();
    await page.click('#owPhoto'); await loaded(1000);
    v = await viewer(); assert.match(v.cap, /^Etsy listing · Piece 2 of 3 · CARDINAL_B$/); assert.deepEqual(v.nat, [1000, 750], 'the largest size failed, the next one loaded');
    await page.keyboard.press('Escape'); await closed();
    await page.click(`#owPieceSw [data-piece="${keys[2]}"]`);
    await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owPhoto img')?.getAttribute('src')?.includes('1800300'), keys[2], { timeout: 10000 });
    await settled();
    await page.click('#owPhoto'); await loaded(570);
    v = await viewer(); assert.match(v.cap, /^Etsy listing · Piece 3 of 3 · CARDINAL_C$/); assert.deepEqual(v.nat, [570, 428], 'no larger size could be had: the thumbnail\'s own is shown');
    assert(asked.filter(u => /1800300/.test(u) && /il_fullxfull\./.test(u)).length >= 1 && asked.some(u => /1800300/.test(u) && /il_1588xN\./.test(u)), 'both larger sizes were tried first');
    // piece 3 has no pooled charm: its vector is drawn from its master's design (Pool.masterFront), large
    await page.keyboard.press('ArrowRight'); await loaded(1600);
    v = await viewer(); assert.match(v.cap, /^Vector design · Piece 3 of 3 · CARDINAL_C$/); assert.deepEqual(v.nat, [1600, 1600], 'a master\'s drawing is drawn large too');
    await page.keyboard.press('Escape'); await closed();
    await page.click(`#owPieceSw [data-piece="${keys[0]}"]`);
    await page.waitForFunction(k => OrderWin.key() === k, keys[0]);

    // 8 · a piece with no photo and no design opens nothing; one with a design but no photo opens the vector alone
    const nothing = await page.evaluate(() => { const r = Orders.rows().find(x => x.key === OrderWin.key()); const imageFor = Orders.imageFor; Orders.imageFor = () => null; const was = r.spec.noDesign; r.spec.noDesign = true;
      document.getElementById('owPhotoBox').click(); document.getElementById('owVectorBox').click(); const open = PhotoView.isOpen(); r.spec.noDesign = was; Orders.imageFor = imageFor; return { open }; });
    assert.equal(nothing.open, false, 'with nothing to show, nothing opens');
    await page.evaluate(() => { const imageFor = Orders.imageFor; Orders.imageFor = () => null; window.__imageFor = imageFor; document.getElementById('owVectorBox').click(); });
    await loaded(1600);
    v = await viewer(); assert.equal(v.n, '', 'one picture: no count'); assert.equal(v.navHidden, true, 'and no side buttons');
    await page.keyboard.press('Escape'); await closed();
    await page.evaluate(() => { Orders.imageFor = window.__imageFor; OrderWin.paint(); });

    // 9 · the vector of a design that cannot be drawn says so, with the Etsy photo one arrow away
    await page.evaluate(() => { const r = Orders.rows().find(x => x.key === OrderWin.key()); window.__size = r.spec.size; r.spec.size = 'zzz'; window.__fp = CharmNestPDF.frontPreview; CharmNestPDF.frontPreview = async () => { throw new Error('boom'); }; document.getElementById('owVectorBox').click(); });
    await page.waitForFunction(() => /could not be drawn/.test(document.querySelector('dialog.phv[open] .phvNote')?.textContent || ''), null, { timeout: 5000 });
    await page.keyboard.press('ArrowLeft'); await loaded(2000);
    await page.keyboard.press('Escape'); await closed();
    await page.evaluate(() => { CharmNestPDF.frontPreview = window.__fp; Orders.rows().find(x => x.key === OrderWin.key()).spec.size = window.__size; });

    // 10 · the order window shuts with Esc once the viewer is closed (the viewer never swallowed the window's own Esc)
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });

    // 11 · the inbox's photo viewer, as it was: on the page (not in a dialog) and in a layer of the order window
    await page.evaluate(() => {
      const fx = document.createElement('div'); fx.id = 'fxPage'; fx.className = 'cmThread'; Object.assign(fx.style, { position: 'fixed', left: '10px', top: '10px', zIndex: 5, background: '#fff' });
      fx.innerHTML = ['a', 'b'].map((c, i) => `<button type="button" id="fx${c}" data-photo="/.netlify/functions/imageProxy?url=${c}" data-photo-cors="1" data-photo-cap="Customer photo ${i + 1}"><img alt="" src="/.netlify/functions/imageProxy?url=${c}" width="40" height="30"></button>`).join('');
      document.body.appendChild(fx);
    });
    await page.click('#fxa'); await loaded(300);
    v = await viewer(); assert.equal(v.layer, false, 'on the page it is a dialog of its own'); assert.equal(v.cap, 'Customer photo 1'); assert.equal(v.n, '1 of 2');
    assert.equal(v.nextTitle, 'Next photo (→)'); assert.equal(v.nextLabel, 'Next photo'); assert.equal(v.label, 'Photo'); assert(v.orig);
    await page.keyboard.press('ArrowRight'); await loaded(300); assert.equal((await viewer()).cap, 'Customer photo 2');
    const s0 = (await viewer()).s; await page.mouse.move(centre.x, centre.y); await page.mouse.wheel(0, -400); await page.waitForTimeout(60); assert((await viewer()).s > s0 * 1.5, 'the inbox viewer zooms');
    await page.keyboard.press('Escape'); await closed();
    assert.equal(await page.evaluate(() => document.activeElement.id), 'fxa', 'the focus goes back to the photo that was clicked');
    await page.evaluate(() => document.getElementById('fxPage').remove());
    await page.evaluate(k => OrderWin.open(k), keys[0]);
    await settled();
    await page.evaluate(() => {
      const fx = document.createElement('div'); fx.id = 'fxIn'; fx.className = 'owThread'; Object.assign(fx.style, { position: 'absolute', left: '10px', top: '60px', zIndex: 5, background: '#fff' });
      fx.innerHTML = ['a', 'b'].map((c, i) => `<button type="button" id="fy${c}" data-photo="/.netlify/functions/imageProxy?url=${c}" data-photo-cors="1" data-photo-cap="Team photo ${i + 1}"><img alt="" src="/.netlify/functions/imageProxy?url=${c}" width="40" height="30"></button>`).join('');
      document.getElementById('orderWin').appendChild(fx);
    });
    await page.click('#fya'); await loaded(300);
    v = await viewer(); assert(v.layer && v.inWin, 'in the order window it is a layer of it'); assert.equal(v.cap, 'Team photo 1');
    await page.keyboard.press('Escape'); await closed();
    assert.equal(await page.evaluate(() => document.getElementById('orderWin').open), true, 'Esc closed the viewer only');
    await page.evaluate(() => document.getElementById('fxIn').remove());
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });

    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ click, Enter and Space open the viewer on either picture (largest Etsy size with fallbacks, the vector drawn at 1600 px from a pooled charm and from a master); wheel and pinch zoom with limits, drag pan to the edges, double-click and 0 reset; ← → step between the two; Esc returns to the same window and the same thumbnail; thumbnails never zoom in place; the inbox viewer is unchanged');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('Order photo zoom OK')).catch(e => { console.error(e); process.exit(1); });
