// Click to zoom and pan on the order window's two pictures, in place (Paul, 5 Oct 2026, 13:25 UTC: "You implemented the zoom
// features here as a fullscreen Zoom view. This is not what I wanted. You need to implement the same type of sophisticated
// zoom and pan as we already have in other parts of this app. When clicking to zoom it should zoom in within its own space and
// should be draggable to pan the photo and when clicking to zoom it should centre the area of the image that was clicked").
// The same click-to-zoom as the lists' and Review's thumbnails, one module (charm-nest-zoompan.js); the full gesture and
// geometry checks are in inplace-zoom-pan.cjs. What this test keeps of round 3 is the order window's side of it:
//   · the Etsy listing photo and the Vector design are each a focusable frame that zooms where it lies: a click, Enter or Space
//     zooms it, and NOTHING opens (no viewer, no layer, no second dialog); the frame keeps its size;
//   · the Etsy photo is swapped for the largest size Etsy keeps when the zoom settles (and falls back to the next size, then
//     to the thumbnail's own, when a size cannot be had), and the small one is back when the picture is whole again;
//   · the vector is drawn again larger (never the small thumbnail enlarged), for a pooled charm and for a master's drawing alike,
//     and a drawing that cannot be made larger leaves the zoomed thumbnail as it is;
//   · Esc puts a zoomed picture back whole first, with the window, its tab and its piece as they were and the focus on the
//     frame; the next Esc closes the window;
//   · a piece with no photo or no design has nothing to zoom, and nothing opens;
//   · the inbox's photo viewer (data-photo in a conversation) still behaves exactly as before, on the page and in a layer.
//   node tests/charm-nest/order-photo-zoom.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOTS=dir keeps screenshots)
const path = require('path'), zlib = require('zlib'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

// ── pictures for the fake image proxy: real PNGs of known sizes, so which size the frame loaded can be read ──
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
    // the image proxy: a photo's size is in its Etsy address (il_<size>.), the second piece's largest size cannot be had,
    // nor can the third piece's two larger ones; the inbox's own test photos (…url=a, b) are small
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
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.PhotoView && window.CNZoomPan && CN.S.cloud.ok === true, null, { timeout: 60000 });

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
    // the inbox viewer (section 8)
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

    // a frame as the eye reads it, and what could have opened over the window
    const frame = sel => page.evaluate(sel => {
      const box = document.querySelector(sel), m = box.querySelector('img, canvas'), r = box.getBoundingClientRect();
      return { zp: box.dataset.zp, role: box.getAttribute('role'), tab: box.tabIndex, label: box.getAttribute('aria-label'), title: box.title, cursor: box.style.cursor, size: [r.width, r.height].map(Math.round).join('x'),
        tag: m && m.tagName, s: m ? +(m.dataset.scale || 1) : 1, nat: m ? [m.naturalWidth || m.width, m.naturalHeight || m.height] : [0, 0], src: m ? decodeURIComponent((m.currentSrc || m.src || '').slice(0, 400)) : '', transform: m ? m.style.transform : '' };
    }, sel);
    const over = () => page.evaluate(() => ({ dialogs: document.querySelectorAll('dialog[open]').length, viewer: !!document.querySelector('dialog.phv[open]'), photoView: window.PhotoView ? PhotoView.isOpen() : false }));
    const natOf = (sel, w) => page.waitForFunction(({ sel, w }) => { const m = document.querySelector(sel + ' img, ' + sel + ' canvas'); return m && (m.naturalWidth || m.width) === w && (m.complete !== false); }, { sel, w }, { timeout: 20000 });
    const bigOf = (sel, w) => page.waitForFunction(({ sel, w }) => { const m = document.querySelector(sel + ' img, ' + sel + ' canvas'); return m && Math.max(m.naturalWidth || m.width, m.naturalHeight || m.height) >= w; }, { sel, w }, { timeout: 20000 });
    const tap = async (sel, fx = .5, fy = .5) => { const b = await page.locator(sel).boundingBox(); await page.mouse.click(b.x + b.width * fx, b.y + b.height * fy); };
    const piece = async i => { await page.click(`#owPcSum .owPcRow[data-piece="${keys[i]}"] .dot`); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owPhoto img')?.complete, keys[i], { timeout: 10000 }); await settled(); };

    // 1 · open the order (piece 1): both pictures drawn, each a focusable frame that zooms where it lies, no tooltip, no reset button
    await page.click(`#ordItems [data-key="${keys[0]}"]`);
    await settled();
    await page.waitForFunction(() => document.querySelector('#owPhoto img')?.complete && document.querySelector('#owVector img, #owVector canvas'), null, { timeout: 20000 });
    const f0 = await frame('#owPhoto'), v0 = await frame('#owVector');
    for (const [g, name, label] of [[f0, 'photo', /Etsy listing photo/], [v0, 'vector', /vector design/i]]) {
      assert.deepEqual([g.role, g.tab, g.zp, g.cursor, g.s], ['button', 0, 'rest', 'zoom-in', 1], `the ${name} is a focusable frame that zooms where it lies: ` + JSON.stringify(g));
      assert.match(g.label, label); assert.equal(g.title, '', `no tooltip on the ${name}`);
    }
    assert.equal(await page.evaluate(() => [document.getElementById('owPhoto').classList.contains('zoomReady'), document.getElementById('owVector').classList.contains('zoomReady'), !!document.querySelector('.owPics .thumbReset')].join()), 'false,false,false', 'neither is bound to the lists\' own zoom, and no list-style reset button');
    assert(v0.nat[0] <= 260 && v0.nat[1] <= 260, 'the vector thumbnail is the small one: ' + v0.nat);
    assert.equal(f0.nat[0], 570, 'the photo at rest is the small one: ' + f0.src);
    assert.equal(await page.evaluate(() => !!document.getElementById('owPhotoBox') || !!document.getElementById('owVectorBox')), false, 'the round-3 wrappers of the fullscreen route are gone');
    // the wheel over a picture scrolls the page as it did: it does not zoom it
    await page.hover('#owPhoto'); await page.mouse.wheel(0, -300); await page.waitForTimeout(150);
    assert.equal((await frame('#owPhoto')).s, 1, 'a wheel does not zoom a picture');
    if (shots) await page.screenshot({ path: path.join(shots, 'order-photo-zoom-rest.png') });

    // 2 · a click on the Etsy photo zooms it in its own frame: nothing opens, the frame keeps its size, the largest size comes
    const before = await winState(), sizeBefore = f0.size, askedBefore = asked.length;
    await tap('#owPhoto', .3, .3);
    await natOf('#owPhoto', 2000);
    let z = await frame('#owPhoto');
    assert(z.s > 1.2 && z.zp === 'zoomed', 'the click zoomed it in place: ' + z.s); assert.equal(z.size, sizeBefore, 'the frame keeps its size');
    assert.deepEqual(await over(), { dialogs: 1, viewer: false, photoView: false }, 'nothing opened over the window: no viewer, no layer, no second dialog');
    assert(/il_fullxfull\./.test(z.src), 'the largest size is the one shown: ' + z.src); assert(asked.slice(askedBefore).some(u => /il_fullxfull\./.test(u)), 'the largest size was asked for');
    assert.equal(asked.slice(askedBefore).filter(u => /il_fullxfull\./.test(u)).length, 1, 'once');
    assert.equal(await page.evaluate(() => document.querySelector('#owPhoto img').crossOrigin), 'anonymous', 'the swapped picture is loaded as the thumbnail was');
    if (shots) await page.screenshot({ path: path.join(shots, 'order-photo-zoom-photo.png') });
    // a drag pans it, and the click that ends the drag is not a click
    const pb = await page.locator('#owPhoto').boundingBox();
    await page.mouse.move(pb.x + pb.width / 2, pb.y + pb.height / 2); await page.mouse.down(); await page.mouse.move(pb.x + pb.width / 2 - 40, pb.y + pb.height / 2 - 24, { steps: 6 }); await page.mouse.up(); await page.waitForTimeout(100);
    const zp = await page.evaluate(() => { const m = document.querySelector('#owPhoto img'); return { s: +m.dataset.scale, x: +m.dataset.offsetX, y: +m.dataset.offsetY }; });
    assert(Math.abs(zp.s - z.s) < 1e-9 && (zp.x !== 0 || zp.y !== 0), 'a drag pans it and does not change the zoom: ' + JSON.stringify(zp));
    assert.deepEqual(await over(), { dialogs: 1, viewer: false, photoView: false });

    // 3 · Esc puts it back whole (the window, its tab and its piece as they were, the focus on the frame); the small photo is back
    await page.keyboard.press('Escape'); await natOf('#owPhoto', 570);
    z = await frame('#owPhoto'); assert(z.s === 1 && z.zp === 'rest' && z.transform === '', 'Esc put the photo back whole: ' + JSON.stringify([z.s, z.zp]));
    const after = await winState();
    assert.deepEqual(after, Object.assign({}, before, { active: 'owPhoto' }), 'the window is as it was, the focus on the Etsy photo');
    assert.equal(await page.evaluate(() => OrderWin.isOpen()), true, 'the first Esc did not close the window');

    // 4 · from the keyboard: Enter, then Space, on the vector; the vector is drawn again larger, then the small one is back
    await page.focus('#owVector'); await page.keyboard.press('Enter');
    await bigOf('#owVector', 500);
    let v = await frame('#owVector');
    assert(v.s >= 1.9 && v.zp === 'zoomed' && Math.max(...v.nat) >= 500 && Math.max(...v.nat) <= 2048, `Enter zooms the vector in place and it is drawn again larger, not the thumbnail enlarged (${v.nat}, x${v.s})`);
    assert.equal(v.size, v0.size, 'the vector frame keeps its size'); assert.deepEqual(await over(), { dialogs: 1, viewer: false, photoView: false });
    if (shots) await page.screenshot({ path: path.join(shots, 'order-photo-zoom-vector.png') });
    await page.keyboard.press('Space'); await page.waitForFunction(() => { const m = document.querySelector('#owVector img, #owVector canvas'); return m && Math.max(m.naturalWidth || m.width, m.naturalHeight || m.height) <= 260; }, null, { timeout: 8000 });
    v = await frame('#owVector'); assert(v.s === 1 && v.transform === '', 'Space put it back whole, and the small drawing with it: ' + JSON.stringify([v.s, v.nat]));
    assert.deepEqual(await page.evaluate(() => { const b = document.getElementById('owVector'), c = getComputedStyle(b); return [b.matches(':focus-visible'), c.outlineStyle, c.outlineWidth]; }), [true, 'solid', '2px'], 'a keyboard user sees where the focus is');
    assert.equal((await winState()).active, 'owVector');
    // and a click on the vector (the pointer, not the keys)
    await tap('#owVector', .5, .4); await page.waitForTimeout(450); assert((await frame('#owVector')).s > 1.2, 'a click zooms the vector'); await page.keyboard.press('Escape'); await page.waitForTimeout(400);
    assert.equal((await frame('#owVector')).s, 1);

    // 5 · another piece starts whole, with its own pictures; a size that cannot be had falls back to the next
    await tap('#owPhoto'); await natOf('#owPhoto', 2000);
    await piece(1);
    z = await frame('#owPhoto'); assert(z.s === 1 && z.nat[0] === 570 && /1800200/.test(z.src), 'the next piece starts whole, with its own photo: ' + JSON.stringify([z.s, z.nat]));
    await tap('#owPhoto'); await natOf('#owPhoto', 1000);
    z = await frame('#owPhoto'); assert(z.s > 1.2 && z.nat[0] === 1000, 'the largest size failed, the next one loaded: ' + z.nat); assert(asked.some(u => /1800200/.test(u) && /il_fullxfull\./.test(u)), 'the largest was tried first');
    await page.keyboard.press('Escape'); await natOf('#owPhoto', 570);
    await piece(2);
    const askedThird = asked.length;
    await tap('#owPhoto'); await page.waitForTimeout(1200);
    z = await frame('#owPhoto'); assert(z.s > 1.2 && z.nat[0] === 570, 'no larger size could be had: the thumbnail\'s own stays, zoomed: ' + JSON.stringify([z.s, z.nat]));
    assert(asked.slice(askedThird).some(u => /1800300/.test(u) && /il_fullxfull\./.test(u)) && asked.slice(askedThird).some(u => /1800300/.test(u) && /il_1588xN\./.test(u)), 'both larger sizes were tried first');
    await page.keyboard.press('Escape'); await page.waitForTimeout(300);
    // piece 3 has no pooled charm: its vector is drawn from its master's design, larger too
    await tap('#owVector'); await bigOf('#owVector', 500);
    v = await frame('#owVector'); assert(v.s > 1.2 && Math.max(...v.nat) >= 500, 'a master\'s drawing is drawn larger too: ' + v.nat);
    await page.keyboard.press('Escape'); await page.waitForTimeout(400);

    // 6 · a drawing that cannot be made larger leaves the zoomed thumbnail as it is, and nothing breaks
    await piece(1);   // (a piece whose larger drawing has not been made yet: the ones made are kept)
    await page.evaluate(() => { window.__fp = CharmNestPDF.frontPreview; CharmNestPDF.frontPreview = async () => { throw new Error('boom'); }; });
    await tap('#owVector'); await page.waitForTimeout(900);
    v = await frame('#owVector'); assert(v.s > 1.2 && Math.max(...v.nat) <= 260 && v.zp === 'zoomed', 'the vector zooms on its thumbnail when the larger drawing fails: ' + JSON.stringify([v.s, v.nat]));
    assert.deepEqual(await over(), { dialogs: 1, viewer: false, photoView: false });
    await page.keyboard.press('Escape'); await page.waitForTimeout(400);
    await page.evaluate(() => { CharmNestPDF.frontPreview = window.__fp; });

    // 7 · a piece with no photo has nothing to zoom there: nothing opens, nothing breaks; the photo coming back zooms again
    const none = await page.evaluate(async () => {
      const r = Orders.rows().find(x => x.key === OrderWin.key()); window.__imageFor = Orders.imageFor; window.__wantImage = Orders.wantImage; Orders.imageFor = () => null; Orders.wantImage = () => Promise.resolve();   // (a listing whose photo is not known, and is not found)
      OrderWin.paint(); await new Promise(res => setTimeout(res, 300));
      const ph = document.getElementById('owPhoto'); ph.click(); ph.dispatchEvent(new MouseEvent('click', { bubbles: true, clientX: 30, clientY: 30, detail: 1 }));
      return { zp: ph.dataset.zp, media: !!ph.querySelector('img, canvas'), role: ph.getAttribute('role'), open: PhotoView.isOpen(), dialogs: document.querySelectorAll('dialog[open]').length };
    });
    assert.deepEqual(none, { zp: 'none', media: false, role: null, open: false, dialogs: 1 }, 'with no photo there is nothing to zoom and nothing opens: ' + JSON.stringify(none));
    await page.evaluate(() => { Orders.imageFor = window.__imageFor; Orders.wantImage = window.__wantImage; OrderWin.paint(); });
    await natOf('#owPhoto', 570);
    await tap('#owPhoto'); await page.waitForTimeout(450); assert((await frame('#owPhoto')).s > 1.2, 'and when the photo is back it zooms again');
    await page.keyboard.press('Escape'); await page.waitForTimeout(400);

    // 8 · the next Esc closes the window as it always did (Esc never closes it while a picture is zoomed)
    await tap('#owPhoto'); await page.waitForTimeout(450);
    await page.keyboard.press('Escape'); await page.waitForTimeout(450);
    assert.equal(await page.evaluate(() => document.getElementById('orderWin').open), true, 'Esc on a zoomed picture leaves the window open');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });

    // 9 · the inbox's photo viewer, as it was: on the page (not in a dialog) and in a layer of the order window
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
    console.log('  ✓ a click, Enter and Space zoom the listing photo and the vector in their own frames (nothing opens, the frame keeps its size, a drag pans); the photo is swapped for the largest Etsy size that can be had and the vector is drawn again larger, from a pooled charm and from a master; Esc puts it back whole with the window as it was; a piece with no photo has nothing to zoom; the inbox viewer is unchanged');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('Order photo zoom OK')).catch(e => { console.error(e); process.exit(1); });
