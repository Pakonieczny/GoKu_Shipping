// Round 3 of the order window, all five changes at once (Paul, 5 Oct 2026): the back engraving card under the thumbnails (R3-B), the
// "Open it in Engraving" / "View in Engrave" deep link (R3-A), the window opened on the piece that was asked for (R3-C), the
// thumbnails shown whole (R3-D) and the click-to-zoom viewer on the Etsy listing and the Vector design (R3-E). Each has its own
// test; this one is the order of an afternoon at the bench, with an order shaped like 4170837249 (3 pieces: CABLE CHAIN ONLY RG,
// MIDDLE 9935 GF, MIDDLE 9935 RG), the RG middle selected and its back in review ("KMB // SMH"), the GF middle approved ("I dissent"),
// the chain with no back engraving, and what only shows when they act together:
//   1 Overview: thumbnails whole, the card under them              6 the viewer (click, step, wheel, drag, Esc) and the card; the
//   2 the piece switcher swaps card, pictures and viewer's caption     viewer opened while an approval's seal is being pressed
//   3 the red box and the card's "View in Engrave" land alike      7 other doors: openOrderFrom / Issues rows / Sheet window / search
//   4 Approve on the card: name, one seal, permanent, red box,     8 390 and 900 px wide: no overlap, no sideways scroll
//     timeline, Sheet tab, the Engraving tab's Decided             9 twenty piece switches while a seal is pressing
//   5 approved in the Engraving tab / the Sheet tab: the card      10 Sandbox and production never mix; a module missing: the window works
//     follows within about 2 s
// Headless Chromium on the fake site (bridge-server.cjs), offline. The approval itself (fit, back file, cloud) is a stand-in with the
// same order of steps as Engrave.approve (preparing, intent, seal added, pressed on the very button, settled, timeline told); the card,
// the link, the viewer, the Sheet tab and the window are the page's own. Assertions are counts and strings, never element handles.
//   node tests/charm-nest/order-window-round3-e2e.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir> keeps screenshots)
const fs = require('fs'), path = require('path'), zlib = require('zlib');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

// ── real PNGs of known sizes for the fake image proxy ──
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
const SIZES = { fullxfull: [2000, 1500], '1588xN': [1000, 750], '570xN': [570, 428] };

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
const GF = 'sheet-r3-gf', RG = 'sheet-r3-rg';
// order B is Paul's 4170837249; R2 and R4 are the orders that get approved from other places; R5 has no back engraving at all
const B = { rid: '4170837249', skus: ['CABLE_CHAIN_ONLY', 'MIDDLE_9935', 'MIDDLE_9935'], metal: ['rose', 'gold', 'rose'], label: ['RG 14/20', 'GF 14/20', 'RG 14/20'], lids: ['1800101', '1800102', '1800103'] };
const R2 = { rid: '4175550002', skus: ['ASTER_FLOWER', 'LEAF_CHARM'], metal: ['gold', 'gold'], label: ['GF 14/20', 'GF 14/20'], lids: ['1800201', '1800202'] };
const R4 = { rid: '4175550004', skus: ['ALPHA_TAG', 'BRAVO_TAG', 'CHARLIE_TAG'], metal: ['gold', 'gold', 'gold'], label: ['GF 14/20', 'GF 14/20', 'GF 14/20'], lids: ['1800401', '1800402', '1800403'] };
const R5 = { rid: '4175550005', skus: ['PLAIN_TAG'], metal: ['gold'], label: ['GF 14/20'], lids: ['1800501'] };
const R6 = { rid: '4175550006', skus: ['ZETA_TAG'], metal: ['gold'], label: ['GF 14/20'], lids: ['1800601'] };
const R7 = { rid: '4175550007', skus: ['ETA_TAG', 'THETA_TAG', 'IOTA_TAG'], metal: ['gold', 'gold', 'gold'], label: ['GF 14/20', 'GF 14/20', 'GF 14/20'], lids: ['1800701', '1800702', '1800703'] };
const R8 = { rid: '4175550008', skus: ['KAPPA_TAG'], metal: ['gold'], label: ['GF 14/20'], lids: ['1800801'] };
const ALL = [B, R2, R4, R5, R6, R7, R8];
const tid = (o, i) => o.rid + (i + 1), key = (o, i) => `${o.rid}_${tid(o, i)}`, pool = (o, i) => key(o, i) + '_1';
const WORDS = { [key(B, 1)]: 'I\ndissent', [key(B, 2)]: 'KMB //\nSMH', [key(R2, 0)]: 'Aster', [key(R2, 1)]: 'Leaf', [key(R4, 0)]: 'Alpha', [key(R4, 1)]: 'Bravo', [key(R4, 2)]: 'Charlie', [key(R6, 0)]: 'Zeta', [key(R7, 0)]: 'Eta', [key(R7, 1)]: 'Theta', [key(R7, 2)]: 'Iota', [key(R8, 0)]: 'Kappa' };
const LID_KEY = {}; for (const o of ALL) o.lids.forEach((l, i) => { LID_KEY[l] = key(o, i); });
const line = (o, i) => ({ transactionId: tid(o, i), listingId: o.lids[i], sku: o.skus[i], title: o.skus[i].replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: o.label[i] }], metalKey: o.metal[i], metalLabel: o.label[i], personalization: '' });
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: o.skus.map((_, i) => line(o, i)) });
const etsy = id => `https://i.etsystatic.com/1/r/il/abcd/${id}/il_570xN.${id}_wxyz.jpg`;

function seed(st) {
  const sheet = (id, metal, pieces, w) => {
    const pl = [], ch = [];
    pieces.forEach(([o, i], n) => { pl.push({ id: 'c' + n, cxPt: 20 + n * 30, cyPt: 30, angle: 0, wPt: 26, hPt: 26 }); ch.push({ id: 'c' + n, name: `${o.rid} · ${o.skus[i]}`, poolId: pool(o, i), order: o.rid, sku: o.skus[i] }); });
    st.put('Charm_Nest_Sheets', id, { id, metal, sheetIndex: 1, setSeq: 1, day: '2026-10-05', status: 'written', fileBase: id, folder: id, stock: { wPt: w, hPt: 90 }, orders: [...new Set(ch.map(c => c.order))], placements: pl, charms: ch, backPool: [] });
    for (const [o, i] of pieces) st.put('Charm_Pool', pool(o, i), { poolId: pool(o, i), orderId: o.rid, transactionId: tid(o, i), lineKey: key(o, i), sku: o.skus[i], material: metal, copy: 1, quantity: 1, state: 'written', sheetId: id, sheetName: id, updatedAt: Date.now() });
  };
  sheet(GF, 'gold', [[B, 1], [R2, 0], [R2, 1], [R4, 0], [R4, 1], [R4, 2], [R5, 0], [R6, 0], [R7, 0], [R7, 1], [R7, 2], [R8, 0]], 360);
  sheet(RG, 'rose', [[B, 0], [B, 2]], 120);
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: the browser checks were not run'); return; } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const T0 = Date.now(), srv = await start({ receipts: [] }); seed(srv.st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const say = m => { if (process.env.R3_LOG) console.log(`    [${((Date.now() - T0) / 1000).toFixed(1)}s] ${m}`); };
  let step = 'start';
  const guard = setTimeout(() => { console.error('  ✗ the run took too long (stopped at: ' + step + ')'); process.exit(2); }, +process.env.R3_BUDGET_MS || 540000);
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, hasTouch: false });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    // the image proxy: the photo's size is in its Etsy address; the chain's photo is a tall one, the others wide
    await context.route(/\/\.netlify\/functions\/imageProxy/, r => {
      const u = new URL(r.request().url()).searchParams.get('url') || '', m = /\/il_([^.]+)\./.exec(u), lid = (/\/(\d{7})\/il_/.exec(u) || [])[1];
      let [w, h] = m && SIZES[m[1]] || [300, 200]; if (lid === B.lids[0]) [w, h] = [h, w];
      return r.fulfill({ status: 200, headers: { 'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: png(w, h) });
    });
    const index = ALL.flatMap(o => o.lids).map(l => [l, '/.netlify/functions/imageProxy?url=' + encodeURIComponent(etsy(l))]);
    await context.addInitScript(idx => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.tour.seen', '1'); localStorage.setItem('cn.listingPhotos.v1', JSON.stringify(idx)); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
    }, index);
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    const noise = t => /Failed to load resource|net::ERR|favicon/i.test(t);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('console', m => { if (m.type() === 'error' && !noise(m.text())) { errors.push('console: ' + m.text()); console.error('console error:', m.text().slice(0, 300)); } });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && window.Engrave && window.OrderEngraving && window.EngraveLink && window.PhotoView && window.CNEngravingSeals && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 90000 });

    // ── the world: three orders, their jobs as Engrave holds them, and the approval as a stand-in ──
    step = 'world';
    await page.evaluate(async ({ orders, WORDS, keys, pools, GF, RG }) => {
      const T0 = Date.now() - 6 * 3600e3;
      for (const o of orders) for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: T0, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      // a vector design for every piece: one pooled charm (a tag with a ring), drawn by the page's own ListMedia
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [20, 0]], ['l', [20, 48]], ['l', [0, 48]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 20, 48] };
      const charm = { id: 'c', name: 'TAG', metal: 'gold', centerPt: [10, 24], widthPt: 20, heightPt: 48, areaPt2: 20 * 48, outline: sq, members: [sq], bbox: [0, 0, 20, 48], thumb: '' };
      const mine = new Set(pools), was = Pool.charmOf; Pool.charmOf = id => mine.has(id) ? Object.assign({ poolId: id }, charm) : was(id);
      const art = text => { const c = document.createElement('canvas'); c.width = c.height = 300; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 300, 300); x.fillStyle = '#111'; x.font = '40px sans-serif'; x.textAlign = 'center'; (text || '').split('\n').forEach((t, i) => x.fillText(t, 150, 130 + i * 48)); return c; };
      Engrave.renderBack = job => art(job.text);
      const fit = () => ({ size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' });
      const mk = (key, state, extra) => { const row = B.orders.byKey.get(key), text = WORDS[key], j = Engrave.ensureJob(row); Object.assign(j, { state, text, lines: text.split('\n'), activityAt: Date.now() }, extra || {}); row.engrave = { needed: true, state, approved: false, text }; return j; };
      const review = () => ({ fit: fit(), view: {}, verify: { geometry: { ok: true } } });
      const at = Date.now() - 2 * 3600e3, g = keys.B1;
      const j2 = mk(g, 'approved', { approvedBy: 'Giovanna', approvedAt: at, backs: [{ poolId: g + '_1', approvedAt: at, approvedBy: 'Giovanna', png: art(WORDS[g]).toDataURL() }] });
      Object.assign(B.orders.byKey.get(g).engrave, { approved: true, approvedBy: 'Giovanna', approvedAt: at }); CNEngravingSeals.add(j2, 'engraveApproved', 'Giovanna', at); CNEngravingSeals.keep(j2);
      for (const k of [keys.B2, ...keys.R2, ...keys.R4, ...keys.R6, ...keys.R7, ...keys.R8]) mk(k, 'review', review());
      // the approval, with the steps of Engrave.approve: one task per job, preparing, intent, the seal added and pressed on the very button, settled, timeline told
      window.__approve = []; window.__links = []; window.__delay = 250; window.__gate = null;
      window.__hold = () => { window.__gate = new Promise(r => { window.__letGo = r; }); }; window.__release = () => { const f = window.__letGo; window.__gate = null; if (f) f(); };
      const sleep = ms => new Promise(r => setTimeout(r, ms));
      Engrave.approve = async (job, by, button) => {
        if (job._approvalTask) return;
        const task = (async () => {
          if (job.state !== 'review' || job.stamping || job.approvalPreparing || job.backSaving || (job.row && job.row.state === 'gone')) return;   // (Engrave.approve's own guards: a line that left the pull is refused without a word)
          __approve.push({ key: job.key, by, button: !!button && button.isConnected });
          const btn = button && button.isConnected === false ? null : button;
          job.approvalPreparing = true;
          try {
            if (window.__gate) await window.__gate; else await sleep(window.__delay);   // (a test may hold the approval at its preparing step, and let it go)
            const at = Date.now(); job.approvalIntent = { phase: 'verified', at, by, text: job.text };
            CNEngravingSeals.keep(job); const seal = CNEngravingSeals.add(job, 'engraveApproved', by, at);
            job.stamping = true; try { await CNEngravingSeals.press(btn, seal); } finally { job.stamping = false; }
            CNEngravingSeals.add(job, 'engraveApproved', by, at);
            Object.assign(job, { state: 'approved', approvedBy: by, approvedAt: at });
            Object.assign(job.row.engrave || (job.row.engrave = {}), { needed: true, state: 'approved', approved: true, approvedBy: by, approvedAt: at, text: job.text });
            delete job.approvalIntent; Engrave.timelineApproved(job);
          } finally { job.approvalPreparing = false; }
        })();
        job._approvalTask = task; try { return await task; } finally { if (job._approvalTask === task) delete job._approvalTask; }
      };
      window.__toasts = []; const toast0 = window.toast; window.toast = function (m) { __toasts.push(String(m)); return toast0.apply(this, arguments); };
      window.__renders = 0; const realRender = Engrave.render; window.__realRender = realRender; Engrave.render = function () { __renders++; return realRender.apply(this, arguments); };
      CN.setMode('orders'); Orders.render();
    }, { orders: [order(B, 'Leslie Suhr'), order(R2, 'Ava Patel'), order(R4, 'Cora Lind'), order(R5, 'Hannah Whitford'), order(R6, 'Mia Lund'), order(R7, 'Noor Haq'), order(R8, 'Pia Roth')],
      WORDS, keys: { B1: key(B, 1), B2: key(B, 2), R2: [key(R2, 0), key(R2, 1)], R4: [key(R4, 0), key(R4, 1), key(R4, 2)], R6: [key(R6, 0)], R7: [key(R7, 0), key(R7, 1), key(R7, 2)], R8: [key(R8, 0)] }, pools: ALL.flatMap(o => o.skus.map((_, i) => pool(o, i))), GF, RG });
    await page.waitForSelector(`#ordItems [data-key="${key(B, 2)}"]`);

    // ── what the checks read, as plain numbers and strings ──
    const animRunning = () => document.getElementById('orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity);
    const settled = () => page.waitForFunction(`(${animRunning})() === false`, null, { timeout: 20000 });
    const card = () => page.evaluate(() => {
      const h = document.getElementById('owEng'), q = s => h.querySelector(s), e = q('.swEng');
      const seals = [...h.querySelectorAll('.egButtonSeal .seal svg[data-seal-model]')].map(s => { try { const m = JSON.parse(s.getAttribute('data-seal-model')); return m.action + '|' + m.by; } catch (_) { return '?'; } });
      const r = h.getBoundingClientRect(), sku = document.getElementById('owSku').getBoundingClientRect(), vec = document.getElementById('owVector').getBoundingClientRect(), ph = document.getElementById('owPhoto').getBoundingClientRect();
      return { hidden: h.hidden, state: e ? e.dataset.state : '', words: (q('.words') || {}).textContent || '', chip: (q('.top span') || {}).textContent || '', title: (q('.top b') || {}).textContent || '', approve: !!q('[data-e=approve]'), disabled: !!(q('.egApproveButton') || {}).disabled,
        open: ((q('[data-e=engrave]') || {}).textContent || '').trim(), preview: !!q('.pv canvas, .pv img'), seals, wait: (q('.owEngWait') || {}).textContent || '', red: document.querySelectorAll('#owFix .owFix').length,
        under: r.height > 0 && r.top >= Math.max(sku.bottom, vec.bottom, ph.bottom) - 1 && Math.abs(r.left - ph.left) < 2, w: Math.round(r.width), h: Math.round(r.height), lid: document.getElementById('owPhoto').dataset.lid || '', key: OrderWin.key() };
    });
    const viewer = () => page.evaluate(() => {
      const d = document.querySelector('dialog.phv'); if (!d || !d.open) return null;
      const i = d.querySelector('.phvImg'), st = d.querySelector('.phvStage'), m = /translate\(([-\d.e]+)px,\s*([-\d.e]+)px\) scale\(([-\d.e]+)\)/.exec(i.style.transform || '');
      return { cap: d.querySelector('.phvCap').textContent, n: d.querySelector('.phvN').textContent, src: (i.currentSrc || i.src || '').slice(0, 200), nat: [i.naturalWidth, i.naturalHeight], tx: m && +m[1], ty: m && +m[2], s: m && +m[3], sw: st.clientWidth, sh: st.clientHeight, layer: d.classList.contains('layer'), inWin: !!d.closest('#orderWin') };
    });
    const loaded = () => page.waitForFunction(() => { const i = document.querySelector('dialog.phv[open] .phvImg'); return i && i.complete && i.naturalWidth > 0 && i.style.visibility !== 'hidden'; }, null, { timeout: 20000 }).catch(async e => {
      const d = await page.evaluate(() => { const d = document.querySelector('dialog.phv'); if (!d) return 'no viewer'; const i = d.querySelector('.phvImg'), n = d.querySelector('.phvNote'); return JSON.stringify({ open: d.open, layer: d.classList.contains('layer'), parent: d.parentElement && (d.parentElement.id || d.parentElement.tagName), vis: i.style.visibility, complete: i.complete, nat: i.naturalWidth, src: (i.src || '').slice(-60), note: n.hidden ? '' : n.textContent, cap: d.querySelector('.phvCap').textContent, seal: Seal.busy() }); });
      console.error('    viewer did not load: ' + d); throw e; });
    const closedViewer = (ms = 4000) => page.waitForFunction(() => !document.querySelector('dialog.phv[open]'), null, { timeout: ms }).then(() => true, () => false);
    const winState = () => page.evaluate(() => ({ open: document.getElementById('orderWin').open, key: OrderWin.key(), view: document.querySelector('.owTabsV [aria-selected=true]').dataset.owView, title: document.getElementById('owTitle').textContent, active: document.activeElement && document.activeElement.id || '' }));
    const open = async k => { await page.evaluate(k => OrderWin.open(k), k); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, k, { timeout: 15000 }); await settled(); };
    const closeWin = async () => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open && !OrderWin.isOpen(), null, { timeout: 8000 }).catch(() => {}); await page.waitForTimeout(250); };
    const pick = async k => { await page.evaluate(k => document.querySelector(`#owPieceSw [data-piece="${k || ''}"]`).click(), k); };
    const waitCard = (state, ms = 9000) => page.waitForFunction(state => { const e = document.querySelector('#owEng .swEng'); return e && e.getClientRects().length && (!state || e.dataset.state === state); }, state, { timeout: ms }).then(() => true, () => false);
    const waitNoCard = (ms = 9000) => page.waitForFunction(() => { const h = document.getElementById('owEng'); return h.hidden && !h.firstChild; }, null, { timeout: ms }).then(() => true, () => false);
    const picsReady = () => page.waitForFunction(() => document.querySelector('#owPhoto img')?.complete && document.querySelector('#owVector img, #owVector canvas'), null, { timeout: 20000 });
    const approvals = () => page.evaluate(() => __approve.map(a => a.key + '|' + a.by + '|' + a.button));
    const sealCount = k => page.evaluate(k => CNEngravingSeals.list(Engrave.items().get(k)).length, k);
    const view = () => page.evaluate(() => { const v = Engrave.view(); return JSON.stringify({ tab: v.tab, focus: v.focus, list: v.list, chosen: v.chosen, q: v.q, openDone: v.openDone || null }); });
    const resetEngrave = async () => { await page.evaluate(() => { Engrave.restoreView({ tab: 'place', focus: null, list: true, chosen: true, q: '', openDone: null }); CN.setMode('orders'); Orders.render(); }); await page.waitForTimeout(300); };
    const k = { c: key(B, 0), g: key(B, 1), r: key(B, 2) };
    const EXPECT = key => key === k.c ? '' : (WORDS[key] || '');
    const bad = [];   // every moment a card showed words that are not its piece's, or a card on the chain

    // ══ 1 · Overview of the RG middle: thumbnails whole, the card under them ══
    step = '1'; say(step);
    await open(k.r); await picsReady();
    check(await waitCard('approve'), 'the Overview of the RG middle (piece 3 of 3) shows the card, in review');
    await settled(); await page.mouse.move(700, 890);
    let c = await card();
    check(c.state === 'approve' && c.words === 'KMB //\nSMH' && c.approve && c.preview && c.open === 'Adjust in Engrave →' && c.title === 'Check the back, then approve', 'the card: words "KMB // SMH", the Approved button, the placement picture, Adjust in Engrave: ' + JSON.stringify([c.state, c.words, c.approve, c.preview, c.open]));
    check(c.under && c.red === 1, 'it sits under the Etsy listing, the Vector design and the SKU line, and the red box is there too');
    const thumbs = () => page.evaluate(() => ['owPhoto', 'owVector'].map(id => {
      const b = document.getElementById(id), e = b.querySelector('img,canvas'); if (!e) return { id, none: true };
      const br = b.getBoundingClientRect(), er = e.getBoundingClientRect(), cs = getComputedStyle(e), t = /matrix\(([^)]+)\)/.exec(cs.transform), tm = t ? t[1].split(',').map(Number) : [1, 0, 0, 1, 0, 0];
      return { id, inside: er.left >= br.left - .5 && er.top >= br.top - .5 && er.right <= br.right + .5 && er.bottom <= br.bottom + .5, fit: cs.objectFit, still: Math.abs(tm[0] - 1) < 1e-3 && Math.abs(tm[3] - 1) < 1e-3 && Math.abs(tm[4]) < .5 && Math.abs(tm[5]) < .5,
        square: Math.abs(br.width - br.height) < 1.5, wide: Math.round(br.width), listZoom: b.classList.contains('zoomReady') };
    }));
    let th = await thumbs();
    check(th.every(t => !t.none && t.inside && t.still && t.square && !t.listZoom) && th[0].fit === 'contain', 'the Etsy photo (a tall one) and the Vector design lie whole inside their boxes, unzoomed: ' + JSON.stringify(th));
    if (shots) await page.screenshot({ path: path.join(shots, '1-overview-1440.png') });

    // ══ 2 · the piece switcher: card, pictures and viewer's caption follow the piece ══
    step = '2'; say(step);
    await page.evaluate(({ WORDS, LID_KEY, chain }) => {
      const h = document.getElementById('owEng'); window.__log = [];
      new MutationObserver(() => { const lid = document.getElementById('owPhoto').dataset.lid || '', want = LID_KEY[lid] || '', w = (h.querySelector('.words') || {}).textContent || '', st = (h.querySelector('.swEng') || {}).dataset?.state || '';
        __log.push({ lid, want, w, st, hidden: h.hidden, wrong: !!w && w !== (WORDS[want] || '#') || (!!st && want === chain) }); }).observe(h, { childList: true, subtree: true, attributes: true, attributeFilter: ['hidden'] });
    }, { WORDS, LID_KEY, chain: k.c });
    const viewOf = async (lid, cap) => {
      await page.click('#owPhoto'); await loaded(); const v = await viewer();
      check(v && v.layer && v.inWin && v.src.includes(lid) && cap.test(v.cap) && v.n === '1 of 2', `the viewer on piece ${cap.source.match(/Piece (\d)/)[1]}: its own listing photo (${lid}), the caption "${v && v.cap}"`);
      await page.keyboard.press('ArrowRight'); await loaded(); const w = await viewer();
      check(w && Math.max(...w.nat) === 1600 && /^Vector design · Piece \d of 3/.test(w.cap) && w.cap.replace(/^Vector design/, 'Etsy listing') === v.cap, `its Vector design follows with the same piece in the caption: "${w && w.cap}"`);
      await page.keyboard.press('Escape'); await closedViewer();
    };
    await pick(k.c); await waitNoCard();
    c = await card(); check(c.hidden && !c.state && c.lid === B.lids[0], 'the chain (piece 1) has no back engraving: no card, its own listing photo');
    await picsReady(); await viewOf(B.lids[0], /^Etsy listing · Piece 1 of 3 · CABLE_CHAIN_ONLY$/);
    await pick(k.g); await waitCard('approved'); await picsReady();
    c = await card(); check(c.state === 'approved' && c.words === 'I\ndissent' && c.disabled && !c.approve && c.open === 'View in Engrave →' && c.seals.length === 1 && c.seals[0] === 'BACK ENGRAVING|Giovanna' && c.lid === B.lids[1], 'the GF middle (piece 2): its own approved card, Giovanna\'s one BACK ENGRAVING seal on the button: ' + JSON.stringify([c.state, c.words, c.seals]));
    await viewOf(B.lids[1], /^Etsy listing · Piece 2 of 3 · MIDDLE_9935$/);
    await page.click('.owTabsV [data-ow-view="sheet"]');
    await page.waitForFunction(() => document.querySelector('#owSheetPanel [data-engraving-panel] .swEng'), null, { timeout: 20000 }).catch(() => {});
    const sg = await page.evaluate(() => ({ state: (document.querySelector('#owSheetPanel [data-engraving-panel] .swEng') || {}).dataset?.state || '', words: (document.querySelector('#owSheetPanel [data-engraving-panel] .words') || {}).textContent || '', seals: document.querySelectorAll('#owSheetPanel [data-engraving-panel] .egButtonSeal .seal').length }));
    check(sg.state === 'approved' && sg.words === 'I\ndissent' && sg.seals === 1, 'the Sheet tab, opened with piece 2 chosen, shows piece 2\'s card (not piece 1\'s or 3\'s): ' + JSON.stringify(sg));
    await page.click('.owTabsV [data-ow-view="info"]'); await settled(); c = await card();
    check(c.key === k.g && c.state === 'approved' && c.words === 'I\ndissent', 'back on the Overview it is still piece 2');
    await pick(k.r); await waitCard('approve'); await picsReady();
    c = await card(); check(c.state === 'approve' && c.words === 'KMB //\nSMH' && c.seals.length === 0 && c.lid === B.lids[2], 'the RG middle (piece 3) is its own again: to approve, no seal');
    await viewOf(B.lids[2], /^Etsy listing · Piece 3 of 3 · MIDDLE_9935$/);
    await pick(null); await page.waitForTimeout(500); c = await card();
    check(c.state === 'approve' && c.words === 'KMB //\nSMH', '"All pieces" keeps the line the Overview holds: the same card');
    await pick(k.r);
    const log = await page.evaluate(() => __log);
    check(log.length > 0 && !log.some(l => l.wrong), 'at no moment did a card show another piece\'s words, or a card on the chain (' + log.length + ' changes seen)');

    // ══ 3 · the red box and the card's shortcut land on the same place ══
    step = '3'; say(step);
    await page.evaluate(() => { window.__realRenderOn = false; });
    // (the Engraving tab of a job in review draws a placement card from a real fit, which this world does not have: its drawing is counted, its place is read)
    await page.evaluate(() => { Engrave.render = function () { __renders++; }; });
    c = await card(); check(c.red === 1, 'the red box "Its engraving is still to be settled" is in the window');
    await page.click('#owFix .owFix button');
    await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 15000 });
    const viaRed = await view(), vr = JSON.parse(viaRed);
    check(vr.tab === 'place' && vr.focus === k.r && vr.chosen === true && vr.list === false && vr.q === B.rid, 'red box → Open it in Engraving: the Engraving tab is on piece 3\'s own placement card, the search box on the order: ' + viaRed);
    check(await page.evaluate(() => document.querySelectorAll('dialog[open]').length) === 0, 'the order window is closed: no pop-up on a pop-up');
    await resetEngrave(); await open(k.r); await waitCard('approve');
    check((await card()).red === 1 && (await card()).state === 'approve' && (await winState()).view === 'info', 'back in the order window (opened again): the Overview, the card and the red box as they were');
    await page.click('#owEng [data-e=engrave]');
    await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 15000 });
    const viaCard = await view();
    check(viaCard === viaRed, 'the card\'s "Adjust in Engrave" lands on exactly the same place as the red box: ' + viaCard);
    await resetEngrave();
    // the Decided tab of the approved piece (piece 2): real drawing of its row, opened
    await page.evaluate(() => { Engrave.render = window.__realRender; });
    await open(k.g); await waitCard('approved');
    await page.click('#owEng [data-e=engrave]');
    await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 15000 });
    await page.waitForFunction(k => { const r = document.querySelector('#engraveView .doneRow.open'); return r && r.dataset.key === k; }, k.g, { timeout: 15000 }).catch(() => {});
    const dec = await page.evaluate(() => ({ v: JSON.parse(JSON.stringify(Engrave.view())), open: [...document.querySelectorAll('#engraveView .doneRow.open')].map(r => r.dataset.key), rows: [...document.querySelectorAll('#engraveView .doneRow')].map(r => r.dataset.key) }));
    check(dec.v.tab === 'done' && dec.v.q === B.rid && dec.open.length === 1 && dec.open[0] === k.g && dec.rows.every(r => r.startsWith(B.rid)), 'the approved piece: View in Engrave opens its Decided row, only this order\'s rows listed: ' + JSON.stringify([dec.v.tab, dec.v.q, dec.open, dec.rows.length]));
    await resetEngrave();

    // ══ 4 · Approve on the card ══
    step = '4'; say(step);
    await open(k.r); await waitCard('approve'); await picsReady();
    await page.evaluate(() => { window.__prompts = 0; window.prompt = () => { __prompts++; return null; }; B.employee = ''; try { localStorage.removeItem('cn.employee'); } catch (_) {} });
    const nameless = await page.evaluate(() => (window.CNEmployee.name() || '') === '');
    let who = 'Test Operator';
    if (nameless) {
      await page.click('#owEng [data-e=approve]'); await page.waitForTimeout(300);
      check((await approvals()).length === 0 && (await page.evaluate(() => __prompts)) === 1 && (await card()).approve, 'with no name known, the name is asked for; a refusal approves nothing and the button stays');
      who = 'Zed Tester'; await page.evaluate(() => { window.prompt = () => 'Zed Tester'; });
    } else console.log('  – the name could not be cleared here: the ask-for-a-name step was not run');
    await page.click('#owEng [data-e=approve]');
    await page.waitForFunction(() => __approve.length === 1, null, { timeout: 8000 });
    const during = await page.evaluate(() => { const b = document.querySelector('#owEng .egApproveButton'); return { disabled: b.disabled, busy: b.getAttribute('aria-busy'), seals: document.querySelectorAll('#owEng .egButtonSeal .seal').length }; });
    check(during.disabled && during.busy === 'true', 'pressed: the button waits (disabled, busy) while the approval runs: ' + JSON.stringify(during));
    await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved]') && !document.querySelector('#owEng .seal.pending') && !Seal.busy(), null, { timeout: 25000 });
    await settled(); c = await card();
    const ap = await approvals();
    check(ap.length === 1 && ap[0] === `${k.r}|${who}|true`, 'Engrave.approve was called once, for piece 3\'s job, with the name and the button that is on screen: ' + JSON.stringify(ap));
    check(c.state === 'approved' && c.disabled && !c.approve && c.seals.length === 1 && c.seals[0] === `BACK ENGRAVING|${who}` && c.open === 'View in Engrave →', 'the card reads approved, with exactly one BACK ENGRAVING seal on its button: ' + JSON.stringify([c.state, c.seals, c.open]));
    check(c.red === 0, 'the red box is gone');
    if (shots) await page.screenshot({ path: path.join(shots, '4-approved-1440.png') });
    await page.evaluate(() => { const b = document.querySelector('#owEng .egApproveButton'); b.disabled = false; b.click(); b.click(); });
    await page.waitForTimeout(500);
    check((await approvals()).length === 1 && (await card()).seals.length === 1 && (await sealCount(k.r)) === 1, 'a second press (forced, twice) adds nothing: one approval, one seal on the card and in the job');
    // the order's timeline (read from the fake record by the page's own feed) shows the approval; so does the Sheet tab
    await page.click('.owTabsV [data-ow-view="timeline"]');
    const tl = await page.waitForFunction(() => document.querySelectorAll('#orderWin [data-tl-face*="engraveApproved"]').length > 0, null, { timeout: 15000 }).then(() => true, () => false);
    check(tl, 'the Timeline tab shows the approval (an Engraved step from the order\'s own record)');
    await page.click('.owTabsV [data-ow-view="sheet"]');
    await page.waitForFunction(() => document.querySelector('#owSheetPanel [data-engraving-panel] .swEng[data-state=approved]'), null, { timeout: 20000 }).catch(() => {});
    const sheetSeals = await page.evaluate(() => ({ state: (document.querySelector('#owSheetPanel [data-engraving-panel] .swEng') || {}).dataset?.state || '', n: document.querySelectorAll('#owSheetPanel [data-engraving-panel] .egButtonSeal .seal svg[data-seal-model]').length, words: (document.querySelector('#owSheetPanel [data-engraving-panel] .words') || {}).textContent || '' }));
    check(sheetSeals.state === 'approved' && sheetSeals.n === 1 && sheetSeals.words === 'KMB //\nSMH', 'the Sheet tab shows the same approval, its own card, one seal: ' + JSON.stringify(sheetSeals));
    await page.click('.owTabsV [data-ow-view="info"]'); await settled();
    c = await card(); check(c.state === 'approved' && c.seals.length === 1, 'back on the Overview: still one seal');
    await page.click('#owEng [data-e=engrave]');
    await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 15000 });
    await page.waitForFunction(k => { const r = document.querySelector('#engraveView .doneRow.open'); return r && r.dataset.key === k; }, k.r, { timeout: 15000 }).catch(() => {});
    const done3 = await page.evaluate(() => ({ tab: Engrave.view().tab, open: [...document.querySelectorAll('#engraveView .doneRow.open')].map(r => r.dataset.key), seals: document.querySelectorAll('#engraveView .doneRow.open .seal').length }));
    check(done3.tab === 'done' && done3.open.length === 1 && done3.open[0] === k.r, 'the Engraving tab shows it as Decided, its row opened: ' + JSON.stringify(done3));
    await resetEngrave();

    // ══ 5 · approved in the Engraving tab or the Sheet tab while the window is open ══
    step = '5'; say(step);
    await open(key(R2, 0)); await waitCard('approve');
    const t0 = Date.now();
    await page.evaluate(k => { const j = Engrave.items().get(k), at = Date.now(); CNEngravingSeals.add(j, 'engraveApproved', 'Sam Tester', at); Object.assign(j, { state: 'approved', approvedBy: 'Sam Tester', approvedAt: at }); Object.assign(j.row.engrave, { state: 'approved', approved: true, approvedBy: 'Sam Tester', approvedAt: at }); }, key(R2, 0));
    const f1 = await waitCard('approved', 4000), took = Date.now() - t0; c = await card();
    check(f1 && took <= 2500 && !c.approve && c.disabled && c.seals.length === 1 && c.seals[0] === 'BACK ENGRAVING|Sam Tester', `approved in the Engraving tab: the open card follows in ${took} ms, one seal, nobody can approve it twice`);
    check(await page.waitForFunction(() => !document.querySelector('#owFix .owFix'), null, { timeout: 4000 }).then(() => true, () => false), 'and the red box goes with it');
    await closeWin(); await open(key(R2, 1)); await waitCard('approve');
    await page.click('.owTabsV [data-ow-view="sheet"]');
    await page.waitForFunction(() => document.querySelector('#owSheetPanel [data-engraving-panel] [data-e=approve]'), null, { timeout: 20000 });
    await page.click('#owSheetPanel [data-engraving-panel] [data-e=approve]');
    await page.waitForFunction(() => document.querySelector('#owSheetPanel [data-engraving-panel] .swEng[data-state=approved]') && !Seal.busy(), null, { timeout: 25000 });
    await page.click('.owTabsV [data-ow-view="info"]'); const t1 = Date.now();
    const f2 = await waitCard('approved', 4000), took2 = Date.now() - t1; c = await card();
    check(f2 && took2 <= 2500 && c.seals.length === 1 && c.words === 'Leaf' && !c.approve && (await approvals()).filter(a => a.startsWith(key(R2, 1))).length === 1, `approved in the Sheet tab: the Overview's card reads approved ${took2} ms after the tab is back, one seal, one approval`);
    await closeWin();

    // ══ 6 · the viewer and the card ══
    step = '6'; say(step);
    await open(k.r); await picsReady();
    const before = await card(), win0 = await winState();
    await page.click('#owPhoto'); await loaded();
    let v = await viewer();
    check(v && /^Etsy listing · Piece 3 of 3/.test(v.cap) && v.nat[0] === 2000, 'click on the listing: the viewer opens on piece 3\'s listing photo at the largest size');
    const fit0 = v.s; await page.mouse.move(720, 450); for (let i = 0; i < 3; i++) await page.mouse.wheel(0, -500); await page.waitForTimeout(100);
    const zoomed = await viewer(); check(zoomed.s > fit0 * 2, 'the wheel zooms the listing photo');
    await page.mouse.move(720, 450); await page.mouse.down(); for (let i = 1; i <= 8; i++) await page.mouse.move(720 - 15 * i, 450 - 9 * i); await page.mouse.up(); await page.waitForTimeout(80);
    const panned = await viewer(); check(Math.abs((panned.tx - zoomed.tx) + 120) < 3 && Math.abs((panned.ty - zoomed.ty) + 72) < 3, 'a drag pans the picture with the pointer: ' + [panned.tx - zoomed.tx, panned.ty - zoomed.ty]);
    await page.keyboard.press('0'); await page.keyboard.press('ArrowRight'); await loaded(); v = await viewer();
    check(/^Vector design/.test(v.cap) && Math.max(...v.nat) === 1600, 'arrow: its Vector design, drawn large');
    await page.mouse.move(720, 450); await page.mouse.wheel(0, -500); await page.waitForTimeout(100);
    check((await viewer()).s > v.s * 1.5, 'the wheel zooms the vector too');
    c = await card(); check(c.state === before.state && c.words === before.words && c.seals.length === before.seals.length, 'the card under the viewer is untouched while it is open');
    await page.keyboard.press('Escape'); check(await closedViewer(), 'Esc closes the viewer only');
    let after = await winState(); check(after.open && after.key === k.r && after.view === 'info' && after.active === 'owPhotoBox', 'the window is as it was and the focus is back on the thumbnail that opened it: ' + JSON.stringify(after));
    c = await card(); check(c.state === before.state && c.words === before.words && c.seals.length === before.seals.length && c.under && !c.wait, 'the card is intact after the viewer closed');
    await page.click('#owVector'); await loaded(); v = await viewer(); check(/^Vector design/.test(v.cap), 'a click on the Vector design opens it');
    await page.keyboard.press('Escape'); await closedViewer(); check((await winState()).active === 'owVectorBox', 'Esc gives the focus back to the Vector design');
    await closeWin();
    // the viewer opened while an approval is being prepared (R4 piece 1; the approval is made slower so the two meet), the seal then pressed under it
    await open(key(R4, 0)); await waitCard('approve'); await picsReady();
    await page.evaluate(() => { window.__hold(); window.__n6 = __approve.length; });
    await page.click('#owEng [data-e=approve]');
    await page.waitForFunction(() => __approve.length > __n6, null, { timeout: 8000 });
    await page.click('#owPhoto'); await loaded();
    v = await viewer(); check(v && /^Etsy listing · Piece 1 of 3 · ALPHA_TAG$/.test(v.cap), 'the viewer opens while the approval runs, on its own piece');
    await page.evaluate(() => __release());
    await page.waitForFunction(() => Seal.busy(), null, { timeout: 8000 });
    // Esc while the seal is being pressed under it: the viewer goes at once, as it is asked to (the press under it goes on)
    await page.keyboard.press('Escape');
    const gone = await closedViewer(700);
    check(gone, 'Esc while the seal is being pressed closes the viewer at once (it stayed over the window, blank, until the seal had landed)');
    await page.waitForFunction(() => !Seal.busy() && !!document.querySelector('#owEng .swEng[data-state=approved]'), null, { timeout: 25000 });
    c = await card(); check(c.state === 'approved' && c.seals.length === 1 && c.words === 'Alpha', 'the approval landed under it once: approved, one seal, its words');
    check(await closedViewer(3000) && (await winState()).open && (await winState()).active === 'owPhotoBox', 'the viewer is closed, the window open, the focus on the thumbnail');
    // a click made while the seal is pressed is not taken by anything (the page waits for the wooden press): it opens nothing, and the next one does
    await pick(key(R4, 1)); await waitCard('approve'); await picsReady();
    await page.click('#owEng [data-e=approve]');
    await page.waitForFunction(() => Seal.busy(), null, { timeout: 8000 });
    await page.click('#owPhoto', { noWaitAfter: true }); await page.waitForTimeout(200);
    check(!(await page.evaluate(() => PhotoView.isOpen())), 'a click on the picture during the press opens nothing (the page is still while the seal is pressed)');
    await page.waitForFunction(() => !Seal.busy() && !!document.querySelector('#owEng .swEng[data-state=approved]'), null, { timeout: 25000 });
    c = await card(); check(c.state === 'approved' && c.seals.length === 1 && c.words === 'Bravo', 'the approval landed once: ' + JSON.stringify([c.state, c.seals.length, c.words]));
    await page.click('#owPhoto'); await loaded(); v = await viewer(); check(v && /Piece 2 of 3 · BRAVO_TAG/.test(v.cap), 'afterwards the same click opens the viewer');
    await page.keyboard.press('Escape'); await closedViewer();
    await page.evaluate(() => { window.__delay = 250; });
    await closeWin();

    // the window closed from under an open viewer (EngraveLink, the Sheet window and a hand-over all close it by code): the viewer goes with it, and the next opening is clean
    await open(key(R5, 0)); await picsReady();
    await page.click('#owPhoto'); await loaded();
    await page.evaluate(() => OrderWin.close());
    await page.waitForFunction(() => !OrderWin.isOpen() && !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {});
    const orphanGone = await closedViewer(3000);   // (the window's close event, which the viewer waits for, comes a moment after the window is closed)
    const orphan = await page.evaluate(() => ({ viewers: document.querySelectorAll('dialog.phv[open]').length, win: document.getElementById('orderWin').open, over: PhotoView.isOpen() }));
    check(orphanGone && orphan.viewers === 0 && !orphan.win && !orphan.over, 'the order window closed by code while the viewer was open: the viewer is closed with it: ' + JSON.stringify(orphan));
    await open(key(R5, 0)); await picsReady(); await page.click('#owVector'); await loaded(); v = await viewer();
    check(v && v.layer && v.inWin && /Vector design/.test(v.cap), 'the next opening of the window and of the viewer is clean (a layer of the window again)');
    await page.keyboard.press('Escape'); await closedViewer(); await closeWin();

    // ══ 7 · the other doors ══
    step = '7'; say(step);
    const expectPiece = async (label, want) => {
      await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, want, { timeout: 15000 }).catch(() => {});
      await settled(); await picsReady().catch(() => {});
      const exp = EXPECT(want); await (exp ? waitCard() : waitNoCard(4000));
      const cc = await card(); const idx = ALL.flatMap(o => o.skus.map((_, i) => key(o, i))).indexOf(want);
      check(cc.key === want, `${label}: the window is on the piece named`);
      check(exp ? cc.words === exp && cc.state !== '' : cc.hidden, `${label}: its card is that piece's own (${exp ? JSON.stringify(cc.words) : 'no card'})`);
      await page.click('#owPhoto'); await loaded(); const vv = await viewer(); const piece = (want.endsWith(tid(B, 0)) ? 1 : want.endsWith(tid(B, 1)) ? 2 : want.endsWith(tid(B, 2)) ? 3 : 0);
      check(vv && (!piece || new RegExp('Piece ' + piece + ' of 3').test(vv.cap)), `${label}: the viewer's caption names the same piece: "${vv && vv.cap}"`);
      await page.keyboard.press('Escape'); await closedViewer();
    };
    // openOrderFrom from a pop-up with only a pool id (the charm inspector, the Moving bar, the Library Issues rows)
    await page.evaluate(() => { const d = document.createElement('dialog'); d.id = '__src'; d.innerHTML = '<div style="padding:30px"><button id="__go" type="button">Open order</button></div>'; document.body.appendChild(d); d.showModal(); });
    await page.evaluate(({ rid, pid }) => { document.getElementById('__go').onclick = e => openOrderFrom(e.currentTarget, rid, { poolId: pid }); }, { rid: B.rid, pid: pool(B, 1) });
    await page.click('#__go'); await expectPiece('openOrderFrom({poolId}) of piece 2', k.g);
    await closeWin();
    check(await page.evaluate(() => document.getElementById('__src').open), 'closing the order window comes back to the pop-up it grew from');
    await page.evaluate(() => { const d = document.getElementById('__src'); d.close(); d.remove(); });
    await page.evaluate(({ rid, pid }) => OrderWin.openOrder(rid, { poolId: pid }), { rid: B.rid, pid: pool(B, 2) });
    await expectPiece('OrderWin.openOrder(rid, {poolId}) as a Library Issues piece row does, piece 3', k.r);
    await closeWin();
    await page.evaluate(({ rid, pid }) => OrderWin.openOrder(rid, { poolId: pid }), { rid: B.rid, pid: pool(B, 0) });
    await expectPiece('the same for piece 1 (the chain)', k.c);
    await closeWin();
    // the Sheet window of the RG sheet, its piece view of the RG middle, Open order
    await page.evaluate(({ sh, ps }) => { SheetWin.open(sh, { select: ps }); }, { sh: RG, ps: pool(B, 2) });
    await page.waitForFunction(() => SheetWin.isOpen && SheetWin.isOpen() && document.querySelector('[data-r2=openOrd]'), null, { timeout: 20000 });
    const sw = await page.evaluate(() => ({ state: (document.querySelector('[data-r2=eng] .swEng') || {}).dataset?.state || '', words: (document.querySelector('[data-r2=eng] .words') || {}).textContent || '', seals: document.querySelectorAll('[data-r2=eng] .egButtonSeal .seal').length }));
    check(sw.state === 'approved' && sw.words === 'KMB //\nSMH' && sw.seals === 1, 'the Sheet window shows the same piece approved, one seal (approved on the card earlier): ' + JSON.stringify(sw));
    await page.click('[data-r2=openOrd]'); await expectPiece('Sheet window → Open order', k.r);
    c = await card(); check(c.state === 'approved' && c.seals.length === 1, 'the Overview agrees with the Sheet window: approved, one seal');
    await closeWin(); await page.evaluate(() => { try { SheetWin.close && SheetWin.close(); } catch (_) {} const d = document.querySelector('dialog.sheetWin[open]'); if (d) d.close(); }); await page.waitForTimeout(300);
    // search
    await page.keyboard.press('/');
    await page.waitForFunction(() => OrderSearch.isOpen() && document.activeElement && document.activeElement.id === 'cnsQ', null, { timeout: 8000 });
    await page.keyboard.type(B.rid);
    await page.waitForSelector(`#cnsList .cnsCard[data-rid="${B.rid}"]`, { timeout: 10000 });
    await page.keyboard.press('Enter');
    await page.waitForFunction(rid => OrderWin.isOpen() && OrderWin.rid() === rid, B.rid, { timeout: 15000 });
    await settled(); await picsReady().catch(() => {});
    const sk = await page.evaluate(() => OrderWin.key());
    await expectPiece('search result', sk);
    await closeWin();
    // an order the page has not read yet (outside the pull): a small labelled wait on the card, nothing opens from its pictures, nothing breaks
    await page.evaluate(() => { OrderWin.openOrder('4179990001', { q: '4179990001', row: null }); });
    await page.waitForFunction(() => OrderWin.isOpen() && OrderWin.rid() === '4179990001', null, { timeout: 10000 });
    const early = await page.evaluate(() => { const h = document.getElementById('owEng'); return { hidden: h.hidden, wait: (h.querySelector('.owEngWait') || {}).textContent || '', open: !!document.querySelector('dialog.phv[open]') }; });
    await page.evaluate(() => { document.getElementById('owPhotoBox').click(); document.getElementById('owVectorBox').click(); });
    await page.waitForTimeout(300);
    check(/Reading the back engraving/.test(early.wait) || early.hidden, 'an order not read yet: the card says what it waits for (or is not there), not another order\'s card: ' + JSON.stringify(early));
    check(!(await page.evaluate(() => PhotoView.isOpen())) || (await closedViewer(1)) === false, 'its pictures open nothing while there is nothing to show');
    await page.evaluate(() => PhotoView.close());
    await page.waitForFunction(() => /no piece in the sorter's records|could not be read/.test(document.getElementById('owNotes').textContent) && document.getElementById('owLoading').hidden, null, { timeout: 25000 }).catch(() => {});
    const lost = await page.evaluate(() => { const h = document.getElementById('owEng'); return { hidden: h.hidden, wait: (h.querySelector('.owEngWait') || {}).textContent || '', card: !!h.querySelector('.swEng') }; });
    check(lost.hidden && !lost.wait && !lost.card, 'an order the records do not have: once that is known the card\'s wait is gone and no card is shown: ' + JSON.stringify(lost));
    await closeWin();
    // the same piece on the Sheet window and in the order window: approved in the order window, the Sheet window under it follows
    await page.evaluate(({ sh, ps }) => { SheetWin.open(sh, { select: ps }); }, { sh: GF, ps: pool(R6, 0) });
    await page.waitForFunction(() => SheetWin.isOpen && SheetWin.isOpen() && document.querySelector('[data-r2=eng] .swEng[data-state=approve]'), null, { timeout: 25000 });
    await page.click('[data-r2=openOrd]'); await expectPiece('Sheet window of a piece to approve → Open order', key(R6, 0));
    await page.click('#owEng [data-e=approve]');
    await page.waitForFunction(() => !Seal.busy() && document.querySelector('#owEng .swEng[data-state=approved]'), null, { timeout: 25000 });
    await closeWin();
    await page.waitForFunction(() => SheetWin.isOpen && SheetWin.isOpen(), null, { timeout: 8000 }).catch(() => {});
    const back = await page.waitForFunction(() => { const e = document.querySelector('[data-r2=eng] .swEng'); return e && e.dataset.state === 'approved' && e.querySelectorAll('.egButtonSeal .seal').length === 1; }, null, { timeout: 6000 }).then(() => true, () => false);
    check(back, 'approved in the order window: the Sheet window that handed over reads approved with one seal when it is back');
    await page.evaluate(() => { try { SheetWin.close && SheetWin.close(); } catch (_) {} const d = document.querySelector('dialog.sheetWin[open]'); if (d) d.close(); }); await page.waitForTimeout(300);
    // Sheet window → Open order → the card's View in Engrave: both windows go, the Engraving tab is on that piece's Decided row
    await page.evaluate(({ sh, ps }) => { SheetWin.open(sh, { select: ps }); }, { sh: GF, ps: pool(R6, 0) });
    await page.waitForFunction(() => SheetWin.isOpen && SheetWin.isOpen() && document.querySelector('[data-r2=openOrd]'), null, { timeout: 25000 });
    await page.click('[data-r2=openOrd]'); await expectPiece('Sheet window → Open order (an approved piece)', key(R6, 0));
    await page.click('#owEng [data-e=engrave]');
    await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 15000 });
    await page.waitForFunction(k => { const r = document.querySelector('#engraveView .doneRow.open'); return r && r.dataset.key === k; }, key(R6, 0), { timeout: 15000 }).catch(() => {});
    const e3 = await page.evaluate(() => ({ dialogs: document.querySelectorAll('dialog[open]').length, sheetWin: SheetWin.isOpen(), orderWin: OrderWin.isOpen(), tab: Engrave.view().tab, open: [...document.querySelectorAll('#engraveView .doneRow.open')].map(r => r.dataset.key) }));
    check(e3.dialogs === 0 && !e3.sheetWin && !e3.orderWin && e3.tab === 'done' && e3.open.length === 1 && e3.open[0] === key(R6, 0), 'Sheet window → Open order → View in Engrave: no window is left over the Engraving tab, which has that piece\'s Decided row open: ' + JSON.stringify(e3));
    await resetEngrave();

    // ══ 8 · narrow widths ══
    step = '8'; say(step);
    for (const [w, h] of [[900, 900], [390, 844]]) {
      await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(300);
      for (const [pk, label] of [[k.r, 'RG middle in review'], [k.g, 'GF middle approved']]) {
        await open(pk); await waitCard(); await picsReady().catch(() => {}); await page.waitForTimeout(400);
        const g = await page.evaluate(() => {
          const r = id => document.getElementById(id).getBoundingClientRect(), box = (a, b) => !(a.right <= b.left + .5 || b.right <= a.left + .5 || a.bottom <= b.top + .5 || b.bottom <= a.top + .5);
          const eng = r('owEng'), ph = r('owPhoto'), vec = r('owVector'), sku = r('owSku'), main = document.querySelector('.owVInfo'), mn = document.querySelector('.owVInfo .owMain').getBoundingClientRect(), seal = document.querySelector('#owEng .egButtonSeal .seal'), sr = seal ? seal.getBoundingClientRect() : null, e = document.getElementById('owEng');
          const over = n => n.scrollWidth - n.clientWidth, win = document.getElementById('orderWin');
          const withCard = over(win); e.style.display = 'none'; const without = over(win); e.style.display = '';
          return { overlap: box(eng, ph) || box(eng, vec) || box(eng, sku), engW: Math.round(eng.width), picW: Math.round(ph.width), inColumn: eng.left >= mn.left - .5 && eng.right <= mn.right + .5, sealIn: !sr || (sr.left >= eng.left - 1 && sr.right <= eng.right + 1),
            regions: [main, document.querySelector('.owVInfo .owMain'), e].map(over), cardAdds: withCard - without, windowOver: withCard, thumbsOk: ph.width > 0 && vec.width > 0 };
        });
        check(!g.overlap && g.inColumn && g.regions.every(x => x <= 1) && g.cardAdds <= 0 && g.sealIn && g.thumbsOk, `${w} px, ${label}: the card does not overlap the thumbnails or the SKU, stays in the Overview's column, adds no sideways scroll, the seal lies inside it: ` + JSON.stringify(g));
        if (g.windowOver > 1) console.log(`    note: the window itself scrolls sideways by ${g.windowOver} px at ${w} px with or without the card (its tab row's tools: the live line and the Skip switch), as it did before round 3 at 390 px`);
        await page.evaluate(() => document.getElementById('owVector').scrollIntoView({ block: 'center' })); await page.waitForFunction(() => document.querySelector('#owVector img, #owVector canvas'), null, { timeout: 10000 }).catch(() => {});
        const th2 = await thumbs(); check(th2.every(t => !t.none && t.inside && t.still), `${w} px: the thumbnails are still whole: ` + JSON.stringify(th2));
        if (shots) await page.screenshot({ path: path.join(shots, `8-${w}-${pk === k.r ? 'review' : 'approved'}.png`) });
        await closeWin();
      }
    }
    await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(300);

    // ══ 9 · twenty piece switches around an approval (before, during and after its seal is pressed) ══
    step = '9'; say(step);
    await open(key(R4, 2)); await waitCard('approve'); await picsReady();
    const n0 = (await approvals()).length;
    await page.evaluate(({ WORDS, LID_KEY }) => {
      const h = document.getElementById('owEng'); window.__log9 = []; window.__max = 0; window.__hold();
      new MutationObserver(() => { const lid = document.getElementById('owPhoto').dataset.lid || '', want = LID_KEY[lid] || '', w = (h.querySelector('.words') || {}).textContent || '', n = h.querySelectorAll('.egButtonSeal .seal').length; __max = Math.max(__max, n); __log9.push({ w, want, wrong: !!w && w !== (WORDS[want] || '#') }); }).observe(h, { childList: true, subtree: true });
    }, { WORDS, LID_KEY });
    await page.click('#owEng [data-e=approve]');
    await page.waitForFunction(n => __approve.length > n, n0, { timeout: 8000 });
    const keys9 = [key(R4, 0), key(R4, 1), key(R4, 2)];
    const sw0 = Date.now(); let during9 = 0;
    for (let i = 0; i < 20; i++) { await pick(i % 4 === 3 ? null : keys9[i % 3]); if (await page.evaluate(() => Seal.busy())) during9++; }
    say('20 switches took ' + (Date.now() - sw0) + ' ms, with the approval held at its preparing step (' + during9 + ' while a seal was pressed)');
    await page.evaluate(() => __release());
    for (let i = 0; i < 6; i++) await pick(keys9[i % 3]);   // (switches while the seal is pressed: a click made then is not taken, the page waits for the wooden press)
    await page.waitForFunction(() => !Seal.busy() && ![...Engrave.items().values()].some(j => j.approvalPreparing || j.stamping), null, { timeout: 30000 });
    await pick(key(R4, 0)); await page.waitForTimeout(1600); await settled();
    let fin = await card();
    check((await approvals()).length === n0 + 1, 'twenty switches around one approval: exactly one approval ran (' + ((await approvals()).length - n0) + ')');
    check((await sealCount(key(R4, 2))) === 1 && (await sealCount(key(R4, 0))) === 1 && (await sealCount(key(R4, 1))) === 1, 'each of the three pieces has exactly one seal in its job');
    let l9 = await page.evaluate(() => ({ n: __log9.length, wrong: __log9.filter(l => l.wrong).length, max: __max }));
    check(l9.wrong === 0 && l9.max <= 1, `no stale card (${l9.n} changes seen, ${l9.wrong} showed another piece's words) and never more than one seal on a card (${l9.max})`);
    check(fin.key === key(R4, 0) && fin.state === 'approved' && fin.words === 'Alpha' && fin.seals.length === 1 && fin.lid === R4.lids[0], 'the card at the end is the piece the window is on (Alpha), approved, one seal: ' + JSON.stringify([fin.key.slice(-1), fin.state, fin.words, fin.seals]));
    await pick(key(R4, 2)); await waitCard('approved'); c = await card();
    check(c.words === 'Charlie' && c.seals.length === 1 && c.seals[0].startsWith('BACK ENGRAVING|') && !c.approve, 'the piece that was approved while the person looked at others shows its seal, once, and no Approve left: ' + JSON.stringify([c.words, c.seals, c.approve]));
    // twenty quick switches with nothing pressing: the last one wins and nothing of the others stays
    await page.evaluate(() => { window.__log9.length = 0; });
    for (let i = 0; i < 20; i++) await pick(i % 4 === 3 ? null : keys9[(i * 2) % 3]);
    await pick(key(R4, 1)); await page.waitForTimeout(1500); await settled(); fin = await card();
    l9 = await page.evaluate(() => ({ n: __log9.length, wrong: __log9.filter(l => l.wrong).length, max: __max }));
    check(fin.key === key(R4, 1) && fin.words === 'Bravo' && fin.lid === R4.lids[1] && fin.seals.length === 1 && l9.wrong === 0 && l9.max <= 1, `twenty quick switches: the last piece's card (Bravo, one seal), no stale words (${l9.n} changes, ${l9.wrong} wrong)`);
    await closeWin();

    // ══ 9b · leftovers around an approval: the window closed, a tab left, an approval another computer made, a cancelled order, Previous and Next ══
    step = '9b'; say(step);
    const settledJob = k => page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'approved' && !Seal.busy() && !j.approvalPreparing && !j.stamping; }, k, { timeout: 40000 });
    // the window is closed the moment Approve is pressed; the approval goes on without it
    await open(key(R7, 0)); await waitCard('approve');
    await page.evaluate(() => { window.__hold(); window.__n11 = __approve.length; });
    await page.click('#owEng [data-e=approve]');
    await page.waitForFunction(() => __approve.length > __n11, null, { timeout: 8000 });
    await closeWin(); await page.evaluate(() => __release()); await settledJob(key(R7, 0));
    check((await sealCount(key(R7, 0))) === 1 && (await approvals()).filter(a => a.startsWith(key(R7, 0))).length === 1, 'the window closed while the approval was preparing: it went through, one seal, one approval');
    await open(key(R7, 0)); await waitCard('approved'); c = await card();
    check(c.state === 'approved' && c.seals.length === 1 && c.red === 0 && c.words === 'Eta', 'opened again: the card reads approved with its one seal, no red box');
    // the person leaves for the Timeline tab (the card is out of sight) while the seal is pressed
    await pick(key(R7, 1)); await waitCard('approve');
    await page.evaluate(() => { window.__hold(); window.__n11 = __approve.length; });
    await page.click('#owEng [data-e=approve]');
    await page.waitForFunction(() => __approve.length > __n11, null, { timeout: 8000 });
    await page.click('.owTabsV [data-ow-view="timeline"]'); await page.evaluate(() => __release()); await settledJob(key(R7, 1));
    await page.click('.owTabsV [data-ow-view="info"]'); await settled(); await waitCard('approved'); c = await card();
    check(c.state === 'approved' && c.seals.length === 1 && c.disabled && (await page.evaluate(() => document.querySelectorAll('.seal.pending').length)) === 0, 'the Overview left during the press: back on it the card is approved, one seal, nothing left "pending": ' + JSON.stringify([c.state, c.seals]));
    // an approval another computer made: only the order's timeline knows it
    await pick(key(R7, 2)); await waitCard('approve');
    await page.evaluate(async ({ rid, k }) => { const at = Date.now(), pid = k + '_1'; await fetch('/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ op: 'timelineAdd', events: [{ orderId: rid, type: 'engraveApproved', id: pid + '-' + at, at, by: 'Giovanna', station: 'sorter', device: 'other-computer', lineKey: k, transactionId: k.split('_')[1], text: '“Iota”', data: { text: 'Iota', poolId: pid, copy: 1 } }] }) }); OrderWin._feed().refresh({ force: true }); }, { rid: R7.rid, k: key(R7, 2) });
    const fa = await waitCard('approved', 8000); c = await card();
    check(fa && c.seals.length === 1 && c.seals[0] === 'BACK ENGRAVING|Giovanna' && !c.approve, 'approved on another computer: the card shows it from the timeline, with that person\'s seal: ' + JSON.stringify([c.state, c.seals]));
    await page.waitForTimeout(800); c = await card();
    check(c.red === 0, 'and the red "still to be settled" box does not stay up beside a card that says approved (red boxes: ' + c.red + ')');
    // Previous / Next walk the Orders list: the card and the pictures are those of the order reached, nothing of the one left stays
    await page.evaluate(({ WORDS, LID_KEY }) => {
      const h = document.getElementById('owEng'); window.__log9b = [];
      new MutationObserver(() => { const lid = document.getElementById('owPhoto').dataset.lid || '', want = LID_KEY[lid] || '', w = (h.querySelector('.words') || {}).textContent || ''; __log9b.push({ wrong: !!w && w !== (WORDS[want] || '#') }); }).observe(h, { childList: true, subtree: true });
    }, { WORDS, LID_KEY });
    const walked = [];
    for (const [b, n] of [['owNext', 4], ['owPrev', 7], ['owNext', 3]]) for (let i = 0; i < n; i++) { await page.evaluate(b => document.getElementById(b).click(), b); await page.waitForTimeout(250); walked.push(await page.evaluate(() => OrderWin.key())); }
    await page.waitForTimeout(1200); await settled(); c = await card();
    const lw = await page.evaluate(() => __log9b.filter(l => l.wrong).length);
    check(new Set(walked).size > 3 && lw === 0 && c.key === walked[walked.length - 1] && (c.hidden || c.words === (WORDS[c.key] || '#')) && c.lid === (ALL.flatMap(o => o.lids.map((l, i) => [l, key(o, i)])).find(([, kk]) => kk === c.key) || [])[0], `Previous/Next over ${new Set(walked).size} different pieces: the card and the picture are always the ones of the piece reached (${lw} wrong moments)`);
    await closeWin();
    // a cancelled order (its line left the pull): what the card says, and what its buttons do, is said plainly and nothing is approved
    await page.evaluate(k => { const r = B.orders.byKey.get(k); r.state = 'gone'; r.reason = 'cancelled'; }, key(R8, 0));
    await page.evaluate(k => OrderWin.openOrder(k.split('_')[0], { poolId: k + '_1' }), key(R8, 0));
    await page.waitForFunction(() => OrderWin.isOpen(), null, { timeout: 10000 }); await settled(); await page.waitForTimeout(800);
    c = await card();
    const n8 = (await approvals()).length;
    const gone8 = { state: c.state, approve: c.approve, open: c.open, red: c.red };
    if (c.approve) { await page.click('#owEng [data-e=approve]'); await page.waitForTimeout(600); }
    check((await approvals()).length === n8 && !(await page.evaluate(() => document.querySelector('#owEng .seal.pending'))) && (!gone8.approve || (await page.evaluate(() => __toasts.some(t => /left the pull/.test(t))))), 'a cancelled order: pressing Approve on its card approves nothing, leaves no seal pending, and says why: ' + JSON.stringify(gone8));
    say('cancelled order card: ' + JSON.stringify(gone8));
    await closeWin();

    // ══ 10 · Sandbox against production, and a module that is not there ══
    step = '10'; say(step);
    await open(k.r); await waitCard('approved');
    const sbo = await page.evaluate(() => EngraveLink.open({ rid: '4170837249', key: '4170837249_41708372493', sandbox: true }));
    check(sbo === false && (await winState()).open && (await page.evaluate(() => CN.S.mode)) === 'orders', 'EngraveLink with a Sandbox flag on the production page opens nothing: the window stays, the tab is not moved');
    await closeWin();
    // and the Sandbox page given a production order: the same, the other way round (a page of its own, in Sandbox mode)
    const sbCtx = await browser.newContext({ viewport: { width: 1200, height: 800 } });
    try {
      await sbCtx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      await sbCtx.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
      await sbCtx.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'on', sandboxStream: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; });
      const sp = await sbCtx.newPage(); sp.setDefaultTimeout(30000);
      sp.on('pageerror', e => { errors.push('sandbox page: ' + e.message); });
      await sp.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await sp.waitForFunction(() => window.CN && window.Engrave && window.EngraveLink && window.OrderEngraving && CN.S.cloud.ok === true && CN.S.settings.sandbox === 'on', null, { timeout: 90000 });
      const mode0 = await sp.evaluate(() => CN.S.mode);
      const refused = await sp.evaluate(() => EngraveLink.open({ rid: '4170837249', key: '4170837249_41708372493', sandbox: false }));
      const kept = await sp.evaluate(() => ({ mode: CN.S.mode, q: Engrave.view().q, toasts: [...document.querySelectorAll('#toasts .toast .m')].map(n => n.textContent) }));
      check(refused === false && kept.mode === mode0 && !kept.q && kept.toasts.some(t => /production/.test(t) && /Sandbox mode/.test(t)), 'the Sandbox page given a production order: nothing opens, nothing moves, the toast says why');
      check(await sp.evaluate(() => !document.getElementById('owEng').firstChild && document.getElementById('owEng').hidden), 'and no production order\'s card is on the Sandbox page');
    } finally { await sbCtx.close(); }
    // each module missing in turn: the window is still the window (the failing script is simulated by taking the object away)
    await open(k.r);
    await page.evaluate(() => { window.__EL = window.EngraveLink; delete window.EngraveLink; });
    await page.click('#owEng [data-e=engrave]');
    await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 15000 });
    const noLink = await view();
    check(JSON.parse(noLink).tab === 'done', 'EngraveLink missing: the card\'s shortcut still takes the person to the Engraving tab (the approved piece\'s Decided tab): ' + noLink);
    await resetEngrave(); await page.evaluate(() => { window.EngraveLink = window.__EL; });
    await open(k.r); await waitCard('approved'); await picsReady();
    await page.evaluate(() => { window.__opened = []; window.__PV = window.PhotoView; delete window.PhotoView; window.__wo = window.open; window.open = u => { __opened.push(String(u)); return null; }; });
    await page.click('#owPhoto'); await page.waitForTimeout(300);
    check((await page.evaluate(() => __opened.length)) === 1 && (await winState()).open && (await card()).state === 'approved', 'PhotoView missing: the listing photo opens in a tab of its own, the window and its card are untouched');
    await page.evaluate(() => { window.open = window.__wo; window.PhotoView = window.__PV; });
    await closeWin();
    check(errors.length === 0, 'no page errors or console errors in the whole run: ' + errors.join(' | '));
  } finally { clearTimeout(guard); await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('order-window-round3-e2e: ok (' + ((Date.now() - T0) / 1000).toFixed(0) + ' s)');
}
main().catch(e => { console.error(e); process.exit(1); });
