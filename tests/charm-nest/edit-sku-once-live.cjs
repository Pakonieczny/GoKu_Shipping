// ONESKU (Paul, 27 Oct 2026): the Change SKU window's toggle "Just this order": "a toggle ... that allows a user to choose an alternate SKU and charm matchup from the repository without
// actually updating anything permanently in the app or on etsy ... only used this one time and all subsequent times a user purchases that given listing it will revert back to the original SKU ...
// This toggle must be off by default. If the user decides to turn it on must be explained to the user in very short and clear terms as to the effect of this toggle."
// The page in headless Chromium over the repo's fake site (bridge-server.cjs), the fixture of edit-sku-live.cjs (order A nested on the draft GF Sheet 1, D of the same listing cut on GF Sheet 2):
//   1  the toggle: a switch, OFF whenever the window opens, nothing more said; ON: one short line (this order only; the listing, Etsy and future orders keep the SKU), the chips and the button say so;
//      it turns off again from its label and on from the keyboard
//   2  saved ON: A is cut as MOON_3288 from the person's choice ("once"), its piece swapped in its spot; NO Etsy write, NO saved link or alias, D untouched, a new order of the listing reads the old SKU;
//      the run's line record keeps the choice and a record read back has it; the window closes, the order window shows the new charm
//   SHOT=<path prefix> keeps pictures of the window (off, on).   node tests/charm-nest/edit-sku-once-live.cjs   (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { (process.env.SKIP_BROWSER === '1' ? console.log : m => { console.error('FAIL: ' + m + ' (no browser here: set PW_DIR and CHROMIUM, or SKIP_BROWSER=1 to skip it on purpose)'); process.exit(1); })('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { (process.env.SKIP_BROWSER === '1' ? console.log : m => { console.error('FAIL: ' + m + ' (no browser here: set PW_DIR and CHROMIUM, or SKIP_BROWSER=1 to skip it on purpose)'); process.exit(1); })('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-wrongcharm2-1';
const LID = '1819191541', OLD = 'TINY MOON & STARS 2', NEW = 'MOON_3288';
const SKUS = [OLD, NEW, 'GFFILL_1', 'GFFILL_3'];
const SHIP = Math.floor(Date.UTC(2026, 9, 24, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 9, 16, 12) / 1000);
const A = '4193172538', D = '4193172541', E = '4198728295', ELID = '1700000299', ESKU = 'AFRICA4-LION_CHARM', F = n => '41910000' + String(n).padStart(2, '0');
const TID = tx => String(5100000000 + tx), pid = (rid, tx) => `${rid}_${TID(tx)}_1`;
// [rid, tx, listing, sku, age (s), sheet page, cell]
const LINES = [[A, 1, LID, OLD, 9000, 1, 0], [F(1), 4, '1700000201', 'GFFILL_1', 6900, 1, 1], [D, 6, LID, OLD, 9500, 2, 0], [F(3), 7, '1700000203', 'GFFILL_3', 9400, 2, 1]];
const PAGES = { 1: { id: 'wn-gold-1', draft: true }, 2: { id: 'wn-gold-2', cut: true } };
const idA = pid(A, 1), idD = pid(D, 6);
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const srv = await start({ receipts: [] }), st = srv.st;
  for (const sku of SKUS) st.put('Charm_Master_Index', sku, { sku, aiPath: 'test/' + sku.replace(/\W+/g, '_') + '.ai', widthPt: 28.35, heightPt: 28.35, holes: 0 }, false);
  for (const [pg, sh] of Object.entries(PAGES)) {
    const mine = LINES.filter(l => l[5] === +pg);
    st.put(SHEETS, sh.id, Object.assign({ id: sh.id, metal: 'gold', charms: mine.map((l, i) => ({ id: `${sh.id}-c${i}`, poolId: pid(l[0], l[1]), order: l[0], sku: l[3] })), placements: mine.map((l, i) => ({ id: `${sh.id}-c${i}`, cxPt: 16 + l[6] * 30, cyPt: 16, angle: 0 })), poolIds: mine.map(l => pid(l[0], l[1])), sheetIndex: +pg, fileBase: 'GF Sheet ' + pg, runId: RUN }, sh.cut ? { laserDoneAt: Date.now() - 3600000 } : {}, sh.draft ? { draft: true } : {}), false);
    for (const l of mine) st.put(POOL, pid(l[0], l[1]), { poolId: pid(l[0], l[1]), orderId: l[0], transactionId: TID(l[1]), lineKey: `${l[0]}_${TID(l[1])}`, sku: l[3], copy: 1, quantity: 1, sheetId: sh.id, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 }, false);
  }
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  const errors = [];
  async function openPage(who) {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
    await ctx.route(u => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
      if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: "window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title>'], { type: 'text/html' })); } }; } };" });
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await ctx.addInitScript(who => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', who); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => who; window.confirm = () => true; window.alert = () => {};
      // the nest: placed pieces stay; waiting pieces go into the next free cell, oldest order first; the rest moves on (the page's own overflowToNextSheet); the sheet is saved
      const iv = setInterval(() => {
        if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
        const P = window.CharmNestPDF; if (P) { P.cutLinesOf = () => []; }
        const stub = sh => {
          window.__nests = (window.__nests || []).concat([{ sheetId: sh.sheetId, page: sh.page, charms: sh.charms.map(c => c.poolId) }]);
          sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.stage = 'test nest';
          setTimeout(async () => {
            try {
              sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
              const cells = []; for (let row = 0; row < 2; row++) for (let col = 0; col < 6; col++) cells.push([16 + col * 30, 16 + row * 30]);
              const free = cells.filter(([x, y]) => !sh.placements.some(p => Math.hypot(p.cxPt - x, p.cyPt - y) < 24));
              const rank = c => (window.CharmNestOrders && CharmNestOrders.rankDate ? CharmNestOrders.rankDate(c) : +c.orderDate || 0);
              const waiting = sh.charms.filter(c => !sh.placements.some(p => p.id === c.id)).sort((a, b) => rank(a) - rank(b));
              for (const c of waiting) { if (!free.length) break; const [x, y] = free.shift(); sh.placements.push({ id: c.id, cxPt: x, cyPt: y, angle: 0, wPt: c.widthPt, hPt: c.heightPt }); }
              const left = sh.charms.filter(c => !sh.placements.some(p => p.id === c.id));
              if (sh.sheetId) await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: sh.draft || undefined, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: (c.orderInfo && c.orderInfo.sku) || c.sku })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true });
              sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.stage = ''; sh.persistedDone = true; sh.verification = { ok: true };
              if (left.length) { sh.rejects = left.map(c => c.id); overflowToNextSheet(sh, false); }
            } catch (e) { sh.problem = e.message; sh.status = 'complete'; sh.persistedDone = true; }
            try { CN.renderCard(sh); } catch (_) {}
          }, 120);
        };
        stub.__stub = true; window.startNest = stub; /* INPLACE (Paul, 20 Oct 2026): a sheet whose replaced piece came off is written as it stands (writeAsPlaced, no nest): stood in for here as the nest is, every placement kept */ const wap = sh => { window.__writes = (window.__writes || []).concat([{ sheetId: sh.sheetId, page: sh.page, placements: sh.placements.map(p => ({ id: p.id, cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle })) }]); sh.status = 'finishing'; sh.persistedDone = false; let done; sh.persisted = new Promise(r => { done = r; }); return new Promise(res => setTimeout(async () => { try { if (sh.sheetId) await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: sh.draft || undefined, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: (c.orderInfo && c.orderInfo.sku) || c.sku })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true }); } catch (e) { sh.problem = e.message; } sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.persistedDone = true; sh.verification = { ok: true }; done(); try { CN.renderCard(sh); } catch (_) {} res(!sh.problem); }, 120)); }; wap.__stub = true; window.writeAsPlaced = wap; clearInterval(iv);
      }, 0);
    }, who);
    const page = await ctx.newPage(); page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(`${who}: ${String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')}`));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && window.Review && Review.relinkNow && window.CharmNestListingRelink && window.CharmNestReviewLink && window.Pool && window.LiveNest && window.startNest && window.startNest.__stub && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    return { ctx, page };
  }
  const setup = page => page.evaluate(async ({ LINES, PAGES, RUN, BASE_TS, SHIP, SKUS, E, ELID, ESKU }) => {
    await Orders.loadMaps(true);
    CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72] });
    const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, TID = tx => String(5100000000 + tx), pid = (rid, tx) => `${rid}_${TID(tx)}_1`;
    // each design draws differently: its outline and one member of its own size (the moon pair a small hole up top, MOON_3288 a large disc), so a picture says which design it is
    const sq = (x0, y0, x1, y1) => ({ kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]] });
    const memberOf = sku => sku === 'MOON_3288' ? sq(D * .2, D * .2, D * .8, D * .8) : sku === 'TINY MOON & STARS 2' ? sq(D * .45, D * .7, D * .55, D * .8) : sq(D * .35, D * .35, D * .65, D * .65);
    const mkCharm = (id, sku, extra) => { const outline = sq(0, 0, D, D); return Object.assign({ id, name: id, sku, sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline, members: [outline, memberOf(sku)], rMm: R, w: 16, h: 16, scale: 2, bits: new Uint8Array(256).fill(1) /* (dense at the check's 2 px/pt: INPLACE fits the new piece by its outline) */, widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2, hash: 'h-' + sku }, extra || {}); };
    const reg = sku => { const key = 'test/' + sku.replace(/\W+/g, '_') + '.ai'; B.master.entries.set(sku, { sku, updatedAt: 1, aiPath: key, widthPt: 2 * R * MM, heightPt: 2 * R * MM, holes: 0, upAngle: null, labelSource: 'text', charmHash: 'h-' + sku }); B.pool.sources.set(key, { id: 'pool:' + sku, pool: true, name: sku, sku, charms: [mkCharm('pool:' + sku + ':0', sku, { name: sku })], usedAt: Date.now() }); };
    for (const sku of SKUS) reg(sku);
    const pr = CN.S.sheets.gold; pr.pages.length = 1; const p0 = pr.pages[0]; Object.assign(p0, { charms: [], placements: [], sheetId: null, fileBase: null, status: 'idle', page: 1 }); for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'releaseFull']) delete p0[k];
    B.pool.rows.clear();
    for (const [pg, sh] of Object.entries(PAGES)) {
      while (pr.pages.length < +pg) addPage('gold');
      const page = pr.pages[+pg - 1]; page.charms = []; page.placements = [];
      LINES.filter(l => l[5] === +pg).forEach((l, i) => {
        const id = `${sh.id}-c${i}`, poolId = pid(l[0], l[1]);
        page.charms.push(mkCharm(id, l[3], { name: `${l[0]} · ${l[3]}`, poolId, order: l[0], lineKey: `${l[0]}_${TID(l[1])}`, orderDate: BASE_TS - l[4], orderInfo: { receiptId: l[0], transactionId: TID(l[1]), sku: l[3], copy: 1, quantity: 1 } }));
        page.placements.push({ id, cxPt: 16 + l[6] * 30, cyPt: 16, angle: 0, wPt: 2 * R * MM, hPt: 2 * R * MM });
      });
      Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: 'GF Sheet ' + pg, sheetIndex: +pg, page: +pg, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false });
      if (sh.cut) page.laserDoneAt = Date.now() - 3600000;
      if (sh.draft) Object.assign(page, { draft: true, setId: null });
      CN.renderCard(page);
    }
    const rows = [];
    for (const l of LINES) {
      const [rid, tx, listingId, sku, age, pg] = l;
      const line = { transactionId: TID(tx), listingId, sku, title: listingId === '1819191541' ? 'Tiny Moon Charm Celestial Charm Moon Jewelry Add On Charm Minimalist Charm Gold Accent Birthday Gift' : sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' };
      const order = { receiptId: rid, orderNumber: rid, createTs: BASE_TS - age, updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-2) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
      rows.push({ key: CharmNestOrders.lineKey(order, line), order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [pid(rid, tx)], engrave: null, material: 'gold' });
      B.pool.rows.set(pid(rid, tx), { poolId: pid(rid, tx), orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sku, copy: 1, quantity: 1, sheetId: PAGES[pg].id, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 });
    }
    { const line = { transactionId: '5100000009', listingId: ELID, sku: ESKU, title: 'Africa Lion Charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }, order = { receiptId: E, orderNumber: E, createTs: BASE_TS - 8000, updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Buyer E' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
      rows.push({ key: CharmNestOrders.lineKey(order, line), order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: 'gold' }); }
    B.orders.rows.length = 0; B.orders.byKey.clear(); for (const r of rows) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); }
    B.sets.clear();
    B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [...new Set(rows.map(r => r.order.receiptId))] };
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    return rows.map(r => [r.order.receiptId, r.spec.designSku, r.spec.skuSource]);
  }, { LINES, PAGES, RUN, BASE_TS, SHIP, SKUS, E, ELID, ESKU });
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + (typeof what === 'function' ? await what() : what)); await sleep(150); } };
  // what a box shows: its picture's address (the drawn image), '' while it loads
  const shown = (page, sel) => page.evaluate(sel => { const h = document.querySelector(sel); if (!h) return null; const i = h.querySelector('img'); if (i && i.src) return i.src; const c = h.querySelector('canvas'); return c ? c.toDataURL() : ''; }, sel);
  const rowSel = (page, rid) => page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); return `[data-key="${CSS.escape(r.key)}"] [data-vector]`; }, rid);
  // the picture the page draws for a piece of `sku` (the renderer and options of the Vector design box: its pooled piece, front, 220 px)
  const pictureOf = (page, sku) => page.evaluate(async sku => { const src = [...B.pool.sources.values()].find(s => s.sku === sku); const c = Object.assign({}, src.charms[0], { id: 'probe:' + sku }); const out = await CharmNestPDF.frontPreview(c, 220); return typeof out === 'string' ? out : out && out.toDataURL ? out.toDataURL('image/png') : null; }, sku);
  const which = async (page, sel, picOld, picNew) => { const s = await shown(page, sel); return s === picNew ? 'MOON_3288' : s === picOld ? 'the OLD design' : s == null ? 'no box' : s === '' ? 'nothing yet' : 'another picture'; };
  try {
    const one = await openPage('Paul');
    const read0 = await setup(one.page);
    assert(read0.filter(r => [A, D].includes(r[0])).every(r => r[1] === OLD), 'before the correction both lines read the exact master SKU: ' + JSON.stringify(read0));
    await one.page.evaluate(() => { if (!B.employee) B.employee = 'Paul'; window.__asked = []; window.confirm = m => { window.__asked.push(m); return true; }; });
    const picOld = await pictureOf(one.page, OLD), picNew = await pictureOf(one.page, NEW);
    assert(picOld && picNew && picOld !== picNew, 'the two designs draw differently');
    const selA = await rowSel(one.page, A), selD = await rowSel(one.page, D);
    await until(async () => (await shown(one.page, selA)) === picOld, 15000, async () => 'A\'s Orders row shows the old design (it shows ' + await which(one.page, selA, picOld, picNew) + ')');
    await one.page.evaluate(A => { const r = B.orders.rows.find(x => x.order.receiptId === A); OrderWin.open(r.key); }, A);
    await until(async () => (await shown(one.page, '#owVector')) === picOld && await one.page.evaluate(() => !!document.getElementById('owSkuCard')), 15000, 'the order window shows the old design');
    // the order window's Sheet tab: the plate drawn with A's piece as it is now
    const plateSku = page => page.evaluate(id => { const inf = OrderWin._sheet(); const x = inf && (inf.pieces || []).find(p => p.poolId === id); return x ? String((x.c && x.c.orderInfo && x.c.orderInfo.sku) || x.sku || '') : ''; }, idA);
    await one.page.evaluate(() => OrderWin.setView('sheet'));
    await until(async () => (await plateSku(one.page)) === OLD, 15000, async () => 'the Sheet tab draws A\'s old piece (it draws "' + await plateSku(one.page) + '")');
    await one.page.evaluate(() => OrderWin.setView('info'));
    const keyBefore = await one.page.evaluate(A => PieceMedia.vectorKey(B.orders.rows.find(x => x.order.receiptId === A)), A);
    pass('A is nested on the draft GF Sheet 1: its Orders row and its order window draw the old design from its piece');
    /* the pick, in the order window */
    // (the cloud answers as it does in the shop, not at once: the piece read before the take-off takes a moment, and the window is painted meanwhile, as the page paints it on every change)
    await one.page.evaluate(() => { const real = window.api; window.api = (fn, body, o) => body && body.op === 'poolGet' ? new Promise(r => setTimeout(r, 400)).then(() => { try { OrderWin.paint(); } catch (_) {} return new Promise(r => setTimeout(r, 200)); }).then(() => real(fn, body, o)) : real(fn, body, o); });
    // (EDITSKU Etsy: the page's editSkuEtsy call is answered here as the server answers a done write (tests/charm-nest/etsy-link.cjs drives the real one against a fake Etsy))
    await one.page.evaluate(() => { const real = window.api; window.__etsy = []; window.api = (fn, body, o) => body && body.op === 'editSkuEtsy' ? (window.__etsy.push(JSON.parse(JSON.stringify(body))), new Promise(r => setTimeout(() => r({ ok: true, state: 'linked', listingId: body.listingId, sku: body.sku, changed: 2, verified: true, beforePath: 'x/before.json' }), 300))) : real(fn, body, o); });
    const w0 = st.calls.length, nests0 = await one.page.evaluate(() => (window.__nests || []).length), writes0 = await one.page.evaluate(() => (window.__writes || []).length), t0 = Date.now();
    const plate = page => page.evaluate(() => Object.fromEntries(CN.S.sheets.gold.pages.flatMap(p => p.placements.map(pl => { const c = p.charms.find(z => z.id === pl.id) || {}; return [c.poolId, [p.sheetId, +pl.cxPt, +pl.cyPt, +pl.angle || 0]]; }))));
    const plate0 = await plate(one.page);
    // ── 1 · the toggle ──
    await until(() => one.page.evaluate(() => !document.getElementById('owEditSku').disabled), 8000, 'Change SKU is offered for A');
    await one.page.click('#owEditSku');
    await until(() => one.page.evaluate(() => document.getElementById('owSkuDlg').open), 5000, 'the Change SKU window');
    const tg = () => one.page.evaluate(() => { const b = document.getElementById('owSkuOnce'), p = document.getElementById('owSkuOnceP'), r = b.getBoundingClientRect(), d = document.getElementById('owSkuDlg').getBoundingClientRect();
      return { on: b.getAttribute('aria-checked'), role: b.getAttribute('role'), seen: r.width > 20 && r.height > 10 && b.checkVisibility() && r.bottom <= d.bottom && r.top >= d.top, label: document.getElementById('owSkuOnceL').textContent,
        say: p.hidden ? '' : p.textContent, save: document.querySelector('#owSkuSave span').textContent, chips: [...document.querySelectorAll('#owSkuScope .seChip')].map(c => c.textContent.trim()) }; });
    let t = await tg();
    assert(t.role === 'switch' && t.on === 'false' && t.seen && t.label === 'Just this order' && t.say === '' && t.save === 'Change SKU', 'the toggle is in the window, OFF when it opens, and says nothing more: ' + JSON.stringify(t));
    const chipsOff = t.chips;
    pass('the window has the "Just this order" switch, off by default');
    await one.page.fill('#owSkuInput', NEW);
    await until(() => one.page.evaluate(() => !document.getElementById('owSkuSave').disabled), 5000, 'Save once the charm is found in the repo');
    if (process.env.SHOT) { await sleep(500); await one.page.locator('#owSkuDlg').screenshot({ path: `${process.env.SHOT}-off.png` }); }
    await one.page.click('#owSkuOnce');
    t = await tg();
    assert(t.on === 'true' && t.say === `Only this order gets the new charm. The listing, Etsy and future orders keep ${OLD}.` && t.save === 'Use for this order' && t.chips.join('|') === `Order ${A} only|Etsy not changed`, 'ON: one short line says what it does, and the chips and the button follow: ' + JSON.stringify(t));
    assert(t.say.split(/\s+/).length <= 20, 'the line is short: ' + t.say.split(/\s+/).length + ' words');
    if (process.env.SHOT) { await sleep(300); await one.page.locator('#owSkuDlg').screenshot({ path: `${process.env.SHOT}-on.png` }); }
    await one.page.click('#owSkuOnceL');
    t = await tg();
    assert(t.on === 'false' && t.say === '' && t.save === 'Change SKU' && t.chips.join('|') === chipsOff.join('|'), 'a press on its label turns it off again: the listing\'s chips are back: ' + JSON.stringify(t));
    await one.page.focus('#owSkuOnce'); await one.page.keyboard.press('Space');
    t = await tg();
    assert(t.on === 'true' && t.save === 'Use for this order', 'and Space on the switch turns it on: ' + JSON.stringify(t));
    pass('ON, the window says in one short line what it does (this order only; the listing, Etsy and future orders keep ' + OLD + '); the chips and the button say so too; off and on again by its label and the keyboard');

    // ── 2 · saved ON: this order only ──
    const calls0 = st.calls.length, etsy0 = await one.page.evaluate(() => (window.__etsy || []).length);
    await one.page.click('#owSkuSave');
    await until(() => one.page.evaluate(() => !document.getElementById('owSkuDlg').open), 20000, 'the window closes once it is done');
    const rowA = () => one.page.evaluate(A => { const r = B.orders.rows.find(x => x.order.receiptId === A); return { sku: r.spec.designSku, src: r.spec.skuSource, ov: r.skuOverride || null, probs: (r.problems || []).map(p => p.kind), state: r.state, poolIds: r.poolIds || [], made: (r.poolIds || []).map(id => (B.pool.rows.get(id) || {}).sku), rec: Orders.lineRecord(r)[1].skuOverride || null }; }, A);
    await until(async () => { const a = await rowA(); return a.sku === NEW && a.made.length && a.made.every(s => s === NEW); }, 30000, async () => 'A is cut as ' + NEW + ' and its piece is made up again: ' + JSON.stringify(await rowA()));
    const a = await rowA();
    assert(a.src === 'once' && a.ov && a.ov.sku === NEW && a.ov.was === OLD && a.ov.by && a.ov.at > 0 && !a.probs.length, 'A reads the person\'s charm for this order, with no question: ' + JSON.stringify(a));
    assert(a.rec && a.rec.sku === NEW && a.rec.was === OLD, 'the run\'s line record keeps the choice: ' + JSON.stringify(a.rec));
    const back = await one.page.evaluate(A => { const r = B.orders.rows.find(x => x.order.receiptId === A), rec = Orders.lineRecord(r), row = Orders.rowFromRecord(rec[0], rec[1]); return row.skuOverride || null; }, A);
    assert(back && back.sku === NEW, 'a line read back from its record has it: ' + JSON.stringify(back));
    const sup = st.calls.slice(calls0).filter(c => c.op === 'poolUpdate' && JSON.stringify(c.body).includes(idA) && /superseded/.test(JSON.stringify(c.body)));
    assert(sup.length >= 1, 'A\'s old piece is superseded on the server');
    pass('saved ON: A is cut as ' + NEW + ' from the person\'s choice, its old piece superseded and the new one made; the choice is in the run\'s line record and comes back with it');
    const ops = st.calls.slice(calls0).map(c => String(c.op));
    assert(!ops.some(o => /^(aliasPut|aliasDelete|optionMapPut|optionMap|linkPut|reviewLink|editSkuEtsy|listingRelink)/.test(o)), 'no saved link, alias or listing write: ' + [...new Set(ops)].join(', '));
    assert.equal(await one.page.evaluate(() => (window.__etsy || []).length), etsy0, 'no Etsy write');
    const al = st.doc('Charm_Sku_Aliases', LID);
    assert(!al || !al.bySku || !Object.values(al.bySku).includes(NEW), 'the listing has no link to ' + NEW + ': ' + JSON.stringify(al));
    const others = await one.page.evaluate(({ A, D }) => { const d = B.orders.rows.find(x => x.order.receiptId === D), r = B.orders.rows.find(x => x.order.receiptId === A);
      const fresh = CharmNestOrders.interpretLine(Object.assign({}, r.order, { receiptId: '4299999999', orderNumber: '4299999999' }), Object.assign({}, r.line, { transactionId: '5199999999' }), Orders.ctx());
      return { d: d.spec.designSku, dOv: !!d.skuOverride, next: fresh.designSku, nextSrc: fresh.skuSource }; }, { A, D });
    assert(others.d === OLD && !others.dOv, 'D, the same listing on the cut sheet, is not touched: ' + JSON.stringify(others));
    assert(others.next === OLD && others.nextSrc !== 'once', 'a new order of the listing reads ' + OLD + ' as before: ' + JSON.stringify(others));
    pass('nothing else changes: no Etsy write, no saved link, D (same listing) untouched, the next order of the listing reads ' + OLD);
    await until(async () => (await shown(one.page, '#owVector')) === picNew, 15000, async () => 'the order window shows the new charm (it shows ' + await which(one.page, '#owVector', picOld, picNew) + ')');
    pass('the order window shows ' + NEW);
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`edit-sku-once-live: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
