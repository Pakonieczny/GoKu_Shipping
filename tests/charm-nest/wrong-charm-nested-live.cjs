// WRONGCHARM2, the page (Paul, 20 Oct 2026): "When I use this menu ['Wrong charm?'] to update an incorrect charm match, and that order is already nested on a sheet, it does not update automatically and
// also the thumbnails that show the vector file do not update. Everything should automatically update as soon as the user chooses an alternate charm, everywhere it must be updated, in real time,
// without requiring a refresh of the browser." Real headless Chromium over the repo's fake site (bridge-server.cjs: the REAL charmNestLibrary handler on an in-memory Firestore; nothing leaves the
// machine). The only stand-in is the nest (the solver), as in wrong-charm-browser.cjs. The pool pieces carry drawings of their own (an outline and a member of its own size), so every picture of a
// line is drawn from its piece, as the page draws a real one.
//   A 4193172538  listing 1819191541, NESTED on the draft GF Sheet 1 (not cut, in no set); its Orders row and its order window draw TINY MOON & STARS 2 from its piece
//       → the SKU card's Change (SKUCARD, 26 Oct 2026: "Wrong charm?" of the order window is the Change SKU window now, its progress drawn in it; Etsy stood in for) → MOON_3288 → within seconds, no reload: the old piece superseded, a MOON_3288 piece placed IN THE OLD SPOT (INPLACE, Paul 20 Oct 2026: the same centre and angle, every
//         other charm where it was, no nest started), GF Sheet 1 written as it stands and saved; the Orders row's Vector design,
//         the order window's Vector design and the shared picture (piece dots, Engrave previews: PieceMedia.vectorKey / vectorThumb) are MOON_3288's drawing, never the old one under the new name
//   D 4193172541  the same listing, CUT on GF Sheet 2 → its piece kept, the line says its piece is the old design and why; its picture is the piece it has
//   (a second computer learning the pick through the maps: listing-pick-relink-browser.cjs)
//   node tests/charm-nest/wrong-charm-nested-live.cjs        (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-wrongcharm2-1';
const LID = '1819191541', OLD = 'TINY MOON & STARS 2', NEW = 'MOON_3288';
const SKUS = [OLD, NEW, 'GFFILL_1', 'GFFILL_3'];
const SHIP = Math.floor(Date.UTC(2026, 9, 24, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 9, 16, 12) / 1000);
const A = '4193172538', D = '4193172541', F = n => '41910000' + String(n).padStart(2, '0');
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
  const setup = page => page.evaluate(async ({ LINES, PAGES, RUN, BASE_TS, SHIP, SKUS }) => {
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
    B.orders.rows.length = 0; B.orders.byKey.clear(); for (const r of rows) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); }
    B.sets.clear();
    B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [...new Set(rows.map(r => r.order.receiptId))] };
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    return rows.map(r => [r.order.receiptId, r.spec.designSku, r.spec.skuSource]);
  }, { LINES, PAGES, RUN, BASE_TS, SHIP, SKUS });
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
    const w0 = st.calls.length, nests0 = await one.page.evaluate(() => (window.__nests || []).length), writes0 = await one.page.evaluate(() => (window.__writes || []).length), t0 = Date.now();
    const plate = page => page.evaluate(() => Object.fromEntries(CN.S.sheets.gold.pages.flatMap(p => p.placements.map(pl => { const c = p.charms.find(z => z.id === pl.id) || {}; return [c.poolId, [p.sheetId, +pl.cxPt, +pl.cyPt, +pl.angle || 0]]; }))));
    // (EDITSKU Etsy: the page's editSkuEtsy call is answered here as the server answers a done write, as in edit-sku-live.cjs; without it the real server would try the real Etsy)
    await one.page.evaluate(() => { const real = window.api; window.__etsy = []; window.api = (fn, body, o) => body && body.op === 'editSkuEtsy' ? (window.__etsy.push(JSON.parse(JSON.stringify(body))), new Promise(r => setTimeout(() => r({ ok: true, state: 'linked', listingId: body.listingId, sku: body.sku, changed: 2, verified: true, beforePath: 'x/before.json' }), 300))) : real(fn, body, o); });
    const plate0 = await plate(one.page);
    // (SKUCARD: "Wrong charm?" of the order window is the SKU card's Change now: the Change SKU window, the exact SKU pasted, Save; the window turns into the progress and says when it is completed)
    await until(() => one.page.evaluate(() => !document.getElementById('owEditSku').disabled), 8000, 'Change SKU is offered for A');
    await one.page.click('#owEditSku');
    await until(() => one.page.evaluate(() => document.getElementById('owSkuDlg').open), 5000, 'the Change SKU window');
    await one.page.fill('#owSkuInput', NEW);
    await until(() => one.page.evaluate(() => !document.getElementById('owSkuSave').disabled && document.getElementById('owSkuTo').dataset.s === 'ok'), 5000, 'the pasted SKU is found in the repo: Save is offered');
    await one.page.click('#owSkuSave');
    await until(() => one.page.evaluate(() => !document.getElementById('owSkuProg').hidden && !!document.querySelector('#owSkuProg .rlFlow') && document.getElementById('owSkuForm').hidden), 8000, 'the window shows the progress of the change');
    await until(() => one.page.evaluate(() => !!document.querySelector('#owSkuProg .rlFlow[data-done="1"]')), 30000, () => one.page.evaluate(() => 'the progress says the change is completed: ' + JSON.stringify([...document.querySelectorAll('#owSkuProg li')].map(li => [li.dataset.s, li.textContent]))));
    assert(await one.page.evaluate(() => document.getElementById('owSkuSave').hidden && document.getElementById('owSkuCancel').textContent === 'Close'), 'once completed: Save is gone, Close is offered');
    await one.page.click('#owSkuCancel');
    await until(() => one.page.evaluate(() => !document.getElementById('owSkuDlg').open), 5000, 'Close closes the Change SKU window');
    assert(await one.page.evaluate(() => OrderWin.isOpen()), 'the order window stays open under it');
    assert.equal(await one.page.evaluate(() => window.__asked.length), 0, 'no confirming line: Save in the window is the yes');
    // the piece: the old one superseded, a MOON_3288 one placed on a sheet, GF Sheet 1 nested again and saved
    await until(() => one.page.evaluate(({ A, NEW }) => { const r = B.orders.rows.find(x => x.order.receiptId === A); const id = r.poolIds[0]; const p = id && B.pool.rows.get(id); const sh = id && Pool.sheetOf(id); const c = sh && sh.charms.find(x => x.poolId === id); return !!(p && p.sku === NEW && c && (c.orderInfo || {}).sku === NEW && sh.placements.some(pl => pl.id === c.id) && CN.S.sheets.gold.pages.every(pg => !['nesting', 'finishing', 'queued', 'ready'].includes(pg.status) && !pg._inplaceWrite)); }, { A, NEW }), 15000,
      () => one.page.evaluate(A => { const r = B.orders.rows.find(x => x.order.receiptId === A); return 'A made up again from MOON_3288 and placed: ' + JSON.stringify({ state: r.state, hold: r.hold, spec: r.spec.designSku, ids: r.poolIds, pool: r.poolIds.map(id => B.pool.rows.get(id)), sheets: CN.S.sheets.gold.pages.map(p => [p.sheetId, p.status, p.charms.map(c => c.poolId + ':' + ((c.orderInfo || {}).sku || c.sku))]) }); }, A));
    const took = Date.now() - t0;
    const sup = st.calls.slice(w0).filter(c => c.op === 'poolUpdate' && JSON.stringify(c.body).includes(idA) && /superseded/.test(JSON.stringify(c.body)));
    assert(sup.length >= 1, 'the old piece of A was taken off: superseded in the cloud');
    assert.equal(st.doc(POOL, idA).sku, NEW, 'A\'s piece record is MOON_3288 now');
    // (INPLACE, Paul 20 Oct 2026: "you're not nesting the entire sheet because of one charm/charms change": the new piece in the old spot, every other charm where it was, no nest at all)
    const nests = await one.page.evaluate(n0 => (window.__nests || []).slice(n0), nests0), writes = await one.page.evaluate(n0 => (window.__writes || []).slice(n0), writes0);
    assert.deepEqual(nests, [], 'no nest was started: ' + JSON.stringify(nests));
    assert.deepEqual(writes.map(x => x.sheetId), ['wn-gold-1'], 'GF Sheet 1 was written as it stands: ' + JSON.stringify(writes.map(x => x.sheetId)));
    const plate1 = await plate(one.page);
    assert.deepEqual(plate1[idA], plate0[idA], 'A\'s new piece is in A\'s old spot, at the same centre and angle: ' + JSON.stringify([plate0[idA], plate1[idA]]));
    for (const [id, at] of Object.entries(plate0)) assert.deepEqual(plate1[id], at, `${id} did not move`);
    const putA = st.calls.slice(w0).filter(c => c.op === 'putSheet' && c.body && c.body.sheet && c.body.sheet.id === 'wn-gold-1' && JSON.stringify(c.body.sheet.charms || []).includes(NEW));
    assert(putA.length >= 1, 'GF Sheet 1 was saved again with A\'s new piece');
    const onSheet = await one.page.evaluate(id => CN.S.sheets.gold.pages.reduce((n, p) => n + p.charms.filter(c => c.poolId === id).length, 0), idA);
    assert.equal(onSheet, 1, 'one piece of A on the sheets, never two');
    pass(`within ${(took / 1000).toFixed(1)} s, no reload: A's old piece superseded, a MOON_3288 piece placed in the old spot (nothing else moved, no nest), GF Sheet 1 written as it stands and saved`);
    /* every picture of A: MOON_3288's drawing */
    // (the order window is where the pick is made: its Vector design repaints at once; the Orders list behind it is drawn again when the window closes, the list's own rule)
    await until(async () => (await shown(one.page, '#owVector')) === picNew, 8000, async () => 'the order window\'s Vector design shows MOON_3288 (it shows ' + await which(one.page, '#owVector', picOld, picNew) + ')');
    await one.page.evaluate(() => OrderWin.setView('sheet'));
    await until(async () => (await plateSku(one.page)) === NEW, 8000, async () => 'the order window\'s Sheet tab draws A\'s MOON_3288 piece (it draws "' + await plateSku(one.page) + '")');
    await one.page.evaluate(() => OrderWin.close());
    await until(async () => (await shown(one.page, selA)) === picNew, 8000, async () => 'A\'s Orders row shows MOON_3288 (it shows ' + await which(one.page, selA, picOld, picNew) + ')');
    const keyAfter = await one.page.evaluate(A => PieceMedia.vectorKey(B.orders.rows.find(x => x.order.receiptId === A)), A);
    assert(keyAfter !== keyBefore && keyAfter.includes(NEW), 'the shared picture key of A (piece dots, Engrave previews) is MOON_3288\'s now: ' + keyBefore + ' → ' + keyAfter);
    const thumbNow = await one.page.evaluate(A => PieceMedia.vectorThumb(B.orders.rows.find(x => x.order.receiptId === A)), A);
    assert.equal(thumbNow, picNew, 'the piece dots\' and Engrave previews\' picture of A is MOON_3288\'s drawing');
    pass('the Orders row, the order window\'s Vector design and Sheet tab, and the shared picture (piece dots, Engrave previews) show MOON_3288 at once');
    /* D, cut: its piece kept, the line says why */
    const d = await one.page.evaluate(D => { const r = B.orders.rows.find(x => x.order.receiptId === D); return { sku: r.spec.designSku, state: r.state, hold: r.hold || '', piece: (B.pool.rows.get(r.poolIds[0]) || {}).sku, keyD: PieceMedia.vectorKey(r) }; }, D);
    assert.equal(st.doc(POOL, idD).sku, OLD, 'D\'s piece record is untouched');
    assert.equal(st.calls.slice(w0).filter(c => /^(poolPut|poolUpdate)$/.test(String(c.op)) && JSON.stringify(c.body).includes(idD)).length, 0, 'no piece record of D is written');
    assert(/The listing's charm is now MOON_3288 \(picked by Paul\), but this piece was cut as TINY MOON & STARS 2 on GF Sheet 2/.test(d.hold), 'D keeps its piece and says it is the old design, cut: ' + JSON.stringify(d));
    assert.notEqual(d.keyD, keyAfter, 'D\'s picture (its old piece) never shares a key with MOON_3288\'s: ' + d.keyD);
    await until(async () => (await shown(one.page, selD)) === picOld, 8000, async () => 'D\'s Orders row still shows the piece it has, the old design (it shows ' + await which(one.page, selD, picOld, picNew) + ')');
    pass('D (cut) keeps its piece; its line says the piece is the old design and why; its picture is the piece it has, under a key of its own');
    /* the progress panel, drawn in the Change SKU window: one line per place updated, a full bar, "Completed at hh:mm", a summary; Close put the window away over the open order window (above) */
    const fl = await one.page.evaluate(() => { const el = document.querySelector('#owSkuProg .rlFlow'); return { done: el.dataset.done, step: el.querySelector('.rlStep').textContent, inWin: !!el.closest('#owSkuDlg'), title: el.querySelector('h3').textContent, bar: el.querySelector('.rlBar').getAttribute('aria-valuenow'), sum: el.querySelector('.rlSum').textContent, lines: [...el.querySelectorAll('li')].map(li => [li.dataset.s, li.querySelector('.rlT').textContent]) }; });
    const has = (state, re) => fl.lines.some(([s, t]) => s === state && re.test(t));
    assert(/^Edit SKU · listing 1819191541: TINY MOON & STARS 2 → MOON_3288/.test(fl.title) && fl.bar === '100' && fl.done === '1', 'the panel names the change and its bar is full: ' + JSON.stringify(fl));
    const ets = await one.page.evaluate(() => window.__etsy);
    assert.deepEqual(ets.map(b => [b.listingId, b.sku, b.onlySku]), [[LID, NEW, OLD]], 'ONE Etsy write, for this listing only, and only for its products with the line\'s Etsy SKU: ' + JSON.stringify(ets));
    assert.equal(st.calls.slice(w0).filter(c => c.op === 'editSkuEtsy').length, 0, 'the Etsy write never reached the server (the page\'s stand-in answered it)');
    assert(has('done', /^Etsy listing 1819191541 updated: “TINY MOON & STARS 2” → MOON_3288 on 2 products/), 'the Etsy line: ' + JSON.stringify(fl.lines));
    assert(has('done', /^Linked listing 1819191541 · SKU “TINY MOON & STARS 2” to MOON_3288/), 'the link line: ' + JSON.stringify(fl.lines));
    assert(has('done', new RegExp(`^Order ${A}: new piece placed in the old spot on GF Sheet 1.*made from MOON_3288`)), 'A\'s line names the sheet, the old spot and the new design: ' + JSON.stringify(fl.lines));
    assert(has('warn', new RegExp(`^Order ${D}: its piece stays TINY MOON & STARS 2 on GF Sheet 2.*cut`)), 'D\'s line says its piece stays, and why: ' + JSON.stringify(fl.lines));
    assert(has('done', /^GF Sheet 1 files saved$/) && has('done', /^Thumbnails updated/), 'the sheet and the pictures have their lines: ' + JSON.stringify(fl.lines));
    assert(has('done', /^Updated everywhere: Orders, Review, the order window/), 'the views line: ' + JSON.stringify(fl.lines));
    assert(!fl.lines.some(([, t]) => /nested again/.test(t)), 'no "nested again" line: ' + JSON.stringify(fl.lines));
    assert(!fl.lines.some(([s]) => s === 'failed') && /^MOON_3288 for listing 1819191541 · Etsy updated$/.test(fl.sum), 'nothing failed; the summary: ' + fl.sum);
    assert(fl.inWin && /^Completed at \d\d:\d\d · 100%$/.test(fl.step), 'the progress is in the Change SKU window and says when it was completed: ' + JSON.stringify([fl.inWin, fl.step]));
    pass('the progress panel in the Change SKU window says each place updated (Etsy listing 1819191541 only, the link, A\'s new piece placed in the old spot on GF Sheet 1, D kept and why, GF Sheet 1 files saved, the pictures, every view), fills, says "Completed at hh:mm · 100%", sums up, and Close puts the window away over the open order window');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`wrong-charm-nested-live: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
