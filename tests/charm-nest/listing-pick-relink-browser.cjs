// LISTPICK, the page (Paul, 18 Oct 2026): a person's pick for a listing re-makes every open order of that listing in the background. Real headless Chromium over the repo's fake site
// (bridge-server.cjs: the REAL charmNestLibrary handler on an in-memory Firestore; nothing leaves the machine). The only stand-in is the nest (the solver): what is placed stays where it is,
// waiting pieces go into the next free cell, oldest order first, __cap[sheetId] limits how many one search places; what it cannot place moves on through the page's own overflowToNextSheet.
//   Listing 1700000001, Etsy SKU "FEATHER" (the old drawing, an exact master match); the pick: FEATHER_4102, saved by the page's own REVLINK call (CharmNestReviewLink.save: aliasPut, read back).
//   A 4201900041  the order Paul corrected, on the open GF Sheet 1          → its old piece comes off, the new one goes into its room on GF Sheet 1
//   C 4204208130  the same listing, nested on GF Sheet 1                     → taken off; GF Sheet 1 has room for ONE new piece (cap) → it moves on to the next open sheet, GF Sheet 3
//   B 4201900042  the same listing, not pooled yet                           → reads FEATHER_4102, nothing written for it
//   D 4204208131  the same listing, its piece CUT on GF Sheet 2              → flagged: held for a person, the Review's Held row, nothing physical changes
//   X 4209999990  ANOTHER listing with the same SKU, on GF Sheet 1           → untouched
//   then: a second sweep writes nothing; a second computer (its own browser context) learns the pick through loadMaps and takes nothing off and places nothing
//   FULLDRAFT=1 (PICKFULL, Paul 19 Oct 2026): GF Sheet 1 is FULL (releaseFull) but still a draft waiting for its set: exactly the same outcome (a full draft has gone nowhere, its pieces are swapped)
//   node tests/charm-nest/listing-pick-relink-browser.cjs        (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', TL = 'Order_Timeline', RUN = 'run-lpick-1';
const LID = '1700000001', OTHER = '1700000099', OLD = 'FEATHER', NEW = 'FEATHER_4102';
const SHIP = Math.floor(Date.UTC(2026, 9, 24, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 9, 16, 12) / 1000);
const A = '4201900041', C = '4204208130', B_ = '4201900042', D = '4204208131', X = '4209999990', F = n => '42000000' + String(n).padStart(2, '0');
const TID = tx => String(5100000000 + tx), pid = (rid, tx) => `${rid}_${TID(tx)}_1`;
// [rid, tx, listing, sku, age (s), sheet page | null, cell]
const LINES = [[A, 1, LID, OLD, 9000, 1, 0], [C, 2, LID, OLD, 8000, 1, 1], [X, 3, OTHER, OLD, 7000, 1, 2], [F(1), 4, '1700000201', 'GFFILL_1', 6900, 1, 3], [F(2), 5, '1700000202', 'GFFILL_2', 6800, 1, 4],
  [D, 6, LID, OLD, 9500, 2, 0], [F(3), 7, '1700000203', 'GFFILL_3', 9400, 2, 1], [F(4), 8, '1700000204', 'GFFILL_4', 6000, 3, 0], [B_, 9, LID, OLD, 5000, null, null]];
const PAGES = { 1: { id: 'lp-gold-1' }, 2: { id: 'lp-gold-2', cut: true }, 3: { id: 'lp-gold-3' } };
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const srv = await start({ receipts: [] }), st = srv.st;
  for (const sku of [OLD, NEW, 'GFFILL_1', 'GFFILL_2', 'GFFILL_3', 'GFFILL_4']) st.put('Charm_Master_Index', sku, { sku, aiPath: 'test/' + sku + '.ai', widthPt: 28.35, heightPt: 28.35, holes: 0 }, false);
  // the cloud: the sheets as saved and the piece records
  for (const [pg, sh] of Object.entries(PAGES)) {
    const mine = LINES.filter(l => l[5] === +pg);
    st.put(SHEETS, sh.id, Object.assign({ id: sh.id, metal: 'gold', charms: mine.map((l, i) => ({ id: `${sh.id}-c${i}`, poolId: pid(l[0], l[1]), order: l[0] })), placements: mine.map((l, i) => ({ id: `${sh.id}-c${i}`, cxPt: 16 + l[6] * 30, cyPt: 16, angle: 0 })), poolIds: mine.map(l => pid(l[0], l[1])), sheetIndex: +pg, fileBase: 'GF Sheet ' + pg, runId: RUN }, sh.cut ? { laserDoneAt: Date.now() - 3600000 } : {}), false);
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
      // the nest: placed pieces stay; waiting pieces go into the next free cell, oldest order first, up to __cap[sheetId]; the rest moves on (the page's own overflowToNextSheet); the sheet is saved
      const iv = setInterval(() => {
        if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
        const P = window.CharmNestPDF; if (P) { P.drawCharm = () => {}; P.cutLinesOf = () => []; }
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
              const cap = (window.__cap && window.__cap[sh.sheetId] != null) ? window.__cap[sh.sheetId] : 999; let k = 0;
              for (const c of waiting) { if (!free.length || k >= cap) break; const [x, y] = free.shift(); sh.placements.push({ id: c.id, cxPt: x, cyPt: y, angle: 0, wPt: c.widthPt, hPt: c.heightPt }); k++; }
              const left = sh.charms.filter(c => !sh.placements.some(p => p.id === c.id));
              if (sh.sheetId) await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: c.sku })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true });
              sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.stage = ''; sh.persistedDone = true; sh.verification = { ok: true };
              if (left.length) { sh.rejects = left.map(c => c.id); overflowToNextSheet(sh, false); }
            } catch (e) { sh.problem = e.message; sh.status = 'complete'; sh.persistedDone = true; }
            try { CN.renderCard(sh); } catch (_) {}
          }, 120);
        };
        stub.__stub = true; window.startNest = stub; clearInterval(iv);
      }, 0);
    }, who);
    const page = await ctx.newPage(); page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(`${who}: ${String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')}`));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && window.Review && Review.relinkNow && window.CharmNestListingRelink && window.CharmNestReviewLink && window.Pool && window.LiveNest && window.startNest && window.startNest.__stub && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    return { ctx, page };
  }
  /** this computer's world: its lines (with or without their pieces), its sheets (or none: a second computer that does not hold them), the run */
  const setup = (page, opts) => page.evaluate(async ({ LINES, PAGES, RUN, BASE_TS, SHIP, opts }) => {
    await Orders.loadMaps(true);
    window.__cap = {};
    CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72] });
    const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, TID = tx => String(5100000000 + tx), pid = (rid, tx) => `${rid}_${TID(tx)}_1`;
    const mkCharm = (id, extra) => Object.assign({ id, name: id, sku: '', sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: 0, cy: 0, r: R * MM }, members: [], rMm: R, w: 8, h: 8, scale: 1, bits: new Uint8Array(64).fill(1), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2 }, extra || {});
    window.__regSku = sku => { const key = 'test/' + sku + '.ai'; B.master.entries.set(sku, { sku, updatedAt: 1, aiPath: key, widthPt: 2 * R * MM, heightPt: 2 * R * MM, holes: 0, upAngle: null, labelSource: 'text', charmHash: 'h-' + sku }); B.pool.sources.set(key, { id: 'pool:' + sku, pool: true, name: sku, sku, charms: [mkCharm('pool:' + sku + ':0', { sku, name: sku })], usedAt: Date.now() }); };
    for (const sku of ['FEATHER', 'FEATHER_4102', 'GFFILL_1', 'GFFILL_2', 'GFFILL_3', 'GFFILL_4']) window.__regSku(sku);
    const pr = CN.S.sheets.gold; pr.pages.length = 1; const p0 = pr.pages[0]; Object.assign(p0, { charms: [], placements: [], sheetId: null, fileBase: null, status: 'idle', page: 1 }); for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'releaseFull']) delete p0[k];
    B.pool.rows.clear();
    if (opts.sheets) for (const [pg, sh] of Object.entries(PAGES)) {
      while (pr.pages.length < +pg) addPage('gold');
      const page = pr.pages[+pg - 1]; page.charms = []; page.placements = [];
      LINES.filter(l => l[5] === +pg).forEach((l, i) => {
        const id = `${sh.id}-c${i}`, poolId = pid(l[0], l[1]);
        page.charms.push(mkCharm(id, { name: `${l[0]} · ${l[3]}`, sku: l[3], poolId, order: l[0], lineKey: `${l[0]}_${TID(l[1])}`, orderDate: BASE_TS - l[4], orderInfo: { receiptId: l[0], transactionId: TID(l[1]), sku: l[3], copy: 1, quantity: 1 } }));
        page.placements.push({ id, cxPt: 16 + l[6] * 30, cyPt: 16, angle: 0, wPt: 2 * R * MM, hPt: 2 * R * MM });
      });
      Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: 'GF Sheet ' + pg, sheetIndex: +pg, page: +pg, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false });
      if (sh.cut) page.laserDoneAt = Date.now() - 3600000;
      if (opts.fullDraft && +pg === 1) Object.assign(page, { releaseFull: true, draft: true });   // (PICKFULL: full, still a draft)
      CN.renderCard(page);
    }
    const rows = [];
    for (const l of LINES) {
      const [rid, tx, listingId, sku, age, pg] = l;
      const line = { transactionId: TID(tx), listingId, sku, title: sku === 'FEATHER' ? 'Feather Necklace Gold Dainty Feather Charm' : sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' };
      const order = { receiptId: rid, orderNumber: rid, createTs: BASE_TS - age, updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-2) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
      const mine = pg != null && opts.pieces;
      rows.push({ key: CharmNestOrders.lineKey(order, line), order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: mine ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: mine ? [pid(rid, tx)] : [], engrave: null, material: 'gold' });
      if (mine) B.pool.rows.set(pid(rid, tx), { poolId: pid(rid, tx), orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sku, copy: 1, quantity: 1, sheetId: PAGES[pg].id, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 });
    }
    B.orders.rows.length = 0; B.orders.byKey.clear(); for (const r of rows) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); }
    B.sets.clear();
    B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [...new Set(rows.map(r => r.order.receiptId))] };
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    return rows.map(r => [r.order.receiptId, r.spec.designSku, r.spec.skuSource]);
  }, { LINES, PAGES, RUN, BASE_TS, SHIP, opts: Object.assign({ fullDraft: !!process.env.FULLDRAFT }, opts) });
  const rowsNow = (page) => page.evaluate(() => Object.fromEntries(B.orders.rows.map(r => [r.order.receiptId, { sku: r.spec.designSku, src: r.spec.skuSource, state: r.state, hold: r.hold || null, poolIds: r.poolIds.slice(), pieces: r.poolIds.map(id => (B.pool.rows.get(id) || {}).sku || null) }])));
  const sheetsNow = page => page.evaluate(() => Object.fromEntries(CN.S.sheets.gold.pages.map(p => [p.sheetId || 'page' + p.page, { charms: p.charms.map(c => [c.poolId, c.sku || null]), placed: p.placements.map(pl => (p.charms.find(c => c.id === pl.id) || {}).poolId), status: p.status }])));
  const writesFor = (from, ids) => st.calls.slice(from).filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate|putSheet)$/.test(String(c.op)) && JSON.stringify(c.body).match(new RegExp(ids.join('|'))));
  const pieceWrites = (from, ids) => st.calls.slice(from).filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate)$/.test(String(c.op)) && JSON.stringify(c.body).match(new RegExp(ids.join('|'))));
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(150); } };
  try {
    /* ── computer 1 ── */
    const one = await openPage('Paul');
    const read0 = await setup(one.page, { sheets: true, pieces: true });
    assert(read0.filter(r => [A, C, B_, D, X].includes(r[0])).every(r => r[1] === OLD && r[2] === 'transaction'), 'before the pick the five lines read as the old FEATHER: ' + JSON.stringify(read0));
    // (computer 2 opens now, so the pick is news to it: it reads the maps once before)
    const two = await openPage('Ann');
    await setup(two.page, { sheets: false, pieces: false });
    const w0 = st.calls.length;
    /* 1 · the pick, through the page's own REVLINK save (aliasPut, read back, the maps read again); the background does the rest */
    // (Paul's case: the order was sent to Review, and its Review answered by picking the charm, "Charm picked: FEATHER_4102": CharmNestReviewLink.pick saves the link and resolves the mark)
    // (and C, the order "Marked OK" in Paul's screenshot, is in Review too: it is answered only after the pick)
    const marked = await one.page.evaluate(async ({ A, C }) => { const out = {}; for (const rid of [A, C]) { const m = await CharmNestReviewMark.mark(rid, { by: 'Paul' }); out[rid] = [m.ok, Pool.keepsPlace(B.orders.rows.find(r => r.order.receiptId === rid))]; } return out; }, { A, C });
    assert.deepEqual(marked, { [A]: [true, true], [C]: [true, true] }, 'orders A and C are in Review, their pieces kept on GF Sheet 1 (REVHOLD)');
    const saved = await one.page.evaluate(async ({ A, NEW }) => { const row = B.orders.rows.find(r => r.order.receiptId === A); const r = await CharmNestReviewLink.pick(row, NEW, { by: 'Paul' }); return { ok: r.ok, via: r.via, key: r.key, resolved: !!(r.resolved && r.resolved.ok), info: CharmNestReviewMark.info(A).how }; }, { A, NEW });
    assert.deepEqual(saved, { ok: true, via: 'alias', key: `${LID}|${OLD}`, resolved: true, info: 'pick' }, 'the pick is kept for the listing and its SKU, and A\'s Review is resolved as a pick: ' + JSON.stringify(saved));
    // nothing else is pressed: the sweep runs by itself. A (its Review answered by the pick) is made up again at once; C, still in Review, keeps its old piece where it is
    await until(() => one.page.evaluate(A => { const s = CN.S.sheets.gold.pages.find(p => p.sheetId === 'lp-gold-1'); const c = s.charms.find(x => x.poolId === A + '_5100000001_1'); return c && c.sku === 'FEATHER_4102' && s.placements.some(pl => pl.id === c.id) && s.status === 'complete'; }, A), 30000, 'A placed on GF Sheet 1');
    await sleep(500);
    { const k = await one.page.evaluate(C => { const r = B.orders.rows.find(x => x.order.receiptId === C), s = CN.S.sheets.gold.pages.find(p => p.sheetId === 'lp-gold-1'); return { sku: r.spec.designSku, piece: (B.pool.rows.get(r.poolIds[0]) || {}).sku, on: s.charms.filter(c => c.poolId === r.poolIds[0]).map(c => c.sku), waiting: Review.relinkPending() }; }, C);
      assert.deepEqual(k, { sku: NEW, piece: OLD, on: [OLD], waiting: 1 }, 'C, in Review, reads the pick but keeps its old piece on GF Sheet 1 until its Review is resolved: ' + JSON.stringify(k)); }
    pass('an order still in Review keeps its piece on its sheet (REVHOLD) and waits; the corrected order is made up at once');
    // C's Review is answered ("Marked OK"): it is made up again by itself; GF Sheet 1 now has no room for it in its next search, so it moves on
    await one.page.evaluate(async C => { window.__cap = { 'lp-gold-1': 0 }; await CharmNestReviewMark.resolve(C, { by: 'Paul', how: 'ok' }); }, C);
    await until(() => one.page.evaluate(({ A, C }) => { const s = Object.fromEntries(CN.S.sheets.gold.pages.map(p => [p.sheetId, p])); const placed = (sh, id) => sh && sh.placements.some(pl => (sh.charms.find(c => c.id === pl.id) || {}).poolId === id); return placed(s['lp-gold-1'], A + '_5100000001_1') && placed(s['lp-gold-3'], C + '_5100000002_1') && CN.S.sheets.gold.pages.every(p => !['nesting', 'queued', 'ready'].includes(p.status)); }, { A, C }), 30000, 'A placed on GF Sheet 1 and C on GF Sheet 3');
    await sleep(600);
    const rows1 = await rowsNow(one.page), sh1 = await sheetsNow(one.page);
    for (const rid of [A, C, B_, D]) assert.equal(rows1[rid].sku, NEW, `${rid} reads ${NEW} now: ${JSON.stringify(rows1[rid])}`);
    assert.equal(rows1[X].sku, OLD, 'the other listing with the same SKU still reads the old FEATHER');
    pass('the pick: every order of the listing reads FEATHER_4102, another listing with the same SKU is untouched');
    // A: in the room its old piece left, on GF Sheet 1
    const idA = pid(A, 1), idC = pid(C, 2), idD = pid(D, 6), idX = pid(X, 3);
    assert.deepEqual(rows1[A].pieces, [NEW], 'A\'s piece is FEATHER_4102: ' + JSON.stringify(rows1[A]));
    assert(sh1['lp-gold-1'].placed.includes(idA) && sh1['lp-gold-1'].charms.some(([id, sku]) => id === idA && sku === NEW), 'A is on GF Sheet 1 as FEATHER_4102: ' + JSON.stringify(sh1['lp-gold-1']));
    assert.equal(sh1['lp-gold-1'].charms.filter(([id]) => id === idA).length, 1, 'one piece of A on the sheet, never two');
    // C: GF Sheet 1 had room for one, so C moved on to the next open sheet (GF Sheet 2 is cut)
    assert(!sh1['lp-gold-1'].charms.some(([id]) => id === idC) && sh1['lp-gold-3'].placed.includes(idC), 'C did not fit GF Sheet 1 and moved on to GF Sheet 3: ' + JSON.stringify(sh1));
    assert.deepEqual(rows1[C].pieces, [NEW], 'C\'s piece is FEATHER_4102');
    assert(!sh1['lp-gold-2'].charms.some(([id]) => id === idC || id === idA), 'nothing went onto the cut GF Sheet 2');
    pass('A goes into its own room on GF Sheet 1; C, with no room left there, moves on to the next open sheet (GF Sheet 3, past the cut GF Sheet 2)');
    // the cloud: one take-off commit per line (superseded), the new rows FEATHER_4102, the saved sheets list them where they are
    const offs = st.calls.slice(w0).filter(c => c.op === 'poolUpdate' && c.body.patch && c.body.patch.state === 'superseded');
    assert.deepEqual(offs.map(c => c.body.poolIds).sort(), [[idA], [idC]], 'one take-off commit for each of A and C: ' + JSON.stringify(offs.map(c => c.body.poolIds)));
    for (const id of [idA, idC]) { const p = st.doc(POOL, id); assert(p.sku === NEW && !['superseded', 'abandoned'].includes(p.state), `${id} is FEATHER_4102 in the cloud: ${JSON.stringify(p)}`); }
    assert(st.doc(SHEETS, 'lp-gold-1').poolIds.includes(idA) && !st.doc(SHEETS, 'lp-gold-1').poolIds.includes(idC), 'the saved GF Sheet 1 lists A, no longer C');
    assert(st.doc(SHEETS, 'lp-gold-3').poolIds.includes(idC), 'the saved GF Sheet 3 lists C');
    pass('the cloud: one take-off commit per line, the pieces recorded as FEATHER_4102, the saved sheets list them where they are');
    // B: nothing written for it
    assert.equal(writesFor(w0, [B_]).length, 0, 'nothing is written for B (not pooled yet): ' + JSON.stringify(writesFor(w0, [B_]).map(c => c.op)));
    assert.equal(rows1[B_].poolIds.length, 0, 'B has no pieces yet');
    // D: on the cut sheet: held, the Review's Held row, nothing physical
    assert.equal(writesFor(w0, [idD]).length, 0, 'nothing is written for D\'s cut piece');
    assert(st.doc(POOL, idD).sku === OLD && st.doc(SHEETS, 'lp-gold-2').poolIds.includes(idD), 'D\'s piece is still the old FEATHER on the cut GF Sheet 2');
    assert(rows1[D].state === 'held' && /The listing's charm is now FEATHER_4102 \(picked by Paul\), but this piece was cut as FEATHER on GF Sheet 2/.test(rows1[D].hold || ''), 'D is held for a person, in plain words: ' + rows1[D].hold);
    const held = await one.page.evaluate(D => B.review.items.filter(i => i.key === 'held:' + D).map(i => i.kind), D);
    assert.deepEqual(held, ['heldOrder'], 'the Review has its Held row');
    assert.equal(pieceWrites(w0, [idX]).length, 0, 'no piece record of the other listing\'s X is written');
    assert(st.doc(POOL, idX).sku === OLD && st.doc(SHEETS, 'lp-gold-1').poolIds.includes(idX) && sh1['lp-gold-1'].placed.includes(idX), 'X stays where it was on GF Sheet 1, the old FEATHER');
    { const chg = await one.page.evaluate(() => B.review.items.filter(i => String(i.key).startsWith('chg:')).map(i => [i.key, i.why, i.row && i.row.changePending, i.row && i.row.repoolChanged, i.row && i.row.state])); assert.deepEqual(chg, [], 'no Order changed card is left: the pick answered it: ' + JSON.stringify(chg)); }
    pass('B (not pooled) reads the pick with nothing written; D (cut) is held for a person with a plain reason and a Review Held row, its cut piece untouched; X untouched');
    // the timeline: one event per order, naming the pick and who
    await one.page.evaluate(async () => { try { OrderTimeline.flush && OrderTimeline.flush(); } catch (_) {} });
    await until(async () => { await one.page.evaluate(() => { try { OrderTimeline.flush && OrderTimeline.flush(); } catch (_) {} }); const k = rid => st.list(TL).filter(e => e._id.startsWith(rid + '~') && /^Charm updated from listing pick by Paul/.test(e.text || '')).length; return [A, B_, D].every(rid => k(rid) >= 1) && k(C) >= 2; }, 20000, 'timeline events');
    const evs = rid => st.list(TL).filter(e => e._id.startsWith(rid + '~') && /^Charm updated from listing pick by Paul/.test(e.text || ''));
    for (const rid of [A, B_, D]) assert.equal(evs(rid).length, 1, `one event for ${rid}: ${JSON.stringify(evs(rid).map(e => e.text))}`);
    assert.deepEqual(evs(C).map(e => /stays on its sheet while the order is in Review/.test(e.text) ? 'kept' : /the old piece came off GF Sheet 1/.test(e.text) ? 'made' : e.text).sort(), ['kept', 'made'], 'C: kept while in Review, then made up again: ' + JSON.stringify(evs(C).map(e => e.text)));
    assert(/FEATHER → FEATHER_4102 · the old piece came off GF Sheet 1/.test(evs(A)[0].text) && evs(A)[0].by === 'Paul', 'A\'s event: ' + evs(A)[0].text);
    assert(/left as it was, a person must make it/.test(evs(D)[0].text), 'D\'s event says it was left: ' + evs(D)[0].text);
    assert.equal(st.list(TL).filter(e => e._id.startsWith(X + '~') && /listing pick/.test(e.text || '')).length, 0, 'X has no such event');
    pass('every affected order has "Charm updated from listing pick by Paul" on its timeline, once');
    /* 2 · again: nothing changes */
    const w1 = st.calls.length;
    const again = await one.page.evaluate(async () => { Review.relinkMaps({ aliases: {}, optionMaps: B.maps.optionMaps }); const r = await Review.relinkNow(); return { swapped: r.swapped.length, flagged: r.flagged.length, design: r.design.length }; });
    await sleep(800);
    assert.deepEqual(again, { swapped: 0, flagged: 0, design: 0 }, 'a second sweep finds nothing to do: ' + JSON.stringify(again));
    assert.equal(writesFor(w1, [A, B_, C, D, X]).length, 0, 'and writes nothing');
    pass('a second sweep over the same state changes nothing');
    /* 3 · the second computer learns the pick through its maps: it reads the new charm, takes nothing off, places nothing */
    const w2 = st.calls.length;
    await two.page.evaluate(async () => { await Orders.loadMaps(true); await Review.relinkNow(); });
    await sleep(800);
    const rows2 = await rowsNow(two.page);
    for (const rid of [A, C, B_, D]) assert.equal(rows2[rid].sku, NEW, `computer 2 reads ${rid} as ${NEW}`);
    assert.equal(rows2[X].sku, OLD, 'computer 2: X is still the old FEATHER');
    assert.equal(writesFor(w2, [A, B_, C, D, X]).length, 0, 'computer 2 took nothing off and placed nothing: ' + JSON.stringify(writesFor(w2, [A, B_, C, D, X]).map(c => c.op)));
    assert.equal(st.doc(SHEETS, 'lp-gold-1').poolIds.filter(id => id === idA).length, 1, 'A is on GF Sheet 1 once');
    // computer 2 holding stale rows of A's piece (the old drawing) still takes nothing off: the cloud says the piece is the pick already
    const stale = await two.page.evaluate(async ({ A, idA }) => {
      const r = B.orders.rows.find(x => x.order.receiptId === A); r.poolIds = [idA]; r.state = 'pooled'; B.pool.rows.set(idA, { poolId: idA, sku: 'FEATHER', state: 'placed', sheetId: null, runId: B.run.runId, copy: 1, updatedAt: 1 });
      Review.relinkMaps({ aliases: {}, optionMaps: B.maps.optionMaps }); const out = await Review.relinkNow(); return { swapped: out.swapped.length, piece: (B.pool.rows.get(idA) || {}).sku };
    }, { A, idA });
    await sleep(500);
    assert.deepEqual(stale, { swapped: 0, piece: NEW }, 'computer 2 with a stale piece reads the cloud first and does nothing: ' + JSON.stringify(stale));
    assert.equal(writesFor(w2, [A, B_, C, D, X]).length, 0, 'still no take-off or placement from computer 2');
    assert.equal(evs(A).length, 1, 'still one timeline event for A');
    pass('a second computer learns the pick through loadMaps: it reads FEATHER_4102, never takes a piece off or places one twice (even holding a stale copy of the piece)');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`listing-pick-relink-browser: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
