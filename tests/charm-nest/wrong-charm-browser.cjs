// WRONGCHARM, the page (Paul, 18 Oct 2026; order 4193072538, Etsy listing 1819191541 "Tiny Moon Charm Celestial Charm … Add On Charm", GF): the listing's Etsy SKU "TINY MOON & STARS 2" is an exact
// master SKU (a star + moon pair), so the line was matched and nested with no question; it is the library's MOON_3288. A line whose SKU matched never reaches Review, so "Wrong charm?" on the
// order's piece (the order window's Vector design, the Orders card's) opens a picker (the Review card's similar charms + the master search); the pick, confirmed in one line, is the SAME saved
// link a Review pick writes (CharmNestReviewLink.pick: Charm_Sku_Aliases/{listing}.bySku[the SKU as bought]), which beats the exact master match for THIS listing only, and LISTPICK re-makes
// every open order of the listing. Real headless Chromium over the repo's fake site (bridge-server.cjs: the REAL charmNestLibrary handler on an in-memory Firestore; nothing leaves the machine).
// The only stand-ins are the nest (the solver), as in listing-pick-relink-browser.cjs, and the Etsy write (editSkuEtsy), as in edit-sku-live.cjs.
// SKUCARD / EDITSKU (26 Oct 2026): the order window's "Wrong charm?" and its inline picker are gone; the SKU card's Suggest shows the similar charms as tiles and Change opens the Change SKU
// window (Review.editSku: the same saved link and LISTPICK, plus the Etsy write for this listing only, plus every unmade line of the listing made up at once), its progress drawn in the window.
//   A 4193072538  Paul's order, on the open GF Sheet 1                 → Change SKU → MOON_3288 pasted → swapped on GF Sheet 1
//   C 4193072539  the same listing, on GF Sheet 1                       → switches too
//   B 4193072540  the same listing, not pooled yet                      → reads MOON_3288, made up at once from it (Edit SKU)
//   D 4193072541  the same listing, CUT on GF Sheet 2                   → flagged and held for a person, nothing physical changes
//   X 4193079999  ANOTHER listing with the same SKU, on GF Sheet 1      → untouched
//   then a reload (a fresh page over the same cloud): the listing reads MOON_3288, X still the pair; the Orders card's "Wrong charm?" opens the window with its suggestions
//   node tests/charm-nest/wrong-charm-browser.cjs        (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', TL = 'Order_Timeline', RUN = 'run-wrongcharm-1', ALIASES = 'Charm_Sku_Aliases';
const LID = '1819191541', OTHER = '1819199999', OLD = 'TINY MOON & STARS 2', NEW = 'MOON_3288', LIKE = 'TINY MOON & STARS 1';
const SKUS = [OLD, NEW, LIKE, 'GFFILL_1', 'GFFILL_2', 'GFFILL_3', 'GFFILL_4'];
const SHIP = Math.floor(Date.UTC(2026, 9, 24, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 9, 16, 12) / 1000);
const A = '4193072538', C = '4193072539', B_ = '4193072540', D = '4193072541', X = '4193079999', F = n => '41900000' + String(n).padStart(2, '0');
const TID = tx => String(5100000000 + tx), pid = (rid, tx) => `${rid}_${TID(tx)}_1`;
// [rid, tx, listing, sku, age (s), sheet page | null, cell]
const LINES = [[A, 1, LID, OLD, 9000, 1, 0], [C, 2, LID, OLD, 8000, 1, 1], [X, 3, OTHER, OLD, 7000, 1, 2], [F(1), 4, '1700000201', 'GFFILL_1', 6900, 1, 3], [F(2), 5, '1700000202', 'GFFILL_2', 6800, 1, 4],
  [D, 6, LID, OLD, 9500, 2, 0], [F(3), 7, '1700000203', 'GFFILL_3', 9400, 2, 1], [F(4), 8, '1700000204', 'GFFILL_4', 6000, 3, 0], [B_, 9, LID, OLD, 5000, null, null]];
const PAGES = { 1: { id: 'wc-gold-1' }, 2: { id: 'wc-gold-2', cut: true }, 3: { id: 'wc-gold-3' } };
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const srv = await start({ receipts: [] }), st = srv.st;
  for (const sku of SKUS) st.put('Charm_Master_Index', sku, { sku, aiPath: 'test/' + sku.replace(/\W+/g, '_') + '.ai', widthPt: 28.35, heightPt: 28.35, holes: 0 }, false);
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
        stub.__stub = true; window.startNest = stub; /* INPLACE (Paul, 20 Oct 2026): a sheet whose replaced piece came off is written as it stands (writeAsPlaced, no nest): stood in for here as the nest is, every placement kept */ const wap = sh => { window.__writes = (window.__writes || []).concat([{ sheetId: sh.sheetId, page: sh.page, placements: sh.placements.map(p => ({ id: p.id, cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle })) }]); sh.status = 'finishing'; sh.persistedDone = false; let done; sh.persisted = new Promise(r => { done = r; }); return new Promise(res => setTimeout(async () => { try { if (sh.sheetId) await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: sh.draft || undefined, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: (c.orderInfo && c.orderInfo.sku) || c.sku })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true }); } catch (e) { sh.problem = e.message; } sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.persistedDone = true; sh.verification = { ok: true }; done(); try { CN.renderCard(sh); } catch (_) {} res(!sh.problem); }, 120)); }; wap.__stub = true; window.writeAsPlaced = wap; clearInterval(iv);
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
    window.__regSku = sku => { const key = 'test/' + sku.replace(/\W+/g, '_') + '.ai'; B.master.entries.set(sku, { sku, updatedAt: 1, aiPath: key, widthPt: 2 * R * MM, heightPt: 2 * R * MM, holes: 0, upAngle: null, labelSource: 'text', charmHash: 'h-' + sku }); B.pool.sources.set(key, { id: 'pool:' + sku, pool: true, name: sku, sku, charms: [mkCharm('pool:' + sku + ':0', { sku, name: sku })], usedAt: Date.now() }); };
    for (const sku of opts.skus) window.__regSku(sku);
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
      CN.renderCard(page);
    }
    const rows = [];
    for (const l of LINES) {
      const [rid, tx, listingId, sku, age, pg] = l;
      const line = { transactionId: TID(tx), listingId, sku, title: listingId === '1819191541' ? 'Tiny Moon Charm Celestial Charm Moon Jewelry Add On Charm Minimalist Charm Gold Accent Birthday Gift' : sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' };
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
  }, { LINES, PAGES, RUN, BASE_TS, SHIP, opts: Object.assign({ skus: SKUS }, opts) });
  const rowsNow = (page) => page.evaluate(() => Object.fromEntries(B.orders.rows.map(r => [r.order.receiptId, { sku: r.spec.designSku, src: r.spec.skuSource, state: r.state, hold: r.hold || null, poolIds: r.poolIds.slice(), pieces: r.poolIds.map(id => (B.pool.rows.get(id) || {}).sku || null) }])));
  const sheetsNow = page => page.evaluate(() => Object.fromEntries(CN.S.sheets.gold.pages.map(p => [p.sheetId || 'page' + p.page, { charms: p.charms.map(c => [c.poolId, c.sku || null]), placed: p.placements.map(pl => (p.charms.find(c => c.id === pl.id) || {}).poolId), status: p.status }])));
  const writesFor = (from, ids) => st.calls.slice(from).filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate|putSheet)$/.test(String(c.op)) && JSON.stringify(c.body).match(new RegExp(ids.join('|'))));
  const pieceWrites = (from, ids) => st.calls.slice(from).filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate)$/.test(String(c.op)) && JSON.stringify(c.body).match(new RegExp(ids.join('|'))));
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + (typeof what === 'function' ? await what() : what)); await sleep(150); } };
  const signIn = page => page.evaluate(() => { if (!B.employee) B.employee = 'Paul'; window.__asked = []; window.confirm = m => { window.__asked.push(m); return true; }; return B.employee; });
  const openWin = async (page, rid) => {
    await page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); OrderWin.open(r.key); }, rid);
    await until(() => page.evaluate(() => OrderWin.isOpen() && !!(document.getElementById('owSkuCard') || {}).dataset.state), 15000, 'the order window with its SKU card');
  };
  // (EDITSKU Etsy: the page's editSkuEtsy call is answered here as the server answers a done write, as in edit-sku-live.cjs; without it the real server would try the real Etsy)
  const etsyStandIn = page => page.evaluate(() => { const real = window.api; window.__etsy = []; window.api = (fn, body, o) => body && body.op === 'editSkuEtsy' ? (window.__etsy.push(JSON.parse(JSON.stringify(body))), new Promise(r => setTimeout(() => r({ ok: true, state: 'linked', listingId: body.listingId, sku: body.sku, changed: 2, verified: true, beforePath: 'x/before.json' }), 300))) : real(fn, body, o); });
  const dlgOpen = page => page.evaluate(() => document.getElementById('owSkuDlg').open);
  try {
    const one = await openPage('Paul');
    const read0 = await setup(one.page, { sheets: true, pieces: true });
    assert(read0.filter(r => [A, C, B_, D, X].includes(r[0])).every(r => r[1] === OLD && r[2] === 'transaction'), 'before the correction the five lines read as the exact master SKU: ' + JSON.stringify(read0));
    assert.equal(await signIn(one.page), 'Paul', 'Paul is signed in');
    await etsyStandIn(one.page);
    const w0 = st.calls.length;
    /* 1 · the SKU card (SKUCARD, 26 Oct 2026: "Wrong charm?" of the order window is the card's Suggest and Change now): the SKU, its state, Copy / Suggest / Change */
    await openWin(one.page, A);
    const card = await one.page.evaluate(() => { const g = id => document.getElementById(id), vis = n => !!n && !n.hidden && n.offsetParent !== null; return { shown: vis(g('owSkuCard')), sku: g('owSku').textContent, state: g('owSkuCard').dataset.state, mark: g('owSkuState').title, btns: ['owCopy', 'owSuggest', 'owEditSku'].map(id => vis(g(id)) && !g(id).disabled), edit: g('owEditSku').title, suggest: g('owSuggest').title, n: g('owSuggestN').textContent, tray: g('owSkuTray').hidden, expanded: g('owSuggest').getAttribute('aria-expanded'), old: [!!g('owWrongCharm'), !!g('owCharmFix')] }; });
    assert.deepEqual([card.shown, card.sku, card.state, card.btns, card.tray, card.expanded, card.old], [true, 'SKU: ' + OLD, 'ok', [true, true, true], true, 'false', [false, false]], 'the order window shows the SKU card for the matched line (its SKU, the "ok" mark, Copy / Suggest / Change), the suggestions shut, no "Wrong charm?" button or inline picker: ' + JSON.stringify(card));
    assert(card.edit.includes(LID) && /repo matches/.test(card.mark), 'Change names the listing; the mark says a repo charm matches: ' + JSON.stringify([card.edit, card.mark]));
    assert(/^\d+ similar charms? in the repo$/.test(card.suggest) && card.suggest.startsWith(card.n + ' '), 'Suggest says how many similar charms it has: ' + JSON.stringify([card.suggest, card.n]));
    // a line that still asks which charm it is is answered on its Review card: the Orders card's "Wrong charm?" (Review.fixMount) is not offered for it
    const asks = await one.page.evaluate(() => { const r = B.orders.rows[0], sp = Object.assign({}, r.spec, { problems: [{ kind: 'unmatchedSku' }] }); return Review.fixOffer(Object.assign({}, r, { spec: sp })).ok; });
    assert.equal(asks, false, 'a line with an unmatched SKU question is not offered the correction');
    pass('the SKU card stands under the Vector design of a matched line: "SKU: TINY MOON & STARS 2", its mark, Copy / Suggest / Change (the Orders card\'s "Wrong charm?" never for a line that still asks)');
    /* 2 · the suggestions (the Review card's similar charms, one tile each) and the Change SKU window (the repo search, what the change reaches); Cancel saves nothing */
    await one.page.click('#owSuggest');
    await until(() => one.page.evaluate(() => !document.getElementById('owSkuTray').hidden && !!document.querySelector('#owSkuTray [data-sku]')), 5000, 'the suggestions tray');
    const tr = await one.page.evaluate(() => ({ tiles: [...document.querySelectorAll('#owSkuTray [data-sku]')].map(b => b.dataset.sku), n: document.getElementById('owSuggestN').textContent, expanded: document.getElementById('owSuggest').getAttribute('aria-expanded') }));
    assert(tr.tiles.includes(LIKE) && !tr.tiles.includes(OLD), 'the similar charms of the Review card are offered (one tap each), the current one is not a tile: ' + JSON.stringify(tr));
    assert.deepEqual([tr.expanded, tr.n], ['true', String(tr.tiles.length)], 'Suggest is pressed and counts its tiles: ' + JSON.stringify(tr));
    if (process.env.CN_SHOT) await one.page.locator('#orderWin .owPics').screenshot({ path: path.join(process.env.CN_SHOT, 'wrong-charm-suggest.png') }).catch(() => {});   // (a look, only when asked: CN_SHOT=<dir>)
    // Suggest again puts the tray away
    await one.page.click('#owSuggest');
    assert.deepEqual(await one.page.evaluate(() => [document.getElementById('owSkuTray').hidden, document.getElementById('owSuggest').getAttribute('aria-expanded')]), [true, 'false'], 'Suggest again puts the tray away');
    // a tile opens the Change SKU window with that charm as the new SKU
    await one.page.click('#owSuggest');
    await one.page.click(`#owSkuTray [data-sku="${LIKE}"]`);
    await until(() => dlgOpen(one.page), 5000, 'the Change SKU window, from a tile');
    const dl = await one.page.evaluate(() => ({ from: document.getElementById('owSkuFrom').textContent, input: document.getElementById('owSkuInput').value, to: document.getElementById('owSkuTo').dataset.s, toName: document.getElementById('owSkuToName').textContent, save: !document.getElementById('owSkuSave').disabled, chips: [...document.querySelectorAll('#owSkuScope .seChip')].map(c => [c.textContent, c.title]) }));
    assert.deepEqual([dl.from, dl.input, dl.to, dl.toName, dl.save], [OLD, LIKE, 'ok', LIKE, true], 'the tile is the new SKU: FROM the Etsy SKU → TO the tile, Save offered: ' + JSON.stringify(dl));
    // (what the change reaches, as the old confirming line counted it: Etsy listing 1819191541 only, its 4 open orders (A, B, C, D; X is another listing), GF Sheet 1, D's cut piece kept)
    assert.deepEqual(dl.chips.map(c => c[0]), [`Etsy ${LID}`, '4 orders', '1 sheet', '1 cut · kept'], 'the window says what the change reaches: ' + JSON.stringify(dl.chips));
    assert(/only its products with the SKU “TINY MOON & STARS 2” take the new one; other listings are not changed/.test(dl.chips[0][1]) && /GF Sheet 1/.test(dl.chips[2][1]) && /GF Sheet 2/.test(dl.chips[3][1]), 'this listing and its SKU only; the sheet and the cut piece named: ' + JSON.stringify(dl.chips));
    // the search reads the repo: the words typed narrow it; the charm matched now is marked and is not a tap
    await one.page.fill('#owSkuInput', 'tiny moon');
    await until(() => one.page.evaluate(OLD => !!document.querySelector(`#owSkuList [data-sku="${CSS.escape(OLD)}"]`), OLD), 5000, 'typing "tiny moon" lists the repo charms');
    const hits = await one.page.evaluate(() => [...document.querySelectorAll('#owSkuList [data-sku]')].map(b => [b.dataset.sku, b.disabled]));
    assert(hits.every(([k]) => /TINY/.test(k) && /MOON/.test(k)) && hits.some(([k, off]) => k === LIKE && !off) && hits.some(([k, off]) => k === OLD && off), 'only the repo SKUs with both words are listed; the charm matched now is marked, not a tap: ' + JSON.stringify(hits));
    assert(await one.page.evaluate(() => document.getElementById('owSkuSave').disabled), 'words alone are no pick: Save waits for a tile or the exact SKU');
    // Cancel closes the window, nothing written
    await one.page.click('#owSkuCancel');
    await until(async () => !(await dlgOpen(one.page)), 3000, 'Cancel closes the Change SKU window');
    await sleep(300);
    assert.equal(st.calls.slice(w0).filter(c => c.op === 'aliasPut' || c.op === 'optionMapPut').length, 0, 'Cancel: nothing is saved');
    assert.deepEqual(await one.page.evaluate(() => window.__etsy), [], 'Cancel: nothing is sent to Etsy');
    assert(!st.doc(ALIASES, LID), 'no link for the listing yet');
    pass('Suggest shows the Review card\'s similar charms as tiles and puts them away; a tile opens the Change SKU window (what it reaches, the repo search, the current charm marked); Cancel saves nothing');
    /* 3 · the pick: Change, the exact SKU pasted, Save (the yes: no confirming line); the listing's saved link written, Etsy written for this listing only, every open order of the listing re-made */
    await until(() => one.page.evaluate(() => !document.getElementById('owEditSku').disabled), 8000, 'Change offered');
    await one.page.click('#owEditSku');
    await until(() => dlgOpen(one.page), 5000, 'the Change SKU window');
    assert.equal(await one.page.evaluate(() => document.getElementById('owSkuInput').value), '', 'Change opens with nothing picked');
    await one.page.fill('#owSkuInput', NEW);
    await until(() => one.page.evaluate(() => !document.getElementById('owSkuSave').disabled && document.getElementById('owSkuTo').dataset.s === 'ok'), 5000, 'the pasted SKU is found in the repo: Save is offered');
    await one.page.click('#owSkuSave');
    await until(() => one.page.evaluate(() => !document.getElementById('owSkuProg').hidden && !!document.querySelector('#owSkuProg .rlFlow') && document.getElementById('owSkuForm').hidden), 8000, 'the window shows the progress of the change');
    await until(() => one.page.evaluate(() => !!document.querySelector('#owSkuProg .rlFlow[data-done="1"]')), 30000, () => one.page.evaluate(() => 'the progress says the change is completed: ' + JSON.stringify([...document.querySelectorAll('#owSkuProg li')].map(li => [li.dataset.s, li.textContent]))));
    const fl = await one.page.evaluate(() => { const el = document.querySelector('#owSkuProg .rlFlow'); return { title: el.querySelector('h3').textContent, step: el.querySelector('.rlStep').textContent, sum: el.querySelector('.rlSum').textContent, lines: [...el.querySelectorAll('li')].map(li => [li.dataset.s, li.querySelector('.rlT').textContent]), save: document.getElementById('owSkuSave').hidden, close: document.getElementById('owSkuCancel').textContent }; });
    assert(/^Edit SKU · listing 1819191541: TINY MOON & STARS 2 → MOON_3288/.test(fl.title) && /^Completed at \d\d:\d\d · 100%$/.test(fl.step) && /^MOON_3288 for listing 1819191541 · Etsy updated$/.test(fl.sum) && !fl.lines.some(([s]) => s === 'failed'), 'the window\'s progress names the change, is completed and sums it up, nothing failed: ' + JSON.stringify(fl));
    assert.deepEqual([fl.save, fl.close], [true, 'Close'], 'once completed: Save is gone, Close is offered');
    await one.page.click('#owSkuCancel');
    await until(async () => !(await dlgOpen(one.page)), 5000, 'Close closes the Change SKU window');
    assert.equal(await one.page.evaluate(() => window.__asked.length), 0, 'no confirming line: Save in the window is the yes');
    // (A's swap written as the sheet stands; GF Sheet 1 itself may stay "ready": B, made up at once by the SKU edit, waits there for the run's next nest, see below)
    await until(() => one.page.evaluate(A => { const s = CN.S.sheets.gold.pages.find(p => p.sheetId === 'wc-gold-1'); const c = s.charms.find(x => x.poolId === A + '_5100000001_1'); return c && c.sku === 'MOON_3288' && s.placements.some(pl => pl.id === c.id) && CN.S.sheets.gold.pages.every(p => !['nesting', 'finishing', 'queued'].includes(p.status) && !p._inplaceWrite); }, A), 30000,
      () => one.page.evaluate(() => 'A swapped on GF Sheet 1: ' + JSON.stringify({ rows: B.orders.rows.map(r => [r.order.receiptId, r.state, r.spec.designSku, r.poolIds, r.hold || null]), pool: [...B.pool.rows.values()].map(p => [p.poolId, p.sku, p.state, p.sheetId]), sheets: CN.S.sheets.gold.pages.map(p => [p.sheetId, p.status, p._inplaceWrite || null, p.charms.map(c => [c.poolId, c.sku, (c.orderInfo || {}).sku, p.placements.some(pl => pl.id === c.id)])]) })));
    await until(() => one.page.evaluate(C => { const s = Object.fromEntries(CN.S.sheets.gold.pages.map(p => [p.sheetId, p])); return Object.values(s).some(sh => sh.charms.some(c => c.poolId === C + '_5100000002_1' && c.sku === 'MOON_3288' && sh.placements.some(pl => pl.id === c.id))); }, C), 30000, 'C swapped');
    await sleep(600);
    const link = st.doc(ALIASES, LID);
    assert(link && link.bySku && link.bySku[OLD] === NEW, 'the saved link is the Review pick\'s: Charm_Sku_Aliases/' + LID + '.bySku["' + OLD + '"] = ' + NEW + ': ' + JSON.stringify(link));
    assert(!st.doc(ALIASES, OTHER), 'no link is written for the other listing');
    const puts = st.calls.slice(w0).filter(c => c.op === 'aliasPut');
    assert.deepEqual(puts.map(c => [c.body.listingId, c.body.fromSku, c.body.sku, c.body.by]), [[LID, OLD, NEW, 'Paul']], 'ONE aliasPut, keyed as the matcher keys it (listing + the SKU as bought): ' + JSON.stringify(puts.map(c => c.body)));
    const ets = await one.page.evaluate(() => window.__etsy);
    assert.deepEqual(ets.map(b => [b.listingId, b.sku, b.onlySku, b.by]), [[LID, NEW, OLD, 'Paul']], 'ONE Etsy write, for this listing only (not X\'s), and only for its products with the SKU as bought: ' + JSON.stringify(ets));
    assert.equal(st.calls.slice(w0).filter(c => c.op === 'editSkuEtsy' || (/etsy/i.test(String(c.name)) && !/get|read|list/i.test(String(c.op)))).length, 0, 'nothing reaches the real Etsy (the page\'s stand-in answered the write)');
    assert(fl.lines.some(([s, t]) => s === 'done' && /^Etsy listing 1819191541 updated: “TINY MOON & STARS 2” → MOON_3288/.test(t)), 'the Etsy line: ' + JSON.stringify(fl.lines));
    const rows1 = await rowsNow(one.page), sh1 = await sheetsNow(one.page);
    for (const rid of [A, C, B_, D]) assert.equal(rows1[rid].sku, NEW, `${rid} reads ${NEW} now: ${JSON.stringify(rows1[rid])}`);
    for (const rid of [A, C, B_]) assert.equal(rows1[rid].src, 'alias', `${rid} is read by the saved link (it beats the exact SKU match)`);
    assert.deepEqual([rows1[X].sku, rows1[X].src], [OLD, 'transaction'], 'the other listing with the same SKU still reads the exact master match');
    const idA = pid(A, 1), idC = pid(C, 2), idD = pid(D, 6), idX = pid(X, 3);
    assert.deepEqual([rows1[A].pieces, rows1[C].pieces], [[NEW], [NEW]], 'A\'s and C\'s pieces are MOON_3288');
    assert(sh1['wc-gold-1'].placed.includes(idA) && sh1['wc-gold-1'].charms.some(([id, sku]) => id === idA && sku === NEW), 'A is swapped on its own sheet, GF Sheet 1: ' + JSON.stringify(sh1['wc-gold-1']));
    assert.equal(sh1['wc-gold-1'].charms.filter(([id]) => id === idA).length, 1, 'one piece of A on the sheet, never two');
    pass('the pick (Save in the Change SKU window) is kept as the listing\'s saved link (listing + SKU), wins over the exact SKU match, and is written to Etsy for this listing only: A swapped on GF Sheet 1, C switched, B reads it');
    assert(st.doc(POOL, idX).sku === OLD && sh1['wc-gold-1'].placed.includes(idX) && sh1['wc-gold-1'].charms.some(([id, sku]) => id === idX && sku === OLD), 'X stays on GF Sheet 1 as the pair');
    assert.equal(st.calls.slice(w0).filter(c => /^(poolPut|poolUpdate)$/.test(String(c.op)) && JSON.stringify(c.body).includes(idX)).length, 0, 'no piece record of X is written');
    assert(st.doc(POOL, idD).sku === OLD && rows1[D].state === 'held' && /The listing's charm is now MOON_3288 \(picked by Paul\), but this piece was cut/.test(rows1[D].hold || ''), 'D (cut) is held for a person, its piece untouched: ' + rows1[D].hold);
    // (EDITSKU: Review.editSku also makes up AT ONCE every open line of the listing and SKU that has no piece yet (its makeUp, after fixCharm), so B is no longer only read: its piece is made from
    //  MOON_3288, never the old design, and joins the open GF Sheet 1 for the run's next nest (this run is manual: no nest starts here); nothing placed there moves for it)
    const idB = pid(B_, 9), bW = pieceWrites(w0, [idB]), recsOf = (o, out = []) => { if (o && typeof o === 'object') { if (o.poolId === idB && o.sku != null) out.push(o.sku); for (const v of Object.values(o)) recsOf(v, out); } return out; }, bSkus = bW.flatMap(c => recsOf(c.body));
    assert(bSkus.length >= 1 && bSkus.every(k => k === NEW) && rows1[B_].pieces.length === 1 && rows1[B_].pieces[0] === NEW && st.doc(POOL, idB) && st.doc(POOL, idB).sku === NEW, 'B (not made before) is made up at once, from MOON_3288 only: ' + JSON.stringify({ row: rows1[B_], skus: bSkus, writes: bW.map(c => [c.op, c.body]), doc: st.doc(POOL, idB) }));
    assert(fl.lines.some(([s, t]) => s === 'done' && new RegExp(`^Order ${B_}: now MOON_3288, going onto GF Sheet 1 \\(placed when it is next arranged\\)$`).test(t)), 'B has its line on the panel, saying its piece is not placed yet: ' + JSON.stringify(fl.lines));
    assert(sh1['wc-gold-1'].charms.some(([id, sku]) => id === idB && sku === NEW) && !sh1['wc-gold-2'].charms.some(([id]) => id === idB), 'B\'s piece joins the open GF Sheet 1, never the cut GF Sheet 2: ' + JSON.stringify(sh1));
    for (const id of [idA, idC, idX, pid(F(1), 4), pid(F(2), 5)]) assert(sh1['wc-gold-1'].placed.includes(id), `${id} is still placed on GF Sheet 1: ` + JSON.stringify(sh1['wc-gold-1']));
    pass('another listing with the same SKU is untouched; the cut piece is held for a person; the order not made yet is made up at once from the pick (Edit SKU) and joins the open GF Sheet 1, nothing placed there moving');
    // the window: the Change SKU window is put away and the SKU card reads the pick (cut as MOON_3288, linked from the Etsy SKU as bought)
    const win = await one.page.evaluate(() => ({ dlg: document.getElementById('owSkuDlg').open, open: OrderWin.isOpen(), sku: document.getElementById('owSku').textContent, state: document.getElementById('owSkuCard').dataset.state, via: document.getElementById('owSkuVia').hidden ? null : document.getElementById('owSkuVia').textContent }));
    assert(!win.dlg && win.open && win.sku === 'SKU: ' + NEW && win.state === 'linked' && String(win.via).includes(OLD), 'the Change SKU window is put away over the open order window, whose SKU card reads MOON_3288, linked from the Etsy SKU: ' + JSON.stringify(win));
    // the timeline: "Charm updated from listing pick by Paul"
    await until(async () => { await one.page.evaluate(() => { try { OrderTimeline.flush && OrderTimeline.flush(); } catch (_) {} }); return [A, C, B_, D].every(rid => st.list(TL).some(e => e._id.startsWith(rid + '~') && /^Charm updated from listing pick by Paul/.test(e.text || ''))); }, 20000, 'timeline events');
    assert.equal(st.list(TL).filter(e => e._id.startsWith(X + '~') && /listing pick/.test(e.text || '')).length, 0, 'X has no such event');
    pass('every order of the listing has "Charm updated from listing pick by Paul" on its timeline; the window reads the pick');
    await one.ctx.close();
    /* 4 · a reload: the saved link is read again; the listing keeps MOON_3288, the other listing its exact match; the Orders card's "Wrong charm?" opens the window with its suggestions (OrderWin.charmFix(true)) */
    const two = await openPage('Paul');
    await setup(two.page, { sheets: false, pieces: false });
    const rows2 = await rowsNow(two.page);
    for (const rid of [A, C, B_, D]) assert.deepEqual([rows2[rid].sku, rows2[rid].src], [NEW, 'alias'], `after the reload ${rid} reads ${NEW} by the saved link`);
    assert.deepEqual([rows2[X].sku, rows2[X].src], [OLD, 'transaction'], 'after the reload X still reads the exact match');
    await signIn(two.page);
    await until(() => two.page.evaluate(A => { const r = B.orders.rows.find(x => x.order.receiptId === A); const n = document.querySelector(`[data-key="${CSS.escape(r.key)}"] .comparePair figcaption .cfOpen`); return !!n; }, A), 15000, 'the Orders card\'s Wrong charm?');
    const cardTitle = await two.page.evaluate(A => { const r = B.orders.rows.find(x => x.order.receiptId === A); const b = document.querySelector(`[data-key="${CSS.escape(r.key)}"] .cfOpen`); const t = b.title; b.click(); return t; }, A);
    assert(cardTitle.includes(NEW), 'the Orders card names the charm the listing reads now: ' + cardTitle);
    await until(() => two.page.evaluate(() => OrderWin.isOpen() && !!document.getElementById('owSkuCard').dataset.state && !document.getElementById('owSkuTray').hidden && !!document.querySelector('#owSkuTray [data-sku]')), 15000,
      () => two.page.evaluate(() => 'the window opened with its suggestions: ' + JSON.stringify({ open: OrderWin.isOpen(), state: document.getElementById('owSkuCard').dataset.state, sku: document.getElementById('owSku').textContent, tray: document.getElementById('owSkuTray').hidden, n: document.getElementById('owSuggestN').textContent, suggest: [document.getElementById('owSuggest').disabled, document.getElementById('owSuggest').title] })));
    const cur2 = await two.page.evaluate(() => ({ sku: document.getElementById('owSku').textContent, state: document.getElementById('owSkuCard').dataset.state, expanded: document.getElementById('owSuggest').getAttribute('aria-expanded'), tiles: [...document.querySelectorAll('#owSkuTray [data-sku]')].map(b => b.dataset.sku) }));
    assert.deepEqual([cur2.sku, cur2.state, cur2.expanded, cur2.tiles.includes(NEW)], ['SKU: ' + NEW, 'linked', 'true', false], 'the SKU card reads MOON_3288 as the charm matched now (linked from the Etsy SKU), its suggestions open, MOON_3288 not a tile: ' + JSON.stringify(cur2));
    pass('a reload keeps MOON_3288 for the listing (and only it); the Orders card\'s "Wrong charm?" opens the order window with its suggestions');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`wrong-charm-browser: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
