// WRONGCHARM, the page (Paul, 18 Oct 2026; order 4193072538, Etsy listing 1819191541 "Tiny Moon Charm Celestial Charm … Add On Charm", GF): the listing's Etsy SKU "TINY MOON & STARS 2" is an exact
// master SKU (a star + moon pair), so the line was matched and nested with no question; it is the library's MOON_3288. A line whose SKU matched never reaches Review, so "Wrong charm?" on the
// order's piece (the order window's Vector design, the Orders card's) opens a picker (the Review card's similar charms + the master search); the pick, confirmed in one line, is the SAME saved
// link a Review pick writes (CharmNestReviewLink.pick: Charm_Sku_Aliases/{listing}.bySku[the SKU as bought]), which beats the exact master match for THIS listing only, and LISTPICK re-makes
// every open order of the listing. Real headless Chromium over the repo's fake site (bridge-server.cjs: the REAL charmNestLibrary handler on an in-memory Firestore; nothing leaves the machine).
// The only stand-in is the nest (the solver), as in listing-pick-relink-browser.cjs.
//   A 4193072538  Paul's order, on the open GF Sheet 1                 → "Wrong charm?" → MOON_3288 from the library search → swapped on GF Sheet 1
//   C 4193072539  the same listing, on GF Sheet 1                       → switches too
//   B 4193072540  the same listing, not pooled yet                      → reads MOON_3288, nothing written for it
//   D 4193072541  the same listing, CUT on GF Sheet 2                   → flagged and held for a person, nothing physical changes
//   X 4193079999  ANOTHER listing with the same SKU, on GF Sheet 1      → untouched
//   then a reload (a fresh page over the same cloud): the listing reads MOON_3288, X still the pair; the Orders card's "Wrong charm?" opens the window with the picker
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
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(150); } };
  const signIn = page => page.evaluate(() => { if (!B.employee) B.employee = 'Paul'; window.__asked = []; window.confirm = m => { window.__asked.push(m); return true; }; return B.employee; });
  const openWin = async (page, rid) => {
    await page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); OrderWin.open(r.key); }, rid);
    await until(() => page.evaluate(() => OrderWin.isOpen() && !!document.getElementById('owWrongCharm')), 15000, 'the order window');
  };
  try {
    const one = await openPage('Paul');
    const read0 = await setup(one.page, { sheets: true, pieces: true });
    assert(read0.filter(r => [A, C, B_, D, X].includes(r[0])).every(r => r[1] === OLD && r[2] === 'transaction'), 'before the correction the five lines read as the exact master SKU: ' + JSON.stringify(read0));
    assert.equal(await signIn(one.page), 'Paul', 'Paul is signed in');
    const w0 = st.calls.length;
    /* 1 · the button: on a matched line only */
    await openWin(one.page, A);
    const btn = await one.page.evaluate(() => { const b = document.getElementById('owWrongCharm'); return { shown: !b.hidden && b.offsetParent !== null, text: b.textContent, title: b.title, picker: document.getElementById('owCharmFix').hidden }; });
    assert.deepEqual([btn.shown, btn.text, btn.picker], [true, 'Wrong charm?', true], 'the order window shows "Wrong charm?" for the matched line, its picker shut: ' + JSON.stringify(btn));
    assert(btn.title.includes(LID) && btn.title.includes(OLD), 'its title names the listing and the charm matched now: ' + btn.title);
    // a line that still asks which charm it is is answered on its Review card: no button
    const asks = await one.page.evaluate(() => { const r = B.orders.rows[0], sp = Object.assign({}, r.spec, { problems: [{ kind: 'unmatchedSku' }] }); return Review.fixOffer(Object.assign({}, r, { spec: sp })).ok; });
    assert.equal(asks, false, 'a line with an unmatched SKU question is not offered the correction');
    pass('"Wrong charm?" stands beside the SKU under the Vector design of a matched line (never one that still asks)');
    /* 2 · the picker: the Review card's similar charms and the library search */
    await one.page.click('#owWrongCharm');
    await until(() => one.page.evaluate(() => !document.getElementById('owCharmFix').hidden && !!document.querySelector('#owCharmFix [data-f=dsku]')), 5000, 'the picker');
    const pk = await one.page.evaluate(() => { const h = document.getElementById('owCharmFix'); return { chips: [...h.querySelectorAll('[data-dsku]')].map(b => b.dataset.dsku), cur: [...h.querySelectorAll('[data-cur]')].map(b => b.dataset.cur), ask: h.querySelector('.sibAsk').textContent, note: h.querySelector('.dsFor:last-child').textContent, expanded: document.getElementById('owWrongCharm').getAttribute('aria-expanded'), list: h.querySelector('[data-f=dsku]').getAttribute('list') }; });
    assert(pk.chips.includes(LIKE) && !pk.chips.includes(OLD), 'the similar charms of the Review card are offered (one tap each), the current one is not a tap: ' + JSON.stringify(pk));
    assert.deepEqual([pk.cur, pk.expanded, pk.list], [[OLD], 'true', 'cnMasterSkus'], 'the charm matched now is marked; the search reads the master index: ' + JSON.stringify(pk));
    assert(/Kept for listing 1819191541 and the SKU “TINY MOON & STARS 2” only/.test(pk.note) && /Etsy is not changed/.test(pk.note), 'the picker says what the pick reaches: ' + pk.note);
    if (process.env.CN_SHOT) await one.page.locator('#orderWin .owPics').screenshot({ path: path.join(process.env.CN_SHOT, 'wrong-charm-picker.png') }).catch(() => {});   // (a look, only when asked: CN_SHOT=<dir>)
    // Cancel puts it away, nothing written
    await one.page.click('#owCharmFix [data-a=close]');
    assert.equal(await one.page.evaluate(() => document.getElementById('owCharmFix').hidden), true, 'Cancel puts the picker away');
    // a "no" to the confirming line writes nothing
    await one.page.click('#owWrongCharm');
    await one.page.evaluate(() => { window.confirm = m => { window.__asked.push(m); return false; }; });
    await one.page.fill('#owCharmFix [data-f=dsku]', NEW);
    await one.page.click('#owCharmFix [data-a=dsku]');
    await until(() => one.page.evaluate(() => window.__asked.length === 1), 5000, 'the confirming line');
    await sleep(300);
    assert.equal(st.calls.slice(w0).filter(c => c.op === 'aliasPut' || c.op === 'optionMapPut').length, 0, 'answered "no": nothing is saved');
    assert(!st.doc(ALIASES, LID), 'no link for the listing yet');
    pass('the picker offers the Review card\'s similar charms and the master search; Cancel and a "no" save nothing');
    /* 3 · the pick: confirmed in one line, the listing's saved link written, every open order of the listing re-made */
    await one.page.evaluate(() => { window.__asked = []; window.confirm = m => { window.__asked.push(m); return true; }; });
    await one.page.fill('#owCharmFix [data-f=dsku]', NEW);
    await one.page.click('#owCharmFix [data-a=dsku]');
    await until(() => one.page.evaluate(() => window.__asked.length === 1), 5000, 'the confirming line');
    const asked = await one.page.evaluate(() => window.__asked[0]);
    assert(asked.startsWith(`Use ${NEW} for every order of listing ${LID}? 4 open orders of it change (3 already made`), 'one confirming line with the count of the listing\'s open orders (A, B, C, D; X is another listing): ' + asked);
    await until(() => one.page.evaluate(A => { const s = CN.S.sheets.gold.pages.find(p => p.sheetId === 'wc-gold-1'); const c = s.charms.find(x => x.poolId === A + '_5100000001_1'); return c && c.sku === 'MOON_3288' && s.placements.some(pl => pl.id === c.id) && CN.S.sheets.gold.pages.every(p => !['nesting', 'queued', 'ready'].includes(p.status)); }, A), 30000, 'A swapped on GF Sheet 1');
    await until(() => one.page.evaluate(C => { const s = Object.fromEntries(CN.S.sheets.gold.pages.map(p => [p.sheetId, p])); return Object.values(s).some(sh => sh.charms.some(c => c.poolId === C + '_5100000002_1' && c.sku === 'MOON_3288' && sh.placements.some(pl => pl.id === c.id))); }, C), 30000, 'C swapped');
    await sleep(600);
    const link = st.doc(ALIASES, LID);
    assert(link && link.bySku && link.bySku[OLD] === NEW, 'the saved link is the Review pick\'s: Charm_Sku_Aliases/' + LID + '.bySku["' + OLD + '"] = ' + NEW + ': ' + JSON.stringify(link));
    assert(!st.doc(ALIASES, OTHER), 'no link is written for the other listing');
    const puts = st.calls.slice(w0).filter(c => c.op === 'aliasPut');
    assert.deepEqual(puts.map(c => [c.body.listingId, c.body.fromSku, c.body.sku, c.body.by]), [[LID, OLD, NEW, 'Paul']], 'ONE aliasPut, keyed as the matcher keys it (listing + the SKU as bought): ' + JSON.stringify(puts.map(c => c.body)));
    assert.equal(st.calls.slice(w0).filter(c => /etsy/i.test(String(c.name)) && !/get|read|list/i.test(String(c.op))).length, 0, 'Etsy is not changed');
    const rows1 = await rowsNow(one.page), sh1 = await sheetsNow(one.page);
    for (const rid of [A, C, B_, D]) assert.equal(rows1[rid].sku, NEW, `${rid} reads ${NEW} now: ${JSON.stringify(rows1[rid])}`);
    for (const rid of [A, C, B_]) assert.equal(rows1[rid].src, 'alias', `${rid} is read by the saved link (it beats the exact SKU match)`);
    assert.deepEqual([rows1[X].sku, rows1[X].src], [OLD, 'transaction'], 'the other listing with the same SKU still reads the exact master match');
    const idA = pid(A, 1), idC = pid(C, 2), idD = pid(D, 6), idX = pid(X, 3);
    assert.deepEqual([rows1[A].pieces, rows1[C].pieces], [[NEW], [NEW]], 'A\'s and C\'s pieces are MOON_3288');
    assert(sh1['wc-gold-1'].placed.includes(idA) && sh1['wc-gold-1'].charms.some(([id, sku]) => id === idA && sku === NEW), 'A is swapped on its own sheet, GF Sheet 1: ' + JSON.stringify(sh1['wc-gold-1']));
    assert.equal(sh1['wc-gold-1'].charms.filter(([id]) => id === idA).length, 1, 'one piece of A on the sheet, never two');
    pass('the pick is confirmed in one line, kept as the listing\'s saved link (listing + SKU), and wins over the exact SKU match: A swapped on GF Sheet 1, C switched, B reads it');
    assert(st.doc(POOL, idX).sku === OLD && sh1['wc-gold-1'].placed.includes(idX) && sh1['wc-gold-1'].charms.some(([id, sku]) => id === idX && sku === OLD), 'X stays on GF Sheet 1 as the pair');
    assert.equal(st.calls.slice(w0).filter(c => /^(poolPut|poolUpdate)$/.test(String(c.op)) && JSON.stringify(c.body).includes(idX)).length, 0, 'no piece record of X is written');
    assert(st.doc(POOL, idD).sku === OLD && rows1[D].state === 'held' && /The listing's charm is now MOON_3288 \(picked by Paul\), but this piece was cut/.test(rows1[D].hold || ''), 'D (cut) is held for a person, its piece untouched: ' + rows1[D].hold);
    assert.equal(st.calls.slice(w0).filter(c => /^(poolPut|poolUpdate|putSheet)$/.test(String(c.op)) && JSON.stringify(c.body).includes(B_)).length, 0, 'nothing is written for B (not made yet)');
    pass('another listing with the same SKU is untouched; the cut piece is held for a person; the order not made yet just reads the pick');
    // the window: the picker is put away and the window reads the pick
    const win = await one.page.evaluate(() => ({ picker: document.getElementById('owCharmFix').hidden, sku: document.getElementById('owSku').textContent, title: document.getElementById('owWrongCharm').title }));
    assert(win.picker && win.sku === 'SKU: ' + NEW && win.title.includes(NEW), 'the picker is put away and the window reads MOON_3288: ' + JSON.stringify(win));
    // the timeline: "Charm updated from listing pick by Paul"
    await until(async () => { await one.page.evaluate(() => { try { OrderTimeline.flush && OrderTimeline.flush(); } catch (_) {} }); return [A, C, B_, D].every(rid => st.list(TL).some(e => e._id.startsWith(rid + '~') && /^Charm updated from listing pick by Paul/.test(e.text || ''))); }, 20000, 'timeline events');
    assert.equal(st.list(TL).filter(e => e._id.startsWith(X + '~') && /listing pick/.test(e.text || '')).length, 0, 'X has no such event');
    pass('every order of the listing has "Charm updated from listing pick by Paul" on its timeline; the window reads the pick');
    await one.ctx.close();
    /* 4 · a reload: the saved link is read again; the listing keeps MOON_3288, the other listing its exact match; the Orders card's "Wrong charm?" opens the window with the picker */
    const two = await openPage('Paul');
    await setup(two.page, { sheets: false, pieces: false });
    const rows2 = await rowsNow(two.page);
    for (const rid of [A, C, B_, D]) assert.deepEqual([rows2[rid].sku, rows2[rid].src], [NEW, 'alias'], `after the reload ${rid} reads ${NEW} by the saved link`);
    assert.deepEqual([rows2[X].sku, rows2[X].src], [OLD, 'transaction'], 'after the reload X still reads the exact match');
    await signIn(two.page);
    await until(() => two.page.evaluate(A => { const r = B.orders.rows.find(x => x.order.receiptId === A); const n = document.querySelector(`[data-key="${CSS.escape(r.key)}"] .comparePair figcaption .cfOpen`); return !!n; }, A), 15000, 'the Orders card\'s Wrong charm?');
    const cardTitle = await two.page.evaluate(A => { const r = B.orders.rows.find(x => x.order.receiptId === A); const b = document.querySelector(`[data-key="${CSS.escape(r.key)}"] .cfOpen`); const t = b.title; b.click(); return t; }, A);
    assert(cardTitle.includes(NEW), 'the Orders card names the charm the listing reads now: ' + cardTitle);
    await until(() => two.page.evaluate(() => OrderWin.isOpen() && !document.getElementById('owCharmFix').hidden && !!document.querySelector('#owCharmFix [data-f=dsku]')), 15000, 'the window opened with its picker');
    const cur2 = await two.page.evaluate(() => [...document.querySelectorAll('#owCharmFix [data-cur]')].map(b => b.dataset.cur));
    assert.deepEqual(cur2, [NEW], 'the picker marks MOON_3288 as the charm matched now');
    pass('a reload keeps MOON_3288 for the listing (and only it); the Orders card\'s "Wrong charm?" opens the order window with the picker');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`wrong-charm-browser: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
