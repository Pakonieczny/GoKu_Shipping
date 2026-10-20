// REVDROP, the page (Paul, 19 Oct 2026: "All orders being held inside the Review tab need to have the Drag and Drop option, so that a user can overwrite the currently matched charm with a new charm";
// with NOMARKOK "Remove the 'Mark OK' button and function ... we already have the 'Release Hold' button" and NOSHEETBTN "remove the 'Open Sheet' button"). Real headless Chromium over the repo's fake site
// (bridge-server.cjs: the REAL charmNestLibrary handler on an in-memory Firestore; every request off the loopback is aborted, nothing leaves the machine). The only stand-in is the nest (the solver), as in
// listing-pick-relink-browser.cjs: what is placed stays where it is, waiting pieces go into the next free cell, the sheet is saved through the page's own putSheet.
//   A 4205500001  sent to Review (CharmNestReviewMark.mark); its ONE line pooled and placed (FEATHER) on the OPEN draft GF Sheet 1 (rd-gold-1)
//       → its Review row takes designs: the drop hint (.cuHint), the row's class cuDropOk; no "Mark OK", no "Open sheet"; Release Hold is there
//       → a .dxf design dropped on that row (a real DragEvent with a DataTransfer, the page's own drop path), its metal picked in the designs window, Send to Sheet on the row:
//         the old FEATHER piece comes off GF Sheet 1 (one take-off commit: superseded), the line is made up again from the dropped design (custom pieces), nothing is placed twice
//   K 4205500002  sent to Review; its piece is on GF Sheet 2, CUT (laserDoneAt)           → no drop hint, no cuDropOk; a drop on it adds nothing; its piece untouched
//   M 4205500003  sent to Review; its piece is on GF Sheet 3, in a COMMITTED set (Set 1)  → the same
//   F 4205500091  an order on GF Sheet 1 that is not in Review                             → its piece stays where it is
//   node tests/charm-nest/review-drop-replace.cjs [playwright-core dir]   (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-rdrop-1', SET_ID = 'set-rdrop-1';
const OLD = 'FEATHER', FILE = 'replace-tag.dxf';
const SHIP = Math.floor(Date.UTC(2026, 9, 24, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 9, 16, 12) / 1000);
const A = '4205500001', K = '4205500002', M = '4205500003', F = '4205500091';
const TID = tx => String(5200000000 + tx), pid = (rid, tx) => `${rid}_${TID(tx)}_1`;
// [rid, tx, listing, sku, age (s), sheet page, cell]
const LINES = [[A, 1, '1700500001', OLD, 9000, 1, 0], [F, 4, '1700500091', 'GFFILL_1', 8000, 1, 1], [K, 2, '1700500002', OLD, 9500, 2, 0], [M, 3, '1700500003', OLD, 9400, 3, 0]];
const PAGES = { 1: { id: 'rd-gold-1', draft: true }, 2: { id: 'rd-gold-2', cut: true }, 3: { id: 'rd-gold-3', setId: SET_ID } };
const idA = pid(A, 1), idK = pid(K, 2), idM = pid(M, 3), idF = pid(F, 4);
// the customer's own design: a 9 mm round tag with a 1.6 mm hole, as a .dxf in millimetres
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DESIGN_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'CIRCLE', 8, 'CUT', 10, 0, 20, 0, 40, 4.5, 0, 'CIRCLE', 8, 'CUT', 10, 0, 20, 3, 40, 0.8, 0, 'ENDSEC', 0, 'EOF');
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const srv = await start({ receipts: [] }), st = srv.st;
  for (const sku of [OLD, 'GFFILL_1']) st.put('Charm_Master_Index', sku, { sku, aiPath: 'test/' + sku + '.ai', widthPt: 28.35, heightPt: 28.35, holes: 0 }, false);
  // the cloud: the sheets as saved and the piece records
  for (const [pg, sh] of Object.entries(PAGES)) {
    const mine = LINES.filter(l => l[5] === +pg);
    st.put(SHEETS, sh.id, Object.assign({ id: sh.id, metal: 'gold', charms: mine.map((l, i) => ({ id: `${sh.id}-c${i}`, poolId: pid(l[0], l[1]), order: l[0], sku: l[3] })), placements: mine.map((l, i) => ({ id: `${sh.id}-c${i}`, cxPt: 16 + l[6] * 30, cyPt: 16, angle: 0 })), poolIds: mine.map(l => pid(l[0], l[1])), sheetIndex: +pg, fileBase: 'GF Sheet ' + pg, runId: RUN },
      sh.cut ? { laserDoneAt: Date.now() - 3600000 } : {}, sh.setId ? { setId: sh.setId } : {}, sh.draft ? { draft: true } : {}), false);
    for (const l of mine) st.put(POOL, pid(l[0], l[1]), { poolId: pid(l[0], l[1]), orderId: l[0], transactionId: TID(l[1]), lineKey: `${l[0]}_${TID(l[1])}`, sku: l[3], copy: 1, quantity: 1, sheetId: sh.id, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 }, false);
  }
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  const errors = [];
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  await ctx.route(u => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: "window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title>'], { type: 'text/html' })); } }; } };" });
    if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: '' });
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  await ctx.addInitScript(() => {
    try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', 'Paul'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
    window.prompt = () => 'Paul'; window.confirm = () => true; window.alert = () => {};
    // the nest (as listing-pick-relink-browser.cjs): placed pieces stay; waiting pieces go into the next free cell, oldest order first; the rest moves on (the page's own overflowToNextSheet); the sheet is saved
    const iv = setInterval(() => {
      if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
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
            if (sh.sheetId) await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: sh.draft || undefined, setId: sh.setId || undefined, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: c.sku, custom: c.custom || undefined })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true });
            sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.stage = ''; sh.persistedDone = true; sh.verification = { ok: true };
            if (left.length) { sh.rejects = left.map(c => c.id); overflowToNextSheet(sh, false); }
          } catch (e) { sh.problem = e.message; sh.status = 'complete'; sh.persistedDone = true; }
          try { CN.renderCard(sh); } catch (_) {}
        }, 120);
      };
      stub.__stub = true; window.startNest = stub; clearInterval(iv);
    }, 0);
  });
  const page = await ctx.newPage(); page.setDefaultTimeout(30000);
  page.on('pageerror', e => errors.push(String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(150); } };
  /** everything about a line, the pieces it has and the sheets: printed when a step fails */
  const dump = (rid) => page.evaluate(rid => {
    const r = B.orders.rows.find(x => x.order.receiptId === rid), ck = r && Review.cardKey(r), e = ck && CustomSheet && (window.B.customDesigns || {})[ck];
    return JSON.stringify({
      row: r && { state: r.state, reason: r.reason, hold: r.hold, poolIds: r.poolIds, repoolChanged: !!r.repoolChanged, changePending: !!r.changePending, decided: !!CustomSheet.decisionOf(r), marked: CharmNestReviewMark.isMarked(rid), keepsPlace: !!(Pool.keepsPlace && Pool.keepsPlace(r)) },
      intent: e && (e.sendIntent || e.sent) ? { replace: (e.sendIntent || e.sent).replace, replaced: (e.sendIntent || e.sent).replaced, lines: (e.sendIntent || e.sent).lines, sent: !!e.sent, error: e.sendError || null } : null,
      pool: [...B.pool.rows.values()].filter(p => p.orderId === rid || String(p.poolId).startsWith(rid)).map(p => ({ id: p.poolId, state: p.state, sku: p.sku, sheetId: p.sheetId, custom: !!p.custom, file: p.customFile || null })),
      sheets: CN.S.sheets.gold.pages.map(p => ({ id: p.sheetId, status: p.status, charms: p.charms.map(c => [c.poolId, c.sku || null, !!c.custom, p.placements.some(pl => pl.id === c.id)]) })),
      toasts: [...document.querySelectorAll('#toasts .toast')].map(t => t.textContent).slice(-6)
    });
  }, rid);
  try {
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && window.Review && window.CustomSheet && window.CharmNestReviewMark && window.Pool && window.startNest && window.startNest.__stub && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    await page.evaluate(() => { const P = window.CharmNestPDF; if (P) { P.drawCharm = () => {}; P.cutLinesOf = () => []; P.pathToCanvas = () => {}; } }); // (the world's charms carry a stand-in outline, no subpaths)
    /* ── the world: three gold sheets (open draft, cut, in a committed set), the lines pooled on them, the run ── */
    await page.evaluate(async ({ LINES, PAGES, RUN, SET_ID, BASE_TS, SHIP }) => {
      await Orders.loadMaps(true);
      CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72] });
      const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, TID = tx => String(5200000000 + tx), pid = (rid, tx) => `${rid}_${TID(tx)}_1`;
      const mkCharm = (id, extra) => Object.assign({ id, name: id, sku: '', sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: 0, cy: 0, r: R * MM }, members: [], rMm: R, w: 8, h: 8, scale: 1, bits: new Uint8Array(64).fill(1), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2 }, extra || {});
      const regSku = sku => { const key = 'test/' + sku + '.ai'; B.master.entries.set(sku, { sku, updatedAt: 1, aiPath: key, widthPt: 2 * R * MM, heightPt: 2 * R * MM, holes: 0, upAngle: null, labelSource: 'text', charmHash: 'h-' + sku }); B.pool.sources.set(key, { id: 'pool:' + sku, pool: true, name: sku, sku, charms: [mkCharm('pool:' + sku + ':0', { sku, name: sku })], usedAt: Date.now() }); };
      for (const sku of ['FEATHER', 'GFFILL_1']) regSku(sku);
      const pr = CN.S.sheets.gold; pr.pages.length = 1; const p0 = pr.pages[0]; Object.assign(p0, { charms: [], placements: [], sheetId: null, fileBase: null, status: 'idle', page: 1 }); for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'releaseFull', 'draft']) delete p0[k];
      B.pool.rows.clear(); B.sets.clear();
      for (const [pg, sh] of Object.entries(PAGES)) {
        while (pr.pages.length < +pg) addPage('gold');
        const page = pr.pages[+pg - 1]; page.charms = []; page.placements = [];
        LINES.filter(l => l[5] === +pg).forEach((l, i) => {
          const id = `${sh.id}-c${i}`, poolId = pid(l[0], l[1]);
          page.charms.push(mkCharm(id, { name: `${l[0]} · ${l[3]}`, sku: l[3], poolId, order: l[0], lineKey: `${l[0]}_${TID(l[1])}`, orderDate: BASE_TS - l[4], orderInfo: { receiptId: l[0], transactionId: TID(l[1]), sku: l[3], copy: 1, quantity: 1 } }));
          page.placements.push({ id, cxPt: 16 + l[6] * 30, cyPt: 16, angle: 0, wPt: 2 * R * MM, hPt: 2 * R * MM });
        });
        Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: 'GF Sheet ' + pg, sheetIndex: +pg, page: +pg, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false });
        if (sh.cut) page.laserDoneAt = Date.now() - 3600000;
        if (sh.draft) page.draft = true;
        if (sh.setId) page.setId = sh.setId;
        CN.renderCard(page);
      }
      // GF Sheet 3 is in Set 1, committed (cut and on its way): nothing on it is changed
      B.sets.set(`${RUN}|dispatch`, { setId: SET_ID, runId: RUN, seq: 1, day: new Date().toISOString().slice(0, 10), group: 'dispatch', name: 'Set 1', committedAt: Date.now() - 1800000, savedCommit: Date.now() - 1800000, sheetIds: [PAGES[3].id], materials: ['gold'], labelFiles: [], orders: {}, status: 'committed' });
      const rows = [];
      for (const l of LINES) {
        const [rid, tx, listingId, sku, age, pg] = l;
        const line = { transactionId: TID(tx), listingId, sku, title: sku === 'FEATHER' ? 'Feather Necklace Gold Dainty Feather Charm' : sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' };
        const order = { receiptId: rid, orderNumber: rid, createTs: BASE_TS - age, updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Name ' + rid.slice(-2) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
        rows.push({ key: CharmNestOrders.lineKey(order, line), order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [pid(rid, tx)], engrave: null, material: 'gold' });
        B.pool.rows.set(pid(rid, tx), { poolId: pid(rid, tx), orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sku, copy: 1, quantity: 1, sheetId: PAGES[pg].id, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 });
      }
      B.orders.rows.length = 0; B.orders.byKey.clear(); for (const r of rows) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); }
      B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [...new Set(rows.map(r => r.order.receiptId))] };
      Orders.interpretAll(); Review.syncOrderItems();
    }, { LINES, PAGES, RUN, SET_ID, BASE_TS, SHIP });
    // A, K and M are sent to Review (the page's own REVMARK call); their pieces keep their place (REVHOLD)
    const marked = await page.evaluate(async rids => { const out = {}; for (const rid of rids) { const m = await CharmNestReviewMark.mark(rid, { by: 'Paul' }); out[rid] = [m.ok, Pool.keepsPlace(B.orders.rows.find(r => r.order.receiptId === rid))]; } Review.syncOrderItems(); CN.setMode('review'); Review.render(); return out; }, [A, K, M]);
    assert.deepEqual(marked, { [A]: [true, true], [K]: [true, true], [M]: [true, true] }, 'A, K and M are in Review, their pieces kept on their sheets: ' + JSON.stringify(marked));
    /** the Review row of an order sent to Review (the Marked one when the order has several), as the person sees it */
    const rowOf = rid => page.evaluate(rid => {
      const all = [...document.querySelectorAll('#rvList > .reviewListRow')].filter(n => n.dataset.rid === rid), n = all.find(x => x.querySelector('.rvMarkSlot')) || all[0];
      if (!n) return null;
      [...document.querySelectorAll('#rvList > .reviewListRow[data-test-row]')].forEach(x => x.removeAttribute('data-test-row')); n.setAttribute('data-test-row', rid);
      return { n: all.length, cls: n.className, label: (n.querySelector('.purchaseLabel') || {}).textContent || '', hint: !!n.querySelector('.cuHint'), hintText: (n.querySelector('.cuHint') || {}).textContent || '', dropOk: n.classList.contains('cuDropOk'),
        markOk: n.querySelectorAll('[data-rv-markok], [data-a="markok"]').length, sheetBtn: n.querySelectorAll('[data-cu-sheet]').length, release: n.querySelectorAll('[data-rel-hold]').length, send: (n.querySelector('[data-cu-send]') || {}).textContent || null,
        buttons: [...n.querySelectorAll('button')].map(b => b.textContent.trim()).filter(Boolean) };
    }, rid);
    await until(async () => { const a = await rowOf(A), k = await rowOf(K), m = await rowOf(M); return a && k && m && a.release && k.release && m.release; }, 15000, 'the three Review rows with their Release Hold').catch(async e => { throw new Error(e.message + ' ' + JSON.stringify([await rowOf(A), await rowOf(K), await rowOf(M)])); });

    /* ── A · the held order on an open sheet takes designs; no Mark OK, no Open sheet; Release Hold ── */
    const rA = await rowOf(A);
    assert.equal(rA.hint, true, 'A\'s Review row shows the drop hint: ' + JSON.stringify(rA));
    assert.match(rA.hintText, /Drop \.ai \/ \.dxf designs here/, 'in words');
    assert.equal(rA.dropOk, true, 'A\'s row takes dropped files (cuDropOk): ' + rA.cls);
    assert.equal(rA.send, 'Send to Sheet', 'and has Send to Sheet: ' + JSON.stringify(rA.buttons));
    assert.equal(rA.markOk, 0, 'no Mark OK (NOMARKOK): ' + JSON.stringify(rA.buttons));
    assert(!rA.buttons.some(b => /^Mark OK$/i.test(b)), 'no button reads Mark OK: ' + JSON.stringify(rA.buttons));
    assert.equal(rA.sheetBtn, 0, 'no Open sheet (NOSHEETBTN): ' + JSON.stringify(rA.buttons));
    assert(!rA.buttons.some(b => /^Open sheet$/i.test(b)), 'no button reads Open sheet: ' + JSON.stringify(rA.buttons));
    assert.equal(rA.release, 1, 'Release Hold is on the row: ' + JSON.stringify(rA.buttons));
    // (the decision card for the mark has no Mark OK either: CharmNestReviewMark.resolve is only reached through Release Hold)
    assert.equal(await page.evaluate(() => document.querySelectorAll('[data-rv-markok], [data-a="markok"]').length), 0, 'no Mark OK anywhere on the page');
    pass('A (in Review, its piece on the open draft GF Sheet 1): the drop hint, cuDropOk and Send to Sheet; no Mark OK, no Open sheet; Release Hold');

    /* ── C · the held orders whose piece is cut, or in a committed set: no drop ── */
    for (const [rid, what] of [[K, 'cut GF Sheet 2'], [M, 'GF Sheet 3 in the committed Set 1']]) {
      const r = await rowOf(rid);
      assert.equal(r.hint, false, `${rid} (on the ${what}) shows no drop hint: ${JSON.stringify(r)}`);
      assert.equal(r.dropOk, false, `${rid}'s row takes no files (no cuDropOk): ${r.cls}`);
      assert.equal(r.markOk + r.sheetBtn, 0, `${rid}: no Mark OK, no Open sheet: ${JSON.stringify(r.buttons)}`);
      assert.equal(r.release, 1, `${rid}: Release Hold: ${JSON.stringify(r.buttons)}`);
    }
    const dropRaw = (rid, files) => page.evaluate(({ rid, files }) => {
      const dt = new DataTransfer();
      for (const f of files) dt.items.add(new File([f.text], f.name));
      const n = [...document.querySelectorAll('#rvList > .reviewListRow')].find(x => x.getAttribute('data-test-row') === rid) || document.querySelector(`#rvList > .reviewListRow[data-rid="${rid}"]`);
      const t = n.querySelector('.engravingIdentity') || n;
      for (const ty of ['dragenter', 'dragover', 'drop']) t.dispatchEvent(new DragEvent(ty, { bubbles: true, cancelable: true, dataTransfer: dt }));
    }, { rid, files });
    const answer = which => page.evaluate(w => { const b = document.querySelector('#cuAsk [data-ask-' + w + ']'); if (b) b.click(); return !!b; }, which);
    const w0 = st.calls.length;
    // a drop on K's row adds nothing to it (no question, no designs window)
    await rowOf(K); await dropRaw(K, [{ name: FILE, text: DESIGN_DXF }]); await sleep(500);
    { const k = await page.evaluate(() => [!!document.getElementById('cuAsk'), !!document.querySelector('#cuDlg[open]')]); if (k[0]) await answer('no');
      assert.deepEqual(k, [false, false], 'a drop on the cut order\'s row asks nothing and opens no designs window');
      assert.equal(await page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); const c = (B.customDesigns || {})[Review.cardKey(r)]; return c ? c.files.length : 0; }, K), 0, 'and no design is added to K'); }
    pass('K (cut GF Sheet 2) and M (committed Set 1), in Review: no drop hint, no cuDropOk, a drop adds nothing; Release Hold, no Mark OK, no Open sheet');

    /* ── B · a design dropped on A's row replaces its matched charm ── */
    await rowOf(A);
    // (NOMATCHASK, Paul 20 Oct 2026: the question names a selected vector design only when the line is matched to a design the library holds; a SKU the library does not hold (TEST_@) has none)
    { const was = await page.evaluate(k => { const r = Orders.rows().find(x => String(x.order.receiptId) === String(k)), sp = r.spec, keep = JSON.stringify({ d: sp.designSku, p: sp.problems }); sp.designSku = 'TEST_@'; sp.problems = (sp.problems || []).concat([{ kind: 'unmatchedSku', reason: 'not in any master file' }]); return keep; }, A);
      await dropRaw(A, [{ name: FILE, text: DESIGN_DXF }]);
      await until(() => page.evaluate(() => !!document.querySelector('#cuAsk [data-ask-no]')), 8000, 'the question for an unmatched line');
      const t0 = await page.evaluate(() => document.getElementById('cuAskB').textContent);
      await answer('no');
      await page.evaluate(([k, w]) => { const r = Orders.rows().find(x => String(x.order.receiptId) === String(k)), o = JSON.parse(w); r.spec.designSku = o.d; r.spec.problems = o.p || []; }, [A, was]);
      assert(!/already has a vector design selected/.test(t0) && /this order only/.test(t0), 'an unmatched SKU (TEST_@) is no selected design: ' + t0); }
    await dropRaw(A, [{ name: FILE, text: DESIGN_DXF }]);
    // (the first design dropped on an order asks first: this order only)
    await until(() => page.evaluate(() => !!document.querySelector('#cuAsk [data-ask-yes]') || !!document.querySelector('#cuDlg[open]')), 8000, 'the drop is taken (its question, or the designs window)')
      .catch(async e => { throw new Error(e.message + ' ' + await dump(A)); });
    { const t1 = await page.evaluate(() => (document.getElementById('cuAskB') || {}).textContent || ''); assert(/already has a vector design selected: FEATHER/.test(t1), 'a line matched to a library design names it: ' + t1); }
    await answer('yes');
    await page.waitForFunction(() => document.querySelector('#cuDlg[open] .cuFile .cuThumb img'), null, { timeout: 30000 }).catch(async e => { throw new Error('the designs window with the read design: ' + e.message + ' ' + await dump(A)); });
    const dlg = await page.evaluate(() => ({ title: document.getElementById('cuDlgT').textContent, rows: [...document.querySelectorAll('#cuDlg .cuFile')].map(n => ({ name: n.querySelector('.cuMeta b').textContent, meta: n.querySelector('.cuMeta .dim').textContent, on: [...n.querySelectorAll('.cuM.on')].map(b => b.dataset.m) })) }));
    assert.equal(dlg.title, `Designs for order ${A}`, 'the designs window is A\'s: ' + JSON.stringify(dlg));
    assert.equal(dlg.rows.length, 1, 'one design: ' + JSON.stringify(dlg)); assert.equal(dlg.rows[0].name, FILE);
    // its metal, as the window asks (the order's metal, GF, is picked)
    if (!dlg.rows[0].on.includes('gold')) await page.click('#cuDlg .cuFile:nth-child(1) .cuM[data-m="gold"]');
    await until(() => page.evaluate(() => { const b = document.querySelector('#cuDlg [data-send]'); return b && !b.disabled; }), 8000, 'the window is ready to send').catch(async e => { throw new Error(e.message + ' ' + await page.evaluate(() => document.getElementById('cuSum') && document.getElementById('cuSum').textContent)); });
    await page.click('#cuDlg [data-x]');
    await page.waitForFunction(() => !document.querySelector('#cuDlg[open]'));
    await until(async () => { const r = await rowOf(A); return r && r.send === 'Send to Sheet' && await page.evaluate(() => /gold/.test((document.querySelector('#rvList > .reviewListRow[data-test-row] [data-cu-send]') || {}).className || '')); }, 8000, 'Send to Sheet is the next step on A\'s row')
      .catch(async e => { throw new Error(e.message + ' ' + JSON.stringify(await rowOf(A))); });
    // nothing has moved yet: A's FEATHER is still on GF Sheet 1
    assert.equal(st.calls.slice(w0).filter(c => /^(poolPut|poolUpdate)$/.test(String(c.op))).length, 0, 'nothing is written before Send to Sheet');
    const wSend = st.calls.length;
    // (what the send hands the make-up: each Review.repool call it makes, with the line's flags and the send's own record of the lines it replaces, read at the call)
    await page.evaluate(rid => {
      const R = window.Review, was = R.repool; window.__repools = [];
      R.repool = function (row, pre) { try { const e = (B.customDesigns || {})[Review.cardKey(row)], i = e && e.sendIntent; window.__repools.push({ key: row.key, rid: row.order.receiptId, repoolChanged: !!row.repoolChanged, changePending: !!row.changePending, keepsPlace: !!(Pool.keepsPlace && Pool.keepsPlace(row)), marked: CharmNestReviewMark.isMarked(row.order.receiptId), replace: i && i.replace || null }); } catch (_) {} return was.apply(this, arguments); };
    }, A);
    await page.click('#rvList > .reviewListRow[data-test-row] [data-cu-send]');
    await until(() => page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); return !!CustomSheet.decisionOf(r) && !document.querySelector('#rvList .cuStat, #rvList .btn.working') && CN.S.sheets.gold.pages.every(p => !['nesting', 'queued', 'ready'].includes(p.status)); }, A), 45000, 'A\'s designs sent and the sheets settled')
      .catch(async e => { throw new Error(e.message + ' ' + await dump(A)); });
    await sleep(800);
    const after = JSON.parse(await dump(A));
    const why = ' · state: ' + JSON.stringify(after);
    // the send recorded which line it replaces (intent.replace), and handed the make-up that line ONCE, flagged as changed (repoolChanged)
    const repools = await page.evaluate(() => window.__repools || []);
    const mineR = repools.filter(x => x.rid === A);
    assert.equal(mineR.length, 1, 'the send makes A\'s line up once: ' + JSON.stringify(repools) + why);
    assert(mineR[0].repoolChanged && mineR[0].changePending && Array.isArray(mineR[0].replace) && mineR[0].replace.length === 1, 'flagged as changed, the send naming the line it replaces: ' + JSON.stringify(mineR[0]) + why);
    // the old piece came off GF Sheet 1, in one take-off commit
    const offs = st.calls.slice(wSend).filter(c => c.op === 'poolUpdate' && c.body.patch && c.body.patch.state === 'superseded');
    assert.deepEqual(offs.map(c => c.body.poolIds), [[idA]], 'one take-off commit for A\'s old FEATHER piece (superseded): ' + JSON.stringify(offs.map(c => c.body)) + why);
    const sh1 = after.sheets.find(s => s.id === PAGES[1].id);
    assert(!sh1.charms.some(([id, sku]) => sku === OLD && id === idA), 'A\'s old FEATHER is no longer on GF Sheet 1' + why);
    assert(!after.sheets.some(s => s.charms.some(([id, sku]) => String(id).startsWith(A + '_') && sku === OLD)), 'nor on any other sheet' + why);
    // the line is made up again from the dropped design: custom pieces, on a sheet or waiting in the pool
    const rowA = after.row;
    assert(rowA.poolIds.length >= 1, 'A has its new pieces' + why);
    for (const id of rowA.poolIds) {
      const p = after.pool.find(x => x.id === id);
      assert(p && p.custom && p.file === FILE && !['superseded', 'abandoned'].includes(p.state), `${id} is a live piece cut from ${FILE}` + why);
      const c = st.doc(POOL, id);
      assert(c && c.custom && c.customFile === FILE && !['superseded', 'abandoned'].includes(c.state), `${id} is recorded in the cloud as a piece of ${FILE}: ${JSON.stringify(c)}` + why);
    }
    // nothing placed twice
    for (const id of rowA.poolIds) {
      const on = after.sheets.flatMap(s => s.charms.filter(([cid]) => cid === id).map(([, , custom, placed]) => [s.id, custom, placed]));
      assert(on.length <= 1, `${id} is on one sheet at most, once: ${JSON.stringify(on)}` + why);
      if (on.length) assert.equal(on[0][1], true, `the piece of ${id} on ${on[0][0]} is the custom design` + why);
      for (const sid of Object.values(PAGES).map(p => p.id)) { const d = st.doc(SHEETS, sid); assert((d.poolIds || []).filter(x => x === id).length <= 1, `the saved ${sid} lists ${id} once at most` + why); }
    }
    assert(['pooled', 'written'].includes(rowA.state), 'A\'s line is pooled again (on a sheet or waiting for the next nest)' + why);
    // the order leaves Review, or its card says it was sent
    const leftReview = await page.evaluate(rid => {
      const r = B.orders.rows.find(x => x.order.receiptId === rid), open = [...document.querySelectorAll('#rvList > .reviewListRow')].filter(n => n.dataset.rid === rid);
      return { marked: CharmNestReviewMark.isMarked(rid), open: open.map(n => ({ cls: n.className, sent: !!n.querySelector('.cuDesigns.sent'), why: (n.querySelector('.reviewReason') || {}).textContent || '' })), decided: !!CustomSheet.decisionOf(r) };
    }, A);
    assert(leftReview.decided && (!leftReview.marked || leftReview.open.every(o => o.sent || /Sent/.test(o.why))), 'A leaves Review, or its card says sent: ' + JSON.stringify(leftReview) + why);
    pass(`B: a ${FILE} dropped on A's Review row and sent: the old FEATHER came off GF Sheet 1 (one superseded commit), the line is made up again from the design (${rowA.poolIds.length} custom piece${rowA.poolIds.length === 1 ? '' : 's'}), nothing placed twice`);

    /* ── C (again) · K's, M's and F's pieces untouched ── */
    const pieceWrites = st.calls.slice(w0).filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate)$/.test(String(c.op)) && new RegExp([idK, idM, idF].join('|')).test(JSON.stringify(c.body)));
    assert.deepEqual(pieceWrites.map(c => c.op), [], 'no piece record of K, M or F is written');
    for (const [id, sid, sku] of [[idK, PAGES[2].id, OLD], [idM, PAGES[3].id, OLD], [idF, PAGES[1].id, 'GFFILL_1']]) {
      assert(st.doc(POOL, id).sku === sku && st.doc(POOL, id).state === 'placed' && st.doc(POOL, id).sheetId === sid, `${id} is still ${sku} on ${sid} in the cloud: ${JSON.stringify(st.doc(POOL, id))}`);
      assert(st.doc(SHEETS, sid).poolIds.includes(id), `the saved ${sid} still lists ${id}`);
      const on = await page.evaluate(({ id, sid }) => { const p = CN.S.sheets.gold.pages.find(x => x.sheetId === sid); const c = p && p.charms.find(x => x.poolId === id); return c ? [c.sku, p.placements.some(pl => pl.id === c.id)] : null; }, { id, sid });
      assert.deepEqual(on, [sku, true], `${id} is still placed on ${sid} as ${sku}`);
    }
    { const k = await page.evaluate(rids => rids.map(rid => CharmNestReviewMark.isMarked(rid)), [K, M]); assert.deepEqual(k, [true, true], 'K and M are still in Review'); }
    pass('K (cut) and M (committed set) keep their FEATHER pieces where they are; F on GF Sheet 1 is untouched');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`review-drop-replace: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
