// REDRAW, the page (Paul, 19 Oct 2026, order 4206638416, SKU HAPPY T-REX): a SKU the library re-points to ANOTHER drawing (a newer master file added in the Library; the entry is keyed by SKU, so its
// charmHash and aiPath change) re-makes the open pieces cut from the old drawing; a cut or committed piece is never touched, only noted. Real headless Chromium over the repo's fake site (bridge-server.cjs:
// the REAL charmNestLibrary handler on an in-memory Firestore; nothing leaves the machine). The only stand-in is the nest, as in listing-pick-relink-browser.cjs / review-drop-replace.cjs.
//   A 4206638416  HAPPY T-REX pooled on the OPEN draft GF Sheet 1, cut from the old drawing            → taken off once, made up again from the new drawing, placed on GF Sheet 1, nothing twice
//   K 4206600002  HAPPY T-REX on GF Sheet 2, CUT                                                       → untouched, noted (log, timeline, row.redrawNote)
//   M 4206600003  HAPPY T-REX on GF Sheet 3, in a COMMITTED set                                       → untouched, noted
//   S 4206600004  HAPPY T-REX on GF Sheet 1, its piece holds the hash of a BODY of the new design      → not remade (a one-body piece of the new drawing)
//   F 4206600091  another SKU on GF Sheet 1                                                            → untouched
//   first a person-field-only change (an angle) does nothing; after the redraw, the same entry read again does nothing, and a sweep run again does nothing
//   node tests/charm-nest/master-redraw-remake.cjs        (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', INDEX = 'Charm_Master_Index', TLC = 'Order_Timeline', RUN = 'run-redraw-1', SET_ID = 'set-redraw-1';
const SKU = 'HAPPY T-REX', OLD_H = 'h-trex-stud', NEW_H = 'h-trex-necklace', BODY_H = 'h-trex-necklace-body1', OLD_M = 'aaaa1111', NEW_M = 'bbbb2222';
const SHIP = Math.floor(Date.UTC(2026, 9, 24, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 9, 16, 12) / 1000);
const A = '4206638416', K = '4206600002', M = '4206600003', S_ = '4206600004', F = '4206600091';
const TID = tx => String(5300000000 + tx), pid = (rid, tx) => `${rid}_${TID(tx)}_1`;
// [rid, tx, listing, sku, age (s), sheet page, cell, piece hash]
const LINES = [[A, 1, '1700600001', SKU, 9000, 1, 0, OLD_H], [S_, 4, '1700600004', SKU, 8500, 1, 2, BODY_H], [F, 5, '1700600091', 'GFFILL_1', 8000, 1, 1, 'h-GFFILL_1'], [K, 2, '1700600002', SKU, 9500, 2, 0, OLD_H], [M, 3, '1700600003', SKU, 9400, 3, 0, OLD_H]];
const PAGES = { 1: { id: 'rw-gold-1', draft: true }, 2: { id: 'rw-gold-2', cut: true }, 3: { id: 'rw-gold-3', setId: SET_ID } };
const idA = pid(A, 1), idK = pid(K, 2), idM = pid(M, 3), idS = pid(S_, 4), idF = pid(F, 5);
const entryDoc = (over) => Object.assign({ sku: SKU, aiPath: 'test/HAPPY_T-REX.ai', charmHash: OLD_H, masterHash: OLD_M, masterName: 'Master Sept.ai', widthPt: 28.35, heightPt: 28.35, holes: 0, hashSource: 'browser', indexedAtMs: Date.now() - 86400000 }, over || {});
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const srv = await start({ receipts: [] }), st = srv.st;
  st.put(INDEX, SKU, entryDoc(), false);
  st.put(INDEX, 'GFFILL_1', { sku: 'GFFILL_1', aiPath: 'test/GFFILL_1.ai', charmHash: 'h-GFFILL_1', masterHash: OLD_M, widthPt: 28.35, heightPt: 28.35, holes: 0 }, false);
  for (const [pg, sh] of Object.entries(PAGES)) {
    const mine = LINES.filter(l => l[5] === +pg);
    st.put(SHEETS, sh.id, Object.assign({ id: sh.id, metal: 'gold', charms: mine.map((l, i) => ({ id: `${sh.id}-c${i}`, poolId: pid(l[0], l[1]), order: l[0], sku: l[3] })), placements: mine.map((l, i) => ({ id: `${sh.id}-c${i}`, cxPt: 16 + l[6] * 30, cyPt: 16, angle: 0 })), poolIds: mine.map(l => pid(l[0], l[1])), sheetIndex: +pg, fileBase: 'GF Sheet ' + pg, runId: RUN },
      sh.cut ? { laserDoneAt: Date.now() - 3600000 } : {}, sh.setId ? { setId: sh.setId } : {}, sh.draft ? { draft: true } : {}), false);
    for (const l of mine) st.put(POOL, pid(l[0], l[1]), { poolId: pid(l[0], l[1]), orderId: l[0], transactionId: TID(l[1]), lineKey: `${l[0]}_${TID(l[1])}`, sku: l[3], copy: 1, quantity: 1, sheetId: sh.id, state: 'placed', material: 'gold', runId: RUN, charmHash: l[7], masterHash: OLD_M, aiPath: 'test/' + l[3].replace(/ /g, '_') + '.ai', updatedAt: Date.now() - 600000 }, false);
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
            const waiting = sh.charms.filter(c => !sh.placements.some(p => p.id === c.id));
            for (const c of waiting) { if (!free.length) break; const [x, y] = free.shift(); sh.placements.push({ id: c.id, cxPt: x, cyPt: y, angle: 0, wPt: c.widthPt, hPt: c.heightPt }); }
            if (sh.sheetId) await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: sh.draft || undefined, setId: sh.setId || undefined, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: c.sku })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true });
            sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.stage = ''; sh.persistedDone = true; sh.verification = { ok: true };
          } catch (e) { sh.problem = e.message; sh.status = 'complete'; sh.persistedDone = true; }
          try { CN.renderCard(sh); } catch (_) {}
        }, 120);
      };
      stub.__stub = true; window.startNest = stub; /* INPLACE (Paul, 20 Oct 2026): a sheet whose replaced piece came off is written as it stands (writeAsPlaced, no nest): stood in for here as the nest is, every placement kept */ const wap = sh => { window.__writes = (window.__writes || []).concat([{ sheetId: sh.sheetId, page: sh.page, placements: sh.placements.map(p => ({ id: p.id, cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle })) }]); sh.status = 'finishing'; sh.persistedDone = false; let done; sh.persisted = new Promise(r => { done = r; }); return new Promise(res => setTimeout(async () => { try { if (sh.sheetId) await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: sh.draft || undefined, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: (c.orderInfo && c.orderInfo.sku) || c.sku })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true }); } catch (e) { sh.problem = e.message; } sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.persistedDone = true; sh.verification = { ok: true }; done(); try { CN.renderCard(sh); } catch (_) {} res(!sh.problem); }, 120)); }; wap.__stub = true; window.writeAsPlaced = wap; clearInterval(iv);
    }, 0);
  });
  const page = await ctx.newPage(); page.setDefaultTimeout(30000);
  page.on('pageerror', e => errors.push(String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what + ' · ' + await dump()); await sleep(150); } };
  const dump = () => page.evaluate(() => JSON.stringify({ wait: Review.redrawPending(), pool: [...B.pool.rows.values()].map(p => [p.poolId, p.state, p.charmHash, p.sheetId]), sheets: CN.S.sheets.gold.pages.map(p => [p.sheetId, p.status, p.charms.map(c => [c.poolId, c.hash || null, p.placements.some(pl => pl.id === c.id)])]), rows: B.orders.rows.map(r => [r.order.receiptId, r.state, r.reason, r.hold, r.poolIds]), log: (window.AG && AG.events || []).slice(-6).map(e => e.text) })).catch(e => 'dump failed: ' + e.message);
  const pieceWrites = (from, ids) => st.calls.slice(from).filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate)$/.test(String(c.op)) && new RegExp(ids.join('|')).test(JSON.stringify(c.body)));
  try {
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && window.Review && Review.redrawNoted && window.Pool && Pool.forgetDrawing && window.startNest && window.startNest.__stub && B.master.loadedAt > 0 && !B.master.loading && Session.ready(), null, { timeout: 60000 });
    await page.evaluate(() => { const P = window.CharmNestPDF; if (P) { P.drawCharm = () => {}; P.cutLinesOf = () => []; P.pathToCanvas = () => {}; } });
    /* ── the world: three gold sheets (open draft, cut, in a committed set), the lines pooled on them, the run ── */
    await page.evaluate(async ({ LINES, PAGES, RUN, SET_ID, BASE_TS, SHIP, SKU, OLD_M }) => {
      await Orders.loadMaps(true);
      CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72] });
      const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, TID = tx => String(5300000000 + tx), pid = (rid, tx) => `${rid}_${TID(tx)}_1`;
      const mkCharm = (id, extra) => Object.assign({ id, name: id, sku: '', sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: 0, cy: 0, r: R * MM }, members: [], rMm: R, w: 8, h: 8, scale: 1, bits: new Uint8Array(64).fill(1), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2 }, extra || {});
      window.__mkCharm = mkCharm;
      // the library as this page holds it (as the cloud has it) and the designs it read from the files: the old T-Rex is the stud
      for (const [sku, hash] of [[SKU, 'h-trex-stud'], ['GFFILL_1', 'h-GFFILL_1']]) { const key = 'test/' + sku.replace(/ /g, '_') + '.ai'; B.pool.sources.set(key, { id: 'pool:' + sku.replace(/\W+/g, '_'), pool: true, name: sku, sku, hash, charms: [mkCharm('pool:' + sku + ':0', { sku, name: sku, hash })], usedAt: Date.now() }); }
      const pr = CN.S.sheets.gold; pr.pages.length = 1; const p0 = pr.pages[0]; Object.assign(p0, { charms: [], placements: [], sheetId: null, fileBase: null, status: 'idle', page: 1 }); for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'releaseFull', 'draft']) delete p0[k];
      B.pool.rows.clear(); B.sets.clear();
      for (const [pg, sh] of Object.entries(PAGES)) {
        while (pr.pages.length < +pg) addPage('gold');
        const page = pr.pages[+pg - 1]; page.charms = []; page.placements = [];
        LINES.filter(l => l[5] === +pg).forEach((l, i) => {
          const id = `${sh.id}-c${i}`, poolId = pid(l[0], l[1]);
          page.charms.push(mkCharm(id, { name: `${l[0]} · ${l[3]}`, sku: l[3], hash: l[7], poolId, order: l[0], lineKey: `${l[0]}_${TID(l[1])}`, orderDate: BASE_TS - l[4], orderInfo: { receiptId: l[0], transactionId: TID(l[1]), sku: l[3], copy: 1, quantity: 1 } }));
          page.placements.push({ id, cxPt: 16 + l[6] * 30, cyPt: 16, angle: 0, wPt: 2 * R * MM, hPt: 2 * R * MM });
        });
        Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: 'GF Sheet ' + pg, sheetIndex: +pg, page: +pg, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false });
        if (sh.cut) page.laserDoneAt = Date.now() - 3600000;
        if (sh.draft) page.draft = true;
        if (sh.setId) page.setId = sh.setId;
        CN.renderCard(page);
      }
      B.sets.set(`${RUN}|dispatch`, { setId: SET_ID, runId: RUN, seq: 1, day: new Date().toISOString().slice(0, 10), group: 'dispatch', name: 'Set 1', committedAt: Date.now() - 1800000, savedCommit: Date.now() - 1800000, sheetIds: [PAGES[3].id], materials: ['gold'], labelFiles: [], orders: {}, status: 'committed' });
      const rows = [];
      for (const l of LINES) {
        const [rid, tx, listingId, sku, age, pg, , hash] = l;
        const line = { transactionId: TID(tx), listingId, sku, title: sku === SKU ? 'Happy T-Rex Necklace Dinosaur Charm' : sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' };
        const order = { receiptId: rid, orderNumber: rid, createTs: BASE_TS - age, updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Name ' + rid.slice(-2) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
        rows.push({ key: CharmNestOrders.lineKey(order, line), order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [pid(rid, tx)], engrave: null, material: 'gold' });
        B.pool.rows.set(pid(rid, tx), { poolId: pid(rid, tx), orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sku, copy: 1, quantity: 1, sheetId: PAGES[pg].id, state: 'placed', material: 'gold', runId: RUN, charmHash: hash, masterHash: OLD_M, aiPath: 'test/' + sku.replace(/ /g, '_') + '.ai', updatedAt: Date.now() - 600000 });
      }
      B.orders.rows.length = 0; B.orders.byKey.clear(); for (const r of rows) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); }
      B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [...new Set(rows.map(r => r.order.receiptId))] };
      await Master.load({ force: true });
      Orders.interpretAll(); Review.syncOrderItems();
      return rows.map(r => [r.order.receiptId, r.spec.designSku]);
    }, { LINES, PAGES, RUN, SET_ID, BASE_TS, SHIP, SKU, OLD_M });
    const before = await page.evaluate(SKU => [Master.entryFor(SKU) && Master.entryFor(SKU).charmHash, Review.redrawPending()], SKU);
    assert.deepEqual(before, [OLD_H, 0], 'the library holds the stud T-Rex, nothing waits: ' + JSON.stringify(before));

    /* 1 · a person sets the angle on the entry (another computer): no drawing changed, nothing happens */
    const w0 = st.calls.length;
    st.put(INDEX, SKU, { upAngle: 45, upSource: 'operator', indexedAtMs: Date.now() - 3600000 });
    await page.evaluate(() => Master.load({ force: true }));
    await sleep(800);
    assert.equal(await page.evaluate(SKU => Master.entryFor(SKU).upAngle, SKU), 45, 'the angle reached this page');
    assert.equal(await page.evaluate(() => Review.redrawPending()), 0, 'nothing waits after a person-field change');
    assert.equal(pieceWrites(w0, [idA, idK, idM, idS, idF]).length, 0, 'no piece is written for a person-field change');
    pass('a change of what a person set (the angle) re-makes nothing');

    /* 2 · a newer master file re-points HAPPY T-REX to the necklace T-Rex (another computer's index run: a new charm hash, a new file, a new master) */
    await page.evaluate(({ BODY_H, NEW_H }) => {
      const key = 'test/HAPPY_T-REX-necklace.ai', base = window.__mkCharm('pool:trexN:0', { sku: 'HAPPY T-REX', name: 'HAPPY T-REX', hash: NEW_H });
      // (the new design draws two bodies, the T-Rex and its ring: a piece cut from one BODY holds the body's hash, soloBodies)
      B.pool.sources.set(key, { id: 'pool:trexN', pool: true, name: 'HAPPY T-REX', sku: 'HAPPY T-REX', hash: NEW_H, charms: [base], soloChecked: true, solo: [Object.assign({}, base, { hash: NEW_H + '-body0' }), Object.assign({}, base, { hash: BODY_H })], bodiesChecked: true, bodies: null, usedAt: Date.now() });
    }, { BODY_H, NEW_H });
    const w1 = st.calls.length;
    st.put(INDEX, SKU, { charmHash: NEW_H, aiPath: 'test/HAPPY_T-REX-necklace.ai', masterHash: NEW_M, masterName: 'Master Oct.ai', indexedAtMs: Date.now(), replaced: { from: OLD_M, fromName: 'Master Sept.ai', at: Date.now(), by: 'Paul' } });
    await page.evaluate(() => Master.load({ force: true }));
    await until(() => page.evaluate(({ idA, NEW_H }) => { const s = CN.S.sheets.gold.pages.find(p => p.sheetId === 'rw-gold-1'), c = s && s.charms.find(x => x.poolId === idA), p = B.pool.rows.get(idA); return !!(c && c.hash === NEW_H && s.placements.some(pl => pl.id === c.id) && s.status === 'complete' && p && p.charmHash === NEW_H && Review.redrawPending() === 0); }, { idA, NEW_H }), 30000, 'A made up again on GF Sheet 1');
    await sleep(600);
    const s1 = await page.evaluate(({ idA, idS, idF, idK, idM }) => {
      const sh = id => CN.S.sheets.gold.pages.find(p => p.sheetId === id), on = (id, pidv) => sh(id).charms.filter(c => c.poolId === pidv).map(c => [c.hash, sh(id).placements.some(pl => pl.id === c.id)]);
      return { A: on('rw-gold-1', idA), S: on('rw-gold-1', idS), F: on('rw-gold-1', idF), K: on('rw-gold-2', idK), M: on('rw-gold-3', idM), Aall: CN.S.sheets.gold.pages.flatMap(p => p.charms.filter(c => c.poolId === idA).map(() => p.sheetId)), pool: Object.fromEntries([idA, idS, idK, idM, idF].map(i => [i, [(B.pool.rows.get(i) || {}).state, (B.pool.rows.get(i) || {}).charmHash]])) };
    }, { idA, idS, idF, idK, idM });
    assert.deepEqual(s1.A, [[NEW_H, true]], 'A is on GF Sheet 1 once, as the new drawing, placed: ' + JSON.stringify(s1));
    assert.deepEqual(s1.Aall, ['rw-gold-1'], 'one piece of A on all the sheets, never two');
    assert.deepEqual(s1.S, [[BODY_H, true]], 'S (a body of the new design) stays as it is');
    assert.deepEqual(s1.K, [[OLD_H, true]], 'K stays on the cut GF Sheet 2 as the old drawing');
    assert.deepEqual(s1.M, [[OLD_H, true]], 'M stays on the committed GF Sheet 3 as the old drawing');
    assert.deepEqual(s1.F, [['h-GFFILL_1', true]], 'F (another SKU) untouched');
    pass('the open draft piece is made up again from the new drawing, once, in its own room on GF Sheet 1');
    const offs = st.calls.slice(w1).filter(c => c.op === 'poolUpdate' && c.body.patch && c.body.patch.state === 'superseded');
    assert.deepEqual(offs.map(c => c.body.poolIds), [[idA]], 'ONE take-off commit, for A only: ' + JSON.stringify(offs.map(c => c.body.poolIds)));
    const putsA = st.calls.slice(w1).filter(c => c.op === 'poolPut' && JSON.stringify(c.body).includes(idA));
    assert(putsA.length >= 1 && st.doc(POOL, idA).charmHash === NEW_H && st.doc(POOL, idA).masterHash === NEW_M && !['superseded', 'abandoned'].includes(st.doc(POOL, idA).state), 'the cloud holds A as the new drawing: ' + JSON.stringify(st.doc(POOL, idA)));
    assert.equal(pieceWrites(w1, [idK, idM, idS, idF]).length, 0, 'no piece record of K, M, S or F is written: ' + JSON.stringify(pieceWrites(w1, [idK, idM, idS, idF]).map(c => c.op)));
    assert(st.doc(SHEETS, 'rw-gold-2').poolIds.includes(idK) && st.doc(SHEETS, 'rw-gold-3').poolIds.includes(idM), 'the cut and committed saved sheets still list K and M');
    pass('the cloud: one take-off commit, A recorded as the new drawing, K, M, S and F untouched');
    const notes = await page.evaluate(({ K, M, A, S }) => { const row = rid => B.orders.rows.find(r => r.order.receiptId === rid); return { K: row(K).redrawNote, M: row(M).redrawNote, A: row(A).redrawNote || null, S: row(S).redrawNote || null, log: AG.events.filter(e => /older drawing|OLDER drawing/.test(e.text || '')).map(e => e.text) }; }, { K, M, A, S: S_ });
    assert(notes.K && notes.K.how === 'cut' && notes.M && notes.M.how === 'in a committed set', 'K and M carry the note: ' + JSON.stringify(notes));
    assert(notes.log.some(t => t.startsWith(K)) && notes.log.some(t => t.startsWith(M)), 'the activity log says K and M are the older drawing: ' + JSON.stringify(notes.log));
    assert(!notes.A && !notes.S, 'A and S carry no note');
    await until(async () => { await page.evaluate(() => { try { OrderTimeline.flush && OrderTimeline.flush(); } catch (_) {} }); const k = rid => st.list(TLC).filter(e => e._id.startsWith(rid + '~') && /older drawing of HAPPY T-REX/.test(e.text || '')).length; return k(K) === 1 && k(M) === 1; }, 20000, 'timeline notes for K and M');
    await until(async () => { await page.evaluate(() => { try { OrderTimeline.flush && OrderTimeline.flush(); } catch (_) {} }); return st.list(TLC).filter(e => e._id.startsWith(A + '~') && /Made again from the new drawing of HAPPY T-REX/.test(e.text || '')).length === 1; }, 20000, 'timeline event for A');
    pass('the cut and committed lines are noted (row, activity log, order timeline); A\'s timeline says it was made again');

    /* 3 · the same entry read again, and the sweep run again: nothing */
    const w2 = st.calls.length, nests = await page.evaluate(() => (window.__nests || []).length);
    st.put(INDEX, SKU, { indexedAtMs: Date.now() + 1 });
    await page.evaluate(() => Master.load({ force: true }));
    await page.evaluate(() => Review.redrawNow());
    await sleep(1500);
    assert.equal(await page.evaluate(() => Review.redrawPending()), 0, 'nothing waits');
    assert.equal(pieceWrites(w2, [idA, idK, idM, idS, idF]).length, 0, 'no piece is written again: ' + JSON.stringify(pieceWrites(w2, [idA, idK, idM, idS, idF]).map(c => c.op)));
    assert.equal(await page.evaluate(() => (window.__nests || []).length), nests, 'no sheet is arranged again');
    // even told of the very same change again (another read from the OLD copy), A's piece holds the new drawing and the noted lines are not told twice
    const again = await page.evaluate(async ({ SKU, OLD_H, OLD_M }) => { const now = Master.entryFor(SKU), was = Object.assign({}, now, { charmHash: OLD_H, masterHash: OLD_M, aiPath: 'test/HAPPY_T-REX.ai' }); const n0 = Review.redrawNoted(new Map([[SKU, was]]), new Map([[SKU, now]])); const r = await Review.redrawNow(); return { n0, swapped: r.swapped.length, flagged: r.flagged.length, wait: Review.redrawPending() }; }, { SKU, OLD_H, OLD_M });
    await sleep(800);
    assert.deepEqual(again, { n0: 1, swapped: 0, flagged: 0, wait: 0 }, 'the same change told twice re-makes and notes nothing: ' + JSON.stringify(again));
    assert.equal(pieceWrites(w2, [idA, idK, idM, idS, idF]).length, 0, 'still nothing written');
    pass('the same entry read again, a second sweep and the same change told twice do nothing');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    console.log(`master-redraw-remake: ${n} checks passed`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
