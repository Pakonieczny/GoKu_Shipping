// COMETMAP, the page · real headless Chromium against the repo's fake site (which runs the REAL charmNestLibrary handler over an in-memory Firestore: zero network, nothing live).
//   node tests/charm-nest/saved-link-repool-browser.cjs [playwright-core dir]      (PW_DIR / CHROMIUM as schro-link-browser.cjs; skipped without playwright / chrome; CL_SHOTS=<dir> saves screenshots)
// Order 4197740562: a Silver 18" necklace, Etsy listing 1687890868, SKU "Celestial3-Comet". The library holds the old comet "CELESTIAL3-COMET" (no loop) under that very name and Paul's repo charm
// "CELESTIAL 3 - COMET" (= COMET_9427, with the loop). The real index entries (read-only masterGet, 12 Oct 2026) and the real design files are served to the page like Storage serves them.
// The saved link is ONE call of the page's own aliasPut (no sandbox flag): a single design for this listing and SKU, the way "Use this charm" saves it.
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const OLD = 'CELESTIAL3-COMET', NEW = 'CELESTIAL 3 - COMET', LID = '1687890868', RAW = 'Celestial3-Comet';
const TITLE = 'Comet Pendant Charm Necklace Dainty Space Jewelry Tiny Celestial Charm Space Necklace Cosmic Pendant Space Jewelry Gift for Her';

(async () => {
  const pwDir = process.env.PW_DIR || process.argv[2] || (fs.existsSync('/opt/node22/lib/node_modules/playwright/node_modules') ? '/opt/node22/lib/node_modules/playwright/node_modules' : path.join(root, 'node_modules'));
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  · saved-link-repool browser part: no playwright-core, not run'); return; }
  const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(chromePath)) { console.log('  · saved-link-repool browser part: no chrome, not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), st = srv.st;
  const masters = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/comet-master-entries.json'), 'utf8')).entries;
  const fileAt = { 'charmnest/sandbox/master/COMET_9427.ai': 'COMET_9427.ai', 'charmnest/sandbox/master/CELESTIAL3-COMET.ai': 'CELESTIAL3-COMET.ai' };
  for (const e of masters) st.put('Charm_Master_Index', e.sku, Object.assign({}, e));
  for (const [p, f] of Object.entries(fileAt)) st.blobs.set(p, { buf: fs.readFileSync(path.join(__dirname, 'fixtures', f)), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });
  const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 20, 17) / 1000);
  const V = (name, value) => ({ name, value });
  const line = (tid, listingId) => ({ transactionId: tid, listingId, sku: RAW, title: TITLE, quantity: 1, expectedShipDate: SHIP, productId: '1', metalKey: 'silver', metalLabel: 'Silver', personalization: [], variations: [V('Metal Choice', 'Silver'), V('Necklace Length in inches', '18"')] });
  const order = (rid, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Test Buyer' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
  const A = '4197740562', E = '4197749999';
  const orders = [order(A, [line('5246703786', LID)]), order(E, [line('5246703787', '1999000777')])];   // (E: the same SKU on ANOTHER listing)
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  const errors = [], shots = process.env.CL_SHOTS || '';
  try {
    const context = await browser.newContext({ viewport: { width: 1500, height: 1500 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(); page.setDefaultTimeout(40000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && window.Pool && window.Engrave && CN.S.cloud.ok === true && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const o of orders) o.lines.forEach(l => { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    }, orders);
    const rowState = rid => page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid), s = r.spec; return { designSku: s.designSku, source: s.skuSource, problems: s.problems.map(x => x.kind), pieces: s.pieceCount }; }, rid);
    /* 1 · before the link: the old comet (no loop), the entry the Etsy SKU names */
    const before = { A: await rowState(A), E: await rowState(E) };
    // (LOOKALIKE, 12 Oct 2026: the Etsy SKU names the loop-less CELESTIAL3-COMET exactly, but on a NECKLACE listing with a ringed lookalike the exact name no longer wins silently:
    //  the line waits with one plain line naming both drawings, until a person picks, or a saved link answers it)
    const asked = x => x.designSku === null && x.problems.join() === 'unmatchedSku';
    const why = await page.evaluate(rid => B.orders.rows.find(x => x.order.receiptId === rid).spec.problems[0].reason, A);
    ok(asked(before.A) && asked(before.E) && /CELESTIAL3-COMET \(no loop/.test(why) && /CELESTIAL 3 - COMET \(with ring/.test(why), 'before the link both necklace lines ask which comet, naming both drawings: ' + JSON.stringify(before) + ' ' + why);
    /* 2 · the order was already pooled as the old comet (what Paul's order is), with an engraving still to approve */
    const pooled = await page.evaluate(async rid => {
      const r = B.orders.rows.find(x => x.order.receiptId === rid), id = CharmNestOrders.poolId(r.order, r.line, 1);
      B.pool.rows.set(id, { poolId: id, state: 'ready', sku: 'CELESTIAL3-COMET', copy: 1, quantity: 1, charmHash: '00375e40', sheetId: null, orderId: rid, material: r.spec.material });
      r.poolIds = [id]; r.state = 'pooled'; r.engrave = { needed: true, state: 'review', approved: false };
      window.__inv = []; window.Engrave.invalidate = (row, why) => { window.__inv.push([row.key, why]); };
      Pool.pinPooled(r); return { flagged: [r.changePending || false, r.repoolChanged || false], items: B.review.items.map(i => i.kind) };
    }, A);
    ok(pooled.flagged.join() === 'false,false' && !pooled.items.includes('orderChanged'), 'before the link nothing is offered: the piece already is what the line reads as: ' + JSON.stringify(pooled));
    /* 3 · the saved link, once, through the page's own call (no sandbox flag: production's store of the fake site) */
    const put = await page.evaluate(async ({ LID, RAW, NEW }) => CN.api('charmNestLibrary', { op: 'aliasPut', listingId: LID, fromSku: RAW, sku: NEW, by: 'COMETMAP', title: 'Comet Pendant Charm Necklace' }), { LID, RAW, NEW });
    ok(put && put.ok === true, 'the real call saved the link: ' + JSON.stringify(put));
    const puts = st.calls.filter(c => c.name === 'charmNestLibrary' && c.body.op === 'aliasPut');
    ok(puts.length === 1 && !puts[0].body.sandbox, 'ONE call, a production call (no sandbox flag)');
    const rec = st.doc('Charm_Sku_Aliases', LID); ok(rec && Object.keys(rec.bySku).join('|') === RAW.toUpperCase() && rec.bySku[RAW.toUpperCase()] === NEW, 'production holds ONE record for the listing, under its SKU in capitals: ' + JSON.stringify(rec && rec.bySku));
    await page.evaluate(async () => { await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); Orders.render(); });
    const after = { A: await rowState(A), E: await rowState(E) };
    ok(after.A.designSku === NEW && after.A.source === 'alias' && after.A.problems.length === 0, 'now the line reads as the repo charm ' + NEW + ' from the saved link: ' + JSON.stringify(after.A));
    ok(JSON.stringify(after.E) === JSON.stringify(before.E), 'the same SKU on another listing is not changed: ' + JSON.stringify(after.E));
    /* 4 · the order window says the repo charm and draws it */
    await page.evaluate(rid => { OrderWin.open(B.orders.rows.find(r => r.order.receiptId === rid).key, { view: 'info' }); }, A);
    await page.waitForFunction(() => { const v = document.getElementById('owVector'); return v && v.querySelector('img') && v.getAttribute('aria-busy') !== 'true'; }, null, { timeout: 30000 });
    const win = await page.evaluate(() => ({ sku: document.getElementById('owSku').textContent, vec: document.getElementById('owVector').textContent.trim() }));
    ok(win.sku === 'SKU: ' + NEW && !/Unavailable|No vector/.test(win.vec), 'the order window names the repo charm and draws it: ' + JSON.stringify(win));
    if (shots) { await page.waitForTimeout(500); await page.screenshot({ path: path.join(shots, 'COMETMAP-order-window.png') }); }
    await page.evaluate(() => OrderWin.close()); await page.waitForTimeout(200);
    /* 5 · the piece on the open sheet is still the old comet: the Review "Changed" row offers the new reading */
    const offered = await page.evaluate(async rid => {
      const r = B.orders.rows.find(x => x.order.receiptId === rid); Pool.pinPooled(r);
      CN.setMode('review'); Review.render(); await new Promise(res => setTimeout(res, 300));
      const row = document.querySelector('#rvList .reviewListRow[data-rid="' + rid + '"]'), b = row && row.querySelector('[data-rv-accept]');
      return { flagged: [r.changePending, r.repoolChanged], items: B.review.items.map(i => i.kind + ':' + i.key), rowText: row ? row.textContent.replace(/\s+/g, ' ') : '', accept: b ? b.textContent.trim() : '', buttons: row ? [...row.querySelectorAll('button')].map(q => q.textContent.trim()).filter(Boolean) : [] };
    }, A);
    ok(offered.flagged.join() === 'true,true', 'the pooled line is flagged to be made up again: ' + JSON.stringify(offered.flagged));
    ok(/Changed/.test(offered.rowText) && /CELESTIAL 3 - COMET/.test(offered.rowText) && /open sheet/.test(offered.rowText), 'Review has a row for the order: Changed, the repo charm, why: ' + offered.rowText.slice(0, 400));
    ok(offered.accept === 'Accept the new order' && !offered.buttons.includes('Hold'), 'with the button Accept the new order and no Hold (NOHOLDREV, Paul 17 Oct 2026): ' + JSON.stringify(offered.buttons));
    ok(!offered.items.some(k => k.includes(E)), 'the other listing\'s order is offered nothing');
    if (shots) await page.screenshot({ path: path.join(shots, 'COMETMAP-review-row.png') });
    const writesBefore = st.calls.filter(c => c.name === 'charmNestLibrary' && /alias|master/i.test(String(c.body.op || ''))).length;
    /* 6 · Accept: the old piece comes off, the line is made up again as the repo charm, the engraving is asked again, nothing else is written */
    await page.evaluate(rid => { document.querySelector('#rvList .reviewListRow[data-rid="' + rid + '"] [data-rv-accept]').click(); }, A);
    await page.waitForFunction(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); return !r.changePending && !(r.poolIds || []).length; }, A, { timeout: 20000 });
    const accepted = await page.evaluate(async rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); await new Promise(res => setTimeout(res, 300)); return { changePending: r.changePending, repoolChanged: r.repoolChanged, poolIds: r.poolIds, state: r.state, designSku: r.spec.designSku, hold: r.hold || null, items: B.review.items.map(i => i.kind), oldRow: B.pool.rows.get(CharmNestOrders.poolId(r.order, r.line, 1)) || null, inv: window.__inv, engrave: r.engrave }; }, A);
    ok(accepted.changePending === false && accepted.repoolChanged === false && accepted.poolIds.length === 0 && accepted.designSku === NEW && !accepted.hold, 'Accept took the old piece off and the line reads as the repo charm: ' + JSON.stringify(accepted));
    ok(!accepted.items.includes('orderChanged'), 'the Review row is gone');
    ok(accepted.inv.length === 1 && accepted.inv[0][0].includes(A) && accepted.inv[0][1] === 'design changed' && accepted.engrave.approved === false && accepted.engrave.state === 'reclassify', 'the engraving fitted to the old outline was asked again, once: ' + JSON.stringify([accepted.inv, accepted.engrave]));
    ok(st.calls.filter(c => c.name === 'charmNestLibrary' && /alias|master/i.test(String(c.body.op || ''))).length === writesBefore, 'Accept wrote no alias and no master');
    await context.close();
    ok(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`  ✓ the page: the link reads the repo charm, the order window names and draws it, the Order changed card offers it on an open sheet, Accept makes it up again and asks the engraving again (${n} checks)`);
})().catch(e => { console.error(e); process.exit(1); });
