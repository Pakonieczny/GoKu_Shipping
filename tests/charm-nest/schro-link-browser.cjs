// SCHROLINK, part E · the page (real headless Chromium against the repo's fake site, which runs the REAL charmNestLibrary handler over an in-memory Firestore: zero network, nothing live).
//   node tests/charm-nest/schro-link-browser.cjs [playwright-core dir]      (PW_DIR / CHROMIUM as tennis-link-browser.cjs; skipped without; SL_SHOTS=<dir> saves screenshots)
// Order 4198409468: a Silver 18" necklace, Etsy listing 1648594139, SKU "SCHRODINGER CAT" (a real master design of that name: the huggie skull cat). The real index entries (read live 12 Oct 2026) and the
// real design files of SCHRODINGER CAT 1 and 2 (re-indexed from the shop's master file) are served to the page like Storage serves them. The saved link is written once through the page's own call
// (aliasPut, no sandbox flag = production's store of the fake site).
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const CAT = 'SCHRODINGER CAT', CAT1 = 'SCHRODINGER CAT 1', CAT2 = 'SCHRODINGER CAT 2', LID = '1648594139', RAW = 'SCHRODINGER CAT';
const TITLE = "Schrödinger's Cat Necklace, Whimsical Cat Gift, Fun Jewelry Gift, Birthday Gift, Cat Necklace Cute Jewelry, Dead and Alive Cat Necklace";

(async () => {
  const pwDir = process.env.PW_DIR || process.argv[2] || (fs.existsSync('/opt/node22/lib/node_modules/playwright/node_modules') ? '/opt/node22/lib/node_modules/playwright/node_modules' : path.join(root, 'node_modules'));
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  · schro-link browser part: no playwright-core, not run'); return; }
  const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(chromePath)) { console.log('  · schro-link browser part: no chrome, not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), st = srv.st;
  const masters = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/schro-master-entries.json'), 'utf8')).entries;
  const fileOf = { [CAT]: 'SCHRODINGER_CAT.ai', [CAT1]: 'SCHRODINGER_CAT_1.ai', [CAT2]: 'SCHRODINGER_CAT_2.ai' };
  for (const e of masters) {
    st.put('Charm_Master_Index', e.sku, Object.assign({}, e));
    if (fileOf[e.sku]) st.blobs.set(e.aiPath, { buf: fs.readFileSync(path.join(__dirname, 'fixtures', fileOf[e.sku])), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });
  }
  const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 20, 17) / 1000);
  const V = (name, value) => ({ name, value });
  const line = (tid, listingId) => ({ transactionId: tid, listingId, sku: RAW, title: TITLE, quantity: 1, expectedShipDate: SHIP, productId: '1', metalKey: 'silver', metalLabel: 'Silver', personalization: ['I did not select + engraving'], variations: [V('Metal Choice', 'Silver'), V('Necklace Length in inches', '18"'), V('Personalization', 'I did not select + engraving')] });
  const order = (rid, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Test Buyer' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
  const A = '4198409468', E = '4198409999';
  const orders = [order(A, [line('5247566510', LID)]), order(E, [line('5247566511', '1999000777')])];   // (E: the same SKU and title on ANOTHER listing)
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  const errors = [], shots = process.env.SL_SHOTS || '';
  async function session() {
    const context = await browser.newContext({ viewport: { width: 1500, height: 1500 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(); page.setDefaultTimeout(40000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && window.OrderPieces && window.PieceMedia && window.Pool && CN.S.cloud.ok === true && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const o of orders) o.lines.forEach(l => { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
      window.__ink = async src => {   // the picture's ink clusters above the chip band, and the chips
        const im = new Image(); im.src = src; await im.decode(); const cv = document.createElement('canvas'); cv.width = im.width; cv.height = im.height; const c = cv.getContext('2d'); c.drawImage(im, 0, 0); const d = c.getImageData(0, 0, im.width, im.height).data;
        const ink = (x, y) => { const i = (y * im.width + x) * 4; return !(d[i] > 200 && d[i + 1] > 200 && d[i + 2] > 200); }, dark = (x, y) => { const i = (y * im.width + x) * 4; return d[i] < 70 && d[i + 1] < 70 && d[i + 2] < 70; };
        const band = Math.round(im.height * .16), H = im.height - band;
        const cl = (y0, y1, f) => { const cols = []; for (let x = 0; x < im.width; x++) { let h = false; for (let y = y0; y < y1 && !h; y++) h = f(x, y); cols.push(h); } const out = []; let s = -1; for (let x = 0; x <= im.width; x++) { if (cols[x] && s < 0) s = x; if (!cols[x] && s >= 0) { out.push([s, x - 1]); s = -1; } } return out; };
        return { w: im.width, h: im.height, bodies: cl(0, im.height, ink), chips: cl(H, im.height, dark), bandInk: cl(H, im.height, ink) };
      };
    }, orders);
    return { page, context };
  }
  const rowState = (page, rid) => page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid), s = r.spec, p = s && s.pair; return { designSku: s.designSku, source: p && p.source, two: !!(p && p.set), designs: p && p.set && p.set.designs, members: p && p.members, earring: p && p.earring, problems: s.problems.map(x => x.kind + (x.reason ? ':' + x.reason : '')), pieces: s.pieceCount, sides: p && (p.sides || []).join('|'), notes: p && p.notes }; }, rid);
  const vectorsDrawn = async (page, sel) => { await page.waitForFunction(s => { const hs = [...document.querySelectorAll(s)]; return hs.length > 0 && hs.every(h => h.getAttribute('aria-busy') === 'false'); }, sel, { timeout: 60000 }); return page.evaluate(async s => Promise.all([...document.querySelectorAll(s)].map(async h => { const im = h.querySelector('img'), nd = h.closest('[data-rid]'); return { text: h.textContent.trim(), rid: nd ? nd.dataset.rid : '', pic: im && im.src ? await window.__ink(im.src) : null }; })), sel); };
  try {
    const { page, context } = await session();
    /* 1 · before the link: the defect Paul saw: ONE piece of the SKU's own design (the huggie skull cat) */
    const before = { A: await rowState(page, A), E: await rowState(page, E) };
    ok(before.A.pieces === 1 && !before.A.two && (before.A.designSku === CAT || before.A.problems.some(p => /huggie design in the library/.test(p))), 'before the link, the necklace is one piece of ' + CAT + ' (the huggie drawing), which the form guard now holds with a plain line: ' + JSON.stringify(before.A));
    /* 2 · the saved link, once, through the page's own call (no sandbox flag: production's store of the fake site) */
    const put = await page.evaluate(async ({ LID, RAW, CAT1, CAT2 }) => CN.api('charmNestLibrary', { op: 'aliasPut', listingId: LID, fromSku: RAW, pair: { L: CAT1, R: CAT2 }, by: 'SCHROLINK', title: "Schrödinger's Cat Necklace" }), { LID, RAW, CAT1, CAT2 });
    ok(put && put.ok === true, 'the real call saved the pair: ' + JSON.stringify(put));
    const calls = st.calls.filter(c => c.name === 'charmNestLibrary' && c.body.op === 'aliasPut');
    ok(calls.length === 1 && !calls[0].body.sandbox, 'ONE call, a production call (no sandbox flag)');
    ok(![...st.docs.keys()].some(k => k.startsWith('Sandbox_Charm_Sku_Aliases/')), 'nothing was written to a sandbox copy');
    const rec = st.doc('Charm_Sku_Aliases', LID); ok(rec && Object.keys(rec.pairBySku).join('|') === RAW && rec.pairBySku[RAW].L === CAT1 && rec.pairBySku[RAW].R === CAT2, 'production holds ONE record for the listing, under its SKU: ' + JSON.stringify(rec && rec.pairBySku));
    await page.evaluate(async () => { await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); Orders.render(); });
    const after = { A: await rowState(page, A), E: await rowState(page, E) };
    ok(after.A.two && after.A.designSku === CAT1 && after.A.pieces === 2 && !after.A.earring && after.A.members === null && after.A.problems.length === 0, 'now a necklace of two charms, read from the saved link, no question: ' + JSON.stringify(after.A));
    ok(JSON.stringify(after.A.designs) === JSON.stringify([CAT1, CAT2]) && after.A.sides === '|', 'CAT 1 + CAT 2, no side on either piece: ' + JSON.stringify([after.A.designs, after.A.sides]));
    ok(after.E.pieces === 1 && !after.E.two && JSON.stringify([after.E.designSku, after.E.problems]) === JSON.stringify([before.E.designSku, before.E.problems]), 'the same SKU on another listing is not changed: ' + JSON.stringify(after.E));
    /* 3 · Review: the line has no row */
    await page.evaluate(() => { Review.syncOrderItems(); CN.setMode('review'); Review.render(); });
    const rv = await page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(r => r.dataset.rid));
    ok(!rv.includes(A), 'Review has no row for the necklace: ' + JSON.stringify(rv));
    /* 4 · Orders tab: one row, BOTH cats side by side, no Left / Right chips */
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    const vecs = await vectorsDrawn(page, '#ordItems [data-vector]');
    ok(vecs.length === 2, 'two rows: ' + vecs.length);
    const va = vecs.find(x => x.rid === A); ok(va && !/Unavailable|No vector/.test(va.text), 'the necklace\'s "Vector design" reads neither "Unavailable · Retry" nor "No vector available" (' + (va && va.text || 'a picture') + ')');
    ok(va.pic && va.pic.bodies.length === 2, 'the Orders row draws BOTH cats side by side: ' + JSON.stringify(va.pic && va.pic.bodies));
    ok(va.pic.chips.length === 0 && va.pic.bandInk.length === 0, 'with no Left or Right chip (a necklace has no ears): ' + JSON.stringify([va.pic.chips, va.pic.bandInk]));
    const ve = vecs.find(x => x.rid === E); ok(ve && (!ve.pic || ve.pic.bodies.length <= 1), 'the other listing\'s row is not drawn as two charms: ' + JSON.stringify(ve && ve.pic && ve.pic.bodies));
    const facts = await page.evaluate(rid => { const nd = document.querySelector(`#ordItems > [data-rid="${rid}"]`); return { facts: ((nd.querySelector('.rowFacts') || {}).textContent || '').replace(/\s+/g, ' '), cap: ((nd.querySelector('.comparePair figure:first-child figcaption') || {}).textContent || '') }; }, A);
    ok(!/Left|Right/.test(facts.facts) && !/Left|Right/.test(facts.cap), 'the row says nothing of a Left or a Right: ' + JSON.stringify(facts));
    /* 5 · the pieces one by one: each piece is its OWN design, never one design twice (piece dots, Engrave tab, efficiency tiles) */
    const pcs = await page.evaluate(async rid => {
      const r = B.orders.rows.find(x => x.order.receiptId === rid), get = async d => { const s = await PieceMedia.vectorThumb(r, { design: d, px: 160 }); return s ? await window.__ink(s) : null; };
      const one = await get('SCHRODINGER CAT 1'), two = await get('SCHRODINGER CAT 2');
      return { one, two, k1: PieceMedia.vectorKey(r, { design: 'SCHRODINGER CAT 1' }), k2: PieceMedia.vectorKey(r, { design: 'SCHRODINGER CAT 2' }), kLine: PieceMedia.vectorKey(r, { pair: true }), list: CharmNestOrders.piecesOf(r.spec).map(p => p.sku) };
    }, A);
    ok(pcs.one && pcs.two && pcs.one.bodies.length === 1 && pcs.two.bodies.length === 1, 'a piece asked for by its design is one cat: ' + JSON.stringify([pcs.one && pcs.one.bodies, pcs.two && pcs.two.bodies]));
    ok(pcs.k1 !== pcs.k2 && /SCHRODINGER CAT 1\|/.test(pcs.k1) && /SCHRODINGER CAT 2\|/.test(pcs.k2) && /\|set:SCHRODINGER CAT 1\+SCHRODINGER CAT 2/.test(pcs.kLine), 'each piece its own picture key, the line\'s own key is the pair\'s: ' + JSON.stringify([pcs.k1, pcs.k2, pcs.kLine]));
    ok(pcs.one.w !== pcs.two.w || pcs.one.h !== pcs.two.h, 'the two cats are two different drawings');
    ok(JSON.stringify(pcs.list) === JSON.stringify([CAT1, CAT2]), 'the pieces list names CAT 1 then CAT 2');
    /* 6 · the order window: the pair picture and one row per piece */
    await page.evaluate(rid => { OrderWin.open(B.orders.rows.find(r => r.order.receiptId === rid).key, { view: 'info' }); }, A);
    await page.waitForFunction(() => { const v = document.getElementById('owVector'); return v && v.querySelector('img') && v.getAttribute('aria-busy') !== 'true'; }, null, { timeout: 30000 });
    const win = await page.evaluate(async () => { const im = document.querySelector('#owVector img'); return { pic: await window.__ink(im.src), sku: document.getElementById('owSku').textContent, rows: [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => ({ side: r.dataset.side || '', text: r.textContent.replace(/\s+/g, ' ').trim().slice(0, 160) })) }; });
    ok(win.pic.bodies.length === 2 && win.pic.chips.length === 0, 'the order window header draws both cats, no chips: ' + JSON.stringify([win.pic.bodies, win.pic.chips]));
    ok(win.sku === 'SKU: SCHRODINGER CAT 1 + SCHRODINGER CAT 2', 'the Vector design line names both designs: ' + win.sku);
    ok(win.rows.length === 1 && !win.rows[0].side && /SCHRODINGER CAT 1 \+ SCHRODINGER CAT 2/.test(win.rows[0].text) && /2 pieces/.test(win.rows[0].text) && !/Left|Right/.test(win.rows[0].text), 'its pieces list names both cats, 2 pieces, no Left or Right: ' + JSON.stringify(win.rows));
    if (shots) { await page.waitForTimeout(500); await page.screenshot({ path: path.join(shots, 'SCHROLINK-order-window.png') }); }
    await page.evaluate(() => OrderWin.close()); await page.waitForTimeout(200);
    if (shots) { await page.evaluate(() => { CN.setMode('orders'); Orders.render(); window.scrollTo(0, 0); }); await vectorsDrawn(page, '#ordItems [data-vector]'); await page.screenshot({ path: path.join(shots, 'SCHROLINK-orders.png') }); }
    /* 7 · a line pooled BEFORE the link (one piece of the SKU's own design, on a draft sheet): the Review "Order changed" card offers the new reading; Accept makes the line up again */
    const offered = await page.evaluate(async rid => {
      const r = B.orders.rows.find(x => x.order.receiptId === rid), id = CharmNestOrders.poolId(r.order, r.line, 1);
      B.pool.rows.set(id, { poolId: id, state: 'ready', sku: 'SCHRODINGER CAT', copy: 1, quantity: 1, charmHash: 'x', sheetId: null, orderId: rid, material: r.spec.material });
      r.poolIds = [id]; r.state = 'pooled'; Pool.pinPooled(r);
      CN.setMode('review'); Review.render(); await new Promise(res => setTimeout(res, 300));
      const row = document.querySelector('#rvList .reviewListRow[data-rid="' + rid + '"]'), b = row && row.querySelector('[data-rv-accept]');
      return { flagged: [r.changePending, r.repoolChanged], pieces: r.spec.pieceCount, items: B.review.items.map(i => i.kind + ':' + i.key), rowText: row ? row.textContent.replace(/\s+/g, ' ') : '', buttons: row ? [...row.querySelectorAll('button')].map(q => q.textContent.trim()).filter(Boolean) : [], accept: b ? b.textContent.trim() : '' };
    }, A);
    ok(offered.flagged.join() === 'true,true' && offered.pieces === 2, 'the pooled line is flagged to be made up again at 2 pieces: ' + JSON.stringify(offered.flagged));
    ok(/Changed/.test(offered.rowText) && /SCHRODINGER CAT 1 \+ SCHRODINGER CAT 2/.test(offered.rowText) && /open sheet/.test(offered.rowText), 'Review has a row for the order: Changed, both designs, why: ' + offered.rowText.slice(0, 500));
    ok(offered.accept === 'Accept the new order' && !offered.buttons.includes('Hold'), 'with the button Accept the new order and no Hold (NOHOLDREV, Paul 17 Oct 2026): ' + JSON.stringify(offered.buttons));
    if (shots) await page.screenshot({ path: path.join(shots, 'SCHROLINK-review-row.png') });
    const writesBefore = st.calls.filter(c => c.name === 'charmNestLibrary' && /alias|master/i.test(String(c.body.op || ''))).length;
    await page.evaluate(rid => { document.querySelector('#rvList .reviewListRow[data-rid="' + rid + '"] [data-rv-accept]').click(); }, A);
    await page.waitForFunction(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); return !r.changePending && !(r.poolIds || []).length; }, A, { timeout: 20000 });
    const accepted = await page.evaluate(async rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); await new Promise(res => setTimeout(res, 300)); return { changePending: r.changePending, repoolChanged: r.repoolChanged, poolIds: r.poolIds, state: r.state, pieces: r.spec.pieceCount, two: !!r.spec.pair.set, hold: r.hold || null, items: B.review.items.map(i => i.kind), oldRow: (B.pool.rows.get(CharmNestOrders.poolId(r.order, r.line, 1)) || {}).state || 'gone' }; }, A);
    ok(accepted.changePending === false && accepted.repoolChanged === false && accepted.poolIds.length === 0 && accepted.two && accepted.pieces === 2 && !accepted.hold, 'Accept took the old piece off and the line is made up again as two pieces: ' + JSON.stringify(accepted));
    ok(!accepted.items.includes('orderChanged'), 'the Review row is gone');
    ok(st.calls.filter(c => c.name === 'charmNestLibrary' && /alias|master/i.test(String(c.body.op || ''))).length === writesBefore, 'Accept wrote no alias and no master');
    await context.close();
    ok(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`  ✓ E · the page: both cats side by side with no chips, a piece per design, no Review row, the Order changed card offers the new reading and Accept makes it up again (${n} checks)`);
})().catch(e => { console.error(e); process.exit(1); });
