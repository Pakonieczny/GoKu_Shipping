// MULTICHARM end to end (Paul, 25 Nov 2026: "Ability to add several charms at once in one .ai file. The system should be able to identify individual charms in a given illustrator file and then add
// all of them to that particular order ... this order has to be treated as an order with multiple pieces ... correctly processed identified and tracked throughout the system process flow"; his
// decision: the file is the whole order, one piece per charm whatever the line's quantity, "but the users should still be able to manually change the quantity of each charm inside the popup modal").
// In headless Chromium against the local fake site (bridge-server.cjs: the real handlers over an in-memory store; every other request aborted; nothing live). A custom necklace order of quantity 1:
//   1  a single .ai drawing THREE charms (two the same size, one larger) dropped on its Review card: the window lists the three charms, one piece of each ("1 design · 3 pieces to cut")
//   2  each charm's own count: charm 1 × 2, charm 2 × 0 (left out), charm 3 × 1 → "3 pieces"; the card says it too
//   3  Release Hold → Nest it (NESTASK): the line becomes three pieces of ONE order: three pool rows on the line, each naming the charm of the file it is (customIndex 0, 0, 2), each cut from exactly that charm
//      (the two of charm 1 the same size, the one of charm 3 its own larger size; charm 2, the same size as charm 1, is not cut), side by side on one sheet of the order's metal
//   4  the order is counted as three pieces: the line's pieceCount and customPieces, the order window's pieces (OrderPieces.of)
//   5  a reload: the pieces, their charm and the count come back from the cloud as they were
//   node tests/charm-nest/custom-multicharm-e2e.cjs     (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 10, 30, 17) / 1000);
const RID = '4190000001', TID = '41900000011', KEY = `${RID}_${TID}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer 0001' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TID, listingId: '1800000011', sku: 'CUSTOM-N-001-665441', title: 'Custom Name Necklace, Personalized Gold Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] };
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  // the .ai: three closed circles side by side, each with a hole (two of radius 6 mm, one of 8 mm): three separate charms in one file (an .ai is a PDF)
  global.window = global; global.PDFLib = require(path.join(root, 'vendor/pdf-lib-1.17.1.min.js'));
  const D = require(path.join(root, 'charm-nest-dxf.js'));
  const ring = (x, r) => [0, 'CIRCLE', 8, '0', 10, x, 20, 0, 40, r, 0, 'CIRCLE', 8, '0', 10, x, 20, r - 1.6, 40, 0.9];
  const three = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES', ...ring(0, 6), ...ring(20, 6), ...ring(44, 8), 0, 'ENDSEC', 0, 'EOF');
  const AI3 = Buffer.from((await D.toPdf(three, 'three.dxf')).bytes).toString('base64');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; console.log('ok   ' + m); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(e.message));
    const boot = async () => {
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => !window.__oldPage && window.CN && window.Orders && window.Review && window.CustomSheet && window.OrderPieces && CN.S.cloud.ok === true, null, { timeout: 60000 });
    };
    await boot();
    await page.evaluate(async order => {
      await Orders.loadMaps(true);
      for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, ORDER);
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const card = `#rvList .reviewListRow[data-rid="${RID}"]`;
    await page.waitForSelector(card);
    /* ── 1 ── */
    await page.evaluate(({ sel, b64 }) => {
      const dt = new DataTransfer(); dt.items.add(new File([Uint8Array.from(atob(b64), c => c.charCodeAt(0))], 'three charms.ai'));
      const node = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) node.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt }));
    }, { sel: card, b64: AI3 });
    await page.waitForFunction(() => !!document.querySelector('#cuAsk [data-ask-yes]'), null, { timeout: 10000 }).then(() => page.click('#cuAsk [data-ask-yes]')).catch(() => {});   // (the first design of an order asks first, REVDROP)
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuCharm').length === 3 && document.querySelectorAll('#cuDlg .cuCharm img').length === 3, null, { timeout: 30000 });
    const w1 = await page.evaluate(() => ({ sum: document.getElementById('cuSum').textContent, meta: document.querySelector('#cuDlg .cuFile .cuMeta .dim').textContent, rows: [...document.querySelectorAll('#cuDlg .cuCharm')].map(c => c.textContent.replace(/\s+/g, ' ').trim()) }));
    ok(/^1 design · 3 pieces to cut/.test(w1.sum), `1 one .ai drawing three charms on a quantity-1 line: one piece of each (${w1.sum})`);
    ok(/3 charms/.test(w1.meta) && w1.rows.length === 3 && w1.rows.every(r => /× 1/.test(r)), `1 the window lists the three charms, each × 1 (${w1.rows.join(' | ')})`);
    const sizes = await page.evaluate(() => [...document.querySelectorAll('#cuDlg .cuCharm .cuCn i')].map(i => +((i.textContent.match(/(\d+\.\d) ×/) || [])[1] || 0)));
    const big = sizes.indexOf(Math.max(...sizes)), small = [0, 1, 2].filter(i => i !== big), [s1, s2] = small;
    ok(sizes.length === 3 && Math.abs(sizes[s1] - sizes[s2]) < 0.15 && sizes[big] > sizes[s1] + 3, `1 each charm with its own size: two of ${sizes[s1]} mm (charms ${s1 + 1}, ${s2 + 1}), one of ${sizes[big]} mm (charm ${big + 1})`);
    /* ── 2 ── */
    await page.click(`#cuDlg .cuCharm:nth-child(${s1 + 1}) [data-q="1"]`);    // (one of the two equal charms twice)
    await page.click(`#cuDlg .cuCharm:nth-child(${s2 + 1}) [data-q="-1"]`);   // (the other, the same size, left out: only its index tells it apart)
    const w2 = await page.evaluate(() => ({ sum: document.getElementById('cuSum').textContent, off: [...document.querySelectorAll('#cuDlg .cuCharm')].map(c => c.classList.contains('off')), q: [...document.querySelectorAll('#cuDlg .cuCharm .cuQty b')].map(b => b.textContent), total: (document.querySelector('#cuDlg .cuQtySum') || {}).textContent }));
    const wantQ = [0, 1, 2].map(i => i === s1 ? '× 2' : i === s2 ? '× 0' : '× 1');
    ok(/^1 design · 3 pieces to cut/.test(w2.sum) && JSON.stringify(w2.q) === JSON.stringify(wantQ), `2 each charm its own count: ${w2.q.join(', ')} → ${w2.sum}`);
    ok(JSON.stringify(w2.off) === JSON.stringify([0, 1, 2].map(i => i === s2)) && /3 pieces/.test(w2.total || ''), `2 charm ${s2 + 1} left out (shown so), the file makes ${w2.total}`);
    await page.click('#cuDlg [data-x]');
    const strip = await page.evaluate(sel => (document.querySelector(sel + ' .cuDesigns') || {}).textContent || '', card);
    ok(/1 design · 3 pieces/.test(strip), `2 the card says it too (${strip.trim()})`);
    /* ── 3 ── */
    // (NESTASK, 23 Nov 2026: the card's Release Hold asks "Nest it or Skip it"; Nest it sends the designs to the sheets)
    await page.click(card + ' [data-cu-release]');
    await page.waitForSelector('dialog.naDlg[open] [data-na="nest"]', { timeout: 15000 });
    const ask = await page.evaluate(() => document.querySelector('dialog.naDlg[open]').textContent.replace(/\s+/g, ' '));
    ok(/3 pieces/.test(ask), `3 Release Hold asks Nest it or Skip it, saying the file makes 3 pieces`);
    await page.click('dialog.naDlg[open] [data-na="nest"]');
    await page.waitForFunction(k => CustomSheet.decisionOf(B.orders.byKey.get(k)) && !document.querySelector('#rvList .cuStat, #rvList .btn.working'), KEY, { timeout: 30000 }).catch(async e => { throw new Error(e.message + ' · ' + JSON.stringify(await page.evaluate(k => ({ dialogs: [...document.querySelectorAll('dialog[open]')].map(d => (d.id || d.className) + ': ' + d.textContent.replace(/\s+/g, ' ').slice(0, 300)), toasts: [...document.querySelectorAll('#toasts .toast')].map(t => t.dataset.msg), sent: !!CustomSheet.sentOf(B.orders.byKey.get(k)), state: B.orders.byKey.get(k).state, busy: [...document.querySelectorAll('#rvList .cuStat')].map(x => x.textContent) }), KEY))); });
    const placed = await page.evaluate(async k => {
      const row = B.orders.byKey.get(k); await Pool.poolAdd(row, null); Orders.interpretAll(); Review.render();
      const charms = allSheets().flatMap(p => p.charms.filter(c => c.custom && c.order === row.order.receiptId).map(c => ({ sheet: p.metal + ':' + (p.sheetId || p.page), poolId: c.poolId, ci: c.customIndex, w: Math.round(c.widthPt * 100) / 100 })));
      return { st: row.state, ids: row.poolIds.slice(), pools: row.poolIds.map(id => { const p = B.pool.rows.get(id); return { ci: p.customIndex, material: p.material, file: p.customFile }; }), charms, spec: { pieceCount: row.spec.pieceCount, customPieces: row.spec.customPieces, quantity: row.spec.quantity } };
    }, KEY);
    ok(placed.st === 'pooled' && placed.ids.length === 3 && placed.ids.every(id => id.startsWith(KEY + '_')), `3 the line is three pieces of the one order (${placed.ids.join(', ')})`);
    const wantCi = JSON.stringify([s1, s1, big].sort());
    ok(JSON.stringify(placed.pools.map(p => p.ci).sort()) === wantCi && placed.pools.every(p => p.material === 'gold' && p.file === 'three charms.ai'), `3 each pool row names its charm of the file: ${placed.pools.map(p => p.ci).join(', ')} (charm ${s2 + 1}, the same size as charm ${s1 + 1}, not cut), all on Gold Filled`);
    const byCi = ci => placed.charms.filter(c => c.ci === ci);
    ok(placed.charms.length === 3 && byCi(s1).length === 2 && byCi(big).length === 1 && !byCi(s2).length, '3 three pieces on the sheets, each cut from its own charm');
    ok(byCi(s1)[0].w === byCi(s1)[1].w && byCi(big)[0].w > byCi(s1)[0].w + 8, `3 the two of charm ${s1 + 1} the same size (${byCi(s1)[0].w} pt), the one of charm ${big + 1} its own larger size (${byCi(big)[0].w} pt)`);
    ok(new Set(placed.charms.map(c => c.sheet)).size === 1, `3 side by side on one sheet of the order's metal (${placed.charms[0].sheet})`);
    /* ── 4 ── */
    ok(placed.spec.pieceCount === 3 && placed.spec.customPieces === 3 && placed.spec.quantity === 1, `4 the order is counted as three pieces (pieceCount ${placed.spec.pieceCount}, customPieces ${placed.spec.customPieces}; Etsy quantity ${placed.spec.quantity})`);
    const ow = await page.evaluate(rid => OrderPieces.of(rid).map(p => ({ poolId: p.poolId, copy: p.copy, qty: p.qty })), RID);
    ok(ow.length === 3 && ow.every(p => p.qty === 3), `4 the order window counts three pieces of three (${ow.map(p => p.copy + '/' + p.qty).join(', ')})`);
    /* ── 5 ── */
    await page.evaluate(async () => { await Session.flush(); window.__oldPage = true; });
    await boot();
    await page.waitForFunction(k => { const r = B.orders.byKey.get(k); return r && (r.poolIds || []).length === 3; }, KEY, { timeout: 60000 });
    const back = await page.evaluate(k => { const row = B.orders.byKey.get(k); return { pools: row.poolIds.map(id => (B.pool.rows.get(id) || {}).customIndex), pieceCount: row.spec.pieceCount, customPieces: row.spec.customPieces }; }, KEY);
    const cloudRows = placed.ids.map(id => (srv.st.doc('Charm_Pool', id) || {}).customIndex);
    ok(JSON.stringify(back.pools) === JSON.stringify(placed.pools.map(p => p.ci)) && JSON.stringify(cloudRows) === JSON.stringify(placed.pools.map(p => p.ci)) && back.pieceCount === 3 && back.customPieces === 3, `5 after a reload the pieces, their charm (${back.pools.join(', ')}; the cloud's rows ${cloudRows.join(', ')}) and the count (${back.pieceCount}) are as they were`);
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | ')); n++;
    console.log(`\n${n}/${n} checks pass`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
