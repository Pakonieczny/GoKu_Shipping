// Every Review card has the Custom Orders card's action column (Paul, 28 Sep 23:55; 29 Sep 00:38: "Remove the review
// and resolve button and make all the buttons look the same as in the other tabs … Including the drop zone purple menu").
// An Unknown SKU or Options card has no "Review & resolve": Print QR label (the primary until its designs are ready),
// Complete Order, Send to Sheet (greyed and disabled until a design is dropped and read, the primary then) and the dashed
// drop zone, the markup, classes and sizes of a custom card's, run by the same code; its question is answered in the
// order window a click on the card opens. A line leaving Review this way has its question answered on the order timeline
// with who, and a completed one is under Completed with its seal. Runs the sorter in headless Chromium against the local
// fake site (bridge-server.cjs): every request that is not to the loopback is aborted and the label printer is a stub, so
// nothing is printed or written live.
//   node tests/charm-nest/rv-all-buttons.cjs [playwright-core dir]   (CN_SHOT=file saves an Unknown SKU and a custom card)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations, metal) => ({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: metal || '', metalLabel: metal === 'silver' ? 'Sterling Silver' : metal === 'gold' ? 'GF 14/20' : '', personalization: [], buyerMessage: '' });
const ORDERS = [
  order(4175892473, [line(41758924731, 'DAVID STAR', 'David Star Necklace', [['Metal Choice', 'Gold Filled']], 'gold')]),              // Unknown SKU, one order
  order(4177368830, [line(41773688301, 'SURFER_4264', 'Surfer Wave Studs', [['Metal', 'Sterling Silver']], 'silver')]),                 // Unknown SKU, two orders
  order(4177368831, [line(41773688311, 'SURFER_4264', 'Surfer Wave Studs', [['Metal', 'Sterling Silver']], 'silver')]),
  order(4178100001, [line(41781000011, 'ROSE_77', 'Rose Charm', [['Metal', 'Sterling Silver']], 'silver')]),                             // Unknown SKU, sent with its own design
  order(4178100002, [line(41781000021, 'BLOOMING_20239', 'Blooming Flower Charm Necklace', [['Metal', '14k Gold Filled'], ['Style', 'Wavy']], 'gold')]),   // Options
  order(4174476673, [line(41744766731, 'CUSTOM_6673', 'CUSTOM CHARM', [['Price', '28']])])                                               // Custom Orders
];
const CUSTOM = '4174476673';
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DESIGN_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, 18, 20, 0, 42, 0.4, 10, 18, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, 9, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const WANT = ['Print QR label', 'Complete Order', 'Send to Sheet', 'Drop .ai / .dxf designs here'];
// a card's action column, as drawn: each control's classes, disabled state, primary and size
const COLUMN = sel => { const n = document.querySelector(sel); return [...n.querySelectorAll('.rowActions > *')].map(b => { const r = b.getBoundingClientRect(); return { t: b.textContent.trim(), cls: b.className, dis: b.getAttribute('aria-disabled'), w: Math.round(r.width), h: Math.round(r.height) }; }); };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1720, height: 950 }, deviceScaleFactor: 2 });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const PDFMAKE = `window.pdfMake = { createPdf(dd) { const top = window.parent; (top.__qrDocs = top.__qrDocs || []).push(JSON.parse(JSON.stringify(dd)));
      return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { window.parent.parent.__printed = (window.parent.parent.__printed || 0) + 1; };<\\/script>'], { type: 'text/html' })); } }; } };`;
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(PDFMAKE));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy/i.test(u)) outside.push(u);
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.CustomSheet && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      // the order timeline, as it is recorded (CNTimeline.line is what answered questions go through)
      window.__tl = []; const was = CNTimeline.line; CNTimeline.line = (row, type, o) => { __tl.push({ key: row && row.key, type, by: o && o.by, text: o && o.text }); return was(row, type, o); };
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, ORDERS);
    const card = rid => `#rvList .reviewListRow[data-rid="${rid}"]`;
    const buttons = () => page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(n => ({ rid: n.dataset.rid, b: [...n.querySelectorAll('.rowActions button')].map(b => b.textContent.trim()), h: Math.round(n.getBoundingClientRect().height) })));

    // 1 · Unknown SKU: every card, no "Review & resolve", the custom card's column: its buttons in its order, classes and
    //     sizes, Send to Sheet greyed and disabled (no design yet) and the dashed drop zone under them
    await page.click('#reviewView .egTab[data-k="unmatchedSku"]');
    let list = await buttons();
    assert.deepEqual(list.map(x => x.rid).sort(), ['4175892473', '4177368830', '4178100001'].sort(), JSON.stringify(list));
    for (const x of list) assert.deepEqual(x.b, WANT, x.rid + ': ' + x.b.join(' | '));
    assert.equal(await page.$('#rvList [data-review-open]'), null, 'no Review & resolve');
    const sizes = await page.evaluate(sel => { const n = document.querySelector(sel); const hs = [...n.querySelectorAll('.rowActions .btn')].map(b => Math.round(b.getBoundingClientRect().height)); const act = n.querySelector('.rowActions').getBoundingClientRect(), side = n.querySelector('.engravingIdentity').getBoundingClientRect(); return { hs, act: Math.round(act.height), card: Math.round(n.getBoundingClientRect().height), dropOk: n.classList.contains('cuDropOk'), side: Math.round(side.height) }; }, card('4175892473'));
    assert(sizes.hs.every(h => h === sizes.hs[0]), 'the buttons are one size: ' + sizes.hs);
    assert(sizes.dropOk, 'the card itself takes a drop too');
    // (in the wide list the buttons are the right-hand column, as a custom card's: the card is as tall as they are)
    assert(sizes.card <= Math.max(sizes.act, sizes.side, 138) + 40, 'no taller than it needs: ' + JSON.stringify(sizes));
    const skuCol = await page.evaluate(COLUMN, card('4175892473'));
    assert.match(skuCol[0].cls, /\bgold\b/, 'Print QR label is the primary while no design is ready');
    assert.equal(skuCol[2].dis, 'true', 'Send to Sheet is disabled with no design'); assert.match(skuCol[2].cls, /\bghost\b/);
    assert.match(await page.getAttribute(card('4175892473') + ' [data-cu-send]', 'title'), /Drop the order's \.ai or \.dxf designs/, 'and says why');
    assert.equal(skuCol[3].cls, 'cuHint', 'the dashed drop zone');
    // the Custom Orders card: the same column, control for control (classes, disabled state, width and height)
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const cuCol = await page.evaluate(COLUMN, card(CUSTOM));
    const same = c => c.map(({ t, cls, dis, w, h }) => ({ t, cls: cls.replace(/\s*\bworking\b/, ''), dis, w, h }));
    assert.deepEqual(same(skuCol), same(cuCol), 'Unknown SKU and Custom Orders: one column\n' + JSON.stringify(skuCol) + '\n' + JSON.stringify(cuCol));
    const shot = process.env.CN_SHOT || '/mnt/project-files/plans/review-buttons-2.png';
    try {
      const cuPng = await page.locator(card(CUSTOM)).screenshot();
      await page.click('#reviewView .egTab[data-k="unmatchedSku"]');
      const skuPng = await page.locator(card('4175892473')).screenshot();
      const side2 = await context.newPage();
      await side2.setContent(`<body style="margin:0;padding:16px;background:#efe9dd;display:flex;gap:16px;align-items:flex-start;font:600 13px system-ui">${[['Unknown SKU', skuPng], ['Custom Orders', cuPng]].map(([t, b]) => `<figure style="margin:0;flex:1"><figcaption style="margin:0 0 6px">${t}</figcaption><img style="width:100%" src="data:image/png;base64,${b.toString('base64')}"></figure>`).join('')}</body>`);
      await side2.setViewportSize({ width: 3400, height: 400 });
      fs.mkdirSync(path.dirname(shot), { recursive: true }); await side2.screenshot({ path: shot, fullPage: true }); await side2.close();
      console.log('  · screenshot: ' + shot);
    } catch (e) { console.log('  – screenshot not saved: ' + e.message); await page.click('#reviewView .egTab[data-k="unmatchedSku"]').catch(() => {}); }

    // 2 · a click on the card opens the order, whose window asks the Unknown SKU question (the charm to use, nothing to
    //     cut, hold the order): what "Review & resolve" showed
    await page.click(card('4175892473') + ' .purchaseSummary');
    await page.waitForFunction(() => OrderWin.isOpen() && document.querySelector('#owFix [data-a=alias]'), null, { timeout: 10000 });
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'one window');
    assert.deepEqual(await page.evaluate(() => ['alias', 'nodesign', 'hold'].map(a => !!document.querySelector(`#owFix [data-a=${a}]`))), [true, true, true], 'the Unknown SKU controls, in the order window');
    await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen());

    // 3 · Complete Order on an Unknown SKU line: completed by hand (how "button", by who), its question answered with who,
    //     the card leaves for Completed with its seal, under the Unknown SKU chip there
    const aKey = '4175892473_41758924731';
    await page.click(card('4175892473') + ' [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k] && !document.querySelector('#rvList .reviewListRow[data-rid="4175892473"]'), aKey, { timeout: 30000 });
    const recA = srv.st.doc('Charm_Custom_Orders', aKey);
    assert(recA && recA.how === 'button' && recA.completedBy === 'Test Operator' && recA.category === 'Unknown SKU' && !recA.prints, 'completed on the server record: ' + JSON.stringify(recA));
    await page.waitForFunction(k => __tl.some(e => e.key === k && e.type === 'decided'), aKey, { timeout: 10000 });
    const tlA = await page.evaluate(k => __tl.filter(e => e.key === k && e.type === 'decided'), aKey);
    assert(tlA.length === 1 && tlA[0].by === 'Test Operator' && /Unknown SKU: completed by hand \(Complete Order\)/.test(tlA[0].text), 'the question is answered, with who: ' + JSON.stringify(tlA));
    await page.waitForFunction(() => /^Order 4175892473 moved to Completed · completed by Test Operator/.test(document.querySelector('.mNote .mNoteT')?.textContent || ''), null, { timeout: 10000 });
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close && n.close()));
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    const doneA = await page.evaluate(sel => { const n = document.querySelector(sel); return n && { seals: [...n.querySelectorAll('.seal')].map(x => x.classList.contains('seal-button')), b: [...n.querySelectorAll('.rowActions button')].map(b => b.textContent.trim()), q: n.querySelector('.queueLabel').textContent, l: n.querySelector('.engravingIdentity .purchaseLabel').textContent, chips: [...document.querySelectorAll('#reviewView .egTab')].map(c => c.dataset.k) }; }, card('4175892473'));
    assert(doneA, 'under Completed');
    assert.deepEqual(doneA.seals, [true], 'the green Complete Order seal'); assert.deepEqual(doneA.b, ['Print QR label', 'Reopen']);
    assert.equal(doneA.q, 'Completed by hand'); assert.equal(doneA.l, 'Unknown SKU'); assert(doneA.chips.includes('unmatchedSku'), 'under its own chip there: ' + doneA.chips);
    await page.click('#reviewView .rvSeg [data-cseg="open"]');

    // 4 · two orders, one question: Print QR label acts for the order shown; the card stays for the other one
    await page.click('#reviewView .egTab[data-k="unmatchedSku"]');
    const shown = await page.evaluate(() => { const it = Review.items().find(x => x.key === 'ord:sku:SURFER_4264'); return it.row.key; });
    const other = shown === '4177368830_41773688301' ? '4177368831_41773688311' : '4177368830_41773688301';
    const p0 = await page.evaluate(() => window.__printed || 0);
    await page.click(card(shown.split('_')[0]) + ' [data-cu-print]');
    await page.waitForFunction(({ p0, k }) => (window.__printed || 0) > p0 && B.maps.customDone[k] && !document.querySelector('.cuStat, .btn.working'), { p0, k: shown }, { timeout: 30000 });
    const recS = srv.st.doc('Charm_Custom_Orders', shown);
    assert(recS && recS.prints === 1 && recS.printedBy === 'Test Operator' && recS.category === 'Unknown SKU', 'the shown order is printed and completed: ' + JSON.stringify(recS));
    assert.equal(srv.st.doc('Charm_Custom_Orders', other), undefined, 'the other order is left as it was');
    const stay = await page.evaluate(() => { const it = Review.items().find(x => x.key === 'ord:sku:SURFER_4264'); return it && it.rows.map(r => r.key); });
    assert.deepEqual(stay, [other], 'the question stays open for the other order');
    list = await buttons(); assert.deepEqual(list.find(x => x.rid === other.split('_')[0]).b, WANT);

    // 5 · Send to Sheet: a design dropped on an Unknown SKU card, its metal in the designs window, sent; the line leaves
    //     Review for the sheets, answered with who (the timeline and Completed)
    const eKey = '4178100001_41781000011';
    // with no design yet, Send to Sheet is greyed and disabled: pressed, it sends nothing and opens nothing (its drop
    // zone lights up)
    await page.click(card('4178100001') + ' [data-cu-send]', { force: true });   // (Playwright itself takes it for disabled)
    await page.waitForTimeout(300);
    assert.deepEqual(await page.evaluate(({ sel, k }) => ({ open: document.querySelectorAll('dialog[open]').length, sent: !!CustomSheet.sentOf(B.orders.byKey.get(k)), lit: document.querySelector(sel + ' .cuHint').classList.contains('cuNudge'), op: getComputedStyle(document.querySelector(sel + ' [data-cu-send]')).opacity }), { sel: card('4178100001'), k: '4178100001_41781000011' }), { open: 0, sent: false, lit: true, op: '0.45' });
    await page.evaluate(({ sel, text }) => {
      const dt = new DataTransfer(); dt.items.add(new File([text], 'rose.dxf'));
      const n = document.querySelector(sel);
      for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt }));
    }, { sel: card('4178100001'), text: DESIGN_DXF });
    await page.waitForFunction(() => document.querySelector('#cuDlg[open] .cuFile .cuThumb img'), null, { timeout: 30000 });
    const dlg = await page.evaluate(() => ({ t: document.getElementById('cuDlgT').textContent, on: [...document.querySelectorAll('#cuDlg .cuFile .cuM.on')].map(b => b.dataset.m), open: document.querySelectorAll('dialog[open]').length }));
    assert.deepEqual(dlg, { t: 'Designs for order 4178100001', on: ['silver'], open: 1 }, 'the metal picker window, alone, the order\'s metal first');
    await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
    await page.click('#cuDlg [data-x]');
    await page.waitForFunction(sel => document.querySelector(sel + ' .cuDesigns.ready') && !document.querySelector('dialog[open]'), card('4178100001'));
    const eCard = await page.evaluate(sel => { const n = document.querySelector(sel); return { strip: n.querySelector('.cuDesigns').textContent, send: n.querySelector('[data-cu-send]').className, dis: n.querySelector('[data-cu-send]').getAttribute('aria-disabled'), print: n.querySelector('[data-cu-print]').className, b: [...n.querySelectorAll('.rowActions button')].map(b => b.textContent.trim()) }; }, card('4178100001'));
    assert.match(eCard.strip, /Custom designs.*1 design · 1 piece.*Ready to send.*Edit designs/); assert.match(eCard.send, /gold/, 'Send to Sheet is the next step'); assert.equal(eCard.dis, null, 'and enabled');
    assert.match(eCard.print, /ghost/, 'Print QR label is plain then');
    assert.deepEqual(eCard.b, ['Print QR label', 'Complete Order', 'Send to Sheet'], 'the same buttons with its designs strip (no drop zone once it has designs)');
    await page.click(card('4178100001') + ' [data-cu-send]');
    await page.waitForFunction(k => CustomSheet.sentOf(B.orders.byKey.get(k)) && !document.querySelector('#rvList .reviewListRow[data-rid="4178100001"]'), eKey, { timeout: 30000 });
    const eRow = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { st: r.state, problems: r.problems.length, metal: CustomSheet.metalOf(r) }; }, eKey);
    assert.deepEqual(eRow, { st: 'pulled', problems: 0, metal: 'gold' }, 'waits for the next run, on the metal picked, its question settled');
    await page.waitForFunction(k => __tl.some(e => e.key === k && e.type === 'decided'), eKey, { timeout: 10000 });
    const tlE = await page.evaluate(k => __tl.filter(e => e.key === k && ['decided', 'designSent'].includes(e.type)).map(e => [e.type, e.by]), eKey);
    assert.deepEqual(tlE.sort(), [['decided', 'Test Operator'], ['designSent', 'Test Operator']], JSON.stringify(tlE));
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    const res = await page.evaluate(sel => { const n = document.querySelector(sel + '.rvSettled'); return n && { why: n.querySelector('.rowExcerpt').textContent, by: n.querySelector('.by').textContent, strip: n.querySelector('.cuDesigns')?.textContent || '' }; }, card('4178100001'));
    assert(res && /sent to the sheets with the order's own designs/.test(res.why) && /^Test Operator/.test(res.by) && /On the sheets.*Sent by Test Operator/.test(res.strip), 'under Completed, with who: ' + JSON.stringify(res));
    await page.click(card('4178100001') + '.rvSettled .purchaseSummary');
    await page.waitForFunction(() => OrderWin.isOpen()); await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen());
    await page.click('#reviewView .rvSeg [data-cseg="open"]');

    // 6 · Options: the same column, and its question in the order window its card opens
    await page.click('#reviewView .egTab[data-k="needsMapping"]');
    list = await buttons();
    assert.deepEqual(list.map(x => [x.rid, x.b]), [['4178100002', WANT]], JSON.stringify(list));
    assert.deepEqual(same(await page.evaluate(COLUMN, card('4178100002'))), same(cuCol), 'Options and Custom Orders: one column');
    await page.click(card('4178100002') + ' .engravingIdentity');
    await page.waitForFunction(() => OrderWin.isOpen() && document.querySelector('#owFix [data-pick]'), null, { timeout: 10000 });
    await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen());
    await page.click('#reviewView .egTab[data-k="needsMapping"]');   // (the filter let go: Open, everything)
    list = await buttons();
    for (const x of list) assert.deepEqual(x.b, WANT, 'Open: ' + x.rid + ' ' + x.b.join(' | '));

    assert.deepEqual(outside, [], 'no Etsy call');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ every Review card: the custom card\'s column (Print QR label, Complete Order, Send to Sheet greyed until ready, drop zone), designs, its question in the order window (Chromium)');
    console.log('Review buttons OK');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
