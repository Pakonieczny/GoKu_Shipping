// CHAMMAP, part D · the page (real headless Chromium against the repo's fake site, which runs the REAL charmNestLibrary handler over an in-memory Firestore: zero network, nothing live).
//   node tests/charm-nest/generic-sku-browser.cjs [playwright-core dir]      (PW_DIR / CHROMIUM as schro-link-browser.cjs; skipped without)
// The Chameleon Charm Necklace listing 4494184284 (Etsy SKU CO-GoldCharmO, a shared charm-only placeholder; the master holds that label on the astronaut drawing): the line is held with a plain
// Review card (never the astronaut), "Use this charm" CHAMELEON_28151 saves the listing's answer once through the page's own call (aliasPut, production's store of the fake site), and the line
// then reads as the chameleon. Another listing carrying the same placeholder stays held.
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const CHAM = 'CHAMELEON_28151', LID = '4494184284', OTHER = '1999000777', RAW = 'CO-GoldCharmO', TITLE = 'Chameleon Charm Necklace';

(async () => {
  const pwDir = process.env.PW_DIR || process.argv[2] || (fs.existsSync('/opt/node22/lib/node_modules/playwright/node_modules') ? '/opt/node22/lib/node_modules/playwright/node_modules' : path.join(root, 'node_modules'));
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  · generic-sku browser part: no playwright-core, not run'); return; }
  const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(chromePath)) { console.log('  · generic-sku browser part: no chrome, not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), st = srv.st;
  for (const e of JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/chammap-master-entries.json'), 'utf8')).entries) st.put('Charm_Master_Index', e.sku, Object.assign({}, e));
  const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 20, 17) / 1000);
  const V = (name, value) => ({ name, value });
  const line = (tid, listingId) => ({ transactionId: tid, listingId, sku: RAW, title: TITLE, quantity: 1, expectedShipDate: SHIP, productId: '1', metalKey: 'gold', metalLabel: 'Gold Filled', personalization: ['/'], variations: [V('Metal Choice', 'Gold-Charm Only'), V('Necklace Length in inches', 'Charm Only-No Chain'), V('Personalization', '/')] });
  const order = (rid, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Test Buyer' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
  const A = '4199572384', E = '4199572999';
  const orders = [order(A, [line('5247356127', LID)]), order(E, [line('5247356128', OTHER)])];   // (E: the same placeholder SKU and title on ANOTHER listing)
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  const errors = [];
  try {
    const context = await browser.newContext({ viewport: { width: 1500, height: 1500 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(); page.setDefaultTimeout(40000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.Pool && CN.S.cloud.ok === true && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const o of orders) o.lines.forEach(l => { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    }, orders);
    const rowState = rid => page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid), s = r.spec; return { designSku: s.designSku, source: s.skuSource, boughtSku: s.boughtSku, problems: s.problems.map(x => x.kind + (x.formHold ? ':hold' : '')), why: (s.problems[0] || {}).reason || '', state: r.state, reason: r.reason || '' }; }, rid);
    /* 1 · before the link: neither order reads the astronaut; each is held with the plain line */
    const before = { A: await rowState(A), E: await rowState(E) };
    for (const [k, b] of Object.entries(before)) {
      ok(b.designSku === null && b.boughtSku === RAW.toUpperCase() && b.problems.join() === 'unmatchedSku:hold', k + ' is held, no design: ' + JSON.stringify(b));
      ok(/generic charm-only SKU the shop uses on many listings/.test(b.why) && !/ASTRONAUT/i.test(JSON.stringify(b)), k + ' says plainly why, the astronaut is nowhere: ' + b.why);
    }
    /* 2 · Review: one card for each listing (the decision is per listing), each with the picker and "Use this charm" */
    await page.evaluate(() => { Review.syncOrderItems(); CN.setMode('review'); Review.render(); });
    const items = await page.evaluate(() => B.review.items.filter(i => i.kind === 'unmatchedSku').map(i => ({ key: i.key, rids: (i.rows || [i.row]).filter(Boolean).map(r => r.order.receiptId) })));
    ok(items.length === 2 && items.every(i => i.rids.length === 1), 'two cards, one per listing: ' + JSON.stringify(items));
    // (the decision card is Review.card(item), the one the order window shows under "Needs a decision": mounted here in a plain host)
    const cardOf = async rid => { await page.evaluate(rid => { document.querySelectorAll('#cardHost').forEach(h => h.remove()); const host = document.createElement('div'); host.id = 'cardHost'; document.body.appendChild(host); const it = B.review.items.find(i => i.kind === 'unmatchedSku' && (i.rows || [i.row]).some(r => r && r.order.receiptId === rid)); Review.cardIn(host, it); }, rid); return page.evaluate(() => { const c = document.querySelector('#cardHost [data-a=alias]'); return { row: true, picker: !!document.querySelector('#cardHost [data-f=sku]'), button: c ? c.textContent.trim() : '', text: c ? c.closest('.rvItem').textContent.replace(/\s+/g, ' ').slice(0, 500) : 'no card' }; }); };
    const card = await cardOf(A);
    ok(card.row && card.picker && card.button === 'Use this charm', 'the card for the necklace offers the picker and Use this charm: ' + JSON.stringify(card));
    ok(!/ASTRONAUT/i.test(card.text), 'the card never names the astronaut: ' + card.text);
    /* 3 · "Use this charm": the page's own button, once */
    const putsBefore = st.calls.filter(c => c.name === 'charmNestLibrary' && c.body.op === 'aliasPut').length;
    await page.evaluate(CHAM => { const f = document.querySelector('#cardHost [data-f=sku]'); f.value = CHAM; f.dispatchEvent(new Event('input', { bubbles: true })); f.dispatchEvent(new Event('change', { bubbles: true })); document.querySelector('#cardHost [data-a=alias]').click(); }, CHAM);
    await page.waitForFunction(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); return r.spec && r.spec.designSku === 'CHAMELEON_28151'; }, A, { timeout: 30000 });
    const puts = st.calls.filter(c => c.name === 'charmNestLibrary' && c.body.op === 'aliasPut');
    ok(puts.length === putsBefore + 1 && puts[puts.length - 1].body.listingId === LID && puts[puts.length - 1].body.fromSku === RAW.toUpperCase() && puts[puts.length - 1].body.sku === CHAM && !puts[puts.length - 1].body.sandbox, 'ONE production aliasPut for the listing and the transaction SKU: ' + JSON.stringify(puts.map(c => c.body)));
    const rec = st.doc('Charm_Sku_Aliases', LID);
    ok(rec && rec.bySku && rec.bySku[RAW.toUpperCase()] === CHAM && rec.sku === undefined && rec.v === undefined, 'the store holds the listing answer under its SKU only: ' + JSON.stringify(rec && { bySku: rec.bySku, sku: rec.sku, v: rec.v }));
    ok(!st.doc('Charm_Sku_Aliases', OTHER), 'the other listing has no record');
    /* 4 · after the link: the chameleon, no question; the other listing is still held */
    const after = { A: await rowState(A), E: await rowState(E) };
    ok(after.A.designSku === CHAM && after.A.source === 'alias' && after.A.problems.length === 0, 'the necklace now reads the chameleon from the saved link, no question: ' + JSON.stringify(after.A));
    ok(JSON.stringify(after.E) === JSON.stringify(before.E), 'the other listing is unchanged, still held: ' + JSON.stringify(after.E));
    await context.close();
    ok(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`  ✓ D · the page: a shop-wide placeholder SKU is held with a plain card, Use this charm saves the listing's answer once, the line then reads the chameleon, another listing stays held (${n} checks)`);
})().catch(e => { console.error(e); process.exit(1); });
