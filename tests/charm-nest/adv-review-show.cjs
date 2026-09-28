// Adversarial (wave 4, item 6): Review's "Show" finds any card, however old.
//  Show (a note after a card moved folder or chip) raised the list to 400 cards at most, so a card older than the newest
//  400 in its folder was never drawn and Show did nothing. It now draws the list down to the card it names (the cards
//  are all in the page already: no server read, no Etsy call). Checked under Open and under Completed, and Jump.
// No network but the loopback.
//   node tests/charm-nest/adv-review-show.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  let failed = 0;
  const check = (name, fn) => { try { fn(); console.log('  ✓ ' + name); } catch (e) { failed++; console.log('  ✗ ' + name + ': ' + String(e.message).split('\n')[0]); } };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(s => { try { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; }, { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' });
    const page = await context.newPage(); page.setDefaultTimeout(30000);
    const errors = []; page.on('pageerror', e => errors.push(e.message));
    const posts = []; page.on('request', q => { if (/etsy/i.test(q.url())) posts.push(q.method() + ' ' + q.url()); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const N = 450;
    // 450 orders, each with a SKU no master file holds (450 Unknown SKU cards, the oldest last), and 450 custom orders
    // completed earlier whose lines have left the pull (450 cards under Completed, the oldest last)
    const r = await page.evaluate(async N => {
      await Orders.loadMaps(true);
      const t0 = Date.now() - 3600e3;
      for (let i = 0; i < N; i++) {
        const rid = String(4400000000 + i), order = { receiptId: rid, orderNumber: rid, createTs: 1789000000 + i, updateTs: 1789000000 + i, shipBy: 1790000000, buyer: { name: 'Buyer ' + i }, buyerMessage: '', staffNote: '', messages: [],
          lines: [{ transactionId: rid + '1', listingId: String(1800000 + i), sku: 'NOPE_' + i, title: 'Nope ' + i, quantity: 1, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF', personalization: [], buyerMessage: '' }] };
        const line = order.lines[0], key = CharmNestOrders.lineKey(order, line);
        const row = { key, order, line, arrivedAt: t0 + i * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row);
      }
      // (seeded again before each Show: a maps reload from the fake server replaces B.maps)
      window.__seedDone = () => { B.maps.customDone = B.maps.customDone || {}; for (let i = 0; i < N; i++) B.maps.customDone['done-line-' + i] = { receiptId: String(4500000000 + i), completedAt: t0 + i * 1000, completedBy: 'Ana', how: 'button' }; };
      Orders.interpretAll(); if (Review.syncOrderItems) Review.syncOrderItems();
      const its = Review.items().filter(it => it.kind === 'unmatchedSku');
      const oldest = its.find(it => /NOPE_0$/.test(it.key));
      return { n: its.length, oldest: oldest ? oldest.key : '', oldestLine: oldest ? (oldest.rows || [oldest.row])[0].key : '' };
    }, N);
    assert(r.n >= N && r.oldest, `${N} Unknown SKU decisions: ` + JSON.stringify(r));
    await page.evaluate(() => { CN.setMode('review'); Review.view().cseg = 'done'; Review.render(); });
    await page.waitForFunction(() => document.querySelector('#reviewView [data-cseg="done"].on'));
    posts.length = 0;
    const drawn = () => [...document.querySelectorAll('#rvList .reviewListRow')].map(x => x.dataset.mkey);
    // Show, from Completed, for the oldest card under Open › Unknown SKU (as a note's Show does)
    const a = await page.evaluate(([k, drawnSrc]) => { Review.showCard(k, 'open', 'unmatchedSku'); const n = eval(drawnSrc)(); return { drawn: n.length, found: n.includes(k), open: !!document.querySelector('#reviewView [data-cseg="open"].on') }; }, [r.oldest, drawn.toString()]);
    check(`Show finds the oldest of ${N} cards under Open (${a.drawn} drawn)`, () => { assert(a.open, 'Open'); assert(a.found, `not drawn (${a.drawn} cards drawn)`); });
    // back to the first 40 under Open, then Show for the oldest card under Completed
    await page.evaluate(() => { const v = Review.view(); v.cseg = 'done'; v.filter = null; Review.render(); v.cseg = 'open'; Review.render(); });
    const b = await page.evaluate(drawnSrc => { const k = 'cu:rec:done-line-0'; __seedDone(); Review.showCard(k, 'done'); const n = eval(drawnSrc)(); return { drawn: n.length, found: n.includes(k), done: !!document.querySelector('#reviewView [data-cseg="done"].on') }; }, drawn.toString());
    check(`Show finds the oldest of ${N} completed cards (${b.drawn} drawn)`, () => { assert(b.done, 'Completed'); assert(b.found, `not drawn (${b.drawn} cards drawn)`); });
    // a newer card is drawn without drawing the whole folder
    await page.evaluate(() => { const v = Review.view(); v.cseg = 'open'; Review.render(); });
    const c = await page.evaluate(drawnSrc => { const k = 'cu:rec:done-line-' + 440; __seedDone(); Review.showCard(k, 'done'); const n = eval(drawnSrc)(); return { drawn: n.length, found: n.includes(k) }; }, drawn.toString());
    check(`Show for a newer card draws only down to it (${c.drawn} drawn)`, () => { assert(c.found, 'found'); assert(c.drawn <= 80, 'drew ' + c.drawn); });
    // Jump (a held order's card): the line's item, however old
    const d = await page.evaluate(([line, k, drawnSrc]) => { Review.view().cseg = 'done'; Review.render(); Review.focus(line); const n = eval(drawnSrc)(); return { found: n.includes(k), drawn: n.length }; }, [r.oldestLine, r.oldest, drawn.toString()]);
    check(`Jump finds the oldest card (${d.drawn} drawn)`, () => assert(d.found));
    check('no Etsy call from Show or Jump', () => assert.deepEqual(posts, []));
    check('no page errors', () => assert.deepEqual(errors, []));
  } finally { await browser.close(); await srv.close(); }
  console.log(failed ? `adv-review-show: ${failed} failed` : 'adv-review-show: all passed'); process.exit(failed ? 1 : 0);
})().catch(e => { console.log('  ✗ ' + String(e.message).split('\n')[0]); process.exit(1); });
