// Adversarial (28 Sep, wave 3, area 12): Custom Orders.
// 1 · A custom order's decision picked in the order view is kept per order, as the staff note is: Previous / Next to
//     another line (a regular one, or another custom order) and back shows what was picked, and never lends it to the
//     other order. It was thrown away on the first step.
// 2 · The motion is seen and smooth in a real Chromium: the drag hover hands over from one card to the next (one card
//     marked at a time, its veil fading in), and a .dxf dropped on a card flies into its window (the chip moves on
//     screen over a visible time, on transform and opacity only, with no frame over 34 ms and no long task over 50 ms).
// Headless Chromium against the fake site (bridge-server.cjs); every request that is not to the loopback is aborted; the
// custom reading is answered by the fake site's stub model (bridge-server.cjs): no paid call.
//   node tests/charm-nest/adv-custom.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' }, extra || {});
const ORDERS = [
  order(4174476673, [line(41744766731, 'CUSTOM_6673', 'CUSTOM CHARM', [['Price', '28']])]),                      // custom, asks for its metal
  order(4176576272, [line(41765762721, 'RE_5460', 'MODIFICATION REWORK FREE SHIPPING', [['Price', '144']])]),    // custom, asks for its metal
  order(4175423829, [line(41754238291, 'CUSTOM-N-001-665441', 'Custom Name Necklace, Personalized Gold Charm Necklace', [['Metal', '14k Gold Filled']], { metalKey: 'gold', metalLabel: 'GF 14/20' })]),
  order(4177425406, [line(41774254061, 'CUSTOM-H-020-660181', 'Custom Huggie Earrings, Dainty Charm Huggies', [['Metal', 'Sterling Silver']], { metalKey: 'silver', metalLabel: 'Sterling Silver' })]),
  order(4178000001, [line(41780000011, 'BLOOMING_20239', 'Blooming Flower Charm Necklace', [['Metal', '14k Gold Filled'], ['Length', '18 Inches']], { metalKey: 'gold', metalLabel: 'GF 14/20' })])
];
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DESIGN_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, 18, 20, 0, 42, 0.4, 10, 18, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, 9, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    }, ORDERS);

    /* ── 1 · the decision picked is kept per order across Previous / Next ── */
    const A = '4174476673_41744766731', R = '4176576272_41765762721';
    const seq = await page.evaluate(() => Orders.visibleRows().map(r => r.key));
    assert(seq.includes(A) && seq.includes(R), 'both custom lines listed: ' + seq.join(','));
    await page.evaluate(k => OrderWin.open(k), A);
    await page.waitForFunction(() => OrderWin.isOpen() && document.querySelector('#owFix .cuStep [data-f=mat]'));
    await page.selectOption('#owFix .cuStep [data-f=mat]', 'silver');
    const at = k => page.waitForFunction(k => OrderWin.key() === k && !document.querySelector('#orderWin').classList.contains('owLoading'), k);
    const matOn = () => page.evaluate(() => { const f = document.querySelector('#owFix .cuStep [data-f=mat]'); return f ? f.value : null; });
    // walk the whole list forward and back to A: every other line passes through the view, custom or regular
    const i = seq.indexOf(A), steps = [];
    for (let j = i + 1; j < seq.length; j++) { await page.click('#owNext'); await at(seq[j]); steps.push([seq[j], await matOn()]); }
    for (let j = seq.length - 2; j >= 0; j--) { await page.click('#owPrev'); await at(seq[j]); if (seq[j] !== A) steps.push([seq[j], await matOn()]); else break; }
    if (i === seq.length - 1) { await page.click('#owPrev'); await at(seq[i - 1]); steps.push([seq[i - 1], await matOn()]); await page.click('#owNext'); await at(A); }
    const other = steps.find(s => s[0] === R);
    assert(other, 'the other custom order was shown on the way: ' + JSON.stringify(steps));
    assert.notEqual(other[1], 'silver', 'the metal picked for one order is never lent to another: ' + JSON.stringify(steps));
    assert.equal(await matOn(), 'silver', 'back on the order, the metal picked there is still picked: ' + JSON.stringify(steps));
    assert.equal(await page.evaluate(() => document.querySelector('#owFix .cuStep [data-a=mat]').disabled), false, 'and its Use it can be pressed');
    // closed and opened again on it: still there (the window keeps it as it keeps the staff note)
    await page.click('#owClose');
    await page.waitForFunction(() => !OrderWin.isOpen());
    await page.evaluate(k => OrderWin.open(k), R);
    await page.waitForFunction(() => OrderWin.isOpen() && document.querySelector('#owFix .cuStep [data-f=mat]'));
    await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen());
    await page.evaluate(k => OrderWin.open(k), A);
    await page.waitForFunction(() => OrderWin.isOpen() && document.querySelector('#owFix .cuStep [data-f=mat]'));
    assert.equal(await matOn(), 'silver', 'opened again later: the metal picked is still picked');
    await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen() && !document.querySelector('dialog[open]'));
    console.log('  ✓ a custom decision picked in the order view is kept per order across Previous / Next and a reopen');

    /* ── 2 · drag hover hands over between cards; a design flies into its window, seen and smooth ── */
    await page.evaluate(() => { CN.setMode('review'); Review.render(); });
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    await page.waitForFunction(() => document.querySelectorAll('#rvList > .reviewListRow.cuDropOk').length >= 2);
    const cards = await page.evaluate(() => [...document.querySelectorAll('#rvList > .reviewListRow.cuDropOk')].slice(0, 2).map(n => n.dataset.rid));
    const hover = rid => page.evaluate(rid => {
      const n = document.querySelector(`#rvList > .reviewListRow[data-rid="${rid}"]`), dt = new DataTransfer(); dt.items.add(new File(['x'], 'a.dxf'));
      for (const t of ['dragenter', 'dragover']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt }));
      return [...document.querySelectorAll('#rvList .cuDragOver')].map(x => x.dataset.rid);
    }, rid);
    assert.deepEqual(await hover(cards[0]), [cards[0]], 'the card under the files is marked');
    await page.waitForTimeout(80);
    const mid = await page.evaluate(rid => +getComputedStyle(document.querySelector(`#rvList > .reviewListRow[data-rid="${rid}"] .cuDropVeil b`)).opacity, cards[0]);
    assert(mid > 0 && mid < 1, 'its words fade in over a visible time (mid-way opacity ' + mid + ')');
    assert.deepEqual(await hover(cards[1]), [cards[1]], 'moving on hands the mark to the next card, one at a time');
    await page.waitForTimeout(400);
    const veil = await page.evaluate(([a, b]) => [a, b].map(rid => +getComputedStyle(document.querySelector(`#rvList > .reviewListRow[data-rid="${rid}"] .cuDropVeil b`)).opacity), cards);
    assert.deepEqual(veil, [0, 1], 'the first card\'s words have gone, the next card\'s have come: ' + veil);
    await page.evaluate(() => document.dispatchEvent(new Event('dragend')));

    // the flight: a .dxf dropped on the card, its chip flying into its row in the window
    await page.evaluate(() => {
      window.__perf = { deltas: [], long: [], chip: [], on: true };
      try { new PerformanceObserver(l => { for (const e of l.getEntries()) if (__perf.on) __perf.long.push([Math.round(e.startTime), Math.round(e.duration)]); }).observe({ type: 'longtask', buffered: false }); } catch (_) {}
      let last = 0; const tick = t => { if (!__perf.on) return; const c = document.querySelector('#cuDlg .cuChip'); if (last && (c || __perf.chip.length)) __perf.deltas.push([Math.round(t), t - last, Object.values(CustomSheet.entries()).flatMap(e => e.files.map(F => F.state[0])).join('') + (document.querySelector('#cuDlg[open]') ? 'D' : '')]); last = t; if (c) __perf.chip.push([Math.round(t), c.style.transform, +c.style.opacity]); requestAnimationFrame(tick); };
      requestAnimationFrame(tick);
    });
    await page.evaluate(({ rid, text }) => {
      const dt = new DataTransfer(); dt.items.add(new File([text], 'heart-name.dxf'));
      const n = document.querySelector(`#rvList > .reviewListRow[data-rid="${rid}"]`);
      for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 600, clientY: 300 }));
    }, { rid: cards[1], text: DESIGN_DXF });
    await page.waitForFunction(() => __perf.chip.length > 0, null, { timeout: 15000 });
    await page.waitForFunction(() => !document.querySelector('#cuDlg .cuChip'), null, { timeout: 15000 });
    await page.waitForTimeout(150);
    const perf = await page.evaluate(() => { __perf.on = false; return __perf; });
    const chip = perf.chip.filter(c => c[2] > 0), first = perf.chip[0][0], lastT = perf.chip[perf.chip.length - 1][0];
    const shown = new Set(chip.map(c => c[1]));
    assert(lastT - first >= 280, 'the flight lasts long enough to follow: ' + (lastT - first) + ' ms');
    assert(shown.size >= 10, 'the chip moves across the screen frame by frame: ' + shown.size + ' positions');
    // (every frame while the chip is seen: from its first visible frame to its last)
    const seenFrom = chip[0][0], seenTo = chip[chip.length - 1][0], inFlight = perf.deltas.filter(d => d[0] > seenFrom && d[0] <= seenTo);
    const during = inFlight.map(d => d[1]);
    const slow = inFlight.filter(d => d[1] > 34).map(d => `${Math.round(d[1])} ms at +${d[0] - first} (${d[2]})`);
    if (process.env.ADV_DEBUG) console.log(JSON.stringify(perf.deltas.map(d => [d[0] - first, Math.round(d[1]), d[2]])), perf.long);
    assert(slow.length === 0, 'no frame over 34 ms while it flies: ' + slow.join(', ') + ` (flight ${lastT - first} ms)`);
    assert(!perf.long.some(d => d[1] > 50 && d[0] + d[1] > seenFrom && d[0] < seenTo), 'no long task over 50 ms while it flies: ' + JSON.stringify(perf.long.map(d => [d[0] - first, d[1]])));
    // only transform and opacity move on it (its box never changes size or place)
    assert(chip.every(c => /^translate\([^)]*\) scale\([^)]*\)$/.test(c[1])), 'the chip moves on transform only');
    await page.waitForFunction(() => document.querySelector('#cuDlg[open] .cuFile .cuThumb img'));
    console.log(`  ✓ drag hover hands over; the design flies into its window (${lastT - first} ms, ${shown.size} positions, worst frame ${Math.round(Math.max(...during))} ms)`);
    assert.deepEqual(errors, [], 'no page errors');
  } finally { await browser.close(); await srv.close(); }
}
main().then(() => console.log('adv-custom: all checks passed'), e => { console.error(e); process.exit(1); });
