// Adversarial checks (wave 3, area 7) of the order search: "/" and Ctrl/Cmd+K, fast typing, many results, a result
// opening the order view, Esc, the animations' visibility and smoothness, and where the focus goes back to.
//  1 · a click inside the box (its footer, the count, the space round the cards) took the focus out of the field:
//      Esc then did nothing, nor did the arrows, Enter or typing
//  2 · the list refreshed while open (a new order in the pull, the Cancelled list read) kept the selection's place,
//      not its order: Enter then opened a different order from the one chosen
//  3 · Ctrl+K or "/" pressed while the box was still fading out after Esc was swallowed: the box closed anyway
//  4 · a card still gliding to its place (FLIP) jumped when the next key moved it again (fast typing)
//  5 · the animations are seen (mid-way differs from start and end) and smooth (rAF gaps, long tasks), and the order
//      view opened from a result grows out of the card; closing it gives the focus back where it was
// Headless Chromium against the local fake site (bridge-server.cjs); every request off the loopback is aborted. No Etsy call.
//   node tests/charm-nest/adv-order-search.cjs [playwright-core dir]      (SOFT=1: run every step, report all failures)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.now() / 1000) + 3 * DAY;
const order = (rid, buyer, sku, ago) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - ago * DAY, updateTs: SHIP - ago * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: String(rid) + '1', listingId: '1800000001', sku, title: sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
// newest first: A B C D; "420" leaves A and C (C moves up one place), "4200000002" leaves C (it moves up again)
const A = '4200000001', Bo = '4211111111', C = '4200000002', D = '4222222222', E = '4299999999';
const ORDERS = [order(A, 'Amy Arden', 'SKU_A', 2), order(Bo, 'Bea Brook', 'SKU_B', 3), order(C, 'Cal Crane', 'SKU_C', 4), order(D, 'Dot Drake', 'SKU_D', 5)];

async function main() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await ctx.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await ctx.newPage(), errors = [];
    page.setDefaultTimeout(15000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderSearch && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      window.__addOrder = order => { for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: order.createTs * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } };
      for (const o of orders) window.__addOrder(o);
      Orders.interpretAll();
    }, ORDERS);
    const etsyCalls = () => srv.st.calls.filter(c => /etsy|listOpenOrders|Receipt/i.test(c.name)).length, etsy0 = etsyCalls();
    const shown = () => page.evaluate(() => [...document.querySelectorAll('#cnsList .cnsCard')].map(n => n.dataset.rid));
    const selRid = () => page.evaluate(() => { const n = document.querySelector('#cnsList .cnsCard.sel'); return n && n.dataset.rid; });
    const closed = () => page.waitForFunction(() => document.getElementById('cnSearch').hidden, null, { timeout: 3000 });
    const reset = () => page.evaluate(() => { OrderSearch.close(true); for (const d of document.querySelectorAll('dialog[open]')) d.close(); document.querySelectorAll('.cnsLift').forEach(n => n.remove()); if (document.activeElement) document.activeElement.blur(); });

    let bad = 0; const step = async (name, fn) => { if (process.env.STEPS && !process.env.STEPS.split(',').includes(name)) return; try { await fn(); } catch (e) { if (!process.env.SOFT) throw e; bad++; console.log('  ✗ ' + name + ': ' + String(e.message).split('\n')[0]); } await reset().catch(() => {}); };

    await step('1', async () => {
      /* 1 · a click inside the box keeps the field's keys: Esc closes, arrows and typing still work */
      await page.keyboard.press('/'); await page.waitForFunction(() => OrderSearch.isOpen() && document.activeElement.id === 'cnsQ');
      await page.keyboard.type('42');
      await page.click('.cnsFoot');
      const act = await page.evaluate(() => document.activeElement && (document.activeElement.id || document.activeElement.tagName));
      await page.keyboard.press('ArrowDown');
      const sel = await selRid();
      await page.keyboard.press('Escape'); await page.keyboard.press('Escape');
      await page.waitForTimeout(400);
      const st = await page.evaluate(() => ({ open: OrderSearch.isOpen(), hidden: document.getElementById('cnSearch').hidden }));
      assert.deepEqual(st, { open: false, hidden: true }, 'Esc closes the search after a click in its footer (focus was on ' + act + ')');
      assert.equal(sel, Bo, 'and the arrows still walked the list: ' + sel);
      console.log('  ✓ 1 a click inside the box: Esc, the arrows and typing stay with the field');
    });

    await step('2', async () => {
      /* 2 · the list refreshed under the selection: the chosen order stays chosen */
      await page.keyboard.press('/'); await page.waitForFunction(() => OrderSearch.isOpen());
      await page.keyboard.type('42');
      assert.deepEqual(await shown(), [A, Bo, C, D], 'newest first');
      await page.keyboard.press('ArrowDown'); await page.keyboard.press('ArrowDown');
      assert.equal(await selRid(), C, 'C chosen');
      await page.evaluate(o => { window.__addOrder(o); Orders.interpretAll(); }, order(E, 'Eve Ember', 'SKU_E', 1));
      await page.waitForFunction(e => [...document.querySelectorAll('#cnsList .cnsCard')].some(n => n.dataset.rid === e), E, { timeout: 4000 });
      assert.equal((await shown())[0], E, 'the new order shows at the top');
      const after = await selRid();
      await page.evaluate(() => { window.__opened = []; window.__real = OrderWin.openOrder; OrderWin.openOrder = (rid) => { window.__opened.push(rid); return Promise.resolve(); }; });
      await page.keyboard.press('Enter');
      await page.waitForFunction(() => window.__opened.length === 1, null, { timeout: 3000 }).catch(() => {});
      const opened = await page.evaluate(() => { OrderWin.openOrder = window.__real; return window.__opened[0]; });
      assert.equal(after, C, 'the selection stayed on the order chosen: ' + after);
      assert.equal(opened, C, 'and Enter opened it: ' + opened);
      console.log('  ✓ 2 a refresh while open keeps the chosen order chosen; Enter opens that one');
    });

    await step('3', async () => {
      /* 3 · Ctrl+K and "/" pressed while the box fades out after Esc open it again */
      for (const key of ['Control+k', '/']) {
        await page.keyboard.press('/'); await page.waitForFunction(() => OrderSearch.isOpen() && document.activeElement.id === 'cnsQ');
        await page.waitForTimeout(300);
        await page.keyboard.press('Escape');
        await page.keyboard.press(key);
        await page.waitForTimeout(450);
        const st = await page.evaluate(() => { const r = document.getElementById('cnSearch'); return { hidden: r.hidden, op: getComputedStyle(r).opacity, focus: document.activeElement && document.activeElement.id }; });
        assert.deepEqual(st, { hidden: false, op: '1', focus: 'cnsQ' }, key + ' right after Esc opens the search again: ' + JSON.stringify(st));
        await reset();
      }
      console.log('  ✓ 3 Ctrl+K or "/" during the closing fade opens it again');
    });

    await step('4', async () => {
      /* 4 · fast typing: a card still gliding is moved again from where it is seen, never jumping */
      await page.keyboard.press('/'); await page.waitForFunction(() => OrderSearch.isOpen());
      await page.keyboard.type('42'); await page.waitForTimeout(500);
      const jumps = await page.evaluate(async ({ C }) => {
        const q = document.getElementById('cnsQ'), card = () => document.querySelector(`#cnsList .cnsCard[data-rid="${C}"]`);
        const set = v => { q.value = v; q.dispatchEvent(new Event('input')); };
        const frame = () => new Promise(r => requestAnimationFrame(() => r()));
        set('420'); await frame(); await frame();
        await new Promise(r => setTimeout(r, 90));                                       // mid-glide
        const before = card().getBoundingClientRect().top;
        set('4200000002');
        const after = card().getBoundingClientRect().top;
        await frame(); const next = card().getBoundingClientRect().top;
        return { before, after, next };
      }, { C });
      assert(Math.abs(jumps.after - jumps.before) < 2 && Math.abs(jumps.next - jumps.before) < 12, 'the gliding card carries on from where it is seen: ' + JSON.stringify(jumps));
      console.log('  ✓ 4 fast typing: a gliding card carries on from where it is, no jump ' + JSON.stringify(jumps));
    });

    await step('5', async () => {
      /* 5 · seen and smooth: the box opening, a keystroke's cards, and the order view growing out of a result */
      await page.evaluate(() => { document.getElementById('cnsFind').focus(); });
      const rec = () => page.evaluate(() => { window.__gaps = []; window.__long = []; let last = performance.now(), on = true; const f = t => { window.__gaps.push(t - last); last = t; if (on) requestAnimationFrame(f); }; requestAnimationFrame(f); window.__stop = () => { on = false; }; try { window.__po = new PerformanceObserver(l => { for (const e of l.getEntries()) window.__long.push(Math.round(e.duration)); }); window.__po.observe({ entryTypes: ['longtask'] }); } catch (_) {} });
      const stop = () => page.evaluate(() => { window.__stop(); try { window.__po.disconnect(); } catch (_) {} const g = window.__gaps.slice(1); return { frames: g.length, worst: Math.round(Math.max(0, ...g)), over34: g.filter(x => x > 34).map(Math.round), long: window.__long }; });
      // the box opening: mid-way differs from start and end
      await rec();
      // (each frame's opacity of the box, from the click until it has landed)
      const seen = await page.evaluate(async () => { document.getElementById('cnsFind').click(); const b = document.querySelector('.cnsBox'), t0 = performance.now(), out = []; while (performance.now() - t0 < 420) { await new Promise(r => requestAnimationFrame(r)); out.push([Math.round(performance.now() - t0), +(+getComputedStyle(b).opacity).toFixed(2), getComputedStyle(b).transform !== 'none']); } return out; });
      const openS = await stop();
      const midF = seen.filter(([, o, tf]) => o > 0.05 && o < 0.95 && tf), last = seen[seen.length - 1];
      assert(midF.length >= 3 && last[1] === 1 && !last[2], 'the box is seen growing in over several frames: ' + JSON.stringify(seen));
      // typing: the cards' pass and glide
      await rec();
      for (const ch of '4200000') { await page.keyboard.type(ch); await page.waitForTimeout(45); }
      await page.waitForTimeout(400);
      const typeS = await stop();
      // Enter: the order view grows out of the card (its clip mid-way is neither the card nor the screen)
      await page.keyboard.type('002');
      const card = await page.evaluate(() => { const r = document.querySelector('#cnsList .cnsCard').getBoundingClientRect(); return { top: r.top, h: r.height }; });
      await rec();
      const cdp = process.env.PROFILE ? await page.context().newCDPSession(page) : null;
      if (cdp) { await cdp.send('Profiler.enable'); await cdp.send('Profiler.setSamplingInterval', { interval: 200 }); await cdp.send('Profiler.start'); }
      await page.keyboard.press('Enter');
      await page.waitForFunction(() => OrderWin.isOpen(), null, { timeout: 4000 });
      const clips = await page.evaluate(async () => { const d = document.getElementById('orderWin'), out = []; for (let i = 0; i < 4; i++) { out.push(getComputedStyle(d).clipPath); await new Promise(r => setTimeout(r, 120)); } await new Promise(r => setTimeout(r, 400)); out.push(getComputedStyle(d).clipPath); return out; });
      const growS = await stop();
      if (cdp) {
        const { profile } = await cdp.send('Profiler.stop'), self = new Map(), byId = new Map(profile.nodes.map(n => [n.id, n])), dt = profile.timeDeltas; const cnt = new Map();
        profile.samples.forEach((id, i) => cnt.set(id, (cnt.get(id) || 0) + (dt[i] || 0)));
        for (const [id, us] of cnt) { const n = byId.get(id), f = n.callFrame, k = `${f.functionName || '(anon)'} ${f.url.split('/').pop()}:${f.lineNumber + 1}`; self.set(k, (self.get(k) || 0) + us); }
        // inclusive time per function
        const parent = new Map(); for (const n of profile.nodes) for (const c of n.children || []) parent.set(c, n.id);
        const incl = new Map(); for (const [id, us] of cnt) { const seen = new Set(); for (let x = id; x != null; x = parent.get(x)) { const f = byId.get(x).callFrame, k = `${f.functionName || '(anon)'} ${f.url.split('/').pop()}:${f.lineNumber + 1}`; if (seen.has(k)) continue; seen.add(k); incl.set(k, (incl.get(k) || 0) + us); } }
        console.log('    self:', [...self].sort((a, b) => b[1] - a[1]).slice(0, 14).map(([k, v]) => `${k} ${(v / 1000).toFixed(1)}ms`).join(' | '));
        console.log('    incl:', [...incl].sort((a, b) => b[1] - a[1]).filter(([k]) => /\.js|\.html/.test(k)).slice(0, 22).map(([k, v]) => `${k} ${(v / 1000).toFixed(1)}ms`).join(' | '));
      }
      assert(clips.some(c => /inset/.test(c) && !/inset\(0px/.test(c)) && new Set(clips).size >= 3, 'the order view is seen growing out of the card: ' + JSON.stringify(clips));
      assert.equal(await page.evaluate(() => OrderSearch.isOpen()), false, 'the search gave way to the view');
      assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'one window');
      console.log('    frames · open ' + JSON.stringify(openS) + ' · typing ' + JSON.stringify(typeS) + ' · order view ' + JSON.stringify(growS));
      // closing the view: the focus goes back where it was before the search opened
      await page.evaluate(() => OrderWin.close());
      await page.waitForFunction(() => !document.querySelector('dialog[open]'), null, { timeout: 3000 });
      await page.waitForTimeout(300);
      const back = await page.evaluate(() => document.activeElement && (document.activeElement.id || document.activeElement.tagName));
      assert.equal(back, 'cnsFind', 'the focus is back on the search button');
      assert.equal(await page.evaluate(() => document.querySelectorAll('.cnsLift').length), 0, 'no lifted card is left');
      // and Esc: back where it was
      await page.evaluate(() => document.getElementById('cnsFind').click());
      await page.waitForFunction(() => OrderSearch.isOpen() && document.activeElement.id === 'cnsQ');
      await page.keyboard.press('Escape'); await closed();
      assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.id), 'cnsFind', 'Esc gives the focus back');
      const worst = Math.max(openS.worst, typeS.worst, growS.worst);
      if (worst > 34 || [...openS.long, ...typeS.long, ...growS.long].some(x => x > 50)) console.log('    ! a frame gap over 34 ms or a long task over 50 ms (see above)');
      console.log('  ✓ 5 open, typing and the order view are seen moving; focus returns to where it was');
    });

    assert.equal(etsyCalls(), etsy0, 'no Etsy call');
    assert.deepEqual(errors, [], 'no page errors');
    assert.equal(bad, 0, bad + ' step(s) failed');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('adv-order-search: ok'), e => { console.error(e); process.exit(1); });
