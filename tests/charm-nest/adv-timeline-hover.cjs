// The step explainer card kept in reach (Paul, 28 Sep 21:18: "the hover must persist"), in the real sorter page with
// the fake site (bridge-server.cjs), every other request aborted:
//  1 · the order view's "Where it is now" card (explainOn, inside a modal dialog as in the order view): the card stays
//      while the pointer is on the seal or on the card and goes shortly after it leaves both; a click pins it (it stays
//      with the pointer gone, and offers the Timeline), Esc or a click elsewhere lets it go and the view stays open;
//  2 · a very short screen: a rail at the top flips the zoomed seal under its dot and the card goes beside them, clear of
//      the seal and inside the view.
//   node tests/charm-nest/adv-timeline-hover.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await ctx.route(() => true, r => { const h = new URL(r.request().url()).hostname; return h === '127.0.0.1' || h === 'localhost' ? r.continue() : r.abort(); });
    await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.OrderTimelineUI && window.OrderTimeline && document.readyState === 'complete', null, { timeout: 60000 });

    // a necklace: in, nested, cut and sorted; Assembled next
    const N = '4200000011', H = 36e5, T0 = Date.now() - 4 * 24 * H;
    const ev = (type, h, x) => Object.assign({ id: `${N}~${type}~${h}`, orderId: N, type, at: T0 + h * H, by: 'Paul', source: 'sorter' }, x || {});
    const evs = Timeline.chronology([ev('arrived', 0, { source: 'etsy', by: 'Etsy' }), ev('placed', 5, { sheetId: 'shA', sheet: 'GF Sheet 1' }), ev('laserDone', 30, { sheetId: 'shA', sheet: 'GF Sheet 1' }), ev('sorted', 51, { station: 'sorting', by: 'Ana' })]).sort(Timeline.byTime);
    await page.evaluate(({ N, evs, where }) => {
      window.__fx = { [N]: { id: N, events: evs, cancelled: null, where } };
      const wait = window.setTimeout.bind(window);
      OrderTimeline.get = async id => { await new Promise(r => wait(r, 20)); return JSON.parse(JSON.stringify(window.__fx[id])); };
    }, { N, evs, where: Timeline.whereOf(evs, null, { record: true }) });

    // ── 1 · the Overview's card, in a modal dialog like the order view ──
    await page.evaluate(N => {
      const d = document.createElement('dialog'); d.className = 'tlTestDlg'; d.style.cssText = 'width:900px;height:600px;padding:40px;border:0';
      d.addEventListener('cancel', () => { window.__closed = true; });
      const host = document.createElement('div'); host.className = 'tlTestHost'; host.style.cssText = 'display:flex;gap:18px;align-items:flex-start;padding:16px;width:420px';
      const a = OrderTimelineUI.nowStamps(window.__fx[N].events, {});
      host.innerHTML = a.seal + '<div><div class="k">Sorted</div><div class="t">Waiting on assembly</div></div>';
      d.appendChild(host); document.body.appendChild(d); d.showModal();
      // (as the order view: the order's own steps, a necklace's with no Welded or Engraved)
      const stages = OrderTimelineUI.stagesFor({ title: 'Initial necklace', variations: [{ name: 'Style', value: 'Necklace' }], engrave: { state: 'none' } });
      OrderTimelineUI.explainOn(host, () => ({ events: window.__fx[N].events, stages }), st => { window.__pinned = st; });
    }, N);
    const shown = () => page.evaluate(() => { const X = document.querySelector('.tlTestDlg .tlExp'); return !!X && getComputedStyle(X).display === 'block' && +getComputedStyle(X).opacity > .9; });
    const seal = await page.locator('.tlTestHost .tlNowSeal').boundingBox();
    const away = { x: 1400, y: 880 };
    await page.mouse.move(away.x, away.y);
    await page.mouse.move(seal.x + seal.width / 2, seal.y + seal.height / 2, { steps: 4 }); await page.waitForTimeout(300);
    assert(await shown(), 'hovering the seal shows its card');
    // across the gap and onto the card
    const cb = await page.evaluate(() => { const r = document.querySelector('.tlTestDlg .tlExp').getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + 24, top: r.top, left: r.left }; });
    await page.mouse.move(cb.x, cb.y, { steps: 8 }); await page.waitForTimeout(450);
    assert(await shown(), 'the card stays while the pointer is on it');
    // back to the seal, then away from both
    await page.mouse.move(seal.x + seal.width / 2, seal.y + seal.height / 2, { steps: 6 }); await page.waitForTimeout(400);
    assert(await shown(), 'and while it is back on the seal');
    await page.mouse.move(away.x, away.y, { steps: 4 }); await page.waitForTimeout(500);
    assert(!(await shown()), 'it goes shortly after the pointer leaves both');
    // a click pins it: it stays with the pointer gone, and offers the Timeline
    await page.mouse.move(seal.x + seal.width / 2, seal.y + seal.height / 2, { steps: 4 }); await page.waitForTimeout(300);
    await page.mouse.click(seal.x + seal.width / 2, seal.y + seal.height / 2); await page.waitForTimeout(100);
    await page.mouse.move(away.x, away.y, { steps: 4 }); await page.waitForTimeout(500);
    assert(await shown(), 'a click pins the card: it stays with the pointer gone');
    assert.equal(await page.evaluate(() => window.__pinned), undefined, 'a click pins; it does not leave the Overview');
    assert.equal(await page.evaluate(() => !!document.querySelector('.tlTestDlg .tlExp [data-tl-open]')), true, 'the pinned card offers the Timeline');
    await page.keyboard.press('Escape'); await page.waitForTimeout(400);
    assert(!(await shown()), 'Esc lets it go');
    assert.equal(await page.evaluate(() => document.querySelector('.tlTestDlg').open && !window.__closed), true, 'and the view stays open');
    // pinned again, a click elsewhere lets it go
    await page.mouse.move(seal.x + seal.width / 2, seal.y + seal.height / 2, { steps: 4 }); await page.waitForTimeout(300);
    await page.mouse.click(seal.x + seal.width / 2, seal.y + seal.height / 2); await page.waitForTimeout(100);
    await page.mouse.move(away.x, away.y, { steps: 4 }); await page.waitForTimeout(300);
    assert(await shown(), 'pinned again');
    await page.mouse.click(away.x, away.y); await page.waitForTimeout(400);
    assert(!(await shown()), 'a click elsewhere lets it go');
    // the pinned card's link opens the step on the Timeline
    await page.mouse.move(seal.x + seal.width / 2, seal.y + seal.height / 2, { steps: 4 }); await page.waitForTimeout(300);
    await page.mouse.click(seal.x + seal.width / 2, seal.y + seal.height / 2); await page.waitForTimeout(250);
    await page.click('.tlTestDlg .tlExp [data-tl-open]'); await page.waitForTimeout(300);
    assert.equal(await page.evaluate(() => window.__pinned && window.__pinned.stage), 'assembled', 'the link asks the host for the step on the Timeline');
    assert(!(await shown()), 'and the card goes');
    await page.evaluate(() => { const d = document.querySelector('.tlTestDlg'); d.close(); d.remove(); });
    console.log('  ✓ 1 · the Overview card: the explainer stays on the seal and on the card, goes 160 ms after both; a click pins it, Esc (the view stays) or a click elsewhere lets it go, its link opens the Timeline');

    // ── 2 · a very short screen: the card beside a flipped seal, clear of it and in the view ──
    await page.setViewportSize({ width: 1440, height: 280 });
    await page.evaluate(N => {
      const h = document.createElement('div'); h.className = 'tlTestHost2'; h.style.cssText = 'position:fixed;inset:0;z-index:2147480000;background:var(--card);display:flex;flex-direction:column';
      window.__el = document.createElement('div'); window.__el.style.cssText = 'flex:1 1 auto;min-height:0;display:flex;flex-direction:column;height:100%'; h.appendChild(window.__el); document.body.appendChild(h);
      window.__tl = OrderTimelineUI.mount(window.__el, { orderId: N, live: true, pollMs: 600000 });
    }, N);
    await page.waitForFunction(() => window.__el.querySelector('.tlStops[data-keys]') && !window.__el.querySelector('.tlMsg:not([hidden])'), null, { timeout: 8000 });
    await page.waitForTimeout(900);
    const out = [];
    for (const k of ['arrived', 'sheet', 'laser', 'sorted']) {
      await page.mouse.move(700, 275); await page.waitForTimeout(250);
      await page.locator(`.tlStop[data-stage="${k}"]`).hover(); await page.waitForTimeout(380);
      out.push(await page.evaluate(k => {
        const el = window.__el, X = el.querySelector('.tlExp'), L = el.querySelector('.tlLoupe'), e = X.getBoundingClientRect(), l = L.getBoundingClientRect(), d = el.querySelector(`.tlStop[data-stage="${k}"] .tlSeal`).getBoundingClientRect();
        return { k, card: getComputedStyle(X).display === 'block', loupe: getComputedStyle(L).display === 'block', flipped: l.top >= d.bottom - 1,
          apart: l.bottom <= e.top + 1 || l.top >= e.bottom - 1 || l.right <= e.left + 1 || l.left >= e.right - 1, dot: d.bottom <= e.top + 1 || d.top >= e.bottom - 1 || d.right <= e.left + 1 || d.left >= e.right - 1,
          inView: e.left >= 0 && e.right <= innerWidth && e.top >= 0 && e.bottom <= innerHeight };
      }, k));
    }
    const bad = out.filter(r => !r.card || !r.loupe || !r.flipped || !r.apart || !r.dot || !r.inView);
    assert.deepEqual(bad, [], 'each card clear of its flipped seal and its dot, inside the view: ' + JSON.stringify(out));
    await page.evaluate(() => { window.__tl.destroy(); document.querySelectorAll('.tlTestHost2').forEach(x => x.remove()); });
    console.log(`  ✓ 2 · a 280 px tall screen: ${out.length} done steps, each seal flipped under its dot and its card beside them, clear of both and inside the view`);

    assert.deepEqual(errors, [], 'no page errors');
    console.log('adv-timeline-hover OK');
  } finally { await browser.close(); if (srv.close) await srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
