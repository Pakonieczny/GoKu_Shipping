// The "N of M" count on the order window's milestone rail is drawn on the NEXT UNFINISHED milestone only (Paul, 5 Oct 2026:
// "This timeline should not show '4 of 6' on every single milestone ... only show it above the next unfinished milestone and do not
// show it on the rest of the milestones that are unfinished"). The sorter page over the local stand-in (bridge-server.cjs); no network
// but the loopback, nothing real is written. A 6-piece order (every piece its own line) whose timeline is written into the fake
// store, the open order window reading it back every 0.3 s (OrderTimelineUI.pollOpenMs, tests only).
//   1 · the order of the screenshot (4 of 6 pieces shipped, 2 only nested): ONE pill, "4 of 6" on Laser cut, none on Sorted,
//       Assembled or Shipped, none on the two done milestones
//   2 · 2 pieces ahead: "2 of 6" on Laser cut; a partly cut order says the real count ("5 of 6") and a later step some pieces
//       reached (Sorted) still says nothing
//   3 · a piece completing a step moves the pill on: all six cut, two sorted: the pill is on Sorted alone, Laser cut is plain done
//   4 · nobody has reached the next step: no pill; every piece shipped: no pill and every milestone done
//   5 · the header rail and the Timeline tab's rail (the one component) follow the same rule; the pill carries the same look and the
//       milestones keep their aria-label with how many pieces are at each step (the hover/explainer info)
//   6 · only one renderer of the pill exists (no other list, card, panel or page draws its own "N of M" milestone count)
//   7 · a mutant that shows the count on every unfinished milestone (or on none) is caught
//   node tests/charm-nest/rail-next-count-only.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOTS=dir keeps screenshots)
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 9, 17) / 1000), NOW = Date.now();
const RID = '4171711853', N = 6;
const TIDS = Array.from({ length: N }, (_, i) => `4171711853${i + 1}`);
const SKUS = ['SPORTS_10_FIGURE_SKATE', 'SNAKE_5', 'FOOTBALL', 'HEART_2', 'LEAF_CHARM', 'MOON_3'];
const KEYS = TIDS.map(t => `${RID}_${t}`);
// the rail of this order: no back engraving and no stud, so six steps as in the screenshot
const RAIL = ['arrived', 'sheet', 'laser', 'sorted', 'assembled', 'shipped'];
// a piece's level: 1 nested, 2 laser cut, 3 sorted, 4 assembled, 5 shipped
const TYPE_AT = [null, 'placed', 'laserDone', 'sorted', 'assembled', 'shipped'];
const order = () => ({ receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Carolyn Schmidt' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: TIDS.map((t, i) => ({ transactionId: t, listingId: '18000' + t.slice(-4), sku: SKUS[i], title: SKUS[i].replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' })) });
const sleep = ms => new Promise(r => setTimeout(r, ms));

/** What the rail must say for these piece levels: how many pieces reached each of its steps, and the one pill (or none). */
function expectFor(levels) {
  const reached = RAIL.map((_, j) => j === 0 ? N : levels.filter(l => l >= j).length);   // (Order in is every piece's)
  const next = reached.findIndex(n => n < N);
  const pills = {};
  if (next >= 0 && reached[next] > 0) pills[RAIL[next]] = `${reached[next]} of ${N}`;
  return { reached, next, pills };
}

const mutant = { every: s => { const t = s.replace('j === next && sr && sr.n > 0', 'sr && sr.n > 0'); assert.notEqual(t, s, 'the mutant found the line it changes'); return t; },
  none: s => { const t = s.replace('j === next && sr && sr.n > 0', 'false && sr && sr.n > 0'); assert.notEqual(t, s, 'the mutant found the line it changes'); return t; } };

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.CN_SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  // the timeline of the order, written as the stations and the sorter would: the order arrived, each piece is on a sheet (level 1) and
  // goes on from there
  const setLevels = levels => {
    for (const k of [...srv.st.docs.keys()]) if (k.startsWith('Order_Timeline/' + RID)) srv.st.docs.delete(k);
    const put = (type, at, id, extra) => srv.st.put('Order_Timeline', `${RID}~${type}~${id}`, Object.assign({ orderId: RID, type, at, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra));
    put('arrived', NOW - 3 * DAY * 1000, 'arrived', { source: 'etsy', by: 'Etsy' });
    levels.forEach((lv, i) => { for (let s = 1; s <= lv; s++) put(TYPE_AT[s], NOW - 2 * DAY * 1000 + (s * 10 + i) * 60e3, `${KEYS[i]}-${s}`, { lineKey: KEYS[i], transactionId: TIDS[i], sheetId: 'sh-a', sheet: 'GF Sheet 1', station: ['', '', '', 'sorting', 'assembly', 'shipping'][s] }); });
  };
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const results = [];
  const check = async (name, fn) => { try { await fn(); results.push(['ok', name]); console.log('  ✓ ' + name); } catch (e) { results.push(['FAIL', name]); console.log('  ✗ ' + name + '\n      ' + String(e.message).split('\n').slice(0, 6).join('\n      ')); } };

  /** A fresh sorter page with the 6-piece order open on the Overview (viewport w×h); `mutate` rewrites charm-nest-timeline-ui.js. */
  async function openPage(width, height, mutate) {
    const context = await browser.newContext({ viewport: { width, height } }), errors = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    if (mutate) await context.route(u => /\/charm-nest-timeline-ui\.js/.test(u.pathname), r => r.fulfill({ status: 200, contentType: 'text/javascript', body: mutate(fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8')) }));
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} });
    const page = await context.newPage();
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderTimelineUI && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async o => {
      OrderTimelineUI.pollOpenMs = 300;   // (tests only: the open view reads every 0.3 s)
      await Orders.loadMaps(true);
      for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, order());
    await page.evaluate(k => OrderWin.open(k), KEYS[0]);
    await page.waitForFunction(() => { const r = document.querySelector('#owRail .tlUI'); return r && r.querySelector('.tlMsg').hidden && r.querySelectorAll('.tlStop').length; });
    return { page, context, errors };
  }
  /** The rail of a host ("#owRail" or "#owTimeline"): each milestone, its state, its pill (null when not shown) and what its aria-label says. */
  const rail = (page, host) => page.evaluate(h => [...document.querySelectorAll(h + ' .tlRail .tlStop')].map(n => { const c = n.querySelector('.tlCnt'); return { k: n.dataset.stage, cls: n.className.replace(/^tlStop\s*/, ''), pill: c && !c.hidden && getComputedStyle(c).display !== 'none' ? c.textContent : null, aria: n.getAttribute('aria-label') || '' }; }), host);
  const piecesAt = a => { const m = /· (\d+) of (\d+) pieces?$/.exec(a); return m ? +m[1] : null; };
  /** Writes the levels, waits until the rail has read them (every milestone says how many pieces are at it), then returns the rail. */
  async function drawn(page, host, levels) {
    setLevels(levels); const want = expectFor(levels).reached;
    const t0 = Date.now(); let r;
    for (;;) { r = await rail(page, host); if (r.length === RAIL.length && r.every((n, j) => piecesAt(n.aria) === want[j])) break; if (Date.now() - t0 > 12000) throw new Error(`the rail never read the new levels ${JSON.stringify(levels)}: ${JSON.stringify(r.map(n => [n.k, piecesAt(n.aria)]))}`); await sleep(120); }
    await sleep(350);   // (one more read, and the redraw settled)
    return rail(page, host);
  }
  /** The rule, for these levels: exactly the pills expectFor says, on the next unfinished milestone, and it is the current one. */
  const assertRule = (r, levels, tag) => {
    const e = expectFor(levels), shown = Object.fromEntries(r.filter(n => n.pill).map(n => [n.k, n.pill]));
    assert.deepEqual(shown, e.pills, `${tag}: pills ${JSON.stringify(shown)} (levels ${JSON.stringify(levels)}), expected ${JSON.stringify(e.pills)}`);
    r.forEach((n, j) => { if (e.next >= 0 && j < e.next) assert(/\bd\b/.test(n.cls), `${tag}: ${n.k} is done (${n.cls})`); });
    for (const n of r) if (/\bd\b/.test(n.cls)) assert.equal(n.pill, null, `${tag}: a done milestone shows no pill (${n.k})`);
    if (e.next >= 0 && e.pills[RAIL[e.next]]) assert(/\bc\b/.test(r[e.next].cls), `${tag}: the pill is on the current milestone (${r[e.next].cls})`);
    if (e.next < 0) assert(r.every(n => /\bd\b/.test(n.cls)), `${tag}: every milestone is done`);
  };
  const SCENES = [
    [[5, 5, 5, 5, 1, 1], 'the order of the screenshot: 4 pieces shipped, 2 only nested'],
    [[5, 5, 1, 1, 1, 1], '2 pieces ahead'],
    [[3, 3, 2, 2, 2, 1], 'a partly cut order: 5 of 6 on Laser cut, Sorted (2 reached it) says nothing'],
    [[3, 3, 2, 2, 2, 2], 'the sixth piece is cut: the pill moves on to Sorted'],
    [[4, 4, 3, 3, 3, 3], 'two pieces assembled: the pill moves on to Assembled'],
    [[3, 3, 3, 3, 3, 3], 'all sorted, none assembled: nobody reached the next milestone, no pill'],
    [[1, 1, 1, 1, 1, 1], 'all only nested: no pill'],
    [[5, 5, 5, 5, 5, 5], 'every piece shipped: no pill and every milestone done']
  ];

  let ctxA;   // the real page, kept for the checks that follow
  try {
    for (const width of [1440, 390]) {
      const tag = `${width}px`;
      const { page, context, errors } = await openPage(width, width === 390 ? 844 : 900); ctxA = ctxA || { page, context, errors };
      await check(`${tag} · the count is on the next unfinished milestone only, and it moves on when a piece completes (${SCENES.length} orders)`, async () => {
        for (const [levels, what] of SCENES) {
          const r = await drawn(page, '#owRail', levels);
          assert.deepEqual(r.map(n => n.k), RAIL, `${tag}: the six milestones of the screenshot`);
          assertRule(r, levels, `${tag} · ${what}`);
          // (the rail alone, and the window; at 390 px the order window does not draw the header rail at all (display:none up to 900 px) and the Timeline tab's own rail is always hidden in it, so the window alone is shot there)
          const snap = async name => { if (!shots) return; if (await page.locator('#owRail').isVisible()) { await page.locator('#owRail').scrollIntoViewIfNeeded(); await page.locator('#owRail').screenshot({ path: path.join(shots, `${name}-${width}.png`) }); } await page.screenshot({ path: path.join(shots, `${name}-window-${width}.png`) }); };
          if (levels.join() === '5,5,5,5,1,1') await snap('rail-next-count');
          if (levels.join() === '3,3,2,2,2,2') await snap('rail-next-count-moved');
          if (levels.join() === '5,5,5,5,5,5') await snap('rail-all-done');
        }
      });
      await check(`${tag} · the Timeline tab's rail (the one component, kept in the page though the window shows only the header's) follows the same rule`, async () => {
        await page.evaluate(() => document.querySelector('.owTabsV [data-ow-view="timeline"]').click());
        await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlRail .tlStop').length === 6, null, { timeout: 15000 });
        for (const levels of [[5, 5, 5, 5, 1, 1], [3, 3, 2, 2, 2, 1], [3, 3, 2, 2, 2, 2], [5, 5, 5, 5, 5, 5]]) {
          const full = await drawn(page, '#owTimeline', levels), head = await rail(page, '#owRail');
          assertRule(full, levels, `${tag} · Timeline tab`); assert.deepEqual(full.map(n => [n.k, n.pill]), head.map(n => [n.k, n.pill]), `${tag}: the two rails show the same pills`);
        }
        await page.evaluate(() => document.querySelector('.owTabsV [data-ow-view="info"]').click());
      });
      await check(`${tag} · the pill keeps its look and takes no room; every milestone keeps its words and how many pieces are at it`, async () => {
        await drawn(page, '#owRail', [5, 5, 5, 5, 1, 1]);
        const m = await page.evaluate(() => { const stops = [...document.querySelectorAll('#owRail .tlRail .tlStop')], c = stops.find(n => n.querySelector('.tlCnt:not([hidden])')).querySelector('.tlCnt'), cs = getComputedStyle(c);
          return { pos: cs.position, bg: cs.backgroundColor, color: cs.color, radius: cs.borderRadius, label: stops.map(n => n.querySelector('span').textContent), tops: stops.map(n => Math.round(n.querySelector('span').getBoundingClientRect().top)), lefts: stops.map(n => Math.round(n.getBoundingClientRect().left)), widths: stops.map(n => Math.round(n.getBoundingClientRect().width)) }; });
        assert.equal(m.pos, 'absolute', 'the pill is out of the flow: no milestone moves for it'); assert.match(m.bg, /^rgb/, 'its own amber ground'); assert.match(m.color, /^rgb\(122, 90, 29\)$/, 'the amber text of the pill is as it was'); assert(parseFloat(m.radius) >= 8, 'a pill');
        assert.deepEqual(m.label, ['Order in', 'Nested', 'Laser cut', 'Sorted', 'Assembled', 'Shipped']); assert.equal(new Set(m.tops).size, 1, `the labels stay on one line (${m.tops})`);
        // the same rail with the pill taken away: nothing moves (the labels keep their places, nothing is reserved for a pill)
        const geo = () => page.evaluate(() => [...document.querySelectorAll('#owRail .tlRail .tlStop')].map(n => [n, n.querySelector('span'), n.querySelector('.tlSeal')].map(e => { const r = e.getBoundingClientRect(); return [r.left, r.top, r.width, r.height].map(v => Math.round(v * 100) / 100); })));
        const withPill = await geo(); await page.evaluate(() => document.querySelectorAll('#owRail .tlCnt').forEach(c => { c.hidden = true; })); const without = await geo();
        assert.deepEqual(without, withPill, 'milestones, labels and seals keep their places with and without the pill: it reserves no room');
        await drawn(page, '#owRail', [5, 5, 5, 5, 5, 5]);   // (the next read puts the pill back where the rule says: nowhere here)
        assert.equal((await rail(page, '#owRail')).filter(n => n.pill).length, 0);
        const r = await drawn(page, '#owRail', [5, 5, 2, 2, 2, 2]);   // (all six are cut, two went on: Sorted is next, Assembled and Shipped are plain, each with its words)
        assert.match(r[3].aria, /Sorted: next · 2 of 6 pieces$/, 'the next milestone says how many pieces reached it, in its words'); assert.equal(r[3].pill, '2 of 6');
        assert.match(r[4].aria, /Assembled: still to come · 2 of 6 pieces$/, 'a plain later milestone still says how many pieces reached it, in its words'); assert.equal(r[4].pill, null);
      });
      await context.close();
      assert.deepEqual(errors, [], `${tag}: no page errors`);
    }

    await check('only one renderer draws the milestone count: no other list, card, panel or page has its own "N of M" rail pill', async () => {
      const files = fs.readdirSync(root).filter(f => /\.(js|html)$/.test(f));
      const users = files.filter(f => /tlCnt/.test(fs.readFileSync(path.join(root, f), 'utf8'))).sort();
      assert.deepEqual(users, ['charm-nest-timeline-ui.js'], 'the tlCnt pill is made in one file');
      // the other rails the sorter draws are dots and sheet steps: none of them counts pieces ("N of M") on a milestone
      const search = fs.readFileSync(path.join(root, 'charm-nest-search.js'), 'utf8'), rail = /function rail\(st\) \{[\s\S]*?\n  \}/.exec(search);
      assert(rail, 'the search cards\' rail function'); assert.doesNotMatch(rail[0], /\} of \$\{|' of '|" of "/, 'the search card rail draws dots only, no count');
      const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), flow = /function railHtml\(e,id,open,w\)\{[\s\S]*?\n  \}/.exec(bridge);
      assert(flow, 'the Library sheet process rail'); assert.doesNotMatch(flow[0], /\} of \$\{|' of '|" of "|tlCnt/, 'the sheet process rail has no count pill');
      // the one renderer's rule, read from its source: the count needs the next unfinished milestone
      const ui = fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8'); assert.match(ui, /part = j === next && sr && sr\.n > 0 && sr\.n < sr\.of/);
    });

    // the mutants: the count on every unfinished milestone that some pieces reached (as it was), and on none at all
    for (const [name, fn] of Object.entries(mutant)) {
      await check(`a mutant that shows the count ${name === 'every' ? 'on every milestone some pieces reached' : 'nowhere'} is caught`, async () => {
        const { page, context } = await openPage(1440, 900, fn);
        try {
          let caught = null;
          try { for (const [levels, what] of SCENES.slice(0, 3)) assertRule(await drawn(page, '#owRail', levels), levels, `mutant ${name} · ${what}`); } catch (e) { caught = e; }
          assert(caught, 'the scenes did not notice the mutant'); console.log('      caught: ' + String(caught.message).split('\n')[0].slice(0, 200));
        } finally { await context.close(); }
      });
    }
  } finally { await browser.close(); srv.close(); }
  const bad = results.filter(r => r[0] !== 'ok');
  if (bad.length) { console.error(`\n${bad.length} failed`); process.exit(1); }
  console.log('Rail next count only OK');
}
main().catch(e => { console.error(e); process.exit(1); });
