// NO "N of M" count on the order window's milestone rail (Paul, 12 Oct 2026: "this 1 of 3 is confusing in the UI. It is not clear what this means and
// doesn't really help the user so I would just remove it"). This REVERSES the 5 Oct 2026 rule that drew the count ("4 of 6") over the next unfinished
// milestone only. The sorter page over the local stand-in (bridge-server.cjs); no network but the loopback, nothing real is written. A 6-piece order
// (every piece its own line) whose timeline is written into the fake store, the open order window reading it back every 0.3 s.
//   1 · for every scene of piece levels (4 of 6 shipped, 2 ahead, partly cut, every step reached by some pieces, nothing reached, all shipped): the header
//       rail has no .tlCnt element and no "N of M" text on any milestone, at 1440 and 390 px
//   2 · the Timeline tab's rail (the one component) the same
//   3 · the rail is otherwise as it was: the six milestones, their words on one line, and each milestone's aria-label still says how many pieces are at it
//       (screen-reader text, never drawn). The pill was out of the flow (position:absolute), so taking it out moves nothing
//   4 · no file draws a pill any more: no .tlCnt / tlCntIn in any page or script, no "N of M" in the other rails (search cards, Library sheet process rail)
//   5 · a mutant that draws the count again (the 5 Oct rule put back) is caught
//   node tests/charm-nest/rail-no-count-pill.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOTS=dir keeps screenshots)
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
const order = () => ({ receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Test Buyer' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: TIDS.map((t, i) => ({ transactionId: t, listingId: '18000' + t.slice(-4), sku: SKUS[i], title: SKUS[i].replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' })) });
const sleep = ms => new Promise(r => setTimeout(r, ms));

/** How many pieces reached each step of the rail for these piece levels (what each step's aria-label still says). */
function expectFor(levels) {
  return { reached: RAIL.map((_, j) => j === 0 ? N : levels.filter(l => l >= j).length) };
}


const mutant = { back: s => { const t = s.replace('<i class="tlSeal"></i><span>${esc(r.s.l)}</span></button>', '<i class="tlSeal"></i><span>${esc(r.s.l)}</span><em class="tlCnt">1 of 2</em></button>'); assert.notEqual(t, s, 'the mutant found the line it changes'); return t; } };

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.CN_SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
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
    page.setDefaultTimeout(120000);
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
  /** The rail of a host ("#owRail" or "#owTimeline"): each milestone, its state, its pill (any .tlCnt element, shown or not, and any "N of M" in its drawn text) and its aria-label. */
  const rail = (page, host) => page.evaluate(h => [...document.querySelectorAll(h + ' .tlRail .tlStop')].map(n => ({ k: n.dataset.stage, cls: n.className.replace(/^tlStop\s*/, ''), pill: n.querySelectorAll('.tlCnt').length, drawn: /\d+ of \d+/.test(n.textContent), words: n.querySelector('span').textContent, aria: n.getAttribute('aria-label') || '' })), host);
  const piecesAt = a => { const m = /· (\d+) of (\d+) pieces?$/.exec(a); return m ? +m[1] : null; };
  /** Writes the levels, gives the open window three of its 0.3 s reads, then returns the rail once every milestone says how many pieces are at it.
   *  (Which pieces count as AT a milestone is the placement's to say, PiecePlacement: this test's pieces sit on no real sheet, so the counts are not the levels'; the rule here holds for any.) */
  async function drawn(page, host, levels) {
    setLevels(levels); await sleep(1000);
    const t0 = Date.now(); let r;
    for (;;) {
      r = await rail(page, host);
      if (r.length === RAIL.length && r.every(n => piecesAt(n.aria) != null)) break;
      if (Date.now() - t0 > 60000) throw new Error(`the rail never read the levels ${JSON.stringify(levels)}: ${JSON.stringify(r.map(n => [n.k, n.aria]))}`);
      await sleep(300);
    }
    return r;
  }
  const assertNone = (r, tag) => { for (const n of r) { assert.equal(n.pill, 0, `${tag}: no pill element on ${n.k}`); assert.equal(n.drawn, false, `${tag}: no "N of M" drawn on ${n.k}`); } };
  const SCENES = [
    [[5, 5, 5, 5, 1, 1], 'the order of the screenshot: 4 pieces shipped, 2 only nested'],
    [[5, 5, 1, 1, 1, 1], '2 pieces ahead'],
    [[3, 3, 2, 2, 2, 1], 'a partly cut order'],
    [[3, 3, 2, 2, 2, 2], 'the sixth piece is cut'],
    [[4, 4, 3, 3, 3, 3], 'two pieces assembled'],
    [[3, 3, 3, 3, 3, 3], 'all sorted, none assembled'],
    [[1, 1, 1, 1, 1, 1], 'all only nested'],
    [[5, 5, 5, 5, 5, 5], 'every piece shipped']
  ];

  try {
    for (const width of [1440, 390]) {
      const tag = `${width}px`;
      const { page, context, errors } = await openPage(width, width === 390 ? 844 : 900);
      await check(`${tag} · no "N of M" pill on any milestone of the header rail, whatever the pieces have reached (${SCENES.length} orders)`, async () => {
        for (const [levels, what] of SCENES) {
          const r = await drawn(page, '#owRail', levels);
          assert.deepEqual(r.map(n => n.k), RAIL, `${tag}: the six milestones`);
          assertNone(r, `${tag} · ${what}`);
          if (shots && levels.join() === '5,5,5,5,1,1' && await page.locator('#owRail').isVisible()) await page.locator('#owRail').screenshot({ path: path.join(shots, `rail-no-pill-${width}.png`) });
        }
      });
      await check(`${tag} · the Timeline tab's rail (the one component) draws none either`, async () => {
        await page.evaluate(() => document.querySelector('.owTabsV [data-ow-view="timeline"]').click());
        await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlRail .tlStop').length === 6, null, { timeout: 15000 });
        for (const levels of [[5, 5, 5, 5, 1, 1], [3, 3, 2, 2, 2, 1], [3, 3, 2, 2, 2, 2], [5, 5, 5, 5, 5, 5]]) assertNone(await drawn(page, '#owTimeline', levels), `${tag} · Timeline tab ${levels}`);
        await page.evaluate(() => document.querySelector('.owTabsV [data-ow-view="info"]').click());
      });
      await check(`${tag} · the rail is otherwise as it was: its words on one line, the screen-reader label still says how many pieces are at each step, nothing moves for levels`, async () => {
        const a = await drawn(page, '#owRail', [5, 5, 5, 5, 1, 1]);
        assert.deepEqual(a.map(n => n.words), ['Order in', 'Nested', 'Laser cut', 'Sorted', 'Assembled', 'Shipped']);
        assert(a.every(n => / · \d+ of 6 pieces$/.test(n.aria)), 'every milestone\'s screen-reader label still says how many pieces are at it: ' + JSON.stringify(a.map(n => n.aria)));
        const b = await drawn(page, '#owRail', [3, 3, 2, 2, 2, 1]);
        assert.equal(new Set(await page.evaluate(() => [...document.querySelectorAll('#owRail .tlRail .tlStop span')].map(e => Math.round(e.getBoundingClientRect().top)))).size, 1, 'the labels stay on one line');
        assertNone(b, tag);
      });
      await context.close();
      assert.deepEqual(errors, [], `${tag}: no page errors`);
    }

    await check('no file draws a milestone count: no tlCnt anywhere, no "N of M" in the search cards\' rail or the Library sheet process rail', async () => {
      const files = fs.readdirSync(root).filter(f => /\.(js|html)$/.test(f));
      const users = files.filter(f => /tlCnt/.test(fs.readFileSync(path.join(root, f), 'utf8'))).sort();
      assert.deepEqual(users, [], 'no page or script has a tlCnt pill, its CSS or its keyframes');
      const search = fs.readFileSync(path.join(root, 'charm-nest-search.js'), 'utf8'), railFn = /function rail\(st\) \{[\s\S]*?\n  \}/.exec(search);
      assert(railFn, 'the search cards\' rail function'); assert.doesNotMatch(railFn[0], /\} of \$\{|' of '|" of "/, 'the search card rail draws dots only, no count');
      const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), flow = /function railHtml\(e,id,open,w\)\{[\s\S]*?\n  \}/.exec(bridge);
      assert(flow, 'the Library sheet process rail'); assert.doesNotMatch(flow[0], /\} of \$\{|' of '|" of "|tlCnt/, 'the sheet process rail has no count pill');
      const ui = fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8'); assert.doesNotMatch(ui, /part = j === next/, 'the 5 Oct rule that drew the count is gone from the renderer');
    });

    // the mutant: the pill put back
    for (const [name, fn] of Object.entries(mutant)) {
      await check(`a mutant that draws the count again is caught`, async () => {
        const { page, context } = await openPage(1440, 900, fn);
        try {
          let caught = null;
          try { assertNone(await drawn(page, '#owRail', SCENES[0][0]), `mutant ${name}`); } catch (e) { caught = e; }
          assert(caught, 'the scenes did not notice the mutant'); console.log('      caught: ' + String(caught.message).split('\n')[0].slice(0, 200));
        } finally { await context.close(); }
      });
    }
  } finally { await browser.close(); srv.close(); }
  const bad = results.filter(r => r[0] !== 'ok');
  if (bad.length) { console.error(`\n${bad.length} failed`); process.exit(1); }
  console.log('Rail no count pill OK');
}
main().catch(e => { console.error(e); process.exit(1); });
