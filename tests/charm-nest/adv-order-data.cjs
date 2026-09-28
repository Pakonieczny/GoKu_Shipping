// Adversarial (wave 2): the order view's data. One order open = ONE timelineGet, shared by the view's Overview, the
// header rail and the Timeline tab; a refresh joins one already on its way; what another device records shows on the
// next refresh (a placement, a hold, a station scan, a cancel, its undo); Previous/Next never paints an old order's
// answer on the new one; a failed or slow read has its spinner and its message; the passcode goes with the read; the
// sandbox reads the sandbox; an order of 2000+ steps says it is cut short.
// Every timelineGet the page sends is counted at the browser's edge (context.route). No network but the loopback.
//   node tests/charm-nest/adv-order-data.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), http = require('http'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000), NOW = Date.now();
const A = { rid: '4176208841', tid: '41762088411', sku: 'TINY_TAG' }, B2 = { rid: '4176200172', tid: '41762001721', sku: 'LEAF_CHARM' }, D = { rid: '4176300999', tid: '41763009991', sku: 'BIG_ORDER' };
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: o.tid, listingId: '1800000' + o.tid.slice(-3), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' }] });
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  let n = 0;
  const ev = (rid, type, at, extra, pre = '') => { const id = `e${++n}`; srv.st.put(pre + 'Order_Timeline', `${rid}~${type}~${id}`, Object.assign({ orderId: rid, type, at, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {})); };
  ev(A.rid, 'arrived', NOW - 3 * DAY * 1000, { source: 'etsy', by: 'Etsy' }); ev(A.rid, 'decided', NOW - 2 * DAY * 1000);
  ev(B2.rid, 'arrived', NOW - 4 * DAY * 1000); ev(B2.rid, 'placed', NOW - 3 * DAY * 1000, { sheet: 'GF Sheet 1', sheetId: 's1' }); ev(B2.rid, 'sorted', NOW - 2 * DAY * 1000, { station: 'sorting' }); ev(B2.rid, 'shipped', NOW - DAY * 1000, { station: 'shipping' });
  for (let i = 0; i < 2100; i++) ev(D.rid, i % 50 ? 'scan' : 'note', NOW - (2100 - i) * 60000, { station: i % 50 ? 'sorting' : '', text: 'step ' + i });
  ev(A.rid, 'welded', NOW - DAY * 1000, { station: 'welding' }, 'Sandbox_');   // the sandbox plays the same number

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctl = { gets: [], hold: null, holdMs: 0, fail: false, pass: '' };
  const errors = [];
  async function session(settings) {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    // the timeline reads, counted, and held, failed or refused (a passcode) on demand
    await context.route(u => /\/\.netlify\/functions\/charmNestLibrary/.test(u.href), async route => {
      let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
      if (b.op !== 'timelineGet') return route.fallback();
      const hdr = route.request().headers()['x-edit-passcode'] || '';
      ctl.gets.push({ rid: b.orderId, sandbox: b.sandbox, pass: hdr, t: Date.now() });
      if (ctl.pass && hdr !== ctl.pass) return route.fulfill({ status: 401, contentType: 'application/json', body: JSON.stringify({ error: 'unauthorized' }) });
      if (ctl.fail) { await sleep(ctl.holdMs || 0); return route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: 'Firestore is unavailable' }) }); }
      if (ctl.hold && ctl.hold(b.orderId)) { const resp = await route.fetch(); await sleep(ctl.holdMs); return route.fulfill({ response: resp }).catch(() => {}); }
      return route.fallback();
    });
    await context.addInitScript(s => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; }, settings);
    const page = await context.newPage();
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderTimelineUI && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, [order(A, 'Hannah Whitford'), order(B2, 'Ava Patel'), order(D, 'Busy Buyer')]);
    return { context, page };
  }
  const keyOf = o => `${o.rid}_${o.tid}`;
  const gets = (rid, since = 0) => ctl.gets.filter(g => g.rid === rid && g.t >= since).length;
  const shown = page => page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, pill: document.getElementById('owNow').textContent, count: document.getElementById('owTlCount').textContent,
    cx: document.getElementById('orderWin').classList.contains('owCancelled'), railFor: (document.querySelector('#owRail .tlUI') || {}).dataset?.order || '', rail: (document.querySelector('#owRail .tlNowT') || {}).textContent || '',
    full: (document.querySelector('#owTimeline .tlUI .tlNowT') || {}).textContent || '', card: (document.querySelector('#owNowCard') || {}).textContent || '',
    railMsg: (document.querySelector('#owRail .tlMsg:not([hidden])') || {}).textContent || '', fullMsg: (document.querySelector('#owTimeline .tlMsg:not([hidden])') || {}).textContent || '',
    railSpin: !!document.querySelector('#owRail .tlMsg:not([hidden]) .tlSpin'), fullSpin: !!document.querySelector('#owTimeline .tlMsg:not([hidden]) .tlSpin') }));
  const railReady = page => page.waitForFunction(() => { const r = document.querySelector('#owRail .tlUI'); return r && r.querySelector('.tlMsg').hidden && document.getElementById('owTlCount').textContent; });
  const fullReady = page => page.waitForFunction(() => document.querySelector('#owTimeline .tlUI .tlSt[data-key]'));
  const closeView = async page => { await page.evaluate(() => OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 }); };
  const refresh = page => page.evaluate(() => OrderWin._feed().refresh({ force: true }));
  const results = [];
  const stepsOf = (page, rid) => page.evaluate(rid => OrderTimeline.get(rid).then(j => String(j.events.length)), rid);
  let cur = null;
  const check = async (name, fn) => { try { await fn(); results.push(['ok', name]); console.log('  ✓ ' + name); } catch (e) { results.push(['FAIL', name]); console.log('  ✗ ' + name + '\n      ' + String(e.message).split('\n')[0]); try { if (cur && await cur.evaluate(() => document.getElementById('orderWin').open)) await closeView(cur); } catch (_) {} } };
  try {
    const { page } = await session({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' });

    cur = page;
    // 1 · the reads per open
    let perOpen = [];
    await check('one timelineGet per open: Overview, the header rail and the Timeline tab share it', async () => {
      let t0 = Date.now(); const fns = {}, onReq = r => { const m = /\/\.netlify\/functions\/([\w-]+)/.exec(r.url()); if (m) { let op = ''; try { op = JSON.parse(r.postData() || '{}').op || ''; } catch (_) {} const k = m[1] + (op ? ':' + op : ''); fns[k] = (fns[k] || 0) + 1; } };
      page.on('request', onReq);
      await page.evaluate(k => OrderWin.open(k), keyOf(A)); await railReady(page); await sleep(600);
      page.off('request', onReq); console.log('      every cloud call of one open (Overview, 0.6 s after the rail is drawn):', JSON.stringify(fns));
      await page.click('.owTabsV [data-ow-view="timeline"]'); await fullReady(page); await sleep(600);
      await page.click('.owTabsV [data-ow-view="info"]'); await sleep(300); await page.click('.owTabsV [data-ow-view="timeline"]'); await sleep(600);
      perOpen.push(gets(A.rid, t0)); await closeView(page);
      t0 = Date.now();
      await page.evaluate(k => OrderWin.open(k, { view: 'timeline' }), keyOf(A)); await fullReady(page); await railReady(page); await sleep(800);
      perOpen.push(gets(A.rid, t0));
      console.log('      timelineGet per open (Overview then Timeline tab, back and forth; opened on Timeline):', perOpen.join(', '));
      assert.deepEqual(perOpen, [1, 1], 'timelineGet calls per open');
    });
    await check('a refresh dedupes: three at once from the view, the rail and the Timeline are one read', async () => {
      const t0 = Date.now();
      await page.evaluate(() => { const f = OrderWin._feed(); return Promise.all([f.refresh({ force: true }), f.refresh({ force: true }), f.refresh()]); });
      await sleep(200); assert.equal(gets(A.rid, t0), 1);
    });
    await check('the live poll: one read per interval for every listener, none after destroy()', async () => {
      const t0 = Date.now();
      const got = await page.evaluate(async rid => { const f = OrderTimelineUI.feed(rid, { pollMs: 300 }); let n = 0; const offs = [f.subscribe(k => { if (k === 'data') n++; }), f.subscribe(() => {}), f.subscribe(() => {})]; f.refresh(); await new Promise(r => setTimeout(r, 1100)); f.destroy(); return { n, offs: offs.length }; }, A.rid);
      const during = gets(A.rid, t0); await sleep(800); const after = gets(A.rid, t0);
      assert(during >= 3 && during <= 5, 'reads while live: ' + during); assert.equal(after, during, 'no read after destroy'); assert(got.n >= 3);
    });

    // 2 · what another device records while the view is open shows on the next refresh, everywhere in the view
    await check('stale data: placement, hold, station scan, cancel and its undo from another device show on the next refresh', async () => {
      let s = await shown(page); const c0 = +s.count;
      ev(A.rid, 'placed', Date.now() - 5000, { sheet: 'GF Sheet 3', sheetId: 'sh3', by: 'Maria' }); await refresh(page); await sleep(150);
      s = await shown(page); assert.match(s.full, /GF Sheet 3/, 'placement: ' + s.full); assert.match(s.rail, /GF Sheet 3/, 'rail placement: ' + s.rail); assert.equal(+s.count, c0 + 1);
      ev(A.rid, 'held', Date.now() - 4000, { text: 'Customer asked to wait', data: { reason: 'Customer asked to wait' }, by: 'Maria' }); await refresh(page); await sleep(150);
      s = await shown(page); assert.match(s.full, /On hold/, 'hold: ' + s.full); assert.match(s.card, /Customer asked to wait/, 'card: ' + s.card);
      const body = JSON.stringify({ timeline: [{ orderId: A.rid, type: 'scan', station: 'sorting', device: 'Sorting-2', by: 'Jo', id: 'scan-dev2', at: Date.now() - 3000 }] });
      await new Promise((res, rej) => { const r = http.request(`${srv.sorterOrigin}/.netlify/functions/firebaseOrders`, { method: 'POST', headers: { 'Content-Type': 'application/json' } }, x => { x.resume(); x.on('end', res); }); r.on('error', rej); r.end(body); });
      await refresh(page); await sleep(150);
      s = await shown(page); assert.equal(+s.count, c0 + 3, 'the scan counted'); assert.match(s.card, /sorting/i, 'card names the station: ' + s.card);
      srv.st.put('Charm_Nest_Cancelled', A.rid, { orderId: A.rid, at: Date.now() - 2000, by: 'Maria', why: 'buyer asked' }); ev(A.rid, 'cancelled', Date.now() - 2000, { by: 'Maria', id: 'cx' }); await refresh(page); await sleep(150);
      s = await shown(page); assert(s.cx, 'the view turns cancelled'); assert.equal(s.pill, 'Cancelled'); assert.match(s.full, /Cancelled/); assert.match(s.rail, /Cancelled/);
      srv.st.docs.delete('Charm_Nest_Cancelled/' + A.rid); ev(A.rid, 'cancelRestored', Date.now() - 1000, { by: 'Maria' }); await refresh(page); await sleep(150);
      s = await shown(page); assert(!s.cx, 'the cancel undone clears it'); assert.doesNotMatch(s.full, /Cancelled/); assert.notEqual(s.pill, 'Cancelled');
    });
    await closeView(page);

    // 3 · Previous/Next fast: an answer for the order left behind never paints the one in front
    await check('prev/next fast: the old order\'s late answer never paints the new one (A → B, and A → B → A)', async () => {
      const list = await page.evaluate(() => Orders.visibleRows().map(r => r.key));
      const iA = list.indexOf(keyOf(A)), iB = list.indexOf(keyOf(B2)); assert(iA >= 0 && iB >= 0, 'both in the list');
      const toB = iB > iA ? '#owNext' : '#owPrev', toA = iB > iA ? '#owPrev' : '#owNext';
      assert.equal(Math.abs(iA - iB), 1, 'neighbours');
      ctl.hold = rid => rid === A.rid; ctl.holdMs = 1200;
      await page.evaluate(k => OrderWin.open(k), keyOf(A)); await sleep(120);
      await page.click(toB); await sleep(1600);
      let s = await shown(page); const nB = await stepsOf(page, B2.rid); assert.equal(s.title, 'Order ' + B2.rid); assert.equal(s.railFor, B2.rid); assert.equal(s.count, nB, 'B\'s steps: ' + s.count); assert(!s.cx);
      assert.match(s.pill, /Shipped/, 'B\'s pill: ' + s.pill);
      // A (held: its answer is read now and arrives late), B, A again (a new step recorded meanwhile, answered at once)
      await page.click(toA); await sleep(80); await page.click(toB); await sleep(80);
      ctl.hold = null; ev(A.rid, 'note', Date.now() - 500, { text: 'late note', id: 'late' });
      await page.click(toA); await sleep(1800);
      s = await shown(page); const want = await stepsOf(page, A.rid);
      assert.equal(s.title, 'Order ' + A.rid); assert.equal(s.count, want, 'A shows its newest answer, not the late one: ' + s.count + ' vs ' + want);
    });
    ctl.hold = null;
    await closeView(page);

    // 4 · errors: the wait has its spinner and the failure its message, in the rail and the Timeline alike; Retry is one read
    await check('errors: spinner while waiting, a message and Retry on failure, one read to recover', async () => {
      ctl.fail = true; ctl.holdMs = 900; const t0 = Date.now();
      await page.evaluate(k => OrderWin.open(k), keyOf(B2)); await sleep(150);
      let s = await shown(page); assert(s.railSpin && /Loading the steps/.test(s.railMsg), 'rail spinner: ' + s.railMsg);
      await page.click('.owTabsV [data-ow-view="timeline"]'); await sleep(150);
      s = await shown(page); assert(s.fullSpin && /Loading the timeline/.test(s.fullMsg), 'timeline spinner: ' + s.fullMsg);
      await sleep(1200);
      s = await shown(page); assert.match(s.railMsg, /Couldn't load the steps/); assert.match(s.fullMsg, /Couldn't load the timeline: Firestore is unavailable/);
      assert.equal(gets(B2.rid, t0), 1, 'one read failed for the whole view');
      ctl.fail = false; ctl.holdMs = 0; const t1 = Date.now();
      await page.click('#owTimeline .tlMsg .tlRetry'); await fullReady(page); await railReady(page); await sleep(300);
      s = await shown(page); assert.equal(s.railMsg, ''); assert.equal(s.fullMsg, ''); assert.equal(gets(B2.rid, t1), 1, 'Retry is one read'); assert.equal(s.count, await stepsOf(page, B2.rid));
    });
    await closeView(page);

    // 5 · the passcode (EDIT_PASSCODE set): the read carries it; a wrong one says so
    await check('EDIT_PASSCODE: the read carries X-Edit-Passcode; a refused read says so', async () => {
      ctl.pass = 'pc-123'; await page.evaluate(() => { CN.S.passcode = 'pc-123'; CNTimeline.sync(); });
      const t0 = Date.now(); await page.evaluate(k => OrderWin.open(k), keyOf(A)); await railReady(page);
      assert(ctl.gets.filter(g => g.t >= t0).every(g => g.pass === 'pc-123'));
      await closeView(page);
      await page.evaluate(() => { CN.S.passcode = 'stale'; CNTimeline.sync(); });
      await page.evaluate(k => OrderWin.open(k, { view: 'timeline' }), keyOf(A)); await page.waitForFunction(() => /Couldn't load/.test((document.querySelector('#owTimeline .tlMsg') || {}).textContent || ''));
      assert.match(await page.textContent('#owTimeline .tlMsg'), /unauthorized/);
      ctl.pass = ''; await page.evaluate(() => { CN.S.passcode = ''; CNTimeline.sync(); });
      await closeView(page);
    });

    // 6 · an order with more than 2000 steps: the server answers 2000 and says truncated; the view says so and stays usable
    await check('2000+ steps: drawn quickly and said to be cut short', async () => {
      const t0 = Date.now();
      await page.evaluate(k => OrderWin.open(k, { view: 'timeline' }), keyOf(D)); await fullReady(page); const ms = Date.now() - t0;
      const s = await page.evaluate(() => ({ sum: document.querySelector('#owTimeline .tlSum').textContent, stamps: document.querySelectorAll('#owTimeline .tlSt[data-key]').length, count: document.getElementById('owTlCount').textContent }));
      console.log(`      2000+ steps: first paint ${ms} ms, ${s.stamps} stamps, count ${s.count}, summary "${s.sum}"`);
      assert(ms < 8000, 'painted in ' + ms + ' ms');
      assert.match(s.sum, /2000|first|cut short|more/i, 'says it is truncated: ' + s.sum);
      await closeView(page);
    });
    assert.deepEqual(errors, [], 'no page errors');

    // 7 · the sandbox reads the sandbox (a real order number the sandbox plays with its own steps)
    await check('sandbox: the view reads Sandbox_ steps, and production reads production', async () => {
      const prodGets = ctl.gets.filter(g => g.rid === A.rid); assert(prodGets.length && prodGets.every(g => g.sandbox === false), 'production reads say sandbox:false');
      const { page: sp, context } = await session({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'on' });
      const t0 = Date.now();
      await sp.evaluate(k => OrderWin.open(k, { view: 'timeline' }), keyOf(A)); await fullReady(sp); await sleep(300);
      const s = await shown(sp); const g = ctl.gets.filter(x => x.t >= t0);
      assert.equal(g.length, 1); assert.equal(g[0].sandbox, true); assert.equal(s.count, await stepsOf(sp, A.rid)); assert.match(s.full, /Welded/i, s.full);
      const prodOnly = await sp.evaluate(() => [...document.querySelectorAll('#owTimeline .tlSt[data-key]')].map(b => b.dataset.key).filter(k => /~(placed|held|cancelled|decided)~/.test(k)));
      assert.deepEqual(prodOnly, [], 'no production step in the sandbox view');
      await context.close();
    });
  } finally { await browser.close(); srv.close(); }
  const bad = results.filter(r => r[0] !== 'ok');
  if (bad.length || errors.length) { console.error(`${bad.length} failed`); process.exit(1); }
}
main().then(() => console.log('Order view data OK')).catch(e => { console.error(e); process.exit(1); });
