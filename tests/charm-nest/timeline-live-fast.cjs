// The open order view follows what is done within 2-3 s (Paul, 3 Oct 04:03: "I just approved the engraving ... the above
// timeline took like 30 seconds to update ... it needs to update in real time or within two or three seconds maximum to
// reflect whatever the user is doing"). The sorter page over the local stand-in (bridge-server.cjs); no network but the
// loopback, nothing real is written.
//   1 · an approval that stands (Engrave.timelineApproved, what settleApproval calls) puts the Engraved seal on the header
//       rail and one stamp on the Timeline tab at once, the server gets it within a second, and the server's own copy
//       (backPut's stamp: the same key, with its sheet) lands on the same seal: never doubled, never gone in between
//   2 · a station scan written by another computer shows on the open view with no refresh from here, inside 3.3 s
//   3 · the open view reads every 2.5 s, one read at a time; none once it is closed
//   4 · a feed backs off after failed reads and keeps what is drawn (no error until the third), pauses in a hidden tab and
//       reads at once when the tab is seen again
//   node tests/charm-nest/timeline-live-fast.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), http = require('http'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000), NOW = Date.now();
const A = { rid: '4176208841', tid: '41762088411', sku: 'TINY_TAG' };
const order = o => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Hannah Whitford' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: o.tid, listingId: '1800000' + o.tid.slice(-3), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' }] });
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const ev = (type, at, extra) => srv.st.put('Order_Timeline', `${A.rid}~${type}~${extra && extra.id || type}`, Object.assign({ orderId: A.rid, type, at, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra));
  ev('arrived', NOW - 3 * DAY * 1000, { source: 'etsy', by: 'Etsy' }); ev('placed', NOW - 2 * DAY * 1000, { sheet: 'GF Sheet 1', sheetId: 'sh1' });

  // the piece stands on its sheet (the page's PiecePlacement reads the pool piece and the sheet): since 5 Oct the rail follows where the piece IS, so a
  // piece on no sheet has Nested and every step after it hollow and would never stamp Engraved, however fast the approval arrives (LB2, 7 Oct)
  { const pid = `${A.rid}_${A.tid}_1`;
    srv.st.put('Charm_Pool', pid, { poolId: pid, orderId: A.rid, transactionId: A.tid, lineKey: `${A.rid}_${A.tid}`, sku: A.sku, material: 'gold', copy: 1, quantity: 1, runId: 'r1', state: 'written', sheetId: 'sh1', setId: 'GF_Set-1', sheetName: 'GF Sheet 1', createdAt: NOW - 3600e3, updatedAt: NOW - 600e3 });
    srv.st.put('Charm_Nest_Sheets', 'sh1', { id: 'sh1', setId: 'GF_Set-1', setSeq: 1, sheetIndex: 1, runId: 'r1', metal: 'gold', day: '2026-10-06', fileBase: 'GF Sheet 1', folder: 'GF Sheet 1', status: 'complete', placedCount: 1, charmCount: 1, placements: [{ id: 'c0', cxPt: 30, cyPt: 30, angle: 0, wPt: 28, hPt: 28 }], charms: [{ id: 'c0', poolId: pid, order: A.rid, sku: A.sku, name: 'x' }], poolIds: [pid], orders: [A.rid] }); }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctl = { gets: [], inflight: 0, maxInflight: 0, fail: false };
  const errors = [];
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await context.route(u => /\/\.netlify\/functions\/charmNestLibrary/.test(u.href), async route => {
    let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
    if (b.op !== 'timelineGet') return route.fallback();
    ctl.gets.push({ rid: b.orderId, t: Date.now() }); ctl.inflight++; ctl.maxInflight = Math.max(ctl.maxInflight, ctl.inflight);
    try {
      await sleep(60);   // (a read is never instant)
      if (ctl.fail) return await route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: 'Firestore is unavailable' }) });
      return await route.fallback();
    } finally { ctl.inflight--; }
  });
  await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
  const page = await context.newPage();
  page.setDefaultTimeout(20000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderTimelineUI && window.Engrave && CN.S.cloud.ok === true, null, { timeout: 60000 });
  await page.evaluate(async o => {
    await Orders.loadMaps(true);
    for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    Orders.interpretAll(); CN.setMode('orders'); Orders.render();
  }, order(A));
  const key = `${A.rid}_${A.tid}`, poolId = `${A.rid}_${A.tid}_1`;
  const results = [];
  const check = async (name, fn) => { try { await fn(); results.push(['ok', name]); console.log('  ✓ ' + name); } catch (e) { results.push(['FAIL', name]); console.log('  ✗ ' + name + '\n      ' + String(e.message).split('\n').slice(0, 3).join('\n      ')); } };
  const getsSince = t => ctl.gets.filter(g => g.rid === A.rid && g.t >= t).length;
  const railDone = stage => page.evaluate(s => !!document.querySelector(`#owRail .tlStop[data-stage="${s}"].d`), stage);
  const stamps = () => page.evaluate(() => document.querySelectorAll('#owTimeline .tlSt[data-key^="engraveApproved~"]').length);
  await page.evaluate(k => OrderWin.open(k), key);
  await page.waitForFunction(() => { const r = document.querySelector('#owRail .tlUI'); return r && r.querySelector('.tlMsg').hidden && document.getElementById('owTlCount').textContent; });
  await page.click('.owTabsV [data-ow-view="timeline"]'); await page.waitForFunction(() => document.querySelector('#owTimeline .tlUI .tlSt[data-key]'));
  await page.click('.owTabsV [data-ow-view="info"]');

  // 1 · the approval
  await check('an approval shows on the rail and the Timeline at once; the server\'s copy lands on the same seal, never doubled', async () => {
    assert(!(await railDone('engraved')) && (await stamps()) === 0, 'not engraved yet');
    const at = Date.now(), id = `${poolId}-${at}`, k = `${A.rid}~engraveApproved~${id}`;
    // a watcher: the rail's Engraved step, once drawn, is never undrawn, and the Timeline never holds two of the stamp
    await page.evaluate(() => { window.__seen = { rail: false, lost: 0, max: 0 }; setInterval(() => { const on = !!document.querySelector('#owRail .tlStop[data-stage="engraved"].d'); if (window.__seen.rail && !on) window.__seen.lost++; window.__seen.rail = window.__seen.rail || on; window.__seen.max = Math.max(window.__seen.max, document.querySelectorAll('#owTimeline .tlSt[data-key^="engraveApproved~"]').length); }, 20); });
    const ms = await page.evaluate(async ({ key, poolId, at }) => {
      const row = Orders.rows().find(r => r.key === key), t0 = performance.now();
      Engrave.timelineApproved({ key, row, copies: [poolId], text: 'Love, Mom', approvedAt: at, approvedBy: 'Test Operator' });
      while (!document.querySelector('#owRail .tlStop[data-stage="engraved"].d') && performance.now() - t0 < 5000) await new Promise(r => setTimeout(r, 10));
      return Math.round(performance.now() - t0);
    }, { key, poolId, at });
    console.log(`      approval → the Engraved seal on the header rail: ${ms} ms`);
    assert(ms < 400, 'the rail stamped it at once: ' + ms + ' ms');
    // the server has it within a second (sent at once, not in the outbox's 900 ms batch)
    const t0 = Date.now(); while (!srv.st.doc('Order_Timeline', k) && Date.now() - t0 < 3000) await sleep(20);
    const sent = Date.now() - t0; console.log(`      approval → the event on the server: ${sent} ms`); assert(srv.st.doc('Order_Timeline', k), 'the server has the approval'); assert(sent < 1000, 'sent at once: ' + sent + ' ms');
    // the Timeline tab (already mounted) has its one stamp, and the Overview's card knows the step
    assert.equal(await stamps(), 1, 'one stamp on the Timeline');
    // backPut's own stamp: the same key, now with its sheet; the next read (a nudge) brings it, and nothing doubles
    srv.st.put('Order_Timeline', k, { sheetId: 'sh1', sheet: 'GF Sheet 1', setId: 'GF_Set-1' });
    const t1 = Date.now(); await page.waitForFunction(k => { const f = OrderWin._feed(), e = f && f.answer && f.answer.events.find(x => x.id && x.id.endsWith(k)); return e && e.sheetId === 'sh1' && !e.pending; }, id, { timeout: 4000 });
    console.log(`      the server's own copy on the open view: ${Date.now() - t1} ms (no refresh asked for)`);
    await sleep(400);
    const answer = await page.evaluate(() => OrderWin._feed().answer.events.filter(e => e.type === 'engraveApproved').length);
    assert.equal(answer, 1, 'one engraveApproved in the answer'); assert.equal(await stamps(), 1, 'one stamp after the server\'s copy');
    const seen = await page.evaluate(() => window.__seen); assert.equal(seen.lost, 0, 'the Engraved step was never undrawn'); assert(seen.max <= 1, 'never two stamps: ' + seen.max);
  });

  // 2 · another computer's scan, with no refresh from here
  await check('a station scan written elsewhere shows on the open view within 3.3 s, with no refresh asked for', async () => {
    assert(!(await railDone('sorted')), 'not sorted yet');
    const body = JSON.stringify({ timeline: [{ orderId: A.rid, type: 'sorted', station: 'sorting', device: 'Sorting-2', by: 'Jo', id: 'sorted-dev2', at: Date.now() }] });
    await new Promise((res, rej) => { const r = http.request(`${srv.sorterOrigin}/.netlify/functions/firebaseOrders`, { method: 'POST', headers: { 'Content-Type': 'application/json' } }, x => { x.resume(); x.on('end', res); }); r.on('error', rej); r.end(body); });
    const t0 = Date.now(); while (!(await railDone('sorted')) && Date.now() - t0 < 8000) await sleep(25);
    const ms = Date.now() - t0; console.log(`      a scan written on another computer → the open view: ${ms} ms`);
    assert(ms < 3300, 'shown within 3.3 s: ' + ms + ' ms');
    assert.match(await page.evaluate(() => document.getElementById('owNowCard').textContent), /sort/i, 'the Overview card says it too');
  });

  // 3 · the cadence: one read at a time, every 2.5 s, none once closed
  await check('the open view reads every 2.5 s, one at a time, and not at all once it is closed', async () => {
    const t0 = Date.now(); await sleep(7600); const n = getsSince(t0);
    console.log(`      reads of the open view in 7.6 s: ${n}; most at once: ${ctl.maxInflight}`);
    assert(n >= 2 && n <= 4, 'about one read per 2.5 s: ' + n); assert.equal(ctl.maxInflight, 1, 'never two reads at once');
    await page.evaluate(() => OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open); await sleep(300);
    const t1 = Date.now(); await sleep(3200); assert.equal(getsSince(t1), 0, 'no read once the view is closed');
  });

  // 4 · a feed's failures, hidden tab
  await check('failed reads back off and keep what is drawn; a hidden tab reads nothing and the return reads at once', async () => {
    await page.evaluate(rid => { const f = window.__f = OrderTimelineUI.feed(rid, { pollMs: 200 }); window.__kinds = []; f.subscribe(k => window.__kinds.push(k)); return f.refresh({ force: true }); }, A.rid);
    assert(await page.evaluate(() => !!__f.answer), 'it has an answer');
    ctl.fail = true; const t0 = Date.now();
    await page.evaluate(() => __f.refresh({ force: true, quiet: true })); await sleep(900);
    let s = await page.evaluate(() => ({ kept: !!__f.answer, errors: __kinds.filter(k => k === 'error').length, fails: __f.fails }));
    assert(s.kept && s.errors === 0 && s.fails >= 1, 'the first failures are quiet and the answer stays: ' + JSON.stringify(s));
    await sleep(2300); const n = getsSince(t0);
    s = await page.evaluate(() => ({ kept: !!__f.answer, errors: __kinds.filter(k => k === 'error').length, fails: __f.fails }));
    console.log(`      failing for 3.2 s at a 200 ms poll: ${n} reads (16 without a back-off), ${JSON.stringify(s)}`);
    assert(n <= 5, 'the poll backs off: ' + n + ' reads'); assert(s.kept && s.errors >= 1 && s.fails >= 3, 'said after the third failure: ' + JSON.stringify(s));
    ctl.fail = false; await page.evaluate(() => window.dispatchEvent(new Event('online')));
    await page.waitForFunction(() => __f.fails === 0 && __kinds[__kinds.length - 1] === 'data', null, { timeout: 2000 });
    // hidden: nothing is read; seen again: read at once
    await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' }); document.dispatchEvent(new Event('visibilitychange')); });
    await sleep(300); const h0 = Date.now(); await sleep(1200); assert.equal(getsSince(h0), 0, 'a hidden tab reads nothing');
    const v0 = Date.now(); await page.evaluate(() => { delete document.visibilityState; document.dispatchEvent(new Event('visibilitychange')); });
    await sleep(300); assert(getsSince(v0) >= 1, 'seen again, it reads at once');
    await page.evaluate(() => __f.destroy());
  });
  assert.deepEqual(errors, [], 'no page errors');
  await browser.close(); srv.close();
  const bad = results.filter(x => x[0] !== 'ok').length;
  console.log(bad ? `${bad} failed` : `${results.length} checks passed`); process.exit(bad ? 1 : 0);
}
main().catch(e => { console.error(e); process.exit(1); });
