// KEEPPLACED / KEEPAPPROVED (Paul, 18 Nov 2026, order 4225100553, Michelle's computer: "the only thing missing is the approved engraving of the emoji heart"). A computer that opened the order with no charm wrote its
// engraving job with NO place and back in Review over the approved placement in the cloud, so every computer that read it afterwards lost the heart.
//   1  server: a job sent with no place, for the same words as the stored job that has one, keeps the stored place and the stored approval
//   2  server: a job whose words changed, or whose place is sent, is written as sent (a person's edit is never held back)
//   3  page: a computer holding an APPROVED placement is handed the degraded cloud record (no place, Review): it keeps its own, and sends it again, so the cloud holds the place and the approval once more
// Real handler and page over the repo's fake site.   PW_DIR=... CHROMIUM=... node tests/charm-nest/engrave-sync-keep-place.cjs
const path = require('path'), assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
const KEY = '4225100553_5283317236', PLACE = { centre: [3.1, 4.2], angle: 0, size: 6.95, lines: ['❤️'], weight: 'Regular', lineGap: 0.216, fittedMax: 7, fontKey: 'source-sans-3' };
(async () => {
  const srv = await start({ receipts: [] }); const { st } = srv;
  const call = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return { status: r.status, ...(await r.json()) }; };
  const put = (job, dev) => call({ op: 'engraveSyncPut', by: 'Ana_M', dev: dev || 'dtest00001', lines: [{ key: KEY, sig: 's' + Math.random(), rec: { v: 1, override: null, engrave: { needed: true, approved: true }, raised: true, jobs: [Object.assign({ key: KEY, pre: true, text: '❤️', lines: ['❤️'] }, job)] } }] });
  const get = async () => { const r = await call({ op: 'engraveSyncGet', orders: ['4225100553'] }); return r.lines[0].rec.jobs[0]; };
  let fails = 0; const ok = (c, m) => { if (!c) fails++; console.log((c ? '  ✓ ' : '  ✗ ') + m); };
  try {
    await put({ state: 'approved', approvedAt: 1794584634942, approvedBy: 'Ana_M', place: PLACE });
    let j = await get(); ok(j.state === 'approved' && !!j.place, '0 the approved, placed job is stored');
    await put({ state: 'review' }, 'dtest00002');   // (the computer with no charm: no place, Review)
    j = await get();
    ok(!!j.place && j.place.centre[0] === 3.1, '1a the placeless job did not take the stored place away');
    ok(j.state === 'approved' && j.approvedAt === 1794584634942 && j.approvedBy === 'Ana_M', '1b nor the approval: ' + JSON.stringify({ state: j.state, by: j.approvedBy }));
    await put({ state: 'review', text: 'Ana', lines: ['Ana'] }, 'dtest00002');
    j = await get(); ok(j.text === 'Ana' && !j.place && j.state === 'review', '2a other words are written as sent (a person\'s edit): ' + JSON.stringify({ text: j.text, place: !!j.place, state: j.state }));
    await put({ state: 'review', text: 'Ana', lines: ['Ana'], place: Object.assign({}, PLACE, { centre: [9, 9] }) }, 'dtest00003');
    j = await get(); ok(j.place && j.place.centre[0] === 9, '2b a place that is sent is written as sent');
  } catch (e) { fails++; console.log('  ✗ server part: ' + (e && e.stack || e)); }

  // 3 · the page
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(__dirname, '../../node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  (no playwright-core: the page part was not run)'); srv.close && srv.close(); process.exit(fails ? 1 : 0); }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 720 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', JSON.stringify({ name: 'Paul_K' })); } catch (_) {} });
    const page = await context.newPage(), errors = []; page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Engrave && window.CharmNestEngraveSync && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async KEY => {
      const [r, t] = KEY.split('_'), order = { receiptId: r, orderNumber: r, createTs: 1, updateTs: 2, shipBy: 3, buyer: { name: 'N' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [{ transactionId: t, listingId: '4458930445', sku: 'MINI_0445', title: 'Mini', quantity: 1, expectedShipDate: 3, metalKey: 'gold', metalLabel: 'GF 14/20', variations: [], personalization: ['❤️'] }] };
      const row = { key: KEY, order, line: order.lines[0], arrivedAt: Date.now(), spec: null, problems: [], state: 'held', hold: 'on hold', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(KEY, row); Orders.interpretAll(); row.spec.engraveCandidate = true;
      const j = Engrave.ensureJob(row);
      Object.assign(j, { state: 'approved', approvedAt: 1794584634942, approvedBy: 'Ana_M', text: '❤️', lines: ['❤️'], lineInput: ['❤️'], copies: [], pre: true, fit: { size: 6.95, centre: [3.1, 4.2], angle: 0, weight: 'Regular', lines: ['❤️'], fittedMax: 7 }, view: {} });
      row.engrave = { needed: true, state: 'none', approved: true };
    }, KEY);
    // the cloud holds the degraded record (as Michelle's computer left it): no place, Review
    await put({ state: 'review' }, 'dtest00002');
    srv.st.calls.length = 0;
    await page.evaluate(async KEY => { const l = (await CN.api('charmNestLibrary', { op: 'engraveSyncGet', orders: ['4225100553'] })).lines[0]; await CharmNestEngraveSync._take(l, 'push'); }, KEY);
    await page.waitForTimeout(2500);
    const local = await page.evaluate(KEY => { const j = Engrave.jobOf(B.orders.byKey.get(KEY)); return { state: j.state, approvedAt: j.approvedAt, fit: !!j.fit }; }, KEY);
    ok(local.state === 'approved' && local.approvedAt === 1794584634942 && local.fit, '3a this computer kept its approved placement: ' + JSON.stringify(local));
    const sent = srv.st.calls.filter(c => c.body && c.body.op === 'engraveSyncPut').length;
    ok(sent >= 1, '3b and sent it again: ' + sent + ' write(s)');
    const j2 = await get();
    ok(j2.state === 'approved' && !!j2.place && Math.abs(j2.place.centre[0] - 3.1) < 0.01, '3c the cloud holds the approved placement once more: ' + JSON.stringify({ state: j2.state, place: !!j2.place }));
    ok(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 200));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails) { console.log('engrave-sync-keep-place: ' + fails + ' failed'); process.exit(1); }
  console.log('engrave-sync-keep-place: ok');
})().catch(e => { console.error(e); process.exit(1); });
