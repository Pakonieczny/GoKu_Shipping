// Send to Review: the model (REVMARK, Paul 12 Oct 2026: "mark an order for review and add it to the review tab"). TWO simulated computers, A and B, are
// two browser contexts on ONE in-memory backend (bridge-server.cjs: the real charmNestLibrary handler over a fake Firestore that refuses nested arrays);
// every request off the loopback is aborted, no Etsy call, no paid AI, no real name (the orders are "Name One" / "Name Two").
//   1 · CharmNestReviewMark.mark(rid): isMarked is true at once, the order is a row in A's Review tab under "Marked" (a slot for suggested charms, Mark OK),
//       ONE document Charm_Custom_Orders/{rid}_review (no stamps, no completion, no seal), a second mark and a double press write nothing more
//   2 · B (another computer) holds it through the Review tab's existing 2 s changes feed: no new op, no new listener; its row, its count
//   3 · Mark OK on B resolves it: A sees it leave Open for Completed (who, how), isMarked false on both, ONE more write; a reload of B still has it, resolved
//   4 · marked again after a resolve starts a new cycle (the earlier one is kept in review.earlier), a failed write puts the order back as it was
//   5 · the sandbox: a sandbox page writes Sandbox_Charm_Custom_Orders only and production's collection is untouched
//   node tests/charm-nest/review-mark.cjs [playwright-core dir]   (CHROMIUM=<chrome>)
const fs = require('fs'), os = require('os'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 18, 17) / 1000);
const order = (rid, i, sku, title, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY - i * 3600, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Name ' + (i === 1 ? 'One' : 'Two') }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], metals: {},
  lines: (lines || [[sku, title]]).map(([s, t], j) => ({ transactionId: String(rid) + (j + 1), listingId: String(1800000100 + i * 10 + j), sku: s, title: t, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal Choice', value: 'Gold Filled' }], metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' })) });
const ORDERS = [order(4100000001, 1, 'ZZ-MARK-01', 'Test Charm Necklace'), order(4100000002, 2, '', '', [['ZZ-MARK-02', 'Test Pair Earrings'], ['ZZ-MARK-03', 'Test Disc Charm']]), order(4100000003, 3, 'ZZ-MARK-01', 'Test Charm Necklace')];
const SKUS = ['ZZ-MARK-01', 'ZZ-MARK-02', 'ZZ-MARK-03'];
const R1 = '4100000001', R2 = '4100000002', R3 = '4100000003', CUSTOM = 'Charm_Custom_Orders';

const K = Math.min(4, Math.max(1, os.loadavg()[0] / os.cpus().length / 4));
const log = (...a) => console.log('  ·', ...a);
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || (fs.existsSync('/opt/pw-browsers/chromium-1194/chrome-linux/chrome') ? '/opt/pw-browsers/chromium-1194/chrome-linux/chrome' : '/opt/pw-browsers/chromium'), args: ['--no-sandbox'] });
  const errors = [], outside = [];
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
  async function client(name, o = {}) {
    const context = o.context || await browser.newContext({ viewport: { width: 1440, height: 900 } });
    if (!o.context) await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy|anthropic/i.test(u)) outside.push(u);
      return r.abort();
    });
    if (!o.context) await context.addInitScript(({ who, sandbox }) => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(Object.assign({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' }, sandbox ? { sandbox: 'on', pollOrders: 'off' } : {}))); localStorage.setItem('cn.employee', who); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} }, { who: name, sandbox: !!o.sandbox });
    const page = await context.newPage(), c = { name, context, page, ops: [] };
    page.setDefaultTimeout(30000 * K);
    page.on('pageerror', e => { errors.push(name + ': ' + e.message); console.error(name, 'page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('request', rq => { if (!/charmNestLibrary/.test(rq.url()) || rq.method() !== 'POST') return; let b = {}; try { b = JSON.parse(rq.postData() || '{}'); } catch (_) {} c.ops.push(b.op + (b.action ? ':' + b.action : '')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CharmNestReviewMark && CN.S.cloud.ok === true, null, { timeout: 90000 });
    c.load = async (mode = 'review') => page.evaluate(async ({ orders, skus, mode }) => {
      await Orders.loadMaps(true);
      for (const s of skus) B.master.entries.set(s, { sku: s, updatedAt: 1 });
      B.orders.rows = []; B.orders.byKey = new Map();
      const at = Date.now();
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: at, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode(mode); Review.render();
    }, { orders: ORDERS, skus: SKUS, mode });
    c.rows = () => page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(n => ({ rid: n.dataset.rid, mkey: n.dataset.mkey, kind: (n.querySelector('.purchaseLabel') || {}).textContent, slots: n.querySelectorAll('.rvMarkSlot').length, ok: !!n.querySelector('[data-rv-markok]'), why: (n.querySelector('.rowExcerpt') || {}).textContent, by: (n.querySelector('.by') || {}).textContent })));
    c.seg = async s => { await page.click(`#reviewView .rvSeg [data-cseg="${s}"]`); await page.waitForFunction(s => Review.view().cseg === s, s); };
    c.counts = () => page.evaluate(() => ({ open: +(document.querySelector('#reviewView .rvSeg [data-cseg="open"] b') || {}).textContent, done: +(document.querySelector('#reviewView .rvSeg [data-cseg="done"] b') || {}).textContent }));
    c.is = rid => page.evaluate(rid => CharmNestReviewMark.isMarked(rid), rid);
    return c;
  }
  const waitFor = async (c, fn, arg, what, ms = 9000) => { ms = Math.round(ms * K); try { await c.page.waitForFunction(fn, arg, { timeout: ms }); } catch (e) { throw new Error(`${c.name}: ${what} did not happen in ${ms} ms`); } };
  const rec = rid => srv.st.doc(CUSTOM, rid + '_review');
  const writes = () => srv.st.calls.filter(c => c.name === 'charmNestLibrary' && c.body && c.body.op === 'reviewMarkPut');

  try {
    const A = await client('Name Alpha'), Bc = await client('Name Beta');
    await A.load(); await Bc.load();
    assert.equal(await A.is(R1), false); assert.deepEqual((await A.rows()).filter(r => r.kind === 'Marked'), [], 'nothing is marked at the start');

    /* ── 1 · mark ── */
    const t0 = Date.now(), opsBefore = Bc.ops.length;
    const r1 = await A.page.evaluate(rid => CharmNestReviewMark.mark(rid), R1);
    assert.deepEqual(r1, { ok: true, already: false, rid: R1 }); log('mark answered in', Date.now() - t0, 'ms');
    assert.equal(await A.is(R1), true, 'marked at once');
    assert.deepEqual(await A.page.evaluate(() => CharmNestReviewMark.markedRids()), [R1]);
    assert.equal(await A.page.evaluate(rid => CharmNestReviewMark.reasonOf(rid), R1), 'Sent to Review by Name Alpha');
    const d = rec(R1);
    assert.ok(d, 'one document in the custom-order records'); assert.equal(d.review.state, 'open'); assert.equal(d.review.markedBy, 'Name Alpha'); assert.equal(d.kind, 'reviewMark'); assert.equal(d.state, 'open');
    assert.ok(!d.stamps && !d.completedAt && !d.printedAt && !d.history, 'no seal, no completion, no print, no history');
    assert.equal(writes().length, 1, 'ONE write');
    let rows = await A.rows(), mine = rows.find(r => r.rid === R1 && r.kind === 'Marked');
    assert.ok(mine, 'the order is a row in Review under Marked: ' + JSON.stringify(rows.map(r => [r.rid, r.kind])));
    assert.equal(mine.slots, 1, 'one slot for the one line'); assert.equal(mine.ok, true, 'Mark OK on the row'); assert.match(mine.why, /^Sent to Review by Name A/);
    // pressing again, and two presses together: nothing more is written
    assert.deepEqual(await A.page.evaluate(rid => CharmNestReviewMark.mark(rid), R1), { ok: true, already: true, rid: R1 });
    const both = await A.page.evaluate(rid => Promise.all([CharmNestReviewMark.mark(rid), CharmNestReviewMark.mark(rid)]), R2);
    assert.ok(both[0].ok && both[1].ok, 'both answer ok'); assert.equal(writes().length, 2, 'a double press on a new order is one write (one for R1, one for R2)');
    if (process.env.SHOT) { await A.page.click('#reviewView .ordBar [data-k="reviewMark"]'); await A.page.waitForTimeout(800); await A.page.screenshot({ path: process.env.SHOT }); await A.page.click('#reviewView .ordBar [data-k="reviewMark"]'); }
    rows = await A.rows(); assert.equal(rows.find(r => r.rid === R2 && r.kind === 'Marked').slots, 2, 'a two-line order has a slot for each line');
    // the order's timeline gets one plain line (a note), never a seal
    const tlEvents = () => srv.st.calls.filter(c => c.name === 'charmNestLibrary' && c.body && c.body.op === 'timelineAdd').flatMap(c => Array.isArray(c.body.events) ? c.body.events : []);
    for (let i = 0; i < 60 && !tlEvents().some(e => /Sent to Review by Name Alpha/.test(e.text || '')); i++) await sleep(100);
    const line = tlEvents().find(e => /Sent to Review by Name Alpha/.test(e.text || ''));
    assert.ok(line, 'a plain line on the order\'s timeline'); assert.equal(line.type, 'note'); assert.equal(String(line.orderId), R1);
    assert.ok(!tlEvents().some(e => /^seal/.test(e.type || '')), 'no seal on the timeline');
    log('marked: one document, no seal, a row with a slot per line and Mark OK; a repeated or double press writes nothing more');

    /* ── 2 · the other computer holds it through the existing feed ── */
    const tB = Date.now();
    await waitFor(Bc, rid => CharmNestReviewMark.isMarked(rid), R1, 'B holding the mark');
    log('B holds the mark', Date.now() - tB, 'ms after A wrote it; ops B made meanwhile:', [...new Set(Bc.ops.slice(opsBefore))].join(','));
    assert.ok(Bc.ops.slice(opsBefore).includes('customGet') && !Bc.ops.slice(opsBefore).some(o => /reviewMark/.test(o)), 'B learned of it from the existing changes feed (customGet); it made no call of its own for it');
    await waitFor(Bc, () => [...document.querySelectorAll('#rvList .reviewListRow')].some(n => n.dataset.rid === '4100000001' && /Marked/.test(n.textContent)), null, 'B drawing the row');
    assert.deepEqual((await Bc.counts()).open >= 2, true);

    /* ── 3 · Mark OK on B ── */
    await Bc.page.click(`#rvList .reviewListRow[data-rid="${R1}"] [data-rv-markok]`);
    await waitFor(Bc, rid => !CharmNestReviewMark.isMarked(rid), R1, 'B resolving');
    assert.equal(rec(R1).review.state, 'done'); assert.equal(rec(R1).review.doneBy, 'Name Beta'); assert.equal(rec(R1).review.how, 'ok'); assert.equal(writes().length, 3);
    await waitFor(A, rid => !CharmNestReviewMark.isMarked(rid), R1, 'A learning of it');
    assert.equal((await A.rows()).some(r => r.rid === R1 && r.kind === 'Marked'), false, 'gone from A\'s Open list');
    await A.seg('done'); rows = await A.rows(); const dn = rows.find(r => r.rid === R1);
    assert.ok(dn, 'in Completed on A'); assert.match(dn.by || '', /Name Beta/); await A.seg('open');
    log('resolved on B: A has it in Completed (by Name Beta)');
    // a reload of B: the cloud's record still says resolved, the order is not back in Review
    await Bc.page.reload({ waitUntil: 'load' });
    await Bc.page.waitForFunction(() => window.CN && window.CharmNestReviewMark && CN.S.cloud.ok === true, null, { timeout: 90000 });
    await Bc.load();
    assert.equal(await Bc.is(R1), false); assert.equal((await Bc.page.evaluate(rid => CharmNestReviewMark.info(rid), R1)).state, 'done');
    assert.equal((await Bc.page.evaluate(() => CharmNestReviewMark.markedRids())).includes(R2), true, 'R2 is still marked after a reload (it comes with the maps\' list)');
    await Bc.seg('done'); assert.ok((await Bc.rows()).some(r => r.rid === R1), 'Completed after a reload keeps it'); await Bc.seg('open');

    /* ── 4 · again, and a failed write ── */
    assert.equal((await A.page.evaluate(rid => CharmNestReviewMark.mark(rid), R1)).ok, true);
    assert.equal(rec(R1).review.state, 'open'); assert.equal(rec(R1).review.earlier.length, 1, 'the earlier cycle is kept'); assert.equal(rec(R1).review.earlier[0].doneBy, 'Name Beta');
    srv.st.fail.charmNestLibrary = true;
    const bad = await A.page.evaluate(rid => CharmNestReviewMark.mark(rid), R3);
    srv.st.fail.charmNestLibrary = false;
    assert.equal(bad.ok, false); assert.equal(await A.is(R3), false, 'a failed write puts the order back'); assert.ok(!rec(R3));
    assert.equal((await A.rows()).some(r => r.rid === R3 && r.kind === 'Marked'), false);
    const unknown = await A.page.evaluate(() => CharmNestReviewMark.mark('4999999999'));
    assert.equal(unknown.ok, false, 'an order that is not in the list is not marked');
    log('marked again after a resolve: a new cycle, the earlier kept; a failed write: back as it was, no row, nothing in the cloud');

    /* ── 5 · the sandbox ── */
    const prodBefore = JSON.stringify(srv.st.list(CUSTOM));
    const S = await client('Name Sigma', { sandbox: true });
    await S.load();
    assert.equal(await S.page.evaluate(() => WORKSPACE_SANDBOX), true);
    assert.equal((await S.page.evaluate(rid => CharmNestReviewMark.mark(rid), R3)).ok, true);
    assert.ok(srv.st.doc('Sandbox_' + CUSTOM, R3 + '_review'), 'the sandbox keeps its own copy'); assert.ok(!rec(R3), 'production has none');
    assert.equal(JSON.stringify(srv.st.list(CUSTOM)), prodBefore, 'production records untouched by the sandbox page');
    assert.equal((await S.rows()).some(r => r.rid === R3 && r.kind === 'Marked'), true, 'a sandbox order is in the sandbox Review tab');
    log('sandbox: Sandbox_Charm_Custom_Orders only; production unchanged');

    assert.deepEqual(errors, [], 'no page error'); assert.deepEqual(outside, [], 'no Etsy / AI call');
    console.log('PASS review-mark');
  } finally { await browser.close(); srv.close && srv.close(); }
})().catch(e => { console.error('FAIL review-mark:', e && e.stack || e); process.exit(1); });
