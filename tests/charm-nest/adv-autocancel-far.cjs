// Adversarial (task G): a cancelled order on a saved sheet this sorter has not loaded (an earlier set's, or one only in
// the Library), and a sandbox reset.
//   1. Its piece records name the sheet (poolList by order, no Etsy call): the run pill says "Order N is cancelled: take
//      its pieces off GF Sheet 3 before cutting.", its cancel record's fates say "open", nothing on that sheet is
//      changed, and it is looked at once.
//   2. A cancelled order found on no sheet at all: nothing is said, and it is looked at once.
//   3. A sandbox reset clears the sandbox's AutoCancel notices and never the production ones.
//   node tests/charm-nest/adv-autocancel-far.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

const F = '4400000001', H = '4400000002';
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', CANCELLED = 'Charm_Nest_Cancelled';

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  // an earlier set's Gold sheet 3, saved and not cut, never loaded by this sorter; order F has two pieces on it
  const pf = c => `${F}_5000000001_${c}`;
  st.put(SHEETS, 'gold-old-3', { id: 'gold-old-3', metal: 'gold', fileBase: 'GF_2026-09-20_Sheet-3', charms: [1, 2].map(c => ({ id: 'g' + c, poolId: pf(c), order: F })), placements: [1, 2].map(c => ({ id: 'g' + c, cxPt: 40 * c, cyPt: 40, angle: 0 })), poolIds: [pf(1), pf(2)] }, false);
  for (const c of [1, 2]) st.put(POOL, pf(c), { poolId: pf(c), orderId: F, sheetId: 'gold-old-3', sheetName: 'GF_2026-09-20_Sheet-3', state: 'placed', material: 'gold' });
  const cancel = (rid, by, extra) => st.put(CANCELLED, rid, Object.assign({ orderId: rid, by, why: '', at: Date.now(), sheets: [], lines: [] }, extra), false);

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    return r.abort();
  });
  await ctx.addInitScript(() => {
    try {
      if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester');
      const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; localStorage.setItem('cn.settings', JSON.stringify(s));
    } catch (_) { /* about:blank */ }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
  });
  const page = await ctx.newPage(), errors = [], ok = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const settle = () => page.evaluate(async () => { const due = await AutoCancel.poll(); await AutoCancel.idle(); return due; });

  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.AutoCancel && AutoCancel.started(), null, { timeout: 60000 });

    /* 1 · on a saved sheet not loaded here */
    const atF = Date.now(); cancel(F, 'Etsy', { source: 'etsy', at: atF });
    const due1 = await settle();
    assert.deepStrictEqual(due1, [F], 'the order is looked at though this sorter holds none of it: ' + JSON.stringify(due1));
    const TEXT = `Order ${F} is cancelled: take its pieces off GF Sheet 3 before cutting.`;
    await page.waitForFunction(() => !document.getElementById('runBanner').classList.contains('hidden') && document.querySelector('#runBanner .rbCxItem'), null, { timeout: 5000 });
    const item = await page.evaluate(() => document.querySelector('#runBanner .rbCxItem span').textContent);
    assert.strictEqual(item, TEXT, 'the run pill says to take them off before cutting: ' + item);
    assert.deepStrictEqual(st.doc(CANCELLED, F).fates, [{ sheet: 'GF Sheet 3', fate: 'open', text: 'on GF Sheet 3, not cut yet: take its pieces off before cutting' }], 'its record says "open": ' + JSON.stringify(st.doc(CANCELLED, F).fates));
    assert.strictEqual(st.doc(SHEETS, 'gold-old-3').charms.length, 2, 'the sheet itself is not changed from here');
    for (const c of [1, 2]) assert.strictEqual(st.doc(POOL, pf(c)).state, 'placed', 'nor its piece records');
    assert.deepStrictEqual(await settle(), [], 'looked at once');
    ok.push('a cancelled order on a saved sheet not loaded here: the run pill says to take its pieces off GF Sheet 3 before cutting, the record says "open", nothing else changes, once');

    /* 2 · on no sheet at all */
    cancel(H, 'Anna');
    assert.deepStrictEqual(await settle(), [H], 'looked at');
    const s2 = await page.evaluate(H => ({ n: AutoCancel.notices().filter(n => n.rid === H).length, done: !!AutoCancel.state().done[H], jobs: Object.keys(AutoCancel.state().jobs) }), H);
    assert(!s2.n && s2.done && !s2.jobs.length, 'nothing said, done, no job left: ' + JSON.stringify(s2));
    assert(!(st.doc(CANCELLED, H).fates || []).length, 'and no fate written');
    assert.deepStrictEqual(await settle(), [], 'and not looked at again');
    ok.push('a cancelled order on no sheet: nothing is said, looked at once');

    /* 3 · a sandbox reset clears the sandbox's notices only */
    const s3 = await page.evaluate(async () => {
      localStorage.setItem('cn.autoCancel.v1:sandbox', JSON.stringify({ known: ['1'], done: {}, jobs: {}, notices: [{ rid: '4499999999', at: 1, text: 'Order 4499999999 was cancelled. Its pieces are already cut on SS Sheet 9: set them aside.', where: 'SS Sheet 9', t: 1 }] }));
      const real = window.api; window.api = (fn, b, o) => b && b.op === 'sandboxReset' ? Promise.resolve({ deleted: 0, files: 0 }) : real(fn, b, o);
      try { await Sandbox.reset(); } finally { window.api = real; }
      return { sandbox: localStorage.getItem('cn.autoCancel.v1:sandbox'), prod: AutoCancel.notices().map(n => n.rid) };
    });
    assert.strictEqual(s3.sandbox, null, 'the sandbox notices are gone: ' + s3.sandbox);
    assert.deepStrictEqual(s3.prod, [F], 'the production notice stays: ' + JSON.stringify(s3.prod));
    ok.push('a sandbox reset clears the sandbox AutoCancel notices, never production\'s');

    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log('adv-autocancel-far: all passed\n  ' + ok.join('\n  '));
  } catch (e) {
    console.error('errors:', errors);
    throw e;
  } finally {
    await browser.close(); srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
