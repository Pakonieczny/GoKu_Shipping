// ADV-MONKEY regressions (Paul, 15 Oct 2026: "resilient against … interruptions and unexpected user behaviour"). Three things a clumsy hand did that the sorter did not survive
// (found by adv-monkey.cjs), each pinned here on the real sorter page in headless Chromium over the fake site; no Etsy, no live record:
//   1  an address that names no tab (a typed #bogus, an old bookmark, Back into one) changes nothing. It used to hide every view (popstate called setMode("bogus")): a blank screen
//      with no tab lit; and a "\" in the address threw in the selector at start-up.
//   2  Pull orders pressed again while the pull is still reading the shop (it takes tens of seconds) asks the station once. It used to ask for the same orders again for every press.
//   3  a wrong file dropped on the nest rail stays there as a red error row and the workspace checkpoint keeps it with its bytes; reading it again at the next start threw, so the
//      WHOLE workspace stayed unrestored at every reload ("Workspace recovery failed; checkpoint retained") and the page ran without its arrivals, upkeep and saving. Now an error
//      row is not read again, one that cannot be read now becomes an error row, and the workspace comes back and the page starts.
//   node tests/charm-nest/adv-monkey-fixes.cjs [playwright-core dir]
const assert = require('assert/strict');
const Base = require('./_orderFactsWorld.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const TABS = ['nest', 'orders', 'engrave', 'review', 'library', 'design', 'master'];
const views = page => page.evaluate(() => ({ mode: CN.S.mode, shown: ['sheets', 'ordersView', 'designView', 'masterView', 'efficiencyView', 'engraveView', 'reviewView', 'libView', 'charmsView'].filter(id => !document.getElementById(id).classList.contains('hidden')), lit: [...document.querySelectorAll('.topbar button[data-mode].on')].map(b => b.dataset.mode) }));
const started = page => page.waitForFunction(() => window.CN && window.Orders && CN.S.cloud.ok === true, null, { timeout: 60000 });

(async () => {
  const w = await Base.world({ viewport: { width: 1280, height: 900 } });
  if (!w) { console.log('  – no playwright-core: not run'); return; }
  const { page } = w, failed = [], red = [];
  const section = async (name, fn) => { try { await fn(); } catch (e) { red.push(name); console.log(`  ✗ ${name}: ${String(e.message).split('\n')[0].slice(0, 400)}`); } };
  page.on('console', m => { if (m.type() === 'error' && /Workspace recovery failed/.test(m.text())) failed.push(m.text().slice(0, 160)); });
  try {
    /* 1 · the address */
    await section('1 the address', async () => {
    await page.evaluate(() => CN.setMode('orders')); await sleep(300);
    for (const h of ['bogus', '__proto__', 'constructor', 'efficiency/x/y', '%00', 'a\\', 'ORDERS', 'library/none', 'x"]y']) {
      await page.evaluate(h => { location.hash = h; }, h); await sleep(300);
      const v = await views(page);
      assert(TABS.includes(v.mode) && v.shown.length === 1 && v.lit.length === 1, `#${h} leaves one tab shown and lit: ${JSON.stringify(v)}`);
      await page.evaluate(() => history.back()); await sleep(200); await page.evaluate(() => history.forward()); await sleep(200);
      const v2 = await views(page);
      assert(TABS.includes(v2.mode) && v2.shown.length === 1, `Back and Forward over #${h} leave one tab shown: ${JSON.stringify(v2)}`);
    }
    await page.evaluate(() => { CN.setMode('library'); }); await sleep(300);
    await page.evaluate(() => { location.hash = 'orders'; }); await sleep(400);
    assert.equal((await views(page)).mode, 'orders', '#orders still opens the Orders tab');
    for (const h of ['a\\', 'bogus', '__proto__']) {
      await page.goto('about:blank'); await page.goto(`${w.srv.sorterOrigin}/charm-nest-1.html#${h}`, { waitUntil: 'load' }); await started(page);   // (from a blank page: the same page with another # would only change the address)
      const v = await views(page);
      assert(TABS.includes(v.mode) && v.shown.length === 1 && v.lit.length === 1, `a fresh load at #${h} starts on a tab: ${JSON.stringify(v)}`);
    }
    console.log('  ✓ 1 an address that names no tab changes nothing: one tab shown and lit through Back and Forward; a fresh load starts on a tab');
    });

    /* 2 · Pull orders pressed three times while the pull runs */
    await section('2 Pull orders', async () => {
    await page.evaluate(() => {
      CN.setMode('orders'); window.__snap = 0;
      DesignLink.ensure = async () => ({}); DesignLink.etsyBudgetOk = () => true; DesignLink.meter = () => {};
      DesignLink.call = (type) => type === 'orders.snapshot' ? (window.__snap++, new Promise(res => setTimeout(() => res({ total: 0, hydrated: 0, etsy: {}, orders: [] }), 1200))) : Promise.resolve({});
    });
    await sleep(500);
    await page.click('#ordPull', { clickCount: 3, delay: 10 });
    await sleep(300);
    assert.equal(await page.evaluate(() => document.getElementById('ordPull').getAttribute('aria-busy')), 'true', 'the button says the pull is running');
    await sleep(2500);
    assert.equal(await page.evaluate(() => window.__snap), 1, 'three presses while the pull runs asked the station once');
    assert.equal(await page.evaluate(() => document.getElementById('ordPull').getAttribute('aria-busy')), null, 'the button is free again when the pull ends');
    await page.click('#ordPull'); await sleep(1800);
    assert.equal(await page.evaluate(() => window.__snap), 2, 'a press after the pull ended pulls again');
    console.log('  ✓ 2 Pull orders pressed three times asks the station once; pressed again afterwards it pulls again');
    });

    /* 3 · a wrong file on the rail does not stop the workspace coming back */
    await section('3 the workspace', async () => {
    await page.goto(`${w.srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' }); await started(page);
    await page.setInputFiles('#fileInput', [{ name: 'notes.pdf', mimeType: 'application/pdf', buffer: Buffer.from('this is not a pdf at all') }]);
    await page.waitForFunction(() => CN.S.sources.some(s => s.name === 'notes.pdf' && s.state === 'error'), null, { timeout: 20000 });
    await page.evaluate(() => CN.S.sources.push({ id: 'monkey-ready', runId: 'monkey', name: 'was-fine.ai', zipName: null, folder: '', bytes: new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9]), metal: null, state: 'ready', charms: [], error: null, hash: 'h', cloud: null, distinct: 0, orders: 0 }));
    await page.evaluate(async () => { Session.schedule(); await Session.flushNow(); });
    await page.reload({ waitUntil: 'load' }); await started(page);
    for (const t = Date.now(); Date.now() - t < 30000;) { if (failed.length || await page.evaluate(() => /Workspace restored/.test((document.getElementById('toasts') || {}).innerText || ''))) break; await sleep(200); }
    await sleep(500);
    assert.deepEqual(failed, [], 'the workspace came back: ' + failed.join(' | '));
    const after = await page.evaluate(() => ({ ready: Session.ready(), sources: CN.S.sources.map(s => [s.name, s.state, String(s.error || '').slice(0, 90)]), toasts: (document.getElementById('toasts') || {}).innerText || '' }));
    assert(after.ready, 'the page started (saving is on): ' + JSON.stringify(after));
    assert(/Workspace restored/.test(after.toasts), 'the workspace was restored: ' + after.toasts);
    const byName = Object.fromEntries(after.sources.map(s => [s[0], s]));
    assert.equal(byName['notes.pdf'] && byName['notes.pdf'][1], 'error', 'the wrong file is still an error row: ' + JSON.stringify(after.sources));
    assert(byName['was-fine.ai'] && byName['was-fine.ai'][1] === 'error' && /could not be read again/.test(byName['was-fine.ai'][2]), 'a file that cannot be read now is an error row that says so: ' + JSON.stringify(after.sources));
    console.log('  ✓ 3 a wrong file on the rail no longer stops the workspace coming back: restored, saving on, the file an error row');
    });
    assert.deepEqual(w.errors, [], 'no page error: ' + w.errors.join(' | '));
    if (red.length) process.exitCode = 1;
  } finally { await w.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
