// A worker that fails while a sheet is finishing or searching must never leave the sheet stuck (adversarial check, 15 Oct 2026).
// The sheet's geometry check runs in its worker after the search. A worker that answered it with an error, or could not start
// (the network gone, its script not served), was dropped as a "late message": the sheet stayed "finishing" for good, and every
// other sheet queued behind it ("Queued - waiting for Gold"), so an Auto run waited for ever. Now the sheet is flagged with the
// reason ("Verification flagged"), the next sheet nests, and the same sheet nests again by hand. A check that never answers is
// given up the same way after verifyWaitS seconds. A search whose workers all fail ends the sheet "ready" (nothing placed), not
// "nesting", and the next Nest works.
// Real code in headless Chromium against the local fake site (bridge-server.cjs); every request that is not to the loopback is
// aborted. The Worker class is wrapped so that chosen messages are answered with a fault instead of reaching the real worker.
//   node tests/charm-nest/adv-nest-worker.cjs [playwright-core dir]   (CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), os = require('os'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const { build } = require('./fixture.cjs');

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return; }
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-wkfault-')), G = path.join(tmp, 'gold-charms.ai'), H = path.join(tmp, 'more-gold-charms.ai'), V = path.join(tmp, 'silver-charms.ai');
  await build(G, 3); await build(H, 3); await build(V, 3);   // (three charms a file: a file is one feed turn of the nest, FEED_ORDERS)
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'off', naming: 'off', autoNest: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator'; window.confirm = () => { window.__asked = (window.__asked || 0) + 1; return true; }; window.alert = () => { window.__asked = (window.__asked || 0) + 1; };
      // verify / solve: 'pass' (the real worker), 'error' (answered with an error message), 'crash' (the worker's error event, as a script that cannot start), 'silent' (never answered)
      window.__wk = { verify: 'pass', solve: 'pass', jobs: new Set() };
      const Real = window.Worker;
      window.Worker = class extends Real {
        postMessage(m, t) {
          if (m && m.type === 'solve') window.__wk.jobs.add(String(m.jobId).split(':')[0]);   // (one search a press: its workers share one job id before the colon)
          const mode = m && m.type === 'verify' ? window.__wk.verify : m && m.type === 'solve' ? window.__wk.solve : 'pass';
          if (mode === 'error') { setTimeout(() => this.dispatchEvent(new MessageEvent('message', { data: { type: 'error', jobId: m.jobId, message: 'Cannot read properties of undefined (injected)' } })), 30); return; }
          if (mode === 'crash') { setTimeout(() => this.dispatchEvent(new ErrorEvent('error', { message: 'Script error (injected: the worker could not start)' })), 30); return; }
          if (mode === 'silent') return;
          return super.postMessage(m, t);
        }
      };
    });
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S && window.Motion && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(() => Object.assign(CN.S.settings, { naming: 'off', review: 'off', autoNest: 'off' }));
    await page.evaluate(() => CN.setMode('nest'));
    const feed = async (f, n, metal) => {
      await page.setInputFiles('#fileInput', f);
      await page.waitForFunction(n => CN.S.sources.length === n && CN.S.sources.every(s => ['ready', 'error'].includes(s.state)), n, { timeout: 60000 });
      await page.evaluate(([i, m]) => document.querySelectorAll('#srcList .srcRow')[i].querySelector(`[data-assign="${m}"]`).click(), [n - 1, metal]);
      await page.waitForFunction(([i, m]) => { const ids = new Set(CN.S.sources[i].charms.map(c => c.id)); return CN.activePage(m).charms.filter(c => ids.has(c.id)).length === CN.S.sources[i].charms.length; }, [n - 1, metal], { timeout: 15000 });
    };
    // Nest on a card; settled() waits for that search (a new job id) to reach a resting state
    const nest = async (metal, presses = 1) => page.evaluate(([m, n]) => { const j = CN.activePage(m).jobId; for (let i = 0; i < n; i++) document.querySelector(`.sheetCard[data-m="${m}"] [data-r="nest"]`).click(); return j; }, [metal, presses]);
    const searches = () => page.evaluate(() => __wk.jobs.size);
    const settled = (metal, before, within) => page.waitForFunction(([m, j]) => { const sh = CN.activePage(m); return sh.jobId !== j && !sh.feedWait?.length && (['ready', 'error', 'idle'].includes(sh.status) || ['complete', 'partial'].includes(sh.status) && (sh.persistedDone || sh.problem)); }, [metal, before], { timeout: within, polling: 100 });
    const mode = (verify, solve) => page.evaluate(([v, s]) => { __wk.verify = v; __wk.solve = s; }, [verify, solve]);
    const stateOf = metal => page.evaluate(m => { const sh = CN.activePage(m); return { status: sh.status, ok: sh.verification ? !!sh.verification.ok : null, geomError: sh.verification && sh.verification.geom ? sh.verification.geom.error || null : null, placed: sh.placements.length, charms: sh.charms.length, ids: sh.placements.map(p => p.id), queue: CN.S.nestQueue.length }; }, metal);
    const results = [];
    const check = async (name, fn) => { try { await fn(); results.push(['ok', name]); console.log('  ok   ' + name); } catch (e) { results.push(['FAIL', name]); console.log('  FAIL ' + name + '\n       ' + String(e.message).split('\n').slice(0, 4).join(' | ')); } };

    await feed(G, 1, 'gold');
    // 0 · the tab is reloaded (or closed) in the middle of a search: the sheet comes back with its charms, once each, and nests again
    await check('a reload in the middle of a search leaves the sheet ready with every charm once, and it nests again', async () => {
      const j = await nest('gold');
      await page.waitForFunction(j => { const sh = CN.activePage('gold'); return sh.jobId !== j && sh.status === 'nesting'; }, j, { timeout: 30000, polling: 20 });
      await page.reload({ waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && CN.S && CN.S.cloud.ok === true && CN.activePage('gold').charms.length === 3, null, { timeout: 60000 });
      await page.evaluate(() => CN.setMode('nest'));
      const after = await stateOf('gold');
      assert.ok(['ready', 'complete', 'partial'].includes(after.status), 'not left "nesting" with no worker: ' + JSON.stringify(after));
      assert.equal(after.charms, 3); assert.equal(new Set(after.ids).size, after.ids.length, 'no charm twice');
      const k = await nest('gold');
      await settled('gold', k, 90000);
      const g = await stateOf('gold');
      assert.equal(g.status, 'complete', JSON.stringify(g)); assert.equal(g.ok, true); assert.equal(g.placed, 3); assert.equal(new Set(g.ids).size, 3);
    });
    await feed(H, 2, 'gold');
    // 1 · the geometry check's worker answers with an error: the sheet is flagged, not stuck
    await check('a geometry check that errors flags the sheet; it is not left finishing', async () => {
      await mode('error', 'pass'); const j = await nest('gold');
      await settled('gold', j, 90000);
      const g = await stateOf('gold');
      assert.equal(g.status, 'partial', 'the sheet ends partial: ' + JSON.stringify(g));
      assert.equal(g.ok, false, 'its verification is flagged');
      assert.match(g.geomError || '', /injected/, 'and says why');
      assert.equal(g.placed, g.charms, 'every charm stays placed (the layout is not thrown away)');
    });
    // 2 · another sheet is not held behind it; its searches all fail first (nothing placed, "ready", not "nesting"), then the next Nest completes it
    await feed(V, 3, 'silver');
    await check('searches that all fail end the sheet ready, not nesting, with nothing queued', async () => {
      await mode('pass', 'error'); const j = await nest('silver');
      await settled('silver', j, 60000);
      const v = await stateOf('silver');
      assert.equal(v.status, 'ready', JSON.stringify(v)); assert.equal(v.placed, 0); assert.equal(v.queue, 0, 'nothing waits in the queue');
    });
    await check('the next sheet nests and completes after a flagged one, and after failed searches', async () => {
      await mode('pass', 'pass'); const j = await nest('silver');
      await settled('silver', j, 90000);
      const v = await stateOf('silver');
      assert.equal(v.status, 'complete', JSON.stringify(v)); assert.equal(v.ok, true); assert.equal(v.placed, v.charms); assert.equal(new Set(v.ids).size, v.ids.length);
    });
    // 3 · the worker cannot start (an error event, not a message): the same flag, and Nest works again by hand
    await check('a geometry worker that cannot start flags the sheet the same way', async () => {
      await mode('crash', 'pass'); const j = await nest('gold');
      await settled('gold', j, 90000);
      const g = await stateOf('gold');
      assert.equal(g.status, 'partial', JSON.stringify(g)); assert.equal(g.ok, false); assert.match(g.geomError || '', /could not start/);
    });
    // 4 · a geometry check that never answers is given up after verifyWaitS
    await check('a geometry check that never answers is given up and the sheet is flagged', async () => {
      await page.evaluate(() => { CN.S.settings.verifyWaitS = 4; }); await mode('silent', 'pass'); const j = await nest('gold');
      await settled('gold', j, 90000);
      const g = await stateOf('gold');
      assert.equal(g.status, 'partial', JSON.stringify(g)); assert.equal(g.ok, false); assert.match(g.geomError || '', /no answer in 4 s/);
      await page.evaluate(() => { delete CN.S.settings.verifyWaitS; });
    });
    await check('a flagged sheet nests again by hand and is verified; a double press starts one search, not two', async () => {
      await mode('pass', 'pass'); const n0 = await searches(); const j = await nest('gold', 2);
      await settled('gold', j, 90000);
      const g = await stateOf('gold');
      assert.equal(g.status, 'complete', JSON.stringify(g)); assert.equal(g.ok, true); assert.equal(new Set(g.ids).size, g.ids.length, 'no charm twice on the sheet');
      assert.equal((await searches()) - n0, 1, 'two presses of Nest started one search');
    });
    assert.deepEqual(errors, [], 'no page errors');
    const bad = results.filter(r => r[0] !== 'ok');
    if (bad.length) { console.error(bad.length + ' check(s) failed'); process.exitCode = 1; } else console.log('  all ' + results.length + ' checks passed');
  } finally { await browser.close(); srv.close(); fs.rmSync(tmp, { recursive: true, force: true }); }
})().catch(e => { console.error(e); process.exit(1); });
