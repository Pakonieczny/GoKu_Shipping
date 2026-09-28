// A file removed on the Nest tab while its Gold sheet is nesting (adversarial check, 28 Sep): the search that was running
// still carried the file's charms, went on placing them and the sheet went on drawing them. Now the search in flight
// ends and the sheet nests again with what is left: none of the removed file's charms is placed or drawn from the
// click on, every charm of the other file stays and is nested, nothing waits on the person and no message pops up.
// Real code in headless Chromium against the local fake site (bridge-server.cjs); every request that is not to the
// loopback is aborted.
//   node tests/charm-nest/adv-remove-nesting.cjs [playwright-core dir]   (CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), os = require('os'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const { build } = require('./fixture.cjs');

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-rmnest-')), A = path.join(tmp, 'first-charms.ai'), B = path.join(tmp, 'second-charms.ai');
  await build(A, 5); await build(B, 5);
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'off', naming: 'off', autoNest: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; window.confirm = () => { window.__asked = (window.__asked || 0) + 1; return true; }; window.alert = () => { window.__asked = (window.__asked || 0) + 1; }; });
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S && window.Motion && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(() => Object.assign(CN.S.settings, { naming: 'off', review: 'off', autoNest: 'off' }));
    // the first file, then the second: the first one's charms are the older, so the first few placed are its own
    for (const [f, n] of [[A, 1], [B, 2]]) {
      await page.setInputFiles('#fileInput', f);
      await page.waitForFunction(n => CN.S.sources.length === n && CN.S.sources.every(s => ['ready', 'error'].includes(s.state)), n, { timeout: 60000 });
      await page.waitForTimeout(20);
    }
    await page.evaluate(() => CN.setMode('nest'));
    for (const i of [0, 1]) {
      await page.evaluate(i => document.querySelectorAll('#srcList .srcRow')[i].querySelector('[data-assign="gold"]').click(), i);
      await page.waitForFunction(i => { const ids = new Set(CN.S.sources[i].charms.map(c => c.id)); return CN.activePage('gold').charms.filter(c => ids.has(c.id)).length === CN.S.sources[i].charms.length; }, i, { timeout: 15000 });
    }
    const ids = await page.evaluate(() => ({ a: CN.S.sources[0].id, b: CN.S.sources[1].id, aN: CN.S.sources[0].charms.length, bN: CN.S.sources[1].charms.length, bIds: CN.S.sources[1].charms.map(c => c.id) }));
    assert(ids.aN >= 4 && ids.bN >= 4, 'both files read: ' + JSON.stringify(ids));
    // Nest, and the moment the search has one of the first file's charms on the sheet, that file's ×
    await page.evaluate(() => document.querySelector('.sheetCard[data-m="gold"] [data-r="nest"]').click());
    await page.waitForFunction(a => { const sh = CN.activePage('gold'), of = id => CN.S.sources[0].charms.some(c => c.id === id && c.sourceId === a); return sh.status === 'nesting' && (sh.probePlaced || []).concat(sh.probe ? [sh.probe] : [], sh.placements).some(p => of(p.id)); }, ids.a, { timeout: 60000, polling: 10 });
    const r = await page.evaluate(async a => {
      const sh = CN.activePage('gold'), gone = new Set(CN.S.sources[0].charms.map(c => c.id)), drawn = [], seen = [], toasts = [];
      const draw = CharmNestPDF.drawCharm; CharmNestPDF.drawCharm = function (ctx, c) { if (c && gone.has(c.id)) drawn.push(c.id); return draw.apply(this, arguments); };
      new MutationObserver(l => l.forEach(m => m.addedNodes.forEach(n => toasts.push(n.textContent)))).observe(document.querySelector('#toasts'), { childList: true, subtree: true });
      const look = where => { const s = CN.activePage('gold'), all = [['placements', s.placements], ['probePlaced', s.probePlaced], ['probe', s.probe ? [s.probe] : []], ['best', s.best?.placements], ['live', s.livePlacements]];
        for (const [k, list] of all) for (const p of list || []) if (gone.has(p.id)) seen.push(`${where}:${k}`); };
      const was = { status: sh.status, jobId: sh.jobId };
      const t0 = performance.now(); document.querySelector('#srcList .srcRow .srcX').click(); const clickMs = performance.now() - t0;
      look('click');
      const after = { sources: CN.S.sources.map(s => s.id), status: sh.status, jobChanged: sh.jobId !== was.jobId, charms: sh.charms.length };
      await new Promise(res => { const end = performance.now() + 3000; const f = () => { look('frame'); performance.now() < end ? requestAnimationFrame(f) : res(); }; requestAnimationFrame(f); });
      CharmNestPDF.drawCharm = draw;
      return { was, after, clickMs, drawn: [...new Set(drawn)].length, seen: [...new Set(seen)], toasts, asked: window.__asked || 0 };
    }, ids.a);
    console.log('  after the ×:', JSON.stringify(r));
    assert.deepEqual(r.after.sources, [ids.b], 'the file is removed at once (not refused while its sheet nests)');
    assert.equal(r.after.charms, ids.bN, "the sheet keeps every charm of the other file, and none of the removed one's");
    assert.deepEqual(r.seen, [], "none of the removed file's charms is placed, tried or held by the search from the click on");
    assert.equal(r.drawn, 0, "none of the removed file's charms is drawn from the click on");
    assert.deepEqual(r.toasts, [], 'no message pops up');
    assert.equal(r.asked, 0, 'nothing is asked');
    assert(r.clickMs < 200, `the click does not block the page (${Math.round(r.clickMs)} ms)`);
    // the sheet nests the other file's charms, and only those
    await page.waitForFunction(n => { const sh = CN.activePage('gold'); return ['complete', 'partial'].includes(sh.status) && sh.placements.length >= n && !sh.feedWait?.length; }, ids.bN, { timeout: 120000 });
    const end = await page.evaluate(() => { const sh = CN.activePage('gold'); return { status: sh.status, placed: sh.placements.map(p => p.id), charms: sh.charms.map(c => c.id) }; });
    assert.deepEqual([...end.placed].sort(), [...ids.bIds].sort(), 'every charm of the other file is placed, and nothing else: ' + JSON.stringify(end));
    assert.deepEqual(errors, [], 'no page errors');
    console.log(`  ✓ removed while nesting: the search started again without the file (${end.status}, ${end.placed.length} placed), nothing of it placed or drawn, no message`);
  } finally { await browser.close(); srv.close(); fs.rmSync(tmp, { recursive: true, force: true }); }
})().catch(e => { console.error(e); process.exit(1); });
