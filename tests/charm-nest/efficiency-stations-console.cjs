// The stations board inside the Employee efficiency console (charm-nest-efficiency.js mounts charm-nest-efficiency-stations.js in its Stations
// tab and uses its order card in the Overview). Fakes only: the harness answers the gated function from tests/charm-nest/efficiency-fixture.cjs
// (the console's own fixture, op live included); everything off the loopback is aborted.
//   · the Stations tab shows every station of the live answer, one card per person, one picture per piece, the QR
//   · ONE live read serves the Overview and the board (the board rides the console's own poll: no second request every 3 s)
//   · the Overview's "Now working on" list is the same card; the Real | Sandbox switch redraws the board from the other store
//   · leaving the tab takes the board away; no console errors
//   node tests/charm-nest/efficiency-stations-console.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require('./efficiency-fixture.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const pwDir = process.env.PW_DIR || (fs.existsSync(path.join(root, 'node_modules/playwright-core')) ? path.join(root, 'node_modules') : '/opt/node22/lib/node_modules/playwright/node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fx = F.make(), seen = { aborted: 0 };
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url());
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      if (u.pathname.endsWith('/employeeEfficiency')) { let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {} const r = fx.answer(b, route.request().headers()); if (fx.delay) await sleep(fx.delay); try { return await route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) }); } catch (_) { return; } }
      return route.continue();
    });
    await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.prompt = () => null; });
    const page = await ctx.newPage(), errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) errs.push('console: ' + m.text()); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.Efficiency && window.EfficiencyStations && window.CN && window.OrderWin && document.readyState === 'complete', null, { timeout: 60000 });
    await page.click('#moreMenu > summary'); await page.click('#moreMenu .moreList button[data-mode="efficiency"]');
    await page.waitForSelector('#efficiencyView .efKey:not(.hidden)');
    await page.fill('#efficiencyView .efKey input', F.KEY); await page.press('#efficiencyView .efKey input', 'Enter');
    await page.waitForSelector('#efficiencyView .efTabBtn');
    const V = '#efficiencyView', live = () => fx.state.calls.filter(c => c.op === 'live');

    /* the Overview's "Now working on": the very same card */
    await page.waitForSelector(`${V} .efWk .esCard`, { timeout: 8000 });
    const wk = await page.$$eval(`${V} .efWk .esCard`, cs => cs.map(c => ({ rid: c.dataset.rid, qr: !!c.querySelector('.esQr img'), pieces: c.querySelectorAll('.esPcTh').length, who: c.querySelector('.esWho').innerText.replace(/\s+/g, ' ') })));
    assert.deepEqual(wk.map(w => w.rid).sort(), ['3521000101', '3521000138', '3521000175'], 'one card per person at work');
    assert(wk.every(w => w.qr), 'each with its QR'); assert.deepEqual(wk.map(w => w.pieces).sort(), [0, 2, 3], 'one picture per piece of a multi-piece order, none for a one-piece order');
    assert(wk.some(w => /Welding/.test(w.who)), 'on the Overview the card also names the station: ' + wk[0].who);
    console.log('  ✓ Overview: the Now working on list is the shared order card (QR, one picture per piece)');

    /* the Stations tab */
    await page.click(`${V} .efTabBtn[data-tab="stations"]`);
    await page.waitForSelector(`${V} .es .esSt`, { timeout: 8000 });
    assert.equal(await page.evaluate(() => location.hash), '#efficiency/stations');
    const rows = await page.$$eval(`${V} .esSt`, rs => rs.map(r => [r.dataset.key, r.dataset.state, r.querySelectorAll('.esCard').length]));
    assert.deepEqual(rows, [['welding', 'working', 1], ['assembly', 'working', 1], ['shipping', 'working', 1], ['sorting', 'idle', 0], ['design', 'offline', 0]], 'every station of the answer, its state, one card per person');
    assert.equal(await page.$$eval(`${V} .esSt[data-key=welding] .esPcTh`, b => b.length), 3, 'the 3-piece order shows 3 pictures');
    assert.equal(await page.$eval(`${V} .esSt[data-key=welding] .esCard .esT`, e => /^\d\d:\d\d$/.test(e.textContent)), true, 'a ticking time since scanned');
    assert(/^Live · updated/.test(await page.$eval(`${V} .es .esLiveT`, e => e.textContent)), 'the board says Live');
    // ONE read for both: about one `live` request every 3 s, not two
    const n0 = live().length; await sleep(7200); const n = live().length - n0;
    assert(n >= 2 && n <= 3, `one live read every ~3 s serves the Overview and the board: ${n} reads in 7 s`);
    // the board follows the answer: a person finishes, another order arrives
    fx.setLiveHook((j) => { const w = j.stations.find(s => s.key === 'sorting'); w.state = 'working'; w.people = ['Dana']; w.current = [{ person: 'Dana', rid: '3521000900', orderNumber: '3521000900', customer: 'Buyer 900', scannedAt: Date.now() - 4000, thumbUrl: '', qr: { text: '3521000900' }, pieces: [] }]; return j; });
    await page.waitForSelector(`${V} .es .esCard[data-rid="3521000900"]`, { timeout: 8000 });
    assert.equal(await page.$eval(`${V} .esSt[data-key=sorting]`, r => r.dataset.state), 'working', 'a new order on a station: it works now');
    fx.setLiveHook(null);
    await page.waitForFunction(() => document.querySelector("#efficiencyView .es .esCard[data-rid=\"3521000900\"]") === null, null, { timeout: 12000 });
    console.log('  ✓ Stations tab: every station, one card per person, one picture per piece; one shared live read; follows the answer');

    /* the live layer's own shape (E2): a laser sheet with a title and no QR, the name of the page, a piece count above the pieces listed; a click on the person goes to their page */
    fx.setLiveHook((j) => {
      const w = j.stations.find(s => s.key === 'welding'); w.current[0].deviceLabel = 'Welding 2'; w.current[0].id = 'welding__weld-2__Giovanna'; w.current[0].pieceCount = 5;
      j.stations.push({ key: 'laser', label: 'Laser', state: 'working', people: ['Zoe'], devices: [{ device: 'laser-1', label: 'Laser 1', state: 'working', person: 'Zoe', since: Date.now() - 3600000 }], lastEventAt: Date.now() - 9000, counts: { partsToday: 3, ordersToday: 1, scansToday: 4 },
        current: [{ id: 'laser__laser-1__Zoe', person: 'Zoe', device: 'laser-1', deviceLabel: 'Laser 1', kind: 'sheet', rid: '', orderNumber: '', customer: '', title: 'GF Sheet 9', scannedAt: Date.now() - 65000, beatAt: Date.now() - 4000, qr: null, pieces: [], pieceCount: 0, thumbUrl: '' }] });
      j.signedIn.push({ name: 'Zoe', stationKey: 'laser', since: Date.now() - 3600000, lastSeenAt: Date.now() - 4000 }); return j;
    });
    await page.waitForSelector(`${V} .es .esSt[data-key=laser] .esCard[data-kind=sheet]`, { timeout: 8000 });
    const sh = await page.$eval(`${V} .es .esSt[data-key=laser] .esCard`, c => ({ oid: c.querySelector('.esOid').textContent, qr: c.querySelector('.esQr').getClientRects().length, tl: c.querySelector('.esTl').textContent, who: c.querySelector('.esWho').innerText.replace(/\s+/g, ' ') }));
    assert.deepEqual([sh.oid, sh.qr, sh.tl], ['GF Sheet 9', 0, 'since started'], 'a laser sheet on the board: its title, no QR'); assert(/Zoe/.test(sh.who) && /Laser 1/.test(sh.who), sh.who);
    await page.waitForFunction(() => { const r = document.querySelector('#efficiencyView .es .esSt[data-key=welding]'); return r && r.querySelectorAll('.esCard').length === 1; }, null, { timeout: 8000 });   // (the card got its server id: the first one is told it is done and goes)
    assert.equal(await page.$eval(`${V} .es .esSt[data-key=welding] .esCard .esSn`, e => e.textContent), 'Welding 2', 'the page of the station, when it has its own name');
    assert.equal(await page.$eval(`${V} .es .esSt[data-key=welding] .esPieces .esMore`, e => e.textContent), '+2', '5 pieces, 3 pictured: the rest counted');
    await page.click(`${V} .es .esSt[data-key=laser] .esWho`);   // (the person's name opens the person's page, as in the rest of the console)
    await page.waitForFunction(() => /^#efficiency\/person\/Zoe/.test(location.hash), null, { timeout: 4000 });
    fx.setLiveHook(null); await page.evaluate(() => Efficiency.go('stations')); await page.waitForSelector(`${V} .es .esSt`, { timeout: 8000 });
    console.log('  ✓ the live layer\'s shape: a laser sheet card, the page name, the piece count; a person opens their page');

    /* the Real | Sandbox switch: the board is rebuilt from the other store */
    await page.click(`${V} .efView button[data-view="sandbox"]`);
    await page.waitForFunction(() => { const r = document.querySelectorAll('#efficiencyView .esSt'); return r.length === 1 && r[0].dataset.key === 'sorting'; }, null, { timeout: 10000 });
    assert.equal(await page.$eval(`${V} .esSt .esCard .esOid`, e => e.textContent), '3521009001', 'Sandbox shows the sandbox copies only');
    assert(live().some(c => c.sandbox === true), 'the sandbox read was asked for with sandbox:true');
    await page.click(`${V} .efView button[data-view="real"]`);
    await page.waitForFunction(() => document.querySelectorAll('#efficiencyView .esSt').length === 5, null, { timeout: 10000 });
    console.log('  ✓ Real | Sandbox: the board is rebuilt from the other store and back');

    /* first load straight onto Stations while the answer is slow: ONE small labelled spinner (the board's), not one in the console's header and two more in the board */
    { const p2 = await ctx.newPage(); p2.on('pageerror', e => errs.push(e.message));
      await p2.addInitScript(k => { try { sessionStorage.setItem('cn.eff.key', k); } catch (_) {} }, F.KEY);
      fx.delay = 2500; await p2.goto(`${srv.sorterOrigin}/charm-nest-1.html#efficiency/stations`);
      await p2.waitForFunction(() => window.Efficiency && window.EfficiencyStations && window.CN && window.OrderWin && document.readyState === 'complete', null, { timeout: 60000 });
      await p2.evaluate(() => { Efficiency.open(); }); await p2.waitForSelector(`${V} .es .esWait`, { state: 'visible', timeout: 8000 });
      const spin = () => p2.evaluate(() => [...document.querySelectorAll('#efficiencyView .spin, #efficiencyView .esSpin')].filter(e => { for (let n = e; n && n !== document.body; n = n.parentElement) { const cs = getComputedStyle(n); if (cs.display === 'none' || cs.visibility === 'hidden' || n.hidden) return false; } const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0; }).map(e => e.parentElement.textContent.trim().slice(0, 30)));
      const wait = await spin(); assert(wait.length === 1 && /^Reading the stations/.test(wait[0]), 'one labelled spinner while the stations load: ' + JSON.stringify(wait));
      fx.delay = 0; await p2.waitForSelector(`${V} .es .esSt`, { timeout: 15000 });
      assert.deepEqual(await spin(), [], 'and none once they are there'); await p2.close(); }
    console.log('  ✓ first load of Stations: one labelled spinner');

    /* leaving the tab takes the board away */
    await page.click(`${V} .efTabBtn[data-tab="overview"]`); await sleep(400);
    assert.equal(await page.$$eval(`${V} .es`, e => e.length), 0, 'the board is gone from the page when another tab is shown');
    assert.deepEqual(errs, [], 'no errors: ' + errs.join(' | ')); assert.equal(seen.aborted, 0, 'nothing went to the internet');
    console.log('  ✓ leaving the tab unmounts the board; no console errors');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
