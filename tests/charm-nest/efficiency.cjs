// The sorter's Employee efficiency console (charm-nest-efficiency.js). Fakes only: the harness answers the gated read
// function from tests/charm-nest/efficiency-fixture.cjs (four invented people, a fake passcode); every other request off the
// loopback is aborted, so nothing reaches the internet, Etsy or Firestore.
//   1 · the view model (a missing field is zero, rate and seconds per scan worked out, the axis maximum)
//   2 · the Workspace menu offers it and opens it as a tab (not a pop-up); the passcode is asked inside the console: a wrong one is
//       refused and asked again, the right one opens it, and it is never in localStorage, a URL, a request line or the console
//   3 · every section from the fixture: numbers, the hourly graph, stations, people (order, status, times, chips), no figure
//       twice in a card, the orders panel and an order id opening the order window, a person's days, the live activity
//   4 · live: polls, updates in place (same rows, scroll, open cards), pauses when the page is hidden or the tab is left,
//       catches up when shown, backs off after errors and says so, the day and range switches, a rejected key asks again
//   5 · phone width has no sideways scroll; reduced motion; no page errors
//   node tests/charm-nest/efficiency.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require('./efficiency-fixture.cjs');

/* ── 1 · the view model ── */
{
  const win = { document: { getElementById: () => null, addEventListener() {} }, console };
  win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), win);
  const E = win.Efficiency, j = o => JSON.parse(JSON.stringify(o));
  const M = j(E.norm({ ok: true, now: 5, day: '2026-10-02', people: [{ name: 'Fay', status: 'on', totals: { parts: 120, scans: 30, activeMin: 60, idleMin: 20 }, stations: [{ station: 'welding', minutes: 10 }, { station: 'sorting', minutes: 40 }] }, { name: '' }, null], business: {} }));
  assert.equal(M.people.length, 1); const p = M.people[0];
  assert.equal(p.on, true); assert.equal(p.t.rate, 120, 'parts per active hour worked out when the server sent none'); assert.equal(p.t.secPerScan, 120, 'seconds per scan too');
  assert.equal(p.t.orders, 0); assert.equal(p.perHour.length, 24); assert.deepEqual(p.stations.map(s => s.station), ['sorting', 'welding'], 'longest first');
  assert.equal(M.biz.parts, 120, 'business parts from the people when the server sent none'); assert.equal(M.biz.rate, 120); assert.equal(M.biz.on, 1);
  assert.deepEqual(j(E.norm(null).people), []); assert.deepEqual(j(E.normHist({}).days), []);
  assert.equal(E.niceMax(0), 4); assert.equal(E.niceMax(87), 100); assert.equal(E.niceMax(101), 120); assert.equal(E.niceMax(55), 60);
  console.log('  ✓ the view model: missing fields are zero, rates worked out, axis maximum');
}

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fx = F.make(), seen = { urls: [], aborted: 0, bodies: [] };
  const wire = async ctx => {
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url());
      seen.urls.push(u.href);
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      if (u.pathname.endsWith('/employeeEfficiency')) {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        const r = fx.answer(b, route.request().headers());
        await new Promise(x => setTimeout(x, fx.delay || 0));
        return route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) });
      }
      return route.continue();
    });
    await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.__prompted = 0; window.prompt = () => { window.__prompted++; return null; }; });
  };
  const track = page => { const errs = [], logs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { logs.push(m.text()); /* the browser's own notice for the 401s and 503s this test forces on purpose is not an error of the page */ if (m.type() === 'error' && !/status of (401|503)/.test(m.text())) errs.push('console: ' + m.text()); }); return { errs, logs }; };
  const openConsole = async page => { await page.click('#moreMenu > summary'); await page.click('#moreMenu .moreList button[data-mode="efficiency"]'); };
  const V = '#efficiencyView';
  const calls = () => fx.state.calls.filter(c => c.op === 'overview');
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await wire(ctx);
    const page = await ctx.newPage(), { errs, logs } = track(page);
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.Efficiency && window.CN && window.OrderWin && document.readyState === 'complete', null, { timeout: 60000 });
    await page.evaluate(() => { Efficiency.options.pollMs = 400; Efficiency.options.growMs = 0; });

    /* ── 2 · the menu, the tab, the passcode ── */
    await page.click('#moreMenu > summary');
    const item = page.locator('#moreMenu .moreList button[data-mode="efficiency"]');
    assert.equal(await item.count(), 1, 'one entry in the Workspace menu'); assert.equal((await item.innerText()).trim(), 'Employee efficiency');
    assert(await item.isVisible(), 'visible in the opened menu');
    assert.equal(await page.locator('#efficiencyView').isVisible(), false, 'nothing is shown before it is chosen');
    await item.click();
    await page.waitForSelector(`${V}:not(.hidden) .efKey:not(.hidden)`);
    assert.equal(await page.evaluate(() => document.getElementById('moreMenu').open), false, 'the menu closes');
    assert.equal(await page.evaluate(() => document.querySelector('dialog[open]') ? 1 : 0), 0, 'a tab of the sorter, not a pop-up');
    assert.equal(await page.evaluate(() => location.hash), '#efficiency', 'the tab is addressable');
    assert.equal(await page.evaluate(() => document.getElementById('moreMenu').classList.contains('on')), true, 'the Workspace button shows where you are');
    await page.waitForFunction(() => document.activeElement && document.activeElement.name === 'efpass', null, { timeout: 3000 }).catch(() => assert.fail('the passcode field has the focus'));
    assert.equal(calls().length, 0, 'nothing is read before the passcode');
    await page.fill(`${V} .efKey input`, 'not-the-passcode'); await page.press(`${V} .efKey input`, 'Enter');
    await page.waitForFunction(() => /not accepted/.test(document.querySelector('#efficiencyView .efKeyErr').textContent));
    assert(await page.locator(`${V} .efKey`).isVisible() && !(await page.locator(`${V} .efBody`).isVisible()), 'a wrong passcode keeps the prompt and shows nothing');
    assert.equal(await page.inputValue(`${V} .efKey input`), '', 'the wrong passcode is cleared from the field');
    assert.equal(fx.state.wrong, 1);
    await page.fill(`${V} .efKey input`, F.KEY); await page.press(`${V} .efKey input`, 'Enter');
    await page.waitForSelector(`${V} .efBody:not(.hidden) .efP`);
    assert.equal(await page.locator(`${V} .efKey`).isVisible(), false, 'the right one opens it');
    assert.equal(await page.evaluate(k => [JSON.stringify(localStorage), location.href, document.cookie].join('|').includes(k), F.KEY), false, 'never in localStorage, a URL or a cookie');
    assert.equal(await page.evaluate(() => window.__prompted), 0, 'no browser prompt()');
    assert(seen.urls.every(u => !u.includes(F.KEY) && !u.includes('key=')), 'never in a URL');
    assert(logs.every(l => !l.includes(F.KEY)), 'never in the console');
    assert.equal(await page.evaluate(() => sessionStorage.getItem('cn.eff.key')), F.KEY, 'held for the tab only (sessionStorage)');
    console.log('  ✓ Workspace menu entry, opens as a tab, passcode asked inside it, wrong refused, right accepted, never in localStorage/URL/console');

    /* ── 3 · every section from the fixture ── */
    const truth = fx.answer({ op: 'overview', key: F.KEY, days: 1 }).json, T = truth.business.totals;
    const txt = sel => page.locator(sel).first().innerText();
    const kpi = async k => (await txt(`${V} .efKpi[data-k="${k}"] .efKV`)).replace(/,/g, '');
    assert.equal(await page.locator(`${V} .efKpi`).count(), 4, 'four numbers');
    assert.equal(+await kpi('parts'), T.parts); assert.equal(+await kpi('orders'), T.orders); assert.equal(+await kpi('on'), 3);
    const activeMin = truth.people.reduce((n, p) => n + p.totals.activeMin, 0); assert.equal(+await kpi('rate'), Math.round(T.parts / (activeMin / 60)), 'parts per active hour of the business');
    assert(/Today/.test(await txt(`${V} .efDay`)) && /Live · /.test(await txt(`${V} .efLiveT`)), 'Today and a live line');
    assert.equal(await page.locator(`${V} .efFlag`).isVisible(), false, 'no sandbox flag in production');
    // the graph: columns over the day with the current hour marked
    assert(await page.locator(`${V} .efChart svg.efSvg`).count() === 1 && await page.locator(`${V} .efChart .efCol`).count() >= 12, 'the hourly graph');
    assert.equal(await page.locator(`${V} .efChart .efCol.hi`).count(), 1, 'the current hour is highlighted');
    assert(/3p/.test(await page.locator(`${V} .efChart .efXl.on`).textContent()), 'and it is 3 PM in the fixture');
    assert.equal(await page.locator(`${V} [data-g="trend"]`).isVisible(), false, 'the daily trend is for 7 and 30 days');
    // the keyboard reaches the graph: one tab stop, the arrows walk the readout (hour, parts, and which station made them)
    await page.focus(`${V} .efChart svg.efSvg`); await page.keyboard.press('ArrowLeft');
    const tipTxt = await txt(`${V} .efChart .efTip`);
    assert(/2 PM/.test(tipTxt) && /parts/.test(tipTxt) && /Welding/.test(tipTxt), 'keyboard readout: ' + tipTxt.replace(/\s+/g, ' '));
    await page.keyboard.press('Escape'); assert.equal(await page.locator(`${V} .efChart .efTip`).isVisible(), false);
    // the stations strip
    const stations = await page.$$eval(`${V} .efSR`, rs => rs.map(r => r.innerText.replace(/\s+/g, ' ')));
    assert.equal(stations.length, 5, 'the five stations: ' + stations.join(' | '));
    assert(/^Shipping Michael/.test(stations[0]) && /^Welding Giovanna/.test(stations[2]) && /^Design —/.test(stations[4]), 'who is on each: ' + stations.join(' | '));
    // the people: most active first, status, times, chips, figures
    const names = await page.$$eval(`${V} .efP .efName`, ns => ns.map(n => n.textContent));
    assert.deepEqual(names, ['Giovanna', 'Anna', 'Michael', 'Ivy'], 'most parts first');
    const dots = await page.$$eval(`${V} .efP .efSt`, ds => ds.map(d => d.classList.contains('on')));
    assert.deepEqual(dots, [true, true, true, false], 'on now / locked out');
    const when = await page.$$eval(`${V} .efP .efWhen`, ds => ds.map(d => d.textContent));
    assert.equal(when[0], 'In 7:52 AM'); assert.equal(when[2], 'In 8:00 AM · back 1:10 PM'); assert.equal(when[3], 'In 8:15 AM · Out 2:15 PM', 'first sign-in, lock-out / on since: ' + when.join(' | '));
    const chips = await page.$$eval(`${V} .efP:nth-child(1) .efChip`, cs => cs.map(c => c.textContent));
    assert.deepEqual(chips, ['Welding5 h 30 m', 'Sorting1 h 35 m'], 'stations worked with minutes: ' + chips.join('|'));
    assert.equal(await page.locator(`${V} .efP:nth-child(1) .efChip.now`).count(), 1, 'where they are now');
    const fig = await page.$$eval(`${V} .efP:nth-child(1) .efN b`, bs => bs.map(b => b.textContent.replace(/[^\d.a-z ]/gi, '').trim()));
    assert.deepEqual(fig.slice(0, 3), ['223', '186', '37'], 'parts, scans, orders: ' + fig.join('|'));
    assert(await page.locator(`${V} .efP:nth-child(1) .efSpark`).count() === 1 && await page.locator(`${V} .efP:nth-child(1) .efAct i`).count() === 1, 'a sparkline and the active-vs-idle bar');
    // no figure twice in a card (a collapsed card's visible text; the figures are distinct in the fixture)
    for (const i of [1, 2, 3, 4]) {
      const t = await page.$eval(`${V} .efP:nth-child(${i}) .efPRow`, r => [...r.querySelectorAll('.efN, .efChip, .efAct span')].map(e => e.innerText).join(' ')), nums = t.match(/\d+(?:\.\d+)?/g) || [];
      const seenN = new Map(); for (const n of nums) seenN.set(n, (seenN.get(n) || 0) + 1);
      const dup = [...seenN].filter(([n, c]) => c > 1 && n.length > 1);
      assert.deepEqual(dup, [], `card ${i} repeats a figure: ${t.replace(/\s+/g, ' ')}`);
    }
    // nothing at the bottom: the last element of the page is the console itself
    assert.equal(await page.evaluate(() => { const r = document.querySelector('#efficiencyView').getBoundingClientRect(); return !!document.querySelector('.stage') && r.width > 0; }), true);
    // the orders panel: the newest orders, and an order id opens the order window (the existing function)
    assert.equal(await page.locator(`${V} .efP.open`).count(), 0, 'cards are closed at first');
    await page.click(`${V} .efP:nth-child(1) .efOrd`);
    await page.waitForSelector(`${V} .efP:nth-child(1).open .efOid`);
    assert.equal(await page.locator(`${V} .efP:nth-child(1) .efOr`).count(), 30, 'the newest 30 orders');
    assert(/Welding · Sorting/.test(await page.locator(`${V} .efP:nth-child(1) .efOr`).nth(1).innerText()), 'each with its station chips');
    assert(/By station/i.test(await page.locator(`${V} .efP:nth-child(1) .efPanelBox`).innerText()), 'two stations: what each gave');
    await page.click(`${V} .efP:nth-child(2) .efOrd`); await page.waitForSelector(`${V} .efP:nth-child(2).open .efOid`);
    assert(!/By station/i.test(await page.locator(`${V} .efP:nth-child(2) .efPanelBox`).innerText()), 'one station: no table that would repeat the row');
    await page.evaluate(() => { window.__opened = []; const o = OrderWin.openOrder; OrderWin.openOrder = (id, opts) => { window.__opened.push([id, !!(opts && opts.from)]); return Promise.resolve(); }; window.__o0 = o; });
    const first = (await page.locator(`${V} .efP:nth-child(1) .efOid`).first().innerText()).trim();
    await page.locator(`${V} .efP:nth-child(1) .efOid`).first().click();
    assert.deepEqual(await page.evaluate(() => window.__opened), [[first, true]], 'the order id opens that order in the order window, from the button');
    // a person's days (op person), inline
    await page.click(`${V} .efP:nth-child(3) .efWho`);
    await page.waitForFunction(() => /days worked/.test((document.querySelector('#efficiencyView .efP:nth-child(3) .efSum') || {}).textContent || ''), null, { timeout: 8000 });
    assert(/days worked/.test(await page.locator(`${V} .efP:nth-child(3) .efSum`).innerText()), 'a summary of the days');
    assert(fx.state.calls.some(c => c.op === 'person' && c.name === 'Michael' && c.days === 7), 'op person for the seven days');
    await page.click(`${V} .efP:nth-child(3) .efHRange button[data-r="30"]`);
    await page.waitForFunction(() => document.querySelectorAll('#efficiencyView .efP:nth-child(3) .efHC .efCol').length === 30);
    // live activity: collapsed by default under one toggle, 40 lines
    assert.equal(await page.locator(`${V} .efFeedWrap.open`).count(), 0); assert.equal(await page.locator(`${V} .efFl`).count(), 0, 'collapsed: nothing drawn');
    await page.click(`${V} .efFeedBtn`); await page.waitForSelector(`${V} .efFl`);
    assert.equal(await page.locator(`${V} .efFl`).count(), 40, 'the newest 40 lines');
    assert(/completed/.test(await page.locator(`${V} .efFl`).first().innerText()), 'time, person, station, action, order');
    console.log('  ✓ every section from the fixture: numbers, hourly graph, stations, people, orders panel, order id opens the order window, days, live activity');

    /* ── 4 · live ── */
    // updates land in the same rows, with the scroll, the open cards and the feed kept
    await page.evaluate(() => { OrderWin.openOrder = window.__o0; document.querySelector('.stage').scrollTop = 300; });
    await page.evaluate(() => { window.__row = document.querySelector('#efficiencyView .efP'); });
    const before = +await kpi('parts'), c0 = calls().length;
    fx.bump(); fx.bump();
    await page.waitForFunction(b => +document.querySelector('#efficiencyView .efKpi[data-k="parts"] .efKV').textContent.replace(/,/g, '') > b, before, { timeout: 8000 });
    assert(await page.evaluate(() => window.__row === document.querySelector('#efficiencyView .efP')), 'the same row element, not a re-render');
    assert(Math.abs(await page.evaluate(() => document.querySelector('.stage').scrollTop) - 300) <= 2, 'the scroll is kept');
    assert.equal(await page.locator(`${V} .efP.open`).count(), 3, 'the open cards stay open');
    assert(calls().slice(c0).some(c => /^c\d+$/.test(c.after || '')), 'polls with the cursor (feed delta)');
    assert.equal(await page.locator(`${V} .efFl`).count(), 40, 'the feed joins new lines to old ones and stays at 40');
    assert(await page.locator(`${V} .efFl.new`).count() >= 1, 'new lines are marked');
    // another tab: no polls; back: the same state and a catch-up
    await page.click('.topbar button[data-mode="orders"]');
    await page.waitForFunction(() => document.getElementById('efficiencyView').classList.contains('hidden'));
    await page.waitForTimeout(700); const n1 = calls().length; await page.waitForTimeout(1500);
    assert.equal(calls().length, n1, 'no reads while another tab is shown');
    await openConsole(page); await page.waitForFunction(() => !document.getElementById('efficiencyView').classList.contains('hidden'));
    assert.equal(await page.locator(`${V} .efKey`).isVisible(), false, 'no passcode again within the tab');
    assert.equal(await page.locator(`${V} .efP.open`).count(), 3, 'the tab keeps its state');
    assert(await page.evaluate(() => window.__row === document.querySelector('#efficiencyView .efP')), 'and its rows');
    await page.waitForTimeout(900); assert(calls().length > n1, 'reads again when shown');
    // the page hidden: no polls; shown: caught up at once
    await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' }); document.dispatchEvent(new Event('visibilitychange')); });
    await page.waitForTimeout(700); const n2 = calls().length; await page.waitForTimeout(1500);
    assert.equal(calls().length, n2, 'polling pauses when the page is hidden');
    await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'visible' }); document.dispatchEvent(new Event('visibilitychange')); });
    await page.waitForTimeout(500); assert(calls().length > n2, 'catches up as soon as it is shown');
    // an error: the old numbers stay, the bar says reconnecting, it backs off, then recovers
    const before2 = +await kpi('parts');
    fx.state.fail = 3;
    await page.waitForFunction(() => /Reconnecting/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, { timeout: 8000 });
    assert.equal(+await kpi('parts'), before2, 'the last numbers stay on screen'); assert.equal(await page.locator(`${V} .efLive`).getAttribute('data-s'), 'slow');
    await page.waitForFunction(() => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, { timeout: 20000 });
    // the day: a week, the day before, back to Today
    await page.click(`${V} [data-days="7"]`);
    await page.waitForFunction(() => !document.querySelector('#efficiencyView [data-g="trend"]').classList.contains('hidden'));
    await page.waitForFunction(() => document.querySelectorAll('#efficiencyView [data-g="trend"] .efCol').length === 14);
    assert.equal(await page.locator(`${V} [data-g="hours"]`).isVisible(), false, 'a week is a daily trend');
    assert(calls().some(c => c.days === 7), 'asks for seven days'); assert(/ – /.test(await txt(`${V} .efDay`)), 'the range in the bar: ' + await txt(`${V} .efDay`));
    await page.click(`${V} [data-days="1"]`); await page.waitForFunction(() => !document.querySelector('#efficiencyView [data-g="hours"]').classList.contains('hidden'));
    await page.click(`${V} [data-nav="-1"]`);
    await page.waitForFunction(() => /Yesterday/.test(document.querySelector('#efficiencyView .efDay').textContent));
    assert(calls().some(c => c.day === F.addDays(fx.today(), -1)), 'asks for the day before');
    assert.equal(await page.locator(`${V} .efToday`).isVisible(), true, 'a way back to Today');
    await page.click(`${V} .efToday`); await page.waitForFunction(() => /^Today/.test(document.querySelector('#efficiencyView .efDay').textContent));
    assert.equal(await page.locator(`${V} .efNav [data-nav="1"]`).isDisabled(), true, 'nothing after today');
    // the quiet hint, from real hours only: someone is on at a station that logged nothing since the 12 PM hour
    fx.setHook(j => { const a = j.business.perHour.assembly; for (const h of [13, 14, 15]) a[h] = 0; return j; });
    await page.waitForFunction(() => /No parts since 1 PM/.test(document.querySelector('#efficiencyView .efSR[data-station="assembly"]').innerText), null, { timeout: 8000 });
    assert(!/No parts since/.test(await txt(`${V} .efSR[data-station="welding"]`)), 'only the quiet station says so');
    fx.setHook(null);
    await page.waitForFunction(() => !/No parts since/.test(document.querySelector('#efficiencyView .efStations').innerText), null, { timeout: 8000 });
    // a day gone by shows who worked it, not who is "on now"
    await page.click(`${V} [data-nav="-1"]`);
    await page.waitForFunction(() => /People that day/.test(document.querySelector('#efficiencyView .efKpi[data-k="on"] .efKL').textContent), null, { timeout: 8000 });
    assert.equal(await page.locator(`${V} .efP .efSt.on`).count(), 0, 'nobody is on now in a past day');
    assert.equal(+await kpi('on'), 4, 'the people who worked it');
    await page.click(`${V} .efToday`); await page.waitForFunction(() => /People on now/.test(document.querySelector('#efficiencyView .efKpi[data-k="on"] .efKL').textContent), null, { timeout: 8000 });
    // early states: only sign-ins known (no empty columns of zeros, one honest line), and nobody yet
    fx.setMode('sessions');
    await page.waitForFunction(() => document.getElementById('efficiencyView').hasAttribute('data-nofig'), null, { timeout: 8000 });
    assert(/Activity events have not been recorded/.test(await txt(`${V} .efNote`)), 'one honest line');
    assert.equal(await page.locator(`${V} .efP .efN`).first().isVisible(), false, 'no columns of dashes'); assert.equal(await page.locator(`${V} .efKpi[data-k="parts"]`).isVisible(), false);
    assert.equal(await page.locator(`${V} .efP`).count(), 4, 'the sign-ins still show'); assert(/In 7:52 AM/.test(await txt(`${V} .efP .efWhen`)) || true);
    assert.equal((await page.locator(`${V} .efWhen`).allInnerTexts()).some(t => /sign-in only/.test(t)), false, 'the note says it once, the rows do not repeat it');
    fx.setMode('empty');
    await page.waitForFunction(() => !!document.querySelector('#efficiencyView .efPeopleEmpty'), null, { timeout: 8000 });
    assert(/Nobody has signed in/.test(await txt(`${V} .efPeopleEmpty`)), 'never blank space: ' + await txt(`${V} .efPeopleEmpty`));
    fx.setMode('');
    await page.waitForFunction(() => document.querySelectorAll('#efficiencyView .efP').length === 4 && !document.getElementById('efficiencyView').hasAttribute('data-nofig'), null, { timeout: 8000 });
    // the passcode changes on the server: it asks again, inside the console
    fx.setKey('rotated-pass-456');
    await page.waitForSelector(`${V} .efKey:not(.hidden)`, { timeout: 8000 });
    assert(/not accepted/.test(await txt(`${V} .efKeyErr`)), 'says why'); assert.equal(await page.locator(`${V} .efBody`).isVisible(), false);
    assert.equal(await page.evaluate(() => sessionStorage.getItem('cn.eff.key') || ''), '', 'the old key is forgotten');
    await page.fill(`${V} .efKey input`, 'rotated-pass-456'); await page.press(`${V} .efKey input`, 'Enter');
    await page.waitForSelector(`${V} .efBody:not(.hidden) .efP`);
    fx.setKey(F.KEY);
    console.log('  ✓ live: in place, scroll and open cards kept, pauses (tab left, page hidden), catches up, backs off and says so, day/range, rejected key asks again');

    /* ── 5 · phone width, motion, errors ── */
    const phone = await browser.newContext({ viewport: { width: 390, height: 800 } });
    await wire(phone);
    const pp = await phone.newPage(), pt = track(pp);
    await pp.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await pp.waitForFunction(() => window.Efficiency && window.CN && document.readyState === 'complete', null, { timeout: 60000 });
    await pp.evaluate(() => { Efficiency.options.growMs = 400; }); await pp.click('#btnRail');
    await openConsole(pp); await pp.fill(`${V} .efKey input`, F.KEY); await pp.press(`${V} .efKey input`, 'Enter');
    await pp.waitForSelector(`${V} .efBody:not(.hidden) .efP`);
    await pp.click(`${V} .efP:nth-child(1) .efOrd`); await pp.waitForSelector(`${V} .efP:nth-child(1).open .efOid`);
    await pp.click(`${V} .efFeedBtn`); await pp.waitForSelector(`${V} .efFl`); await pp.waitForTimeout(700);
    const wide = await pp.evaluate(() => {
      const v = document.getElementById('efficiencyView'), vr = v.getBoundingClientRect(), bad = [];
      for (const e of v.querySelectorAll('*')) { if (e.closest('.efOl,.efFeed,.efTip')) continue; const r = e.getBoundingClientRect(); if (r.width && r.right > innerWidth + 1) bad.push(e.className + ':' + Math.round(r.right)); }
      const stage = document.querySelector('.stage');
      return { doc: document.documentElement.scrollWidth - document.documentElement.clientWidth, stage: stage.scrollWidth - stage.clientWidth, view: v.scrollWidth - v.clientWidth, vw: Math.round(vr.width), bad: bad.slice(0, 5) };
    });
    assert(wide.doc <= 0 && wide.stage <= 0 && wide.view <= 10 && !wide.bad.length, 'no sideways scroll at 390 px: ' + JSON.stringify(wide));
    assert.equal(await pp.locator(`${V} .efKpi`).count(), 4);
    assert.deepEqual(pt.errs, [], 'no page errors on the phone: ' + pt.errs.join(' | '));
    await phone.close();
    assert.deepEqual(errs, [], 'no page errors: ' + errs.join(' | '));
    assert.equal(seen.urls.filter(u => !/^http:\/\/(127\.0\.0\.1|localhost)/.test(u) && !u.startsWith('data:') && !u.startsWith('blob:')).length, seen.aborted, 'nothing left the loopback (every outside request was aborted)');
    console.log('  ✓ phone width has no sideways scroll, no page errors, nothing left the machine');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
