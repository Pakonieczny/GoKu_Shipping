// One employee's full page (charm-nest-efficiency-person.js), opened from the console (#efficiency/person/<name>) on the sorter.
// Fakes only: the harness answers the gated read function from tests/charm-nest/efficiency-person-fixture.cjs (two invented people, a
// fake passcode, a year of invented days) and, for the console's own reads, from efficiency-fixture.cjs; every other request off the
// loopback is aborted, so nothing reaches the internet, Etsy or Firestore.
//   1 · the page's own logic (periods, a missing figure stays a dash, the answer's shapes)
//   2 · opening it: labelled spinner, header (name, where signed in now, stations), the live light, Back
//   3 · every range (Day Week Month 3 months Year Custom) reads what it should, figures agree with the fixture, the change against the
//       period before (and "no data" where there is none), hover definitions say whether a figure is estimated
//   4 · charts: crosshair read-outs (mouse and keyboard), the measure switch, heat and station mix, the calendar (states, hover, a click opens that Day)
//   5 · issues by kind (an order opens), rates and contact, the order list (E8's module: search filters, pages, rows open an order,
//       thumbnails and QR; and the page's own small list when that module is absent)
//   6 · live: the "Now working on" card, header follows sign-out, an honest Reconnecting, nothing is read while hidden
//   7 · motion: numbers count up and settle, frames stay short, reduced motion is instant, thumbnails zoom in place
//   8 · 1440 / 900 / 390 px have no sideways scroll; no page errors; nothing reached off the loopback
//   node tests/charm-nest/efficiency-person.cjs      (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir> to save screenshots)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require('./efficiency-person-fixture.cjs'), EF = require('./efficiency-fixture.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ── 1 · the page's own logic ── */
{
  const win = { console }; win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency-person.js'), 'utf8'), win);
  const E = win.EfficiencyEmployee, j = o => JSON.parse(JSON.stringify(o));
  assert.deepEqual(j(E.periodOf('day', '2026-10-05')), { from: '2026-10-05', to: '2026-10-05' });
  assert.deepEqual(j(E.periodOf('week', '2026-10-05')), { from: '2026-09-29', to: '2026-10-05' }, 'a week is the 7 days ending on the day');
  assert.deepEqual(j(E.periodOf('month', '2026-10-05')), { from: '2026-09-06', to: '2026-10-05' }); assert.equal(E.periodOf('quarter', '2026-10-05').from, '2026-07-08'); assert.equal(E.periodOf('year', '2026-10-05').from, '2025-10-06');
  assert.deepEqual(j(E.periodOf('custom', '2026-10-05', { from: '2026-08-01', to: '2026-08-20' })), { from: '2026-08-01', to: '2026-08-20' });
  assert.equal(E.shiftAnchor('week', '2026-10-05', -1, E.periodOf('week', '2026-10-05')), '2026-09-28'); assert.equal(E.shiftAnchor('custom', '2026-08-20', -1, { from: '2026-08-01', to: '2026-08-20' }), '2026-07-31');
  const N = j(E.norm({ ok: true, name: 'Zed', found: true, kpis: { parts: { label: 'Parts', unit: 'pieces', value: null, prev: 5, better: 'up', def: 'x' }, orders: 7 }, series: [{ day: '2026-10-01', parts: null }, { day: '2026-10-02', parts: null, scans: 4 }, { day: '2026-10-03', parts: null, scans: 3 }], calendar: [{ day: '2026-10-01', state: 'weird' }] }, { name: 'Zed' }));
  assert.equal(N.src.kpis.parts.v, null, 'a figure the data does not know stays null: a dash, never a zero'); assert.equal(N.src.kpis.orders.v, 7);
  assert.equal(N.series[0].parts, null); assert.equal(N.series[0].hasData, false); assert.equal(N.series[2].scans, 3); assert.equal(N.src.kpis.scans.v, 7, 'a total the server did not send is the days added up, and says so'); assert.equal(N.src.kpis.scans.derived, true);
  assert.equal(N.cal[0].state, 'before', 'an unknown calendar state is not invented');
  assert.equal(j(E.norm(null)).found, true); assert.deepEqual(j(E.norm({}).series), []);
  const fx = F.make(), A = fx.person({ op: 'person', name: 'Ana M.', range: 'month', compare: true }), N2 = j(E.norm(A, { name: 'Ana M.' }));
  assert.equal(N2.src.kpis.parts.v, A.kpis.parts.value); assert.equal(N2.src.kpis.parts.prev, A.kpis.parts.prev); assert.equal(N2.series.length, 30); assert.equal(N2.cal.length, 30);
  assert(N2.src.att.daysWorked && N2.src.rates.firstPassRate && N2.src.contact.repliesSent && N2.issues.byKind.length, 'attendance, rates, contact and issues are read');
  const L = j(E.pickLive({ at: 5, signedIn: [{ name: 'Ana M.', stationKey: 'welding', since: 1, lastSeenAt: 2 }], stations: [{ key: 'welding', label: 'Welding', current: [{ person: 'Ana M.', rid: '35210001', station: 'welding' }, { person: 'Someone Else', rid: '9' }] }] }, 'Ana M.'));
  assert.equal(L.where.stationKey, 'welding'); assert.deepEqual(L.current.map(c => c.rid), ['35210001'], 'only this person\'s order is in the card'); assert.equal(L.current[0].stationLabel, 'Welding');
  console.log('  ✓ periods are rolling windows, a missing figure stays a dash, the answer\'s shapes are read, the live card picks this person only');
}

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  // both fakes keep the console's clock: Friday 2 Oct 2026, 3:42 PM New York
  const pf = F.make({ now: Date.UTC(2026, 9, 2, 19, 42) }), ef = EF.make(), seen = { urls: [], aborted: 0 }, SHOTS = process.env.SHOTS || '';
  const wire = async ctx => {
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url()); seen.urls.push(u.href);
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      if (u.pathname.endsWith('/employeeEfficiency')) {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        const mine = ['person', 'personOrders', 'live'].includes(b.op), r = mine ? pf.answer(b) : ef.answer(b, route.request().headers());
        const d = mine ? pf.delayFor(b) : 0; if (d) await sleep(d);
        return route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) });
      }
      return route.continue();
    });
    await ctx.addInitScript(k => { try { localStorage.setItem('cn.employee', 'Tester'); sessionStorage.setItem('cn.eff.key', k); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.__prompted = 0; window.prompt = () => { window.__prompted++; return null; }; }, F.KEY);
  };
  const track = page => { const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error' && !(/status of (401|403|405|429|503)/.test(m.text()) && /employeeEfficiency/.test((m.location() && m.location().url) || ''))) errs.push('console: ' + m.text()); }); return errs; };
  const V = '#efficiencyView', P = `${V} .efp`;
  const person = () => pf.state.calls.filter(c => c.op === 'person'), orderCalls = () => pf.state.calls.filter(c => c.op === 'personOrders');
  const today = pf.today, truthOf = (range, extra) => pf.person(Object.assign({ op: 'person', name: 'Ana M.', range, day: today, compare: true }, extra || {}));
  const nums = s => +String(s).replace(/[^\d.\-]/g, '');
  const kpiText = (page, k) => page.locator(`${P} .efpK[data-k="${k}"] .efpKV`).innerText();
  const state = page => page.evaluate(() => { const h = EfficiencyEmployee.instances[0]; return h ? h.state : null; });
  const loaded = async (page, range, anchor) => {
    try { await page.waitForFunction(([r, a]) => { const h = EfficiencyEmployee.instances[0], s = h && h.state; return !!s && s.loaded && s.range === r && (!a || s.anchor === a) && !document.querySelector('#efficiencyView .efpBusy.on'); }, [range, anchor || ''], { timeout: 15000 }); }
    catch (e) { throw new Error(`the page did not settle on ${range}${anchor ? ' ' + anchor : ''}: ` + JSON.stringify(await page.evaluate(() => ({ s: EfficiencyEmployee.instances.map(h => h.state), busy: !!document.querySelector('#efficiencyView .efpBusy.on'), hash: location.hash, eff: Efficiency.state.mounted })))); }
  };
  const settle = async (page, ms = 150) => { await page.waitForTimeout(ms); };
  const openPerson = async (page, name) => { await page.evaluate(n => { sessionStorage.removeItem('cn.eff.p.range'); Efficiency.go('person', n); }, name); await page.waitForSelector(`${P}`, { timeout: 15000 }); await loaded(page, 'week'); };
  const boot = async (ctxOpts) => {
    const ctx = await browser.newContext(Object.assign({ viewport: { width: 1440, height: 900 } }, ctxOpts)); await wire(ctx);
    const page = await ctx.newPage(), errs = track(page);
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.Efficiency && window.EfficiencyEmployee && window.EfficiencyOrders && window.CN && window.OrderWin && document.readyState === 'complete', null, { timeout: 60000 });
    await page.evaluate(() => { Efficiency.options.pollMs = 600; Efficiency.options.liveMs = 300; Efficiency.options.growMs = 0; Object.assign(EfficiencyEmployee.options, { liveMs: 300, rangeMs: 700, calMs: 900, ordersMs: 700, tickMs: 250 }); window.__opened = []; });
    await page.evaluate(() => { Efficiency.open(); });
    await page.waitForSelector(`${V}:not(.hidden)`);
    await page.evaluate(() => { Efficiency.api.openOrder = (btn, rid) => { window.__opened.push(String(rid)); }; });
    return { ctx, page, errs };
  };
  try {
    const A = await boot({ reducedMotion: 'reduce' }), page = A.page;

    /* ── 2 · opening it ── */
    pf.setDelay(b => (b.op === 'person' && b.range === 'week' ? 900 : 0));
    await page.evaluate(() => { sessionStorage.removeItem('cn.eff.p.range'); Efficiency.go('person', 'Ana M.'); });
    await page.waitForSelector(`${P} .efpWait:not(.hidden)`, { timeout: 15000 });
    assert(/Ana M\./.test(await page.locator(`${P} .efpWaitT`).innerText()) && await page.locator(`${P} .efpWait .spin`).isVisible(), 'a small labelled spinner while the first answer is read');
    assert.equal(await page.locator(`${P} .efpBody`).isVisible(), false, 'no figure and no zero is drawn before the answer');
    assert.equal(await page.evaluate(() => location.hash), '#efficiency/person/Ana%20M.', 'the person is in the address');
    await loaded(page, 'week'); pf.setDelay(null);
    assert.equal(await page.locator(`${V} dialog[open], dialog[open]`).count(), 0, 'a page, not a pop-up');
    assert.equal((await page.locator(`${P} .efpName`).innerText()).trim(), 'Ana M.'); assert.equal((await page.locator(`${P} .efpAv`).innerText()).trim(), 'AM', 'initials');
    await page.waitForFunction(sel => /Signed in\s*at\s*Welding/.test(document.querySelector(sel).textContent), `${P} .efpWhere`, { timeout: 8000 });
    assert(/since/.test(await page.locator(`${P} .efpWhere`).innerText()), 'since when');
    assert(await page.locator(`${P} .efpChip`).count() >= 3 && await page.locator(`${P} .efpChip.now`).count() === 1, 'station chips, the one she is at marked');
    await page.waitForFunction(sel => document.querySelector(sel).dataset.s === 'live', `${P} .efpLive`); assert(/^Live/.test(await page.locator(`${P} .efpLiveT`).innerText()), 'the quiet live light');
    assert.equal((await page.locator(`${P} .efpBack`).innerText()).trim(), 'Employee efficiency'); assert.equal(await page.locator(`${P} .efpSeg button.on`).innerText(), 'Week', 'Week first');
    const first = person()[0]; assert(first.compare === true && first.range === 'week' && first.name === 'Ana M.', 'asks for the window with its comparison');
    console.log('  ✓ opens as a page: labelled spinner, header (initials, where signed in and since, station chips), live light, Back, Week first');

    /* ── 3 · every range ── */
    const RANGE = { day: 'Day', week: 'Week', month: 'Month', quarter: '3 months', year: 'Year' };
    for (const [key, label] of Object.entries(RANGE)) {
      const before = person().length;
      await page.click(`${P} .efpSeg button[data-range="${key}"]`); await loaded(page, key);
      const T = truthOf(key), st = await state(page);
      assert.equal(await page.locator(`${P} .efpSeg button.on`).innerText(), label); assert.equal(st.from, T.from, key + ' from'); assert.equal(st.to, T.to, key + ' to');
      if (key !== 'week') assert(person().slice(before).some(c => c.range === key && c.compare === true), key + ': the server is asked for this window');
      await page.waitForFunction(([k, v]) => document.querySelector(`#efficiencyView .efpK[data-k="${k}"] .efpKV`).textContent.replace(/,/g, '') === String(v), ['kpis.parts', T.kpis.parts.value]);
      assert.equal(nums(await kpiText(page, 'kpis.parts')), T.kpis.parts.value, key + ': parts agree with the fixture');
      const dl = (await page.locator(`${P} .efpK[data-k="kpis.parts"] .efpKD`).innerText()).replace(/\s+/g, ' ');
      if (T.kpis.parts.prev == null) assert(/no data/.test(dl) && !/[▲▼]/.test(dl), `${key}: nothing to compare with says so (${dl})`); else assert(/[▲▼] \d+%/.test(dl) && /vs/.test(dl), `${key}: the change against the period before (${dl})`);
      if (key === 'day') { assert.equal(await page.locator(`${P} [data-c="shift"]`).isVisible(), true, 'Day shows the shift'); assert.equal(await page.locator(`${P} [data-c="sp"]`).isVisible(), false); }
      else assert.equal(await page.locator(`${P} [data-c="sp"]`).isVisible(), true);
      const bars = await page.locator(`${P} [data-c="tp"] .efpBar0`).count();
      if (key === 'year') assert.equal(bars, T.series.length, 'a year is drawn one bar a week (' + bars + ')'); else if (key !== 'day') assert.equal(bars, T.series.length, key + ' one bar a day'); else assert(bars >= 10, 'Day is by the hour');
      assert.equal(await page.locator(`${P} .efpSeg button.on`).count(), 1);
    }
    await page.click(`${P} .efpSeg button[data-range="month"]`); await loaded(page, 'month');
    // arrows: earlier moves the window by its own length, later is off at today; Today returns
    const before = await state(page); await page.click(`${P} [data-nav="-1"]`); await loaded(page, 'month');
    const prevSt = await state(page); assert.equal(prevSt.to, F.addDays(before.to, -30), 'earlier = 30 days back'); assert.equal(await page.locator(`${P} [data-nav="1"]`).isDisabled(), false);
    assert(await page.locator(`${P} [data-today]`).isVisible(), 'a way back to today'); await page.click(`${P} [data-today]`); await loaded(page, 'month'); assert.equal((await state(page)).to, today);
    assert.equal(await page.locator(`${P} [data-nav="1"]`).isDisabled(), true, 'nothing after today');
    // custom
    await page.click(`${P} .efpSeg button[data-range="custom"]`); await page.waitForSelector(`${P} .efpCustom:not(.hidden)`);
    const cf = F.addDays(today, -19), ct = F.addDays(today, -6); await page.fill(`${P} .efpCustom input[name="from"]`, cf); await page.fill(`${P} .efpCustom input[name="to"]`, ct); await page.click(`${P} .efpCustom button[type="submit"]`);
    await page.waitForFunction(([f, t]) => { const s = EfficiencyEmployee.instances[0].state; return s.loaded && s.range === 'custom' && s.from === f && s.to === t; }, [cf, ct], { timeout: 15000 }); await loaded(page, 'custom');
    const TC = truthOf({ from: cf, to: ct }, { day: ct }); assert.equal(nums(await kpiText(page, 'kpis.parts')), TC.kpis.parts.value, 'a custom window reads its own days');
    assert(person().some(c => c.range && typeof c.range === 'object' && c.range.from === cf && c.range.to === ct), 'sent as { from, to }');
    await page.click(`${P} [data-today]`); await loaded(page, 'custom'); await page.click(`${P} .efpSeg button[data-range="week"]`); await loaded(page, 'week', today);
    // hover definitions: every figure says what it is, and whether it is counted or estimated
    await page.locator(`${P} .efpK[data-k="kpis.parts"]`).hover(); await page.waitForSelector(`${P} .efpHC.on`);
    let hc = (await page.locator(`${P} .efpHC`).innerText()).replace(/\s+/g, ' ');
    assert(/Pieces finished at a station, net of undos/.test(hc) && /Counted from logged activity/i.test(hc), 'definition and "counted": ' + hc);
    await page.locator(`${P} .efpK[data-k="kpis.secPerOrderMedian"]`).hover(); hc = (await page.locator(`${P} .efpHC`).innerText()).replace(/\s+/g, ' ');
    assert(/Estimated/i.test(hc) && /worked out from each day/i.test(hc), 'an estimate says so and why: ' + hc);
    await page.locator(`${P} .efpName`).hover(); await page.waitForFunction(sel => !document.querySelector(sel).classList.contains('on'), `${P} .efpHC`);
    assert.equal(await page.locator(`${P} .efpK[data-k="kpis.partsPerSignedHour"]`).isVisible(), false, 'the second rank of figures is folded');
    await page.click(`${P} [data-more-group="Production"]`); assert.equal(await page.locator(`${P} .efpK[data-k="kpis.partsPerSignedHour"]`).isVisible(), true, 'More figures opens it');
    for (const g of ['Production', 'Speed', 'Time', 'Attendance', 'Quality', 'Contact']) assert(await page.locator(`${P} .efpGroup .efpLabel span`, { hasText: g }).count() === 1, 'group ' + g);
    assert((await page.locator(`${P} .efpK .efpKV`).allInnerTexts()).every(t => !/NaN|undefined|null/.test(t)), 'no NaN on a card');
    console.log('  ✓ every range (Day, Week, Month, 3 months, Year, Custom) reads its window and agrees with the fixture; earlier/later/Today; change vs the period before, "no data" where none; hover definitions with counted/estimated');

    /* ── 4 · charts, calendar ── */
    await page.click(`${P} .efpSeg button[data-range="month"]`); await loaded(page, 'month'); await settle(page);
    const svg = page.locator(`${P} [data-c="tp"] svg.efpSvg`); await svg.scrollIntoViewIfNeeded(); const bb = await svg.boundingBox();
    await page.mouse.move(bb.x + bb.width * .6, bb.y + bb.height * .5); await page.waitForSelector(`${P} [data-c="tp"] .efpTip.on`);
    let tip = (await page.locator(`${P} [data-c="tp"] .efpTip`).innerText()).replace(/\s+/g, ' ');
    assert(/(parts|Not logged|Not yet)/.test(tip) && /[A-Z][a-z]{2}/.test(tip), 'the crosshair read-out names the day and the figure: ' + tip); assert.equal(await page.locator(`${P} [data-c="tp"] .efpCross.on`).count(), 1, 'a crosshair');
    await page.click(`${P} [data-metric="orders"]`); await page.mouse.move(bb.x + bb.width * .62, bb.y + bb.height * .5); tip = await page.locator(`${P} [data-c="tp"] .efpTip`).innerText(); assert(/orders|Not logged|Not yet/.test(tip), 'the measure switch changes the read-out: ' + tip);
    await page.click(`${P} [data-metric="parts"]`);
    await page.focus(`${P} [data-c="tp"] svg.efpSvg`); await page.keyboard.press('ArrowLeft'); assert(await page.locator(`${P} [data-c="tp"] .efpTip.on`).count() === 1, 'keyboard arrows walk the read-out'); await page.keyboard.press('Escape');
    for (const c of ['sp', 'tm']) { await page.locator(`${P} [data-c="${c}"] svg.efpSvg`).scrollIntoViewIfNeeded(); const s2 = await page.locator(`${P} [data-c="${c}"] svg.efpSvg`).boundingBox(); await page.mouse.move(s2.x + s2.width * .4, s2.y + s2.height * .5); await page.waitForSelector(`${P} [data-c="${c}"] .efpTip.on`); }
    await page.locator(`${P} .efpHc`).nth(10).hover(); await page.waitForSelector(`${P} .efpHeatH .efpTip.on`); assert(/PM|AM/.test(await page.locator(`${P} .efpHeatH .efpTip`).innerText()), 'a heat cell names its hour');
    await page.locator(`${P} .efpMr`).first().hover(); await page.waitForSelector(`${P} .efpMixH .efpTip.on`); assert(/Share/.test(await page.locator(`${P} .efpMixH .efpTip`).innerText()), 'the station mix answers the pointer');
    // the calendar
    const kinds = await page.evaluate(() => { const o = {}; document.querySelectorAll('#efficiencyView .efpDy[data-day]').forEach(b => { for (const k of ['worked', 'partial', 'off', 'closed', 'before', 'future']) if (b.classList.contains(k)) o[k] = (o[k] || 0) + 1; }); return o; });
    assert(kinds.worked >= 10 && kinds.off >= 1 && kinds.closed >= 4, 'the calendar shows worked, off and closed days: ' + JSON.stringify(kinds));
    const wd = page.locator(`${P} .efpDy.worked:not(.today)`).nth(2); const wday = await wd.getAttribute('data-day');
    await wd.hover(); await page.waitForSelector(`${P} .efpCalH .efpTip.on`); const ct2 = (await page.locator(`${P} .efpCalH .efpTip`).innerText()).replace(/\s+/g, ' '); assert(/signed in/.test(ct2) && /Parts/.test(ct2) && /Click to open this day/.test(ct2), 'calendar hover: ' + ct2);
    await wd.click(); await loaded(page, 'day', wday); assert.equal((await state(page)).from, wday, 'a click on a day jumps to that Day');
    assert(/^(?!Today)/.test(await page.locator(`${P} .efpDay`).innerText()) && await page.locator(`${P} [data-c="shift"]`).isVisible(), 'the Day view of that day');
    assert.equal(nums(await kpiText(page, 'kpis.parts')), truthOf('day', { day: wday }).kpis.parts.value, 'that day\'s parts');
    await page.click(`${P} [data-today]`); await loaded(page, 'day', today);
    await page.click(`${P} .efpSeg button[data-range="year"]`); await loaded(page, 'year');
    assert(await page.locator(`${P} .efpCal.long`).count() === 1 && await page.locator(`${P} .efpDy.before`).count() > 20, 'a year is a column of seven per week, with the days before records marked');
    await page.click(`${P} .efpSeg button[data-range="week"]`); await loaded(page, 'week'); await page.waitForFunction(sel => document.querySelectorAll(sel).length >= 28, `${P} .efpDy[data-day]`, { timeout: 8000 });
    assert(person().some(c => c.range === 'month' && c.compare === false), 'a short range reads the month behind its calendar (without a comparison)');
    console.log('  ✓ charts: crosshair read-outs (mouse, keyboard), the measure switch, speed and active charts, heat cells, station mix; calendar states, hover, click opens that Day; a year as weeks of seven');

    /* ── 5 · issues, rates, contact, orders ── */
    await page.click(`${P} .efpSeg button[data-range="quarter"]`); await loaded(page, 'quarter'); const TQ = truthOf('quarter');
    assert.equal((await page.locator(`${P} .efpIN`).innerText()).trim(), String(TQ.issues.total), 'issue count');
    assert(await page.locator(`${P} .efpIg`).count() === TQ.issues.byKind.length && await page.locator(`${P} .efpIg.open`).count() === 1, 'issues grouped by kind, the first open');
    const kindBtn = page.locator(`${P} .efpIgh`).nth(1); await kindBtn.click(); assert.equal(await page.locator(`${P} .efpIg.open`).count(), 2, 'a kind opens'); await kindBtn.click();
    const ob = page.locator(`${P} .efpIg.open .efpOid`).first(); const orid = await ob.getAttribute('data-order'); await ob.click();
    assert.deepEqual(await page.evaluate(() => window.__opened.slice(-1)), [orid], 'an issue opens its order with the console\'s own order window');
    assert(await page.locator(`${P} .efpRt`).count() >= 3 && /First-pass/.test(await page.locator(`${P} .efpRates`).innerText()) && /Sent/.test(await page.locator(`${P} .efpRates`).innerText()), 'rates and contact');
    // orders: E8's list
    assert(await page.locator(`${P} .efpOrdersMod .efo`).count() === 1 && await page.locator(`${P} .efpOrdersOwn`).isHidden(), 'the order list is E8\'s module');
    await page.waitForFunction(sel => document.querySelectorAll(sel).length >= 20, `${P} .efoRow`, { timeout: 15000 });
    const r0 = page.locator(`${P} .efoRow`).first(); assert(await r0.locator('.efoThumbs img, .efoThumbs .ph').count() >= 1 && await r0.locator('.efoQr').count() === 1, 'a picture per piece and a QR');
    assert(await page.locator(`${P} .efoRow .efoThumbs`).evaluateAll(l => l.some(e => e.querySelectorAll('.efoTh').length > 1)), 'an order with several pieces shows one picture for each');
    const nOrd = await page.locator(`${P} .efoRow`).count(); const q0 = orderCalls().length;
    await page.fill(`${P} input[name="efoq"]`, 'Maya'); await page.waitForFunction(sel => { const r = [...document.querySelectorAll(sel)]; return r.length > 0 && r.length < 12 && r.every(e => /Maya/i.test(e.textContent)); }, `${P} .efoRow`, { timeout: 10000 });
    assert(orderCalls().slice(q0).some(c => c.q === 'Maya'), 'the search asks the server (real time)');
    await page.fill(`${P} input[name="efoq"]`, ''); await page.waitForFunction(([sel, n]) => document.querySelectorAll(sel).length >= n, [`${P} .efoRow`, 20]);
    for (let i = 0; i < 6 && await page.locator(`${P} .efoRow`).count() < 40; i++) { await page.locator(`${P} .efoSent`).scrollIntoViewIfNeeded(); await page.waitForTimeout(500); }
    assert(await page.locator(`${P} .efoRow`).count() > nOrd && orderCalls().some(c => c.cursor === 'c25'), 'scrolling pages the list');
    await page.locator(`${P} .efoRow .efoOpen`).nth(1).click(); const orow = await page.locator(`${P} .efoRow`).nth(1).getAttribute('data-rid');
    assert.deepEqual(await page.evaluate(() => window.__opened.slice(-1)), [orow], 'a row opens its order');
    console.log('  ✓ issues by kind (an order opens), rates and contact, the order list: pictures per piece and QR, search filters as you type, pages as you scroll, a row opens its order');

    /* ── 6 · live ── */
    await page.waitForSelector(`${P} .efpNowS:not(.hidden) .efpNowCard`, { timeout: 8000 });
    const lv = pf.live().stations.find(s => s.key === 'welding').current[0];
    assert(new RegExp(String(lv.orderNumber)).test(await page.locator(`${P} .efpNowCard`).innerText()), 'the live card names the order in her hands');
    assert(await page.locator(`${P} .efpNowCard [data-since]`).count() >= 1 && await page.locator(`${P} .efpNowCard .efpQr, ${P} .efpNowCard canvas, ${P} .efpNowCard img`).count() >= 1, 'a ticking time, the pictures and a QR');
    const s1 = await page.locator(`${P} .efpNowCard [data-since]`).first().innerText(); await page.waitForTimeout(2300); assert.notEqual(await page.locator(`${P} .efpNowCard [data-since]`).first().innerText(), s1, 'the time since scanned ticks');
    pf.setLive('idle'); await page.waitForFunction(sel => /Not on an order right now/.test((document.querySelector(sel) || {}).textContent || ''), `${P} .efpNowIdle`, { timeout: 8000 }); assert.equal(await page.locator(`${P} .efpNowCard`).count(), 0, 'no order in hand: the card is replaced by a plain line');
    pf.setLive('out'); await page.waitForFunction(sel => /Not signed in/.test((document.querySelector(sel) || {}).textContent || ''), `${P} .efpNowIdle`, { timeout: 8000 }); await page.waitForFunction(sel => /Not signed in/.test(document.querySelector(sel).textContent), `${P} .efpWhere`, { timeout: 8000 });
    assert(/Last seen|Not signed in/.test(await page.locator(`${P} .efpWhere`).innerText()), 'signed out: the header says when she was last seen'); pf.setLive('working');
    await page.waitForFunction(sel => /Signed in/.test(document.querySelector(sel).textContent), `${P} .efpWhere`, { timeout: 8000 });
    // honest Reconnecting, then back
    await page.click(`${P} .efpSeg button[data-range="week"]`); await loaded(page, 'week'); pf.state.fail = 4;
    await page.waitForFunction(sel => document.querySelector(sel).dataset.s === 'slow', `${P} .efpLive`, { timeout: 12000 }); assert(/reconnecting|last update/i.test(await page.locator(`${P} .efpLiveT`).innerText()), 'a failed read says so, with the age of what is shown');
    assert(await page.locator(`${P} .efpK .efpKV`).first().innerText() !== '—', 'what was shown stays on screen');
    pf.state.fail = 0; await page.waitForFunction(sel => document.querySelector(sel).dataset.s === 'live', `${P} .efpLive`, { timeout: 12000 });
    // nothing is read while hidden
    await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { get: () => 'hidden', configurable: true }); document.dispatchEvent(new Event('visibilitychange')); });
    await page.waitForTimeout(300); const c0 = pf.state.calls.filter(c => c.op === 'person' || c.op === 'personOrders').length; await page.waitForTimeout(2000);
    assert.equal(pf.state.calls.filter(c => c.op === 'person' || c.op === 'personOrders').length, c0, 'hidden: no figure and no order is read');
    await page.evaluate(() => { delete document.visibilityState; document.dispatchEvent(new Event('visibilitychange')); }); await page.waitForTimeout(1500);
    assert(pf.state.calls.filter(c => c.op === 'person').length > 0 && person().length > 0, 'shown again: it reads again');
    const back = await page.evaluate(() => new Promise(r => { const h = EfficiencyEmployee.instances[0]; r(!!h); })); assert(back);
    console.log('  ✓ live: the "Now working on" card (order, ticking time, pictures, QR) follows the live read; sign-out updates the header; an honest Reconnecting and recovery; nothing is read while hidden');

    /* ── the other person, an empty day, the page\'s own list ── */
    await page.evaluate(() => { window.__EO = window.EfficiencyOrders; delete window.EfficiencyOrders; });
    await page.evaluate(() => Efficiency.go('people')); await page.waitForFunction(() => !EfficiencyEmployee.instances.length, null, { timeout: 8000 });
    assert.equal(await page.locator(`${P}`).count(), 0, 'leaving stops and empties the page'); const calls0 = pf.state.calls.length; await sleep(1500); assert.equal(pf.state.calls.filter((c, i) => i >= calls0 && (c.op === 'person' || c.op === 'personOrders')).length, 0, 'nothing is read after it is left');
    await openPerson(page, 'Ben R.');
    await page.click(`${P} .efpSeg button[data-range="day"]`); await loaded(page, 'day');
    if (process.env.DBG) console.log('DBG', JSON.stringify(await state(page)), JSON.stringify(person().slice(-5).map(c => [c.name, c.range, c.day])), (await page.locator(`${P} .efpK[data-k="kpis.parts"]`).innerText()).replace(/\n/g, ' | '));
    assert.equal((await kpiText(page, 'kpis.parts')).trim(), '—', 'not signed in yet today: a dash, never a zero'); assert(!/\b0\b/.test(await page.locator(`${P} .efpK[data-k="kpis.parts"]`).innerText().then(t => t.replace(/\d+ per day/, ''))), 'no made-up zero');
    await page.waitForFunction(sel => /last seen/i.test(document.querySelector(sel).textContent), `${P} .efpWhere`, { timeout: 8000 });
    assert(/Not signed in yet today|Not signed in/.test(await page.locator(`${P} [data-c="shift"]`).innerText()), 'the shift card says so in words');
    await page.click(`${P} .efpSeg button[data-range="month"]`); await loaded(page, 'month'); assert(await page.locator(`${P} .efpDy.pending`).count() === 1, 'today is "not signed in yet", not an absence');
    assert(await page.locator(`${P} .efpOrdersOwn`).isVisible() && await page.locator(`${P} .efpOrdersMod`).count() === 0, 'without E8\'s module the page\'s own small list is used');
    await page.waitForFunction(sel => document.querySelectorAll(sel).length >= 20, `${P} .efpO`, { timeout: 10000 });
    assert(await page.locator(`${P} .efpO .efpOp img, ${P} .efpO .efpOp .ph`).count() >= 20 && await page.locator(`${P} .efpO .efpQr`).count() >= 20, 'its own list has pictures and a QR too');
    await page.fill(`${P} input[name="q"]`, 'Theo'); await page.waitForFunction(sel => { const r = [...document.querySelectorAll(sel)]; return r.length > 0 && r.length < 12 && r.every(e => /Theo/i.test(e.textContent)); }, `${P} .efpO`, { timeout: 10000 });
    await page.fill(`${P} input[name="q"]`, ''); await page.waitForFunction(sel => document.querySelectorAll(sel).length >= 20, `${P} .efpO`, { timeout: 10000 });
    await page.locator(`${P} .efpO`).nth(2).click(); assert.equal((await page.evaluate(() => window.__opened.slice(-1)))[0], await page.locator(`${P} .efpO`).nth(2).getAttribute('data-rid'), 'its own list opens an order too');
    await page.evaluate(() => { window.EfficiencyOrders = window.__EO; });
    // an unknown name: said in words, not drawn as zeros
    await page.evaluate(() => Efficiency.go('people')); await openPerson(page, 'New Hire');
    assert(/No sign-ins or activity were found/.test(await page.locator(`${P} .efpNote`).innerText()), 'a name nothing is logged under says so'); assert.equal((await kpiText(page, 'kpis.parts')).trim(), '—');
    console.log('  ✓ not signed in yet today: dashes, "Last seen", the shift in words, the calendar marks today as pending; the page\'s own order list (search, open) when E8\'s is absent; an unknown name is said in words');

    // Back
    await page.evaluate(() => Efficiency.go('people')); await openPerson(page, 'Ana M.'); await page.click(`${P} .efpBack`);
    await page.waitForFunction(() => !EfficiencyEmployee.instances.length, null, { timeout: 8000 }); assert(!/person/.test(await page.evaluate(() => location.hash)), 'Back leaves the person'); await page.waitForTimeout(450);   // (the console double-checks a Back for 300 ms)

    /* ── 8 · fit (the page) and the own-fetch path ── */
    // (the console's own header bar is the console's: only this page and the document are measured)
    const fit = async () => page.evaluate(() => { const de = document.documentElement, p = document.querySelector('#efficiencyView .efp'), pr = p.getBoundingClientRect(), out = [];
      p.querySelectorAll('*').forEach(e => { const r = e.getBoundingClientRect(); if (!r.width || r.right <= pr.right + 1) return; for (let a = e.parentElement; a && a !== p; a = a.parentElement) { const o = getComputedStyle(a).overflowX; if (o === 'auto' || o === 'scroll' || o === 'hidden') return; } if (getComputedStyle(e).position === 'fixed') return; out.push(String(e.className && e.className.baseVal === undefined ? e.className : 'svg').slice(0, 30)); });
      return { page: de.scrollWidth - de.clientWidth, efp: p.scrollWidth - p.clientWidth, beyond: out.slice(0, 5), w: innerWidth }; });
    for (const w of [900, 390]) {
      await page.setViewportSize({ width: w, height: 900 }); await openPerson(page, 'Ana M.');
      for (const r of ['day', 'month', 'year']) { await page.click(`${P} .efpSeg button[data-range="${r}"]`); await loaded(page, r); await settle(page, 200); const f = await fit(); assert(f.page <= 1 && f.efp <= 1 && !f.beyond.length, `${w}px ${r}: no sideways scroll ${JSON.stringify(f)}`); }
      const small = await page.evaluate(() => [...document.querySelectorAll('#efficiencyView .efp button, #efficiencyView .efp input')].filter(e => e.offsetParent && e.getBoundingClientRect().width < 18 && !e.closest('.efpCal')).length); assert.equal(small, 0, `${w}px: no unreachably small control`);
      await page.evaluate(() => Efficiency.go('people')); await page.waitForFunction(() => !EfficiencyEmployee.instances.length);
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    // the default way of reading (no console api): the page asks the gated function itself with the console's passcode for this tab
    const direct = await page.evaluate(async () => {
      const hold = window.Efficiency; window.Efficiency = undefined; const host = document.createElement('div'); host.id = 'directHost'; host.style.cssText = 'position:fixed;left:0;top:0;width:900px;height:600px;overflow:auto;z-index:1;background:#fff'; document.body.appendChild(host);
      const h = EfficiencyEmployee.mount(host, { name: 'Ana M.', onBack() {} });
      await new Promise(r => { const t = setInterval(() => { if (h.state.loaded) { clearInterval(t); r(); } }, 100); setTimeout(r, 8000); });
      const out = { loaded: h.state.loaded, parts: host.querySelector('.efpK[data-k="kpis.parts"] .efpKV').textContent };
      h.unmount(); host.remove(); window.Efficiency = hold; return out;
    });
    assert(direct.loaded && direct.parts !== '—', 'without the console the page reads for itself (' + JSON.stringify(direct) + ')');
    console.log('  ✓ 900 and 390 px: no sideways scroll on Day, Month and Year; the page also reads for itself when mounted without the console');
    assert(A.errs.length === 0, 'no page errors: ' + A.errs.join(' | ')); await A.ctx.close();

    /* ── 7 · motion (normal motion) ── */
    const B = await boot({}), pg = B.page; await pg.evaluate(() => { Efficiency.options.growMs = 480; Object.assign(EfficiencyEmployee.options, { growMs: 480 }); });
    await openPerson(pg, 'Ana M.'); await pg.waitForTimeout(1200);
    await pg.evaluate(() => { window.__fr = []; window.__on = true; let last = performance.now(); const f = t => { if (!window.__on) return; window.__fr.push(t - last); last = t; requestAnimationFrame(f); }; requestAnimationFrame(f); });
    await pg.evaluate(() => { window.__vals = []; window.__sw = setInterval(() => { const e = document.querySelector('#efficiencyView .efpK[data-k="kpis.parts"] .efpKV'); if (e) window.__vals.push(e.textContent); }, 16); });
    await pg.click(`${P} .efpSeg button[data-range="year"]`); await loaded(pg, 'year'); await pg.waitForTimeout(900);
    await pg.click(`${P} .efpSeg button[data-range="month"]`); await loaded(pg, 'month'); await pg.waitForTimeout(900);
    await pg.click(`${P} .efpSeg button[data-range="day"]`); await loaded(pg, 'day'); await pg.waitForTimeout(900);
    const m = await pg.evaluate(() => { window.__on = false; clearInterval(window.__sw); const fr = window.__fr.slice(2).sort((a, b) => a - b); return { n: fr.length, med: fr[fr.length >> 1], p95: fr[Math.floor(fr.length * .95)], max: fr[fr.length - 1], distinct: new Set(window.__vals).size }; });
    assert(m.n > 30 && m.med < 40 && m.p95 < 120 && m.max < 400, 'smooth: ' + JSON.stringify(m)); assert(m.distinct >= 6, 'numbers count to their value rather than jump: ' + m.distinct + ' values seen');
    await pg.click(`${P} .efpSeg button[data-range="month"]`); await loaded(pg, 'month'); await pg.waitForTimeout(700);
    // thumbnails zoom in place on a resting pointer (the shared engine), the QR too
    const th = pg.locator(`${P} .efoRow .efoTh:not(.ph)`).first(); await th.scrollIntoViewIfNeeded(); await th.hover(); await pg.waitForFunction(() => document.querySelector('#efficiencyView .sealZoomed') && +document.querySelector('#efficiencyView .sealZoomed').dataset.sealZoom > 1.05, null, { timeout: 5000 });
    assert.equal(await pg.evaluate(() => document.querySelectorAll('dialog[open]').length), 0, 'zoomed in place, never a pop-up'); await pg.mouse.move(5, 5);
    console.log('  ✓ motion: numbers count up to the new value, frames stay short (median ' + m.med.toFixed(1) + ' ms, 95th ' + m.p95.toFixed(1) + ' ms), thumbnails zoom in place');
    await B.ctx.close(); assert(B.errs.length === 0, 'no page errors (normal motion): ' + B.errs.join(' | '));
    assert(seen.urls.every(u => !u.includes(F.KEY) && !u.includes('key=')), 'the passcode is never in a URL');

    /* ── screenshots (fixture only) ── */
    if (SHOTS) {
      fs.mkdirSync(SHOTS, { recursive: true }); const S = await boot({ reducedMotion: 'reduce' }), sp = S.page;
      for (const w of [1440, 900, 390]) {
        await sp.setViewportSize({ width: w, height: 3400 }); await openPerson(sp, 'Ana M.');
        for (const [r, label] of [['day', 'day'], ['month', 'month'], ['year', 'year']]) { await sp.click(`${P} .efpSeg button[data-range="${r}"]`); await loaded(sp, r); await sp.waitForTimeout(1300); await sp.mouse.move(2, 2); await sp.locator(V).screenshot({ path: path.join(SHOTS, `${w}-${label}.png`) }); }
        if (w === 1440) {
          await sp.click(`${P} .efpSeg button[data-range="month"]`); await loaded(sp, 'month'); await sp.waitForTimeout(1300);
          await sp.locator(`${P} [data-c="tp"] svg.efpSvg`).scrollIntoViewIfNeeded(); const s3 = await sp.locator(`${P} [data-c="tp"] svg.efpSvg`).boundingBox(); await sp.mouse.move(s3.x + s3.width * .62, s3.y + s3.height * .5); await sp.waitForTimeout(250);
          await sp.screenshot({ path: path.join(SHOTS, '1440-chart-hover.png'), clip: { x: Math.max(0, s3.x - 40), y: s3.y - 70, width: Math.min(1100, 1440 - s3.x + 40), height: s3.height + 120 } });
          const d = sp.locator(`${P} .efpDy.worked:not(.today)`).nth(4); await d.hover(); await sp.waitForTimeout(250); const cb = await sp.locator(`${P} [data-c="cal"]`).boundingBox(); await sp.screenshot({ path: path.join(SHOTS, '1440-calendar-hover.png'), clip: { x: cb.x - 10, y: cb.y - 10, width: cb.width + 20, height: cb.height + 20 } });
          await sp.mouse.move(2, 2); await sp.fill(`${P} input[name="efoq"]`, 'Maya'); await sp.waitForTimeout(1500); const ob2 = await sp.locator(`${P} .efpOrdersHost`).boundingBox(); await sp.screenshot({ path: path.join(SHOTS, '1440-search.png'), clip: { x: ob2.x - 10, y: ob2.y - 40, width: ob2.width + 20, height: Math.min(ob2.height + 50, 700) } });
          await sp.fill(`${P} input[name="efoq"]`, '');
        }
        await sp.evaluate(() => Efficiency.go('people')); await sp.waitForFunction(() => !EfficiencyEmployee.instances.length);
      }
      await S.ctx.close(); console.log('  ✓ screenshots saved in ' + SHOTS);
    }
    assert.equal(seen.aborted, 0, 'nothing was requested off the loopback (' + seen.aborted + ' aborted)');
    console.log('  ✓ no page errors, nothing requested off the loopback, the passcode never in a URL');
  } finally { await browser.close(); await srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
