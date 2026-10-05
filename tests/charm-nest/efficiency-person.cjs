// One employee's full page (charm-nest-efficiency-person.js), opened from the console (#efficiency/person/<name>) on the sorter.
// Fakes only: the harness answers the gated read function from tests/charm-nest/efficiency-person-fixture.cjs (two invented people, a
// fake passcode, a year of invented days) and, for the console's own reads, from efficiency-fixture.cjs; every other request off the
// loopback is aborted, so nothing reaches the internet, Etsy or Firestore.
//   1 · the page's own logic (periods, a missing figure stays a dash, the answer's shapes)
//   2 · opening it: labelled spinner, header (name, where signed in now, stations), the live light, Back
//   3 · every range (Day Week Month 3 months Year Custom) reads what it should, figures agree with the fixture, the change against the
//       period before (and "no data" where there is none), hover definitions say whether a figure is estimated
//   4 · charts (E7's library): hover read-outs (mouse and keyboard), the measure switch, heat and station mix, the calendar (states, hover, a click opens that Day)
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
  assert(N2.src.att.daysWorked && N2.src.att.medianStart && N2.src.rates.firstPass && N2.src.rates.rescanRate && N2.src.contact.sent && N2.src.contact.medianFirstReplyMs && N2.issues.byKind.length === 13, 'attendance, the seven rates, contact and the 13 issue kinds are read');
  const OLD = j(E.norm(fx.person({ op: 'person', name: 'Ana M.', range: { from: F.addDays(fx.today, -180), to: F.addDays(fx.today, -170) }, compare: false }), { name: 'Ana M.' }));
  assert.equal(OLD.issues.byKind.find(k => k.kind === 'reprint').count, null, 'a kind that was not counted on those days is a dash, never a 0'); assert(typeof OLD.issues.byKind.find(k => k.kind === 'undone').count === 'number', 'an exact kind is counted'); assert.equal(OLD.issues.byKind[OLD.issues.byKind.length - 1].count, null, 'the dashes come last');
  assert.equal(N2.src.att.avgShiftHours.est, true, 'an estimated figure says so'); assert(/time clock/.test(N2.attNote), 'the honest sentence about days off is kept');
  const L = j(E.pickLive({ at: 5, signedIn: [{ name: 'Ana M.', stationKey: 'welding', since: 1, lastSeenAt: 2 }], stations: [{ key: 'welding', label: 'Welding', current: [{ person: 'Ana M.', rid: '35210001', station: 'welding' }, { person: 'Someone Else', rid: '9' }] }] }, 'Ana M.'));
  assert.equal(L.where.stationKey, 'welding'); assert.deepEqual(L.current.map(c => c.rid), ['35210001'], 'only this person\'s order is in the card'); assert.equal(L.current[0].stationLabel, 'Welding');
  const snapA = { at: 5, signedIn: [{ name: 'Ana M.', stationKey: 'welding', since: 1, lastSeenAt: 2 }], stations: [{ key: 'welding', label: 'Welding', current: [{ person: 'Ana M.', rid: '35210001', station: 'welding' }] }] };
  assert.equal(j(E.pickLive(snapA, 'Ana Maria')).where, null, 'another spelling alone is not recognised'); assert.equal(j(E.pickLive(snapA, 'Ana Maria', ['Ana Maria', 'Ana M.'])).where.stationKey, 'welding', 'a spelling the server lists (E4 spellings) finds the person on the live board');
  console.log('  ✓ periods are rolling windows, a missing figure stays a dash, the answer\'s shapes are read, the live card picks this person only (under any spelling the server lists)');
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
  // the sorter keeps a 344 px rail on the left; on a phone it is folded away with its own button (as the console's own test does)
  const rail = (page, off) => page.evaluate(o => { const app = document.getElementById('app'); if (app.classList.contains('railOff') !== o) document.getElementById('btnRail').click(); }, off);
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
    pf.setDelay(b => (b.op === 'person' && b.range === 'week' ? 2500 : 0));
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
      if (T.kpis.parts.prev == null) assert(/no data/.test(dl) && !/[▲▼]/.test(dl), `${key}: nothing to compare with says so (${dl})`); else assert(/([▲▼] \d+%|no change)/.test(dl) && /vs/.test(dl), `${key}: the change against the period before (${dl})`);
      if (key === 'day') { assert.equal(await page.locator(`${P} [data-c="shift"]`).isVisible(), true, 'Day shows the shift'); assert.equal(await page.locator(`${P} [data-c="sp"]`).isVisible(), false); }
      else assert.equal(await page.locator(`${P} [data-c="sp"]`).isVisible(), true);
      const bars = await page.locator(`${P} [data-c="tp"] .efc-bar`).count();
      if (key === 'year') assert.equal(bars, T.series.length, 'a year is drawn one bar a week (' + bars + ')'); else if (key !== 'day') assert.equal(bars, T.series.length, key + ' one bar a day'); else assert(bars >= 8, 'Day is by the hour (up to the hour it is now)');
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
    // keyboard: a group of figures is one Tab stop, and the arrow keys, Home and End move inside it
    assert.equal(await page.locator(`${P} .efpK[tabindex="0"]`).count(), 6, 'one Tab stop for each of the six groups of figures');
    const kf = page.locator(`${P} .efpK[tabindex="0"]`).first(), kk0 = await kf.getAttribute('data-k'); await kf.focus(); await page.keyboard.press('ArrowRight');
    const kk1 = await page.evaluate(() => (document.activeElement && document.activeElement.dataset ? document.activeElement.dataset.k : '')); assert(kk1 && kk1 !== kk0, 'an arrow key moves to the next figure of the group');
    await page.keyboard.press('End'); const kk2 = await page.evaluate(() => document.activeElement.dataset.k); await page.keyboard.press('Home'); assert.equal(await page.evaluate(() => document.activeElement.dataset.k), kk0, 'Home goes back to the first figure'); assert(kk2 !== kk0, 'End goes to the last');
    assert.equal(await page.locator(`${P} .efpK[tabindex="0"]`).count(), 6, 'still one stop for each group'); assert.equal(await page.locator(`${P} .efpRt[tabindex="0"]`).count(), 1, 'the rates are one Tab stop too');
    await page.evaluate(() => document.activeElement && document.activeElement.blur());
    console.log('  ✓ every range (Day, Week, Month, 3 months, Year, Custom) reads its window and agrees with the fixture; earlier/later/Today; change vs the period before, "no data" where none; hover definitions with counted/estimated');

    /* ── 4 · charts, calendar (E7's EfficiencyCharts draws them; the page hands over the data) ── */
    await page.click(`${P} .efpSeg button[data-range="month"]`); await loaded(page, 'month'); await settle(page, 500);
    const CH = c => `${P} [data-c="${c}"]`, TIP = c => `${CH(c)} .efc-tip[data-open]`, mid = async (loc, fx = .5, fy = .5) => { await loc.evaluate(e => e.scrollIntoView({ block: 'center', inline: 'nearest' })); await page.waitForTimeout(60); const r = await loc.boundingBox(); return { x: r.x + r.width * fx, y: r.y + r.height * fy, r }; };
    assert.equal(await page.locator(`${P} .efpSvg`).count(), 0, 'the page draws no chart of its own: the library does'); assert(await page.locator(`${P} .efpK[data-k="kpis.parts"] .efpKsp svg`).count() === 1, 'a sparkline on the pieces card');
    const svg = page.locator(`${CH('tp')} svg.efc-svg`); const m1 = await mid(svg, .6);
    await page.mouse.move(m1.x, m1.y); await page.waitForSelector(TIP('tp'));
    let tip = (await page.locator(TIP('tp')).innerText()).replace(/\s+/g, ' ');
    assert(/(piece|Nothing logged)/i.test(tip) && /[A-Z][a-z]{2}/.test(tip), 'the read-out names the day and the figure: ' + tip); assert(/Click to open this day|Nothing logged/.test(tip), 'a day says it opens: ' + tip);
    await page.click(`${P} [data-metric="orders"]`); const m2 = await mid(svg, .62); await page.mouse.move(m2.x, m2.y); await page.waitForSelector(TIP('tp')); tip = await page.locator(TIP('tp')).innerText(); assert(/order|Nothing logged/i.test(tip), 'the measure switch changes the read-out: ' + tip);
    await page.click(`${P} [data-metric="parts"]`);
    await page.focus(`${CH('tp')} svg.efc-svg`); await page.keyboard.press('ArrowLeft'); assert(await page.locator(TIP('tp')).count() === 1, 'keyboard arrows walk the read-out'); await page.keyboard.press('Escape'); await page.mouse.move(2, 2);
    for (const c of ['sp', 'tm']) { const s2 = await mid(page.locator(`${CH(c)} svg.efc-svg`), .4); await page.mouse.move(s2.x, s2.y); await page.waitForSelector(TIP(c)); }
    assert(/Signed in/.test(await page.locator(`${CH('tm')} .efc-legend`).innerText()) && /Active/.test(await page.locator(`${CH('tm')} .efc-legend`).innerText()), 'the time chart names its two bars');
    const mh = await mid(page.locator(`${CH('heat')} svg.efc-svg`), .5, .1); await page.mouse.move(mh.x, mh.y + 12); await page.waitForSelector(TIP('heat')); assert(/PM|AM/.test(await page.locator(TIP('heat')).innerText()), 'a heat cell names its hour');
    await page.locator(`${CH('mix')} .efc-dli`).first().hover(); await page.waitForSelector(TIP('mix')); assert(/Share/.test(await page.locator(TIP('mix')).innerText()), 'the station mix answers the pointer'); await page.mouse.move(2, 2);
    // the calendar
    const kinds = await page.evaluate(() => { const o = {}; document.querySelectorAll('#efficiencyView [data-c="cal"] .efc-day').forEach(g => { const k = g.getAttribute('data-state'); o[k] = (o[k] || 0) + 1; }); return o; });
    assert(kinds.worked >= 10 && kinds.off >= 1 && kinds.closed >= 4, 'the calendar shows worked, off and closed days: ' + JSON.stringify(kinds));
    const wd = page.locator(`${CH('cal')} .efc-day.worked:not(.today)`).nth(2), wday = await wd.getAttribute('data-day'), wp = await mid(wd);
    await page.mouse.move(wp.x, wp.y); await page.waitForSelector(TIP('cal')); const ct2 = (await page.locator(TIP('cal')).innerText()).replace(/\s+/g, ' '); assert(/Signed in/.test(ct2) && /Pieces/.test(ct2) && /Click to open this day/.test(ct2), 'calendar hover: ' + ct2);
    await page.mouse.click(wp.x, wp.y); await loaded(page, 'day', wday); assert.equal((await state(page)).from, wday, 'a click on a day jumps to that Day');
    assert(/^(?!Today)/.test(await page.locator(`${P} .efpDay`).innerText()) && await page.locator(`${P} [data-c="shift"]`).isVisible(), 'the Day view of that day');
    assert.equal(nums(await kpiText(page, 'kpis.parts')), truthOf('day', { day: wday }).kpis.parts.value, 'that day\'s parts');
    await page.click(`${P} [data-today]`); await loaded(page, 'day', today);
    await page.click(`${P} .efpSeg button[data-range="year"]`); await loaded(page, 'year'); await settle(page, 500);
    assert(/Counted on \d+ of 365 days/.test(await page.locator(`${P} .efpIs`).innerText()), 'a year that is only partly counted says so (and does not guess the rest)'); assert(/\d+ of 365 days/i.test(await page.locator(`${P} .efpK[data-k="issues.issues"] .efpKT`).innerText()), 'the issues card carries the same words');
    assert(await page.locator(`${CH('cal')} .efc-mt`).count() >= 12 && await page.locator(`${CH('cal')} .efc-day[data-state="before"]`).count() > 20, 'a year is its months, with the days before sign-in logging began (and before records) marked, not counted');
    const bb0 = await mid(page.locator(`${CH('cal')} .efc-day[data-state="before"]`).first()); await page.mouse.move(bb0.x, bb0.y); try { await page.waitForSelector(TIP('cal'), { timeout: 4000 }); } catch (e) { throw new Error('no card on an early day: ' + JSON.stringify(await page.evaluate(p => { const e = document.elementFromPoint(p.x, p.y); return { at: e ? e.tagName + '.' + (e.getAttribute('class') || '') + ' ' + (e.closest('.efc-day') ? e.closest('.efc-day').getAttribute('data-day') : '') : null, p, vh: innerHeight, open: [...document.querySelectorAll('.efc-tip[data-open]')].map(t => t.parentElement.parentElement.getAttribute('data-c') || t.parentElement.dataset.chart) }; }, { x: bb0.x, y: bb0.y }))); } assert(/Before/i.test(await page.locator(TIP('cal')).innerText()), 'a day before records says so'); await page.mouse.move(2, 2);
    await page.click(`${P} .efpSeg button[data-range="week"]`); await loaded(page, 'week'); await page.waitForFunction(sel => document.querySelectorAll(sel).length >= 28, `${CH('cal')} .efc-day`, { timeout: 8000 });
    assert(person().some(c => c.range === 'month' && c.compare === false), 'a short range reads the month behind its calendar (without a comparison)');
    console.log('  ✓ charts (the shared library): hover read-outs (mouse, keyboard), the measure switch, speed and active charts with their legends, heat cells, station mix, sparklines; calendar states, hover, click opens that Day; a year as months');

    /* ── 5 · issues, rates, contact, orders ── */
    await page.click(`${P} .efpSeg button[data-range="quarter"]`); await loaded(page, 'quarter'); const TQ = truthOf('quarter');
    assert.equal((await page.locator(`${P} .efpIN`).innerText()).trim(), String(TQ.issues.total), 'issue count');
    assert(await page.locator(`${P} .efpIg`).count() === TQ.issues.byKind.filter(k => k.count > 0).length && await page.locator(`${P} .efpIg.open`).count() === 1, 'issues grouped by kind (only kinds that have some), the first open');
    assert(await page.locator(`${P} .efpIg .efpAt.own`).count() >= 1 && await page.locator(`${P} .efpIg .efpAt.order, ${P} .efpIg .efpAt.system`).count() >= 1, 'who each kind is about is shown beside it'); assert(/None logged:/.test(await page.locator(`${P} .efpIs`).innerText()) || TQ.issues.byKind.every(k => k.count > 0), 'kinds with none are named once, not drawn as empty rows');
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
    assert(await page.locator(`${P} .efoRow`).count() > nOrd && orderCalls().some(c => /^[oc]25$/.test(c.cursor || '')), 'scrolling pages the list');
    await page.locator(`${P} .efoRow .efoOpen`).nth(1).click(); const orow = await page.locator(`${P} .efoRow`).nth(1).getAttribute('data-rid');
    assert.deepEqual(await page.evaluate(() => window.__opened.slice(-1)), [orow], 'a row opens its order');
    console.log('  ✓ issues by kind (an order opens), rates and contact, the order list: pictures per piece and QR, search filters as you type, pages as you scroll, a row opens its order');
    // the list follows the date chips (E8's own From / To fields are off); "Show all time" lifts the range; Real <-> Sandbox mounts a fresh page
    const until = async (fn, what, ms = 8000) => { for (let t = 0; t < ms; t += 100) { if (fn()) return; await sleep(100); } throw new Error('timed out: ' + what); };
    assert(orderCalls().some(c => c.from === TQ.from && c.to === TQ.to), 'the order list asks for the days the chips show'); assert.equal(await page.locator(`${P} .efo input[type="date"]:visible`).count(), 0, 'no second pair of date fields');
    const o1 = orderCalls().length; await page.click(`${P} [data-orders-all]`); await until(() => orderCalls().slice(o1).some(c => !c.from && !c.to), 'all time'); assert(/All time/.test(await page.locator(`${P} .efpLr`).innerText()), 'the heading says all time');
    const o2 = orderCalls().length; await page.click(`${P} [data-orders-all]`); await until(() => orderCalls().slice(o2).some(c => c.from === TQ.from && c.to === TQ.to), 'back to the period');
    const o3 = orderCalls().length; await page.click(`${P} .efpSeg button[data-range="month"]`); await loaded(page, 'month'); const TM = truthOf('month'); await until(() => orderCalls().slice(o3).some(c => c.from === TM.from && c.to === TM.to), 'the list follows a new chip');
    await page.click(`${P} .efpSeg button[data-range="quarter"]`); await loaded(page, 'quarter');
    const swap = async (view) => {
      await page.click(`${V} .efView button[data-view="${view}"]`);
      await page.waitForFunction(() => EfficiencyEmployee.instances.length === 1 && EfficiencyEmployee.instances[0].state.loaded && document.querySelectorAll('#efficiencyView .efpOrdersMod .efoRow').length > 0, null, { timeout: 20000 });
      assert.equal(await page.locator(`${V} .efp`).count(), 1, view + ': one page, not two'); assert.equal(await page.locator(`${P} .efpOrdersMod .efo`).count(), 1, view + ': one order list, the old one is gone');
      await sleep(700); const c1 = pf.state.calls.length; await sleep(2500); const got = pf.state.calls.slice(c1).filter(c => c.op === 'person' || c.op === 'personOrders');
      assert(got.some(c => c.op === 'person') && got.every(c => (c.sandbox === true) === (view === 'sandbox')), view + ': every read (figures and orders) is for ' + view + ' only: ' + JSON.stringify(got.map(c => [c.op, c.sandbox])));
    };
    await swap('sandbox'); await swap('real');
    console.log('  ✓ the order list follows the date chips (and "Show all time"), no second pair of date fields; a Real / Sandbox switch destroys the page and its list and mounts a fresh one that reads only that side');

    /* ── 6 · live ── */
    const CARD = `${P} .efpNow .esCard, ${P} .efpNow .efpNowCard`, TICK = `${P} .efpNow .esT, ${P} .efpNow [data-since]`;
    await page.waitForSelector(`${P} .efpNowS:not(.hidden) .esCard`, { timeout: 8000 });   // E6's shared order card, the same one the stations board draws
    const lv = pf.live().stations.find(s => s.key === 'welding').current[0];
    assert(new RegExp(String(lv.orderNumber)).test(await page.locator(CARD).first().innerText()), 'the live card names the order in her hands');
    assert(await page.locator(`${P} .efpNow .esTh, ${P} .efpNow img`).count() >= 1 && await page.locator(`${P} .efpNow .esQr, ${P} .efpNow canvas, ${P} .efpNow .efpQr`).count() >= 1, 'the pictures and a QR');
    const s1 = await page.locator(TICK).first().innerText(); await page.waitForTimeout(2300); assert.notEqual(await page.locator(TICK).first().innerText(), s1, 'the time since scanned ticks');
    await page.locator(`${P} .efpNow .esCard`).first().evaluate(e => { e.__keep = 1; }); await page.waitForTimeout(700);
    assert.equal(await page.locator(`${P} .efpNow .esCard`).first().evaluate(e => e.__keep), 1, 'the same card stays in the page across live reads (changed in place, not redrawn)'); assert.equal(await page.locator(`${P} .efpNow .esCard`).count(), 1, 'one order in hand: one card');
    pf.setLive('idle');
    await page.waitForFunction(sel => /Done in|Done|Left/.test((document.querySelector(sel) || {}).textContent || ''), `${P} .efpNow .esCard`, { timeout: 8000 });   // the card says how long it took, stays a moment ...
    await page.waitForFunction(sel => /Not on an order right now/.test((document.querySelector(sel) || {}).textContent || ''), `${P} .efpNowIdle`, { timeout: 8000 }); assert.equal(await page.locator(CARD).count(), 0, 'no order in hand: the card has folded away and a plain line is shown');
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
    await page.click(`${P} .efpSeg button[data-range="month"]`); await loaded(page, 'month'); await page.waitForSelector(`${P} [data-c="cal"] .efc-day`, { timeout: 8000 }); assert(await page.locator(`${P} [data-c="cal"] .efc-day[data-state="pending"]`).count() === 1, 'today is "not signed in yet", not an absence');
    assert(await page.locator(`${P} .efpOrdersOwn`).isVisible() && await page.locator(`${P} .efpOrdersMod`).count() === 0, 'without E8\'s module the page\'s own small list is used');
    await page.waitForFunction(sel => document.querySelectorAll(sel).length >= 20, `${P} .efpO`, { timeout: 10000 });
    assert(await page.locator(`${P} .efpO .efpOp img, ${P} .efpO .efpOp .ph`).count() >= 20 && await page.locator(`${P} .efpO .efpQr`).count() >= 20, 'its own list has pictures and a QR too');
    await page.fill(`${P} input[name="q"]`, 'Theo'); await page.waitForFunction(sel => { const r = [...document.querySelectorAll(sel)]; return r.length > 0 && r.length < 12 && r.every(e => /Theo/i.test(e.textContent)); }, `${P} .efpO`, { timeout: 10000 });
    await page.fill(`${P} input[name="q"]`, ''); await page.waitForFunction(sel => document.querySelectorAll(sel).length >= 20, `${P} .efpO`, { timeout: 10000 });
    await page.locator(`${P} .efpO`).nth(2).click(); assert.equal((await page.evaluate(() => window.__opened.slice(-1)))[0], await page.locator(`${P} .efpO`).nth(2).getAttribute('data-rid'), 'its own list opens an order too');
    await page.evaluate(() => { window.EfficiencyOrders = window.__EO; });
    // without E6's shared card the page draws its own small one (a ticking time, pictures, a QR, a click opens the order)
    await page.evaluate(() => { window.__OC = EfficiencyStations.orderCard; EfficiencyStations.orderCard = undefined; }); pf.setLive('working', 'Ben R.');
    await page.waitForSelector(`${P} .efpNowS:not(.hidden) .efpNowCard`, { timeout: 8000 }); assert.equal(await page.locator(`${P} .efpNow .esCard`).count(), 0, 'no shared card: the page\'s own is used');
    assert(await page.locator(`${P} .efpNowCard [data-since]`).count() >= 1 && await page.locator(`${P} .efpNowCard .efpQr, ${P} .efpNowCard canvas, ${P} .efpNowCard img`).count() >= 1, 'its own card has a ticking time, the pictures and a QR');
    await page.locator(`${P} .efpNowCard .efpOid`).click(); assert((await page.evaluate(() => window.__opened.slice(-1)))[0], 'its own card opens the order');
    pf.setLive('idle', 'Ben R.'); await page.waitForFunction(sel => /Not on an order right now/.test((document.querySelector(sel) || {}).textContent || ''), `${P} .efpNowIdle`, { timeout: 8000 });
    await page.evaluate(() => { EfficiencyStations.orderCard = window.__OC; }); pf.setLive('working');
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
      await page.setViewportSize({ width: w, height: 900 }); await rail(page, w < 600); await openPerson(page, 'Ana M.');
      for (const r of ['day', 'month', 'year']) { await page.click(`${P} .efpSeg button[data-range="${r}"]`); await loaded(page, r); await settle(page, 200); const f = await fit(); assert(f.page <= 1 && f.efp <= 1 && !f.beyond.length, `${w}px ${r}: no sideways scroll ${JSON.stringify(f)}`); }
      const small = await page.evaluate(() => [...document.querySelectorAll('#efficiencyView .efp button, #efficiencyView .efp input')].filter(e => e.offsetParent && e.getBoundingClientRect().width < 18 && !e.closest('.efc')).length); assert.equal(small, 0, `${w}px: no unreachably small control`);
      await page.evaluate(() => Efficiency.go('people')); await page.waitForFunction(() => !EfficiencyEmployee.instances.length);
    }
    await page.setViewportSize({ width: 1440, height: 900 }); await rail(page, false);
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
    // frames: judged strictly only when the machine itself is calm (an empty page is timed first); a shared, busy machine would fail any honest page
    const cal = await B.ctx.newPage(); await cal.goto('about:blank'); const base = await cal.evaluate(() => new Promise(r => { const fr = []; let last = performance.now(), n = 0; const f = t => { fr.push(t - last); last = t; if (++n < 50) requestAnimationFrame(f); else { fr.shift(); fr.sort((a, b) => a - b); r({ med: fr[fr.length >> 1], p95: fr[Math.floor(fr.length * .95)] }); } }; requestAnimationFrame(f); })); await cal.close();
    const calm = base.p95 < 40; if (!calm) console.log('  (machine busy: an empty page alone shows 95th ' + base.p95.toFixed(0) + ' ms per frame, so the strict frame gate is skipped)');
    assert(m.n > 20, 'frames were measured: ' + JSON.stringify(m)); assert(m.med < 500, 'the page is not frozen while switching ranges: ' + JSON.stringify(m)); if (calm && (m.med >= 40 || m.p95 >= 200)) console.log('  (note: slow frames on this machine, software drawing: ' + JSON.stringify(m) + ')'); assert(m.distinct >= (calm ? 6 : 3), 'numbers count to their value rather than jump: ' + m.distinct + ' values seen');
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
      // the console scrolls inside its own box, so the window is made as tall as the whole page before a full-page picture (else the lower part is not painted)
      // (the order list pages in more rows whenever its end is on screen, so the pictures show its first rows only: the page keeps one steady height)
      const fit = async w => { await sp.addStyleTag({ content: '#efficiencyView .efoRow:nth-child(n+7){display:none!important}#efficiencyView .efoSent{display:none!important}' }).catch(() => {}); await sp.setViewportSize({ width: w, height: 900 }); await sp.waitForTimeout(500); const sh = await sp.evaluate(() => document.querySelector('#efficiencyView').scrollHeight); await sp.setViewportSize({ width: w, height: Math.min(15000, sh + 80) }); await sp.waitForTimeout(600); };
      for (const w of [1440, 900, 390]) {
        await sp.setViewportSize({ width: w, height: 3400 }); await rail(sp, w < 600); await openPerson(sp, 'Ana M.');
        for (const [r, label] of [['day', 'day'], ['month', 'month'], ['year', 'year']]) { await sp.click(`${P} .efpSeg button[data-range="${r}"]`); await loaded(sp, r); await sp.waitForTimeout(1300); await sp.mouse.move(2, 2); await fit(w); await sp.locator(V).screenshot({ path: path.join(SHOTS, `${w}-${label}.png`) }); }
        if (w === 1440) {
          await sp.click(`${P} .efpSeg button[data-range="month"]`); await loaded(sp, 'month'); await sp.waitForTimeout(1300);
          await sp.locator(`${P} [data-c="tp"] svg.efc-svg`).scrollIntoViewIfNeeded(); const s3 = await sp.locator(`${P} [data-c="tp"] svg.efc-svg`).boundingBox(); await sp.mouse.move(s3.x + s3.width * .62, s3.y + s3.height * .5); await sp.waitForTimeout(250);
          await sp.screenshot({ path: path.join(SHOTS, '1440-chart-hover.png'), clip: { x: Math.max(0, s3.x - 40), y: s3.y - 70, width: Math.min(1100, 1440 - s3.x + 40), height: s3.height + 120 } });
          const d = sp.locator(`${P} [data-c="cal"] .efc-day.worked:not(.today)`).nth(4); await d.scrollIntoViewIfNeeded(); const db = await d.boundingBox(); await sp.mouse.move(db.x + db.width / 2, db.y + db.height / 2); await sp.waitForTimeout(250); const cb = await sp.locator(`${P} [data-c="cal"]`).boundingBox(); await sp.screenshot({ path: path.join(SHOTS, '1440-calendar-hover.png'), clip: { x: cb.x - 10, y: cb.y - 10, width: cb.width + 20, height: cb.height + 20 } });
          const kh = sp.locator(`${P} .efpK:not(.hidden)`).nth(4); await kh.scrollIntoViewIfNeeded(); const kb = await kh.boundingBox(); await sp.mouse.move(kb.x + kb.width / 2, kb.y + kb.height / 2); await sp.waitForTimeout(400);
          await sp.screenshot({ path: path.join(SHOTS, '1440-kpi-hover.png'), clip: { x: Math.max(0, kb.x - 20), y: Math.max(0, kb.y - 200), width: Math.min(700, 1440 - kb.x + 20), height: kb.height + 260 } });
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
