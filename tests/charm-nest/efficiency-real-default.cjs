// Employee efficiency: the REAL stations are what the screen shows by default, even when the sorter itself is in Sandbox mode.
// (Why this exists: Paul, 5 Oct: "I'm not seeing any of the live logins from any of my employees, I'm not seeing any metrics". The sorter
// was in Sandbox mode, the old console followed it and read the Sandbox_ copies: one person, Paul, 29 parts, 25 orders.)
// Fakes only: invented people, a fake passcode, no network. The browser part is served by tests/charm-nest/efficiency-fixture.cjs
// (two stores, like the server: real = the crew, `sandbox:true` = Paul alone); every request off the loopback is aborted.
//   1 · the REAL function over an in-memory Firestore seeded with BOTH stores: no flag = the crew only, sandbox:true = Paul only
//   2 · the sorter in Sandbox mode: the console opens on Real, asks without the flag, lists who is signed in and what they work on
//   3 · it keeps reading (live every ~0.3 s here), pauses when the page is hidden or another Workspace tab is open, catches up with a label
//   4 · Sandbox is a choice: only the Sandbox copies, never mixed back, a slow answer from the other side is dropped
//   5 · Overview / Stations / People / a person: addresses, Back, the two modules mounted when shown and destroyed when left
//   6 · honest states: nobody signed in, a service without the live read yet, a read that hangs ("Reconnecting"), 390 px wide
//   node tests/charm-nest/efficiency-real-default.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict'), Module = require('module');
const root = path.join(__dirname, '../..');
const F = require('./efficiency-fixture.cjs');

/* ── 1 · the real function, both stores in one in-memory Firestore ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const val = v => (v instanceof Ts ? v.m : v), kind = v => (v instanceof Ts ? 'ts' : typeof v);
const colls = new Map(), data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
function query(name, filters, order, lim) {
  return {
    where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim), orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim), limit: n => query(name, filters, order, n),
    get: async () => {
      let docs = [...data(name)].map(([id, d]) => ({ id, d }));
      for (const [f, op, v] of filters) docs = docs.filter(({ d }) => { const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false; const a = val(x), b = val(v); return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false; });
      if (order) docs = docs.filter(({ d }) => d[order[0]] !== undefined).sort((p, q) => (val(p.d[order[0]]) < val(q.d[order[0]]) ? -1 : val(p.d[order[0]]) > val(q.d[order[0]]) ? 1 : 0) * (order[1] === 'desc' ? -1 : 1));
      if (lim != null) docs = docs.slice(0, lim);
      return { docs: docs.map(({ id, d }) => ({ id, data: () => d })), size: docs.length, empty: !docs.length };
    }
  };
}
const db = { collection: name => Object.assign(query(name, [], null, null), { doc: id => ({ id, get: async () => { const d = data(name).get(id); return { exists: !!d, data: () => d }; }, set: async v => { data(name).set(id, v); } }) }) };
const fakeAdmin = { firestore: Object.assign(() => ({}), { Timestamp: Ts, FieldValue: { serverTimestamp: () => 'ts' } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const mod = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
Module._load = realLoad;
const T = mod._t, PASS = 'synthetic-pass-ef-default';
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
const NOW = Date.parse('2026-10-02T19:42:00Z'), MIN = 60000, realNow = Date.now;
const answer = async body => { Date.now = () => NOW; process.env.EDIT_PASSCODE = PASS; EP.resetCache(); const r = await T.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.9' }, body: JSON.stringify(Object.assign({ key: PASS }, body)) }, db); delete process.env.EDIT_PASSCODE; EP.resetCache(); Date.now = realNow; return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
const stat = o => Object.assign({ scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0 }, o);
const hrs = (map, station) => Object.fromEntries(Object.entries(map).map(([h, parts]) => [String(h).padStart(2, '0'), { parts, scans: parts, by: { [station]: { parts, scans: parts } } }]));
const at = (h, m) => Date.parse('2026-10-02T04:00:00Z') + (h * 60 + m) * MIN;   // New York clock time, 2 Oct
const touch = (n, from, st) => Object.fromEntries(Array.from({ length: n }, (_, i) => [String(3521000000 + from + i), { [st]: true }]));
function seed() {
  // the shop's real stations (no flag on any document) ...
  [['Giovanna', 'welding', { 8: 40, 9: 50, 10: 60 }, 30, [7, 52]], ['Anna', 'assembly', { 8: 20, 9: 30, 10: 40 }, 20, [8, 3]]].forEach(([name, st, h, orders, inn], i) => {
    const parts = Object.values(h).reduce((a, b) => a + b, 0);
    data('Efficiency_Daily').set('2026-10-02__' + name, { day: '2026-10-02', person: name, v: 1, events: 40, firstAt: at(...inn), lastAt: at(15, 40), stations: { [st]: stat({ scans: parts, scanParts: parts, completes: 10, parts, orders, activeMs: 300 * MIN, idleMs: 30 * MIN, firstAt: at(...inn), lastAt: at(15, 40) }) }, hours: hrs(h, st), touched: touch(orders, i * 100, st) });
    data('Station_Sessions').set('s-' + name, { id: 's-' + name, person: name, station: st, device: st + '-1', computerId: 'pc-' + name, computerLabel: '', startAt: at(...inn), lastSeenAt: NOW - 2 * MIN, endAt: null, endReason: null, minutes: null });
  });
  // ... and the sorter's Sandbox copies (Paul alone, at the sorter: 29 parts, 25 orders), every document flagged sandbox:true
  data('Sandbox_Efficiency_Daily').set('2026-10-02__Paul', { sandbox: true, day: '2026-10-02', person: 'Paul', v: 1, events: 25, firstAt: at(0, 40), lastAt: at(12, 10), stations: { sorter: stat({ scans: 0, scanParts: 0, completes: 25, parts: 29, orders: 25, activeMs: 5 * MIN, idleMs: 700 * MIN, firstAt: at(0, 40), lastAt: at(12, 10) }) }, hours: hrs({ 8: 19, 9: 3, 10: 2, 11: 2, 12: 3 }, 'sorter'), touched: touch(25, 9000, 'sorter') });
  data('Sandbox_Station_Sessions').set('s-Paul', { sandbox: true, id: 's-Paul', person: 'Paul', station: 'sorter', device: 'sorter-1', computerId: 'pc-Paul', computerLabel: '', startAt: at(0, 40), lastSeenAt: NOW - MIN, endAt: null, endReason: null, minutes: null });
}
seed();

(async () => {
  const names = r => r.body.people.map(p => p.name).sort();
  const real = await answer({ op: 'overview' }), sand = await answer({ op: 'overview', sandbox: true });
  assert.equal(real.status, 200, JSON.stringify(real.body).slice(0, 300)); assert.equal(sand.status, 200, JSON.stringify(sand.body).slice(0, 300));
  assert.deepEqual(names(real), ['Anna', 'Giovanna'], 'no flag: the real crew, and not Paul of the Sandbox');
  assert.deepEqual(names(sand), ['Paul'], 'sandbox:true: the Sandbox copies only');
  assert.equal(sand.body.business.totals.parts, 29); assert.equal(sand.body.business.totals.orders, 25); assert.equal(sand.body.business.totals.people, 1);
  assert.equal(real.body.business.totals.parts, 90, 'R2: the Welding station adds no pieces (Giovanna is seeded there); the crew total is the pieces of the throughput stations'); assert(real.body.business.totals.orders >= 20, 'the real orders (Assembly only): ' + real.body.business.totals.orders); assert.equal(real.body.business.totals.people, 2);
  assert(!JSON.stringify(real.body).includes('Paul') && !JSON.stringify(sand.body).includes('Giovanna'), 'neither store leaks into the other');
  console.log('  ✓ the real function: no flag = the crew (90 parts, Welding adds none, 2 people), sandbox:true = Paul alone (29 parts, 25 orders, 1 person), nothing crosses');

  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fx = F.make(), seen = { aborted: 0 };
  const V = '#efficiencyView', sleep = ms => new Promise(r => setTimeout(r, ms));
  const wire = async ctx => {
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url());
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      if (u.pathname.endsWith('/employeeEfficiency')) {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        const r = fx.answer(b, route.request().headers());
        if (fx.delay) await sleep(fx.delay);
        try { return await route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) }); } catch (_) { return; }   // (the page gave up on a slow one)
      }
      return route.continue();
    });
    // the sorter itself is in SANDBOX mode (Settings > Sandbox on): the very thing that used to switch the console to the Sandbox copies
    await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'on' })); } catch (_) {} window.confirm = () => true; window.alert = () => {}; });
  };
  const track = page => { const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error' && !/status of (400|503)/.test(m.text()) && !(/status of 409/.test(m.text()) && /charmNestLibrary/.test((m.location() && m.location().url) || ''))) errs.push(m.text() + ' @ ' + ((m.location() && m.location().url) || '')); }); return errs; };   // (the browser's own notice for the answers this test forces; and the library's "another tab holds it" 409 when a second sorter tab opens, which is the sorter's, not the console's)
  const names2 = page => page.locator(`${V} .efP .efName`).allInnerTexts().then(a => a.map(s => s.trim()).sort());
  const chips = page => page.locator(`${V} .efSi .efSiNm`).allInnerTexts().then(a => a.map(s => s.trim()).sort());
  const kpi = (page, k) => page.locator(`${V} .efKpi[data-k="${k}"] .efKV`).innerText().then(s => s.replace(/,/g, ''));
  const live = page => page.locator(`${V} .efLiveT`).innerText();
  const hash = page => page.evaluate(() => location.hash);
  const waitFor = (page, fn, arg, ms) => page.waitForFunction(fn, arg, { timeout: ms || 8000 });
  const calls = () => fx.state.calls, mark = () => calls().length;
  const openConsole = async page => { await page.click('#moreMenu > summary'); await page.click('#moreMenu .moreList button[data-mode="efficiency"]'); };
  const fresh = async (ctx, vp) => {   // a new tab of the sorter, signed in to the console
    const page = await ctx.newPage(), errs = track(page);
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.Efficiency && window.CN && document.readyState === 'complete', null, { timeout: 60000 });
    // the shell is tested on its own: the stations board and the employee page (other pieces, with their own tests) are taken away, so the
    // simple order card, the waiting spinner and the mount rules are what is seen here
    await page.evaluate(() => { for (const k of ['EfficiencyStations', 'EfficiencyEmployee']) { try { delete window[k]; } catch (_) {} if (window[k]) window[k] = undefined; } Object.assign(Efficiency.options, { pollMs: 500, liveMs: 300, growMs: 0, nudgeMs: 400, tabMs: 0 }); });
    if (vp && vp.width < 700) await page.click('#btnRail');
    await openConsole(page);
    await page.fill(`${V} .efKey input`, F.KEY); await page.press(`${V} .efKey input`, 'Enter');
    await page.waitForSelector(`${V} .efBody:not(.hidden) .efP`);
    return { page, errs };
  };
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await wire(ctx);
    const { page, errs } = await fresh(ctx);
    const ref = F.make(), refOv = ref.answer({ op: 'overview', key: F.KEY }).json;

    /* ── 2 · Sandbox mode in the sorter, the console still opens on the real stations ── */
    assert.equal(await page.evaluate(() => CN.S.settings.sandbox), 'on', 'the sorter itself is in Sandbox mode');
    assert.equal(await page.locator(V).getAttribute('data-view'), 'real', 'the console opens on Real');
    assert.equal(await page.locator(`${V} .efView button[data-view="real"]`).getAttribute('aria-pressed'), 'true');
    assert.deepEqual(await names2(page), ['Anna', 'Giovanna', 'Ivy', 'Michael'], 'the real crew, not only Paul');
    assert.equal(await kpi(page, 'parts'), String(refOv.business.totals.parts), 'the real parts');
    assert.equal(await page.locator(`${V} .efFlag`).isVisible(), false, 'no "Sandbox data only" flag on Real');
    assert(/Sorter is in Sandbox mode/.test(await page.locator(`${V} .efHint`).innerText()), 'a quiet hint that this sorter is in Sandbox mode');
    assert(calls().length > 0 && calls().every(c => c.sandbox === false), 'no request carried the sandbox flag');
    await waitFor(page, () => document.querySelectorAll('#efficiencyView .efSi').length === 3);
    assert.deepEqual(await chips(page), ['Anna', 'Giovanna', 'Michael'], 'the live logins: everyone signed in now');
    const gio = page.locator(`${V} .efSi[data-name="Giovanna"]`), gioT = await gio.innerText();
    assert(/Welding/.test(gioT) && /since\s+7:52\s?AM/.test(gioT) && /seen/.test(gioT), 'station, since when, last seen: ' + JSON.stringify(gioT));
    assert.equal((await page.locator(`${V} .efSiC`).innerText()).trim(), '3');
    await waitFor(page, () => document.querySelectorAll('#efficiencyView .efOc').length === 3);
    const who = (await page.locator(`${V} .efOc .efOcWho`).allInnerTexts()).sort();
    assert.deepEqual(who, ['Anna', 'Giovanna', 'Michael'], 'who works on which order');
    assert(await page.locator(`${V} .efOc .efOcImg`).count() === 3 && await page.locator(`${V} .efOc .efOcQr`).count() === 3, 'a picture and a QR code for each order');
    assert(/Live · /.test(await live(page)) && await page.locator(`${V} .efLive`).getAttribute('data-s') === 'live');
    const t0 = await page.locator(`${V} .efOc [data-since]`).first().innerText(); await sleep(2300);
    assert.notEqual(await page.locator(`${V} .efOc [data-since]`).first().innerText(), t0, 'the time since the scan ticks on its own');
    console.log('  ✓ the sorter in Sandbox mode: the console opens on Real (crew, parts, no sandbox flag in any request), hint shown, logins and orders listed, times tick');

    /* ── 3 · it keeps reading, pauses, catches up ── */
    let m = mark(); await sleep(1500);
    const lv = calls().slice(m).filter(c => c.op === 'live'), ov = calls().slice(m).filter(c => c.op === 'overview');
    assert(lv.length >= 3, 'the small live payload every ~0.3 s here: ' + lv.length); assert(ov.length >= 1 && ov.length < lv.length, 'the heavy overview less often: ' + ov.length + ' vs ' + lv.length);
    assert(lv.every(c => c.days === undefined && c.after === undefined && c.name === undefined), 'the live read is the small one');
    await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' }); document.dispatchEvent(new Event('visibilitychange')); });
    await sleep(700); m = mark(); await sleep(1600);
    assert.equal(mark(), m, 'nothing is read while the page is hidden');
    fx.delay = 700;
    await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'visible' }); document.dispatchEvent(new Event('visibilitychange')); });
    await waitFor(page, () => /Catching up/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 3000);
    assert(await page.locator(`${V} .efLive .spin`).isVisible(), 'a small spinner with its label while it catches up');
    fx.delay = 0; await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 8000);
    await page.click('.topbar button[data-mode="orders"]'); await waitFor(page, () => document.getElementById('efficiencyView').classList.contains('hidden'));
    await sleep(700); m = mark(); await sleep(1600); assert.equal(mark(), m, 'nothing is read while another Workspace tab is open');
    await openConsole(page); await waitFor(page, () => !document.getElementById('efficiencyView').classList.contains('hidden'));
    assert.equal(await page.locator(`${V} .efKey`).isVisible(), false, 'no passcode again');
    await sleep(900); assert(calls().slice(m).some(c => c.op === 'live'), 'reads again when it is back');
    assert.deepEqual(await chips(page), ['Anna', 'Giovanna', 'Michael']);
    // a change at a station shows within a few seconds, in place
    const before = await kpi(page, 'parts'); fx.bump(); fx.bump();
    await waitFor(page, b => +document.querySelector('#efficiencyView .efKpi[data-k="parts"] .efKV').textContent.replace(/,/g, '') > b, +before, 6000);
    console.log('  ✓ it keeps reading (live every poll, overview less often), pauses when hidden / another tab is open, catches up with a labelled spinner, a change shows');

    /* ── 4 · Sandbox is a choice ── */
    m = mark();
    await page.click(`${V} .efView button[data-view="sandbox"]`);
    await waitFor(page, () => document.querySelectorAll('#efficiencyView .efP').length === 1 && /Paul/.test(document.querySelector('#efficiencyView .efP .efName').textContent));
    assert.equal(await page.locator(V).getAttribute('data-view'), 'sandbox'); assert.equal(await kpi(page, 'parts'), '29'); assert.equal(await kpi(page, 'orders'), '25'); assert.equal(await kpi(page, 'on'), '1');
    assert(await page.locator(`${V} .efFlag`).isVisible(), 'a flag says Sandbox data only'); assert.equal(await page.locator(`${V} .efHint`).isVisible(), false);
    await waitFor(page, () => document.querySelectorAll('#efficiencyView .efSi').length === 1);
    assert.deepEqual(await chips(page), ['Paul']); assert(/Sorting/.test(await page.locator(`${V} .efSi`).innerText()), 'a session stored under the Sorter app shows as Sorting');
    await sleep(1200);
    assert(!/Giovanna|Anna|Michael|Ivy/.test(await page.locator(V).innerText()), 'nothing of the real crew on the Sandbox view');
    assert(calls().slice(m).length > 2 && calls().slice(m).every(c => c.sandbox === true), 'every request since the switch asked for the Sandbox copies');
    assert.equal(await page.evaluate(() => sessionStorage.getItem('cn.eff.view')), 'sandbox');
    // a slow answer of the other side arrives after the switch: it is dropped
    fx.delay = 900; await page.click(`${V} .efView button[data-view="real"]`); await sleep(120); await page.click(`${V} .efView button[data-view="sandbox"]`); await sleep(2600); fx.delay = 0;
    assert.deepEqual(await names2(page), ['Paul'], 'the late real answer did not draw'); assert(!/Giovanna|Anna|Michael|Ivy/.test(await page.locator(V).innerText()));
    m = mark(); await page.click(`${V} .efView button[data-view="real"]`);
    await waitFor(page, () => document.querySelectorAll('#efficiencyView .efP').length === 4);
    assert.deepEqual(await names2(page), ['Anna', 'Giovanna', 'Ivy', 'Michael']); await waitFor(page, () => document.querySelectorAll('#efficiencyView .efSi').length === 3);
    await sleep(900); assert(!/Paul/.test(await page.locator(V).innerText()), 'nothing of the Sandbox on the Real view');
    assert(calls().slice(m).every(c => c.sandbox === false), 'and back to asking without the flag'); assert.equal(await page.evaluate(() => sessionStorage.getItem('cn.eff.view') || ''), '', 'Real is the default, so nothing is remembered');
    console.log('  ✓ Sandbox only when chosen: just Paul (29 parts, 25 orders), flagged, a late answer of the other side dropped, Real again shows only the crew');

    /* ── 5 · tabs, addresses, the two modules ── */
    await page.click(`${V} .efTabBtn[data-tab="stations"]`);
    assert.equal(await hash(page), '#efficiency/stations'); await waitFor(page, () => !document.getElementById('efPgStations').classList.contains('hidden'));
    assert(/Loading the stations board/.test(await page.locator(`${V} #efPgStations`).innerText()), 'a labelled spinner while the board is not there yet'); assert(await page.locator(`${V} #efPgStations .spin`).isVisible());
    await page.evaluate(() => {
      window.__mock = { stationsMount: 0, stationsDestroy: 0, personMount: [], personDestroy: 0 };
      window.EfficiencyStations = { mount(el) { window.__mock.stationsMount++; el.innerHTML = '<div id="mockStations">the stations board</div>'; return { destroy() { window.__mock.stationsDestroy++; } }; }, orderCard(c) { const d = document.createElement('div'); d.className = 'mockCard'; d.textContent = 'CARD ' + c.orderNumber + ' ' + (c.pieces || []).length; return d; } };
      window.EfficiencyEmployee = { mount(el, o) { window.__mock.personMount.push(o.name); el.innerHTML = '<button id="mockBack" type="button">Back</button><span id="mockWho"></span>'; el.querySelector('#mockWho').textContent = o.name; el.querySelector('#mockBack').onclick = () => o.onBack(); return { destroy() { window.__mock.personDestroy++; } }; } };
    });
    await waitFor(page, () => !!document.getElementById('mockStations'), null, 4000);
    assert.equal(await page.evaluate(() => __mock.stationsMount), 1, 'the board is mounted once, when shown');
    await page.click(`${V} .efTabBtn[data-tab="people"]`); assert.equal(await hash(page), '#efficiency/people');
    assert.equal(await page.evaluate(() => __mock.stationsDestroy), 1, 'and destroyed when left'); assert.equal(await page.locator('#mockStations').count(), 0);
    await waitFor(page, () => document.querySelectorAll('#efficiencyView .efRc').length === 4);
    await page.click(`${V} .efRc[data-name="Giovanna"]`);
    assert.equal(await hash(page), '#efficiency/person/Giovanna'); await waitFor(page, () => !!document.getElementById('mockWho'), null, 4000);
    assert.equal(await page.locator('#mockWho').innerText(), 'Giovanna'); assert.deepEqual(await page.evaluate(() => __mock.personMount), ['Giovanna']);
    assert.equal(await page.locator(`${V} #efPgPeople`).isVisible(), false, 'the person page is a whole page, not a pop-up');
    await page.click('#mockBack'); await waitFor(page, () => location.hash === '#efficiency/people'); assert.equal(await page.evaluate(() => __mock.personDestroy), 1);
    await page.goForward(); await waitFor(page, () => location.hash === '#efficiency/person/Giovanna'); await waitFor(page, () => !!document.getElementById('mockWho'), null, 4000);
    await page.goBack(); await waitFor(page, () => location.hash === '#efficiency/people'); assert(await page.locator(`${V} #efPgPeople`).isVisible());
    await page.click(`${V} .efTabBtn[data-tab="overview"]`); assert.equal(await hash(page), '#efficiency');
    await waitFor(page, () => document.querySelectorAll('#efficiencyView .efWk .mockCard').length === 3, null, 4000);
    assert(/CARD 3521000101 3/.test(await page.locator(`${V} .efWk .mockCard`).first().innerText()), 'the stations board draws the order cards when it has them');
    assert.deepEqual(await page.evaluate(() => Efficiency.state.mounted), [], 'nothing mounted on the Overview');
    // a person's row on the Overview opens that person's page
    await page.click(`${V} .efP[data-name="Anna"] .efWho`); await waitFor(page, () => location.hash === '#efficiency/person/Anna'); await waitFor(page, () => document.getElementById('mockWho') && document.getElementById('mockWho').textContent === 'Anna', null, 4000);
    await page.click('#mockBack'); await waitFor(page, () => location.hash === '#efficiency');
    console.log('  ✓ Overview / Stations / People / person: addresses and Back, the board and the person page mounted when shown and destroyed when left, order cards from the board');

    /* ── 6 · honest states ── */
    fx.setLiveHook(j => Object.assign({}, j, { stations: j.stations.map(s => Object.assign({}, s, { people: [], current: [], state: 'idle' })), signedIn: [] }));
    await waitFor(page, () => /No one is signed in right now/.test(document.querySelector('#efficiencyView .efSiGrid').textContent), null, 4000);
    assert(/No one is working on an order right now/.test(await page.locator(`${V} .efWkList`).innerText()));
    assert.equal(await page.locator(`${V} .efSi`).count(), 0); assert.equal(await page.locator(`${V} .efOc`).count(), 0);
    assert(/^Live/.test(await live(page)), 'an empty answer is still a live one'); fx.setLiveHook(null);
    await waitFor(page, () => document.querySelectorAll('#efficiencyView .efSi').length === 3, null, 4000);
    // a read that hangs: it does not say Live, the last numbers stay, it recovers
    await page.evaluate(() => { Efficiency.options.timeoutMs = 700; Efficiency.options.staleMs = 1500; });
    const keep = await kpi(page, 'parts'); fx.delay = 6000;
    await waitFor(page, () => /Reconnecting/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 9000);
    assert.equal(await page.locator(`${V} .efLive`).getAttribute('data-s'), 'slow'); assert.equal(await kpi(page, 'parts'), keep, 'the last numbers stay');
    fx.delay = 0; await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 20000);
    // a service that does not have the live read yet: the overview alone, signed-in people from it, a plain note, no "Reconnecting"
    const ctx2 = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' }); await wire(ctx2);
    fx.setLiveMode('old'); const old = await fresh(ctx2);
    await waitFor(old.page, () => Efficiency.state.liveSupported === false, null, 4000);
    await waitFor(old.page, () => document.querySelectorAll('#efficiencyView .efSi').length === 3, null, 4000);
    assert.deepEqual(await chips(old.page), ['Anna', 'Giovanna', 'Michael'], 'who is signed in comes from the overview');
    assert(/not available from the service yet/.test(await old.page.locator(`${V} .efWkList`).innerText()), 'a plain note about the orders');
    await sleep(1200); assert(/^Live/.test(await live(old.page)), 'and the bar does not claim a failure');
    assert.deepEqual(old.errs, [], 'no page errors on the older service: ' + old.errs.join(' | ')); fx.setLiveMode('');
    await ctx2.close();
    // a phone: no sideways scroll on the Overview and the People tab
    const ctx3 = await browser.newContext({ viewport: { width: 390, height: 800 }, reducedMotion: 'reduce' }); await wire(ctx3);
    const ph = await fresh(ctx3, { width: 390 });
    await waitFor(ph.page, () => document.querySelectorAll('#efficiencyView .efSi').length === 3 && document.querySelectorAll('#efficiencyView .efOc').length === 3, null, 6000);
    const wide = () => ph.page.evaluate(() => [document.documentElement.scrollWidth - innerWidth, document.querySelector('.stage').scrollWidth - document.querySelector('.stage').clientWidth]);   // (the page and the scrolling stage; the sticky bar bleeds into the stage's padding on purpose)
    assert((await wide()).every(x => x <= 1), 'no sideways scroll on the Overview at 390 px: ' + await wide());
    await ph.page.click(`${V} .efTabBtn[data-tab="people"]`); await waitFor(ph.page, () => document.querySelectorAll('#efficiencyView .efRc').length === 4);
    assert((await wide()).every(x => x <= 1), 'no sideways scroll on People at 390 px: ' + await wide());
    assert.deepEqual(ph.errs, [], 'no page errors on a phone: ' + ph.errs.join(' | '));
    await ctx3.close();
    assert.deepEqual(errs, [], 'no page errors: ' + errs.join(' | '));
    console.log('  ✓ honest states: nobody signed in says so, a hung read says Reconnecting and recovers, an older service falls back quietly, a phone does not scroll sideways, no page errors, nothing left the machine (' + seen.aborted + ' outside requests refused)');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
