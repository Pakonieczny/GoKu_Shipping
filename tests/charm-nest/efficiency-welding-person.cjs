// The Welding station on one employee's page (charm-nest-efficiency-person.js) and on the console's Overview (charm-nest-efficiency.js),
// stations round 2, worker WS2. Fakes only: the harness answers the gated read function from tests/charm-nest/efficiency-person-fixture.cjs
// (invented people, a fake passcode) with a `welding` block added in the shape plans/stations-round2/api.md (WS2 section) documents, and the
// console's own reads from efficiency-fixture.cjs; every request off the loopback is aborted: nothing reaches the internet, Etsy or Firestore.
//   1 · the page's own logic: the welding block, its metrics, the points' time per task, a person with none is null; the live row's tasks
//   2 · a person with welding time: the "Welding station" figures (Welding hours, Matching hours, Orders matched, Welding (task not recorded)),
//       the time-by-task chart (stacked) and the orders-matched chart, the matched orders list (asked for `matched` orders, over the same days)
//   3 · every range (Day, Week, Month, 3 months, Year): the figures, the charts and the matched list follow the date chips
//   4 · a person without any: no Welding group, no charts, no list; the tab stops of the figures are unchanged
//   5 · the header says where, as which tasks (a person signed in to both); the station chip says matched and the minutes per task
//   6 · the Overview's Welding row: matched and time on task instead of pieces and orders; a station that counts pieces is unchanged
//   7 · 1440 / 900 / 390 px have no sideways scroll; no page errors
//   node tests/charm-nest/efficiency-welding-person.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require('./efficiency-person-fixture.cjs'), EF = require('./efficiency-fixture.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const HR = 3600000;

/** The welding block of op person for a window, from the points the fixture sent (the same buckets), in the documented shape. */
function weldingFor(j, o = {}) {
  const pts = j.series.map((p, i) => {
    if (i % 5 === 4) return { day: p.day, to: p.to, days: p.days, weldingMs: null, matchingMs: null, unknownMs: null, matched: null };
    return { day: p.day, to: p.to, days: p.days, weldingMs: (1 + i % 3) * 2 * HR, matchingMs: (1 + i % 2) * 1.5 * HR, unknownMs: i < 3 && o.unknown !== false ? HR : 0, matched: 6 + i };
  });
  const sum = k => pts.reduce((n, p) => n + (p[k] || 0), 0), r1 = v => Math.round(v * 10) / 10, H = v => r1(v / HR), matched = pts.reduce((n, p) => n + (p.matched || 0), 0);
  const m = (label, unit, value, def, extra) => Object.assign({ label, unit, value, prev: null, delta: null, better: null, def, estimated: false }, extra || {});
  return { label: 'Welding station', def: 'Time signed in at the Welding station, per task, and the orders scanned as Matching. Welding is not counted in pieces or orders.',
    hours: { welding: H(sum('weldingMs')), matching: H(sum('matchingMs')), unknown: H(sum('unknownMs')), station: H(sum('weldingMs') + sum('matchingMs') + sum('unknownMs')) }, matched,
    metrics: { weldingHours: m('Welding hours', 'hours', H(sum('weldingMs')), 'Time signed in under the Welding task.'), matchingHours: m('Matching hours', 'hours', H(sum('matchingMs')), 'Time signed in under the Matching task.'), matchedOrders: m('Orders matched', 'orders', matched, 'Order codes scanned as Matching at the Welding station. Every scan counts, so an order scanned twice counts twice. Not a completion: the Welding station is not counted in pieces or orders.', { estimated: true, why: 'A phone scan is credited to the person signed in under Matching.' }) },
    series: pts };
}

{
  const win = { console }; win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency-person.js'), 'utf8'), win);
  const E = win.EfficiencyEmployee, j = o => JSON.parse(JSON.stringify(o)), fx = F.make({ now: Date.UTC(2026, 9, 2, 19, 42) });
  const A = fx.person({ op: 'person', name: 'Ana M.', range: 'week', compare: false });
  const N0 = j(E.norm(A, { name: 'Ana M.' })); assert.equal(N0.welding, null, 'a person with no Welding block has none'); assert.deepEqual(N0.src.welding, {});
  A.welding = weldingFor(A); A.stations = A.stations.map(s => (s.station === 'welding' ? Object.assign({}, s, { parts: 0, orders: 0, matched: 9, taskMin: { welding: 60, matching: 30, unknown: 10 } }) : s));
  const N = j(E.norm(A, { name: 'Ana M.' }));
  assert.equal(N.welding.matched, A.welding.matched); assert.deepEqual(N.welding.hours, A.welding.hours);
  assert.deepEqual(Object.keys(N.src.welding).sort(), ['matchedOrders', 'matchingHours', 'unknownHours', 'weldingHours'], 'three figures from the server and the one with no task recorded');
  assert.equal(N.src.welding.weldingHours.v, A.welding.metrics.weldingHours.value); assert.equal(N.src.welding.unknownHours.v, A.welding.hours.unknown); assert.equal(N.src.welding.matchedOrders.est, true);
  assert.equal(N.series[0].weldingMs, 2 * HR, 'the points carry the time per task'); assert.equal(N.series[4].weldingMs, null, 'a day with none is a gap, not a zero'); assert.equal(N.series[0].matched, 6);
  assert.deepEqual(N.stations.find(s => s.station === 'welding').taskMin, { welding: 60, matching: 30, unknown: 10 }); assert.equal(N.stations.find(s => s.station === 'welding').matched, 9);
  const B = A.welding; B.hours.unknown = 0; const N2 = j(E.norm(Object.assign({}, A, { welding: B }), { name: 'Ana M.' })); assert.equal(N2.src.welding.unknownHours, undefined, 'no "task not recorded" figure when there is no such time');
  const L = j(E.pickLive({ at: 5, signedIn: [{ name: 'Ana M.', stationKey: 'welding', since: 50, lastSeenAt: 2, task: 'matching' }, { name: 'Ana M.', stationKey: 'welding', since: 10, lastSeenAt: 7, task: 'welding', lastInputAt: 9 }, { name: 'Bo', stationKey: 'welding', since: 1, task: 'welding' }], stations: [] }, 'Ana M.'));
  assert.deepEqual(L.where.tasks, ['welding', 'matching'], 'both tasks, one place'); assert.equal(L.where.since, 10, 'since the earlier of the two'); assert.equal(L.where.lastInputAt, 9);
  assert.deepEqual(j(E.pickLive({ at: 5, signedIn: [{ name: 'Ana M.', stationKey: 'assembly', since: 10 }] }, 'Ana M.')).where.tasks, [], 'no task at another station');
  console.log('  ✓ the welding block, its figures and the points\' time per task are read (none is null; a gap stays a gap); the live row names both tasks');
}

(async () => {
  const pwDir = process.env.PW_DIR || (fs.existsSync(path.join(root, 'node_modules/playwright-core')) ? path.join(root, 'node_modules') : '/opt/node22/lib/node_modules/playwright/node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const pf = F.make({ now: Date.UTC(2026, 9, 2, 19, 42) }), ef = EF.make(), seen = { urls: [], aborted: 0 }, SHOTS = process.env.SHOTS || '';
  const W = { on: true, tasks: ['matching', 'welding'], asked: [], overview: true };      // what the fake backend adds for Ana M.
  const wire = async ctx => {
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url()); seen.urls.push(u.href);
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      if (u.pathname.endsWith('/employeeEfficiency')) {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        const mine = ['person', 'personOrders', 'live'].includes(b.op); let r = mine ? pf.answer(b) : ef.answer(b, route.request().headers());
        if (b.op === 'overview' && r.status === 200 && W.overview) {   // the Welding station as the overview now sends it: matched and minutes per task, never pieces or orders
          r = { status: 200, json: JSON.parse(JSON.stringify(r.json)) };
          const bs = r.json.business.stations.find(x => x.station === 'welding'); if (bs) Object.assign(bs, { parts: 0, orders: 0, matched: 12, taskMin: { welding: 180, matching: 240, unknown: 60 } });
          for (const p of r.json.people) for (const x of p.stations) if (x.station === 'welding') Object.assign(x, { parts: 0, orders: 0, completes: 0, matched: 12, taskMin: { welding: 180, matching: 240, unknown: 60 } });
          const gio = r.json.people.find(p => p.name === 'Giovanna'); if (gio) { Object.assign(gio.totals, { parts: 0, orders: 0, rate: 0 }); gio.noThroughput = true; }   // (worked at the Welding station alone: the server says so)
        }
        if (mine && r.status === 200) {
          const ana = String(b.name || '').toLowerCase() === 'ana m.';
          if (b.op === 'person' && ana && W.on) { r = { status: 200, json: JSON.parse(JSON.stringify(r.json)) }; r.json.welding = weldingFor(r.json); r.json.stations = r.json.stations.map(s => (s.station === 'welding' ? Object.assign({}, s, { parts: 0, orders: 0, completes: 0, shareParts: null, perActiveHour: null, matched: r.json.welding.matched, taskMin: { welding: r.json.welding.hours.welding * 60, matching: r.json.welding.hours.matching * 60, unknown: r.json.welding.hours.unknown * 60 } }) : s)); }
          if (b.op === 'person' && ana && !W.on) { r = { status: 200, json: JSON.parse(JSON.stringify(r.json)) }; r.json.welding = null; }
          if (b.op === 'personOrders') W.asked.push({ matched: b.matched === true, from: b.from, to: b.to, name: b.name });
          if (b.op === 'live' && W.tasks.length) { r = { status: 200, json: JSON.parse(JSON.stringify(r.json)) }; r.json.signedIn = r.json.signedIn.flatMap(x => (x.name === 'Ana M.' ? W.tasks.map((t, i) => Object.assign({}, x, { task: t, lastInputAt: x.lastSeenAt - 1000 * (i + 1) })) : [x])); }
        }
        const d = mine ? pf.delayFor(b) : 0; if (d) await sleep(d);
        return route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) });
      }
      return route.continue();
    });
    await ctx.addInitScript(k => { try { localStorage.setItem('cn.employee', 'Tester'); sessionStorage.setItem('cn.eff.key', k); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.prompt = () => null; }, F.KEY);
  };
  const track = page => { const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) errs.push('console: ' + m.text()); }); return errs; };
  const V = '#efficiencyView', P = `${V} .efp`;
  const state = page => page.evaluate(() => { const h = EfficiencyEmployee.instances[0]; return h ? h.state : null; });
  const loaded = async (page, range) => page.waitForFunction(r => { const h = EfficiencyEmployee.instances[0], s = h && h.state; return !!s && s.loaded && s.range === r && !document.querySelector('#efficiencyView .efpBusy.on'); }, range, { timeout: 15000 });
  const openPerson = async (page, name) => { await page.evaluate(n => { sessionStorage.removeItem('cn.eff.p.range'); Efficiency.go('person', n); }, name); await page.waitForSelector(P, { timeout: 15000 }); await loaded(page, 'week'); };
  const kpi = (page, k) => page.locator(`${P} .efpK[data-k="${k}"]`);
  const txt = async l => (await l.innerText()).replace(/\s+/g, ' ').trim();
  const shot = async (page, name, sel) => { if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true }); if (sel) { await page.locator(sel).first().scrollIntoViewIfNeeded(); await sleep(400); await page.locator(sel).first().screenshot({ path: path.join(SHOTS, name) }); } else await page.screenshot({ path: path.join(SHOTS, name) }); };
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' }); await wire(ctx);
    const page = await ctx.newPage(), errs = track(page);
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.Efficiency && window.EfficiencyEmployee && window.EfficiencyOrders && window.CN && window.OrderWin && document.readyState === 'complete', null, { timeout: 60000 });
    await page.evaluate(() => { Efficiency.options.pollMs = 600; Efficiency.options.liveMs = 300; Efficiency.options.growMs = 0; Object.assign(EfficiencyEmployee.options, { liveMs: 300, rangeMs: 700, calMs: 900, ordersMs: 700, tickMs: 250 }); window.__opened = []; });
    await page.evaluate(() => { Efficiency.open(); }); await page.waitForSelector(`${V}:not(.hidden)`);
    await page.evaluate(() => { Efficiency.api.openOrder = (btn, rid) => { window.__opened.push(String(rid)); }; });

    /* ── 2 · a person with welding time ── */
    await openPerson(page, 'Ana M.');
    await page.waitForFunction(() => { const h = EfficiencyEmployee.instances[0]; return h && h.state.welding && h.state.matchedModule; }, null, { timeout: 8000 });
    assert.equal(await page.locator(`${P} .efpGroup[data-g="Welding station"]`).isVisible(), true, 'the Welding station group shows for a person with welding time');
    const T = (await page.evaluate(() => 0), pf.person({ op: 'person', name: 'Ana M.', range: 'week', day: pf.today, compare: false })), WB = weldingFor(T);
    const want = (ms) => { const h = ms / HR; return h >= 10 ? `${Math.round(h * 10) / 10} h` : `${Math.floor(h)} h${Math.round((h % 1) * 60) ? ' ' + Math.round((h % 1) * 60) + ' m' : ''}`; };
    const lab = async k => (await txt(kpi(page, k).locator('.efpKL'))).toLowerCase();
    assert.equal(await lab('welding.weldingHours'), 'welding hours'); assert.equal(await lab('welding.matchingHours'), 'matching hours'); assert.equal(await lab('welding.matchedOrders'), 'orders matched');
    const val = async k => (await txt(kpi(page, k).locator('.efpKV')));
    assert.equal(await val('welding.matchedOrders'), String(WB.matched), 'orders matched = the server\'s count'); assert(/h/.test(await val('welding.weldingHours')) && /h|m/.test(await val('welding.matchingHours')), 'hours read in hours');
    assert.equal(await kpi(page, 'welding.unknownHours').isVisible(), true, 'older sign-ins with no task are their own figure'); assert.equal(await lab('welding.unknownHours'), 'welding (task not recorded)');
    // never pieces or orders for the Welding station
    const grp = await txt(page.locator(`${P} .efpGroup[data-g="Welding station"]`)); assert(!/pieces|per day|completed/i.test(grp.replace(/scans, not completions/, '')), 'no pieces or orders in the group: ' + grp);
    // hover: the definition says it is not a completion
    await kpi(page, 'welding.matchedOrders').hover(); await page.waitForSelector(`${P} .efpHC.on`); const hc = await txt(page.locator(`${P} .efpHC`)); assert(/not a completion/i.test(hc) && /estimated/i.test(hc), 'the matched card says what it is: ' + hc);
    await page.mouse.move(2, 2); await page.waitForFunction(() => !document.querySelector('#efficiencyView .efpHC.on'), null, { timeout: 4000 });
    // charts
    assert.equal(await page.locator(`${P} .efpWC`).isVisible(), true);
    const bars = await page.$$eval(`${P} [data-c="wt"] .efc-bar`, b => b.length), pts = T.series.length;
    assert(bars >= pts, `the time-by-task chart draws bars (${bars}) for the ${pts} days`);
    assert(/welding/.test(await txt(page.locator(`${P} [data-c="wt"] .efpCP`))) && /matching/.test(await txt(page.locator(`${P} [data-c="wt"] .efpCP`))), 'the chart says its totals');
    assert.equal(await page.$$eval(`${P} [data-c="wt"] .efc-legend > *`, s => s.map(x => x.textContent.trim()).join('|')), 'Welding|Matching|Welding (task not recorded)', 'a legend for the tasks');
    assert.equal(await page.$$eval(`${P} [data-c="wm"] .efc-bar`, b => b.length) >= pts, true, 'the orders-matched chart draws a bar per day');
    // the matched list
    assert.equal(await page.locator(`${P} .efpWO`).isVisible(), true); await page.waitForSelector(`${P} .efpWO .eoRow, ${P} .efpWO [data-rid]`, { timeout: 8000 });
    const first = W.asked.filter(a => a.matched)[0]; assert(first && first.name === 'Ana M.', 'the list asks for matched orders'); assert.equal(first.from, T.from, 'over the same days as the date chips'); assert.equal(first.to, T.to);
    assert(W.asked.some(a => !a.matched) === true, 'the ordinary list is still the ordinary list'); assert(W.asked.filter(a => !a.matched).every(a => a.matched === false));
    assert.equal(await txt(page.locator(`${P} .efpWR`)), (await txt(page.locator(`${P} .efpDay`))), 'the list says which days it covers');
    const rowSel = `${P} .efpWOrders [data-rid]`; const rid = await page.locator(rowSel).first().getAttribute('data-rid'); await page.locator(rowSel).first().click({ position: { x: 30, y: 8 } });
    await page.waitForFunction(r => window.__opened.includes(r), rid, { timeout: 4000 });
    await shot(page, 'person-welding-1440.png', `${P} .efpGroup[data-g="Welding station"]`); await shot(page, 'person-welding-charts-1440.png', `${P} .efpWC`);
    console.log('  ✓ Welding station figures (Welding hours, Matching hours, Orders matched, task not recorded), the stacked time-by-task chart, orders matched per day and the matched orders list');

    /* ── 3 · every range ── */
    for (const [key, days] of [['day', 1], ['month', 30], ['quarter', 90], ['year', 365], ['week', 7]]) {
      const n0 = W.asked.length; await page.click(`${P} .efpSeg button[data-range="${key}"]`); await loaded(page, key);
      const Tk = pf.person({ op: 'person', name: 'Ana M.', range: key, day: pf.today, compare: false }), Wk = weldingFor(Tk);
      await page.waitForFunction(m => EfficiencyEmployee.instances[0].state.welding && document.querySelector('#efficiencyView .efp .efpK[data-k="welding.matchedOrders"] .efpKV').textContent.replace(/,/g, '') === String(m), Wk.matched, { timeout: 8000 });
      assert.equal(await val('welding.matchedOrders'), Wk.matched.toLocaleString('en-US'), key + ': orders matched follow the range');
      const st = await state(page); assert.equal(st.from, Tk.from); await sleep(900);
      const askedNow = W.asked.slice(n0).filter(a => a.matched), last = askedNow[askedNow.length - 1]; assert(last, key + ': the matched list is read again for the new days'); assert.deepEqual([last.from, last.to], [Tk.from, Tk.to], key + ': the matched list covers the same days as the date chips');
      const nb = await page.$$eval(`${P} [data-c="wt"] .efc-bar`, b => b.length); assert(nb >= (key === 'day' ? 1 : Math.min(Tk.series.length, 92)), `${key}: bars ${nb} for ${Tk.series.length} buckets`);
      if (key === 'day') { assert(await page.locator(`${P} .efpWC`).isVisible(), 'a Day has its (one-bar) welding charts too'); }
    }
    console.log('  ✓ Day, Week, Month, 3 months, Year: the figures, the charts and the matched list follow the date chips');

    /* ── 5 · the header and the station chip ── */
    await page.waitForFunction(sel => /Signed in\s*at\s*Welding\s*as\s*Welding and Matching/.test(document.querySelector(sel).textContent), `${P} .efpWhere`, { timeout: 8000 });
    const chip = await page.locator(`${P} .efpChip`, { hasText: 'Welding' }).first().getAttribute('title'); assert(/Welding .* · Matching .* · \d+ matched/.test(chip), 'the station chip says minutes per task and matched, not pieces: ' + chip);
    console.log('  ✓ header: signed in at Welding as Welding and Matching; the station chip says matched and the minutes per task');

    /* ── 4 · a person with nothing at Welding ── */
    W.on = false; await page.evaluate(() => { EfficiencyEmployee.instances[0].go({ range: 'month' }); }); await loaded(page, 'month'); await page.evaluate(() => { EfficiencyEmployee.instances[0].go({ range: 'week' }); }); await loaded(page, 'week');
    await page.evaluate(() => { EfficiencyEmployee.instances[0].go({}); document.querySelector('#efficiencyView .efp'); });
    await page.evaluate(() => { const h = EfficiencyEmployee.instances[0]; h.go({ anchor: h.state.anchor }); }); await page.waitForFunction(() => !EfficiencyEmployee.instances[0].state.welding, null, { timeout: 8000 });
    assert.equal(await page.locator(`${P} .efpGroup[data-g="Welding station"]`).isVisible(), false, 'no Welding group'); assert.equal(await page.locator(`${P} .efpWC`).isVisible(), false, 'no welding charts'); assert.equal(await page.locator(`${P} .efpWO`).isVisible(), false, 'no matched list');
    assert.equal(await page.locator(`${P} .efpKGroups .efpK[tabindex="0"]`).count(), 6, 'the Welding group is not a Tab stop when it is not there: one stop for each of the six groups');
    console.log('  ✓ no welding time, no Welding group, charts or list; the figures keep their six Tab stops');

    /* ── 6 · the Overview ── */
    await page.evaluate(() => { Efficiency.go('overview'); });
    const row = page.locator(`${V} .efSR[data-station="welding"]`); await row.waitFor({ timeout: 10000 });
    await page.waitForFunction(() => document.querySelector('#efficiencyView .efSR[data-station="welding"]').dataset.weld === '1', null, { timeout: 8000 });
    assert.match(await txt(row.locator('.efSV[data-c="parts"]')), /^12\s*matched$/, 'the Welding row counts matched scans where the others count pieces');
    assert.equal(await txt(row.locator('.efSV[data-c="orders"]')), '8 h', 'and time on task where the others count orders (welding 3 h + 1 h with no task, matching 4 h)');
    assert.equal(await row.locator('.efSV[data-c="orders"]').getAttribute('aria-label'), 'Time on task');
    const asm = page.locator(`${V} .efSR[data-station="assembly"]`); assert.equal(await asm.getAttribute('data-weld'), null); assert.match(await txt(asm.locator('.efSV[data-c="parts"]')), /pieces$/); assert.match(await txt(asm.locator('.efSV[data-c="orders"]')), /orders$/, 'a station that counts pieces is unchanged');
    await row.locator('.efSV[data-c="orders"]').hover(); await page.waitForSelector('.esTip[data-on]'); const tip = await txt(page.locator('.esTip'));
    assert(/Time on task/i.test(tip) && /Welding\s*4 h/i.test(tip) && /Matching\s*4 h/i.test(tip) && /No task recorded\s*1 h/i.test(tip), 'the time-on-task card: ' + tip); await page.mouse.move(2, 2);
    await row.locator('.efSV[data-c="parts"]').hover(); await page.waitForSelector('.esTip[data-on]'); assert(/not a finished piece or a completed order/i.test(await txt(page.locator('.esTip'))), 'the matched card says a scan is not a completion'); await page.mouse.move(2, 2);
    await shot(page, 'overview-welding-1440.png', `${V} .efSR[data-station="welding"]`);
    // the person's own row in the Overview: no pieces or orders for the Welding station
    await page.locator(`${V} .efP[data-name="Giovanna"] .efOrd`).click(); await page.waitForSelector(`${V} .efP[data-name="Giovanna"] .efMini`, { timeout: 8000 });
    const wr = page.locator(`${V} .efP[data-name="Giovanna"] .efMini tbody tr`, { hasText: 'Welding' }); const cells = (await wr.locator('td').allInnerTexts()).map(x => x.replace(/\s+/g, ' ').trim());
    assert.deepEqual([cells[1], cells[3]], ['—', '—'], 'the Welding row of the by-station table has no pieces and no orders: ' + cells.join('|')); assert(/12 matched/.test(cells[0]), 'it says matched: ' + cells[0]);
    // the People card of somebody who worked at Welding alone: a dash for pieces and orders (as the person's page says), the time on task and the matched count instead; everybody else's card is as before
    await page.evaluate(() => { Efficiency.go('people'); });
    const gcard = page.locator(`${V} .efRoster .efRc[data-name="Giovanna"]`); await gcard.waitFor({ timeout: 10000 });
    assert.deepEqual([await txt(gcard.locator('[data-f="parts"]')), await txt(gcard.locator('[data-f="orders"]')), await txt(gcard.locator('[data-f="rate"]'))], ['—', '—', '—'], 'Welding alone: a dash for pieces, orders and per hour on the People card');
    const wline = await txt(gcard.locator('.efRcW')); assert.equal(wline, 'Welding 4 h · Matching 4 h · 12 matched', 'the time on task per task and the matched count stand in: ' + wline);
    await shot(page, 'people-welding-1440.png', `${V} .efRoster`);
    const others = await page.$$eval(`${V} .efRoster .efRc`, cs => cs.filter(c => c.dataset.name !== 'Giovanna').map(c => c.querySelector('[data-f="parts"]').innerText.trim()));
    assert(others.length > 0 && others.some(t => /^\d/.test(t)), 'everybody else still has pieces on the card: ' + others.join(','));
    console.log('  ✓ the Overview: the Welding row says matched and time on task (with their cards), the by-station table has no pieces or orders for it, other stations unchanged; the People card of a Welding-only person says a dash and the time on task and matched instead');
    assert.deepEqual(errs, [], 'no page errors: ' + errs.join(' | '));
    assert(seen.aborted >= 0);
    await ctx.close();
  } finally { await browser.close(); await srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
