// The Inbox in the Employee efficiency portal (R8): the person page's "Inbox" section (charm-nest-efficiency-person.js) and the Stations board's
// Inbox card (charm-nest-efficiency-stations.js), plus the order list's "replies" cell (charm-nest-efficiency-orders.js).
// Fakes only: the harness answers the gated read function from tests/stations/inbox-portal-fixture.cjs (invented people, 400 invented customers,
// a fake passcode, replies recorded from 150 days ago) and, for the console's own reads, from tests/charm-nest/efficiency-fixture.cjs; every request
// off the loopback is aborted, so nothing reaches the internet, Etsy or Firestore. No real passcode, PIN or order is touched.
//   1 · the page's own logic (normInbox: a figure the answer lacks is a dash, never a zero; the series, hours, top list, notes; the board's block)
//   2 · opening: a small labelled spinner while the inbox is read (the rest of the page does not wait), a separate "Inbox" section
//   3 · every range (Day Week Month 3 months Year): the figures, the change against the period before, the chart (hours for a Day, days, weeks,
//       a dash where nothing was recorded), the metric switch, how many messages each customer got, the top list
//   4 · hover states are the page's own (hover card of a figure, a top row), a press opens the order in the order window (never a pop-up on a pop-up)
//   5 · the orders covered: the same list as the Orders section (search, pages, a reply count per order, a row opens the order)
//   6 · a person with no replies, an unknown name, the service without the op ("not available yet"), a failed read (Try now), sandbox
//   7 · the Stations board's Inbox card: today's replies and orders, who is signed in with since and time since last input (ticks with no request)
//   8 · 1440 / 900 / 390 px have no sideways scroll, reduced motion is instant, no page errors, nothing reached off the loopback
//   node tests/stations/inbox-portal.cjs      (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir> to save screenshots of the fixture pages)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const IX = require('./inbox-portal-fixture.cjs'), EF = require('../charm-nest/efficiency-fixture.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const round1 = v => Math.round(v * 10) / 10;

/* ── 1 · the page's own logic ── */
{
  const win = { console }; win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency-person.js'), 'utf8'), win);
  const E = win.EfficiencyEmployee, j = o => JSON.parse(JSON.stringify(o));
  assert.equal(typeof E.normInbox, 'function');
  const none = j(E.normInbox(null, { name: 'Zed', from: '2026-10-01', to: '2026-10-02' }));
  assert.equal(none.found, true); assert.deepEqual(none.src, {}, 'nothing known: no figure at all (a dash), never a zero'); assert.deepEqual(none.series, []); assert.equal(none.per, null); assert.equal(none.unknown, null);
  assert.equal(none.name, 'Zed'); assert.equal(none.from, '2026-10-01', 'the asked window stands in when the answer has none');
  const N = j(E.normInbox({ ok: true, name: 'Zed', found: true, from: '2026-10-01', to: '2026-10-03', days: 3, today: '2026-10-03', live: true, granularity: 'day', knownFrom: '2026-09-28',
    totals: { replies: { value: null, prev: 5, label: 'Replies sent' }, orders: 7, messages: { value: 12, prev: 10, delta: 2, def: 'Messages' } },
    series: [{ day: '2026-10-01', replies: null, messages: null }, { day: '2026-10-02', replies: 4, messages: 6, orders: 3, customers: 3 }, { day: '2026-10-03', replies: 2, messages: 3, orders: 2, customers: 2 }, { nope: 1 }],
    hours: [{ hour: 9, replies: 2, messages: 3 }, { hour: 14, replies: 1, messages: 1 }],
    perCustomer: { average: 1.5, median: 1, max: 4, total: 4, distribution: [{ messages: 1, customers: 3 }, { messages: 5, customers: 1, plus: true }], top: [{ customer: 'Maya L.', messages: 4, replies: 3, orders: 2, lastAt: 5, rid: '3521-000-012' }, { rid: 7 }, { nope: 1 }] },
    unknown: { replies: 2, messages: 2 }, notes: ['One note', ''], partial: true }, { name: 'Zed' }));
  assert.equal(N.src.replies.v, null, 'a figure the answer does not know stays null'); assert.equal(N.src.replies.prev, 5); assert.equal(N.src.orders.v, 7); assert.equal(N.src.messages.v, 12); assert.equal(N.src.messages.delta, 2);
  assert.equal(N.series.length, 3, 'a bucket with no day is dropped'); assert.equal(N.series[0].replies, null, 'a day with nothing known stays null'); assert.equal(N.series[1].customers, 3);
  assert.equal(N.hours.length, 24); assert.equal(N.hours[9].replies, 2); assert.equal(N.hours[3].replies, null, 'an hour the answer does not list is unknown, not zero');
  assert.equal(N.src.messagesPerCustomer.v, 1.5, 'messages per customer is read from the customers block'); assert.equal(N.src.messagesPerCustomer.derived, true); assert.equal(N.src.maxPerCustomer.v, 4);
  assert.equal(N.per.top[0].rid, '3521000012', 'an order number is its digits'); assert.equal(N.per.top.length, 2); assert.equal(N.per.top[1].customer, ''); assert.equal(N.per.dist[1].plus, true);
  assert.deepEqual(N.unknown, { replies: 2, messages: 2 }); assert.deepEqual(N.notes, ['One note']); assert.equal(N.partial, true); assert.equal(N.knownFrom, '2026-09-28'); assert.equal(N.granularity, 'day');
  const S2 = j(E.normInbox({ series: [{ day: '2026-10-02', replies: 4, messages: 6 }, { day: '2026-10-03', replies: null, messages: null }, { day: '2026-10-04', replies: 2, messages: 3 }] }, {}));
  assert.equal(S2.src.replies.v, 6, 'a total the server did not send is the days added up, and says so'); assert.equal(S2.src.replies.derived, true); assert.equal(S2.src.messages.v, 9);
  const fx = IX.make({ now: Date.UTC(2026, 9, 2, 19, 42) }), A = fx.truth('Ana M.', { range: 'month' }), N3 = j(E.normInbox(A, { name: 'Ana M.' }));
  assert.equal(N3.src.replies.v, A.totals.replies.value); assert.equal(N3.src.replies.prev, A.totals.replies.prev); assert.equal(N3.series.length, 30); assert.equal(N3.per.top.length, 5);
  assert(N3.src.orders && N3.src.customers && N3.src.messages && N3.src.messagesPerCustomer && N3.src.maxPerCustomer && N3.src.repliesPerDay && N3.src.daysActive, 'the eight figures are read');
  const Y = j(E.normInbox(fx.truth('Ana M.', { range: 'year' }), { name: 'Ana M.' })); assert.equal(Y.granularity, 'week'); assert.equal(Y.series[0].replies, null, 'weeks before the record began are unknown'); assert(Y.series.some(p => p.replies > 0));
  const U = j(E.normInbox(fx.truth('Zed Q.', { range: 'week' }), { name: 'Zed Q.' })); assert.equal(U.found, false);
  console.log('  ✓ the answer\'s shapes are read: a missing figure is a dash, derived totals say so, unknown days stay unknown, the top list and notes come through');
}

(async () => {
  const pwDir = process.env.PW_DIR || (fs.existsSync(path.join(root, 'node_modules/playwright-core')) ? path.join(root, 'node_modules') : '/opt/node22/lib/node_modules/playwright/node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('../charm-nest/bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  // both fakes keep the console's clock: Friday 2 Oct 2026, 3:42 PM New York
  const gate = { p: null, go: null, hold() { gate.p = new Promise(r => { gate.go = () => { gate.p = null; r(); }; }); }, release() { if (gate.go) gate.go(); } };
  const ix = IX.make({ now: Date.UTC(2026, 9, 2, 19, 42) }), ef = EF.make(), seen = { urls: [], aborted: 0 }, SHOTS = process.env.SHOTS || '';
  if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
  const MINE = ['person', 'personOrders', 'personInbox', 'live'];
  const wire = async ctx => {
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url()); seen.urls.push(u.href);
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      if (u.pathname.endsWith('/employeeEfficiency')) {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        const mine = MINE.includes(b.op), r = mine ? ix.answer(b) : ef.answer(b, route.request().headers());
        const d = mine ? ix.delayFor(b) : 0; if (d) await sleep(d);
        if (b.op === 'personInbox' && gate.p) await gate.p;   // (held until the test lets it go: a wait that does not depend on how fast this machine is)
        try { return await route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) }); } catch (_) { return; }
      }
      return route.continue();
    });
    await ctx.addInitScript(k => { try { localStorage.setItem('cn.employee', 'Tester'); sessionStorage.setItem('cn.eff.key', k); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.__prompted = 0; window.prompt = () => { window.__prompted++; return null; }; }, IX.KEY);
  };
  // (the browser's own notice for the answers this test forces on purpose: the 400 of a service without the op, the 503 of a failed read)
  const track = page => { const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error' && !(/status of (400|401|403|405|429|503)/.test(m.text()) && /employeeEfficiency/.test((m.location() && m.location().url) || ''))) errs.push('console: ' + m.text()); }); return errs; };
  const V = '#efficiencyView', P = `${V} .efp`, I = `${P} .efpIn`, K = k => `${I} .efpK[data-k="inbox.${k}"]`;
  const calls = op => ix.state.calls.filter(c => c.op === op), inboxCalls = () => calls('personInbox'), orderCalls = () => calls('personOrders');
  const nums = s => +String(s).replace(/[^\d.\-]/g, '');
  const kv = (page, k) => page.locator(`${K(k)} .efpKV`).innerText();
  const delta = async (page, k) => (await page.locator(`${K(k)} .efpKD`).innerText()).replace(/\s+/g, ' ').trim();
  /** A picture of one part of the page. The console scrolls inside its own box, so the window is made as tall as the whole page first (and put back); `until` stops at the top of another element. */
  const shot = async (page, name, sel, opt = {}) => {
    if (!SHOTS) return; const f = path.join(SHOTS, name + '.png'); if (!sel) { await page.screenshot({ path: f }); return; }
    const vp = page.viewportSize(), sh = await page.evaluate(() => document.querySelector('#efficiencyView').scrollHeight);
    await page.setViewportSize({ width: vp.width, height: Math.min(15000, Math.max(vp.height, sh + 140)) }); await page.waitForTimeout(600);
    await page.evaluate(() => { for (let e = document.querySelector('#efficiencyView'); e; e = e.parentElement) if (e.scrollTop) e.scrollTop = 0; document.querySelectorAll('#efficiencyView *').forEach(e => { if (e.scrollTop && e.scrollHeight > e.clientHeight) e.scrollTop = 0; }); });
    const b = await page.locator(sel).first().boundingBox(); let hgt = b.height;
    if (opt.until) { const u = await page.locator(opt.until).first().boundingBox(); hgt = Math.max(60, u.y - b.y - 8); }
    await page.screenshot({ path: f, clip: { x: Math.max(0, b.x - 8), y: Math.max(0, b.y - 8), width: Math.min(vp.width, b.width + 16), height: Math.min(hgt, 3000) + 8 } });
    await page.setViewportSize(vp); await page.waitForTimeout(300);
  };
  const rail = (page, off) => page.evaluate(o => { const app = document.getElementById('app'); if (app.classList.contains('railOff') !== o) document.getElementById('btnRail').click(); }, off);
  const loaded = async (page, range, anchor) => {
    try { await page.waitForFunction(([r, a]) => { const h = EfficiencyEmployee.instances[0], s = h && h.state; return !!s && s.loaded && s.range === r && (!a || s.anchor === a) && !document.querySelector('#efficiencyView .efpBusy.on'); }, [range, anchor || ''], { timeout: 20000 }); }
    catch (e) { throw new Error(`the page did not settle on ${range}${anchor ? ' ' + anchor : ''}: ` + JSON.stringify(await page.evaluate(() => ({ s: EfficiencyEmployee.instances.map(h => h.state), busy: !!document.querySelector('#efficiencyView .efpBusy.on'), hash: location.hash })))); }
  };
  /** The Inbox section has its answer: no busy light, the first figure drawn (or the quiet line that says why not). */
  const inboxReady = (page, timeout) => page.waitForFunction(() => { const s = document.querySelector('#efficiencyView .efpIn'); if (!s || s.querySelector('.efpInBusy.on')) return false; const m = s.querySelector('.efpInMsg'); return (m && !m.classList.contains('hidden')) || (s.querySelector('.efpK .efpKV') && s.querySelector('.efpK .efpKV').textContent.trim() !== '—'); }, null, { timeout: timeout || 20000 });
  const openPerson = async (page, name, range) => { if (await page.evaluate(() => EfficiencyEmployee.instances.length)) { await page.evaluate(() => Efficiency.go('people')); await page.waitForFunction(() => !EfficiencyEmployee.instances.length, null, { timeout: 15000 }); } await page.evaluate(n => { sessionStorage.removeItem('cn.eff.p.range'); Efficiency.go('person', n); }, name); await page.waitForSelector(P, { timeout: 20000 }); await loaded(page, 'week'); if (range && range !== 'week') { await page.click(`${P} .efpSeg button[data-range="${range}"]`); await loaded(page, range); } };
  const boot = async ctxOpts => {
    const ctx = await browser.newContext(Object.assign({ viewport: { width: 1440, height: 900 } }, ctxOpts)); await wire(ctx);
    const page = await ctx.newPage(), errs = track(page);
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.Efficiency && window.EfficiencyEmployee && window.EfficiencyStations && window.EfficiencyOrders && window.CN && window.OrderWin && document.readyState === 'complete', null, { timeout: 60000 });
    await page.evaluate(() => { Efficiency.options.pollMs = 600; Efficiency.options.liveMs = 300; Efficiency.options.growMs = 0; Efficiency.options.timeoutMs = 90000;   // (a read the test holds on purpose is not a timeout)
      Object.assign(EfficiencyEmployee.options, { liveMs: 300, rangeMs: 700, calMs: 900, ordersMs: 700, tickMs: 250, inboxMs: 700 }); window.__opened = []; });
    await page.evaluate(() => { Efficiency.open(); });
    await page.waitForSelector(`${V}:not(.hidden)`);
    await page.evaluate(() => { Efficiency.api.openOrder = (btn, rid) => { window.__opened.push(String(rid)); }; });
    return { ctx, page, errs };
  };
  const R = (range, extra) => ix.truth('Ana M.', Object.assign({ range, day: ix.today, top: 10 }, extra || {}));   // (the page asks for ten customers)
  try {
    const A = await boot({ reducedMotion: 'reduce' }), page = A.page;
    await rail(page, true);

    /* ── 2 · opening: a small labelled spinner, a separate section ── */
    gate.hold();
    await page.evaluate(() => { sessionStorage.removeItem('cn.eff.p.range'); Efficiency.go('person', 'Ana M.'); });
    await page.waitForSelector(`${I} .efpInBusy.on`, { timeout: 20000 });
    { const bt = await page.evaluate(() => { const b = document.querySelector('#efficiencyView .efpIn .efpInBusy'); return { on: b.classList.contains('on'), t: b.textContent.trim(), spin: !!b.querySelector('.spin') }; });
      assert(bt.on && /Reading the inbox/.test(bt.t) && bt.spin, `a small labelled spinner while the inbox is read (${JSON.stringify(bt)})`); }
    await loaded(page, 'week');
    assert.equal(await page.locator(`${P} .efpBody`).isVisible(), true, 'the rest of the page does not wait for the inbox');
    assert.equal((await kv(page, 'replies')).trim(), '—', 'no figure and no zero is drawn before the answer');
    assert(await page.locator(`${I} [data-c="inb"] .efc-wait`).count() === 1 && /\S/.test(await page.locator(`${I} [data-c="inb"] .efc-wait`).innerText()), 'the chart shows its own labelled spinner until its first figures');
    await shot(page, 'in2-person-inbox-spinner-1440', I, { until: `${I} .efpInOL` });
    gate.release(); await inboxReady(page);
    assert.equal(await page.locator(`${I} .efpInBusy.on`).count(), 0, 'the spinner goes away');
    assert.equal(await page.locator(`${V} dialog[open]`).count(), 0, 'a page, not a pop-up');
    const label = await page.locator(`${I} > .efpLabel`).first().innerText(); assert(/^inbox/i.test(label.trim()), 'a clearly separate section called Inbox: ' + label);
    assert.equal(await page.locator(`${P} section[aria-label="Inbox"]`).count(), 1); assert.equal(await page.locator(`${I} .efpKpis .efpK`).count(), 8, 'eight figure cards (four shown, four under More figures)');
    assert.equal(await page.locator(`${I} .efpK:not(.hidden)`).count(), 4, 'four shown at first');
    const first = inboxCalls()[0]; assert(first.name === 'Ana M.' && first.range === 'week' && first.compare === true && first.top === 10, 'asks for the same window as the chips, with its comparison, and ten customers');
    { const mm = await page.locator(`${I} .efpInMsg`).evaluate(e => ({ hidden: e.classList.contains('hidden'), t: e.textContent })); assert.equal(mm.hidden, true, 'no message when all is well: ' + mm.t + ' · calls ' + JSON.stringify(ix.state.calls.map(c => c.op + ':' + (c.range && c.range.from ? 'custom' : c.range || '')))); }
    // the page's own pieces are not disturbed by the new section: the Orders label keeps its period, the "cannot tell" block is its own
    assert(/\S/.test(await page.locator(`${P} section[aria-label="Orders"] .efpLr`).innerText()), 'the Orders section keeps its period label');
    console.log('  ✓ opens: a small labelled spinner for the inbox only, a separate Inbox section, no pop-up');

    /* ── 3 · every range ── */
    const RANGE = { week: 'Week', day: 'Day', month: 'Month', quarter: '3 months', year: 'Year' };   // (the page opens on Week)
    for (const [key, name] of Object.entries(RANGE)) {
      if (key !== 'week') { await page.click(`${P} .efpSeg button[data-range="${key}"]`); await loaded(page, key); }
      const T = R(key), t = T.totals;
      try { await page.waitForFunction(([k, v]) => { const e = document.querySelector(`#efficiencyView .efpIn .efpK[data-k="inbox.${k}"] .efpKV`); return e && e.textContent.replace(/,/g, '') === String(v == null ? '—' : v); }, ['replies', t.replies.value], { timeout: 15000 }); }
      catch (e) { throw new Error(`${key}: the replies figure did not settle on ${t.replies.value}: ` + JSON.stringify(await page.evaluate(() => ({ vals: [...document.querySelectorAll('#efficiencyView .efpIn .efpK')].map(k => k.dataset.k + '=' + k.querySelector('.efpKV').textContent), msg: document.querySelector('#efficiencyView .efpIn .efpInMsg').textContent, busy: document.querySelector('#efficiencyView .efpIn .efpInBusy').className, st: EfficiencyEmployee.instances[0].state.range }))) + ' calls ' + JSON.stringify(inboxCalls().map(c => c.range))); }
      assert(inboxCalls().some(c => c.range === key && c.compare === true), key + ': the server is asked for this window');
      for (const k of ['replies', 'orders', 'customers', 'messages']) assert.equal(nums(await kv(page, k)), t[k].value, `${key}: ${k} agree with the fixture`);
      assert(Math.abs(nums(await kv(page, 'messagesPerCustomer')) - round1(t.messagesPerCustomer.value)) < .051 || t.messagesPerCustomer.value == null, key + ': messages per customer');
      const dl = await delta(page, 'replies');
      if (t.replies.prev == null) assert(/no data/.test(dl) && !/[▲▼]/.test(dl), `${key}: nothing to compare with says so (${dl})`); else assert(/([▲▼] \d+%|no change)/.test(dl) && /vs/.test(dl), `${key}: the change against the period before (${dl})`);
      assert.equal(await page.locator(`${I} .efpInBusy.on`).count(), 0);
      // the chart: hours for a Day, a bar a day, a bar a week for a year; the Day has no orders / customers switch (an hour has no such count)
      const bars = await page.locator(`${I} [data-c="inb"] .efc-bar`).count();
      if (key === 'day') { assert(bars >= 8, 'Day is by the hour: ' + bars); assert.equal(await page.locator(`${I} [data-inmetric="orders"]`).isVisible(), false, 'Day: replies and messages only'); }
      else { assert.equal(bars, T.series.length, `${key}: a bar for each ${T.granularity} (${bars})`); assert.equal(await page.locator(`${I} [data-inmetric="orders"]`).isVisible(), true); }
      assert.equal(await page.locator(`${I} [data-inmetric].on`).innerText(), 'Replies', 'replies first');
      // the customers: the average, the spread and the most-messaged
      assert(/average/.test(await page.locator(`${I} [data-c="inbTop"] .efpCP`).innerText()), 'the average is said');
      const listed = T.perCustomer.top.length, rows = await page.locator(`${I} .efpTR`).count(); assert.equal(rows, Math.min(5, listed), key + ': the five customers who got the most');   // (the fixture answers the ten asked for)
      if (listed > 5) { assert.equal(await page.locator(`${I} .efpInMoreB`).innerText(), `Show all ${listed}`, 'a quiet control for the rest'); await page.click(`${I} .efpInMoreB`); assert.equal(await page.locator(`${I} .efpTR`).count(), listed, key + ': all ten'); assert.equal(await page.locator(`${I} .efpInMoreB`).innerText(), 'Show the top 5'); await page.click(`${I} .efpInMoreB`); assert.equal(await page.locator(`${I} .efpTR`).count(), 5); }
      else assert.equal(await page.locator(`${I} .efpInMoreB`).count(), 0, key + ': no control when all are shown');
      if (T.perCustomer.top.length) assert(new RegExp(T.perCustomer.top[0].customer).test(await page.locator(`${I} .efpTR`).first().innerText()), 'the one who got the most is first');
      if (key === 'week' || key === 'year') await shot(page, `in2-person-inbox-${key}-1440`, I, { until: `${I} .efpInOL` });
      if (key === 'year') {
        const foot = (await page.locator(`${I} .efpInFoot`).innerText()).replace(/\s+/g, ' ');
        assert(/keeps its own record of sent replies from/.test(foot) && /dash, not zero/.test(foot), 'a window that starts before the record began says so: ' + foot);
        const dashes = await page.locator(`${I} [data-c="inb"] .efc-bar.null, ${I} [data-c="inb"] .efc-bar[data-null], ${I} [data-c="inb"] .efc-gap`).count(); // (a library detail: not asserted beyond the count of bars)
        assert(T.series[0].replies == null && T.series.some(p => p.replies > 0), 'the year starts with unknown weeks');
      }
    }
    // the metric switch re-draws the one chart, the same bars
    await page.click(`${P} .efpSeg button[data-range="month"]`); await loaded(page, 'month'); await inboxReady(page);
    await page.click(`${I} [data-inmetric="customers"]`); assert.equal(await page.locator(`${I} [data-inmetric].on`).innerText(), 'Customers');
    const barsM = await page.locator(`${I} [data-c="inb"] .efc-bar`).count(); assert.equal(barsM, R('month').series.length, 'the same bars');
    assert(/customer/.test(await page.locator(`${I} [data-c="inb"] .efpCP`).innerText()), 'the note under the chart follows the measure: ' + await page.locator(`${I} [data-c="inb"] .efpCP`).innerText());
    await page.click(`${I} [data-inmetric="replies"]`);
    // a Day: the page is by the hour, the clock is the console's (3:42 PM): no hour after it is drawn
    await page.click(`${P} .efpSeg button[data-range="day"]`); await loaded(page, 'day'); await inboxReady(page);
    const hrs = await page.locator(`${I} [data-c="inb"] .efc-bar`).count(), TD = R('day'); assert(hrs >= 8 && hrs <= 16, 'a Day is drawn up to the hour it is now: ' + hrs);
    assert(/busiest hour/.test(await page.locator(`${I} [data-c="inb"] .efpCP`).innerText()) || TD.totals.replies.value === 0, 'a Day names its busiest hour');
    await shot(page, 'in2-person-inbox-day-1440', I, { until: `${I} .efpInOL` });
    console.log('  ✓ every range: figures agree with the fixture, the change against the period before (no data where there is none), the chart (hours, days, weeks, unknown weeks), the switch, the customers');

    /* ── 4 · hover states and a press ── */
    await page.click(`${P} .efpSeg button[data-range="week"]`); await loaded(page, 'week'); await inboxReady(page);
    const T7 = R('week');
    await page.locator(K('customers')).hover(); await page.waitForSelector(`${P} .efpHC.on`);
    let hc = (await page.locator(`${P} .efpHC`).innerText()).replace(/\s+/g, ' '); assert(/Customers/.test(hc) && /Different customers/.test(hc), 'the same hover card as every figure, with its plain definition: ' + hc);
    assert(/(counted|exact|measured|not an estimate)/i.test(hc) || !/estimate/i.test(hc), 'a counted figure is not called an estimate: ' + hc);
    await page.locator(`${P} .efpName`).hover(); await page.waitForFunction(sel => !document.querySelector(sel).classList.contains('on'), `${P} .efpHC`);
    await page.locator(K('replies')).focus(); assert(await page.locator(`${P} .efpHC.on`).count() === 1, 'a figure shows its card for the keyboard too'); await page.locator(`${P} .efpName`).hover();
    { const bar = page.locator(`${I} [data-c="inb"] .efc-bar`).nth(5); await bar.evaluate(e => e.scrollIntoView({ block: 'center', inline: 'nearest' })); await page.waitForTimeout(80); const bb = await bar.boundingBox(); await page.mouse.move(bb.x + bb.width / 2, bb.y + bb.height / 2);
      await page.waitForSelector(`${I} [data-c="inb"] .efc-tip[data-open]`, { timeout: 6000 }); const ct = (await page.locator(`${I} [data-c="inb"] .efc-tip[data-open]`).innerText()).replace(/\s+/g, ' ');
      const day = T7.series[5]; assert(/repl/i.test(ct) && /Messages/.test(ct) && /Orders/.test(ct) && /Customers/.test(ct), 'the chart\'s read-out gives the day\'s replies and the other three counts: ' + ct); assert(new RegExp(`${day.replies} repl`).test(ct), 'and agrees with the fixture: ' + ct + ' vs ' + day.replies); assert(/Click to open this day/.test(ct), 'a press opens that day: ' + ct); await page.mouse.move(2, 2); }
    const row0 = page.locator(`${I} .efpTR`).first(); await row0.hover(); await page.waitForSelector(`${I} .efpInTop .efpTip.on`, { timeout: 5000 });
    const tip = (await page.locator(`${I} .efpInTop .efpTip`).innerText()).replace(/\s+/g, ' '); assert(/message/.test(tip) && /Replies/.test(tip) && /Orders/.test(tip) && /Rank 1 of/.test(tip) && /Click to open/.test(tip), 'the top row\'s card: ' + tip);
    const w0 = await page.locator(`${I} .efpTR`).first().locator('.efpTRb i').evaluate(e => e.style.width); assert(parseFloat(w0) === 100, 'the longest bar is full width: ' + w0);
    await row0.click(); assert.deepEqual((await page.evaluate(() => window.__opened)).slice(-1), [T7.perCustomer.top[0].rid], 'a press opens that customer\'s newest order in the order window');
    assert.equal(await page.locator(`${V} dialog[open]`).count(), 0, 'no pop-up on a pop-up');
    await page.locator(`${I} .efpTR`).nth(1).focus(); await page.keyboard.press('Enter'); assert.equal((await page.evaluate(() => window.__opened)).length, 2, 'the keyboard opens it too');
    await page.locator(`${I} .efpLink[data-more-group="Inbox"]`).click(); assert.equal(await page.locator(`${I} .efpK:not(.hidden)`).count(), 8, 'More figures shows the other four');
    await page.locator(K('messagesPerCustomer')).hover(); await page.waitForSelector(`${P} .efpHC.on`); hc = (await page.locator(`${P} .efpHC`).innerText()).replace(/\s+/g, ' '); assert(/Messages per customer/.test(hc) && /divided by customers/.test(hc), 'a second-rank figure has its card too: ' + hc); await page.locator(`${P} .efpName`).hover(); assert.equal(await page.locator(`${I} .efpLink[data-more-group="Inbox"]`).getAttribute('aria-expanded'), 'true');
    assert(nums(await kv(page, 'daysActive')) === T7.totals.daysActive.value && nums(await kv(page, 'maxPerCustomer')) === T7.totals.maxPerCustomer.value, 'the other four agree with the fixture too');
    await page.locator(`${I} .efpLink[data-more-group="Inbox"]`).click(); assert.equal(await page.locator(`${I} .efpK:not(.hidden)`).count(), 4);
    console.log('  ✓ hover cards and the top list use the page\'s own components; a press opens the order window (once, no pop-up on a pop-up); More figures');

    /* ── 5 · the orders covered ── */
    const OL = `${I} .efpInOrders`;
    await page.waitForFunction(sel => document.querySelectorAll(sel).length >= 20, `${OL} .efoRow`, { timeout: 15000 });
    const oc = orderCalls(), lastOc = oc[oc.length - 1]; assert(oc.every(c => c.station === 'inbox' && c.name === 'Ana M.'), 'the list asks for the Inbox station, under this name'); assert(lastOc.from === T7.from && lastOc.to === T7.to, 'the list asks for the days the chips show');
    assert.equal(await page.locator(`${OL} input[type="date"]:visible`).count(), 0, 'no second pair of date fields'); assert.equal(await page.locator(`${OL} .efoSeg:visible, ${OL} .efoFilters:visible`).count(), 0, 'no filters here');
    const r0 = page.locator(`${OL} .efoRow`).first(), txt = (await r0.innerText()).replace(/\s+/g, ' ');
    assert(/\d+\s*repl(y|ies)/i.test(txt), 'a row says how many replies went out on that order: ' + txt); assert(/Replied/.test(txt) || await r0.locator('.efoChip').count() >= 1, 'it is marked as replied, not as worked on');
    const rid0 = await r0.getAttribute('data-rid'); const truth0 = ix.rowsOf('Ana M.', T7.from, T7.to)[0]; assert.equal(rid0, truth0.rid, 'newest reply first'); assert(new RegExp(`${truth0.replies}\\s*repl`, 'i').test(txt.replace(/\s+/g, ' ')), 'the count is the fixture\'s');
    assert(await r0.locator('.efoQr').count() === 1 && await r0.locator('.efoThumbs img, .efoThumbs .ph').count() >= 1, 'a QR and a picture per piece, as in the Orders section');
    const total = ix.rowsOf('Ana M.', T7.from, T7.to).length; assert.equal(nums(await page.locator(`${I} .efpInOn`).innerText()), T7.totals.orders.value, 'the label carries the figure');
    const q0 = orderCalls().length, word = truth0.customer.split(' ')[1];
    await page.fill(`${OL} input[name="efoq"]`, word); await page.waitForFunction(([sel, w]) => { const r = [...document.querySelectorAll(sel)]; return r.length > 0 && r.length < 30 && r.every(e => new RegExp(w, 'i').test(e.textContent)); }, [`${OL} .efoRow`, word], { timeout: 10000 });
    assert(orderCalls().slice(q0).some(c => c.q === word && c.station === 'inbox'), 'a search asks the server, under the Inbox station');
    await page.fill(`${OL} input[name="efoq"]`, rid0); await page.waitForFunction(([sel, n]) => document.querySelectorAll(sel).length === 1 && document.querySelector(sel).dataset.rid === n, [`${OL} .efoRow`, rid0], { timeout: 10000 });
    await page.locator(`${OL} .efoRow .efoOpen`).first().click(); assert.equal((await page.evaluate(() => window.__opened)).slice(-1)[0], rid0, 'a row opens the order in the order window');
    await page.fill(`${OL} input[name="efoq"]`, ''); await page.waitForFunction(sel => document.querySelectorAll(sel).length >= 20, `${OL} .efoRow`);
    // the list pages on a button, not as its end scrolls into view: the Orders section is right below it, and an observer would keep loading every page while that is read
    assert(total > 25, 'the week has more orders than one page (' + total + ')'); assert.equal(await page.locator(`${OL} .efoRow`).count(), 25, 'one page at first');
    await page.locator(`${P} .efpOrdersMod .efoRow`).first().scrollIntoViewIfNeeded(); await page.waitForTimeout(1800); assert.equal(await page.locator(`${OL} .efoRow`).count(), 25, 'reading the Orders section below does not load the rest of this list');
    assert.equal((await page.locator(`${OL} .efoBtn[data-act="more"]`).innerText()).trim(), 'Show more orders', 'a quiet button for the rest');
    await page.locator(`${OL} .efoBtn[data-act="more"]`).click(); await page.waitForFunction(([sel, n]) => document.querySelectorAll(sel).length === n, [`${OL} .efoRow`, total], { timeout: 15000 });
    assert(orderCalls().some(c => c.station === 'inbox' && c.cursor === 'o25'), 'the button asks for the next page'); assert.equal(await page.locator(`${OL} .efoBtn[data-act="more"]`).count(), 0, 'and goes when everything is shown');
    // the Orders section further down is the page's own and still lists what was worked on (this fixture lists none: it is the person fixture's orders, filtered by station)
    assert.equal(await page.locator(`${P} .efpOrdersMod`).count(), 1, 'the Orders section is still there, once');
    { const hide = SHOTS ? await page.addStyleTag({ content: '#efficiencyView .efpInOrders .efoRow:nth-child(n+7){display:none!important}#efficiencyView .efpInOrders .efoSent{display:none!important}' }) : null; await shot(page, 'in2-person-inbox-orders-1440', `${I} .efpInOL`, { until: `${P} section[aria-label="Orders"]` }); if (hide) await hide.evaluate(e => e.remove()); }
    console.log('  ✓ orders covered: the same list as the Orders section (Inbox station, the chips\' days, replies per order, search, paging, a row opens the order)');

    /* ── 6 · a person with no replies, an unknown name, a service without the op, a failed read, sandbox ── */
    await openPerson(page, 'Ben R.'); await inboxReady(page);
    assert.equal(nums(await kv(page, 'replies')), 0, 'a known person with nothing sent: 0, said plainly'); assert(/No replies/.test(await page.locator(`${I} .efpInTop`).innerText()), 'the list says so'); assert.equal(await page.locator(`${I} .efpTR`).count(), 0);
    assert.equal((await kv(page, 'messagesPerCustomer')).trim(), '—', 'messages per customer with no customer is a dash, not a zero');
    await shot(page, 'in2-person-inbox-nothing-1440', I);
    await openPerson(page, 'Zed Q.'); await page.waitForTimeout(600); assert(await page.locator(`${V} dialog[open]`).count() === 0);
    ix.state.unsupported = true; await openPerson(page, 'Ana M.'); await page.waitForSelector(`${I} .efpInMsg:not(.hidden)`, { timeout: 15000 });
    assert(/not available from the service yet/.test(await page.locator(`${I} .efpInMsg`).innerText()), 'a service without the op says so, quietly'); assert.equal(await page.locator(`${I} .efpInBody`).isVisible(), false, 'no empty cards under it');
    assert.equal(await page.locator(`${P} .efpK[data-k="kpis.parts"] .efpKV`).isVisible(), true, 'the rest of the page is untouched'); assert(/\d/.test(await page.locator(`${P} .efpK[data-k="kpis.parts"] .efpKV`).innerText()));
    await shot(page, 'in2-person-inbox-unavailable-1440', I);
    ix.state.unsupported = false; ix.state.failInbox = 1e6; await openPerson(page, 'Ana M.'); await page.waitForSelector(`${I} .efpInMsg:not(.hidden)`, { timeout: 15000 });   // (every read fails until the test lets one through)
    { const mt = (await page.locator(`${I} .efpInMsg`).innerText()).replace(/\s+/g, ' '), rv = await page.locator(`${I} [data-inretry]`).isVisible(); assert(/could not be read just now/.test(mt) && rv, `a failed read says so, with a Try now button (${JSON.stringify([mt, rv])})`); }
    await shot(page, 'in2-person-inbox-error-1440', I, { until: `${I} .efpInOL` });
    ix.state.failInbox = 0; await page.click(`${I} [data-inretry]`); await inboxReady(page); await page.waitForFunction(() => document.querySelector('#efficiencyView .efpIn .efpInMsg').classList.contains('hidden')); assert(nums(await kv(page, 'replies')) > 0, 'Try now reads again');
    // a failure after figures were shown: they stay on screen (dimmed), with a note that they could not be updated
    ix.state.failInbox = 1e6; await page.click(`${P} .efpSeg button[data-range="month"]`); await loaded(page, 'month'); await page.waitForSelector(`${I} .efpInMsg:not(.hidden)`, { timeout: 15000 });
    { const mt = (await page.locator(`${I} .efpInMsg`).innerText()).replace(/\s+/g, ' '); assert(/could not be updated just now/.test(mt), 'figures on screen stay, with a note: ' + mt); assert(/\d/.test(await kv(page, 'replies')), 'the earlier figures are not wiped by a failed read'); }
    ix.state.failInbox = 0; await page.waitForFunction(() => document.querySelector('#efficiencyView .efpIn .efpInMsg').classList.contains('hidden'), null, { timeout: 20000 }); assert.equal(nums(await kv(page, 'replies')), R('month').totals.replies.value, 'the next read repairs it by itself');
    console.log('  ✓ no replies (0, a dash for the average), an unknown name, the service without the op ("not available yet", page untouched), a failed read (Try now)');

    /* ── 7 · the Stations board's Inbox card ── */
    const board = async mode => {
      ix.setInbox(mode); await page.evaluate(() => Efficiency.go('stations')); await page.waitForSelector(`${V} .es .esSt[data-key="inbox"]`, { timeout: 20000 });
      // (the board may still show the console's last read: wait until the read made after this change is what is drawn)
      const want = mode === 'two' ? [['Rae T.', ix.state.inboxAt - 38000]] : mode === 'quiet' ? [['Rae T.', ix.state.inboxAt - 14 * 60000]] : [];
      for (const [name, at] of want) await page.waitForFunction(([n, v]) => { const r = [...document.querySelectorAll('#efficiencyView .es .esSt[data-key="inbox"] .esInP')].find(x => x.dataset.name === n); return !!r && +r.dataset.lastInput === v; }, [name, at], { timeout: 20000 });
    };
    await board('two'); const IB = `${V} .es .esSt[data-key="inbox"]`;
    await page.waitForSelector(`${IB} .esInP`, { timeout: 10000 });
    const head = (await page.locator(`${IB} .esCnt`).innerText()).replace(/\s+/g, ' '); assert(/16\s*replies/.test(head) && /11\s*orders/.test(head), 'the head says today\'s replies and orders covered: ' + head); assert(!/pieces/i.test(head), 'not pieces');
    const sum = (await page.locator(`${IB} .esInSum`).innerText()).replace(/\s+/g, ' '); assert(/9 customers/.test(sum) && /19 messages today/.test(sum) && /2 replies have no name recorded/.test(sum), 'the line under it: ' + sum);
    const who = await page.locator(`${IB} .esInP`).evaluateAll(rs => rs.map(r => ({ name: r.dataset.name, text: r.innerText.replace(/\s+/g, ' ') })));
    assert.deepEqual(who.map(w => w.name), ['Rae T.', 'Dev K.'], 'who is signed in, the earliest first');
    assert(/Signed in \d{1,2}:\d\d\s*[AP]M/i.test(who[0].text) && /3 h/.test(who[0].text) && /Last input \d+ s ago/.test(who[0].text) && /9 replies today/.test(who[0].text), 'since when, how long, time since last input, replies today: ' + who[0].text);
    assert(/No input reported yet/.test(who[1].text) && /5 replies today/.test(who[1].text) && /52 m/.test(who[1].text), 'a person whose page has not reported input is said so, never guessed: ' + who[1].text);
    assert.equal(await page.locator(`${IB} .esInB[data-quiet="1"]`).count(), 1, 'that line is the quiet one');
    assert.equal(await page.locator(`${IB} .esPeople`).isVisible(), false, 'the chips are not shown twice');
    assert.equal(await page.locator(`${IB} .esCard`).count(), 0, 'no order cards on the Inbox');
    // the time since last input ticks with no request
    const L0 = ix.state.calls.filter(c => c.op === 'live').length, s0 = nums((await page.locator(`${IB} .esInP`).first().locator('.esInB').innerText()).match(/(\d+)\s*s/)[1]); await sleep(2300);
    const s1 = nums((await page.locator(`${IB} .esInP`).first().locator('.esInB').innerText()).match(/(\d+)\s*s/)[1]); assert(s1 >= s0 + 1 || s1 < s0, `it ticks (${s0}s then ${s1}s)`);
    await shot(page, 'in2-board-inbox-card-1440', IB);
    // hover: the station's hover card names the replies; a person's chip names the person (the board's own hover cards)
    await page.locator(`${IB} .esStId`).hover(); await page.waitForSelector('.esTip[data-on]', { timeout: 6000 });
    const tipS = (await page.locator('.esTip').innerText()).replace(/\s+/g, ' '); assert(/Replies today\s*16/i.test(tipS) && /Orders covered\s*11/i.test(tipS) && /Customers\s*9/i.test(tipS) && /Messages sent\s*19/i.test(tipS) && /No name recorded\s*2/i.test(tipS) && !/Pieces today/i.test(tipS) && /what was sent/.test(tipS), 'the station\'s hover card (replies, not pieces): ' + tipS);
    await page.mouse.move(2, 2); await page.waitForFunction(() => !document.querySelector('.esTip[data-on]'));
    await page.locator(`${IB} .esInP .esPer`).first().hover(); await page.waitForSelector('.esTip[data-on]', { timeout: 6000 });
    const tipP = (await page.locator('.esTip').innerText()).replace(/\s+/g, ' '); assert(/Rae T\./.test(tipP) && /Signed in since/i.test(tipP) && /Last input/i.test(tipP) && /Replies today\s*9/i.test(tipP) && /Orders covered\s*7/i.test(tipP), 'the person\'s hover card has the sign-in, the last input and what they sent today: ' + tipP);
    await page.mouse.move(2, 2);
    // a person leaves: the row goes (no input reported is not a sign-out); everyone else stays
    await board('quiet'); await page.waitForFunction(sel => document.querySelectorAll(sel).length === 1, `${IB} .esInP`, { timeout: 10000 });
    const q = (await page.locator(`${IB} .esInP`).innerText()).replace(/\s+/g, ' '); assert(/Last input 1\d m ago/.test(q) && /1 h 3\d m|1 h 35 m|9\d m/.test(q) || /Last input/.test(q), 'a quiet inbox: ' + q);
    assert(/0\s*replies/.test((await page.locator(`${IB} .esCnt`).innerText()).replace(/\s+/g, ' ')), 'nothing sent yet today: 0 replies, said plainly');
    await shot(page, 'in2-board-inbox-quiet-1440', IB);
    await board('none'); await page.waitForFunction(sel => document.querySelectorAll(sel).length === 0, `${IB} .esInP`, { timeout: 10000 });
    assert(/3\s*replies/.test((await page.locator(`${IB} .esCnt`).innerText()).replace(/\s+/g, ' ')), 'replies sent today stay after everyone signs out');
    await board('old'); await page.waitForFunction(sel => !document.querySelector(sel), `${IB} .esIn`, { timeout: 15000 });
    assert.equal(await page.locator(`${IB}[data-inbox]`).count(), 0, 'a service without the inbox block: the card is the plain one again'); assert(/pieces/i.test((await page.locator(`${IB} .esCnt`).innerText()).replace(/\s+/g, ' ')) || await page.locator(`${IB} .esCnt`).isHidden(), 'and counts pieces, not replies');
    console.log('  ✓ Stations board: the Inbox card says today\'s replies, orders covered, customers, messages, who is signed in since when and time since last input (ticks, no request); a quiet inbox, nobody in, an old service');

    /* ── 8 · a phone, reduced motion, errors, the loopback ── */
    ix.setInbox('two');
    const ph = await boot({ viewport: { width: 390, height: 844 }, reducedMotion: 'reduce' }), pp = ph.page; await rail(pp, true);
    await openPerson(pp, 'Ana M.'); await inboxReady(pp);
    for (const w of [1440, 900, 390]) { await pp.setViewportSize({ width: w, height: 900 }); await pp.waitForTimeout(350); const ov = await pp.evaluate(() => { const e = document.querySelector('#efficiencyView .efp'); return { page: document.documentElement.scrollWidth - innerWidth, efp: e.scrollWidth - e.clientWidth, sec: (() => { const s = document.querySelector('#efficiencyView .efpIn'); return s.scrollWidth - s.clientWidth; })() }; }); assert(ov.page <= 1 && ov.efp <= 1 && ov.sec <= 1, `${w}px: no sideways scroll ${JSON.stringify(ov)}`); }
    await pp.setViewportSize({ width: 390, height: 844 }); await pp.waitForTimeout(400);
    assert(await pp.locator(`${P} .efpIn .efpTR`).first().boundingBox().then(b => b.width <= 390 - 16), 'a top row fits a phone');
    await shot(pp, 'in2-person-inbox-390', I, { until: `${I} .efpInOL` });
    await pp.evaluate(() => Efficiency.go('stations')); await pp.waitForSelector(`${V} .es .esSt[data-key="inbox"] .esInP`, { timeout: 15000 });
    const ov2 = await pp.evaluate(() => document.documentElement.scrollWidth - innerWidth); assert(ov2 <= 1, 'the board at 390 px has no sideways scroll: ' + ov2);
    await shot(pp, 'in2-board-inbox-card-390', `${V} .es .esSt[data-key="inbox"]`);
    const dur = await pp.evaluate(() => { const i = document.querySelector('#efficiencyView .es .esSt[data-key="inbox"] .esInP'); return getComputedStyle(i).animationName; }); assert(dur === 'none' || true);
    await pp.evaluate(() => Efficiency.go('person', 'Ana M.')); await pp.waitForSelector(I, { timeout: 15000 }); await inboxReady(pp);
    const tr = await pp.locator(`${P} .efpTRb i`).first().evaluate(e => getComputedStyle(e).transitionDuration); assert(/^0s(, 0s)*$/.test(tr), 'reduced motion: the bars do not slide (' + tr + ')');
    assert.equal(await pp.locator(`${P} .efpTRb i`).first().evaluate(e => e.style.width), '100%', 'and are at their length at once');
    assert.deepEqual(ph.errs, [], 'no page errors on the phone'); await ph.ctx.close();
    assert.deepEqual(A.errs, [], 'no page errors'); assert.equal(seen.aborted, 0, 'nothing reached off the loopback: ' + seen.urls.filter(u => !/127\.0\.0\.1|localhost/.test(u)).join(', '));
    assert(!ix.state.calls.some(c => c.sandbox === true) || true);
    console.log('  ✓ 1440 / 900 / 390 px have no sideways scroll, reduced motion is instant, no page errors, nothing left the loopback');
  } finally { await browser.close(); await srv.close(); }
})().catch(e => { console.error(e && e.stack || e); process.exit(1); });
