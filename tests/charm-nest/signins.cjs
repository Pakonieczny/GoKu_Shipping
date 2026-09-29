// The sorter's Sign-ins window and its read (plans/sign-in-sessions.md, part L). Fakes only: an in-memory Firestore for
// the op, and in the browser a local site whose every request off the loopback is aborted.
//   1 · sessionsList: one range on startAt, newest first, bounded (span, count, `truncated`); times kept as milliseconds
//       or as Firestore times are both read; an open session whose heartbeat stopped 15 min ago is closed at its last
//       beat, one still beating is live; no employee id leaves; behind the passcode gate.
//   2 · the view's New York days (daylight saving), a person's time counted once over two computers, the day a session
//       started in, a fake clock crossing midnight.
//   3 · the window (when Playwright is here): opens from the Workspace menu with the app's grow, a spinner while it reads,
//       who is signed in now with a live duration, per person / per computer, totals per day; reads only while open;
//       past midnight (the server's clock, faked) today becomes Yesterday.
//   node tests/charm-nest/signins.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

/* ── 1 · the op ── */
class TS { constructor(ms) { this.ms = ms; } toMillis() { return this.ms; } static fromMillis(ms) { return new TS(ms); } }
const store = new Map(), used = [];
const kind = v => (v instanceof TS ? 'ts' : typeof v);
function query(coll, filters = [], order = null, lim = 0) {
  return {
    where(f, op, v) { used.push(f); return query(coll, filters.concat([[f, op, v]]), order, lim); },
    orderBy(f, dir) { used.push(f); return query(coll, filters, [f, dir || 'asc'], lim); },
    limit(n) { return query(coll, filters, order, n); },
    doc(id) { return { id, get: async () => ({ exists: store.has(coll + '/' + id), data: () => store.get(coll + '/' + id) }) }; },
    async get() {
      let rows = [...store].filter(([k]) => k.startsWith(coll + '/')).map(([k, v]) => ({ id: k.slice(coll.length + 1), v }));
      for (const [f, op, v] of filters) rows = rows.filter(({ v: d }) => {
        const x = d[f]; if (x == null || kind(x) !== kind(v)) return false;   // a range never matches the other kind of value
        const a = x instanceof TS ? x.ms : x, b = v instanceof TS ? v.ms : v;
        return op === '>=' ? a >= b : op === '<' ? a < b : op === '>' ? a > b : op === '<=' ? a <= b : a === b;
      });
      if (order) rows.sort((p, q) => { const a = p.v[order[0]], b = q.v[order[0]], x = a instanceof TS ? a.ms : a, y = b instanceof TS ? b.ms : b; return order[1] === 'desc' ? y - x : x - y; });
      if (lim) rows = rows.slice(0, lim);
      const docs = rows.map(r => ({ id: r.id, data: () => Object.assign({}, r.v) }));
      return { docs, size: docs.length, empty: !docs.length };
    }
  };
}
const db = { collection: c => query(c), batch: () => ({ set() {}, commit: async () => {} }) };
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => ({}), delete: () => ({}) }, FieldPath: { documentId: () => '__name__' }, Timestamp: TS }), storage: () => ({ bucket: () => ({}) }) };
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return fakeAdmin;
  return realLoad.call(this, req, ...rest);
};
const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
const call = (body, headers = {}) => lib.handler({ httpMethod: 'POST', headers, body: JSON.stringify(body) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const MIN = 60000, NOW = Date.now();
const put = (id, d) => store.set('Station_Sessions/' + id, d);

(async () => {
  // Ana: ended by signing out (the server's minutes kept); Ben: still beating (live); Cy: heartbeat stopped 40 min ago
  // (closed at his last beat); Dee: times kept as Firestore times, ended at midnight; Old: outside the span asked for.
  put('ana1', { id: 'ana1', person: 'Ana P.', employeeId: '123456', station: 'assembly', device: 'assembly-2', computerId: 'c-aaaa1111', computerLabel: 'Assembly 2 · a111', startAt: NOW - 300 * MIN, lastSeenAt: NOW - 181 * MIN, endAt: NOW - 180 * MIN, endReason: 'signOut', minutes: 120 });
  put('ben1', { id: 'ben1', person: 'Ben', employeeId: '654321', station: 'shipping', device: 'shipping-1', computerId: 'c-bbbb', computerLabel: 'Shipping 1 · b222', startAt: NOW - 95 * MIN, lastSeenAt: NOW - 3 * MIN });
  put('cy1', { id: 'cy1', person: 'Cy', station: 'welding', device: 'weld-1', computerId: 'c-cccc', computerLabel: 'Weld 1 · c333', startAt: NOW - 100 * MIN, lastSeenAt: NOW - 40 * MIN });
  put('dee1', { id: 'dee1', person: 'Dee', station: 'sorting', computerId: 'c-dddd', startAt: new TS(NOW - 26 * 60 * MIN), lastSeenAt: new TS(NOW - 20 * 60 * MIN), endAt: new TS(NOW - 20 * 60 * MIN), endReason: 'midnight', minutes: 360 });
  put('old1', { id: 'old1', person: 'Old', station: 'sorting', startAt: NOW - 90 * 24 * 60 * MIN, lastSeenAt: NOW - 90 * 24 * 60 * MIN });
  put('bad1', { id: 'bad1', person: 'No start', station: 'sorting' });
  used.length = 0;
  let r = await call({ op: 'sessionsList', since: NOW - 3 * 24 * 60 * MIN });
  assert.equal(r.status, 200, JSON.stringify(r.body));
  assert(used.length && used.every(f => f === 'startAt'), 'one field only (its single-field index): ' + used.join());
  assert.deepEqual(r.body.sessions.map(s => s.id), ['ben1', 'cy1', 'ana1', 'dee1'], 'newest first, both kinds of time, span kept');
  const by = Object.fromEntries(r.body.sessions.map(s => [s.id, s]));
  assert.equal(by.ana1.endReason, 'signOut'); assert.equal(by.ana1.minutes, 120); assert.equal(by.ana1.live, false);
  assert.equal(by.ben1.live, true); assert.equal(by.ben1.endAt, null); assert(Math.abs(by.ben1.minutes - 95) < 1, 'live minutes to now: ' + by.ben1.minutes);
  assert.equal(by.cy1.endReason, 'closed'); assert.equal(by.cy1.endAt, NOW - 40 * MIN); assert.equal(by.cy1.minutes, 60);
  assert.equal(by.dee1.endReason, 'midnight'); assert.equal(by.dee1.startAt, NOW - 26 * 60 * MIN);
  assert(!JSON.stringify(r.body).includes('123456') && !JSON.stringify(r.body).includes('employeeId'), 'no employee id (it can be the PIN) leaves');
  assert(r.body.now >= NOW && r.body.truncated === false);
  // bounded: the count, and the span (a year asked for is 62 days)
  r = await call({ op: 'sessionsList', since: NOW - 3 * 24 * 60 * MIN, limit: 2 });
  assert.deepEqual(r.body.sessions.map(s => s.id), ['ben1', 'cy1']); assert.equal(r.body.truncated, true);
  r = await call({ op: 'sessionsList', since: 1, limit: 99999 });
  assert(r.body.until - r.body.since <= 62 * 24 * 60 * MIN, 'span capped'); assert(!r.body.sessions.some(s => s.id === 'old1'));
  r = await call({ op: 'sessionsList', since: NOW, until: NOW - MIN });
  assert.equal(r.status, 400);
  // the gate: with a passcode set, no read without it
  process.env.EDIT_PASSCODE = 'sesame';
  assert.equal((await call({ op: 'sessionsList' })).status, 401);
  assert.equal((await call({ op: 'sessionsList' }, { 'X-Edit-Passcode': 'sesame' })).status, 200);
  delete process.env.EDIT_PASSCODE;
  console.log('  ✓ sessionsList: one range on startAt, bounded, closed/live worked out when read, no id, gated');

  /* ── 2 · the view's days and totals ── */
  const win = { document: { querySelector: () => null, getElementById: () => null, addEventListener() {} }, console };
  win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-signins.js'), 'utf8'), win);
  const SI = win.SignIns;
  assert.equal(new Date(SI.nyMidnight('2026-09-28')).toISOString(), '2026-09-28T04:00:00.000Z', 'EDT midnight');
  assert.equal(new Date(SI.nyMidnight('2026-12-01')).toISOString(), '2026-12-01T05:00:00.000Z', 'EST midnight');
  assert.equal(new Date(SI.nyMidnight('2026-11-02')).toISOString(), '2026-11-02T05:00:00.000Z', 'the day after the clocks go back');
  const mid = SI.nyMidnight('2026-09-29');
  assert.equal(SI.nyDay(mid - 1), '2026-09-28'); assert.equal(SI.nyDay(mid), '2026-09-29');
  // across midnight: a session before it belongs to the 28th, one after to the 29th; Ana on two computers at once counts once
  const S = [
    { id: 'a', person: 'Ana', computerId: 'c1', computerLabel: 'Assembly 1', station: 'assembly', startAt: mid - 120 * MIN, endAt: mid - 60 * MIN, minutes: 60, endReason: 'signOut' },
    { id: 'b', person: 'Ana', computerId: 'c2', computerLabel: 'Shipping 1', station: 'shipping', startAt: mid - 90 * MIN, endAt: mid - 30 * MIN, minutes: 60, endReason: 'switched' },
    { id: 'c', person: 'ana', computerId: 'c1', computerLabel: 'Assembly 1', station: 'assembly', startAt: mid + 10 * MIN, live: true }
  ];
  const now = mid + 40 * MIN, M = JSON.parse(JSON.stringify(SI.model(S, now)));   // (out of the vm's realm)
  assert.equal(M.today, '2026-09-29');
  assert.deepEqual(M.days.map(d => d.day), ['2026-09-29', '2026-09-28']);
  assert.equal(M.days[1].people.length, 1); assert.equal(Math.round(M.days[1].people[0].minutes), 90, 'overlap counted once');
  assert.equal(M.days[1].computers.length, 2);
  assert.equal(M.live.length, 1); assert.equal(Math.round(M.live[0].minutes), 30);
  assert.equal(M.totals.length, 1); assert.equal(Math.round(M.totals[0].days['2026-09-28']), 90); assert.equal(Math.round(M.totals[0].days['2026-09-29']), 30); assert.equal(Math.round(M.totals[0].total), 120);
  assert.equal(SI.dur(0.4), 'under 1 min'); assert.equal(SI.dur(59.6), '1 h'); assert.equal(SI.dur(125), '2 h 5 min');
  console.log('  ✓ New York days (DST), a session keeps the day it began, a person counted once over two computers');

  /* ── 3 · the window ── */
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  Module._load = realLoad;
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    // the server's clock: 20 s before a New York midnight (the page's own clock is left alone)
    const srvNow0 = SI.nyMidnight('2026-09-29') - 20000, t0 = Date.now();
    const srvNow = () => srvNow0 + (Date.now() - t0);
    let reads = 0, delay = 700;
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url());
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') return route.abort();
      if (u.pathname.endsWith('/charmNestLibrary') && route.request().method() === 'POST') {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        if (b.op === 'sessionsList') {
          reads++; await new Promise(r => setTimeout(r, delay));
          const n = srvNow();
          const sessions = [
            { id: 's-ben', person: 'Ben', station: 'shipping', device: 'shipping-1', computerId: 'c-b', computerLabel: 'Shipping 1 · b222', startAt: n - 95 * MIN, lastSeenAt: n - 2 * MIN, endAt: null, endReason: null, minutes: 95, live: true },
            { id: 's-ana', person: 'Ana P.', station: 'assembly', device: 'assembly-2', computerId: 'c-a', computerLabel: 'Assembly 2 · a111', startAt: n - 300 * MIN, lastSeenAt: n - 181 * MIN, endAt: n - 180 * MIN, endReason: 'signOut', minutes: 120, live: false },
            { id: 's-cy', person: 'Cy', station: 'welding', device: 'weld-1', computerId: 'c-c', computerLabel: 'Weld 1 · c333', startAt: n - 30 * 60 * MIN, lastSeenAt: n - 26 * 60 * MIN, endAt: n - 26 * 60 * MIN, endReason: 'closed', minutes: 240, live: false }
          ];
          return route.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ sessions, truncated: false, since: b.since, until: n + 60000, now: n }) });
        }
      }
      return route.continue();
    });
    await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.SignIns && window.CN && document.readyState === 'complete', null, { timeout: 60000 });
    await page.evaluate(() => { SignIns.options.pollMs = 1500; SignIns.options.tickMs = 400; });
    assert.equal(await page.locator('#moreMenu .moreList #btnSignins').count(), 1, 'in the Workspace menu');
    assert.equal(await page.evaluate(() => document.getElementById('btnSignins').nextElementSibling.id), 'btnSettings', 'just above Settings');
    await page.click('#moreMenu > summary');
    await page.click('#btnSignins');
    await page.waitForSelector('#dlgSignins[open]');
    assert.equal(await page.evaluate(() => document.getElementById('moreMenu').open), false, 'the menu closes');
    assert(await page.evaluate(() => !!document.getElementById('dlgSignins')._mdShown), 'opened through the app\'s grow (Motion.dialogOpen)');
    assert(await page.locator('#dlgSignins .siBody .spin').isVisible(), 'a spinner while it reads');
    await page.waitForSelector('#dlgSignins .siLive', { timeout: 5000 });
    const live = await page.locator('#dlgSignins .siLive').innerText();
    assert(/Ben/.test(live) && /Shipping · shipping-1/.test(live) && /Shipping 1 · b222/.test(live) && /1 h 35 min/.test(live), 'who, where, which computer, how long: ' + live);
    const head = await page.locator('#dlgSignins .siT').first().innerText();
    assert(/today/i.test(head) && /Ana P\./.test(head) && /2 h/.test(head) && /Cy/.test(head) && /4 h/.test(head), 'time per person per day: ' + head);
    const body = await page.locator('#dlgSignins .siBody').innerText();
    assert(/Signed out/.test(body) && /Closed · no heartbeat/.test(body) && /Signed in/.test(body), 'why each ended');
    await page.click('#dlgSignins .seg[data-k="by"] button[data-v="computer"]');
    assert(/Assembly 2 · a111/.test(await page.locator('#dlgSignins .siGHead').first().innerText() + await page.locator('#dlgSignins .siBody').innerText()), 'per computer');
    // reads again while open; past the (faked) midnight "Today" becomes "Yesterday"
    const r1 = reads;
    assert(!/No sign-ins yet today/.test(body), 'before midnight today has its sign-ins');
    await page.waitForFunction(() => /No sign-ins yet today/.test(document.querySelector('#dlgSignins .siBody').innerText), null, { timeout: 30000 });
    assert(/Yesterday · /i.test(await page.locator('#dlgSignins .siBody').innerText()), 'the day before is Yesterday now');
    await page.waitForTimeout(2000);
    assert(reads > r1, 'polls while open');
    // closed: no more reads, no timers
    await page.click('#dlgSignins [data-close]');
    await page.waitForFunction(() => !document.getElementById('dlgSignins').open);
    await page.waitForTimeout(400);
    const r2 = reads; await page.waitForTimeout(4000);
    assert.equal(reads, r2, 'no reads once closed');
    assert.equal(await page.evaluate(() => SignIns.isOpen), false);
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    console.log('  ✓ the window: Workspace menu, grow, spinner, live now, per person / computer, totals, polls only while open, midnight');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
