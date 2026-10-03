// The console against the REAL read function (netlify/functions/employeeEfficiency.js) over an in-memory Firestore: the shape
// the function sends is the shape the screen takes, so a change on either side shows here. Fakes only (four invented people,
// a synthetic passcode); the browser is served the function's own answers, every other request off the loopback is aborted.
//   node tests/charm-nest/efficiency-real.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir> to keep a screenshot)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict'), Module = require('module');
const root = path.join(__dirname, '../..');

/* ── a small in-memory Firestore (typed range filters, orderBy, limit, doc get/set) ── */
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
const T = mod._t, PASS = 'synthetic-pass-ef-real';
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));   // (the passcode is set only while the function answers: the sorter's own functions in the test server stay open)
const NOW = Date.parse('2026-10-02T19:42:00Z');   // 3:42 PM, 2 Oct, New York
const realNow = Date.now; Date.now = () => NOW;
const Z = iso => Date.parse(iso), MIN = 60000;
const answer = async body => { process.env.EDIT_PASSCODE = PASS; EP.resetCache(); const r = await T.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.9' }, body: JSON.stringify(Object.assign({ key: PASS }, body)) }, db); delete process.env.EDIT_PASSCODE; EP.resetCache(); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };

/* ── seed: today's rollups, sessions, events, and a week behind ── */
const stat = o => Object.assign({ scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0 }, o);
const hrs = (map, station) => Object.fromEntries(Object.entries(map).map(([h, parts]) => [String(h).padStart(2, '0'), { parts, scans: Math.round(parts * 1.4), by: { [station]: { parts, scans: Math.round(parts * 1.4) } } }]));
const sess = (id, person, station, start, o = {}) => ({ id, person, station, device: station + '-1', computerId: 'pc-' + id, computerLabel: '', startAt: start, lastSeenAt: o.last || start, endAt: o.end || null, endReason: o.reason || null, minutes: o.end ? Math.round((o.end - start) / MIN) : null });
const roll = (day, person, stations, hours, touched, a, b) => ({ day, person, v: 1, events: 50, firstAt: a, lastAt: b, stations, hours, touched });
const touch = (n, from, st) => Object.fromEntries(Array.from({ length: n }, (_, i) => [String(3521000000 + from + i), { [st]: true }]));
function seed() {
  const at = (h, m) => Z('2026-10-02T04:00:00Z') + (h * 60 + m) * MIN;   // New York clock time today
  const people = [
    ['Giovanna', 'welding', { 8: 14, 9: 28, 10: 31, 11: 22, 13: 30, 14: 34, 15: 19 }, 37, 186, 392, 33, [7, 52], null],
    ['Anna', 'assembly', { 8: 9, 9: 17, 10: 21, 11: 19, 13: 20, 14: 23, 15: 12 }, 29, 143, 361, 71, [8, 3], null],
    ['Michael', 'shipping', { 8: 11, 9: 15, 10: 18, 11: 14, 13: 9, 14: 16, 15: 8 }, 33, 131, 301, 52, [8, 0], null],
    ['Ivy', 'design', { 8: 5, 9: 7, 10: 9, 11: 6, 13: 8, 14: 4 }, 12, 64, 288, 44, [8, 15], [14, 15]]
  ];
  people.forEach(([name, st, h, orders, scans, act, idle, inn, out], i) => {
    const parts = Object.values(h).reduce((a, b) => a + b, 0);
    data('Efficiency_Daily').set('2026-10-02__' + name, roll('2026-10-02', name, { [st]: stat({ scans, scanParts: scans, completes: Math.round(parts / 4), parts, orders, prints: 9, activeMs: act * MIN, idleMs: idle * MIN, firstAt: at(...inn), lastAt: at(out ? out[0] : 15, out ? out[1] : 40) }) }, hrs(h, st), touch(orders, i * 100, st), at(...inn), at(15, 40)));
    data('Station_Sessions').set('s-' + name, sess('s-' + name, name, st, at(...inn), out ? { end: at(...out), last: at(...out), reason: 'signOut' } : { last: NOW - 2 * MIN }));
    for (let d = 1; d <= 6; d++) { const day = new Date(Z('2026-10-02T12:00:00Z') - d * 86400000).toISOString().slice(0, 10), p2 = 180 + ((d * 37 + i * 11) % 120); data('Efficiency_Daily').set(day + '__' + name, roll(day, name, { [st]: stat({ scans: 90, scanParts: 90, completes: 20, parts: p2, orders: 20 + d, activeMs: 300 * MIN, idleMs: 50 * MIN, firstAt: 1, lastAt: 2 }) }, hrs({ 10: p2 }, st), touch(20 + d, d * 1000, st), 1, 2)); }
    for (let k = 0; k < 12; k++) { const id = `${st}-1_AAAA_${i}${k}_${k}`, t = NOW - (k * 4 + i) * MIN; data('Station_Activity').set(id, { id, station: st, device: st + '-1', computer: 'pc-AAAA', session: '', person: name, action: k % 3 ? 'scan' : 'complete', orderId: String(3521000000 + i * 100 + k), line: '', sku: '', parts: k % 3 ? 1 : 4, orders: 0, detail: '', at: t, seq: k, sincePrevMs: 60000, ts: Ts.fromMillis(t), serverAt: t, day: '2026-10-02', hour: '14', v: 1 }); }
  });
}
seed();

(async () => {
  /* ── the real answer, through the console's own view model ── */
  const win = { document: { getElementById: () => null, addEventListener() {} }, console }; win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), win);
  const j = o => JSON.parse(JSON.stringify(o));
  const r = await answer({ op: 'overview' }); assert.equal(r.status, 200, JSON.stringify(r.body).slice(0, 300));
  const M = j(win.Efficiency.norm(r.body));
  assert.equal(M.people.length, 4, 'four people'); assert.deepEqual(M.people.map(p => p.name).sort(), ['Anna', 'Giovanna', 'Ivy', 'Michael']);
  const gio = M.people.find(p => p.name === 'Giovanna');
  assert.equal(gio.on, true); assert.equal(gio.t.parts, 178); assert.equal(gio.t.scans, 186); assert.equal(gio.t.orders, 37); assert.equal(gio.source, 'events');
  assert(gio.t.rate > 0 && gio.t.secPerScan > 0 && gio.stations[0].station === 'welding' && gio.stations[0].minutes > 300, 'rate, seconds per scan, station minutes: ' + JSON.stringify(gio.t));
  assert.equal(M.people.find(p => p.name === 'Ivy').on, false); assert(M.people.find(p => p.name === 'Ivy').lastOut > 0, 'a lock-out');
  assert.equal(M.biz.parts, M.people.reduce((n, p) => n + p.t.parts, 0)); assert.equal(M.biz.on, 3); assert(M.biz.hours.some(v => v > 0));
  assert.equal(M.biz.trend.length, 14); assert(M.feed.length > 0 && M.feed.length <= 40); assert.equal(M.sources.events, true);
  const per = (await answer({ op: 'person', name: 'Anna', days: 7 })).body, H = j(win.Efficiency.normHist(per));
  assert.equal(H.days.length, 7); assert(H.worked >= 6 && H.parts > 0, 'a week of Anna: ' + JSON.stringify([H.worked, H.parts]));
  const ord = (await answer({ op: 'orders', orderId: '3521000100' })).body, O = j(win.Efficiency.normOrder(ord));
  assert.equal(O.orderId, '3521000100'); assert(O.steps.length >= 1 && O.steps[0].station === 'assembly' && O.steps[0].person === 'Anna' && O.steps[0].firstAt > 0 && O.steps[0].source === 'events', 'an order\'s step: ' + JSON.stringify(O.steps[0]));
  assert.equal(O.steps[0].waitMs, 0, 'the first step waited for nothing');
  console.log('  ✓ the real function\'s answer is taken whole: people, totals, rates, hours, trend, feed, one person\'s days');

  /* ── and drawn ── */
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  Date.now = realNow;
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url());
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') return route.abort();
      if (u.pathname.endsWith('/employeeEfficiency')) { let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {} Date.now = () => NOW; const a = await answer(b); Date.now = realNow; return route.fulfill({ status: a.status, contentType: 'application/json', body: JSON.stringify(a.body) }); }
      return route.continue();
    });
    await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} window.confirm = () => true; window.alert = () => {}; });
    const page = await ctx.newPage(), errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.Efficiency && window.CN && document.readyState === 'complete', null, { timeout: 60000 });
    await page.evaluate(() => { Efficiency.options.growMs = 0; });
    await page.click('#moreMenu > summary'); await page.click('#moreMenu button[data-mode="efficiency"]');
    await page.fill('#efficiencyView .efKey input', PASS); await page.press('#efficiencyView .efKey input', 'Enter');
    await page.waitForSelector('#efficiencyView .efBody:not(.hidden) .efP');
    assert.equal(await page.locator('#efficiencyView .efP').count(), 4);
    assert.equal((await page.locator('#efficiencyView .efKpi[data-k="parts"] .efKV').innerText()).replace(/,/g, ''), String(M.biz.parts), 'the number on the screen is the function\'s');
    assert.equal(await page.locator('#efficiencyView .efKpi[data-k="on"] .efKV').innerText(), '3');
    await page.click('#efficiencyView .efP:nth-child(2) .efWho'); await page.waitForFunction(() => /days worked/.test((document.querySelector('#efficiencyView .efP:nth-child(2) .efSum') || {}).textContent || ''), null, { timeout: 8000 });
    await page.click('#efficiencyView .efFeedBtn'); await page.waitForSelector('#efficiencyView .efFl');
    await page.fill('#efficiencyView .efFind input', '3521000100'); await page.press('#efficiencyView .efFind input', 'Enter');
    await page.waitForSelector('#efficiencyView .efOView:not(.hidden) .efTr:not(.head)', { timeout: 8000 });
    assert(/Assembly/.test(await page.locator('#efficiencyView .efOView .efTr:not(.head)').first().innerText()), 'the order trace is drawn from the real answer');
    await page.waitForTimeout(500);
    if (process.env.SHOTS) await page.screenshot({ path: path.join(process.env.SHOTS, 'ef-real.png') });
    assert.deepEqual(errs, [], 'no page errors: ' + errs.join(' | '));
    console.log('  ✓ drawn from the real function\'s answer: numbers agree, a person\'s days load, the feed opens, an order is traced, no page errors');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
