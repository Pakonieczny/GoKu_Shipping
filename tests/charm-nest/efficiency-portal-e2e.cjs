// The whole Employee efficiency portal, end to end, in a real browser (Chromium via Playwright), as Paul would drive it (Paul, 5 Oct 2026, 16:54 UTC:
// "live logins ... every station with the current order ... a full page per employee ... all station apps interconnected live").
// The pieces are REAL: the sorter's console (charm-nest-efficiency.js) with its Stations board, employee page, order list and charts, the server
// functions (employeeEfficiency, firebaseOrders) and the station libraries (station-session.js, station-activity.js, station-live-order.js). Only the
// outside is fake: ONE in-memory Firestore filled with the invented shop of employee-profile-seed.cjs, a fake manager passcode, a fake clock
// (Mon 5 Oct 2026, 15:00 New York, then running), fake station pages that sign people in and send beats through the real door, and a
// loopback bridge for the sorter's other calls. Everything off the loopback is aborted. No Etsy call, no AI call, no real passcode, no PIN.
//   A  the sorter in Sandbox mode opens the console on REAL; the passcode gate (wrong, right); Signed in now fills from the real sign-ins
//   B  fake station pages sign in and send beats: Signed in now, Now working on and the Stations tab fill (QR decodes, one picture per piece,
//      time since scanned ticks), a completed order moves the numbers, an idle person leaves the board
//   C  a person: click a name, the full page, Day / Week / Month / 3 months / Year, charts hover, a calendar day, search an order by number,
//      date and customer, click an order, the numbers agree across the page, the order list, the stations board and the Overview
//   D  Back / Forward / reload on every route; Back then re-open at once is not undone
//   E  Real | Sandbox: the modules are destroyed and mounted again, nothing of the other store stays
//   F  a hidden tab reads nothing and catches up; a hung read says so and recovers; a 401 asks the passcode again
//   G  empty shop, a person with no data, a very long name
//   H  1440 / 900 / 390 px (rail open and folded): no sideways scroll, the header bar fits
//   node tests/charm-nest/efficiency-portal-e2e.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<folder for screenshots>, ONLY=A,B ...)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict'), { spawn } = require('child_process');
const root = path.join(__dirname, '../..');
const S = require('./employee-profile-seed.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const PASS = 'portal-fake-pass-4417';             // invented for this file: the real passcode is never used, read or printed
const SHOTS = process.env.SHOTS || '', ONLY = (process.env.ONLY || '').split(',').filter(Boolean);
const want = k => !ONLY.length || ONLY.includes(k);
const results = [];

/* ── the fake shop in its own process ── */
function backend(env) {
  return new Promise((resolve, reject) => {
    const ch = spawn(process.execPath, [path.join(__dirname, 'efficiency-portal-backend.cjs')], { env: Object.assign({}, process.env, { PORTAL_PASS: PASS }, env || {}) });
    let buf = '', err = ''; ch.stderr.on('data', d => { err += d; });
    const t = setTimeout(() => reject(new Error('the fake shop did not start: ' + err)), 90000);
    ch.stdout.on('data', d => { buf += d; const m = /PORT (\d+)/.exec(buf); if (m) { clearTimeout(t); const port = +m[1], base = `http://127.0.0.1:${port}`;
      const get = async p => (await fetch(base + p)).json();
      resolve({ port, base, child: ch, kill: () => { try { ch.kill(); } catch (_) {} },
        call: async (fn, body, q) => { const r = await fetch(`${base}/fn/${fn}${q || ''}`, { method: 'POST', body: JSON.stringify(body) }); return { status: r.status, json: await r.json().catch(() => null) }; },
        ask: async body => (await (async () => { const r = await fetch(`${base}/fn/employeeEfficiency`, { method: 'POST', body: JSON.stringify(Object.assign({ key: PASS }, body)) }); return r.json(); })()),
        now: async () => (await get('/ctl/now')).now, skew: ms => get('/ctl/skew?ms=' + ms), reads: reset => get('/ctl/reads' + (reset ? '?reset=1' : '')),
        list: c => get('/ctl/list?c=' + encodeURIComponent(c)), doc: (c, id) => get(`/ctl/doc?c=${encodeURIComponent(c)}&id=${encodeURIComponent(id)}`) }); } });
    ch.on('exit', c => { if (!buf) reject(new Error('the fake shop stopped: ' + err)); });
  });
}

(async () => {
  const pwDir = process.env.PW_DIR || (fs.existsSync(path.join(root, 'node_modules/playwright-core')) ? path.join(root, 'node_modules') : '/opt/node22/lib/node_modules/playwright/node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const B = await backend();
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const seen = { urls: [], aborted: 0, eff: [] };
  const ctl = { hook: null, B };                     // hook(body) -> null | { status, json } | 'hang' | 'abort'
  const V = '#efficiencyView', sorter = `${srv.sorterOrigin}/charm-nest-1.html`;
  const PNG = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/Z8gVq4AAAAASUVORK5CYII=', 'base64');
  const picSvg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 8 8"><rect width="8" height="8" fill="#c9b27a"/></svg>`;

  /** one browser context: the fake clock, the routes, the fake passcode kept per tab */
  async function context(opts) {
    opts = opts || {};
    const bk = opts.B || B, ctx = await browser.newContext(Object.assign({ viewport: { width: 1440, height: 900 } }, opts.ctx || {}));
    await ctx.clock.install({ time: await bk.now() });
    await ctx.route(() => true, async route => {
      const req = route.request(), u = new URL(req.url()); seen.urls.push(u.href);
      if (u.hostname === 'i.etsystatic.com' || u.hostname === 'thumbs.test') return route.fulfill({ status: 200, contentType: 'image/svg+xml', headers: { 'Access-Control-Allow-Origin': '*' }, body: picSvg });   // (a stored listing picture or vector design: answered here, never fetched)
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      const m = /^\/\.netlify\/functions\/(employeeEfficiency|firebaseOrders)$/.exec(u.pathname);
      if (!m) return route.continue();
      let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {}
      if (m[1] === 'employeeEfficiency') {
        seen.eff.push({ op: b.op, range: b.range, name: b.name, sandbox: !!b.sandbox, at: Date.now(), has: !!b.key, q: b.q, B: bk === B ? 'main' : 'other' });
        const h = ctl.hook && ctl.hook(b);
        if (h === 'hang') { await new Promise(r => { ctl.held = (ctl.held || []); ctl.held.push(r); }); try { return await route.abort(); } catch (_) { return; } }
        if (h === 'abort') return route.abort();
        if (h && h.status) { try { return await route.fulfill({ status: h.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(h.json || {}) }); } catch (_) { return; } }
        if (h && h.delay) await sleep(h.delay);
      }
      try { const r = await route.fetch({ url: `${bk.base}/fn/${m[1]}${u.search}` }); return await route.fulfill({ response: r }); } catch (_) { try { return await route.abort(); } catch (_2) {} }
    });
    await ctx.addInitScript(([k, sb]) => {
      try { localStorage.setItem('cn.employee', 'Tester'); if (sb) localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'on' })); } catch (_) {}
      try { if (k && !sessionStorage.getItem('__seeded')) { sessionStorage.setItem('__seeded', '1'); sessionStorage.setItem('cn.eff.key', k); } } catch (_) {}
      window.confirm = () => true; window.alert = () => {}; window.prompt = () => null;
    }, [opts.key === false ? '' : PASS, !!opts.sorterSandbox]);
    return ctx;
  }
  const errors = [];
  const track = (page, label) => { page.on('pageerror', e => errors.push(`${label || 'page'}: ${e.message}`)); page.on('console', m => { if (m.type() === 'error' && !/status of (401|403|429|503|404)|Failed to load resource/.test(m.text())) errors.push(`${label || 'page'} console: ${m.text()} @ ${(m.location() && m.location().url) || ''}`); }); };
  const shot = async (page, name) => { if (!SHOTS) return; try { fs.mkdirSync(SHOTS, { recursive: true }); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (_) {} };

  /** the sorter, then the console (as Paul: Workspace > Employee efficiency) */
  async function openSorter(ctx, o) {
    o = o || {};
    const page = await ctx.newPage(); track(page, 'sorter');
    await page.goto(o.url || sorter);
    await page.waitForFunction(() => window.Efficiency && window.EfficiencyStations && window.EfficiencyEmployee && window.EfficiencyOrders && window.EfficiencyCharts && window.CN && window.OrderWin && document.readyState === 'complete', null, { timeout: 90000 });
    if (!o.slow) await page.evaluate(() => { Object.assign(Efficiency.options, { pollMs: 2500, liveMs: 700, growMs: 0, nudgeMs: 1000 }); Object.assign(EfficiencyEmployee.options, { liveMs: 700, rangeMs: 1500, calMs: 1500, ordersMs: 1500, tickMs: 500 }); });
    if (o.vp) await page.setViewportSize(o.vp);
    return page;
  }
  const openConsole = async page => { await page.evaluate(() => { Efficiency.open(); }); await page.waitForSelector(`${V}:not(.hidden)`, { timeout: 15000 }); };
  const mod = (name, fn) => fn;
  async function section(key, title, fn) {
    if (!want(key)) return;
    try { await fn(); results.push([key + ' · ' + title, null]); console.log(`  ✓ ${key} · ${title}`); }
    catch (e) { results.push([key + ' · ' + title, e]); console.log(`  ✗ ${key} · ${title}\n    ${String(e && e.stack || e).split('\n').slice(0, 16).join('\n    ')}`); }
  }
  // helpers for reading the console
  const text = (page, sel) => page.locator(sel).first().innerText().then(s => s.replace(/\s+/g, ' ').trim());
  const nums = s => +String(s).replace(/[^\d.\-]/g, '');
  const kpi = (page, k) => page.locator(`${V} .efKpi[data-k="${k}"] .efKV`).innerText().then(s => s.replace(/,/g, '').trim());
  const siNames = page => page.locator(`${V} .efSiGrid .efSiNm`).allInnerTexts().then(a => a.map(s => s.trim()).sort());
  const liveText = page => page.locator(`${V} .efLiveT`).innerText();
  const hash = page => page.evaluate(() => location.hash);
  const waitFor = (page, fn, arg, ms) => page.waitForFunction(fn, arg, { timeout: ms || 20000 });
  const hidden = (page, on) => page.evaluate(v => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => (v ? 'hidden' : 'visible') }); Object.defineProperty(document, 'hidden', { configurable: true, get: () => !!v }); document.dispatchEvent(new Event('visibilitychange')); }, on);
  const jsQR = fs.readFileSync(path.join(root, 'lib/jsQR.js'), 'utf8');
  const decodeQr = (page, sel) => page.evaluate(async sel => {
    const im = document.querySelector(sel); if (!im) return null;
    await new Promise(r => { if (im.complete && im.naturalWidth) r(); else { im.addEventListener('load', r); setTimeout(r, 4000); } });
    const c = document.createElement('canvas'), pad = 24; c.width = im.naturalWidth + pad * 2; c.height = im.naturalHeight + pad * 2; const g = c.getContext('2d'); g.fillStyle = '#fff'; g.fillRect(0, 0, c.width, c.height); g.imageSmoothingEnabled = false; g.drawImage(im, pad, pad);
    const d = g.getImageData(0, 0, c.width, c.height), r = window.jsQR && window.jsQR(d.data, d.width, d.height); return r ? r.data : null;
  }, sel);

  const ctx0 = { page: null };
  try {
    /* ─────────────── A · the sorter in Sandbox mode, the console on Real, the passcode gate ─────────────── */
    await section('A', 'the console opens on Real though the sorter is in Sandbox mode; the passcode gate; the real crew is signed in', async () => {
      const ctx = await context({ sorterSandbox: true, key: false });
      const page = await openSorter(ctx);
      await page.addScriptTag({ content: jsQR });
      assert.equal(await page.evaluate(() => CN.S.settings.sandbox), 'on', 'the sorter itself is in Sandbox mode');
      // as Paul: Workspace menu > Employee efficiency
      await page.click('#moreMenu > summary'); await page.click('#moreMenu .moreList button[data-mode="efficiency"]');
      await page.waitForSelector(`${V} .efKey:not(.hidden)`, { timeout: 15000 });
      assert.equal(seen.eff.filter(c => c.B === 'main').length, 0, 'nothing is read before the passcode is typed');
      // a wrong passcode: said in plain words, the box stays
      await page.fill(`${V} .efKey input`, 'not-the-passcode'); await page.press(`${V} .efKey input`, 'Enter');
      await waitFor(page, sel => /passcode|Wrong|wrong|try again/i.test(document.querySelector(sel).textContent), `${V} .efKeyErr`);
      assert.equal(await page.locator(`${V} .efKey`).isVisible(), true, 'the box stays after a wrong passcode');
      await shot(page, 'a1-passcode-wrong');
      await page.fill(`${V} .efKey input`, PASS); await page.press(`${V} .efKey input`, 'Enter');
      await page.waitForSelector(`${V} .efTabBtn`, { timeout: 15000 }); await page.waitForSelector(`${V} .efBody:not(.hidden) .efP`, { timeout: 30000 });
      assert.equal(await page.locator(V).getAttribute('data-view'), 'real', 'Real, though the sorter is in Sandbox mode');
      assert(/Sorter is in Sandbox mode/.test(await page.locator(`${V} .efHint`).innerText()), 'a quiet hint');
      const mine = seen.eff.filter(c => c.B === 'main' && c.has); assert(mine.length && mine.every(c => c.sandbox === false), 'no request asked for the Sandbox copies');
      // the crew is on: the people the shop's own sign-ins say are in right now
      const ov = await B.ask({ op: 'overview', trend: false }), on = ov.people.filter(p => p.status === 'on').map(p => p.name).sort();
      assert(on.length >= 3, 'the invented shop has people signed in: ' + on.join(', '));
      await waitFor(page, n => document.querySelectorAll('#efficiencyView .efSiGrid .efSiNm').length >= n, on.length);
      assert.deepEqual(await siNames(page), on, 'Signed in now lists exactly the people the sign-ins say');
      assert.equal(await kpi(page, 'on'), String(on.length), 'People on now');
      assert.equal(await kpi(page, 'parts'), String(ov.business.totals.parts), 'Parts today = the server\'s own number'); assert.equal(await kpi(page, 'orders'), String(ov.business.totals.orders));
      await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent)); await shot(page, 'a2-overview-real-1440');
      assert.equal(await page.locator(`${V} .efFlag`).isVisible(), false, 'no Sandbox flag on Real');
      await ctx.close();
    });

    /* ─────────────── B · fake station pages sign in and send beats: Signed in now, Now working on and the Stations tab fill ─────────────── */
    let shared = null;      // the context of B..F: one sorter tab with the console open, and the station pages beside it
    const getShared = async () => {
      if (shared) return shared.ctx;
      const ctx = await context({}), page = await openSorter(ctx); await page.addScriptTag({ content: jsQR });
      await openConsole(page); await page.waitForSelector(`${V} .efBody:not(.hidden) .efP`, { timeout: 30000 });
      shared = { ctx, page, W: {} }; return ctx;
    };
    const TX = (rid, lines) => lines.map((l, i) => ({ transaction_id: Number(rid) * 10 + i, listing_id: l.listing, sku: l.sku, title: l.title, quantity: l.q, variations: l.size ? [{ formatted_name: 'Size', formatted_value: l.size }] : [] }));
    const R = { weld: '3529000101', asm: '3529000202', ship: '3529000303', design: '3529000404' };
    const LINES = {
      [R.weld]: [{ listing: 1912340001, sku: 'CH-MOON-GF', title: 'Moon charm, gold', q: 2, size: 'M' }, { listing: 1912340002, sku: 'ST-PEARL-GF', title: 'Pearl stud', q: 1 }],
      [R.asm]: [{ listing: 1912340003, sku: 'CH-HEART-RG', title: 'Heart charm, rose', q: 1 }],
      [R.ship]: [{ listing: 1912340004, sku: 'ST-BEE-SS', title: 'Bee stud', q: 2 }, { listing: 1912340005, sku: 'CH-LEAF-GF', title: 'Leaf charm', q: 2 }],
      [R.design]: [{ listing: 1912340006, sku: 'CH-STAR-SS', title: 'Star charm', q: 1 }]
    };
    const stationPage = async (ctx, name, who, sandbox) => {
      const p = await ctx.newPage(); track(p, name);
      await p.goto(`${B.base}/${name}.html${sandbox ? '?sandbox=1' : ''}`);
      await p.waitForFunction(() => window.StationSession && window.StationActivity && window.StationLiveOrder && window.__signIn, null, { timeout: 30000 });
      await p.evaluate(n => window.__signIn(n), who); return p;
    };
    const scan = (p, rid, note) => p.evaluate(([rid, tx, note]) => { const n = tx.reduce((a, t) => a + t.quantity, 0); StationLiveOrder.start(rid, tx, { note }); StationActivity.log('scan', { orderId: rid, parts: n, detail: note || 'typed' }); return StationActivity.flush(); }, [rid, TX(rid, LINES[rid]), note || '']);
    const finish = (p, rid, parts) => p.evaluate(([rid, parts]) => { StationActivity.log('complete', { orderId: rid, parts, orders: 1, detail: '' }); StationLiveOrder.end(rid); return StationActivity.flush(); }, [rid, parts]);
    await section('B', 'station pages sign in and send beats: Signed in now, Now working on, the Stations tab, a finished order moves the numbers', async () => {
      const ctx = await getShared(), page = shared.page;
      assert.equal(await page.locator(`${V} .efWkList .esCard`).count(), 0, 'nobody has an order in hand yet'); assert(/No one is working on an order/.test(await text(page, `${V} .efWkList`)));
      // four station pages: welding, assembly, shipping, design (a new starter, Tess), each scans an order
      const W = shared.W;
      W.weld = await stationPage(ctx, 'weld-1', 'Giovanna C.'); W.asm = await stationPage(ctx, 'assembly-2', 'Ana M.'); W.ship = await stationPage(ctx, 'shipping-1', 'Michael V.'); W.design = await stationPage(ctx, 'design-message', 'Tess Welder');
      await scan(W.weld, R.weld, 'phone scan'); await scan(W.asm, R.asm); await scan(W.ship, R.ship); await scan(W.design, R.design);
      // the beats reached the real door: one Station_Live document each, the person's NAME, never a PIN
      await waitFor(page, () => document.querySelectorAll('#efficiencyView .efWkList .esCard').length >= 4, null, 30000);
      const live = await B.list('Station_Live'); assert.equal(live.length, 4, 'one live document per station page');
      assert(live.every(d => d.state === 'working' && !/\b\d{6}\b/.test(JSON.stringify(d).replace(/\b\d{10}\b/g, ''))), 'working, and no PIN-like number in a stored beat');
      assert(await siNames(page).then(n => n.includes('Tess Welder')), 'a brand new person appears in Signed in now: ' + (await siNames(page)).join(', '));
      const cards = await page.$$eval(`${V} .efWkList .esCard`, cs => cs.map(c => ({ rid: c.dataset.rid, who: c.querySelector('.esWho').innerText.replace(/\s+/g, ' '), pcs: c.querySelectorAll('.esPcTh').length, qr: !!c.querySelector('.esQr img'), since: (c.querySelector('[data-since]') || {}).textContent || '' })));
      const byRid = Object.fromEntries(cards.map(c => [c.rid, c]));
      assert.deepEqual(Object.keys(byRid).sort(), Object.values(R).sort(), 'one card per person who has an order in hand');
      assert(/Giovanna/.test(byRid[R.weld].who) && /Welding/.test(byRid[R.weld].who), 'the card names the person and the station: ' + byRid[R.weld].who);
      assert(/Tess Welder/.test(byRid[R.design].who) && /Design/.test(byRid[R.design].who), byRid[R.design].who);
      assert.deepEqual([byRid[R.weld].pcs, byRid[R.asm].pcs, byRid[R.ship].pcs, byRid[R.design].pcs], [3, 0, 4, 0], 'one picture per piece of a multi-piece order (3 and 4 pieces); a one-piece order shows the order picture only');
      for (const rid of Object.values(R)) assert.equal(await decodeQr(page, `${V} .efWkList .esCard[data-rid="${rid}"] .esQr img`), rid, 'the QR decodes to the order ' + rid);
      // the pictures are really there: the vector design the app serves per SKU and the stored listing photo
      const okImgs = await page.$$eval(`${V} .efWkList .esCard[data-rid="${R.weld}"] img`, is => is.filter(i => !i.closest('.esQr')).map(i => [i.complete, i.naturalWidth]));
      assert(okImgs.length >= 1 && okImgs.every(([c, w]) => c && w > 0), 'order and piece pictures loaded: ' + JSON.stringify(okImgs));
      // "time since scanned" ticks
      if (process.env.DEBUGB) { for (let i = 0; i < 4; i++) { console.log('DEBUG', JSON.stringify(await page.evaluate(rid => { const e = document.querySelector(`#efficiencyView .efWkList .esCard[data-rid="${rid}"] .esT`); return { t: e.textContent, title: e.title, vis: document.visibilityState, st: Efficiency.api.state(), now: Date.now(), dt: new Date().toISOString(), conn: e.isConnected }; }, R.weld))); await sleep(1500); } }
      const s0 = await page.$eval(`${V} .efWkList .esCard[data-rid="${R.weld}"] .esT`, e => e.textContent);
      await waitFor(page, ([rid, t0]) => document.querySelector(`#efficiencyView .efWkList .esCard[data-rid="${rid}"] .esT`).textContent !== t0, [R.weld, s0], 10000).catch(() => { throw new Error(`the time since scanned did not tick for 10 s (stuck at ${s0})`); });
      await shot(page, 'b1-overview-now-working-1440');
      // the Stations tab: every station with its current order
      await page.click(`${V} .efTabBtn[data-tab="stations"]`); await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 });
      assert.equal(await hash(page), '#efficiency/stations');
      const rows = await page.$$eval(`${V} .esSt`, rs => Object.fromEntries(rs.map(r => [r.dataset.key, { state: r.dataset.state, cards: [...r.querySelectorAll('.esCard')].map(c => c.dataset.rid) }])));
      assert.deepEqual(rows.welding, { state: 'working', cards: [R.weld] }); assert.deepEqual(rows.assembly.cards, [R.asm]); assert.deepEqual(rows.shipping.cards, [R.ship]); assert.deepEqual(rows.design.cards, [R.design]);
      assert(['sorting', 'laser', 'sorter', 'qr', 'inbox'].every(k => rows[k] && rows[k].cards.length === 0), 'every other station is listed, with no order in hand: ' + JSON.stringify(Object.keys(rows)));
      // the board's own cards: every picture settles (none stays white), the QR decodes to the order, one picture per piece
      await waitFor(page, () => { const b = document.querySelectorAll('#efficiencyView .es .esCard [data-state="wait"]'); return !b.length && document.querySelectorAll('#efficiencyView .es .esCard .esQr img').length >= 4; }, null, 20000);
      for (const rid of Object.values(R)) assert.equal(await decodeQr(page, `${V} .es .esCard[data-rid="${rid}"] .esQr img`), rid, 'on the board too, the QR decodes to the order ' + rid);
      assert.equal(await page.$$eval(`${V} .es .esSt[data-key="shipping"] .esPcTh img`, is => is.filter(i => i.complete && i.naturalWidth > 0).length), 4, 'the 4-piece order: four pictures, all loaded');
      assert.equal(await page.$$eval(`${V} .es .esSt[data-key="welding"] .esPcTh`, is => is.length), 3, 'the 3-piece order: three tiles');
      await shot(page, 'b2-stations-board-1440');
      // a finished order moves the numbers (Overview KPI = the server's own, after the console's catch-up read)
      const before = (await B.ask({ op: 'overview', trend: false })).business.totals;
      await finish(W.weld, R.weld, 3);
      await waitFor(page, rid => !document.querySelector(`#efficiencyView .es .esCard[data-rid="${rid}"]:not([data-leaving])`), R.weld, 30000);
      await page.click(`${V} .efTabBtn[data-tab="overview"]`);
      await waitFor(page, p => +document.querySelector('#efficiencyView .efKpi[data-k="parts"] .efKV').textContent.replace(/,/g, '') === p, before.parts + 3, 40000);
      const now = (await B.ask({ op: 'overview', trend: false })).business.totals;
      assert.equal(now.parts, before.parts + 3, 'three pieces were finished'); assert.equal(await kpi(page, 'parts'), String(now.parts)); assert.equal(await kpi(page, 'orders'), String(now.orders), 'orders agree with the server');
      // the person at the Welding page signs out: she leaves Signed in now
      await W.weld.evaluate(() => window.__signOut()); await sleep(500);
      await waitFor(page, () => ![...document.querySelectorAll('#efficiencyView .efSiGrid .efSiNm')].some(e => /Giovanna/.test(e.textContent) && /Welding/.test(e.closest('.efSi').textContent)), null, 30000);
      await shot(page, 'b3-after-finish-and-signout-1440');
    });

    /* ─────────────── C · a person: the full page, ranges, hover, a calendar day, order search, an order opens, numbers agree ─────────────── */
    const P = `${V} .efp`;
    const pstate = page => page.evaluate(() => { const h = EfficiencyEmployee.instances[0]; return h ? h.state : null; });
    const ploaded = async (page, range, anchor) => {
      try { await page.waitForFunction(([r, a]) => { const h = EfficiencyEmployee.instances[0], s = h && h.state; return !!s && s.loaded && s.range === r && (!a || s.anchor === a) && !document.querySelector('#efficiencyView .efpBusy.on'); }, [range, anchor || ''], { timeout: 40000 }); }
      catch (e) { throw new Error(`the person page did not settle on ${range} ${anchor || ''}: ` + JSON.stringify(await page.evaluate(() => ({ s: EfficiencyEmployee.instances.map(h => h.state), hash: location.hash })))); }
    };
    const pk = (page, k) => page.locator(`${P} .efpK[data-k="${k}"] .efpKV`).innerText().then(s => s.replace(/,/g, '').trim());
    const orderRows = page => page.$$eval(`${P} .efoRow`, rs => rs.map(r => ({ rid: r.dataset.rid, text: r.innerText.replace(/\s+/g, ' ') })));
    const pAsk = (name, range, day, extra) => B.ask(Object.assign({ op: 'person', name, range, compare: true }, day ? { day } : {}, extra || {}));
    const allOrders = async (name, from, to, q) => { const out = []; let cursor = ''; for (let i = 0; i < 80; i++) { const r = await B.ask({ op: 'personOrders', name, from, to, q: q || '', limit: 100, cursor }); out.push(...r.orders); if (!r.next) return { orders: out, total: r.total, scanned: r.scanned }; cursor = r.next; } throw new Error('too many pages'); };
    await section('C', 'a person: click a name, the full page, every range, hover, a calendar day, search an order, open it, the numbers agree', async () => {
      await getShared(); const page = shared.page;
      await page.click(`${V} .efTabBtn[data-tab="people"]`);
      await page.waitForSelector(`${V} .efRoster .efRc[data-name="Ana M."]`, { timeout: 30000 });
      const ov = await B.ask({ op: 'overview', trend: false });
      assert.deepEqual((await page.$$eval(`${V} .efRoster .efRc`, cs => cs.map(c => c.dataset.name))).sort(), ov.people.map(p => p.name).sort(), 'the People tab lists everyone who signed in today');
      await shot(page, 'c1-people-1440');
      await page.click(`${V} .efRoster .efRc[data-name="Ana M."]`);
      await page.waitForSelector(P, { timeout: 30000 }); await ploaded(page, 'week');
      assert.equal(await hash(page), '#efficiency/person/Ana%20M.'); assert.equal((await page.locator(`${P} .efpName`).innerText()).trim(), 'Ana M.');
      await waitFor(page, sel => /Assembly/.test(document.querySelector(sel).textContent), `${P} .efpWhere`);
      await shot(page, 'c2-person-week-1440');
      // every range: from / to and the pieces figure are the server's own for that window
      const RANGE = { day: 'Day', week: 'Week', month: 'Month', quarter: '3 months', year: 'Year' };
      for (const [key, label] of Object.entries(RANGE)) {
        await page.click(`${P} .efpSeg button[data-range="${key}"]`); await ploaded(page, key);
        const T = await pAsk('Ana M.', key), st = await pstate(page);
        assert.equal(await page.locator(`${P} .efpSeg button.on`).innerText(), label); assert.equal(st.from, T.from, key + ' from'); assert.equal(st.to, T.to, key + ' to');
        await waitFor(page, ([k, v]) => document.querySelector(`#efficiencyView .efp .efpK[data-k="${k}"] .efpKV`).textContent.replace(/,/g, '').trim() === String(v), ['kpis.parts', T.kpis.parts.value], 20000)
          .catch(async () => { throw new Error(`${key}: pieces ${await pk(page, 'kpis.parts')} on screen, ${T.kpis.parts.value} from the server`); });
        await waitFor(page, ([k, v]) => document.querySelector(`#efficiencyView .efp .efpK[data-k="${k}"] .efpKV`).textContent.replace(/,/g, '').trim() === String(v), ['kpis.orders', T.kpis.orders.value], 20000)   // (the figures count up: wait for the last step)
          .catch(async () => { throw new Error(`${key}: orders ${await pk(page, 'kpis.orders')} on screen, ${T.kpis.orders.value} from the server`); });
        if (key === 'month') await shot(page, 'c3-person-month-1440');
      }
      // hover on the throughput chart (month): a read-out names the day and the figure
      await page.click(`${P} .efpSeg button[data-range="month"]`); await ploaded(page, 'month'); await sleep(600);
      const svg = page.locator(`${P} [data-c="tp"] svg.efc-svg`); await svg.evaluate(e => e.scrollIntoView({ block: 'center' })); await sleep(120);
      const bb = await svg.boundingBox(); await page.mouse.move(bb.x + bb.width * 0.7, bb.y + bb.height * 0.5);
      await page.waitForSelector(`${P} [data-c="tp"] .efc-tip[data-open]`, { timeout: 8000 });
      const tip = (await page.locator(`${P} [data-c="tp"] .efc-tip[data-open]`).innerText()).replace(/\s+/g, ' '); assert(/(piece|Nothing logged|Closed)/i.test(tip) && /[A-Z][a-z]{2}/.test(tip), 'the read-out names the day and the figure: ' + tip);
      await shot(page, 'c4-person-hover-1440'); await page.mouse.move(2, 2);
      // the calendar: a worked day opens that day
      const cal = page.locator(`${P} [data-c="cal"] .efc-day.worked:not(.today)`).first(); await cal.evaluate(e => e.scrollIntoView({ block: 'center' })); await sleep(120);
      const day = await cal.getAttribute('data-day'), cb = await cal.boundingBox();
      if (!cb) throw new Error('the calendar day ' + day + ' has no box: ' + JSON.stringify(await page.evaluate(() => [...document.querySelectorAll('#efficiencyView .efp [data-c="cal"] .efc-day')].map(g => [g.getAttribute('data-day'), g.getAttribute('data-state'), g.getAttribute('class'), g.getBoundingClientRect().width]).slice(0, 40)))); await page.mouse.move(cb.x + cb.width / 2, cb.y + cb.height / 2);
      await page.waitForSelector(`${P} [data-c="cal"] .efc-tip[data-open]`, { timeout: 8000 });
      assert(/Signed in/.test(await page.locator(`${P} [data-c="cal"] .efc-tip[data-open]`).innerText()), 'a calendar day says how long the person was signed in');
      await page.mouse.click(cb.x + cb.width / 2, cb.y + cb.height / 2); await ploaded(page, 'day', day);
      const D = await pAsk('Ana M.', 'day', day); assert.equal((await pstate(page)).from, day);
      await waitFor(page, ([k, v]) => document.querySelector(`#efficiencyView .efp .efpK[data-k="${k}"] .efpKV`).textContent.replace(/,/g, '').trim() === String(v), ['kpis.parts', D.kpis.parts.value], 20000);
      await page.click(`${P} .efpSeg button[data-range="month"]`); await ploaded(page, 'month');
      // search the orders in real time: by number (the last digits), by date, by customer; every count is the server's own
      const ST = await pstate(page), mine = await allOrders('Ana M.', ST.from, ST.to);
      assert(mine.orders.length >= 20, 'Ana has orders in the month: ' + mine.orders.length);
      await page.locator(`${P} .efoSearch input`).evaluate(e => e.scrollIntoView({ block: 'center' })); await page.waitForSelector(`${P} .efoRow`, { timeout: 30000 });
      await waitFor(page, n => { const c = document.querySelector('#efficiencyView .efp .efoCount'); return !!c && /^\d+ orders?$/.test(c.textContent.trim()) && +c.textContent.trim().split(' ')[0] === n; }, mine.total, 20000)
        .catch(async () => { throw new Error(`the order list says "${await text(page, `${P} .efoCount`)}" for the month ${ST.from}..${ST.to}; the server counts ${mine.total}`); });
      const withCust = mine.orders.find(o => o.customer), pick = mine.orders[7];
      const searchFor = async q => { await page.locator(`${P} .efoSearch input`).fill(q); await sleep(200); await waitFor(page, () => !document.querySelector('#efficiencyView .efp .efoWait:not([hidden])') && !(document.querySelector('#efficiencyView .efp .efoLive') || {}).dataset?.busy, null, 20000).catch(() => {}); };
      const settleList = async total => waitFor(page, n => { const c = document.querySelector('#efficiencyView .efp .efoCount'); return !!c && new RegExp('^(' + n + ' of \\d+|' + n + ') orders?$').test(c.textContent.trim()); }, total, 25000);
      const last4 = pick.number.slice(-4); let srv = await allOrders('Ana M.', ST.from, ST.to, last4);
      await searchFor(last4); await settleList(srv.total);
      let rows = await orderRows(page); assert(rows.length === Math.min(srv.total, rows.length) && rows.some(r => r.rid === pick.rid), `searching "${last4}" finds the order ${pick.rid}: ${rows.length} rows`);
      assert(rows.every(r => r.rid.includes(last4)), 'every row carries those digits');
      const dd = new Date(pick.at); const dayWord = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric' }).format(dd).toLowerCase();   // "oct 1"
      srv = await allOrders('Ana M.', ST.from, ST.to, dayWord); await searchFor(dayWord); await settleList(srv.total);
      rows = await orderRows(page); assert(rows.length > 0 && rows.length <= srv.total && rows.every(r => r.text.toLowerCase().includes(dayWord.replace(/^(\w+) 0?/, (m, a) => a + ' '))), `searching the date "${dayWord}": ${rows.length} of ${srv.total} rows, all on that day`);
      if (withCust) {
        srv = await allOrders('Ana M.', ST.from, ST.to, withCust.customer); await searchFor(withCust.customer); await settleList(srv.total);
        rows = await orderRows(page); assert(rows.length > 0 && rows.every(r => r.text.includes(withCust.customer)), `searching the customer "${withCust.customer}": ${rows.length} rows, all theirs`);
      }
      await searchFor('zzzqq'); await waitFor(page, () => /No orders match/.test(document.querySelector('#efficiencyView .efp').textContent), null, 15000);
      await page.locator(`${P} .efoClear`).first().click().catch(() => {}); await searchFor(''); await settleList(mine.total);
      // an order opens the sorter's order window (zooming from the row)
      rows = await orderRows(page); const target = rows[1].rid;
      await page.locator(`${P} .efoRow[data-rid="${target}"] .efoOpen`).click();
      await page.waitForSelector('#orderWin[open]', { timeout: 15000 });
      assert(new RegExp(target).test(await page.locator('#owTitle').innerText().catch(() => '') + await page.locator('#owSub').innerText().catch(() => '') + await page.locator('#orderWin').innerText()), 'the order window is for order ' + target);
      await shot(page, 'c5-order-window-1440');
      await page.keyboard.press('Escape'); await waitFor(page, () => !document.querySelector('#orderWin[open]'), null, 10000);
      assert.equal(await hash(page), '#efficiency/person/Ana%20M.', 'closing the order leaves the person page where it was');
    });
  } finally {
    await browser.close(); srv.close && srv.close(); B.kill();
  }
  const bad = results.filter(r => r[1]);
  if (errors.length) console.log('  page errors:\n    ' + [...new Set(errors)].slice(0, 12).join('\n    '));
  console.log(`\n${results.length - bad.length}/${results.length} sections passed`);
  if (bad.length || errors.length) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
