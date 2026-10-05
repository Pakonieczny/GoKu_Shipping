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
//   I  one number everywhere: Overview, People cards, Stations board, person page (Day) and its order list agree
//   K  scrolled down a person page: the console header and the page's range bar stack
//   L  the Stations board by hand: hover a station and a person, press a name, press an order, Back
//   J  what an open console costs per hour with the shipped timings; a hidden page costs nothing
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
    await ctx.addInitScript(([k, sb, who]) => {
      try { if (who) localStorage.setItem('cn.employee', who); if (sb) localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'on' })); } catch (_) {}
      try { if (k && !sessionStorage.getItem('__seeded')) { sessionStorage.setItem('__seeded', '1'); sessionStorage.setItem('cn.eff.key', k); } } catch (_) {}
      window.confirm = () => true; window.alert = () => {}; window.prompt = () => null;
    }, [opts.key === false ? '' : PASS, !!opts.sorterSandbox, opts.employee === null ? '' : (opts.employee || 'Tester')]);
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
    /** the shared tab, put back as a section expects it (a section that failed half way must not poison the next): Real view, nothing held, page shown, no order window, 1440 wide with the rail open */
    const fresh = async () => {
      await getShared(); const page = shared.page; ctl.hook = null; for (const r of (ctl.held || []).splice(0)) r();
      await page.bringToFront(); await page.evaluate(() => { try { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'visible' }); Object.defineProperty(document, 'hidden', { configurable: true, get: () => false }); document.dispatchEvent(new Event('visibilitychange')); } catch (_) {} Object.assign(Efficiency.options, { timeoutMs: 9000, staleMs: 13000 }); if (document.querySelector('#orderWin[open]')) document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); });
      if ((await page.viewportSize()).width !== 1440) await page.setViewportSize({ width: 1440, height: 900 });
      await page.evaluate(() => { const app = document.getElementById('app'); if (app.classList.contains('railOff')) document.getElementById('btnRail').click(); });
      if ((await page.locator(V).getAttribute('data-view')) !== 'real') { await page.click(`${V} .efView button[data-view="real"]`); await waitFor(page, () => document.getElementById('efficiencyView').getAttribute('data-view') === 'real', null, 20000); }
      if (await page.locator(`${V} .efKey`).isVisible()) { await page.fill(`${V} .efKey input`, PASS); await page.press(`${V} .efKey input`, 'Enter'); await page.waitForSelector(`${V} .efTabBtn`, { timeout: 20000 }); }
      return page;
    };
    const TX = (rid, lines) => lines.map((l, i) => ({ transaction_id: Number(rid) * 10 + i, listing_id: l.listing, sku: l.sku, title: l.title, quantity: l.q, variations: l.size ? [{ formatted_name: 'Size', formatted_value: l.size }] : [] }));
    const R = { weld: '3529000101', asm: '3529000202', ship: '3529000303', design: '3529000404' }, R_L = '3529000505', R_G = '3529000606', R_I = '3529000707';
    const LINES = {
      [R.weld]: [{ listing: 1912340001, sku: 'CH-MOON-GF', title: 'Moon charm, gold', q: 2, size: 'M' }, { listing: 1912340002, sku: 'ST-PEARL-GF', title: 'Pearl stud', q: 1 }],
      [R.asm]: [{ listing: 1912340003, sku: 'CH-HEART-RG', title: 'Heart charm, rose', q: 1 }],
      [R.ship]: [{ listing: 1912340004, sku: 'ST-BEE-SS', title: 'Bee stud', q: 2 }, { listing: 1912340005, sku: 'CH-LEAF-GF', title: 'Leaf charm', q: 2 }],
      [R.design]: [{ listing: 1912340006, sku: 'CH-STAR-SS', title: 'Star charm', q: 1 }],
      [R_I]: [{ listing: 1912340005, sku: 'CH-LEAF-GF', title: 'Leaf charm', q: 1 }],
      [R_G]: [{ listing: 1912340003, sku: 'CH-HEART-RG', title: 'Heart charm, rose', q: 1 }],
      [R_L]: [{ listing: 1912340001, sku: 'CH-MOON-GF', title: 'Moon charm, gold', q: 2, size: 'M' }, { listing: 1912340004, sku: 'ST-BEE-SS', title: 'Bee stud', q: 1 }]
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
      try { await page.waitForFunction(([r, a]) => { const h = EfficiencyEmployee.instances[0], s = h && h.state; return !!s && s.loaded && (!r || s.range === r) && (!a || s.anchor === a) && !document.querySelector('#efficiencyView .efpBusy.on'); }, [range, anchor || ''], { timeout: 40000 }); }
      catch (e) { throw new Error(`the person page did not settle on ${range} ${anchor || ''}: ` + JSON.stringify(await page.evaluate(() => ({ s: EfficiencyEmployee.instances.map(h => h.state), hash: location.hash })))); }
    };
    /** the page keeps the range last chosen (also across people), so say which one a check needs */
    const ensureRange = async (page, r) => {
      await ploaded(page); if ((await pstate(page)).range !== r) await page.click(`${P} .efpSeg button[data-range="${r}"]`); await ploaded(page, r);
      const td = (await B.ask({ op: 'overview', trend: false })).day, st = await pstate(page);       // (and the window that ends today, not a day an earlier check had opened)
      if (st.anchor && st.anchor < td) { await page.click(`${P} .efpToday`); await ploaded(page, r); }
    };
    const pk = (page, k) => page.locator(`${P} .efpK[data-k="${k}"] .efpKV`).innerText().then(s => s.replace(/,/g, '').trim());
    const orderRows = page => page.$$eval(`${P} .efoRow`, rs => rs.map(r => ({ rid: r.dataset.rid, text: r.innerText.replace(/\s+/g, ' ') })));
    const pAsk = (name, range, day, extra) => B.ask(Object.assign({ op: 'person', name, range, compare: true }, day ? { day } : {}, extra || {}));
    const allOrders = async (name, from, to, q) => { const out = []; let cursor = ''; for (let i = 0; i < 80; i++) { const r = await B.ask({ op: 'personOrders', name, from, to, q: q || '', limit: 100, cursor }); out.push(...r.orders); if (!r.next) return { orders: out, total: r.total, scanned: r.scanned }; cursor = r.next; } throw new Error('too many pages'); };
    await section('C', 'a person: click a name, the full page, every range, hover, a calendar day, search an order, open it, the numbers agree', async () => {
      const page = await fresh();
      await page.click(`${V} .efTabBtn[data-tab="people"]`);
      await page.waitForSelector(`${V} .efRoster .efRc[data-name="Ana M."]`, { timeout: 30000 });
      const ov = await B.ask({ op: 'overview', trend: false });
      assert.deepEqual((await page.$$eval(`${V} .efRoster .efRc`, cs => cs.map(c => c.dataset.name))).sort(), ov.people.map(p => p.name).sort(), 'the People tab lists everyone who signed in today');
      await shot(page, 'c1-people-1440');
      await page.click(`${V} .efRoster .efRc[data-name="Ana M."]`);
      await page.waitForSelector(P, { timeout: 30000 }); await ensureRange(page, 'week');
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
      // hover on a figure: its card says what it is, in the server's own words, and whether it is estimated
      await page.click(`${P} .efpSeg button[data-range="month"]`); await ploaded(page, 'month'); await sleep(600);
      const TM = await pAsk('Ana M.', 'month');
      await page.locator(`${P} .efpK[data-k="kpis.parts"]`).hover(); await page.waitForSelector(`${P} .efpHC.on`, { timeout: 8000 });
      assert((await page.locator(`${P} .efpHC`).innerText()).replace(/\s+/g, ' ').includes(TM.kpis.parts.def.slice(0, 40)), 'the hover card carries the server\'s definition of pieces');
      await page.locator(`${P} .efpK[data-k="kpis.secPerOrderMedian"]`).hover();
      assert(/Estimated/i.test((await page.locator(`${P} .efpHC`).innerText())), 'an estimated figure says so');
      await page.locator(`${P} .efpName`).hover();
      // the charts really drew the month (bars with a height, not an empty frame)
      await waitFor(page, () => { const b = [...document.querySelectorAll('#efficiencyView .efp [data-c="tp"] .efc-bar')]; return b.length >= 15 && b.some(x => x.getBoundingClientRect().height > 3); }, null, 15000)
        .catch(async () => { throw new Error('the throughput chart drew no bars: ' + await page.evaluate(() => [...document.querySelectorAll('#efficiencyView .efp [data-c="tp"] .efc-bar')].length)); });
      // hover on the throughput chart (month): a read-out names the day and the figure
      const svg = page.locator(`${P} [data-c="tp"] svg.efc-svg`); await svg.evaluate(e => e.scrollIntoView({ block: 'center' })); await sleep(120);
      const bb = await svg.boundingBox(); await page.mouse.move(bb.x + bb.width * 0.7, bb.y + bb.height * 0.5);
      await page.waitForSelector(`${P} [data-c="tp"] .efc-tip[data-open]`, { timeout: 8000 });
      const tip = (await page.locator(`${P} [data-c="tp"] .efc-tip[data-open]`).innerText()).replace(/\s+/g, ' '); assert(/(piece|Nothing logged|Closed)/i.test(tip) && /[A-Z][a-z]{2}/.test(tip), 'the read-out names the day and the figure: ' + tip);
      await shot(page, 'c4-person-hover-1440'); await page.mouse.move(2, 2);
      // the calendar: a worked day opens that day
      const cal = page.locator(`${P} [data-c="cal"] .efc-day.worked:not(.today)`).first(); await cal.evaluate(e => e.scrollIntoView({ block: 'center' })); await sleep(120);
      const day = await cal.getAttribute('data-day'); let cb = await cal.boundingBox();
      for (let i = 0; i < 12 && !cb; i++) { await sleep(300); cb = await page.locator(`${P} [data-c="cal"] .efc-day[data-day="${day}"]`).first().boundingBox(); }   // (the calendar draws again on each poll: a node can be gone for a moment)
      if (!cb) throw new Error('the calendar day ' + day + ' has no box: ' + JSON.stringify(await page.evaluate(d => { const g = document.querySelector(`#efficiencyView .efp [data-c="cal"] .efc-day[data-day="${d}"]`); if (!g) return 'gone'; const r = g.getBoundingClientRect(), c = getComputedStyle(g), sv = g.closest('svg'); return { tag: g.tagName, cls: g.getAttribute('class'), rect: [r.left, r.top, r.width, r.height], display: c.display, vis: c.visibility, op: c.opacity, svg: sv && sv.getBoundingClientRect().toJSON(), kids: g.children.length, html: g.outerHTML.slice(0, 300) }; }, day))); await page.mouse.move(cb.x + cb.width / 2, cb.y + cb.height / 2);
      await page.waitForSelector(`${P} [data-c="cal"] .efc-tip[data-open]`, { timeout: 8000 }).catch(async e => { await shot(page, 'zz-cal-hover-failed'); throw new Error('no card on the calendar day ' + day + ' at ' + JSON.stringify(cb) + ': ' + JSON.stringify(await page.evaluate(([x, y]) => { const t = document.elementFromPoint(x, y); return { at: t && (t.tagName + '.' + (t.getAttribute('class') || '')), scrollY: [...document.querySelectorAll('*')].filter(e => e.scrollTop > 0).map(e => e.tagName + '#' + e.id + ':' + e.scrollTop).slice(0, 3), tips: document.querySelectorAll('.efc-tip').length }; }, [cb.x + cb.width / 2, cb.y + cb.height / 2]))); });
      assert(/Signed in/.test(await page.locator(`${P} [data-c="cal"] .efc-tip[data-open]`).innerText()), 'a calendar day says how long the person was signed in');
      await page.mouse.click(cb.x + cb.width / 2, cb.y + cb.height / 2); await ploaded(page, 'day', day);
      const D = await pAsk('Ana M.', 'day', day); assert.equal((await pstate(page)).from, day);
      await waitFor(page, ([k, v]) => document.querySelector(`#efficiencyView .efp .efpK[data-k="${k}"] .efpKV`).textContent.replace(/,/g, '').trim() === String(v), ['kpis.parts', D.kpis.parts.value], 20000);
      await page.click(`${P} .efpSeg button[data-range="month"]`); await ploaded(page, 'month');
      // search the orders in real time: by number (the last digits), by date, by customer; every count is the server's own
      const ST = await pstate(page), mine = await allOrders('Ana M.', ST.from, ST.to);
      assert(mine.orders.length >= 20, 'Ana has orders in the month: ' + mine.orders.length);
      await page.locator(`${P} .efoSearch input`).evaluate(e => e.scrollIntoView({ block: 'center' })); await page.waitForSelector(`${P} .efoRow`, { timeout: 30000 });
      await waitFor(page, n => { const c = document.querySelector('#efficiencyView .efp .efoCount'); return !!c && /^[\d,]+( of [\d,]+)? orders?$/.test(c.textContent.trim()) && +c.textContent.trim().split(' ')[0].replace(/,/g, '') === n; }, mine.total, 20000)
        .catch(async () => { throw new Error(`the order list says "${await text(page, `${P} .efoCount`)}" for the month ${ST.from}..${ST.to}; the server counts ${mine.total}`); });
      const withCust = mine.orders.find(o => o.customer), pick = mine.orders[7];
      const searchFor = async q => { await page.locator(`${P} .efoSearch input`).fill(q); await sleep(200); await waitFor(page, () => !document.querySelector('#efficiencyView .efp .efoWait:not([hidden])') && !(document.querySelector('#efficiencyView .efp .efoLive') || {}).dataset?.busy, null, 20000).catch(() => {}); };
      const settleList = async total => waitFor(page, n => { const c = document.querySelector('#efficiencyView .efp .efoCount'); return !!c && /^[\d,]+( of [\d,]+)? orders?$/.test(c.textContent.trim()) && +c.textContent.trim().split(' ')[0].replace(/,/g, '') === n; }, total, 25000);
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

    /* ─────────────── D · Back / Forward / reload on every route; Back then re-open at once is not undone ─────────────── */
    const where = page => page.evaluate(() => ({ hash: location.hash, tab: Efficiency.state.tab, person: Efficiency.state.person, mounted: Efficiency.state.mounted, view: [...document.querySelectorAll('#efficiencyView .efPage')].filter(e => !e.classList.contains('hidden')).map(e => e.id), efp: document.querySelectorAll('#efficiencyView .efp').length, es: document.querySelectorAll('#efficiencyView .es').length }));
    const settled = (page, hashWant, viewId) => waitFor(page, ([h, v]) => location.hash === h && [...document.querySelectorAll('#efficiencyView .efPage')].some(e => e.id === v && !e.classList.contains('hidden')), [hashWant, viewId], 30000)
      .catch(async () => { throw new Error(`expected ${hashWant} on ${viewId}: ` + JSON.stringify(await where(page))); });
    const reloadSorter = async page => {
      await page.reload();
      await page.waitForFunction(() => window.Efficiency && window.EfficiencyStations && window.EfficiencyEmployee && window.EfficiencyOrders && window.CN && window.OrderWin && document.readyState === 'complete', null, { timeout: 90000 });
      await page.evaluate(() => { Object.assign(Efficiency.options, { pollMs: 2500, liveMs: 700, growMs: 0, nudgeMs: 1000 }); Object.assign(EfficiencyEmployee.options, { liveMs: 700, rangeMs: 1500, calMs: 1500, ordersMs: 1500, tickMs: 500 }); });
    };
    await section('D', 'Back / Forward / reload on every route; Back then re-open at once is not undone', async () => {
      const page = await fresh();
      const A = '#efficiency/person/Ana%20M.';
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview');
      await page.click(`${V} .efTabBtn[data-tab="stations"]`); await settled(page, '#efficiency/stations', 'efPgStations');
      await page.click(`${V} .efTabBtn[data-tab="people"]`); await settled(page, '#efficiency/people', 'efPgPeople');
      await page.click(`${V} .efRoster .efRc[data-name="Ana M."]`); await settled(page, A, 'efPgPerson'); await ensureRange(page, 'week');
      // Back through every page, then Forward again
      await page.goBack(); await settled(page, '#efficiency/people', 'efPgPeople'); assert.equal((await where(page)).efp, 0, 'the employee page is taken away when it is left');
      await page.goBack(); await settled(page, '#efficiency/stations', 'efPgStations'); await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 });
      await page.goBack(); await settled(page, '#efficiency', 'efPgOverview'); assert.equal((await where(page)).es, 0, 'the stations board is taken away when it is left');
      await page.goForward(); await settled(page, '#efficiency/stations', 'efPgStations'); await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 });
      await page.goForward(); await settled(page, '#efficiency/people', 'efPgPeople');
      await page.goForward(); await settled(page, A, 'efPgPerson'); await ensureRange(page, 'week'); assert.equal((await pstate(page)).name, 'Ana M.');
      // the page's own Back button returns to where the person came from (the People tab) and a second open still works
      await page.click(`${P} .efpBack`); await settled(page, '#efficiency/people', 'efPgPeople');
      // Back then re-open at once: the shell must not undo the re-open (it used to, 300 ms later)
      await page.click(`${V} .efRoster .efRc[data-name="Ana M."]`); await settled(page, A, 'efPgPerson'); await ensureRange(page, 'week');
      const r = await page.evaluate(async () => {
        document.querySelector('#efficiencyView .efp .efpBack').click();
        await new Promise(res => addEventListener('popstate', res, { once: true }));
        document.querySelector('#efficiencyView .efRoster .efRc[data-name="Ana M."]').click();
        await new Promise(res => setTimeout(res, 900));
        return { hash: location.hash, efp: document.querySelectorAll('#efficiencyView .efp').length, person: Efficiency.state.person };
      });
      assert.deepEqual([r.hash, r.person], [A, 'Ana M.'], 'Back then re-open within 300 ms stays on the person: ' + JSON.stringify(r)); assert.equal(r.efp, 1);
      // reload on every route keeps the route (the address is the state)
      for (const [h, id, ready] of [[A, 'efPgPerson', async () => { await page.waitForSelector(P, { timeout: 40000 }); await ensureRange(page, 'week'); }], ['#efficiency/stations', 'efPgStations', () => page.waitForSelector(`${V} .es .esSt`, { timeout: 40000 })], ['#efficiency/people', 'efPgPeople', () => page.waitForSelector(`${V} .efRoster .efRc`, { timeout: 40000 })], ['#efficiency', 'efPgOverview', () => page.waitForSelector(`${V} .efBody:not(.hidden) .efP`, { timeout: 40000 })]]) {
        await page.evaluate(x => { history.replaceState(null, '', x); }, h); await page.evaluate(() => dispatchEvent(new HashChangeEvent('hashchange')));
        await settled(page, h, id); await reloadSorter(page);
        await settled(page, h, id); await ready();
        assert.equal(await page.locator(`${V} .efKey`).isVisible(), false, 'no passcode box after a reload (the passcode is kept for the tab)');
        if (id === 'efPgPerson') assert.equal((await pstate(page)).name, 'Ana M.');
      }
      await page.addScriptTag({ content: jsQR });
    });

    /* ─────────────── E · Real | Sandbox: the board and the person page are destroyed and mounted again, nothing of the other store stays ─────────────── */
    await section('E', 'Real | Sandbox: modules destroyed and mounted again, nothing of the other store stays, a late answer from the other store is dropped', async () => {
      const page = await fresh();
      await page.evaluate(() => { window.__m = { sM: 0, sD: 0, pM: 0, pD: 0 }; const wrap = (M, m, d) => { const orig = M.mount; M.mount = function () { const h = orig.apply(this, arguments); window.__m[m]++; if (h && typeof h.destroy === 'function') { const od = h.destroy; h.destroy = function () { window.__m[d]++; return od.apply(this, arguments); }; } return h; }; }; wrap(EfficiencyStations, 'sM', 'sD'); wrap(EfficiencyEmployee, 'pM', 'pD'); });
      const counts = () => page.evaluate(() => Object.assign({}, window.__m));
      const text_ = () => page.evaluate(() => document.getElementById('efficiencyView').innerText);
      const REAL_ONLY = /Empress D|Ana M|Michael V|Tess Welder|Paul K/;
      await page.evaluate(() => Efficiency.go('stations')); await settled(page, '#efficiency/stations', 'efPgStations'); await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 });
      await waitFor(page, () => /Empress D|Ana M|Michael V/.test(document.getElementById('efficiencyView').innerText), null, 20000);
      let c0 = await counts();
      await page.click(`${V} .efView button[data-view="sandbox"]`); const nReal = seen.eff.length;   // (what was asked up to the press is Real; what was already on its way may still be recorded just after)
      await waitFor(page, () => document.getElementById('efficiencyView').getAttribute('data-view') === 'sandbox' && !!document.querySelector('#efficiencyView .es .esSt'), null, 20000);
      await waitFor(page, n => window.__m.sM > n && window.__m.sD >= 1, c0.sM, 10000);
      let c1 = await counts(); assert.equal(c1.sD, c0.sD + 1, 'the Real board was destroyed'); assert.equal(c1.sM, c0.sM + 1, 'and a new one mounted for the Sandbox');
      await sleep(2500);
      assert(!REAL_ONLY.test(await text_()), 'nothing of the real crew stays on the Sandbox board: ' + (await text_()).replace(/\s+/g, ' ').slice(0, 300));
      assert(await page.locator(`${V} .efFlag`).isVisible(), 'a flag says Sandbox data only'); assert.equal(await page.evaluate(() => sessionStorage.getItem('cn.eff.view')), 'sandbox');
      // the switch is the first request that asks for the Sandbox copies; a read that was already on its way when the button was pressed may still be recorded just before it
      const since = seen.eff.slice(nReal).filter(c => c.B === 'main'), first = since.findIndex(c => c.sandbox === true);
      assert(first >= 0, 'the console asked for the Sandbox copies after the switch'); assert(first <= 2, 'at most the reads already on their way were Real: ' + JSON.stringify(since.slice(0, first + 1).map(c => [c.op, c.sandbox])));
      assert(since.slice(first).every(c => c.sandbox === true), 'every request since the switch asked for the Sandbox copies only: ' + JSON.stringify(since.slice(first).filter(c => c.sandbox !== true).map(c => [c.op, c.sandbox])));
      await shot(page, 'e1-stations-sandbox-1440');
      await page.click(`${V} .efView button[data-view="real"]`);
      await waitFor(page, () => document.getElementById('efficiencyView').getAttribute('data-view') === 'real' && /Empress D|Ana M|Michael V/.test(document.getElementById('efficiencyView').innerText), null, 25000);
      const c2 = await counts(); assert.equal(c2.sD, c1.sD + 1, 'the Sandbox board was destroyed'); assert.equal(c2.sM, c1.sM + 1, 'and the real one mounted again');
      // a person's page: Ana is in the real crew only; Giovanna C. is in both stores with very different numbers
      await page.evaluate(() => Efficiency.go('person', 'Giovanna')); await settled(page, '#efficiency/person/Giovanna', 'efPgPerson'); await ensureRange(page, 'week');
      const realG = await pAsk('Giovanna', 'week'), sbG = await B.ask({ op: 'person', name: 'Giovanna', range: 'week', compare: true, sandbox: true });
      assert.notEqual(realG.kpis.parts.value, sbG.kpis.parts.value, 'the two stores give different numbers for the same name (the test would not see a leak otherwise)');
      await waitFor(page, v => document.querySelector('#efficiencyView .efp .efpK[data-k="kpis.parts"] .efpKV').textContent.replace(/,/g, '').trim() === String(v), realG.kpis.parts.value, 45000)
        .catch(async () => { const again = []; for (let i = 0; i < 3; i++) { const g = await pAsk('Giovanna', 'week'); again.push([g.kpis.parts.value, g.kpis.orders.value, g.to]); await sleep(1500); } throw new Error(`Giovanna (Real, week): the page shows ${await pk(page, 'kpis.parts')} pieces, the server said ${realG.kpis.parts.value} (${realG.to}) and now says ${JSON.stringify(again)}; state ${JSON.stringify(await pstate(page))}`); });
      const d0 = await counts();
      await page.click(`${V} .efView button[data-view="sandbox"]`);
      await waitFor(page, n => window.__m.pM > n, d0.pM, 20000); await ensureRange(page, 'week');
      const d1 = await counts(); assert.equal(d1.pD, d0.pD + 1, 'the real person page was destroyed'); assert.equal(d1.pM, d0.pM + 1, 'and a new one mounted for the Sandbox');
      await waitFor(page, v => document.querySelector('#efficiencyView .efp .efpK[data-k="kpis.parts"] .efpKV').textContent.replace(/,/g, '').trim() === String(v), sbG.kpis.parts.value, 45000)
        .catch(async () => { throw new Error(`Sandbox: the page shows ${await pk(page, 'kpis.parts')} pieces, the Sandbox copies say ${sbG.kpis.parts.value} (the real store says ${realG.kpis.parts.value})`); });
      assert.equal(await hash(page), '#efficiency/person/Giovanna', 'the same person stays in the address');
      await shot(page, 'e2-person-sandbox-1440');
      // a slow REAL answer must not land on the Sandbox view: switch back and forth while the real read is held
      ctl.hook = b => (b.op === 'person' && !b.sandbox ? { delay: 3000 } : null);
      await page.click(`${V} .efView button[data-view="real"]`); await sleep(250); await page.click(`${V} .efView button[data-view="sandbox"]`);
      await waitFor(page, () => document.getElementById('efficiencyView').getAttribute('data-view') === 'sandbox', null, 10000); await sleep(4500); ctl.hook = null;
      await ensureRange(page, 'week');
      await waitFor(page, v => document.querySelector('#efficiencyView .efp .efpK[data-k="kpis.parts"] .efpKV').textContent.replace(/,/g, '').trim() === String(v), sbG.kpis.parts.value, 45000).catch(() => {}); // the figure counts up to its final value
      assert.equal(await pk(page, 'kpis.parts'), String(sbG.kpis.parts.value), `a slow real answer that arrived after the switch was thrown away (real says ${realG.kpis.parts.value}, Sandbox says ${sbG.kpis.parts.value}; mounts ${JSON.stringify(await counts())}; view ${await page.evaluate(() => document.getElementById('efficiencyView').getAttribute('data-view'))})`);
      await page.click(`${V} .efView button[data-view="real"]`); await waitFor(page, v => document.querySelector('#efficiencyView .efp .efpK[data-k="kpis.parts"] .efpKV').textContent.replace(/,/g, '').trim() === String(v), realG.kpis.parts.value, 45000);
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview');
    });

    /* ─────────────── F · a hidden tab reads nothing and catches up; a hung read says so and recovers; a 401 asks the passcode again ─────────────── */
    const reads = (from, filter) => seen.eff.slice(from).filter(c => c.B === 'main' && (!filter || filter(c)));
    const release = () => { for (const r of (ctl.held || []).splice(0)) r(); };
    await section('F', 'a hidden tab and another Workspace tab read nothing and catch up; a hung read says Reconnecting and recovers; a 401 asks the passcode again', async () => {
      const page = await fresh();
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview'); await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 20000);
      // 1 · the page is hidden: no read at all, the timers stop; shown again: a labelled catch-up, then Live
      await hidden(page, true); await sleep(900);
      let m = seen.eff.length; await sleep(4000);
      assert.equal(reads(m).length, 0, `a hidden page makes no request (${JSON.stringify(reads(m).map(c => c.op))})`);
      await hidden(page, false);
      await waitFor(page, n => true, 0, 1000);
      await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 20000); assert(reads(m).some(c => c.op === 'live'), 'shown again, it reads again');
      // 2 · the stations board and the person page stay in place while hidden (their cards and scroll are kept) but read nothing, and carry on when shown again
      for (const [route, id, sel, mod] of [['stations', 'efPgStations', `${V} .es .esSt`], ['person', 'efPgPerson', P]]) {
        if (route === 'person') await page.evaluate(() => Efficiency.go('person', 'Ana M.')); else await page.evaluate(() => Efficiency.go('stations'));
        await settled(page, route === 'person' ? '#efficiency/person/Ana%20M.' : '#efficiency/stations', id); await page.waitForSelector(sel, { timeout: 20000 }); if (route === 'person') await ensureRange(page, 'week');
        await hidden(page, true); await sleep(900); m = seen.eff.length; await sleep(4000);
        assert.equal(reads(m).length, 0, `${route}: a hidden page makes no request (${JSON.stringify(reads(m).map(c => c.op))})`);
        assert(await page.locator(sel).first().count() > 0, `${route}: what was on screen stays while hidden`);
        await hidden(page, false); await waitFor(page, () => true, 0, 500); await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 25000);
        assert(reads(m).length > 0, `${route}: shown again, it reads again`);
      }
      await page.evaluate(() => Efficiency.go('stations')); await settled(page, '#efficiency/stations', 'efPgStations'); await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 });
      // 3 · another Workspace tab open: nothing is read; back on the console it catches up
      await page.evaluate(() => CN.setMode('orders')); await sleep(900); m = seen.eff.length; await sleep(3500);
      assert.equal(reads(m).length, 0, 'another Workspace tab: no request from the console');
      await page.evaluate(() => CN.setMode('efficiency')); await page.waitForSelector(`${V}:not(.hidden) .es .esSt`, { timeout: 20000 });
      await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 20000);
      // 4 · a read that never comes back: the bar says so, the numbers stay, and it recovers by itself
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview'); await page.evaluate(() => Object.assign(Efficiency.options, { timeoutMs: 1500, staleMs: 3000 }));
      const partsBefore = await kpi(page, 'parts'), whoBefore = await siNames(page);
      ctl.hook = b => (b.op === 'live' || b.op === 'overview' ? 'hang' : null);
      await waitFor(page, () => /Reconnecting/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 30000);
      assert.equal(await page.locator(`${V} .efLive`).getAttribute('data-s') !== 'live', true, 'the light is not green while the read hangs');
      assert.equal(await kpi(page, 'parts'), partsBefore, 'the last numbers stay on screen'); assert.deepEqual(await siNames(page), whoBefore, 'and so does who is signed in');
      await shot(page, 'f1-reconnecting-1440');
      ctl.hook = null; release();
      await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 30000);
      await page.evaluate(() => Object.assign(Efficiency.options, { timeoutMs: 9000, staleMs: 13000 }));
      // 5 · the server says 401 (the passcode was changed): the passcode box comes back, the modules go, nothing is read; the right passcode opens the same page
      await page.evaluate(() => Efficiency.go('person', 'Ana M.')); await settled(page, '#efficiency/person/Ana%20M.', 'efPgPerson'); await ensureRange(page, 'week');
      ctl.hook = b => ({ status: 401, json: { ok: false, error: 'Enter the manager passcode.' } });
      await page.waitForSelector(`${V} .efKey:not(.hidden)`, { timeout: 30000 });
      assert.equal((await where(page)).efp, 0, 'the employee page is taken away while the passcode is asked'); assert.equal(await page.locator(`${V} .efBody`).isVisible(), false, 'no numbers behind the passcode box');
      await shot(page, 'f2-passcode-again-1440');
      m = seen.eff.length; await sleep(3000); assert(reads(m).length <= 1, 'no read loop while the passcode is asked: ' + reads(m).length);
      ctl.hook = null;
      await page.fill(`${V} .efKey input`, 'not-the-passcode'); await page.press(`${V} .efKey input`, 'Enter');
      await waitFor(page, sel => document.querySelector(sel).textContent.trim().length > 0, `${V} .efKeyErr`, 20000); assert.equal(await page.locator(`${V} .efKey`).isVisible(), true, 'a wrong passcode keeps the box');
      await page.fill(`${V} .efKey input`, PASS); await page.press(`${V} .efKey input`, 'Enter');
      await page.waitForSelector(P, { timeout: 30000 }); await ensureRange(page, 'week'); assert.equal(await hash(page), '#efficiency/person/Ana%20M.', 'the same page opens after the passcode');
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview');
    });

    /** open or fold the rail and wait until the workspace has stopped sliding */
    const railTo = async (page, open) => {
      await page.evaluate(open => { const app = document.getElementById('app'); if (app.classList.contains('railOff') === open) document.getElementById('btnRail').click(); }, open);
      let last = -1; for (let i = 0; i < 40; i++) { const l = await page.evaluate(() => Math.round(document.getElementById('efficiencyView').closest('.stage').getBoundingClientRect().left * 10)); if (l === last) return; last = l; await sleep(250); }
    };
    const fit = (page, route) => page.evaluate(() => {
      // the scroller of the sorter's workspace (.stage) holds the console; its own width is the limit. The bar bleeds into the stage's padding on purpose, never past the stage.
      const v = document.getElementById('efficiencyView'), stage = v.closest('.stage'), sr = stage.getBoundingClientRect(), cs = getComputedStyle(stage);
      const pl = parseFloat(cs.paddingLeft) || 0, pr = parseFloat(cs.paddingRight) || 0, lo = sr.left + pl - 1, hi = sr.right - pr + 1, bad = [], badEls = [];
      const bar = v.querySelector('.efBar'), br = bar.getBoundingClientRect();
      for (const e of v.querySelectorAll('*')) {
        if (e === bar || bar.contains(e)) continue;
        if (!e.getClientRects().length) continue;
        const r = e.getBoundingClientRect(); if (r.width === 0 || r.height === 0) continue;
        let clipped = false; for (let a = e.parentElement; a && a !== v; a = a.parentElement) { const o = getComputedStyle(a).overflowX; if (o === 'auto' || o === 'hidden' || o === 'scroll' || o === 'clip') { clipped = true; break; } }
        if (!clipped && (r.right > hi || r.left < lo) && getComputedStyle(e).position !== 'fixed' && !e.closest('svg') && !e.closest('.efc-tip, .esTip, .efpHC, [role="tooltip"]')) badEls.push(e), bad.push((typeof e.className === 'string' && e.className ? '.' + e.className.split(' ')[0] : e.tagName) + ' ' + Math.round(r.left) + '..' + Math.round(r.right));
      }
      // what sits inside the bar must sit inside the bar (its own padding edge), else it spills past the stage
      const inner = [...bar.children].filter(c => c.getClientRects().length).map(c => { const r = c.getBoundingClientRect(); return { c: c.className.split(' ')[0], over: Math.round(r.right - br.right), under: Math.round(br.left - r.left) }; }).filter(x => x.over > 0 || x.under > 0);
      let chain = []; if (bad.length) { const first = badEls[badEls.length - 1] || null; let a = first; while (a && a !== v.parentElement && chain.length < 8) { const c = getComputedStyle(a), r = a.getBoundingClientRect(); chain.push(`${a.tagName.toLowerCase()}${typeof a.className === 'string' && a.className ? '.' + a.className.split(' ').join('.') : ''} ${Math.round(r.left)}..${Math.round(r.right)} ${c.display} cols=${c.gridTemplateColumns.slice(0, 60)} minw=${c.minWidth} ws=${c.whiteSpace} ovf=${c.overflowX}`); a = a.parentElement; } }
      return { chain, doc: document.documentElement.scrollWidth - innerWidth, stage: stage.scrollWidth - stage.clientWidth, view: v.scrollWidth - v.clientWidth, barOver: Math.round(br.right - sr.right), barUnder: Math.round(sr.left - br.left), inner, bad: bad.slice(0, 6), stageBox: `${Math.round(sr.left)}..${Math.round(sr.right)}`, barBox: `${Math.round(br.left)}..${Math.round(br.right)}`, barH: Math.round(br.height) };
    });

    /* ─────────────── G · an empty shop, a person with no data, a very long name ─────────────── */
    await section('G', 'an empty shop says so in words; a person with no data; a very long name fits', async () => {
      // the empty shop: its own process with nothing in it
      const E0 = await backend({ PORTAL_EMPTY: '1' });
      try {
        const ctx = await context({ B: E0, employee: null }), page = await openSorter(ctx); await openConsole(page);   // (the sorter has no person of its own until one is typed)
        await page.waitForSelector(`${V} .efBody:not(.hidden)`, { timeout: 40000 });
        await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 30000);
        const body = (await text(page, V));
        assert(/No one is signed in right now|No one is signed in/.test(body), 'an honest "nobody is signed in": ' + body.slice(0, 300)); assert(/No one is working on an order right now/.test(body), 'and nobody has an order in hand');
        assert(!/NaN|undefined|Infinity|\[object/.test(body), 'no broken numbers on an empty shop: ' + body.slice(0, 200));
        assert.equal(await kpi(page, 'on'), '0'); await shot(page, 'g1-empty-shop-overview-1440');
        await page.click(`${V} .efTabBtn[data-tab="stations"]`); await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 });
        const keys = await page.$$eval(`${V} .es .esSt`, rs => rs.map(r => [r.dataset.key, r.dataset.state]));
        assert(keys.length >= 5 && keys.every(([, s]) => s === 'offline'), 'every station is listed, offline: ' + JSON.stringify(keys)); assert(!/NaN|undefined|Infinity/.test(await text(page, V)));
        await shot(page, 'g2-empty-shop-stations-1440');
        await page.click(`${V} .efTabBtn[data-tab="people"]`); await waitFor(page, () => /No one has signed in|Nobody signed in/.test(document.getElementById('efficiencyView').innerText), null, 20000);
        await page.evaluate(() => Efficiency.go('person', 'Nobody Here')); await page.waitForSelector(P, { timeout: 30000 }); await ensureRange(page, 'week');
        const pt = await text(page, P); assert(/No sign-ins or activity were found|nothing is logged|Nothing is logged/i.test(pt), 'a person nobody logged says so: ' + pt.slice(0, 300)); assert(!/NaN|undefined|Infinity|\[object/.test(pt), 'no broken numbers: ' + pt.slice(0, 200));
        await shot(page, 'g3-empty-shop-person-1440'); await ctx.close();
      } finally { E0.kill(); }
      // the real shop: a person nobody has logged anything for, and a very long name
      const page = await fresh();
      await page.evaluate(() => Efficiency.go('person', 'Zed Nobody')); await settled(page, '#efficiency/person/Zed%20Nobody', 'efPgPerson'); await ensureRange(page, 'week');
      const zt = await text(page, P); assert(/No sign-ins or activity were found/.test(zt), 'a name with no data says so: ' + zt.slice(0, 300)); assert(!/NaN|undefined|Infinity|\[object/.test(zt), 'no broken numbers for a person with no data');
      assert.equal(await page.locator(`${P} .efpK[data-k="kpis.parts"] .efpKV`).innerText().then(s => s.trim()), '—', 'a figure nobody logged is a dash, not a 0');
      await shot(page, 'g4-person-no-data-1440');
      const LONG = 'Bartholomew-Maximilian Featherstonehaugh-Cholmondeley-Wolfeschlegelstein';
      const lp = await stationPage(shared.ctx, 'sorting', LONG); await scan(lp, R_G, 'typed'); await page.bringToFront();   // (a tab behind another one draws no frames: its page-in slide and the rail's fold would stand still)
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview');
      await waitFor(page, n => document.getElementById('efficiencyView').innerText.includes(n.slice(0, 20)), LONG, 30000);
      const longBad = [];
      for (const [w, railOpen] of [[1440, true], [900, true], [390, false]]) {
        await page.setViewportSize({ width: w, height: 900 });
        await railTo(page, railOpen);
        for (const [h, id] of [['#efficiency', 'efPgOverview'], ['#efficiency/stations', 'efPgStations'], ['#efficiency/people', 'efPgPeople'], ['#efficiency/person/' + encodeURIComponent(LONG), 'efPgPerson']]) {
          await page.evaluate(x => { history.pushState(null, '', x); dispatchEvent(new HashChangeEvent('hashchange')); }, h); await settled(page, h, id);
          if (id === 'efPgPerson') await ensureRange(page, 'week'); if (id === 'efPgStations') await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 }); if (id === 'efPgPeople') await page.waitForSelector(`${V} .efRoster .efRc`, { timeout: 20000 });
          await sleep(400); const f = await fit(page, id);
          if (process.env.DEBUGG && id === 'efPgPerson') console.log('DEBUGG', w, JSON.stringify(await page.evaluate(() => { const v = document.getElementById('efficiencyView'); const out = []; let a = [...v.querySelectorAll('.efp')].pop(); while (a && a !== v.parentElement) { const c = getComputedStyle(a), r = a.getBoundingClientRect(); out.push(`${a.tagName.toLowerCase()}.${typeof a.className === 'string' ? a.className.split(' ').join('.') : ''} l=${Math.round(r.left)} r=${Math.round(r.right)} tr=${c.transform} tx=${c.translate} ml=${c.marginLeft} pl=${c.paddingLeft} pos=${c.position} left=${c.left} jc=${c.justifyContent} ji=${c.justifyItems} anim=${a.getAnimations ? a.getAnimations().map(x => (x.animationName || x.id || 'x') + ':' + x.playState + ':' + Math.round(x.currentTime)).join(',') : ''}`); a = a.parentElement; } return out; })));
          if (f.doc > 0 || f.stage > 0 || f.view > 0 || f.inner.length || f.bad.length) longBad.push(`long name at ${w}px on ${id}: ${JSON.stringify(f)}`);
          if (id === 'efPgOverview' || id === 'efPgPerson') await shot(page, `g5-long-name-${id === 'efPgPerson' ? 'person' : 'overview'}-${w}`);
        }
      }
      await page.setViewportSize({ width: 1440, height: 900 }); await page.evaluate(() => { const app = document.getElementById('app'); if (app.classList.contains('railOff')) document.getElementById('btnRail').click(); });
      await lp.evaluate(() => window.__signOut()); await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview');
      assert.deepEqual(longBad, [], 'a very long name overflows:\n      ' + longBad.join('\n      '));
    });

    /* ─────────────── I · one number everywhere: Overview, People, the Stations board, the person page and its order list agree ─────────────── */
    await section('I', 'one number everywhere: the Overview, the People cards, the Stations board, the person page (Day) and its order list agree for the same day', async () => {
      const page = await fresh();
      const fig = (page, sel) => page.locator(sel).innerText().then(s => s.replace(/,/g, '').trim());
      const same = async (sel, want, what) => waitFor(page, ([sel, want]) => { const e = document.querySelector(sel); return !!e && e.textContent.replace(/,/g, '').trim() === String(want); }, [sel, want], 25000)
        .catch(async () => { const o2 = await B.ask({ op: 'overview', trend: false }), l2 = await B.ask({ op: 'live' }); throw new Error(`${what}: the screen says "${await page.locator(sel).first().innerText().catch(() => '(missing)')}", the server says ${want} (now: overview stations ${JSON.stringify(o2.business.stations.map(x => [x.station, x.parts, x.orders]))}; live counts ${JSON.stringify(l2.stations.map(x => [x.key, x.counts.partsToday, x.counts.ordersToday]))})`); });
      // an order IN HAND (scanned, not finished) at Sorting: the day's order counts must already include it everywhere (they did not on the board: it counted only finished orders)
      const base = (await B.ask({ op: 'overview', trend: false })).business.stations.find(x => x.station === 'sorting').orders;
      const ip = await stationPage(shared.ctx, 'sorting', 'Empress D.'); await scan(ip, R_I, 'in hand'); await page.bringToFront();
      let ov, lv; const t0 = Date.now();
      for (;;) {   // (both answers keep today's numbers for a few seconds, the live one for up to 20 s: wait until the Overview has the scan, then until the live answer agrees)
        ov = await B.ask({ op: 'overview', trend: false }); lv = await B.ask({ op: 'live' });
        if (ov.business.stations.find(x => x.station === 'sorting').orders < base + 1 && Date.now() - t0 < 40000) { await sleep(1500); continue; }
        const diff = ov.business.stations.filter(x => (lv.stations.find(l => l.key === x.station) || { counts: {} }).counts.ordersToday !== x.orders).map(x => `${x.station}: overview ${x.orders}, live ${(lv.stations.find(l => l.key === x.station) || { counts: {} }).counts.ordersToday}`);
        if (!diff.length) break; if (Date.now() - t0 > 40000) assert.fail('the live board and the Overview count a different number of orders per station while an order is in hand: ' + diff.join('; ')); await sleep(2000);
      }
      const T = ov.business.totals, names = ov.people.map(p => p.name), onNames = ov.people.filter(p => p.status === 'on').map(p => p.name).sort();
      // 1 · the Overview: the day's totals and who is in
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview');
      await same(`${V} .efKpi[data-k="parts"] .efKV`, T.parts, 'Overview parts today'); await same(`${V} .efKpi[data-k="orders"] .efKV`, T.orders, 'Overview orders today'); await same(`${V} .efKpi[data-k="on"] .efKV`, onNames.length, 'Overview people on now');
      await waitFor(page, n => JSON.stringify([...document.querySelectorAll('#efficiencyView .efSiGrid .efSiNm')].map(e => e.textContent.trim()).sort()) === JSON.stringify(n), onNames, 25000).catch(() => {});
      assert.deepEqual(await siNames(page), onNames, 'Signed in now = the people the sign-ins say');
      // 2 · the Stations board: each station's day counts are the server's, and they add up to the Overview's totals
      await page.click(`${V} .efTabBtn[data-tab="stations"]`); await settled(page, '#efficiency/stations', 'efPgStations'); await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 });
      let boardParts = 0, boardOrders = 0;
      for (const s of ov.business.stations) {
        await same(`${V} .es .esSt[data-key="${s.station}"] .esCnt [data-n="parts"]`, s.parts, `the board's ${s.station} parts`); await same(`${V} .es .esSt[data-key="${s.station}"] .esCnt [data-n="orders"]`, s.orders, `the board's ${s.station} orders`);
        const l = lv.stations.find(x => x.key === s.station); assert.equal(l.counts.partsToday, s.parts, `${s.station}: the live door and the overview count the same parts`);
        boardParts += s.parts; boardOrders += s.orders;
      }
      assert.equal(boardParts, T.parts, 'the stations add up to the day: pieces'); assert.equal(boardOrders, T.orders, 'the stations add up to the day: orders');
      // 3 · People: a card per person, with the server's figures; the pieces of all the cards are the day's pieces
      await page.click(`${V} .efTabBtn[data-tab="people"]`); await settled(page, '#efficiency/people', 'efPgPeople'); await page.waitForSelector(`${V} .efRoster .efRc`, { timeout: 20000 });
      let cardParts = 0; const card = {}, noneCard = {};
      for (const p of ov.people) {
        const sel = `${V} .efRoster .efRc[data-name="${p.name}"]`; await page.waitForSelector(sel, { timeout: 20000 });
        if (p.totals.parts === 0 && p.totals.orders === 0) { const t = await page.locator(`${sel} [data-f="parts"]`).innerText(); assert(/^(—|0)$/.test(t.trim()), `${p.name} logged nothing: the card says a dash or 0, not "${t}"`); noneCard[p.name] = [t.trim(), (await page.locator(`${sel} [data-f="orders"]`).innerText()).trim()]; }
        else { await same(`${sel} [data-f="parts"]`, p.totals.parts, `${p.name}'s card: pieces`); await same(`${sel} [data-f="orders"]`, p.totals.orders, `${p.name}'s card: orders`); }
        card[p.name] = { parts: p.totals.parts, orders: p.totals.orders, on: p.status === 'on', live: await page.locator(`${sel}`).evaluate(e => e.classList.contains('on')) };
        cardParts += p.totals.parts;
      }
      assert.equal(cardParts, T.parts, 'the people add up to the day: pieces'); assert.deepEqual(Object.entries(card).filter(([, c]) => c.on !== c.live).map(([n]) => n), [], 'a card is lit exactly when the person is signed in');
      // 4 · the person page on Day: the same two figures as the card, and its order list holds those orders and those pieces
      const rowsInfo = [];
      for (const p of ov.people) {
        await page.click(`${V} .efRoster .efRc[data-name="${p.name}"]`); await settled(page, '#efficiency/person/' + encodeURIComponent(p.name), 'efPgPerson'); await ploaded(page);   // (the page keeps the range last chosen)
        if ((await pstate(page)).range !== 'day') await page.click(`${P} .efpSeg button[data-range="day"]`); await ploaded(page, 'day');
        const none = p.totals.parts === 0 && p.totals.orders === 0;
        if (none) { const pg = [await pk(page, 'kpis.parts'), await pk(page, 'kpis.orders')]; rowsInfo.push(`${p.name}: card ${noneCard[p.name].join('/')}, page ${pg.join('/')}`); assert.equal(pg[0], noneCard[p.name][0], `${p.name}: the card and the page say the same about a person who logged no pieces (card "${noneCard[p.name][0]}", page "${pg[0]}")`); }
        else {
          await same(`${P} .efpK[data-k="kpis.parts"] .efpKV`, p.totals.parts, `${p.name}'s page (Day): pieces`); await same(`${P} .efpK[data-k="kpis.orders"] .efpKV`, p.totals.orders, `${p.name}'s page (Day): orders`);
          await page.locator(`${P} .efoSearch input`).evaluate(e => e.scrollIntoView({ block: 'center' }));
          await waitFor(page, n => { const c = document.querySelector('#efficiencyView .efp .efoCount'); return !!c && +c.textContent.trim().split(' ')[0].replace(/,/g, '') === n; }, p.totals.orders, 25000).catch(async () => { throw new Error(`${p.name}: the order list says "${await text(page, `${P} .efoCount`)}" for the day; the card says ${p.totals.orders} orders`); });
          await waitFor(page, n => document.querySelectorAll('#efficiencyView .efp .efoRow').length >= n, p.totals.orders, 25000);
          const rows = await orderRows(page); assert.equal(rows.length, p.totals.orders, `${p.name}: one row per order of the day`);
          if (process.env.DEBUGI) console.log('DEBUGI', JSON.stringify(rows.slice(0, 2)));
          const srvList = await allOrders(p.name, ov.day, ov.day); assert.equal(srvList.orders.reduce((a, o) => a + o.parts, 0), p.totals.parts, `${p.name}: the pieces of the day's orders add up to the card`);
          assert.deepEqual(rows.map(r => r.rid).sort(), srvList.orders.map(o => o.rid).sort(), `${p.name}: the rows are the orders of the day`);
        }
        await page.click(`${P} .efpBack`); await settled(page, '#efficiency/people', 'efPgPeople'); await page.waitForSelector(`${V} .efRoster .efRc`, { timeout: 20000 });
      }
      await ip.evaluate(() => window.__signOut()); await page.bringToFront();
      if (rowsInfo.length) console.log('    note (nobody logged anything today):', rowsInfo.join(' | '));
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview');
    });

    /* ─────────────── J · what an open console costs: calls and document reads per minute and per hour, with the shipped timings (nothing else is running) ─────────────── */
    await section('J', 'read cost of an open console with the shipped timings: Overview, Stations, a person page; a hidden page costs nothing', async () => {
      const J0 = await backend(), WIN = +process.env.COST_SECONDS || 40;
      try {
        const ctx = await context({ B: J0 }), page = await openSorter(ctx, { slow: true }); await openConsole(page);
        await page.waitForSelector(`${V} .efBody:not(.hidden) .efP`, { timeout: 40000 }); await waitFor(page, () => /^Live/.test(document.querySelector('#efficiencyView .efLiveT').textContent), null, 30000);
        const cost = {}, measure = async (label, ms) => {
          await sleep(3500); await J0.reads(true); const m = seen.eff.length; seen.eff.length; const t0 = Date.now(); await sleep(ms);
          const r = await J0.reads(), secs = (Date.now() - t0) / 1000, calls = seen.eff.slice(m).filter(c => c.B === 'other'), by = {}; for (const c of calls) by[c.op] = (by[c.op] || 0) + 1;
          cost[label] = { secs: Math.round(secs), calls: by, callsPerHour: Math.round(calls.length / secs * 3600), docReadsPerHour: Math.round(r.reads / secs * 3600), byColl: r.by };
          return cost[label];
        };
        await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview'); await measure('overview', WIN * 1000);
        await page.evaluate(() => Efficiency.go('stations')); await settled(page, '#efficiency/stations', 'efPgStations'); await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 }); await measure('stations', WIN * 1000);
        await page.evaluate(() => Efficiency.go('person', 'Ana M.')); await settled(page, '#efficiency/person/Ana%20M.', 'efPgPerson'); await ploaded(page); await measure('person', WIN * 1000);
        await hidden(page, true); await sleep(1500); const m = seen.eff.length; await J0.reads(true); await sleep(8000); const hr = await J0.reads();
        assert.equal(seen.eff.slice(m).filter(c => c.B === 'other').length, 0, 'a hidden console makes no call'); assert.equal(hr.reads, 0, 'and reads no document');
        await hidden(page, false);
        console.log('    cost per open view (shipped timings, ' + WIN + ' s windows):');
        for (const [k, c] of Object.entries(cost)) console.log(`      ${k.padEnd(9)} ${JSON.stringify(c.calls)} in ${c.secs}s = ${c.callsPerHour} calls/h, ${c.docReadsPerHour} document reads/h  ${JSON.stringify(c.byColl)}`);
        for (const [k, c] of Object.entries(cost)) { assert(c.callsPerHour <= 3600 / 3 + 600 + 600, `${k}: the console makes more calls than "about every 3 s plus the overview about every 10 s" (${c.callsPerHour}/h)`); assert(c.callsPerHour >= 600, `${k}: the console is reading (${c.callsPerHour}/h)`); assert(c.docReadsPerHour < 150000, `${k}: document reads per hour stay modest (${c.docReadsPerHour})`); }
        await ctx.close();
      } finally { J0.kill(); }
    });

    /* ─────────────── K · scrolled down a person page: the console's header bar and the page's own range bar stack, they do not cover each other ─────────────── */
    await section('K', 'a person page scrolled down: the console header stays readable, the range bar sits under it', async () => {
      const page = await fresh(); const problems = [];
      await page.bringToFront();
      for (const [w, railOpen] of [[1440, true], [900, true], [390, false]]) {
        await page.setViewportSize({ width: w, height: 800 }); await railTo(page, railOpen);
        await page.evaluate(() => Efficiency.go('person', 'Ana M.')); await settled(page, '#efficiency/person/Ana%20M.', 'efPgPerson'); await ploaded(page);
        await page.waitForSelector(`${P} .efpBar`, { timeout: 20000 }); await sleep(900);
        const r = await page.evaluate(async () => {
          const v = document.getElementById('efficiencyView'), stage = v.closest('.stage'), bar = v.querySelector('.efBar'), pb = v.querySelector('.efp .efpBar');
          const out = [];
          for (const y of [0, 400, 900, 1600]) {
            stage.scrollTop = y; await new Promise(r => setTimeout(r, 150));
            const b = bar.getBoundingClientRect(), q = pb.getBoundingClientRect(), mid = (b.left + b.right) / 2;
            const top = document.elementFromPoint(b.left + 60, Math.max(1, b.top + Math.min(12, b.height / 2))), topPb = document.elementFromPoint(q.left + 20, q.top + q.height / 2);
            out.push({ y: stage.scrollTop, barTop: Math.round(b.top), barBottom: Math.round(b.bottom), rangeTop: Math.round(q.top), rangeBottom: Math.round(q.bottom), overlap: Math.round(b.bottom - q.top), headerOnTop: !!top && bar.contains(top), rangeOnTop: !!topPb && pb.contains(topPb), stuck: q.top < 120 });
          }
          stage.scrollTop = 0; return { out, stageTop: Math.round(stage.getBoundingClientRect().top), pbTopVar: getComputedStyle(pb).top };
        });
        for (const o of r.out) if (o.y > 0 && (o.overlap > 1 || !o.headerOnTop)) problems.push(`${w}px scrolled ${o.y}: console header ${o.barTop}..${o.barBottom}, range bar ${o.rangeTop}..${o.rangeBottom} (overlap ${o.overlap}px, header readable: ${o.headerOnTop}, sticky top ${r.pbTopVar})`);
        if (process.env.DEBUGK) console.log('DEBUGK', w, JSON.stringify(r));
        if (w === 1440) { await page.evaluate(() => { document.getElementById('efficiencyView').closest('.stage').scrollTop = 900; }); await sleep(300); await shot(page, 'k-person-scrolled-1440'); await page.evaluate(() => { document.getElementById('efficiencyView').closest('.stage').scrollTop = 0; }); }
      }
      await page.setViewportSize({ width: 1440, height: 900 }); await railTo(page, true);
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview');
      assert.deepEqual(problems, [], 'the sticky bars cover each other:\n      ' + problems.join('\n      '));
    });

    /* ─────────────── L · the Stations board by hand: hover a station and a person, press a name, press an order, Back ─────────────── */
    await section('L', 'the Stations board: hover a station and a person, press a name to open the person page, press an order to open it, Back returns to the board', async () => {
      const ctx = shared.ctx, page = await fresh();
      const L1 = await stationPage(ctx, 'assembly-3', 'Ivy Y.'); await scan(L1, R_L, 'by hand'); await page.bringToFront();
      await page.evaluate(() => Efficiency.go('stations')); await settled(page, '#efficiency/stations', 'efPgStations');
      const card = `${V} .es .esSt[data-key="assembly"] .esCard[data-rid="${R_L}"]`;
      await page.waitForSelector(card, { timeout: 30000 }); await waitFor(page, sel => !document.querySelector(sel + ' [data-state="wait"]'), card, 20000).catch(() => {});
      // hover the station name: a card names the station and what is in it
      await page.locator(`${V} .es .esSt[data-key="assembly"] .esStId`).hover(); await page.waitForSelector('.esTip[data-on]', { timeout: 8000 });
      let tip = (await page.locator('.esTip[data-on]').innerText()).replace(/\s+/g, ' '); assert(/Assembly/.test(tip), 'the station card names the station: ' + tip);
      // hover the person chip in the station header
      const chip = `${V} .es .esSt[data-key="assembly"] .esPer[data-name="Ivy Y."]`; await page.waitForSelector(chip, { timeout: 20000 });
      await page.mouse.move(2, 2); await sleep(250); await page.locator(chip).hover(); await page.waitForSelector('.esTip[data-on]', { timeout: 8000 });
      tip = (await page.locator('.esTip[data-on]').innerText()).replace(/\s+/g, ' '); assert(/Ivy/.test(tip), 'the person card names the person: ' + tip);
      await shot(page, 'l1-board-hover-person-1440'); await page.mouse.move(2, 2);
      // a piece picture carries its piece name
      const pcs = await page.$$eval(`${card} .esPcTh`, ps => ps.map(p => p.title)); assert.equal(pcs.length, 3, 'one picture per piece (2 + 1): ' + JSON.stringify(pcs)); assert(pcs.every(t => t.length > 0), 'every piece picture says what it is');
      // press the order's own number: the order window opens over the board, Escape closes it, the board is still there
      await page.locator(`${card} .esOid`).click(); await page.waitForSelector('#orderWin[open]', { timeout: 15000 });
      assert(new RegExp(R_L).test(await page.locator('#orderWin').innerText()), 'the order window is for order ' + R_L); await shot(page, 'l2-board-order-window-1440');
      await page.keyboard.press('Escape'); await waitFor(page, () => !document.querySelector('#orderWin[open]'), null, 10000);
      assert.equal(await hash(page), '#efficiency/stations'); assert(await page.locator(card).count() === 1, 'the board still shows the order');
      // press the person's name on the card: their page opens, showing the order they are working on; Back returns to the board with the card in place
      await page.locator(`${card} .esWho`).click(); await settled(page, '#efficiency/person/Ivy%20Y.', 'efPgPerson'); await ploaded(page);
      await waitFor(page, rid => !!document.querySelector(`#efficiencyView .efp .esCard[data-rid="${rid}"]`) || document.querySelector('#efficiencyView .efp .efpLive') && document.querySelector('#efficiencyView .efp').textContent.includes(rid), R_L, 25000).catch(async () => { throw new Error('the page of the person at the station shows the order in hand: ' + (await text(page, `${P} .efpWhere`)) + ' | ' + (await text(page, P)).slice(0, 300)); });
      await shot(page, 'l3-person-from-board-1440');
      await page.goBack(); await settled(page, '#efficiency/stations', 'efPgStations'); await page.waitForSelector(card, { timeout: 20000 }); assert.equal((await where(page)).efp, 0, 'the person page is taken away');
      // the person finishes and signs out: the card leaves the board, the chip goes
      await finish(L1, R_L, 3); await L1.evaluate(() => window.__signOut()); await page.bringToFront();
      await waitFor(page, sel => !document.querySelector(sel), card, 30000); await waitFor(page, sel => !document.querySelector(sel), chip, 30000);
      await page.evaluate(() => Efficiency.go('overview')); await settled(page, '#efficiency', 'efPgOverview');
    });

    /* ─────────────── H · 1440 / 900 / 390 px: nothing sideways, the header bar fits ─────────────── */
    await section('H', '1440 / 900 / 390 px (rail open and folded) on every route: no sideways scroll, the header bar fits', async () => {
      const page = await fresh();
      const A = '#efficiency/person/Ana%20M.';
      const routes = [['overview', '#efficiency', 'efPgOverview'], ['stations', '#efficiency/stations', 'efPgStations'], ['people', '#efficiency/people', 'efPgPeople'], ['person', A, 'efPgPerson']];
      const problems = [];
      for (const [w, railOpen] of [[1440, true], [900, true], [900, false], [390, false]]) {
        await page.setViewportSize({ width: w, height: 900 });
        await railTo(page, railOpen);
        for (const [name, h, id] of routes) {
          await page.evaluate(x => { if (location.hash !== x) { history.pushState(null, '', x); dispatchEvent(new HashChangeEvent('hashchange')); } }, h);
          await settled(page, h, id);
          if (name === 'person') await ensureRange(page, 'week'); if (name === 'stations') await page.waitForSelector(`${V} .es .esSt`, { timeout: 20000 }); if (name === 'people') await page.waitForSelector(`${V} .efRoster .efRc`, { timeout: 20000 });
          await sleep(500);
          const f = await fit(page, name);
          const tag = `${w}px rail ${railOpen ? 'open' : 'folded'} ${name}`;
          if (f.doc > 0 || f.stage > 0 || f.view > 0 || f.barOver > 0 || f.barUnder > 0 || f.inner.length || f.bad.length) problems.push(`${tag}: page ${f.doc}px, stage ${f.stage}px, view ${f.view}px, bar ${f.barBox} in stage ${f.stageBox}, spilling inside the bar: ${JSON.stringify(f.inner)}, outside: ${f.bad.join(' | ')}`);
          if (name === 'overview' || name === 'person') await shot(page, `h-${name}-${w}${railOpen ? '' : '-folded'}`);
        }
      }
      await page.setViewportSize({ width: 1440, height: 900 }); await page.evaluate(() => { const app = document.getElementById('app'); if (app.classList.contains('railOff')) document.getElementById('btnRail').click(); });
      assert.deepEqual(problems, [], 'sideways overflow:\n      ' + problems.join('\n      '));
    });
  } finally {
    await browser.close(); srv.close && srv.close(); B.kill();
  }
  const bad = results.filter(r => r[1]);
  if (errors.length) console.log('  page errors:\n    ' + [...new Set(errors)].slice(0, 12).join('\n    '));
  console.log(`\n${results.length - bad.length}/${results.length} sections passed`);
  if (bad.length || errors.length) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
