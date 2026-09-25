// The Design Stations with the operator passcode set (EDIT_PASSCODE): a station on its own asks for it once per browser
// session, before anything else, as the sorter does (a wrong one is refused; a reload asks nothing); every call to the
// emulated Etsy and the order archive carries it; a passcode changed meanwhile is asked for again and the refused call goes
// once more; the station the sorter frames never asks: the sorter hands its passcode over. The older station (design.html)
// asks the same way for its archive. With no passcode set, nothing is asked (the other browser tests run that way).
//   node tests/charm-nest/station-passcode.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

const day = Math.floor(Date.now() / 1000);
const receipt = (rid, i) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + i, country_iso: 'US', status: 'Paid', is_paid: true, is_shipped: false, create_timestamp: day - 3600 * i, created_timestamp: day - 3600 * i, update_timestamp: day - 3600 * i, transactions: [{ transaction_id: rid * 10 + 1, receipt_id: rid, listing_id: 1718001, sku: 'BR-TST-01', title: 'charm', quantity: 1, expected_ship_date: day + 3 * 86400 }] });

(async () => {
  let PASS = 'pc-test'; process.env.EDIT_PASSCODE = PASS;   // the real emulator and library handlers read it per call
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  const snapPath = 'charmnest/sandbox/orders-passcode-test.json', snapshot = [receipt(4170000101, 1), receipt(4170000102, 2)];
  st.blobs.set(snapPath, { buf: Buffer.from(JSON.stringify({ at: Date.now(), count: snapshot.length, receipts: snapshot })), generation: 1, meta: { contentType: 'application/json', metadata: {} } });
  st.put('Charm_Sandbox', 'current', { path: snapPath, count: snapshot.length, at: Date.now(), takenBy: 'test' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  async function context(init) {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    // the passcode door as Netlify runs it (authGate), and the archive behind it
    const cors = { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' };
    await ctx.route(/\/\.netlify\/functions\/authGate/, r => { const q = r.request(); if (q.method() === 'GET') return r.fulfill({ status: 200, contentType: 'application/json', headers: cors, body: JSON.stringify({ ok: true, service: 'authGate', locked: true }) }); const ok = q.headers()['x-edit-passcode'] === PASS; return r.fulfill({ status: ok ? 200 : 401, contentType: 'application/json', headers: cors, body: JSON.stringify(ok ? { ok: true } : { error: 'unauthorized' }) }); });
    await ctx.route(/\/\.netlify\/functions\/designArchive/, r => r.request().method() === 'OPTIONS' ? r.continue() : r.request().headers()['x-edit-passcode'] === PASS ? r.continue() : r.fulfill({ status: 401, contentType: 'application/json', headers: cors, body: '{"error":"unauthorized"}' }));
    await ctx.addInitScript(init || (() => {}), { station: stationOrigin, sorter: sorterOrigin });
    await ctx.addInitScript(({ station }) => { if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); } window.confirm = () => true; window.alert = () => {}; }, { station: stationOrigin });
    return ctx;
  }
  const watch = (page, who) => { const seen = []; page.on('pageerror', e => errors.push(`${who}: ${e.message}`)); page.on('response', r => { const m = /\/\.netlify\/functions\/(etsySandbox|designArchive)\b/.exec(r.url()); if (m && r.request().method() !== 'OPTIONS') seen.push({ fn: m[1], status: r.status() }); }); return seen; };
  const box = page => page.evaluate(() => { const b = document.getElementById('pcGate'); return !!(b && b.isConnected && b.offsetParent !== null); });

  // ── a station on its own, in the sandbox: asked once, before anything, and a wrong passcode is refused ──
  let ctx = await context(), page = await ctx.newPage(), seen = watch(page, 'station');
  await page.goto(`${stationOrigin}/design-1.html?sandbox=1`);
  await page.waitForSelector('#pcGate .pcInput', { timeout: 30000 });
  assert(!seen.length, 'nothing is read from the emulator or the archive before the passcode: ' + JSON.stringify(seen));
  await page.fill('#pcGate .pcInput', 'nope'); await page.click('#pcGate button[type=submit]');
  await page.waitForFunction(() => /not accepted/.test(document.querySelector('#pcGate .err').textContent), null, { timeout: 10000 });
  await page.fill('#pcGate .pcInput', PASS); await page.click('#pcGate button[type=submit]');
  await page.waitForFunction(() => !document.getElementById('pcGate'), null, { timeout: 10000 });
  const listed = async () => { await page.click('#updateOrderListBtn'); await page.waitForFunction(() => document.querySelectorAll('.orderRow[data-receipt]').length >= 2, null, { timeout: 60000 }); };
  await listed();
  assert(seen.some(s => s.fn === 'etsySandbox' && s.status === 200) && seen.every(s => s.status !== 401), 'signed in: the emulated Etsy answers every call: ' + JSON.stringify(seen));
  const arch = await page.evaluate(() => fetch(`${FN}/designArchive?op=have&ids=4170000101&sandbox=1`).then(r => r.status));
  assert.strictEqual(arch, 200, 'and the archive answers its calls');
  seen.length = 0; await page.reload();
  await page.waitForSelector('#updateOrderListBtn', { timeout: 30000 }); await listed();
  assert(!(await box(page)) && seen.every(s => s.status === 200), 'a reload in the same session asks nothing: ' + JSON.stringify(seen));
  // the passcode is changed meanwhile: the next call is refused, the box asks again, and that call goes once more
  PASS = process.env.EDIT_PASSCODE = 'pc-new';
  const late = page.evaluate(() => fetch(`${FN}/designArchive?op=have&ids=4170000102&sandbox=1`).then(r => r.status));
  await page.waitForSelector('#pcGate .pcInput', { timeout: 20000 });
  await page.fill('#pcGate .pcInput', PASS); await page.click('#pcGate button[type=submit]');
  assert.strictEqual(await late, 200, 'a call refused for a changed passcode waits for the new one and goes once more');
  console.log('station on its own: asked once, wrong passcode refused, reload asks nothing, a changed passcode asked again');
  await ctx.close();

  // ── the station the sorter frames: never asks; the sorter hands its passcode over ──
  ctx = await context(({ sorter }) => { if (location.origin === sorter) { sessionStorage.setItem('cn.passcode', 'pc-new'); localStorage.setItem('cn.employee', 'Tester'); } });
  page = await ctx.newPage(); watch(page, 'sorter');
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  await page.evaluate(station => { Object.assign(CN.S.settings, { dsOrigin: station, sandbox: 'on', sandboxStream: 'off', pollOrders: 'off' }); CN.saveSettings(); }, stationOrigin);
  await page.reload(); await page.waitForFunction(() => window.CN && window.DesignLink && CN.S.cloud.ok, null, { timeout: 60000 });
  await page.evaluate(() => CN.setMode('design')); await page.evaluate(() => DesignLink.ensure());
  const frame = page.frames().find(f => /design-1\.html/.test(f.url()));
  assert(frame, 'the sorter framed the station');
  await frame.waitForFunction(() => { try { return sessionStorage.getItem('designStation.passcode') === 'pc-new'; } catch (_) { return false; } }, null, { timeout: 30000 });
  const inFrame = await frame.evaluate(async () => ({ box: !!document.getElementById('pcGate'), emu: await fetch(`${FN}/etsySandbox?fn=status`).then(r => r.status), arch: await fetch(`${FN}/designArchive?op=have&ids=x&sandbox=1`).then(r => r.status) }));
  assert(!inFrame.box && inFrame.emu === 200 && inFrame.arch === 200, 'the framed station never asks, and its calls carry the sorter\'s passcode: ' + JSON.stringify(inFrame));
  const logged = JSON.stringify(st.list('Design_Bridge').concat(st.list('Sandbox_Design_Bridge')));
  assert(!logged.includes('pc-new'), 'the passcode is never written to the bridge log');
  console.log('framed station: handed the passcode by the sorter, never asked');
  await ctx.close();

  // ── the older station (design.html) asks the same way for its archive ──
  ctx = await context(); page = await ctx.newPage(); watch(page, 'design.html');
  await page.goto(`${stationOrigin}/design.html`);
  await page.waitForSelector('#pcGate .pcInput', { timeout: 30000 });
  await page.fill('#pcGate .pcInput', PASS); await page.click('#pcGate button[type=submit]');
  await page.waitForFunction(() => !document.getElementById('pcGate'), null, { timeout: 10000 });
  assert.strictEqual(await page.evaluate(() => fetch(`${ARCHIVE_FN}?op=have&ids=x`).then(r => r.status)), 200, 'design.html: its archive calls carry the passcode');
  console.log('design.html: asked once, archive calls carry it');
  await ctx.close();

  delete process.env.EDIT_PASSCODE;
  const fatal = errors.filter(e => !/firebase stub|Failed to fetch|NetworkError|net::ERR/.test(e));
  assert.deepStrictEqual(fatal, [], 'no page errors');
  console.log('station passcode OK');
  await browser.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
