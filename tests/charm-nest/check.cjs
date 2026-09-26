// The connections check page (charm-nest-check.html) against the two-origin stand-in: every group runs, the report is
// produced, and nothing fails. The rows that need live services are answered by the in-memory server, so a red row here
// is a bug in the page or the check function, not in the environment.
//   node tests/charm-nest/check.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

const day = Math.floor(Date.now() / 1000);
const receipts = [1, 2].map(n => ({ receipt_id: 3521000000 + n, order_number: 3521000000 + n, name: 'B' + n, update_timestamp: day, transactions: [{ transaction_id: Number(`${3521000000 + n}1`), listing_id: 5, receipt_id: 3521000000 + n, sku: 'BR-TST-0' + n, title: 'x', quantity: 1, expected_ship_date: day + 86400, variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold Filled' }] }] }));

(async () => {
  const srv = await start({ receipts });
  const { st, sorterOrigin, stationOrigin } = srv;
  st.corsOrigins = [sorterOrigin, stationOrigin];
  process.env.EDIT_PASSCODE = 'check-secret';
  Object.assign(process.env, { FIREBASE_PROJECT_ID: 'test-project', FIREBASE_CLIENT_EMAIL: 'svc@test-project.iam.gserviceaccount.com', FIREBASE_PRIVATE_KEY: '-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n', CLIENT_ID: 'etsy-test-client-id-0000', ETSY_SHARED_SECRET: 'etsy-secret', SHOP_ID: '12345' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1300, height: 1000 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
  await ctx.addInitScript(({ station }) => { if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); } }, { station: stationOrigin });
  const page = await ctx.newPage(); const errors = [];
  page.on('pageerror', e => errors.push(e.message)); page.on('console', m => { if (m.type() === 'error') errors.push(m.text().slice(0, 200)); });

  // 1 · read-only, without the passcode: the gate rows say so and the server half is skipped, nothing crashes
  await page.goto(`${sorterOrigin}/charm-nest-check.html?station=${encodeURIComponent(stationOrigin)}`);
  await page.waitForFunction(() => window.__checkDone === true, null, { timeout: 120000 });
  let rows = await page.evaluate(() => CheckPage.rows.map(r => ({ g: r.group, n: r.name, s: r.status, v: r.value })));
  const find = (g, n) => rows.find(r => r.g === g && r.n === n);
  assert.strictEqual(find('Cloud from this page', 'charmNestLibrary · ping').s, 'fail', 'without the passcode the ping is refused');
  assert.strictEqual(find('Server', 'charmNestCheck').s, 'skip', 'the server half is skipped when the ping fails');
  assert.strictEqual(find('This page', 'geometry self-test').s, 'ok', 'the geometry self-test passes: ' + JSON.stringify(find('This page', 'geometry self-test')));
  assert.strictEqual(find('Design Station', 'hello').s, 'ok', 'the station answers hello from the check page');
  // the refused ping asks for the passcode in the bar; it is kept for this tab's session, never read from the address
  await page.waitForSelector('#pcForm:not(.hidden) #pcIn', { timeout: 10000 });
  await page.fill('#pcIn', 'check-secret');
  await Promise.all([page.waitForNavigation(), page.click('#pcUse')]);
  assert.strictEqual(await page.evaluate(() => sessionStorage.getItem('cn.passcode')), 'check-secret', 'the passcode typed in the bar is kept for this tab');

  // 2 · everything on, with the passcode: every group runs and no row fails
  await page.goto(`${sorterOrigin}/charm-nest-check.html?station=${encodeURIComponent(stationOrigin)}&passcode=ignored&write=1&etsy=1&ai=1`);
  assert(!/passcode=/.test(page.url()), 'a passcode in the address is dropped from it, and never used');
  await page.waitForFunction(() => window.__checkDone === true, null, { timeout: 180000 });
  rows = await page.evaluate(() => CheckPage.rows.map(r => ({ g: r.group, n: r.name, s: r.status, v: r.value, note: r.note })));
  const groups = [...new Set(rows.map(r => r.g))];
  console.log('groups', groups.join(' · '));
  console.log('summary', { ok: rows.filter(r => r.s === 'ok').length, warn: rows.filter(r => r.s === 'warn').length, fail: rows.filter(r => r.s === 'fail').length, skip: rows.filter(r => r.s === 'skip').length, total: rows.length });
  for (const r of rows.filter(r => r.s !== 'ok')) console.log(' ', r.s, r.g, '·', r.n, '·', r.v.slice(0, 160), r.note ? '— ' + r.note.slice(0, 100) : '');
  for (const g of ['This page', 'Cloud from this page', 'Credentials', 'Firestore', 'Firestore queries', 'Storage', 'Station page', 'Sorter assets', 'Functions deployed', 'Claude', 'Design Station', 'Claude through the sorter']) assert(groups.includes(g), 'group ran: ' + g);
  const fails = rows.filter(r => r.s === 'fail');
  assert.strictEqual(fails.length, 0, 'no failed rows: ' + JSON.stringify(fails));
  assert.strictEqual(rows.filter(r => r.s === 'skip' && !/security rules/.test(r.n)).length, 0, 'with every option on, nothing is skipped but the rules note');
  assert(find('Storage', 'write · read · signed URL · delete').s === 'ok' && find('Cloud from this page', 'Storage · signed PUT → finalize → CORS GET').s === 'ok', 'both storage routes round-trip');
  assert(find('Station page', 'frame-ancestors').s === 'ok' && find('Station page', 'Cross-Origin-Resource-Policy').s === 'ok', 'the station headers are read from the outside');
  assert(find('Design Station', 'orders.snapshot (Etsy)').s === 'ok' && find('Design Station', 'orders.check (list reuse)').s === 'ok', 'the Etsy rows ran through the station');
  assert(find('Design Station', 'claim → read back → unclaim').s === 'ok', 'the claim probe round-tripped');
  assert(find('Design Station', 'complete.commit without a preview').s === 'ok' && find('Design Station', 'complete.preview of an unselected order').s === 'ok', 'the guards refuse');
  assert(rows.filter(r => r.g === 'Firestore queries' && r.s === 'ok').length >= 12, 'the library queries all answered');
  assert(rows.filter(r => r.g === 'Sorter assets' && r.s === 'ok').length === rows.filter(r => r.g === 'Sorter assets').length, 'every asset is served');
  assert(!st.blobs.has([...st.blobs.keys()].find(k => /charmnest\/diag\/probe-/.test(k)) || ''), 'the server probe object was deleted again');
  const text = await page.evaluate(() => CheckPage.report());
  assert(/reached · 0 failed/.test(text) && /Design Station\n/.test(text), 'the copyable report is produced');
  await page.click('#bCopy'); await page.waitForTimeout(200);
  assert(await page.evaluate(() => document.getElementById('report').style.display === 'block'), 'the report unfolds on Copy');
  await page.screenshot({ path: path.join(require('os').tmpdir(), 'cn-check.png'), fullPage: true });
  const fatal = errors.filter(e => !/favicon|net::ERR|Failed to load resource|401|404/.test(e));
  assert.strictEqual(fatal.length, 0, 'no page errors: ' + fatal.join(' | '));
  console.log('check page OK ·', rows.length, 'rows · screenshot', path.join(require('os').tmpdir(), 'cn-check.png'));
  await browser.close(); srv.close();
})().catch(e => { console.error(e); process.exit(1); });
