// FC9: what the sandbox order stream asks the server per check, counted in a real Chromium (Sorter + framed Design Station) over
// the bridge test server: the calls of every kind while a 120-order snapshot replays at 1000x (Manual mode), and over 20 s once
// every order has come ("done"). Run it on two trees to compare; nothing leaves the machine.
//   node tests/cost/fc9-station-sweeps.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require(path.join(root, 'tests/charm-nest/bridge-server.cjs'));

const day = Math.floor(Date.now() / 1000), N = 120;
const GF = '14k Gold Filled';
const tx = (rid, i, sku, created, ship) => ({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, create_timestamp: created, created_timestamp: created, paid_timestamp: created, expected_ship_date: ship, shipped_timestamp: null, variations: [{ formatted_name: 'Metal', formatted_value: GF }], is_personalized: false });
const receipt = (rid, hoursAgo, leadDays) => { const created = day - hoursAgo * 3600, ship = created + leadDays * 86400; return { receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', create_timestamp: created, created_timestamp: created, update_timestamp: created + 60, updated_timestamp: created + 60, status: 'Paid', is_paid: true, is_shipped: false, transactions: [tx(rid, 1, 'BR-TST-0' + (1 + rid % 5), created, ship)] }; };
const snapshot = Array.from({ length: N }, (_, i) => receipt(3521000101 + i, 400 - 3 * i, 2 + i % 6));

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
  await ctx.addInitScript(({ station, sorter }) => {
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
    if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester');
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
  }, { station: stationOrigin, sorter: sorterOrigin });
  const page = await ctx.newPage(); const errors = [];
  page.on('pageerror', e => errors.push('sorter: ' + e.message));
  const settle = extra => page.evaluate(([station, extra]) => { const s = CN.S.settings; Object.assign(s, { dsOrigin: station, engine: 'solver', budgetS: 12, review: 'off', naming: 'off', packingAI: 'off', notify: 'off', sound: 'off', autoCommit: 'on', runMode: 'manual', pullMode: 'all', heartbeatS: 2, heartbeatMiss: 2 }, extra || {}); CN.saveSettings(); }, [stationOrigin, extra]);
  const booted = () => page.waitForFunction(() => window.CN && window.Sandbox && window.Arrivals && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  const stream = () => st.doc('Charm_Sandbox', 'stream') || null;
  const kind = c => {
    if (c.name === 'etsySandbox') return 'etsySandbox ' + c.q.fn;
    if (c.name === 'firebaseOrders') { const q = c.q; for (const k of ['dcFor', 'staffNotesFor', 'dcSince', 'rtSince', 'rtFor', 'messagesFor', 'orderId']) if (q[k] != null) return 'firebaseOrders ' + k; if (q.rt === '1') return 'firebaseOrders rt=1'; return 'firebaseOrders ' + (c.body && c.body.rtClaimIds ? 'POST claim' : 'other'); }
    if (c.name === 'charmNestLibrary') return 'charmNestLibrary ' + (c.op || c.body.op) + (c.body.action ? ':' + c.body.action : '');
    return c.name;
  };
  const tally = from => { const t = {}; for (const c of st.calls.slice(from)) { const k = kind(c); t[k] = (t[k] || 0) + 1; } return t; };
  const show = (label, t) => { console.log(label); for (const [k, n] of Object.entries(t).sort((a, b) => b[1] - a[1])) if (n > 2 || /dcFor|staffNotes|rt|dcSince|sandboxStream|arrival|etsySandbox|optionMap|aliasGet|noDesign|masterListFiles|cancelList/.test(k)) console.log('   ' + String(n).padStart(5) + '  ' + k); };

  await page.goto(`${sorterOrigin}/charm-nest-1.html`); await booted(); await settle();
  const snapPath = 'charmnest/sandbox/orders-fc9.json', snapAt = Date.now();
  st.blobs.set(snapPath, { buf: Buffer.from(JSON.stringify({ at: snapAt, count: snapshot.length, receipts: snapshot })), generation: 1, meta: { contentType: 'application/json', metadata: {} } });
  st.put('Charm_Sandbox', 'current', { path: snapPath, count: snapshot.length, at: snapAt, takenBy: 'test', source: 'etsy-pull' });
  await settle({ sandbox: 'on', sandboxStream: 'on', sandboxSpeed: 1000, sandboxSeed: 3 });
  await page.evaluate(() => sessionStorage.setItem('cn.sandboxAutoPull', '1'));
  const c0 = st.calls.length, t0 = Date.now(); await page.reload(); await booted();
  await page.waitForFunction(() => Sandbox.stream() && SimClock.on() && CN.S.mode === 'orders', null, { timeout: 30000 });
  // the replay: until every order has come
  for (; Date.now() - t0 < 240000;) { const s = stream(); if (s && s.done) break; await page.waitForTimeout(500); }
  const s1 = stream(), tReplay = Date.now() - t0, c1 = st.calls.length;
  console.log(`replay at 1000x: ${s1.tick} steps, ${s1.brought} of ${s1.total} orders in ${(tReplay / 1000).toFixed(1)} s (${(tReplay / s1.tick / 1000).toFixed(2)} s per step)`);
  const rep = tally(c0); show('calls during the replay:', rep);
  const sweeps = rep['etsySandbox listOpenOrders'] || 0;
  // done: 20 s
  await page.waitForTimeout(2000); const c2 = st.calls.length; await page.waitForTimeout(20000);
  const quiet = tally(c2); show('calls in 20 s once every order has come:', quiet);
  const rows = await page.evaluate(() => new Set(B.orders.rows.map(r => String(r.order.receiptId))).size);
  console.log(`orders in the sorter: ${rows} of ${N}; list sweeps in the 20 s: ${quiet['etsySandbox listOpenOrders'] || 0}; errors: ${errors.length}`);
  assert.strictEqual(rows, N, 'every order reached the sorter');
  assert.strictEqual(errors.filter(e => !/favicon|net::ERR|404/.test(e)).length, 0, 'no page errors: ' + errors.join(' | '));
  await browser.close(); srv.close();
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
