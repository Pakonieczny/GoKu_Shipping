// Two tabs of one browser share the sandbox's wait-for-Start mark (ADV-SANDBOX, 15 Oct 2026, Paul's "two benches/tabs at once").
// A Start deletes the old set in the first moment of its pull and writes the new one seconds later. A second tab that was waiting too
// saw the mark go, checked, found "no pulled orders" and put the WHOLE sandbox back to waiting (dirty) while the first tab's pull was
// still out: the finished set sat in the cloud, the first tab said "paused", and Start had to clear it and pull again (3 more Etsy calls).
// The real page in real headless Chromium, two tabs of ONE context (they share localStorage), against the repo's fake server and a FAKE
// Etsy (no network, no live record). It proves:
//   1 · tab A presses Start while tab B waits idle: the wait-for-Start mark never comes back, ONE pull and no clean-up reach the
//       cloud, both tabs end running (B takes the stream at its next check), the pull's mark is gone;
//   2 · a pull mark older than 45 s (its tab died) is no excuse: a page that finds no set then waits for Start, dirty, as before;
//   3 · a fresh pull mark (a Start out in another tab): a page that finds no set does not call the sandbox empty;
//   4 · the answer of a Start's pull is lost and the internet stays away a few seconds: the page asks the very same start again
//       (it used to give up after 3 s over a finished set): one pull, 3 Etsy calls, the sandbox runs.
//   node tests/charm-nest/sandbox-two-tabs-start.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const { start } = require('./bridge-server.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const day = Math.floor(Date.now() / 1000);
const SKUS = ['BR-TST-01', 'BR-TST-02', 'BR-TST-03', 'BR-TST-04', 'BR-TST-05', 'BR-TST-06'];
const tx = (rid, i, sku, metal, created, ship) => ({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, create_timestamp: created, created_timestamp: created, paid_timestamp: created, expected_ship_date: ship, variations: [{ property_id: 513, value_id: 1, formatted_name: 'Metal', formatted_value: metal }, { property_id: 54, value_id: 2, formatted_name: 'Personalization', formatted_value: 'ANNA' }] });
const SHOP = Array.from({ length: 60 }, (_, k) => { const i = 59 - k, rid = 3700000001 + i, created = day - (60 - i) * 7200, ship = created + 3 * 86400;
  return { receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, status: 'Paid', is_paid: true, is_shipped: false, created_timestamp: created, create_timestamp: created, expected_ship_date: ship, transactions: [tx(rid, 1, SKUS[i % 6], i % 2 ? 'Sterling Silver' : '14k Gold Filled', created, ship)] }; });
(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  // a slow-ish Etsy (2.5 s a page, all pages at once): the pull is out long enough for the other tab to check meanwhile
  const etsy = { calls: [], delay: 2500 };
  const fakeFetch = async url => { const u = new URL(url); etsy.calls.push({ offset: +u.searchParams.get('offset') }); await sleep(etsy.delay);
    const off = +u.searchParams.get('offset'), lim = +u.searchParams.get('limit'); return { ok: true, status: 200, headers: { get: () => null }, text: async () => '', json: async () => ({ count: SHOP.length, results: JSON.parse(JSON.stringify(SHOP.slice(off, off + lim))) }) }; };
  const Pull = require(path.join(root, 'netlify/functions/_charmNestSandboxPull.js')), realPull = Pull.pull;
  Pull.pull = (b, ctx) => realPull(b, ctx, { fetch: fakeFetch, token: async () => 'test-token', meter: { bump: () => ({ fromHttp() {}, failNet() {} }), flushNow: async () => {} } });
  const fdb = st.admin.firestore(); if (!fdb.doc) fdb.doc = p => { const [c, ...rest] = p.split('/'); return fdb.collection(c).doc(rest.join('/')); };
  st.put('config', 'etsyOauth', { access_token: 'a', refresh_token: 'r', expires_at_ms: Date.now() + 3600e3 });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1300, height: 900 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.addInitScript(({ station, sorter }) => {
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
    if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester');
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
  }, { station: stationOrigin, sorter: sorterOrigin });
  const booted = p => p.waitForFunction(() => window.CN && window.Sandbox && window.Arrivals && CN.S.cloud.ok !== null, null, { timeout: 90000 });
  const lib = name => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === name);
  const errors = [];
  const A = await ctx.newPage(); A.on('pageerror', e => errors.push('A: ' + e.message));
  await A.goto(`${sorterOrigin}/charm-nest-1.html`); await booted(A);
  // the sandbox switched on and waiting for Start, as after a Reset
  await A.evaluate(([station]) => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'on', sandboxStream: 'on', dsOrigin: station, engine: 'solver', review: 'off', naming: 'off', notify: 'off', sound: 'off', runMode: 'manual', heartbeatS: 2, heartbeatMiss: 2, v: 26 }); localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.sandboxHold', JSON.stringify({ at: Date.now() })); }, [stationOrigin]);
  await A.reload(); await booted(A);
  const B = await ctx.newPage(); B.on('pageerror', e => errors.push('B: ' + e.message));
  await B.goto(`${sorterOrigin}/charm-nest-1.html`); await booted(B);
  assert.deepStrictEqual([await A.evaluate(() => Sandbox.held()), await B.evaluate(() => Sandbox.held())], [true, true], 'both tabs wait for Start');
  // B's own check is due as soon as the mark goes (it has waited more than one interval)
  await B.waitForFunction(() => Arrivals.state().nextCheck < Date.now() - 500, null, { timeout: 60000 });

  // ══ 1 · A presses Start; B is an idle second tab ══
  const mark = st.calls.length;
  await A.evaluate(() => { CN.setMode('orders'); window.__start = Sandbox.start(); });
  await sleep(800);   // (the old set is gone, the new one is 2 s away) B's check, as its own timer would make it:
  await B.evaluate(() => Arrivals.check());
  const marks = [];
  for (const t = Date.now(); Date.now() - t < 60000;) {
    const a = await A.evaluate(() => ({ hold: localStorage.getItem('cn.sandboxHold'), pulling: Sandbox.pulling(), stream: !!Sandbox.stream(), pullMark: localStorage.getItem('cn.sandboxPulling') })).catch(() => null);
    if (a) marks.push(a);
    if (a && !a.pulling && a.stream) break;
    await sleep(250);
  }
  const bad = marks.filter(m => m.hold !== null);
  assert.strictEqual(bad.length, 0, 'the wait-for-Start mark never came back while the pull was out: ' + JSON.stringify(bad[0]));
  assert(marks.some(m => m.pulling === 'pull' && m.pullMark), 'the pull was out and marked, for the other tabs to see');
  const fin = marks[marks.length - 1]; assert(fin && !fin.pulling && fin.stream && fin.hold === null && fin.pullMark === null, 'A ends running, and the pull mark is gone: ' + JSON.stringify(fin));
  assert.strictEqual(await A.evaluate(() => Sandbox.held()), false, 'A is not waiting');
  assert.strictEqual(await B.evaluate(() => Sandbox.ready(true).then(s => !!s)), true, 'B takes the stream the pull began when it looks again (it was not left believing the sandbox empty)');
  assert.strictEqual(await B.evaluate(() => Sandbox.held()), false, 'B is not waiting either, and plays the same set');
  const run = st.calls.slice(mark).filter(c => c.name === 'charmNestLibrary').map(c => c.op);
  assert.strictEqual(lib('sandboxPullOrders').length, 1, 'ONE pull reached the cloud: ' + run.join(' '));
  assert.strictEqual(lib('sandboxReset').length, 0, 'and no clean-up'); assert.strictEqual(etsy.calls.length, 3, 'three Etsy calls in all');
  const cur = st.doc('Charm_Sandbox', 'current'), stm = st.doc('Charm_Sandbox', 'stream');
  assert(cur && cur.source === 'etsy-pull' && stm && stm.on && stm.snapshotPath === cur.path, 'the cloud holds the pulled set and its stream');
  assert.strictEqual(await B.evaluate(() => Sandbox.stream().startedAt), stm.startedAt, 'B plays the very stream A started');
  console.log('1 ok · two tabs, one Start: ' + marks.length + ' looks at the wait mark, none set; pulls 1, clean-ups 0, Etsy calls 3');

  // ══ 2 · a pull mark that is old (its tab died), and the cloud holds no set: a page that opens waits for Start, as before ══
  await B.close();
  st.docs.delete('Charm_Sandbox/current'); st.docs.delete('Charm_Sandbox/stream');
  await A.evaluate(() => { localStorage.setItem('cn.sandboxPulling', String(Date.now() - 60000)); });
  await A.reload(); await booted(A);
  await A.waitForFunction(() => Sandbox.held(), null, { timeout: 30000 });
  assert.strictEqual(await A.evaluate(() => JSON.parse(localStorage.getItem('cn.sandboxHold') || '{}').dirty), true, 'an old mark is no excuse: no set means waiting for Start (dirty)');
  console.log('2 ok · a mark 60 s old does not hide an empty sandbox');

  // ══ 3 · a fresh pull mark (a Start is out in another tab) and no set yet: this page does not call the sandbox empty ══
  await A.evaluate(() => { localStorage.removeItem('cn.sandboxHold'); localStorage.setItem('cn.sandboxPulling', String(Date.now() - 2000)); });
  await A.reload(); await booted(A);
  await sleep(5000);
  const c3 = await A.evaluate(() => ({ held: Sandbox.held(), hold: localStorage.getItem('cn.sandboxHold'), stream: !!Sandbox.stream() }));
  assert(!c3.held && c3.hold === null && !c3.stream, 'a Start is out elsewhere: the page waits for its set instead of holding the sandbox: ' + JSON.stringify(c3));
  console.log('3 ok · a fresh mark: no false "empty"');

  // ══ 4 · the internet goes away in the middle of Start: the server finishes the pull, its answer is lost, and the page asks the very same start again ══
  // (it used to ask once more after 3 s and give up: the page then said "empty" over a finished set in the cloud, and Start pulled all over again, 3 more Etsy calls)
  await A.evaluate(() => { localStorage.removeItem('cn.sandboxPulling'); localStorage.setItem('cn.sandboxHold', JSON.stringify({ at: Date.now() })); });
  await A.reload(); await booted(A);
  const pulls4 = lib('sandboxPullOrders').length, etsy4 = etsy.calls.length; let asked = 0;
  await ctx.route(/charmNestLibrary/, async route => {
    let op = ''; try { op = JSON.parse(route.request().postData() || '{}').op; } catch (_) {}
    if (op !== 'sandboxPullOrders') return route.continue();
    asked++;
    if (asked === 1) { await route.fetch().catch(() => null); return route.abort('connectionreset'); }   // the cloud did the pull; the answer never arrives
    if (asked === 2) return route.abort('internetdisconnected');                                           // still away
    return route.continue();
  });
  await A.evaluate(() => { CN.setMode('orders'); window.__start = Sandbox.start(); });
  await A.waitForFunction(() => Sandbox.pulling() === 'pull', null, { timeout: 30000 });
  await sleep(6000);   // (the answer was lost 2.5 s in, the next ask fails at once, the one after waits 5 s)
  const mid = await A.evaluate(() => ({ held: Sandbox.held(), pulling: Sandbox.pulling() }));
  assert(!mid.held && mid.pulling === 'pull', 'the page is still on its Start, not given up: ' + JSON.stringify(mid));
  await A.waitForFunction(() => !Sandbox.pulling() && !!Sandbox.stream(), null, { timeout: 60000 });
  assert.deepStrictEqual(await A.evaluate(() => [Sandbox.held(), localStorage.getItem('cn.sandboxHold')]), [false, null], 'A runs: the sandbox is not waiting');
  assert.strictEqual(asked, 3, 'the same start was asked three times (the answer lost, away, back)');
  assert.strictEqual(lib('sandboxPullOrders').length - pulls4, 2, 'two of them reached the cloud');
  assert.strictEqual(etsy.calls.length - etsy4, 3, 'and the pull was made ONCE: three Etsy calls, the second ask was answered with the stored set');
  assert.strictEqual(new Set(lib('sandboxPullOrders').slice(pulls4).map(c => c.body.startId)).size, 1, 'under one start id');
  await ctx.unroute(/charmNestLibrary/);
  console.log('4 ok · the answer lost, the internet away for a few seconds: one pull, asked again, taken');
  assert.deepStrictEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors');
  console.log('sandbox two tabs start OK');
  await browser.close(); srv.close && srv.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
