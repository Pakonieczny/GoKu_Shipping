// SBX1000 (Paul, 16 Oct 2026: "consider shipped orders as open. I need to be able to pull/test up-to 1000 of the most recent orders from etsy."). The PAGE side of 1000 orders, in real headless
// Chromium against the repo's fake server: the REAL charmNestLibrary, etsySandbox and sandboxPullOrders code over an in-memory Firestore/Storage, and a FAKE Etsy of 1000 realistic receipts (4-8 KB each,
// some shipped, cancelled and unpaid; every request recorded; no network, no live record). With the sandbox's number set to 1000 and "the pulled orders all at once" it proves, for a pull that
// brings all of them to the order list:
//   · Settings show the limits the server sent (the number box max 1000, the value 1000, "10 to 1000" in its help, "About 10 Etsy calls per pull.");
//   · Start pulls once (10 Etsy calls, offsets 0 to 900, started 110 ms apart), the toast says "<played> of 1000 newest Etsy orders play (shipped ones count as open) · N cancelled or unpaid left out ·
//     10 Etsy calls", the status line says "1000 newest Etsy orders, pulled just now (N cancelled or unpaid left out) with 10 Etsy calls", with no "(N open)";
//   · every receipt that plays reaches the sorter as an order (a row; the shipped ones too, none of them gone; no cancelled or unpaid one), and the Design Station's fresh read of a shipped one says
//     not gone, not shipped;
//   · how long the whole intake takes and whether the page stays responsive (the longest stretch the page did not answer), printed.
//   node tests/charm-nest/sandbox-pull-1000-page.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const { start, unlockSettings } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');

const sleep = ms => new Promise(r => setTimeout(r, ms));
const DAY = 86400, now = Math.floor(Date.now() / 1000), N = 1000;
const GF = '14k Gold Filled', SS = 'Sterling Silver', SKUS = ['BR-TST-01', 'BR-TST-02', 'BR-TST-03', 'BR-TST-04', 'BR-TST-05', 'BR-TST-06'];
const LOREM = 'Handmade personalized initial charm, stamped and polished in our studio from solid 14k gold filled or sterling silver. Choose a letter, a birthstone or a zodiac sign, then a cable chain. ';
const tx = (rid, i, j, created, shipped) => { const sku = SKUS[(i + j) % 6], lead = 2 + (i % 6); return { transaction_id: Number(`${rid}${j + 1}`), listing_id: 1718000 + ((i + j) % 6), receipt_id: rid, sku, title: `${sku} charm`, description: LOREM.repeat(1 + ((i + j) % 4)), quantity: 1, create_timestamp: created, created_timestamp: created,
  paid_timestamp: created, shipped_timestamp: shipped ? created + 3 * DAY : null, expected_ship_date: created + lead * DAY, price: { amount: 2495, divisor: 100, currency_code: 'USD' }, variations: [{ formatted_name: 'Metal', formatted_value: (i + j) % 2 ? SS : GF }], is_personalized: false }; };
function receipt(i) {   // i = 0 is the oldest
  const rid = 3900000001 + i, created = now - (N - i) * 1500, kind = i % 11 === 0 ? 'cancel' : i % 29 === 0 ? 'unpaid' : i % 3 === 0 ? 'shipped' : 'open', shipped = kind === 'shipped';
  const r = { receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', state: 'TX', zip: '78701', first_line: `${100 + i} Maple Street`, buyer_email: `buyer${i}@example.com`, message_from_buyer: '', create_timestamp: created, created_timestamp: created, update_timestamp: created + 60, updated_timestamp: created + 60,
    status: kind === 'cancel' ? 'Canceled' : shipped ? 'Completed' : kind === 'unpaid' ? 'Payment Processing' : 'Paid', is_paid: kind !== 'unpaid', is_shipped: shipped, shipments: shipped ? [{ receipt_shipping_id: 7000 + i, carrier_name: 'usps', tracking_code: '9400' + rid, shipment_notification_timestamp: created + 3 * DAY }] : [],
    transactions: Array.from({ length: 1 + (i % 2) }, (_, j) => tx(rid, i, j, created, shipped)) };
  if (kind === 'cancel') r.is_canceled = true;
  const base = Buffer.byteLength(JSON.stringify(r)), target = 4096 + (i * 997) % 3800;
  if (base < target) r.message_from_seller = 'Thank you for your order, it will be packed with care. '.repeat(Math.ceil((target - base) / 53)).slice(0, target - base);
  return r;
}
const SHOP = Array.from({ length: N }, (_, k) => receipt(N - 1 - k));   // newest first, as Etsy lists them
const plays = r => !r.is_canceled && r.is_paid !== false;
const PLAY_IDS = SHOP.filter(plays).map(r => String(r.receipt_id)), SHIPPED_IDS = SHOP.filter(r => plays(r) && r.is_shipped).map(r => String(r.receipt_id)), LEFT_OUT = N - PLAY_IDS.length;
const bytesOf = SHOP.map(r => Buffer.byteLength(JSON.stringify(r)));

(async () => {
  assert(bytesOf.every(b => b >= 4096 && b <= 8192) && SHIPPED_IDS.length > 150 && LEFT_OUT > 60, 'the fake shop: 4-8 KB receipts, shipped, cancelled and unpaid ones');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-pull1000-'));
  const masterPath = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(masterPath, { count: 6, edge: false });
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  const etsy = { calls: [], delay: 350 };
  const fakeFetch = async url => {
    const u = new URL(url), off = +u.searchParams.get('offset'), lim = +u.searchParams.get('limit'); etsy.calls.push({ path: u.pathname, offset: off, limit: lim, at: Date.now() });
    await sleep(etsy.delay);
    return { ok: true, status: 200, headers: { get: () => null }, text: async () => '', json: async () => ({ count: SHOP.length, results: JSON.parse(JSON.stringify(SHOP.slice(off, off + lim))) }) };
  };
  const Pull = require(path.join(root, 'netlify/functions/_charmNestSandboxPull.js')), realPull = Pull.pull, answers = [];
  Pull.pull = async (b, ctx) => { const t = Date.now(), r = await realPull(b, ctx, { fetch: fakeFetch, token: async () => 'test-token', meter: { bump: () => ({ fromHttp() {}, failNet() {} }), flushNow: async () => {} } }); answers.push({ ms: Date.now() - t, r }); return r; };
  const fdb = st.admin.firestore(); if (!fdb.doc) fdb.doc = p => { const [c, ...rest] = p.split('/'); return fdb.collection(c).doc(rest.join('/')); };
  st.put('config', 'etsyOauth', { access_token: 'a', refresh_token: 'r', expires_at_ms: Date.now() + 3600e3 });
  st.put('config', 'charmNestSandboxPull', { v: 1, limit: 1000, by: 'Paul', at: Date.now() });   // (Paul's number, as he saves it in Settings)

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
    // how long the page did not answer once Start is pressed: the longest gap between two animation frames and the longest task, kept in this tab's sessionStorage (Start clears the old sandbox and reloads the page once)
    if (location.origin === sorter) { let last = 0; const ON = '__gapOn', put = (k, v) => { try { if (v > (+sessionStorage.getItem(k) || 0)) sessionStorage.setItem(k, String(Math.round(v))); } catch (_) {} }, on = () => { try { return sessionStorage.getItem(ON) === '1'; } catch (_) { return false; } };
      const tick = t => { if (last && on()) put('__gapmax', t - last); last = t; requestAnimationFrame(tick); }; requestAnimationFrame(tick);
      try { new PerformanceObserver(l => { if (on()) for (const e of l.getEntries()) put('__taskmax', e.duration); }).observe({ entryTypes: ['longtask'] }); } catch (_) {} }
  }, { station: stationOrigin, sorter: sorterOrigin });
  const page = await ctx.newPage(); const errors = [];
  page.on('pageerror', e => errors.push('sorter: ' + e.message)); page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text().slice(0, 200)); });
  const settle = extra => page.evaluate(([station, extra]) => { const s = CN.S.settings; Object.assign(s, { dsOrigin: station, engine: 'solver', budgetS: 12, review: 'off', naming: 'off', packingAI: 'off', notify: 'off', sound: 'off', autoCommit: 'on', runMode: 'manual', pullMode: 'all', heartbeatS: 2, heartbeatMiss: 2 }, extra || {}); CN.saveSettings(); }, [stationOrigin, extra]);
  const booted = () => page.waitForFunction(() => window.CN && window.Sandbox && window.Arrivals && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  const ev = async (fn, arg) => { try { return await page.evaluate(fn, arg); } catch (_) { return undefined; } };
  const until = async (fn, ms, why, arg) => { for (const t = Date.now(); Date.now() - t < ms;) { const v = await ev(fn, arg); if (v) return v; await sleep(150); } throw new Error('timed out: ' + why); };

  // ── production: the master is indexed; then the sandbox is switched on with "the pulled orders all at once" ──
  await page.goto(`${sorterOrigin}/charm-nest-1.html`); await booted(); await settle();
  await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mFile', { state: 'attached' }); await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
  await settle({ sandbox: 'on', sandboxStream: 'off' });
  await page.reload(); await booted();
  await until(() => Sandbox.held() && !Sandbox.pulling(), 60000, 'the sandbox waits for Start');

  // ── Settings show the limits the server sent ──
  await page.evaluate(() => { openSettings(); }); await unlockSettings(page); const set0 = await page.evaluate(() => new Promise(res => { setTimeout(() => { const box = document.getElementById('stSbPull'), f = box.closest('.field'); const t = { max: box.max, min: box.min, step: box.step, value: box.value, note: document.getElementById('stSbPullNote').textContent, help: f.innerText }; closeDlg(document.getElementById('dlgSettings')); res(t); }, 1200); }));
  assert(set0.max === '1000' && set0.min === '10' && set0.value === '1000' && /10 to 1000/.test(set0.help) && !/10 to 500|up to 500/.test(set0.help) && /About 10 Etsy calls per pull\./.test(set0.note), 'Settings show the number box with the server\'s limits: ' + JSON.stringify(set0));
  // typing in the box: the cost of each number, 1001 held to 1000 (the server holds it again), a stored 500 still plain, a non-number asked for again
  await page.evaluate(() => { openSettings(); }); await unlockSettings(page); const typed = await page.evaluate(() => new Promise(res => { setTimeout(() => {
    const box = document.getElementById('stSbPull'), note = document.getElementById('stSbPullNote'), save = document.getElementById('stSbPullSave'), out = {};
    const type = v => { box.value = v; box.dispatchEvent(new Event('input', { bubbles: true })); return { note: note.textContent, save: !save.disabled }; };
    out.t500 = type('500'); out.t1000 = type('1000'); out.t1001 = type('1001'); out.t5000 = type('5000'); out.t9 = type('9'); out.junk = type('abc'); type('1000'); closeDlg(document.getElementById('dlgSettings')); res(out); }, 1200); }));
  assert(typed.t500.note === 'About 5 Etsy calls per pull.' && typed.t500.save === true, '500 is a plain number: ' + JSON.stringify(typed.t500));
  assert(typed.t1000.note === 'About 10 Etsy calls per pull.' && typed.t1000.save === false, '1000 is the number now set, nothing to save: ' + JSON.stringify(typed.t1000));
  assert(typed.t1001.note === 'Held to 1000: the most one pull may bring. About 10 Etsy calls per pull.' && typed.t1001.save === true && typed.t5000.note === typed.t1001.note, '1001 and 5000 are held to 1000: ' + JSON.stringify([typed.t1001, typed.t5000]));
  assert(typed.t9.note === 'Held to 10: the fewest one pull may bring. About 1 Etsy call per pull.' && typed.junk.note === 'Type a whole number from 10 to 1000.' && typed.junk.save === false, 'the small end and a non-number: ' + JSON.stringify([typed.t9, typed.junk]));
  const wait0 = await page.evaluate(() => Sandbox.heldText()); assert(/pull the 1000 newest Etsy orders/.test(wait0), 'the empty sandbox says what Start does: ' + wait0);

  // ── Start: one pull of ten calls, then the whole set into the order list ──
  const seen = { toasts: new Set(), bar: '' }, t0 = Date.now();
  await page.evaluate(() => { sessionStorage.removeItem('__gapmax'); sessionStorage.removeItem('__taskmax'); sessionStorage.setItem('__gapOn', '1'); CN.setMode('orders'); Orders.render(); });
  await page.click('[data-sb-start]');
  let pullDone = 0, rowsDone = 0;
  for (; Date.now() - t0 < 900000;) {
    const v = await ev(() => ({ pulled: !!(window.Sandbox && Sandbox.pulled && Sandbox.pulled()), pulling: Sandbox.pulling && Sandbox.pulling(), bar: (document.getElementById('cnpSlot') || {}).innerText || '', n: new Set(B.orders.rows.map(r => String(r.order.receiptId))).size, toasts: [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent) }));
    if (v) { v.toasts.forEach(x => seen.toasts.add(x)); if (/Pulling the 1000 newest orders/.test(v.bar)) seen.bar = v.bar; if (v.pulled && !pullDone) pullDone = Date.now() - t0; if (v.n >= PLAY_IDS.length) { rowsDone = Date.now() - t0; break; } }
    await sleep(250);
  }
  assert(rowsDone, 'all ' + PLAY_IDS.length + ' orders reached the sorter');
  const gap = await page.evaluate(() => { sessionStorage.setItem('__gapOn', '0'); return { frame: +sessionStorage.getItem('__gapmax') || 0, task: +sessionStorage.getItem('__taskmax') || 0 }; });
  const rows = await page.evaluate(() => B.orders.rows.map(r => ({ rid: String(r.order.receiptId), state: r.state })));
  const ids = new Set(rows.map(r => r.rid));
  console.log(`pull answered after ${pullDone} ms (server ${answers[0].ms} ms), ${ids.size} orders / ${rows.length} lines in the sorter after ${rowsDone} ms, longest gap between frames ${gap.frame} ms, longest task ${gap.task} ms`);

  assert.strictEqual(answers.length, 1, 'one start asked the op exactly once'); assert.strictEqual(etsy.calls.length, 10, 'ten Etsy calls');
  const asked = etsy.calls.slice().sort((a, b) => a.offset - b.offset); assert.deepStrictEqual(asked.map(c => [c.offset, c.limit]), Array.from({ length: 10 }, (_, i) => [i * 100, 100]), 'ten pages of 100, offsets 0 to 900'); assert(asked.every((c, i) => c.at - asked[0].at >= i * Pull.STAGGER_MS - 10) && asked[9].at - asked[0].at < 3000, 'page k starts k x ' + Pull.STAGGER_MS + ' ms after the first (timers may run late under load, never early): ' + asked.map(c => c.at - asked[0].at).join(','));
  const cur = st.doc('Charm_Sandbox', 'current'); assert(cur.source === 'etsy-pull' && cur.count === 1000 && cur.want === 1000 && cur.open === PLAY_IDS.length && cur.closed === LEFT_OUT && cur.shipped === SHIPPED_IDS.length && cur.calls === 10 && !st.doc('Charm_Sandbox', 'stream'), 'the stored pointer: ' + JSON.stringify({ ...cur, updatedAt: undefined }));
  assert(seen.bar, 'a labelled spinner says "Pulling the 1000 newest orders"');
  const toast = [...seen.toasts].find(t => /newest Etsy orders play/.test(t));
  assert(toast && toast.includes(`${PLAY_IDS.length} of 1000 newest Etsy orders play (shipped ones count as open) · ${LEFT_OUT} cancelled or unpaid left out · 10 Etsy calls`) && !/are open|\(\d+ open\)/.test(toast), 'the toast: ' + [...seen.toasts].join(' | '));
  assert(ids.size === PLAY_IDS.length && PLAY_IDS.every(id => ids.has(id)), 'every receipt that plays is an order in the sorter (' + ids.size + ' of ' + PLAY_IDS.length + ')');
  assert(SHIPPED_IDS.every(id => ids.has(id)) && rows.filter(r => SHIPPED_IDS.includes(r.rid)).every(r => r.state !== 'gone'), 'the ' + SHIPPED_IDS.length + ' that Etsy had shipped are there, none gone');
  assert(!SHOP.filter(r => !plays(r)).some(r => ids.has(String(r.receipt_id))), 'no cancelled or unpaid receipt is an order');
  const fresh = await page.evaluate(id => DesignLink.call('orders.detail', { receiptId: id, fresh: true }, { timeoutMs: 60000 }), SHIPPED_IDS[0]);
  assert(fresh.gone === false && fresh.isShipped === false && fresh.status === 'Paid' && fresh.order && fresh.order.lines.length >= 1, 'the station reads a shipped one fresh as an open order: ' + JSON.stringify({ gone: fresh.gone, isShipped: fresh.isShipped, status: fresh.status }));
  const line = await page.evaluate(() => Sandbox.streamText());
  assert(new RegExp(`^Orders: 1000 newest Etsy orders, pulled (just now|\\d+ \\w+, \\d\\d:\\d\\d) \\(${LEFT_OUT} cancelled or unpaid left out\\) with 10 Etsy calls, all at once$`).test(line) && !/\(\d+ open\)/.test(line), 'the status line: ' + line);
  await page.evaluate(() => { openSettings(); }); await unlockSettings(page); const set1 = await page.evaluate(() => new Promise(res => { setTimeout(() => { const t = document.getElementById('stSbStatus').textContent; closeDlg(document.getElementById('dlgSettings')); res(t); }, 500); }));
  assert(/1000 newest Etsy orders, pulled .+ with 10 Etsy calls/.test(set1), 'Settings name the pulled set: ' + set1);
  assert.deepStrictEqual(errors.filter(e => !/firebase stub|HTTP 4\d\d|HTTP 50\d|status of (409|429|50\d)|Failed to load resource/.test(e)), [], 'no page errors');
  console.log(`sandbox pull 1000 page OK: ${ids.size} of 1000 pulled orders in the sorter (${SHIPPED_IDS.length} shipped ones as open, ${LEFT_OUT} cancelled or unpaid left out), 10 Etsy calls, ${rowsDone} ms Start to the last order (the pull answered at ${pullDone} ms), longest gap between frames ${gap.frame} ms, longest task ${gap.task} ms`);
  await browser.close(); srv.close && srv.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
