// The sandbox's orders are the 250 newest Etsy orders, pulled once each time a sandbox run starts (Paul, 10 Oct 2026: "when in
// the Sandbox mode always pull the 250 newest orders from etsy. Never retain previous or old orders from prior Sandbox
// runs/testing"). This is the PAGE side, in real headless Chromium, against the repo's fake server: the REAL charmNestLibrary,
// etsySandbox and sandboxPullOrders code over an in-memory Firestore/Storage, and a FAKE Etsy (every request recorded; no
// network, no live record, no live op). It proves:
//   · switching Sandbox ON from the real orders clears the previous sandbox first (the one complete wipe), then asks the op
//     for the 250 newest exactly once (one startId, 3 Etsy calls), under a labelled spinner, and plays them through the
//     stream (oldest first); the old orders and the old browser copy never reach the sorter; production is untouched;
//   · a plain reload with the sandbox on makes no pull and no wipe, and the stream plays on;
//   · Reset followed by Start (pressed three ways at once: Start, Pull orders, Auto's pull) pulls once, with a NEW startId;
//   · a failed pull (the daily cap; Etsy down) leaves the sandbox EMPTY with the op's own line, costs exactly what the op
//     says, is never retried by the page (a reload neither), and the next Start asks again;
//   · switching back to the real orders touches no sandbox record and makes no call that writes;
//   · a sandbox with no pulled set (the old Sep 17 snapshot) waits empty for Start instead of playing it;
//   · Settings say "N newest Etsy orders, pulled <time>" with the Etsy calls, and the old snapshot button is gone.
//   node tests/charm-nest/sandbox-etsy-pull-page.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');

const sleep = ms => new Promise(r => setTimeout(r, ms));
const day = Math.floor(Date.now() / 1000);
const GF = '14k Gold Filled', SS = 'Sterling Silver';
const tx = (rid, i, sku, metal, created, ship) => ({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, create_timestamp: created, created_timestamp: created, paid_timestamp: created, expected_ship_date: ship, shipped_timestamp: null, variations: [{ formatted_name: 'Metal', formatted_value: metal }], is_personalized: false });
const receipt = (rid, hoursAgo, leadDays, sku, metal, extra = {}) => { const created = day - Math.round(hoursAgo * 3600), ship = created + leadDays * 86400; return Object.assign({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', create_timestamp: created, created_timestamp: created, update_timestamp: created + 60, updated_timestamp: created + 60, status: 'Paid', is_paid: true, is_shipped: false, transactions: [tx(rid, 1, sku, metal, created, ship)] }, extra); };
// the shop's 250 newest receipts, newest first as Etsy lists them; a few are already shipped / cancelled (kept, never played)
const SKUS = ['BR-TST-01', 'BR-TST-02', 'BR-TST-03', 'BR-TST-04', 'BR-TST-05', 'BR-TST-06'];
const SHOP = Array.from({ length: 250 }, (_, k) => { const i = 249 - k; return receipt(3700000001 + i, 2 * (250 - i), 2 + (i % 6), SKUS[i % 6], i % 2 ? SS : GF, i % 13 === 0 ? { is_shipped: true, status: 'Completed' } : i % 41 === 0 ? { is_canceled: true, status: 'Canceled' } : {}); });
const isOpen = r => !r.is_shipped && !r.is_canceled && r.is_paid !== false;
const OPEN_IDS = SHOP.filter(isOpen).map(r => String(r.receipt_id)).reverse();   // oldest first: the order the stream plays them
// the previous run's set (the Sep 17 kind: a pointer without source "etsy-pull") and what was built on it
const OLD_RECEIPTS = Array.from({ length: 12 }, (_, i) => receipt(3300000001 + i, 900 - i, 3, SKUS[i % 6], GF));
const OLD_IDS = new Set(OLD_RECEIPTS.map(r => String(r.receipt_id)));
const OLD = 'OLDRUN';
const PROD_COLLECTIONS = ['Charm_Custom_Orders', 'Charm_Pool', 'Charm_Nest_Sheets', 'Charm_Nest_Cancelled', 'Brites_Orders', 'Station_Activity'];

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-etsypull-'));
  const masterPath = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(masterPath, { count: 6, edge: false });
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;

  // ── the fake Etsy under the REAL op: every request recorded, a token that needs no refresh ──
  const etsy = { calls: [], delay: 700, status: 200 };
  const fakeFetch = async (url) => {
    const u = new URL(url); etsy.calls.push({ path: u.pathname, offset: +u.searchParams.get('offset'), limit: +u.searchParams.get('limit'), sort: u.searchParams.get('sort_on'), order: u.searchParams.get('sort_order') });
    await sleep(etsy.delay);
    if (etsy.status !== 200) return { ok: false, status: etsy.status, headers: { get: () => null }, text: async () => 'down', json: async () => ({}) };
    const off = +u.searchParams.get('offset'), lim = +u.searchParams.get('limit');
    return { ok: true, status: 200, headers: { get: () => null }, text: async () => '', json: async () => ({ count: SHOP.length, results: JSON.parse(JSON.stringify(SHOP.slice(off, off + lim))) }) };
  };
  const Pull = require(path.join(root, 'netlify/functions/_charmNestSandboxPull.js')), realPull = Pull.pull, atEntry = [];
  Pull.pull = (b, ctx) => {
    atEntry.push({ startId: b.startId, pool: st.list('Sandbox_Charm_Pool').length, sheets: st.list('Sandbox_Charm_Nest_Sheets').length, oldSet: !!st.doc('Charm_Sandbox', 'current'), body: JSON.parse(JSON.stringify(b)) });
    return realPull(b, ctx, { fetch: fakeFetch, token: async () => 'test-token', meter: { bump: () => ({ fromHttp() {}, failNet() {} }), flushNow: async () => {} } });
  };
  const fdb = st.admin.firestore(); if (!fdb.doc) fdb.doc = p => { const [c, ...rest] = p.split('/'); return fdb.collection(c).doc(rest.join('/')); };
  st.put('config', 'etsyOauth', { access_token: 'a', refresh_token: 'r', expires_at_ms: Date.now() + 3600e3 });

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
  page.on('pageerror', e => errors.push('sorter: ' + e.message)); page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text().slice(0, 200)); });
  const settle = extra => page.evaluate(([station, extra]) => { const s = CN.S.settings; Object.assign(s, { dsOrigin: station, engine: 'solver', budgetS: 12, review: 'off', naming: 'off', packingAI: 'off', notify: 'off', sound: 'off', autoCommit: 'on', runMode: 'manual', pullMode: 'all', heartbeatS: 2, heartbeatMiss: 2 }, extra || {}); CN.saveSettings(); }, [stationOrigin, extra]);
  const booted = () => page.waitForFunction(() => window.CN && window.Sandbox && window.Arrivals && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  const ev = async (fn, arg) => { try { return await page.evaluate(fn, arg); } catch (_) { return undefined; } };   // (a page that is navigating answers nothing)
  const until = async (fn, ms, why, arg) => { for (const t = Date.now(); Date.now() - t < ms;) { const v = await ev(fn, arg); if (v) return v; await sleep(120); } throw new Error('timed out: ' + why); };
  const lib = name => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === name);
  const pulls = () => lib('sandboxPullOrders'), wipes = () => lib('sandboxReset');
  const stream = () => st.doc('Charm_Sandbox', 'stream') || null, current = () => st.doc('Charm_Sandbox', 'current') || null;
  const rows = () => page.evaluate(() => B.orders.rows.map(r => String(r.order.receiptId)));
  const prodSnapshot = () => JSON.stringify(PROD_COLLECTIONS.map(c => [c, st.list(c)]));
  const prodLocal = () => page.evaluate(() => localStorage.getItem('cn.team.drafts'));

  // ── production: the master is indexed (shared, read-only in the sandbox) ──
  await page.goto(`${sorterOrigin}/charm-nest-1.html`); await booted(); await settle();
  await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mFile', { state: 'attached' }); await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });

  // ── the previous sandbox run: its old snapshot (no source), its stream, records built on it, and the browser's copy ──
  const oldPath = 'charmnest/sandbox/orders-old-snapshot.json', oldAt = Date.now() - 7 * 86400000;
  st.blobs.set(oldPath, { buf: Buffer.from(JSON.stringify({ at: oldAt, count: OLD_RECEIPTS.length, receipts: OLD_RECEIPTS })), generation: 1, meta: { contentType: 'application/json', metadata: {} } });
  st.put('Charm_Sandbox', 'current', { path: oldPath, count: OLD_RECEIPTS.length, at: oldAt, takenBy: 'someone' });
  st.put('Charm_Sandbox', 'stream', { on: true, v: 2, seed: 99, speed: 50, stepMs: 600000, min: 2, max: 5, simStart: oldAt, simNow: oldAt + 600000, tick: 1, snapshotPath: oldPath, total: OLD_RECEIPTS.length, brought: 3, startedAt: oldAt, tickAt: oldAt });
  st.put('Sandbox_Charm_Pool', 'pool-old', { poolId: 'pool-old', state: 'ready', receiptId: [...OLD_IDS][0] });
  st.put('Sandbox_Charm_Nest_Sheets', 'sheet-old', { sheetId: 'sheet-old', mark: OLD });
  st.put('Sandbox_Charm_Custom_Orders', `${[...OLD_IDS][0]}_1`, { key: `${[...OLD_IDS][0]}_1`, receiptId: [...OLD_IDS][0], state: 'completed' });
  st.put('Charm_Custom_Orders', 'prod-1', { key: 'prod-1', receiptId: '4100000001', state: 'completed' });
  st.put('Charm_Pool', 'prod-pool', { poolId: 'prod-pool', state: 'ready' });
  await page.evaluate(() => { localStorage.setItem('cn.team.drafts:sandbox', JSON.stringify({ 3300000001: { t: 'OLDRUN draft', at: Date.now() } })); localStorage.setItem('cn.team.drafts', JSON.stringify({ 4100000001: { t: 'production draft', at: Date.now() } })); });
  const prod0 = prodSnapshot(), prodKeys0 = await prodLocal();
  assert.strictEqual(await page.evaluate(() => !!document.getElementById('stSbSnap')), false, 'the old "Take a sandbox snapshot" button is gone');
  const settingsText = await page.evaluate(() => { openSettings(); const t = document.getElementById('dlgSettings').innerText; closeDlg(document.getElementById('dlgSettings')); return t; });
  assert(!/Take a sandbox snapshot|stored snapshot|snapshot taken on the Orders tab/i.test(settingsText) && /250 newest Etsy orders/.test(settingsText), 'Settings name the new source: ' + settingsText.match(/.{60}250 newest.{60}/)?.[0]);

  // ══ 1 · Sandbox switched ON from the real orders: clear the old, pull the 250 newest once, play them ══
  const mark1 = st.calls.length;
  await page.evaluate(() => { openSettings(); document.getElementById('stSbOn').click(); });
  const seen = { bar: '', button: '', spin: false, toasts: new Set(), starting: '' };
  for (const t = Date.now(); Date.now() - t < 120000;) {
    const v = await ev(() => ({ pulling: window.Sandbox && Sandbox.pulling && Sandbox.pulling(), bar: (document.getElementById('cnpSlot') || {}).innerText || '', btn: (document.querySelector('[data-sb-start]') || {}).textContent || '', spin: !!document.querySelector('[data-sb-start] .spin'), line: (document.querySelector('.sbWait') || {}).textContent || '', toasts: [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent) }));
    if (v) {
      if (v.pulling === 'pull') { if (/Pulling the 250 newest orders/.test(v.bar)) seen.bar = v.bar; if (/Pulling the 250 newest orders…/.test(v.btn) && v.spin) { seen.button = v.btn; seen.spin = true; seen.line = v.line; } }
      if (v.pulling === 'start' && v.line) seen.starting = v.line;
      v.toasts.forEach(x => seen.toasts.add(x));
      if (pulls().length >= 1 && v.pulling === '' && stream() && stream().on && current() && current().source === 'etsy-pull') break;
    }
    await sleep(100);
  }
  await booted();
  assert.strictEqual(pulls().length, 1, 'one start asked the op exactly once');
  const wipeAt = st.calls.findIndex((c, i) => i >= mark1 && c.op === 'sandboxReset'), pullAt = st.calls.findIndex((c, i) => i >= mark1 && c.op === 'sandboxPullOrders');
  assert(wipeAt >= 0 && wipeAt < pullAt, `the complete clean-up ran first (wipe at ${wipeAt}, pull at ${pullAt})`);
  assert(wipes().length >= 1 && !st.calls.slice(pullAt).some(c => c.op === 'sandboxReset'), 'no second clean-up after the pull');
  const req1 = pulls()[0].body;
  assert(req1.sandbox === true && /^sbx-[a-z0-9]+-[a-z0-9]+$/.test(req1.startId) && req1.startId.length <= 80 && req1.speed === 50 && req1.stream === true && req1.seed === 0, 'the request: op, sandbox, one startId, speed, stream: ' + JSON.stringify(req1));
  assert.strictEqual(atEntry[0].pool, 0, "the old sandbox's records were gone when the pull began");
  assert.strictEqual(atEntry[0].sheets, 0, "the old sandbox's sheets were gone when the pull began");
  assert.strictEqual(st.doc('Sandbox_Charm_Custom_Orders', `${[...OLD_IDS][0]}_1`), undefined, 'the old custom order is gone');
  assert.strictEqual(etsy.calls.length, 3, 'one pull cost 3 Etsy calls');
  assert.deepStrictEqual(etsy.calls.map(c => [c.offset, c.limit]).sort((a, b) => a[0] - b[0]), [[0, 100], [100, 100], [200, 50]], 'pages of 100, 100 and 50');
  assert(etsy.calls.every(c => /\/receipts$/.test(c.path) && c.sort === 'created' && c.order === 'desc'), 'the newest first, the receipts endpoint only');
  const cur1 = current(); assert(cur1.source === 'etsy-pull' && cur1.startId === req1.startId && cur1.count === 250 && cur1.open === OPEN_IDS.length && cur1.calls === 3, 'the set the op stored: ' + JSON.stringify({ ...cur1, updatedAt: undefined }));
  assert(!st.blobs.has(oldPath), 'the old snapshot file is gone');
  assert(seen.bar, 'a labelled spinner in the top bar says "Pulling the 250 newest orders"');
  assert(seen.spin && /Pulling the 250 newest orders…/.test(seen.button), 'the Orders tab button shows a spinner and says what it waits for: ' + seen.button);
  assert(/clearing the old orders, then pulling the 250 newest|Asking Etsy/.test(seen.starting + ' ' + (seen.line || '')), 'the empty Orders tab says what is happening: ' + seen.starting + ' / ' + seen.line);
  const toast1 = [...seen.toasts].find(t => /newest Etsy orders are open and play/.test(t));
  assert(toast1 && toast1.includes(`${OPEN_IDS.length} of 250 newest Etsy orders are open and play · 3 Etsy calls`) && /\(3 of 20 today, 5 pulls left\)/.test(toast1), 'the Etsy call count the op reports is shown: ' + [...seen.toasts].join(' | '));
  assert(!/until you press Start/.test([...seen.toasts].join(' ')), 'the start does not say the sandbox waits for Start: ' + [...seen.toasts].join(' | '));
  // the stream plays the NEW set, oldest first, and none of the old orders ever reaches the sorter
  const s1 = stream(); assert(s1.on && s1.total === OPEN_IDS.length && s1.snapshotPath === cur1.path, 'the stream plays the pulled set: ' + JSON.stringify(s1));
  await until(() => B.orders.rows.length > 0, 90000, 'orders arrive from the stream');
  await sleep(500);
  const got1 = [...new Set(await rows())];
  assert(got1.length >= 2 && got1.every(id => OPEN_IDS.includes(id)) && !got1.some(id => OLD_IDS.has(id)), 'only the 250 newest reach the sorter, none of the old orders: ' + got1.join(','));
  assert.deepStrictEqual([...got1].sort((a, b) => a - b), OPEN_IDS.slice(0, got1.length), 'the oldest first, each once');
  assert.strictEqual(await page.evaluate(() => localStorage.getItem('cn.team.drafts:sandbox')), null, 'the browser\'s old sandbox draft is gone');
  const line1 = await page.evaluate(() => { openSettings(); return new Promise(res => setTimeout(() => { const t = document.getElementById('stSbStatus').textContent; closeDlg(document.getElementById('dlgSettings')); res(t); }, 400)); });
  assert.match(line1, new RegExp(`^Order stream: 250 newest Etsy orders, pulled \\d+ \\w+, \\d\\d:\\d\\d \\(${OPEN_IDS.length} open\\) with 3 Etsy calls · seed \\d+ · step \\d+ · \\d+ of ${OPEN_IDS.length} orders in · simulated .+ · 50x$`), 'Settings say where the orders come from: ' + line1);
  assert.strictEqual(prodSnapshot(), prod0, 'production records are untouched');
  assert.strictEqual(await prodLocal(), prodKeys0, "the real side's browser keys are untouched");
  console.log('1 ok · switch ON:', pulls().length, 'pull,', etsy.calls.length, 'Etsy calls ·', line1);

  // ══ 2 · a plain reload with the sandbox on: no pull, no wipe, the stream plays on ══
  const before2 = { pulls: pulls().length, wipes: wipes().length, tick: stream().tick, startId: current().startId, etsy: etsy.calls.length, ensures: lib('sandboxStream').length };
  await page.reload(); await booted(); await sleep(4000);
  assert.strictEqual(pulls().length, before2.pulls, 'a reload pulls nothing');
  assert.strictEqual(wipes().length, before2.wipes, 'a reload clears nothing');
  assert.strictEqual(etsy.calls.length, before2.etsy, 'a reload makes no Etsy call');
  assert(current().startId === before2.startId && stream().on && stream().tick >= before2.tick, 'the stored set plays on');
  assert.strictEqual(await page.evaluate(() => Sandbox.held()), false, 'the sandbox is not waiting');
  assert(lib('sandboxStream').length > before2.ensures, 'the stream was resumed (ensure) from the stored set');
  const line2 = await page.evaluate(() => { openSettings(); return new Promise(res => setTimeout(() => { const t = document.getElementById('stSbStatus').textContent; closeDlg(document.getElementById('dlgSettings')); res(t); }, 1500)); });
  assert.match(line2, /^Order stream: 250 newest Etsy orders, pulled .+ with 3 Etsy calls · seed \d+/, 'after a reload the line still names the pulled set: ' + line2);
  console.log('2 ok · reload:', line2.slice(0, 90));

  // ══ 3 · Reset, then Start pressed three ways at once: one pull, a NEW startId ══
  const mark3 = st.calls.length, oldStart = current().startId;
  await page.evaluate(() => { window.__old = true; window.__reset = Sandbox.reset({}); });
  await until(() => !window.__old && window.Sandbox && Sandbox.held(), 90000, 'the sandbox waits after the reset'); await booted();
  assert.strictEqual(pulls().length, before2.pulls, 'Reset pulls nothing');
  const waitText = await page.evaluate(() => { CN.setMode('orders'); Orders.render(); return Sandbox.heldText(); });
  assert.match(waitText, /^Sandbox is empty\. Press Start \(or turn Auto on\) to pull the 250 newest Etsy orders and play them\.$/, 'the empty sandbox says what Start does: ' + waitText);
  assert.strictEqual((await rows()).length, 0, 'nothing of the old run is listed');
  const wipes3 = wipes().length;
  await page.evaluate(() => { window.__s = [Sandbox.start(), Sandbox.ready(true), Sandbox.ready(true)]; });
  await until(() => Sandbox.stream() && Sandbox.pulling() === '', 90000, 'the second pull completed');
  assert.strictEqual(pulls().length, before2.pulls + 1, 'Start, Pull orders and Auto asked for one pull between them: ' + JSON.stringify(st.calls.slice(mark3).map(c => c.op || c.name)) + ' ERR ' + JSON.stringify(errors.slice(-5)) + ' ' + JSON.stringify(await page.evaluate(() => window.__s && Promise.allSettled(window.__s))) + ' ' + JSON.stringify(await page.evaluate(() => ({ held: Sandbox.held(), pulling: Sandbox.pulling(), stream: !!Sandbox.stream(), hold: localStorage.getItem('cn.sandboxHold'), s: Sandbox.stream() }))));
  assert.strictEqual(new Set(pulls().map(c => c.body.startId)).size, 2, 'a NEW startId for the new start');
  assert(current().startId !== oldStart && current().source === 'etsy-pull', 'the set was replaced');
  assert.strictEqual(wipes().length, wipes3, 'Start after a clean-up does not clean again');
  assert.strictEqual(etsy.calls.length, 6, 'two pulls, 6 Etsy calls in all');
  console.log('3 ok · reset + start:', pulls().length, 'pulls in all');

  // ══ 4 · failures leave the sandbox EMPTY, say why in the op's words, and are never retried by the page ══
  // 4a · the daily cap (the ledger the wipe keeps)
  await page.evaluate(() => { window.__old = true; window.__reset = Sandbox.reset({}); });
  await until(() => !window.__old && window.Sandbox && Sandbox.held(), 90000, 'waiting again'); await booted();
  const today = new Date().toISOString().slice(0, 10);
  st.put('Charm_Sandbox', 'pulls', { day: today, pulls: 6, calls: 18, tokenRefreshes: 0, starts: [], claim: null });
  const n4 = { pulls: pulls().length, etsy: etsy.calls.length, wipes: wipes().length };
  await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
  await page.click('[data-sb-start]');
  await until(() => Sandbox.held() && /used up/.test(Sandbox.heldText()), 60000, 'the cap message');
  const cap = await page.evaluate(() => ({ text: Sandbox.heldText(), btn: document.querySelector('[data-sb-start]').textContent.trim(), disabled: document.querySelector('[data-sb-start]').disabled, stream: !!Sandbox.stream(), rows: B.orders.rows.length, shown: document.querySelector('.sbWait').textContent }));
  assert.strictEqual(cap.text, "Sandbox is empty. Today's 6 sandbox pulls from Etsy are used up, so the sandbox stays empty until tomorrow (UTC midnight).", 'the cap: the op\'s own line, shown as it is: ' + cap.text);
  assert(cap.btn === 'Start' && !cap.disabled && !cap.stream && cap.rows === 0 && cap.shown === cap.text, 'empty, with Start ready again: ' + JSON.stringify(cap));
  assert(!current() && !stream(), 'the cloud holds no orders after the refused pull (never the old ones)');
  await sleep(5000);
  assert.strictEqual(pulls().length, n4.pulls + 1, 'one request, no retry');
  assert.strictEqual(etsy.calls.length, n4.etsy, 'a refused pull makes no Etsy call');
  await page.reload(); await booted(); await sleep(2500);
  assert(pulls().length === n4.pulls + 1 && wipes().length === n4.wipes && etsy.calls.length === n4.etsy, 'a reload after a failed pull neither pulls nor clears');
  assert(await page.evaluate(() => Sandbox.held() && /used up/.test(Sandbox.heldText()) && !Sandbox.stream()), 'the sandbox is still empty and still says why');
  assert.strictEqual((await rows()).length, 0, 'no old orders');
  // 4b · Etsy down: one call, the op's own line, the count it reports
  st.put('Charm_Sandbox', 'pulls', { day: today, pulls: 2, calls: 6, tokenRefreshes: 0, starts: [], claim: null });
  etsy.status = 503; const e4 = etsy.calls.length, p4 = pulls().length;
  await page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await page.click('[data-sb-start]');
  await until(() => Sandbox.held() && /did not answer/.test(Sandbox.heldText()), 60000, 'the Etsy-down message');
  const down = await page.evaluate(() => Sandbox.heldText());
  assert.strictEqual(down, 'Sandbox is empty. Etsy did not answer (HTTP 503), so the sandbox is empty. Press Start to try again. (1 Etsy call used)', 'Etsy down: the op\'s line once, with the calls it used: ' + down);
  await sleep(4000);
  assert(pulls().length === p4 + 1 && etsy.calls.length === e4 + 1, 'one request and one Etsy call, never retried: ' + (pulls().length - p4) + ' requests, ' + (etsy.calls.length - e4) + ' calls');
  assert(!current() && !stream() && (await rows()).length === 0, 'empty');
  // 4c · the next Start (a person's press) asks again, and works
  etsy.status = 200; const e5 = etsy.calls.length;
  await page.click('[data-sb-start]');
  await until(() => Sandbox.stream() && Sandbox.pulling() === '' && !Sandbox.held(), 90000, 'the pull after the failure');
  assert.strictEqual(etsy.calls.length, e5 + 3, 'the next press made its one pull of 3 calls');
  assert(current().source === 'etsy-pull' && stream().on, 'the sandbox plays the new set');
  console.log('4 ok · cap, Etsy down, recovery: Etsy calls in all', etsy.calls.length);

  // ══ 5 · back to the real orders: no sandbox record touched, nothing written ══
  await page.evaluate(() => { openSettings(); document.getElementById('stSbOn').click(); });
  await until(() => window.CN && window.Sandbox && typeof WORKSPACE_SANDBOX !== 'undefined' && WORKSPACE_SANDBOX === false && CN.S.cloud.ok !== null, 60000, 'the real orders again'); const mark5b = st.calls.length; await sleep(2500);
  const writes5 = st.calls.slice(mark5b).filter(c => /^(sandboxReset|sandboxPullOrders|sandboxStream|sandboxPut|sandboxCancel)$/.test(c.op || ''));
  assert.deepStrictEqual(writes5.map(c => c.op), [], 'switching back wipes, pulls and steps nothing');
  assert.strictEqual(etsy.calls.length, e5 + 3, 'switching back makes no Etsy call');
  assert.strictEqual(prodSnapshot(), prod0, 'production records are untouched');
  assert.strictEqual(await prodLocal(), prodKeys0, "the real side's browser keys are untouched");
  console.log('5 ok · back to the real orders');

  // ══ 6 · a sandbox with no pulled set (the old snapshot) waits empty; it is not played ══
  st.docs.delete('Charm_Sandbox/current'); st.put('Charm_Sandbox', 'current', { path: oldPath, count: OLD_RECEIPTS.length, at: oldAt, takenBy: 'someone' });
  st.blobs.set(oldPath, { buf: Buffer.from(JSON.stringify({ at: oldAt, count: OLD_RECEIPTS.length, receipts: OLD_RECEIPTS })), generation: 2, meta: { contentType: 'application/json', metadata: {} } });
  st.docs.delete('Charm_Sandbox/stream');
  await settle({ sandbox: 'on', sandboxStream: 'on' }); await page.evaluate(() => { localStorage.removeItem('cn.sandboxHold'); sessionStorage.removeItem('cn.sandboxPullNext'); });
  const n6 = { calls: st.calls.length, pulls: pulls().length, wipes: wipes().length, ensures: lib('sandboxStream').length, etsy: etsy.calls.length };
  await page.reload(); await booted();
  await until(() => Sandbox.held(), 30000, 'the sandbox with no pulled set waits').catch(async e => { console.log('DEBUG', JSON.stringify(await ev(() => ({ sb: WORKSPACE_SANDBOX, held: Sandbox.held(), hold: localStorage.getItem('cn.sandboxHold'), stream: !!Sandbox.stream(), st: Sandbox.status && Sandbox.status() }))), JSON.stringify(st.calls.slice(n6.calls).map(c => c.op || c.name)), JSON.stringify(errors.slice(-4))); throw e; }); await sleep(3000);
  const w6 = await page.evaluate(() => ({ text: Sandbox.heldText(), stream: !!Sandbox.stream(), rows: B.orders.rows.length, label: Sandbox.label() }));
  assert(/it held orders from an earlier run, which are cleared first\. Press Start to pull the 250 newest Etsy orders/.test(w6.text) && !w6.stream && w6.rows === 0 && w6.label === 'Sandbox · paused', 'it says it is empty and what Start does: ' + JSON.stringify(w6));
  assert(pulls().length === n6.pulls && wipes().length === n6.wipes && lib('sandboxStream').length === n6.ensures && etsy.calls.length === n6.etsy, 'a reload that finds the old snapshot pulls, clears and plays nothing');
  console.log('6 ok · old snapshot is not played');

  // ══ 7 · "the pulled orders all at once" (no stream): the same one pull, asked with stream:false, and no stream is started ══
  await page.evaluate(() => { const s = CN.S.settings; s.sandboxStream = 'off'; CN.saveSettings(); });
  const n7 = { pulls: pulls().length, etsy: etsy.calls.length };
  await page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await page.click('[data-sb-start]');
  await until(() => window.__never || (window.Sandbox && !Sandbox.held() && !Sandbox.pulling() && Sandbox.pulled()), 120000, 'the pull without a stream');
  assert.strictEqual(pulls().length, n7.pulls + 1, 'one pull for the one Start'); assert.strictEqual(etsy.calls.length, n7.etsy + 3, '3 Etsy calls');
  assert.strictEqual(pulls()[pulls().length - 1].body.stream, false, 'asked without a stream');
  assert(current() && current().source === 'etsy-pull' && !stream(), 'the set is stored and no stream was started');
  const line7 = await page.evaluate(() => Sandbox.streamText());
  assert.match(line7, /^Orders: 250 newest Etsy orders, pulled .+ \(\d+ open\) with 3 Etsy calls, all at once$/, 'the line names the set: ' + line7);
  console.log('7 ok · no stream:', line7);

  assert.deepStrictEqual(etsy.calls.filter(c => !/\/receipts$/.test(c.path)), [], 'the only Etsy endpoint used was the receipts list');
  assert.deepStrictEqual(errors.filter(e => !/firebase stub|HTTP 4\d\d|HTTP 50\d|status of (409|429|50\d)|Failed to load resource/.test(e)), [], 'no page errors');
  console.log(`sandbox etsy pull page OK: ${pulls().length} pull requests, ${etsy.calls.length} Etsy calls in all (3 per pull)`);
  await browser.close(); srv.close && srv.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
