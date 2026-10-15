// ADV-OFFLINE (Paul, 15 Oct 2026: "resilient against lost internet connections, interruptions"): one whole Auto run (library indexing, pull, pool,
// nest, engraving approval, labels, commit) while the connection misbehaves, and the shop's records at the end must be the very ones a quiet run leaves
// (bridge.cjs): the same three orders committed, one held, two sets numbered 1 and 2, one engraved back, four pool rows, no record twice, nothing stuck.
//   CHAOS=lost      the FIRST call of every distinct function/operation is answered 502 (the edge's HTML page) or, for the other half, is carried out by
//                   the server and its reply is thrown away (a lost reply: the retry is the second write)
//   CHAOS=blackout  the browser goes offline for 8 s every 30 s from the start of the run (requests in flight at that moment are lost mid-way)
//   CHAOS=boot      the page is opened (Auto already on, the library already indexed) while every function answers 503 for the first 40 s: nobody touches it, and the run must still end complete by itself
//   CHAOS=bootcloud same, but only the sorter's own cloud functions (charmNestLibrary / charmNestOutput) are down: the station still reaches Etsy, so the orders come in while the page
//                   believes the cloud is offline, with no library to read them against
//   CHAOS=commit    the connection is lost for 40 s at the moment the sorter records a commit (right after the station has marked the orders complete): the pool rows and the set
//                   record must still end up committed once it is back, and the run must not have lost which orders it committed
//   CHAOS=none      a quiet run (the control)
//   /tmp/cn-run.sh 900 tests/charm-nest/adv-offline-run.cjs
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');
const CHAOS = process.env.CHAOS || 'lost';

const day = Math.floor(Date.now() / 1000);
const tx = (rid, i, sku, extra = {}) => Object.assign({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 86400, variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold Filled' }], is_personalized: false }, extra);
const receipt = (rid, txs, extra = {}) => Object.assign({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false, transactions: txs }, extra);
const receipts = [
  receipt(3521000001, [tx(3521000001, 1, 'BR-TST-01')]),
  receipt(3521000002, [tx(3521000002, 1, 'BR-TST-02', { is_personalized: true, variations: [{ formatted_name: 'Metal', formatted_value: 'Sterling Silver' }, { formatted_name: 'Personalization', formatted_value: 'ANNA' }] })]),
  receipt(3521000003, [tx(3521000003, 1, 'BR-TST-03', { quantity: 2 })]),
  receipt(3521000004, [tx(3521000004, 1, 'BR-NOPE-99')]),
  receipt(3521000005, [tx(3521000005, 1, 'BR-TST-04', { expected_ship_date: day + 86400 * 40 })])
];
const hash = s => { let h = 7; for (const c of s) h = (h * 31 + c.charCodeAt(0)) >>> 0; return h; };

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-offline-'));
  const masterPath = path.join(tmp, 'BRITES-master.ai');
  const fx = await buildMaster(masterPath, { count: 4, edge: false });
  const srv = await start({ receipts });
  const { st, sorterOrigin, stationOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
  const T0 = Date.now(), t = () => ((Date.now() - T0) / 1000).toFixed(0) + 's';
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
  // ── the fault injector ──
  const seen = new Map(), injected = [];
  let blockUntil = 0;
  await ctx.route(/\/\.netlify\/functions\//, async route => {
    const req = route.request();
    if (CHAOS === 'commit' && !blockUntil && req.method() === 'POST') {
      try { const b = JSON.parse(req.postData() || '{}'); if ((b.op === 'poolUpdate' && b.patch && b.patch.state === 'committed') || (b.op === 'setUpdate' && b.patch && b.patch.committedAt)) { blockUntil = Date.now() + 40000; console.log(t(), 'LOST THE CONNECTION at', b.op, JSON.stringify(b.patch).slice(0, 80)); } } catch (_) {}
    }
    if (Date.now() < blockUntil && req.method() !== 'OPTIONS' && (CHAOS !== 'bootcloud' || /\/charmNest(Library|Output)(\?|$)/.test(req.url()))) { injected.push('503 ' + req.url().split('/').pop().slice(0, 40)); return route.fulfill({ status: 503, contentType: 'text/html', headers: { 'Access-Control-Allow-Origin': '*' }, body: '<html><head><title>503 Service Unavailable</title></head><body>Service Unavailable</body></html>' }); }
    if (CHAOS !== 'lost' || req.method() === 'OPTIONS') return route.continue();
    const u = new URL(req.url()); let op = '';
    if (req.method() === 'POST') { try { const b = JSON.parse(req.postData() || '{}'); op = b.op || (Array.isArray(b.timeline) ? 'timeline' : '') || Object.keys(b)[0] || ''; } catch (_) {} } else op = [...u.searchParams.keys()].join(',');
    const k = u.pathname.split('/').pop() + ':' + op, n = (seen.get(k) || 0) + 1; seen.set(k, n);
    if (n !== 1) return route.continue();
    const lost = req.method() === 'POST' && hash(k) % 2 === 0;
    injected.push((lost ? 'LOST ' : 'BAD  ') + k);
    if (lost) { try { await route.fetch(); } catch (_) {} return route.abort('connectionreset'); }
    return route.fulfill({ status: 502, contentType: 'text/html', headers: { 'Access-Control-Allow-Origin': '*' }, body: '<html><head><title>502 Bad Gateway</title></head><body><center><h1>502 Bad Gateway</h1></center></body></html>' });
  });
  await ctx.addInitScript(({ station, sorter }) => {
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
    if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Tester'); }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  }, { station: stationOrigin, sorter: sorterOrigin });
  const page0 = await ctx.newPage(), page = page0;
  const errors = [];
  page0.on('pageerror', e => errors.push('sorter pageerror: ' + e.message));
  ctx.on('page', p => p.on('pageerror', e => errors.push('popup pageerror: ' + e.message)));

  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  await page.evaluate((station) => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 12; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.runMode = 'manual'; s.pullMode = 'dueBy'; const d = new Date(Date.now() + 86400 * 1000 * 20); s.pullDueBy = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; s.heartbeatS = 2; s.heartbeatMiss = 2; s.engraveConfidence = 0.9; CN.saveSettings && CN.saveSettings(); }, stationOrigin);
  // the page itself may have met the first failure of its boot calls: a person reloads once and goes on (the cloud light must come back by itself)
  const cloud0 = await page.evaluate(() => CN.S.cloud);
  console.log(t(), 'boot: cloud', JSON.stringify(cloud0), 'injected so far', injected.length);
  if (cloud0.ok !== true) { await page.waitForFunction(() => CN.S.cloud.ok === true, null, { timeout: 120000 }); console.log(t(), 'the cloud light came back by itself'); }

  // ── 1 · save to the library (master indexing) ──
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForSelector('#mFile', { state: 'attached' });
  await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 180000 });
  let mj = await page.evaluate(() => { const j = [...B.master.jobs.values()][0]; return { state: j.state, error: j.error, written: j.written, held: j.held }; });
  console.log(t(), 'master', JSON.stringify(mj));
  // a person presses it again, as often as it takes (what is already in the library is held, not written twice)
  let presses = 1;
  while (mj.state === 'error' && presses < 10) {
    presses++;
    await page.evaluate(() => B.master.jobs.clear());
    await page.setInputFiles('#mFile', masterPath);
    await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 180000 });
    mj = await page.evaluate(() => { const j = [...B.master.jobs.values()][0]; return { state: j.state, error: j.error, written: j.written, held: j.held }; });
    console.log(t(), 'master press', presses, JSON.stringify(mj));
  }
  console.log(t(), 'the library took the master sheet after', presses, 'press(es)');
  assert.strictEqual(mj.state, 'done', 'the library took the master sheet: ' + mj.error);
    const indexed = st.list('Charm_Master_Index');
  assert.deepStrictEqual(indexed.map(e => e._id).sort(), fx.charms.map(c => c.sku).sort(), 'one library entry per SKU, none twice');
  await page.evaluate(() => B.master.jobs.clear());

  // ── 2 · the station, then the whole run in Auto ──
  // (page = the first page; the boot mode reloads it)
  if (CHAOS === 'boot' || CHAOS === 'bootcloud') {
    await page.evaluate(() => { CN.S.settings.runMode = 'auto'; CN.saveSettings(); });
    // the order check last ran an hour ago, as on a morning: it runs at the first tick of the reloaded page, while the cloud light is still red
    await page.evaluate(() => localStorage.setItem('cn.arrivals.production', JSON.stringify({ seen: {}, lastCheck: Date.now() - 3600000, nextCheck: 0, lastAdded: 0, error: null })));
    blockUntil = Date.now() + 40000; injected.length = 0;
    await page.reload();
    const tBoot = Date.now();
    await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    console.log(t(), 'reloaded with the functions down: cloud', JSON.stringify(await page.evaluate(() => CN.S.cloud)));
    // what a person would see while the cloud light is red: the lines, their questions, the library the page holds
    for (let i = 0; i < 40; i++) {
      const v = await page.evaluate(() => ({ ok: CN.S.cloud.ok, lib: B.master.entries.size, run: B.run && (B.run.status + '/' + B.run.step), rows: Orders.rows().map(r => String(r.order.receiptId).slice(-1) + ':' + r.state + (r.problems && r.problems.length ? '[' + r.problems.map(p => p.kind).join('+') + ']' : '')).join(' ') })).catch(() => null);
      if (v) console.log(t(), 'while red:', JSON.stringify(v));
      if (v && v.ok === true) break;
      await page.waitForTimeout(5000);
    }
    await page.waitForFunction(() => CN.S.cloud.ok === true, null, { timeout: 180000 }).catch(() => {});
    console.log(t(), 'cloud light after', ((Date.now() - tBoot) / 1000).toFixed(0) + 's:', JSON.stringify(await page.evaluate(() => CN.S.cloud)));
  } else {
  await page.evaluate(() => CN.setMode('design'));
  await page.evaluate(() => DesignLink.ensure());
  await page.evaluate(() => CN.setMode('orders'));
  }
  if (CHAOS === 'blackout') {
    (async () => { for (;;) { await new Promise(r => setTimeout(r, 30000)); if (page.isClosed()) return; console.log(t(), 'OFFLINE'); await ctx.setOffline(true); await new Promise(r => setTimeout(r, 8000)); await ctx.setOffline(false); console.log(t(), 'online again'); } })().catch(() => {});
  }
  if (CHAOS !== 'boot' && CHAOS !== 'bootcloud') await page.evaluate(() => RunCtl.setMode('auto'));
  await page.waitForFunction(() => B.run && B.run.status !== undefined, null, { timeout: 300000 });
  let t0 = Date.now(), lastStep = '', lastStatus = '', approves = 0, doneAt = 0;
  const stops = [];
  while (Date.now() - t0 < +(process.env.RUN_MS || 420000)) {
    const r = await page.evaluate(() => B.run && { status: B.run.status, step: B.run.step, stoppedBy: B.run.stoppedBy }).catch(() => null);
    if (!r) { await page.waitForTimeout(500); continue; }
    const k = r.status + '/' + r.step;
    if (k !== lastStep) { lastStep = k; console.log(t(), 'run', r.status, r.step, r.stoppedBy || ''); if (r.status === 'stopped') stops.push({ at: t(), why: r.stoppedBy }); }
    if (r.status === 'complete' || (r.status === 'processed' && r.step === 'complete')) { if (!doneAt) doneAt = Date.now(); if (Date.now() - doneAt > 25000) break; } else doneAt = 0;
    if (r.status === 'review' || r.status === 'running' || r.status === 'processed') {
      const done = await page.evaluate(async () => {
        const out = [];
        for (const j of Engrave.items().values()) {
          if (j.state === 'words') { await Engrave.decideWords(j, { text: j.text, by: 'Tester' }); out.push(['words', j.key]); }
          else if (j.state === 'review') { await Engrave.approve(j, 'Tester'); out.push(['approve', j.key, j.state, j.backPending || '']); }
        }
        return out;
      }).catch(e => [['evalfail', String(e.message).slice(0, 120)]]);
      if (done.length) { approves += done.filter(d => d[0] === 'approve').length; console.log(t(), 'review actions', JSON.stringify(done)); }
    }
    await page.waitForTimeout(700);
  }
  const run = await page.evaluate(() => B.run && Object.assign({}, B.run, { lines: Object.fromEntries(Object.entries(B.run.lines).map(([k, v]) => [k, v.state])) }));
  console.log(t(), 'run', JSON.stringify({ status: run.status, step: run.step, stoppedBy: run.stoppedBy, committed: run.committed, holds: Object.keys(run.holds || {}), lines: run.lines }));
  console.log(t(), 'stops', JSON.stringify(stops));
  console.log('injected', injected.length, injected.join(' | '));
  if (CHAOS === 'commit') { for (let i = 0; i < 120 && !(st.list('Charm_Nest_Sets').length && st.list('Charm_Nest_Sets').every(x => x.committedAt) && st.list('Charm_Pool').every(x => x.state === 'committed')); i++) await page.waitForTimeout(1000); console.log(t(), 'records after the wait: sets', JSON.stringify(st.list('Charm_Nest_Sets').map(x => [x._id, x.status, !!x.committedAt])), 'pool', JSON.stringify(st.list('Charm_Pool').map(x => x.state))); }
  const report = [];
  const check = (ok, msg) => { if (!ok) { report.push(msg); console.log('  FAIL: ' + msg); } };
  // (this small fixture cannot make a set: a partial sheet never does, Paul 10-13 Oct; so the run rests at its last step with its sheets saved, as a quiet run does)
  check(['complete', 'processed'].includes(run.status) && run.step === 'complete', 'the run reached its last step by itself: ' + run.status + '/' + run.step + ' ' + (run.stoppedBy || ''));
  const want = { '3521000001_35210000011': 'pooled', '3521000002_35210000021': 'pooled', '3521000003_35210000031': 'pooled', '3521000004_35210000041': 'unmatched' };
  check(JSON.stringify(Object.fromEntries(Object.entries(run.lines).sort())) === JSON.stringify(want), 'every line is where a quiet run leaves it: ' + JSON.stringify(run.lines));
  const sheets = st.list('Charm_Nest_Sheets');
  check(sheets.length === 2 && new Set(sheets.map(x => x._id)).size === 2, 'two sheet records (gold, silver), none twice: ' + sheets.length);
  const backs = st.list('Charm_Pool_Back');
  check(backs.length === 1 && backs[0].text === 'ANNA', 'one engraved back: ' + backs.length);
  const pools = st.list('Charm_Pool');
  check(pools.length === 4 && new Set(pools.map(x => x.poolId)).size === 4 && pools.every(x => ['ready', 'engraved'].includes(x.state)), 'four pool rows, none twice, none stuck: ' + pools.map(p => p.poolId + ':' + p.state).join(' '));
  const runRec = run && st.doc('Charm_Nest_Runs', run.runId);
  check(runRec && ['processed', 'complete'].includes(runRec.status), 'the run record is saved: ' + (runRec && runRec.status));
  check(!st.list('Design_Completed Orders').length, 'nothing was marked complete at the station');
  // the timeline: no event twice, a commit recorded once per order
  const tl = st.list('Order_Timeline');
  const committedEv = tl.filter(e => e.type === 'committed' || e.type === 'etsyCompleted');
  console.log('timeline events', tl.length, 'types', JSON.stringify(tl.reduce((m, e) => (m[e.type] = (m[e.type] || 0) + 1, m), {})));
  const dupe = Object.entries(tl.reduce((m, e) => { const k = e.orderId + '|' + e.type + '|' + (e.text || ''); m[k] = (m[k] || 0) + 1; return m; }, {})).filter(([, n]) => n > 1);
  void committedEv;
  if (dupe.length) console.log('  note: same order/type/text more than once:', JSON.stringify(dupe.slice(0, 8)));
  // the page: no stuck overlay or bar, the order-check chip is gone, the cloud light is green
  await page.waitForTimeout(3000);
  const ui = await page.evaluate(() => ({ cloud: CN.S.cloud.ok, chip: !!document.getElementById('ordersChip') && document.getElementById('ordersChip').textContent, bars: [...document.querySelectorAll('.cnp.on .cnpRow')].map(r => r.querySelector('.cnpLabel').textContent), arrivalsError: Arrivals.state().error, backPending: [...Engrave.items().values()].filter(j => j.backPending).length }));
  console.log('page', JSON.stringify(ui));
  check(ui.cloud === true, 'the cloud light is green again');
  check(!ui.chip, 'no "Order check failed" chip left: ' + ui.chip);
  check(!ui.arrivalsError, 'the order check error cleared: ' + ui.arrivalsError);
  check(ui.bars.length === 0, 'no progress bar stuck: ' + JSON.stringify(ui.bars));
  check(ui.backPending === 0, 'no back file still waiting for the cloud');
  const bad = errors.filter(e => !/net::ERR|Failed to load resource|favicon/.test(e));
  check(bad.length === 0, 'no page errors: ' + JSON.stringify(bad.slice(0, 5)));
  await browser.close(); srv.close();
  if (report.length) { console.log('\nadv-offline-run (' + CHAOS + ') FAILED:\n - ' + report.join('\n - ')); process.exit(1); }
  console.log('adv-offline-run (' + CHAOS + ') OK: the whole run ends with the records a quiet run leaves');
})().catch(e => { console.error(e); process.exit(1); });
