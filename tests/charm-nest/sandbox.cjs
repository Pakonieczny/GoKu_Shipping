// The sandbox: one real read of the open orders is stored, then the sorter runs a whole Auto set against an emulated Etsy
// that serves that snapshot, and NOTHING lands in a production collection, file or Etsy call. This test proves the
// isolation by counting: every production collection the apps write has the same documents before and after the run,
// every new file sits under charmnest/sandbox/, and the real Etsy functions are never called once the sandbox is on.
//   node tests/charm-nest/sandbox.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');

const day = Math.floor(Date.now() / 1000);
const tx = (rid, i, sku, extra = {}) => Object.assign({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 86400,   /* due tomorrow: three small pieces never fill a sheet, and a piece that is due is what makes a partial sheet cut */ variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold Filled' }], is_personalized: false }, extra);
const receipt = (rid, txs) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false, transactions: txs });
const receipts = [
  receipt(3521000001, [tx(3521000001, 1, 'BR-TST-01')]),
  receipt(3521000002, [tx(3521000002, 1, 'BR-TST-02', { is_personalized: true, variations: [{ formatted_name: 'Metal', formatted_value: 'Sterling Silver' }, { formatted_name: 'Personalization', formatted_value: 'ANNA' }] })]),
  receipt(3521000003, [tx(3521000003, 1, 'BR-TST-03', { quantity: 2 })])
];
const PROD = ['Design_Completed Orders', 'Design_RealTime_Selected_Orders', 'Design_Order_Archive', 'Brites_Orders', 'Brites_Messages', 'Charm_Pool', 'Charm_Pool_Back', 'Charm_Nest_Sets', 'Charm_Nest_Counters', 'Charm_Nest_Runs', 'Charm_Nest_Sheets', 'Design_Bridge'];

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-sandbox-'));
  const masterPath = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(masterPath, { count: 4, edge: false });
  const srv = await start({ receipts });
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
  page.on('pageerror', e => errors.push('sorter: ' + e.message)); page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text().slice(0, 200)); });
  const settle = (station) => page.evaluate((station) => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 12; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.runMode = 'manual'; s.pullMode = 'all'; s.heartbeatS = 2; s.heartbeatMiss = 2; CN.saveSettings(); }, station);

  // ── production mode: index the master, take the snapshot through the station (one real sweep) ──
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.Sandbox && CN.S.cloud.ok !== null);
  await settle(stationOrigin);
  await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mFile', { state: 'attached' }); await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
  await page.evaluate(() => CN.setMode('design')); await page.evaluate(() => DesignLink.ensure());
  assert.strictEqual(await page.evaluate(() => DesignLink.state().sandbox), false, 'production first');
  // opening the session in production writes the bridge's own record; wait for it before the baseline, or a slow machine
  // lands that production-mode write after the count and it reads as a sandbox leak
  await page.waitForFunction(() => DesignLink.state().up !== undefined, null, { timeout: 20000 }).catch(() => {});
  await new Promise(r => setTimeout(r, 1500));
  const before = Object.fromEntries(PROD.map(c => [c, st.list(c).length])); const blobsBefore = new Set(st.blobs.keys());
  // one press does the chain: snapshot (the station is signed in), switch on, reload, pull from the copy
  await page.evaluate(() => CN.setMode('orders'));
  // the sandbox is a mode, not a control panel: it is switched on from Settings, one press for snapshot + switch + reload
  await page.evaluate(() => openSettings());
  await page.click('#stSbOn');
  await page.waitForFunction(() => !!(window.CN && window.B) && CN.S.settings.sandbox === 'on' && B.orders.rows.length > 0, null, { timeout: 120000 }).catch(() => {});
  const snap = st.doc('Charm_Sandbox', 'current');
  console.log('snapshot', snap);
  assert(snap && snap.count === receipts.length && /^charmnest\/sandbox\//.test(snap.path) && st.blobs.has(snap.path), 'the snapshot holds every open order in a sandbox file');
  const stored = JSON.parse(st.blobs.get(snap.path).buf.toString('utf8'));
  assert(stored.receipts.length === 3 && stored.receipts[1].transactions[0].variations.some(v => /Personalization/.test(v.formatted_name)), 'the snapshot keeps the raw Etsy shape with transactions and variations');
  const etsyBefore = st.calls.filter(c => /^(listOpenOrders|etsyOrderProxy|etsyImages|refreshEtsyToken)$/.test(c.name)).length;
  console.log('production counts before', before, 'etsy calls so far', etsyBefore);

  // ── the press switched the sandbox on, reloaded the sorter and pulled: the station is framed with ?sandbox=1 ──
  await page.waitForFunction(() => window.CN && window.Sandbox && CN.S.cloud.ok !== null && CN.S.settings.sandbox === 'on');
  // the pull that follows the switch is asynchronous: wait for it rather than reading the instant the switch lands
  await page.waitForFunction(() => !!(window.CN && window.B) && B.orders.rows.length >= 3 && CN.S.mode === 'orders', null, { timeout: 90000 })
    .catch(() => {});
  const pulled = await page.evaluate(() => ({ rows: B.orders.rows.length, mode: CN.S.mode }));
  assert(pulled.rows === 3 && pulled.mode === 'orders', 'after the switch the orders were pulled from the copy by themselves: ' + JSON.stringify(pulled));
  await settle(stationOrigin); await page.evaluate(() => { CN.S.settings.sandbox = 'on'; CN.saveSettings(); Sandbox.render(); });
  await page.evaluate(() => CN.setMode('design')); await page.evaluate(() => DesignLink.ensure());
  const hello = await page.evaluate(() => ({ sandbox: DesignLink.state().sandbox, etsy: DesignLink.state().etsy, pill: !document.getElementById('sandboxPill').classList.contains('hidden'), frame: document.getElementById('dsFrame').src }));
  console.log('sandbox hello', hello);
  assert(hello.sandbox === true && hello.etsy.sandbox === true && hello.etsy.signedIn && hello.pill && /sandbox=1/.test(hello.frame), 'the station runs in sandbox mode with the emulated Etsy: ' + JSON.stringify(hello));
  const frame = page.frames().find(f => f.url().startsWith(stationOrigin));
  assert(await frame.evaluate(() => !document.getElementById('sandboxBar').classList.contains('hidden')), 'the station shows its SANDBOX bar');
  const snapCalls = st.calls.length;
  const s2 = await page.evaluate(() => DesignLink.call('orders.snapshot', { hydrate: true, refresh: true, withNotes: false }));
  assert(s2.total === 3 && s2.hydrated === 3, 'the emulator serves the snapshot: ' + JSON.stringify({ total: s2.total, hydrated: s2.hydrated }));
  const since = st.calls.slice(snapCalls);
  assert(since.some(c => c.name === 'etsySandbox') && !since.some(c => /^(listOpenOrders|etsyOrderProxy|etsyImages|refreshEtsyToken)$/.test(c.name)), 'Etsy is emulated: ' + [...new Set(since.map(c => c.name))].join(','));

  // ── a whole Auto run in the sandbox ──
  await page.evaluate(() => CN.setMode('orders')); await page.evaluate(() => RunCtl.setMode('auto'));
  await page.waitForFunction(() => !!window.B && B.run && B.run.status !== undefined, null, { timeout: 10000 });
  let t0 = Date.now(), last = '';
  while (Date.now() - t0 < 600000) {
    const r = await page.evaluate(() => B.run && { status: B.run.status, step: B.run.step, stoppedBy: B.run.stoppedBy, fix: B.run.fix });
    if (r && r.step !== last) { last = r.step; console.log('  run', r.status, r.step); }
    if (r && r.status === 'complete') break;
    if (r && r.status === 'stopped') throw new Error(`run stopped: ${r.stoppedBy} — ${r.fix}\n` + (await page.evaluate(() => CN.AG.events.slice(-10).map(e => e.text))).join('\n'));
    if (r && r.status === 'review') await page.evaluate(async () => { for (const j of Engrave.items().values()) { if (j.state === 'words') await Engrave.decideWords(j, { text: j.text, by: 'Tester' }); else if (j.state === 'review') await Engrave.approve(j, 'Tester'); } });
    await page.waitForTimeout(700);
  }
  const run = await page.evaluate(() => ({ status: B.run.status, committed: B.run.committed, setId: B.run.setId }));
  console.log('run', run);
  assert(run.status === 'complete' && run.committed.length === 3, 'the sandbox run committed all three orders');

  // ── isolation: nothing real changed ──
  const after = Object.fromEntries(PROD.map(c => [c, st.list(c).length]));
  for (const c of PROD) assert.strictEqual(after[c], before[c], `production collection untouched: ${c} (${before[c]} → ${after[c]})`);
  const sandboxDocs = [...st.docs.keys()].filter(k => k.startsWith('Sandbox_'));
  const kinds = [...new Set(sandboxDocs.map(k => k.split('/')[0]))];
  console.log('sandbox collections written', kinds.join(', '));
  for (const need of ['Sandbox_Charm_Pool', 'Sandbox_Charm_Nest_Sets', 'Sandbox_Charm_Nest_Runs', 'Sandbox_Charm_Nest_Sheets', 'Sandbox_Design_Completed Orders', 'Sandbox_Design_RealTime_Selected_Orders']) assert(kinds.includes(need), 'sandbox copy written: ' + need);
  const newBlobs = [...st.blobs.keys()].filter(k => !blobsBefore.has(k));   // includes the snapshot itself, also under charmnest/sandbox/
  assert(newBlobs.length > 5 && newBlobs.every(k => k.startsWith('charmnest/sandbox/')), 'every new file is under charmnest/sandbox/: ' + newBlobs.filter(k => !k.startsWith('charmnest/sandbox/')).join(','));
  const etsyAfter = st.calls.filter(c => /^(listOpenOrders|etsyOrderProxy|etsyImages|refreshEtsyToken)$/.test(c.name)).length;
  assert.strictEqual(etsyAfter, etsyBefore, 'no real Etsy function was called during the sandbox run');
  const masterCount = st.list('Charm_Master_Index').length; assert(masterCount === 4, 'the shared master index is read, not rewritten');
  // the station's own ledger in the sandbox is separate: production orders are still open there
  await page.evaluate(() => DesignLink.call('orders.snapshot', { hydrate: false, withNotes: false })).then(s3 => assert.strictEqual(s3.total, 0, 'in the sandbox the three orders are complete now')).catch(e => { throw e; });
  // status, reset, and back to production
  const status = await page.evaluate(() => Sandbox.refresh());
  // two sets: the SS order and the GF orders share nothing, so each material is a set of its own
  assert(status.snapshot && status.records.Charm_Pool === 4 && status.records.Charm_Nest_Sets === 2 && status.records.Charm_Nest_Release === 0, 'sandbox status counts its own records: ' + JSON.stringify(status.records));
  const reset = await page.evaluate(() => CN.api('charmNestLibrary', { op: 'sandboxReset' }));
  assert(reset.deleted >= 10 && ![...st.docs.keys()].some(k => k.startsWith('Sandbox_')), 'reset removed every sandbox record');
  for (const c of PROD) assert.strictEqual(st.list(c).length, before[c], `reset left production alone: ${c}`);
  // a production sorter refuses a sandbox station and the other way round
  await page.evaluate(() => { CN.S.settings.sandbox = 'off'; CN.saveSettings(); });
  await page.reload(); await page.waitForFunction(() => window.CN && window.Sandbox && CN.S.cloud.ok !== null); await settle(stationOrigin);
  await page.evaluate(() => CN.setMode('design'));
  const prod = await page.evaluate(() => DesignLink.ensure().then(s => ({ sandbox: s.sandbox })));
  assert.strictEqual(prod.sandbox, false, 'back in production the station is framed without the sandbox');
  await page.screenshot({ path: path.join(tmp, 'sandbox.png') });
  const fatal = errors.filter(e => !/favicon|net::ERR|Failed to load resource|404/.test(e));
  assert.strictEqual(fatal.length, 0, 'no page errors: ' + fatal.join(' | '));
  console.log('sandbox OK · production untouched ·', sandboxDocs.length, 'sandbox docs ·', newBlobs.length, 'sandbox files');
  await browser.close(); srv.close();
})().catch(e => { console.error(e); process.exit(1); });
