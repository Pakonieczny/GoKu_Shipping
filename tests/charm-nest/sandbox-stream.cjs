// The sandbox order stream: instead of the whole snapshot at once, the emulated Etsy lists 2 to 5 new orders per simulated
// ten minutes, each a copy of a random open snapshot receipt under fresh numbers and the simulated time, and the sorter
// plays that day at 50x — a check every 12 s — while its next check waits for it to finish taking in the last. This test
// replays a short day against the real sorter, station, emulator and library code, and proves: the counts and the
// randomness per step, fresh numbers, simulated timestamps and lead times, the clock held while the sorter is busy, the
// replay repeated by its seed after a reset (pressed while a check is out, with orders finished at the station), and
// that nothing lands in a production collection, file or Etsy call.
//   node tests/charm-nest/sandbox-stream.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');

const STEP = 600000, day = Math.floor(Date.now() / 1000);
// six open orders, each with a lead time of its own (2…7 days to ship) so a copy shows which one it came from, and one
// shipped and one cancelled order that must never be streamed
const GF = '14k Gold Filled', SS = 'Sterling Silver';
const tx = (rid, i, sku, metal, created, ship, extra = {}) => Object.assign({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, create_timestamp: created, created_timestamp: created, paid_timestamp: created, expected_ship_date: ship, shipped_timestamp: null, variations: [{ formatted_name: 'Metal', formatted_value: metal }], is_personalized: false }, extra);
const receipt = (rid, hoursAgo, leadDays, lines, extra = {}) => { const created = day - hoursAgo * 3600, ship = created + leadDays * 86400; return Object.assign({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', create_timestamp: created, created_timestamp: created, update_timestamp: created + 60, updated_timestamp: created + 60, status: 'Paid', is_paid: true, is_shipped: false, transactions: lines.map(([sku, metal], i) => tx(rid, i + 1, sku, metal, created, ship)) }, extra); };
const snapshot = [
  receipt(3521000101, 30, 2, [['BR-TST-01', GF]]),
  receipt(3521000102, 26, 3, [['BR-TST-02', SS], ['BR-TST-03', SS]]),
  receipt(3521000103, 20, 4, [['BR-TST-03', GF]]),
  receipt(3521000104, 12, 5, [['BR-TST-04', GF], ['BR-TST-05', GF]]),
  receipt(3521000105, 6, 6, [['BR-TST-05', SS]]),
  receipt(3521000106, 2, 7, [['BR-TST-06', GF], ['BR-TST-01', SS]]),
  receipt(3521000107, 40, 8, [['BR-TST-02', GF]], { is_shipped: true, status: 'Completed' }),
  receipt(3521000108, 40, 9, [['BR-TST-04', SS]], { is_canceled: true, status: 'Canceled' })
];
const OPEN_LEADS = [2, 3, 4, 5, 6, 7];
const leadOf = r => Math.round((r.transactions[0].expected_ship_date - r.create_timestamp) / 86400);
const PROD = ['Design_Completed Orders', 'Design_RealTime_Selected_Orders', 'Design_Order_Archive', 'Brites_Orders', 'Brites_Messages', 'Charm_Pool', 'Charm_Pool_Back', 'Charm_Nest_Sets', 'Charm_Nest_Counters', 'Charm_Nest_Runs', 'Charm_Nest_Sheets', 'Charm_Nest_Release', 'Charm_Nest_Arrivals', 'Design_Bridge'];
const REAL_ETSY = /^(listOpenOrders|etsyOrderProxy|etsyImages|refreshEtsyToken)$/;

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-stream-'));
  const masterPath = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(masterPath, { count: 6, edge: false });
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  const emulator = st.handlers.etsySandbox;
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
  const settle = (extra) => page.evaluate(([station, extra]) => { const s = CN.S.settings; Object.assign(s, { dsOrigin: station, engine: 'solver', budgetS: 12, review: 'off', naming: 'off', packingAI: 'off', notify: 'off', sound: 'off', autoCommit: 'on', runMode: 'manual', pullMode: 'all', heartbeatS: 2, heartbeatMiss: 2 }, extra || {}); CN.saveSettings(); }, [stationOrigin, extra]);
  const booted = () => page.waitForFunction(() => window.CN && window.Sandbox && window.Arrivals && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  const stream = () => st.doc('Charm_Sandbox', 'stream') || null;
  const rows = () => page.evaluate(() => B.orders.rows.map(r => ({ rid: String(r.order.receiptId), tx: String(r.line.transactionId), createTs: r.order.createTs, updateTs: r.order.updateTs, shipBy: r.order.shipBy, expected: r.line.expectedShipDate, arrivedAt: r.arrivedAt, sku: r.line.sku })));

  // ── production: index the master (shared, read-only in the sandbox), then store the snapshot the emulator serves ──
  await page.goto(`${sorterOrigin}/charm-nest-1.html`); await booted(); await settle();
  await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mFile', { state: 'attached' }); await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
  assert.strictEqual(await page.evaluate(() => SimClock.on()), false, 'production has no simulated clock');
  // ── the library op and the emulator on their own: sandbox only, one step per check, a stale step never taken twice ──
  const lib = b => st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
  const etsy = q => emulator.handler({ httpMethod: 'GET', queryStringParameters: q }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
  assert.strictEqual((await lib({ op: 'sandboxStream', action: 'ensure' })).status, 403, 'no order stream outside the sandbox');
  assert.strictEqual((await lib({ op: 'sandboxStream', action: 'ensure', sandbox: true })).status, 409, 'no order stream without a snapshot');
  const snapPath = 'charmnest/sandbox/orders-stream-test.json', snapAt = Date.now();
  st.blobs.set(snapPath, { buf: Buffer.from(JSON.stringify({ at: snapAt, count: snapshot.length, receipts: snapshot })), generation: 1, meta: { contentType: 'application/json', metadata: {} } });
  st.put('Charm_Sandbox', 'current', { path: snapPath, count: snapshot.length, at: snapAt, takenBy: 'test' });
  let op = await lib({ op: 'sandboxStream', action: 'ensure', sandbox: true, seed: 777 });
  assert(op.status === 200 && op.body.stream.seed === 777 && op.body.stream.tick === 0, 'a seed given in Settings starts the stream');
  assert.strictEqual((await etsy({ fn: 'listOpenOrders' })).body.results.length, 0, 'a new stream lists nothing yet');
  op = await lib({ op: 'sandboxStream', action: 'tick', sandbox: true, expect: op.body.stream.simNow });
  assert(op.body.advanced && op.body.stream.tick === 1 && op.body.stream.simNow === op.body.stream.simStart + STEP, 'one check, one step of ten simulated minutes');
  const stale = await lib({ op: 'sandboxStream', action: 'tick', sandbox: true, expect: op.body.stream.simNow - STEP });
  assert(!stale.body.advanced && stale.body.stream.tick === 1, 'a step asked for from a stale clock (a second tab) is not taken twice');
  const listed = (await etsy({ fn: 'listOpenOrders' })).body.results, step1 = emulator.batch(op.body.stream, snapshot, 1, st.doc('Charm_Sandbox', 'current'));
  assert.deepStrictEqual(listed.map(x => x.receipt_id), step1.map(x => x.receipt_id).reverse(), 'the emulator lists the arrived orders, newest first as Etsy does');
  assert(listed.every(x => x.transactions.length && x.is_paid && !x.is_shipped), 'with their transactions, open and paid');
  const alone = await etsy({ fn: 'etsyOrderProxy', orderId: String(listed[0].receipt_id) });
  assert(alone.status === 200 && alone.body.transactions.length === listed[0].transactions.length && !alone.body.receipt.transactions, 'a streamed order also reads on its own');
  const status = (await etsy({ fn: 'status' })).body; assert(status.stream && status.stream.tick === 1 && status.stream.arrived === step1.length && status.count === snapshot.length);
  await lib({ op: 'sandboxStream', action: 'off', sandbox: true });
  assert.strictEqual((await etsy({ fn: 'listOpenOrders' })).body.results.length, 6, 'stream off: the whole open snapshot at once again');
  const offTick = await lib({ op: 'sandboxStream', action: 'tick', sandbox: true, expect: op.body.stream.simNow });
  assert(!offTick.body.stream && !offTick.body.advanced && !st.doc('Charm_Sandbox', 'stream').on, 'a stream switched off is not stepped (or switched on) by a check');
  op = await lib({ op: 'sandboxStream', action: 'ensure', sandbox: true, seed: 5 });
  assert(op.body.stream.seed === 777 && op.body.stream.tick === 1 && op.body.stream.on, 'switched back on, it resumes where it was; a new seed waits for a reset');
  await lib({ op: 'sandboxStream', action: 'reset', sandbox: true }); assert(!st.doc('Charm_Sandbox', 'stream'), 'reset removes the stream');
  const orphan = await lib({ op: 'sandboxStream', action: 'tick', sandbox: true, expect: op.body.stream.simNow });
  assert(orphan.status === 200 && !orphan.body.stream && !orphan.body.advanced && !st.doc('Charm_Sandbox', 'stream'), 'a step asked of a deleted stream (a check still out at a reset) starts none');
  await new Promise(r => setTimeout(r, 1500));
  const before = new Map([...st.docs.entries()].map(([k, v]) => [k, JSON.stringify(v)])), blobsBefore = new Set(st.blobs.keys()), callsBefore = st.calls.length;
  const prodBefore = Object.fromEntries(PROD.map(c => [c, st.list(c).length]));

  // ── the sandbox, streaming at 50x (the defaults), Manual mode: the orders come by themselves, a few per check ──
  await settle({ sandbox: 'on', sandboxStream: 'on', sandboxSpeed: 50, sandboxSeed: 0 });
  await page.evaluate(() => sessionStorage.setItem('cn.sandboxAutoPull', '1'));
  const t0 = Date.now(); await page.reload(); await booted();
  await page.waitForFunction(() => Sandbox.stream() && SimClock.on() && CN.S.mode === 'orders', null, { timeout: 20000 });
  const s0 = stream(); console.log('stream started', s0);
  assert(s0 && s0.on && s0.tick === 0 && s0.min === 2 && s0.max === 5 && s0.stepMs === STEP && s0.seed > 0 && s0.snapshotPath === snapPath, 'a fresh stream starts at step 0 with a random seed');
  assert(Math.abs(s0.simStart - Math.floor(t0 / STEP) * STEP) <= STEP, 'the simulated day starts at the real ten minutes it began in');
  assert.strictEqual((await rows()).length, 0, 'no pull of the whole snapshot: the stream starts empty');
  const meta = st.doc('Charm_Sandbox', 'current');
  const expectStep = (s, k) => emulator.batch(s, snapshot, k, meta);
  const seen = { toasts: new Set(), counter: '', pill: '' };
  const watch = async () => { const v = await page.evaluate(() => ({ toasts: [...document.querySelectorAll('#toasts > *')].map(n => n.textContent), counter: document.getElementById('arrivalCounter')?.textContent || '', pill: document.getElementById('sandboxPill').textContent })); v.toasts.forEach(t => seen.toasts.add(t)); seen.counter = v.counter; seen.pill = v.pill; return v; };
  const ticks = [];
  for (const t = Date.now(); Date.now() - t < 150000;) {
    const s = stream(); if (s.tick && (!ticks.length || ticks[ticks.length - 1].tick !== s.tick)) ticks.push({ tick: s.tick, at: Date.now() });
    await watch();
    const have = new Set((await rows()).map(r => r.rid)), want = Array.from({ length: s.tick }, (_, i) => expectStep(s, i + 1)).flat();
    if (s.tick >= 4 && want.every(r => have.has(String(r.receipt_id)))) break;
    await page.waitForTimeout(300);
  }
  const s1 = stream(), got = await rows();
  console.log('after', s1.tick, 'steps:', got.length, 'lines ·', seen.counter, '·', seen.pill);
  assert(s1.tick >= 4, 'the stream stepped at least four times in the time four checks take at 50x: ' + s1.tick);
  const period = (ticks[ticks.length - 1].at - ticks[0].at) / (ticks[ticks.length - 1].tick - ticks[0].tick); console.log('real seconds per simulated ten minutes', (period / 1000).toFixed(1));
  assert(period > 10000 && period < 16000, 'a check every simulated ten minutes: about 12 s apart at 50x');
  const byRid = new Map(); for (const r of got) { if (!byRid.has(r.rid)) byRid.set(r.rid, []); byRid.get(r.rid).push(r); }
  const snapIds = new Set(snapshot.map(r => String(r.receipt_id))), snapTx = new Set(snapshot.flatMap(r => r.transactions.map(t => String(t.transaction_id))));
  const leads = new Set();
  for (let k = 1; k <= s1.tick; k++) {
    const step = expectStep(s1, k), from = (s1.simStart + (k - 1) * STEP) / 1000, to = (s1.simStart + k * STEP) / 1000;
    assert(step.length >= 2 && step.length <= 5, `step ${k} brings 2 to 5 orders: ${step.length}`);
    for (const r of step) {
      const rid = String(r.receipt_id), mine = byRid.get(rid);
      assert(mine, `step ${k}: order ${rid} reached the sorter`);
      assert(!snapIds.has(rid) && mine.every(l => !snapTx.has(l.tx)), 'fresh receipt and transaction numbers');
      assert(mine.every(l => l.createTs > from && l.createTs <= to && l.updateTs === l.createTs), `step ${k}: created at the simulated arrival time, inside its ten minutes`);
      const lead = Math.round((mine[0].expected - mine[0].createTs) / 86400); leads.add(lead);
      assert(OPEN_LEADS.includes(lead) && Math.abs(mine[0].expected - mine[0].createTs - lead * 86400) < 1, `the ship date keeps the original's lead time (${lead} days)`);
      assert(mine.every(l => l.arrivedAt >= l.createTs * 1000 && l.arrivedAt <= s1.simNow + STEP), 'the arrival is stamped on the simulated clock');
    }
  }
  assert.strictEqual(byRid.size, new Set(got.map(r => r.rid)).size); assert.strictEqual(new Set(got.map(r => r.tx)).size, got.length, 'every line number is unique');
  assert([...byRid.keys()].every(rid => Array.from({ length: s1.tick }, (_, i) => expectStep(s1, i + 1)).flat().some(r => String(r.receipt_id) === rid)), 'nothing but streamed orders reached the sorter');
  // randomness, deterministic in the seed: over many seeds every count from 2 to 5 and every open order turn up, never a shipped or cancelled one
  const counts = new Set(), sources = new Set();
  for (let seed = 1; seed <= 40; seed++) for (let k = 1; k <= 6; k++) { const b = expectStep(Object.assign({}, s1, { seed }), k); counts.add(b.length); b.forEach(r => sources.add(leadOf(r))); }
  assert.deepStrictEqual([...counts].sort(), [2, 3, 4, 5], 'the counts per step vary over the whole range');
  assert.deepStrictEqual([...sources].sort((a, b) => a - b), OPEN_LEADS, 'every open snapshot order is copied, and no shipped or cancelled one');
  assert.strictEqual(JSON.stringify(expectStep(s1, 2)), JSON.stringify(expectStep(Object.assign({}, s1), 2)), 'the same seed and step give the same orders');
  assert.notStrictEqual(JSON.stringify(expectStep(s1, 2).map(r => r.receipt_id)), JSON.stringify(expectStep(Object.assign({}, s1, { seed: s1.seed + 1 }), 2).map(r => r.receipt_id)), 'another seed gives other orders');
  // the illusion: production's toasts and counter, with the speed and simulated time; the sorter's day is the simulated one
  assert([...seen.toasts].some(t => /new order\(s\) arrived/.test(t)), 'the new-order toast shows as in production: ' + [...seen.toasts].join(' | '));
  assert.match(seen.counter, /^Sandbox 50x · sim [A-Z][a-z]{2} \d{2}:\d{2} · Orders received · 24h \d+ · 1h \d+ · /, 'the counter shows the speed and simulated time');
  assert.match(seen.pill, /^Sandbox 50x · sim [A-Z][a-z]{2} \d{2}:\d{2}$/, 'the pill shows the speed and simulated time');
  const form = await page.evaluate(() => { openSettings(); const v = { stream: $('#stSbStream').value, speed: $('#stSbSpeed').value, seed: $('#stSbSeed').placeholder, status: $('#stSbStatus').textContent }; closeDlg($('#dlgSettings')); return v; });
  assert(form.stream === 'on' && +form.speed === 50 && form.seed.includes(String(s1.seed)) && /^Order stream: seed \d+ · step \d+ · simulated .+ · 50x$/.test(form.status), 'Settings show the stream, its speed and its seed: ' + JSON.stringify(form));
  const clock = await page.evaluate(() => ({ sim: SimClock.now(), real: Date.now(), today: today(), simDay: CharmNestOrders.localDay(new Date(SimClock.now())) }));
  assert(clock.sim > clock.real + 20 * 60000 && clock.sim <= stream().simNow + STEP, 'the simulated clock runs ahead of the real one, never past the next step: ' + JSON.stringify(clock));
  assert.strictEqual(clock.today, clock.simDay, "the sorter's day is the simulated day");
  const ledger = st.list('Sandbox_Charm_Nest_Arrivals');
  assert(ledger.length === byRid.size && ledger.every(d => d.firstSeenAt >= d.createTs * 1000 && d.firstSeenAt <= s1.simNow + STEP) && Math.max(...ledger.map(d => d.firstSeenAt)) > clock.real, 'the sandbox arrivals ledger counts in simulated time');
  const sweeps = st.calls.slice(callsBefore).filter(c => c.name === 'etsySandbox' && c.q.fn === 'listOpenOrders').length;
  assert(sweeps >= s1.tick, 'the station swept the emulator on every check (no reuse window while streaming): ' + sweeps);

  // ── Auto mode: the run takes the orders in, and the clock waits for it every time ──
  await page.evaluate(() => { window.__advances = []; const real = Sandbox.advance; Sandbox.advance = (...a) => { __advances.push({ at: Date.now(), held: Arrivals.held(), status: B.run && B.run.status, busy: !!(B.run && B.run.arrivalBusy), pending: !!Arrivals.state().pending, nesting: allSheets().filter(p => ['nesting', 'finishing', 'queued'].includes(p.status)).length }); return real(...a); }; });
  const tickAuto = stream().tick;
  await page.evaluate(() => RunCtl.setMode('auto'));
  let sawRunning = false, heldSeen = new Set(), rest = null;
  for (const t = Date.now(); Date.now() - t < 420000;) {
    const r = await page.evaluate(() => ({ run: B.run && { status: B.run.status, step: B.run.step, busy: !!B.run.arrivalBusy, stoppedBy: B.run.stoppedBy, fix: B.run.fix }, held: Arrivals.held(), counter: document.getElementById('arrivalCounter').textContent, n: __advances.length }));
    if (r.run && r.run.status === 'running') sawRunning = true;
    if (r.held) heldSeen.add(r.held);
    if (r.run && r.run.status === 'stopped') throw new Error(`run stopped: ${r.run.stoppedBy} — ${r.run.fix}\n` + (await page.evaluate(() => CN.AG.events.slice(-12).map(e => e.text))).join('\n'));
    if (r.run && r.run.status === 'review') await page.evaluate(async () => { for (const j of Engrave.items().values()) { if (j.state === 'words') await Engrave.decideWords(j, { text: j.text, by: 'Tester' }); else if (j.state === 'review') await Engrave.approve(j, 'Tester'); } });
    if (sawRunning && r.n >= 3 && r.run && ['processed', 'complete', 'review'].includes(r.run.status) && !r.run.busy && !r.held) { rest = r; break; }
    await page.waitForTimeout(500);
  }
  const advances = await page.evaluate(() => __advances);
  console.log('auto:', stream().tick - tickAuto, 'steps · held for:', [...heldSeen].join(' / '), '· at rest:', rest && rest.run.status);
  assert(rest, 'the Auto run took in several steps of arrivals and came to rest');
  assert(sawRunning && heldSeen.size > 0, 'the clock was held while the sorter worked');
  assert(advances.length >= 3 && advances.every(a => !a.held && a.status !== 'running' && !a.busy && !a.nesting), 'the stream only stepped while the sorter was free: ' + JSON.stringify(advances.filter(a => a.held || a.status === 'running' || a.busy || a.nesting)));
  const taken = await page.evaluate(() => { const ids = new Set((B.run.orders || []).map(String)); return { rows: B.orders.rows.length, inRun: B.orders.rows.filter(r => ids.has(String(r.order.receiptId))).length, pooled: B.orders.rows.filter(r => r.poolIds.length).length, placed: allSheets().reduce((n, p) => n + p.placements.length, 0) }; });
  console.log('taken in', taken);
  assert(taken.rows > 0 && taken.inRun === taken.rows && taken.pooled === taken.rows && taken.placed >= taken.rows, 'the run took every streamed order in and nested it: ' + JSON.stringify(taken));
  const streamedIds = s => new Set(Array.from({ length: s.tick }, (_, i) => expectStep(s, i + 1)).flat().map(r => String(r.receipt_id)));
  const autoIds = streamedIds(stream());
  assert((await rows()).every(r => autoIds.has(r.rid)), 'nothing but streamed orders reached the sorter in Auto either');
  // hold by hand: while arrivals are being added the clock stops at the next step; once they are in, one step, ten minutes
  await page.evaluate(() => { B.run.arrivalBusy = true; });
  await page.waitForFunction(() => !/Checking/.test(document.getElementById('arrivalCounter').textContent), null, { timeout: 60000 });
  await page.waitForTimeout(500);
  const hold0 = stream();
  await page.waitForTimeout(26000);
  const during = await page.evaluate(() => ({ counter: document.getElementById('arrivalCounter').textContent, sim: SimClock.now() }));
  assert.strictEqual(stream().tick, hold0.tick, 'no step while the sorter is still adding arrivals (two check intervals passed)');
  assert.match(during.counter, /waiting for the sorter: adding the last arrivals/, 'the counter says why it waits');
  assert(during.sim <= hold0.simNow + STEP, 'the simulated clock waits at the next step');
  await page.evaluate(() => { B.run.arrivalBusy = false; });
  for (const t = Date.now(); Date.now() - t < 180000 && stream().tick === hold0.tick;) await new Promise(r => setTimeout(r, 200));
  const hold1 = stream();
  assert(hold1.tick === hold0.tick + 1 && hold1.simNow === hold0.simNow + STEP, 'released, the stream steps once: ten simulated minutes');

  // ── isolation: nothing outside the sandbox's own records, files and emulator ──
  const changed = [...st.docs.entries()].filter(([k, v]) => before.get(k) !== JSON.stringify(v)).map(([k]) => k);
  // Claude's job queue (Charm_Nest_Agent, charmnest/agent/) is shared scratch, not an order record, and predates the stream
  const outside = changed.filter(k => !k.startsWith('Sandbox_') && !k.startsWith('Charm_Sandbox/') && !k.startsWith('Charm_Nest_Agent/'));
  assert.deepStrictEqual(outside, [], 'every record written is a sandbox record');
  for (const c of PROD) assert.strictEqual(st.list(c).length, prodBefore[c], `production collection untouched: ${c}`);
  const newBlobs = [...st.blobs.keys()].filter(k => !blobsBefore.has(k)), stray = newBlobs.filter(k => !k.startsWith('charmnest/sandbox/') && !k.startsWith('charmnest/agent/'));
  assert.deepStrictEqual(stray, [], 'every new file is under charmnest/sandbox/');
  assert(!st.calls.slice(callsBefore).some(c => REAL_ETSY.test(c.name)), 'no real Etsy function was called');
  assert(st.calls.slice(callsBefore).filter(c => c.op === 'sandboxStream').every(c => c.body.sandbox === true), 'the stream is only ever asked for in the sandbox');

  // ── reset starts the stream over; with its seed in Settings the replay brings the same orders ──
  const seed = stream().seed, first = expectStep(stream(), 1).map(r => String(r.receipt_id));
  // a commit marks orders finished at the station, which keeps them in a browser ledger of its own as well: two of the
  // first step's are marked there as a commit would, so the replay shows whether the reset reached that ledger too
  await page.frames().find(f => /design-1\.html/.test(f.url())).evaluate(ids => { ids.forEach(id => completedOrders.add(id)); return persistCompleted(ids); }, first.slice(0, 2));
  const finished = st.list('Sandbox_Design_Completed Orders').map(d => d._id);
  assert(first.slice(0, 2).every(id => finished.includes(id)), 'the station recorded the orders as finished');
  await page.evaluate(() => RunCtl.setMode('manual'));
  await page.waitForFunction(() => !B.run || !['running'].includes(B.run.status) && !B.run.arrivalBusy, null, { timeout: 120000 });
  await settle({ sandbox: 'on', sandboxStream: 'on', sandboxSpeed: 50, sandboxSeed: seed });
  const reloaded = page.waitForEvent('load', { timeout: 90000 });
  // pressed while a check is out: that check's step goes with the stream it stepped, and no sweep lists the whole snapshot
  const midCheck = await page.evaluate(() => new Promise((res, rej) => { const t0 = Date.now(), t = setInterval(() => { const out = /Checking/.test(document.getElementById('arrivalCounter').textContent); if (out || Date.now() - t0 > 40000) { clearInterval(t); Sandbox.reset().then(() => res(out), rej); } }, 10); }));
  assert(midCheck, 'the reset was pressed while an arrivals check was out');
  assert(!stream() && !st.list('Sandbox_Charm_Nest_Arrivals').length && !st.list('Sandbox_Charm_Nest_Runs').length, 'reset removed the stream with the sandbox records');
  await reloaded; await booted();
  let replay = [];
  for (const t = Date.now(); Date.now() - t < 60000;) { replay = await rows(); const have = new Set(replay.map(r => r.rid)), s = stream(); if (s && s.tick >= 1 && first.every(id => have.has(id))) break; await page.waitForTimeout(400); }
  const s2 = stream(), replayIds = streamedIds(s2), replayLedger = st.list('Sandbox_Charm_Nest_Arrivals').map(d => d._id);
  console.log('replay seed', s2.seed, 'step', s2.tick, 'orders', [...new Set(replay.map(r => r.rid))].join(','));
  assert(s2.seed === seed && s2.tick >= 1 && s2.simStart >= s1.simStart, 'the replay is a new stream with the recorded seed');
  assert.deepStrictEqual([...new Set(replay.filter(r => first.includes(r.rid)).map(r => r.rid))].sort(), first.slice().sort(), 'the replay brings the same first orders');
  assert(replay.every(r => replayIds.has(r.rid)) && replayLedger.every(id => replayIds.has(id)), 'and nothing else: no order of the whole snapshot, nor of the step the reset cut off: ' + [...new Set(replay.map(r => r.rid))].filter(id => !replayIds.has(id)).concat(replayLedger.filter(id => !replayIds.has(id))).join(','));
  // two of those numbers were finished before the reset: the station's own browser ledger of finished orders forgot them
  const station =page.frames().find(f => /design-1\.html/.test(f.url()));
  const stationLedger = await station.evaluate(() => Object.keys(JSON.parse(localStorage.getItem('designCompletedLedger.v1:sandbox') || '{}')));
  console.log('finished before the reset:', finished.length, '· in the first step:', first.filter(id => finished.includes(id)).join(',') || 'none', '· left in the station ledger:', stationLedger.length);
  assert(!stationLedger.some(id => finished.includes(id)), 'the station forgot the orders it had finished before the reset');
  assert.strictEqual(await page.evaluate(() => Arrivals.state().lastAdded >= 2 && Object.keys(Arrivals.state().seen).length === new Set(B.orders.rows.map(r => String(r.order.receiptId))).size), true, 'the arrivals count started over');

  // ── back to production: nothing of the stream runs (the sandbox page stops checking first, so its last calls are not counted) ──
  await page.evaluate(() => { CN.S.settings.pollOrders = 'off'; localStorage.setItem('cn.settings', JSON.stringify(Object.assign({}, CN.S.settings, { sandbox: 'off', pollOrders: 'on' }))); });
  await page.waitForTimeout(3000);
  const callsProd = st.calls.length;
  await page.reload(); await booted(); await settle(); await page.evaluate(() => CN.setMode('design')); await page.evaluate(() => DesignLink.ensure());
  await page.waitForTimeout(3000);
  const prod = await page.evaluate(() => ({ sim: SimClock.on(), skew: Math.abs(SimClock.now() - Date.now()), today: today(), real: CharmNestOrders.localDay(), pill: document.getElementById('sandboxPill').classList.contains('hidden'), label: Sandbox.label(), counter: document.getElementById('arrivalCounter').textContent, next: Arrivals.state().nextCheck - Date.now(), frame: document.getElementById('dsFrame').src, stored: localStorage.getItem('cn.simClock.sandbox') }));
  console.log('production', prod);
  assert(!prod.sim && prod.skew < 1000 && prod.today === prod.real && prod.pill && prod.label === '' && !/Sandbox/.test(prod.counter), 'production runs on the real clock with no sandbox label');
  assert(prod.next > 9 * 60000 && /sandbox=0/.test(prod.frame), 'production checks every ten minutes, through the real station');
  assert(!st.calls.slice(callsProd).some(c => c.op === 'sandboxStream' || c.name === 'etsySandbox' || c.body.sandbox === true), 'production makes no sandbox call');
  const fatal = errors.filter(e => !/favicon|net::ERR|Failed to load resource|404/.test(e));
  assert.strictEqual(fatal.length, 0, 'no page errors: ' + fatal.join(' | '));
  console.log(`sandbox stream OK · ${s1.tick} steps in Manual, ${hold1.tick - tickAuto} in Auto at 50x · seed ${seed} replayed · production untouched`);
  await browser.close(); srv.close();
})().catch(e => { console.error(e); process.exit(1); });
