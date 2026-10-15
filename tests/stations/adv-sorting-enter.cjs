// Adversarial (stations, 15 Oct 2026): Enter pressed twice on the Sorting Station's order field (sorting.html and sorting-2.html), with the real
// station-activity.js / station-session.js. Etsy answers slowly (400 ms an order), so the second Enter comes while the first batch is still loading.
//   1 · the same batch entered twice in a row is ONE scan per order in the efficiency record (it was two: the first, replaced run still recorded its scans)
//   2 · a batch replaced by a different one before it loaded is not counted; the batch that is on screen is
//   3 · the same batch entered again after it had loaded is a second scan (a deliberate "again", as before)
// Fakes only: every request off the fake origin is aborted; Etsy, Firebase, the printer and the door are the test's own.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/stations/adv-sorting-enter.cjs
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

const ORIGIN = 'http://sorting.test';
const A = '3700000001', B = '3700000002', C = '3700000003', D = '3700000009';   // D is not found
const PIN = '424242';
const day = Math.floor(Date.now() / 1000);
const QTY = { [A + '1']: 1, [A + '2']: 2 };         // order A has two lines, three pieces
const receipt = rid => ({
  receipt_id: Number(rid), status: 'Paid', message_from_buyer: '',
  transactions: (rid === A ? [1, 2] : [1]).map(n => ({ transaction_id: Number(rid) * 10 + n, receipt_id: Number(rid), listing_id: 1718000 + n,
    title: 'Silver Dog Charm Necklace', quantity: QTY[rid + n] || 1, variations: [{ formatted_name: 'Metal', formatted_value: 'Silver' }], expected_ship_date: day + 3 * 86400 }))
});
const firebaseStub = `
  window.__snap = { cb: null };
  const mk = d => ({ exists: !!d, data: () => d });
  window.firebase = { initializeApp() { return {}; }, firestore() { return { collection() { return { doc() { return {
    onSnapshot(cb) { window.__snap.cb = cb; setTimeout(() => cb(mk(window.__relayDoc)), 0); return () => {}; } }; } }; } }; } };
  window.__fireSnap = d => { window.__relayDoc = d; window.__snap.cb && window.__snap.cb(mk(d)); };`;
const materializeStub = `
  window.__toasts = [];
  const inst = { open() {}, close() {} };
  window.M = { AutoInit() {}, toast(o) { window.__toasts.push(o && o.html); }, Modal: { init() { return inst; }, getInstance() { return inst; } } };`;
// StationTimeline is the timeline's own business here: it only says B is cancelled (the page's guard and alert)
const stationStub = `
  const TL = new Set(${JSON.stringify([B])});
  window.StationTimeline = {
    init() {}, did() {}, isCancelled(id) { return TL.has(id); }, guard(id) { return Promise.resolve(!TL.has(id)); },
    scanned(id) { return new Promise(r => setTimeout(() => r(TL.has(id) ? { cancelled: true, record: { by: 'Paul', why: 'Buyer asked to cancel', at: Date.now() } } : { cancelled: false }), 30)); }
  };`;

const doorBodies = []; let mapGets = 0;
const wait = ms => new Promise(r => setTimeout(r, ms));
const iso = ms => new Date(Date.now() - ms).toISOString();
// the events the door received, flattened, with the raw requests kept for the "no PIN anywhere" check
function recorder() {
  const raw = [], events = [], sessions = [], lives = [];
  return {
    raw, events, sessions, lives,
    take(url, body) {
      raw.push(url + ' ' + body);
      let j = null; try { j = JSON.parse(body || 'null'); } catch (_) {}
      if (j && Array.isArray(j.activity)) events.push(...j.activity);
      if (j && j.session) sessions.push(j.session);
      if (j && j.live) lives.push(j.live);
    },
    ev: (action, filter) => events.filter(e => e.action === action && (!filter || filter(e)))
  };
}
const brief = e => [e.action, e.orderId, e.parts, e.orders, e.person, e.station, e.device];
// the live board (station-live-order.js): what the page said is in hand right now, one word per write
const liveBrief = (rec, device) => rec.lives.filter(l => l.event !== 'beat' && l.device === device)
  .map(l => l.event + ':' + (l.order ? l.order.kind + ':' + (l.order.rid || l.order.title) + ':' + l.order.pieces.length + ':' + l.order.note : l.ended.rid || l.ended.title));


(async () => {
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  for (const pg of [{ file: 'sorting.html', ready: 'window.SortTL' }, { file: 'sorting-2.html', ready: 'window.SortAct' }]) {
    const rec = recorder();
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', body });
    await ctx.route('**/*', async r => {
      const u = new URL(r.request().url());
      if (/gstatic\.com$/.test(u.hostname)) return r.fulfill(js(/firebase-app-compat/.test(u.pathname) ? firebaseStub : ''));
      if (/materialize/.test(u.pathname)) return r.fulfill(js(materializeStub));
      if (/jquery/.test(u.pathname)) return r.fulfill(js(fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : ''));
      if (u.origin !== ORIGIN) return r.abort();
      const p = decodeURIComponent(u.pathname);
      if (p === '/' + pg.file) return r.fulfill({ status: 200, contentType: 'text/html', body: fs.readFileSync(path.join(root, pg.file)) });
      if (['/order-timeline.js', '/station-session.js', '/station-activity.js', '/station-live-order.js'].includes(p)) return r.fulfill(js(fs.readFileSync(path.join(root, p.slice(1)))));
      if (p === '/station-timeline.js') return r.fulfill(js(stationStub));
      if (p === '/QR Printer.html') return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><title>printer stub</title>' });
      if (p === '/.netlify/functions/etsyOrderProxy') { await wait(400); return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(receipt(u.searchParams.get('orderId'))) }); }
      if (p === '/.netlify/functions/firebaseOrders') { rec.take(u.pathname + u.search, r.request().postData() || ''); return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, cancelled: {} }) }); }
      if (p === '/.netlify/functions/etsyImages') return r.fulfill({ status: 200, contentType: 'application/json', body: '[]' });
      if (p.startsWith('/.netlify/functions/')) return r.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      return r.fulfill({ status: 404, body: '' });
    });
    await ctx.addInitScript(() => { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('sorting.employee', 'Maya Sorter'); window.__relayDoc = null; });
    const page = await ctx.newPage(); const errors = []; page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${ORIGIN}/${pg.file}`);
    await page.waitForFunction(r => eval(r) && window.StationActivity && window.StationSession && document.querySelector('#sortingAsChip .st-as-name'), pg.ready, { timeout: 30000 });
    await page.waitForFunction(() => StationActivity.who());
    const scans = async () => { await page.evaluate(() => StationActivity.flush(true)); return rec.ev('scan').map(e => e.orderId); };
    const enter = async ids => { await page.fill('#etsyOrderNumber', ids.join(',')); await page.press('#etsyOrderNumber', 'Enter'); };
    const loaded = n => page.waitForFunction(n => (window.cachedOrderItems || []).length === n && document.getElementById('previewCell' + (n - 1)), n, { timeout: 20000 });

    // 1 · Enter twice in a row on one batch: one scan per order
    await enter([A, C]); await wait(150); await page.press('#etsyOrderNumber', 'Enter');
    await loaded(3); await wait(2500);
    assert.deepStrictEqual(await scans(), [A, C], pg.file + ': the same batch entered twice in a row is one scan per order');
    assert.strictEqual((await page.evaluate(() => window.cachedOrderItems.length)), 3, 'and the screen holds the batch once');

    // 2 · a different batch typed before the first had loaded: only the one on screen is counted
    rec.events.length = 0;
    await enter([A]); await wait(150); await enter([C]);
    await loaded(1); await wait(2500);
    assert.deepStrictEqual(await scans(), [C], pg.file + ': a batch replaced before it loaded is not counted; the one on screen is');

    // 3 · the same batch again after it had loaded: a second scan, as before
    rec.events.length = 0;
    await enter([C]); await loaded(1); await wait(800);
    await enter([C]); await loaded(1); await wait(800);
    assert.deepStrictEqual(await scans(), [C, C], pg.file + ': the same batch entered again after it loaded is a second scan');
    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log('  ok ' + pg.file + ': Enter twice = one scan per order; a replaced batch is not counted; entering a loaded batch again is a second scan');
    await ctx.close();
  }
  await browser.close();
  console.log('adv-sorting-enter: all passed');
})().catch(e => { console.error(e); process.exit(1); });
