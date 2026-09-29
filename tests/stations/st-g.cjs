// Station tracking, part G (Paul, 28 Sep 23:51): the Assembly stations record `assembled` with the person signed in on the
// station's own PIN login when the assembly step is completed: the Team's done stamp ("QA1", "Done", "Assembled") sent from
// the station's chat. Runs assembly-1.html with the real order-timeline.js and station-timeline.js; Firebase, Materialize
// and the Netlify functions are fakes, and every request that is not to 127.0.0.1 is aborted.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/stations/st-g.cjs
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const results = [];
async function check(name, fn) { try { await fn(); results.push([name, null]); } catch (e) { results.push([name, e]); } }

const FIREBASE = `(function () {
  const snaps = window.__fbSnaps = {};
  const empty = () => ({ exists: false, data: () => undefined, docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] });
  function ref(p) {
    const r = { path: p, id: p.split('/').pop(), collection: n => ref(p + '/' + n), doc: n => ref(p + '/' + n),
      where: () => r, orderBy: () => r, limit: () => r, limitToLast: () => r, startAfter: () => r,
      onSnapshot(cb) { (snaps[p] = snaps[p] || []).push(cb); setTimeout(() => { try { cb(empty()); } catch (_) {} }, 0); return () => {}; },
      get: async () => empty(), set: async () => {}, update: async () => {}, add: async () => ref(p + '/new'), delete: async () => {} };
    return r;
  }
  const firestore = () => ({ collection: n => ref(n), doc: p => ref(p), batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }) });
  firestore.FieldValue = { delete: () => ({}), serverTimestamp: () => ({}), arrayUnion: (...a) => a, increment: n => n };
  firestore.Timestamp = { now: () => ({ toDate: () => new Date(), toMillis: () => Date.now() }) };
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; }, currentUser: { uid: 'anon' } });
  window.firebase = { apps: [], initializeApp() { return {}; }, firestore, auth, storage: () => ({ ref: () => ({}) }) };
})();`;
const MATERIALIZE = `window.M = { AutoInit() {}, updateTextFields() {}, toast() {},
  Modal: { init(el) { const i = { open() {}, close() {}, isOpen: false }; if (el) el.__m = i; return i; }, getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
  FormSelect: { init(el) { const i = { destroy() {}, getSelectedValues: () => [el && el.value] }; if (el) el.__fs = i; return i; }, getInstance(el) { return el && el.__fs; } },
  Dropdown: { init() {} } };`;
const PIN = '123456';
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const server = await new Promise(ok => { const s = http.createServer((req, res) => {
    const f = path.join(root, decodeURIComponent(req.url.split('?')[0]));
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
  }).listen(0, '127.0.0.1', () => ok(s)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM, args: ['--no-sandbox'] });
  const posts = [], errors = [];
  try {
    const context = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    await context.route(() => true, r => r.abort());                                     // nothing leaves the machine
    await context.route(u => u.href.startsWith('http://127.0.0.1'), r => r.continue());
    await context.route(u => /gstatic\.com\/firebasejs\//.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: /firebase-app-compat/.test(r.request().url()) ? FIREBASE : '' }));
    await context.route(u => /materialize/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: MATERIALIZE }));
    await context.route(u => /code\.jquery\.com|qz-tray/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: '' }));
    await context.route(u => u.href.startsWith('http://127.0.0.1') && u.pathname.includes('/.netlify/functions/'), async r => {
      const req = r.request(), u = new URL(req.url()), fn = u.pathname.split('/').pop();
      const json = o => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(o) });
      if (req.method() === 'POST') posts.push({ fn, raw: req.postData() || '' });
      if (fn === 'etsyOrderProxy') return json({ receipt_id: Number(u.searchParams.get('orderId')) || 0, status: 'Paid', transactions: [] });
      if (fn === 'firebaseOrders' && u.searchParams.get('cancelCheck')) return json({ success: true, cancelled: {} });
      if (fn === 'firebaseOrders') return json({ success: true, data: {} });
      return json({});
    });
    await context.addInitScript(p => {
      try { if (!sessionStorage.getItem('__seeded')) {
        localStorage.setItem('employee_id', p); localStorage.setItem('employee_name', 'Marco R.');
        localStorage.setItem('access_token', 'test-token'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400));
        sessionStorage.setItem('__seeded', '1');
      } } catch (_) {}
    }, PIN);
    const page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
    await page.goto(base + '/assembly-1.html');
    await page.waitForFunction(() => window.OrderTimeline && window.StationTimeline && (window.__fbSnaps['Brites_Orders/assembly-scan-1'] || []).length > 0, null, { timeout: 20000 });
    await page.evaluate(() => { window.__ev = []; OrderTimeline.onRecord(e => window.__ev.push(e)); });
    const events = type => page.evaluate(t => window.__ev.filter(e => e.type === t), type);
    const send = async text => {
      await page.fill('#britesMsgInput', text);
      await page.evaluate(() => document.getElementById('goScreenTwoBtn').click());
      await page.waitForFunction(() => document.getElementById('britesMsgInput').value === '', null, { timeout: 5000 });
    };
    await page.fill('#etsyOrderNumber', '3812345678');

    await check('an ordinary chat message is not a completion', async () => {
      await send('please check the chain length');
      assert.equal((await events('assembled')).length, 0);
    });
    await check('the Team stamp "QA1" records assembled by the signed-in person, at Assembly, with a stable id', async () => {
      await send('QA1');
      const [e] = await events('assembled');
      assert(e, 'no assembled event');
      assert.equal(e.by, 'Marco R.'); assert.equal(e.station, 'assembly'); assert.equal(e.device, 'assembly-1');
      assert.equal(e.orderId, '3812345678');
      assert.match(e.id, /^assembly-1-3812345678-assembled-\d+$/);
      assert.deepEqual(e.data, { message: 'QA1' });
    });
    await check('the same stamp again within the minute keeps the same id (one seal)', async () => {
      await send('QA1 done');
      const all = await events('assembled');
      assert.equal(all.length, 2); assert.equal(all[1].id, all[0].id);
    });
    await check('after Sign Out a stamp says "not signed in": by "" and signedIn false, never the last name', async () => {
      await page.evaluate(() => document.getElementById('signOutBtn').click());
      await page.fill('#etsyOrderNumber', '3812345679');
      await send('Done');
      const e = (await events('assembled')).pop();
      assert.equal(e.orderId, '3812345679'); assert.equal(e.by, ''); assert.equal(e.data.signedIn, false);
    });
    await check('a scan after Sign Out is not put on the last person either', async () => {
      await page.fill('#etsyOrderNumber', '3812345680'); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter');
      await page.waitForFunction(() => window.__ev.some(e => e.type === 'scan' && e.orderId === '3812345680'), null, { timeout: 10000 });
      const e = (await events('scan')).find(x => x.orderId === '3812345680');
      assert.equal(e.by, '');
    });
    await check('the events reach the station door (firebaseOrders {timeline}), and the PIN is never sent with them', async () => {
      await page.waitForFunction(() => OrderTimeline.pending() === 0, null, { timeout: 10000 });
      const tl = posts.filter(p => p.fn === 'firebaseOrders' && p.raw.includes('"timeline"'));
      assert(tl.some(p => p.raw.includes('"assembled"') && p.raw.includes('Marco R.')), 'assembled was posted');
      for (const p of tl) assert(!p.raw.includes(`"${PIN}"`) && !/employee_?id/i.test(p.raw), 'a timeline post carried the PIN');
    });
    await check('no page error from the timeline wiring', async () => assert.deepEqual(errors.filter(m => /StationTimeline|OrderTimeline|getEmployee|isAssemblyStamp|signedIn/.test(m)), []));
    if (errors.length) console.log('(page errors, not from this wiring:', errors.join(' | ').slice(0, 300) + ')');
    await context.close();
  } finally { await browser.close(); server.close(); }

  await check('assembly-2..4 carry the same wiring (identical to assembly-1 apart from their name)', async () => {
    const norm = s => s.replace(/assembly-(scan-)?[1-4]/g, 'assembly-$1N').replace(/Assembly_[1-4]/g, 'Assembly_N');
    const one = norm(fs.readFileSync(path.join(root, 'assembly-1.html'), 'utf8'));
    for (const n of [2, 3, 4]) assert.equal(norm(fs.readFileSync(path.join(root, `assembly-${n}.html`), 'utf8')) === one, true, `assembly-${n}.html differs`);
  });
}

main().catch(e => results.push(['setup', e])).finally(() => {
  let bad = 0;
  for (const [name, e] of results) { if (e) { bad++; console.log('FAIL', name, '\n   ', String(e && e.stack || e).split('\n').slice(0, 4).join('\n    ')); } else console.log('ok  ', name); }
  console.log(`${results.length - bad}/${results.length} passed`);
  process.exit(bad ? 1 : 0);
});
