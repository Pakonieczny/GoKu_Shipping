// Employee efficiency, Assembly (Paul, 2 Oct): assembly-1.html runs a fake order day (sign-in, scan, flag, QA1, Done) with the
// real station-session.js, station-activity.js, order-timeline.js and station-timeline.js. Firebase, Materialize and the Netlify
// functions are fakes and every request that is not to 127.0.0.1 is aborted. It proves the page emits each event once with the
// right person, station, device, order and pieces; nothing while signed out; and no PIN in any request.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/stations/assembly-activity.cjs
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
const PIN = String(100000 + Math.floor(Math.random() * 900000)).replace(/^(\d)\1{5}$/, '482913');   // a fake test PIN made up per run: only the login door's request may carry it
const NAME = 'Marco R.';
const O_TWO = '3812345678';                 // two lines: 2 + 1 pieces
const O_ONE = '3812345679';                 // one line: 3 pieces, SKU SOLO-9, arrives from the phone
const O_CANC = '3812345680';                // cancelled
const O_GONE = '3812345681';                // Etsy cannot find it
const O_FAIL = '3812345682';                // the Team message fails to send
const O_OUT = '3812345683';                 // used while signed out
const O_QA2 = '3812345684';                 // two pieces: a QA 2 check before and after the first assembled stamp, and a picture to the Team
const LINES = {
  [O_TWO]: [{ quantity: 2, sku: 'AAA-1' }, { quantity: 1, sku: 'BBB-2' }],
  [O_ONE]: [{ quantity: 3, sku: 'SOLO-9' }],
  [O_QA2]: [{ quantity: 2, sku: 'Q-1' }],
};
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
  const events = [], reqs = [], errors = [];
  try {
    const context = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    await context.route(() => true, r => r.abort());                                     // nothing leaves the machine
    await context.route(u => u.href.startsWith('http://127.0.0.1'), r => r.continue());
    await context.route(u => /gstatic\.com\/firebasejs\//.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: /firebase-app-compat/.test(r.request().url()) ? FIREBASE : '' }));
    await context.route(u => /materialize/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: MATERIALIZE }));
    await context.route(u => /code\.jquery\.com|qz-tray/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: '' }));
    await context.route(u => u.href.startsWith('http://127.0.0.1') && u.pathname.includes('/.netlify/functions/'), async r => {
      const req = r.request(), u = new URL(req.url()), fn = u.pathname.split('/').pop();
      const json = (o, status) => r.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(o) });
      reqs.push({ method: req.method(), url: req.url(), raw: req.postData() || '' });
      if (req.method() === 'POST') {
        let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {}
        if (fn === 'firebaseOrders' && b.pinLogin !== undefined) return json(b.pinLogin === PIN ? { ok: true, name: NAME } : { ok: false, error: 'not on the list' });   // the server's login door
        if (fn === 'firebaseOrders' && Array.isArray(b.activity)) { events.push(...b.activity); return json({ success: true, written: b.activity.length, duplicate: 0, refused: 0, scrubbed: 0 }); }
        if (fn === 'firebaseOrders' && b.newMessage && b.orderNumber === O_FAIL) return json({ success: false, error: 'down' }, 500);
        return json({ success: true });
      }
      if (fn === 'etsyOrderProxy') {
        const id = u.searchParams.get('orderId');
        if (id === O_GONE) return json({ error: 'not found' }, 404);
        return json({ receipt_id: Number(id) || 0, status: id === O_CANC ? 'Paid' : 'Paid', transactions: LINES[id] || [] });
      }
      if (fn === 'firebaseOrders' && u.searchParams.get('cancelCheck')) {
        const out = {};
        for (const id of u.searchParams.get('cancelCheck').split(',')) if (id === O_CANC) out[id] = { at: Date.now() - 3600e3, by: 'Sam', why: 'Buyer cancelled', source: 'sheet' };
        return json({ success: true, cancelled: out, now: Date.now() });
      }
      if (fn === 'firebaseOrders' && /employee/i.test(u.searchParams.get('orderId') || '')) return json({ success: false, error: 'closed' }, 401);   // the roster is never read
      if (fn === 'firebaseOrders') return json({ success: true, data: {} });
      return json({});
    });
    await context.addInitScript(() => {          // an Etsy token so the page does not start OAuth; nobody is signed in
      try { localStorage.setItem('access_token', 'test-token'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400)); } catch (_) {}
    });
    const page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
    await page.goto(base + '/assembly-1.html');
    await page.waitForFunction(() => window.StationActivity && window.StationSession && window.OrderTimeline && window.StationTimeline, null, { timeout: 20000 });
    await page.waitForTimeout(500);

    const send = async text => {
      await page.fill('#britesMsgInput', text);
      await page.evaluate(() => document.getElementById('goScreenTwoBtn').click());
      await page.waitForFunction(() => document.getElementById('britesMsgInput').value === '', null, { timeout: 5000 }).catch(() => {});
      await page.waitForTimeout(150);
    };
    const open = async (id, how) => {            // an order typed in (Enter), or scanned by the phone (the relay doc changes)
      if (how === 'phone') {
        await page.evaluate(n => { const cbs = window.__fbSnaps['Brites_Orders/assembly-scan-1']; cbs[cbs.length - 1]({ exists: true, data: () => ({ 'Order Number': n }) }); }, id);
        await page.waitForFunction(n => document.getElementById('etsyOrderNumber').value === n, id, { timeout: 5000 });
      } else {
        await page.fill('#etsyOrderNumber', id); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter');
      }
      await page.waitForTimeout(700);           // the lookup, the cancel check and the grid
    };
    const flush = () => page.evaluate(() => window.StationActivity.flush());
    const drop = async ok => {                  // a picture dropped into the order chat (the upload is a stub that works, or fails)
      await page.evaluate(async ok => {
        window.uploadViaResumable = async () => { if (!ok) throw new Error('storage down'); return 'http://127.0.0.1/x.png'; };
        const dt = new DataTransfer(); dt.items.add(new File(['x'], 'a.png', { type: 'image/png' }));
        document.getElementById('customerMessageHistory').dispatchEvent(new DragEvent('drop', { dataTransfer: dt, bubbles: true, cancelable: true }));
      }, ok);
      await page.waitForTimeout(300);
    };
    const mine = () => events.filter(e => e.device === 'assembly-1');
    const by = (a, id) => mine().filter(e => e.action === a && (id == null || e.orderId === id));

    /* nobody is signed in: an order opened and a Team stamp send nothing */
    await check('signed out: an order opened and a Team stamp record no activity', async () => {
      await open(O_OUT);
      await send('QA1');
      await flush();
      assert.equal(mine().length, 0, JSON.stringify(mine()));
      assert.equal(await page.evaluate(() => window.StationActivity.pending()), 0);
    });

    /* sign in with the PIN box, as a person does */
    await page.evaluate(p => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = p; i.value = '******'; }, PIN);
    await page.evaluate(() => document.getElementById('employeeLoginBtn').click());
    await page.waitForFunction(() => window.isEmployeeLoggedIn === true && window.StationActivity.who(), null, { timeout: 8000 });
    await page.waitForFunction(() => (window.__fbSnaps['Brites_Orders/assembly-scan-1'] || []).length > 0);
    await page.waitForTimeout(300);

    await open(O_TWO);                                                       // typed
    await send('please check the chain length');                             // a plain message
    await send('wrong chain, needs rework');                                  // a flag
    await send('QA1');                                                       // the completing stamp
    await send('Done');                                                      // a second stamp for the same order
    await open(O_ONE, 'phone');                                              // from the phone scanner
    await send('Done');
    await open(O_CANC);
    await page.evaluate(() => { const b = document.querySelector('.sttl-ok'); if (b) b.click(); });
    await open(O_GONE);
    await send('QA1').catch(() => {});                                       // (for O_GONE: a stamp with no loaded order)
    await page.fill('#etsyOrderNumber', O_FAIL);
    await send('Done');                                                      // the Team message fails to send
    await open(O_QA2);                                                       // QA 2: a check of finished work is a note, never a second completion
    await send('QA 2');                                                      // before anyone's first assembled stamp: a note
    await send('qa2 ok');                                                    // another spelling: a note
    await send('QA1');                                                       // the first assembled stamp: the completion
    await send('QA 2');                                                      // a check after it: a note
    await send('QA3');                                                       // a later check: a note too
    await send('Done 2');                                                    // (2 is only a number here, no QA stage): a stamp again, a note
    await drop(true); await drop(false);                                     // a picture to the Team (a note), one that fails (an error)
    await flush();

    await check('every event is this person at this station and device, with a session and a computer, and ids are unique', async () => {
      assert(mine().length >= 10, 'events: ' + mine().length);
      for (const e of mine()) {
        assert.equal(e.person, NAME); assert.equal(e.station, 'assembly'); assert.equal(e.device, 'assembly-1');
        assert(/^pc-/.test(e.computer), 'computer ' + e.computer); assert(e.session, 'session'); assert(e.at > 0 && e.sincePrevMs >= 0);
      }
      assert.equal(new Set(mine().map(e => e.id)).size, mine().length, 'duplicate ids');
    });
    await check('an order typed in is one scan with its piece count (2 + 1 = 3), no SKU for a two-line order', async () => {
      const s = by('scan', O_TWO); assert.equal(s.length, 1);
      assert.equal(s[0].parts, 3); assert.equal(s[0].sku, ''); assert.equal(s[0].detail, 'typed'); assert.equal(s[0].orders, 0);
    });
    await check('the phone scan is one scan on the desktop that receives it, with the single line SKU and 3 pieces', async () => {
      const s = by('scan', O_ONE); assert.equal(s.length, 1);
      assert.equal(s[0].parts, 3); assert.equal(s[0].sku, 'SOLO-9'); assert.equal(s[0].detail, 'phone scan');
    });
    await check('QA1 completes the order once: 1 order, 3 pieces; the later Done for the same order is a note, not a second completion', async () => {
      const c = by('complete', O_TWO); assert.equal(c.length, 1);
      assert.equal(c[0].orders, 1); assert.equal(c[0].parts, 3); assert.match(c[0].detail, /^Team stamp QA1/);
      const n = by('note', O_TWO).filter(e => /stamp again/.test(e.detail)); assert.equal(n.length, 1); assert.equal(n[0].orders, 0); assert.equal(n[0].parts, 0);
    });
    await check('Done on the phone-scanned order completes it with its SKU and 3 pieces', async () => {
      const c = by('complete', O_ONE); assert.equal(c.length, 1);
      assert.equal(c[0].orders, 1); assert.equal(c[0].parts, 3); assert.equal(c[0].sku, 'SOLO-9');
    });
    await check('a plain message is a note and a rework message is a reject, neither with the message text', async () => {
      const notes = by('note', O_TWO).filter(e => e.detail === 'message to Team'); assert.equal(notes.length, 1);
      const rej = by('reject', O_TWO); assert.equal(rej.length, 1); assert.equal(rej[0].detail, 'flag to Team: wrong');
      for (const e of [...notes, ...rej]) assert(!/chain|rework/.test(e.detail.replace('flag to Team: wrong', '')), 'text leaked: ' + e.detail);
    });
    await check('a QA 2 stamp is a note with fixed words, no pieces and no order, never a completion; QA1 still completes once, with its 2 pieces', async () => {
      const notes = by('note', O_QA2).filter(e => /^QA \d+ check$/.test(e.detail));
      assert.deepEqual(notes.map(e => e.detail), ['QA 2 check', 'QA 2 check', 'QA 2 check', 'QA 3 check']);
      for (const n of notes) { assert.equal(n.parts, 0); assert.equal(n.orders, 0); assert.equal(n.sku, ''); }
      const c = by('complete', O_QA2); assert.equal(c.length, 1, JSON.stringify(c));
      assert.equal(c[0].orders, 1); assert.equal(c[0].parts, 2); assert.match(c[0].detail, /^Team stamp QA1/);
      assert.equal(mine().filter(e => e.action === 'complete' && /QA ?[2-9]/i.test(e.detail)).length, 0, 'no QA 2 completion anywhere');
      const again = by('note', O_QA2).filter(e => /stamp again: Done 2/.test(e.detail)); assert.equal(again.length, 1);
      const total = mine().filter(e => e.action === 'complete' && e.orderId === O_QA2).reduce((s, e) => s + e.orders, 0); assert.equal(total, 1, 'the order is counted once');
    });
    await check('a picture to the Team is a note and a failed one an error, never the picture', async () => {
      const ok = by('note', O_QA2).filter(e => e.detail === 'image to Team'); assert.equal(ok.length, 1); assert.equal(ok[0].parts, 0); assert.equal(ok[0].orders, 0);
      const bad = by('error', O_QA2); assert.equal(bad.length, 1); assert.equal(bad[0].detail, 'Team image not sent');
    });
    await check('a cancelled order is one scan and one reject', async () => {
      assert.equal(by('scan', O_CANC).length, 1);
      const r = by('reject', O_CANC); assert.equal(r.length, 1); assert.equal(r[0].detail, 'cancelled order alert');
    });
    await check('an order Etsy cannot find is one scan and one error; a failed Team send is one error and no completion', async () => {
      assert.equal(by('scan', O_GONE).length, 1);
      const e1 = by('error', O_GONE); assert.equal(e1.length, 1); assert.equal(e1[0].detail, 'order lookup failed');
      const e2 = by('error', O_FAIL); assert.equal(e2.length, 1); assert.equal(e2[0].detail, 'Team stamp not sent');
      assert.equal(by('complete', O_FAIL).length, 0);
    });
    await check('the stamp for an order whose lookup failed still completes it once (pieces unknown, 0)', async () => {
      const c = by('complete', O_GONE); assert.equal(c.length, 1); assert.equal(c[0].orders, 1); assert.equal(c[0].parts, 0);
      assert.match(c[0].detail, /pieces unknown/);
    });

    /* sign out: more scans and stamps send nothing */
    const before = mine().length;
    await page.evaluate(() => document.getElementById('signOutBtn').click());
    await open(O_OUT);
    await send('QA1');
    await flush();
    await check('after Sign Out an order opened and a stamp record nothing', async () => {
      assert.equal(mine().length, before);
      assert.equal(by('scan', O_OUT).length, 0);
      assert.equal(await page.evaluate(() => window.StationActivity.pending()), 0);
    });
    await check('the PIN is in no request but the login door\'s: not in a URL, not in any other body; the roster is never read', async () => {
      for (const q of reqs) { assert(!q.url.includes(PIN), 'PIN in a URL: ' + q.url.replace(PIN, '*')); assert(!q.raw.includes(PIN) || q.raw === JSON.stringify({ pinLogin: PIN }), 'PIN in a body to ' + q.url.split('/').pop()); }
      assert(reqs.some(q => q.raw === JSON.stringify({ pinLogin: PIN })), 'the sign-in did not use the login door');
      assert(!reqs.some(q => /employee/i.test(q.url)), 'the roster document was read');
      assert(reqs.some(q => /"activity"/.test(q.raw)), 'no activity request was made');
    });
    await check('no page error from the activity wiring', async () => assert.deepEqual(errors.filter(m => /StationActivity|actLog|actScan|actStamp|actMessage|actDone|actLoaded/.test(m)), []));
    if (errors.length) console.log('(page errors, not from this wiring:', errors.join(' | ').slice(0, 300) + ')');
    await context.close();
  } finally { await browser.close(); server.close(); }

  await check('assembly-2..4 carry the same wiring (identical to assembly-1 apart from their name), and each loads station-activity.js after station-session.js', async () => {
    const norm = s => s.replace(/assembly-(scan-)?[1-4]/g, 'assembly-$1N').replace(/Assembly_[1-4]/g, 'Assembly_N');
    const one = norm(fs.readFileSync(path.join(root, 'assembly-1.html'), 'utf8'));
    for (const n of [1, 2, 3, 4]) {
      const t = fs.readFileSync(path.join(root, `assembly-${n}.html`), 'utf8');
      assert.equal(norm(t) === one, true, `assembly-${n}.html differs`);
      assert(/station-session\.js\?v=[\w-]+"><\/script>\s*(<!--[\s\S]*?-->\s*)?<script src="station-activity\.js\?v=[\w-]+"><\/script>/.test(t), `assembly-${n}.html: station-activity.js is not right after station-session.js`);
      assert(t.includes(`device: "assembly-${n}"`), `assembly-${n}.html: device`);
    }
  });
}

main().catch(e => results.push(['setup', e])).finally(() => {
  let bad = 0;
  for (const [name, e] of results) { if (e) { bad++; console.log('FAIL', name, '\n   ', String(e && e.stack || e).split('\n').slice(0, 4).join('\n    ')); } else console.log('ok  ', name); }
  console.log(`${results.length - bad}/${results.length} passed`);
  process.exit(bad ? 1 : 0);
});
