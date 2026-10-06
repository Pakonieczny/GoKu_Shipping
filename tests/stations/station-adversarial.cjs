// Adversarial test of the station-side reporting (the live board, the activity events and the sessions): a real browser running the
// real station-session.js, station-activity.js and station-live-order.js against the REAL server code (the open door in firebaseOrders.js
// and the live reader in employeeEfficiency.js) over an in-memory Firestore. Nothing real is touched: no network but a loopback server,
// no Firebase, no Etsy, no PIN. Every attack below must leave the page working (never throws, never blocks or slows a scan or a print),
// the numbers right (nothing counted twice, a person's order off the board when the work is done) and nothing private on the wire.
//   PRIVACY    a PIN typed as a name is nobody; a PIN-sized order id, sku, listing id, label, note, customer or detail is dropped or masked;
//              every request body of every scenario is scanned at the end (no 4 to 8 digit number, no lone 6-digit run, no e-mail)
//   CLOCKS     a computer whose clock is 5 minutes slow or fast still shows the true time since the scan (client sentAt + server skew)
//   FLOOD      100 scans in 10 s cost about 10 live writes and the last order wins; 100 of 100 events stored once; each call under 100 ms
//   FAILURES   500, 429, a dropped connection, a 4 s answer, a network that goes away and comes back: calls stay instant, retries are
//              bounded (backoff), nothing is lost, the page recovers by itself
//   LEAVING    a hidden tab keeps its order and sends what is queued; a page that is closed or navigated away sends its idle (beacon)
//   PEOPLE     two tabs of one station and person (one document, no double count); sign-out mid-order; two people on one device
//              (the first one's order is not the second one's); a name with a PIN in it
//   NIGHT      the New York midnight sign-out ends the order; a page left open ends it after its hold; a key press every 10 minutes
//              costs the same as working, never more
//   COST       one hour: idle = 0 live writes, working = about 115 live + 12 session writes (about 125 per person, as documented)
//   SIZE       a 300-line order, a 600-piece line, 24 unicode labels: one request under the byte limit, accepted
//   SANDBOX    a sandbox page writes only Sandbox_ documents, a real page never does, a sandbox leftover in storage stays sandbox
//   WIRING     every page that loads the helpers loads them in order with ONE ?v= tag per file, and is a station the console lists
//   node tests/stations/station-adversarial.cjs [playwright-core dir]     (NODE_PATH/PW_DIR as the other station tests)
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), Module = require('module'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

/* ── an in-memory Firestore for the REAL door and reader ── */
const INC = n => ({ __inc: n }), TS = { __ts: true };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && v.__inc == null && !v.__ts;
function apply(prev, data, merge) {
  const out = merge && prev ? JSON.parse(JSON.stringify(prev)) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v && v.__inc != null) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = 'TS';
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge);
    else out[k] = v;
  }
  return out;
}
const store = () => {
  const colls = new Map(), reads = [], writes = [];
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const clone = v => v == null ? v : JSON.parse(JSON.stringify(v));
  const notFound = () => Object.assign(new Error('5 NOT_FOUND: No document to update'), { code: 5 });
  function query(name, filters, order, lim, sel) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
      limit: n => query(name, filters, order, n, sel),
      select: (...f) => query(name, filters, order, lim, f),
      get: async () => {
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || typeof x !== typeof v) return false;
          return op === '==' ? x === v : op === '>=' ? x >= v : op === '>' ? x > v : op === '<' ? x < v : op === '<=' ? x <= v : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (p.d[f] < q.d[f] ? -1 : p.d[f] > q.d[f] ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push({ name, n: docs.length });
        return { docs: docs.map(({ id, d }) => ({ id, data: () => clone(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const ref = (name, id) => ({ id, name, path: name + '/' + id,
    get: async () => { reads.push({ name, doc: id }); const d = data(name).get(id); return { exists: !!d, id, data: () => clone(d), ref: ref(name, id) }; },
    set: async (v, o) => { writes.push([name, id, 'set']); data(name).set(id, apply(data(name).get(id), clone(v), !!(o && o.merge))); },
    update: async v => { writes.push([name, id, 'update']); if (!data(name).has(id)) throw notFound(); data(name).set(id, apply(data(name).get(id), clone(v), true)); } });
  const db = {
    collection: name => Object.assign(query(name, [], null, null, null), { doc: id => ref(name, id) }),
    getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
    runTransaction: async fn => {
      const q = [];
      const out = await fn({ get: r => r.get(), getAll: (...rs) => Promise.all(rs.map(r => r.get())), set: (r, v, o) => q.push([r, v, o]) });
      for (const [r, v, o] of q) await r.set(v, o);
      return out;
    }
  };
  return { db, all: name => [...data(name)].map(([id, d]) => Object.assign({ _id: id }, d)), writesOf: name => writes.filter(w => w[0] === name),
    colls, count: name => data(name).size, reset: () => { reads.length = 0; writes.length = 0; } };
};
let cur = store();
const dbNow = { collection: n => cur.db.collection(n), getAll: (...a) => cur.db.getAll(...a), runTransaction: f => cur.db.runTransaction(f) };
const fakeAdmin = { firestore: Object.assign(() => dbNow, { FieldValue: { serverTimestamp: () => TS, increment: INC, delete: () => null }, Timestamp: { fromMillis: m => m } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const L = require(path.join(root, 'netlify/functions/_stationLive.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;
const PASS = 'synthetic-pass-9f3k';
process.env.EDIT_PASSCODE = PASS;

/* the server's clock is the test's: `clock.off` follows the fake time of the pages */
const realNow = Date.now.bind(Date);
const clock = { off: 0 };
Date.now = () => realNow() + clock.off;
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 8000) { const t0 = realNow(); for (;;) { const v = await fn(); if (v) return v; if (realNow() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(30); } }
async function step(page, ms, chunk = 5000, settle = 30) {      // fake time forward (pages and server together), the network answering in between
  let left = ms;
  while (left > 0) { const d = Math.min(chunk, left); clock.off += d; await page.clock.runFor(d); left -= d; await wait(settle); }
}
const live = () => cur.all('Station_Live');
const nowS = () => realNow() + clock.off;
const fresh = () => { cur = store(); EP.resetCache(); L._t.seen.clear(); clock.off = 0; return cur; };
const ask = async (body = {}) => {
  const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.7' }, body: JSON.stringify(Object.assign({ op: 'live', key: PASS }, body)) }, cur.db);
  return { status: r.statusCode, body: JSON.parse(r.body || '{}') };
};

/* ── the lab page (a station page in miniature) and one loopback server per window ── */
const LAB = `<!doctype html><html><head><meta charset="utf-8"><title>lab</title></head><body>
<script src="/station-session.js"></script><script src="/station-activity.js"></script><script src="/station-live-order.js"></script>
<script>
(function () {
  const q = new URLSearchParams(location.search);
  window.__who = q.get('who') || '';
  StationSession.init({ station: q.get('station') || 'shipping', device: q.get('device') || 'shipping-1', sandbox: q.get('sandbox') === '1',
    person: () => window.__who ? { name: window.__who } : null, signOut: () => { window.__who = ''; } });
  window.signIn = n => { window.__who = n; StationSession.signedIn({ name: n }); };
  window.signOutNow = () => { StationSession.signedOut('signOut'); window.__who = ''; };
})();
</script></body></html>`;
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };
const ALL = [];                       // every request body any window sent to the station doors, with the scenario that sent it
let TEST = '';
let ipN = 0;
async function site(browser, extra) {
  const net = { ip: '198.51.100.' + (++ipN % 250), log: [], behave: () => 'ok' };
  const srv = http.createServer((req, res) => {
    const u = new URL(req.url, 'http://x'), p = decodeURIComponent(u.pathname);
    if (p.startsWith('/.netlify/functions/')) {
      const chunks = []; req.on('data', c => chunks.push(c));
      req.on('end', async () => {
        try {
          const text = Buffer.concat(chunks).toString('utf8');
          let body = null; try { body = JSON.parse(text); } catch (_) {}
          const kind = body && body.live ? 'live' : body && Array.isArray(body.activity) ? 'activity' : body && body.session ? 'session' : 'other';
          const rec = { test: TEST, at: realNow(), kind, body, text, sandbox: u.searchParams.get('sandbox') === '1', method: req.method, status: null };
          ALL.push(rec); net.log.push(rec);
          if (net.closed) { rec.status = 0; return req.socket.destroy(); }       // a window that is closing (its pagehide beacons) writes nothing into the next scenario's store
          const b = net.behave(rec) || 'ok';
          if (b === 'abort') { rec.status = 0; return req.socket.destroy(); }
          if (b === 'hang') { rec.status = -1; return; }
          if (typeof b === 'number') { rec.status = b; res.writeHead(b, { 'Content-Type': 'application/json' }); return res.end('{"error":"x"}'); }
          if (b && b.slow) await wait(b.slow);
          const r = await door.handler({ httpMethod: req.method, headers: { 'x-nf-client-connection-ip': net.ip }, queryStringParameters: rec.sandbox ? { sandbox: '1' } : {}, body: text });
          rec.status = r.statusCode; rec.out = r.body;
          res.writeHead(r.statusCode, { 'Content-Type': 'application/json' }); res.end(r.body);
        } catch (_) { try { res.destroy(); } catch (_2) {} }
      });
      return;
    }
    if (p === '/lab.html') { res.writeHead(200, { 'Content-Type': 'text/html' }); return res.end(LAB); }
    const file = path.join(root, p);
    if (!file.startsWith(root) || !MIME[path.extname(file)] || !fs.existsSync(file) || !fs.statSync(file).isFile()) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] });
    fs.createReadStream(file).pipe(res);
  });
  await new Promise(r => srv.listen(0, '127.0.0.1', r));
  const ctx = await browser.newContext({ viewport: { width: 1200, height: 800 } });
  if (extra && extra.offlineSwitch) await ctx.addInitScript(() => { window.__online = true; Object.defineProperty(Navigator.prototype, 'onLine', { get: () => window.__online, configurable: true }); });
  return { ctx, net, origin: `http://127.0.0.1:${srv.address().port}`,
    async close() { net.closed = true; try { await ctx.close(); } catch (_) {} await wait(150); try { srv.closeAllConnections(); } catch (_) {} srv.close(); } };
}
async function lab(c, o = {}) {
  const page = await c.ctx.newPage();
  page.errs = []; page.on('pageerror', e => page.errs.push(e.message));
  await page.goto(`${c.origin}/lab.html?station=${o.station || 'shipping'}&device=${o.device || 'shipping-1'}${o.sandbox ? '&sandbox=1' : ''}${o.who ? '&who=' + encodeURIComponent(o.who) : ''}`);
  await page.waitForFunction(() => window.StationSession && window.StationActivity && window.StationLiveOrder);
  return page;
}
const TX = [{ transaction_id: 9100000001, listing_id: 155510010, title: 'Pet Portrait', quantity: 2, sku: 'PET-ST' }];
const ORDER = '3812300001';
const kinds = (c, k) => c.net.log.filter(r => r.kind === k);

/* ── the privacy scan: every string in every body ── */
const OPAQUE = new Set(['id', 'session', 'computer', 'computerId', 'device', 'rid']);        // ids the code minted (rid is checked on its own: an order id)
function strings(v, key, out) {
  if (typeof v === 'string') out.push([key, v]);
  else if (Array.isArray(v)) for (const x of v) strings(x, key, out);
  else if (v && typeof v === 'object') for (const [k, x] of Object.entries(v)) strings(x, k, out);
  return out;
}
function privacyProblems(rec) {
  const bad = [];
  if (!rec.body) return bad;
  if (/"(pin|passcode|password|token)"\s*:/i.test(rec.text) || /"employeeId"\s*:\s*"[^"]/.test(rec.text)) bad.push('a pin/passcode/employeeId/token field');   // (the session's employeeId is always "": no page passes an id)
  if (/[\w.+-]+@[\w-]+\.[a-z]{2,}/i.test(rec.text)) bad.push('an e-mail address');
  for (const [k, s] of strings(rec.body, '', [])) {
    if (/^\d+$/.test(s)) { if (s.length >= 4 && s.length <= 8) bad.push(`${k}: a digits-only ${s.length}-digit value`); }
    if (!OPAQUE.has(k) && /(?<!\d)\d{6}(?!\d)/.test(s)) bad.push(`${k}: a lone 6-digit number (${s.slice(0, 40)})`);
  }
  return bad;
}

const results = [];
async function check(name, fn) {
  if (process.env.ONLY && !name.includes(process.env.ONLY)) return;               // ONLY="failure offline" runs one scenario
  TEST = name.split(' ')[0]; try { await fn(); results.push([name, null]); } catch (e) { results.push([name, e]); }
}

async function main() {
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  try {

    /* ───────── PRIVACY ───────── */
    await check('privacy: a PIN typed as a name is nobody; PIN-sized ids, notes, labels and details never reach the wire', async () => {
      fresh(); const c = await site(browser);
      const page = await lab(c);
      await page.evaluate(() => signIn('482913'));
      await page.evaluate(t => { StationLiveOrder.start('3812300001', t, { note: 'x' }); StationActivity.log('scan', { orderId: '3812300001', parts: 1 }); }, TX);
      await wait(500);
      assert.equal(await page.evaluate(() => StationActivity.who()), null, 'a name that is only digits is nobody');
      assert.equal(c.net.log.length, 0, 'and nothing is sent for them: ' + c.net.log.map(r => r.kind).join());
      await page.evaluate(() => signIn('Marco 482913'));
      assert.equal(await page.evaluate(() => StationActivity.who().person), 'Marco', 'a PIN inside a name is cut out');
      await page.evaluate(() => {
        const T = [{ transaction_id: 9100000001, listing_id: 482913, title: '482913', quantity: 1, sku: '482913' },
                   { transaction_id: 9100000002, listing_id: 155510010, title: 'Pet Portrait 482913 charm', quantity: 2, sku: 'PET-1' }];
        StationLiveOrder.start('482913', T, { note: '482913', customer: '482913' });                       // an order id the size of a PIN is not an order
        StationLiveOrder.start('3812300001', T, { note: 'label 482913 printed', customer: '482913' });
        StationLiveOrder.sheet('k482913', '482913', { note: '482913' });                                      // a sheet whose title is only digits: nothing in hand
        StationActivity.log('scan', { orderId: '482913', sku: '482913', detail: '482913', parts: 3 });
        StationActivity.log('scan', { orderId: '3812300001', sku: '48291345', detail: 'typed 482913', parts: 3 });
        StationActivity.log('print', { orderId: '3812300001', detail: 'label 482913' });
      });
      await page.evaluate(() => StationActivity.flush()); await wait(800);
      const d = live().find(x => x.person === 'Marco');
      assert(d && d.rid === ORDER && d.state === 'working', 'the real order is in hand: ' + JSON.stringify(d));
      assert.equal(d.note, 'label [#] printed', 'a 6-digit run in a note is masked');
      assert(!d.customer, 'a digits-only customer is dropped: ' + d.customer);
      assert(d.pieces.every(p => !p.listingId || p.listingId.length > 8) && d.pieces[0].label === '', 'a digits-only label and listing id are dropped');
      const wire = c.net.log.map(r => r.text).join('\n');
      assert(!wire.includes('482913'), 'the PIN-like number is nowhere on the wire');
      const ev = cur.all('Station_Activity');
      assert(ev.length === 3 && ev.every(e => e.person === 'Marco' && !/482913/.test(JSON.stringify(e))), 'the activity events carry none of it: ' + JSON.stringify(ev.map(e => [e.orderId, e.sku, e.detail])));
      assert.deepEqual(page.errs, []);
      await c.close();
    });

    /* ───────── CLOCKS ───────── */
    await check('clock: a computer 5 minutes slow or fast still shows the true time since the scan', async () => {
      for (const skew of [-300000, 300000]) {
        fresh();
        const c = await site(browser);
        await c.ctx.clock.install({ time: realNow() + skew });
        const page = await lab(c);
        await page.evaluate(() => signIn('Tess Welder'));
        await page.clock.pauseAt(realNow() + skew + 2000);
        await page.evaluate(t => StationLiveOrder.start('3812300001', t, { note: '' }), TX);
        await step(page, 100, 50, 150);
        await until(() => live().length, 'the live document');
        const d0 = live()[0];
        assert(Math.abs(d0.scannedAt - nowS()) < 3000, `${skew / 1000}s: the stored scan time is the true one (off by ${d0.scannedAt - nowS()} ms)`);
        await step(page, 125000);
        const d1 = live()[0], since = (nowS() - d1.scannedAt) / 1000;
        assert(since > 118 && since < 135, `${skew / 1000}s: 125 s later the order shows ${since} s since the scan (the refresh keeps the scan time)`);
        assert.equal(d1.state, 'working');
        assert.deepEqual(page.errs, []);
        await c.close();
      }
    });

    /* ───────── FLOOD ───────── */
    await check('flood: 100 scans in 10 s cost about 10 live writes, the last order wins, all 100 events stored once, no call is slow', async () => {
      fresh(); const c = await site(browser);
      const page = await lab(c);
      await page.evaluate(() => signIn('Tess Welder'));
      await wait(300);
      let worst = 0;
      const t0 = realNow();
      for (let i = 0; i < 100; i++) {
        const rid = String(3812300100 + i);
        const ms = await page.evaluate(([rid, t]) => { const a = performance.now(); StationActivity.log('scan', { orderId: rid, parts: 3, detail: 'typed' }); StationLiveOrder.start(rid, t, {}); return performance.now() - a; }, [rid, TX]);
        worst = Math.max(worst, ms);
        await wait(70);
      }
      await page.evaluate(() => StationActivity.flush()); await wait(1200);
      const lives = kinds(c, 'live');
      assert(lives.length <= 16, `${lives.length} live requests for 100 scans in ${((realNow() - t0) / 1000).toFixed(1)} s`);
      await until(() => live().length && live()[0].rid === '3812300199', 'the last order on the board', 8000);
      assert.equal(cur.count('Station_Activity'), 100, 'every scan is one stored event');
      assert.equal(await page.evaluate(() => StationActivity.pending()), 0);
      assert(worst < 100, `a scan call took ${worst.toFixed(1)} ms`);
      assert.deepEqual(page.errs, []);
      await c.close();
    });

    /* ───────── FAILURES ───────── */
    for (const bad of [500, 429, 'abort']) await check(`failure ${bad}: the station doors fail for 5 minutes: calls stay instant, retries are bounded, everything arrives afterwards`, async () => {
      fresh(); const c = await site(browser);
      await c.ctx.clock.install({ time: realNow() });
      const page = await lab(c);
      await page.evaluate(() => signIn('Tess Welder'));
      await page.clock.pauseAt(realNow() + 3000);
      c.net.behave = r => (r.kind === 'live' || r.kind === 'activity') ? bad : 'ok';
      const ms = await page.evaluate(t => { const a = performance.now(); StationLiveOrder.start('3812300001', t, {}); StationActivity.log('scan', { orderId: '3812300001', parts: 2 }); return performance.now() - a; }, TX);
      assert(ms < 100, 'the call took ' + ms.toFixed(1) + ' ms');
      await step(page, 100, 50, 100);
      await step(page, 300000, 5000, 12);
      const lives = kinds(c, 'live').length, acts = kinds(c, 'activity').length;
      assert(lives >= 2 && lives <= 20, `live attempts in 5 minutes: ${lives}`);
      assert(acts >= 1 && acts <= 45, `activity attempts in 5 minutes: ${acts}`);
      assert.equal(await page.evaluate(() => StationActivity.pending()), 1, 'the event waits in the queue');
      assert.deepEqual(page.errs, [], 'no page error');
      c.net.behave = () => 'ok';
      await step(page, 135000, 5000, 40);                       // (the longest wait between two tries is 2 minutes)
      assert(live().length === 1 && live()[0].rid === ORDER && live()[0].state === 'working', 'the order is on the board once the server answers: ' + JSON.stringify(live().map(d => d.rid + ':' + d.state)));
      assert.equal(cur.count('Station_Activity'), 1, 'the event arrived once');
      assert.equal(await page.evaluate(() => StationActivity.pending()), 0);
      await c.close();
    });

    await check('failure slow: a 4 s answer never holds up a scan, and the last order still wins', async () => {
      fresh(); const c = await site(browser);
      const page = await lab(c);
      await page.evaluate(() => signIn('Tess Welder'));
      await wait(300);
      c.net.behave = r => r.kind === 'live' ? { slow: 4000 } : 'ok';
      await page.evaluate(t => StationLiveOrder.start('3812300001', t, {}), TX);
      await wait(300);
      const ms = await page.evaluate(t => { const a = performance.now(); StationLiveOrder.start('3812300002', t, {}); StationLiveOrder.start('3812300003', t, {}); return performance.now() - a; }, TX);
      assert(ms < 100, 'two scans took ' + ms.toFixed(1) + ' ms while the server was slow');
      await until(() => live().length && live()[0].rid === '3812300003', 'the last order wins', 25000);
      assert(kinds(c, 'live').length <= 4, 'one request at a time, only the newest order sent: ' + kinds(c, 'live').length);
      assert.deepEqual(page.errs, []);
      await c.close();
    });

    await check('failure hang: a request that is never answered is given up, the next one goes out, no page error', async () => {
      fresh(); const c = await site(browser);
      await c.ctx.clock.install({ time: realNow() });
      const page = await lab(c);
      await page.evaluate(() => signIn('Tess Welder'));
      await page.clock.pauseAt(realNow() + 3000);
      c.net.behave = r => r.kind === 'live' ? 'hang' : 'ok';
      await page.evaluate(t => StationLiveOrder.start('3812300001', t, {}), TX);
      await step(page, 100, 50, 100);
      await step(page, 20000, 1000, 15);                       // past the 15 s give-up
      c.net.behave = () => 'ok';
      await step(page, 70000, 5000, 60);
      assert(live().length === 1 && live()[0].rid === ORDER && live()[0].state === 'working', 'the order reaches the board after the hung request is given up: ' + JSON.stringify(live().map(d => d.rid + ':' + d.state)));
      assert.deepEqual(page.errs, []);
      await c.close();
    });

    await check('failure offline: events wait while the network is away; the moment it comes back live and activity recover (no waiting out a backoff)', async () => {
      fresh(); const c = await site(browser, { offlineSwitch: true });
      await c.ctx.clock.install({ time: realNow() });
      const page = await lab(c);
      await page.evaluate(() => signIn('Tess Welder'));
      await page.clock.pauseAt(realNow() + 3000);
      await page.evaluate(t => StationLiveOrder.start('3812300001', t, {}), TX);
      await step(page, 100, 50, 150);
      await until(() => live().length, 'the live document');
      c.net.behave = () => 'abort';
      await page.evaluate(() => { window.__online = false; window.dispatchEvent(new Event('offline')); });
      await page.evaluate(t => { StationActivity.log('scan', { orderId: '3812300002', parts: 1 }); StationLiveOrder.start('3812300002', t, {}); }, TX);
      await step(page, 300000, 5000, 12);                      // five minutes without a network: every retry has backed off as far as it goes (a minute or two)
      assert.equal(live()[0].rid, ORDER, 'offline: the board still shows the last order it heard');
      assert.equal(cur.count('Station_Activity'), 0);
      assert.equal(await page.evaluate(() => StationActivity.pending()), 1, 'the event waits');
      c.net.behave = () => 'ok';
      await page.evaluate(() => { window.__online = true; window.dispatchEvent(new Event('online')); });
      await step(page, 1000, 250, 150);                        // one second of the page's time, not a minute
      await until(() => live()[0].rid === '3812300002', 'live recovered within a second of the network coming back', 6000);
      await until(() => cur.count('Station_Activity') === 1, 'activity recovered', 6000);
      assert.deepEqual(page.errs, []);
      await c.close();
    });

    /* ───────── LEAVING ───────── */
    await check('leaving: a hidden tab keeps its order and sends what is queued; a closed page sends its idle', async () => {
      fresh(); const c = await site(browser);
      const page = await lab(c);
      await page.evaluate(() => signIn('Tess Welder'));
      await wait(300);
      await page.evaluate(t => { StationLiveOrder.start('3812300001', t, {}); StationActivity.log('scan', { orderId: '3812300001', parts: 2 }); }, TX);
      await until(() => live().length, 'the live document');
      await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { get: () => 'hidden', configurable: true }); document.dispatchEvent(new Event('visibilitychange')); });
      await until(() => cur.count('Station_Activity') === 1, 'the queued event sent on hide', 4000);
      assert.equal(live()[0].state, 'working', 'a hidden tab is still working');
      await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { get: () => 'visible', configurable: true }); document.dispatchEvent(new Event('visibilitychange')); });
      await wait(300);
      assert.equal(await page.evaluate(() => StationActivity.current().length), 1, 'shown again: the order is still in hand');
      assert.equal(cur.count('Station_Activity'), 1, 'and nothing is sent twice');
      // the page goes away (a link, a reload, a closed tab): the idle goes by beacon
      await page.evaluate(t => { StationLiveOrder.start('3812300009', t, {}); StationActivity.log('scan', { orderId: '3812300009', parts: 2 }); }, TX);
      await until(() => live()[0].rid === '3812300009', 'the second order');
      await page.goto('about:blank');
      await until(() => live()[0].state === 'idle', 'the idle sent as the page went away', 4000);
      await until(() => cur.count('Station_Activity') === 2, 'the last event sent as the page went away', 4000);
      assert.equal(live().length, 1);
      assert.deepEqual(page.errs, []);
      await c.close();
    });

    /* ───────── PEOPLE ───────── */
    await check('people: two tabs of one station and person share one session and one document; nothing is counted twice', async () => {
      fresh(); const c = await site(browser);
      const a = await lab(c);
      await a.evaluate(() => signIn('Tess Welder'));
      await wait(300);
      const b = await lab(c, { who: 'Tess Welder' });                       // the second tab resumes the shared session
      await b.waitForFunction(() => StationActivity.who());
      assert.equal(await a.evaluate(() => StationActivity.who().session), await b.evaluate(() => StationActivity.who().session), 'one session');
      await a.evaluate(t => { StationLiveOrder.start('3812300001', t, {}); StationActivity.log('scan', { orderId: '3812300001', parts: 2 }); }, TX);
      await until(() => live().length, 'the live document');
      await b.evaluate(t => { StationLiveOrder.start('3812300002', t, {}); StationActivity.log('scan', { orderId: '3812300002', parts: 2 }); }, TX);
      await wait(700);
      await a.evaluate(() => StationActivity.flush()); await b.evaluate(() => StationActivity.flush()); await wait(500);
      assert.equal(live().length, 1, 'one document for the person at the page, not two');
      assert.equal(cur.count('Station_Activity'), 2, 'each scan is one event');
      assert.equal(cur.count('Station_Sessions'), 1, 'one session');
      await a.evaluate(() => StationLiveOrder.end()); await b.evaluate(() => StationLiveOrder.end()); await wait(900);
      assert.equal(live()[0].state, 'idle', 'when both are done nothing is in hand');
      assert.deepEqual([...a.errs, ...b.errs], []);
      await c.close();
    });

    await check('people: sign-out mid-order takes the order off the board; the next person on the device starts clean; a stale order is not theirs', async () => {
      fresh(); const c = await site(browser);
      await c.ctx.clock.install({ time: realNow() });
      const page = await lab(c);
      await page.evaluate(() => signIn('Tess Welder'));
      await page.clock.pauseAt(realNow() + 3000);
      await page.evaluate(t => StationLiveOrder.start('3812300001', t, {}), TX);
      await step(page, 100, 50, 150);
      assert.equal(live()[0].state, 'working');
      await page.evaluate(() => signOutNow());
      await step(page, 6000, 1000, 100);
      assert.deepEqual(live().map(d => d.person + ':' + d.state), ['Tess Welder:idle'], 'signed out: off the board');
      assert.equal(cur.all('Station_Sessions')[0].endReason, 'signOut');
      await page.evaluate(() => signIn('Ray Welder'));
      await page.evaluate(t => StationLiveOrder.start('3812300002', t, {}), TX);
      await step(page, 100, 50, 150);
      assert.deepEqual(live().map(d => d.person + ':' + d.state).sort(), ['Ray Welder:working', 'Tess Welder:idle'], 'the second person has their own order; the first is still off');
      const rayPage = await page.evaluate(() => StationActivity.current()[0].scannedAt);          // (in the page's own clock: the same clock for both people)
      await page.evaluate(() => signOutNow()); await step(page, 6000, 1000, 100);
      await page.evaluate(() => signIn('Tess Welder'));
      await step(page, 3000, 1000, 100);
      await page.evaluate(() => StationLiveOrder.note('Label printed'));     // the page still holds Ray's last order: this is Tess's first word about it
      await step(page, 100, 50, 150);
      const tess = live().find(d => d.person === 'Tess Welder');
      const tessPage = await page.evaluate(() => StationActivity.current()[0].scannedAt);
      assert(tess.state === 'working' && tess.rid === '3812300002', 'the page still held Ray\'s order: it is on the board under Tess: ' + JSON.stringify(tess));
      assert(tessPage - rayPage >= 8000, `a stale order is a scan of the person who touches it, not Ray's time borrowed: Tess's scan is ${tessPage - rayPage} ms after Ray's (about 9 s)`);
      assert.equal(live().find(d => d.person === 'Ray Welder').state, 'idle', "and Ray's is off the board");
      // two people on one device without a sign-out between them (a PIN typed over the first login) and a scan in the same second
      await page.evaluate(t => { signIn('Mia Welder'); StationLiveOrder.start('3812300003', t, {}); }, TX);
      await step(page, 6000, 1000, 100);
      assert.equal(live().find(d => d.person === 'Tess Welder').state, 'idle', "switching person ends the first person's order");
      assert.equal(live().find(d => d.person === 'Mia Welder').rid, '3812300003');
      assert.deepEqual(page.errs, []);
      await c.close();
    });

    await check('people: a name with a PIN in it is cleaned in the session too ("Marco 482913" is Marco; "482913" is nobody)', async () => {
      fresh(); const c = await site(browser);
      const page = await lab(c);
      await page.evaluate(() => signIn('482913'));
      await wait(300);
      assert.equal(cur.count('Station_Sessions'), 0, 'no session for a PIN');
      await page.evaluate(() => signIn('Marco 482913'));
      await until(() => cur.count('Station_Sessions') === 1, 'the session');
      assert.equal(cur.all('Station_Sessions')[0].person, 'Marco');
      assert(!JSON.stringify(cur.all('Station_Sessions')).includes('482913'));
      await c.close();
    });

    /* ───────── NIGHT ───────── */
    await check('night: the New York midnight signs everyone out and the order leaves the board; a page left open lets go after its hold', async () => {
      fresh();
      let target = Date.parse('2026-10-06T03:55:00Z');                        // 23:55 in New York (EDT)
      while (target < realNow() + 60000) target += 86400000;                  // (the next such night: the page's clock cannot be set into the past; right until the clocks change on 1 Nov)
      clock.off = target - realNow();
      const c = await site(browser);
      await c.ctx.clock.install({ time: target });
      const page = await lab(c);
      await page.clock.pauseAt(target + 3000);
      await page.evaluate(() => signIn('Tess Welder'));                         // (after the jump to 23:55: hours without input would sign the person out, Rule A)
      await page.evaluate(t => StationLiveOrder.start('3812300001', t, {}), TX);
      await step(page, 100, 50, 150);
      for (let m = 0; m < 2; m++) { await step(page, 100000, 5000, 15); await page.mouse.click(40 + m, 40); }          // (a real click: a script's own event is not input)
      assert.equal(live()[0].state, 'working', '23:58 and still working');
      assert(await page.evaluate(() => !!StationActivity.who()), 'still signed in');
      await step(page, 300000, 5000, 15);                                      // 00:03
      assert.equal(live()[0].state, 'idle', 'after midnight the order is off the board');
      assert.equal(await page.evaluate(() => StationActivity.who()), null, 'and nobody is signed in');
      const ended = cur.all('Station_Sessions').filter(s => s.endReason === 'midnight');
      assert(ended.length === 1 && ended[0].minutes >= 4 && ended[0].minutes <= 8, 'the session ended at midnight: ' + JSON.stringify(cur.all('Station_Sessions').map(s => [s.endReason, s.minutes])));
      const tail = kinds(c, 'live').slice(-1)[0];
      assert.equal(tail.body.live.event, 'idle', 'the last live request is the idle');
      await step(page, 120000, 5000, 15);
      assert.equal(kinds(c, 'live').slice(-1)[0], tail, 'nothing more is sent for nobody');
      await c.close();
      // a page left open with an order and nobody touching it
      fresh();
      const t1 = realNow();
      const c2 = await site(browser);
      await c2.ctx.clock.install({ time: t1 });
      const p2 = await lab(c2);
      await p2.evaluate(() => signIn('Tess Welder'));
      await p2.clock.pauseAt(t1 + 3000);
      await p2.evaluate(t => StationLiveOrder.start('3812300001', t, {}), TX);
      await step(p2, 100, 50, 150);
      await step(p2, 9 * 60000, 5000, 10);
      assert.equal(live()[0].state, 'working', '9 minutes quiet: still held');
      await step(p2, 2 * 60000, 5000, 10);
      assert.equal(live()[0].state, 'idle', '11 minutes quiet: the person is signed out (10 minutes without input) and the order leaves the board');
      assert.equal(await p2.evaluate(() => StationActivity.current().length), 0);
      // a key now and then (a cat on the keyboard) keeps it, at the cost of working, never more
      await p2.evaluate(() => signIn('Tess Welder'));                          // (signed out above by the 10 minute rule)
      await p2.evaluate(t => StationLiveOrder.start('3812300002', t, {}), TX); await step(p2, 100, 50, 150);
      cur.reset();
      for (let i = 0; i < 24; i++) { await step(p2, 300000, 5000, 4); await p2.keyboard.press('a'); }      // (a real key every 5 minutes, 2 hours)
      assert.equal(live()[0].state, 'working');
      const per = cur.writesOf('Station_Live').length / 2;
      assert(per <= 130, 'live writes per hour while a key is pressed now and then: ' + per);
      assert.deepEqual(p2.errs, []);
      await c2.close();
    });

    /* ───────── COST ───────── */
    await check('cost: one hour signed in is 0 live writes idle, and about 125 writes working', async () => {
      fresh(); const t = realNow();
      const c = await site(browser);
      await c.ctx.clock.install({ time: t });
      const page = await lab(c);
      await page.evaluate(() => signIn('Tess Welder'));
      await page.clock.pauseAt(t + 3000);
      cur.reset();
      await step(page, 3600000, 5000, 4);
      assert.equal(cur.writesOf('Station_Live').length, 0, 'idle: no live writes');
      assert(cur.writesOf('Station_Sessions').length <= 14, 'idle: session writes ' + cur.writesOf('Station_Sessions').length);
      assert.equal(cur.writesOf('Station_Activity').length, 0);
      c.net.log.length = 0; cur.reset();
      await page.evaluate(() => signIn('Tess Welder'));                        // (an hour without input signed the person out: this is a new sign-in)
      await page.evaluate(t => StationLiveOrder.start('3812300001', t, {}), TX);
      await step(page, 100, 50, 150);
      for (let i = 0; i < 12; i++) { await step(page, 300000, 5000, 4); await page.mouse.click(40 + i, 40); }
      const lw = cur.writesOf('Station_Live').length, sw = cur.writesOf('Station_Sessions').length;
      assert(lw >= 100 && lw <= 125, 'working: live writes in an hour ' + lw);
      assert(sw <= 14, 'working: session writes ' + sw);
      assert(lw + sw <= 135, 'working: about 125 writes an hour per person, was ' + (lw + sw));
      const perMinute = kinds(c, 'live').length / 60;
      assert(perMinute < 3, 'live requests per minute from one person: ' + perMinute.toFixed(1) + ' (the shop limit is 600 a minute for one address)');
      await c.close();
    });

    /* ───────── SIZE ───────── */
    await check('size: a 300-line order, a 600-piece line and 24 unicode labels are one accepted request under the byte limit', async () => {
      fresh(); const c = await site(browser);
      const page = await lab(c);
      await page.evaluate(() => signIn('Tess Welder'));
      await wait(300);
      const tx300 = Array.from({ length: 300 }, (_, i) => ({ transaction_id: 9200000000 + i, listing_id: 155510010 + i, title: 'Charm ' + i + ' with a fairly long title for a listing that goes on', quantity: 1, sku: 'GF-' + i }));
      const ms = await page.evaluate(t => { const a = performance.now(); StationLiveOrder.start('3812300077', t, {}); return performance.now() - a; }, tx300);
      assert(ms < 100, '300 lines took ' + ms.toFixed(1) + ' ms');
      await until(() => live().length, 'the live document');
      const rq = kinds(c, 'live')[0];
      assert(rq.text.length <= 8000 && rq.status === 200, `300 lines: ${rq.text.length} characters, status ${rq.status}`);
      assert(live()[0].pieces.length > 0 && live()[0].pieceCount >= 24, 'pieces ' + live()[0].pieces.length + ' of ' + live()[0].pieceCount);
      await page.evaluate(t => StationLiveOrder.start('3812300078', t, {}), [{ transaction_id: 9200000001, listing_id: 155510010, title: 'Bulk charm', quantity: 600, sku: 'GF-1' }]);
      await until(() => live()[0].rid === '3812300078', 'the 600-piece order');
      assert(live()[0].pieceCount >= 500, 'the true count (to 500) is kept: ' + live()[0].pieceCount);
      await page.evaluate(t => StationLiveOrder.start('3812300079', t, {}), [{ transaction_id: 9200000002, listing_id: 155510011, title: '宝'.repeat(60), quantity: 30, sku: 'GF-2' }]);
      await until(() => live()[0].rid === '3812300079', 'the unicode order');
      const rq2 = kinds(c, 'live').filter(r => r.body.live.order && r.body.live.order.rid === '3812300079').pop();
      assert(rq2.status === 200 && Buffer.byteLength(rq2.text) <= 8000, `24 unicode labels: ${Buffer.byteLength(rq2.text)} bytes, status ${rq2.status}`);
      assert.deepEqual(page.errs, []);
      await c.close();
    });

    /* ───────── SANDBOX ───────── */
    await check('sandbox: a sandbox page writes only Sandbox_ documents, a real page never does, a sandbox leftover stays sandbox', async () => {
      fresh(); const c = await site(browser);
      const page = await lab(c, { sandbox: true });
      await page.evaluate(() => signIn('Paul K.'));
      await wait(200);
      await page.evaluate(t => { StationLiveOrder.start('3812300001', t, {}); StationActivity.log('scan', { orderId: '3812300001', parts: 2 }); StationActivity.log('complete', { orderId: '3812300001', parts: 2, orders: 1 }); }, TX);
      await page.evaluate(() => StationActivity.flush()); await wait(800);
      const names = () => [...cur.colls.keys()].filter(k => cur.count(k));
      assert(names().length >= 3 && names().every(k => /^Sandbox_/.test(k)), 'a sandbox page wrote: ' + names().join(', '));
      for (const k of ['Sandbox_Station_Live', 'Sandbox_Station_Sessions', 'Sandbox_Station_Activity']) assert(names().includes(k), 'missing ' + k);
      const real = await ask({}), sb = await ask({ sandbox: true });
      assert.equal(real.body.stations.flatMap(s => s.current).length, 0, 'the real board shows no sandbox order');
      assert.equal(real.body.signedIn.length, 0, 'and no sandbox person');
      assert.equal(sb.body.stations.flatMap(s => s.current).length, 1, 'the sandbox board shows it');
      assert(kinds(c, 'live').concat(kinds(c, 'activity'), kinds(c, 'session')).every(r => r.sandbox), 'every request went to the sandbox door');
      // a real page never writes a Sandbox_ document
      await page.close(); await wait(400);
      fresh();
      const p2 = await lab(c, { device: 'shipping-2' });
      await p2.evaluate(() => signIn('Tess Welder'));
      await p2.evaluate(t => { StationLiveOrder.start('3812300002', t, {}); StationActivity.log('scan', { orderId: '3812300002', parts: 2 }); }, TX);
      await p2.evaluate(() => StationActivity.flush()); await wait(800);
      assert(names().length >= 3 && names().every(k => !/^Sandbox_/.test(k)), 'a real page wrote: ' + names().join(', '));
      await c.close();
      // an event a sandbox page left in storage is still a sandbox event when a real page on the same device sends it
      fresh();
      const c2 = await site(browser);
      const pa = await lab(c2, { sandbox: true });
      await pa.evaluate(() => signIn('Paul K.'));
      c2.net.behave = () => 'abort';
      await pa.evaluate(() => { StationActivity.log('scan', { orderId: '3812300003', parts: 1 }); });
      await wait(300); await pa.close();
      c2.net.behave = () => 'ok';
      const pb = await lab(c2, {});
      await pb.evaluate(() => signIn('Tess Welder'));
      await pb.evaluate(() => { StationActivity.log('scan', { orderId: '3812300004', parts: 1 }); });
      await pb.evaluate(() => StationActivity.flush()); await wait(1000);
      assert.deepEqual(cur.all('Sandbox_Station_Activity').map(e => e.orderId), ['3812300003'], 'the leftover stays in the sandbox');
      assert.deepEqual(cur.all('Station_Activity').map(e => e.orderId), ['3812300004'], 'the real event is the real one');
      await c2.close();
    });

    /* ───────── WIRING (static) ───────── */
    await check('wiring: every page that loads the helpers loads them in order, with one ?v= tag per file, as a station the console lists', async () => {
      const pages = fs.readdirSync(root).filter(f => /\.html$/.test(f)).map(f => [f, fs.readFileSync(path.join(root, f), 'utf8')]).filter(([, s]) => /station-(session|activity|live-order)\.js/.test(s));
      assert(pages.length >= 16, 'pages carrying the helpers: ' + pages.length);
      const tags = { 'station-session.js': new Map(), 'station-activity.js': new Map(), 'station-live-order.js': new Map() };
      const catalog = new Map();                                              // device -> every station the console lists it under (the Sorter app, charm-nest-1, is one page for Sorting, Laser and Design: LD1/LD2)
      for (const s of L.CATALOG) for (const [d] of s.devices) { if (!catalog.has(d)) catalog.set(d, new Set()); catalog.get(d).add(s.key); }
      for (const [f, s] of pages) {
        const at = n => { const m = new RegExp('<script[^>]+src=["\']' + n.replace('.', '\\.') + '(\\?v=([^"\']+))?["\']').exec(s); return m ? { i: m.index, v: m[2] || '' } : null; };
        const ses = at('station-session.js'), act = at('station-activity.js'), ord = at('station-live-order.js');
        assert(ses, f + ': loads station-session.js (a page that reports must be able to sign in)');
        assert(act, f + ': loads station-activity.js (a page that signs in must report)');
        assert(ses.i < act.i, f + ': station-session.js comes before station-activity.js');
        if (ord) assert(act.i < ord.i, f + ': station-live-order.js comes after station-activity.js');
        for (const [n, x] of [['station-session.js', ses], ['station-activity.js', act], ['station-live-order.js', ord]]) {
          if (!x) continue;
          assert(x.v, f + ': ' + n + ' has a ?v= tag (a browser must not keep the old file)');
          tags[n].set(f, x.v);
        }
        const init = /StationSession\.init\(\{\s*station:\s*["'](\w+)["']\s*,\s*device:\s*["']([\w-]+)["']/.exec(s);
        assert(init, f + ': has a StationSession.init with its station and device');
        const shown = require(path.join(root, 'netlify/functions/_activityKinds.js')).displayStation(init[1]);
        assert(catalog.has(init[2]) && catalog.get(init[2]).has(shown), f + ': ' + init[1] + '/' + init[2] + ' is a station and device the console lists (it lists ' + init[2] + ' under ' + JSON.stringify([...(catalog.get(init[2]) || [])]) + ', the page says ' + shown + ')');
      }
      for (const [n, m] of Object.entries(tags)) {
        const set = new Set(m.values());
        assert.equal(set.size, 1, n + ' has more than one ?v= tag across the pages (bump every page that loads it): ' + JSON.stringify([...m].reduce((o, [f, v]) => ((o[v] = o[v] || []).push(f), o), {})));
      }
    });

    /* ───────── every request of every scenario ───────── */
    await check('privacy: no request body of any scenario carries a PIN-sized number, an e-mail, a pin field or a canary', async () => {
      assert(ALL.length > 200, 'requests scanned: ' + ALL.length);
      const bad = [];
      for (const r of ALL) for (const p of privacyProblems(r)) bad.push(`[${r.test}] ${r.kind}: ${p}`);
      assert.deepEqual([...new Set(bad)].slice(0, 12), [], 'private values on the wire');
      assert(!ALL.some(r => /482913/.test(r.text)), 'the PIN-like canary is on the wire');
    });
  } finally { await browser.close(); }
}

main().catch(e => results.push(['setup', e])).finally(() => {
  let bad = 0;
  for (const [name, e] of results) { if (e) { bad++; console.log('FAIL', name, '\n   ', String(e && e.stack || e).split('\n').slice(0, 14).join('\n    ')); } else console.log('ok  ', name); }
  console.log(`${results.length - bad}/${results.length} passed`);
  process.exit(bad ? 1 : 0);
});
