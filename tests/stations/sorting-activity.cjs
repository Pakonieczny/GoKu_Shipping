// What the Sorting station and the Charm Sorter record for the Employee efficiency console (station-activity.js), offline.
// The real station-session.js and station-activity.js run; every request off the fake origin is aborted, Etsy, Firebase,
// the printer and the order timeline are stubs, and the door (firebaseOrders) is the test's own recorder.
//   1 · sorting.html: nobody signed in records nothing (a sticker printed then does not use the order up); a typed name
//       or a 6-digit employee number signs in (the number is never sent); a typed batch is one scan per order with its
//       pieces; a sheet-QR batch says so; a cancelled order is one reject; a sticker is a print and, the first time that
//       day, the order sorted (complete, once); the same sticker again is a print only; Print Row does the same per order.
//   2 · the sorter (charm-nest-1.html, its own fake server): the typed name is the person (station sorter, device
//       charm-nest-1); nothing before a name is set; Print QR label is a print and the order completed (once); printing it
//       again is a print only; Reopen is a note; Complete Order is the order completed again; Undo is an undo.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/stations/sorting-activity.cjs
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
  const raw = [], events = [], sessions = [];
  return {
    raw, events, sessions,
    take(url, body) {
      raw.push(url + ' ' + body);
      let j = null; try { j = JSON.parse(body || 'null'); } catch (_) {}
      if (j && Array.isArray(j.activity)) events.push(...j.activity);
      if (j && j.session) sessions.push(j.session);
    },
    ev: (action, filter) => events.filter(e => e.action === action && (!filter || filter(e)))
  };
}
const brief = e => [e.action, e.orderId, e.parts, e.orders, e.person, e.station, e.device];

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const errors = [];

  /* ───────────── 1 · sorting.html ───────────── */
  {
    const rec = recorder();
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', body });
    await ctx.route('**/*', r => {
      const u = new URL(r.request().url());
      if (/gstatic\.com$/.test(u.hostname)) return r.fulfill(js(/firebase-app-compat/.test(u.pathname) ? firebaseStub : ''));
      if (/materialize/.test(u.pathname)) return r.fulfill(js(materializeStub));
      if (/jquery/.test(u.pathname)) return r.fulfill(js(fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : ''));
      if (u.origin !== ORIGIN) return r.abort();
      const p = decodeURIComponent(u.pathname);
      if (p === '/sorting.html') return r.fulfill({ status: 200, contentType: 'text/html', body: fs.readFileSync(path.join(root, 'sorting.html')) });
      if (['/order-timeline.js', '/station-session.js', '/station-activity.js'].includes(p)) return r.fulfill(js(fs.readFileSync(path.join(root, p.slice(1)))));
      if (p === '/station-timeline.js') return r.fulfill(js(stationStub));
      if (p === '/QR Printer.html') return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><title>printer stub</title>' });
      if (p === '/.netlify/functions/etsyOrderProxy') {
        const id = u.searchParams.get('orderId');
        return id === D ? r.fulfill({ status: 404, contentType: 'application/json', body: '{}' }) : r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(receipt(id)) });
      }
      if (p === '/.netlify/functions/firebaseOrders') {
        // the server's login door ({pinLogin}) is the one request that carries the number; it is kept apart from the rest
        const pl = (() => { try { return JSON.parse(r.request().postData() || 'null'); } catch (_) { return null; } })();
        if (pl && pl.pinLogin !== undefined) { doorBodies.push(r.request().postData()); return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(pl.pinLogin === PIN ? { ok: true, name: 'Maya Sorter' } : { ok: false, error: 'not on the list' }) }); }
        rec.take(u.pathname + u.search, r.request().postData() || '');
        if (/employee/i.test(u.searchParams.get('orderId') || '')) { mapGets++; return r.fulfill({ status: 401, contentType: 'application/json', body: JSON.stringify({ success: false }) }); }   // the roster is never asked for
        if (r.request().method() === 'POST') return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true }) });
        return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, cancelled: {} }) });
      }
      if (p === '/.netlify/functions/etsyImages') return r.fulfill({ status: 200, contentType: 'application/json', body: '[]' });
      if (p.startsWith('/.netlify/functions/')) return r.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      return r.fulfill({ status: 404, body: '' });
    });
    await ctx.addInitScript(() => {
      localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
      localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
      window.__relayDoc = null;
    });
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${ORIGIN}/sorting.html`);
    await page.waitForFunction(() => window.SortTL && window.StationActivity && window.StationSession && document.querySelector('#sortingAsChip .st-as-name') && window.__snap && window.__snap.cb);
    const flush = async () => { await page.evaluate(() => StationActivity.flush()); };
    const tilesFor = n => page.waitForFunction(n => (window.cachedOrderItems || []).length === n && document.getElementById('previewCell' + (n - 1)), n, { timeout: 15000 });
    const typeBatch = async ids => { await page.fill('#etsyOrderNumber', ids.join(',')); await page.press('#etsyOrderNumber', 'Enter'); };
    const cellOf = rid => page.evaluate(rid => window.cachedOrderItems.findIndex(t => String(t.receipt_id) === rid), rid);
    const printed = () => page.evaluate(() => [...document.querySelectorAll('iframe')].filter(f => /QR/.test(f.src)).length);

    // nobody is signed in: a batch is loaded and a sticker printed, and nothing is recorded; the order is not used up
    await typeBatch([A, C]); await tilesFor(3);
    let iA = await cellOf(A);
    await page.evaluate(i => openIframePrinterForListing(i), iA);
    await page.waitForFunction(() => [...document.querySelectorAll('iframe')].some(f => /QR/.test(f.src)));
    await page.waitForFunction(() => window.__toasts !== undefined);
    await flush();
    assert.deepStrictEqual(rec.events, [], 'nobody signed in: no activity is recorded');
    assert.strictEqual(await page.evaluate(() => localStorage.getItem('sorting.sortedToday')), null, 'and the printed order is not counted as sorted');
    assert.strictEqual(await page.evaluate(() => StationActivity.who()), null);
    console.log('sorting.html: nobody signed in → no events, the order is not used up');

    // a 6-digit employee number signs in (the name is kept, the number never sent)
    await page.click('#sortingAsChip .st-as-name');
    await page.waitForSelector('#sortingAsChip .st-as-input:not([hidden])');
    await page.keyboard.type(PIN); await page.keyboard.press('Enter');
    await page.waitForFunction(() => localStorage.getItem('sorting.employee') === 'Maya Sorter');
    await page.waitForFunction(() => StationActivity.who() && StationActivity.who().person === 'Maya Sorter');
    const who = await page.evaluate(() => StationActivity.who());
    assert.deepStrictEqual([who.person, who.station, who.device], ['Maya Sorter', 'sorting', 'sorting-1']);

    // a typed batch: one scan per order, with its pieces (A: 3, C: 1)
    await typeBatch([A, C]); await tilesFor(3);
    await page.waitForFunction(() => StationActivity.pending() >= 2);
    iA = await cellOf(A); const iC = await cellOf(C);
    await flush();
    assert.deepStrictEqual(rec.ev('scan').map(brief), [['scan', A, 3, 0, 'Maya Sorter', 'sorting', 'sorting-1'], ['scan', C, 1, 0, 'Maya Sorter', 'sorting', 'sorting-1']], 'a typed batch is one scan per order, with its pieces');
    assert(rec.ev('scan').every(e => e.detail === 'typed'));

    // a sticker: a print, and the order sorted (complete, once, orders 1, its three pieces)
    await page.evaluate(i => openIframePrinterForListing(i), iA);
    await page.waitForFunction(() => [...document.querySelectorAll('iframe')].filter(f => /QR/.test(f.src)).length >= 2);
    await flush();
    assert.deepStrictEqual(rec.ev('print').map(brief), [['print', A, 3, 0, 'Maya Sorter', 'sorting', 'sorting-1']]);
    assert.deepStrictEqual(rec.ev('complete').map(brief), [['complete', A, 3, 1, 'Maya Sorter', 'sorting', 'sorting-1']], 'the first sticker of the order is the order sorted');
    // the same sticker again: a print only
    await page.evaluate(i => openIframePrinterForListing(i), iA);
    await page.waitForFunction(() => [...document.querySelectorAll('iframe')].filter(f => /QR/.test(f.src)).length >= 3);
    await flush();
    assert.strictEqual(rec.ev('print').length, 2, 'a second print is recorded'); assert(/again/.test(rec.ev('print')[1].detail));
    assert.strictEqual(rec.ev('complete').length, 1, 'but the order is sorted once');
    // Print Row: C is new (print + complete), A again (print only)
    await page.evaluate(() => handlePrintRowClick());
    await page.waitForFunction(() => localStorage.getItem('qrPrintBatch'));
    await flush();
    assert.deepStrictEqual(rec.ev('complete').map(e => e.orderId), [A, C], 'Print Row: the new order is sorted, the one already sorted is not counted again');
    assert.deepStrictEqual(rec.ev('print').map(e => e.orderId), [A, A, A, C], 'every sticker of the row is a print');
    console.log('sorting.html: scans, prints and the order sorted once, with the person, station, device and pieces');

    // a sheet-QR batch with a cancelled order: scans say sheet QR, one reject for the cancelled order
    await page.evaluate(d => __fireSnap(d), { 'Order Number': [A, B].join(','), 'Shipping Label Timestamps': iso(0) });
    await tilesFor(3);
    await page.waitForSelector('#stCancelAlert.on', { timeout: 10000 });
    await page.waitForTimeout(6300);                 // past the fallback that could raise the alert twice
    await flush();
    const sheet = rec.ev('scan').filter(e => e.detail === 'sheet QR');
    assert.deepStrictEqual(sheet.map(e => e.orderId), [A, B], 'a sheet-QR batch says so');
    assert.deepStrictEqual(rec.ev('reject').map(brief), [['reject', B, 0, 0, 'Maya Sorter', 'sorting', 'sorting-1']], 'the cancelled order is one reject');
    await page.click('#stCancelAlert .st-ok');
    // an order that did not load is an error, not a scan
    await typeBatch([A, D]); await tilesFor(2);
    await page.waitForFunction(() => StationActivity.pending() >= 1);
    await flush();
    assert.deepStrictEqual(rec.ev('error').map(e => [e.orderId, e.person]), [[D, 'Maya Sorter']], 'an order that did not load is one error');
    assert(!rec.ev('scan').some(e => e.orderId === D), 'and not a scan');
    console.log('sorting.html: sheet-QR scans, one reject for the cancelled order, one error for the order that did not load');

    // nothing in any request carries the PIN, and every event is one of the contract's
    const all = rec.raw.join('\n');
    assert(!all.includes(PIN), 'the PIN is never sent (but to the login door)');
    assert(doorBodies.length >= 1 && doorBodies.every(b => /^\{"pinLogin":"\d{6}"\}$/.test(b)) && mapGets === 0, 'the sign-in went to the login door only; the roster was never read');
    assert(rec.events.every(e => ['scan', 'reject', 'complete', 'print', 'undo', 'error', 'note'].includes(e.action) && e.person === 'Maya Sorter' && e.station === 'sorting' && e.device === 'sorting-1'));
    assert(rec.events.every(e => e.session && e.computer), 'every event joins its session and computer');
    assert(rec.sessions.length && rec.sessions.every(s => s.person === 'Maya Sorter' && !('employeeId' in s && /\d{6}/.test(s.employeeId))), 'the session names the person');
    assert.strictEqual(new Set(rec.events.map(e => e.id)).size, rec.events.length, 'every event has its own id (nothing doubled)');
    await ctx.close();
  }

  /* ───────────── 1b · sorting-2.html (the second sorting station, sorting-2.goldenspike.app) ───────────── */
  {
    const rec = recorder();
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', body });
    await ctx.route('**/*', r => {
      const u = new URL(r.request().url());
      if (/gstatic\.com$/.test(u.hostname)) return r.fulfill(js(/firebase-app-compat/.test(u.pathname) ? firebaseStub : ''));
      if (/materialize/.test(u.pathname)) return r.fulfill(js(materializeStub));
      if (/jquery/.test(u.pathname)) return r.fulfill(js(fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : ''));
      if (u.origin !== ORIGIN) return r.abort();
      const p = decodeURIComponent(u.pathname);
      if (p === '/sorting-2.html') return r.fulfill({ status: 200, contentType: 'text/html', body: fs.readFileSync(path.join(root, 'sorting-2.html')) });
      if (['/station-session.js', '/station-activity.js'].includes(p)) return r.fulfill(js(fs.readFileSync(path.join(root, p.slice(1)))));
      if (p === '/QR Printer.html') return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><title>printer stub</title>' });
      if (p === '/.netlify/functions/etsyOrderProxy') return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(receipt(u.searchParams.get('orderId'))) });
      if (p === '/.netlify/functions/firebaseOrders') {
        const pl = (() => { try { return JSON.parse(r.request().postData() || 'null'); } catch (_) { return null; } })();
        if (pl && pl.pinLogin !== undefined) { doorBodies.push(r.request().postData()); return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(pl.pinLogin === PIN ? { ok: true, name: 'Maya Sorter' } : { ok: false, error: 'not on the list' }) }); }
        rec.take(u.pathname + u.search, r.request().postData() || '');
        if (/employee/i.test(u.searchParams.get('orderId') || '')) { mapGets++; return r.fulfill({ status: 401, contentType: 'application/json', body: JSON.stringify({ success: false }) }); }
        return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true }) });
      }
      if (p === '/.netlify/functions/etsyImages') return r.fulfill({ status: 200, contentType: 'application/json', body: '[]' });
      if (p.startsWith('/.netlify/functions/')) return r.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      return r.fulfill({ status: 404, body: '' });
    });
    await ctx.addInitScript(() => {
      localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
      localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
      window.__relayDoc = null;
    });
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${ORIGIN}/sorting-2.html`);
    await page.waitForFunction(() => window.SortAct && window.StationActivity && document.querySelector('#sortingAsChip .st-as-name') && window.__snap && window.__snap.cb);
    const flush = () => page.evaluate(() => StationActivity.flush());
    const tilesFor = n => page.waitForFunction(n => (window.cachedOrderItems || []).length === n && document.getElementById('previewCell' + (n - 1)), n, { timeout: 15000 });
    // nobody signed in: nothing; then a typed name signs in at sorting-2
    await page.fill('#etsyOrderNumber', [A, C].join(',')); await page.press('#etsyOrderNumber', 'Enter'); await tilesFor(3);
    await flush();
    assert.deepStrictEqual(rec.events, [], 'sorting-2: nobody signed in, nothing recorded');
    await page.click('#sortingAsChip .st-as-name');
    await page.waitForSelector('#sortingAsChip .st-as-input:not([hidden])');
    await page.keyboard.type('Ivy Two'); await page.keyboard.press('Enter');
    await page.waitForFunction(() => StationActivity.who() && StationActivity.who().person === 'Ivy Two');
    const who = await page.evaluate(() => StationActivity.who());
    assert.deepStrictEqual([who.station, who.device], ['sorting', 'sorting-2']);
    await page.fill('#etsyOrderNumber', [A, C].join(',')); await page.press('#etsyOrderNumber', 'Enter'); await tilesFor(3);
    const iA = await page.evaluate(rid => window.cachedOrderItems.findIndex(t => String(t.receipt_id) === rid), A);
    await page.evaluate(i => openIframePrinterForListing(i), iA);
    await page.waitForFunction(() => [...document.querySelectorAll('iframe')].some(f => /QR/.test(f.src)));
    await page.evaluate(i => openIframePrinterForListing(i), iA);
    await flush();
    assert.deepStrictEqual(rec.ev('scan').map(brief), [['scan', A, 3, 0, 'Ivy Two', 'sorting', 'sorting-2'], ['scan', C, 1, 0, 'Ivy Two', 'sorting', 'sorting-2']], 'sorting-2: a typed batch is one scan per order');
    assert.deepStrictEqual(rec.ev('complete').map(brief), [['complete', A, 3, 1, 'Ivy Two', 'sorting', 'sorting-2']], 'sorting-2: the order is sorted once');
    assert.strictEqual(rec.ev('print').length, 2, 'sorting-2: each sticker is a print');
    assert(rec.sessions.some(s => s.station === 'sorting' && s.device === 'sorting-2' && s.person === 'Ivy Two'), 'sorting-2 now has its sign-in session');
    console.log('sorting-2.html: the chip, a StationSession and scans, prints and the order sorted once');
    // the chip takes a 6-digit number too: the server's login door answers with the name, and the roster is never read
    const door0 = doorBodies.length, map0 = mapGets;
    await page.click('#sortingAsChip .st-as-name');
    await page.waitForSelector('#sortingAsChip .st-as-input:not([hidden])');
    await page.keyboard.type(PIN); await page.keyboard.press('Enter');
    await page.waitForFunction(() => StationActivity.who() && StationActivity.who().person === 'Maya Sorter', null, { timeout: 5000 });
    assert(doorBodies.length === door0 + 1 && /^\{"pinLogin":"\d{6}"\}$/.test(doorBodies[door0]) && mapGets === map0, 'sorting-2: the number goes to the login door only; the roster is not read');
    assert(!(await page.evaluate(() => JSON.stringify(Object.assign({}, localStorage)))).includes(PIN), 'sorting-2: the number is kept nowhere');
    console.log('sorting-2.html: a 6-digit number signs in through the login door and is kept nowhere');
    await ctx.close();
  }

  /* ───────────── 2 · the sorter ───────────── */
  {
    const { start } = require('../charm-nest/bridge-server.cjs');
    const srv = await start({ receipts: [] });
    const rec = recorder();
    try {
      const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
      const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
      const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => {};<\\/script>'], { type: 'text/html' })); } }; } };`;
      await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
        const u = r.request().url();
        if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(PDFMAKE));
        if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
        if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
        if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
        return r.abort();
      });
      // the door: activity and sessions are the test's; anything else (the timeline) goes on to the fake server
      await context.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), r => {
        const body = r.request().postData() || '';
        let j = null; try { j = JSON.parse(body || 'null'); } catch (_) {}
        if (r.request().method() === 'POST' && j && (Array.isArray(j.activity) || j.session)) { rec.take(new URL(r.request().url()).pathname, body); return r.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: true }) }); }
        return r.fallback();
      });
      await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); } catch (_) {} });
      const page = await context.newPage();
      page.setDefaultTimeout(30000);
      page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.CNAct && window.StationActivity && CN.S.cloud.ok === true, null, { timeout: 60000 });
      const RID = '4176576272', KEY = '4176576272_41765762721', SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000), DAY = 86400;
      const ORDERS = [{ receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Jessica Strom' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
        lines: [{ transactionId: '41765762721', listingId: '1800062721', sku: 'RE_5460', title: 'MODIFICATION REWORK FREE SHIPPING', quantity: 2, expectedShipDate: SHIP, variations: [{ name: 'Price', value: '144' }], metalKey: '', metalLabel: '', personalization: '' }] }];
      await page.evaluate(async orders => {
        await Orders.loadMaps(true);
        for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      }, ORDERS);
      await page.click('#reviewView .egTab[data-k="customOrder"]');
      const card = `#rvList .reviewListRow[data-rid="${RID}"]`;
      const settle = () => page.waitForFunction(() => !document.querySelector('.cuStat, .btn.working, .cuSealHost, #motionLayer .mGhost, .sealTool, .seal.pending'), null, { timeout: 15000 });
      const flush = () => page.evaluate(() => StationActivity.flush());
      const dismiss = () => page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));

      // nobody named: nothing is recorded, and the helper says so
      assert.strictEqual(await page.evaluate(() => CNAct('note', { detail: 'x' })), false, 'no name yet: nothing recorded');
      assert.strictEqual(await page.evaluate(() => StationActivity.who()), null);
      // the typed name is the person (as the sorter's own prompts set it: B.employee + cn.employee)
      await page.evaluate(() => { B.employee = 'Tess Sorter'; localStorage.setItem('cn.employee', 'Tess Sorter'); });
      const who = await page.evaluate(() => StationActivity.who());
      assert.deepStrictEqual([who && who.person, who && who.station, who && who.device], ['Tess Sorter', 'sorter', 'charm-nest-1'], 'the typed name is the person, at the sorter');
      assert.strictEqual(await page.evaluate(() => CNAct('note', { detail: 'hello' })), true); await flush();
      assert.deepStrictEqual(rec.ev('note').map(e => e.detail), ['hello']);
      rec.events.length = 0;

      // 1 · Print QR label: a print, and the order completed (once, its two pieces)
      await page.click(card + ' [data-cu-print]');
      await page.waitForFunction(k => B.maps.customDone[k], KEY); await settle(); await dismiss(); await flush();
      assert.deepStrictEqual(rec.ev('print').map(brief), [['print', RID, 2, 0, 'Tess Sorter', 'sorter', 'charm-nest-1']], 'Print QR label is a print');
      assert.deepStrictEqual(rec.ev('complete').map(brief), [['complete', RID, 2, 1, 'Tess Sorter', 'sorter', 'charm-nest-1']], 'and the order completed, once');
      // 2 · printed again from Completed: a print only
      await page.click('#reviewView .rvSeg [data-cseg="done"]'); await page.waitForSelector(card + ' [data-cu-print]');
      await page.click(card + ' [data-cu-print]');
      await page.waitForFunction(k => B.maps.customDone[k] && B.maps.customDone[k].prints === 2, KEY); await settle(); await dismiss(); await flush();
      assert.strictEqual(rec.ev('print').length, 2, 'a second print is recorded');
      assert.strictEqual(rec.ev('complete').length, 1, 'the order is not completed twice');
      // 3 · Reopen is a note; Complete Order is the order completed again
      await page.click(card + ' [data-cu-reopen]');
      await page.waitForFunction(k => !B.maps.customDone[k], KEY);
      await settle(); await dismiss(); await flush();
      assert.deepStrictEqual(rec.ev('note').filter(e => /reopened/.test(e.detail)).map(brief), [['note', RID, 0, 0, 'Tess Sorter', 'sorter', 'charm-nest-1']], 'Reopen is a note');
      await page.click('#reviewView .rvSeg [data-cseg="open"]'); await page.waitForSelector(card + ' [data-cu-complete]');
      await page.click(card + ' [data-cu-complete]');
      await page.waitForFunction(k => B.maps.customDone[k], KEY); await settle(); await flush();
      const completes = rec.ev('complete');
      assert.deepStrictEqual(completes.map(brief), [['complete', RID, 2, 1, 'Tess Sorter', 'sorter', 'charm-nest-1'], ['complete', RID, 2, 1, 'Tess Sorter', 'sorter', 'charm-nest-1']], 'Complete Order is the order completed again');
      assert(/Complete Order/.test(completes[1].detail));
      // 4 · the Undo the order window offers for 12 s after a completion is an undo (what was produced is taken back)
      await page.evaluate(k => OrderWin.open(k), KEY);
      await page.waitForSelector('#owCustom [data-cu-undo]');
      await page.click('#owCustom [data-cu-undo]');
      await page.waitForFunction(k => !B.maps.customDone[k], KEY); await settle(); await flush();
      assert.deepStrictEqual(rec.ev('undo').map(brief), [['undo', RID, 2, 1, 'Tess Sorter', 'sorter', 'charm-nest-1']], 'Undo is an undo');
      console.log('sorter: Print QR label → print + complete once; print again → print only; Reopen → note; Complete Order → complete; Undo → undo');

      const all = rec.raw.join('\n');
      assert(!/"employeeId":"\d{4,}"/.test(all), 'no PIN-like id in any request');
      assert(rec.events.every(e => e.session && e.computer && e.person === 'Tess Sorter' && e.station === 'sorter' && e.device === 'charm-nest-1'));
      assert.strictEqual(new Set(rec.events.map(e => e.id)).size, rec.events.length, 'every event has its own id');
      assert(rec.sessions.some(s => s.station === 'sorter' && s.person === 'Tess Sorter'), 'the sorter has a session for the typed name');
    } finally { await browser.close(); srv.close(); }
  }

  assert.deepStrictEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  console.log('sorting-activity: all passed');
})().catch(e => { console.error(e); process.exit(1); });
