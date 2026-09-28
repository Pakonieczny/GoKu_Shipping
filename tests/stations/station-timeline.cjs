// The shared station module (station-timeline.js) on the real Welding station page (weld-1.html), in headless Chromium,
// with Firebase, Materialize and every function stubbed here: no request leaves the machine.
//   · a typed order and a relayed scan are recorded (who, station, device, how, stable id) and a clear order is "welded"
//   · a cancelled order raises the full-screen alert; the guard asks inside it; a click on Understood records cancelAlert;
//     afterwards Complete Order asks "Do it anyway?" and only a yes goes on (recorded, then etsyCompleted)
//   · a slow check and an offline page: no alert, no crash, the scan says it was not checked
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/station-timeline.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://weld.test';
const CLEAR = '3521000001', CANCELLED = '3521000002', SLOW = '3521000003', OFF = '3521000004';
const CANCEL_AT = Date.now() - 3 * 3600e3;
const st = { events: [], checks: [], completes: 0, offline: false };

const fbStub = `window.firebase = (() => {
  window.__snaps = [];
  const snap = (exists, data) => ({ exists, data: () => data || {}, get: f => (data || {})[f] });
  const doc = (c, id) => ({ id, onSnapshot(cb) { window.__snaps.push({ c, id, cb }); try { cb(snap(false)); } catch (_) {} return () => {}; },
    set: async () => {}, update: async () => {}, get: async () => snap(false), collection: n => col(c + '/' + id + '/' + n) });
  const col = c => { const q = { doc: id => doc(c, id), where: () => q, orderBy: () => q, limit: () => q, add: async () => ({ id: 'x' }),
    onSnapshot(cb) { try { cb({ docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] }); } catch (_) {} return () => {}; },
    get: async () => ({ docs: [], empty: true, size: 0, forEach() {} }) }; return q; };
  const firestore = () => ({ collection: col });
  firestore.FieldValue = { delete: () => null, serverTimestamp: () => null, arrayUnion: () => null };
  return { initializeApp() {}, firestore, auth: () => ({ signInAnonymously: async () => ({}), onAuthStateChanged() {} }) };
})();`;
const mStub = `window.M = { AutoInit() {}, toast() {}, updateTextFields() {},
  Modal: { init() { return { open() {}, close() {} }; }, getInstance() { return { open() {}, close() {} }; } },
  FormSelect: { init() { return {}; }, getInstance() { return { getSelectedValues: () => [] }; } } };`;
const json = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 8000) {
  const t0 = Date.now();
  for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(60); }
}
const ev = (type, id) => st.events.find(e => e.type === type && e.orderId === id);

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url()), m = r.request().method();
    if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : 'window.$=window.jQuery=()=>({on(){},ready(){}});' });
    if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
    if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
    if (u.origin !== ORIGIN) return r.abort();                    // nothing leaves the machine
    if (u.pathname.startsWith('/.netlify/functions/')) {
      if (st.offline) return r.abort('internetdisconnected');   // a route still answers under setOffline, so say it here too
      const fn = u.pathname.split('/').pop();
      if (fn === 'firebaseOrders') {
        const ids = u.searchParams.get('cancelCheck');
        if (ids) {
          st.checks.push(ids);
          if (ids === OFF) return r.abort('internetdisconnected');
          if (ids === SLOW) await wait(4000);
          return json(r, { cancelled: ids === CANCELLED ? { [CANCELLED]: { at: CANCEL_AT, by: 'Paul', why: 'Buyer asked to cancel', source: 'sorter' } } : {}, now: Date.now() });
        }
        if (m === 'POST') {
          const b = JSON.parse(r.request().postData() || '{}');
          if (Array.isArray(b.timeline)) { st.events.push(...b.timeline); return json(r, { ok: true, ids: b.timeline.map(e => e.id) }); }
          return json(r, { success: true });
        }
        return json(r, { success: true, data: {} });
      }
      if (fn === 'etsyOrderProxy') return json(r, { receipt_id: Number(u.searchParams.get('orderId')), status: 'Paid', transactions: [] });
      if (fn === 'trackOrderProxy') { st.completes++; return r.fulfill({ status: 200, contentType: 'text/plain', body: 'ok' }); }
      return json(r, {});
    }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  await ctx.addInitScript(() => {
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
    localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Tess Welder');
  });
  const page = await ctx.newPage(), ours = [];
  page.on('pageerror', e => { if (/station-timeline|StationTimeline|OrderTimeline|order-timeline/.test(String(e.stack || e.message))) ours.push(e.message); });
  page.on('console', m => { if (m.type() === 'error' && /station-timeline|order-timeline/.test(m.text())) ours.push(m.text()); });

  await page.goto(ORIGIN + '/weld-1.html');
  await page.waitForFunction(() => window.StationTimeline && window.OrderTimeline && window.__snaps && window.__snaps.some(s => s.id === 'weld-scan-1'), null, { timeout: 15000 });
  assert.deepStrictEqual(await page.evaluate(() => { const c = StationTimeline.config(); return [c.station, c.device]; }), ['welding', 'weld-1'], 'weld-1 initialises the station module');
  const enter = async id => { await page.fill('#etsyOrderNumber', id); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter'); };

  // 1 · a typed order that is not cancelled: scan (typed) + welded, no alert
  await enter(CLEAR);
  await until(() => ev('scan', CLEAR) && ev('welded', CLEAR), 'the typed scan and the weld');
  const s1 = ev('scan', CLEAR), minute = Math.floor(s1.at / 60000);
  assert.strictEqual(s1.by, 'Tess Welder', 'the scan is by the employee who signed in with the PIN');
  assert.strictEqual(s1.station, 'welding'); assert.strictEqual(s1.device, 'weld-1');
  assert.strictEqual(s1.id, `weld-1-${CLEAR}-${minute}`, 'stable id device-order-minute');
  assert.strictEqual(s1.data.how, 'typed'); assert.strictEqual(s1.data.check, 'clear');
  assert.strictEqual(s1.data.extra.etsyStatus, 'Paid', 'the Etsy status the page already had rides along (no extra Etsy call)');
  assert.strictEqual(ev('welded', CLEAR).by, 'Tess Welder');
  assert.strictEqual(await page.evaluate(() => StationTimeline.alertOpen() || !!document.querySelector('.sttl-alert')), false, 'no alert for a clear order');

  // 2 · the phone scanner relays a cancelled order: full-screen alert, scan says cancelled, no weld recorded
  await page.evaluate(id => { const s = window.__snaps.find(x => x.id === 'weld-scan-1'); s.cb({ exists: true, data: () => ({ 'Order Number': id }) }); }, CANCELLED);
  await page.waitForSelector('.sttl-alert.in', { timeout: 8000 });
  const shown = await page.evaluate(() => {
    const a = document.querySelector('.sttl-alert'), cs = getComputedStyle(a), h = a.querySelector('h1');
    return { text: a.innerText, label: a.getAttribute('aria-label'), h1: h.textContent, h2: a.querySelector('h2').textContent, pos: cs.position, bg: cs.backgroundColor, z: +cs.zIndex, size: parseFloat(getComputedStyle(h).fontSize), cover: a.getBoundingClientRect().width >= innerWidth - 1 && a.getBoundingClientRect().height >= innerHeight - 1, focus: document.activeElement && document.activeElement.textContent };
  });
  if (process.env.SHOTS) { await wait(2500); await page.screenshot({ path: path.join(process.env.SHOTS, 'alert.png') }); }
  assert.strictEqual(shown.label, 'CANCELLED ORDER — DO NOT PROCEED');
  assert.deepStrictEqual([shown.h1, shown.h2], ['CANCELLED ORDER', 'DO NOT PROCEED'], 'the design spec\'s two lines');
  assert(shown.pos === 'fixed' && shown.cover && /^rgb\((1[2-8]\d|7\d|9\d|1[01]\d), /.test(shown.bg) && shown.z > 1e6 && shown.size >= 48, 'full-screen red, big letters: ' + JSON.stringify(shown));
  assert(shown.text.includes('Order ' + CANCELLED) && shown.text.includes('Cancelled by Paul') && shown.text.includes('Reason: Buyer asked to cancel') && /ago\)/.test(shown.text), 'order, who, when and why: ' + shown.text);
  assert(shown.text.includes('Scanned at Welding by Tess Welder'), 'where and by whom it was scanned');
  assert.strictEqual(shown.focus, 'Understood', 'Understood has the focus');
  await until(() => ev('scan', CANCELLED), 'the cancelled scan');
  assert.strictEqual(ev('scan', CANCELLED).data.how, 'scan', 'a relayed scan is a scan');
  assert.strictEqual(ev('scan', CANCELLED).data.check, 'cancelled');
  assert.strictEqual((await page.evaluate(id => StationTimeline.isCancelled(id), CANCELLED)).by, 'Paul', 'the cached answer');

  //   the guard while the alert is up asks inside it (no pop-up on a pop-up); Stop says no and the alert stays
  await page.evaluate(id => { window.__g1 = StationTimeline.guard(id, { action: 'test' }); }, CANCELLED);
  await page.waitForSelector('.sttl-alert .sttl-guard', { timeout: 3000 });
  assert.strictEqual(await page.evaluate(() => document.querySelectorAll('.sttl-bar').length), 0, 'no second pop-up');
  assert(/is cancelled\. Do it anyway\?/.test(await page.textContent('.sttl-guard')));
  if (process.env.SHOTS) { await wait(400); await page.screenshot({ path: path.join(process.env.SHOTS, 'alert-guard.png') }); }
  await page.click('.sttl-guard .sttl-btn.stop');
  assert.strictEqual(await page.evaluate(() => window.__g1), false, 'Stop → the action does not go on');
  assert.strictEqual(await page.evaluate(() => StationTimeline.alertOpen()), true, 'the alert stays until Understood');

  //   a click on Understood (never Enter: a scanner ends every scan with it) → cancelAlert with the worker's name, the alert zooms out
  await wait(500); await page.click('.sttl-ok');
  await page.waitForFunction(() => !document.querySelector('.sttl-alert'), null, { timeout: 3000 });
  await until(() => ev('cancelAlert', CANCELLED), 'cancelAlert');
  assert.strictEqual(ev('cancelAlert', CANCELLED).by, 'Tess Welder'); assert.strictEqual(ev('cancelAlert', CANCELLED).data.how, 'button');
  assert(!ev('welded', CANCELLED), 'a cancelled order is not recorded as welded');

  //   afterwards, Complete Order asks inline first: Stop → nothing sent; Do it anyway → sent, recorded
  await page.evaluate(() => { document.getElementById('trackingNumberInput').value = '9400100000000000000000'; document.getElementById('carrierSelect').value = 'usps'; document.getElementById('completeOrderBtn').click(); });
  await page.waitForSelector('.sttl-bar.in', { timeout: 3000 });
  if (process.env.SHOTS) { await wait(400); await page.screenshot({ path: path.join(process.env.SHOTS, 'bar.png') }); }
  await page.click('.sttl-bar .sttl-btn.stop'); await wait(400);
  assert.strictEqual(st.completes, 0, 'Stop: the order is not completed');
  await page.evaluate(() => document.getElementById('completeOrderBtn').click());
  await page.waitForSelector('.sttl-bar.in', { timeout: 3000 });
  await page.click('.sttl-bar .sttl-btn.go');
  await until(() => st.completes === 1 && ev('etsyCompleted', CANCELLED) && ev('note', CANCELLED), 'Complete Order after Do it anyway');
  assert.strictEqual(ev('etsyCompleted', CANCELLED).data.despiteCancel, true, 'the completion says it went ahead despite the cancel');
  assert(/Complete Order/.test(ev('note', CANCELLED).text), 'the "do it anyway" is on the timeline');
  assert.strictEqual(await page.evaluate(id => StationTimeline.guard(id), CLEAR), true, 'a clear order is never asked');

  // 3 · a check that takes too long: not blocked (~2.5 s), no alert, the scan says so
  const t0 = Date.now(); await enter(SLOW);
  await until(() => ev('scan', SLOW), 'the slow scan', 12000);
  assert(Date.now() - t0 < 6000, 'the check gives up after about 2.5 s');
  assert.strictEqual(ev('scan', SLOW).data.check, 'unchecked'); assert.strictEqual(ev('scan', SLOW).data.checkNote, 'timeout');
  assert(ev('welded', SLOW), 'the station goes on');

  // 4 · offline: no alert, no crash; the scan waits in the outbox and says it was not checked
  await enter(OFF);
  await until(() => ev('scan', OFF), 'the offline scan');
  assert.strictEqual(ev('scan', OFF).data.check, 'unchecked'); assert(ev('scan', OFF).data.checkNote, 'why it was not checked');
  await ctx.setOffline(true); st.offline = true;
  const r4 = await page.evaluate(() => StationTimeline.scanned('3521000005', { how: 'paste' }));
  assert.deepStrictEqual(r4, { cancelled: false }, 'offline is not blocked');
  assert.strictEqual(await page.evaluate(() => JSON.parse(localStorage.getItem('orderTimeline.outbox.v1') || '[]').some(e => e.orderId === '3521000005' && e.type === 'scan' && e.data.check === 'unchecked' && e.data.how === 'paste')), true, 'kept in the outbox until the network is back');
  await ctx.setOffline(false); st.offline = false;
  assert.strictEqual(await page.evaluate(() => !!document.querySelector('.sttl-alert')), false, 'no alert offline');

  // 5 · a broken timeline client never breaks the page
  const r5 = await page.evaluate(async () => { const o = window.OrderTimeline; window.OrderTimeline = { config() { throw new Error('x'); }, record() { throw new Error('x'); }, cancelCheck() { throw new Error('x'); } };
    try { return [await StationTimeline.scanned('3521000006'), StationTimeline.did('welded', '3521000006'), await StationTimeline.guard('3521000006')]; } finally { window.OrderTimeline = o; } });
  assert.deepStrictEqual(r5, [{ cancelled: false }, null, true], 'failures answer "go on"');

  assert.deepStrictEqual(ours, [], 'no errors from the timeline modules');
  console.log(`station timeline OK: ${st.events.length} events (${[...new Set(st.events.map(e => e.type))].join(', ')}); ${st.checks.length} cancel checks`);
  await browser.close();
})().catch(e => { console.error(e); process.exit(1); });
