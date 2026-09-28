// Adversarial (wave 3, area 5): the Welding, Assembly and Shipping stations with the real station module.
//   1 · weld-1: while the red alert is up, a barcode scanner (digits, then Enter), NumpadEnter, Space or Escape never
//       closes it, and Tab + Enter never presses "Do it anyway" in it; only a click (or tap) on Understood closes it
//   2 · weld-1 and assembly-1: Buy & Print on a cancelled order asks "Do it anyway?" first; Stop buys no label
//   3 · shipping-1: an order Etsy already shows as canceled (status read by the page's own Etsy pull, no extra call)
//       raises the alert at Shipping too, as it does at Welding and Assembly
// Firebase, Materialize and every function are stubbed here; any request off the test origin is aborted.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/adv-station-alert.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://station.test';
const CANC = '3521000501', NEXT = '3521000502', ETSYC = '3521000503';
const rec = { at: Date.now() - 3600e3, by: 'Paul', why: 'Buyer asked to cancel', source: 'sorter' };

const FB = `window.firebase = (() => {
  window.__snaps = [];
  const empty = id => ({ exists: false, id, data: () => undefined, get: () => undefined });
  function docRef(p) { const id = p.split('/').pop(); return { path: p, id, collection: n => colRef(p + '/' + n),
    get: async () => empty(id), set: async () => {}, update: async () => {}, delete: async () => {},
    onSnapshot(cb) { window.__snaps.push({ id, cb }); setTimeout(() => { try { cb(empty(id)); } catch (_) {} }, 0); return () => {}; } }; }
  function colRef(p) { const q = { path: p, doc: id => docRef(p + '/' + (id || 'auto')), add: async () => docRef(p + '/auto'),
    where: () => q, orderBy: () => q, limit: () => q, limitToLast: () => q, startAfter: () => q,
    get: async () => ({ empty: true, size: 0, docs: [], forEach() {} }),
    onSnapshot(cb) { setTimeout(() => { try { cb({ docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] }); } catch (_) {} }, 0); return () => {}; } }; return q; }
  const db = { collection: colRef, doc: docRef, batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }) };
  const firestore = () => db;
  firestore.FieldValue = { delete: () => null, serverTimestamp: () => null, arrayUnion: (...a) => a, increment: n => n };
  firestore.Timestamp = { now: () => ({ toDate: () => new Date(), toMillis: () => Date.now() }), fromDate: d => ({ toDate: () => d }) };
  const auth = { currentUser: { uid: 'anon' }, signInAnonymously: async () => ({}), onAuthStateChanged(cb) { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; } };
  return { apps: [], initializeApp: () => ({}), firestore, auth: () => auth, storage: () => ({ ref: () => ({}) }) };
})();`;
const FB_MODULE = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = () => ({}), getApp = () => ({}), getStorage = () => ({}), ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = () => ({}), signInAnonymously = () => Promise.resolve({});";
const MAT = `window.M = { AutoInit() {}, updateTextFields() {}, toast(o) { (window.__toasts = window.__toasts || []).push(String((o && o.html) || '')); },
  Modal: { init() { return { open() {}, close() {}, isOpen: false }; }, getInstance() { return { open() {}, close() {} }; } },
  FormSelect: { init() { return { getSelectedValues: () => [] }; }, getInstance() { return { getSelectedValues: () => [] }; } }, Dropdown: { init() {} } };`;
const wait = ms => new Promise(r => setTimeout(r, ms));
const js = body => ({ status: 200, contentType: 'text/javascript', body });
const json = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });

async function open(browser, file, st) {
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url()), m = r.request().method();
    if (/gstatic\.com\/firebasejs\//.test(u.href)) return r.fulfill(js(/firebase-app-compat/.test(u.pathname) ? FB : /-compat\.js/.test(u.pathname) ? '' : FB_MODULE));
    if (/materialize/.test(u.href)) return /\.css/.test(u.pathname) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.fulfill(js(MAT));
    if (/code\.jquery\.com|qz-tray/.test(u.href)) return r.fulfill(js(''));
    if (u.origin !== ORIGIN) return r.abort();
    if (u.pathname.startsWith('/.netlify/functions/')) {
      const fn = u.pathname.split('/').pop();
      let body = null; try { body = JSON.parse(r.request().postData() || 'null'); } catch (_) {}
      st.calls.push({ fn, method: m, body });
      if (fn === 'firebaseOrders') {
        const ids = u.searchParams.get('cancelCheck');
        if (ids) { const out = {}; for (const id of ids.split(',')) if (st.cancelled.has(id)) out[id] = rec; return json(r, { success: true, cancelled: out, now: Date.now() }); }
        if (m === 'POST' && body && Array.isArray(body.timeline)) { st.events.push(...body.timeline); return json(r, { ok: true }); }
        return json(r, { success: true, data: {} });
      }
      if (fn === 'etsyOrderProxy') { const id = u.searchParams.get('orderId'); return json(r, { receipt_id: Number(id), name: 'Buyer', status: st.etsyStatus[id] || 'Paid', is_shipped: false, transactions: [] }); }
      if (fn === 'trackOrderProxy') return r.fulfill({ status: 200, contentType: 'text/plain', body: 'ok' });
      if (fn === 'testChitChats' || fn === 'chitChatSearch') return json(r, { batches: [], shipments: [], shipment: { id: 'SHIP1' } });
      return json(r, {});
    }
    const f = path.join(root, decodeURIComponent(u.pathname));
    if (f.startsWith(root) && fs.existsSync(f) && fs.statSync(f).isFile()) return r.fulfill({ status: 200, path: f });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  await ctx.addInitScript(() => {
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
    localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Tess');
    window.alert = () => {}; window.print = () => {}; window.open = () => null;
  });
  const page = await ctx.newPage();
  await page.goto(ORIGIN + '/' + file);
  await page.waitForFunction(() => window.StationTimeline && window.OrderTimeline && (window.__snaps || []).some(s => /-scan-\d$/.test(s.id)), null, { timeout: 20000 });
  await page.waitForTimeout(150);
  await page.evaluate(() => { window.__rec = []; OrderTimeline.onRecord(e => window.__rec.push(e)); });
  return { ctx, page };
}
/* animation probes: every frame, the alert's opacity and its card's scale, the rAF gaps and any long task */
const REC = () => {
  window.__animStop = false; const A = window.__anim = { frames: [], long: [] };
  try { new PerformanceObserver(l => { for (const e of l.getEntries()) A.long.push([Math.round(e.startTime), Math.round(e.duration)]); }).observe({ type: 'longtask' }); } catch (_) {}
  let last = performance.now();
  const tick = t => {
    const a = document.querySelector('.sttl-alert'), c = a && a.querySelector('.sttl-card');
    const m = c ? new DOMMatrix(getComputedStyle(c).transform === 'none' ? undefined : getComputedStyle(c).transform) : null;
    A.frames.push({ t: Math.round(t), dt: Math.round(t - last), op: a ? +getComputedStyle(a).opacity : null, sc: m ? +m.a.toFixed(3) : null });
    last = t; if (!window.__animStop && A.frames.length < 600) requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
};
const PROPS = sel => {        // the properties every running animation under sel animates
  const skip = new Set(['offset', 'computedOffset', 'easing', 'composite']), out = new Set();
  for (const a of document.getAnimations()) {
    const t = a.effect && a.effect.target; if (!t || !t.closest || !t.closest(sel)) continue;
    if (a.transitionProperty) out.add(a.transitionProperty);
    else for (const k of a.effect.getKeyframes()) for (const p of Object.keys(k)) if (!skip.has(p)) out.add(p);
  }
  return [...out];
};
function checkAnim(what, A, props, { from, to }) {
  if (props) { const bad = props.filter(p => !/^(opacity|transform|clip-?path|clipPath)$/.test(p)); assert.deepStrictEqual(bad, [], what + ': animates only transform, opacity and clip-path, not ' + bad.join(', ')); }
  const on = A.frames.filter(f => f.op != null);
  assert(on.length >= 6, what + ': on screen for several frames (' + on.length + ')');
  const first = on.findIndex(f => Math.abs(f.op - from) > 0.02), end = on.findIndex((f, i) => i > first && Math.abs(f.op - to) < 0.02);
  const mid = on.slice(Math.max(0, first), end < 0 ? on.length : end);
  assert(first >= 0 && mid.length >= 4, what + ': the fade is seen over at least 4 frames (' + JSON.stringify(on.slice(0, 12)) + ')');
  if (to === 0) assert(on[on.length - 1].op < 0.1, what + ': not removed before it has faded (last opacity ' + on[on.length - 1].op + ')');
  else assert(new Set(on.map(f => f.sc)).size >= 3 && on[on.length - 1].sc === 1, what + ': the card zooms in over several frames');
  const t0 = on[0].t, t1 = Math.min(on[on.length - 1].t + 1, t0 + 500), jank = A.frames.filter(f => f.t >= t0 && f.t <= t1 && f.dt > 34), long = A.long.filter(([s, d]) => s + d >= t0 && s <= t1 && d > 50);
  if (process.env.ANIM_LOG) console.log(what, JSON.stringify(on.map(f => [f.t - t0, f.dt, f.op, f.sc])), JSON.stringify(A.long));
  // returned, not thrown: a busy test machine drops frames on its own, so the caller measures again before it fails
  return jank.length || long.length ? `${what}: frames over 34 ms ${JSON.stringify(jank.map(f => [f.t - t0, f.dt]))}, long tasks ${JSON.stringify(long.map(([s, d]) => [s - t0, d]))}` : '';
}
const stopRec = page => page.evaluate(() => { window.__animStop = true; return window.__anim; });
const enter = async (page, id) => { await page.fill('#etsyOrderNumber', id); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter'); };
const up = page => page.evaluate(() => StationTimeline.alertOpen());
const acks = page => page.evaluate(() => window.__rec.filter(e => e.type === 'cancelAlert').length);

module.exports = { open, REC, PROPS };
if (require.main === module) (async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const done = [];
  try {
    /* 1 · weld-1: the scanner's Enter (and every other key) leaves the alert up */
    {
      const st = { calls: [], events: [], cancelled: new Set([CANC]), etsyStatus: {} };
      const { ctx, page } = await open(browser, 'weld-1.html', st);
      await page.evaluate(REC);
      await enter(page, CANC);
      await page.waitForSelector('.sttl-alert.in', { timeout: 8000 });
      const props = await page.evaluate(PROPS, '.sttl-alert');
      await wait(900);
      const jank = [checkAnim('the alert coming in', await stopRec(page), props, { from: 0, to: 1 })];
      await wait(700);                                              // well past any "just shown" grace
      assert.strictEqual(await page.evaluate(() => document.activeElement && document.activeElement.textContent), 'Understood');
      await page.keyboard.type(NEXT, { delay: 4 }); await page.keyboard.press('Enter');           // the next barcode
      await wait(350);
      assert.strictEqual(await up(page), true, "a scanner's Enter does not close the alert");
      for (const k of ['NumpadEnter', 'Space', 'Escape']) { await page.keyboard.press(k); await wait(150); assert.strictEqual(await up(page), true, k + ' does not close the alert'); }
      await page.keyboard.down('Enter'); await page.keyboard.down('Enter'); await page.keyboard.up('Enter'); await wait(150);
      assert.strictEqual(await up(page), true, 'a held Enter does not close the alert');
      assert.strictEqual(await acks(page), 0, 'nothing recorded as seen');
      assert(!/Enter/.test(await page.textContent('.sttl-alert')), 'the alert does not say Enter closes it');

      //   the question inside the alert: Tab (some scanners end with Tab) then Enter never presses "Do it anyway"
      await page.evaluate(id => { window.__g = 'pending'; StationTimeline.guard(id, { action: 'test' }).then(v => { window.__g = v; }); }, CANC);
      await page.waitForSelector('.sttl-alert .sttl-guard', { timeout: 3000 });
      await page.keyboard.press('Tab');
      assert.strictEqual(await page.evaluate(() => document.activeElement.textContent), 'Do it anyway');
      await page.keyboard.press('Enter'); await page.keyboard.press('Space'); await wait(300);
      assert.strictEqual(await page.evaluate(() => window.__g), 'pending', 'Enter or Space on "Do it anyway" does not go ahead');
      assert.strictEqual(await up(page), true);
      await page.click('.sttl-guard .sttl-btn.stop'); await wait(100);
      assert.strictEqual(await page.evaluate(() => window.__g), false, 'a click on Stop answers no');

      //   a click on Understood closes it and is recorded; the fade out runs to its end before the alert is removed
      await page.evaluate(REC);
      await page.click('.sttl-ok');
      await page.waitForFunction(() => !document.querySelector('.sttl-alert'), null, { timeout: 3000 });
      await wait(100);
      jank.push(checkAnim('the alert going out', await stopRec(page), null, { from: 1, to: 0 }));
      const a = await page.evaluate(() => window.__rec.filter(e => e.type === 'cancelAlert'));
      assert.deepStrictEqual(a.map(e => [e.orderId, e.data.how]), [[CANC, 'button']], 'Understood recorded once, by the click');
      done.push('weld-1: scanner Enter, NumpadEnter, Space, Escape, a held Enter and Tab+Enter leave the alert up; only a click closes it');

      //   smooth: no frame over 34 ms and no long task over 50 ms while it fades and zooms in and out (up to 3 shows)
      const runs = [jank.filter(Boolean)];
      while (runs[runs.length - 1].length && runs.length < 3) {
        await page.evaluate(REC); await page.evaluate(id => StationTimeline.scanned(id), CANC);
        await page.waitForSelector('.sttl-alert.in', { timeout: 8000 }); await wait(900);
        const r = [checkAnim('the alert coming in', await stopRec(page), await page.evaluate(PROPS, '.sttl-alert'), { from: 0, to: 1 })];
        await page.evaluate(REC); await page.click('.sttl-ok');
        await page.waitForFunction(() => !document.querySelector('.sttl-alert'), null, { timeout: 3000 }); await wait(100);
        r.push(checkAnim('the alert going out', await stopRec(page), null, { from: 1, to: 0 }));
        runs.push(r.filter(Boolean));
      }
      assert(!runs[runs.length - 1].length, 'the alert animates smoothly: ' + JSON.stringify(runs));
      done.push(`weld-1: the alert fades and zooms in and out over visible frames, opacity/transform only, not cut off, smooth (${runs.length} show${runs.length > 1 ? 's; busy-machine drops before: ' + JSON.stringify(runs.slice(0, -1)) : ''})`);

      /* 2 · Buy & Print on the cancelled order asks first; Stop buys no label */
      const buys = () => st.calls.filter(c => c.fn === 'testChitChats' && c.method !== 'GET').length;
      await page.evaluate(() => { document.getElementById('ccShipmentId').value = 'SHIP1'; const b = document.getElementById('ccBuy'); b.disabled = false; b.click(); });
      await page.waitForSelector('.sttl-bar.in', { timeout: 3000 }).catch(() => { throw new Error('weld-1: Buy & Print on a cancelled order did not ask "Do it anyway?"'); });
      const barProps = await page.evaluate(PROPS, '.sttl-bar');
      assert(barProps.length && barProps.every(p => /^(opacity|transform)$/.test(p)), 'the question bar animates only opacity and transform: ' + barProps);
      await page.click('.sttl-bar .sttl-btn.stop'); await wait(400);
      assert.strictEqual(buys(), 0, 'weld-1: Stop → no Chit Chats label bought');
      done.push('weld-1: Buy & Print on a cancelled order asks first, Stop buys nothing');
      await ctx.close();
    }
    {
      const st = { calls: [], events: [], cancelled: new Set([CANC]), etsyStatus: {} };
      const { ctx, page } = await open(browser, 'assembly-1.html', st);
      await enter(page, CANC);
      await page.waitForSelector('.sttl-alert.in', { timeout: 8000 });
      await wait(500); await page.click('.sttl-ok');
      await page.waitForFunction(() => !document.querySelector('.sttl-alert'), null, { timeout: 3000 });
      await page.evaluate(() => { document.getElementById('ccShipmentId').value = 'SHIP1'; const b = document.getElementById('ccBuy'); b.disabled = false; b.click(); });
      await page.waitForSelector('.sttl-bar.in', { timeout: 3000 }).catch(() => { throw new Error('assembly-1: Buy & Print on a cancelled order did not ask "Do it anyway?"'); });
      await page.click('.sttl-bar .sttl-btn.stop'); await wait(400);
      assert.strictEqual(st.calls.filter(c => c.fn === 'testChitChats' && c.method !== 'GET').length, 0, 'assembly-1: Stop → no label bought');
      done.push('assembly-1: Buy & Print asks first');
      await ctx.close();
    }

    /* 3 · shipping-1: Etsy already says canceled (no cancel record yet): the alert still shows */
    {
      const st = { calls: [], events: [], cancelled: new Set(), etsyStatus: { [ETSYC]: 'canceled' } };
      const { ctx, page } = await open(browser, 'shipping-1.html', st);
      await enter(page, ETSYC);
      await page.waitForSelector('.sttl-alert.in', { timeout: 8000 }).catch(() => { throw new Error('shipping-1: an order Etsy shows as canceled raised no alert'); });
      assert(/Cancelled on Etsy/.test(await page.textContent('.sttl-alert')), 'the alert says Etsy cancelled it');
      const etsyReads = st.calls.filter(c => c.fn === 'etsyOrderProxy').length;
      assert(etsyReads <= 2, 'no extra Etsy call for the check: ' + etsyReads);
      done.push('shipping-1: an Etsy-canceled order raises the alert (from the pull the page already made)');
      await ctx.close();
    }
    console.log('adv-station-alert OK\n  ' + done.join('\n  '));
  } finally { await browser.close(); }
})().catch(e => { console.error(e); process.exit(1); });
