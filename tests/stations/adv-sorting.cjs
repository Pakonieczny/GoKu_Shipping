// Adversarial: the Sorting station (sorting.html) with the REAL order-timeline.js and station-timeline.js, the cancel
// check and the timeline door faked here (no network except this fake origin; everything else is aborted).
//   1 · a batch's cancel check that answers after the 2.5 s: the late "cancelled" still reaches the page's batch alert
//       and marks the order's tile (the module was told to stay quiet and dropped it); one cancel check for the batch
//   2 · a scanner's Enter (or Space) never closes the batch alert, even with "Understood" focused, nor presses a button
//       behind it; only a click does, and a relayed batch (a synthetic Enter) still loads behind it
//   3 · the batch alert fades in and flashes (opacity only, no per-frame background paint), smoothly
//   4 · printing one sticker of a cancelled order: "Do it anyway" prints it (and records sorted); Stop does not;
//       the answer is never overruled by a second pop-up
//   5 · a second batch loaded within the 2.5 s: the first batch's late "cancelled" still raises the alert naming it
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/adv-sorting.cjs
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://sorting-adv.test';
const A = '3710000001', B = '3710000002', C = '3710000003';
const st = { checks: [], events: [], delay: 3500, cancelled: new Set([B]) };
const day = Math.floor(Date.now() / 1000);
const receipt = rid => ({ receipt_id: Number(rid), status: 'Paid', message_from_buyer: '',
  transactions: [{ transaction_id: Number(rid) * 10 + 1, receipt_id: Number(rid), listing_id: 1718001, title: 'Silver Dog Charm Necklace', quantity: 1,
    variations: [{ formatted_name: 'Metal', formatted_value: 'Silver' }], expected_ship_date: day + 3 * 86400 }] });
const firebaseStub = `
  window.__snap = { cb: null };
  const mk = d => ({ exists: !!d, data: () => d });
  window.firebase = { initializeApp() { return {}; }, firestore() { return { collection() { return { doc() { return {
    onSnapshot(cb) { window.__snap.cb = cb; setTimeout(() => cb(mk(null)), 0); return () => {}; } }; } }; } }; } };
  window.__fireSnap = d => { window.__snap.cb && window.__snap.cb(mk(d)); };`;
const materializeStub = `window.__toasts = []; const inst = { open() {}, close() {} };
  window.M = { AutoInit() {}, toast(o) { window.__toasts.push(o && o.html); }, Modal: { init() { return inst; }, getInstance() { return inst; } } };`;
const wait = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 900 } });
  const js = body => ({ status: 200, contentType: 'text/javascript', body });
  await ctx.route('**/*', async r => {
    const u = new URL(r.request().url());
    if (/gstatic\.com$/.test(u.hostname)) return r.fulfill(js(/firebase-app-compat/.test(u.pathname) ? firebaseStub : ''));
    if (/materialize/.test(u.pathname)) return r.fulfill(js(materializeStub));
    if (u.origin !== ORIGIN) return r.abort();
    const p = decodeURIComponent(u.pathname);
    if (p === '/sorting.html') return r.fulfill({ status: 200, contentType: 'text/html', body: fs.readFileSync(process.env.SORTING_HTML || path.join(root, 'sorting.html')) });
    if (p === '/order-timeline.js' || p === '/station-timeline.js') return r.fulfill(js(fs.readFileSync(path.join(root, p))));
    if (p === '/QR Printer.html') return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><title>printer stub</title>' });
    if (p === '/.netlify/functions/firebaseOrders') {
      const q = u.searchParams.get('cancelCheck');
      if (q) {
        const ids = q.split(','); st.checks.push(ids);
        await wait(st.delay);
        const cancelled = {}; for (const id of ids) if (st.cancelled.has(id)) cancelled[id] = { at: Date.now() - 3600e3, by: 'Paul', why: 'Buyer asked to cancel', source: 'sorter' };
        return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, cancelled }) });
      }
      if (r.request().method() === 'POST') { try { st.events.push(...(JSON.parse(r.request().postData() || '{}').timeline || [])); } catch (_) {} }
      return r.fulfill({ status: 200, contentType: 'application/json', body: '{"success":true}' });
    }
    if (p === '/.netlify/functions/etsyOrderProxy') return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(receipt(u.searchParams.get('orderId'))) });
    if (p === '/.netlify/functions/etsyImages') return r.fulfill({ status: 200, contentType: 'application/json', body: '[]' });
    if (p.startsWith('/.netlify/functions/')) return r.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    return r.fulfill({ status: 404, body: '' });
  });
  await ctx.addInitScript(() => {
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
    localStorage.setItem('sorting.employee', 'Maya');
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(`${ORIGIN}/sorting.html`);
  await page.waitForFunction(() => window.SortTL && window.StationTimeline && window.__snap && window.__snap.cb);
  const failures = [];
  const check = (ok, msg) => { if (!ok) failures.push(msg); console.log((ok ? 'ok   ' : 'FAIL ') + msg); };

  // ── 1 · a slow cancel check (3.5 s): the late "cancelled" still reaches the batch alert ──
  await page.evaluate(d => __fireSnap(d), { 'Order Number': [A, B, C].join(','), 'Shipping Label Timestamps': new Date().toISOString() });
  await page.waitForFunction(() => (window.cachedOrderItems || []).length === 3 && document.getElementById('previewCell2'), null, { timeout: 15000 });
  const late = await page.waitForFunction(b => { const a = document.querySelector('#stCancelAlert.on'); return a && [...a.querySelectorAll('.st-list .st-who')].some(r => r.dataset.order === b); }, B, { timeout: 12000 })
    .then(() => true, () => false);
  check(late, 'a cancel check answering after 2.5 s still raises the batch alert for the cancelled order');
  check(st.checks.length === 1 && st.checks[0].length === 3, 'one cancel check for the batch of 3: ' + JSON.stringify(st.checks));
  const iB = await page.evaluate(b => window.cachedOrderItems.findIndex(t => String(t.receipt_id) === b), B);
  check(await page.evaluate(i => document.getElementById('previewCell' + i).classList.contains('st-cancelled'), iB), 'and its tile is marked CANCELLED — DO NOT SORT');
  if (!late) { await page.evaluate(b => SortTL.batchStart([b], { how: 'typed' }), B); await page.evaluate(() => SortTL.batchLoaded()); await page.waitForSelector('#stCancelAlert.on', { timeout: 8000 }); }

  // ── 2 · Enter / Space from a scanner never close it; only a click on Understood does ──
  await page.waitForTimeout(600);
  await page.focus('#stCancelAlert .st-ok');
  for (const k of ['Enter', 'NumpadEnter', 'Space']) {
    await page.keyboard.press(k); await page.waitForTimeout(350);
    check(await page.evaluate(() => !!document.querySelector('#stCancelAlert.on')), `${k} with Understood focused does not close the alert`);
    if (!(await page.evaluate(() => !!document.querySelector('#stCancelAlert.on')))) { await page.evaluate(b => SortTL.batchStart([b], { how: 'typed' }), B); await page.evaluate(() => SortTL.batchLoaded()); await page.waitForSelector('#stCancelAlert.on', { timeout: 8000 }); await page.focus('#stCancelAlert .st-ok'); }
  }
  // nor a button behind it (Tab from a scanner's suffix can move the focus there)
  await page.evaluate(() => { window.__rowClicks = 0; document.getElementById('printRowBtn').addEventListener('click', () => __rowClicks++); document.getElementById('printRowBtn').focus(); });
  await page.keyboard.press('Enter'); await page.keyboard.press('Space'); await page.waitForTimeout(300);
  check(await page.evaluate(() => __rowClicks === 0 && !!document.querySelector('#stCancelAlert.on')), 'Enter or Space never press a button behind the alert');
  // a relay batch arriving while it is up (a synthetic Enter on the order field) still loads behind it
  await page.evaluate(() => { document.getElementById('etsyOrderNumber').value = ''; });
  await page.evaluate(d => __fireSnap(d), { 'Order Number': [A, B, C].join(','), 'Shipping Label Timestamps': new Date(Date.now() + 1000).toISOString() });
  await page.waitForFunction(() => (window.cachedOrderItems || []).length === 3 && document.getElementById('previewCell2'), null, { timeout: 15000 });
  check(await page.evaluate(() => !!document.querySelector('#stCancelAlert.on')), 'a relayed batch (synthetic Enter) loads behind the alert without closing it');
  await page.click('#stCancelAlert .st-ok');
  check(await page.waitForFunction(() => !document.querySelector('#stCancelAlert'), null, { timeout: 3000 }).then(() => true, () => false), 'a click on Understood closes it');

  // ── 3 · the alert's look: fades in, flashes on opacity alone, smooth frames ──
  await page.evaluate(() => {
    window.__lt = []; try { new PerformanceObserver(l => l.getEntries().forEach(e => __lt.push(e.duration))).observe({ entryTypes: ['longtask'] }); } catch (_) {}
  });
  // drive the alert through the page's own path (Etsy's answer says cancelled), then measure
  const measured = await page.evaluate(async () => {
    const frames = [], ops = [];
    let last = performance.now(), run = true;
    let onAt = 0;
    const tick = t => { const a = document.getElementById('stCancelAlert'); if (a && !onAt) onAt = t; if (onAt && t - onAt < 2600) frames.push(t - last); last = t; if (a) ops.push(+getComputedStyle(a).opacity); if (run) requestAnimationFrame(tick); };
    // the frames of the page idle, before (a headless machine's own pace, for comparison)
    const idle = await new Promise(r => { const f = []; let l = performance.now(); const k = t => { f.push(t - l); l = t; if (f.length < 90) requestAnimationFrame(k); else r(f.slice(2)); }; requestAnimationFrame(k); });
    window.__idleSlow = idle.filter(d => d > 34).length;
    SortTL.batchStart(['3710000008'], { how: 'typed' });
    SortTL.etsyReceipt('3710000008', { status: 'Canceled' });
    requestAnimationFrame(tick);
    SortTL.batchLoaded();
    await new Promise(r => setTimeout(r, 8000));      // the scan's check (3.5 s here) settles, then the alert runs its two flashes
    run = false;
    return { frames: frames.slice(1), ops, idleSlow: window.__idleSlow };
  });
  const mid = measured.ops.filter(o => o > 0.05 && o < 0.95).length;
  check(measured.ops.length && mid >= 2, `the alert fades in over visible frames (${mid} frames between 0 and 1)`);
  const slow = measured.frames.filter(d => d > 34);
  if (process.env.DEBUG_FRAMES) console.log(measured.frames.map((d, i) => d > 34 ? i + ':' + d.toFixed(0) : '').filter(Boolean).join(' '));
  // flagged, not failed: on a shared, loaded machine a headless frame can run late whatever the page does (the idle
  // count beside it says how busy the machine was); the long-task and animated-property checks below are the hard ones
  (slow.length ? console.log : () => {})('flag ' + `frames over 34 ms while it opens and flashes`);
  check(slow.length <= Math.max(2, measured.frames.length * 0.03), `no run of frames over 34 ms while it opens and flashes (${slow.map(x => x.toFixed(0)).join(',') || 'none'} of ${measured.frames.length}; idle page: ${measured.idleSlow} of 88 over 34 ms)`);
  const lt = await page.evaluate(() => __lt.filter(d => d > 50));
  check(lt.length === 0, 'no long task over 50 ms: ' + lt.join(','));
  const flashProps = await page.evaluate(() => {
    // the flash keyframes: every property they animate (the fade and the slam are opacity/transform)
    const out = new Set();
    for (const sh of document.styleSheets) { let rules; try { rules = sh.cssRules; } catch (_) { continue; }
      for (const r of rules) if (r.type === CSSRule.KEYFRAMES_RULE && /^st(Alarm|Slam|Fade)/.test(r.name)) for (const k of r.cssRules) for (let i = 0; i < k.style.length; i++) out.add(k.style[i]); }
    return [...out];
  });
  const bad = flashProps.filter(p => !/^(opacity|transform|clip-path)$/.test(p));
  check(bad.length === 0, 'the alert animates only opacity/transform (per-frame paint of: ' + (bad.join(', ') || 'none') + ')');
  await page.click('#stCancelAlert .st-ok');
  await page.waitForFunction(() => !document.querySelector('#stCancelAlert'), null, { timeout: 3000 });

  // ── 4 · one sticker of a cancelled order: the guard's answer is kept ──
  st.delay = 50;
  const frames = () => page.evaluate(() => [...document.querySelectorAll('iframe')].filter(f => /QR%20Printer|QR Printer/.test(f.src)).length);
  const before = await frames();
  const iB2 = await page.evaluate(b => window.cachedOrderItems.findIndex(t => String(t.receipt_id) === b), B);
  await page.evaluate(i => openIframePrinterForListing(i), iB2);
  const bar = await page.waitForSelector('.sttl-bar.in', { timeout: 4000 }).then(() => true, () => false);
  check(bar, 'printing a cancelled order asks "Do it anyway?"');
  if (bar) {
    await page.click('.sttl-bar .sttl-btn.stop');
    await page.waitForTimeout(500);
    check((await frames()) === before, 'Stop: the sticker is not printed');
    await page.waitForTimeout(300);
    await page.evaluate(i => openIframePrinterForListing(i), iB2);
    await page.waitForSelector('.sttl-bar.in', { timeout: 4000 });
    await page.click('.sttl-bar .sttl-btn.go');
    await page.waitForTimeout(700);
    const after = await page.evaluate(() => ({ alert: !!document.querySelector('#stCancelAlert'), printed: JSON.parse(localStorage.getItem('qrPrintAll') || '{}').userTypedOrderNum }));
    check((await frames()) === before + 1 && after.printed === B, '"Do it anyway": the sticker is printed, as the timeline then says: ' + JSON.stringify(after));
    check(!after.alert, 'and no second full-screen alert overrules the answer');
    await page.evaluate(() => OrderTimeline.flush());
    await page.waitForTimeout(400);
    const anyway = st.events.filter(e => e.orderId === B && e.type === 'note' && /Went ahead/.test(e.text || '')).length;
    const sorted = st.events.filter(e => e.orderId === B && e.type === 'sorted').length;
    check(anyway === 1 && sorted === 1, `the timeline says went ahead (${anyway}) and sorted (${sorted}), and both are true`);
  }

  // ── 5 · a second batch within 2.5 s of the first: the first batch's late "cancelled" still raises the alert naming it ──
  const D = '3710000011', E = '3710000012';
  st.delay = 3500; st.cancelled.add(D);
  await page.evaluate(([d, e]) => {
    SortTL.batchStart([d], { how: 'typed', batch: 'first' });
    setTimeout(() => { SortTL.batchStart([e], { how: 'typed', batch: 'second' }); SortTL.batchLoaded(); }, 1000);
  }, [D, E]);
  const earlier = await page.waitForFunction(d => { const a = document.querySelector('#stCancelAlert.on'); return a && [...a.querySelectorAll('.st-list .st-who')].some(r => r.dataset.order === d); }, D, { timeout: 12000 })
    .then(() => true, () => false);
  check(earlier, "an earlier batch's cancelled order answering late still raises the alert naming it (a second batch loaded 1 s later)");
  if (earlier) await page.click('#stCancelAlert .st-ok');

  await browser.close();
  check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  if (failures.length) { console.error('\n' + failures.length + ' failed'); process.exit(1); }
  console.log('adv-sorting: all passed');
})().catch(e => { console.error(e); process.exit(1); });
