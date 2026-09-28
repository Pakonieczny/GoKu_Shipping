// Orders › Cancelled (Paul, 28 Sep, A5): a tab that is always there and lists every cancelled order, Etsy's (found by
// the system, source "etsy") and a person's, newest first. Opens the sorter in headless Chromium against the local fake
// site (bridge-server.cjs); every request that is not to the loopback is aborted. The library's cancel records are this
// test's own: charmNestLibrary's cancelList / cancelCheck / cancelRestore are answered here, everything else goes on to
// the fake server. Checks: the tab with its count at 0 and after, each row (order, buyer, SKUs, when, "Cancelled on
// Etsy" / "Cancelled by <name>", the reason, what happened on each sheet), no Restore for Etsy's, the search (order,
// buyer, SKU), paging past 200, a click opening the order view (OrderWin.openOrder, from the row) and nothing before it
// exists, Restore asked inline (no pop-up) then done, a cancel arriving live flying into the tab (and taken in at the
// top when the tab is shown), the pile switch animating, and the old callers (showPile, showCancelled).
//   node tests/charm-nest/cancelled-tab.cjs [playwright-core dir]      (SHOTS=<dir> also saves screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';

const NOW = Date.now(), H = 3600000;
const ETSY = { orderId: '4100000001', source: 'etsy', by: 'Etsy', why: 'Buyer requested cancellation', at: NOW - 1 * H, buyer: 'Ada Lovelace', placedAt: NOW - 30 * H, shipBy: 0,
  sheets: ['GF Sheet 2'], fates: [{ sheet: 'GF Sheet 2', fate: 'removed' }, { sheet: 'SS Sheet 1', fate: 'cut' }], lines: [{ transactionId: '1', sku: 'GF-HEART-01', title: 'Heart charm', quantity: 2, material: 'gold' }] };
const PERSON = { orderId: '4100000002', by: 'Anna', why: 'Customer changed their mind', at: NOW - 2 * H, buyer: 'Grace Hopper', placedAt: NOW - 40 * H, shipBy: 0,
  sheets: ['GF Sheet 3'], lines: [{ transactionId: '2', sku: 'SS-STAR-07', title: 'Star charm', quantity: 1, material: 'silver' }] };
const OLD_ETSY = { orderId: '4100000003', by: 'Etsy', why: '', at: NOW - 3 * H, buyer: 'Alan Turing', sheets: [], lines: [{ transactionId: '3', sku: 'RG-MOON-02', title: 'Moon', quantity: 1 }] };
const FILLER = Array.from({ length: 227 }, (_, i) => ({ orderId: String(4200000000 + i), by: 'Operator', why: 'test', at: NOW - 4 * H - i * 60000, buyer: 'Buyer ' + i, sheets: [], lines: [{ transactionId: String(i), sku: 'SKU-' + i, title: 't', quantity: 1 }] }));

(async () => {
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  let failed = false;
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await ctx.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; window.confirm = () => { window.__popup = true; return true; }; });
    const page = await ctx.newPage(), errors = [];
    page.setDefaultTimeout(15000);
    page.on('pageerror', e => errors.push(e.message));
    // the library's cancel records: this test's own list, answered as charmNestLibrary answers them
    let records = [];
    const asked = [];
    await page.route(/\/\.netlify\/functions\/charmNestLibrary/, r => {
      let b = {}; try { b = r.request().postDataJSON() || {}; } catch (_) {}
      const ok = body => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
      if (b.op === 'cancelList') {
        asked.push(b);
        if (b.idsOnly) return ok({ ids: records.map(c => c.orderId), truncated: false });
        const n = Math.max(1, Math.min(500, Math.round(+b.limit) || 200)), s = records.slice().sort((a, c) => c.at - a.at);
        return ok({ list: s.slice(0, n), truncated: s.length >= n });
      }
      if (b.op === 'cancelCheck') { asked.push(b); const out = {}; for (const id of b.orderIds || []) { const c = records.find(x => x.orderId === String(id)); if (c) out[id] = { at: c.at, by: c.by, why: c.why, source: c.source || (c.by === 'Etsy' ? 'etsy' : 'sorter'), sheets: c.sheets }; } return ok({ cancelled: out, now: Date.now() }); }
      if (b.op === 'cancelRestore') { asked.push(b); records = records.filter(c => c.orderId !== String(b.orderId)); return ok({ ok: true }); }
      return r.continue();
    });
    const shot = async name => { if (SHOTS) { await page.waitForTimeout(450); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } };
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Cancelled && window.OrderWin && window.Motion && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async () => { await Cancelled.load(true); CN.setMode('orders'); Orders.renderNow(); });

    /* 1 · the tab is always there, with its count, even at 0 */
    const chip = () => page.evaluate(() => { const b = document.querySelector('#ordChips [data-pile="cancelled"]'); return b ? { text: b.textContent.trim(), on: b.classList.contains('on'), n: b.querySelector('b').textContent } : null; });
    let c0 = await chip();
    assert(c0 && c0.n === '0' && /^Cancelled/.test(c0.text), 'Cancelled is in the Orders bar with 0: ' + JSON.stringify(c0));
    const oneLine = await page.evaluate(() => { const bar = document.getElementById('ordBar'), b = bar.querySelector('[data-pile="cancelled"]'), o = bar.querySelector('[data-pile=""]'); return Math.abs(b.getBoundingClientRect().top - o.getBoundingClientRect().top) < 2 && b.getBoundingClientRect().height <= 30; });
    assert(oneLine, 'the tab sits on the bar\'s one thin line, beside Open Orders');
    await page.click('#ordChips [data-pile="cancelled"]');
    await page.waitForFunction(() => /No order has been cancelled/.test(document.getElementById('ordBody').textContent));

    /* 2 · the records come: the count, and each row */
    records = [ETSY, PERSON, OLD_ETSY, ...FILLER];
    await page.evaluate(async () => { await Cancelled.load(true); await Cancelled.history(); Orders.render(); });
    await page.click('#ordChips [data-pile=""]');
    const switched = await page.evaluate(() => new Promise(res => { document.querySelector('#ordChips [data-pile="cancelled"]').click(); res(document.getElementById('ordBody').getAnimations().length); }));
    assert(switched >= 1, 'switching to Cancelled slides the list in, as the other piles do');
    await page.waitForFunction(() => document.querySelectorAll('#ordBody .cxRow').length === 200);
    const cxIn = await page.evaluate(() => document.querySelectorAll('#ordBody .cxRow.cxIn').length);
    assert(cxIn >= 10, 'the rows come in with a soft slide and fade: ' + cxIn);
    c0 = await chip();
    assert(c0.on && c0.n === '230', 'the tab counts every cancelled order: ' + JSON.stringify(c0));
    const bar = await page.evaluate(() => ({ cls: document.getElementById('ordBar').className, ph: document.getElementById('ordQ').placeholder, sort: document.getElementById('ordSort').getClientRects().length }));
    assert(/onCx/.test(bar.cls) && /buyer/.test(bar.ph) && bar.sort === 0, 'on Cancelled the search reads order, buyer, SKU and the open-lines controls step aside: ' + JSON.stringify(bar));
    const row = rid => page.evaluate(rid => { const n = [...document.querySelectorAll('#ordBody .cxRow')].find(x => x.dataset.rid === rid); if (!n) return null; return { i: [...n.parentElement.children].indexOf(n), id: n.querySelector('.cxId').textContent.replace(/\s+/g, ' ').trim(), what: n.querySelector('.cxWhat').textContent, badge: n.querySelector('.cxBadge').textContent, time: n.querySelector('time').textContent, reason: n.querySelector('.cxReason').textContent, fates: [...n.querySelectorAll('.cxFates span')].map(s => s.className + ':' + s.textContent), restore: !!n.querySelector('[data-cx=restore]'), open: n.classList.contains('open') }; }, rid);
    const a = await row(ETSY.orderId), p = await row(PERSON.orderId), o = await row(OLD_ETSY.orderId);
    assert.equal(a.i, 0, 'newest first'); assert.equal(p.i, 1); assert.equal(o.i, 2);
    assert(/4100000001/.test(a.id) && /Ada Lovelace/.test(a.id) && /GF-HEART-01 ×2/.test(a.what) && a.time, 'order, buyer, SKUs and when: ' + JSON.stringify(a));
    assert.equal(a.badge, 'Cancelled on Etsy'); assert.equal(a.reason, 'Buyer requested cancellation');
    assert.deepEqual(a.fates, ['off:taken off GF Sheet 2', 'cut:already cut on SS Sheet 1: set aside'], 'what happened on each sheet');
    assert.equal(a.restore, false, 'no Restore for Etsy\'s cancel');
    assert.equal(p.badge, 'Cancelled by Anna'); assert.equal(p.reason, 'Customer changed their mind'); assert.deepEqual(p.fates, ['off:taken off GF Sheet 3']); assert.equal(p.restore, true, 'a person\'s cancel keeps Restore');
    assert.equal(o.badge, 'Cancelled on Etsy', 'an older record by "Etsy" without a source is Etsy\'s'); assert.equal(o.reason, 'Etsy gave no reason'); assert.equal(o.restore, false);
    assert.equal(a.open, false, 'before the order view exists a row opens nothing');
    await shot('cancelled-list');

    /* 3 · the search: order number, buyer, SKU */
    // (typing holds the list's redraw a moment, as it does for every list: the rows are waited for)
    const only = async (q, rid, what) => {
      await page.fill('#ordQ', q);
      try { await page.waitForFunction(rid => { const r = [...document.querySelectorAll('#ordBody .cxRow')]; return r.length === 1 && r[0].dataset.rid === rid; }, rid, { timeout: 4000 }); }
      catch (_) { assert.fail(what + ': ' + JSON.stringify(await page.evaluate(() => [...document.querySelectorAll('#ordBody .cxRow')].slice(0, 5).map(n => n.dataset.rid)))); }
    };
    await only('grace', PERSON.orderId, 'by buyer');
    await only('gf-heart', ETSY.orderId, 'by SKU');
    await only('4100000003', OLD_ETSY.orderId, 'by order number');
    assert(/1\b.*of 200 match/.test(await page.textContent('#ordBody .cxSum')), 'says how many match');
    await page.fill('#ordQ', 'nobody-at-all');
    await page.waitForSelector('#ordBody [data-cx=all]');
    await page.click('#ordBody [data-cx=all]');
    await page.waitForFunction(() => document.querySelectorAll('#ordBody .cxRow').length === 200 && document.getElementById('ordQ').value === '');

    /* 4 · paging past 200: older ones read from the library */
    await page.click('#ordBody .cxFoot .listMore');
    await page.waitForFunction(() => document.querySelectorAll('#ordBody .cxRow').length === 230);
    assert(asked.some(b => b.op === 'cancelList' && b.limit === 400), 'the next page asked the library for more');
    const older = await row('4200000226');
    assert(older && older.i === 229, 'the oldest is last');

    /* 5 · a click opens the order view, from the row */
    await page.evaluate(() => { OrderWin.openOrder = (rid, opts) => { window.__opened = { rid, from: opts && opts.from && opts.from.dataset.rid }; }; Orders.renderBody(); });
    await page.waitForFunction(() => document.querySelector('#ordBody .cxRow.open'));
    await page.click(`#ordBody .cxRow[data-rid="${ETSY.orderId}"] .cxWhy`);
    assert.deepEqual(await page.evaluate(() => window.__opened), { rid: ETSY.orderId, from: ETSY.orderId }, 'OrderWin.openOrder(rid, { from: row })');
    await page.evaluate(() => { window.__opened = null; });
    await page.click(`#ordBody .cxRow[data-rid="${PERSON.orderId}"] [data-cx=restore]`);
    assert.equal(await page.evaluate(() => window.__opened), null, 'Restore does not open the order');

    /* 6 · Restore asks inline (never a pop-up), Keep leaves it, Restore again brings it back */
    const ask = await page.evaluate(rid => { const n = document.querySelector(`#ordBody .cxRow[data-rid="${rid}"]`); return { asking: n.classList.contains('asking'), text: (n.querySelector('.cxAskBar') || {}).textContent, same: n.getBoundingClientRect().height, dialogs: document.querySelectorAll('dialog[open]').length, popup: !!window.__popup }; }, PERSON.orderId);
    assert(ask.asking && /Bring it back\?/.test(ask.text) && !ask.dialogs && !ask.popup, 'an inline confirm, no pop-up: ' + JSON.stringify(ask));
    await shot('cancelled-restore-ask');
    await page.click(`#ordBody .cxRow[data-rid="${PERSON.orderId}"] [data-cx=no]`);
    assert(await page.evaluate(rid => { const n = document.querySelector(`#ordBody .cxRow[data-rid="${rid}"]`); return !n.classList.contains('asking') && !!n.querySelector('[data-cx=restore]'); }, PERSON.orderId), 'Keep leaves it cancelled');
    assert(!asked.some(b => b.op === 'cancelRestore'), 'nothing restored yet');
    await page.click(`#ordBody .cxRow[data-rid="${PERSON.orderId}"] [data-cx=restore]`);
    await page.click(`#ordBody .cxRow[data-rid="${PERSON.orderId}"] [data-cx=yes]`);
    await page.waitForFunction(rid => !document.querySelector(`#ordBody .cxRow[data-rid="${rid}"]`), PERSON.orderId);
    assert(asked.some(b => b.op === 'cancelRestore' && b.orderId === PERSON.orderId), 'restored in the library');
    await page.waitForFunction(() => document.querySelector('#ordChips [data-pile="cancelled"] b').textContent === '229');
    await page.waitForFunction(() => !document.querySelector('#motionLayer .mGhost'), null, { timeout: 5000 });

    /* 7 · a cancel arriving live flies into the tab, whose count answers */
    await page.click('#ordChips [data-pile=""]');
    const LIVE = { orderId: '4100000009', source: 'etsy', by: 'Etsy', why: 'Out of stock', at: Date.now(), buyer: 'Live Buyer', sheets: ['SS Sheet 4'], lines: [{ sku: 'SS-LIVE-1', quantity: 1 }] };
    records.push(LIVE);
    await page.evaluate(() => { window.__tok = null; new MutationObserver((m, ob) => { const t = document.querySelector('#motionLayer .mGhost .cxToken'); if (t) { window.__tok = t.textContent; ob.disconnect(); } }).observe(document.body, { childList: true, subtree: true }); Cancelled.load(true); });
    await page.waitForFunction(() => window.__tok, null, { timeout: 5000 });
    assert(/4100000009/.test(await page.evaluate(() => window.__tok)) && /Cancelled on Etsy/.test(await page.evaluate(() => window.__tok)), 'a token for the new cancel flies');
    await shot('cancelled-live-flight');
    await page.waitForFunction(() => document.querySelector('#ordChips [data-pile="cancelled"] b').textContent === '230');
    await page.waitForFunction(() => document.querySelector('.mPlus'), null, { timeout: 4000 });
    await page.waitForFunction(() => !document.querySelector('#motionLayer .mGhost'), null, { timeout: 5000 });
    await page.click('#ordChips [data-pile="cancelled"]');
    await page.waitForFunction(rid => { const n = document.querySelector('#ordBody .cxRow'); return n && n.dataset.rid === rid && n.classList.contains('mFound'); }, LIVE.orderId);

    /* … and, the tab shown, a new one is taken in at the top */
    const LIVE2 = { orderId: '4100000010', by: 'Ben', why: 'Duplicate order', at: Date.now() + 5, buyer: 'Second', sheets: ['GF Sheet 5 (cut)'], lines: [{ sku: 'GF-X', quantity: 1 }] };
    records.push(LIVE2);
    await page.evaluate(() => Cancelled.load(true));
    await page.waitForFunction(rid => { const n = document.querySelector('#ordBody .cxRow'); return n && n.dataset.rid === rid; }, LIVE2.orderId);
    const l2 = await row(LIVE2.orderId);
    assert.equal(l2.badge, 'Cancelled by Ben'); assert.deepEqual(l2.fates, ['cut:already cut on GF Sheet 5: set aside']);
    await page.waitForFunction(() => document.querySelector('#ordChips [data-pile="cancelled"] b').textContent === '231');

    /* 8 · the old callers: showPile("cancelled", rid) and showCancelled(rid), from another tab */
    await page.evaluate(() => CN.setMode('review'));
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('cancelled', '4200000150'); });
    await page.waitForFunction(() => { const r = [...document.querySelectorAll('#ordBody .cxRow')]; return r.length === 1 && r[0].dataset.rid === '4200000150'; });
    await page.evaluate(() => CN.setMode('review'));
    await page.evaluate(() => Orders.showCancelled('4200000200'));
    await page.waitForFunction(() => { const n = document.querySelector('#ordBody .cxRow[data-rid="4200000200"]'); return n && n.classList.contains('mFound') && document.getElementById('ordQ').value === ''; });
    assert.equal(await page.evaluate(() => CN.S.mode), 'orders', 'showCancelled opens the Orders tab');

    assert.deepEqual(errors, [], 'no page errors');
    console.log('Cancelled tab OK: always there with its count, Etsy\'s and a person\'s rows with who, when, why and each sheet, search, paging past 200, the order view from a row, inline Restore, live arrivals flying in, old callers');
  } catch (e) { failed = true; console.error(e); }
  finally { await browser.close(); srv.close(); }
  if (failed) process.exit(1);
})();
