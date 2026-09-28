// Adversarial (station leftovers): single-flight buttons and the phone relay behind the red alert, with the real module.
//   1 · weld-1, assembly-1, shipping-1 (the three Complete Order variants): a double-click on Complete Order posts to
//       Etsy once; the button is disabled at once and comes back after; a failure re-enables it with a calm message
//   2 · weld-1: Buy & Print is single-flight too, even when other code re-enables the button mid-flight
//   3 · weld-1: a phone scan relayed while the red CANCELLED alert is up keeps the focus on the alert, typed keys never
//       reach the fields behind it, and the relayed order waits: after Understood the focus comes back and it loads
// Firebase, Materialize and every function are stubbed (see adv-station-alert.cjs); nothing leaves the test origin.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/adv-station-flight.cjs
'use strict';
const path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { open } = require('./adv-station-alert.cjs');

const CANC = '3521000601', NEXT = '3521000602', OK = '3521000603';
const wait = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const done = [], fails = [];
  const part = async (name, fn) => { let c = null; try { await fn(x => { c = x; }); } catch (e) { fails.push(name + ': ' + e.message); console.log('FAIL ' + name + ': ' + e.message); } finally { if (c) await c.close().catch(() => {}); } };
  try {
    /* 1 · Complete Order: one post per press, however fast the second press comes */
    for (const file of ['weld-1.html', 'assembly-1.html', 'shipping-1.html']) await part(file, async keep => {
      const st = { calls: [], events: [], cancelled: new Set(), etsyStatus: {} };
      const { ctx, page } = await open(browser, file, st); keep(ctx);
      const track = { posts: 0, fail: false };
      await page.route('**/.netlify/functions/trackOrderProxy', async r => {
        if (r.request().method() !== 'POST') return r.fulfill({ status: 200, body: 'ok' });
        track.posts++;
        await wait(700);                                            // Etsy takes its time
        return track.fail ? r.fulfill({ status: 502, contentType: 'application/json', body: JSON.stringify({ error: 'Etsy is busy' }) })
                          : r.fulfill({ status: 200, contentType: 'application/json', body: '{"ok":true}' });
      });
      const fillIn = () => page.evaluate(id => {
        document.getElementById('etsyOrderNumber').value = id;
        document.getElementById('trackingNumberInput').value = 'TRK' + id;
        const c = document.getElementById('carrierSelect'); c.value = 'usps';
      }, OK);
      const disabled = () => page.evaluate(() => document.getElementById('completeOrderBtn').disabled);
      // Shipping shows the button (a real double-click); Welding and Assembly keep it hidden, so two quick presses in code
      const press = () => page.evaluate(() => document.getElementById('completeOrderBtn').click());
      await fillIn();
      if (await page.isVisible('#completeOrderBtn')) await page.dblclick('#completeOrderBtn');
      else await page.evaluate(() => { const b = document.getElementById('completeOrderBtn'); b.click(); b.click(); });
      assert.strictEqual(await disabled(), true, file + ': Complete Order is disabled at once');
      await press();                                                 // a third press in the flight
      await wait(1800);
      assert.strictEqual(track.posts, 1, file + ': a double-click completes the order on Etsy once, not ' + track.posts);
      assert.strictEqual(await disabled(), false, file + ': the button comes back after the post');
      //   a failure: the button comes back, with a calm message; a second press then posts again
      track.fail = true; await fillIn();
      await press();
      await wait(1500);
      assert.strictEqual(await disabled(), false, file + ': a failed Complete Order re-enables the button');
      const last = await page.evaluate(() => (window.__toasts || []).slice(-1)[0] || '');
      assert(/try again/i.test(last) && !/failed/i.test(last), file + ': the failure message is calm and says what to do: ' + last);
      assert.strictEqual(await page.evaluate(() => document.getElementById('trackingNumberInput').value), 'TRK' + OK, file + ': the tracking number typed is kept');
      track.fail = false;
      await press(); await wait(1500);
      assert.strictEqual(track.posts, 3, file + ': a press after the failure posts again');
      done.push(file + ': Complete Order posts once per flight; a failure re-enables it calmly');
    });

    /* 2 · Buy & Print: a second press in the flight is ignored, even if other code re-enables the button */
    await part('weld-1 Buy & Print', async keep => {
      const st = { calls: [], events: [], cancelled: new Set(), etsyStatus: {} };
      const { ctx, page } = await open(browser, 'weld-1.html', st); keep(ctx);
      await page.evaluate(() => {
        window.__buyFlights = 0; const g = StationTimeline.guard;
        StationTimeline.guard = function (id, o) { if (o && o.action === 'Buy & Print') window.__buyFlights++; return g.apply(this, arguments); };
        document.getElementById('etsyOrderNumber').value = '3521000603';
        document.getElementById('ccShipmentId').value = 'SHIP1';
        const b = document.getElementById('ccBuy'); b.disabled = false; b.click();
        b.disabled = false; b.click();                              // e.g. Update ID finishing re-enables the button
      });
      await wait(300);
      assert.strictEqual(await page.evaluate(() => window.__buyFlights), 1, 'weld-1: Buy & Print runs once per flight');
      done.push('weld-1: Buy & Print ignores a second press in its flight');
    });

    /* 3 · the phone relay while the red alert is up */
    await part('relay', async keep => {
      const st = { calls: [], events: [], cancelled: new Set([CANC]), etsyStatus: {} };
      const { ctx, page } = await open(browser, 'weld-1.html', st); keep(ctx);
      await page.fill('#etsyOrderNumber', CANC); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter');
      await page.waitForSelector('.sttl-alert.in', { timeout: 8000 });
      await wait(500);
      const inAlert = () => page.evaluate(() => !!(document.activeElement && document.activeElement.closest('.sttl-alert')));
      assert(await inAlert(), 'the alert has the focus');
      await page.evaluate(n => { const s = window.__snaps.filter(x => x.id === 'weld-scan-1').pop();
        s.cb({ exists: true, id: 'weld-scan-1', data: () => ({ 'Order Number': n }), get: k => (k === 'Order Number' ? n : undefined) }); }, NEXT);
      await wait(400);
      assert(await inAlert(), 'a relayed scan leaves the focus on the alert (not ' + await page.evaluate(() => document.activeElement && (document.activeElement.id || document.activeElement.tagName)) + ')');
      assert.strictEqual(await page.inputValue('#etsyOrderNumber'), CANC, 'the relayed number does not reach the order field behind the alert');
      await page.keyboard.type('77', { delay: 5 });
      await page.focus('#trackingNumberInput').catch(() => {});      // page code moving the focus behind it
      assert(await inAlert(), 'focus moved behind the alert is taken back');
      await page.keyboard.type('55', { delay: 5 });
      assert.strictEqual(await page.inputValue('#etsyOrderNumber'), CANC, 'typed keys do not reach the order field');
      assert.strictEqual(await page.inputValue('#trackingNumberInput'), '', 'typed keys do not reach the tracking field');
      assert.strictEqual(await page.evaluate(() => StationTimeline.alertOpen()), true, 'the alert is still up');
      await page.click('.sttl-ok');
      await page.waitForFunction(() => !document.querySelector('.sttl-alert'), null, { timeout: 3000 });
      assert.strictEqual(await page.evaluate(() => document.activeElement && document.activeElement.id), 'etsyOrderNumber', 'Understood gives the focus back to the order field');
      await page.waitForFunction(n => window.__rec.some(e => e.type === 'scan' && e.orderId === n), NEXT, { timeout: 8000 })
        .catch(() => { throw new Error('the relayed scan held behind the alert was lost'); });
      assert.strictEqual(await page.inputValue('#etsyOrderNumber'), NEXT, 'the held relayed order loads after Understood');
      done.push('weld-1: the relay keeps the focus on the alert, no typing reaches behind it, the scan loads after Understood');
    });
    if (fails.length) throw new Error(fails.length + ' failed:\n  ' + fails.join('\n  '));
    console.log('adv-station-flight OK\n  ' + done.join('\n  '));
  } finally { await browser.close(); }
})().catch(e => { console.error(e); process.exit(1); });
