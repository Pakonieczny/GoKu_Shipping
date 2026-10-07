// The Inbox's Etsy API-meter panel (etsy-mail-1.html), in a real browser on the fake shop, with the fake clock (Firebase cost emergency, FC14):
//   · active (a person is working, or the counter is moving): one read every 3 s, exactly as before
//   · nobody has touched the page and the counter has not moved for 2 minutes: one read every 12 s
//   · the first click, key, scroll or touch, or the counter moving, takes it back to 3 s
//   · a hidden tab asks nothing at all; coming back asks at once
//   · errors back off (6 s, 12 s, 24 s, 48 s, 60 s) instead of retrying every 3 s
//
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules NO_CLOCK_WORKER=1 node tests/cost/fc14-inbox-meter.cjs
//
// The test answers the meter's own request itself (page.route), so it can move the counter or fail it on demand. Nothing leaves the machine
// (the fake shop answers every other function; no sign-in, no PIN, no paid or Etsy call).
'use strict';
const path = require('path'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || '/opt/node22/lib/node_modules/playwright/node_modules';
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROME = process.env.CHROMIUM || (() => { try { const d = '/opt/pw-browsers'; const c = fs.readdirSync(d).filter(x => /^chromium-/.test(x)).sort().pop(); return c ? path.join(d, c, 'chrome-linux/chrome') : undefined; } catch (_) { return undefined; } })();
const { World, sleep } = require(path.join(root, 'tests/stations/all-stations/world.cjs'));
const say = s => process.stdout.write(s + '\n');

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const W = await World.start({ now: '2026-10-07T13:00:00Z', browser });
  let failed = 0;
  const check = (name, ok, detail) => { say((ok ? 'ok   ' : 'FAIL ') + name + (detail ? '  (' + detail + ')' : '')); if (!ok) failed++; };
  let hits = 0, mode = 'ok', total = 1000;
  const run = async ms => { const a = hits; await W.advance(ms, { step: 1000, settle: 25 }); return hits - a; };

  try {
    const ctx = await W.context({ label: 'inbox-meter' });
    await ctx.route(/firestoreProxy\?.*etsyApiCounters/, route => {
      hits++;
      if (mode === 'error') return route.fulfill({ status: 500, contentType: 'application/json', body: '{"error":"x"}' });
      route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ exists: true, doc: { day: '2026-10-07', grandTotal: total, sites: {}, updatedAt: total } }) });
    });
    const page = await W.open(ctx, 'etsy-mail-1.html');
    await sleep(3500);
    await W.advance(2000, { step: 1000, settle: 40 });

    // 1. right after load a person is "there": 3 s
    let n = await run(60000);
    check('active: about one read every 3 s (60 s => 18 to 22)', n >= 18 && n <= 22, n + ' reads');

    // 2. quiet for two minutes, then a minute of quiet polling
    await run(130000);
    n = await run(60000);
    check('quiet 2 min: about one read every 12 s (60 s => 4 to 6)', n >= 4 && n <= 6, n + ' reads');

    // 3. a click takes it back to 3 s
    await page.evaluate(() => document.dispatchEvent(new Event('pointerdown', { bubbles: true })));
    await run(14000);   // (the pending 12 s wait ends)
    n = await run(30000);
    check('after a click: about one read every 3 s (30 s => 9 to 11)', n >= 9 && n <= 11, n + ' reads');

    // 4. the counter moving keeps it at 3 s with nobody touching the page
    await run(130000);
    for (let i = 0; i < 6; i++) { total++; await run(10000); }
    n = await run(30000);
    check('counter moving: quick polling resumes without any input', n >= 8, n + ' reads in the 30 s after the last move');

    // 5. hidden: nothing; visible again: at once
    await page.evaluate(() => {
      Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' });
      document.dispatchEvent(new Event('visibilitychange'));
    });
    await run(5000);
    n = await run(300000);
    check('hidden for 5 minutes: no reads', n === 0, n + ' reads');
    await page.evaluate(() => {
      Object.defineProperty(document, 'hidden', { configurable: true, get: () => false }); Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'visible' });
      document.dispatchEvent(new Event('visibilitychange'));
    });
    n = await run(2000);
    check('visible again: reads at once', n >= 1, n + ' reads in 2 s');

    // 6. errors back off: 5 minutes of a failing answer is far fewer than the 100 reads a 3 s retry would make
    mode = 'error';
    await run(15000);
    n = await run(300000);
    check('failing for 5 minutes: backs off (at most 10 reads, a 3 s retry would be 100)', n <= 10, n + ' reads');
    mode = 'ok';
    await W.closeContext(ctx);
  } catch (e) { say('FAIL error: ' + (e && e.stack || e)); failed++; }
  W.stop(); await browser.close();
  say(failed ? '\nFAILED ' + failed : '\nok');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error('FAILED', e && e.stack || e); process.exit(1); });
