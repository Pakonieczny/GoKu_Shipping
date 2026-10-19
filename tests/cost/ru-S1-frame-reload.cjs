// RU-S1 (Firestore cost, 18 Oct 2026): the Design Station frame the Sorter keeps (charm-nest-bridge.js DesignLink.reloadFrame) was loaded again every 45 s for as long as the
// station stayed quiet after a reload (sent to an Etsy sign-in page, a script that stops at start): a whole station start each time, about 20 reads, 1,900 reloads a day for a
// tab left in that state. The first retry still comes after 45 s, then the wait doubles with each reload that did not bring the station back (90 s, 3 min, 6 min, 10 min) and starts over once it answers; a network
// that has just come back is given the 45 s alone; a station that has spoken since it loaded is never reloaded under a person (unchanged).
// The real page over the bridge test server, a stand-in frame that never answers, and a clock the test moves one second at a time for one hour.
//   node tests/cost/ru-S1-frame-reload.cjs [playwright-core dir]
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }; const say = s => process.stdout.write(s + '\n');
let chromium = null;
for (const d of [process.argv[2], process.env.PW_DIR, path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].filter(Boolean)) { try { ({ chromium } = require(path.join(d, 'playwright-core'))); break; } catch (_) { /* next */ } }
const CHROME = process.env.CHROMIUM || (() => { try { const d = '/opt/pw-browsers'; const c = fs.readdirSync(d).filter(x => /^chromium-/.test(x)).sort().pop(); return c ? path.join(d, c, 'chrome-linux/chrome') : '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'; } catch (_) { return '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'; } })();

(async () => {
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  ok(/reloadsQuiet = 0; reloadFrame\("the network is back"\)/.test(bridge) && /S_\.up = true; S_\.misses = 0; S_\.reloadsQuiet = 0;/.test(bridge) && /S_\.up = true; S_\.reloadsQuiet = 0; agent/.test(bridge), 'the count starts over when the station answers and when the network is back');
  if (!chromium || !fs.existsSync(CHROME)) { say('browser part skipped: no playwright-core or chrome'); say(`ru-S1-frame-reload OK · ${n} checks`); return; }
  const { start } = require('../charm-nest/bridge-server.cjs');
  const srv = await start({ receipts: [] }), { sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1300, height: 900 } });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: '' }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.addInitScript(() => { window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; try { localStorage.setItem('cn.employee', 'Tester'); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; localStorage.setItem('cn.settings', JSON.stringify(Object.assign({ v: 26 }, s))); } catch (_) {} });
    const page = await ctx.newPage(), errs = [];
    page.on('pageerror', e => errs.push(e.message));
    await page.goto(sorterOrigin + '/charm-nest-1.html');
    await page.waitForFunction(() => window.DesignLink && DesignLink._reloadFrame && window.CN && CN.S && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    const out = await page.evaluate(() => {
      const L = DesignLink, S = L._S, realNow = Date.now, srcs = [];
      const frame = { get src() { return srcs[srcs.length - 1] || ''; }, set src(v) { srcs.push(v); }, contentWindow: null };
      const reset = () => { S.frame = frame; S.up = false; S.heardAt = 0; S.loadedAt = 0; S.reloadedAt = 0; S.reloadsQuiet = 0; srcs.length = 0; };
      let t = realNow.call(Date) + 1e6; Date.now = () => t;
      const hour = (fn) => { const at = []; for (let s = 1; s <= 3600; s++) { t += 1000; if (fn && fn(s)) continue; if (L._reloadFrame('the test')) at.push(s); } return at; };
      try {
        reset(); const quiet = hour();
        // the station answers (hello, or a heartbeat that finds it back) after the third reload: the count starts over, a later silence is given 45 s again
        reset(); let lost = 0; const back = hour(s => { if (s === 400) { S.up = true; S.reloadsQuiet = 0; } if (s === 401) { S.up = false; lost = s; } return false; });
        // a network that came back: the 45 s alone, however many reloads went before
        reset(); S.reloadsQuiet = 6; S.reloadedAt = t; t += 46000; const slow = L._reloadFrame('x'); S.reloadsQuiet = 0; const fresh = L._reloadFrame('the network is back');   // (the online handler starts the count over, then asks)
        // a station that has spoken since its load is not reloaded under a person
        reset(); S.loadedAt = t; S.heardAt = t + 1000; t += 100000; const spoken = L._reloadFrame('x');
        return { quiet, back, lost, fresh, slow, spoken, offline: (() => { reset(); const was = Object.getOwnPropertyDescriptor(Navigator.prototype, 'onLine'); Object.defineProperty(navigator, 'onLine', { value: false, configurable: true }); t += 1e6; const r = L._reloadFrame('x'); delete navigator.onLine; return r; })() };
      } finally { Date.now = realNow; }
    });
    say('one hour of a station that never answers: reloads at (s) ' + out.quiet.join(', ') + `  -> ${out.quiet.length} (was 79: every 45 s)`);
    ok(out.quiet.length >= 8 && out.quiet.length <= 10, 'an hour holds ' + out.quiet.length + ' reloads, not 79');
    const gaps = out.quiet.slice(1).map((t, i) => t - out.quiet[i]);
    ok(JSON.stringify(gaps.slice(0, 5)) === JSON.stringify([45, 90, 180, 360, 600]) && gaps.every(g => g <= 600), 'the first retry comes at 45 s, then the wait doubles (90, 180, 360 s) and stops at ten minutes: ' + gaps.join(', '));
    const after = out.back.filter(t => t > 401);
    ok(after.length && after[0] - 401 >= 45 && after[0] - 401 <= 46, 'after the station answered, the next silence is given 45 s again (' + (after[0] - 401) + ' s)');
    ok(out.slow === false, 'after six reloads that did not help, a plain reload still waits (' + out.slow + ')');
    ok(out.fresh === true, 'a network that has just come back reloads at 45 s, whatever went before');
    ok(out.spoken === false, 'a station that has said anything since it loaded is not reloaded');
    ok(out.offline === false, 'nothing is reloaded while the browser is offline');
    ok(!errs.length, 'no page error: ' + errs.slice(0, 3).join(' | '));
    await ctx.close();
  } finally { await browser.close(); try { srv.close(); } catch (_) { /* done */ } }
  say(`ru-S1-frame-reload OK · ${n} checks`);
})().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
