// The IDLE cost of every page of the shop (Firebase cost emergency, FC14).
//
// WHAT IT DOES  Opens each HTML page in a real browser (Chromium, Playwright) over the fake shop of tests/stations/all-stations (the repo served on
// two origins, the REAL firebaseOrders and employeeEfficiency doors over one in-memory Firestore, every other function answered by a fixture), leaves it
// alone with nobody signed in and nobody touching it, and moves the fake clock: IDLE_MIN minutes with the tab shown, then IDLE_MIN minutes with the tab
// hidden (document.hidden / visibilityState flipped and visibilitychange fired, as a browser does for a background tab or a locked screen). Every request
// the page makes to a Netlify function is counted by function name and op (the fake shop's call log), every request to the Firestore door by kind.
// The boot calls (the first seconds after load) are counted apart. Numbers are scaled to one hour.
//
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules NO_CLOCK_WORKER=1 node tests/cost/fc14-idle-pages.cjs [pageA.html pageB.html ...]
//   env: IDLE_MIN (default 20), OUT=/path/result.json, SIGNED=1 (sign a made-up person in first: PIN pages, Welding, the Sorter as Admin; SORTER_ROLE=laser|design for the other Sorter roles)
//
// The "fs" numbers (requests to the fake Firestore door) are NOT production cost: the fake Firebase client (fake-firebase.js) emulates a
// realtime listener by polling its door every 300 ms of fake time, where the real SDK holds one open stream and bills only the documents
// delivered on connect and on each change. Listener cost is therefore taken from the listener table in fc14-findings.md, not from them.
//
// Nothing leaves the machine: the fake shop refuses every outbound connection, the browser context aborts every request that is not the fake shop's or a
// library stand-in, and no sign-in, PIN, passcode or paid call is made. Reads per call are NOT measured here (the in-memory Firestore of the shop is not metered);
// they are measured per endpoint in tests/cost/fc14-doors.cjs and multiplied in plans/firebase-cost/fc14-findings.md.
'use strict';
const path = require('path'), fs = require('fs'), os = require('os');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || '/opt/node22/lib/node_modules/playwright/node_modules';
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROME = process.env.CHROMIUM || (() => { try { const d = '/opt/pw-browsers'; const c = fs.readdirSync(d).filter(x => /^chromium-/.test(x)).sort().pop(); return c ? path.join(d, c, 'chrome-linux/chrome') : undefined; } catch (_) { return undefined; } })();
const { World, sleep } = require(path.join(root, 'tests/stations/all-stations/world.cjs'));
const { makeCast, rosterOf } = require(path.join(root, 'tests/stations/all-stations/cast.cjs'));
const FX = require(path.join(root, 'tests/stations/all-stations/fixtures.cjs'));
const { Apps } = require(path.join(root, 'tests/stations/all-stations/apps.cjs'));

const IDLE_MIN = Math.max(2, Number(process.env.IDLE_MIN) || 20);
const SCALE = 60 / IDLE_MIN;
const SORTER = new Set(['charm-nest-1.html', 'charm-nest-check.html']);
const CAMERA = /scan|scanner/;
// SIGNED=1: leave the page signed in (a made-up person of the fake shop; the Sorter as its Admin, who is never signed out for being idle) instead of at the door.
// A non-Admin PIN page signs the person out after 10 minutes without input, so those pages show their last beats and then fall silent.
const SIGNED = !!process.env.SIGNED;
const PINPAGE = { 'assembly-1.html': 'asm', 'assembly-2.html': 'asm', 'assembly-3.html': 'asm', 'assembly-4.html': 'asm', 'shipping-1.html': 'ship', 'shipping-2.html': 'ship', 'shipping-3.html': 'ship', 'design-message.html': 'designMsg', 'design-message-1.html': 'designMsg2' };
const SORTINGPAGE = { 'sorting.html': 'sort', 'sorting-2.html': 'sortSmoke' };
const PAGES = process.argv.slice(2).length ? process.argv.slice(2) : fs.readdirSync(root).filter(f => /\.html$/.test(f)).sort();
// pages that need a hand before they sit idle in a state a person would leave them in
const PREP = {
  'charm-nest-1.html': ['library', 'orders', 'review', 'nest']      // the Sorter app: each view it can be left on
};

const tally = calls => { const o = {}; for (const c of calls) { const k = c.name + (c.op ? ':' + c.op : '') + (c.method === 'POST' ? ' (POST)' : ''); o[k] = (o[k] || 0) + 1; } return o; };
const scale = o => Object.fromEntries(Object.entries(o).map(([k, v]) => [k, Math.round(v * SCALE * 10) / 10]));
const sum = o => Object.values(o).reduce((a, b) => a + b, 0);

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox', '--autoplay-policy=no-user-gesture-required'] });
  const W = await World.start({ now: '2026-10-07T13:00:00Z', browser });
  const cast = makeCast();
  await W.post('/__ctl/roster', rosterOf(cast));
  for (const [u, o] of Object.entries(FX.OPS)) await W.post('/__ctl/put', { path: 'EtsyMail_Operators/' + u, data: { displayName: o.display, role: o.role, username: u } });
  const out = {};
  const callsSince = async from => (await W.calls(from)).calls;

  async function measure(file, view) {
    const label = file + (view ? ' [' + view + ']' : '') + (SIGNED ? ' (signed in' + (file === 'charm-nest-1.html' ? ' as ' + (process.env.SORTER_ROLE || 'admin') : '') + ')' : '');
    const ctx = await W.context({ label });
    if (SORTER.has(file)) await ctx.addInitScript(() => {
      try { if (location.hostname === '127.0.0.1') localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off', sandboxStream: 'off' })); } catch (_) {}
      window.confirm = () => true; window.prompt = () => null;
    });
    const fsReq = { shown: {}, hidden: {}, boot: {} };
    let bucket = 'boot';
    const t0 = await W.calls(0), base = t0.total;
    let page, err = '';
    try {
      const A = SIGNED ? Apps(W, cast) : null;
      if (SIGNED && PINPAGE[file]) { page = await A.pin(ctx, file, cast.people[PINPAGE[file]]); }
      else if (SIGNED && SORTINGPAGE[file]) { page = await A.sorting(ctx, file, cast.people[SORTINGPAGE[file]]); }
      else if (SIGNED && file === 'weld-1.html') { page = await A.weld(ctx, cast.people.welder, 'welding'); }
      else if (SIGNED && file === 'charm-nest-1.html') { page = await A.sorterApp(ctx); const R = process.env.SORTER_ROLE || 'admin'; if (R === 'admin') await A.sorterAdmin(page, cast.people.admin); else await A.sorterRole(page, R === 'laser' ? cast.people.laserApp : cast.people.designApp, R); }
      else page = await ctx.newPage();
      page.on('request', r => { const m = /\/__fs\/(\w+)/.exec(r.url()); if (m) fsReq[bucket][m[1]] = (fsReq[bucket][m[1]] || 0) + 1; });
      page.on('pageerror', e => { err = err || e.message; });
      if (CAMERA.test(file)) await page.addInitScript({ content: fs.readFileSync(path.join(root, 'tests/stations/all-stations/fake-camera.js'), 'utf8') });
      const origin = SORTER.has(file) ? W.sorterOrigin : W.stationOrigin;
      if (!(SIGNED && (PINPAGE[file] || SORTINGPAGE[file] || file === 'weld-1.html' || file === 'charm-nest-1.html'))) await page.goto(origin + '/' + file.split('/').map(encodeURIComponent).join('/'), { waitUntil: 'load', timeout: 60000 });
      await sleep(3500);
      if (file === 'charm-nest-1.html') { try { await page.waitForFunction(() => window.CN && window.CNEmployee && document.readyState === 'complete', null, { timeout: 60000 }); } catch (_) {} }
      if (view) { try { await page.evaluate(v => window.CN.setMode(v), view); } catch (e) { err = err || ('setMode: ' + e.message); } await sleep(2500); }
      const bootCalls = await callsSince(base);
      // a minute to settle (first reads, first timers), not counted in the idle numbers
      await W.advance(60000, { step: 30000, settle: 60 });
      const after = (await W.calls(0)).total;
      bucket = 'shown';
      await W.advance(IDLE_MIN * 60000, { step: 60000, settle: 40 });
      const shown = (await W.calls(after)).calls;
      const mid = (await W.calls(0)).total;
      await page.evaluate(() => {
        try { Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' }); document.dispatchEvent(new Event('visibilitychange')); window.dispatchEvent(new Event('pagehide')); } catch (_) {}
      });
      await W.advance(60000, { step: 30000, settle: 60 });
      const mid2 = (await W.calls(0)).total;
      bucket = 'hidden';
      await W.advance(IDLE_MIN * 60000, { step: 60000, settle: 40 });
      const hidden = (await W.calls(mid2)).calls;
      const key = label;
      out[key] = {
        boot: tally(bootCalls), shownPerHour: scale(tally(shown)), hiddenPerHour: scale(tally(hidden)),
        shownCallsPerHour: Math.round(shown.length * SCALE), hiddenCallsPerHour: Math.round(hidden.length * SCALE),
        fsBoot: fsReq.boot, fsShownPerHour: scale(fsReq.shown), fsHiddenPerHour: scale(fsReq.hidden), error: err || ''
      };
      process.stdout.write(label.padEnd(40) + ' boot ' + String(bootCalls.length).padStart(3) + ' calls | shown ' + String(out[key].shownCallsPerHour).padStart(6) + '/h | hidden ' + String(out[key].hiddenCallsPerHour).padStart(6) + '/h' + (err ? ' | err: ' + err.slice(0, 80) : '') + '\n');
    } catch (e) {
      out[label] = { error: String(e && e.message || e).slice(0, 300) };
      process.stdout.write(label.padEnd(40) + ' FAILED ' + out[label].error.slice(0, 160) + '\n');
    }
    await W.closeContext(ctx);
  }

  for (const f of PAGES) {
    if (PREP[f] && !process.env.NO_VIEWS) for (const v of PREP[f]) await measure(f, v);
    else await measure(f);
  }
  const outFile = process.env.OUT || path.join(os.tmpdir(), 'fc14-idle.json');
  fs.writeFileSync(outFile, JSON.stringify({ idleMin: IDLE_MIN, scaleToHour: SCALE, pages: out }, null, 1));
  process.stdout.write('\nwritten ' + outFile + '\n');
  const st = await W.stats();
  process.stdout.write('egress attempts: ' + (st.egress || []).length + ', paid calls tried: ' + st.paidTried + ', outside requests refused: ' + W.outside.length + '\n');
  W.stop(); await browser.close();
  process.exit(0);
})().catch(e => { console.error('FAILED', e && e.stack || e); process.exit(1); });
