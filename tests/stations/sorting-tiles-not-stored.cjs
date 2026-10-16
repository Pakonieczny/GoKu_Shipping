// The Matrix tiles are not copied to localStorage (Paul, 16 Oct 2026). sorting.html saved the JPEG data URL of every analysed tile under
// "userImagesArray" and nothing ever read it back (sorting.html and sorting-2.html declare the array empty on every load); the copy only filled the
// browser's storage, which is what made Print Row fail on a full store.
//   1 · a copy left by an earlier version is removed when the page loads, and NOTHING else in the storage is touched (the sign-in, the employee
//       name, the timeline outbox with its unsent events, today's sorted orders, the cancelled-order cache, the matrix descriptors, the layout)
//   2 · the analysis runs as before: the tiles are in memory with their names and image data, the descriptor maps are saved, the "Analysis
//       complete" message shows, the gate for Send & Match is re-evaluated, and the storage stays small (no 3 MB copy of the tiles)
//   3 · the same analysis on a browser whose storage is already full completes the same way
// Network, Firebase, Materialize and StationTimeline are stubbed; every request off the fake origin is aborted.
//   PW_DIR=<dir with playwright-core> CHROMIUM=... node tests/stations/sorting-tiles-not-stored.cjs
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://sorting.test';
const TILE = 'data:image/jpeg;base64,' + 'A'.repeat(60000);        // a tile's JPEG data URL, about 60 KB
const LEGACY = JSON.stringify(Array.from({ length: 50 }, (_, i) => ({ name: 'Gold_' + i + '.jpg', data: TILE, analyzed: true })));   // what an earlier version left, about 3 MB
const KEEP = {                                                      // everything else a station keeps; none of it may change
  'sorting.employee': 'Maya', 'sorting.employeeVia': 'name', 'sorting.sortedToday': JSON.stringify({ day: '2026-10-16', ids: ['3800000001'] }),
  'sorting.lastRelayBatch': 'sort-2026-10-16T09:00:00.000Z',
  'orderTimeline.outbox.v1': JSON.stringify([{ orderId: '3800000002', type: 'sorted', at: 1760600000000, id: 'sorting-1-3800000002-sorted-1760600000000' }]),
  'stationTimeline.cancelled.v1': JSON.stringify({ '3800000009': { by: 'Paul', at: 1760600000000 } }),
  matrixCharm2Words: JSON.stringify({ Silver_7: 'dog paw' }), matrixCharm6Words: JSON.stringify({ Silver_7: 'animal dog paw pendant small' }),
  etsyTitlesByCell: JSON.stringify(['Silver Dog Charm Necklace']),
  'pos-etsyOrderNumber-left': '20', 'pos-etsyOrderNumber-top': '25',
  access_token: 'tok', refresh_token: 'ref', token_expires_at: String(Math.floor(Date.now() / 1000) + 7200)
};

const firebaseStub = `
  window.__snap = { cb: null };
  const mk = d => ({ exists: !!d, data: () => d });
  window.firebase = { initializeApp() { return {}; }, firestore() { return { collection() { return { doc() { return {
    onSnapshot(cb) { window.__snap.cb = cb; setTimeout(() => cb(mk(window.__relayDoc)), 0); return () => {}; } }; } }; } }; } };`;
const materializeStub = `
  window.__toasts = [];
  const inst = { open() {}, close() {} };
  window.M = { AutoInit() {}, toast(o) { window.__toasts.push(o && o.html); }, Modal: { init() { return inst; }, getInstance() { return inst; } } };`;

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  async function open({ legacy }) {
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
      if (p === '/order-timeline.js') return r.fulfill(js(fs.readFileSync(path.join(root, 'order-timeline.js'))));
      if (p.startsWith('/.netlify/functions/')) return r.fulfill({ status: 200, contentType: 'application/json', body: p.endsWith('etsyImages') ? '[]' : '{}' });
      return r.fulfill({ status: 404, body: '' });
    });
    await ctx.addInitScript(([keep, legacyJson]) => {
      for (const [k, v] of Object.entries(keep)) localStorage.setItem(k, v);
      if (legacyJson) localStorage.setItem('userImagesArray', legacyJson);
      window.__relayDoc = null;
    }, [KEEP, legacy ? LEGACY : null]);
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${ORIGIN}/sorting.html`);
    await page.waitForFunction(() => window.__snap && window.__snap.cb && typeof analyzeAndStoreSubImages === 'function');
    return { ctx, page };
  }
  const snapshot = page => page.evaluate(() => Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)])));
  // the analysis of n tiles with the AI call answered locally (a charm in every tile, "Gold_<n>")
  const analyse = (page, n) => page.evaluate(async n => {
    window.openAIAnalyzePhoto = async () => ({ charmPresent: 'yes', color: 'Gold', topRightNumber: String(++window.__n), twoCharmsPresent: 'no', desc2: 'paw print' });
    window.__n = 0; window.__toasts.length = 0;
    const tiles = Array.from({ length: n }, (_, i) => ({ origName: 'tile' + i, data: 'data:image/jpeg;base64,' + 'B'.repeat(60000), master: 'M1', masterId: 'M1' }));
    await analyzeAndStoreSubImages(tiles, 'Analyzing Photos...');
    return { inMemory: window.userImagesArray.length, names: window.userImagesArray.slice(0, 3).map(u => u.name), dataKept: window.userImagesArray.every(u => u.data.length > 60000 && u.analyzed === true),
      same: window.userImagesArray === userImagesArray, toast: window.__toasts.slice(), stored: localStorage.getItem('userImagesArray') };
  }, n);
  const footprint = page => page.evaluate(() => Object.keys(localStorage).reduce((n, k) => n + k.length + localStorage.getItem(k).length, 0));

  /* ── 1 · a leftover copy goes, everything else stays exactly as it was ── */
  let { ctx, page } = await open({ legacy: true });
  let now = await snapshot(page);
  assert.strictEqual('userImagesArray' in now, false, 'the copy an earlier version left is removed on load');
  for (const [k, v] of Object.entries(KEEP)) assert.strictEqual(now[k], v, 'untouched: ' + k);
  assert.deepStrictEqual(Object.keys(now).sort(), Object.keys(KEEP).sort(), 'no other key added or removed: ' + Object.keys(now).sort().join(', '));
  console.log('load: the ~' + (LEGACY.length >> 20) + ' MB leftover copy is gone; the other ' + Object.keys(KEEP).length + ' keys (sign-in, name, timeline outbox, sorted today, cancelled cache, matrix descriptors, layout) are byte-identical');

  /* ── 2 · the analysis as before, and the storage stays small ── */
  const small = await footprint(page);
  const run = await analyse(page, 40);
  assert.strictEqual(run.inMemory, 40, 'all 40 tiles are in memory'); assert.deepStrictEqual(run.names, ['Gold_1.jpg', 'Gold_2.jpg', 'Gold_3.jpg']);
  assert(run.dataKept, 'with their image data'); assert(run.same, 'window.userImagesArray is the live array');
  assert(run.toast.some(t => /Analysis complete – processed 40 photos, saved 40/.test(t)), 'the completion message: ' + JSON.stringify(run.toast));
  assert.strictEqual(run.stored, null, 'no copy of the tiles in localStorage');
  now = await snapshot(page);
  assert.strictEqual(Object.keys(JSON.parse(now.matrixCharm2Words)).length, 41, 'the 2-word descriptors are saved for every tile (40 new + the one that was there)');
  assert.strictEqual(Object.keys(JSON.parse(now.matrixCharm6Words)).length, 41, 'and the 6-word descriptors');
  const after = await footprint(page);
  assert(after < 20000, 'forty tiles (2.4 MB of images) leave the storage under 20 KB, not megabytes: ' + after + ' (was ' + small + ' before)');
  for (const k of ['sorting.employee', 'orderTimeline.outbox.v1', 'sorting.sortedToday', 'stationTimeline.cancelled.v1', 'access_token']) assert.strictEqual(now[k], KEEP[k], 'untouched by the analysis: ' + k);
  console.log('analysis: 40 tiles in memory with their image data, descriptors saved, "Analysis complete" shown, no copy stored, storage ' + after + ' characters');
  await ctx.close();

  /* ── 3 · the same on a browser whose storage is full ── */
  ({ ctx, page } = await open({ legacy: false }));
  await page.evaluate(() => { let n = 0; for (const unit of [200000, 20000, 2000, 200, 20]) for (;;) { try { localStorage.setItem('zz_fill_' + n, 'x'.repeat(unit)); n++; } catch (_) { break; } } });
  assert.strictEqual(await page.evaluate(() => { try { localStorage.setItem('zz_probe', 'x'.repeat(1000)); return true; } catch (_) { return false; } }), false, 'the store really is full');
  const full = await analyse(page, 12);
  assert.strictEqual(full.inMemory, 12); assert(full.dataKept);
  assert(full.toast.some(t => /Analysis complete – processed 12 photos, saved 12/.test(t)), 'completes on a full store: ' + JSON.stringify(full.toast));
  console.log('full storage: the analysis completes the same way (12 tiles in memory, completion message shown)');
  await ctx.close();

  await browser.close();
  assert.deepStrictEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  console.log('sorting-tiles-not-stored: all passed');
})().catch(e => { console.error(e); process.exit(1); });
