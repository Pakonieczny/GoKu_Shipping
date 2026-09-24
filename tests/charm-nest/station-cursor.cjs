// Browser test of the Design Station's remote pointer (design-1.html, Cursor): an order check the sorter waits on
// (open-list read, claim, release, re-read) never waits for the pointer. Its glides start at once and play one after
// another; when they come faster than they can be shown the oldest waiting ones are left out; a command that waits for
// its motion waits only for the glide playing, not for the ones queued; and a page out of view (the sorter's tab in the
// background) only notes each motion, at once.
//   node tests/charm-nest/station-cursor.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

(async () => {
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.addInitScript(() => { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.stationOrigin}/design-1.html`);
    await page.waitForFunction(() => window.DesignStation && DesignStation.bridge && DesignStation.bridge.cursor && typeof DesignStation.bridge.cursor.glide === 'function');
    const captions = (from) => page.evaluate(n => DesignStation.bridge.cursor.log.slice(n).map(l => l.caption), from);
    const logLength = () => page.evaluate(() => DesignStation.bridge.cursor.log.length);

    // 1 · ten glides queued at once: the caller is not held, the first plays now, the oldest waiting ones are left out
    let n0 = await logLength();
    const queuedMs = await page.evaluate(() => { const c = DesignStation.bridge.cursor, t0 = performance.now(); for (let i = 0; i < 10; i++) c.glide('banner', null, 'g' + i, { click: false }); return performance.now() - t0; });
    assert(queuedMs < 50, `queuing ten glides returned at once: ${queuedMs.toFixed(1)} ms`);
    await page.waitForFunction(n => DesignStation.bridge.cursor.log.slice(n).some(l => l.caption === 'g9'), n0, { timeout: 15000 });
    const shown = await captions(n0);
    assert.deepStrictEqual(shown, ['g0', 'g4', 'g5', 'g6', 'g7', 'g8', 'g9'], 'the first glide and the newest six were shown, in order: ' + JSON.stringify(shown));

    // 2 · a row that is not on the page is skipped by a glide, not shown on the banner
    n0 = await logLength();
    await page.evaluate(() => DesignStation.bridge.cursor.glide('row', '999000111', 'Claim 999000111'));
    await page.evaluate(() => DesignStation.bridge.cursor.act('banner', null, 'after the row', { click: false }));
    assert.deepStrictEqual(await captions(n0), ['after the row'], 'a missing row was skipped, not drawn on the banner');

    // 3 · a command that waits for its own motion waits for the glide playing, and the glides still waiting are left out
    n0 = await logLength();
    const act = await page.evaluate(async () => {
      const c = DesignStation.bridge.cursor;
      for (let i = 0; i < 5; i++) c.glide('banner', null, 'h' + i, { click: false });
      const t0 = performance.now(); await c.act('banner', null, 'waited', { click: false });
      return performance.now() - t0;
    });
    assert.deepStrictEqual(await captions(n0), ['h0', 'waited'], 'the playing glide finished, the four waiting were left out');
    assert(act < 2500, `the waiting command took one glide and its own motion, not six: ${Math.round(act)} ms`);

    // 4 · out of view nobody sees the pointer: a motion is noted at once, with no animation and no wait
    n0 = await logLength();
    const hidden = await page.evaluate(async () => {
      Object.defineProperty(document, 'hidden', { configurable: true, get: () => true });
      const c = DesignStation.bridge.cursor, t0 = performance.now();
      await c.act('banner', null, 'unseen press', { click: true });
      c.glide('banner', null, 'unseen glide', { click: false });
      const ms = performance.now() - t0;
      await new Promise(r => setTimeout(r, 50));
      delete document.hidden;
      return { ms, last: c.log.slice(-2) };
    });
    assert(hidden.ms < 50, `a motion out of view took no time: ${hidden.ms.toFixed(1)} ms`);
    assert.deepStrictEqual(hidden.last.map(l => [l.caption, l.hidden, l.click]), [['unseen press', true, true], ['unseen glide', true, false]], 'both motions were noted as out of view: ' + JSON.stringify(hidden.last));
    assert.deepStrictEqual(await captions(n0), ['unseen press', 'unseen glide']);

    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log(`station cursor OK · ten glides queued in ${queuedMs.toFixed(1)} ms, 7 shown · a waiting command ${Math.round(act)} ms · out of view ${hidden.ms.toFixed(1)} ms`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
