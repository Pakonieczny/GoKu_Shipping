// Settings > "Build · last reset": the line about the page build (Paul, 10 Oct 2026: it was ALWAYS red, "a newer build is live",
// because the build it called the page's own was a constant typed into charm-nest-bridge.js on 3 Oct, while the live build is
// read from the ?v= tokens of the page's tags). The page build is read from the running page's own tags; whether a newer one
// is live is PageVersion's HEAD check of the page (its ETag: COST, Oct 2026, it was a no-store download of the whole HTML).
// The real page in real headless Chromium, offline, against the repo's fake server (bridge-server.cjs). Nothing here touches
// a live site, and nothing is written: Settings is only opened.
//   1 · the server holds the build the page runs      -> grey "Page build <id> — up to date" (no red, no "newer")
//   2 · the server holds a newer build (another ETag) -> red: "Page build <id> — a newer build is live: reload this page
//       with Ctrl+Shift+R to run it"
//   and each time: one HEAD of the page per opening of Settings and no download of it, the id is not a constant of the
//   script (Sandbox.build() is what the page's tags say), and a server that cannot answer leaves the line neutral, never red.
//   COST (Oct 2026): opening Settings on a production page reads the light sandbox status only (no census of every family);
//   "Count the sandbox" reads the counts when it is pressed.
//   node tests/charm-nest/settings-build-line.cjs [playwright-core dir]
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, unlockSettings } = require('./bridge-server.cjs');
const HTML = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
assert(/charm-nest-bridge\.js\?v=[\w.-]+/.test(HTML), 'the page has the bridge script tag with a ?v= token');

async function main() {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  const errors = [];
  const srv = await start({ receipts: [] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.addInitScript(({ sorter }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester'); window.confirm = () => true; window.alert = () => {}; }, { sorter: srv.sorterOrigin });
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push(e.message));

    // The server's answer to the page's own check (a HEAD of charm-nest-1.html): the live page's ETag, per scenario. Every
    // other request goes on to the fake server untouched. `checks` counts the page's HEAD checks, `downloads` any GET of the page after its load.
    let live = '"build-1"', checks = 0, fail = false, loaded = false, downloads = 0;
    await page.route(/charm-nest-1\.html(\?|$)/, r => {
      if (r.request().method() === 'HEAD') { checks++; return fail ? r.fulfill({ status: 503, contentType: 'text/plain', body: '' }) : r.fulfill({ status: 200, contentType: 'text/html; charset=utf-8', headers: { etag: live }, body: '' }); }
      if (loaded && r.request().resourceType() !== 'document') downloads++;
      return r.continue();
    });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.Sandbox && window.Session && Session.ready() && window.PageVersion, null, { timeout: 60000 });
    loaded = true;
    await page.waitForTimeout(4500);   // (the page's own first check, 4 s after its load, has set its starting point)
    const known = tag => page.evaluate(t => PageVersion._set(t), tag);   // (the page's ETag at load, as the tests' known starting point)
    await known('"build-1"');
    const status = () => srv.st.calls.filter(c => c.op === 'sandboxStatus').map(c => (c.body && c.body.light ? 'light' : 'full'));

    const written = () => srv.st.calls.filter(c => c.op && /^(poolPut|poolUpdate|setAllocate|setUpdate|putSheet|deleteSheet|runPut|runArchive|backPut|releasePut|arrivalRecord|customPut|timelineAdd|sandboxStream|sandboxPut|sandboxPullOrders|putCharms|startAgent|startJob|purgeHistory|sandboxReset)$/.test(c.op)).length;
    const writesBefore = written();
    /** Opens Settings and waits for the build line to say what the check found (or, when it can not, to stay as it first reads). */
    async function look(settle) {
      const before = checks;
      await page.evaluate(() => { const d = document.getElementById('dlgSettings'); if (d && d.open) closeDlg(d); openSettings(); }); await unlockSettings(page);   // (PERSON, 26 Oct: Settings asks the admin password at every opening)
      await page.waitForFunction(re => new RegExp(re).test(document.querySelector('#stBuildNote [data-i="build"]').textContent), settle.source, { timeout: 15000 });
      await page.waitForTimeout(400);   // (a second answer, or a second check, would show now)
      return page.evaluate(() => { const n = document.querySelector('#stBuildNote [data-i="build"]'), host = document.getElementById('stBuildNote'), grey = getComputedStyle(host).color; return { text: n.textContent, color: getComputedStyle(n).color, inline: n.style.color, grey, build: Sandbox.build(), page: Sandbox.pageBuild(), lines: host.querySelectorAll('[data-i="build"]').length }; })
        .then(r => Object.assign(r, { checks: checks - before }));
    }
    const ID = '2\\d{7}-[0-9a-f]{6}';

    /* ── 1 · the server holds the build the page runs ── */
    const status0 = status().length;
    live = '"build-1"';
    const same = await look(/up to date/);
    console.log('1 up to date:', same.text);
    assert(new RegExp(`^Page build ${ID} — up to date$`).test(same.text), 'the line says the page build and that it is up to date: ' + same.text);
    assert(!/newer|reload|Ctrl/.test(same.text), 'no word of a newer build or of reloading');
    assert.strictEqual(same.inline, '', 'it is not set red (no colour of its own)');
    assert.strictEqual(same.color, same.grey, 'it is the same quiet grey as the lines under it, not the red: ' + JSON.stringify([same.color, same.grey]));
    assert.strictEqual(same.build, same.page, 'the build is what the page loaded');
    assert(same.text.includes(same.page), 'the line names it: ' + JSON.stringify([same.page, same.text]));
    assert.strictEqual(same.checks, 1, 'one check of the page per opening of Settings, as before: ' + same.checks);
    assert.strictEqual(same.lines, 1, 'one build line');
    assert.deepStrictEqual(status().slice(status0), ['light'], 'a production page reads the light status on opening Settings, not the census: ' + JSON.stringify(status().slice(status0)));

    /* ── the census only on a press ── */
    await page.click('#stBuildNote [data-i="count"] button');
    await page.waitForFunction(() => /In the sandbox now/.test(document.querySelector('#stBuildNote [data-i="now"]').textContent), null, { timeout: 15000 });
    assert.deepStrictEqual(status().slice(status0), ['light', 'full'], '"Count the sandbox" reads the counts once: ' + JSON.stringify(status().slice(status0)));

    /* ── 2 · a newer build: the page's ETag changed ── */
    live = '"build-2"';
    const newer = await look(/a newer build/);
    console.log('2 newer:', newer.text);
    assert(new RegExp(`^Page build ${same.page} — a newer build is live: reload this page with Ctrl\\+Shift\\+R to run it$`).test(newer.text), 'the red line names the page build and says what to do: ' + newer.text);
    assert.strictEqual(newer.inline, 'var(--clay)', 'it is red (the page\'s own clay colour, as before)');
    assert.notStrictEqual(newer.color, newer.grey, 'and it looks different from the grey lines: ' + JSON.stringify([newer.color, newer.grey]));
    assert.strictEqual(newer.page, same.page, 'the page build did not move: it is what this page loaded');
    assert(newer.checks <= 1, 'at most one check per opening of Settings: ' + newer.checks);

    /* ── back to the same (a page reloaded on it): the line is grey again ── */
    live = '"build-1"'; await known('"build-1"');
    const again = await look(/up to date/);
    assert.strictEqual(again.inline, '', 'when the server holds the page\'s build again the line is grey again');

    /* ── a server that cannot answer: neutral, never red, never "up to date" ── */
    fail = true;
    const down = await look(/^Page build \S+$/);
    console.log('3 no answer:', down.text);
    assert(new RegExp(`^Page build ${same.page}$`).test(down.text) && down.inline === '' && down.checks === 1, 'an unanswered check leaves the plain page build, not red: ' + JSON.stringify(down));
    fail = false;

    assert.strictEqual(downloads, 0, 'the page is never downloaded again to compare its tokens');
    assert.strictEqual(written(), writesBefore, 'Settings only reads: nothing was written');
    assert.deepStrictEqual(errors, [], 'no script error: ' + errors.join(' | '));
    await ctx.close();
  } finally { srv.close(); await browser.close(); }
  console.log('settings-build-line: all checks passed');
}
main().catch(e => { console.error(e); process.exit(1); });
