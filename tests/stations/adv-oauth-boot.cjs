// Adversarial: a station opened with no Etsy token must load cleanly. The boot code used to call startOAuth("boot")
// before `let __oauthInFlight` was reached, so every such load threw "Cannot access '__oauthInFlight' before
// initialization". Today's behaviour is kept: no automatic Etsy login redirect, no Etsy request, no code verifier.
// No network except loopback stubs; every etsy.com request is aborted and counted.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/adv-oauth-boot.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const PAGES = ['weld-1', 'assembly-1', 'assembly-2', 'assembly-3', 'assembly-4', 'shipping-1', 'shipping-2', 'shipping-3', 'sorting'];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const ORIGIN = 'http://station.test';
  const fbStub = fs.readFileSync(path.join(__dirname, 'adv-stations.cjs'), 'utf8').match(/const fbStub = `([\s\S]*?)`;/)[1];
  const mStub = `window.M = { AutoInit() {}, toast() {}, updateTextFields() {},
    Modal: { init() { return { open() {}, close() {} }; }, getInstance() { return { open() {}, close() {} }; } },
    FormSelect: { init() { return {}; }, getInstance() { return { getSelectedValues: () => [] }; } } };`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const failures = [];
  try {
    for (const name of PAGES) {
      const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
      const etsy = [];
      await ctx.route(/.*/, async r => {
        const u = new URL(r.request().url());
        if (/(^|\.)etsy\.com$/.test(u.host)) { etsy.push(u.href.slice(0, 90)); return r.abort(); }
        if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : 'window.$=window.jQuery=()=>({on(){},ready(){}});' });
        if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
        if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
        if (u.origin !== ORIGIN) return r.abort();
        if (u.pathname.startsWith('/.netlify/functions/')) {
          if (/etsy|exchangeToken|OAuth/i.test(u.pathname)) etsy.push(u.pathname);
          return r.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
        }
        const file = path.join(root, decodeURIComponent(u.pathname));
        if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
        return r.fulfill({ status: 404, body: 'not here' });
      });
      // A signed-in employee, but no Etsy token of any kind.
      await ctx.addInitScript(() => { localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Tess Welder'); });
      const page = await ctx.newPage();
      const errs = [];
      page.on('pageerror', e => errs.push(e.message));
      await page.goto(ORIGIN + '/' + name + '.html', { waitUntil: 'load' });
      await page.waitForTimeout(400);
      const st = await page.evaluate(() => ({
        path: location.pathname,
        verifier: localStorage.getItem('etsy_code_verifier'),
        startOAuth: typeof window.startOAuth,
        inFlight: (() => { try { return __oauthInFlight; } catch (e) { return 'unreachable: ' + e.message; } })()
      }));
      const tdz = errs.filter(m => /before initialization/.test(m));
      const bad = [];
      if (tdz.length) bad.push('threw at load: ' + tdz.join(' | '));
      if (st.path !== '/' + name + '.html') bad.push('left the page for ' + st.path);
      if (etsy.length) bad.push('made Etsy requests: ' + etsy.join(', '));
      if (st.verifier) bad.push('started an Etsy login (code verifier stored)');
      if (st.startOAuth !== 'function') bad.push('startOAuth missing (' + st.startOAuth + ')');
      if (st.inFlight !== false) bad.push('boot script did not reach the OAuth guard cleanly: ' + st.inFlight);
      if (bad.length) failures.push(name + ': ' + bad.join('; '));
      else console.log(name + ': loads with no Etsy token, no error, no redirect, no Etsy request');
      await ctx.close();
    }
  } finally { await browser.close(); }
  assert.deepStrictEqual(failures, [], '\n  ' + failures.join('\n  '));
  console.log('adv-oauth-boot: all ' + PAGES.length + ' stations pass');
})().catch(e => { console.error(e.message || e); process.exit(1); });
