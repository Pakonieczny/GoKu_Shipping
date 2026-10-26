// Every page the site serves loads (pages-smoke, Paul 26 Oct 2026: "every user-accessible feature working"): each .html in
// scripts/build-public.cjs `assets` (the Brites Ads pages brites-adwords / brites-vfx are left out, as the owner asked) opens in
// headless Chromium on tests/charm-nest/bridge-server.cjs (its fake Netlify functions), external hosts blocked, and must show
// no uncaught page error, no console error of its own, no failed load of the site's own files (a file a page loads that is not
// in the public manifest is a 404 in production, so it is answered 404 here too), and a first screen that is not blank.
// The sorter (charm-nest-1.html) also opens each top tab, the main dialogs and the Account panel.
//   PW_DIR=/opt/node-tools/node_modules CHROMIUM=/opt/pw-browsers/chromium-*/chrome-linux64/chrome node tests/smoke/all-pages.cjs [page.html …]
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { chromium } = require(path.join(process.env.PW_DIR || '/opt/node-tools/node_modules', 'playwright-core'));
const { start, signInAs } = require('../charm-nest/bridge-server.cjs');
const SKIP = new Set(['brites-adwords.html', 'brites-vfx.html']);
// the public manifest, read from the build script itself (its `assets` array, plus the optional font folders it copies)
const src = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');
const assets = JSON.parse(src.slice(src.indexOf('[', src.indexOf('const assets')), src.indexOf('];', src.indexOf('const assets')) + 1));
const published = new Set(assets.map(a => '/' + a));
const isPublished = p => published.has(p) || /^\/vendor\/fonts\/(scripts\/)?[^/]+\.(otf|ttf)$/i.test(p) || (/^\/vendor\/fonts\/scripts\/[^/]+-OFL\.txt$/i.test(p));
const CDN_GLOBALS = { firebase: /gstatic\.com\/firebasejs|firebase/, M: /materialize/i, pdfMake: /pdfmake/, QRCode: /qrcode/i, JSZip: /jszip/i, Chart: /chart/i };
// QR Printer.html has no screen of its own: the stations open it with a label in localStorage ("qrPrintAll") and it makes the PDF and opens the
// print dialog. It is given one label here, as sorting.html leaves it. screenTwo.html is a fragment ("no <html> or <body> tags; inner markup
// only", hidden by its own style): it must load clean, but shows nothing alone.
const NO_SCREEN = new Set(['QR Printer.html', 'screenTwo.html']);
const SEED = { 'QR Printer.html': { qrPrintAll: JSON.stringify({ dispatchDate: '26 Oct 2026', userTypedOrderNum: '1234567890', items: [{ receipt_id: 1234567890, title: 'Smoke charm', keywords: [] }] }) } };
const pages = process.argv.slice(2).length ? process.argv.slice(2) : assets.filter(a => /\.html$/.test(a) && !SKIP.has(a));

async function visit(browser, srv, name, extra) {
  const errors = [], blocked = [], own = new URL(srv.sorterOrigin).host;
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  await ctx.route('**/*', route => {
    const u = new URL(route.request().url());
    if (u.protocol === 'data:' || u.protocol === 'blob:') return route.continue();
    // a page that names its production origin (SKU_List_V1.html: https://sku.goldenspike.app) calls the same Netlify functions: the fake answers
    if (/(^|\.)goldenspike\.app$/.test(u.hostname) && u.pathname.startsWith('/.netlify/functions/')) return route.fetch({ url: srv.sorterOrigin + u.pathname + u.search }).then(r => route.fulfill({ response: r, headers: Object.assign({}, r.headers(), { 'access-control-allow-origin': srv.sorterOrigin }) }))   /* (served from that origin in production, so the call is same-origin there) */.catch(() => route.abort());
    if (u.host !== own && u.host !== new URL(srv.stationOrigin).host) { blocked.push(u.href); return route.abort('blockedbyclient'); }   // external hosts: blocked
    const p = decodeURIComponent(u.pathname);
    // "is this name an Admin?" (firebaseOrders { stationAdmin }, _stationAdmins.js), which the fake does not answer: Paul K is (the sign-in writes him "Paul K."), as config/stationAdmins says
    if (p === '/.netlify/functions/firebaseOrders' && /"stationAdmin"/.test(route.request().postData() || '')) { const n = JSON.parse(route.request().postData()).stationAdmin; return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, admin: /^Paul K\.?$/.test(n) }) }); }
    if (p.startsWith('/.netlify/functions/') || p.startsWith('/__') || isPublished(p)) return route.continue();
    errors.push('not in scripts/build-public.cjs: ' + p);
    return route.fulfill({ status: 404, body: '' });
  });
  if (SEED[name]) await ctx.addInitScript(kv => { for (const k in kv) try { localStorage.setItem(k, kv[k]); } catch (_) {} }, SEED[name]);
  const page = await ctx.newPage();
  // a library a page takes from a CDN (Firebase, Materialize, pdfmake…) is blocked here: its missing global is the block's doing, not the page's
  page.on('pageerror', e => { const g = /^(\w+) is not defined$/.exec(e.message); if (g && CDN_GLOBALS[g[1]] && blocked.some(u => CDN_GLOBALS[g[1]].test(u))) return; errors.push('pageerror: ' + (e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
  page.on('console', m => { if (m.type() !== 'error') return; const t = m.text(); if (/ERR_BLOCKED_BY_CLIENT|net::ERR_FAILED/.test(t)) return; if (/^Failed to load resource/.test(t) && /\/\.netlify\/functions\//.test(m.location().url || '')) return;   /* (a function the fake does not stand in for, or a probe it answers no: the fake's limit, not the page's) */ const g = Object.keys(CDN_GLOBALS).find(k => new RegExp('\\b' + k + ' is not defined').test(t)); if (g && blocked.some(u => CDN_GLOBALS[g].test(u))) return; errors.push('console: ' + t.slice(0, 300) + ' @ ' + (m.location().url || '').replace(srv.sorterOrigin, '')); });
  page.on('response', r => { const u = new URL(r.url()); if (u.host === own && r.status() >= 400 && !u.pathname.startsWith('/.netlify/')) errors.push(`HTTP ${r.status()} ${u.pathname}`); });
  try {
    await page.goto(`${srv.sorterOrigin}/${encodeURI(name)}`, { waitUntil: 'load', timeout: 30000 });
    await page.waitForTimeout(1500);
    // the first screen shows something: visible text, or a visible control or picture, in the viewport (a fixed-position gate counts)
    const shown = await page.evaluate(() => [...document.querySelectorAll('body *')].filter(e => { const r = e.getBoundingClientRect(), cs = getComputedStyle(e); return r.width > 4 && r.height > 4 && r.bottom > 0 && r.top < innerHeight && cs.visibility !== 'hidden' && +cs.opacity > 0 && (/^(CANVAS|IMG|VIDEO|SVG|INPUT|BUTTON|SELECT|TEXTAREA|IFRAME)$/i.test(e.tagName) || [...e.childNodes].some(n => n.nodeType === 3 && n.textContent.trim())); }).length);
    if (!shown && !NO_SCREEN.has(name)) errors.push('blank first screen');
    if (extra) await extra(page, errors);
  } catch (e) { errors.push('load: ' + e.message.split('\n')[0]); }
  await ctx.close();
  return errors;
}

// the sorter: each top tab (the Workspace menu's too: Design Station, Master files, Employee efficiency), the main dialogs reachable on an empty
// workspace (Earlier sets, the Workspace menu) and the Account panel; Settings, behind its lock, is skipped
async function sorterTour(page, errors, srv) {
  await page.waitForFunction(() => window.CNAccount && document.querySelector('#modeSeg'), null, { timeout: 15000 });
  await signInAs(page, srv, 'Paul K').catch(e => errors.push('sign-in: ' + e.message));
  const visible = sel => page.evaluate(sel => { const e = document.querySelector(sel); if (!e) return false; const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0 && getComputedStyle(e).visibility !== 'hidden'; }, sel);
  const modes = await page.$$eval('[data-mode]', bs => [...new Set(bs.map(b => b.dataset.mode))]);
  for (const m of modes) {
    if (m === 'efficiency') await page.waitForFunction(() => !document.querySelector('[data-mode="efficiency"]').hidden, null, { timeout: 5000 }).catch(() => errors.push('Employee efficiency is not shown to the master admin'));
    await page.evaluate(m => document.querySelector(`[data-mode="${m}"]`).click(), m);
    await page.waitForTimeout(700);
    const view = { nest: '#sheets', orders: '#ordersView', engrave: '#engraveView', review: '#reviewView', library: '#libView', design: '#designView', master: '#masterView', efficiency: '#efficiencyView' }[m];
    if (!view) { errors.push('tab ' + m + ': no view known to this test'); continue; }
    const st = await page.evaluate(sel => { const v = document.querySelector(sel); return { hidden: !v || v.classList.contains('hidden'), text: v ? (v.innerText || '').trim().length + v.querySelectorAll('canvas,img,iframe,svg,button,input').length : 0 }; }, view);
    if (st.hidden || (m !== 'nest' && !st.text)) errors.push(`tab ${m}: ${st.hidden ? 'its view did not open' : 'its view is blank'}`);
  }
  await page.evaluate(() => document.querySelector('[data-mode="nest"]').click());
  await page.evaluate(() => { document.querySelector('#moreMenu').open = true; });
  if (!(await visible('#moreMenu .moreList'))) errors.push('Workspace menu did not open');
  await page.evaluate(() => { document.querySelector('#moreMenu').open = false; });
  await page.evaluate(() => document.querySelector('#btnEarlierSets').click());
  await page.waitForTimeout(800);
  if (!(await page.evaluate(() => [...document.querySelectorAll('dialog[open], [role=dialog]')].some(d => { const r = d.getBoundingClientRect(); return r.width > 0 && r.height > 0; })))) errors.push('Earlier sets: no dialog opened');
  await page.keyboard.press('Escape'); await page.waitForTimeout(300);
  await page.evaluate(() => CNAccount.open());
  await page.waitForTimeout(500);
  if (!(await page.evaluate(() => CNAccount.isOpen()))) errors.push('Account panel did not open');
  const acct = await page.evaluate(() => (document.body.innerText || '').includes('Paul K'));
  if (!acct) errors.push('Account panel does not show the signed-in person');
  await page.evaluate(() => CNAccount.close());
}

(async () => {
  const t0 = Date.now();
  const srv = await start();
  // the SKU console's "already checked" marks: the real function over the fake's in-memory Firestore (the fake has no stand-in of its own)
  srv.st.fn.skuChecked = require(path.join(root, 'netlify/functions/skuChecked.js')).handler;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || undefined, args: ['--no-sandbox'] });
  let failed = 0;
  for (const name of pages) {
    const t = Date.now();
    const errs = await visit(browser, srv, name, name === 'charm-nest-1.html' ? (p, e) => sorterTour(p, e, srv) : null);
    const uniq = [...new Set(errs)];
    console.log(`${uniq.length ? 'FAIL' : 'ok  '} ${name} (${Date.now() - t} ms)`);
    uniq.slice(0, 12).forEach(e => console.log('     ' + e));
    if (uniq.length) failed++;
  }
  await browser.close(); srv.close();
  console.log(`${pages.length - failed}/${pages.length} pages clean in ${((Date.now() - t0) / 1000).toFixed(1)} s`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
