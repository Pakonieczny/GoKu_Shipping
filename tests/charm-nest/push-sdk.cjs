/* PUSH, the REAL vendored library (Paul, 28 Oct 2026: live push, plan 5.5 option a): vendor/firebase-app-compat-10.14.1.js and vendor/firebase-firestore-compat-10.14.1.js, not
   a stand-in, loaded by charm-nest-push.js into the real page in Chromium under the site's own headers (Cross-Origin-Opener-Policy same-origin, Cross-Origin-Embedder-Policy
   require-corp: the page is cross-origin isolated, which is why no CDN script could be used), the network CUT (the browser resolves no host but this machine's, and every request
   to another host is refused and recorded: nothing reaches the live cloud).
     1 · the files are the ones the licence file names (sha256), served 200 under COEP, the page is crossOriginIsolated, and they expose firebase.firestore (SDK 10.14.1) with no
         auth SDK in the page
     2 · the real SDK, asked to listen to the counter with no network, tries the cloud's listen channel and nothing else: every request it makes is to firestore.googleapis.com, on the
         Listen channel (never Write, never a commit, never a lookup, never an identity call), for the project's database and the ONE document Charm_Nest_Push/counter
     3 · and the page falls back to the polled feed, with the reason, within the 20 s it is given, and keeps asking at the polled pace (no network is not an error page)
     4 · the service worker keeps the two files by their ?v= token (the page hands them over like any versioned file), so after one load a warm reload never asks the network for
         them, and they are no cost at boot: the warm boot makes the same few static requests it made, none of them the library's
   node tests/charm-nest/push-sdk.cjs   (through the slot queue: slot.sh timeout 300 node tests/charm-nest/push-sdk.cjs) */
'use strict';
const path = require('path'), fs = require('fs'), crypto = require('crypto'), assert = require('node:assert/strict');
const root = path.join(__dirname, '../..');
// (every request that reaches the server is counted: a file the service worker answers from its cache never does)
const http = require('http'), hits = [], realCreate = http.createServer;
http.createServer = handler => realCreate((req, res) => { hits.push(req.url); return handler(req, res); });
const { start } = require('./bridge-server.cjs'), { seed } = require('./laser-workflow.cjs');
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome';
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
const sleep = ms => new Promise(r => setTimeout(r, ms));
const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return Date.now() - t0; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(100); } };
const say = s => console.log('  ' + s);
const sha = f => crypto.createHash('sha256').update(fs.readFileSync(path.join(root, f))).digest('hex');

(async () => {
  const srv = await start({ receipts: [] }); seed(srv);
  let browser = null;
  try {
    const { chromium } = require(path.join(pwDir, 'playwright-core'));
    // (no host resolves but this machine: the SDK cannot reach anything even if a route let it)
    browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox', '--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE 127.0.0.1 , EXCLUDE localhost'] });
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    const outside = [];
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => { const q = r.request(); if (/fonts\.g/.test(q.url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' }); outside.push({ url: q.url(), method: q.method(), body: q.postData() || '' }); return r.abort(); });
    await ctx.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.push', 'on'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await ctx.newPage(); page.setDefaultTimeout(60000);
    const calls = [], responses = [];
    page.on('request', q => { const m = /\/\.netlify\/functions\/([\w-]+)/.exec(q.url()); if (m) calls.push({ t: Date.now(), op: (/"op":"(\w+)"/.exec(q.postData() || '') || [])[1] || '' }); });
    page.on('response', r => { if (/\/vendor\/firebase-/.test(r.url())) responses.push({ url: r.url(), status: r.status(), headers: r.headers() }); });
    const docResp = await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.CNSync && window.CNPush);

    /* ── 1 · the files, under COEP ── */
    const hdr = docResp.headers();
    assert.equal(hdr['cross-origin-embedder-policy'], 'require-corp'); assert.equal(hdr['cross-origin-opener-policy'], 'same-origin');
    assert.equal(await page.evaluate(() => window.crossOriginIsolated), true, 'the page is cross-origin isolated (COEP require-corp, COOP same-origin)');
    await page.waitForFunction(() => window.firebase && typeof firebase.firestore === 'function', null, { timeout: 30000 });
    const sdk = await page.evaluate(() => ({ version: firebase.SDK_VERSION, firestore: typeof firebase.firestore, initializeApp: typeof firebase.initializeApp, auth: typeof firebase.auth, apps: firebase.apps.map(a => a.name + ':' + a.options.projectId), isolated: crossOriginIsolated }));
    assert.equal(sdk.version, '10.14.1'); assert.equal(sdk.firestore, 'function'); assert.equal(sdk.auth, 'undefined', 'no auth SDK in the page'); assert.deepEqual(sdk.apps, ['cn-push:gokudatabase']);
    assert.equal(responses.length, 2, 'two library files requested: ' + responses.map(r => r.url).join(' '));
    for (const r of responses) assert.equal(r.status, 200, r.url);
    const lic = fs.readFileSync(path.join(root, 'vendor/firebase-10.14.1-LICENSE.txt'), 'utf8');
    for (const f of ['vendor/firebase-app-compat-10.14.1.js', 'vendor/firebase-firestore-compat-10.14.1.js']) assert(lic.includes(sha(f)), f + ': the sha256 in the licence file is the file\'s: ' + sha(f));
    assert(lic.includes('Apache License') && lic.includes('Version 2.0, January 2004'), 'the Apache-2.0 text is beside them');
    say(`1 · the real firebase ${sdk.version} compat bundles (${responses.map(r => r.url.replace(/.*\//, '').split('?')[0]).join(', ')}) loaded 200 under COEP require-corp in a cross-origin isolated page, firebase.firestore is a ${sdk.firestore}, no auth SDK, their sha256 are the licence file's`);

    /* ── 2 · what the real SDK asks of the cloud (refused: the network is cut) ── */
    await until(() => outside.length > 0, 20000, 'the SDK tries the cloud');
    await sleep(6000);   // (its retries and its long-polling fallback too)
    // (the SDK's own check of whether the network is there at all, after its channel failed: one GET of Google's 1-pixel image, no data of ours in it)
    const probe = outside.filter(o => o.method === 'GET' && /^https:\/\/www\.google\.com\/images\/cleardot\.gif(\?|$)/.test(o.url));
    const cloud = outside.filter(o => !probe.includes(o));
    assert.equal(probe.filter(o => o.body).length, 0, 'the connectivity probe carries nothing');
    const hosts = [...new Set(cloud.map(o => new URL(o.url).hostname))];
    assert.deepEqual(hosts, ['firestore.googleapis.com'], 'the only host it tried: ' + hosts.join(', ') + ' (and ' + probe.length + ' connectivity probe(s))');
    const paths = [...new Set(cloud.map(o => new URL(o.url).pathname))];
    assert(paths.every(p => /^\/google\.firestore\.v1\.Firestore\/Listen\/channel$/.test(p)), 'only the Listen channel: ' + paths.join(' '));
    assert(!cloud.some(o => /\/Write\b|:commit|:batchGet|:runQuery|:beginTransaction|identitytoolkit|securetoken|installations/i.test(o.url + o.body)), 'never a Write, a commit, a query, a lookup or an identity call');
    const dbs = [...new Set(cloud.map(o => new URL(o.url).searchParams.get('database')).filter(Boolean))];
    assert.deepEqual(dbs, ['projects/gokudatabase/databases/(default)'], 'for the project\'s own database: ' + dbs.join(' '));
    const docsAsked = new Set(); for (const o of cloud) { let b = o.body; try { b = decodeURIComponent(b.replace(/\+/g, ' ')); } catch (_) {} for (const m of b.matchAll(/documents\/([\w./()-]+)/g)) docsAsked.add(m[1]); }
    assert.deepEqual([...docsAsked], ['Charm_Nest_Push/counter'], 'the only document it named: ' + [...docsAsked].join(', '));
    say(`2 · with the network cut the real SDK made ${cloud.length} request(s) to ${hosts.join(', ')}${probe.length ? ' (+ ' + probe.length + ' probe(s) of google.com/images/cleardot.gif, its own connectivity check)' : ''}: ${paths.join(', ')} for ${dbs[0]}; the one document it named: ${[...docsAsked].join(', ')}; no Write, no commit, no query, no identity call`);

    /* ── 3 · and the page is on the polled feed again, with the reason ── */
    await until(() => page.evaluate(() => !CNSync.push.status().ok && /no answer from the live channel in 20 s|offline for more than 20 s|unavailable/.test(CNSync.push.status().why)), 40000, 'polling with its reason');
    const s = await page.evaluate(() => CNSync.push.status());
    assert(s.state === 'polling' && s.text === `Live updates: polling (${s.why})`, JSON.stringify(s));
    const t0 = Date.now(); for (let i = 0; i < 10; i++) { await page.mouse.move(400 + (i % 5) * 20, 400); await sleep(1000); }
    const n = calls.filter(c => c.t >= t0 && c.op === 'getChanges').length;
    assert(n >= 2, 'the polled pace: ' + n + ' feed questions in 10 s of use');
    say(`3 · "${s.text}", ${n} feed questions in 10 s of use (the polled pace)`);

    /* ── 4 · the service worker keeps them by token; no boot cost ── */
    {
      const C = await browser.newContext({ viewport: { width: 1440, height: 900 } });
      await C.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      await C.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.push', 'on'); localStorage.setItem('cn.sw', 'on'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
      const P = await C.newPage(); P.setDefaultTimeout(60000);
      const lib = /\/vendor\/firebase-(app|firestore)-compat-10\.14\.1\.js\?v=/;
      await P.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await P.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.CNPush);
      await until(() => P.evaluate(async () => { const c = await caches.open('cn-static-v1'), k = (await c.keys()).map(r => r.url); return k.filter(u => /vendor\/firebase-(app|firestore)-compat-10\.14\.1\.js\?v=/.test(u)).length === 2; }), 60000, 'the worker keeps the two library files');
      const kept = await P.evaluate(async () => (await (await caches.open('cn-static-v1')).keys()).map(r => r.url.replace(/^.*\//, '')).filter(u => /firebase/.test(u)));
      const first = hits.filter(u => lib.test(u)).length;
      assert.equal(first, 2, 'the first load asked the network for each file once: ' + first);
      // a warm reload: the library comes from the worker's cache, never the network; the boot's own static requests do not include it
      const n0 = hits.length; const reqs = [];
      P.on('request', q => { if (q.url().startsWith(srv.sorterOrigin) && !/\.netlify\//.test(q.url())) reqs.push(q.url().replace(srv.sorterOrigin + '/', '')); });
      await P.reload({ waitUntil: 'load' }); const atLoad = reqs.slice();
      await P.waitForFunction(() => window.firebase && typeof firebase.firestore === 'function', null, { timeout: 30000 });
      await sleep(1500);
      const after = hits.slice(n0), warmStatic = after.filter(u => !/\.netlify\//.test(u) && !/\/charm-nest-1\.html/.test(u));
      assert.equal(after.filter(u => lib.test(u)).length, 0, 'a warm reload asks the network for none of the library (the worker answers): ' + after.filter(u => lib.test(u)).join(' '));
      assert(!atLoad.some(u => /vendor\/firebase/.test(u)), 'and it is not among the boot\'s requests: ' + atLoad.filter(u => /vendor\/firebase/.test(u)).join(' '));
      assert(warmStatic.length <= 3, 'the warm boot still makes ≤ 3 requests for static files at the server: ' + warmStatic.length + ' [' + warmStatic.map(u => u.split('?')[0]).join(' ') + ']');
      say(`4 · the service worker keeps ${kept.join(', ')} by their token; a warm reload makes ${warmStatic.length} static request(s) at the server, none for the library (it is read from the worker's cache when the leader listens, ${reqs.filter(u => /vendor\/firebase/.test(u)).length} request(s) in the page, 0 at the server), and none of it at boot`);
      await C.close();
    }
    console.log('push-sdk: OK');
  } catch (e) { console.error(e); process.exitCode = 1; }
  finally { if (browser) await browser.close().catch(() => {}); srv.close(); }
})();
