// The browser half of the complete sandbox wipe (Paul, 10 Oct 2026: "None of these options actually fully delete all memory of
// the Sandbox testing ... ALL other data should be wiped. The only thing should remain is the employee efficiency and the Charm
// repo"; 03:58: even the sandbox's copies of real orders, seals and cancels are sandbox data).
// Every store a sandbox run writes in the BROWSER is one entry of the registry (charm-nest-sandbox-families.js: browser()),
// worked by one engine (charm-nest-sandbox-browser.js). This test checks that the list is complete and that it works:
//   A · derived from the CODE (no hand list): every localStorage key the page and the station build with the sandbox's mark
//       is matched by the registry, and every other "sandbox"-named key is a declared control mark (browserKept), so a future
//       store that writes a sandbox key the wipe does not match fails here
//   B · real headless Chromium, offline, against the repo's fake server: a sandbox page holding one entry of EVERY store of the
//       registry (IndexedDB workspace parts and databases, localStorage keys of every file, shared lists, mail notices, QR label,
//       saved seed, sessionStorage, Cache API, page memory) presses Reset: the count (Sandbox.browserLeft, read only) goes from
//       positive to zero for every store, a timer that keeps writing during the reset is held back, the real side (settings but
//       the seed, keys, IndexedDB, caches, labels, notices, the pull marker) is byte for byte as it was, and another tab of the
//       browser stands down by the storage event, by the BroadcastChannel alone, and on waking, never without a reset
//   node tests/charm-nest/sandbox-wipe-browser.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const reg = require(path.join(root, 'charm-nest-sandbox-families.js')), engine = require(path.join(root, 'charm-nest-sandbox-browser.js'));

/* ══ A · the keys, derived from the code ══ */
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const files = ['charm-nest-1.html', 'design-1.html', ...[...html.matchAll(/<script src="([^"?]+)/g)].map(m => m[1]).filter(f => !/^(vendor|lib)\//.test(f) && fs.existsSync(path.join(root, f)))]
  .filter(f => !/^charm-nest-sandbox-(families|browser)\.js$/.test(f));
const SB = new RegExp(reg.browser().find(e => e.key === 'localStorage:sandbox').match);
const derived = new Map();   // key → files
const add = (k, f) => { if (!derived.has(k)) derived.set(k, new Set()); derived.get(k).add(f); };
const controls = new Map();  // a "sandbox"-named key that is NOT the sandbox's data (the reset's own marks)
for (const f of files) {
  const src = fs.readFileSync(path.join(root, f), 'utf8');
  // 1 · a key written out whole: "cn.simClock.sandbox", "cn.autoCancel.v1:sandbox", "cn.roseRehearsal.sandbox.v1"
  for (const m of src.matchAll(/["'`]([A-Za-z][\w.\-]*[:.]sandbox(?:[:.][\w.\-]*)?)["'`]/g)) { if (m[1] !== 'designStation.sandbox') add(m[1], f); }
  // 2 · a name with the mark added to it: "cn.tl.sheets" + (WORKSPACE_SANDBOX ? ":sandbox" : ""), 'cn.listImageFraming.'+(X?'sandbox':'production'),
  //     and "…" + NS where NS = X ? ":sandbox" : ""
  for (const t of src.matchAll(/["'`]([A-Za-z][\w.\-]*[.:]?)["'`]\s*\+\s*\(([^;]{0,160}?)\?\s*(["'])(:?sandbox)\3\s*:/g)) add(t[1] + t[4], f);
  // …or the namespace variable NS = X ? ":sandbox" : "" and every "base" + NS of the file
  if (/\bNS\s*=\s*[^;]{0,120}\?\s*(["']):sandbox\1/.test(src)) for (const u of src.matchAll(/["'`]([A-Za-z][\w.\-]*)["'`]\s*\+\s*NS\b/g)) add(u[1] + ':sandbox', f);
  // 3 · the reset's own marks (a name that only CONTAINS the word): cn.sandboxHold, cn.sandboxLastReset, cn.sandboxAutoPull …
  for (const m of src.matchAll(/["'`](cn\.sandbox[A-Za-z]+)["'`]/g)) controls.set(m[1], f);
}
const keys = [...derived.keys()].sort();
for (const anchor of ['cn.simClock.sandbox', 'cn.arrivals.sandbox', 'cn.team.drafts:sandbox', 'cn.mail.drafts:sandbox', 'cn.tl.sheets:sandbox', 'cn.orderhold.run:sandbox', 'cn.sheetwin.freed:sandbox', 'cn.sheetwin.moves:sandbox', 'cn.customRead.sandbox', 'cn.listImageFraming.sandbox', 'cn.roseRehearsal.sandbox.v1', 'cn.autoCancel.v1:sandbox', 'designStation.chatDrafts.v1:sandbox', 'designCompletedLedger.v1:sandbox', 'designStation.etsyMeter.v1:sandbox'])
  assert(derived.has(anchor), `the scan of the code finds the key ${anchor} (the scan is not vacuous): found ${keys.join(', ')}`);
const unmatched = keys.filter(k => !engine.isMarked('localStorage', k));
assert.deepStrictEqual(unmatched, [], 'every sandbox-marked key the code builds is matched by the registry: ' + unmatched.join(', '));
const keptText = JSON.stringify(reg.browserKept());
for (const [k, f] of controls) assert(keptText.includes(k.replace(/^cn\./, 'cn.')), `the key ${k} (${f}) names the sandbox without carrying its mark, so it must be a declared control mark in browserKept()`);
// the station's own clean-up (design-1.html sandbox.reset) uses the same mark as the registry
const station = fs.readFileSync(path.join(root, 'design-1.html'), 'utf8');
assert(station.includes('const mark = /' + SB.source + '/'), 'the Design Station clears every key with the registry\'s mark: ' + SB.source);
// the registry's entries are well formed: a kind the engine knows, a label, store "browser"
for (const e of reg.browser()) { assert(e.store === 'browser' && e.key && e.label && e.kind, 'entry ' + JSON.stringify(e)); assert(['localStorage', 'sessionStorage', 'sharedList', 'sharedMap', 'flagged', 'setting', 'idb', 'idbDatabases', 'cache', 'memory', 'station'].includes(e.kind), 'a kind the engine works: ' + e.kind); }
console.log(`A · ${keys.length} sandbox keys derived from ${files.length} files, all matched by the registry; ${controls.size} control marks declared`);

/* ══ B · the real page ══ */
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const RID = '4173162973', OLD = 'OLDREC';

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  st.put('Sandbox_Charm_Custom_Orders', `${RID}_9001`, { key: `${RID}_9001`, receiptId: RID, sku: 'CHAIN_8941', state: 'completed', updatedAtMs: Date.now() });
  st.put('Charm_Custom_Orders', 'prod-1', { key: 'prod-1', receiptId: '4100000001', state: 'completed' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.addInitScript(({ sorter }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester'); window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; }, { sorter: sorterOrigin });
  await ctx.route(/firebaseOrders/, r => (r.request().method() === 'POST' ? r.abort() : r.continue()));   // (no message is ever delivered: what waits in an outbox stays)
  const errors = [];
  const open = async () => { const p = await ctx.newPage(); p.on('pageerror', e => errors.push(e.message)); await p.goto(`${sorterOrigin}/charm-nest-1.html`); return p; };
  const boot = p => p.waitForFunction(() => window.CN && window.TeamMail && window.Sandbox && window.Session && Session.ready() && window.CharmNestSandboxBrowser, null, { timeout: 60000 });
  const page = await open();
  await page.waitForFunction(() => window.CN && window.Sandbox, null, { timeout: 60000 });
  const SETTINGS_EXTRA = { sandbox: 'on', sandboxStream: 'on', sandboxSpeed: 50, sandboxSeed: 424242, dsOrigin: stationOrigin, pollOrders: 'off', runMode: 'manual' };
  await page.evaluate(x => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, x); localStorage.setItem('cn.settings', JSON.stringify(s)); }, SETTINGS_EXTRA);
  await page.reload(); await boot(page);
  assert(await page.evaluate(() => WORKSPACE_SANDBOX), 'the page is in the sandbox');

  // ── seed: one entry of EVERY store, plus the real side beside them ──
  // (every sandbox key the code builds, found in A, is seeded with a record of the old run)
  const sandboxKeys = Object.fromEntries(keys.filter(k => !/^designStation|^designCompletedLedger/.test(k)).map(k => [k, { [OLD]: RID, at: Date.now() }]));
  Object.assign(sandboxKeys, { 'cn.team.outbox:sandbox': [{ id: 'q-old', rid: RID, text: OLD + ' message', who: 'paul', at: Date.now(), tries: 0 }], 'future.store:sandbox': { [OLD]: 1 }, 'cn.sandboxPull:sandbox': { at: 1, count: 250, calls: 3 } });
  const prodKeys = { 'cn.team.outbox': [{ id: 'q-prod', rid: '4100000001', text: 'production message', who: 'paul', at: Date.now(), tries: 0, error: 'held' }], 'cn.team.drafts': { 4100000001: { t: 'production draft', at: Date.now() } }, 'cn.arrivals.production': { seen: { 4100000001: 1 }, lastCheck: 1, nextCheck: 1, lastAdded: 0, error: null }, 'cn.customRead.production': { '4100000001_1': { hash: 'h', at: Date.now() } }, 'cn.listImageFraming.production': [['2', { s: 3, x: 0, y: 0 }]], 'cn.listingSkus.v1': { v: 1, tables: { 111: { at: Date.now(), skus: ['A'] } } }, 'cn.listingPhotos.v1': [['111', '/img/111']], 'cn.mail.drafts': { 4100000001: { t: 'production mail draft', at: Date.now() } }, 'cn.sheetwin.moves': { 4100000001: { rid: '4100000001' } } };
  const shared = {
    'orderTimeline.outbox.v1': [{ orderId: RID, type: 'qrLabel', id: OLD + '-ev', sandbox: true, mode: 'sorter', at: Date.now() }, { orderId: '4100000001', type: 'scan', id: 'prod-ev', sandbox: false, mode: 'station', at: Date.now() }],
    'cn.mail.outbox': [{ id: 'm-sb', body: { sandbox: true, receiptId: RID, clientId: 'm-sb', text: OLD }, at: Date.now(), tries: 0 }, { id: 'm-prod', body: { sandbox: false, receiptId: '4100000001', clientId: 'm-prod', text: 'prod' }, at: Date.now(), tries: 0 }],
    'station_activity_q.dev1': [{ id: 'a-sb', sandbox: true, action: 'print', at: Date.now() }, { id: 'a-prod', action: 'scan', at: Date.now() }],
    'station_activity_q.dev2': [{ id: 'a-sb2', sandbox: true, action: 'scan', at: Date.now() }],
    'cn.mail.told': { olsb_1: 5, ol_1: 6 },
    'qrPrintAll': { sandbox: true, dispatchDate: '10 Oct 2026', userTypedOrderNum: RID, items: [{ receipt_id: RID, title: 'x' }] }
  };
  await page.evaluate(({ a, b, c, RID }) => {
    for (const o of [a, b, c]) for (const [k, v] of Object.entries(o)) localStorage.setItem(k, JSON.stringify(v));
    sessionStorage.setItem('cn.sandboxPullNext', '1'); sessionStorage.setItem('probe:sandbox', 'old'); sessionStorage.setItem('cn.eff.view', 'real');
    Review.settled().unshift({ key: `ord:custom:${RID}:CHAIN_8941`, kind: 'customOrder', why: 'QR label printed by paul', lines: 1, orders: [RID], by: 'paul', t: Date.now() - 3600000 });
    B.customDesigns[`custom:${RID}:X`] = { ck: `custom:${RID}:X`, rid: RID, at: Date.now(), files: [], sent: { id: 's', at: Date.now(), by: 'paul', lines: {} } };
    Session.schedule();
  }, { a: sandboxKeys, b: prodKeys, c: shared, RID });
  // IndexedDB: the workspace's two sides, and databases of the sandbox's name and of a real one; Cache API likewise
  const idbDo = (p, fn, arg) => p.evaluate(async ([src, arg]) => (0, eval)(src)(arg), [fn.toString(), arg]);
  const PROD_CHECKPOINT = { v: 1, at: 1, epoch: '', marker: 'production-untouched', settled: [{ key: 'prod' }] };
  await idbDo(page, ([k, v]) => new Promise((res, rej) => { const r = indexedDB.open('charm-nest-workspace', 1); r.onsuccess = () => { const tx = r.result.transaction('workspaces', 'readwrite'); tx.objectStore('workspaces').put(v, k); tx.oncomplete = () => res(true); tx.onerror = () => rej(tx.error); }; r.onerror = () => rej(r.error); }), ['production', PROD_CHECKPOINT]);
  await idbDo(page, ([k, v]) => new Promise((res, rej) => { const r = indexedDB.open('charm-nest-workspace', 1); r.onsuccess = () => { const tx = r.result.transaction('workspaces', 'readwrite'); tx.objectStore('workspaces').put(v, k); tx.oncomplete = () => res(true); tx.onerror = () => rej(tx.error); }; r.onerror = () => rej(r.error); }), ['sandbox:best:sheet-old', { jobId: OLD }]);
  await idbDo(page, () => Promise.all(['probe:sandbox', 'probe-real'].map(n => new Promise(res => { const r = indexedDB.open(n, 1); r.onupgradeneeded = () => r.result.createObjectStore('s'); r.onsuccess = () => { r.result.close(); res(); }; }))));
  await idbDo(page, async () => { for (const n of ['probe:sandbox', 'probe-real']) { const c = await caches.open(n); await c.put('/x', new Response('old')); } });
  assert.strictEqual(await page.evaluate(() => Session.flushNow()), true, 'the workspace was saved');
  const snapshot = p => p.evaluate(() => ({ ls: Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)])), ss: Object.fromEntries(Object.keys(sessionStorage).map(k => [k, sessionStorage.getItem(k)])) }));

  // ── the count: positive for every store the page can count, read only ──
  // (a spy on every storage write that comes from the engine's own file: counting writes nothing; the page's own timers may write meanwhile)
  await page.evaluate(() => { window.__engineWrites = []; for (const m of ['setItem', 'removeItem', 'clear']) { const real = Storage.prototype[m]; Storage.prototype[m] = function (...a) { if (/charm-nest-sandbox-browser/.test(new Error().stack || '')) window.__engineWrites.push([m, a[0]]); return real.apply(this, a); }; } });
  const rows = await page.evaluate(() => Sandbox.browserLeft());
  assert.deepStrictEqual(await page.evaluate(() => window.__engineWrites), [], 'browserLeft() wrote nothing to localStorage or sessionStorage');
  assert.deepStrictEqual(rows.map(r => r.key), reg.browser().map(e => e.key), 'one row per store of the registry, in its order');
  for (const r of rows) { if (r.key === 'station:browser') assert.strictEqual(r.n, null, 'the Design Station\'s own storage cannot be counted from here'); else assert(r.n > 0, `before the reset the page holds entries of "${r.label}": ${JSON.stringify(r)}`); }
  assert.strictEqual(rows.find(r => r.key === 'localStorage:queued-items').n, 4, 'only the sandbox\'s rows of the shared lists are counted (timeline, mail, two activity)');
  const beforeSettings = JSON.parse((await snapshot(page)).ls['cn.settings']);
  assert.strictEqual(beforeSettings.sandboxSeed, 424242);

  // ── other tabs of the same browser, each able to hear the reset in ONE way only ──
  await page.waitForTimeout(2600);   // (the outboxes flush 1.5 s after a load)
  const tab2 = await open(); await boot(tab2); await tab2.evaluate(() => { window.__stale = 1; });                       // hears it by the storage event (and the channel)
  const tab3 = await open(); await boot(tab3); await tab3.evaluate(() => { window.__stale = 1; window.addEventListener('storage', e => e.stopImmediatePropagation(), true); });   // by the BroadcastChannel alone

  // ── the press; a timer that goes on writing the sandbox's keys while it runs ──
  await page.evaluate(() => { window.__writer = setInterval(() => { for (const [k, v] of [['cn.arrivals.sandbox', '{"seen":{"OLDREC":1}}'], ['qrPrintAll', '{"sandbox":true,"items":[1]}'], ['cn.customRead.sandbox', '{"x":1}'], ['cn.mail.told', '{"olsb_9":1,"ol_1":6}']]) localStorage.setItem(k, v); try { const s = JSON.parse(localStorage.getItem('cn.settings')); s.sandboxSeed = 424242; localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} sessionStorage.setItem('late:sandbox', '1'); }, 30); });
  await page.evaluate(() => { window.__reset = Sandbox.reset({ button: document.getElementById('stSandboxReset'), note: document.getElementById('stSandboxResetNote') }); });
  // while the reset runs the write guard is up: the sandbox's keys (every kind) are refused, the real side's are written, a shared
  // list or map is written without the sandbox's items, the settings without the saved seed
  await page.waitForTimeout(150);
  const guard = await page.evaluate(() => {
    const out = {}, put = (k, v) => { localStorage.setItem(k, v); return localStorage.getItem(k); };
    out.sandboxKey = put('cn.arrivals.sandbox', '{"seen":{"NEWREC":1}}'); out.future = put('guard.fresh:sandbox', 'NEWREC'); out.label = put('qrPrintAll', '{"sandbox":true,"items":["NEWREC"]}');
    out.real = put('cn.guard.real', 'ok'); out.realLabel = put('qrPrintAll2', '{"items":[1]}');
    out.map = put('cn.mail.told', '{"olsb_9":1,"ol_1":6}'); out.list = put('station_activity_q.probe', JSON.stringify([{ id: 'x', sandbox: true }, { id: 'y', sandbox: false }]));
    const s = JSON.parse(localStorage.getItem('cn.settings')); s.sandboxSeed = 7; out.settings = JSON.parse(put('cn.settings', JSON.stringify(s)));
    sessionStorage.setItem('late:sandbox', '1'); out.session = sessionStorage.getItem('late:sandbox'); sessionStorage.setItem('cn.guard.real', 'ok'); out.sessionReal = sessionStorage.getItem('cn.guard.real');
    out.active = window.CNWipe.active; return out;
  });
  assert(guard.active === true, 'the reset is under way (the guard is up)');
  assert.deepStrictEqual([/NEWREC/.test(guard.sandboxKey), guard.future, /NEWREC/.test(guard.label), guard.session], [false, null, false, null], 'the guard refuses the sandbox\'s keys, a store added later, the sandbox\'s label and its session keys: ' + JSON.stringify(guard));
  assert.deepStrictEqual([guard.real, guard.realLabel, guard.sessionReal], ['ok', '{"items":[1]}', 'ok'], 'the guard lets the real side write');
  assert.strictEqual(guard.map, '{"ol_1":6}', 'a shared map is written without the sandbox\'s ids');
  assert.deepStrictEqual(JSON.parse(guard.list).map(e => e.id), ['y'], 'a shared list is written without the sandbox\'s items');
  assert.strictEqual(guard.settings.sandboxSeed, 0, 'the settings are written without the saved seed');
  await page.waitForNavigation({ waitUntil: 'load', timeout: 60000 }); await boot(page);
  await tab2.waitForFunction(() => !window.__stale, null, { timeout: 15000 });
  await tab3.waitForFunction(() => !window.__stale, null, { timeout: 15000 });
  await boot(tab2); await boot(tab3);
  await page.waitForTimeout(1500);

  // ── after: every store at zero, the real side as it was ──
  const after = await snapshot(page);
  const left = await page.evaluate(() => Sandbox.browserLeft());
  for (const r of left) if (r.key !== 'station:browser') assert.strictEqual(r.n, 0, `after the reset nothing is left of "${r.label}": ${JSON.stringify(r)} · sandbox keys: ${JSON.stringify(Object.entries(after.ls).filter(([k]) => SB.test(k)).map(([k, v]) => [k, String(v).slice(0, 80)]))}`);
  for (const k of Object.keys(after.ls)) assert(!(SB.test(k) && k !== 'cn.resetEpoch.sandbox'), `a sandbox-marked key survived the reset: ${k}`);
  assert(!/OLDREC|olsb_|q-old|m-sb|a-sb/.test(JSON.stringify(Object.entries(after.ls).filter(([k]) => k !== 'cn.settings'))), 'no record of the old run is left in localStorage');
  assert(after.ls['cn.resetEpoch.sandbox'], 'the epoch (the mark every tab reads) is there');
  assert(after.ls['cn.sandboxHold'] != null, 'the clean sandbox waits for Start (the Hold mark)');
  for (const [k, v] of Object.entries(prodKeys)) assert.strictEqual(after.ls[k], JSON.stringify(v), `the real key ${k} is unchanged`);
  assert.deepStrictEqual(JSON.parse(after.ls['orderTimeline.outbox.v1']).map(e => e.id), ['prod-ev'], 'the shared timeline list keeps the real event only');
  assert.deepStrictEqual(JSON.parse(after.ls['cn.mail.outbox']).map(e => e.id), ['m-prod'], 'the shared mail list keeps the real message only');
  assert.deepStrictEqual(JSON.parse(after.ls['station_activity_q.probe']).map(e => e.id), ['y'], 'what the guard let through of a shared list (the real item) is kept');
  assert.deepStrictEqual(JSON.parse(after.ls['station_activity_q.dev1']).map(e => e.id), ['a-prod'], 'the shared activity queue keeps the real event only');
  assert.strictEqual(after.ls['station_activity_q.dev2'], undefined, 'a queue holding only the sandbox\'s events is gone');
  assert.deepStrictEqual(JSON.parse(after.ls['cn.mail.told']), { ol_1: 6 }, 'the customer-mail notices keep the real ids only');
  assert.strictEqual(after.ls['qrPrintAll'], undefined, 'the sandbox\'s QR label is gone');
  assert(after.ls['cn.guard.real'] === 'ok' && after.ls['qrPrintAll2'] === '{"items":[1]}', 'what the guard let through (the real side) is kept');
  const afterSettings = JSON.parse(after.ls['cn.settings']);
  assert.deepStrictEqual(afterSettings, Object.assign({}, beforeSettings, { sandboxSeed: 0 }), 'the page settings are as they were but the saved seed (the sandbox stays on, its stream, speed, Auto and the rest keep their values)');
  assert.strictEqual(after.ss['cn.sandboxPullNext'], '1', 'the pull marker the page after the reload reads is kept');
  assert.strictEqual(after.ss['cn.eff.view'], 'real', 'the real side\'s session key is kept');
  assert(!('probe:sandbox' in after.ss) && !('late:sandbox' in after.ss), 'the sandbox\'s session keys are gone, and the one a late timer wrote');
  const st2 = await page.evaluate(async () => ({ dbs: (await indexedDB.databases()).map(d => d.name).sort(), caches: (await caches.keys()).sort() }));
  assert(st2.dbs.includes('probe-real') && !st2.dbs.includes('probe:sandbox'), 'the sandbox\'s database is gone, the real one stays: ' + st2.dbs);
  assert(st2.caches.includes('probe-real') && !st2.caches.includes('probe:sandbox'), 'the sandbox\'s cache is gone, the real one stays: ' + st2.caches);
  const dump = await idbDo(page, () => new Promise((res, rej) => { const r = indexedDB.open('charm-nest-workspace', 1); r.onsuccess = () => { const s = r.result.transaction('workspaces', 'readonly').objectStore('workspaces'), keys = s.getAllKeys(), out = {}; keys.onsuccess = () => { let n = keys.result.length; if (!n) return res(out); for (const k of keys.result) { const g = s.get(k); g.onsuccess = () => { out[k] = g.result; if (!--n) res(out); }; } }; }; r.onerror = () => rej(r.error); }));
  assert.deepStrictEqual(dump.production, PROD_CHECKPOINT, 'the real workspace checkpoint is unchanged');
  assert(!('sandbox:best:sheet-old' in dump) && (!dump.sandbox || (dump.sandbox.settled.length === 0 && dump.sandbox.epoch)), 'the sandbox\'s workspace parts are gone');
  const mem = await page.evaluate(() => ({ settled: Review.settled().length, designs: Object.keys(B.customDesigns).length, seed: S.settings.sandboxSeed }));
  assert.deepStrictEqual(mem, { settled: 0, designs: 0, seed: 0 }, 'page memory holds none of it: ' + JSON.stringify(mem));
  assert.strictEqual(st.list('Sandbox_Charm_Custom_Orders').length, 0, 'the cloud\'s sandbox records went (the server half)');
  assert.strictEqual(JSON.stringify(st.list('Charm_Custom_Orders')), JSON.stringify([Object.assign({ _id: 'prod-1' }, { key: 'prod-1', receiptId: '4100000001', state: 'completed' })]), 'production records untouched');

  // ── the real side is never matched: a real label and a real notice survive the engine itself ──
  await page.evaluate(() => { localStorage.setItem('qrPrintAll', JSON.stringify({ items: [{ receipt_id: '4100000001' }] })); localStorage.setItem('cn.mail.told', JSON.stringify({ ol_5: 1 })); localStorage.setItem('cn.team.outbox', '[]'); CharmNestSandboxBrowser.sweepSync(); });
  const real = await snapshot(page);
  assert(real.ls['qrPrintAll'] && real.ls['cn.mail.told'] === '{"ol_5":1}' && real.ls['cn.team.outbox'] === '[]', 'sweeping again leaves a real label, a real notice and a real queue as they are');

  // ── the other tabs: both stood down, neither without a reset ──
  assert.deepStrictEqual(await tab2.evaluate(() => ({ settled: Review.settled().length, designs: Object.keys(B.customDesigns).length })), { settled: 0, designs: 0 }, 'tab 2 (storage event) came back clean');
  assert.deepStrictEqual(await tab3.evaluate(() => ({ settled: Review.settled().length })), { settled: 0 }, 'tab 3 (BroadcastChannel alone) came back clean');
  // a tab that slept through the reset hears it when it wakes (no event reached it): focus, visibility, pageshow
  const tab4 = await open(); await boot(tab4); await tab4.evaluate(() => { window.__stale = 1; });
  await tab4.evaluate(() => { localStorage.setItem('cn.resetEpoch.sandbox', 'made-while-asleep'); window.dispatchEvent(new Event('focus')); });
  await tab4.waitForFunction(() => !window.__stale, null, { timeout: 15000 });
  await boot(tab4);
  // …and none stands down without one
  const tab5 = await open(); await boot(tab5); await tab5.evaluate(() => { window.__stay = 1; window.dispatchEvent(new Event('focus')); window.dispatchEvent(new Event('pageshow')); document.dispatchEvent(new Event('visibilitychange')); new BroadcastChannel('cn-sandbox-epoch').postMessage({ scope: 'production', epoch: 'other-scope' }); });
  await tab5.waitForTimeout(2500);
  assert.strictEqual(await tab5.evaluate(() => window.__stay), 1, 'a tab is never reloaded without a reset of its own side (focus, wake, a message for another scope)');

  assert.deepStrictEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors');
  console.log('B · every store of the registry went in one reset and was counted at zero after it (read only before), a late writer was held back, the real side is unchanged, three other tabs heard it by the storage event, the channel alone and on waking, none without a reset');
  await browser.close(); srv.close && srv.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
