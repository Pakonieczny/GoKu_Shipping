// KEEP-CHAOS: the gate for "drop a vector file on a Review order + switch SAVE on -> a permanent Charm repository entry under the listing's SKU"
// (Paul, 16 Oct 2026; properties P1-P8 of the KEEP brief). A fault-injection END-TO-END harness over the REAL page in Chromium:
//   the real charm-nest-1.html + bridge + charm-nest-keep.js, the stand-in site of bridge-server.cjs (the REAL charmNestLibrary / charmNestOutput handlers over an in-memory
//   Firestore + Storage, the REAL password door), a fake Etsy (the listing's SKU is written once, only after a saved repository step). No live service, no Etsy, no Netlify.
// Every scenario drives the person's path: open Review (Custom orders) -> DROP a real .ai on the order's card -> answer the REVDROP question -> switch
// "Save to the charm repository and link this listing" ON -> and then breaks something at one step, or two. Then it waits for quiescence (with a time limit) and asserts
// the FINAL state: server (exactly one entry for the dropped file's geometry hash, its files and records consistent, no duplicate alias, no litter, the Etsy SKU
// written once after the repository), the page (one calm line that tells the truth, no spinner left, no unhandled error, no second dialog) and the NEXT order of the
// same listing (a fresh page: it must pick the saved design by itself, the listing's other variation too).
//   node tests/charm-nest/keep-chaos.cjs          (run through /tmp/cn-run.sh <seconds> ...; PW_DIR=<playwright dir>, CHROMIUM=<chrome>)
//   ONLY=<regex on scenario names> · LIST=1 (names only) · SHARD=i/n · QUICK=1 (a representative subset) · TRIGGER=switch|send (default switch: the switch alone starts the
//   save; send = the switch arms it and Send to Sheet starts it, the way main did before KEEPSAVE) · SHOTS_DIR=<dir> (a picture of each scenario's final state)
//   OUT=<file.json> (the results) · MAXS=<seconds a save may take to finish, default 40>
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
const { start, REPO_TEST } = require('./bridge-server.cjs');

const ONLY = process.env.ONLY ? new RegExp(process.env.ONLY, 'i') : null, TRIGGER = process.env.TRIGGER || 'switch', SHOTS = process.env.SHOTS_DIR || '';
const STALL = (+process.env.STALL || 10) * 1000, MAXMS = (+process.env.MAXS || 40) * 1000, QUICK = process.env.QUICK === '1' || process.env.SAMPLE === '1', SAMPLE = process.env.SAMPLE === '1';
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ── the world's facts ── */
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const FILE_A = 'SCHRODINGER_CAT_1.ai', FILE_B = 'COMET_9427.ai', FILE_C = 'TINY_INITIAL_TAG_1.ai';   // (three real single-charm fixtures)
const fixture = n => fs.readFileSync(path.join(__dirname, 'fixtures', n));
const LISTING = '1800004821', SKU_RAW = 'ACORN&#39;S LEAF', TITLE = "Custom Charm - Acorn's Leaf, Personalized";
const order = (rid, listing = LISTING, sku = SKU_RAW, title = TITLE, variations) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: String(rid) + '1', listingId: String(listing), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations || [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
const RID1 = '4196630529', RID2 = '4196630530', RID3 = '4196630531';

/* ── the verdicts ── */
const results = [];

/* ═══════════════════════════ the world: server + browser context + fault layer ═══════════════════════════ */
async function mkWorld(browser, o = {}) {
  const srv = await start({ receipts: [], repoSeed: o.locked ? false : undefined });
  const w = { srv, o, log: [], stepN: {}, faults: [], inject: null, errors: [], consoleErrors: [], offline: false, inflight: 0, seq: 0, switchedOn: false, pages: [], orders: o.orders || [order(RID1)],
    etsy: { writes: {}, noops: 0, beforeRepo: 0, listingSku: {} }, barrier: null, holds: {}, pingAct: null, notes: [], t0: Date.now() };
  w.ctx = await browser.newContext({ viewport: o.viewport || { width: 1440, height: 950 }, timezoneId: 'UTC', locale: 'en-US', reducedMotion: 'reduce' });
  await w.ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await w.ctx.addInitScript(sandbox => {
    try { if (!sessionStorage.getItem('__seeded')) { if (!localStorage.getItem('cn.settings')) localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: sandbox ? 'on' : 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
    window.__dlg = { confirm: 0, alert: 0 }; window.confirm = () => { window.__dlg.confirm++; return true; }; window.prompt = () => 'Test Operator'; window.alert = () => { window.__dlg.alert++; };
    window.__errs = []; window.addEventListener('unhandledrejection', e => window.__errs.push('unhandledrejection: ' + String((e.reason && e.reason.message) || e.reason).slice(0, 200)));
    window.__toasts = [];
    const step = n => (typeof window.__cnStep === 'function' ? window.__cnStep(n) : Promise.resolve());
    try { const dig = crypto.subtle.digest.bind(crypto.subtle); crypto.subtle.digest = async function (a, d) { const n = d ? (d.byteLength || d.length || 0) : 0; if (n > 4000) await step('hash').catch(() => {}); return dig(a, d); }; } catch (_) {}
    try { const tb = HTMLCanvasElement.prototype.toBlob; HTMLCanvasElement.prototype.toBlob = function (cb, ...r) { const self = this; step('build').catch(() => {}).then(() => tb.call(self, cb, ...r)); }; } catch (_) {}
    window.addEventListener('DOMContentLoaded', () => {
      try { const P = window.CharmNestPDF; if (P && P.parseSource && !P.parseSource.__g) { const ps = P.parseSource.bind(P); P.parseSource = async function (...a) { await step('read').catch(() => {}); return ps(...a); }; P.parseSource.__g = true; } } catch (_) {}
      const t = window.toast; if (typeof t === 'function' && !t.__spy) { window.toast = function (m) { window.__toasts.push(String(m)); return t.apply(this, arguments); }; window.toast.__spy = true; }
    });
  }, !!o.sandbox);
  await w.ctx.exposeBinding('__cnStep', async (source, raw) => w.onStep(raw, source.page));
  await w.ctx.route(u => /\/\.netlify\/functions\/|\/__put\//.test(u.pathname), r => onRoute(w, r));
  w.base = null; w.onStep = (raw, page, extra) => onStepImpl(w, raw, page, extra);
  w.newPage = async () => {
    const page = await w.ctx.newPage(); page.setDefaultTimeout(25000);
    page.on('pageerror', e => w.errors.push('pageerror: ' + String(e.message).slice(0, 240)));
    page.on('console', m => { if (m.type() !== 'error') return; const t = m.text(); if (/Failed to load resource|net::ERR|status of 5\d\d|status of 40[1-9]|forced failure|Failed to fetch/i.test(t)) return; w.consoleErrors.push(t.slice(0, 240)); });
    w.pages.push(page); return page;
  };
  if (!o.noTable) for (const od of w.orders) { const L = od.lines[0].listingId, raw = od.lines[0].sku; srv.st.put('Charm_Listing_Skus', String(L), { at: Date.now(), n: 1, products: [{ id: '9100000001', sku: String(raw || '').replace('&#39;', "'"), d: 0, pv: ['513:1234'] }], nm: [], pn: [] }); }
  w.page = await w.newPage();
  await bootPage(w, w.page);
  w.base = snapshot(w);
  return w;
}
const closeWorld = async w => { try { await w.ctx.close(); } catch (_) {} try { w.srv.close(); } catch (_) {} };

/** load the sorter, seed the orders, open Review -> Custom orders (what a person who opens the app and looks at Review sees). */
async function bootPage(w, page, o = {}) {
  if (w.pingAct && !o.noPing) { /* (the route layer answers ping 401 until released) */ }
  await page.goto(`${w.srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  if (w.o.cloudDown && !o.skipReady) { await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet, null, { timeout: 60000 }); }
  else await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
  await seedOrders(page, o.orders || w.orders);
  await page.click('#reviewView .egTab[data-k="customOrder"]');
  await page.waitForSelector('#rvList .reviewListRow[data-rid="' + (o.orders || w.orders)[0].receiptId + '"]');
}
const seedOrders = (page, orders) => page.evaluate(async orders => {
  await Orders.loadMaps(true);
  for (const order of orders) for (const line of order.lines) {
    const key = CharmNestOrders.lineKey(order, line);
    if (B.orders.byKey.has(key)) continue;
    const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row);
  }
  B.maps.customRead = Object.assign({}, B.maps.customRead || {}, Object.fromEntries(orders.flatMap(o => o.lines.map(l => [CharmNestOrders.lineKey(o, l), { kind: 'custom', confidence: 0.96, summary: "the buyer's own design" }]))));
  Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
}, orders);

/* ── the fault layer: every call to the functions / the upload door goes through here ── */
const stepOf = (fn, op) => fn === '__put' ? 'upload' : fn === 'charmNestOutput' && /^(sign|put|signedUrl|upload)/.test(op) ? 'upload' : op === 'customSaveGet' ? 'open' : op === 'customSkuCheck' ? 'sku' : op === 'customSaveCommit' ? 'commit' : op === 'customEtsySkuWrite' ? 'etsy' : op === 'ping' ? 'ping' : 'other';
const WRITES = new Set(['upload', 'commit', 'etsy']);
async function onRoute(w, route) {
  const req = route.request(), url = new URL(req.url()), isPut = url.pathname.startsWith('/__put/');
  const fn = isPut ? '__put' : decodeURIComponent(url.pathname.split('/functions/')[1] || ''); let body = {}; try { body = JSON.parse(req.postData() || '{}'); } catch (_) {}
  const op = isPut ? 'put' : String(body.op || ''), step = stepOf(fn, op);
  const call = { i: ++w.seq, fn, op, step, t: Date.now() - w.t0, rid: body.rid, sku: body.sku, listing: body.listingId, sandbox: !!body.sandbox, switchedOn: w.switchedOn, status: null, result: null }; w.log.push(call);
  const n = w.stepN[step] = (w.stepN[step] || 0) + 1; call.n = n; w.inflight++;
  const out = async (status, obj, type) => { try { await route.fulfill({ status, contentType: type || 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: typeof obj === 'string' ? obj : JSON.stringify(obj) }); } catch (_) {} };
  try {
    if (w.offline) { call.status = 'offline'; return await route.abort('internetdisconnected'); }
    if (step === 'ping' && w.pingAct === '401') { call.status = 401; return await out(401, { error: 'unauthorized' }); }
    // the real call (the stand-in site), or, for the Etsy step, the fake Etsy
    const real = async () => {
      if (step === 'etsy') return fakeEtsy(w, body);
      const r = await route.fetch(); const buf = await r.body(); return { status: r.status(), type: r.headers()['content-type'] || 'application/json', body: buf };
    };
    const finish = async r => { call.status = r.status; try { await route.fulfill({ status: r.status, contentType: r.type, headers: { 'Access-Control-Allow-Origin': '*' }, body: r.body }); } catch (_) {} };
    // a standing fault of the scenario?
    const f = w.faults.find(f => !f.done && (f.step === step || (f.step === '*' && NET_STEPS.includes(step))) && (!f.nth || f.nth === n) && (!f.rid || f.rid === String(body.rid)));
    if (f) { f.hits = (f.hits || 0) + 1; if (f.times && f.hits >= f.times) f.done = true; if (!f.times) f.done = true; call.fault = f.act; if (f.on) { try { await f.on(w, call); } catch (_) {} } }
    // the step hook of the scenario (an interruption in the middle of this step; the server still does what it was asked)
    const act = f ? f.act : null;
    if (act === 'http500') { call.status = 500; return await out(500, { error: 'forced failure' }); }
    if (act === 'html') { call.status = 200; return await out(200, '<html><body>Bad gateway</body></html>', 'text/html'); }
    if (act === 'abort') { call.status = 'abort'; return await route.abort('connectionrefused'); }
    if (act === 'repo-locked') { call.status = 401; return await out(401, { error: 'repo-locked' }); }
    if (act === 'lost') { const r = await real(); call.result = 'lost'; call.status = 'lost'; return await out(500, { error: 'the reply was lost' }); }
    if (act === 'drop-after') { await real(); call.status = 'dropped'; return await route.abort('connectionreset'); }
    if (act === 'timeout') { await sleep(f.ms || 6000); call.status = 'timeout'; return await route.abort('timedout'); }
    if (act === 'slow') { await sleep(f.ms || 7000); return await finish(await real()); }
    if (act === 'hold') { await (w.holds[f.key || step] || Promise.resolve()); return await finish(await real()); }
    if (act === 'barrier') { await barrierWait(w, f); return await finish(await real()); }
    if (act === 'needs-scope') { await real(); call.status = 200; return await out(200, { ok: false, state: 'failed', reason: 'scope', retry: true }); }
    // no fault: the step hook (page-side damage while the request is in flight), then the real answer
    const hooked = await w.onStep(step, null, { call });
    const r = await real();
    if (w.offline) { call.status = 'offline-after'; return await route.abort('internetdisconnected'); }
    return await finish(r);
  } catch (e) { try { call.status = call.status || 'error:' + String(e.message).slice(0, 60); await route.abort('failed'); } catch (_) {} }
  finally { w.inflight--; w.lastNet = Date.now(); }
}
function barrierWait(w, f) { const b = w.barrier = w.barrier || { n: f.n || 2, wait: [] }; return new Promise(res => { b.wait.push(res); const go = () => { if (w.barrier === b) w.barrier = null; const all = b.wait; b.wait = []; all.forEach(r => r()); }; if (b.wait.length >= b.n) go(); else setTimeout(go, f.maxWait || 2500); }); }   // (a barrier nobody else reaches opens after 2.5 s: a keeper that serialises the saves is no deadlock)
/** the fake Etsy: the listing's SKU is written once; refused when the repository step is not saved; counts everything. */
function fakeEtsy(w, body) {
  const st = w.srv.st, rec = st.docs.get('Charm_Custom_Saves/' + body.rid), json = (status, o) => ({ status, type: 'application/json', body: Buffer.from(JSON.stringify(o)) });
  if (!rec || !rec.sku) { w.etsy.beforeRepo++; return json(200, { ok: false, state: 'failed', reason: 'no-record', retry: false, text: 'no saved record' }); }
  const L = String(rec.listingId || ''); const cur = w.etsy.listingSku[L];
  if (rec.etsy && rec.etsy.state === 'linked') { w.etsy.noops++; return json(200, { ok: true, state: 'linked' }); }
  if (cur === rec.sku) { w.etsy.noops++; st.put('Charm_Custom_Saves', String(body.rid), { etsy: Object.assign({}, rec.etsy || {}, { state: 'linked' }) }); return json(200, { ok: true, state: 'linked' }); }
  w.etsy.writes[L] = (w.etsy.writes[L] || 0) + 1; w.etsy.listingSku[L] = rec.sku;
  st.put('Charm_Custom_Saves', String(body.rid), { etsy: Object.assign({}, rec.etsy || {}, { state: 'linked' }) });
  return json(200, { ok: true, state: 'linked' });
}
/** the step hook: gates in the page (read, hash, build) and the calls (open, sku, upload, commit, etsy) arrive here; the scenario's interruption fires at the nth of its step. */
async function onStepImpl(w, raw, page, extra) {
  let step = raw;
  if ((raw === 'read' || raw === 'hash') && !w.switchedOn) step = 'drop-' + raw;
  if ((raw === 'build') && !w.switchedOn) return;
  const inj = w.inject; if (!inj || inj.fired || inj.step !== step) return;
  inj.hits = (inj.hits || 0) + 1; if (inj.hits < (inj.nth || 1)) return;
  inj.fired = true; inj.firedAt = Date.now() - w.t0;
  try { await inj.damage(w, { page, step, call: extra && extra.call }); } catch (e) { w.notes.push('damage failed: ' + String(e.message).slice(0, 100)); }
}

/* ═══════════════════════════ the person's moves ═══════════════════════════ */
const card = rid => `#rvList .reviewListRow[data-rid="${rid}"]`;
const dropRaw = (page, sel, name, b64) => page.evaluate(({ sel, name, b64 }) => {
  const dt = new DataTransfer(); dt.items.add(new File([Uint8Array.from(atob(b64), c => c.charCodeAt(0))], name));
  const n = document.querySelector(sel); if (!n) throw new Error('nothing to drop on: ' + sel);
  for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt }));
}, { sel, name, b64 });
/** drop a real .ai on the order's card and answer the REVDROP question (the first design of an order asks). */
async function dropFile(w, rid, name, o = {}) {
  const page = o.page || w.page, b64 = fixture(o.bytesOf || name).toString('base64');
  await dropRaw(page, o.sel || card(rid), name, b64);
  if (!o.noAsk) { await page.waitForSelector('#cuAsk [data-ask-yes]', { timeout: 8000 }); await page.click('#cuAsk [data-ask-yes]'); }
  if (!o.noWait) await fileReady(w, page, o.count || 1);
}
/** the design is on the order and its window is open and read: it is already there (restored after a reload), else dropped (and the question answered). */
async function ensureDesign(w, rid, name, page = w.page) {
  for (let i = 0; i < 2; i++) {
    const has = await page.evaluate(rid => Object.values(CustomSheet.entries()).some(e => String(e.rid) === String(rid) && e.files.length), rid).catch(() => false);
    if (!has) { try { await dropFile(w, rid, name, { page }); } catch (e) { w.notes.push('drop: ' + String(e.message).split('\n')[0].slice(0, 60)); } }
    else if (!(await winOpen(page))) await openWindow(w, rid, page);
    try { await fileReady(w, page, 1, 20000); return; } catch (_) { w.stuck = true; w.notes.push('the design stayed "reading" for 20 s'); await page.evaluate(() => { const b = document.querySelector('#cuDlg .cuFile [data-rm]'); if (b) b.click(); }).catch(() => {}); await sleep(500); }
  }
}
const fileReady = (w, page, count = 1, ms = 60000) => page.waitForFunction(c => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length >= c && !document.querySelector('#cuDlg .cuFile.reading'), count, { timeout: ms });
const switchVisible = page => page.evaluate(() => { const s = document.getElementById('cuKeepSwitch'), h = document.getElementById('cuKeep'); return !!(s && h && !h.hidden && s.getClientRects().length && !s.closest('[hidden]')); }).catch(() => false);
const stripPhase = page => page.evaluate(() => { const h = document.getElementById('cuKeep'); return h ? h.dataset.phase || '' : ''; });
const winOpen = page => page.evaluate(() => { const d = document.getElementById('cuDlg'); return !!(d && d.open); });
async function switchReady(page, ms = 45000) { await page.waitForFunction(() => { const s = document.getElementById('cuKeepSwitch'), h = document.getElementById('cuKeep'); return !!(s && h && !h.hidden && s.getClientRects().length && s.getAttribute('aria-checked') !== null); }, null, { timeout: ms }); }
const switchState = page => page.evaluate(() => { const s = document.getElementById('cuKeepSwitch'); return s ? s.getAttribute('aria-checked') === 'true' : null; });
async function switchOn(w, o = {}) {
  const page = o.page || w.page;
  await switchReady(page, o.ms);
  if (!(await switchState(page))) { w.switchedOn = true; w.onAt = Date.now() - w.t0; await page.click('#cuKeepSwitch'); }
  else { w.switchedOn = true; w.onAt = Date.now() - w.t0; w.notes.push('the switch was already ON when the window opened'); }
  if (TRIGGER === 'send' && !o.noSend) await sendPress(w, page);
}
async function sendPress(w, page) {
  await page.evaluate(() => { const day = new Date().toISOString().slice(0, 10); if (!B.run) B.run = { runId: `run-${day}-keep`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; });
  try { await page.click('#cuDlg .cuFile .cuM[data-m="gold"]', { timeout: 3000 }); } catch (_) {}
  try { await page.click('#cuDlg [data-send]', { timeout: 5000 }); } catch (e) { w.notes.push('Send to Sheet could not be pressed: ' + String(e.message).split('\n')[0].slice(0, 80)); }
}
/** a person who opens the order's window again (a card press), as one does after being away. */
async function openWindow(w, rid, page = w.page) {
  await page.evaluate(rid => { const it = Review.customItemFor(Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith(rid + '_'))); CustomSheet.open(it, {}); }, rid);
  await page.waitForSelector('#cuDlg[open]'); await page.waitForTimeout(300);
}
const closeWindow = (w, page = w.page) => page.evaluate(() => { try { CustomSheet.shut(); } catch (_) {} const x = document.querySelector('#cuDlg [data-x]'); if (document.getElementById('cuDlg') && document.getElementById('cuDlg').open && x) x.click(); }).catch(() => {});
const flipSwitch = (w, page = w.page) => page.evaluate(() => { const s = document.getElementById('cuKeepSwitch'); if (s) s.click(); }).catch(() => {});
/** the password, given the way a person gives it: the strip's field when it is there, else the lock itself. Returns how it was given. */
async function givePassword(w, page = w.page) {
  try {
    const shown = await page.evaluate(() => { const i = document.getElementById('cuKeepPw'); return !!(i && i.getClientRects().length && !i.closest('[hidden]')); });
    if (shown) { await page.fill('#cuKeepPw', REPO_TEST.password); await page.click('#cuKeep [data-k-unlock]'); await page.waitForTimeout(600); return 'strip'; }
    // the window is closed: a card press (Save now / Retry) opens it where the field is
    const linkBtn = await page.$('[data-cu-keep-retry]'); if (linkBtn) { await linkBtn.click(); await page.waitForTimeout(500); const again = await page.evaluate(() => { const i = document.getElementById('cuKeepPw'); return !!(i && i.getClientRects().length); }); if (again) { await page.fill('#cuKeepPw', REPO_TEST.password); await page.click('#cuKeep [data-k-unlock]'); await page.waitForTimeout(600); return 'card-retry+strip'; } }
  } catch (e) { w.notes.push('strip password failed: ' + String(e.message).split('\n')[0].slice(0, 80)); }
  const ok = await page.evaluate(async pw => { try { const r = await CharmNestRepoLock.submitInline('upload', pw, { target: 'chaos' }); return !!(r && (r.ok || r.token)); } catch (_) { return false; } }, REPO_TEST.password).catch(() => false);
  return ok ? 'lock-direct' : 'failed';
}
/** reload the page the way a person does (F5): the old page is marked so that nothing mistakes it for the new one. Returns once the new document is committed. */
async function hardReload(w, page = w.page) {
  await page.evaluate(() => { window.__old = 1; }).catch(() => {});
  await Promise.race([page.reload({ waitUntil: 'commit' }).catch(() => {}), sleep(4000)]);
}
/** after a reload: the person opens the app and looks at Review (nothing else: no window, no press). */
async function reopenApp(w, page = w.page) {
  await page.waitForFunction(() => !window.__old && document.readyState === 'complete' && window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
  await seedOrders(page, w.orders); await page.click('#reviewView .egTab[data-k="customOrder"]'); await page.waitForSelector(card(w.orders[0].receiptId));
}

/* ═══════════════════════════ what the world looks like ═══════════════════════════ */
function snapshot(w) {
  const st = w.srv.st, keys = coll => [...st.docs.keys()].filter(k => k.startsWith(coll + '/')).map(k => k.slice(coll.length + 1));
  return { index: keys('Charm_Master_Index'), files: keys('Charm_Master_Files'), saves: keys('Charm_Custom_Saves'), aliases: keys('Charm_Sku_Aliases'), blobs: [...st.blobs.keys()].filter(k => !/^charmnest\/(sandbox\/)?custom\/\d+\//.test(k)), docsAll: st.docs.size };
}
const added = (a, b) => b.filter(x => !a.includes(x));
async function uiText(w, page = w.page) {
  return page.evaluate(rid => {
    const vis = n => !!n && n.getClientRects().length > 0 && !n.closest('[hidden]');
    const out = { strip: '', card: '', spinners: 0, dialogs: [], toasts: (window.__toasts || []).slice(-5), errs: (window.__errs || []).slice() };
    const h = document.getElementById('cuKeep'), d = document.getElementById('cuDlg');
    if (d && d.open && h && !h.hidden) { out.strip = [...h.querySelectorAll('.cuKeepLine b, .cuKeepLine .sub, .cuKeepTx, .cuKr .cuKd, .cuKeepMsg')].filter(vis).map(n => n.textContent.trim()).filter(Boolean).join(' | '); out.spinners += [...h.querySelectorAll('.spin')].filter(vis).length; out.spinWhere = (out.spinWhere || []).concat([...h.querySelectorAll('.spin')].filter(vis).map(n => 'strip:' + ((n.closest('.cuKr, .cuKeepLine, .cuKeepNote') || n.parentElement || {}).textContent || '').trim().slice(0, 50))); }
    if (d && d.open && h && !h.hidden && !out.strip) out.strip = (h.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 300);
    const c = document.querySelector('#rvList .reviewListRow[data-rid="' + rid + '"]');
    if (c) { out.card = [...c.querySelectorAll('.cuKeepNote, .cuKeepLine, [data-keep-state]')].filter(vis).map(n => n.textContent.trim()).filter(Boolean).join(' | '); out.spinners += [...c.querySelectorAll('.cuKeepNote .spin')].filter(vis).length; out.spinWhere = (out.spinWhere || []).concat([...c.querySelectorAll('.cuKeepNote .spin')].filter(vis).map(n => 'card:' + ((n.closest('.cuKeepNote') || {}).textContent || '').trim().slice(0, 50))); }
    out.dialogs = [...document.querySelectorAll('dialog[open]')].map(x => x.id || x.className).filter(x => x !== 'cuDlg');
    out.win = !!(d && d.open); out.phase = h ? h.dataset.phase || '' : '';
    return out;
  }, w.orders[0].receiptId).catch(e => ({ strip: '', card: '', spinners: 0, dialogs: [], toasts: [], errs: [], error: String(e.message).slice(0, 80) }));
}
const pageErrors = async (w) => { const e = [...w.errors, ...w.consoleErrors]; for (const p of w.pages) { if (p.isClosed()) continue; try { e.push(...(await p.evaluate(() => (window.__errs || []).slice()))); } catch (_) {} } return e; };

/** The geometry hash of a file the way the Designs window reads it (CustomSheet.read), so the saved design can be compared with the dropped file. */
async function geometryHash(page, name, b64) {
  return page.evaluate(async ({ name, b64 }) => {
    const P = CharmNestPDF, bytes = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
    const parsed = await P.parseSource(bytes, name), g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: +S.settings.minPt || 6 });
    for (const m of g.orphans) { if (!m.bbox) continue; const c = g.charms.find(c => { const b = c.bbox, pad = 2; return m.bbox[0] >= b[0] - pad && m.bbox[1] >= b[1] - pad && m.bbox[2] <= b[2] + pad && m.bbox[3] <= b[3] + pad; }); if (c && !c.members.includes(m)) c.members.push(m); }
    for (const c of g.charms) P.integrateRings(c);
    await P.buildSilhouettes(parsed, g.charms, +S.settings.silhouetteRes || 6);
    const c = g.charms.filter(c => !c.open)[0]; return c ? c.hash : null;
  }, { name, b64 });
}
const hashCache = {};
async function expectHash(w, name) { if (hashCache[name]) return hashCache[name]; const p = await w.newPage(); try { await p.goto(`${w.srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' }); await p.waitForFunction(() => window.CharmNestPDF && typeof S !== 'undefined' && S.settings, null, { timeout: 60000 }); hashCache[name] = await geometryHash(p, name, fixture(name).toString('base64')); } finally { await p.close().catch(() => {}); } return hashCache[name]; }

/* ═══════════════════════════ waiting for quiescence ═══════════════════════════ */
/** wait until the world has been still (no call in flight, the server's records and the page's line unchanged) for `calm` ms, or `max` ms pass. */
async function quiesce(w, { calm = 2500, max = MAXMS, until = null, page = w.page, stall = STALL } = {}) {
  const t0 = Date.now(); let last = '', since = Date.now(), doneAt = null;
  for (;;) {
    const s = snapshot(w); const ui = page && !page.isClosed() ? await uiText(w, page) : {};
    const rel = w.log.filter(c => c.step !== 'other' && c.step !== 'ping').length;
    const sig = JSON.stringify([rel, w.inflight, s.index.length, s.blobs.length, s.saves.length, s.aliases.length, s.files.length, ui.strip, ui.card, ui.phase, ui.spinners]);
    if (sig !== last) { last = sig; since = Date.now(); }
    const ok = until ? await until().catch(() => false) : true;
    if (ok && w.inflight === 0 && Date.now() - since >= calm) return { quiet: true, ms: Date.now() - t0 };
    if (!ok && w.inflight === 0 && Date.now() - since >= stall) return { quiet: false, ms: Date.now() - t0, stuck: 'nothing happens any more and the save is not done (idle ' + Math.round((Date.now() - since) / 1000) + ' s)' };
    if (Date.now() - t0 > max) return { quiet: false, ms: Date.now() - t0, stuck: ok ? 'never stopped changing' : 'goal not reached' };
    await sleep(250);
  }
}
/** the goal "saved and linked": the save record says linked. */
const linkedGoal = (w, rid) => async () => { const r = w.srv.st.docs.get('Charm_Custom_Saves/' + rid); return !!(r && r.sku && r.etsy && r.etsy.state === 'linked'); };

/* ═══════════════════════════ the final-state assertions ═══════════════════════════ */
const KNOWN_FAIL_TEXT = /(went wrong|error|failed|not saved|could not)/i;
/** server + page state for "saved": exactly one entry for the file's geometry hash, files and records consistent, no litter, no duplicate alias, Etsy once. */
async function checkSaved(w, ex) {
  const f = [], st = w.srv.st, snap = snapshot(w), base = w.base || snapshot({ srv: { st: { docs: new Map(), blobs: new Map() } } });
  const newIdx = added(base.index, snap.index), newBlobs = added(base.blobs, snap.blobs), newSaves = added(base.saves, snap.saves), newFiles = added(base.files, snap.files);
  const wantHash = ex.hash || await expectHash(w, ex.file || FILE_A), rid = ex.rid || w.orders[0].receiptId, listing = ex.listing || LISTING;
  const byHash = newIdx.map(k => ({ k, d: st.docs.get('Charm_Master_Index/' + k) })).filter(x => x.d && x.d.charmHash === wantHash);
  if (ex.entries == null) ex.entries = 1;
  if (byHash.length !== ex.entries) f.push(`repository entries for the dropped file's geometry: ${byHash.length} (${byHash.map(x => x.k).join(',') || 'none'}), wanted ${ex.entries}`);
  if (newIdx.length !== byHash.length) f.push(`entries in the repository that are not the dropped file: ${newIdx.filter(k => !byHash.some(x => x.k === k)).join(',')}`);
  const idx = byHash[0] && byHash[0].d, sku = idx && (idx.sku || byHash[0].k);
  if (idx) {
    // (KEEPOWNSKU, Paul 20 Oct 2026: "use the existing Etsy listing/order SKU": the design may be saved under the order's own SKU as the library keys it, whatever characters the shop gave it)
    const ownKey = (() => { try { return require(path.join(root, 'charm-nest-sku.js')).etsyKey(ex_rawSku(w)); } catch (_) { return ''; } })(), ownSaved = !!ownKey && sku === ownKey;
    if (!/^[A-Z0-9][A-Z0-9_-]{1,31}$/.test(sku) && !ownSaved) f.push('SKU not valid: ' + sku);
    for (const p of [idx.aiPath, idx.thumbPath]) if (!p || !st.blobs.has(p)) f.push('file missing from storage: ' + p);
    const ai = idx.aiPath && st.blobs.get(idx.aiPath); if (ai && (ai.buf.length < 200 || ai.buf.slice(0, 4).toString() !== '%PDF')) f.push('the saved .ai is not a real file');
    const png = idx.thumbPath && st.blobs.get(idx.thumbPath); if (png && png.buf.slice(1, 4).toString() !== 'PNG') f.push('the picture is not a PNG');
    if (idx.masterHash && !st.docs.has('Charm_Master_Files/' + idx.masterHash)) f.push('the file record is missing (Charm_Master_Files/' + idx.masterHash + ')');
    if (ai && ai.buf.length > 200 && ex.reread !== false) { try { const hs = await geometryHash(w.page, sku + '.ai', ai.buf.toString('base64')); if (hs !== wantHash) f.push('the saved ' + sku + '.ai read back is not the dropped drawing (geometry hash ' + hs + ' vs ' + wantHash + ')'); } catch (e) { w.notes.push('re-read of the saved file failed: ' + String(e.message).split('\n')[0].slice(0, 60)); } }
    if (idx.open) f.push('the entry says its outline is open');
    // no litter: every new file is the entry's own
    const own = new Set([idx.aiPath, idx.thumbPath]); const litter = newBlobs.filter(b => !own.has(b)); if (litter.length) f.push('uploaded files that are not the entry\'s: ' + litter.join(','));
    // the alias: one mapping to this SKU on the listing, no key twice (case/space look-alikes)
    const al = st.docs.get('Charm_Sku_Aliases/' + listing);
    if (!al) { if (!ex.aliasOptional && !ownSaved) f.push('no alias for listing ' + listing); }   // (the order's own SKU needs no alias: the next order reads it directly)
    else if (ownSaved) { /* (its own SKU: an alias, when there is one, is the listing's old answer put right or another key) */ }
    else { const m = al.bySku || {}, to = Object.entries(m).filter(([, v]) => v === sku); const others = Object.entries(m).filter(([, v]) => v !== sku); if (to.length < 1 && !(al.sku === sku)) f.push('the alias does not point at ' + sku + ': ' + JSON.stringify(al).slice(0, 160)); if (to.length > 1) f.push('duplicate alias keys to ' + sku + ': ' + to.map(x => x[0]).join(' | ')); if (ex.noOtherAliases !== false && others.length) f.push('other aliases on the listing: ' + others.map(x => x[0] + '->' + x[1]).join(' | ')); }
    const rec = st.docs.get('Charm_Custom_Saves/' + rid);
    if (!rec) f.push('no save record for the order ' + rid); else { if (rec.sku !== sku) f.push('the order\'s save record names ' + rec.sku + ', the entry is ' + sku); if (ex.etsy !== false && !(rec.etsy && rec.etsy.state === 'linked')) f.push('Etsy not linked in the record: ' + JSON.stringify(rec.etsy)); }
    // the Etsy step: once, only after the repository
    if (ex.etsy !== false) { const wr = w.etsy.writes[listing] || 0; if (wr !== 1) f.push('the SKU was written on Etsy ' + wr + ' times (wanted 1)'); }
    if (w.etsy.beforeRepo) f.push('Etsy was asked ' + w.etsy.beforeRepo + ' time(s) before the repository step was saved');
    if (!ex.reuseOk && newSaves.length > (ex.saves || 1)) f.push('save records: ' + newSaves.join(','));
  }
  if (ex.assoc !== false && idx && !f.length) f.push(...await checkAssociation(w, sku, listing));
  return { f, sku };
}
/** a fresh page, nothing in its storage: the NEXT order of this listing (and another variation of it) is associated with the saved design by the real orders code. */
async function checkAssociation(w, sku, listing) {
  const f = []; const page = await w.newPage();
  try {
    const rid = '4199990001', rid2 = '4199990002', raw = ex_rawSku(w);
    const orders = [order(rid, listing, raw, TITLE), order(rid2, listing, raw, TITLE, [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Length', value: '18 Inches' }])];
    await page.goto(`${w.srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true && window.Master, null, { timeout: 60000 });
    const got = await page.evaluate(async ({ orders, sku }) => {
      await Orders.loadMaps(true); try { await Master.load(true); } catch (_) {}
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll();
      return orders.map(o => { const r = B.orders.byKey.get(CharmNestOrders.lineKey(o, o.lines[0])), sp = r && r.spec || {}; return { rid: o.receiptId, designSku: sp.designSku || null, own: !!sp.ownDesign, noDesign: !!sp.noDesign, special: sp.special ? sp.special.kind || sp.special.label || true : null, problems: (r && r.problems || []).map(p => p.kind), entry: !!(sp.designSku && Master.entryFor(sp.designSku)) }; });
    }, { orders, sku });
    for (const g of got) { if (g.designSku !== sku) f.push(`the next order ${g.rid} of listing ${listing} is not associated with ${sku}: ${JSON.stringify(g)}`); else if (g.special || g.problems.length) f.push(`the next order ${g.rid} is associated but still asks/special: ${JSON.stringify(g)}`); }
  } catch (e) { f.push('association check failed to run: ' + String(e.message).split('\n')[0].slice(0, 120)); }
  finally { await page.close().catch(() => {}); }
  return f;
}
const ex_rawSku = w => { const l = w.orders[0].lines[0]; return l.sku; };
/** the page: one calm truthful line, no spinner left, no unhandled error, no other dialog. */
async function checkPage(w, ex) {
  const f = [], before = await uiText(w); let ui = before;
  if (before.win) { await closeWindow(w); await sleep(1200); ui = await uiText(w); }   // (what the person sees once the window is put away: the card)
  let line = (before.strip + ' || ' + ui.card); if (!(before.strip + ui.card).trim() && ui.toasts.length) line += ' [toast] ' + ui.toasts.slice(-2).join(' / ');
  const errs = await pageErrors(w); if (errs.length) f.push('unhandled errors: ' + [...new Set(errs)].slice(0, 3).join(' ; '));
  if (ui.dialogs.length) f.push('a dialog is open: ' + ui.dialogs.join(','));
  if (ui.spinners) f.push('a spinner is still turning in the final state (' + ui.spinners + ' at ' + (ui.spinWhere || []).join(' ; ').slice(0, 140) + '): "' + line.slice(0, 100) + '"');
  if (ex.ui === 'linked') { if (!/Linked/i.test(line)) f.push('the card/window does not say it is linked: "' + line.slice(0, 160) + '"'); }
  else if (ex.ui === 'nothing') { if (/Linked|Saved to the repository|Saving/i.test(line.replace(/Nothing was saved to the repository/gi, '')))   // (the OFF switch's own true words, "Switched off. Nothing was saved to the repository.", claim nothing)
    f.push('the page claims a save that did not happen: "' + line.slice(0, 400) + '"'); }
  else if (ex.ui === 'waiting-password') { if (!/password/i.test(line)) f.push('the page does not say it waits for the password: "' + line.slice(0, 160) + '"'); }
  else if (ex.ui === 'waiting-connection') { if (!/connection|offline|online/i.test(line)) f.push('the page does not say it waits for the connection: "' + line.slice(0, 160) + '"'); }
  return { f, line };
}
/** nothing at all was written (the switch was off, a rehearsal, an unreadable file): no record, no file, no write call. */
function checkNothing(w, ex = {}) {
  const f = [], snap = snapshot(w), base = w.base;
  for (const k of ['index', 'files', 'saves', 'aliases', 'blobs']) { const a = added(base[k], snap[k]); if (a.length) f.push(`${k} written: ${a.slice(0, 3).join(',')}`); }
  const wr = w.log.filter(c => WRITES.has(c.step) && (ex.since == null || c.i > ex.since)); if (wr.length) f.push('write calls were made: ' + [...new Set(wr.map(c => c.fn + ':' + c.op))].join(','));
  if (Object.keys(w.etsy.writes).length) f.push('Etsy was written');
  return { f };
}
/** all or nothing: whatever is in the repository for this file is whole; no half state is left (records without files, files without records, alias without entry). */
function checkWhole(w, ex = {}) {
  const f = [], st = w.srv.st, snap = snapshot(w), base = w.base, newIdx = added(base.index, snap.index), newBlobs = added(base.blobs, snap.blobs), newSaves = added(base.saves, snap.saves), newAl = added(base.aliases, snap.aliases);
  for (const k of newIdx) { const d = st.docs.get('Charm_Master_Index/' + k); for (const p of [d.aiPath, d.thumbPath]) if (!st.blobs.has(p)) f.push('entry ' + k + ' without its file ' + p); }
  for (const b of newBlobs) { const own = newIdx.some(k => { const d = st.docs.get('Charm_Master_Index/' + k); return d.aiPath === b || d.thumbPath === b; }); if (!own) f.push('file without an entry (litter): ' + b); }
  for (const k of newSaves) { const r = st.docs.get('Charm_Custom_Saves/' + k); if (r.sku && !newIdx.includes(r.sku) && !base.index.includes(r.sku)) f.push('save record ' + k + ' names ' + r.sku + ' which is not in the repository'); }
  for (const k of newAl) { const a = st.docs.get('Charm_Sku_Aliases/' + k); const targets = Object.values(a.bySku || {}).concat(a.sku ? [a.sku] : []); for (const t of targets) if (!newIdx.includes(t) && !base.index.includes(t)) f.push('alias on ' + k + ' points at ' + t + ' which is not in the repository'); }
  return { f, newIdx };
}

/* ═══════════════════════════ scenarios ═══════════════════════════ */
const SC = [];
const scen = (name, props, fn) => { const s = { name, props, fn }; SC.push(s); return s; };
/** the common path: boot (done), drop, switch ON; the scenario's injection is armed between. Returns when the switch has been pressed. */
async function standard(w, o = {}) {
  await dropFile(w, w.orders[0].receiptId, o.file || FILE_A);
  if (o.preOn) await o.preOn(w);
  await switchOn(w);
}
/** common ending for "the save must complete by itself": wait for linked + calm, then assert. */
async function expectSaved(w, ex = {}) {
  const rid = ex.rid || w.orders[0].receiptId;
  const q = await quiesce(w, { until: ex.until || linkedGoal(w, rid), max: ex.max || MAXMS, stall: ex.stall || STALL });
  const verdict = []; if (!q.quiet) verdict.push(`not finished after ${Math.round(q.ms / 1000)} s: ${q.stuck}`);
  if (ex.reopen && !q.quiet) { /* the assisted level is reported by the scenario itself */ }
  const s = await checkSaved(w, Object.assign({ ui: 'linked' }, ex)); verdict.push(...s.f);
  const p = await checkPage(w, Object.assign({ ui: 'linked' }, ex)); verdict.push(...p.f);
  return { f: verdict, sku: s.sku, ms: q.ms, line: p.line };
}
async function expectNothing(w, ex = {}) {
  const q = await quiesce(w, { calm: ex.calm || 3500, max: 20000 });
  const n = checkNothing(w, ex), p = await checkPage(w, Object.assign({ ui: 'nothing' }, ex));
  return { f: [...n.f, ...p.f], line: p.line, ms: q.ms };
}

/* — the interruptions: damage(w, ctx) happens in the middle of the step; recover(w) is what the person (or time) does afterwards — */
const INT = {
  reload: { page: true, damage: async w => { await hardReload(w); }, recover: async w => { await sleep(500); await reopenApp(w); } },
  'close-window': { page: true, damage: async w => { await closeWindow(w); }, recover: async w => { await sleep(300); } },
  'other-tab': { page: true, damage: async w => { await closeWindow(w); await w.page.evaluate(() => { const t = [...document.querySelectorAll('#reviewView .egTab')].find(x => x.dataset.k && x.dataset.k !== 'customOrder'); if (t) t.click(); }).catch(() => {}); }, recover: async w => { await sleep(2500); } },
  'other-order': { page: true, orders2: true, damage: async w => { await closeWindow(w); await sleep(150); await openWindow(w, RID2).catch(() => {}); }, recover: async w => { await sleep(1500); await closeWindow(w); } },
  offline: { page: true, damage: async w => { w.offline = true; await w.ctx.setOffline(true); }, recover: async w => { await sleep(4500); w.offline = false; await w.ctx.setOffline(false); } },
  freeze: { page: true, damage: async w => { const c = await w.ctx.newCDPSession(w.page); w.cdp = c; await c.send('Page.setWebLifecycleState', { state: 'frozen' }).catch(() => {}); }, recover: async w => { await sleep(5000); try { await w.cdp.send('Page.setWebLifecycleState', { state: 'active' }); } catch (_) {} await sleep(500); } },
  'token-expiry': { page: true, damage: async w => { w.srv.st.repoStrict = true; }, recover: async w => { await sleep(2500); w.srv.st.repoStrict = true; const how = await givePassword(w); w.notes.push('password given via ' + how); await sleep(1500); w.srv.st.repoStrict = false; } },
  'switch-off': { page: true, whole: true, damage: async w => { await flipSwitch(w); }, recover: async w => { await sleep(300); } },
  'file-removed': { page: true, whole: true, damage: async w => { await w.page.evaluate(() => { const b = document.querySelector('#cuDlg .cuFile [data-rm]'); if (b) b.click(); }); }, recover: async w => { await sleep(300); } },
  'second-file': { page: true, second: true, damage: async w => { await dropRaw(w.page, '#cuDlg', FILE_B, fixture(FILE_B).toString('base64')); }, recover: async w => { await sleep(500); } },
  'same-file-twice': { page: true, damage: async w => { await dropRaw(w.page, '#cuDlg', FILE_A, fixture(FILE_A).toString('base64')); }, recover: async w => { await sleep(500); } },
  'double-click': { page: true, whole: true, damage: async w => { await w.page.evaluate(() => { const s = document.getElementById('cuKeepSwitch'); if (s) { s.click(); s.click(); } }); }, recover: async w => { await sleep(300); } }
};
const FAULT = {   // faults of the call itself (route steps only): the first call of the step suffers it, then it passes
  http500: { act: 'http500', times: 2 }, html: { act: 'html', times: 1 }, abort: { act: 'abort', times: 2 }, timeout: { act: 'timeout', ms: 6000, times: 1 }, slow: { act: 'slow', ms: 7000, times: 1 }, lost: { act: 'lost', times: 1 }, 'drop-after': { act: 'drop-after', times: 1 }
};
const PAGE_STEPS = ['drop-read', 'drop-hash', 'open', 'sku', 'build', 'upload', 'commit', 'etsy'];
const NET_STEPS = ['open', 'sku', 'upload', 'commit', 'etsy'];
const ENGINE_STEPS_FOR = { 'drop-read': 'a', 'drop-hash': 'a' };

// 0 · the baseline: nothing goes wrong
scen('baseline: drop, switch ON, saved and linked', ['P1', 'P5', 'P7'], async w => { await standard(w); return expectSaved(w); });
scen('baseline: switch never turned on -> nothing saved', ['P8'], async w => { await dropFile(w, RID1, FILE_A); await switchReady(w.page); await sleep(1500); return expectNothing(w); });
scen('baseline: switch ON then OFF before anything ran -> nothing saved', ['P4', 'P8'], async w => { await dropFile(w, RID1, FILE_A); await switchReady(w.page); w.faults.push({ step: 'sku', act: 'hold', key: 'never', times: 9 }); w.holds.never = new Promise(() => {}); await switchOn(w, { noSend: true }); await sleep(200); await flipSwitch(w); await sleep(300); return expectNothing(w); });
scen('window opens with the switch OFF every time (REVDROP): a second order after one was saved', ['P8'], async w => {
  await dropFile(w, RID1, FILE_A); await switchOn(w); await sleep(1500); await closeWindow(w); await sleep(400);
  const f = []; await dropFile(w, RID2, FILE_B, { sel: card(RID2) }); await switchReady(w.page); const on = await switchState(w.page); if (on === true) f.push('the window of another order opens with the switch ON');
  const ui = await uiText(w); return { f, line: ui.strip };
}).world = { orders: [order(RID1), order(RID2, '1800004822', 'OTHER LEAF', 'Custom Charm Two')] };
// 1 · every interruption at every step
for (const [iname, I] of Object.entries(INT)) {
  const steps = PAGE_STEPS.filter(s => !(I.second || I.whole) || !/^drop-/.test(s));
  for (const step of steps) {
    if (QUICK && !['upload', 'commit'].includes(step) && iname !== 'reload') continue;
    if (SAMPLE && !(step === 'commit' || (iname === 'reload' && ['drop-read', 'upload'].includes(step)))) continue;
    const sc = scen(`${iname} @ ${step}`, ['P1', 'P4'], null);
    sc.world = { orders: I.orders2 ? [order(RID1), order(RID2, '1800004822', 'OTHER LEAF', 'Custom Charm Two')] : undefined, locked: iname === 'token-expiry' ? false : undefined };
    sc.fn = async w => {
      w.inject = { step, damage: I.damage, nth: step === 'upload' ? 2 : 1 };
      if (/^drop-/.test(step)) {   // during the reading of the file, before the switch exists: the person looks again (the design is on the card, or drops it again), then saves
        await dropFile(w, RID1, FILE_A, { noWait: true }).catch(() => {});
        const t0 = Date.now(); while (!w.inject.fired && Date.now() - t0 < 10000) await sleep(200);
        await sleep(300); if (w.inject.fired) await I.recover(w);
        await sleep(800); await ensureDesign(w, RID1, FILE_A);
        await switchOn(w);
        const rr = await expectSaved(w); if (w.stuck) rr.f.unshift('after the reload the design stayed "reading" for ever (no error, no way on but removing it)'); return rr;
      }
      await dropFile(w, RID1, FILE_A);
      await switchOn(w);
      const t0 = Date.now(); while (!w.inject.fired && Date.now() - t0 < 12000) { await sleep(200); }
      if (!w.inject.fired) { w.notes.push('step not reached'); }
      if (w.inject.fired) await I.recover(w);
      if (I.whole) {   // switch off / file removed / double click: all-or-nothing, then the person turns it ON again and it completes
        await quiesce(w, { calm: 3500, max: 25000 });
        const whole = checkWhole(w); const pg = await checkPage(w, { ui: null }); const f = [...whole.f, ...pg.f];
        if (!w.inject.fired && NET_STEPS.includes(step)) f.push('the switch was ON and the save never reached "' + step + '" (it never started): a vacuous pass is not a pass');
        return { f, line: pg.line };
      }
      if (I.second) {
        await quiesce(w, { calm: 3500, max: MAXMS, until: linkedGoal(w, RID1) });
        const snap = snapshot(w), newIdx = added(w.base.index, snap.index); const hA = await expectHash(w, FILE_A), hB = await expectHash(w, FILE_B), f = [];
        const hashes = newIdx.map(k => w.srv.st.docs.get('Charm_Master_Index/' + k).charmHash);
        if (hashes.includes(hB)) f.push('the SECOND file was saved'); if (hashes.length > 1) f.push('more than one design saved: ' + newIdx.join(','));
        f.push(...checkWhole(w).f); const pg = await checkPage(w, { ui: null }); f.push(...pg.f);
        if (!hashes.length) f.push('the first file was not saved either, and nothing said why: "' + pg.line.slice(0, 120) + '"');
        if (!w.inject.fired && NET_STEPS.includes(step)) f.push('the save never reached "' + step + '" (it never started)');
        return { f, line: pg.line };
      }
      return expectSaved(w);
    };
  }
}
// 2 · faults of the call itself, at the steps that are calls
for (const [fname, F] of Object.entries(FAULT)) for (const step of NET_STEPS) {
  if (QUICK && !['commit', 'upload', 'etsy'].includes(step)) continue;
  if (SAMPLE && !['commit', 'etsy'].includes(step)) continue;
  if (SAMPLE && !['http500', 'lost', 'timeout'].includes(fname)) continue;
  scen(`server ${fname} @ ${step}`, ['P3', 'P6'], async w => {
    w.faults.push(Object.assign({ step, nth: step === 'upload' ? 1 : 1 }, F)); await standard(w); return expectSaved(w);
  });
}
scen('server 500 @ every call (3 in a row, any step), then fine', ['P3'], async w => { w.faults.push({ step: '*', act: 'http500', times: 3, nth: 0 }); await standard(w); return expectSaved(w); });
scen('Etsy refuses (needs re-authorisation): repository saved, one calm line', ['P7'], async w => {
  w.faults.push({ step: 'etsy', act: 'needs-scope', times: 99 }); await standard(w);
  const q = await quiesce(w, { until: async () => !!w.srv.st.docs.get('Charm_Custom_Saves/' + RID1), max: MAXMS, calm: 4000 });
  const s = await checkSaved(w, { etsy: false, ui: null }); const p = await checkPage(w, { ui: null }); const f = [...s.f, ...p.f];
  if (!/Etsy/i.test(p.line)) f.push('the page does not say that only Etsy is not done: "' + p.line.slice(0, 140) + '"'); return { f, line: p.line };
});
// 3 · the data is not ready
scen('cloud not signed in at load, signed in later', ['P3'], async w => {
  w.pingAct = '401'; await w.page.reload(); await w.page.waitForFunction(() => window.CN && window.CustomSheet, null, { timeout: 60000 });
  w.pingAct = null; await w.page.evaluate(() => { S.cloud.locked = false; return cloudProbe(true); }); await reopenApp(w);
  await standard(w); return expectSaved(w);
});
scen('repository locked at the switch, password given later (window stays open)', ['P3', 'P8'], async w => {
  await dropFile(w, RID1, FILE_A); await switchOn(w); await sleep(2500);
  const mid = await uiText(w); const f = []; if (!/password/i.test(mid.strip + mid.card)) f.push('while locked the page does not say it waits for the password: "' + (mid.strip + ' || ' + mid.card).slice(0, 140) + '"');
  const how = await givePassword(w); w.notes.push('password via ' + how); const r = await expectSaved(w); r.f.unshift(...f); return r;
}).world = { locked: true };
scen('repository locked, window closed, password given much later', ['P1', 'P3'], async w => {
  await dropFile(w, RID1, FILE_A); await switchOn(w); await sleep(1500); await closeWindow(w); await sleep(4000);
  const mid = await uiText(w); const f = []; if (!/password/i.test(mid.card)) f.push('the card does not say it waits for the password: "' + mid.card.slice(0, 140) + '"');
  const how = await givePassword(w); w.notes.push('password via ' + how); const r = await expectSaved(w); r.f.unshift(...f); return r;
}).world = { locked: true };
scen('repository locked, then reload, then password', ['P1', 'P3'], async w => {
  await dropFile(w, RID1, FILE_A); await switchOn(w); await sleep(1500); await hardReload(w); await sleep(500); await reopenApp(w); await sleep(2500);
  const how = await givePassword(w); w.notes.push('password via ' + how); return expectSaved(w);
}).world = { locked: true };
scen('switch pressed while the first check of the listing is still under way', ['P3'], async w => {
  w.holds.open = new Promise(r => { w.releaseOpen = r; }); w.faults.push({ step: 'open', act: 'hold', key: 'open', times: 1 });
  await dropFile(w, RID1, FILE_A, { noWait: false }).catch(() => {}); await sleep(300); const t = setTimeout(() => w.releaseOpen(), 4000); await switchOn(w, { ms: 8000 }).catch(e => w.notes.push('switch not available while checking: ' + String(e.message).split('\n')[0].slice(0, 60))); await sleep(5000); w.releaseOpen(); clearTimeout(t);
  if (!w.switchedOn) { await switchOn(w); }
  return expectSaved(w);
});
scen('the listing\'s SKU table loads late (variations listing)', ['P2', 'P3'], async w => {
  w.srv.st.put('Charm_Listing_Skus', LISTING, { at: Date.now(), n: 2, products: [{ id: '9100000001', sku: SKU_RAW.replace('&#39;', "'"), d: 0, pv: ['513:1234'] }, { id: '9100000002', sku: SKU_RAW.replace('&#39;', "'"), d: 0, pv: ['513:1235'] }], nm: [], pn: [] });
  await standard(w); return expectSaved(w);
});
scen('the listing\'s SKU list never loads (Etsy not answering): the save goes on after a short wait and the line stays true', ['P3', 'P7'], async w => { await standard(w); return expectSaved(w, { max: 60000, stall: 30000 }); }).world = { noTable: true };
// 4 · the sandbox, SKUs, files
scen('sandbox: rehearses, writes nothing', ['P8'], async w => {
  await dropFile(w, RID1, FILE_A); await switchOn(w); await sleep(1500); const q = await quiesce(w, { calm: 4000, max: 30000 });
  const n = checkNothing(w); const p = await checkPage(w, { ui: null }); const f = [...n.f, ...p.f];
  const txt = p.line; if (!/rehears|sandbox|nothing was saved|Nothing is saved/i.test(txt)) f.push('the sandbox line does not say it was a rehearsal: "' + txt.slice(0, 140) + '"'); return { f, line: txt };
}).world = { sandbox: true };
scen('SKU already taken by the SAME file (other listing): linked to the existing design, no second entry', ['P2', 'P5'], async w => {
  await dropFile(w, RID1, FILE_A); await switchOn(w); const a = await expectSaved(w); if (a.f.length) return a;
  const sku1 = a.sku; await closeWindow(w);
  // an order of ANOTHER listing, the same file
  const o2 = order(RID2, '1800004999', 'OTHER LEAF', 'Custom Charm Two'); w.orders = [o2]; const p2 = w.page; await seedOrders(p2, [o2]); await p2.click('#reviewView .egTab[data-k="customOrder"]'); await p2.waitForSelector(card(RID2));
  const before = snapshot(w); await dropFile(w, RID2, FILE_A); await switchOn(w);
  await quiesce(w, { until: linkedGoal(w, RID2), max: MAXMS });
  const snap = snapshot(w), f = []; const newIdx = added(before.index, snap.index); if (newIdx.length) f.push('a second entry was made for the same file: ' + newIdx.join(','));
  const newBlobs = added(before.blobs, snap.blobs); if (newBlobs.length) f.push('files were uploaded again: ' + newBlobs.join(','));
  const rec = w.srv.st.docs.get('Charm_Custom_Saves/' + RID2); if (!rec || rec.sku !== sku1) f.push('the second order is not linked to ' + sku1 + ': ' + JSON.stringify(rec && { sku: rec.sku, etsy: rec.etsy }));
  const al = w.srv.st.docs.get('Charm_Sku_Aliases/1800004999'); if (!al || !Object.values(al.bySku || {}).includes(sku1)) f.push('no alias from the other listing to ' + sku1);
  const p = await checkPage(w, { ui: null }); f.push(...p.f); return { f, line: p.line };
});
scen('SKU taken by ANOTHER design (appeared a moment before the save)', ['P2', 'P4'], async w => {
  await dropFile(w, RID1, FILE_A); await switchReady(w.page); const pre = await w.page.evaluate(() => (document.querySelector('#cuKeepP') || {}).textContent || ''); const sku = (/New SKU (\S+)/.exec(pre) || [])[1];
  if (!sku) return { f: ['no proposed SKU shown: "' + pre + '"'] };
  const st = w.srv.st; st.put('Charm_Master_Index', sku, { sku, charmHash: 'deadbeef'.repeat(8), masterHash: 'cafe'.repeat(16), aiPath: 'charmnest/master/' + sku + '.ai', thumbPath: 'charmnest/master/' + sku + '.png', widthPt: 50, heightPt: 50, areaPt2: 2000, members: 1, holes: 0, open: false, hashSource: 'index' }); st.blobs.set('charmnest/master/' + sku + '.ai', { buf: Buffer.from('%PDF-OTHER' + 'x'.repeat(300)), meta: { metadata: {} } }); st.blobs.set('charmnest/master/' + sku + '.png', { buf: Buffer.from('89504e470d0a1a0a', 'hex'), meta: { metadata: {} } });
  w.base = snapshot(w); const orig = st.docs.get('Charm_Master_Index/' + sku);
  await switchOn(w); const q = await quiesce(w, { until: linkedGoal(w, RID1), max: MAXMS });
  const f = []; const now = st.docs.get('Charm_Master_Index/' + sku); if (JSON.stringify(now) !== JSON.stringify(orig)) f.push('the other design\'s entry was changed'); if (st.blobs.get('charmnest/master/' + sku + '.ai').buf.toString().slice(0, 10) !== '%PDF-OTHER') f.push('the other design\'s file was overwritten');
  const hA = await expectHash(w, FILE_A); const mine = st.list('Charm_Master_Index').filter(e => e.charmHash === hA); if (mine.length !== 1) f.push('entries for the dropped file: ' + mine.length + ' (wanted 1 under a free SKU)'); else if (mine[0]._id === sku) f.push('saved over the taken SKU');
  const rec = st.docs.get('Charm_Custom_Saves/' + RID1); if (!rec || !(rec.etsy && rec.etsy.state === 'linked')) f.push('not linked: ' + JSON.stringify(rec && { sku: rec.sku, etsy: rec.etsy }));
  const p = await checkPage(w, { ui: 'linked' }); f.push(...p.f); return { f, line: p.line };
});
for (const [label, sku] of [['missing', ''], ['malformed (spaces, symbols)', 'a b/c?? !!'], ['lower case', 'acorn leaf 4821'], ['reserved family', 'CUSTOM_6673']]) {
  scen(`listing SKU ${label}`, ['P2'], async w => {
    await dropFile(w, RID1, FILE_A); await switchReady(w.page, 8000).catch(() => {}); const shown = await w.page.evaluate(() => { const h = document.getElementById('cuKeep'); return { hidden: !h || h.hidden, text: h ? h.innerText : '' }; });
    if (shown.hidden || !(await switchVisible(w.page))) { const q = await quiesce(w, { calm: 2500, max: 10000 }); const pg = await checkPage(w, { ui: null }); const n = checkNothing(w); return { f: [...pg.f, ...n.f], line: pg.line + ' [not offered: ' + shown.text.slice(0, 80) + ']' }; }
    await switchOn(w);   // (offered: it must end saved and linked, with a SKU of its own; the old listing SKU is no alias key when it is unusable)
    const r = await expectSaved(w, { assoc: false, noOtherAliases: false, aliasOptional: true }); return r;
  }).world = { orders: [order(RID1, LISTING, sku, TITLE)] };
}
// 5 · two of them at once
scen('two tabs, same order, same file: one entry', ['P4'], async w => {
  const p2 = await w.newPage(); await bootPage(w, p2, { skipReady: false });
  await dropFile(w, RID1, FILE_A); await dropFile(w, RID1, FILE_A, { page: p2 });
  w.faults.push({ step: 'commit', act: 'barrier', n: 2, times: 2 });
  await switchOn(w); await sleep(1200); await switchReady(p2, 20000); if (!(await switchState(p2))) await switchOn(w, { page: p2 }); else w.notes.push('tab 2 already shows the switch ON (synced)');
  const r = await expectSaved(w); return r;
});
scen('two orders of the same listing, same file, saving at once', ['P4'], async w => {
  const o2 = order(RID2); w.orders = [order(RID1), o2]; const p2 = await w.newPage(); await bootPage(w, p2, { orders: [o2] });
  await dropFile(w, RID1, FILE_A); await dropFile(w, RID2, FILE_A, { page: p2, sel: card(RID2) });
  w.faults.push({ step: 'commit', act: 'barrier', n: 2, times: 2 });
  await Promise.all([switchOn(w), switchOn(w, { page: p2 })]);
  const q = await quiesce(w, { until: async () => { const a = w.srv.st.docs.get('Charm_Custom_Saves/' + RID1), b = w.srv.st.docs.get('Charm_Custom_Saves/' + RID2); return !!(a && b && a.etsy && b.etsy && a.etsy.state === 'linked' && b.etsy.state === 'linked'); }, max: MAXMS });
  const s = await checkSaved(w, { rid: RID1, ui: null, saves: 2, reuseOk: true, assoc: true }); const f = [...s.f]; if (!q.quiet) f.push('not finished');
  const r2 = w.srv.st.docs.get('Charm_Custom_Saves/' + RID2); if (!r2 || r2.sku !== s.sku) f.push('the second order is not linked to ' + s.sku + ': ' + JSON.stringify(r2 && { sku: r2.sku, etsy: r2.etsy }));
  const pg = await checkPage(w, { ui: null }); f.push(...pg.f); return { f, line: pg.line };
});
scen('two orders of the same listing, DIFFERENT files: the first design keeps the listing', ['P2', 'P4'], async w => {
  const o2 = order(RID2); w.orders = [order(RID1), o2]; const p2 = await w.newPage(); await bootPage(w, p2, { orders: [o2] });
  await dropFile(w, RID1, FILE_A); await dropFile(w, RID2, FILE_B, { page: p2, sel: card(RID2) });
  await switchOn(w); await quiesce(w, { until: linkedGoal(w, RID1), max: MAXMS });
  const hA = await expectHash(w, FILE_A), idxA = w.srv.st.list('Charm_Master_Index').filter(e => e.charmHash === hA); const f = []; if (idxA.length !== 1) f.push('first design: ' + idxA.length + ' entries'); const skuA = idxA[0] && idxA[0]._id;
  await switchOn(w, { page: p2 }); await quiesce(w, { calm: 4000, max: MAXMS, page: p2 });
  const al = w.srv.st.docs.get('Charm_Sku_Aliases/' + LISTING); const aliasTargets = al ? Object.values(al.bySku || {}) : []; if (skuA && (aliasTargets.length !== 1 || aliasTargets[0] !== skuA)) f.push('the listing\'s alias was changed by the second design: ' + JSON.stringify(aliasTargets) + ' (first design ' + skuA + ')');
  const n = checkWhole(w); f.push(...n.f); const dlg = (await uiText(w, p2)).dialogs; if (dlg.length) f.push('a dialog opened: ' + dlg); const e = await pageErrors(w); if (e.length) f.push('errors: ' + e[0]);
  const ui2 = await uiText(w, p2); if (!(ui2.strip + ui2.card).trim()) f.push('the second order says nothing'); return { f, line: ui2.strip + ' || ' + ui2.card };
});
scen('the same file dropped twice (before the switch): one design, one entry', ['P4'], async w => {
  await dropFile(w, RID1, FILE_A); await dropRaw(w.page, '#cuDlg', FILE_A, fixture(FILE_A).toString('base64')); await sleep(1200);
  await switchReady(w.page, 8000).catch(() => {}); const ui = await uiText(w);
  if (!(await switchVisible(w.page))) { const f = []; const n = checkNothing(w); f.push(...n.f); if (!/one design|one at a time|same/i.test(ui.strip + ui.card)) f.push('two designs and no line saying why saving is not offered: "' + (ui.strip + ui.card).slice(0, 120) + '"'); return { f, line: ui.strip }; }
  await switchOn(w); return expectSaved(w);
});
scen('the order is cancelled / completed while saving', ['P4'], async w => {
  w.faults.push({ step: 'upload', act: 'slow', ms: 3000, times: 1 }); await standard(w);
  await sleep(500); await w.page.evaluate(() => { const k = Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith('4196630529_')); const r = B.orders.byKey.get(k); r.order.cancelled = true; try { B.maps.customDone = Object.assign({}, B.maps.customDone, { [k]: { completed: true, at: Date.now(), how: 'button', completedBy: 'Test Operator' } }); Orders.interpretAll(); Review.render(); } catch (_) {} }).catch(() => {});
  const q = await quiesce(w, { calm: 4000, max: MAXMS }); const f = checkWhole(w).f; const p = await checkPage(w, { ui: null }); f.push(...p.f); return { f, line: p.line };
});

/* ═══════════════════════════ the runner ═══════════════════════════ */
async function main() {
  const names = SC.map(s => s.name);
  if (process.env.LIST === '1') { console.log(names.join('\n')); console.log(names.length + ' scenarios'); return; }
  let list = SC.filter(s => !ONLY || ONLY.test(s.name));
  if (process.env.SHARD) { const [i, n] = process.env.SHARD.split('/').map(Number); list = list.filter((_, k) => k % n === i); }
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  console.log(`keep-chaos: ${list.length} scenarios, trigger=${TRIGGER}, max ${MAXMS / 1000}s per save`);
  for (const s of list) {
    const t0 = Date.now(); let w = null, res = { f: [] }, crash = null;
    try {
      w = await mkWorld(browser, Object.assign({}, s.world || {}));
      res = await Promise.race([s.fn(w), sleep(MAXMS + 90000).then(() => { throw new Error('scenario timed out'); })]) || { f: ['(scenario returned nothing)'] };
      try { const ints = await w.page.evaluate(() => (window.CustomSheet && CustomSheet.keepIntents ? CustomSheet.keepIntents().map(r => r.state + (r.wait ? ':' + r.wait : '') + (r.why ? '(' + String(r.why).slice(0, 40) + ')' : '')) : null)); if (ints && ints.length) w.notes.push('intents: ' + ints.join(' ; ')); } catch (_) {}
      if (w.notes.includes('step not reached')) res.note = (res.note ? res.note + '; ' : '') + 'step not reached';
      if (SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); await w.page.screenshot({ path: path.join(SHOTS, s.name.replace(/[^a-z0-9]+/gi, '_').slice(0, 80) + '.png') }).catch(() => {}); }
    } catch (e) { crash = String((e && e.message) || e).split('\n')[0].slice(0, 200); const where = String((e && e.stack) || '').split('\n').find(l => /keep-chaos\.cjs:\d+/.test(l)); res = { f: ['scenario crashed: ' + crash + (where ? ' @ ' + where.trim().replace(/^.*keep-chaos\.cjs:/, 'line ') : '')] };
      if (w) { try { const ui = await uiText(w), ent = await w.page.evaluate(() => Object.values(CustomSheet.entries()).map(e => e.files.map(f => f.name + ':' + f.state + (f.error ? ':' + f.error : '')).join(',')).join(';')); res.f.push('state at the crash: window=' + ui.win + ' phase=' + ui.phase + ' strip="' + String(ui.strip).slice(0, 120) + '" card="' + String(ui.card).slice(0, 80) + '" designs=[' + ent + ']'); } catch (_) {} } if (w && SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); await w.page.screenshot({ path: path.join(SHOTS, 'CRASH_' + s.name.replace(/[^a-z0-9]+/gi, '_').slice(0, 70) + '.png') }).catch(() => {}); } }
    const r = { name: s.name, props: s.props, pass: res.f.length === 0, f: res.f, line: res.line || '', notes: w ? w.notes : [], ms: Date.now() - t0, calls: w ? w.log.filter(c => c.switchedOn && c.step !== 'other' && c.step !== 'ping').map(c => c.step + (c.fault ? '!' + c.fault : '')).join(' ').slice(0, 160) : '' };
    results.push(r);
    console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${s.name}  (${Math.round(r.ms / 1000)}s)${r.pass ? '' : '\n      - ' + r.f.slice(0, 4).join('\n      - ')}${r.notes.length ? '\n      . ' + r.notes.slice(0, 3).join(' ; ') : ''}`);
    if (w) await closeWorld(w);
  }
  await browser.close();
  const pass = results.filter(r => r.pass).length;
  console.log('\n┌──────── keep-chaos results ────────');
  for (const r of results) console.log(`│ ${r.pass ? 'ok  ' : 'FAIL'} ${r.name}${r.pass ? '' : '  => ' + (r.f[0] || '')}`);
  console.log(`└ ${pass}/${results.length} scenarios green, ${results.length - pass} failing (trigger=${TRIGGER})`);
  if (process.env.OUT) fs.writeFileSync(process.env.OUT, JSON.stringify({ trigger: TRIGGER, at: new Date().toISOString(), results }, null, 1));
  process.exit(pass === results.length ? 0 : 1);
}
main().catch(e => { console.error(e && e.stack || e); process.exit(2); });
