// ITEM 14 (Paul 21 Nov 2026): a design save (a keep intent) begun on a staff computer in cloud mode "on" (a viewer) was finished only while its tab stayed open: after a reload nothing
// resumed it (keepKick / keepRepair returned early, the viewer's boot never kicked the keeper) and its design is only in this browser (IndexedDB cn.keep.v1), so the cloud's repair cannot
// finish it. Now a viewer resumes its OWN unfinished saves after a reload (the same keeper, PRESS ops only), says in words what it is finishing and when each is done, and closes as done
// (never runs again) a save the cloud already holds. Mode off is unchanged.
//   U  the keeper alone (node): deps.skip keeps an intent from resume() and arm(); cloudDone() ends a live intent as done "by the cloud" in words; CharmNestKeep.resumesOnViewer
//   V1 the REAL page over the stand-in site (bridge-server.cjs), the REAL engine: a save switched on and waiting for the password, the site switched to mode "on", the page reloaded (a viewer):
//      it says it is finishing that save, waits for the password, then finishes it (the design in the repository, Etsy asked once, no request refused), says it is done; no repair pass, no
//      adopted Etsy step from the viewer; reloaded again, nothing is written twice
//   V2 the same, but the cloud already holds the save (its record in Charm_Custom_Saves): after the reload the intent ends as done by the cloud, the engine never runs, it is said in words
//   V3 mode off: the reload resumes as before (keepKick "load", keepPending, keepRepair), the item-14 words are not said, a cloud record does not close the intent here
// Offline only: no Etsy (its step is a stand-in answer), no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item14.cjs
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
let failed = 0, n = 0;
const ok = (c, m, d) => { n++; if (!c) failed++; console.log((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + (typeof d === 'string' ? d : JSON.stringify(d)).slice(0, 1500))); };

/* ── U · the keeper alone ── */
async function partU() {
  const K = require(path.join(root, 'charm-nest-keep.js'));
  ok(K.resumesOnViewer === true, 'U0 CharmNestKeep.resumesOnViewer is true (the viewer\'s boot words read it)');
  const mem = {}, timers = [];
  let skipAll = true, made = 0;
  const kp = K.durable({
    store: { get: k => mem[k] || null, set: (k, v) => { mem[k] = v; } }, online: () => true, lock: { find: () => ({ t: 'x' }) },
    setTimeout: (f, ms) => { timers.push({ f, ms }); return timers.length; }, clearTimeout: () => {},
    resolve: async () => ({ state: 'ready', ctx: {} }), make: () => { made++; return { on() {}, run: async () => ({ done: 'linked' }), outcome: () => ({ done: 'linked', sku: 'LEAF_1' }) }; },
    skip: () => skipAll
  });
  const now = Date.now();
  mem['cn.keepIntents.v1'] = JSON.stringify({ v: 1, intents: {
    a: { v: 1, key: 'a', sandbox: false, rid: '4196631001', listingId: '1800001001', hash: 'aa', state: 'waiting', wait: 'password', at: now, upd: now, fails: 0, nextAt: 0 },
    b: { v: 1, key: 'b', sandbox: false, rid: '4196631002', listingId: '1800001002', hash: 'bb', state: 'waiting', wait: 'password', at: now, upd: now, fails: 0, nextAt: 0 } } });
  timers.length = 0; kp.resume('load'); await kp.idle();
  ok(made === 0 && timers.length === 0, 'U1 an intent deps.skip names is neither run by resume() nor armed', { made, timers: timers.length });
  const d = kp.cloudDone('a', { sku: 'leaf_1', etsy: false });
  const v = kp.describe(kp.get('a'));
  ok(d && d.state === 'done' && d.outcome === 'cloud' && d.repo === true && d.etsy === false && v.phase === 'linked' && /Saved to the repository by the cloud · leaf_1\. The cloud links it on Etsy/.test(v.line), 'U2 cloudDone ends a live intent as done by the cloud, in words', { d, v });
  ok(kp.cloudDone('a', {}) === null, 'U3 an intent already done is not closed twice');
  skipAll = false; kp.resume('viewer'); await kp.idle();
  ok(made === 1 && kp.get('b').state === 'done' && kp.get('a').outcome === 'cloud', 'U4 not skipped any more: the other intent runs once; the one the cloud holds is not run', { made, b: kp.get('b').state });
}

/* ── the real page (keep-durable-browser.cjs's world: the REAL keeper, the fake engine unless a scene asks for the real one) ── */
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 10, 24, 17) / 1000);
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const LEAF_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 2, 70, 1, 10, 0, 20, 0, 42, -0.62, 10, 13, 20, 0, 42, -0.62,
  0, 'CIRCLE', 8, 'CUT', 10, 3.2, 20, 0, 40, 0.9, 0, 'ENDSEC', 0, 'EOF');
const order = (rid, listing) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: String(rid) + '1', listingId: String(listing), sku: 'ACORN&#39;S LEAF', title: "Custom Charm - Acorn's Leaf, Personalized", quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
function fakeKeep() {
  const W = window.__kw = { calls: [] };
  const FAKE = {
    durable(deps) { return W.real.durable(deps); },
    make(ctx) {
      const L = [];
      return { on: f => { L.push(f); }, async open() { W.calls.push('open'); return { offered: true, sku: 'ACORN_LEAF_4821', group: 'all', aliasWorks: true, saved: null, locked: false, rehearsal: false, reuse: null }; },
        async run() { W.calls.push('run'); for (const f of L) try { f({ done: 'linked', sku: 'ACORN_LEAF_4821' }); } catch (_) {} return { done: 'linked' }; }, outcome: () => ({ done: 'linked', sku: 'ACORN_LEAF_4821' }), retry() { return this.run(); }, settled: () => Promise.resolve() };
    }
  };
  W.real = null; Object.defineProperty(window, 'CharmNestKeep', { configurable: true, get: () => (W.useReal ? W.real : FAKE), set: v => { W.real = v; } });
}

async function partPage() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('page parts skipped: no playwright-core (' + pwDir + ')'); return; }
  const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(exe)) { console.log('page parts skipped: no browser at ' + exe); return; }
  const { start, REPO_TEST } = require('./bridge-server.cjs');
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const mk = async () => {
    const srv = await start({ receipts: [], repoSeed: false });
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 }, timezoneId: 'UTC', locale: 'en-US' });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await ctx.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} });
    await ctx.addInitScript(fakeKeep);
    await ctx.addInitScript(() => {
      window.confirm = () => true; window.prompt = () => 'Test Operator'; window.alert = () => {};
      window.__toasts = []; window.addEventListener('DOMContentLoaded', () => { const t = window.toast; if (typeof t === 'function' && !t.__spy) { window.toast = function (m) { window.__toasts.push(String(m)); return t.apply(this, arguments); }; window.toast.__spy = true; } });
    });
    const page = await ctx.newPage(), errors = [], ops = [], refused = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => errors.push(String(e.message)));
    page.on('request', r => { if (r.method() === 'POST' && /\.netlify\/functions\/charmNest/.test(r.url())) { try { const b = r.postDataJSON() || {}; ops.push(b.op || ''); } catch (_) {} } });
    page.on('response', async r => { if (r.status() === 409 && /\.netlify\/functions\/charmNest/.test(r.url())) { let op = ''; try { op = (r.request().postDataJSON() || {}).op; } catch (_) {} refused.push(op); } });
    const loaded = () => page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' }); await loaded();
    return { srv, ctx, page, errors, ops, refused, loaded, close: async () => { await ctx.close(); await srv.close(); } };
  };
  const seed = (page, orders) => page.evaluate(async orders => {
    await Orders.loadMaps(true);
    for (const order of orders) for (const line of order.lines) {
      const key = CharmNestOrders.lineKey(order, line);
      const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row);
    }
    B.maps.customRead = Object.assign({}, B.maps.customRead || {}, Object.fromEntries(orders.flatMap(o => o.lines.map(l => [CharmNestOrders.lineKey(o, l), { kind: 'custom', confidence: 0.96, summary: 'the buyer\'s own design' }]))));
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
  }, orders);
  const startRun = page => page.evaluate(() => { const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-keep`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; });
  const dropDesign = async (page, rid) => {
    await page.evaluate(async ({ rid, dxf }) => {
      const key = Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith(rid + '_')), it = Review.customItemFor(key);
      if (!it) throw new Error('no custom card for ' + rid);
      const r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'x.dxf');
      CustomSheet.add(it, [new File([r.bytes], 'Leaf_Test.ai')]);
    }, { rid, dxf: LEAF_DXF });
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length >= 1, null, { timeout: 30000 });
    await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
  };
  const phase = (page, p, ms = 15000) => page.waitForFunction(p => { const h = document.getElementById('cuKeep'); return h && h.dataset.phase === p; }, p, { timeout: ms });
  const intents = page => page.evaluate(() => Object.values(JSON.parse(localStorage.getItem('cn.keepIntents.v1') || '{"intents":{}}').intents));
  const toasts = page => page.evaluate(() => (window.__toasts || []).slice());
  const runs = page => page.evaluate(() => window.__kw.calls.filter(c => c === 'run').length);
  const unlock = page => page.evaluate(async pw => (await CharmNestRepoLock.submitInline('upload', pw, { target: 'test' })).ok, REPO_TEST.password);
  const waitFor = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
  /** A save switched on and waiting for the password, the window shut (the page in mode off, as it was when the person pressed). */
  const begun = async (w, rid, listing) => {
    const ORD = [order(rid, listing)];
    await seed(w.page, ORD); await startRun(w.page); await w.page.click('#reviewView .egTab[data-k="customOrder"]');
    await dropDesign(w.page, String(rid)); await phase(w.page, 'off');
    await w.page.click('#cuKeepSwitch'); await w.page.click('#cuKeepAsk [data-k-yes]'); await phase(w.page, 'waiting');   // (NEWSKU: the switch asks first; Save and link)
    await w.page.evaluate(() => CustomSheet.shut()); await w.page.waitForTimeout(400);
    // (the order's designs are in the cloud before the reload, as the window saves them)
    await w.page.waitForFunction(() => Object.keys(CustomSheet.entries()).length > 0);
    return ORD;
  };
  const driveOn = (srv, mode) => { srv.st.put('Charm_Nest_Open', 'drive', { mode, runMode: 'manual', cloudSteps: [], flippedAt: Date.now() - 3600e3 }); for (const h of Object.values(srv.st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {} };
  const reload = async w => { w.ops.length = 0; w.refused.length = 0; await w.page.reload({ waitUntil: 'load' }); await w.loaded(); await w.page.waitForFunction(() => window.CNViewer && CNViewer.known(), null, { timeout: 40000 }); };

  try {
    /* ── V1 · mode on, the real engine: resumed after the reload, finished, said; nothing twice ── */
    { const w = await mk(); const { page, srv } = w; const rid = 4196631401, listing = 1800001401;
      try {
        await begun(w, rid, listing);
        const before = await intents(page);
        ok(before.length === 1 && before[0].state === 'waiting', 'V1 a save switched on and waiting (unfinished) before the reload', before.map(r => [r.state, r.wait]));
        driveOn(srv, 'on'); await reload(w);
        const viewer = await page.evaluate(() => ({ on: CNViewer.on(), frozen: Session.frozen(), run: !!B.run, resumes: CustomSheet.keepResumes() }));
        ok(viewer.on && viewer.frozen && !viewer.run && viewer.resumes === true, 'V1 the reloaded page is a viewer (mode "on", workspace frozen, no run held)', viewer);
        const said = await waitFor(async () => (await toasts(page)).find(t => /^Finishing 1 design save begun on this computer: order 4196631401 \(Leaf_Test\.ai\)/.test(t)), 20000);
        ok(!!said && /you are told when each is done/.test(said) && /repository password is needed first/.test(said), 'V1 the viewer says what it is finishing, that it goes on by itself, and that the password is needed: "' + (said || '').slice(0, 220) + '"', await toasts(page));
        const waiting = await intents(page);
        ok(waiting.length === 1 && waiting[0].state === 'waiting' && w.ops.includes('customSaveGet'), 'V1 the cloud was asked what it holds (customSaveGet); nothing there, so the save waits here (not run, not closed)', { st: waiting.map(r => [r.state, r.wait]), ops: [...new Set(w.ops)] });
        // the real engine, Etsy a stand-in answer (nothing leaves this machine)
        await page.evaluate(() => { window.__kw.useReal = true; window.__etsy = []; const real = window.api; window.api = async function (fn, body) { if (body && body.op === 'customEtsySkuWrite') { window.__etsy.push(body.rid); return { ok: true, state: 'linked' }; } return real.apply(this, arguments); }; });
        ok(await unlock(page), 'V1 the password is given on the viewer');
        const done = await waitFor(async () => { const r = (await intents(page))[0]; return r && r.state === 'done' ? r : null; }, 90000);
        const idx = srv.st.list('Charm_Master_Index').map(e => e._id);
        const etsy = await page.evaluate(() => window.__etsy.slice());
        ok(!!done && done.outcome !== 'cloud' && done.sku && idx.includes(done.sku) && etsy.length <= 1, 'V1 the viewer finished its own save after the reload: ' + (done && done.sku) + ' is in the repository (Etsy asked at most once)', { done: done && { state: done.state, outcome: done.outcome, sku: done.sku, why: done.why }, etsy, idx: idx.slice(0, 8), intents: done ? null : await intents(page) });
        ok(w.refused.length === 0, 'V1 no request of the save was refused in mode "on" (all PRESS)', w.refused);
        ok(!w.ops.includes('customSaveRepair') && !w.ops.includes('customSavePending'), 'V1 the viewer ran no repair pass and adopted no Etsy step (the cloud\'s)', [...new Set(w.ops)]);
        const fin = await waitFor(async () => (await toasts(page)).find(t => new RegExp(`^Order ${rid}: ${done && done.sku} saved to the repository and linked on Etsy`).test(t)), 15000);
        ok(!!fin, 'V1 the person is told when it is done: "' + (fin || '') + '"', await toasts(page));
        const nIdx = idx.length;
        await reload(w); await page.waitForTimeout(3000);
        const after = await intents(page);
        ok(srv.st.list('Charm_Master_Index').length === nIdx && after.length === 1 && after[0].state === 'done' && !(await toasts(page)).some(t => /^Finishing/.test(t)), 'V1 reloaded again: nothing is written twice, the save stays done, nothing more is "finishing"', { idx: srv.st.list('Charm_Master_Index').length, nIdx, after: after.map(r => r.state) });
        ok(w.errors.length === 0, 'V1 no page error', w.errors.slice(0, 2));
      } catch (e) { ok(false, 'V1 run', String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); if (process.env.DBG) console.log('DBG', JSON.stringify({ errors: w.errors, ops: w.ops, strip: await page.evaluate(() => { const h = document.getElementById('cuKeep'); return { phase: h && h.dataset.phase, text: h && h.innerText.slice(0, 400), intents: localStorage.getItem('cn.keepIntents.v1'), toasts: window.__toasts }; }) }).slice(0, 3000)); }
      await w.close(); }

    /* ── V2 · mode on, the cloud already holds the save: not done twice ── */
    { const w = await mk(); const { page, srv } = w; const rid = 4196631402, listing = 1800001402;
      try {
        await begun(w, rid, listing);
        srv.st.put('Charm_Custom_Saves', String(rid), { rid: String(rid), listingId: String(listing), sku: 'ACORN_LEAF_7001', phase: 'repo', etsy: { state: 'linked' }, at: Date.now(), by: 'Cloud' });
        driveOn(srv, 'on'); await reload(w);
        const done = await waitFor(async () => { const r = (await intents(page))[0]; return r && r.state === 'done' ? r : null; }, 20000);
        ok(!!done && done.outcome === 'cloud' && done.sku === 'ACORN_LEAF_7001' && done.etsy === true, 'V2 the save the cloud already holds ends here as done by the cloud', done || await intents(page));
        const said = await waitFor(async () => (await toasts(page)).find(t => new RegExp(`^Order ${rid}: Saved and linked by the cloud · ACORN_LEAF_7001\\. Already done by the cloud: not saved again on this computer`).test(t)), 10000);
        ok(!!said, 'V2 said in words: "' + (said || '') + '"', await toasts(page));
        ok(!(await toasts(page)).some(t => /^Finishing/.test(t)), 'V2 nothing is said to be finishing');
        ok(await unlock(page), 'V2 the password is given anyway');
        await page.waitForTimeout(2500);
        ok(await runs(page) === 0 && (await page.evaluate(() => window.__kw.calls.length)) === 0, 'V2 the save engine never ran: not done twice', await page.evaluate(() => window.__kw.calls));
        const rec = srv.st.doc('Charm_Custom_Saves', String(rid));
        ok(rec && rec.sku === 'ACORN_LEAF_7001' && rec.by === 'Cloud', 'V2 the cloud\'s record is untouched', rec);
        ok(w.errors.length === 0, 'V2 no page error', w.errors.slice(0, 2));
      } catch (e) { ok(false, 'V2 run', String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
      await w.close(); }

    /* ── V3 · mode off: unchanged ── */
    { const w = await mk(); const { page, srv } = w; const rid = 4196631403, listing = 1800001403;
      try {
        const ORD = await begun(w, rid, listing);
        srv.st.put('Charm_Custom_Saves', String(rid), { rid: String(rid), listingId: String(listing), sku: 'ACORN_LEAF_7002', phase: 'repo', etsy: { state: 'linked' }, at: Date.now(), by: 'Cloud' });
        driveOn(srv, 'off'); await reload(w);
        await seed(page, ORD); await startRun(page);
        const passes = await waitFor(async () => w.ops.includes('customSavePending') && w.ops.includes('customSaveRepair'), 15000);
        ok(!!passes && !(await page.evaluate(() => CNViewer.on())), 'V3 mode off: the page load runs the keeper\'s passes as before (customSavePending, customSaveRepair)', [...new Set(w.ops)]);
        const st = (await intents(page))[0];
        ok(st && st.state === 'waiting' && st.outcome !== 'cloud', 'V3 mode off: the intent is not closed by the cloud\'s record here (as before): it still waits', st && [st.state, st.wait, st.outcome]);
        ok(!(await toasts(page)).some(t => /^Finishing|Already done by the cloud/.test(t)), 'V3 mode off: none of the item-14 words');
        ok(await unlock(page), 'V3 the password is given');
        const done = await waitFor(async () => { const r = (await intents(page))[0]; return r && r.state === 'done' ? r : null; }, 30000);
        ok(!!done && done.outcome === 'linked' && await runs(page) === 1, 'V3 mode off: it completes by itself, once, as before', done && [done.state, done.outcome]);
        ok(w.errors.length === 0, 'V3 no page error', w.errors.slice(0, 2));
      } catch (e) { ok(false, 'V3 run', String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
      await w.close(); }
  } finally { await browser.close(); }
}

(async () => {
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  ok(/charm-nest-keep\.js\?v=[^"]*-k14(?:-\w+)*"/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-k14[^"]*"/.test(html), 'S0 charm-nest-1.html loads the changed keeper and bridge under new tokens (-k14)');
  await partU();
  if (!process.env.SKIP_PAGE) await partPage();
  console.log(failed ? `FAIL viewer-item14 (${failed} of ${n})` : `PASS viewer-item14 (${n} checks)`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL viewer-item14: ' + (e && e.stack || e)); process.exit(1); });
