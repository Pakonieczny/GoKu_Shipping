/* PUSH (Paul, 28 Oct 2026: "I want a change made on one computer to appear on the others at once, with no idle polling"; plan: the local-first design, 5.5 option a):
   charm-nest-push.js and its wiring in charm-nest-sync-worker.js, on the real page over bridge-server.cjs. No network, no live record, no credential: the Firebase
   SDK is a STAND-IN (a fake `firebase` whose onSnapshot the test drives: tests/charm-nest/push.cjs FAKE_SDK) served for the two vendored files; nothing leaves the
   machine (every request to another host is refused and counted).
     A · the healthy path: the library is NOT a boot cost (not requested before the page is loaded and idle), only the leader tab loads it and listens, to its world's
         counter and nothing else; a sibling tab never loads it and is told; the Settings row reads "Live updates: push"
     B · a nudge (the counter moved) asks the feed within 300 ms; the first snapshot, a metadata-only and a from-cache one ask nothing (one catch-up question for the first)
     C · 2 minutes idle: no question at all (the polled feed asked about 20 a minute); the surfaces read nothing
     D · handover: the leader hidden, then closed: the next tab listens within a few seconds, the old one lets go, the others are told
     E · worlds apart: a sandbox tab listens to Sandbox_Charm_Nest_Push/counter, production to Charm_Nest_Push/counter; neither is asked for the other's nudge
     F · fallback, each failure: the library does not load, permission denied (403), another listener error, no answer in 20 s, offline for more than 20 s (the SDK
         answering from its cache): the polled pace is back at once, a question is asked for what the listener missed, the reason is said; a retry listens again
     G · the switches: window.CN_PUSH = "off", ?push=off|on (kept on this computer), RETIRED, automation's default; none loads the library, all say why
     H · it never reads anything but the counter and never writes: the stand-in refuses every other call and counts it; no call to push.js's source can write
   node tests/charm-nest/push.cjs   (through the slot queue: slot.sh timeout 900 node tests/charm-nest/push.cjs; SLOT_MAX=900) */
'use strict';
const path = require('path'), fs = require('fs'), assert = require('node:assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs'), { seed } = require('./laser-workflow.cjs');
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome';
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
const sleep = ms => new Promise(r => setTimeout(r, ms));
const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return Date.now() - t0; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(100); } };
const say = s => console.log('  ' + s);
const IDLE_SECS = +process.env.PUSH_IDLE_SECS || 120;

const COUNTER = 'Charm_Nest_Push/counter', SB_COUNTER = 'Sandbox_Charm_Nest_Push/counter';
/* the stand-in for firebase-app-compat.js (the firestore file is empty): the compat surface the page may use (initializeApp, apps, firestore, db.settings,
   collection().doc().onSnapshot) and every other call refused and logged, so a test can say "never anything but the counter" and "never a write" */
const FAKE_SDK = `(function () {
  var log = window.__fb = { apps: [], settings: [], collections: [], docs: [], listens: [], unsubs: 0, reads: [], writes: [], persistence: 0, auth: 0, other: [] };
  var live = [], docs = {};
  window.__fbHold = window.__fbHold || false;
  function snap(data, cache) { return { id: 'counter', metadata: { fromCache: !!cache, hasPendingWrites: false }, exists: data != null, data: function () { return data == null ? undefined : JSON.parse(JSON.stringify(data)); } }; }
  function refuse(kind, what) { log[kind].push(what); throw new Error('stand-in: ' + kind + ' ' + what + ' is not allowed'); }
  function docRef(path) {
    return {
      path: path,
      onSnapshot: function (a, b, c) {
        var fn = typeof a === 'function', l = { path: path, opts: fn ? {} : a, next: fn ? a : b, err: fn ? b : c, on: true };
        live.push(l); log.listens.push({ path: path, at: performance.now(), opts: l.opts });
        setTimeout(function () { if (l.on && !window.__fbHold) l.next(snap(docs[path] === undefined ? { n: 1 } : docs[path])); }, 30);
        return function () { if (l.on) { l.on = false; log.unsubs++; live = live.filter(function (x) { return x !== l; }); } };
      },
      get: function () { refuse('reads', path); }, set: function () { refuse('writes', path); }, update: function () { refuse('writes', path); }, delete: function () { refuse('writes', path); },
      collection: function (n) { refuse('reads', path + '/' + n); }
    };
  }
  var db = {
    settings: function (o) { log.settings.push(o); },
    collection: function (name) { log.collections.push(name); return { doc: function (id) { log.docs.push(name + '/' + id); return docRef(name + '/' + id); }, get: function () { refuse('reads', name); }, add: function () { refuse('writes', name); }, where: function () { refuse('reads', name + '?'); } }; },
    doc: function (p) { log.docs.push(p); return docRef(p); },
    batch: function () { refuse('writes', 'batch'); }, runTransaction: function () { refuse('writes', 'transaction'); },
    enablePersistence: function () { log.persistence++; return Promise.reject(new Error('stand-in: no persistence')); }, enableMultiTabIndexedDbPersistence: function () { log.persistence++; return Promise.reject(new Error('stand-in: no persistence')); },
    enableIndexedDbPersistence: function () { log.persistence++; return Promise.reject(new Error('stand-in: no persistence')); }, terminate: function () { log.other.push('terminate'); return Promise.resolve(); }
  };
  var firebase = { SDK_VERSION: 'stand-in', apps: [], initializeApp: function (cfg, name) { log.apps.push({ cfg: cfg, name: name }); var app = { name: name || '[DEFAULT]', options: cfg, firestore: function () { return db; } }; firebase.apps.push(app); return app; }, firestore: function () { return db; }, auth: function () { log.auth++; throw new Error('stand-in: no auth'); } };
  window.firebase = firebase;
  window.__fbDrive = {
    set: function (path, data) { docs[path] = data; live.slice().forEach(function (l) { if (l.path === path && l.on) l.next(snap(data)); }); },
    emit: function (path, data, cache) { live.slice().forEach(function (l) { if (l.path === path && l.on) l.next(snap(data, cache)); }); },
    fail: function (path, code, message) { live.slice().forEach(function (l) { if (l.path === path && l.on) { l.on = false; log.failedAt = performance.now(); live = live.filter(function (x) { return x !== l; }); if (l.err) l.err({ code: code, message: message || code }); } }); },
    listening: function (path) { return live.filter(function (l) { return l.on && (!path || l.path === path); }).length; }
  };
})();`;

(async () => {
  const srv = await start({ receipts: [] }); seed(srv); const { st } = srv;
  const handler = st.handlers.charmNestLibrary;
  let browser = null;
  const outside = [];   // every request to another host (refused): the stand-in never makes one
  try {
    const { chromium } = require(path.join(pwDir, 'playwright-core'));
    browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
    const settings = { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' };
    const calls = [], vendor = [];   // calls: the page's function calls; vendor: each request for a vendored library file
    /** One computer: a context with the stand-in (or not), the switches, and the page's own files unchanged. o: { sdk: 'fake' | 'abort' | 'slow' | 'real', html: fn(text), push: fn(text), pref: 'on' | 'off' | null } */
    const computer = async (o = {}) => {
      const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
      await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => { if (/fonts\.g/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' }); outside.push(r.request().url()); return r.abort(); });
      ctx.__sdk = o.sdk || 'fake';
      await ctx.route(/\/vendor\/firebase-(app|firestore)-compat-[\d.]+\.js/, async r => {
        const m = /firebase-(app|firestore)-compat/.exec(r.request().url()); vendor.push({ t: Date.now(), ctx, file: m[1], url: r.request().url(), mode: ctx.__sdk });
        if (ctx.__sdk === 'abort') return r.abort();
        if (ctx.__sdk === 'real') return r.continue();
        if (ctx.__sdk === 'slow') await sleep(1500);
        return r.fulfill({ status: 200, contentType: 'text/javascript', body: m[1] === 'app' ? FAKE_SDK : '/* stand-in: the firestore file has nothing more to add */' });
      });
      if (o.html) await ctx.route(/\/charm-nest-1\.html(\?|$)/, async r => { const resp = await r.fetch(); r.fulfill({ response: resp, body: o.html(await resp.text()) }); });
      if (o.push) await ctx.route(/\/charm-nest-push\.js(\?|$)/, async r => { const resp = await r.fetch(); r.fulfill({ response: resp, body: o.push(await resp.text()) }); });
      await ctx.addInitScript(({ s, pref, hold }) => {
        try { if (!sessionStorage.getItem('__seeded')) { if (!localStorage.getItem('cn.settings')) localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); if (pref) localStorage.setItem('cn.push', pref); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
        window.prompt = () => 'Test Operator'; window.__fbHold = !!hold;
        window.__calls = []; const f = window.fetch; window.fetch = function (u, init) { try { const b = init && typeof init.body === 'string' ? init.body : ''; const m = /\/\.netlify\/functions\/([\w-]+)/.exec(String(u && u.url || u)); if (m) window.__calls.push({ t: performance.now(), fn: m[1], op: (/"op":"(\w+)"/.exec(b) || [])[1] || '' }); } catch (_) {} return f.apply(this, arguments); };
      }, { s: settings, pref: o.pref === undefined ? 'on' : o.pref, hold: !!o.hold });
      return ctx;
    };
    const tab = async (ctx, tag, url) => {
      const page = await ctx.newPage(); page.setDefaultTimeout(60000);
      page.__tag = tag; page.__ctx = ctx;
      page.on('request', q => { const m = /\/\.netlify\/functions\/([\w-]+)/.exec(q.url()); if (m) calls.push({ t: Date.now(), tag, fn: m[1], op: (/"op":"(\w+)"/.exec(q.postData() || '') || [])[1] || '', body: q.postData() || '' }); });
      await page.goto(`${srv.sorterOrigin}/${url || 'charm-nest-1.html'}`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.CNSync && window.CNPush);
      return page;
    };
    const sync = p => p.evaluate(() => CNSync.state());
    const status = p => p.evaluate(() => CNSync.push.status());
    const fb = p => p.evaluate(() => window.__fb || null);
    const since = (t0, f) => calls.filter(x => x.t >= t0 && (!f || f(x)));
    const healthy = async (p, ms, what) => until(() => status(p).then(s => s.ok), ms || 20000, what || 'push is healthy');
    const hide = p => p.evaluate(() => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' }); Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); document.dispatchEvent(new Event('visibilitychange')); });
    const feedCalls = (t0, tag) => since(t0, x => x.tag === tag && x.op === 'getChanges').length;
    const logs = [];   // every stand-in log we saw, for H

    /* ── A · the healthy path ── */
    const A = await computer();
    const A1 = await tab(A, 'A1');
    let loadedAt = { loadEnd: 0 }; for (let i = 0; i < 20 && !loadedAt.loadEnd; i++) { loadedAt = await A1.evaluate(() => ({ loadEnd: performance.getEntriesByType('navigation')[0].loadEventEnd })); await sleep(50); }
    await until(() => sync(A1).then(s => s.leader), 15000, 'tab 1 leads');
    const tLead = Date.now();
    await healthy(A1, 20000, 'tab 1 listens and is healthy');
    const tHealthy = Date.now() - tLead;
    const entries = await A1.evaluate(() => performance.getEntriesByType('resource').filter(e => /vendor\/firebase-/.test(e.name)).map(e => ({ n: e.name.replace(/.*\//, ''), start: e.startTime })));
    assert(entries.length === 2 && entries.every(e => e.start > loadedAt.loadEnd), `the library is requested only after the page has loaded (load event at ${Math.round(loadedAt.loadEnd)} ms, library at ${entries.map(e => Math.round(e.start)).join('/')} ms)`);
    assert(entries.every(e => /\?v=[\w.-]+$/.test(e.n)), 'by a ?v= token (the service worker keeps it by that): ' + entries.map(e => e.n).join(' '));
    const f1 = await fb(A1); logs.push(f1);
    assert.deepEqual(f1.listens.map(l => l.path), [COUNTER], 'tab 1 listens to the real world\'s counter, once');
    assert.equal(f1.apps[0].name, 'cn-push', 'in an app of its own'); assert.equal(f1.apps[0].cfg.projectId, 'gokudatabase');
    assert.equal(f1.listens[0].opts.includeMetadataChanges, true, 'with metadata changes (the page sees from-cache)');
    let s1 = await status(A1);
    assert(s1.ok && s1.state === 'push' && s1.text === 'Live updates: push' && s1.leader, 'status: ' + JSON.stringify(s1));
    const A2 = await tab(A, 'A2');
    await until(() => status(A2).then(s => s.ok), 15000, 'tab 2 is told that push is on');
    const s2 = await status(A2);
    assert(s2.ok && !s2.leader && s2.text === 'Live updates: push', 'a sibling tab says the same, told on the channel: ' + JSON.stringify(s2));
    assert.equal(await A2.evaluate(() => typeof window.firebase), 'undefined', 'a sibling tab never loads the library');
    assert.equal(vendor.filter(v => v.ctx === A).length, 2, 'the computer asked for the library once (two files), by its leader');
    // Settings: the row next to "This computer's copy"
    const row = await A2.evaluate(() => { const h = document.getElementById('stLiveUpdates'), say = h && h.querySelector('[data-push-say]'); return { prev: h && h.previousElementSibling && h.previousElementSibling.id, text: say && say.textContent, label: h && h.querySelector('label').textContent }; });
    assert.deepEqual(row, { prev: 'stLocalCopy', text: 'Live updates: push', label: 'Live updates' }, 'Settings row: ' + JSON.stringify(row));
    say(`A · tab 1 (the leader) loaded the library ${Math.round(entries[0].start - loadedAt.loadEnd)} ms after the page loaded, listens to ${COUNTER} (one listener, ${tHealthy} ms after it led); tab 2 never loaded it and says "${s2.text}"; Settings row beside "This computer's copy": "${row.text}"`);

    /* ── B · a nudge asks the feed within 300 ms; echoes ask nothing ── */
    await sleep(2500);   // (the catch-up question and the first answer are over)
    const lat = async (page, path, data) => {
      const r = await page.evaluate(async ({ path, data }) => { const t = performance.now(); window.__fbDrive.set(path, data); const end = performance.now() + 3000; for (;;) { const c = window.__calls.find(x => x.t >= t && x.op === 'getChanges'); if (c) return { ms: c.t - t }; if (performance.now() > end) return { ms: null }; await new Promise(r => setTimeout(r, 5)); } }, { path, data });
      assert(r.ms != null, 'a nudge asks the feed'); return r.ms;
    };
    const lats = []; for (let i = 0; i < 3; i++) { lats.push(await lat(A1, COUNTER, { n: 10 + i, at: 'x' })); await sleep(1500); }
    assert(Math.max(...lats) <= 300, `a nudge → a feed check within 300 ms: ${lats.join('/')} ms`);
    const c0 = Date.now();
    await A1.evaluate(p => { window.__fbDrive.set(p, { n: 12, at: 'same' }); window.__fbDrive.emit(p, { n: 12 }, false); window.__fbDrive.emit(p, { n: 99 }, true); window.__fbDrive.emit(p, { n: 12 }, true); }, COUNTER);   // (the same n again, a metadata-only echo, and two from-cache snapshots, one of them with another n)
    await sleep(2500);
    assert.equal(feedCalls(c0, 'A1'), 0, 'the same n, a metadata-only echo and from-cache snapshots ask nothing: ' + feedCalls(c0, 'A1'));
    assert.equal((await status(A1)).ok, true, 'and a from-cache snapshot alone does not make it unhealthy (the offline clock is 20 s)');
    // a counter that goes away (a sandbox reset) is a change too
    const d0 = Date.now(); await A1.evaluate(p => window.__fbDrive.set(p, null), COUNTER); await until(() => feedCalls(d0, 'A1') > 0, 3000, 'the counter gone asks the feed');
    await A1.evaluate(p => window.__fbDrive.set(p, { n: 1 }), COUNTER); await sleep(1500);
    say(`B · counter moved → feed question ${lats.join(' / ')} ms later (budget 300); the same n, metadata-only and from-cache snapshots → 0 questions; the counter gone (a reset) → asked`);

    /* ── C · 2 minutes idle ── */
    await sleep(3000);
    const i0 = Date.now(), n0 = (await sync(A1)).polls;
    await sleep(IDLE_SECS * 1000);
    const idle = since(i0), q = (await sync(A1)).polls - n0, perOp = {}; for (const x of idle) perOp[x.tag + ' ' + x.op] = (perOp[x.tag + ' ' + x.op] || 0) + 1;
    // (the feed's own surfaces: nothing; cancelList is the auto-cancel check of sheetwin.js on its own 2-minute timer per tab, not part of the feed)
    const feeds = idle.filter(x => ['getChanges', 'getOrderPieces', 'customGet', 'laserStatus', 'timelineGet', 'listSheets', 'setList'].includes(x.op)), cancels = idle.filter(x => x.op === 'cancelList');
    say(`C · ${IDLE_SECS} s idle (two tabs, push healthy): ${q} feed question(s), ${feeds.length} feed or surface reads; the rest of the ${idle.length} calls: ${JSON.stringify(perOp)} (auto-cancel's own 2-minute check per tab); the polled feed asked ~20 a minute`);
    assert(q === 0 && feeds.length === 0, `no question, no read while idle: ${q} / ${feeds.length}`);
    assert(cancels.length <= 2 * Math.ceil(IDLE_SECS / 120 + 0.5) && idle.length === cancels.length, 'nothing else is asked: ' + JSON.stringify(perOp));
    assert((await sync(A1)).live && (await sync(A2)).live, 'and both tabs still hear the feed as live (the surfaces keep their own timers let go)');
    assert((await status(A1)).ok, 'still healthy after the idle window');

    /* ── D · handover: the leader hidden, then closed ── */
    const A3 = await tab(A, 'A3');
    await until(() => sync(A3).then(s => s.live), 15000, 'tab 3 hears the leader');
    const listening = async () => { const o = []; for (const p of [A1, A2, A3]) { if (p.isClosed()) continue; o.push([p.__tag, await p.evaluate(() => (window.__fbDrive ? window.__fbDrive.listening() : -1))]); } return o; };
    const th = Date.now(); await hide(A1);
    await until(async () => (await sync(A2)).leader || (await sync(A3)).leader, 8000, 'a visible tab leads once the leader is hidden');
    const L = (await sync(A2)).leader ? A2 : A3, other = L === A2 ? A3 : A2;
    await until(async () => (await status(L)).ok, 15000, 'the new leader listens');
    const tHand = Date.now() - th;
    assert(tHand < 8000, 're-attached within a few seconds: ' + tHand + ' ms');
    let lis = await listening(); assert.deepEqual(lis.filter(x => x[1] > 0).map(x => x[0]), [L.__tag], 'one listener on the computer, in the new leader: ' + JSON.stringify(lis));
    assert.equal((await fb(A1)).unsubs, 1, 'the old leader let go (unsubscribed)'); assert.equal((await status(A1)).leader, false);
    await until(() => status(other).then(s => s.ok), 8000, 'the other tab is told'); assert.equal(vendor.filter(v => v.ctx === A).length, 4, 'the new leader loaded the library once itself (its own page)');
    const tn = Date.now(), l1 = await lat(L, COUNTER, { n: 40, at: 'after the handover' }); assert(l1 <= 300, 'a nudge reaches the new leader\'s feed check: ' + l1 + ' ms');
    logs.push(await fb(L)); const tc = Date.now(); await L.close();
    await until(() => sync(other).then(s => s.leader), 8000, 'the last visible tab leads once the leader is closed');
    await until(() => status(other).then(s => s.ok), 15000, 'and listens'); const tClose = Date.now() - tc;
    assert(tClose < 8000, 'handover on close within a few seconds: ' + tClose + ' ms');
    const l2 = await lat(other, COUNTER, { n: 41, at: 'and again' }); assert(l2 <= 300, 'and its nudge is a feed check within 300 ms: ' + l2);
    logs.push(await fb(other));
    say(`D · the leader hidden → the next tab listens ${tHand} ms later (the old one unsubscribed, one listener on the computer), nudge → ${l1} ms; that one closed → the last listens ${tClose} ms later, nudge → ${l2} ms`);
    await A.close();

    /* ── E · worlds apart ── */
    {
      const W = await computer();
      const P = await tab(W, 'P1'); await healthy(P, 20000, 'production listens');
      await P.evaluate(() => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.sandbox = 'on'; localStorage.setItem('cn.settings', JSON.stringify(s)); });
      const S = await tab(W, 'S1'); await P.evaluate(() => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); delete s.sandbox; localStorage.setItem('cn.settings', JSON.stringify(s)); });
      assert.equal((await sync(S)).world, 'sandbox'); await healthy(S, 20000, 'the sandbox tab listens');
      const fp = await fb(P), fs = await fb(S); logs.push(fp, fs);
      assert.deepEqual(fp.listens.map(l => l.path), [COUNTER], 'production listens to Charm_Nest_Push/counter only: ' + JSON.stringify(fp.listens.map(l => l.path)));
      assert.deepEqual(fs.listens.map(l => l.path), [SB_COUNTER], 'the sandbox to Sandbox_Charm_Nest_Push/counter only: ' + JSON.stringify(fs.listens.map(l => l.path)));
      assert.deepEqual([...new Set(fs.collections)], ['Sandbox_Charm_Nest_Push'], 'and asks the sandbox\'s collection for nothing else');
      await sleep(2500);
      const w0 = Date.now(); const lp = await lat(P, COUNTER, { n: 50 });
      await sleep(1200); assert.equal(feedCalls(w0, 'S1'), 0, 'production\'s counter moving asks nothing in the sandbox tab');
      const w1 = Date.now(); const ls = await lat(S, SB_COUNTER, { n: 7 });
      await sleep(1200); assert.equal(feedCalls(w1, 'P1'), 0, 'the sandbox\'s counter moving asks nothing in the production tab');
      const mixed = since(0, x => x.tag === 'S1' && x.op === 'getChanges').filter(x => !/"sandbox":true/.test(x.body));
      assert.equal(mixed.length, 0, 'every question of the sandbox tab is the sandbox\'s');
      say(`E · worlds apart: production leads and listens to ${COUNTER}, the sandbox tab to ${SB_COUNTER}; a nudge in one asks the feed of that world only (${lp} / ${ls} ms), none in the other`);
      await W.close();
    }

    /* ── F · fallback on each failure, at once, with its reason ── */
    const polled = async (page, secs) => { const t = Date.now(); const end = t + secs * 1000; let i = 0; while (Date.now() < end) { await page.mouse.move(400 + (i++ % 5) * 20, 400); await sleep(1000); } return feedCalls(t, page.__tag); };
    const fall = async (name, setup, trigger, reasonRe) => {
      const C = await computer(setup && setup.ctx || {});
      const P = await tab(C, name);
      await trigger.wait(P);   // (the state it needs before the failure: healthy, or not yet)
      const t0 = Date.now(); await trigger.go(P, C);
      const tFall = await until(() => status(P).then(s => !s.ok && reasonRe.test(s.why)), trigger.within || 5000, name + ': polling with its reason');
      const s = await status(P);
      assert(s.state === 'polling' && s.text === `Live updates: polling (${s.why})` && reasonRe.test(s.why), name + ': says why: ' + s.text);
      const row = await P.evaluate(() => document.querySelector('#stLiveUpdates [data-push-say]').textContent); assert.equal(row, s.text, 'Settings says it too');
      let asked = null;
      if (trigger.healthyBefore) {   // (it was push, its next question was ten minutes away: a question at once, for what the listener missed, not at the polled pace's next beat)
        const count = () => since(t0 + (trigger.quiet || 0), x => x.tag === name && x.op === 'getChanges').length;
        await until(() => count() >= 1, 5000, name + ': a question at once for what the listener missed'); asked = count();
      }
      const n = await polled(P, 10);
      assert(n >= 2, `${name}: the polled pace is back (≥ 2 questions in 10 s of use): ${n}`);
      if (setup && setup.after) setup.after(C);   // (a library that did not load is there for its retry)
      return { C, P, tFall, asked, n, s };
    };
    const healthyWait = P => healthy(P, 20000, 'healthy first');
    const results = await Promise.all([
      fall('F-load', { ctx: { sdk: 'abort' }, after: C => { C.__sdk = 'fake'; } }, { wait: async P => { await until(async () => vendor.some(v => v.mode === 'abort' && v.ctx === P.__ctx), 15000, 'the library was asked for'); await sleep(500); }, go: async () => {}, within: 15000 }, /did not load/),
      fall('F-denied', {}, { wait: healthyWait, go: P => P.evaluate(p => window.__fbDrive.fail(p, 'permission-denied', '403 referrer'), COUNTER), healthyBefore: true }, /permission denied/),
      fall('F-error', {}, { wait: healthyWait, go: P => P.evaluate(p => window.__fbDrive.fail(p, 'internal', 'boom'), COUNTER), healthyBefore: true }, /listener error \(internal\)/),
      fall('F-first', { ctx: { hold: true } }, { wait: async P => { await until(() => fb(P).then(f => f && f.listens.length === 1), 15000, 'it listens'); }, go: async () => {}, within: 30000 }, /no answer from the live channel in 20 s/),
      fall('F-offline', {}, { wait: healthyWait, go: P => P.evaluate(p => window.__fbDrive.emit(p, null, true), COUNTER), within: 30000, healthyBefore: true, quiet: 15000 }, /offline for more than 20 s/)
    ]);
    for (const r of results) logs.push(await fb(r.P));
    say('F · ' + results.map(r => `${r.P.__tag}: polling in ${r.tFall} ms ("${r.s.why}")${r.asked == null ? '' : ', ' + r.asked + ' question(s) at once'}, ${r.n} in 10 s of use`).join(' · '));
    // recovery: from-cache → a server snapshot is push again (and a question for what was missed); a failed listener tries again after its wait
    {
      const R = results.find(r => r.P.__tag === 'F-offline'), t = Date.now();
      await R.P.evaluate(p => window.__fbDrive.set(p, { n: 77 }), COUNTER);
      await until(() => status(R.P).then(s => s.ok), 5000, 'a server snapshot after the offline spell is healthy again');
      await until(() => feedCalls(t, 'F-offline') >= 1, 5000, 'and asks once for what was missed');
      const F1 = results.find(r => r.P.__tag === 'F-first'); await F1.P.evaluate(p => window.__fbDrive.set(p, { n: 9 }), COUNTER); await until(() => status(F1.P).then(s => s.ok), 5000, 'an answer after the 20 s is healthy again');
      say('F · recovery: a server snapshot after the offline spell / the late first answer → push again, one question for what was missed');
      const D = results.find(r => r.P.__tag === 'F-denied'), E = results.find(r => r.P.__tag === 'F-error'), L = results.find(r => r.P.__tag === 'F-load');
      await until(async () => (await status(D.P)).ok && (await status(E.P)).ok && (await status(L.P)).ok, 45000, 'a failed listener and a library that did not load try again after their wait (30 s) and are push again');
      const fd = await fb(D.P), fe = await fb(E.P), reL = vendor.filter(v => v.ctx === L.C);
      assert(fd.listens.length === 2 && fe.listens.length === 2, 'it listened again');
      const waits = [fd.listens[1].at - fd.failedAt, fe.listens[1].at - fe.failedAt, reL[reL.length - 1].t - reL[0].t];
      assert(waits.every(w => w > 28000 && w < 33000), 'each tried again after its 30 s wait: ' + waits.map(Math.round).join(' / ') + ' ms');
      say(`F · a failure tries again after its wait: permission-denied, a listener error and a library that did not load listen again ${waits.map(w => (w / 1000).toFixed(1)).join(' / ')} s after, and are push again`);
      logs.push(await fb(D.P));
      for (const r of results) await r.C.close();
    }

    /* ── G · the switches ── */
    {
      const off = async (name, o, reRe, go) => {
        const t = vendor.length; const C = await computer(o); const P = await tab(C, name, go && go.url);
        if (go && go.after) await go.after(P);
        await sleep(6000);
        const s = await status(P);
        assert(!s.ok && s.state === 'polling' && reRe.test(s.why) && s.text === `Live updates: polling (${s.why})`, name + ': ' + JSON.stringify(s));
        assert.equal(vendor.length, t, name + ': the library is never asked for'); assert.equal(await P.evaluate(() => typeof window.firebase), 'undefined');
        const n = await polled(P, 8); assert(n >= 2, name + ': the polled pace: ' + n + ' questions in 8 s');
        return { C, P, s };
      };
      const g1 = await off('G-head', { html: h => h.replace('window.CN_PUSH="on"', 'window.CN_PUSH="off"') }, /every computer/);
      const g2 = await off('G-retired', { push: t => t.replace('const RETIRED = false;', 'const RETIRED = true;') }, /in the code/);
      const g3 = await off('G-auto', { pref: null }, /automation/);
      const g4 = await off('G-query', { pref: null }, /on this computer/, { url: 'charm-nest-1.html?push=off' });
      // ?push=off is kept on this computer: the next load, with no query, is still off; ?push=on turns it back on
      const keep = await g4.P.evaluate(() => localStorage.getItem('cn.push')); assert.equal(keep, 'off', '?push=off is kept (localStorage cn.push)');
      await g4.P.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' }); await g4.P.waitForFunction(() => window.CNPush && window.CNSync && CN.S.cloud.ok === true);
      await sleep(4000); assert((await status(g4.P)).state === 'polling' && /on this computer/.test((await status(g4.P)).why), 'still off on the next load');
      await g4.P.goto(`${srv.sorterOrigin}/charm-nest-1.html?push=on`, { waitUntil: 'load' }); await g4.P.waitForFunction(() => window.CNPush && window.CNSync && CN.S.cloud.ok === true);
      await healthy(g4.P, 25000, '?push=on turns it back on'); assert.equal(await g4.P.evaluate(() => localStorage.getItem('cn.push')), 'on');
      logs.push(await fb(g4.P));
      say('G · switches: CN_PUSH="off" in the head, RETIRED, automation\'s default and ?push=off each leave the library unrequested and say why ("' + [g1, g2, g3].map(g => g.s.why).join('" / "') + '"); ?push=off is kept across loads and ?push=on turns it back on');
      for (const g of [g1, g2, g3, g4]) await g.C.close();
    }

    /* ── boot: the library is no cost on the boot path ── */
    {
      const staticReqs = async (o) => {
        const C = await computer(o), reqs = []; const page = await C.newPage();
        page.on('request', q => { if (q.url().startsWith(srv.sorterOrigin) && !/\.netlify\//.test(q.url())) reqs.push(q.url().replace(srv.sorterOrigin + '/', '')); });
        await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
        const atLoad = reqs.slice(); await page.waitForFunction(() => window.CNSync); await sleep(500);
        await C.close(); return { atLoad, vendors: atLoad.filter(u => /vendor\/firebase/.test(u)) };
      };
      const without = await staticReqs({ html: h => h.replace(/<script src="charm-nest-push\.js[^>]*><\/script>/, '') }), withPush = await staticReqs({});
      const extra = withPush.atLoad.filter(u => !without.atLoad.includes(u));
      const gone = without.atLoad.filter(u => !withPush.atLoad.includes(u));
      assert.deepEqual(extra.map(u => u.split('?')[0]), ['charm-nest-push.js'], 'the boot makes exactly one more static request than before: the small file itself (' + extra.join(' ') + ')');
      assert.equal(withPush.vendors.length, 0, 'and none for the library');
      say(`G · boot: ${without.atLoad.length} static requests before, ${withPush.atLoad.length} now (+ charm-nest-push.js${gone.length ? '; a lazily loaded file of the other run: ' + gone.map(u => u.split('?')[0]).join(' ') : ''}, ${(fs.statSync(path.join(root, 'charm-nest-push.js')).size / 1024).toFixed(1)} KB); the library's two files (${((fs.statSync(path.join(root, 'vendor/firebase-app-compat-10.14.1.js')).size + fs.statSync(path.join(root, 'vendor/firebase-firestore-compat-10.14.1.js')).size) / 1024).toFixed(0)} KB) not among them`);
    }

    /* ── H · never anything but the counter, never a write ── */
    const all = logs.filter(Boolean);
    assert(all.length >= 8, 'stand-in logs collected: ' + all.length);
    const uniq = a => [...new Set(a)];
    assert.deepEqual(uniq(all.flatMap(l => l.docs)).sort(), [COUNTER, SB_COUNTER].sort(), 'the only documents ever asked for: ' + uniq(all.flatMap(l => l.docs)).join(', '));
    assert(uniq(all.flatMap(l => l.collections)).every(c => c === 'Charm_Nest_Push' || c === 'Sandbox_Charm_Nest_Push'), 'the only collections: ' + uniq(all.flatMap(l => l.collections)).join(', '));
    assert.equal(all.flatMap(l => l.reads).length, 0, 'no read (get, query, another document): ' + JSON.stringify(all.flatMap(l => l.reads)));
    assert.equal(all.flatMap(l => l.writes).length, 0, 'no write (set, update, add, delete, batch, transaction): ' + JSON.stringify(all.flatMap(l => l.writes)));
    assert.equal(all.reduce((n, l) => n + l.persistence + l.auth, 0), 0, 'no IndexedDB persistence, no auth');
    assert(all.every(l => l.settings.every(s => Object.keys(s).join() === 'experimentalAutoDetectLongPolling')), 'settings: nothing but the connection kind');
    assert.equal(all.flatMap(l => l.other).length, 0, 'nothing else called');
    assert.equal(outside.filter(u => !/fonts\.g/.test(u)).length, 0, 'no request left the machine: ' + outside.join(' '));
    const src = fs.readFileSync(path.join(root, 'charm-nest-push.js'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
    const writers = src.match(/\.(set|add|update|delete|commit|runTransaction|batch|setDoc|addDoc|updateDoc|deleteDoc|enablePersistence|enableIndexedDbPersistence|enableMultiTabIndexedDbPersistence|getAuth|signInAnonymously|signIn\w*|getDocs?)\s*\(|\bauth\s*\(/g);
    assert(!writers, 'charm-nest-push.js has no write (or read other than onSnapshot) call in its source: ' + JSON.stringify(writers));
    assert.equal(src.match(/\.onSnapshot\(/g).length, 1, 'one onSnapshot in it');
    say(`H · across ${all.length} pages: documents ${uniq(all.flatMap(l => l.docs)).join(' and ')} only; reads 0, writes 0, persistence 0, auth 0; no request left the machine; the file's source holds one onSnapshot and no write call`);
    console.log('push: OK');
  } catch (e) { console.error(e); process.exitCode = 1; }
  finally { if (browser) await browser.close().catch(() => {}); srv.close(); }
})();
