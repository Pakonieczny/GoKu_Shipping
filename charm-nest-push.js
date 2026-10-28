/* charm-nest-push.js — LIVE PUSH, the page's half (Paul, 28 Oct 2026: "I want a change made on one computer to appear on the others at once, with no idle polling";
   plan: the local-first design, section 5.5 option (a), phase 5; he published the Firestore rule for it).

   What the page did before: the one tab of a computer that leads (charm-nest-sync-worker.js) asked the cloud "what moved?" every 3 s while somebody used it, every 15 s
   or 60 s after: about 20 calls a minute for a computer in use, about 60 an hour for one left alone, and a change was seen at the next ask.

   What this does: that same tab, and only that one, listens to ONE tiny document of the cloud and asks the changes feed the moment it moves.
       Charm_Nest_Push/counter           the real world's       { n, at }
       Sandbox_Charm_Nest_Push/counter   the sandbox's          { n, at }
   The server raises n after every write that moved the feed (netlify/functions/_charmNestPush.js); the document holds nothing else (no order, no piece, no list of
   names: those stay behind charmNestLibrary's feed, asked for). Paul's published rule lets a browser read exactly this document, unauthenticated, and write nothing:
       match /Charm_Nest_Push/{docId} { allow read: if docId == 'counter'; allow write: if false; }   (Sandbox_Charm_Nest_Push the same)
   So this file reads that one document of ONE world (a page is one world: a sandbox page listens to the sandbox's counter, a production page to the real one,
   and the two never meet), never anything else, and never writes: it holds no write call at all (tests/charm-nest/push.cjs asserts both against a stand-in).
     · the library: Firebase's web SDK 10.14.1, compat build, vendored (vendor/firebase-app-compat-10.14.1.js, vendor/firebase-firestore-compat-10.14.1.js, the licence
       beside them: the site is cross-origin isolated, COEP require-corp, so no CDN). Loaded LAZILY: only after the page has loaded and is idle, and only in the tab
       that holds the leader lock (a sibling tab never loads it, a tab that has just been handed the lock loads it at once if it has not), never on the boot path.
       No auth SDK (the rule asks for none), the memory cache only (no IndexedDB persistence: it must not fight cn-local), a named app of its own ('cn-push');
     · onSnapshot on the counter: the first server snapshot names no change (one catch-up question to the feed, for what moved before the listener came), after it a
       different n (or the document gone: a sandbox reset) asks the feed at once (CNSync.push.nudge); a snapshot that only says "from cache", or that only changed
       metadata, is no news;
     · HEALTHY (listening, answered by the cloud, not offline for long) → the feed's own pace relaxes to a ten-minute safety question (charm-nest-sync-worker.js, LIVE
       PUSH); on ANY failure it is the polled pace again at once, with a question for what the listener missed, and the reason is said (CNSync.push.status(), Settings
       → Live updates): the library does not load (20 s) · permission denied or a 403 (the rule, or the API key's website restriction: a key restricted by HTTP
       referrer must list this site) · any other listener error · no answer in 20 s · offline for more than 20 s (the browser's own word, or the SDK answering from
       its cache). A failure tries again after 30 s, then 1, 2, 5, 10 minutes; the polling carries the page meanwhile;
     · a leader handover (the tab hidden, closed, or the next one shown): the old leader lets go (unsubscribe), the new one listens within a second or two and asks once;
       the other tabs are told on the sync worker's channel (CNSync.push.status() in every tab says the leader's state).

   SWITCHING IT OFF (any one is enough; each leaves the polled feed exactly as before this file)
     · every computer, from the page: window.CN_PUSH = "off" in charm-nest-1.html's <head> (the page is never cached: the next load of every computer is without it);
     · every computer, from this file: RETIRED = true below (the file's ?v= token must move with it);
     · one computer: ?push=off in the address (kept as localStorage cn.push = "off"; ?push=on undoes it), or CNPush.off() / CNPush.on() in the console.
   Automation (navigator.webdriver: the test suites) runs without it unless localStorage cn.push = "on": a test must not open a connection to the live cloud.

   CNPush (window; module.exports in tests)
     lead(world) / lose()      the sync worker's: this tab leads (listen) / does not any more (let go)
     info()                    { off, loaded, listening, healthy, why, snapshots, … } (CNSync.push.status() adds the rest)
     on() / off()              this computer (localStorage cn.push)
   Settings → "Live updates" (#stLiveUpdates in charm-nest-1.html): "Live updates: push" / "Live updates: polling (why)". */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(root);
  else root.CNPush = factory(root);
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const VERSION = '10.14.1';
  const RETIRED = false;                      // true: no computer listens (the polled feed only); move this file's ?v= token with it
  const FILES = ['vendor/firebase-app-compat-10.14.1.js', 'vendor/firebase-firestore-compat-10.14.1.js'];
  const FILE_TOKEN = '20261028-ps1';          // the vendored files' ?v= (the service worker keeps them by it)
  // (the project's public web config, as design-1.html has it: a web API key is not a secret; the rule and the key's own restrictions are the door)
  const CONFIG = { apiKey: 'AIzaSyBXhQLsYRa4i0bX1TPTRiElF9Zjy5vSHlA', authDomain: 'gokudatabase.firebaseapp.com', projectId: 'gokudatabase', appId: '1:1078662308113:web:41df0e5d229ff2af7a6cb0' };
  const APP = 'cn-push', COLL = 'Charm_Nest_Push', DOC = 'counter';
  const T = { idle: 1500, load: 20000, first: 20000, offline: 20000, retry: [30000, 60000, 120000, 300000, 600000] };
  const doc = root.document || null;
  const now = () => Date.now();
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const me = doc && doc.currentScript;
  const ls = () => safe(() => root.localStorage, null);

  // ?push=off|on in the address: this computer, kept (as the service worker's ?sw=)
  safe(() => { const q = /[?&]push=(on|off)\b/.exec((root.location && root.location.search) || ''), s = ls(); if (q && s) s.setItem('cn.push', q[1]); });
  const pref = () => safe(() => (ls() && ls().getItem('cn.push')) || '', '');
  /** Whether this page may listen, and why not. */
  function sw() {
    if (RETIRED) return { on: false, why: 'switched off in the code (charm-nest-push.js)', kill: true };
    if (root.CN_PUSH === 'off') return { on: false, why: 'switched off for every computer (charm-nest-1.html)', kill: true };
    if (pref() === 'off') return { on: false, why: 'switched off on this computer (?push=on turns it back on)', kill: true };
    if (safe(() => root.navigator && root.navigator.webdriver, false) && pref() !== 'on') return { on: false, why: 'automation: off unless localStorage cn.push = "on"' };
    if (!doc || typeof root.addEventListener !== 'function') return { on: false, why: 'this browser has no page to listen from' };
    return { on: true, why: '' };
  }

  let world = 'production', leading = false, db = null, unsub = null, gen = 0, loadP = null, retries = 0;
  let attachedPrefix = '', attachedAt = 0, gotServer = false, last, healthy = false, why = 'starting', since = 0;
  let idleT = 0, firstT = 0, offT = 0, retryT = 0;
  const stats = { loads: 0, attaches: 0, detaches: 0, snapshots: 0, cached: 0, nudges: 0, catchups: 0, errors: 0, retries: 0, lastSnapshotAt: 0, lastNudgeAt: 0, lastError: '' };

  const ready = () => !!(root.firebase && typeof root.firebase.initializeApp === 'function' && typeof root.firebase.firestore === 'function');
  const urlOf = f => { let base = ''; try { base = (me && me.src) || (root.location && root.location.href) || ''; } catch (_) {} try { return new URL(f + '?v=' + FILE_TOKEN, base).href; } catch (_) { return f + '?v=' + FILE_TOKEN; } };
  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const el = doc.createElement('script'); let done = false, t = 0;
      const end = err => { if (done) return; done = true; clearTimeout(t); el.onload = el.onerror = null; if (err) { safe(() => el.remove()); reject(err); } else resolve(); };
      t = setTimeout(() => end(new Error('timed out')), T.load);
      el.async = true; el.src = src; el.onload = () => end(); el.onerror = () => end(new Error('did not load'));
      doc.head.appendChild(el);
    });
  }
  /** The two files, one after the other (the firestore one needs the app one first), once per page. */
  function load() {
    if (ready()) return Promise.resolve();
    if (loadP) return loadP;
    stats.loads++;
    loadP = (async () => { for (const f of FILES) await loadScript(urlOf(f)); if (!ready()) throw new Error('the library did not define firebase.firestore'); })().catch(e => { loadP = null; throw e; });
    return loadP;
  }
  /** The page's own Firestore: a named app, no auth, the memory cache (enablePersistence is never called), the connection that works through a proxy. */
  function firestore() {
    if (db) return db;
    const f = root.firebase; let app = null;
    try { if (typeof f.initializeApp === 'function') app = ((f.apps || []).find(a => a && a.name === APP)) || f.initializeApp(CONFIG, APP); } catch (_) { app = null; }
    const d = app && typeof app.firestore === 'function' ? app.firestore() : f.firestore();
    safe(() => d.settings({ experimentalAutoDetectLongPolling: true }));
    return (db = d);
  }

  const idle = fn => (typeof root.requestIdleCallback === 'function' ? root.requestIdleCallback(fn, { timeout: 2000 }) : setTimeout(fn, 100));
  const whenLoaded = fn => { if (!doc || doc.readyState === 'complete') fn(); else root.addEventListener('load', fn, { once: true }); };
  const offline = () => safe(() => !!(root.navigator && root.navigator.onLine === false), false);
  const sync = () => root.CNSync && root.CNSync.push ? root.CNSync.push : null;

  function setHealth(ok, why_, force) {
    const w = ok ? '' : String(why_ || 'unknown');
    if (!force && ok === healthy && w === why) return;
    healthy = ok; why = w; if (ok) { since = now(); retries = 0; }
    if (leading) { const s = sync(); if (s) safe(() => s.health(ok, w)); }
  }
  const catchUp = () => { stats.catchups++; const s = sync(); if (s) safe(() => s.nudge('catch-up')); };

  function startOffline() { if (offT || !unsub) return; offT = setTimeout(() => { offT = 0; if (leading && unsub) setHealth(false, 'offline for more than 20 s'); }, T.offline); }
  function stopOffline() { clearTimeout(offT); offT = 0; }
  function detach() {
    gen++; clearTimeout(firstT); firstT = 0; stopOffline();
    if (unsub) { const u = unsub; unsub = null; stats.detaches++; safe(() => u()); }
    gotServer = false;
  }
  const reasonOf = e => {
    const code = String((e && e.code) || '').replace(/^firestore\//, ''), msg = String((e && e.message) || e || '');
    if (code === 'permission-denied' || /\b403\b|permission[- ]denied|insufficient permissions/i.test(msg)) return 'permission denied (the cloud\'s rule, or the API key\'s website restriction)';
    if (code === 'unauthenticated') return 'the cloud asks for a sign-in';
    if (code === 'resource-exhausted') return 'the cloud\'s quota is used up';
    if (code === 'unavailable') return 'the cloud\'s live channel is unavailable';
    return 'listener error (' + (code || msg.slice(0, 60) || 'unknown') + ')';
  };
  function retryLater() {
    clearTimeout(retryT); retryT = 0; if (!leading) return;
    const wait = T.retry[Math.min(retries, T.retry.length - 1)]; retries++;
    retryT = setTimeout(() => { retryT = 0; stats.retries++; go(); }, wait);
  }

  function onSnap(my, snap) {
    if (my !== gen || !snap) return;
    const meta = snap.metadata || {}, exists = typeof snap.exists === 'function' ? !!snap.exists() : !!snap.exists;
    if (meta.fromCache) { stats.cached++; if (healthy) startOffline(); return; }   // (the SDK answering from what it holds because it cannot reach the cloud: no news, and the offline clock runs)
    stopOffline(); clearTimeout(firstT); firstT = 0;
    stats.snapshots++; stats.lastSnapshotAt = now();
    const d = exists ? safe(() => snap.data(), null) : null, v = d ? +d.n : NaN, n = exists ? (Number.isFinite(v) ? v : 0) : null;   // (a document that is gone, a sandbox reset: n null, and that is a change)
    if (!gotServer) { gotServer = true; last = n; setHealth(true); catchUp(); return; }   // (the first answer names no change: one question for what moved before the listener came)
    const back = !healthy; if (back) setHealth(true);
    if (n !== last) { last = n; stats.nudges++; stats.lastNudgeAt = now(); const s = sync(); if (s) safe(() => s.nudge('counter')); }
    else if (back) catchUp();   // (the connection came back with nothing new in the counter: a question anyway, for what the polling may not have seen)
  }
  function onError(my, e) {
    if (my !== gen) return;
    stats.errors++; stats.lastError = String((e && (e.code || e.message)) || e || '').slice(0, 120);
    detach();   // (a listener that failed is dead: it is made again, after a wait, below)
    setHealth(false, reasonOf(e)); retryLater();
  }
  function attach() {
    const prefix = world === 'sandbox' ? 'Sandbox_' : '';
    if (unsub && attachedPrefix === prefix) return;
    detach();
    let ref;
    try { ref = firestore().collection(prefix + COLL).doc(DOC); } catch (e) { stats.errors++; stats.lastError = String((e && e.message) || e).slice(0, 120); setHealth(false, 'the live-updates library failed to start (' + stats.lastError.slice(0, 60) + ')'); retryLater(); return; }
    const my = ++gen; attachedPrefix = prefix; attachedAt = now(); gotServer = false; last = undefined; stats.attaches++;
    try { unsub = ref.onSnapshot({ includeMetadataChanges: true }, s => onSnap(my, s), e => onError(my, e)); } catch (e) { onError(my, e); return; }
    firstT = setTimeout(() => { firstT = 0; if (my === gen && !gotServer) setHealth(false, 'no answer from the live channel in 20 s'); }, T.first);
    if (offline()) startOffline();
  }
  async function go() {
    if (!leading) return;
    const s = sw(); if (!s.on) { setHealth(false, s.why); return; }
    try { await load(); } catch (e) { stats.errors++; stats.lastError = String((e && e.message) || e).slice(0, 120); setHealth(false, 'the live-updates library did not load'); retryLater(); return; }
    if (!leading) return;
    attach();
  }
  function arm(ms) { clearTimeout(idleT); idleT = setTimeout(() => { idleT = 0; whenLoaded(() => idle(go)); }, ms); }

  /** The sync worker's: this tab is the leader of `w` now (a handover included): listen, after the page is idle. */
  function lead(w) {
    world = w === 'sandbox' ? 'sandbox' : 'production'; leading = true;
    const s = sw();
    if (!s.on) { setHealth(false, s.why, true); return; }
    setHealth(false, 'starting', true);
    arm(ready() ? 0 : T.idle);   // (a tab that has the library already, a handover, listens at once; the first one waits for the page to be idle)
  }
  /** This tab is not the leader any more (hidden, closed, or another was elected): let go of the connection. */
  function lose() {
    leading = false; clearTimeout(idleT); idleT = 0; clearTimeout(retryT); retryT = 0; detach(); healthy = false; why = 'this tab does not lead';
  }
  function info() {
    const s = sw();
    return { off: s.on ? '' : s.why, kill: !!s.kill, loaded: ready(), listening: !!unsub, healthy, since, prefix: attachedPrefix, attachedAt, n: last, retries, snapshots: stats.snapshots, cached: stats.cached, pushes: stats.nudges,
      catchups: stats.catchups, errors: stats.errors, lastError: stats.lastError, lastSnapshotAt: stats.lastSnapshotAt, lastNudgeAt: stats.lastNudgeAt, loads: stats.loads, attaches: stats.attaches, detaches: stats.detaches };
  }
  const api = {
    VERSION, RETIRED, FILES, lead, lose, info, switchState: sw,
    /** this computer: listening again (also under automation) */
    on() { safe(() => ls() && ls().setItem('cn.push', 'on')); if (leading) { setHealth(false, 'starting', true); arm(0); } return sw(); },
    /** this computer: polling only (the listener lets go at once) */
    off() { safe(() => ls() && ls().setItem('cn.push', 'off')); detach(); clearTimeout(idleT); idleT = 0; clearTimeout(retryT); retryT = 0; if (leading) setHealth(false, sw().why, true); return sw(); }
  };

  /* Settings → Live updates: the row (charm-nest-1.html's #stLiveUpdates), repainted whenever the state moves and when Settings opens */
  function settingsRow() {
    if (!doc) return;
    const mount = () => {
      const host = doc.getElementById('stLiveUpdates'); if (!host || host.dataset.push) return;
      host.dataset.push = '1';
      host.innerHTML = '<label>Live updates</label><div class="help" data-push-say role="status" aria-live="polite"></div>';
      const say = host.querySelector('[data-push-say]');
      say.title = 'This computer listens to one tiny counter in the cloud and asks what changed the moment it moves, so a change made on another computer shows at once; a safety check runs every 10 minutes. When it cannot, it asks every few seconds as before.';
      const paint = () => { const s = root.CNSync && root.CNSync.push ? root.CNSync.push.status() : null; say.textContent = s ? s.text : 'Live updates: polling (its file is not loaded)'; say.dataset.state = s ? s.state : 'polling'; };
      paint();
      root.addEventListener('cn:push', paint);
      const dlg = doc.getElementById('dlgSettings');
      if (dlg && typeof root.MutationObserver === 'function') new root.MutationObserver(() => { if (dlg.open) paint(); }).observe(dlg, { attributes: true, attributeFilter: ['open'] });
    };
    if (doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', mount, { once: true }); else mount();
  }

  if (root.CNSync && root.CNSync.push && typeof root.CNSync.push.attach === 'function') root.CNSync.push.attach(api);
  settingsRow();
  if (root.addEventListener) {
    root.addEventListener('offline', () => { if (leading && unsub && healthy) startOffline(); });
    root.addEventListener('online', () => { stopOffline(); });
  }
  return api;
});
