// station-fresh.js: a station page that runs an older deploy reloads itself ONCE, at a quiet moment, after somebody uses it.
// One focused offline test of the real file:
//   A (pure decisions, no browser): stale detection from two page texts (the ?v= tokens against what the page loaded, and the text hash
//     against the text it was loaded with), the quiet-moment rule (every blocker), once per build, hold/release with a time limit
//   B (the real script in jsdom, a fake fetch standing in for the CDN): one conditional request at load and none after that until the
//     station is used; a deploy is noticed on the next use and the page reloads ONCE, after the quiet time; a hold, a request in
//     flight, a focused field with unsent text, an open dialog and a new hidden frame each put the reload off until they end;
//     the same build is never reloaded for twice, the next build is; a failed check, a hidden tab, a tab that cannot keep its note
//     and an automated browser do nothing; the query string (a phone's pairing code) is never sent; a 4 s phone page waits 4 s
//   jsdom is optional (part B is skipped, and says so, without it):
//     NODE_PATH=/tmp/ps1-jsdom/node_modules node tests/stations/station-fresh.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const SRC = fs.readFileSync(path.join(root, 'station-fresh.js'), 'utf8');
const T = require(path.join(root, 'station-fresh.js'))._t;
let checks = 0;
const ok = (c, m) => { checks++; assert.ok(c, m); };
const eq = (a, b, m) => { checks++; assert.strictEqual(a, b, m); };

/* ───────────── A: pure decisions ───────────── */
const pageA = '<html><head><link rel="stylesheet" href="css/x.css?v=1"></head><body><script src="station-session.js?v=20261006-st3"></script>' +
              "<script src='station-activity.js?v=20261006-st2'></script><script src=\"lib/qr.js\"></script><script>var inline = 1;</script></body></html>";
const pageB = pageA.replace('station-session.js?v=20261006-st3', 'station-session.js?v=20261007-new');
const pageInline = pageA.replace('var inline = 1;', 'var inline = 2;');
const tk = T.pageTokens(pageA);
eq(JSON.stringify(tk), JSON.stringify({ 'css/x.css': ['1'], 'station-session.js': ['20261006-st3'], 'station-activity.js': ['20261006-st2'] }), 'tokens of script/link tags, either quote, none for a tag without ?v=');
eq(JSON.stringify(T.pageTokens('<script src="a.js?x=1&v=7#h"></script><script src="b.js?version=3"></script>')), JSON.stringify({ 'a.js': ['7'] }), 'v= among other parameters; version= is not v=');
ok(T.hashText(pageA) === T.hashText(pageA) && T.hashText(pageA) !== T.hashText(pageB) && T.hashText(pageA) !== T.hashText(pageInline), 'hash: same text same, any change different');
const fa = T.fingerprint(pageA), fb = T.fingerprint(pageB), fi = T.fingerprint(pageInline);
const running = T.runningTokens({ querySelectorAll: () => [{ getAttribute: n => ({ src: 'station-session.js?v=20261006-st3' })[n] || null }, { getAttribute: n => ({ href: 'css/x.css?v=1' })[n] || null }] });
eq(JSON.stringify(running), JSON.stringify({ 'station-session.js': ['20261006-st3'], 'css/x.css': ['1'] }), 'running tokens come from the tags the page really has');
ok(!T.isStale(fa, fa, running), 'the same page is not stale');
ok(T.isStale(fb, fa, running), 'a new token: stale (text and token)');
ok(T.isStale(fi, fa, running), 'an inline-only change (same tokens): stale by the text hash');
ok(T.isStale(fb, null, running), 'no baseline (page restored from a cache): the token against the running tag is enough');
ok(!T.isStale(fa, null, running), 'no baseline and the same tokens: not stale');
ok(!T.isStale(fb, null, T.runningTokens({ querySelectorAll: () => [] })), 'a tag the page never loaded is not a reason');
ok(!T.isStale(T.fingerprint(pageA + '<script src="station-session.js?v=zzz"></script>'), null, running), 'two tokens for one path, one of them running: not stale');
ok(!T.isStale(null, fa, running) && !T.isStale({ hash: '' }, fa, running), 'nothing fetched: never stale');

const base = { now: 100000, lastInputAt: 98000, quietMs: 1500, armMs: 120000, visible: true, online: true, oauth: false, holds: 0, writes: 0, frameUntil: 0, dirty: false, dialog: false, busy: false };
const why = o => T.quietReason(Object.assign({}, base, o));
eq(why({}), '', 'quiet: nothing in the way');
eq(why({ visible: false }), 'hidden', 'hidden tab');
eq(why({ online: false }), 'offline', 'offline');
eq(why({ lastInputAt: 0 }), 'nobody used it', 'nobody used the station');
eq(why({ lastInputAt: 99000 }), 'typing', 'a key or tap less than 1.5 s ago (a scanner burst)');
eq(why({ lastInputAt: 100000 - 1499 }), 'typing', '1.499 s is still too soon');
eq(why({ lastInputAt: 100000 - 1500 }), '', '1.5 s is quiet');
eq(why({ now: 300000, lastInputAt: 300000 - 120001 }), 'idle', 'nobody around for 2 minutes: wait for the next use');
eq(why({ oauth: true }), 'oauth', 'OAuth return in progress');
eq(why({ holds: 1 }), 'hold', 'hold()');
eq(why({ writes: 1 }), 'request', 'a request in flight');
eq(why({ frameUntil: 100001 }), 'frame', 'a new hidden frame (print helper)');
eq(why({ frameUntil: 100000 }), '', 'the frame hold is over');
eq(why({ dirty: true }), 'unsent text', 'a focused field with unsent text');
eq(why({ dialog: true }), 'dialog', 'an open dialog');
eq(why({ busy: true }), 'busy', 'a busy marker');
eq(why({ quietMs: 4000, lastInputAt: 97000 }), 'typing', 'a phone page (4 s) waits 4 s');
ok(T.worthReload(fb, ''), 'a build not reloaded for yet');
ok(!T.worthReload(fb, fb.hash), 'the build this tab already reloaded for: never again');
ok(T.worthReload(fb, fa.hash), 'a newer build than the one reloaded for');
ok(!T.worthReload(null, '') && !T.worthReload({ hash: '' }, ''), 'no build, no reload');
{
  const h = T.createHolds(1000);
  const a = h.add('print', 0), b = h.add('label', 10);
  eq(h.count(20), 2, 'two holds');
  ok(h.drop(a), 'release by id'); eq(h.count(20), 1, 'one left');
  ok(h.drop('label') && !h.drop('label'), 'release by tag, once'); eq(h.count(20), 0, 'none left');
  h.add('forgotten', 0); eq(h.count(999), 1, 'a hold counts for its time'); eq(h.count(1000), 0, 'a forgotten release lapses by itself');
  const h2 = T.createHolds(1000); h2.add('x', 0); h2.add('y', 0); ok(h2.drop(null), 'release() with no tag drops the latest'); eq(h2.count(1), 1, 'one left');
}

/* ───────────── B: the real script in jsdom ───────────── */
let JSDOM;
try { ({ JSDOM } = require('jsdom')); } catch (_) { JSDOM = null; }
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(cond, ms, what) { const t0 = Date.now(); while (!cond()) { if (Date.now() - t0 > (ms || 1500)) throw new Error('timed out waiting for ' + what); await wait(5); } }

async function boot(opts) {
  opts = opts || {};
  const html = '<!doctype html><html><head></head><body><script src="station-session.js?v=20261006-st3"></script><input id="scan" type="text"><div id="modal" class="modal" style="display:none">m</div></body></html>';
  const dom = new JSDOM(html, { url: opts.url || 'https://station.test/assembly-1.html?pair=SECRET-PAIR-CODE', runScripts: 'dangerously', pretendToBeVisual: true });
  const win = dom.window;
  win.Element.prototype.getClientRects = function () { return this.style && this.style.display === 'none' ? [] : [{}]; };   // jsdom has no layout
  if (opts.webdriver) Object.defineProperty(win.navigator, 'webdriver', { get: () => true });
  if (opts.brokenStorage) win.Storage.prototype.setItem = function () { throw new Error('quota'); };
  const w = { win, doc: win.document, text: pageA, requests: [], reloads: 0, failing: false, pending: [] };
  win.fetch = (url, init) => {
    w.requests.push({ url: String(url), cache: init && init.cache });
    if (/\.netlify\/functions/.test(String(url))) return new Promise(res => w.pending.push(() => res({ ok: true, headers: { get: () => 'application/json' }, text: async () => '{}' })));
    if (w.failing) return Promise.reject(new Error('offline'));
    return Promise.resolve({ ok: true, headers: { get: () => 'text/html; charset=UTF-8' }, text: async () => w.text });
  };
  const s = win.document.createElement('script');
  if (opts.quiet) s.setAttribute('data-quiet-ms', String(opts.quiet));
  s.textContent = SRC;
  win.document.body.appendChild(s);
  w.api = win.StationFresh;
  if (w.api._t) {
    w.api._t.hooks.reload = () => { w.reloads++; w.api._t.st.reloading = false; };   // worst case: the reload happened and the page is still the old one
    Object.assign(w.api._t.cfg, { tickMs: 10, armMs: 5000, noteMs: 120 }, opts.quiet ? {} : { quietMs: 40 });
  }
  w.pages = () => w.requests.filter(r => /assembly-1\.html$/.test(r.url));
  w.use = () => { w.api._t.st.lastCheckAt = 0; w.api._t.onUse({ type: 'pointerdown', isTrusted: true }); };   // a person's tap, long after the last check
  return w;
}

(async () => {
  if (!JSDOM) { console.log('station-fresh: part A passed (' + checks + ' checks); jsdom not found, part B skipped'); return; }

  // load: ONE conditional request, to the page itself without its query string; then nothing until the station is used
  let w = await boot();
  eq(w.api.inert, undefined, 'an ordinary page is not inert');
  await until(() => w.api.status().baseline, 800, 'the baseline');
  eq(w.requests.length, 1, 'one request at load'); eq(w.requests[0].url, 'https://station.test/assembly-1.html', 'the page itself, no query string (a phone pairing code stays out of it)');
  eq(w.requests[0].cache, 'no-cache', 'a conditional request (304 on the CDN when nothing changed)');
  await wait(150); eq(w.requests.length, 1, 'nothing on a timer'); eq(w.reloads, 0, 'no reload without a deploy');
  w.use(); await wait(150); eq(w.requests.length, 2, 'the first use after 3 minutes: one more check'); eq(w.reloads, 0, 'same build, no reload');
  w.api._t.onUse({ type: 'pointerdown', isTrusted: true }); await wait(60); eq(w.requests.length, 2, 'a second tap within 3 minutes: no request');
  w.api._t.onUse({ type: 'pointerdown', isTrusted: false }); eq(w.api.status().lastInputAt === w.api._t.st.lastInputAt, true, 'status is read only');
  w.win.close();

  // a deploy lands: noticed on the next use, ONE reload after the quiet time, the build remembered, the note shown after it
  w = await boot();
  await until(() => w.api.status().baseline, 800, 'the baseline');
  w.text = pageB;
  const t0 = Date.now(); w.use();
  await until(() => w.reloads === 1, 1500, 'the reload');
  ok(Date.now() - t0 >= 40, 'not before the quiet time');
  ok(w.requests.some(r => r.cache === 'reload'), 'the newest page is fetched with cache: reload first');
  eq(w.win.sessionStorage.getItem('stationFresh.v1:/assembly-1.html'), T.hashText(pageB), 'the build is remembered in sessionStorage');
  eq(w.win.sessionStorage.getItem('stationFresh.v1.note'), '1', 'the note is asked for after the reload');
  await wait(100); eq(w.reloads, 1, 'reloaded once');
  for (let i = 0; i < 3; i++) { w.use(); await wait(120); }
  eq(w.reloads, 1, 'the same build is never reloaded for twice (no loop), however often it is used');
  w.text = pageB.replace('var inline = 1;', 'var inline = 3;'); w.use();
  await until(() => w.reloads === 2, 1500, 'the reload for the next build');
  eq(w.win.sessionStorage.getItem('stationFresh.v1:/assembly-1.html'), T.hashText(w.text), 'the newer build is remembered');
  w.win.close();

  // the note after a reload: shown, then gone
  w = await boot({ });
  w.win.close();
  {
    const dom = new JSDOM('<!doctype html><body><script src="station-session.js?v=20261006-st3"></script></body>', { url: 'https://station.test/assembly-1.html', runScripts: 'dangerously', pretendToBeVisual: true });
    dom.window.sessionStorage.setItem('stationFresh.v1.note', '1');
    dom.window.fetch = () => Promise.resolve({ ok: true, headers: { get: () => 'text/html' }, text: async () => pageA });
    const s = dom.window.document.createElement('script'); s.textContent = SRC; dom.window.document.body.appendChild(s);
    const note = () => dom.window.document.getElementById('stationFreshNote');
    ok(note() && note().textContent === 'Updated to the newest version' && note().getAttribute('role') === 'status', 'the small labelled note appears after the reload');
    eq(dom.window.sessionStorage.getItem('stationFresh.v1.note'), null, 'and is asked for once');
    const shownAt = Date.now();
    await until(() => !note(), 4000, 'the note to fade away');
    ok(Date.now() - shownAt >= 2800 && Date.now() - shownAt <= 3600, 'it fades by itself in about 3 s'); dom.window.close();
  }

  // holds and everything else that puts the reload off until it ends
  async function blocked(setup, release, label) {
    const x = await boot();
    await until(() => x.api.status().baseline, 800, 'the baseline');
    x.text = pageB;
    const undo = setup(x);
    x.use(); await wait(250);
    eq(x.reloads, 0, label + ': no reload while it lasts');
    ok(x.api.status().pending, label + ': but the new build is known');
    release(x, undo); x.api._t.onUse({ type: 'pointerdown', isTrusted: true });
    await until(() => x.reloads === 1, 1500, label + ': the reload once it ends');
    x.win.close();
  }
  await blocked(x => x.api.hold('label'), (x, undo) => undo(), 'hold() (a label being bought)');
  await blocked(x => { const r = x.api.hold('a'); x.api.hold('b'); return r; }, (x, undo) => { undo(); x.api.release(); }, 'two holds, both released');
  await blocked(x => { x.win.fetch('/.netlify/functions/firebaseOrders', { method: 'POST' }); return null; }, x => { x.pending.forEach(f => f()); }, 'a request in flight');
  await blocked(x => { const el = x.doc.getElementById('scan'); el.focus(); el.value = '123'; x.api._t.onTyped({ target: el, isTrusted: true }); return null; },
                x => { const el = x.doc.getElementById('scan'); x.api._t.onKey({ key: 'Enter', target: el, isTrusted: true }); }, 'a focused field with unsent text');
  await blocked(x => { x.doc.getElementById('modal').style.display = 'block'; return null; }, x => { x.doc.getElementById('modal').style.display = 'none'; }, 'an open dialog');
  await blocked(x => { const sp = x.doc.createElement('button'); sp.id = 'b'; sp.setAttribute('aria-busy', 'true'); x.doc.body.appendChild(sp); return null; }, x => { x.doc.getElementById('b').remove(); }, 'a busy marker');
  {
    const x = await boot();                                      // a hidden frame (the print helpers) added by the tap that just happened
    await until(() => x.api.status().baseline, 800, 'the baseline');
    x.api._t.cfg.frameHoldMs = 200; x.text = pageB; x.use();
    x.doc.body.appendChild(x.doc.createElement('iframe'));
    await wait(120); eq(x.reloads, 0, 'a new hidden frame: no reload while the print helper may be working');
    await until(() => x.reloads === 1, 1500, 'the reload after the frame hold'); x.win.close();
  }
  {
    const x = await boot();                                      // typing keeps pushing it back: a key within the quiet time
    await until(() => x.api.status().baseline, 800, 'the baseline');
    x.text = pageB; x.use();
    for (let i = 0; i < 8; i++) { await wait(15); x.api._t.onKey({ key: '1', target: x.doc.body, isTrusted: true }); }
    eq(x.reloads, 0, 'a burst of keys (a scanner): no reload in the middle of it');
    await until(() => x.reloads === 1, 1500, 'the reload after the burst'); x.win.close();
  }

  // nothing happens: failed check, hidden tab, no way to keep the note, automated browser, nobody around
  {
    const x = await boot();
    await until(() => x.api.status().baseline, 800, 'the baseline');
    x.failing = true; x.text = pageB; x.use(); await wait(150);
    eq(x.reloads, 0, 'a failed check (offline) does nothing'); eq(x.api.status().pending, null, 'and learns nothing'); x.win.close();
  }
  {
    const x = await boot();
    await until(() => x.api.status().baseline, 800, 'the baseline');
    Object.defineProperty(x.doc, 'visibilityState', { get: () => 'hidden', configurable: true });
    const n = x.requests.length; x.text = pageB; x.use(); await wait(150);
    eq(x.requests.length, n, 'a hidden tab makes no request'); eq(x.reloads, 0, 'and does not reload'); x.win.close();
  }
  {
    const x = await boot({ brokenStorage: true });
    await until(() => x.api.status().baseline, 800, 'the baseline'); x.text = pageB; x.use(); await wait(250);
    eq(x.reloads, 0, 'a tab that cannot keep its note never reloads (no loop is possible)'); x.win.close();
  }
  {
    const x = await boot({ webdriver: true });
    eq(x.api.inert, 'automated browser', 'an automated browser is left alone'); await wait(60);
    eq(x.requests.length, 0, 'no request'); ok(typeof x.api.hold() === 'function', 'the API still answers (hold returns a release)'); x.api.touch(); x.win.close();
  }
  {
    const x = await boot();
    await until(() => x.api.status().baseline, 800, 'the baseline'); x.text = pageB;
    x.api._t.st.lastCheckAt = 0; x.api._t.cfg.armMs = 60; x.api._t.onUse({ type: 'pointerdown', isTrusted: true });
    await until(() => x.api.status().pending, 800, 'the new build to be known');
    x.api.hold('long'); await wait(200);                         // somebody walked away behind a hold: the looking stops
    x.api._t.st.lastInputAt = Date.now() - 1000000; const r = x.api.release(); ok(r, 'release answers true when it released');
    await wait(120); eq(x.reloads, 0, 'nobody around: it waits for the next use instead of reloading by itself'); x.win.close();
  }
  {
    const x = await boot({ quiet: 4000 });                       // a phone scanner page
    eq(x.api._t.cfg.quietMs, 4000, 'data-quiet-ms="4000" is read'); x.win.close();
  }
  {
    const x = await boot();                                      // every Netlify function call counts while in flight, and stops counting
    await until(() => x.api.status().baseline, 800, 'the baseline');
    x.win.fetch('/.netlify/functions/a'); eq(x.api.status().writes, 1, 'a function call is in flight'); x.pending.forEach(f => f());
    await wait(20); eq(x.api.status().writes, 0, 'and done'); x.win.close();
  }
  console.log('station-fresh: OK, ' + checks + ' checks');
})().catch(e => { console.error('station-fresh FAILED:', e && e.stack || e); process.exit(1); });
