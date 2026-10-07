// The test's side of the fake shop: starts backend.cjs, talks to its control door, and makes browser contexts that can reach nothing else.
//   const W = await World.start({ now: '2026-10-07T13:00:00Z', browser });  const ctx = await W.context();  const page = await W.open(ctx, 'weld-1.html');
'use strict';
const path = require('path'), fs = require('fs'), { spawn } = require('child_process');
const root = path.join(__dirname, '../../..');

const sleep = ms => new Promise(r => setTimeout(r, ms));
const LIB_DIRS = [process.env.E2E_LIBS, '/tmp/claude-0/-home-user-GoKu-Shipping/ff0c0738-06e1-5ab2-b954-36b896202d59/scratchpad/e2e-libs'].filter(Boolean);
const lib = n => { for (const d of LIB_DIRS) { try { return fs.readFileSync(path.join(d, n)); } catch (_) {} } return null; };
const MATERIALIZE_STUB = `window.M = (function () {
  var inst = new Map(), mk = function () { var i = { isOpen: false, open: function () { i.isOpen = true; }, close: function () { i.isOpen = false; }, destroy: function () {} }; return i; };
  var of = function (el) { if (!inst.has(el)) inst.set(el, mk()); return inst.get(el); }, any = { init: function (el) { return of(el); }, getInstance: function (el) { return of(el); } };
  return { AutoInit: function () {}, toast: function (o) { (window.__toasts = window.__toasts || []).push(String(o && o.html || '')); }, updateTextFields: function () {}, textareaAutoResize: function () {},
    Modal: any, Tabs: any, Dropdown: any, Tooltip: any, Collapsible: any, Sidenav: any, FormSelect: { init: function () { return { getSelectedValues: function () { return []; } }; }, getInstance: function () { return { getSelectedValues: function () { return []; } }; } } };
})();`;
// the modular SDK (pages import it only for picture uploads and an anonymous sign-in): names that exist and do nothing
const ESM_STUB = `export const initializeApp = () => ({ options: {} }), getApp = () => ({ options: { storageBucket: 'fake.appspot.com' } }), getApps = () => [],
  getStorage = () => ({}), ref = () => ({}), uploadBytes = async () => { throw new Error('no storage in the test'); }, uploadBytesResumable = () => { throw new Error('no storage in the test'); },
  getDownloadURL = async () => '/__pic/x.png', getAuth = () => ({ currentUser: { uid: 'anon' } }), signInAnonymously = async () => ({ user: { uid: 'anon' } }),
  onAuthStateChanged = cb => { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; };`;
// the label maker of the QR Printer (a PDF library): a stand-in that says how many labels were asked for and hands back a page
const PDFMAKE_STUB = `window.__pdfMade = []; window.pdfMake = { vfs: {}, fonts: {}, createPdf: function (def) { var n = 0; try { n = (def.content || []).filter(function (c) { return c && c.pageBreak !== 'after' || (c && c.image); }).length; } catch (e) {} window.__pdfMade.push({ at: Date.now(), blocks: n, text: JSON.stringify(def.content || '').slice(0, 4000) });
  return { getBlob: function (cb) { cb(new Blob(['<!doctype html><title>label</title><p>label</p>'], { type: 'text/html' })); }, download: function () {}, open: function () {}, print: function () {} }; } };`;
const JQUERY_STUB = 'window.$=window.jQuery=function(){var o={on:function(){return o},ready:function(f){try{f()}catch(e){}return o},off:function(){return o},each:function(){return o},css:function(){return o},hide:function(){return o},show:function(){return o}};return o};';

class World {
  constructor() { this.child = null; this.ctl = ''; this.sorterOrigin = ''; this.stationOrigin = ''; this.pass = ''; this.pages = []; this.ctxs = []; this.outside = []; this.stubbed = []; this.errors = []; this.clockFailures = []; this.maxLag = { ms: 0 }; this.lagFixed = 0; this.browser = null; }

  static async start(o) {
    const W = new World(); W.browser = o.browser;
    await new Promise((resolve, reject) => {
      const ch = spawn(process.execPath, [path.join(__dirname, 'backend.cjs'), '--now=' + o.now], { env: Object.assign({}, process.env), cwd: root });
      W.child = ch; let buf = '', err = '';
      ch.stderr.on('data', d => { err += d; });
      const t = setTimeout(() => reject(new Error('the fake shop did not start: ' + err.slice(-600))), 120000);
      ch.stdout.on('data', d => { buf += d; const m = /READY (\{.*\})/.exec(buf); if (m) { clearTimeout(t); Object.assign(W, JSON.parse(m[1])); resolve(); } });
      ch.on('exit', c => { if (!W.ctl) { clearTimeout(t); reject(new Error('the fake shop stopped (' + c + '): ' + err.slice(-600))); } });
    });
    W.beErr = () => '';
    return W;
  }
  stop() { try { this.child.kill(); } catch (_) {} }

  async get(p) { const r = await fetch(this.ctl + p); return r.json(); }
  async post(p, body) { const r = await fetch(this.ctl + p, { method: 'POST', body: JSON.stringify(body || {}) }); return r.json(); }
  now() { return this.get('/__ctl/now').then(r => r.now); }
  skew(ms) { return this.get('/__ctl/skew?ms=' + ms); }
  doc(p) { return this.get('/__ctl/doc?path=' + encodeURIComponent(p)); }
  list(c) { return this.get('/__ctl/list?coll=' + encodeURIComponent(c)); }
  stats() { return this.get('/__ctl/stats'); }
  calls(from) { return this.get('/__ctl/calls?from=' + (from || 0)); }
  /** the efficiency console's own reader, asked directly (with the fake passcode): what the page should say */
  eff(body) { return this.post('/__ctl/eff', body).then(r => r); }

  /** a browser context that reaches only the fake shop; every other request is answered by a stub or refused (and written down) */
  async context(o) {
    o = o || {};
    const W = this, now = await this.now();
    const ctx = await this.browser.newContext(Object.assign({ viewport: { width: 1400, height: 950 } }, o.ctx || {}));
    // (the real timers, kept for the fake Firestore's polling, before the fake clock replaces them)
    await ctx.addInitScript(() => { window.__rst = window.setTimeout.bind(window); window.__rsi = window.setInterval.bind(window); });
    await ctx.clock.install({ time: now });
    /* The Sorter app (and the Design Station framed by it) loads charm-nest-clock.js, which routes every setTimeout / setInterval of the page through a dedicated
       Web Worker so a tab out of view keeps full speed. A Worker's timers are NOT driven by Playwright's fake clock: they run on REAL time. So in this test the Sorter's
       StationSession tick (every 30 s), rules tick (10 s) and midnight timer fired only when real seconds passed, not when the fake clock was stepped: a Laser person
       signed in again at 16:20 got no beat for 40 fake minutes (they fall inside about 30 real seconds) while the other pages beat on schedule, and which beats
       fell where depended on the machine's speed (LS2: this was the "intermittent" Laser / idle failures, a test artifact, not a product defect: a real browser's
       worker runs on real time, which IS the page's time). charm-nest-clock.js itself says "If the worker cannot start, the page keeps the browser's own timers":
       the clock worker alone is refused here, so the page's timers are the fake clock's, like every other page's. Any other Worker is untouched.
       OFF unless NO_CLOCK_WORKER=1: in the full end-to-end test it made every Sorter page run all its (many) timers inside each 8-second clock piece, which slowed the
       steps and widened the measured clock lag of those pages (a first run then showed Laser / Design hours 31-37 s off); the end-to-end test wakes the Laser page by a
       focus event instead (stepTo), as the page's own wake handler does. tests/stations/laser-resignin.cjs sets NO_CLOCK_WORKER=1 (KEEP_CLOCK_WORKER=1 there shows the old behaviour). */
    if (process.env.NO_CLOCK_WORKER === '1') await ctx.addInitScript(() => {
      try {
        const Real = window.Worker;
        if (typeof Real === 'function') window.Worker = new Proxy(Real, { construct(target, args, nt) { if (/charm-nest-clock/.test(String(args[0]))) throw new Error('no clock worker in the test (its timers would not follow the fake clock)'); return Reflect.construct(target, args, nt); } });
      } catch (_) {}
    });
    const fbJs = fs.readFileSync(path.join(__dirname, 'fake-firebase.js'));
    const cors = { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' };
    const jq = lib('jquery.min.js'), mz = lib('materialize.min.js'), mzCss = lib('materialize.min.css');
    await ctx.route(() => true, async route => {
      const req = route.request(), u = new URL(req.url());
      if (u.hostname === '127.0.0.1' || u.hostname === 'localhost') return route.continue();
      const note = k => W.stubbed.push({ k, url: u.href.slice(0, 120) });
      if (/gstatic\.com$/.test(u.hostname) && /firebasejs/.test(u.pathname) && !/-compat\.js$/.test(u.pathname)) { note('firebase-esm'); return route.fulfill({ status: 200, contentType: 'text/javascript', headers: cors, body: ESM_STUB }); }
      if (/gstatic\.com$/.test(u.hostname) && /firebasejs/.test(u.pathname)) { note('firebase'); return route.fulfill({ status: 200, contentType: 'text/javascript', headers: cors, body: /firebase-app-compat/.test(u.pathname) ? fbJs : '' }); }
      if (/materialize/.test(u.pathname)) { note('materialize'); const css = /\.css/.test(u.pathname); return route.fulfill({ status: 200, contentType: css ? 'text/css' : 'text/javascript', headers: cors, body: css ? (mzCss || '') : (mz || MATERIALIZE_STUB) }); }
      if (/code\.jquery\.com|jquery/.test(u.hostname + u.pathname)) { note('jquery'); return route.fulfill({ status: 200, contentType: 'text/javascript', headers: cors, body: jq || JQUERY_STUB }); }
      if (/pdfmake|vfs_fonts/.test(u.pathname)) { note('pdfmake'); return route.fulfill({ status: 200, contentType: 'text/javascript', headers: cors, body: /vfs_fonts/.test(u.pathname) ? '' : PDFMAKE_STUB }); }
      if (/qz-tray|qz\.io/.test(u.href)) { note('qz'); return route.fulfill({ status: 200, contentType: 'text/javascript', headers: cors, body: 'window.qz = window.qz || { websocket: { isActive: function () { return false; }, connect: function () { return Promise.reject(new Error("no printer in the test")); } }, security: { setCertificatePromise: function () {}, setSignaturePromise: function () {} } };' }); }
      if (/qrcodejs|qrcode\.min/.test(u.pathname)) { note('qrcode'); return route.fulfill({ status: 200, contentType: 'text/javascript', headers: cors, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }); }
      if (/fonts\.googleapis\.com|fonts\.gstatic\.com/.test(u.hostname)) { note('font'); return route.fulfill({ status: 200, contentType: /gstatic/.test(u.hostname) ? 'font/woff2' : 'text/css', headers: cors, body: '' }); }
      if (u.hostname === 'i.etsystatic.com' || u.hostname === 'thumbs.test') { note('picture'); return route.fulfill({ status: 200, contentType: 'image/png', headers: cors, body: Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/Z8gVq4AAAAASUVORK5CYII=', 'base64') }); }
      W.outside.push({ method: req.method(), url: u.origin + u.pathname });
      return route.abort();
    });
    await ctx.addInitScript(([tok]) => {
      try { localStorage.setItem('access_token', tok); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400 * 30)); } catch (_) {}
      try { window.alert = function () {}; window.print = function () { (window.__prints = window.__prints || []).push(Date.now()); }; } catch (_) {}
    }, ['test-token']);
    ctx.__label = o.label || ('ctx' + this.ctxs.length); this.ctxs.push(ctx);
    return ctx;
  }
  /** a station computer is switched off: its browser profile goes (its pages stop beating, as a closed lid does) */
  async closeContext(ctx) {
    this.ctxs = this.ctxs.filter(c => c !== ctx);
    try { await ctx.close(); } catch (_) {}
  }
  /** a long jump of the shop's clock (a night, an afternoon): every computer's clock jumps with it and its timers fire once, as a page wakes after a long gap */
  async jump(ms, o) {
    o = o || {};
    await this.skew(ms);
    for (const c of this.ctxs) { if (o.except && o.except.includes(c)) continue; await this.tick(c, 'fastForward', ms); }
    await this.syncLag(o.except);
  }
  /** a clock operation on one computer; a failure (a slow machine, a closed page) is written down: a computer whose clock was left behind looks like a product defect */
  async tick(c, op, ms) { try { await c.clock[op](ms); } catch (e) { this.clockFailures.push({ ctx: c.__label || '?', op, ms, why: String(e && e.message || e).slice(0, 100) }); } }
  /** how far each computer's clock is behind the shop's after a long step; the widest is kept and printed at the end (it is only looked at, no clock is moved here: moving a page's clock
      forward by a few seconds in the middle of a run was tried and made the idle checks worse). The shop's clock and the computers' move one after the other, a page that is 10 s or more
      behind has the input times it reports shifted by the door, and on a busy machine that is what the intermittent idle / Laser failures look like. Computers in `except` are asleep on purpose. */
  async syncLag(except) {
    for (const c of this.ctxs) {
      if (except && except.includes(c)) continue;
      const pg = c.pages().find(p => !p.isClosed()); if (!pg) continue;
      let pt; try { pt = await pg.evaluate(() => Date.now()); } catch (_) { continue; }
      const lag = (await this.now()) - pt;
      if (lag > this.maxLag.ms) this.maxLag = { ms: lag, ctx: c.__label || '?' };
    }
  }
  /** one computer asleep through a stretch of time: its clock jumps (its timers fire once, on waking), the shop's clock is not touched here */
  async wake(ctx, ms) { try { await ctx.clock.fastForward(ms); } catch (_) {} }
  /** a computer that slept wakes at the shop's time: its clock jumps to it (its timers fire once, as when a lid opens), so what it then sends carries a true time */
  async syncClock(ctx) {
    try {
      const pg = ctx.pages().find(p => !p.isClosed()); if (!pg) return 0;
      const pt = await pg.evaluate(() => Date.now()), sn = await this.now();
      if (sn > pt) await ctx.clock.fastForward(sn - pt);
      return sn - pt;
    } catch (_) { return 0; }
  }
  /** the fake clock of every browser context moves together (and the shop's own clock with it): steps of `step` ms, so timers and beats fire as they would */
  async advance(ms, o) {
    o = o || {}; const step = o.step || 60000;
    for (let left = ms; left > 0; left -= step) {
      const d = Math.min(step, left);
      // The shop's clock and a computer's clock move one after the other, so while a page's timers fire inside its burst the two differ by up to the whole step. The
      // session door undoes a computer clock that is 10 s or more off (AD2: sentAt), which would move every end time by that difference: so a step is cut into pieces
      // of 8 s, and the two clocks never differ by 10 s while anything is sent.
      for (let rest = d; rest > 0; rest -= 8000) {
        const s = Math.min(8000, rest);
        await this.skew(s);
        for (const c of this.ctxs) { if (o.except && o.except.includes(c)) continue; await this.tick(c, 'runFor', s); }
      }
      if (o.between) await o.between(d);
      await sleep(o.settle || 120);
    }
    if (ms >= 60000) await this.syncLag(o.except);
  }

  /** a page of the shop on the station origin (localhost) or the sorter origin (127.0.0.1) */
  async open(ctx, file, o) {
    o = o || {};
    const page = await ctx.newPage();
    const errs = { page: [], console: [] };
    page.on('pageerror', e => { errs.page.push(e.message); this.errors.push(file + ': ' + e.message); });
    page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource|net::ERR|status of (401|403|404|429|503)/.test(m.text())) errs.console.push(m.text().slice(0, 240)); });
    page.__errs = errs; page.__file = file; page.__frames = [];
    page.on('framenavigated', f => { try { page.__frames.push(f.url().replace(/^https?:\/\/[^/]+/, '')); } catch (_) {} });
    if (o.camera) await page.addInitScript({ content: fs.readFileSync(path.join(__dirname, 'fake-camera.js'), 'utf8') });
    await page.goto((o.sorter ? this.sorterOrigin : this.stationOrigin) + '/' + file.split('/').map(encodeURIComponent).join('/') + (o.query || ''), { waitUntil: o.wait || 'load', timeout: 60000 });
    this.pages.push(page);
    return page;
  }
}
module.exports = { World, sleep, lib };
