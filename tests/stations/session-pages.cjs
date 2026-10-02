// Sign-in time and the midnight sign-out on the Design Station pages and the Inbox (part K of sign-in-sessions).
// station-session.js is replaced by a tiny stand-in (this file only) that keeps the plan's API: init / signedIn /
// signedOut, the page's signOut called at the next New York midnight and on a load on a later day. The clock is
// Playwright's, set to 23:58 New York and run past midnight. Every non-loopback request is aborted.
//   · design-message.html (6-digit PIN): the session names the person, never the PIN; at midnight the PIN's keys go,
//     the PIN box opens, the order on screen stays; a new PIN sign-in reports the name only; Sign Out ends the session
//   · design.html (operator passcode + the chat's name): the name is the person; at midnight the name and this tab's
//     passcode go and the station's own sign-in box asks again; a load on a later day signs out before anything else
//   · etsy-mail-1.html (operator accounts): the account is the person; at midnight the token goes, the server session
//     is ended, the sign-in screen covers the inbox; a new sign-in reports the account, never the password
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/session-pages.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const ORIGIN = 'http://station.test';
const T0 = Date.parse('2026-09-29T03:58:00Z');          // 23:58 in New York (EDT)
const NY = t => new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date(t));

const standIn = `window.__ss = { calls: [] };
window.StationSession = (() => {
  const nyDay = t => new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date(t));
  const nextMidnight = now => { const d = nyDay(now); let t = now - now % 60000 + 60000; while (nyDay(t) === d) t += 60000; return t; };
  const log = (k, v) => window.__ss.calls.push([k, JSON.parse(JSON.stringify(v === undefined ? null : v))]);
  let o = null;
  return {
    init(opts) {
      o = opts; log('init', { station: opts.station, device: opts.device, person: opts.person() });
      const day = localStorage.getItem('station_signin_day');
      if (day && day !== nyDay(Date.now())) { localStorage.removeItem('station_signin_day'); log('end', 'midnight'); opts.signOut('midnight'); }
      setTimeout(() => { localStorage.removeItem('station_signin_day'); log('end', 'midnight'); o.signOut('midnight'); log('after', o.person()); }, nextMidnight(Date.now()) - Date.now());
    },
    signedIn(p) { log('signedIn', p); localStorage.setItem('station_signin_day', nyDay(Date.now())); },
    signedOut(r) { log('signedOut', r); localStorage.removeItem('station_signin_day'); },
  };
})();`;
const fbStub = `window.firebase = (() => {
  const snap = (exists, data) => ({ exists, data: () => data || {}, get: f => (data || {})[f] });
  const doc = (c, id) => ({ id, onSnapshot(cb) { try { cb(snap(false)); } catch (_) {} return () => {}; },
    set: async () => {}, update: async () => {}, get: async () => snap(false), collection: n => col(c + '/' + id + '/' + n) });
  const col = c => { const q = { doc: id => doc(c, id), where: () => q, orderBy: () => q, limit: () => q, limitToLast: () => q, startAfter: () => q, add: async () => ({ id: 'x' }),
    onSnapshot(cb) { try { cb({ docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] }); } catch (_) {} return () => {}; },
    get: async () => ({ docs: [], empty: true, size: 0, forEach() {} }) }; return q; };
  const firestore = () => ({ collection: col, batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }), runTransaction: async () => {} });
  firestore.FieldValue = { delete: () => null, serverTimestamp: () => null, arrayUnion: () => null, increment: () => null };
  firestore.Timestamp = { now: () => ({ toMillis: () => Date.now() }), fromMillis: ms => ({ toMillis: () => ms }) };
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { try { cb({ uid: 'u' }); } catch (_) {} return () => {}; }, currentUser: { uid: 'u' } });
  return { initializeApp() {}, firestore, auth, app: () => ({ options: {} }), storage: () => ({ ref: () => ({}) }) };
})();`;
const mStub = `window.M = (() => {
  const inst = new Map();
  const mk = () => { const i = { isOpen: false, open() { i.isOpen = true; window.__opens = (window.__opens || 0) + 1; }, close() { i.isOpen = false; } }; return i; };
  const of = el => { if (!inst.has(el)) inst.set(el, mk()); return inst.get(el); };
  const any = { init: el => of(el), getInstance: el => of(el) };
  return { AutoInit() {}, toast(o) { (window.__toasts = window.__toasts || []).push(o && o.html); }, updateTextFields() {}, textareaAutoResize() {},
    Modal: any, Tabs: any, Dropdown: any, Tooltip: any, Collapsible: any, Sidenav: any,
    FormSelect: { init: () => ({ getSelectedValues: () => [] }), getInstance: () => ({ getSelectedValues: () => [] }) } };
})();`;

const json = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 9000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(60); } }

async function context(browser, st, init) {
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url()), m = r.request().method();
    if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : 'window.$=window.jQuery=()=>({on(){},ready(){}});' });
    if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
    if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
    if (u.origin !== ORIGIN) return r.abort();
    if (u.pathname === '/station-session.js') return r.fulfill({ status: 200, contentType: 'text/javascript', body: standIn });
    const body = r.request().postData() || '';
    st.sent.push(u.pathname + u.search + ' ' + body + ' ' + JSON.stringify(r.request().headers()));
    if (u.pathname.startsWith('/.netlify/functions/')) {
      const fn = u.pathname.split('/').pop();
      if (fn === 'firebaseOrders' && u.searchParams.get('orderId') === 'Employee Numbers') return json(r, { success: true, data: { '424242': 'Rosa Designer' } });
      if (fn === 'authGate') {
        if (m === 'GET') return json(r, { locked: true });
        return r.request().headers()['x-edit-passcode'] === 'pc-1' ? json(r, { ok: true }) : json(r, { ok: false }, 401);
      }
      if (fn === 'etsyMailAuth') {
        const b = JSON.parse(body || '{}'); st.auth.push({ op: b.op, session: r.request().headers()['x-etsymail-session'] || null });
        if (b.op === 'currentUser') return json(r, { ok: true, username: 'tess', displayName: 'Tess Inbox', role: 'operator' });
        if (b.op === 'login') return b.password === 'right-horse-9' ? json(r, { ok: true, sessionToken: 'tok-2', username: 'ana', displayName: 'Ana Mail', role: 'operator' }) : json(r, { error: 'bad', reason: 'BAD_PASSWORD' }, 401);
        return json(r, { ok: true });
      }
      return json(r, {});
    }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  await ctx.addInitScript(init);
  const page = await ctx.newPage();
  page.on('pageerror', e => st.errors.push(String(e && e.message || e)));
  await page.clock.install({ time: T0 });
  return { ctx, page };
}
const calls = page => page.evaluate(() => window.__ss.calls);
const has = (list, k) => list.filter(c => c[0] === k);

async function designMessage(browser) {
  const st = { sent: [], auth: [], errors: [] };
  const { ctx, page } = await context(browser, st, () => {
    if (sessionStorage.getItem('seeded')) return; sessionStorage.setItem('seeded', '1');
    localStorage.setItem('employee_id', '424242'); localStorage.setItem('employee_name', 'Rosa Designer');
    localStorage.setItem('station_signin_day', new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date(Date.parse('2026-09-29T03:58:00Z'))));
  });
  await page.goto(ORIGIN + '/design-message.html');
  await until(() => page.evaluate(() => window.__ss && window.__ss.calls.length && window.isEmployeeLoggedIn === true), 'design-message signed in on load');
  let c = await calls(page);
  assert.deepStrictEqual(c[0], ['init', { station: 'design', device: 'design-message', person: { name: 'Rosa Designer', id: '' } }], 'the session names the person, never the PIN');
  await page.fill('#etsyOrderNumber', '3521009999');
  await page.clock.runFor(3 * 60e3);
  await until(async () => has(await calls(page), 'end').length, 'the midnight sign-out');
  const after = await page.evaluate(() => ({ id: localStorage.getItem('employee_id'), name: localStorage.getItem('employee_name'), logged: window.isEmployeeLoggedIn,
    open: M.Modal.getInstance(document.getElementById('userLoginModal')).isOpen, field: document.getElementById('employeeName').value, order: document.getElementById('etsyOrderNumber').value }));
  assert.deepStrictEqual(after, { id: null, name: null, logged: false, open: true, field: '', order: '3521009999' }, 'midnight: the PIN keys go, the PIN box opens, the order stays');
  assert.deepStrictEqual((await calls(page)).pop(), ['after', null], 'nobody is signed in after midnight');
  // the next PIN sign-in reports the name only
  await page.focus('#employeeNumberInput');
  for (const d of '424242') await page.keyboard.press(d);
  await page.click('#employeeLoginBtn');
  await until(async () => has(await calls(page), 'signedIn').length, 'the PIN sign-in');
  assert.deepStrictEqual(has(await calls(page), 'signedIn')[0][1], { name: 'Rosa Designer', id: '' });
  await page.click('#signOutBtn');
  assert.deepStrictEqual(has(await calls(page), 'signedOut').map(x => x[1]), ['signOut'], 'the Sign Out button ends the session');
  const leak = [...st.sent.filter(s => /424242/.test(s) && !/Employee%20Numbers/.test(s)), ...(await calls(page)).map(JSON.stringify).filter(s => /424242/.test(s))];
  assert.deepStrictEqual(leak, [], 'the PIN is never sent nor handed to the session');
  await ctx.close();
  console.log('design-message: name only, midnight → PIN box with the order kept, PIN sign-in and Sign Out reported');
}

async function design(browser) {
  const st = { sent: [], auth: [], errors: [] };
  const { ctx, page } = await context(browser, st, () => {
    if (sessionStorage.getItem('seeded')) return; sessionStorage.setItem('seeded', '1');
    sessionStorage.setItem('designStation.passcode', 'pc-1'); localStorage.setItem('employee_name', 'Dana Design');
    localStorage.setItem('station_signin_day', new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date(Date.parse('2026-09-29T03:58:00Z'))));
  });
  await page.goto(ORIGIN + '/design.html');
  await until(() => page.evaluate(() => window.__ss && window.__ss.calls.length > 0), 'design init');
  assert.deepStrictEqual((await calls(page))[0], ['init', { station: 'design', device: 'design', person: { name: 'Dana Design', id: '' } }]);
  await page.clock.runFor(5000);
  assert(!(await page.$('#pcGate')), 'the held passcode opens the station');
  await page.clock.runFor(3 * 60e3);
  await until(async () => has(await calls(page), 'end').length, 'the midnight sign-out');
  await until(() => page.$('#pcGate'), 'the station\'s own sign-in box');
  const after = await page.evaluate(() => ({ name: localStorage.getItem('employee_name'), pc: sessionStorage.getItem('designStation.passcode') }));
  assert.deepStrictEqual(after, { name: null, pc: null }, 'midnight: the name and this tab\'s passcode go');
  // the passcode again, then a name in the chat: that is the sign-in
  await page.fill('#pcGate .pcInput', 'pc-1'); await page.click('#pcGate button');
  await until(async () => !(await page.$('#pcGate')), 'the box goes once the passcode is accepted');
  await page.evaluate(() => BritesChat.open('3521000777'));
  await until(() => page.$('#bcWho'), 'the chat');
  // the chat lives in the order window (closed here): the same field, driven directly
  await page.evaluate(() => { document.getElementById('bcWho').click(); const inp = document.querySelector('#bcWho input'); inp.value = 'Nora Night'; inp.dispatchEvent(new Event('blur')); });
  await until(async () => has(await calls(page), 'signedIn').length, 'the name sign-in');
  assert.deepStrictEqual(has(await calls(page), 'signedIn')[0][1], { name: 'Nora Night', id: '' });
  assert.strictEqual(await page.evaluate(() => document.getElementById('bcWhoName').textContent), 'Nora Night');
  await ctx.close();

  // a load on a later day: signed out before the station opens, so the box asks at once
  const st2 = { sent: [], auth: [], errors: [] };
  const b = await context(browser, st2, () => {
    sessionStorage.setItem('designStation.passcode', 'pc-1'); localStorage.setItem('employee_name', 'Dana Design');
    localStorage.setItem('station_signin_day', '2026-09-27');
  });
  await b.page.goto(ORIGIN + '/design.html');
  await until(() => b.page.$('#pcGate'), 'the sign-in box on a later day');
  assert.strictEqual(await b.page.evaluate(() => localStorage.getItem('employee_name')), null);
  await b.ctx.close();
  console.log('design: chat name is the person; midnight → name and passcode go, sign-in box; later-day load signs out first');
}

async function inbox(browser) {
  const st = { sent: [], auth: [], errors: [] };
  const { ctx, page } = await context(browser, st, () => {
    if (sessionStorage.getItem('seeded')) return; sessionStorage.setItem('seeded', '1');
    localStorage.setItem('etsymail_session', 'tok-1');
    localStorage.setItem('etsymail_session_profile', JSON.stringify({ username: 'tess', displayName: 'Tess Inbox', role: 'operator', cachedAtMs: Date.now() }));
    localStorage.setItem('station_signin_day', new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date(Date.parse('2026-09-29T03:58:00Z'))));
  });
  await page.goto(ORIGIN + '/etsy-mail-1.html');
  await until(() => page.evaluate(() => window.__ss && window.__ss.calls.length > 0), 'inbox init');
  assert.deepStrictEqual((await calls(page))[0], ['init', { station: 'inbox', device: 'etsy-mail-1', person: { name: 'Tess Inbox', id: 'tess' } }]);
  await page.clock.runFor(3000);
  await until(() => page.evaluate(() => document.body.classList.contains('authed')), 'the inbox opens on the held session');
  await page.clock.runFor(3 * 60e3);
  await until(async () => has(await calls(page), 'end').length, 'the midnight sign-out');
  await until(() => st.auth.some(a => a.op === 'logout' && a.session === 'tok-1'), 'the server session ended');
  const after = await page.evaluate(() => ({ tok: localStorage.getItem('etsymail_session') || sessionStorage.getItem('etsymail_session'), authed: document.body.classList.contains('authed'),
    banner: document.getElementById('siBanner').classList.contains('show') && document.getElementById('siBannerText').textContent }));
  assert.deepStrictEqual(after, { tok: null, authed: false, banner: 'Signed out at midnight. Sign in again to open the inbox.' });
  assert.deepStrictEqual((await calls(page)).pop(), ['after', null]);
  await page.fill('#siUser', 'ana'); await page.fill('#siPass', 'right-horse-9');
  await page.click('#siBtn');
  await until(async () => has(await calls(page), 'signedIn').length, 'the account sign-in');
  assert.deepStrictEqual(has(await calls(page), 'signedIn')[0][1], { name: 'Ana Mail', id: 'ana' });
  assert(!JSON.stringify(await calls(page)).includes('right-horse-9'), 'the password never reaches the session');
  await ctx.close();
  console.log('inbox: account is the person; midnight → token gone, server session ended, sign-in screen; sign-in reported');
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    assert.strictEqual(NY(T0), '2026-09-28'); assert.strictEqual(NY(T0 + 3 * 60e3), '2026-09-29');
    await designMessage(browser);
    await design(browser);
    await inbox(browser);
    console.log('session-pages: all passed');
  } finally { await browser.close(); }
})().catch(e => { console.error(e); process.exit(1); });
