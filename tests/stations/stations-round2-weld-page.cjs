// ST2 · the page half of the adversarial tests for the Welding station (weld-1.html), run by stations-round2-adversarial.cjs when
// PW_DIR is set, or by itself:   PW_DIR=<playwright node_modules> node tests/stations/stations-round2-weld-page.cjs
// Fakes only: every request that is not the page itself is stubbed or aborted, the clock is Playwright's, nothing leaves the machine.
// The login door is a stub that answers { pinLogin } with a name from a table of made-up numbers; the numbers exist only in this run.
//   a · three people: one tap signs out one task, a double click / double touch / key repeat on a chip never signs out the chip that
//       slid under the finger, a slow second tap is accepted, a stray click on the hidden Sign Out ends everybody once each
//   b · the clock set back an hour after a tap on a chip: the chips still answer (a "last tap" time in the future must not lock them)
//   c · hostile names from the login door (markup, quotes, a number inside the name, control characters, no letters): nothing runs,
//       the chips and the sessions agree, midnight leaves nobody half signed in
//   d · garbage in the browser storage (weld_people): the page loads, shows only what is valid, never a number
//   e · two tabs of one computer: each sign-in is one session, a sign-out in one tab is followed by the other
//   f · midnight with the "Welding or Matching?" step open: everybody ends, the step cannot sign the pending person in afterwards
//   g · a page that crashes with two people signed in: the next page goes on with the same sessions, nothing started, nothing ended
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), http = require('http');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROMIUM = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const wait = ms => new Promise(r => setTimeout(r, ms));
const until = async (fn, what, ms = 9000) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(50); } };
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png' };
const MIDNIGHT = Date.parse('2026-10-07T04:00:00Z');       // 00:00 on 7 Oct in New York (EDT)

const used = ['987654'];
const fakePin = () => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!/^(\d)\1{5}$/.test(p) && !/(012345|123456|234567|345678|456789)/.test(p) && !used.includes(p)) { used.push(p); return p; } } };
const PIN = { tess: fakePin(), ray: fakePin(), ivy: fakePin(), xss: fakePin(), quote: fakePin(), num: fakePin(), ctl: fakePin(), none: fakePin(), digits: fakePin() };
const NUMBER = '987654';                                   // inside a name: the kind of number a PIN is
const NAMES = {
  [PIN.tess]: 'Tess Welder', [PIN.ray]: 'Ray Matcher', [PIN.ivy]: 'Ivy Third',
  [PIN.xss]: '<img src=x onerror="window.__xss=(window.__xss||0)+1"> Evil', [PIN.quote]: 'Mary "Q" O\'Brien </button><b id="bad">x</b>',
  [PIN.num]: 'Nina ' + NUMBER, [PIN.ctl]: 'Cy\u0001ril\u0007 Bell', [PIN.none]: '- - -', [PIN.digits]: NUMBER + ' ' + NUMBER
};

const FIREBASE = `(function () {
  const snaps = window.__fbSnaps = {}; window.__sets = [];
  const empty = () => ({ exists: false, data: () => undefined, docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] });
  function ref(p) {
    const r = { path: p, id: p.split('/').pop(), collection: n => ref(p + '/' + n), doc: n => ref(p + '/' + n),
      where: () => r, orderBy: () => r, limit: () => r, limitToLast: () => r, startAfter: () => r,
      onSnapshot(cb) { (snaps[p] = snaps[p] || []).push(cb); setTimeout(() => { try { cb(empty()); } catch (_) {} }, 0); return () => {}; },
      get: async () => empty(), set: async d => { window.__sets.push({ path: p, data: d }); }, update: async () => {}, add: async () => ref(p + '/new'), delete: async () => {} };
    return r;
  }
  const firestore = () => ({ collection: n => ref(n), doc: p => ref(p), batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }) });
  firestore.FieldValue = { delete: () => ({ __delete: true }), serverTimestamp: () => ({}), arrayUnion: (...a) => a, increment: n => n };
  firestore.Timestamp = { now: () => ({ toDate: () => new Date(), toMillis: () => Date.now() }) };
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; }, currentUser: { uid: 'anon' } });
  window.firebase = { apps: [], initializeApp() { return {}; }, firestore, auth, storage: () => ({ ref: () => ({}) }) };
})();`;
/* Materialize's toast puts its html in the page (innerHTML), so the stub does the same: a name that reaches a toast unescaped runs here too */
const MATERIALIZE = `window.__toasts = []; window.__opens = 0; window.M = { AutoInit() {}, updateTextFields() {},
  toast(o) { const h = String((o && o.html) || ''); window.__toasts.push(h); try { const d = document.createElement('div'); d.className = 'toast'; d.innerHTML = h; (document.getElementById('__toastHost') || document.body).appendChild(d); } catch (_) {} },
  Modal: { init(el) { const i = { open() { i.isOpen = true; window.__opens++; }, close() { i.isOpen = false; }, isOpen: false }; if (el) el.__m = i; return i; }, getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
  FormSelect: { init(el) { const i = { destroy() {}, getSelectedValues: () => [el && el.value] }; if (el) el.__fs = i; return i; }, getInstance(el) { return el && el.__fs; } },
  Dropdown: { init() {} } };`;

async function context(browser, base, o = {}) {
  const st = { sessions: [], events: [], timeline: [], doors: [], bodies: [], errors: [], roster: 0 };
  const ctx = await browser.newContext({ viewport: o.viewport || { width: 1500, height: 900 }, hasTouch: !!o.touch });
  await ctx.route(() => true, r => r.abort());                                         // nothing leaves the machine
  await ctx.route(u => u.href.startsWith('http://127.0.0.1'), r => r.continue());
  await ctx.route(u => /gstatic\.com\/firebasejs\//.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: /firebase-app-compat/.test(r.request().url()) ? FIREBASE : '' }));
  await ctx.route(u => /materialize/.test(u.href), r => r.fulfill({ contentType: /\.css/.test(r.request().url()) ? 'text/css' : 'text/javascript', body: /\.css/.test(r.request().url()) ? '' : MATERIALIZE }));
  await ctx.route(u => /code\.jquery\.com|qz-tray/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: '' }));
  await ctx.route(u => u.href.startsWith('http://127.0.0.1') && u.pathname.includes('/.netlify/functions/'), async r => {
    const req = r.request(), u = new URL(req.url()), fn = u.pathname.split('/').pop();
    const reply = (b, status) => r.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(b) });
    if (req.method() === 'POST') {
      const text = req.postData() || '{}'; let b = {}; try { b = JSON.parse(text); } catch (_) {}
      if (fn === 'firebaseOrders' && b.pinLogin !== undefined) { st.doors.push(text); return reply(NAMES[b.pinLogin] !== undefined ? { ok: true, name: NAMES[b.pinLogin] } : { ok: false, error: 'not on the list' }); }
      st.bodies.push(text);
      if (fn === 'firebaseOrders' && b.session) { st.sessions.push(b.session); return reply({ success: true }); }
      if (fn === 'firebaseOrders' && Array.isArray(b.activity)) { st.events.push(...b.activity); return reply({ success: true, written: b.activity.length, duplicate: 0, refused: 0, scrubbed: 0 }); }
      if (fn === 'firebaseOrders' && Array.isArray(b.timeline)) { st.timeline.push(...b.timeline); return reply({ ok: true, ids: b.timeline.map(e => e.id) }); }
      return reply({ success: true });
    }
    if (fn === 'etsyOrderProxy') return reply({ receipt_id: Number(u.searchParams.get('orderId')) || 0, status: 'Paid', transactions: [] });
    if (fn === 'firebaseOrders' && u.searchParams.get('cancelCheck')) return reply({ success: true, cancelled: {}, now: Date.now() });
    if (fn === 'firebaseOrders' && /employee/i.test(u.searchParams.get('orderId') || '')) { st.roster++; return reply({ success: false, error: 'closed' }, 401); }   // the roster is never read
    if (fn === 'firebaseOrders') return reply({ success: true, data: {} });
    return reply({});
  });
  await ctx.addInitScript(() => { try { localStorage.setItem('access_token', 'test-token'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400)); } catch (_) {} });
  if (o.init) await ctx.addInitScript(o.init, o.initArg);
  if (o.time) await ctx.clock.install({ time: o.time });
  const open = async () => {
    const page = await ctx.newPage();
    page.on('pageerror', e => { if (!/gstatic\.com\/firebasejs|getApp/.test(String(e))) st.errors.push(String(e && e.message || e)); });
    await page.goto(base + '/weld-1.html');
    await page.waitForFunction(() => window.StationSession && window.StationActivity && window.weldAfterChange && document.querySelector('#userLoginModal .station-session-pc'), null, { timeout: 20000 });
    return page;
  };
  return { ctx, st, open, page: await open() };
}

const chips = (page, task) => page.evaluate(t => [...document.querySelectorAll('#weldRoster .weld-group[data-task="' + t + '"] .weld-chip')].map(b => b.dataset.name), task);
const state = page => page.evaluate(() => ({ on: document.getElementById('weldRoster').classList.contains('on'), loggedIn: window.isEmployeeLoggedIn === true, modalOpen: !!document.getElementById('userLoginModal').__m.isOpen,
  step: document.getElementById('userLoginModal').classList.contains('weld-task'), multi: document.body.classList.contains('weld-multi'),
  signOut: getComputedStyle(document.getElementById('signOutBtn')).display !== 'none' }));
const typePin = async (page, pin) => {                                                  // through the real masking keys, like a person
  await page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = ''; i.value = ''; });
  await page.focus('#employeeNumberInput'); await page.keyboard.type(pin); await page.click('#employeeLoginBtn');
};
/** a person signs in: the number, then the task (the Add person button first when somebody is already in) */
async function signIn(page, pin, task) {
  if ((await state(page)).on) await page.click('#weldAddPerson');
  await typePin(page, pin);
  await page.waitForSelector('#userLoginModal.weld-task');
  await page.click(task === 'welding' ? '#weldTaskWelding' : '#weldTaskMatching');
}
const chipPoint = (page, name, task) => page.evaluate(([n, t]) => { const b = [...document.querySelectorAll('#weldRoster .weld-chip')].find(c => c.dataset.name === n && c.dataset.task === t); if (!b) return null; const r = b.getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; }, [name, task]);
const tap = async (page, name, task) => { const p = await chipPoint(page, name, task); assert(p, `no chip for ${name} / ${task}`); await page.mouse.click(p.x, p.y); return p; };
const sess = (st, event, person, task, reason) => st.sessions.filter(s => s.event === event && (!person || s.person === person) && (!task || s.task === task) && (!reason || s.reason === reason));
const anyPin = (st, extra) => { const all = JSON.stringify(st.bodies) + (extra || ''); return Object.values(PIN).filter(p => all.includes(p)); };
const noNumbers = async (st, page) => {
  const stored = await page.evaluate(() => JSON.stringify(Object.entries(localStorage)) + JSON.stringify(Object.entries(sessionStorage)));
  const hits = anyPin(st, stored + (await page.evaluate(() => window.__toasts.join('|'))));
  assert.deepStrictEqual(hits, [], 'a made-up Employee Number was stored, shown or sent beyond the login door');
  assert(st.doors.every(b => /^\{"pinLogin":"\d{6}"\}$/.test(b)), 'only the login door got numbers');
  assert.strictEqual(st.roster, 0, 'the whole roster was read');
  assert(!JSON.stringify(st.bodies).includes(NUMBER), 'a number inside a name was sent');
};

const results = { pass: 0, fail: [] };
async function check(name, fn) {
  try { await fn(); results.pass++; console.log('  PASS ' + name); }
  catch (e) { results.fail.push({ name, msg: String(e && e.message || e).split('\n').slice(0, 6).join('\n      ') }); console.log('  FAIL ' + name + '\n      ' + String(e && e.message || e).split('\n').slice(0, 6).join('\n      ')); }
}
const noErrors = st => assert.deepStrictEqual(st.errors, [], 'page errors: ' + st.errors.join('; '));

async function a_taps(browser, base) {
  const { ctx, st, page } = await context(browser, base, { touch: true });
  await signIn(page, PIN.tess, 'welding'); await until(() => sess(st, 'start', 'Tess Welder', 'welding').length === 1, 'Tess');
  await signIn(page, PIN.ray, 'matching'); await signIn(page, PIN.ivy, 'matching');
  await until(() => st.sessions.filter(s => s.event === 'start').length === 3, 'three starts');
  assert.deepStrictEqual([await chips(page, 'welding'), await chips(page, 'matching')], [['Tess Welder'], ['Ray Matcher', 'Ivy Third']]);
  let s = await state(page); assert(s.multi && !s.signOut, 'three people: chips only, no Sign Out button');
  // a double click on Ray: Ivy's chip slides under the finger and must stay
  const p = await chipPoint(page, 'Ray Matcher', 'matching'); await page.mouse.dblclick(p.x, p.y); await wait(900);
  assert.deepStrictEqual(sess(st, 'end').map(e => e.person + ':' + e.task), ['Ray Matcher:matching'], 'a double click signed out one person');
  assert.deepStrictEqual(await chips(page, 'matching'), ['Ivy Third']);
  // a slow second tap (the guard is a moment, not a lock): Ivy leaves
  await tap(page, 'Ivy Third', 'matching'); await until(() => sess(st, 'end', 'Ivy Third').length === 1, 'Ivy out');
  s = await state(page); assert(s.loggedIn && !s.multi && s.signOut, 'Tess alone: still signed in, Sign Out is back (it ends only her)');
  // a double touch-tap on a chip that has a neighbour sliding in
  await wait(800); await signIn(page, PIN.ray, 'matching'); await signIn(page, PIN.ivy, 'matching'); await until(() => sess(st, 'start', 'Ivy Third').length === 2, 'Ivy again');
  await wait(300);
  const q = await chipPoint(page, 'Ray Matcher', 'matching'); const ends0 = sess(st, 'end').length;
  await page.touchscreen.tap(q.x, q.y); await page.touchscreen.tap(q.x, q.y); await wait(900);
  assert.strictEqual(sess(st, 'end').length, ends0 + 1, 'a double touch signed out one person'); assert.deepStrictEqual(await chips(page, 'matching'), ['Ivy Third']);
  // a held key on a focused chip: the chip is gone after the first press
  await wait(800); await signIn(page, PIN.ray, 'matching'); await until(() => sess(st, 'start', 'Ray Matcher').length === 3, 'Ray again'); await wait(300);
  const ends1 = sess(st, 'end').length;
  await page.evaluate(() => [...document.querySelectorAll('#weldRoster .weld-chip')].find(c => c.dataset.name === 'Ray Matcher').focus());
  await page.keyboard.down('Enter'); await page.keyboard.down('Enter'); await page.keyboard.down('Enter'); await page.keyboard.up('Enter'); await wait(900);
  assert.strictEqual(sess(st, 'end').length, ends1 + 1, 'a held Enter key signed out one person');
  // a stray click on the hidden Sign Out button signs everybody out, each once, and brings the number box back
  await signIn(page, PIN.ray, 'matching'); await until(() => sess(st, 'start', 'Ray Matcher').length === 4, 'Ray again'); await wait(300);
  const endsBefore = sess(st, 'end').length, open = new Set(st.sessions.filter(x => x.event === 'start').map(x => x.id)); for (const e of sess(st, 'end')) open.delete(e.id);
  s = await state(page); assert(!s.signOut, 'three people: Sign Out hidden');
  await page.evaluate(() => document.getElementById('signOutBtn').click()); await until(async () => sess(st, 'end').length === endsBefore + open.size, 'everybody out');
  assert.deepStrictEqual(sess(st, 'end').slice(endsBefore).map(e => e.id).sort(), [...open].sort(), 'each open session ended once');
  s = await state(page); assert(!s.loggedIn && s.modalOpen && !s.on, 'the number box is back');
  assert.strictEqual(await page.evaluate(() => localStorage.getItem('weld_people')), '[]');
  await noNumbers(st, page); noErrors(st); await ctx.close();
}

async function b_clockBack(browser, base) {
  const { ctx, st, page } = await context(browser, base, { time: Date.parse('2026-10-06T15:00:00Z') });
  await signIn(page, PIN.tess, 'welding'); await signIn(page, PIN.ray, 'matching'); await signIn(page, PIN.ivy, 'matching');
  await until(() => st.sessions.filter(s => s.event === 'start').length === 3, 'three starts');
  await tap(page, 'Ray Matcher', 'matching'); await until(() => sess(st, 'end', 'Ray Matcher').length === 1, 'Ray out');
  await page.clock.setSystemTime(Date.parse('2026-10-06T14:00:00Z'));                     // somebody set the computer's clock back an hour
  await wait(900);
  await tap(page, 'Ivy Third', 'matching'); await wait(600);
  assert.strictEqual(sess(st, 'end', 'Ivy Third').length, 1, 'with the clock an hour back, a tap on a chip must still sign that person out');
  await page.clock.setSystemTime(Date.parse('2026-10-06T16:00:00Z')); await wait(900);
  await tap(page, 'Tess Welder', 'welding'); await wait(600);
  assert.strictEqual(sess(st, 'end', 'Tess Welder').length, 1, 'and with it set forward');
  noErrors(st); await ctx.close();
}

async function c_hostileNames(browser, base) {
  const { ctx, st, page } = await context(browser, base, { time: MIDNIGHT - 6 * 60000 });
  await page.evaluate(() => { const h = document.createElement('div'); h.id = '__toastHost'; document.body.appendChild(h); });
  await signIn(page, PIN.xss, 'matching'); await signIn(page, PIN.quote, 'welding'); await signIn(page, PIN.num, 'matching'); await signIn(page, PIN.ctl, 'welding');
  await wait(400);
  assert.strictEqual(await page.evaluate(() => window.__xss), undefined, 'markup in a name ran in the page (a toast takes html)');
  assert.strictEqual(await page.evaluate(() => document.querySelectorAll('#bad, img[src="x"], .toast img, .toast b').length), 0, 'markup in a name became elements of the page');
  const shown = [...await chips(page, 'welding'), ...await chips(page, 'matching')];
  const sent = st.sessions.filter(x => x.event === 'start').map(x => x.person);
  assert.strictEqual(sent.length, 4, 'four people, four sessions: ' + JSON.stringify(sent));
  for (const n of shown) assert(!/\d{4,}/.test(n) && !/[\u0000-\u001f]/.test(n), 'a chip shows a number or control characters: ' + JSON.stringify(n));
  assert.deepStrictEqual(shown.map(n => n.toLowerCase()).sort(), sent.map(n => n.toLowerCase()).sort(), 'the chips and the sessions name the same people: ' + JSON.stringify([shown, sent]));
  // the names with no letter, or only a number, sign nobody in
  const before = st.sessions.length;
  for (const pin of [PIN.none, PIN.digits]) { if (!(await state(page)).modalOpen) await page.click('#weldAddPerson'); await typePin(page, pin); await wait(400); assert.strictEqual((await state(page)).step, false, 'a name with no letter was offered the task step'); }
  assert.strictEqual(st.sessions.length, before, 'a name with no letter signed somebody in');
  assert.deepStrictEqual([...await chips(page, 'welding'), ...await chips(page, 'matching')].sort(), shown.slice().sort(), 'and left no chip');
  assert((await state(page)).loggedIn, 'the others are still signed in (the step was left open by the refused name, closed here)');
  // one tap per chip ends that person, with the name that started the session
  await page.evaluate(() => { try { document.getElementById('weldLoginCancel').click(); } catch (_) {} });
  for (const n of shown) { for (const t of ['welding', 'matching']) if ((await chips(page, t)).includes(n)) { await tap(page, n, t); await wait(750); } }
  await until(() => sess(st, 'end').length === 4, 'four ends');
  assert.deepStrictEqual(sess(st, 'end').map(e => e.id).sort(), st.sessions.filter(x => x.event === 'start').map(x => x.id).sort(), 'every session ended once');
  await noNumbers(st, page); noErrors(st); await ctx.close();
}
async function c2_midnightOddNames(browser, base) {
  // midnight with a name the page and StationSession tidy differently: nobody is left half signed in
  for (const [pin, task] of [[PIN.num, 'matching'], [PIN.ctl, 'welding']]) {
    const w = await context(browser, base, { time: MIDNIGHT - 6 * 60000 });
    await signIn(w.page, pin, task); await until(() => w.st.sessions.some(x => x.event === 'start'), 'the start');
    await w.page.clock.runFor(8 * 60000); await wait(400);
    assert(w.st.sessions.some(x => x.event === 'end' && x.reason === 'midnight'), 'ended at midnight: ' + JSON.stringify(w.st.sessions.map(x => x.event + ':' + x.reason)));
    const s = await state(w.page); const left = [...await chips(w.page, 'welding'), ...await chips(w.page, 'matching')];
    assert(!s.loggedIn && s.modalOpen && !s.on && left.length === 0, `after midnight ${JSON.stringify(NAMES[pin])} is still on the page: ${JSON.stringify({ s, left })}`);
    assert.strictEqual(await w.page.evaluate(() => localStorage.getItem('weld_people')), '[]');
    await w.page.clock.runFor(20 * 60000); await wait(300);
    assert.strictEqual(w.st.sessions.filter(x => x.event === 'start').length, 1, 'nobody was started again by the page the next day');
    noErrors(w.st); await w.ctx.close();
  }
}

async function d_garbage(browser, base) {
  const GOOD = { name: 'Ok Person', task: 'welding', at: 5 };
  const stores = [
    ['not json', 'this is not json'], ['an object', '{"a":1}'], ['a number', '12'], ['null', 'null'], ['nested junk', '[null, 5, "x", [], {"name": {"a": 1}, "task": "welding"}, {"name": "A", "task": ["welding"]}]'],
    ['bad tasks', JSON.stringify([GOOD, { name: 'Odd Task', task: '__proto__' }, { name: 'Odd Task', task: 'constructor' }, { name: 'Odd Task', task: 'Welding' }, { name: 'Odd Task', task: '' }])],
    ['numbers and no letters', JSON.stringify([GOOD, { name: NUMBER, task: 'matching' }, { name: '- - -', task: 'matching' }, { name: '   ', task: 'welding' }, { name: NUMBER + ' ' + NUMBER, task: 'welding' }])],
    ['a huge list', JSON.stringify(Array.from({ length: 300 }, (_, i) => ({ name: 'Person ' + String.fromCharCode(65 + (i % 26)) + String.fromCharCode(65 + Math.floor(i / 26)), task: i % 2 ? 'welding' : 'matching', at: i })))],
    ['a very long name', JSON.stringify([{ name: 'L'.repeat(5000), task: 'welding' }])]
  ];
  for (const [label, value] of stores) {
    const { ctx, st, page } = await context(browser, base, { init: v => { try { if (sessionStorage.getItem('__seeded') === null) { sessionStorage.setItem('__seeded', '1'); localStorage.setItem('weld_people', v); } } catch (_) {} }, initArg: value });
    await wait(500);
    const shown = [...await chips(page, 'welding'), ...await chips(page, 'matching')];
    const started = st.sessions.filter(x => x.event === 'start');
    assert.deepStrictEqual(started.map(x => x.person).sort(), shown.map(n => n).sort().filter(n => started.some(x => x.person === n)), `${label}: a session for a chip that is not shown, or the other way round: ${JSON.stringify([shown.slice(0, 5), started.map(x => x.person).slice(0, 5)])}`);
    for (const x of started) assert(/\p{L}/u.test(x.person) && !/\d{4,}/.test(x.person) && x.person.length <= 80 && (x.task === 'welding' || x.task === 'matching'), `${label}: a session of the wrong shape ${JSON.stringify(x.person.slice(0, 30))} ${x.task}`);
    if (label === 'bad tasks') assert.deepStrictEqual(shown, ['Ok Person'], label + ': ' + JSON.stringify(shown));
    if (label === 'numbers and no letters') assert.deepStrictEqual(shown, ['Ok Person'], label + ': ' + JSON.stringify(shown));
    const s = await state(page); assert.strictEqual(s.loggedIn, shown.length > 0, `${label}: signed in only when somebody valid is listed`); if (!shown.length) assert(s.modalOpen, label + ': the number box is up');
    assert(!JSON.stringify(st.bodies).includes(NUMBER), label + ': a number was sent');
    noErrors(st); await ctx.close();
  }
}

async function e_twoTabs(browser, base) {
  const { ctx, st, page: A, open } = await context(browser, base);
  await signIn(A, PIN.tess, 'welding'); await signIn(A, PIN.ray, 'matching'); await until(() => st.sessions.filter(s => s.event === 'start').length === 2, 'two starts');
  const B = await open(); await B.waitForFunction(() => StationSession.people().length === 2, null, { timeout: 15000 }); await wait(400);
  assert.strictEqual(st.sessions.filter(s => s.event === 'start').length, 2, 'a second tab started nothing');
  assert.deepStrictEqual([await chips(B, 'welding'), await chips(B, 'matching')], [['Tess Welder'], ['Ray Matcher']], 'the second tab shows both');
  await signIn(B, PIN.ivy, 'matching'); await until(() => sess(st, 'start', 'Ivy Third').length >= 1, 'Ivy'); await wait(1500);
  assert.strictEqual(sess(st, 'start', 'Ivy Third').length, 1, 'one session for Ivy, not one per tab: ' + JSON.stringify(sess(st, 'start', 'Ivy Third').map(x => [x.id, x.computerId, x.at, x.sentAt])));
  await until(async () => (await chips(A, 'matching')).includes('Ivy Third'), 'tab one follows'); assert.strictEqual(st.sessions.filter(s => s.event === 'start').length, 3);
  await tap(A, 'Ray Matcher', 'matching'); await until(() => sess(st, 'end', 'Ray Matcher').length >= 1, 'Ray out'); await wait(1500);
  const endsOf = () => { const m = new Map(); for (const e of sess(st, 'end')) m.set(e.id, (m.get(e.id) || []).concat(e.person + ':' + e.reason)); return m; };
  await until(async () => !(await chips(B, 'matching')).includes('Ray Matcher'), 'tab two follows');
  assert.deepStrictEqual([...new Set(sess(st, 'end').map(e => e.person))], ['Ray Matcher'], 'only Ray was ended');
  await tap(B, 'Ivy Third', 'matching'); await wait(750); await tap(B, 'Tess Welder', 'welding');
  try { await until(() => endsOf().size === 3, 'everybody out'); } catch (e) {
    throw new Error(e.message + '; ends so far ' + JSON.stringify(sess(st, 'end').map(x => x.person + ':' + x.reason)) + ', chips A ' + JSON.stringify([await chips(A, 'welding'), await chips(A, 'matching')]) + ', chips B ' + JSON.stringify([await chips(B, 'welding'), await chips(B, 'matching')]));
  }
  await wait(1200);
  // each tab that holds a session ends it, so one sign-out may reach the door twice (it ends a session once: the door's own tests); never a different reason, never more than one per tab
  for (const [id, list] of endsOf()) { assert(list.length <= 2 && list.every(x => /:signOut$/.test(x)) && new Set(list).size === 1, 'one sign-out, at most one end per tab: ' + JSON.stringify(list)); }
  assert.strictEqual(endsOf().size, 3, 'three people ended, whichever tab did it');
  for (const p of [A, B]) { const s = await state(p); assert(!s.loggedIn && s.modalOpen && !s.on, 'both tabs show the number box: ' + JSON.stringify(s)); }
  assert.strictEqual(st.sessions.filter(s => s.event === 'start').length, 3, 'nobody was started again');
  await noNumbers(st, A); noErrors(st); await ctx.close();
}

async function f_midnightStep(browser, base) {
  const { ctx, st, page } = await context(browser, base, { time: MIDNIGHT - 6 * 60000 });
  await signIn(page, PIN.tess, 'welding'); await signIn(page, PIN.ray, 'matching'); await until(() => st.sessions.filter(s => s.event === 'start').length === 2, 'two starts');
  await page.click('#weldAddPerson'); await typePin(page, PIN.ivy); await page.waitForSelector('#userLoginModal.weld-task');
  await page.clock.runFor(8 * 60000); await wait(400);
  const ends = sess(st, 'end'); assert.deepStrictEqual(ends.map(e => e.person + ':' + e.reason).sort(), ['Ray Matcher:midnight', 'Tess Welder:midnight']);
  const s = await state(page); assert(!s.loggedIn && s.modalOpen && !s.step && !s.on, 'the number box, not the old step: ' + JSON.stringify(s));
  await page.evaluate(() => { document.getElementById('weldTaskWelding').click(); document.getElementById('weldTaskMatching').click(); }); await wait(500);
  assert.strictEqual(sess(st, 'start', 'Ivy Third').length, 0, 'a task button left over from before midnight signed the pending person in');
  assert.strictEqual((await state(page)).loggedIn, false); assert.strictEqual(await page.evaluate(() => localStorage.getItem('weld_people')), '[]');
  await noNumbers(st, page); noErrors(st); await ctx.close();
}

async function g_crash(browser, base) {
  const { ctx, st, page, open } = await context(browser, base);
  await signIn(page, PIN.tess, 'welding'); await signIn(page, PIN.ray, 'matching'); await until(() => st.sessions.filter(s => s.event === 'start').length === 2, 'two starts');
  const ids = st.sessions.filter(s => s.event === 'start').map(s => s.id).sort();
  let crashed = false; page.on('crash', () => { crashed = true; });
  try { await page.goto('chrome://crash', { timeout: 5000 }); } catch (_) {}
  await wait(500);
  if (!crashed) await page.close({ runBeforeUnload: false });
  const page2 = await open(); await page2.waitForFunction(() => StationSession.people().length === 2, null, { timeout: 15000 }); await wait(500);
  assert.strictEqual(st.sessions.filter(s => s.event === 'start').length, 2, `the page after a crash (${crashed ? 'renderer crashed' : 'tab closed'}) started nothing`);
  assert.strictEqual(sess(st, 'end').length, 0, 'and ended nobody');
  assert.deepStrictEqual(await page2.evaluate(() => StationSession.people().map(p => p.session).sort()), ids, 'the same two sessions');
  assert.deepStrictEqual([await chips(page2, 'welding'), await chips(page2, 'matching')], [['Tess Welder'], ['Ray Matcher']]);
  noErrors(st); await ctx.close();
}

(async () => {
  const server = await new Promise(ok => { const s = http.createServer((req, res) => {
    const f = path.join(root, decodeURIComponent(req.url.split('?')[0]));
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
  }).listen(0, '127.0.0.1', () => ok(s)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ executablePath: CHROMIUM, args: ['--no-sandbox'] });
  const only = process.env.ONLY;
  const T = [['three people: one tap, one person; double click, double touch and a held key sign out one; a stray Sign Out click ends everybody once', a_taps],
    ['the clock set back or forward after a tap on a chip: the chips still answer', b_clockBack],
    ['hostile names from the login door: nothing runs, chips and sessions agree, a refused name signs nobody in', c_hostileNames],
    ['midnight with a name that holds a number or control characters leaves nobody half signed in', c2_midnightOddNames],
    ['garbage in weld_people: the page loads and shows only what is valid', d_garbage],
    ['two tabs of one computer: one session per sign-in, one end per sign-out, both tabs follow', e_twoTabs],
    ['midnight with the "Welding or Matching?" step open', f_midnightStep],
    ['a page that crashes with two people signed in', g_crash]];
  try { for (const [name, fn] of T) if (!only || name.includes(only)) await check(name, () => fn(browser, base)); }
  finally { await browser.close(); server.close(); }
  console.log(`\nweld-1.html page: ${results.pass} passed, ${results.fail.length} failed`);
  process.exit(results.fail.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
