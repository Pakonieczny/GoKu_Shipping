// Two people at the Welding station at once (Paul, 6 Oct 2026, items 2 and 3; plans/stations-round2 C2 and R3). Fakes only:
// every request that is not the page itself is stubbed or aborted, the clock is Playwright's, nothing leaves the machine.
//   1 · the StationSession multi-person API on a small fixture page: init({multi, people, signOut(reason, who)}), signedIn adds
//       one person and never ends another, the same person in two tasks, signedOut ends that one, people(), who() (Matching,
//       latest input), touch()/lastInput(), a reload restores every session (no second start), a name the page drops is signed
//       out, midnight ends everybody and calls signOut once per person, 15 quiet minutes close one session and start it again,
//       the session id is `welding__weld-1__<person>__<task>__…`, a PIN-looking id is never sent; a single-person page is as before
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/weld-two-people.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROMIUM = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const wait = ms => new Promise(r => setTimeout(r, ms));
const json = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
async function until(fn, what, ms = 9000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(50); } }

/* ── 1 · the API ── */
const T0 = Date.parse('2026-10-06T15:00:00Z');            // 11:00 in New York (EDT)
const MIDNIGHT = Date.parse('2026-10-07T04:00:00Z');       // 00:00 on 7 Oct in New York
const ORIGIN = 'http://api.test';
async function openFixture(browser, time) {
  const sent = [];
  const fixture = `<!doctype html><meta charset="utf-8"><title>fixture</title><body>
  <script>
    window.__signOuts = []; window.__list = []; window.__lastErr = [];
    const read = () => { try { return JSON.parse(localStorage.getItem('fx_people') || '[]'); } catch (_) { return []; } };
    window.__people = () => read();
    window.__put = l => localStorage.setItem('fx_people', JSON.stringify(l));
    window.addEventListener('error', e => __lastErr.push(String(e.message)));
  </script>
  <script src="/station-session.js"></script>
  <script>
    StationSession.init({ station: 'welding', device: 'weld-1', multi: true,
      people: () => read(),
      signOut: (reason, who) => { __signOuts.push([reason, who]); __put(read().filter(p => !(p.name === who.name && p.task === who.task))); } });
  </script></body>`;
  const ctx = await browser.newContext({ viewport: { width: 900, height: 600 } });
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url()), m = r.request().method();
    if (u.origin !== ORIGIN) return r.abort();
    if (u.pathname === '/fixture.html') return r.fulfill({ status: 200, contentType: 'text/html', body: fixture });
    if (u.pathname.startsWith('/.netlify/functions/')) { if (m === 'POST') { const b = JSON.parse(r.request().postData() || '{}'); if (b.session) sent.push(b.session); } return json(r, { success: true }); }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  const page = await ctx.newPage();
  const errors = []; page.on('pageerror', e => errors.push(String(e && e.message || e)));
  await page.clock.install({ time });
  await page.goto(ORIGIN + '/fixture.html');
  const people = () => page.evaluate(() => StationSession.people().map(p => ({ name: p.name, task: p.task })));
  const kinds = () => sent.map(s => `${s.event}:${s.person}:${s.task || ''}${s.reason ? ':' + s.reason : ''}`);
  const signIn = (name, task, id) => page.evaluate(([n, t, i]) => { __put(__people().concat([{ name: n, task: t }])); StationSession.signedIn({ name: n, id: i, task: t }); }, [name, task, id]);
  const signOut = (name, task) => page.evaluate(([n, t]) => { __put(__people().filter(p => !(p.name === n && p.task === t))); StationSession.signedOut('signOut', { name: n, task: t }); }, [name, task]);
  const flush = () => page.clock.runFor(50).then(() => wait(120));
  return { ctx, page, sent, errors, people, kinds, signIn, signOut, flush };
}
async function api(browser) {
  const { ctx, page, sent, errors, people, kinds, signIn, signOut, flush } = await openFixture(browser, T0);

  // nobody yet
  assert.deepStrictEqual(await people(), []);
  assert.strictEqual(await page.evaluate(() => StationSession.who()), null);
  assert(await page.evaluate(() => StationSession.lastInput()) > 0, 'opening the page is an input');

  // Tess signs in under Welding: one session, with her task
  await signIn('Tess Welder', 'welding', '');
  await flush();
  assert.deepStrictEqual(kinds(), ['start:Tess Welder:welding']);
  assert.match(sent[0].id, /^welding__weld-1__Tess_Welder__welding__[\w]+$/, 'the session id says station, device, person and task');
  assert(sent[0].id.length <= 100 && /^[\w.:-]{8,100}$/.test(sent[0].id), 'a valid door id');
  assert.strictEqual(await page.evaluate(() => StationSession.who()), null, 'a scan is credited to the Matching person: with nobody in Matching there is none');
  assert.strictEqual((await page.evaluate(() => StationSession.who('welding'))).person, 'Tess Welder');

  // Ray signs in under Matching: Tess carries on untouched (no end, no second start for her)
  await page.clock.runFor(60000);
  await signIn('Ray Matcher', 'matching', '');
  await flush();
  assert.deepStrictEqual(kinds(), ['start:Tess Welder:welding', 'start:Ray Matcher:matching'], 'a second sign-in never ends the first');
  assert.deepStrictEqual(await people(), [{ name: 'Tess Welder', task: 'welding' }, { name: 'Ray Matcher', task: 'matching' }]);
  assert.strictEqual((await page.evaluate(() => StationSession.who())).person, 'Ray Matcher');

  // Tess is also in Matching (the same person in both tasks: two sessions); the latest input wins the scans
  await page.clock.runFor(30000);
  await signIn('Tess Welder', 'matching', '123456');                       // (an id made of digits is a PIN: never sent)
  await flush();
  assert.deepStrictEqual(kinds().slice(2), ['start:Tess Welder:matching']);
  assert.strictEqual(sent[2].employeeId, '', 'a PIN-looking id is dropped');
  assert.strictEqual(new Set(sent.slice(0, 3).map(s => s.id)).size, 3, 'three sessions, three ids');
  assert.strictEqual((await page.evaluate(() => StationSession.who())).person, 'Tess Welder', 'two in Matching: the latest input');
  await page.evaluate(() => StationSession.touch(Date.now(), { name: 'Ray Matcher', task: 'matching' }));
  assert.strictEqual((await page.evaluate(() => StationSession.who())).person, 'Ray Matcher', 'an input that is Ray\'s makes him the latest');
  assert.strictEqual((await page.evaluate(() => StationSession.people())).length, 3);
  assert(await page.evaluate(() => StationSession.people().every(p => p.device === 'weld-1' && p.since > 0 && p.lastInputAt >= p.since)));

  // Tess signs out of Matching only: that one ends, her Welding session and Ray carry on
  await page.clock.runFor(60000);
  await signOut('Tess Welder', 'matching');
  await flush();
  assert.deepStrictEqual(kinds().slice(3), ['end:Tess Welder:matching:signOut']);
  assert.deepStrictEqual(await people(), [{ name: 'Tess Welder', task: 'welding' }, { name: 'Ray Matcher', task: 'matching' }]);
  assert.strictEqual((await page.evaluate(() => StationSession.who())).person, 'Ray Matcher');

  // a reload restores both: the same sessions go on (a beat each), no second start, nothing ended
  const ids = sent.slice(0, 2).map(s => s.id), n0 = sent.length;
  await page.reload(); await page.clock.runFor(50); await wait(150);
  const after = sent.slice(n0);
  assert(after.length >= 2 && after.every(s => s.event === 'beat'), 'a reload only beats: ' + JSON.stringify(after.map(s => s.event)));
  assert.deepStrictEqual([...new Set(after.map(s => s.id))].sort(), ids.slice().sort(), 'a reload goes on with both sessions');
  assert.deepStrictEqual(await people(), [{ name: 'Tess Welder', task: 'welding' }, { name: 'Ray Matcher', task: 'matching' }]);

  // the page drops Ray (signed out in another tab): his session ends at the next look, Tess carries on
  await page.evaluate(() => __put(__people().filter(p => p.name !== 'Ray Matcher')));
  await page.clock.runFor(31000); await wait(150);
  assert(kinds().includes('end:Ray Matcher:matching:signOut'), 'a name the page no longer lists is signed out');
  assert.deepStrictEqual(await people(), [{ name: 'Tess Welder', task: 'welding' }]);

  // 20 minutes frozen: the session closed at its last beat and a new one goes on from now
  await page.evaluate(() => { StationSession.touch(); });
  const firstTess = sent.find(s => s.person === 'Tess Welder' && s.task === 'welding' && s.event === 'start').id;
  await page.clock.fastForward(20 * 60000); await page.clock.runFor(31000); await wait(150);
  assert(sent.some(s => s.id === firstTess && s.event === 'end' && s.reason === 'closed'), 'closed at its last beat');
  const restarted = sent.filter(s => s.event === 'start' && s.person === 'Tess Welder' && s.task === 'welding');
  assert.strictEqual(restarted.length, 2, 'and started again from now');
  assert.notStrictEqual(restarted[1].id, firstTess);

  assert.deepStrictEqual(errors, [], 'no page error');
  await ctx.close();

  await midnight(browser);

  // a single-person page is as before: no task, a second sign-in ends the first ("switched")
  const sent2 = [];
  const ctx2 = await browser.newContext();
  await ctx2.route(/.*/, async r => {
    const u = new URL(r.request().url());
    if (u.origin !== ORIGIN) return r.abort();
    if (u.pathname === '/single.html') return r.fulfill({ status: 200, contentType: 'text/html', body: `<!doctype html><title>s</title><script src="/station-session.js"></script><script>
      window.__who = null; StationSession.init({ station: 'welding', device: 'weld-9', person: () => window.__who, signOut: () => {} });</script>` });
    if (u.pathname.startsWith('/.netlify/functions/')) { const b = JSON.parse(r.request().postData() || '{}'); if (b.session) sent2.push(b.session); return json(r, { success: true }); }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file)) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: '' });
  });
  const p2 = await ctx2.newPage(); await p2.goto(ORIGIN + '/single.html');
  await p2.evaluate(() => { window.__who = { name: 'Ana One' }; StationSession.signedIn({ name: 'Ana One' }); });
  await p2.evaluate(() => { window.__who = { name: 'Bo Two' }; StationSession.signedIn({ name: 'Bo Two' }); });
  await wait(150);
  assert.deepStrictEqual(sent2.map(s => s.event + ':' + s.person + (s.reason ? ':' + s.reason : '')), ['start:Ana One', 'end:Ana One:switched', 'start:Bo Two'], 'single-person pages still switch');
  assert(sent2.every(s => s.task === undefined), 'no task on a single-person page');
  assert.deepStrictEqual(await p2.evaluate(() => StationSession.people().map(p => p.name)), ['Bo Two']);
  await ctx2.close();
  console.log('API: two sessions at once, never ended by a second sign-in, same person in both tasks, one sign-out, who() = Matching (latest input), reload restores all, dropped name ends, closed + restart, midnight once per person, single page unchanged');
}

/* midnight: everybody ends "midnight" (at midnight, not when it was noticed) and the page is told once per person */
async function midnight(browser) {
  const { ctx, page, sent, errors, people, signIn, flush } = await openFixture(browser, MIDNIGHT - 6 * 60000);
  await signIn('Tess Welder', 'welding', ''); await signIn('Ray Matcher', 'matching', ''); await flush();
  await page.clock.runFor(2 * 60000);
  await signIn('Tess Welder', 'matching', ''); await flush();
  await page.clock.runFor(7 * 60000); await wait(250);
  const ends = sent.filter(s => s.event === 'end');
  assert.deepStrictEqual(ends.map(s => s.person + ':' + s.task + ':' + s.reason).sort(), ['Ray Matcher:matching:midnight', 'Tess Welder:matching:midnight', 'Tess Welder:welding:midnight']);
  assert(ends.every(s => s.at === MIDNIGHT), 'each ended at midnight, not when the turn was noticed');
  const so = await page.evaluate(() => __signOuts.filter(x => x[0] === 'midnight'));
  assert.deepStrictEqual(so.map(x => x[1].name + ':' + x[1].task).sort(), ['Ray Matcher:matching', 'Tess Welder:matching', 'Tess Welder:welding'], 'signOut("midnight", who) once per person');
  assert.deepStrictEqual(await people(), []);
  assert.strictEqual(sent.filter(s => s.event === 'start').length, 3, 'nobody was started again');
  assert.deepStrictEqual(errors, []);
  await ctx.close();
}

/* ── 2 · weld-1.html: the real page, the real StationSession / StationActivity / StationTimeline / StationScanQueue ── */
const http = require('http');
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png' };
const fakePin = used => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!/^(\d)\1{5}$/.test(p) && !used.includes(p)) return p; } };
const P1 = fakePin([]), P2 = fakePin([P1]);              // made up for this run: only ever typed on the page and sent to the fake login door
const NAMES = { [P1]: 'Tess Welder', [P2]: 'Ray Matcher' };
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
const MATERIALIZE = `window.__toasts = []; window.__opens = 0; window.M = { AutoInit() {}, updateTextFields() {}, toast(o) { window.__toasts.push(String((o && o.html) || '')); },
  Modal: { init(el) { const i = { open() { i.isOpen = true; window.__opens++; }, close() { i.isOpen = false; }, isOpen: false }; if (el) el.__m = i; return i; }, getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
  FormSelect: { init(el) { const i = { destroy() {}, getSelectedValues: () => [el && el.value] }; if (el) el.__fs = i; return i; }, getInstance(el) { return el && el.__fs; } },
  Dropdown: { init() {} } };`;

async function weldContext(browser, base, o = {}) {
  const st = { sessions: [], events: [], timeline: [], doors: [], bodies: [], errors: [] };
  const ctx = await browser.newContext({ viewport: o.viewport || { width: 1500, height: 900 } });
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
      if (fn === 'firebaseOrders' && b.pinLogin !== undefined) { st.doors.push(text); return reply(NAMES[b.pinLogin] ? { ok: true, name: NAMES[b.pinLogin] } : { ok: false, error: 'not on the list' }); }
      st.bodies.push(text);
      if (fn === 'firebaseOrders' && b.session) { st.sessions.push(b.session); return reply({ success: true }); }
      if (fn === 'firebaseOrders' && Array.isArray(b.activity)) { st.events.push(...b.activity); return reply({ success: true, written: b.activity.length, duplicate: 0, refused: 0, scrubbed: 0 }); }
      if (fn === 'firebaseOrders' && Array.isArray(b.timeline)) { st.timeline.push(...b.timeline); return reply({ ok: true, ids: b.timeline.map(e => e.id) }); }
      return reply({ success: true });
    }
    if (fn === 'etsyOrderProxy') { const id = u.searchParams.get('orderId'); return reply({ receipt_id: Number(id) || 0, status: 'Paid', transactions: [{ transaction_id: 91000 + (Number(id) % 1000), title: 'Custom Stud Earrings', quantity: 2, sku: 'ST-1', variations: [] }] }); }
    if (fn === 'firebaseOrders' && u.searchParams.get('cancelCheck')) return reply({ success: true, cancelled: {}, now: Date.now() });
    if (fn === 'firebaseOrders' && /employee/i.test(u.searchParams.get('orderId') || '')) { st.roster = (st.roster || 0) + 1; return reply({ success: false, error: 'closed' }, 401); }   // the roster is never read
    if (fn === 'firebaseOrders') return reply({ success: true, data: {} });
    return reply({});
  });
  await ctx.addInitScript(() => { try { localStorage.setItem('access_token', 'test-token'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400)); } catch (_) {} });
  if (o.init) await ctx.addInitScript(o.init, o.initArg);
  const page = await ctx.newPage();
  page.on('pageerror', e => { if (!/gstatic\.com\/firebasejs|getApp/.test(String(e))) st.errors.push(String(e && e.message || e)); });
  if (o.time) await page.clock.install({ time: o.time });
  await page.goto(base + '/weld-1.html');
  await page.waitForFunction(() => window.StationSession && window.StationActivity && window.weldAfterChange && document.querySelector('#userLoginModal .station-session-pc'), null, { timeout: 20000 });
  return { ctx, page, st };
}
const chips = (page, task) => page.evaluate(t => [...document.querySelectorAll('#weldRoster .weld-group[data-task="' + t + '"] .weld-chip')].map(b => b.dataset.name), task);
const rosterState = page => page.evaluate(() => ({ on: document.getElementById('weldRoster').classList.contains('on'), loggedIn: window.isEmployeeLoggedIn === true, opens: window.__opens,
  modalOpen: !!document.getElementById('userLoginModal').__m.isOpen, step: document.getElementById('userLoginModal').classList.contains('weld-task'), add: document.getElementById('userLoginModal').classList.contains('weld-add') }));
const wait2 = async (fn, what, ms = 9000) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(50); } };

async function weld(browser, base) {
  const { ctx, page, st } = await weldContext(browser, base);
  const ev = (event, person, task, reason) => st.sessions.filter(s => s.event === event && (!person || s.person === person) && (!task || s.task === task) && (!reason || s.reason === reason));
  const typePin = async pin => {                                                       // through the real masking keys, like a person
    await page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = ''; i.value = ''; });
    await page.focus('#employeeNumberInput'); await page.keyboard.type(pin);
    await page.click('#employeeLoginBtn');
  };
  const addPerson = async (pin, task) => {
    await page.click('#weldAddPerson');
    await typePin(pin);
    await page.waitForSelector('#userLoginModal.weld-task');
    await page.click(task === 'welding' ? '#weldTaskWelding' : '#weldTaskMatching');
  };

  // nobody yet: the number box, no roster
  let r = await rosterState(page);
  assert.strictEqual(r.loggedIn, false); assert.strictEqual(r.on, false); assert(r.opens >= 1, 'the number box is open');
  const signOutShown = () => page.evaluate(() => getComputedStyle(document.getElementById('signOutBtn')).display !== 'none');

  // PIN, then the extra step: nobody is signed in until a task is chosen; the number is gone from the box
  await typePin(P1);
  await page.waitForSelector('#userLoginModal.weld-task');
  r = await rosterState(page);
  assert.strictEqual(r.loggedIn, false, 'not signed in until the task is chosen'); assert.strictEqual(st.sessions.length, 0, 'no session before the task');
  assert.match(await page.textContent('#weldTaskWho'), /^Tess Welder, which task/);
  assert.deepStrictEqual(await page.evaluate(() => ({ raw: document.getElementById('employeeNumberInput').dataset.raw, v: document.getElementById('employeeNumberInput').value })), { raw: '', v: '' }, 'the number is cleared as soon as the name is known');
  assert.strictEqual(await page.isVisible('#weldTaskWelding') && await page.isVisible('#weldTaskMatching'), true, 'two large buttons');
  assert.strictEqual(await page.isVisible('#employeeLoginBtn'), false, 'the step replaces the number box in the same window');
  // Back returns to the number box
  await page.click('#weldTaskBack');
  assert.strictEqual((await rosterState(page)).step, false); assert.strictEqual(await page.isVisible('#employeeLoginBtn'), true);
  await typePin(P1); await page.waitForSelector('#userLoginModal.weld-task');
  await page.click('#weldTaskWelding');
  await wait2(() => ev('start', 'Tess Welder', 'welding').length === 1, 'Tess\'s Welding session');
  r = await rosterState(page);
  assert.strictEqual(r.loggedIn, true); assert.strictEqual(r.on, true); assert.strictEqual(r.modalOpen, false);
  assert.deepStrictEqual([await chips(page, 'welding'), await chips(page, 'matching')], [['Tess Welder'], []]);
  assert.strictEqual(await page.textContent('#weldRoster .weld-group[data-task="matching"] .weld-none'), 'nobody');
  assert.strictEqual(await signOutShown(), true, 'one person here: the Sign Out button is still there (it ends only them)');
  assert.strictEqual(st.sessions[0].employeeId, '', 'name only'); assert.strictEqual(st.sessions[0].station, 'welding'); assert.strictEqual(st.sessions[0].device, 'weld-1');

  // Add person, then Cancel: nobody is touched
  const n1 = st.sessions.length;
  await page.click('#weldAddPerson');
  r = await rosterState(page);
  assert(r.modalOpen && r.add, 'Add person opens the number box in add mode'); assert.strictEqual(await page.textContent('#loginModalTitle'), 'Add person');
  assert.strictEqual(await page.isVisible('#weldLoginCancel'), true);
  await page.click('#weldLoginCancel');
  r = await rosterState(page);
  assert.strictEqual(r.modalOpen, false); assert.strictEqual(r.loggedIn, true); assert.deepStrictEqual(await chips(page, 'welding'), ['Tess Welder']); assert.strictEqual(st.sessions.length, n1);

  // Ray signs in under Matching: Tess carries on untouched
  await addPerson(P2, 'matching');
  await wait2(() => ev('start', 'Ray Matcher', 'matching').length === 1, 'Ray\'s Matching session');
  assert.deepStrictEqual([await chips(page, 'welding'), await chips(page, 'matching')], [['Tess Welder'], ['Ray Matcher']]);
  assert.strictEqual(ev('end').length, 0, 'a second sign-in ends nobody');
  assert.strictEqual(ev('start').length, 2);
  assert.strictEqual(await signOutShown(), false, 'two people here: the one Sign Out button gives way to the name chips');
  assert.strictEqual((await rosterState(page)).loggedIn, true);

  // Tess is also in Matching (two chips for one person)
  await addPerson(P1, 'matching');
  await wait2(() => ev('start', 'Tess Welder', 'matching').length === 1, 'Tess\'s Matching session');
  assert.deepStrictEqual([await chips(page, 'welding'), await chips(page, 'matching')], [['Tess Welder'], ['Ray Matcher', 'Tess Welder']]);
  assert.strictEqual(ev('end').length, 0);
  assert.strictEqual(new Set(ev('start').map(s => s.id)).size, 3);
  // the same person, the same task again: no change
  await addPerson(P1, 'welding');
  assert(await page.evaluate(() => window.__toasts.some(t => /already on Welding/.test(t))), 'a calm line, no second chip');
  assert.deepStrictEqual(await chips(page, 'welding'), ['Tess Welder']); assert.strictEqual(ev('start').length, 3);

  // who stamps what: the Welding person stamps welded; scans are credited to the Matching person (the latest sign-in of two)
  const who = await page.evaluate(() => ({ seal: weldSignedIn(), welding: weldPerson('welding'), matching: weldPerson('matching'), credit: StationSession.who().person, people: StationSession.people().map(p => p.name + ':' + p.task), chat: document.getElementById('employeeName').value }));
  assert.deepStrictEqual(who, { seal: 'Tess Welder', welding: 'Tess Welder', matching: 'Tess Welder', credit: 'Tess Welder', people: ['Tess Welder:welding', 'Ray Matcher:matching', 'Tess Welder:matching'], chat: 'Tess Welder' });

  // one tap on Ray's chip: Ray leaves Matching, nothing else changes, the page stays usable (no number box)
  const opens0 = (await rosterState(page)).opens;
  await page.click('#weldRoster .weld-chip[data-name="Ray Matcher"][data-task="matching"]');
  await wait2(() => ev('end', 'Ray Matcher', 'matching', 'signOut').length === 1, 'Ray\'s end');
  r = await rosterState(page);
  assert.strictEqual(r.opens, opens0, 'no number box: others are still signed in'); assert.strictEqual(r.loggedIn, true); assert.strictEqual(r.modalOpen, false);
  assert.deepStrictEqual([await chips(page, 'welding'), await chips(page, 'matching')], [['Tess Welder'], ['Tess Welder']]);
  assert.strictEqual(ev('end').length, 1, 'only Ray\'s one session ended');
  assert(await page.evaluate(() => window.__toasts.some(t => /Ray Matcher signed out of Matching\./.test(t))));
  await page.fill('#etsyOrderNumber', '3521000123');                                     // the page is usable
  assert.strictEqual(await page.inputValue('#etsyOrderNumber'), '3521000123');
  assert.strictEqual(await signOutShown(), true, 'Tess is here twice (two chips) and alone: Sign Out ends only her');

  // Ray comes back under Matching: a phone scan is credited to him (Matching), the welded seal names the Welding person (Tess)
  await addPerson(P2, 'matching');
  await wait2(() => ev('start', 'Ray Matcher', 'matching').length === 2, 'Ray\'s second Matching session');
  assert.notStrictEqual(ev('start', 'Ray Matcher', 'matching')[0].id, ev('start', 'Ray Matcher', 'matching')[1].id, 'a new session, a new id');
  assert.strictEqual(await page.evaluate(() => StationSession.who().person), 'Ray Matcher');
  assert.strictEqual(await page.evaluate(() => weldSignedIn()), 'Tess Welder');
  await page.evaluate(() => { const cbs = window.__fbSnaps['Brites_Orders/weld-scan-1']; cbs[cbs.length - 1]({ exists: true, data: () => ({ 'Order Number': '3522000011' }) }); });
  await wait2(async () => { await page.evaluate(() => window.StationActivity.flush()); return st.events.some(e => e.orderId === '3522000011') && st.timeline.some(e => e.type === 'welded' && e.orderId === '3522000011'); }, 'the scan\'s events and its welded seal', 20000);
  const scanEvents = st.events.filter(e => e.orderId === '3522000011');
  assert(scanEvents.length > 0 && scanEvents.every(e => e.person === 'Ray Matcher'), 'the scan is credited to the Matching person: ' + JSON.stringify(scanEvents.map(e => e.action + ':' + e.person)));
  assert(st.timeline.filter(e => e.type === 'welded' && e.orderId === '3522000011').every(e => e.by === 'Tess Welder'), 'the welded seal names the Welding person');

  // a double tap must not sign out the chip that moved under the finger
  const box = await page.locator('#weldRoster .weld-chip[data-name="Ray Matcher"][data-task="matching"]').boundingBox();
  const endsBefore = ev('end').length;
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2); await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  await wait(500);
  assert.strictEqual(ev('end').length, endsBefore + 1, 'two quick taps sign out one person');
  await addPerson(P2, 'matching');
  await wait2(() => ev('start', 'Ray Matcher', 'matching').length === 3, 'Ray back again');

  // a reload restores everybody: the same sessions go on, no second start, nothing ended
  const before = { starts: ev('start').length, ends: ev('end').length };
  const keep = st.sessions.filter(s => s.event === 'start' && !st.sessions.some(e => e.event === 'end' && e.id === s.id)).map(s => s.id).sort();
  await page.reload();
  await page.waitForFunction(() => window.StationSession && window.weldAfterChange && StationSession.people().length >= 3, null, { timeout: 15000 });
  await wait(300);
  assert.strictEqual(ev('start').length, before.starts, 'a reload starts nothing new'); assert.strictEqual(ev('end').length, before.ends, 'and ends nothing');
  assert.deepStrictEqual(await page.evaluate(() => StationSession.people().map(p => p.session).sort()), keep, 'the same sessions');
  assert.deepStrictEqual([await chips(page, 'welding'), await chips(page, 'matching')].map(a => a.slice().sort()), [['Tess Welder'], ['Ray Matcher', 'Tess Welder']], 'the chips are back');
  r = await rosterState(page); assert.strictEqual(r.loggedIn, true); assert.strictEqual(r.modalOpen, false);

  // everybody taps out: the last one brings the number box back
  for (const [name, task] of [['Tess Welder', 'matching'], ['Ray Matcher', 'matching'], ['Tess Welder', 'welding']]) {
    await page.click(`#weldRoster .weld-chip[data-name="${name}"][data-task="${task}"]`);
    await wait(750);
  }
  await wait2(() => ev('end').length === before.ends + 3, 'the last three ends');
  r = await rosterState(page);
  assert.strictEqual(r.loggedIn, false); assert.strictEqual(r.on, false); assert(r.modalOpen, 'the number box is back'); assert.strictEqual(r.step, false); assert.strictEqual(r.add, false);
  assert(await page.evaluate(() => window.__toasts.some(t => t === 'Signed out.')));
  assert.strictEqual(await page.evaluate(() => localStorage.getItem('weld_people')), '[]');
  assert.strictEqual(await page.evaluate(() => StationSession.people().length), 0);
  assert(st.sessions.filter(s => s.event === 'end').every(s => s.reason === 'signOut'));

  // no number anywhere but the login door
  const stored = await page.evaluate(() => JSON.stringify(Object.entries(localStorage)) + JSON.stringify(Object.entries(sessionStorage)) + window.__toasts.join('|'));
  for (const pin of [P1, P2]) {
    assert(!stored.includes(pin), 'a number in the browser storage or a toast');
    assert(!st.bodies.some(b => b.includes(pin)), 'a number sent to anything but the login door');
  }
  assert(st.doors.length >= 5 && st.doors.every(b => /^\{"pinLogin":"\d{6}"\}$/.test(b)), 'only the login door got numbers'); assert(!st.roster, 'the roster list was never read');
  assert.deepStrictEqual(st.errors, [], 'no page errors: ' + st.errors.join('; '));
  await ctx.close();
  console.log('weld-1: PIN then Welding or Matching (Back and Cancel work), two people and the same person twice, a second sign-in ends nobody, one tap on a chip signs out one task and the page stays usable, a double tap signs out one, seal = Welding person / scan = Matching person, reload restores everybody, the last one out brings the number box back, no number kept');
}

/* a login left from before this release is restored once as one Matching person; the old number key is not copied anywhere */
async function legacy(browser, base) {
  const { ctx, page, st } = await weldContext(browser, base, { init: () => { try { if (localStorage.getItem('weld_people') === null) { localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Tess Welder'); } } catch (_) {} } });
  await wait2(() => st.sessions.some(s => s.event === 'start'), 'the restored person\'s session');
  assert.deepStrictEqual([await chips(page, 'welding'), await chips(page, 'matching')], [[], ['Tess Welder']]);
  assert.strictEqual((await rosterState(page)).loggedIn, true);
  assert.strictEqual(st.sessions[0].task, 'matching'); assert.strictEqual(st.sessions[0].employeeId, '');
  assert.strictEqual(await page.evaluate(() => localStorage.getItem('weld_people').includes('123456')), false, 'the old number key is not copied');
  assert(!st.bodies.some(b => b.includes('123456')), 'and never sent');
  // signing out (the last chip) and a reload does not bring the old login back
  await page.click('#weldRoster .weld-chip');
  await wait2(() => st.sessions.some(s => s.event === 'end'), 'the end');
  await page.reload(); await page.waitForFunction(() => window.StationSession && window.weldAfterChange, null, { timeout: 15000 }); await wait(300);
  assert.strictEqual((await rosterState(page)).loggedIn, false, 'the old login does not come back after a sign-out');
  assert.deepStrictEqual(st.errors, []);
  await ctx.close();
  console.log('legacy: an old login becomes one Matching person once, the number key is not copied, a sign-out stays signed out');
}

/* midnight: two people, both ended "midnight", one number box, the work on screen stays */
async function midnightWeld(browser, base) {
  const { ctx, page, st } = await weldContext(browser, base, { time: MIDNIGHT - 6 * 60000, init: () => {
    try { if (!sessionStorage.getItem('seeded')) { sessionStorage.setItem('seeded', '1'); localStorage.setItem('weld_people', JSON.stringify([{ name: 'Tess Welder', task: 'welding', at: 1 }, { name: 'Ray Matcher', task: 'matching', at: 2 }]));
      localStorage.setItem('station_signin_day', '2026-10-06'); } } catch (_) {}
  } });
  await page.waitForFunction(() => StationSession.people().length === 2, null, { timeout: 15000 });
  await page.fill('#etsyOrderNumber', '3521000999');
  await page.clock.runFor(8 * 60000); await wait(300);
  const ends = st.sessions.filter(s => s.event === 'end');
  assert.deepStrictEqual(ends.map(s => s.person + ':' + s.task + ':' + s.reason).sort(), ['Ray Matcher:matching:midnight', 'Tess Welder:welding:midnight']);
  assert(ends.every(s => s.at === MIDNIGHT), 'each ended at midnight');
  const r = await rosterState(page);
  assert.strictEqual(r.loggedIn, false); assert(r.modalOpen, 'one number box'); assert.strictEqual(r.on, false);
  assert.strictEqual(await page.inputValue('#etsyOrderNumber'), '3521000999', 'the order typed on screen stays');
  assert.strictEqual(await page.evaluate(() => window.__toasts.filter(t => /midnight/i.test(t)).length), 1, 'one calm line, not one per person');
  assert.strictEqual(await page.evaluate(() => localStorage.getItem('weld_people')), '[]');
  assert.deepStrictEqual(st.errors, []);
  await ctx.close();
  console.log('midnight: both people end at midnight, one number box, one line, the work stays');
}

async function weldAll(browser) {
  const server = await new Promise(ok => { const s = http.createServer((req, res) => {
    const f = path.join(root, decodeURIComponent(req.url.split('?')[0]));
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
  }).listen(0, '127.0.0.1', () => ok(s)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  try { await weld(browser, base); await legacy(browser, base); await midnightWeld(browser, base); } finally { server.close(); }
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROMIUM, args: ['--no-sandbox'] });
  try {
    await api(browser);
    await weldAll(browser);
  } finally { await browser.close(); }
})().catch(e => { console.error(e); process.exit(1); });
