// ACCOUNT (Paul, 15 Oct 2026: "There is no way for a user to Log_Out and Sign_In, and it is not obvious who is currently signed in. Also remove the
// 'Sign-ins' menu and option from the UI. The Sign_In and Sign_Out process needs to be absolutely seamless, fast and easy ... really easy and fast to
// move between accounts"): the identity chip in the top bar, the Account panel (sign in, "Laser or Design?" inside the panel, switch person, sign out),
// the cross-tab follow, and the Sign-ins menu gone.
// EMPNUM (Paul, 26 Oct 2026: "for some reason anyone can switch from one user to another without putting in their password, which is a big safety
// concern"): a person signs in, or is switched to, ONLY with their 6-digit Employee Number, checked by the server (firebaseOrders {pinLogin}); a tile or a
// row only says whose number is asked, a typed or script-set name signs nobody in. Here every sign-in types the person's number (pinOf puts the name on
// the fake server's roster); tests/charm-nest/account-employee-number.cjs is the gate itself (wrong numbers, too many tries, no answer).
// Part 1 runs in node (the pure helpers). Part 2 runs the real sorter page (charm-nest-1.html with its bridge, charm-nest-role.js, station-session.js and
// charm-nest-account.js) in headless Chromium against the local fake site (bridge-server.cjs, which also answers the Employee Number); the stations' door
// (firebaseOrders: the Admin question and the session writes) is this test's own recorder, so nothing leaves the machine and nothing live is written.
//   /tmp/cn-run.sh 240 tests/charm-nest/account.cjs        (SHOTS=dir keeps the screenshots, ACCT_ONLY=name,name runs some scenarios)
'use strict';
const path = require('path'), assert = require('assert/strict'), fs = require('fs');
const root = path.join(__dirname, '../..');
const H = require(path.join(root, 'charm-nest-account.js'));
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const BS = require('./bridge-server.cjs'), { start, pinOf } = BS;
const SHOTS = process.env.SHOTS || '';
if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const wait = ms => new Promise(r => setTimeout(r, ms));
const fold = n => String(n || '').replace(/[._]/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase();
let passed = 0; const say = s => { passed++; console.log('  ok  ' + s); };

/* ───────────── 1 · node: the pure helpers ───────────── */
function part1() {
  assert.equal(H.initials('Tess W.'), 'TW'); assert.equal(H.initials('Tess'), 'T'); assert.equal(H.initials('Ana_M'), 'AM'); assert.equal(H.initials('  ray   k  '), 'RK');
  assert.equal(H.initials('Jean-Luc Picard'), 'JP', 'first and last part'); assert.equal(H.initials('Émile'), 'É'); assert.equal(H.initials('!!'), '?');
  assert.equal(H.firstName('Tess W.'), 'Tess'); assert.equal(H.firstName('Ana_M'), 'Ana'); assert.equal(H.display('Ana_M'), 'Ana M');
  assert.equal(H.hueOf('Tess W.'), H.hueOf('tess w'), 'the same person, the same colour, whatever the case'); assert.notEqual(H.hueOf('Tess'), H.hueOf('Ray')); assert(H.hueOf('x') >= 0 && H.hueOf('x') < 360);
  const at = (h, m) => new Date(2026, 9, 15, h, m);
  assert.deepEqual([at(0, 0), at(5, 59), at(11, 59)].map(H.greetingFor), ['Good morning', 'Good morning', 'Good morning']);
  assert.deepEqual([at(12, 0), at(16, 59)].map(H.greetingFor), ['Good afternoon', 'Good afternoon']);
  assert.deepEqual([at(17, 0), at(23, 59)].map(H.greetingFor), ['Good evening', 'Good evening']);
  assert.equal(H.clockOf(at(9, 12).getTime()), '9:12 am'); assert.equal(H.clockOf(at(0, 5).getTime()), '12:05 am'); assert.equal(H.clockOf(at(12, 0).getTime()), '12:00 pm'); assert.equal(H.clockOf(at(17, 45).getTime()), '5:45 pm');
  const t0 = at(9, 12).getTime();
  assert.deepEqual([0, 59e3, 60e3, 7 * 60e3, 59 * 60e3, 60 * 60e3, 125 * 60e3, 120 * 60e3].map(d => H.sinceOf(t0, t0 + d)), ['under 1 min', 'under 1 min', '1 min', '7 min', '59 min', '1 h', '2 h 5 min', '2 h']);
  assert.deepEqual([10, 60, 120, 30].map(H.spanOf), ['10 min', '1 hour', '2 hours', '30 min']);
  // the people list: newest first, one person once (case, accents, punctuation folded), at most 6, junk dropped
  const norm = s => (/\p{L}/u.test(s) ? s.trim().replace(/\s+/g, ' ') : '');
  let l = [];
  for (const n of ['Ann', 'Bea', 'Cy', 'Dee', 'Eve', 'Flo', 'Gus']) l = H.addPerson(l, n, norm, 6);
  assert.deepEqual(l, ['Gus', 'Flo', 'Eve', 'Dee', 'Cy', 'Bea'], 'six at most, newest first');
  l = H.addPerson(l, 'dee', norm, 6); assert.deepEqual(l, ['dee', 'Gus', 'Flo', 'Eve', 'Cy', 'Bea'], 'the same person again moves up, never twice');
  assert.deepEqual(H.addPerson(['Zoë'], 'ZOE', norm, 6), ['ZOE'], 'accents and case folded');
  assert.deepEqual(H.cleanPeople([1, null, {}, 'Ann', '  ', '1234', 'ann'], norm, 6), ['Ann'], 'a stored list is cleaned: only names, once');
  assert.deepEqual(H.cleanPeople('not a list', norm, 6), []);
  assert.deepEqual(H.dropPerson(['Ann', 'Bea'], 'ANN'), ['Bea']);
  say('node: initials, first name, colour, greeting, clock, time since, policy words, people list');
}

/* ───────────── 2 · the page ───────────── */
const door = { admins: ['paul'], delay: 0, delays: {}, mode: 'ok' };    // mode: 'ok' | 'down' (every request fails: offline)
async function newContext(browser, srv, opts = {}) {
  const rec = { sessions: [], events: [], asked: [], requests: [], reqs: 0 };
  const context = await browser.newContext({ viewport: opts.viewport || { width: 1440, height: 900 }, reducedMotion: opts.reducedMotion || 'no-preference', hasTouch: !!opts.touch });
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  const ok = body => ({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) });
  await context.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), async r => {
    let j = null; try { j = JSON.parse(r.request().postData() || 'null'); } catch (_) {}
    if (r.request().method() !== 'POST' || !j) return r.fallback();
    if (typeof j.stationAdmin === 'string') {
      rec.asked.push(j.stationAdmin);
      const d = door.delays[fold(j.stationAdmin)] != null ? door.delays[fold(j.stationAdmin)] : door.delay;
      if (d) await wait(d);
      if (door.mode === 'down') return r.abort('internetdisconnected');
      return r.fulfill(ok({ ok: true, admin: door.admins.includes(fold(j.stationAdmin)) }));
    }
    if (j.session || Array.isArray(j.activity) || j.live) {
      if (door.mode === 'down') return r.abort('internetdisconnected');
      if (j.session) rec.sessions.push(j.session);
      if (Array.isArray(j.activity)) for (const e of j.activity) rec.events.push(e);
      return r.fulfill(ok({ success: true, written: 1 }));
    }
    return r.fallback();
  });
  context.on('request', rq => { rec.reqs++; rec.requests.push(rq.url()); });
  await context.addInitScript(({ blocked }) => {
    try {
      localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off', sandboxStream: 'off' }));
      localStorage.setItem('cn.tour.seen', '1');
    } catch (_) {}
    window.__storage = []; window.addEventListener('storage', e => window.__storage.push(e.key + '=' + e.newValue));
    window.alert = () => {}; window.prompt = () => null;
    if (blocked) { try { Storage.prototype.setItem = function () { throw new DOMException('full', 'QuotaExceededError'); }; } catch (_) {} }
  }, { blocked: !!opts.blocked });
  return { context, rec };
}
async function openPage(ctx, srv, errors, label) {
  const page = await ctx.context.newPage();
  page.setDefaultTimeout(20000);
  page.on('pageerror', e => { errors.push((label || '') + e.message); console.error('page error:', e.message); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.CNAct && window.CNEmployee && window.StationSession && window.CNRole && window.CNAccount && window.StationActivity && CN.S.cloud.ok === true, null, { timeout: 60000 });
  return page;
}
const snap = page => page.evaluate(() => {
  const c = document.getElementById('cnAcctChip'), p = document.getElementById('cnAcctPanel');
  const cur = StationSession.current();
  return { name: CNEmployee.name(), st: CNRole.state(), role: CNRole.role(), admin: CNRole.admin(), lsName: localStorage.getItem('cn.employee'), held: CNAct.held(),
    session: cur ? { id: cur.id, person: cur.person, station: cur.station, role: cur.role } : null,
    chip: c ? { text: [(c.querySelector('.cnAcName') || {}).textContent, (c.querySelector('.cnAcPill:not([hidden])') || {}).textContent].filter(Boolean).join(' '), st: c.dataset.st, label: c.getAttribute('aria-label'), title: c.title, expanded: c.getAttribute('aria-expanded'), pill: (c.querySelector('.cnAcPill') || {}).textContent || '', pillShown: !!c.querySelector('.cnAcPill:not([hidden])') } : null,
    panel: p ? { mode: p.dataset.mode, role: p.getAttribute('role'), modal: p.getAttribute('aria-modal'), text: p.innerText.replace(/\s+/g, ' ').trim() } : null,
    bars: [...document.querySelectorAll('.cnNameBar')].map(b => ({ kind: b.dataset.kind, hidden: getComputedStyle(b).display === 'none' })) };
});
const until = async (page, fn, arg, what) => { try { await page.waitForFunction(fn, arg, { timeout: 8000 }); } catch (e) { throw new Error('timed out waiting for ' + what + ': ' + JSON.stringify(await snap(page).catch(() => '?'))); } };
const sessions = rec => rec.sessions.map(s => s.event + ':' + s.person + ':' + (s.station || '') + (s.reason ? ':' + s.reason : ''));
const ends = (rec, id) => rec.sessions.filter(s => s.event === 'end' && (!id || s.id === id));
const chipClick = page => page.click('#cnAcctChip');
const panelOpen = page => page.waitForSelector('#cnAcctPanel', { state: 'visible' });
const settleUi = () => wait(420);       // (the panel's controls take no click for 350 ms after a new question appears: a double click never answers it)
/** EMPNUM: types an Employee Number into the panel's field and presses Enter, as a person does */
async function typeNum(page, num) { await page.fill('#cnAcctPanel .cnAcIn', num); await page.press('#cnAcctPanel .cnAcIn', 'Enter'); }
/** ... the number of a person (put on the fake server's roster under that name: the page signs in the name the server answers, tidied as a typed one) */
const numIn = (page, srv, name) => typeNum(page, pinOf(srv.st, name));
/** the Employee Numbers of the roster that this browser keeps anywhere (localStorage, sessionStorage): none, ever (the number goes to the server once) */
const kept = (page, srv) => page.evaluate(nums => { const all = []; for (const st of [localStorage, sessionStorage]) for (let i = 0; i < st.length; i++) { const k = st.key(i); all.push(k + '=' + st.getItem(k)); } return nums.filter(n => all.some(x => new RegExp('(^|\\D)' + n + '(\\D|$)').test(x))); }, Object.keys(srv.st.pins || BS.PINS));
/** signs a non-Admin in through the panel: their Employee Number typed, "Laser or Design?" answered inside the panel */
async function signInAs(page, srv, name, role) {
  if (!(await page.$('#cnAcctPanel'))) { await chipClick(page); await panelOpen(page); }
  await numIn(page, srv, name);
  await until(page, () => document.querySelector('#cnAcctPanel') && document.querySelector('#cnAcctPanel').dataset.mode === 'role', null, 'the role question for ' + name);
  await settleUi();
  await page.click(`#cnAcctPanel [data-role="${role}"]`);
  await until(page, () => !document.querySelector('#cnAcctPanel') && CNRole.ready(), null, 'the sign-in of ' + name + ' to complete');
}
async function reset(rec) { door.mode = 'ok'; door.delay = 0; door.delays = {}; door.admins = ['paul']; rec.sessions.length = 0; rec.events.length = 0; rec.asked.length = 0; }

const SC = {};

/* ── A · chip states, the panel's open and close, the keyboard ── */
SC.chip = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors);
  try {
    let s = await snap(page);
    assert.equal(s.chip.st, 'none'); assert.equal(s.chip.text, 'Sign in'); assert.match(s.chip.label, /Sign in/); assert.match(s.chip.label, /Nobody is signed in/);
    const place = await page.evaluate(() => { const c = document.getElementById('cnAcctChip'), t = c.parentElement, kids = [...t.children]; return { parent: t.className, after: kids[kids.indexOf(c) - 1].id, before: kids[kids.indexOf(c) + 1].id, outlined: getComputedStyle(c).borderTopWidth, icon: !!c.querySelector('svg') }; });
    assert.deepEqual([place.parent, place.after, place.before, place.icon], ['topTools', 'btnRunMode', 'moreMenu', true], 'between the Auto switch and the Workspace menu, with the person icon');
    assert.notEqual(place.outlined, '0px', 'an outlined button');
    assert.equal(s.chip.expanded, 'false');
    assert.equal(await page.getAttribute('#cnAcctChip', 'aria-haspopup'), 'dialog');
    // nothing is open by itself and no request is made to open or close it
    await wait(2600);
    // (the page's own start-up reads - the cancel list, the pending custom saves, a version check - can still be arriving on a slow computer: the count below is taken once the page has been quiet for 1.5 s, so only what opening the panel does is counted)
    for (let q = ctx.rec.reqs, since = Date.now(), t0 = Date.now(); Date.now() - t0 < 30000 && Date.now() - since < 1500; await wait(100)) if (ctx.rec.reqs !== q) { q = ctx.rec.reqs; since = Date.now(); }
    assert.equal(await page.$('#cnAcctPanel'), null, 'the panel does not open by itself on load');
    assert.equal((await snap(page)).bars.length, 0, 'the load-time name hint is replaced by the chip: no name bar appears by itself');
    const reqs0 = ctx.rec.reqs;
    await chipClick(page); await panelOpen(page);
    s = await snap(page);
    assert.equal(s.chip.expanded, 'true'); assert.equal(s.panel.role, 'dialog'); assert.equal(s.panel.modal, 'false'); assert.equal(s.panel.mode, 'who'); assert.match(s.panel.text, /Who.s working\?/);
    assert.equal(ctx.rec.reqs, reqs0, 'opening the panel makes no request at all');
    // the field is where the focus is when nobody has signed in here before; Esc closes and the chip has the focus again
    assert.equal(await page.evaluate(() => document.activeElement.className), 'cnAcIn', 'no recent people: the Employee Number field has the focus');
    // EMPNUM (Paul, 26 Oct 2026): the field is the 6-digit Employee Number, masked, digits only; there is no name field any more
    assert.deepEqual(await page.evaluate(() => { const i = document.querySelector('#cnAcctPanel .cnAcIn'); return [i.type, i.inputMode, i.maxLength, document.querySelector('#cnAcctPanel .cnAcForm label').textContent]; }), ['password', 'numeric', 6, 'Your Employee Number'], 'the field asks for the Employee Number (masked, 6 digits)');
    await page.keyboard.press('Escape');
    assert.equal(await page.$('#cnAcctPanel'), null); assert.equal(await page.evaluate(() => document.activeElement.id), 'cnAcctChip', 'Esc returns the focus to the chip'); assert.equal((await snap(page)).chip.expanded, 'false');
    // click outside closes, the chip has the focus (nothing else took it)
    await chipClick(page); await panelOpen(page);
    await page.mouse.click(700, 500); await page.waitForSelector('#cnAcctPanel', { state: 'detached' });
    await until(page, () => document.activeElement && document.activeElement.id === 'cnAcctChip', null, 'the focus back on the chip after a click outside');
    // the chip toggles; the keyboard opens it (Enter) and Tab walks the panel
    await page.focus('#cnAcctChip'); await page.keyboard.press('Enter'); await panelOpen(page);
    assert.equal(await page.evaluate(() => document.activeElement.className), 'cnAcIn', 'opened from the keyboard: the field has the focus');
    await page.keyboard.press('Shift+Tab'); assert.equal(await page.evaluate(() => document.activeElement.id), 'cnAcctChip', 'Shift+Tab from the first control goes back to the chip (the panel stays)');
    await page.keyboard.press('Tab'); assert.equal(await page.evaluate(() => document.activeElement.className), 'cnAcIn', 'Tab from the chip goes into the panel');
    await page.keyboard.press('Tab'); assert.equal(await page.evaluate(() => document.activeElement.textContent), 'Continue');
    await page.keyboard.press('Tab');
    assert.equal(await page.$('#cnAcctPanel'), null, 'Tab past the last control puts the panel away'); assert.equal(await page.evaluate(() => document.activeElement.parentElement.id), 'moreMenu', 'and goes on to the next control of the top bar');
    await chipClick(page); await panelOpen(page); await chipClick(page); await page.waitForSelector('#cnAcctPanel', { state: 'detached' });
    // the 30 s clock runs only while the panel is open (and not otherwise)
    assert.equal(await page.evaluate(() => CNAccount.timers()), 0, 'no timer while the panel is closed');
    await chipClick(page); await panelOpen(page); assert.equal(await page.evaluate(() => CNAccount.timers()), 1, 'one timer while it is open');
    await page.keyboard.press('Escape'); assert.equal(await page.evaluate(() => CNAccount.timers()), 0, 'and none after');
    say('chip: nobody → "Sign in" (outlined, icon, between Auto and Workspace); opens at once with no request; the Employee Number field (masked); Esc / click outside / Tab; no hint, no pop-up; timer only while open');

    // the states: name with the role question open, Laser, Design, Admin (the name is the one the server answers for the number, tidied: "tess  w" reads "Tess W.")
    await chipClick(page); await panelOpen(page); await numIn(page, srv, 'tess  w');
    await until(page, () => CNRole.state() === 'ask', null, 'the role question');
    s = await snap(page);
    assert.equal(s.chip.st, 'ask'); assert.equal(s.chip.text, 'Tess Choose role'); assert.equal(s.chip.label, 'Signed in as Tess W., choosing Laser or Design. Open account menu'); assert.equal(s.panel.mode, 'role');
    await settleUi(); await page.click('#cnAcctPanel [data-role="laser"]');
    await until(page, () => CNRole.role() === 'laser' && !document.getElementById('cnAcctPanel'), null, 'Laser');
    s = await snap(page);
    assert.equal(s.chip.st, 'role'); assert.equal(s.chip.text, 'Tess Laser'); assert.equal(s.chip.label, 'Signed in as Tess W., Laser. Open account menu'); assert.equal(s.chip.title, s.chip.label); assert.equal(s.chip.pill, 'Laser');
    assert.equal(await page.evaluate(() => document.getElementById('btnRoleSwitch').textContent), 'Laser · switch to Design', 'the Workspace menu item still works');
    await page.click('#moreMenu > summary'); await page.click('#btnRoleSwitch');
    await until(page, () => CNRole.role() === 'design', null, 'Design from the Workspace item');
    s = await snap(page); assert.equal(s.chip.text, 'Tess Design'); assert.equal(s.chip.label, 'Signed in as Tess W., Design. Open account menu');
    await page.evaluate(() => { B.employee = ''; }); await until(page, () => CNRole.state() === 'none', null, 'sign-out');
    s = await snap(page); assert.equal(s.chip.text, 'Sign in', 'cleared by any path: the chip follows');
    await chipClick(page); await panelOpen(page); await numIn(page, srv, 'Paul');
    await until(page, () => CNRole.state() === 'admin' && !document.getElementById('cnAcctPanel'), null, 'the Admin');
    s = await snap(page); assert.equal(s.chip.text, 'Paul Admin'); assert.equal(s.chip.label, 'Signed in as Paul, Admin. Open account menu'); assert.equal(s.panel, null, 'the Admin is never asked: no role step');
    say('chip: name + "Choose role", Laser, Design (Workspace item still works), Admin, back to Sign in; labels say who is signed in');
  } finally { await ctx.context.close(); }
};

/* ── B · sign in (a tile, then the Employee Number): the role question inside the panel, the welcome, the session ── */
SC.signin = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    await page.evaluate(() => { localStorage.setItem('cn.people', JSON.stringify(['Tess W.', 'Ray K.', 'Dana'])); localStorage.setItem('cn.lastRole', JSON.stringify({ 'ray k': 'design' })); });
    await chipClick(page); await panelOpen(page);
    let s = await snap(page);
    assert.equal(s.panel.mode, 'who'); const tiles = await page.$$eval('#cnAcctPanel .cnAcTile .cnAcTn', t => t.map(x => x.textContent.trim())); assert.deepEqual(tiles, ['Tess W.', 'Ray K.', 'Dana'], 'the people of this computer, newest first');
    assert.equal(await page.evaluate(() => document.activeElement.classList.contains('cnAcTile')), true, 'the first tile has the focus');
    // EMPNUM (Paul, 26 Oct 2026): one click on a tile signs nobody in; it says whose Employee Number the field asks for and puts the focus in it
    await page.click('#cnAcctPanel .cnAcTile:nth-child(2)'); await wait(100);
    s = await snap(page);
    assert.equal(s.name, '', 'a tapped tile signs nobody in'); assert.equal(s.session, null); assert.equal(s.panel.mode, 'who');
    assert.deepEqual(await page.evaluate(() => [document.querySelector('#cnAcctPanel .cnAcForm label').textContent, document.querySelector('#cnAcctPanel .cnAcTile:nth-child(2)').getAttribute('aria-pressed'), document.activeElement.className]), ['Ray’s Employee Number', 'true', 'cnAcIn'], 'the tile names whose number is asked; the field has the focus');
    // his number: the same panel asks "Laser or Design?"; the name bar\'s own role bar does not also appear
    await numIn(page, srv, 'Ray K.');
    await until(page, () => document.getElementById('cnAcctPanel') && document.getElementById('cnAcctPanel').dataset.mode === 'role', null, 'the role step');
    s = await snap(page);
    assert.match(s.panel.text, /Laser or Design\?/); assert.equal(s.bars.length, 0, 'no name bar (its role bar is not shown while the panel is open)'); assert.equal(s.name, 'Ray K.'); assert.equal(s.session, null, 'nobody is signed in for the stations until the role is answered');
    assert.deepEqual(await page.$$eval('#cnAcctPanel .cnAcRole', b => b.map(x => [x.dataset.role, x.classList.contains('last')])), [['laser', false], ['design', true]], 'the last role of Ray on this computer is highlighted (never chosen for him)');
    assert.equal(await page.evaluate(() => document.activeElement.dataset.role), 'design', 'and has the focus, so Enter confirms');
    assert.equal(sessions(rec).length, 0, 'no session yet');
    // a double click (or a key held down) does not answer the question by accident
    await page.evaluate(() => document.querySelector('#cnAcctPanel [data-role="laser"]').click());
    await wait(60); assert.equal((await snap(page)).st, 'ask', 'a click in the first moments of the question answers nothing');
    await settleUi(); await page.keyboard.press('Enter');
    await until(page, () => CNRole.role() === 'design' && !document.getElementById('cnAcctPanel'), null, 'Design by Enter');
    s = await snap(page);
    assert.equal(s.chip.text, 'Ray Design'); assert.deepEqual([s.session.person, s.session.station, s.session.role], ['Ray K.', 'design', 'design']);
    // the welcome toast: first name only, by the hour; the chip settles (a class for 400 ms)
    const toast = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent));
    assert(toast.some(t => /^Good (morning|afternoon|evening), Ray\.$/.test(t)), 'welcome toast: ' + JSON.stringify(toast));
    assert.equal(await page.evaluate(() => document.getElementById('cnAcctChip').classList.contains('cnAcSettle')), true, 'the chip settles');
    assert.equal(await page.evaluate(() => document.activeElement.id), 'cnAcctChip', 'the panel is put away and the chip has the focus');
    await wait(150); assert.deepEqual(sessions(rec), ['start:Ray K.:design']);
    assert.equal(rec.asked.length >= 1, true, 'the Admin question was asked (read-only door)');
    assert.deepEqual(await page.evaluate(() => JSON.parse(localStorage.getItem('cn.lastRole'))), { 'ray k': 'design' });
    assert.deepEqual(await page.evaluate(() => JSON.parse(localStorage.getItem('cn.people'))), ['Ray K.', 'Tess W.', 'Dana'], 'the person who signed in moves to the front');
    assert.equal(await page.evaluate(() => /Ray|design|laser|admin|true/i.test(localStorage.getItem('cn.people')) && /laser|design|admin/i.test(localStorage.getItem('cn.people'))), false, 'cn.people holds names only: never a role or an Admin answer');
    assert.deepEqual(await kept(page, srv), [], 'the Employee Number is kept nowhere in this browser');
    // reload: the same sign-in goes on; no welcome (it was not a sign-in), no question
    await page.reload({ waitUntil: 'load' }); await until(page, () => window.CNAccount && CNRole.ready(), null, 'the reload');
    await wait(500); s = await snap(page); assert.equal(s.chip.text, 'Ray Design'); assert.equal(s.panel, null);
    const after = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent));
    assert(!after.some(x => /^Good (morning|afternoon|evening)/.test(x)), 'no welcome after a reload (it was not a sign-in): ' + JSON.stringify(after));
    say('sign in: a tile only names whose Employee Number is asked; the number: role step inside the panel (no name-bar role bar), last role highlighted and focused, never silent; welcome toast; session under the role; number kept nowhere; reload keeps it');
  } finally { await ctx.context.close(); }
};

/* ── C · the Admin: a delayed answer shows a spinner on the tile and never blocks typing; no role step ── */
SC.admin = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    await page.evaluate(() => localStorage.setItem('cn.people', JSON.stringify(['Paul', 'Tess W.'])));
    door.delay = 700;
    await chipClick(page); await panelOpen(page);
    await page.click('#cnAcctPanel .cnAcTile:nth-child(1)'); await numIn(page, srv, 'Paul');      // (EMPNUM: his tile, then his Employee Number)
    await until(page, () => document.querySelector('#cnAcctPanel .cnAcTile[aria-busy="true"] .cnAcSpin'), null, 'the spinner on the tile');
    assert.equal(await page.evaluate(() => document.querySelector('#cnAcctPanel .cnAcIn').disabled), false, 'typing is never blocked');
    await page.fill('#cnAcctPanel .cnAcIn', '12'); assert.equal(await page.inputValue('#cnAcctPanel .cnAcIn'), '12', 'the field takes typing while the answer is on its way');
    await until(page, () => CNRole.state() === 'admin' && !document.getElementById('cnAcctPanel'), null, 'the Admin answer');
    let s = await snap(page);
    assert.equal(s.admin, true); assert.equal(s.role, ''); assert.equal(s.chip.text, 'Paul Admin'); assert.equal(s.bars.length, 0, 'no role step, no name bar'); assert.equal(s.session.station, 'sorter', 'the Admin signs in as the sorter');
    // the panel of the Admin: stays signed in until midnight, no "Working as"
    await chipClick(page); await panelOpen(page); await wait(200);
    s = await snap(page); assert.match(s.panel.text, /Paul/); assert.match(s.panel.text, /Admin/); assert.match(s.panel.text, /Stays signed in until midnight/); assert.doesNotMatch(s.panel.text, /Working as/i); assert.match(s.panel.text, /Signed in \d{1,2}:\d{2} (am|pm) · (under 1 min|\d+ min)/);
    door.delay = 0; await page.keyboard.press('Escape');
    say('Admin: spinner on the tile during the answer, typing never blocked, no role step, "Stays signed in until midnight"');
  } finally { await ctx.context.close(); }
};

/* ── D · the panel signed in: policy line, Working as, switch person, forget, sign out ── */
SC.panel = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    await page.evaluate(() => localStorage.setItem('cn.people', JSON.stringify(['Ray K.', 'Dana', 'Eve', 'Flo', 'Gus', 'Hal'])));
    await signInAs(page, srv, 'Tess W.', 'laser');
    await chipClick(page); await panelOpen(page); await wait(250);
    let s = await snap(page);
    assert.equal(s.panel.mode, 'me'); assert.match(s.panel.text, /^TW Tess W\. Laser Signed in \d{1,2}:\d{2} (am|pm) · (under 1 min|\d+ min) Signs out by itself after 1 hour without input\./);
    assert.match(s.panel.text, /Working as Laser Design/i); assert.match(s.panel.text, /Switch person/i);
    const rows = await page.$$eval('#cnAcctPanel .cnAcRow .cnAcPick .cnAcTn', b => b.map(x => x.textContent.trim()));
    assert.equal(rows.length, 5, 'the recent people of this computer: at most 5, the current person left out'); assert(!rows.includes('Tess W.'));
    assert.equal(rows[0], 'Ray K.', 'newest first (Tess signed in last, so she is left out and Ray comes first)');
    // touch targets: every control of the panel is at least 44 px
    const small = await page.evaluate(() => [...document.querySelectorAll('#cnAcctPanel button, #cnAcctPanel input')].map(b => { const r = b.getBoundingClientRect(); return [b.className || b.tagName, Math.round(r.width), Math.round(r.height)]; }).filter(x => x[2] < 44 || x[1] < 44));
    assert.deepEqual(small, [], 'every control is at least 44 px tall and wide: ' + JSON.stringify(small));
    // Working as: Design (the same switch as the Workspace item): the session ends "switched", the one under the other role starts
    await page.click('#cnAcctPanel .cnAcSeg [data-fk="w:design"]');
    await until(page, () => CNRole.role() === 'design', null, 'Design from the panel');
    await wait(150);
    assert.deepEqual(sessions(rec), ['start:Tess W.:laser', 'end:Tess W.:laser:switched', 'start:Tess W.:design']);
    s = await snap(page); assert.equal(s.panel.mode, 'me'); assert.equal(s.chip.pill, 'Design'); assert.match(s.panel.text, /Signs out by itself after 10 min without input\./, 'Design: 10 minutes');
    // forget a name here (✕): gone from the list, from the name bar's "Continue as" too when it is the last name
    await page.click('#cnAcctPanel [data-fk="x:ray k"]');
    assert.equal((await page.evaluate(() => JSON.parse(localStorage.getItem('cn.people')))).includes('Ray K.'), false, 'forgotten'); assert.equal(await page.$('#cnAcctPanel [data-fk="p:ray k"]'), null);
    // switch person: a row only names whose Employee Number is asked (EMPNUM, Paul 26 Oct 2026: nobody is switched without it)
    await settleUi(); await wait(0);
    const n1 = rec.sessions.length;
    await page.click('#cnAcctPanel .cnAcRow:nth-child(1) .cnAcPick'); await wait(150);
    s = await snap(page); assert.equal(s.name, 'Tess W.', 'a tapped row switches nobody'); assert.equal(s.panel.mode, 'me'); assert.equal(rec.sessions.length, n1, 'and ends no session');
    assert.deepEqual(await page.evaluate(() => [document.querySelector('#cnAcctPanel .cnAcForm label').textContent, document.querySelector('#cnAcctPanel .cnAcRow:nth-child(1) .cnAcPick').getAttribute('aria-pressed'), document.activeElement.className]), ['Dana’s Employee Number', 'true', 'cnAcIn'], 'the row names whose number is asked; the field has the focus');
    // her number switches in one step: the old session ends "switched", the new one starts (non-Admin: asked Laser or Design first)
    await numIn(page, srv, 'Dana');
    await until(page, () => document.getElementById('cnAcctPanel') && document.getElementById('cnAcctPanel').dataset.mode === 'role', null, 'the role step of the new person');
    s = await snap(page); assert.equal(s.name, 'Dana'); assert.equal(s.role, '', 'the new person does not inherit the old role'); assert.equal(s.admin, false); assert.equal(s.session, null, 'and has no session until the question is answered');
    assert.deepEqual(sessions(rec), ['start:Tess W.:laser', 'end:Tess W.:laser:switched', 'start:Tess W.:design', 'end:Tess W.:design:switched'], 'the old session ended at the moment of the switch');
    await settleUi(); await page.click('#cnAcctPanel [data-role="laser"]'); await until(page, () => CNRole.role() === 'laser' && !document.getElementById('cnAcctPanel'), null, 'Dana is in');
    await wait(150); assert.equal(sessions(rec).slice(-1)[0], 'start:Dana:laser');
    // the same person again (her own number): nothing happens
    const n0 = rec.sessions.length; await chipClick(page); await panelOpen(page);
    await numIn(page, srv, 'Dana'); await wait(300);
    assert.equal(rec.sessions.length, n0, 'switching to the same person is a no-op: no end, no start'); assert.equal(await page.$('#cnAcctPanel'), null, 'and the panel is put away');
    say('panel: policy line (Laser 1 hour, Design 10 min), Working as, recents (5, current left out), touch targets ≥ 44, forget, a row only names whose number is asked, switch by the Employee Number (old ends "switched", no inherited role), same person = no-op');
  } finally { await ctx.context.close(); }
};

/* ── E · sign out: the session ends now, name/role/admin/held presses go, the panel opens, idempotent ── */
SC.signout = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    await signInAs(page, srv, 'Tess W.', 'laser'); await wait(150);
    const id = (await snap(page)).session.id;
    // something the page was doing stays exactly as it is
    await page.evaluate(() => { window.__keep = { mode: CN.S.mode, rail: document.documentElement.dataset.rail }; });
    await chipClick(page); await panelOpen(page); await wait(250); await settleUi();
    await page.click('#cnAcctPanel .cnAcOut');
    await until(page, () => CNRole.state() === 'none' && document.getElementById('cnAcctPanel') && document.getElementById('cnAcctPanel').dataset.mode === 'who', null, 'signed out and the Sign in panel');
    await wait(150);
    let s = await snap(page);
    assert.equal(s.chip.text, 'Sign in'); assert.equal(s.name, ''); assert.equal(s.lsName, null, 'cn.employee is cleared'); assert.equal(s.role, ''); assert.equal(s.admin, false); assert.equal(s.session, null);
    assert.equal(await page.evaluate(() => localStorage.getItem('cn.role')), null, 'the role is cleared with the name');
    assert.equal(ends(rec, id).length, 1); assert.equal(ends(rec, id)[0].reason, 'signOut', 'the station session ended NOW with reason signOut');
    assert.match(s.panel.text, /Who.s working\?/); assert.match(s.panel.text, /Tess W\./, 'the next person taps their name: the people of this computer are the tiles');
    const toast = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent)); assert(toast.includes('Signed out. See you, Tess.'), JSON.stringify(toast));
    assert.deepEqual(await page.evaluate(() => ({ mode: CN.S.mode, rail: document.documentElement.dataset.rail })), await page.evaluate(() => window.__keep), 'the screen and the run are left as they are');
    // idempotent: a second call, or a double click, or nobody signed in: nothing, no error, no second end
    const r = await page.evaluate(() => [CNAccount.signOut(), CNAccount.signOut()]); assert.deepEqual(r, [false, false]);
    await wait(200); assert.equal(ends(rec).length, 1, 'one end only');
    // a double click on Sign out does not sign the next tile in (the controls that appear take no click for a moment)
    await page.keyboard.press('Escape'); await signInAs(page, srv, 'Ray K.', 'design'); await wait(150);
    await chipClick(page); await panelOpen(page); await settleUi();
    const box = await page.locator('#cnAcctPanel .cnAcOut').boundingBox();
    await page.mouse.dblclick(box.x + box.width / 2, box.y + box.height / 2);
    await wait(500); s = await snap(page);
    assert.equal(s.st, 'none', 'a double click on Sign out signs out once and signs nobody in'); assert.equal(ends(rec).filter(e => e.person === 'Ray K.').length, 1);
    // held presses: with a role question open a press is kept; signing out drops it (it is never put under the next person)
    await page.keyboard.press('Escape'); await BS.signInAs(page, srv, 'Held Person'); await until(page, () => CNRole.state() === 'ask', null, 'the question');
    await page.evaluate(() => { CNAct('note', { detail: 'x' }); CNAct('note', { detail: 'y' }); }); assert.equal((await snap(page)).held, 2);
    assert.equal(await page.evaluate(() => CNAccount.signOut()), true); assert.equal((await snap(page)).held, 0, 'held presses dropped on sign-out'); assert.equal(sessions(rec).filter(x => /Held Person/.test(x)).length, 0, 'a person who never finished signing in has no session to end');
    // a double click on a tile signs nobody in (EMPNUM: it names whose number is asked, twice the same); her number asks the role question once, and a
    // second Enter right after it answers nothing (her last role's button appears under the key with the focus: it takes no press for a moment)
    await page.keyboard.press('Escape'); await page.evaluate(() => { localStorage.setItem('cn.people', JSON.stringify(['Dana', 'Eve'])); localStorage.removeItem('cn.lastEmployee'); localStorage.setItem('cn.lastRole', JSON.stringify({ dana: 'laser' })); }); await chipClick(page); await panelOpen(page); await settleUi();
    const tb = await page.locator('#cnAcctPanel .cnAcTile').first().boundingBox();
    await page.mouse.dblclick(tb.x + tb.width / 2, tb.y + tb.height / 2); await wait(250);
    s = await snap(page); assert.equal(s.name, '', 'a double click on a tile signs nobody in'); assert.equal(s.st, 'none'); assert.equal(s.panel.mode, 'who');
    assert.equal(await page.evaluate(() => document.querySelector('#cnAcctPanel .cnAcForm label').textContent), 'Dana’s Employee Number');
    await numIn(page, srv, 'Dana');
    await until(page, () => document.getElementById('cnAcctPanel') && document.getElementById('cnAcctPanel').dataset.mode === 'role' && document.activeElement && document.activeElement.dataset.role === 'laser', null, 'Dana asked, her last role focused');
    await page.keyboard.press('Enter'); await wait(150);
    s = await snap(page); assert.equal(s.name, 'Dana'); assert.equal(s.st, 'ask', 'a second Enter answers no question: the role question stays open and no role is chosen'); assert.equal(s.role, ''); assert.equal(s.panel.mode, 'role');
    await page.evaluate(() => CNAccount.signOut()); await page.keyboard.press('Escape');
    say('sign out: session ended "signOut" now, cn.employee/role/Admin/held presses cleared, screen left alone, Sign in panel opens, welcome-style toast, idempotent, double click safe (a tile signs nobody in)');
  } finally { await ctx.context.close(); }
};

/* ── F · a press while signed out still asks (the Employee Number now); it goes on once a person is signed in from the panel ── */
SC.press = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    await page.evaluate(() => { window.__ran = 0; localStorage.setItem('cn.people', JSON.stringify(['Paul'])); });
    // a press that needs a name: the name bar asks, as before (EMPNUM: for the Employee Number, masked; no one-tap "Continue as <name>")
    await page.evaluate(() => { CNEmployee.ask(() => { window.__ran++; }); });
    await page.waitForSelector('.cnNameBar[data-kind="ask"]'); assert.equal(await page.evaluate(() => window.__ran), 0);
    assert.deepEqual(await page.evaluate(() => { const b = document.querySelector('.cnNameBar[data-kind="ask"]'), c = b.querySelector('.cnNbCont'); return [b.dataset.num, b.querySelector('label b').textContent, b.querySelector('input').type, !!(c && !c.hidden)]; }), ['1', 'Your Employee Number', 'password', false], 'the bar asks for the Employee Number');
    // the person opens the panel instead: the bar steps aside (out of sight, the press still waits); a tile, then the number
    await chipClick(page); await panelOpen(page);
    let s = await snap(page); assert.deepEqual(s.bars, [{ kind: 'ask', hidden: true }], 'the name bar is out of sight while the panel is open (not two number fields)');
    await page.click('#cnAcctPanel .cnAcTile'); await wait(100); assert.equal(await page.evaluate(() => window.__ran), 0, 'a tile alone signs nobody in: the press still waits');
    await numIn(page, srv, 'Paul'); await until(page, () => CNRole.state() === 'admin', null, 'Paul');
    await until(page, () => window.__ran === 1, null, 'the press that asked to go on');
    s = await snap(page); assert.equal(s.bars.length, 0, 'the name bar is gone'); assert.equal(await page.evaluate(() => window.__ran), 1, 'and the press ran once');
    // Esc on the panel with the bar waiting: the bar comes back
    await page.evaluate(() => CNAccount.signOut()); await page.keyboard.press('Escape');
    await page.evaluate(() => { CNEmployee.ask(() => {}); }); await page.waitForSelector('.cnNameBar[data-kind="ask"]');
    await chipClick(page); await panelOpen(page); await page.keyboard.press('Escape');
    s = await snap(page); assert.deepEqual(s.bars, [{ kind: 'ask', hidden: false }], 'put the panel away and the name bar is back');
    await page.focus('.cnNameBar input'); await page.keyboard.press('Escape'); await page.waitForFunction(() => !document.querySelector('.cnNameBar'));
    // a press with the panel OPEN (the kiosk after a sign-out): the panel steps aside for the bar the press asks with
    await chipClick(page); await panelOpen(page);
    await page.evaluate(() => { CNEmployee.ask(() => { window.__ran++; }); }); await page.waitForSelector('.cnNameBar[data-kind="ask"]');
    assert.equal(await page.$('#cnAcctPanel'), null, 'a press that needs a name takes the field; the panel steps aside');
    await page.fill('.cnNameBar input', pinOf(srv.st, 'Tess W')); await page.press('.cnNameBar input', 'Enter');
    await until(page, () => window.__ran === 2, null, 'the press after the bar');
    assert.equal(await page.evaluate(() => CNEmployee.name()), 'Tess W.', 'under the person the number names');
    // a held press with nobody signed in: kept, the calm hint appears as before (only when the panel is closed)
    await page.evaluate(() => CNAccount.signOut()); await page.keyboard.press('Escape'); await page.evaluate(() => { CNAct.drop(); CNAct('note', { detail: 'z' }); });
    await page.waitForSelector('.cnNameBar[data-kind="hint"]'); assert.equal(await page.evaluate(() => CNAct.held()), 1);
    assert.equal(await page.evaluate(() => document.querySelector('.cnNameBar[data-kind="hint"]').dataset.num), '1', 'the hint asks for the Employee Number too');
    say('a press while signed out: the name bar asks (the Employee Number), parks while the panel is open, comes back with it closed, the press goes on once a person is signed in from the panel');
  } finally { await ctx.context.close(); }
};

/* ── G · the name bar's role bar is the fallback when the panel is closed ── */
SC.fallback = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    // EMPNUM (Paul, 26 Oct 2026): a name set by a script (or a dialog's old name field) signs nobody in; the panel opens and asks for the number
    await page.evaluate(() => { B.employee = 'Tess W'; }); await wait(150);
    let s = await snap(page); assert.equal(s.name, '', 'a script-set name is refused'); assert.equal(s.st, 'none'); assert.equal(s.session, null); assert.equal(s.panel && s.panel.mode, 'who', 'the panel asks instead');
    assert.match(s.panel.text, /Sign in with your Employee Number\./); assert.equal(await page.evaluate(() => localStorage.getItem('cn.employee')), null, 'and nothing is kept');
    await page.keyboard.press('Escape');
    // a person signed in elsewhere: the name bar's field (EMPNUM: their Employee Number), with the panel closed
    await page.evaluate(() => { CNEmployee.edit({}); }); await page.waitForSelector('.cnNameBar[data-kind="edit"] input');
    await page.fill('.cnNameBar input', pinOf(srv.st, 'Tess W')); await page.press('.cnNameBar input', 'Enter');
    await page.waitForSelector('.cnNameBar[data-kind="role"]');
    s = await snap(page); assert.equal(s.chip.text, 'Tess Choose role'); assert.equal(s.panel, null);
    await chipClick(page); await panelOpen(page);
    s = await snap(page); assert.equal(s.panel.mode, 'role'); assert.deepEqual(s.bars, [{ kind: 'role', hidden: true }], 'the panel asks the question; the bar waits out of sight');
    await page.keyboard.press('Escape'); s = await snap(page); assert.deepEqual(s.bars, [{ kind: 'role', hidden: false }]);
    await page.click('.cnNameBar[data-kind="role"] [data-role="design"]'); await until(page, () => CNRole.role() === 'design', null, 'Design from the bar');
    s = await snap(page); assert.equal(s.chip.text, 'Tess Design'); assert.equal(s.bars.length, 0);
    say('a script-set name signs nobody in (the panel asks for the number); role question: asked in the panel when it is open, in the name bar when it is closed; the chip follows either');
  } finally { await ctx.context.close(); }
};

/* ── H · a second tab of this computer follows a sign-out and a switch, with no echo and one end ── */
SC.tabs = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const p1 = await openPage(ctx, srv, errors, 'tab1: '); reset(rec);
  try {
    await signInAs(p1, srv, 'Tess W.', 'laser'); await wait(200);
    const id = (await snap(p1)).session.id;
    const p2 = await openPage(ctx, srv, errors, 'tab2: ');
    await until(p2, () => CNEmployee.name() === 'Tess W.' && CNRole.role() === 'laser', null, 'the second tab to have the same person');
    assert.equal((await snap(p2)).chip.text, 'Tess Laser'); await wait(1700);
    assert.equal(rec.sessions.filter(s => s.event === 'start').length, 1, 'one shared session, not one per tab: ' + JSON.stringify(sessions(rec)));
    await p1.evaluate(() => { window.__storage.length = 0; }); await p2.evaluate(() => { window.__storage.length = 0; });
    // sign-out in tab 1
    await p1.bringToFront(); await chipClick(p1); await panelOpen(p1); await settleUi(); await p1.click('#cnAcctPanel .cnAcOut');
    await until(p2, () => CNRole.state() === 'none' && document.getElementById('cnAcctChip').dataset.st === 'none', null, 'tab 2 to follow the sign-out');
    await wait(2300);
    assert.equal(await p2.evaluate(() => CNEmployee.name()), ''); assert.equal(await p2.$('#cnAcctPanel'), null, 'the other tab does not open its panel');
    assert.equal(ends(rec, id).length, 1, 'the shared session ended once, not once per tab: ' + JSON.stringify(rec.sessions.map(s => [s.event, s.reason, s.id.slice(-6)])));
    assert.deepEqual(await p1.evaluate(() => window.__storage.filter(x => /^cn\.employee=/.test(x))), [], 'no echo back to the tab that signed out');
    await p1.keyboard.press('Escape');
    // sign in again (tab 1), then a switch in tab 1 reaches tab 2; the old session ends once ("switched")
    await signInAs(p1, srv, 'Ray K.', 'design'); await wait(2300);
    await until(p2, () => CNEmployee.name() === 'Ray K.', null, 'tab 2 to adopt Ray');
    const rayId = (await snap(p1)).session.id; await p2.evaluate(() => { window.__storage.length = 0; }); await p1.evaluate(() => { window.__storage.length = 0; });
    await chipClick(p1); await panelOpen(p1); await wait(250); await settleUi();
    await p1.click('#cnAcctPanel .cnAcRow:nth-child(1) .cnAcPick'); await numIn(p1, srv, 'Tess W.');     // Tess W.: her row, her Employee Number
    await until(p1, () => document.getElementById('cnAcctPanel') && document.getElementById('cnAcctPanel').dataset.mode === 'role', null, 'the question for Tess');
    await until(p2, () => CNEmployee.name() === 'Tess W.' && document.getElementById('cnAcctChip').dataset.st !== 'none', null, 'tab 2 to follow the switch');
    await wait(2300);
    assert.equal(ends(rec, rayId).length, 1, 'Ray\'s session ended once: ' + JSON.stringify(rec.sessions.map(s => [s.event, s.person, s.reason])));
    assert.equal(ends(rec, rayId)[0].reason, 'switched');
    assert.deepEqual(await p1.evaluate(() => window.__storage.filter(x => /^cn\.employee=/.test(x))), [], 'no echo of the switch back to tab 1');
    // the role chosen in tab 1 reaches tab 2
    await settleUi(); await p1.click('#cnAcctPanel [data-role="laser"]');
    await until(p2, () => CNRole.role() === 'laser' && document.getElementById('cnAcctChip').dataset.st === 'role', null, 'tab 2 to show the role');
    await wait(2300);
    const all = rec.sessions.filter(s => s.event !== 'beat').map(s => s.event + ':' + s.person + (s.reason ? ':' + s.reason : '')).sort();
    assert.deepEqual(all, ['end:Ray K.:switched', 'end:Tess W.:signOut', 'start:Ray K.', 'start:Tess W.', 'start:Tess W.'], 'two tabs, one session per sign-in (Tess, Ray, Tess again), each ended once: ' + JSON.stringify(rec.sessions.map(s => [s.event, s.person, s.reason || '', s.id.slice(-9)])));
    assert.deepEqual(await kept(p2, srv), [], 'the other tab is told the name, never the number');
    say('two tabs: a sign-out and a switch (by the Employee Number) in one reach the other; no echo (no write loop), the shared session ends once; the other tab opens no panel');
  } finally { await ctx.context.close(); }
};

/* ── I · offline: sign out works and the end is queued; sign in waits for the Employee Number's answer; the Admin lookup fails closed ── */
SC.offline = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    await signInAs(page, srv, 'Tess W.', 'laser'); await wait(200);
    const id = (await snap(page)).session.id;
    door.mode = 'down'; await ctx.context.setOffline(true); await page.evaluate(() => window.dispatchEvent(new Event('offline')));
    // sign out offline
    await chipClick(page); await panelOpen(page); await settleUi(); await page.click('#cnAcctPanel .cnAcOut');
    await until(page, () => CNRole.state() === 'none', null, 'sign-out offline');
    assert.equal(await page.evaluate(() => localStorage.getItem('cn.employee')), null);
    await wait(300); assert.equal(await page.evaluate(() => JSON.parse(localStorage.getItem('station_session_unsent') || '[]').filter(b => b.event === 'end' && b.reason === 'signOut').length), 1, 'the end of the session is queued by StationSession');
    // sign in offline (EMPNUM, Paul 26 Oct 2026): the number cannot be checked, so nobody is signed in; one calm line says why, no alarm
    await numIn(page, srv, 'Paul');
    await until(page, () => /could not be checked/.test((document.querySelector('#cnAcctPanel .cnAcErr') || {}).textContent || ''), null, 'the no-answer line, offline');
    let s = await snap(page); assert.equal(s.name, '', 'no answer signs nobody in'); assert.equal(s.st, 'none'); assert.equal(s.panel.mode, 'who');
    assert.equal(await page.evaluate(() => document.querySelector('#toasts .toast.bad')), null, 'no alarm toast');
    // the network is back but the stations' door is not (the Admin question and the session writes fail): his number signs him in; the Admin
    // lookup is not told ("not Admin"), so the question is asked (even the Admin's name: it fails closed)
    await ctx.context.setOffline(false); await page.evaluate(() => window.dispatchEvent(new Event('online')));
    await numIn(page, srv, 'Paul');
    await until(page, () => CNRole.state() === 'ask', null, 'the role question, the door down'); assert.equal((await snap(page)).admin, false);
    await settleUi(); await page.click('#cnAcctPanel [data-role="design"]'); await until(page, () => CNRole.role() === 'design', null, 'Design, the door down');
    assert.equal((await snap(page)).chip.text, 'Paul Design');
    // the door is back: the end goes out once, the Admin question is asked again by CNRole and Paul is the Admin
    door.mode = 'ok'; await page.evaluate(() => window.dispatchEvent(new Event('online')));
    await until(page, () => CNRole.state() === 'admin', null, 'the Admin answer after the network is back');
    await page.evaluate(() => StationSession.touch && StationSession.touch()); await wait(500);
    assert(ends(rec, id).length >= 1 && ends(rec, id).every(e => e.reason === 'signOut'), 'the queued end arrived: ' + JSON.stringify(rec.sessions.map(s => [s.event, s.reason])));
    assert.equal((await snap(page)).chip.text, 'Paul Admin');
    say('offline: sign-out works with no network (end queued); the Employee Number unanswered signs nobody in (a calm line); the Admin lookup fails closed and the question is asked; the door back settles it');
  } finally { door.mode = 'ok'; await ctx.context.close(); }
};

/* ── J · a slow Admin answer for the old name never applies to the new person ── */
SC.race = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    door.delays = { paul: 900 };
    await chipClick(page); await panelOpen(page); await numIn(page, srv, 'Paul');        // Paul's Admin answer is on its way (900 ms) ...
    await until(page, () => CNRole.state() === 'checking', null, 'checking'); await wait(100);
    await numIn(page, srv, 'Tess W');                                                      // ... and the person is switched (her Employee Number) before it arrives
    await wait(1400);
    let s = await snap(page);
    assert.equal(s.name, 'Tess W.'); assert.equal(s.admin, false, 'the old lookup\'s answer (Admin) did not apply to the new person'); assert.equal(s.st, 'ask'); assert.equal(s.panel.mode, 'role');
    assert.equal(sessions(rec).filter(x => /Paul/.test(x)).length, 0, 'Paul never got a session (his answer came too late): ' + JSON.stringify(sessions(rec)));
    // rapid A → B → C: only C stays
    await page.evaluate(() => CNAccount.signOut()); await page.keyboard.press('Escape'); await signInAs(page, srv, 'Ann A', 'laser'); await wait(150);
    rec.sessions.length = 0; door.delays = { 'bea b': 900 }; door.admins = ['paul', 'bea b'];      // (B's answer, "Admin", is still on its way when C's number is in)
    await page.evaluate(([b, c]) => CNAccount.signInNumber(b).then(() => CNAccount.signInNumber(c)), [pinOf(srv.st, 'Bea B'), pinOf(srv.st, 'Cy C')]);
    await until(page, () => CNRole.state() === 'ask' && CNEmployee.name() === 'Cy C.', null, 'C asked');
    await page.evaluate(() => CNRole.choose('design')); await wait(1000);
    assert.deepEqual(sessions(rec), ['end:Ann A.:laser:switched', 'start:Cy C.:design'], 'only C stays: A ended once, B never started, one session for C: ' + JSON.stringify(sessions(rec)));
    assert.equal((await snap(page)).admin, false, 'B\'s late answer (Admin) never applies to C');
    assert.equal((await snap(page)).chip.text, 'Cy Design');
    say('a stale Admin lookup never applies to the new person; rapid A → B → C leaves one session, C\'s, with no orphan');
  } finally { await ctx.context.close(); }
};

/* ── K · the people list ── */
SC.recents = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    const people = () => page.evaluate(() => JSON.parse(localStorage.getItem('cn.people') || '[]'));
    // (each person signed in by their Employee Number: BS.signInAs types it through CNAccount.signInNumber, the name is the server's, tidied)
    for (const n of ['Ann A', 'Bea B', 'Cy C', 'Dee D', 'Eve E', 'Flo F', 'Gus G']) { await BS.signInAs(page, srv, n); await wait(30); }
    assert.deepEqual(await people(), ['Gus G.', 'Flo F.', 'Eve E.', 'Dee D.', 'Cy C.', 'Bea B.'], 'six at most, newest first (names as the name bar spells them)');
    await BS.signInAs(page, srv, 'cy c'); await wait(30);
    assert.deepEqual(await people(), ['Cy C.', 'Gus G.', 'Flo F.', 'Eve E.', 'Dee D.', 'Bea B.'], 'the same person again moves up, never twice (case folded)');
    await BS.signInAs(page, srv, 'Anna'); await wait(30);
    assert.equal((await people())[0], 'Ana_M', 'one login, one name: "Anna" is Ana_M (ONE_LOGIN)'); await BS.signInAs(page, srv, 'Ana M'); await wait(30);
    assert.equal((await people()).filter(n => n === 'Ana_M').length, 1, 'and only once');
    // the panel shows them: signed in → the others as rows (5), forget removes one
    await page.evaluate(() => CNRole.choose('laser')); await until(page, () => CNRole.role() === 'laser', null, 'a role for the last person');
    await chipClick(page); await panelOpen(page); await wait(300);
    assert.equal((await page.$$('#cnAcctPanel .cnAcRow')).length, 5);
    const first = await page.$eval('#cnAcctPanel .cnAcRow:nth-child(1) .cnAcPick .cnAcTn', b => b.textContent.trim());
    await settleUi(); await page.click('#cnAcctPanel .cnAcRow:nth-child(1) .cnAcX'); await wait(60);
    assert.equal((await people()).some(n => n.replace(/_/g, ' ') === first), false, 'forgotten: ' + first);
    // names only: nothing but strings, no role, no Admin answer, nothing else of a person
    assert((await people()).every(n => typeof n === 'string')); assert.equal(await page.evaluate(() => Object.keys(localStorage).filter(k => /^cn\.(people)$/.test(k) && /admin|laser|design/i.test(localStorage.getItem(k))).length), 0);
    await page.keyboard.press('Escape');
    // the last name used here (cn.lastEmployee, kept from before the list existed) is the first tile when the list is empty (EMPNUM: it names whose number is asked)
    await page.evaluate(() => { CNAccount.signOut(); localStorage.removeItem('cn.people'); localStorage.setItem('cn.lastEmployee', 'Old Timer'); }); await page.keyboard.press('Escape');
    await chipClick(page); await panelOpen(page);
    assert.deepEqual(await page.$$eval('#cnAcctPanel .cnAcTile .cnAcTn', t => t.map(x => x.textContent.trim())), ['Old Timer'], 'the last name used here is simply the first tile');
    say('people: names only, newest first, max 6, one person once (case, Anna = Ana_M), forget, the last name is the first tile');
  } finally { await ctx.context.close(); }
  // storage blocked or full: the list is just empty and everything works
  const ctx2 = await newContext(br, srv, { blocked: true }); const page2 = await openPage(ctx2, srv, errors); reset(ctx2.rec);
  try {
    await chipClick(page2); await panelOpen(page2); assert.equal((await page2.$$('#cnAcctPanel .cnAcTile')).length, 0, 'no list when storage is blocked');
    await numIn(page2, srv, 'Tess W'); await until(page2, () => CNRole.state() === 'ask', null, 'the question with blocked storage'); await settleUi();
    await page2.click('#cnAcctPanel [data-role="laser"]'); await until(page2, () => CNRole.role() === 'laser', null, 'Laser with blocked storage');
    assert.equal((await snap(page2)).chip.text, 'Tess Laser');
    await page2.evaluate(() => CNAccount.signOut()); await until(page2, () => CNRole.state() === 'none', null, 'sign-out with blocked storage');
    assert.equal((await snap(page2)).chip.text, 'Sign in');
    say('storage blocked or full: no list, and sign in / sign out still work');
  } finally { await ctx2.context.close(); }
};

/* ── L · the Employee Number field (EMPNUM: it took the name field's place): names, nothing, markup, a wrong number; names are text ── */
SC.names = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    await chipClick(page); await panelOpen(page);
    // EMPNUM (Paul, 26 Oct 2026): a typed name signs nobody in. The field keeps digits only, so a name, nothing, "!!!", markup or an emoji is no number; a
    // number that is not 6 digits, or not on the list, signs nobody in either. Each time one calm line in the panel says why (no toast, no element)
    const tries = [['Tess Welder', /6 digits/], ['', /6 digits/], ['   ', /6 digits/], ['!!!', /6 digits/], ['<img src=x onerror="window.__x=1">', /6 digits/], ['<b>Tess</b>', /6 digits/], ['😀', /6 digits/], ['12345', /6 digits/], ['123456', /not on the list/], ['999999', /not on the list/]];
    for (const [t, why] of tries) {
      await page.fill('#cnAcctPanel .cnAcIn', '1'); await page.fill('#cnAcctPanel .cnAcIn', t);       // (typing first: the line of the try before is put away)
      assert.equal(await page.evaluate(() => document.querySelector('#cnAcctPanel .cnAcErr').hidden), true, 'typing clears the message');
      await page.press('#cnAcctPanel .cnAcIn', 'Enter');
      await until(page, w => { const e = document.querySelector('#cnAcctPanel .cnAcErr'); return !!e && !e.hidden && new RegExp(w).test(e.textContent); }, why.source, 'the calm line for ' + JSON.stringify(t));
      const s = await snap(page);
      assert.equal(s.name, '', 'refused: ' + JSON.stringify(t)); assert.equal(s.st, 'none'); assert.equal(s.panel.mode, 'who');
      assert.deepEqual(await page.evaluate(() => { const i = document.querySelector('#cnAcctPanel .cnAcIn'); return [i.getAttribute('aria-invalid'), i.value, document.activeElement === i]; }), ['true', '', true], 'marked, the field emptied (the number is not kept even there) and ready for the next try (it has the focus)');
    }
    assert.equal(sessions(rec).length, 0, 'no sign-in, no session'); assert.equal(await page.evaluate(() => window.__x), undefined, 'markup ran nothing'); assert.equal(await page.$('#cnAcctPanel img'), null, 'and made no element');
    assert.equal(await page.evaluate(() => document.querySelector('#toasts .toast.bad')), null, 'no alarm toast for a refused number');
    // digits only, 6 at most, masked
    await page.fill('#cnAcctPanel .cnAcIn', 'a1b2c3'); assert.equal(await page.inputValue('#cnAcctPanel .cnAcIn'), '123', 'digits only');
    await page.fill('#cnAcctPanel .cnAcIn', '12345678'); assert.equal(await page.inputValue('#cnAcctPanel .cnAcIn'), '123456', '6 at most');
    await page.fill('#cnAcctPanel .cnAcIn', ''); await page.locator('#cnAcctPanel .cnAcIn').pressSequentially('12 34 56'); assert.equal(await page.inputValue('#cnAcctPanel .cnAcIn'), '123456', 'spaces typed between the digits are dropped');
    assert.deepEqual(await page.evaluate(() => { const i = document.querySelector('#cnAcctPanel .cnAcIn'); return [i.type, i.maxLength, i.autocomplete]; }), ['password', 6, 'off']);
    // the right number: the person the server names; the number is shown, kept and sent nowhere else
    const num = pinOf(srv.st, 'Tess Welder');
    await typeNum(page, num); await until(page, () => CNRole.state() === 'ask', null, 'Tess Welder');
    assert.equal(await page.evaluate(() => CNEmployee.name()), 'Tess Welder');
    const has = x => new RegExp('(^|\\D)' + num + '(\\D|$)').test(x);
    assert.deepEqual(await kept(page, srv), [], 'kept nowhere in this browser'); assert(!ctx.rec.requests.some(has), 'in no address'); assert(!has(await page.evaluate(() => document.body.innerText)), 'shown nowhere');
    await page.evaluate(() => CNAccount.signOut()); await page.keyboard.press('Escape');
    // a name with markup characters (as the server spells it) is text everywhere it is shown
    await BS.signInAs(page, srv, 'Tess <i>X</i>'); await wait(200); await chipClick(page); await panelOpen(page);
    assert.equal(await page.evaluate(() => document.querySelectorAll('#cnAcctChip i, #cnAcctPanel i').length), 0, 'no markup from a name');
    say('the Employee Number field: a name, nothing, "!!!", markup, an emoji, a short or unknown number sign nobody in, with a calm line in the panel (no toast, no element); digits only, 6 at most, masked; the number kept, sent and shown nowhere else; names are text');
  } finally { await ctx.context.close(); }
};

/* ── M · automatic sign-outs paint the chip at once ── */
SC.auto = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    await signInAs(page, srv, 'Tess W.', 'laser'); await wait(200);
    const id = (await snap(page)).session.id;
    await chipClick(page); await panelOpen(page); await wait(250);
    // the day turned (the midnight sign-out): the page's own callback clears the sign-in
    await page.evaluate(() => { const m = JSON.parse(localStorage.getItem('station_signin_days') || '{}'); m['Tess W.'] = '2020-01-01'; localStorage.setItem('station_signin_days', JSON.stringify(m)); localStorage.setItem('station_signin_day', '2020-01-01'); window.dispatchEvent(new Event('focus')); });
    await until(page, () => CNRole.state() === 'none' && document.getElementById('cnAcctChip').dataset.st === 'none', null, 'the midnight sign-out');
    const s = await snap(page);
    assert.equal(s.chip.text, 'Sign in'); assert.equal(s.panel.mode, 'who', 'an open panel turns into the Sign in panel (it does not open by itself otherwise)');
    assert.equal(ends(rec, id)[0].reason, 'midnight');
    const toast = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent).join('|')); assert.match(toast, /Signed out at midnight/);
    say('automatic sign-out (midnight): the chip and an open panel go to signed-out at once, the page\'s own notice is shown');
    // no input for a while (a Design person: 10 minutes; a Laser person: 1 hour; the Admin: never): the page's own rule signs out, the chip says so at once, nothing opens by itself
    await page.keyboard.press('Escape'); await signInAs(page, srv, 'Ray K.', 'design'); await wait(150);
    const idleId = (await snap(page)).session.id;
    await page.evaluate(() => { const real = Date.now.bind(Date); window.__off = 0; Date.now = () => real() + window.__off; });
    const later = async min => { await page.evaluate(m => { window.__off += m * 60000; window.dispatchEvent(new Event('focus')); }, min); await wait(400); };
    await later(9); assert.equal((await snap(page)).chip.text, 'Ray Design', 'nine quiet minutes: still signed in');
    await later(2);
    await until(page, () => CNRole.state() === 'none' && document.getElementById('cnAcctChip').dataset.st === 'none', null, 'the idle sign-out of a Design person');
    assert.equal(await page.$('#cnAcctPanel'), null, 'an automatic sign-out opens nothing'); assert.equal(ends(rec, idleId)[0].reason, 'idle');
    assert.match(await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent).join('|')), /Signed out after 10 minutes without input/);
    // (a Laser person's limit is 1 hour, and 30 minutes from 17:00 New York: the page's clock moves forward to the next 11:00 there when 55 quiet minutes
    //  from now would reach 17:00, or it is night, so this check does not depend on the time of day it runs; nobody is signed in while it moves. Audit, 26 Oct 2026)
    await page.evaluate(() => { const p = {}; for (const x of new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hourCycle: 'h23', hour: '2-digit', minute: '2-digit' }).formatToParts(new Date(Date.now()))) p[x.type] = +x.value || 0; const m = p.hour * 60 + p.minute; if (m < 360 || m > 15 * 60) window.__off += ((24 * 60 + 11 * 60 - m) % (24 * 60)) * 60000; window.dispatchEvent(new Event('focus')); }); await wait(400);
    await signInAs(page, srv, 'Lena L', 'laser'); await wait(150); const laserId = (await snap(page)).session.id;
    await later(30); await later(25); assert.equal((await snap(page)).chip.text, 'Lena Laser', 'a Laser person is still in after 55 quiet minutes');
    await later(10); await until(page, () => CNRole.state() === 'none', null, 'the idle sign-out of a Laser person after an hour'); assert.equal(ends(rec, laserId)[0].reason, 'idle');
    door.admins = ['paul']; await chipClick(page); await panelOpen(page); await numIn(page, srv, 'Paul'); await until(page, () => CNRole.state() === 'admin', null, 'the Admin'); await later(120);
    assert.equal((await snap(page)).chip.text, 'Paul Admin', 'the Admin gets no idle sign-out');
    say('automatic sign-outs: idle (Design 10 min, Laser 1 hour, never the Admin) paint the chip signed-out at once and open nothing');
  } finally { await ctx.context.close(); }
};

/* ── N · the load-time pulse, reduced motion ── */
SC.pulse = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const page = await openPage(ctx, srv, errors);
  try {
    await page.waitForFunction(() => document.getElementById('cnAcctChip').classList.contains('cnAcPulse'), null, { timeout: 6000 });
    // (the pulse is a ring on the chip's ::after, grown and faded on transform and opacity only: charm-nest-account.js)
    assert.equal(await page.evaluate(() => getComputedStyle(document.getElementById('cnAcctChip'), '::after').animationName), 'cnAcPulse', 'the chip pulses once');
    assert.equal(await page.$('#cnAcctPanel'), null, 'and no panel opens'); assert.equal((await snap(page)).bars.length, 0);
  } finally { await ctx.context.close(); }
  const ctx2 = await newContext(br, srv, { reducedMotion: 'reduce' }); const p2 = await openPage(ctx2, srv, errors);
  try {
    await p2.waitForFunction(() => document.getElementById('cnAcctChip').classList.contains('cnAcPulse'), null, { timeout: 6000 });
    assert.equal(await p2.evaluate(() => getComputedStyle(document.getElementById('cnAcctChip')).animationName + '|' + getComputedStyle(document.getElementById('cnAcctChip'), '::after').animationName), 'none|none', 'no animation under reduced motion');
    await signInAs(p2, srv, 'Tess W.', 'laser');
    assert.equal(await p2.evaluate(() => document.getElementById('cnAcctChip').classList.contains('cnAcSettle')), false, 'no settle animation under reduced motion');
    await chipClick(p2); await panelOpen(p2);
    assert.equal(await p2.evaluate(() => getComputedStyle(document.getElementById('cnAcctPanel')).animationName), 'none', 'the panel does not animate');
  } finally { await ctx2.context.close(); }
  say('the chip pulses once at load (no pop-up); nothing animates under reduced motion');
};

/* ── O · the Sign-ins menu and window are gone ── */
SC.signins = async (br, srv, errors) => {
  const ctx = await newContext(br, srv); const page = await openPage(ctx, srv, errors);
  try {
    await page.click('#moreMenu > summary');
    const items = await page.$$eval('#moreMenu .moreList button, #moreMenu .moreList .menuLabel', n => n.map(x => x.textContent.trim()));
    assert(!items.some(t => /sign-?ins?/i.test(t)), 'the Workspace menu has no Sign-ins: ' + JSON.stringify(items)); assert(items.some(t => /^Settings/.test(t)), 'Settings is still there');
    assert.equal(await page.$('#btnSignins'), null); assert.equal(await page.$('#dlgSignins'), null);
    assert.equal(await page.evaluate(() => typeof window.SignIns), 'undefined', 'no SignIns object');
    assert(!ctx.rec.requests.some(u => /charm-nest-signins/.test(u)), 'the script is not even requested');
    assert(!fs.existsSync(path.join(root, 'charm-nest-signins.js')), 'the file is gone');
    assert(!/charm-nest-signins/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')), 'and its manifest line');
    assert(!/charm-nest-signins|SignIns\b/.test(fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8')), 'and every mention in the page');
    assert(/charm-nest-account\.js/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')), 'the account module is in the public manifest');
    say('the Sign-ins menu entry, window, script, file and manifest line are gone (the server read stays for the portal)');
  } finally { await ctx.context.close(); }
};

/* ── P · 360 px: nothing overflows; screenshots of the panel signed out and signed in ── */
SC.narrow = async (br, srv, errors) => {
  const ctx = await newContext(br, srv, { viewport: { width: 360, height: 740 }, touch: true }); const { rec } = ctx; const page = await openPage(ctx, srv, errors); reset(rec);
  try {
    await page.evaluate(() => localStorage.setItem('cn.people', JSON.stringify(['Tess W.', 'Ray K.', 'Dana Ortiz', 'Maximilian Alexander', 'Eve', 'Flo'])));
    await page.click('#btnRail'); await wait(400);          // (the app's own queue rail is 344 px wide: on a phone it is put away, as on any narrow screen)
    const fits = async what => {
      const o = await page.evaluate(() => { const p = document.getElementById('cnAcctPanel'), r = p && p.getBoundingClientRect(); return { scrollW: document.documentElement.scrollWidth, innerW: innerWidth, panel: r && [Math.round(r.left), Math.round(r.right), Math.round(r.top), Math.round(r.bottom)], overflow: p ? p.scrollWidth - p.clientWidth : 0, chip: (() => { const c = document.getElementById('cnAcctChip').getBoundingClientRect(); return [Math.round(c.left), Math.round(c.right), Math.round(c.width), Math.round(c.height)]; })() }; });
      assert(o.scrollW <= o.innerW, what + ': no horizontal scroll of the page ' + JSON.stringify(o)); if (o.panel) { assert(o.panel[0] >= 0 && o.panel[1] <= o.innerW + 0.5, what + ': the panel is inside the screen ' + JSON.stringify(o)); assert.equal(o.overflow, 0, what + ': the panel does not scroll sideways'); }
      assert(o.chip[1] <= o.innerW && o.chip[0] >= 0, what + ': the chip is on the screen ' + JSON.stringify(o.chip)); assert(o.chip[3] >= 44 && o.chip[2] >= 44 - 1, what + ': the chip is a touch target (' + o.chip + ')');
      return o;
    };
    let o = await fits('signed out, chip');
    await chipClick(page); await panelOpen(page); await wait(450); o = await fits('signed out, panel');
    assert.equal(o.panel[3], 740, 'a bottom sheet under 560 px');
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'account-360-signed-out.png') });
    await page.click('#cnAcctPanel .cnAcTile:nth-child(1)'); await wait(150); await fits('signed out, a tile picked (her Employee Number asked)');
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'account-360-number.png') });
    await numIn(page, srv, 'Tess W.'); await until(page, () => CNRole.state() === 'ask', null, 'role'); await wait(450);
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'account-360-role.png') });
    await page.click('#cnAcctPanel [data-role="laser"]'); await until(page, () => CNRole.role() === 'laser', null, 'laser');
    await wait(300); o = await fits('signed in, chip'); assert.equal(await page.evaluate(() => getComputedStyle(document.querySelector('#cnAcctChip .cnAcName')).display), 'none', 'under 720 px only the avatar');
    assert.match(await page.getAttribute('#cnAcctChip', 'aria-label'), /Signed in as Tess W\., Laser/);
    await chipClick(page); await panelOpen(page); await wait(450); o = await fits('signed in, panel');
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'account-360-signed-in.png') });
    const small = await page.evaluate(() => [...document.querySelectorAll('#cnAcctPanel button, #cnAcctPanel input')].map(b => { const r = b.getBoundingClientRect(); return [b.className, Math.round(r.width), Math.round(r.height)]; }).filter(x => x[2] < 44 || x[1] < 44));
    assert.deepEqual(small, [], 'touch targets at 360 px: ' + JSON.stringify(small));
    // the long name does not break the tile
    await page.click('#cnAcctPanel .cnAcOut'); await wait(450); await fits('signed out again, panel');
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'account-360-signed-out-2.png') });
    await page.setViewportSize({ width: 1440, height: 900 }); await wait(300);
    if (SHOTS) { await page.screenshot({ path: path.join(SHOTS, 'account-1440-signed-out.png') }); }
    say('360 px: a bottom sheet, no horizontal scroll (the Employee Number field too), avatar-only chip with a full label, touch targets ≥ 44 (screenshots when SHOTS is set)');
  } finally { await ctx.context.close(); }
};

(async () => {
  part1();
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const srv = await start({ receipts: [] });
  const errors = [], fails = [];
  const only = (process.env.ACCT_ONLY || '').split(',').filter(Boolean);
  try {
    for (const name of Object.keys(SC)) {
      if (only.length && !only.includes(name)) continue;
      try { await SC[name](browser, srv, errors); }
      catch (e) { fails.push(name); console.error(`  FAIL ${name}: ${String(e && e.stack || e).split('\n').slice(0, 6).join('\n        ')}`); }
    }
  } finally { await browser.close().catch(() => {}); try { srv.close && srv.close(); } catch (_) {} }
  if (errors.length) { fails.push('page errors'); console.error('page errors: ' + JSON.stringify(errors.slice(0, 5))); }
  console.log(fails.length ? `\n${fails.length} FAILED: ${fails.join(', ')} (${passed} ok)` : `\nall passed (${passed} groups)`);
  process.exit(fails.length ? 1 : 0);
})();
