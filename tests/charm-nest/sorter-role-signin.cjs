// Laser or Design? in the Sorter app (charm-nest-1.html), in a browser, over the fake backend (Paul, 6 Oct 2026, item 4).
// The real page (its bridge, name bar, Library and Rose Gold scripts), the real station-session.js, station-activity.js and
// charm-nest-role.js, the test server of tests/charm-nest/bridge-server.cjs for everything else, and the stations' door
// (firebaseOrders) as the test's own recorder. The Admin question is AD2's read-only door (POST firebaseOrders { stationAdmin: name } ->
// { ok:true, admin }), answered by the test with the shape plans/stations-round2/api.md documents (and made to fail, to hang, to be slow).
// Nothing leaves the machine; no PIN is typed anywhere (a number typed as a name is refused, and is in no request).
//   1 · a non-Admin who sets a name is asked "Laser or Design?" in the same small name bar (not a pop-up, not a modal), once; until it is
//       answered nobody is signed in for the stations (no session, no event) and what is pressed is kept, then recorded under the role
//   2 · the Admin is never asked and signs in exactly as before: station `sorter`, no role, the laser marks at `laser`
//   3 · the role is the session's station (`laser` | `design`, device charm-nest-1, field `role`) and is on every event, whatever the action
//   4 · switching ("Laser · switch to Design") in the Workspace menu and in the name bar: the one session ends ("switched"), the other starts,
//       nothing else is asked; the events after it carry the new role
//   5 · a reload keeps name and role (the same session goes on, nothing is asked); sign-out (midnight / a cleared name) clears the role
//       with the name, so the next sign-in asks again
//   6 · the question never overflows at 280, 390 and 700 px; it sits inside an open window (never a pop-up on a pop-up) and blocks nothing;
//       an Admin answer that cannot be had (offline) is "not Admin" and is asked again when the network is back
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/charm-nest/sorter-role-signin.cjs
const path = require('path'), assert = require('assert'), fs = require('fs'), Module = require('module');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const SHOTS = process.env.SHOTS || '';
const { start } = require('./bridge-server.cjs');

const wait = ms => new Promise(r => setTimeout(r, ms));
const brief = e => [e.action, e.station, e.person, e.role || '', !!e.sandbox];
const PIN = '000000';   // an obviously fake number (never a real one)
const DEV = 'charm-nest-1';
if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const shot = async (page, name) => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name + '.png') }); };

const door = { admins: ['paul', 'paul k'], delay: 0, mode: 'ok' };    // the Admin door's list, its delay, and how it fails ('ok' | 'down' (503) | 'junk' (200, not an answer) | 'hang')
async function sorterPage(browser, srv, errors) {
  const rec = { reqs: [], events: [], sessions: [], adminAsked: [], live: [] };
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  const ok = body => ({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) });
  await context.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), async r => {
    const body = r.request().postData() || '';
    let j = null; try { j = JSON.parse(body || 'null'); } catch (_) {}
    if (r.request().method() === 'POST' && j && typeof j.stationAdmin === 'string') {       // AD2's door
      rec.adminAsked.push({ body, url: r.request().url(), keys: Object.keys(j) });
      if (door.delay) await wait(door.delay);
      if (door.mode === 'hang') return r.abort();
      if (door.mode === 'down') return r.fulfill({ status: 503, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ error: 'the list cannot be read' }) });
      if (door.mode === 'junk') return r.fulfill(ok({ success: true }));
      return r.fulfill(ok({ ok: true, admin: door.admins.includes(String(j.stationAdmin).replace(/[._]/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase()) }));
    }
    if (r.request().method() === 'POST' && j && (Array.isArray(j.activity) || j.session || j.live)) {
      rec.reqs.push({ body });
      if (Array.isArray(j.activity)) for (const e of j.activity) rec.events.push(e);
      if (j.session) rec.sessions.push(j.session);
      if (j.live) rec.live.push(j.live);
      return r.fulfill(ok({ success: true, written: 1 }));
    }
    return r.fallback();
  });
  await context.addInitScript(() => {
    try {
      localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off', sandboxStream: 'off' }));
      window.__prompts = [];
      window.prompt = (...a) => { window.__prompts.push(a); return null; };
    } catch (_) {}
  });
  const page = await context.newPage();
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await ready(page);
  return { context, page, rec, flush: () => page.evaluate(() => StationActivity.flush()) };
}
async function ready(page) {
  await page.waitForFunction(() => window.CN && window.Orders && window.CNAct && window.CNEmployee && window.StationActivity && window.StationSession && window.CNRole && CN.S.cloud.ok === true, null, { timeout: 60000 });
}
/** a name typed into the small name bar, as a person does (the first approval, label or decision asks for it) */
async function typeName(page, name) {
  await page.evaluate(() => { CNEmployee.edit({}); });
  await page.waitForSelector('.cnNameBar[data-kind="edit"] input');
  await page.fill('.cnNameBar input', name);
  await page.press('.cnNameBar input', 'Enter');
}
async function signOutNow(page) {
  await page.evaluate(() => { StationSession.signedOut('signOut'); try { localStorage.removeItem('cn.employee'); } catch (_) {} B.employee = ''; });
  await page.waitForFunction(() => !document.querySelector('.cnNameBar') && CNRole.state() === 'none');
}
const roleBar = '.cnNameBar[data-kind="role"]';

/** the stations' door (firebaseOrders {session}), the real handler over a Map for Firestore: a Sorter session under `laser` or `design` keeps its role;
    nothing else does (a role that is not the station, another station, junk) and a plain session is as before */
async function doorRole() {
  const docs = new Map();
  const ref = p => ({ path: p, id: p.split('/').pop(), get: async () => ({ exists: docs.has(p), data: () => docs.get(p) }) });
  const fakeDb = { collection: c => ({ doc: id => ref(c + '/' + id) }),
    runTransaction: async fn => { const w = []; const r = await fn({ get: x => x.get(), set: (x, d, o) => w.push([x.path, d, o]) }); for (const [p, d, o] of w) docs.set(p, o && o.merge ? Object.assign({}, docs.get(p) || {}, d) : d); return r; } };
  const fakeAdmin = { firestore: Object.assign(() => fakeDb, { FieldValue: { serverTimestamp: () => 'ts', delete: () => null } }) };
  const realLoad = Module._load;
  Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
  const fn = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
  Module._load = realLoad;
  const post = (session, n) => fn.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + n }, body: JSON.stringify({ session }) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
  const S = (o = {}) => Object.assign({ id: 'charm-nest-1-ABCD-k1', event: 'start', person: 'Tess Welder', station: 'laser', device: 'charm-nest-1', computerId: 'pc-ABCDEFGHJKMN', computerLabel: 'Sorter charm-nest-1 · ABCD', at: Date.now(), role: 'laser' }, o);
  const doc = id => docs.get('Station_Sessions/' + id);
  assert.strictEqual((await post(S(), 11)).status, 200); assert.strictEqual(doc('charm-nest-1-ABCD-k1').role, 'laser', 'a Laser session of the Sorter app keeps its role');
  assert.strictEqual(doc('charm-nest-1-ABCD-k1').station, 'laser');
  await post(S({ id: 'charm-nest-1-ABCD-k2', station: 'design', role: 'design' }), 12); assert.strictEqual(doc('charm-nest-1-ABCD-k2').role, 'design');
  await post(S({ id: 'charm-nest-1-ABCD-k3', station: 'laser', role: 'design' }), 13); assert.strictEqual(doc('charm-nest-1-ABCD-k3').role, undefined, 'a role that is not the station is dropped');
  await post(S({ id: 'charm-nest-1-ABCD-k4', station: 'sorter', role: 'laser' }), 14); assert.strictEqual(doc('charm-nest-1-ABCD-k4').role, undefined, 'another station keeps none (the Admin: station sorter, no role)');
  await post(S({ id: 'charm-nest-1-ABCD-k5', station: 'sorter', role: undefined }), 15); assert.strictEqual('role' in doc('charm-nest-1-ABCD-k5'), false, 'a plain session is as before');
  await post(S({ id: 'charm-nest-1-ABCD-k6', station: 'laser', role: 'boss' }), 16); assert.strictEqual(doc('charm-nest-1-ABCD-k6').role, undefined, 'junk is dropped, the session is still written');
  const end = await post(S({ event: 'end', reason: 'switched', role: undefined }), 17);
  assert.strictEqual(end.status, 200); assert.strictEqual(doc('charm-nest-1-ABCD-k1').endReason, 'switched'); assert.strictEqual(doc('charm-nest-1-ABCD-k1').role, 'laser', 'the role stays on the ended session');
  console.log('door: a Laser or Design session of the Sorter app keeps its role, nothing else does');
}

(async () => {
  await doorRole();
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const errors = [];
  const srv = await start({ receipts: [] });
  try {
    const { context, page, rec, flush } = await sorterPage(browser, srv, errors);
    const evs = () => rec.events.map(brief);
    const lastSession = (id) => rec.sessions.filter(s => !id || s.id === id);

    /* ───────────── 1 · a non-Admin is asked, once; nothing is recorded until it is answered ───────────── */
    assert.strictEqual(await page.evaluate(() => CNRole.state()), 'none');
    // a press before any name: the name bar offers itself (as before), the press is kept
    await page.evaluate(() => CNAct('note', { detail: 'before the name' }));
    await page.waitForSelector('.cnNameBar[data-kind="hint"]');
    assert.strictEqual(await page.evaluate(() => CNAct.held()), 1);
    await page.fill('.cnNameBar input', '  tess   WELDER '); await page.press('.cnNameBar input', 'Enter');
    // the name is set; the same bar now asks the one question
    await page.waitForSelector(roleBar);
    const q = await page.evaluate(() => {
      const f = document.querySelector('.cnNameBar'), b = [...f.querySelectorAll('[data-role]')];
      return { n: document.querySelectorAll('.cnNameBar').length, kind: f.dataset.kind, label: f.querySelector('.cnNbLbl').textContent, buttons: b.map(x => x.textContent), modal: !!document.querySelector('dialog:modal'), tag: f.tagName,
        name: B.employee, state: CNRole.state(), ready: CNRole.ready(), role: CNRole.role(), cls: f.className, close: !!f.querySelector('.cnNbX'), role_attr: f.getAttribute('role') };
    });
    assert.strictEqual(q.n, 1, 'one bar, not a second pop-up'); assert.strictEqual(q.tag, 'FORM'); assert.strictEqual(q.cls, 'cnNameBar', 'the very same bar component');
    assert(/Laser or Design\?/.test(q.label), q.label); assert.deepStrictEqual(q.buttons, ['Laser', 'Design']);
    assert.strictEqual(q.modal, false, 'not a modal'); assert.strictEqual(q.name, 'Tess Welder'); assert.strictEqual(q.state, 'ask'); assert.strictEqual(q.ready, false); assert.strictEqual(q.role, '');
    assert.deepStrictEqual(await page.evaluate(() => window.__prompts), [], 'no browser pop-up');
    assert.strictEqual(await page.evaluate(() => document.activeElement && document.activeElement.className), 'cnNbLbl', 'the question has the focus (not a button: Enter never answers by accident)');
    await shot(page, '1-role-question-1440');
    await page.keyboard.press('Tab');
    assert.strictEqual(await page.evaluate(() => document.activeElement && document.activeElement.dataset.role), 'laser', 'Tab reaches Laser, then Design');
    await page.keyboard.press('Tab');
    assert.strictEqual(await page.evaluate(() => document.activeElement && document.activeElement.dataset.role), 'design');
    // nobody is signed in for the stations yet: no session, no event; what is pressed is kept; the work on screen is not blocked
    await flush();
    assert.strictEqual(rec.sessions.length, 0, 'no session before the role: ' + JSON.stringify(rec.sessions));
    assert.strictEqual(await page.evaluate(() => StationActivity.who()), null);
    await page.evaluate(() => CNAct('complete', { orderId: '4176576272', parts: 2, orders: 1, detail: 'Complete Order' }));
    assert.strictEqual(await page.evaluate(() => CNAct.held()), 2, 'presses are kept, not lost, not blocked');
    assert.strictEqual((await page.$$('.cnNameBar')).length, 1, 'still one bar however many presses');
    assert.strictEqual(await page.evaluate(() => B.employee), 'Tess Welder', 'the name (and the approval it allows) went through');
    // Laser
    await page.click(roleBar + ' [data-role="laser"]');
    await page.waitForFunction(() => !document.querySelector('.cnNameBar') && StationActivity.who() && StationActivity.who().role === 'laser');
    assert.strictEqual(await page.evaluate(() => CNAct.held()), 0, 'what was pressed meanwhile is released under the role');
    await flush();
    assert.deepStrictEqual(evs(), [['note', 'laser', 'Tess Welder', 'laser', false], ['complete', 'laser', 'Tess Welder', 'laser', false]], 'recorded at the laser station, with the role: ' + JSON.stringify(evs()));
    const s1 = rec.sessions.filter(s => s.event === 'start');
    assert.strictEqual(s1.length, 1, 'one session started'); assert.deepStrictEqual([s1[0].station, s1[0].role, s1[0].device, s1[0].person], ['laser', 'laser', DEV, 'Tess Welder']);
    assert.strictEqual(await page.evaluate(() => StationSession.role()), 'laser'); assert.strictEqual(await page.evaluate(() => localStorage.getItem('cn.employee')), 'Tess Welder');
    // the Admin question: asked ONCE for this sign-in, through the read-only door, the name in the body only (never in a URL), nothing else sent
    assert.strictEqual(rec.adminAsked.length, 1, 'asked once: ' + JSON.stringify(rec.adminAsked));
    assert.deepStrictEqual([rec.adminAsked[0].body, rec.adminAsked[0].keys, /\/\.netlify\/functions\/firebaseOrders$/.test(rec.adminAsked[0].url)], ['{"stationAdmin":"Tess Welder"}', ['stationAdmin'], true], 'the door gets the name in the body and nothing else');
    assert.deepStrictEqual(await page.evaluate(() => { const r = JSON.parse(localStorage.getItem('cn.role')); return [r.name, r.admin, r.role]; }), ['Tess Welder', false, 'laser']);
    rec.events.length = 0;
    // 3 · every action is the role's: the sorter's own work, a laser mark, an undo
    await page.evaluate(() => {
      CNAct('complete', { orderId: '4176576273', parts: 1, orders: 1, detail: 'Send to Sheet' });
      CNAct('print', { orderId: '4176576273', parts: 1, detail: 'QR label' });
      CNAct('complete', { station: 'laser', parts: 5, detail: 'Library laser check' });
      CNAct('undo', { station: 'laser', parts: 5, detail: 'back to Laser cutting' });
      CNAct('note', { orderId: '4176576273', detail: 'decided: customOrder' });
    });
    await flush();
    assert.deepStrictEqual(evs().map(e => [e[0], e[1], e[3]]), [['complete', 'laser', 'laser'], ['print', 'laser', 'laser'], ['complete', 'laser', 'laser'], ['undo', 'laser', 'laser'], ['note', 'laser', 'laser']], 'every action at the role, with the role: ' + JSON.stringify(evs()));
    assert(rec.events.every(e => e.device === DEV && e.session && e.computer), 'with the device, session and computer');
    rec.events.length = 0;
    // asked once: another approval, and the same name set again, ask nothing
    await page.evaluate(() => { B.employee = 'tess welder'; });
    await wait(500);
    assert.strictEqual((await page.$$('.cnNameBar')).length, 0, 'the same person is not asked again');
    assert.strictEqual(rec.sessions.filter(s => s.event === 'start').length, 1, 'and no second session');

    /* ───────────── 4 · switching: in the Workspace menu, then in the name bar ───────────── */
    const firstId = s1[0].id;
    await page.click('#moreMenu > summary');
    const sw = await page.evaluate(() => { const b = document.getElementById('btnRoleSwitch'); const r = b.getBoundingClientRect(); return { hidden: b.hidden, text: b.textContent.trim(), shown: r.width > 0 && r.height > 0, inMenu: !!b.closest('.moreList') }; });
    assert.deepStrictEqual([sw.hidden, sw.text, sw.shown, sw.inMenu], [false, 'Laser · switch to Design', true, true], 'the quiet control is in the existing Workspace menu');
    await wait(350); await shot(page, '4-menu-switch-laser');
    await page.click('#btnRoleSwitch');
    await page.waitForFunction(() => StationActivity.who() && StationActivity.who().role === 'design');
    assert.strictEqual((await page.$$('.cnNameBar')).length, 0, 'switching asks nothing else');
    await flush();
    const ended = rec.sessions.filter(s => s.id === firstId && s.event === 'end'), starts = rec.sessions.filter(s => s.event === 'start');
    assert.strictEqual(ended.length, 1, 'the laser session ended'); assert.strictEqual(ended[0].reason, 'switched'); assert.strictEqual(ended[0].station, 'laser');
    assert.strictEqual(starts.length, 2, 'and a second one started'); assert.deepStrictEqual([starts[1].station, starts[1].role, starts[1].device, starts[1].person], ['design', 'design', DEV, 'Tess Welder']);
    assert.notStrictEqual(starts[1].id, firstId, 'each role is its own session');
    await page.evaluate(() => {
      CNAct('complete', { orderId: '4176576274', parts: 3, orders: 1, detail: 'Complete Order' });
      CNAct('note', { orderId: '4176576274', detail: 'decided: design' });
    });
    await flush();
    assert.deepStrictEqual(evs().map(e => [e[0], e[1], e[3]]), [['complete', 'design', 'design'], ['note', 'design', 'design']], 'after the switch: the other role: ' + JSON.stringify(evs()));
    assert.strictEqual(await page.evaluate(() => document.getElementById('btnRoleSwitch').textContent.trim()), 'Design · switch to Laser');
    // the live board: an open sheet is the ROLE's live card (Design here), and the switch ends the card of the role that was left
    await page.evaluate(() => CNLive.sheet('GF Sheet 2 · Set 4'));
    await page.waitForFunction(() => StationActivity.current().length === 1);
    await page.waitForFunction(() => StationActivity.current()[0].sent === true, null, { timeout: 15000 }).catch(() => {});
    assert.deepStrictEqual(await page.evaluate(() => StationActivity.current().map(c => [c.station, c.device])), [['design', 'charm-nest-1']], 'the open sheet is the Design person\'s live card');
    assert(rec.live.some(l => l.event === 'work' && l.station === 'design' && l.person === 'Tess Welder' && l.device === DEV), 'the live write names the role as its station: ' + JSON.stringify(rec.live.map(l => [l.event, l.station])));
    rec.events.length = 0;
    // the name bar has the same quiet control (it is what the name buttons open); one tap, nothing asked, the bar stays for the name
    await page.evaluate(() => { CNEmployee.edit({}); });
    await page.waitForSelector('.cnNameBar[data-kind="edit"] .cnNbSwitch:not([hidden])');
    assert.strictEqual((await page.textContent('.cnNbSwitch')).trim(), 'Design · switch to Laser');
    await shot(page, '5-namebar-switch-design');
    await page.click('.cnNbSwitch');
    await page.waitForFunction(() => StationActivity.who() && StationActivity.who().role === 'laser');
    assert.strictEqual((await page.textContent('.cnNbSwitch')).trim(), 'Laser · switch to Design', 'the control says the new state');
    assert.strictEqual(await page.evaluate(() => StationActivity.current().length), 0, 'the live card of the role that was left is ended');
    await flush(); await page.evaluate(() => new Promise(r => setTimeout(r, 400)));
    assert(rec.live.some(l => l.event === 'idle' && l.station === 'design'), 'and the board hears it: ' + JSON.stringify(rec.live.map(l => [l.event, l.station])));
    assert.strictEqual((await page.$$(roleBar)).length, 0, 'no question after a switch');
    await page.press('.cnNameBar input', 'Escape');
    await page.waitForFunction(() => !document.querySelector('.cnNameBar'));
    await flush();
    const seq = rec.sessions.filter(s => s.event === 'start' || s.event === 'end').map(s => [s.event, s.station, s.reason || '']);
    assert.deepStrictEqual(seq, [['start', 'laser', ''], ['end', 'laser', 'switched'], ['start', 'design', ''], ['end', 'design', 'switched'], ['start', 'laser', '']], 'both roles are tracked on their own: ' + JSON.stringify(seq));
    assert.strictEqual(new Set(rec.sessions.filter(s => s.event === 'start').map(s => s.id)).size, 3, 'three separate sessions');
    // (the session of the same role is a new one each time: hours of Laser and of Design never mix)

    /* ───────────── 5 · a reload keeps name and role; sign-out clears them ───────────── */
    const beforeReload = await page.evaluate(() => StationSession.current().id);
    rec.sessions.length = 0; rec.events.length = 0;
    await page.reload({ waitUntil: 'load' }); await ready(page);
    assert.strictEqual(await page.evaluate(() => [B.employee, CNRole.role(), CNRole.state(), StationSession.role()]).then(a => a.join('|')), 'Tess Welder|laser|role|laser', 'name and role kept');
    await wait(600);
    assert.strictEqual((await page.$$('.cnNameBar')).length, 0, 'nothing is asked after a reload');
    assert.strictEqual(await page.evaluate(() => StationSession.current().id), beforeReload, 'the same session goes on');
    assert.strictEqual(rec.sessions.filter(s => s.event === 'start').length, 0, 'no new session');
    assert.strictEqual(rec.adminAsked.filter(a => /Tess/.test(a.body)).length, 1, 'a reload does not ask again for a person whose role is kept');
    assert.strictEqual(await page.evaluate(() => document.getElementById('btnRoleSwitch').hidden), false);
    assert(!(await page.evaluate(() => localStorage.getItem('cn.role'))).includes('"admin":true'), 'nobody is recorded as Admin in storage');
    // a name cleared (the page's sign-out) takes the role with it; the next sign-in is asked
    await page.evaluate(() => { const day = '2026-01-01'; localStorage.setItem('station_signin_day', day); localStorage.setItem('station_signin_days', JSON.stringify({ 'Tess Welder': day })); window.dispatchEvent(new Event('focus')); });    // the real midnight path: the day is not today's
    await page.waitForFunction(() => !localStorage.getItem('cn.employee'));
    assert.strictEqual(await page.evaluate(() => localStorage.getItem('cn.role')), null, 'the role is cleared with the name (midnight)');
    assert.strictEqual(await page.evaluate(() => [CNRole.state(), CNRole.role(), StationSession.role(), StationSession.who()].join('|')), 'none|||', 'nobody is signed in');
    assert.strictEqual(await page.evaluate(() => document.getElementById('btnRoleSwitch').hidden), true, 'the menu control is gone');
    await flush();
    assert(rec.sessions.some(s => s.event === 'end' && s.reason === 'midnight' && s.station === 'laser'), 'the session ended (midnight) under its role: ' + JSON.stringify(rec.sessions));
    await typeName(page, 'tess welder');
    await page.waitForSelector(roleBar);
    assert.strictEqual(await page.evaluate(() => CNRole.state()), 'ask', 'the next sign-in asks again');
    assert.strictEqual(rec.adminAsked.filter(a => /Tess/.test(a.body)).length, 2, 'and is a new sign-in for the Admin question too');
    await page.click(roleBar + ' [data-role="design"]');
    await page.waitForFunction(() => StationSession.role() === 'design');
    // a named person who has not answered yet is signed out by the day turning as well (the name is not left for tomorrow's person)
    await page.evaluate(() => { StationSession.signedOut('signOut'); localStorage.removeItem('cn.employee'); B.employee = ''; });
    await page.waitForFunction(() => CNRole.state() === 'none');
    await page.evaluate(() => { B.employee = 'pia pending'; });
    await page.waitForSelector(roleBar);
    assert.strictEqual(await page.evaluate(() => [StationSession.who(), StationSession.role(), CNRole.state()].join('|')), '||ask', 'named, not signed in');
    await page.evaluate(() => window.dispatchEvent(new Event('focus')));                    // (the page notices the name: its sign-in day is kept)
    await page.waitForFunction(() => { try { return !!JSON.parse(localStorage.getItem('station_signin_days') || '{}')['Pia Pending']; } catch (_) { return false; } });
    await page.evaluate(() => { const day = '2026-01-01'; localStorage.setItem('station_signin_day', day); localStorage.setItem('station_signin_days', JSON.stringify({ 'Pia Pending': day })); window.dispatchEvent(new Event('focus')); });
    await page.waitForFunction(() => !localStorage.getItem('cn.employee') && !document.querySelector('.cnNameBar'));
    assert.strictEqual(await page.evaluate(() => [CNRole.state(), B.employee].join('|')), 'none|', 'the day turned: the name and the question are gone');
    await typeName(page, 'tess welder');
    await page.waitForSelector(roleBar);
    await page.click(roleBar + ' [data-role="design"]');
    await page.waitForFunction(() => StationSession.role() === 'design');
    // sign-out by clearing the name (B.employee = "") clears the role too
    await page.evaluate(() => { B.employee = ''; });
    await page.waitForFunction(() => localStorage.getItem('cn.role') === null && CNRole.state() === 'none');
    await signOutNow(page);

    /* ───────────── 2 · the Admin is never asked and signs in as before ───────────── */
    rec.sessions.length = 0; rec.events.length = 0;
    await page.evaluate(() => { B.employee = 'paul'; });
    await page.waitForFunction(() => CNRole.state() === 'admin' && StationActivity.who());
    await wait(500);
    assert.strictEqual((await page.$$('.cnNameBar')).length, 0, 'the Admin is never asked');
    assert.deepStrictEqual(await page.evaluate(() => [CNRole.admin(), CNRole.role(), StationSession.role(), StationActivity.who().station, document.getElementById('btnRoleSwitch').hidden].join('|')), 'true|||sorter|true', 'station sorter, no role, no switch control');
    await page.evaluate(() => {
      CNAct('complete', { orderId: '4176576275', parts: 2, orders: 1, detail: 'Complete Order' });
      CNAct('complete', { station: 'laser', parts: 5, detail: 'Library laser check' });
    });
    await flush();
    assert.deepStrictEqual(evs(), [['complete', 'sorter', 'Paul', '', false], ['complete', 'laser', 'Paul', '', false]], 'exactly as before: the sorter, and the laser for a laser mark; no role: ' + JSON.stringify(evs()));
    const ps = rec.sessions.find(s => s.event === 'start'); assert.deepStrictEqual([ps.station, ps.role, ps.device, ps.person], ['sorter', undefined, DEV, 'Paul']);
    rec.live.length = 0;
    await page.evaluate(() => CNLive.sheet('GF Sheet 3 · Set 1'));
    await page.waitForFunction(() => StationActivity.current().length === 1);
    assert.deepStrictEqual(await page.evaluate(() => StationActivity.current().map(c => c.station)), ['laser'], 'the Admin\'s open sheet is the laser\'s card, as before');
    await page.evaluate(() => CNLive.close('laser'));
    assert.strictEqual(await page.evaluate(() => { CNEmployee.edit({}); return true; }), true);
    await page.waitForSelector('.cnNameBar[data-kind="edit"]');
    assert.strictEqual(await page.evaluate(() => document.querySelector('.cnNbSwitch').hidden), true, 'no switch in the name bar for the Admin');
    await page.press('.cnNameBar input', 'Escape');
    // a reload keeps the Admin's sign-in, with nothing asked, and the answer is not asked again
    rec.sessions.length = 0;
    await page.reload({ waitUntil: 'load' }); await ready(page);
    await wait(500);
    assert.strictEqual(await page.evaluate(() => [CNRole.state(), StationSession.who() && StationSession.who().station].join('|')), 'admin|sorter');
    assert.strictEqual((await page.$$('.cnNameBar')).length, 0);
    assert.strictEqual(rec.sessions.filter(s => s.event === 'start').length, 0, 'the same session goes on');
    assert.strictEqual(rec.adminAsked.filter(a => /Paul/.test(a.body)).length, 2, 'the answer is in memory only: a reload asks it once more, quietly');
    assert(!(await page.evaluate(() => localStorage.getItem('cn.role') || '')).includes('admin":true'), 'and it is kept nowhere');
    await signOutNow(page);
    // a slow answer shows a small labelled spinner in the same bar, then the question (or nothing for the Admin)
    door.delay = 900;
    await page.evaluate(() => { B.employee = 'dana designer'; });
    await page.waitForSelector('.cnNameBar[data-kind="wait"] .spin');
    assert.match(await page.textContent('.cnNameBar'), /Checking your sign-in/);
    assert.strictEqual(await page.evaluate(() => document.querySelectorAll('.cnNameBar').length), 1);
    await shot(page, '6-checking-spinner');
    await page.waitForSelector(roleBar);
    assert.strictEqual(await page.evaluate(() => document.querySelectorAll('.cnNameBar').length), 1, 'the spinner became the question in the same bar');
    door.delay = 0;
    await signOutNow(page);

    /* ───────────── 6 · an unknown answer is not Admin and is asked again when the network is back ───────────── */
    {
      door.mode = 'down';
      await page.evaluate(() => { B.employee = 'paul'; });
      await page.waitForSelector(roleBar);
      await page.click(roleBar + ' [data-role="laser"]');
      await page.waitForFunction(() => StationSession.role() === 'laser');
      assert.strictEqual(await page.evaluate(() => JSON.parse(localStorage.getItem('cn.role')).unk), true, 'the pick says the Admin answer was not had');
      door.mode = 'ok'; rec.sessions.length = 0;
      await page.reload({ waitUntil: 'load' }); await ready(page);
      await page.waitForFunction(() => CNRole.state() === 'admin' && StationSession.who() && StationSession.who().station === 'sorter');
      assert.strictEqual(await page.evaluate(() => localStorage.getItem('cn.role')), null, 'the Admin has no role');
      await flush();
      assert.deepStrictEqual(rec.sessions.filter(s => s.event === 'start' || s.event === 'end').map(s => [s.event, s.station, s.reason || '']), [['end', 'laser', 'switched'], ['start', 'sorter', '']], 'the Admin who could not be told at sign-in is the Admin at the next load');
      await signOutNow(page);
    }
    for (const mode of ['down', 'junk']) {
      door.mode = mode;
      await page.evaluate(() => { B.employee = 'paul'; });
      await page.waitForSelector(roleBar);                       // (fail closed: the Admin who could not be told is asked, like everyone)
      assert.strictEqual(await page.evaluate(() => [CNRole.state(), CNRole.unknown(), StationSession.who()].join('|')), 'ask|true|', mode);
      await signOutNow(page);
    }
    door.mode = 'hang'; door.delay = 0;
    await page.evaluate(() => { B.employee = 'paul'; });
    await page.waitForSelector(roleBar, { timeout: 15000 });   // (no answer in six seconds is no answer)
    assert.strictEqual(await page.evaluate(() => [CNRole.state(), CNRole.unknown()].join('|')), 'ask|true', 'hang');
    door.mode = 'ok';
    await page.evaluate(() => { window.dispatchEvent(new Event('online')); });
    await page.waitForFunction(() => CNRole.state() === 'admin' && StationSession.who());
    await page.waitForFunction(() => !document.querySelector('.cnNameBar'));
    assert.strictEqual(await page.evaluate(() => StationSession.who().station), 'sorter', 'the Admin, once the server could answer, is signed in as before');
    await signOutNow(page);

    /* ───────────── 6 · the question never overflows at 280, 390 and 700 px; inside an open window; blocks nothing ───────────── */
    for (const w of [280, 390, 700]) {
      await page.setViewportSize({ width: w, height: 800 });
      await wait(200);
      const baseW = await page.evaluate(() => document.documentElement.scrollWidth);      // (what the page itself is at this width, with no bar)
      await page.evaluate(() => { B.employee = 'rae  roleless'; });
      await page.waitForSelector(roleBar);
      await wait(150);
      const m = await page.evaluate(() => {
        const f = document.querySelector('.cnNameBar'), r = f.getBoundingClientRect(), vw = document.documentElement.clientWidth;
        const kids = [...f.querySelectorAll('*')].filter(e => e.getClientRects().length).map(e => { const k = e.getBoundingClientRect(); return { cls: e.className && e.className.baseVal === undefined ? e.className : '', l: k.left, r: k.right, t: k.top, b: k.bottom, clipX: e.scrollWidth > e.clientWidth + 1 && getComputedStyle(e).overflow !== 'visible' }; });
        const btn = [...f.querySelectorAll('[data-role]')].map(b => { const k = b.getBoundingClientRect(); return { l: k.left, r: k.right, w: k.width, h: k.height, over: b.scrollWidth > b.clientWidth + 1 }; });
        return { l: r.left, r: r.right, t: r.top, b: r.bottom, vw, vh: innerHeight, scrollW: f.scrollWidth, clientW: f.clientWidth, pageW: document.documentElement.scrollWidth, kids, btn, labelH: f.querySelector('.cnNbLbl').getBoundingClientRect().height };
      });
      assert(m.l >= 0 && m.r <= m.vw + 0.5, `${w}px: the bar is inside the window width (${m.l}..${m.r} of ${m.vw})`);
      assert(m.b <= m.vh, `${w}px: inside the height`);
      assert(m.scrollW <= m.clientW + 1, `${w}px: nothing overflows the bar (${m.scrollW} > ${m.clientW})`);
      assert(m.kids.every(k => k.l >= m.l - 0.5 && k.r <= m.r + 0.5 && k.t >= m.t - 0.5 && k.b <= m.b + 0.5), `${w}px: every part is inside the bar`);
      assert(m.btn.length === 2 && m.btn.every(b => !b.over && b.w >= 56 && b.h >= 22), `${w}px: two whole buttons ${JSON.stringify(m.btn)}`);
      assert(!m.kids.some(k => k.clipX), `${w}px: no clipped text`);
      assert(m.pageW <= Math.max(baseW, m.vw), `${w}px: the page gets no sideways scroll from it (${m.pageW} vs ${baseW} without it)`);
      await shot(page, `2-role-question-${w}`);
      // and the name bar with the quiet switch, in the same width
      await page.click(roleBar + ' [data-role="laser"]');
      await page.waitForFunction(() => !document.querySelector('.cnNameBar') && StationSession.role() === 'laser');
      await page.evaluate(() => { CNEmployee.edit({}); });
      await page.waitForSelector('.cnNameBar[data-kind="edit"] .cnNbSwitch:not([hidden])');
      await wait(100);
      const n = await page.evaluate(() => { const f = document.querySelector('.cnNameBar'), r = f.getBoundingClientRect(), vw = document.documentElement.clientWidth; const sw = f.querySelector('.cnNbSwitch').getBoundingClientRect(); return { l: r.left, r: r.right, vw, scrollW: f.scrollWidth, clientW: f.clientWidth, swIn: sw.left >= r.left - 0.5 && sw.right <= r.right + 0.5 }; });
      assert(n.l >= 0 && n.r <= n.vw + 0.5 && n.scrollW <= n.clientW + 1 && n.swIn, `${w}px: the name bar with the switch stays inside: ${JSON.stringify(n)}`);
      await shot(page, `3-namebar-switch-${w}`);
      await page.press('.cnNameBar input', 'Escape');
      await signOutNow(page);
    }
    await page.setViewportSize({ width: 1440, height: 950 });

    // inside an open window (a modal): the question is IN it, usable, and the window stays (never a pop-up on a pop-up)
    await page.evaluate(() => {
      const d = document.createElement('dialog'); d.id = 'testWin'; d.innerHTML = '<p style="padding:20px">An open window <button id="inWin">press</button></p>'; document.body.appendChild(d); d.showModal();
      window.__inWin = 0; document.getElementById('inWin').onclick = () => { window.__inWin++; };
      B.employee = 'wes windowed';
    });
    await page.waitForSelector('#testWin ' + roleBar);
    assert.strictEqual(await page.evaluate(() => document.querySelectorAll('dialog').length >= 1 && document.querySelector('.cnNameBar').closest('dialog').id), 'testWin', 'inside the open window');
    await page.click('#inWin'); assert.strictEqual(await page.evaluate(() => window.__inWin), 1, 'the window still works with the question open');
    await page.click('#testWin ' + roleBar + ' [data-role="design"]');
    await page.waitForFunction(() => StationSession.role() === 'design');
    assert.strictEqual(await page.evaluate(() => document.getElementById('testWin').open), true, 'the window stays open');
    // the window closes while the question is open: the question follows to the page
    await signOutNow(page);
    await page.evaluate(() => { B.employee = 'wes windowed'; });
    await page.waitForSelector('#testWin ' + roleBar);
    await page.evaluate(() => document.getElementById('testWin').close());
    await page.waitForFunction(() => { const f = document.querySelector('.cnNameBar'); return f && f.parentNode === document.body; });
    await page.click(roleBar + ' [data-role="laser"]');
    await page.waitForFunction(() => StationSession.role() === 'laser');
    await page.evaluate(() => document.getElementById('testWin').remove());
    // put away with the x: kept presses are not lost; the question does not nag, and comes back at a press after the pause
    await signOutNow(page);
    await page.evaluate(() => { B.employee = 'quin quiet'; });
    await page.waitForSelector(roleBar);
    await page.click(roleBar + ' .cnNbX');
    await page.waitForFunction(() => !document.querySelector('.cnNameBar'));
    await page.evaluate(() => CNAct('note', { detail: 'quiet 1' }));
    await wait(300);
    assert.strictEqual((await page.$$('.cnNameBar')).length, 0, 'a put-away question does not come straight back');
    assert.strictEqual(await page.evaluate(() => CNAct.held()), 1, 'the press is kept');
    await page.evaluate(() => { B.employee = 'quin quiet'; });          // the name typed again: asked again
    await page.waitForSelector(roleBar);
    await page.click(roleBar + ' [data-role="design"]');
    await page.waitForFunction(() => CNAct.held() === 0);
    await flush();
    assert.deepStrictEqual(evs().slice(-1), [['note', 'design', 'Quin Quiet', 'design', false]], 'the press kept while the question was put away is recorded under the role: ' + JSON.stringify(evs().slice(-2)));
    // the PIN: a number is no name (as before, typing one clears the name, and the role with it) and is in no request
    await page.evaluate(pin => { B.employee = pin; }, PIN);
    await page.waitForFunction(() => CNRole.state() === 'none' && !localStorage.getItem('cn.role'));
    assert.strictEqual(await page.evaluate(() => B.employee), '', 'a number is refused');
    assert.strictEqual(await page.evaluate(pin => { const o = {}; for (let i = 0; i < localStorage.length; i++) { const k = localStorage.key(i); if (/^cn\.(employee|role)$/.test(k)) o[k] = localStorage.getItem(k); } return JSON.stringify(o).includes(pin); }, PIN), false, 'and kept nowhere');
    assert.deepStrictEqual(await page.evaluate(() => window.__prompts), [], 'no browser pop-up anywhere');
    await flush();
    const everything = rec.reqs.map(r => r.body).join('\n');
    assert(!everything.includes(PIN), 'no number in any request');
    console.log('sorter role sign-in: asked once, Admin skipped, role on session and events, switch, reload, sign-out, no overflow at 280/390/700');
    await context.close();
  } finally { await browser.close(); srv.close(); }

  assert.deepStrictEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  console.log('sorter-role-signin: all passed');
})().catch(e => { console.error(e); process.exit(1); });
