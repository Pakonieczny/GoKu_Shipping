// Auto sign-out (station-session.js): Rule A (10 minutes without input signs a non-Admin person out) and Rule B (17:00 America/Toronto:
// everybody with no input in the last 10 minutes is signed out; anybody with input inside the window stays and Rule A takes over).
// Fakes only: the real station-session.js (and station-scan-queue.js) run in a vm with a fake window, document, localStorage, fetch and a
// FAKE CLOCK (Date and timers). `sleep` moves the clock with no timer running (a computer asleep); `advance` runs the timers.
//   1  Rule A at exactly 10:00 and 9:59, input resets, scripts' own events and throttling, nothing but a time is kept or sent
//   2  Admin: never signed out by A or B (asked once per sign-in); unknown / offline / false is not Admin; midnight stays for everyone
//   3  Rule B at 17:00 with and without recent input, to the second; a sleeping page; a late sign-in
//   4  the two DST change days (8 Mar and 1 Nov 2026) for 17:00 and for 10 minutes of real time
//   5  reload mid-idle (a reload is input) and a login left while the page was closed
//   6  a scan counts as input (StationSession.touch and the scan queue); frames pass the time to each other
//   7  the first input after a long gap signs the person out first; beats and the end carry lastInputAt; the end is the last input
//   7b two people at one page (multi mode): input at the page counts for both, an Admin stays, 17:00, reload, midnight
//   8  every page that calls StationSession.init has a signOut that takes the reason and words idle / closing
//   9  the real pages (weld-1, assembly-1, shipping-1, design-message-1, design, design-1, etsy-mail-1, sorting, sorting-2) with Playwright's clock:
//      10 minutes without input signs out through the page's own callback with the reason in its notice; an Admin stays
//   node tests/stations/auto-signout.cjs            (part 9 needs Playwright: NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... )
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), vm = require('vm');
const root = path.join(__dirname, '../..');
const SRC = fs.readFileSync(process.env.SS_FILE || path.join(root, 'station-session.js'), 'utf8');       // (SS_FILE: a changed copy, to see that the test notices)
const QUEUE = fs.readFileSync(path.join(root, 'station-scan-queue.js'), 'utf8');
const MIN = 60000, SEC = 1000, HOUR = 3600000;
const Z = s => Date.parse(s);
const hhmm = t => new Intl.DateTimeFormat('en-GB', { timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' }).format(new Date(t));

/* ── the fake world ── */
function makeEnv(startIso) {
  const clock = { t: Z(startIso), timers: [], seq: 1 };
  const env = {
    clock, posts: [], gets: [], asks: [], endBeats: null, sessionStatus: 0, holdDoor: null, storage: new Map(), admin: {}, door: 'ok',           // door: ok | offline | confused | noOk | 503 | busy | refused
    set(t) { clock.t = t; },
    /** the clock moves and the timers run in order (an open page) */
    advance(ms) {
      const to = clock.t + ms;
      for (;;) {
        const next = clock.timers.filter(x => x.at <= to).sort((a, b) => a.at - b.at || a.id - b.id)[0];
        if (!next) break;
        clock.t = Math.max(clock.t, next.at);
        if (next.every) next.at += next.every; else clock.timers.splice(clock.timers.indexOf(next), 1);
        try { next.fn(); } catch (e) { env.errors.push(e); }
      }
      clock.t = to;
    },
    advanceTo(t) { env.advance(t - clock.t); },
    /** the clock moves and NO timer runs (a computer asleep, a frozen page); overdue timers fire once when it wakes */
    sleep(ms) {
      clock.t += ms;
      for (const x of clock.timers) if (x.at < clock.t) x.at = x.every ? clock.t + x.every : clock.t;
    },
    errors: [],
    sessionPosts: () => env.posts.filter(b => b.session).map(b => b.session),
    ends: () => env.sessionPosts().filter(s => s.event === 'end'),
    starts: () => env.sessionPosts().filter(s => s.event === 'start'),
    beats: () => env.sessionPosts().filter(s => s.event === 'beat'),
    adminGets: () => env.asks.length,
  };
  return env;
}
const settle = async () => { for (let i = 0; i < 6; i++) await new Promise(r => setImmediate(r)); };

function FDateFor(clock) {
  return class FDate extends Date {
    constructor(...a) { if (a.length) super(...a); else super(clock.t); }
    static now() { return clock.t; }
  };
}

/** a page: a new window with the real station-session.js, the page's own sign-in keys in the shared localStorage, and its signOut callback */
function openPage(env, o = {}) {
  const { clock } = env;
  const idKey = (o.keys || '') + 'employee_id', nameKey = (o.keys || '') + 'employee_name';     // (a frame has its own origin, so its own keys)
  const listeners = {}, docListeners = {};
  const storage = {
    getItem: k => (env.storage.has(k) ? env.storage.get(k) : null),
    setItem: (k, v) => { env.storage.set(k, String(v)); },
    removeItem: k => { env.storage.delete(k); },
  };
  const win = {
    __frames: [], __signOuts: [], __toasts: [],
    addEventListener(t, fn) { (listeners[t] = listeners[t] || []).push(fn); },
    removeEventListener() {},
    postMessage(m) { win.__deliver({ data: JSON.parse(JSON.stringify(m)), source: win.__peer }); },     // delivered TO this window, from its peer (its frame or its parent)
    __deliver(ev) { for (const f of listeners.message || []) f(ev); },
    /** a trusted event of this type (a person's), or one a script made (isTrusted false) */
    fire(type, trusted = true, extra = {}) { for (const f of listeners[type] || []) f(Object.assign({ type, isTrusted: trusted }, extra)); },
    listenerCount: t => (listeners[t] || []).length,
  };
  const doc = {
    readyState: 'complete', visibilityState: 'visible', hidden: false,
    addEventListener(t, fn) { (docListeners[t] = docListeners[t] || []).push(fn); },
    querySelector: () => null,
    getElementsByTagName: t => (t === 'iframe' ? win.__frames : []),
    createElement: () => ({ style: {}, appendChild() {}, addEventListener() {}, querySelector: () => null }),
  };
  const fetchFake = (url, init = {}) => {
    const method = (init.method || 'GET').toUpperCase();
    if (method === 'POST') {
      const b = JSON.parse(init.body || '{}');
      if (b.stationAdmin !== undefined) {                       // AD2's door: POST { stationAdmin: name } -> { ok: true, admin }
        env.asks.push({ url, body: b, method });
        if (env.door === 'offline') return Promise.reject(new Error('offline'));
        if (env.door === 'confused') return Promise.resolve({ status: 200, ok: true, json: async () => ({ success: true, data: {} }) });     // 200 but not { ok: true, admin }
        if (env.door === 'noOk') return Promise.resolve({ status: 200, ok: true, json: async () => ({ admin: true }) });                      // a body without ok: true is not an answer
        if (env.door === '503') return Promise.resolve({ status: 503, ok: false, json: async () => ({ error: 'the list cannot be read' }) });
        if (env.door === 'busy') return Promise.resolve({ status: 429, ok: false, json: async () => ({ ok: false, tooMany: true }) });
        if (env.door === 'refused') return Promise.resolve({ status: 400, ok: false, json: async () => ({ ok: false, error: 'bad name' }) });
        const name = String(b.stationAdmin).toLowerCase(), answer = () => ({ status: 200, ok: true, json: async () => ({ ok: true, admin: !!env.admin[name] }) });
        if (env.holdDoor) return new Promise(res => env.holdDoor.push(() => res(answer())));            // a slow door: answered when the test lets it
        return Promise.resolve(answer());
      }
      env.posts.push(b);
      if (env.sessionStatus) return Promise.resolve({ status: env.sessionStatus, ok: false, json: async () => ({ error: 'down' }) });
      // a beat the server already ended (idle / closing) is answered { success, ended, endReason } (AD2)
      const s = b.session, ended = s && s.event === 'beat' && env.endBeats && env.endBeats[s.id];
      return Promise.resolve({ status: 200, ok: true, json: async () => (ended ? { success: true, ended: true, endReason: ended } : { success: true }) });
    }
    env.gets.push(url);
    return Promise.resolve({ status: 404, ok: false, json: async () => ({}) });
  };
  const timers = clock.timers;
  const sandbox = {
    window: win, document: doc, localStorage: storage, sessionStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    navigator: { onLine: true, sendBeacon: () => true }, location: { search: o.search || '' },
    fetch: fetchFake, Intl, Promise, JSON, Math, Set, Map, Uint32Array, Array, Object, String, Number, RegExp, Error, Blob: class {},
    AbortController, console: { warn() {}, log() {}, error() {} },
    Date: FDateFor(clock),
    setTimeout: (fn, ms) => { const id = clock.seq++; timers.push({ id, at: clock.t + Math.max(0, ms || 0), fn }); return id; },
    setInterval: (fn, ms) => { const id = clock.seq++; timers.push({ id, at: clock.t + ms, every: ms, fn }); return id; },
    clearTimeout: id => { const i = timers.findIndex(x => x.id === id); if (i >= 0) timers.splice(i, 1); },
    clearInterval: id => { const i = timers.findIndex(x => x.id === id); if (i >= 0) timers.splice(i, 1); },
  };
  sandbox.clearTimeout = sandbox.clearInterval = sandbox.clearTimeout;
  win.self = win; win.top = o.parent ? o.parent : win; win.parent = o.parent || win;
  if (o.parent) { o.parent.__peer = win; win.__peer = o.parent; }
  Object.defineProperty(sandbox, 'StationSession', { get: () => win.StationSession, configurable: true });     // (in a browser window is the global: a bare StationSession works)
  Object.defineProperty(sandbox, 'StationScanQueue', { get: () => win.StationScanQueue, configurable: true });
  const ctx = vm.createContext(sandbox);
  vm.runInContext(SRC, ctx, { filename: 'station-session.js' });
  if (o.queue) vm.runInContext(QUEUE, ctx, { filename: 'station-scan-queue.js' });
  const ss = win.StationSession;
  const page = { win, ss, ctx, doc, docListeners, listeners,
    get signOuts() { return win.__signOuts; },
    /** the PIN-station login keys, as weld-1 / assembly / shipping keep them */
    logIn(name) { env.storage.set(idKey, 'x'); env.storage.set(nameKey, name); },
    loggedIn: () => !!env.storage.get(idKey),
    init(extra = {}) {
      ss.init(Object.assign({
        station: o.station || 'welding', device: o.device || 'weld-1',
        person: () => (env.storage.get(idKey) && env.storage.get(nameKey) ? { name: env.storage.get(nameKey), id: null } : null),
        signOut: (reason, who) => {
          win.__signOuts.push({ reason, who, at: clock.t });
          env.storage.delete(idKey); env.storage.delete(nameKey);
          win.__toasts.push(ss.notice(reason, 'Please sign in with your Employee Number.') || 'Signed out at midnight.');
        },
      }, extra));
      return page;
    },
    /** a person signs in on the page (the page's login success) */
    signIn(name) { page.logIn(name); ss.signedIn({ name, id: null }); },
    wake() { doc.visibilityState = 'visible'; for (const f of docListeners.visibilitychange || []) f(); },
    close() { win.fire('pagehide', true); },
  };
  return page;
}

const today = '2026-10-06T14:00:00Z';                 // Tuesday 10:00 in Toronto (EDT)
const T0 = Z(today);

/* ── 1 · Rule A ── */
async function ruleA() {
  // exactly 10:00 and 9:59
  let env = makeEnv(today); env.door = 'ok';
  let pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  assert.strictEqual(pg.ss.lastInput(), T0, 'a sign-in is input');
  env.advance(9 * MIN + 59 * SEC); await settle();
  assert.strictEqual(pg.signOuts.length, 0, 'not at 9:59 (timers run)');
  env.set(T0 + 10 * MIN - 1); pg.wake();
  assert.strictEqual(pg.signOuts.length, 0, 'not at 9:59.999');
  env.set(T0 + 10 * MIN); pg.wake();
  assert.strictEqual(pg.signOuts.length, 1, 'at exactly 10:00');
  assert.strictEqual(pg.signOuts[0].reason, 'idle'); assert.strictEqual(pg.signOuts[0].who.name, 'Tess Welder');
  assert(!pg.loggedIn(), 'the page cleared its login');
  assert.strictEqual(pg.win.__toasts[0], 'Signed out after 10 minutes without input. Please sign in with your Employee Number.');
  assert.strictEqual(pg.ss.notice('idle'), 'Signed out after 10 minutes without input.');
  assert.strictEqual(pg.ss.notice('closing'), 'Signed out at 5:00 pm.');
  assert.strictEqual(pg.ss.notice('midnight'), '', 'the page keeps its own midnight wording');
  let ends = env.ends(); assert.strictEqual(ends.length, 1);
  assert.strictEqual(ends[0].reason, 'idle'); assert.strictEqual(ends[0].at, T0, 'the end is the last input, not the time it was noticed');
  assert.strictEqual(ends[0].lastInputAt, T0);
  env.advance(5 * MIN); await settle();
  assert.strictEqual(pg.signOuts.length, 1, 'signed out once only'); assert.strictEqual(env.ends().length, 1);
  assert.strictEqual(pg.ss.current(), null, 'no session runs');

  // the timers alone sign out within one 10 second tick of the 10 minutes, at the last input
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(10 * MIN + 10 * SEC); await settle();
  assert.strictEqual(pg.signOuts.length, 1, 'the tick signs out'); assert.strictEqual(env.ends()[0].at, T0);

  // input resets the clock; scripts' own events do not; one stamp a second
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(5 * MIN); pg.win.fire('keydown'); const k = pg.ss.lastInput();
  assert.strictEqual(k, T0 + 5 * MIN, 'a key is input');
  env.advance(4 * MIN + 59 * SEC); pg.win.fire('click', false);                 // a script's own click (isTrusted false)
  assert.strictEqual(pg.ss.lastInput(), k, 'an event a script made is not input');
  // TRUSTED ONLY: no kind of event counts unless the browser says a person made it (isTrusted true): not false, not missing, not a lookalike
  for (const type of ['pointerdown', 'pointermove', 'mousedown', 'mousemove', 'touchstart', 'touchmove', 'keydown', 'wheel', 'click', 'input', 'paste']) {
    for (const flag of [false, null, 'true', 1]) { pg.win.fire(type, flag); assert.strictEqual(pg.ss.lastInput(), k, type + ' with isTrusted ' + JSON.stringify(flag) + ' is not input'); }
    for (const e of [undefined, null]) for (const f of pg.listeners[type] || []) f(e);
    assert.strictEqual(pg.ss.lastInput(), k, type + ': no event object, no input');
  }
  env.advance(1 * SEC); env.set(T0 + 15 * MIN - 1); pg.wake(); assert.strictEqual(pg.signOuts.length, 0, 'the key moved the deadline to 15:00');
  env.set(T0 + 15 * MIN); pg.wake();
  assert.strictEqual(pg.signOuts.length, 1); assert.strictEqual(env.ends()[0].at, T0 + 5 * MIN);
  // every kind of input counts
  for (const type of ['pointerdown', 'pointermove', 'mousedown', 'mousemove', 'touchstart', 'touchmove', 'keydown', 'wheel', 'click', 'input', 'paste']) {
    env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
    env.advance(2 * MIN); pg.win.fire(type); assert.strictEqual(pg.ss.lastInput(), T0 + 2 * MIN, type + ' is input');
  }
  // throttle: one stamp a second
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.set(T0 + 30 * SEC); pg.win.fire('mousemove'); const a = pg.ss.lastInput();
  for (let i = 1; i < 10; i++) { env.set(T0 + 30 * SEC + i * 90); pg.win.fire('mousemove'); }
  assert.strictEqual(pg.ss.lastInput(), a, 'moves inside one second are not stamped again');
  env.set(T0 + 31 * SEC + 5); pg.win.fire('mousemove'); assert.strictEqual(pg.ss.lastInput(), T0 + 31 * SEC + 5, 'the next second is');
  {
    // touch() is stamped at most once a second too
    const e2 = makeEnv(today), p2 = openPage(e2).init(); p2.signIn('Tess Welder'); await settle();
    e2.set(T0 + 20 * SEC); p2.ss.touch(); e2.set(T0 + 20 * SEC + 400); p2.ss.touch(); assert.strictEqual(p2.ss.lastInput(), T0 + 20 * SEC, 'touch() twice inside one second is one stamp');
    e2.set(T0 + 21 * SEC + 1); p2.ss.touch(); assert.strictEqual(p2.ss.lastInput(), T0 + 21 * SEC + 1);
    // a load is input, with nobody signed in as well
    const e3 = makeEnv(today); e3.advance(1234); const p3 = openPage(e3).init(); assert.strictEqual(p3.ss.lastInput(), e3.clock.t, 'a page load is input');
  }
  // never the content: only times are kept or sent, no text typed or pressed
  for (const ch of 'secret-typed-text') pg.win.fire('keydown', true, { key: ch, code: 'Key' + ch.toUpperCase(), target: { value: 'secret-typed-text' } });
  pg.win.fire('input', true, { data: 'secret-typed-text', target: { value: 'secret-typed-text' } });
  env.advance(6 * MIN); await settle();
  const everything = JSON.stringify(env.posts) + JSON.stringify([...env.storage.entries()]);
  assert(!/secret|KeyS|typed/i.test(everything), 'no keystroke, no text in any request or in storage');
  for (const s of env.sessionPosts()) assert(Object.keys(s).every(x => ['id', 'event', 'person', 'employeeId', 'station', 'device', 'computerId', 'computerLabel', 'at', 'reason', 'lastInputAt', 'sentAt'].includes(x)), 'the session body keeps its shape: ' + Object.keys(s));
  const stored = [...env.storage.keys()].filter(x => /^station_|employee/.test(x)).sort();
  assert.deepStrictEqual(stored, ['employee_id', 'employee_name', 'station_computer_id', 'station_session.welding.weld-1', 'station_signin_day', 'station_signin_days'].sort(), 'nothing new is stored: ' + stored);
  console.log('A: idle at exactly 10:00 (not 9:59), input resets, a script\'s own events are not input, one stamp a second, no content kept');
}

/* ── 2 · Admin ── */
async function admin() {
  // an Admin is never signed out by Rule A; the door is asked once per sign-in; midnight stays
  let env = makeEnv(today); env.admin = { 'paul k': true };
  let pg = openPage(env).init(); pg.signIn('Paul K'); await settle();
  env.advance(3 * HOUR); await settle();
  assert.strictEqual(pg.signOuts.length, 0, 'an Admin stays signed in after 3 hours without input');
  assert.strictEqual(env.adminGets(), 1, 'asked once for the sign-in');
  assert(env.asks.every(a => a.method === 'POST' && Object.keys(a.body).join() === 'stationAdmin' && a.body.stationAdmin === 'Paul K'), 'a POST with the name in the body and nothing else');
  assert(env.asks.every(a => !/Paul|%20/i.test(a.url)) && env.gets.length === 0, 'the name is never in a URL');
  assert(env.beats().length >= 30, 'his beats go on'); assert(env.beats().every(b => b.person === 'Paul K'));
  assert.strictEqual(pg.ss.lastInput() <= T0 + 1000, true, 'no input was made');
  // midnight (New York) stays for everybody: 14:00Z + 14h = 04:00Z the next day = 00:00 EDT
  const mid = Z('2026-10-07T04:00:00Z');
  env.advanceTo(mid + 40 * SEC); await settle();
  assert.strictEqual(pg.signOuts.length, 1, 'midnight signs the Admin out'); assert.strictEqual(pg.signOuts[0].reason, 'midnight');
  // the other answers are not Admin: false, offline, and an answer the door does not give
  for (const door of ['false', 'offline', 'confused', 'noOk', '503', 'busy', 'refused']) {
    // (even a door that would call the person an Admin is not believed when the answer is not exactly { ok: true, admin })
    env = makeEnv(today); env.admin = door === 'noOk' ? { 'tess welder': true } : {}; env.door = door === 'false' ? 'ok' : door;
    pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
    env.advance(10 * MIN + 10 * SEC); await settle();
    assert.strictEqual(pg.signOuts.length, 1, door + ': not Admin, signed out at 10 minutes');
    assert.strictEqual(pg.signOuts[0].reason, 'idle');
    assert.strictEqual(env.adminGets(), 1, door + ': ONE question a sign-in, whatever the answer (never asked again)');
  }
  // R5: asked ONCE per sign-in. The door down at the sign-in is "not Admin" for that sign-in even when it comes back a minute later
  // (the Admin signs in again); a late answer that does come (a slow door) is believed
  for (const down of ['offline', '503']) {
    env = makeEnv(today); env.admin = { 'paul k': true }; env.door = down;
    pg = openPage(env).init(); pg.signIn('Paul K'); await settle();
    env.advance(2 * MIN); await settle(); env.door = 'ok'; env.advance(30 * MIN); await settle();
    assert.strictEqual(pg.signOuts.length, 1, 'the door was down at the sign-in (' + down + '): fail closed, signed out at idle'); assert.strictEqual(pg.signOuts[0].reason, 'idle');
    assert.strictEqual(env.adminGets(), 1, 'asked once, not again when the door came back');
  }
  env = makeEnv(today); env.admin = { 'paul k': true };
  const holdAnswer = env.holdDoor = [];                                               // a slow door: the answer comes after 5 minutes, and says Admin
  pg = openPage(env).init(); pg.signIn('Paul K'); await settle();
  env.advance(5 * MIN); await settle(); assert.strictEqual(pg.signOuts.length, 0, 'not yet 10 minutes'); assert.strictEqual(holdAnswer.length, 1, 'one question waiting');
  holdAnswer[0](); await settle(); env.advance(30 * MIN); await settle();
  assert.strictEqual(pg.signOuts.length, 0, 'a late "Admin" answer is believed: no sign-out'); assert.strictEqual(env.adminGets(), 1);
  // a name is compared as the server compares it: this client sends the cleaned name, never digits (a PIN)
  env = makeEnv(today); env.admin = { 'paul k': true }; pg = openPage(env).init(); pg.signIn('Paul K 482915'); await settle();
  assert(env.asks.length === 1 && env.asks[0].body.stationAdmin === 'Paul K' && !/482915/.test(JSON.stringify(env.asks)), 'no digits of a PIN leave in the question');
  // the answer is asked again at the next sign-in (a new person, or the same one again)
  env = makeEnv(today); env.admin = { 'paul k': true }; pg = openPage(env).init(); pg.signIn('Paul K'); await settle();
  pg.ss.signedOut('signOut'); env.storage.delete('employee_id'); env.storage.delete('employee_name');
  pg.signIn('Tess Welder'); await settle();
  assert.strictEqual(env.adminGets(), 2, 'asked once for each sign-in');
  env.advance(10 * MIN + 10 * SEC); await settle();
  assert.strictEqual(pg.signOuts.length, 1, 'the second person is not an Admin'); assert.strictEqual(pg.signOuts[0].who.name, 'Tess Welder');
  // isAdmin() for a page that asks before its own sign-in (the Laser or Design question): cached, once
  env = makeEnv(today); env.admin = { 'paul k': true }; pg = openPage(env).init();
  assert.strictEqual(await pg.ss.isAdmin('Paul K'), true); assert.strictEqual(await pg.ss.isAdmin('paul k'), true); assert.strictEqual(await pg.ss.isAdmin('Tess Welder'), false);
  assert.strictEqual(env.adminGets(), 2, 'one question a name');
  pg.signIn('Paul K'); await settle(); assert.strictEqual(env.adminGets(), 2, 'the sign-in uses the answer it has');
  env.advance(2 * HOUR); await settle(); assert.strictEqual(pg.signOuts.length, 0);
  env = makeEnv(today); env.door = 'offline'; pg = openPage(env).init();
  assert.strictEqual(await pg.ss.isAdmin('Paul K'), null, 'offline: not known');
  console.log('Admin: never signed out by A (asked once a sign-in); false, offline and an unclear answer are not Admin; midnight still signs an Admin out');
}

/* ── 3 · Rule B at 17:00 Toronto ── */
async function ruleB() {
  const T = (hhmmss) => Z('2026-10-06T' + hhmmss + 'Z');              // UTC; 17:00 EDT = 21:00Z
  assert.strictEqual(hhmm(T('21:00:00')), '17:00:00');
  const CLOSE = T('21:00:00');
  // awake, last input 16:30: Rule A signs out at 16:40 (reason idle), end 16:30
  let env = makeEnv('2026-10-06T20:25:00Z'); let pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advanceTo(T('20:30:00')); pg.win.fire('keydown'); env.advanceTo(T('21:10:00')); await settle();
  assert.strictEqual(pg.signOuts.length, 1); assert.strictEqual(pg.signOuts[0].reason, 'idle', 'an awake page signs out at 16:40 by Rule A');
  assert.strictEqual(env.ends()[0].at, T('20:30:00'));
  // a page that slept through 17:00 with the last input at 16:30: Rule B (closing), end at the last input
  env = makeEnv('2026-10-06T20:25:00Z'); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advanceTo(T('20:30:00')); pg.win.fire('keydown'); env.sleep(35 * MIN); pg.wake();      // wakes 17:05
  assert.strictEqual(pg.signOuts.length, 1); assert.strictEqual(pg.signOuts[0].reason, 'closing');
  assert.strictEqual(env.ends()[0].reason, 'closing'); assert.strictEqual(env.ends()[0].at, T('20:30:00'), 'closing ends at the last input');
  assert.strictEqual(pg.win.__toasts[0], 'Signed out at 5:00 pm. Please sign in with your Employee Number.');
  // input inside the 10 minutes before 17:00 (16:57): stays at 17:00, Rule A takes over from that input (idle at 17:07, end 16:57)
  env = makeEnv('2026-10-06T20:50:00Z'); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advanceTo(T('20:57:00')); pg.win.fire('keydown');
  env.set(T('21:00:30')); pg.wake(); assert.strictEqual(pg.signOuts.length, 0, 'input in the last 10 minutes: stays at 17:00');
  env.set(T('21:06:59')); pg.wake(); assert.strictEqual(pg.signOuts.length, 0);
  env.set(T('21:07:00')); pg.wake();
  assert.strictEqual(pg.signOuts.length, 1); assert.strictEqual(pg.signOuts[0].reason, 'idle', 'Rule A took over'); assert.strictEqual(env.ends()[0].at, T('20:57:00'));
  // to the second: the last input at 16:50:00 has had 10 minutes at 17:00:00 (closing); at 16:50:01 it has not
  for (const [li, at, reason] of [['20:50:00', '21:00:00', 'closing'], ['20:50:01', '21:00:00', null], ['20:50:01', '21:00:01', 'idle'], ['20:49:59', '21:00:00', 'closing']]) {
    env = makeEnv('2026-10-06T20:45:00Z'); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
    env.set(T(li)); pg.win.fire('keydown'); assert.strictEqual(pg.ss.lastInput(), T(li));
    env.set(T(at)); pg.wake();
    assert.strictEqual(pg.signOuts.length, reason ? 1 : 0, `last input ${li}, looked at ${at}`);
    if (reason) { assert.strictEqual(pg.signOuts[0].reason, reason, `${li} / ${at}`); assert.strictEqual(env.ends()[0].at, T(li)); }
  }
  // a sign-in after 17:00 is input: nothing signs the person out at once; Rule A does at 10 minutes
  env = makeEnv('2026-10-06T21:20:00Z'); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advanceTo(T('21:29:00')); await settle(); assert.strictEqual(pg.signOuts.length, 0, 'signed in at 17:20: still here at 17:29');
  env.advanceTo(T('21:31:00')); await settle(); assert.strictEqual(pg.signOuts.length, 1); assert.strictEqual(pg.signOuts[0].reason, 'idle'); assert.strictEqual(env.ends()[0].at, T('21:20:00'));
  // continuing input past 17:00 keeps the person, hour after hour
  env = makeEnv('2026-10-06T20:00:00Z'); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  for (let t = T('20:05:00'); t < T('22:30:00'); t += 4 * MIN) { env.advanceTo(t); pg.win.fire('pointermove'); }
  await settle(); assert.strictEqual(pg.signOuts.length, 0, 'a person who keeps working is not signed out at 17:00 or after');
  // an Admin is exempt from closing as well
  env = makeEnv('2026-10-06T20:25:00Z'); env.admin = { 'paul k': true }; pg = openPage(env).init(); pg.signIn('Paul K'); await settle();
  env.advanceTo(T('20:30:00')); env.sleep(35 * MIN); pg.wake(); await settle();
  assert.strictEqual(pg.signOuts.length, 0, 'an Admin is not signed out at 17:00');
  console.log('B: closing at 17:00 Toronto only with no input in the last 10 minutes (to the second), ends at the last input; recent input stays and Rule A takes over; an Admin is exempt');
}

/* ── 4 · the two DST change days ── */
async function dst() {
  const ss = openPage(makeEnv(today)).ss;
  const at = (iso, want) => assert.strictEqual(ss.closingAt(Z(iso)), Z(want), `closingAt(${iso}) = ${want}`);
  // spring forward, Sunday 8 Mar 2026 (clocks go 02:00 -> 03:00; 17:00 is EDT = 21:00Z; Saturday's 17:00 was EST = 22:00Z)
  at('2026-03-08T21:00:00Z', '2026-03-08T21:00:00Z'); at('2026-03-08T20:59:59Z', '2026-03-07T22:00:00Z');
  at('2026-03-08T05:30:00Z', '2026-03-07T22:00:00Z');                    // 00:30 EST that morning, before the change
  at('2026-03-08T07:30:00Z', '2026-03-07T22:00:00Z');                    // 03:30 EDT, just after it
  at('2026-03-09T20:59:59Z', '2026-03-08T21:00:00Z');                    // Monday 16:59:59 EDT is still Sunday's 17:00
  at('2026-03-09T21:00:00Z', '2026-03-09T21:00:00Z');
  // fall back, Sunday 1 Nov 2026 (02:00 -> 01:00; 17:00 is EST = 22:00Z; Saturday's 17:00 was EDT = 21:00Z)
  at('2026-11-01T22:00:00Z', '2026-11-01T22:00:00Z'); at('2026-11-01T21:59:59Z', '2026-10-31T21:00:00Z');
  at('2026-11-01T05:30:00Z', '2026-10-31T21:00:00Z');                    // 01:30 EDT, the first 01:30
  at('2026-11-01T06:30:00Z', '2026-10-31T21:00:00Z');                    // 01:30 EST, the second 01:30
  at('2026-11-02T21:59:59Z', '2026-11-01T22:00:00Z');
  for (const [iso, want] of [['2026-03-08T21:00:00Z', '17:00:00'], ['2026-11-01T22:00:00Z', '17:00:00'], ['2026-03-07T22:00:00Z', '17:00:00'], ['2026-10-31T21:00:00Z', '17:00:00']]) assert.strictEqual(hhmm(Z(iso)), want);
  // Rule B on each change day: a frozen page woken at 17:05 local with the last input at 16:30 local; and 16:55 stays
  for (const [day, closeIso] of [['2026-03-08', '2026-03-08T21:00:00Z'], ['2026-11-01', '2026-11-01T22:00:00Z']]) {
    const C = Z(closeIso);
    let env = makeEnv(new Date(C - 35 * MIN).toISOString()); let pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
    env.advanceTo(C - 30 * MIN); pg.win.fire('keydown'); env.sleep(35 * MIN); pg.wake();
    assert.strictEqual(pg.signOuts.length, 1, day + ': closing found at 17:05'); assert.strictEqual(pg.signOuts[0].reason, 'closing'); assert.strictEqual(env.ends()[0].at, C - 30 * MIN);
    env = makeEnv(new Date(C - 12 * MIN).toISOString()); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
    env.advanceTo(C - 5 * MIN); pg.win.fire('keydown'); env.set(C + 30 * SEC); pg.wake();
    assert.strictEqual(pg.signOuts.length, 0, day + ': input at 16:55 stays at 17:00');
    env.set(C + 5 * MIN); pg.wake(); assert.strictEqual(pg.signOuts.length, 1); assert.strictEqual(pg.signOuts[0].reason, 'idle'); assert.strictEqual(env.ends()[0].at, C - 5 * MIN);
  }
  // ten minutes is ten minutes of real time across the change itself: 01:55 EST on 8 Mar is 06:55Z, and 10 minutes later is 03:05 EDT
  let env = makeEnv('2026-03-08T06:55:00Z'); let pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.set(Z('2026-03-08T07:04:59Z')); pg.wake(); assert.strictEqual(pg.signOuts.length, 0);
  env.set(Z('2026-03-08T07:05:00Z')); pg.wake(); assert.strictEqual(pg.signOuts.length, 1, 'spring forward: 10 real minutes'); assert.strictEqual(env.ends()[0].at, Z('2026-03-08T06:55:00Z'));
  // 01:55 EDT on 1 Nov is 05:55Z; 10 minutes later is 01:05 EST (the clock reads 01:05 again 70 minutes after 01:55 if you count wall time: it must not wait)
  env = makeEnv('2026-11-01T05:55:00Z'); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.set(Z('2026-11-01T06:04:59Z')); pg.wake(); assert.strictEqual(pg.signOuts.length, 0);
  env.set(Z('2026-11-01T06:05:00Z')); pg.wake(); assert.strictEqual(pg.signOuts.length, 1, 'fall back: 10 real minutes'); assert.strictEqual(env.ends()[0].at, Z('2026-11-01T05:55:00Z'));
  // midnight (New York) is still the day's own: on 8 Mar, 00:00 EST = 05:00Z, and the Admin / idle rules do not move it
  assert.strictEqual(ss.nextMidnight(Z('2026-03-07T20:00:00Z')), Z('2026-03-08T05:00:00Z'));
  assert.strictEqual(ss.nextMidnight(Z('2026-03-08T20:00:00Z')), Z('2026-03-09T04:00:00Z'));
  assert.strictEqual(ss.nextMidnight(Z('2026-11-01T20:00:00Z')), Z('2026-11-02T05:00:00Z'));
  console.log('DST: 17:00 Toronto on 7 Mar, 8 Mar, 31 Oct and 1 Nov, around the clock change itself; closing and 10 minutes of real time on both change days; midnight unchanged');
}

/* ── 5 · reload mid-idle ── */
async function reload() {
  // a reload inside the 10 minutes is input: the session goes on (same id, no new start) and the clock starts again from the load
  let env = makeEnv(today); let pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  const id = env.starts()[0].id;
  env.advance(6 * MIN); pg.close(); await settle();
  let pg2 = openPage(env).init(); await settle();
  assert.strictEqual(env.starts().length, 1, 'the session goes on across the reload'); assert.strictEqual(pg2.ss.current().id, id);
  assert.strictEqual(pg2.ss.lastInput(), T0 + 6 * MIN, 'a reload is input');
  env.set(T0 + 15 * MIN + 59 * SEC); pg2.wake(); assert.strictEqual(pg2.signOuts.length, 0, 'not 9:59 after the reload');
  env.set(T0 + 16 * MIN); pg2.wake(); assert.strictEqual(pg2.signOuts.length, 1); assert.strictEqual(pg2.signOuts[0].reason, 'idle');
  assert.strictEqual(env.ends().length, 1); assert.strictEqual(env.ends()[0].id, id); assert.strictEqual(env.ends()[0].at, T0 + 6 * MIN, 'ended at the reload, the last input');
  // the page is closed and its login left: reopened 11 minutes after the last input, the person is signed out before the page starts
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(3 * MIN); pg.win.fire('keydown'); env.advance(1 * MIN); pg.close(); await settle();      // last input 3:00, closed at 4:00
  env.sleep(11 * MIN - 1 * MIN * 1);                                                                       // reopened at 14:00 (11 minutes after the last input)
  pg2 = openPage(env).init(); await settle();
  assert.strictEqual(pg2.signOuts.length, 1, 'signed out while loading'); assert.strictEqual(pg2.signOuts[0].reason, 'idle');
  assert(!pg2.loggedIn(), 'the page finds no login and shows its own sign-in');
  assert.strictEqual(env.ends().length, 1); assert.strictEqual(env.ends()[0].at, T0 + 3 * MIN, 'ended at the last input it had stored');
  assert.strictEqual(env.starts().length, 1, 'no session starts for a login that had lapsed');
  assert.strictEqual(pg2.ss.current(), null);
  // reopened at 9 minutes after the last input: still the person's, and the reload counts as input
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(3 * MIN); pg.win.fire('keydown'); env.advance(1 * MIN); pg.close(); env.sleep(5 * MIN);
  pg2 = openPage(env).init(); await settle(); assert.strictEqual(pg2.signOuts.length, 0); assert.strictEqual(pg2.ss.current().person, 'Tess Welder');
  // a closed page that last beat more than 15 minutes ago and was left logged in: signed out, not given a new session
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.sleep(40 * MIN); pg2 = openPage(env).init(); await settle();
  assert.strictEqual(pg2.signOuts[0].reason === 'idle' || pg2.signOuts[0].reason === 'closing', true); assert.strictEqual(env.starts().length, 1);
  // another person signs in on a page whose stored session had gone quiet: it ended "closed" at the last input it had, not at its later beat
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(3 * MIN); pg.win.fire('keydown'); const lastIn = env.clock.t; env.advance(1 * MIN); pg.close(); env.sleep(40 * MIN);
  env.storage.set('employee_name', 'Ray Welder'); pg2 = openPage(env).init(); await settle();
  assert.strictEqual(env.ends()[0].reason, 'closed'); assert.strictEqual(env.ends()[0].at, lastIn, 'a page that died ended at the last input it knew of');
  assert.strictEqual(pg2.ss.current().person, 'Ray Welder');
  // an Admin's page reopened after 30 minutes keeps its login (the answer was kept with the sign-in)
  env = makeEnv(today); env.admin = { 'paul k': true }; pg = openPage(env).init(); pg.signIn('Paul K'); await settle();
  env.advance(2 * MIN); pg.close(); env.sleep(30 * MIN);
  pg2 = openPage(env).init(); await settle();
  assert.strictEqual(pg2.signOuts.length, 0, 'an Admin is not signed out by a reload either');
  // another person's login on the page is not the stored one: the old session ends "switched", the new person starts
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(2 * MIN); pg.close(); env.storage.set('employee_name', 'Ray Welder'); pg2 = openPage(env).init(); await settle();
  assert.strictEqual(pg2.signOuts.length, 0); assert.strictEqual(env.ends()[0].reason, 'switched'); assert.strictEqual(pg2.ss.current().person, 'Ray Welder');
  console.log('reload: inside the 10 minutes it is input and the session goes on; a login left past 10 minutes signs out first, ended at the stored last input; Admin kept');
}

/* ── 6 · a scan is input; frames ── */
async function scansAndFrames() {
  let env = makeEnv(today); let pg = openPage(env, { queue: true }).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(9 * MIN); pg.ss.touch();                                      // a scan relayed from the scanner app
  assert.strictEqual(pg.ss.lastInput(), T0 + 9 * MIN, 'touch() is input');
  env.set(T0 + 19 * MIN - 1); pg.wake(); assert.strictEqual(pg.signOuts.length, 0, 'the scan moved the deadline to 19:00');
  env.set(T0 + 19 * MIN); pg.wake(); assert.strictEqual(pg.signOuts.length, 1); assert.strictEqual(env.ends()[0].at, T0 + 9 * MIN);
  // through the page's own scan queue: every offered scan is input (the order number is never sent anywhere here)
  env = makeEnv(today); pg = openPage(env, { queue: true }).init(); pg.signIn('Tess Welder'); await settle();
  const q = pg.win.StationScanQueue.create({ device: 'weld-1', signedIn: () => pg.loggedIn(), run: async () => {} });
  env.advance(8 * MIN); assert.strictEqual(q.offer('3521000777'), true);
  assert.strictEqual(pg.ss.lastInput(), T0 + 8 * MIN, 'a scan offered to the queue is input');
  assert(!JSON.stringify(env.posts).includes('3521000777'), 'the order is not in any session request');
  // touch(ts): never in the future, never older than the last input
  env.advance(1 * MIN); pg.ss.touch(env.clock.t + 3 * HOUR); assert.strictEqual(pg.ss.lastInput(), env.clock.t, 'a time in the future is now');
  const was = pg.ss.lastInput(); env.advance(5 * SEC); pg.ss.touch(was - 5 * MIN); assert.strictEqual(pg.ss.lastInput(), was, 'an older time changes nothing');
  // before init, touch() does not throw (the input is simply the page's, from then on)
  const bare = openPage(makeEnv(today)); bare.ss.touch(); assert.strictEqual(bare.ss.lastInput(), T0);
  // two pages (frames): the Design Station frame inside the sorter. Input in either counts for both; a stranger's message does not
  env = makeEnv(today);
  const parent = openPage(env, { station: 'sorter', device: 'charm-nest-1' }); parent.init();
  const child = openPage(env, { station: 'design', device: 'design-1', parent: parent.win, keys: 'frame.' }); child.init();
  parent.win.__frames = [{ contentWindow: child.win }];
  parent.signIn('Tess Welder'); child.signIn('Tess Welder'); await settle();
  env.advance(7 * MIN); child.win.fire('keydown');
  assert.strictEqual(child.ss.lastInput(), T0 + 7 * MIN); assert.strictEqual(parent.ss.lastInput(), T0 + 7 * MIN, 'input in the frame counts for the page that holds it');
  env.advance(1 * MIN); parent.win.fire('click');
  assert.strictEqual(child.ss.lastInput(), T0 + 8 * MIN, 'input in the page counts for its frame');
  const stranger = { __deliver: () => {} };
  parent.win.__deliver({ data: { source: 'station-session', type: 'input', at: env.clock.t + 1000 }, source: { other: true } });
  env.advance(2 * SEC); assert.strictEqual(parent.ss.lastInput(), T0 + 8 * MIN, 'a message from a window that is not its frame is ignored'); void stranger;
  env.set(T0 + 17 * MIN + 59 * SEC); parent.wake(); child.wake(); assert.strictEqual(parent.signOuts.length + child.signOuts.length, 0, 'both stay until 10 minutes after the last input at either');
  env.set(T0 + 18 * MIN); parent.wake(); child.wake(); assert.strictEqual(parent.signOuts.length, 1); assert.strictEqual(child.signOuts.length, 1);
  console.log('scan: touch() and the scan queue count as input (a time, never the order); frames pass the time of an input to each other and ignore strangers');
}

/* ── 7 · the first input after a gap; the payloads ── */
async function gapsAndPayloads() {
  // a computer asleep for 3 hours: the first input after it is NOT a reason to stay: the person is signed out first, at the last input
  let env = makeEnv(today); let pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(2 * MIN); pg.win.fire('keydown');
  env.sleep(3 * HOUR); pg.win.fire('mousemove');                              // no tick, no wake: the first thing that happens is a mouse move
  assert.strictEqual(pg.signOuts.length, 1, 'the person who was gone for 3 hours is signed out before the input counts');
  assert.strictEqual(env.ends()[0].at, T0 + 2 * MIN); assert.strictEqual(env.ends()[0].reason, 'idle');
  assert.strictEqual(pg.ss.lastInput(), env.clock.t, 'and the input is the page\'s now');
  // the heartbeat carries the last input; the start carries the sign-in
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  assert.strictEqual(env.starts()[0].lastInputAt, T0);
  env.advance(4 * MIN); pg.win.fire('keydown'); env.advance(1 * MIN + 1 * SEC); await settle();
  const beat = env.beats()[0]; assert(beat, 'a beat at 5 minutes'); assert.strictEqual(beat.lastInputAt, T0 + 4 * MIN, 'the beat carries the last input');
  assert(beat.lastInputAt <= beat.at);
  env.advance(3 * MIN); pg.win.fire('keydown'); const lastKey = env.clock.t; env.advance(2 * MIN + 30 * SEC); pg.ss.signedOut('signOut'); await settle();
  assert.strictEqual(env.ends().at(-1).lastInputAt, lastKey, 'the end carries the last input too');
  assert.strictEqual(env.ends().at(-1).reason, 'signOut');
  // a page that dies (no pagehide): a closed end after 15 quiet minutes is at the last input it knew
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(3 * MIN); pg.win.fire('keydown'); const L = env.clock.t; env.advance(2 * MIN + 1 * SEC);      // a beat at 5:00 carries the input of 3:00
  env.advanceTo(T0 + 5 * MIN + 5 * SEC);
  const beatAt = env.clock.t; env.sleep(20 * MIN);                                                             // frozen
  env.admin = {}; const p2 = openPage(env); p2.init(); await settle();
  assert.strictEqual(p2.signOuts.length, 1);
  assert(env.ends()[0].at <= beatAt, 'a page that went quiet ended at or before its last beat'); assert.strictEqual(env.ends()[0].at, L, 'at the last input it knew');
  // the person's page signing out through StationSession.signedOut("idle") itself also ends at the last input
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(4 * MIN); pg.ss.signedOut('idle'); assert.strictEqual(env.ends()[0].reason, 'idle'); assert.strictEqual(env.ends()[0].at, T0, 'ended at the last input');
  // after a rule sign-out, signing in again starts a new session and the clock is that sign-in
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(11 * MIN); await settle(); assert.strictEqual(pg.signOuts.length, 1);
  pg.signIn('Tess Welder'); await settle(); assert.strictEqual(env.starts().length, 2); assert.strictEqual(pg.ss.lastInput(), env.clock.t);
  env.advance(9 * MIN); await settle(); assert.strictEqual(pg.signOuts.length, 1, 'the new sign-in has its own 10 minutes');
  env.advance(1 * MIN + 10 * SEC); await settle(); assert.strictEqual(pg.signOuts.length, 2);
  // the midnight sign-out is as it was: a person with input up to 23:58 is signed out at midnight, reason midnight, not idle
  env = makeEnv('2026-10-07T03:50:00Z'); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();     // 23:50 EDT
  env.advanceTo(Z('2026-10-07T03:58:00Z')); pg.win.fire('keydown'); env.advanceTo(Z('2026-10-07T04:00:40Z')); await settle();
  assert.strictEqual(pg.signOuts.length, 1); assert.strictEqual(pg.signOuts[0].reason, 'midnight'); assert.strictEqual(env.ends()[0].reason, 'midnight');
  assert.strictEqual(env.ends()[0].at, Z('2026-10-07T04:00:00Z'));
  // a background tab: a hidden page's timers are slow (one a minute) but the time is read from the clock: signed out at the first one after 10 minutes, ended at the last input
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  pg.doc.visibilityState = 'hidden'; for (const x of env.clock.timers) if (x.every) x.every = 60000;
  env.advance(11 * MIN); await settle(); assert.strictEqual(pg.signOuts.length, 1); assert.strictEqual(env.ends()[0].at, T0);
  // AD2's contract: every start, beat and end carries lastInputAt AND sentAt (this page's clock when it was sent); an idle end's `at` is the last input
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  for (let i = 0; i < 3; i++) { env.advance(4 * MIN); pg.win.fire('keydown'); }
  const lastTouch = env.clock.t; env.advance(10 * MIN + 10 * SEC); await settle();
  const all = env.sessionPosts();
  assert(all.length >= 5 && all.every(x => x.sentAt > 0 && x.lastInputAt > 0), 'start, beats and end all carry sentAt and lastInputAt');
  assert(all.filter(x => x.event !== 'end').every(x => x.sentAt === x.at), 'a start or a beat: sentAt is the moment of sending');
  const idleEnd = env.ends()[0];
  assert.strictEqual(idleEnd.reason, 'idle'); assert.strictEqual(idleEnd.at, lastTouch, 'an idle end\'s at is the last input, not the time of the notice');
  assert.strictEqual(idleEnd.lastInputAt, lastTouch); assert(idleEnd.sentAt >= lastTouch + 10 * MIN, 'sentAt is the time it was sent, which is later than the end it records');
  // a beat the server answers `ended: true` with idle, closing or closed while this page has had input a moment ago (the server ended it on what it
  // knew, e.g. beats that did not arrive): nobody is thrown out of work; that session is over and a new one carries on from now. Another end
  // reason (midnight, a hand sign-out elsewhere) is left to the page's own rules.
  for (const [answer, reopens] of [['idle', true], ['closing', true], ['closed', true], ['midnight', false]]) {
    env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
    env.endBeats = { [env.starts()[0].id]: answer };
    env.advance(2 * MIN); pg.win.fire('keydown'); env.advance(3 * MIN + 5 * SEC); await settle();
    assert.strictEqual(pg.signOuts.length, 0, 'a beat answered ended/' + answer + ' does not sign out a person who has been working');
    assert.strictEqual(env.starts().length, reopens ? 2 : 1, 'a beat answered ended/' + answer + (reopens ? ': a new session carries on' : ': nothing new starts'));
    if (reopens) assert.strictEqual(env.ends()[0].reason, 'closed', 'the old session is closed on the record');
  }
  // the same answer for a person who really has been away 10 minutes: signed out at the last input (the page's own rule), never later
  for (const answer of ['idle', 'closing']) {
    env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
    env.endBeats = { [env.starts()[0].id]: answer };
    env.advance(2 * MIN); pg.win.fire('keydown'); const L2 = env.clock.t; env.advance(11 * MIN); await settle();
    assert.strictEqual(pg.signOuts.length, 1, 'idle for 11 minutes: signed out once'); assert.strictEqual(pg.signOuts[0].reason, 'idle'); assert.strictEqual(env.ends()[0].at, L2, 'at the last input');
  }
  // an end that could not be sent is sent again later with a fresh sentAt (the server undoes the clock with it) and the SAME end time
  env = makeEnv(today); env.sessionStatus = 503; pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(11 * MIN); await settle(); assert.strictEqual(pg.signOuts.length, 1);
  const firstEnd = env.ends()[0], firstSent = firstEnd.sentAt; env.sessionStatus = 0;
  env.advance(2 * MIN); await settle();
  const again = env.ends().filter(x => x.id === firstEnd.id);
  assert(again.length >= 2, 'the end was sent again once the door answered'); assert.strictEqual(again.at(-1).at, firstEnd.at); assert(again.at(-1).sentAt > firstSent, 'with a fresh sentAt');
  console.log('payloads: start, beats and the end carry lastInputAt and sentAt; the first input after a gap signs out first; an asleep or frozen page ends at the last input; a beat answered ended/idle for a person who is working opens a new session instead of throwing them out; midnight unchanged');
}

/* ── 7a · the computer's clock set back, and two tabs of one computer ── */
async function clockAndTabs() {
  /** the wall clock is set back; the timers (a monotonic clock) are not affected: they keep their distance from now */
  const setBack = (env, ms) => { env.clock.t -= ms; for (const x of env.clock.timers) x.at -= ms; };
  // a clock set 2 hours BACK under a person who has been idle: the input "in the future" must not hold the rule still for 2 hours
  let env = makeEnv(today), pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(2 * MIN); pg.win.fire('keydown'); setBack(env, 2 * HOUR); const back = env.clock.t;
  env.advance(9 * MIN); await settle(); assert.strictEqual(pg.signOuts.length, 0, 'not signed out 9 minutes after the clock was set back');
  env.advance(2 * MIN); await settle(); assert.strictEqual(pg.signOuts.length, 1, 'signed out 10 minutes after the change, not 2 hours');
  assert.strictEqual(pg.signOuts[0].reason, 'idle'); const e = env.ends()[0];
  assert(e.at >= back && e.at <= back + 15 * SEC, 'the end is about the moment of the change: ' + (e.at - back) + ' ms after it');
  assert.strictEqual(env.errors.length, 0, 'no page error');
  // the same with a person who then types: input after the change counts as usual
  env = makeEnv(today); pg = openPage(env).init(); pg.signIn('Tess Welder'); await settle();
  env.advance(2 * MIN); pg.win.fire('keydown'); setBack(env, 2 * HOUR);
  for (let i = 0; i < 4; i++) { env.advance(5 * MIN); pg.win.fire('keydown'); }
  assert.strictEqual(pg.signOuts.length, 0, 'a person working after the clock change stays in');
  env.advance(11 * MIN); await settle(); assert.strictEqual(pg.signOuts.length, 1, 'and is signed out 10 minutes after the last key');

  // two tabs of one computer share one sign-in: typing in either keeps the person in; the end is the last input of either
  env = makeEnv(today); const a = openPage(env).init(); a.signIn('Tess Welder'); await settle();
  const b = openPage(env).init(); await settle();
  env.advance(8 * MIN); b.win.fire('keydown'); const Lb = env.clock.t;
  env.advance(8 * MIN); await settle();
  assert.strictEqual(a.signOuts.length + b.signOuts.length, 0, 'the other tab\'s input keeps the person in (16 minutes after sign-in, 8 after the last key)');
  env.advance(3 * MIN); await settle();
  assert(a.signOuts.length + b.signOuts.length >= 1, 'signed out once both tabs were quiet for 10 minutes');
  const ends = env.ends(); assert(ends.length >= 1 && ends[0].reason === 'idle' && ends[0].at === Lb, 'the first end says idle at the last key of either tab: ' + JSON.stringify(ends.map(x => [x.reason, (x.at - Lb) / 1000])));
  assert(ends.every(x => x.id === ends[0].id), 'a second tab that sees the login gone only repeats the end of the same session (the door keeps the first)');
  assert.strictEqual(env.starts().length, 1, 'one session for the two tabs');
  console.log('clock and tabs: a clock set back is not input from the future (out 10 minutes after the change, at the change); typing after it counts; two tabs share one idle clock');
}

/* ── 7b · two people at one page (the Welding station's multi mode) ── */
async function multi() {
  /** a multi page: `list` is the page's own list of who is signed in; its signOut drops the person it is told about, as weld-1 does */
  const mk = (env, preset = []) => {
    const list = preset.map(p => Object.assign({}, p)), pg = openPage(env);
    pg.list = list;
    pg.init({
      multi: true, people: () => list.map(p => Object.assign({}, p)),
      signOut: (reason, who) => {
        pg.win.__signOuts.push({ reason, who, at: env.clock.t });
        const i = list.findIndex(p => p.name === who.name && (!who.task || p.task === who.task)); if (i >= 0) list.splice(i, 1);
      },
    });
    pg.add = (name, task) => { list.push({ name, id: null, task }); pg.ss.signedIn({ name, id: null, task }); };
    return pg;
  };
  const ppl = pg => pg.ss.people().map(p => p.name + '/' + p.task).sort().join(',');
  const who = pg => pg.signOuts.map(x => x.who.name + '/' + x.who.task).sort();
  const both = pg => { pg.add('Tess Welder', 'welding'); pg.add('Ray Matcher', 'matching'); };

  // both idle: the page's last input is the later sign-in (a sign-in is input at the page); both go 10:00 after it, each ended at it
  let env = makeEnv(today), pg = mk(env);
  pg.add('Tess Welder', 'welding'); env.advance(2 * MIN); pg.add('Ray Matcher', 'matching'); await settle();
  const sin = env.clock.t; assert.strictEqual(pg.ss.lastInput(), sin);
  env.advance(10 * MIN - 5 * SEC); await settle();
  assert.strictEqual(pg.signOuts.length, 0, 'nobody before 10 minutes without input at the page'); assert.strictEqual(ppl(pg), 'Ray Matcher/matching,Tess Welder/welding');
  env.advance(15 * SEC); await settle();
  assert.deepStrictEqual(who(pg), ['Ray Matcher/matching', 'Tess Welder/welding']); assert(pg.signOuts.every(x => x.reason === 'idle'));
  assert.deepStrictEqual(env.ends().map(e => e.at), [sin, sin]); assert(env.ends().every(e => e.reason === 'idle' && e.lastInputAt === sin && e.sentAt >= sin));
  assert.strictEqual(env.starts().length, 2); assert.strictEqual(ppl(pg), ''); assert.strictEqual(env.errors.length, 0);

  // a tap at the page keeps both; the idle clock then runs from the tap
  env = makeEnv(today); pg = mk(env); both(pg); await settle();
  env.advance(9 * MIN); pg.win.fire('mousemove'); const tap = env.clock.t;
  env.advance(9 * MIN + 30 * SEC); await settle();
  assert.strictEqual(pg.signOuts.length, 0, 'a tap at the page keeps both signed in');
  env.advance(1 * MIN); await settle();
  assert.strictEqual(pg.signOuts.length, 2); assert(env.ends().every(e => e.at === tap && e.reason === 'idle'), 'both end at that tap');

  // a scan credited to one person is input at the page: it keeps both
  env = makeEnv(today); pg = mk(env); both(pg); await settle();
  env.advance(9 * MIN); pg.ss.touch(env.clock.t, { name: 'Ray Matcher', task: 'matching' }); const scan = env.clock.t;
  env.advance(9 * MIN + 30 * SEC); await settle();
  assert.strictEqual(pg.signOuts.length, 0, 'a scan credited to one keeps both');
  env.advance(1 * MIN); await settle();
  assert.strictEqual(pg.signOuts.length, 2); assert(env.ends().every(e => e.at === scan));

  // an Admin in one task stays; the same non-Admin person in two tasks goes from both; one question a name
  env = makeEnv(today); env.admin = { 'paul k': true }; pg = mk(env);
  pg.add('Paul K', 'welding'); pg.add('Tess Welder', 'welding'); pg.add('Tess Welder', 'matching'); await settle();
  env.advance(10 * MIN + 10 * SEC); await settle();
  assert.deepStrictEqual(who(pg), ['Tess Welder/matching', 'Tess Welder/welding']); assert.strictEqual(ppl(pg), 'Paul K/welding', 'the Admin stays');
  assert.strictEqual(env.adminGets(), 2, 'one question a name, not a session');
  env.advance(3 * HOUR); await settle();
  assert.strictEqual(ppl(pg), 'Paul K/welding', 'still there 3 hours later'); assert.strictEqual(env.ends().length, 2); assert(env.beats().filter(b => b.person === 'Paul K').length >= 30);

  // one signs out by hand (a click is input): the other carries on and has the page's idle clock from that click
  env = makeEnv(today); pg = mk(env); both(pg); await settle();
  env.advance(4 * MIN); pg.win.fire('click'); const click = env.clock.t; pg.list.shift(); pg.ss.signedOut('signOut', { name: 'Tess Welder', task: 'welding' }); await settle();
  assert.strictEqual(env.ends().length, 1); assert.strictEqual(env.ends()[0].reason, 'signOut'); assert.strictEqual(ppl(pg), 'Ray Matcher/matching');
  env.advance(10 * MIN + 10 * SEC); await settle();
  assert.deepStrictEqual(who(pg), ['Ray Matcher/matching']); assert.strictEqual(env.ends().length, 2); assert.strictEqual(env.ends()[1].at, click);

  // 17:00 with a frozen page: everybody but the Admin is signed out "closing" at the last input; midnight is untouched
  env = makeEnv('2026-10-06T20:40:00Z'); env.admin = { 'paul k': true }; pg = mk(env);
  pg.add('Paul K', 'welding'); pg.add('Tess Welder', 'welding'); pg.add('Ray Matcher', 'matching'); await settle();
  const L0 = env.clock.t; env.sleep(25 * MIN); pg.wake();                             // found at 21:05, after 17:00 (21:00Z)
  assert.deepStrictEqual(who(pg), ['Ray Matcher/matching', 'Tess Welder/welding']); assert(pg.signOuts.every(x => x.reason === 'closing'));
  const nonAdminEnds = env.ends().filter(e => e.person !== 'Paul K'); assert(nonAdminEnds.length === 2 && nonAdminEnds.every(e => e.reason === 'closing' && e.at === L0), 'ended at the last input');
  assert(env.ends().filter(e => e.person === 'Paul K').every(e => e.reason === 'closed'), 'the Admin\'s page slept: the old closed rule, never closing'); assert.strictEqual(ppl(pg), 'Paul K/welding', 'the Admin is still signed in');
  // input inside the 10 minutes before 17:00: everybody stays, and Rule A runs from the last input
  env = makeEnv('2026-10-06T20:40:00Z'); pg = mk(env); both(pg); await settle();
  for (let i = 0; i < 4; i++) { env.advance(4 * MIN); pg.win.fire('keydown'); }       // the last at 20:56
  const last = env.clock.t; assert.strictEqual(hhmm(last), '16:56:00');
  env.advanceTo(Z('2026-10-06T21:00:30Z')); await settle();
  assert.strictEqual(pg.signOuts.length, 0, 'input inside the window: everybody stays at 17:00');
  env.advanceTo(Z('2026-10-06T21:06:20Z')); await settle();
  assert.strictEqual(pg.signOuts.length, 2); assert(pg.signOuts.every(x => x.reason === 'idle')); assert(env.ends().every(e => e.at === last));

  // a reload inside the 10 minutes: the same sessions go on (no new starts) and the load is input; one past 10 minutes lapses both while loading
  env = makeEnv(today); pg = mk(env); both(pg); await settle();
  const ids = env.starts().map(s => s.id).sort();
  env.advance(6 * MIN); pg.close(); await settle();
  let pg2 = mk(env, pg.list); await settle();
  assert.strictEqual(env.starts().length, 2, 'no new sessions'); assert.strictEqual(JSON.stringify(pg2.ss.people().map(p => p.session).sort()), JSON.stringify(ids), 'the same sessions');
  assert.strictEqual(pg2.ss.lastInput(), T0 + 6 * MIN, 'a reload is input');
  env.set(T0 + 15 * MIN + 59 * SEC); pg2.wake(); assert.strictEqual(pg2.signOuts.length, 0, 'not 9:59 after the reload');
  env.set(T0 + 16 * MIN + 1 * SEC); pg2.wake();
  assert.strictEqual(pg2.signOuts.length, 2); assert(env.ends().length === 2 && env.ends().every(e => e.at === T0 + 6 * MIN && e.reason === 'idle' && ids.includes(e.id)));
  env = makeEnv(today); pg = mk(env); both(pg); await settle();
  env.advance(3 * MIN); pg.win.fire('keydown'); env.advance(1 * MIN); pg.close(); env.sleep(11 * MIN);          // reopened 12 minutes after the last input
  pg2 = mk(env, pg.list); await settle();
  assert.strictEqual(pg2.signOuts.length, 2, 'both lapse while the page loads'); assert(pg2.signOuts.every(x => x.reason === 'idle'));
  assert.strictEqual(env.starts().length, 2, 'no session starts for a login that had lapsed'); assert(env.ends().length === 2 && env.ends().every(e => e.at === T0 + 3 * MIN));
  assert.strictEqual(ppl(pg2), '');

  // midnight (New York) signs everybody out, an Admin too, as "midnight" (input up to 23:58 does not matter)
  env = makeEnv('2026-10-07T03:50:00Z'); env.admin = { 'paul k': true }; pg = mk(env);
  pg.add('Paul K', 'welding'); pg.add('Tess Welder', 'welding'); await settle();
  for (let i = 0; i < 2; i++) { env.advance(4 * MIN); pg.win.fire('keydown'); }
  env.advanceTo(Z('2026-10-07T04:00:40Z')); await settle();
  assert.deepStrictEqual(who(pg), ['Paul K/welding', 'Tess Welder/welding']); assert(pg.signOuts.every(x => x.reason === 'midnight'));
  assert(env.ends().length === 2 && env.ends().every(e => e.reason === 'midnight' && e.at === Z('2026-10-07T04:00:00Z')));
  console.log('multi: input at the page counts for both people; each ends at the page\'s last input; an Admin stays; a hand sign-out leaves the other; 17:00; reload; midnight');
}

/* ── 8 · every page ── */
function pages() {
  const files = fs.readdirSync(root).filter(f => /\.html$/.test(f)).filter(f => /StationSession\.init\(/.test(fs.readFileSync(path.join(root, f), 'utf8')));
  assert(files.length >= 16, 'pages that call StationSession.init: ' + files.length);
  for (const f of files) {
    const html = fs.readFileSync(path.join(root, f), 'utf8');
    assert(/station-session\.js\?v=\d{8}-/.test(html), f + ' loads station-session.js with a version tag');
    const init = html.slice(html.indexOf('StationSession.init('));
    // the callback takes the reason: "signOut: reason =>", "signOut: (reason) =>", a named function(reason) handed over as signOut / stationSignOut
    const named = /StationSession\.init\(\{[^}]*signOut(?::\s*(\w+))?\s*[,}]/.exec(init.slice(0, 600));
    if (named) {
      const fn = named[1] || 'signOut';
      assert(new RegExp(`function\\s+${fn}\\s*\\(\\s*reason\\b`).test(html), f + ': ' + fn + '(reason) takes the reason');
    } else assert(/signOut:\s*\(?\s*reason\b/.test(init.slice(0, 1500)), f + ': signOut: reason => …');
    // and it words idle / closing from StationSession.notice (midnight keeps the page's own wording)
    const region = html.slice(Math.max(0, html.indexOf('StationSession.init(') - 4500), html.indexOf('StationSession.init(') + 2500);
    assert(/StationSession\s*&&\s*StationSession\.notice|StationSession\.notice\(/.test(region) || (/reason\s*===\s*["']idle["']/.test(region) && /reason\s*===\s*["']closing["']/.test(region) && /Signed out after 10 minutes without input/.test(region))
      || (/\bidle\s*:\s*["']Signed out after 10 minutes without input/.test(html) && /\bclosing\s*:\s*["']Signed out at/.test(html)),        // (a map of the reasons to words, as design-1 keeps)
      f + ': its sign-out notice uses StationSession.notice(reason), or words idle and closing itself');
    assert(!/\bif\s*\(\s*reason\s*===\s*["']midnight["']\s*\)\s*\{?\s*(?:try\s*\{)?\s*(?:ui\.banner|toast|M\.toast)/.test(region), f + ': the notice is not limited to midnight');
  }
  console.log('pages: ' + files.length + ' pages call StationSession.init; each signOut takes the reason and words idle / closing');
}

/* ── 9 · the real pages, the real module, Playwright's clock: ten minutes with no input ── */
const ORIGIN = 'http://station.test';
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
const mStub = `window.__toasts = [];
  window.M = (() => { const inst = new Map();
    const mk = () => { const i = { isOpen: false, open() { i.isOpen = true; }, close() { i.isOpen = false; } }; return i; };
    const of = el => { if (!inst.has(el)) inst.set(el, mk()); return inst.get(el); };
    const any = { init: el => of(el), getInstance: el => of(el) };
    return { AutoInit() {}, toast(o) { __toasts.push(o && o.html); }, updateTextFields() {}, textareaAutoResize() {},
      Modal: any, Tabs: any, Dropdown: any, Tooltip: any, Collapsible: any, Sidenav: any,
      FormSelect: { init: () => ({ getSelectedValues: () => [] }), getInstance: () => ({ getSelectedValues: () => [] }) } }; })();`;
const NYDAY = '2026-10-06';
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 12000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(60); } }

/** a page of the real site with the real station-session.js: the stubs are Firebase, Materialize, jQuery and the functions; nothing leaves the machine */
async function openReal(browser, spec, rec) {
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url()), m = r.request().method();
    const json = (body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: 'window.$=window.jQuery=()=>({on(){},ready(){}});' });
    if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
    if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
    if (u.origin !== ORIGIN) return r.abort();
    if (u.pathname.startsWith('/.netlify/functions/')) {
      const fn = u.pathname.split('/').pop(), text = r.request().postData() || '';
      let b = {}; try { b = JSON.parse(text || '{}'); } catch (_) {}
      if (fn === 'firebaseOrders' && m === 'POST' && b.session) { rec.sessions.push(b.session); return json({ success: true }); }
      if (fn === 'firebaseOrders' && m === 'POST' && b.stationAdmin !== undefined) { rec.gets.push(text); return json({ ok: true, admin: spec.admin === true }); }
      if (fn === 'authGate') return m === 'GET' ? json({ locked: true }) : (r.request().headers()['x-edit-passcode'] === 'pc-1' ? json({ ok: true }) : json({ ok: false }, 401));
      if (fn === 'etsyMailAuth') {
        rec.posts.push(b.op);
        if (b.op === 'currentUser') return json({ ok: true, username: 'tess', displayName: 'Tess Inbox', role: 'operator' });
        return json({ ok: true });
      }
      if (u.searchParams.get('cancelCheck')) return json({ success: true, cancelled: {}, now: Date.now() });
      return json({ success: true, data: {} });
    }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  await ctx.addInitScript(seed => {
    if (sessionStorage.getItem('seeded')) return; sessionStorage.setItem('seeded', '1');
    for (const [k, v] of Object.entries(seed.local || {})) localStorage.setItem(k, v);
    for (const [k, v] of Object.entries(seed.session || {})) sessionStorage.setItem(k, v);
  }, { local: Object.assign({ station_signin_day: NYDAY }, spec.local), session: spec.session });
  const page = await ctx.newPage();
  page.on('pageerror', e => { if (!/gstatic\.com\/firebasejs/.test(String(e))) rec.errors.push(String(e && e.message || e)); });
  await page.clock.install({ time: new Date(spec.at || '2026-10-06T14:00:00Z') });
  await page.goto(ORIGIN + '/' + spec.file);
  await until(() => page.evaluate(() => !!(window.StationSession && StationSession.current && StationSession.current())), spec.file + ': a session runs');
  if (spec.ready) await until(() => page.evaluate(spec.ready), spec.file + ': the page is ready');
  await page.clock.runFor(2000);
  return { ctx, page };
}

async function pageIdle(browser, spec) {
  const rec = { sessions: [], errors: [], gets: [], posts: [] };
  const { ctx, page } = await openReal(browser, spec, rec);
  if (spec.work) await spec.work(page);                                           // work on screen (typed with the page's own fill)
  const li = await page.evaluate(() => StationSession.lastInput());
  const startEv = await until(() => rec.sessions.find(x => x.event === 'start') || rec.sessions[0], spec.file + ': a session start was sent');
  // 9:20 with no input: still signed in
  await page.clock.runFor(9 * 60000 + 20000);
  assert.strictEqual(rec.sessions.filter(x => x.event === 'end').length, 0, spec.file + ': not signed out at 9:20');
  assert.strictEqual(await page.evaluate(() => !!StationSession.current()), true);
  if (spec.stillIn) assert.strictEqual(await page.evaluate(spec.stillIn), true, spec.file + ': still signed in on the page at 9:20');
  // ten minutes: signed out (one second at a time, so the page's own notice is read while it is still on screen)
  for (let i = 0; i < 90 && !rec.sessions.some(x => x.event === 'end'); i++) { await page.clock.runFor(1000); await wait(25); }
  const end = await until(() => rec.sessions.find(x => x.event === 'end'), spec.file + ': the idle end');
  assert.strictEqual(end.reason, 'idle', spec.file + ': the end reason'); assert.strictEqual(end.at, li, spec.file + ': the end is the last input (not the time it was noticed)');
  assert.strictEqual(end.lastInputAt, li);
  assert(end.at <= startEv.at + 60000, spec.file + ': ended at the last input, not 10 minutes later');
  await spec.after(page, { li });
  assert.strictEqual(await page.evaluate(() => StationSession.current()), null, spec.file + ': no session runs');
  assert.deepStrictEqual(rec.errors, [], spec.file + ': no page errors: ' + rec.errors.join('; '));
  await ctx.close();
  return rec;
}

async function browserPages() {
  let chromium;
  try { chromium = require(path.join(process.argv[3] || process.env.PW_DIR || path.join(root, 'node_modules'), 'playwright-core')).chromium; } catch (_) { console.log('browser: skipped (no Playwright: set PW_DIR)'); return; }
  const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(CHROME)) { console.log('browser: skipped (no Chromium: set CHROMIUM)'); return; }
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const IDLE_TEXT = 'Signed out after 10 minutes without input';
  const fake = () => String(100000 + Math.floor(Math.random() * 900000));         // a made-up number for this run, never printed
  const pinKeys = { employee_id: fake(), employee_name: 'Tess Welder' };
  const body = page => page.evaluate(() => document.body.innerText);
  const pinPage = file => ({
    file, local: pinKeys, ready: () => window.isEmployeeLoggedIn === true,
    stillIn: () => window.isEmployeeLoggedIn === true && !!localStorage.getItem('employee_id'),
    work: async page => { await page.fill('#etsyOrderNumber', '3521000777'); },
    after: async page => {
      const st = await page.evaluate(() => ({ id: localStorage.getItem('employee_id'), name: localStorage.getItem('employee_name'), in: window.isEmployeeLoggedIn,
        open: M.Modal.getInstance(document.getElementById('userLoginModal')).isOpen, order: document.getElementById('etsyOrderNumber').value, toast: (window.__toasts || []).join(' | ') }));
      assert.strictEqual(st.id, null, file + ': the login key is cleared'); assert.strictEqual(st.name, null); assert.notStrictEqual(st.in, true);
      assert.strictEqual(st.open, true, file + ': the PIN box is back'); assert.strictEqual(st.order, '3521000777', file + ': the work on screen stays');
      assert(st.toast.includes(IDLE_TEXT) && !/midnight/i.test(st.toast), file + ': the notice says why: ' + st.toast);
    }
  });
  /** weld-1 keeps a roster of the people signed in (weld_people), so the sign-out takes the person out of the roster; the number box is back when the roster is empty */
  const weldPage = () => Object.assign(pinPage('weld-1.html'), {
    after: async page => {
      const st = await page.evaluate(() => ({ roster: JSON.parse(localStorage.getItem('weld_people') || '[]').map(p => p.name), in: window.isEmployeeLoggedIn,
        open: M.Modal.getInstance(document.getElementById('userLoginModal')).isOpen, order: document.getElementById('etsyOrderNumber').value, toast: (window.__toasts || []).join(' | ') }));
      assert.deepStrictEqual(st.roster, [], 'weld-1.html: the roster is empty'); assert.notStrictEqual(st.in, true);
      assert.strictEqual(st.open, true, 'weld-1.html: the number box is back'); assert.strictEqual(st.order, '3521000777', 'weld-1.html: the work on screen stays');
      assert(st.toast.includes(IDLE_TEXT) && !/midnight/i.test(st.toast), 'weld-1.html: the notice says why: ' + st.toast);
    }
  });
  try {
    await pageIdle(browser, weldPage()); console.log('browser: weld-1.html idle at 10:00 -> out of the roster, number box back, order stays, notice says why, end at the last input');
    for (const f of ['assembly-1.html', 'shipping-1.html', 'design-message-1.html']) { await pageIdle(browser, pinPage(f)); console.log('browser: ' + f + ' idle at 10:00 -> PIN box, login cleared, order stays, notice says why, end at the last input'); }
    // an Admin (the door says so) stays on the same page
    {
      const rec = { sessions: [], errors: [], gets: [], posts: [] };
      const spec = Object.assign(pinPage('weld-1.html'), { admin: true, local: { employee_id: pinKeys.employee_id, employee_name: 'Paul K' } });
      const { ctx, page } = await openReal(browser, spec, rec);
      await page.clock.runFor(2 * 3600000);
      assert.strictEqual(await page.evaluate(() => window.isEmployeeLoggedIn === true && !!StationSession.current()), true, 'an Admin is still signed in after 2 hours without input');
      assert(!rec.sessions.some(x => x.event === 'end'), 'no end was sent');
      assert(rec.sessions.some(x => x.event === 'beat' && x.lastInputAt > 0), 'his beats go on and carry lastInputAt');
      assert.strictEqual(rec.gets.length, 1, 'the door was asked once');
      await ctx.close();
      console.log('browser: weld-1.html an Admin (the door says so) is still signed in after 2 hours without input, asked once');
    }
    // only a person's own input counts: every kind of event a script dispatches (dispatchEvent, el.click(), a remote cursor) is not input; a real click is
    {
      const rec = { sessions: [], errors: [], gets: [], posts: [] };
      const { ctx, page } = await openReal(browser, pinPage('weld-1.html'), rec);
      const li0 = await page.evaluate(() => StationSession.lastInput());
      await page.clock.runFor(9 * 60000 + 20000);
      await page.evaluate(() => {
        for (const t of ['pointerdown', 'pointermove', 'mousedown', 'mousemove', 'keydown', 'wheel', 'touchstart', 'input', 'click', 'paste']) {
          document.dispatchEvent(new Event(t, { bubbles: true })); window.dispatchEvent(new Event(t)); document.body.dispatchEvent(new Event(t, { bubbles: true }));
        }
        document.body.dispatchEvent(new MouseEvent('mousemove', { bubbles: true, clientX: 5, clientY: 5 }));
        document.body.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, key: 'a' }));
        document.body.click();
      });
      assert.strictEqual(await page.evaluate(() => StationSession.lastInput()), li0, 'events a script dispatched are not input');
      for (let i = 0; i < 90 && !rec.sessions.some(x => x.event === 'end'); i++) { await page.clock.runFor(1000); await wait(25); }
      const end = await until(() => rec.sessions.find(x => x.event === 'end'), 'the idle end');
      assert.strictEqual(end.reason, 'idle'); assert.strictEqual(end.at, li0, 'the scripted events did not move the deadline: ended at the last real input');
      await ctx.close();
      const rec2 = { sessions: [], errors: [], gets: [], posts: [] };
      const r2 = await openReal(browser, pinPage('weld-1.html'), rec2);
      await r2.page.clock.runFor(9 * 60000 + 20000);
      await r2.page.mouse.click(30, 30);                                           // a real click (the browser's own, isTrusted true)
      const lc = await r2.page.evaluate(() => StationSession.lastInput());
      assert(lc >= end.at + 9 * 60000, 'a real click is input: ' + lc);
      await r2.page.clock.runFor(60000);
      assert.strictEqual(rec2.sessions.filter(x => x.event === 'end').length, 0, 'a real click at 9:20 keeps the person signed in past 10:00');
      await r2.ctx.close();
      console.log('browser: weld-1.html only trusted events are input: scripted events (dispatchEvent, click(), keydown, mousemove) never keep anybody awake, a real click does');
    }
    // the Design Station: the name and this tab's passcode go and its own sign-in box asks again; the notice says why
    for (const file of ['design.html', 'design-1.html']) {
      await pageIdle(browser, { file, session: { 'designStation.passcode': 'pc-1' }, local: { employee_name: 'Dana Design' },
        stillIn: () => !!localStorage.getItem('employee_name'),
        after: async page => {
          await until(() => page.$('#pcGate'), file + ': the station\'s own sign-in box');
          const st = await page.evaluate(() => ({ name: localStorage.getItem('employee_name'), pc: sessionStorage.getItem('designStation.passcode') }));
          assert.deepStrictEqual(st, { name: null, pc: null }, file + ': the name and this tab\'s passcode go');
          assert((await body(page)).includes(IDLE_TEXT), file + ': the notice says why');
        } });
      console.log('browser: ' + file + ' idle at 10:00 -> name and passcode go, the station\'s own sign-in box, notice says why');
    }
    // the Inbox: the token goes, the server session ends, the sign-in screen covers the inbox with the reason
    await pageIdle(browser, { file: 'etsy-mail-1.html', local: { etsymail_session: 'tok-1', etsymail_session_profile: JSON.stringify({ username: 'tess', displayName: 'Tess Inbox', role: 'operator', cachedAtMs: Date.parse('2026-10-06T14:00:00Z') }) },
      ready: () => document.body.classList.contains('authed'),
      stillIn: () => document.body.classList.contains('authed'),
      after: async page => {
        const st = await page.evaluate(() => ({ tok: localStorage.getItem('etsymail_session') || sessionStorage.getItem('etsymail_session'), authed: document.body.classList.contains('authed'),
          banner: document.getElementById('siBanner').classList.contains('show') && document.getElementById('siBannerText').textContent }));
        assert(st.tok === null && st.authed === false && typeof st.banner === 'string' && st.banner.startsWith(IDLE_TEXT + '.'), 'inbox after idle (the token goes, the sign-in screen says why): ' + JSON.stringify(st));
      } });
    console.log('browser: etsy-mail-1.html idle at 10:00 -> token gone, sign-in screen with the reason');
    // Sorting: the "Sorting as" name is the sign-in: cleared, the chip asks again, the notice says why
    for (const file of ['sorting.html', 'sorting-2.html']) {
      await pageIdle(browser, { file, local: { 'sorting.employee': 'Tess Welder' }, stillIn: () => !!localStorage.getItem('sorting.employee'),
        after: async page => {
          const st = await page.evaluate(() => ({ name: localStorage.getItem('sorting.employee'), via: localStorage.getItem('sorting.employeeVia'), text: document.body.innerText + ' | ' + (window.__toasts || []).join(' | ') }));
          assert.strictEqual(st.name, null, file + ': the name is cleared'); assert.strictEqual(st.via, null);
          assert(st.text.includes(IDLE_TEXT) && /Set your name/.test(st.text), file + ': the chip asks again and the notice says why');
        } });
      console.log('browser: ' + file + ' idle at 10:00 -> name cleared, the chip asks again, notice says why');
    }
    // the Sorter (charm-nest-1): the name it keeps is the sign-in; 10 minutes without input clears it, CN's own notice says why, the run is left alone
    {
      const { start } = require('../charm-nest/bridge-server.cjs');
      const srv = await start({ receipts: [] });
      try {
        const rec = { sessions: [], errors: [] };
        const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
        const js = b => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body: b });
        await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
          const u = r.request().url();
          if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
          if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
          return r.abort();
        });
        await ctx.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), r => {
          let j = null; try { j = JSON.parse(r.request().postData() || 'null'); } catch (_) {}
          if (r.request().method() === 'POST' && j && j.session) { rec.sessions.push(j.session); return r.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: '{"success":true}' }); }
          return r.fallback();
        });
        await ctx.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off', sandboxStream: 'off' })); window.prompt = () => null; } catch (_) {} });
        const page = await ctx.newPage(); page.setDefaultTimeout(30000);
        page.on('pageerror', e => rec.errors.push(e.message));
        await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
        await page.waitForFunction(() => window.CN && window.CNEmployee && window.StationSession && window.B && CN.S.cloud.ok === true, null, { timeout: 60000 });
        await page.evaluate(() => { B.employee = 'Tess Welder'; });
        await page.waitForFunction(() => window.CNRole && CNRole.state() === 'ask');                        // (not the Admin: asked Laser or Design once; nobody is signed in until it is answered)
        await page.evaluate(() => { CNRole.choose('laser'); });
        await page.waitForFunction(() => StationSession.current() && StationSession.current().person === 'Tess Welder' && StationSession.current().station === 'laser');
        const li = await page.evaluate(() => StationSession.lastInput());
        const shift = ms => page.evaluate(m => { if (!window.__realNow) window.__realNow = Date.now; Date.now = () => window.__realNow() + m; document.dispatchEvent(new Event('visibilitychange')); }, ms);
        await shift(10 * 60000 - 2000); await wait(300);
        assert(!rec.sessions.some(x => x.event === 'end'), 'sorter: not signed out before 10 minutes');
        await shift(10 * 60000 + 30000);
        const end = await until(() => rec.sessions.find(x => x.event === 'end'), 'sorter: the idle end');
        assert.strictEqual(end.reason, 'idle'); assert(end.at >= li && end.at <= li + 5000, 'sorter: ended at the last input'); assert.strictEqual(end.station, 'laser', 'sorter: the end is under the role');
        const st = await page.evaluate(() => ({ name: localStorage.getItem('cn.employee'), role: CNRole.role(), b: B.employee, cur: StationSession.current(), toasts: [...document.querySelectorAll('#toasts .m')].map(n => n.textContent).join(' | ') }));
        assert.strictEqual(st.name, null); assert.strictEqual(st.role, '', 'sorter: the role goes with the name'); assert.strictEqual(st.b, ''); assert.strictEqual(st.cur, null);
        assert(st.toasts.includes(IDLE_TEXT) && /asked again/.test(st.toasts), 'sorter: the notice says why: ' + st.toasts);
        assert.deepStrictEqual(rec.errors, [], 'sorter: no page errors: ' + rec.errors.join('; '));
        await ctx.close();
        console.log('browser: charm-nest-1.html idle at 10:00 -> the name is cleared, the notice says why, the run is left alone');
      } finally { srv.close(); }
    }
  } finally { await browser.close(); }
}

(async () => {
  const only = process.argv[2];
  const parts = { ruleA, admin, ruleB, dst, reload, scansAndFrames, gapsAndPayloads, clockAndTabs, multi, pages, browserPages };
  for (const [name, fn] of Object.entries(parts)) { if (only && only !== name) continue; await fn(); }
  console.log('auto-signout: all passed');
})().catch(e => { console.error(e); process.exit(1); });
