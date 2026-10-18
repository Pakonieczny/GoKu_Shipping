// ONE signed-in person (Paul, 18 Oct 2026: "I should not be seeing my name logged in and also again my name when logging information about a
// given user activities. The user that is officially logged in must have all attributes and actions recorded under that particular signed in
// user"). The real sorter page (charm-nest-1.html) framing the real Design Station (design-1.html, another origin, its own browser storage
// holding ANOTHER person's name), over tests/charm-nest/bridge-server.cjs in the sandbox. Nothing leaves the machine; every name is synthetic.
//   1 · the sorter never takes the frame's stored name: nobody signed in here stays nobody after the hello, and the frame's own name is cleared
//   2 · signing in on the sorter signs the frame in under that person: what the frame records (completedBy, the order timeline's labelPrinted
//       `by`, its own station session, the activity log) carries the sorter's signed-in name
//   3 · switching person on the sorter switches the frame (its session and what it records); signing out on the sorter signs the frame out
//   4 · while the sorter drives it, the frame's own "Set your name" gate never sets a different person: nobody signed in → it points to the
//       sorter, and the sorter opens its Account panel; the sign-in there reaches the frame and the action goes through
//   5 · the frame's own name in Design Station standalone (no sorter) is untouched by any of this (DsName.driven() false, its gate as before)
//   PW_DIR=… node tests/stations/sorter-frame-identity.cjs
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('../charm-nest/bridge-server.cjs');

const day = Math.floor(Date.now() / 1000);
const receipt = (rid, i) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + i, country_iso: 'US', status: 'Paid', is_paid: true, is_shipped: false, create_timestamp: day - 3600 * i, created_timestamp: day - 3600 * i, update_timestamp: day - 3600 * i, transactions: [{ transaction_id: rid * 10 + 1, receipt_id: rid, listing_id: 1718001, sku: 'BR-TST-01', title: 'charm', quantity: 1, expected_ship_date: day + 3 * 86400 }] });
const wait = ms => new Promise(r => setTimeout(r, ms));
const FRAME_OWN = 'Frame Fran';          // the name the station's own browser storage holds: never the sorter's person

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  const snapPath = 'charmnest/sandbox/orders-frame-identity.json', snapshot = [receipt(4170000201, 1), receipt(4170000202, 2)];
  st.blobs.set(snapPath, { buf: Buffer.from(JSON.stringify({ at: Date.now(), count: snapshot.length, receipts: snapshot })), generation: 1, meta: { contentType: 'application/json', metadata: {} } });
  st.put('Charm_Sandbox', 'current', { path: snapPath, count: snapshot.length, at: Date.now(), takenBy: 'test', source: 'etsy-pull' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  async function context() {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.addInitScript(({ station, own }) => {
      if (location.origin === station && !sessionStorage.getItem('__seeded')) { sessionStorage.setItem('__seeded', '1'); localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', own); }
      window.confirm = () => true; window.alert = () => {};
    }, { station: stationOrigin, own: FRAME_OWN });
    return ctx;
  }
  const watch = (page, who) => page.on('pageerror', e => errors.push(`${who}: ${e.message}`));
  try {
    /* ── the sorter framing the station ── */
    const ctx = await context(), page = await ctx.newPage(); watch(page, 'sorter');
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await page.evaluate(station => { Object.assign(CN.S.settings, { dsOrigin: station, sandbox: 'on', sandboxStream: 'off', pollOrders: 'off' }); CN.saveSettings(); }, stationOrigin);
    await page.reload(); await page.waitForFunction(() => window.CN && window.DesignLink && window.CNAccount && CN.S.cloud.ok, null, { timeout: 60000 });
    await page.evaluate(() => window.CNRole && CNRole.setAdminLookup(async () => true));   // (the Admin's sign-in: no "Laser or Design?" step; that step has its own test)
    assert.strictEqual(await page.evaluate(() => B.employee), '', 'nobody is signed in to the sorter');
    await page.evaluate(() => CN.setMode('design')); await page.evaluate(() => DesignLink.ensure());
    const frame = page.frames().find(f => /design-1\.html/.test(f.url()));
    assert(frame, 'the sorter framed the station');
    const fr = () => frame.evaluate(() => ({ name: localStorage.getItem('employee_name'), driven: DsName.driven(), session: (StationSession.current() || {}).person || null, who: (StationActivity.who() || {}).person || null }));

    // 1 · the sorter never adopts the station's stored name; nobody here is nobody there
    await frame.waitForFunction(() => typeof Bridge === 'object' && Bridge.active() && Bridge.employee() === '', null, { timeout: 30000 });
    assert.strictEqual(await page.evaluate(() => B.employee), '', 'the hello did not put the station\'s own name on the sorter');
    assert.strictEqual(await page.evaluate(() => CNEmployee.name()), '', 'employeeName() is the sorter\'s sign-in only');
    assert.strictEqual(await page.evaluate(() => CNAccount.view().name), '', 'the account chip says nobody');
    assert.deepStrictEqual(await fr(), { name: null, driven: true, session: null, who: null }, 'and the station\'s own name is gone: nothing is recorded under it');
    console.log('1 · the frame\'s own stored name is never the sorter\'s person, and is signed out while nobody is signed in here');

    // 2 · signing in on the sorter signs the frame in under that person; what the frame records carries it
    await page.evaluate(() => CNAccount.signIn('tess  WELDER'));
    await frame.waitForFunction(() => localStorage.getItem('employee_name') === 'Tess Welder', null, { timeout: 15000 });
    await frame.waitForFunction(() => (StationSession.current() || {}).person === 'Tess Welder', null, { timeout: 15000 });
    assert.deepStrictEqual(await fr(), { name: 'Tess Welder', driven: true, session: 'Tess Welder', who: 'Tess Welder' });
    const rec = await frame.evaluate(() => {
      const out = { tl: [] }, T = window.OrderTimeline, real = T.record;
      T.record = e => { out.tl.push({ type: e.type, by: e.by, signed: e.data && e.data.signedIn }); return true; };
      try { DsTimeline.printed({ gold: ['4170000201'] }, [{ ids: ['4170000201'] }], 'print'); } finally { T.record = real; }
      out.completedBy = buildArchiveRecord('4170000201', { receipt: { receipt_id: 4170000201 }, transactions: [] }).completedBy;
      out.gate = DsName.need('Print labels');
      return out;
    });
    assert.strictEqual(rec.completedBy, 'Tess Welder', 'completedBy is the sorter\'s signed-in person');
    assert(rec.tl.length >= 1 && rec.tl.every(e => e.by === 'Tess Welder' && e.signed !== false), 'the order timeline\'s labelPrinted is by the sorter\'s person: ' + JSON.stringify(rec.tl));
    assert.strictEqual(rec.gate, true, 'the frame\'s gate lets the action through under that person');
    assert.strictEqual(await page.evaluate(() => B.employee), 'Tess Welder', 'the sorter keeps its person');
    console.log('2 · signed in on the sorter: the frame\'s completedBy, timeline `by`, station session and activity carry that person');

    // 3 · switching person on the sorter switches the frame; a sign-out signs it out
    await page.evaluate(() => CNAccount.signIn('Pat Packer'));
    await frame.waitForFunction(() => localStorage.getItem('employee_name') === 'Pat Packer' && (StationSession.current() || {}).person === 'Pat Packer', null, { timeout: 15000 });
    assert.strictEqual(await frame.evaluate(() => buildArchiveRecord('4170000202', { receipt: {}, transactions: [] }).completedBy), 'Pat Packer', 'the switch reaches what the frame records');
    assert.strictEqual(await frame.evaluate(() => (StationActivity.who() || {}).person), 'Pat Packer');
    await page.evaluate(() => CNAccount.signOut());
    await frame.waitForFunction(() => localStorage.getItem('employee_name') === null && !StationSession.current(), null, { timeout: 15000 });
    assert.strictEqual(await frame.evaluate(() => buildArchiveRecord('4170000202', { receipt: {}, transactions: [] }).completedBy), 'Design', 'signed out: nobody\'s name, never the old one');
    assert.strictEqual(await page.evaluate(() => B.employee), '');
    console.log('3 · a switch of person on the sorter switches the frame; a sign-out there signs the frame out');

    // 4 · nobody signed in to the sorter: the frame's gate never takes a typed name; it points to the sorter, which opens its Account panel
    await page.evaluate(() => CNAccount.close && CNAccount.close());
    const g = await frame.evaluate(() => { const ok = DsName.need('Print labels'); const b = document.getElementById('dsNameGate'), i = document.getElementById('dsNameInput'); return { ok, shown: !!b && !b.hidden, inputShown: !!i && !i.hidden && i.offsetParent !== null, text: b ? b.textContent : '' }; });
    assert.strictEqual(g.ok, false, 'nobody signed in: the action waits');
    assert(g.shown && !g.inputShown && /Charm Sorter/.test(g.text), 'the gate points to the sorter and has no field to type another person: ' + JSON.stringify(g));
    assert.strictEqual(await frame.evaluate(() => { const i = document.getElementById('dsNameInput'); i.value = 'Typed Person'; i.dispatchEvent(new Event('blur')); return localStorage.getItem('employee_name'); }), null, 'the gate\'s field never signs a different person in while the sorter drives');
    await page.waitForSelector('#cnAcctPanel', { timeout: 10000 });
    await page.evaluate(() => CNAccount.signIn('dana  d'));
    await frame.waitForFunction(() => localStorage.getItem('employee_name') === 'Dana D.', null, { timeout: 15000 });
    assert.strictEqual(await frame.evaluate(() => DsName.need('Print labels')), true, 'signed in on the sorter: the action goes through under that person');
    console.log('4 · driven by the sorter, the frame\'s gate points to the sorter; the sign-in there reaches the frame');

    // the sorter shows the name once: no Employee card on the Design Station tab, no pill in Review
    assert.strictEqual(await page.$('#dsEmployee'), null, 'no Employee card on the Design Station tab');
    assert.strictEqual(await page.$('#dsSetEmployee'), null);
    await ctx.close();

    /* 5 · the station on its own: its name is its own, exactly as before */
    const ctx2 = await context(), solo = await ctx2.newPage(); watch(solo, 'station');
    await solo.goto(`${stationOrigin}/design-1.html?sandbox=1`);
    await solo.waitForFunction(() => typeof DsName === 'object' && typeof Bridge === 'object' && window.StationSession && StationSession.page(), null, { timeout: 30000 });
    assert.deepStrictEqual(await solo.evaluate(() => ({ name: localStorage.getItem('employee_name'), driven: DsName.driven(), sorter: Bridge.employee(), need: DsName.need('Print labels') })), { name: FRAME_OWN, driven: false, sorter: null, need: true }, 'standalone: the station\'s own name, not driven, the gate as before');
    await solo.evaluate(() => DsName.clear());
    const g2 = await solo.evaluate(() => { const ok = DsName.need('Print labels'); const b = document.getElementById('dsNameGate'), i = document.getElementById('dsNameInput'); return { ok, inputShown: !!i && !i.hidden, text: b.textContent, focused: document.activeElement === i }; });
    assert(!g2.ok && g2.inputShown && g2.focused && /Set your name/.test(g2.text), 'standalone, nobody named: the "Set your name" field, focused, as before: ' + JSON.stringify(g2));
    await solo.fill('#dsNameInput', 'ana  b'); await solo.press('#dsNameInput', 'Enter');
    assert.strictEqual(await solo.evaluate(() => localStorage.getItem('employee_name')), 'Ana B.', 'standalone: the typed name is the station\'s person');
    console.log('5 · the station on its own keeps its own name and gate, as before');
    await ctx2.close();
  } finally { await browser.close(); srv.close(); }
  const fatal = errors.filter(e => !/firebase stub|Failed to fetch|NetworkError|net::ERR/.test(e));
  assert.deepStrictEqual(fatal, [], 'no page errors: ' + fatal.join(' | '));
  console.log('sorter-frame-identity: all passed');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
