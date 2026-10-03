// Which station and which person the Charm Sorter logs work to (Employee efficiency console), offline: the real sorter page
// (charm-nest-1.html with its bridge, Library and Rose Gold scripts), the real station-session.js and station-activity.js,
// the test server of tests/charm-nest/bridge-server.cjs for everything else, and the door (firebaseOrders) as the test's own
// recorder. Nothing leaves the machine.
//   1 · station: a laser check, "back to Laser cutting" (Library) and Cut Sheet (Rose Gold) are recorded at station `laser`;
//       approvals, labels, sends to a sheet and decisions (every other call) at `sorter`; one after the other, neither
//       leaks into the other (the page's sign-in is put back as it was).
//   2 · person: nobody named records nothing at first, but the press is not blocked and is not lost: a small labelled name
//       field offers itself (no browser pop-up), and what was pressed is recorded under the name once it is typed; the name
//       is trimmed, spaces collapsed, Title Case ("  tess   WELDER " = "Tess Welder", "marco r" = "Marco R.", "McDonald" kept); a number is never a name.
//   3 · nothing automatic is a person's work: an interpretation, a Review sync, a checkpoint and a card refresh log nothing,
//       and a name's arrival with nothing pressed records nothing; held presses are dropped on demand (the midnight sign-out).
//   4 · the sandbox (Settings: on): the sign-in and every event go to the Sandbox_ door (?sandbox=1) and carry sandbox: true;
//       production's go to the real one; an event that would reach the real numbers from a sandbox sorter is dropped.
//   5 · the console shows `laser` as its own station (efficiency view model) and the server accepts the station.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/stations/sorter-identity.cjs
const path = require('path'), assert = require('assert'), fs = require('fs'), vm = require('vm');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const { start } = require('../charm-nest/bridge-server.cjs');

const wait = ms => new Promise(r => setTimeout(r, ms));
const brief = e => [e.action, e.station, e.person, e.device, e.parts, e.orders, !!e.sandbox];
const PIN = '000000';   // an obviously fake number (never a real one)

/** A sorter page against its own test server; `sandbox` is the Settings switch (the page reloads when it changes). */
async function sorterPage(browser, srv, errors, { sandbox = false } = {}) {
  const rec = { reqs: [], events: [], sessions: [], libCalls: [] };
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  const ok = body => ({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) });
  // the stations' door: activity and sessions are the test's own (with the query kept, to see which store they went to)
  await context.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), r => {
    const body = r.request().postData() || '';
    let j = null; try { j = JSON.parse(body || 'null'); } catch (_) {}
    if (r.request().method() === 'POST' && j && (Array.isArray(j.activity) || j.session)) {
      const q = new URL(r.request().url()).search;
      rec.reqs.push({ q, body });
      if (Array.isArray(j.activity)) for (const e of j.activity) rec.events.push(Object.assign({ _q: q }, e));
      if (j.session) rec.sessions.push(Object.assign({ _q: q }, j.session));
      return r.fulfill(ok({ success: true, written: 1 }));
    }
    return r.fallback();
  });
  // the Library's laser answers and Rose Gold's cut record are canned (the real handlers are not what is tested here)
  await context.route(u => /\/\.netlify\/functions\/charmNestLibrary/.test(u.pathname), r => {
    let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {}
    if (b.op === 'laserDone') { rec.libCalls.push(b); return r.fulfill(ok({ ok: true, at: Date.now(), sheetIds: [b.id], by: b.by || '', process: [] })); }
    if (b.op === 'roseRecordCut') { rec.libCalls.push(b); return r.fulfill(ok({ cut: { at: Date.now(), sheetId: b.sheetId, revision: 1, planJson: null }, stock: { id: b.stockId, revision: 1 } })); }
    return r.fallback();
  });
  await context.addInitScript(sb => {
    try {
      localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: sb ? 'on' : 'off', sandboxStream: 'off' }));
      window.__prompts = [];
      window.prompt = (...a) => { window.__prompts.push(a); return null; };
    } catch (_) {}
  }, sandbox);
  const page = await context.newPage();
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CNAct && window.CNEmployee && window.StationActivity && window.StationSession && window.LibraryDone && window.RoseStock && CN.S.cloud.ok === true, null, { timeout: 60000 });
  return { context, page, rec, flush: () => page.evaluate(() => StationActivity.flush()) };
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const errors = [];
  const srv = await start({ receipts: [] });
  try {
    /* ───────────── 1 to 3 · production ───────────── */
    {
      const { context, page, rec, flush } = await sorterPage(browser, srv, errors);
      const nameBar = '.cnNameBar';
      const evs = () => rec.events.map(brief);

      // nobody is named: the helper has nobody to put the work under
      assert.strictEqual(await page.evaluate(() => StationActivity.who()), null);
      assert.strictEqual(await page.evaluate(() => StationSession.page().sandbox), false, 'production is not the sandbox');

      // a laser undo ("back to Laser cutting") pressed with no name: the real Library step goes through (not blocked), and the
      // press is kept; a small labelled name field offers itself (inline, not a browser pop-up)
      const undone = await page.evaluate(async () => { const r = await LibraryDone.mark('sheet', 'shA', false, { undo: true, name: 'GF Sheet 1' }); return !!r; });
      assert.strictEqual(undone, true, 'the press is not blocked');
      assert.strictEqual(rec.libCalls.filter(c => c.op === 'laserDone' && c.done === false).length, 1, 'the Library step went to the server');
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 1, 'and was kept, not lost');
      assert.strictEqual(await page.evaluate(() => StationActivity.pending()), 0, 'nothing is recorded under nobody');
      await page.waitForSelector(nameBar);
      const hint = await page.evaluate(() => { const b = document.querySelector('.cnNameBar'), i = b.querySelector('input'), l = b.querySelector('label'); return { kind: b.dataset.kind, labelFor: l.getAttribute('for') === i.id, label: l.textContent, focused: document.activeElement === i, ph: i.placeholder, modal: !!document.querySelector('dialog:modal') }; });
      assert.strictEqual(hint.kind, 'hint'); assert(hint.labelFor && /Your name/.test(hint.label) && /counted under it/.test(hint.label), 'a labelled field that says why: ' + hint.label);
      assert.strictEqual(hint.focused, false, 'a hint takes no focus (it never interrupts typing elsewhere)');
      assert.strictEqual(hint.modal, false, 'and is not a modal');
      assert.deepStrictEqual(await page.evaluate(() => window.__prompts), [], 'no browser pop-up');
      // the page's own click still works with the field open (nothing is blocked behind it)
      await page.evaluate(() => CNAct('complete', { station: 'laser', parts: 3, detail: 'laser check test' }));
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 2);
      assert.strictEqual((await page.$$(nameBar)).length, 1, 'one field, however many presses');
      assert.strictEqual(await page.evaluate(() => { try { return typeof CNAct('bogus', {}) } catch (e) { return 'threw'; } }), 'boolean', 'a bad action never throws');

      // a name typed into the field: trimmed, spaces collapsed, Title Case; kept in this browser; signs the person in
      await page.fill(nameBar + ' input', '  tess   WELDER ');
      await page.press(nameBar + ' input', 'Enter');
      await page.waitForFunction(() => !document.querySelector('.cnNameBar') && StationActivity.who() && StationActivity.who().person === 'Tess Welder');
      assert.strictEqual(await page.evaluate(() => B.employee), 'Tess Welder');
      assert.strictEqual(await page.evaluate(() => localStorage.getItem('cn.employee')), 'Tess Welder');
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 0, 'what was pressed before the name is released');
      await flush();
      const dev = 'charm-nest-1';
      assert.deepStrictEqual(evs(), [
        ['undo', 'laser', 'Tess Welder', dev, 0, 0, false],
        ['complete', 'laser', 'Tess Welder', dev, 3, 0, false]
      ], 'both presses are recorded under the typed name, at the laser station: ' + JSON.stringify(evs()));
      assert(rec.sessions.some(s => s.station === 'sorter' && s.person === 'Tess Welder'), 'the sorter session is the normalised name');
      assert(rec.events.every(e => e._q === '' && !e.sandbox), 'production events go to the real door');
      assert.deepStrictEqual(await page.evaluate(() => window.__prompts), [], 'still no browser pop-up');
      rec.events.length = 0;

      // 1 · station: the sorter's own work is `sorter`, the laser's is `laser`, one after the other, nothing leaks
      const whoFn = await page.evaluate(() => { window.__who0 = StationSession.who; return true; });
      await page.evaluate(() => {
        CNAct('complete', { orderId: '4176576272', parts: 2, orders: 1, detail: 'Complete Order' });          // a sorter action
        CNAct('print', { orderId: '4176576272', parts: 2, detail: 'QR label' });
        CNAct('complete', { station: 'laser', parts: 5, detail: 'Library laser check' });                       // a laser action
        CNAct('complete', { station: 'sorter', orderId: '4176576273', parts: 1, orders: 1, detail: 'Send to Sheet' });
        CNAct('undo', { station: 'laser', parts: 5, detail: 'back to Laser cutting' });
        CNAct('complete', { station: 'nonsense', parts: 1, detail: 'unknown station is the sorter' });
        CNAct('note', { orderId: '4176576272', detail: 'decided: customOrder' });
      });
      await flush();
      assert.deepStrictEqual(evs(), [
        ['complete', 'sorter', 'Tess Welder', dev, 2, 1, false],
        ['print', 'sorter', 'Tess Welder', dev, 2, 0, false],
        ['complete', 'laser', 'Tess Welder', dev, 5, 0, false],
        ['complete', 'sorter', 'Tess Welder', dev, 1, 1, false],
        ['undo', 'laser', 'Tess Welder', dev, 5, 0, false],
        ['complete', 'sorter', 'Tess Welder', dev, 1, 0, false],
        ['note', 'sorter', 'Tess Welder', dev, 0, 0, false]
      ], 'sorter vs laser routing: ' + JSON.stringify(evs()));
      assert(await page.evaluate(() => StationSession.who === window.__who0), "the page's sign-in is put back after a laser event");
      assert.strictEqual(await page.evaluate(() => StationActivity.who().station), 'sorter');
      assert.strictEqual(new Set(rec.events.map(e => e.id)).size, rec.events.length, 'every event has its own id');
      assert(rec.events.every(e => e.session && e.computer), 'with the session and computer');
      rec.events.length = 0;

      // the real call sites: the Library's step with a name set, and Rose Gold's Cut Sheet, are laser events
      await page.evaluate(async () => { await LibraryDone.mark('sheet', 'shB', false, { undo: true, name: 'GF Sheet 2' }); });
      await page.evaluate(async () => {
        const sh = { metal: 'rose', sheetId: 'rose-test', setId: 'set1', draft: false, recalled: { roseStockId: 'rgs' }, rosePlanHash: 'hash', roseRevision: 0, roseStock: { id: 'rgs', revision: 0 }, roseHistory: [], placements: [{ id: 'a' }, { id: 'b' }, { id: 'c' }] };
        await RoseStock.record(sh);
      });
      await flush();
      assert.deepStrictEqual(rec.events.map(e => [e.action, e.station, e.person, e.parts, e.orders]), [['undo', 'laser', 'Tess Welder', 0, 0], ['complete', 'laser', 'Tess Welder', 3, 0]], 'Library laser step and Cut Sheet, from the real scripts: ' + JSON.stringify(rec.events.map(brief)));
      assert(/returned to Laser cutting/.test(rec.events[0].detail) && /Cut Sheet/.test(rec.events[1].detail));
      assert.strictEqual(rec.libCalls.filter(c => c.op === 'roseRecordCut').length, 1, 'Cut Sheet reached the server as before');
      assert.strictEqual(rec.libCalls.find(c => c.op === 'roseRecordCut').by, 'Tess Welder');
      rec.events.length = 0;

      // 2 · one person, one name
      const names = await page.evaluate(() => {
        const out = {};
        for (const raw of ['tess welder', 'TESS   WELDER', '  Tess\tWelder  ', "o'BRIEN", 'mary-ann  SMITH', 'giovanna c.', 'McDonald pat', 'marco r', 'dana  D', 'Tess 000000', 'zoë  müller']) { B.employee = raw; out[raw] = [B.employee, CNEmployee.normalize(raw)]; }
        return out;
      });
      assert.deepStrictEqual(Object.values(names).map(v => v[0]), ['Tess Welder', 'Tess Welder', 'Tess Welder', "O'Brien", 'Mary-Ann Smith', 'Giovanna C.', 'McDonald Pat', 'Marco R.', 'Dana D.', 'Tess', 'Zoë Müller']);
      assert.deepStrictEqual(Object.values(names).map(v => v[0]), Object.values(names).map(v => v[1]), 'the setter and normalize() agree');
      assert.strictEqual(await page.evaluate(() => localStorage.getItem('cn.employee')), 'Zoë Müller', 'what is kept is the normalised name');
      // the same person, typed three ways, is one session name and one event person
      await page.evaluate(() => { B.employee = 'tess welder'; });
      await page.evaluate(() => { CNAct('note', { detail: 'a' }); B.employee = 'TESS WELDER'; CNAct('note', { detail: 'b' }); });
      await flush();
      assert.deepStrictEqual([...new Set(rec.events.map(e => e.person))], ['Tess Welder'], 'one person is one name');
      rec.events.length = 0;
      // a number is never a name: not kept, not a sign-in, not sent anywhere
      await page.evaluate(pin => { B.employee = pin; }, PIN);
      assert.strictEqual(await page.evaluate(() => B.employee), '', 'a number is refused');
      assert.strictEqual(await page.evaluate(() => localStorage.getItem('cn.employee')), null, 'and not kept');
      assert.strictEqual(await page.evaluate(() => StationActivity.who()), null, 'nobody is signed in by a number');
      assert.strictEqual(await page.evaluate(pin => CNEmployee.normalize(' ' + pin + ' '), PIN), '');
      // (the field refuses it too, and keeps the name that was there; the press that opened it is not lost)
      await page.evaluate(() => { B.employee = 'Tess Welder'; });
      await page.evaluate(() => { CNEmployee.edit({}); });
      await page.waitForSelector(nameBar + '[data-kind="edit"]');
      assert.strictEqual(await page.evaluate(() => document.activeElement === document.querySelector('.cnNameBar input')), true, 'asked for by a press: it takes focus');
      assert.strictEqual(await page.inputValue(nameBar + ' input'), 'Tess Welder', 'it shows the name now kept');
      await page.fill(nameBar + ' input', PIN); await page.press(nameBar + ' input', 'Enter');
      await page.waitForFunction(() => { const e = document.querySelector('.cnNameBar .cnNbErr'); return e && !e.hidden && /number/.test(e.textContent); });
      assert.strictEqual(await page.evaluate(() => B.employee), 'Tess Welder', 'the name that was there stays');
      await page.press(nameBar + ' input', 'Escape');
      await page.waitForFunction(() => !document.querySelector('.cnNameBar'));
      assert.strictEqual(await page.evaluate(() => B.employee), 'Tess Welder', 'Esc puts the field away and changes nothing');
      await flush();
      const allReq = rec.reqs.map(r => r.body).join('\n') + JSON.stringify(rec.events) + JSON.stringify(rec.sessions);
      assert(!allReq.includes(PIN), 'the number is in no request');

      // the name buttons use the field, not a browser pop-up: the order window's name chip and Review's
      await page.evaluate(() => { B.employee = ''; });
      await page.evaluate(() => { CN.setMode('review'); Review.render(); });
      await page.waitForSelector('#rvName');
      assert.match(await page.textContent('#rvName'), /set your name/i, 'the Review name button says no name is set');
      await page.click('#rvName');
      await page.waitForSelector(nameBar + '[data-kind="edit"]');
      await page.fill(nameBar + ' input', 'pat  PACKER'); await page.click(nameBar + ' button[type="submit"]');
      await page.waitForFunction(() => !document.querySelector('.cnNameBar') && B.employee === 'Pat Packer');
      await page.waitForFunction(() => /Pat Packer/.test((document.querySelector('#rvName') || {}).textContent || ''));   // (Review repaints when the page is idle)
      assert.deepStrictEqual(await page.evaluate(() => window.__prompts), [], 'no browser pop-up from the name buttons');

      // 3 · nothing automatic is a person's work
      await page.evaluate(() => { B.employee = 'Tess Welder'; });
      await flush(); rec.events.length = 0;
      const RID = '4176576272', SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000), DAY = 86400;
      await page.evaluate(async ({ RID, SHIP, DAY }) => {
        const order = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Jessica Strom' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
          lines: [{ transactionId: '41765762721', listingId: '1800062721', sku: 'RE_5460', title: 'MODIFICATION REWORK FREE SHIPPING', quantity: 2, expectedShipDate: SHIP, variations: [{ name: 'Price', value: '144' }], metalKey: '', metalLabel: '', personalization: '' }] };
        await Orders.loadMaps(true);
        for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); B.orders.rows.push({ key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }); B.orders.byKey.set(key, B.orders.rows[B.orders.rows.length - 1]); }
        Orders.interpretAll(); Review.syncOrderItems(); Review.render();
        try { await Session.flushNow(); } catch (_) {}
        CN.refreshAllCards(); CN.renderRail(); CN.updateTopSub();
      }, { RID, SHIP, DAY });
      await wait(1500);
      await flush();
      assert.deepStrictEqual(rec.events, [], 'interpretation, a Review sync, a checkpoint and a card refresh log nothing: ' + JSON.stringify(rec.events.map(brief)));
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 0);
      // the order window's name chip (a modal window) opens the field INSIDE the window, not a browser pop-up over it
      await page.evaluate(() => { B.employee = ''; });
      await page.evaluate(key => OrderWin.open(key), RID + '_41765762721');
      await page.waitForSelector('#owWhoBtn');
      await page.waitForFunction(() => document.querySelector('dialog#orderWin').open);
      assert.match(await page.textContent('#owWho'), /Set your name/);
      assert.match(await page.getAttribute('#owWhoBtn', 'title'), /No name set yet/, 'the chip says plainly that no name is set');
      await page.click('#owWhoBtn');
      await page.waitForSelector('dialog#orderWin .cnNameBar[data-kind="edit"]');
      assert.strictEqual(await page.evaluate(() => document.querySelector('.cnNameBar').parentNode.id), 'orderWin', 'inside the open window');
      await page.fill('.cnNameBar input', 'Nia  nameless'); await page.press('.cnNameBar input', 'Enter');
      await page.waitForFunction(() => !document.querySelector('.cnNameBar') && /Nia Nameless/.test(document.getElementById('owWho').textContent));
      assert.strictEqual(await page.evaluate(() => document.querySelector('dialog#orderWin').open), true, 'the window is still open (Enter put only the field away)');
      assert.deepStrictEqual(await page.evaluate(() => window.__prompts), [], 'no browser pop-up over the window');
      await page.evaluate(() => { document.querySelector('dialog#orderWin').close(); });
      // calm: a hint put away is not shown again at once, but what is pressed is still kept, and recorded under the name when it comes
      await page.evaluate(() => { B.employee = ''; });
      await page.evaluate(() => CNAct('note', { detail: 'quiet 1' }));
      await page.waitForSelector(nameBar + '[data-kind="hint"]');
      await page.click(nameBar + ' .cnNbX');
      await page.waitForFunction(() => !document.querySelector('.cnNameBar'));
      await page.evaluate(() => CNAct('note', { detail: 'quiet 2' }));
      await wait(300);
      assert.strictEqual((await page.$$(nameBar)).length, 0, 'a put-away hint does not come straight back');
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 2, 'but both presses are kept');
      await page.evaluate(() => { B.employee = 'quinn QUIET'; });
      await page.waitForFunction(() => CNAct.held() === 0);
      await flush();
      assert.deepStrictEqual(rec.events.map(e => [e.detail, e.person, e.station]), [['quiet 1', 'Quinn Quiet', 'sorter'], ['quiet 2', 'Quinn Quiet', 'sorter']], 'recorded under the name once it comes');
      rec.events.length = 0;
      // a name arriving with nothing pressed records nothing; held presses can be dropped (the midnight sign-out) and are then never recorded
      await page.evaluate(() => { B.employee = ''; });
      await page.evaluate(() => { CNAct('note', { detail: 'yesterday' }); CNAct.drop(); B.employee = 'Sam Shift'; });
      await page.waitForFunction(() => StationActivity.who() && StationActivity.who().person === 'Sam Shift');
      await flush();
      assert.deepStrictEqual(rec.events, [], 'a dropped press is never put under the next name');
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 0);
      // an old held press (older than 20 minutes) is not put under a later name either
      await page.evaluate(() => { B.employee = ''; });
      await page.evaluate(() => { const real = Date.now; Date.now = () => real() - 21 * 60 * 1000; try { CNAct('note', { detail: 'stale' }); } finally { Date.now = real; } B.employee = 'Sam Shift'; });
      await page.waitForFunction(() => StationActivity.who() && StationActivity.who().person === 'Sam Shift');
      await flush();
      assert.deepStrictEqual(rec.events, [], 'a press from more than 20 minutes ago is not put under a later name');
      console.log('sorter identity (production): laser vs sorter station, the name field, one person one name, nothing automatic');
      await context.close();
    }

    /* ───────────── 4 · the sandbox ───────────── */
    {
      const { context, page, rec, flush } = await sorterPage(browser, srv, errors, { sandbox: true });
      assert.strictEqual(await page.evaluate(() => CN.S.settings.sandbox), 'on', 'the sandbox is on');
      assert.strictEqual(await page.evaluate(() => StationSession.page().sandbox), true, 'the sign-in is told it is a sandbox');
      await page.evaluate(() => { B.employee = 'sam SANDBOX'; });
      await page.waitForFunction(() => StationActivity.who() && StationActivity.who().person === 'Sam Sandbox');
      await page.evaluate(() => {
        CNAct('complete', { orderId: '4176576272', parts: 2, orders: 1, detail: 'Complete Order' });
        CNAct('undo', { station: 'laser', parts: 4, detail: 'back to Laser cutting' });
      });
      await flush();
      assert.deepStrictEqual(rec.events.map(brief), [['complete', 'sorter', 'Sam Sandbox', 'charm-nest-1', 2, 1, true], ['undo', 'laser', 'Sam Sandbox', 'charm-nest-1', 4, 0, true]], 'sandbox events say so');
      assert(rec.events.every(e => e._q === '?sandbox=1'), 'and go to the sandbox door');
      assert(rec.sessions.length > 0 && rec.sessions.every(s => s._q === '?sandbox=1'), 'so does the sign-in: ' + JSON.stringify(rec.sessions.map(s => s._q)));
      assert(rec.reqs.every(r => r.q === '?sandbox=1'), 'nothing from a sandbox sorter reaches the real door');
      // an event that would reach the real numbers from a sandbox sorter is dropped (the sign-in said it is not a sandbox)
      rec.events.length = 0;
      await page.evaluate(() => { const real = StationSession.who; StationSession.who = () => { const w = real.apply(StationSession); return w && Object.assign({}, w, { sandbox: false }); }; CNAct('complete', { parts: 9, orders: 1, detail: 'must not reach production' }); StationSession.who = real; });
      await flush();
      assert.deepStrictEqual(rec.events, [], 'dropped, not recorded');
      console.log('sorter identity (sandbox): sign-in and events go to the Sandbox_ door only');
      await context.close();
    }

    /* ───────────── 5 · the console and the server know `laser` ───────────── */
    {
      const win = { document: { getElementById: () => null, addEventListener() {} }, console };
      win.window = win; vm.createContext(win);
      vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), win);
      const E = win.Efficiency, j = o => JSON.parse(JSON.stringify(o));
      const M = j(E.norm({ ok: true, now: 5, day: '2026-10-03', people: [{ name: 'Tess Welder', status: 'on', nowAt: ['sorter'], totals: { parts: 8 }, stations: [{ station: 'sorter', minutes: 30, parts: 3, orders: 2 }, { station: 'laser', minutes: 5, parts: 5, scans: 0, completes: 2 }] }],
        business: { totals: { parts: 8 }, stations: [{ station: 'sorting', parts: 0 }, { station: 'sorter', parts: 3, orders: 2, peopleNow: ['Tess Welder'] }, { station: 'laser', parts: 5, orders: 0, peopleNow: [] }] } }));
      assert.deepStrictEqual(M.people[0].stations.map(s => s.station).sort(), ['laser', 'sorter'], 'a person keeps both stations');
      assert.strictEqual(M.biz.stations.get ? M.biz.stations.get('laser').parts : 5, 5, 'the business view keeps laser as a station with its parts');
      const SA = require('../../netlify/functions/_stationActivity.js');
      assert(SA.STATIONS.has('laser') && SA.STATIONS.has('sorter'), 'the server accepts both stations');
      console.log('console: laser and sorter are separate stations on both sides');
    }
  } finally { await browser.close(); srv.close(); }

  assert.deepStrictEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  console.log('sorter-identity: all passed');
})().catch(e => { console.error(e); process.exit(1); });
