// The LASER station, fully wired into the Employee efficiency portal (stations round 2, SA5; Paul, 6 Oct 2026). The real Sorter page
// (charm-nest-1.html: its bridge, Library, flow, sheet window and Rose Gold scripts, the new charm-nest-laser-act.js), the real
// station-session.js and station-activity.js, and the REAL doors (firebaseOrders for sessions, activity and the live board;
// employeeEfficiency for the reads) over the in-memory Firestore of tests/charm-nest/bridge-server.cjs. Nothing real is touched: no
// network, no real order, a synthetic passcode.
//   1 · a person with no role (Admin, a Sorting person): Approve for laser cutting (the real button), the laser cut and its Undo (the real
//       Library step), "put back in progress" (the real flow) and Rose Gold's Cut Sheet are each written at station `laser` with the
//       person's NAME, ONE EVENT FOR EACH ORDER of the sheet (order id, sheet id, its charms), never as a sorter press; a failed step
//       writes nothing; a sorter press (complete order) is still the sorter's
//   2 · the signed-in role (R4; StationSession.role(), or the session's station): a Laser person's presses are all Laser, a Design
//       person's all Design (laser-side ones too); the page's sign-in is put back after each; the live board shows the sheet and the
//       order in hand at the role's station (person, time since start), ends on a cut, and follows a role switch
//   3 · nobody named: the cut is kept with its orders and recorded under the name once typed (nothing lost, nothing blocked)
//   4 · the portal reads it: the person's order list (personOrders, station laser) has both orders; the live board (op live) has the
//       Laser card's sheet; the rollup counts the cut's charms
//   NODE_PATH=... PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/stations/laser-wired.cjs
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const { start } = require('../charm-nest/bridge-server.cjs');
const { seed } = require('../charm-nest/laser-workflow.cjs');

const wait = ms => new Promise(r => setTimeout(r, ms));
const PASS = 'synthetic-pass-not-real', S = 'Charm_Nest_Sheets', DEV = 'charm-nest-1';

(async () => {
  // ── 0 · wiring that needs no browser: the new file is loaded by the page and shipped by the build
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), build = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');
  assert(/<script src="charm-nest-laser-act\.js\?v=[^"]+"><\/script>/.test(html), 'charm-nest-1.html loads charm-nest-laser-act.js');
  assert(html.indexOf('charm-nest-laser-act.js') < html.indexOf('<script src="charm-nest-bridge.js'), 'before the bridge (which routes CNAct)');
  assert(/"charm-nest-laser-act\.js"/.test(build), 'scripts/build-public.cjs ships it');
  // a set committed into the framed Design Station (and its Undo) is recorded by the Sorter, in the same run of steps that does it
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  assert(/DesignLink\.call\("complete\.commit"[\s\S]{0,2500}humanAct\.set\(false, r\.completed, set\.name\)[\s\S]{0,300}committed: \$\{r\.completed\.length\} order\(s\) marked design-complete/.test(bridge), 'Sets.commit records the set after the station committed it');
  assert(/DesignLink\.call\("complete\.undo"[\s\S]{0,900}humanAct\.set\(true, reopened, set\.name\)/.test(bridge), 'Sets.undo records the undo after the station reopened the orders');

  const srv = await start({ receipts: [] });
  seed(srv);
  const fnDir = path.join(root, 'netlify/functions'), { st } = srv;
  st.fn.firebaseOrders = require(path.join(fnDir, 'firebaseOrders.js')).handler;
  st.fn.employeeEfficiency = require(path.join(fnDir, 'employeeEfficiency.js')).handler;
  st.put('config', 'editPasscode', { passcode: PASS });
  const now = Date.now();
  { const run = st.doc('Charm_Nest_Runs', 'run-fixture'); run.lines['3700000301_1'] = { orderId: '3700000301', state: 'written', quantity: 2, poolIds: ['3700000301_1_1', '3700000301_1_2'], engraveCandidate: false }; run.lines['3700000302_1'] = { orderId: '3700000302', state: 'written', quantity: 1, poolIds: ['3700000302_1_1'], engraveCandidate: false }; st.put('Charm_Nest_Runs', 'run-fixture', run); }
  const img = st.doc(S, 'ready-sheet');
  // a held sheet of two orders (GF Sheet 4): three charms, two for 3700000301 and one for 3700000302
  st.put(S, 'held-sheet', Object.assign({}, img, { id: 'held-sheet', setId: null, setSeq: null, sheetIndex: 4, metal: 'gold', poolIds: ['3700000301_1_1', '3700000301_1_2', '3700000302_1_1'], orders: ['3700000301', '3700000302'], placedCount: 3, charmCount: 3, label: { files: [{ path: 'held-qr.png', url: img.label.files[0].url, payload: '3700000301', orders: ['3700000301', '3700000302'] }] }, laserHold: { at: now - 5000, by: 'Someone', note: 'Moved back to In progress' }, laserDoneAt: undefined, laserDoneBy: undefined }));

  const call = async (name, body) => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/${name}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return r.json(); };
  const events = () => st.list('Station_Activity').sort((a, b) => (a.at - b.at) || (a.seq - b.seq));
  const brief = e => [e.action, e.station, e.person, e.orderId, e.line, e.parts, e.orders];
  const byOrder = (a, b) => (a.orderId < b.orderId ? -1 : a.orderId > b.orderId ? 1 : 0);

  /** LD1's real sign-in at the Sorter: a name, then "Laser or Design?" (the Admin, by the server's answer, is not asked and has no role) */
  const signIn = async (page, name, as) => {
    await page.evaluate(([n, adm]) => { CNRole.setAdminLookup(async () => adm); B.employee = n; }, [name, as === 'admin']);
    if (as !== 'admin') { await page.waitForFunction(() => CNRole.state() === 'ask'); await page.evaluate(r => CNRole.choose(r), as); }
    await page.waitForFunction(([n, r]) => { const w = StationActivity.who(); return !!w && w.person === n && (w.role || '') === r; }, [name, as === 'admin' ? '' : as]);
  };
  const signOut = async page => { await page.evaluate(() => { StationSession.signedOut('signOut'); try { localStorage.removeItem('cn.employee'); } catch (_) {} B.employee = ''; }); await page.waitForFunction(() => CNRole.state() === 'none'); };

  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const errors = [];
  /** a sorter page on the fake backend; `name` is the person kept in cn.employee ('' = nobody named) */
  async function sorterPage(name) {
    const rec = { live: [] };
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    // the live board's writes go to the REAL door too (the test server hands the door only sessions and activity), and are kept to look at
    await context.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), async r => {
      const req = r.request(); let j = null; try { j = JSON.parse(req.postData() || 'null'); } catch (_) {}
      if (req.method() === 'POST' && j && j.live) {
        rec.live.push(j.live);
        const out = await st.fn.firebaseOrders({ httpMethod: 'POST', headers: {}, queryStringParameters: Object.fromEntries(new URL(req.url()).searchParams), body: req.postData() });
        return r.fulfill({ status: out.statusCode || 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: out.body || '{}' });
      }
      return r.fallback();
    });
    // Rose Gold's cut record is canned (the stock ledger is not what is tested here); every other call is the real handler
    await context.route(u => /\/\.netlify\/functions\/charmNestLibrary/.test(u.pathname), r => {
      let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {}
      if (b.op === 'roseRecordCut') return r.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ cut: { at: Date.now(), sheetId: b.sheetId, revision: 1, planJson: null }, stock: { id: b.stockId, revision: 1 } }) });
      return r.fallback();
    });
    await context.addInitScript(n => {
      try {
        localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off', sandboxStream: 'off' }));
        if (n) localStorage.setItem('cn.employee', n); else localStorage.removeItem('cn.employee');
        window.__prompts = []; window.prompt = (...a) => { window.__prompts.push(a); return null; };
      } catch (_) {}
    }, name);
    const page = await context.newPage();
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.CNAct && window.CNLive && window.CNLaserAct && window.CNRole && window.StationActivity && window.StationSession && window.LibraryDone && window.LibraryFlow && window.RoseStock && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const flush = async () => { await page.evaluate(() => StationActivity.flush()); await wait(700); };
    /** the events written since the last call; waits for `n` of them (a flush and a look every half second, 20 s at most) */
    let seen = events().length;
    const fresh = async n => {
      for (let i = 0; i < 40; i++) { await flush(); if (events().length - seen >= n) break; await wait(500); }
      const all = events(), out = all.slice(seen); seen = all.length; return out;
    };
    const none = async () => { await flush(); await wait(600); await flush(); assert.strictEqual(events().length, seen, 'nothing was written: ' + JSON.stringify(events().slice(seen).map(brief))); };
    return { context, page, rec, flush, fresh, none };
  }

  try {
    /* ───────────── 1 · no role (the Admin): the laser-side presses, one event for each order ───────────── */
    {
      const { context, page, rec, flush, fresh, none } = await sorterPage('');
      await signIn(page, 'Tess Welder', 'admin');
      await page.evaluate(() => CN.setMode('library')); await wait(600); await page.evaluate(() => CN.setMode('library'));   // (shown once; asked again as the first answers arrive)
      await wait(4000);
      for (let i = 0; ; i++) {      // (a load-timing flake of the page's first Library show: asked again, never more than three times)
        try { await page.waitForSelector('.approveBox[data-approve-for="sheet:held-sheet"][data-mode="ready"] [data-approve-btn]', { state: 'visible', timeout: 15000 }); break; }
        catch (e) { if (i >= 2) throw e; await page.evaluate(() => CN.setMode('library')); await wait(1500); }
      }
      assert.deepStrictEqual(await page.evaluate(() => [StationSession.role(), StationSession.who().station]), ['', 'sorter'], 'no role: the sorter, as before');

      // Approve for laser cutting, the real button: the hold lifted and the person's ready seal, ONE press
      await page.click('.approveBox[data-approve-for="sheet:held-sheet"] [data-approve-btn]');
      let ev = (await fresh(2)).sort(byOrder);
      assert.deepStrictEqual(ev.map(brief), [['note', 'laser', 'Tess Welder', '3700000301', 'held-sheet', 0, 0], ['note', 'laser', 'Tess Welder', '3700000302', 'held-sheet', 0, 0]], 'Approve: a laser event for each order, with the sheet: ' + JSON.stringify(ev.map(brief)));
      assert(ev.every(e => e.detail === 'approved for laser cutting · GF Sheet 4' && e.device === DEV && e.session && !e.sandbox), 'its words, device and session: ' + ev[0].detail);
      await none();
      assert.strictEqual(st.doc(S, 'held-sheet').processSeals.filter(s => s.how === 'laserReady').length, 1, 'the real flow sealed it (the seal names the person: ' + (st.doc(S, 'held-sheet').processSeals[0] || {}).by + ')');
      assert.strictEqual(st.doc(S, 'held-sheet').processSeals[0].by, 'Tess Welder');

      // what the portal reads of it (the fake store keeps only the newest batch of a day's rollup, so this is read right after the first batch):
      // the person's Laser order list has both orders, and the rollup says they were touched at the laser
      const po = await call('employeeEfficiency', { op: 'personOrders', key: PASS, name: 'Tess Welder', station: 'laser' });
      assert(po.ok !== false, 'personOrders answers: ' + JSON.stringify(po).slice(0, 300));
      const listed = (po.orders || []).map(o => String(o.rid || o.orderId || o.id || o.orderNumber));
      assert(listed.includes('3700000301') && listed.includes('3700000302'), "the person's Laser order list has the orders of the sheet: " + JSON.stringify(po).slice(0, 500));
      const roll = st.list('Efficiency_Daily').find(r => r && r.person === 'Tess Welder');
      assert(roll && roll.touched['3700000301'].laser && roll.touched['3700000302'].laser && !roll.touched['3700000301'].sorter, 'the day rollup: both orders touched at laser, none at the sorter: ' + JSON.stringify(roll && roll.touched));

      // the laser cut done (the real Library step), and its Undo: complete / undo, orders 0, the charms of each order
      for (let i = 0; i < 30 && !(await page.evaluate(() => LibraryDone.canComplete('sheet', 'held-sheet'))); i++) await wait(500);
      await page.evaluate(() => LibraryDone.mark('sheet', 'held-sheet', true));
      ev = (await fresh(2)).sort(byOrder);
      assert.deepStrictEqual(ev.map(brief), [['complete', 'laser', 'Tess Welder', '3700000301', 'held-sheet', 2, 0], ['complete', 'laser', 'Tess Welder', '3700000302', 'held-sheet', 1, 0]], 'laser cut done: ' + JSON.stringify(ev.map(brief)));
      assert(ev.every(e => e.detail === 'GF Sheet 4 marked completed (laser)'));
      assert.strictEqual(st.doc(S, 'held-sheet').laserDoneBy, 'Tess Welder', 'the permanent mark names the person');
      await page.evaluate(() => LibraryDone.mark('sheet', 'held-sheet', false, { undo: true, name: 'GF Sheet 4' }));
      ev = (await fresh(2)).sort(byOrder);
      assert.deepStrictEqual(ev.map(brief), [['undo', 'laser', 'Tess Welder', '3700000301', 'held-sheet', 2, 0], ['undo', 'laser', 'Tess Welder', '3700000302', 'held-sheet', 1, 0]], 'back to Laser cutting: ' + JSON.stringify(ev.map(brief)));
      assert(ev.every(e => e.detail === 'GF Sheet 4 returned to Laser cutting'));

      // a step the server does not take writes nothing (a sheet of an unfinished set cannot be completed alone)
      const failed = await page.evaluate(() => LibraryDone.mark('sheet', 'ready-sheet', true).then(() => 'went through', e => String(e.message)));
      assert(/Complete sheets from Laser cutting/.test(failed), 'refused: ' + failed);
      await none();

      // "put back in progress" (a drag out of Laser cutting, the real flow plan and commit)
      const back = await page.evaluate(async () => { const p = await LibraryFlow.plan({ kind: 'sheet', id: 'held-sheet', to: { area: 'progress' } }); const r = await LibraryFlow.commit(p, { confirmed: [] }); return { ok: r.ok, error: r.error, steps: p.steps.map(s => s.type) }; });
      assert(back.ok && back.steps.join() === 'hold', 'the move went through: ' + JSON.stringify(back));
      ev = (await fresh(2)).sort(byOrder);
      assert.deepStrictEqual(ev.map(brief), [['note', 'laser', 'Tess Welder', '3700000301', 'held-sheet', 0, 0], ['note', 'laser', 'Tess Welder', '3700000302', 'held-sheet', 0, 0]], 'put back: ' + JSON.stringify(ev.map(brief)));
      assert(ev.every(e => e.detail === 'put back in progress · GF Sheet 4'));
      assert(st.doc(S, 'held-sheet').laserHold, 'and the sheet is held again');

      // a SET approved (the seal is on the set): an event for each order of each of its sheets, the set's name in the words
      await page.evaluate(() => CNLaserAct.flow([{ type: 'seal', kind: 'set', id: 'set-fixture' }]));
      ev = await fresh(3);
      assert.deepStrictEqual(ev.map(e => [e.action, e.station, e.orderId, e.line, e.detail]).sort(), [
        ['note', 'laser', '3700000100', 'cut-sheet', 'approved for laser cutting · Set 1'],
        ['note', 'laser', '3700000101', 'ready-sheet', 'approved for laser cutting · Set 1'],
        ['note', 'laser', '3700000102', 'pending-sheet', 'approved for laser cutting · Set 1']], 'a set: ' + JSON.stringify(ev.map(brief)));

      // Rose Gold's Cut Sheet (the real RoseStock.record): an event for each order of the sheet; a sheet whose orders are not known is one event
      await page.evaluate(async () => {
        const base = { metal: 'rose', setId: 'set1', draft: false, recalled: { roseStockId: 'rgs' }, rosePlanHash: 'hash', roseRevision: 0, roseStock: { id: 'rgs', revision: 0 }, roseHistory: [] };
        await RoseStock.record(Object.assign({ sheetId: 'rose-two', page: 2, placements: [{ id: 'a' }, { id: 'b' }, { id: 'c' }], charms: [{ id: 'a', poolId: '3700000301_1_1' }, { id: 'b', poolId: '3700000301_1_2' }, { id: 'c', poolId: '3700000302_1_1' }] }, base));
        await RoseStock.record(Object.assign({ sheetId: 'rose-plain', page: 1, placements: [{ id: 'a' }, { id: 'b' }, { id: 'c' }] }, base));
      });
      ev = await fresh(3);
      const two = ev.filter(e => e.line === 'rose-two').sort(byOrder), plain = ev.filter(e => e.line === 'rose-plain');
      assert.deepStrictEqual(two.map(brief), [['complete', 'laser', 'Tess Welder', '3700000301', 'rose-two', 2, 0], ['complete', 'laser', 'Tess Welder', '3700000302', 'rose-two', 1, 0]], 'Cut Sheet: ' + JSON.stringify(ev.map(brief)));
      assert(two.every(e => e.detail === 'RG Sheet 2 cut (Cut Sheet)'));
      assert.deepStrictEqual(plain.map(brief), [['complete', 'laser', 'Tess Welder', '', 'rose-plain', 3, 0]], 'a sheet whose orders are unknown: one event with the sheet');

      // a sorter press keeps its station (nothing about the role changed it)
      await page.evaluate(() => CNAct('complete', { orderId: '3700000301', parts: 2, orders: 1, detail: 'Complete Order' }));
      ev = await fresh(1);
      assert.deepStrictEqual(ev.map(brief), [['complete', 'sorter', 'Tess Welder', '3700000301', '', 2, 1]], 'a sorter press with no role stays the sorter: ' + JSON.stringify(ev.map(brief)));
      // and no live slot of the laser was made by any of it
      assert.deepStrictEqual(rec.live.filter(l => l.station === 'laser'), [], 'no live laser slot from pressing buttons in the Library');

      /* ───────────── 2 · the signed-in role (LD1) routes everything, and the live board follows it ───────────── */
      const act = (action, o) => page.evaluate(([a, x]) => CNAct(a, x), [action, o]);
      const rows = [{ order: { receiptId: '3700000301', buyer: { name: 'Pat Example' } }, line: { transactionId: '1', quantity: 2, title: 'Fixture charm', sku: 'FX-1' }, spec: { designSku: 'FX-1', size: 'S' }, state: 'pulled' }];

      // a Laser person: a sorter press, a print and a laser cut are all Laser, with the role on each
      await signOut(page); await signIn(page, 'Tess Welder', 'laser');
      assert.deepStrictEqual(await page.evaluate(() => [StationSession.role(), StationActivity.who().station]), ['laser', 'laser'], 'signed in as Laser');
      await act('complete', { orderId: '3700000301', parts: 2, orders: 1, detail: 'Complete Order' });
      await act('print', { orderId: '3700000301', parts: 2, detail: 'QR label' });
      await page.evaluate(() => CNLaserAct.cut(true, { name: 'GF Sheet 4', ids: ['held-sheet'], rec: LibraryDone.recordOf }));
      ev = await fresh(4);
      assert.deepStrictEqual(ev.map(e => [e.action, e.station, e.person, e.device, e.role]), [['complete', 'laser', 'Tess Welder', DEV, 'laser'], ['print', 'laser', 'Tess Welder', DEV, 'laser'], ['complete', 'laser', 'Tess Welder', DEV, 'laser'], ['complete', 'laser', 'Tess Welder', DEV, 'laser']], 'a Laser person: all of it is Laser: ' + JSON.stringify(ev.map(brief)));
      assert.deepStrictEqual(ev.slice(2).map(e => [e.orderId, e.line, e.parts]).sort(), [['3700000301', 'held-sheet', 2], ['3700000302', 'held-sheet', 1]], 'and the laser cut is still one event for each order');

      // the live board: the order in hand, then the sheet in hand (the same Laser slot), with the person and the time since it was opened
      await page.evaluate(rs => CNLive.order('3700000301', rs), rows);
      let cur = await live(c => c.laser.length === 1);
      assert.deepStrictEqual(cur.laser.map(c => [c.person, c.kind, c.rid]), [['Tess Welder', 'order', '3700000301']], 'the Laser card shows the order in hand: ' + JSON.stringify(cur.laser));
      assert.strictEqual(cur.sorting.length, 0, 'and nothing at Sorting');
      assert(cur.laser[0].scannedAt > 0 && cur.laser[0].scannedAt <= Date.now(), 'with the time it was opened');
      await page.evaluate(() => CNLive.sheet('GF Sheet 4 · Set 2'));
      cur = await live(c => c.laser.length === 1 && c.laser[0].kind === 'sheet');
      assert.deepStrictEqual(cur.laser.map(c => [c.person, c.kind, c.title]), [['Tess Welder', 'sheet', 'GF Sheet 4 · Set 2']], 'the sheet in hand replaces it: ' + JSON.stringify(cur.laser));
      // closing the order window does not end the sheet that now holds the slot
      await page.evaluate(() => CNLive.close('sorter'));
      await wait(2600);
      assert.strictEqual((await live()).laser.length, 1, "the order window's close leaves the sheet in hand alone");
      // closing the sheet window while the order window is open: the order in hand shows again
      await page.evaluate(rs => CNLive.order('3700000301', rs), rows);
      await page.evaluate(() => CNLive.close('laser'));
      cur = await live(c => c.laser.length === 1 && c.laser[0].kind === 'order');
      assert.deepStrictEqual(cur.laser.map(c => [c.kind, c.rid]), [['order', '3700000301']], "the sheet window's close leaves the order window's order in hand: " + JSON.stringify(cur.laser));
      await page.evaluate(() => CNLive.close('sorter'));
      assert.strictEqual((await live(c => c.laser.length === 0)).laser.length, 0, 'and closing it ends the card');
      // a laser cut ends the sheet in hand
      await page.evaluate(() => CNLive.sheet('GF Sheet 4 · Set 2'));
      assert.strictEqual((await live(c => c.laser.length === 1)).laser.length, 1);
      await page.evaluate(() => CNLaserAct.cut(true, { name: 'GF Sheet 4', ids: ['held-sheet'], rec: LibraryDone.recordOf }));
      assert.strictEqual((await live(c => c.laser.length === 0)).laser.length, 0, 'a laser cut ends the sheet at the Laser card');
      await fresh(2);

      // the quiet switch to Design: the Laser card ends, and the open windows show at the Design card; every press is Design now
      await page.evaluate(rs => CNLive.order('3700000301', rs), rows);
      assert.strictEqual((await live(c => c.laser.length === 1)).laser.length, 1);
      await page.evaluate(() => CNRole.switchTo('design'));
      await page.waitForFunction(() => StationActivity.who() && StationActivity.who().role === 'design');
      cur = await live(c => c.laser.length === 0);
      assert.strictEqual(cur.laser.length, 0, 'the Laser card ended with the switch');
      await page.evaluate(rs => CNLive.order('3700000301', rs), rows);        // (the open window shows again at the page's next read of it)
      await page.evaluate(() => CNLive.sheet('GF Sheet 4'));
      await act('undo', { station: 'laser', parts: 1, detail: 'GF Sheet 4 returned to Laser cutting' });
      ev = await fresh(1);
      assert.deepStrictEqual(ev.map(e => [e.action, e.station, e.person, e.role]), [['undo', 'design', 'Tess Welder', 'design']], 'a Design person: a laser-side press is Design: ' + JSON.stringify(ev.map(brief)));
      cur = await live(c => c.design.length === 1 && c.design[0].kind === 'sheet');
      assert.deepStrictEqual([cur.design.map(c => [c.person, c.kind, c.title]), cur.laser.length], [[['Tess Welder', 'sheet', 'GF Sheet 4']], 0], 'the Design card shows the sheet in hand: ' + JSON.stringify(cur));
      await page.evaluate(() => { CNLive.close('sorter'); CNLive.close('laser'); });

      // the sorter's pressed buttons are never keystrokes or hovers: an input in the page writes nothing
      await page.mouse.move(300, 300); await page.keyboard.press('Shift'); await page.mouse.click(5, 5);
      await none();

      /* ───────────── 2b · the Sorter app's DESIGN work (still the Design person above): labels, decisions, engraving approvals and the
         sets committed into the framed Design Station are Design events with the name, the order and the role; as Laser they are Laser ───────────── */
      const DS = e => [e.action, e.station, e.person, e.role, e.orderId, e.parts, e.orders];
      // the calls are shaped exactly as the bridge's own (a label print, an engraving approved, a review decision)
      await act('print', { orderId: '3700000301', parts: 2, detail: 'QR label' });
      await act('complete', { orderId: '3700000301', line: '1', sku: 'FX-1', parts: 1, detail: 'engraving approved' });
      await act('note', { orderId: '3700000302', detail: 'decided: heldOrder' });
      ev = await fresh(3);
      assert.deepStrictEqual(ev.map(DS), [['print', 'design', 'Tess Welder', 'design', '3700000301', 2, 0], ['complete', 'design', 'Tess Welder', 'design', '3700000301', 1, 0], ['note', 'design', 'Tess Welder', 'design', '3700000302', 0, 0]], 'a Design person\'s label, engraving approval and decision: ' + JSON.stringify(ev.map(brief)));
      assert(ev.every(e => e.device === DEV && e.session && !e.sandbox), 'on this device and session');

      // the set committed into the framed page: the framed page records nothing for a Sorter-driven commit, so the Sorter does, one event for each order
      await page.evaluate(([rs]) => { window.__rowsWas = Orders.rows; Orders.rows = () => rs; }, [[
        { order: { receiptId: '3700000301' }, line: { transactionId: '1', quantity: 2 }, state: 'committed' },
        { order: { receiptId: '3700000302' }, line: { transactionId: '1', quantity: 1 }, state: 'committed' }]]);
      assert.strictEqual(await page.evaluate(() => CNAct.set(false, ['3700000301', 3700000302, '3700000301'], 'Set  4')), true, 'recorded');
      ev = (await fresh(2)).sort(byOrder);
      assert.deepStrictEqual(ev.map(DS), [['complete', 'design', 'Tess Welder', 'design', '3700000301', 2, 1], ['complete', 'design', 'Tess Welder', 'design', '3700000302', 1, 1]], 'Design: a commit is a completion of each order, with its pieces: ' + JSON.stringify(ev.map(brief)));
      assert(ev.every(e => e.detail === 'Set 4 committed (Design Station)' && e.device === DEV), 'its words: ' + ev[0].detail);

      // the real Sets.undo (the station's own call stubbed, nothing leaves the page): the order this person's commit was recorded for is taken back
      const undoSet = ids => page.evaluate(async ids => {
        const calls = []; const was = [DesignLink.ensure, DesignLink.call];
        DesignLink.ensure = async () => true; DesignLink.call = async (cmd, body) => { calls.push([cmd, (body.receiptIds || []).slice()]); return { ok: true }; };
        try { await Sets.undo({ setId: 'set-sa5', runId: 'run-fixture', name: 'Set 4', committed: ids, offline: true, sheetIds: [], orders: {}, labelFiles: [] }); }
        finally { [DesignLink.ensure, DesignLink.call] = was; }
        return calls;
      }, ids);
      assert.deepStrictEqual(await undoSet(['3700000301']), [['complete.undo', ['3700000301']]], 'the station was asked to reopen it');
      ev = await fresh(1);
      assert.deepStrictEqual(ev.map(DS), [['undo', 'design', 'Tess Welder', 'design', '3700000301', 2, 1]], 'Design: the undo takes back the order this person completed: ' + JSON.stringify(ev.map(brief)));
      assert.strictEqual(ev[0].detail, 'Set 4 completion undone (Design Station)');
      // an order this person's commit was not recorded for (another person's, or from before this page opened) is a note, never a take-back
      await undoSet(['3700000301', '3700000302']);
      ev = (await fresh(2)).sort(byOrder);
      assert.deepStrictEqual(ev.map(DS), [['note', 'design', 'Tess Welder', 'design', '3700000301', 0, 0], ['undo', 'design', 'Tess Welder', 'design', '3700000302', 1, 1]].sort((a, b) => (a[4] < b[4] ? -1 : 1)), 'Design: only what this person completed is undone: ' + JSON.stringify(ev.map(brief)));
      await page.evaluate(() => { Orders.rows = window.__rowsWas; });

      // the same actions as a Laser person: Laser events with the role, a commit is a note (the Design Station's completion is the Design person's)
      await page.evaluate(() => CNRole.switchTo('laser'));
      await page.waitForFunction(() => StationActivity.who() && StationActivity.who().role === 'laser');
      await act('print', { orderId: '3700000301', parts: 2, detail: 'QR label' });
      await page.evaluate(() => CNAct.set(false, ['3700000301'], 'Set 4'));
      ev = await fresh(2);
      assert.deepStrictEqual(ev.map(DS), [['print', 'laser', 'Tess Welder', 'laser', '3700000301', 2, 0], ['note', 'laser', 'Tess Welder', 'laser', '3700000301', 0, 0]], 'a Laser person: laser events, the commit a note: ' + JSON.stringify(ev.map(brief)));
      assert.strictEqual(ev[1].detail, 'Set 4 committed (Design Station)');

      // the Admin (no role): the commit is a note at the sorter, as every other press of the Admin's
      await signOut(page); await signIn(page, 'Tess Welder', 'admin');
      await page.evaluate(() => CNAct.set(false, ['3700000301'], 'Set 4'));
      ev = await fresh(1);
      assert.deepStrictEqual(ev.map(e => [e.action, e.station, e.person, e.orderId, e.orders]), [['note', 'sorter', 'Tess Welder', '3700000301', 0]], 'the Admin: a note at the sorter: ' + JSON.stringify(ev.map(brief)));

      // nobody signed in: nothing is written, nothing is kept, nothing is asked (the commit itself ran; only its record is not made)
      await signOut(page);
      assert.strictEqual(await page.evaluate(() => CNAct.set(false, ['3700000301'], 'Set 4')), false);
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 0, 'not kept for a name: the commit is not a press of this person');
      await none();
      assert.deepStrictEqual(await page.evaluate(() => window.__prompts), [], 'no browser pop-up');
      assert(!(await page.$('.cnNameBar')), 'and no name field offered');

      await context.close();
    }

    /* ───────────── 3 · nobody named: kept with its orders, recorded under the name typed ───────────── */
    {
      const { context, page, flush, fresh, none } = await sorterPage('');
      await page.evaluate(() => CNLaserAct.cut(true, { name: 'GF Sheet 4', ids: ['held-sheet'], rec: () => ({ poolIds: ['3700000301_1_1', '3700000301_1_2', '3700000302_1_1'], placedCount: 3 }) }));
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 1, 'kept, not lost, not blocked');
      await none();
      await page.waitForSelector('.cnNameBar');
      // the name typed, then "Laser or Design?" (LD1): the press waits for the role as well, then is recorded under both
      await page.evaluate(() => { CNRole.setAdminLookup(async () => false); B.employee = 'mia  cutter'; });
      await page.waitForFunction(() => CNRole.state() === 'ask');
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 1, 'still kept while the role is asked');
      await none();
      await page.evaluate(() => CNRole.choose('laser'));
      const ev = (await fresh(2)).sort(byOrder);
      assert.deepStrictEqual(ev.map(e => brief(e).concat(e.role)), [['complete', 'laser', 'Mia Cutter', '3700000301', 'held-sheet', 2, 0, 'laser'], ['complete', 'laser', 'Mia Cutter', '3700000302', 'held-sheet', 1, 0, 'laser']], 'under the typed name and the role, an event for each order: ' + JSON.stringify(ev.map(brief)));
      assert.strictEqual(await page.evaluate(() => CNAct.held()), 0);
      assert.deepStrictEqual(await page.evaluate(() => window.__prompts), [], 'no browser pop-up');
      await context.close();
    }
    assert.deepStrictEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    console.log('laser-wired: ok');
  } finally {
    await browser.close(); srv.close();
  }

  /** the live board as the portal reads it (op live): the cards of the stations, with what each one has in hand */
  async function live(ok) {       // (the board's read is kept two seconds, and the page paces its live writes: look until `ok` holds, 9 s at most)
    let out = null;
    for (let i = 0; i < 13; i++) {
      const r = await call('employeeEfficiency', { op: 'live', key: PASS });
      assert(r && r.ok !== false && Array.isArray(r.stations), 'the live board answers: ' + JSON.stringify(r).slice(0, 300));
      const of = k => ((r.stations.find(s => s.key === k) || {}).current || []);
      out = { laser: of('laser'), design: of('design'), sorting: of('sorting') };
      if (!ok || ok(out)) break;
      await wait(700);
    }
    return out;
  }
})().catch(e => { console.error(e); process.exit(1); });
