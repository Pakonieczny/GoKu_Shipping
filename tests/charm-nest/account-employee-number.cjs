// EMPNUM (Paul, 26 Oct 2026: "The user login panel and drop down are really nice however, for some reason anyone can switch from one user to another
// without putting in their password, which is a big safety concern."): in the sorter a person signs in, or is switched to, ONLY with their 6-digit
// Employee Number, the stations' own login (firebaseOrders {pinLogin}); the name comes from the server. Runs the real sorter page (charm-nest-1.html with
// its bridge, charm-nest-role.js, station-session.js and charm-nest-account.js) in headless Chromium against the local fake site (bridge-server.cjs);
// the stations' door is this test's own recorder (the Admin question, the session writes, the Employee Number roster), so nothing live is touched.
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/account-employee-number.cjs
'use strict';
const path = require('path'), assert = require('assert/strict'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const wait = ms => new Promise(r => setTimeout(r, ms));
const fold = n => String(n || '').replace(/[._]/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase();
let passed = 0; const say = s => { passed++; console.log('  ok  ' + s); };

const ROSTER = { '111111': 'Paul K', '222222': 'Ana M', '333333': 'Tess Welder' };
const door = { admins: ['paul k'], mode: 'ok', tries: [] };       // mode: 'ok' | 'down' (the Employee Number door does not answer) | 'tooMany'
async function newContext(browser) {
  const rec = { sessions: [] };
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
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
    if (j.pinLogin !== undefined) {
      door.tries.push(String(j.pinLogin));
      if (door.mode === 'down') return r.abort('internetdisconnected');
      if (door.mode === 'tooMany') return r.fulfill({ status: 429, contentType: 'application/json', body: JSON.stringify({ ok: false, tooMany: true, error: 'Too many tries, wait a minute' }) });
      const nm = ROSTER[String(j.pinLogin)];
      return r.fulfill(ok(nm ? { ok: true, name: nm } : { ok: false, error: 'not on the list' }));
    }
    if (typeof j.stationAdmin === 'string') return r.fulfill(ok({ ok: true, admin: door.admins.includes(fold(j.stationAdmin)) }));
    if (j.session || Array.isArray(j.activity) || j.live) { if (j.session) rec.sessions.push(j.session); return r.fulfill(ok({ success: true, written: 1 })); }
    return r.fallback();
  });
  await context.addInitScript(() => {
    try {
      localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off', sandboxStream: 'off' }));
      localStorage.setItem('cn.tour.seen', '1');
      // people this computer has seen before (the tiles): a name alone must sign nobody in
      if (!localStorage.getItem('cn.people')) localStorage.setItem('cn.people', JSON.stringify(['Ana M', 'Tess Welder']));
    } catch (_) {}
    window.alert = () => {}; window.prompt = () => null;
    window.__who = () => String((window.CNEmployee && CNEmployee.name()) || '').replace(/[._]/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase();   // (the page tidies a name: "Paul K" is kept as "Paul K.")
  });
  return { context, rec };
}
async function openPage(ctx, srv, errors) {
  const page = await ctx.context.newPage();
  page.setDefaultTimeout(20000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.CNEmployee && window.StationSession && window.CNRole && window.CNAccount && CN.S.cloud.ok === true, null, { timeout: 60000 });
  return page;
}
const snap = page => page.evaluate(() => {
  const p = document.getElementById('cnAcctPanel'), i = p && p.querySelector('.cnAcIn'), e = p && p.querySelector('.cnAcErr');
  return { name: CNEmployee.name(), st: CNRole.state(), admin: CNRole.admin(), gated: CNAccount.gated(),
    panel: p ? { mode: p.dataset.mode, text: p.innerText.replace(/\s+/g, ' ').trim(), input: i ? { type: i.type, mode: i.inputMode, max: i.maxLength, label: (p.querySelector('.cnAcForm label') || {}).textContent } : null, err: e && !e.hidden ? e.textContent : '' } : null,
    bars: [...document.querySelectorAll('.cnNameBar')].map(b => ({ kind: b.dataset.kind, num: b.dataset.num, label: (b.querySelector('label b') || {}).textContent, type: (b.querySelector('input') || {}).type, cont: !!(b.querySelector('.cnNbCont') && !b.querySelector('.cnNbCont').hidden) })) };
});
const until = async (page, fn, arg, what) => { try { await page.waitForFunction(fn, arg, { timeout: 8000 }); } catch (e) { throw new Error('timed out waiting for ' + what + ': ' + JSON.stringify(await snap(page).catch(() => '?'))); } };
const settleUi = () => wait(420);
async function panel(page) { if (!(await page.$('#cnAcctPanel'))) { await page.click('#cnAcctChip'); await page.waitForSelector('#cnAcctPanel', { state: 'visible' }); await settleUi(); } }
async function enter(page, num) { await page.fill('#cnAcctPanel .cnAcIn', num); await page.press('#cnAcctPanel .cnAcIn', 'Enter'); }
const secrets = page => page.evaluate(nums => { const all = []; for (const st of [localStorage, sessionStorage]) for (let i = 0; i < st.length; i++) { const k = st.key(i); all.push(k + '=' + st.getItem(k)); } return nums.filter(n => all.some(x => x.includes(n))); }, Object.keys(ROSTER));

(async () => {
  const srv = await start(), browser = await chromium.launch(process.env.CHROMIUM ? { executablePath: process.env.CHROMIUM } : {}), errors = [];
  try {
    const ctx = await newContext(browser), page = await openPage(ctx, srv, errors);

    // 1 · nobody signed in: the panel asks for the Employee Number (masked digits), never a name; tapping a tile signs nobody in
    let s = await snap(page);
    assert.equal(s.gated, true, 'the page turns the gate on'); assert.equal(s.name, '');
    await panel(page); s = await snap(page);
    assert.equal(s.panel.mode, 'who'); assert.equal(s.panel.input.type, 'password'); assert.equal(s.panel.input.mode, 'numeric'); assert.equal(s.panel.input.max, 6);
    assert.match(s.panel.text, /Employee Number/);
    await page.click('#cnAcctPanel .cnAcTile[data-fk="p:ana m"]'); await wait(150); s = await snap(page);
    assert.equal(s.name, '', 'a tapped name signs nobody in'); assert.equal(s.panel.input.label, 'Ana’s Employee Number', 'the tile only says whose number is asked for');
    say('signed out: the panel asks for a 6-digit Employee Number (masked); a tile alone signs nobody in');

    // 2 · a wrong number, then a malformed one: nobody is signed in, a calm line says why
    await enter(page, '999999');
    await until(page, () => /not on the list/.test((document.querySelector('#cnAcctPanel .cnAcErr') || {}).textContent || ''), null, 'the refusal');
    s = await snap(page); assert.equal(s.name, '');
    await enter(page, '12'); await wait(100); s = await snap(page);
    assert.equal(s.name, ''); assert.match(s.panel.err, /6 digits/);
    say('a number not on the list, or not 6 digits, signs nobody in and says why');

    // 3 · the right number: the person the server names is signed in (the Admin here), whatever tile was tapped before
    await enter(page, '111111');
    await until(page, () => __who() === 'paul k' && CNRole.ready(), null, 'Paul K signed in');
    s = await snap(page); assert.equal(s.admin, true);
    assert.deepEqual(await secrets(page), [], 'the number is never kept in the browser');
    say('the right number signs in the person the server names (Paul K, Admin); the number is not kept');

    // 4 · a name set by a script (or any old name field) switches nobody: the panel asks for the number instead
    await page.evaluate(() => { B.employee = 'Tess Welder'; }); await wait(150);
    s = await snap(page); assert.equal(fold(s.name), 'paul k', 'nobody switched'); assert.ok(s.panel, 'the panel opened'); assert.match(s.panel.err, /enter their Employee Number/);
    await page.evaluate(() => CNAccount.signIn('Tess Welder')); await wait(100);
    assert.equal(fold((await snap(page)).name), 'paul k', 'CNAccount.signIn(name) switches nobody either');
    say('setting a name (a script, a dialog\'s old field, signIn(name)) switches nobody: the panel asks for the number');

    // 5 · switch person: tap Ana, her number; the old session ends "switched"
    await panel(page);
    await page.click('#cnAcctPanel .cnAcPick[data-fk="p:ana m"]'); await wait(100);
    assert.equal((await snap(page)).panel.input.label, 'Ana’s Employee Number');
    await enter(page, '222222');
    await until(page, () => __who() === 'ana m' && document.querySelector('#cnAcctPanel') && document.querySelector('#cnAcctPanel').dataset.mode === 'role', null, 'Ana asked Laser or Design');
    await settleUi(); await page.click('#cnAcctPanel [data-role="design"]');
    await until(page, () => CNRole.ready() && CNRole.role() === 'design', null, 'Ana signed in as Design');
    assert.ok(ctx.rec.sessions.some(x => x.event === 'end' && fold(x.person) === 'paul k' && x.reason === 'switched'), 'Paul\'s session ended "switched"');
    say('switch person: her Employee Number switches to Ana (the old session ends "switched")');

    // 6 · sign out needs nothing; the small name bar (a press that needs a person) asks for the number, never offers "Continue as"
    await panel(page); await page.click('#cnAcctPanel .cnAcOut');
    await until(page, () => !CNEmployee.name(), null, 'signed out');
    await page.evaluate(() => { if (CNAccount.isOpen()) CNAccount.close(); window.__need = CNEmployee.need('test'); }); await wait(200);
    s = await snap(page); const bar = s.bars.find(b => b.kind === 'ask');
    assert.ok(bar, 'the name bar opened'); assert.equal(bar.num, '1'); assert.equal(bar.label, 'Your Employee Number'); assert.equal(bar.type, 'password'); assert.equal(bar.cont, false, 'no one-tap "Continue as <name>"');
    await page.fill('.cnNameBar[data-kind="ask"] input', '333333'); await page.press('.cnNameBar[data-kind="ask"] input', 'Enter');
    assert.equal(fold(await page.evaluate(() => window.__need)), 'tess welder', 'the press that asked goes on under the person the number names');
    say('sign out needs nothing; the name bar asks for the Employee Number (no "Continue as") and the press goes on under that person');
    await until(page, () => CNRole.state() === 'ask', null, 'Tess asked Laser or Design');
    await page.evaluate(() => CNRole.choose('laser'));
    await until(page, () => CNRole.ready(), null, 'Tess signed in as Laser');

    // 7 · the door does not answer, or too many tries: nobody changes
    door.mode = 'down';
    await panel(page); await enter(page, '222222');
    await until(page, () => /could not be checked/.test((document.querySelector('#cnAcctPanel .cnAcErr') || {}).textContent || ''), null, 'the no-answer line');
    assert.equal(fold((await snap(page)).name), 'tess welder', 'no answer: the person signed in stays');
    door.mode = 'tooMany';
    await enter(page, '222222');
    await until(page, () => /Too many tries/.test((document.querySelector('#cnAcctPanel .cnAcErr') || {}).textContent || ''), null, 'the too-many line');
    assert.equal(fold((await snap(page)).name), 'tess welder');
    door.mode = 'ok';
    say('no answer from the server, or too many tries: nobody is switched, the line says why');

    // 8 · another tab of this computer: a person signed in there (through its own gate) is this tab's person too
    const page2 = await openPage(ctx, srv, errors);
    assert.equal(fold(await page2.evaluate(() => CNEmployee.name())), 'tess welder');
    await page2.evaluate(() => { if (!document.querySelector('#cnAcctPanel')) CNAccount.open(); });
    await page2.fill('#cnAcctPanel .cnAcIn', '222222'); await page2.press('#cnAcctPanel .cnAcIn', 'Enter');
    await until(page2, () => __who() === 'ana m', null, 'Ana in tab 2');
    await until(page, () => __who() === 'ana m', null, 'tab 1 follows tab 2');
    say('another tab of this computer: its signed-in person (by number) is followed here');

    assert.deepEqual(errors, [], 'no page error');
    console.log(`PASS: account-employee-number (${passed} checks): signing in and switching person in the sorter take the person's Employee Number`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error('FAIL:', e && e.stack || e); process.exit(1); });
