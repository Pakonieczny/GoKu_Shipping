// The Employee efficiency screen is the master admin's alone (Paul, 11 Oct 2026: "Hide the Employee Efficiency option in the UI from
// all users other than myself"). Fakes only: the sorter's own test server (bridge-server.cjs), the employeeEfficiency fixture, and a stand-in
// for the server's Admin list (master-admin-door.cjs: "Paul K" is the Admin, nobody else). Everything off the loopback is aborted.
//   1 · the helper (charm-nest-master-admin.js, with the REAL charm-nest-role.js, in jsdom): is() / login() / state() / onChange, false when
//       nobody is signed in, for a normal employee, while the server's answer is on its way, when it cannot be had; true for the master admin;
//       it follows a login change at once (another person takes over, sign-out); data-master-only elements show only for the master admin
//   2 · SIGNED OUT, then a NORMAL EMPLOYEE (the real sorter page): the Workspace menu entry is there but hidden (the other entries are not),
//       the tab cannot be opened by CN.setMode, Efficiency.open / go, the address (#efficiency, #efficiency/stations, typed or by Back) or a
//       hand-removed `hidden`: the screen they are on stays; the console makes no request and writes no address; no page error
//   3 · NOTHING FLASHES: with the helper's script held back, the entry is hidden in the page as served and stays hidden for a normal employee
//   4 · the MASTER ADMIN: the entry shows (after the server's answer), opens the tab, the passcode is still asked; a deep link
//       (#efficiency/stations) that arrives before the answer is kept and opened once it is Paul's; and with NO reload: another person signing in
//       takes the screen away and stops its reads, signing out hides it, Paul back shows it again
//   node tests/charm-nest/efficiency-hidden.cjs     (NODE_PATH=<jsdom>, PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require('./efficiency-fixture.cjs');
const door = require('./master-admin-door.cjs');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ── 1 · the helper, with the real role module ── */
async function helper() {
  let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('  – no jsdom: the helper checks were not run'); return; }
  const dom = new JSDOM('<!doctype html><body><button id="e" hidden data-master-only>E</button><button id="o">other</button></body>', { url: 'http://localhost/', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window; let nm = '';
  w.CNEmployee = { name: () => nm, roleBar: () => true };
  w.eval(read('charm-nest-role.js')); w.eval(read('charm-nest-master-admin.js'));
  const M = w.CharmNestMasterAdmin, got = [], tick = () => sleep(30);
  assert.deepEqual(Object.keys(M).sort(), ['is', 'login', 'onChange', 'refresh', 'state'], 'the contract: is() and login(), plus state, onChange, refresh');
  const stop = M.onChange(d => got.push(`${d.is}:${d.login}`));
  const el = w.document.getElementById('e'), other = w.document.getElementById('o'), ev = [];
  w.addEventListener('charmnest:master-admin', e => ev.push(e.detail.is));
  assert.equal(M.is(), false, 'nobody signed in'); assert.equal(M.login(), ''); assert.equal(M.state(), 'none'); assert.equal(el.hidden, true);
  w.CNRole.setAdminLookup(async n => n === 'Paul K');
  nm = 'Tess W.'; w.CNRole.nameSet(nm); await tick();
  assert.equal(M.is(), false, 'a normal employee'); assert.equal(M.login(), 'Tess W.'); assert.equal(M.state(), 'other'); assert.equal(el.hidden, true);
  nm = 'Paul K'; w.CNRole.nameSet(nm);
  assert.equal(M.is(), false, 'while the answer is on its way'); assert.equal(M.state(), 'checking'); assert.equal(el.hidden, true);
  await tick();
  assert.equal(M.is(), true, 'the master admin'); assert.equal(M.login(), 'Paul K'); assert.equal(M.state(), 'master'); assert.equal(el.hidden, false); assert.equal(other.hidden, false, 'other elements are left alone');
  nm = 'Tess W.'; w.CNRole.nameSet(nm);
  assert.equal(M.is(), false, 'another person took the page over: gone at once, not at the next look'); assert.equal(el.hidden, true);
  nm = 'Paul K'; w.CNRole.nameSet(nm); await tick(); assert.equal(M.is(), true);
  nm = ''; w.CNRole.nameSet('');
  assert.equal(M.is(), false, 'signed out: gone at once'); assert.equal(M.login(), ''); assert.equal(el.hidden, true);
  assert.deepEqual(got, ['false:Tess W.', 'true:Paul K', 'false:Tess W.', 'true:Paul K', 'false:'], 'told on every change, and only on a change');
  assert.deepEqual(ev, [false, true, false, true, false], 'the same changes as the window event');
  w.CNRole.setAdminLookup(async () => null); nm = 'Paul K'; w.CNRole.nameSet(nm); await tick();
  assert.equal(M.is(), false, 'the server could not be asked: not the master admin (fail closed)');
  w.CNRole.setAdminLookup(async () => { throw new Error('offline'); }); nm = 'Someone'; w.CNRole.nameSet(nm); await tick(); assert.equal(M.is(), false);
  nm = '1234'; assert.equal(M.login(), '', 'a number is a PIN, never a login'); assert.equal(M.is(), false);
  stop(); M.refresh();
  assert.equal(Object.isFrozen(M), true, 'the helper cannot be replaced by a call');
  // a name typed in any case or spelling the list folds to the same person is the Admin: the check is a NAME match (said plainly in the file)
  assert(/NAME match/.test(read('charm-nest-master-admin.js')) && /not a security boundary/.test(read('charm-nest-master-admin.js')));
  w.close();
  console.log('  ✓ the helper: false signed out / for a normal employee / while checking / when the server cannot be asked; true for the master admin; follows a login change at once');
}

(async () => {
  await helper();
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fx = F.make(), seen = { eff: [], aborted: 0, errs: [] };
  const V = '#efficiencyView', ITEM = '#moreMenu .moreList button[data-mode="efficiency"]', sorter = `${srv.sorterOrigin}/charm-nest-1.html`;
  /** a browser context: the stand-in Admin list says "Paul K" and nobody else; `who` is the name already kept in this browser */
  async function context(who, o = {}) {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url());
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      if (u.pathname.endsWith('/employeeEfficiency')) {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        seen.eff.push(b.op || '?');
        const r = fx.answer(b, route.request().headers());
        return route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) });
      }
      if (o.holdHelperMs && u.pathname.endsWith('/charm-nest-master-admin.js')) { await sleep(o.holdHelperMs); }
      return route.continue();
    });
    await door.admin(ctx, { names: ['Paul K'], delayMs: o.doorMs || 0 });
    await ctx.addInitScript(who => {
      try { if (who && !sessionStorage.getItem('seeded')) { localStorage.setItem('cn.employee', who); sessionStorage.setItem('seeded', '1'); } } catch (_) {}
      window.confirm = () => true; window.alert = () => {}; window.prompt = () => null;
    }, who);
    return ctx;
  }
  const open = async (ctx, url = sorter) => {
    const page = await ctx.newPage(); page.on('pageerror', e => seen.errs.push(e.message));
    await page.goto(url);
    await page.waitForFunction(() => window.Efficiency && window.CN && window.CNRole && window.CharmNestMasterAdmin && document.readyState === 'complete', null, { timeout: 60000 });
    return page;
  };
  /** the Admin answer for the signed-in name has arrived (either way) */
  const answered = page => page.waitForFunction(() => { const s = CNRole.state(); return s === 'admin' || s === 'role' || s === 'ask' || s === 'none'; }, null, { timeout: 15000 });
  const menu = async page => { if (!(await page.evaluate(() => document.getElementById('moreMenu').open))) await page.click('#moreMenu > summary'); };
  const noTab = async (page, why) => {
    const s = await page.evaluate(() => ({ mode: CN.S.mode, view: !document.getElementById('efficiencyView').classList.contains('hidden'), shown: Efficiency.state.shown, hash: location.hash, btn: document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]').hidden }));
    assert.equal(s.view, false, why + ': the screen is not shown'); assert.equal(s.shown, false, why + ': the console is not running'); assert.notEqual(s.mode, 'efficiency', why);
    assert(!/^#efficiency/.test(s.hash), why + ': no efficiency address (' + s.hash + ')');
    return s;
  };
  /** everything a person without the right can try */
  async function tryEverything(page, who) {
    await menu(page);
    const item = page.locator(ITEM);
    assert.equal(await item.count(), 1); assert.equal(await item.isVisible(), false, who + ': the entry is not seen in the opened Workspace menu');
    for (const t of ['Design Station', 'Master files', 'Settings']) assert(await page.locator(`#moreMenu .moreList button:has-text("${t}")`).first().isVisible(), who + ': the other entries are unchanged: ' + t);
    await page.click('#moreMenu > summary');    // (close it)
    const before = seen.eff.length;
    await page.evaluate(() => { CN.setMode('efficiency'); }); await noTab(page, who + ' CN.setMode');
    await page.evaluate(() => { Efficiency.open(); Efficiency.go('stations'); Efficiency.go('person', 'Ana M.'); Efficiency.go('overview'); Efficiency.api.openPerson('Ana M.'); Efficiency.api.openStations(); }); await noTab(page, who + ' Efficiency.open / go / api');
    const r = await page.evaluate(() => Efficiency.api.call({ op: 'overview' }).then(() => 'answered', e => (e && e.barred ? 'barred' : String(e && e.message)))); assert.equal(r, 'barred', who + ': the console makes no read');
    // the address, typed, and by Back
    for (const h of ['#efficiency', '#efficiency/stations', '#efficiency/person/Ana%20M.']) {
      await page.evaluate(h => { location.hash = h; }, h); await sleep(250);
      const s = await noTab(page, who + ' typed ' + h); assert.equal(s.hash, '#' + s.mode, who + ': the address says where the person really is');
    }
    await page.evaluate(() => { history.pushState(null, '', '#efficiency'); history.pushState(null, '', '#nest'); history.back(); }); await sleep(250); await noTab(page, who + ' Back');
    // a hand-removed `hidden` (the browser's inspector): the entry clicked, the screen un-hidden
    await page.evaluate(() => { const b = document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]'); b.hidden = false; b.click(); document.getElementById('efficiencyView').classList.remove('hidden'); });
    await sleep(300); const s = await noTab(page, who + ' forced');
    await page.evaluate(() => { document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]').hidden = true; });
    assert.equal(await page.evaluate(() => document.getElementById('efficiencyView').classList.contains('hidden')), true, who + ': a screen un-hidden by hand is put away again');
    assert.equal(seen.eff.length, before, who + ': not one request to employeeEfficiency');
    assert.equal(s.mode, await page.evaluate(() => CN.S.mode));
  }

  try {
    /* ── 2a · nobody signed in ── */
    {
      const html = read('charm-nest-1.html');
      assert(/<button data-mode="efficiency" hidden data-master-only /.test(html), 'the entry is written hidden in the page as served');
      assert(/<script src="charm-nest-master-admin\.js\?v=[^"]+"><\/script>/.test(html) && /charm-nest-role\.js\?v=[^"]+-ma1"/.test(html) && html.indexOf('<script src="charm-nest-master-admin.js') > html.indexOf('<script src="charm-nest-role.js'), 'the helper loads after the role module, with a version token');
      assert(/"charm-nest-master-admin\.js"/.test(read('scripts/build-public.cjs')), 'and it is in the published asset list');
      const ctx = await context(null); const page = await open(ctx); await answered(page);
      assert.equal(await page.evaluate(() => [CharmNestMasterAdmin.is(), CharmNestMasterAdmin.login(), CharmNestMasterAdmin.state()].join('|')), 'false||none');
      await tryEverything(page, 'signed out');
      // the deep link, signed out: the default view, the address rewritten
      const p2 = await open(ctx, sorter + '#efficiency/stations'); await answered(p2); await sleep(600);
      const s = await noTab(p2, 'deep link signed out'); assert.equal(s.hash, '#nest'); assert.equal(s.mode, 'nest');
      assert.equal(await p2.evaluate(() => !document.getElementById('sheets').classList.contains('hidden')), true, 'the normal default view is what shows');
      await ctx.close();
      console.log('  ✓ signed out: entry hidden, tab unreachable by CN.setMode / Efficiency.open / go / api / typed address / Back / un-hiding by hand; deep link lands on the default view; no request');
    }
    /* ── 2b · a normal employee ── */
    {
      const ctx = await context('Tess W.'); const page = await open(ctx); await answered(page);
      assert.equal(await page.evaluate(() => [CharmNestMasterAdmin.is(), CharmNestMasterAdmin.login()].join('|')), 'false|Tess W.');
      await tryEverything(page, 'employee');
      const p2 = await open(ctx, sorter + '#efficiency'); await answered(p2); await sleep(600);
      const s = await noTab(p2, 'deep link employee'); assert.equal(s.hash, '#nest');
      // names that are not the Admin's, however close
      for (const n of ['Paula K', 'Paul Kowalski', 'Pau K']) { await page.evaluate(n => { B.employee = n; }, n); await answered(page); await sleep(200); assert.equal(await page.evaluate(() => CharmNestMasterAdmin.is()), false, n); }
      await ctx.close();
      console.log('  ✓ a normal employee: the same, with the Admin answer "no"; look-alike names are not the Admin');
    }
    /* ── 3 · nothing flashes ── */
    {
      const ctx = await context('Tess W.', { holdHelperMs: 1200, doorMs: 400 });
      const page = await ctx.newPage(); page.on('pageerror', e => seen.errs.push(e.message));
      await page.addInitScript(() => { window.__shown = []; const tickf = () => { const b = document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]'); if (b && !b.hidden) window.__shown.push(Math.round(performance.now())); requestAnimationFrame(tickf); }; tickf(); });
      await page.goto(sorter, { waitUntil: 'commit' });
      await page.waitForFunction(() => document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]'), null, { timeout: 30000 });   // (the page is read as far as the held script: the entry is in it, the helper is not)
      assert.equal(await page.evaluate(() => !!window.CharmNestMasterAdmin), false, 'the helper is still on its way');
      assert.equal(await page.evaluate(() => document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]').hidden), true, 'hidden before any check could have run');
      await page.waitForFunction(() => window.CharmNestMasterAdmin && window.CNRole && document.readyState === 'complete', null, { timeout: 60000 });
      await answered(page); await sleep(800);
      assert.deepEqual(await page.evaluate(() => window.__shown), [], 'never un-hidden for a normal employee, before, during or after the check');
      await ctx.close();
      // and for the master admin: hidden until the check has finished, then shown
      const ctx2 = await context('Paul K', { holdHelperMs: 800, doorMs: 600 });
      const p2 = await ctx2.newPage(); p2.on('pageerror', e => seen.errs.push(e.message));
      await p2.addInitScript(() => { window.__t = {}; const tickf = () => { const b = document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]'); if (b && !b.hidden && !window.__t.shown) window.__t.shown = { at: performance.now(), master: window.CharmNestMasterAdmin ? window.CharmNestMasterAdmin.is() : null }; requestAnimationFrame(tickf); }; tickf(); });
      await p2.goto(sorter, { waitUntil: 'commit' });
      await p2.waitForFunction(() => document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]'), null, { timeout: 30000 });
      assert.equal(await p2.evaluate(() => document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]').hidden), true);
      await p2.waitForFunction(() => window.__t.shown, null, { timeout: 30000 });
      assert.equal(await p2.evaluate(() => window.__t.shown.master), true, 'shown only once the check said Paul');
      await ctx2.close();
      console.log('  ✓ nothing flashes: hidden in the page as served, hidden while the helper and the server answer are on their way, shown only once the answer is Paul');
    }
    /* ── 4 · the master admin ── */
    {
      const ctx = await context('Paul K'); const page = await open(ctx);
      await page.evaluate(() => { Efficiency.options.pollMs = 400; Efficiency.options.liveMs = 400; Efficiency.options.growMs = 0; });
      await page.waitForFunction(() => CharmNestMasterAdmin.is() === true, null, { timeout: 15000 });
      assert.equal(await page.evaluate(() => CharmNestMasterAdmin.login()), 'Paul K.', 'the name as the sorter keeps it (a lone initial gets its dot; the server folds the dot away)');
      await page.click('#moreMenu > summary');
      const item = page.locator(ITEM);
      assert(await item.isVisible(), 'the entry shows for the master admin'); assert.equal((await item.innerText()).trim(), 'Employee efficiency');
      assert.match(await item.getAttribute('title'), /Who is in, what they made and scanned/);
      await item.click();
      await page.waitForSelector(`${V}:not(.hidden) .efKey:not(.hidden)`, { timeout: 15000 });
      assert.equal(await page.evaluate(() => location.hash), '#efficiency'); assert.equal(await page.evaluate(() => CN.S.mode), 'efficiency');
      assert.equal(seen.eff.length, 0, 'the passcode is still asked first (the server gate is unchanged)');
      await page.fill(`${V} .efKey input`, F.KEY); await page.press(`${V} .efKey input`, 'Enter');
      await page.waitForSelector(`${V} .efBody:not(.hidden) .efP`, { timeout: 15000 });
      // another person signs in, no reload: the screen goes, the reads stop
      const n0 = seen.eff.length; assert(n0 > 0);
      await page.evaluate(() => { B.employee = 'Tess W.'; });
      await page.waitForFunction(() => CN.S.mode === 'nest' && document.getElementById('efficiencyView').classList.contains('hidden'), null, { timeout: 5000 });
      assert.equal(await page.evaluate(() => CharmNestMasterAdmin.is()), false);
      assert.equal(await page.evaluate(() => document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]').hidden), true, 'the entry is gone from the menu');
      assert.equal(await page.evaluate(() => /^#efficiency/.test(location.hash)), false, 'and the address');
      await sleep(300); const n1 = seen.eff.length; await sleep(1800);
      assert.equal(seen.eff.length, n1, 'no read after the other person signed in (' + n1 + ' -> ' + seen.eff.length + ')');
      assert.equal(await page.evaluate(() => Efficiency.state.shown), false);
      // sign-out, then Paul back: shown again, no reload
      await page.evaluate(() => { B.employee = ''; }); await sleep(200);
      assert.equal(await page.evaluate(() => [CharmNestMasterAdmin.is(), document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]').hidden].join('|')), 'false|true', 'signed out: hidden');
      await page.evaluate(() => { B.employee = 'Paul K'; });
      await page.waitForFunction(() => CharmNestMasterAdmin.is() === true && !document.querySelector('#moreMenu .moreList button[data-mode="efficiency"]').hidden, null, { timeout: 15000 });
      await page.click('#moreMenu > summary'); await page.click(ITEM);
      await page.waitForSelector(`${V}:not(.hidden)`, { timeout: 15000 });
      assert.equal(await page.evaluate(() => CN.S.mode), 'efficiency', 'Paul is back in: it opens again');
      await page.close();
      // a reload on a deep link: kept while the server's answer is on its way, opened when it is Paul's
      const ctxD = await context('Paul K', { doorMs: 900 });
      const pd = await ctxD.newPage(); pd.on('pageerror', e => seen.errs.push(e.message));
      await pd.addInitScript(k => { try { sessionStorage.setItem('cn.eff.key', k); } catch (_) {} }, F.KEY);
      await pd.goto(sorter + '#efficiency/stations');
      await pd.waitForFunction(() => window.Efficiency && window.CN && document.readyState === 'complete', null, { timeout: 60000 });
      assert.equal(await pd.evaluate(() => CN.S.mode), 'nest', 'before the answer: the normal default view, nothing of the screen');
      assert.equal(await pd.evaluate(() => document.getElementById('efficiencyView').classList.contains('hidden')), true);
      await pd.waitForFunction(() => CN.S.mode === 'efficiency' && location.hash === '#efficiency/stations' && Efficiency.state.tab === 'stations', null, { timeout: 20000 });
      await pd.waitForSelector(`${V}:not(.hidden)`);
      console.log('  ✓ the master admin: entry shows, opens the tab, the passcode is still asked; another sign-in takes it away with no reload and the reads stop; Paul back shows it; a deep link waits for the answer and opens on the stations tab');
      await ctxD.close(); await ctx.close();
    }
    assert.deepEqual(seen.errs, [], 'no page errors: ' + seen.errs.join(' | '));
    console.log('  ✓ no page errors; nothing left the machine (' + seen.aborted + ' outside requests aborted)');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
