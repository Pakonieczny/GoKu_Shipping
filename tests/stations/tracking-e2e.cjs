// Tracking, end to end (production-readiness audit, 26 Oct 2026). Paul wants "every user's activity tracked as they move through the sorter and the
// connected apps". One pass/fail test, in a real browser (Chromium, Playwright) over the fake shop of tests/stations/all-stations (the REAL station door,
// firebaseOrders: PIN login, sessions, activity; the REAL portal reader, employeeEfficiency; one in-memory Firestore; a fake clock). Nothing leaves the machine.
//
//   SORTER     A person signs in with their 6-digit Employee Number ONLY (EMPNUM, 26 Oct: charm-nest-account.js -> firebaseOrders {pinLogin}), answers
//              "Laser or Design?", presses work (a label, a laser mark): a Station_Sessions document is opened under the NAME (never the number), with the
//              role as the station; each press is a Station_Activity event under that name and rolls into Efficiency_Daily. A second person switches in with
//              their number: the first session ends "switched", the second opens. Signing out ends it "signOut". A name offered without its number signs nobody in.
//   SHIPPING   A person signs in with the number, completes an order: the event is under the name at Shipping; ten quiet minutes end the session "idle".
//   DESIGN     design-message.html: number sign-in, then the Sign out button ends the session "signOut".
//   CONSOLE    employeeEfficiency (the Employee efficiency console's reader) lists every one of them with their station.
//   NEVER      No stored document carries a typed number; nothing reaches a host outside the fake.
//
//   node tests/stations/tracking-e2e.cjs            (CHROMIUM=... PW_DIR=...)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const pwCore = (() => { for (const d of [process.env.PW_DIR, path.join(root, 'node_modules'), '/opt/node-tools/node_modules']) { if (!d) continue; try { return require(path.join(d, 'playwright-core')); } catch (_) {} } return require('playwright-core'); })();
const CHROME = process.env.CHROMIUM || (() => { try { const d = '/opt/pw-browsers'; const c = fs.readdirSync(d).filter(x => /^chromium-/.test(x)).sort().pop(); for (const s of ['chrome-linux64/chrome', 'chrome-linux/chrome']) if (fs.existsSync(path.join(d, c, s))) return path.join(d, c, s); } catch (_) {} return undefined; })();
const { World, sleep } = require('./all-stations/world.cjs');
const { Apps, until } = require('./all-stations/apps.cjs');

const TOR = (h, m) => Date.parse('2026-10-07T00:00:00Z') + 4 * 3600e3 + (h * 60 + m) * 60e3;
const PEOPLE = { dara: 'Dara F.', nico: 'Nico M.', ivy: 'Ivy R.', rosa: 'Rosa D.' };
const pins = {}; { const used = new Set(); for (const n of Object.values(PEOPLE)) { let p; do { p = String(100000 + Math.floor(Math.random() * 900000)); } while (used.has(p) || /^(\d)\1{5}$/.test(p)); used.add(p); pins[n] = p; } }
const cast = { pins, people: PEOPLE };

let failed = 0, passed = 0;
const check = async (name, fn) => { try { await fn(); passed++; console.log('  ok   ' + name); } catch (e) { failed++; console.log('  FAIL ' + name + '\n       ' + String(e && e.message || e).slice(0, 600)); } };
const assert = (ok, msg) => { if (!ok) throw new Error(msg); };

(async () => {
  const t0 = Date.now();
  const browser = await pwCore.chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const W = await World.start({ now: new Date(TOR(9, 0)).toISOString(), browser });
  const A = Apps(W, cast);
  await W.post('/__ctl/roster', Object.fromEntries(Object.entries(pins).map(([n, p]) => [p, n])));
  const rows = async c => { const r = await W.list(c); return (r.docs || r.rows || r || []).map(d => d.data ? Object.assign({ _id: d.id }, d.data) : d); };
  const sessionsOf = async person => (await rows('Station_Sessions')).filter(s => s.person === person);
  const eventsOf = async person => (await rows('Station_Activity')).filter(e => e.person === person);
  try {
    /* ───────── SORTER ───────── */
    const sctx = await W.context({ label: 'sorter' });
    const sorter = await A.sorterApp(sctx);
    await check('sorter: a name offered without its Employee Number signs nobody in', async () => {
      await sorter.evaluate(n => { try { CNAccount.signIn(n); } catch (_) {} }, PEOPLE.dara);
      await sleep(800);
      assert(!(await sorter.evaluate(() => window.StationActivity.who())), 'somebody is signed in from a bare name');
      assert(!(await sessionsOf(PEOPLE.dara)).length, 'a session was opened from a bare name');
    });
    const signNum = async (page, name, role) => {
      const out = await page.evaluate(p => CNAccount.signInNumber(p), pins[name]);
      assert(out && out.ok, 'the number was refused: ' + JSON.stringify(out));
      await page.waitForSelector('.cnNameBar[data-kind="role"] [data-role="' + role + '"], .cnAc [data-role="' + role + '"]', { timeout: 20000 });
      await page.evaluate(r => { const b = document.querySelector('.cnNameBar[data-kind="role"] [data-role="' + r + '"]') || document.querySelector('.cnAc [data-role="' + r + '"]'); b.click(); }, role);
      await page.waitForFunction(([n, r]) => { const w = StationActivity.who(); return !!w && w.person === n && w.role === r; }, [name, role], { timeout: 20000 });
    };
    await check('sorter: the Employee Number signs Dara in as Design; a session opens under her name', async () => {
      await signNum(sorter, PEOPLE.dara, 'design');
      const s = await until(async () => (await sessionsOf(PEOPLE.dara)).find(x => x.endAt == null), 'Dara\'s session', 20000);
      assert(s.station === 'design' && s.role === 'design', 'session station/role: ' + s.station + '/' + s.role);
    });
    await check('sorter: a label and a laser mark are recorded under Dara and rolled into Efficiency_Daily', async () => {
      await sorter.evaluate(() => { CNAct('print', { orderId: '3900000001', parts: 2 }); CNAct('complete', { station: 'laser', orderId: '3900000001', parts: 2, orders: 1 }); });
      await sorter.evaluate(() => StationActivity.flush());
      const ev = await until(async () => { const e = await eventsOf(PEOPLE.dara); return e.length >= 2 && e; }, 'Dara\'s events', 20000);
      assert(ev.every(e => e.station === 'design' && e.orderId === '3900000001'), 'events: ' + JSON.stringify(ev.map(e => [e.station, e.action, e.orderId])));
      assert(ev.some(e => e.action === 'print') && ev.some(e => e.action === 'complete'), 'actions: ' + ev.map(e => e.action));
      const daily = (await rows('Efficiency_Daily')).filter(d => d.person === PEOPLE.dara);
      assert(daily.length === 1, 'Efficiency_Daily rows for Dara: ' + daily.length);
    });
    await check('sorter: Nico switches in with his number; Dara\'s session ends "switched"', async () => {
      await signNum(sorter, PEOPLE.nico, 'laser');
      const d = await until(async () => (await sessionsOf(PEOPLE.dara)).find(x => x.endAt != null), 'Dara\'s session to end', 20000);
      assert(d.endReason === 'switched', 'Dara ended ' + d.endReason);
      const n = await until(async () => (await sessionsOf(PEOPLE.nico)).find(x => x.endAt == null), 'Nico\'s session', 20000);
      assert(n.station === 'laser', 'Nico station ' + n.station);
      await sorter.evaluate(() => { CNAct('complete', { station: 'laser', orderId: '3900000002', parts: 1, orders: 1 }); return StationActivity.flush(); });
      const ev = await until(async () => { const e = await eventsOf(PEOPLE.nico); return e.length && e; }, 'Nico\'s event', 20000);
      assert(ev[0].station === 'laser', 'Nico event station ' + ev[0].station);
    });
    await check('sorter: Sign out ends Nico\'s session "signOut" and nobody is signed in', async () => {
      await sorter.evaluate(() => CNAccount.signOut());
      const n = await until(async () => (await sessionsOf(PEOPLE.nico)).find(x => x.endAt != null), 'Nico\'s session to end', 20000);
      assert(n.endReason === 'signOut', 'Nico ended ' + n.endReason);
      assert(!(await sorter.evaluate(() => StationActivity.who())), 'still somebody signed in');
    });

    /* ───────── SHIPPING ───────── */
    const ship = await A.pin(null, 'shipping-1.html', PEOPLE.ivy);
    await check('shipping: the number signs Ivy in; a completed order is recorded under her name at Shipping', async () => {
      const s = await until(async () => (await sessionsOf(PEOPLE.ivy)).find(x => x.endAt == null), 'Ivy\'s session', 20000);
      assert(s.station === 'shipping', 'session station ' + s.station);
      await ship.evaluate(() => StationActivity.log('complete', { orderId: '3900000003', parts: 1, orders: 1 }));
      await A.flush(ship);
      const ev = await until(async () => { const e = await eventsOf(PEOPLE.ivy); return e.length && e; }, 'Ivy\'s event', 20000);
      assert(ev[0].station === 'shipping' && ev[0].action === 'complete', 'Ivy event ' + JSON.stringify([ev[0].station, ev[0].action]));
    });

    /* ───────── DESIGN (a PIN page) ───────── */
    const des = await A.pin(null, 'design-message.html', PEOPLE.rosa);
    await check('design: the number signs Rosa in at Design; Sign out ends it "signOut"', async () => {
      const s = await until(async () => (await sessionsOf(PEOPLE.rosa)).find(x => x.endAt == null), 'Rosa\'s session', 20000);
      assert(s.station === 'design', 'session station ' + s.station);
      await A.pinSignOut(des);
      const e = await until(async () => (await sessionsOf(PEOPLE.rosa)).find(x => x.endAt != null), 'Rosa\'s session to end', 20000);
      assert(e.endReason === 'signOut', 'Rosa ended ' + e.endReason);
    });

    /* ───────── IDLE (10 quiet minutes, a non-Admin) ───────── */
    await check('shipping: ten quiet minutes end Ivy\'s session "idle"', async () => {
      await W.advance(12 * 60000);
      const e = await until(async () => (await sessionsOf(PEOPLE.ivy)).find(x => x.endAt != null), 'Ivy\'s session to end', 30000);
      assert(e.endReason === 'idle', 'Ivy ended ' + e.endReason);
    });

    /* ───────── CONSOLE ───────── */
    await check('console: employeeEfficiency lists Dara, Nico and Ivy', async () => {
      const r = await W.eff({ op: 'overview', trend: false });
      const txt = JSON.stringify((r && r.people) || r);
      for (const n of [PEOPLE.dara, PEOPLE.nico, PEOPLE.ivy]) assert(txt.includes(n), n + ' missing from the console: ' + txt.slice(0, 400));
    });

    /* ───────── SANDBOX: a rehearsal never reaches the real numbers ───────── */
    await check('sandbox: a sorter in the sandbox writes its session and events only to the Sandbox_ copies', async () => {
      const before = { s: (await rows('Station_Sessions')).length, a: (await rows('Station_Activity')).length, d: (await rows('Efficiency_Daily')).length };
      const bctx = await W.context({ label: 'sorter sandbox' });
      await bctx.addInitScript(() => {
        try { if (location.hostname === '127.0.0.1') localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'on', sandboxStream: 'off' })); } catch (_) {}
        window.confirm = () => true; window.prompt = () => null;
      });
      const sb = await W.open(bctx, 'charm-nest-1.html', { sorter: true });
      await sb.waitForFunction(() => window.CNAccount && window.CNAct && window.StationSession && StationSession.page() && document.readyState === 'complete', null, { timeout: 90000 });
      assert(await sb.evaluate(() => StationSession.page().sandbox), 'the sorter page is not in the sandbox');
      await signNum(sb, PEOPLE.dara, 'design');
      await sb.evaluate(() => { CNAct('print', { orderId: '3900000009', parts: 1 }); return StationActivity.flush(); });
      await until(async () => (await rows('Sandbox_Station_Activity')).some(e => e.person === PEOPLE.dara), 'the sandbox event', 20000);
      assert((await rows('Sandbox_Station_Sessions')).some(s => s.person === PEOPLE.dara), 'no sandbox session');
      await sb.evaluate(() => CNAccount.signOut()); await sleep(1500);
      const after = { s: (await rows('Station_Sessions')).length, a: (await rows('Station_Activity')).length, d: (await rows('Efficiency_Daily')).length };
      assert(JSON.stringify(after) === JSON.stringify(before), 'production collections changed: ' + JSON.stringify([before, after]));
      await W.closeContext(bctx);
    });

    /* ───────── NEVER ───────── */
    await check('never: no stored session or event carries a typed Employee Number; nothing left the fake', async () => {
      const all = JSON.stringify([await rows('Station_Sessions'), await rows('Station_Activity'), await rows('Efficiency_Daily'), await rows('Station_Live')]);
      for (const [n, p] of Object.entries(pins)) assert(!all.includes(p), 'the number of ' + n + ' is stored');
      assert(!W.outside.length, 'requests outside the fake: ' + JSON.stringify(W.outside.slice(0, 5)));
    });
  } catch (e) { failed++; console.log('  FAIL (setup) ' + (e && e.stack || e)); }
  finally { try { W.stop(); } catch (_) {} try { await browser.close(); } catch (_) {} }
  console.log(`\ntracking-e2e: ${passed} passed, ${failed} failed (${Math.round((Date.now() - t0) / 1000)} s)`);
  process.exit(failed ? 1 : 0);
})();
