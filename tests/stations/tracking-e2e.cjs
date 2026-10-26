// Tracking, end to end (production-readiness audit, 26 Oct 2026). Paul wants "every user's activity tracked as they move through the sorter and the
// connected apps". One pass/fail test, in a real browser (Chromium, Playwright) over the fake shop of tests/stations/all-stations (the REAL station door,
// firebaseOrders: PIN login, sessions, activity; the REAL portal reader, employeeEfficiency; one in-memory Firestore; a fake clock). Nothing leaves the machine.
//
//   SORTER     A person signs in with their 6-digit Employee Number ONLY (EMPNUM, 26 Oct: charm-nest-account.js -> firebaseOrders {pinLogin}), answers
//              "Laser or Design?", presses work (a label, a laser mark): a Station_Sessions document is opened under the NAME (never the number), with the
//              role as the station; each press is a Station_Activity event under that name and rolls into Efficiency_Daily. A second person switches in with
//              their number: the first session ends "switched", the second opens. Signing out ends it "signOut". A name offered without its number signs nobody in.
//              The framed Design Station's commit/undo (CNAct.set) is credited to whoever is signed in at the sorter NOW. A timeline point held in the queue
//              across a switch, and work done offline then sent after a sign-out and the next sign-in, keep the person who did them (FIX, 26 Oct: CNTimeline
//              stamped `by` when its idle pump ran, not when the point was made). 5:00 pm signs a non-Admin out; midnight ends the Admin's session
//              "midnight"; a press after either records nothing under the old name. The role step is a real click on the Account panel's button.
//   SHIPPING   A person signs in with the number, types an order and presses the page's own Complete button: the activity event and the order timeline
//              points are under her name at Shipping; ten quiet minutes end the session "idle".
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
const FX = require('./all-stations/fixtures.cjs');

const TOR = (h, m) => Date.parse('2026-10-07T00:00:00Z') + 4 * 3600e3 + (h * 60 + m) * 60e3;
const PEOPLE = { dara: 'Dara F.', nico: 'Nico M.', ivy: 'Ivy R.', rosa: 'Rosa D.', admin: 'Paul K.' };
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
  const rows = async c => { const r = await W.list(c); return Array.isArray(r) ? r : []; };          // (the fake shop's list: one flat object per document, its id in _id)
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
      // (the role question, unless this person's role is already known on this computer today; a real click on the bar's button)
      await page.waitForFunction(([n, r]) => { const w = StationActivity.who(); return (!!w && w.person === n && w.role === r) || !!(document.querySelector('.cnNameBar[data-kind="role"] [data-role="' + r + '"]') || document.querySelector('.cnAcRole[data-role="' + r + '"]')); }, [name, role], { timeout: 20000 }).catch(async () => { throw new Error('no role step for ' + name + ': ' + JSON.stringify(await page.evaluate(() => ({ who: StationActivity.who(), name: B.employee, role: CNRole.state(), roles: [...document.querySelectorAll('[data-role]')].map(b => b.className + ':' + b.dataset.role + ':' + (b.offsetParent ? 'shown' : 'hidden')) })))); });
      const sel = '.cnNameBar[data-kind="role"] [data-role="' + role + '"], .cnAcRole[data-role="' + role + '"]';
      // (the panel's buttons are locked for a moment while the number is checked: a click then does nothing, as for a person, who clicks again)
      for (let i = 0; i < 12 && await page.$(sel) && !(await page.evaluate(([n, r]) => { const w = StationActivity.who(); return !!w && w.person === n && w.role === r; }, [name, role])); i++) { await page.click(sel, { force: true, timeout: 3000 }).catch(() => {}); await sleep(700); }
      await page.waitForFunction(([n, r]) => { const w = StationActivity.who(); return !!w && w.person === n && w.role === r; }, [name, role], { timeout: 20000 });
    };
    await check('sorter: the Employee Number signs Dara in as Design; a session opens under her name', async () => {
      await signNum(sorter, PEOPLE.dara, 'design');
      const s = await until(async () => (await sessionsOf(PEOPLE.dara)).find(x => x.endAt == null), 'Dara\'s session', 20000);
      assert(s.station === 'design' && s.role === 'design', 'session station/role: ' + s.station + '/' + s.role);
    });
    await check('sorter: a label and a laser mark are recorded under Dara and rolled into Efficiency_Daily', async () => {
      await sorter.evaluate(() => { CNAct('print', { orderId: '3900000001', parts: 2 }); CNAct('complete', { station: 'laser', orderId: '3900000001', parts: 2, orders: 1 }); CNAct.set(false, ['3900000005'], 'Set 4'); });
      await sorter.evaluate(() => StationActivity.flush());
      const ev = await until(async () => { const e = (await eventsOf(PEOPLE.dara)).filter(x => x.orderId === '3900000001'); return e.length >= 2 && e; }, 'Dara\'s events', 20000);
      assert((await eventsOf(PEOPLE.dara)).some(e => e.orderId === '3900000005' && e.action === 'complete'), 'the framed Design Station commit is not Dara\'s completion');
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
    await check('sorter: the framed Design Station\'s commit and undo are credited to whoever is signed in at the sorter NOW', async () => {
      // (charm-nest-bridge.js CNAct.set: the framed page records nothing itself; Dara (Design) committed 3900000005 above; Nico (Laser) undoing it is a note, never Dara's undo)
      await sorter.evaluate(() => { CNAct.set(true, ['3900000005'], 'Set 4'); return StationActivity.flush(); });
      const ev = await until(async () => { const r = (await rows('Station_Activity')).filter(e => e.orderId === '3900000005' && e.action !== 'complete'); return r.length && r; }, 'the undo record', 20000);
      assert(ev.length === 1 && ev[0].person === PEOPLE.nico && ev[0].action === 'note', 'framed undo: ' + JSON.stringify(ev.map(e => [e.person, e.action])));
    });
    await check('sorter: a timeline point waiting in the queue keeps the person who made it across a switch', async () => {
      // the idle pump is held (as on a busy page) while Nico makes a point and Dara switches in; it is handed over afterwards
      await sorter.evaluate(() => { try { Object.defineProperty(document, 'hidden', { configurable: true, get: () => false }); } catch (_) {} window.__ric = window.requestIdleCallback; window.__held = []; window.requestIdleCallback = fn => { window.__held.push(fn); return 1; }; CNTimeline.rec({ orderId: '3900000006', type: 'note', id: 'tq1', text: 'queued point' }); if (!window.__held.length || CNTimeline.pending() !== 1) throw new Error('the point was not held in the queue'); });
      const out = await sorter.evaluate(p => CNAccount.signInNumber(p), pins[PEOPLE.dara]);
      assert(out && out.ok, 'switch refused');
      await sorter.waitForSelector('.cnNameBar[data-kind="role"] [data-role="design"], .cnAcRole[data-role="design"]', { timeout: 20000 });
      await sorter.evaluate(() => { const b = document.querySelector('.cnNameBar[data-kind="role"] [data-role="design"]') || document.querySelector('.cnAcRole[data-role="design"]'); b.click(); });
      await sorter.waitForFunction(n => { const w = StationActivity.who(); return !!w && w.person === n; }, PEOPLE.dara, { timeout: 20000 });
      await sorter.evaluate(() => { window.requestIdleCallback = window.__ric; for (const f of window.__held.splice(0)) f({ timeRemaining: () => 50, didTimeout: true }); try { delete document.hidden; } catch (_) {} if (window.OrderTimeline && OrderTimeline.flush) OrderTimeline.flush(); });
      const tl = await until(async () => { const r = (await rows('Order_Timeline')).filter(t => t.orderId === '3900000006'); return r.length && r; }, 'the queued point', 20000);
      assert(tl.every(t => t.by === PEOPLE.nico), 'the queued point is credited to ' + JSON.stringify(tl.map(t => t.by)) + ', wanted ' + PEOPLE.nico);
    });
    await check('sorter: work done offline is kept under its person after a sign-out and the next sign-in', async () => {
      await sctx.setOffline(true);
      await sorter.evaluate(() => { CNAct('print', { orderId: '3900000007', parts: 1 }); CNTimeline.rec({ orderId: '3900000007', type: 'note', id: 'off1', text: 'offline point' }, false, true); });
      await sorter.evaluate(() => CNAccount.signOut());
      await sctx.setOffline(false);
      await signNum(sorter, PEOPLE.nico, 'laser');
      await sorter.evaluate(() => { StationActivity.flush(); if (window.OrderTimeline && OrderTimeline.flush) OrderTimeline.flush(); });
      const ev = await until(async () => { const r = (await rows('Station_Activity')).filter(e => e.orderId === '3900000007'); return r.length && r; }, 'the offline event', 30000);
      assert(ev.length === 1 && ev[0].person === PEOPLE.dara, 'offline event: ' + JSON.stringify(ev.map(e => e.person)));
      const tl = await until(async () => { const r = (await rows('Order_Timeline')).filter(t => t.orderId === '3900000007'); return r.length && r; }, 'the offline point', 30000);
      assert(tl.every(t => t.by === PEOPLE.dara), 'offline point by ' + JSON.stringify(tl.map(t => t.by)));
      const d = (await sessionsOf(PEOPLE.dara)).filter(x => x.endAt != null).pop();
      assert(d && d.endReason === 'signOut', 'Dara\'s offline sign-out ended ' + (d && d.endReason));
    });
    await check('sorter: Sign out ends Nico\'s session "signOut" and nobody is signed in', async () => {
      await sorter.evaluate(() => CNAccount.signOut());
      const n = await until(async () => { const l = (await sessionsOf(PEOPLE.nico)).sort((a, b) => a.startAt - b.startAt).pop(); return l && l.endAt != null && l; }, 'Nico\'s latest session to end', 20000);
      assert(n.endReason === 'signOut', 'Nico ended ' + n.endReason);
      assert(!(await sorter.evaluate(() => StationActivity.who())), 'still somebody signed in');
    });

    /* ───────── SHIPPING ───────── */
    const ship = await A.pin(null, 'shipping-1.html', PEOPLE.ivy);
    await check('shipping: the number signs Ivy in; a completed order is recorded under her name at Shipping', async () => {
      const s = await until(async () => (await sessionsOf(PEOPLE.ivy)).find(x => x.endAt == null), 'Ivy\'s session', 20000);
      assert(s.station === 'shipping', 'session station ' + s.station);
      await A.type(ship, FX.R.ship1);                          // (the order typed in, then the page's own Complete button)
      await A.shipComplete(ship);
      await A.flush(ship);
      await W.advance(4000, { step: 4000 });                     // (the timeline's outbox sends on the page's own timer, which runs on the fake clock)
      const ev = await until(async () => { const e = (await eventsOf(PEOPLE.ivy)).filter(x => x.action === 'complete'); return e.length && e; }, 'Ivy\'s completion from the Complete button', 20000);
      assert(ev[0].station === 'shipping' && ev[0].orderId === FX.R.ship1, 'Ivy event ' + JSON.stringify([ev[0].station, ev[0].action, ev[0].orderId]));
      const tl = await until(async () => { const r = (await rows('Order_Timeline')).filter(t => t.orderId === FX.R.ship1); return r.length && r; }, 'the shipping timeline point', 20000).catch(() => []);
      assert(tl.length >= 2 && tl.every(t => t.by === PEOPLE.ivy) && tl.some(t => t.type === 'shipped'), 'shipping timeline by: ' + JSON.stringify(tl.map(t => [t.type, t.by, t.station])));
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

    /* ───────── 5:00 PM and MIDNIGHT ───────── */
    await check('sorter: 5:00 pm signs Dara out; a press after it records nothing under her', async () => {
      await sorter.evaluate(() => CNAccount.signOut());
      await W.jump(TOR(16, 50) - await W.now());
      await signNum(sorter, PEOPLE.dara, 'design');
      await W.advance(3 * 60000);
      await sorter.evaluate(() => { CNAct('print', { orderId: '3900000011', parts: 1 }); return StationActivity.flush(); });   // (16:53: her last input)
      await W.advance(14 * 60000);
      const s = await until(async () => (await sessionsOf(PEOPLE.dara)).filter(x => x.startAt >= TOR(16, 45)).find(x => x.endAt != null), 'Dara\'s 5 pm sign-out', 30000);
      assert(['closing', 'idle'].includes(s.endReason) && s.endAt <= TOR(17, 4), 'Dara ended ' + s.endReason + ' at ' + new Date(s.endAt).toISOString());
      assert(!(await sorter.evaluate(() => StationActivity.who())), 'Dara is still signed in after 5 pm');
      await sorter.evaluate(() => { CNAct('print', { orderId: '3900000012', parts: 1 }); return StationActivity.flush(); });
      await sleep(800);
      assert(!(await rows('Station_Activity')).some(e => e.orderId === '3900000012'), 'a press after the 5 pm sign-out was recorded');
    });
    await check('sorter: the Admin stays until midnight; midnight ends the session "midnight" and nothing is recorded under him after', async () => {
      await W.jump(TOR(23, 50) - await W.now());               // (a jump is a closed lid: the Admin signs in after it, and the clock then runs to midnight)
      const out = await sorter.evaluate(p => CNAccount.signInNumber(p), pins[PEOPLE.admin]);
      await sorter.waitForFunction(n => { const w = StationActivity.who(); return !!w && w.person === n; }, PEOPLE.admin, { timeout: 25000 }).catch(async e => { throw new Error('Admin sign-in: ' + JSON.stringify(out) + ' ' + JSON.stringify(await sorter.evaluate(() => ({ who: StationActivity.who(), name: window.B && B.employee, role: CNRole.state() })))); });
      await W.advance(5 * 60000);
      assert((await sessionsOf(PEOPLE.admin)).find(x => x.endAt == null), 'the Admin was signed out before midnight');
      await W.advance(8 * 60000);
      const s = await until(async () => (await sessionsOf(PEOPLE.admin)).find(x => x.endAt != null), 'the Admin\'s midnight sign-out', 30000);
      assert(s.endReason === 'midnight', 'the Admin ended ' + s.endReason);
      await sorter.evaluate(() => { CNAct('print', { orderId: '3900000013', parts: 1 }); return StationActivity.flush(); });
      await sleep(800);
      assert(!(await rows('Station_Activity')).some(e => e.orderId === '3900000013' && e.person === PEOPLE.admin), 'a press after midnight was recorded under the Admin');
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
