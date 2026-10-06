// All stations, end to end (stations round 2, worker ST1). One test that proves the whole round works together, in a real browser (Chromium, Playwright)
// over a fake shop on this machine: nothing leaves it.
//
//   WHO WORKS WHERE      A person signs in at every station app (the way that app asks: a six-digit number, a name in the order chat, a name in the
//                        small bar, the inbox's own sign-in) and does one real action there: a scan (a phone's, through the scanner page and its camera),
//                        a stamp, a label, a sorted order, a design finished, a reply sent. Every station app and every scanner page is used.
//   THE PORTAL           Then the Employee efficiency portal (Real view) is read the way Paul reads it: the Stations board has no Sorter and no QR Printer
//                        card; each station shows exactly the people signed in, their task or role and the order in hand; Welding shows two groups and no
//                        order throughput; Laser and Design are two cards; each person's page shows hours and work per station, equal to the board and to
//                        the Overview; the Inbox counts replies, orders and customers.
//   THE CLOCK            Then the shop's clock moves: ten minutes without input signs out a person who is not an Admin at every station (the Admin stays);
//                        17:00 Toronto signs out the people with no recent input; the portal says so plainly and the hours end at the last input.
//   NOTHING LOST         Work on screen and drafts stay after those sign-outs. Zero Etsy calls, zero paid calls, zero requests outside the fake.
//
// The fake shop (tests/stations/all-stations/*): the repo served on two origins, the REAL station door (firebaseOrders: PIN login, sessions, activity, live,
// the scanners' relay) and the REAL portal reader (employeeEfficiency) over one in-memory Firestore that computes rollups the way Firestore does, a
// fake clock for the shop and for every browser, invented orders and people (PINs drawn per run, typed into the pages' own boxes, never printed), the
// inbox's recorder (the real recordOutcome). Etsy, Chit Chats and the model are faked or refused; every other host is refused and written down.
// Every check names the workers whose pieces it needs; a check that fails while one of them has not landed is PENDING (with the name), not failed.
//
//   node tests/stations/all-stations-e2e.cjs            (CHROMIUM=... PW_DIR=... SHOTS_DIR=... ONLY_CHECKS=A-,B- STRICT=1 PHASES=A,B,C)
'use strict';
const fs = require('fs'), os = require('os'), path = require('path'), { spawnSync } = require('child_process');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || '';
const pwCore = (() => { for (const d of [pwDir, path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules']) { if (!d) continue; try { return require(path.join(d, 'playwright-core')); } catch (_) {} } return require('playwright-core'); })();
const { chromium } = pwCore;
const CHROME = process.env.CHROMIUM || (() => { try { const d = '/opt/pw-browsers'; const c = fs.readdirSync(d).filter(x => /^chromium-/.test(x)).sort().pop(); return c ? path.join(d, c, 'chrome-linux/chrome') : undefined; } catch (_) { return undefined; } })();
const SHOTS = process.env.SHOTS_DIR || (fs.existsSync('/mnt/project-files/plans/stations-round2') ? '/mnt/project-files/plans/stations-round2/st1-shots' : path.join(os.tmpdir(), 'st1-shots'));
const PHASES = (process.env.PHASES || 'A,B,C').split(',');

const { World, sleep } = require('./all-stations/world.cjs');
const { Apps, until } = require('./all-stations/apps.cjs');
const { Portal } = require('./all-stations/portal.cjs');
const { makeCast, rosterOf, PEOPLE } = require('./all-stations/cast.cjs');
const FX = require('./all-stations/fixtures.cjs');
const { check, pending, results, table, landed, landedAll } = require('./all-stations/registry.cjs');
const R = FX.R, THREADS = FX.THREAD_IDS;

const assert = (ok, msg) => { if (!ok) throw new Error(msg); };
const eq = (a, b, msg) => { const x = JSON.stringify(a), y = JSON.stringify(b); if (x !== y) throw new Error(msg + ': got ' + x + ', wanted ' + y); };
const mv = x => (x && typeof x === 'object' ? x.value : x);          // (the reader hands figures as { value, prev, delta ... })
const same = (a, b) => JSON.stringify([...a].sort()) === JSON.stringify([...b].sort());
const say = s => process.stdout.write(s + '\n');
const TOR = (h, m) => Date.parse('2026-10-07T00:00:00Z') + 4 * 3600e3 + (h * 60 + m) * 60e3;        // Toronto wall time on the test day (EDT, UTC-4)
const hhmm = ms => new Intl.DateTimeFormat('en-GB', { timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }).format(new Date(ms));
fs.mkdirSync(SHOTS, { recursive: true });

(async () => {
  const t00 = Date.now();
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox', '--autoplay-policy=no-user-gesture-required'] });
  const W = await World.start({ now: new Date(TOR(9, 0)).toISOString(), browser });
  const cast = makeCast();
  await W.post('/__ctl/roster', rosterOf(cast));
  for (const [u, o] of Object.entries(FX.OPS)) await W.post('/__ctl/put', { path: 'EtsyMail_Operators/' + u, data: { displayName: o.display, role: o.role, username: u } });
  const A = Apps(W, cast), P = Portal(W, cast);
  const S = { pages: {}, ctx: {}, last: {}, input: {} };
  const need = k => { if (!S.pages[k] || S.pages[k].isClosed()) throw new Error(k + ': the page is not there (its own sign-in check failed above)'); return S.pages[k]; };

  /* ───────── the shop's own records, read straight (the test never trusts the portal for what the stations wrote) ───────── */
  const events = async f => (await W.list('Station_Activity')).filter(f || (() => true)).sort((a, b) => (a.at || 0) - (b.at || 0));
  const sessionsOf = async f => (await W.list('Station_Sessions')).filter(f || (() => true));
  const sessionFor = async (person, device, task) => { const l = await sessionsOf(s => s.person === person && s.device === device && (task === undefined || (s.task || '') === task)); return l.sort((a, b) => (b.startAt || 0) - (a.startAt || 0))[0]; };
  const brief = e => [e.action, e.person, e.device, e.orderId || '', e.parts, e.orders].join('|');
  /** waits until each wanted event exists (all fields given match); says what was missing */
  async function seen(wants, ms) {
    const t0 = Date.now(); let got;
    for (;;) {
      const all = await events();
      got = wants.map(w => all.filter(e => Object.entries(w).every(([k, v]) => (typeof v === 'function' ? v(e[k]) : e[k] === v))));
      if (got.every(g => g.length)) return got.map(g => g[0]);
      if (Date.now() - t0 > (ms || 20000)) throw new Error('no such event yet: ' + JSON.stringify(wants[got.findIndex(g => !g.length)]) + ' · stored for that page: ' + all.filter(e => e.device === (wants[got.findIndex(g => !g.length)] || {}).device).map(brief).join(' ; '));
      await sleep(250);
    }
  }
  const count = async f => (await events(f)).length;
  const hasPin = text => Object.values(cast.pins).some(p => text.includes(p));
  const endedAs = async (person, device, task, ms) => {
    const t0 = Date.now();
    for (;;) { const s = await sessionFor(person, device, task); if (s && s.endAt != null) return s; if (Date.now() - t0 > (ms || 15000)) throw new Error('the session of ' + person + ' at ' + device + ' did not end' + (s ? '' : ' (and there is none)')); await sleep(250); }
  };
  const openSession = async (person, device, task) => { const s = await sessionFor(person, device, task); assert(s, 'no session for ' + person + ' at ' + device); assert(s.endAt == null, 'the session of ' + person + ' at ' + device + ' is already ended (' + s.endReason + ')'); return s; };

  /* ───────── keeping the kept pages awake: real input on them while the test works elsewhere (the idle rule is about input, not about time) ───────── */
  const keep = {
    pages: new Set(), timer: null,
    add(...p) { p.forEach(x => this.pages.add(x)); }, drop(p) { this.pages.delete(p); },
    async poke() { for (const p of [...this.pages]) { try { if (p.isClosed()) { this.pages.delete(p); continue; } await p.mouse.move(60 + Math.random() * 400, 140 + Math.random() * 200); await p.mouse.move(80 + Math.random() * 400, 150 + Math.random() * 200); } catch (_) {} } },
    start() { if (!this.timer) this.timer = setInterval(() => this.poke(), 25000); }, stop() { clearInterval(this.timer); this.timer = null; }
  };
  const shot = async (target, name) => { try { await target.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { say('   (no screenshot ' + name + ': ' + String(e.message).slice(0, 80) + ')'); } };
  // the time of a page's last input, in the SHOP's time (a computer's own clock can be seconds off, a browser that was opened late under load is; the session door
  // undoes that with the time the page sends along, so the stored hours are in the shop's time and so is this)
  const lastInputOf = async page => {
    const [pt, li] = await page.evaluate(() => { try { return [Date.now(), StationSession.lastInput()]; } catch (_) { return [Date.now(), 0]; } });
    const sn = await W.now(); return li ? li + (sn - pt) : 0;
  };

  const phonesCtx = await W.context({ label: 'phones' });
  const scanned = async (file, code, desk, rid) => {     // a phone reads a QR through its camera; the desk page has the order in its box
    const sc = await A.scanner(phonesCtx, file);
    try { await A.scan(sc, code); if (desk) await A.desktopHas(desk, rid || code); } finally { await sc.close().catch(() => {}); }
  };
  const quiet = async page => sleep(1100);
  const signedOutCleanly = async (page, person, device, task) => {
    await page.evaluate(() => window.dispatchEvent(new Event('focus'))).catch(() => {});
    return endedAs(person, device, task, 20000);
  };

  say('All stations, end to end · fake shop at ' + hhmm(await W.now()) + ' Toronto · landed: ' + Object.entries(landedAll()).filter(([, v]) => v).map(([k]) => k).join(' ') + '  ·  not yet: ' + Object.entries(landedAll()).filter(([, v]) => !v).map(([k]) => k).join(' '));

  /* ═════════════════════════════ 0 · the door and the Admin list ═════════════════════════════ */
  await check('A-ADM', 'the Admin door (real, one name): Paul K, Paul K. and Paul are Admins, the crew are not; the inbox\'s Paul is "Paul K"', ['AD2'], async () => {
    const ask = async n => { const r = await fetch(W.ctl + '/.netlify/functions/firebaseOrders', { method: 'POST', body: JSON.stringify({ stationAdmin: n }) }); const j = await r.json(); assert(r.status === 200 && j && j.ok === true, 'the door did not answer for ' + n + ': ' + r.status + ' ' + JSON.stringify(j)); assert(Object.keys(j).sort().join() === 'admin,ok', 'the answer has more than ok and admin: ' + Object.keys(j)); return j.admin; };
    for (const n of ['Paul K', 'Paul K.', 'Paul_K', 'Paul']) assert(await ask(n) === true, n + ' should be an Admin');
    assert(FX.OPS.paul.display === 'Paul K', 'the inbox operator Paul is shown as "Paul K"');
    for (const n of Object.values(PEOPLE).filter(n => n !== PEOPLE.admin)) assert(await ask(n) === false, n + ' should not be an Admin');
  });

  /* ═════════════════════════════ PHASE A · every app, one real action ═════════════════════════════ */
  if (PHASES.includes('A')) {
    say('\nA · sign in and one real action at every station app and scanner');
    keep.start();

    /* ── Welding: two people at once, Welding and Matching; the phone scans for the Matching person ── */
    await check('A-WE1', 'weld-1: two people sign in (PIN, then Welding or Matching); a phone scan is Matching\'s one "matched" event, never the welder\'s', ['WS1', 'WS3', 'WS2'], async () => {
      const page = S.pages.weld = await A.weld(null, PEOPLE.matcher, 'matching'); keep.add(page);
      await A.weldSignIn(page, PEOPLE.welder, 'welding');
      const sm = await openSession(PEOPLE.matcher, 'weld-1', 'matching'), sw = await openSession(PEOPLE.welder, 'weld-1', 'welding');
      assert(!sm.employeeId && !sw.employeeId, 'a session carries a PIN or id');
      await scanned('weld-scan-1.html', R.weldA, page, R.weldA);
      await sleep(1200);
      await A.type(page, R.weldB);
      await A.flush(page);
      const [m] = await seen([{ action: 'matched', device: 'weld-1', orderId: R.weldA }]);
      eq([m.person, m.task, m.station], [PEOPLE.matcher, 'matching', 'welding'], 'the matched scan is credited to the Matching person');
      const all = await events(e => e.device === 'weld-1');
      assert(!all.some(e => e.person === PEOPLE.welder && (e.action === 'matched' || e.orderId === R.weldA)), 'the welder was credited with the phone scan: ' + all.map(brief).join(' ; '));
      assert(all.filter(e => e.action === 'matched' && e.orderId === R.weldA).length === 1, 'the phone scan is not one matched event');
      assert(!all.some(e => e.action === 'scan' && e.orderId === R.weldA), 'a phone scan also logged a plain scan (it counts twice)');
      await seen([{ action: 'scan', device: 'weld-1', orderId: R.weldB, person: PEOPLE.matcher }]);
    });

    /* ── Assembly: assembly-1 (Hana), assembly-3 (Nico, who later works at Design too), assembly-4 (Hana), assembly-2 (Michael, kept) ── */
    const asmDesk = async (n, person, rid, o) => {
      const page = await A.pin(null, `assembly-${n}.html`, person);
      if (o.keep) { S.pages['asm' + n] = page; keep.add(page); }
      await scanned(`assembly-scan-${n}.html`, rid, page, rid);
      await sleep(1200);
      await A.say(page, o.stamp || 'QA1'); await A.flush(page);
      const [sc, done] = await seen([{ action: 'scan', device: 'assembly-' + n, person, orderId: rid }, { action: 'complete', device: 'assembly-' + n, person, orderId: rid }]);
      assert(sc.station === 'assembly' && done.orders === 1 && done.parts >= 1, 'assembly-' + n + ': scan/complete not as expected: ' + brief(sc) + ' / ' + brief(done));
      assert(sc.session && sc.computer, 'assembly-' + n + ': the event has no session or computer');
      assert(!(await events(e => e.device === 'assembly-' + n)).some(e => e.person !== person), 'assembly-' + n + ': an event for another person');
      const s = await openSession(person, 'assembly-' + n); assert(!s.employeeId, 'a session carries an id');
      return page;
    };
    await check('A-AS1', 'assembly-1 + assembly-scan-1: sign in, phone scan, QA1 stamp = one completion, sign out', ['SA2'], async () => {
      const page = await asmDesk(1, PEOPLE.asmSmoke, R.asm1, {});
      await A.pinSignOut(page); await signedOutCleanly(page, PEOPLE.asmSmoke, 'assembly-1');
      await W.closeContext(page.context());
    });
    await check('A-AS3', 'assembly-3 + assembly-scan-3 (Nico): phone scan, QA1; he signs out and works at Design in the Sorter app later', ['SA2'], async () => {
      const page = S.pages.asm3 = await asmDesk(3, PEOPLE.multi, R.asm3, {}); S.last.nicoAsm = await lastInputOf(page);
      // a few quiet minutes of work at this desk: the hours at Assembly are real hours
      await W.advance(5 * 60000, { step: 60000, between: async () => { await keep.poke(); await page.mouse.move(120 + Math.random() * 60, 140).catch(() => {}); } });
      await A.pinSignOut(page); await signedOutCleanly(page, PEOPLE.multi, 'assembly-3');
      await W.closeContext(page.context());
    });
    await check('A-AS4', 'assembly-4 + assembly-scan-4: sign in, phone scan, Done stamp, sign out', ['SA2'], async () => {
      const page = await asmDesk(4, PEOPLE.asmSmoke, R.asm4, { stamp: 'Done' });
      await A.pinSignOut(page); await signedOutCleanly(page, PEOPLE.asmSmoke, 'assembly-4');
      await W.closeContext(page.context());
    });
    await check('A-AS2', 'assembly-2 + assembly-scan-2 (kept): QA1 stamp = one completion, then the next order stays in hand with a draft typed', ['SA2'], async () => {
      const page = await asmDesk(2, PEOPLE.asm, R.asm2, { keep: true });
      await scanned('assembly-scan-2.html', R.asm5, page, R.asm5);
      await page.fill('#britesMsgInput', 'Check the clasp on this one before it goes');             // an unsent note: it must still be there after any sign-out
      S.last.asmDraft = 'Check the clasp on this one before it goes';
    });

    /* ── Shipping: shipping-2, shipping-3 (Ravi), shipping-1 (Ivy, kept) ── */
    const shipDesk = async (n, person, rid, o) => {
      const page = await A.pin(null, `shipping-${n}.html`, person);
      if (o.keep) { S.pages['ship' + n] = page; keep.add(page); }
      await scanned(`shipping-scan-${n}.html`, rid, page, rid);
      await sleep(1200);
      await A.shipBuyAndPrint(page); await A.shipComplete(page); await A.flush(page);
      const [scn, prn, cmp] = await seen([{ action: 'scan', device: 'shipping-' + n, person, orderId: rid }, { action: 'print', device: 'shipping-' + n, person }, { action: 'complete', device: 'shipping-' + n, person, orderId: rid }]);
      assert(cmp.orders === 1 && scn.station === 'shipping', 'shipping-' + n + ': the events are not as expected');
      return page;
    };
    for (const [n, person, rid] of [[2, PEOPLE.shipSmoke, R.ship2], [3, PEOPLE.shipSmoke, R.ship3]]) {
      await check('A-SH' + n, `shipping-${n} + shipping-scan-${n}: phone scan, label bought and printed, Complete Order, sign out`, ['SA3'], async () => {
        const page = await shipDesk(n, person, rid, {});
        await A.pinSignOut(page); await signedOutCleanly(page, person, 'shipping-' + n);
        await W.closeContext(page.context());
      });
    }
    await check('A-SH1', 'shipping-1 + shipping-scan-1 (kept): scan, label, Complete Order, then the next order stays in hand', ['SA3'], async () => {
      const page = await shipDesk(1, PEOPLE.ship, R.ship1, { keep: true });
      await scanned('shipping-scan-1.html', R.ship4, page, R.ship4);
    });

    /* ── Sorting: sorting-2 (Omar) first, then sorting-1 (Maya, kept) and the phone's batch, which both desks would hear ── */
    await check('A-SO2', 'sorting-2: number in the name chip, a typed order, its sticker = one print and the order sorted once, QR Printer made the label', ['SA1', 'PB1'], async () => {
      const page = await A.sorting(null, 'sorting-2.html', PEOPLE.sortSmoke);
      await A.sortBatch(page, [R.sort1]); await A.sortSticker(page, R.sort1); await A.flush(page);
      await seen([{ action: 'scan', device: 'sorting-2', person: PEOPLE.sortSmoke, orderId: R.sort1 }, { action: 'print', device: 'sorting-2', orderId: R.sort1 }, { action: 'complete', device: 'sorting-2', orderId: R.sort1, orders: 1 }]);
      const fr = await A.qrFrame(page); assert(fr && fr.made >= 1, 'the QR Printer frame made no label: ' + JSON.stringify(fr && { made: fr.made }));
      await page.evaluate(() => StationSession.signedOut('signOut'));                      // (the Sorting pages have no Sign Out button)
      await endedAs(PEOPLE.sortSmoke, 'sorting-2', undefined, 20000);
      await W.closeContext(page.context());
    });
    await check('A-SO1', 'sorting.html (kept): a typed order and its sticker (QR Printer), then the phone\'s batch (sort-scan) loads and is the order in hand', ['SA1', 'PB1'], async () => {
      const page = S.pages.sort = await A.sorting(null, 'sorting.html', PEOPLE.sort); keep.add(page);
      await A.sortBatch(page, [R.sort2]); await A.sortSticker(page, R.sort2); await A.flush(page);
      await seen([{ action: 'print', device: 'sorting-1', orderId: R.sort2, person: PEOPLE.sort }, { action: 'complete', device: 'sorting-1', orderId: R.sort2, orders: 1 }]);
      const fr = await A.qrFrame(page); assert(fr && fr.made >= 1, 'the QR Printer frame made no label');
      const b36 = n => BigInt(n).toString(36);
      const sc = await A.scanner(phonesCtx, 'sort-scan.html');
      try {
        await sc.evaluate(() => window.__showQr('')); await sleep(700);
        await sc.evaluate(t => window.__showQr(t), `B36|silver|${b36(R.sort3)}.${b36(R.sort4)}`);
        await until(() => sc.evaluate(() => document.getElementById('orderNumInput').value), 'sort-scan to read the code', 30000);
        assert((await sc.evaluate(() => document.getElementById('orderNumInput').value)) === [R.sort3, R.sort4].join(','), 'sort-scan decoded something else');
        await sc.click('#sendBtn');
      } finally { await sleep(500); }
      await until(() => page.evaluate(() => (window.cachedOrderItems || []).length >= 2 && document.getElementById('etsyOrderNumber').value.includes('3820000403')), 'the phone batch on the desk', 40000);
      await sc.close().catch(() => {}); await A.flush(page);
      await seen([{ action: 'scan', device: 'sorting-1', person: PEOPLE.sort, orderId: R.sort3 }, { action: 'scan', device: 'sorting-1', person: PEOPLE.sort, orderId: R.sort4 }]);
    });

    /* ── Design: design, design-1 (Pia, the name in the order chat), design-message (Rosa, kept), design-message-1 (Gus) with the Design scanners ── */
    for (const [file, rid] of [['design.html', R.des3], ['design-1.html', R.des4]]) {
      await check('A-' + file.replace('.html', '').toUpperCase().replace('DESIGN-1', 'DE1').replace('DESIGN', 'DE0'), `${file}: name set in the order chat, a chat message, a finished design with its label = one print and one completion`, ['SA4'], async () => {
        const dev = file.replace('.html', '');
        const page = await A.design(null, file, PEOPLE.designSmoke, rid);
        await A.designChat(page, 'hello from the design bench'); await A.designFinish(page, rid, 1); await A.flush(page);
        await seen([{ action: 'note', device: dev, person: PEOPLE.designSmoke }, { action: 'print', device: dev }, { action: 'complete', device: dev, orderId: rid, orders: 1 }]);
        const frames = page.__frames.filter(u => /design-print/.test(u)); assert(frames.length, 'the print frame (design-print) was never loaded: ' + page.__frames.slice(-4));
        await page.evaluate(() => StationSession.signedOut('signOut'));                    // (no Sign Out button on these pages)
        await endedAs(PEOPLE.designSmoke, dev, undefined, 20000);
        await W.closeContext(page.context());
      });
    }
    await check('A-DM1', 'design-message + design-scan (kept): PIN sign-in, a phone scan puts the order in hand, a chat message', ['SA4'], async () => {
      const page = S.pages.dmsg = await A.pin(null, 'design-message.html', PEOPLE.designMsg); keep.add(page);
      await scanned('design-scan.html', R.des1, page, R.des1);
      await sleep(1000); await A.say(page, 'please check the engraving on this one'); await A.flush(page);
      await seen([{ action: 'scan', device: 'design-message', person: PEOPLE.designMsg, orderId: R.des1 }, { action: 'note', device: 'design-message', person: PEOPLE.designMsg }]);
    });
    await check('A-DM2', 'design-message-1 + design-scan-1: PIN sign-in, phone scan, chat message, sign out', ['SA4'], async () => {
      const page = await A.pin(null, 'design-message-1.html', PEOPLE.designMsg2);
      await scanned('design-scan-1.html', R.des2, page, R.des2);
      await sleep(1000); await A.say(page, 'second bench note'); await A.flush(page);
      await seen([{ action: 'scan', device: 'design-message-1', person: PEOPLE.designMsg2, orderId: R.des2 }, { action: 'note', device: 'design-message-1', person: PEOPLE.designMsg2 }]);
      await A.pinSignOut(page); await signedOutCleanly(page, PEOPLE.designMsg2, 'design-message-1');
      await W.closeContext(page.context());
    });

    /* ── Inbox: Ines, three replies on two conversations (2 customers, 2 orders) and one unsent draft ── */
    await check('A-IN1', 'etsy-mail-1 (kept): operator sign-in, three replies on two conversations are three "reply sent" events and three entries in the day\'s record', ['IN1', 'IN3'], async () => {
      const page = S.pages.inbox = await A.inbox(null, 'ines'); keep.add(page);
      await page.waitForSelector(`[data-id="${THREADS.A}"]`, { timeout: 40000 });
      await A.inboxReply(page, THREADS.A, 'Hi Ingrid, your order ships tomorrow. Many thanks!');
      await sleep(1500);
      await A.inboxReply(page, THREADS.A, 'Hi Ingrid, one more thing: the clasp is lobster style.');
      await sleep(1500);
      await A.inboxReply(page, THREADS.B, 'Hi Ivan, your order is on its way. Many thanks!');
      await A.flush(page);
      await until(async () => (await events(e => e.device === 'etsy-mail-1' && e.person === PEOPLE.inbox && e.action === 'note' && /reply sent/.test(e.detail || ''))).length >= 3, 'three "reply sent" events', 25000);
      const day = (await W.list('EtsyMail_ReplyDaily'))[0]; assert(day && day.replies.length === 3, 'the day\'s record has ' + (day ? day.replies.length : 0) + ' replies, wanted 3');
      eq([...new Set(day.replies.map(r => r.r))].sort(), [R.inb1, R.inb2].sort(), 'orders in the day\'s record');
      assert(new Set(day.replies.map(r => r.c)).size === 2, 'customers in the day\'s record');
      await page.click(`[data-id="${THREADS.B}"]`); await page.waitForSelector('#emDraftText'); await page.fill('#emDraftText', 'Hi Ivan, also: the tracking link follows');   // an unsent draft
      S.last.inboxDraft = 'Hi Ivan, also: the tracking link follows';
    });

    /* ── the Sorter app: Laser (Lena), Design (Dara), Nico at Design; the Admin comes with the portal ── */
    await check('A-LA1', 'Sorter app as Laser: name, then "Laser or Design?" = Laser; a sheet in hand and a cut marked = events at the Laser station with the role', ['LD1', 'SA5'], async () => {
      const page = S.pages.laser = await A.sorterApp(null, { label: 'laser' }); keep.add(page);
      await A.sorterRole(page, PEOPLE.laserApp, 'laser');
      await page.evaluate(([rid]) => { CNAct('complete', { station: 'laser', each: [{ orderId: rid, line: 'GF Sheet 2', parts: 1 }], detail: 'GF Sheet 2 marked completed (laser)' }); CNLive.sheet('GF Sheet 3 · Set 1'); }, [R.las1]);
      await A.flush(page);
      const [e] = await seen([{ action: 'complete', device: 'charm-nest-1', person: PEOPLE.laserApp, station: 'laser', role: 'laser', orderId: R.las1 }]);
      const s = await openSession(PEOPLE.laserApp, 'charm-nest-1'); eq([s.station, s.role], ['laser', 'laser'], 'the Laser session');
    });
    await check('A-DA1', 'Sorter app as Design: name, then Design; an order approved = an event at the Design station with the role; a window open = the order in hand', ['LD1'], async () => {
      const page = S.pages.designApp = await A.sorterApp(null, { label: 'design' }); keep.add(page);
      await A.sorterRole(page, PEOPLE.designApp, 'design');
      await page.evaluate(([rid]) => { CNAct('complete', { orderId: rid, parts: 1, orders: 1, detail: 'custom design approved' }); CNLive.sheet('Design set 7'); }, [R.des5]);
      await A.flush(page);
      await seen([{ action: 'complete', device: 'charm-nest-1', person: PEOPLE.designApp, station: 'design', role: 'design', orderId: R.des5 }]);
      const s = await openSession(PEOPLE.designApp, 'charm-nest-1'); eq([s.station, s.role], ['design', 'design'], 'the Design session');
    });
    await check('A-MU1', 'one person at three stations in a day: Nico at Assembly (above), then Design in the Sorter app, then one tap to Laser', ['LD1', 'SA2', 'SA1'], async () => {
      const page = await A.sorterApp(null, { label: 'nico' });
      await A.sorterRole(page, PEOPLE.multi, 'design');
      await page.evaluate(([rid]) => { CNAct('complete', { orderId: rid, parts: 1, orders: 1, detail: 'custom design approved' }); }, [R.des6]);
      await A.flush(page);
      await seen([{ action: 'complete', device: 'charm-nest-1', person: PEOPLE.multi, station: 'design', role: 'design', orderId: R.des6 }]);
      await W.advance(4 * 60000, { step: 60000, between: async () => { await keep.poke(); await page.mouse.move(100 + Math.random() * 50, 100).catch(() => {}); } });
      // switches to Laser (one tap: the session at Design ends "switched", the Laser one starts), then signs out for the day
      await page.evaluate(() => { CNRole.switchTo('laser'); });
      await page.waitForFunction(() => StationSession.role() === 'laser', null, { timeout: 15000 });
      await page.evaluate(() => CNAct('complete', { station: 'laser', parts: 2, detail: 'Library laser check' }));
      await A.flush(page);
      await W.advance(3 * 60000, { step: 60000, between: async () => { await keep.poke(); await page.mouse.move(100 + Math.random() * 50, 100).catch(() => {}); } });
      await page.evaluate(() => { StationSession.signedOut('signOut'); try { localStorage.removeItem('cn.employee'); } catch (_) {} B.employee = ''; });
      await W.closeContext(page.context());
      const ss = await sessionsOf(s => s.person === PEOPLE.multi);
      eq(ss.map(s => s.station).sort(), ['assembly', 'design', 'laser'], 'Nico\'s sessions');
    });
  }

  /* ═════════════════════════════ PHASE B · the portal ═════════════════════════════ */
  let portal;
  const cardOf = key => { const c = (S.board || []).find(s => s.key === key); assert(c, 'the board has no ' + key + ' card (it has ' + (S.board || []).map(s => s.key).join(', ') + ')'); return c; };
  const names = c => c.people.map(p => p.name).sort();
  // the board needs a few seconds after the work: poll until it says what the shop's own records say (the readers cache for a moment, by design).
  // (while S.quiet is set nobody touches a station page: waiting must not count as input at any desk)
  const boardUntil = async (what, fn, ms) => { const t0 = Date.now(); let last; for (;;) { S.board = await P.board(); try { fn(); return; } catch (e) { last = e; } if (Date.now() - t0 > (ms || 40000)) throw last; await sleep(1500); if (!S.quiet) await keep.poke(); } };
  if (PHASES.includes('B')) {
    say('\nB · the Employee efficiency portal (Real view)');
    await W.advance(90000, { step: 30000, between: async () => { await keep.poke(); } });
    for (const k of Object.keys(S.pages)) { try { await A.flush(S.pages[k]); } catch (_) {} }
    portal = await P.open({ who: 'Paul K' }); S.pages.admin = portal;
    await check('B-AD1', 'Sorter app as the Admin (Paul K): not asked Laser or Design, a session at the Sorter ("Sorting" in the portal), a QR label print recorded', ['AD2', 'LD1'], async () => {
      await portal.waitForFunction(() => { const w = StationActivity.who(); return !!w; }, null, { timeout: 30000 });
      await portal.evaluate(([rid]) => { CNAct('print', { orderId: rid, parts: 1, detail: 'QR label' }); }, [R.sort1]);
      await A.flush(portal);
      assert(await portal.evaluate(() => !document.querySelector('.cnNameBar[data-kind="role"]') && StationSession.role() === ''), 'the Admin was asked Laser or Design (or got a role)');
      const s = await openSession('Paul K.', 'charm-nest-1'); eq([s.station, s.role || ''], ['sorter', ''], 'the Admin\'s session');
      await seen([{ action: 'print', device: 'charm-nest-1', person: 'Paul K.', station: 'sorter' }]);
    });
    await sleep(3000);
    S.board = null;
    await check('B-BD0', 'the portal is in the Real view and the Stations board lists the stations', ['PB1'], async () => {
      eq(await P.view(), 'real', 'the console view');
      S.board = await P.board(); assert(S.board.length >= 6, 'the board lists ' + S.board.length + ' stations');
    });
    await shot(portal, 'B1-board-crew');

    await check('B-BD1', 'the board has NO Sorter and NO QR Printer card (they are Sorting)', ['PB1'], async () => {
      const keys = S.board.map(s => s.key), nm = S.board.map(s => s.name);
      assert(!keys.some(k => /^(sorter|qr)/i.test(k)), 'a station card has the key ' + keys.join(','));
      assert(!nm.some(n => /sorter|qr\s*printer/i.test(n)), 'a station card is named ' + nm.join(' | '));
      assert(keys.includes('sorting'), 'there is no Sorting card');
      assert(!S.board.some(s => s.key !== 'sorting' && /QR Printer/i.test(s.text)), 'QR Printer is named on another card');
    });

    const WHO = {
      sorting: [PEOPLE.sort, 'Paul K.'], welding: [PEOPLE.matcher, PEOPLE.welder], assembly: [PEOPLE.asm], shipping: [PEOPLE.ship],
      design: [PEOPLE.designMsg, PEOPLE.designApp], laser: [PEOPLE.laserApp], inbox: [PEOPLE.inbox]
    };
    S.who = WHO;
    for (const [key, who] of Object.entries(WHO)) {
      await check('B-BD2-' + key, `${key} card: exactly ${who.join(' and ')} signed in, nobody else`, ['PB1', { sorting: 'SA1', welding: 'WS1', assembly: 'SA2', shipping: 'SA3', design: 'SA4', laser: 'SA5', inbox: 'IN2' }[key]].concat(key === 'design' || key === 'laser' ? ['LD2'] : []), async () => {
        await boardUntil(key, () => { const c = cardOf(key); eq(names(c), [...who].sort(), 'people on the ' + key + ' card'); assert(c.state !== 'offline', key + ' is shown offline with people signed in'); });
      });
    }
    // the order in hand
    const hasCard = (c, rid) => c.cards.some(x => x.rid === rid || x.text.includes(rid));
    await check('B-BD3-asm', 'assembly card: the current order is the one on the desk, with its person', ['SA2'], async () => {
      await boardUntil('assembly', () => { const c = cardOf('assembly'); assert(hasCard(c, R.asm5), 'no order card for ' + R.asm5 + ': ' + JSON.stringify(c.cards)); const k = c.cards.find(x => x.rid === R.asm5 || x.text.includes(R.asm5)); assert(k.text.includes(PEOPLE.asm), 'the card does not name ' + PEOPLE.asm + ': ' + k.text); });
    });
    await check('B-BD3-ship', 'shipping card: the current order is the one on the desk, with its person', ['SA3'], async () => {
      await boardUntil('shipping', () => { const c = cardOf('shipping'); assert(hasCard(c, R.ship4), 'no order card for ' + R.ship4 + ': ' + JSON.stringify(c.cards)); const k = c.cards.find(x => x.rid === R.ship4 || x.text.includes(R.ship4)); assert(k.text.includes(PEOPLE.ship), 'the card does not name ' + PEOPLE.ship); });
    });
    await check('B-BD3-sort', 'sorting card: the phone\'s batch of two orders is the order in hand of the sorter at the desk', ['SA1'], async () => {
      await boardUntil('sorting', () => { const c = cardOf('sorting'); assert(c.cards.some(x => /Batch of 2 orders/.test(x.text) && x.text.includes(PEOPLE.sort)), 'no batch card for ' + PEOPLE.sort + ': ' + JSON.stringify(c.cards)); });
    });
    await check('B-BD3-design', 'design card: the order the phone put in hand at design-message, and the Sorter app\'s Design window', ['SA4', 'LD2'], async () => {
      await boardUntil('design', () => { const c = cardOf('design'); assert(hasCard(c, R.des1) && c.cards.find(x => x.text.includes(R.des1)).text.includes(PEOPLE.designMsg), 'no card for ' + R.des1 + ' under ' + PEOPLE.designMsg + ': ' + JSON.stringify(c.cards)); assert(c.cards.some(x => /Design set 7/.test(x.text) && x.text.includes(PEOPLE.designApp)), 'no "Design set 7" card for ' + PEOPLE.designApp); });
    });
    await check('B-BD3-laser', 'laser card: the sheet in hand of the Laser person', ['SA5', 'LD2'], async () => {
      await boardUntil('laser', () => { const c = cardOf('laser'); assert(c.cards.some(x => /GF Sheet 3/.test(x.text) && x.text.includes(PEOPLE.laserApp)), 'no sheet card for ' + PEOPLE.laserApp + ': ' + JSON.stringify(c.cards)); });
    });
    await check('B-BD3-weld', 'welding card: the people, the task each is in, and the orders matched (credited to the person in Matching, never to the welder)', ['WS2', 'WS1'], async () => {
      await boardUntil('welding', () => {
        const c = cardOf('welding'); assert(c.weld && c.weld.visible, 'the Welding card has no Welding block (the two groups and the matched list): ' + c.text.slice(0, 160));
        const row = c.weld.rows.find(m => m.text.includes(R.weldA)); assert(row, 'no matched row for ' + R.weldA + ': ' + JSON.stringify(c.weld.rows.map(m => m.text)));
        assert(row.text.includes(PEOPLE.matcher), 'the matched row does not say ' + PEOPLE.matcher + ': ' + row.text);
        assert(!c.weld.rows.some(m => m.text.includes(PEOPLE.welder)), 'a matched order is credited to the welder: ' + JSON.stringify(c.weld.rows.map(m => m.text)));
        const task = Object.fromEntries(c.people.map(p => [p.name, p.task]));
        eq([task[PEOPLE.matcher], task[PEOPLE.welder]], ['matching', 'welding'], 'the task each person is in');
      }, 20000);
    });
    await check('B-BD4', 'Welding shows two groups (Welding and Matching, one person each) and NO order throughput (no pieces, no orders on the card; the Overview row shows matched and time on task instead)', ['WS2', 'WS1'], async () => {
      const c = cardOf('welding');
      const groups = c.groups.map(g => [g.group.toLowerCase(), g.people.sort().join()]).sort();
      eq(groups, [['matching', PEOPLE.matcher], ['welding', PEOPLE.welder]], 'the two groups');
      assert(!c.countsVisible, 'the Welding card draws order throughput: "' + c.counts + '"');
      assert(!/\b\d+\s*pieces?\b/i.test(c.weld ? c.weld.text : c.text) && !/\b\d+\s*orders?\s+today\b/i.test(c.weld ? c.weld.text : c.text), 'the Welding block talks about pieces or orders: ' + (c.weld ? c.weld.text : c.text).slice(0, 200));
      // the Overview's Welding row: the reader counts no pieces and no orders; the row draws "matched" and the time on task where the others draw pieces and orders
      const bw = ((await W.eff({ op: 'overview', trend: false })).business.stations || []).find(x => x.station === 'welding');
      assert(bw && !(bw.parts > 0) && !(bw.orders > 0), 'the reader counts Welding pieces or orders: ' + JSON.stringify(bw));
      const ov = (await P.overviewStations()).find(r => r.key === 'welding'); assert(ov, 'the Overview has no Welding row');
      assert(ov.weld && /matched/i.test(ov.partsLabel) && !/pieces?/i.test(ov.partsLabel), 'the Overview\'s Welding row counts "pieces": ' + JSON.stringify(ov));
      assert(!/orders?/i.test(ov.ordersLabel) && /^(\d+(\.\d+)?\s*(h|m|min)\b.*|—)$/i.test(ov.ordersText), 'the Overview\'s Welding row counts orders, not time on task: ' + JSON.stringify(ov));
      eq([ov.parts], [bw.matched || 0], 'the Overview\'s Welding "matched" vs the reader');
    });
    await check('B-BD5', 'Laser and Design are two separate cards: each person is on the card of the role they signed in under, nobody on both', ['LD2', 'LD1'], async () => {
      const L = names(cardOf('laser')), D = names(cardOf('design'));
      assert(L.includes(PEOPLE.laserApp) && !L.includes(PEOPLE.designApp) && !L.includes(PEOPLE.designMsg), 'Laser card: ' + L.join());
      assert(D.includes(PEOPLE.designApp) && D.includes(PEOPLE.designMsg) && !D.includes(PEOPLE.laserApp), 'Design card: ' + D.join());
      const dc = cardOf('design').text, lc = cardOf('laser').text;
      assert(/Sorter app \(Design\)/.test(dc) && !/Sorter app \(Laser\)/.test(dc), 'Design card does not say where Dara is: ' + dc.slice(0, 300));
      assert(/Sorter app \(Laser\)/.test(lc) && !/Sorter app \(Design\)/.test(lc), 'Laser card does not say where Lena is: ' + lc.slice(0, 300));
    });
    await P.tab('stations');   // (the checks above read the Overview too: the cards are on screen again for the pictures)
    await shot(portal, 'B2-board-after-checks');
    for (const key of Object.keys(WHO)) { try { const el = portal.locator(`#efficiencyView .es .esSt[data-key="${key}"]`); await shot(el, 'card-' + key + '-crew'); } catch (_) {} }

    /* ── the person views ── */
    await check('B-PV0', 'the Overview agrees with the shop\'s own records: People on now, and the stations\' pieces and orders', ['PB1'], async () => {
      const ov = await W.eff({ op: 'overview', trend: false }); assert(ov && ov.ok !== false, 'overview: ' + JSON.stringify(ov).slice(0, 200));
      const on = ov.people.filter(p => p.status === 'on').map(p => p.name).sort();
      const want = [...new Set(Object.values(WHO).flat())].sort();
      eq(on, want, 'people on now (the Overview)');
      S.ov = ov;
    });
    S.persons = {};
    // truth from the shop's own records for one person: pieces = what they completed, orders = distinct orders they touched, per station (the portal's own rules, read from the stored events)
    const truthOf = async person => {
      const evs = await events(e => e.person === person), per = {};
      for (const e of evs) {
        const st = FOLD(e.station); const o = per[st] || (per[st] = { parts: 0, orders: new Set(), scans: 0 });
        if (e.action === 'complete' && st !== 'welding') o.parts += Number(e.parts) || 0;
        if (e.action === 'scan' || e.action === 'matched') o.scans++;
        if (e.orderId && st !== 'welding' || (e.orderId && e.action === 'matched')) o.orders.add(e.orderId);
      }
      const ss = await sessionsOf(s => s.person === person), mins = {}, now = await W.now();
      const byStation = {};
      for (const s of ss) (byStation[FOLD(s.station)] = byStation[FOLD(s.station)] || []).push([s.startAt, s.endAt != null ? s.endAt : now]);
      for (const [st, spans] of Object.entries(byStation)) { spans.sort((a, b) => a[0] - b[0]); let tot = 0, cur = null; for (const [a, b] of spans) { if (!cur || a > cur[1]) { if (cur) tot += cur[1] - cur[0]; cur = [a, b]; } else cur[1] = Math.max(cur[1], b); } if (cur) tot += cur[1] - cur[0]; mins[st] = tot / 60000; }
      return { per, mins };
    };
    const FOLD = k => (k === 'sorter' || k === 'qr' ? 'sorting' : k);
    for (const [label, person] of [['asm', PEOPLE.asm], ['ship', PEOPLE.ship], ['sort', PEOPLE.sort], ['designMsg', PEOPLE.designMsg], ['nico', PEOPLE.multi], ['laser', PEOPLE.laserApp], ['designApp', PEOPLE.designApp], ['inbox', PEOPLE.inbox], ['matcher', PEOPLE.matcher]]) {
      await check('B-PV-' + label, `${person}: the person page shows hours and work per station, and they equal the Overview and the board`, ['PB1', 'LD2'].concat(label === 'matcher' ? ['WS2'] : []), async () => {
        const nameOnPage = person.replace(/ ([A-Z])$/, ' $1.');
        let pv = null, srv = null, ovRow = null, tr = null;
        const t0 = Date.now();
        for (;;) {                                                           // (the readers cache a few seconds: wait for the numbers to settle, then they must agree)
          srv = await W.eff({ op: 'person', name: person, range: 'day' }); tr = await truthOf(person);
          pv = await P.personPage(person, { settle: 3000 }); await P.back();
          ovRow = (S.ov = await W.eff({ op: 'overview', trend: false })).people.find(p => p.name === person);
          try {
            assert(srv && srv.ok !== false && Array.isArray(srv.stations), 'op person: ' + JSON.stringify(srv).slice(0, 160));
            for (const st of srv.stations) {
              const t = tr.per[st.station]; assert(t, 'the reader has a station ' + st.station + ' the shop\'s records do not (' + Object.keys(tr.per) + ')');
              if (st.station !== 'welding' && st.station !== 'inbox') { eq([st.parts, st.orders], [t.parts, t.orders.size], person + ' at ' + st.station + ' (pieces, orders) vs the stored events'); }
              assert(Math.abs(st.minutes - (tr.mins[st.station] || 0)) <= 1.0, person + ' at ' + st.station + ': ' + st.minutes + ' min on the page, ' + (tr.mins[st.station] || 0).toFixed(1) + ' from the sessions');
            }
            assert(ovRow, 'the Overview has no row for ' + person);
            // (a person with no throughput at all, a Welding-only person, has a dash on the page and 0 in the Overview: the same "nothing")
            eq([ovRow.totals.parts, ovRow.totals.orders], [mv(srv.kpis.parts) == null ? 0 : mv(srv.kpis.parts), mv(srv.kpis.orders) == null ? 0 : mv(srv.kpis.orders)], person + ': Overview row vs the person page (pieces, orders)');
            for (const st of srv.stations) { const o = ovRow.stations.find(x => x.station === st.station); assert(o, 'the Overview row has no ' + st.station); eq([o.parts, o.orders], [st.parts, st.orders], person + ' at ' + st.station + ': Overview row vs person (pieces, orders)'); assert(Math.abs(o.minutes - st.minutes) <= 0.6, person + ' at ' + st.station + ': Overview ' + o.minutes + ' min vs person ' + st.minutes); }
            // on screen
            assert(pv.name === person || pv.name === nameOnPage, 'the page names ' + pv.name);
            for (const [k, what] of [['parts', '"Pieces finished"'], ['orders', '"Orders worked"']]) {
              const on = String(pv.kpis['kpis.' + k]).replace(/[^\d.]/g, ''), want = mv(srv.kpis[k]);
              assert(want == null ? (on === '' || on === '0') : on === String(want), person + ': ' + what + ' on screen is "' + pv.kpis['kpis.' + k] + '", the reader says ' + want);
            }
            for (const st of srv.stations) assert(new RegExp(st.label + '\\s*[\\d.]+\\s*(min|m|h)').test(pv.chips) || pv.chips.includes(st.label), person + ': the page shows no hours chip for ' + st.label + ': "' + pv.chips + '"');
            break;
          } catch (e) { if (Date.now() - t0 > 60000) throw e; await sleep(2500); await keep.poke(); }
        }
        S.persons[person] = { srv, pv, ovRow, tr };
      });
    }
    await check('B-PV-board', 'the board\'s counts per station equal the sum of what the people there did (Overview Stations card = board card = people)', ['PB1', 'LD2'], async () => {
      await boardUntil('counts', () => {}, 1000);
      const ov = await W.eff({ op: 'overview', trend: false }), rows = await P.overviewStations();
      for (const st of ['assembly', 'shipping', 'sorting', 'design', 'laser']) {
        const sum = ov.people.reduce((a, p) => { const o = p.stations.find(x => x.station === st); return o ? [a[0] + o.parts, a[1] + o.orders] : a; }, [0, 0]);
        const b = ov.business.stations.find(x => x.station === st); assert(b, 'business has no ' + st);
        eq([b.parts], [sum[0]], st + ': station pieces vs the people\'s');
        const row = rows.find(r => r.key === st); assert(row, 'the Overview card has no ' + st + ' row'); eq([row.parts, row.orders], [b.parts, b.orders], st + ': the Overview card row vs the reader');
        const card = cardOf(st); const m = /(\d+)\s*pieces?\s*(\d+)\s*orders?/i.exec(card.counts); assert(m, st + ' card counts: "' + card.counts + '"'); eq([+m[1], +m[2]], [b.parts, b.orders], st + ': the board card vs the Overview');
      }
    });
    await check('B-PV-task', 'per task and per role: Welding hours split Welding | Matching with the matched count; Laser and Design are their own stations in a person\'s page', ['WS2', 'LD2'], async () => {
      const m = await W.eff({ op: 'person', name: PEOPLE.matcher, range: 'day' });
      assert(m.welding && m.welding.hours && m.welding.hours.matching > 0, 'no Matching time in the person view of ' + PEOPLE.matcher + ': ' + JSON.stringify(m.welding).slice(0, 200));
      assert(m.welding.matched >= 1, 'no matched scans for ' + PEOPLE.matcher);
      const w = await W.eff({ op: 'person', name: PEOPLE.welder, range: 'day' });
      assert(w.welding && w.welding.hours.welding > 0 && !(w.welding.hours.matching > 0), 'the welder has Matching time or none at Welding: ' + JSON.stringify(w.welding && w.welding.hours));
      // on screen (WS2's "Welding station" figures): Matching hours and Orders matched equal the reader's, Welding hours are the welder's
      const dig = s => String(s).replace(/[^\d.]/g, ''), mins = t => { const s = String(t), h = /(\d+(?:\.\d+)?)\s*h/i.exec(s), mm = /(\d+(?:\.\d+)?)\s*m(?:in)?\b/i.exec(s); return h || mm ? (h ? parseFloat(h[1]) * 60 : 0) + (mm ? parseFloat(mm[1]) : 0) : null; };
      const pv = await P.personPage(PEOPLE.matcher, { settle: 3000, range: 'day' }); await P.back();
      assert(/matching/i.test(pv.text) && /matched/i.test(pv.text), 'the page of ' + PEOPLE.matcher + ' does not show Matching hours or the matched count');
      assert(pv.kpis['welding.matchedOrders'] != null && pv.kpis['welding.matchingHours'] != null, 'no "Orders matched" / "Matching hours" figure on the page of ' + PEOPLE.matcher + ' (figures: ' + Object.keys(pv.kpis).join(',') + ')');
      eq(dig(pv.kpis['welding.matchedOrders']), String(m.welding.matched), 'Orders matched on the page of ' + PEOPLE.matcher);
      const shownMin = mins(pv.kpis['welding.matchingHours']); assert(shownMin != null && Math.abs(shownMin - m.welding.hours.matching * 60) <= 1.5, 'Matching hours on the page "' + pv.kpis['welding.matchingHours'] + '" vs the reader ' + (m.welding.hours.matching * 60).toFixed(1) + ' min');
      const pw = await P.personPage(PEOPLE.welder, { settle: 3000, range: 'day' }); await P.back();
      const wMin = mins(pw.kpis['welding.weldingHours']); assert(wMin != null && Math.abs(wMin - w.welding.hours.welding * 60) <= 1.5, 'Welding hours on the page of ' + PEOPLE.welder + ' "' + pw.kpis['welding.weldingHours'] + '" vs the reader ' + (w.welding.hours.welding * 60).toFixed(1) + ' min');
      assert(!(mins(pw.kpis['welding.matchingHours']) > 0), PEOPLE.welder + ' shows Matching hours: "' + pw.kpis['welding.matchingHours'] + '"');
      const n = await W.eff({ op: 'person', name: PEOPLE.multi, range: 'day' });
      eq(n.stations.map(s => s.station).sort(), ['assembly', 'design', 'laser'], 'the stations in Nico\'s page (Design and Laser are separate)');
    });

    /* ── the Inbox ── */
    await check('B-IN1', 'Inbox (server): Ines\'s day = 3 replies, 2 orders, 2 customers, 3 messages; the day\'s team total and the live block say the same', ['IN1'], async () => {
      const r = await W.eff({ op: 'personInbox', name: PEOPLE.inbox, range: 'day' });
      assert(r.ok, 'personInbox: ' + JSON.stringify(r).slice(0, 200));
      const v = k => (r.totals[k] && typeof r.totals[k] === 'object' ? r.totals[k].value : r.totals[k]);
      eq([v('replies'), v('orders'), v('customers'), v('messages')], [3, 2, 2, 3], 'Ines\'s replies, orders, customers, messages');
      const live = (await W.eff({ op: 'live' })).stations.find(s => s.key === 'inbox'); assert(live && live.inbox, 'the live inbox block is missing');
      eq([live.inbox.replies, live.inbox.orders, live.inbox.customers], [3, 2, 2], 'the board\'s inbox block');
      const bp = live.inbox.byPerson.find(p => p.name === PEOPLE.inbox); assert(bp && bp.replies === 3, 'byPerson has no Ines with 3 replies: ' + JSON.stringify(live.inbox.byPerson));
    });
    await check('B-IN2', 'Inbox (screen): the board\'s Inbox card and the person page\'s Inbox section show 3 replies, 2 orders, 2 customers', ['IN2', 'IN1'], async () => {
      await boardUntil('inbox', () => {
        const c = cardOf('inbox');
        assert(/\b3\s*repl/i.test(c.text) && /\b2\s*orders?\b/i.test(c.text), 'the Inbox card does not say 3 replies and 2 orders: ' + c.text.slice(0, 220));
        assert(/\b2\s*customers?\b/i.test(c.text) && /\b3\s*messages?\b/i.test(c.text), 'the Inbox card does not say 2 customers and 3 messages: ' + c.text.slice(0, 220));
        assert(c.people.some(p => p.name === PEOPLE.inbox), 'the Inbox card does not name ' + PEOPLE.inbox);
      }, 20000);
      const pv = await P.personPage(PEOPLE.inbox, { settle: 3500, range: 'day' }); await P.back();
      const dig = s => String(s == null ? '' : s).replace(/[^\d.]/g, '');
      assert(/Inbox/.test(pv.text), 'the person page of ' + PEOPLE.inbox + ' has no Inbox section');
      eq([dig(pv.kpis['inbox.replies']), dig(pv.kpis['inbox.orders']), dig(pv.kpis['inbox.customers'])], ['3', '2', '2'], 'the Inbox figures on the page of ' + PEOPLE.inbox + ' (replies sent, orders covered, customers)');
    });
    // the Inbox card with its replies block (it draws a moment after the first pictures were taken): its picture is taken again once the check has seen it
    if (landed('IN2')) { try { await P.tab('stations'); await portal.waitForSelector('#efficiencyView .es .esSt[data-key="inbox"] .esIn', { timeout: 10000 }); await shot(portal.locator('#efficiencyView .es .esSt[data-key="inbox"]'), 'card-inbox-crew'); } catch (_) {} }
    await shot(portal, 'B3-board-final');
  }

  /* ═════════════════════════════ PHASE C · the clock ═════════════════════════════ */
  if (PHASES.includes('C')) {
    say('\nC · the clock: ten minutes without input, then 17:00 Toronto');
    const crew = [
      ['weld', PEOPLE.matcher, 'weld-1', 'matching'], ['weld', PEOPLE.welder, 'weld-1', 'welding'], ['asm2', PEOPLE.asm, 'assembly-2'], ['ship1', PEOPLE.ship, 'shipping-1'],
      ['sort', PEOPLE.sort, 'sorting-1'], ['dmsg', PEOPLE.designMsg, 'design-message'], ['laser', PEOPLE.laserApp, 'charm-nest-1'], ['designApp', PEOPLE.designApp, 'charm-nest-1'], ['inbox', PEOPLE.inbox, 'etsy-mail-1']
    ].map(([k, person, device, task]) => ({ k, person, device, task }));
    const pageKey = { dmsg: 'dmsg' };
    // what is on every screen right now (what must still be there after the sign-outs)
    const screenOf = async () => {
      const o = {};
      for (const [k, sel] of [['asm2', '#etsyOrderNumber'], ['ship1', '#etsyOrderNumber'], ['weld', '#etsyOrderNumber'], ['dmsg', '#etsyOrderNumber']]) { const p = S.pages[k]; if (p && !p.isClosed()) o[k] = await p.evaluate(s => ({ order: (document.querySelector(s) || {}).value || '', msg: (document.getElementById('britesMsgInput') || {}).value || '' }), sel).catch(() => null); }
      if (S.pages.sort && !S.pages.sort.isClosed()) o.sort = await S.pages.sort.evaluate(() => ({ items: (window.cachedOrderItems || []).length, order: document.getElementById('etsyOrderNumber').value })).catch(() => null);
      if (S.pages.inbox && !S.pages.inbox.isClosed()) o.inbox = await S.pages.inbox.evaluate(() => ({ draft: (document.getElementById('emDraftText') || {}).value || '' })).catch(() => null);
      return o;
    };
    const before = {};
    keep.stop();
    await keep.poke();
    S.quiet = true;                                                          // (from here on nothing waits by touching a station page)
    for (const c of crew) { const p = c.k === 'laser' || c.k === 'designApp' ? S.pages[c.k] : S.pages[c.k]; if (p && !p.isClosed()) before[c.person + '@' + c.device] = await lastInputOf(p); }
    const screenBefore = await screenOf();
    const quietFrom = await W.now();
    await shot(portal, 'C0-board-before-quiet');
    // eleven quiet minutes: no input anywhere (the portal's own polling is not input at a station; the Admin's Sorter page is the portal and stays untouched)
    await W.advance(11 * 60000, { step: 60000, settle: 200 });
    await sleep(4000);
    await W.advance(60000, { step: 30000, settle: 300 });
    const nonAdmin = crew.filter(c => true);
    await check('C-IDLE1', 'ten minutes without input: every person who is not an Admin is signed out at every station (sessions end "idle")', ['AD1', 'AD2'], async () => {
      const bad = [];
      for (const c of nonAdmin) {
        const s = await sessionFor(c.person, c.device, c.task === undefined ? undefined : c.task);
        if (!s || s.endAt == null) bad.push(c.person + '@' + c.device + ' still signed in'); else if (s.endReason !== 'idle') bad.push(c.person + '@' + c.device + ' ended "' + s.endReason + '"');
      }
      assert(!bad.length, bad.join(' ; '));
    });
    await check('C-IDLE2', 'the Admin (Paul K at the Sorter app) is NOT signed out, while the others are (the same quiet)', ['AD1', 'AD2'], async () => {
      let gone = 0; for (const c of nonAdmin) { const s = await sessionFor(c.person, c.device, c.task); if (s && s.endAt != null) gone++; }
      assert(gone > 0, 'nobody else was signed out in that quiet, so the Admin staying in proves nothing');
      const s = await openSession('Paul K.', 'charm-nest-1'); assert(s.endAt == null, 'the Admin session ended');
      assert(await portal.evaluate(() => !!StationActivity.who()), 'the Admin is signed out in the page');
    });
    await check('C-IDLE3', 'hours end at the LAST INPUT: each session\'s end is the time of that person\'s last input at the page (not the time the timer fired)', ['AD1', 'AD2'], async () => {
      const bad = [];
      for (const c of nonAdmin) {
        const s = await sessionFor(c.person, c.device, c.task); const li = before[c.person + '@' + c.device];
        if (!s || s.endAt == null || !li) { bad.push(c.person + ': no end'); continue; }
        if (Math.abs(s.endAt - li) > 20000 && !(s.endAt > li && s.endAt - li < 12000)) bad.push(c.person + '@' + c.device + ': ended ' + Math.round((s.endAt - li) / 1000) + ' s after the last input');
        if (s.endAt > quietFrom + 30000) bad.push(c.person + ': ended after the quiet began');
      }
      assert(!bad.length, bad.join(' ; '));
    });
    await check('C-IDLE4', 'the portal shows the sign-out plainly: nobody but the Admin on the board, "Signed out after 10 minutes without input" on the person\'s page', ['AD2', 'AD1'], async () => {
      await boardUntil('after quiet', () => { for (const [k, c] of Object.entries(Object.fromEntries(S.board.map(s => [s.key, s])))) { const left = c.people.map(p => p.name).filter(n => n !== 'Paul K.'); assert(!left.length, k + ' still lists ' + left.join()); } }, 40000);
      await shot(portal, 'C1-board-after-ten-quiet-minutes');
      const pv = await P.personPage(PEOPLE.asm, { settle: 3500, range: 'day' }); await P.back();
      assert(/Signed out after 10 minutes without input/.test(pv.text), 'the person page of ' + PEOPLE.asm + ' does not say why they are out: "' + pv.where + '"');
    });
    const loginShown = async k => {
      const p = S.pages[k]; if (!p || p.isClosed()) return false;
      if (k === 'sort') return p.evaluate(() => /Set your name/i.test((document.querySelector('#sortingAsChip .st-as-name') || {}).textContent || ''));
      if (k === 'inbox') return p.evaluate(() => !document.body.classList.contains('authed'));
      if (k === 'laser' || k === 'designApp') return p.evaluate(() => !CNEmployee.name() && StationSession.role() === '');
      if (k === 'weld') return p.evaluate(() => weldRoster().length === 0 && !!document.getElementById('employeeLoginBtn') && document.getElementById('employeeLoginBtn').offsetParent !== null);
      return p.evaluate(() => window.isEmployeeLoggedIn === false && !!document.getElementById('employeeLoginBtn') && document.getElementById('employeeLoginBtn').offsetParent !== null);
    };
    await check('C-LOST1', 'nothing lost after the idle sign-out: each page shows its own sign-in again, and the orders in the boxes, the sorting batch, the typed notes and the draft are still on screen', ['SA1', 'SA2', 'SA3', 'SA4', 'IN3', 'WS1', 'AD1'], async () => {
      const bad = [], out = [];
      for (const k of ['weld', 'asm2', 'ship1', 'sort', 'dmsg', 'laser', 'designApp', 'inbox']) if (!(await loginShown(k))) bad.push(k + ': no sign-in shown (still signed in, or the page lost its state)'); else out.push(k);
      assert(out.length, 'nobody was signed out, so there is nothing to compare: ' + bad.join(' ; '));
      const now = await screenOf();
      for (const [k, v] of Object.entries(screenBefore)) { const n = now[k]; if (!n) { bad.push(k + ' page gone'); continue; } for (const f of Object.keys(v)) if (v[f] && String(n[f]) !== String(v[f])) bad.push(k + '.' + f + ' was "' + v[f] + '" now "' + n[f] + '"'); }
      assert((screenBefore.asm2 || {}).msg === S.last.asmDraft && (screenBefore.inbox || {}).draft === S.last.inboxDraft, 'the typed note and draft were not on screen before the quiet');
      assert(!bad.length, bad.join(' ; '));
    });

    /* ── 17:00 Toronto: the same four desks signed in again in the late afternoon; two keep working, two computers sleep through five ── */
    if (!landed('AD1') && !process.env.FORCE_C2) {
      pending('C-FIVE1', 'at 17:00 Toronto the ones with input in the last ten minutes stay (then go idle ten minutes after their last input)', ['AD1'], 'runs when AD1\'s timers are on main (or FORCE_C2=1)');
      pending('C-FIVE2', 'a computer that slept through 17:00 with no recent input is signed out when it wakes: hours end at its last input, the portal says so plainly, the draft is kept', ['AD1'], 'runs when AD1\'s timers are on main (or FORCE_C2=1)');
      pending('C-FIVE3', 'the Admin is still signed in after 17:00 and after more quiet', ['AD1'], 'runs when AD1\'s timers are on main (or FORCE_C2=1)');
    } else {
      await W.jump(TOR(16, 20) - await W.now()); await sleep(3000);
      const desks = [['asm2', PEOPLE.asm, 'assembly-2'], ['ship1', PEOPLE.ship, 'shipping-1'], ['sort', PEOPLE.sort, 'sorting-1'], ['inbox', PEOPLE.inbox, 'etsy-mail-1']];
      await check('C-FIVE0', 'late afternoon: the four desks sign in again (PIN, name or inbox sign-in), the inbox draft is back', ['AD1', 'IN3'], async () => {
        await A.pinSignIn(S.pages.asm2, PEOPLE.asm); await A.pinSignIn(S.pages.ship1, PEOPLE.ship); await A.sortingSignIn(S.pages.sort, PEOPLE.sort); await A.inboxSignIn(S.pages.inbox, 'ines');
        for (const [, person, dev] of desks) await openSession(person, dev);
        assert((await S.pages.inbox.evaluate(() => (document.getElementById('emDraftText') || {}).value)) === S.last.inboxDraft, 'the inbox draft did not come back at the next sign-in');
      });
      const poke = p => p.mouse.move(70 + Math.random() * 300, 150 + Math.random() * 200).then(() => p.mouse.move(90 + Math.random() * 300, 160 + Math.random() * 200)).catch(() => {});
      const asleep = [S.pages.sort.context(), S.pages.inbox.context()];
      S.last.sleepInput = { [PEOPLE.sort]: await lastInputOf(S.pages.sort), [PEOPLE.inbox]: await lastInputOf(S.pages.inbox) };
      const sleptDraft = await S.pages.inbox.evaluate(() => { const t = document.getElementById('emDraftText'); if (t) { t.value = t.value + ' (typed just before five)'; t.dispatchEvent(new Event('input', { bubbles: true })); return t.value; } return ''; });
      S.last.ivyInput = 0;
      const stepTo = async (h, m) => {
        for (;;) { const n = await W.now(); if (n >= TOR(h, m)) return; await W.advance(60000, { step: 60000, except: asleep, settle: 150, between: async () => { const t = await W.now(); if (t <= TOR(17, 5)) await poke(S.pages.asm2); if (t <= TOR(16, 55)) { await poke(S.pages.ship1); S.last.ivyInput = await lastInputOf(S.pages.ship1); } } }); }
      };
      await stepTo(17, 0); await W.advance(30000, { step: 30000, except: asleep, settle: 150 });
      const at500 = { asm: await sessionFor(PEOPLE.asm, 'assembly-2'), ship: await sessionFor(PEOPLE.ship, 'shipping-1') };
      // the two sleeping computers wake at 17:01 by the shop's clock: no input since 16:20
      await stepTo(17, 1);
      await W.syncClock(asleep[0]); await W.syncClock(asleep[1]); asleep.length = 0; await sleep(5000);
      await W.advance(40000, { step: 20000, settle: 200 });
      await check('C-FIVE2', 'a computer that slept through 17:00 with no recent input is signed out when it wakes (reason "closing", or "idle" when its first beat reaches the shop first: both are 10+ minutes of no input); hours end at its last input; the portal says so plainly; the draft stays', ['AD1', 'AD2', 'IN3'], async () => {
        const WORDS = { idle: /Signed out after 10 minutes without input/, closing: /Signed out at 5:00 pm/ };
        for (const [k, person, dev] of [['sort', PEOPLE.sort, 'sorting-1'], ['inbox', PEOPLE.inbox, 'etsy-mail-1']]) {
          const s = await endedAs(person, dev, undefined, 25000);
          assert(['idle', 'closing'].includes(s.endReason), person + ' (no input since 16:20, woke at 17:01) ended "' + s.endReason + '"');
          assert(Math.abs(s.endAt - S.last.sleepInput[person]) < 20000, person + '\'s hours end ' + Math.round((s.endAt - S.last.sleepInput[person]) / 1000) + ' s from the last input');
          assert(await loginShown(k), person + '\'s page shows no sign-in');
          S.last['why_' + k] = s.endReason;
        }
        assert((await S.pages.inbox.evaluate(() => (document.getElementById('emDraftText') || {}).value)) === sleptDraft, 'the inbox draft changed across the sign-out');
        const pv = await P.personPage(PEOPLE.sort, { settle: 3500, range: 'day' }); await P.back();
        assert(WORDS[S.last.why_sort].test(pv.text), 'the person page of ' + PEOPLE.sort + ' (ended "' + S.last.why_sort + '") does not say why: "' + pv.where + '"');
        say('           (the reasons written: Sorting "' + S.last.why_sort + '", Inbox "' + S.last.why_inbox + '")');
        await shot(portal, 'C2-board-after-five');
      });
      await check('C-FIVE1', 'at 17:00 Toronto the ones with input in the last ten minutes stay signed in; ten minutes after their last input they go idle', ['AD1', 'AD2'], async () => {
        assert(at500.asm.endAt == null && at500.ship.endAt == null, 'signed out at 17:00 with recent input: ' + JSON.stringify([at500.asm.endReason, at500.ship.endReason]));
        await stepTo(17, 7);
        const ship = await endedAs(PEOPLE.ship, 'shipping-1', undefined, 25000); eq(ship.endReason, 'idle', 'Ivy (last input 16:55) ended');
        assert(Math.abs(ship.endAt - S.last.ivyInput) < 20000, 'Ivy\'s hours end ' + Math.round((ship.endAt - S.last.ivyInput) / 1000) + ' s from her last input');
        assert((await sessionFor(PEOPLE.asm, 'assembly-2')).endAt == null, 'Michael (input at 17:05) is already out at 17:07');
      });
      await check('C-FIVE3', 'the Admin is still signed in after 17:00 and more quiet; Michael goes idle ten minutes after his last input (17:15), not at five', ['AD1', 'AD2'], async () => {
        await stepTo(17, 17);
        const m = await endedAs(PEOPLE.asm, 'assembly-2', undefined, 25000); eq(m.endReason, 'idle', 'Michael ended');
        assert(m.endAt > TOR(17, 3) && m.endAt < TOR(17, 8), 'Michael\'s hours end at ' + hhmm(m.endAt) + ', his last input was about 17:05');
        const adm = await sessionFor('Paul K.', 'charm-nest-1'); assert(adm && adm.endAt == null || (await portal.evaluate(() => !!StationActivity.who())), 'the Admin was signed out');
      });
    }
  }

  /* ═════════════════════════════ the whole run: nothing outside, nothing paid, no PIN ═════════════════════════════ */
  say('\nZ · the whole run');
  const stats = await W.stats();
  await check('Z-NET', 'zero requests outside the fake shop (every other host was refused and written down), zero Etsy calls', [], async () => {
    assert(!W.outside.length, 'refused requests: ' + JSON.stringify(W.outside.slice(0, 6)));
    assert(!stats.egress.length, 'the shop reached out: ' + JSON.stringify(stats.egress.slice(0, 4)));
    assert(![...W.outside, ...stats.egress].some(x => /etsy/i.test(JSON.stringify(x))), 'a request went to Etsy');
  });
  await check('Z-PAID', 'zero paid calls: no model, no Etsy API, no AI draft ever ran (a paid-looking call is answered by the fake "AI is off" and counted)', [], async () => {
    assert(stats.paid === 0, stats.paid + ' paid calls reached real code: ' + JSON.stringify(stats.paidNames));
    assert(!Object.keys(stats.unfaked).length, 'functions nobody faked: ' + JSON.stringify(stats.unfaked));
    if (stats.paidTried) say('           (' + stats.paidTried + ' paid-looking call(s) were tried by the pages and answered by the fake, none bought anything: ' + JSON.stringify(stats.paidNames) + ')');
  });
  await check('Z-PIN', 'no PIN in any request, response or stored record (the roster itself apart)', [], async () => {
    assert(stats.pinLeaks === 0, stats.pinLeaks + ' leaks counted by the shop');
    for (const coll of ['Station_Sessions', 'Station_Activity', 'Station_Live', 'Efficiency_Daily', 'EtsyMail_ReplyDaily']) { const txt = JSON.stringify(await W.list(coll)); assert(!hasPin(txt), 'a PIN is stored in ' + coll); }
  });
  await check('Z-ERR', 'no page error on any page', [], async () => {
    const errs = W.errors.filter(e => !/Failed to fetch|Load failed|ResizeObserver/.test(e)); assert(!errs.length, errs.slice(0, 4).join(' | '));
  });

  await browser.close(); W.stop();
  // the inbox's own sign-out suite runs against the real station-session.js once AD1's timers are on main (it opens its own browser, so ours is closed first)
  if (PHASES.includes('C')) {
    await check('C-IN3', 'the inbox suite (IN3: idle, 5 pm, Admin, input, a reply in flight, another person) passes against the REAL station-session.js: SS=real node tests/etsy-mail/inbox-signout-rules.cjs', ['IN3', 'AD1'], async () => {
      assert(landed('AD1'), 'AD1 timers are not on main yet (SS=real needs them)');
      const env = Object.assign({}, process.env, { SS: 'real' });
      // (PW_DIR = the node_modules folder that holds playwright-core; CHROMIUM = the browser this test uses too)
      if (!env.PW_DIR) env.PW_DIR = [pwDir, path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => d && fs.existsSync(path.join(d, 'playwright-core'))) || '';
      if (!env.CHROMIUM && CHROME) env.CHROMIUM = CHROME;
      const r = spawnSync(process.execPath, [path.join(root, 'tests/etsy-mail/inbox-signout-rules.cjs')], { env, encoding: 'utf8', timeout: 900000 });
      const out = (r.stdout || '') + (r.stderr || ''); const fails = out.split('\n').filter(l => /^\s*FAIL/.test(l));
      assert(r.status === 0 && !fails.length, 'inbox-signout-rules (SS=real) exit ' + r.status + (fails.length ? ': ' + fails.slice(0, 4).join(' | ') : ': ' + out.slice(-300)));
    });
  }
  say(table());
  fs.writeFileSync(path.join(SHOTS, 'st1-results.json'), JSON.stringify({ at: new Date().toISOString(), seconds: Math.round((Date.now() - t00) / 1000), landed: landedAll(), results }, null, 1));
  const failed = results.filter(r => r.status === 'FAILED').length, pend = results.filter(r => r.status === 'PENDING').length;
  process.exit(failed || (process.env.STRICT && pend) ? 1 : 0);
})().catch(e => { console.error('THE TEST ITSELF BROKE:', e && e.stack || e); process.exit(2); });
