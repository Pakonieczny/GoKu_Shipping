// A Laser person who was signed out for an hour of quiet and signs in AGAIN (stations round 2, LS2: ST3's open question).
//
//   The Sorter app (charm-nest-1.html, the REAL page with the REAL station-session.js) over the fake shop of tests/stations/all-stations (the REAL
//   firebaseOrders door and the in-memory Firestore), in real Chromium with Playwright's clock:
//     09:00  Lena signs in as Laser, works (input), goes quiet; 50 minutes later she is still in; after 60 minutes the page signs her out ("idle", the
//            hours end at the last input);
//     later  she signs in AGAIN (name, then "Laser or Design?" = Laser) and keeps the page open for 45 minutes with clock ticks and ONE input
//            (like the end-to-end test: 25 minutes after the sign-in).
//   Every request the page sends to the session door is written down (start, beat, end, with the page's clock and the door's answer); the stored session
//   is read after every step. Expected, whatever the way the clock is stepped: a beat about every five minutes from the NEW session, the new session
//   stays open (the limit is an hour before 17:00 and 30 minutes from 17:00 after the last input), no "closing" at the moment of the sign-in.
//
//   RESULT (LS2): the gap ST3 saw is a TEST ARTIFACT, not a product defect. The Sorter app loads charm-nest-clock.js, which runs every timer of the page in a
//   Web Worker; Playwright's fake clock does not drive a Worker, so the page's 30-second tick (the one that beats) ran on real seconds, not on the stepped clock.
//   With the real worker kept (KEEP_CLOCK_WORKER=1) this test fails exactly like the end-to-end: 0 beats in 45 minutes, the door's last beat stays at the
//   sign-in, with the browser clock less than a second behind the shop's (so it is not clock lag). With the clock worker refused in the test world
//   (world.cjs, the default) the page beats every 5 minutes and the session stays open (the first run: 8 beats, gaps 5 to 5.5 minutes, the browser's clock
//   ended 2 minutes behind the shop's on a loaded machine and nothing went wrong).
//   node tests/stations/laser-resignin.cjs             (PW_DIR=... CHROMIUM=... ONLY=steps,jump5 SIGNIN=16:20,10:20 VERBOSE=1 KEEP_CLOCK_WORKER=1)
//   Ways of stepping the clock (all of them keep the page clock and the shop clock together, as the end-to-end test's World does):
//     steps      60-second steps of 8-second pieces (World.advance: the end-to-end test's own way between 16:20 and 17:00)
//     jumpN      a jump of N minutes at a time (World.jump: fastForward, every timer fires at most once, as a lid opened after N minutes)
//     lag200     60-second steps with the browser's clock 200 seconds behind the shop's from the second sign-in on (the lag the end-to-end test measured on its portal)
//     lagGrows   60-second steps and the browser's clock falls 5 seconds further behind every minute
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || '';
const pwCore = (() => { for (const d of [pwDir, path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules']) { if (!d) continue; try { return require(path.join(d, 'playwright-core')); } catch (_) {} } return require('playwright-core'); })();
const CHROME = process.env.CHROMIUM || (() => { try { const d = '/opt/pw-browsers'; const c = fs.readdirSync(d).filter(x => /^chromium-/.test(x)).sort().pop(); return c ? path.join(d, c, 'chrome-linux/chrome') : undefined; } catch (_) { return undefined; } })();
const { World, sleep } = require('./all-stations/world.cjs');
const { Apps } = require('./all-stations/apps.cjs');
const { makeCast } = require('./all-stations/cast.cjs');

const MIN = 60000;
const TOR = (h, m) => Date.parse('2026-10-07T00:00:00Z') + 4 * 3600e3 + (h * 60 + m) * MIN;        // Toronto wall time on the test day (EDT, UTC-4)
const hhmm = ms => new Intl.DateTimeFormat('en-GB', { timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' }).format(new Date(ms));
const say = s => process.stdout.write(s + '\n');
const LENA = 'Lena G.';
const pick = (process.env.ONLY || '').split(',').map(s => s.trim()).filter(Boolean);
const signins = (process.env.SIGNIN || '16:20,10:20').split(',').map(s => s.trim()).filter(Boolean).map(s => s.split(':').map(Number));
const WAYS = ['steps', 'jump1', 'jump5', 'jump10', 'jump20', 'lag200', 'lagGrows'].filter(w => !pick.length || pick.includes(w));

let browser;
const failures = [];

async function scenario(way, at) {
  const label = way + ' @' + String(at[0]).padStart(2, '0') + ':' + String(at[1]).padStart(2, '0');
  const W = await World.start({ now: new Date(TOR(9, 0)).toISOString(), browser });
  const A = Apps(W, makeCast());
  const out = { label, problems: [], sent: [], answers: [], server: [] };
  const bad = m => out.problems.push(m);
  try {
    const ctx = await W.context({ label: 'laser' });
    // everything the page sends to the session door, with the page's own clock; and what the door answered
    ctx.on('request', rq => { try { if (!/firebaseOrders/.test(rq.url()) || rq.method() !== 'POST') return; const b = JSON.parse(rq.postData() || '{}'); if (b.session) out.sent.push({ ev: b.session.event, id: b.session.id, st: b.session.station, why: b.session.reason, sentAt: b.session.sentAt, li: b.session.lastInputAt, at: b.session.at }); } catch (_) {} });
    ctx.on('response', async rs => { try { const rq = rs.request(); if (!/firebaseOrders/.test(rq.url()) || rq.method() !== 'POST') return; const b = JSON.parse(rq.postData() || '{}'); if (!b.session) return; const j = await rs.json().catch(() => null); out.answers.push({ ev: b.session.event, id: b.session.id, status: rs.status(), ended: j && j.ended, why: j && j.endReason }); } catch (_) {} });
    const page = await A.sorterApp(ctx, { label: 'laser' });
    const poke = async () => { await page.mouse.move(70 + Math.random() * 300, 150 + Math.random() * 200).catch(() => {}); await page.mouse.move(90 + Math.random() * 300, 160 + Math.random() * 200).catch(() => {}); };
    const pageNow = () => page.evaluate(() => Date.now());
    const lastInput = async () => { const [pt, li] = await page.evaluate(() => [Date.now(), StationSession.lastInput()]); return li ? li + ((await W.now()) - pt) : 0; };
    const sessions = async () => (await W.list('Station_Sessions')).filter(s => s.person === LENA).sort((a, b) => (a.startAt || 0) - (b.startAt || 0));
    const shown = () => page.evaluate(() => ({ name: CNEmployee.name(), role: StationSession.role(), cur: StationSession.current() && StationSession.current().id }));

    /* 1 · the first sign-in, some work, an hour of quiet */
    await A.sorterRole(page, LENA, 'laser'); await poke();
    await page.evaluate(() => { CNAct('complete', { station: 'laser', each: [{ orderId: 'LS2-1', line: 'GF Sheet 1', parts: 1 }], detail: 'GF Sheet 1 marked completed (laser)' }); }).catch(() => {});
    const in1 = await lastInput(); if (!in1) bad('no last input after the first sign-in');
    await W.jump(in1 + 50 * MIN - await W.now()); await sleep(800); await W.advance(30000, { step: 15000, settle: 150 });
    let l = await sessions(); if (!l.length || l[0].endAt != null) bad('signed out 50 minutes after her last input (' + (l[0] && l[0].endReason) + ')');
    await W.jump(in1 + 65 * MIN - await W.now()); await sleep(800); await W.advance(30000, { step: 15000, settle: 150 });
    for (let i = 0; i < 40 && (!(l = await sessions()).length || l[0].endAt == null); i++) await sleep(250);
    if (!l.length || l[0].endAt == null) bad('NOT signed out after 65 quiet minutes');
    else { if (l[0].endReason !== 'idle') bad('first sign-in ended "' + l[0].endReason + '", wanted idle'); if (Math.abs(l[0].endAt - in1) > 20000) bad('the first hours end ' + Math.round((l[0].endAt - in1) / 1000) + ' s from her last input'); }
    const s0 = await shown(); if (s0.name) bad('the page still shows a name after the idle sign-out: ' + JSON.stringify(s0));

    /* 2 · a few more minutes of her (empty) page, then the second sign-in at the given time */
    await W.jump(TOR(at[0], at[1]) - await W.now()); await sleep(1500);
    const lagStart = way === 'lag200' ? 200000 : 0;
    await A.sorterRole(page, LENA, 'laser');
    const signedAt = await W.now();
    const sinceIdx = out.sent.length;
    l = await sessions();
    if (l.length !== 2 || l[1].endAt != null) bad('the second sign-in has no open session (' + l.length + ' sessions)');
    const second = l[l.length - 1];
    if (lagStart) await W.skew(lagStart);                       // the shop's clock moves, the browser's does not: the browser is behind from now on

    /* 3 · 45 minutes with the page open: one input, 25 minutes in */
    const lenaIn2 = await lastInput();
    let pokedAt = 0;
    const checkpoints = [];
    const record = async tag => { const s = (await sessions()).pop(); checkpoints.push({ tag, shop: await W.now(), lastSeen: s && s.lastSeenAt, li: s && s.lastInputAt, end: s && s.endAt, why: s && s.endReason, id: s && s.id }); };
    const total = 45 * MIN, isJump = /^jump(\d+)$/.exec(way), jumpMin = isJump ? +isJump[1] : 0;
    let done = 0;
    while (done < total) {
      if (!pokedAt && done >= 25 * MIN) { await poke(); pokedAt = await lastInput(); }
      let d = Math.min(jumpMin ? jumpMin * MIN : MIN, total - done);
      if (!pokedAt) d = Math.min(d, 25 * MIN - done);              // (stop at the input)
      if (jumpMin) { await W.jump(d); await sleep(350); }
      else { await W.advance(d, { step: d, settle: 120 }); if (way === 'lagGrows') await W.skew(5000); }
      done += d;
      if (jumpMin || done % (5 * MIN) === 0) await record(hhmm(await W.now()));
    }
    await sleep(1500); await W.advance(10000, { step: 5000, settle: 150 });
    await record('end');

    /* what the page sent for the NEW session (the page's own clock), and what the door kept */
    const mine = out.sent.slice(sinceIdx).filter(x => x.id === second.id || x.ev === 'start');
    const beats = out.sent.filter(x => x.id === second.id && x.ev === 'beat');
    const gaps = beats.map((b, i) => (b.sentAt - (i ? beats[i - 1].sentAt : signedAt)) / MIN);
    const last = checkpoints[checkpoints.length - 1];
    const wantBeats = Math.floor(total / (5 * MIN)) - 1;
    if (beats.length < wantBeats) bad('the page sent ' + beats.length + ' beat(s) for the new session in 45 minutes, wanted at least ' + wantBeats);
    const biggest = Math.max(0, ...gaps);
    if (biggest > Math.max(6.2, jumpMin + 1.2)) bad('a gap of ' + biggest.toFixed(1) + ' minutes between beats of the new session (the page beats every 5)');
    const first = (await sessions()).find(s => s.id === second.id);
    if (!first || first.endAt != null) bad('the new session was ended by the rules while the page was open: ' + JSON.stringify(first && { start: hhmm(first.startAt), end: first.endAt && hhmm(first.endAt), why: first.endReason, lastSeen: hhmm(first.lastSeenAt) }));
    if (first && first.lastSeenAt < (await W.now()) - 8 * MIN) bad('the door\'s last beat of the new session is ' + hhmm(first.lastSeenAt) + ', the shop is at ' + hhmm(await W.now()));
    const answered = out.answers.filter(a => a.id === second.id && a.ended);
    if (answered.length) bad('a beat was answered ended: ' + JSON.stringify(answered[0]));
    const starts = out.sent.filter(x => x.ev === 'start');
    if (starts.length !== 2) bad('the page started ' + starts.length + ' sessions in all, wanted 2 (the first and the second sign-in)');
    out.summary = { signedIn: hhmm(signedAt), lenaInput: hhmm(lenaIn2), poked: pokedAt ? hhmm(pokedAt) : 'none', beats: beats.length, gapsMin: gaps.map(g => +g.toFixed(1)), checkpoints: checkpoints.map(c => c.tag + ' seen ' + (c.lastSeen ? hhmm(c.lastSeen) : '-') + (c.end ? ' END ' + c.why + '@' + hhmm(c.end) : '')).slice(-4), pageClockBehindMs: (await W.now()) - (await pageNow()) };
    if (process.env.VERBOSE) out.allSent = out.sent.map(x => x.ev + ' ' + x.id.slice(-12) + ' ' + (x.sentAt ? hhmm(x.sentAt) : '-') + (x.why ? ' ' + x.why : '') + (x.li ? ' li ' + hhmm(x.li) : ''));
    if (process.env.VERBOSE) out.checkpoints = checkpoints.map(c => c.tag + ' lastSeen ' + (c.lastSeen ? hhmm(c.lastSeen) : '-') + ' li ' + (c.li ? hhmm(c.li) : '-') + (c.end ? ' END ' + c.why + '@' + hhmm(c.end) : ''));
  } catch (e) { bad('the scenario broke: ' + String(e && e.message || e).slice(0, 400)); }
  finally { try { W.stop(); } catch (_) {} for (const c of W.ctxs.slice()) { try { await c.close(); } catch (_) {} } }
  return out;
}

(async () => {
  browser = await pwCore.chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  say('A Laser person: signed out after an hour of quiet, signs in again, 45 minutes with the page open');
  for (const at of signins) for (const way of WAYS) {
    const t0 = Date.now(), r = await scenario(way, at);
    say((r.problems.length ? '  FAIL ' : '  ok   ') + r.label + '  (' + Math.round((Date.now() - t0) / 1000) + ' s)  ' + JSON.stringify(r.summary || {}));
    for (const p of r.problems) say('         ' + p);
    if (r.allSent) for (const c of r.allSent) say('           sent: ' + c);
    if (r.checkpoints) for (const c of r.checkpoints) say('           ' + c);
    if (r.problems.length) failures.push(r.label);
  }
  await browser.close();
  say(failures.length ? '\n' + failures.length + ' scenario(s) failed: ' + failures.join(' ; ') : '\nall scenarios passed');
  process.exit(failures.length ? 1 : 0);
})().catch(e => { console.error('THE TEST ITSELF BROKE:', e && e.stack || e); process.exit(2); });
