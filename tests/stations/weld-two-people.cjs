// Two people at the Welding station at once (Paul, 6 Oct 2026, items 2 and 3; plans/stations-round2 C2 and R3). Fakes only:
// every request that is not the page itself is stubbed or aborted, the clock is Playwright's, nothing leaves the machine.
//   1 · the StationSession multi-person API on a small fixture page: init({multi, people, signOut(reason, who)}), signedIn adds
//       one person and never ends another, the same person in two tasks, signedOut ends that one, people(), who() (Matching,
//       latest input), touch()/lastInput(), a reload restores every session (no second start), a name the page drops is signed
//       out, midnight ends everybody and calls signOut once per person, 15 quiet minutes close one session and start it again,
//       the session id is `welding__weld-1__<person>__<task>__…`, a PIN-looking id is never sent; a single-person page is as before
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/weld-two-people.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROMIUM = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const wait = ms => new Promise(r => setTimeout(r, ms));
const json = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
async function until(fn, what, ms = 9000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(50); } }

/* ── 1 · the API ── */
const T0 = Date.parse('2026-10-06T15:00:00Z');            // 11:00 in New York (EDT)
const MIDNIGHT = Date.parse('2026-10-07T04:00:00Z');       // 00:00 on 7 Oct in New York
const ORIGIN = 'http://api.test';
async function openFixture(browser, time) {
  const sent = [];
  const fixture = `<!doctype html><meta charset="utf-8"><title>fixture</title><body>
  <script>
    window.__signOuts = []; window.__list = []; window.__lastErr = [];
    const read = () => { try { return JSON.parse(localStorage.getItem('fx_people') || '[]'); } catch (_) { return []; } };
    window.__people = () => read();
    window.__put = l => localStorage.setItem('fx_people', JSON.stringify(l));
    window.addEventListener('error', e => __lastErr.push(String(e.message)));
  </script>
  <script src="/station-session.js"></script>
  <script>
    StationSession.init({ station: 'welding', device: 'weld-1', multi: true,
      people: () => read(),
      signOut: (reason, who) => { __signOuts.push([reason, who]); __put(read().filter(p => !(p.name === who.name && p.task === who.task))); } });
  </script></body>`;
  const ctx = await browser.newContext({ viewport: { width: 900, height: 600 } });
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url()), m = r.request().method();
    if (u.origin !== ORIGIN) return r.abort();
    if (u.pathname === '/fixture.html') return r.fulfill({ status: 200, contentType: 'text/html', body: fixture });
    if (u.pathname.startsWith('/.netlify/functions/')) { if (m === 'POST') { const b = JSON.parse(r.request().postData() || '{}'); if (b.session) sent.push(b.session); } return json(r, { success: true }); }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  const page = await ctx.newPage();
  const errors = []; page.on('pageerror', e => errors.push(String(e && e.message || e)));
  await page.clock.install({ time });
  await page.goto(ORIGIN + '/fixture.html');
  const people = () => page.evaluate(() => StationSession.people().map(p => ({ name: p.name, task: p.task })));
  const kinds = () => sent.map(s => `${s.event}:${s.person}:${s.task || ''}${s.reason ? ':' + s.reason : ''}`);
  const signIn = (name, task, id) => page.evaluate(([n, t, i]) => { __put(__people().concat([{ name: n, task: t }])); StationSession.signedIn({ name: n, id: i, task: t }); }, [name, task, id]);
  const signOut = (name, task) => page.evaluate(([n, t]) => { __put(__people().filter(p => !(p.name === n && p.task === t))); StationSession.signedOut('signOut', { name: n, task: t }); }, [name, task]);
  const flush = () => page.clock.runFor(50).then(() => wait(120));
  return { ctx, page, sent, errors, people, kinds, signIn, signOut, flush };
}
async function api(browser) {
  const { ctx, page, sent, errors, people, kinds, signIn, signOut, flush } = await openFixture(browser, T0);

  // nobody yet
  assert.deepStrictEqual(await people(), []);
  assert.strictEqual(await page.evaluate(() => StationSession.who()), null);
  assert(await page.evaluate(() => StationSession.lastInput()) > 0, 'opening the page is an input');

  // Tess signs in under Welding: one session, with her task
  await signIn('Tess Welder', 'welding', '');
  await flush();
  assert.deepStrictEqual(kinds(), ['start:Tess Welder:welding']);
  assert.match(sent[0].id, /^welding__weld-1__Tess_Welder__welding__[\w]+$/, 'the session id says station, device, person and task');
  assert(sent[0].id.length <= 100 && /^[\w.:-]{8,100}$/.test(sent[0].id), 'a valid door id');
  assert.strictEqual(await page.evaluate(() => StationSession.who()), null, 'a scan is credited to the Matching person: with nobody in Matching there is none');
  assert.strictEqual((await page.evaluate(() => StationSession.who('welding'))).person, 'Tess Welder');

  // Ray signs in under Matching: Tess carries on untouched (no end, no second start for her)
  await page.clock.runFor(60000);
  await signIn('Ray Matcher', 'matching', '');
  await flush();
  assert.deepStrictEqual(kinds(), ['start:Tess Welder:welding', 'start:Ray Matcher:matching'], 'a second sign-in never ends the first');
  assert.deepStrictEqual(await people(), [{ name: 'Tess Welder', task: 'welding' }, { name: 'Ray Matcher', task: 'matching' }]);
  assert.strictEqual((await page.evaluate(() => StationSession.who())).person, 'Ray Matcher');

  // Tess is also in Matching (the same person in both tasks: two sessions); the latest input wins the scans
  await page.clock.runFor(30000);
  await signIn('Tess Welder', 'matching', '123456');                       // (an id made of digits is a PIN: never sent)
  await flush();
  assert.deepStrictEqual(kinds().slice(2), ['start:Tess Welder:matching']);
  assert.strictEqual(sent[2].employeeId, '', 'a PIN-looking id is dropped');
  assert.strictEqual(new Set(sent.slice(0, 3).map(s => s.id)).size, 3, 'three sessions, three ids');
  assert.strictEqual((await page.evaluate(() => StationSession.who())).person, 'Tess Welder', 'two in Matching: the latest input');
  await page.evaluate(() => StationSession.touch(Date.now(), { name: 'Ray Matcher', task: 'matching' }));
  assert.strictEqual((await page.evaluate(() => StationSession.who())).person, 'Ray Matcher', 'an input that is Ray\'s makes him the latest');
  assert.strictEqual((await page.evaluate(() => StationSession.people())).length, 3);
  assert(await page.evaluate(() => StationSession.people().every(p => p.device === 'weld-1' && p.since > 0 && p.lastInputAt >= p.since)));

  // Tess signs out of Matching only: that one ends, her Welding session and Ray carry on
  await page.clock.runFor(60000);
  await signOut('Tess Welder', 'matching');
  await flush();
  assert.deepStrictEqual(kinds().slice(3), ['end:Tess Welder:matching:signOut']);
  assert.deepStrictEqual(await people(), [{ name: 'Tess Welder', task: 'welding' }, { name: 'Ray Matcher', task: 'matching' }]);
  assert.strictEqual((await page.evaluate(() => StationSession.who())).person, 'Ray Matcher');

  // a reload restores both: the same sessions go on (a beat each), no second start, nothing ended
  const ids = sent.slice(0, 2).map(s => s.id), n0 = sent.length;
  await page.reload(); await page.clock.runFor(50); await wait(150);
  const after = sent.slice(n0);
  assert(after.length >= 2 && after.every(s => s.event === 'beat'), 'a reload only beats: ' + JSON.stringify(after.map(s => s.event)));
  assert.deepStrictEqual([...new Set(after.map(s => s.id))].sort(), ids.slice().sort(), 'a reload goes on with both sessions');
  assert.deepStrictEqual(await people(), [{ name: 'Tess Welder', task: 'welding' }, { name: 'Ray Matcher', task: 'matching' }]);

  // the page drops Ray (signed out in another tab): his session ends at the next look, Tess carries on
  await page.evaluate(() => __put(__people().filter(p => p.name !== 'Ray Matcher')));
  await page.clock.runFor(31000); await wait(150);
  assert(kinds().includes('end:Ray Matcher:matching:signOut'), 'a name the page no longer lists is signed out');
  assert.deepStrictEqual(await people(), [{ name: 'Tess Welder', task: 'welding' }]);

  // 20 minutes frozen: the session closed at its last beat and a new one goes on from now
  await page.evaluate(() => { StationSession.touch(); });
  const firstTess = sent.find(s => s.person === 'Tess Welder' && s.task === 'welding' && s.event === 'start').id;
  await page.clock.fastForward(20 * 60000); await page.clock.runFor(31000); await wait(150);
  assert(sent.some(s => s.id === firstTess && s.event === 'end' && s.reason === 'closed'), 'closed at its last beat');
  const restarted = sent.filter(s => s.event === 'start' && s.person === 'Tess Welder' && s.task === 'welding');
  assert.strictEqual(restarted.length, 2, 'and started again from now');
  assert.notStrictEqual(restarted[1].id, firstTess);

  assert.deepStrictEqual(errors, [], 'no page error');
  await ctx.close();

  await midnight(browser);

  // a single-person page is as before: no task, a second sign-in ends the first ("switched")
  const sent2 = [];
  const ctx2 = await browser.newContext();
  await ctx2.route(/.*/, async r => {
    const u = new URL(r.request().url());
    if (u.origin !== ORIGIN) return r.abort();
    if (u.pathname === '/single.html') return r.fulfill({ status: 200, contentType: 'text/html', body: `<!doctype html><title>s</title><script src="/station-session.js"></script><script>
      window.__who = null; StationSession.init({ station: 'welding', device: 'weld-9', person: () => window.__who, signOut: () => {} });</script>` });
    if (u.pathname.startsWith('/.netlify/functions/')) { const b = JSON.parse(r.request().postData() || '{}'); if (b.session) sent2.push(b.session); return json(r, { success: true }); }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file)) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: '' });
  });
  const p2 = await ctx2.newPage(); await p2.goto(ORIGIN + '/single.html');
  await p2.evaluate(() => { window.__who = { name: 'Ana One' }; StationSession.signedIn({ name: 'Ana One' }); });
  await p2.evaluate(() => { window.__who = { name: 'Bo Two' }; StationSession.signedIn({ name: 'Bo Two' }); });
  await wait(150);
  assert.deepStrictEqual(sent2.map(s => s.event + ':' + s.person + (s.reason ? ':' + s.reason : '')), ['start:Ana One', 'end:Ana One:switched', 'start:Bo Two'], 'single-person pages still switch');
  assert(sent2.every(s => s.task === undefined), 'no task on a single-person page');
  assert.deepStrictEqual(await p2.evaluate(() => StationSession.people().map(p => p.name)), ['Bo Two']);
  await ctx2.close();
  console.log('API: two sessions at once, never ended by a second sign-in, same person in both tasks, one sign-out, who() = Matching (latest input), reload restores all, dropped name ends, closed + restart, midnight once per person, single page unchanged');
}

/* midnight: everybody ends "midnight" (at midnight, not when it was noticed) and the page is told once per person */
async function midnight(browser) {
  const { ctx, page, sent, errors, people, signIn, flush } = await openFixture(browser, MIDNIGHT - 6 * 60000);
  await signIn('Tess Welder', 'welding', ''); await signIn('Ray Matcher', 'matching', ''); await flush();
  await page.clock.runFor(2 * 60000);
  await signIn('Tess Welder', 'matching', ''); await flush();
  await page.clock.runFor(7 * 60000); await wait(250);
  const ends = sent.filter(s => s.event === 'end');
  assert.deepStrictEqual(ends.map(s => s.person + ':' + s.task + ':' + s.reason).sort(), ['Ray Matcher:matching:midnight', 'Tess Welder:matching:midnight', 'Tess Welder:welding:midnight']);
  assert(ends.every(s => s.at === MIDNIGHT), 'each ended at midnight, not when the turn was noticed');
  const so = await page.evaluate(() => __signOuts.filter(x => x[0] === 'midnight'));
  assert.deepStrictEqual(so.map(x => x[1].name + ':' + x[1].task).sort(), ['Ray Matcher:matching', 'Tess Welder:matching', 'Tess Welder:welding'], 'signOut("midnight", who) once per person');
  assert.deepStrictEqual(await people(), []);
  assert.strictEqual(sent.filter(s => s.event === 'start').length, 3, 'nobody was started again');
  assert.deepStrictEqual(errors, []);
  await ctx.close();
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROMIUM, args: ['--no-sandbox'] });
  try {
    await api(browser);
  } finally { await browser.close(); }
})().catch(e => { console.error(e); process.exit(1); });
