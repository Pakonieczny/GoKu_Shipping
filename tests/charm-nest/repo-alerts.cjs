// The persistent Alert for password attempts on the Charm Repo Library (charm-nest-repo-alerts.js; Paul, 11 Oct:
// "persistent and visible only in my login until I manually close the Alert"). Offline: jsdom, a mocked server (CN.api over
// an in-memory list of attempts, answering as REPOAUTH-api.md says), a fake clock and fake timers. No password, token or PIN
// appears anywhere here.
//   1 · hidden and silent for anyone but the master admin (no helper, helper says no, helper throws): no DOM, no style, no
//       timer, no call, whatever happens (ten minutes, focus, tab events, an attempt event, refresh()).
//   2 · the master admin: one read at load; the pill (count, tint) and the card: newest first, who / what / which file /
//       when (Toronto time, EDT and EST) / wrong, locked out, right; text only (no markup from the server is run).
//   3 · persistence: a reload (new page, same server) shows the same alert; nothing is kept in the browser.
//   4 · Close: one repoAlertClose for exactly the attempts on the card, then the pill is gone, also after a reload;
//       an attempt that arrived after the card was drawn is not closed with it; a failed close keeps the alert and says why.
//   5 · a new attempt after a close raises a new alert at once (repo-auth-attempt), without the one-minute cap.
//   6 · the cap: at most one read a minute while the tab is in view (the timer, focus, visibilitychange count the same), one
//       timer at a time, none while the tab is hidden, one read when it is back after a minute.
//   7 · honest errors: a failed read keeps what is listed and says so in one line; a 403 says the login was not accepted.
//   8 · signing out (the helper's own event) removes everything and stops the timer; signing in reads once.
//   9 · the server lists at most 100: the pill says the real number, Close closes the 100 shown, the rest follows; an
//       attempt that could not be checked is neither wrong nor right.
//   node tests/charm-nest/repo-alerts.cjs     (NODE_PATH=<node_modules with jsdom>)
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const { JSDOM } = require('jsdom');
const root = path.join(__dirname, '../..');
const SRC = fs.readFileSync(path.join(root, 'charm-nest-repo-alerts.js'), 'utf8');
const settle = async () => { for (let i = 0; i < 8; i++) await new Promise(r => setImmediate(r)); };
const T = (iso) => Date.parse(iso);

/* ── the mocked server: attempts as REPOAUTH stores them, answers as REPOAUTH-api.md says ── */
function makeServer() {
  const s = { rows: [], n: 0, admin: true, fail: null, closeFail: null, calls: [], hold: null, gates: [] };
  s.add = (o) => { const id = 'a' + (++s.n); s.rows.push(Object.assign({ id, at: 0, by: 'Ana_M', action: 'download', target: '', ok: false, reason: 'wrong', closed: false }, o, { id })); return id; };
  s.api = async (fn, body, opts) => {
    s.calls.push({ fn, op: body && body.op, body: JSON.parse(JSON.stringify(body || {})), opts });
    assert.equal(fn, 'charmNestLibrary', 'every call goes through the library function');
    if (s.hold) await s.hold;
    if (body.op === 'repoAlerts') {
      if (s.fail) throw s.fail;
      if (!s.admin) throw Object.assign(new Error('admin-only'), { status: 403, data: { error: 'admin-only' } });
      const open = s.rows.filter(r => !r.closed).map(({ closed, ...r }) => r);
      // as the server does: newest first, at most 100, a total (capped at 300) and `more`
      const list = open.slice().sort((a, b) => b.at - a.at);
      const answer = { ok: true, alerts: list.slice(0, 100).reverse(), total: Math.min(300, list.length), more: list.length > 100 };   // (reversed here, on purpose: the client sorts)
      const gate = s.gates.shift(); if (gate) await gate;                  // (the answer was made now and arrives later: a slow read)
      return answer;
    }
    if (body.op === 'repoAlertClose') {
      if (s.closeFail) throw s.closeFail;
      assert.ok(Array.isArray(body.ids) && body.ids.length <= 100, 'the close names its attempts, a hundred at most');
      for (const r of s.rows) if (body.ids.includes(r.id)) r.closed = true;
      return { ok: true, closed: body.ids.length };
    }
    throw new Error('unexpected op ' + body.op);
  };
  s.reads = () => s.calls.filter(c => c.op === 'repoAlerts').length;
  s.closes = () => s.calls.filter(c => c.op === 'repoAlertClose');
  return s;
}

/* ── a page: the sorter's top bar, the helper's answer, a fake clock and fake timers ── */
async function page(server, o = {}) {
  const dom = new JSDOM('<!doctype html><html><head></head><body><div class="topbar"><div class="topTools"><button id="other">Workspace</button></div></div><main id="m">page</main></body></html>',
    { url: 'https://sorter.test/', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, P = { w, server, t: o.t || T('2026-10-11T20:00:00Z'), timers: [], tid: 0, admin: o.admin !== false, vis: o.visible !== false, login: o.login || 'Paul' };
  w.setTimeout = (fn, ms) => { const id = ++P.tid; P.timers.push({ id, fn, at: P.t + ms }); return id; };
  w.clearTimeout = (id) => { P.timers = P.timers.filter(x => x.id !== id); };
  w.setInterval = () => { throw new Error('no interval: a timer only while the master admin is signed in'); };
  w.Date.now = () => P.t;                                                  // (the page's own clock)
  Object.defineProperty(w.document, 'visibilityState', { configurable: true, get: () => (P.vis ? 'visible' : 'hidden') });
  if (o.helper !== false) w.CharmNestMasterAdmin = { is: () => P.admin, login: () => P.login };
  w.CN = { api: (fn, body, opts) => server.api(fn, body, opts) };
  w.eval(SRC);
  await new Promise(r => (w.document.readyState === 'loading' ? w.document.addEventListener('DOMContentLoaded', r) : r()));
  await settle();
  P.A = w.CharmNestRepoAlerts;
  P.pill = () => w.document.getElementById('cnRepoAlert');
  P.rows = () => [...w.document.querySelectorAll('#cnRepoAlert .raItem')];
  P.advance = async (ms) => {                                              // time passes: due timers fire in order
    const end = P.t + ms;
    for (;;) {
      const due = P.timers.filter(x => x.at <= end).sort((a, b) => a.at - b.at)[0];
      if (!due) break;
      P.timers = P.timers.filter(x => x !== due); P.t = Math.max(P.t, due.at); due.fn(); await settle();
    }
    P.t = end; await settle();
  };
  P.focus = async () => { w.dispatchEvent(new w.Event('focus')); await settle(); };
  P.setVisible = async (v) => { P.vis = v; w.document.dispatchEvent(new w.Event('visibilitychange')); await settle(); };
  P.attempt = async () => { w.dispatchEvent(new w.CustomEvent('repo-auth-attempt', { detail: { action: 'download', ok: false, reason: 'wrong' } })); await settle(); };
  P.who = async (admin) => { P.admin = admin; w.dispatchEvent(new w.CustomEvent('charmnest:master-admin', { detail: { is: admin, login: P.login } })); await settle(); };   // (the helper's own announcement)
  P.click = async (sel) => { w.document.querySelector(sel).dispatchEvent(new w.MouseEvent('click', { bubbles: true })); await settle(); };
  P.close = () => w.close();
  return P;
}
const text = (e) => e.textContent.replace(/\s+/g, ' ').trim();

(async () => {
  /* ── 0 · wiring: the tag and the public file list ── */
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  assert.equal((html.match(/<script src="charm-nest-repo-alerts\.js\?v=[\w-]+"><\/script>/g) || []).length, 1, 'one script tag with a cache token');
  assert.ok(/"charm-nest-repo-alerts\.js"/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')), 'the file is in the public manifest');

  /* ── 1 · anyone but the master admin: nothing, whatever happens ── */
  for (const variant of ['helper says no', 'no helper', 'helper throws']) {
    const server = makeServer(); server.add({ at: T('2026-10-11T19:00:00Z'), by: 'Someone' });
    const P = await page(server, variant === 'no helper' ? { helper: false } : { admin: false });
    if (variant === 'helper throws') { P.w.CharmNestMasterAdmin = { is() { throw new Error('boom'); }, login() { throw new Error('boom'); } }; }
    const before = P.w.document.body.innerHTML, head = P.w.document.head.innerHTML;
    for (let i = 0; i < 10; i++) { await P.advance(60000); assert.equal(P.timers.length, 0, variant + ': no timer for anyone else'); }
    await P.focus(); await P.setVisible(false); await P.setVisible(true); await P.attempt(); await P.attempt();
    P.w.dispatchEvent(new P.w.CustomEvent('charmnest:master-admin', { detail: { is: false, login: 'Ana_M' } })); await settle();
    assert.equal(await P.A.refresh(), 0); await settle();
    assert.equal(server.calls.length, 0, variant + ': not one call to the server');
    assert.equal(P.pill(), null, variant + ': no pill'); assert.equal(P.w.document.getElementById('cnRepoAlertCss'), null, variant + ': no style');
    assert.equal(P.w.document.body.innerHTML, before, variant + ': the page is untouched'); assert.equal(P.w.document.head.innerHTML, head);
    assert.equal(P.timers.length, 0); assert.equal(P.A.count(), 0); assert.equal(P.A.isShown(), false);
    P.close();
  }

  /* ── 2 · the master admin: the pill and the card ── */
  const server = makeServer();
  const idWrong = server.add({ at: T('2026-10-11T19:41:00Z'), by: 'Ana_M', action: 'download', target: 'RIBBON_BOW_2201.ai', ok: false, reason: 'wrong' });
  const idLocked = server.add({ at: T('2026-10-11T19:55:00Z'), by: 'Ana_M', action: 'upload', target: 'master-2026-10-11.zip', ok: false, reason: 'locked' });
  const idOk = server.add({ at: T('2026-10-11T19:50:30Z'), by: 'Paul', action: 'delete', target: '<img src=x onerror="window.pwned=1">', ok: true, reason: 'ok' });
  const idWinter = server.add({ at: T('2025-12-24T15:05:00Z'), by: 'Ivy', action: 'history', target: '', ok: false, reason: 'wrong' });
  server.add({ at: T('2026-10-01T15:05:00Z'), by: 'Ivy', action: 'modify', target: 'old', ok: false, reason: 'wrong' }); server.rows.at(-1).closed = true;
  let P = await page(server, { t: T('2026-10-11T20:00:00Z') });
  assert.equal(server.reads(), 1, 'one read at page load');
  assert.deepEqual(server.calls[0].body, { op: 'repoAlerts', by: 'Paul' }, 'the server wants the signed-in login as `by`'); assert.equal(server.calls[0].opts.quiet, true, 'no progress bar for a quiet look');
  assert.ok(P.A.isShown() && P.A.count() === 4, 'four unclosed attempts');
  const pill = P.pill(); assert.ok(pill.parentElement.classList.contains('topTools'), 'at the right end of the top bar');
  assert.equal(pill.parentElement.lastElementChild, pill, 'after the existing tools');
  assert.ok(pill.querySelector('details.runMenu') && pill.querySelector('.runMenuBody'), 'the run pill\'s own component');
  assert.equal(pill.querySelector('.rbText').textContent, 'Repo alert'); assert.equal(pill.querySelector('.raN').textContent, '4'); assert.equal(pill.querySelector('.raN').hidden, false);
  assert.ok(pill.classList.contains('bad'), 'wrong and locked tries tint it');
  assert.deepEqual(P.rows().map(r => r.dataset.id), [idLocked, idOk, idWrong, idWinter], 'newest first, whatever order the server used');
  const rows = P.rows();
  assert.ok(rows[0].classList.contains('locked') && /Locked out/.test(text(rows[0])) && /Ana_M tried to upload master-2026-10-11\.zip/.test(text(rows[0])) && /too many wrong tries/.test(text(rows[0])));
  assert.ok(rows[0].querySelector('time').textContent.includes('3:55 PM EDT'), 'Toronto time in October: ' + rows[0].querySelector('time').textContent);
  assert.ok(rows[1].classList.contains('ok') && /Right password/.test(text(rows[1])) && /Paul tried to delete/.test(text(rows[1])));
  assert.ok(rows[2].classList.contains('wrong') && /Wrong password/.test(text(rows[2])) && /Ana_M tried to download RIBBON_BOW_2201\.ai/.test(text(rows[2])));
  assert.ok(rows[2].querySelector('time').textContent.includes('3:41 PM EDT'));
  assert.ok(/Ivy tried to open the upload history/.test(text(rows[3])) && rows[3].querySelector('time').textContent.includes('10:05 AM EST') && rows[3].querySelector('time').textContent.includes('2025'), 'winter time, other year shown: ' + rows[3].querySelector('time').textContent);
  assert.equal(rows[1].querySelector('.raTarget').textContent, '<img src=x onerror="window.pwned=1">', 'server text is shown as text');
  assert.equal(rows[1].querySelector('img'), null); assert.equal(P.w.pwned, undefined);
  assert.equal(text(pill.querySelector('.raSum')), '4 not closed · 2 wrong · 1 locked out · 1 right');
  assert.equal(pill.querySelectorAll('.raClose').length, 1, 'ONE close button');
  assert.equal(pill.querySelector('details').open, false, 'the card opens on a press, not by itself');
  assert.equal(P.timers.length, 1, 'one timer, for the next look in a minute');
  assert.equal(P.w.localStorage.length + P.w.sessionStorage.length, 0, 'nothing kept in the browser');
  assert.ok(!/password|token|hash|salt|pin/i.test(JSON.stringify(server.calls)), 'no secret field travels');

  /* ── 3 · persistence: a reload shows the same alert ── */
  P.close();
  P = await page(server, { t: T('2026-10-11T20:30:00Z') });
  assert.deepEqual(P.rows().map(r => r.dataset.id), [idLocked, idOk, idWrong, idWinter], 'after a reload');
  assert.equal(P.A.count(), 4);

  /* ── 4 · Close: exactly the attempts on the card ── */
  await P.click('#cnRepoAlert summary'); assert.equal(P.pill().querySelector('details').open, true);
  server.closeFail = Object.assign(new Error('The cloud did not answer'), { transient: true });
  const readsBefore = server.reads();
  await P.click('.raClose');
  assert.equal(P.A.count(), 4, 'a failed close keeps the alert'); assert.ok(P.A.isShown());
  assert.ok(/Not closed: the cloud did not answer/.test(text(P.pill().querySelector('.raErr'))) && !P.pill().querySelector('.raErr').hidden, 'one honest line');
  assert.equal(P.pill().querySelector('.raClose').disabled, false, 'it can be tried again');
  assert.equal(server.reads(), readsBefore + 1, 'a failed close reads again once (nothing was lost)');
  server.closeFail = null;
  // the close is on its way when another attempt is made: it is not closed with the four on the card
  let go; server.hold = new Promise(r => { go = r; });
  await P.click('.raClose');
  assert.ok(P.pill().querySelector('.raClose').disabled && /Closing/.test(text(P.pill().querySelector('.raWait'))) && !P.pill().querySelector('.raWait').hidden, 'a small labelled spinner while it closes');
  const arrived = server.add({ at: T('2026-10-11T20:31:00Z'), by: 'Ana_M', action: 'delete', target: 'late', ok: false, reason: 'wrong' });
  server.hold = null; go(); await settle();
  const closes = server.closes();
  assert.equal(closes.length, 2); assert.deepEqual([...closes[1].body.ids].sort(), [idLocked, idOk, idWrong, idWinter].sort(), 'only the attempts that were on the card');
  assert.deepEqual(closes[1].body.by, 'Paul'); assert.ok(!closes[1].body.ids.includes(arrived), 'a later attempt is not closed with them');
  assert.equal(P.A.count(), 1, 'the attempt that arrived meanwhile is a new alert'); assert.equal(P.rows()[0].dataset.id, arrived);
  assert.equal(P.pill().querySelector('.raErr').hidden, true);
  await P.click('.raClose');
  assert.equal(P.A.count(), 0); assert.equal(P.A.isShown(), false, 'closed: gone'); assert.equal(P.pill().hidden, true);
  assert.equal(P.pill().querySelector('details').open, false);
  P.close();
  P = await page(server, { t: T('2026-10-11T21:00:00Z') });
  assert.equal(P.A.count(), 0); assert.equal(P.pill(), null, 'still gone after a reload: the server remembers the close');
  assert.ok(server.rows.filter(r => r.closed).length === 6 && server.rows.length === 6, 'closing keeps every record');

  /* ── 4b · a slow read that left before the close cannot bring the closed attempts back ── */
  const ls = makeServer(); const lsA = ls.add({ at: T('2026-10-11T19:00:00Z') }), lsB = ls.add({ at: T('2026-10-11T19:01:00Z') });
  const Q = await page(ls); assert.equal(Q.A.count(), 2);
  let gate1, gate2; ls.gates.push(new Promise(r => { gate1 = r; }), new Promise(r => { gate2 = r; }));
  await Q.attempt();                                                       // a read leaves with both attempts in its answer ...
  await Q.click('.raClose');                                               // ... and Close is pressed meanwhile (it asks for one more read, which waits behind it)
  assert.equal(Q.A.count(), 0, 'closed'); assert.deepEqual(ls.closes()[0].body.ids.sort(), [lsA, lsB].sort());
  gate1(); await settle();
  assert.equal(Q.A.count(), 0, 'the slow answer still lists them: they stay closed'); assert.equal(Q.A.isShown(), false);
  gate2(); await settle(); assert.equal(Q.A.count(), 0); assert.equal(Q.A.isShown(), false); Q.close();

  /* ── 5 · a new attempt after a close raises a new alert at once ── */
  const reads0 = server.reads();
  const again = server.add({ at: T('2026-10-11T21:00:20Z'), by: 'Ana_M', action: 'modify', target: 'STAR_11', ok: false, reason: 'wrong' });
  await P.attempt();                                                       // (no minute has passed)
  assert.equal(server.reads(), reads0 + 1, 'the attempt event reads right away');
  assert.equal(P.A.count(), 1); assert.equal(P.rows()[0].dataset.id, again); assert.ok(P.A.isShown());
  assert.ok(/Ana_M tried to change STAR_11/.test(text(P.rows()[0])));
  // three events together: no pile of reads (one in flight, one after it)
  let release; server.hold = new Promise(r => { release = r; });
  const r1 = server.reads();
  for (let i = 0; i < 3; i++) P.w.dispatchEvent(new P.w.CustomEvent('repo-auth-attempt'));
  await settle(); assert.equal(server.reads(), r1 + 1, 'one read at a time');
  assert.equal(P.pill().querySelector('.raWait').hidden, false, 'a small labelled spinner while it waits');
  assert.ok(/Checking for new attempts/.test(text(P.pill().querySelector('.raWait'))) && P.pill().querySelector('.raWait .spin'));
  server.hold = null; release(); await settle();
  assert.equal(server.reads(), r1 + 2, 'and one more after it, not three');
  assert.equal(P.pill().querySelector('.raWait').hidden, true);
  P.close();

  /* ── 6 · the cap: one read a minute while the tab is in view ── */
  const cs = makeServer(); cs.add({ at: T('2026-10-11T19:00:00Z') });
  P = await page(cs, { t: T('2026-10-11T22:00:00Z') });
  assert.equal(cs.reads(), 1); assert.equal(P.timers.length, 1);
  await P.advance(10000); await P.advance(20000); await P.focus(); await P.setVisible(false); await P.setVisible(true); await P.advance(29000);
  assert.equal(cs.reads(), 1, 'nothing inside the first minute (59 s), whatever focus and the tab did'); assert.equal(P.timers.length, 1, 'still one timer');
  cs.add({ at: T('2026-10-11T22:00:30Z'), by: 'Ivy', action: 'upload', target: 'x.zip' });
  await P.advance(1500);
  assert.equal(cs.reads(), 2, 'one at the minute'); assert.equal(P.A.count(), 2, 'an attempt on another device shows within a minute');
  await P.advance(30000); await P.focus(); assert.equal(cs.reads(), 2);
  await P.setVisible(false);
  await P.advance(6 * 60000);                                              // six minutes in the background
  assert.equal(cs.reads(), 2, 'nothing while the tab is hidden'); assert.equal(P.timers.length, 0, 'and no timer waits');
  await P.setVisible(true);
  assert.equal(cs.reads(), 3, 'one when it is back in view'); await P.focus(); assert.equal(cs.reads(), 3);
  for (let i = 0; i < 15; i++) await P.advance(4000);                      // one minute: one read
  assert.equal(cs.reads(), 4);
  await P.attempt(); assert.equal(cs.reads(), 5, 'an attempt reads at once'); await P.advance(59000); assert.equal(cs.reads(), 5, 'and the minute starts again from it'); await P.advance(2000); assert.equal(cs.reads(), 6);
  P.close();
  // a tab that loads in the background reads when it is first seen, not before
  const hs = makeServer(); hs.add({ at: T('2026-10-11T19:00:00Z') });
  P = await page(hs, { visible: false });
  assert.equal(hs.reads(), 0, 'hidden at load: no read'); assert.equal(P.timers.length, 0); await P.setVisible(true); assert.equal(hs.reads(), 1); assert.equal(P.A.count(), 1);
  P.close();

  /* ── 7 · honest errors ── */
  const es = makeServer(); es.add({ at: T('2026-10-11T19:00:00Z'), by: 'Ana_M', action: 'download', target: 'A.ai' });
  P = await page(es);
  assert.equal(P.A.count(), 1);
  es.fail = Object.assign(new Error('x'), { transient: true });
  await P.advance(61000);
  assert.equal(P.A.count(), 1, 'a failed read keeps what was listed'); assert.equal(P.rows().length, 1);
  assert.ok(/Not read just now: the cloud did not answer\. Trying again within a minute\./.test(text(P.pill().querySelector('.raErr'))));
  assert.equal(P.timers.length, 1, 'and it tries again');
  es.fail = null; await P.advance(61000);
  assert.equal(P.pill().querySelector('.raErr').hidden, true, 'the line goes when a read works'); P.close();
  const e2 = makeServer(); e2.fail = Object.assign(new Error('Request failed'), { status: 503, transient: true, data: { error: 'unavailable' } });
  P = await page(e2);
  assert.ok(P.A.isShown() && P.A.count() === 0 && /Repo alert · not read/.test(text(P.pill().querySelector('summary'))), 'nothing loaded yet: the pill says it could not read');
  assert.ok(P.pill().classList.contains('bad')); assert.ok(/the server could not read the alert list/.test(text(P.pill().querySelector('.raErr')))); P.close();
  const e3 = makeServer(); e3.admin = false;
  P = await page(e3);
  assert.ok(/did not accept this login for alerts/.test(text(P.pill().querySelector('.raErr'))), 'a 403 is said plainly'); P.close();

  /* ── 8 · signing out and in (the helper's event) ── */
  const ss = makeServer(); ss.add({ at: T('2026-10-11T19:00:00Z') });
  P = await page(ss);
  assert.equal(P.A.count(), 1); const nReads = ss.reads();
  await P.who(false);
  assert.equal(P.pill(), null, 'signed out: the pill is gone at once'); assert.equal(P.A.count(), 0); assert.equal(P.A.isShown(), false); assert.equal(P.timers.length, 0, 'and its timer');
  await P.advance(10 * 60000); await P.attempt(); await P.focus(); await P.setVisible(false); await P.setVisible(true);
  assert.equal(ss.reads(), nReads, 'and no more reads');
  await P.who(true);
  assert.equal(ss.reads(), nReads + 1, 'signed in as the master admin: one read'); assert.equal(P.A.count(), 1); assert.ok(P.A.isShown());
  // the Admin leaving without an event is noticed at the next look (the timer, or a focus)
  P.admin = false; await P.advance(61000); assert.equal(P.pill(), null); assert.equal(P.timers.length, 0);
  P.close();

  /* ── 9 · more than the server lists; an attempt it could not check ── */
  const ms = makeServer(); const ids = [];
  for (let i = 0; i < 130; i++) ids.push(ms.add({ at: T('2026-10-11T19:00:00Z') + i * 1000, by: 'Ana_M', action: 'download', target: 'C' + i, ok: i % 10 === 0, reason: i % 10 === 0 ? 'ok' : 'wrong' }));
  P = await page(ms);
  assert.equal(P.A.count(), 100, 'the newest 100 are listed'); assert.equal(P.pill().querySelector('.raN').textContent, '130', 'the pill says how many there really are');
  assert.ok(/130 not closed/.test(text(P.pill().querySelector('.raSum'))) && /newest 100 listed/.test(text(P.pill().querySelector('.raSum'))));
  assert.equal(P.rows()[0].dataset.id, ids[129]);
  await P.click('.raClose');
  assert.equal(ms.closes().length, 1); assert.equal(ms.closes()[0].body.ids.length, 100, 'Close closes the 100 shown');
  assert.equal(P.A.count(), 30, 'the other 30 come next'); assert.equal(P.pill().querySelector('.raN').textContent, '30'); assert.ok(P.A.isShown());
  await P.click('.raClose'); assert.equal(P.A.count(), 0); assert.equal(P.A.isShown(), false); assert.ok(ms.rows.every(r => r.closed));
  for (let i = 0; i < 320; i++) ms.add({ at: T('2026-10-11T20:00:00Z') + i * 1000 });
  await P.attempt(); assert.equal(P.pill().querySelector('.raN').textContent, '300+', 'the server\'s own cap shows as 300+'); P.close();
  const us = makeServer(); us.add({ at: T('2026-10-11T19:00:00Z'), by: 'Ana_M', action: 'delete', target: 'X', ok: false, reason: 'unavailable' }); us.add({ at: T('2026-10-11T19:01:00Z'), by: 'Paul', action: 'upload', target: 'Y', ok: true, reason: 'ok' });
  P = await page(us);
  assert.ok(P.pill().classList.contains('info') && !P.pill().classList.contains('bad'), 'right and unchecked tries do not look like a break-in');
  assert.deepEqual(P.rows().map(r => text(r.querySelector('.raTop b'))), ['Right password', 'Could not check']);
  assert.ok(/the password could not be checked/.test(text(P.rows()[1]))); P.close();

  console.log('PASS: repo alerts: silent for everyone but the master admin; pill and card (newest first, Toronto time, wrong / locked / right, text only); persists across reloads; one Close for the shown attempts; a new attempt re-raises it; one read a minute while in view; honest errors; sign-out clears it; the 100-row cap and unchecked tries');
})().catch(e => { console.error(e); process.exitCode = 1; });
