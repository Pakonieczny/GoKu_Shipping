// How long each laser-cut sheet took (Paul, 6 Oct 2026; plans/stations-round2/plan.md R7, worker LS1), on the fake backend only.
//   A Laser person marks a sheet completed (Library, op laserDone) only after it is cut AND every back engraving is done. Its time = the moment it was
//   marked - START; START = the LATER of (that person's most recent sign-in as Laser, that person's previous standing completion); per person; never
//   earlier than the sign-in; no Laser sign-in (Admin, no role, another station) = NO time (unknown, never invented). Kept for good on its own record
//   (Laser_Sheet_Times), decided once on the server, the page's figure kept beside it, never recomputed or overwritten.
//   A · the rule, pure (the server's and the page's copies agree on a grid of cases), the readers' sums, series and buckets
//   B · the server end to end over the REAL handlers (laserDone, the stations' door, the efficiency console's reader) on the in-memory shop, with a
//       controllable clock: first sheet after sign-in, continuous sign-in (two sheets), two people, a sign-out between sheets, no session (Admin, another
//       station), the page disagreeing / agreeing under a wrong clock / not knowing the previous sheet / no session record, a set marked in one press,
//       undo and marking again, a retry, the sandbox, the previous sheet found again after a "reload", the portal's list, sums, windows, aliases and
//       the Laser card's block of op live
//   C · the page's own module (charm-nest-laser-time.js, window.LaserSheetTime) with stubs for the sign-in: the same cases, reload mid-way (another
//       computer's sheet is read from the server), a clock that is an hour off
//   D · the real pages (Chromium): the sorter's api() sends the page's figure with a laserDone and the stored record agrees; the person page's "Laser
//       sheets" section and the Laser card's line (real portal op, fixture data), the date chips, a labelled wait, no sideways scroll at 390 px
//   NODE_PATH=<jsdom not needed> PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=/opt/pw-browsers/chromium-1194/chrome-linux/chrome \
//     SHOTS=<dir> node tests/stations/laser-sheet-time.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const LT = require(path.join(fnDir, '_laserSheetTime.js'));
const REAL_NOW = Date.now.bind(Date);
const MIN = 60e3, HOUR = 3600e3, DAY = 86400e3;
const sleep = ms => new Promise(r => setTimeout(r, ms));
let checks = 0; const ok = m => { checks++; console.log('  ✓ ' + m); };
const j = o => JSON.parse(JSON.stringify(o));

/* ═══════════════════════ A · the rule, pure ═══════════════════════ */
function partA() {
  const at = 1.8e12, login = at - 10 * MIN;
  assert.deepEqual(j(LT.computeStart(at, login, 0)), { startAt: login, startedFrom: 'login', seconds: 600 }, 'the first sheet after sign-in counts from the sign-in');
  assert.deepEqual(j(LT.computeStart(at, login, at - 200e3)), { startAt: at - 200e3, startedFrom: 'previousSheet', seconds: 200 }, 'a continuous sign-in counts from the previous sheet');
  assert.equal(LT.computeStart(at, login, login - 5e3).startedFrom, 'login', 'a previous sheet from BEFORE the sign-in is not the start');
  assert.equal(LT.computeStart(at, login, login).startedFrom, 'login', 'a tie counts as the sign-in');
  assert.equal(LT.computeStart(at, login, at + 5e3).startedFrom, 'login', 'a "previous" sheet in the future is ignored');
  assert.deepEqual(j(LT.computeStart(at, 0, at - 1e3)), { startAt: null, startedFrom: 'unknown', seconds: null }, 'no sign-in: unknown, never invented');
  assert.equal(LT.computeStart(at, at - 30 * HOUR, 0).seconds, 86400, 'a day is the longest a sheet can be given');
  assert.equal(LT.computeStart(at, at + 1e3, 0).seconds, 0, 'never negative');
  ok('the rule: from sign-in, from the previous sheet, never before the sign-in, a tie is the sign-in, no sign-in is unknown');

  // the page's copy of the rule is the server's, over a grid
  const ctx = vm.createContext({ console, setTimeout, clearTimeout, setInterval, clearInterval, localStorage: { getItem: () => null, setItem() {}, removeItem() {} } });
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-laser-time.js'), 'utf8'), ctx);
  const C = ctx.LaserSheetTime;
  let n = 0;
  for (const lg of [0, at - 90 * MIN, at - 10 * MIN, at - 1 * MIN, at + 5e3]) for (const pv of [0, at - 95 * MIN, at - 30 * MIN, at - 10 * MIN, at - 2e3, at + 7e3, lg]) { assert.deepEqual(j(C.computeStart(at, lg, pv)), j(LT.computeStart(at, lg, pv)), `grid ${lg} ${pv}`); n++; }
  for (const nm of ['Ana M.', "  O'Brien  ", 'Zoë  García', 'Michael_V', 'MARCO   r', 'Paul 482915', 'Tess-Welder', '12 34 56']) assert.equal(C.keyOf(nm), LT.personKey(nm), 'the same person key: ' + nm);
  assert.equal(LT.personKey('Michael_V'), LT.personKey('Michael V.'), 'punctuation does not make a second person');
  assert.equal(LT.personKey('Paul 482915'), 'paul', 'a number is never part of a name (a PIN slipped in is dropped)');
  ok(`the page's copy of the rule gives the server's answer on ${n} cases, and its person key is the server's`);

  // which sign-in covers a completion
  const ses = (o) => Object.assign({ id: 's-' + Math.random().toString(36).slice(2, 8) + 'xyz', person: 'Ana M.', station: 'laser', startAt: at - 20 * MIN, lastSeenAt: at - 1 * MIN, endAt: null }, o), key = LT.personKey('Ana M.');
  assert.equal(LT.pickSession([ses({ id: 'one-laser-1' })], key, at).id, 'one-laser-1');
  assert.equal(LT.pickSession([ses({ person: 'ana m' })], key, at).startAt, at - 20 * MIN, 'another spelling of the name is the same person');
  assert.equal(LT.pickSession([ses({ station: 'sorting' })], key, at), null, 'a sign-in at another station is not a Laser sign-in');
  assert.equal(LT.pickSession([ses({ station: 'sorter', role: 'laser' })], key, at).startAt, at - 20 * MIN, 'a Sorter-app sign-in with the Laser role is one');
  assert.equal(LT.pickSession([ses({ person: 'Ben R.' })], key, at), null, 'another person');
  assert.equal(LT.pickSession([ses({ startAt: at + 1000 })], key, at), null, 'a sign-in that starts after the sheet');
  assert.equal(LT.pickSession([ses({ endAt: at - 5 * MIN })], key, at), null, 'a sign-out before the sheet ends the clock');
  assert.equal(LT.pickSession([ses({ endAt: at + 5 * MIN })], key, at).startAt, at - 20 * MIN, 'a sign-out after it does not');
  // CHANGED 6 Oct (RG2): a Laser page that is merely quiet for 16 minutes still covers the sheet. Paul's Addendum 2 made the Laser station keep a sign-in open until ITS limit (60 minutes
  // without input, 30 from 17:00; _stationAutoSignout.decide / keptOpen): the board shows that person signed in, so the sheet cut while the page was quiet is timed from the sign-in. It used to
  // be unknown here (the old 15 quiet minutes, which no longer sign a Laser person out).
  assert.equal(LT.pickSession([ses({ lastSeenAt: at - 16 * MIN })], key, at).startAt, at - 20 * MIN, 'a page quiet for 16 minutes is still signed in at the Laser station');
  assert.equal(LT.pickSession([ses({ id: 'older-1234', startAt: at - 50 * MIN }), ses({ id: 'newer-1234', startAt: at - 5 * MIN })], key, at).id, 'newer-1234', 'the most recent sign-in wins');
  const doc = (o) => Object.assign({ kind: 'laserSheetDone', personKey: key, sheetId: 'x', at: at - 5 * MIN }, o);
  assert.equal(LT.pickPrevious([doc({ sheetId: 'a', at: at - 9 * MIN }), doc({ sheetId: 'b', at: at - 4 * MIN })], key, at, at - 20 * MIN).sheetId, 'b', 'the latest one');
  assert.equal(LT.pickPrevious([doc({ at: at - 30 * MIN })], key, at, at - 20 * MIN), null, 'one from before the sign-in');
  assert.equal(LT.pickPrevious([doc({ personKey: 'ben r' })], key, at, at - 20 * MIN), null, 'another person');
  assert.equal(LT.pickPrevious([doc({ sheetId: 'a', at: at - 9 * MIN }), { kind: 'laserSheetUndone', sheetId: 'a', was: at - 9 * MIN }], key, at, at - 20 * MIN), null, 'a completion that was taken back is not a previous sheet');
  ok('which sign-in covers a sheet (name folding, Laser station or role, ended, a quiet page, latest wins) and which completion is the previous one');

  // the Laser station's own limits decide whether an OPEN sign-in still covers the moment (RG2; _stationAutoSignout.decide, the rules the board and the sweep apply to the same rows):
  // 60 minutes without input before 17:00 Toronto, 30 from 17:00, ended AT THE LAST INPUT; an Admin's row keeps the old 15 minutes; the New York midnight ends everybody
  const NYT = (h, m = 0, s = 0) => Date.UTC(2026, 9, 6, h + 4, m, s);                  // Tuesday 6 Oct 2026 on the New York / Toronto clock (EDT)
  const open = o => ses(Object.assign({ id: 'open-0001', startAt: NYT(9, 0), lastSeenAt: NYT(9, 0), lastInputAt: undefined }, o));
  const covers = (row, t) => !!LT.pickSession([row], key, t);
  assert(covers(open({ lastSeenAt: NYT(10, 0) }), NYT(10, 16)), 'a page quiet for 16 minutes still covers the sheet (the sheet being cut while nobody touches the page)');
  assert(covers(open({ lastSeenAt: NYT(10, 0) }), NYT(10, 59, 59)), '59:59 without a beat still covers');
  assert(!covers(open({ lastSeenAt: NYT(10, 0) }), NYT(11, 0)), '60:00 without a beat is a dead page the rules ended at its last beat: not covered');
  assert(covers(open({ lastSeenAt: NYT(10, 58), lastInputAt: NYT(10, 0) }), NYT(11, 0)), 'an input 60 minutes old that the page has not reported yet: the server waits for the page\'s own word, so it still covers');
  assert(!covers(open({ lastSeenAt: NYT(11, 5), lastInputAt: NYT(10, 0) }), NYT(11, 6)), 'a beat that reported 60 minutes without input ended the session AT the last input (10:00, idle): the sheet at 11:06 is not covered');
  assert(covers(open({ startAt: NYT(15, 0), lastSeenAt: NYT(17, 10), lastInputAt: NYT(17, 5) }), NYT(17, 20)), 'from 17:00 the limit is 30 minutes: input 15 minutes ago still covers');
  assert(!covers(open({ startAt: NYT(15, 0), lastSeenAt: NYT(17, 20), lastInputAt: NYT(16, 40) }), NYT(17, 21)), 'from 17:00 an input 41 minutes old ended the session at it (closing): not covered');
  assert(covers(open({ startAt: NYT(15, 0), lastSeenAt: NYT(16, 55), lastInputAt: NYT(16, 40) }), NYT(16, 59)), 'before 17:00 an input 19 minutes old covers');
  assert(!covers(open({ admin: true, lastSeenAt: NYT(10, 0) }), NYT(10, 16)), 'an Admin\'s row keeps the old rule: 15 quiet minutes closed it at its last beat');
  assert(covers(open({ station: 'sorter', role: 'laser', lastSeenAt: NYT(10, 0) }), NYT(10, 20)), 'the Sorter app\'s Laser role is judged by the Laser limits, not the default ten minutes');
  const lateStart = Date.UTC(2026, 9, 7, 3, 0);                                         // 23:00 on Tue 6 Oct, an hour before the New York midnight (04:00Z)
  assert(!covers(open({ startAt: lateStart, lastSeenAt: Date.UTC(2026, 9, 7, 3, 58) }), Date.UTC(2026, 9, 7, 4, 10)), 'past the New York midnight the sign-in is over, however fresh its beat: a sheet at 00:10 is not timed from 23:00');
  assert(covers(open({ startAt: Date.UTC(2026, 9, 7, 4, 5), lastSeenAt: Date.UTC(2026, 9, 7, 4, 25) }), Date.UTC(2026, 9, 7, 4, 30)), 'a sign-in made after the midnight covers the new day');
  // a session the rules ended idle (the end is stored at the LAST INPUT) and a new sign-in made afterwards: the sheet is the new sign-in's
  const idled = open({ id: 'idled-0001', lastSeenAt: NYT(10, 0), endAt: NYT(10, 0), endReason: 'idle' }), again = open({ id: 'again-0002', startAt: NYT(10, 20), lastSeenAt: NYT(10, 25) });
  assert(!covers(idled, NYT(10, 30)), 'a session ended idle at its last input does not cover a later sheet');
  assert.equal(LT.pickSession([idled, again], key, NYT(10, 30)).id, 'again-0002', 'a person signed in again after the idle sign-out: the new sign-in covers it');
  assert.equal(LT.pickSession([idled], key, NYT(10, 0) + 500).id, 'idled-0001', 'a sheet marked at the very moment of the sign-out still belongs to that sign-in');
  ok('an open Laser sign-in covers a sheet while the station keeps it open: a quiet page (15+ minutes), 59:59 / 60:00, input not yet reported, 30 minutes from 17:00, ended at the last input, an Admin\'s old rule, the midnight, a new sign-in after an idle one');

  // the figure of one press
  const sess = { id: 'sess-a-0001', startAt: at - 20 * MIN }, prev = { at: at - 5 * MIN, sheetId: 'p' };
  let d = LT.decide(at, { session: sess, prev }, null);
  assert.deepEqual([d.seconds, d.startedFrom, d.source, d.verified, d.disagree, d.prevSheetId], [300, 'previousSheet', 'server', true, false, 'p']);
  d = LT.decide(at, { session: sess, prev: null }, LT.cleanClient({ role: 'laser', seconds: 1203, loginAgo: 20 * MIN, prevKnown: true, startedFrom: 'login' }));
  assert.deepEqual([d.seconds, d.disagree, d.clientSeconds], [1200, false, 1203], 'a page within 10 s agrees');
  d = LT.decide(at, { session: sess, prev: null }, LT.cleanClient({ role: 'laser', seconds: 9000, loginAgo: 20 * MIN, prevKnown: true, startedFrom: 'login' }));
  assert.deepEqual([d.seconds, d.disagree, d.clientSeconds], [1200, true, 9000]); assert(/9000 s/.test(d.note) && /1200 s/.test(d.note), 'the server\'s figure is kept and the note says what the page said: ' + d.note);
  d = LT.decide(at, { session: sess, prev }, LT.cleanClient({ role: 'laser', seconds: 1200, loginAgo: 20 * MIN, prevKnown: false, startedFrom: 'login' }));
  assert.deepEqual([d.seconds, d.disagree], [300, false], 'a page that could not know the previous sheet (another computer) is a note, not a disagreement'); assert(/did not know the previous sheet/.test(d.note));
  d = LT.decide(at, { session: null, prev: null }, LT.cleanClient({ role: 'laser', seconds: 480, loginAgo: 8 * MIN, prevKnown: true, startedFrom: 'login', session: 'sess-dee-00001' }));
  assert.deepEqual([d.seconds, d.source, d.verified, d.startedFrom], [480, 'client', false, 'login'], 'no session record: the page\'s sign-in is used and flagged unverified');
  d = LT.decide(at, { session: null, prev: null }, LT.cleanClient({ role: '', seconds: null }));
  assert.deepEqual([d.seconds, d.source, d.startedFrom], [null, 'none', 'unknown'], 'no session and no claim: unknown');
  d = LT.decide(at, { session: null, prev: null }, null); assert.equal(d.seconds, null);
  assert.equal(LT.cleanClient({ role: 'laser', loginAgo: -5, prevAgo: 1e15, seconds: 'x' }).loginAgo, null, 'nonsense ages are dropped');
  ok('the figure of a press: the server\'s wins, the page\'s is kept beside it, a page that could not know is a note, no session is a flagged fallback or unknown');

  // the readers' sums: an unknown time is not in the average, fastest or slowest; and never a zero
  const row = (seconds, day, o) => Object.assign({ seconds, day: day || '2026-10-05', pieces: 3, orders: 2 }, o);
  let s = LT.summarize([row(600), row(300), row(null), row(900)]);
  assert.deepEqual([s.sheets, s.timed, s.unknown, s.avgSec, s.fastestSec, s.slowestSec, s.pieces, s.orders], [4, 3, 1, 600, 300, 900, 12, 8]);
  s = LT.summarize([row(null)]); assert.deepEqual([s.avgSec, s.fastestSec, s.slowestSec], [null, null, null], 'nothing timed: no number, not 0'); assert.deepEqual(j(LT.summarize([])).avgSec, null);
  let ser = LT.seriesOf([row(600, '2026-10-05'), row(200, '2026-10-05'), row(null, '2026-10-03')], '2026-09-30', '2026-10-06');
  assert.deepEqual(ser.map(b => [b.day, b.sheets, b.timed, b.avgSec]), [['2026-10-03', 1, 0, null], ['2026-10-05', 2, 2, 400]], 'a bucket per day with sheets; a day with none is left out; a bucket of unknowns has no average');
  ser = LT.seriesOf([row(600, '2026-10-05'), row(300, '2026-10-07'), row(100, '2026-07-02')], '2026-06-01', '2026-10-06');
  assert.deepEqual(ser.map(b => [b.day, b.days, b.avgSec]), [['2026-06-29', 7, 100], ['2026-10-05', 7, 450]], 'beyond 92 days the buckets are Monday weeks');
  ok('the readers\' sums leave out an unknown time, say null instead of 0, and bucket by day (up to 92 days) or by Monday week');
}

/** A standalone sheet that passes the Laser gate (the seed's ready sheet, cloned): `pieces` pool ids spread over `orders` orders, each order's QR label, the run's lines. Returns the order ids. */
let ORDER_SEQ = 3800000000;
function makeSheet(st, id, o = {}) {
  const S = 'Charm_Nest_Sheets', RUN = 'Charm_Nest_Runs', pre = o.sandbox ? 'Sandbox_' : '', base = st.doc(S, 'ready-sheet'), n = o.orders || 1, pieces = Math.max(n, o.pieces || 2);
  const ords = Array.from({ length: n }, () => String(++ORDER_SEQ)), per = ords.map((_, i) => Math.floor(pieces / n) + (i === 0 ? pieces % n : 0));
  const pools = ords.map((x, i) => Array.from({ length: per[i] }, (_, k) => `${x}_1_${k + 1}`));
  const d = Object.assign({}, base, { id, orders: ords, poolIds: pools.flat(), placedCount: pieces, charmCount: pieces, listings: ords.map((_, i) => String(1719000 + i)), verification: { ok: true }, label: { files: [Object.assign({}, base.label.files[0], { payload: ords[0], orders: ords })] } });
  delete d.setId; delete d.setSeq; delete d.laserDoneAt; delete d.laserDoneBy; st.docs.set(pre + S + '/' + id, d);
  const run = st.docs.get(pre + RUN + '/run-fixture') || { runId: 'run-fixture', lines: {} }, lines = Object.assign({}, run.lines);
  ords.forEach((x, i) => { lines[x + '_1'] = { orderId: x, state: 'written', quantity: per[i], poolIds: pools[i], engraveCandidate: false }; });
  st.docs.set(pre + RUN + '/run-fixture', Object.assign({}, run, { lines })); return ords;
}

/* ═══════════════════════ B · the server, end to end ═══════════════════════ */
async function partB() {
  const { start } = require('../charm-nest/bridge-server.cjs'), { seed } = require('../charm-nest/laser-workflow.cjs');
  const srv = await start({ receipts: [] }), { st } = srv; seed(srv);
  const door = require(path.join(fnDir, 'firebaseOrders.js')), eff = require(path.join(fnDir, 'employeeEfficiency.js'));
  st.fn.firebaseOrders = door.handler; st.fn.employeeEfficiency = eff.handler;
  const PASS = 'ls1-synthetic-passcode'; st.put('config', 'editPasscode', { passcode: PASS });
  const B0 = Date.UTC(2026, 9, 6, 14, 0, 0);            // Tuesday 6 Oct 2026, 10:00 New York
  let T = B0; Date.now = () => T;
  const S = 'Charm_Nest_Sheets', RUN = 'Charm_Nest_Runs', LTC = 'Laser_Sheet_Times';
  const lib = b => st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
  const doorCall = (b, sandbox) => door.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: sandbox ? { sandbox: '1' } : {}, body: JSON.stringify(b) });
  const computers = new Map();
  const sess = async (event, id, person, o = {}) => {
    if (!computers.has(id)) computers.set(id, 'pc-' + id);
    const r = await doorCall({ session: { id, event, station: o.station || 'laser', device: o.device || 'charm-nest-1', computerId: computers.get(id), person, computerLabel: 'Laser PC', reason: o.reason } }, o.sandbox);
    assert.equal(r.statusCode, 200, `${event} ${id}: ${r.body}`); return JSON.parse(r.body);
  };
  // the pages that are open keep beating (every 5 simulated minutes, as a live page does); a page that is not in `live` goes quiet and is closed after 15 minutes
  const live = new Map();
  const signIn = async (id, person, o = {}) => { const r = await sess('start', id, person, o); if (!o.quiet) live.set(id, { person, o }); return r; };
  const signOut = async (id, person, o) => { live.delete(id); return sess('end', id, person, Object.assign({ reason: 'signOut' }, o)); };
  const beat = (id, person, o) => sess('beat', id, person, o);
  const adv = async ms => { while (ms > 0) { const d = Math.min(ms, 5 * MIN); T += d; ms -= d; for (const [id, w] of live) await beat(id, w.person, w.o); } return T; };
  const mkSheet = (id, o = {}) => makeSheet(st, id, o);
  const press = async (id, by, o = {}) => {
    const r = await lib(Object.assign({ op: 'laserDone', kind: o.kind || 'sheet', id, by, stage: 'laser', device: 'charm-nest-1', via: 'Laser cutting' }, o.body || {}, o.sandbox ? { sandbox: true } : {}));
    assert.equal(r.status, 200, `${id}: ${JSON.stringify(r.body).slice(0, 240)}`); return Object.assign({ at: r.body.at }, r.body);
  };
  const undo = async (id, by) => { const r = await lib({ op: 'laserDone', kind: 'sheet', id, done: false, by, device: 'charm-nest-1', via: 'undo' }); assert.equal(r.status, 200, JSON.stringify(r.body).slice(0, 200)); return r.body; };
  const rec = (sheetId, at) => st.doc(LTC, `${sheetId}__${at}`);
  const recsOf = sheetId => st.list(LTC).filter(d => d.kind === 'laserSheetDone' && d.sheetId === sheetId);
  const only = id => { const a = recsOf(id); assert.equal(a.length, 1, `one record for ${id}, found ${a.length}`); return a[0]; };
  const eq = (d, exp) => { for (const [k, v] of Object.entries(exp)) assert.deepEqual(d[k], v, `${d.sheetId || d.id}.${k}: ${JSON.stringify(d[k])} !== ${JSON.stringify(v)}`); };
  const portal = async (b, key = PASS) => { const r = await eff.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(Object.assign({}, b, key == null ? {} : { key })) }); return { status: r.statusCode, body: JSON.parse(r.body) }; };

  try {
    /* a person at the laser: first sheet after sign-in */
    await signIn('ana-laser-0001', 'Ana M.');
    for (const id of ['sh-a1', 'sh-a2', 'sh-a3', 'sh-a4', 'sh-a5', 'sh-a6']) mkSheet(id, { pieces: 3, orders: 2 });
    await adv(11 * MIN); await beat('ana-laser-0001', 'Ana M.');
    const r1 = await press('sh-a1', 'Ana M.');
    const a1 = only('sh-a1');
    eq(a1, { kind: 'laserSheetDone', v: 1, person: 'Ana M.', personKey: 'ana m', seconds: 660, actionSeconds: 660, together: 1, startedFrom: 'login', source: 'server', verified: true, disagree: false, pieces: 3, orders: 2, prevAt: null, device: 'charm-nest-1', via: 'Laser cutting', at: r1.at, day: '2026-10-06', id: `sh-a1__${r1.at}` });
    assert.equal(a1.session, 'ana-laser-0001'); assert.equal(a1.startAt, B0); assert.equal(a1.loginAt, B0); assert(/^GF|Sheet|a1/i.test(a1.sheet) || a1.sheet, 'the sheet\'s own label is kept: ' + a1.sheet);
    assert.deepEqual(j(r1.laserTime), { at: r1.at, sheets: [{ sheetId: 'sh-a1', seconds: 660, startedFrom: 'login', source: 'server', disagree: false }] }, 'the press answers with the sheet\'s time');
    ok('first sheet after sign-in: 11 min from the sign-in, stored once on its own record with pieces, orders, who, where');
    // the order's timeline carries the same two numbers (stored, not drawn)
    const tl = st.list('Order_Timeline').filter(e => e.type === 'laserDone' && e.sheetId === 'sh-a1');
    assert(tl.length >= 1 && tl.every(e => e.data && e.data.sheetSeconds === 660 && e.data.startedFrom === 'login'), 'the order timeline event has sheetSeconds and startedFrom: ' + JSON.stringify(tl[0]));

    /* continuous sign-in: the next sheet counts from the previous one */
    await adv(7 * MIN); await beat('ana-laser-0001', 'Ana M.');
    const r2 = await press('sh-a2', 'Ana M.');
    eq(only('sh-a2'), { seconds: 420, startedFrom: 'previousSheet', prevAt: r1.at, prevSheetId: 'sh-a1', loginAt: B0, verified: true });
    ok('continuous sign-in: the second sheet counts 7 min from the first, not from the sign-in');

    /* two people at the laser: each has their own clock */
    await signIn('ben-laser-0002', 'Ben R.');
    mkSheet('sh-b1', { pieces: 1 }); for (const id of ['sh-b2', 'sh-b3', 'sh-b4']) mkSheet(id);
    await adv(9 * MIN); await beat('ana-laser-0001', 'Ana M.'); await beat('ben-laser-0002', 'Ben R.');
    const rb1 = await press('sh-b1', 'Ben R.');
    eq(only('sh-b1'), { seconds: 540, startedFrom: 'login', prevAt: null, session: 'ben-laser-0002' });
    await adv(3 * MIN); await beat('ana-laser-0001', 'Ana M.');
    const r3 = await press('sh-a3', 'Ana M.');
    eq(only('sh-a3'), { seconds: 720, startedFrom: 'previousSheet', prevAt: r2.at, prevSheetId: 'sh-a2' });       // 9 + 3 minutes after Ana's own a2, not Ben's b1
    ok('two Laser people: Ben\'s first sheet is 9 min from HIS sign-in; Ana\'s next is 12 min from HER previous sheet, not from Ben\'s');

    /* a sign-out between sheets: the new sign-in restarts the clock */
    await signOut('ana-laser-0001', 'Ana M.');
    await adv(30 * MIN); await signIn('ana-laser-0003', 'Ana M.');
    await adv(5 * MIN); await beat('ana-laser-0003', 'Ana M.');
    const r4 = await press('sh-a4', 'Ana M.');
    eq(only('sh-a4'), { seconds: 300, startedFrom: 'login', prevAt: null, session: 'ana-laser-0003' });
    await adv(4 * MIN); await beat('ana-laser-0003', 'Ana M.');
    const r5 = await press('sh-a5', 'Ana M.');
    eq(only('sh-a5'), { seconds: 240, startedFrom: 'previousSheet', prevAt: r4.at, prevSheetId: 'sh-a4' });
    ok('a sign-out and a new sign-in between sheets restarts the clock: 5 min from the new sign-in (not 35 from the old sheet), then 4 min from that sheet');

    /* no Laser sign-in: unknown, and it does not touch anyone's clock */
    mkSheet('sh-p1'); mkSheet('sh-c1'); mkSheet('sh-d1');
    await adv(2 * MIN); const rp = await press('sh-p1', 'Paul');
    eq(only('sh-p1'), { seconds: null, actionSeconds: null, startedFrom: 'unknown', source: 'none', verified: false, startAt: null, person: 'Paul', pieces: 2, orders: 1 });
    assert.deepEqual(j(rp.laserTime.sheets[0]), { sheetId: 'sh-p1', seconds: null, startedFrom: 'unknown', source: 'none', disagree: false });
    await signIn('cy-sorting-0004', 'Cy D.', { station: 'sorting', device: 'sorting-1' });
    await adv(5 * MIN); await press('sh-c1', 'Cy D.');
    eq(only('sh-c1'), { seconds: null, startedFrom: 'unknown', source: 'none' });
    await adv(3 * MIN); await beat('ana-laser-0003', 'Ana M.');
    const rA6 = await press('sh-a6', 'Ana M.');
    eq(only('sh-a6'), { seconds: 120 + 5 * 60 + 3 * 60, startedFrom: 'previousSheet', prevAt: r5.at, prevSheetId: 'sh-a5' });
    ok('a completion with no Laser sign-in (Admin with no session, a sign-in at another station) is stored as unknown, never invented, and Ana\'s clock ignores it');

    /* the page's own figure beside the server's: agree under a wrong clock, disagree, did not know the previous sheet, no session record */
    await adv(4 * MIN); await beat('ben-laser-0002', 'Ben R.');
    const benLogin = (st.doc('Station_Sessions', 'ben-laser-0002') || {}).startAt;
    assert(benLogin, 'Ben\'s session is on the server');
    const mePrev = rb1.at;                                          // Ben's previous standing completion is b1
    let cl = { v: 1, role: 'laser', session: 'ben-laser-0002', loginAgo: T - benLogin, prevAgo: T - mePrev, prevKnown: true, seconds: Math.round((T - mePrev) / 1000), startedFrom: 'previousSheet', now: T + HOUR };   // (this computer's clock is an hour fast: it sends ages, so it agrees)
    const rb2 = await press('sh-b2', 'Ben R.', { body: { laserTime: cl } });
    eq(only('sh-b2'), { seconds: Math.round((T - mePrev) / 1000), clientSeconds: cl.seconds, disagree: false, startedFrom: 'previousSheet', verified: true });
    await adv(2 * MIN);
    cl = { v: 1, role: 'laser', session: 'ben-laser-0002', loginAgo: T - benLogin, prevAgo: null, prevKnown: true, seconds: 5000, startedFrom: 'login', now: T };
    await press('sh-b3', 'Ben R.', { body: { laserTime: cl } });
    const b3 = only('sh-b3'); eq(b3, { seconds: 120, clientSeconds: 5000, disagree: true, startedFrom: 'previousSheet', source: 'server' });
    assert(/5000 s/.test(b3.note) && /server's 120 s/.test(b3.note), 'the note says what the page said and what was kept: ' + b3.note);
    await adv(1 * MIN);
    cl = { v: 1, role: 'laser', session: 'ben-laser-0002', loginAgo: T - benLogin, prevAgo: null, prevKnown: false, seconds: Math.round((T - benLogin) / 1000), startedFrom: 'login', now: T };
    await press('sh-b4', 'Ben R.', { body: { laserTime: cl } });
    const b4 = only('sh-b4'); eq(b4, { seconds: 60, disagree: false, startedFrom: 'previousSheet' }); assert(/did not know the previous sheet/.test(b4.note), b4.note);
    ok('the page\'s figure is kept beside the server\'s: a computer an hour off still agrees (ages, not clock times); a wrong figure is flagged and the server\'s kept; a page that did not know another computer\'s sheet is only a note');
    mkSheet('sh-e1'); mkSheet('sh-e2'); await adv(8 * MIN);
    await press('sh-e1', 'Dee', { body: { laserTime: { v: 1, role: 'laser', session: 'sess-dee-00001', loginAgo: 8 * MIN, prevAgo: null, prevKnown: true, seconds: 480, startedFrom: 'login', now: T } } });
    eq(only('sh-e1'), { seconds: 480, source: 'client', verified: false, startedFrom: 'login', session: 'sess-dee-00001' });
    await adv(1 * MIN); await press('sh-e2', 'Dee', { body: { laserTime: { v: 1, role: '', seconds: null, startedFrom: 'unknown' } } });
    eq(only('sh-e2'), { seconds: null, source: 'none', startedFrom: 'unknown' });
    ok('a Laser page whose start write never reached the server keeps its own sign-in time (flagged unverified); a page with no Laser role has none');
    // CHANGED 6 Oct (RG2): a Laser page that stopped beating 20 minutes ago is still signed in (Paul's Addendum 2: the Laser station signs out after 60 minutes without input, 30 from 17:00,
    // never after 15 quiet minutes; the board, the hours and the sweep all still show that person signed in): the sheet cut while nobody touched the page counts from the sign-in. It used
    // to be unknown (the old "closed after 15 quiet minutes"). Past the Laser limit the dead page is over and the sheet is unknown again.
    await signIn('quin-laser-0006', 'Quin T.', { quiet: true }); mkSheet('sh-q1'); await adv(20 * MIN);
    await press('sh-q1', 'Quin T.'); eq(only('sh-q1'), { seconds: 1200, source: 'server', startedFrom: 'login', session: 'quin-laser-0006', verified: true });
    ok('a Laser page that went quiet for 20 minutes still covers the sheet marked later: 20 minutes from the sign-in (the station keeps it open)');
    await signIn('rae-laser-0007', 'Rae V.', { quiet: true }); mkSheet('sh-r1'); mkSheet('sh-r2'); await adv(61 * MIN);
    await press('sh-r1', 'Rae V.'); eq(only('sh-r1'), { seconds: null, source: 'none', startedFrom: 'unknown' });
    await adv(1 * MIN); await signIn('rae-laser-0008', 'Rae V.'); await adv(8 * MIN); await beat('rae-laser-0008', 'Rae V.');
    await press('sh-r2', 'Rae V.'); eq(only('sh-r2'), { seconds: 480, source: 'server', startedFrom: 'login', session: 'rae-laser-0008', prevAt: null });
    ok('a Laser page silent past its 60 minute limit no longer covers a sheet (unknown, not 61 minutes); signed in again, the clock restarts at the new sign-in');
    // a session ended idle (by the page, at its last input), a sheet marked with nobody signed in, then a new sign-in: the new one's clock
    mkSheet('sh-i1'); mkSheet('sh-i2'); await signIn('ida-laser-0009', 'Ida W.'); await adv(10 * MIN); await signOut('ida-laser-0009', 'Ida W.', { reason: 'idle' });
    await adv(30 * MIN); await press('sh-i1', 'Ida W.'); eq(only('sh-i1'), { seconds: null, source: 'none', startedFrom: 'unknown' });
    await adv(1 * MIN); await signIn('ida-laser-0010', 'Ida W.'); await adv(6 * MIN); await beat('ida-laser-0010', 'Ida W.');
    await press('sh-i2', 'Ida W.'); eq(only('sh-i2'), { seconds: 360, startedFrom: 'login', session: 'ida-laser-0010', prevAt: null });
    ok('a sheet marked after an idle sign-out has no time; signed in again, it counts from the new sign-in and not from before the idle sign-out');

    /* undo and marking again: history is kept, an undone completion is not standing */
    const b1Before = j(only('sh-b1'));
    await undo('sh-b1', 'Ben R.');
    const marker = st.list(LTC).filter(d => d.kind === 'laserSheetUndone' && d.sheetId === 'sh-b1'); assert.equal(marker.length, 1); eq(marker[0], { was: rb1.at, id: `undo__sh-b1__${rb1.at}` });
    assert.deepEqual(j(only('sh-b1')), b1Before, 'the first record is never edited');
    assert(!LT.standing(st.list(LTC)).some(d => d.sheetId === 'sh-b1'), 'an undone completion is not standing');
    await adv(6 * MIN); await beat('ben-laser-0002', 'Ben R.');
    const rb1b = await press('sh-b1', 'Ben R.');
    const b1s = recsOf('sh-b1'); assert.equal(b1s.length, 2, 'marked again = a second record; the first stays');
    const b1New = b1s.find(d => d.at === rb1b.at); eq(b1New, { prevSheetId: 'sh-b4', startedFrom: 'previousSheet', seconds: (rb1b.at - only('sh-b4').at) / 1000 }); assert.equal(b1New.seconds, (8 + 1 + 20 + 61 + 1 + 8 + 10 + 30 + 1 + 6 + 6) * 60, 'counted from his last STANDING sheet (b4), not from the sheet he took back');
    assert.deepEqual(Object.assign({ _id: `sh-b1__${rb1.at}` }, j(rec('sh-b1', rb1.at))), b1Before, 'and the first record of b1 is exactly as it was');
    ok('undo writes a marker and edits nothing; marking again is a new record whose previous sheet is the last STANDING one');

    /* a retry never changes a record; a record is written once */
    const count = st.list(LTC).length, a2Before = j(only('sh-a2'));
    await press('sh-a2', 'Ana M.');
    assert.equal(st.list(LTC).length, count, 'pressing an already completed sheet writes nothing'); assert.deepEqual(j(only('sh-a2')), a2Before);
    const again = await LT.recordDone(st.db, st.admin.firestore.FieldValue, '', { by: 'Ana M.', at: rA6.at, device: 'x', via: 'y', client: null, marks: [{ sheetId: 'sh-a6', sheet: 'other', pieces: 99, orders: 9 }] });
    assert.equal(again.sheets.length, 1); assert.equal(st.list(LTC).length, count, 'the same sheet at the same moment is not written twice'); assert.equal(only('sh-a6').pieces, 3, 'and the first record is not overwritten');
    ok('a retry writes nothing and no record is overwritten');

    /* a set marked completed in one press: the sheets share the time equally, once */
    st.put(S, 'pending-sheet', { verification: { ok: true } });
    await adv(10 * MIN); await beat('ana-laser-0003', 'Ana M.');
    const rs = await press('set-fixture', 'Ana M.', { kind: 'set' });
    const rr = only('ready-sheet'), pp = only('pending-sheet');
    assert.deepEqual([rr.together, pp.together, rr.seconds, pp.seconds, rr.actionSeconds], [2, 2, rr.actionSeconds / 2, rr.actionSeconds / 2, rr.actionSeconds]);
    eq(rr, { startedFrom: 'previousSheet', prevSheetId: 'sh-a6', at: rs.at }); assert.equal(rr.actionSeconds, (rs.at - only('sh-a6').at) / 1000, 'the whole press counts from Ana\'s previous sheet (a6)'); assert.equal(rr.seconds, rr.actionSeconds / 2, 'each sheet has half');
    assert.equal(recsOf('cut-sheet').length, 0, 'a sheet that was already completed gets no record in this press');
    ok('a set marked completed in one press: its two remaining sheets share the 10 minutes equally (together: 2), the one already done has no new record');

    /* the previous sheet comes from the stored records: a "reload" or another computer gives the same answer */
    const last = await lib({ op: 'laserSheetLast', by: 'Ana M.' });
    assert.equal(last.status, 200); eq(last.body.last, { at: rs.at, startedFrom: 'previousSheet', seconds: rr.seconds }); assert.equal(last.body.now, T);
    assert.equal((await lib({ op: 'laserSheetLast', by: 'ANA m' })).body.last.at, rs.at, 'another spelling of the name is the same person');
    assert.equal((await lib({ op: 'laserSheetLast', by: 'Nobody Q.' })).body.last, null);
    assert.match((await lib({ op: 'laserSheetLast' })).body.error || '', /whose clock/i);
    await adv(2 * MIN); await beat('ana-laser-0003', 'Ana M.'); mkSheet('sh-a7');
    await press('sh-a7', 'Ana M.'); eq(only('sh-a7'), { seconds: 120, startedFrom: 'previousSheet', prevAt: rs.at });
    ok('the previous completion is read from the stored records (laserSheetLast; the next sheet counts from it with nothing held in memory)');

    /* the sandbox is its own: its sessions, its records */
    const n0 = st.list(LTC).length;
    await signIn('sandy-laser-0005', 'Sandy B.', { sandbox: true }); mkSheet('sh-s1', { sandbox: true }); mkSheet('sh-s2', { sandbox: true });
    await adv(6 * MIN); await press('sh-s1', 'Sandy B.', { sandbox: true });
    const sb = st.list('Sandbox_' + LTC).filter(d => d.sheetId === 'sh-s1'); assert.equal(sb.length, 1); eq(sb[0], { seconds: 360, startedFrom: 'login', source: 'server' });
    assert.equal(st.list(LTC).length, n0, 'nothing in the real collection'); assert.equal(st.list(LTC).filter(d => d.sheetId === 'sh-s1').length, 0);
    await adv(1 * MIN); await press('sh-s2', 'Ana M.', { sandbox: true });                  // Ana is signed in on the REAL side only
    eq(st.list('Sandbox_' + LTC).find(d => d.sheetId === 'sh-s2'), { seconds: null, startedFrom: 'unknown' });
    ok('the sandbox keeps its own records and reads only its own sessions: a real sign-in does not time a sandbox sheet');

    /* ── the portal ── */
    st.put('config', 'employeeAliases', { 'Ana M.': ['Ana Maria'] });
    mkSheet('sh-am1'); await adv(3 * MIN); await press('sh-am1', 'Ana Maria');             // another spelling with no Laser sign-in of its own
    await adv(61 * 1000);
    const today = LT.nyDay(T);
    assert.equal((await portal({ op: 'laserSheets', name: 'Ana M.', range: 'week', day: today }, null)).status, 401, 'no passcode: no answer');
    assert.equal((await portal({ op: 'laserSheets', name: 'Ana M.', range: 'week', day: today }, 'wrong')).status, 401);
    assert.equal((await portal({ op: 'laserSheets', name: '', range: 'week' })).status, 400);
    const P = await portal({ op: 'laserSheets', name: 'Ana M.', range: 'week', day: today });
    assert.equal(P.status, 200, JSON.stringify(P.body).slice(0, 200));
    const A = P.body, ana = ['sh-a1', 'sh-a2', 'sh-a3', 'sh-a4', 'sh-a5', 'sh-a6', 'sh-a7', 'ready-sheet', 'pending-sheet', 'sh-am1'];
    assert.deepEqual(A.sheets.map(r => r.sheetId).sort(), ana.slice().sort(), 'Ana\'s sheets, in every spelling the alias list merges');
    assert(A.sheets.every((r, i) => i === 0 || A.sheets[i - 1].at >= r.at), 'newest first');
    const timed = [660, 420, 720, 300, 240, 0, 120, rr.seconds, rr.seconds];       // a1 a2 a3 a4 a5 a6 a7 and the two set sheets (am1 has no time)
    const a6s = A.sheets.find(r => r.sheetId === 'sh-a6').seconds; timed[5] = a6s; assert.equal(a6s, 600);
    const avg = Math.round(timed.reduce((n, x) => n + x, 0) / timed.length);
    assert.deepEqual([A.totals.sheets, A.totals.timed, A.totals.unknown, A.totals.avgSec, A.totals.fastestSec, A.totals.slowestSec], [10, 9, 1, avg, Math.min(...timed), Math.max(...timed)], 'average, fastest and slowest of the timed sheets only');
    assert.equal(A.totals.pieces, 3 * 6 + 2 + 1 + 1 + 2, 'pieces (a1 to a6: 3 each, a7: 2, the two set sheets: 1 each, am1: 2)'); assert(A.spellings.includes('Ana M.') && A.spellings.includes('Ana Maria'));
    assert.equal(A.sheets.find(r => r.sheetId === 'sh-am1').seconds, null, 'a sheet with no time is listed with null');
    assert(A.notes.some(x => /no Laser sign-in/.test(x)), 'and the page is told why it is not in the average: ' + A.notes.join(' | '));
    assert(!A.sheets.some(r => ['sh-b1', 'sh-p1'].includes(r.sheetId) && r.person === 'Paul' && A.name === 'Ana M.'), 'nobody else\'s sheets');
    assert.equal(A.series.length, 1); eq(A.series[0], { day: today, sheets: 10, timed: 9, avgSec: avg, fastestSec: Math.min(...timed), slowestSec: Math.max(...timed) });
    ok('the portal list: this person only (aliases merged), newest first, average / fastest / slowest leave out the sheet with no time, and say why');
    const Ben = (await portal({ op: 'laserSheets', name: 'Ben R.', range: 'day', day: today })).body;
    assert.deepEqual(Ben.sheets.map(r => r.sheetId).sort(), ['sh-b1', 'sh-b2', 'sh-b3', 'sh-b4'], 'Ben\'s b1 appears once: the undone completion is left out'); assert.equal(Ben.sheets.filter(r => r.sheetId === 'sh-b1').length, 1);
    assert.equal(Ben.sheets.find(r => r.sheetId === 'sh-b1').at, rb1b.at);
    const old = (await portal({ op: 'laserSheets', name: 'Ana M.', range: { from: '2026-09-01', to: '2026-09-30' } })).body;
    assert.deepEqual([old.found, old.sheets.length, old.totals.avgSec, old.series.length], [false, 0, null, 0], 'a window with none: found false, no average (not 0)');
    assert.equal((await portal({ op: 'laserSheets', name: 'Ana M.', range: 'day', day: LT.addDays(today, -1) })).body.found, false, 'yesterday has none');
    ok('another person, an undone sheet left out, a window with no sheet says nothing (found false, null, not zero)');

    const L = (await portal({ op: 'live' })).body, laser = (L.stations || []).find(x => x.key === 'laser');
    assert(laser && laser.laserSheet, 'the live payload carries the Laser block: ' + JSON.stringify((L.stations || []).map(x => x.key)));
    const standingToday = LT.standing(st.list(LTC)).filter(d => d.day === today), tm = LT.summarize(standingToday.map(d => ({ seconds: d.seconds, pieces: d.pieces, orders: d.orders })));
    assert.deepEqual([laser.laserSheet.today.sheets, laser.laserSheet.today.timed, laser.laserSheet.today.avgSec], [tm.sheets, tm.timed, tm.avgSec], 'today\'s count and average over every Laser person');
    const newest = standingToday.sort((a, b) => b.at - a.at)[0];
    assert.deepEqual([laser.laserSheet.last.sheetId, laser.laserSheet.last.at, laser.laserSheet.last.person, laser.laserSheet.last.seconds], [newest.sheetId, newest.at, 'Ana M.', newest.seconds]);
    assert.equal(laser.laserSheet.day, today); assert(!JSON.stringify(L).includes(PASS), 'no passcode in an answer');
    ok('op live: the Laser station carries today\'s sheets, average and the last sheet (all Laser people, standing completions only)');
    st.__ls1 = { today, rb1b };
  } finally { Date.now = REAL_NOW; }
  return srv;
}

/* ═══════════════════════ C · the page's own module ═══════════════════════ */
async function partC() {
  const load = () => {
    const store = new Map();
    const ctx = vm.createContext({ console, setTimeout, clearTimeout, setInterval, clearInterval, localStorage: { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) } });
    vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-laser-time.js'), 'utf8'), ctx); return { L: ctx.LaserSheetTime, store };
  };
  const flush = () => sleep(8);
  let T = 1.8e12, lastAnswer = null, calls = 0, clientOff = 0;
  const T0 = T, who = { person: 'Ana M.', startAt: T, session: 'sess-ana-0001' };
  let role = 'laser';
  const { L, store } = load();
  L.configure({ who: () => who, role: () => role, now: () => T + clientOff, call: async b => { calls++; assert.equal(b.op, 'laserSheetLast'); return { ok: true, now: T, last: lastAnswer }; } });

  role = ''; assert.deepEqual(j(L.measure('Ana M.')), { v: 1, role: '', seconds: null, startedFrom: 'unknown' }, 'no role: no time');
  role = 'admin'; assert.equal(L.measure('Ana M.').seconds, null, 'Admin: no time');
  role = 'design'; assert.equal(L.measure('Ana M.').role, '', 'a Design sign-in: no time'); role = 'laser';
  assert.equal(L.measure('Somebody Else').seconds, null, 'a name that is not the signed-in person: no time');
  who.startAt = 0; assert.equal(L.measure('Ana M.').seconds, null, 'no sign-in time: no time'); who.startAt = T;
  ok('the page sends no time for no role, Admin, Design, another name, or no sign-in');

  await L.prepare('Ana M.');                                       // the server has no sheet of hers yet
  T += 11 * MIN; let m = L.measure('Ana M.');
  assert.deepEqual([m.role, m.seconds, m.startedFrom, m.loginAgo, m.prevAgo, m.prevKnown, m.session], ['laser', 660, 'login', 660e3, null, true, 'sess-ana-0001'], 'first sheet after sign-in: from the sign-in');
  L.noted({ laserTime: { sheets: [{ sheetId: 'sh-a1' }] } }, m, 'Ana M.');
  T += 7 * MIN; m = L.measure('Ana M.');
  assert.deepEqual([m.seconds, m.startedFrom, m.prevAgo], [420, 'previousSheet', 420e3], 'continuous sign-in: from the previous sheet'); L.noted({ laserTime: { sheets: [{ sheetId: 'sh-a2' }] } }, m, 'Ana M.');
  await flush();
  ok('first sheet after sign-in counts from it (660 s); the next counts from the previous sheet (420 s)');

  // two people have their own clocks
  const ben = { person: 'Ben R.', startAt: T - 9 * MIN, session: 'sess-ben-0002' }; Object.assign(who, ben);
  lastAnswer = null; await L.prepare('Ben R.'); m = L.measure('Ben R.');
  assert.deepEqual([m.seconds, m.startedFrom, m.prevAgo], [540, 'login', null], 'Ben\'s first sheet is from HIS sign-in, not Ana\'s sheet');
  Object.assign(who, { person: 'Ana M.', startAt: T0, session: 'sess-ana-0001' });
  T += 3 * MIN; m = L.measure('Ana M.'); assert.deepEqual([m.seconds, m.startedFrom], [3 * 60, 'previousSheet'], 'and Ana\'s next is still from her own previous sheet');
  ok('two people: each has their own clock');

  // a sign-out between sheets restarts the clock
  who.startAt = T + 30 * MIN; who.session = 'sess-ana-0003'; T += 35 * MIN;
  m = L.measure('Ana M.'); assert.deepEqual([m.seconds, m.startedFrom, m.prevAgo], [300, 'login', null], 'a new sign-in after the previous sheet: from the new sign-in');
  ok('a sign-out and a new sign-in restart the clock');

  // reload mid-way: nothing is in memory, the server says when the previous sheet was
  L.reset(); lastAnswer = { at: T - 3 * MIN, sheetId: 'sh-a4', sheet: 'x', seconds: 300, startedFrom: 'login' };
  const c0 = calls; await L.prepare('Ana M.'); assert.equal(calls, c0 + 1);
  m = L.measure('Ana M.'); assert.deepEqual([m.seconds, m.startedFrom, m.prevAgo, m.prevKnown], [180, 'previousSheet', 180e3, true], 'after a reload: the previous sheet is the one the server names');
  L.reset(); lastAnswer = { at: who.startAt - 20 * MIN, sheetId: 'old', sheet: 'x', seconds: 1, startedFrom: 'login' };
  await L.prepare('Ana M.'); m = L.measure('Ana M.'); assert.deepEqual([m.startedFrom, m.prevKnown], ['login', true], 'a sheet from before the sign-in is not the start');
  L.reset(); m = L.measure('Ana M.'); assert.equal(m.prevKnown, false, 'a page that has not been able to ask says it does not know the previous sheet');
  ok('reload mid-way: the previous sheet is read from the server (another computer\'s sheet counts), one from before the sign-in does not, and an unread page says it does not know');

  // a clock that is an hour fast or slow
  for (const off of [HOUR, -HOUR, 5 * MIN]) {
    clientOff = off; who.startAt = T - 20 * MIN + off; L.reset(); lastAnswer = { at: T - 4 * MIN, sheetId: 'a9', sheet: 'x', seconds: 1, startedFrom: 'login' };     // (the sign-in is stamped by this computer's own clock)
    await L.prepare('Ana M.'); m = L.measure('Ana M.');
    assert.equal(m.startedFrom, 'previousSheet'); assert(Math.abs(m.prevAgo - 4 * MIN) <= 20 && m.loginAgo === 20 * MIN && m.seconds === 240, `the ages survive a clock that is ${off / MIN} min off: ${JSON.stringify(m)}`);
  }
  clientOff = 0; who.startAt = T0;
  ok('a computer whose clock is an hour off (either way) still sends the right ages');

  // an answer for another person does not move this person's clock; forget() drops the memory of a taken-back sheet
  L.reset(); lastAnswer = null; await L.prepare('Ana M.'); L.noted({ laserTime: { sheets: [{ sheetId: 'zz' }] } }, L.measure('Ana M.'), 'Ana M.');
  assert(L.remembered('Ana M.') && L.remembered('Ana M.').from === 'press'); L.forget('Ana M.'); assert.equal(L.remembered('Ana M.'), null, 'a taken-back completion is forgotten');
  assert.equal(L.measure('Ana M.').prevKnown, false);
  ok('a completion taken back is forgotten (and read again)');
  assert(store.size >= 0);
}

/* ═══════════════════════ D · the real pages ═══════════════════════ */
async function partD(srv) {
  const pwDir = process.env.PW_DIR || '/opt/node22/lib/node_modules/playwright/node_modules';
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks (D) were not run'); return; }
  const { st } = srv, PASS = 'ls1-synthetic-passcode', SHOTS = process.env.SHOTS || '';
  const FV = st.admin.firestore.FieldValue, FX = require('../charm-nest/efficiency-person-fixture.cjs');
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [], nyMid = d => require(path.join(fnDir, 'employeeEfficiency.js'))._t.nyMidnight(d);
  try {
    // fixture days, on the real clock: Dana K. signs in at 9:00 each day and marks 3, 4, 5 sheets ...; one sheet marked with no Laser sign-in
    const now = REAL_NOW(), today = LT.nyDay(now);
    const day0 = k => { const d = LT.addDays(today, -k); return Date.parse(d + 'T13:00:00Z'); };       // 9 am New York (EDT) of that day
    const sessDoc = (id, person, startAt, endAt) => st.put('Station_Sessions', id, { id, person, station: 'laser', device: 'charm-nest-1', computerId: 'pc-' + id, startAt, lastSeenAt: endAt || startAt + 1000, endAt: endAt || null, endReason: endAt ? 'signOut' : null, minutes: 0 });
    let n = 0;
    const sheets = async (person, startAt, gaps, o = {}) => {
      let at = startAt; const id = 'ui-' + (++n) + '-' + LT.personKey(person).replace(/ /g, '');
      if (!o.noSession) sessDoc(id + 'sess', person, startAt, o.open ? null : startAt + 8 * HOUR);
      for (const [i, g] of gaps.entries()) { at += g * MIN; const r = await LT.recordDone(st.db, FV, '', { by: o.by || person, at, device: 'charm-nest-1', via: 'Laser cutting', client: null, marks: [{ sheetId: `${id}-s${i}`, sheet: `GF Sheet ${i + 1}`, setId: '', metal: 'gold', pieces: 6 + i, orders: 2 + (i % 2) }] }); assert.equal(r.sheets.length, 1); }
    };
    await sheets('Dana K.', day0(0), [14, 9, 22]);                // today: sign-in then three sheets (the first counts from the sign-in)
    await sheets('Dana K.', day0(1), [18, 11, 7, 25]);
    await sheets('Dana K.', day0(3), [30, 12, 16, 8, 19]);
    await sheets('Dana K.', day0(3) + 10 * HOUR, [5], { by: 'Dana K.', noSession: true });          // marked with no Laser sign-in: a dash
    await sheets('Dana K.', day0(20), [21, 13]);
    await sheets('Dana K.', day0(150), [17]);
    await sheets('Eli W.', day0(0) + 20 * MIN, [26, 31]);        // somebody else on the same day
    const ana = LT.standing(st.list('Laser_Sheet_Times')).filter(d => d.personKey === 'dana k');
    const expectWeek = ana.filter(d => d.at >= nyMid(LT.addDays(today, -6)));
    assert(expectWeek.length >= 8, 'fixture: ' + expectWeek.length);

    const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => { const u = r.request().url(); if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }); if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' }); return r.abort(); });
    const page = await ctx.newPage(); page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) errors.push('console: ' + m.text().slice(0, 200)); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.StationSession && window.LaserSheetTime && window.EfficiencyEmployee && window.EfficiencyLaser && window.EfficiencyStations && window.EfficiencyCharts && CN.S.cloud.ok === true, null, { timeout: 60000 });
    ok('the page loads the new module and the new section\'s file');

    /* D1 · the sorter's api() sends the page's figure with the press, and the stored record agrees */
    const s0 = await page.evaluate(() => LaserSheetTime.measure('Zed Q.')); assert.equal(s0.seconds, null, 'nobody is signed in: the real page sends no time');
    const login = now - 25 * MIN; sessDoc('d1-live-sess-0001', 'Zed Q.', login, null); st.put('Station_Sessions', 'd1-live-sess-0001', { lastSeenAt: now });
    const mk = id => makeSheet(st, id, { pieces: 4, orders: 1 });
    mk('sh-d1'); mk('sh-d2');
    await page.evaluate(login => { LaserSheetTime.configure({ who: () => ({ person: 'Zed Q.', startAt: login, session: 'd1-live-sess-0001' }), role: () => 'laser' }); }, login);
    const send = id => page.evaluate(async id => { const r = await CN.api('charmNestLibrary', { op: 'laserDone', kind: 'sheet', id, by: 'Zed Q.', stage: 'laser', device: 'charm-nest-1', via: 'Laser cutting' }, {}); return { laserTime: r.laserTime, at: r.at }; }, id);
    const p1 = await send('sh-d1');
    const sent = st.calls.filter(c => c.op === 'laserDone' && c.body.id === 'sh-d1').pop().body.laserTime;
    assert(sent && sent.role === 'laser' && sent.session === 'd1-live-sess-0001' && Math.abs(sent.seconds - 1500) <= 5 && sent.startedFrom === 'login' && sent.prevAgo === null, 'the real api() attached the page\'s figure: ' + JSON.stringify(sent));
    const d1 = st.list('Laser_Sheet_Times').find(d => d.sheetId === 'sh-d1'); assert(d1, 'the record exists');
    assert.deepEqual([d1.source, d1.verified, d1.disagree, d1.startedFrom], ['server', true, false, 'login']); assert(Math.abs(d1.seconds - 1500) <= 5 && Math.abs(d1.clientSeconds - d1.seconds) <= 5, `the page's ${d1.clientSeconds} and the server's ${d1.seconds} agree`);
    assert.equal(p1.laserTime.sheets[0].seconds, d1.seconds);
    await sleep(30); const t1 = await page.evaluate(() => LaserSheetTime.measure('Zed Q.')); assert(t1.prevAgo != null && t1.startedFrom === 'previousSheet', 'the page now remembers its own previous sheet: ' + JSON.stringify(t1));
    await send('sh-d2'); const d2 = st.list('Laser_Sheet_Times').find(d => d.sheetId === 'sh-d2');
    assert.equal(d2.startedFrom, 'previousSheet'); assert.equal(d2.prevSheetId, 'sh-d1'); assert(d2.seconds < 20, 'the second sheet counts from the first: ' + d2.seconds); assert(d2.disagree === false, 'and the page agreed');
    ok('real page: api() sends the page\'s figure with a laserDone, the server stores its own (they agree) and the second press counts from the first');

    /* D2 · the person page's Laser section (the real portal op over the fixture days) */
    const fx = FX.make({ now }), call = async b => {
      if (b.op === 'laserSheets') { await sleep(b.__slow ? 0 : (globalThis.__ls1delay || 0)); const r = await require(path.join(fnDir, 'employeeEfficiency.js')).handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(Object.assign({}, b, { key: PASS })) }); if (r.statusCode !== 200) throw Object.assign(new Error('laser read failed ' + r.statusCode), { status: r.statusCode }); return JSON.parse(r.body); }
      const r = fx.answer(Object.assign({}, b, { key: fx.state.key })); if (r.status !== 200) throw Object.assign(new Error('fixture ' + r.status), { status: r.status }); return r.json;
    };
    st.__calls = [];
    await page.exposeFunction('__ls1call', async b => { st.__calls.push(b.op + ':' + (b.name || '')); return call(b); });
    const mountPerson = (name, range) => page.evaluate(([name, range]) => {
      document.getElementById('ls1Host') && document.getElementById('ls1Host').remove();
      const h = document.createElement('div'); h.id = 'ls1Host'; h.style.cssText = 'position:fixed;inset:0;overflow:auto;background:var(--paper,#f6f2ea);z-index:99999;padding:16px;display:block'; document.body.appendChild(h);
      // (RG2: the person page's ORDER LIST asks through the console shell's own api.call, which here holds no passcode: every poll reached the real door as a WRONG passcode, ten of them in a
      //  minute are answered 429 to everybody at that address, and the Laser read after them was refused: a failure that depended on how fast the machine ran. It goes to the same fixture as the rest.)
      try { if (window.Efficiency && Efficiency.api) Efficiency.api.call = (b) => window.__ls1call(b); } catch (_) {}
      window.__p && window.__p.unmount(); window.__p = EfficiencyEmployee.mount(h, { name, range, call: (b) => window.__ls1call(b), onBack: () => {} }); return true;
    }, [name, range]);
    globalThis.__ls1delay = 2500;
    await mountPerson('Dana K.', 'week');
    try { await page.waitForSelector('#ls1Host .efpLs [data-wait]:not([hidden])', { timeout: 5000 }); }
    catch (e) { console.log(await page.evaluate(() => { const s = document.querySelector('#ls1Host .efpLaserS'); return s ? s.outerHTML.slice(0, 700) + ' | body.hidden=' + document.querySelector('#ls1Host .efpBody').className : 'no section'; })); throw e; }
    const waitTxt = await page.$eval('#ls1Host .efpLs [data-wait]', e => e.textContent.replace(/\s+/g, ' ').trim()); assert.match(waitTxt, /Reading laser sheets/, 'the wait says what it is waiting for');
    globalThis.__ls1delay = 0;
    await page.waitForSelector('#ls1Host .efpLaserS:not(.hidden) .efpLsR', { timeout: 15000 });
    const ui = () => page.evaluate(() => { const s = document.querySelector('#ls1Host .efpLaserS'); return { hidden: s.hidden || s.classList.contains('hidden'), n: s.querySelector('[data-n]').textContent, stats: [...s.querySelectorAll('.efpLsK')].map(k => [k.querySelector('.efpLsKL').textContent, k.querySelector('.efpLsKV').textContent]), rows: [...s.querySelectorAll('.efpLsR')].map(r => r.innerText.replace(/\s+/g, ' ').trim()), chart: !!s.querySelector('.efpLsChart svg') && !s.querySelector('.efpLsChart').hidden, label: s.querySelector('.efpLabel').innerText.replace(/\s+/g, ' ') }; });
    let U = await ui();
    assert(!U.hidden); assert.equal(+U.n, expectWeek.length, 'the count of the label is the sheets of the week: ' + U.n);
    assert.equal(U.rows.length, Math.min(15, expectWeek.length), 'a row per sheet (the first 15)');
    const badRows = U.rows.filter(r => !(/piece/.test(r) && /order/.test(r) && /(min| s| h|—)/.test(r)));
    assert.deepEqual(badRows, [], 'each row: pieces, orders, minutes');
    assert(U.rows.some(r => /From sign-in/.test(r)) && U.rows.some(r => /From previous sheet/.test(r)), 'rows say where the clock started');
    assert(!/lines?\b/i.test(U.rows.join(' ')), 'pieces, never lines');
    const tms = expectWeek.filter(d => d.seconds != null).map(d => d.seconds), avgS = Math.round(tms.reduce((a, b) => a + b, 0) / tms.length);
    const fmt = s => (s < 60 ? `${Math.round(s)} s` : s < 3570 ? `${(s / 60 < 9.95 ? Math.round(s / 6) / 10 : Math.round(s / 60))} min` : `${Math.floor(Math.round(s / 60) / 60)} h${Math.round(s / 60) % 60 ? ' ' + (Math.round(s / 60) % 60) + ' m' : ''}`);
    const stat = k => (U.stats.find(x => x[0] === k) || [])[1];
    assert.equal(stat('Average'), fmt(avgS)); assert.equal(stat('Fastest'), fmt(Math.min(...tms))); assert.equal(stat('Slowest'), fmt(Math.max(...tms))); assert.equal(stat('Sheets'), String(expectWeek.length));
    assert(U.rows.some(r => /—/.test(r) && /No time/.test(r)), 'the sheet marked with no Laser sign-in has a dash and says why');
    assert(U.chart, 'a chart of the time per day');
    ok('person page, Week: the section shows count, average, fastest, slowest, a chart and a row per sheet (pieces, orders, minutes, from sign-in or previous sheet); a labelled wait while reading; a sheet with no time is a dash');
    if (SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); await page.evaluate(() => { document.querySelector('#ls1Host .efpLaserS').scrollIntoView({ block: 'start' }); document.getElementById('ls1Host').scrollBy(0, -86); }); await sleep(500); await page.screenshot({ path: path.join(SHOTS, 'person-laser-week-1440.png') }); }

    // the date chips: Day shows one bar per sheet; Month / 3 months / Year follow
    await page.click('#ls1Host button[data-range="day"]');
    await page.waitForFunction(() => document.querySelectorAll('#ls1Host .efpLaserS .efpLsR').length > 0 && /Today/.test(document.querySelector('#ls1Host .efpLaserS [data-lr]').textContent), null, { timeout: 15000 });
    U = await ui(); const todayRows = ana.filter(d => d.day === today); assert.equal(+U.n, todayRows.length, 'Day: today\'s sheets: ' + U.n); assert(U.chart, 'one bar per sheet on a day');
    await page.click('#ls1Host button[data-range="month"]'); await page.waitForFunction(n => +document.querySelector('#ls1Host .efpLaserS [data-n]').textContent !== n, todayRows.length, { timeout: 15000 });
    U = await ui(); const monthRows = ana.filter(d => d.at >= nyMid(LT.addDays(today, -29))); assert.equal(+U.n, monthRows.length, 'Month: ' + U.n);
    await page.click('#ls1Host button[data-range="year"]'); await page.waitForFunction(n => +document.querySelector('#ls1Host .efpLaserS [data-n]').textContent !== n, monthRows.length, { timeout: 15000 });
    U = await ui(); assert.equal(+U.n, ana.length, 'Year: every sheet: ' + U.n); assert(U.chart);
    await page.click('#ls1Host button[data-range="quarter"]'); await page.waitForFunction(n => +document.querySelector('#ls1Host .efpLaserS [data-n]').textContent !== n, ana.length, { timeout: 15000 });
    U = await ui(); assert.equal(+U.n, ana.filter(d => d.at >= nyMid(LT.addDays(today, -89))).length, '3 months: ' + U.n);
    ok('the date chips (Day, Week, Month, 3 months, Year) move the Laser section with the rest of the page');
    await page.click('#ls1Host button[data-range="week"]'); await page.waitForFunction(n => +document.querySelector('#ls1Host .efpLaserS [data-n]').textContent === n, expectWeek.length, { timeout: 15000 });

    // a person who never cut a sheet has no section
    // (RG2: the section starts HIDDEN and shows only when the read says this person has a sheet, so "hidden" is also the state before the answer: the old wait took it for "answered, nothing" and
    //  read the count before the read had come back. The wait is for the section to SHOW with Eli's two; the person with no sheet waits for the read to have been asked, then a moment.)
    await mountPerson('Eli W.', 'week'); await page.waitForSelector('#ls1Host .efpLaserS', { state: 'attached' });
    await page.waitForFunction(() => { const s = document.querySelector('#ls1Host .efpLaserS'), n = s && s.querySelector('[data-n]'); return !!n && !s.classList.contains('hidden') && n.textContent === '2'; }, null, { timeout: 15000 });
    // (Eli did cut 2 sheets today: his own, never Ana's)
    U = await ui(); assert.equal(+U.n, 2, 'Eli sees his own two sheets'); assert.equal(U.rows.length, 2);
    await mountPerson('Cleo Nobody', 'week'); for (let i = 0; i < 300 && !st.__calls.includes('laserSheets:Cleo Nobody'); i++) await sleep(50); assert(st.__calls.includes('laserSheets:Cleo Nobody'), 'the page asked for Cleo\'s sheets'); await sleep(900);
    assert.equal(await page.$eval('#ls1Host .efpLaserS', s => s.hidden || s.classList.contains('hidden')), true, 'nobody cut a sheet: the section is not shown');
    ok('a person with no laser sheet in the range has no Laser section; another person sees only their own sheets');

    /* D3 · the Laser card on the stations board */
    await page.evaluate(key => { sessionStorage.setItem('cn.eff.key', key); }, PASS);
    await page.evaluate(() => {
      document.getElementById('ls1Host') && document.getElementById('ls1Host').remove();
      const h = document.createElement('div'); h.id = 'ls1Board'; h.style.cssText = 'position:fixed;inset:0;overflow:auto;background:var(--paper,#f6f2ea);z-index:99999;padding:16px'; document.body.appendChild(h);
      EfficiencyStations.options.pollMs = 600000; window.__b = EfficiencyStations.mount(h, { own: true, mode: () => 'real' });
    });
    await page.waitForSelector('#ls1Board .esSt[data-key="laser"]', { timeout: 20000 });
    await page.waitForSelector('#ls1Board .esSt[data-key="laser"] .esLs', { timeout: 20000 });
    const card = await page.$eval('#ls1Board .esSt[data-key="laser"] .esLs', e => e.innerText.replace(/\s+/g, ' ').trim());
    assert(/Last sheet/i.test(card) && /Today's average/i.test(card), 'the Laser card says the last sheet and today\'s average: ' + card);
    const todayAll = LT.standing(st.list('Laser_Sheet_Times')).filter(d => d.day === today), newest = todayAll.sort((a, b) => b.at - a.at)[0];
    const words = s => { s = Math.round(s); return s < 60 ? `${s} s` : s < 3600 ? (s % 60 ? `${Math.floor(s / 60)} m ${s % 60} s` : `${Math.floor(s / 60)} m`) : `${Math.floor(s / 3600)} h ${Math.floor(s / 60) % 60} m`; };
    assert(card.includes(words(newest.seconds)), `the last sheet's time ${words(newest.seconds)} is on the card: ${card}`);
    assert.equal(await page.$$eval('#ls1Board .esSt:not([data-key="laser"]) .esLs', e => e.length), 0, 'only the Laser card has the line');
    ok('stations board: the Laser card carries one quiet line, the last sheet\'s time and today\'s average, from the real portal op');
    if (SHOTS) { await sleep(1800); await page.evaluate(() => document.querySelector('#ls1Board .esSt[data-key="laser"]').scrollIntoView({ block: 'start' })); await sleep(300); } if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'laser-card-1440.png') });

    /* D4 · no sideways scroll on a phone */
    await page.setViewportSize({ width: 390, height: 844 });
    await page.evaluate(() => { window.__b && window.__b.unmount(); document.getElementById('ls1Board') && document.getElementById('ls1Board').remove(); });
    await mountPerson('Dana K.', 'week'); await page.waitForSelector('#ls1Host .efpLaserS:not(.hidden) .efpLsR', { timeout: 15000 });
    const sw = await page.evaluate(() => ({ doc: document.documentElement.scrollWidth, win: innerWidth, host: document.getElementById('ls1Host').scrollWidth, hostW: document.getElementById('ls1Host').clientWidth }));
    assert(sw.doc <= sw.win + 1 && sw.host <= sw.hostW + 1, 'no sideways scroll at 390 px: ' + JSON.stringify(sw));
    if (SHOTS) { await page.evaluate(() => { document.querySelector('#ls1Host .efpLaserS').scrollIntoView({ block: 'start' }); document.getElementById('ls1Host').scrollBy(0, -120); }); await sleep(500); await page.screenshot({ path: path.join(SHOTS, 'person-laser-week-390.png') }); }
    ok('390 px: no sideways scroll');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok('no page errors');
  } finally { await browser.close(); }
}

(async () => {
  console.log('A · the rule'); partA();
  console.log('B · the server, end to end'); const srv = await partB();
  console.log('C · the page\'s module'); await partC();
  console.log('D · the real pages'); try { await partD(srv); } finally { srv.close(); }
  console.log(`PASS: ${checks} checks`);
})().catch(e => { Date.now = REAL_NOW; console.error(e); process.exitCode = 1; setTimeout(() => process.exit(1), 100); });
