// RU-S3: what the shop-floor STATION pages cost Firestore, per call, MEASURED (Firebase cost emergency; scope S3: assembly-*, shipping-*, weld-*,
// design*, sorting*, the phone scanners and station-*.js).
//
// Every number is read off tests/cost/meter.cjs against the REAL netlify/functions code on an in-memory Firestore seeded the way production is shaped.
// The station pages themselves hold almost no Firestore client traffic (one single-document relay listener per desk, one chat listener per OPEN order,
// the design pages' limit(1) message probes); everything else goes through the firebaseOrders door, which this test meters op by op. It then projects
// the whole shop from the idle call rates measured in a browser and prints the share of the project's overnight baseline
// (about 145,000 reads an hour, Firebase console, 18 Oct 2026) and of its 24 hour total (4.9 million reads, 77,000 writes) that the stations can account for.
//
//   node tests/cost/ru-S3-station-ops.cjs        prints the table; exits 1 when a budget below is exceeded
//
// No network, no secret, no Etsy call, no paid call.
'use strict';
const path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const meter = require('./meter.cjs');
const fn = f => path.join(root, 'netlify/functions', f);
const say = s => process.stdout.write(s + '\n');

process.env.CONTEXT = 'dev';
const m = meter.create();
const db = m.db, Timestamp = meter.Timestamp;
const T = ms => Timestamp.fromMillis(ms);
const NOW = Date.now();

/* ── production-shaped fixtures ── */
const seed = {};
seed['config/stationAdmins'] = { names: ['Paul K'] };
seed['Brites_Orders/Employee Numbers'] = { '123456': 'Test Person', '654321': 'Other Person' };
seed['Brites_Orders/3521000777'] = { 'Order Number': '3521000777', 'Client Name': 'Buyer', 'Staff Note': '', 'Brites Messages': 'x'.repeat(1500) };
// the person's rollup of the day (Efficiency_Daily): every activity event reads it and writes it back; late in a shift it is a few KB
const nyDay = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(NOW));
seed['Efficiency_Daily/' + nyDay + '__Test Person'] = { day: nyDay, person: 'Test Person', scans: 120, blob: 'y'.repeat(6000) };
seed['Brites_Orders/3521000777/messages/a'] = { text: 'hello', senderName: 'Staff', timestamp: T(NOW - 3600e3) };
seed['Brites_Orders/3521000777/messages/b'] = { text: 'again', senderName: 'Staff', timestamp: T(NOW - 1800e3) };
// the shared lock table of the Design Stations: 300 tombstones and 12 live locks
for (let i = 0; i < 300; i++) seed['Design_RealTime_Selected_Orders/' + (1000000 + i)] = { selected: false, selectedBy: null, page: null, at: T(NOW - (i + 1) * 3600e3), expireAt: new Date(NOW + 20 * 86400e3) };
for (let i = 0; i < 12; i++) seed['Design_RealTime_Selected_Orders/' + (2000000 + i)] = { selected: true, selectedBy: 'client-' + i, page: 'design', at: T(NOW - i * 60e3) };
for (let i = 0; i < 3000; i++) seed['Design_Completed Orders/' + (5000000 + i)] = { completedAt: T(NOW - i * 3600e3), by: 'x' };
// the desk presence question: 3 signed-in desks
for (let i = 1; i <= 3; i++) seed['Station_Sessions/assembly-' + i + '-OPEN' + i] = { id: 'assembly-' + i + '-OPEN' + i, person: 'P' + i, station: 'assembly', device: 'assembly-' + i, computerId: 'comp-open-' + i, startAt: NOW - 600e3, lastSeenAt: NOW - 30e3, endAt: null, lastInputAt: NOW - 30e3 };
db.seed(seed);
m.install();

const orders = require(fn('firebaseOrders.js')), archive = require(fn('designArchive.js')), auto = require(fn('_stationAutoSignout.js'));
let ipN = 0;
const ip = () => '203.0.113.' + (++ipN % 250 + 1);
const post = b => orders.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': ip() }, queryStringParameters: {}, body: JSON.stringify(b) });
const get = q => orders.handler({ httpMethod: 'GET', headers: { 'x-nf-client-connection-ip': ip() }, queryStringParameters: q, multiValueQueryStringParameters: {}, body: null });


/* ── the Design pages' polls while a person WORKS at them (every click or key restarts the back-off ladders, design.html / design-1.html "Wake the pollers on
      interaction"): the ladders are read from the pages themselves and run against a seeded click process (poll round trip 200 ms). Reads an hour. ── */
const fs = require('fs');
const ladder = (file, name) => { const mm = new RegExp('const ' + name + ' *= *\\[([0-9 ,]+)\\]').exec(fs.readFileSync(path.join(root, file), 'utf8')); assert.ok(mm, file + ' has no ' + name); return mm[1].split(',').map(Number); };
function clicks(meanMs, hours, seed) { let s = seed; const rnd = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296; const out = []; for (let t = 0; t < hours * 3600e3; ) { t += -Math.log(1 - rnd()) * meanMs; out.push(t); } return out; }
/** a ladder that restarts only when 2+ empty answers have passed (the lock poll: `if (__rtIdleHits > 1) { __rtIdleHits = 0; scheduleRtPoll(); }`) */
function lockPolls(steps, ck, hours) { let t = 0, idle = 0, polls = 0, next = steps[0], i = 0; const end = hours * 3600e3; while (t < end) { if (i < ck.length && ck[i] <= next) { t = ck[i++]; if (idle > 1) { idle = 0; next = t + steps[0]; } } else { t = next; polls++; t += 200; idle++; next = t + steps[Math.min(idle, steps.length - 1)]; } } return polls / hours; }
/** cn-poll.js wake(): a click at least 0.5 s after the last one pulls the next look forward to the first step (design.html's completion poll) */
function cnPolls(steps, ck, hours) { let t = 0, un = 0, polls = 0, next = steps[0], i = 0, last = -1e9; const end = hours * 3600e3; while (t < end) { if (i < ck.length && ck[i] <= next) { t = ck[i++]; if (t - last >= 500) { last = t; if (un !== 0) { un = 0; if (next - t > steps[0]) next = t + steps[0]; } } } else { t = next; polls++; t += 200; un++; next = t + steps[Math.min(un, steps.length - 1)]; } } return polls / hours; }
/** design-1.html's own completion loop: a click only sets the step back to 0 for the tick after the one already waiting */
function ownPolls(steps, ck, hours) { let t = 0, idle = 0, polls = 0, next = steps[0], i = 0; const end = hours * 3600e3; while (t < end) { if (i < ck.length && ck[i] <= next) { t = ck[i++]; idle = 0; } else { t = next; polls++; t += 200; idle = Math.min(idle + 1, steps.length - 1); next = t + steps[idle]; } } return polls / hours; }
const active = gap => { const ck = clicks(gap, 40, 11); return {
  d0: Math.round(lockPolls(ladder('design.html', 'RT_STEPS'), ck, 40) + 2 * cnPolls(ladder('design.html', 'COMPLETE_STEPS'), ck, 40)),                 // design.html: lock poll 1 read + dcSince 2 reads a look
  d1: Math.round(lockPolls(ladder('design-1.html', 'RT_STEPS'), ck, 40) + 2 * ownPolls(ladder('design-1.html', 'COMPLETE_STEPS'), ck, 40)) }; };   // design-1.html: lock poll 1 read + dcFor 20 ids = 2 reads a look

const rows = [];
async function one(name, call, budget) {
  const a = m.snapshot();
  const res = await m.op(name, call);
  const d = m.since(a);
  rows.push({ name, reads: d.reads + d.aggs, bytes: d.bytes, writes: d.writes, deletes: d.deletes });
  if (budget) meter.assertMax(d, budget, name);
  return res;
}
const body = r => { try { return JSON.parse(r.body); } catch (_) { return {}; } };

(async () => {
  const sid = 'assembly-1-FIXTURE1';
  const base = { id: sid, station: 'assembly', device: 'assembly-1', person: 'Test Person', computerId: 'comp-fixture-1', computerLabel: 'Bench PC' };
  // 1 · sign-in session (station-session.js): start, beat every 5 minutes, end
  let r = await one('session start (once per sign-in)', () => post({ session: Object.assign({ event: 'start', at: Date.now(), sentAt: Date.now() }, base) }), { reads: 3, writes: 3 });
  assert.strictEqual(r.statusCode, 200);
  r = await one('session beat (every 5 min per signed-in page; +1 read when the 60 s admin list has lapsed)', () => post({ session: Object.assign({ event: 'beat', at: Date.now(), sentAt: Date.now() }, base) }), { reads: 2, writes: 1 });
  assert.strictEqual(r.statusCode, 200);
  // 2 · the Employee Number login and the Admin question (once per sign-in)
  r = await one('pinLogin, a known number (once per sign-in)', () => post({ pinLogin: '123456' }), { reads: 2, writes: 0 });
  assert.strictEqual(body(r).ok, true);
  r = await one('stationAdmin door, list warm (once per sign-in)', () => post({ stationAdmin: 'Test Person' }), { reads: 1, writes: 0 });
  assert.strictEqual(body(r).ok, true);
  // 3 · one order scanned at a desk: the order, the cancel check, the timeline event, the activity event, the live card
  r = await one('scan: GET firebaseOrders?orderId= (the order document)', () => get({ orderId: '3521000777' }), { reads: 1 });
  assert.strictEqual(body(r).success, true);
  await one('scan: cancelCheck, 1 order', () => get({ cancelCheck: '3521000777' }), { reads: 1 });
  await one('scan: cancelCheck, 5 orders (a scanned batch)', () => get({ cancelCheck: '3521000777,3521000778,3521000779,3521000780,3521000781' }), { reads: 5 });
  r = await one('scan: timeline event (scan)', () => post({ timeline: [{ type: 'scan', orderId: '3521000777', station: 'assembly', device: 'assembly-1', by: 'Test Person', at: Date.now(), text: 'Scanned at Assembly 1' }] }), { reads: 0, writes: 2 });
  assert.strictEqual(r.statusCode, 200);
  let n = 0;
  const ev = () => { n++; return { id: `assembly-1_ABCD_${n}_${Date.now()}`, station: 'assembly', device: 'assembly-1', computer: 'comp-fixture-1', session: sid, person: 'Test Person', action: 'scan', orderId: '3521000777', line: '', sku: '', parts: 0, orders: 0, detail: '', at: Date.now(), seq: n, sincePrevMs: 0 }; };
  r = await one('scan: activity event (1 event, flushed within 10 s)', () => post({ activity: [ev()] }), { reads: 4, writes: 4 });
  assert.strictEqual(body(r).written, 1);
  const live = { v: 1, event: 'work', station: 'assembly', device: 'assembly-1', person: 'Test Person', computer: 'comp-fixture-1', session: sid, startAt: Date.now(), sentAt: Date.now(), order: { kind: 'order', rid: '3521000777', orderNumber: '3521000777', title: '', customer: '', scannedAt: Date.now(), pieces: [], pieceCount: 1, note: '' } };
  await one('scan: live card "work" (an order opened)', () => post({ live }), { reads: 2, writes: 4 });
  const realNow = Date.now; Date.now = () => realNow() + 31000;      // the next beat is 30 s on (the server coalesces a beat inside 15 s of the last write)
  try { await one('live card beat (every 30 s while an order is open)', () => post({ live: Object.assign({}, live, { event: 'beat' }) }), { reads: 0, writes: 1 }); } finally { Date.now = realNow; }
  // 4 · the phone scanners
  await one('phone scan: POST firebaseOrders (writes the relay document)', () => post({ orderNumber: 'assembly-scan-1', orderNumField: '3521000777', clientName: 'Scanner Page', britesMessages: '(Auto push from assembly-scan-1.html)', shippingLabelTimestamps: new Date().toISOString(), employeeName: 'ScannerBot' }), { reads: 0, writes: 1 });
  r = await one('phone scan: deskFor (is somebody signed in at the desk?)', () => get({ deskFor: 'assembly-1' }), { reads: 5, writes: 0 });
  assert.strictEqual(body(r).signedIn, true);
  // 5 · the Team's chat on an order
  await one('chat: a message sent from a station (POST newMessage)', () => post({ orderNumber: '3521000777', newMessage: 'DESIGNED :)', employeeName: 'Test Person' }), { reads: 1, writes: 3 });
  await one('chat: messagesFor, 1 order (the Design page, limit 80)', () => get({ messagesFor: '3521000777' }), { reads: 4 });
  // 6 · the Design pages: the lock table and the completion ledger
  await one('design lock poll: rtSince, nothing new (60 s at rest, 0.1 s while clicking)', () => get({ rtSince: String(NOW + 1000) }), { reads: 1 });
  await one('design lock write: select 5 orders', () => post({ rtLockIds: ['9000001', '9000002', '9000003', '9000004', '9000005'], clientId: 'c-1', page: 'design' }), { reads: 0, writes: 5 });
  await one('design lock write: release 5 orders (reads their docs for the tombstone, then the 10-minute sweep)', () => post({ rtUnlockIds: ['9000001', '9000002', '9000003', '9000004', '9000005'], clientId: 'c-1', page: 'design' }), { reads: 40, writes: 5 });
  await one('design completion poll: dcSince, nothing new (design.html: 30 s at rest)', () => get({ dcSince: String(NOW + 1000) }), { reads: 2 });
  await one('design completion check: dcFor, 20 ids (design-1.html: 60 s at rest, 10 ids a query)', () => get({ dcFor: Array.from({ length: 20 }, (_, i) => String(7000000 + i)).join(',') }), { reads: 2 });
  await one('design sweep: dcFor, 100 ids (one request of an open-order sweep)', () => get({ dcFor: Array.from({ length: 100 }, (_, i) => String(7000000 + i)).join(',') }), { reads: 10 });
  await one('design boot: rt=1 (the locks and claims held now)', () => get({ rt: '1' }), { reads: 20 });
  await one('design: mark 1 order complete (POST completedIds)', () => post({ completedIds: ['7000001'] }), { writes: 4 });
  await one('design: History opened, archive delta since=now (designArchive?op=index)', () => archive.handler({ httpMethod: 'GET', headers: { 'x-nf-client-connection-ip': ip() }, queryStringParameters: { op: 'index', limit: '400', since: String(NOW + 1000) }, body: null }), { reads: 1 });
  // 7 · the server's sign-in sweep (stationSessionsSweepCron, every 5 minutes)
  auto.resetSweep();
  const sw = await one('stationSessionsSweepCron run, 3 open sessions, both stores', () => auto.sweep({ db, force: true, now: NOW }), { reads: 6 });
  assert.ok(sw.stores && sw.stores.real && sw.stores.real.open === 4, JSON.stringify(sw));   // (the three desks above and this test's own session)
  for (let i = 1; i <= 3; i++) await db.collection('Station_Sessions').doc('assembly-' + i + '-OPEN' + i).set({ endAt: NOW, endReason: 'signOut' }, { merge: true });
  r = await one('session end (sign-out, midnight, idle: once per sign-in)', () => post({ session: Object.assign({ event: 'end', reason: 'signOut', at: Date.now(), sentAt: Date.now() }, base) }), { reads: 2, writes: 2 });
  assert.strictEqual(r.statusCode, 200);
  auto.resetSweep();
  await one('stationSessionsSweepCron run, nobody signed in (overnight), both stores', () => auto.sweep({ db, force: true, now: NOW }), { reads: 2, writes: 0 });

  say('\ncall                                                                                reads   bytes  writes deletes');
  for (const x of rows) say(x.name.padEnd(86) + String(x.reads).padStart(5) + String(x.bytes).padStart(8) + String(x.writes).padStart(8) + String(x.deletes).padStart(8));

  /* ── the whole shop, projected. ASSUMPTIONS (estimates, not measurements): 11 desktop station pages open (assembly 1-4, shipping 1-3, weld 1,
        design-message 1, sorting 1, design 1), 9 of them with a person signed in 9 hours; 12 beats an hour while signed in; 600 order scans a day over the
        shop (each: order + cancel check + timeline + activity + live work, then the relay delivery and the chat listener attach); live cards open about
        5 minutes an order; 2 Design pages visible all day and night with orders on screen (design.html's cadence for both: an upper bound); 2 designers
        working 5 hours at design.html with a click every 8 s on average. Everything else is signed out after the midnight sign-out and asks nothing. ── */
  const find = s => rows.find(x => x.name.startsWith(s));
  const beat = find('session beat (every 5 min'), sweep = find('stationSessionsSweepCron run, 3 open');
  const scan = ['scan: GET', 'scan: cancelCheck, 1', 'scan: timeline', 'scan: activity', 'scan: live card "work"'].map(find);
  const perScan = scan.reduce((a, x) => ({ reads: a.reads + x.reads, writes: a.writes + x.writes }), { reads: 0, writes: 0 });
  const A8 = active(8000), A4 = active(4000), A30 = active(30000);
  say(`\nA DESIGN PAGE WORKED AT (reads an hour, polls only; seeded clicks, ladders read from the pages): click every 30 s: design.html ${A30.d0}, design-1.html ${A30.d1} · every 8 s: ${A8.d0}, ${A8.d1} · every 4 s: ${A4.d0}, ${A4.d1}`);
  const rest = 60 * 1 + 120 * 2;                                  // a Design page at rest, visible, orders on screen: rtSince 60/h x 1 read, dcSince 120/h x 2 reads
  const beats = 9 * 9 * 12, scans = 600, liveBeats = scans * 5;   // pages signed in x hours x beats an hour · a live card open ~5 min = 10 beats, half of them past the server's 15 s coalescing
  const day = {
    beatsReads: beats * beat.reads, scanReads: scans * (perScan.reads + 2), sweepReads: 288 * 5, restReads: 2 * 24 * rest, activeReads: 2 * 5 * A8.d0,
    writes: beats * beat.writes + scans * (perScan.writes + 2) + liveBeats
  };
  day.reads = day.beatsReads + day.scanReads + day.sweepReads + day.restReads + day.activeReads;
  const overnight = 2 * rest + 288 / 24 * 2;                      // reads an hour after the midnight sign-out: two Design pages at rest + the sweep (design-1.html asks about 180 an hour, design.html 300)
  say(`SHOP MODEL: about ${Math.round(day.reads).toLocaleString()} reads and ${Math.round(day.writes).toLocaleString()} writes a day from the stations (beats ${day.beatsReads.toLocaleString()}, scans ${day.scanReads.toLocaleString()}, sweep ${day.sweepReads.toLocaleString()}, Design pages at rest ${day.restReads.toLocaleString()}, Design pages worked at ${day.activeReads.toLocaleString()})`);
  say(`  = ${(day.reads / 4.9e6 * 100).toFixed(2)} % of the 4.9 million reads and ${(day.writes / 77e3 * 100).toFixed(1)} % of the 77,000 writes of the last 24 hours (console, 18 Oct 2026)`);
  say(`  overnight (after the midnight sign-out): about ${Math.round(overnight)} reads an hour = ${(overnight / 145e3 * 100).toFixed(2)} % of the 145,000 an hour baseline`);
  assert.ok(day.reads / 4.9e6 < 0.05, 'the stations are meant to stay a small share of the project\'s reads');
  assert.ok(overnight / 145e3 < 0.01, 'a station left open overnight must stay under 1 % of the overnight baseline');
  m.uninstall();
  say('\nok');
})().catch(e => { say('FAILED ' + (e && e.stack || e)); process.exit(1); });
