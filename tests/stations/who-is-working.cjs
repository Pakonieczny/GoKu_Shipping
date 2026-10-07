// Who is working (Paul, 7 Oct 2026): "There should be 3 employees Ivy, Anna, Giovanna. I'm not seeing their presence and it looks like Anna has been duplicated with
// something called 'Anns'. You need to amalgamate this duplicate under 'Anna' ... the totals for assembly and shipping do not match the only employee that is being shown as working."
// Offline: the REAL modules over one in-memory Firestore (no network, no Etsy, no AI, no PIN, no passcode but a synthetic one), a faked clock, and the real station-session.js and
// station-scan-queue.js in a vm. Stored records are never rewritten: two spellings stay two documents and the reader adds them.
//   1  "Anns" and "Anna" (and " ANNS ", "Anns.", "Ánns") are ONE "Anna" in EVERY view: the overview (people, minutes, counts, desks, station rows), the live board (one chip),
//      the person page (either spelling), the order trace, the person's order list, the feed, the attendance reader and the Sign-ins list's name; "Ana M." and "Ann" are NOT merged
//   2  config/employeeAliases keeps working (a plain string list too), loops settle, and the built-in table still applies beside it
//   3  Ivy: a roster person with nothing today is listed "not signed in today" (a day, a past day, a week), and is not a row of `people` or a count; once she signs in she is a person
//   4  the unattributed line: a station's figure = the people + "No one signed in at the desk" - the orders two people both touched, at every station, and the console says so
//   5  a phone scan is input at its desk: the desk stays signed in while scans arrive (the real queue), is credited to the person signed in, ends at the LAST input, and the
//      10-minute rule is untouched; every desk page (assembly 1-4, shipping 1-3, weld-1, sorting, sorting-2) handles a relayed scan as input
//   6  cost: the overview and the live board read the same documents as before and write nothing; the stored documents hold no array inside an array
//   node tests/stations/who-is-working.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module'), vm = require('vm'), fs = require('fs');
const root = path.join(__dirname, '../..');
const noNested = require('../charm-nest/_noNestedArrays.cjs');

/* ── one in-memory Firestore: nested increments, merge, transactions, counted reads and writes ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const SERVER_TS = { __ts: true }, inc = n => ({ __inc: n });
const colls = new Map(), reads = [], writes = [];
const col = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Ts) && v.__inc === undefined && !v.__ts;
const clone = v => v instanceof Ts ? new Ts(v.m) : Array.isArray(v) ? v.map(clone) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
function apply(prev, patch, merge, at) {
  const out = merge && prev ? clone(prev) : {};
  for (const [k, v] of Object.entries(patch)) {
    if (v && v.__inc !== undefined) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = new Ts(at);
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge, at);
    else out[k] = clone(v);
  }
  return out;
}
const put = (c, id, d, o) => { const doc = apply(col(c).get(id), d, !!(o && o.merge), Date.now()); noNested(doc, c + '/' + id); col(c).set(id, doc); };
const kind = v => v instanceof Ts ? 'ts' : typeof v, val = v => v instanceof Ts ? v.m : v;
const refOf = (c, id) => ({ c, id, path: c + '/' + id });
const snapOf = r => { const d = col(r.c).get(r.id); reads.push({ c: r.c, doc: r.id }); return { id: r.id, exists: !!d, data: () => d ? clone(d) : undefined, ref: r }; };
function query(name, filters, order, lim, sel) {
  return {
    where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel), orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel), limit: n => query(name, filters, order, n, sel), select: (...f) => query(name, filters, order, lim, f),
    get: async () => {
      let docs = [...col(name)].map(([id, d]) => ({ id, d }));
      for (const [f, op, v] of filters) docs = docs.filter(({ d }) => { const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false; const a = val(x), b = val(v); return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<=' ? a <= b : op === '<' ? a < b : false; });
      if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
      if (lim != null) docs = docs.slice(0, lim);
      reads.push({ c: name, n: docs.length, select: sel || null });
      return { docs: docs.map(({ id, d }) => ({ id, data: () => clone(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
    }
  };
}
const fakeDb = {
  collection: c => Object.assign(query(c, [], null, null, null), { doc: id => Object.assign(refOf(c, id), {
    get: async () => snapOf(refOf(c, id)),
    set: async (v, o) => { writes.push([c, id]); put(c, id, v, o); },
    update: async v => { writes.push([c, id]); if (!col(c).has(id)) throw Object.assign(new Error('5 NOT_FOUND'), { code: 5 }); put(c, id, v, { merge: true }); } }) }),
  getAll: async (...a) => a.filter(x => x && x.c).map(snapOf),
  runTransaction: async fn => {
    const w = [], tx = { get: async r => snapOf(r), getAll: async (...rs) => rs.map(snapOf), set: (r, d, o) => { w.push([r, d, o]); return tx; } };
    const out = await fn(tx);
    for (const [r, d, o] of w) { writes.push([r.c, r.id]); put(r.c, r.id, d, o); }
    return out;
  }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { Timestamp: Ts, FieldValue: { serverTimestamp: () => SERVER_TS, increment: inc, delete: () => null } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const att = require(path.join(root, 'netlify/functions/_employeeAttendance.js'));
const KIND = require(path.join(root, 'netlify/functions/_activityKinds.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;

const PASS = 'synthetic-who-pass-4q7m';
process.env.EDIT_PASSCODE = PASS;
const realNow = Date.now;
let NOW = Date.UTC(2026, 9, 7, 15, 0);                                            // 11:00 on 7 Oct in New York (EDT)
Date.now = () => NOW;
console.warn = console.log = console.info = () => {};
const say = s => process.stdout.write(s + '\n');
let ipN = 0;
const MIN = 60000, DAY = '2026-10-07', YDAY = '2026-10-06';
const post = async payload => { const r = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, queryStringParameters: {}, body: JSON.stringify(payload) }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
const ask = async body => { const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (++ipN % 200) }, body: JSON.stringify(Object.assign({ key: PASS }, body)) }, fakeDb); assert.strictEqual(r.statusCode, 200, r.body.slice(0, 300)); return JSON.parse(r.body); };
const dropCaches = () => { const c = eff._t.cacheOf(fakeDb); c.memo.clear(); c.recent.clear(); c.fails.clear(); EP.resetCache(); };
const eq = (a, b, msg) => assert.deepStrictEqual(a, b, msg);
const J = o => JSON.parse(JSON.stringify(o));
let seq = 0;
const O = n => String(3521000000 + n);
const ev = (station, device, person, action, o = {}) => Object.assign({ id: `${device || 'old'}_${String(seq).padStart(4, '0')}_${++seq}_${NOW}`, station, device, computer: 'pc-ABCDEFGHJKMN', session: `${device || 'old'}-sess-0001`, person, action, orderId: '', line: '', sku: '', parts: 0, orders: 0, detail: '', at: NOW - 5 * MIN + seq * 1000, seq, sincePrevMs: 30000 }, o);
const sess = (id, person, station, device, from, to, extra) => put('Station_Sessions', id, Object.assign({ person, station, device, startAt: NOW - from * MIN, lastSeenAt: to == null ? NOW - 20000 : NOW - to * MIN, endAt: to == null ? null : NOW - to * MIN }, to == null ? {} : { endReason: 'idle', minutes: from - to }, extra || {}));
const sum = (a, f) => a.reduce((n, x) => n + f(x), 0);

(async () => {
  /* ═══ 1 · the shop's day: Anna typed three ways, Giovanna, nobody for Ivy, and work nobody is credited with ═══ */
  sess('s1', 'Anna', 'assembly', 'assembly-1', 180, 120);                          // 60 min
  sess('s2', 'Anns', 'assembly', 'assembly-1', 100, 70);                           // 30 min (the typo)
  sess('s3', ' ANNS ', 'shipping', 'shipping-1', 60, 40);                          // 20 min (upper case, spaces)
  sess('s4', 'Giovanna C.', 'assembly', 'assembly-2', 90, null);                   // open now (the Employee Numbers spelling)
  sess('s5', 'Anns', 'shipping', 'shipping-2', 10, null); sess('s6', 'Anna', 'shipping', 'shipping-2', 9, null);   // both spellings open at one desk, now
  const batch = [
    ev('assembly', 'assembly-1', 'Anna', 'scan', { orderId: O(1), parts: 4 }), ev('assembly', 'assembly-1', 'Anna', 'complete', { orderId: O(1), parts: 4, orders: 1 }),
    ev('assembly', 'assembly-1', 'Anna', 'scan', { orderId: O(2), parts: 3 }), ev('assembly', 'assembly-1', 'Anna', 'complete', { orderId: O(2), parts: 3, orders: 1 }),
    ev('assembly', 'assembly-1', 'Anns', 'scan', { orderId: O(3), parts: 5 }), ev('assembly', 'assembly-1', 'Anns', 'complete', { orderId: O(3), parts: 5, orders: 1 }),
    ev('assembly', 'assembly-1', 'Anns', 'scan', { orderId: O(2), parts: 0 }),                                              // Anns also scanned order 2 (Anna's): one order of ONE person
    ev('shipping', 'shipping-1', 'ANNS', 'scan', { orderId: O(4), parts: 2 }), ev('shipping', 'shipping-1', 'ANNS', 'complete', { orderId: O(4), parts: 2, orders: 1 }),
    ev('assembly', 'assembly-2', 'Giovanna C.', 'scan', { orderId: O(5), parts: 6 }), ev('assembly', 'assembly-2', 'Giovanna C.', 'complete', { orderId: O(5), parts: 6, orders: 1 }),
    ev('assembly', 'assembly-2', 'Giovanna C.', 'scan', { orderId: O(2), parts: 0 }),                                         // Giovanna touched order 2 as well: two people, one order
    ev('shipping', 'shipping-2', 'Giovanna C.', 'scan', { orderId: O(6), parts: 1 }), ev('shipping', 'shipping-2', 'Giovanna C.', 'complete', { orderId: O(6), parts: 1, orders: 1 }),
    ev('welding', 'weld-1', 'Giovanna C.', 'matched', { orderId: O(61), task: 'matching' }),
    ev('welding', 'weld-1', '', 'matched', { orderId: O(60), task: 'matching', unattributed: true })];                          // a Welding scan with nobody in Matching: stored under "Unattributed"
  const r0 = await post({ activity: batch }); assert.strictEqual(r0.status, 200, JSON.stringify(r0.body)); assert.strictEqual(r0.body.refused, 0, 'every event is accepted');
  // work at Assembly with nobody credited (a rollup under the one name "Unattributed": the same record the Welding door writes, here at a counted station)
  put('Efficiency_Daily', `${DAY}__Unattributed`, { day: DAY, person: 'Unattributed', v: 1, events: 3, firstAt: NOW - 50 * MIN, lastAt: NOW - 40 * MIN, stations: { assembly: { events: 3, scans: 2, completes: 1, parts: 3, orders: 1, firstAt: NOW - 50 * MIN, lastAt: NOW - 40 * MIN } }, touched: { [O(50)]: { assembly: true } } }, { merge: true });   // (merged into the Welding scan of nobody the door just wrote: ONE record)
  for (const who of ['Anna', 'Anns', 'ANNS', 'Giovanna C.']) assert(col('Efficiency_Daily').get(`${DAY}__${who}`), 'the door stored one rollup per SPELLING: ' + who);
  const stored = JSON.stringify([...col('Efficiency_Daily')].map(([id, d]) => [id, d.person]));
  say('1 the day is stored the way the stations write it: Anna, Anns and ANNS are three rollups of three spellings; nothing is merged on write');

  /* ── the overview ── */
  dropCaches(); reads.length = 0; writes.length = 0;
  const ov = await ask({ op: 'overview', day: DAY, days: 1, trend: false, roster: true });
  const readsOverview = reads.slice(), writesOverview = writes.slice();
  const P = n => ov.people.find(p => p.name === n);
  eq(ov.people.map(p => p.name).sort(), ['Anna', 'Giovanna'], 'ONE Anna (never Anns, ANNS or Anns.) and Giovanna; Ivy has nothing today');
  assert(!ov.people.some(p => /anns/i.test(p.name)), 'no row spelled Anns');
  eq([P('Anna').totals.parts, P('Anna').totals.orders, P('Anna').totals.signedInMin], [4 + 3 + 5 + 2, 4, 120], 'Anna = the sum of her spellings: pieces 14, orders 4 (1, 2, 3 and 4), signed in 60+30+20+10 min');
  eq(P('Anna').stations.map(s => [s.station, s.parts, s.orders]).sort(), [['assembly', 12, 3], ['shipping', 2, 1]], 'Anna per station: Assembly 12 pieces 3 orders, Shipping 2 pieces 1 order');
  eq(P('Anna').status, 'on', 'Anna is on (one of her spellings is open)'); eq(P('Anna').nowAt, ['shipping']); eq(P('Anna').nowDevices, ['shipping-2']);
  eq(P('Anna').devices.map(d => [d.device, d.minutes]).sort(), [['assembly-1', 90], ['shipping-1', 20], ['shipping-2', 10]], 'her desks and minutes, the spellings added (10 minutes at Shipping 2 once, not twice)');
  eq([P('Giovanna').totals.parts, P('Giovanna').totals.orders], [7, 3], 'Giovanna C. is Giovanna (the existing alias)');
  const bs = k => ov.business.stations.find(s => s.station === k);
  eq(bs('assembly').peopleNow, ['Giovanna'], 'Assembly now: Giovanna (Anna is at Shipping 2)'); eq(bs('shipping').peopleNow, ['Anna'], 'Shipping now: Anna, once');
  eq(bs('assembly').devices.map(d => [d.device, d.peopleNow]).filter(x => x[1].length), [['assembly-2', ['Giovanna']]], 'desk rows: the right name');
  eq(bs('shipping').devices.map(d => [d.device, d.peopleNow]).filter(x => x[1].length), [['shipping-2', ['Anna']]], 'Anns and Anna at one desk are ONE name there');
  assert(ov.feed.length && ov.feed.every(f => !/anns/i.test(f.person)), 'the feed names Anna, not Anns'); assert(ov.feed.every(f => ['Anna', 'Giovanna', 'Unattributed'].includes(f.person)), 'the feed has Anna, Giovanna and the scan of nobody (Unattributed) only');
  assert(ov.feed.some(f => f.person === 'Anna'), 'Anna has feed rows');
  say('1a overview: one Anna with the spellings summed (pieces, orders, minutes, stations, desks, now, feed); Giovanna C. still Giovanna');

  /* ── the live board ── */
  dropCaches(); reads.length = 0; writes.length = 0;
  const live = await ask({ op: 'live' });
  const readsLive = reads.slice(), writesLive = writes.slice();
  eq(live.signedIn.map(p => `${p.name}:${p.stationKey}:${p.device || ''}`).sort(), ['Anna:shipping:shipping-2', 'Giovanna:assembly:assembly-2'], 'Signed in now: Anns and Anna at one desk are ONE chip');
  assert(!JSON.stringify(live).match(/Anns|ANNS/), 'the live answer never says Anns');
  say('1b live board: one chip for Anns + Anna at one desk');

  /* ── the person page, either spelling ── */
  dropCaches();
  const pa = await ask({ op: 'person', name: 'Anna', range: 'week', day: DAY }), pb = await ask({ op: 'person', name: 'Anns', range: 'week', day: DAY }), pc = await ask({ op: 'person', name: ' ANNS ', range: 'week', day: DAY });
  eq(pa.kpis.parts.value, 14, 'the person page: Anna\'s 14 pieces are the sum of her spellings'); eq([pb.kpis.parts.value, pc.kpis.parts.value], [14, 14], 'asked as Anns or " ANNS ": the same person');
  eq(pb.kpis.orders.value, pa.kpis.orders.value);
  const old1 = await ask({ op: 'person', name: 'Anns', day: DAY, days: 2 });
  eq([old1.name, old1.totals.parts, old1.totals.signedInMin], ['Anna', 14, 120], 'the plain person read (days) names Anna and sums too');
  const ol = await ask({ op: 'personOrders', name: 'Anns', from: DAY, to: DAY, limit: 50 }), ol2 = await ask({ op: 'personOrders', name: 'Anna', from: DAY, to: DAY, limit: 50 });
  eq(ol.orders.map(o => o.rid).sort(), [O(1), O(2), O(3), O(4)], 'Anna\'s orders: those of every spelling'); eq(ol2.orders.map(o => o.rid).sort(), ol.orders.map(o => o.rid).sort());
  const tr = await ask({ op: 'orders', orderId: O(2) });
  eq(tr.steps.map(s => `${s.station}:${s.person}`).sort(), ['assembly:Anna', 'assembly:Giovanna'], 'the order trace: Anna (typed Anna and Anns: one step), Giovanna');
  eq(tr.totals.people, 2); const tr4 = await ask({ op: 'orders', orderId: O(4) }); eq(tr4.steps.map(s => s.person), ['Anna'], 'an order only ANNS touched is Anna\'s');
  say('1c person page, plain person read, order list, order trace: either spelling is Anna with the sum');

  /* ── the attendance reader and the Sign-ins list's name use the same table ── */
  const kf = att._t.keyFn(att._t.buildAliases(null));
  eq([kf('Anns'), kf(' ANNS '), kf('Anns.'), kf('Ánns'), kf('Anna')], ['anna', 'anna', 'anna', 'anna', 'anna'], 'the attendance reader folds the spellings to one person');
  eq([kf('Ana M.'), kf('Ann'), kf('Anna M')].map(k => k === 'anna'), [false, false, false], 'a different person is never merged on a guess ("Ana M.", "Ann", "Anna M")');
  eq([kf('Giovanna C.'), kf('Ivy_Y'), kf('Ivy Y.')], ['giovanna', 'ivy', 'ivy'], 'Giovanna C. and Ivy_Y (the Employee Numbers spellings) fold to Giovanna and Ivy');
  eq([KIND.personName(' ANNS '), KIND.personName('Anns'), KIND.personName('Anna'), KIND.personName('Bea K.'), KIND.personName('Ana M.')], ['Anna', 'Anna', 'Anna', 'Bea K.', 'Ana M.'], 'the Sign-ins list\'s own name: the built-in table alone, no read');
  assert(/personName\(str\(d\.person, 80\)\)/.test(fs.readFileSync(path.join(root, 'netlify/functions/charmNestLibrary.js'), 'utf8')), 'sessionsList uses it');
  eq(JSON.stringify(Object.keys(KIND.PEOPLE_ALIASES)), '["Giovanna","Anna","Ivy"]'); assert(Object.isFrozen(KIND.PEOPLE_ALIASES));
  const f = eff._t.fold; eq([f('Anns'), f(' ANNS '), f('anns.'), f('Ánns')].every(x => x === 'anns'), true);
  say('1d attendance, Sign-ins name and the built-in table: the same fold; strangers stay separate');

  /* ═══ 2 · config/employeeAliases keeps working beside the built-in table ═══ */
  put('config', 'employeeAliases', { Anna: 'Annie', Giovanna: ['Gio', 'G. C'], Ivy: ['Ivy Two'] });                    // a plain string, lists, an extra spelling of Ivy
  sess('s7', 'Annie', 'sorting', 'sorting-1', 118, 115); sess('s8', 'Gio', 'sorting', 'sorting-1', 4, 2);
  dropCaches();
  const ovC = await ask({ op: 'overview', day: DAY, days: 1, trend: false, roster: true });
  eq(ovC.people.map(p => p.name).sort(), ['Anna', 'Giovanna'], 'Annie (a plain string in the config doc) is Anna, Gio is Giovanna, and Anns still is, beside it');
  eq([ovC.people.find(p => p.name === 'Anna').totals.signedInMin, ovC.people.find(p => p.name === 'Giovanna').stations.some(s => s.station === 'sorting')], [123, true], 'the 3 minutes of Annie are Anna\'s');
  const al = eff._t.buildAliases({ Anna: 'Annie', Anns: ['Anna'] }), key = n => { let k = eff._t.fold(n); const seen = []; while (al.map.has(k) && al.map.get(k) !== k && !seen.includes(k) && seen.length < 8) { seen.push(k); k = al.map.get(k); } return seen.includes(k) ? seen.slice(seen.indexOf(k)).sort()[0] : k; };
  eq([key('Anns'), key('Anna'), key('Annie')], ['anna', 'anna', 'anna'], 'a loop in the config (Anns is Anna and Anna is Anns) settles on one person'); eq(al.display.get('anna'), 'Anna');
  eq(eff._t.buildAliases({ Anna: 7, Ivy: null, '': ['x'], Zed: 'zed' }).map.get('anns'), 'anna', 'odd config values never break the built-in table');
  col('config').delete('employeeAliases'); for (const id of ['s7', 's8']) col('Station_Sessions').delete(id);
  say('2 config/employeeAliases adds to the built-in table (string or list), loops settle, odd values are ignored');

  /* ═══ 3 · Ivy: on the roster, nothing today ═══ */
  dropCaches();
  eq(ov.absent, ['Ivy'], 'Ivy has no sign-in and no work today: the answer names her'); assert(!ov.people.some(p => p.name === 'Ivy'), 'she is not a row of people (no rankings, no counts)');
  eq(ov.business.totals.people, 2, 'the people count is the people with something');
  const wo = J(ov);
  const win = (() => { const w = { document: { getElementById: () => null, addEventListener() {}, createElement: () => ({}) }, console }; w.window = w; vm.createContext(w); return w; })();
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), win);
  const Eff = win.Efficiency;
  const M = J(Eff.norm(wo));
  eq(M.everyone.map(p => p.name).sort(), ['Anna', 'Giovanna', 'Ivy'], 'the console lists THREE people: Ivy, Anna, Giovanna'); eq(M.people.length, 2);
  const ivy = M.everyone.find(p => p.name === 'Ivy');
  eq([ivy.absent, ivy.on, ivy.t.parts, ivy.source], [true, false, 0, 'none']);
  eq([Eff.whenText(ivy, Object.assign({}, M, { past: false })), Eff.whenText(ivy, Object.assign({}, M, { past: true })), Eff.whenText(ivy, Object.assign({}, M, { days: 7 })), Eff.absentText(M)], ['Not signed in today', 'Not signed in on this day', 'Not signed in in these days', 'Not signed in today'], 'plain words for a day, a past day and a week');
  assert(!M.people.some(p => p.name === 'Ivy') && M.biz.people === 2, 'the totals and counts do not include her');
  const ovY = await ask({ op: 'overview', day: YDAY, days: 1, trend: false, roster: true });
  eq([ovY.people.length, ovY.absent.slice().sort()], [0, ['Anna', 'Giovanna', 'Ivy']], 'a day nobody worked lists all three as not signed in');
  const ov7 = await ask({ op: 'overview', day: DAY, days: 7, trend: false, roster: true }); eq(ov7.absent, ['Ivy'], 'a week in which she did nothing');
  // she signs in as "Ivy_Y" (the Employee Numbers spelling): she is a person now, and no longer listed as absent
  sess('s9', 'Ivy_Y', 'shipping', 'shipping-3', 6, null); put('Efficiency_Daily', `${DAY}__Ivy Y.`, { day: DAY, person: 'Ivy Y.', v: 1, events: 1, firstAt: NOW - 3 * MIN, lastAt: NOW - 3 * MIN, stations: { shipping: { events: 1, scans: 1, firstAt: NOW - 3 * MIN, lastAt: NOW - 3 * MIN } }, touched: { [O(7)]: { shipping: 'shipping-3' } } });
  dropCaches();
  const ovI = await ask({ op: 'overview', day: DAY, days: 1, trend: false, roster: true });
  eq([ovI.people.map(p => p.name).sort(), ovI.absent], [['Anna', 'Giovanna', 'Ivy'], []], 'Ivy_Y / "Ivy Y." is Ivy once she has signed in, and nobody is absent');
  eq([ovI.people.find(p => p.name === 'Ivy').status, ovI.people.find(p => p.name === 'Ivy').nowDevices], ['on', ['shipping-3']]);
  eq(J(Eff.norm(J(ovI))).everyone.length, 3);
  col('Station_Sessions').delete('s9'); col('Efficiency_Daily').delete(`${DAY}__Ivy Y.`);
  say('3 Ivy: listed "Not signed in today" (day, past day, week) outside the counts; a person the moment she signs in, whatever the spelling');

  /* ═══ 4 · the unattributed line: the figure is the people + nobody - the orders two people share ═══ */
  dropCaches();
  const ov4 = await ask({ op: 'overview', day: DAY, days: 1, trend: false, roster: true });
  let checked = 0;
  for (const row of ov4.business.stations) {
    const by = row.byPerson, un = row.unattributed || { parts: 0, scans: 0, orders: 0, matched: 0 };
    assert(Array.isArray(by), row.station + ' says who its figure is made of');
    eq(sum(by, b => b.parts) + un.parts, row.parts, `${row.station}: pieces = the people + no one`);
    eq(sum(by, b => b.scans) + un.scans, row.scans, `${row.station}: scans = the people + no one`);
    if (!row.taskMin) eq(sum(by, b => b.orders) + un.orders - row.shared, row.orders, `${row.station}: orders = the people + no one - the orders two people share`);
    else eq(sum(by, b => b.matched || 0) + (un.matched || 0), row.matched, `${row.station}: matched = the people + no one`);
    checked++;
  }
  assert(checked >= 4, 'every station row reconciles (' + checked + ')');
  const as = ov4.business.stations.find(s => s.station === 'assembly');
  eq([as.parts, as.orders, as.shared, as.unattributed], [12 + 6 + 3, 5, 1, { parts: 3, scans: 2, orders: 1 }], 'Assembly: Anna 12 + Giovanna 6 + no one 3 = 21 pieces; 3 + 2 + 1 orders less the one both touched = 5');
  eq(as.byPerson.map(b => [b.name, b.parts, b.orders]), [['Anna', 12, 3], ['Giovanna', 6, 2]], 'who it is made of: Anna once, with her spellings added');
  const sh = ov4.business.stations.find(s => s.station === 'shipping'); eq([sh.parts, sh.orders, sh.shared, 'unattributed' in sh], [3, 2, 0, false], 'Shipping: nothing unattributed, nothing shared: the line is only there when it has something');
  const wd = ov4.business.stations.find(s => s.station === 'welding'); eq([wd.matched, wd.unattributed.matched, wd.byPerson.map(b => [b.name, b.matched])], [2, 1, [['Giovanna', 1]]], 'Welding: the matched scan of nobody in Matching is in its figure and named');
  eq(ov4.business.unattributed, { parts: 3, scans: ov4.business.unattributed.scans, orders: 1, matched: 1 }, 'the whole line');
  eq(ov4.business.totals.parts, sum(ov4.people, p => p.totals.parts) + ov4.business.unattributed.parts, 'the business pieces = the people + no one');
  eq(ov4.business.totals.scans, sum(ov4.people, p => p.totals.scans) + ov4.business.unattributed.scans);
  assert(!ov4.people.some(p => /unattributed/i.test(p.name)), 'it is no person: no row in people');
  // what the console shows: the rows of a station's card add up, in plain words
  const M4 = Eff.norm(J(ov4)), asRow = M4.biz.stations.get('assembly'), shown = rows => J(rows.map(r => [r.k, r.v]));
  eq(shown(Eff.madeOf(asRow, 'orders')), [['Anna', '3'], ['Giovanna', '2'], ['No one signed in at the desk', '1'], ['Counted by two people', '−1']], 'the Orders card: Anna 3, Giovanna 2, No one signed in at the desk 1, counted by two people −1 = 5');
  eq(shown(Eff.madeOf(asRow, 'parts')), [['Anna', '12 pieces'], ['Giovanna', '6 pieces'], ['No one signed in at the desk', '3 pieces']]);
  eq(Eff.NOONE, 'No one signed in at the desk'); eq(M4.biz.unattributed.orders, 1);
  const rows4 = J(Eff.stationRowsOf(M4)); const asHead = rows4.find(r => r.head === 'assembly'); eq([asHead.s.parts, asHead.s.orders], [21, 5], 'the caption row of the Assembly group carries the same totals, with the same people behind it');
  eq(J(Eff.madeOf(M4.biz.stations.get('sorting') || { byPerson: [] }, 'orders')), [], 'a station nobody worked at has no breakdown');
  // an older answer (no byPerson, no unattributed, no absent) reads exactly as it did
  const olderA = J(ov4); for (const s of olderA.business.stations) { delete s.byPerson; delete s.unattributed; delete s.shared; } delete olderA.business.unattributed; delete olderA.absent;
  const MO = J(Eff.norm(olderA)); eq([MO.everyone.length, MO.biz.unattributed, MO.biz.stations.assembly === undefined], [2, null, true]);
  // without `roster: true` the answer is exactly the shape it was (other readers and tests pin it): no absent list, no breakdown, no unattributed figure; the names are merged all the same
  dropCaches(); const plain = await ask({ op: 'overview', day: DAY, days: 1, trend: false });
  eq(Object.keys(plain).sort(), ['business', 'cursor', 'day', 'days', 'delta', 'feed', 'notes', 'now', 'ok', 'people', 'sources'], 'the plain answer\'s keys'); eq(Object.keys(plain.business).sort(), ['perHour', 'stations', 'totals', 'trend']);
  assert(plain.business.stations.every(s => !('byPerson' in s) && !('shared' in s) && !('unattributed' in s)), 'plain rows carry no breakdown');
  eq(plain.people.map(p => p.name).sort(), ['Anna', 'Giovanna'], 'the plain answer has the one Anna too'); eq(plain.business.stations.find(s => s.station === 'welding').matched, 1, 'and the figures it always had');
  say('4 unattributed: every station row = people + "No one signed in at the desk" - shared; the cards say it; an older answer reads as before');

  /* ═══ 5 · a phone scan is input at its desk ═══ */
  const HOUR = 3600000, Z0 = Date.UTC(2026, 9, 6, 14, 0);                          // Tuesday 10:00 in Toronto
  function world() {
    const clock = { t: Z0, timers: [], seq: 1 }, posts = [], store = new Map(), signOuts = [], docL = {}, winL = {};
    class FDate extends Date { constructor(...a) { if (a.length) super(...a); else super(clock.t); } static now() { return clock.t; } }
    const winO = { addEventListener(t, fn) { (winL[t] = winL[t] || []).push(fn); }, removeEventListener() {}, postMessage() {}, fire(type, extra) { for (const fn of winL[type] || []) fn(Object.assign({ type, isTrusted: true }, extra)); } };
    const doc = { readyState: 'complete', visibilityState: 'visible', hidden: false, addEventListener(t, fn) { (docL[t] = docL[t] || []).push(fn); }, querySelector: () => null, getElementsByTagName: () => [], createElement: () => ({ style: {}, appendChild() {}, addEventListener() {}, querySelector: () => null }) };
    const fetchFake = (url, init = {}) => {
      if ((init.method || 'GET').toUpperCase() !== 'POST') return Promise.resolve({ status: 404, ok: false, json: async () => ({}) });
      const b = JSON.parse(init.body || '{}');
      if (b.stationAdmin !== undefined) return Promise.resolve({ status: 200, ok: true, json: async () => ({ ok: true, admin: false }) });
      posts.push(b); return Promise.resolve({ status: 200, ok: true, json: async () => ({ success: true }) });
    };
    const timers = clock.timers;
    const sb = { window: winO, document: doc, localStorage: { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => { store.set(k, String(v)); }, removeItem: k => { store.delete(k); } }, sessionStorage: { getItem: () => null, setItem() {}, removeItem() {} },
      navigator: { onLine: true, sendBeacon: () => true }, location: { search: '' }, fetch: fetchFake, Intl, Promise, JSON, Math, Set, Map, Uint32Array, Array, Object, String, Number, RegExp, Error, Blob: class {}, AbortController, console: { warn() {}, log() {}, error() {} }, Date: FDate,
      setTimeout: (fn, ms) => { const id = clock.seq++; timers.push({ id, at: clock.t + Math.max(0, ms || 0), fn }); return id; }, setInterval: (fn, ms) => { const id = clock.seq++; timers.push({ id, at: clock.t + ms, every: ms, fn }); return id; },
      clearTimeout: id => { const i = timers.findIndex(x => x.id === id); if (i >= 0) timers.splice(i, 1); } };
    sb.clearInterval = sb.clearTimeout; winO.self = winO; winO.top = winO; winO.parent = winO;
    Object.defineProperty(sb, 'StationSession', { get: () => winO.StationSession, configurable: true }); Object.defineProperty(sb, 'StationScanQueue', { get: () => winO.StationScanQueue, configurable: true });
    const ctx = vm.createContext(sb);
    vm.runInContext(fs.readFileSync(path.join(root, 'station-session.js'), 'utf8'), ctx, { filename: 'station-session.js' }); vm.runInContext(fs.readFileSync(path.join(root, 'station-scan-queue.js'), 'utf8'), ctx, { filename: 'station-scan-queue.js' });
    const ss = winO.StationSession;
    const w = { clock, posts, signOuts, ss, win: winO, store,
      advance(ms) { const to = clock.t + ms; for (;;) { const nx = timers.filter(x => x.at <= to).sort((a, b) => a.at - b.at || a.id - b.id)[0]; if (!nx) break; clock.t = Math.max(clock.t, nx.at); if (nx.every) nx.at += nx.every; else timers.splice(timers.indexOf(nx), 1); nx.fn(); } clock.t = to; },
      init(station, device) { ss.init({ station, device, person: () => (store.get('employee_id') && store.get('employee_name') ? { name: store.get('employee_name'), id: null } : null), signOut: (reason, who) => { signOuts.push({ reason, who, at: clock.t }); store.delete('employee_id'); store.delete('employee_name'); } }); return w; },
      signIn(name) { store.set('employee_id', 'x'); store.set('employee_name', name); ss.signedIn({ name, id: null }); },
      who: () => store.get('employee_name') || '', beats: () => posts.filter(b => b.session && b.session.event === 'beat').map(b => b.session), ends: () => posts.filter(b => b.session && b.session.event === 'end').map(b => b.session) };
    return w;
  }
  const settleTicks = async () => { for (let i = 0; i < 6; i++) await new Promise(r => setImmediate(r)); };
  {
    const w = world().init('assembly', 'assembly-1'); w.signIn('Anna'); await settleTicks();
    const ran = [], q = w.win.StationScanQueue.create({ device: 'assembly-1', signedIn: () => !!w.store.get('employee_id'), run: async (order, item) => { ran.push({ order, by: w.who(), at: w.clock.t }); } });
    // a phone scan arrives every 4 minutes for 40 minutes, and nobody touches the desktop page (the page's relay handler calls q.offer, which is input)
    for (let i = 1; i <= 10; i++) { w.advance(4 * MIN); assert.strictEqual(q.offer('352100' + String(1000 + i)), true, 'the scan is kept'); await settleTicks(); }
    eq(w.signOuts.length, 0, 'Anna is still signed in 40 minutes later: every scan was input at her desk (without it she is out after 10 minutes)');
    eq(w.ss.lastInput(), Z0 + 40 * MIN, 'the last input is the last scan'); eq(ran.length, 10, 'all ten scans loaded'); eq([...new Set(ran.map(r => r.by))], ['Anna'], 'each one credited to the person signed in at the desk');
    const lastBeat = w.beats().pop(); assert(lastBeat && lastBeat.lastInputAt >= Z0 + 35 * MIN, 'the beats the server judges her by carry the scans as lastInputAt (' + (lastBeat && (lastBeat.lastInputAt - Z0) / MIN) + ' min)');
    // the rule is untouched: ten minutes with no input at all and she is out, ended at her LAST input (the last scan), not at the time of the check
    w.advance(9 * MIN + 59000); eq(w.signOuts.length, 0, '9 min 59 s after the last scan: still in'); w.advance(2000); w.win.fire('visibilitychange'); w.advance(1000);
    eq(w.signOuts.map(s => s.reason), ['idle'], '10 minutes after the last scan: out (idle)'); eq(w.ends()[0] && w.ends()[0].at, Z0 + 40 * MIN, 'ended at her last input');
    // a scan at a desk nobody is signed in at is KEPT (never dropped, never credited to Anna) and loads under whoever signs in next
    assert.strictEqual(q.offer('3521009999'), true); await settleTicks(); eq([ran.length, q.count()], [10, 1], 'nobody signed in: the scan waits');
    w.signIn('Giovanna'); q.drain(); await settleTicks(); eq([ran.length, ran[10].order, ran[10].by, q.count()], [11, '3521009999', 'Giovanna', 0], 'it loads right after the next sign-in, under that person');
  }
  {   // Welding keeps its own rule: no idle sign-out at all, so a scan changes nothing there (17:00 sharp is the rule)
    const w = world().init('welding', 'weld-1'); w.signIn('Giovanna'); await settleTicks();
    w.advance(3 * HOUR); w.win.fire('visibilitychange'); eq(w.signOuts.length, 0, 'Welding: still in after 3 hours with no input (the sign-out table is as Paul set it)');
    eq(J([w.ss.policy('welding'), w.ss.policy('assembly'), w.ss.policy('shipping'), w.ss.policy('laser')].map(p => [p.idleMin, p.closeAt17])), [[0, 'always'], [10, 'idleWindow'], [10, 'idleWindow'], [60, 'idleWindow']], 'the sign-out table as Paul set it: Welding no idle rule and 17:00 sharp, Assembly and Shipping 10 minutes, Laser 1 hour');
  }
  // every desk page that receives a relayed scan treats it as input: through the page's own touch call or through the queue (which touches)
  const pageSrc = f => fs.readFileSync(path.join(root, f), 'utf8');
  for (const f of ['assembly-1', 'assembly-2', 'assembly-3', 'assembly-4', 'shipping-1', 'shipping-2', 'shipping-3', 'sorting', 'sorting-2'].map(n => n + '.html')) assert(/StationSession\.touch\(\)|StationSession\.touch &&|StationSession\.touch\b/.test(pageSrc(f)), f + ' treats a relayed phone scan as input (StationSession.touch)');
  assert(/scanQueue\.offer\(/.test(pageSrc('weld-1.html')) && /S\.touch\(\)/.test(pageSrc('station-scan-queue.js')), 'weld-1 offers the scan to the queue, which touches the session');
  for (const f of ['assembly-1', 'assembly-2', 'assembly-3', 'assembly-4', 'shipping-1', 'shipping-2', 'shipping-3'].map(n => n + '.html')) assert(/scanQueue\.offer\(/.test(pageSrc(f)), f + ' keeps a scan at a signed-out desk in the queue');
  say('5 a phone scan is input: the desk stays signed in, the scan is credited to the person, the last input ends the session, the rules stand (10 min, Welding none, Laser 1 h)');

  /* ═══ 6 · cost ═══ */
  const by = (list, c) => list.filter(r => r.c === c).length;
  // (measured on main before this change, with the same fixture: 2 queries on the rollups, 2 on the sessions, 1 on the newest events, 1 revision document, 2 config documents; this change reads exactly that)
  eq([by(readsOverview, 'Efficiency_Daily'), by(readsOverview, 'Station_Sessions'), by(readsOverview, 'Station_Activity'), readsOverview.length], [2, 2, 1, 8], 'the overview reads what it read before (the people, the roster, the unattributed line and the station breakdown all come from those reads)');
  eq(writesOverview.filter(w => !/^Station_Rev$|^Station_Sessions$/.test(w[0])).length, 0, 'the overview writes nothing'); eq(writesLive.filter(w => !/^Station_Rev$|^Station_Sessions$/.test(w[0])).length, 0, 'the live board writes nothing');
  eq([by(readsLive, 'Station_Sessions'), by(readsLive, 'Efficiency_Daily')], [1, 1], 'the live board: one read of today\'s sessions and one of the rollups, as before');
  // no new poll and no new listener: the console's own timers and requests are the ones it had (the code adds none)
  const src = fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), base = require('child_process').spawnSync('git', ['show', 'origin/main:charm-nest-efficiency.js'], { cwd: root, encoding: 'utf8' }).stdout || '';
  if (base) { const cnt = (s, re) => (s.match(re) || []).length; for (const re of [/setInterval\(/g, /setTimeout\(/g, /addEventListener\(/g, /\bapi\(\{/g, /CN\.api\(/g, /onSnapshot\(/g]) assert(cnt(src, re) <= cnt(base, re), 'the console adds no ' + re.source); }
  for (const [c, m] of colls) for (const [id, d] of m) noNested(d, c + '/' + id);
  void stored;
  say('6 cost: the overview and the live board read exactly what they read before, nothing written, no poll or listener added, no nested arrays stored');
  say('OK');
})().then(() => { Date.now = realNow; }, e => { Date.now = realNow; process.stdout.write('FAIL ' + (e && e.stack || e) + '\n'); process.exit(1); });
