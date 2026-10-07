// The employee data revision (FC5, Firebase cost emergency, 7 Oct 2026): netlify/functions/_employeeRev.js and the readers that lean on it.
// Offline: an in-memory Firestore (where/orderBy/limit/select, doc get/set with increments, getAll, transactions), a faked clock, synthetic passcode.
//   1 · the revision: a writer's bump counts one change per kind, the sandbox never bumps, a failed or missing document is "unknown", never an exception
//   2 · the console overview and the live board keep what they read while the revision has not moved (nothing is read again), read it again at once when
//       a writer bumped it, and at their maximum age whatever it says (a bump that never arrived is a delay, not a stale screen)
//   3 · without a revision document (before the first bump, or when bumping fails) every reader behaves as it did: the short lifetimes alone
//   4 · the sign-out rules run on the clock: a session kept under an unmoved revision is still ended by the idle rule when its time comes
//   5 · the writers: the activity door, the live door and the session door each count their change, a plain beat counts none
//   node tests/stations/employee-rev.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');
const clone = v => v == null ? v : JSON.parse(JSON.stringify(v));

/* ── fake Firestore ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const val = v => v instanceof Ts ? v.m : v, kind = v => v instanceof Ts ? 'ts' : typeof v;
const colls = new Map(), reads = [], writes = [];
const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
const FV = { serverTimestamp: () => 'ts', increment: n => ({ __inc: n }), delete: () => null };
function merge(prev, v) { const out = Object.assign({}, prev); for (const [k, x] of Object.entries(v)) out[k] = x && typeof x === 'object' && '__inc' in x ? (typeof prev[k] === 'number' ? prev[k] : 0) + x.__inc : x === 'ts' ? Date.now() : x; return out; }
function query(name, filters, order, lim, sel) {
  return {
    where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel), orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
    limit: n => query(name, filters, order, n, sel), select: (...f) => query(name, filters, order, lim, f),
    get: async () => {
      let docs = [...data(name)].map(([id, d]) => ({ id, d }));
      for (const [f, op, v] of filters) docs = docs.filter(({ d }) => { const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false; const a = val(x), b = val(v); return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false; });
      if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
      if (lim != null) docs = docs.slice(0, lim);
      reads.push({ name, n: docs.length });
      return { docs: docs.map(({ id, d }) => ({ id, ref: ref(name, id), exists: true, data: () => clone(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
    }
  };
}
let failRev = false;
const ref = (name, id) => ({ id, name,
  get: async () => { if (name === 'Station_Rev' && failRev) throw new Error('14 UNAVAILABLE'); reads.push({ name, doc: id, n: 1 }); const d = data(name).get(id); return { exists: !!d, id, ref: ref(name, id), data: () => clone(d) }; },
  set: async (v, o) => { if (name === 'Station_Rev' && failRev) throw new Error('14 UNAVAILABLE'); writes.push([name, id]); data(name).set(id, o && o.merge ? merge(data(name).get(id) || {}, clone(v)) : clone(v)); },
  update: async v => { writes.push([name, id]); if (!data(name).has(id)) throw Object.assign(new Error('5 NOT_FOUND'), { code: 5 }); data(name).set(id, merge(data(name).get(id), clone(v))); } });
const db = {
  collection: n => Object.assign(query(n, [], null, null, null), { doc: id => ref(n, id) }),
  getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
  runTransaction: async fn => fn({ get: r => r.get(), getAll: (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())), set: (r, v, o) => r.set(v, o), update: (r, v) => r.update(v) })
};
const fakeAdmin = { firestore: Object.assign(() => db, { Timestamp: Ts, FieldValue: FV }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const PASS = 'synthetic-pass-rev1';
process.env.EDIT_PASSCODE = PASS;
const Rev = require(path.join(root, 'netlify/functions/_employeeRev.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const A = require(path.join(root, 'netlify/functions/_stationActivity.js'));
const L = require(path.join(root, 'netlify/functions/_stationLive.js'));
const AS = require(path.join(root, 'netlify/functions/_stationAutoSignout.js'));
const T = eff._t;
const logs = []; console.warn = console.log = console.error = (...a) => logs.push(a.join(' '));
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
const realNow = Date.now; let NOW = Date.parse('2026-10-07T14:00:00Z');       // 10:00 in New York
Date.now = () => NOW;
const tick = ms => { NOW += ms; };
const dayNow = () => T.nyDay(NOW);
let ip = 0;
async function call(body, who) {
  const r = await T.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (1 + (ip++ % 200)) }, body: JSON.stringify(Object.assign({ key: PASS }, body)) }, db);
  assert.strictEqual(r.statusCode, 200, r.body.slice(0, 200));
  return JSON.parse(r.body);
}
const reset = () => { reads.length = 0; writes.length = 0; };
const dataReads = (...names) => reads.filter(r => r.name !== 'Station_Rev' && r.name !== 'config' && (!names.length || names.includes(r.name))).length;
const revDoc = () => data('Station_Rev').get('employee');
const sess = (id, person, start, o = {}) => ({ id, person, employeeId: '', station: 'assembly', device: 'assembly-1', computerId: 'pc-aaaaaaaa', computerLabel: '', startAt: start, lastSeenAt: o.last || start, endAt: o.end || null, endReason: o.reason || null, minutes: 0, lastInputAt: o.input, admin: false });
const roll = (day, person, parts) => ({ day, person, v: 1, events: 3, firstAt: NOW - 3600e3, lastAt: NOW - 60e3, stations: { assembly: { scans: 2, completes: 1, parts, orders: 1, firstAt: NOW - 3600e3, lastAt: NOW - 60e3 } }, touched: { '3900000001': { assembly: true } } });

(async () => {
  /* 1 · the revision itself */
  {
    assert.deepStrictEqual(await Rev.read(db, NOW), { ok: false, missing: true }, 'no document yet: unknown');
    assert.strictEqual(await Rev.afterWrite(db, '', ['act']), true);
    assert.strictEqual(await Rev.afterWrite(db, '', ['act', 'ses', 'bogus']), true);
    tick(2000); let r = await Rev.read(db, NOW); assert.deepStrictEqual([r.ok, r.act, r.ses, r.live], [true, 2, 1, 0], 'one count per call and kind; an unknown kind is ignored');
    assert.strictEqual(await Rev.afterWrite(db, 'Sandbox_', ['act']), false, 'the sandbox never bumps'); assert.strictEqual(revDoc().act, 2);
    assert.strictEqual(await Rev.afterWrite(db, '', []), false, 'nothing to count: nothing written');
    failRev = true; assert.strictEqual(await Rev.afterWrite(db, '', ['act']), false, 'a failed bump is false, never an exception'); tick(2000); assert.deepStrictEqual(await Rev.read(db, NOW), { ok: false }, 'a failed read is unknown'); failRev = false;
    assert.strictEqual(await Rev.afterWrite(null, '', ['act']), false);
    say('1 the revision: one count per kind, the sandbox never bumps, a failed or missing document is unknown and never throws');
  }

  /* 2 · the overview and the live board keep what they read while the revision has not moved */
  {
    colls.clear(); reset();
    const today = dayNow();
    data('Efficiency_Daily').set(today + '__Ana M', roll(today, 'Ana M', 5));
    data('Station_Sessions').set('s1aaaaaaaa', sess('s1aaaaaaaa', 'Ana M', NOW - 3600e3, { last: NOW - 10e3, input: NOW - 5e3 }));
    await Rev.afterWrite(db, '', ['act', 'ses', 'live']); tick(2000);
    let o = await call({ op: 'overview', days: 1, trend: false }); assert.strictEqual(o.people.length, 1); assert.strictEqual(o.people[0].totals.parts, 5);
    await call({ op: 'live' });
    reset(); tick(8000);
    o = await call({ op: 'overview', days: 1, trend: false }); await call({ op: 'live' });
    assert.strictEqual(dataReads(), 0, 'eight seconds later, nothing was written: nothing is read (the revision probe is the only read): ' + JSON.stringify(reads));
    assert(reads.every(r => r.name === 'Station_Rev' || r.name === 'config') && reads.filter(r => r.name === 'Station_Rev').length >= 1, 'the probe is one tiny document');
    // the same rollup changed by a writer that counts: seen at once (after the 5 s answer cache)
    data('Efficiency_Daily').set(today + '__Ana M', roll(today, 'Ana M', 9)); await Rev.afterWrite(db, '', ['act']); reset(); tick(6000);
    o = await call({ op: 'overview', days: 1, trend: false }); assert.strictEqual(o.people[0].totals.parts, 9, 'a counted change is read again');
    assert(dataReads('Efficiency_Daily') >= 1, 'and only what it can change: ' + JSON.stringify(reads)); assert.strictEqual(dataReads('Station_Sessions'), 0, 'sessions did not change');
    // a change whose bump never arrived: seen at the maximum age (2 minutes) at the latest
    data('Efficiency_Daily').set(today + '__Ana M', roll(today, 'Ana M', 11)); reset(); tick(30000);
    o = await call({ op: 'overview', days: 1, trend: false }); assert.strictEqual(o.people[0].totals.parts, 9, 'a lost bump: still the kept answer inside the maximum age');
    tick(95000); o = await call({ op: 'overview', days: 1, trend: false }); assert.strictEqual(o.people[0].totals.parts, 11, 'and the truth at the maximum age');
    // the live board: a new order is read at once; a keep-alive is not a change
    reset(); tick(3000);
    await L.write(db, FV, { v: 1, event: 'work', station: 'assembly', device: 'assembly-1', person: 'Ana M', startAt: NOW, order: { kind: 'order', rid: '3900000555', scannedAt: NOW, pieces: [] } }, { prefix: '', now: NOW });
    reset(); tick(3000); const lv = await call({ op: 'live' });
    assert.strictEqual(lv.stations.find(s => s.key === 'assembly').current.length, 1, 'the order shows at once'); assert(dataReads('Station_Live') >= 1);
    say('2 the overview and the live board: kept while the revision is unmoved, read again when a writer counted a change, and at the maximum age whatever it says');
  }

  /* 3 · no revision document: the readers behave as before (short lifetimes only) */
  {
    colls.clear(); reset();
    const today = dayNow();
    data('Efficiency_Daily').set(today + '__Ana M', roll(today, 'Ana M', 5));
    await call({ op: 'overview', days: 1, trend: false }); reset(); tick(6000);
    data('Efficiency_Daily').set(today + '__Ana M', roll(today, 'Ana M', 8));
    const o = await call({ op: 'overview', days: 1, trend: false });
    assert.strictEqual(o.people[0].totals.parts, 8, 'with no revision the 5 s lifetime alone applies: a change is seen 6 s later');
    failRev = true; colls.delete('Station_Rev'); tick(6000); data('Efficiency_Daily').set(today + '__Ana M', roll(today, 'Ana M', 12));
    assert.strictEqual((await call({ op: 'overview', days: 1, trend: false })).people[0].totals.parts, 12, 'an unreadable revision is no revision'); failRev = false;
    say('3 without a revision (not there yet, or unreadable) every reader keeps its short lifetime and nothing else changes');
  }

  /* 4 · the sign-out rules run on the clock, whatever the age of the read */
  {
    colls.clear(); reset(); tick(11 * 60000);       // (the instance's kept reads of the sections above are all older than their lives)
    const today = dayNow();
    // a Laser page that beat 59 minutes 30 s ago (its limit is 60 minutes since the last beat): signed in now, out by the rule 30 s on
    data('Station_Sessions').set('s2aaaaaaaa', Object.assign(sess('s2aaaaaaaa', 'Ben T', NOW - 3 * 3600e3, { last: NOW - 3570e3, input: NOW - 3575e3 }), { station: 'laser', device: 'charm-nest-1' }));
    data('Efficiency_Daily').set(today + '__Ben T', roll(today, 'Ben T', 1));
    await Rev.afterWrite(db, '', ['act', 'ses', 'live']); tick(2000);
    let lv = await call({ op: 'live' }); assert.strictEqual(lv.signedIn.length, 1, 'inside its limit: signed in');
    await call({ op: 'overview', days: 1, trend: false });
    assert.strictEqual(data('Station_Sessions').get('s2aaaaaaaa').endAt, null, 'not ended yet');
    reset(); tick(40000);       // nothing was written (the revision is the same), so the sessions read is kept
    await call({ op: 'overview', days: 1, trend: false });
    assert.strictEqual(reads.filter(r => r.name === 'Station_Sessions' && !r.doc).length, 0, 'the sessions were not read again: ' + JSON.stringify(reads) + JSON.stringify(revDoc()));
    assert(data('Station_Sessions').get('s2aaaaaaaa').endAt > 0, 'the rule ended the session on the rows kept, as soon as its time came');
    assert.strictEqual(revDoc().ses, 2, 'and that end is a counted change (the other readers look again)');
    tick(3000); lv = await call({ op: 'live' }); assert.strictEqual(lv.signedIn.length, 0, 'the board shows nobody signed in there');
    say('4 the sign-out rules are applied again to the rows kept, on every call (no read), so a kept read never delays a rule');
  }

  /* 5 · the writers count their changes; a plain beat counts none */
  {
    colls.clear(); reset();
    const ev = (id, at) => ({ id, station: 'assembly', device: 'assembly-1', computer: 'pc-aaaaaaaa', session: 's-aaaaaaaa', person: 'Ana M', action: 'scan', orderId: '3900000777', parts: 1, at, seq: 1, sincePrevMs: 1000 });
    const out = await A.add(db, FV, [ev('e1aaaaaaaa', NOW - 1000)], {}); assert.strictEqual(out.written, 1);
    assert.strictEqual(revDoc().act, 1, 'an activity event is one counted change');
    await A.add(db, FV, [ev('e1aaaaaaaa', NOW - 1000)], {}); assert.strictEqual(revDoc().act, 1, 'a repeat of the same event changes nothing: not counted');
    await A.add(db, FV, [ev('e2aaaaaaaa', NOW - 500)], { prefix: 'Sandbox_' }); assert.strictEqual(revDoc().act, 1, 'the sandbox is not counted');
    const w = { v: 1, event: 'work', station: 'assembly', device: 'assembly-1', person: 'Ana M', startAt: NOW, order: { kind: 'order', rid: '3900000888', scannedAt: NOW, pieces: [] } };
    await L.write(db, FV, w, { prefix: '', now: NOW }); assert.strictEqual(revDoc().live, 1, 'a new order is a counted change');
    tick(20000); await L.write(db, FV, { v: 1, event: 'beat', station: 'assembly', device: 'assembly-1', person: 'Ana M' }, { prefix: '', now: NOW }); assert.strictEqual(revDoc().live, 1, 'a keep-alive beat is not');
    await L.write(db, FV, { v: 1, event: 'idle', station: 'assembly', device: 'assembly-1', person: 'Ana M', ended: { kind: 'order', rid: '3900000888', scannedAt: NOW } }, { prefix: '', now: NOW + 1 }); assert.strictEqual(revDoc().live, 2, 'an order finished is');
    // a session ended by the rules (the reader or the sweep) is a counted change
    data('Station_Sessions').set('s3aaaaaaaa', sess('s3aaaaaaaa', 'Cy', NOW - 5 * 3600e3, { last: NOW - 3 * 3600e3, input: NOW - 3 * 3600e3 }));
    const got = await AS.endOne(db, 'Station_Sessions', 's3aaaaaaaa', NOW); assert.strictEqual(got, 'ended'); assert.strictEqual(revDoc().ses, 1, 'a session the rules end is counted');
    assert.strictEqual(await AS.endOne(db, 'Station_Sessions', 's3aaaaaaaa', NOW), 'ended-by-then'); assert.strictEqual(revDoc().ses, 1, 'ended once, counted once');
    say('5 the writers: an activity event, a new or finished order and a session the rules end are counted; a repeat, a beat and the sandbox are not');
  }
  Date.now = realNow;
  say('employee-rev: all checks passed');
})().catch(e => { Date.now = realNow; process.stdout.write('FAILED: ' + (e && e.stack || e) + '\n' + logs.slice(-5).join('\n') + '\n'); process.exit(1); });
