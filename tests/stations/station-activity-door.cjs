// The activity door (firebaseOrders {activity} + _stationActivity.js): events created once, the person-day rollup counted in
// the same transaction, so a retry or a double send never counts twice. Fakes only: Firestore is a Map with transactions,
// nested merges and increments; the clock is faked to cross New York midnight (summer and winter).
//   1 · rollup math (scans, scanParts, completes, parts, orders, prints, undo, rejects, errors, notes, active/idle gaps,
//       hours, touched, firstAt/lastAt), two people, a name with "/"
//   2 · idempotency: the same batch twice, one id twice in a request, an overlap batch (only the new one counts)
//   3 · the New York day: 23:59:59 and 00:00:01 in EDT and in EST land on different days and hours, an event that was
//       queued over midnight keeps its own day
//   4 · no PIN: a digits-only name is refused; digit ids and details are blanked; a 6-digit number inside a detail is
//       hidden; nothing digits-only leaks into any stored document; fields that are not the contract's are dropped
//   5 · limits: 50 events and 600 bytes and 40000 characters, vocabulary, stale and future clocks, flood, sandbox
//   NODE_PATH=… node tests/stations/station-activity-door.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

/* a Map-backed Firestore: transactions apply their writes at the end, set({merge}) merges nested maps, increment adds */
const docs = new Map();
const INC = n => ({ __inc: n }), TS = { __ts: true };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !v.__inc && !v.__ts;
function apply(prev, data, merge) {
  const out = merge && prev ? JSON.parse(JSON.stringify(prev)) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v && v.__inc != null) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = 'TS';
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge);
    else out[k] = v;
  }
  return out;
}
let txRuns = 0;
const ref = p => ({ path: p, id: p.split('/').pop() });
const snap = r => ({ exists: docs.has(r.path), data: () => docs.get(r.path), ref: r });
const fakeDb = {
  collection: c => ({ doc: id => ref(c + '/' + id) }),
  runTransaction: async fn => {
    txRuns++;
    const writes = [];
    const out = await fn({ getAll: (...rs) => Promise.resolve(rs.map(snap)), get: r => Promise.resolve(snap(r)), set: (r, d, o) => writes.push([r.path, d, o]) });
    for (const [p, d, o] of writes) docs.set(p, apply(docs.get(p), d, !!(o && o.merge)));
    return out;
  }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { FieldValue: { serverTimestamp: () => TS, increment: INC, delete: () => null } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const fn = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
Module._load = realLoad;

const realNow = Date.now; let now = Date.parse('2026-10-02T15:00:00Z');   // 11:00 in New York (EDT)
Date.now = () => now;
let ipN = 0;
const send = (activity, { ip = '203.0.113.' + (++ipN % 250), sandbox = false, raw } = {}) => fn.handler({
  httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': ip }, queryStringParameters: sandbox ? { sandbox: '1' } : {},
  body: raw != null ? raw : JSON.stringify({ activity })
}).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
let n = 0;
const E = (o = {}) => { n++; return Object.assign({ id: `weld-1_ABCD_${n}_${now}`, station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', session: 'weld-1-ABCD-k1-XYZW',
  person: 'Tess Welder', action: 'scan', orderId: '3521000777', line: '', sku: '', parts: 0, orders: 0, detail: '', at: now, seq: n, sincePrevMs: 0 }, o); };
const roll = (day, person) => docs.get(`Efficiency_Daily/${day}__${person}`);
const acts = () => [...docs.entries()].filter(([k]) => k.startsWith('Station_Activity/')).map(([, v]) => v);
const reset = () => { docs.clear(); txRuns = 0; };

(async () => {
  /* 1 · rollup math */
  {
    reset();
    const t0 = Date.parse('2026-10-02T13:05:00Z');            // 09:05 EDT
    const batch = [
      E({ action: 'scan', at: t0, parts: 1, sincePrevMs: 0, orderId: '3521000777' }),
      E({ action: 'scan', at: t0 + 60000, parts: 12, sincePrevMs: 60000, orderId: '3521000888' }),
      E({ action: 'complete', at: t0 + 4 * 60000, parts: 12, orders: 1, sincePrevMs: 3 * 60000, orderId: '3521000888' }),
      E({ action: 'print', at: t0 + 11 * 60000, sincePrevMs: 7 * 60000 }),             // > 5 min: idle
      E({ action: 'undo', at: t0 + 12 * 60000, parts: 2, orders: 1, sincePrevMs: 60000 }),
      E({ action: 'reject', at: t0 + 13 * 60000, sincePrevMs: 60000 }),
      E({ action: 'error', at: t0 + 14 * 60000, sincePrevMs: 60000 }),
      E({ action: 'note', at: t0 + 15 * 60000, sincePrevMs: 2 * 3600e3 }),            // capped at 1 h, idle
      E({ station: 'assembly', device: 'assembly-2', action: 'complete', at: t0 + 3600e3, parts: 5, orders: 1, sincePrevMs: -50, orderId: '3521000777' }),
    ];
    now = t0 + 2 * 3600e3;
    const r = await send(batch);
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.deepStrictEqual(r.body, { success: true, written: 9, duplicate: 0, refused: 0, scrubbed: 0 });
    assert.strictEqual(acts().length, 9); assert.strictEqual(txRuns, 1, 'one transaction for the batch');
    const d = roll('2026-10-02', 'Tess Welder');
    assert(d, 'the rollup document');
    assert.strictEqual(d.person, 'Tess Welder'); assert.strictEqual(d.day, '2026-10-02'); assert.strictEqual(d.events, 9);
    assert.strictEqual(d.firstAt, t0); assert.strictEqual(d.lastAt, t0 + 3600e3);
    const w = d.stations.welding;
    assert.deepStrictEqual({ scans: w.scans, scanParts: w.scanParts, completes: w.completes, parts: w.parts, orders: w.orders, prints: w.prints, rejects: w.rejects, errors: w.errors, notes: w.notes, undos: w.undos, undoParts: w.undoParts, undoOrders: w.undoOrders },
      { scans: 2, scanParts: 13, completes: 1, parts: 12, orders: 1, prints: 1, rejects: 1, errors: 1, notes: 1, undos: 1, undoParts: 2, undoOrders: 1 });
    assert.strictEqual(w.activeMs, 60000 + 3 * 60000 + 60000 * 3, 'gaps up to 5 minutes are working time');
    assert.strictEqual(w.idleMs, 7 * 60000 + 3600e3, 'a longer gap is idle, each capped at an hour');
    assert.strictEqual(w.firstAt, t0); assert.strictEqual(w.lastAt, t0 + 15 * 60000);
    assert.strictEqual(d.stations.assembly.parts, 5); assert.strictEqual(d.stations.assembly.orders, 1);
    assert.strictEqual(d.stations.assembly.activeMs, undefined, 'a counter never added is missing, read as 0');
    assert.deepStrictEqual(d.hours['09'], { scans: 2, parts: 12, undoParts: 2, by: { welding: { scans: 2, parts: 12, undoParts: 2 } } });
    assert.deepStrictEqual(d.hours['10'], { parts: 5, by: { assembly: { parts: 5 } } });
    assert.deepStrictEqual(d.touched, { 3521000777: { welding: true, assembly: true }, 3521000888: { welding: true } }, 'distinct orders worked');
    const ev = acts().find(e => e.action === 'complete' && e.station === 'welding');
    assert.deepStrictEqual(Object.keys(ev).sort(), ['action', 'at', 'computer', 'day', 'detail', 'device', 'hour', 'id', 'line', 'orderId', 'orders', 'parts', 'person', 'seq', 'serverAt', 'session', 'sincePrevMs', 'sku', 'station', 'ts', 'v'].sort(), 'the Station_Activity shape');
    assert.strictEqual(ev.ts, 'TS'); assert.strictEqual(ev.day, '2026-10-02'); assert.strictEqual(ev.hour, '09'); assert.strictEqual(ev.serverAt, now);
    assert.strictEqual(acts().find(e => e.station === 'assembly').sincePrevMs, 0, 'a negative gap is 0');
    // a later batch adds to the same rollup; first/last move only outward
    now = t0 + 3 * 3600e3;
    await send([E({ action: 'scan', at: t0 - 5 * 60000, parts: 1, sincePrevMs: 30000 }), E({ action: 'complete', at: t0 + 2 * 3600e3, parts: 3, orders: 1, sincePrevMs: 100000 })]);
    const d2 = roll('2026-10-02', 'Tess Welder');
    assert.strictEqual(d2.events, 11); assert.strictEqual(d2.firstAt, t0 - 5 * 60000); assert.strictEqual(d2.lastAt, t0 + 2 * 3600e3);
    assert.strictEqual(d2.stations.welding.parts, 15); assert.strictEqual(d2.stations.welding.scans, 3); assert.strictEqual(d2.stations.welding.firstAt, t0 - 5 * 60000);
    assert.strictEqual(d2.hours['09'].scans, 3);
    // a second person and a name with a slash have their own rollups
    await send([E({ person: 'Ray Welder', action: 'complete', parts: 4, orders: 1 }), E({ person: 'A/B Smith', action: 'scan', parts: 1 })]);
    assert(roll('2026-10-02', 'Ray Welder') && roll('2026-10-02', 'A-B Smith'));
    assert.strictEqual(roll('2026-10-02', 'A-B Smith').person, 'A/B Smith');
    assert.strictEqual(roll('2026-10-02', 'Tess Welder').events, 11, 'nobody else\'s events touch it');
    console.log('1 rollup math, two people, slash name');
  }

  /* 2 · idempotency */
  {
    reset(); now = Date.parse('2026-10-02T15:00:00Z');
    const b = [E({ action: 'scan', parts: 1, sincePrevMs: 1000 }), E({ action: 'complete', parts: 6, orders: 1, sincePrevMs: 2000 })];
    let r = await send(b); assert.strictEqual(r.body.written, 2);
    const before = JSON.stringify([...docs.entries()]);
    r = await send(b);
    assert.deepStrictEqual([r.status, r.body.written, r.body.duplicate], [200, 0, 2], 'a retry stores nothing');
    assert.strictEqual(JSON.stringify([...docs.entries()]), before, 'a retry changes no document, no rollup');
    const c = E({ action: 'complete', parts: 2, orders: 1, sincePrevMs: 3000 });
    r = await send([b[0], c, c]);
    assert.deepStrictEqual([r.body.written, r.body.duplicate], [1, 2], 'an overlap batch counts only the new event; one id twice in a request counts once');
    const d = roll('2026-10-02', 'Tess Welder');
    assert.strictEqual(d.events, 3); assert.strictEqual(d.stations.welding.parts, 8); assert.strictEqual(d.stations.welding.orders, 2); assert.strictEqual(d.stations.welding.scans, 1);
    assert.strictEqual(d.stations.welding.activeMs, 6000);
    console.log('2 idempotent: double send, overlap, one id twice');
  }

  /* 3 · the New York day */
  {
    reset();
    const cases = [['EDT', '2026-10-03T03:59:59Z', '2026-10-02', '23', '2026-10-03T04:00:01Z', '2026-10-03', '00'],
                   ['EST', '2026-12-02T04:59:59Z', '2026-12-01', '23', '2026-12-02T05:00:01Z', '2026-12-02', '00']];
    for (const [tz, a, dayA, hA, b, dayB, hB] of cases) {
      now = Date.parse(b) + 60000;
      const ta = Date.parse(a), tb = Date.parse(b);
      await send([E({ action: 'complete', at: ta, parts: 1, orders: 1 }), E({ action: 'complete', at: tb, parts: 2, orders: 1 }), E({ action: 'scan', at: tb + 1000, parts: 1 })]);
      assert.strictEqual(roll(dayA, 'Tess Welder').stations.welding.parts, 1, tz + ': before midnight is the earlier day');
      assert.deepStrictEqual(Object.keys(roll(dayA, 'Tess Welder').hours), [hA]);
      assert.strictEqual(roll(dayB, 'Tess Welder').stations.welding.parts, 2, tz + ': after midnight is the next day');
      assert.deepStrictEqual(Object.keys(roll(dayB, 'Tess Welder').hours), [hB]);
      assert.strictEqual(acts().find(e => e.at === ta).day, dayA); assert.strictEqual(acts().find(e => e.at === tb).day, dayB);
    }
    // an event queued before midnight and sent after keeps the day it happened
    now = Date.parse('2026-10-03T14:00:00Z');
    await send([E({ action: 'complete', at: Date.parse('2026-10-03T03:30:00Z'), parts: 7, orders: 1 })]);
    assert.strictEqual(roll('2026-10-02', 'Tess Welder').stations.welding.parts, 8);
    console.log('3 New York day boundary (EDT and EST), late event keeps its own day');
  }

  /* 4 · no PIN */
  {
    reset(); now = Date.parse('2026-10-02T15:00:00Z');
    const r = await send([
      E({ person: '123456', action: 'scan' }),                                          // a PIN as a name: refused
      E({ person: ' 654321 ' }),
      E({ action: 'scan', orderId: '123456', line: '482913', sku: '9999', detail: '654321' }),
      E({ action: 'note', detail: 'typed pin 654321 by mistake, order 3521000777', orderId: '3521000999' }),
      E({ action: 'note', detail: 'sheet 2 of 12, 480 mm', orderId: '3521000555' }),
      Object.assign(E({ action: 'scan', orderId: '3521000444' }), { employeeId: '123456', employee_id: '123456', pin: '123456', password: 'x' })
    ]);
    assert.deepStrictEqual(r.body, { success: true, written: 4, duplicate: 0, refused: 2, scrubbed: 2 });
    const all = JSON.stringify([...docs.entries()]);
    assert(!/123456|654321|482913/.test(all), 'no PIN-looking value is stored anywhere: ' + all.match(/.{20}(123456|654321|482913).{20}/));
    const byOrder = o => acts().find(e => e.orderId === o);
    assert.strictEqual(acts().find(e => e.detail.startsWith('typed pin')).detail, 'typed pin [#] by mistake, order 3521000777');
    assert.strictEqual(byOrder('3521000555').detail, 'sheet 2 of 12, 480 mm', 'ordinary numbers stay');
    assert.deepStrictEqual(Object.keys(byOrder('3521000444')).includes('employeeId') || Object.keys(byOrder('3521000444')).includes('pin'), false, 'fields outside the contract are dropped');
    const blank = acts().find(e => e.orderId === '' && e.action === 'scan');
    assert(blank && blank.line === '' && blank.sku === '' && blank.detail === '', 'digit-only order, line, sku and detail are blanked');
    assert(!roll('2026-10-02', '123456') && !roll('2026-10-02', '654321'), 'no rollup under a PIN');
    console.log('4 no PIN: names refused, ids and details blanked, extras dropped');
  }

  /* 5 · limits, vocabulary, clocks, flood, sandbox */
  {
    reset(); now = Date.parse('2026-10-02T15:00:00Z');
    let r = await send(Array.from({ length: 51 }, () => E()));
    assert.strictEqual(r.status, 413, '51 events are refused'); assert.strictEqual(docs.size, 0);
    r = await send(Array.from({ length: 50 }, () => E({ action: 'scan' })));
    assert.deepStrictEqual([r.status, r.body.written], [200, 50], '50 events are fine');
    reset();
    r = await send([E({ detail: 'x'.repeat(100) }), Object.assign(E({ id: 'big-event-001' }), { note: 'y'.repeat(700) })]);
    assert.deepStrictEqual([r.body.written, r.body.refused], [1, 1], 'an event over 600 bytes is refused');
    r = await send(null, { raw: JSON.stringify({ activity: [E()], pad: 'z'.repeat(41000) }) });
    assert.strictEqual(r.status, 413, 'a body over 40000 characters is refused');
    reset();
    r = await send([E({ station: 'kitchen' }), E({ action: 'poke' }), E({ id: 'x' }), E({ id: 5 }), E({ person: '' }), E({ action: 'scan' }), null, 'junk', [1]]);
    assert.deepStrictEqual([r.status, r.body.written, r.body.refused], [200, 1, 8], 'unknown station, action, id, empty name and junk are refused');
    // clocks: far past refused; future clamped to the server's now; a missing time is now
    reset();
    r = await send([E({ at: now - 8 * 86400e3 }), E({ at: now + 3 * 3600e3, action: 'scan' }), E({ at: 'soon', action: 'note' })]);
    assert.deepStrictEqual([r.body.written, r.body.refused], [2, 1]);
    assert(acts().every(e => e.at === now && e.serverAt === now), 'an event is never in the future');
    reset();
    r = await send([E({ at: now - 6 * 86400e3, action: 'complete', parts: 1 })]);
    assert.strictEqual(r.body.written, 1, 'a week-old queue is still accepted');
    // sandbox: separate collections, flagged; an event that names the other store is refused
    reset();
    r = await send([E({ action: 'complete', parts: 2, orders: 1 }), E({ sandbox: false }), E({ sandbox: true })], { sandbox: true });
    assert.deepStrictEqual([r.body.written, r.body.refused], [2, 1]);
    assert([...docs.keys()].every(k => /^Sandbox_(Station_Activity|Efficiency_Daily)\//.test(k)), 'the sandbox never touches the real collections: ' + [...docs.keys()].join());
    assert(acts().length === 0 && [...docs.values()].filter(v => v.sandbox === true).length === 3);
    r = await send([E({ sandbox: true })]); assert.deepStrictEqual([r.body.written, r.body.refused], [0, 1], 'a sandbox event cannot land in production');
    // flood guard: its own counter, 1500 a minute per sender
    reset();
    for (let i = 0; i < 30; i++) r = await send(Array.from({ length: 50 }, () => E({ action: 'note' })), { ip: '198.51.100.7' });
    assert.strictEqual(r.status, 200, '1500 events a minute from one sender pass');
    r = await send([E()], { ip: '198.51.100.7' }); assert.strictEqual(r.status, 429, 'then 429, so the client keeps them');
    now += 61000; r = await send([E()], { ip: '198.51.100.7' }); assert.strictEqual(r.status, 200, 'and a minute later it passes');
    // a failing store answers an error (the client keeps and retries), and nothing half-written is counted
    reset();
    const keep = fakeDb.runTransaction; fakeDb.runTransaction = async () => { throw new Error('unavailable'); };
    r = await send([E()]); assert(r.status >= 500, 'a store error is a 5xx: ' + r.status);
    fakeDb.runTransaction = keep; assert.strictEqual(docs.size, 0);
    console.log('5 limits (50/600/40000), vocabulary, clocks, sandbox, flood, store error');
  }
  /* 6 · the PIN list's real names: underscore style and a trailing period pass as sent, are never a PIN, make valid rollup ids */
  {
    reset();
    const NAMES = ['Giovanna C.', 'Empress D.', 'Michael_V', 'Michelle_R', 'Ivy_Y', 'Ana_M', 'Paul_K'];
    const validId = s => s.length > 0 && Buffer.byteLength(s) <= 1500 && !s.includes('/') && s !== '.' && s !== '..' && !/^__.*__$/.test(s);
    let r = await send(NAMES.flatMap((p, i) => [E({ person: p, action: 'scan', parts: 2, orderId: '35210001' + String(i).padStart(2, '0') }), E({ person: p, action: 'complete', parts: 2, orders: 1, orderId: '35210001' + String(i).padStart(2, '0') })]));
    assert.deepStrictEqual([r.status, r.body.written, r.body.refused], [200, 14, 0], 'all seven names pass the door');
    assert.deepStrictEqual(acts().map(e => e.person).sort(), NAMES.concat(NAMES).sort(), 'each is stored exactly as sent: the underscore stays, the period stays');
    const ids = [...docs.keys()].filter(k => k.startsWith('Efficiency_Daily/')).map(k => k.slice('Efficiency_Daily/'.length));
    assert.deepStrictEqual(ids.sort(), NAMES.map(p => '2026-10-02__' + p).sort(), 'one rollup per name, id = day__name');
    assert(ids.every(validId), 'every rollup id is a valid Firestore id');
    for (const p of NAMES) { const d = roll('2026-10-02', p); assert.deepStrictEqual([d.person, d.events, d.stations.welding.parts, d.stations.welding.completes], [p, 2, 2, 1], p + ': its own rollup'); }
    // two spellings of one person are two documents (the reader merges them); nothing here folds names
    r = await send([E({ person: 'Michael V.', action: 'complete', parts: 3, orders: 1 }), E({ person: 'MICHAEL  V', action: 'complete', parts: 4, orders: 1 })]);
    assert.strictEqual(r.body.written, 2);
    assert.deepStrictEqual([roll('2026-10-02', 'Michael_V').stations.welding.parts, roll('2026-10-02', 'Michael V.').stations.welding.parts, roll('2026-10-02', 'MICHAEL V').stations.welding.parts], [2, 3, 4], 'three spellings, three rollups; a doubled space is collapsed, nothing else');
    // no letter, no person: underscores, periods and digits alone could be a PIN
    reset();
    r = await send(['___', '. .', '_', '123_456', '12-34-56', '+', '.'].map(p => E({ person: p })));
    assert.deepStrictEqual([r.body.written, r.body.refused], [0, 7], 'a name without a letter is refused');
    assert.strictEqual(docs.size, 0);
    console.log('6 the PIN list names (Giovanna C., Empress D., Michael_V, Michelle_R, Ivy_Y, Ana_M, Paul_K): stored as sent, one valid rollup id each, spellings stay separate documents, no letter = refused');
  }
})().then(() => { Date.now = realNow; console.log('activity door: all passed'); }, e => { Date.now = realNow; console.error(e); process.exit(1); });
