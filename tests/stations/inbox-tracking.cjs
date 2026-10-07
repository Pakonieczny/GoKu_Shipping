// Inbox tracking, server side (Paul, 6 Oct 2026: "track all the responses a given user/employee has sent in a day/week/month/3 months/1 year,
// how many orders does that cover, how many messages to by customer"). netlify/functions/_employeeInbox.js behind employeeEfficiency.js (ops
// `inbox` and `personInbox`, personOrders station "inbox", the live board's inbox block) and the write side in _etsyMailLearning.js
// (recordOutcome also appends one small entry per sent reply to EtsyMail_ReplyDaily/{day}). Offline: the REAL code over an in-memory Firestore,
// a fake clock, a synthetic passcode; no Etsy, no AI, nothing real is read or written.
//   1 · gate: no key / wrong key / GET refused with no data, bad ranges, the passcode and an operator's password hash never in an answer
//   2 · the write side: what recordOutcome records (manual / auto / no name, order, customer key), the learning record untouched, a failing
//       entry write never costs the outcome, the same entry twice is one, the day of an entry (daylight saving edges)
//   3 · counts per window (day, week, month, 3 months, year) for every person against an independent count (Toronto days)
//   4 · distinct orders, distinct customers, a thread with several replies, messages per customer, auto kept out of a person's count,
//       unknown sender, alias merge (two inbox accounts, one person), team totals over the union
//   5 · boundaries: local midnight, the week and month edges, the daylight-saving days (23 and 25 hours), hours of the day, Toronto = New York
//   6 · history: the learning store before the first daily entry, order and customer from the conversation, no double count at the join,
//       days before the first record are null (never zero), a reply with no name is unknown
//   7 · IN2's shape (personInbox: METRICs with prev and delta, series, hours, perCustomer, unknown), personOrders station "inbox", the live block
//   8 · sandbox separate (nothing read), cost (documents per call, warm repeat 0, a day window reads only today)
//   node tests/stations/inbox-tracking.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

/* ── fake Firestore: collections of plain documents, where / orderBy / limit / select, doc get / set(merge, arrayUnion), getAll with a field mask, batch ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const AU = '__arrayUnion';
const clone = v => JSON.parse(JSON.stringify(v));
function fakeStore(opts) {
  opts = opts || {};
  const colls = new Map(), reads = [], writes = [], failing = new Set(), failingWrites = new Set(), selects = [], masks = [];
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const val = v => (v instanceof Ts ? v.m : v);
  const pick = (d, fields) => { if (!fields) return clone(d); const o = {}; for (const f of fields) if (d[f] !== undefined) o[f] = clone(d[f]); return o; };
  function apply(prev, v, merge) {
    const out = merge && prev ? clone(prev) : {};
    for (const [k, x] of Object.entries(v)) {
      if (x && typeof x === 'object' && x[AU]) { const cur = Array.isArray(out[k]) ? out[k] : []; for (const e of x[AU]) if (!cur.some(c => JSON.stringify(c) === JSON.stringify(e))) cur.push(clone(e)); out[k] = cur; }
      else if (x && typeof x === 'object' && x.__inc) out[k] = (out[k] || 0) + x.__inc;
      else if (x === 'SERVER_TS') out[k] = 'ts';
      else out[k] = clone(x);
    }
    return out;
  }
  function query(name, filters, order, lim, fields) {
    const q = {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, fields),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, fields),
      limit: n => query(name, filters, order, n, fields),
      get: async () => {
        if (failing.has(name)) throw Object.assign(new Error('14 UNAVAILABLE: synthetic outage of ' + name), { code: 14 });
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || typeof val(x) !== typeof val(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q2) => (val(p.d[f]) < val(q2.d[f]) ? -1 : val(p.d[f]) > val(q2.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push({ name, n: docs.length, kind: 'query', filters: filters.map(f => f[0] + f[1]), vals: filters.map(f => val(f[2])), order: order && order[0] });
        return { docs: docs.map(({ id, d }) => ({ id, exists: true, data: () => pick(d, fields) })), size: docs.length, empty: !docs.length };
      }
    };
    if (opts.select !== false) q.select = (...f) => { selects.push([name, f]); return query(name, filters, order, lim, f); };
    return q;
  }
  const ref = (name, id) => ({ id, _name: name,
    get: async () => { if (name !== 'Station_Rev') reads.push({ name, n: 1, kind: 'doc', id }); if (failing.has(name)) throw new Error('14 UNAVAILABLE'); const d = data(name).get(id); return { id, exists: !!d, data: () => (d ? clone(d) : undefined) }; },
    set: async (v, o) => { if (failingWrites.has(name)) throw new Error('synthetic write failure of ' + name); writes.push([name, id]); data(name).set(id, apply(data(name).get(id), v, !!(o && o.merge))); } });
  const db = {
    collection: name => Object.assign(query(name, [], null, null, null), { doc: id => ref(name, id) }),
    getAll: async (...args) => {
      const last = args[args.length - 1], mask = last && !last._name ? last.fieldMask : null, refs = args.filter(a => a && a._name);
      if (mask) masks.push(mask);
      return refs.map(r => { const d = data(r._name).get(r.id); reads.push({ name: r._name, n: 1, kind: 'getAll', id: r.id }); return { id: r.id, exists: !!d, data: () => (d ? pick(d, mask) : undefined) }; });
    },
    batch: () => { const ops = []; return { set: (r, v, o) => ops.push([r, v, o]), commit: async () => { for (const [r, v, o] of ops) await r.set(v, o); } }; }
  };
  if (opts.getAll === false) delete db.getAll;
  return { db, put: (name, id, d) => data(name).set(id, clone(d)), get: (name, id) => data(name).get(id), reads, writes, selects, masks, fail: n => failing.add(n), heal: n => failing.delete(n),
    failWrite: n => failingWrites.add(n), healWrite: n => failingWrites.delete(n), colls, count: n => data(n).size,
    docsOf: name => reads.filter(r => r.name === name).reduce((n, r) => n + r.n, 0), docsRead: () => reads.reduce((n, r) => n + r.n, 0), clear: () => { reads.length = 0; writes.length = 0; selects.length = 0; masks.length = 0; } };
}
const fakeAdmin = { firestore: Object.assign(() => ({}), { Timestamp: Ts, FieldValue: { serverTimestamp: () => 'SERVER_TS', increment: n => ({ __inc: n }), arrayUnion: (...a) => ({ [AU]: a }) } }) };

/* ── the function under test over the fake admin ── */
const realLoad = Module._load;
function loadFn() {
  for (const k of Object.keys(require.cache)) if (/netlify[\/]functions[\/](employeeEfficiency|_employee\w+|_stationLive|_etsyMailLearning)\.js$/.test(k)) delete require.cache[k];
  Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
  try { return { fn: require(path.join(root, 'netlify/functions/employeeEfficiency.js')), L: require(path.join(root, 'netlify/functions/_etsyMailLearning.js')), I: require(path.join(root, 'netlify/functions/_employeeInbox.js')) }; }
  finally { Module._load = realLoad; }
}
const { fn, L, I } = loadFn();
const T = fn._t;
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
const PASS = 'synthetic-pass-ib-9k', HASH = 'SECRET-PASSWORD-HASH-xyz';
process.env.EDIT_PASSCODE = PASS;
const logs = [], keepLog = (...a) => logs.push(a.map(x => (typeof x === 'string' ? x : JSON.stringify(x))).join(' '));
console.warn = console.log = console.error = console.info = keepLog;
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
const bodies = [];
let NOW = 0; Date.now = () => NOW;
let ipN = 0;
async function call(st, body, o) {
  o = o || {};
  const r = await T.handle({ httpMethod: o.method || 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, body: JSON.stringify(o.nokey ? body : Object.assign({ key: o.key || PASS }, body)) }, st.db);
  bodies.push(r.body);
  return { status: r.statusCode, body: JSON.parse(r.body || '{}'), size: (r.body || '').length };
}
const inbox = (st, b) => call(st, Object.assign({ op: 'inbox' }, b || {}));
const personInbox = (st, name, range, b) => call(st, Object.assign({ op: 'personInbox', name, range }, b || {}));

/* ── days, independent of the portal: Toronto's own clock ── */
const TORONTO = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Toronto', year: 'numeric', month: '2-digit', day: '2-digit' });
const NYC = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' });
const torontoDay = ms => TORONTO.format(new Date(ms));
const addDays = (d, n) => new Date(Date.UTC(+d.slice(0, 4), +d.slice(5, 7) - 1, +d.slice(8, 10) + n)).toISOString().slice(0, 10);
const WIN_N = { day: 1, week: 7, month: 30, quarter: 90, year: 365 };
const win = (range, to) => ({ from: addDays(to, -(WIN_N[range] - 1)), to, days: WIN_N[range] });
const HOUR = 3600000, DAYMS = 86400000;
const at = (iso) => Date.parse(iso);                                     // an exact instant (UTC)

/* ── the shop: conversations (an order and a customer each), operators ── */
const THREADS = {
  T1: { id: '1001', etsyOrderId: '3900000001', customerName: 'Sue Q', etsyUsername: 'sueq' },
  T2: { id: '1002', etsyOrderId: '3900000002', customerName: 'Tom R', etsyUsername: 'tomr' },
  T3: { id: '1003', etsyOrderId: '', customerName: 'Una S', etsyUsername: 'unas' },            // a question before any order
  T4: { id: '1004', etsyOrderId: '3900000004', customerName: 'Vic T', etsyUsername: 'vict' },
  T5: { id: '1005', etsyOrderId: '3900000005', customerName: 'Wes U', etsyUsername: 'wesu' }
};
const OPERATORS = { Paul_K: 'Paul K', ana: 'Ana M.', giovanna: 'Giovanna', gio_c: 'Giovanna C.' };
const AUTO = 'system:auto-pipeline';
function newShop(o) {
  const st = fakeStore(o);
  for (const [u, d] of Object.entries(OPERATORS)) st.put('EtsyMail_Operators', u, { displayName: d, role: u === 'Paul_K' ? 'owner' : 'operator', passwordHash: HASH });
  for (const [k, t] of Object.entries(THREADS)) st.put('EtsyMail_Threads', 'etsy_conv_' + t.id, { etsyOrderId: t.etsyOrderId, customerName: t.customerName, etsyUsername: t.etsyUsername, lastInboundPreview: 'PRIVATE-MESSAGE-TEXT' });
  return st;
}
const truth = [];                                                                  // every reply the tests send: { t, by, auto, th, order, cust }
/** One reply through the REAL writer (the entry the send path appends), also kept in `truth`. */
async function send(st, by, thread, t, o) {
  o = o || {};
  const th = THREADS[thread], rec = st.get('EtsyMail_Threads', 'etsy_conv_' + th.id);
  const entry = L.replyEntry({ atMs: t, employeeName: by, sendOrigin: o.auto ? 'auto' : 'manual', thread: rec, threadId: 'etsy_conv_' + th.id });
  assert.strictEqual(await L.recordReply({ db: st.db, admin: fakeAdmin, entry }), true);
  truth.push({ t, by, auto: !!o.auto || /^system:/.test(by), th: th.id, order: th.etsyOrderId, cust: th.etsyUsername, name: th.customerName });
  return entry;
}
const who = by => (by === 'Paul_K' ? 'Paul K.' : by === 'ana' ? 'Ana M.' : by === 'giovanna' || by === 'gio_c' ? 'Giovanna' : by === '' ? '' : by);   // the portal's names
const kindOfTruth = x => (x.auto ? 'auto' : x.by ? 'person' : 'unknown');
/** What a group of replies says in a window, counted here from scratch (Toronto days). */
function expectFor(list, from, to) {
  const rows = list.filter(x => { const d = torontoDay(x.t); return d >= from && d <= to; });
  const orders = new Set(rows.filter(x => x.order).map(x => x.order)), custs = new Map();
  for (const x of rows) custs.set(x.cust, (custs.get(x.cust) || 0) + 1);
  const counts = [...custs.values()], days = new Set(rows.map(x => torontoDay(x.t)));
  return { sent: rows.length, orders: orders.size, customers: custs.size, withoutOrder: rows.filter(x => !x.order).length, max: counts.length ? Math.max(...counts) : null,
    avg: counts.length ? Math.round(rows.length / counts.length * 10) / 10 : null, activeDays: days.size, first: rows.length ? Math.min(...rows.map(x => x.t)) : null, last: rows.length ? Math.max(...rows.map(x => x.t)) : null,
    threads: new Set(rows.map(x => x.th)).size, perCust: custs };
}
const groupOf = (kind, person) => truth.filter(x => (kind === 'team' ? kindOfTruth(x) !== 'auto' : kindOfTruth(x) === kind) && (!person || who(x.by) === person));

function build() {
  EP.resetCache(); NOW = at('2026-10-14T16:00:00Z');          // Wednesday 12:00 in Toronto
  truth.length = 0;
  return newShop();
}
function mulberry(seed) { let a = seed >>> 0; return () => { a |= 0; a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }

let passed = 0;
async function test(name, f) { await f(); passed++; say('ok -', name); }

(async () => {
  /* ═══ 1 · the gate ═══ */
  await test('1 · gate: no key, a wrong key, GET and a bad range are refused with no data; the passcode and the operators\' password hash never leave', async () => {
    const st = build();
    await send(st, 'Paul_K', 'T1', NOW - 3 * HOUR);
    const a = await call(st, { op: 'inbox' }, { nokey: true });
    assert.strictEqual(a.status, 401); assert.deepStrictEqual(Object.keys(a.body).sort(), ['error', 'ok']);
    assert.strictEqual((await call(st, { op: 'personInbox', name: 'Paul K' }, { key: 'wrong' })).status, 401);
    assert.strictEqual((await call(st, { op: 'inbox' }, { method: 'GET' })).status, 405);
    assert.strictEqual(st.docsRead(), 0, 'a refused request reads nothing');
    assert.strictEqual((await inbox(st, { range: 'fortnight' })).status, 400);
    assert.strictEqual((await inbox(st, { range: { from: '2026-10-09', to: '2026-10-01' } })).status, 400);
    assert.strictEqual((await personInbox(st, '', 'week')).status, 400);
    assert.strictEqual((await personInbox(st, '123456', 'week')).status, 400);          // a PIN is not a name
    assert.strictEqual(st.docsOf('EtsyMail_ReplyDaily') + st.docsOf('EtsyMail_DraftOutcomes') + st.docsOf('EtsyMail_Operators'), 0, 'a bad request reads no inbox record');
    const ok = await inbox(st, { range: 'day' });
    assert.strictEqual(ok.status, 200); assert.strictEqual(ok.body.ok, true);
    const txt = JSON.stringify(ok.body) + JSON.stringify((await personInbox(st, 'Paul K', 'day')).body);
    assert(!txt.includes(PASS) && !txt.includes(HASH) && !txt.includes('PRIVATE-MESSAGE-TEXT'), 'no passcode, no password hash, no message text');
  });

  /* ═══ 2 · the write side ═══ */
  await test('2 · write side: recordOutcome appends one small entry (who, manual or auto, order, customer) and leaves the learning record exactly as it was', async () => {
    const st = build(), A = fakeAdmin;
    const th = 'etsy_conv_1001', prev = { text: 'AI words', status: 'draft', generatedByAI: true, aiConfidence: 0.8 };
    NOW = at('2026-10-14T14:30:00Z');
    const doc = await L.recordOutcome({ db: st.db, admin: A, draftId: 'draft_' + th, threadId: th, prev, sentText: 'Human words', sendOrigin: 'manual', employeeName: 'Paul_K' });
    assert(doc && doc.employeeName === 'Paul_K' && doc.sendOrigin === 'manual' && doc.kind, 'the learning record is as before');
    const day = st.get('EtsyMail_ReplyDaily', '2026-10-14');
    assert(day && day.day === '2026-10-14' && Array.isArray(day.replies) && day.replies.length === 1);
    const e = day.replies[0];
    assert.deepStrictEqual(Object.keys(e).sort(), ['by', 'c', 'n', 'o', 'r', 't', 'th']);
    assert.strictEqual(e.t, NOW); assert.strictEqual(e.by, 'Paul_K'); assert.strictEqual(e.o, 'm'); assert.strictEqual(e.r, '3900000001'); assert.strictEqual(e.n, 'Sue Q'); assert.strictEqual(e.th, '1001');
    assert(/^[0-9a-f]{12}$/.test(e.c));
    assert(!JSON.stringify(day).includes('Human words') && !JSON.stringify(day).includes('PRIVATE-MESSAGE-TEXT'), 'no words of the reply or the conversation in the entry');
    assert.strictEqual(st.count('EtsyMail_DraftOutcomes'), 1);
    // the same customer through another conversation of theirs has the same key; another customer another key
    const e2 = L.replyEntry({ atMs: NOW, employeeName: 'ana', sendOrigin: 'manual', thread: { customerName: 'Sue Q', etsyUsername: 'SueQ' }, threadId: 'etsy_conv_77' });
    assert.strictEqual(e2.c, e.c, 'Etsy username folded: SueQ and sueq are one customer');
    assert.notStrictEqual(L.replyEntry({ atMs: 1, employeeName: 'ana', sendOrigin: 'manual', thread: THREADS.T2, threadId: 'etsy_conv_1002' }).c, e.c);
    // auto: sendOrigin auto, or a system: name, is never a person
    NOW += 60000;
    await L.recordOutcome({ db: st.db, admin: A, draftId: 'draft_' + th, threadId: th, prev: null, sentText: 'auto words', sendOrigin: 'auto', employeeName: AUTO });
    NOW += 60000;
    await L.recordOutcome({ db: st.db, admin: A, draftId: 'draft_' + th, threadId: th, prev: null, sentText: 'odd words', sendOrigin: 'manual', employeeName: 'system:simulation' });
    NOW += 60000;
    await L.recordOutcome({ db: st.db, admin: A, draftId: 'draft_' + th, threadId: th, prev: null, sentText: 'nameless', sendOrigin: 'manual', employeeName: null });
    const r = st.get('EtsyMail_ReplyDaily', '2026-10-14').replies;
    assert.deepStrictEqual(r.map(x => x.o), ['m', 'a', 'a', 'm']); assert.strictEqual(r[3].by, '', 'no name sent: empty, never guessed');
    // a conversation with no order, one that is missing: still recorded
    await L.recordOutcome({ db: st.db, admin: A, draftId: 'draft_x', threadId: 'etsy_conv_1003', prev: null, sentText: 'x', sendOrigin: 'manual', employeeName: 'ana' });
    await L.recordOutcome({ db: st.db, admin: A, draftId: 'draft_y', threadId: 'etsy_conv_9999', prev: null, sentText: 'y', sendOrigin: 'manual', employeeName: 'ana' });
    const r2 = st.get('EtsyMail_ReplyDaily', '2026-10-14').replies;
    assert.strictEqual(r2[4].r, ''); assert.strictEqual(r2[5].r, ''); assert.strictEqual(r2[5].th, '9999'); assert.strictEqual(r2.length, 6);
    // the same entry twice is one (arrayUnion), and a failing entry write never costs the learning record
    assert.strictEqual(await L.recordReply({ db: st.db, admin: A, entry: r2[0] }), true); assert.strictEqual(st.get('EtsyMail_ReplyDaily', '2026-10-14').replies.length, 6);
    st.failWrite('EtsyMail_ReplyDaily');
    const before = st.count('EtsyMail_DraftOutcomes');
    NOW += 60000;
    const d3 = await L.recordOutcome({ db: st.db, admin: A, draftId: 'draft_z', threadId: th, prev: null, sentText: 'z', sendOrigin: 'manual', employeeName: 'Paul_K' });
    assert(d3 && d3.employeeName === 'Paul_K', 'the outcome is still recorded'); assert.strictEqual(st.count('EtsyMail_DraftOutcomes'), before + 1);
    assert.strictEqual(st.get('EtsyMail_ReplyDaily', '2026-10-14').replies.length, 6);
    st.healWrite('EtsyMail_ReplyDaily');
    // a failing thread read still records the reply (with what it knows)
    st.fail('EtsyMail_Threads'); NOW += 60000;
    await L.recordOutcome({ db: st.db, admin: A, draftId: 'draft_w', threadId: th, prev: null, sentText: 'w', sendOrigin: 'manual', employeeName: 'Paul_K' });
    assert.strictEqual(st.get('EtsyMail_ReplyDaily', '2026-10-14').replies.length, 7); st.heal('EtsyMail_Threads');
    // an AI draft nobody sent writes nothing here: only recordOutcome (a send) does
    assert.strictEqual(st.count('EtsyMail_ReplyDaily'), 1);
    // the day of an entry: the shop's New York day, daylight saving edges included (Toronto is the same clock)
    for (const [iso, want] of [['2026-03-08T04:59:59.999Z', '2026-03-07'], ['2026-03-08T05:00:00.000Z', '2026-03-08'], ['2026-03-09T03:59:59.999Z', '2026-03-08'], ['2026-03-09T04:00:00.000Z', '2026-03-09'],
      ['2026-11-01T03:59:59.999Z', '2026-10-31'], ['2026-11-01T04:00:00.000Z', '2026-11-01'], ['2026-11-02T04:59:59.999Z', '2026-11-01'], ['2026-11-02T05:00:00.000Z', '2026-11-02']]) {
      assert.strictEqual(L.replyDay(at(iso)), want, iso); assert.strictEqual(torontoDay(at(iso)), want, 'Toronto ' + iso); assert.strictEqual(T.nyDay(at(iso)), want, 'portal ' + iso);
    }
  });
  /* ═══ 3 · counts per window against an independent count ═══ */
  async function bigShop() {
    const st = build(), rnd = mulberry(20261006), start = '2025-11-20', today = '2026-10-14';
    const keys = Object.keys(THREADS);
    for (let d = start; d <= today; d = addDays(d, 1)) {
      const mid = Date.UTC(+d.slice(0, 4), +d.slice(5, 7) - 1, +d.slice(8, 10)), dow = new Date(mid).getUTCDay();
      const n = dow === 0 ? 0 : 1 + Math.floor(rnd() * 6);
      for (let i = 0; i < n; i++) {
        const r = rnd(), by = r < 0.35 ? 'Paul_K' : r < 0.6 ? 'ana' : r < 0.75 ? 'giovanna' : r < 0.85 ? 'gio_c' : r < 0.92 ? '' : AUTO;
        const th = keys[Math.floor(rnd() * keys.length)], t = Math.floor(mid + (13 + rnd() * 9) * HOUR);       // 13:00 to 22:00 UTC: the same Toronto day all year
        if (t > NOW) continue;
        await send(st, by, th, t, { auto: by === AUTO });
      }
    }
    return st;
  }
  const checkS = (s, e, label, knownFull) => {
    assert.strictEqual(s.sent, e.sent, label + ': sent'); assert.strictEqual(s.replies, e.sent, label + ': replies'); assert.strictEqual(s.messages, e.sent, label + ': messages');
    assert.strictEqual(s.orders, e.orders, label + ': orders'); assert.strictEqual(s.customers, e.customers, label + ': customers'); assert.strictEqual(s.conversations, e.threads, label + ': conversations');
    assert.strictEqual(s.withoutOrder, e.withoutOrder, label + ': withoutOrder'); assert.strictEqual(s.activeDays, e.activeDays, label + ': activeDays');
    assert.strictEqual(s.firstAt, e.first, label + ': firstAt'); assert.strictEqual(s.lastAt, e.last, label + ': lastAt');
    assert.strictEqual(s.perCustomer.average, e.avg, label + ': per customer average'); assert.strictEqual(s.perCustomer.max, e.max, label + ': most to one customer');
    assert.strictEqual(s.repliesPerActiveDay, e.activeDays ? Math.round(e.sent / e.activeDays * 10) / 10 : null, label + ': per active day');
    if (knownFull) assert.strictEqual(s.knownDays, s.days, label + ': fully known');
  };
  let BIG = null, BIG_TRUTH = [];
  await test('3 · every person, every window (day, week, month, 3 months, year): sent, orders, customers, conversations, per customer, first and last, against an independent count', async () => {
    const st = BIG = await bigShop(); BIG_TRUTH = truth.slice();
    assert(truth.length > 800, 'a real year of replies: ' + truth.length);
    const r = await inbox(st, { range: 'week', top: 5 });
    assert.strictEqual(r.status, 200); const b = r.body;
    assert.strictEqual(b.mode, 'real'); assert.strictEqual(b.day, '2026-10-14'); assert.strictEqual(b.granularity, 'day');
    for (const w of Object.keys(WIN_N)) assert.deepStrictEqual(b.windows[w], win(w, '2026-10-14'), 'the window of ' + w);
    assert.strictEqual(b.knownFrom, L.replyDay(Math.min(...truth.map(x => x.t))), 'records begin with the first entry');
    assert.deepStrictEqual(b.people.map(p => p.name).sort(), ['Ana M.', 'Giovanna', 'Paul K.']);
    const ranged = b.people.map(p => p.windows.week.sent);
    assert.deepStrictEqual(ranged, ranged.slice().sort((x, y) => y - x), 'most replies in the range first');
    for (const w of Object.keys(WIN_N)) {
      const { from, to } = win(w, '2026-10-14');
      for (const p of b.people) {
        const e = expectFor(groupOf('person', p.name), from, to), s = p.windows[w];
        checkS(s, e, `${p.name} ${w}`, w === 'day' || w === 'week' || w === 'month');
        assert(s.perCustomer.median == null || Number.isFinite(s.perCustomer.median));
        // the top list: most messages first (ties: the newest), at most 5
        const want = [...e.perCust].map(([c, n]) => ({ c, n, last: Math.max(...groupOf('person', p.name).filter(x => x.cust === c && torontoDay(x.t) >= from && torontoDay(x.t) <= to).map(x => x.t)) }))
          .sort((x, y) => y.n - x.n || y.last - x.last).slice(0, 5);
        assert.deepStrictEqual(s.top.map(t => t.messages), want.map(t => t.n), `${p.name} ${w}: top counts`);
        assert.deepStrictEqual(s.top.map(t => t.customer), want.map(t => Object.values(THREADS).find(h => h.etsyUsername === t.c).customerName), `${p.name} ${w}: top customers`);
        assert(s.top.every(t => t.orderIds.length <= 3 && t.orders >= t.orderIds.length));
      }
      for (const [key, kind] of [['unknown', 'unknown'], ['auto', 'auto'], ['team', 'team']]) checkS(b[key].windows[w], expectFor(groupOf(kind), from, to), `${key} ${w}`, false);
    }
    // the year starts before the first record: partial, said so; month is fully inside the records
    assert(b.people[0].windows.year.knownDays < 365 && b.people[0].windows.year.knownDays > 300, 'the year is partly known: ' + b.people[0].windows.year.knownDays);
    assert.strictEqual(b.people[0].windows.month.knownDays, 30);
    assert(b.notes.some(n => /records begin/i.test(n)), 'says where the records begin');
    // team = people + unknown, never auto
    const t = b.team.windows.year, sum = b.people.reduce((n, p) => n + p.windows.year.sent, 0) + b.unknown.windows.year.sent;
    assert.strictEqual(t.sent, sum); assert(b.auto.windows.year.sent > 0, 'auto replies exist and are counted on their own');
    // series of the range: a day each, against the same count; for a year, Monday weeks
    const p0 = b.people[0], w0 = win('week', '2026-10-14');
    assert.deepStrictEqual(p0.series.map(x => x.day), Array.from({ length: 7 }, (_, i) => addDays(w0.from, i)));
    for (const pt of p0.series) { const e = expectFor(groupOf('person', p0.name), pt.day, pt.day); assert.strictEqual(pt.sent, e.sent, 'series ' + pt.day); assert.strictEqual(pt.orders, e.orders); assert.strictEqual(pt.customers, e.customers); assert.strictEqual(pt.firstAt, e.first); assert.strictEqual(pt.lastAt, e.last); assert.strictEqual(pt.days, 1); }
    const y = (await inbox(st, { range: 'year', windows: ['year'] })).body, py = y.people.find(p => p.name === p0.name);
    assert.strictEqual(y.granularity, 'week'); assert(py.series.length >= 52 && py.series.length <= 54, 'about 53 weekly points: ' + py.series.length);
    assert.strictEqual(py.series.reduce((n, x) => n + (x.sent || 0), 0), py.windows.year.sent, 'the weeks add up to the year');
    assert.strictEqual(py.series[0].day, win('year', '2026-10-14').from); assert(py.series.every(x => x.firstAt === null || x.days === 1));
    for (const pt of py.series) { const e = expectFor(groupOf('person', p0.name), pt.day, pt.to); if (pt.sent != null) { assert.strictEqual(pt.sent, e.sent, 'week of ' + pt.day); assert.strictEqual(pt.orders, e.orders); assert.strictEqual(pt.customers, e.customers); } }
    assert(py.series.some(x => x.sent === null) && py.series.some(x => x.sent > 0), 'weeks before the first record are null, the others counted');
    // custom range
    const c = (await inbox(st, { range: { from: '2026-03-01', to: '2026-03-31' }, windows: ['week'] })).body;
    assert.strictEqual(c.range, 'custom'); assert.deepStrictEqual(c.windows.custom, { from: '2026-03-01', to: '2026-03-31', days: 31 });
    checkS(c.people.find(p => p.name === 'Paul K.').windows.custom, expectFor(groupOf('person', 'Paul K.'), '2026-03-01', '2026-03-31'), 'custom March', true);
    // narrowing to one person by any spelling
    const one = (await inbox(st, { range: 'week', name: 'paul_k' })).body;
    assert.deepStrictEqual(one.people.map(p => p.name), ['Paul K.']);
  });

  /* ═══ 4 · the scenarios Paul asks about, small and exact ═══ */
  await test('4 · distinct orders and customers, a thread with several replies, messages per customer, auto kept out of a person, unknown sender, two inbox accounts of one person', async () => {
    const st = build(); const T0 = at('2026-10-14T13:00:00Z'), m = i => T0 + i * 20 * 60000;
    let i = 0;
    for (const [by, th, auto] of [['Paul_K', 'T1'], ['Paul_K', 'T1'], ['Paul_K', 'T1'], ['Paul_K', 'T2'], ['Paul_K', 'T2'], ['Paul_K', 'T3'], ['ana', 'T1'], ['ana', 'T4'],
      [AUTO, 'T1', true], [AUTO, 'T1', true], [AUTO, 'T5', true], ['', 'T2'], ['gio_c', 'T5'], ['giovanna', 'T5'], ['giovanna', 'T2']]) await send(st, by, th, m(i++), { auto });
    const b = (await inbox(st, { range: 'day', top: 5 })).body, P = n => b.people.find(p => p.name === n), D = n => P(n).windows.day;
    assert.deepStrictEqual(b.people.map(p => p.name).sort(), ['Ana M.', 'Giovanna', 'Paul K.']);
    // Paul: 6 replies, 2 orders (the third conversation has none), 3 customers; a thread with 3 replies is ONE customer and ONE order
    assert.deepStrictEqual([D('Paul K.').sent, D('Paul K.').orders, D('Paul K.').customers, D('Paul K.').conversations, D('Paul K.').withoutOrder], [6, 2, 3, 3, 1]);
    assert.deepStrictEqual(D('Paul K.').perCustomer, { average: 2, median: 2, max: 3 });
    assert.deepStrictEqual(D('Paul K.').top.map(t => [t.customer, t.messages]), [['Sue Q', 3], ['Tom R', 2], ['Una S', 1]]);
    assert.deepStrictEqual(D('Paul K.').top[0].orderIds, ['3900000001']); assert.strictEqual(D('Paul K.').top[2].orders, 0, 'a customer with no order');
    assert.deepStrictEqual(D('Paul K.').top[2].orderIds, []);
    // Ana: shares order 1 and customer Sue with Paul; each counts it for herself
    assert.deepStrictEqual([D('Ana M.').sent, D('Ana M.').orders, D('Ana M.').customers], [2, 2, 2]);
    // the auto-pipeline's replies on Sue's order are nobody's: not in Paul's 6 nor Ana's 2, counted on their own
    assert.deepStrictEqual([b.auto.windows.day.sent, b.auto.windows.day.orders, b.auto.windows.day.customers], [3, 2, 2]);
    // unknown sender: no name was recorded: one reply, shown as unknown, in nobody's count
    assert.strictEqual(b.unknown.windows.day.sent, 1);
    // two inbox accounts (giovanna and gio_c) are one person through the portal's alias rule
    assert.deepStrictEqual([D('Giovanna').sent, D('Giovanna').orders, D('Giovanna').customers], [3, 2, 2]);
    assert.deepStrictEqual(P('Giovanna').spellings.sort(), ['Giovanna', 'Giovanna C.', 'gio_c', 'giovanna'].sort());
    // the team: persons + unknown (never auto); distinct orders and customers over the UNION (order 1 and Sue count once)
    assert.deepStrictEqual([b.team.windows.day.sent, b.team.windows.day.orders, b.team.windows.day.customers], [12, 4, 5]);
    assert.strictEqual(b.team.windows.day.sent, D('Paul K.').sent + D('Ana M.').sent + D('Giovanna').sent + b.unknown.windows.day.sent);
    // first and last reply time of the day
    assert.strictEqual(D('Paul K.').firstAt, m(0)); assert.strictEqual(D('Paul K.').lastAt, m(5));
    assert.strictEqual(P('Paul K.').series[0].firstAt, m(0)); assert.strictEqual(P('Paul K.').series[0].lastAt, m(5));
    // IN2's shape agrees (one person): the same numbers
    const pi = (await personInbox(st, 'Paul K', 'day', { top: 10 })).body;
    assert.deepStrictEqual(['replies', 'orders', 'customers', 'messages', 'messagesPerCustomer', 'maxPerCustomer', 'repliesPerDay', 'daysActive'].map(k => pi.totals[k].value), [6, 2, 3, 6, 2, 3, 6, 1]);
    assert.deepStrictEqual(pi.perCustomer.distribution.map(x => x.customers), [1, 1, 1, 0, 0]);       // one customer got 1 message, one 2, one 3
    assert.deepStrictEqual(pi.unknown, { replies: 1, messages: 1 });
    // any spelling reaches the same person; the account name, the display name, the portal's name
    for (const nm of ['paul_k', 'Paul K', 'PAUL K.', 'Paul_K']) assert.strictEqual((await personInbox(st, nm, 'day')).body.totals.replies.value, 6, nm);
    assert.strictEqual((await personInbox(st, 'gio_c', 'day')).body.totals.replies.value, 3); assert.strictEqual((await personInbox(st, 'Giovanna C.', 'day')).body.name, 'Giovanna');
  });

  /* ═══ 5 · boundaries: local midnight, week / month / 3 month / year edges, daylight saving ═══ */
  await test('5 · boundaries in Toronto time: local midnight, the edges of every window, the 23 and 25 hour days, the hour of the day; Toronto is New York', async () => {
    EP.resetCache(); const st = newShop(); truth.length = 0; NOW = at('2027-01-15T12:00:00Z');
    const sends = ['2026-03-08T04:59:59.999Z', '2026-03-08T05:00:00.000Z', '2026-03-08T06:59:00.000Z', '2026-03-08T07:00:00.000Z', '2026-03-09T03:59:59.999Z', '2026-03-09T04:00:00.000Z',
      '2026-11-01T03:59:59.999Z', '2026-11-01T04:00:00.000Z', '2026-11-01T05:30:00.000Z', '2026-11-01T06:30:00.000Z', '2026-11-01T07:30:00.000Z', '2026-11-02T04:59:59.999Z', '2026-11-02T05:00:00.000Z'];
    for (const iso of sends) await send(st, 'Paul_K', 'T1', at(iso));
    const day = async (d, extra) => (await personInbox(st, 'Paul K', 'day', Object.assign({ day: d }, extra || {}))).body;
    // spring forward (a 23 hour day): 05:00Z is midnight; there is no 02:00 hour
    let b = await day('2026-03-07'); assert.strictEqual(b.totals.replies.value, 1, 'one millisecond before local midnight is still the 7th');
    b = await day('2026-03-08'); assert.strictEqual(b.totals.replies.value, 4);
    assert.deepStrictEqual(b.hours.map(h => h.replies).map((n, h) => (n ? h + ':' + n : '')).filter(Boolean), ['0:1', '1:1', '3:1', '23:1'], 'hours on the 23 hour day: 01:59 then 03:00');
    assert.strictEqual(b.hours.reduce((n, h) => n + h.replies, 0), 4);
    b = await day('2026-03-09'); assert.strictEqual(b.totals.replies.value, 1, 'the instant after local midnight is the 9th');
    // fall back (a 25 hour day): 01:30 happens twice
    b = await day('2026-10-31'); assert.strictEqual(b.totals.replies.value, 1);
    b = await day('2026-11-01'); assert.strictEqual(b.totals.replies.value, 5);
    assert.deepStrictEqual(b.hours.map(h => h.replies).map((n, h) => (n ? h + ':' + n : '')).filter(Boolean), ['0:1', '1:2', '2:1', '23:1'], 'hours on the 25 hour day: 01:30 twice');
    b = await day('2026-11-02'); assert.strictEqual(b.totals.replies.value, 1);
    // windows are New York days ending on `day`: a week that starts on the spring-forward day, a month that starts on it
    b = (await personInbox(st, 'Paul K', 'week', { day: '2026-03-14' })).body; assert.strictEqual(b.from, '2026-03-08'); assert.strictEqual(b.totals.replies.value, 5, 'the week from the 8th has 4 + 1');
    b = (await personInbox(st, 'Paul K', 'week', { day: '2026-03-13' })).body; assert.strictEqual(b.from, '2026-03-07'); assert.strictEqual(b.totals.replies.value, 6, 'one day earlier the 7th is in the week');
    b = (await personInbox(st, 'Paul K', 'week', { day: '2026-03-07' })).body; assert.strictEqual(b.from, '2026-03-01'); assert.strictEqual(b.totals.replies.value, 1);
    b = (await personInbox(st, 'Paul K', 'month', { day: '2026-04-06' })).body; assert.strictEqual(b.from, '2026-03-08'); assert.strictEqual(b.totals.replies.value, 5, 'a month (30 days) from the 8th: the 7th is out');
    b = (await personInbox(st, 'Paul K', 'month', { day: '2026-04-05' })).body; assert.strictEqual(b.from, '2026-03-07'); assert.strictEqual(b.totals.replies.value, 6, 'one day later back: the 7th is in');
    // every window edge, on a clean shop: the instant after local midnight of the first day is in, the millisecond before it is out
    EP.resetCache(); const s2 = newShop(); truth.length = 0; NOW = at('2026-10-14T16:00:00Z');
    const edges = { day: ['2026-10-14T03:59:59.999Z', '2026-10-14T04:00:00.000Z'], week: ['2026-10-08T03:59:59.999Z', '2026-10-08T04:00:00.000Z'], month: ['2026-09-15T03:59:59.999Z', '2026-09-15T04:00:00.000Z'],
      quarter: ['2026-07-17T03:59:59.999Z', '2026-07-17T04:00:00.000Z'], year: ['2025-10-15T03:59:59.999Z', '2025-10-15T04:00:00.000Z'] };
    for (const [w, [out, inn]] of Object.entries(edges)) { await send(s2, 'Paul_K', 'T1', at(out)); await send(s2, 'Paul_K', 'T2', at(inn)); }
    const eb = (await inbox(s2, { range: 'week' })).body, pk = eb.people[0].windows;
    assert.deepStrictEqual(Object.fromEntries(Object.keys(edges).map(w => [w, [eb.windows[w].from, pk[w].sent]])), { day: ['2026-10-14', 1], week: ['2026-10-08', 3], month: ['2026-09-15', 5], quarter: ['2026-07-17', 7], year: ['2025-10-15', 9] },
      'each window takes its first local day whole and nothing before it: its own edge-in plus every narrower window\'s two edge entries (the year crosses a daylight-saving change)');
    for (const w of Object.keys(edges)) checkS(pk[w], expectFor(groupOf('person', 'Paul K.'), win(w, '2026-10-14').from, '2026-10-14'), 'edge ' + w, false);
    // Toronto and New York are one clock: every hour around the changes of 2026 and 2027 has the same day
    for (const start of ['2026-03-07', '2026-10-31', '2027-03-13', '2027-11-06']) for (let h = 0; h < 96; h++) { const t = at(start + 'T00:00:00Z') + h * HOUR; assert.strictEqual(NYC.format(new Date(t)), torontoDay(t)); assert.strictEqual(T.nyDay(t), torontoDay(t)); assert.strictEqual(L.replyDay(t), torontoDay(t)); }
  });

  /* ═══ 6 · history before the daily entries: the learning store, joined without a double count ═══ */
  await test('6 · history: the learning store before the first daily entry (order and customer from the conversation), no double count at the join, days before the first record are null', async () => {
    EP.resetCache(); const st = newShop(); truth.length = 0; NOW = at('2026-10-14T16:00:00Z');
    const outcome = (id, t, by, origin, th, name) => st.put('EtsyMail_DraftOutcomes', id, { threadId: 'etsy_conv_' + th, draftId: 'draft_etsy_conv_' + th, atMs: t, employeeName: by, sendOrigin: origin, customerName: name, sentText: 'PRIVATE-REPLY-TEXT', aiText: '', kind: 'staff_only' });
    // the tail: 28 Sep to 3 Oct, stored only in the learning store (no daily entry yet)
    const tail = [['2026-09-28T15:00:00Z', 'Paul_K', 'manual', 'T1'], ['2026-09-28T16:00:00Z', 'Paul_K', 'manual', 'T1'], ['2026-09-29T15:00:00Z', 'ana', 'manual', 'T2'], ['2026-09-29T17:00:00Z', null, 'manual', 'T3'],
      ['2026-09-30T14:00:00Z', 'gio_c', 'manual', 'T4'], ['2026-10-01T14:00:00Z', AUTO, 'auto', 'T1'], ['2026-10-02T14:00:00Z', 'Paul_K', 'manual', 'T2'], ['2026-10-03T03:30:00Z', 'ana', 'manual', 'T1'], ['2026-10-03T13:00:00Z', 'Paul_K', 'manual', 'T6']];
    tail.forEach(([iso, by, o, th], i) => {
      const t = at(iso), thr = THREADS[th];
      outcome('d' + i + '_' + t, t, by, o, thr ? thr.id : '1006', thr ? thr.customerName : 'Gone Gail');
      truth.push({ t, by: by || '', auto: o === 'auto', th: thr ? thr.id : '1006', order: thr ? thr.etsyOrderId : '', cust: thr ? thr.etsyUsername : 'gone gail', name: thr ? thr.customerName : 'Gone Gail' });
    });
    // from 4 Oct the real send path writes both stores
    const A = fakeAdmin; let n = 0;
    for (const [iso, by, o, th] of [['2026-10-04T14:00:00Z', 'Paul_K', 'manual', 'T1'], ['2026-10-05T14:00:00Z', 'ana', 'manual', 'T2'], ['2026-10-05T15:00:00Z', 'Paul_K', 'manual', 'T1'], ['2026-10-12T14:00:00Z', 'Paul_K', 'manual', 'T5'], ['2026-10-14T14:00:00Z', 'ana', 'manual', 'T5']]) {
      NOW = at(iso); const thr = THREADS[th];
      await L.recordOutcome({ db: st.db, admin: A, draftId: 'draft_etsy_conv_' + thr.id, threadId: 'etsy_conv_' + thr.id, prev: null, sentText: 'PRIVATE-REPLY-TEXT ' + (n++), sendOrigin: o, employeeName: by });
      truth.push({ t: at(iso), by, auto: false, th: thr.id, order: thr.etsyOrderId, cust: thr.etsyUsername, name: thr.customerName });
    }
    NOW = at('2026-10-14T16:00:00Z');
    // the conversation of the first tail reply lost its order link later: the portal says what the conversation says today
    st.put('EtsyMail_Threads', 'etsy_conv_1004', { customerName: 'Vic T', etsyUsername: 'vict' });
    truth.filter(x => x.th === '1004').forEach(x => { x.order = ''; });
    // the thread 1006 does not exist any more: the customer comes from the outcome's own name, no order
    st.clear();
    const b = (await inbox(st, { range: 'month', top: 10 })).body;
    assert.strictEqual(b.knownFrom, '2026-09-28', 'records begin with the first outcome');
    assert(b.notes.some(x => /learning store/i.test(x)) && b.notes.some(x => /records begin/i.test(x)));
    const r = b.reads;
    assert.strictEqual(r.outcomeDocs, tail.length, 'the tail only: the outcomes of the days that have daily entries are not read'); assert.strictEqual(r.trackDocs, 2, 'plus one document each to find where the records begin');
    assert.strictEqual(r.threadDocs, 5, 'one read per distinct conversation of the tail (one of them no longer exists)');
    assert.strictEqual(st.masks.length > 0 && st.masks.every(m => m.join() === 'etsyOrderId,linkedOrderId,etsyUsername,customerName' || m.join() === 'displayName,revokedAt'), true, 'only the named fields of a conversation are read');
    const D = (n, w) => b.people.find(p => p.name === n).windows[w];
    const { from, to } = win('month', '2026-10-14');
    for (const n of ['Paul K.', 'Ana M.', 'Giovanna']) checkS(D(n, 'month'), expectFor(groupOf('person', n), from, to), n + ' month with history', false);
    checkS(b.unknown.windows.month, expectFor(groupOf('unknown'), from, to), 'unknown with history'); assert.strictEqual(b.unknown.windows.month.sent, 1, 'the outcome with no name is unknown, never a guessed person');
    checkS(b.auto.windows.month, expectFor(groupOf('auto'), from, to), 'auto with history'); assert.strictEqual(b.auto.windows.month.sent, 1);
    assert.strictEqual(D('Giovanna', 'month').sent, 1); assert.strictEqual(D('Giovanna', 'month').orders, 0, 'the order link of that conversation is gone today: no order, counted as without order'); assert.strictEqual(D('Giovanna', 'month').withoutOrder, 1);
    // a customer whose conversation is gone: grouped by the outcome's own name
    assert.strictEqual(b.team.windows.month.customers, expectFor(groupOf('team'), from, to).customers); assert.strictEqual(b.team.windows.month.customers, 6);
    // a month starting 15 Sep is partly known (17 days: 28 Sep to 14 Oct), the week is fully known, the year says so too
    assert.strictEqual(D('Paul K.', 'month').knownDays, 17); assert.strictEqual(D('Paul K.', 'week').knownDays, 7); assert.strictEqual(D('Paul K.', 'year').knownDays, 17);
    // per day: days before the first record are null (not 0), a known day with nothing is 0
    const pm = (await personInbox(st, 'Paul K', 'month', { compare: true })).body;
    assert.strictEqual(pm.knownFrom, '2026-09-28');
    const byDay = Object.fromEntries(pm.series.map(x => [x.day, x.replies]));
    assert.strictEqual(byDay['2026-09-20'], null, 'before the first record: unknown'); assert.strictEqual(byDay['2026-09-27'], null);
    assert.strictEqual(byDay['2026-09-28'], 2); assert.strictEqual(byDay['2026-09-30'], 0, 'a known day with nothing sent'); assert.strictEqual(byDay['2026-10-04'], 1); assert.strictEqual(byDay['2026-10-14'], 0);
    assert.strictEqual(pm.totals.replies.estimated, true); assert(/Counted from 2026-09-28/.test(pm.totals.replies.why), 'the figure says it is counted from the first record');
    assert.strictEqual(pm.totals.replies.prev, null, 'the window before is entirely before the first record: nothing is claimed');
    // a window entirely before the records: every figure is a dash
    const old = (await personInbox(st, 'Paul K', 'week', { day: '2026-09-20' })).body;
    assert(old.totals.replies.value === null && old.totals.orders.value === null && old.series.every(x => x.replies === null) && old.hours.every(h => h.replies === null) && old.perCustomer.top.length === 0, 'unknown, not zero');
    assert.strictEqual(old.unknown.replies, null);
    // the tail and the daily entries never count one reply twice: all outcomes since 4 Oct also exist in the learning store
    assert.strictEqual(st.count('EtsyMail_DraftOutcomes'), tail.length + 5);
    assert.strictEqual(b.team.windows.month.sent, truth.filter(x => !x.auto).length);
    // before any daily entry exists at all, the tail alone answers (nothing is claimed as zero for later days)
    EP.resetCache(); const s3 = newShop(); tail.forEach(([iso, by, o, th], i) => s3.put('EtsyMail_DraftOutcomes', 'e' + i, { threadId: 'etsy_conv_' + (THREADS[th] || { id: '1006' }).id, atMs: at(iso), employeeName: by, sendOrigin: o, customerName: 'x' }));
    const b3 = (await inbox(s3, { range: 'month' })).body;
    assert.strictEqual(b3.team.windows.month.sent, tail.filter(x => x[2] !== 'auto').length); assert.strictEqual(b3.knownFrom, '2026-09-28');
  });

  /* ═══ 7 · IN2's shape ═══ */
  await test('7 · personInbox (IN2): METRICs with prev and delta, series, hours, perCustomer with distribution and top, unknown, notes; found:false is dashes', async () => {
    const st = BIG; EP.resetCache(); NOW = at('2026-10-14T16:00:00Z'); truth.length = 0; truth.push(...BIG_TRUTH);
    const fresh = fakeStore(); for (const [c, m] of st.colls) for (const [id, d] of m) fresh.put(c, id, d);
    const r = await personInbox(fresh, 'paul_k', 'month', { compare: true, top: 10 }), b = r.body;
    assert.strictEqual(r.status, 200); assert.strictEqual(b.found, true); assert.strictEqual(b.name, 'Paul K.'); assert.strictEqual(b.mode, 'real'); assert.strictEqual(b.range, 'month');
    assert.deepStrictEqual([b.from, b.to, b.days, b.today, b.live, b.granularity], ['2026-09-15', '2026-10-14', 30, '2026-10-14', true, 'day']);
    assert.deepStrictEqual(b.prev, { from: '2026-08-16', to: '2026-09-14', days: 30 });
    assert(b.spellings.includes('Paul_K') && typeof b.knownFrom === 'string');
    const e = expectFor(groupOf('person', 'Paul K.'), b.from, b.to), ep = expectFor(groupOf('person', 'Paul K.'), b.prev.from, b.prev.to);
    const want = { replies: [e.sent, ep.sent], orders: [e.orders, ep.orders], customers: [e.customers, ep.customers], messages: [e.sent, ep.sent], messagesPerCustomer: [e.avg, ep.avg], maxPerCustomer: [e.max, ep.max],
      repliesPerDay: [Math.round(e.sent / e.activeDays * 10) / 10, Math.round(ep.sent / ep.activeDays * 10) / 10], daysActive: [e.activeDays, ep.activeDays] };
    for (const [k, [v, p]] of Object.entries(want)) {
      const m = b.totals[k]; assert(m && typeof m.label === 'string' && m.label && typeof m.def === 'string' && m.def.length > 30, k + ' has label and definition');
      assert.strictEqual(m.unit, 'count'); assert.strictEqual(m.value, v, k + ' value'); assert.strictEqual(m.prev, p, k + ' prev'); assert.strictEqual(m.delta, Math.round((v - p) * 10) / 10, k + ' delta');
      assert.strictEqual(m.deltaPct, p ? Math.round((v - p) / Math.abs(p) * 1000) / 10 : null, k + ' deltaPct'); assert.strictEqual(m.better, null, 'no verdict: more is not better'); assert.strictEqual(m.estimated, false);
    }
    assert.strictEqual(b.series.length, 30); b.series.forEach(pt => { assert.deepStrictEqual(Object.keys(pt).sort(), ['activeDays', 'customers', 'day', 'days', 'firstAt', 'lastAt', 'messages', 'orders', 'replies', 'sent', 'to'].sort()); });
    assert.strictEqual(b.series.reduce((n, x) => n + x.replies, 0), e.sent);
    assert.strictEqual(b.hours.length, 24); assert.strictEqual(b.hours.reduce((n, h) => n + h.replies, 0), e.sent); assert(b.hours.every((h, i) => h.hour === i && h.replies === h.messages));
    const pc = b.perCustomer; assert.deepStrictEqual([pc.average, pc.max, pc.total], [e.avg, e.max, e.customers]);
    assert.strictEqual(pc.distribution.length, 5); assert.strictEqual(pc.distribution[4].plus, true); assert.strictEqual(pc.distribution.reduce((n, x) => n + x.customers, 0), e.customers);
    for (const m of [1, 2, 3, 4]) assert.strictEqual(pc.distribution[m - 1].customers, [...e.perCust.values()].filter(n => n === m).length);
    assert.strictEqual(pc.distribution[4].customers, [...e.perCust.values()].filter(n => n >= 5).length);
    assert(pc.top.length <= 10 && pc.top.length === e.customers); pc.top.forEach((t, i) => { assert(i === 0 || pc.top[i - 1].messages >= t.messages, 'most first'); assert.deepStrictEqual(Object.keys(t).sort(), ['customer', 'lastAt', 'messages', 'orderIds', 'orders', 'replies', 'rid'].sort()); assert.strictEqual(t.replies, t.messages); });
    const top0 = pc.top[0], hold = groupOf('person', 'Paul K.').filter(x => x.name === top0.customer && torontoDay(x.t) >= b.from && torontoDay(x.t) <= b.to && x.order).sort((x, y) => y.t - x.t);
    assert.strictEqual(top0.rid, hold.length ? hold[0].order : '', 'rid is that customer\'s newest order in the window');
    const unk = expectFor(groupOf('unknown'), b.from, b.to).sent; assert.deepStrictEqual(b.unknown, { replies: unk, messages: unk }); assert(unk > 0, 'the shop has replies with no name');
    assert(Array.isArray(b.notes) && b.notes.length >= 1 && !b.partial && b.reads.documents > 0);
    // without compare: prev is null and so are the deltas
    const nc = (await personInbox(fresh, 'Paul K', 'week')).body; assert.strictEqual(nc.prev, null); assert(nc.totals.replies.prev === null && nc.totals.replies.delta === null);
    // a week is daily, a year weekly, with the same rules as op person
    const yr = (await personInbox(fresh, 'Paul K', 'year')).body; assert.strictEqual(yr.granularity, 'week'); assert.strictEqual(yr.from, '2025-10-15'); assert.strictEqual(yr.totals.replies.estimated, true);
    // not an inbox person (a Sorting-only name): found:false and dashes, never zeros
    const nb = (await personInbox(fresh, 'Quentin Q', 'week', { compare: true })).body;
    assert.strictEqual(nb.found, false); assert(Object.values(nb.totals).every(m => m.value === null && m.prev === null), 'dashes'); assert(nb.series.every(x => x.replies === null)); assert(nb.hours.every(h => h.replies === null));
    assert(nb.notes.some(x => /No inbox account or reply/.test(x)));
    // an inbox account with no replies in the window is found, with real zeros
    fresh.put('EtsyMail_Operators', 'newbie', { displayName: 'Nell N.', role: 'operator', passwordHash: HASH }); EP.resetCache();
    const f2 = fakeStore(); for (const [c, m] of fresh.colls) for (const [id, d] of m) f2.put(c, id, d);
    const z = (await personInbox(f2, 'Nell N.', 'week')).body; assert.strictEqual(z.found, true); assert.strictEqual(z.totals.replies.value, 0); assert(z.series.every(x => x.replies === 0));
    assert.strictEqual(z.totals.messagesPerCustomer.value, null, 'no customers: no average'); assert.strictEqual(z.totals.repliesPerDay.value, null);
  });

  await test('7b · personOrders with station "inbox": the orders the person SENT a reply on (not every conversation they opened), with replies and messages per order', async () => {
    EP.resetCache(); const st = newShop(); truth.length = 0; NOW = at('2026-10-14T16:00:00Z');
    const T0 = at('2026-10-14T13:00:00Z');
    for (const [by, th, k] of [['Paul_K', 'T1', 0], ['Paul_K', 'T1', 1], ['Paul_K', 'T1', 2], ['Paul_K', 'T2', 3], ['Paul_K', 'T2', 4], ['Paul_K', 'T3', 5], ['ana', 'T4', 6]]) await send(st, by, th, T0 + k * 600000);
    await send(st, 'Paul_K', 'T4', at('2026-10-12T15:00:00Z'));
    // Paul also OPENED order 9 and finished order 8 at Sorting: rollups say he touched them, but he sent no reply on them
    st.put('Efficiency_Daily', '2026-10-14__Paul K', { person: 'Paul K', day: '2026-10-14', events: 4, stations: { inbox: { notes: 2 }, sorting: { scans: 1, completes: 1, parts: 1, orders: 1 } }, touched: { '3900000009': { inbox: true }, '3900000008': { sorting: true } } });
    const r = await call(st, { op: 'personOrders', name: 'Paul K', station: 'inbox', from: '2026-10-01', to: '2026-10-14' });
    assert.strictEqual(r.status, 200); const b = r.body;
    assert.strictEqual(b.found, true); assert.deepStrictEqual(b.orders.map(o => o.rid), ['3900000001', '3900000002', '3900000004'].sort((x, y) => 0) && b.orders.map(o => o.rid), 'order of the rows is the page order');
    const byRid = Object.fromEntries(b.orders.map(o => [o.rid, o]));
    assert.deepStrictEqual(Object.keys(byRid).sort(), ['3900000001', '3900000002', '3900000004'], 'only orders with a sent reply; not the opened order 9, not Sorting\'s order 8, not the conversation with no order');
    assert.deepStrictEqual([byRid['3900000001'].replies, byRid['3900000001'].messages], [3, 3]); assert.deepStrictEqual([byRid['3900000002'].replies, byRid['3900000002'].messages], [2, 2]);
    assert.deepStrictEqual([byRid['3900000004'].replies, byRid['3900000004'].messages], [1, 1], 'Paul\'s reply on order 4, not Ana\'s');
    assert.strictEqual(b.total, 3); assert(b.orders.every(o => o.stations.join() === 'inbox' && typeof o.at === 'number' && o.qr.text === o.rid));
    assert.strictEqual(b.orders[b.orders.length - 1].rid, '3900000004', 'newest day first: the 12th is last');
    // the other stations\' lists are unchanged (order 8 at Sorting), and the sandbox lists nothing of the inbox
    const sort = (await call(st, { op: 'personOrders', name: 'Paul K', station: 'sorting', from: '2026-10-01', to: '2026-10-14' })).body;
    assert.deepStrictEqual(sort.orders.map(o => o.rid), ['3900000008']); assert(!('replies' in sort.orders[0]));
    const w = (await call(st, { op: 'personOrders', name: 'Paul K', station: 'inbox', from: '2026-10-01', to: '2026-10-14' })).body;
    assert.strictEqual(w.total, 3);
    EP.resetCache(); const sb = (await call(newShop(), { op: 'personOrders', name: 'Paul K', station: 'inbox', sandbox: true, from: '2026-10-01', to: '2026-10-14' })).body; assert.strictEqual(sb.total, 0);
    // no sent-reply record exists anywhere yet: the list is what the person's inbox activity touched (the behaviour before this existed)
    EP.resetCache(); const old = newShop(); old.put('Efficiency_Daily', '2026-10-14__Paul K', { person: 'Paul K', day: '2026-10-14', events: 2, stations: { inbox: { notes: 2 } }, touched: { '3900000009': { inbox: true } } });
    const fb = (await call(old, { op: 'personOrders', name: 'Paul K', station: 'inbox', from: '2026-10-01', to: '2026-10-14' })).body;
    assert.deepStrictEqual(fb.orders.map(o => o.rid), ['3900000009']); assert(!('replies' in fb.orders[0]), 'no reply counts where nothing was recorded');
    // a person with no replies: found false, nothing listed
    const none = (await call(st, { op: 'personOrders', name: 'Quentin Q', station: 'inbox', from: '2026-10-01', to: '2026-10-14' })).body; assert.strictEqual(none.total, 0); assert.strictEqual(none.found, false);
  });

  await test('7c · the live board: the inbox station carries today\'s block (people + unknown, never auto); a failing read leaves it out and the board stays whole', async () => {
    EP.resetCache(); const st = newShop(); truth.length = 0; NOW = at('2026-10-14T16:00:00Z');
    let b = (await call(st, { op: 'live' })).body, ib = b.stations.find(s => s.key === 'inbox');
    assert(ib && ib.inbox && ib.inbox.day === '2026-10-14' && ib.inbox.replies === null && ib.inbox.byPerson.length === 0, 'no record yet: a dash, never a zero');
    const T0 = at('2026-10-14T13:00:00Z'); let k = 0;
    for (const [by, th, auto] of [['Paul_K', 'T1'], ['Paul_K', 'T1'], ['Paul_K', 'T2'], ['ana', 'T1'], ['', 'T4'], [AUTO, 'T5', true], ['giovanna', 'T5'], ['gio_c', 'T5']]) await send(st, by, th, T0 + (k++) * 60000, { auto });
    await send(st, 'Paul_K', 'T3', at('2026-10-13T15:00:00Z'));
    NOW += 30000;
    b = (await call(st, { op: 'live' })).body; ib = b.stations.find(s => s.key === 'inbox');
    assert.deepStrictEqual([ib.inbox.day, ib.inbox.replies, ib.inbox.orders, ib.inbox.customers, ib.inbox.messages, ib.inbox.unknown], ['2026-10-14', 7, 4, 4, 7, 1], 'people + unknown, today only, no auto, no yesterday');
    assert.deepStrictEqual(ib.inbox.byPerson, [{ name: 'Paul K.', replies: 3, orders: 2, customers: 2, messages: 3 }, { name: 'Giovanna', replies: 2, orders: 1, customers: 1, messages: 2 }, { name: 'Ana M.', replies: 1, orders: 1, customers: 1, messages: 1 }]);
    assert(b.stations.filter(s => s.key !== 'inbox').every(s => !('inbox' in s)), 'only the inbox station');
    assert(!b.partial, 'the live answer is whole');
    // the replies could not be read: the block is simply not there, the board is whole and not partial
    EP.resetCache(); const bad = newShop(); bad.fail('EtsyMail_ReplyDaily'); NOW += 60000;
    const lb = (await call(bad, { op: 'live' })).body; assert.strictEqual(lb.ok, true); assert(!lb.stations.find(s => s.key === 'inbox').inbox); assert(!lb.partial);
  });

  /* ═══ 8 · sandbox separate, cost ═══ */
  await test('8 · sandbox: nothing of the inbox is read or shown; real and sandbox never mix', async () => {
    EP.resetCache(); const st = newShop(); truth.length = 0; NOW = at('2026-10-14T16:00:00Z');
    await send(st, 'Paul_K', 'T1', NOW - HOUR); st.clear();
    const a = (await inbox(st, { range: 'week', sandbox: true })).body, p = (await personInbox(st, 'Paul K', 'week', { sandbox: true })).body, o = (await call(st, { op: 'personOrders', name: 'Paul K', station: 'inbox', sandbox: true })).body;
    const l = (await call(st, { op: 'live', sandbox: true })).body;
    for (const x of [a, p]) { assert.strictEqual(x.ok, true); assert.strictEqual(x.mode, 'sandbox'); assert(x.notes.some(n => /no inbox/i.test(n))); }
    assert.deepStrictEqual(a.people, []); assert.strictEqual(p.found, false); assert.strictEqual(o.total, 0); assert(!l.stations.find(s => s.key === 'inbox').inbox);
    assert.strictEqual(st.docsOf('EtsyMail_ReplyDaily') + st.docsOf('EtsyMail_DraftOutcomes') + st.docsOf('EtsyMail_Threads') + st.docsOf('EtsyMail_Operators'), 0, 'the sandbox reads no inbox collection');
    const real = (await inbox(st, { range: 'week' })).body; assert.strictEqual(real.mode, 'real'); assert.strictEqual(real.people.length, 1);
    assert.strictEqual(st.writes.length, 0, 'reading never writes');
  });

  await test('8b · cost: a cold year is the days that have replies plus a few; a warm repeat reads nothing; a day window reads only today; the board block reads today\'s one document', async () => {
    const mk = () => { EP.resetCache(); NOW = at('2026-10-14T16:00:00Z'); const f = fakeStore(); for (const [c, m] of BIG.colls) for (const [id, d] of m) f.put(c, id, d); return f; };
    const days = BIG.count('EtsyMail_ReplyDaily'), aliasDocs = st => st.docsOf('config');
    let st = mk(); const c1 = (await inbox(st, { range: 'year' })).body;
    assert.strictEqual(c1.reads.replyQueries, 1, 'one range query for the year'); assert.strictEqual(c1.reads.replyDocs, days, 'one document per day with replies: ' + days);
    assert.strictEqual(c1.reads.operatorDocs, 4); assert.strictEqual(c1.reads.trackDocs, 1, 'the first daily document (no outcome exists in this shop)'); assert.strictEqual(c1.reads.outcomeDocs, 0, 'no history tail without outcomes'); assert.strictEqual(c1.reads.threadDocs, 0);
    assert.strictEqual(c1.reads.documents, days + 5); assert.strictEqual(st.docsRead() - aliasDocs(st), c1.reads.documents, 'the answer counts exactly what was read (the alias list is the portal\'s own)');
    assert(days <= 330 && days > 200, 'about a year of days: ' + days);
    st.clear(); NOW += 5000; const c2 = (await inbox(st, { range: 'year' })).body;
    assert.strictEqual(c2.reads.documents, 0, 'a warm repeat reads no inbox document'); assert.strictEqual(st.docsRead() - aliasDocs(st), 0);
    st.clear(); NOW += 25000; const c3 = (await inbox(st, { range: 'year' })).body;
    assert.strictEqual(c3.reads.replyDocs, 1, 'half a minute later only today\'s document is read again'); assert.strictEqual(c3.reads.documents, 1);
    st.clear(); NOW += 40 * 60000; const c4 = (await inbox(st, { range: 'year' })).body;
    assert.strictEqual(c4.reads.replyQueries, 1); assert.strictEqual(c4.reads.replyDocs, days, 'past days are read again after 30 minutes (one query)'); assert.strictEqual(c4.reads.trackDocs, 0, 'where records begin is kept an hour');
    assert.strictEqual(c4.reads.operatorDocs, 4, 'the operators are read again after 10 minutes');
    st.clear(); NOW += 30 * 60000; const c5 = (await inbox(st, { range: 'year' })).body; assert.strictEqual(c5.reads.trackDocs, 1, 'and again after the hour');
    // another viewer, another person, another window: the same caches
    st = mk(); await inbox(st, { range: 'year' }); st.clear();
    const p1 = (await personInbox(st, 'Ana M.', 'quarter', { compare: true })).body; assert.strictEqual(p1.reads.documents, 0, 'a person page after the overview reads nothing');
    // a day window reads only today: 1 query, 1 document, plus the first-record lookup and the operators
    st = mk(); const d1 = (await inbox(st, { range: 'day', windows: ['day'] })).body;
    assert.deepStrictEqual([d1.reads.replyQueries, d1.reads.replyDocs], [1, 1]); assert.strictEqual(st.reads.filter(r => r.name === 'EtsyMail_ReplyDaily' && r.kind === 'query' && r.filters.length).every(r => r.vals[0] === '2026-10-14' && r.vals[1] === '2026-10-14'), true);
    assert.deepStrictEqual(Object.keys(d1.windows), ['day']);
    // the board block: today's document only (and the operators once)
    st = mk(); await call(st, { op: 'live' }); const lr = st.reads.filter(r => r.name === 'EtsyMail_ReplyDaily');
    assert.deepStrictEqual(lr.filter(r => r.filters && r.filters.length).map(r => r.vals.join('..')), ['2026-10-14..2026-10-14']); st.clear(); NOW += 3000; await call(st, { op: 'live' });
    assert.strictEqual(st.docsOf('EtsyMail_ReplyDaily') + st.docsOf('EtsyMail_Operators') + st.docsOf('EtsyMail_DraftOutcomes'), 0, 'the board polls every 3 s: warm = nothing');
    // an outage of the replies: the answer says so (partial), and with nothing readable at all it is a 503, not a zero
    st = mk(); st.fail('EtsyMail_ReplyDaily'); const o1 = await inbox(st, { range: 'week' }); assert.strictEqual(o1.status, 503); assert.strictEqual(o1.body.ok, false);
    st = mk(); st.fail('EtsyMail_Operators'); const o2 = (await inbox(st, { range: 'week' })).body; assert.strictEqual(o2.partial, true); assert(o2.errors.some(e => /^operators:/.test(e))); assert(o2.people.length > 0, 'the numbers still show; names fall back to the account name');
    assert.strictEqual(st.writes.length, 0);
    // the passcode never reaches a log
    assert(!logs.join('\n').includes(PASS) && !logs.join('\n').includes(HASH) && !bodies.join('\n').includes(HASH), 'no passcode or password hash in any log or answer');
  });

  say(`${passed} sections passed`);
})().catch(e => { process.stdout.write('FAIL ' + (e && e.stack || e) + '\n'); process.exit(1); });
