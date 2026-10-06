// Issues attributed to one employee, success / failure rates and contact (inbox) rates:
// netlify/functions/_employeeIssues.js (the reader), _activityKinds.js (the one classifier) and the issue counters the rollup
// writer (_stationActivity.js) keeps. Offline: Firestore is a Map with transactions, nested merges, increments and equality /
// range queries; the clock is faked; events go in through the REAL writer and come out through the REAL reader.
//   1 · a person with 2 reprints, 1 scan the page marked 'again', 1 undone completion, 1 completion reopened by someone else, 1 order that came back,
//       1 QA flag, 1 cancelled order, 1 lookup failure, 2 held / skipped cards, 3 failed inbox replies and 40 clean orders:
//       counts per kind, items with order links, rates with their numerators and denominators, definitions on everything
//   2 · the counters and the events agree: the same days counted from events (no counters) give the same kinds
//   3 · old days without the counters show dashes (never zeros); a day inside the event window is counted from its events
//   4 · names: spellings merge (Ana_M, Ana M.), the inbox name joins through the alias map and stays apart without it
//   5 · Real and Sandbox never mix; empty data; no PIN, no passcode, no digits-only name anywhere; read cost per call; paging
//   6 · the real `person` op (employeeEfficiency.js + _employeeProfile.js) hands the same context and merges the answer
//   node tests/stations/employee-issues.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

/* ── a Map-backed Firestore: transactions, nested merges, increments, equality and range queries ── */
const INC = n => ({ __inc: n }), TSV = { __ts: true };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && v.__inc == null && !v.__ts;
const NOW0 = Date.parse('2026-10-05T16:00:00Z');                 // 12:00 on Monday 5 Oct in New York (EDT)
let NOW = NOW0;
function apply(prev, data, merge) {
  const out = merge && prev ? JSON.parse(JSON.stringify(prev)) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v && v.__inc != null) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = NOW;
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge);
    else out[k] = v;
  }
  return out;
}
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
function makeStore() {
  const docs = new Map(), reads = [], failing = new Set();
  const ref = p => ({ path: p, id: p.split('/').pop(), get: async () => ({ exists: docs.has(p), data: () => (docs.has(p) ? JSON.parse(JSON.stringify(docs.get(p))) : undefined) }) });
  const snap = r => ({ exists: docs.has(r.path), data: () => docs.get(r.path), ref: r });
  function query(name, filters, order, lim) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim),
      limit: n => query(name, filters, order, n),
      get: async () => {
        if (failing.has(name)) throw Object.assign(new Error('14 UNAVAILABLE: synthetic outage of ' + name), { code: 14 });
        let list = [...docs].filter(([k]) => k.startsWith(name + '/')).map(([k, d]) => ({ id: k.slice(name.length + 1), d }));
        for (const [f, op, v] of filters) list = list.filter(({ d }) => {
          const x = d[f]; if (x === undefined || (typeof x === 'object') !== (typeof v === 'object')) return false;
          return op === '==' ? x === v : op === '>=' ? x >= v : op === '>' ? x > v : op === '<' ? x < v : op === '<=' ? x <= v : false;
        });
        if (order) { const [f, dir] = order; list = list.filter(({ d }) => d[f] !== undefined).sort((p, q) => (p.d[f] < q.d[f] ? -1 : p.d[f] > q.d[f] ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) list = list.slice(0, lim);
        reads.push({ name, n: list.length, filters: filters.map(f => f[0]) });
        return { docs: list.map(({ id, d }) => ({ id, data: () => JSON.parse(JSON.stringify(d)) })), size: list.length };
      }
    };
  }
  const db = {
    collection: name => Object.assign(query(name, [], null, null), { doc: id => ref(name + '/' + id) }),
    runTransaction: async fn => {
      const writes = [];
      const out = await fn({ getAll: (...rs) => Promise.resolve(rs.map(snap)), get: r => Promise.resolve(snap(r)), set: (r, d, o) => writes.push([r.path, d, o]) });
      for (const [p, d, o] of writes) docs.set(p, apply(docs.get(p), d, !!(o && o.merge)));
      return out;
    }
  };
  return { db, docs, reads, fail: n => failing.add(n), heal: n => failing.delete(n) };
}

/* ── the modules under test (the writer is the real one) ── */
const SA = require(path.join(root, 'netlify/functions/_stationActivity.js'));
const K = require(path.join(root, 'netlify/functions/_activityKinds.js'));
const ISS = require(path.join(root, 'netlify/functions/_employeeIssues.js'));
const FV = { serverTimestamp: () => TSV, increment: INC };
const realNow = Date.now; Date.now = () => NOW;
const say = (...a) => process.stdout.write(a.join(' ') + '\n');

/* ── builders ── */
let SEQ = 0;
const Z = iso => Date.parse(iso);
const ev = (person, station, action, at, o = {}) => ({ id: `${station}-1_ABCD_${++SEQ}_${at}`, station, device: station + '-1', computer: 'pc-ABCDEFGHJKMN', session: '', person, action,
  orderId: o.orderId || '', line: '', sku: '', parts: o.parts || 0, orders: o.orders || 0, detail: o.detail || '', at, seq: SEQ, sincePrevMs: o.since == null ? 20000 : o.since });
const nyDay = at => SA.nyDayHour(at).day;
async function emit(store, evs, prefix = '') {
  const last = Math.max(...evs.map(e => e.at)); NOW = last + 1000;      // the commit comes just after the last event of the batch
  const r = await SA.add(store.db, FV, evs, { prefix });
  NOW = NOW0;
  assert.strictEqual(r.refused, 0, 'every fixture event passes the door');
  return r;
}
const oid = n => String(3520100000 + n);                              // 10-digit receipt ids

/* ── the shop: Ana M. (with an inbox account called Ana), Tom R. as the other person ── */
const ANA = 'Ana M.', ANA_INBOX = 'Ana', TOM = 'Tom R.';
const LOG = [];                                                       // every event Ana's account wrote in the real store (for the expectations)
const keep = e => { LOG.push(Object.assign({}, e, { day: nyDay(e.at) })); return e; };
async function build(store) {
  const D1 = Z('2026-10-01T13:00:00Z'), D2 = Z('2026-10-02T13:00:00Z');
  // 40 clean orders at Assembly: scan, then the Team stamp (complete, one order), 20 on each of two days
  for (const [d0, off] of [[D1, 0], [D2, 20]]) {
    const evs = [];
    for (let i = 0; i < 20; i++) { const id = oid(off + i + 1), t = d0 + i * 120000; evs.push(keep(ev(ANA, 'assembly', 'scan', t, { orderId: id, parts: 2, detail: 'typed' })), keep(ev(ANA, 'assembly', 'complete', t + 30000, { orderId: id, parts: 2, orders: 1, detail: 'Team stamp Done' }))); }
    await emit(store, evs);
  }
  // 10-02, later: the issues
  const T = Z('2026-10-02T15:00:00Z'), min = n => T + n * 60000;
  await emit(store, [
    keep(ev(ANA, 'shipping', 'scan', min(0), { orderId: oid(101), parts: 1, detail: 'typed' })), keep(ev(ANA, 'shipping', 'print', min(1), { orderId: oid(101), detail: 'Chit Chats label' })),
    keep(ev(ANA, 'shipping', 'complete', min(2), { orderId: oid(101), parts: 1, orders: 1, detail: 'Etsy order completed' })),
    keep(ev(ANA, 'shipping', 'scan', min(3), { orderId: oid(102), parts: 1, detail: 'typed' })), keep(ev(ANA, 'shipping', 'print', min(4), { orderId: oid(102), detail: 'Chit Chats label' })),
    keep(ev(ANA, 'shipping', 'complete', min(5), { orderId: oid(102), parts: 1, orders: 1, detail: 'Etsy order completed' })),
    keep(ev(ANA, 'shipping', 'print', min(6), { orderId: oid(101), detail: 'reprint' })), keep(ev(ANA, 'shipping', 'print', min(7), { orderId: oid(102), detail: 'reprint' })),   // 2 reprints
    keep(ev(ANA, 'assembly', 'reject', min(8), { orderId: oid(103), detail: 'flag to Team: rework' })),                                                                        // 1 QA flag
    keep(ev(ANA, 'assembly', 'reject', min(9), { orderId: oid(104), detail: 'cancelled order alert' })),                                                                       // 1 cancelled order
    keep(ev(ANA, 'assembly', 'error', min(10), { orderId: oid(105), detail: 'order lookup failed' })),                                                                          // 1 lookup failure
    keep(ev(ANA, 'sorter', 'complete', min(11), { orderId: oid(106), parts: 3, orders: 1, detail: 'Complete Order' })),
    keep(ev(ANA, 'sorter', 'undo', min(12), { orderId: oid(106), parts: 3, orders: 1, detail: 'completion undone' })),                                                          // 1 undone
    keep(ev(ANA, 'sorter', 'reject', min(13), { orderId: oid(107), detail: 'skipped: unmatchedSku' })),                                                                         // 1 unknown SKU
    keep(ev(ANA, 'sorter', 'reject', min(14), { orderId: oid(108), detail: 'held: customOrder' })),                                                                             // 1 held
    keep(ev(ANA, 'sorter', 'complete', min(15), { orderId: oid(109), parts: 2, orders: 1, detail: 'Complete Order' })),                                                         // reopened later by Tom
    keep(ev(ANA, 'assembly', 'scan', min(16), { orderId: oid(110), parts: 1, detail: 'typed' })), keep(ev(ANA, 'assembly', 'complete', min(17), { orderId: oid(110), parts: 1, orders: 1, detail: 'Team stamp Done' }))  // came back
  ]);
  await emit(store, [keep(ev(ANA, 'welding', 'scan', min(30), { orderId: oid(103), parts: 1, detail: 'phone scan · again' }))]);                                                  // 1 scan the page marked 'again' (later batch)
  // Tom: reopens Ana's sorter completion; scans her assembled order at Sorting again (it came back)
  await emit(store, [ev(TOM, 'sorter', 'note', min(90), { orderId: oid(109), detail: 'completed order reopened' }), ev(TOM, 'sorting', 'scan', min(120), { orderId: oid(110), parts: 1, detail: 'typed' })]);
  // the inbox (the account is called "Ana"): 3 drafts, 6 sent, 2 delivered, 3 failed, 1 unconfirmed, 2 done, 1 reopened
  const I = Z('2026-10-01T18:00:00Z'), im = n => I + n * 60000;
  await emit(store, [
    keep(ev(ANA_INBOX, 'inbox', 'note', im(0), { detail: 'reply drafted' })), keep(ev(ANA_INBOX, 'inbox', 'note', im(1), { detail: 'reply drafted · follow-up' })), keep(ev(ANA_INBOX, 'inbox', 'note', im(2), { detail: 'reply drafted · revised' })),
    keep(ev(ANA_INBOX, 'inbox', 'note', im(3), { orderId: oid(201), detail: 'reply sent · ai draft edited · first reply 12m' })), keep(ev(ANA_INBOX, 'inbox', 'note', im(4), { detail: 'reply sent · ai draft edited' })),
    keep(ev(ANA_INBOX, 'inbox', 'note', im(5), { detail: 'reply sent · ai draft · first reply 30m' })), keep(ev(ANA_INBOX, 'inbox', 'note', im(6), { detail: 'reply sent · ai draft · waited 90m' })),
    keep(ev(ANA_INBOX, 'inbox', 'note', im(7), { detail: 'reply sent' })), keep(ev(ANA_INBOX, 'inbox', 'note', im(8), { detail: 'reply sent · design note' })),
    keep(ev(ANA_INBOX, 'inbox', 'note', im(10), { detail: 'reply delivered' })), keep(ev(ANA_INBOX, 'inbox', 'note', im(11), { detail: 'reply delivered · images not sent' })),
    keep(ev(ANA_INBOX, 'inbox', 'error', im(12), { orderId: oid(202), detail: 'reply failed · QUEUED_EXPIRED' })), keep(ev(ANA_INBOX, 'inbox', 'error', im(13), { detail: 'reply failed · QUEUED_EXPIRED' })), keep(ev(ANA_INBOX, 'inbox', 'error', im(14), { detail: 'reply failed' })),
    keep(ev(ANA_INBOX, 'inbox', 'note', im(15), { detail: 'reply unconfirmed' })),
    keep(ev(ANA_INBOX, 'inbox', 'complete', im(16), { detail: 'conversation done' })), keep(ev(ANA_INBOX, 'inbox', 'complete', im(17), { detail: 'conversation done' })), keep(ev(ANA_INBOX, 'inbox', 'undo', im(18), { detail: 'conversation reopened' }))
  ]);
  // a PIN-looking detail (the door blanks it) and a person that is only digits (the door refuses it)
  await emit(store, [keep(ev(ANA, 'assembly', 'note', Z('2026-10-02T19:00:00Z'), { detail: 'typed pin 123456 here' }))]);
  NOW = Z('2026-10-02T19:02:00Z');
  const r = await SA.add(store.db, FV, [ev('123456', 'assembly', 'scan', Z('2026-10-02T19:01:00Z'), { orderId: oid(300) })], {}); assert.strictEqual(r.refused, 1, 'a digits-only person is refused by the door');
  NOW = NOW0;
  // a day inside the event window from BEFORE the counters existed: written through the writer, then stripped to the old shape ("Ana_M" is a spelling of the same person)
  const L = Z('2026-09-28T14:00:00Z');
  await emit(store, [keep(ev('Ana_M', 'assembly', 'scan', L, { orderId: oid(401), parts: 1, detail: 'typed' })), keep(ev('Ana_M', 'assembly', 'reject', L + 60000, { orderId: oid(401), detail: 'cancelled order alert' })),
    keep(ev('Ana_M', 'assembly', 'scan', L + 120000, { orderId: oid(402), parts: 1, detail: 'typed' })), keep(ev('Ana_M', 'assembly', 'complete', L + 150000, { orderId: oid(402), parts: 1, orders: 1, detail: 'Team stamp Done' }))]);
  const lid = 'Efficiency_Daily/2026-09-28__Ana_M', ld = store.docs.get(lid); delete ld.ixv; for (const s of Object.values(ld.stations)) for (const k of Object.keys(s)) if (/^x_/.test(k)) delete s[k];
  // a day outside the window from before the counters existed: only the old counters
  const touched = {}; for (let i = 0; i < 30; i++) touched[oid(500 + i)] = { assembly: true };
  store.docs.set('Efficiency_Daily/2026-08-10__Ana M.', { day: '2026-08-10', person: ANA, v: 1, events: 70, firstAt: Z('2026-08-10T13:00:00Z'), lastAt: Z('2026-08-10T20:00:00Z'),
    stations: { assembly: { scans: 30, scanParts: 60, completes: 30, parts: 60, orders: 30, prints: 0, rejects: 2, errors: 1, notes: 0, undos: 1, undoParts: 2, undoOrders: 1, activeMs: 600000, idleMs: 0, firstAt: 1, lastAt: 2 } }, touched });
}
const ALIASES = { map: new Map([['ana m', 'ana m'], ['ana', 'ana m']]), display: new Map([['ana m', 'Ana M.']]) };
const ctxOf = (store, o = {}) => Object.assign({ db: store.db, prefix: '', now: NOW, today: '2026-10-05', aliases: ALIASES }, o);
const asks = (o = {}) => Object.assign({ name: ANA, from: '2026-08-01', to: '2026-10-05' }, o);
const kind = (r, k) => r.issues.byKind.find(x => x.kind === k);
const walk = (v, f) => { if (v && typeof v === 'object') for (const x of Object.values(v)) walk(x, f); else f(v); };

(async () => {
  const store = makeStore(), SEEN = [];
  await build(store);
  const roll = [...store.docs].filter(([k]) => k.startsWith('Efficiency_Daily/')).map(([, d]) => d);

  /* 1 · the person's counts, items, rates, definitions ─────────────────────────────────────────────────────────────── */
  const r = await ISS.issues(ctxOf(store), asks()); SEEN.push(r);
  assert.strictEqual(r.ok, true); assert.strictEqual(r.mode, 'real'); assert.strictEqual(r.name, 'Ana M.'); assert.strictEqual(r.found, true);
  const want = { undone: 2, reprint: 2, rescan: 1, heldOrSkipped: 1, unknownSku: 1, qaFlag: 1, cancelAlert: 2, refused: 0, lookupFailed: 1, failed: 0, replyFailed: 3, reopenedLater: 1, cameBack: 1 };
  for (const [k, n] of Object.entries(want)) assert.strictEqual(kind(r, k).count, n, 'kind ' + k);
  assert.deepStrictEqual(r.issues.byKind.map(x => x.kind), ['undone', 'reprint', 'rescan', 'heldOrSkipped', 'reopenedLater', 'cameBack', 'unknownSku', 'qaFlag', 'cancelAlert', 'refused', 'lookupFailed', 'failed', 'replyFailed']);
  assert.strictEqual(r.issues.total, 16); assert.strictEqual(r.issues.own, 6); assert.strictEqual(r.issues.order, 6); assert.strictEqual(r.issues.system, 4, 'own 6 + order 6 + system 4');
  assert.strictEqual(r.issues.daysActive, 4); assert.strictEqual(r.issues.daysCounted, 3, '10-01 and 10-02 from the counters, 09-28 from its events; the 08-10 day has neither');
  assert.strictEqual(kind(r, 'undone').complete, true, 'Undo presses are in the old counters: every day');
  assert.strictEqual(kind(r, 'reprint').complete, false); assert.strictEqual(kind(r, 'reprint').coverage, 'range-partial');
  for (const k of r.issues.byKind) { assert(k.definition && /\./.test(k.definition), k.kind + ' has a definition'); assert(k.how, k.kind + ' says how it is detected'); assert(['own', 'order', 'system'].includes(k.attribution)); }
  for (const k of ['reprint', 'cancelAlert', 'unknownSku']) assert(/not|no |missing|jam/i.test(kind(r, k).definition), k + ': the definition says what it is not');
  assert(/printer jam/.test(kind(r, 'reprint').definition) && /customer cancelled/.test(kind(r, 'cancelAlert').definition), 'the reprint can be a printer jam, a cancelled order is not the person');
  // items: newest first, each links an order and says why
  const items = r.issues.items;
  assert.strictEqual(r.issues.itemsTotal, items.length); assert(items.length >= 15);
  assert(items.every((x, i) => i === 0 || items[i - 1].at >= x.at), 'newest first');
  const item = (k, rid) => items.find(x => x.kind === k && (rid == null || x.rid === rid));
  assert.deepStrictEqual([item('qaFlag').rid, item('qaFlag').number, item('qaFlag').station, item('qaFlag').note], [oid(103), oid(103), 'assembly', 'Problem flagged to the Team: rework']);
  assert.strictEqual(items.filter(x => x.kind === 'reprint').length, 2); assert.deepStrictEqual(items.filter(x => x.kind === 'reprint').map(x => x.rid).sort(), [oid(101), oid(102)]);
  assert.strictEqual(item('rescan').rid, oid(103)); assert.strictEqual(item('rescan').station, 'welding'); assert.strictEqual(item('undone').rid, oid(106)); assert.strictEqual(item('unknownSku').rid, oid(107)); assert(/Skipped an Unknown SKU/.test(item('unknownSku').note));
  assert.strictEqual(item('heldOrSkipped').rid, oid(108)); assert(/Held a Review card \(custom order\)/.test(item('heldOrSkipped').note));
  assert.strictEqual(item('lookupFailed').rid, oid(105)); assert.strictEqual(items.filter(x => x.kind === 'replyFailed').length, 3);
  assert.strictEqual(item('reopenedLater').rid, oid(109)); assert(/another person/.test(item('reopenedLater').note) && !/Tom/.test(JSON.stringify(items)), 'the other person is never named');
  assert.strictEqual(item('cameBack').rid, oid(110)); assert(/Assembly/.test(item('cameBack').note) && /Sorting/.test(item('cameBack').note));
  assert(items.every(x => x.id && x.at > 0 && x.day && ['own', 'order', 'system'].includes(x.attribution) && x.note && x.label && x.source === 'events'));
  assert.strictEqual(items.filter(x => x.kind === 'cancelAlert').length, 2, 'the 09-28 cancel alert is an item too');
  // the rates: each with numerator, denominator, a definition, estimated and a coverage
  const ev0 = LOG.filter(e => e.station !== 'inbox'), cnt = (f, ex = 0) => ev0.filter(f).length + ex;
  const handled = new Map(); for (const e of ev0) if (e.orderId) { if (!handled.has(e.day)) handled.set(e.day, new Set()); handled.get(e.day).add(e.orderId); }
  const handledAll = [...handled.values()].reduce((n, s) => n + s.size, 0) + 30;             // + the 30 orders of the old 08-10 day
  const acts = ['scan', 'complete', 'print', 'reject', 'error', 'undo', 'note'].reduce((n, a) => n + cnt(e => e.action === a), 0) + (30 + 30 + 2 + 1 + 1 /* old day: scans completes rejects errors undos */);
  const R = r.rates;
  assert.deepStrictEqual(Object.keys(R), ['firstPass', 'reworkRate', 'successRate', 'failureRate', 'holdRate', 'reprintRate', 'rescanRate']);
  assert.deepStrictEqual([R.reworkRate.numerator, R.reworkRate.denominator], [cnt(e => e.action === 'undo', 1), cnt(e => e.action === 'complete', 30)]);
  assert.strictEqual(R.reworkRate.value, Math.round(R.reworkRate.numerator / R.reworkRate.denominator * 1000) / 10);
  assert.deepStrictEqual([R.failureRate.numerator, R.failureRate.denominator], [cnt(e => e.action === 'error', 1), acts]);
  assert.deepStrictEqual([R.successRate.numerator, R.successRate.denominator], [acts - cnt(e => e.action === 'error', 1), acts]);
  assert.deepStrictEqual([R.holdRate.numerator, R.holdRate.denominator], [cnt(e => e.action === 'reject', 2), handledAll]);
  assert.deepStrictEqual([R.reprintRate.numerator, R.reprintRate.denominator], [2, cnt(e => e.action === 'print')], 'reprints over label prints, on the counted days');
  assert.deepStrictEqual([R.rescanRate.numerator, R.rescanRate.denominator], [1, cnt(e => e.action === 'scan' && e.day !== '2026-08-10')], 'repeat scans over scans, on the counted days');
  // first pass: orders finished in the window (42 assembly + 2 shipping + 2 sorter) without an own Undo, Reopen or reprint
  const fin = new Set(LOG.filter(e => e.action === 'complete' && e.orders === 1 && e.station !== 'inbox' && e.day >= '2026-09-22').map(e => e.orderId + '|' + e.station));
  assert.strictEqual(R.firstPass.denominator, fin.size); assert.strictEqual(R.firstPass.denominator, 46);
  assert.strictEqual(R.firstPass.numerator, 46 - 3, 'the 2 reprinted shipping orders and the undone sorter order are not first pass');
  assert.strictEqual(R.firstPass.coverage, 'window'); assert.strictEqual(R.firstPass.estimated, true); assert(R.firstPass.why.length);
  for (const k of Object.keys(R)) { assert(R[k].definition && R[k].def === R[k].definition && R[k].label, k + ' definition'); assert.strictEqual(typeof R[k].estimated, 'boolean'); assert('numerator' in R[k] && 'denominator' in R[k] && R[k].unit === 'percent'); }
  assert(/own presses|only/i.test(R.firstPass.definition));
  // contact
  const C = r.contact;
  assert.deepStrictEqual([C.available, C.source], [true, 'counters']);
  assert.deepStrictEqual([C.drafted, C.sent, C.delivered, C.unconfirmed, C.failed, C.refused, C.edited, C.aiSentUnchanged], [3, 6, 2, 1, 3, 0, 2, 2]);
  assert.deepStrictEqual([C.deliveryRate, C.failureRate, C.editedShare], [40, 60, 50]);
  assert.deepStrictEqual([C.metrics.deliveryRate.numerator, C.metrics.deliveryRate.denominator, C.metrics.failureRate.numerator, C.metrics.failureRate.denominator], [2, 5, 3, 5], 'rates over replies with a known result');
  assert.strictEqual(C.medianFirstReplyMs, 21 * 60000, 'median of 12 and 30 minutes'); assert.strictEqual(C.meanFirstReplyMs, 21 * 60000); assert.strictEqual(C.firstReplyCount, 2);
  assert.deepStrictEqual([C.conversationsDone, C.reopened, C.reopenRate], [2, 1, 50]);
  for (const [k, m] of Object.entries(C.metrics)) assert(m.label && m.def && m.definition === m.def && 'value' in m, 'contact ' + k + ' has a label and a definition');
  assert.strictEqual(C.unconfirmed, 1, 'unconfirmed is shown apart');
  // definitions and the honest limits
  const D = r.definitions;
  assert(D.hygiene && /not a verdict/.test(D.hygiene)); assert.strictEqual(Object.keys(D.kinds).length, 13); assert.strictEqual(Object.keys(D.rates).length, 7); assert.strictEqual(Object.keys(D.contact).length, 16);
  assert(D.notDetected.some(x => /Cancels/.test(x.topic)) && D.notDetected.some(x => /Abandoned/.test(x.topic)) && D.notDetected.some(x => /edited/i.test(x.topic)));
  assert.strictEqual(r.estimated, true);
  assert(kind(r, 'rescan').estimated && kind(r, 'rescan').why.some(w => /Phone scans/.test(w)), 'a repeat scan at an assembly or shipping desktop is credited to its signed-in person');
  assert(kind(r, 'heldOrSkipped').estimated && kind(r, 'heldOrSkipped').why.some(w => /typed/.test(w)), 'the sorter name is typed');
  assert.strictEqual(kind(r, 'replyFailed').estimated, false); assert.strictEqual(kind(r, 'lookupFailed').attribution, 'system');
  assert.strictEqual(r.eventWindow.to, '2026-10-02'); assert.strictEqual(r.eventWindow.from, '2026-09-28');
  assert(r.issues.byDay.length === 4 && r.issues.byDay.find(d => d.day === '2026-08-10').total === null && JSON.stringify(r.issues.byDay.find(d => d.day === '2026-10-02')) === JSON.stringify({ day: '2026-10-02', total: 9, own: 5, system: 1, order: 3, source: 'counters' }), 'a total per day; null where the day is not counted');
  assert(r.notes.some(n => /counted on 3 of 4/.test(n)));
  say('1 counts, items, rates, contact, definitions');

  /* 2 · counters and events agree ──────────────────────────────────────────────────────────────────────────────────── */
  {
    const docsNoCounters = [...store.docs].filter(([k]) => k.startsWith('Efficiency_Daily/')).map(([, d]) => { const c = JSON.parse(JSON.stringify(d)); delete c.ixv; for (const s of Object.values(c.stations || {})) for (const k of Object.keys(s)) if (/^x_/.test(k)) delete s[k]; return c; });
    const r2 = await ISS.issues(ctxOf(makeFresh(store), { rollups: docsNoCounters }), asks({ from: '2026-09-22' }));
    const r1 = await ISS.issues(ctxOf(store), asks({ from: '2026-09-22' }));
    for (const k of ISS.KINDS.map(x => x.key)) assert.strictEqual(kind(r2, k).count, kind(r1, k).count, 'from events or from counters: ' + k);
    assert.strictEqual(r2.issues.byDay.every(d => d.source === 'events'), true); assert(r1.issues.byDay.some(d => d.source === 'counters'));
    assert.deepStrictEqual([r2.contact.sent, r2.contact.delivered, r2.contact.failed, r2.contact.edited, r2.contact.deliveryRate], [r1.contact.sent, r1.contact.delivered, r1.contact.failed, r1.contact.edited, r1.contact.deliveryRate]);
    assert.strictEqual(r2.contact.source, 'events'); assert.strictEqual(r2.issues.complete, true);
    // the classifier is one pure function: the phrases the pages write
    const k = (st, ac, d) => K.classify({ station: st, action: ac, detail: d }).kind;
    assert.deepStrictEqual([k('sorting', 'print', 'order QR sticker, again'), k('shipping', 'print', 'reprint'), k('shipping', 'print', 'Chit Chats label'), k('welding', 'scan', 'phone scan · again'), k('assembly', 'note', 'stamp again: Done'), k('assembly', 'note', 'QA 2 check'), k('welding', 'scan', 'phone scan'), k('shipping', 'scan', 'typed')],
      ['reprint', 'reprint', '', 'rescan', 'rescan', '', '', '']);              // a plain second scan is NOT a kind (a kind never depends on an earlier event)
    assert.deepStrictEqual([k('welding', 'reject', 'cancelled order'), k('sorting', 'reject', 'sticker blocked: cancelled order'), k('sorter', 'reject', 'cancelled order: print held (press again to go on)'), k('sorter', 'reject', 'held: heldOrder'), k('sorter', 'reject', 'skipped: unmatchedSku'), k('sorter', 'reject', 'engraving sent back to the words'),
      k('assembly', 'reject', 'flag to Team: damaged'), k('welding', 'reject', 'no stud earrings on this order'), k('design', 'reject', 'no recognised metal'), k('welding', 'error', 'Etsy order lookup failed: x'), k('sorting', 'error', 'order not found or not loaded (typed)'), k('sorter', 'error', 'Undo not completed'), k('inbox', 'error', 'reply failed · X'), k('inbox', 'error', 'reply not sent'), k('inbox', 'error', 'status change not saved'), k('inbox', 'undo', 'conversation reopened'), k('design', 'undo', 'completion undone')],
      ['cancelAlert', 'cancelAlert', 'cancelAlert', 'heldOrSkipped', 'unknownSku', 'heldOrSkipped', 'qaFlag', 'refused', 'refused', 'lookupFailed', 'lookupFailed', 'failed', 'replyFailed', 'replyFailed', 'failed', '', 'undone']);
    assert.deepStrictEqual(K.classify(null).kind, ''); assert.deepStrictEqual(K.classify({}, false), { kind: '', x: {} }); assert.doesNotThrow(() => K.classify({ station: 5, action: {}, detail: [] }));
    say('2 counters and events give the same kinds; the classifier reads every phrase the pages write');
  }
  function makeFresh(from) { const s = makeStore(); for (const [k, v] of from.docs) s.docs.set(k, JSON.parse(JSON.stringify(v))); return s; }

  /* 3 · old days: dashes, not zeros ────────────────────────────────────────────────────────────────────────────────── */
  {
    const old = await ISS.issues(ctxOf(store), asks({ from: '2026-08-01', to: '2026-08-31' })); SEEN.push(old);
    assert.strictEqual(old.found, true); assert.strictEqual(old.issues.daysActive, 1); assert.strictEqual(old.issues.daysCounted, 0);
    assert.strictEqual(old.issues.total, null, 'no day was counted: a dash'); assert.strictEqual(old.issues.own, null);
    for (const k of old.issues.byKind) { if (k.kind === 'undone') continue; assert.strictEqual(k.count, null, k.kind + ' is a dash on an old day'); assert.strictEqual(k.per100Orders, null); }
    assert.strictEqual(kind(old, 'undone').count, 1, 'the old counters still count the Undo');
    assert.strictEqual(old.rates.reprintRate.value, null); assert.strictEqual(old.rates.reprintRate.denominator, null); assert.strictEqual(old.rates.rescanRate.value, null); assert.strictEqual(old.rates.firstPass.value, null);
    assert.deepStrictEqual([old.rates.reworkRate.numerator, old.rates.reworkRate.denominator, old.rates.reworkRate.value], [1, 30, 3.3], 'what the old counters can say is said');
    assert.deepStrictEqual([old.rates.failureRate.numerator, old.rates.failureRate.denominator], [1, 30 + 30 + 2 + 1 + 1], 'errors over the old actions');
    assert.strictEqual(old.contact.available, false); assert.strictEqual(old.contact.deliveryRate, null); assert.strictEqual(old.eventWindow, null);
    assert(old.issues.items.length === 0 && old.issues.byDay[0].total === null);
    const nine = await ISS.issues(ctxOf(store), asks({ from: '2026-09-25', to: '2026-09-30' }));   // a day inside the event window with no counters: counted from its events
    assert.strictEqual(nine.issues.daysCounted, 1); assert.strictEqual(kind(nine, 'cancelAlert').count, 1); assert.strictEqual(kind(nine, 'reprint').count, 0, 'a counted day with none is a real zero'); assert.strictEqual(nine.issues.byDay[0].source, 'events');
    assert.strictEqual(nine.name, 'Ana M.', 'the Ana_M spelling is the same person');
    say('3 old days show dashes; a counted day with none is a real zero; the event window counts a day with no counters');
  }

  /* 4 · names ──────────────────────────────────────────────────────────────────────────────────────────────────────── */
  {
    const noAlias = await ISS.issues(ctxOf(makeFresh(store), { aliases: null }), asks({ from: '2026-09-22' })); SEEN.push(noAlias);
    assert.strictEqual(noAlias.contact.available, false, 'without the alias the inbox name "Ana" is another person');
    assert.strictEqual(kind(noAlias, 'replyFailed').count, 0); assert(noAlias.definitions.notDetected.some(x => /Inbox names/.test(x.topic)));
    assert.strictEqual(kind(noAlias, 'reprint').count, 2, 'Ana_M still joins Ana M. (the same folded name)');
    const inbox = await ISS.issues(ctxOf(makeFresh(store), { aliases: null }), asks({ name: 'Ana', from: '2026-09-22' })); SEEN.push(inbox);
    assert.strictEqual(inbox.contact.available, true); assert.strictEqual(inbox.contact.failed, 3); assert.strictEqual(kind(inbox, 'reprint').count, 0, 'nothing of the station work is under that name');
    const viaAlias = await ISS.issues(ctxOf(makeFresh(store)), asks({ name: 'Ana', from: '2026-09-22' }));
    assert.strictEqual(viaAlias.contact.failed, 3); assert.strictEqual(kind(viaAlias, 'reprint').count, 2, 'asking for "Ana" with the alias is Ana M.'); assert.strictEqual(viaAlias.name, 'Ana M.');
    const underscore = await ISS.issues(ctxOf(makeFresh(store), { aliases: null }), asks({ name: 'ANA_M', from: '2026-09-22' }));
    assert.strictEqual(kind(underscore, 'reprint').count, 2, 'any spelling of the name works');
    const hook = await ISS.issues(ctxOf(makeFresh(store), { aliases: null, nameKey: n => 'ana m' }), asks({ name: 'whoever', from: '2026-09-22' }));
    assert.strictEqual(kind(hook, 'reprint').count, 2, "the caller's own alias resolver wins");
    say('4 names: spellings merge, the alias joins the inbox, none leaks');
  }

  /* 5 · sandbox, empty, no PIN, cost, paging ───────────────────────────────────────────────────────────────────────── */
  {
    const sb = makeFresh(store);
    const T = Z('2026-10-02T15:00:00Z');
    await emit(sb, [ev(ANA, 'shipping', 'print', T, { orderId: oid(701), detail: 'reprint' }), ev(ANA, 'shipping', 'print', T + 60000, { orderId: oid(702), detail: 'reprint' }), ev(ANA, 'shipping', 'print', T + 120000, { orderId: oid(703), detail: 'again' }),
      ev(ANA, 'shipping', 'complete', T + 180000, { orderId: oid(701), parts: 1, orders: 1 })], 'Sandbox_');
    sb.reads.length = 0;
    const real = await ISS.issues(ctxOf(sb), asks({ from: '2026-09-22' })), box = await ISS.issues(ctxOf(sb, { mode: 'sandbox', prefix: 'Sandbox_' }), asks({ from: '2026-09-22' }));
    assert.strictEqual(real.mode, 'real'); assert.strictEqual(box.mode, 'sandbox');
    assert.strictEqual(kind(real, 'reprint').count, 2, 'the real store has its own 2 reprints'); assert.strictEqual(kind(box, 'reprint').count, 3, 'the sandbox has its 3 and none of the real ones');
    assert.strictEqual(box.issues.daysActive, 1); assert(box.issues.items.every(x => /^35201007/.test(x.rid)), 'sandbox items are sandbox orders');
    assert(!JSON.stringify(real).includes(oid(701)) && !JSON.stringify(box).includes(oid(101)), 'no order of the other store');
    assert(sb.reads.filter(x => /^Sandbox_/.test(x.name)).length > 0 && sb.reads.every((x, i) => x.name), 'the sandbox read its own collections');
    const modeFromPrefix = await ISS.issues(ctxOf(sb, { prefix: 'Sandbox_' }), asks({ from: '2026-09-22' }));
    assert.strictEqual(modeFromPrefix.mode, 'sandbox', 'the console context says it with prefix');
    // empty data
    const empty = await ISS.issues(ctxOf(store), asks({ name: 'Nobody Here' }));
    assert.strictEqual(empty.ok, true); assert.strictEqual(empty.found, false); assert.strictEqual(empty.issues.total, null); assert.strictEqual(empty.issues.daysActive, 0);
    assert(empty.issues.byKind.every(k => k.count === null && k.definition) && empty.issues.items.length === 0 && empty.issues.next === null);
    assert(Object.values(empty.rates).every(x => x.value === null && x.definition)); assert.strictEqual(empty.contact.available, false); assert.strictEqual(empty.contact.deliveryRate, null);
    assert(empty.definitions.hygiene && !empty.partial);
    const none = await ISS.issues(ctxOf(makeStore()), asks());
    assert.strictEqual(none.ok, true); assert.strictEqual(none.found, false); assert.strictEqual(none.partial, undefined, 'an empty store is not an error');
    // bad input
    for (const bad of [{ name: '' }, { name: '123456' }, { name: '12-34-56' }, { name: undefined }]) assert.strictEqual((await ISS.issues(ctxOf(store), asks(bad))).ok, false, 'name ' + JSON.stringify(bad.name));
    assert.strictEqual((await ISS.issues(ctxOf(store), asks({ from: 'nope' }))).ok, false); assert.strictEqual((await ISS.issues(ctxOf(store), asks({ from: '2020-01-01' }))).ok, false, 'a range over 731 days');
    assert.strictEqual((await ISS.issues(ctxOf(store), asks({ to: '2030-01-01', from: '2026-10-01' }))).to, '2026-10-05', 'never later than today');
    // no PIN, no digits-only strings, no passcode, no other person (every answer of this run)
    SEEN.push(box, empty);
    const all = JSON.stringify(SEEN);
    assert(!/(?<!\d)\d{6}(?!\d)/.test(all.replace(/\d{10}/g, '')), 'no 6-digit number anywhere');
    walk(SEEN, v => { if (typeof v === 'string') assert(!/^\d{4,8}$/.test(v), 'no digits-only string: ' + v); });
    assert(!/123456/.test(all), 'the PIN-looking detail never reaches the answer');
    assert(!/passcode|EDIT_PASSCODE|Tom/.test(all));
    // cost per call: preloaded rollups read nothing; events one query per person-day and spelling; one query per finished order checked
    const c1 = makeFresh(store); c1.reads.length = 0;
    const a = await ISS.issues(ctxOf(c1, { rollups: roll }), asks({ from: '2026-09-22' }));
    assert.deepStrictEqual([a.reads.rollups.queries, a.rollupsPreloaded], [0, true]);
    assert.strictEqual(a.reads.events.queries, 4, 'person-days with a rollup in the window: 09-28 (Ana_M), 10-01 (Ana M. and Ana), 10-02 (Ana M.)');
    assert.strictEqual(a.reads.orders.queries, 30, 'the newest 30 finished orders'); assert.strictEqual(a.reads.total.queries, 34);
    assert.strictEqual(c1.reads.length, 34, 'the counter matches what the store saw');
    assert(c1.reads.filter(x => x.name === 'Station_Activity' && x.filters.join() === 'day,person').length === 4, 'events: two equalities (day, person): no composite index');
    assert(a.reads.total.docs < 700, 'a few hundred documents: ' + a.reads.total.docs);
    const again = await ISS.issues(ctxOf(c1, { rollups: roll }), asks({ from: '2026-09-22' }));
    assert.deepStrictEqual([again.reads.events.queries, again.reads.orders.queries, again.reads.total.queries], [0, 0, 0], 'the second call is served from the instance cache');
    assert(again.reads.events.cached === 4 && again.reads.orders.cached === 30);
    const own = await ISS.issues(ctxOf(makeFresh(store)), asks({ from: '2026-09-22' }));
    assert.strictEqual(own.reads.rollups.queries, 1, 'without loaded rows: one range read'); assert.strictEqual(own.rollupsPreloaded, false);
    const lite = await ISS.issues(ctxOf(makeFresh(store)), asks({ from: '2026-09-22', crossCheck: false }));
    assert.strictEqual(lite.reads.orders.queries, 0); assert.strictEqual(kind(lite, 'cameBack').count, null, 'not checked is a dash, not a zero'); assert.strictEqual(kind(lite, 'reopenedLater').count, null);
    // a failed read is named, the rest stands
    const bad = makeFresh(store); bad.fail('Station_Activity');
    const part = await ISS.issues(ctxOf(bad), asks({ from: '2026-09-22' }));
    assert.strictEqual(part.ok, true); assert.strictEqual(part.partial, true); assert(part.errors.some(e => /events/.test(e)));
    assert.strictEqual(kind(part, 'reprint').count, 2, 'the counters still answer'); assert.strictEqual(part.issues.items.length, 0); assert.strictEqual(kind(part, 'cameBack').count, null);
    // paging
    const p1 = await ISS.issues(ctxOf(store), asks({ limit: 4 }));
    assert.strictEqual(p1.issues.items.length, 4); assert(p1.issues.next && p1.issues.itemsCapped);
    const seen = [...p1.issues.items]; let cur = p1.issues.next, guard = 0;
    while (cur && guard++ < 30) { const p = await ISS.issues(ctxOf(store), asks({ limit: 4, cursor: cur })); seen.push(...p.issues.items); cur = p.issues.next; }
    assert.deepStrictEqual(seen.map(x => x.id), r.issues.items.map(x => x.id), 'the pages join up to the full list, in order, no repeats'); assert.strictEqual(new Set(seen.map(x => x.id)).size, seen.length);
    assert.strictEqual((await ISS.issues(ctxOf(store), asks({ limit: 9999 }))).issues.items.length, r.issues.items.length, 'the limit is at most 200');
    say('5 sandbox apart, empty data, no PIN, read cost (34 queries cold, 0 warm), paging, a failed read is named');
  }

  /* 6 · the real person op: employeeEfficiency.js hands the context, _employeeProfile.js merges the answer ───────────── */
  {
    const fakeAdmin = { firestore: Object.assign(() => ({}), { Timestamp: Ts, FieldValue: { serverTimestamp: () => TSV, increment: INC } }) };
    const realLoad = Module._load;
    Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
    const EE = require(path.join(root, 'netlify/functions/employeeEfficiency.js')), EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
    Module._load = realLoad;
    const PASS = 'synthetic-pass-e10';
    process.env.EDIT_PASSCODE = PASS; EP.resetCache();
    const st = makeFresh(store); st.docs.set('config/employeeAliases', { 'Ana M.': ['Ana'] });
    const logs = []; const keepLog = (...x) => logs.push(x.join(' ')); const w = console.warn; console.warn = keepLog;
    NOW = Z('2026-10-05T16:00:00Z');
    const call = async body => { const x = await EE._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.9' }, body: JSON.stringify(Object.assign({ op: 'person', key: PASS, name: 'Ana', range: { from: '2026-09-22', to: '2026-10-05' }, compare: true }, body)) }, st.db); return { status: x.statusCode, body: JSON.parse(x.body) }; };
    const p = await call({});
    console.warn = w;
    assert.strictEqual(p.status, 200, JSON.stringify(p.body).slice(0, 300)); assert.strictEqual(p.body.ok, true);
    assert(p.body.issues && p.body.rates && p.body.contact, 'the profile carries the issues, rates and contact the helper returned: ' + JSON.stringify(p.body.errors || []));
    assert(!(p.body.errors || []).some(e => /^issues/.test(e)), 'the helper did not fail: ' + JSON.stringify(p.body.errors));
    assert.strictEqual(p.body.name, 'Ana M.'); assert.strictEqual(p.body.issues.byKind.find(k => k.kind === 'reprint').count, 2);
    assert.strictEqual(p.body.issues.byKind.find(k => k.kind === 'replyFailed').count, 3); assert.strictEqual(p.body.contact.failed, 3); assert.strictEqual(p.body.rates.reworkRate.numerator, 1);
    assert.strictEqual(p.body.issues.items.length, p.body.issues.itemsTotal); assert(p.body.issues.items.every(x => x.rid || x.kind));
    assert.deepStrictEqual(p.body.issues.byKind.map(k => k.prev), p.body.issues.byKind.map(() => null), 'no earlier window on file: a dash, never a zero');
    assert(!JSON.stringify(p.body).includes(PASS) && !/123456/.test(JSON.stringify(p.body)));
    const reads0 = st.reads.filter(x => x.name === 'Station_Activity').length;
    const q = await call({ name: 'Ana M.', compare: false });
    assert.strictEqual(q.body.issues.byKind.find(k => k.kind === 'reprint').count, 2);
    assert(st.reads.filter(x => x.name === 'Station_Activity').length - reads0 <= 30, 'the second request reuses what the first one read');
    // comparison with an earlier window: delta only where both windows are counted on every day
    const cmp = await ISS.issues(ctxOf(makeFresh(store)), { name: ANA, from: '2026-10-01', to: '2026-10-05', prev: { from: '2026-09-26', to: '2026-09-30', days: 5 } });
    assert.deepStrictEqual(cmp.prev, { from: '2026-09-26', to: '2026-09-30', days: 5 }); assert.strictEqual(kind(cmp, 'reprint').prev, null, 'the earlier window has a day without counters: no delta');
    const cmp2 = await ISS.issues(ctxOf(makeFresh(store)), { name: ANA, from: '2026-10-02', to: '2026-10-05', prev: { from: '2026-10-01', to: '2026-10-01', days: 1 } });
    assert.strictEqual(kind(cmp2, 'reprint').prev, 0, 'a counted earlier window with none is a real zero'); assert.strictEqual(kind(cmp2, 'reprint').delta, 2); assert.strictEqual(cmp2.rates.reworkRate.prev, 0); assert.strictEqual(cmp2.rates.reworkRate.delta, cmp2.rates.reworkRate.value);
    assert.strictEqual(kind(cmp2, 'replyFailed').prev, 3, 'the earlier day had the 3 failed replies'); assert.strictEqual(kind(cmp2, 'replyFailed').delta, -3); assert.strictEqual(kind(cmp2, 'replyFailed').better, null);
    assert.strictEqual(cmp2.issues.prevTotal, 3); assert.strictEqual(cmp2.contact.metrics.sent.prev, 6); assert.strictEqual(cmp2.contact.metrics.sent.value, null, 'no inbox day in this window: a dash'); assert.strictEqual(cmp2.contact.metrics.sent.delta, null);
    // with the prepared context the helper also takes the shared reader (ctx.prof.events) instead of querying itself
    const fakeProf = { mode: 'real', name: 'Ana M.', key: 'ana m', eventDays: 31, errors: [], capped: [], raw: { rollups: roll },
      events: async (a, z) => { const out = []; for (const [k, d] of st.docs) if (k.startsWith('Station_Activity/') && ['Ana M.', 'Ana', 'Ana_M'].includes(d.person) && d.day >= a && d.day <= z) out.push({ id: d.id, at: d.at, k: d.serverAt, day: d.day, person: d.person, station: require(path.join(root, 'netlify', 'functions', '_activityKinds')).displayStation(d.station), device: d.device, action: d.action, orderId: d.orderId, parts: d.parts, detail: d.detail, sincePrevMs: d.sincePrevMs, orders: d.orders, seq: d.seq }); return out.sort((x, y) => x.at - y.at); } };
    const viaProf = await ISS.issues(ctxOf(makeFresh(store), { prof: fakeProf }), { name: 'Ana M.', from: '2026-09-22', to: '2026-10-05' });
    const own922 = await ISS.issues(ctxOf(makeFresh(store)), { name: 'Ana M.', from: '2026-09-22', to: '2026-10-05' });
    for (const k of ISS.KINDS.map(x => x.key)) assert.strictEqual(kind(viaProf, k).count, kind(own922, k).count, 'the shared reader and its own reads agree: ' + k);
    assert.strictEqual(viaProf.reads.events.queries, 0, 'the shared reader did the event reads'); assert(own922.reads.events.queries > 0);
    assert.deepStrictEqual(viaProf.issues.items.map(x => x.id), own922.issues.items.map(x => x.id)); assert.deepStrictEqual(viaProf.rates.firstPass, own922.rates.firstPass);
    assert.deepStrictEqual([viaProf.contact.failed, viaProf.contact.medianFirstReplyMs], [own922.contact.failed, own922.contact.medianFirstReplyMs]);
    say('6 the real person op carries issues, rates and contact; deltas only on counted windows; the shared reader works');
  }

  Date.now = realNow;
  say('employee-issues: all checks passed');
})().catch(e => { Date.now = realNow; console.error(e && e.stack || e); process.exit(1); });
