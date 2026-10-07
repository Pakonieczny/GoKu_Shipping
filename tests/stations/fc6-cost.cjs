// Firebase cost, worker FC6 (stations, sessions, sweep, doors): reads must follow changes, not time or page loads. Real code, fake backend, no network.
//   A · the Design pages no longer download the whole completion ledger at boot; the first Refresh of a page session reads it (as always), later Refreshes
//       ask only about the open orders (dcFor, 100 ids a request), with the same answer for every order shown
//   B · the Laser page's "previous sheet" read looks at the last 3 hours first and the whole 26 only when that finds nobody: same answer, far fewer reads
//   C · firebaseOrders ?staffNotesFor reads only the note (a field mask), the answer is unchanged
//   node tests/stations/fc6-cost.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const j = o => JSON.parse(JSON.stringify(o));
let checks = 0; const ok = m => { checks++; console.log('  ✓ ' + m); };
const d1 = fs.readFileSync(path.join(root, 'design-1.html'), 'utf8'), d0 = fs.readFileSync(path.join(root, 'design.html'), 'utf8');
const slice = (src, from, to) => { const a = src.indexOf(from); assert.ok(a >= 0, 'missing: ' + from); const b = src.indexOf(to, a + from.length); assert.ok(b > a, 'missing end: ' + to); return src.slice(a, b); };

(async () => {
  /* ═════════ A · the Design pages ═════════ */
  for (const [name, src] of [['design-1.html', d1], ['design.html', d0]]) {
    const boot = slice(src, 'async function boot()', '\nif (document.readyState');
    const bootCode = name === 'design-1.html' ? slice(src, 'async function boot()', '/* The station\'s saved state') : boot;
    assert.ok(!/restoreCompleted\(/.test(bootCode), name + ': boot must not read the whole completion ledger');
    assert.ok(!/refreshStaffNoteIDs\(\{ ?mode: ?"all"/.test(bootCode), name + ': boot must not read every staff note');
    assert.ok(!/designCompleted=1|staffNotes=1/.test(bootCode), name + ': no whole-collection door at boot');
  }
  ok('both Design pages: the boot reads neither the whole completion ledger nor every staff note');

  // design-1: the real restoreCompleted / syncCompletedFor / refreshOrders against a fake door
  {
    const calls = [];
    const ledger = new Set(['1001', '1002', '1003']);               // completed on record
    const fetchStub = async url => {
      calls.push(url);
      if (/designCompleted=1/.test(url)) return { ok: true, status: 200, json: async () => ({ success: true, orderNumbers: [...ledger] }) };
      const m = /dcFor=([^&]*)/.exec(url);
      if (m) { const ids = decodeURIComponent(m[1]).split(','); assert.ok(ids.length <= 100, 'at most 100 ids a request'); return { ok: true, status: 200, json: async () => ({ success: true, orderNumbers: ids.filter(i => ledger.has(i)) }) }; }
      throw new Error('unexpected ' + url);
    };
    class CompletedSet extends Set {
      add(id) { super.add(id); if (this.watchers) this.watchers.forEach(w => w.add(String(id))); return this; }
      delete(id) { const had = super.delete(id); if (this.watchers) this.watchers.forEach(w => w.add(String(id))); return had; }
      clear() { if (this.watchers) this.forEach(id => this.watchers.forEach(w => w.add(String(id)))); super.clear(); }
    }
    const completedOrders = new CompletedSet(); completedOrders.watchers = new Set();
    const builds = [];
    const ctx = { Set, Map, Array, Promise, JSON, Date, Error, console, encodeURIComponent, decodeURIComponent, fetch: fetchStub, FN: '/fn', completedOrders,
      loadLocalCompleted: () => new Set(), makeQueue: max => { let a = 0; const q = []; const pump = () => { if (a >= max || !q.length) return; a++; const { fn, resolve, reject } = q.shift(); fn().then(v => { a--; pump(); resolve(v); }, e => { a--; pump(); reject(e); }); }; return fn => new Promise((res, rej) => { q.push({ fn, resolve: res, reject: rej }); pump(); }); },
      allOpenReceipts: [], updateRunId: 0, window: {}, abortAllUpdateWork() {}, resetMessageFlags() {}, fetchAllLocksOnce: async () => {}, SETTINGS: { autoHeal: false },
      repairMissingReceiptData: async () => {}, healZeroMetalRows: async () => {}, ensureSelectedPreviews: async () => {},
      buildNewOrderList: async o => { builds.push(o); }, sbSweep: { inc: false }, fetchLocksOnce: async () => {} };   // (sbSweep: FC9's incremental sandbox sweep state, off in a person's Refresh)
    vm.createContext(ctx);
    vm.runInContext(slice(d1, 'let __ledgerFullAt = 0;', '/** The sorter\'s sweeps ask only') .replace(/\/\*\* Ask firebaseOrders a per-order question[\s\S]*$/, '') + '\n' +
      slice(d1, '/** Ask firebaseOrders a per-order question', 'async function persistCompleted') + '\n' +
      slice(d1, 'async function refreshOrders(', '\n}\n') + '\n}\n;this.refreshOrders=refreshOrders;this.syncCompletedFor=syncCompletedFor;this.restoreCompleted=restoreCompleted;this.ledgerAt=()=>__ledgerFullAt;', ctx);
    // first person's Refresh: the whole ledger once, the rows built from it (no dcFor)
    await ctx.refreshOrders();
    assert.equal(calls.filter(u => /designCompleted=1/.test(u)).length, 1, 'the first Refresh reads the whole ledger once');
    assert.deepEqual(j(builds[0]), { syncCompleted: false, allNotes: true, stream: false });
    assert.ok(ctx.ledgerAt() > 0);
    assert.deepEqual(j([...completedOrders].sort()), ['1001', '1002', '1003']);
    // another bench completes 1004 and un-completes 1002 meanwhile
    ledger.add('1004'); ledger.delete('1002');
    await ctx.refreshOrders(); await ctx.refreshOrders();
    assert.equal(calls.filter(u => /designCompleted=1/.test(u)).length, 1, 'later Refreshes do not read the whole ledger again');
    assert.deepEqual(j(builds[1]), { syncCompleted: true, allNotes: true, stream: false }, 'later Refreshes ask only about the open orders');
    // the sync itself: exact for the open orders, chunks of 100, two at a time
    const open = Array.from({ length: 230 }, (_, i) => String(1000 + i));
    calls.length = 0;
    assert.equal(await ctx.syncCompletedFor(open), true);
    assert.equal(calls.length, 3, '230 ids = 3 requests of at most 100');
    assert.ok(completedOrders.has('1004') && completedOrders.has('1001') && completedOrders.has('1003') && !completedOrders.has('1002'), 'an order un-completed at another bench is open again; one completed there is hidden');
    // the sorter's sweep (targeted) never reads the whole ledger either
    calls.length = 0; builds.length = 0;
    await ctx.refreshOrders({ heal: false, targeted: true });
    assert.deepEqual(j(builds[0]), { syncCompleted: true, allNotes: false, stream: false });
    assert.equal(calls.filter(u => /designCompleted=1/.test(u)).length, 0);
    ok('design-1: first Refresh reads the whole ledger once; later Refreshes and the sorter sweeps ask only about the open orders, with the same completed state for every order shown');

    // a failed first read leaves the page asking for the whole ledger next time (nothing is assumed)
    const calls2 = []; let failing = true;
    const ctx2 = Object.assign({}, ctx, { fetch: async url => { calls2.push(url); if (failing) throw new Error('offline'); return fetchStub(url); } });
    ctx2.completedOrders = new CompletedSet(); ctx2.completedOrders.watchers = new Set();
    vm.createContext(ctx2);
    vm.runInContext(slice(d1, 'let __ledgerFullAt = 0;', 'async function persistCompleted') + '\n' + slice(d1, 'async function refreshOrders(', '\n}\n') + '\n}\n;this.refreshOrders=refreshOrders;this.ledgerAt=()=>__ledgerFullAt;', ctx2);
    ctx2.buildNewOrderList = async () => {};
    await ctx2.refreshOrders();
    assert.equal(ctx2.ledgerAt(), 0, 'a failed whole read is not remembered');
    failing = false; await ctx2.refreshOrders();
    assert.ok(ctx2.ledgerAt() > 0 && calls2.filter(u => /designCompleted=1/.test(u)).length === 2);
    ok('design-1: a failed whole read is retried at the next Refresh');
  }

  // design.html: the same two paths
  {
    const calls = []; const ledger = new Set(['2001', '2002']);
    const fetchStub = async url => {
      calls.push(url);
      if (/designCompleted=1/.test(url)) return { ok: true, status: 200, json: async () => ({ success: true, orderNumbers: [...ledger] }) };
      const ids = decodeURIComponent(/dcFor=([^&]*)/.exec(url)[1]).split(',');
      return { ok: true, status: 200, json: async () => ({ success: true, orderNumbers: ids.filter(i => ledger.has(i)) }) };
    };
    const completedOrders = new Set();
    const ctx = { Set, Map, Array, Promise, JSON, Date, Error, console, encodeURIComponent, decodeURIComponent, fetch: fetchStub, FN: '/fn', completedOrders, loadLocalCompleted: () => new Set(),
      makeQueue: max => { let a = 0; const q = []; const pump = () => { if (a >= max || !q.length) return; a++; const { fn, resolve, reject } = q.shift(); fn().then(v => { a--; pump(); resolve(v); }, e => { a--; pump(); reject(e); }); }; return fn => new Promise((res, rej) => { q.push({ fn, resolve: res, reject: rej }); pump(); }); } };
    vm.createContext(ctx);
    vm.runInContext(slice(d0, 'let __ledgerFullAt = 0;', 'async function persistCompleted') + ';this.restoreCompleted=restoreCompleted;this.syncCompletedFor=syncCompletedFor;this.ledgerAt=()=>__ledgerFullAt;', ctx);
    await ctx.restoreCompleted();
    assert.ok(ctx.ledgerAt() > 0); assert.deepEqual(j([...completedOrders].sort()), ['2001', '2002']);
    ledger.delete('2001'); ledger.add('2003');
    calls.length = 0;
    await ctx.syncCompletedFor(['2001', '2002', '2003', '2004']);
    assert.equal(calls.length, 1); assert.ok(!completedOrders.has('2001') && completedOrders.has('2002') && completedOrders.has('2003') && !completedOrders.has('2004'));
    // a completion made here while the answer is on its way is kept
    const slow = { ...ctx, fetch: async url => { completedOrders.add('2004'); return fetchStub(url); } };
    vm.createContext(slow); vm.runInContext(slice(d0, 'async function syncCompletedFor', 'async function persistCompleted') + ';this.s=syncCompletedFor;', slow);
    await slow.s(['2004']); assert.ok(completedOrders.has('2004'), 'completed here meanwhile: kept');
    const refresh = slice(d0, 'const wholeLedger = !__ledgerFullAt;', 'await fetchAllLocksOnce();');
    assert.ok(/if \(wholeLedger\) await restoreCompleted\(\)/.test(refresh) && /buildNewOrderList\(\{ syncCompleted: !wholeLedger \}\)/.test(refresh));
    ok('design.html: the first Refresh reads the whole ledger, later ones sync the open orders (exact, race-safe)');
  }

  /* ═════════ B · the Laser "previous sheet" read ═════════ */
  {
    const LT = require(path.join(fnDir, '_laserSheetTime.js'));
    const HOUR = 3600e3, now = 1.8e12;
    const docs = [];
    for (let i = 0; i < 400; i++) docs.push({ id: 'd' + i, kind: 'laserSheetDone', sheetId: 'S' + i, sheet: 'S' + i, personKey: i % 3 ? 'ann lee' : 'bob ray', person: i % 3 ? 'Ann Lee' : 'Bob Ray', at: now - i * 3 * 60e3, seconds: 120, startedFrom: 'previousSheet' });
    docs.push({ id: 'u1', kind: 'laserSheetUndone', sheetId: 'S0', was: now, at: now - 1e3 });   // S0 (Bob's newest) was undone
    const stats = { reads: 0, queries: [] };
    const db = { collection: () => ({ where: (f, op, v) => ({ orderBy: () => ({ limit: n => ({ get: async () => {
      const rows = docs.filter(d => d.at >= v).sort((a, b) => b.at - a.at).slice(0, n); stats.reads += rows.length; stats.queries.push(now - v);
      return { docs: rows.map(r => ({ id: r.id, data: () => r })) }; } }) }) }) }) };
    // the old behaviour = one read of the whole 26 hours
    const whole = async key => { const r = await db.collection().where('at', '>=', now - 26 * HOUR).orderBy().limit(800).get(); const rows = r.docs.map(x => ({ _id: x.id, ...x.data() })); return rows; };
    for (const [by, expectSheet] of [['Ann Lee', 'S1'], ['Bob Ray', 'S3'], ['ann_lee', 'S1']]) {
      stats.reads = 0; stats.queries.length = 0;
      const got = await LT.lastFor(db, '', by, now);
      assert.equal(got && got.sheetId, expectSheet, by + ': the newest standing sheet (an undone one is not standing)');
      assert.equal(stats.queries.length, 1, 'found in the last 3 hours: one small read');
      assert.ok(stats.reads <= 62, 'at most the 3 hour window: ' + stats.reads + ' reads');
    }
    // nobody found near: the whole window is read, the answer is the same as before
    docs.length = 0;
    docs.push({ id: 'old', kind: 'laserSheetDone', sheetId: 'OLD', personKey: 'cy dee', person: 'Cy Dee', at: now - 20 * HOUR, seconds: 90, startedFrom: 'login' });
    stats.queries.length = 0;
    const far = await LT.lastFor(db, '', 'Cy Dee', now);
    assert.equal(far.sheetId, 'OLD'); assert.deepEqual(j(stats.queries), [3 * HOUR, 26 * HOUR], 'near window first, then the whole 26 hours');
    stats.queries.length = 0;
    assert.equal(await LT.lastFor(db, '', 'Nobody Here', now), null); assert.deepEqual(j(stats.queries), [3 * HOUR, 26 * HOUR]);
    ok('lastFor: same answer as the whole-window read (undone sheets, spelling of the name, a sheet 20 hours old, nobody), one 3 hour read in the usual case');
  }

  /* ═════════ C · ?staffNotesFor reads only the note ═════════ */
  {
    const selected = []; let reads = 0;
    const orders = { 5001: { 'Staff Note': ' glue ', 'Client Name': 'X'.repeat(500) }, 5002: { 'Client Name': 'Y' }, 5003: { 'Staff Note': '' } };
    const query = ids => ({ select: (...f) => { selected.push(f.join('|')); return { get: async () => {
      const rows = ids.filter(i => orders[i]).map(i => ({ id: i, data: () => Object.fromEntries(f.map(k => [k, orders[i][k]]).filter(([, v]) => v !== undefined)) }));
      reads += rows.length; return { docs: rows }; } }; } });
    const admin = { firestore: Object.assign(() => ({ collection: () => ({ where: (_f, _op, ids) => query(ids) }), batch() {}, getAll: async () => [] }), { FieldPath: { documentId: () => '__name__' }, FieldValue: {}, Timestamp: {} }) };
    const sb = { exports: {}, console, Date, Promise, Map, Set, Array, JSON, Number, String, Math, Object, encodeURIComponent, Intl, require: n => { if (n === './firebaseAdmin') return admin; throw new Error('dep ' + n); } };
    vm.runInNewContext(fs.readFileSync(path.join(fnDir, 'firebaseOrders.js'), 'utf8'), sb);
    const r = await sb.exports.handler({ httpMethod: 'GET', queryStringParameters: { staffNotesFor: '5001,5002,5003,5004' }, headers: {} });
    assert.equal(r.statusCode, 200);
    assert.deepEqual(j(JSON.parse(r.body).orderNumbers), ['5001']);
    assert.deepEqual(j(selected), ['Staff Note'], 'only the Staff Note field is asked for');
    ok('firebaseOrders ?staffNotesFor: field mask "Staff Note", same answer');
  }
  console.log('PASS: ' + checks + ' checks');
})().catch(e => { console.error(e); process.exit(1); });
