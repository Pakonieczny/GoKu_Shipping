// RU-S4 (Firestore cost, sorter + station server side): the cost of what runs without a person and of three polls, measured with FC1's meter on the REAL handlers
// over the meter's in-memory Firestore. Nothing leaves the machine.
//   node tests/cost/ru-S4-sorter-server.cjs
//
//  1 · the scheduled jobs of this part: stationSessionsSweepCron (*/5), charmNestKeepRepairCron (*/10), syncCharmSets (hourly), charmBatchSweepCron (*/10, no Firestore of its own)
//      -> budgets per run; none of them reads more than the open sessions / one page of saves / one state document when nothing is to be done.
//  2 · the Charm repo's Alert list (_charmNestRepoAuth.alerts, polled once a minute by the master admin's open Sorter): one read of CharmRepo_Rev/alerts while it has not moved,
//      the whole list again after any attempt or Close (never a stale card), after ALERT_MEMO_MS, and whenever the revision cannot be read.
//  3 · Manual Control's claim (op manualClaim, kept alive every 30 s): reads the three fields it decides on, not the whole sheet record; the claim rules are unchanged.
//  4 · the sandbox's placement feed and Library live read (they read every piece, sheet and card at every beat, because the sandbox kept no revision): now one document each
//      while nothing moved, a full read after a write, a reset leaves nothing behind, and production's counters are untouched by the sandbox (and the sandbox's by production).
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const root = path.join(__dirname, '../..');
const say = s => process.stdout.write(s + '\n');
const logs = []; const keepLog = console.log, keepWarn = console.warn, keepErr = console.error;
const quiet = () => { console.log = console.warn = console.error = (...a) => logs.push(a.join(' ')); }, loud = () => { console.log = keepLog; console.warn = keepWarn; console.error = keepErr; };
delete process.env.EDIT_PASSCODE; process.env.CHARM_NEST_HOLDERS_MS = '0';

const NOW0 = Date.now();
const m = meter.create(), seed = {};

/* ── fixtures ── */
// sessions: 12 open + 300 ended (real), 3 open (sandbox)
const stations = ['assembly', 'shipping', 'sorting', 'welding', 'laser', 'design'];
for (let i = 0; i < 12; i++) seed['Station_Sessions/s' + i] = { person: 'Person ' + i, employeeId: '', station: stations[i % 6], device: stations[i % 6] + '-1', computerId: 'pc-' + i, startAt: NOW0 - 3600e3, lastSeenAt: NOW0 - 60e3, lastInputAt: NOW0 - 90e3, endAt: null, endReason: null, admin: false };
for (let i = 0; i < 3; i++) seed['Sandbox_Station_Sessions/b' + i] = { person: 'Test ' + i, station: 'assembly', device: 'assembly-1', startAt: NOW0 - 3600e3, lastSeenAt: NOW0 - 60e3, lastInputAt: NOW0 - 90e3, endAt: null, admin: false };
for (let i = 0; i < 300; i++) seed['Station_Sessions/e' + i] = { person: 'Old ' + i, station: 'assembly', startAt: NOW0 - 2 * 86400e3, lastSeenAt: NOW0 - 2 * 86400e3 + 1000, endAt: NOW0 - 2 * 86400e3 + 5000, endReason: 'signOut' };
seed['config/stationAdmins'] = { names: ['Paul K'] };
// saved custom designs: 1,000 whole ones (entry, alias and both files in place)
const N_SAVES = 1000;
m.install();
const files = m._storage.files;
for (let i = 0; i < N_SAVES; i++) {
  const sku = 'CUSTOM_' + i, rid = String(3900000000 + i), lid = String(1700000000 + i);
  seed['Charm_Custom_Saves/' + rid + '-' + lid] = { rid, listingId: lid, sku, phase: 'repo', alias: 'whole', charmHash: 'h' + i, at: NOW0 - (N_SAVES - i) * 60000, etsy: { state: 'linked' }, by: 'Paul' };
  seed['Charm_Master_Index/' + sku] = { sku, charmHash: 'h' + i, hashSource: 'custom-save', aiPath: 'charmnest/master/custom/' + sku + '.ai', thumbPath: 'charmnest/master/custom/' + sku + '.png', backKeepOut: [], sizes: null, masterName: 'x'.repeat(200) };
  seed['Charm_Sku_Aliases/' + lid] = { listingId: lid, sku, v: 2, keep: { rid, sku, was: '', at: NOW0, by: 'Paul' } };
  files.set('charmnest/master/custom/' + sku + '.ai', Buffer.alloc(10)); files.set('charmnest/master/custom/' + sku + '.png', Buffer.alloc(10));
}
// 250 open Alerts (under the 300 the server reads), one big sheet record
for (let i = 0; i < 250; i++) seed['CharmRepo_Alerts/alert' + i] = { at: NOW0 - i * 1000, by: 'Paul K', action: 'upload', target: 'x', ok: i % 2 === 0, reason: i % 2 ? 'wrong' : 'ok', closed: false, closedAt: null, closedBy: '' };
seed['Charm_Nest_Sheets/sheet-big'] = { id: 'sheet-big', metal: 'gold14k', laserDoneAt: null, charms: 'c'.repeat(400000), placements: 'p'.repeat(100000), manualEdit: null, archived: false };
seed['Charm_Nest_Sheets/sheet-cut'] = { id: 'sheet-cut', metal: 'gold14k', laserDoneAt: NOW0 - 1000, charms: 'c'.repeat(1000), manualEdit: null, archived: false };
// the sandbox: 48 orders on screen over six sheets (the FC3 shop), the same in a production copy for the cross-checks
const N = 48, PER = 3, SHEETS = 6, rid = i => String(4190000000 + i);
const sheets = Array.from({ length: SHEETS }, (_, i) => ({ id: 'sheet-ru-' + i, poolIds: [], orders: [] }));
for (const P of ['Sandbox_', '']) {
  for (const s of sheets) { s.poolIds = []; s.orders = []; }
  for (let i = 0; i < N; i++) for (let k = 1; k <= PER; k++) {
    const r = rid(i), poolId = `${r}_${r}0${k}_1`, s = sheets[(i * 2 + k) % SHEETS]; s.poolIds.push(poolId); if (!s.orders.includes(r)) s.orders.push(r);
    seed[P + 'Charm_Pool/' + poolId] = { poolId, orderId: r, transactionId: r + '0' + k, lineKey: r + '_' + r + '0' + k, sku: 'DUCK-' + k, material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: s.id, sheetName: s.id, setId: 'set-ru', runId: 'run-1', updatedAt: NOW0 };
  }
  for (const s of sheets) {
    for (let j = 0; j < 40; j++) { const r = rid(1000 + j + 50 * sheets.indexOf(s)); s.poolIds.push(r + '_x_1'); if (!s.orders.includes(r)) s.orders.push(r); }
    seed[P + 'Charm_Nest_Sheets/' + s.id] = { id: s.id, metal: 'gold', sheetIndex: 1, setId: 'set-ru', setSeq: 1, folder: s.id, fileBase: s.id, day: '2026-10-07', status: 'written', orders: s.orders.slice(), poolIds: s.poolIds.slice(), page: 1, placements: 'p'.repeat(6000), updatedAt: NOW0 };
  }
  seed[P + 'Charm_Nest_Sets/set-ru'] = { setId: 'set-ru', seq: 1, day: '2026-10-07', runId: 'run-1', sheetIds: sheets.map(s => s.id), materials: ['gold'], orders: {}, status: 'open', updatedAt: NOW0, createdAt: NOW0 };
}
m.db.seed(seed);

const delta = async (label, fn) => { const a = m.snapshot(); const out = await m.op(label, fn); return { out, d: m.since(a) }; };
const reads = d => d.reads + (d.aggs || 0);
const rows = [];
const row = (name, d, budget) => { rows.push([name, reads(d), d.bytes, d.writes]); say('  ' + name.padEnd(86) + String(reads(d)).padStart(6) + ' reads' + String(d.bytes).padStart(9) + ' B' + String(d.writes).padStart(4) + ' w' + (budget != null ? '   (budget ' + budget + ')' : '')); if (budget != null) meter.assertMax(d, { reads: budget }, name); };

(async () => {
  quiet();
  const AS = require(path.join(root, 'netlify/functions/_stationAutoSignout.js'));
  const sweepCron = require(path.join(root, 'netlify/functions/stationSessionsSweepCron.js'));
  const keepCron = require(path.join(root, 'netlify/functions/charmNestKeepRepairCron.js'));
  const sync = require(path.join(root, 'netlify/functions/syncCharmSets.js'));
  const data = require(path.join(root, 'netlify/functions/charmSetsData.js'));
  const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
  const RA = require(path.join(root, 'netlify/functions/_charmNestRepoAuth.js'));
  const post = async (body, sb) => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(sb ? Object.assign({ sandbox: true }, body) : body) }); return JSON.parse(r.body || '{}'); };
  loud();

  say('\n1 · scheduled jobs, one idle run each (what a run costs when there is nothing to do)');
  let x = await delta('sweep', () => sweepCron.handler());
  assert.strictEqual(JSON.parse(x.out.body).stores.real.open, 12);
  row('stationSessionsSweepCron: 12 open sessions + 3 in the sandbox, 300 ended ones (not read)', x.d, 20);
  assert.strictEqual(x.d.writes, 0, 'an idle sweep writes nothing');
  AS.resetSweep();
  x = await delta('keep1', () => keepCron.handler());
  row('charmNestKeepRepairCron, first pass of the day (orphan + intent steps), ' + N_SAVES + ' saved designs', x.d, 1300);
  x = await delta('keep2', () => keepCron.handler());
  row('charmNestKeepRepairCron, a call inside the 5 minute throttle (a sorter tab, or the next cron)', x.d, 2);
  const realNow = Date.now; Date.now = () => realNow() + 6 * 60000;
  x = await delta('keep3', () => keepCron.handler());
  row('charmNestKeepRepairCron, a later pass (40 records against their entry, alias and files)', x.d, 130);
  assert.strictEqual(JSON.parse(x.out.body).repair.records.checked, 40);
  Date.now = realNow;
  m.db.seed({ 'Brites_Editor_Meta/charmSetsState': { idx: Object.keys(data.CHARM_SETS).length + (data.REMOVED_HANDLES || []).length, done: true, dataVersion: data.DATA_VERSION } });
  x = await delta('sync', () => sync.handler({ httpMethod: 'POST', headers: { 'x-nf-event': 'schedule' }, body: '{}' }));
  row('syncCharmSets (hourly), every set already synced', x.d, 1);
  const sweepPerDay = reads(x.d); void sweepPerDay;

  say('\n2 · the Alert list, polled once a minute by the master admin\'s open Sorter (250 open Alerts)');
  const alerts = async () => { const r = await RA.alerts(m.db, { by: 'Paul K' }); return JSON.parse(r.body); };
  x = await delta('alerts1', alerts); assert.strictEqual(x.out.total, 250); assert.strictEqual((await m.db.collection('CharmRepo_Rev').doc('alerts').get()).data().rev, 'init', 'the first ask creates the revision document'); row('first ask after this shipped: the list (kept on CharmRepo_Rev/alerts)', x.d);
  const first = x.out;
  x = await delta('alerts2', alerts); assert.deepStrictEqual(x.out, first, 'the same answer'); row('every later ask, on any instance, while nothing happened', x.d, 1);
  x = await delta('alerts2b', alerts); assert.strictEqual(reads(x.d), 1);
  // an attempt (a wrong password, recorded as an Alert) is on the next ask
  const ev = { headers: { 'x-nf-client-connection-ip': '203.0.113.9' }, body: '{}' };
  await RA.unlock(m.db, ev, { password: 'not-the-password', action: 'upload', by: 'Paul K', target: 'x', tab: 'tabaaaa' });
  x = await delta('alerts3', alerts); assert.strictEqual(x.out.total, 251, 'the new attempt is listed at once'); row('the ask after a new attempt (the list is read again and kept)', x.d);
  const held = x.out;
  x = await delta('alerts4', alerts); assert.deepStrictEqual(x.out, held); row('every later ask while nothing happened', x.d, 1);
  // Close
  const ids = held.alerts.slice(0, 3).map(a => a.id);
  const closed = JSON.parse((await RA.alertClose(m.db, { by: 'Paul K', ids })).body); assert.strictEqual(closed.closed, 3);
  x = await delta('alerts6', alerts); assert.strictEqual(x.out.total, 248, 'a Close shows at once'); assert(!x.out.alerts.some(a => ids.includes(a.id))); row('the ask after a Close (the list is read again and kept)', x.d);
  x = await delta('alerts6b', alerts); assert.strictEqual(reads(x.d), 1);
  // a change that landed while the list was being read (the revision moved after it was read): the kept answer is not believed
  { const doc = m.db.collection('CharmRepo_Rev').doc('alerts'), cur = (await doc.get()).data(); await doc.set(Object.assign({}, cur, { rev: 'a-writer-moved-it' }));
    x = await delta('alerts6c', alerts); assert(reads(x.d) > 200, 'builtRev behind rev: the list is read again'); }
  // a change nothing announced (an older instance, the console) is a delay of at most ALERT_MEMO_MS
  const T = Date.now; Date.now = () => T() + 11 * 60000; RA.deps.now = () => Date.now();
  await m.db.collection('CharmRepo_Alerts').doc('alert100').set({ closed: true }, { merge: true });
  x = await delta('alerts8', alerts); assert.strictEqual(x.out.total, 248, 'not announced: still the kept list'); assert(reads(x.d) <= 2, 'the revision, and the admin list (kept a minute)');
  Date.now = () => T() + 27 * 60000;
  x = await delta('alerts9', alerts); assert.strictEqual(x.out.total, 247, 'fifteen minutes on: read again'); row('the first ask after ALERT_MEMO_MS (the net for a change nothing announced)', x.d);
  Date.now = T; RA.deps.now = () => Date.now();
  // the revision cannot be read: the list is read as before and nothing is kept
  const flaky = new Proxy(m.db, { get(t, p) { if (p === 'collection') return n => { if (n === RA.C.ALERT_REV) throw new Error('14 UNAVAILABLE'); return t.collection(n); }; const v = t[p]; return typeof v === 'function' ? v.bind(t) : v; } });
  for (let k = 0; k < 2; k++) { const o = JSON.parse((await RA.alerts(flaky, { by: 'Paul K' })).body); assert.strictEqual(o.total, 247); }
  x = await delta('alerts10', async () => RA.alerts(flaky, { by: 'Paul K' })); assert(reads(x.d) >= 247, 'fail open: the whole list: ' + reads(x.d)); row('the revision unreadable (fail open: the list, as before)', x.d);
  // a revision document that appears between the read and the create (a writer got there first) is never replaced by the create
  { const db2 = new Proxy(m.db, { get(t, p) { if (p === 'collection') return n => { const c = t.collection(n); if (n !== RA.C.ALERT_REV) return c; return new Proxy(c, { get(c2, q) { if (q !== 'doc') { const f = c2[q]; return typeof f === 'function' ? f.bind(c2) : f; } return id => { const d = c2.doc(id); return new Proxy(d, { get(d2, r) { const f = d2[r]; if (r === 'create') return async v => { await d2.set({ rev: 'writer-won', at: 1 }); return f.call(d2, v); }; return typeof f === 'function' ? f.bind(d2) : f; } }); }; } }); }; const v = t[p]; return typeof v === 'function' ? v.bind(t) : v; } });
    await m.db.collection(RA.C.ALERT_REV).doc('alerts').delete(); const o = JSON.parse((await RA.alerts(db2, { by: 'Paul K' })).body); assert.strictEqual(o.total, 247); assert.strictEqual((await m.db.collection(RA.C.ALERT_REV).doc('alerts').get()).data().rev, 'writer-won', 'the create did not replace the writer\'s document'); }

  say('\n3 · Manual Control\'s claim (kept alive every 30 s while it is on): the record is hundreds of KB, the decision needs three fields');
  x = await delta('claim1', () => post({ op: 'manualClaim', id: 'sheet-big', tab: 'tab-aaaa', by: 'Paul' })); assert.strictEqual(x.out.ok, true); row('claim / keep-alive of a 500 KB sheet record', x.d, 2);
  assert(x.d.bytes < 2000, 'only the fields it decides on cross the wire: ' + x.d.bytes);
  x = await delta('claim2', () => post({ op: 'manualClaim', id: 'sheet-big', tab: 'tab-bbbb', by: 'Ana' })); assert.strictEqual(x.out.ok, false); assert.strictEqual(x.out.held.by, 'Paul', 'a second screen is refused while the claim is fresh');
  x = await delta('claim3', () => post({ op: 'manualClaim', id: 'sheet-cut', tab: 'tab-aaaa', by: 'Paul' })); assert.strictEqual(x.out.ok, false); assert.strictEqual(x.out.refused, 'cut', 'a cut sheet is refused');
  x = await delta('claim4', () => post({ op: 'manualClaim', id: 'no-such-sheet', tab: 'tab-aaaa', by: 'Paul' })); assert.strictEqual(x.out.status, 404); assert(/no longer in the Library/.test(x.out.error || ''), 'a missing sheet is said');
  await post({ op: 'manualClaim', id: 'sheet-big', tab: 'tab-aaaa', by: 'Paul', release: true });
  assert.strictEqual((await m.db.collection('Charm_Nest_Sheets').doc('sheet-big').get()).data().manualEdit, null, 'released');
  x = await delta('claim5', () => post({ op: 'manualClaim', id: 'sheet-big', tab: 'tab-bbbb', by: 'Ana' })); assert.strictEqual(x.out.ok, true, 'free again');

  say('\n4 · the sandbox\'s placement feed and Library live read (48 orders on screen, 6 sheets; a Library of 6 sheets and 1 set)');
  const A = Array.from({ length: 30 }, (_, i) => rid(i)), B = Array.from({ length: 18 }, (_, i) => rid(30 + i));
  const feed = async (sb, ask) => { const out = []; for (const o of [A, B]) out.push(await post(Object.assign({ op: 'getOrderPieces', orderIds: o }, ask ? ask[out.length] : {}), sb)); return out; };
  for (const sb of [false, true]) {
    const tag = sb ? 'sandbox   ' : 'production';
    x = await delta('feed1', () => feed(sb)); const [a, b] = x.out; assert(a.rev && b.rev && a.gen != null && b.gen != null, tag + ': the first answer names its gen');
    row(tag + ' placement feed, first read of the 48 orders', x.d);
    const held = [{ ifRev: a.rev, ifGen: a.gen }, { ifRev: b.rev, ifGen: b.gen }];
    x = await delta('feed2', () => feed(sb, held)); assert(x.out.every(o => o.unchanged), tag + ': unchanged'); row(tag + ' placement feed, nothing changed (a tick)', x.d, 2); assert.strictEqual(x.d.bytes, 0);
    // a hold: the pool row of an order moves; the next tick reads in full once and is cheap again
    await post({ op: 'poolUpdate', poolIds: [`${rid(3)}_${rid(3)}01_1`], patch: { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: 1790000009999 }, by: 'Paul' }, sb);
    x = await delta('feed3', () => feed(sb, held)); assert(x.out[0].orders && !x.out[0].unchanged, tag + ': a hold is read in full'); assert(x.out[0].orders[rid(3)].pools.some(p => p.state === 'abandoned'), tag + ': the held piece shows');
    row(tag + ' placement feed, after a hold (one full read of the asked orders)', x.d);
    const held2 = [{ ifRev: x.out[0].rev, ifGen: x.out[0].gen }, { ifRev: x.out[1].rev, ifGen: x.out[1].gen }];
    x = await delta('feed4', () => feed(sb, held2)); assert(x.out.every(o => o.unchanged), tag + ': cheap again'); assert(reads(x.d) <= 2);
    // the two stores never move each other's counter
  }
  // each store's gen moved only for its own writes
  const gens = async () => ({ p: (await feed(false))[0].gen, s: (await feed(true))[0].gen });
  const g0 = await gens(); await post({ op: 'poolUpdate', poolIds: [`${rid(5)}_${rid(5)}01_1`], patch: { note: 'sb' }, by: 'Paul' }, true);
  const g1 = await gens(); assert.strictEqual(g1.p, g0.p, 'a sandbox write does not move production\'s gen'); assert.notStrictEqual(g1.s, g0.s, 'it moves the sandbox\'s');
  await post({ op: 'poolUpdate', poolIds: [`${rid(5)}_${rid(5)}01_1`], patch: { note: 'prod' }, by: 'Paul' }, false);
  const g2 = await gens(); assert.notStrictEqual(g2.p, g1.p, 'a production write moves production\'s'); assert.strictEqual(g2.s, g1.s, 'and not the sandbox\'s');
  // a read-only op raises nothing
  await post({ op: 'cancelList', idsOnly: true }, true); await post({ op: 'laserStatus', sheetIds: ['sheet-ru-0'] }, true);
  assert.strictEqual((await gens()).s, g2.s, 'read-only ops do not raise the sandbox gen');
  // memo: a second tab of the same sandbox, behind the gen, is answered from the first one's read (and never from production's)
  { const behind = { op: 'getOrderPieces', orderIds: A, ifRev: 'ffffffffffff', ifGen: 123 };
    const one = await delta('feed5', async () => [await post(behind, true)]); assert(!one.out[0].unchanged && one.out[0].orders, 'a page behind the gen is answered in full'); assert(reads(one.d) > 100);
    const two = await delta('feed6', async () => [await post(behind, true)]); assert(reads(two.d) <= 1 && two.out[0].orders, 'the next asker at the same gen is served from the instance: ' + reads(two.d));
    const prod = await delta('feed7', async () => [await post(behind, false)]); assert(reads(prod.d) > 100, 'production does not take the sandbox\'s answer (its prefix is in the key)'); }
  // a reset deletes the sandbox's counters: the next ask is a full read and holds no stale `unchanged`
  { const [a] = await feed(true); const held = [{ ifRev: a.rev, ifGen: a.gen }];
    for (const doc of ['_placement', '_library']) await m.db.collection('Sandbox_Order_Rev').doc(doc).delete();
    for (const c of ['Sandbox_Charm_Pool', 'Sandbox_Charm_Nest_Sheets']) { const snap = await m.db.collection(c).get(); for (const d of snap.docs) await d.ref.delete(); }
    const r = (await feed(true, held))[0]; assert(!r.unchanged && r.gen === 0, 'after a reset: not unchanged, gen 0'); assert.deepStrictEqual(Object.values(r.orders).map(o => o.pools.length), Object.values(r.orders).map(() => 0)); }

  say('\n   the Library live read of the sandbox (laserStatus with the revisions of its last answer)');
  const SB = id => 'sheet-sb-' + id, sbIds = [], setIds = [];
  for (let k = 0; k < 6; k++) { const setId = 'set-sb-' + k, mine = []; for (let i = 0; i < 4; i++) { const id = SB(k + '-' + i); sbIds.push(id); mine.push(id); m.db.seed({ ['Sandbox_Charm_Nest_Sheets/' + id]: { id, setId, setSeq: k + 1, sheetIndex: i + 1, metal: 'gold', day: '2026-10-03', status: 'complete', placedCount: 6, charmCount: 6, density: 0.7, poolIds: [], orders: [], updatedAt: NOW0 } }); } m.db.seed({ ['Sandbox_Charm_Nest_Sets/' + setId]: { setId, seq: k + 1, day: '2026-10-03', sheetIds: mine, materials: ['gold'], orders: {}, status: 'open', updatedAt: NOW0 } }); setIds.push(setId); }
  await post({ op: 'putSheet', sheet: Object.assign({}, { id: SB('0-0'), metal: 'gold', day: '2026-10-03', poolIds: [], orders: [], setId: 'set-sb-0', setSeq: 1, sheetIndex: 1 }) }, true);   // (the revision document exists, with its list)
  const view = { sheetIds: sbIds, setIds }, status = (sb, extra) => post(Object.assign({ op: 'laserStatus', recordSeals: false, wantRevs: true }, view, extra), sb);
  let full = await status(true); assert(full.rev && /-\d+$/.test(full.rev), 'the sandbox names a revision');
  const probe = () => delta('live', () => status(true, { ifRevs: full.revs, ifRev: full.rev }));
  x = await probe(); assert.strictEqual(x.out.unchanged, true); row('sandbox Library live read, nothing moved (' + Object.keys(full.revs).length + ' documents in its answer)', x.d, 1);
  await post({ op: 'putSheet', sheet: { id: 'sheet-sb-elsewhere', metal: 'gold', day: '2026-10-03', poolIds: [], orders: [] } }, true);
  x = await probe(); assert.strictEqual(x.out.unchanged, true); assert.strictEqual(x.out.via, 'list'); row('sandbox Library live read, another sheet was saved (the revision\'s list says: none of ours)', x.d, 2); full.rev = x.out.rev;
  await post({ op: 'putSheet', sheet: { id: SB('2-1'), metal: 'gold', day: '2026-10-03', poolIds: [], orders: [], setId: 'set-sb-2', setSeq: 3, sheetIndex: 2, density: 0.9 } }, true);
  x = await probe(); assert(!x.out.unchanged && x.out.sheets.find(s => s.id === SB('2-1')).density === 0.9, 'a sheet of ours was saved: answered in full'); row('sandbox Library live read, one of our sheets was saved (a full answer)', x.d); full = x.out;
  const n0 = (await m.db.collection('Charm_Nest_Rev').doc('library').get()); await post({ op: 'putSheet', sheet: { id: SB('0-1'), metal: 'gold', day: '2026-10-03', poolIds: [], orders: [] } }, true);
  const n1 = (await m.db.collection('Charm_Nest_Rev').doc('library').get()); assert.strictEqual(n0.exists ? n0.data().n : 0, n1.exists ? n1.data().n : 0, 'a sandbox write does not move production\'s revision');
  assert.strictEqual((await m.db.collection('Sandbox_Charm_Nest_Rev').get()).size, 0, 'no Sandbox_Charm_Nest_Rev leftover');

  say('\n5 · a Complete Order, a QR print, a Review mark, a decision and a custom design sent to a sheet raise no placement counter (RU-S2 E1): the feed of every other tab stays at one read');
  { const wr = c => ((m.report().byCollection[c] || {}).writes || 0), placed = () => wr('Charm_Pool') + wr('Charm_Nest_Sheets') + wr('Sandbox_Charm_Pool') + wr('Sandbox_Charm_Nest_Sheets');
    const R = '4170000001', key = R + '_10010', g0 = (await feed(false))[0].gen, s0 = (await feed(true))[0].gen, w0 = placed();
    for (const sb of [false, true]) {
      const a = await post({ op: 'customPut', key, by: 'paul', receiptId: R, transactionId: '10010', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', category: 'Chain only', kind: 'chainOnly', label: { w: 40, h: 20, qr: 'x'.repeat(40), lines: [R] } }, sb); assert(!a.error, 'customPut: ' + JSON.stringify(a).slice(0, 120));
      const b = await post({ op: 'reviewMarkPut', rid: R, action: 'mark', by: 'paul' }, sb); assert(!b.error, 'reviewMarkPut: ' + JSON.stringify(b).slice(0, 120));
      await post({ op: 'customDecide', key, kind: 'chainOnly', by: 'paul' }, sb);
      const c = await post({ op: 'customReopen', key, by: 'paul', how: 'reopen' }, sb); assert(!c.error, 'customReopen: ' + JSON.stringify(c).slice(0, 120));
      await post({ op: 'customDelete', key, by: 'paul', how: 'reopen' }, sb);
      await post({ op: 'customSheetPut', record: { ck: 'custom:' + R + ':X', rid: R, at: Date.now() - 1000, phase: 'sent', files: [], sent: { id: 'custom-sheet:x:1', at: Date.now() - 1000, by: 'paul', lines: {} } } }, sb);
    }
    assert.strictEqual((await feed(false))[0].gen, g0, 'production\'s placement gen did not move'); assert.strictEqual((await feed(true))[0].gen, s0, 'the sandbox\'s did not either');
    assert.strictEqual(placed(), w0, 'none of them wrote a pool row or a sheet'); say('  six ops x production and sandbox: placement gen unmoved, no pool row or sheet written'); }

  { const ticks = 5760, keys = Object.keys(full.revs).length, fmt = n => n.toLocaleString('en-US');
    say('\nper day, one sandbox tab in view with the Library open and a 15 s beat (' + fmt(ticks) + ' beats), nothing changing: before ' + fmt((174 + keys) * ticks) + ' reads (the feed read every piece and sheet, ' + keys + ' documents probed for the Library), now ' + fmt((2 + 1) * ticks) + ' (production already: ' + fmt((2 + 1) * ticks) + ')'); }
  say('ru-S4-sorter-server: passed');
  m.uninstall(); process.exit(0);
})().catch(e => { loud(); console.error(e); console.error(logs.slice(-6).join('\n')); process.exit(1); });
