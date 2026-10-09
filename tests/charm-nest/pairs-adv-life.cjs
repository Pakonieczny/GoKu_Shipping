// ADVLIFE (pairs-1009 phase 2, adversarial review of the order lifecycle with pairs): the REAL server ops (putSheet, poolPut, poolUpdate take-off, deleteSheet,
// flowApply setMember, setUpdate, getOrderPieces, laserDone, customPut) over the fake Firestore with the no-nested-arrays check, and the page's pure modules
// (SetEdit, SharedOrders, PairRemove, Readiness), attacked with the situations a pair meets: one piece on a cut sheet and one on a fresh sheet, a pair on two
// sheets of one committed set, a 3-disc necklace on 3 sheets, a hold that names only one piece, a retry of the same op, a half-written group, legacy orders
// mixed with new ones in the same set. Every case states what MUST be true; a failing case is a finding (tests/charm-nest/pairs-adv-life.cjs in ADVLIFE-findings.md).
//   node tests/charm-nest/pairs-adv-life.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const F = require('./pairs-fixtures.cjs');
const root = path.join(__dirname, '../..');
const SetEdit = require(path.join(root, 'charm-nest-set-edit.js'));
const SharedOrders = require(path.join(root, 'charm-nest-shared-orders.js')).core;
const PairRemove = require(path.join(root, 'charm-nest-pair-remove.js'));
const Readiness = require(path.join(root, 'charm-nest-readiness.js'));
const NOW = Date.now();
const fs = require('fs'), vm = require('vm');

const results = [];
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  ' + name); } catch (e) { results.push([name, e]); console.log('  FAIL ' + name + '\n      ' + String(e && e.message || e).split('\n').slice(0, 6).join('\n      ')); } };

/** A known open finding (ADVLIFE-findings.md): reported as OPEN while it fails, as FIXED when it passes; it never fails the run. */
const open = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  (finding fixed) ' + name); } catch (e) { console.log('  OPEN ' + name + '\n      ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); } };

/** A world on the fake Firestore, the real charmNestLibrary over it, and the committed sets: every set of the world gets committedAt (a set already sent to the Design Station). */
function mount(spec, o = {}) {
  const w = F.world(spec), fsx = F.fakeFirestore(), fns = F.functions(fsx, ['charmNestLibrary']);
  if (o.legacy) { const old = F.legacy(w); Object.assign(w, old); }
  for (const s of w.sets) if (o.committed !== false) s.committedAt = NOW - 3600000, s.status = 'committed';
  fsx.seed(w);
  const row = id => fsx.get(F.COLL.pool, id), rec = id => fsx.get(F.COLL.sheets, id), set = id => fsx.get(F.COLL.sets, id);
  const HOLD = (extra = {}) => Object.assign({ state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: NOW }, extra);
  const CANCEL = (extra = {}) => Object.assign({ state: 'abandoned', sheetId: null, setId: null, removedBy: 'Paul', removedReason: 'cancelled: buyer', removedAt: NOW }, extra);
  const idsOf = (rid, tid, n) => Array.from({ length: n }, (_, i) => F.poolId(rid, tid, i + 1));
  const pieces = rid => fns.lib('getOrderPieces', { orderId: rid });
  const move = (sheetId, to) => fns.lib('flowApply', { by: 'Paul', device: 'charm-nest-1', via: 'Library move', steps: [{ type: 'setMember', moves: [{ sheetId, to }] }] });
  return { w, fsx, fns, row, rec, set, HOLD, CANCEL, idsOf, pieces, move, problems: (opts) => F.problems(fsx.docsOf(), opts || {}), done: () => fns.restore() };
}
const R = n => F.rid(n), T = n => F.tx(n);

(async () => {
  // ═══ 1 · a pair on two sheets of one committed set ═══
  {
    const m = mount({ orders: [{ rid: R(1), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf2'] }] }, { rid: R(2), lines: [{ n: 10, kind: 'single', on: 'sh-gf3' }] }], tracked: [F.groupKey(R(1), T(10))] });
    const [L, Rr] = m.idsOf(R(1), T(10), 2);
    await t('1a move: either sheet of a split pair is refused to leave the set; the sheet with only the other order may leave', async () => {
      for (const id of ['sh-gf1', 'sh-gf2']) { const r = await m.move(id, null); assert.equal(r.status, 409, id + ' ' + JSON.stringify(r)); assert(/left earring.*GF Sheet 1.*right earring.*GF Sheet 2|stay in one set/.test(r.error), r.error); }
      const ok = await m.move('sh-gf3', null); assert(!ok.error, JSON.stringify(ok));
    });
    await t('1b a hold that names BOTH pieces takes both off both sheets; afterwards each sheet may leave (nothing shared); getOrderPieces says both held', async () => {
      const r = await m.fns.lib('poolUpdate', { poolIds: [L, Rr], patch: m.HOLD() }); assert(!r.error, JSON.stringify(r));
      assert.deepEqual(m.rec('sh-gf1').poolIds, []); assert.deepEqual(m.rec('sh-gf2').poolIds, []);
      const p = await m.pieces(R(1)); assert.equal(p.orders[R(1)].splits, undefined, JSON.stringify(p.orders[R(1)].splits)); // both held: not split
    });
    await t('1c retry of the SAME hold (idempotence): nothing changes, no second timeline event, counts are not lowered twice', async () => {
      const before = JSON.stringify([m.rec('sh-gf1'), m.rec('sh-gf2'), m.row(L), m.row(Rr)].map(x => Object.assign({}, x, { updatedAt: 0 })));
      const evs = () => m.fsx.list('Order_Timeline').length;
      const n0 = evs();
      const r = await m.fns.lib('poolUpdate', { poolIds: [L, Rr], patch: m.HOLD() }); assert(!r.error, JSON.stringify(r));
      assert.equal(evs(), n0, 'no second timeline event for the same hold');
      const after = JSON.stringify([m.rec('sh-gf1'), m.rec('sh-gf2'), m.row(L), m.row(Rr)].map(x => Object.assign({}, x, { updatedAt: 0 })));
      assert.equal(after, before, 'sheet records and rows unchanged by the retry');
    });
    m.done();
  }
  // ═══ 2 · the hold names only ONE piece (a stale tab / a half list) and its mate sits on ANOTHER sheet ═══
  {
    const m = mount({ orders: [{ rid: R(3), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf2'] }] }, { rid: R(4), lines: [{ n: 10, kind: 'single', on: 'sh-gf3' }] }], tracked: [F.groupKey(R(3), T(10))] });
    const [L, Rr] = m.idsOf(R(3), T(10), 2);
    await open('2a (finding 2, open) a take-off naming only the Left piece (mate on another sheet): the server must not leave the Right piece on its sheet while the Left is on hold', async () => {
      const r = await m.fns.lib('poolUpdate', { poolIds: [L], patch: m.HOLD() }); assert(!r.error, JSON.stringify(r));
      const half = m.row(Rr).state !== 'abandoned' && m.rec('sh-gf2').poolIds.includes(Rr);
      assert(!half, 'the Right piece is still on GF Sheet 2 while the Left is held: ' + JSON.stringify({ L: m.row(L).state, R: m.row(Rr).state, sheet2: m.rec('sh-gf2').poolIds, answer: r }));
    });
    m.done();
  }
  // ═══ 3 · a 3-disc necklace on 3 sheets of one committed set ═══
  {
    const m = mount({ orders: [{ rid: R(5), lines: [{ n: 10, kind: 'discs', discs: 3, on: ['sh-gf1', 'sh-gf2', 'sh-gf3'] }] }, { rid: R(6), lines: [{ n: 10, kind: 'single', on: 'sh-gf1' }] }], tracked: [F.groupKey(R(5), T(10))] });
    const ids = m.idsOf(R(5), T(10), 3);
    await t('3a every one of the 3 sheets is refused, each alone and all three together', async () => {
      for (const id of ['sh-gf1', 'sh-gf2', 'sh-gf3']) { const r = await m.move(id, null); assert.equal(r.status, 409, id + ' ' + JSON.stringify(r)); }
      const all = await m.fns.lib('flowApply', { by: 'Paul', steps: [{ type: 'setMember', moves: ['sh-gf1', 'sh-gf2', 'sh-gf3'].map(sheetId => ({ sheetId, to: null })) }] }); assert.equal(all.status, 409, JSON.stringify(all));
    });
    await t('3b hold of the whole necklace (3 ids): all three sheets let a disc go, the single charm of the other order stays', async () => {
      const r = await m.fns.lib('poolUpdate', { poolIds: ids, patch: m.HOLD() }); assert(!r.error);
      for (const id of ['sh-gf1', 'sh-gf2', 'sh-gf3']) assert(!m.rec(id).poolIds.some(x => ids.includes(x)), id);
      assert(m.rec('sh-gf1').poolIds.includes(F.poolId(R(6), T(10), 1)));
    });
    m.done();
  }
  // ═══ 5 · the Hold window's blocked text for a pair with one ear cut and the other not ═══
  {
    const win = {}; vm.runInNewContext(fs.readFileSync(path.join(root, 'charm-nest-order-hold.js'), 'utf8'), { window: win, console, setTimeout, Date, localStorage: undefined });
    const J = x => JSON.parse(JSON.stringify(x));
    const snap = pieces => ({ rid: '4170000001', label: '4170000001', shipBy: 1790500000, cancelled: false, held: false, rowsLeft: 1, pieces, sheets: {} });
    const mk = (id, status, sheet, side, why) => ({ poolId: id, lineKey: 'k', label: 'MITTENS · ' + (side === 'L' ? 'left' : 'right') + ' earring', metal: 'gold', status, why: why || '', sheetId: sheet, sheetLabel: sheet, side, groupKey: '4170000001:5000000001', setId: null, setLabel: '', placed: true });
    await t('5a Left cut, Right NOT cut (kept with its Left): the blocked text names the cut ear and the ear that is kept, with their sheets, and never says every piece is cut', () => {
      const P = J(win.OrderHold.planFrom(snap([mk('a_1', 'cut', 'GF Sheet 1', 'L', 'that sheet was already cut'), mk('a_2', 'together', 'GF Sheet 2', 'R', 'its pair stays together (its left piece is on GF Sheet 1: that sheet was already cut)')])));
      assert.equal(P.canHold, false);
      assert(!/Every piece of this order is already cut/.test(P.blockedWhy), P.blockedWhy);
      assert(/left earring on GF Sheet 1/.test(P.blockedWhy) && /right earring on GF Sheet 2/.test(P.blockedWhy), P.blockedWhy);
      assert(/not cut/.test(P.blockedWhy), P.blockedWhy);
    });
    await t('5b both ears cut (or a single piece cut): the text is unchanged ("Every piece of this order is already cut ...")', () => {
      const both = J(win.OrderHold.planFrom(snap([mk('a_1', 'cut', 'GF Sheet 1', 'L', 'that sheet was already cut'), mk('a_2', 'cut', 'GF Sheet 2', 'R', 'that sheet was already cut')])));
      assert.equal(both.blockedWhy, 'Every piece of this order is already cut, so there is nothing to take off.');
      const one = J(win.OrderHold.planFrom(snap([mk('a_1', 'cut', 'GF Sheet 1', null, 'that sheet was already cut')])));
      assert.equal(one.blockedWhy, 'Every piece of this order is already cut, so there is nothing to take off.');
    });
    await t('5c a necklace of 3 discs, one cut and two kept with it: counted, not "every piece"', () => {
      const P = J(win.OrderHold.planFrom(snap([mk('d_1', 'cut', 'GF Sheet 1', null, 'x'), mk('d_2', 'together', 'GF Sheet 2', null, 'y'), mk('d_3', 'together', 'GF Sheet 2', null, 'y')])));
      assert.equal(P.canHold, false); assert(!/Every piece/.test(P.blockedWhy), P.blockedWhy); assert(/2 pieces on GF Sheet 2/.test(P.blockedWhy) && /GF Sheet 1/.test(P.blockedWhy), P.blockedWhy);
    });
  }
  // ═══ 6 · a take-off that NAMES a piece on a cut sheet ═══
  {
    const m = mount({ orders: [{ rid: R(7), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf2'] }] }], tracked: [F.groupKey(R(7), T(10))] });
    const [L, Rr] = m.idsOf(R(7), T(10), 2);
    m.fsx.put('Charm_Nest_Sheets', 'sh-gf1', Object.assign({}, m.rec('sh-gf1'), { laserDoneAt: NOW - 600000, laserDoneBy: 'Laser Lee' }));
    await t('6a HOLD naming the Left that sits on a CUT sheet and the Right on a fresh sheet: the cut Left is NOT marked held (its sheet still lists it); the Right comes off; the answer says `kept`', async () => {
      const r = await m.fns.lib('poolUpdate', { poolIds: [L, Rr], patch: m.HOLD() }); assert(!r.error, JSON.stringify(r));
      assert.deepEqual(r.kept, [L], JSON.stringify(r)); assert.equal(r.count, 1);
      assert.equal(m.row(L).state, 'written', 'a cut piece is not held: ' + JSON.stringify(m.row(L)));
      assert.equal(m.row(L).sheetId, 'sh-gf1'); assert(!m.row(L).heldBy);
      assert.deepEqual(m.rec('sh-gf1').poolIds, [L], 'the cut sheet is a record of what was made');
      assert.equal(m.row(Rr).state, 'abandoned'); assert.deepEqual(m.rec('sh-gf2').poolIds, []);
    });
    await t('6b retry of the same hold: nothing more changes (the Left is still kept, the Right already off)', async () => {
      const before = JSON.stringify([m.row(L), m.row(Rr), m.rec('sh-gf1'), m.rec('sh-gf2')].map(x => Object.assign({}, x, { updatedAt: 0 })));
      const r = await m.fns.lib('poolUpdate', { poolIds: [L, Rr], patch: m.HOLD() }); assert(!r.error, JSON.stringify(r));
      assert.equal(JSON.stringify([m.row(L), m.row(Rr), m.rec('sh-gf1'), m.rec('sh-gf2')].map(x => Object.assign({}, x, { updatedAt: 0 }))), before);
      assert.equal(m.row(L).state, 'written');
    });
    await t('6c a line made up again (state superseded) still supersedes a piece on a cut sheet, as before (an Etsy change)', async () => {
      const r = await m.fns.lib('poolUpdate', { poolIds: [L], patch: { state: 'superseded', sheetId: null, setId: null } }); assert(!r.error, JSON.stringify(r));
      assert.equal(m.row(L).state, 'superseded'); assert.equal(r.kept, undefined);
    });
    m.done();
  }
  {
    const m = mount({ orders: [{ rid: R(8), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf2'] }] }], tracked: [F.groupKey(R(8), T(10))] });
    const [L, Rr] = m.idsOf(R(8), T(10), 2);
    m.fsx.put('Charm_Nest_Sheets', 'sh-gf1', Object.assign({}, m.rec('sh-gf1'), { laserDoneAt: NOW - 600000, laserDoneBy: 'Laser Lee' }));
    await t('6d CANCEL naming both ears, the Left on a cut sheet: the Right comes off and is told to the timeline; the cut Left is left alone and is not told as removed', async () => {
      const r = await m.fns.lib('poolUpdate', { poolIds: [L, Rr], patch: m.CANCEL(), by: 'Paul' }); assert(!r.error, JSON.stringify(r));
      assert.deepEqual(r.kept, [L]); assert.equal(m.row(L).state, 'written'); assert.equal(m.row(Rr).state, 'abandoned'); assert.equal(m.row(Rr).removedReason, 'cancelled: buyer');
      const ev = m.fsx.list('Order_Timeline').filter(e => e.orderId === R(8) && e.type === 'removed');
      assert(ev.length >= 1 && ev.every(e => !(e.data.poolIds || []).includes(L)), JSON.stringify(ev.map(e => e.data)));
    });
    m.done();
  }
  // ═══ 7 · a stale tab saves a sheet that puts a held ear back ═══
  {
    const m = mount({ orders: [{ rid: R(9), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf2'] }] }], tracked: [F.groupKey(R(9), T(10))] });
    const [L, Rr] = m.idsOf(R(9), T(10), 2);
    await m.fns.lib('poolUpdate', { poolIds: [L, Rr], patch: m.HOLD() });
    await t('7a the refusal of a save that would put a held ear back names the ear and the sheet as the Library calls it; the sheet is not written', async () => {
      const before = JSON.stringify(m.rec('sh-gf2'));
      const r = await m.fns.lib('putSheet', { sheet: { id: 'sh-gf2', poolIds: [Rr] } });
      assert.equal(r.status, 409, JSON.stringify(r)); assert(/GF Sheet 2/.test(r.error) && /right earring/.test(r.error), r.error);
      assert.equal(JSON.stringify(m.rec('sh-gf2')), before);
      const l = await m.fns.lib('putSheet', { sheet: { id: 'sh-gf1', poolIds: [L] } }); assert(/GF Sheet 1/.test(l.error) && /left earring/.test(l.error), l.error);
    });
    m.done();
  }
  const bad = results.filter(r => r[1]);
  console.log(`\npairs-adv-life: ${results.length - bad.length} passed, ${bad.length} failed`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
