// One answer to "where is this piece now" in the order window (Paul, 5 Oct 2026, image 3: an order ON HOLD whose hold card said "Not on a
// sheet yet" for both pieces while the pieces' rows right under it said "Waiting . next: Engraved" / "Waiting . next: Laser cut" and drew
// Nested as done: "both of these orders are already on a sheet so this is a contradiction and needs to be corrected").
//
// Root cause (charm-nest-timeline-ui.js derive/whereOf): the step a piece has reached (D.step) is a HIGH-WATER MARK over the permanent
// history, so a piece that was placed once keeps Nested (and the "next" step after it) for ever, even after it is taken off its sheet
// (Hold, remove, sheet deleted, set undone), while the hold card's chips and the Sheet tab read where the piece IS (the sheets' own records).
// Two independent sources, so they disagreed. charm-nest-piece-placement.js (window.PiecePlacement) is now the ONE answer, read from the
// live sources; the dots, the header rail, the row status, the chips, the Sheet tab and the Timeline NOW marker all follow it. The permanent
// history (every seal on the Timeline) is untouched.
//
// Proves, in headless Chromium over the fake site (tests/charm-nest/bridge-server.cjs: the real charmNestLibrary ops over an in-memory store;
// nothing live, no Etsy, no paid call), by opening the order window for an order in every state and asking each surface what it says:
//   on a sheet · waiting for a sheet · held (Paul's image 3: order 4174601819, SS + GF, "Taken off SS Sheet 1, GF Sheet 2 by Paul") · released
//   and placed again · released and not placed yet · cancelled · completed by hand (Complete Order and QR label) · one piece split over two
//   sheets · half placed · the sheet deleted behind a history that says "placed" · records ahead of the timeline
// and asserting that the hold card's chips, every piece row (status text and its six dots), the header rail, the Sheet tab and the Timeline's
// NOW marker never disagree, that the ON SHEET seals stay on the Timeline for a piece taken off its sheet, and that a MUTANT that restores
// the old contradiction (the resolver says nothing, so the dots fall back to the high-water mark) is caught by the very same check.
//   node tests/charm-nest/placement-truth.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>, PROBE=1 prints what each surface says)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const T0 = Date.UTC(2026, 9, 4, 18, 0, 0);        // the order came in
const MIN = 60000;
const HOLD_TEXT = 'Taken off SS Sheet 1, GF Sheet 2 by Paul: Add to next sheet';
const PRODUCTION_SHEETS = { gf1: 'sheet-pt-gf1', gf2: 'sheet-pt-gf2', ss1: 'sheet-pt-ss1' };
const GF1 = PRODUCTION_SHEETS.gf1, GF2 = PRODUCTION_SHEETS.gf2, SS1 = PRODUCTION_SHEETS.ss1;
const HUG = 'HUGGIE HOOPS- RUBBER DUCK(SHAPE)', DUCK = 'DUCK 38090';
const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title>'], { type: 'text/html' })); } }; } };`;

// ── the orders: every one has the same two pieces (HUGGIE gold, DUCK silver), in a different state ──
const mk = (rid, buyer, opts = {}) => {
  const o = { rid, hug: rid + '1', duck: rid + '2' };
  const line = (tid, sku, mk2, ml, qty) => ({ transactionId: tid, listingId: '19008' + tid.slice(-5), sku, title: sku + ' earrings', quantity: qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: ml }], metalKey: mk2, metalLabel: ml, personalization: [], buyerMessage: '' });
  o.order = { receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
    lines: [line(o.hug, HUG, 'gold', '14k Gold Filled', opts.hugQty || 1), line(o.duck, DUCK, 'silver', 'Sterling Silver', 1)] };
  return o;
};
const kOf = (o, t) => `${o.rid}_${o[t]}`, pidOf = (o, t, n) => `${o.rid}_${o[t]}_${n || 1}`;
const ON = mk('4180000001', 'Olive Onsheet'), WAIT = mk('4180000002', 'Wanda Waiting'), HELD = mk('4174601819', 'Dana Duck'), REL1 = mk('4180000004', 'Rae Replaced'), REL2 = mk('4180000005', 'Ron Released'),
  CANC = mk('4180000006', 'Cass Cancelled'), HAND = mk('4180000007', 'Hank Hand'), SPLIT = mk('4180000008', 'Sol Split', { hugQty: 2 }), HALF = mk('4180000009', 'Hal Half', { hugQty: 2 }),
  GONE = mk('4180000010', 'Gwen Gone'), AHEAD = mk('4180000011', 'Ada Ahead');
const ALL = [ON, WAIT, HELD, REL1, REL2, CANC, HAND, SPLIT, HALF, GONE, AHEAD];

// what each order really is, per piece (the truth the surfaces must agree with):
//   state: sheet | waiting | hold | hand | cancelled · on: the piece is on a sheet · label: its sheet(s)
const TRUTH = {
  [ON.rid]: { hug: { state: 'sheet', on: true, label: 'GF Sheet 1' }, duck: { state: 'sheet', on: true, label: 'SS Sheet 1' } },
  [WAIT.rid]: { hug: { state: 'waiting', on: false }, duck: { state: 'waiting', on: false } },
  [HELD.rid]: { hug: { state: 'hold', on: false }, duck: { state: 'hold', on: false } },
  [REL1.rid]: { hug: { state: 'sheet', on: true, label: 'GF Sheet 1' }, duck: { state: 'sheet', on: true, label: 'SS Sheet 1' } },
  [REL2.rid]: { hug: { state: 'waiting', on: false }, duck: { state: 'waiting', on: false } },
  [CANC.rid]: { hug: { state: 'cancelled', on: false }, duck: { state: 'cancelled', on: false } },
  [HAND.rid]: { hug: { state: 'hand', on: false, how: 'button' }, duck: { state: 'hand', on: false, how: 'print' } },
  [SPLIT.rid]: { hug: { state: 'sheet', on: true, label: 'GF Sheet 1', more: 1 }, duck: { state: 'sheet', on: true, label: 'SS Sheet 1' } },
  [HALF.rid]: { hug: { state: 'sheet', on: true, label: 'GF Sheet 1', partial: true }, duck: { state: 'waiting', on: false } },
  [GONE.rid]: { hug: { state: 'waiting', on: false }, duck: { state: 'waiting', on: false } },
  [AHEAD.rid]: { hug: { state: 'sheet', on: true, label: 'GF Sheet 1' }, duck: { state: 'sheet', on: true, label: 'SS Sheet 1' } }
};

// ── the pure answer (no page): the facts of now in, one Placement out; and timeline-ui's derive clamping the steps to it ──
function unit() {
  const PP = require(path.join(root, 'charm-nest-piece-placement.js'));
  global.window = global; delete require.cache[require.resolve(path.join(root, 'charm-nest-timeline-ui.js'))]; require(path.join(root, 'charm-nest-timeline-ui.js'));
  const UI = global.OrderTimelineUI, GFS = [{ id: 'g1', label: 'GF Sheet 1', metal: 'gold', cut: false, pools: ['p1'] }], SSS = [{ id: 's1', label: 'SS Sheet 1', metal: 'silver', pools: ['p2'] }];
  const A = (cond, msg) => assert(cond, msg);
  // every state
  const wait = PP.resolve({ key: 'k' }), on = PP.resolve({ key: 'k', sheets: GFS }), two = PP.resolve({ key: 'k', sheets: GFS.concat(SSS), copies: 2 }), held = PP.resolve({ key: 'k', hold: 'Hold: wrong size' });
  const cutHeld = PP.resolve({ key: 'k', hold: true, sheets: [Object.assign({}, GFS[0], { cut: true })] }), hand = PP.resolve({ key: 'k', hand: { state: 'completed', how: 'button', completedAt: 5, completedBy: 'Paul' } });
  const printed = PP.resolve({ key: 'k', hand: { state: 'completed', how: 'print' } }), cx = PP.resolve({ key: 'k', cancelled: { at: 9, by: 'Paul' }, hold: true }), load = PP.resolve({ key: 'k', loading: true }), half = PP.resolve({ key: 'k', sheets: GFS, copies: 2, copiesOn: 1 });
  A(wait.state === 'waiting' && !wait.onSheet && wait.fence && wait.floor === 0 && wait.text === 'Waiting for a sheet' && wait.next === 'sheet', 'waiting: ' + JSON.stringify(wait));
  A(on.state === 'sheet' && on.onSheet && !on.fence && on.floor === 1 && on.text === 'GF Sheet 1' && on.next === null, 'on a sheet: ' + JSON.stringify(on));
  A(two.state === 'sheet' && two.more === 1 && two.text === 'GF Sheet 1', 'two sheets');
  A(held.state === 'hold' && !held.onSheet && held.fence && held.text === 'On hold' && /wrong size/.test(held.why) && held.reason === 'wrong size', 'held: ' + JSON.stringify(held));
  A(cutHeld.state === 'hold' && cutHeld.onSheet && !cutHeld.fence && cutHeld.floor === 1, 'held on a cut sheet is held AND on it');
  A(hand.state === 'hand' && hand.how === 'button' && !hand.fence && hand.text === 'Completed by hand' && hand.by === 'Paul' && printed.how === 'print', 'completed by hand');
  A(cx.state === 'cancelled' && !cx.fence && cx.text === 'Cancelled', 'cancelled wins over a hold');
  A(load.state === 'loading' && !load.fence && load.floor === 0 && load.text === '' && !load.onSheet, 'loading says nothing about sheets');
  A(half.state === 'sheet' && half.partial && half.onSheet, 'half placed');
  A(PP.resolve({ key: 'k', hold: true, hand: { how: 'print' } }).state === 'hold', 'a hold wins over a hand completion');
  A(PP.resolve({ key: 'k', loading: true, sheets: GFS }).state === 'sheet', 'a piece on a sheet is never loading');
  // the order's roll-up
  const roll = PP.roll([on, wait]), allOn = PP.roll([on, PP.resolve({ key: 'j', sheets: SSS })]), rh = PP.roll([held, hand]);
  A(roll.state === 'waiting' && roll.fence && roll.partial && !roll.onSheet && /1 of 2/.test(roll.text), 'roll: one waits ' + JSON.stringify([roll.state, roll.text]));
  A(allOn.state === 'sheet' && allOn.onSheet && !allOn.fence && allOn.floor === 1 && allOn.text === 'GF Sheet 1 + SS Sheet 1', 'roll: all on sheets ' + allOn.text);
  A(rh.state === 'hold' && rh.fence, 'roll: a held piece holds the order (a hand piece is not what it waits on)');
  A(PP.roll([hand, printed]).state === 'hand' && PP.roll([cx]).state === 'cancelled' && PP.roll([]).state === 'loading', 'roll: all hand, all cancelled, none');
  // the history says what happened; withHistory adds when / who / the sheet it was taken off, and never changes the state
  const evs = [{ type: 'arrived', at: 1 }, { type: 'placed', at: 100, by: 'Paul', sheet: 'SS Sheet 1', sheetId: 's1' }, { type: 'removed', at: 500, by: 'Paul', sheet: 'SS Sheet 1', sheetId: 's1', text: 'Taken off SS Sheet 1' }];
  const w = PP.withHistory(wait, evs);
  A(w.state === 'waiting' && w.wasOn && w.wasOn.label === 'SS Sheet 1' && w.wasOn.until === 500 && w.wasOn.by === 'Paul' && w.since === 500, 'wasOn: ' + JSON.stringify(w.wasOn));
  A(PP.withHistory(wait, evs.concat({ type: 'held', at: 600, by: 'Paul', text: 'Add to next sheet' })).state === 'hold', 'an unreleased hold in the history holds a piece on no sheet');
  A(PP.withHistory(wait, evs.concat({ type: 'held', at: 600 }, { type: 'released', at: 700 })).state === 'waiting', 'a released hold does not');
  A(PP.withHistory(on, evs).wasOn === null && PP.withHistory(on, evs).state === 'sheet', 'a piece on a sheet was taken off nothing');
  A(PP.withHistory(wait, [{ type: 'placed', at: 5, sheet: 'GF Sheet 2', sheetId: 'g2' }]).wasOn.how === 'gone', 'history says placed, no record holds it: its sheet is gone');
  // derive: the steps follow where the piece IS; the events are never changed
  const ev2 = evs.map((e, i) => Object.assign({ id: 'e' + i, orderId: '1', lane: 'sheet' }, e)), before = JSON.stringify(ev2);
  const old = UI.derive(ev2, null), now = UI.derive(ev2, null, null, null, wait);
  A(old.step === 1 && now.step === 0 && now.fenced && !old.fenced, 'derive clamps a piece on no sheet: ' + old.step + ' -> ' + now.step);
  A(!UI.stepDone(now, 1) && UI.stepDone(now, 0) && UI.stepDone(old, 1), 'stepDone: Nested hollow once it is off its sheet, its seal event still there');
  A(now.stages[1].first && now.stages[1].first.type === 'placed', 'the ON SHEET event is still in the history');
  A(now.cur === 1 && now.W.stage === 'waiting' && !now.hold, 'the next step is Nested, nothing says it is on a sheet');
  const nh = UI.derive(ev2, null, null, null, held); A(nh.hold && nh.W.stage === 'held' && nh.fenced && nh.step === 0, 'held: hold, step 0');
  const raised = UI.derive([{ type: 'arrived', at: 1, id: 'a', orderId: '1', lane: 'etsy' }], null, null, null, on); A(raised.step === 1 && raised.raised && raised.W.stage === 'sheet' && /GF Sheet 1/.test(raised.W.label), 'the record is ahead of the timeline: Nested done');
  const cut = [{ type: 'arrived', at: 1 }, { type: 'placed', at: 2, sheet: 'GF Sheet 1', sheetId: 'g1' }, { type: 'laserDone', at: 3, sheetId: 'g1' }].map((e, i) => Object.assign({ id: 'c' + i, orderId: '1', lane: 'sheet' }, e));
  const past = UI.derive(cut, null, null, null, wait); A(past.step === 3 && past.beyond && !past.fenced, 'a piece the history shows cut is not un-cut: ' + past.step);
  A(UI.derive(ev2, { at: 9, by: 'Paul' }, null, null, wait).step === 1, 'a cancelled order keeps its history');
  A(UI.derive(ev2, null, null, null, load).step === 1 && UI.derive(ev2, null, null, null, hand).step === 1, 'loading and hand are not clamped');
  A(JSON.stringify(ev2) === before, 'derive never touches the events');
  const rq = UI.requirementsOf('sheet', { events: ev2, place: PP.withHistory(wait, evs) });
  A(rq.state === 'now' && rq.fenced && rq.need.some(n => n.t === 'Not on a sheet now') && /^Was on SS Sheet 1 until Paul took it off, /.test(rq.was) && rq.done.length === 0, 'the Nested card: ' + JSON.stringify([rq.state, rq.need, rq.was]));
  const rl = UI.requirementsOf('laser', { events: ev2, place: wait }); A(rl.state === 'later' && rl.need.some(n => n.kind === 'after' && /Nested/.test(n.t)), 'the laser card waits for Nested: ' + JSON.stringify(rl.need));
  const sum = UI.summary(ev2, [{ key: 'a', qty: 1, line: {} }, { key: 'b', qty: 1, line: {} }], null, k => (k === 'a' ? on : wait));
  A(sum.rail.find(r => r.i === 1).n === 1 && sum.step === 0, 'summary counts a piece off its sheet as not at Nested: ' + JSON.stringify(sum.rail.map(r => r.n + '/' + r.of)));
  console.log('  ok  placement-truth: the pure answer');
}
async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  - no playwright-core: the browser checks were not run'); return; } }
  const shots = process.env.SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const probe = !!process.env.PROBE;
  unit();
  const srv = await start({ receipts: [] });
  const st = srv.st;
  const LIB = `${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`;
  const post = async body => (await fetch(LIB, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })).json();

  // ── the store ──
  const box = (id, cx) => ({ id, cxPt: cx, cyPt: 60, angle: 0, wPt: 34, hPt: 34 });
  const sheetDocs = { [GF1]: { metal: 'gold', n: 1, charms: [] }, [GF2]: { metal: 'gold', n: 2, charms: [] }, [SS1]: { metal: 'silver', n: 1, charms: [] } };
  const onSheet = (sid, o, t, copy, sku) => sheetDocs[sid].charms.push({ id: `c${sheetDocs[sid].charms.length + 1}`, name: `${o.rid} · ${sku}`, poolId: pidOf(o, t, copy), order: o.rid, sku });
  const poolRow = (o, t, copy, sku, material, sheetId, extra) => st.put('Charm_Pool', pidOf(o, t, copy), Object.assign({ poolId: pidOf(o, t, copy), orderId: o.rid, transactionId: o[t], lineKey: kOf(o, t), sku, material, copy, quantity: 1, state: sheetId ? 'written' : 'pooled', sheetId: sheetId || null, sheetName: sheetId ? `2026-10-04_${material === 'gold' ? 'GF' : 'SS'}_Set-1_Sheet-${sheetDocs[sheetId].n}` : '', updatedAt: Date.now() }, extra || {}));
  let evN = 0;
  const ev = (o, type, at, extra) => st.put('Order_Timeline', `${o.rid}~${type}~e${++evN}`, Object.assign({ orderId: o.rid, type, at, by: 'Paul', source: 'sorter', station: '', text: '', data: {} }, extra || {}));
  const placed = (o, t, sid, at, copy) => ev(o, 'placed', at, { id: `pl-${kOf(o, t)}-${copy || 1}-${at}`, sheet: `${sheetDocs[sid].metal === 'gold' ? 'GF' : 'SS'} Sheet ${sheetDocs[sid].n}`, sheetId: sid, lineKey: kOf(o, t), transactionId: o[t], text: `Placed on ${sheetDocs[sid].metal === 'gold' ? 'GF' : 'SS'} Sheet ${sheetDocs[sid].n}`, data: { poolId: pidOf(o, t, copy || 1) } });
  for (const o of ALL) ev(o, 'arrived', T0, { source: 'etsy', by: 'Etsy', id: 'ar-' + o.rid });
  // 1 · on a sheet: HUGGIE on GF Sheet 1, DUCK on SS Sheet 1
  onSheet(GF1, ON, 'hug', 1, HUG); onSheet(SS1, ON, 'duck', 1, DUCK);
  poolRow(ON, 'hug', 1, HUG, 'gold', GF1); poolRow(ON, 'duck', 1, DUCK, 'silver', SS1);
  placed(ON, 'hug', GF1, T0 + 60 * MIN); placed(ON, 'duck', SS1, T0 + 61 * MIN);
  // 2 · waiting: both in the pool, on no sheet
  poolRow(WAIT, 'hug', 1, HUG, 'gold', null); poolRow(WAIT, 'duck', 1, DUCK, 'silver', null);
  // 3 · Paul's image 3: placed on SS Sheet 1 and GF Sheet 2, then the whole order taken off both and held ("Hold")
  poolRow(HELD, 'hug', 1, HUG, 'gold', null, { state: 'abandoned' }); poolRow(HELD, 'duck', 1, DUCK, 'silver', null, { state: 'abandoned' });
  placed(HELD, 'hug', GF2, T0 + 60 * MIN); placed(HELD, 'duck', SS1, T0 + 61 * MIN);
  ev(HELD, 'removed', T0 + 120 * MIN, { id: 'rm1-' + HELD.rid, sheet: 'SS Sheet 1', text: 'Taken off SS Sheet 1' });
  ev(HELD, 'removed', T0 + 121 * MIN, { id: 'rm2-' + HELD.rid, sheet: 'GF Sheet 2', text: 'Taken off GF Sheet 2' });
  ev(HELD, 'held', T0 + 122 * MIN, { id: 'held-' + HELD.rid, sheet: 'SS Sheet 1, GF Sheet 2', text: HOLD_TEXT + ', on hold', data: { pieces: 2, note: 'Add to next sheet' } });
  // 4 · held, then released and placed again (back on GF Sheet 1 and SS Sheet 1)
  onSheet(GF1, REL1, 'hug', 1, HUG); onSheet(SS1, REL1, 'duck', 1, DUCK);
  poolRow(REL1, 'hug', 1, HUG, 'gold', GF1); poolRow(REL1, 'duck', 1, DUCK, 'silver', SS1);
  placed(REL1, 'hug', GF2, T0 + 60 * MIN); placed(REL1, 'duck', SS1, T0 + 61 * MIN);
  ev(REL1, 'held', T0 + 122 * MIN, { id: 'held-' + REL1.rid, sheet: 'SS Sheet 1, GF Sheet 2', text: HOLD_TEXT + ', on hold' });
  ev(REL1, 'released', T0 + 180 * MIN, { id: 'rel-' + REL1.rid, text: 'Released by Paul' });
  placed(REL1, 'hug', GF1, T0 + 181 * MIN, 1); placed(REL1, 'duck', SS1, T0 + 181 * MIN + 1000, 1);
  // 5 · held, then released, not placed again yet
  poolRow(REL2, 'hug', 1, HUG, 'gold', null); poolRow(REL2, 'duck', 1, DUCK, 'silver', null);
  placed(REL2, 'hug', GF2, T0 + 60 * MIN); placed(REL2, 'duck', SS1, T0 + 61 * MIN);
  ev(REL2, 'held', T0 + 122 * MIN, { id: 'held-' + REL2.rid, sheet: 'SS Sheet 1, GF Sheet 2', text: HOLD_TEXT + ', on hold' });
  ev(REL2, 'released', T0 + 180 * MIN, { id: 'rel-' + REL2.rid, text: 'Released by Paul' });
  // 6 · cancelled after it had been placed (taken off its sheets by the cancel)
  poolRow(CANC, 'hug', 1, HUG, 'gold', null, { state: 'abandoned' }); poolRow(CANC, 'duck', 1, DUCK, 'silver', null, { state: 'abandoned' });
  placed(CANC, 'hug', GF1, T0 + 60 * MIN); placed(CANC, 'duck', SS1, T0 + 61 * MIN);
  // 7 · completed by hand: HUGGIE with Complete Order, DUCK with its QR label (no sheet, never)
  const rec = (o, t, o2) => st.put('Charm_Custom_Orders', kOf(o, t), Object.assign({ key: kOf(o, t), receiptId: o.rid, transactionId: o[t], sku: 'x', title: 'x', category: 'Unknown SKU', kind: '', state: 'completed', updatedAtMs: Date.now() }, o2));
  const TH = T0 + 300 * MIN;
  rec(HAND, 'hug', { how: 'button', completedAt: TH, completedBy: 'Paul', stamps: [{ how: 'button', at: TH, by: 'Paul' }] });
  rec(HAND, 'duck', { how: 'print', completedAt: TH + MIN, completedBy: 'Paul', printedAt: TH + MIN, printedBy: 'Paul', lastPrintedAt: TH + MIN, lastPrintedBy: 'Paul', prints: 1, hasLabel: true, stamps: [{ how: 'print', at: TH + MIN, by: 'Paul' }] });
  ev(HAND, 'sealCompleted', TH, { id: `hand-b-${HAND.rid}`, lineKey: kOf(HAND, 'hug'), transactionId: HAND.hug, text: 'Completed', data: { how: 'button', completed: true, pressedIn: 'Review · Open' } });
  ev(HAND, 'sealPrinted', TH + MIN, { id: `hand-p-${HAND.rid}`, lineKey: kOf(HAND, 'duck'), transactionId: HAND.duck, text: 'QR label printed', data: { how: 'print', prints: 1, completed: true } });
  // 8 · one piece split over two sheets (HUGGIE copy 1 on GF Sheet 1, copy 2 on GF Sheet 2), DUCK on SS Sheet 1
  onSheet(GF1, SPLIT, 'hug', 1, HUG); onSheet(GF2, SPLIT, 'hug', 2, HUG); onSheet(SS1, SPLIT, 'duck', 1, DUCK);
  poolRow(SPLIT, 'hug', 1, HUG, 'gold', GF1); poolRow(SPLIT, 'hug', 2, HUG, 'gold', GF2); poolRow(SPLIT, 'duck', 1, DUCK, 'silver', SS1);
  placed(SPLIT, 'hug', GF1, T0 + 60 * MIN, 1); placed(SPLIT, 'hug', GF2, T0 + 60 * MIN + 500, 2); placed(SPLIT, 'duck', SS1, T0 + 61 * MIN);
  // 9 · half placed: HUGGIE copy 1 on GF Sheet 1, copy 2 waiting; DUCK waiting
  onSheet(GF1, HALF, 'hug', 1, HUG);
  poolRow(HALF, 'hug', 1, HUG, 'gold', GF1); poolRow(HALF, 'hug', 2, HUG, 'gold', null); poolRow(HALF, 'duck', 1, DUCK, 'silver', null);
  placed(HALF, 'hug', GF1, T0 + 60 * MIN, 1);
  // 10 · the sheet was deleted behind a history that says "placed": no removed event, no sheet record lists the pieces
  poolRow(GONE, 'hug', 1, HUG, 'gold', null); poolRow(GONE, 'duck', 1, DUCK, 'silver', null);
  placed(GONE, 'hug', GF2, T0 + 60 * MIN); placed(GONE, 'duck', SS1, T0 + 61 * MIN);
  // 11 · the records are ahead of the timeline: both pieces are on sheets, no placed event is written yet
  onSheet(GF1, AHEAD, 'hug', 1, HUG); onSheet(SS1, AHEAD, 'duck', 1, DUCK);
  poolRow(AHEAD, 'hug', 1, HUG, 'gold', GF1); poolRow(AHEAD, 'duck', 1, DUCK, 'silver', SS1);
  for (const [sid, d] of Object.entries(sheetDocs)) {
    // (other orders' charms keep a sheet from being empty, as a real sheet is)
    d.charms.push({ id: 'x1', name: 'other', poolId: `9990000001_1_1_${sid}`, order: '9990000001', sku: 'OTHER' });
    st.put('Charm_Nest_Sheets', sid, { id: sid, metal: d.metal, sheetIndex: d.n, day: '2026-10-04', status: 'written', stock: { wPt: 300, hPt: 140 }, orders: [...new Set(d.charms.map(c => c.order))], placements: d.charms.map((c, i) => box(c.id, 40 + i * 40)), charms: d.charms });
  }
  // the cancelled order's record (the real op), after its steps
  await post({ op: 'cancelPut', orderId: CANC.rid, by: 'Paul', why: 'Buyer asked to cancel', at: Date.now() - 30 * MIN, record: { buyer: { name: 'Cass Cancelled' }, sheets: 'GF Sheet 1, SS Sheet 1' } });

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const outside = [], errors = [];
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, timezoneId: 'America/Toronto' });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(PDFMAKE));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy/i.test(u)) outside.push(u);
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Paul'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Paul'; });
    const page = await context.newPage();
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.Cancelled && window.OrderWin && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // the rows, as the sorter holds them for each state
    const HOLD_ROW = HOLD_TEXT + ', on hold';
    await page.evaluate(async ({ orders, truth, holdRow }) => {
      await Orders.loadMaps(true); await Cancelled.load(true);
      for (const sku of ['HUGGIE HOOPS- RUBBER DUCK(SHAPE)', 'DUCK 38090']) B.master.entries.set(sku, { sku, updatedAt: 1 });
      for (const o of orders) o.order.lines.forEach((line, li) => {
        const key = CharmNestOrders.lineKey(o.order, line), t = truth[o.order.receiptId][li ? 'duck' : 'hug'], qty = line.quantity;
        const pools = []; for (let c = 1; c <= qty; c++) pools.push(`${key}_${c}`);
        const row = { key, order: o.order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: pools.slice(), engrave: null, material: null };
        if (t.state === 'hold') { row.state = 'held'; row.hold = row.reason = holdRow; row.heldAt = Date.now() - 3600e3; row.poolIds = []; }
        if (t.state === 'hand') { row.state = 'pulled'; row.poolIds = []; }
        if (t.state === 'cancelled') { row.state = 'gone'; row.poolIds = []; }
        B.orders.rows.push(row); B.orders.byKey.set(key, row);
      });
      Orders.interpretAll(); Review.syncOrderItems();
    }, { orders: ALL.map(o => ({ order: o.order })), truth: TRUTH, holdRow: HOLD_ROW });
    await page.waitForTimeout(600);

    const idle = () => page.evaluate(() => window.Seal && Seal.whenIdle ? Seal.whenIdle() : null);
    const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 }); };
    const openWin = async (o, n) => {
      // (a cancelled order is not in the pull: it opens from its records by its number, and has no list of pieces)
      const cancelled = o === CANC, key = cancelled ? o.rid : kOf(o, 'hug');
      if (cancelled) await page.evaluate(rid => OrderWin.openOrder(rid, {}), o.rid); else await page.evaluate(k => OrderWin.open(k), key);
      try { await page.waitForFunction(([k, n, cx]) => OrderWin.key() === k && document.querySelectorAll('#owPcSum .owPcRow').length === n && OrderWin._feed() && OrderWin._feed().answer && (!cx || /Cancelled/.test(document.getElementById('owNow').textContent)), [key, cancelled ? 0 : n, cancelled], { timeout: 25000 }); }
      catch (e) { const got = await page.evaluate(() => ({ key: OrderWin.key(), rows: document.querySelectorAll('#owPcSum .owPcRow').length, feed: !!(OrderWin._feed() && OrderWin._feed().answer), hidden: document.getElementById('owPcSum').hidden })); throw new Error(`the window of ${o.rid} did not settle: ${JSON.stringify(got)}`); }
      await page.mouse.move(3, 3); await page.waitForTimeout(1800); await idle();
    };
    const view = async v => { await page.evaluate(x => OrderWin.setView(x), v); await page.mouse.move(3, 3); await page.waitForTimeout(1800); await idle(); };
    const shot = async name => { if (shots) { await page.mouse.move(3, 3); await page.waitForTimeout(500); await page.screenshot({ path: path.join(shots, 'c1-' + name + '.png') }); } };

    // what every place of the Overview says (and, when asked, the Timeline's NOW marker)
    const snap = () => page.evaluate(() => {
      const t = n => n ? n.textContent.replace(/\s+/g, ' ').trim() : null;
      const card = document.getElementById('owNowCard');
      const chips = [...card.querySelectorAll('.shs .owShChip')].map(c => ({ pc: c.dataset.pc || '', text: t(c.querySelector('b')), sub: t(c.querySelector('span span')), off: c.classList.contains('off'), hand: !!c.dataset.hand }));
      const rows = {};
      for (const r of document.querySelectorAll('#owPcSum .owPcRow')) {
        const k = r.dataset.piece || '', chip = r.querySelector('.st .pcSt'), sr = r.querySelector('.st.owPcSr');
        rows[k] = { st: t(chip) || t(sr), k: chip ? chip.dataset.k : null, dots: [...r.querySelectorAll('.steps i')].map(i => i.classList.contains('on')) };
      }
      const rail = [...document.querySelectorAll('#owRail .tlStop')].map(s => ({ k: s.dataset.stage, cls: s.className.replace('tlStop', '').trim(), cnt: t(s.querySelector('.tlCnt')) }));
      const tab = document.querySelector('.owTabsV [data-ow-view="sheet"]');
      return { head: t(card.querySelector('.k')), line: t(card.querySelector('.t')), pill: t(document.getElementById('owNow')), chips, rows, rail, tab: { off: tab.classList.contains('off'), count: t(document.getElementById('owShCount')) } };
    });
    const timelineSnap = () => page.evaluate(() => {
      const t = n => n ? n.textContent.replace(/\s+/g, ' ').trim() : null;
      const seals = [...document.querySelectorAll('#owTimeline .tlCanvas button.tlSt:not(.ghost) svg[data-seal-model]')].map(s => { const j = JSON.parse(s.getAttribute('data-seal-model')); return j.action; });
      const ghosts = [...document.querySelectorAll('#owTimeline .tlCanvas .tlSt.ghost')].map(g => g.dataset.stage);
      return { now: t(document.querySelector('#owTimeline .tlNowLine:not(.cx) span')), nowText: t(document.querySelector('#owTimeline .tlNowT')), sub: t(document.querySelector('#owTimeline .tlNowS')), seals, ghosts };
    });

    // ── the one check: every surface says what the order's truth is ──
    const WORD = { sheet: l => new RegExp('^' + l.replace(/ /g, '\\s')), waiting: /^Waiting for a sheet/, hold: /^On hold/, hand: /^Completed/, cancelled: /^Cancelled/ };
    const problems = (o, s, tl) => {
      const out = [], truth = TRUTH[o.rid], keys = { hug: kOf(o, 'hug'), duck: kOf(o, 'duck') };
      const allOn = Object.values(truth).every(t => t.on), noneOn = Object.values(truth).every(t => !t.on);
      for (const [name, key] of Object.entries(keys)) {
        const t = truth[name], row = s.rows[key], chip = s.chips.find(c => c.pc === key), tag = `${o.rid} ${name} (${t.state})`;
        if (!row && t.state === 'cancelled') { if (chip && !chip.off) out.push(`${tag}: a cancelled piece's chip names a sheet ${JSON.stringify(chip)}`); continue; }   // (a cancelled order has no list of pieces)
        if (!row) { out.push(`${tag}: no row`); continue; }
        // the row's status
        const want = t.state === 'sheet' ? WORD.sheet(t.label) : WORD[t.state];
        if (!want.test(row.st || '')) out.push(`${tag}: the row says "${row.st}"`);
        // the six dots: Order in is done for every piece that has arrived; Nested only while the piece is on a sheet; every later dot only after it
        const dots = row.dots;
        if (dots.length < 4) out.push(`${tag}: the row draws ${dots.length} dots`);
        if (t.on && !dots[1]) out.push(`${tag}: on a sheet, but its Nested dot is hollow ${JSON.stringify(dots)}`);
        if (!t.on && t.state !== 'hand' && t.state !== 'cancelled') {
          if (dots[1]) out.push(`${tag}: not on a sheet, but its Nested dot is solid ${JSON.stringify(dots)}`);
          if (dots.slice(1).some(Boolean)) out.push(`${tag}: not on a sheet, but a later dot is solid ${JSON.stringify(dots)}`);
        }
        // the hold card's chip
        if (t.on) { if (!chip || chip.off || !(chip.text || '').includes(t.label)) out.push(`${tag}: its chip says ${JSON.stringify(chip)}`); }
        else if (t.state === 'hand') { if (!chip || !chip.off || !/Completed by hand/.test(chip.text)) out.push(`${tag}: its chip says ${JSON.stringify(chip)}`); }
        else if (t.state === 'cancelled') { /* (a cancelled order keeps its sheets' history only: it has no chips of a sheet it is not on) */ if (chip && !chip.off) out.push(`${tag}: a cancelled piece's chip names a sheet ${JSON.stringify(chip)}`); }
        else if (!chip || !chip.off || !/^Not on a sheet yet/.test(chip.text)) out.push(`${tag}: its chip says ${JSON.stringify(chip)}`);
      }
      // the header rail: Nested is done only when every piece is on a sheet; never "done" for a piece that is on none
      const nested = s.rail.find(r => r.k === 'sheet');
      if (!nested) out.push(`${o.rid}: the rail has no Nested step ${JSON.stringify(s.rail)}`);
      else {
        const done = /\bd\b/.test(nested.cls);
        if (allOn && !done) out.push(`${o.rid}: every piece is on a sheet but the rail's Nested is "${nested.cls}"`);
        if (noneOn && !['hand', 'cancelled'].includes(Object.values(truth)[0].state) && done) out.push(`${o.rid}: no piece is on a sheet but the rail's Nested is done ("${nested.cls}")`);
        if (noneOn && ['waiting', 'hold'].includes(Object.values(truth)[0].state)) {
          if (!/\bc\b/.test(nested.cls)) out.push(`${o.rid}: the next step is Nested, but the rail says ${JSON.stringify(s.rail.map(r => r.k + ':' + r.cls))}`);
          const later = s.rail.filter(r => r.k !== 'arrived' && r.k !== 'sheet' && /\bd\b/.test(r.cls)); if (later.length) out.push(`${o.rid}: steps after Nested are done on the rail ${JSON.stringify(later)}`);
        }
      }
      // the header pill and the hold card's head say where the order is now: on no sheet, they never say "On a sheet"
      const first = Object.values(truth)[0].state;
      if (noneOn && ['waiting', 'hold'].includes(first)) {
        if (/on (a )?sheet/i.test(s.pill || '') || /^on (a )?sheet/i.test(s.head || '')) out.push(`${o.rid}: no piece is on a sheet but the pill says "${s.pill}" and the card says "${s.head}"`);
        if (first === 'hold' && !(/On hold/i.test(s.pill || '') && /On hold/i.test(s.head || ''))) out.push(`${o.rid}: a held order's pill says "${s.pill}" and its card "${s.head}"`);
      }
      if (allOn && !/sheet/i.test((s.pill || '') + ' ' + (s.head || ''))) out.push(`${o.rid}: every piece is on a sheet but the pill says "${s.pill}" and the card "${s.head}"`);
      // an order with one piece on a sheet and one not travels whole: Nested is not done on the rail, and the row of the piece that waits says so
      if (!allOn && !noneOn && nested && /\bd\b/.test(nested.cls)) out.push(`${o.rid}: one piece is not on a sheet but the rail's Nested is done`);
      // the Sheet tab: open for an order that has a piece on a sheet (the piece shown is on one), greyed for one on none
      const shown = truth.hug;
      const orderState = Object.values(truth)[0].state;
      if (orderState === 'cancelled') { if (!/Cancelled/.test(s.pill || '')) out.push(`${o.rid}: a cancelled order's pill says "${s.pill}"`); if (s.rail.some(r => /\bd\b/.test(r.cls) && r.k !== 'arrived' && r.k !== 'sheet')) out.push(`${o.rid}: a cancelled order's rail shows later steps done ${JSON.stringify(s.rail)}`); }
      else if (shown.on === s.tab.off) out.push(`${o.rid}: the Sheet tab is ${s.tab.off ? 'greyed' : 'open'} but the piece shown is ${shown.on ? 'on a sheet' : 'on none'}`);
      // the Timeline's NOW marker and its next step
      if (tl) {
        const state = Object.values(truth)[0].state;
        if (noneOn && state === 'hold' && !/ON HOLD/i.test(tl.now || '')) out.push(`${o.rid}: the Timeline's NOW says "${tl.now}"`);
        if (noneOn && state === 'waiting' && /ON A SHEET|ON (GF|SS)/i.test(tl.now || '')) out.push(`${o.rid}: the Timeline's NOW says "${tl.now}" for an order on no sheet`);
        if (allOn && !/SHEET/i.test(tl.now || '')) out.push(`${o.rid}: the Timeline's NOW says "${tl.now}" for an order on sheets`);
        if (noneOn && ['waiting', 'hold'].includes(state) && /next: (Engraved|Laser cut|Sorted)/i.test(tl.sub || '')) out.push(`${o.rid}: the Timeline's strip says "${tl.sub}"`);
        // history is permanent: a seal that was stamped stays (a piece taken off its sheet keeps its ON SHEET seals)
        const placedN = [...st.docs.keys()].filter(k => k.startsWith('Order_Timeline/' + o.rid + '~placed~')).length;
        const have = tl.seals.filter(a => a === 'ON SHEET').length;
        if (placedN && have < Math.min(placedN, 1)) out.push(`${o.rid}: the ON SHEET seals are gone from the Timeline (${placedN} were stamped): ${JSON.stringify(tl.seals)}`);
      }
      return out;
    };
    const run = async (o, withTimeline = true) => {
      await openWin(o, 2);
      const s = await snap(); let tl = null;
      if (withTimeline) { await view('timeline'); tl = await timelineSnap(); await view('info'); }
      if (probe) {
        const d = r => (r.dots || []).map(x => x ? '#' : '.').join('');
        console.log(`\n== ${o.rid} head="${s.head}" pill="${s.pill}" tab=${JSON.stringify(s.tab)}\n  chips: ${s.chips.map(c => (c.off ? '(off) ' : '') + c.text + ' / ' + c.sub).join(' | ')}\n  rows: ${Object.entries(s.rows).map(([k, r]) => k.slice(-2) + ' "' + r.st + '" ' + d(r)).join(' | ')}\n  rail: ${s.rail.map(r => r.k + ':' + r.cls + (r.cnt ? '(' + r.cnt + ')' : '')).join(' ')}` + (tl ? `\n  timeline: now="${tl.now}" sub="${tl.sub}" seals=${tl.seals.join(',')} ghosts=${tl.ghosts.join(',')}` : ''));
      }
      return { s, tl, problems: problems(o, s, tl) };
    };

    const results = {};
    for (const o of ALL) {
      const r = await run(o); results[o.rid] = r;
      if (shots && [HELD, ON, WAIT].includes(o)) { await page.evaluate(() => { const n = document.getElementById('owNowCard'); if (n && !n.hidden) n.scrollIntoView({ block: 'start' }); }); await shot('overview-' + o.rid); }
      // the Nested step's hover card, for the order and then for each piece of it: "Not on a sheet now" and, from the history, "Was on SS Sheet 1 until Paul took it off"
      if (o === HELD) {
        const card = async () => { await page.hover('#owRail .tlStop[data-stage="sheet"]'); await page.waitForTimeout(1600); return page.evaluate(() => { const c = [...document.querySelectorAll('.tlExp')].find(n => getComputedStyle(n).display !== 'none' && n.textContent.trim()); return c ? c.textContent.replace(/\s+/g, ' ').trim() : null; }); };
        const all = await card(); results.hover = { all };
        if (shots) await page.screenshot({ path: path.join(shots, 'c1-hover-nested-' + o.rid + '-order.png') });   // (the pointer stays on the step: the card is in the picture)
        await page.mouse.move(3, 3); await page.waitForTimeout(600);
        await page.evaluate(k => OrderWin.selectPiece(k), kOf(o, 'hug')); await page.waitForTimeout(900);
        results.hover.hug = await card();
        if (shots) await page.screenshot({ path: path.join(shots, 'c1-hover-nested-' + o.rid + '-piece.png') });   // (the pointer stays on the step: the card is in the picture)
        await page.mouse.move(3, 3); await page.evaluate(() => OrderWin.selectPiece(null)); await page.waitForTimeout(500);
        // the Timeline tab keeps every seal (history) and its strip says where the order is
        await view('timeline'); if (shots) await shot('timeline-' + o.rid); await view('info');
      }
      await closeWin();
    }
    // ── the mutants: the old contradiction restored (the placement's fence off: the steps fall back to the history's high-water mark; or no placement at all)
    const mutantRun = async (label, mutate) => {
      await page.evaluate(mutate);
      const out = {};
      for (const o of [HELD, REL2, GONE]) { const r = await run(o, true); out[o.rid] = r.problems; if (shots && o === HELD) { await page.evaluate(() => { const n = document.getElementById('owNowCard'); if (n && !n.hidden) n.scrollIntoView({ block: 'start' }); }); await shot('mutant-' + label + '-overview-' + o.rid); } await closeWin(); }
      return out;
    };
    const mutants = {};
    mutants.noFence = await mutantRun('nofence', () => { const PP = window.PiecePlacement; window.__pp = { resolve: PP.resolve, roll: PP.roll }; const flat = x => Object.assign(x, { fence: false, floor: 0 }); PP.resolve = f => flat(window.__pp.resolve(f)); PP.roll = l => flat(window.__pp.roll(l)); });
    await page.evaluate(() => { window.PiecePlacement.resolve = window.__pp.resolve; window.PiecePlacement.roll = window.__pp.roll; });
    mutants.none = await mutantRun('none', () => { window.__ppAll = window.PiecePlacement; window.PiecePlacement = null; });
    await page.evaluate(() => { window.PiecePlacement = window.__ppAll; });
    // (and the real thing is back: Paul's order reads right again)
    const again = await run(HELD, true); await closeWin();
    if (probe) { for (const [rid, r] of Object.entries(results)) if (r.problems) console.log(rid, r.problems.length ? '\n   ' + r.problems.join('\n   ') : 'ok'); console.log('hover', JSON.stringify(results.hover, null, 1)); for (const [m, r] of Object.entries(mutants)) console.log('mutant', m, Object.entries(r).map(([k, v]) => k + ': ' + v.length + ' problems').join(', ')); console.log('after the mutants', again.problems.length ? again.problems.join(' | ') : 'ok'); await closeWin().catch(() => {}); }
    else {
      for (const [rid, r] of Object.entries(results)) if (r.problems) assert.deepEqual(r.problems, [], `order ${rid}: ${r.problems.join(' | ')}`);
      // hover: "Not on a sheet now" and the sheet it was taken off, from the history; nothing about a laser or a step after the sheet as done
      for (const [what, t] of Object.entries(results.hover)) {
        assert(/Not on a sheet now/.test(t || ''), `the Nested hover (${what}) says "${t}"`);
        assert(/Was on (SS|GF) Sheet \d until Paul took it off/.test(t || ''), `the Nested hover (${what}) does not say where it was: "${t}"`);
      }
      assert(/Was on GF Sheet 2 until Paul took it off/.test(results.hover.hug), `the huggie's own hover: "${results.hover.hug}"`);
      // the mutants are caught by the very same check (the old contradiction restored fails it, on Paul's order first)
      for (const [m, r] of Object.entries(mutants)) {
        assert(r[HELD.rid].length > 0, `mutant ${m}: the held order's contradiction was not caught`);
        assert(r[REL2.rid].length + r[GONE.rid].length > 0, `mutant ${m}: a piece off its sheet with a history that says placed was not caught`);
        assert(r[HELD.rid].some(p => /Nested|dot|rail|pill|NOW|strip/i.test(p)), `mutant ${m}: caught for the wrong reason: ${r[HELD.rid].join(' | ')}`);
      }
      assert.deepEqual(again.problems, [], 'after the mutants the real answer is back: ' + again.problems.join(' | '));
      assert.deepEqual(outside, [], 'no Etsy call'); assert.deepEqual(errors, [], 'no page error');
      console.log('  ok  placement-truth');
    }
  } finally { await browser.close(); srv.close(); }
}
main().catch(e => { console.error(e); process.exit(1); });
