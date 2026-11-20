// CLOUDWORLD (Paul, 20 Nov 2026: "Look at the other tabs don't you see that the numbers don't match?"): the rail's steps are worked out ONCE, in the cloud (_charmNestWorld.js 9c), by the
// page's own rules (charm-nest-flow-stages.js) over every order's pieces as an order's window reads them (charm-nest-order-pieces.js resolve, charm-nest-piece-placement.js resolve).
// This checks the cloud's answer step by step on made-up orders, with the same answers the page gives (flow-stages count over PiecePlacement), in well under a second.
//   node tests/charm-nest/cloud-flow.cjs
const W = require('../../netlify/functions/_charmNestWorld.js');
const CNFlow = require('../../charm-nest-flow-stages.js').CNFlow;
let failed = 0; const say = (ok, name, d) => { console.log((ok ? 'OK   ' : 'FAIL ') + name + (d ? ' :: ' + d : '')); if (!ok) failed++; };
const line = (rid, tid, extra) => Object.assign({ key: `${rid}_${tid}`, order: { receiptId: rid }, line: { transactionId: tid, quantity: 1, sku: 'A1' }, spec: { designSku: 'A1', quantity: 1 }, state: 'pooled', poolIds: [`${rid}_${tid}_1`] }, extra || {});
const pool = (id, sheetId, extra) => Object.assign({ poolId: id, orderId: id.split('_')[0], lineKey: id.replace(/_\d+$/, ''), state: sheetId ? 'written' : 'pooled', sheetId: sheetId || null }, extra || {});
const sheet = (id, ids, extra) => Object.assign({ id, metal: 'gold', sheetIndex: 1, poolIds: ids }, extra || {});
const rows = [
  line('101', '1'),                                                                  // on a sheet, cut → Laser cut
  line('102', '1'),                                                                  // on a sheet not cut → On a sheet
  line('103', '1', { poolIds: [], state: 'pulled' }),                                // nothing placed → Order in
  line('104', '1', { hold: { reason: 'a person held it' } }),                       // held → In review / On hold
  line('105', '1', { poolIds: [], state: 'noDesign' }),                              // no design → Design & engrave
  line('106', '1'), line('106', '2', { poolIds: [], state: 'pulled' }),             // one line on a sheet, one not → Order in (the slowest)
  line('107', '1'),                                                                  // cut, Sorted learned → Sorted
  line('108', '1'),                                                                  // on Review's Open list → In review
  line('109', '1'),                                                                  // engraving to settle → Design & engrave
  line('110', '1'),                                                                  // a hold the cloud keeps on the pool row (taken off its sheet) → In review / On hold
  line('111', '1', { poolIds: [], spec: { designSku: 'A1', quantity: 1, customDone: { state: 'done', how: 'button', completedAt: 1 } } }),   // completed by hand → Assembled
  line('112', '1', { spec: { designSku: 'A1', quantity: 2 }, line: { transactionId: '1', quantity: 2, sku: 'A1' }, poolIds: ['112_1_1', '112_1_2'] })   // 1 of 2 copies on a sheet → Order in
];
const answers = {
  101: { pools: [pool('101_1_1', 'S1')], sheets: [sheet('S1', ['101_1_1'], { laserDoneAt: 5 })] },
  102: { pools: [pool('102_1_1', 'S2')], sheets: [sheet('S2', ['102_1_1'])] },
  103: { pools: [], sheets: [] }, 104: { pools: [], sheets: [] }, 105: { pools: [], sheets: [] },
  106: { pools: [pool('106_1_1', 'S2')], sheets: [sheet('S2', ['106_1_1'])] },
  107: { pools: [pool('107_1_1', 'S3')], sheets: [sheet('S3', ['107_1_1'], { setId: 'T1' })] },   // (cut by its SET's laser mark, as the page's replaceCutWhy reads the Library's sets)
  108: { pools: [pool('108_1_1')], sheets: [] }, 109: { pools: [pool('109_1_1')], sheets: [] },
  110: { pools: [pool('110_1_1', null, { state: 'abandoned', heldAt: 9, heldBy: 'Ana' })], sheets: [] },
  111: { pools: [], sheets: [] },
  112: { pools: [pool('112_1_1', 'S2'), pool('112_1_2')], sheets: [sheet('S2', ['112_1_1'])] }
};
const setCut = (sheetId, setId) => setId === 'T1' && sheetId === 'S3';
const facts = {}; for (const rid of Object.keys(answers)) Object.assign(facts, W.sheetFactsOf(rid, rows.filter(r => r.order.receiptId === rid), answers[rid], null, setCut));
const x = { cancelled: new Set(), learnedMap: { 107: [5, 1] }, completedMap: { 900: { rid: '900' }, 901: { rid: '901' } }, openOrders: new Set(['108']), engraving: new Set(['109']), reviewLines: new Set() };
const f = W.flowOf(rows, facts, x);
const want = { 101: 'cut', 102: 'sheet', 103: 'order', 104: 'review', 105: 'design', 106: 'order', 107: 'sorted', 108: 'review', 109: 'design', 110: 'review', 111: 'assembled', 112: 'order' };
for (const [rid, id] of Object.entries(want)) say(f && f.byRid[rid] === id, `1 · order ${rid} stands at ${id}`, f ? `cloud says ${f.byRid[rid]}` : 'no answer');
say(f && f.counts.completed === 2, '2 · Completed is the Completed pile, every order of it', f && f.counts.completed);
const open = f ? Object.entries(f.counts).filter(([k]) => k !== 'completed').reduce((a, [, n]) => a + n, 0) : -1;
say(open === 12, '3 · every open order stands on exactly one open step (they add up to the open orders)', open);
// the same rules the page runs: CNFlow.count over the same placement, as charm-nest-bridge.js flowCtx hands it
const place = r => { const p = W.placeOfLine(r, facts[r.key], false); return p && p.state !== 'loading' ? p : null; };
const page = CNFlow.count(rows, { place, held: r => !!r.hold || (place(r) || {}).state === 'hold', nested: () => false, review: () => false, cut: (r, pl) => { const on = pl && pl.sheets ? pl.sheets : []; return on.length > 0 && on.every(s => s.cut); }, learned: rid => (x.learnedMap[rid] || [0])[0], reviewOrder: rid => x.openOrders.has(rid), engraving: rid => x.engraving.has(rid) }, [{ rid: '900' }, { rid: '901' }]);
say(f && JSON.stringify(page.counts) === JSON.stringify(f.counts), "4 · the cloud's counts are the page's own rules' counts", JSON.stringify(f && f.counts));
// a line the cloud could not read the pieces of is never put on a step it is not on (it waits as the page waits, "reading its sheets")
const g = W.flowOf([line('120', '1')], {}, Object.assign({}, x, { openOrders: new Set(), engraving: new Set() }));
say(g && g.byRid['120'] === 'order', '5 · an order whose pieces were not read stands at Order in, never further', g && g.byRid['120']);
console.log(failed ? 'FAIL cloud-flow' : 'PASS cloud-flow');
process.exit(failed ? 1 : 0);
