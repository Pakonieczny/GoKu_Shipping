/* The oracle against Paul's rules by hand: each case is a tiny shop and the exact answer a person reading Paul's point 9
 * would give. If one of these fails the ORACLE is wrong (not the code under test). Offline, no network, no browser. */
'use strict';
const assert = require('node:assert/strict');
const { truth, issueOrders } = require('./issues-oracle.cjs');
const { materialize } = require('./issues-shop.cjs');

const sheet = (id, metal, own = 'ok', setId = 'set-1', index = 1) => ({ id, metal, index, own, setId });
const copy = (sheet, pooled = true) => ({ sheet, pooled });
const NONE = { sheet: null, pooled: false }, POOLED = { sheet: null, pooled: true };
let tx = 100;
const line = (o = {}) => ({ tx: ++tx, metal: 'gold', q: (o.copies || [1]).length, kind: 'hand', copies: [copy('A')], state: 'written', problems: [], sku: 'SKU', hold: null, change: false, engrave: 'plain', noDesign: false, ...o });
const order = (id, ...lines) => ({ id: String(id), buyer: 'Buyer ' + id, lines });
const shop = (sheets, orders) => materialize({ seed: 1, sandbox: false, sheets, orders, archivedSheets: [] });
const issues = (s, sid) => issueOrders(truth(s), sid);
const A = sheet('A', 'gold'), B = sheet('B', 'silver', 'ok', 'set-1', 1);
let n = 0;
const t = (name, s, expect) => {
  const tr = truth(s);
  for (const [sid, want] of Object.entries(expect)) assert.deepEqual(issueOrders(tr, sid), want.map(String).sort(), `${name}: sheet ${sid}`);
  n++;
};

// 1 two ready sheets, an order spread over both: nothing holds either back
t('ready+ready', shop([A, B], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [copy('B')] }))]), { A: [], B: [] });
// 2 the other sheet is not ready (layout unverified): an issue for A only; B itself is the one that is not ready
t('other not ready', shop([A, { ...B, own: 'unverified' }], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [copy('B')] }))]), { A: [1], B: [] });
// 3 both not ready: each sees the other
t('both not ready', shop([{ ...A, own: 'noQr' }, { ...B, own: 'unverified' }], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [copy('B')] }))]), { A: [1], B: [1] });
// 4 single-piece orders on a sheet that is itself not ready (Paul's 48 of 48): never an order issue
t('48 of 48', shop([{ ...A, own: 'unverified' }], [order(1, line()), order(2, line()), order(3, line({ engrave: 'unapproved' }))]), { A: [] });
// 5 every piece on this sheet, the sheet itself not ready: never an order issue
t('all on this sheet', shop([{ ...A, own: 'noQr' }], [order(1, line({ copies: [copy('A'), copy('A')] }), line())]), { A: [] });
// 6 another piece not on any sheet yet (pooled, pool id exists), or not even pooled (pulled)
t('pooled', shop([A], [order(1, line(), line({ copies: [POOLED], state: 'pooled' }))]), { A: [1] });
t('pulled', shop([A], [order(1, line(), line({ copies: [NONE], state: 'pulled' }))]), { A: [1] });
// 7 SKU problems on the other piece, with and without a SKU
t('no sku', shop([A], [order(1, line(), line({ copies: [NONE], problems: ['unmatchedSku'], sku: '', state: 'unmatched' }))]), { A: [1] });
t('unmatched', shop([A], [order(1, line(), line({ copies: [NONE], problems: ['unmatchedSku'], state: 'unmatched' }))]), { A: [1] });
t('no design for size', shop([A], [order(1, line(), line({ copies: [NONE], problems: ['missingSize'], state: 'waiting' }))]), { A: [1] });
// 8 a no-design line (chain, packaging) and a cancelled line are not pieces: they never block and do not make an order multi-piece
t('noDesign', shop([A], [order(1, line(), line({ copies: [NONE], noDesign: true, state: 'noDesign' }))]), { A: [] });
t('gone unnested', shop([A], [order(1, line(), line({ copies: [NONE], state: 'gone' }))]), { A: [] });
t('gone with a problem', shop([A], [order(1, line(), line({ copies: [NONE], state: 'gone', problems: ['unmatchedSku'] }))]), { A: [] });
t('gone on a not-ready sheet', shop([A, { ...B, own: 'unverified' }], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], state: 'gone' }))]), { A: [], B: [] });
t('noDesign + a real second piece unnested', shop([A], [order(1, line(), line({ copies: [NONE], noDesign: true, state: 'noDesign' }), line({ copies: [POOLED], state: 'pooled' }))]), { A: [1] });
// 9 quantity 2: copies on two sheets, one not ready; copies not both pooled
t('qty 2 over two sheets', shop([A, { ...B, own: 'unverified' }], [order(1, line({ copies: [copy('A'), copy('B')] }))]), { A: [1], B: [] });
t('qty 2 one unnested', shop([A], [order(1, line({ copies: [copy('A'), POOLED] }))]), { A: [1] });
t('qty 2 both here', shop([A], [order(1, line({ copies: [copy('A'), copy('A')] }))]), { A: [] });
t('qty 2 fewer pool ids than copies', shop([A], [order(1, line({ copies: [copy('A'), NONE] }))]), { A: [1] });
// 10 stale: state stays "pooled" though every piece is on a ready sheet: no issue
t('stale pooled state', shop([A, B], [order(1, line({ state: 'pooled' }), line({ metal: 'silver', copies: [copy('B')], state: 'pooled' }))]), { A: [], B: [] });
// 11 stale: a leftover problems[] on a piece that is already on a sheet is nested, not a problem
t('stale problem, nested', shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], problems: ['unmatchedSku'], state: 'written' }))]), { A: [], B: [] });
// 12 stale: the piece moved to B a second ago; A's old archived record still lists it
{
  const s = shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], staleArchive: true }))]);
  assert(s.sheets.some(x => x.archived && x.poolIds.length), 'the fixture has an archived copy');
  t('archived copy ignored', s, { A: [], B: [] });
  // the archived copy cannot make a not-ready sheet out of an unnested piece either
  const u = shop([A], [order(1, line(), line({ copies: [POOLED], staleArchive: false }))]);
  t('pooled not archived', u, { A: [1] });
}
// 13 held (a person's hold, an Etsy change waiting for review) is an explicit stop: it holds the sheet it sits on AND the sheets of the order's other pieces,
//    even a one-piece order; never for a cancelled or no-design line
t('held elsewhere', shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], hold: 'changed', state: 'written' }))]), { A: [1], B: [1] });
t('held own piece only (one-piece order)', shop([A], [order(1, line({ hold: 'changed' }))]), { A: [1] });
t('changePending elsewhere', shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], change: true }))]), { A: [1], B: [1] });
t('held but cancelled', shop([A], [order(1, line({ hold: 'changed', state: 'gone' }), line())]), { A: [] });
t('held but no design', shop([A], [order(1, line({ hold: 'changed', noDesign: true, state: 'noDesign' }), line())]), { A: [] });
t('held, unnested', shop([A], [order(1, line(), line({ copies: [NONE], hold: 'changed', state: 'held' }))]), { A: [1] });
t('held on a sheet that holds none of the order', shop([A, B], [order(1, line({ metal: 'silver', copies: [copy('B')], hold: 'changed' }), line({ metal: 'silver', copies: [copy('B')] }))]), { A: [], B: [1] });
// 13b older records: no problems entry, only the state the page gave the line, while the piece is on no sheet
t('old state unmatched', shop([A], [order(1, line(), line({ copies: [NONE], state: 'unmatched', problems: [], sku: '' }))]), { A: [1] });
t('old state oversize', shop([A], [order(1, line(), line({ copies: [NONE], state: 'oversize' }))]), { A: [1] });
// 13c an order the sheet lists but no line record exists for: not an order issue (the sheet gets one 'not checked yet' entry, see ghosts)
{
  const g = shop([{ ...A, staleOrder: undefined }], [order(1, line())]);
  g.sheets[0].orders.push('4179000001');
  const tr = truth(g);
  assert.deepEqual(issueOrders(tr, 'A'), []); assert.deepEqual(tr.sheets.A.ghosts, ['4179000001']); assert.equal(tr.sheets.A.laserReady, false, 'the order gate stays closed for an unverifiable order');
  n++;
}
// 14 the other sheet's own state decides "ready"
for (const [own, issue] of [['ok', false], ['unverified', true], ['noQr', true], ['qrPartial', true], ['held', true], ['draft', true], ['solidExcluded', true], ['dirty', true], ['saving', true], ['done', false], ['reopened', false], ['noFront', true], ['error', true]]) {
  const s = shop([A, { ...B, own, setId: own === 'draft' ? null : 'set-1' }], [order(1, line(), line({ metal: 'silver', copies: [copy('B')] }), line({ metal: 'silver', copies: [copy('B')], tx: 999 + (++tx) }))]);
  t('other sheet ' + own, s, { A: issue ? [1] : [] });
}
// 15 engraving: an unapproved engraving on the other sheet's piece makes that sheet not ready; approved + saved back does not
t('other has unapproved engraving', shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], engrave: 'unapproved' }))]), { A: [1], B: [] });
t('other has approved saved back', shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], engrave: 'approved' }))]), { A: [], B: [] });
// (A's own unapproved engraving never makes the order an issue for A, but it does make A "not ready" for B's side of the order)
t('own unapproved engraving never counts', shop([A, B], [order(1, line({ engrave: 'unapproved' }), line({ metal: 'silver', copies: [copy('B')] }))]), { A: [], B: [1] });
// 16 a piece on two live sheets (a re-nest caught half way): it is on this sheet, and elsewhere only blocks when none of its sheets is ready
{
  const C = sheet('C', 'silver', 'unverified', 'set-1', 2);
  // p1 is listed on C (not ready) and on B (ready): for A it is on a ready sheet, so it holds A back no more than B does
  let s = shop([A, B, C], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [{ sheet: 'C', pooled: true, dual: 'B' }] }))]);
  t('piece on two sheets, one ready', s, { A: [], B: [], C: [] });
  // listed on two sheets that are both not ready: it blocks
  s = shop([A, { ...B, own: 'noQr' }, C], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [{ sheet: 'C', pooled: true, dual: 'B' }] }))]);
  t('piece on two sheets, none ready', s, { A: [1], B: [], C: [] });
}
// 17 a sheet already cut (done or reopened) needs nothing more: no order issue is shown for it (it is laser-ready)
t('done sheet silent', shop([{ ...A, own: 'done' }], [order(1, line(), line({ copies: [POOLED], state: 'pooled' }))]), { A: [] });
t('reopened sheet silent', shop([{ ...A, own: 'reopened' }], [order(1, line(), line({ copies: [POOLED], state: 'pooled' }))]), { A: [] });
// 18 Paul's order 4170252963: piece 1 on GF, piece 2 on SS, SS not ready -> an issue for GF naming SS, none for SS
{
  const GF = sheet('GF', 'gold'), SS = { ...sheet('SS', 'silver'), own: 'backUnsaved' };
  const s = shop([GF, SS], [order(4170252963, line({ copies: [copy('GF')], state: 'written' }), line({ metal: 'silver', copies: [copy('SS')], state: 'pooled', engrave: 'approved' })), order(4170252964, line({ copies: [copy('GF')], engrave: 'approved' }))]);
  const tr = truth(s), e = tr.sheets.GF.orders['4170252963'];
  assert.equal(tr.sheets.GF.label, 'GF Sheet 1'); assert.equal(tr.sheets.SS.label, 'SS Sheet 1');
  assert.deepEqual(issueOrders(tr, 'GF'), ['4170252963']); assert.deepEqual(issueOrders(tr, 'SS'), []);
  assert.equal(e.offenders.length, 1); assert.deepEqual([...e.offenders[0].reasons], ['otherSheetNotReady']); assert.equal(e.offenders[0].sheets[0].label, 'SS Sheet 1');
  n++;
}
// 19 the other sheet is ready again once its trouble is fixed (same pieces): the issue disappears
t('fixed', shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')] }))]), { A: [], B: [] });
// 20 a piece COMPLETED BY HAND (Paul, 5 Oct round 6, order 4170837249: a cable chain only, a custom order completed in Review, plus a middle on each of two
//    sheets) needs no sheet: it blocks nothing, whatever the run's copy of its line still says. The custom order's own record decides (shop.customs)
{
  const GF = sheet('GF', 'gold', 'ok', 'set-1'), RG = sheet('RG', 'rose', 'ok', 'set-2');
  const chain = (hand, o = {}) => line({ metal: 'rose', copies: [NONE], problems: ['unmatchedSku'], sku: '', state: 'unmatched', hand, ...o });
  const paul = hand => shop([GF, RG], [order(4170837249, chain(hand), line({ copies: [copy('GF')] }), line({ metal: 'rose', copies: [copy('RG')] }))]);
  t('Paul: hand piece, the run still says unmatched', paul({ how: 'button', state: 'completed', rec: 'stale' }), { GF: [], RG: [] });
  t('the same piece, not completed: it blocks both sheets', paul(null), { GF: [4170837249], RG: [4170837249] });
  t('completed by its printed QR label', paul({ how: 'print', state: 'completed', rec: 'stale' }), { GF: [], RG: [] });
  t('run copy hinted (handDone, noDesign state)', paul({ how: 'button', state: 'completed', rec: 'hinted' }), { GF: [], RG: [] });
  t('run copy flagged noDesign (an older save)', paul({ how: 'button', state: 'completed', rec: 'legacy' }), { GF: [], RG: [] });
  // REOPEN: a piece again, blocking again (also when the record still carries the hint the page wrote while it was completed)
  t('reopened, run copy stale', paul({ how: 'button', state: 'open', rec: 'stale' }), { GF: [4170837249], RG: [4170837249] });
  t('reopened, run copy still hinted', shop([GF, RG], [order(4170837249, line({ metal: 'rose', copies: [NONE], state: 'noDesign', hand: { how: 'button', state: 'open', rec: 'hinted' } }), line({ copies: [copy('GF')] }), line({ metal: 'rose', copies: [copy('RG')] }))]), { GF: [4170837249], RG: [4170837249] });
  // sent to the sheets with its own designs (how 'sheet') is cut, not completed by hand: its place is still awaited
  t('sent to the sheets is no hand completion', paul({ how: 'sheet', state: 'completed', rec: 'stale' }), { GF: [4170837249], RG: [4170837249] });
  // HELD: a person's or Etsy's stop still holds the sheets of its order, completed or not
  t('held and completed by hand: still holds', shop([GF, RG], [order(4170837249, chain({ how: 'button', state: 'completed', rec: 'stale' }, { hold: 'Customer changed the order', state: 'held' }), line({ copies: [copy('GF')] }), line({ metal: 'rose', copies: [copy('RG')] }))]), { GF: [4170837249], RG: [4170837249] });
  // quantity 2: every copy is resolved; a cancelled order is unchanged (nothing blocks either way)
  t('two copies completed by hand', shop([GF, RG], [order(4170837249, chain({ how: 'button', state: 'completed', rec: 'stale' }, { copies: [NONE, NONE], q: 2 }), line({ copies: [copy('GF')] }), line({ metal: 'rose', copies: [copy('RG')] }))]), { GF: [], RG: [] });
  t('cancelled hand piece', shop([GF, RG], [order(4170837249, chain({ how: 'button', state: 'completed', rec: 'stale' }, { state: 'gone' }), line({ copies: [copy('GF')] }), line({ metal: 'rose', copies: [copy('RG')] }))]), { GF: [], RG: [] });
  // a line that was pooled (it has pool ids) is waiting for the nester: it stays a piece
  t('pooled line is no hand piece', shop([GF, RG], [order(4170837249, chain({ how: 'button', state: 'completed', rec: 'stale' }, { copies: [POOLED], state: 'pooled' }), line({ copies: [copy('GF')] }), line({ metal: 'rose', copies: [copy('RG')] }))]), { GF: [4170837249], RG: [4170837249] });
  // the only other piece is hand-completed too / the order is that piece alone: nothing is left to wait for
  t('only hand pieces besides one sheet piece', shop([GF], [order(4170837249, chain({ how: 'button', state: 'completed', rec: 'stale' }), chain({ how: 'print', state: 'completed', rec: 'stale' }), line({ copies: [copy('GF')] }))]), { GF: [] });
  // it does not make a one-piece order multi-piece, and a real second piece that is not placed still blocks
  t('hand piece does not hide a real unplaced piece', shop([GF], [order(1, chain({ how: 'button', state: 'completed', rec: 'stale' }), line({ copies: [copy('GF')] }), line({ copies: [POOLED], state: 'pooled' }))]), { GF: [1] });
}
console.log(`PASS: ${n} hand-made cases agree with Paul's rules (oracle self-check)`);
