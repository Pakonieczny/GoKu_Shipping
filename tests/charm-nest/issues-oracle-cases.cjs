/* The oracle against Paul's rules by hand: each case is a tiny shop and the exact answer a person reading Paul's point 9
 * would give. If one of these fails the ORACLE is wrong (not the code under test). Offline, no network, no browser. */
'use strict';
const assert = require('node:assert/strict');
const { truth, issueOrders, setWaits, hardBlock } = require('./issues-oracle.cjs');
const { materialize } = require('./issues-shop.cjs');

const sheet = (id, metal, own = 'ok', setId = 'set-1', index = 1) => ({ id, metal, index, own, setId });
const copy = (sheet, pooled = true) => ({ sheet, pooled });
const NONE = { sheet: null, pooled: false }, POOLED = { sheet: null, pooled: true };
let tx = 100;
const line = (o = {}) => ({ tx: ++tx, metal: 'gold', q: (o.copies || [1]).length, kind: 'hand', copies: [copy('A')], state: 'written', problems: [], sku: 'SKU', hold: null, change: false, engrave: 'plain', noDesign: false, ...o });
const order = (id, ...lines) => ({ id: String(id), buyer: 'Buyer ' + id, lines });
const shop = (sheets, orders) => materialize({ seed: 1, sandbox: false, sheets, orders, archivedSheets: [] });
const issues = (s, sid) => issueOrders(truth(s), sid);
// (A and B are in DIFFERENT sets unless a case says otherwise: round 7, a not-ready mate of the SAME set is the set's wait, never an order issue; sections 20+ are those cases)
const A = sheet('A', 'gold'), B = sheet('B', 'silver', 'ok', 'set-2', 1);
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
  const s = shop([A, { ...B, own, setId: own === 'draft' ? null : 'set-2' }], [order(1, line(), line({ metal: 'silver', copies: [copy('B')] }), line({ metal: 'silver', copies: [copy('B')], tx: 999 + (++tx) }))]);
  t('other sheet ' + own, s, { A: issue ? [1] : [] });
}
// 15 engraving: an unapproved engraving on the other sheet's piece makes that sheet not ready; approved + saved back does not
t('other has unapproved engraving', shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], engrave: 'unapproved' }))]), { A: [1], B: [] });
t('other has approved saved back', shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], engrave: 'approved' }))]), { A: [], B: [] });
// (A's own unapproved engraving never makes the order an issue for A, but it does make A "not ready" for B's side of the order)
t('own unapproved engraving never counts', shop([A, B], [order(1, line({ engrave: 'unapproved' }), line({ metal: 'silver', copies: [copy('B')] }))]), { A: [], B: [1] });
// 16 a piece on two live sheets (a re-nest caught half way): it is on this sheet, and elsewhere only blocks when none of its sheets is ready
{
  const C = sheet('C', 'silver', 'unverified', 'set-2', 2);
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
// 18 Paul's order 4170252963 (round 7): piece 1 on GF Sheet 1, piece 2 on SS Sheet 1, SS not ready (back engravings unsaved), BOTH SHEETS IN THE SAME SET.
//    "It looks perfectly fine to me and it's on both sheets and both sheets are in the same set": the order is no issue for either sheet.
{
  const GF = sheet('GF', 'gold'), SS = { ...sheet('SS', 'silver'), own: 'backUnsaved' };
  const mk = (gf, ss) => shop([gf, ss], [order(4170252963, line({ copies: [copy('GF')], state: 'written' }), line({ metal: 'silver', copies: [copy('SS')], state: 'pooled', engrave: 'approved' })), order(4170252964, line({ copies: [copy('GF')], engrave: 'approved' }))]);
  const tr = truth(mk(GF, SS));
  assert.equal(tr.sheets.GF.label, 'GF Sheet 1'); assert.equal(tr.sheets.SS.label, 'SS Sheet 1');
  assert.deepEqual(issueOrders(tr, 'GF'), [], 'same set: the order is fine'); assert.deepEqual(issueOrders(tr, 'SS'), []);
  assert.equal(tr.sheets.GF.laserReady, true, 'GF has nothing of its own or of any order holding it');
  // the same shop under the OLD rule listed it (this is the false alarm Paul saw)
  assert.deepEqual(issueOrders(truth(mk(GF, SS), { sameSetIsIssue: true }), 'GF'), ['4170252963']);
  // the very same order with SS in ANOTHER set is split between two sets: still a real issue for GF, flagged as a split
  const split = truth(mk(GF, { ...SS, setId: 'set-9' })), e = split.sheets.GF.orders['4170252963'];
  assert.deepEqual(issueOrders(split, 'GF'), ['4170252963']); assert.deepEqual(issueOrders(split, 'SS'), []);
  assert.equal(e.offenders.length, 1); assert.deepEqual([...e.offenders[0].reasons], ['otherSheetNotReady']); assert.equal(e.offenders[0].sheets[0].label, 'SS Sheet 1'); assert.equal(e.offenders[0].split, true);
  n++;
}
// 19 the other sheet is ready again once its trouble is fixed (same pieces): the issue disappears
t('fixed', shop([A, B], [order(1, line(), line({ metal: 'silver', copies: [copy('B')] }))]), { A: [], B: [] });

// ── round 7: a set advances as ONE ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
const two = (x, y) => shop([x, y], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [copy('B')] }))]);
const SA = sheet('A', 'gold', 'ok', 'set-1'), SB = sheet('B', 'silver', 'ok', 'set-1', 1);
// 20 same set, the mate not ready for any reason that matters: the order is no issue for either sheet
for (const own of ['unverified', 'noQr', 'qrPartial', 'held', 'dirty', 'saving', 'noFront', 'error', 'backUnsaved', 'unidentified']) t('same set, mate ' + own, two(SA, { ...SB, own }), { A: [], B: [] });
t('same set, mate has an unapproved engraving', shop([SA, SB], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], engrave: 'unapproved' }))]), { A: [], B: [] });
t('same set, both not ready', two({ ...SA, own: 'noQr' }, { ...SB, own: 'unverified' }), { A: [], B: [] });
// 20b another set is another set: still an issue, flagged split (and only when both sheets are really in a set)
t('another set', two(SA, { ...SB, own: 'unverified', setId: 'set-2' }), { A: [1], B: [] });
// (each sheet reads the other as not in its set; the one that is not ready is what the other waits on: here the not-ready one is the draft / left-out sheet, or B)
for (const [why, x, y, want] of [['mate is a draft (in no set)', SA, { ...SB, own: 'draft', setId: null }, { A: [1], B: [] }], ['mate left out of the set', SA, { ...SB, own: 'solidExcluded' }, { A: [1], B: [] }], ['this sheet is a draft', { ...SA, own: 'draft', setId: null }, { ...SB, own: 'unverified' }, { A: [1], B: [1] }], ['this sheet is left out of the set', { ...SA, own: 'solidExcluded' }, { ...SB, own: 'unverified' }, { A: [1], B: [1] }]]) t(why + ': not the same set', two(x, y), want);
{
  const f = (x, y) => truth(two(x, y)).sheets.A.orders[1];
  assert.equal(f(SA, { ...SB, own: 'unverified', setId: 'set-2' }).offenders[0].split, true, 'another set: split');
  assert.equal(f(SA, { ...SB, own: 'draft', setId: null }).offenders[0].split, undefined, 'a draft is in no set: a wait, not a split');
  assert.equal(f(SA, { ...SB, own: 'solidExcluded' }).offenders[0].split, undefined, 'left out of its set: a wait, not a split');
  n++;
}
// 20c an order over three sheets: the not-ready mate of the same set is the set's wait, the one in another set is the real issue (only that piece is listed)
{
  const C2 = sheet('C', 'rose', 'unverified', 'set-2', 1);
  const s = shop([SA, { ...SB, own: 'unverified' }, C2], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [copy('B')] }), line({ metal: 'rose', copies: [copy('C')] }))]);
  const tr = truth(s), e = tr.sheets.A.orders[1];
  assert.deepEqual(issueOrders(tr, 'A'), ['1']); assert.deepEqual(e.offenders.map(o => o.sheets.map(z => z.id)), [['C']], 'only the piece on the other set is listed');
  assert.deepEqual(issueOrders(tr, 'B'), ['1'], 'B sees C, in another set, the same way'); assert.deepEqual(tr.sheets.B.orders[1].offenders.map(o => o.sheets.map(z => z.id)), [['C']]);
  assert.deepEqual(issueOrders(tr, 'C'), ['1'], 'C sees A (ready) and B (not ready, in another set from C): B is the issue'); assert.deepEqual(tr.sheets.C.orders[1].offenders.map(o => o.sheets.map(z => z.id)), [['B']]);
  n++;
}
// 20d a real problem on another piece still lists the order (the same-set mate's piece is not what is listed)
{
  const s = shop([SA, { ...SB, own: 'unverified' }], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [copy('B')] }), line({ copies: [POOLED], state: 'pooled' }))]);
  const tr = truth(s), e = tr.sheets.A.orders[1];
  assert.deepEqual(issueOrders(tr, 'A'), ['1']); assert.deepEqual(e.offenders.map(o => [...o.reasons]), [['pooled']]); n++;
}
// 20e a hold still holds, in the same set too (an explicit stop is not an inference about where pieces are)
t('same set, held mate piece', shop([SA, SB], [order(1, line(), line({ metal: 'silver', copies: [copy('B')], hold: 'changed', state: 'written' }))]), { A: [1], B: [1] });
t('same set, mate not ready AND a piece unnested', shop([SA, { ...SB, own: 'unverified' }], [order(1, line(), line({ metal: 'silver', copies: [copy('B')] }), line({ copies: [NONE], problems: ['unmatchedSku'], sku: '', state: 'unmatched' }))]), { A: [1], B: [1] });
// 20f both sheets laser-ready: nothing at all
t('same set, both ready', two(SA, SB), { A: [], B: [] });
// 21 the SET's wait (R7-1's gate, said once and never an order issue): the other sheets of the set that keep it from being approved
{
  const w = (own, extra = {}) => { const s = shop([SA, { ...SB, own, ...extra }], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [copy('B')] }))]); return [setWaits(s, truth(s), 'A'), setWaits(s, truth(s), 'B')]; };
  for (const [own, want] of [['ok', []], ['noQr', []], ['qrPartial', []], ['unverified', []], ['held', []], ['noFront', []], ['backUnsaved', []], ['done', []], ['reopened', []], ['dirty', ['B']], ['saving', ['B']], ['error', ['B']], ['solidExcluded', ['B']], ['unidentified', ['B']]]) {
    assert.deepEqual(w(own)[0], want, `set wait of A for a mate that is ${own}`); assert.deepEqual(w(own)[1], [], 'a sheet never waits on itself'); n++;
  }
  const un = shop([SA, SB], [order(1, line({ copies: [copy('A')] }), line({ metal: 'silver', copies: [copy('B')], engrave: 'unapproved' }))]);
  assert.deepEqual(setWaits(un, truth(un), 'A'), ['B'], 'a mate with an engraving still to approve is the set wait'); assert.deepEqual(setWaits(un, truth(un), 'B'), []);
  // a cut sheet, a sheet in no set, and a one-sheet set have none
  const cut = shop([{ ...SA, own: 'done' }, { ...SB, own: 'unidentified' }], [order(1, line({ copies: [copy('A')] }))]);
  assert.deepEqual(setWaits(cut, truth(cut), 'A'), []); const lone = shop([{ ...SA, own: 'ok' }], [order(1, line())]); assert.deepEqual(setWaits(lone, truth(lone), 'A'), []);
  const draftA = shop([{ ...SA, own: 'draft', setId: null }, { ...SB, own: 'unidentified' }], [order(1, line({ copies: [copy('A')] }))]); assert.deepEqual(setWaits(draftA, truth(draftA), 'A'), []);
  // the set wait never depends on an order issue: B with an order problem of its own (a piece of its order still unnested) is soft, so it is not a set wait
  const soft = shop([SA, SB], [order(1, line({ metal: 'silver', copies: [copy('B')] }), line({ copies: [POOLED], state: 'pooled' }))]);
  assert.equal(hardBlock(soft.sheets.find(x => x.id === 'B'), truth(soft)), null, 'an order waiting for another piece is soft'); assert.deepEqual(setWaits(soft, truth(soft), 'A'), []); n++;
}
console.log(`PASS: ${n} hand-made cases agree with Paul's rules (oracle self-check)`);
