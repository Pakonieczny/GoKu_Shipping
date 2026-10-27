// DUPID / DASHFACE (Paul, 27 Oct 2026, order 4208640203, SANDBOX, Set-2 · 2026-10-28): "I noticed that this order is correctly added to the sheet for both pieces. However, the system does not
// recognize their presence on the sheet and is falsely stating that they're not there. I also noticed that, even though this is one order in the same order, these pieces on the sheet are not
// recognized as being part of the same order. There's a mistake somewhere and I need this corrected as I can't have this problem in production."
// What the sandbox records showed (read only): the order has TWO lines (two Etsy transactions, 5262269683 "Front - CMG    Back - 3.22.19" and 5262269685 "Front - LRG    Back- 9.2.16"), each quantity 1,
// one piece each. SS Sheet 1 (silver-s5-mvpxs58r) holds 76 charms and 75 placements: the CMG piece (…683_1) and order 4211837232's piece (…5264597300_1), both 5X8.5, carry the SAME charm id
// "pool:charmnest_sandbox_master_5X8_5_ai:0" (the master's own charm, handed to the first line of the design, and again after the design was read again). The nest placed that id once; the
// sheet's files and its poolIds give the spot to 4211837232 (a map by id: the last wins), the page's own index gave it to …683_1 (the first wins): the order window said "SS Sheet 1" for both pieces
// while the order check said "Not on a sheet yet", and the sheet showed only ONE piece of the order. Offline: plain records shaped like the sandbox ones, no network.
//   node tests/charm-nest/order-on-sheet.cjs
'use strict';
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const PP = require(path.join(root, 'charm-nest-pool-pieces.js'));
const R = require(path.join(root, 'charm-nest-readiness.js'));
const OP = require(path.join(root, 'charm-nest-order-pieces.js'));

let ran = 0; const ok = name => { ran++; console.log('  ✓ ' + name); };

/* ── the sandbox, as its records are (only the fields these readers use) ── */
const SRC = 'pool:charmnest_sandbox_master_5X8_5_ai', SHARED = SRC + ':0';
const RID = '4208640203', TX1 = '5262269683', TX2 = '5262269685', OTHER = '4211837232', TXO = '5264597300';
const P1 = `${RID}_${TX1}_1`, P2 = `${RID}_${TX2}_1`, PO = `${OTHER}_${TXO}_1`;
const F1 = '4205952163_5260212512_1', F2 = '4209440796_5260148933_1';
const SHEET_ID = 'silver-s5-mvpxs58r', SET2 = 'set-2026-10-28-2';
const charm = (id, poolId, order, sku = '5X8.5', src = SRC) => ({ id, poolId, order, sourceId: src, sku, name: `${order} · ${sku}`, hash: '061cfa08', index: 0, layer: 'CUT' });
const place = (id, n, cx, layer) => ({ n, id, layer, cxPt: cx, cyPt: 30, angle: 0, wPt: 17, hPt: 26, scale: 0.971 });
/** SS Sheet 1 of Set-2 as the sandbox holds it (cut down to five charms): the CMG piece and 4211837232's piece both under SHARED, the CMG one first; ONE placement of SHARED (written for 4211837232). */
function sandboxSheet() {
  const charms = [charm(`pool:m_MICROPHONE:0`, F1, '4205952163', 'MICROPHONE', 'pool:m_MICROPHONE'), charm(SHARED, P1, RID), charm(`${SRC}:${P2}`, P2, RID), charm(`pool:m_TAG3:${F2}`, F2, '4209440796', 'TINY INITIAL TAG 3', 'pool:m_TAG3'), charm(SHARED, PO, OTHER)];
  const placements = [place(charms[0].id, 1, 10, '4205952163  MICROPHONE'), place(SHARED, 2, 40, `${OTHER}  5X8.5`), place(charms[2].id, 3, 70, `${RID}  5X8.5`), place(charms[3].id, 4, 100, '4209440796  TINY INITIAL TAG 3')];
  return { id: SHEET_ID, metal: 'silver', metalLabel: 'SS · sheet 5', fileBase: 'SS_Oct.28.26_Set-2_Sheet-1', folder: 'SS_Oct.28.26_Set-2_Sheet-1', sheetIndex: 1, page: 5, setId: SET2, setSeq: 2, status: 'complete', draft: false, releaseFull: true,
    runId: 'run-2026-10-26-n35zh9urw9', charms, placements, charmCount: charms.length, placedCount: placements.length, poolIds: [F1, PO, P2, F2], orders: ['4205952163', OTHER, RID, '4209440796'], updatedAt: 1793072603694 };
}
const SNAP = { title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm Minimalist Tiny Pendant Gift for Her Custom Initial Charm', listingId: '4458930445', buyer: 'Megan Gaudio' };
/** The run's lines (the server's rows): the CMG line still "pooled" (its piece on no saved sheet), the LRG line and the others written. */
function runLines() {
  const line = (rid, tx, poolIds, state, extra) => Object.assign({ key: `${rid}_${tx}`, orderId: rid, transactionId: tx, poolIds, state, sku: '5X8.5', material: 'silver', quantity: 1, pieceCount: 1, snap: SNAP }, extra || {});
  return [line(RID, TX1, [P1], 'pooled'), line(RID, TX2, [P2], 'written'), line(OTHER, TXO, [PO], 'written', { snap: { buyer: 'Other buyer' } }),
    line('4205952163', '5260212512', [F1], 'written', { sku: 'MICROPHONE' }), line('4209440796', '5260148933', [F2], 'written', { sku: 'TINY INITIAL TAG 3' })];
}
/** The pool rows of the order as getOrderPieces answered them: the CMG piece ready on no sheet (and an older set), the LRG piece written on SS Sheet 1. */
const POOLS = [
  { poolId: P1, orderId: RID, transactionId: TX1, lineKey: `${RID}_${TX1}`, sku: '5X8.5', material: 'silver', copy: 1, quantity: 1, state: 'ready', sheetId: null, setId: 'set-2026-10-27-1', updatedAt: 1793062896422 },
  { poolId: P2, orderId: RID, transactionId: TX2, lineKey: `${RID}_${TX2}`, sku: '5X8.5', material: 'silver', copy: 1, quantity: 1, state: 'written', sheetId: SHEET_ID, setId: SET2, sheetName: 'SS_Oct.28.26_Set-2_Sheet-1', updatedAt: 1793068943547 }];
/** The page's own index of a live sheet (charm-nest-bridge.js Pool.index: of charms with one id the FIRST takes the placement). */
const pageIndex = sh => { const byId = new Map(), on = new Map(); for (const c of sh.charms) if (!byId.has(c.id)) byId.set(c.id, c); for (const p of sh.placements) { const c = byId.get(p.id); if (c && !on.has(c.poolId)) on.set(c.poolId, sh); } return on; };
const orderLines = () => runLines().filter(l => l.orderId === RID).map(l => Object.assign({}, l, { spec: { designSku: '5X8.5', material: 'silver', quantity: 1 } }));
const windowPieces = (live, rec) => OP.resolve({ orderId: RID, lines: orderLines(), pools: POOLS, sheets: [rec], sheetsKnown: true, livePlaced: id => pageIndex(live).get(id) || null, liveSheet: id => (id === live.sheetId ? live : null) });
const liveOf = rec => ({ sheetId: rec.id, metal: rec.metal, sheetIndex: rec.sheetIndex, page: rec.page, fileBase: rec.fileBase, setId: rec.setId, charms: rec.charms.map(c => Object.assign({}, c)), placements: rec.placements.map(p => Object.assign({}, p)) });
const byOrderOnSheet = rec => { const m = new Map(); for (const { c } of PP.placedCharms(rec)) if (c && (rec.poolIds || []).includes(c.poolId)) { const k = String(c.order); (m.get(k) || m.set(k, []).get(k)).push(c.poolId); } return m; };

/* ═══ 1 · how many pieces: two lines, one piece each (two pieces are right) ═══ */
const V = (name, value) => ({ name, value });
const ctx = { optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: s => ({ sku: s }), masterLoose: () => '' };
const readLine = (tx, pers) => O.interpretLine({ receiptId: RID, updateTs: 0 }, { transactionId: tx, listingId: '4458930445', sku: 'Mini_0445', title: SNAP.title, quantity: 1, metalKey: 'silver', metalLabel: 'Sterling Silver', personalization: [pers],
  variations: [V('Metal Choice', 'Sterling Silver'), V('Charm Type', 'Tag1 (front+back)'), V('Personalization', pers), V('Font Choice', 'Ace')] }, ctx);
{
  const a = readLine(TX1, 'Front - CMG    Back - 3.22.19'), b = readLine(TX2, 'Front - LRG    Back- 9.2.16');
  assert.equal(O.pieceCountOf(a), 1, 'the CMG line is ONE piece, engraved on both faces'); assert.equal(O.pieceCountOf(b), 1, 'the LRG line is ONE piece');
  assert.equal(a.quantity, 1); assert.equal(b.quantity, 1);
  const ps = R.pieces(runLines(), [sandboxSheet()])[RID];
  assert.deepEqual(ps.map(p => [p.poolId, p.lineKey]), [[P1, `${RID}_${TX1}`], [P2, `${RID}_${TX2}`]], 'the order has two pieces, one per Etsy line, both under ONE order');
  ok('1 · "Front - … Back - …" on a front + back tag is ONE piece; the order has two because it has two Etsy lines (two transactions, quantity 1 each, two personalisations)');
}

/* ═══ 2 · the words: "Back - 3.22.19" is the back's (DASHFACE) ═══ */
{
  for (const [tx, pers, F, B] of [[TX1, 'Front - CMG    Back - 3.22.19', 'CMG', '3.22.19'], [TX2, 'Front - LRG    Back- 9.2.16', 'LRG', '9.2.16'], [TX1, 'Front - CMG Back - 3.22.19', 'CMG', '3.22.19'], [TX1, 'Front – CMG Back – 3.22.19', 'CMG', '3.22.19']]) {
    const e = readLine(tx, pers).engraving;
    assert.deepEqual([e.mode, e.front && e.front.text, e.back && e.back.text, e.needsPerson, e.reviewKind], ['both', F, B, false, undefined], JSON.stringify(pers) + ' → ' + JSON.stringify(e));
  }
  // the forms that were already read stay as they were; a face word joined to the words before it, or inside them, is no label
  assert.deepEqual(['front - A back B', 'front: A back: B', 'front - W\nback - 1. 27. 24'].map(t => { const f = O.faceParse(t); return [f.front && f.front.text, f.back && f.back.text]; }), [['A', 'B'], ['A', 'B'], ['W', '1. 27. 24']]);
  { const f = O.faceParse('front - Mary-Back - 1999'); assert.deepEqual([f.front && f.front.text, f.back], ['Mary-Back - 1999', null], 'a hyphen-joined face word stays part of the words'); }
  { const f = O.faceParse('front - Front Row\nback - Back to Basics'); assert.deepEqual([f.front.text, f.back.text], ['Front Row', 'Back to Basics'], 'face words inside the words are no labels'); }
  { const f = O.faceParse('front - A - B'); assert.deepEqual([f.front.text, f.back], ['A - B', null], 'a dash inside the words is no label'); }
  ok('2 · "Front - CMG    Back - 3.22.19" reads front CMG and back 3.22.19 (a date is the back\'s words), no person needed; the forms read before are read the same');
}

/* ═══ 3 · the record: which piece a placement stands for (DUPID) ═══ */
{
  const rec = sandboxSheet(), pc = PP.placedCharms(rec);
  assert.equal(pc.length, rec.placements.length);
  assert.equal(pc.find(x => x.p.id === SHARED).c.poolId, PO, 'the shared spot is the piece the record lists (4211837232), as the files were written');
  assert.ok(!pc.some(x => x.c && x.c.poolId === P1), 'the CMG piece has no spot on the saved sheet');
  // the record's list decides: a record that lists the first of them gives it the spot; one with no list gives it to the last (as the writer's map by id)
  assert.equal(PP.placedCharms(Object.assign(sandboxSheet(), { poolIds: [F1, P1, P2, F2] })).find(x => x.p.id === SHARED).c.poolId, P1);
  assert.equal(PP.placedCharms(Object.assign(sandboxSheet(), { poolIds: [] })).find(x => x.p.id === SHARED).c.poolId, PO);
  const by = byOrderOnSheet(rec);
  assert.deepEqual(by.get(RID), [P2], 'only the LRG piece of the order is on the saved sheet'); assert.deepEqual(by.get(OTHER), [PO]);
  ok('3 · a saved sheet whose two pieces share one id: its placement is the piece its own poolIds names (4211837232), the CMG piece is on no saved sheet');
}

/* ═══ 4 · the order check (server and page alike) and the order window agree ═══ */
{
  const rec = sandboxSheet(), reps = R.orderReports(runLines(), [rec]);
  const mine = R.forSheet(reps[RID], SHEET_ID, SET2, 'silver');
  assert.equal(mine.ready, false); assert.deepEqual(mine.blocks.map(b => [b.key, b.poolId, b.sheetId]), [['pooled', P1, null]], 'the order check: the CMG piece waits, on no sheet');
  assert.equal(reps[RID].pieceCount, 2, 'one order, two pieces');
  assert.equal(R.forSheet(reps[OTHER], SHEET_ID, SET2, 'silver').ready, true, '4211837232 is whole on the sheet');
  const iss = R.issues(rec, { steps: ['orders'], rows: runLines(), allSheets: [rec] }).filter(x => x.orderId === RID);
  assert.equal(iss.length, 1); assert.match(iss[0].pieces[0].why, /not on a sheet yet/i, 'the Library\'s Order check: "Not on a sheet yet" for the CMG piece: ' + JSON.stringify(iss[0].pieces));
  // the page BEFORE (its index: the first of two charms with one id takes the spot): the window said "SS Sheet 1" for the CMG piece, against the order check
  const before = windowPieces(liveOf(rec), rec);
  assert.deepEqual(before.map(p => [p.poolId, p.nested]), [[P1, true], [P2, true]], 'the old reading: both pieces "on SS Sheet 1" (the bug the window showed)');
  // the page NOW: its sheet told apart first (Pool's index: CharmNestPoolPieces.uniqueIds), then read the same way
  const live = liveOf(rec), renamed = PP.uniqueIds(live);
  assert.deepEqual(renamed.map(x => [x.was, x.charm.poolId, x.charm.id]), [[SHARED, P1, `${SRC}:${P1}`]], 'the CMG piece gets its own id; 4211837232 keeps the spot it was written in');
  assert.equal(new Set(live.charms.map(c => c.id)).size, live.charms.length, 'every charm of the sheet has an id of its own');
  assert.deepEqual(live.placements, rec.placements, 'no placement is touched');
  const after = windowPieces(live, rec);
  assert.deepEqual(after.map(p => [p.poolId, p.nested, p.sheetLabel || null]), [[P1, false, null], [P2, true, 'SS Sheet 1']], 'the window now: the CMG piece waits, the LRG piece is on SS Sheet 1');
  assert.match(after[0].reason, /waiting to be placed|not on a sheet yet/);
  const checkSays = new Set(mine.blocks.filter(b => b.key === 'pooled').map(b => b.poolId)), windowSays = new Set(after.filter(p => !p.nested).map(p => p.poolId));
  assert.deepEqual([...windowSays], [...checkSays], 'the window and the order check name the same piece as on no sheet');
  assert.equal(pageIndex(live).get(PO), live, 'the page reads 4211837232 on the sheet, as its record does');
  ok('4 · the order check says the CMG piece is not on a sheet; the order window (page) said "SS Sheet 1"; with the sheet told apart both say the same, and 4211837232 keeps its spot');
}

/* ═══ 5 · placed, the two pieces are on the sheet as ONE order (the next nest of the sheet places the waiting piece, append only) ═══ */
{
  const rec = sandboxSheet(), live = liveOf(rec); PP.uniqueIds(live);
  // what the sheet's next nest writes: every placed charm where it was, the waiting piece in a free spot, under its own id; the record lists it
  const waiting = live.charms.find(c => !live.placements.some(p => p.id === c.id));
  assert.equal(waiting.poolId, P1, 'the one piece the sheet still has to place is the CMG piece');
  const nested = Object.assign(sandboxSheet(), { charms: live.charms, placements: live.placements.concat([place(waiting.id, 5, 130, `${RID}  5X8.5`)]), poolIds: [F1, PO, P2, F2, P1] });
  nested.placedCount = nested.placements.length;
  const by = byOrderOnSheet(nested);
  assert.deepEqual(by.get(RID).slice().sort(), [P1, P2].sort(), 'both pieces of the order are on the sheet, grouped under the one order');
  assert.deepEqual(by.get(OTHER), [PO], '4211837232 keeps its own piece');
  const lines = runLines().map(l => (l.orderId === RID ? Object.assign({}, l, { state: 'written' }) : l)), reps = R.orderReports(lines, [nested]);
  assert.equal(R.forSheet(reps[RID], SHEET_ID, SET2, 'silver').ready, true, 'the order check passes: both pieces are on this sheet');
  assert.deepEqual(R.pieces(lines, [nested])[RID].map(p => [p.poolId, p.sheetId]), [[P1, SHEET_ID], [P2, SHEET_ID]]);
  assert.equal(R.issues(nested, { steps: ['orders'], rows: lines, allSheets: [nested] }).filter(x => x.orderId === RID).length, 0, 'no Order check issue');
  const w = OP.resolve({ orderId: RID, lines: orderLines().map(l => Object.assign(l, { state: 'written' })), pools: POOLS, sheets: [nested], sheetsKnown: true, livePlaced: id => pageIndex(Object.assign(live, { placements: nested.placements })).get(id) || null, liveSheet: () => null });
  assert.deepEqual(w.map(p => [p.poolId, p.sheetLabel]), [[P1, 'SS Sheet 1'], [P2, 'SS Sheet 1']], 'the order window: both on SS Sheet 1');
  ok('5 · once placed, both pieces sit on SS Sheet 1 as one order: the order check, the issues list and the order window agree');
}

/* ═══ 6 · uniqueIds: what it renames and what it leaves ═══ */
{
  // nothing to do on a sheet of distinct ids; a second pass changes nothing
  const plain = { charms: [charm('a:1', 'X_1_1', 'X'), charm('a:2', 'X_2_1', 'X')], placements: [] }; assert.deepEqual(PP.uniqueIds(plain), []);
  const s = liveOf(sandboxSheet()); PP.uniqueIds(s); assert.deepEqual(PP.uniqueIds(s), [], 'a second pass renames nothing');
  // ONE piece listed twice under one id is no clash of two pieces; a charm with no piece is never renamed
  const twice = { charms: [charm('d:0', 'Y_1_1', 'Y'), charm('d:0', 'Y_1_1', 'Y')], placements: [{ id: 'd:0' }] }; assert.deepEqual(PP.uniqueIds(twice), []);
  const loose = { charms: [Object.assign(charm('e:0', null, 'Z'), { poolId: undefined }), charm('e:0', 'Z_1_1', 'Z')], placements: [] }; assert.deepEqual(PP.uniqueIds(loose), []);
  // three pieces under one id: the last keeps it, the other two get their own; an id already taken by another piece is not handed out again
  const three = { charms: [charm('f:0', 'A_1_1', 'A', 'S', 'f'), charm('f:A_1_1', 'Q_9_1', 'Q', 'S', 'f'), charm('f:0', 'B_1_1', 'B', 'S', 'f'), charm('f:0', 'C_1_1', 'C', 'S', 'f')], placements: [{ id: 'f:0' }] };
  const r3 = PP.uniqueIds(three); assert.deepEqual(r3.map(x => x.charm.poolId), ['A_1_1', 'B_1_1']);
  assert.equal(new Set(three.charms.map(c => c.id)).size, 4, 'every id distinct: ' + JSON.stringify(three.charms.map(c => c.id)));
  assert.equal(three.charms[3].id, 'f:0', 'the last keeps the placed id'); assert.equal(three.charms[0].id, 'f:A_1_1~2', 'an id another piece holds is not reused');
  // the record's list names the owner when the caller has it
  const s2 = liveOf(sandboxSheet()); PP.uniqueIds(s2, [F1, P1, P2, F2]); assert.equal(s2.charms.find(c => c.poolId === P1).id, SHARED, 'listed: the CMG piece keeps the spot'); assert.equal(s2.charms.find(c => c.poolId === PO).id, `${SRC}:${PO}`);
  ok('6 · uniqueIds renames only charms of ANOTHER piece under a shared id, keeps the placed one, never reuses an id, and is idempotent');
}

/* ═══ 7 · the root cause in the page: a piece is never the master's own charm ═══ */
{
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const line = src.split('\n').find(l => l.includes('const charm = pp ? cloneCharm(baseFor(cut,'));
  assert.ok(line, 'makePool\'s charm line is found');
  assert.ok(!/\?\s*base\s*:/.test(line) && !/!base\.poolId/.test(line), 'no copy of a line is the master\'s own charm (it was: copy === 1 && !base.poolId ? base : …): ' + line.trim().slice(0, 200));
  assert.ok(/: cloneCharm\(base, `\$\{src\.id\}:\$\{poolId\}`\);/.test(line), 'every plain copy is a clone named by its pool id');
  // the page tells two pieces of one id apart wherever it reads where pieces are: its index, a restore, a nest before it starts, a sheet put back from its record, the sheet window
  assert.ok(/healIds\(sheets\);/.test(src) && /Pool\.uniqueIds\(null, \{ settle: true \}\)/.test(src), 'Pool\'s index and the workspace restore tell the pieces apart');
  assert.equal((src.match(/for \(const \{ p, c: rc \} of placedOfRecord\(d\)\)/g) || []).length, 2, 'both sheet rebuilds from a record take the charm its poolIds names');
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), sn = html.slice(html.indexOf('function startNestReady('), html.indexOf('let items = activeCharms(sh);', html.indexOf('function startNestReady(')));
  assert.ok(/Pool\.uniqueIds\(\[sh\]\)/.test(sn), 'a nest tells the sheet\'s pieces apart before it reads them');
  const sw = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8'); assert.ok(/PP\.placedCharms\(rec\)/.test(sw), 'the sheet window reads a record\'s placements by its poolIds');
  ok('7 · makePool clones every copy under its pool id; the index, the restore, a nest, a rebuild from a record and the sheet window all tell two pieces of one id apart');
}

console.log(`order-on-sheet: ${ran} checks passed`);
