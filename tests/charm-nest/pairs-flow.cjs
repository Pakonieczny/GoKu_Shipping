// PAIRFLOW: the Sorter's policy for pairs, mismatched pairs and multi-piece groups (Paul, 9 Oct 2026).
//   R2  a pair is placed together: same order, same sheet, same metal
//   R3  when a pair's (or a disc necklace's) pieces still end up on different sheets, it is said plainly and tracked
// A GROUP is one order line (receipt + transaction). The unit the sorter keeps whole is the ORDER, which holds it; the group is the
// second guard for the rare ways one line still comes apart. Each path is the page's own code sliced out of the real files
// (charm-nest-1.html, charm-nest-bridge.js, charm-nest-order-release.js), with fakes only for what it reads. No network, no Firestore.
//   node tests/charm-nest/pairs-flow.cjs
const fs = require('fs'), vm = require('vm'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const noNested = require('./_noNestedArrays.cjs');
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const slice = (src, from, to) => { const a = src.indexOf(from); assert(a >= 0, 'not found: ' + from); const b = src.indexOf(to, a + from.length); assert(b > a, 'not found: ' + to); return src.slice(a, b); };
const plain = v => JSON.parse(JSON.stringify(v));          // made in the vm, compared here
const ok = [];
let Pair = null; try { Pair = require(path.join(root, 'charm-nest-pair.js')); } catch (_) { /* the page works without it */ }

// the page's own policy code: the helpers, keepOrdersWhole, orderSummary and overflowToNextSheet in one slice
const policy = slice(html, 'function groupOf(', 'function inflatedArea(');
const feed = slice(html, 'const FEED_ORDERS = 3;', '/* A stopped run starts none of its sheets');

// a charm of an order line: the pool id is `receipt_transaction_copy`, as Pool.makePool writes it
const piece = (receipt, tx, copy, extra) => Object.assign({ id: `${receipt}_${tx}_${copy}`, poolId: `${receipt}_${tx}_${copy}`, order: String(receipt), orderDate: +receipt, orderInfo: { receiptId: String(receipt), transactionId: String(tx), copy, quantity: 2 } }, extra);
const placement = c => ({ id: c.id, cxPt: 5, cyPt: 5, angle: 0 });
const ctxFor = (extra) => {
  const log = [], toasts = [], ctx = Object.assign({ Set, Map, Math, JSON, Object, Array, String, Number, window: {}, agent: (a, k, t) => log.push({ k, t }), toast: (t, k) => toasts.push({ t, k }), labelOf: m => m, renderCard() {}, renderRail() {}, updateTopSub() {}, computeSaturation() {}, manualSheetClosed: () => false,
    S: { sheets: {}, settings: {} }, startNest() {} }, extra);
  ctx.log = log; ctx.toasts = toasts; vm.createContext(ctx); return ctx;
};

// 1 · a charm's group: the same string the contract's groupKey gives, from the order fields or the pool id, none for a file dropped by hand
{
  const c = ctxFor(); vm.runInContext(policy + ';this.groupOf = groupOf; this.sideWord = sideWord;', c);
  assert.equal(c.groupOf(piece(1001, 77, 1)), '1001:77', 'from orderInfo');
  assert.equal(c.groupOf({ poolId: '1001_77_2' }), '1001:77', 'from the pool id alone (an old record)');
  assert.equal(c.groupOf(piece(1001, 77, 2)), c.groupOf(piece(1001, 77, 1)), 'every piece of a line has one group');
  assert.notEqual(c.groupOf(piece(1001, 78, 1)), c.groupOf(piece(1001, 77, 1)), 'another line of the same order is another group');
  assert.equal(c.groupOf({ id: 'file:3', order: 'file/Layer 1' }), null, 'artwork dropped by hand has no group');
  assert.equal(c.groupOf(null), null);
  assert.equal(c.sideWord({ side: 'L' }), 'Left'); assert.equal(c.sideWord({ side: 'R' }), 'Right'); assert.equal(c.sideWord({}), '');
  if (Pair) { const x = piece(1001, 77, 1); assert.equal(c.groupOf(x), Pair.groupKey(x), 'agrees with CharmNestPair.groupKey'); const c2 = ctxFor({ CharmNestPair: Pair }); vm.runInContext(policy + ';this.groupOf = groupOf;', c2); assert.equal(c2.groupOf(x), Pair.groupKey(x), 'and uses it when the page has loaded it'); }
  ok.push('a charm\'s group is its order line (receipt:transaction), read from its order fields or its pool id; a hand-dropped file has none');
}

// 2 · splitGroups and its words
{
  const c = ctxFor(); vm.runInContext(policy + ';this.splitGroups = splitGroups; this.splitWords = splitWords;', c);
  const l = piece(2001, 5, 1, { side: 'L' }), r = piece(2001, 5, 2, { side: 'R' }), other = piece(2002, 9, 1);
  const s1 = { page: 1, charms: [l, other] }, s2 = { page: 2, charms: [r] };
  const g = c.splitGroups([s1, s2]);
  assert.equal(g.length, 1, 'only the pair that is on two sheets'); assert.equal(g[0].group, '2001:5');
  assert.equal(c.splitWords(g[0]), 'Left on sheet 1, Right on sheet 2', 'a mismatched pair is told by its sides');
  assert.deepEqual(plain(c.splitGroups([{ page: 1, charms: [l, r] }, { page: 2, charms: [other] }]).map(x => x.group)), [], 'a pair on one sheet is not split');
  const a = piece(2003, 1, 1), b = piece(2003, 1, 2);
  assert.equal(c.splitWords(c.splitGroups([{ page: 1, charms: [a] }, { page: 4, charms: [b] }])[0]), '1 piece on sheet 1, 1 piece on sheet 4', 'a pair of one design says pieces');
  assert.deepEqual(plain(c.splitGroups([s1, s2], new Set(['nope']))), [], 'limited to the groups asked for');
  assert.deepEqual(plain(c.splitGroups([{ page: 1, charms: [Object.assign({}, a, { excluded: true })] }, { page: 2, charms: [b] }])), [], 'an excluded piece is not a piece');
  ok.push('splitGroups finds a line on two sheets and words it (Left on sheet 1, Right on sheet 2); a line on one sheet is whole');
}

// 3 · feedTurn: a piece whose mate is already placed on the sheet comes in this turn, whatever its order's rank
{
  const mk = { S: { settings: {} }, carefulNest: () => true, topupRoom: () => Infinity };
  const c = ctxFor(mk); vm.runInContext(policy + ';' + feed + ';this.feedTurn = feedTurn;', c);
  // the control: nothing placed, ten single-piece lines in ten orders, the oldest three go (older = smaller date)
  const singles = Array.from({ length: 10 }, (_, i) => ({ id: 's' + i, order: 's' + i, orderDate: 100 + i, poolId: `${900 + i}_1_1`, orderInfo: { receiptId: String(900 + i), transactionId: '1', copy: 1, quantity: 1 } }));
  const ctl = { runId: 'r', metal: 'gold', placements: [], topup: null };
  assert.deepEqual(c.feedTurn(ctl, singles).map(x => x.id).sort(), ['s0', 's1', 's2'], 'normal orders: the oldest three, as before');
  assert.equal(ctl.feedWait.length, 7);
  // a pair: one piece placed earlier (its order is the youngest), the other fresh: it joins the turn
  const a = piece(5000, 1, 1), b = piece(5000, 1, 2);
  const sh = { runId: 'r', metal: 'gold', placements: [placement(a)], topup: null };
  const turn = c.feedTurn(sh, [a, b].concat(singles)).map(x => x.id);
  assert(turn.includes(b.id) && turn.includes(a.id), 'the pair\'s other piece comes in with the turn, next to its mate: ' + turn);
  assert(!sh.feedWait.includes(b.id), 'and never waits to be passed on alone');
  assert.deepEqual(turn.filter(id => id.startsWith('s')).sort(), ['s0', 's1', 's2'], 'the three oldest orders still go too');
  // a pair order that is fresh as a whole is one order of the three, both pieces together
  const p1 = piece(60, 1, 1), p2 = piece(60, 1, 2), sh2 = { runId: 'r', metal: 'gold', placements: [], topup: null };
  const t2 = c.feedTurn(sh2, [p1, p2].concat(singles)).map(x => x.id);
  assert(t2.includes(p1.id) && t2.includes(p2.id) && t2.filter(id => id.startsWith('s')).length === 2, 'a whole pair is one order of the three: ' + t2);
  // a sheet filling its gaps: the same
  c.topupRoom = () => 1; const sh3 = { runId: 'r', metal: 'gold', placements: [placement(a)], topup: { tried: [] } };
  const t3 = c.feedTurn(sh3, [a, b].concat(singles)).map(x => x.id);
  assert(t3.includes(b.id), 'a sheet filling its gaps takes the late mate too (room for one order, the mate\'s order is that one)');
  c.topupRoom = () => Infinity;
  // a line with no piece placed is left exactly as it was: the same turn with or without the group code
  const sh4 = { runId: 'r', metal: 'gold', placements: [], topup: null }, plainItems = singles.map(x => ({ id: x.id, order: x.order, orderDate: x.orderDate }));
  assert.deepEqual(c.feedTurn(sh4, plainItems).map(x => x.id).sort(), ['s0', 's1', 's2'], 'charms with no order fields: unchanged');
  ok.push('feedTurn: a late piece whose mate is placed joins the turn; a normal order, a whole pair and a hand-dropped charm are fed exactly as before');
}

// 4 · feedOn: a full sheet passes on only whole waiting orders; no line has pieces both placed and passed on
{
  const mk = { S: { settings: {} }, carefulNest: () => true, topupRoom: () => Infinity, allSheets: () => [], activeCharms: sh => sh.charms.filter(c => !c.excluded) };
  const moved = [];
  const c = ctxFor(mk);
  vm.runInContext(policy + ';' + feed + ';this.feedTurn = feedTurn; this.feedOn = feedOn;', c);
  c.overflowToNextSheet = sh => { moved.push(sh.rejects.slice()); return null; };   // (the move itself is section 6)
  const singles = Array.from({ length: 6 }, (_, i) => ({ id: 's' + i, order: 's' + i, orderDate: 100 + i }));
  const a = piece(7000, 1, 1), b = piece(7000, 1, 2);
  const sh = { runId: 'r', metal: 'gold', page: 1, status: 'complete', placements: [placement(a)], rejects: [], charms: [a, b].concat(singles), releaseFull: true, verification: { ok: true }, endedBy: 'complete' };
  c.allSheets = () => [sh];
  const turn = c.feedTurn(sh, sh.charms.slice());
  assert(turn.some(x => x.id === b.id));
  sh.placements = turn.map(placement);   // the turn is placed and saved
  c.feedOn(sh);
  const passed = moved.at(-1) || [];
  assert(passed.length && !passed.includes(a.id) && !passed.includes(b.id), 'the pair stays together on its sheet while the waiting orders move on: ' + passed);
  ok.push('feedOn: a full sheet passes on the waiting orders and never half a pair');
}

// 5 · keepOrdersWhole: the pair moves whole
{
  const c = ctxFor(); vm.runInContext(policy + ';this.keepOrdersWhole = keepOrdersWhole;', c);
  const L = piece(8100, 1, 1, { side: 'L' }), R = piece(8100, 1, 2, { side: 'R' }), other = piece(8000, 1, 1), byId = ids => new Map(ids.map(x => [x.id, x]));
  // (a) a search seated one body and missed the other: both go (order level, as always)
  let sh = { metal: 'gold', placements: [placement(L), placement(other)], rejects: [R.id] };
  c.keepOrdersWhole(sh, byId([L, R, other]));
  assert.deepEqual(plain(sh.placements.map(p => p.id)), [other.id], 'the placed body is lifted with the one that missed'); assert.deepEqual(plain(sh.rejects).sort(), [L.id, R.id].sort());
  // (b) the mate is SAVED (locked: an append) and the late piece missed: the mate is lifted with it, the pair moves whole
  sh = { metal: 'gold', appendOnly: true, nestInitial: [placement(L), placement(other)], placements: [placement(L), placement(other)], rejects: [R.id] };
  c.log.length = 0; c.keepOrdersWhole(sh, byId([L, R, other]));
  assert.deepEqual(plain(sh.placements.map(p => p.id)), [other.id], 'the saved mate is lifted with its late piece: a pair is never left half on a sheet');
  assert.deepEqual(plain(sh.rejects).sort(), [L.id, R.id].sort());
  assert(/saved piece\(s\) of 1 pair\/line/.test(c.log.at(-1).t), 'the log says why a saved piece came off: ' + c.log.at(-1).t);
  // (c) the same without a group (older records, a hand-dropped file): the saved charm stays, as before
  const A = { id: 'A', order: 'o1' }, B = { id: 'B', order: 'o1' };
  sh = { metal: 'gold', appendOnly: true, nestInitial: [placement(A)], placements: [placement(A)], rejects: ['B'] };
  c.keepOrdersWhole(sh, byId([A, B])); assert.deepEqual(plain(sh.placements.map(p => p.id)), ['A'], 'no group: a locked charm is never lifted (unchanged)');
  // (d) another LINE of the same order that is saved stays (only the rejected line's own mates are lifted)
  const X = piece(8300, 1, 1), Y = piece(8300, 2, 1);
  sh = { metal: 'gold', appendOnly: true, nestInitial: [placement(X)], placements: [placement(X)], rejects: [Y.id] };
  c.keepOrdersWhole(sh, byId([X, Y])); assert.deepEqual(plain(sh.placements.map(p => p.id)), [X.id], 'a saved piece of another line of the order is left as it was');
  // (e) Rose Gold protected pieces, a piece a person pinned, and a Merge rerun never lose a saved piece: the line is split and overflow says so
  for (const [name, setup] of [['protected Rose Gold piece', s => { s.roseProtected = { placements: [placement(L)] }; }], ['piece pinned by a person', s => { L.pinned = { cxPt: 1, cyPt: 1, angle: 0 }; }], ['Merge rerun', s => { s._mergeNext = {}; }]]) {
    delete L.pinned; sh = { metal: 'gold', appendOnly: true, nestInitial: [placement(L)], placements: [placement(L)], rejects: [R.id] }; setup(sh);
    c.keepOrdersWhole(sh, byId([L, R])); assert.deepEqual(plain(sh.placements.map(p => p.id)), [L.id], name + ' stays where it is'); delete L.pinned;
  }
  // (f) the run's own pin (arrivalPin) on every saved piece is not a person's pin
  L.pinned = { cxPt: 1, cyPt: 1, angle: 0 }; L.arrivalPin = true;
  sh = { metal: 'gold', appendOnly: true, nestInitial: [placement(L)], placements: [placement(L)], rejects: [R.id] }; c.keepOrdersWhole(sh, byId([L, R]));
  assert.deepEqual(plain(sh.placements), [], 'the run\'s own pin does not hold a saved mate back'); delete L.pinned; delete L.arrivalPin;
  ok.push('keepOrdersWhole: a pair moves whole, saved mate included; protected, pinned and merge-rerun pieces stay; no group, no change');
}

// 6 · overflowToNextSheet: a pair moves whole and says nothing; a pair that had to be split is said, on the card, in the log and in a toast
{
  const mkPages = (extra) => {
    const S = { sheets: { gold: { pages: [] } }, settings: {} }, c = ctxFor(Object.assign({ S, addPage: m => { const pg = { metal: m, page: S.sheets[m].pages.length + 1, charms: [], placements: [], rejects: [], status: 'idle' }; S.sheets[m].pages.push(pg); return pg; } }, extra));
    vm.runInContext(policy + ';this.overflowToNextSheet = overflowToNextSheet; this.orderSummary = orderSummary;', c); return c;
  };
  const L = piece(9100, 1, 1, { side: 'L', name: 'M7134 Left' }), R = piece(9100, 1, 2, { side: 'R', name: 'M7134 Right' }), keep = piece(9200, 1, 1);
  // (a) the whole pair rejected: nothing is split
  let c = mkPages(); let s1 = { metal: 'gold', page: 1, charms: [keep, L, R], placements: [placement(keep)], rejects: [L.id, R.id], status: 'complete', verification: { ok: true } };
  c.S.sheets.gold.pages.push(s1); let next = c.overflowToNextSheet(s1);
  assert.deepEqual(plain(next.charms.map(x => x.id)), [L.id, R.id], 'the pair moves together'); assert.equal(s1.movedOn.split, undefined, 'a pair that moved whole is not a split');
  assert.deepEqual(plain(c.toasts.filter(t => t.k === 'bad')), [], 'no warning');
  assert(/1 pair\(s\)/.test(c.orderSummary([L, R]).text), 'the move says it carried a pair: ' + c.orderSummary([L, R]).text);
  assert.equal(c.orderSummary([L, R]).pairs, 1); assert.equal(c.orderSummary([keep]).pairs, 0); assert.equal(c.orderSummary([keep]).text, '1 order(s)', 'a single keeps its text');
  // (b) only the Right body moved on (its Left is protected on this sheet): split on the card, in the log and in a toast
  c = mkPages(); s1 = { metal: 'gold', page: 1, charms: [keep, L, R], placements: [placement(keep), placement(L)], rejects: [R.id], status: 'complete', verification: { ok: true } };
  c.S.sheets.gold.pages.push(s1); next = c.overflowToNextSheet(s1);
  assert.deepEqual(plain(s1.movedOn.split), [{ group: '9100:1', where: 'Left on sheet 1, Right on sheet 2' }], 'recorded on the sheet\'s moved-on note');
  noNested(plain(s1.movedOn), 'movedOn');
  assert(c.log.some(x => x.k === 'warn' && /Split pair\/line 9100:1/.test(x.t) && /Left on sheet 1, Right on sheet 2/.test(x.t)), 'warned in the log');
  assert(c.toasts.some(t => t.k === 'bad' && /split across sheets/.test(t.t)), 'and in a toast');
  // in Auto the move is routine: the toast is not shown, the log and the card still say it
  c = mkPages(); s1 = { metal: 'gold', page: 1, charms: [keep, L, R], placements: [placement(keep), placement(L)], rejects: [R.id], status: 'complete', verification: { ok: true } };
  c.S.sheets.gold.pages.push(s1); c.overflowToNextSheet(s1, false);
  assert.equal(c.toasts.length, 0, 'announce false: no toast'); assert(s1.movedOn.split.length === 1 && c.log.some(x => x.k === 'warn'));
  // (c) an order that fits no empty sheet stops the sheet and says it is a pair; nothing is split
  const stuck = []; c = mkPages(); c.RunCtl = c.window.RunCtl = { onSheetDone: (sh, e) => stuck.push(e) };
  let s2 = { metal: 'gold', page: 1, charms: [L, R], placements: [], rejects: [L.id, R.id], status: 'partial' }; c.S.sheets.gold.pages.push(s2);
  assert.equal(c.overflowToNextSheet(s2), null); assert(/cannot fit on an empty sheet at these settings\. It holds a pair that must stay on one sheet, so it is not split\./.test(s2.problem), s2.problem);
  assert.equal(s2.charms.length, 2, 'nothing moved'); assert(stuck[0] && stuck[0].sheetPending);
  const D = [1, 2, 3].map(i => piece(9300, 1, i)); c = mkPages(); c.RunCtl = c.window.RunCtl = { onSheetDone() {} };
  s2 = { metal: 'gold', page: 1, charms: D, placements: [], rejects: D.map(x => x.id) }; c.S.sheets.gold.pages.push(s2); c.overflowToNextSheet(s2);
  assert(/a line of 3 pieces that must stay on one sheet/.test(s2.problem), 'a disc necklace of three: ' + s2.problem);
  c = mkPages(); c.RunCtl = c.window.RunCtl = { onSheetDone() {} }; s2 = { metal: 'gold', page: 1, charms: [{ id: 'x', order: 'x' }], placements: [], rejects: ['x'] }; c.S.sheets.gold.pages.push(s2); c.overflowToNextSheet(s2);
  assert.equal(s2.problem, 'The oldest order cannot fit on an empty sheet at these settings. Review its size or the fill ceiling before continuing.', 'a single charm: the words it always had');
  ok.push('overflowToNextSheet: a pair moves whole and quietly; a split is on the card, in the log and in a toast; an order that fits no empty sheet stops, naming the pair or the line');
}

// 7 · sheetFull and the 35-order gap fill count a pair as one order that needs room for both pieces
{
  const S = { settings: { maxFill: .8, clearancePt: 0 } };
  const c = ctxFor({ S, window: {}, SimClock: null });
  vm.runInContext(slice(html, 'function inflatedArea(', 'const TOPUP') + ';' + slice(html, 'const untriedIds', 'function topupSettle(') + ';this.sheetFull = sheetFull;', c);
  const mkc = (id, order, area) => ({ id, order, areaPt2: area, widthPt: 0, heightPt: 0 });
  const usable = 1000, items = [mkc('a', 'o1', 300), mkc('p1', 'pair', 250), mkc('p2', 'pair', 250), mkc('t', 'tiny', 10)];
  const full = (rejects, placed) => c.sheetFull({ rejects, verification: { ok: true }, placements: [{}] }, { endedBy: 'stalled', density: .5, usablePt2: usable, placedPt2: placed, timedOut: false }, items);
  // the pair together is 500, under the ceiling of 800 for an empty sheet: it missed only because the sheet is busy -> the sheet is full (released)
  assert.equal(full(['p1', 'p2'], 300), true, 'a pair that would fit an empty sheet but missed this one makes the sheet full');
  // a pair too big for an empty sheet whole (each piece fits, together they do not) never makes a sheet full: the stop in overflowToNextSheet is the answer
  const big = [mkc('a', 'o1', 300), mkc('p1', 'pair', 450), mkc('p2', 'pair', 450)];
  assert.equal(c.sheetFull({ rejects: ['p1', 'p2'], verification: { ok: true }, placements: [{}] }, { endedBy: 'stalled', density: .5, usablePt2: usable, placedPt2: 300, timedOut: false }, big), false, 'a pair that fits no empty sheet does not make a sheet full');
  ok.push('sheetFull: a pair counts both pieces (a pair that fits an empty sheet makes a full sheet full, one that fits none does not)');
}

// 8 · Pool.attachPool + LiveNest.intakePage: a late piece goes to its mate's sheet when it is open, and is reported when it cannot
{
  const code = slice(bridge, '  function attachPool(', '  /* One placement of a line at a time.');
  const S = { sheets: { gold: { pages: [] } }, settings: {}, cloud: { ok: false } };
  const pages = S.sheets.gold.pages, mkPage = (n, extra) => { const p = Object.assign({ metal: 'gold', page: n, charms: [], placements: [], rejects: [], status: 'complete', runId: 'run-1' }, extra); pages.push(p); return p; };
  const log = [];
  const c = ctxFor({ S, B: { pool: { rows: new Map() } }, update: async () => {}, charmOf: id => pages.flatMap(p => p.charms).find(x => x.poolId === id) || null, settle() {}, sheetDirty() {}, window: { Cancelled: { has: () => false }, CN: null, LiveNest: null },
    pagesOf: m => S.sheets[m].pages, addPage: m => mkPage(S.sheets[m].pages.length + 1), labelOf: m => m, holding: ids => { const out = new Set(); for (const id of ids) for (const p of pages) if (p.charms.some(x => x.poolId === id)) out.add(p); return out; },
    Gate: null, agent: (a, k, t) => log.push({ k, t }), O: {} });
  c.LiveNest = c.window.LiveNest = { closed: p => !!p.closed, intakePage: (m, run, rid) => pages.find(p => p.charms.some(x => String(x.order) === String(rid)) && !p.closed) || pages.at(-1) };
  vm.runInContext(policy + ';' + code + ';this.attachPool = attachPool;', c);
  const row = () => ({ key: 'r', order: { receiptId: '4100' }, spec: { designSku: 'MIS', material: 'gold', quantity: 2 }, state: 'pulled' });
  const A = piece(4100, 1, 1, { side: 'L' }), B = piece(4100, 1, 2, { side: 'R' });
  const pools = [A, B].map(x => ({ poolId: x.poolId }));
  // the pair's Left is on sheet 2 (the order's first sheet is 1: another line of the order), the Right arrives late: it goes to sheet 2 with its mate
  const s1 = mkPage(1), s2 = mkPage(2), other = piece(4100, 2, 1); s1.charms.push(other); s2.charms.push(A);
  c.attachPool(row(), { runId: 'run-1' }, { sp: { designSku: 'MIS', material: 'gold', quantity: 2 }, pools, charms: [B] }, [], []);
  assert(s2.charms.includes(B) && !s1.charms.includes(B), 'the late Right goes to the sheet its Left is on, not the order\'s first sheet');
  assert.equal(log.filter(x => /Split pair/.test(x.t)).length, 0, 'whole: no split to report');
  // the mate's sheet is closed (released full): the late piece goes elsewhere and the split is reported
  const s3 = mkPage(3), C = piece(4400, 1, 1, { side: 'L' }), D = piece(4400, 1, 2, { side: 'R' }); s3.charms.push(C); s3.closed = true;
  log.length = 0; c.attachPool({ key: 'q', order: { receiptId: '4400' }, spec: { designSku: 'MIS', material: 'gold', quantity: 2 }, state: 'pulled' }, { runId: 'run-1' }, { sp: { designSku: 'MIS', material: 'gold', quantity: 2 }, pools: [C, D].map(x => ({ poolId: x.poolId })), charms: [D] }, [], []);
  const landed = pages.find(p => p.charms.includes(D)); assert(landed && landed !== s3, 'a closed sheet takes no more');
  assert(log.some(x => x.k === 'warn' && /Split pair\/line 4400:1/.test(x.t) && /Left on sheet 3, Right on sheet \d/.test(x.t)), 'the split is reported: ' + JSON.stringify(log));
  // a line with no piece anywhere is placed exactly as before (no extra look at the sheets)
  const E = piece(4500, 1, 1); log.length = 0;
  c.attachPool({ key: 'z', order: { receiptId: '4500' }, spec: { designSku: 'X', material: 'gold', quantity: 1 }, state: 'pulled' }, { runId: 'run-1' }, { sp: { designSku: 'X', material: 'gold', quantity: 1 }, pools: [{ poolId: E.poolId }], charms: [E] }, [], []);
  assert(pages.some(p => p.charms.includes(E)) && log.filter(x => x.k === 'warn').length === 0, 'a fresh line: untouched, no warning at all');
  ok.push('attachPool: a late piece goes to its mate\'s open sheet; when the mate\'s sheet is closed the split is reported; a fresh line is placed as before');
}

// 9 · assignCharm: a piece of a pair is never dragged alone to another metal
{
  const S = { unassigned: [], sheets: {}, settings: {} };
  const L = piece(6100, 1, 1, { side: 'L' }), R = piece(6100, 1, 2, { side: 'R' }), solo = { id: 'file:1', order: 'file:1' };
  const pg = { metal: 'gold', charms: [L, R, solo], placements: [], status: 'ready' };
  const c = ctxFor({ S, allSheets: () => [pg], manualSheetClosed: () => false, flushManualIntake() {}, sheetDirty() {}, window: {} });
  vm.runInContext(policy + ';' + slice(html, 'function assignCharm(', 'function removeSource(') + ';this.assignCharm = assignCharm;', c);
  c.assignCharm(L, 'silver');
  assert(c.toasts.some(t => /pair/.test(t.t)) && pg.charms.includes(L) && !L.pendingMetal, 'refused with a plain line, the piece stays');
  c.toasts.length = 0; c.assignCharm(solo, 'silver');
  assert.equal(c.toasts.length, 0, 'a hand-dropped charm moves as before'); assert.equal(solo.pendingMetal, 'silver');
  ok.push('assignCharm: a piece of a pair cannot be moved to another metal alone; a file charm can');
}

// 10 · Release hold: the preview and the release say the pair goes whole, and say plainly when it is split (a piece stays on a sheet already cut)
(async () => {
  const code = fs.readFileSync(path.join(root, 'charm-nest-order-release.js'), 'utf8');
  const cut = { sheetId: 'gold-cut', metal: 'gold', page: 1, roseCutAt: 1, charms: [], placements: [], status: 'complete', runId: 'run-1' };
  const open = { sheetId: 'gold-open', metal: 'gold', page: 2, runId: 'run-1', charms: [{ id: 'z', order: '1', poolId: '1_1_1' }], placements: [], status: 'complete' };
  const G = { CharmNestPair: Pair, B: { run: { runId: 'run-1', status: 'nest', step: 'nest' } }, CharmNestOrders: { stepIndex: s => ({ pool: 2, nest: 3 }[s] ?? -1), FAST_MATERIALS: new Set(['gold', 'silver']) }, document: undefined,
    CN: { allSheets: () => [cut, open], pagesOf: m => [cut, open].filter(p => p.metal === m), METAL_TAG: { gold: 'GF' }, labelOf: m => 'Gold', topupRoom: () => 5 }, Sets: { ofRun: () => [] }, Cancelled: { has: () => false } };
  const row = (key, tx, qty, extra) => Object.assign({ key, order: { receiptId: '7000' }, line: { transactionId: tx, sku: 'MITTENS' }, spec: { designSku: 'MITTENS', material: 'gold', quantity: qty }, hold: 'on hold', state: 'held', problems: [], poolIds: Array.from({ length: qty }, (_, i) => `7000_${tx}_${i + 1}`) }, extra);
  const rows = [row('a', 11, 2)];
  G.Orders = { rows: () => rows };
  const c = { window: G, globalThis: G, console, setTimeout, setInterval: undefined, Promise, Set, Map, Math, JSON, Object, Array, String, Number, Date }; c.self = c;
  vm.createContext(c); vm.runInContext(code, c);
  const OH = G.OrderHold, P = OH._internals;
  // (a) a held pair line whose two pieces were both taken off: whole, no split, the plain wording as before
  let plan = await OH.releasePlan('7000');
  assert.equal(plan.canRelease, true); assert.equal(plan.split, undefined, 'both pieces on hold: nothing is split');
  assert.deepEqual(plain(plan.pieces[0]), { lineKey: 'a', label: 'MITTENS', metal: 'gold', qty: 2 }, 'a normal line\'s piece entry is exactly what it was');
  // (b) a mismatched line (the intake says so): two pieces per unit, Left and Right, one sentence that they go together
  rows[0] = row('a', 11, 1, { spec: { designSku: 'MIS_7134', material: 'gold', quantity: 1, pair: { kind: 'mismatched', mismatched: true }, pieceCount: 2 }, poolIds: ['7000_11_1', '7000_11_2'] });
  plan = await OH.releasePlan('7000');
  assert.deepEqual(plain(plan.pieces[0].sides), ['Left', 'Right']); assert.equal(plan.pieces[0].pieces, 2);
  assert(plan.effects.some(t => /MIS_7134: its 2 pieces \(Left and Right of one pair\) go on the same sheet, together\./.test(t)), JSON.stringify(plan.effects));
  assert(plan.effects.some(t => /^2 pieces go on /.test(t)), 'the sheet sentence counts the two pieces: ' + JSON.stringify(plan.effects));
  // (c) one piece of the pair stays on a sheet already cut: split, said plainly, kept on the plan
  cut.charms.push({ id: 'c1', order: '7000', poolId: '7000_11_1', orderInfo: { receiptId: '7000', transactionId: '11' } });
  plan = await OH.releasePlan('7000');
  assert.equal(plan.split.length, 1); assert.equal(plan.split[0].groupKey, '7000:11'); assert.deepEqual(plain(plan.split[0].stays), ['GF Sheet 1']);
  assert(plan.effects.some(t => /1 piece of MIS_7134 stays on GF Sheet 1, so this pair is on two sheets \(both sheets must go in one set\)\./.test(t)), JSON.stringify(plan.effects));
  assert(plan.stays.length === 1, 'the existing stays line is still there');
  ok.push('Release hold preview: a mismatched pair is two pieces going on one sheet together; a piece that stays on a cut sheet is said plainly as a split; ordinary lines read as before');
})().then(() => {
  console.log('Pairs flow OK:\n  ' + ok.join('\n  '));
}).catch(e => { console.error(e); process.exit(1); });
