// PAIRPARTIAL: pairs and groups on partial sheets and leftovers (Paul, 9 Oct 2026: a mismatched pair, a matching pair or a multi-piece order such as a disc necklace is tracked
// everywhere; a pair needs TWO places and is never split across sheets, R3). OFFLINE. Needs jsdom for part B (NODE_PATH=<a folder with node_modules/jsdom>); parts A and C run without it.
//   A  the pure module: estimate counts pieces AND pairs, the plan places whole groups, the shared words
//   B  Use this one: a trial that places one piece of a pair, the cut between orders, the best fit, the guard, a pair already split over two sheets
//   C  the server: the Cut Sheet record keeps which pieces of which group were cut, the leftover keeps the numbers, an automatic claim skips a leftover that cannot hold a pair, Remove reports a half-removed pair
//   node tests/charm-nest/pairs-partial.cjs
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const R = require('../../charm-nest-rose'), Solver = require('../../charm-nest-solver'), P = require('../../charm-nest-partial'), Readiness = require('../../charm-nest-readiness');
const Remnants = require('../../netlify/functions/_charmNestRemnants'), RoseStock = require('../../netlify/functions/_charmNestRoseStock');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { JSDOM = null; }
const MM = 25.4 / 72, SC = 6, W = 150, H = 60;   // a 150 x 60 pt sheet; pieces are 24 x 16 pt blocks
const block = id => { const w = 24 * SC, h = 16 * SC, bits = new Uint8Array(w * h).fill(1); return { id, order: id, w, h, scale: SC, bits, areaPt2: bits.length / (SC * SC), outline: { subpaths: [[['m', [0, 0]], ['l', [24, 0]], ['l', [24, 16]], ['l', [0, 16]], ['h']]] }, centerPt: [12, 8], members: [] }; };
// stock left of x = cut is already gone, on every row: what is left is the strip from cut to the sheet's right edge
const stockOf = (id, cut, rev = 1) => ({ id, metal: 'gold14k', wPt: W, hPt: H, revision: rev, owner: null, available: true, profileJson: JSON.stringify({ version: 1, wPt: W, hPt: H, axis: 'x', step: .5, values: Array(H / .5).fill(cut) }) });
const card = s => ({ id: s.id + '-' + s.revision, stockId: s.id, revision: s.revision, metal: s.metal, status: 'available', sheetWMm: W * MM, sheetHMm: H * MM, bboxMm: { x: s.cut * MM, y: 0, w: (W - s.cut) * MM, h: H * MM }, areaMm2: (W - s.cut) * H * MM * MM, outline: null });
const wait = ms => new Promise(r => setTimeout(r, ms));

function world(metal, policy) {
  const dom = new JSDOM('<body></body>', { url: 'https://example.test', runScripts: 'outside-only' }), w = dom.window;
  w.IntersectionObserver = class { observe() { } unobserve() { } };
  const log = { api: [], started: [], dirty: [], toasts: [], stocks: [], agent: [], kept: [] }, docs = new Map(), cuts = new Map();
  const add = (id, cut, rev) => { const s = stockOf(id, cut, rev); docs.set(id, s); cuts.set(id, cut); return card({ ...s, cut }); };
  const page = n => ({ metal, page: n, charms: [], placements: [], rejects: [], status: 'idle', persisted: false, persistedDone: true });
  const sh = page(1), S = { sheets: { [metal]: { pages: [sh], active: 0 } }, settings: { maxFill: .8, clearancePt: -.5, insetPt: 1.5, stock: {} }, cloud: { ok: true } };
  let cards = [], pol = policy || { mode: 'auto', wMm: 100, hMm: 50 };
  const api = async (name, b) => {
    log.api.push(b); const d = docs.get(b.stockId);
    if (b.op === 'roseGet') { if (!d) throw new Error('Sheet not found'); return { stock: { ...d }, cuts: x.history[b.stockId] || [], more: false }; }
    if (b.op === 'roseRelease') {
      const own = [...docs.values()].find(x => x.id === b.stockId); if (!own || own.owner !== b.sheetId) throw new Error('This stock reservation changed');
      if (!own.revision && !own.profileJson) docs.delete(own.id); else { own.owner = null; own.available = true; } return { ok: true };
    }
    if (b.op === 'roseClaim') {
      const held = [...docs.values()].find(x => x.owner === b.sheetId);
      if (b.exact && held && held.id !== b.stockId && !b.swap) throw new Error('This sheet already holds another physical sheet. Give it back before choosing a partial sheet');
      let st = b.exact ? docs.get(b.stockId) : held || (b.stockId ? docs.get(b.stockId) : null);
      if (b.exact && !st) throw new Error('Partial sheet not found');
      if (!st && !b.fresh && !b.stockId) st = [...docs.values()].find(x => x.available && x.metal === b.metal && Math.abs(x.wPt - b.wPt) < .01 && Math.abs(x.hPt - b.hPt) < .01 && !x.owner);
      if (!st && b.onlyRemnant) return { stock: null, protectedJson: null };
      if (!st) { st = { id: 'rgs-new-' + docs.size, metal: b.metal, wPt: b.wPt, hPt: b.hPt, revision: 0, profileJson: null, owner: null, available: true }; docs.set(st.id, st); }
      if (st.owner && st.owner !== b.sheetId) throw new Error('This ' + b.metal + ' sheet is reserved for another layout');
      if (b.revision != null && st.revision !== b.revision) throw new Error('This remnant changed. Reload its history before nesting');
      // swap: what the sheet holds goes back in the same call, after every check has passed (a refused claim loses nothing)
      if (b.swap && held && held.id !== st.id) { if (!held.revision && !held.profileJson) docs.delete(held.id); else { held.owner = null; held.available = true; } }
      st.owner = b.sheetId; st.available = false; return { stock: { ...st }, protectedJson: null, ...(x.aside ? { setAside: x.aside } : {}) };   // (aside: what the server did with a recorded cut it set aside)
    }
    return {};
  };
  const pieces = (n, tag = 'p') => Array.from({ length: n }, (_, i) => block(tag + i));
  w.CharmNestRose = R; w.CharmNestPartial = P; w.CharmNestSolver = Solver; w.Sets = { ofRun: () => [] };
  w.PartialSheets = { list: async () => ({ items: cards.filter(c => c.status === 'available' && (x.stale || !docs.get(c.stockId).owner)) }), policy: () => pol, changed() { },
    stocks: async ids => { log.stocks.push(ids.slice()); return Object.fromEntries(ids.map(id => { const m = /^(.+)-(\d+)$/.exec(id), d = m && docs.get(m[1]); return [id, d ? { stockId: d.id, revision: d.revision, wPt: d.wPt, hPt: d.hPt, profileJson: d.profileJson, metal: d.metal, current: d.revision === +m[2] } : { missing: true }]; })); } };
  const C = w.CN = {
    S, esc: s => String(s), uid: () => 'u', agent(...a) { log.agent.push(a); }, toast: (m, k) => log.toasts.push(m), inflatedArea: c => c.areaPt2, activeCharms: s => s.charms.filter(c => !c.excluded),
    stockFor: (m, s) => { const k = s && (s.roseStock || s.recalled && s.recalled.stock || s.keptStock || s.newStock); return k && k.wPt ? { wPt: k.wPt, hPt: k.hPt } : { wPt: W, hPt: H }; },
    pagesOf: m => S.sheets[m].pages, allSheets: () => Object.values(S.sheets).flatMap(x => x.pages), renderCard() { }, drawPreview() { }, api,
    addPage: m => { const p = page(Math.max(...S.sheets[m].pages.map(q => q.page)) + 1); S.sheets[m].pages.push(p); return p; },
    sheetDirty: s => { if (s.roseCutAt) return; /* (as the page's own: a sheet with a recorded cut is not marked changed) */ log.dirty.push(s); s.dirty = true; s.status = 'ready'; s.placements = []; s.rejects = []; s.releaseFull = false; delete s.rosePlan; },
    startNest: s => { log.started.push({ sheet: s, byHand: !!s._byHand }); },
    buildJob: s => { const st = C.stockFor(s.metal, s), items = C.activeCharms(s); return { sheet: { wPt: st.wPt, hPt: st.hPt, insetPt: 1.5, ...(s.roseStock && s.roseStock.profileJson ? { remnant: JSON.parse(s.roseStock.profileJson) } : {}) }, clearancePt: -.5, angles: [0, 90], fineRes: 2, coarseRes: .5, timeBudgetMs: 3000, maxFill: .8, maxTrials: 60, seed: 1, careful: true, block: true, pieces: items.map(c => ({ id: c.id, w: c.w, h: c.h, scale: c.scale, bits: c.bits, areaPt2: c.areaPt2, order: c.order || c.id, orderDate: 0, pinned: c.pinned || null })) }; },
    solverWorker: () => { const k = { onmessage: null, dead: false, terminate() { k.dead = true; }, postMessage(m) { if (m.type !== 'solve') return; Solver.solve(m.job, {}).then(r => { if (!k.dead && k.onmessage) k.onmessage({ data: { type: 'done', jobId: m.jobId, result: Solver.publicLayout(r) } }); }).catch(e => k.onmessage && k.onmessage({ data: { type: 'error', jobId: m.jobId, message: e.message } })); } }; return k; }
  };
  w.eval(`window.Session={schedule(){}};`);
  w.eval(fs.readFileSync('charm-nest-rose-ui.js', 'utf8'));
  w.eval(fs.readFileSync('charm-nest-partial-nest.js', 'utf8'));
  const x = { stale: false, history: {}, aside: null, w, C, sh, log, docs, S, add, pieces, setCards: c => { cards = c; }, setPolicy: p => { pol = p; }, page };
  return x;
}
const ids = list => list.map(c => c.id).sort().join();

// ── part A helpers: leftovers of known shape (a 100 x 50 pt stock cut from the left up to `cut`; what is left is the strip from cut to the right edge) ──
const T = { areaMm2: 61, minMm: 6.7, maxMm: 10.4 };
const stripOf = cut => { const prof = { version: 1, wPt: 100, hPt: 50, axis: 'x', step: .5, values: Array(100).fill(cut) }, g = Remnants.leftover(prof); return { prof, g, card: id => ({ id, status: 'available', outline: g.rings, sheetWMm: g.sheetWMm, sheetHMm: g.sheetHMm, lastUsedAt: 0 }) }; };
const est = cut => { const s = stripOf(cut); return P.estimateFit(s.g.rings, T, { sheetWMm: s.g.sheetWMm, sheetHMm: s.g.sheetHMm }); };
const pl = (id, rid, tid, copy, extra) => ({ id, order: String(rid), poolId: `${rid}_${tid}_${copy}`, areaMm2: 61, ...extra });

function partA() {
  // A1. the estimate counts pieces AND pairs: a pair needs two places, so 3 pieces hold 1 pair, never 1.5; 1 piece holds no pair
  const e10 = est(10), e50 = est(50), e70 = est(70), e92 = est(92);
  assert.deepEqual([e10.pieces, e10.pairs, e10.pairsLow, e10.pairsHigh], [6, 3, 2, 3], 'six pieces, three pairs');
  assert.deepEqual([e50.pieces, e50.pairs, e50.pairsHigh], [3, 1, 1], 'three pieces hold one pair');
  assert.deepEqual([e70.pieces, e70.high, e70.pairs, e70.pairsHigh], [1, 1, 0, 0], 'a leftover for ONE piece holds no pair at all');
  assert.deepEqual([e92.pieces, e92.pairs, e92.pairsLow, e92.pairsHigh], [0, 0, 0, 0]);
  for (const cut of [0, 10, 30, 50, 60, 70, 80, 92]) { const e = est(cut); assert.equal(e.pairs, Math.floor(e.pieces / 2)); assert.equal(e.pairsLow, Math.floor(e.low / 2)); assert.equal(e.pairsHigh, Math.floor(e.high / 2)); assert(e.pairsLow <= e.pairs && e.pairs <= e.pairsHigh, 'low <= pairs <= high at ' + cut); }
  assert.deepEqual(P.estimateFit([], T, {}), { pieces: 0, low: 0, high: 0, pairs: 0, pairsLow: 0, pairsHigh: 0, packedPct: 0, usableMm2: 0, packMm2: 0 }, 'nothing left, no pairs');

  // A2. a group is one order line: the contract's key when the page has it, else the line's own receipt:transaction (orderInfo, then the pool id), else the order
  assert.equal(P.groupKeyOf({ groupKey: '1:2', groupSize: 2 }), '1:2'); assert.equal(P.groupKeyOf({ groupKey: '1:2' }), '', 'a key with no group size is a plain piece');
  assert.equal(P.groupKeyOf({ orderInfo: { receiptId: 1001, transactionId: 5001 }, order: 'x', groupSize: 2 }), '1001:5001');
  assert.equal(P.groupKeyOf({ poolId: '1001_5001_2', order: '1001', groupSize: 2 }), '1001:5001', 'a saved sheet keeps order, poolId and the group size: the line is read from the pool id');
  assert.equal(P.groupKeyOf({ poolId: '1001_5001_1', groupSize: 2 }), P.groupKeyOf({ poolId: '1001_5001_2', groupSize: 2 }), 'both pieces of a pair are one group');
  assert.notEqual(P.groupKeyOf({ poolId: '1001_5001_1', groupSize: 2 }), P.groupKeyOf({ poolId: '1001_5002_1', groupSize: 2 }), 'two lines of one receipt are two groups');
  // only a GROUP has a key (Paul 9 Oct, ruling after ADVCOMPAT 2): the copies of a plain quantity-N line, a piece alone and a record with no group fields each stand alone
  assert.equal(P.groupKeyOf({ poolId: '1001_5001_2', order: '1001' }), '', 'a plain copy has no group');
  assert.equal(P.groupKeyOf({ orderInfo: { receiptId: 1001, transactionId: 5001 }, order: 'x' }), '', 'a plain copy read from its order fields has none either');
  assert.equal(P.groupKeyOf({ poolId: '1001_5001_1', groupSize: 1, side: 'L' }), '', 'a single earring that names its ear is a group of one');
  assert.equal(P.groupKeyOf({ poolId: '1001_5001_1', side: 'R' }), '1001:5001', 'an older sided piece with no size is the half of a pair');
  assert.equal(P.groupKeyOf({ order: 'A', id: 'z', groupSize: 2 }), 'A'); assert.equal(P.groupKeyOf({ id: 'z', groupSize: 2 }), 'z'); assert.equal(P.groupKeyOf(null), '');
  global.self = { CharmNestPair: { groupKey: p => 'pair:' + p.id } };
  try { assert.equal(P.groupKeyOf({ id: 'q', groupSize: 2 }), 'pair:q', 'the shared module wins when it is there'); global.self.CharmNestPair.groupKey = () => ':'; assert.equal(P.groupKeyOf({ order: 'A', id: 'z', groupSize: 2 }), 'A', 'its "no key" (a bare colon) is no key: the order stands in'); global.self.CharmNestPair.groupKey = () => 'undefined:undefined'; assert.equal(P.groupKeyOf({ poolId: '7_8_1', groupSize: 2 }), '7:8', 'a key that is not usable falls back to the local reading'); global.self.CharmNestPair.groupKey = () => { throw new Error('boom'); }; assert.equal(P.groupKeyOf({ poolId: '7_8_1', groupSize: 2 }), '7:8', 'and so does a module that throws'); } finally { delete global.self; }

  // A3. what a group is, read from its pieces
  assert.equal(P.kindOfGroup([pl('a', 1, 1, 1)]), 'single');
  assert.equal(P.kindOfGroup([pl('a', 1, 1, 1), pl('b', 1, 1, 2)]), 'pair', 'two pieces of one line, no form named: a pair');
  assert.equal(P.kindOfGroup([pl('a', 1, 1, 1, { orderInfo: { form: 'Huggie Hoop' } }), pl('b', 1, 1, 2, { orderInfo: { form: 'Huggie Hoop' } })]), 'pair');
  assert.equal(P.kindOfGroup([pl('a', 1, 1, 1, { orderInfo: { form: 'Pendant' } }), pl('b', 1, 1, 2, { orderInfo: { form: 'Pendant' } })]), 'multi', 'two pendants are a 2-piece order, not an earring pair');
  // Amendment 2 (Paul, 9 Oct): EVERY earring pair is one Left and one Right, matching or mismatched, so a side is no sign of "mismatched"; two different bodies are
  assert.equal(P.kindOfGroup([pl('a', 1, 1, 1, { side: 'L', bodyIndex: 0 }), pl('b', 1, 1, 2, { side: 'R', bodyIndex: 0 })]), 'pair', 'a matching pair: a Left and a Right of the same body');
  assert.equal(P.kindOfGroup([pl('a', 1, 1, 1, { side: 'L', bodyIndex: 0 }), pl('b', 1, 1, 2, { side: 'R', bodyIndex: 1 })]), 'mismatched', 'a mismatched pair: two different bodies');
  assert.equal(P.kindOfGroup([pl('a', 1, 1, 1, { side: 'L' }), pl('b', 1, 1, 2, { side: 'R' }), pl('c', 1, 1, 3, { side: 'L' }), pl('d', 1, 1, 4, { side: 'R' })]), 'pair', 'a quantity-2 earring line: 2 Left + 2 Right, two pairs of one group');
  assert.equal(P.pairsIn([pl('a', 1, 1, 1, { side: 'L' }), pl('b', 1, 1, 2, { side: 'R' }), pl('c', 1, 1, 3, { side: 'L' }), pl('d', 1, 1, 4, { side: 'R' })]), 2);
  assert.equal(P.pairsIn([pl('a', 1, 1, 1, { side: 'L' })]), 0, 'a Left whose Right is elsewhere is not a pair here'); assert.equal(P.pairsIn([pl('a', 1, 1, 1), pl('b', 1, 1, 2)]), 1, 'an older record with no sides: 2 pieces of one line are a pair');
  assert.equal(P.kindOfGroup([1, 2, 3, 4].map(i => pl('d' + i, 1, 1, i))), 'multi', 'a disc necklace is one order of several pieces');

  // A4. the words: exactly "N pieces" while nothing is in a group, so every old line stays as it was
  const six = [pl('a0', 1, 1, 1, { side: 'L', bodyIndex: 0 }), pl('a1', 1, 1, 2, { side: 'R', bodyIndex: 0 }), pl('b0', 2, 2, 1, { side: 'L', bodyIndex: 0 }), pl('b1', 2, 2, 2, { side: 'R', bodyIndex: 1 }), pl('c0', 3, 3, 1), pl('d0', 4, 4, 1)];
  assert.deepEqual(P.describe(six), { pieces: 6, groups: 4, singles: 2, pairs: 2, mismatched: 1, multi: 0, minGroup: 1, maxGroup: 2 });
  assert.equal(P.pieceWords(six), '6 pieces (2 pairs and 2 single pieces)');
  assert.equal(P.pieceWords([pl('a0', 1, 1, 1), pl('c0', 3, 3, 1)]), '2 pieces'); assert.equal(P.pieceWords([pl('a0', 1, 1, 1)]), '1 piece'); assert.equal(P.pieceWords([]), '0 pieces');
  assert.equal(P.pieceWords([pl('a0', 1, 1, 1, { groupSize: 2 }), pl('a1', 1, 1, 2, { groupSize: 2 })]), '2 pieces (1 pair)');
  assert.equal(P.pieceWords([pl('a0', 1, 1, 1), pl('a1', 1, 1, 2)]), '2 pieces', 'two copies of a PLAIN quantity-2 line are two single pieces, not a pair (Paul 9 Oct, ADVCOMPAT 2)');
  assert.equal(P.describe([pl('a0', 1, 1, 1), pl('a1', 1, 1, 2), pl('a2', 1, 1, 3)]).singles, 3, 'three plain copies: three single pieces');
  const q2 = [1, 2, 3, 4].map(i => pl('e' + i, 8, 8, i, { side: i % 2 ? 'L' : 'R' }));
  assert.equal(P.pieceWords(q2), '4 pieces (2 pairs)', 'a quantity-2 earring line is two pairs, not "an order of 4 pieces"'); assert.equal(P.describe(q2).pairs, 2);
  assert.equal(P.pieceWords([pl('a0', 1, 1, 1, { side: 'L' }), pl('s', 3, 3, 1)]), '2 pieces', 'a Left whose partner is on another sheet is a single piece here: nothing to say about pairs'); assert.equal(P.describe([pl('a0', 1, 1, 1, { side: 'L' })]).singles, 1);
  assert.equal(P.pieceWords([1, 2, 3, 4, 5].map(i => pl('d' + i, 9, 9, i, { groupSize: 5 })).concat(pl('s', 3, 3, 1))), '6 pieces (1 order of 3 or more pieces and 1 single piece)');

  // A5. the plan places WHOLE groups: a card takes a pair or leaves it for the next card, never half of it
  const big = stripOf(10).card('big'), mid = stripOf(50).card('mid'), one = stripOf(70).card('one'), cards = [big, mid, one];
  const three = ['g1', 'g2', 'g3'].flatMap(g => [{ areaMm2: 61, group: g }, { areaMm2: 61, group: g }]);
  const smallest = P.planFor(cards, three, { typical: T, order: 'smallest' });
  assert.equal(smallest.fitsAll, true); assert.deepEqual([smallest.groups, smallest.pairs], [3, 3]);
  assert.deepEqual(smallest.partials.map(p => [p.id, p.pieces, p.groups, p.pairs]), [['mid', 2, 1, 1], ['big', 4, 2, 2]], 'the one-piece leftover cannot take a pair; the three-piece one takes one pair (3 places, a second pair would need 4)');
  assert.deepEqual(smallest.partials.map(p => p.keys), [['g1'], ['g2', 'g3']]); assert.deepEqual(smallest.needed, ['mid', 'big']);
  for (const p of smallest.partials) assert.equal(p.pieces % 2, 0, 'every card holds an even number of pieces: no pair is cut in half');
  assert.deepEqual(smallest.short, { mm2: 0, pieces: 0, pairs: 0, groups: [], unfit: [] });
  const largest = P.planFor(cards, three, { typical: T, order: 'largest' });
  assert.deepEqual(largest.partials.map(p => [p.id, p.groups]), [['big', 3]], 'largest first: all three pairs on the biggest');
  // a pair is not placed on the one-piece leftover even when the metal would fit by area; a single piece is
  const lone = P.planFor([one], [{ areaMm2: 61, group: 's' }], { typical: T }); assert.equal(lone.fitsAll, true); assert.equal(lone.partials[0].id, 'one');
  const pairOnOne = P.planFor([one], [{ areaMm2: 61, group: 'g' }, { areaMm2: 61, group: 'g' }], { typical: T }); assert.equal(pairOnOne.fitsAll, false); assert.deepEqual(pairOnOne.short.unfit, ['g'], 'a pair that no leftover can hold at all is named');
  assert.deepEqual([pairOnOne.short.pieces, pairOnOne.short.pairs], [2, 1]);
  // a disc necklace (4 pieces) with only three-piece leftovers: it fits none, and the plan says which
  const disc = P.planFor([mid, mid].map((c, i) => ({ ...c, id: 'm' + i })), [1, 2, 3, 4].map(() => ({ areaMm2: 61, group: 'disc' })), { typical: T });
  assert.equal(disc.fitsAll, false); assert.deepEqual(disc.short.unfit, ['disc']); assert.equal(disc.short.pieces, 4); assert.equal(disc.partials.length, 0, 'nothing is half-placed');
  // a quantity-2 earring line (L R L R) is one group of 4 places: the three-piece leftover cannot take it, the six-piece one can, and it counts as two pairs
  const q2g = [0, 1, 2, 3].map(i => ({ areaMm2: 61, group: 'q', side: i % 2 ? 'R' : 'L' })), q2p = P.planFor([mid, big], q2g, { typical: T, order: 'smallest' });
  assert.equal(q2p.fitsAll, true); assert.deepEqual(q2p.partials.map(p => [p.id, p.pieces, p.groups, p.pairs]), [['big', 4, 1, 2]], 'on the six-piece leftover, whole; 2 pairs'); assert.equal(q2p.pairs, 2);
  // a single and a pair share the three-piece leftover (1 + 2 = 3 places)
  const mixed = P.planFor([mid], [{ areaMm2: 61, group: 's' }, { areaMm2: 61, group: 'g' }, { areaMm2: 61, group: 'g' }], { typical: T }); assert.equal(mixed.fitsAll, true); assert.deepEqual([mixed.partials[0].pieces, mixed.partials[0].pairs], [3, 1]);
  // no group on any piece: the plan is exactly what it always was (no group fields)
  const plain = P.planFor(cards, [{ areaMm2: 61 }, { areaMm2: 61 }, { areaMm2: 61 }], { typical: T });
  assert.equal(plain.groups, undefined); assert.equal(plain.pairs, undefined); assert.equal(plain.short.groups, undefined); assert.equal(plain.fitsAll, true);
  // a total or a count (no pieces to read groups from) is as before
  assert.equal(P.planFor(cards, { areaMm2: 300, count: 5 }, { typical: T }).groups, undefined);
}


// ── part B: Use this one (the page's PartialNest) ──
const plain = v => (v === undefined ? v : JSON.parse(JSON.stringify(v)));   // (values made inside the jsdom window have that window's prototypes)
const rectMm = (x0, y0, x1, y1) => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]];
const grp = (id, rid, tid, copy, extra) => ({ ...block(id), order: String(rid), poolId: `${rid}_${tid}_${copy}`, ...extra });
// a leftover card with its outline (so the estimate has something to read): the strip from `cut` to the sheet's right edge
const outlined = (x, id, cut) => { const c = x.add(id, cut); return { ...c, outline: [rectMm(cut * MM, 0, W * MM, H * MM)], sheetWMm: W * MM, sheetHMm: H * MM }; };
// a trial pack that places exactly the pieces named (the real solver would place what fits; here the test says what fit)
const canned = (x, idsToPlace) => {
  x.C.solverWorker = () => { const k = { onmessage: null, terminate() { }, postMessage(m) { if (m.type !== 'solve') return; const set = new Set(idsToPlace), placements = m.job.pieces.filter(p => set.has(p.id)).map((p, i) => ({ id: p.id, cxPt: 10 + i * 30, cyPt: 20, angle: 0 })); Promise.resolve().then(() => k.onmessage({ data: { type: 'done', jobId: m.jobId, result: { placements, density: .4 } } })); } }; return k; };
};
const seated = (x, charms) => { const sh = x.sh; sh.charms = charms; sh.placements = []; sh.status = 'complete'; sh.sheetId = 'g14-s1'; return sh; };

async function partB() {
  // B1. the trial placed ONE piece of a pair: the pair stays whole and moves on, and the answer says so plainly
  {
    const x = world('gold14k'), { w } = x, PN = w.PartialNest, p = outlined(x, 'rgs-p', 100); x.setCards([p]);
    const sh = seated(x, [grp('a0', 1001, 5001, 1, { side: 'L' }), grp('a1', 1001, 5001, 2, { side: 'R' }), grp('b0', 1002, 5002, 1, { side: 'L' }), grp('b1', 1002, 5002, 2, { side: 'R' }), grp('s0', 1003, 5003, 1)]);
    canned(x, ['a0', 'a1', 'b0', 's0']);   // the second pair got one place only (its Left)
    const pv = await PN.preview(sh, [p.id], { maxMs: 1500 });
    assert(pv.ok && !pv.fitsAll && pv.links.length === 1, JSON.stringify({ ...pv, links: pv.links.map(l => ({ ...l, placements: undefined, outline: undefined })) }));
    assert.deepEqual(plain(pv.links[0].pieceIds).sort(), ['a0', 'a1', 's0'], 'the whole pair and the single went onto the leftover; the half pair did NOT');
    assert.equal(pv.links[0].placed, 3); assert.equal(pv.continues.n, 2); assert.deepEqual(plain(pv.continues.ids).sort(), ['b0', 'b1'], 'both pieces of the pair move on together');
    assert.deepEqual(plain(pv.splits), [{ order: '1002', placed: 1, of: 2, side: 'Left' }]);
    assert.equal(pv.splitWords, 'Only 1 of the 2 pieces of order 1002 fits on it (the Left one; the Right one does not), and a pair is never split across sheets, so the order moves on whole.');
    assert.equal(pv.shared, undefined, 'nothing of these orders is on another sheet');
    // the panel's face gets the same words
    const pe = await w.PartialEngine.preview(sh, p.id); assert.equal(pe.splitWords, pv.splitWords);
  }

  // B2. the trial placed one piece of the ONLY pair: nothing is placed, and it is said (not the old "No piece fits on it."). Every earring piece has its side, matching pair or mismatched;
  //     the words name the one that fit. An older record with no sides says the same without the side.
  for (const [mism, sided, fitIds] of [[false, true, ['b0']], [true, true, ['b1']], [false, false, ['b0']]]) {
    const x = world('gold14k'), { w, log, docs } = x, PN = w.PartialNest, p = outlined(x, 'rgs-p', 100); x.setCards([p]);
    const sh = seated(x, [grp('b0', 1002, 5002, 1, sided ? { side: 'L', bodyIndex: 0 } : {}), grp('b1', 1002, 5002, 2, sided ? { side: 'R', bodyIndex: mism ? 1 : 0 } : {})]);
    canned(x, fitIds);
    const pv = await PN.preview(sh, [p.id], { maxMs: 1500 }), tag = (mism ? 'a mismatched pair' : sided ? 'a matching pair' : 'an older pair with no sides') + ', ' + fitIds[0] + ' fits';
    assert.equal(w.CharmNestPartial.describe(sh.charms).mismatched, mism ? 1 : 0, tag);
    assert(pv.ok && !pv.fitsAll && pv.links.length === 0, tag + ': ' + JSON.stringify({ ...pv, links: [] }));
    assert.equal(pv.skipped.length, 1);
    const which = !sided ? '' : fitIds[0] === 'b0' ? ' \\(the Left one; the Right one does not\\)' : ' \\(the Right one; the Left one does not\\)';
    assert.match(pv.skipped[0].why, new RegExp('^Only 1 of the 2 pieces of order 1002 fits on it' + which + ', and a pair is never split across sheets'), tag);
    assert.equal(pv.words, 'None of the 2 pieces (1 pair) fit on the partial sheet you chose. ' + pv.splitWords, tag); assert.equal(pv.continues.n, 2);
    assert.equal(log.api.length, 0, 'a preview calls nothing'); assert.equal(docs.get('rgs-p').owner, null, 'and claims nothing');
  }

  // B3. the head of the trial (the pieces the trial pack is given) is cut between ORDERS, never inside a pair
  {
    const x = world('gold14k'), { w } = x, PN = w.PartialNest, p = outlined(x, 'rgs-p', 100); x.setCards([p]);
    const stock = x.docs.get('rgs-p'), usable = Solver.makeSheetGrid({ wPt: stock.wPt, hPt: stock.hPt, insetPt: 1.5, remnant: JSON.parse(stock.profileJson) }, -.5, 2).usableCells / 4;
    const per = 24 * 16, oldN = Math.ceil(usable * 1.5 / per);   // (the old rule: stop at the piece where the running area passes 1.5 x the room)
    const lead = oldN % 2 === 0 ? 1 : 0;   // (a single first, when that is what puts the old cut in the middle of a pair)
    const list = []; for (let i = 0; i < lead; i++) list.push(grp('s' + i, 900 + i, 1, 1));
    for (let k = 0; list.length < 60; k++) list.push(grp(`p${k}a`, 2000 + k, 1, 1), grp(`p${k}b`, 2000 + k, 1, 2));
    assert(oldN >= 4 && oldN < 50, 'the scenario needs a head that is shorter than the list: ' + oldN);
    const oldHead = list.slice(0, oldN), oldOrders = new Map(); for (const c of oldHead) oldOrders.set(c.order, (oldOrders.get(c.order) || 0) + 1);
    assert([...oldOrders.values()].some(n => n === 1) && list.filter(c => c.order === [...oldOrders].find(([, n]) => n === 1)[0]).length === 2, 'the old rule would have cut a pair in half here');
    const head = PN._headFor(stock, list, p), by = new Map(); for (const c of head) by.set(c.order, (by.get(c.order) || 0) + 1);
    assert(head.length >= oldN && head.length < list.length, 'the head is still a head: ' + head.length + ' of ' + list.length);
    assert([...by.values()].every((n, i) => n === (i === 0 && lead ? 1 : 2)), 'every order in the head has all its pieces');
    assert.deepEqual(plain(head.map(c => c.id)), list.slice(0, head.length).map(c => c.id), 'in the list\'s own order');
    // a pair that has to move on whole is the whole point: _wholeOrders judges against EVERYTHING still to place, not against the head
    const rest = [grp('x0', 1, 1, 1), grp('x1', 1, 1, 2), grp('y0', 2, 1, 1), grp('y1', 2, 1, 2)];
    assert.deepEqual(plain([...PN._wholeOrders(rest, new Set(['x0', 'x1', 'y0']))].sort()), ['x0', 'x1']);
    assert.deepEqual(plain([...PN._wholeOrders(rest.slice(0, 3), new Set(['x0', 'x1', 'y0']))].sort()), ['x0', 'x1', 'y0'], '(the same trial, when the second pair was not part of what was still to place at all)');
    assert.deepEqual(plain([...PN._wholeOrders(rest, new Set(['x0', 'x1']))].sort()), ['x0', 'x1'], 'the head cut off the second pair: the first pair is whole and the cut one is not hurt');
  }

  // B4. the best leftover for pieces that include pairs: one that cannot hold a pair is skipped; for single pieces nothing changed
  {
    const x = world('gold14k'), { w } = x, PN = w.PartialNest, tiny = outlined(x, 'rgs-tiny', 135), roomy = outlined(x, 'rgs-roomy', 20);
    const pieces = [grp('a0', 1, 1, 1), grp('a1', 1, 1, 2), grp('b0', 2, 1, 1), grp('b1', 2, 1, 2)], singles = [grp('s0', 11, 1, 1), grp('s1', 12, 1, 1), grp('s2', 13, 1, 1), grp('s3', 14, 1, 1)];
    const et = PN._estimateOf(tiny, pieces), er = PN._estimateOf(roomy, pieces);
    assert(et && er && et.high < 2 && et.high >= 0 && er.pieces >= 4, 'the scenario: the tiny leftover holds under 2 pieces, the roomy one holds the lot ' + JSON.stringify([et && et.high, er && er.pieces]));
    assert.equal(PN._bestFit([tiny, roomy], pieces, new Set()).id, roomy.id, 'pairs: the leftover that can hold a pair');
    assert.equal(PN._bestFit([tiny], pieces, new Set()), null, 'pairs and only a leftover that cannot hold even one: none (it would only strand a piece)');
    assert.equal(PN._bestFit([tiny, roomy], pieces, new Set([roomy.id])), null, 'the roomy one already used: none, not the tiny one');
    // singles: as before (the tight fit that takes everything, else the biggest)
    const as = PN._bestFit([tiny, roomy], singles, new Set()); assert(as && as.id === roomy.id);
    // a disc necklace: the leftover must be able to hold the whole order
    const disc = [grp('d0', 7, 1, 1), grp('d1', 7, 1, 2), grp('d2', 7, 1, 3), grp('d3', 7, 1, 4), grp('d4', 7, 1, 5), grp('d5', 7, 1, 6), grp('d6', 7, 1, 7), grp('d7', 7, 1, 8), grp('d8', 7, 1, 9)];
    const four = outlined(x, 'rgs-four', 85), ed = PN._estimateOf(four, disc);
    assert(ed.pieces < 9 && ed.high >= 2, 'the scenario: a leftover with room for part of the disc necklace only: ' + JSON.stringify(ed));
    assert.equal(PN._bestFit([four, roomy], disc, new Set()).id, roomy.id, 'the leftover that can take the whole disc necklace is chosen over the one that takes part of it');
  }

  // B5. Use this one on a leftover that cannot hold a pair: refused before anything is claimed, in plain words
  {
    const x = world('gold14k'), { w, log, docs } = x, PN = w.PartialNest, tiny = outlined(x, 'rgs-tiny', 135); x.setCards([tiny]);
    const sh = seated(x, [grp('a0', 1, 1, 1), grp('a1', 1, 1, 2)]);
    docs.set('rgs-fresh', { id: 'rgs-fresh', metal: 'gold14k', wPt: W, hPt: H, revision: 0, profileJson: null, owner: 'g14-s1', available: false }); sh.roseStock = { ...docs.get('rgs-fresh') };
    log.api.length = 0; const r = await PN.seat(sh, [tiny.id]);
    assert(!r.ok && r.code === 'pairs', JSON.stringify(r)); assert.match(r.why, /^This partial sheet can hold about \d+ pieces? at most, and every order on this sheet has 2 or more pieces that are never split across sheets\. Choose a bigger partial sheet\.$/);
    assert.equal(log.api.filter(b => b.op === 'roseClaim').length, 0, 'no claim was made'); assert.equal(docs.get('rgs-fresh').owner, 'g14-s1', 'the sheet still holds what it held'); assert.equal(log.started.length, 0, 'nothing was nested');
    // automatic: the only leftover cannot hold a pair, so there is none to pick, and it says why
    const auto = await PN.seat(sh, []); assert(!auto.ok && auto.code === 'none' && /can hold even one whole order of this sheet \(every order has 2 or more pieces/.test(auto.why), JSON.stringify(auto));
    // single pieces on the same leftover: the guard is silent (today's behaviour)
    const sh2 = seated(x, [grp('s0', 21, 1, 1)]); sh2.roseStock = { ...docs.get('rgs-fresh') }; const ok = await PN.seat(sh2, [tiny.id]); assert(ok.ok, 'a single piece is not blocked: ' + JSON.stringify(ok));
  }

  // B6. a pair that already sits on TWO sheets (split before, R3): a move says so before it is pressed, and Cut Sheet says it after
  {
    const x = world('gold14k'), { w } = x, PN = w.PartialNest, p = outlined(x, 'rgs-p', 20); x.setCards([p]);
    const sh = seated(x, [grp('a0', 1001, 5001, 1), grp('s0', 1003, 5003, 1)]), other = x.page(2); other.charms = [grp('a1', 1001, 5001, 2)]; x.S.sheets.gold14k.pages.push(other);
    canned(x, ['a0', 's0']);
    const sharedList = PN._sharedOf(sh, sh.charms);
    assert.deepEqual(plain(sharedList.map(o => [o.group, o.order, o.here, o.elsewhere, o.sheets])), [['1001:5001', '1001', 1, 1, ['14K Sheet 2']]]);
    const pv = await PN.preview(sh, [p.id], { maxMs: 1500 });
    assert(pv.ok && pv.fitsAll, pv.words); assert.equal(pv.shared.length, 1);
    assert.equal(pv.sharedWords, 'Order 1001 also has 1 piece on 14K Sheet 2. A piece of it that does not fit here moves to another sheet, so the order would sit on more sheets than before.');
    assert.equal(PN.cutNote(sh), 'Order 1001: 1 piece of it is cut on this sheet, the other 1 is on 14K Sheet 2.');
    const y = world('gold14k'), alone = seated(y, [grp('s0', 1003, 5003, 1)]); assert.equal(y.w.PartialNest.cutNote(alone), '', 'no order shared with another sheet: nothing to say');
  }

  // B6b. the page's overflow asks for the next sheet of the chain (PAIRFLOW: a pair needs two places there too): a listed partial that cannot hold even one whole order of what moves on is
  //      passed over (not claimed, still free for others); single pieces still take it, as before
  {
    const x = world('gold14k'), { w, log, docs } = x, PN = w.PartialNest, tiny = outlined(x, 'rgs-tiny', 135), roomy = outlined(x, 'rgs-roomy', 20); x.setCards([tiny, roomy]);
    await PN._cards('gold14k');   // (the list the panel read: the page keeps it)
    const stepOf = c => ({ id: c.id, stock: { id: c.stockId, metal: 'gold14k', wPt: W, hPt: H, revision: 1, profileJson: docs.get(c.stockId).profileJson } });
    const sh = seated(x, [grp('z', 99, 1, 1)]); sh._partialId = 'rgs-old-1';
    sh._partialChain = [stepOf(tiny), stepOf(roomy)];
    const moving = [grp('a0', 1001, 5001, 1, { side: 'L' }), grp('a1', 1001, 5001, 2, { side: 'R' })];
    const pg = PN.nextPage(sh, moving);
    assert(pg && pg._partialId === roomy.id, 'the pair goes to the partial that can hold a pair: ' + (pg && pg._partialId));
    assert.equal(docs.get('rgs-tiny').owner, null, 'the one-piece partial was not claimed'); assert.equal(sh._partialChain.length, 0);
    assert(log.agent.some(a => /passed over/.test(a[2]) && /2 pieces \(1 pair\)/.test(a[2])), 'and the history says why: ' + JSON.stringify(log.agent.map(a => a[2])));
    // single pieces: the first listed partial, as always
    const y = world('gold14k'), t2 = outlined(y, 'rgs-tiny', 135), r2 = outlined(y, 'rgs-roomy', 20); y.setCards([t2, r2]); await y.w.PartialNest._cards('gold14k');
    const sh2 = seated(y, [grp('z', 99, 1, 1)]); sh2._partialId = 'rgs-old-1'; sh2._partialChain = [{ id: t2.id, stock: { id: t2.stockId, metal: 'gold14k', wPt: W, hPt: H, revision: 1, profileJson: y.docs.get(t2.stockId).profileJson } }];
    const pg2 = y.w.PartialNest.nextPage(sh2, [grp('s', 98, 1, 1)]); assert(pg2 && pg2._partialId === t2.id, 'a single piece takes the one-piece partial as before');
  }

  // B6c. a sheet of a COMMITTED set is nested again on a partial: what does not fit goes to a new page OUTSIDE the set. An order the set shares with another of its sheets must not be
  //      split that way (rule A / R3): the preview says so, and Use this one refuses before anything is claimed. Otherwise the move works exactly as it did.
  {
    const build = (sets, placeIds) => {
      const x = world('gold14k'), { w, docs } = x, p = outlined(x, 'rgs-p', 20); x.setCards([p]);
      const sh = seated(x, [grp('a0', 1001, 5001, 1, { side: 'L' }), grp('s0', 1003, 5003, 1)]); sh.setId = 'set-1'; sh.draft = false; sh.runId = 'run-1';
      const other = x.page(2); other.sheetId = 'g14-s2'; other.charms = [grp('a1', 1001, 5001, 2, { side: 'R' })]; x.S.sheets.gold14k.pages.push(other);
      docs.set('rgs-fresh', { id: 'rgs-fresh', metal: 'gold14k', wPt: W, hPt: H, revision: 0, profileJson: null, owner: 'g14-s1', available: false }); sh.roseStock = { ...docs.get('rgs-fresh') };
      w.Sets.ofRun = () => sets; canned(x, placeIds); return { x, p, sh };
    };
    const committed = [{ setId: 'set-1', committedAt: 5, sheetIds: ['g14-s1', 'g14-s2'] }];
    // the Left earring would not fit: it moves on to a page outside the set while the Right stays on Sheet 2 of the set
    {
      const { x, p, sh } = build(committed, ['s0']), PN = x.w.PartialNest;
      const pv = await PN.preview(sh, [p.id], { maxMs: 1500 });
      assert(pv.ok && !pv.fitsAll && pv.continues.n === 1, JSON.stringify({ ...pv, links: undefined }));
      assert.deepEqual(plain(pv.setSplit.map(o => [o.order, o.here, o.elsewhere, o.sheets])), [['1001', 1, 1, ['14K Sheet 2']]]);
      assert.equal(pv.setSplitWords, 'Order 1001 has 1 piece on 14K Sheet 2, in the same committed set. Its 1 piece here would move on to a new sheet outside the set, and a pair is never split between a committed set and a sheet outside it. Choose a bigger partial sheet, one that holds every piece of the order.');
      assert(pv.words.endsWith(pv.setSplitWords), 'the words say it too');
      x.log.api.length = 0; const r = await PN.seat(sh, [p.id]);
      assert(!r.ok && r.code === 'setpair' && r.why === pv.setSplitWords, JSON.stringify(r)); assert.equal(x.log.api.length, 0, 'nothing was claimed or released'); assert.equal(x.log.started.length, 0); assert.equal(x.docs.get('rgs-fresh').owner, 'g14-s1', 'the sheet still holds what it held');
      const again = await PN.seat(sh, [p.id], { preview: pv }); assert(!again.ok && again.code === 'setpair', 'with the previewed answer in hand, the same');
    }
    // everything fits: no page outside the set is needed, the move works
    { const { x, p, sh } = build(committed, ['a0', 's0']), r = await x.w.PartialNest.seat(sh, [p.id]); assert(r.ok && r.started, JSON.stringify(r)); }
    // the same sheet in a set that is not committed yet (its sets are made again as sheets change): not refused
    { const { x, p, sh } = build([{ setId: 'set-1', sheetIds: ['g14-s1', 'g14-s2'] }], ['s0']), pv = await x.w.PartialNest.preview(sh, [p.id], { maxMs: 1500 }); assert.equal(pv.setSplit, undefined); const r = await x.w.PartialNest.seat(sh, [p.id]); assert(r.ok, JSON.stringify(r)); }
    // an order that sits on a sheet OUTSIDE the set is not this rule's business (it was split before)
    { const { x, p, sh } = build([{ setId: 'set-1', committedAt: 5, sheetIds: ['g14-s1'] }], ['s0']), pv = await x.w.PartialNest.preview(sh, [p.id], { maxMs: 1500 }); assert.equal(pv.setSplit, undefined); assert.equal(pv.shared.length, 1, 'but it is still said that the order sits on another sheet'); }
  }

  // B7. nothing of this touches a sheet without pairs: the same words, no extra keys
  {
    const x = world('gold14k'), { w } = x, PN = w.PartialNest, p = outlined(x, 'rgs-p', 20); x.setCards([p]);
    const sh = seated(x, [1, 2, 3, 4].map(i => grp('s' + i, 30 + i, 1, 1))); canned(x, ['s1', 's2', 's3', 's4']);
    const pv = await PN.preview(sh, [p.id], { maxMs: 1500 });
    assert.equal(pv.words, 'All 4 pieces fit on this partial sheet.'); assert.equal(pv.splits, undefined); assert.equal(pv.splitWords, undefined); assert.equal(pv.shared, undefined);
    assert.equal(PN.cutNote(sh), '');
  }
}


// ── part C: the server (Cut Sheet records, leftovers, automatic claims, Remove) on a fake Firestore that refuses a read after a write and an array in an array ──
async function partC() {
  const store = new Map(), clone = x => structuredClone(x);
  const ref = path => ({ path, id: path.split('/').at(-1), collection: n => query(path + '/' + n), get: async () => snap(path), set: async (v, o) => put({ path }, v, o && o.merge) });
  const snap = path => ({ id: path.split('/').at(-1), ref: ref(path), exists: store.has(path), data: () => clone(store.get(path)) });
  const query = (path, filters = [], order = null, limit = Infinity) => ({ doc: id => ref(path + '/' + id), where: (...f) => query(path, [...filters, f], order, limit), orderBy: (...o) => query(path, filters, o, limit), limit: n => query(path, filters, order, n),
    get: async () => { let docs = [...store.keys()].filter(k => k.startsWith(path + '/') && !k.slice(path.length + 1).includes('/')).map(snap); docs = docs.filter(d => filters.every(([f, , v]) => d.data()[f] === v)); if (order) docs.sort((a, b) => (a.data()[order[0]] - b.data()[order[0]]) * (order[1] === 'desc' ? -1 : 1)); docs = docs.slice(0, limit); return { docs, size: docs.length }; } });
  const put = (r, v, merge) => { refuseNestedArrays(v, r.path); const old = merge ? store.get(r.path) || {} : {}; const out = { ...old }; for (const [k, x] of Object.entries(v)) out[k] = x && x.__inc ? (+old[k] || 0) + x.__inc : clone(x); store.set(r.path, out); };
  let serial = Promise.resolve();
  const db = { runTransaction: fn => { const p = serial.then(async () => { const writes = []; let wrote = false; const r = await fn({ get: async x => { assert(!wrote, 'Firestore requires all reads before writes'); return x.get(); }, set: (x, v, o) => { wrote = true; writes.push(() => put(x, v, o && o.merge)); }, update: (x, v) => { wrote = true; writes.push(() => put(x, v, true)); }, delete: x => { wrote = true; writes.push(() => store.delete(x.path)); } }); writes.forEach(f => f()); return r; }); serial = p.catch(() => {}); return p; } };
  const FV = { serverTimestamp: () => 123456, increment: n => ({ __inc: n }) };
  const sheetLabel = d => `RG Sheet ${d.sheetIndex}`, setLabel = id => 'Set ' + (/-(\d+)$/.exec(id) || [])[1];
  const events = [];
  const rem = Remnants({ db, col: query, FV, sheetLabel, setLabel, revDoc: () => ref('Charm_Nest_Rev/remnants') });
  const api = RoseStock({ db, col: query, FV, Readiness, sheetLabel, recordRemnant: rem.recordRemnant, stamp: async fn => { events.push(...fn()); } });
  const shape = (id, x, y, w, h) => ({ id, paths: [[[x, y], [x + w, y], [x + w, y + h], [x, y + h]]] });
  store.set('Charm_Nest_Runs/run-test', { lines: { a: { poolIds: ['1001_5001_1', '1001_5001_2', '1002_5002_1', '1002_5002_2', '1003_5003_1', '2001_', 'q1', 'q2', 'q3', 'z1', 'r1', 'r2', 'r3', '4001_6001_1', '4001_6001_2', '4002_6002_1', '1004_7001_1', '1004_7001_2', '1004_7001_3', '1004_7001_4'], spec: { engraveCandidate: false } } } });
  // a sheet record as the page saves it: each charm keeps order and poolId (and, since the pair work, side and groupSize)
  const sheetDoc = (id, idx, charms, shapes, orders) => ({ id, metal: 'rose', sheetIndex: idx, setId: 'set-2026-1', fileBase: 'RG_x_Sheet-' + idx, verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, runId: 'run-test', poolIds: charms.map(c => c.poolId || c.id), placedCount: shapes.length,
    charms, placements: shapes.map(s => ({ id: s.id, cxPt: 20, cyPt: 20, angle: 0, scale: 1 })), outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'test', orders: [] }] }, orders: [], orderReadiness: Object.fromEntries(charms.map(c => String(c.poolId || c.id).split('_')[0]).filter(k => /^\d+$/.test(k)).map(k => [k, { ready: true }])) });   // (orders [] as the stamp then reads each order from the pool ids; the sheet's order-readiness is the Library's business)
  const cutOne = async (sheetId, doc, shapes, stockArgs) => {
    const claim = await api.roseClaim({ sheetId, wPt: 100, hPt: 50, ...stockArgs }); store.set('Charm_Nest_Sheets/' + sheetId, doc);
    const p = await api.rosePlan({ sheetId, stockId: claim.stock.id, revision: claim.stock.revision, fingerprint: RoseStock.fingerprint(doc), shapesJson: JSON.stringify(shapes), allowanceMm: .2, cut: true });
    return api.roseRecordCut({ sheetId, stockId: claim.stock.id, revision: claim.stock.revision, planHash: p.planHash, by: 'Pat Lee', via: 'nest', device: 'charm-nest-1' });
  };
  const ch = (id, rid, tid, copy, extra) => ({ id, order: String(rid), poolId: `${rid}_${tid}_${copy}`, ...extra });

  // C1. Cut Sheet keeps which pieces of which group were cut (Amendment 2: every earring piece has its side, matching or mismatched): a pair cut together, a pair of which only ONE
  //     piece (its Left) is inside the green line, a single, and a quantity-2 earring line (2 Left + 2 Right) cut whole
  const charms = [ch('p1', 1001, 5001, 1, { side: 'L', groupSize: 2 }), ch('p2', 1001, 5001, 2, { side: 'R', groupSize: 2 }), ch('p3', 1002, 5002, 1, { side: 'L', groupSize: 2 }), ch('p4', 1002, 5002, 2, { side: 'R', groupSize: 2 }), ch('p5', 1003, 5003, 1),
    ...[1, 2, 3, 4].map(i => ch('p' + (5 + i), 1004, 7001, i, { side: i % 2 ? 'L' : 'R', groupSize: 4 }))];
  const cutShapes = [shape('p1', 2, 2, 10, 10), shape('p2', 14, 2, 10, 10), shape('p3', 26, 2, 10, 10), shape('p5', 38, 2, 10, 10), ...[1, 2, 3, 4].map(i => shape('p' + (5 + i), 2 + (i - 1) * 12, 16, 10, 10))];   // (p4, the Right of the second pair, is not inside this cut)
  const one = await cutOne('sheet-1', sheetDoc('sheet-1', 1, charms, cutShapes, ['1001', '1002', '1003', '1004']), cutShapes, {});
  const cutDoc = store.get(`Charm_Nest_Rose_Stock/${one.stock.id}/cuts/sheet-1`);
  assert(cutDoc && Array.isArray(cutDoc.groups), 'the cut keeps its groups');
  assert.deepEqual(cutDoc.groups, [
    { k: '1001:5001', order: '1001', cut: 2, onSheet: 2, size: 2, sides: 'LR', ids: ['p1', 'p2'] },
    { k: '1002:5002', order: '1002', cut: 1, onSheet: 2, size: 2, sides: 'L', ids: ['p3'] },
    { k: '1003:5003', order: '1003', cut: 1, onSheet: 1, size: null, sides: '', ids: ['p5'] },
    { k: '1004:7001', order: '1004', cut: 4, onSheet: 4, size: 4, sides: 'LRLR', ids: ['p6', 'p7', 'p8', 'p9'] }], 'which pieces of which line were cut, with their sides, and that the second pair was cut only in part');
  const r1 = store.get(`Charm_Nest_Remnants/${one.stock.id}-1`);
  assert.deepEqual([r1.cutPieces, r1.cutPairs, r1.cutSplit], [8, 3, 1], 'the leftover keeps the numbers: 8 pieces cut, 3 pairs cut together (one pair, and the two pairs of the quantity-2 line), 1 order cut in part');
  assert.deepEqual(events.map(e => [e.orderId, e.data.pieces]), [['1001', 2], ['1002', 1], ['1003', 1], ['1004', 4]], 'each order\'s timeline says how many of its pieces the cut took');
  const listed = (await rem.ops.remnantList({})).items.find(i => i.id === `${one.stock.id}-1`);
  assert.deepEqual([listed.cutPieces, listed.cutPairs, listed.cutSplit], [8, 3, 1], 'and the list gives them back');
  const card1 = (await rem.ops.partialList({ metal: 'rose' })).items.find(i => i.id === `${one.stock.id}-1`);
  assert.deepEqual(card1.cut, { pieces: 8, pairs: 3, split: 1 }, 'the partial sheet card carries them as cut');
  assert(card1.estimate && card1.estimate.pairs === Math.floor(card1.estimate.pieces / 2) && card1.estimate.pairsHigh === Math.floor(card1.estimate.high / 2), 'and its estimate counts pairs: ' + JSON.stringify(card1.estimate));

  // C2. an older sheet record (no side, no groupSize, no poolId): the order stands in for the group, and nothing is claimed about a split it cannot see
  events.length = 0;
  const old = [{ id: 'q1', order: '2001' }, { id: 'q2', order: '2001' }, { id: 'q3', order: '2002' }], oldShapes = [shape('q1', 56, 2, 10, 10), shape('q2', 68, 2, 10, 10), shape('q3', 80, 2, 10, 10)];   // (on the leftover of the first cut: beyond its green line)
  const two = await cutOne('sheet-2', sheetDoc('sheet-2', 2, old, oldShapes, ['2001', '2002']), oldShapes, { stockId: one.stock.id, revision: 1 });
  assert.deepEqual(store.get(`Charm_Nest_Rose_Stock/${two.stock.id}/cuts/sheet-2`).groups.map(g => [g.k, g.cut, g.size, g.sides]), [['2001', 2, null, ''], ['2002', 1, null, '']]);
  const r2 = store.get(`Charm_Nest_Remnants/${two.stock.id}-2`); assert.deepEqual([r2.cutPieces, r2.cutPairs, r2.cutSplit], [3, 1, 0], 'two cut together count as a pair; a split is only claimed when the group\'s size was saved');

  // C3. a record with no pieces to read (the way every cut was recorded before): the cut works exactly as it did and says nothing about pieces
  events.length = 0;
  const bareShapes = [shape('z1', 2, 32, 10, 10)];
  const bareDoc = { ...sheetDoc('sheet-3', 3, [{ id: 'z1' }], bareShapes, []) }; delete bareDoc.charms;   // (a record that has pool ids but no charms array)
  const three = await cutOne('sheet-3', bareDoc, bareShapes, { stockId: one.stock.id, revision: 2 });
  assert.equal(store.get(`Charm_Nest_Rose_Stock/${three.stock.id}/cuts/sheet-3`).groups, undefined, 'no groups recorded when there is nothing to read');
  const r3 = store.get(`Charm_Nest_Remnants/${three.stock.id}-3`); assert.equal(r3.cutPieces, undefined, 'a leftover whose cut cannot say what it was made of says nothing (not "0 pieces")');
  assert.equal((await rem.ops.remnantList({ scope: 'all' })).items.find(i => i.id === `${three.stock.id}-3`).cutPieces, undefined);

  // C4. an automatic claim skips a leftover that cannot hold one whole order (a leftover for one piece, for a sheet of pairs)
  const profileOf = cut => JSON.stringify({ version: 1, wPt: 100, hPt: 50, axis: 'x', step: .5, values: Array(100).fill(cut) });
  const addStock = (id, cut, ms) => store.set('Charm_Nest_Rose_Stock/' + id, { id, metal: 'rose', wPt: 100, hPt: 50, revision: 1, profileJson: profileOf(cut), owner: null, available: true, createdMs: ms });
  addStock('rgs-onepiece', 70, 1); addStock('rgs-roomy', 10, 2);
  const fit = { unit: 2, areaMm2: 61, minMm: 6.7, maxMm: 10.4 };
  assert.deepEqual([...[70, 10].map(c => RoseStock.fitCheck(fit)({ profileJson: profileOf(c) }))], [false, true], 'fitCheck: a leftover for one piece cannot take a pair, a roomy one can');
  assert.equal(RoseStock.fitCheck(undefined)({ profileJson: profileOf(70) }), true, 'no hint, no change'); assert.equal(RoseStock.fitCheck({ unit: 1 })({ profileJson: profileOf(70) }), true, 'single-piece orders: no change');
  assert.equal(RoseStock.fitCheck(fit)({ id: 'x', profileJson: null }), true, 'a sheet with no profile is a whole one: it holds anything');
  const picky = await api.roseClaim({ sheetId: 'sheet-b', wPt: 100, hPt: 50, metal: 'rose', fit });
  assert.equal(picky.stock.id, 'rgs-roomy', 'the sheet of pairs got the roomy leftover, not the first one on the list');
  assert.equal(store.get('Charm_Nest_Rose_Stock/rgs-onepiece').owner, null, 'and the one-piece leftover is still free for a single piece');
  const plainClaim = await api.roseClaim({ sheetId: 'sheet-c', wPt: 100, hPt: 50, metal: 'rose' });
  assert.equal(plainClaim.stock.id, 'rgs-onepiece', 'without the hint, the first on the list as always');
  // 10K / 14K: only a leftover that fits, and nothing else; a leftover that cannot hold a pair is no leftover for that sheet
  store.set('Charm_Nest_Rose_Stock/g14-onepiece', { id: 'g14-onepiece', metal: 'gold14k', wPt: 100, hPt: 50, revision: 1, profileJson: profileOf(70), owner: null, available: true, createdMs: 1 });
  const none = await api.roseClaim({ sheetId: 'sheet-g', wPt: 100, hPt: 50, metal: 'gold14k', onlyRemnant: true, fit });
  assert.deepEqual(none, { stock: null, protectedJson: null }, 'a 14K sheet of pairs claims nothing when the only leftover is for one piece'); assert.equal(store.get('Charm_Nest_Rose_Stock/g14-onepiece').owner, null);
  const g1 = await api.roseClaim({ sheetId: 'sheet-g', wPt: 100, hPt: 50, metal: 'gold14k', onlyRemnant: true, fit: { ...fit, unit: 1 } });
  assert.equal(g1.stock.id, 'g14-onepiece', 'the same leftover, for single pieces: claimed as always');

  // C5. Remove from a sheet: the answer says when only SOME pieces of an order line were asked to leave (the page passes every piece of a group; a lone one is told)
  const rm = [ch('r1', 4001, 6001, 1, { groupSize: 2 }), ch('r2', 4001, 6001, 2, { groupSize: 2 }), ch('r3', 4002, 6002, 1)];
  store.set('Charm_Nest_Sheets/sheet-r', sheetDoc('sheet-r', 9, rm, [], ['4001', '4002']));
  const half = await api.roseTakeOff({ sheetId: 'sheet-r', ids: ['r1'] });
  assert.deepEqual(half.partialGroups, [{ k: '4001:6001', order: '4001', taken: 1, left: 1 }], 'one piece of a pair asked to leave: the pair is named');
  const whole = await api.roseTakeOff({ sheetId: 'sheet-r', ids: ['r1', 'r2'] }); assert.equal(whole.partialGroups, undefined, 'both pieces: nothing to say');
  const single = await api.roseTakeOff({ sheetId: 'sheet-r', ids: ['r3'] }); assert.equal(single.partialGroups, undefined);
  assert.equal(RoseStock.partialGroups({ charms: rm }, new Set(['r1'])).length, 1);
}

(async () => {
  partA(); console.log('A ok');
  if (!JSDOM) console.log('B skipped (jsdom is not installed)'); else { await partB(); console.log('B ok'); }
  await partC(); console.log('C ok');
  console.log('pairs-partial: ok (estimate counts pieces and pairs, plan places whole groups, Use this one never splits or strands a pair and says so, cut records keep the groups, automatic claims skip a leftover too small for a pair)');
})().catch(e => { console.error(e); process.exit(1); });
