/* Every piece of an order on ONE sheet (NESTWHOLE, Paul 13 Oct 2026):
 *   "Keep that in mind when a sheet is being made that the priority should be to maintain all of the pieces for each given order on that one sheet."
 *   "Try to keep orders of the same colour on one sheet ... earrings or multipiece orders ... attempt to keep those all in one sheet."
 * The unit that stays whole is the ORDER (every line, every copy, every pair), above density and above filling a gap.
 *
 *   1. keepOrdersWhole: a piece of an order that did not fit takes the order's SAVED pieces (another line, a copy of a plain quantity-N line)
 *      off the sheet with it, so the order moves on as one; unless the order cannot fit an empty sheet, unless nothing else would stay on the sheet,
 *      never a protected Rose Gold piece or one a person pinned, never in a Merge sheets rerun.
 *   2. feedTurn: a line of an order with a piece already on the sheet takes the turn with it (it is not left waiting and then passed on alone).
 *   3. The sorter's own flow around the real solver (pocket-fill-lib runStream, `home`: a late line goes to the sheet that holds its order):
 *      an order whose late piece does not fit its sheet moves on whole; on seeded streams with late lines no order is split except when its sheet
 *      was already closed (released) when the late piece came; no piece is lost, every sheet verifies.
 *   4. orderSplits (pure): which orders have pieces on two sheets.
 *   5. LiveNest.intakePage (bridge): the order's home sheet is looked at first for every metal (a planned or protected contour is not).
 * Offline and deterministic; no network, no Firestore, no paid call.   node tests/charm-nest/nest-whole-orders.cjs  [--root <dir with the older page and solver>]  */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const L = require('./pocket-fill-lib.cjs');
const argv = process.argv.slice(2), ROOT = argv.includes('--root') ? path.resolve(argv[argv.indexOf('--root') + 1]) : L.REPO, BASE = ROOT !== L.REPO;
const S = L.solverOf(ROOT);
const has = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8').includes('function orderMatesOf(');
const ok = [];
const done = name => { ok.push(name); console.log('  ok', name); };
const plain = v => JSON.parse(JSON.stringify(v));

(async () => {
  /* ── 1 · keepOrdersWhole lifts the saved pieces of an order with the piece that did not fit ── */
  {
    const ctl = { pages: [], work: [], overflow: () => null }, ctx = L.pageRules(ROOT, ctl);
    ctx.usableArea = () => 80 * 50;   // 4000 pt2: the ceiling .8 and the room .7 make 2240 pt2 for one order
    const mk = (rejects, extra = {}) => {
      // the order o0 has a saved line (a) and a line (b) that did not fit; o1 is another saved order; o2 is another order that did not fit
      const mkc = (id, order, side, date) => Object.assign(L.square(id, side, order, date), {});
      const charms = [mkc('o0a', 'o0', 30, 1), mkc('o0b', 'o0', 30, 1), mkc('o1', 'o1', 34, 2), mkc('o2', 'o2', 30, 3)];
      const sh = Object.assign({ metal: 'gold', charms, appendOnly: true, nestInitial: [{ id: 'o0a' }, { id: 'o1' }], placements: [{ id: 'o0a' }, { id: 'o1' }], rejects: rejects.slice() }, extra);
      return { sh, byId: new Map(charms.map(c => [c.id, c])) };
    };
    let t = mk(['o0b']);
    ctx.keepOrdersWhole(t.sh, t.byId);
    if (has) {
      assert.deepEqual(t.sh.placements.map(p => p.id), ['o1'], 'the saved line of the order leaves with the line that did not fit: ' + JSON.stringify(t.sh.placements));
      assert.deepEqual(t.sh.rejects.slice().sort(), ['o0a', 'o0b'], 'the whole order moves on');
    } else assert.deepEqual(t.sh.placements.map(p => p.id), ['o0a', 'o1'], 'before: the saved line stays and the order is split');
    if (has) {
      // an order too big for one empty sheet is not lifted (it cannot be whole anyway)
      t = mk(['o0b']); t.sh.charms.find(c => c.id === 'o0b').areaPt2 = 2400;
      ctx.keepOrdersWhole(t.sh, t.byId); assert.deepEqual(t.sh.placements.map(p => p.id), ['o0a', 'o1'], 'an order bigger than a sheet stays as it is');
      // nothing else on the sheet: it would be emptied, and an emptied sheet passes nothing on
      t = mk(['o0b']); t.sh.placements = [{ id: 'o0a' }]; t.sh.nestInitial = [{ id: 'o0a' }];
      ctx.keepOrdersWhole(t.sh, t.byId); assert.deepEqual(t.sh.placements.map(p => p.id), ['o0a'], 'the only piece on the sheet is not lifted');
      // a piece a person pinned, a protected Rose Gold piece, a Merge sheets rerun
      t = mk(['o0b']); t.byId.get('o0a').pinned = { cxPt: 1, cyPt: 1, angle: 0 };
      ctx.keepOrdersWhole(t.sh, t.byId); assert.deepEqual(t.sh.placements.map(p => p.id), ['o0a', 'o1'], 'a piece a person pinned stays');
      t = mk(['o0b']); t.byId.get('o0a').pinned = { cxPt: 1, cyPt: 1, angle: 0 }; t.byId.get('o0a').arrivalPin = true;
      ctx.keepOrdersWhole(t.sh, t.byId); assert.deepEqual(t.sh.placements.map(p => p.id), ['o1'], 'a pin the sheet only made to hold the saved layout (arrivalPin) does not keep it');
      t = mk(['o0b'], { roseProtected: { placements: [{ id: 'o0a' }, { id: 'o1' }] } });
      ctx.keepOrdersWhole(t.sh, t.byId); assert.deepEqual(t.sh.placements.map(p => p.id), ['o0a', 'o1'], 'protected Rose Gold pieces stay');
      t = mk(['o0b'], { _mergeNext: {} });
      ctx.keepOrdersWhole(t.sh, t.byId); assert.deepEqual(t.sh.placements.map(p => p.id), ['o0a', 'o1'], 'a Merge sheets rerun lifts nothing saved');
      // another order's saved piece is never lifted for this order, and an order with nothing turned away is untouched
      t = mk(['o2']); ctx.keepOrdersWhole(t.sh, t.byId); assert.deepEqual(t.sh.placements.map(p => p.id), ['o0a', 'o1'], 'only the order that missed takes its pieces along');
      t = mk([]); ctx.keepOrdersWhole(t.sh, t.byId); assert.deepEqual(t.sh.placements.map(p => p.id), ['o0a', 'o1'], 'nothing turned away: nothing lifted');
      // a sheet whose usable area the page cannot give (a stand-in without usableArea) keeps the old behaviour
      const bare = L.pageRules(ROOT, ctl); t = mk(['o0b']); bare.keepOrdersWhole(t.sh, t.byId);
      assert.deepEqual(t.sh.placements.map(p => p.id), ['o0a', 'o1'], 'without the sheet\'s usable area nothing new is lifted');
    }
    done('keepOrdersWhole: the saved pieces of an order that missed leave with it (' + (has ? 'after' : 'before') + ')');
  }

  /* ── 2 · feedTurn: a line of an order with a piece on the sheet takes the turn ── */
  {
    const ctl = { pages: [], work: [], overflow: () => null }, ctx = L.pageRules(ROOT, ctl);
    const c = (id, order, date) => Object.assign(L.square(id, 6, order, date), { orderInfo: { receiptId: order, transactionId: id } });
    // o0 has a piece placed; five newer orders wait (more than the three of a turn) and a second line of o0 has come in last
    const items = [c('o0a', 'o0', 1), c('n1', 'n1', 2), c('n2', 'n2', 3), c('n3', 'n3', 4), c('n4', 'n4', 5), c('n5', 'n5', 6), c('o0b', 'o0', 1)];
    const sh = { metal: 'gold', runId: 'run-1', charms: items, placements: [{ id: 'o0a', cxPt: 5, cyPt: 5, angle: 0 }], rejects: [] };
    ctl.pages.push(sh);
    const turn = ctx.feedTurn(sh, items).map(x => x.id).sort();
    assert.deepEqual(turn, ['n1', 'n2', 'o0a', 'o0b'], 'an older order\'s second line takes the turn with the order (it is one of the three oldest orders): ' + turn);
    assert.deepEqual(plain(sh.feedWait).sort(), ['n3', 'n4', 'n5'], 'and the others wait');
    // the order's rank behind the three oldest orders (a newer order, one of whose lines was saved on this sheet before the rest came in)
    const late = [c('o9a', 'o9', 9), c('m1', 'm1', 2), c('m2', 'm2', 3), c('m3', 'm3', 4), c('o9b', 'o9', 9)];
    const sh2 = { metal: 'gold', runId: 'run-1', charms: late, placements: [{ id: 'o9a', cxPt: 5, cyPt: 5, angle: 0 }], rejects: [] }; ctl.pages.push(sh2);
    const turn2 = ctx.feedTurn(sh2, late).map(x => x.id);
    if (has) assert(turn2.includes('o9b') && turn2.includes('o9a'), 'a later order\'s second line is not left waiting behind three older orders: ' + turn2);
    else assert(!turn2.includes('o9b'), 'before: it waits behind the three older orders and would be passed on alone');
    done('feedTurn: a line of an order with a piece on the sheet takes the turn (' + (has ? 'after' : 'before') + ')');
  }

  /* ── 3 · the sorter's flow around the real solver ── */
  {
    // the sheet is 80 x 50 pt; o0 and o1 fill it, a second line of o0 comes in the next update and cannot fit
    const stock = { wPt: 80, hPt: 50 }, mk = (id, side, date, order) => ({ id, kind: 'x', pieces: [L.square(id, side, order || id, date)] });
    const orders = () => [mk('o0', 30, 1001), mk('o1', 34, 1002), mk('o0.late', 30, 1001, 'o0')];
    const run = await L.runStream({ root: ROOT, metal: 'gold', sheet: stock, orders: orders(), batch: [2, 1], seed: 1, home: true, unjam: false });
    const where = id => run.pages.filter(p => p.placements.some(q => q.id === id) || p.charms.some(c => c.id === id)).map(p => p.page).join('+');
    const sheetsOfO0 = new Set(['o0', 'o0.late'].map(where));
    if (has) assert.equal(sheetsOfO0.size, 1, 'the order is on one sheet: o0 on ' + where('o0') + ', its second line on ' + where('o0.late'));
    else assert.equal(sheetsOfO0.size, 2, 'before: the saved line stays and the late one starts the next sheet');
    assert(run.pages.every(p => !p.placements.length || p.verification.ok), 'every sheet verified');
    assert.equal(run.pages.reduce((n, p) => n + p.placements.length, 0), 3, 'no piece is lost');
    done('flow: the late line of an order that does not fit its sheet moves on with the order (' + (has ? 'after' : 'before') + ')');

    // seeded streams of squares (small, middle, large; half the orders hold two pieces, most of those come back as a late line three orders later): the sheets close at
    // the fill ceiling, so some late lines meet a full sheet. No order is split but those whose sheet was released when the late piece came (a built sheet is never
    // rebuilt); the older tree split one more order on each of these seeds (seed 6 order o21, seed 7 order o29), a lifted piece costs its sheet a few points of fill once
    const report = [];
    for (const seed of [6, 7]) {
      const r0 = L.rng(seed), size = () => { const u = r0(); return u < .5 ? 6 + r0() * 4 : u < .85 ? 10 + r0() * 6 : 16 + r0() * 8; };
      const stream = [], lates = [], pieces = [];
      for (let i = 0; i < 30; i++) {
        const id = 'o' + i, two = r0() < .5, late = two && r0() < .7, date = 1000 + i;
        const a = L.square(id + 'a', size(), id, date); pieces.push(a);
        if (!two) { stream.push({ id, pieces: [a] }); continue; }
        const b = L.square(id + 'b', size() + 2, id, date); pieces.push(b);
        if (late) { stream.push({ id, pieces: [a] }); lates.push({ at: stream.length + 3, o: { id: id + '.late', pieces: [b] } }); } else stream.push({ id, pieces: [a, b] });
      }
      for (const l of lates.sort((x, y) => y.at - x.at)) stream.splice(Math.min(stream.length, l.at), 0, l.o);
      const t0 = Date.now(), r = await L.runStream({ root: ROOT, metal: 'gold', sheet: { wPt: 80, hPt: 50 }, orders: stream, batch: [3, 2, 4], seed, home: true, unjam: false, job: { timeBudgetMs: 20000, angles: [0, 90] } });
      assert(r.pages.every(p => !p.placements.length || p.verification.ok), 'seed ' + seed + ': every sheet verified');
      assert.equal(r.pages.reduce((n, p) => n + p.placements.length, 0), pieces.length, 'seed ' + seed + ': every piece is on a sheet');
      const at = new Map(); for (const p of r.pages) for (const q of p.placements) at.set(q.id, p.page);
      let split = 0, avoidable = [];
      for (const id of new Set(pieces.map(c => c.order))) {
        const sheets = new Set(pieces.filter(c => c.order === id).map(c => at.get(c.id)));
        if (sheets.size > 1) { split++; if (!r.homeClosed.has(id)) avoidable.push(id); }
      }
      if (has) assert.deepEqual(avoidable, [], `seed ${seed}: an order is split over sheets though its sheet was open when its late piece came`);
      report.push(`seed ${seed}: ${lates.length} late lines, ${split} split orders (${r.homeClosed.size} came back to a sheet already released, ${avoidable.length} did not), ${r.pages.length} sheets, ${Math.round((Date.now() - t0) / 1000)} s`);
    }
    console.log('    ' + report.join('\n    '));
    done('flow: seeded streams with late lines (' + (has ? 'after' : 'before') + ')');
  }

  /* ── 4 · orderSplits (pure) ── */
  if (has) {
    const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8'), a = html.indexOf('function orderSplits('), b = html.indexOf('/** A split in plain words', a);
    const vm = require('node:vm'), ctx = { Set, Map, String }; vm.createContext(ctx); vm.runInContext(html.slice(a, b), ctx);
    const p1 = { page: 1, charms: [{ id: 'a', order: 'X' }, { id: 'b', order: 'Y' }, { id: 'c', order: 'Z', excluded: true }] }, p2 = { page: 2, charms: [{ id: 'd', order: 'X' }, { id: 'e', order: 'Z' }, { id: 'f' }] };
    const r = ctx.orderSplits([p1, p2]);
    assert.deepEqual(plain(r.map(x => [x.order, x.pages.map(p => p.page)])), [['X', [1, 2]]], 'X is on two sheets; Y on one; the excluded piece of Z does not count; a piece with no order stands alone');
    assert.deepEqual(plain(ctx.orderSplits([p1, p2], new Set(['Y']))), [], 'asked about one order only');
    done('orderSplits: orders with pieces on two sheets');
  }

  /* ── 5 · the bridge's intakePage looks at an order's home sheet for every metal ── */
  if (has) {
    const br = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8'), a = br.indexOf('function intakePage('), b = br.indexOf('async function add(run)', a);
    const vm = require('node:vm');
    const pages = { rose: [], gold: [], silver: [], gold14k: [] };
    const ctx = { String, window: { CN: { topupRoom: () => 1 }, Gate: {} }, O: { FAST_MATERIALS: new Set(['silver', 'gold']) }, pagesOf: m => pages[m], addPage: m => { const p = { charms: [], runId: 'r1', metal: m, fresh: true }; pages[m].push(p); return p; }, Sets: { ofRun: () => [] }, CharmNestRose: { cuts: m => m === 'gold14k' || m === 'rose' } };
    vm.createContext(ctx); vm.runInContext('const closed = p => !!p.releaseFull || !!p.roseCutAt;' + br.slice(a, b) + ';this.intakePage = intakePage;', ctx);
    const run = { runId: 'r1' }, pg = (o, extra = {}) => Object.assign({ runId: 'r1', charms: [{ order: o }] }, extra);
    // 14K: the order's sheet is the first page, the newest is the second; the late line goes to the order's own sheet
    pages.gold14k = [pg('A'), pg('B')]; assert.equal(ctx.intakePage('gold14k', run, 'A'), pages.gold14k[0], 'a 14K late line goes to the sheet that holds its order');
    assert.equal(ctx.intakePage('gold14k', run, 'C'), pages.gold14k[1], 'a new order of a cut metal still takes the newest sheet');
    pages.gold14k = [pg('A', { roseProtected: { placements: [] } }), pg('B')]; assert.equal(ctx.intakePage('gold14k', run, 'A'), pages.gold14k[1], 'a protected contour is not looked at');
    pages.gold14k = [pg('A', { releaseFull: true }), pg('B')]; assert.equal(ctx.intakePage('gold14k', run, 'A'), pages.gold14k[1], 'a closed sheet is not looked at');
    pages.gold = [pg('A'), pg('B')]; assert.equal(ctx.intakePage('gold', run, 'A'), pages.gold[0], 'Gold: as before');
    assert.equal(ctx.intakePage('gold', run, null), pages.gold[0], 'Gold with no order named: the earliest open sheet with room, as before');
    done('intakePage: the order\'s home sheet first for every metal');
  }
  console.log(`Whole orders OK (${BASE ? 'older tree' : 'this tree'}): ${ok.length} checks`);
})().catch(e => { console.error(e); process.exitCode = 1; });
