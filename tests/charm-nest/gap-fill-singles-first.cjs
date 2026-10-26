/* A multi-piece order that cannot fit a nearly finished sheet whole waits for the next sheet; single-charm orders fill the small
 * gaps (Paul, 13 Oct 2026): "If her sheet is close to being full and you want to add a multipiece order that has to spill over to
 * the next sheet, then keep that in queue and then add it to the next sheet and wait for an order that has a single charm orders
 * to fill in small remaining gaps on all nearly finished sheets when performing nesting."
 *
 * The page's own functions (charm-nest-1.html: turnOrder, feedTurn, feedOn, topupTakes, notePassed, buildJob's flag; the bridge's
 * LiveNest.intakePage) run in a vm with stand-ins, and the real solver seats a hole by hand. Offline, no network, no Firestore.
 *   node tests/charm-nest/gap-fill-singles-first.cjs                                                                         */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const L = require('./pocket-fill-lib.cjs'), S = L.solverOf(L.REPO);
const root = path.join(__dirname, '../..'), bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const plain = v => JSON.parse(JSON.stringify(v));
const K = 4;
const piece = (id, w, h, date = 0, order) => ({ id, w: w * K, h: h * K, scale: K, bits: new Uint8Array(Math.round(w * K) * Math.round(h * K)).fill(1), areaPt2: w * h, widthPt: w, heightPt: h, order: order || id, orderDate: date });

(async () => {
  /* ── 1 · the page's rules ── */
  const ctl = { pages: [], work: [], overflowed: [] };
  const ctx = L.pageRules(L.REPO, ctl);
  ctl.overflow = sh => { ctl.overflowed.push(sh.rejects.slice()); sh.rejects = []; };
  const logs = []; ctx.log = (sh, text) => logs.push(text);
  const sq = (id, order, date, side = 6) => piece(id, side, side, date, order);
  const gap = (tried, extra = {}) => Object.assign({ metal: 'gold', runId: 'run-1', page: 1, charms: [], placements: [], rejects: [], status: 'complete', topup: { at: 0, base: .7, tried: Array.from({ length: tried }, (_, i) => 't' + i), placed: 5 } }, extra);
  const placedOf = (sh, ids) => { sh.placements = ids.map((id, i) => ({ id, cxPt: 5 + 10 * i, cyPt: 5, angle: 0 })); };

  // an older pair, then two younger single-charm orders; one try left
  const mk = () => [sq('P.a', 'PAIR', 1), sq('P.b', 'PAIR', 1), sq('S1', 'S1', 2), sq('S2', 'S2', 3)];
  {
    const sh = gap(34); sh.charms = mk(); ctl.pages = [sh];
    const turn = ctx.feedTurn(sh, ctx.activeCharms(sh));
    assert.deepEqual(turn.map(c => c.id), ['S1'], 'the last try of a gap fill goes to a single-charm order, the older pair does not take it');
    assert.deepEqual(plain(sh.feedWait).sort(), ['P.a', 'P.b', 'S2'], 'the others wait on the sheet, the pair whole');
  }
  {
    const sh = gap(10); sh.charms = mk(); ctl.pages = [sh];
    const turn = ctx.feedTurn(sh, ctx.activeCharms(sh));
    assert.deepEqual(turn.map(c => c.id).sort(), ['P.a', 'P.b', 'S1', 'S2'], 'with tries to spare every order tries, the pair too');
    assert(!sh.feedWait, 'nothing waits');
  }
  {   // a sheet that is not filling its gaps keeps the oldest-first turn (three orders at a time), pair included
    const sh = gap(0); sh.topup = null; sh.charms = mk().concat([sq('S3', 'S3', 4)]); ctl.pages = [sh];
    const turn = ctx.feedTurn(sh, ctx.activeCharms(sh));
    assert.deepEqual(turn.map(c => c.id).sort(), ['P.a', 'P.b', 'S1', 'S2'], 'oldest three orders, the pair first');
    assert.deepEqual(plain(sh.feedWait), ['S3']);
  }
  {   // and one whose gap fill is over
    const sh = gap(0); sh.topup.closedAt = 1; sh.charms = mk(); ctl.pages = [sh];
    assert.equal(ctx.turnOrder(sh, sh.charms)(sh.charms[0], sh.charms[2]) < 0, true, 'a closed gap fill is back to oldest first');
  }
  {   // after the search (S1 has had its try): the one try left goes to the single S2, the pair goes on whole to the next sheet at once
    const sh = gap(34, { verification: { ok: true } }); sh.charms = mk().concat([sq('X', 'X', 0)]); placedOf(sh, ['X']); ctl.pages = [sh]; ctl.overflowed.length = 0; sh.feedWait = ['P.a', 'P.b', 'S2'];
    ctx.feedOn(sh);
    assert.deepEqual(ctl.overflowed.map(a => a.slice().sort()), [['P.a', 'P.b']], 'one try left: the single-charm order keeps it, the older pair goes on to the next sheet now, whole, no wait: ' + JSON.stringify(ctl.overflowed));
  }
  {   // two tries left: the single S2 and the pair both keep a try, nothing goes on
    const sh = gap(33, { verification: { ok: true } }); sh.charms = mk().concat([sq('X', 'X', 0)]); placedOf(sh, ['X']); ctl.pages = [sh]; ctl.overflowed.length = 0; sh.feedWait = ['P.a', 'P.b', 'S2'];
    ctx.feedOn(sh);
    assert.deepEqual(ctl.overflowed, [], 'two tries left: the single takes one, the pair the other: ' + JSON.stringify(ctl.overflowed));
  }

  /* topupTakes: a multi-piece order that needs more than half the free area cannot fit the gaps whole; singles always try */
  {
    const sh = gap(3); sh.liveInfo = { freePt2: 300, usablePt2: 2000 };
    const free = 300, big = [sq('B.a', 'B', 1, 10), sq('B.b', 'B', 1, 10)], small = [sq('C.a', 'C', 1, 6), sq('C.b', 'C', 1, 6)];
    const area = list => list.reduce((n, c) => n + ctx.inflatedArea(c), 0);
    assert(area(big) > free * .5 && area(small) < free * .5, 'the fixtures: ' + area(big) + ' / ' + area(small));
    assert.equal(ctx.topupTakes(sh, big), false, 'a pair too big for the gaps skips the sheet and costs it no try');
    assert.equal(ctx.topupTakes(sh, small), true, 'a small pair still tries the gaps');
    assert.equal(ctx.topupTakes(sh, [sq('Z', 'Z', 1, 30)]), true, 'a single-charm order always tries, whatever its size: the solver says whether it fits');
    // the room under the fill ceiling (80%) counts when it is less than the free area: 900 free, but 1500 of 2000 placed leaves 100 under the ceiling
    sh.liveInfo = { freePt2: 900, usablePt2: 2000, placedPt2: 1500 };
    assert.equal(ctx.topupTakes(sh, small), false, 'a pair needing more than half of what the ceiling leaves cannot fit: ' + area(small) + ' vs 100');
    assert.equal(ctx.topupTakes(sh, [sq('C.a', 'C', 1, 3), sq('C.b', 'C', 1, 3)]), true, 'a small pair that asks for little still tries');
    sh.liveInfo = { freePt2: 900, usablePt2: 2000, placedPt2: 1700 };
    assert.equal(ctx.topupTakes(sh, [sq('C.a', 'C', 1, 3), sq('C.b', 'C', 1, 3)]), false, 'over the ceiling already: no room for a multi-piece order');
    assert.equal(ctx.topupTakes(sh, [sq('Z', 'Z', 1, 3)]), true, 'a single-charm order always tries');
    sh.liveInfo = null; assert.equal(ctx.topupTakes(sh, big), true, 'free area unknown: it tries');
    const open = gap(0); open.topup = null; open.liveInfo = { freePt2: 10 }; assert.equal(ctx.topupTakes(open, big), true, 'a sheet that is not filling its gaps takes any order');
    const rose = gap(3, { metal: 'rose' }); rose.liveInfo = { freePt2: 10 }; assert.equal(ctx.topupTakes(rose, big), true, 'only Gold and Silver fill gaps');
  }

  /* notePassed: the multi-piece orders turned away are on the sheet's record and in its log, once */
  {
    const sh = gap(5); sh.charms = mk(); ctl.pages = [sh]; sh.rejects = ['P.a', 'P.b', 'S2']; logs.length = 0;
    ctx.notePassed(sh);
    assert.deepEqual(plain(sh.topup.passed), ['PAIR'], 'only the multi-piece order is noted, not the single that did not fit');
    assert(logs.some(t => /1 multi-piece order did not fit whole and went on to the next sheet, none is split/.test(t)), logs.join(' | '));
    logs.length = 0; ctx.notePassed(sh); assert.equal(logs.length, 0, 'said once');
  }

  /* ── 2 · the intake: a multi-piece order that cannot fit a gap-filling sheet goes to the next sheet; a single-charm order tries the gaps ── */
  {
    const c = { Set, Map, Math, JSON, Object, Array, String, Number, window: { CN: { topupRoom: ctx.topupRoom, topupTakes: ctx.topupTakes } }, Sets: { ofRun: () => [] } };
    const pages = []; c.pagesOf = () => pages; c.allSheets = () => pages; c.addPage = () => { const p = { metal: 'gold', runId: 'run-1', charms: [], placements: [] }; pages.push(p); return p; };
    c.O = { FAST_MATERIALS: new Set(['gold', 'silver']) }; c.CharmNestRose = { cuts: () => false };
    vm.createContext(c);
    const src = bridge, a = src.indexOf('const closed = p => !!p.roseCutAt'), b = src.indexOf('async function add(run) {', a);
    assert(a > 0 && b > a, 'the intake source is where the test expects it');
    vm.runInContext(src.slice(a, b), c);
    const run = { runId: 'run-1' }, full = gap(4, { charms: [sq('X', 'X', 1)], liveInfo: { freePt2: 300, usablePt2: 2000 } }), fresh = { metal: 'gold', runId: 'run-1', charms: [sq('Y', 'Y', 2)], placements: [] };
    pages.push(full, fresh);
    const pair = [sq('Q.a', 'Q', 5, 10), sq('Q.b', 'Q', 5, 10)];
    assert.equal(c.LiveNest ? 1 : 1, 1);
    const intake = vm.runInContext('intakePage', c);
    assert.equal(intake('gold', run, 'Q', pair), fresh, 'a pair too big for the gaps goes on to the next sheet');
    assert.equal(intake('gold', run, 'W', [sq('W', 'W', 5, 10)]), full, 'a single-charm order tries the gap-filling sheet first');
    assert.equal(intake('gold', run, 'V', [sq('V.a', 'V', 5, 3), sq('V.b', 'V', 5, 3)]), full, 'a small pair still tries the gaps');
    assert.equal(intake('gold', run), full, 'an intake that names no pieces (the page start) is what it was');
    // the order's home sheet stays first: a line of an order already waiting on a sheet in line joins it
    full.charms.push(sq('Q.0', 'Q', 5, 4));
    assert.equal(intake('gold', run, 'Q', pair), full, "an order's home sheet comes first");
  }

  /* ── 3 · the solver: a tight slot (it takes a bar only at an angle the 2° steps skip, so the pocket pass's half steps seat it) that either a
        pair of short bars or one long bar can fill. The orders are undated, as on a sheet filling its gaps. Without the flag the pocket pass
        takes them as listed; with job.singlesFirst the single-charm order takes the slot and the pair is turned away whole. ── */
  {
    const slanted = (id, len, th, deg, order) => {
      const W0 = Math.round(len * K), H0 = Math.round(th * K), r = S.rotateBitmap(new Uint8Array(W0 * H0).fill(1), W0, H0, deg); let n = 0; for (const v of r.bits) n += v;
      return { id, w: r.w, h: r.h, scale: K, bits: r.bits, areaPt2: n / (K * K), widthPt: r.w / K, heightPt: r.h / K, order: order || id, orderDate: 0 };
    };
    const W = 100, Ht = 30, inset = 1, y0 = 9.5, H = 4.8, top = piece('top', W - 2 * inset, y0 - inset), bot = piece('bot', W - 2 * inset, Ht - inset - (y0 + H));
    const lock = [{ id: 'top', cxPt: W / 2, cyPt: inset + (y0 - inset) / 2, angle: 0 }, { id: 'bot', cxPt: W / 2, cyPt: y0 + H + (Ht - inset - (y0 + H)) / 2, angle: 0 }];
    const long = slanted('LONG', 70, 3, 31), m1 = slanted('M.a', 33, 3, 31, 'M'), m2 = slanted('M.b', 33, 3, 31, 'M');
    const angles = Array.from({ length: 180 }, (_, i) => i * 2);
    const job = { sheet: { wPt: W, hPt: Ht, insetPt: inset }, angles, fineRes: 2, coarseRes: .5, maxFill: 1, timeBudgetMs: 60000, maxTrials: 50, clearancePt: 0, seed: 3, careful: true, lockedPlacements: lock };
    for (const [name, order] of [['pair listed first', [m1, m2, long]], ['single listed first', [long, m1, m2]]]) {
      const plain = await S.solve({ ...job, pieces: [top, bot, ...order] }, {}), first = await S.solve({ ...job, singlesFirst: true, pieces: [top, bot, ...order] }, {});
      const ids = r => r.placements.map(p => p.id).filter(id => !['top', 'bot'].includes(id)).sort();
      assert.deepEqual(ids(first), ['LONG'], `${name}: with singlesFirst the single-charm order takes the slot, the pair is turned away whole: ${ids(first)}`);
      assert.deepEqual(first.rejects.slice().sort(), ['M.a', 'M.b'], name + ': and goes on to the next sheet with both its pieces');
      assert(S.verify({ ...job, pieces: [top, bot, ...order] }, first.placements, 4).ok, name + ': no overlap');
      for (const l of lock) assert(first.placements.some(p => p.id === l.id && p.cxPt === l.cxPt && p.cyPt === l.cyPt), name + ': a seated charm never moves');
      const pair = ids(plain).filter(id => id.startsWith('M.')), got = ids(plain);
      assert(pair.length === 0 || pair.length === 2, name + ': without the flag the pair is never split either: ' + got);
      if (name === 'pair listed first') assert.deepEqual(got, ['M.a', 'M.b'], 'without the flag the pocket pass takes the orders as listed (nothing changed): ' + got);
    }
  }
  console.log('gap-fill-singles-first: ok');
})().catch(e => { console.error(e); process.exit(1); });
