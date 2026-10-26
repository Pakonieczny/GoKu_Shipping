/* R16 HARD (Paul, 26 Oct 2026): "You will notice that my Set of Sheets rules are still being broken again. Orders are still being spread across sheets of the same colour where I explicitly forbid that
 * from happening and I believe I told you many times that sheets of the same colour cannot have cross-pollination of charms from a multi-charm/earring orders ... All multi-piece orders that do not fit on
 * a given sheet (Near the end of that sheet) should be put onto the next avaliable sheet and only single piece/charm orders should be used to fill/complete a given sheet and thus this will prevent cross
 * contamination of charms from multi-piece orders for given Sheet colour/type."
 * His sandbox Set-1 of 27 Oct held GF Sheet 1 + 2 and SS Sheet 1 + 2, chained by multi-piece orders. The cause: a LATE line of a multi-piece order (held for a question or its engraving, a design that
 * loaded late, a line waiting its day, a replaced charm) came after the order's first pieces were seated on a sheet released full meanwhile; the intake looked for the order's sheet among open sheets only
 * and put the line on the next sheet. Now the order's own sheet of a colour takes every later piece of it (LiveNest.orderHome / reopensHome / reopenHome), its nest keeps the order whole or passes it on
 * whole, and a net at every nest (homeward, charm-nest-1.html) brings a piece that still finds itself apart from its order back to it.
 *
 * 1 · the rule's functions (charm-nest-bridge.js LiveNest): which sheet is an order's home, which released sheet may take its own order's later piece, how it is kept released
 * 2 · scenes through the page's own code (tests/charm-nest/nospread.cjs Scene: one colour's sheets around a stand-in packer): a near-complete sheet is topped up with single-piece orders only; a multi-piece
 *     order that does not fit whole goes whole to the next sheet; a partial seat is lifted whole; a late line finds its order on a released sheet (fits: whole there; does not fit: the whole order moves on);
 *     a sheet that is cut leaves a split of necessity, said so; the page without NOSPREAD (S.settings.nospread = "off") splits, for the record
 * 3 · the net (homeward): a stray piece on another sheet goes home; a stray alone on a fresh sheet brings its order to it; never off a cut sheet
 * 4 · the other paths that move a piece: PAIR's follow (loose pieces never leave their order's seated pieces) and INPLACE's next sheet (the order's home first)
 * 5 · a sandbox-like day: both colours, many multi-piece and earring orders, mixed-metal orders, 40 % of the extra lines 3 to 60 updates late, bulk updates, one order bigger than a sheet, a leaky packer,
 *     and the live timing of the sandbox at 1000x (the next update arrives while a search is out): zero multi-piece orders on two sheets of one colour, by the page's own count and by the app's checker
 *     (CharmNestSetInvariants warnings, rule orderSplit, R16); every piece placed, nothing stuck, the fill of released sheets kept
 * Fake orders only; offline; deterministic (seeded). No network, no Firestore, no Etsy, no paid call.
 *   node tests/charm-nest/nospread-hard.cjs [--quick] [--root DIR --day-only]    (--root DIR --day-only: the day's numbers for another checkout, e.g. the commit before this change; nothing is asserted) */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
process.env.SETSIM_QUIET = process.env.SETSIM_QUIET || '1';
const Lib = require('./setsim-lib.cjs'), NL = require('./setsim-nospread-lib.cjs');
const { Sim, orderOf, lineOf, partOf, piecesOf, schedule, makeStream, L } = Lib;
const argv = process.argv.slice(2), QUICK = argv.includes('--quick'), opt = (k, d) => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : d; };
const REPO = path.join(__dirname, '../..'), ROOT = path.resolve(opt('root', REPO)), DAY_ONLY = argv.includes('--day-only');
let checks = 0;
const ok = (c, msg) => { checks++; assert.ok(c, msg); };
const eq = (a, b, msg) => { checks++; assert.deepEqual(a, b, msg); };
const section = name => console.log('  ' + name);
const S0 = L.solverOf(REPO);

/* ═══ 1 · the rule's functions ═══ */
function functions() {
  const sim = new Sim({ root: REPO, step: 60, seed: 1, sheet: '60x40' }), ctx = sim.ctx; sim.metal('gold'); sim.metal('silver'); sim.metal('rose');
  for (const n of ['orderHome', 'reopensHome', 'reopenHome']) ok(vm.runInContext(`typeof ${n}`, ctx) === 'function', `LiveNest has ${n}`);
  const G = ctx.S.sheets.gold.pages, run = { runId: 'run-1' };
  const piece = (o, i, placed = true) => ({ c: { id: `${o}_1_${i}`, poolId: `${o}_1_${i}`, order: o }, placed });
  const page = (n, list, extra) => { const p = Object.assign({ metal: 'gold', runId: 'run-1', page: n, sheetId: 'g' + n, charms: list.map(x => x.c), placements: list.filter(x => x.placed).map(x => ({ id: x.c.id })), rejects: [], status: 'complete' }, extra || {}); G.push(p); return p; };
  const g1 = page(1, [piece('A', 1), piece('A', 2), piece('B', 1)], { releaseFull: true }), g2 = page(2, [piece('C', 1)]);
  ok(ctx.closed(g1) && !ctx.closed(g2), 'sheet 1 is released (closed to new orders), sheet 2 is open');
  eq(ctx.orderHome('gold', run, 'A'), g1, 'the home of order A is sheet 1, although it was released: it is not cut, approved or sent');
  eq(ctx.orderHome('gold', run, 'C'), g2, 'an order on an open sheet: that sheet');
  eq(ctx.orderHome('gold', run, 'Z'), null, 'a new order has no home');
  eq(ctx.orderHome('rose', run, 'A'), null, 'Gold Filled and Sterling Silver only (Rose Gold, 10K and 14K keep their own rules: greenFits)');
  for (const [what, set, undo] of [['cut by the laser', p => { p.laserDoneAt = Date.now(); }, p => { delete p.laserDoneAt; }], ['recalled from the library', p => { p.recalled = true; }, p => { delete p.recalled; }],
    ['approved for laser cutting', p => { p.processReady = true; }, p => { delete p.processReady; }], ['being moved by hand (Manual Control)', p => { p._manualHold = true; }, p => { delete p._manualHold; }],
    ['held by a problem', p => { p.runHold = 'x'; }, p => { delete p.runHold; }]]) {
    set(g1); ok(!ctx.reopensHome(g1, run, 'A') && ctx.orderHome('gold', run, 'A') === null, `a sheet ${what} never reopens: the order's later piece is a split of necessity`); undo(g1);
  }
  const sets = ctx.Sets.ofRun; ctx.Sets.ofRun = () => [{ setId: 's1', sheetIds: ['g1'], committedAt: Date.now() }];
  ok(!ctx.reopensHome(g1, run, 'A'), 'a sheet of a set sent to the station (committed) never reopens'); ctx.Sets.ofRun = () => [{ setId: 's1', sheetIds: ['g1'], processReady: true }];
  ok(!ctx.reopensHome(g1, run, 'A'), 'nor one of a set approved for laser cutting'); ctx.Sets.ofRun = () => [{ setId: 's1', sheetIds: ['g1'] }];
  ok(ctx.reopensHome(g1, run, 'A'), 'a sheet of a set that is only formed reopens (Gate.keep keeps it in its set while it nests)'); ctx.Sets.ofRun = sets;
  const hold = ctx.window.Gate.holding; ctx.window.Gate.holding = p => !!p.keepRelease;
  g1.keepRelease = { full: true, at: Date.now(), rid: 'Q' }; ok(!ctx.reopensHome(g1, run, 'A'), 'a sheet kept for ANOTHER order\'s change (the sheet window\'s move, a release from hold) is left alone');
  g1.keepRelease.rid = 'A'; ok(ctx.reopensHome(g1, run, 'A'), 'one kept for this order\'s own later pieces takes the next one too'); delete g1.keepRelease; ctx.window.Gate.holding = hold;
  ok(ctx.reopenHome(g1, 'A') && g1.keepRelease && g1.keepRelease.full === true && g1.keepRelease.rid === 'A', 'reopenHome keeps the sheet released while it nests (keepRelease.full, for this order)');
  ok(!ctx.reopenHome(g2, 'C') && !g2.keepRelease, 'an open sheet needs nothing kept');
  delete g1.keepRelease;
  g2.charms.push({ id: 'A_2_1', poolId: 'A_2_1', order: 'A' }); g2.placements.push({ id: 'A_2_1' });
  eq(ctx.orderHome('gold', run, 'A'), g2, 'an order already apart (made before this rule): the open sheet gathers what comes, the net brings the rest');
  const big = page(3, [piece('D', 1), piece('D', 2)]); big.charms.forEach((c, i) => { c.chunk = i + 1; });
  eq(ctx.orderHome('gold', run, 'D'), null, 'an order bigger than one sheet (its units, chunk) fills consecutive sheets by design: no home');
  ctx.S.settings.nospread = 'off'; eq(ctx.orderHome('gold', run, 'C'), null, 'S.settings.nospread = "off" puts it back with the rest of NOSPREAD'); delete ctx.S.settings.nospread;
  section('the rule\'s functions: the order\'s home, which released sheet reopens for it, how it is kept released');
}

/* ═══ 2 · scenes through the page's own code ═══ */
const { Scene } = require('./nospread.cjs');
const lateLine = (o, n, spec) => { const l = lineOf(S0, o.id, n, spec.kind || 'single', spec, 'gold', 1000 + o.n); o.lines.push(l); return l; };
const singles = async (sc, page, until, tag) => { for (let i = 0; i < 80 && !until(); i++) await sc.bring(sc.mk([{ kind: 'single', idx: 1 + (i % 6), f: .85 }], tag + i)); };
const listen = sc => { const said = []; const was = sc.sim.ctx.agent; sc.sim.ctx.agent = (who, kind, text) => { said.push({ kind, text: String(text || '') }); return was(who, kind, text); }; return said; };
async function scenes() {
  // a near-complete sheet is topped up with single-piece orders only; a multi-piece order that does not fit whole goes whole to the next sheet
  {
    const sc = new Scene('near'); await sc.fillTo(.60, .72);
    const before = new Set(sc.pages[0].charms.map(c => String(c.order)));
    const N = sc.discs(5, 1.6), P = sc.discs(2, 1.3), T = [0, 1, 2, 0].map((idx, i) => sc.mk([{ kind: 'single', idx, f: 1 }], 300 + i));
    await sc.bring(N, P, ...T); await sc.idle(2);
    const added = [...new Set(sc.pages[0].placements.map(q => String((sc.pages[0].charms.find(c => c.id === q.id) || {}).order)))].filter(o => !before.has(o));
    ok(added.length >= 1 && added.every(o => sc.orders.find(x => x.id === o).lines.every(l => l.pieces.length === 1)), `the near-complete sheet is topped up with single-piece orders only (added: ${added.join(', ')}; ${sc.report()})`);
    for (const o of [N, P]) { const w = sc.where(o); ok(w.unplaced === 0 && w.pages.length === 1 && w.pages[0] !== 1, `a multi-piece order that does not fit the near-complete sheet whole goes whole to the next sheet (${JSON.stringify(w)})`); }
    section('a near-complete sheet: single-piece orders top it up; multi-piece orders that do not fit go whole to the next sheet');
  }
  // a partial seat is lifted whole: a packer that seats part of an order (leak) near completion and on the order's own sheet
  {
    const sc = new Scene('leak', { leak: 1 }); await sc.fillTo(.40, .50);
    // (order O: an earring pair now, four discs later that ask for 60 % of the room the sheet has now: by the time they come the sheet is near completion and they cannot all fit)
    const O = sc.mk([{ kind: 'pair', idx: 4, f: .9 }]), base = L.charm(S0, 'probe', 10, 1, 'p', 0).areaPt2;
    lateLine(O, 2, { kind: 'discs', k: 4, idx: 10, f: Math.sqrt(.6 * sc.room(1) / (4 * base)) });
    await sc.bring({ order: O, lines: [0] });
    await singles(sc, 1, () => sc.fill(1) >= .64 || sc.pages[0].releaseFull, 400);
    await sc.bring({ order: O, lines: [1] }); await sc.idle(2);
    const others = [0, 1, 2, 3, 4, 5].map(i => sc.mk([{ kind: i % 2 ? 'discs' : 'pair', k: 4, idx: 8 + i, f: 1.1 }], 800 + i)); await sc.bring(...others); await sc.idle(3);
    for (const o of [O, ...others]) { const w = sc.where(o); ok(w.unplaced === 0 && w.pages.length === 1, `an order the packer left half seated is lifted whole and ends on ONE sheet: ${o.id} ${JSON.stringify(w)} (${sc.report()})`); }
    section('a packer that seats part of an order: every piece of it comes off and the whole order goes on');
  }
  // a late line finds its order on a sheet released meanwhile: it goes there; whole there if it fits, else the WHOLE order moves on; the sheet stays released
  for (const [label, share] of [['fits a gap', .003], ['does not fit', 1.6]]) {
    const run = async settings => {
      const sc = new Scene('late ' + label, { settings }); await sc.fillTo(.40, .50);
      const O = sc.mk([{ kind: 'pair', idx: 3, f: .8 }]), room0 = sc.room(1), base = L.charm(S0, 'probe', 12, 1, 'p', 0).areaPt2;
      lateLine(O, 2, { kind: 'single', idx: 12, f: Math.sqrt(share * room0 / base) });
      await sc.bring({ order: O, lines: [0] });
      const first = sc.where(O).pages.slice();
      await singles(sc, 1, () => !!sc.pages[0].releaseFull, 500);
      const released = !!sc.pages[0].releaseFull;
      await sc.idle(2); await sc.bring({ order: O, lines: [1] }); await sc.idle(3);
      return { sc, O, first, released, w: sc.where(O), still: !!sc.pages[0].releaseFull, onOne: sc.pages[0].charms.filter(c => String(c.order) === O.id).length };
    };
    const on = await run({}), off = await run({ nospread: 'off' });
    ok(on.first.join() === '1' && on.released, `the first line went on sheet 1, which was released before the late line came (${on.sc.report()})`);
    ok(on.w.unplaced === 0 && on.w.pages.length === 1, `the late line that ${label} ends with its order on ONE sheet: ${JSON.stringify(on.w)} (${on.sc.report()})`);
    if (share < 1) ok(on.w.pages[0] === 1, `a late line that fits a gap of the released sheet joins its order there (${JSON.stringify(on.w)})`);
    else ok(on.w.pages[0] !== 1 && on.onOne === 0, `a late line that does not fit takes its whole order to the next sheet: nothing of it is left on sheet 1 (${JSON.stringify(on.w)})`);
    ok(on.still, 'sheet 1 stays released (it is kept released while it nests the late line: Gate.keep / reopenHome)');
    ok(off.w.unplaced === 0 && off.w.pages.length === 2, `for the record, the page without NOSPREAD splits it: ${JSON.stringify(off.w)}`);
    section(`a late line that ${label}: the order ends whole on sheet ${on.w.pages[0]} (the page without the rule: on sheets ${off.w.pages.join(' and ')})`);
  }
  // a sheet that can take nothing more (cut): the late line is a split of necessity, said so
  {
    const sc = new Scene('late cut'), said = listen(sc); await sc.fillTo(.40, .50);
    const O = sc.mk([{ kind: 'pair', idx: 3, f: .8 }]); lateLine(O, 2, { kind: 'single', idx: 2, f: .8 });
    await sc.bring({ order: O, lines: [0] }); await singles(sc, 1, () => !!sc.pages[0].releaseFull, 600);
    sc.pages[0].laserDoneAt = Date.now();
    await sc.bring({ order: O, lines: [1] }); await sc.idle(2);
    const w = sc.where(O);
    ok(w.unplaced === 0 && w.pages.length === 2, `the late line of an order whose sheet is cut cannot join it: ${JSON.stringify(w)}`);
    ok(said.some(x => x.kind === 'warn' && /Split order .*split of necessity/.test(x.text)), 'it is said plainly in the log: a split of necessity');
    section('a sheet that is cut: the late line is a split of necessity, said in the log');
  }
}

/* ═══ 3 · the net ═══ */
async function net() {
  const stray = (sc, o, n, page, spec) => { const l = lateLine(o, n, spec || { kind: 'single', idx: 1, f: .7 }); for (const c of l.pieces) page.charms.push(Object.assign({}, c, { pinned: null })); page.appendOnly = page.intakeAppend = page.placements.length > 0; sc.m.startNest(page); return l; };
  // a stray piece put on another open sheet by a path that bypassed the intake: it goes to its order's released sheet, which nests it (whole there, or the whole order moves on)
  {
    const sc = new Scene('net push'), said = listen(sc); await sc.fillTo(.40, .50);
    const O = sc.mk([{ kind: 'pair', idx: 3, f: .8 }]); await sc.bring(O);
    await singles(sc, 1, () => !!sc.pages[0].releaseFull, 700); await singles(sc, 2, () => sc.pages.length > 1 && sc.pages[1].placements.length >= 3, 750);
    ok(sc.where(O).pages.join() === '1' && sc.pages[0].releaseFull && sc.pages[1].placements.length >= 3, `order O is on released sheet 1, sheet 2 is filling (${sc.report()})`);
    stray(sc, O, 5, sc.pages[1]); await sc.idle(3);
    const w = sc.where(O);
    ok(w.unplaced === 0 && w.pages.length === 1, `the stray piece is brought to its order: the order ends whole on ONE sheet ${JSON.stringify(w)} (${sc.report()})`);
    ok(!sc.pages[1].charms.some(c => String(c.order) === O.id) || w.pages[0] === 2, 'nothing of it is left on sheet 2 unless the whole order is there');
    ok(sc.pages[0].releaseFull, 'sheet 1 stays released');
    ok(said.some(x => /moved from this sheet to sheet 1, where the rest of the order is/.test(x.text)), 'the move is said in the log');
    section(`the net: a stray piece on an open sheet goes home to its order on the released sheet (whole on sheet ${w.pages[0]})`);
  }
  // a stray alone on a fresh sheet: its order comes to it (an empty sheet is never written for a stray)
  {
    const sc = new Scene('net pull'); await sc.fillTo(.40, .50);
    const O = sc.mk([{ kind: 'pair', idx: 3, f: .8 }]); await sc.bring(O);
    await singles(sc, 1, () => !!sc.pages[0].releaseFull, 900);
    const fresh = sc.m.addPage(); stray(sc, O, 5, fresh); await sc.idle(3);
    const w = sc.where(O);
    ok(w.unplaced === 0 && w.pages.length === 1 && w.pages[0] === fresh.page, `the order is brought whole to the fresh sheet that held only its stray: ${JSON.stringify(w)} (${sc.report()})`);
    ok(sc.pages[0].releaseFull && !sc.pages[0].charms.some(c => String(c.order) === O.id), 'sheet 1 gave its pieces of the order up and stays released');
    section('the net: a stray alone on a fresh sheet brings its whole order to it');
  }
  // never off a cut sheet: a split of necessity, said so
  {
    const sc = new Scene('net cut'), said = listen(sc); await sc.fillTo(.40, .50);
    const O = sc.mk([{ kind: 'pair', idx: 3, f: .8 }]); await sc.bring(O);
    await singles(sc, 1, () => !!sc.pages[0].releaseFull, 950); sc.pages[0].laserDoneAt = Date.now();
    await singles(sc, 2, () => sc.pages.length > 1 && sc.pages[1].placements.length >= 3, 980);
    stray(sc, O, 5, sc.pages[1]); await sc.idle(2);
    const w = sc.where(O);
    ok(w.pages.length === 2 && sc.pages[0].charms.filter(c => String(c.order) === O.id).length === 2, `a cut sheet gives nothing up and takes nothing: ${JSON.stringify(w)}`);
    ok(said.some(x => x.kind === 'warn' && /Split order .*split of necessity/.test(x.text)), 'the split of necessity is said in the log');
    section('the net: never off or onto a cut sheet (a split of necessity, said so)');
  }
}

/* ═══ 4 · the other paths that move a piece ═══ */
function paths() {
  // PAIR's follow: loose pieces of an order never leave its seated pieces of their colour
  {
    const sim = new Sim({ root: REPO, step: 60, seed: 1, sheet: '60x40' }), ctx = sim.ctx; sim.metal('gold'); sim.metal('silver');
    const run = { runId: 'run-1' }, mk = (metal, n, list, extra) => { const p = Object.assign({ metal, runId: 'run-1', page: n, sheetId: metal[0] + n, charms: list.map(x => x.c), placements: list.filter(x => x.placed).map(x => ({ id: x.c.id })), rejects: [], status: 'complete' }, extra || {}); ctx.S.sheets[metal].pages.push(p); return p; };
    const pc = (o, id, placed = true) => ({ c: { id, poolId: id, order: o }, placed });
    // X: GF piece moved on from GF1 to GF2 (overflow); its SS pieces: one seated on SS1, one loose on SS1; SS1 also shares order Y with GF1
    mk('gold', 1, [pc('Y', 'y1')]); mk('gold', 2, [pc('X', 'xg', false)]);
    const s1 = mk('silver', 1, [pc('Y', 'ys'), pc('X', 'xs1'), pc('X', 'xs2', false)]);
    const moved = ctx.follow(['X'], run, { except: 'gold' });
    ok(moved.length === 0 && s1.charms.filter(c => c.order === 'X').length === 2, `a loose piece of an order with a piece of its colour seated on the same sheet stays with it (moved: ${JSON.stringify(moved.map(m => m.pieces))})`);
    // the same with the seated piece on ANOTHER SS sheet: the loose one goes to it
    s1.charms = s1.charms.filter(c => c.id !== 'xs1'); s1.placements = s1.placements.filter(p => p.id !== 'xs1');
    const s2 = mk('silver', 2, [pc('X', 'xs1'), pc('W', 'w1')]);
    const moved2 = ctx.follow(['X'], run, { except: 'gold' });
    ok(moved2.length === 1 && moved2[0].to === s2 && s2.charms.some(c => c.id === 'xs2') && !s1.charms.some(c => c.order === 'X'), 'a loose piece whose order is seated on another sheet of its colour goes to that sheet');
    section('PAIR\'s follow: loose pieces never leave their order\'s seated pieces; they go to them');
  }
  // INPLACE's next sheet: the order's own sheet first (released, it is kept released), the latest unfinished sheet only for an order with no other piece of this colour on a sheet
  {
    const sim = new Sim({ root: REPO, step: 60, seed: 1, sheet: '60x40' }), ctx = sim.ctx; sim.metal('gold');
    const bridge = fs.readFileSync(path.join(REPO, 'charm-nest-bridge.js'), 'utf8'), a = bridge.indexOf('  function inPlaceTarget('), b = bridge.indexOf('  /** A replaced line\'s new pieces of one metal', a);
    ok(a > 0 && b > a, 'the bridge has inPlaceTarget');
    ctx.Gate = { modern: () => true, holding: () => false }; ctx.orderOfCharm = c => String(c && c.order != null ? c.order : '').split('/')[0];
    ctx.window.LiveNest = { orderHome: ctx.orderHome, reopenHome: ctx.reopenHome, closed: ctx.closed, lineFits: ctx.lineFits };
    vm.runInContext(bridge.slice(a, b) + '\nthis.inPlaceTarget = inPlaceTarget;', ctx);
    const run = { runId: 'run-1' }, G = ctx.S.sheets.gold.pages;
    const p1 = { metal: 'gold', runId: 'run-1', page: 1, sheetId: 'g1', charms: [{ id: 'x1', poolId: 'x1', order: 'X' }, { id: 'q1', poolId: 'q1', order: 'Q' }], placements: [{ id: 'x1' }, { id: 'q1' }], rejects: [], status: 'complete', releaseFull: true, _inplaceHold: { at: 1 } };
    const p2 = { metal: 'gold', runId: 'run-1', page: 2, sheetId: 'g2', charms: [{ id: 'r1', poolId: 'r1', order: 'R' }], placements: [{ id: 'r1' }], rejects: [], status: 'complete' };
    G.push(p1, p2);
    const fresh = [{ id: 'x9', poolId: 'x9', order: 'X' }];
    eq(ctx.inPlaceTarget('gold', run, fresh, { pieces: fresh, expected: 2, placed: 1 }), p1, 'a new piece of order X goes to the sheet the rest of X is on (its old spot\'s sheet), not the latest unfinished one');
    ok(p1.keepRelease && p1.keepRelease.full && p1._inplaceHold, 'that released sheet is kept released while it nests, and its INPLACE hold is left as it was');
    const lone = [{ id: 'z9', poolId: 'z9', order: 'Z' }];
    eq(ctx.inPlaceTarget('gold', run, lone, { pieces: lone, expected: 1, placed: 0 }), p2, 'an order with no other piece of this colour on a sheet goes to the latest unfinished sheet, as before');
    section('INPLACE\'s next sheet: the order\'s own sheet first');
  }
}

/* ═══ 5 · a sandbox-like day ═══ */
/** One day: both colours, the live timing (the next update's intake runs while a search is out; one search per colour finishes per update), long late lines, bulk updates, a big order. */
async function liveDay(o) {
  const root = o.root || REPO, S = L.solverOf(root), mix = Object.assign({}, NL.MIXES.heavy, { late: o.late, otherMetal: o.mixed });
  let stream = makeStream(S, { seed: o.seed, orders: o.orders, mix });
  const r = L.rng(o.seed * 31 + 7);
  for (const ord of stream) for (const l of ord.lines) if (l.delay) l.delay = o.minDelay + Math.floor(r() * (o.maxDelay - o.minDelay + 1));   // (held for a question or its engraving, read late, released from hold: many updates later)
  stream = NL.withBig(S, stream, o.big || [], o.seed);
  const sheet = NL.sheetFor(S, stream, ['gold', 'silver'], 45), sim = new Sim({ root, step: 60, seed: o.seed, sheet });
  const fake = NL.standIn(L.solverOf(root), { smallPt2: NL.smallArea(stream), leak: o.leak || 0, maxOrder: 0, rng: L.rng(o.seed * 17 + 3) });
  Object.assign(sim.ctx.S.settings, o.settings || {});
  const metals = ['gold', 'silver'], live = {}, moves = { reopen: 0, homeward: 0 };
  if (typeof sim.ctx.reopenHome === 'function') { const f = sim.ctx.reopenHome; sim.ctx.reopenHome = (p, rid) => { const x = f(p, rid); if (x) moves.reopen++; return x; }; }
  if (typeof sim.ctx.homeward === 'function') { const f = sim.ctx.homeward; sim.ctx.homeward = sh => { const x = f(sh); moves.homeward += x.length; return x; }; }
  for (const m of metals) { const ms = sim.metal(m), st = live[m] = { gate: null, open: null, inflight: null }; const Sx = Object.create(fake); Sx.solve = async (job, x) => { const res = await fake.solve(job, x); if (st.gate) await st.gate; return res; }; ms.S = Sx; }
  const expectedOf = (ord, m) => ord.lines.filter(l => l.metal === m).reduce((n, l) => n + l.pieces.length, 0), seen = new Map();
  const settle = async m => { const st = live[m]; if (st.inflight) { st.open(); await st.inflight; st.inflight = null; st.gate = null; } };
  const nestOne = async (ms, sh) => { sh.status = 'nesting'; await ms.nest(sh); };
  for (const ev of schedule(stream, o.batches, 2)) {
    for (const m of metals) sim.metal(m).intake(ev.arrivals.map(a => { const p = partOf(a.order, m, a.lines); if (p) { const k = a.order.id + '|' + m, before = seen.get(k) || 0; seen.set(k, before + p.pieces.length); p.expected = expectedOf(a.order, m); p.order = { pieces: p.pieces, expected: p.expected, placed: before }; } return p; }).filter(Boolean), ev.tick);
    for (const m of metals) {
      const ms = sim.metal(m), st = live[m];
      await settle(m);
      if (typeof sim.ctx.nospreadSettle === 'function') sim.ctx.nospreadSettle();
      if (ms.work.length) { const sh = ms.work.shift(); st.gate = new Promise(res => { st.open = res; }); st.inflight = nestOne(ms, sh); }   // (still searching when the next update comes in)
    }
  }
  for (const m of metals) await settle(m);
  for (let k = 0; k < 4; k++) { if (typeof sim.ctx.nospreadSettle === 'function') sim.ctx.nospreadSettle(); for (const m of metals) { const ms = sim.metal(m); for (let i = 0; i < 500 && ms.work.length; i++) await ms.nest(ms.work.shift()); } }
  const res = NL.measure(sim, stream, metals, { seed: o.seed });
  // the app's own checker over the day's sheets: R16 warnings (an order bigger than a sheet fills consecutive sheets by design and is not counted)
  const I = require(path.join(root, 'charm-nest-set-invariants.js')), bigs = new Set(stream.filter(x => x.big).map(x => x.id));
  const pages = sim.pages.filter(p => p.placements.length).map(p => ({ id: p.metal + '#' + p.page, metal: p.metal, page: p.page, releaseFull: !!p.releaseFull, charms: p.charms.map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: p.placements.map(q => ({ id: q.id })) }));
  res.r16 = I.check([], pages, {}).warnings.filter(w => w.rule === 'orderSplit' && !bigs.has(String(w.id).split(':')[1]));
  res.moves = moves; res.problems = sim.pages.filter(p => p.problem).map(p => p.problem);
  return res;
}
async function day() {
  const seeds = QUICK ? [1] : [1, 2, 3], base = { orders: QUICK ? 260 : 360, late: .4, mixed: .08, minDelay: 3, maxDelay: 60, batches: [2, 3, 5, 4, 20], big: [{ pieces: 120, metal: 'gold', design: [10, 1], at: .5 }] };
  const rows = [];
  for (const seed of seeds) {
    const o = Object.assign({ seed, root: ROOT, leak: seed === 2 ? .3 : 0 }, base), on = await liveDay(o), off = DAY_ONLY ? null : await liveDay(Object.assign({}, o, { settings: { nospread: 'off' } }));
    rows.push({ seed, on, off });
    console.log(`    seed ${seed}${o.leak ? ' (a leaky packer)' : ''}: ${on.orders} orders, ${on.partsMulti} multi-piece parts, ${on.sheets} sheets (${on.closed} released, GF+SS fill ${(100 * on.fillClosedGfss).toFixed(1)}%) | same-colour splits ${on.multiOrdersSplitNoBig} (checker R16: ${on.r16.length}), unit of a sheet (one colour) ${on.same.perSheetMean}, any colour ${on.all.perSheetMean} | late pieces sent to their order's released sheet ${on.moves.reopen}, by the net ${on.moves.homeward}${off ? ` | the page without NOSPREAD: ${off.multiOrdersSplitNoBig} splits, unit of a sheet ${off.all.perSheetMean}` : ''}`);
    if (DAY_ONLY) continue;
    ok(on.multiOrdersSplitNoBig === 0, `seed ${seed}: no multi-piece order on two sheets of one colour: ${JSON.stringify(on.splitList.slice(0, 5))}`);
    ok(on.r16.length === 0, `seed ${seed}: the app's checker finds no R16 split: ${on.r16.slice(0, 3).map(w => w.message).join(' | ')}`);
    ok(on.pairLinesSplit === 0, `seed ${seed}: no earring pair on two sheets`);
    ok(on.unplaced === 0 && !on.problems.length, `seed ${seed}: every piece placed (${on.unplaced} not), no sheet stuck (${on.problems.join(' | ')})`);
    ok(on.big.every(b => b.miss === 0 && b.sheets <= b.fewest + 1), `seed ${seed}: the order bigger than a sheet fills consecutive sheets, the fewest possible: ${JSON.stringify(on.big)}`);
    ok(on.fillClosedGfss >= .75, `seed ${seed}: released Gold and Silver sheets keep the release fill (${(100 * on.fillClosedGfss).toFixed(1)}%)`);
  }
  if (!DAY_ONLY) ok(rows.reduce((n, x) => n + x.off.multiOrdersSplitNoBig, 0) > 0, 'for the record: on the same days the page without NOSPREAD puts multi-piece orders on two sheets of one colour (the days exercise the fault)');
}

function files() {
  const bridge = fs.readFileSync(path.join(REPO, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(REPO, 'charm-nest-1.html'), 'utf8'), doc = fs.readFileSync(path.join(REPO, 'docs/charm-nest/set-rules.md'), 'utf8');
  ok(/return \{ add, finish, prepareSheet, closed, intakePage, pairFits, lineFits, follow, orderHome, reopensHome, reopenHome \}/.test(bridge), 'LiveNest hands out orderHome, reopensHome and reopenHome');
  ok(/LN16\.orderHome\(metal, run, rid16, \{ skip: list \}\)/.test(bridge), 'Pool.attachPool asks for the order\'s home after every other choice');
  ok(/function homeward\(sh\)/.test(html) && /homeward\(sh\);\n\}/.test(html), 'keepOrdersWhole ends with the net (homeward) at every nest');
  ok(/R16 .*HARD/.test(doc), 'docs/charm-nest/set-rules.md states the hard rule');
}

module.exports = { liveDay };
if (require.main === module) (async () => {
  const t0 = Date.now();
  console.log('R16 HARD: a multi-piece order on ONE sheet of its colour');
  if (!DAY_ONLY) { functions(); console.log('  · scenes through the page\'s own code'); await scenes(); await net(); paths(); }
  console.log('  · a sandbox-like day (live timing, long late lines, bulk updates, a big order)'); await day();
  if (!DAY_ONLY) files();
  console.log(DAY_ONLY ? `DONE: the day's numbers for ${ROOT}` : `PASS: nospread-hard: ${checks} checks (${Math.round((Date.now() - t0) / 1000)}s; fake orders, a stand-in packer, offline)`);
})().catch(e => { console.error('FAIL: ' + (e && e.stack || e)); process.exit(1); });
