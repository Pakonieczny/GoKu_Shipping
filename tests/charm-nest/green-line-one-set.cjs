// Paul, 26 Oct 2026, the live Set 1 ("Set-1 · 2026-10-26": GF Sheet 1 and 2 full, RG Sheet 1 and 2 partial, SS Sheet 1 full): "How am I supposed to have two partial rose gold sheets here???
// Why are there two full GF sheets when I gave you the rules for that?"
// The causes, each reproduced here with the page's REAL code (the intake, the overflow, the set assembly, the checker), and what the code does now:
//   1  PAIR · the intake sent each piece to the earliest open sheet of its metal: SS Sheet 1, filling its gaps, took the SS piece of order 4200987085 whose GF piece was on GF Sheet 2. A GF / SS sheet now
//      takes a piece of an order only when the order's other GF / SS pieces are on its ONE partner of the other metal (or it has none yet): LiveNest.pairFits, in intakePage, attachPool, the overflow
//      (with the pieces still waiting on the other metal's sheets following, LiveNest.follow) and greenFits (two draft partners only when they are each other's pair).
//   2  ONE SET · a Rose Gold / 10K / 14K sheet never ties two GF / SS units together, cut or uncut: an RG sheet filled before the intake rule, with orders of GF Sheet 1 and GF Sheet 2, chained GF Sheet 2
//      into Set 1 once it was ticked or cut. It belongs to ONE set (its partners' earliest-formed set); its orders with the others are a known split, said plainly, never a violation, a hold or a pull.
//      A set that already holds the surplus unit is corrected by the assembly itself: the unit leaves with its own green-line sheet (Decision 1's trim).
//   3  UNPAIR · the five-sheet state as it is live (order 4200987085 ties GF Sheet 2 to SS Sheet 1 DIRECTLY, a GF + SS order): the assembly moves that order's SS piece off SS Sheet 1 to SS Sheet 2
//      (the sheet window's own move, SheetWin.holdKit.move), then the trim takes GF Sheet 2 out. Set 1 = GF Sheet 1 + SS Sheet 1 (+ RG Sheet 2, whose cut is recorded in it: a cut sheet never moves, and
//      it holds nothing, its order with GF Sheet 2 is a known split; an uncut one leaves with its GF sheet, part 2).
//   4  AUTOCUT · the automatic Cut Sheet put RG Sheet 1 in the set first, the server refused the cut (an order waited for its own design), the include stayed: the include is undone on a refusal (the
//      sheet waits outside, its card says why in the server's words) and it is cut and joins once the server accepts.
//   5  RED LINE · a green-line sheet whose back engraving is open is what the set's Approve waits for: one plain waiting line, never "breaks your rules" with Rebuild.
//   node tests/charm-nest/green-line-one-set.cjs
'use strict';
process.env.SETSIM_QUIET = '1';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const { world, I } = require('./audit-cardinal-lib.cjs');
const { Sim } = require('./setsim-lib.cjs');
const SO = require('../../charm-nest-shared-orders.js'), SRp = require('../../charm-nest-set-repair.js'), RD = require('../../charm-nest-readiness.js');
const code = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const ok = (name, cond, detail) => { assert(cond, name + (detail ? ' :: ' + detail : '')); console.log('  ok  ' + name); };
const settle = async (w, n = 40) => { for (let i = 0; i < n; i++) await new Promise(r => setTimeout(r, 5)); };
const A = '4202194362', B = '4200987085', C = '4200871209';    // the three orders of the live Set 1: A on GF 1 + SS 1 + RG 1, B on GF 2 + SS 1, C on GF 2 + RG 2

/** the page's own intake (LiveNest.intakePage, closed, the gap fill's topupRoom / topupTakes) in setsim's page world, greenFits sliced in */
function intakeWorld() {
  const sim = new Sim({ root }), ctx = sim.ctx;
  const a = code.indexOf('  const lineCuts = m =>'), b = code.indexOf('    return true;\n  }', code.indexOf('  function greenFits(', a)) + '    return true;\n  }'.length;
  vm.runInContext(code.slice(a, b) + '\nthis.greenFits = greenFits; this.partnerSheets = partnerSheets;', ctx);
  const pc = (id, metal) => ({ id, poolId: id, order: id.split('_')[0], metal, areaPt2: 300, widthPt: 20, heightPt: 20, index: 0 });
  const place = (p, c) => { p.charms.push(c); p.placements.push({ id: c.id, cxPt: 10, cyPt: 10, angle: 0 }); return c; };
  const run = { runId: 'run-1' };
  const intake = (metal, orderId, line) => { const c = pc(`${orderId}_${line}_1`, metal), page = ctx.intakePage(metal, run, orderId, [c], { pieces: [c], expected: 1, placed: 0 }); place(page, c); return page; };
  return { sim, ctx, pc, place, intake, run, G: sim.metal('gold'), S: sim.metal('silver'), R: sim.metal('rose') };
}

(async () => {
  /* ═══ 1 · PAIR: the intake ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  {
    // the live sequence: GF 1 and SS 1 fill together (order A on both); ~03:18 GF 1 closes and GF 2 opens; ~03:27 SS 1 starts its gap fill and SS 2 opens; order B (GF + SS) arrives
    const { ctx, pc, place, intake, G, S } = intakeWorld();
    const gf1 = G.addPage(), ss1 = S.addPage();
    for (let i = 0; i < 38; i++) { place(gf1, pc(`41000${i}_1_1`, 'gold')); place(ss1, pc(`42000${i}_1_1`, 'silver')); }
    intake('gold', A, 1); intake('silver', A, 2);
    gf1.releaseFull = true; const gf2 = G.addPage(); place(gf2, pc('4300000_1_1', 'gold'));
    ss1.topup = { at: 0, base: .76, tried: [], placed: ss1.placements.length }; ss1.liveInfo = { usablePt2: 1e4, placedPt2: 0, freePt2: 1e4 };
    const ss2 = S.addPage(); place(ss2, pc('4400000_1_1', 'silver'));
    ok('SS 1 is filling its gaps and has room: the earliest open SS sheet', ctx.topupRoom(ss1) > 0 && ctx.closed(ss1) === false);
    const onGF = intake('gold', B, 1), onSS = intake('silver', B, 2);
    ok('order B: its GF piece goes to GF 2 (GF 1 is closed)', onGF === gf2);
    ok('and its SS piece to SS 2, the sheet that pairs with GF 2, not to SS 1 (paired with GF 1): one GF sheet and one SS sheet share an order', onSS === ss2, 'SS page ' + onSS.page);
    ok('a single-metal order still goes to the earliest open sheet with room (SS 1 filling its gaps)', intake('silver', '4500001', 1) === ss1);
    // the "will go to" side: GF 1 filling its gaps while its partner SS 1 is closed: an order whose SS line is still to come does not go on GF 1 (its SS piece could not go to SS 1)
    const w2 = intakeWorld(), g1 = w2.G.addPage(), s1 = w2.S.addPage(); for (let i = 0; i < 20; i++) { w2.place(g1, w2.pc(`51000${i}_1_1`, 'gold')); w2.place(s1, w2.pc(`52000${i}_1_1`, 'silver')); }
    w2.intake('gold', '5300001', 1); w2.intake('silver', '5300001', 2); s1.releaseFull = true; g1.topup = { at: 0, base: .76, tried: [], placed: g1.placements.length }; g1.liveInfo = { usablePt2: 1e4, placedPt2: 0, freePt2: 1e4 };
    const g2 = w2.G.addPage(); w2.place(g2, w2.pc('5400000_1_1', 'gold'));
    w2.ctx.Orders = { rows: () => [{ order: { receiptId: '5300009' }, spec: { material: 'silver' }, state: 'pooled', poolIds: ['5300009_2_1'] }] };
    ok('an order whose SS line is still to come does not go on GF 1 while its partner SS 1 takes nothing more', w2.intake('gold', '5300009', 1) === g2);
    ok('one with no SS line goes on GF 1, filling its gaps', w2.intake('gold', '5300010', 1) === g1);
  }
  {
    // greenFits with draft partners: two GF partners are refused; a GF and an SS partner in no set yet only when they are each other's pair (one unit, one set)
    const { ctx, pc, place, G, S, R } = intakeWorld(), run = { runId: 'run-1' };
    const gf1 = G.addPage(), gf2 = G.addPage(), ss1 = S.addPage(), ss2 = S.addPage(), ss3 = S.addPage(), rg = R.addPage();
    place(gf1, pc('6001_1_1', 'gold')); place(gf2, pc('6002_1_1', 'gold')); place(ss1, pc('6001_2_1', 'silver')); place(ss2, pc('6003_2_1', 'silver')); place(gf2, pc('6004_1_1', 'gold')); place(ss3, pc('6004_2_1', 'silver'));
    place(rg, pc('6001_3_1', 'rose'));
    ok('an RG sheet with an order of GF 1 (a draft) does not take one of GF 2 (a draft): two GF partners', !ctx.greenFits(rg, '6002', run));
    ok('it takes another order of GF 1 / SS 1, the pair GF 1 shares an order with', ctx.greenFits(rg, '6001', run));
    const rg2 = R.addPage(); place(rg2, pc('6002_3_1', 'rose'));
    ok('a GF draft and an SS draft that share no order are refused (they could end in two sets)', !ctx.greenFits(rg2, '6003', run));
    ok('the SS sheet that IS GF 2\'s pair is taken', ctx.greenFits(rg2, '6004', run));
  }
  {
    // the overflow: an RG line approved with the order's GF piece on GF 1; GF 1's nest turns that GF piece away and passes it on: the RG piece, not placed yet, follows to an RG sheet that keeps the rule
    const { ctx, pc, place, G, R } = intakeWorld();
    const gf1 = G.addPage(), rg1 = R.addPage();
    for (const o of ['9001', '9002', '9003']) place(gf1, pc(`${o}_1_1`, 'gold'));
    const x = pc('9009_1_1', 'gold'); gf1.charms.push(x); place(rg1, pc('9001_2_1', 'rose'));
    ok('greenFits says yes while the GF piece is on GF 1', ctx.greenFits(rg1, '9009', { runId: 'run-1' }));
    rg1.charms.push(pc('9009_2_1', 'rose'));
    gf1.rejects = [x.id]; gf1.verification = { ok: true };
    const next = ctx.overflowToNextSheet(gf1, false), part = ctx.partnerSheets(rg1.charms.map(c => c.order), { runId: 'run-1' });
    ok('the GF piece moved to the next GF sheet', next && next.charms.includes(x));
    ok('RG 1 keeps ONE GF partner: the RG piece of the moved order followed it', part.gold.size === 1 && !rg1.charms.some(c => c.order === '9009') && R.pages.some(p => p !== rg1 && p.charms.some(c => c.order === '9009')), JSON.stringify(R.pages.map(p => p.charms.map(c => c.order))));
    // and an SS piece not placed yet follows a GF piece that moved
    const w3 = intakeWorld(), a1 = w3.G.addPage(), b1 = w3.S.addPage(), a2 = w3.G.addPage(), b2 = w3.S.addPage();
    for (const o of ['9101', '9102']) w3.place(a1, w3.pc(`${o}_1_1`, 'gold')); w3.place(b1, w3.pc('9101_2_1', 'silver')); w3.place(a2, w3.pc('9201_1_1', 'gold')); w3.place(b2, w3.pc('9201_2_1', 'silver'));
    const y = w3.pc('9109_1_1', 'gold'); a1.charms.push(y); b1.charms.push(w3.pc('9109_2_1', 'silver'));
    a1.rejects = [y.id]; a1.verification = { ok: true }; const n2 = w3.ctx.overflowToNextSheet(a1, false);
    ok('the GF piece of order 9109 moved on to GF 2, and its SS piece (not placed yet) left SS 1 for SS 2, GF 2\'s pair', n2 === a2 && b2.charms.some(c => c.order === '9109') && !b1.charms.some(c => c.order === '9109'));
  }

  /* ═══ 2 · ONE SET: a green-line sheet ties no two units ══════════════════════════════════════════════════════════════════════════════════════════ */
  const base = { verification: { ok: true }, persistedDone: true, status: 'complete' };
  const recStub = (w, o = {}) => {   // RoseStock.record as it behaves: a sheet outside a set is included first (its own tick), then the server records the cut, or refuses it
    w.calls = [];
    w.c.__rec = async sh => { w.calls.push(sh.sheetId); if (!(sh.setId && !sh.draft)) { sh.solidPick = true; await vm.runInContext('Gate.assemble(B.run)', w.c); } if (w.refuse) throw new Error('Not cut yet: ' + w.refuse); sh.roseCutAt = Date.now(); await vm.runInContext('Gate.assemble(B.run)', w.c); };
    vm.runInContext('window.RoseStock = { record: (sh) => __rec(sh) }; var stampWho = () => ({ by: "Test" }); var pagesOf = m => pages.filter(p => p.metal === m);', w.c);
    w.refuse = o.refuse || null;
  };
  const chain = w => {   // the task's chain: order 100 on GF 1 + SS 1; RG 1 (filled before the intake rule) has order 200 of GF 1 and order 300 of GF 2; RG 2 has order 400 of GF 2
    const gf1 = w.full(w.page('gold', base)), ss1 = w.full(w.page('silver', base)), gf2 = w.full(w.page('gold', base));
    const rg1 = w.page('rose', Object.assign({ density: .32 }, base)), rg2 = w.page('rose', Object.assign({ density: .07 }, base));
    w.put(gf1, '100_1_1', '200_1_1'); w.put(ss1, '100_2_1'); w.put(gf2, '300_1_1', '400_1_1'); w.put(rg1, '200_2_1', '300_2_1'); w.put(rg2, '400_2_1');
    return { gf1, ss1, gf2, rg1, rg2 };
  };
  const formAll = (w, list, setId = 'set-live') => {   // a set that already holds them all, formed and uncommitted (as the live Set 1)
    const s = vm.runInContext(`(() => { const s = { runId: RUN, group: 'dispatch', seq: 1, day: today(), setId: '${setId}', name: 'Set-1', sheetIds: [], labelFiles: [], materials: [], orders: {}, sheetNos: {}, formedAt: 1 }; B.sets.set(s.setId, s); return s; })()`, w.c);
    list.forEach((p, k) => { Object.assign(p, { draft: false, setId, seq: 1, sheetIndex: 1 + k, joinedAt: 1 }); s.sheetIds.push(p.sheetId); });
    return s;
  };
  {
    // 2a · the set has formed already with all five (RG 1 and RG 2 ticked, uncut): the next look corrects it by itself
    const w = world(); recStub(w, { refuse: 'its production checks are not done' }); const s = chain(w); s.rg1.solidPick = true; s.rg2.solidPick = true;
    formAll(w, [s.gf1, s.ss1, s.gf2, s.rg1, s.rg2]); s.rg1.autoMatched = 'set-live';
    const before = I.check(w.input().sets, w.input().sheets, { skipExempt: true });
    ok('the checker now calls the five-sheet set bigger than the minimum: GF 2 and its RG 2 are not needed', before.violations.some(v => v.rule === 'surplus' && v.unit.includes('GF2') && v.unit.includes('RG2') && !v.unit.includes('RG1')), I.explain(before).join(' | '));
    await w.assemble(); await settle(w);
    const set1 = w.sets()[0], m = w.members(set1).map(p => p.sheetId).sort();
    ok('Set 1 = GF 1 + SS 1 (+ RG 1 once its cut is accepted): GF 2 and RG 2 left it by themselves', ['GF1', 'SS1'].every(x => m.includes(x)) && !m.includes('GF2') && !m.includes('RG2'), w.word());
    ok('RG 1 is not left in the set uncut: its automatic cut was refused, so it waits outside, and its card says why', !w.inSet(s.rg1) && /production checks are not done/.test(s.rg1.autoCutWait || ''), (s.rg1.autoCutWait || '') + ' ' + w.word());
    ok('GF 2 waits for an SS sheet of its own; RG 2 waits for GF 2\'s set', !w.inSet(s.gf2) && !w.inSet(s.rg2) && /in no set yet/.test(s.rg2.cardinalHold || ''), s.rg2.cardinalHold);
    const after = w.selfCheck();
    ok('the checker reports no violation for the RG split', after.ok, (after.lines || []).join(' | '));
    ok('the split is said in a plain line: RG 1 belongs to Set 1, order 300 with GF 2 ships in two parts', (after.splits || []).some(x => x.sheet === 'RG1' && x.orders.includes('300') && /ships in two parts/.test(x.message)), JSON.stringify(after.splits));
    ok('Set 1 waits for RG 1 as the partner still to come (R10: Approve grey), never for GF 2', after.partners.some(p => p.sheet === 'RG1' && p.set === set1.setId) && !after.partners.some(p => p.sheet === 'GF2'));
    // the server accepts the cut now: RG 1 is cut and joins Set 1
    w.refuse = null; s.rg1.jobId = 'j2'; vm.runInContext('Gate.autoCut(B.run)', w.c); await settle(w);
    ok('once the server accepts the cut, RG 1 is cut and in Set 1, with GF 1 and SS 1', w.inSet(s.rg1) && s.rg1.setId === set1.setId && !!s.rg1.roseCutAt, w.word());
    ok('and GF 2 is still not pulled in by it', !w.inSet(s.gf2), w.word());
  }
  {
    // 2b · the set has not formed yet: the same sheets form Set 1 = GF 1 + SS 1, RG 1 joins it by its automatic cut, GF 2 and RG 2 wait
    const w = world(); recStub(w); const s = chain(w);
    await w.assemble(); await settle(w);
    const set1 = w.sets()[0];
    ok('Set 1 = GF 1 + SS 1 + RG 1 (cut automatically): the earliest set of RG 1\'s partners', set1 && ['GF1', 'SS1', 'RG1'].every(id => w.members(set1).some(p => p.sheetId === id)) && !!s.rg1.roseCutAt, w.word());
    ok('GF 2 is not in Set 1 (RG 1 ties nothing): it waits for an SS sheet of its own', !w.inSet(s.gf2) && /needs a completed SS sheet/.test(s.gf2.setWait || ''), s.gf2.setWait);
    ok('RG 2 is not cut and not in a set: its one partner GF 2 is in none', !w.inSet(s.rg2) && !s.rg2.roseCutAt && !w.calls.includes('RG2'));
    const r = w.selfCheck(); ok('the checker accepts it, the split said plainly', r.ok && (r.splits || []).some(x => x.sheet === 'RG1'), (r.lines || []).join(' | '));
    // a second SS sheet comes: GF 2 forms Set 2 with it, and RG 2 belongs there (never to Set 1)
    const ss2 = w.full(w.page('silver', base)); w.put(ss2, '700_1_1'); await w.assemble(); await settle(w);
    const set2 = w.sets().find(x => x.setId !== set1.setId);
    ok('GF 2 + SS 2 form Set 2, RG 2 joins Set 2 by its automatic cut', set2 && w.inSet(s.gf2) && s.gf2.setId === set2.setId && w.inSet(s.rg2) && s.rg2.setId === set2.setId, w.word());
    ok('Set 1 is untouched', ['GF1', 'SS1', 'RG1'].every(id => w.by(id).setId === set1.setId), w.word());
  }
  {
    // 2c · the shared helpers the page, the checker and the server read: greenHomes and realPartners
    const sh = (id, metal, setId, keys) => ({ id, label: id, metal, setId, fixed: '', pieces: keys.map(k => ({ key: k, orderId: k.split('_')[0] })) });
    const all = [sh('gf1', 'gold', 's1', ['1_1_1', '2_1_1']), sh('ss1', 'silver', 's1', ['1_2_1']), sh('gf2', 'gold', 's2', ['3_1_1']), sh('ss2', 'silver', 's2', ['3_2_1']), sh('rg', 'rose', null, ['2_2_1', '3_3_1'])];
    const H = SO.core.greenHomes(all, { rank: sid => ({ s1: 1, s2: 2 })[sid] });
    ok('greenHomes: an RG sheet with partners in Set 1 and Set 2 belongs to the earliest, Set 1, and its order with Set 2 is a known split', H.get('rg').home === 's1' && H.get('rg').split.map(x => x.sheet).sort().join() === 'gf2,ss2' && H.get('rg').split.every(x => x.orders.join() === '3'), JSON.stringify(H.get('rg')));
    all[4].matched = 's2'; ok('the set the system matched it to wins while it holds a partner', SO.core.greenHomes(all, { rank: sid => ({ s1: 1, s2: 2 })[sid] }).get('rg').home === 's2'); delete all[4].matched;
    const items = SO.core.between(all, ['gf2', 'ss2'], 's2'), real = SO.core.realPartners(all, items, 's2', H);
    ok('realPartners: Set 2 does not wait for the RG sheet of Set 1', items.length === 1 && real.length === 0 && real.split.length === 1);
  }

  /* ═══ 3 · UNPAIR: the five-sheet state as it is live ═════════════════════════════════════════════════════════════════════════════════════════════ */
  {
    const w = world(); recStub(w, { refuse: `order 4199104309: waiting for its own design` });
    const gf1 = w.full(w.page('gold', base)), ss1 = w.full(w.page('silver', base)), gf2 = w.full(w.page('gold', base));
    const rg1 = w.page('rose', Object.assign({ density: .32 }, base)), rg2 = w.page('rose', Object.assign({ density: .07 }, base)), ss2 = w.page('silver', Object.assign({ density: .3 }, base));
    w.put(gf1, `${A}_1_1`, '8100_1_1'); w.put(ss1, `${A}_2_1`, `${B}_2_1`, '8200_1_1'); w.put(rg1, `${A}_3_1`); w.put(gf2, `${B}_1_1`, `${C}_1_1`); w.put(rg2, `${C}_2_1`); w.put(ss2, '8300_1_1');
    // (as the records are: RG 1 in Set 1 uncut, ticked by the automatic press whose cut the server refused; RG 2 in Set 1 with its cut recorded)
    rg1.solidPick = true; rg2.solidPick = true; rg2.roseCutAt = 1792985938012; const set = formAll(w, [gf1, ss1, gf2, rg1, rg2]);
    const pre = I.check(w.input().sets, w.input().sheets, { skipExempt: true });
    ok('as it is live, GF 2 is tied to SS 1 DIRECTLY by order B (a GF + SS order): one unit by the cardinal rule, so no rule can call it spare', !pre.violations.some(v => v.rule === 'surplus'), I.explain(pre).join(' | '));
    ok('the plan: order B\'s SS piece leaves SS 1 for SS 2, the open SS sheet that keeps the pairing', (() => { const p = vm.runInContext('Gate.unpairPlan(B.run)', w.c); return p.length === 1 && p[0].from === ss1 && p[0].to === ss2 && p[0].orders[0] === B && p[0].second === gf2; })());
    const moves = [];
    w.c.__move = async (k, target, who, o) => {   // the sheet window's move, as it ends: the pieces off the sheet they were on (its other charms stay), onto the target, an assembly after (remakeLabels)
      moves.push({ rid: k.rid, from: k.pieces[0].src.sheetId, to: target.sheetId, who, guard: o.guard(k.pieces[0].src) });
      for (const z of k.pieces) { z.src.charms = z.src.charms.filter(c => c !== z.c); z.src.placements = z.src.placements.filter(p => p.id !== z.c.id); target.charms.push(z.c); target.placements.push({ id: z.c.id }); }
      await vm.runInContext('Gate.assemble(B.run)', w.c); return true;
    };
    vm.runInContext('window.SheetWin = { holdKit: { busy: () => false, move: (k, t, who, o) => __move(k, t, who, o) } }; var SheetWin = window.SheetWin;', w.c);
    await w.assemble(); await settle(w, 60);
    ok('the assembly moved order B\'s SS piece from SS 1 to SS 2 by itself, with the sheet window\'s move (automatic, nothing cut or approved in the way)', moves.length === 1 && moves[0].rid === B && moves[0].from === 'SS1' && moves[0].to === 'SS2' && moves[0].guard === '' && /automatic/.test(moves[0].who), JSON.stringify(moves));
    const m = w.members(set).map(p => p.sheetId).sort();
    ok('then the trim took GF 2 out: Set 1 = GF 1 + SS 1 (+ RG 2, whose cut is recorded there: a cut sheet never moves)', m.join() === 'GF1,RG2,SS1', w.word());
    ok('RG 1, uncut, is not left in the set: its automatic cut was refused (order 4199104309 waits for its own design) and its card says so', !w.inSet(rg1) && /order 4199104309: waiting for its own design/.test(rg1.autoCutWait || ''), rg1.autoCutWait);
    ok('GF 2 waits with SS 2 until SS 2 is full (they share order B now)', !w.inSet(gf2) && /not full yet/.test(gf2.cardinalHold || ''), gf2.cardinalHold);
    const r = w.selfCheck();
    ok('RG 2 holds nothing: its order C with GF 2 is a known split, said plainly', (r.splits || []).some(x => x.sheet === 'RG2' && x.orders.includes(C) && /ships in two parts/.test(x.message)) && /ships in two parts/.test(gf2.cardinalNote || rg2.cardinalNote || ''), JSON.stringify(r.splits));
    ok('the checker: no violation; Set 1 waits for RG 1 (its partner still to come), not approved meanwhile', r.ok && r.partners.some(p => p.sheet === 'RG1' && p.set === set.setId) && r.sets.find(x => x.id === set.setId).canApprove === false, (r.lines || []).join(' | '));
    ok('the move is done once: a later look plans nothing', vm.runInContext('Gate.unpairPlan(B.run)', w.c).length === 0);
    // never a set approved for laser cutting, committed, or a sheet that is cut
    const w2 = world(); const g1 = w2.full(w2.page('gold', base)), s1 = w2.full(w2.page('silver', base)), g2 = w2.full(w2.page('gold', base)), s2 = w2.page('silver', Object.assign({ density: .3 }, base));
    w2.put(g1, `${A}_1_1`); w2.put(s1, `${A}_2_1`, `${B}_2_1`); w2.put(g2, `${B}_1_1`); w2.put(s2, '8300_1_1'); vm.runInContext('var pagesOf = m => pages.filter(p => p.metal === m);', w2.c);
    const st2 = formAll(w2, [g1, s1, g2]);
    st2.processReady = true; ok('a set approved for laser cutting is never repaired', vm.runInContext('Gate.unpairPlan(B.run)', w2.c).length === 0); delete st2.processReady;
    s1.laserDoneAt = 5; ok('a sheet the laser cut gives nothing up', vm.runInContext('Gate.unpairPlan(B.run)', w2.c).length === 0); delete s1.laserDoneAt;
    st2.committedAt = 5; st2.status = 'complete'; ok('a committed set is never repaired', vm.runInContext('Gate.unpairPlan(B.run)', w2.c).length === 0);
  }

  /* ═══ 4 · AUTOCUT: the include is undone when the server refuses the cut ═════════════════════════════════════════════════════════════════════════ */
  {
    const w = world(); recStub(w, { refuse: 'order 4199104309: waiting for its own design' });
    const gf = w.full(w.page('gold', base)), ss = w.full(w.page('silver', base)), rg = w.page('rose', Object.assign({ density: .3 }, base));
    w.put(gf, '7001_1_1', '7002_1_1'); w.put(ss, '7001_2_1'); w.put(rg, '7002_2_1');
    await w.assemble(); await settle(w);
    ok('the press was tried, the server refused: the RG sheet was put in, then taken out again', w.calls.includes('RG1') && !w.inSet(rg) && !rg.roseCutAt, w.word());
    ok('its card says why, in the server\'s words', /^Its green line is cut automatically, and it joins Set-1, once it is ready: order 4199104309: waiting for its own design$/.test(rg.autoCutWait || ''), rg.autoCutWait);
    ok('its tick is not left behind (it would come back uncut)', rg.solidPick !== true);
    const r = w.selfCheck(); ok('the set names it as the partner still to come (R10), no violation', r.ok && r.partners.some(p => p.sheet === 'RG1'), (r.lines || []).join(' | '));
    const n = w.calls.length; await w.assemble(); await settle(w); ok('nothing changed: it is not tried again at every look', w.calls.length === n);
    w.refuse = null; rg.jobId = 'j2'; await w.assemble(); await settle(w);
    ok('something changed and the server accepts: cut and in its set', w.inSet(rg) && !!rg.roseCutAt && !rg.autoCutWait, w.word());
  }

  /* ═══ 5 · RED LINE: an open back engraving is a wait, never a break ══════════════════════════════════════════════════════════════════════════════ */
  {
    const rec = (id, metal, o = {}) => Object.assign({ id, metal, runId: 'r', page: 1, sheetIndex: 1, setId: 's1', draft: false, releaseFull: metal !== 'rose', density: .7, placedCount: 3, poolIds: [] }, o);
    const gf = rec('gf', 'gold', { poolIds: [`${A}_1_1`] }), ss = rec('ss', 'silver', { poolIds: [`${A}_2_1`] }), rg = rec('rg', 'rose', { solidIncluded: true, backsApproved: false, poolIds: [`${A}_3_1`] });
    const set = { setId: 's1', seq: 1, runId: 'r', sheetIds: ['gf', 'ss', 'rg'], status: 'open' };
    const d = SRp.diagnose([set], [gf, ss, rg]);
    ok('a set whose RG sheet\'s back engraving is open does not "break your rules": no red line, no Rebuild', !d.bySet.s1, d.bySet.s1 && d.bySet.s1.line);
    const g = { setId: 's1', seq: 1, sheets: [gf, ss, rg] }; SRp.annotate([g], [gf, ss, rg], [set]);
    ok('it carries one plain waiting line', g.setWaits && g.setWaits.line === 'Waiting for the back engraving approval of RG Sheet 1' && !g.setBreaks, JSON.stringify(g.setWaits));
    const gate = RD.setRules(Object.assign({}, set, { setWaits: g.setWaits }), [gf, ss, rg]);
    ok('the Approve gate is grey with that line', gate.applies && !gate.ok && /Waiting for the back engraving approval of RG Sheet 1/.test(gate.line) && !gate.breaks, JSON.stringify(gate));
    ok('a remedy that is only a back engraving to approve never draws a Rebuild button', !/data-rebuild-set/.test(SRp.lineHtml({ setId: 's1', setBreaks: { line: 'x', rules: [], remedy: { kind: 'backs', say: 'Approve the back engraving of RG Sheet 1 (Engraving tab).' } } })));
  }
  console.log('PASS: green-line one set: one GF + one SS sheet share an order at the intake, the overflow and greenFits; a green-line sheet ties no two units and belongs to one set (a set holding a surplus unit is corrected by itself); the live five-sheet set is repaired (order B\'s SS piece moved, GF 2 out, the cut RG 2 a known split); an automatic cut refused undoes its include; an open back engraving is a wait, never a red line');
})().catch(e => { console.error(e); process.exit(1); });
