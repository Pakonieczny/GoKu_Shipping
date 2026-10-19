// charm-nest-set-invariants.js: THE acceptance criterion of a Set of Sheets (docs/charm-nest/set-rules.md is the rule list).
// Paul stated the rules of a Set of Sheets more than once and the app still formed one wrongly; this suite holds the checker to the owner's own two sets and to each rule alone.
//   1  Set 1 of the owner: GF Sheet 1 + SS Sheet 1 sharing order 4191315127: OK (one unit, nothing spare, nothing missing)
//   2  Set 2 of the owner, as the app built it (14 sheets in ONE set): the checker names the surplus units (b), the two uncut Rose Gold sheets (d), the sheets that joined after the set had formed (f),
//      the partial GF Draft 10 whose group should not be in a set (R11), and nothing it should not
//   3  what the CORRECT formation of those same sheets is, derived from the rules (SetRules.formAll over SharedOrders units): the checker accepts it, every set minimal, valid, the Rose Gold sheets named as partners to add
//   4  each rule alone: (a) invalid, (b) surplus, (c) partial, (d) gold-family conditions, (e) cardinal, (f) closed set, R11, numbering, membership, exempt sets
//   5  property: random pools. expected() is always accepted; one set holding everything is refused exactly when a unit could leave (brute force, written here, not the helper under test)
//   6  the module loads the way a page loads it (window globals, no require)
// Fixtures only: made-up ids and order numbers (4191315127 is the owner's Set 1 order, as the owner quoted it). No network, no page, no cloud.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const I = require('../../charm-nest-set-invariants.js'), SR = require('../../charm-nest-set-rules.js'), SO = require('../../charm-nest-shared-orders.js'), SN = require('../../charm-nest-sheet-name.js');

const piece = (order, n) => `${order}_${order}1_${n}`;            // "receipt_transaction_copy": a shared order has two pieces, one on each sheet
let single = 9000000000;
const own = k => Array.from({ length: k }, () => piece(++single, 1)); // single-piece orders: never shared
const link = (a, b, order) => { a.poolIds.push(piece(order, 1)); b.poolIds.push(piece(order, 2)); };
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
const sheet = (id, metal, no, o = {}) => ({ id, metal, page: no + 1, sheetIndex: no, releaseFull: true, placedCount: 40, density: .76, draft: false, poolIds: own(3), ...o });
const ids = l => l.map(s => (typeof s === 'string' ? s : s.id));
const names = (r, rule) => r.violations.filter(v => v.rule === rule).map(v => v.names.join(' + '));
const ofRule = (r, rule) => r.violations.filter(v => v.rule === rule);
const show = r => I.explain(r).join('\n  ');

/* ── 1 · Set 1 of the owner: GF Sheet 1 + SS Sheet 1, one shared order ──────────────────────────────────────────────────────────────────── */
function owner1() {
  const gf = sheet('s1-gf1', 'gold', 1, { page: 1, setId: 'set-1', joinedAt: 100 }), ss = sheet('s1-ss1', 'silver', 1, { page: 1, setId: 'set-1', joinedAt: 101, density: .74 });
  link(gf, ss, 4191315127);
  return { sheets: [gf, ss], sets: [{ id: 'set-1', name: 'Set 1', seq: 1, sheetIds: [gf.id, ss.id] }] };
}
{
  const f = owner1(), r = I.check(f.sets, f.sheets);
  assert.equal(r.ok, true, 'Set 1 of the owner is a right set:\n  ' + show(r));
  assert.deepEqual([r.violations.length, r.partners.length, r.warnings.length], [0, 0, 0]);
  assert.deepEqual(r.sets[0].names, ['GF Sheet 1', 'SS Sheet 1'], 'the names are the owner\'s');
  assert.deepEqual(r.sets[0].units, [['s1-gf1', 's1-ss1']], 'the two sheets are ONE unit: the order ties them');
  assert.equal(r.sets[0].valid, true); assert.equal(r.sets[0].formedAt, 101, 'the set formed when the SS sheet joined');
  // the same two sheets without the order are two units, and the set is still not too big: both are needed
  const loose = owner1(); loose.sheets.forEach(s => { s.poolIds = own(2); });
  assert.equal(I.check(loose.sets, loose.sheets).ok, true, 'GF + SS with no shared order: both are needed, nothing is spare');
}

/* ── 2 · Set 2 of the owner, as the app built it ─────────────────────────────────────────────────────────────────────────────────────────── */
/* GF Sheet 1..8 and SS Sheet 1..4 full; RG Sheet 16 (58 %) and RG Sheet 29 (29 %) not cut. Shared-order chains (the owner's listing):
     A  SS4 - GF7 - RG29 - GF6 - SS3      B  GF5 - RG16 - GF4 - SS2 - GF3      C  SS1 - GF2 - GF1      GF8 loose.
   Plus the partial GF Draft 10, which shares orders with the set: `tie` says with which unit (the owner's words do not say; B is the default, the others are tested too). */
function owner2({ tie = 'B', setId = 'set-2' } = {}) {
  const gf = {}, ss = {}; let t = 0;
  for (let n = 1; n <= 8; n++) gf[n] = sheet('gf' + n, 'gold', n, { setId, density: .77 });
  for (let n = 1; n <= 4; n++) ss[n] = sheet('ss' + n, 'silver', n, { setId, density: .75 });
  const rg16 = sheet('rg16', 'rose', 16, { page: 16, setId, releaseFull: false, density: .58 }), rg29 = sheet('rg29', 'rose', 29, { page: 29, setId, releaseFull: false, density: .29 });
  const draft = sheet('gf-draft-10', 'gold', 0, { page: 10, draft: true, releaseFull: false, density: .62, sheetIndex: undefined });
  const chain = (order0, ...sheets) => sheets.slice(1).forEach((s, i) => link(sheets[i], s, order0 + i));
  chain(4200000400, ss[4], gf[7], rg29, gf[6], ss[3]);                       // A
  chain(4200000500, gf[5], rg16, gf[4], ss[2], gf[3]);                       // B
  chain(4200000600, ss[1], gf[2], gf[1]);                                    // C
  const to = { A: gf[6], B: gf[4], C: gf[2] }[tie]; if (to) link(to, draft, 4200000999);
  // the order the live app joined them in (joinedAt): C first, then B, A and the loose GF 8 one after the other
  const at = { gf1: 1000, ss1: 1001, gf2: 1002, gf3: 2000, gf4: 2001, gf5: 2002, ss2: 2003, rg16: 2004, ss3: 3000, ss4: 3001, gf6: 3002, gf7: 3003, rg29: 3004, gf8: 4000 };
  const sheets = [...Object.values(gf), ...Object.values(ss), rg16, rg29, draft];
  for (const s of sheets) if (at[s.id]) s.joinedAt = at[s.id] + t;
  const members = sheets.filter(s => s.setId === setId);
  return { sheets, gf, ss, rg16, rg29, draft, sets: [{ id: setId, name: 'Set 2', seq: 2, sheetIds: members.map(s => s.id) }] };
}
{
  const f = owner2(), r = I.check(f.sets, f.sheets);
  assert.equal(r.ok, false, 'the 14-sheet Set 2 is refused');
  assert.equal(f.sets[0].sheetIds.length, 14, 'the owner\'s Set 2 holds 14 sheets');
  assert.deepEqual(r.sets[0].names.slice().sort(), ['GF Sheet 1', 'GF Sheet 2', 'GF Sheet 3', 'GF Sheet 4', 'GF Sheet 5', 'GF Sheet 6', 'GF Sheet 7', 'GF Sheet 8', 'RG Sheet 16', 'RG Sheet 29', 'SS Sheet 1', 'SS Sheet 2', 'SS Sheet 3', 'SS Sheet 4']);
  // (b) the surplus units: GF 8 alone, group A, group B. Group C (the earliest unit that holds a completed GF and a completed SS sheet) is what Set 2 may be.
  const sur = ofRule(r, 'surplus');
  assert.equal(sur.length, 3, 'three units are more than the set needs:\n  ' + show(r));
  const byFirst = k => sur.find(v => v.sheets.includes(k));
  assert.deepEqual(byFirst('gf8').sheets, ['gf8']); assert.equal(byFirst('gf8').message, 'GF Sheet 8 shares no order with the others and Set 2 is valid without it (GF Sheet 1, GF Sheet 2 and SS Sheet 1 are enough)', 'the owner\'s own example sentence');
  assert.deepEqual(byFirst('gf4').sheets.slice().sort(), ['gf3', 'gf4', 'gf5', 'rg16', 'ss2'], 'unit B: GF 3, GF 4, GF 5, SS 2 and the Rose Gold sheet 16 they are tied to');
  assert.deepEqual(byFirst('gf7').sheets.slice().sort(), ['gf6', 'gf7', 'rg29', 'ss3', 'ss4'], 'unit A: SS 3, SS 4, GF 6, GF 7 and Rose Gold sheet 29');
  assert.deepEqual(byFirst('gf4').orders, ['4200000500', '4200000501', '4200000502', '4200000503'], 'the unit says which orders tie it'); assert.deepEqual(byFirst('gf8').keeps.slice().sort(), ['gf1', 'gf2', 'ss1'], 'what is enough: group C');
  assert(!sur.some(v => v.sheets.includes('gf1') || v.sheets.includes('ss1') || v.sheets.includes('gf2')), 'group C is never called spare');
  // (d) the two uncut Rose Gold sheets
  const nr = ofRule(r, 'notReady');
  assert.deepEqual(nr.map(v => v.sheets[0]).sort(), ['rg16', 'rg29'], 'RG Sheet 16 and RG Sheet 29 are in the set uncut'); assert.deepEqual(nr.map(v => v.names[0]).sort(), ['RG Sheet 16', 'RG Sheet 29']);
  assert(nr.every(v => /green line is not cut/.test(v.message) && /Set 2/.test(v.message)));
  // (f) A, B and the loose GF 8 joined after the set had formed (it formed when SS 1 joined GF 1); GF 2 came with its group (the cardinal rule), the Rose Gold sheets join by their own press
  const late = ofRule(r, 'closedJoin');
  assert.deepEqual(late.map(v => v.sheets.slice().sort()).sort((a, b) => a[0] < b[0] ? -1 : 1), [['gf3', 'gf4', 'gf5', 'ss2'], ['gf6', 'gf7', 'ss3', 'ss4'], ['gf8']].sort((a, b) => a[0] < b[0] ? -1 : 1), 'what joined a closed set:\n  ' + show(r));
  assert.equal(r.sets[0].formedAt, 1001);
  // R11: GF Draft 10 shares an order with unit B and is not full: the group should not be in a set at all
  const pp = ofRule(r, 'partialPartner'); assert.equal(pp.length, 1); assert.equal(pp[0].partial, 'gf-draft-10'); assert.deepEqual(r.partners.map(p => [p.sheet, p.kind]), [['gf-draft-10', 'partial']]);
  // what it does NOT say
  for (const rule of ['invalid', 'partial', 'cardinal', 'numbering', 'membership']) assert.equal(ofRule(r, rule).length, 0, rule + ' is not violated by Set 2');
  assert.equal(r.sets[0].valid, true, 'Set 2 does hold a completed GF and a completed SS sheet: its fault is being too big, not too small');
  // the nesting fault behind the chain is reported as a warning only (GF Sheet 1 and GF Sheet 2 share an order)
  assert.deepEqual(r.warnings.map(w => w.names.slice().sort()).sort(), [['GF Draft 10', 'GF Sheet 4'], ['GF Sheet 1', 'GF Sheet 2']], 'GF 1 + GF 2, and the partial GF 10 + GF 4, share an order: a multi-piece order spread over two sheets of one colour'); assert(r.warnings.every(w => w.rule === 'orderSplit'));
  // Set 1 and Set 2 together: Set 1 stays clean, nothing crosses between them
  const one = owner1(), both = I.check(one.sets.concat(f.sets), one.sheets.concat(f.sheets));
  assert.equal(both.violations.filter(v => v.set === 'set-1').length, 0); assert.equal(both.violations.length, r.violations.length, 'the sets do not disturb each other');
}

/* ── 3 · the correct formation of the same sheets ────────────────────────────────────────────────────────────────────────────────────────── */
/* Every unit of Set 2 except GF 8 holds a completed GF and a completed SS sheet by itself, so each makes its OWN set (SETRULE: one set at a time, each as small as the minimum allows);
   the units form in the order the sheets were seen completed (unknown here), then sheet number, GF before SS. GF 8 has no SS to go with and waits outside every set.
   The Rose Gold sheets are never in a set uncut: each is named as the partner to add to the set(s) of the sheets it shares orders with. A group that shares an order with a partial sheet waits whole.
   SETFIX (Paul, 19 Oct 2026): an uncut Rose Gold sheet outside every set cannot join any set yet, so it ties nothing together (charm-nest-shared-orders.js formation `anchors`): RG 16 no longer glues
   GF 5 to GF 4 (GF 5 waits for an SS sheet of its own, and joins GF 4's set by the cardinal rule once RG 16 is cut), RG 29 no longer glues GF 7 + SS 4 to GF 6 + SS 3 (two valid units, two sets,
   RG 29 is the partner of both). That is what stops one Rose Gold sheet that keeps filling from pulling every later unit into Set 2. */
{
  const sorted = l => l.slice().sort();
  const f0 = owner2({ tie: null }), e0 = I.expected(f0.sheets, { firstSeq: 2 });
  const C = ['gf1', 'gf2', 'ss1'], Bm = ['gf3', 'gf4', 'ss2'], A1 = ['gf6', 'ss3'], A2 = ['gf7', 'ss4'];
  assert.deepEqual(e0.sets.map(s => [s.name, sorted(s.sheetIds)]), [['Set 2', C], ['Set 3', Bm], ['Set 4', A1], ['Set 5', A2]],
    'no partial tie: Set 2 = group C (GF 1, GF 2, SS 1), Set 3 = GF 3, GF 4, SS 2, Set 4 = GF 6 + SS 3, Set 5 = GF 7 + SS 4; GF 5 and the loose GF 8 wait');
  assert.deepEqual(e0.waiting.filter(w => w.id === 'gf8' || w.id === 'gf5').map(w => [w.id, w.why]).sort(), [['gf5', 'The set has formed, so this sheet waits for the next set: it needs a completed SS sheet with it'], ['gf8', 'The set has formed, so this sheet waits for the next set: it needs a completed SS sheet with it']]);
  assert.deepEqual(e0.partners.map(p => [p.setName, p.sheet, p.kind]).sort(), [['Set 3', 'rg16', 'notReady'], ['Set 4', 'rg29', 'notReady'], ['Set 5', 'rg29', 'notReady']].sort(), 'the Rose Gold sheets are partners to add, in the set of each sheet they share an order with');
  const r0 = I.check(e0.sets, f0.sheets);
  assert.equal(r0.ok, true, 'the correct formation is accepted:\n  ' + show(r0));
  assert(r0.sets.every(s => !s.canApprove || s.id === 'set-2'), 'a set with a partner to add cannot be approved for laser cutting; the one with none can');
  assert.deepEqual(r0.sets.map(s => s.canApprove), [true, false, false, false]);
  assert.deepEqual(r0.sets.map(s => s.units.length), [1, 1, 1, 1], 'each set is exactly one unit');
  // the same formation by hand, with the joinedAt times of an assembly that formed them in turn: still accepted (nothing joined late)
  const hand = owner2({ tie: null }); const times = { gf1: 10, ss1: 11, gf2: 12, gf3: 20, gf4: 21, ss2: 23, gf6: 30, ss3: 32, gf7: 40, ss4: 41 }; hand.sheets.forEach(s => { s.joinedAt = times[s.id]; });
  const handSets = [C, Bm, A1, A2].map((l, k) => ({ id: 'set-' + (k + 2), name: 'Set ' + (k + 2), seq: k + 2, sheetIds: l }));
  hand.sheets.forEach(s => { s.setId = handSets.find(x => x.sheetIds.includes(s.id))?.id; if (!s.setId) { delete s.setId; s.draft = true; } });
  const rh = I.check(handSets, hand.sheets); assert.equal(rh.ok, true, 'the hand-made correct formation is accepted:\n  ' + show(rh));
  // ... and the sets are exactly what the checker says one big set should have been: merging any two of them is refused
  for (const [a, b] of [[0, 1], [0, 2], [1, 2], [2, 3]]) { const merged = [{ id: 'm', name: 'Set 2', sheetIds: handSets[a].sheetIds.concat(handSets[b].sheetIds) }], rm = I.check(merged, hand.sheets); assert.equal(ofRule(rm, 'surplus').length, 1, 'two valid units in one set: one is spare'); }
  // RG 16 is cut (and its backs approved): it can join now, so it ties again: GF 5 belongs with GF 4 (and RG 16 with them)
  const cut = owner2({ tie: null }); cut.rg16.roseCutAt = 5; cut.rg16.backsApproved = true;
  assert.deepEqual(I.expected(cut.sheets, { firstSeq: 2 }).sets.map(s => sorted(s.sheetIds)), [C, ['gf3', 'gf4', 'gf5', 'rg16', 'ss2'], A1, A2], 'a cut Rose Gold sheet ties the sheets it shares orders with, as every sheet that can join');

  // the partial GF Draft 10 ties to a unit: that whole unit waits (with the draft) until the draft is full; the other units form as before
  const held = { A: [['Set 2', C], ['Set 3', Bm], ['Set 4', A2]], B: [['Set 2', C], ['Set 3', A1], ['Set 4', A2]], C: [['Set 2', Bm], ['Set 3', A1], ['Set 4', A2]] };
  for (const tie of ['A', 'B', 'C']) {
    const f = owner2({ tie }), e = I.expected(f.sheets, { firstSeq: 2 });
    assert.deepEqual(e.sets.map(s => [s.name, sorted(s.sheetIds)]), held[tie], `GF Draft 10 shares an order with unit ${tie}: that unit waits outside every set`);
    assert.equal(e.held.length, 1); assert(e.held[0].ids.includes('gf-draft-10') && /not full yet/.test(e.held[0].line), e.held[0].line);
    assert(e.waiting.some(w => w.id === 'gf-draft-10' || w.id === 'gf8'));
    const r = I.check(e.sets, f.sheets); assert.equal(r.ok, true, `tie ${tie}: the correct formation is accepted:\n  ` + show(r));
    // the app's 14-sheet Set 2 is refused for every tie
    const bad = I.check(f.sets, f.sheets); assert.equal(bad.ok, false); assert(ofRule(bad, 'surplus').length >= 2 && ofRule(bad, 'notReady').length === 2, `tie ${tie}: surplus units and the two uncut Rose Gold sheets are named`);
    assert.equal(ofRule(bad, 'partialPartner').length, 1, `tie ${tie}: the group of the partial sheet should not be in a set`);
  }
  // when GF Draft 10 is full, its group is eligible: it forms its set too
  const full = owner2({ tie: 'B' }); full.draft.releaseFull = true; full.draft.draft = true;
  assert.deepEqual(I.expected(full.sheets, { firstSeq: 2 }).sets.map(s => sorted(s.sheetIds)), [['gf1', 'gf2', 'ss1'], ['gf-draft-10', 'gf3', 'gf4', 'ss2'], ['gf6', 'ss3'], ['gf7', 'ss4']], 'a full Draft 10 joins its group\'s set (it is a unit mate), nothing waits for it any more');
}

/* ── 4 · each rule alone ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{
  const S = (id, sheets) => ({ id, name: id, sheetIds: sheets.map(x => x.id) });
  const gf = (id, no, o) => sheet(id, 'gold', no, o), ss = (id, no, o) => sheet(id, 'silver', no, o);
  // (a) a set without an SS sheet / without any sheet
  let a = gf('a1', 1), b = gf('a2', 2), r = I.check([S('x', [a, b])], [a, b]);
  assert.deepEqual(ofRule(r, 'invalid').map(v => [v.ruleNo, v.letter, v.missing]), [['R1', 'a', ['SS']]]); assert.match(ofRule(r, 'invalid')[0].message, /no completed SS sheet \(it holds 2 completed GF and 0 completed SS\)/);
  assert.equal(I.check([S('x', [])], []).ok, false, 'an empty set is no set'); assert.equal(I.check([S('x', [])], [], { allowShort: ['x'] }).ok, true, 'unless it is declared still forming');
  // a GF sheet that is only partial does not make a set valid, and it is flagged (c)
  a = gf('a1', 1); b = ss('b1', 1, { releaseFull: false, density: .4 }); r = I.check([S('x', [a, b])], [a, b]);
  assert.deepEqual(ofRule(r, 'partial').map(v => [v.letter, v.sheets, v.message]), [['c', ['b1'], 'SS Sheet 1 is in x but is not full yet (40% full): a partial sheet never joins a set']]); assert.equal(ofRule(r, 'invalid').length, 1, 'the partial SS does not count towards the minimum');
  // a sheet with no completion mark at all is still filling (a page's sheet); a cut sheet is completed
  const nomark = { id: 'n1', metal: 'silver', sheetIndex: 1, page: 1, poolIds: own(1), placedCount: 3 }; delete nomark.releaseFull; a = gf('a1', 1);
  assert.equal(I.check([S('x', [a, nomark])], [a, nomark]).ok, false); nomark.laserDoneAt = 5; assert.equal(I.check([S('x', [a, nomark])], [a, nomark]).ok, true, 'a laser cut sheet is completed');
  // (b) surplus, the smallest cases
  const g1 = gf('g1', 1), g2 = gf('g2', 2), s1 = ss('s1', 1), s2 = ss('s2', 2);
  r = I.check([S('x', [g1, s1, g2])], [g1, s1, g2]); assert.deepEqual(names(r, 'surplus'), ['GF Sheet 2'], 'GF 1 + SS 1 are the minimum; the later GF is spare');
  r = I.check([S('x', [g1, s1, g2, s2])], [g1, s1, g2, s2]); assert.deepEqual(names(r, 'surplus'), ['GF Sheet 2', 'SS Sheet 2'], 'two sheets that each make nothing the set needs');
  r = I.check([S('x', [g1, g2, s1])], [g1, g2, s1].map(x => x)); assert(names(r, 'surplus').includes('GF Sheet 2'));
  // ... but sheets tied by an order are not spare: they are one unit with the sheets that make the set valid
  const t1 = gf('t1', 1), t2 = gf('t2', 2), u1 = ss('u1', 1); link(t1, u1, 4300000001); link(u1, t2, 4300000002);
  r = I.check([S('x', [t1, t2, u1])], [t1, t2, u1]); assert.equal(r.ok, true, 'GF 1, SS 1 and GF 2 are one unit (orders tie them): nothing is spare\n  ' + show(r)); assert.deepEqual(r.sets[0].units, [['t1', 'u1', 't2']]);
  // a set short of nothing but with an extra GF that waits for its own SS: surplus names it; a unit that is the only GF is never spare
  r = I.check([S('x', [g1, s1])], [g1, s1]); assert.equal(r.ok, true);
  // (d) the gold-family conditions
  const rose = (id, o) => sheet(id, 'rose', 7, { page: 7, releaseFull: false, density: .5, ...o }), k14 = (id, o) => sheet(id, 'gold14k', 3, { releaseFull: false, density: .5, ...o }), k10 = (id, o) => sheet(id, 'gold10k', 2, { releaseFull: false, ...o });
  const base = () => [gf('c1', 1), ss('c2', 1)];
  for (const [label, sh, ok, why] of [
    ['uncut Rose Gold', rose('r1'), false, /green line is not cut/], ['cut Rose Gold, backs not reported', rose('r1', { roseCutAt: 5 }), true],
    ['Rose Gold, the Cut Sheet press approved', rose('r1', { cutApproved: true }), true], ['cut Rose Gold, backs not approved', rose('r1', { roseCutAt: 5, backsApproved: false }), false, /back engraving is not approved/],
    ['uncut 14K', k14('r1'), false, /green line is not cut/], ['14K cut and backs approved', k14('r1', { roseCutAt: 5, backsApproved: true }), true],
    ['10K, laser cut', k10('r1', { laserDoneAt: 9 }), true], ['10K uncut, backs not approved', k10('r1', { backsApproved: false }), false, /green line is not cut.*back engraving is not approved/],
    ['a full Rose Gold sheet takes the rest of the metal whole: no line', rose('r1', { roseFull: true, backsApproved: true }), true],
    ['backs not approved by the readiness report', rose('r1', { roseCutAt: 5, laser: { stages: { approval: false }, approved: 0, required: 2 } }), false, /back engraving is not approved/],
    ['backs approved by the readiness report', rose('r1', { roseCutAt: 5, laser: { stages: { approval: true } } }), true]]) {
    const all = base().concat([sh]); r = I.check([S('x', all)], all);
    assert.equal(ofRule(r, 'notReady').length === 0, ok, label + ':\n  ' + show(r)); if (why) assert.match(ofRule(r, 'notReady')[0].message, why, label);
    assert.equal(ofRule(r, 'surplus').length, 0, label + ': a gold-family sheet is a person\'s choice, never "spare"');
  }
  // (e) the cardinal rule: one order, two sets
  const p = gf('p1', 1), q = ss('q1', 1), p2 = gf('p2', 2), q2 = ss('q2', 2); link(p, q2, 4400000001);
  r = I.check([S('one', [p, q]), S('two', [p2, q2])], [p, q, p2, q2]); const card = ofRule(r, 'cardinal');
  assert.equal(card.length, 1); assert.deepEqual(card[0].sheets.sort(), ['p1', 'q2']); assert.deepEqual(card[0].sets.sort(), ['one', 'two']); assert.match(card[0].message, /order 4400000001 has pieces on sheets of different sets/i);
  // a completed sheet outside every set that shares an order with a set's sheet should be in that set (a closed set still takes it)
  const o1 = gf('o1', 1), o2 = ss('o2', 1), o3 = gf('o3', 2, { draft: true }); link(o1, o3, 4400000002);
  r = I.check([S('x', [o1, o2])], [o1, o2, o3]); assert.deepEqual(ofRule(r, 'cardinal').map(v => v.outside), ['o3'], 'a completed GF that shares an order stays out of the set it belongs to'); assert.match(ofRule(r, 'cardinal')[0].message, /it joins x/);
  // a partner that cannot join yet is a PARTNER, not a violation (R10); once it can, it must be in
  const m1 = gf('m1', 1), m2 = ss('m2', 1), mr = rose('mr', { draft: true }); link(m1, mr, 4400000003);
  r = I.check([S('x', [m1, m2])], [m1, m2, mr]); assert.equal(r.ok, true, 'the set forms without the uncut Rose Gold sheet'); assert.deepEqual(r.partners.map(x => [x.name, x.kind, x.reasons[0].slice(0, 22)]), [['RG Draft 7', 'notReady', 'its green line is not ']]);
  assert.equal(r.sets[0].canApprove, false, 'it cannot be approved for laser cutting until the partner is in it'); assert.match(I.explain(r)[0], /^\[R10 partner\] x: add RG Draft 7/);
  mr.roseCutAt = 5; mr.backsApproved = true; r = I.check([S('x', [m1, m2])], [m1, m2, mr]); assert.equal(r.ok, false); assert.match(ofRule(r, 'cardinal')[0].message, /nothing stops it joining/);
  assert.match(show(I.check([S('x', [m1, m2, mr])], [m1, m2, mr])), /says it is a draft/, 'a sheet that says it is a draft is in no set');
  mr.draft = false; r = I.check([S('x', [m1, m2, mr])], [m1, m2, mr]); assert.equal(r.ok, true, 'cut, approved and in the set: right\n  ' + show(r)); assert.equal(r.sets[0].canApprove, true, 'its partner is in: the set can go to laser cutting');
  // R11: a sheet of the set shares an order with a partial GF outside it
  const w1 = gf('w1', 1), w2 = ss('w2', 1), wp = gf('wp', 0, { draft: true, releaseFull: false, density: .65, page: 3 }); link(w1, wp, 4400000004);
  r = I.check([S('x', [w1, w2])], [w1, w2, wp]); assert.deepEqual(ofRule(r, 'partialPartner').map(v => v.sheets.sort()), [['w1', 'wp']]); assert.match(ofRule(r, 'partialPartner')[0].message, /GF Draft 3 is not full yet \(65% full\)/);
  assert.equal(I.check([S('x', [w1, w2])], [w1, w2, wp], { partialPartners: 'pending' }).ok, true, 'pending: a set that already held a sheet of the group'); assert.equal(I.check([S('x', [w1, w2])], [w1, w2, wp], { legacySets: ['x'] }).ok, true);
  assert.deepEqual(I.check([S('x', [w1, w2])], [w1, w2, wp], { legacySets: ['x'] }).partners.map(x => x.kind), ['partial']);
  // (f) a closed set: a sheet that joined after the set had formed
  const c1 = gf('c1', 1, { joinedAt: 10 }), c2 = ss('c2', 1, { joinedAt: 11 }), c3 = gf('c3', 2, { joinedAt: 50 });
  r = I.check([S('x', [c1, c2, c3])], [c1, c2, c3]); assert.deepEqual(ofRule(r, 'closedJoin').map(v => [v.letter, v.sheets, v.formedAt]), [['f', ['c3'], 11]]);
  c3.joinedBy = 'person'; assert.equal(ofRule(I.check([S('x', [c1, c2, c3])], [c1, c2, c3]), 'closedJoin').length, 0, 'a person\'s drag is allowed'); delete c3.joinedBy;
  link(c1, c3, 4500000001); assert.equal(ofRule(I.check([S('x', [c1, c2, c3])], [c1, c2, c3]), 'closedJoin').length, 0, 'it shares an order with a sheet that was there: the cardinal rule outranks the closing');
  c3.poolIds = own(2); const c4 = rose('c4', { joinedAt: 60, roseCutAt: 5, backsApproved: true }); assert.equal(ofRule(I.check([S('x', [c1, c2, c4])], [c1, c2, c4]), 'closedJoin').length, 0, 'Rose Gold joins by its own press');
  assert.equal(ofRule(I.check([{ id: 'x', name: 'x', formedAt: 20, sheetIds: ['c1', 'c2', 'c3'] }], [c1, c2, c3]), 'closedJoin').length, 1, 'a recorded formedAt is read too');
  const sim = gf('d1', 1, { joinedAt: 10 }), sim2 = ss('d2', 1, { joinedAt: 10 }), sim3 = gf('d3', 2, { joinedAt: 10 }); assert.equal(ofRule(I.check([S('x', [sim, sim2, sim3])], [sim, sim2, sim3]), 'closedJoin').length, 0, 'sheets that joined together formed it together (its other fault is the spare GF)');
  // R5: completed sheets that wait outside every set although they make one ("as soon as the minimum criteria is met ... the set should be formed")
  const f1 = gf('f1', 1), f2 = ss('f2', 1), f3 = gf('f3', 2), f4 = ss('f4', 2, { releaseFull: false, density: .4 });
  r = I.check([], [f1, f2]); assert.deepEqual(ofRule(r, 'unformed').map(v => [v.ruleNo, v.sheets.sort()]), [['R5', ['f1', 'f2']]]); assert.match(ofRule(r, 'unformed')[0].message, /GF Draft 2 and SS Draft 2 are completed, in no set, and make a valid set/);
  assert.equal(I.check([], [f1, f2], { unformed: false }).ok, true); assert.equal(I.check([], [f1, f3]).ok, true, 'two GF sheets make no set'); assert.equal(I.check([], [f1, f4]).ok, true, 'a partial SS sheet makes none either');
  r = I.check([S('x', [f1, f2])], [f1, f2, f3, ss('f5', 3)]); assert.deepEqual(ofRule(r, 'unformed').map(v => v.sheets.sort()), [['f3', 'f5']], 'after Set x formed, the next completed pair is a set of its own');
  // a completed sheet that shares an order with a sheet of a set belongs to THAT set (cardinal), it does not make a new one
  const h1 = gf('h1', 1), h2 = ss('h2', 1), h3 = gf('h3', 2, { draft: true }), h4 = ss('h4', 2, { draft: true }); link(h1, h3, 4700000001);
  r = I.check([S('x', [h1, h2])], [h1, h2, h3, h4]); assert.equal(ofRule(r, 'unformed').length, 0, 'h3 is tied to Set x; h4 alone is no set'); assert.equal(ofRule(r, 'cardinal').length, 1);
  // numbering and membership
  const n1 = gf('n1', 1), n2 = gf('n2', 1), n3 = ss('n3', 1); r = I.check([S('x', [n1, n2, n3])], [n1, n2, n3]); assert.deepEqual(names(r, 'numbering'), ['GF Sheet 1 + GF Sheet 1'], 'two GF sheets numbered 1 in one set');
  r = I.check([S('x', [n1, n3, { id: 'ghost' }])], [n1, n3]); assert.deepEqual(ofRule(r, 'membership').map(v => v.sheets), [['ghost']]);
  const dr = gf('dr', 2, { draft: true }); r = I.check([S('x', [n1, n3, dr])], [n1, n3, dr]); assert.match(ofRule(r, 'membership')[0].message, /says it is a draft/); assert.equal(ofRule(I.check([S('x', [n1, n3, dr])], [n1, n3, dr], { membership: false }), 'membership').length, 0);
  const el = gf('el', 2, { setId: 'elsewhere' }); assert.equal(ofRule(I.check([S('x', [n1, n3, el])], [n1, n3, el]), 'membership').length, 0, 'the sets argument is the truth unless membership is strict');
  assert.match(ofRule(I.check([S('x', [n1, n3, el])], [n1, n3, el], { membership: 'strict' }), 'membership')[0].message, /says it is in set elsewhere/);
  r = I.check([S('x', [n1, n3]), S('y', [n1, n3])], [n1, n3]); assert.equal(ofRule(r, 'membership').length, 4, 'a sheet is in exactly one set');
  const ar = gf('ar', 2, { archived: true }); assert.match(ofRule(I.check([S('x', [n1, n3, ar])], [n1, n3, ar]), 'membership')[0].message, /archived/);
  // exempt sets (committed / completed / replaced under older rules) are checked unless skipExempt, as SetRules.gate
  const e1 = gf('e1', 1), e2 = gf('e2', 2), es = [e1, e2]; const done = { id: 'x', name: 'Set 9', committedAt: 5, sheetIds: ['e1', 'e2'] };
  assert.equal(I.check([done], es).ok, false); assert.equal(I.check([done], es, { skipExempt: true }).ok, true); assert.match(I.check([done], es, { skipExempt: true }).notes[0], /not checked/);
  // the inputs: sets as arrays of ids / a map, sheets as a map, a page's placed charms
  const pg = { sheetId: 'pg1', metal: 'gold', page: 1, runId: 'r', releaseFull: true, placements: [{ id: 'c1' }, { id: 'c2' }], charms: [{ id: 'c1', poolId: piece(4600000001, 1) }, { id: 'c2', poolId: piece(4600000002, 1) }, { id: 'c3', poolId: piece(4600000003, 1) }], draft: false };
  const pg2 = { sheetId: 'pg2', metal: 'silver', page: 1, runId: 'r', releaseFull: true, placements: [{ id: 'c1' }], charms: [{ id: 'c1', poolId: piece(4600000001, 2) }], draft: false };
  r = I.check([['pg1', 'pg2']], [pg, pg2]); assert.equal(r.ok, true); assert.deepEqual(r.sets[0].units, [['pg1', 'pg2']], 'a page\'s placed charms give its pieces; only what is placed counts (c3 is not on the sheet)');
  assert.equal(I.check({ s: ['pg1', 'pg2'] }, { pg1: pg, pg2: pg2 }).ok, true); assert.equal(I.check(new Map([['s', { sheetIds: [pg, pg2] }]]), new Map([['pg1', pg], ['pg2', pg2]])).ok, true);
  assert.deepEqual(I.check([['pg1', 'pg2']], [pg, pg2]).sets[0].names, ['GF Sheet 1', 'SS Sheet 1']);
  // pieces given directly and orders only
  const d1 = { id: 'd1', metal: 'gold', page: 1, releaseFull: true, placedCount: 3, pieces: [{ key: 'k1', orderId: '77' }] }, d2 = { id: 'd2', metal: 'silver', page: 1, releaseFull: true, placedCount: 3, pieces: [{ key: 'k2', orderId: '77' }] };
  assert.deepEqual(I.check([['d1', 'd2']], [d1, d2]).sets[0].units, [['d1', 'd2']]);
  const l1 = { id: 'l1', metal: 'gold', releaseFull: true, placedCount: 2, orders: ['88'] }, l2 = { id: 'l2', metal: 'silver', releaseFull: true, placedCount: 2, orders: ['88'] };
  assert.deepEqual(I.check([['l1', 'l2']], [l1, l2]).sets[0].units, [['l1', 'l2']], 'a record that lists orders and no pieces is one unknown piece per order and sheet (SharedOrders.sheetOf)');
}

/* ── 5 · property: random pools of sheets ────────────────────────────────────────────────────────────────────────────────────────────────── */
// expected() is always accepted by check(); one set holding every free sheet is refused exactly when a whole unit could leave and keep it valid (brute force written here,
// not the helper under test: union-find over the shared orders, then try removing each unit)
{
  const rng = seed => () => (seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296;
  let mergedRefused = 0, mergedFine = 0; const seenSets = new Set();
  for (let seed = 1; seed <= 400; seed++) {
    const r = rng(seed * 13 + 5), n = 3 + Math.floor(r() * 12), sheets = [];
    for (let i = 0; i < n; i++) {
      const k = r(), no = i + 1;
      sheets.push(k < .42 ? sheet('g' + i, 'gold', no) : k < .78 ? sheet('s' + i, 'silver', no) : k < .86 ? sheet('p' + i, r() < .5 ? 'gold' : 'silver', no, { releaseFull: false, density: .5 })
        : sheet('x' + i, ['rose', 'gold14k', 'gold10k'][i % 3], no, { releaseFull: r() < .3, ...(r() < .5 ? { roseCutAt: 7, backsApproved: true } : {}) }));
    }
    for (let i = 0; i < Math.floor(r() * 5); i++) { const a = sheets[Math.floor(r() * n)], b = sheets[Math.floor(r() * n)]; if (a !== b) link(a, b, 5000000000 + seed * 100 + i); }
    const e = I.expected(sheets, { firstSeq: 1 }), res = I.check(e.sets, sheets);
    assert.equal(res.ok, true, `seed ${seed}: expected() is accepted\n  ` + show(res)); seenSets.add(e.sets.length);
    for (const s of e.sets) assert.equal(SR.validSet(s.sheetIds.map(id => sheets.find(x => x.id === id))).ok, true, 'every formed set is valid');
    // one set with every sheet the rules put in a set (the sheets of the groups a partial sheet holds are left out, as the rules say)
    const all = e.sets.flatMap(s => s.sheetIds);
    if (all.length < 2) continue;
    const members = all.map(id => sheets.find(x => x.id === id)), merged = I.check([{ id: 'm', name: 'Set 1', sheetIds: all }], sheets), spare = ofRule(merged, 'surplus').length > 0;
    // brute force: units by union-find over the orders of ALL the sheets (a sheet outside the set, a Rose Gold partner, still ties the sheets that share an order with it: the cardinal rule),
    // restricted to the set's members; then try every unit out
    const pieces = new Map(); for (const sh of sheets) for (const key of sh.poolIds) { const o = key.split('_')[0]; if (!pieces.has(o)) pieces.set(o, new Set()); pieces.get(o).add(sh.id); }
    const parent = new Map(sheets.map(sh => [sh.id, sh.id])), find = x => { while (parent.get(x) !== x) x = parent.get(x); return x; };
    for (const set of pieces.values()) { const l = [...set]; if (set.size > 1) for (let i = 1; i < l.length; i++) parent.set(find(l[i]), find(l[0])); }
    const units = new Map(); for (const id of all) { const root = find(id); (units.get(root) || units.set(root, []).get(root)).push(id); }
    const removable = [...units.values()].some(u => SR.validSet(members.filter(sh => !u.includes(sh.id))).ok);
    assert.equal(spare, removable, `seed ${seed}: the checker calls a set spare exactly when a unit can leave (${all.join(',')})\n  ` + show(merged));
    if (removable) mergedRefused++; else mergedFine++;
  }
  assert(mergedRefused > 40 && mergedFine > 20 && seenSets.size > 3, `the random pools cover both outcomes (${mergedRefused} refused, ${mergedFine} fine, ${seenSets.size} set counts)`);
}

/* ── 6 · the module loads the way a page loads it (window globals, no require) ──────────────────────────────────────────────────────────── */
{
  const root = path.join(__dirname, '../..'), read = f => fs.readFileSync(path.join(root, f), 'utf8');
  const ctx = vm.createContext({}); ctx.self = ctx;
  for (const f of ['charm-nest-sheet-name.js', 'charm-nest-set-rules.js', 'charm-nest-shared-orders.js']) vm.runInContext(read(f), ctx, { filename: f });
  vm.runInContext(read('charm-nest-set-invariants.js'), ctx, { filename: 'charm-nest-set-invariants.js' });
  assert.equal(typeof ctx.CharmNestSetInvariants.check, 'function', 'window.CharmNestSetInvariants');
  const f = owner1(), r = ctx.CharmNestSetInvariants.check(JSON.parse(JSON.stringify(f.sets)), JSON.parse(JSON.stringify(f.sheets)));
  assert.equal(r.ok, true); assert.deepEqual(JSON.parse(JSON.stringify(r.sets[0].names)), ['GF Sheet 1', 'SS Sheet 1']);
  // without the rule files it fails loudly rather than judging nothing
  const bare = vm.createContext({}); bare.self = bare; vm.runInContext(read('charm-nest-set-invariants.js'), bare);
  assert.throws(() => bare.CharmNestSetInvariants.check([], []), /needs charm-nest-set-rules\.js/);
  // the file is pure: no page state, no network, no timers
  const src = read('charm-nest-set-invariants.js').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
  assert(!/\b(fetch|XMLHttpRequest|localStorage|document\.|window\.|setTimeout|Date\.now|Math\.random)\b/.test(src), 'the checker reads its arguments and nothing else');
}
console.log('PASS: set-invariants: the owner\'s Set 1 is right; the owner\'s 14-sheet Set 2 is refused with its surplus units, its two uncut Rose Gold sheets, its late joiners and its partial partner named; the correct formation (one set per unit, in order, Rose Gold named as partners, a partial sheet\'s group held whole) is accepted; every rule alone; 400 random pools agree with a brute-force minimality check; the module loads as a page file');
