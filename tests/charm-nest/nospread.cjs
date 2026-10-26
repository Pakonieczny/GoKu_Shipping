/* NOSPREAD (Paul, 17 Oct 2026): the owner's remedy for sheets linked in long chains, as tests.
 *   "A. When practicable try to avoid spreading multi-piece and earring orders of the same colour between sheets. This approach is most important/effective at the transition point between sheets.
 *    B. When a sheet of a given colour is near completion the system should avoid adding orders of earrings and multi-piece orders to the almost complete sheet (Unless they all fit) and instead add
 *    them to the next sheet in-line ... There could be a circumstance where a given order is so big that it fills several sheets and the system must be smart enough and robust enough to deal with such a scenario."
 *
 * 1 · the pure policy (charm-nest-nospread.js): what near completion is, what "they all fit" is, which sheet takes an order, how a big order is cut
 * 2 · the page's own rules (charm-nest-1.html: topupTakes, feedTurn, feedOn, keepOrdersWhole, overflowToNextSheet, sheetFull; charm-nest-bridge.js: LiveNest.intakePage) in a vm around a stand-in packer, on
 *     small explicit scenes: the transition point, an order that fits, a line that comes late, an order bigger than a sheet (consecutive sheets, the fewest, nothing lost, nothing stuck, nothing loops),
 *     a poor packer, an oversize piece, a packer that leaves half an order seated, the policy switched off
 * 3 · a day of made-up orders (tests/charm-nest/setsim-nospread-lib.cjs), with the policy on and the same code with it off (S.settings.nospread = "off"): the invariants every seed must keep
 * Fake orders only; offline; deterministic (seeded). No network, no Firestore, no Etsy, no paid call.        node tests/charm-nest/nospread.cjs [--quick] [--verbose]                          */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
process.env.SETSIM_QUIET = process.env.SETSIM_QUIET || '1';
const NS = require('../../charm-nest-nospread.js');
const Lib = require('./setsim-lib.cjs'), NL = require('./setsim-nospread-lib.cjs');
const { Sim, orderOf, partOf, piecesOf, fillOf, L } = Lib;
const argv = process.argv.slice(2), QUICK = argv.includes('--quick'), VERBOSE = argv.includes('--verbose');
const REPO = path.join(__dirname, '../..');
let checks = 0;
const ok = (c, msg) => { checks++; assert.ok(c, msg); };
const eq = (a, b, msg) => { checks++; assert.deepEqual(a, b, msg); };
const section = name => console.log('  ' + name);

/* ═══ 1 · the pure policy ═══ */
function policy() {
  const sheet = (fill, o = {}) => Object.assign({ usablePt2: 1000, placedPt2: fill * 1000, pendingPt2: 0, maxFill: 0.8, gapFilling: false }, o);
  const order = (n, each, more = 0, placed = 0) => NS.orderOf({ pieces: Array.from({ length: n }, () => ({ areaPt2: each })), more, placed });

  // the setting: one number, the app's own 75 % target less the margin
  eq(NS.NOSPREAD.nearMargin, 0.15, 'THE setting is NOSPREAD.nearMargin, 0.15');
  eq(NS.NOSPREAD.target, 0.75, 'the target is topupSettle\'s TOPUP.target');
  eq(NS.NOSPREAD.fitAtTarget, 0.5, 'the share of the room a whole order may take at the target is GAP_PACK');
  ok(NS.view(sheet(0.59)).near === false && NS.view(sheet(0.60)).near === true, 'near completion starts at 60 % (75 % less the margin)');
  ok(NS.view(sheet(0.30, { gapFilling: true })).near, 'a sheet filling its gaps is near completion at any fill');
  ok(NS.view(sheet(0.50, { pendingPt2: 110 })).near, 'what is attached and waits counts as filled');
  ok(NS.view(sheet(0.59), { nearMargin: 0.30 }).near && !NS.view(sheet(0.59), { nearMargin: 0.10 }).near, 'the margin is the one setting that moves the zone');

  // orders: counted by the app's own count; pieces still to come count at the mean of those in hand
  const o = order(2, 10, 1, 1);
  eq([o.known, o.more, o.placed, o.count, o.multi, o.footprint], [2, 1, 1, 4, true, 30], 'order: pieces in hand, pieces to come (a held or late line), pieces already seated');
  ok(!order(1, 10).multi && order(1, 10, 1).multi && order(1, 10, 0, 1).multi && order(2, 10).multi, 'multi-piece means two or more pieces in all, whatever line they are on');

  // B: a sheet near completion takes a multi-piece order only if ALL its pieces fit
  const a = (s, ord, cfg) => NS.admits(s, ord, cfg);
  ok(a(sheet(0.30), order(1, 500)).take, 'a single piece is never refused by the policy');
  ok(a(sheet(0.70), order(1, 90)).take && a(sheet(0.70), order(1, 90)).why === 'single', 'a single piece goes into any gap, whatever the fill');
  ok(a(sheet(0.30), order(4, 50)).take && a(sheet(0.30), order(4, 50)).why === 'filling', 'A: a sheet that is only filling takes the order, its pieces stay together on it');
  ok(!a(sheet(0.30), order(4, 130)).take && a(sheet(0.30), order(4, 130)).why === 'no-room-for-all', 'whatever the sheet is, an order that asks for more than the room left under the ceiling is not put on it');
  ok(a(sheet(0.65), order(2, 15)).take && a(sheet(0.65), order(2, 15)).why === 'fits-whole', 'B: an earring pair that fits the near-complete sheet whole is put on it ("unless they all fit")');
  ok(!a(sheet(0.65), order(5, 28)).take && a(sheet(0.65), order(5, 28)).why === 'near-completion', 'B: a necklace of 5 that asks for most of the room (140 of 150) goes to the next sheet in line');
  ok(a(sheet(0.65), order(5, 28)).need <= a(sheet(0.65), order(5, 28)).room, '... although by area alone it would fit: the pieces need their own pockets');
  ok(!a(sheet(0.70), order(2, 40)).take && a(sheet(0.70), order(2, 40)).why === 'near-completion', 'B: at 70 % a pair that asks for 80 of the 100 left does not go on');
  ok(a(sheet(0.74), order(2, 14)).take, 'B: a small pair still fits the gap of a sheet at the target');
  ok(!a(sheet(0.45, { gapFilling: true }), order(3, 70)).take && a(sheet(0.45, { gapFilling: true }), order(3, 15)).take, 'a sheet that fills its gaps applies the measured gap-fill share at any fill');
  ok(a(sheet(0.65), order(2, 40)).take && !a(sheet(0.65), order(2, 40, 2)).take && a(sheet(0.65), order(2, 40, 2)).need === 160, 'a held or late line counts: the order asks for the room of the pieces still to come as well');
  ok(!a(sheet(0.65), order(2, 10, 1)).take && a(sheet(0.65), order(2, 10, 1)).why === 'lines-still-to-come' && a(sheet(0.65), order(2, 10)).take, 'a sheet near completion does not take an order with a line still to come even if all of it fits now: the room goes to other orders and the sheet is released before the line arrives');
  ok(a(sheet(0.40), order(2, 10, 1)).take, 'a sheet that is only filling takes it');
  ok(a(sheet(0.65), order(1, 50)).take && !a(sheet(0.65), order(1, 50, 2)).take, 'a single piece whose order will have 3 is a multi-piece order for the sheet');
  ok(a(sheet(0.60), order(3, 25)).take && !a(sheet(0.60, { pendingPt2: 120 }), order(3, 25)).take, 'what is attached and waits takes room: the order that fits the empty gap does not fit what is left');
  // fit share: all of the room where the zone starts, GAP_PACK at the target
  eq([NS.fitShare(0.60, false), NS.fitShare(0.75, false), NS.fitShare(0.90, false), NS.fitShare(0.675, false), NS.fitShare(0.1, true)].map(x => +x.toFixed(6)), [1, 0.5, 0.5, 0.75, 0.5], 'the share of the room a whole order may ask for falls from 1 to 1/2 across the zone');
  ok(a(sheet(0.30), order(4, 50), { nearMargin: 0 }).take && !a(sheet(0.74), order(2, 40), { nearMargin: 0 }).take, 'with margin 0 only the target and the gap fill apply');

  // a BIG order starts a fresh sheet, never shares one
  const cap = 800;
  const big = order(12, 80);   // 960 > 0.9 * 800
  ok(NS.isBig(big, cap) && !NS.isBig(order(8, 80), cap), 'an order that asks for more than 90 % of what one sheet takes is big');
  ok(a(sheet(0), big).take && a(sheet(0), big).why === 'big-starts-a-fresh-sheet', 'a big order starts on a fresh sheet');
  ok(!a(sheet(0.05), big).take && !a(sheet(0.30), big).take && a(sheet(0.30), big).why === 'big-needs-a-fresh-sheet', 'a big order is not put on a sheet that holds other orders');
  // cutting a big order into units
  const pieces = Array.from({ length: 14 }, (_, i) => ({ id: i, areaPt2: 100, g: i < 2 ? 'pair' : i >= 12 ? 'neck' : null }));
  const area = c => c.areaPt2, grp = c => c.g;
  const units = NS.chunksOf(pieces, area, grp, 1000);   // limit 900
  eq(units.map(u => u.length), [9, 5], 'the units are consecutive and each within 90 % of what a sheet takes');
  eq(units.flat().map(c => c.id), pieces.map(c => c.id), 'no piece is lost, none is repeated, the order is kept');
  ok(units.every(u => u.reduce((n, c) => n + c.areaPt2, 0) <= 900) && units[0].filter(c => c.g === 'pair').length === 2 && units[1].filter(c => c.g === 'neck').length === 2, 'a pair and a necklace stay in one unit');
  eq(NS.chunksOf(pieces.slice(0, 5), area, grp, 1000).length, 1, 'an order that fits is one unit');
  eq(NS.chunksOf([], area, grp, 1000), [], 'nothing to cut');
  const huge = Array.from({ length: 40 }, (_, i) => ({ id: i, areaPt2: 100, g: 'one' }));
  const cut = NS.chunksOf(huge, area, grp, 1000);
  ok(cut.length === 5 && cut.every(u => u.length <= 9) && cut.flat().length === 40, 'a group bigger than a unit is cut piece by piece');
  const h = NS.halve(pieces.slice(2, 9), area, grp, 500); eq(h.map(u => u.length), [4, 3], 'a unit that does not pack is cut in two of about half its area');
  eq(NS.halve([pieces[2]], area, grp, 500), [], 'a single piece cannot be cut');
  const withPair = pieces.slice(0, 6);   // a pair (ids 0, 1) and four single pieces
  eq(NS.halve(withPair, area, grp, 500).map(u => u.map(c => c.id)), [[0, 1, 2], [3, 4, 5]], 'a pair is never parted when the unit can be cut between pieces');
  eq(NS.halve(pieces.slice(0, 2), area, grp, 500), [], 'a pair that does not pack an empty sheet is not cut: that is a problem for a person');
  eq(NS.halve(pieces.slice(0, 2), area, grp, 150).map(u => u.length), [1, 1], 'unless the group alone asks for more than half a sheet');
}

/* ═══ 2 · the page's rules on small scenes ═══ */
const S0 = L.solverOf(REPO);
let uid = 0;
const oid = () => String(8100000000 + (++uid) * 3);
/** A scene: one metal's sheets, fed by hand, around the stand-in packer. */
class Scene {
  constructor(name, o = {}) {
    this.name = name; this.metal = o.metal || 'gold';
    const mid = L.charm(S0, 'probe', 12, 1, 'p', 0), cap = 0.8;
    this.mid = mid.areaPt2;
    // a sheet that takes about `per` mid-size charms under the ceiling
    const need = (o.per || 40) * mid.areaPt2 / (cap * .93), w = Math.sqrt(need * 1.4), h = need / w, mm = pt => Math.ceil(pt * 25.4 / 72 + 2.2);
    this.sim = new Sim({ root: REPO, step: 60, seed: 1, sheet: `${mm(w)}x${mm(h)}` });
    this.cfg = { smallPt2: L.charm(S0, 'probe', 1, 1, 'p', 0).areaPt2 * 0.5, leak: o.leak || 0, maxOrder: o.maxOrder || 0, rng: L.rng(7) };
    this.fake = NL.standIn(L.solverOf(REPO), this.cfg);
    Object.assign(this.sim.ctx.S.settings, o.settings || {});
    this.m = this.sim.metal(this.metal); this.m.S = this.fake;
    this.tick = 0; this.orders = []; this.seen = new Map(); this.searches = 0;
    const solve = this.fake.solve; this.fake.solve = async (...a) => { this.searches++; return solve.apply(this.fake, a); };
  }
  get pages() { return this.m.pages; }
  mk(specs, n) { const o = orderOf(S0, oid(), n || this.orders.length, specs.map(sp => Object.assign({ metal: this.metal }, sp))); this.orders.push(o); return o; }
  /** entries: an order (all its lines arrive) or { order, lines:[index] } */
  async bring(...entries) {
    this.tick++; const parts = [];
    for (const e of entries) {
      const o = e.order || e, idx = e.order ? e.lines : o.lines.map((_, i) => i);
      const p = partOf(o, this.metal, idx.map(i => o.lines[i])); if (!p) continue;
      const before = this.seen.get(o.id) || 0; this.seen.set(o.id, before + p.pieces.length);
      p.expected = o.lines.filter(l => l.metal === this.metal).reduce((n, l) => n + l.pieces.length, 0); p.order = { pieces: p.pieces, expected: p.expected, placed: before };
      parts.push(p);
    }
    await this.m.step(parts, this.tick);
  }
  async idle(n = 1) { for (let i = 0; i < n; i++) { this.tick++; await this.m.step([], this.tick); } }
  where(o) { const placed = new Map(); for (const p of this.pages) for (const q of p.placements) placed.set(q.id, p.page); const pages = [...new Set(piecesOf(o).map(c => placed.get(c.id)).filter(x => x != null))]; return { pages, unplaced: piecesOf(o).filter(c => !placed.has(c.id)).length }; }
  fill(page) { const p = this.pages.find(x => x.page === page); return p ? fillOf(p) : 0; }
  room(page) { const p = this.pages.find(x => x.page === page); return p ? (0.8 - fillOf(p)) * p.usablePt2 : 0; }
  /** sheet 1 filled with mid-size singles, one batch at a time, until its fill is in [lo, hi] and it is still open */
  async fillTo(lo, hi) {
    const designs = [[9, 1], [13, 1], [8, 1.02], [11, .98], [10, 1.05], [12, 1], [5, 1.1], [17, .9], [4, 1.1], [7, .95], [3, 1.1], [6, .95]];
    for (let i = 0; i < 80 && this.fill(1) < lo; i++) { const [idx, f] = designs[i % designs.length]; await this.bring(this.mk([{ kind: 'single', idx, f }], 100 + i)); }
    const p = this.pages[0]; this.setup = { fill: this.fill(1), closed: !!(p && p.releaseFull) };
    ok(this.setup.fill >= lo && this.setup.fill <= hi && !this.setup.closed, `${this.name}: sheet 1 should be open and between ${Math.round(100 * lo)}% and ${Math.round(100 * hi)}% full: it is ${Math.round(100 * this.setup.fill)}%${this.setup.closed ? ' and released' : ''}`);
  }
  /** an order of `k` discs asking for `share` of the room sheet `page` has left */
  discs(k, share, page = 1, idx = 8) { const base = L.charm(S0, 'probe', idx, 1, 'p', 0).areaPt2, f = Math.sqrt(share * this.room(page) / (k * base)); return this.mk([{ kind: 'discs', k, idx, f }], 200 + this.orders.length); }
  report() { return this.pages.filter(p => p.placements.length).map(p => `${p.page}:${Math.round(100 * fillOf(p))}%${p.releaseFull ? '*' : ''}/${new Set(p.placements.map(q => (p.charms.find(c => c.id === q.id) || {}).order)).size}o`).join(' '); }
}

async function scenes() {
  // the transition point: sheet 1 about 65 % full and open
  {
    const sc = new Scene('transition'); await sc.fillTo(.58, .72);
    const N = sc.discs(5, 1.6), T = [0, 1, 0].map((idx, i) => sc.mk([{ kind: 'single', idx, f: 1 }], 300 + i));
    await sc.bring(N, ...T); await sc.idle(2);
    const wN = sc.where(N);
    ok(wN.unplaced === 0 && wN.pages.length === 1 && wN.pages[0] !== 1, `B: a necklace that cannot fit the almost complete sheet goes whole to the next sheet (${JSON.stringify(wN)}; ${sc.report()})`);
    ok(T.filter(o => sc.where(o).pages.includes(1)).length >= 1, 'B: the almost complete sheet is topped up with the single-charm orders');
    section('transition point: a necklace that cannot fit goes whole to the next sheet, single charms top up the almost complete one');
  }
  // "unless they all fit"
  {
    const sc = new Scene('fits'); await sc.fillTo(.58, .70);
    const P = sc.mk([{ kind: 'pair', idx: 3, f: .8 }]);
    await sc.bring(P); await sc.idle(1);
    const w = sc.where(P);
    ok(w.unplaced === 0 && w.pages.length === 1, 'an earring pair that fits is not split');
    ok(w.pages[0] === 1, `B ("unless they all fit"): a small earring pair is put on the almost complete sheet (${JSON.stringify(w)}; ${sc.report()})`);
    section('an earring pair that fits the almost complete sheet whole is put on it');
  }
  // before the sheet is gap-filling: the near zone (60 % to 75 %) alone decides
  {
    const sc = new Scene('zone'); await sc.fillTo(.61, .68);
    ok(!sc.pages[0].topup, 'sheet 1 is not yet filling its gaps (no overflow has happened)');
    const N = sc.discs(4, .97);   // it fits by area (97 % of the room) but asks for nearly all of it
    await sc.bring(N); await sc.idle(1);
    const w = sc.where(N);
    ok(w.unplaced === 0 && w.pages.length === 1, 'the order is whole');
    ok(w.pages[0] !== 1, `B: an order that asks for nearly all the room of a sheet in the near zone goes to the next sheet without being tried (${JSON.stringify(w)}; ${sc.report()})`);
    ok(!sc.pages[0].topup, 'it did not even try sheet 1: no overflow, so sheet 1 is not pushed into its gap fill by an order it could not take (the page without the policy does)');
    section('the near zone applies before the sheet fills its gaps');
  }
  // a line that comes late: the first line must not go to a sheet that is released before the rest of the order arrives
  {
    const run = async settings => {
      const sc = new Scene('late', { settings }); await sc.fillTo(.66, .72);
      const room = sc.room(1), base = L.charm(S0, 'probe', 12, 1, 'p', 0).areaPt2, f = share => Math.sqrt(share * room / base);
      // an order of three lines that asks for 1.4 times the room sheet 1 has left; its first line is small and would fit the gap, the rest comes two updates later
      const O = sc.mk([{ kind: 'single', idx: 12, f: f(.25) }, { kind: 'pair', idx: 12, f: f(.2) }, { kind: 'single', idx: 12, f: f(.75) }]);
      await sc.bring({ order: O, lines: [0] });
      const first = sc.where(O).pages.slice();
      // small single-charm orders fill the last gaps until the sheet is released (full)
      for (let i = 0; i < 40 && !(sc.pages[0].releaseFull); i++) await sc.bring(sc.mk([{ kind: 'single', idx: 1 + (i % 6), f: .9 }], 900 + i));
      const released = !!sc.pages[0].releaseFull;
      await sc.idle(2); await sc.bring({ order: O, lines: [1, 2] }); await sc.idle(2);
      return { sc, O, first, released, w: sc.where(O) };
    };
    const on = await run({}), off = await run({ nospread: 'off' });
    ok(on.w.unplaced === 0 && off.w.unplaced === 0, 'no piece is lost, with the policy or without it');
    ok(off.first.length === 1 && off.first[0] === 1 && off.released && off.w.pages.length === 2, `without the policy the first line goes on sheet 1, which is released before the other lines come: the order is split (${JSON.stringify(off.w)}; ${off.sc.report()})`);
    ok(on.first.length === 1 && on.first[0] !== 1, `A/B: the first line of an order that will not fit the almost complete sheet whole is not put on it (it went to sheet ${on.first[0]})`);
    ok(on.released && on.w.pages.length === 1, `A/B: its late lines find it whole on the next sheet although sheet 1 was released meanwhile (${JSON.stringify(on.w)}; ${on.sc.report()})`);
    section(`a line that comes late: on ${on.w.pages.length} sheet with the policy, on ${off.w.pages.length} sheets without`);
  }
  // a line that comes late finds its order on a sheet that has since grown near completion: it goes there if it fits, and if it does not the order moves on whole (never one line on each sheet)
  for (const share of [.05, 1.4]) {
    const sc = new Scene('home near completion'); await sc.fillTo(.40, .50);
    const room0 = sc.room(1), base = L.charm(S0, 'probe', 12, 1, 'p', 0).areaPt2, f = x => Math.sqrt(x * room0 / base);
    const O = sc.mk([{ kind: 'single', idx: 12, f: f(.04) }, { kind: 'single', idx: 12, f: f(share) }]);
    await sc.bring({ order: O, lines: [0] });
    ok(sc.where(O).pages.join() === '1', 'the first line goes on sheet 1, which is only filling');
    // other orders bring sheet 1 into the near zone (not yet released)
    for (let i = 0; i < 60 && sc.fill(1) < .66; i++) await sc.bring(sc.mk([{ kind: 'single', idx: 1 + (i % 6), f: .8 }], 950 + i));
    ok(sc.fill(1) >= .6 && !sc.pages[0].releaseFull, `sheet 1 is near completion and open (${Math.round(100 * sc.fill(1))}%)`);
    await sc.bring({ order: O, lines: [1] }); await sc.idle(2);
    const w = sc.where(O);
    ok(w.unplaced === 0 && w.pages.length === 1, `the late line ${share < 1 ? 'that fits joins its order on sheet 1' : 'that does not fit takes its order with it to the next sheet'}: ${JSON.stringify(w)} (${sc.report()})`);
    section(`a late line (asking ${Math.round(share * 100)}% of the room sheet 1 had) finds its order on a sheet near completion: the order ends whole on sheet ${w.pages[0]}`);
  }
  // the policy switched off is the page as it was
  {
    const sc = new Scene('off', { settings: { nospread: 'off' } }); await sc.fillTo(.58, .72);
    ok(typeof sc.sim.ctx.nospreadAdmitted === 'function', 'the page has the policy code');
    const NB = sc.discs(5, .97), P = sc.mk([{ kind: 'single', idx: 2, f: 1 }]);
    await sc.bring(NB, P); await sc.idle(2);
    ok(sc.where(NB).unplaced === 0 && sc.where(NB).pages.length === 1, 'the switched-off page keeps orders whole as before');
    section('S.settings.nospread = "off" puts the page back');
  }
  // an order bigger than a sheet: consecutive sheets, the fewest possible, nothing lost, nothing stuck, no loop
  for (const [label, pieces, o] of [['1.7 sheets', 62, {}], ['3.3 sheets', 120, {}]]) {
    const sc = new Scene('big ' + label); await sc.fillTo(.30, .55);
    const before = sc.pages.length;
    const B = sc.mk([{ kind: 'qty', k: pieces, idx: 12, f: 1 }]);
    const after = [0, 1, 2, 3, 4].map(i => sc.mk([{ kind: 'single', idx: i, f: 1 }], 500 + i));
    await sc.bring(B); await sc.bring(...after); await sc.idle(3);
    const w = sc.where(B), area = piecesOf(B).reduce((n, c) => n + sc.sim.ctx.inflatedArea(c), 0), capacity = sc.sim.ctx.usableArea(sc.pages[0]) * 0.8, fewest = Math.ceil(area / capacity - 1e-9);
    ok(w.unplaced === 0, `a piece of the order bigger than a sheet was never placed: ${JSON.stringify(w)} (${sc.report()})`);
    ok(!sc.pages.some(p => p.problem), `no sheet is stuck on a problem: ${sc.pages.filter(p => p.problem).map(p => p.problem).join(' | ')}`);
    ok(w.pages.length <= fewest + 1, `the order fills the fewest sheets possible (+1 for the packing): ${w.pages.length} sheets, ${fewest} by area (${area.toFixed(0)} of ${capacity.toFixed(0)} pt2 a sheet)`);
    ok(sc.searches < 40, `no loop: ${sc.searches} searches in all`);
    for (const a of after) ok(sc.where(a).unplaced === 0 && sc.where(a).pages.length === 1, 'the orders behind it are placed whole');
    const seats = piecesOf(B).map(c => c.chunk); ok(seats.every(x => x >= 1), 'every piece of it belongs to a unit');
    eq([...new Set(piecesOf(B).map(c => c.order))].length, 1, 'the pieces keep their one order, so the sheets that hold them are one unit for a set');
    ok(w.pages.length >= 2, 'it is on more than one sheet');
    section(`an order of ${pieces} pieces (${(area / capacity).toFixed(2)} sheets of area) fills ${w.pages.length} consecutive sheets, the fewest by area ${fewest}, ${sc.searches} searches`);
  }
  // the metals with a green cut line (Rose Gold, 10K, 14K) seat a big order too: consecutive sheets, never "cannot fit on an empty sheet"
  for (const metal of ['rose', 'gold10k', 'gold14k']) {
    const sc = new Scene('big ' + metal, { metal }); await sc.fillTo(.30, .55);
    const B = sc.mk([{ kind: 'qty', k: 62, idx: 12, f: 1 }]);
    const after = [0, 1, 2].map(i => sc.mk([{ kind: 'single', idx: i, f: 1 }], 900 + i));
    await sc.bring(B); await sc.bring(...after); await sc.idle(3);
    const w = sc.where(B);
    ok(w.unplaced === 0, `${metal}: every piece of the order bigger than a sheet is placed: ${JSON.stringify(w)} (${sc.report()})`);
    ok(!sc.pages.some(p => p.problem), `${metal}: no sheet is stuck on a problem: ${sc.pages.filter(p => p.problem).map(p => p.problem).join(' | ')}`);
    ok(w.pages.length >= 2, `${metal}: it is on more than one sheet`);
    eq([...new Set(piecesOf(B).map(c => c.order))].length, 1, `${metal}: the pieces keep their one order`);
    for (const a of after) ok(sc.where(a).unplaced === 0, `${metal}: the orders behind it are placed`);
    ok(sc.searches < 40, `${metal}: no loop: ${sc.searches} searches`);
    section(`${metal}: an order of 62 pieces fills ${w.pages.length} consecutive sheets, nothing stuck`);
  }
  // not interleaved with the spreading of other multi-piece orders
  {
    const sc = new Scene('big and others'); await sc.fillTo(.30, .55);
    const B = sc.mk([{ kind: 'qty', k: 90, idx: 12, f: 1 }]);
    const others = [];
    for (let i = 0; i < 8; i++) others.push(sc.mk([{ kind: i % 2 ? 'pair' : 'discs', k: 4, idx: 3 + i, f: .9 }], 600 + i));
    await sc.bring(B, ...others.slice(0, 4)); await sc.bring(...others.slice(4)); await sc.idle(3);
    ok(sc.where(B).unplaced === 0, 'the big order is placed');
    for (const o of others) { const w = sc.where(o); ok(w.unplaced === 0 && w.pages.length === 1, `an order that came with the big one is whole: ${JSON.stringify(w)}`); }
    section('other multi-piece orders that arrive with a big one are each whole on one sheet');
  }
  // a poor packer: even the units do not pack an empty sheet; the sheet cuts the unit in two and goes on
  {
    const sc = new Scene('poor packer', { maxOrder: .45 }); await sc.fillTo(.20, .45);
    const B = sc.mk([{ kind: 'qty', k: 80, idx: 12, f: 1 }]);
    await sc.bring(B); await sc.idle(3);
    const w = sc.where(B);
    ok(w.unplaced === 0, `the safety net seats every piece of an order whose units do not pack: ${JSON.stringify(w)} (${sc.report()}; ${sc.pages.filter(p => p.problem).map(p => p.problem)})`);
    ok(!sc.pages.some(p => p.problem), 'the run does not wait for a person');
    ok(sc.searches < 80, `no loop: ${sc.searches} searches`);
    section(`a poor packer: the unit that does not pack is cut in two and nested again (${w.pages.length} sheets, ${sc.searches} searches)`);
  }
  // the stall safety net numbers every unit of the order again, those still waiting their turn too: two units never share a number
  {
    const sc = new Scene('renumber'); const O = sc.mk([{ kind: 'qty', k: 30, idx: 12, f: 1 }]), ps = O.lines[0].pieces.map(c => Object.assign({}, c, { pinned: null }));
    ps.forEach((c, i) => { c.chunk = 1 + Math.floor(i / 6); });   // five units of six pieces: 1..5
    const sh = sc.m.newPage(); sh.charms = ps; sh.rejects = ps.slice(0, 18).map(c => c.id); sh.feedWait = ps.slice(18).map(c => c.id); sc.m.pages.push(sh);
    const moving = ps.slice(0, 18), stalled = sc.sim.ctx.rechunkStalled(sh, moving);
    ok(stalled === true, 'a unit of six pieces that does not pack an empty sheet is cut');
    const byNumber = new Map(); for (const c of ps) { if (!byNumber.has(c.chunk)) byNumber.set(c.chunk, []); byNumber.get(c.chunk).push(c); }
    eq([...byNumber.keys()].sort((a, b) => a - b), [1, 2, 3, 4, 5, 6], 'six units, numbered 1 to 6 in order (the two halves of unit 1 are 1 and 2, those that waited are 5 and 6)');
    eq([...byNumber.values()].map(u => u.length).reduce((a, b) => a + b, 0), 30, 'no piece is lost');
    eq(ps.slice(0, 6).map(c => c.chunk), [1, 1, 1, 2, 2, 2], 'the first unit is cut in half, in order');
    eq(ps.slice(24).map(c => c.chunk), [6, 6, 6, 6, 6, 6], 'a unit that waited its turn follows the others');
    ok(sh.status === 'ready' && !sh.rejects.length && sc.m.work.includes(sh), 'the sheet nests again');
    section('the safety net numbers every unit of the order again: two units never share a number');
  }
  // one piece bigger than the sheet is a real problem: said once, no loop
  {
    const sc = new Scene('oversize piece'); await sc.fillTo(.10, .30);
    const area = sc.pages[0].usablePt2, base = L.charm(S0, 'probe', 20, 1, 'p', 0).areaPt2, f = Math.sqrt(1.4 * area / base);
    const X = sc.mk([{ kind: 'single', idx: 20, f }], 700);
    await sc.bring(X); await sc.idle(2);
    ok(sc.where(X).unplaced === 1, 'a piece bigger than the sheet cannot be placed');
    ok(sc.pages.some(p => p.problem && /cannot fit on an empty sheet/.test(p.problem)), 'the sheet says why, as it always did');
    ok(sc.searches < 30, `no loop: ${sc.searches} searches`);
    section('a single piece bigger than a sheet: the plain stop that says so, no loop');
  }
  // a packer that leaves half an order seated: the page lifts it whole (keepOrdersWhole)
  {
    const sc = new Scene('leak', { leak: 1 }); await sc.fillTo(.58, .72);
    const set = [0, 1, 2, 3, 4, 5].map(i => sc.mk([{ kind: i % 2 ? 'discs' : 'pair', k: 4, idx: 8 + i, f: 1.1 }], 800 + i));
    await sc.bring(...set); await sc.idle(3);
    for (const o of set) { const w = sc.where(o); ok(w.unplaced === 0 && w.pages.length === 1, `an order the packer left half seated is whole: ${JSON.stringify(w)} ${sc.report()}`); }
    section('a packer that leaves half an order seated: every order ends whole');
  }
}

/* ═══ 2b · the bridge's side: what the intake knows of an order, and the order a big update is put on the sheets in ═══ */
function bridgeSide() {
  const vm = require('node:vm'), text = fs.readFileSync(path.join(REPO, 'charm-nest-bridge.js'), 'utf8');
  const a = text.indexOf('  function pieceTotals('), b = text.indexOf('  /** The recorded line joins its sheet.', a);
  ok(a > 0 && b > a, 'the bridge has pieceTotals, orderInfo and attachOrder');
  const O = require('../../charm-nest-orders.js');
  const rows = [], pages = { gold: [], silver: [] }, S = { settings: {} };
  const win = { CharmNestNoSpread: NS };   // (the page with the policy module loaded)
  const ctx = vm.createContext({ Orders: { rows: () => rows }, O, pagesOf: m => pages[m], S, window: win, Set, Map, Math, Number, String, Array });
  vm.runInContext(text.slice(a, b) + '\nthis.orderInfo = orderInfo; this.attachOrder = attachOrder;', ctx);
  const row = (rid, state, material, n, extra) => ({ order: { receiptId: rid }, state, spec: Object.assign({ material, pieceCount: n }, extra || {}) });
  const charm = (rid, id, metal) => ({ order: rid, poolId: id, id, metal });
  rows.push(row('R1', 'pulled', 'gold', 2), row('R1', 'held', 'gold', 1), row('R1', 'pulled', 'silver', 1), row('R2', 'pulled', 'gold', 1), row('R3', 'pulled', 'gold', 1), row('R3', 'gone', 'gold', 4), row('R4', 'pooled', 'gold', 2), row('R4', 'pulled', 'gold', 1));
  const pair = [charm('R1', 'p1', 'gold'), charm('R1', 'p2', 'gold')];
  const batch = new Map([['R1|gold', pair]]);
  let info = ctx.orderInfo('R1', 'gold', pair, batch, null);
  eq([info.pieces.length, info.expected, info.placed], [2, 3, 0], 'a pair with a held line of the same metal: 2 in hand, 3 in all (the other metal and a gone line do not count)');
  info = ctx.orderInfo('R1', 'silver', [charm('R1', 's1', 'silver')], new Map(), null);
  eq([info.pieces.length, info.expected, info.placed], [1, 1, 0], 'the silver line of the same order is an order of one for the silver sheets');
  info = ctx.orderInfo('R3', 'gold', [charm('R3', 'q1', 'gold')], new Map(), null);
  eq([info.expected], [1], 'a line that is gone is not counted');
  pages.gold.push({ charms: [charm('R4', 'x1', 'gold'), charm('R4', 'x2', 'gold'), charm('R9', 'y', 'gold')] });
  info = ctx.orderInfo('R4', 'gold', [charm('R4', 'x3', 'gold')], new Map(), null);
  eq([info.pieces.length, info.expected, info.placed], [1, 3, 2], 'the pieces of the order already on a sheet are counted as placed, not as still to come');
  // the order an update is put on the sheets in: the order with a line to come goes after the others, the others keep their order
  const prep = (rid, id) => ({ sp: { material: 'gold' }, charms: [charm(rid, id, 'gold')], custom: false });
  const made = [[row('R1', 'pulled', 'gold', 2), { sp: { material: 'gold' }, charms: pair, custom: false }], [row('R2', 'pulled', 'gold', 1), prep('R2', 'a')], [row('R3', 'pulled', 'gold', 1), prep('R3', 'b')]];
  eq(ctx.attachOrder(made, batch).map(x => x[0].order.receiptId), ['R2', 'R3', 'R1'], 'an order with a line still to come is put on its sheet after the others of the update');
  S.settings.nospread = 'off';
  eq(ctx.attachOrder(made, batch).map(x => x[0].order.receiptId), ['R1', 'R2', 'R3'], 'with the policy off the update keeps its order');
  // a line that is never cut and never pooled (no design: a chain, an extender, packaging; skipped; committed) is no piece to come: a one-charm order with a chain line is still a single-charm order
  S.settings.nospread = undefined; delete S.settings.nospread;
  rows.length = 0; pages.gold.length = 0;
  rows.push(row('N1', 'pulled', 'gold', 1), row('N1', 'noDesign', '', 1, { noDesign: true }), row('N1', 'skipped', 'gold', 2), row('N1', 'committed', 'gold', 1), row('N1', 'pulled', 'gold', 1, { noDesign: true }));
  const one = [charm('N1', 'n1', 'gold')];
  info = ctx.orderInfo('N1', 'gold', one, new Map(), null);
  eq([info.pieces.length, info.expected, info.placed], [1, 1, 0], 'a chain / packaging line (state noDesign or spec.noDesign), a skipped and a committed line are no piece to come');
  const single = NS.orderOf({ pieces: info.pieces.map(c => ({ areaPt2: 100 })), more: Math.max(0, info.expected - info.pieces.length - info.placed), placed: info.placed });
  ok(!single.multi && NS.admits({ usablePt2: 1000, placedPt2: 650, pendingPt2: 0, maxFill: .8, gapFilling: false }, single).take, 'so a one-charm order with a chain line tops up a sheet near completion, as any single charm does');
  rows.push(row('N2', 'pulled', 'gold', 1), row('N2', 'held', 'gold', 1));
  eq(ctx.orderInfo('N2', 'gold', [charm('N2', 'm1', 'gold')], new Map(), null).expected, 2, 'a line held for a question is still to come');
  // the policy module absent (or switched off): the update keeps its order, as before
  const made2 = [[row('N2', 'pulled', 'gold', 1), { sp: { material: 'gold' }, charms: [charm('N2', 'm1', 'gold')], custom: false }], [row('N3', 'pulled', 'gold', 1), prep('N3', 'c')]];
  rows.push(row('N3', 'pulled', 'gold', 1));
  eq(ctx.attachOrder(made2, new Map()).map(x => x[0].order.receiptId), ['N3', 'N2'], 'with the module an order with a line to come goes after the others');
  delete win.CharmNestNoSpread;
  eq(ctx.attachOrder(made2, new Map()).map(x => x[0].order.receiptId), ['N2', 'N3'], 'without the module the update keeps its order');
  section('the bridge: what the intake knows of an order (its other lines, the other metal, gone lines, pieces already placed) and the order of an update');
}

/* ═══ 3 · a day of made-up orders, the policy on and off ═══ */
async function days() {
  const seeds = QUICK ? [1] : [1, 2, 3], orders = QUICK ? 300 : 600;
  const run = async (o, off) => { const rs = []; for (const seed of seeds) { const r = await NL.runDay(Object.assign({ seed, orders, mixed: .08, settings: off ? { nospread: 'off' } : {} }, o)); delete r.sim; rs.push(r); } return rs; };
  const sum = (rs, f) => rs.reduce((n, r) => n + f(r), 0), mean = (rs, f, w) => rs.reduce((n, r) => n + f(r) * w(r), 0) / Math.max(1, rs.reduce((n, r) => n + w(r), 0));
  const row = (name, on, off) => {
    const so = sum(on, r => r.multiOrdersSplit), sf = sum(off, r => r.multiOrdersSplit), mo = sum(on, r => r.partsMulti);
    const uo = mean(on, r => r.all.perSheetMean, r => r.sheets), uf = mean(off, r => r.all.perSheetMean, r => r.sheets);
    const fo = mean(on, r => r.fillClosedGfss, r => r.closed), ff = mean(off, r => r.fillClosedGfss, r => r.closed);
    console.log(`    ${name}: split ${sf} -> ${so} of ${mo} multi-piece orders; unit of a sheet ${uf.toFixed(2)} -> ${uo.toFixed(2)}; fill of released sheets ${(100 * ff).toFixed(1)}% -> ${(100 * fo).toFixed(1)}%`);
    ok(sum(on, r => r.unplaced) === 0 && sum(off, r => r.unplaced) === 0, `${name}: every piece is placed`);
    ok(sum(on, r => (r.problems || []).length) === 0, `${name}: no sheet is stuck on a problem`);
    ok(so <= sf, `${name}: the policy splits no more orders than the page without it (${so} vs ${sf})`);
    // (PAIR, Paul 26 Oct 2026: "Why are there two full GF sheets when I gave you the rules for that?": a GF or SS sheet takes an order's piece only beside its one partner of the other
    // metal, with the policy on or off, so both sides now keep units near one GF + one SS sheet; the policy must not make them bigger than the page without it, or than that pair)
    ok(uo <= Math.max(uf, 2) + 1e-9, `${name}: the units of sheets that share an order are no bigger than without the policy or than one GF + one SS pair (${uo.toFixed(2)} vs ${uf.toFixed(2)})`);
    ok(fo >= ff - 0.01, `${name}: the fill of the released sheets does not fall by more than a point (${(100 * ff).toFixed(1)}% -> ${(100 * fo).toFixed(1)}%)`);
    ok(sum(on, r => r.pairLinesSplit) <= sum(off, r => r.pairLinesSplit), `${name}: no more earring pairs on two sheets`);
    return { so, sf };
  };
  {
    const on = await run({ mix: 'day', late: 0 }), off = await run({ mix: 'day', late: 0 }, true);
    row('a day, every line together', on, off);
    ok(sum(on, r => r.multiOrdersSplit) === 0, 'when all the lines of an order come together, no multi-piece order is on two sheets of a colour');
    ok(sum(on, r => r.pairLinesSplit) === 0, 'no earring pair has its two pieces on two sheets');
  }
  { const on = await run({ mix: 'day', late: .3 }), off = await run({ mix: 'day', late: .3 }, true); row('a day, 30 % of the extra lines come late', on, off); }
  {
    // a bulk update (40 to 60 orders at once: the morning import, a page back after an outage): feedTurn / feedOn / the overflow carry the orders three at a time through sheets that fill within one update
    const o = { mix: 'heavy', late: 0, batches: [40, 60, 30], orders: QUICK ? 300 : 400 }, on = await run(o), off = await run(o, true);
    row('a bulk day (updates of 30 to 60 orders), every line together', on, off);
    ok(sum(on, r => r.multiOrdersSplit) === 0, 'in a bulk update no multi-piece order is on two sheets of a colour');
    const l = Object.assign({}, o, { late: .2 }), lon = await run(l), loff = await run(l, true);
    row('a bulk day, 20 % of the extra lines come late', lon, loff);
  }
  if (!QUICK) { const on = await run({ mix: 'heavy', late: .3 }), off = await run({ mix: 'heavy', late: .3 }, true); const r = row('a heavy day (45 % multi-piece), 30 % late', on, off); ok(r.so <= Math.max(1, r.sf / 3), `the heavy day: splits fall to a third or less (${r.sf} -> ${r.so})`); }
  {
    // a day of earring pairs only (or the last orders of a day): a pair that cannot fit the almost complete sheet is turned away at the door, so nothing is rejected -- the sheet is released all the same
    const o = { mix: 'pairs', late: 0, orders: QUICK ? 150 : 200, perSheet: 20, mixed: 0 }, on = await run(o), off = await run(o, true);
    console.log(`    a day of earring pairs only: sheets released ${sum(off, r => r.closed)} of ${sum(off, r => r.sheets)} -> ${sum(on, r => r.closed)} of ${sum(on, r => r.sheets)}`);
    ok(sum(on, r => r.unplaced) === 0 && sum(on, r => (r.problems || []).length) === 0, 'a day of pairs only: every piece placed, no sheet stuck');
    ok(sum(on, r => r.closed) >= sum(off, r => r.closed), `a day of pairs only: the policy releases as many sheets as the page without it (${sum(on, r => r.closed)} vs ${sum(off, r => r.closed)})`);
    ok(sum(on, r => r.multiOrdersSplit) === 0, 'a day of pairs only: no pair is on two sheets');
  }
  {
    const big = [{ pieces: 120, metal: 'gold', design: [10, 1], at: .3 }, { pieces: 200, metal: 'silver', design: [12, 1], at: .6 }];
    const on = await run({ mix: 'day', late: .1, orders: QUICK ? 300 : 400, big }), off = await run({ mix: 'day', late: .1, orders: QUICK ? 300 : 400, big }, true);
    ok(sum(on, r => r.unplaced) === 0, 'a day with two orders bigger than a sheet: every piece is placed');
    ok(sum(off, r => r.unplaced) > 0 && sum(off, r => (r.problems || []).length) > 0, 'for the record: the page without the policy leaves such an order unplaced and its sheet stuck ("cannot fit on an empty sheet")');
    for (const r of on) for (const b of r.big) ok(b.miss === 0 && b.sheets <= b.fewest + 1, `a big order of ${b.pieces} pieces is on ${b.sheets} sheets, ${b.fewest} by area`);
    console.log(`    a day with 2 big orders: unplaced pieces ${sum(off, r => r.unplaced)} -> ${sum(on, r => r.unplaced)}; sheets stuck ${sum(off, r => (r.problems || []).length)} -> ${sum(on, r => (r.problems || []).length)}`);
  }
}

/* ═══ 4 · the files ═══ */
function files() {
  const html = fs.readFileSync(path.join(REPO, 'charm-nest-1.html'), 'utf8'), build = fs.readFileSync(path.join(REPO, 'scripts/build-public.cjs'), 'utf8');
  ok(/<script src="charm-nest-nospread\.js\?v=[^"]+"><\/script>/.test(html), 'charm-nest-1.html loads charm-nest-nospread.js');
  ok(build.includes('"charm-nest-nospread.js"'), 'scripts/build-public.cjs publishes charm-nest-nospread.js');
  const bridge = fs.readFileSync(path.join(REPO, 'charm-nest-bridge.js'), 'utf8');
  ok(/intakePage\(metal, run, row\.order\.receiptId, list, info\)/.test(bridge) && /function intakePage\(m, run, orderId, pieces, order\)/.test(bridge), 'the intake is told the whole order');
}

module.exports = { Scene };
if (require.main === module) (async () => {
  const t0 = Date.now();
  console.log('NOSPREAD'); section('1 · the pure policy'); policy();
  console.log('  2 · the page\'s rules, scene by scene'); await scenes();
  bridgeSide();
  console.log('  3 · a day of made-up orders, policy on and off'); await days();
  files();
  console.log(`PASS: nospread: ${checks} checks (${Math.round((Date.now() - t0) / 1000)}s; fake orders, a stand-in packer, offline)`);
})().catch(e => { console.error('FAIL: ' + (e && e.stack || e)); process.exit(1); });
