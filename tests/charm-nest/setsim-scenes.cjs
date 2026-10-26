/* SETSIM scenes: a handful of small, explicit situations from Paul's 13 Oct 2026 request, each run through the sorter's own feed / intake / keep-whole / overflow / gap-fill
 * code (the slices setsim-lib.cjs makes) around the real solver, on FAKE orders only (order ids made up here, no buyer, no Etsy, no Firestore, no paid call).
 *   node tests/charm-nest/setsim-scenes.cjs [--root DIR] [--only late,queue,queue2,pair,lines,chain] [--metal gold|silver|rose] [--sheet 60x40] [--step 60] [--measure] [--json F]
 * A scene is red (exit 1, the exact case printed) when an order's pieces end on two sheets, a piece is lost or waits to the end, or a multi-piece order
 * that cannot fit a nearly full sheet takes room that small single-charm orders could have filled.  --measure prints the same without failing.
 * Scenes:
 *   closable (NOSPREAD) a sheet at the release fill (74-79%) that only earring pairs come to close: every pair is turned away at the door (nothing rejects), so the sheet must be released all the same, by ONE more nest
 *           with nothing new on it (the 'nothing new went in' path): layout unchanged, no second record, no loop, no piece lost or doubled.  --off runs the page with S.settings.nospread = 'off' for comparison.
 *   big     (NOSPREAD) an order of about 2.4 sheets of area arrives with single charms behind it: it fills consecutive sheets, none stuck on "cannot fit on an empty sheet", none lost.
 *   late    a pair of an order sits on an OPEN sheet (about 65% full); the order's other line (5 discs) arrives 2 updates later and does not fit the gap.
 *           The WHOLE order must be on one sheet at the end (the pair moves with the discs, or the discs stay: never one on each).
 *   queue   an OPEN sheet about 65% full; one update brings a 5-disc necklace (cannot fit whole) and then three tiny single-charm orders.
 *           The necklace goes whole to one sheet; the tiny singles fill the first sheet's gaps; nothing is lost and nothing waits to the end.
 *   queue2  as queue, with three older necklaces ahead of four tiny singles in one update: the singles still take the gaps, and the necklaces spend none of the sheet's 35 tries.
 *   pair    an OPEN sheet about 68% full; an earring pair of which one piece fits the gap and two do not.  Both pieces on one sheet.
 *   lines   an order of three lines (a pair, 3 copies of one charm, a single), all one metal, arrives in one update on a half-full sheet: one sheet.
 *   chain   three orders of 4-5 pieces on three consecutive updates onto a sheet about 60% full, then singles: each order whole, none lost, waits small.
 * Fake-data limits: the sheet is smaller than the shop's default stock (60x40 mm here against 100x50; 36x24 also works but is too small for the gap fill to start), angles are coarse (60 degrees) so a scene runs in a few minutes on a busy machine,
 * and a scene shows what the code does on this made-up case; it does not prove what the live sorter does.                                                               */
'use strict';
const fs = require('node:fs'), path = require('node:path');
const Lib = require('./setsim-lib.cjs');
const { orderOf, partOf, piecesOf, Sim, fillOf, L } = Lib;

const argv = process.argv.slice(2), opt = (k, d) => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : d; };
const root = path.resolve(opt('root') || Lib.REPO), step = +opt('step', 60), sheet = opt('sheet', '60x40'), measure = argv.includes('--measure');
const only = String(opt('only', 'late,queue,queue2,pair,lines,chain,big,closable')).split(',').filter(Boolean), metal = opt('metal', 'gold');
const S = L.solverOf(root);

let nextId = 0;
const oid = () => String(7000000000 + (++nextId) * 3);
const mk = (id, n, specs) => orderOf(S, id, n, specs.map(sp => Object.assign({ metal }, sp)));   // (every line of the scene is of the scene's metal)

class Scene {
  constructor(name) {
    this.name = name; this.sim = new Sim({ root, step, seed: 1, sheet }); this.m = this.sim.metal(metal); this.tick = 0;
    if (argv.includes('--off')) Object.assign(this.sim.ctx.S.settings, { nospread: 'off' });   // (the page with the policy switched off, for comparison)
    this.orders = []; this.arrived = new Map(); this.done = new Map(); this.sentLines = new Map(); this.fails = []; this.notes = []; this.t0 = Date.now();
  }
  get pages() { return this.m.pages; }
  /** entries: an order (all its lines arrive) or { order, lines: [index...] } (only those lines arrive in this update) */
  async bring(...entries) {
    this.tick++; const parts = [];
    for (const e of entries) {
      const o = e.order || e, idx = e.order ? e.lines : o.lines.map((_, i) => i);
      if (!this.orders.includes(o)) { this.orders.push(o); this.arrived.set(o.id, this.tick); this.sentLines.set(o.id, new Set()); }
      const before = [...this.sentLines.get(o.id)].reduce((n, i) => n + o.lines[i].pieces.length, 0);
      idx.forEach(i => this.sentLines.get(o.id).add(i)); const part = partOf(o, metal, idx.map(i => o.lines[i]));
      // (NOSPREAD, Paul 17 Oct 2026: the intake knows how many pieces the order will have in all, lines still to come included, and how many came before this update)
      if (part) { part.expected = o.lines.filter(l => l.metal === metal).reduce((n, l) => n + l.pieces.length, 0); part.order = { pieces: part.pieces, expected: part.expected, placed: before }; }
      parts.push(part);
    }
    await this.m.step(parts.filter(Boolean), this.tick); this.mark();
  }
  async idle(n = 1) { for (let i = 0; i < n; i++) { this.tick++; await this.m.step([], this.tick); this.mark(); } }
  placedIds() { return new Set(this.pages.flatMap(p => p.placements.map(q => q.id))); }
  mark() {
    const placed = this.placedIds();
    for (const o of this.orders) if (!this.done.has(o.id) && this.sentLines.get(o.id).size === o.lines.length && piecesOf(o).every(c => placed.has(c.id))) this.done.set(o.id, this.tick);
  }
  /** page number -> number of the order's pieces on it; unplaced = pieces nowhere */
  where(o) {
    const m = new Map(), placed = this.placedIds();
    for (const p of this.pages) for (const q of p.placements) { const c = p.charms.find(x => x.id === q.id); if (c && c.order === o.id) m.set(p.page, (m.get(p.page) || 0) + 1); }
    return { pages: m, unplaced: piecesOf(o).filter(c => !placed.has(c.id)).length, whole: m.size <= 1 };
  }
  fill(page) { const p = this.pages.find(x => x.page === page); return p ? fillOf(p) : 0; }
  room(page) { const p = this.pages.find(x => x.page === page); return p && p.usablePt2 ? (0.8 - fillOf(p)) * p.usablePt2 : 0; }
  /** fill sheet 1 with mid-size singles, one order per update, until its fill is within [lo, hi] (and it is still open) */
  async fillTo(lo, hi) {
    const designs = [[9, 1], [13, 1], [8, 1.02], [11, .98], [10, 1.05], [12, 1], [5, 1.1], [17, .9], [4, 1.1], [7, .95], [3, 1.1], [6, .95], [2, 1.15], [1, 1.2]];
    for (let i = 0, k = 0; k < 40 && !(this.fill(1) >= lo); k++) {
      const n = this.fill(1) < lo - .2 ? 4 : this.fill(1) < lo - .08 ? 2 : 1, batch = [];   // (a bigger sheet takes more singles per update to get near the target)
      for (let j = 0; j < n; j++, i++) { const [idx, f] = designs[i % designs.length]; batch.push(mk(oid(), 100 + i, [{ kind: 'single', idx, f }])); }
      await this.bring(...batch);
    }
    const p = this.pages[0];
    this.setup = { fill: +this.fill(1).toFixed(3), closed: !!(p && p.releaseFull), inRange: this.fill(1) >= lo && this.fill(1) <= hi };
    this.notes.push(`sheet 1 before the case: ${Math.round(100 * this.setup.fill)}% full${this.setup.closed ? ', already released' : ', open'}`);
    return this.setup;
  }
  expect(ok, text) { if (!ok) this.fails.push(text); return ok; }
  report(extra) {
    const rows = this.pages.filter(p => p.placements.length).map(p => ({ page: p.page, fill: +fillOf(p).toFixed(3), released: !!p.releaseFull, pieces: p.placements.length, orders: [...new Set(p.placements.map(q => (p.charms.find(c => c.id === q.id) || {}).order))].length }));
    return { scene: this.name, ok: !this.fails.length, fails: this.fails, notes: this.notes, sheets: rows, ticks: this.tick, seconds: Math.round((Date.now() - this.t0) / 1000), ...(extra || {}) };
  }
}
/** The scale at which `k` copies of library charm `idx` together are `share` times the room a sheet has left to the fill ceiling: a necklace that cannot fit however the sheet is cut. */
const bigScale = (sc, page, k, idx, share = 1.6) => Math.min(2.2, Math.max(.5, Math.sqrt(share * sc.room(page) / (k * L.charm(S, 'probe', idx, 1, 'probe', 0).areaPt2))));
const label = (sc, o) => `${o.id}(${o.lines.map(l => l.kind + 'x' + l.pieces.length).join('+')})`;
const where = (sc, o) => { const w = sc.where(o); return `${label(sc, o)} on sheets {${[...w.pages].map(([p, n]) => p + ':' + n).join(', ')}}${w.unplaced ? ' + ' + w.unplaced + ' unplaced' : ''}`; };
const precond = (sc, text, ok) => { if (!ok) { sc.fails.push('SCENE NOT REACHED (the setup did not come out as designed, so this case was not tested): ' + text); sc.unreached = true; } return ok; };

const SCENES = {
  async late() {
    const sc = new Scene('late');
    await sc.fillTo(.52, .68); precond(sc, `sheet 1 should be open and about 60% full: it is ${Math.round(100 * sc.setup.fill)}%${sc.setup.closed ? ' and released' : ''}`, sc.setup.inRange && !sc.setup.closed);
    const B = mk(oid(), 200, [{ kind: 'pair', idx: 0, f: .8 }, { kind: 'discs', k: 5, idx: 8, f: bigScale(sc, 1, 5, 8) }]);
    await sc.bring({ order: B, lines: [0] });
    const w1 = sc.where(B); sc.notes.push('the pair arrives: ' + where(sc, B));
    precond(sc, 'the pair should sit on sheet 1 before the discs come: ' + where(sc, B), w1.pages.size === 1 && w1.pages.has(1) && !sc.pages[0].releaseFull);
    await sc.idle(2); await sc.bring({ order: B, lines: [1] }); await sc.idle(2);
    sc.notes.push('after the 5 discs arrive 2 updates later: ' + where(sc, B));
    const w = sc.where(B);
    sc.expect(w.unplaced === 0, `a piece of ${label(sc, B)} was never placed: ${where(sc, B)}`);
    sc.expect(w.pages.size <= 1, `ORDER SPLIT: ${where(sc, B)} (the pair stayed on sheet 1 and the late discs went elsewhere)`);
    return sc.report({ order: where(sc, B) });
  },
  async queue() {
    const sc = new Scene('queue');
    await sc.fillTo(.58, .72); precond(sc, `sheet 1 should be open and about 65% full: it is ${Math.round(100 * sc.setup.fill)}%${sc.setup.closed ? ' and released' : ''}`, sc.setup.inRange && !sc.setup.closed);
    const before = sc.fill(1), N = mk(oid(), 300, [{ kind: 'discs', k: 5, idx: 8, f: bigScale(sc, 1, 5, 8) }]);
    const T = [0, 1, 0].map((idx, i) => mk(oid(), 301 + i, [{ kind: 'single', idx, f: 1 }]));
    await sc.bring(N, ...T);
    await sc.idle(2);
    const w = sc.where(N), tinyOn1 = T.filter(o => sc.where(o).pages.has(1)).length, after = sc.fill(1);
    sc.notes.push(`necklace: ${where(sc, N)}; tiny singles on sheet 1: ${tinyOn1} of 3; sheet 1 ${Math.round(100 * before)}% -> ${Math.round(100 * after)}%`);
    const tp = sc.pages[0].topup, tried = tp ? (tp.tried || []).map(String) : null;
    if (tp) sc.notes.push(`sheet 1 gap fill: ${tried.length} orders have tried it${tp.closedAt ? ', closed (' + (tp.why || '?') + ')' : ''}${tp.passed ? ', turned away: ' + tp.passed.length : ''}; the necklace ${tried.includes(N.id) ? 'COUNTED' : 'did not count'} as a try`);
    else sc.notes.push('sheet 1 had no gap-fill record' + (sc.pages[0].releaseWhy ? ' (released: ' + sc.pages[0].releaseWhy + ')' : ''));
    if (tp && !tp.closedAt) sc.expect(!tried.includes(N.id), `A TRY WAS SPENT on a necklace that cannot fit sheet 1 whole (sheet 1 offers only ${35} tries to later orders): tried=${tried.length}`);
    for (const o of [N, ...T]) sc.expect(sc.where(o).unplaced === 0, `a piece of ${label(sc, o)} was never placed: ${where(sc, o)}`);
    sc.expect(w.pages.size <= 1, `ORDER SPLIT: ${where(sc, N)}`);
    sc.expect(tinyOn1 >= 1, `GAPS NOT FILLED: the necklace (5 discs, cannot fit sheet 1 whole) was held back, but none of the 3 tiny single-charm orders took sheet 1's gaps (${T.map(o => where(sc, o)).join('; ')})`);
    sc.expect((sc.done.get(N.id) || 99) - sc.arrived.get(N.id) <= 2, `the necklace waited ${(sc.done.get(N.id) || 99) - sc.arrived.get(N.id)} updates`);
    return sc.report({ order: where(sc, N), tinyOnSheet1: tinyOn1, sheet1Before: +before.toFixed(3), sheet1After: +after.toFixed(3) });
  },
  async queue2() {
    // three older necklaces that cannot fit the gap, then four tiny single-charm orders, all in one update, on an open sheet that is about 60% full
    const sc = new Scene('queue2');
    await sc.fillTo(.52, .70); precond(sc, `sheet 1 should be open and about 60% full: it is ${Math.round(100 * sc.setup.fill)}%${sc.setup.closed ? ' and released' : ''}`, sc.setup.inRange && !sc.setup.closed);
    const before = sc.fill(1), f = bigScale(sc, 1, 5, 8, 1.3);
    const N = [0, 1, 2].map(i => mk(oid(), 700 + i, [{ kind: 'discs', k: 5, idx: [8, 10, 12][i], f }])), T = [0, 1, 0, 1].map((idx, i) => mk(oid(), 710 + i, [{ kind: 'single', idx, f: 1 }]));
    await sc.bring(...N, ...T); await sc.idle(3);
    const tinyOn1 = T.filter(o => sc.where(o).pages.has(1)).length, after = sc.fill(1), tp = sc.pages[0].topup, tried = tp ? (tp.tried || []).map(String) : null;
    sc.notes.push(`necklaces: ${N.map(o => where(sc, o)).join(' | ')}; tiny singles on sheet 1: ${tinyOn1} of 4; sheet 1 ${Math.round(100 * before)}% -> ${Math.round(100 * after)}%${tried ? '; orders that have tried sheet 1: ' + tried.length + ' (necklaces among them: ' + N.filter(o => tried.includes(o.id)).length + (tp.passed ? ', turned away: ' + tp.passed.length : '') + ')' : ''}`);
    for (const o of [...N, ...T]) { const w = sc.where(o); sc.expect(w.unplaced === 0, `a piece of ${label(sc, o)} was never placed: ${where(sc, o)}`); sc.expect(w.pages.size <= 1, `ORDER SPLIT: ${where(sc, o)}`); }
    sc.expect(tinyOn1 >= 2, `GAPS NOT FILLED: only ${tinyOn1} of 4 tiny single-charm orders took sheet 1's gaps while three necklaces that cannot fit were ahead of them`);
    if (tp && !tp.closedAt) sc.expect(N.every(o => !tried.includes(o.id)), `TRIES SPENT: ${N.filter(o => tried.includes(o.id)).length} of 3 necklaces that cannot fit counted as one of the 35 tries of sheet 1`);
    return sc.report({ tinyOnSheet1: tinyOn1, sheet1Before: +before.toFixed(3), sheet1After: +after.toFixed(3) });
  },
  async pair() {
    const sc = new Scene('pair');
    await sc.fillTo(.55, .72); precond(sc, `sheet 1 should be open and about 60% full: it is ${Math.round(100 * sc.setup.fill)}%${sc.setup.closed ? ' and released' : ''}`, sc.setup.inRange && !sc.setup.closed);
    // a design whose one piece fits the gap (about 60% of the room left to the fill ceiling) and whose two pieces do not
    const room = sc.room(1), base = L.charm(S, 'probe', 12, 1, 'probe', 0), f = Math.sqrt(.6 * room / base.areaPt2);
    const P = mk(oid(), 400, [{ kind: 'pair', idx: 12, f }]);
    await sc.bring(P); await sc.idle(2);
    const w = sc.where(P); sc.notes.push(`gap before: ${Math.round(room)} pt2, one piece ${Math.round(piecesOf(P)[0].areaPt2)} pt2; ${where(sc, P)}`);
    sc.expect(w.unplaced === 0, `a piece of ${label(sc, P)} was never placed: ${where(sc, P)}`);
    sc.expect(w.pages.size <= 1, `PAIR SPLIT: ${where(sc, P)} (one earring on each sheet)`);
    return sc.report({ order: where(sc, P) });
  },
  async lines() {
    const sc = new Scene('lines');
    await sc.fillTo(.40, .62);
    sc.notes.push(`sheet 1 before the case: ${Math.round(100 * sc.fill(1))}% full`);
    const O = mk(oid(), 500, [{ kind: 'pair', idx: 3, f: 1 }, { kind: 'qty', k: 3, idx: 1, f: 1 }, { kind: 'single', idx: 13, f: 1 }]);
    await sc.bring(O); await sc.idle(2);
    const w = sc.where(O); sc.notes.push(where(sc, O));
    sc.expect(w.unplaced === 0, `a piece of ${label(sc, O)} was never placed: ${where(sc, O)}`);
    sc.expect(w.pages.size <= 1, `ORDER SPLIT: ${where(sc, O)}`);
    return sc.report({ order: where(sc, O) });
  },
  async big() {
    // NOSPREAD (Paul, 17 Oct 2026): "a given order so big that it fills several sheets and the system must be smart enough and robust enough": 64 copies of a charm, about 2.5 sheets of this size, arrive with
    // single-charm orders behind it. Consecutive sheets, the fewest, nothing lost, no sheet stuck on "cannot fit on an empty sheet"; the orders behind it whole.
    const sc = new Scene('big');
    const B = mk(oid(), 800, [{ kind: 'qty', k: 64, idx: 12, f: 1 }]);   // (a 60 x 40 mm sheet seats about 26 of these at the real solver's fill)
    const sm = [0, 1, 2, 3].map(i => mk(oid(), 810 + i, [{ kind: 'single', idx: i, f: 1 }]));
    await sc.bring(B); await sc.bring(...sm); await sc.idle(3);
    const w = sc.where(B);
    sc.notes.push(`big order: ${piecesOf(B).length} pieces: ${where(sc, B)}; ${sc.pages.filter(p => p.problem).length} sheet(s) stuck`);
    sc.expect(w.unplaced === 0, `a piece of ${label(sc, B)} was never placed: ${where(sc, B)}`);
    sc.expect(!sc.pages.some(p => p.problem), 'a sheet is stuck: ' + sc.pages.filter(p => p.problem).map(p => p.problem).join(' | '));
    sc.expect(w.pages.size >= 2 && w.pages.size <= 4, `the order should fill 2 to 4 consecutive sheets, it is on ${w.pages.size}`);
    for (const o of sm) { sc.expect(sc.where(o).unplaced === 0, `a piece of ${label(sc, o)} was never placed`); sc.expect(sc.where(o).pages.size <= 1, `ORDER SPLIT: ${where(sc, o)}`); }
    return sc.report({ order: where(sc, B) });
  },
  async closable() {
    // NOSPREAD (Paul, 17 Oct 2026): the policy turns a multi-piece order away from a sheet that cannot take it whole BEFORE it joins, so nothing is rejected and the miss that used to close the sheet never happens. A sheet at the
    // release fill that only earring pairs come to is closed all the same: the refusal is counted (nospreadPassed) and the sheet is nested once more with nothing new on it (nospreadSettle), which finishNest takes as "nothing new went in".
    const sc = new Scene('closable');
    // (the real solver on this small sheet stops near 70-73% and closes a sheet itself by a missed single before 75% is in reach: the fill ceiling is lowered so that the release fill, min(75%, ceiling), is within its reach.
    //  --maxfill .80 --fill .66 is the same case with the gap fill (below the release fill) as the outcome)
    sc.sim.ctx.S.settings.maxFill = +opt('maxfill', .68);
    // the page's own layoutKey (the test finishNest makes for "nothing new went in": the layout is the one already written), taken from its text
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), at = html.indexOf('const layoutKey = '), layoutKey = require('node:vm').runInNewContext('(' + html.slice(at + 'const layoutKey = '.length, html.indexOf('\n', at)).replace(/;\s*$/, '') + ')');
    // sheet 1: mid-size singles to about 60%, then tiny ones (they fit the last gaps; a mid-size single that misses would close the sheet before the case starts) one at a time to where the real solver stops: near completion, still open
    await sc.fillTo(.56, .70);
    for (let i = 0; i < 80 && sc.fill(1) < +opt('fill', .677) && !sc.pages[0].releaseFull; i++) await sc.bring(mk(oid(), 950 + i, [{ kind: 'single', idx: i % 4, f: .55 + .1 * (i % 3) }]));
    sc.setup = { fill: +sc.fill(1).toFixed(3), closed: !!sc.pages[0].releaseFull }; sc.notes.push(`sheet 1 before the case: ${Math.round(100 * sc.setup.fill)}% full${sc.setup.closed ? ', already released' : ', open'}`);
    precond(sc, `sheet 1 should be open and 62% to the ceiling full (--fill N sets how far the tiny singles go, default .677 with the ceiling at .68, within the .4 point the release test allows): it is ${Math.round(100 * sc.setup.fill)}%${sc.setup.closed ? ' and released' : ''}`, sc.setup.fill >= .62 && sc.setup.fill <= +opt('maxfill', .68) && !sc.setup.closed);
    const p1 = sc.pages[0], key = () => layoutKey(p1, new Map(p1.charms.map(c => [c.id, c])));
    const was = { placements: JSON.stringify(p1.placements), key: key(), searches: p1.searches, sheets: sc.pages.length, released: !!p1.releaseFull, topup: !!p1.topup };
    sc.notes.push(`policy ${typeof sc.sim.ctx.nospread === 'function' && sc.sim.ctx.nospread() ? 'ON' : 'OFF'}; fill ceiling ${sc.sim.ctx.S.settings.maxFill}`);
    sc.notes.push(`sheet 1 before the pairs: ${Math.round(100 * sc.fill(1))}% full, ${p1.searches} searches, ${p1.placements.length} pieces, released ${was.released}, gap fill ${was.topup}`);
    // four earring pairs of different designs, each pair asking for about twice the room sheet 1 has left: none fits it whole
    const room = sc.room(1), base = idx => L.charm(S, 'probe', idx, 1, 'probe', 0).areaPt2;
    const P = [3, 5, 9, 12].map((idx, i) => mk(oid(), 900 + i, [{ kind: 'pair', idx, f: Math.sqrt(1.0 * room / base(idx)) }]));
    await sc.bring(...P); await sc.idle(5);   // (half an hour of quiet is needed: 10 simulated minutes an update)
    const searches = p1.searches - was.searches, sheets = sc.pages.filter(p => p.placements.length).length;
    sc.notes.push(`pairs: ${P.map(o => where(sc, o)).join(' | ')}`);
    sc.notes.push(`sheet 1 after: released ${!!p1.releaseFull}, gap fill ${p1.topup ? (p1.topup.closedAt ? 'closed' : 'open') : 'none'}, ${searches} more search(es), layout ${key() === was.key ? 'unchanged' : 'CHANGED'}; sheets with pieces ${was.sheets} -> ${sheets}`);
    for (const o of P) { const w = sc.where(o); sc.expect(w.unplaced === 0, `a piece of ${label(sc, o)} was never placed: ${where(sc, o)}`); sc.expect(w.pages.size <= 1, `PAIR SPLIT: ${where(sc, o)}`); }
    sc.expect(!sc.pages.some(p => p.problem), 'a sheet is stuck: ' + sc.pages.filter(p => p.problem).map(p => p.problem).join(' | '));
    const ids = sc.pages.flatMap(p => p.charms.map(c => c.id)); sc.expect(new Set(ids).size === ids.length, 'a piece is on two sheets');
    const placed = sc.pages.flatMap(p => p.placements.map(q => q.id)); sc.expect(new Set(placed).size === placed.length, 'a piece is placed twice');
    sc.expect(JSON.stringify(p1.placements) === was.placements, 'sheet 1 was rearranged');
    if (!argv.includes('--off')) {
      const cap = +opt('maxfill', .68), shown = Math.round(100 * sc.fill(1)) / 100;
      sc.expect(!!p1.releaseFull || !!p1.topup, `sheet 1 (${Math.round(100 * sc.fill(1))}%) was neither released nor put on its gap fill although every pair was turned away`);
      if (shown >= Math.min(.75, cap)) sc.expect(!!p1.releaseFull, `sheet 1 (${Math.round(100 * sc.fill(1))}% full, the release fill is ${Math.round(100 * Math.min(.75, cap))}%) was not released`);
      sc.expect(searches === 1, `sheet 1 was nested ${searches} more times; the one-off nest must be exactly one (no loop)`);
      sc.expect(key() === was.key, 'sheet 1 layout key changed: finishNest would not take the "nothing new went in" path');
      sc.expect(sheets >= was.sheets + 1 && sheets <= was.sheets + 3, `sheets with pieces: ${was.sheets} -> ${sheets} (the four pairs go whole to one or two new sheets)`);
    }
    return sc.report({ searches, released: !!p1.releaseFull });
  },
  async chain() {
    const sc = new Scene('chain');
    await sc.fillTo(.52, .66); precond(sc, `sheet 1 should be open and about 60% full: it is ${Math.round(100 * sc.setup.fill)}%${sc.setup.closed ? ' and released' : ''}`, sc.setup.inRange && !sc.setup.closed);
    const big = [mk(oid(), 600, [{ kind: 'discs', k: 5, idx: 8, f: .9 }]), mk(oid(), 601, [{ kind: 'trio', designs: [[9, 1], [13, 1], [10, 1]] }, { kind: 'single', idx: 4, f: 1 }]), mk(oid(), 602, [{ kind: 'pair', idx: 17, f: 1 }, { kind: 'pair', idx: 5, f: 1 }])];
    for (const o of big) await sc.bring(o);
    const sm = [0, 1, 2, 3].map(i => mk(oid(), 610 + i, [{ kind: 'single', idx: i, f: 1 }]));
    await sc.bring(...sm); await sc.idle(3);
    for (const o of [...big, ...sm]) {
      const w = sc.where(o);
      sc.expect(w.unplaced === 0, `a piece of ${label(sc, o)} was never placed: ${where(sc, o)}`);
      sc.expect(w.pages.size <= 1, `ORDER SPLIT: ${where(sc, o)}`);
    }
    const waits = [...big, ...sm].map(o => sc.done.has(o.id) ? sc.done.get(o.id) - sc.arrived.get(o.id) : 99);
    sc.notes.push('orders: ' + [...big, ...sm].map(o => where(sc, o)).join(' | '), 'waits (updates): ' + waits.join(','));
    sc.expect(Math.max(...waits) <= 3, `an order waited ${Math.max(...waits)} updates (99 = never placed)`);
    return sc.report({ waits });
  },
};

(async () => {
  const results = [], t0 = Date.now();
  for (const name of only) {
    if (!SCENES[name]) { console.error('unknown scene ' + name); process.exit(2); }
    let r; try { r = await SCENES[name](); } catch (e) { r = { scene: name, ok: false, fails: ['the scene crashed: ' + (e && e.stack || e)], notes: [], sheets: [] }; }
    results.push(r);
    console.log(`${r.ok ? 'PASS' : 'FAIL'}  ${r.scene}  (${r.seconds || 0}s)`); for (const n of r.notes) console.log('      ' + n); for (const f of r.fails) console.log('      !! ' + f);
    console.log('      sheets: ' + r.sheets.map(s => `${s.page}:${Math.round(100 * s.fill)}%${s.released ? '*' : ''}/${s.orders}o`).join('  ') + '   (* = released)');
  }
  const bad = results.filter(r => !r.ok);
  console.log(`\nscenes: ${results.length - bad.length} pass, ${bad.length} fail   root ${root}   metal ${metal}   sheet ${sheet} mm   angle step ${step}   ${Math.round((Date.now() - t0) / 1000)}s   (fake orders; not live proof)`);
  if (opt('json')) fs.writeFileSync(opt('json'), JSON.stringify({ root, metal, sheet, step, results }, null, 1));
  process.exit(bad.length && !measure ? 1 : 0);
})();
