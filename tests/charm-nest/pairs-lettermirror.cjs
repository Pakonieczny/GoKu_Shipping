// LETTERMIRROR (Paul, 10 Oct 2026, 15:42 UTC): "If they are necklaces, then they should not be mirrored; if they are stud earrings, or hoop/huggie earrings, they need to be mirrored."
// Every earring pair is a Left and an EXACT-MIRROR Right, letters, initials, numbers and scripts included. The old exemption (charm-nest-pair.js readsOneWay: a design named a
// letter, initial, number or script, or holding facing "X", was cut as drawn on both ears; PAIRMIRROR decision 4) is gone. Necklaces, discs, pendants and singles are never mirrored.
// The line decides, never the design's name: the same letter design on a necklace line stays one unmirrored piece per counted option.
//   node tests/charm-nest/pairs-lettermirror.cjs          (offline, no network, nothing live, nothing paid; about 90 s with the real master, a few seconds without)
//   LETTERMIRROR_FIXTURES=1 node tests/charm-nest/pairs-lettermirror.cjs     fixtures only (skips the real master file)
// Proves, with the real charm-nest-orders.js (the intake), charm-nest-pair.js, charm-nest-pair-thumb.js, charm-nest-pool-pieces.js and charm-nest-engrave-sides.js:
//  1  the 80 earring-named SKUs the old rule exempted (LETTER_EARRING 26, INITIAL_LETTER_STUD_EARRINGS 25, INITIAL_HEART_EARRING 26, HUGGIE HOOPS- LOWERCASE INITIALS, HUGGIE HOOPS- ANGEL NUMBER,
//     COMIC NUMBER (HUGGIE)) on a stud, hoop, huggie or Pair-option line (quantity 1 and 2): Left as drawn, Right = exact x-mirror, geometry and pool-row fields
//  2  the same designs on a necklace, pendant, charm or counted-disc line, and as a single earring: no side, no mirror, as many pieces as before
//  3  an old facing "X" on a record stops nothing; the Master tab no longer offers "reads one way"; the engraving text of a mirrored Right is still the very same, unreversed object
//  4  the thumbnail of the pair line draws the Left, then the Right turned over (once)
//  5  the REAL master file (/mnt/project-files/MASTER SKU_CUSTOMS-MISC_MV_2026-0914.ai, parsed by the app's own reader) when present: all 80 designs, Right = exact mirror of Left
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const O = require('../../charm-nest-orders.js');
const Pair = require('../../charm-nest-pair.js');
const PT = require('../../charm-nest-pair-thumb.js');
const PoolPieces = require('../../charm-nest-pool-pieces.js');
const Sides = require('../../charm-nest-engrave-sides.js');
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; };
const close = (a, b, e) => Math.abs(a - b) <= (e == null ? 1e-9 : e);

/* ── the 80 SKUs (the live index, 9 Oct re-index) ── */
const seq = (k, f) => Array.from({ length: k }, (_, i) => f(i));
const EARRING_SKUS = [].concat(
  seq(26, i => 'LETTER_EARRING-' + i),
  seq(26, i => i).filter(i => i !== 8).map(i => 'INITIAL_LETTER_STUD_EARRINGS-' + i),   // (-0 to -25 without -8: 25 SKUs, as the live index holds them)
  seq(26, i => 'INITIAL_HEART_EARRING_' + i),
  ['HUGGIE HOOPS- LOWERCASE INITIALS', 'HUGGIE HOOPS- ANGEL NUMBER', 'COMIC NUMBER (HUGGIE)']);
ok(EARRING_SKUS.length === 80 && new Set(EARRING_SKUS).size === 80, '1a the 80 earring-named SKUs');
ok(EARRING_SKUS.every(s => Pair.readsOneWay({ sku: s })), '1b every one of them is a name the OLD rule exempted from mirroring (it is the set Paul decided on)');

/* ── a letter design: a chiral "F" with a hole, a hoop ring off to one side, an engraving stroke and an engraved text member ── */
const poly = (pts, extra) => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]); return Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)], subpaths: [pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['h']])] }, extra || {}); };
const F_SHAPE = [[0, 0], [6, 0], [6, 12], [16, 12], [16, 17], [6, 17], [6, 24], [22, 24], [22, 30], [0, 30]];
function letter(sku, extra) {
  const outline = poly(F_SHAPE, { index: 1 }), hole = poly([[2, 3], [4, 3], [4, 5], [2, 5]], { index: 2 }), ring = poly([[1, 30], [5, 30], [5, 34], [1, 34]], { index: 3 });
  const stroke = Object.assign(poly([[8, 26], [20, 27.5]], { index: 4, closed: false, layer: 'ENGRAVE', strokeRGB: [1, 0, 0] }), { subpaths: [[['m', [8, 26]], ['l', [20, 27.5]]]] });
  const text = { kind: 'text', index: 5, layer: 'ENGRAVE', text: 'AB', bbox: [8, 18, 14, 22], reversed: false };
  const members = [outline, hole, ring, stroke, text];
  return Object.assign({ id: 'c-' + sku, sourceId: 's', name: sku, sku, outline, members, bbox: [0, 0, 22, 34], topIndices: [1, 2, 3, 4, 5], extras: [], centerPt: [11, 17], strokePt: .5, upAngle: 90 }, extra || {});
}
const ptsOf = seg => seg.subpaths.flatMap(sp => sp.filter(o => o[0] !== 'h').flatMap(o => o.slice(1).filter(Array.isArray)));
const pathsOf = c => c.members.filter(m => m.kind === 'path');
/** The Right is the exact x-mirror of the Left (every path member, the box), about the Left's own box centre; the mirror of the mirror is the Left. */
function exactMirror(L, R, tag, eps) {
  const cx = (L.bbox[0] + L.bbox[2]) / 2, lm = pathsOf(L), rm = pathsOf(R);
  ok(lm.length === rm.length && lm.length > 0, tag + ': the same path members');
  let worst = 0;
  for (let i = 0; i < lm.length; i++) { const a = ptsOf(lm[i]), b = ptsOf(rm[i]); ok(a.length === b.length, tag + ': member ' + i + ' keeps its points'); for (let k = 0; k < a.length; k++) worst = Math.max(worst, Math.abs(b[k][0] - (2 * cx - a[k][0])), Math.abs(b[k][1] - a[k][1])); }
  worst = Math.max(worst, Math.abs(R.bbox[0] - (2 * cx - L.bbox[2])), Math.abs(R.bbox[2] - (2 * cx - L.bbox[0])), Math.abs(R.bbox[1] - L.bbox[1]), Math.abs(R.bbox[3] - L.bbox[3]));
  ok(worst <= (eps == null ? 1e-9 : eps), tag + ': Right = exact x-mirror of Left (worst error ' + worst + ')');
  const back = Pair.mirrorOf(R, cx), bm = pathsOf(back);
  ok(bm.length === lm.length && bm.every((m, i) => { const a = ptsOf(m), b = ptsOf(lm[i]); return a.length === b.length && a.every((q, k) => Math.abs(q[0] - b[k][0]) <= (eps == null ? 1e-9 : eps) && Math.abs(q[1] - b[k][1]) <= (eps == null ? 1e-9 : eps)); }), tag + ': the mirror of the mirror is the Left');
}

/* ── the intake: a real line through charm-nest-orders.js, then the pair module ── */
const order = { receiptId: '4171043395', updateTs: 1789100000 };
const V = (name, value) => ({ name, value });
const read = (line, sku) => { const spec = O.interpretLine(order, line, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => (s === sku ? { sku } : null) }); return { order, line, spec }; };
const lineOf = (sku, title, variations, quantity) => ({ transactionId: '5213726668', listingId: '1718', sku, title, quantity: quantity || 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [], variations: variations || [V('Metal Choice', 'Silver')] });
const lineFor = r => Object.assign({ receiptId: order.receiptId }, r.line, { quantity: r.spec.quantity, form: r.spec.form, spec: r.spec });
const word = ps => ps.map(p => (p.side || '-') + (p.mirror ? 'm' : '')).join();

/* ── 1 · every lettering earring SKU on an earring line: a Left as drawn and a Right that is its exact mirror ── */
const EAR_LINES = [
  ['stud earrings', 'Initial Letter Stud Earrings Gold Dainty Studs Personalized Gift', null],
  ['hoop earrings', 'Letter Hoop Earrings Gold Initial Hoops', null],
  ['huggie hoops', 'Angel Number Huggie Hoop Earrings Minimalist Small Hoops', null],
  ['a Pair option', 'Initial Charm Personalized Gift', [V('Metal Choice', 'Silver'), V('Single or Pair', 'Pair')]]
];
{
  let checked = 0;
  for (const sku of EARRING_SKUS) {
    const c = letter(sku), before = JSON.stringify(c.members.filter(m => m.kind === 'path').map(m => m.subpaths));
    for (const [what, title, vars] of EAR_LINES) {
      for (const q of [1, 2]) {
        const r = read(lineOf(sku, title, vars, q), sku), ln = lineFor(r), ps = Pair.piecesFor(ln, c);
        ok(r.spec.pair && r.spec.pair.earring === true && r.spec.pieceCount === 2 * q, sku + ' / ' + what + ': an earring pair line, two pieces per unit');
        ok(word(ps) === (q === 1 ? 'L,Rm' : 'L,Rm,L,Rm'), sku + ' / ' + what + ' x' + q + ': Left as drawn, Right the mirror (' + word(ps) + ')');
        checked++;
      }
    }
    // the geometry of the two pieces of one unit, as the pool and the laser file take it
    const [pl, pr] = Pair.piecesFor(lineFor(read(lineOf(sku, EAR_LINES[0][1]), sku)), c);
    const Lg = Pair.pieceGeometry(c, pl), Rg = Pair.pieceGeometry(c, pr);
    ok(Lg === c && Rg !== c && Rg.mirror === true && Rg.mirrored === true, sku + ': the Left is the drawing itself, the Right is a new, mirrored charm');
    exactMirror(Lg, Rg, sku);
    ok(JSON.stringify(c.members.filter(m => m.kind === 'path').map(m => m.subpaths)) === before, sku + ': the master drawing is untouched');
    // the pool row fields the page records for each piece (side, mirror, group): a Right row says mirror true
    const fr = PoolPieces.fieldsOf(pr), fl = PoolPieces.fieldsOf(pl);
    ok(fl.side === 'L' && fl.mirror === false && fr.side === 'R' && fr.mirror === true && fl.groupSize === 2 && fr.groupSize === 2, sku + ': the pool rows say Left as drawn, Right mirror, group of two');
  }
  ok(checked === 80 * 4 * 2, '1c checked every SKU on every earring line at quantity 1 and 2: ' + checked);
  // the other lettering names an earring line may carry (not earring-named: sold as earrings they are earrings)
  for (const sku of ['INITIAL HEART', 'LETTER_7', 'NUMBER 5', 'SCRIPTURE_2', 'ALPHABET LETTER A', 'BIBLE SCRIPTURE (HUGGIE)']) {
    const r = read(lineOf(sku, 'Stud Earrings'), sku);
    ok(word(Pair.piecesFor(lineFor(r), letter(sku))) === 'L,Rm', '1d ' + sku + ' sold as stud earrings: the Right is the mirror');
  }
  // a person's word still decides WHICH ear the drawing is (the facing), exactly as for every design: the pair stays a mirror pair
  ok(word(Pair.piecesFor(lineFor(read(lineOf('LETTER_EARRING-3', 'Stud Earrings'), 'LETTER_EARRING-3')), letter('LETTER_EARRING-3', { facing: 'R' }))) === 'Lm,R', '1e a facing word on a letter design turns the other ear, as for any design');
  // an old facing "X" (the former "reads one way") says nothing any more
  const X = letter('LETTER_EARRING-4', { facing: 'X' });
  ok(Pair.facingOf(X) === null && word(Pair.piecesFor(lineFor(read(lineOf('LETTER_EARRING-4', 'Stud Earrings'), 'LETTER_EARRING-4')), X)) === 'L,Rm', '1f a design holding the old facing X is mirrored on an earring pair');
  ok(word(Pair.piecesFor(lineFor(read(lineOf('LETTER_EARRING-4', 'Stud Earrings'), 'LETTER_EARRING-4')), { sku: 'LETTER_EARRING-4', facing: 'X' })) === 'L,Rm', '1g the order window asks with the index entry only (no drawing): the same answer');
  ok(word(Pair.piecesFor(lineFor(read(lineOf('LETTER_EARRING-4', 'Stud Earrings'), 'LETTER_EARRING-4')), null)) === 'L,Rm', '1h and with nothing at all (the server): the same answer');
}

/* ── 2 · the same designs on a necklace, pendant, charm or counted-disc line, and as a single earring: never mirrored ── */
{
  // (the intake reads the shop's own earring SKUs as earrings unless the title names another product or the buyer chose the loose charm: so a necklace line says "necklace", a charm-only line chose "Charm only")
  const NECK = [['necklace', 'Initial Letter Necklace Gold Personalized Gift', [V('Metal Choice', 'Silver'), V('Necklace Length', '18"')]], ['pendant', 'Letter Pendant Gold Initial Charm', [V('Metal Choice', 'Silver'), V('Necklace Length', '18"')]], ['charm only', 'Angel Number Charm Add On Charm', [V('Metal Choice', 'Silver'), V('Charm Type', 'Charm only')]]];
  for (const sku of EARRING_SKUS) {
    const c = letter(sku);
    for (const [what, title, vars] of NECK) {
      const r = read(lineOf(sku, title, vars), sku), ps = Pair.piecesFor(lineFor(r), c);
      ok(r.spec.pair.earring === false && ps.length === 1 && ps[0].side === null && ps[0].mirror === false, sku + ' / ' + what + ': one piece, no side, not mirrored (' + word(ps) + ')');
      ok(Pair.pieceGeometry(c, ps[0]) === c, sku + ' / ' + what + ': cut from the drawing itself');
    }
    // the same design, quantity 2 on a necklace: two plain copies, still no side and no mirror
    const r2 = read(lineOf(sku, NECK[0][1], null, 2), sku);
    ok(word(Pair.piecesFor(lineFor(r2), c)) === '-,-', sku + ': two necklaces are two plain copies');
    // a single earring that names no ear: cut as drawn
    const s = read(lineOf(sku, 'Initial Stud Earrings', [V('Metal Choice', 'Silver'), V('Single or Pair', 'Single')]), sku), sp = Pair.piecesFor(lineFor(s), c);
    ok(sp.length === 1 && sp[0].side === null && sp[0].mirror === false, sku + ': a single earring is one piece, no side, not mirrored (' + word(sp) + ')');
  }
  // a counted-option necklace keeps making one piece per counted option (3 discs: three pieces, no side, no mirror), whatever the design is called
  for (const sku of ['INITIAL_DISC', 'INITIAL_LETTER_STUD_EARRINGS-3']) {
    const r = read(lineOf(sku, 'Initial Disc Necklace Gold', [V('Number of Discs / Metal', '3 discs • gold')]), sku), ps = Pair.piecesFor(lineFor(r), letter(sku));
    ok(r.spec.pieceCount === 3 && ps.length === 3 && ps.every(p => p.side === null && p.mirror === false), sku + ' on a 3-disc necklace: three pieces, none mirrored (' + word(ps) + ')');
  }
  // the raw word forms the pair module also reads (no intake): a necklace form and a pendant are never mirrored
  const c = letter('LETTER_EARRING-1');
  ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, form: 'necklace' }, c).every(p => p.side === null && !p.mirror) && Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1 }, c).every(p => p.side === null && !p.mirror), '2a a necklace or a line with no form is never mirrored');
  ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, spec: { quantity: 1, pieceCount: 3, form: 'necklace' } }, c).every(p => p.side === null && !p.mirror), '2b three counted pieces: no side, no mirror');
  // a single earring that NAMES its ear is that ear (Amendment 3): by the same rule as every design, its Right is the mirror
  const named = read(lineOf('LETTER_EARRING-1', 'Custom Single Replacement Letter Earring Right Ear', [V('Metal Choice', 'Silver')]), 'LETTER_EARRING-1'), np = Pair.piecesFor(lineFor(named), c);
  const plainNamed = Pair.piecesFor(lineFor(read(lineOf('MITTENS 1', 'Custom Single Replacement Mitten Earring Right Ear', [V('Metal Choice', 'Silver')]), 'MITTENS 1')), letter('MITTENS 1'));
  ok(np.length === 1 && np[0].side === 'R' && word(np) === word(plainNamed), '2c a single earring that names its Right ear is treated like every other design (' + word(np) + ' = ' + word(plainNamed) + ')');
}

/* ── 3 · the Master tab and the engraving text ── */
{
  const ctl = Pair.facingControl;
  const sym = { sku: 'LETTER_EARRING-3', sym: 'directional' };
  ok(ctl(sym).options.map(o => o[0]).join() === ',L,R' && ctl(sym).options[0][1] === 'faces: not set', '3a a letter design shows "not set / faces left / faces right": "reads one way" would stop nothing, so it is not offered');
  ok(ctl(Object.assign({ facing: 'X' }, sym)).options.map(o => o[0]).join() === ',L,R,X' && /no longer used/.test(ctl(Object.assign({ facing: 'X' }, sym)).options[3][1]), '3b a record that still holds X shows it, saying it is no longer used, so it can be cleared');
  ok(!/reads one way|cut as drawn/i.test(ctl(sym).hint), '3c the explanation no longer promises that a design is cut as drawn');
  // the engraved text of a mirrored Right is the very same object, as drawn (never reversed); the geometry the fit sees is the mirrored one
  const c = letter('LETTER_EARRING-5'), text = c.members.find(m => m.kind === 'text');
  const pool = new Map([['P1', { side: 'L', mirror: false, bodyIndex: 0 }], ['P2', { side: 'R', mirror: true, bodyIndex: 0 }]]);
  const ctx = { poolRow: id => pool.get(id), pair: Pair };
  const Lc = Sides.pieceCharm(ctx, 'P1', c), Rc = Sides.pieceCharm(ctx, 'P2', c);
  ok(Lc === c && Rc !== c && Rc.mirrored === true && Rc.members.includes(text) && text.reversed === false && text.bbox.join() === '8,18,14,22', '3d the engraving fit is made on the mirrored letter, and its text stays the same unreversed object');
  exactMirror(Lc, Rc, '3d the geometry the engraving is fitted to');
  // a piece record with a side but no mirror flag asks the same question and now agrees with the cut
  ok(Sides.mirrorOfId({ poolRow: () => ({ side: 'R' }), pair: Pair }, 'P3', c) === true && Sides.mirrorOfId({ poolRow: () => ({ side: 'L' }), pair: Pair }, 'P4', c) === false, '3e a stored Right with no flag is the mirror, a Left is not');
}

/* ── 4 · the thumbnail of the pair line: Left, then the Right turned over ── */
{
  const recorder = () => { const log = []; const ctx = new Proxy({}, { get: (t, k) => k === 'canvas' ? ctx._cv : (k in t ? t[k] : (...a) => { log.push([k, a]); }), set: (t, k, v) => { t[k] = v; log.push(['set:' + k, v]); return true; } }); return { log, make: (w, h) => { const cv = { width: w, height: h, getContext: () => ctx }; ctx._cv = cv; return cv; } }; };
  const turns = log => log.filter(l => l[0] === 'scale' && l[1][0] === -1 && l[1][1] === 1).length;
  const chips = log => log.filter(l => l[0] === 'fillText').map(l => l[1][0]);
  PT.use(Pair);
  let drew = []; const fakeP = { drawCharm: (ctx, c, tx, k) => drew.push({ c, k, p: tx(0, 0), q: tx(22, 34) }) };
  for (const sku of ['LETTER_EARRING-7', 'INITIAL_LETTER_STUD_EARRINGS-7', 'INITIAL_HEART_EARRING_7', 'HUGGIE HOOPS- LOWERCASE INITIALS', 'HUGGIE HOOPS- ANGEL NUMBER', 'COMIC NUMBER (HUGGIE)']) {
    const c = letter(sku), mp = PT.matchPlan(c, { sku });
    ok(mp && mp.bodies.map(b => b.side).join() === 'L,R' && mp.bodies.map(b => b.mirror).join() === 'false,true', '4a ' + sku + ': the plan is the Left as drawn and the Right turned over');
    ok(PT.matchPlan(c, { sku, facing: 'X' }).bodies.map(b => b.mirror).join() === 'false,true', '4b ' + sku + ': an old facing X changes nothing');
    const piece = Pair.piecesFor(lineFor(read(lineOf(sku, 'Stud Earrings'), sku)), c);
    ok(piece.map(p => p.mirror).join() === mp.bodies.map(b => b.mirror).join(), '4c ' + sku + ': the picture turns the same ear the piece is cut turned');
    const r = recorder(); drew = [];
    const cv = PT.canvasFor(fakeP, c, { size: 220, padPt: 3, bg: '#fff', pair: true, sku, makeCanvas: r.make });
    ok(cv && drew.length === 2 && drew[0].k === drew[1].k && drew[1].p[0] > drew[0].q[0] && turns(r.log) === 1 && chips(r.log).join() === 'Left,Right', '4d ' + sku + ': drawn twice side by side, exactly one copy (the Right) turned, chips Left and Right');
  }
}

/* ── 5 · the real master file, when present (the app's own reader; no write, no network) ── */
async function realMaster() {
  const file = process.env.LETTERMIRROR_MASTER || '/mnt/project-files/MASTER SKU_CUSTOMS-MISC_MV_2026-0914.ai';
  if (process.env.LETTERMIRROR_FIXTURES || !fs.existsSync(file)) { console.log('pairs-lettermirror: real master file not used (' + (process.env.LETTERMIRROR_FIXTURES ? 'LETTERMIRROR_FIXTURES set' : 'not found: ' + file) + '); fixtures only'); return; }
  const g = globalThis; g.window = g.window || g; g.self = g.self || g;
  g.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js'); require('../../charm-nest-pdf.js');
  const P = g.CharmNestPDF, buf = fs.readFileSync(file);
  const parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), 'master.ai');
  const grouped = P.groupCharms(parsed, { minPt: 6 });
  const lab = P.labelCharms(parsed, grouped.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / (25.4 / 72), widen: 0.25 });
  const want = new Set(EARRING_SKUS), seen = new Set();
  for (const [index, l] of lab.labels) {
    const mine = [l.sku, ...(l.extra || []).map(x => x.sku)].filter(Boolean).map(String).filter(s => want.has(s)); if (!mine.length) continue;
    const c = grouped.charms.find(x => x.index === index); if (!c) continue;
    const b = c.bbox; Object.assign(c, { id: 'real:' + mine[0], sourceId: 's', name: mine[0], sku: mine[0], centerPt: [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2], strokePt: c.strokePt || .5 });
    for (const sku of mine) {
      seen.add(sku);
      const cs = Object.assign({}, c, { sku, name: sku });
      ok(Pair.bodiesOf(cs).length === 1 && !Pair.isMismatched(cs), '5a ' + sku + ': one body');
      ok(cs.members.every(m => m.kind === 'path'), '5b ' + sku + ': only paths (no text or image member is left unmirrored)');
      const r = read(lineOf(sku, 'Initial Stud Earrings'), sku), ps = Pair.piecesFor(lineFor(r), cs);
      ok(word(ps) === 'L,Rm', '5c ' + sku + ': Left as drawn, Right the mirror (' + word(ps) + ')');
      const Lg = Pair.pieceGeometry(cs, ps[0]), Rg = Pair.pieceGeometry(cs, ps[1]);
      ok(Lg === cs && Rg.mirror === true, '5d ' + sku + ': Left is the master drawing, Right is the mirrored charm');
      exactMirror(Lg, Rg, '5e real ' + sku, 1e-6);
      const nk = read(lineOf(sku, 'Initial Letter Necklace Gold', [V('Metal Choice', 'Silver'), V('Necklace Length', '18"')]), sku);
      ok(word(Pair.piecesFor(lineFor(nk), cs)) === '-', '5f ' + sku + ': the same real design on a necklace line is one plain piece');
    }
  }
  ok(seen.size === 80, '5g all 80 designs were found on the real master by their labels: ' + seen.size);
  console.log('pairs-lettermirror: real master read (' + grouped.charms.length + ' charms, ' + lab.labels.size + ' labelled); 80 lettering earring designs: Right = exact mirror of Left');
}

realMaster().then(() => console.log('pairs-lettermirror: ' + n + ' checks passed'), e => { console.error(e); process.exit(1); });
