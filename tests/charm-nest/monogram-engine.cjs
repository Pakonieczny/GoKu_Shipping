// MONOGRAM engine (Paul, 15 Oct 2026): three buyer letters from three alphabets become ONE interconnected charm piece, the centre letter bigger than the sides,
// every letter keeping its shape and its counters. Pure geometry, offline, no browser:
//   · lettersOf: what the buyer wrote (40 strings),  familiesOf: the 26-letter alphabets a SKU list holds (6,000 mixed SKUs),
//   · build: size rule, overlap ladder, connectivity, counters kept, slivers, degenerate input, determinism, speed,
//   · toPdf: the finished piece read back by the app's own reader as ONE charm with the right holes, and svgPath.
//   node tests/charm-nest/monogram-engine.cjs
'use strict';
const assert = require('assert');
const C = require('../../vendor/clipper-6.4.2.js');
const M = require('../../charm-nest-monogram.js');
const { CharmNestPDF: PDF } = require('../../netlify/functions/_charmNestPdf.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const MM = 25.4 / 72;

// ── synthetic block letters (ring lists, not fonts): outer outline + one ring per counter, in each letter's own file coordinates ───────────────────────────────
const ell = (cx, cy, rx, ry, k = 64, wob = 0, lobes = 5) => Array.from({ length: k }, (_, i) => { const t = (i / k) * 2 * Math.PI, r = 1 + wob * Math.sin(lobes * t); return [cx + rx * r * Math.cos(t), cy + ry * r * Math.sin(t)]; });
const rect = (x0, y0, x1, y1) => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]];
const A = { letter: 'A', rings: [[[0, 0], [5, 0], [6.5, 9], [13.5, 9], [15, 0], [20, 0], [11.5, 30], [8.5, 30]], [[8.2, 14], [11.8, 14], [10, 23]]] };
const Pp = { letter: 'P', rings: [[[0, 0], [5, 0], [5, 12], [12, 12], [17, 16], [17, 26], [12, 30], [0, 30]], [[5, 17], [11, 17], [11, 25], [5, 25]]] };
const V = { letter: 'V', rings: [[[0, 30], [5, 30], [10, 10], [15, 30], [20, 30], [12, 0], [8, 0]]] };
const O = { letter: 'O', rings: [ell(10, 15, 10, 15, 48), ell(10, 15, 5, 10, 48)] };
const Mm = { letter: 'M', rings: [[[0, 0], [5, 0], [5, 20], [10, 8], [15, 20], [15, 0], [20, 0], [20, 30], [15, 30], [10, 16], [5, 30], [0, 30]]] };
const block = (letter, w, h) => ({ letter, rings: [rect(0, 0, w, h)] });
const scaleL = (l, k) => ({ letter: l.letter, rings: l.rings.map(r => r.map(p => [p[0] * k, p[1] * k])) });
const moveL = (l, dx, dy) => ({ letter: l.letter, rings: l.rings.map(r => r.map(p => [p[0] + dx, p[1] + dy])) });
const hOf = l => Math.max(...l.rings.flat().map(p => p[1])) - Math.min(...l.rings.flat().map(p => p[1]));
const shoelace = pts => pts.reduce((a, p, i) => a + (p[0] * pts[(i + 1) % pts.length][1] - pts[(i + 1) % pts.length][0] * p[1]), 0) / 2;
const inPoly = (pt, poly) => { let c = false; for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) if ((poly[i][1] > pt[1]) !== (poly[j][1] > pt[1]) && pt[0] < (poly[j][0] - poly[i][0]) * (pt[1] - poly[i][1]) / (poly[j][1] - poly[i][1]) + poly[i][0]) c = !c; return c; };
const outerOf = r => r.rings.filter(x => !x.hole), holesOf = r => r.rings.filter(x => x.hole);
const heightOf = pl => pl.bboxPt[3] - pl.bboxPt[1];
const by = (r, slot) => r.placed.find(p => p.slot === slot);
// independent read of a result: every letter as its own filled region, placed where `placed` says, and how much of it is left visible by the other two (shape retained)
const regionOf = (l, pl) => { const pts = l.rings.flat(), x0 = Math.min(...pts.map(p => p[0])), y0 = Math.min(...pts.map(p => p[1]));
  const c = new C.Clipper(), sol = new C.Paths(); c.AddPaths(l.rings.map(r => r.map(p => ({ X: Math.round((p[0] - x0) * pl.scale * 1000 + pl.bboxPt[0] * 1000), Y: Math.round((p[1] - y0) * pl.scale * 1000 + pl.bboxPt[1] * 1000) }))), C.PolyType.ptSubject, true);
  c.Execute(C.ClipType.ctUnion, sol, C.PolyFillType.pftEvenOdd, C.PolyFillType.pftEvenOdd); return sol; };
const areaOf = ps => ps.reduce((a, p) => a + C.Clipper.Area(p), 0);
const visibleShare = (r, inputs) => { const regs = ['left', 'middle', 'right'].map((s, i) => regionOf(inputs[i], by(r, s)));
  return regs.map((g, i) => { const others = [].concat(...regs.filter((_, j) => j !== i)), c = new C.Clipper(), sol = new C.Paths(); c.AddPaths(g, C.PolyType.ptSubject, true); c.AddPaths(others, C.PolyType.ptClip, true); c.Execute(C.ClipType.ctDifference, sol, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero); return areaOf(sol) / areaOf(g); }); };
// the structural promises of every finished piece
function sound(r, what) {
  ok(r.ok === true && r.why === '', what + ': built');
  ok(outerOf(r).length === 1 && r.rings[0].hole === false, what + ': ONE outer ring, first');
  ok(shoelace(r.rings[0].pts) > 0, what + ': the outer ring runs counter-clockwise');
  ok(holesOf(r).every(h => shoelace(h.pts) < 0), what + ': every hole runs clockwise');
  ok(holesOf(r).every(h => h.pts.every(p => inPoly(p, r.rings[0].pts))), what + ': every hole lies inside the outer ring');
  ok(r.bboxPt[0] === 0 && r.bboxPt[1] === 0 && Math.abs(r.wMm - r.bboxPt[2] * MM) < 1e-3 && Math.abs(r.hMm - r.bboxPt[3] * MM) < 1e-3, what + ': bbox starts at 0,0, wMm/hMm are the bbox in mm');
  const xs = r.rings[0].pts.map(p => p[0]), ys = r.rings[0].pts.map(p => p[1]);
  ok(Math.abs(Math.max(...xs) - r.bboxPt[2]) < 2e-3 && Math.abs(Math.max(...ys) - r.bboxPt[3]) < 2e-3 && Math.min(...xs) === 0 && Math.min(...ys) === 0, what + ': bboxPt is the outer ring\'s bbox');
  ok(r.report.connected === true && Array.isArray(r.report.notes) && r.report.tries >= 1 && r.placed.length === 3, what + ': report');
}

// ── 1 · lettersOf: what the buyer wrote ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
const READ = [
  ['APV', 'APV'], ['A P V', 'APV'], ['apv', 'APV'], ['a.p.v', 'APV'], ['A.P.V.', 'APV'], ['A-P-V', 'APV'], ['A, P, V', 'APV'], ['A/P/V', 'APV'], ['  O  S  R  ', 'OSR'], ['osr', 'OSR'],
  ['Á-P-V', 'APV'], ['É.M.Ñ', 'EMN'], ['Åsa Öberg', null], ['ÀÈÌ', 'AEI'], ['Ø.Ł.Đ', 'OLD'],
  ['Initials: ASK', 'ASK'], ['Initials: A S K', 'ASK'], ['initials A.S.K please', 'ASK'], ['Monogram for APV please', 'APV'], ['Letters: J, K, L', 'JKL'], ['Initials: AND', 'AND'],
  ['M & J & S', 'MJS'], ['A and B and C', 'ABC'], ['A 😀 P 😀 V', 'APV'], ['😀APV😀', 'APV'], ['★A★P★V★', 'APV'], ['A​P​V', 'APV'], ['A P V', 'APV'], ['ＡＰＶ', 'APV'], ['A_P_V', 'APV'], ['A1P2V3', 'APV'], ['\n\tK\nL\tM\n', 'KLM'],
  ['AP', null], ['A', null], ['APVX', null], ['A P V X', null], ['123', null], ['', null], ['   ', null], ['!!! ??? ...', null], ['Hello', null], ['Mary Ann Lee', null], ['Jane Doe', null], ['Initials: Jane Doe', null], ['Initials: A P V (large)', null], ['Initials', null], ['Initials: AP', null], ['АПВ', null], [null, null], [undefined, null], [42, null]
];
{
  eq(READ.length >= 40, true, 'the table holds at least 40 buyer strings');
  for (const [text, want] of READ) {
    const r = M.lettersOf(text);
    if (want === null) { ok(r.ok === false && r.letters === null && typeof r.why === 'string' && r.why.length > 3 && !/\n/.test(r.why), `lettersOf(${JSON.stringify(text)}) is refused with a plain reason, got ${JSON.stringify(r)}`); }
    else { ok(r.ok === true && r.letters.join('') === want && r.letters.length === 3, `lettersOf(${JSON.stringify(text)}) -> ${want}, got ${JSON.stringify(r)}`); }
  }
  eq(M.lettersOf('AP').why, '2 letters, a monogram needs 3', 'the plain reason for two letters');
  eq(M.lettersOf('A').why, '1 letter, a monogram needs 3', 'one letter');
  eq(M.lettersOf('APVX').why, '4 letters, a monogram needs 3', 'four letters');
  ok(/digit/.test(M.lettersOf('123').why) && /needs 3/.test(M.lettersOf('123').why), 'digits only is said so');
  ok(/nothing|no letters/.test(M.lettersOf('').why), 'empty is said so');
  eq(M.lettersOf('Jane Doe').why, '7 letters, a monogram needs 3', 'a name is never read as three initials (words are not dropped unless they are known label words)');
  eq(M.lettersOf('Initials').why, 'no letters written, only words', 'a label with no letters');
  ok(M.lettersOf('Initials: ASK').letters.every(c => /^[A-Z]$/.test(c)), 'letters come back as single upper-case A to Z');
}

// ── 2 · familiesOf: the alphabets a SKU list holds ───────────────────────────────────────────────────────────────────────────────────────────────────────────
let seed = 20261015; const rnd = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 4294967296; };
const pick = a => a[Math.floor(rnd() * a.length)];
const ALPHA = [...'ABCDEFGHIJKLMNOPQRSTUVWXYZ'];
{
  const WORDS = 'HEART STAR MOON SUN SKULL CAT DOG HORSE FLOWER ROSE DAISY TULIP LOTUS KEY LOCK ANCHOR COMPASS FEATHER WING ANGEL CROSS CLOVER BEE BUTTERFLY SPIDER WEB GHOST PUMPKIN WITCH BAT OWL FOX WOLF BEAR DEER MITTENS SNOWFLAKE TREE LEAF FERN PINE WAVE SHELL PEARL DIAMOND CROWN CASTLE DRAGON UNICORN MERMAID TRIDENT ARROW BOW RING HOOP DISC BAR TAG COIN BOOK PEN CAMERA GUITAR PIANO NOTE'.split(' ');
  const FORMS = ['(HUGGIE)', 'EARRING', 'STUD', 'NECKLACE', 'HOOP'];
  const skus = new Set();
  while (skus.size < 6000) {
    const w = pick(WORDS) + (rnd() < .3 ? ' ' + pick(WORDS) : ''), code = String(1000 + Math.floor(rnd() * 9000)), r = rnd();
    if (r < .35) skus.add(w.replace(/ /g, rnd() < .5 ? '_' : ' ') + '_' + code);                         // HEART_9491, SCHRODINGER CAT_4571
    else if (r < .5) skus.add(w + ' ' + (1 + Math.floor(rnd() * 3)) + (rnd() < .4 ? ' ' + pick(FORMS) : ''));  // SCHRODINGER CAT 2 (HUGGIE)
    else if (r < .6) skus.add(w.replace(/ /g, '-') + '_' + (80000 + Math.floor(rnd() * 19999)));        // T-REX_84495
    else if (r < .7) skus.add('INITIAL_' + pick(WORDS) + '_' + code);                                     // INITIAL_DISC_4571
    else if (r < .8) skus.add(pick(WORDS) + '_' + (1 + Math.floor(rnd() * 12)));                          // numbered designs, never 20
    else if (r < .9) skus.add(w + '-' + pick(['S', 'M', 'L']));                                          // sizes
    else skus.add(pick(WORDS) + pick(WORDS));
  }
  for (const lit of ['HEART_9491', 'SCHRODINGER CAT 2 (HUGGIE)', 'SCHRODINGER CAT 1', 'SCHRODINGER CAT 2', 'INITIAL_DISC_4571', 'T-REX_84495', 'KEY_5598', 'LETTER_EARRING-3']) skus.add(lit);   // the shop's own kinds
  const real = [];
  const fam = (build, drop) => ALPHA.filter(L => !(drop || []).includes(L)).map(build);
  real.push(...fam(L => `MONOGRAM_LEFT_${L}`));                              // STEM_A
  real.push(...fam((L, i) => `MIDDLE-MONO-${L.toLowerCase()}`, ['Q']));        // STEM-a, 25 letters, lower case
  real.push(...ALPHA.map((L, i) => `INITIAL_RIGHT_${i}`));                     // STEM_0 .. STEM_25 (the app's INDEXED family)
  real.push(...ALPHA.map(L => `Script Letter ${L}`));                         // STEM A, mixed case
  real.push(...ALPHA.map((L, i) => `FANCYSET${i + 1}`));                       // STEM1 .. STEM26
  real.push(...ALPHA.map(L => `SWASHFONT${L}`));                             // STEMA, joined, 9-char stem
  real.push(...ALPHA.map(L => `LOVELY_${L}_8832`));                           // STEM_A_1234 shop code
  const traps = [];
  traps.push(...ALPHA.slice(0, 19).map(L => `NINETEEN_${L}`));                // 19 letters: not a family
  traps.push(...Array.from({ length: 40 }, (_, i) => `HEART_NUMBERED_${i + 1}`));   // numbered series that runs past 26
  traps.push(...ALPHA.slice(0, 18).map(L => `ODD_${L}_5555`));                 // 18 letters on a code
  traps.push(...Array.from({ length: 12 }, (_, i) => `ZODIAC_EARRINGS-${i}`)); // 12 signs
  traps.push(...ALPHA.map((L, i) => `ABC${L}`));                               // 3-char stem joined: not allowed
  const mixed = [...skus, ...real, ...traps].sort(() => rnd() - .5);
  eq(mixed.length >= 6000, true, 'the list holds 6,000+ SKUs');
  const t0 = Date.now(), fams = M.familiesOf(mixed), dt = Date.now() - t0;
  ok(dt < 500, `6,000 SKUs sorted in ${dt} ms`);
  eq(fams.length, 7, 'exactly the 7 real families come back: ' + fams.map(f => f.label).join(' | '));
  const get = base => fams.find(f => f.base === base);
  const F1 = get('MONOGRAM_LEFT_{L}'); ok(F1 && F1.count === 26 && F1.missing.length === 0 && F1.letters.A === 'MONOGRAM_LEFT_A' && F1.letters.Z === 'MONOGRAM_LEFT_Z' && F1.label === 'MONOGRAM LEFT', 'STEM_A shape');
  const F2 = get('MIDDLE-MONO-{L}'); ok(F2 && F2.count === 25 && eqArr(F2.missing, ['Q']) && F2.letters.A === 'MIDDLE-MONO-a' && F2.label === 'MIDDLE MONO' && !('Q' in F2.letters), 'STEM-a shape, 25 letters, the SKU text kept as written');
  const F3 = get('INITIAL_RIGHT_{N}'); ok(F3 && F3.count === 26 && F3.letters.A === 'INITIAL_RIGHT_0' && F3.letters.T === 'INITIAL_RIGHT_19' && F3.letters.Z === 'INITIAL_RIGHT_25' && F3.shape === 'index0', 'indexed family, 0 = A');
  const F4 = get('SCRIPT LETTER {L}'); ok(F4 && F4.count === 26 && F4.letters.M === 'Script Letter M' && F4.label === 'SCRIPT LETTER', 'STEM A shape with a space');
  const F5 = get('FANCYSET{N}'); ok(F5 && F5.count === 26 && F5.letters.A === 'FANCYSET1' && F5.letters.Z === 'FANCYSET26' && F5.shape === 'index1', 'indexed family, 1 = A');
  const F6 = get('SWASHFONT{L}'); ok(F6 && F6.count === 26 && F6.letters.S === 'SWASHFONTS', 'joined STEMA shape (stem of 4+ characters)');
  const F7 = fams.find(f => f.shape === 'code'); ok(F7 && F7.count === 26 && F7.letters.L === 'LOVELY_L_8832' && /LOVELY/.test(F7.label) && /8832/.test(F7.label), 'STEM_A_1234 shop-code shape');
  ok(fams.every((f, i) => i === 0 || fams[i - 1].count >= f.count), 'biggest families first');
  ok(!fams.some(f => /NINETEEN|NUMBERED|ODD|ZODIAC|ABC/.test(f.base)), 'a stem of 2 to 19 letters, a numbered series past 26, a 12-sign set and a 3-character joined stem are not families');
  for (const f of fams) ok(f.count === Object.keys(f.letters).length && f.count + f.missing.length === 26 && Object.entries(f.letters).every(([L, s]) => mixed.includes(s) && /^[A-Z]$/.test(L)), `${f.base}: letters map to SKUs that are in the list`);
  // the answer does not depend on the order of the list, and the plain mixed list alone holds none
  eq(M.familiesOf([...mixed].reverse()), fams, 'the same families whatever the order of the SKUs');
  eq(M.familiesOf([...skus]), [], 'the 6,000 ordinary SKUs alone hold no alphabet');
  eq(M.familiesOf([]), [], 'an empty list'); eq(M.familiesOf(null), [], 'no list'); eq(M.familiesOf([null, 5, {}, '', '   ', 'A']), [], 'junk entries are skipped');
  // a gap in the alphabet: 20 letters is still a family, 19 is not
  const twenty = ALPHA.slice(0, 20).map(L => `GAPPY_${L}`), nineteen = ALPHA.slice(0, 19).map(L => `GAPPY_${L}`);
  ok(M.familiesOf(twenty).length === 1 && eqArr(M.familiesOf(twenty)[0].missing, [...'UVWXYZ']), '20 distinct letters is a family and names the 6 missing');
  eq(M.familiesOf(nineteen), [], '19 letters is not');
  // two spellings of one letter and two families of one stem
  const twin = [...ALPHA.map(L => `TWIN_${L}`), 'twin_a', ...ALPHA.map(L => `TWIN-${L}`)];
  const tf = M.familiesOf(twin); ok(tf.length === 2 && tf.every(f => f.count === 26) && tf[0].label !== tf[1].label, 'the same stem with two separators is two families with distinct labels: ' + tf.map(f => f.label));
  ok(tf.find(f => f.base === 'TWIN_{L}').letters.A === 'TWIN_A', 'of two spellings of one letter the smaller text is kept, always');
}
function eqArr(a, b) { return a.length === b.length && a.every((v, i) => v === b[i]); }

// ── 3 · build ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
const ABC = [A, Pp, V];
{ // normal three letters
  const r = M.build({ left: A, middle: Pp, right: V }); sound(r, 'APV');
  eq(holesOf(r).length, 2, 'APV: the counters of A and P are holes of the piece');
  ok(r.report.holesKept >= 0.9 && r.report.holesKept <= 1, 'APV: holesKept ' + r.report.holesKept);
  const hM = heightOf(by(r, 'middle')), hL = heightOf(by(r, 'left')), hR = heightOf(by(r, 'right'));
  ok(hM >= 1.2 * hL && hM >= 1.2 * hR, `APV: the middle (${hM}) is at least 1.2 times the sides (${hL}, ${hR})`);
  ok(Math.abs(hM - 1.5 * 30) < 0.01 && by(r, 'middle').scale === 1.5 && r.report.centerScale === 1.5, 'APV: the middle was drawn the size of the sides, so it is raised to 1.5 times');
  ok(by(r, 'left').scale === 1 && by(r, 'right').scale === 1, 'APV: the sides keep their size');
  const pl = by(r, 'left'), pm = by(r, 'middle'), pr = by(r, 'right');
  ok(pl.bboxPt[0] < pm.bboxPt[0] && pm.bboxPt[0] < pr.bboxPt[0] && pl.bboxPt[2] < pm.bboxPt[2] && pm.bboxPt[2] < pr.bboxPt[2], 'APV: left, middle, right in that order');
  ok(pl.bboxPt[2] > pm.bboxPt[0] && pr.bboxPt[0] < pm.bboxPt[2], 'APV: each side overlaps the middle');
  ok(Math.abs((pl.bboxPt[2] - pm.bboxPt[0]) - 0.18 * 20) < 0.01 && Math.abs((pm.bboxPt[2] - pr.bboxPt[0]) - 0.18 * 20) < 0.01, 'APV: the overlap is 18 % of the side letter width');
  ok(Math.abs((pl.bboxPt[1] + pl.bboxPt[3]) / 2 - (pm.bboxPt[1] + pm.bboxPt[3]) / 2) < 0.01 && Math.abs((pr.bboxPt[1] + pr.bboxPt[3]) / 2 - (pm.bboxPt[1] + pm.bboxPt[3]) / 2) < 0.01, 'APV: the sides are centred on the middle\'s vertical centre');
  ok(r.report.tries === 1 && r.report.overlap === 0.18 && r.report.overlapPct.leftMiddle > 0 && r.report.overlapPct.middleRight > 0 && r.report.overlapPct.leftMiddle <= 35, 'APV: joined at the first try');
  const vis = visibleShare(r, ABC); ok(vis.every(v => v >= 0.7), 'APV: every letter keeps at least 70 % of its area in view: ' + vis.map(v => v.toFixed(2)));
  const area = shoelace(r.rings[0].pts) + holesOf(r).reduce((a, h) => a + shoelace(h.pts), 0);
  ok(area > 0 && area < ['left', 'middle', 'right'].reduce((a, s, i) => a + areaOf(regionOf(ABC[i], by(r, s))) / 1e6, 0), 'APV: the piece is the union (smaller than the sum of the three letters)');
  // baseline alignment: the sides sit on the middle's bottom
  const rb = M.build({ left: A, middle: Pp, right: V }, { valign: 'baseline' }); sound(rb, 'APV baseline');
  ok(by(rb, 'left').bboxPt[1] === by(rb, 'middle').bboxPt[1] && by(rb, 'right').bboxPt[1] === by(rb, 'middle').bboxPt[1], 'baseline: the sides start at the middle\'s bottom');
  // other letters
  sound(M.build({ left: O, middle: Mm, right: O }), 'OMO'); sound(M.build({ left: scaleL(Mm, 0.8), middle: O, right: scaleL(A, 0.9) }), 'MOA');
  const omo = M.build({ left: O, middle: Mm, right: O }); eq(holesOf(omo).length, 2, 'OMO: the two O counters are holes');
  // options
  const r2 = M.build({ left: A, middle: Pp, right: V }, { centerRatio: 2 }); ok(r2.ok && Math.abs(heightOf(by(r2, 'middle')) - 60) < 0.01, 'centerRatio 2');
  const r3 = M.build({ left: A, middle: Pp, right: V }, { centerRatio: 1.0 }); ok(r3.ok && heightOf(by(r3, 'middle')) >= 1.2 * 30 - 0.01, 'a centerRatio under 1.2 never lets the middle drop below 1.2 times the sides');
  const r4 = M.build({ left: A, middle: Pp, right: V }, { overlap: 0.3 }); ok(r4.ok && Math.abs((by(r4, 'left').bboxPt[2] - by(r4, 'middle').bboxPt[0]) - 6) < 0.01, 'overlap 0.3');
}
{ // alphabets already drawn bigger in the middle: every glyph stays as drawn
  const mid = scaleL(Pp, 1.4);
  const r = M.build({ left: A, middle: mid, right: V }); sound(r, 'big middle');
  ok(r.report.centerScale === 1 && by(r, 'middle').scale === 1 && by(r, 'left').scale === 1 && by(r, 'right').scale === 1, 'a middle drawn 1.4 times the sides is kept as drawn');
  ok(Math.abs(heightOf(by(r, 'middle')) - 42) < 0.01, 'its height is the drawn 42');
  const edge = M.build({ left: A, middle: scaleL(Pp, 1.25), right: V }); ok(edge.ok && edge.report.centerScale === 1, 'exactly 1.25 times is already big enough');
  const under = M.build({ left: A, middle: scaleL(Pp, 1.24), right: V }); ok(under.ok && under.report.centerScale > 1 && Math.abs(heightOf(by(under, 'middle')) - 45) < 0.01, '1.24 times is raised to 1.5 times the sides');
  const huge = M.build({ left: A, middle: scaleL(Pp, 3), right: V }); ok(huge.ok && huge.report.centerScale === 1 && heightOf(by(huge, 'middle')) === 90, 'a middle drawn 3 times is never shrunk');
}
{ // sides: the smaller one is raised to the larger when they differ by more than 10 %
  const r = M.build({ left: A, middle: Pp, right: scaleL(V, 0.8) }); sound(r, 'unequal sides');
  ok(Math.abs(by(r, 'right').scale - 1.25) < 1e-3 && by(r, 'left').scale === 1, 'the smaller right side was raised 1.25 times');
  ok(Math.abs(heightOf(by(r, 'left')) - heightOf(by(r, 'right'))) < 0.01 && Math.abs(heightOf(by(r, 'middle')) - 45) < 0.01, 'both sides are now the same height, the middle 1.5 times that');
  const l = M.build({ left: scaleL(A, 0.8), middle: Pp, right: V }); ok(Math.abs(by(l, 'left').scale - 1.25) < 1e-3 && by(l, 'right').scale === 1, 'and the other way round');
  const close = M.build({ left: A, middle: Pp, right: scaleL(V, 0.95) }); ok(close.ok && close.placed.every(p => p.slot === 'middle' || p.scale === 1) && Math.abs(heightOf(by(close, 'right')) - 28.5) < 0.01, 'sides within 10 % keep their own relative size');
  const ten = M.build({ left: A, middle: Pp, right: scaleL(V, 1 / 1.1) }); ok(ten.ok && by(ten, 'right').scale === 1, 'exactly 10 % apart is still kept');
  ok(heightOf(by(close, 'middle')) >= 1.2 * Math.max(heightOf(by(close, 'left')), heightOf(by(close, 'right'))), 'the middle is the biggest');
}
{ // letters too far apart at the default overlap: the overlap is raised until they join
  const diamond = { letter: 'D', rings: [[[0, 22.5], [15, 45], [30, 22.5], [15, 0]], [[9, 22.5], [15, 31], [21, 22.5], [15, 14]]] };
  const r = M.build({ left: V, middle: diamond, right: V }); sound(r, 'V D V');
  ok(r.report.tries > 1 && r.report.overlap > 0.18 && r.report.overlap <= 0.38, `the overlap was raised from 0.18 to ${r.report.overlap} (tries ${r.report.tries})`);
  ok(r.report.notes.some(t => /raised/.test(t)), 'and the note says so');
  const noRaise = M.build({ left: V, middle: diamond, right: V }, { maxOverlap: 0.18, reach: false }); ok(noRaise.ok === false && noRaise.why === 'the letters do not touch' && noRaise.report.tries === 1, 'with the ladder switched off the same letters do not touch');
}
{ // letters that can never touch
  const diamond = { letter: 'D', rings: [[[0, 22.5], [15, 45], [30, 22.5], [15, 0]], [[9, 22.5], [15, 31], [21, 22.5], [15, 14]]] };
  const flag = { letter: 'F', rings: [[[0, 0], [3, 0], [3, 28], [20, 28], [20, 30], [0, 30]]] };
  const r = M.build({ left: flag, middle: diamond, right: V }, { reach: false });
  ok(r.ok === false && r.why === 'the letters do not touch' && r.rings.length === 0 && r.bboxPt === null && r.report.connected === false, 'a flag whose only edge ink sits where the middle is empty never touches: ' + r.why);
  ok(r.report.tries === 6, 'it tried 0.18 to 0.38 in steps of 0.04: ' + r.report.tries);
  ok(!/\n/.test(r.why) && r.why.length < 90, 'one plain line');
  ok(M.svgPath(r) === '', 'no path for a refused piece');
  // a letter with a loose part (a stem with a dot beside it, far from the neighbour) is said so, not "do not touch"
  const dotted = { letter: 'I', rings: [rect(0, 0, 6, 30), rect(12, 10, 16, 14)] };
  const r2 = M.build({ left: A, middle: Pp, right: dotted }, { reach: false });
  ok(r2.ok === false && /separate parts/.test(r2.why) && r2.rings.length === 0, 'a letter made of two parts that stay loose: ' + r2.why);
  const r3 = M.build({ left: { letter: 'I', rings: [rect(14, 0, 20, 30), rect(0, 10, 4, 14)] }, middle: Pp, right: V }, { reach: false });
  ok(r3.ok === false && /separate parts/.test(r3.why), 'the same on the left: ' + r3.why);
  // a part that does reach a neighbour is fine
  const flagged = { letter: 'I', rings: [rect(0, 0, 6, 30), rect(0, 31, 16, 35)] };
  const r4 = M.build({ left: A, middle: Pp, right: flagged }); ok(r4.ok === true || /separate|touch/.test(r4.why), 'a second part answered either way: ' + (r4.ok ? 'built' : r4.why));
}
{ // a counter that a neighbour would cover: the overlap goes down until the counter stays open
  const r = M.build({ left: block('B', 30, 30), middle: Pp, right: V }, { overlap: 0.36, maxOverlap: 0.38 }); sound(r, 'cover');
  ok(r.report.holesKept >= 0.9 && r.report.overlap < 0.36 && r.report.tries > 1, `holesKept ${r.report.holesKept} at overlap ${r.report.overlap} (tries ${r.report.tries})`);
  ok(r.report.notes.some(t => /lowered/.test(t)), 'the note says the overlap was lowered');
  const high = M.build({ left: block('B', 30, 30), middle: Pp, right: V }, { overlap: 0.36, maxOverlap: 0.38 });
  eq(JSON.stringify(high), JSON.stringify(r), 'the same again');
  ok(holesOf(r).length === 1, 'the counter of the P is still a hole');
  const vis = visibleShare(r, [block('B', 30, 30), Pp, V]); ok(vis[1] >= 0.5 && vis[0] >= 0.7, 'a solid 30 pt block over a P: the P keeps over half in view (' + vis.map(v => v.toFixed(2)) + ')');
  // a neighbour overlapping more than 35 % of the smaller letter's area goes down too
  const heavy = M.build({ left: block('B', 30, 30), middle: block('M', 12, 45), right: block('B', 30, 30) }, { overlap: 0.38 });
  ok(heavy.ok && heavy.report.overlapPct.leftMiddle <= 35 && heavy.report.overlap < 0.38, 'a block side that would overlap more than 35 % of the smaller letter is brought under it: ' + JSON.stringify(heavy.report.overlapPct) + ' at ' + heavy.report.overlap);
  // unavoidable: the neighbour only touches the middle beyond its thin wall, so the counter is covered from the first join on: ok, still built, and a note
  const spike = { letter: 'S', rings: [[[0, 45], [28, 45], [28, 0], [9.5, 0], [9.5, 43], [0, 43]], [[10, 10], [12, 10], [12, 35], [10, 35]]] };
  const u = M.build({ left: block('B', 40, 30), middle: spike, right: block('B', 20, 30) }); sound(u, 'unavoidable');
  ok(u.report.holesKept < 0.9 && u.report.notes.some(t => /inner openings/.test(t)), `a counter lost with no way round it is noted, not hidden: holesKept ${u.report.holesKept}; ${u.report.notes.join(' | ')}`);
  ok(u.report.tries >= 2, 'it still tried the overlaps (raised until the letters joined)');
}
{ // slivers: a hole from two edges touching and a speck are removed, a real hole stays
  const notch = { letter: 'N', rings: [[[0, 0], [20, 0], [20, 12], [14, 12], [14, 12.1], [20, 12.1], [20, 30], [0, 30]]] };   // a 6 x 0.1 pt slot cut in from its right edge: the middle letter closes its mouth and leaves 2.4 x 0.1 pt = 0.03 mm2 behind
  const r = M.build({ left: notch, middle: block('M', 30, 45), right: block('R', 20, 30) }); sound(r, 'slot');
  eq(holesOf(r).length, 0, 'a 0.03 mm2 hole where two edges close up is dropped');
  const bigNotch = { letter: 'N', rings: [[[0, 0], [20, 0], [20, 12], [14, 12], [14, 13], [20, 13], [20, 30], [0, 30]]] };   // 6 x 1 pt slot: 2.4 x 1 pt = 0.30 mm2 left behind
  const r2 = M.build({ left: bigNotch, middle: block('M', 30, 45), right: block('R', 20, 30) }); sound(r2, 'bigger slot');
  eq(holesOf(r2).length, 1, 'a 0.30 mm2 hole is real and stays');
  ok(Math.abs(Math.abs(shoelace(holesOf(r2)[0].pts)) - 2.4) < 0.01, 'and it is the 2.4 pt2 left behind: ' + Math.abs(shoelace(holesOf(r2)[0].pts)));
  const specked = { letter: 'S', rings: [...block('S', 20, 30).rings, rect(25, 5, 25.4, 5.4)] };                                  // 0.16 pt2 speck beside the letter
  const r3 = M.build({ left: specked, middle: block('M', 30, 45), right: block('R', 20, 30) }); sound(r3, 'speck');
  const tinyCounter = { letter: 'T', rings: [...block('T', 20, 30).rings, rect(8, 8, 8.5, 8.5)] };                                 // a 0.25 pt2 counter
  const r4 = M.build({ left: tinyCounter, middle: block('M', 30, 45), right: block('R', 20, 30) }); sound(r4, 'tiny counter');
  eq(holesOf(r4).length, 0, 'a counter under 0.05 mm2 is dropped');
  const okCounter = { letter: 'T', rings: [...block('T', 20, 30).rings, rect(8, 8, 9, 9)] };                                      // 1 pt2 = 0.12 mm2
  eq(holesOf(M.build({ left: okCounter, middle: block('M', 30, 45), right: block('R', 20, 30) })).length, 1, 'a 0.12 mm2 counter stays');
}
{ // even-odd: a letter drawn as outline + counter rings; duplicate and closing points and ring direction do not matter
  const plain = M.build({ left: A, middle: Pp, right: V });
  const dup = { letter: 'A', rings: A.rings.map(r => [r[0], r[0], ...r, r[0], r[r.length - 1]]) };
  eq(M.build({ left: dup, middle: Pp, right: V }).rings, plain.rings, 'duplicate points and a repeated closing point change nothing');
  const rev = { letter: 'A', rings: A.rings.map(r => r.slice().reverse()) }; eq(M.build({ left: rev, middle: Pp, right: V }).rings, plain.rings, 'reversed rings change nothing');
  const mixedDir = { letter: 'A', rings: [A.rings[0], A.rings[1].slice().reverse()] }; eq(M.build({ left: mixedDir, middle: Pp, right: V }).rings, plain.rings, 'a counter wound the same way or the opposite way is still a counter (even-odd)');
  const objPts = { letter: 'A', rings: A.rings.map(r => r.map(([x, y]) => ({ x, y }))) }; eq(M.build({ left: objPts, middle: Pp, right: V }).rings, plain.rings, '{x,y} points are read too');
  const moved = M.build({ left: moveL(A, 500, -300), middle: moveL(Pp, -77.5, 1000), right: moveL(V, 3000, 3000) }); eq(moved.rings, plain.rings, 'letters in their own file coordinates (not pre-aligned) give the same piece');
}
{ // degenerate input never throws
  const bad = {
    'no arguments': [], 'null': [null], 'a string': ['APV'], 'empty object': [{}], 'missing right': [{ left: A, middle: Pp }],
    'empty rings': [{ left: { letter: 'A', rings: [] }, middle: Pp, right: V }],
    'no rings key': [{ left: { letter: 'A' }, middle: Pp, right: V }],
    'rings not an array': [{ left: { letter: 'A', rings: 'x' }, middle: Pp, right: V }],
    'NaN': [{ left: { letter: 'A', rings: [[[NaN, 0], [1, 1], [2, 0]]] }, middle: Pp, right: V }],
    'Infinity': [{ left: A, middle: { letter: 'P', rings: [[[0, 0], [Infinity, 0], [1, 1]]] }, right: V }],
    'string coordinates': [{ left: A, middle: Pp, right: { letter: 'V', rings: [[['0', '0'], ['5', '0'], ['5', '5']]] } }],
    'huge numbers': [{ left: A, middle: { letter: 'P', rings: [[[0, 0], [1e300, 0], [0, 1e300]]] }, right: V }],
    'one point': [{ left: { letter: 'A', rings: [[[1, 1]]] }, middle: Pp, right: V }],
    'two points': [{ left: { letter: 'A', rings: [[[1, 1], [4, 4]]] }, middle: Pp, right: V }],
    'duplicate points only': [{ left: { letter: 'A', rings: [[[1, 1], [1, 1], [1, 1], [1, 1]]] }, middle: Pp, right: V }],
    'zero-area ring (a line)': [{ left: { letter: 'A', rings: [[[0, 0], [5, 5], [10, 10], [20, 20]]] }, middle: Pp, right: V }],
    'zero-area ring (back and forth)': [{ left: A, middle: { letter: 'P', rings: [[[0, 0], [10, 0], [0, 0], [10, 0]]] }, right: V }],
    'a speck of 0.001 pt': [{ left: A, middle: Pp, right: { letter: 'V', rings: [rect(0, 0, 0.001, 0.001)] } }],
    'a ring of null points': [{ left: { letter: 'A', rings: [[null, null, null]] }, middle: Pp, right: V }],
    'self-touching ring (bow tie)': [{ left: { letter: 'A', rings: [[[0, 0], [20, 30], [20, 0], [0, 30]]] }, middle: Pp, right: V }],
    'self-touching ring (pinched at a vertex)': [{ left: { letter: 'A', rings: [[[0, 0], [10, 0], [10, 10], [20, 10], [20, 20], [10, 20], [10, 10], [0, 10]]] }, middle: Pp, right: V }],
    'self-overlapping ring': [{ left: { letter: 'A', rings: [[[0, 0], [20, 0], [20, 20], [0, 20], [0, 0], [10, 10], [30, 10], [30, 30]]] }, middle: Pp, right: V }],
    'identical rings twice (even-odd cancels)': [{ left: { letter: 'A', rings: [rect(0, 0, 10, 10), rect(0, 0, 10, 10)] }, middle: Pp, right: V }],
    'hostile options': [{ left: A, middle: Pp, right: V }, { overlap: NaN, maxOverlap: -5, centerRatio: 'x', valign: 7 }],
    'options as a string': [{ left: A, middle: Pp, right: V }, 'fast'],
    'a getter that throws': [{ get left() { throw new Error('boom'); }, middle: Pp, right: V }],
    'a ring that is a proxy that throws': [{ left: { letter: 'A', rings: [new Proxy([], { get() { throw new Error('boom'); } })] }, middle: Pp, right: V }]
  };
  for (const [name, args] of Object.entries(bad)) {
    let r; try { r = M.build(...args); } catch (e) { assert.fail(`${name}: build threw ${e && e.message}`); }
    ok(r && typeof r.ok === 'boolean' && typeof r.why === 'string' && Array.isArray(r.rings) && r.report && typeof r.report === 'object', `${name}: an answer of the right shape`);
    if (!r.ok) ok(r.why.length > 3 && r.why.length < 140 && !/\n/.test(r.why) && !/undefined|NaN|\[object/.test(r.why), `${name}: a plain one-line reason: ${r.why}`);
    else sound(r, name);
  }
  ok(!M.build({ left: { letter: 'A', rings: [] }, middle: Pp, right: V }).ok && /left letter \(A\) has no outline/.test(M.build({ left: { letter: 'A', rings: [] }, middle: Pp, right: V }).why), 'the reason names the letter');
  ok(/middle letter/.test(M.build({ left: A, middle: { letter: 'P', rings: [[[0, 0], [10, 0], [20, 0]]] }, right: V }).why), 'and the slot');
  ok(/bad coordinate/.test(M.build({ left: { letter: 'A', rings: [[[NaN, 0], [1, 1], [2, 0]]] }, middle: Pp, right: V }).why), 'a bad coordinate is said so');
  ok(M.build({ left: { letter: 'A', rings: [[[0, 0], [20, 30], [20, 0], [0, 30]]] }, middle: Pp, right: V }, undefined).ok !== undefined, 'a bow tie is answered either way');
}
{ // deterministic, and fast with many points per ring
  const a = JSON.stringify(M.build({ left: A, middle: Pp, right: V })), b = JSON.stringify(M.build({ left: A, middle: Pp, right: V })); eq(a, b, 'same input twice: identical output');
  const fresh = JSON.stringify(M.build(JSON.parse(JSON.stringify({ left: A, middle: Pp, right: V })))); eq(a, fresh, 'and from a deep copy');
  const script = (letter, rx, ry, seed0) => ({ letter, rings: [ell(0, 0, rx, ry, 500, 0.12, 5 + seed0), ell(0, 0, rx * 0.45, ry * 0.5, 500, 0.08, 3 + seed0), ell(rx * 0.1, ry * 0.8, rx * 0.15, ry * 0.1, 500, 0.05, 7)] });
  const L = { left: script('J', 11, 15, 1), middle: script('R', 14, 22, 2), right: script('S', 11, 15, 3) };
  const times = []; let r; for (let i = 0; i < 6; i++) { const t = process.hrtime.bigint(); r = M.build(L); times.push(Number(process.hrtime.bigint() - t) / 1e6); }
  times.shift(); times.sort((x, y) => x - y);
  ok(r.ok, 'script shapes with 500 points per ring build: ' + (r.ok ? '' : r.why));
  ok(times[Math.floor(times.length / 2)] < 100, `3 letters x 3 rings x 500 points: median ${times[Math.floor(times.length / 2)].toFixed(1)} ms (best ${times[0].toFixed(1)}, worst ${times[times.length - 1].toFixed(1)})`);
  if (r.ok) sound(r, 'script shapes');
  const cold = process.hrtime.bigint(); M.build({ left: script('A', 12, 16, 4), middle: script('B', 15, 23, 5), right: script('C', 12, 16, 6) }); const coldMs = Number(process.hrtime.bigint() - cold) / 1e6; ok(coldMs < 250, `another set, still fast: ${coldMs.toFixed(1)} ms`);
}

// ── 3b · slotsOf: which alphabet is the Left, the Middle and the Right one (MONOGRAMUI, Paul's file: Monogram_Left_A … Monogram_Mid_A … Monogram_Right_Z) ──────────────────────────────────────────────────────────────
{
  const names = (fams, saved) => { const r = M.slotsOf(fams, saved); return [r.left, r.middle, r.right].map(f => (f ? f.name : '-')).join(' / '); };
  const sk = (stem, sep = '_') => ALPHA.map(L => `${stem}${sep}${L}`);
  const PAUL = ['Left', 'Mid', 'Right'].flatMap(k => sk('Monogram_' + k)).map(x => x.toUpperCase());     // the library upper-cases a SKU
  eq(PAUL.length, 78, '78 SKUs as Paul\'s file names them');
  const fams = M.familiesOf(PAUL);
  eq(fams.map(f => f.base).sort(), ['MONOGRAM_LEFT_{L}', 'MONOGRAM_MID_{L}', 'MONOGRAM_RIGHT_{L}'], 'familiesOf finds exactly the three families from the upper-cased list');
  eq(fams.map(f => f.name).sort(), ['Monogram Left', 'Monogram Mid', 'Monogram Right'], 'and names them Monogram Left / Mid / Right');
  eq(fams.map(f => f.label).sort(), ['MONOGRAM LEFT', 'MONOGRAM MID', 'MONOGRAM RIGHT'], 'the upper-case label stays as it was');
  ok(fams.every(f => f.count === 26 && f.missing.length === 0 && f.letters.A === f.base.replace('{L}', 'A') && f.letters.Z === f.base.replace('{L}', 'Z')), 'each holds all 26 letters, A to Z, under their own SKU');
  const s = M.slotsOf(fams);
  ok(s.ok === true && s.left.base === 'MONOGRAM_LEFT_{L}' && s.middle.base === 'MONOGRAM_MID_{L}' && s.right.base === 'MONOGRAM_RIGHT_{L}' && s.why === '', 'slotsOf: Left, Middle (MID) and Right found by their names');
  eq(s.by, { left: 'name', middle: 'name', right: 'name' }, 'by their names, nothing chosen');
  eq(s.sets.map(x => x.name), ['Monogram'], 'one set, called Monogram');
  eq(M.slotsOf([...fams].reverse()).left.base, 'MONOGRAM_LEFT_{L}', 'whatever the order of the families');
  // the other spellings of the middle
  for (const mid of ['MIDDLE', 'CENTER', 'CENTRE', 'CENTRAL', 'MID']) { const r = M.slotsOf(M.familiesOf([...sk('SCRIPT_LEFT'), ...sk('SCRIPT_' + mid), ...sk('SCRIPT_RIGHT')])); ok(r.ok && r.middle.base === `SCRIPT_${mid}_{L}`, `${mid} is the middle`); }
  eq(names(M.familiesOf([...sk('monogram left', ' '), ...sk('Monogram-Mid', '-'), ...sk('MONOGRAM RIGHT')])), 'Monogram Left / Monogram Mid / Monogram Right', 'spaces, dashes and mixed case');
  // never guesses: a bare letter is no place, two place words are no place, a word two families share leaves the slot empty
  eq(M.slotsOf(M.familiesOf([...sk('LETTER_L'), ...sk('LETTER_M'), ...sk('LETTER_R')])).ok, false, 'LETTER_L, LETTER_M, LETTER_R say no place');
  eq(M.slotsOf(M.familiesOf([...sk('LEFT_RIGHT'), ...sk('FANCY_MID'), ...sk('FANCY_RIGHT')])).left, null, 'a name with two places ("LEFT RIGHT") is no Left');
  const two = M.slotsOf(M.familiesOf([...sk('MONO_LEFT'), ...sk('MONO_MID')]));
  ok(two.ok === false && two.right === null && /no Right alphabet found/.test(two.why) && /Monogram_Left_A/.test(two.why), 'two of three: the third is named as missing: ' + two.why);
  eq(M.slotsOf([]).why, 'no 26-letter alphabet in the library', 'none at all'); eq(M.slotsOf(null).ok, false, 'null'); eq(M.slotsOf([null, 5, {}, 'x']).ok, false, 'junk');
  // two complete sets: the biggest first, then the one called Monogram, then by name; all of them offered
  const both = M.familiesOf([...sk('CIRCLE_LEFT'), ...sk('CIRCLE_MID'), ...sk('CIRCLE_RIGHT'), ...PAUL]), sb = M.slotsOf(both);
  eq(sb.sets.map(x => x.name), ['Monogram', 'Circle'], 'two complete sets are both offered, Monogram first'); eq(sb.left.base, 'MONOGRAM_LEFT_{L}', 'and the first is used');
  const thin = M.slotsOf(M.familiesOf([...sk('CIRCLE_LEFT').slice(0, 21), ...sk('CIRCLE_MID'), ...sk('CIRCLE_RIGHT'), ...PAUL]));
  eq(thin.left.base, 'MONOGRAM_LEFT_{L}', 'of two sets the one whose smallest alphabet is bigger comes first');
  // a person's choice stands for the slot it names, the rest stay by name
  const saved = M.slotsOf(both, { middle: 'CIRCLE_MID_{L}' }); eq([saved.left.base, saved.middle.base, saved.right.base, saved.by.middle, saved.by.left], ['MONOGRAM_LEFT_{L}', 'CIRCLE_MID_{L}', 'MONOGRAM_RIGHT_{L}', 'saved', 'name'], 'a saved middle is used, the other two by name');
  eq(M.slotsOf(fams, { left: 'NOT_THERE_{L}' }).left.base, 'MONOGRAM_LEFT_{L}', 'a saved choice that is no longer in the library is ignored');
  const hand = M.slotsOf(M.familiesOf([...sk('FANCY_ONE'), ...sk('FANCY_TWO'), ...sk('FANCY_THREE')]), { left: 'FANCY_ONE_{L}', middle: 'FANCY_TWO_{L}', right: 'FANCY_THREE_{L}' });
  ok(hand.ok && hand.by.left === 'saved' && hand.by.right === 'saved', 'three alphabets with no place in their names work once a person chose them');
  eq(M.slotsOf(M.familiesOf([...sk('FANCY_ONE'), ...sk('FANCY_TWO'), ...sk('FANCY_THREE')])).ok, false, 'and not before');
}

// ── 3c · joins that hold (MONOGRAMUI): "touching" is not enough, a join is as wide as the widest disc inside the overlap of two letters; thin script strokes need a wider reach ───────────────────
{
  const diamond = { letter: 'D', rings: [[[0, 22.5], [15, 45], [30, 22.5], [15, 0]], [[9, 22.5], [15, 31], [21, 22.5], [15, 14]]] };
  const legacy = M.build({ left: V, middle: diamond, right: V }, { reach: false });
  ok(legacy.ok && legacy.report.joinMm.leftMiddle < 0.3 && legacy.report.reach === false && legacy.report.warnings.length === 2 && /thin/.test(legacy.report.notes.join(' ')), 'without the reach the V D V ladder joins at the first touch: a hairline, and it says so: ' + JSON.stringify(legacy.report.joinMm));
  const r = M.build({ left: V, middle: diamond, right: V }); sound(r, 'V D V reach');
  ok(r.report.reach === true && r.report.joinMm.leftMiddle >= 0.3 && r.report.joinMm.middleRight >= 0.3 && r.report.warnings.length === 0 && r.report.overlap > legacy.report.overlap, `with it the overlap goes up until the join holds: ${r.report.overlap} (was ${legacy.report.overlap}), joins ${JSON.stringify(r.report.joinMm)}`);
  ok(r.report.notes.some(t => /raised .* so the letters join firmly/.test(t)) && r.report.searched > 0, 'and the note says so: ' + r.report.notes.join(' | '));
  eq(JSON.stringify(M.build({ left: V, middle: diamond, right: V })), JSON.stringify(r), 'the same again');
  // a join that already holds is left exactly as it was (the first touch of block letters is wide)
  const apv = M.build({ left: A, middle: Pp, right: V }), apvOld = M.build({ left: A, middle: Pp, right: V }, { reach: false });
  eq(JSON.stringify(apv.rings), JSON.stringify(apvOld.rings), 'a join that holds is not moved: A P V is the same piece with and without the reach'); ok(apv.report.reach === false && apv.report.searched === 0 && apv.report.joinMm.leftMiddle >= 0.3, 'it never searched: ' + JSON.stringify(apv.report.joinMm));
  // a flag whose edge ink sits where the middle is empty used to be refused; with the reach its bar goes into the middle letter and the piece is one
  const flag = { letter: 'F', rings: [[[0, 0], [3, 0], [3, 28], [20, 28], [20, 30], [0, 30]]] };
  const f = M.build({ left: flag, middle: diamond, right: V }); sound(f, 'flag'); ok(f.report.reach === true && f.report.joinMm.leftMiddle > 0, 'the flag joins by moving into the middle letter: ' + f.report.notes.join(' | '));
  // a letter made of two parts: each part must be held by the middle letter
  const dotted = { letter: 'I', rings: [rect(0, 0, 6, 30), rect(14, 10, 20, 14)] };            // a stem and, beyond the stem, a mark (on the left letter: the mark is at its far left when mirrored)
  const left2 = { letter: 'J', rings: [rect(14, 0, 20, 30), rect(0, 10, 6, 14)] };               // stem at the right edge, a mark 8 pt from it: held only when the letter goes well into the middle one
  const two = M.build({ left: left2, middle: Pp, right: V }); sound(two, 'two parts');
  ok(two.report.reach === true && two.report.joinMm.leftMiddle >= 0.3, 'both parts of a letter are held: the left letter goes far enough in for its second part to cross the middle one: ' + two.report.notes.join(' | '));
  const lost = M.build({ left: A, middle: Pp, right: { letter: 'I', rings: [rect(0, 0, 6, 30), rect(40, 10, 46, 14)] } });
  ok(lost.ok === false && /separate parts/.test(lost.why), 'a part that no overlap can reach is said so, never a silent loose piece: ' + lost.why); void dotted;
  // a thin letter (I) next to wide ones, and a wide one next to thin
  const thinI = { letter: 'I', rings: [rect(0, 0, 4, 30)] }, wide = { letter: 'W', rings: [[[0, 0], [8, 0], [20, 14], [32, 0], [40, 0], [26, 30], [22, 30], [20, 24], [18, 30], [14, 30]]] };
  for (const [name, lt] of [['I M I', { left: thinI, middle: Mm, right: thinI }], ['W I W', { left: wide, middle: thinI, right: wide }], ['I W I', { left: thinI, middle: wide, right: thinI }], ['W M W', { left: wide, middle: Mm, right: wide }]]) {
    const b = M.build(lt); sound(b, name); ok(b.report.joinMm.leftMiddle >= 0.3 && b.report.joinMm.middleRight >= 0.3, `${name}: both joins hold (${JSON.stringify(b.report.joinMm)}), x${b.report.centerScale}`);
  }
  // thin script-like strokes (rings 1.2 pt wide = 0.4 mm): connected, joins counted, nothing thrown
  const ring = (rx, ry, w) => ({ letter: 'O', rings: [ell(0, 0, rx, ry, 120), ell(0, 0, rx - w, ry - w, 120)] });
  const sc = M.build({ left: ring(8, 14, 1.2), middle: ring(11, 20, 1.2), right: ring(8, 14, 1.2) }); sound(sc, 'thin rings');
  ok(sc.report.joinMm.leftMiddle > 0 && sc.report.joinMm.middleRight > 0 && sc.report.holesKept >= 0.9, 'three thin rings: one piece, joins ' + JSON.stringify(sc.report.joinMm) + ', holesKept ' + sc.report.holesKept);
  // the centre size rule on proportions like a real alphabet: a middle only 11 % taller than the sides is raised to 1.5 times; one already 1.25 times is kept
  const tall = (k) => ({ letter: 'T', rings: [[[0, 0], [8, 0], [8, 30 * k], [0, 30 * k]]] });
  const k11 = M.build({ left: block('L', 14, 30), middle: { letter: 'M', rings: [rect(0, 0, 16, 33.3)] }, right: block('R', 14, 30) }); ok(k11.ok && Math.abs(k11.report.centerScale - 1.5 * 30 / 33.3) < 0.01 && Math.abs(heightOf(by(k11, 'middle')) - 45) < 0.05, 'a middle 1.11 times the sides: raised to 1.5 times (45 pt): x' + k11.report.centerScale);
  const k125 = M.build({ left: block('L', 14, 30), middle: { letter: 'M', rings: [rect(0, 0, 16, 37.5)] }, right: block('R', 14, 30) }); ok(k125.ok && k125.report.centerScale === 1, 'a middle 1.25 times the sides is kept as drawn'); void tall;
  // the reach is bounded: a hard pair does not take long
  const t0 = Date.now(); M.build({ left: flag, middle: diamond, right: lost.ok ? V : flag }); ok(Date.now() - t0 < 4000, 'a search over the whole reach takes ' + (Date.now() - t0) + ' ms');
}

// ── 3c' · how a letter's rings make its shape: counters by default, direction for a font's overlapping contours (the real Mid Q of Paul's file: three rings that cross each other) ──────────────────────
{
  const rot = (pts, deg, cx, cy) => pts.map(([x, y]) => { const a = deg * Math.PI / 180, dx = x - cx, dy = y - cy; return [cx + dx * Math.cos(a) - dy * Math.sin(a), cy + dx * Math.sin(a) + dy * Math.cos(a)]; });
  const cw = pts => pts.slice().reverse();
  const loop = ell(20, 28, 15, 21, 90), counter = cw(ell(20, 28, 7, 12, 60)), tail = rot(ell(30, 12, 11, 3, 60), 40, 30, 12);
  const q = (extra = {}) => Object.assign({ letter: 'Q', rings: [loop, tail, counter] }, extra);
  const piece = (l, ex) => M.build({ left: A, middle: l, right: V }, ex);
  const dflt = piece(q()), nz = piece(q({ fill: 'nonzero' })), auto = piece(q({ fill: 'auto' }));
  sound(dflt, 'Q default'); sound(nz, 'Q nonzero'); sound(auto, 'Q auto');
  eq(holesOf(auto).length, holesOf(dflt).length - 1, "auto: one hole fewer than the default: the lens where the tail crosses the loop is ink, not a hole"); eq(holesOf(nz).length, holesOf(auto).length, 'nonzero: the same');
  const ink = r => r.rings.reduce((a, x) => a + shoelace(x.pts), 0);
  ok(ink(auto) > ink(dflt) + 20, 'the default reads the part where the tail crosses the loop as a hole (it bites out the letter and cuts the tail loose): ' + ink(dflt).toFixed(0) + ' pt2 of ink against ' + ink(auto).toFixed(0));
  eq(JSON.stringify(piece(q({ fill: 'auto' })).rings), JSON.stringify(auto.rings), 'the same again');
  // a letter drawn as nested rings that all run the same way (an even-odd compound path): auto and the default keep its counter, nonzero would fill it
  const ccw = ell(10, 15, 10, 15, 60), inner = ell(10, 15, 5, 10, 60);
  const eo = l => piece(Object.assign({ letter: 'O', rings: [ccw, inner] }, l));
  eq(holesOf(eo({ fill: 'auto' })).length, 2, 'auto keeps the counter of a ring drawn inside another the same way (A and O: 2 holes)'); eq(holesOf(eo({})).length, 2, 'and so does the default');
  eq(holesOf(eo({ fill: 'nonzero' })).length, 1, 'nonzero reads direction only: the same-direction inner ring is ink');
  // a counter of the other direction is a counter in every mode
  const prop = l => piece(Object.assign({ letter: 'O', rings: [ccw, cw(inner)] }, l));
  ok([prop({}), prop({ fill: 'auto' }), prop({ fill: 'nonzero' })].every(r => holesOf(r).length === 2), 'a counter wound the other way is a counter whatever the mode');
  eq(JSON.stringify(piece({ letter: 'O', rings: [ccw, cw(inner)] }).rings), JSON.stringify(piece({ letter: 'O', rings: [ccw, cw(inner)], fill: 'auto' }).rings), 'and the piece is the same');
  // nothing in the letters that already worked changes
  eq(JSON.stringify(M.build({ left: A, middle: Pp, right: V }, undefined).rings), JSON.stringify(M.build({ left: Object.assign({ fill: 'auto' }, A), middle: Object.assign({ fill: 'auto' }, Pp), right: Object.assign({ fill: 'auto' }, V) }).rings), 'with fill auto the letters of the other tests are the same pieces');
}

// ── 3d · real combinations: three 26-letter alphabets (the vendored fonts; Paul's own fonts too when MONO_FONTS names them), the middle one a different font ───────────────────────────────────────────────
const sampleAlphabets = async () => {
  const AL = require('./_monogramAlphabets.cjs'), set = await AL.make(), cache = {};
  const spec = (slot, L) => cache[slot + L] || (cache[slot + L] = { letter: L, rings: set.letters[slot][L].rings.map(r => r.pts) });
  return { set, spec };
};
const checkSample = (sp, words, what) => {
  let bad = [], thin = 0, minCenter = 9, worstHoles = 1, ms = 0;
  for (const w of words) {
    const t = Date.now(), r = M.build({ left: sp.spec('left', w[0]), middle: sp.spec('middle', w[1]), right: sp.spec('right', w[2]) }); ms = Math.max(ms, Date.now() - t);
    if (!r.ok) { bad.push(w + ' ' + r.why); continue; }
    const hl = heightOf(by(r, 'left')), hm = heightOf(by(r, 'middle')), hr = heightOf(by(r, 'right'));
    if (r.rings.filter(x => !x.hole).length !== 1) bad.push(w + ' not one piece');
    if (r.report.holesKept < 0.9) bad.push(w + ' counters ' + r.report.holesKept);
    if (hm < 1.2 * Math.max(hl, hr) - 0.01) bad.push(w + ' centre ' + (hm / Math.max(hl, hr)).toFixed(2));
    if (Math.min(r.report.joinMm.leftMiddle, r.report.joinMm.middleRight) < 0.3) thin++;
    minCenter = Math.min(minCenter, hm / Math.max(hl, hr)); worstHoles = Math.min(worstHoles, r.report.holesKept);
  }
  return { bad, thin, minCenter, worstHoles, ms, n: words.length, what };
};
// ── 4 · svgPath and the PDF file ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
(async () => {
  const r = M.build({ left: A, middle: Pp, right: V }); sound(r, 'APV for the file');
  const d = M.svgPath(r);
  ok(typeof d === 'string' && /^M[\d.]+ [\d.]+L/.test(d) && (d.match(/Z/g) || []).length === r.rings.length && (d.match(/M/g) || []).length === r.rings.length && !/NaN|undefined/.test(d), 'svgPath: one M...Z per ring');
  { const nums = d.match(/-?\d+(\.\d+)?/g).map(Number), xs = nums.filter((_, i) => i % 2 === 0), ys = nums.filter((_, i) => i % 2 === 1);
    ok(Math.min(...xs) === 0 && Math.min(...ys) === 0 && Math.abs(Math.max(...xs) - r.bboxPt[2]) < 0.01 && Math.abs(Math.max(...ys) - r.bboxPt[3]) < 0.01, 'svgPath: the screen box starts at 0,0 and is the bbox in size');
    const first = r.rings[0].pts[0]; ok(d.startsWith('M' + (Math.round(first[0] * 1000) / 1000) + ' ' + (Math.round((r.bboxPt[3] - first[1]) * 1000) / 1000)), 'svgPath: y is flipped for the screen'); }
  ok(M.svgPath(null) === '' && M.svgPath({ ok: false }) === '', 'svgPath of nothing is empty');
  { // the path under the even-odd rule leaves the holes empty: sample it through the rings
    const polys = r.rings.map(x => x.pts); const inside = pt => polys.reduce((c, p) => c ^ inPoly(pt, p), 0) === 1;
    const cx = r.rings[1].pts.reduce((a, p) => a + p[0], 0) / r.rings[1].pts.length, cy = r.rings[1].pts.reduce((a, p) => a + p[1], 0) / r.rings[1].pts.length;
    ok(!inside([cx, cy]) && inside([by(r, 'middle').bboxPt[0] + 3, 2]), 'even-odd over the rings: the counter is empty, the body is filled'); }

  // the file, read back by the app's own reader as ONE charm with the right holes
  const bytes = await M.toPdf(r, { title: 'MONOGRAM APV' });
  ok(bytes instanceof Uint8Array && bytes.length > 400 && PDF.isPdfBytes(bytes), 'toPdf: a PDF');
  eq(Buffer.from(bytes).toString('latin1', 0, 5), '%PDF-', 'toPdf: starts with the PDF header');
  const again = await M.toPdf(r, { title: 'MONOGRAM APV' }); eq(Buffer.from(again).equals(Buffer.from(bytes)), true, 'toPdf: the same bytes for the same piece');
  const parsed = await PDF.parseSource(bytes, 'MONOGRAM_APV.ai'), g = PDF.groupCharms(parsed, { minPt: 6 });
  eq(parsed.segments.filter(s => s.kind === 'path').length, r.rings.length, 'one stroked path per ring');
  eq(g.charms.length, 1, 'the reader finds exactly 1 charm');
  const charm = g.charms[0];
  eq(PDF.cutLinesOf(charm).length, holesOf(r).length, 'with ' + holesOf(r).length + ' holes (cut lines)');
  const wMm = (charm.bbox[2] - charm.bbox[0]) * MM, hMm = (charm.bbox[3] - charm.bbox[1]) * MM;
  ok(Math.abs(wMm - r.wMm) < 0.1 && Math.abs(hMm - r.hMm) < 0.1, `its size ${wMm.toFixed(2)} x ${hMm.toFixed(2)} mm is the result's ${r.wMm} x ${r.hMm} mm`);
  ok(parsed.pageW > r.bboxPt[2] && Math.abs(parsed.pageW - (r.bboxPt[2] + 2 * Math.max(2.25, 0.125 * Math.max(r.bboxPt[2], r.bboxPt[3])))) < 0.01, 'the page is the bbox plus an eighth of the larger side on each side');
  ok(g.orphans.length === 0, 'nothing is left over on the page');
  // the same for a piece with no hole and for the one with many
  const noHole = M.build({ left: V, middle: block('M', 24, 48), right: V }); sound(noHole, 'V M V');
  const g0 = PDF.groupCharms(await PDF.parseSource(await M.toPdf(noHole, { title: 'VMV' }), 'VMV.ai'), { minPt: 6 });
  ok(g0.charms.length === 1 && PDF.cutLinesOf(g0.charms[0]).length === 0, 'a piece without counters is one charm with no holes');
  const omo = M.build({ left: O, middle: Mm, right: O }), g2 = PDF.groupCharms(await PDF.parseSource(await M.toPdf(omo, { title: 'OMO' }), 'OMO.ai'), { minPt: 6 });
  ok(g2.charms.length === 1 && PDF.cutLinesOf(g2.charms[0]).length === 2, 'OMO is one charm with its 2 counters');
  const noTitle = await M.toPdf(r); ok(noTitle.length > 400, 'no options needed');
  await assert.rejects(() => M.toPdf(M.build({}), {}), /no finished monogram/); n++;
  await assert.rejects(() => M.toPdf(null), /no finished monogram/); n++;
  // the built-in script shapes (500 points each) survive the round trip too
  const script = (letter, rx, ry, k) => ({ letter, rings: [ell(0, 0, rx, ry, 500, 0.12, 5 + k), ell(0, 0, rx * 0.45, ry * 0.5, 500, 0.08, 3 + k)] });
  const sr = M.build({ left: script('J', 11, 15, 1), middle: script('R', 14, 22, 2), right: script('S', 11, 15, 3) }), sg = PDF.groupCharms(await PDF.parseSource(await M.toPdf(sr, { title: 'script' }), 'script.ai'), { minPt: 6 });
  ok(sr.ok && sg.charms.length === 1 && PDF.cutLinesOf(sg.charms[0]).length === holesOf(sr).length, 'script monogram: one charm, ' + holesOf(sr).length + ' holes');

  // 3d · real combinations: all 26 letters in the middle with fixed sides, all 26 on each side, every triple (AAA … ZZZ), and APV / OSR
  {
    const sp = await sampleAlphabets(), W = [];
    for (const m of ALPHA) W.push('A' + m + 'V'); for (const l of ALPHA) W.push(l + 'PV'); for (const r of ALPHA) W.push('AP' + r); for (const L of ALPHA) W.push(L + L + L); W.push('APV', 'OSR', 'MOW', 'IWI', 'WIW');
    const res = checkSample(sp, [...new Set(W)], 'vendored fonts');
    const line = `${res.n} real combinations (vendored fonts: 26 middles, 26 lefts, 26 rights, 26 triples, APV, OSR): each ONE piece, counters kept (worst ${res.worstHoles}), the centre at least 1.2 times the sides (least x${res.minCenter.toFixed(2)}), slowest ${res.ms} ms; ${res.thin} with a thin join; problems: ${res.bad.join(' | ') || 'none'}`;
    console.log('  ' + line); ok(res.bad.filter(b => !process.env.MONO_FONTS || !/counters/.test(b)).length === 0, line);   // (Paul's own fonts: a counter a neighbour's stroke crosses is reported, not refused)
    if (process.env.MONO_FONTS) {
      const real = []; let seed2 = 7; const rnd2 = () => { seed2 = (seed2 * 1664525 + 1013904223) >>> 0; return seed2 / 4294967296; };
      for (let i = 0; i < +(process.env.MONO_SAMPLE || 300); i++) real.push(ALPHA[Math.floor(rnd2() * 26)] + ALPHA[Math.floor(rnd2() * 26)] + ALPHA[Math.floor(rnd2() * 26)]);
      const rr = checkSample(sp, [...new Set([...W, ...real])], 'MONO_FONTS');
      console.log(`  MONO_FONTS: ${rr.n} combinations of Paul's fonts: ${rr.bad.length} refused or wrong, ${rr.thin} thin joins, worst counters ${rr.worstHoles}, least centre x${rr.minCenter.toFixed(2)}, slowest ${rr.ms} ms` + (rr.bad.length ? ': ' + rr.bad.slice(0, 20).join(' | ') : ''));
      ok(rr.bad.filter(b => !/counters/.test(b)).length === 0, 'Paul\'s fonts: every combination is one piece with the centre bigger');
    }
  }
  console.log(`monogram engine OK: ${n} checks`);
})().catch(e => { console.error(e); process.exit(1); });
