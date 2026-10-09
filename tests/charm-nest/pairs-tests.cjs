// PAIRTESTS (pairs-1009, area 15): the shared fixtures (pairs-fixtures.cjs), the pair checker the placement oracle uses (sheet, side, group AND mirror:
// a Right is the Left mirrored, a piece is never reflected by the nester), and the sandbox replay carrying a mismatched pair, a disc necklace and a
// letters necklace. Offline, no browser, fake Firestore with the no-nested-arrays check.
//   node tests/charm-nest/pairs-tests.cjs
'use strict';
const assert = require('assert');
const F = require('./pairs-fixtures.cjs');
const Pair = require('../../charm-nest-pair.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, differ = [];
const codes = list => [...new Set(list.map(p => p.code))].sort();
const T10 = F.tx(10), gkey = o => `${F.rid(o)}:${T10}`;

(async () => {
  // 1. the designs say what they are; the piece counts, sides and mirror flags agree with the shared module for every kind
  const D = F.designs();
  ok(!Pair.isMismatched(D['PAIR-STUD'].charm) && Pair.bodiesOf(D['PAIR-HOOP'].charm).length === 1, 'a stud and a hoop welded to its body are ONE body');
  ok(Pair.isMismatched(D['MITTENS-MIS'].charm) && Pair.isMismatched(D['TENNIS-MIS'].charm), 'MITTENS-MIS and TENNIS-MIS are mismatched');
  ok(Pair.bodiesOf(D['PAIR-TWIN'].charm).length === 2 && !Pair.isMismatched(D['PAIR-TWIN'].charm), 'two identical bodies are a matching pair drawn twice');
  const t = D['TENNIS-MIS'].bodies; ok(t.length === 2 && Math.abs(t[0].w - t[1].w) > 3, 'the ball and the racket are two different outlines');
  ok(F.entryOf('MITTENS-MIS').pair.mismatched === true && F.entryOf('PAIR-STUD').pair === undefined, 'the master entry carries `pair` only for a design with several bodies');
  ok(F.entryOf('PAIR-SET-R').facing === 'R' && F.entryOf('PAIR-FACE-L').facing === undefined, 'a person\'s facing is on the master entry; the heuristic\'s is not stored');
  // which way each fixture design faces (written by hand in the fixtures) against what the shared module reads from the drawing
  for (const [sku, per] of Object.entries(F.DESIGN_FACING)) {
    const c = F.charmOf(sku), bodies = Pair.bodiesOf(c);
    ok(bodies.length === per.length, `${sku}: ${bodies.length} bodies, the fixtures say ${per.length}`);
    // (a shape never says which way it faces: null is the Left as drawn, so a declared "L" reads back as null; "R" is only ever a person's word, on the record)
    bodies.forEach((b, i) => ok((Pair.facingOfBody(b, c) || 'L') === (per[i] || 'L'), `${sku} body ${i}: CharmNestPair says it faces ${Pair.facingOfBody(b, c)}, the fixtures say ${per[i]}`));
    ok((Pair.facingOf(c) || 'L') === (per[0] || 'L'), `${sku}: facingOf ${Pair.facingOf(c)} vs ${per[0]}`);
  }
  for (const kind of Object.keys(F.KINDS)) for (const qty of [1, 2, 3]) for (const count of kind === 'discs' || kind === 'letters' ? [2, 3, 4] : [0]) {
    const w = F.world({ orders: [{ rid: F.rid(1), lines: [{ n: 10, kind, qty, discs: count, letters: count }] }] }), pcs = w.pieces, per = F.KINDS[kind].pair, earring = kind === 'pair' || kind === 'hoop' || kind === 'mismatched';
    ok(pcs.length === F.pieceCount(kind, qty, count), `${kind} x${qty} ${count || ''}: ${pcs.length} pieces`);
    if (earring) {   // quantity q = q Left + q Right, copies alternate L, R; the mirror flag follows the side and the way the body faces
      ok(pcs.filter(p => p.side === 'L').length === qty && pcs.filter(p => p.side === 'R').length === qty && pcs.map(p => p.side).join() === Array.from({ length: pcs.length }, (_, i) => (i % 2 ? 'R' : 'L')).join(), `${kind} x${qty}: ${qty} Left and ${qty} Right, alternating`);
      ok(pcs.every(p => p.mirror === F.expectedMirror(p.sku, p.side, p.bodyIndex)), `${kind} x${qty}: the mirror flag is side !== facing for every piece`);
      ok(pcs.every((p, i) => i % 2 === 0 || kind === 'mismatched' || pcs[i - 1].mirror !== p.mirror), `${kind} x${qty}: of a Left and a Right of one body exactly one is the mirror`);
    } else ok(pcs.every(p => p.side === null && p.mirror === false && p.bodyIndex === 0), `${kind} x${qty} ${count || ''}: no side, never mirrored`);
    // (informational: what the shared module says when the line carries NO explicit pieceCount and no pair kind)
    const bare = Object.assign({}, w.orders[0].lines[0].line); delete bare.pieceCount; delete bare.pair; const free = Pair.pieceCountOf(bare, w.orders[0].lines[0].charm);
    if (free !== pcs.length) differ.push(`${kind} x${qty}${count ? ' ' + count : ''}: plan ${pcs.length}, module without an explicit count ${free}`);
    void per;
  }
  const mis = F.world({ orders: [{ rid: F.rid(1), lines: [{ n: 10, kind: 'mismatched', qty: 2 }] }] }).pieces;
  ok(mis.map(p => p.side).join() === 'L,R,L,R' && mis.map(p => p.bodyIndex).join() === '0,1,0,1' && mis.every(p => p.groupSize === 4 && p.groupKey === gkey(1)), 'a mismatched line x2 makes L,R,L,R in one group of 4');
  ok(mis.map(p => p.mirror).join() === 'false,true,false,true', 'the picture\'s two mittens both face left: the Right earring (body 2) is the mirror');
  const mp = F.world({ orders: [{ rid: F.rid(1), lines: [{ n: 10, kind: 'pair' }, { n: 11, kind: 'pair', sku: 'PAIR-FACE-R' }, { n: 12, kind: 'pair', sku: 'PAIR-SET-R' }] }] }).pieces.map(p => `${p.sku}:${p.side}:${p.mirror}`);
  ok(mp.join() === 'PAIR-FACE-L:L:false,PAIR-FACE-L:R:true,PAIR-FACE-R:L:true,PAIR-FACE-R:R:false,PAIR-SET-R:L:true,PAIR-SET-R:R:false', 'a matching pair has a Left and a Right; whichever faces the way the drawing faces is as drawn: ' + mp.join());

  // 2. every named case: the checker finds exactly what the case says, and nothing in the same world with the pair fields taken away (an old record)
  for (const c of F.caseList()) {
    ok(codes(F.problems(c.world)).join() === c.expect.slice().sort().join(), `case ${c.id}: ${JSON.stringify(F.problems(c.world).slice(0, 3))} vs ${JSON.stringify(c.expect)}`);
    const old = F.legacy(c.world); ok(!old.pool.some(p => 'side' in p || 'groupKey' in p || 'mirror' in p) && !old.sheets.some(s => s.charms.some(ch => 'side' in ch || 'groupSize' in ch || 'mirror' in ch || 'shapeJson' in ch)), `case ${c.id}: legacy strips the pair fields`);
    ok(codes(F.problems(old)).join() === c.expect.filter(x => !/side|key|size|mirror|shape|reflected/.test(x)).sort().join(), `case ${c.id} (old records): ${JSON.stringify(codes(F.problems(old)))}`);
  }
  const sheetOf = (w, id) => w.sheets.find(s => s.id === id), charmsOf = (w, o, id) => sheetOf(w, id || 'sh-gf1').charms.filter(c => c.groupKey === gkey(o));
  const mt = F.cases.mismatchedTwoOutlines(), ms = charmsOf(mt, 8);
  ok(ms.length === 2 && ms[0].side === 'L' && ms[1].side === 'R' && ms[0].widthPt !== ms[1].widthPt, 'the sheet record carries both bodies, L then R, with their own sizes');
  const mx = F.cases.mixedSheet(); ok(sheetOf(mx, 'sh-gf1').charms.length === 2 + 2 + 4 + 1 + 2 + 3 + 2, 'the mixed sheet holds 2 studs + 2 hoops + 4 discs + 1 pendant + 2 mismatched + 3 letters + 2 singles');
  // the Right lies as the mirror of the Left, whichever way the drawing faces, and turning changes nothing
  const state = (w, o, copy, sku, id) => { const c = charmsOf(w, o, id).find(c => c.poolId.endsWith('_' + copy)); return F.shapeState(JSON.parse(c.shapeJson), F.bodyPolys(sku || 'PAIR-FACE-L', c.bodyIndex)); };
  const p1 = F.cases.pairOneSheet(), p2 = F.cases.pairFacesRight(), p3 = F.cases.pairQty2(), p4 = F.cases.pairRotated();
  ok(state(p1, 1, 1) === 'asDrawn' && state(p1, 1, 2) === 'mirrored', 'a left-facing pair: the Left lies as drawn, the Right mirrored');
  ok(state(p2, 11, 1, 'PAIR-FACE-R') === 'mirrored' && state(p2, 11, 2, 'PAIR-FACE-R') === 'asDrawn', 'a right-facing pair: the Left is the mirror, the Right is as drawn');
  ok(charmsOf(p3, 13).map(c => c.side).join() === 'L,R,L,R' && [1, 2, 3, 4].every(k => state(p3, 13, k) === (k % 2 ? 'asDrawn' : 'mirrored')), 'quantity 2 lies as 2 Left (as drawn) and 2 Right (mirrored)');
  ok(new Set(sheetOf(p4, 'sh-gf1').placements.map(p => p.angle)).size === 4 && [1, 2, 3, 4].every(k => state(p4, 14, k) === (k % 2 ? 'asDrawn' : 'mirrored')), 'turned by 0, 90, 180 and 270 degrees the pieces keep their direction');
  ok(charmsOf(F.cases.pairSymmetric(), 12).every(c => F.shapeState(JSON.parse(c.shapeJson), F.bodyPolys('PAIR-STUD', 0)) === 'either'), 'a symmetric stud fits both ways (the checker cannot see a reflection there, only the flags)');
  ok(charmsOf(F.cases.discs3Sheets(), 4, 'sh-gf1').concat(charmsOf(F.cases.lettersNecklace(), 15)).every(c => c.side === null && c.mirror === false), 'discs and letters: no side, not mirrored');
  ok(charmsOf(F.cases.singleLine(), 16).length === 2 && charmsOf(F.cases.singleLine(), 16).every(c => c.side === null && c.mirror === false), 'a "Single" earring line, quantity 2: two pieces, no side, not mirrored');

  // 3. the fake Firestore refuses an array inside an array, on every path a write takes
  const fs = F.fakeFirestore();
  fs.seed(F.cases.discs3Sheets());
  ok(fs.list(F.COLL.pool).length >= 7, 'the world is in the fake');
  assert.throws(() => fs.put(F.COLL.sheets, 'bad', { id: 'bad', outline: [[0, 0], [1, 1]] }), /Nested arrays are not allowed/); n++;
  await assert.rejects(fs.db.collection(F.COLL.sheets).doc('bad2').set({ id: 'bad2', polygon: [[0, 0], [1, 1]] }), /Nested arrays are not allowed/); n++;
  await assert.rejects(fs.db.collection(F.COLL.sheets).doc('sh-gf1').update({ outline: [[0, 0]] }), /Nested arrays are not allowed/); n++;
  await fs.db.collection(F.COLL.sheets).doc('ok1').set({ id: 'ok1', outlineJson: JSON.stringify([[0, 0], [1, 1]]), flat: [0, 0, 1, 1], inMap: [{ a: [1, 2] }] }); n++;   // (JSON strings, flat arrays and arrays of maps are fine)
  for (const w of F.caseList().map(c => c.world)) F.fakeFirestore().seed(w);   // (no case writes a nested array: a laid outline is a shapeJson STRING)
  n++;
  const nested = F.clone(F.cases.pairOneSheet()); nested.sheets[0].charms[0].shape = JSON.parse(nested.sheets[0].charms[0].shapeJson);   // (the same outline as a list of point lists: Firestore refuses it)
  assert.throws(() => F.fakeFirestore().seed(nested), /Nested arrays are not allowed/); n++;

  // 4. the mutants: a pair whose pieces disagree about their sheet, side, group or mirror is found, each by its own code
  const find = (name, mutate, want, base, opts) => { const w = F.clone((base || F.cases.mismatchedSplit)()); mutate(w); const got = codes(F.problems(w, opts)); ok(want.every(c => got.includes(c)), `mutant ${name}: wanted ${want} in ${JSON.stringify(got)}`); return got; };
  const clean = (name, mutate, base, opts) => { const w = F.clone(base()); mutate(w); const got = F.problems(w, opts); ok(got.length === 0, `${name}: nothing to report, got ${JSON.stringify(got.slice(0, 2))}`); };
  const rowOf = (w, i, o) => w.pool.filter(p => p.groupKey && p.groupKey.endsWith(':' + T10) && (!o || p.groupKey === gkey(o)))[i], chOf = (w, i, o) => w.sheets.flatMap(s => s.charms).filter(c => c.groupKey && c.groupKey.endsWith(':' + T10) && (!o || c.groupKey === gkey(o)))[i];
  const setBoth = (w, i, o, patch) => { Object.assign(rowOf(w, i, o), patch); Object.assign(chOf(w, i, o), patch); };
  const reflect = c => { c.shapeJson = JSON.stringify(JSON.parse(c.shapeJson).map(p => p.map(q => [-q[0], q[1]]))); };   // mirror a laid outline (what a reflecting nester would do)
  find('a pool row flips its side', w => { rowOf(w, 0).side = 'R'; }, ['side-mismatch', 'side-pairing']);
  find('a sheet charm flips its side', w => { chOf(w, 1).side = 'L'; }, ['side-mismatch']);
  find('both pieces are Left', w => { setBoth(w, 1, 9, { side: 'L' }); }, ['side-mismatch', 'side-pairing']);
  find('a pool row carries another group', w => { rowOf(w, 1).groupKey = `${F.rid(1)}:${F.tx(77)}`; }, ['key-mismatch']);
  find('a sheet charm carries another group', w => { chOf(w, 0).groupKey = `${F.rid(1)}:${F.tx(77)}`; }, ['key-mismatch']);
  find('one piece thinks the group is 3', w => { rowOf(w, 0).groupSize = 3; }, ['size-mismatch']);
  find('a pool row is gone', w => { w.pool.splice(w.pool.indexOf(rowOf(w, 1)), 1); }, ['missing-piece']);
  find('the pool row names another sheet than the one that lists it', w => { rowOf(w, 1).sheetId = 'sh-gf3'; }, ['sheet-disagree']);
  find('a sheet lists a piece it holds no charm for', w => { const s = w.sheets.find(s => s.id === 'sh-gf2'); s.charms = s.charms.filter(c => !(c.groupKey || '').endsWith(':' + T10)); }, ['sheet-disagree']);
  find('a charm has no placement', w => { const s = w.sheets.find(s => s.id === 'sh-gf2'); const c = s.charms.find(c => (c.groupKey || '').endsWith(':' + T10)); s.placements = s.placements.filter(p => p.id !== c.id); }, ['sheet-disagree']);
  find('one piece listed by two sheets', w => { const a = w.sheets.find(s => s.id === 'sh-gf1'), b = w.sheets.find(s => s.id === 'sh-gf2'), id = rowOf(w, 0).poolId; b.poolIds.push(id); b.charms.push(Object.assign({}, a.charms.find(c => c.poolId === id), { id: 'dup' })); b.placements.push({ id: 'dup', cxPt: 1, cyPt: 1, angle: 0, wPt: 1, hPt: 1 }); }, ['duplicate-piece']);
  find('the split is not tracked', w => { w.tracked = []; }, ['untracked-split']);
  find('the right piece on a sheet of another set', w => { w.sheets.find(s => s.id === 'sh-gf2').setId = 'set-2'; w.sets[0].sheetIds = w.sets[0].sheetIds.filter(id => id !== 'sh-gf2'); w.sets.push({ setId: 'set-2', seq: 2, sheetIds: ['sh-gf2'] }); rowOf(w, 1).setId = 'set-2'; }, ['split-across-sets']);
  find('a silver piece among gold ones', w => { rowOf(w, 1).material = 'silver'; }, ['metal-mismatch']);
  find('the pool row says another set than its sheet', w => { rowOf(w, 0).setId = 'set-2'; }, ['set-disagree']);
  find('a set does not list its sheet', w => { w.sets[0].sheetIds = w.sets[0].sheetIds.filter(id => id !== 'sh-gf1'); }, ['set-disagree']);
  find('the set record lists the Right on the Left\'s sheet', w => { w.sets[0].orders[F.rid(2)].lines[0].copies[1].sheetId = 'sh-gf1'; }, ['set-copy-disagree'], F.cases.pairSplitOneSet);
  find('the set record says the Right is a Left', w => { w.sets[0].orders[F.rid(2)].lines[0].copies[1].side = 'L'; }, ['set-copy-disagree'], F.cases.pairSplitOneSet);
  find('the set record lists a copy on a sheet that does not hold it', w => { w.sets[0].orders[F.rid(2)].lines[0].copies[0].sheetId = 'sh-gf3'; }, ['set-copy-disagree'], F.cases.pairSplitOneSet);
  find('half the pair is on hold', w => { Object.assign(rowOf(w, 0), { state: 'abandoned', sheetId: null, setId: null, heldAt: 1, heldBy: 'Paul' }); const s = w.sheets.find(s => s.id === 'sh-gf1'); const id = rowOf(w, 0).poolId; s.poolIds = s.poolIds.filter(p => p !== id); s.charms = s.charms.filter(c => c.poolId !== id); }, ['half-held']);
  find('a disc is missing from a 3-disc necklace', w => { w.pool.splice(w.pool.findIndex(p => p.copy === 3), 1); }, ['missing-piece'], F.cases.discs3Sheets);
  find('a disc waits while the others are on sheets', w => { const p = w.pool.find(p => p.copy === 3 && p.groupSize === 3); const s = w.sheets.find(s => s.id === p.sheetId); Object.assign(p, { state: 'ready', sheetId: null, setId: null }); s.poolIds = s.poolIds.filter(x => x !== p.poolId); s.charms = s.charms.filter(c => c.poolId !== p.poolId); }, ['untracked-split'], () => { const w = F.cases.discs3Sheets(); w.tracked = []; return w; });
  // sides (Amendment 2: a matching pair has a Left and a Right too)
  find('a matching pair loses its sides (the old model)', w => { for (const p of w.pool.filter(p => p.groupKey === gkey(1))) p.side = null; for (const c of chOf(w, 0, 1) ? w.sheets.flatMap(s => s.charms).filter(c => c.groupKey === gkey(1)) : []) c.side = null; }, ['side-missing'], F.cases.pairOneSheet);
  find('a sheet charm of a pair has no side field', w => { delete chOf(w, 1, 1).side; }, ['side-missing'], F.cases.pairOneSheet);
  find('a matching pair has two Lefts', w => { setBoth(w, 1, 1, { side: 'L' }); }, ['side-mismatch', 'side-pairing'], F.cases.pairOneSheet);
  find('a matching pair\'s Right is on body 2', w => { setBoth(w, 1, 1, { bodyIndex: 1 }); }, ['side-pairing'], F.cases.pairOneSheet);
  find('a mismatched Left uses the right body', w => { setBoth(w, 0, 9, { bodyIndex: 1 }); }, ['side-pairing']);
  find('a disc says it is the Left', w => { rowOf(w, 0, 4).side = 'L'; chOf(w, 0, 4).side = 'L'; }, ['side-unexpected'], F.cases.discs3Sheets);
  find('a letter says it is the Right', w => { rowOf(w, 1, 15).side = 'R'; chOf(w, 1, 15).side = 'R'; }, ['side-unexpected'], F.cases.lettersNecklace);
  find('a Single earring line gets a Left and a Right', w => { setBoth(w, 0, 16, { side: 'L' }); setBoth(w, 1, 16, { side: 'R' }); }, ['side-unexpected'], F.cases.singleLine);
  // mirror flags
  find('the Right is not flagged mirrored', w => { setBoth(w, 1, 1, { mirror: false }); }, ['mirror-mismatch', 'mirror-pairing'], F.cases.pairOneSheet);
  find('the Left is flagged mirrored too', w => { setBoth(w, 0, 1, { mirror: true }); }, ['mirror-mismatch', 'mirror-pairing'], F.cases.pairOneSheet);
  find('a right-facing design: the Right flagged mirrored', w => { setBoth(w, 1, 11, { mirror: true }); }, ['mirror-mismatch', 'mirror-pairing'], F.cases.pairFacesRight);
  find('a symmetric pair with both flags false', w => { setBoth(w, 1, 12, { mirror: false }); }, ['mirror-mismatch', 'mirror-pairing'], F.cases.pairSymmetric);
  find('the pool row says mirrored, the sheet says not', w => { chOf(w, 1, 1).mirror = false; }, ['mirror-mismatch'], F.cases.pairOneSheet);
  find('a pool row has no mirror flag', w => { delete rowOf(w, 1, 1).mirror; }, ['mirror-missing'], F.cases.pairOneSheet);
  find('a sheet charm has no mirror flag', w => { delete chOf(w, 1, 1).mirror; }, ['mirror-missing'], F.cases.pairOneSheet);
  find('the second Right of quantity 2 is not mirrored', w => { setBoth(w, 3, 13, { mirror: false }); }, ['mirror-mismatch', 'mirror-pairing'], F.cases.pairQty2);
  find('the mismatched Right body (facing left) is not mirrored', w => { setBoth(w, 1, 18, { mirror: false }); }, ['mirror-mismatch'], F.cases.mismatchedMittens);
  find('a disc is marked mirrored', w => { rowOf(w, 1, 4).mirror = true; }, ['mirror-unexpected'], F.cases.discs3Sheets);
  find('a letter is marked mirrored', w => { chOf(w, 0, 15).mirror = true; }, ['mirror-unexpected'], F.cases.lettersNecklace);
  find('a Single earring is marked mirrored', w => { rowOf(w, 1, 16).mirror = true; }, ['mirror-unexpected'], F.cases.singleLine);
  // the geometry on the sheet: the Right is the Left mirrored, and the nester only turned it
  const lay = (w, o, copy, sh) => { const c = w.sheets.flatMap(s => s.charms).find(c => c.groupKey === gkey(o) && c.poolId.endsWith('_' + copy)); c.shapeJson = sh; return c; };
  find('the Right lies as the Left (never mirrored): not a mirror image', w => { const L = chOf(w, 0, 1); lay(w, 1, 2, L.shapeJson); }, ['not-mirror', 'reflected'], F.cases.pairOneSheet);
  find('the Right was never mirrored and its flag says so too', w => { const L = chOf(w, 0, 1); lay(w, 1, 2, L.shapeJson); setBoth(w, 1, 1, { mirror: false }); }, ['not-mirror', 'mirror-mismatch', 'mirror-pairing'], F.cases.pairOneSheet);
  find('the nester reflected the Left', w => { reflect(chOf(w, 0, 1)); }, ['reflected', 'not-mirror'], F.cases.pairOneSheet);
  find('the nester reflected the Right back', w => { reflect(chOf(w, 1, 1)); }, ['reflected', 'not-mirror'], F.cases.pairOneSheet);
  find('the nester reflected the second Right of quantity 2', w => { reflect(chOf(w, 3, 13)); }, ['reflected'], F.cases.pairQty2);
  find('a right-facing design: the Left lies as drawn (not mirrored)', w => { const R = chOf(w, 1, 11); lay(w, 11, 1, R.shapeJson); }, ['reflected', 'not-mirror'], F.cases.pairFacesRight);
  find('a placement carries flipX', w => { w.sheets.find(s => s.id === 'sh-gf1').placements[0].flipX = true; }, ['reflected'], F.cases.pairOneSheet);
  find('a placement carries a negative scale', w => { w.sheets.find(s => s.id === 'sh-gf1').placements[1].scaleX = -1; }, ['reflected'], F.cases.pairOneSheet);
  find('a mismatched Right body was reflected', w => { reflect(chOf(w, 1, 18)); }, ['reflected'], F.cases.mismatchedMittens);
  find('the nester reflected a turned piece (turned and reflected)', w => { reflect(chOf(w, 2, 14)); }, ['reflected'], F.cases.pairRotated);
  find('a Single earring (asymmetric design) was reflected', w => { const w2 = F.world({ orders: [{ rid: F.rid(30), lines: [{ n: 10, kind: 'earring-single', sku: 'PAIR-FACE-L', on: 'sh-gf1' }] }] }); w.sheets = w2.sheets; w.pool = w2.pool; w.sets = w2.sets; w.run = w2.run; w.kinds = w2.kinds; reflect(w.sheets[0].charms[0]); }, ['reflected'], F.cases.pairOneSheet);
  find('the wrong body is on the sheet', w => { lay(w, 1, 1, w.sheets.flatMap(s => s.charms).find(c => c.sku === 'ONE-PENDANT').shapeJson); }, ['shape-mismatch'], F.cases.pairOneSheet);
  find('an unreadable outline', w => { chOf(w, 0, 1).shapeJson = '[[[0,0'; }, ['shape-mismatch'], F.cases.pairOneSheet);
  find('the mismatched Left shows the Right body', w => { lay(w, 8, 1, chOf(w, 1, 8).shapeJson); }, ['shape-mismatch'], F.cases.mismatchedTwoOutlines);
  // what must NOT be reported
  clean('turning every piece by 90 degrees', w => { for (const s of w.sheets) for (const p of s.placements) p.angle = (p.angle + 90) % 360; }, F.cases.pairRotated);
  clean('a mirrored piece in a world made before Amendment 2 (checked with mirror: false)', w => { for (const p of w.pool) { delete p.mirror; } for (const s of w.sheets) for (const c of s.charms) { delete c.mirror; delete c.shapeJson; } }, F.cases.pairOneSheet, { mirror: false });
  clean('a waiting pair piece that has no shape yet', w => { const p = w.pool.find(p => p.groupKey === gkey(10)); Object.assign(p, { state: 'ready', sheetId: null, setId: null }); }, () => { const w = F.world({ orders: [{ rid: F.rid(10), lines: [{ n: 10, kind: 'pair', on: [null, null] }] }] }); return w; }, { allowWaiting: true });
  // (and the broken worlds read through the fake Firestore the way a test reads a flow's result)
  const bad = F.clone(F.cases.mismatchedTwoOutlines()); bad.pool.find(p => p.side === 'R').side = 'L';
  const fs2 = F.fakeFirestore(); fs2.seed(bad); ok(codes(F.problems(fs2.docsOf())).includes('side-pairing'), 'the checker reads the fake Firestore too (docsOf)');
  const bad2 = F.clone(F.cases.pairOneSheet()); { const c = bad2.sheets.flatMap(s => s.charms).find(c => c.groupKey === gkey(1) && c.side === 'R'); reflect(c); }
  const fs5 = F.fakeFirestore(); fs5.seed(bad2); ok(codes(F.problems(fs5.docsOf())).includes('reflected'), 'a reflected piece is found when the records are read back from the fake Firestore (the kinds come from the run record)');

  // 5. the real server over the fake: a pair's records (side, groupKey, bodyIndex, groupSize, mirror) are kept by putSheet, and getOrderPieces reads the order's pieces
  const fs3 = F.fakeFirestore(), fns = F.functions(fs3, ['charmNestLibrary']);
  try {
    const w = F.cases.mismatchedSplit(); fs3.seed(w);
    const sheet = F.clone(w.sheets.find(s => s.id === 'sh-gf1')), r = await fns.lib('putSheet', { sheet });
    ok(!r.error, 'putSheet over the fake accepts a sheet with pair fields: ' + (r.error || 'ok'));
    const saved = fs3.get(F.COLL.sheets, 'sh-gf1'); ok(saved.charms.some(c => c.side === 'L' && c.groupKey === gkey(9)), 'the server kept the charm\'s side and groupKey');
    const keptMirror = saved.charms.filter(c => c.groupKey === gkey(9)).map(c => c.mirror);
    if (!keptMirror.every(v => typeof v === 'boolean')) differ.push('putSheet drops the `mirror` flag of a sheet charm (PAIRSERVER)');
    const ce = console.error; console.error = () => {};   // (the handler logs the refusal's stack)
    const bad = await fns.lib('putSheet', { sheet: Object.assign(F.clone(sheet), { id: 'sh-bad', outline: [[0, 0], [1, 1]] }) }).finally(() => { console.error = ce; });   // (a polygon stored as an array of arrays: live Firestore refuses it, the fake does too)
    ok(/Nested arrays are not allowed/.test(bad.error || ''), 'the real putSheet over the strict fake refuses an array inside an array: ' + JSON.stringify(bad).slice(0, 120));
    const rp = await fns.lib('getOrderPieces', { orderIds: [F.rid(9)] });
    ok(!rp.error, 'getOrderPieces answers over the fake: ' + (rp.error || 'ok'));
    ok(codes(F.problems(fs3.docsOf(), { tracked: w.tracked })).length === 0, 'nothing the server did made the pair disagree: ' + JSON.stringify(F.problems(fs3.docsOf(), { tracked: w.tracked })));
  } finally { fns.restore(); }

  // 6. the sandbox replay carries a mismatched pair, a disc necklace, a letters necklace and a matching pair unchanged (only the times move)
  const fs4 = F.fakeFirestore(), fns4 = F.functions(fs4, ['etsySandbox']);
  try {
    const w = F.world({ orders: [{ rid: F.rid(21), lines: [{ n: 10, kind: 'mismatched', sku: 'TENNIS-MIS', qty: 1 }] }, { rid: F.rid(22), lines: [{ n: 10, kind: 'discs', discs: 3 }, { n: 11, kind: 'pair', qty: 2 }, { n: 12, kind: 'earring-single', qty: 2 }, { n: 13, kind: 'letters', letters: 4 }] }, { rid: F.rid(23), lines: [{ n: 10, kind: 'hoop' }] }, { rid: F.rid(24), lines: [{ n: 10, kind: 'mismatched', sku: 'MITTENS-MIS', qty: 2 }] }] });
    const snap = F.receipts(w), stream = { seed: 42069871, min: 2, max: 5, simStart: 1791600000000, stepMs: 600000, on: true, v: 2, tick: 0 }, came = new Map();
    for (let k = 1; k <= 4; k++) for (const r of fns4.handlers.etsySandbox.batch(stream, snap, k, { at: 1791500000000 })) came.set(String(r.receipt_id), r);
    ok(came.size === snap.length, `the stream brought every order once (${came.size} of ${snap.length})`);
    for (const src of snap) {
      const got = came.get(String(src.receipt_id)); ok(got && got.transactions.length === src.transactions.length, `${src.receipt_id}: every line arrived`);
      src.transactions.forEach((tx0, i) => { const a = got.transactions[i]; ok(['transaction_id', 'receipt_id', 'listing_id', 'sku', 'title', 'quantity'].every(k => a[k] === tx0[k]) && JSON.stringify(a.variations) === JSON.stringify(tx0.variations), `${src.receipt_id} line ${i}: sku, quantity, variations and numbers are unchanged`); });
    }
    // what the sorter will make of each arriving line (the shared module, from the receipt line alone): the same pieces the world expects
    for (const o of w.orders) for (const l of o.lines) { const arrived = came.get(o.rid).transactions.find(x => String(x.transaction_id) === F.tx(l.n)); ok(arrived.quantity === l.qty && /\d discs/.test(JSON.stringify(arrived.variations)) === (l.kind === 'discs'), `${o.rid}/${l.n}: the disc count rides in the variation`); }
  } finally { fns4.restore(); }
  if (differ.length) console.log(`  note: ${differ.length} difference(s) between the plan and the code today, e.g. ${differ.slice(0, 3).join('; ')} (the intake must set the count: PAIRINTAKE)`);
  console.log(`pairs-tests: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
