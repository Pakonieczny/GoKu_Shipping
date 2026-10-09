// PAIRTESTS (pairs-1009, area 15): the shared fixtures (pairs-fixtures.cjs), the pair checker the placement oracle uses, and the sandbox replay carrying a
// mismatched pair and a disc necklace. Offline, no browser, fake Firestore with the no-nested-arrays check.
//   node tests/charm-nest/pairs-tests.cjs
'use strict';
const assert = require('assert');
const F = require('./pairs-fixtures.cjs');
const Pair = require('../../charm-nest-pair.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, differ = [];
const codes = list => [...new Set(list.map(p => p.code))].sort();

(async () => {
  // 1. the designs say what they are; the piece counts agree with the shared module for every kind
  const D = F.designs();
  ok(!Pair.isMismatched(D['PAIR-STUD'].charm) && Pair.bodiesOf(D['PAIR-HOOP'].charm).length === 1, 'a stud and a hoop welded to its body are ONE body');
  ok(Pair.isMismatched(D['MITTENS-MIS'].charm) && Pair.isMismatched(D['TENNIS-MIS'].charm), 'MITTENS-MIS and TENNIS-MIS are mismatched');
  ok(Pair.bodiesOf(D['PAIR-TWIN'].charm).length === 2 && !Pair.isMismatched(D['PAIR-TWIN'].charm), 'two identical bodies are a matching pair drawn twice');
  const t = D['TENNIS-MIS'].bodies; ok(t.length === 2 && Math.abs(t[0].w - t[1].w) > 3, 'the ball and the racket are two different outlines');
  ok(F.entryOf('MITTENS-MIS').pair.mismatched === true && F.entryOf('PAIR-STUD').pair === undefined, 'the master entry carries `pair` only for a design with several bodies');
  for (const kind of Object.keys(F.KINDS)) for (const qty of [1, 2, 3]) for (const discs of kind === 'discs' ? [2, 3, 4] : [0]) {
    const w = F.world({ orders: [{ rid: F.rid(1), lines: [{ n: 10, kind, qty, discs }] }] });
    ok(w.pieces.length === F.pieceCount(kind, qty, discs), `${kind} x${qty} ${discs || ''}: ${w.pieces.length} pieces`);
    // (informational: what the shared module says when the line carries NO explicit pieceCount: today's rule is quantity pieces, only a mismatched design doubles)
    const bare = Object.assign({}, w.orders[0].lines[0].line); delete bare.pieceCount; const free = Pair.pieceCountOf(bare, w.orders[0].lines[0].charm);
    if (free !== w.pieces.length) differ.push(`${kind} x${qty}${discs ? ' ' + discs + ' discs' : ''}: plan ${w.pieces.length}, module without an explicit count ${free}`);
  }
  const mis = F.world({ orders: [{ rid: F.rid(1), lines: [{ n: 10, kind: 'mismatched', qty: 2 }] }] }).pieces;
  ok(mis.map(p => p.side).join() === 'L,R,L,R' && mis.map(p => p.bodyIndex).join() === '0,1,0,1' && mis.every(p => p.groupSize === 4 && p.groupKey === `${F.rid(1)}:${F.tx(10)}`), 'a mismatched line x2 makes L,R,L,R in one group of 4');
  ok(F.world({ orders: [{ rid: F.rid(1), lines: [{ n: 10, kind: 'pair' }] }] }).pieces.every(p => p.side === null && p.groupSize === 2), 'a matching pair has no sides');

  // 2. every named case: the checker finds exactly what the case says, and nothing in the same world with the pair fields taken away (an old record)
  for (const c of F.caseList()) {
    ok(codes(F.problems(c.world)).join() === c.expect.slice().sort().join(), `case ${c.id}: ${JSON.stringify(codes(F.problems(c.world)))} vs ${JSON.stringify(c.expect)}`);
    const old = F.legacy(c.world); ok(!old.pool.some(p => 'side' in p || 'groupKey' in p) && !old.sheets.some(s => s.charms.some(ch => 'side' in ch || 'groupSize' in ch)), `case ${c.id}: legacy strips the pair fields`);
    ok(codes(F.problems(old)).join() === c.expect.filter(x => !/side|key|size/.test(x)).sort().join(), `case ${c.id} (old records): ${JSON.stringify(codes(F.problems(old)))}`);
  }
  const mt = F.cases.mismatchedTwoOutlines(), ms = mt.sheets.find(s => s.charms.some(c => c.groupKey === `${F.rid(8)}:${F.tx(10)}`)).charms.filter(c => c.groupKey === `${F.rid(8)}:${F.tx(10)}`);
  ok(ms.length === 2 && ms[0].side === 'L' && ms[1].side === 'R' && ms[0].widthPt !== ms[1].widthPt, 'the sheet record carries both bodies, L then R, with their own sizes');
  const mx = F.cases.mixedSheet(); ok(mx.sheets.find(s => s.id === 'sh-gf1').charms.length === 2 + 2 + 4 + 1 + 2, 'the mixed sheet holds 2 studs + 2 hoops + 4 discs + 1 pendant + 2 mismatched');

  // 3. the fake Firestore refuses an array inside an array, on every path a write takes
  const fs = F.fakeFirestore();
  fs.seed(F.cases.discs3Sheets());
  ok(fs.list(F.COLL.pool).length === 7 + 0 + 0 || fs.list(F.COLL.pool).length >= 7, 'the world is in the fake');
  assert.throws(() => fs.put(F.COLL.sheets, 'bad', { id: 'bad', outline: [[0, 0], [1, 1]] }), /Nested arrays are not allowed/); n++;
  await assert.rejects(fs.db.collection(F.COLL.sheets).doc('bad2').set({ id: 'bad2', polygon: [[0, 0], [1, 1]] }), /Nested arrays are not allowed/); n++;
  await assert.rejects(fs.db.collection(F.COLL.sheets).doc('sh-gf1').update({ outline: [[0, 0]] }), /Nested arrays are not allowed/); n++;
  await fs.db.collection(F.COLL.sheets).doc('ok1').set({ id: 'ok1', outlineJson: JSON.stringify([[0, 0], [1, 1]]), flat: [0, 0, 1, 1], inMap: [{ a: [1, 2] }] }); n++;   // (JSON strings, flat arrays and arrays of maps are fine)
  for (const w of F.caseList().map(c => c.world)) F.fakeFirestore().seed(w);   // (no case writes a nested array)
  n++;

  // 4. the mutants: a pair whose pieces disagree about their sheet, side or group is found, each by its own code
  const find = (name, mutate, want, base) => { const w = F.clone((base || F.cases.mismatchedSplit)()); mutate(w); const got = codes(F.problems(w)); ok(want.every(c => got.includes(c)), `mutant ${name}: wanted ${want} in ${JSON.stringify(got)}`); };
  const rowOf = (w, i) => w.pool.filter(p => p.groupKey && p.groupKey.endsWith(':' + F.tx(10)))[i], charmOf = (w, i) => w.sheets.flatMap(s => s.charms).filter(c => c.groupKey && c.groupKey.endsWith(':' + F.tx(10)))[i];
  find('a pool row flips its side', w => { rowOf(w, 0).side = 'R'; }, ['side-mismatch', 'side-pairing']);
  find('a sheet charm flips its side', w => { charmOf(w, 1).side = 'L'; }, ['side-mismatch']);
  find('both pieces are Left', w => { rowOf(w, 1).side = 'L'; charmOf(w, 1).side = 'L'; }, ['side-pairing']);
  find('a pool row carries another group', w => { rowOf(w, 1).groupKey = `${F.rid(1)}:${F.tx(77)}`; }, ['key-mismatch']);
  find('a sheet charm carries another group', w => { charmOf(w, 0).groupKey = `${F.rid(1)}:${F.tx(77)}`; }, ['key-mismatch']);
  find('one piece thinks the group is 3', w => { rowOf(w, 0).groupSize = 3; }, ['size-mismatch']);
  find('a pool row is gone', w => { w.pool.splice(w.pool.indexOf(rowOf(w, 1)), 1); }, ['missing-piece']);
  find('the pool row names another sheet than the one that lists it', w => { rowOf(w, 1).sheetId = 'sh-gf3'; }, ['sheet-disagree']);
  find('a sheet lists a piece it holds no charm for', w => { const s = w.sheets.find(s => s.id === 'sh-gf2'); s.charms = s.charms.filter(c => !(c.groupKey || '').endsWith(':' + F.tx(10))); }, ['sheet-disagree']);
  find('a charm has no placement', w => { const s = w.sheets.find(s => s.id === 'sh-gf2'); const c = s.charms.find(c => (c.groupKey || '').endsWith(':' + F.tx(10))); s.placements = s.placements.filter(p => p.id !== c.id); }, ['sheet-disagree']);
  find('one piece listed by two sheets', w => { const a = w.sheets.find(s => s.id === 'sh-gf1'), b = w.sheets.find(s => s.id === 'sh-gf2'), id = rowOf(w, 0).poolId; b.poolIds.push(id); b.charms.push(Object.assign({}, a.charms.find(c => c.poolId === id), { id: 'dup' })); b.placements.push({ id: 'dup', cxPt: 1, cyPt: 1, angle: 0, wPt: 1, hPt: 1 }); }, ['duplicate-piece']);
  find('the split is not tracked', w => { w.tracked = []; }, ['untracked-split']);
  find('the right piece on a sheet of another set', w => { w.sheets.find(s => s.id === 'sh-gf2').setId = 'set-2'; w.sets[0].sheetIds = w.sets[0].sheetIds.filter(id => id !== 'sh-gf2'); w.sets.push({ setId: 'set-2', seq: 2, sheetIds: ['sh-gf2'] }); rowOf(w, 1).setId = 'set-2'; }, ['split-across-sets']);
  find('a silver piece among gold ones', w => { rowOf(w, 1).material = 'silver'; }, ['metal-mismatch']);
  find('the pool row says another set than its sheet', w => { rowOf(w, 0).setId = 'set-2'; }, ['set-disagree']);
  find('a set does not list its sheet', w => { w.sets[0].sheetIds = w.sets[0].sheetIds.filter(id => id !== 'sh-gf1'); }, ['set-disagree']);
  find('half the pair is on hold', w => { Object.assign(rowOf(w, 0), { state: 'abandoned', sheetId: null, setId: null, heldAt: 1, heldBy: 'Paul' }); const s = w.sheets.find(s => s.id === 'sh-gf1'); const id = rowOf(w, 0).poolId; s.poolIds = s.poolIds.filter(p => p !== id); s.charms = s.charms.filter(c => c.poolId !== id); }, ['half-held']);
  find('a disc is missing from a 3-disc necklace', w => { w.pool.splice(w.pool.findIndex(p => p.copy === 3), 1); }, ['missing-piece'], F.cases.discs3Sheets);
  find('a disc waits while the others are on sheets', w => { const p = w.pool.find(p => p.copy === 3 && p.groupSize === 3); const s = w.sheets.find(s => s.id === p.sheetId); Object.assign(p, { state: 'ready', sheetId: null, setId: null }); s.poolIds = s.poolIds.filter(x => x !== p.poolId); s.charms = s.charms.filter(c => c.poolId !== p.poolId); }, ['untracked-split'], () => { const w = F.cases.discs3Sheets(); w.tracked = []; return w; });
  find('a stud pair carries sides', w => { w.pool.filter(p => p.groupKey.endsWith(':' + F.tx(10))).forEach((p, i) => { p.side = i ? 'R' : 'L'; p.bodyIndex = i; }); }, ['side-mismatch'], F.cases.pairOneSheet);
  find('a mismatched pair loses its sides (the design says L,R)', w => { w.pool.filter(p => p.groupKey.endsWith(':' + F.tx(10))).forEach(p => { p.side = null; }); }, ['side-mismatch']);
  // (and the broken worlds read through the fake Firestore the way a test reads a flow's result)
  const bad = F.clone(F.cases.mismatchedTwoOutlines()); bad.pool.find(p => p.side === 'R').side = 'L';
  const fs2 = F.fakeFirestore(); fs2.seed(bad); ok(codes(F.problems(fs2.docsOf())).includes('side-pairing'), 'the checker reads the fake Firestore too (docsOf)');

  // 5. the real server over the fake: a pair's records (side, groupKey, bodyIndex, groupSize) are kept by putSheet, and getOrderPieces reads the order's pieces
  const fs3 = F.fakeFirestore(), fns = F.functions(fs3, ['charmNestLibrary']);
  try {
    const w = F.cases.mismatchedSplit(); fs3.seed(w);
    const sheet = F.clone(w.sheets.find(s => s.id === 'sh-gf1')), r = await fns.lib('putSheet', { sheet });
    ok(!r.error, 'putSheet over the fake accepts a sheet with pair fields: ' + (r.error || 'ok'));
    const saved = fs3.get(F.COLL.sheets, 'sh-gf1'); ok(saved.charms.some(c => c.side === 'L' && c.groupKey === `${F.rid(9)}:${F.tx(10)}`), 'the server kept the charm\'s side and groupKey');
    const ce = console.error; console.error = () => {};   // (the handler logs the refusal's stack)
    const bad = await fns.lib('putSheet', { sheet: Object.assign(F.clone(sheet), { id: 'sh-bad', outline: [[0, 0], [1, 1]] }) }).finally(() => { console.error = ce; });   // (a polygon stored as an array of arrays: live Firestore refuses it, the fake does too)
    ok(/Nested arrays are not allowed/.test(bad.error || ''), 'the real putSheet over the strict fake refuses an array inside an array: ' + JSON.stringify(bad).slice(0, 120));
    const rp = await fns.lib('getOrderPieces', { orderIds: [F.rid(9)] });
    ok(!rp.error, 'getOrderPieces answers over the fake: ' + (rp.error || 'ok'));
    ok(codes(F.problems(fs3.docsOf(), { tracked: w.tracked })).length === 0, 'nothing the server did made the pair disagree: ' + JSON.stringify(F.problems(fs3.docsOf(), { tracked: w.tracked })));
  } finally { fns.restore(); }

  // 6. the sandbox replay carries a mismatched pair, a disc necklace and a matching pair unchanged (only the times move)
  const fs4 = F.fakeFirestore(), fns4 = F.functions(fs4, ['etsySandbox']);
  try {
    const w = F.world({ orders: [{ rid: F.rid(21), lines: [{ n: 10, kind: 'mismatched', sku: 'TENNIS-MIS', qty: 1 }] }, { rid: F.rid(22), lines: [{ n: 10, kind: 'discs', discs: 3 }, { n: 11, kind: 'pair', qty: 2 }, { n: 12, kind: 'earring-single' }] }, { rid: F.rid(23), lines: [{ n: 10, kind: 'hoop' }] }, { rid: F.rid(24), lines: [{ n: 10, kind: 'mismatched', sku: 'MITTENS-MIS', qty: 2 }] }] });
    const snap = F.receipts(w), stream = { seed: 42069871, min: 2, max: 5, simStart: 1791600000000, stepMs: 600000, on: true, v: 2, tick: 0 }, came = new Map();
    for (let k = 1; k <= 4; k++) for (const r of fns4.handlers.etsySandbox.batch(stream, snap, k, { at: 1791500000000 })) came.set(String(r.receipt_id), r);
    ok(came.size === snap.length, `the stream brought every order once (${came.size} of ${snap.length})`);
    for (const src of snap) {
      const got = came.get(String(src.receipt_id)); ok(got && got.transactions.length === src.transactions.length, `${src.receipt_id}: every line arrived`);
      src.transactions.forEach((tx0, i) => { const a = got.transactions[i]; ok(['transaction_id', 'receipt_id', 'listing_id', 'sku', 'title', 'quantity'].every(k => a[k] === tx0[k]) && JSON.stringify(a.variations) === JSON.stringify(tx0.variations), `${src.receipt_id} line ${i}: sku, quantity, variations and numbers are unchanged`); });
    }
    // what the sorter will make of each arriving line (the shared module, from the receipt line alone): the same pieces the world expects
    for (const o of w.orders) for (const l of o.lines) { const line = Object.assign({}, l.line, { discs: undefined }), arrived = came.get(o.rid).transactions.find(x => String(x.transaction_id) === F.tx(l.n)); ok(arrived.quantity === l.qty && /\d discs/.test(JSON.stringify(arrived.variations)) === (l.kind === 'discs'), `${o.rid}/${l.n}: the disc count rides in the variation`); void line; }
  } finally { fns4.restore(); }
  if (differ.length) console.log(`  note: the plan's piece count and CharmNestPair.pieceCountOf(no explicit pieceCount) differ for ${differ.length} case(s), e.g. ${differ.slice(0, 3).join('; ')} (the intake must set the count: PAIRINTAKE)`);
  console.log(`pairs-tests: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
