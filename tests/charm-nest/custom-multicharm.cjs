// MULTICHARM (25 Nov 2026, Paul: "Ability to add several charms at once in one .ai file. The system should be able to identify individual charms in a given illustrator file and then add all of them to
// that particular order ... this order has to be treated as an order with multiple pieces"; "the file is the whole order — one piece per charm in it, whatever the order line's quantity — BUT the users
// should still be able to manually change the quantity of EACH charm inside the popup modal").
//   A  the Review card's planner (charm-nest-bridge.js CustomSheet, its MULTICHARM block run as it stands): a 3-charm file is 3 pieces whatever the line's quantity; each charm its own count (2, 0, 1 →
//      3 pieces, charm 2 left out); a 1-charm file keeps the lines' quantity; the pieces spread over the card's lines; an earring-pair card keeps a copy whole on one line; a monogram's file its line
//   B  the cloud keeps each charm's count on the card's record (customSheetPut, a map: never an array inside the files' array)
//   C  the cloud's piece maker (_charmNestCustomMake.js): every pool row says which charm of its file it is (customIndex), each piece cut from that charm
//   D  the cloud's nest rebuild (_charmNestNestPieces.js through charm-nest-pool-make.js ensureBase): two charms of the SAME size are each made again from their own charm, exactly
//   E  downstream: the line counts the pieces it was sent (the cloud world's afterRead pin, CharmNestOrders.pieceCountOf), the order window (charm-nest-order-pieces.js) and Completed say that count;
//      an earring pair and a line with no send are counted as before
// A real PDF (three closed outlines, two of them equal squares) is built with the vendored pdf-lib. Offline only: no Etsy, no live write. Made-up names.   node tests/charm-nest/custom-multicharm.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 25, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console), quiet = !process.env.MC_LOUD; if (quiet) { console.log = () => {}; console.warn = () => {}; }
let failed = 0, passed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1500))); if (cond) passed++; else failed++; };
const RID = '4260000001', TX = '42600000011', KEY = `${RID}_${TX}`, CK = 'custom:' + KEY, PDF = `charmnest/custom/${RID}/abc123def456-THREE.pdf`;

/* ── the file: charm A (a 40 pt square, a hole at its left), charm B (the same square, its hole at its right), charm C (a disc), and a loose stroke outside every outline ── */
async function threeCharms() {
  const { PDFDocument, rgb } = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
  const doc = await PDFDocument.create(), page = doc.addPage([300, 120]);
  const sq = (x, y, s) => page.drawSvgPath(`M 0 0 L ${s} 0 L ${s} ${s} L 0 ${s} Z`, { x, y: y + s, borderColor: rgb(0, 0, 0), borderWidth: 0.25 });
  const circ = (cx, cy, r) => page.drawCircle({ x: cx, y: cy, size: r, borderColor: rgb(0, 0, 0), borderWidth: 0.25 });
  sq(20, 30, 40); circ(30, 50, 3);
  sq(110, 30, 40); circ(140, 50, 3);
  circ(230, 50, 18);
  page.drawSvgPath('M 0 0 L 12 0', { x: 270, y: 110, borderColor: rgb(0, 0, 0), borderWidth: 0.25 });
  return new Uint8Array(await doc.save());
}
const BOX = { A: [20, 30, 60, 70], B: [110, 30, 150, 70], C: [212, 32, 248, 68] };
const near = (b, want, tol = 2) => Array.isArray(b) && b.length === 4 && b.every((v, i) => Math.abs(v - want[i]) <= tol);

/* ── A · the card's planner, the bridge's own code ── */
function planner() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const a = src.indexOf('  /* MULTICHARM (25 Nov 2026, Paul: "Ability to add several charms'), b = src.indexOf('  /* MULTICHARM end */', a);
  if (a < 0 || b < a) throw new Error('the MULTICHARM block of CustomSheet is not in charm-nest-bridge.js');
  const dq = /\n  const defaultQty = it => [^\n]+\n/.exec(src); if (!dq) throw new Error('defaultQty is not in charm-nest-bridge.js');
  const ctx = vm.createContext({ Math, Object, String, Array, JSON });
  vm.runInContext(src.slice(a, b) + dq[0] + '\nthis.X = { multiFile, charmQtyOf, filePieces, startCounts, fileWords, planLines, defaultQty };', Object.assign(ctx, { linesOf: it => it.rows }));
  return ctx.X;
}
function partA(X) {
  const line = (n, q) => ({ key: `${RID}_${TX}${n}`, spec: { quantity: q } }), one = [line(1, 2)];
  const dropped = (id, it) => ({ id, name: id + '.ai', qty: X.defaultQty(it), pieces: 0, state: 'reading' });   // (CustomSheet.add: the count a file starts at, before it is read)
  const read = (F, n) => { F.pieces = n; F.state = 'ready'; return X.startCounts(F, n); };                      // (CustomSheet.ingest: the charms it was read with)
  const card = { rows: one };
  const F3 = read(dropped('three', card), 3);
  ok('A1 a file of 3 charms on a quantity-2 line starts at one of each: 3 pieces, whatever the line\'s quantity', X.defaultQty(card) === 2 && X.multiFile(F3) && JSON.stringify(F3.charmQty) === '{"0":1,"1":1,"2":1}' && X.filePieces(F3) === 3, F3);
  const L1 = X.planLines([F3], one, () => 3, false);
  ok('A2 sent: the line gets ONE piece of each charm, charm by charm', JSON.stringify(L1[one[0].key]) === JSON.stringify([{ f: 'three', i: 0 }, { f: 'three', i: 1 }, { f: 'three', i: 2 }]), L1);
  F3.charmQty = { 0: 2, 1: 0, 2: 1 };
  const L2 = X.planLines([F3], one, () => 3, false), got = L2[one[0].key];
  ok('A3 each charm its own count (2, 0, 1): 3 pieces, charm 2 left out', X.filePieces(F3) === 3 && got.length === 3 && got.filter(pc => pc.i === 0).length === 2 && !got.some(pc => pc.i === 1) && got.filter(pc => pc.i === 2).length === 1, got);
  ok('A4 its words say each charm\'s count', X.fileWords(F3) === 'three.ai: 3 pieces (charm 1 × 2, charm 3 × 1)', X.fileWords(F3));
  const F1 = read(dropped('one', card), 1);
  const L3 = X.planLines([F1], one, () => 1, false);
  ok('A5 a file of ONE charm keeps the lines\' quantity (an order for 2 of one design still makes 2), no per-charm counts', !X.multiFile(F1) && !F1.charmQty && F1.qty === 2 && X.filePieces(F1) === 2 && L3[one[0].key].length === 2, { F1, L3 });
  const three = [line(1, 1), line(2, 1), line(3, 1)], F3b = read(dropped('set', { rows: three }), 3);
  const L4 = X.planLines([F3b], three, () => 3, false);
  ok('A6 three lines: one charm on each line, in turn', three.every((r, i) => JSON.stringify(L4[r.key]) === JSON.stringify([{ f: 'set', i }])), L4);
  const two = [line(1, 1), line(2, 1)], F3c = read(dropped('two', { rows: two }), 3); F3c.charmQty = { 0: 2, 1: 0, 2: 1 };
  const L5 = X.planLines([F3c], two, () => 3, false);
  ok('A7 two lines, counts 2, 0, 1: the pieces in turn (line 1: charm 1 twice, line 2: charm 3)', JSON.stringify(L5[two[0].key].map(pc => pc.i)) === '[0,0]' && JSON.stringify(L5[two[1].key].map(pc => pc.i)) === '[2]', L5);
  const F1b = read(dropped('solo', { rows: two }), 1);
  const L6 = X.planLines([F1b], two, () => 1, false);
  ok('A8 a file of one charm over two lines: one copy on each, as before', F1b.qty === 2 && L6[two[0].key].length === 1 && L6[two[1].key].length === 1, L6);
  const ears = read({ id: 'ears', name: 'ears.ai', qty: 1, pieces: 0 }, 2); ears.charmQty = { 0: 2, 1: 2 };
  const L7 = X.planLines([ears], two, () => 2, true);
  ok('A9 an earring-pair card: each copy (its Left and its Right) stays whole on one line', JSON.stringify(L7[two[0].key].map(pc => pc.i)) === '[0,1]' && JSON.stringify(L7[two[1].key].map(pc => pc.i)) === '[0,1]', L7);
  const mono = read({ id: 'mono', name: 'Monogram ABC.ai', qty: 1, pieces: 0, mono: { line: two[1].key } }, 2);
  const L8 = X.planLines([mono], two, () => 2, false);
  ok('A10 a monogram\'s design: no per-charm counts, all on its own line, as before', !mono.charmQty && !X.multiFile(mono) && L8[two[0].key].length === 0 && JSON.stringify(L8[two[1].key].map(pc => pc.i)) === '[0,1]', L8);
  const old = { id: 'old', name: 'old.ai', qty: 2, pieces: 3, state: 'ready' };
  const L9 = X.planLines([old], one, () => 3, false);
  ok('A11 a file read before MULTICHARM (no per-charm counts) is sent as it always was: every charm in each copy', !X.multiFile(old) && X.filePieces(old) === 6 && JSON.stringify(L9[one[0].key].map(pc => pc.i)) === '[0,1,2,0,1,2]', L9);
  const zero = read(dropped('zero', card), 3); zero.charmQty = { 0: 0, 1: 0, 2: 0 };
  ok('A12 every charm at 0: the file makes nothing (the card says so before any send: notReady)', X.filePieces(zero) === 0 && X.planLines([zero], one, () => 3, false)[one[0].key].length === 0);
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  ok('A13 the window: each charm its own count control (data-ci), the send uses planLines, the cloud record keeps charmQty, a pool row its customIndex', /data-q="-1" data-ci="\$\{i\}"/.test(src) && /const lines=planLines\(files,rows,/.test(src) && /charmQty:Object\.assign\(\{\},F\.charmQty\)/.test(src) && /customFile: F\.name, customIndex: pc\.i/.test(src) && /setQty\(D\.ck, id, \+q\.dataset\.q, q\.dataset\.ci != null \? \+q\.dataset\.ci : null\)/.test(src));
  ok('A14 ink outside every cut outline is said in the window (F.loose), the held charm reads its customIndex', /outside every cut outline: not cut/.test(src) && /loose: loose \+ \(g\.charms\.length - charms\.length\)/.test(src) && /ci >= 0 && ci < charms\.length \? charms\[ci\]/.test(src));
}

/* ── B · the card's record in the cloud keeps each charm's count ── */
async function partB() {
  const lib = F.load('charmNestLibrary.js');
  const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
  const file = { id: 'f1', name: 'THREE.ai', kind: 'ai', size: 1000, hash: 'abc123def456', cloud: { path: PDF, url: '' }, metal: 'gold', qty: 1, pieces: 3, wMm: 80, hMm: 14, maxPt: 40, minPt: 36, maxAreaPt2: 1600, state: 'ready', charmQty: { 0: 2, 1: 0, 2: 1 } };
  const rec = { ck: CK, rid: RID, at: T0 - 5000, phase: 'sent', files: [file], sent: { id: `custom-sheet:${CK}:${T0 - 5000}`, at: T0 - 5000, by: 'Robin', lines: { [KEY]: [{ f: 'f1', i: 0 }, { f: 'f1', i: 2 }, { f: 'f1', i: 0 }] } } };
  const put = await P({ op: 'customSheetPut', record: rec });
  const got = await P({ op: 'customSheetGet', keys: [CK] }), kept = got.records && got.records[CK], kf = kept && kept.files && kept.files[0];
  ok('B1 the card\'s record keeps each charm\'s count, as a map', put.http === 200 && put.ok && kf && JSON.stringify(kf.charmQty) === '{"0":2,"1":0,"2":1}' && !Array.isArray(kf.charmQty), { put, kf });
  const again = await P({ op: 'customSheetPut', record: rec });
  ok('B2 the same send recorded again is the same decision (no conflict)', again.http === 200 && again.ok, again);
  const bad = JSON.parse(JSON.stringify(rec)); bad.ck = CK + 'x'; bad.sent.id = `custom-sheet:${bad.ck}:${T0 - 5000}`; bad.sent.lines = { [`${RID}_42600000099`]: [{ f: 'f1', i: 0 }] }; bad.files[0].charmQty = { 0: 'lots', 7: 3, 1: 120, 2: 1 };
  const b2 = await P({ op: 'customSheetPut', record: bad }), k2 = ((await P({ op: 'customSheetGet', keys: [bad.ck] })).records || {})[bad.ck];
  ok('B3 a count that is not 0..99 for a charm of the file is left out, the rest kept', b2.http === 200 && k2 && JSON.stringify(k2.files[0].charmQty) === '{"2":1}', { b2, k2: k2 && k2.files[0].charmQty });
  return kept;
}

/* ── C · the cloud's piece maker: each pool row names its charm ── */
async function partC(bytes, kept) {
  global.self = global;
  const Pdf = require(path.join(ROOT, 'netlify/functions/_charmNestPdf.js')).withCanvas().CharmNestPDF;
  const Make = require(path.join(ROOT, 'netlify/functions/_charmNestCustomMake.js'));
  const M = Make.create({ sends: new Map([[KEY, kept]]), pdf: () => Pdf, bytes: async p => { if (p !== PDF) throw new Error('not the file: ' + p); return bytes; }, settings: { minPt: 6, silhouetteRes: 6 } });
  const row = { key: KEY, order: { receiptId: RID, createTs: 1795000000, updateTs: 1795000001 }, line: { transactionId: TX, sku: 'CUSTOM_ORDER' }, spec: { quantity: 1, form: 'charm' }, arrivedAt: T0 - 86400e3 };
  const prep = await M.prepare(row, { runId: 'run-mc', setId: null });
  const pools = (prep && prep.pools) || [], charms = (prep && prep.charms) || [];
  ok('C1 the line\'s 3 pieces are made (2 of charm 1, 1 of charm 3), copies 1..3 of the line', pools.length === 3 && JSON.stringify(pools.map(p => p.poolId)) === JSON.stringify([1, 2, 3].map(n => `${KEY}_${n}`)) && pools.every(p => p.quantity === 3 && p.custom === true && p.aiPath === PDF), pools.map(p => [p.poolId, p.quantity]));
  ok('C2 every pool row says which charm of its file it is (customIndex)', JSON.stringify(pools.map(p => p.customIndex)) === '[0,2,0]' && JSON.stringify(charms.map(c => c.customIndex)) === '[0,2,0]', pools.map(p => p.customIndex));
  ok('C3 each piece is cut from its own charm (charm 1 the left square, charm 3 the disc)', near(charms[0] && charms[0].bbox, BOX.A) && near(charms[1] && charms[1].bbox, BOX.C) && near(charms[2] && charms[2].bbox, BOX.A), charms.map(c => c.bbox));
  return { pools, charms };
}

/* ── D · the cloud's nest makes each piece again from ITS charm, two equal squares told apart ── */
async function partD(bytes) {
  const Pieces = require(path.join(ROOT, 'netlify/functions/_charmNestNestPieces.js'));
  const bucket = { file: p => ({ download: async () => { if (p !== PDF) throw new Error('no such file ' + p); return [Buffer.from(bytes)]; } }) };
  const S = { settings: { minPt: 6, silhouetteRes: 6, insetPt: 0, maxFill: 0.8 } };
  const rcOf = (n, extra) => Object.assign({ id: `cust:abc123def456:${KEY}_${n}`, poolId: `${KEY}_${n}`, sourceId: 'cust:abc123def456', sourceName: 'THREE.ai (custom · ' + RID + ')', custom: true, name: `${RID} · Custom · THREE · ${n}/2`, sku: 'CUSTOM', widthPt: 40, heightPt: 40 }, extra || {});
  const poolOf = (n, ci) => Object.assign({ poolId: `${KEY}_${n}`, orderId: RID, transactionId: TX, lineKey: KEY, sku: 'CUSTOM', material: 'gold', copy: n, quantity: 2, custom: true, customFile: 'THREE.ai', aiPath: PDF, state: 'nested', widthPt: 40, heightPt: 40, areaPt2: 1600 }, ci == null ? {} : { customIndex: ci });
  const rec = { id: 'gold-mc', metal: 'gold', sources: [{ id: 'cust:abc123def456', name: 'THREE.ai (custom · ' + RID + ')', path: PDF, custom: true, pool: true }], charms: [rcOf(1), rcOf(2)] };
  const make = () => Pieces.create({ db: F.db, bucket, S, log: () => {} });
  const now = await make().rebuild(rec, new Map([[`${KEY}_1`, poolOf(1, 0)], [`${KEY}_2`, poolOf(2, 1)]]));
  const [a, b] = now.charms;
  ok('D1 two charms of the same size: piece 1 is made again from charm 1 (its hole on the left), piece 2 from charm 2 (its hole on the right)', now.missing.length === 0 && now.charms.length === 2 && near(a && a.bbox, BOX.A) && near(b && b.bbox, BOX.B) && a.poolId === `${KEY}_1` && b.poolId === `${KEY}_2`, { missing: now.missing, boxes: now.charms.map(c => c.bbox) });
  ok('D2 each made piece has its own silhouette, of its own charm only (one charm\'s size, never the whole file\'s)', [a, b].every(c => c && c.bits && c.w > 0 && c.h > 0 && Math.abs(c.widthPt - 40) < 4 && Math.abs(c.heightPt - 40) < 4), [a, b].map(c => c && [c.widthPt, c.heightPt, c.w, c.h]));
  const withHash = await make().rebuild(Object.assign({}, rec, { charms: [rcOf(1, { hash: 'page-hash-1' }), rcOf(2, { hash: 'page-hash-2' })] }), new Map([[`${KEY}_1`, poolOf(1, 0)], [`${KEY}_2`, poolOf(2, 1)]]));
  ok('D3 a sheet record whose pieces keep a page\'s hash: the charm is still read from the pool row\'s customIndex', withHash.missing.length === 0 && near(withHash.charms[0] && withHash.charms[0].bbox, BOX.A) && near(withHash.charms[1] && withHash.charms[1].bbox, BOX.B), { missing: withHash.missing, boxes: withHash.charms.map(c => c.bbox) });
  const old = await make().rebuild(rec, new Map([[`${KEY}_1`, poolOf(1, null)], [`${KEY}_2`, poolOf(2, null)]]));
  // (what the old reading gives: the file's master reading folds every charm into the largest, so each old piece is the whole file; a piece made from now on names its charm, D1)
  ok('D4 a piece made before MULTICHARM (no customIndex) is read exactly as before (the file\'s master reading: the whole file)', old.missing.length === 0 && old.charms.length === 2 && old.charms.every(c => near(c.bbox, [BOX.A[0], BOX.A[1], BOX.C[2], BOX.A[3]])), { missing: old.missing, boxes: old.charms.map(c => c.bbox) });
  const bad = await make().rebuild(Object.assign({}, rec, { charms: [rcOf(1)] }), new Map([[`${KEY}_1`, poolOf(1, 7)]]));
  ok('D5 a piece naming a charm its file does not have is refused in words, never cut from another charm', bad.charms.length === 0 && bad.missing.length === 1 && /no charm 8/.test(bad.missing[0].why), bad);
  // the piece maker itself (the page's Pool and every cloud host share it): only a custom order's own upload is cut by a piece's index
  const PoolMake = require(path.join(ROOT, 'charm-nest-pool-make.js')), Pdf = require(path.join(ROOT, 'netlify/functions/_charmNestPdf.js')).withCanvas().CharmNestPDF;
  const host = () => PoolMake.create({ window: { CharmNestPair: require(path.join(ROOT, 'charm-nest-pair.js')) }, S: { settings: S.settings, poolSources: {} }, B: { pool: { rows: new Map(), sources: new Map() }, master: { entries: new Map() } },
    O: require(path.join(ROOT, 'charm-nest-orders.js')), P: Pdf, MM: 25.4 / 72, Master: { keepOutOf: () => null, entryFor: () => null, skuRegex: () => /^$/ }, PoolPieces: require(path.join(ROOT, 'charm-nest-pool-pieces.js')),
    agent: () => {}, api: async () => { throw new Error('no call'); }, stockFor: () => null, labelOf: m => m, now: () => Date.now(), CharmNestAssets: () => ({ bytes: async () => bytes }),
    Orders: () => null, Review: () => null, CustomPrint: () => null, CustomSheet: () => null, ArtEdit: () => null, keptMaster: async () => null, keepMaster: () => {}, pinPooled: () => {}, charmOf: () => null, greenHeld: () => false, cancelled: () => false, cloud: true });
  const mk1 = host(), lib = await mk1.masterCharm({ sku: 'THREE', aiPath: 'charmnest/master/THREE.ai', aiUrl: 'bucket:charmnest/master/THREE.ai' }, null), libBase = await mk1.ensureBase(lib, null, { poolId: `${KEY}_9`, customIndex: 1 });
  const mk2 = host(), own = await mk2.masterCharm({ sku: 'CUSTOM', aiPath: PDF, aiUrl: 'bucket:' + PDF }, null), ownBase = await mk2.ensureBase(own, null, { poolId: `${KEY}_9`, customIndex: 1 });
  ok('D6 a library master is never cut by a piece\'s index (a stale record): it is read as it always was; the custom upload gives charm 2', libBase === lib.charms[0] && near(ownBase && ownBase.bbox, BOX.B), { lib: libBase && libBase.bbox, own: ownBase && ownBase.bbox });
}

/* ── E · downstream: the line is the pieces it was sent ── */
async function partE() {
  const W = require(path.join(ROOT, 'netlify/functions/_charmNestWorld.js'));
  const O = require(path.join(ROOT, 'charm-nest-orders.js'));
  const pair = earring => ({ earring, mismatched: false, glued: false, perUnit: earring ? 2 : 1, kind: earring ? 'pair' : 'single', sides: earring ? ['L', 'R'] : [null] });
  const rowOf = (q, earring) => ({ key: KEY, state: 'held', hold: null, poolIds: [], order: { receiptId: RID }, line: { transactionId: TX, quantity: q }, spec: { quantity: q, pieceCount: earring ? 2 * q : q, form: earring ? 'stud' : 'charm', pair: pair(earring), problems: [] } });
  const r1 = W.afterRead(rowOf(1, false), null, { metal: 'gold', pieces: 3, sent: true });
  const Pair = require(path.join(ROOT, 'charm-nest-pair.js')), asLine = r => ({ receiptId: RID, transactionId: TX, quantity: r.spec.quantity, form: r.spec.form, spec: r.spec });
  ok('E1 a quantity-1 line sent 3 pieces counts 3 (spec.pieceCount, CharmNestOrders.pieceCountOf), pinned as a pooled line is (not made a group), with no "pooled as" note, ready to pool',
    r1.spec.pieceCount === 3 && r1.spec.customPieces === 3 && O.pieceCountOf(r1) === 3 && Pair.pieceCountOf(asLine(r1)) === 3 && !r1.spec.pieceNote && r1.spec.pair.legacy === true && !Pair.isGroupLine(asLine(r1)) && r1.state === 'pulled', r1.spec);
  const again = W.afterRead(r1, null, { metal: 'gold', pieces: 3, sent: true });
  ok('E1b read again it stays as it is', again.spec.pieceCount === 3 && !again.spec.pieceNote && again.spec.pieceRule === 1, again.spec);
  const r2 = W.afterRead(rowOf(3, false), null, { metal: 'gold', pieces: 2, sent: true });
  ok('E2 a quantity-3 line sent 2 pieces counts 2: the file is the whole order', r2.spec.pieceCount === 2 && O.pieceCountOf(r2) === 2 && !r2.spec.pieceNote, r2.spec);
  const r3 = W.afterRead(rowOf(1, true), null, { metal: 'gold', pieces: 2, sent: true });
  ok('E3 an earring pair keeps its own Left + Right count', r3.spec.pieceCount === 2 && r3.spec.customPieces === undefined && r3.spec.pair.kind === 'pair', r3.spec);
  const r4 = W.afterRead(rowOf(2, false), null, null);
  ok('E4 a line with no send is counted as before', r4.spec.pieceCount === 2 && r4.spec.customPieces === undefined, r4.spec);
  // the order window: OrderPieces.resolve over the line as lineOfRow gives it
  const OP = require(path.join(ROOT, 'charm-nest-order-pieces.js'));
  const lineOf = (r, poolIds) => ({ key: r.key, receiptId: RID, transactionId: TX, sku: 'CUSTOM', quantity: r.spec.quantity, state: r.state, poolIds: poolIds || [], spec: { noDesign: false, customDone: null, pieceCount: r.spec.pieceCount, pair: r.spec.pair, ...(r.spec.customPieces > 0 ? { customPieces: r.spec.customPieces } : {}) } });
  const countedOf = l => { const c = O.countedPieces(l); return c ? c.total : 0; };
  const w1 = OP.resolve({ orderId: RID, lines: [lineOf(r1)], pools: [], sheets: [], countOf: countedOf });
  const w2 = OP.resolve({ orderId: RID, lines: [lineOf(r2)], pools: [], sheets: [], countOf: countedOf });
  const w4 = OP.resolve({ orderId: RID, lines: [lineOf(r4)], pools: [], sheets: [], countOf: countedOf });
  ok('E5 the order window: 3 pieces for the 3 sent before they are made, 2 for the 2 sent on a quantity-3 line (no piece that will never exist), a line with no send its quantity', w1.length === 3 && w2.length === 2 && w4.length === 2, [w1.length, w2.length, w4.length]);
  const w5 = OP.resolve({ orderId: RID, lines: [lineOf(r1, [1, 2, 3].map(n => `${KEY}_${n}`))], pools: [1, 2, 3].map(n => ({ poolId: `${KEY}_${n}`, orderId: RID, lineKey: KEY, copy: n, quantity: 3, state: 'nested', sheetId: 'gold-mc', custom: true, customIndex: n - 1 })), sheets: [{ id: 'gold-mc', poolIds: [1, 2, 3].map(n => `${KEY}_${n}`) }], sheetsKnown: true, countOf: countedOf });
  ok('E6 once made, the window counts the line\'s pieces as its pool rows: 3 of 3', w5.length === 3 && w5.every(p => p.lineKey === KEY), w5.map(p => [p.id, p.qty]));
  const op = fs.readFileSync(path.join(ROOT, 'charm-nest-order-pieces.js'), 'utf8');
  ok('E7 the page\'s and the cloud\'s lineOfRow carry customPieces to the order window', /\.\.\.\(sp\.customPieces > 0 \? \{ customPieces: sp\.customPieces \} : \{\}\)/.test(op) && /\.\.\.\(sp\.customPieces > 0 \? \{ customPieces: sp\.customPieces \} : \{\}\)/.test(fs.readFileSync(path.join(ROOT, 'netlify/functions/_charmNestWorld.js'), 'utf8')));
  // Completed: the list's pieces
  const store = new Map(), win = { localStorage: { getItem: k => store.get(k) || null, setItem: (k, v) => store.set(k, String(v)) }, document: { dispatchEvent() {}, addEventListener() {}, createElement: () => ({}) }, CustomEvent: function (t, d) { this.type = t; this.detail = d && d.detail; }, console: { warn() {}, log() {} }, Intl, Date, Math, JSON, setTimeout: () => 0, clearTimeout() {} };
  win.self = win;
  vm.runInContext(fs.readFileSync(path.join(ROOT, 'charm-nest-completed.js'), 'utf8'), vm.createContext(win));
  const C = win.CNCompleted, fact = { type: 'shipped', at: T0, by: 'Pat', src: 'timeline', ev: { type: 'shipped', at: T0, by: 'Pat', source: 'station' } };
  C.learn(RID, fact, [Object.assign(r1, { order: { receiptId: RID, buyer: { name: 'A. Buyer' } } })]);
  C.learn('4260000002', fact, [Object.assign(rowOf(2, false), { key: '4260000002_1', order: { receiptId: '4260000002', buyer: { name: 'B. Buyer' } } })]);
  const s1 = (C.of(RID) || {}).snap, s2 = (C.of('4260000002') || {}).snap;
  ok('E8 Completed says the pieces the line was sent (3), a line with no send its quantity (2)', s1 && s1.lines[0].qty === 3 && s2 && s2.lines[0].qty === 2, { s1, s2 });
}

(async () => {
  try {
    const X = planner(); partA(X);
    const bytes = await threeCharms();
    await F.bucket.file(PDF).save(Buffer.from(bytes), { contentType: 'application/pdf' });
    const kept = await partB();
    if (kept) await partC(bytes, kept); else ok('C run', false, 'no record');
    await partD(bytes);
    await partE();
  } catch (e) { ok('run', false, String(e && e.stack || e).split('\n').slice(0, 6).join(' | ')); }
  log0(failed ? `FAIL custom-multicharm (${failed} of ${passed + failed})` : `PASS custom-multicharm (${passed} checks)`);
  process.exit(failed ? 1 : 0);
})();
