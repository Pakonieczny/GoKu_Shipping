// PAIRLABELS (pairs-1009, area 11): what labels, set files, laser export, the sticker and the seals say about a pair.
// Offline, no browser, no network, nothing paid. Every record this test builds goes through tests/charm-nest/_noNestedArrays.cjs.
//   node tests/charm-nest/pairs-labels.cjs
// Proves, with the real code (the real Sets.onSheetSaved / labelsReady cut out of charm-nest-bridge.js, the real SetEdit.relabel / remakeFiles,
// the real CharmNestPDF.buildSheet and CharmNestExport.compose, the real QR Printer.html page functions, the real CharmNestLibrary customPut):
//  1  helper words: a copy with a side is "SKU Left", a copy without one is worded exactly as before; a split group is flagged only when it is split
//  2  a sheet QR label carries a note line for a group split over two sheets (a mismatched pair: "Left here, Right on Sheet 2"; a matching pair and a
//     necklace's discs: "1 of 2 here, 1 on Sheet 2"), none for any other sheet: the label record, its PNG call and labelsReady are as before
//  3  a label is out of date when its split note is (the sibling piece moved): labelsReady false, the repair path (onSheetSaved) makes it true again
//  4  set copies carry side and bodyIndex for a mismatched pair and nothing else
//  5  the manifest line of the real finalize/remakeFiles: "MITTENS-MIS Left->..." for a mismatched pair, "[split over 2 sheets]" for a split group,
//     the old text for every other order; set.json keeps the side
//  6  an old record (no side, no notes) is worded as today everywhere
//  7  the sheet's layer names: " Left" / " Right" for a piece with a side (the .ai and the DXF), unchanged for every other charm, inside 60 characters
//  8  the back layer and the back list of the export say the ear; the zip's IMPORT.txt says so only when a sheet has such a piece
//  9  the sticker: a mismatched pair line makes a LEFT and a RIGHT page of the same order (QR Printer.html's real functions), a matching pair or a
//     single charm makes the one page it always made; the pieces behind a card for the efficiency record
// 10  Complete Order / print records the pair as one group (customPut over the real function): one seal for the pair, "Left + Right" on its event
// 11  laser acts: the pieces of a pair are counted per sheet (both on one sheet: 2; split: 1 and 1), never as an order completion
// 12  the welded seal of a pair of studs (the Welding page's own rule): one event for the pair line, never one per ear
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const F = require('./pairs-fixtures.cjs');
const PL = require('../../charm-nest-pair-labels.js');
const Pair = require('../../charm-nest-pair.js');
const O = require('../../charm-nest-orders.js');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const ok = m => console.log('  ✓ ' + m);
const noNest = (v, w) => refuseNestedArrays(JSON.parse(JSON.stringify(v)), w);

(async () => {
  /* ── 1 · the words ── */
  assert.equal(PL.sideOf({ side: 'L' }), 'L'); assert.equal(PL.sideOf({ side: 'x' }), null); assert.equal(PL.sideOf({}), null); assert.equal(PL.sideOf(null), null);
  assert.equal(PL.wordOf({ side: 'R' }), 'Right'); assert.equal(PL.wordOf('L'), 'Left'); assert.equal(PL.wordOf({}), '');
  assert.equal(PL.copyWord('MITTENS-MIS', 1, 2, { side: 'L' }), 'MITTENS-MIS Left');
  assert.equal(PL.copyWord('PAIR-STUD', 2, 2, {}), 'PAIR-STUD#2', 'a matching pair copy is worded as before');
  assert.equal(PL.copyWord('ONE', 1, 1, {}), 'ONE', 'a lone copy too');
  const old = lines => Object.values(lines).flatMap(l => l.copies.map(c => `${l.sku}${l.copies.length > 1 ? '#' + c.copy : ''}->${c.sheet}`));   // the text the two manifest makers wrote before
  const plain = { a: { transactionId: 'a', sku: 'PAIR-STUD', copies: [{ copy: 1, sheetId: 's1', sheet: 'GF Sheet 1' }, { copy: 2, sheetId: 's1', sheet: 'GF Sheet 1' }] }, b: { transactionId: 'b', sku: 'ONE', copies: [{ copy: 1, sheetId: 's2', sheet: 'GF Sheet 2' }] } };
  assert.deepEqual(PL.manifestEntries(plain), old(plain), 'unsplit lines: the old manifest words, byte for byte');
  const mis = [{ transactionId: 'm', sku: 'MITTENS-MIS', copies: [{ copy: 1, side: 'L', sheetId: 's1', sheet: 'GF Sheet 1' }, { copy: 2, side: 'R', sheetId: 's2', sheet: 'GF Sheet 2' }] }];
  assert.deepEqual(PL.manifestEntries(mis), ['MITTENS-MIS Left->GF Sheet 1', 'MITTENS-MIS Right->GF Sheet 2', '[split over 2 sheets]']);
  const split = [{ transactionId: 'p', sku: 'PAIR-STUD', copies: [{ copy: 1, sheetId: 's1', sheet: 'GF Sheet 1' }, { copy: 2, sheetId: 's2', sheet: 'GF Sheet 2' }] }];
  assert.deepEqual(PL.manifestEntries(split), ['PAIR-STUD#1->GF Sheet 1', 'PAIR-STUD#2->GF Sheet 2', '[split over 2 sheets]'], 'a matching pair split over sheets is said too (R3)');
  assert.deepEqual(PL.manifestEntries(split, c => 'X' + c.copy).slice(0, 2), ['PAIR-STUD#1->X1', 'PAIR-STUD#2->X2']);
  // split notes
  const P = (rid, tid, copy, sheetId, sheet, side) => ({ rid, tid, sku: 'S', copy, poolId: `${rid}_${tid}_${copy}`, sheetId, sheet, side: side || null });
  assert.deepEqual(PL.splitNotes('s1', [P('1', 't', 1, 's1', 'GF_x_Set-1_Sheet-1', 'L'), P('1', 't', 2, 's2', 'GF_x_Set-1_Sheet-2', 'R')]), ['1 Left here, Right on Sheet 2']);
  assert.deepEqual(PL.splitNotes('s2', [P('1', 't', 1, 's1', 'GF_x_Set-1_Sheet-1', 'L'), P('1', 't', 2, 's2', 'GF_x_Set-1_Sheet-2', 'R')]), ['1 Right here, Left on Sheet 1']);
  assert.deepEqual(PL.splitNotes('s1', [P('1', 't', 1, 's1', 'A_Sheet-1'), P('1', 't', 2, 's2', 'A_Sheet-2')]), ['1 1 of 2 here, 1 on Sheet 2'], 'matching pair');
  const discs = [P('9', 'd', 1, 's1', 'A_Sheet-1'), P('9', 'd', 2, 's2', 'A_Sheet-2'), P('9', 'd', 3, 's3', 'A_Sheet-3')];
  assert.deepEqual(PL.splitNotes('s2', discs), ['9 1 of 3 here, 1 on Sheet 1, 1 on Sheet 3'], 'discs on three sheets');
  assert.deepEqual(PL.splitNotes('s1', [P('1', 't', 1, 's1', 'A_Sheet-1'), P('1', 't', 2, 's1', 'A_Sheet-1')]), [], 'a group wholly on this sheet says nothing');
  assert.deepEqual(PL.splitNotes('s3', discs.slice(0, 2)), [], 'a sheet that holds none of the group says nothing');
  assert.deepEqual(PL.splitNotes('s1', [P('1', 't', 1, 's1', 'A_Sheet-1', 'L'), P('1', 't', 2, 's2', 'A_Sheet-2', 'R')], ['7']), [], 'only: the orders of one label part');
  const many = Array.from({ length: 6 }, (_, i) => [P(String(100 + i), 't', 1, 's1', 'A_Sheet-1'), P(String(100 + i), 't', 2, 's2', 'A_Sheet-2')]).flat();
  const nl = PL.splitNotes('s1', many); assert.equal(nl.length, PL.MAX_NOTES); assert.match(nl[nl.length - 1], /^\+3 more split orders$/);
  // reconcile: a live sheet is the truth about itself
  const base = [P('1', 't', 1, 's1', 'A_Sheet-1', 'L'), P('1', 't', 2, 's2', 'A_Sheet-2', 'R')];
  assert.deepEqual(PL.splitNotes('s1', PL.reconcile(base, [{ sheetId: 's2', sheet: 'A_Sheet-2', pieces: [] }, { sheetId: 's1', sheet: 'A_Sheet-1', pieces: [P('1', 't', 1, 'x', 'x', 'L'), P('1', 't', 2, 'x', 'x', 'R')] }])), [], 'the right ear moved onto sheet 1: no split any more');
  // a charm with no order info is read from its pool id
  assert.deepEqual(PL.piecesOfCharms([{ poolId: '4190000009_5000000010_2', order: '4190000009', side: 'R' }, { id: 'no pool id' }]), [{ rid: '4190000009', tid: '5000000010', sku: '', copy: 2, poolId: '4190000009_5000000010_2', side: 'R' }]);
  ok('helper words: sides, manifest entries (old text kept), split notes, reconcile');

  /* ── 2-4 · the real Sets code over the fixture's mismatched pair split over two sheets of one set ── */
  const bridge = read('charm-nest-bridge.js');
  const section = (name, end) => { const a = bridge.indexOf(`const ${name} = window.${name} =`); return bridge.slice(a, bridge.indexOf(end, a)); };
  let setsSrc = section('Sets', '/* ═══ 23');
  { const a = setsSrc.indexOf('  async function renderLabelPng('), b = setsSrc.indexOf('  /** After a sheet', a); assert(a > 0 && b > a); setsSrc = setsSrc.slice(0, a) + '  async function renderLabelPng(payload,label,scale,notes){pngCalls.push({payload,label,notes});return {blob:new Uint8Array([1]),dataUrl:"data:image/png;base64,AA==",ecc:"M"};}\n' + setsSrc.slice(b); }
  const prelude = `
const O=CharmNestOrders,windowRef=window;
const S={cloud:{ok:true},mode:'library',settings:{},library:{kind:'sets'}};
const B={run:{runId:'run-pairs',status:'stopped',sheets:{}},sets:new Map(),pool:{rows:new Map()}};
const pngCalls=[],puts=[],setPuts=[];
const METAL_TAG={gold:'GF',silver:'SS',rose:'RG',gold10k:'10K',gold14k:'14K'},labelOf=m=>m,today=()=> '2026-10-09',agent=()=>{},toast=()=>{},employeeName=()=> 'Test';
const esc=x=>String(x??''),document={activeElement:null};
let pages=[];const allSheets=()=>pages,Orders={rows:()=>[],render(){}},Pool={update:async()=>{}},Engrave={items:()=>new Map()},Session={schedule(){}},RunCtl={renderBanner(){}};
const stampWho=()=>({by:'Test'});
const api=async(name,b)=>{ if(b.op==='putSheet')puts.push(structuredClone(b.sheet)); if(b.op==='setUpdate')setPuts.push(structuredClone(b.patch)); return {}; };
const uploadBytes=async(p)=>({path:p,url:'https://example.com/'+p});
const Gate={solidSelected:()=>false,modern:()=>true};
${setsSrc}
`;
  const ctx = vm.createContext({ assert, structuredClone, Uint8Array, console, setTimeout, window: {} });
  ctx.CharmNestOrders = O; ctx.window = { CharmNestOrders: O, CharmNestPairLabels: PL, CharmNestPair: Pair };
  vm.runInContext(prelude, ctx);
  const world = F.cases.mismatchedSplit();   // order 4190000009: left on sh-gf1, right on sh-gf2, both in set-1; fillers on both
  const page = (rec, extra) => ({ sheetId: rec.id, metal: 'gold', page: rec.sheetIndex, sheetIndex: rec.sheetIndex, fileBase: rec.fileBase, runId: 'run-pairs', folderPath: 'charmnest/sets/' + rec.fileBase, draft: false, setId: 'set-1', seq: 1,
    placements: rec.placements.map(p => ({ id: p.id })), charms: rec.charms.map(c => Object.assign({}, c, { orderInfo: { receiptId: c.order, transactionId: c.groupKey.split(':')[1], sku: c.side ? 'MITTENS-MIS' : 'FILLER', copy: +c.poolId.split('_')[2] } })), outputs: {}, persistedDone: true, ...extra });
  const A = page(world.sheets.find(s => s.id === 'sh-gf1')), Bp = page(world.sheets.find(s => s.id === 'sh-gf2'));
  ctx.__pages = [A, Bp]; vm.runInContext('pages = __pages;', ctx);
  const set = { setId: 'set-1', seq: 1, name: 'Set-1', day: '2026-10-09', runId: 'run-pairs', folder: 'charmnest/sets/Set-1', orders: {}, sheetIds: [], materials: [], labelFiles: [], status: 'nesting' };
  ctx.__set = set; ctx.__A = A; ctx.__B = Bp;
  await vm.runInContext('(async()=>{await Sets.onSheetSaved(__A,__A.charms,undefined,{setOverride:__set,labelsOnly:true});await Sets.onSheetSaved(__B,__B.charms,undefined,{setOverride:__set,labelsOnly:true});})()', ctx);
  const callOf = sheet => pngCalls().filter(c => c.label.includes('Sheet ' + sheet)).pop();
  function pngCalls() { return vm.runInContext('pngCalls', ctx); }
  // sheet 1 holds the left ear: its label says where the right one is; sheet 2 the other way round; both labels' payload is still the order list
  assert.deepEqual(callOf(1).notes, ['4190000009 Left here, Right on Sheet 2'], 'sheet 1 label note: ' + JSON.stringify(callOf(1)));
  assert.deepEqual(callOf(2).notes, ['4190000009 Right here, Left on Sheet 1']);
  assert.equal(callOf(1).payload, O.encodeOrderList(['4190000009', '4190000090'], 'gold'), 'the QR payload stays the order list');
  assert.deepEqual(A.label.files[0].notes, ['4190000009 Left here, Right on Sheet 2'], 'the note rides on the label file record');
  assert.deepEqual(Bp.label.files[0].notes, ['4190000009 Right here, Left on Sheet 1']);
  assert(set.labelFiles.every(f => f.notes), 'and on the set record\'s label list');
  assert(vm.runInContext('Sets.labelsReady(__A,__set)', ctx) && vm.runInContext('Sets.labelsReady(__B,__set)', ctx), 'both labels are current');
  // 4 · copies carry the side for the mismatched pair only
  const copies = Object.values(set.orders['4190000009'].lines)[0].copies;
  assert.deepEqual(copies.map(c => [c.copy, c.side, c.bodyIndex]), [[1, 'L', 0], [2, 'R', 1]], 'copies: side and body index');
  const filler = Object.values(set.orders['4190000090'].lines)[0].copies[0]; assert(!('side' in filler) && !('bodyIndex' in filler), 'a normal copy gets no new field: ' + JSON.stringify(filler));
  noNest(set, 'set'); for (const p of puts) noNest(p, 'sheet put'); for (const p of vm.runInContext('setPuts', ctx)) noNest(p, 'set put');
  // the filler orders: no note
  assert(!('notes' in A.label.files[0]) === false, 'sheet 1 has a note');   // (sanity)
  ok('sheet labels: the note names Left and Right and the other sheet; payload and normal copies are untouched');

  // 3 · the sibling moves onto sheet 1: sheet 1's label (notes now none) is out of date; the repair path makes it current without a note
  const rightCharm = Bp.charms.find(c => c.side === 'R'), moved = Object.assign({}, rightCharm, { id: 'sh-gf1-moved' });
  A.charms.push(moved); A.placements.push({ id: moved.id }); Bp.charms = Bp.charms.filter(c => c !== rightCharm); Bp.placements = Bp.charms.map(c => ({ id: c.id }));
  assert.equal(vm.runInContext('Sets.labelsReady(__A,__set)', ctx), false, 'the pair is whole on sheet 1 now: its label with a split note is stale');
  assert.equal(vm.runInContext('Sets.labelsReady(__B,__set)', ctx), false, 'sheet 2 still says "Right here": stale as well');
  await vm.runInContext('(async()=>{await Sets.onSheetSaved(__A,__A.charms,undefined,{setOverride:__set,labelsOnly:true});await Sets.onSheetSaved(__B,__B.charms,undefined,{setOverride:__set,labelsOnly:true});})()', ctx);
  assert.equal(callOf(1).notes.length, 0, 'remade without a note'); assert(!('notes' in A.label.files[0]), 'no notes key on the record when there is none');
  assert(vm.runInContext('Sets.labelsReady(__A,__set)', ctx) && vm.runInContext('Sets.labelsReady(__B,__set)', ctx), 'both current again');
  ok('stale split note: labelsReady false for both sheets, repaired by the existing remake path');

  // 6 · an old label file (no notes) on a sheet with no split is current: nothing to remake on today's sheets
  const oldSheet = page(world.sheets.find(s => s.id === 'sh-gf4'), { setId: 'set-2', seq: 2 }), oldSet = { setId: 'set-2', seq: 2, name: 'Set-2', orders: {}, sheetIds: [], materials: [], labelFiles: [] };
  ctx.__old = oldSheet; ctx.__oldSet = oldSet;
  await vm.runInContext('(async()=>{await Sets.onSheetSaved(__old,__old.charms,undefined,{setOverride:__oldSet,labelsOnly:true});})()', ctx);
  assert(!('notes' in oldSheet.label.files[0]) && vm.runInContext('Sets.labelsReady(__old,__oldSet)', ctx));
  ok('old and normal sheets: no notes key, labelsReady true, as before');

  /* ── 5 · the manifest of the real finalize and the real remakeFiles ── */
  const SE = require('../../charm-nest-set-edit.js');
  const finalizeSrc = (() => { const a = bridge.indexOf('    for (const [rid, o] of Object.entries(set.orders).sort()) { const copies ='); return bridge.slice(a, bridge.indexOf('\n', a)); })();
  assert(/CharmNestPairLabels\.manifestEntries\(o\.lines\)/.test(finalizeSrc), 'finalize uses the helper');
  const lines = []; const mk = vm.createContext({ window: { CharmNestPairLabels: PL }, CharmNestPairLabels: PL, set: { orders: { A1: { lines: { t1: { transactionId: 't1', sku: 'MITTENS-MIS', copies: [{ copy: 1, side: 'L', sheetId: 's1', sheet: 'GF Sheet 1' }, { copy: 2, side: 'R', sheetId: 's2', sheet: 'GF Sheet 2' }] } } }, A2: { lines: { t2: { transactionId: 't2', sku: 'PAIR-STUD', copies: [{ copy: 1, sheetId: 's1', sheet: 'GF Sheet 1' }, { copy: 2, sheetId: 's1', sheet: 'GF Sheet 1' }] } } } } }, ev: { held: {} }, line: t => lines.push(t) });
  vm.runInContext(finalizeSrc, mk);
  assert.deepEqual(lines, ['A1  MITTENS-MIS Left->GF Sheet 1  MITTENS-MIS Right->GF Sheet 2  [split over 2 sheets]', 'A2  PAIR-STUD#1->GF Sheet 1  PAIR-STUD#2->GF Sheet 1'], 'finalize manifest lines: ' + JSON.stringify(lines));
  // remakeFiles: the real function over a stub CN that records the manifest page text via a fake PDFLib
  const draws = [], jsonSaved = {};
  const fakePDF = { PDFDocument: { create: async () => { const pages = []; const d = { embedFont: async () => ({}), embedPng: async () => ({}), addPage: () => { const pg = { drawText: t => draws.push(t), drawImage() {} }; pages.push(pg); return pg; }, save: async () => new Uint8Array([1]) }; return d; } }, StandardFonts: { Helvetica: 'h', HelveticaBold: 'hb' } };
  const setDoc = { setId: 'set-1', name: 'Set-1', day: '2026-10-09', runId: 'run-pairs', folder: 'f', sheetIds: ['sh-gf1', 'sh-gf2'], materials: ['gold'], labelFiles: [{ sheetId: 'sh-gf1', sheet: 'GF_Sheet-1', url: 'u1', path: 'p1', part: 1, label: 'L1' }],
    orders: { 4190000009: { lines: [{ transactionId: 't', sku: 'MITTENS-MIS', copies: [{ copy: 1, side: 'L', sheetId: 'sh-gf1', sheet: 'GF Sheet 1' }, { copy: 2, side: 'R', sheetId: 'sh-gf2', sheet: 'GF Sheet 2' }] }] }, 4190000090: { lines: [{ transactionId: 'u', sku: 'ONE', copies: [{ copy: 1, sheetId: 'sh-gf1', sheet: 'GF Sheet 1' }] }] } } };
  const cloud = async b => { if (b.op === 'setGet') return { set: setDoc }; if (b.op === 'listSheets') return { sheets: world.sheets.filter(s => s.setId === 'set-1').map(s => Object.assign({}, s, { backs: [{ poolId: '4190000009_5000000010_1', order: '4190000009', sku: 'MITTENS-MIS', copy: 1, side: 'L', groupKey: '4190000009:5000000010', text: 'A', approvedBy: 'T' }] })) }; if (b.op === 'flowApply') { noNest(b, 'flowApply'); return { ok: true }; } return {}; };
  const sandbox = { CN: { api: cloud, uploadBytes: async (p, bytes) => { if (/set\.json$/.test(p)) Object.assign(jsonSaved, JSON.parse(new TextDecoder().decode(bytes))); return { path: p, url: 'u:' + p }; } }, PDFLib: fakePDF, CharmNestAssets: { bytes: async () => new Uint8Array([1]) }, CharmNestPairLabels: PL };
  const seLoad = new Function('self', 'module', 'require', fs.readFileSync(path.join(root, 'charm-nest-set-edit.js'), 'utf8') + '\nreturn module.exports;');
  const SEp = seLoad(Object.assign({}, sandbox, { document: undefined }), { exports: {} }, require);
  // (the page exposes it as root.SetEdit; here the function bound to our sandbox as root)
  assert(SEp && typeof SEp.remakeFiles === 'function'); await SEp.remakeFiles({ setId: 'set-1' }, { by: 'T' });
  const man = draws.map(d => d.x === undefined ? d : '').filter(Boolean);
  const txt = draws.join('\n');
  assert(/4190000009  MITTENS-MIS Left->GF Sheet 1  MITTENS-MIS Right->GF Sheet 2  \[split over 2 sheets\]/.test(txt), 'remakeFiles manifest: ' + txt.slice(0, 600));
  assert(/4190000090  ONE->GF Sheet 1/.test(txt), 'a single charm line is the old text');
  assert(/MITTENS-MIS #1 · Left "A"|#1 - Left "A"/.test(txt), 'the Engraving line names the ear: ' + txt.slice(0, 900));
  assert.equal(jsonSaved.sheets[0].backs[0].side, 'L'); assert.equal(jsonSaved.sheets[0].backs[0].groupKey, '4190000009:5000000010');
  noNest(jsonSaved, 'set.json');
  ok('manifest and set.json (finalize and remakeFiles): Left/Right, "[split over 2 sheets]", engraving ear; every other order line is the old text');

  /* ── 7 · layer names in the real sheet builder, 8 · export ── */
  g = globalThis; if (!g.window) g.window = g; g.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js');
  require('../../charm-nest-pdf.js'); const PDF = g.CharmNestPDF;
  const Ex = require('../../charm-nest-export.js');
  const mini = await g.PDFLib.PDFDocument.create(); const pg = mini.addPage([200, 100]); pg.drawRectangle({ x: 10, y: 10, width: 20, height: 20 });
  const src = await PDF.parseSource(await mini.save(), 'm');
  const mkCharm = (name, extra) => Object.assign({ name, sourceId: 's', topIndices: [], members: [], strokePt: .25, bbox: [10, 70, 30, 90], centerPt: [20, 80] }, extra);
  const sheetBytes = await PDF.buildSheet({ sheet: { wPt: 300, hPt: 150 }, sources: new Map([['s', src]]), placements: [
    { charm: mkCharm('4190000009 · MITTENS-MIS', { side: 'L' }), angle: 0, cxPt: 30, cyPt: 30 }, { charm: mkCharm('4190000009 · MITTENS-MIS', { side: 'R' }), angle: 0, cxPt: 90, cyPt: 30 },
    { charm: mkCharm('4190000001 · PAIR-STUD · 1/2'), angle: 0, cxPt: 150, cyPt: 30 }, { charm: mkCharm('4190000001 · PAIR-STUD · 2/2'), angle: 0, cxPt: 210, cyPt: 30 },
    { charm: mkCharm('A'.repeat(70), { side: 'R' }), angle: 0, cxPt: 30, cyPt: 90 }, { charm: mkCharm('X Left', { side: 'L' }), angle: 0, cxPt: 90, cyPt: 90 }], title: 't' });
  const out = await PDF.parseSource(sheetBytes, 'sheet'), layers = Ex.layerNames(out);
  assert(layers.includes('4190000009  MITTENS-MIS Left') && layers.includes('4190000009  MITTENS-MIS Right'), 'ear layers: ' + JSON.stringify(layers));
  assert(layers.includes('4190000001  PAIR-STUD  1/2') && layers.includes('4190000001  PAIR-STUD  2/2'), 'matching pair layers are named as before');
  const long = layers.find(n => /^A+ Right$/.test(n)); assert(long && long.length <= 60, 'long name cut, side kept, at most 60: ' + long);
  assert(layers.includes('X Left'), 'a name already ending in Left is not doubled');
  const dxf = Ex.dxf(Ex.productionPaths(out), layers); assert(dxf.layers.some(([, n]) => n === '4190000009  MITTENS-MIS Left') && dxf.layers.some(([, n]) => n === '4190000009  MITTENS-MIS Right'), 'the DXF layers carry the ear');
  ok('layer names: the .ai and the DXF say Left and Right for a piece with a side; other names unchanged; at most 60 characters');

  // back layer and the export's back list
  const front = await PDF.buildSheet({ sheet: { wPt: 300, hPt: 150 }, sources: new Map([['s', src]]), placements: [{ charm: mkCharm('4190000009 · MITTENS-MIS', { side: 'L' }), angle: 0, cxPt: 30, cyPt: 30 }], title: 't' });
  const backDoc = await g.PDFLib.PDFDocument.create(); backDoc.addPage([40, 40]).drawRectangle({ x: 5, y: 5, width: 20, height: 20 });
  const parsedFront = await PDF.parseSource(front, 'f'), formLayer = parsedFront.segments.find(s => s.kind === 'xobj').layer;
  const sheetRec = { id: 'sh-1', fileBase: 'GF_x', charms: [{ id: 'c0', poolId: '4190000009_5000000010_1', side: 'L' }], placements: [{ id: 'c0', layerName: formLayer, scale: 1 }] };
  const composed = await Ex.compose(front, sheetRec, [{ poolId: '4190000009_5000000010_1', bytes: await backDoc.save() }]);
  assert.equal(composed.layout[0].side, 'L'); assert.equal(composed.layout[0].groupKey, '4190000009:5000000010');
  const backLayers = Ex.layerNames(await PDF.parseSource(composed.ai, 'c'));
  assert(backLayers.some(n => /^BACK 4190000009_5000000010_1 Left \/ /.test(n)), 'back layer says the ear: ' + JSON.stringify(backLayers));
  const plainRec = { id: 'sh-2', charms: [{ id: 'c0', poolId: '4190000001_5000000010_1' }], placements: [{ id: 'c0', layerName: formLayer, scale: 1 }] };
  const composed2 = await Ex.compose(front, plainRec, [{ poolId: '4190000001_5000000010_1', bytes: await backDoc.save() }]);
  assert(!('side' in composed2.layout[0]) && Ex.layerNames(await PDF.parseSource(composed2.ai, 'c')).some(n => /^BACK 4190000001_5000000010_1 \/ /.test(n)), 'a back with no side: the old layer and the old layout entry');
  ok('export: the back layer and the layout name the ear; a back with no side is as before');
  const ui = read('charm-nest-export-ui.js');
  assert(/pairPieces:earPieces/.test(ui) && /A layer ending Left or Right is one ear/.test(ui) && /files\.some\(f=>f\.metadata\.pairPieces\)/.test(ui), 'IMPORT.txt explains the layers only for a sheet with such a piece');

  /* ── 9 · the sticker ── */
  const entryMis = F.entryOf('MITTENS-MIS'), entryStud = F.entryOf('PAIR-STUD');
  assert.deepEqual(PL.labelPieces({ quantity: 1 }, entryMis), [{ side: 'L', n: 1, of: 1 }, { side: 'R', n: 1, of: 1 }]);
  assert.deepEqual(PL.labelPieces({ quantity: 2 }, entryMis).map(p => p.side + p.n + '/' + p.of), ['L1/2', 'R1/2', 'L2/2', 'R2/2']);
  assert.equal(PL.labelPieces({ quantity: 1 }, entryStud), null, 'a matching pair: no pieces, the one sticker'); assert.equal(PL.labelPieces({ quantity: 1 }, null), null);
  assert.equal(PL.stickerPieces([{ line: { quantity: 1 } }], () => entryStud), null);
  assert.equal(PL.stickerPieces([{ line: { quantity: 1 } }, { line: { quantity: 1 } }], r => (r === undefined ? null : entryMis)).length, 4);
  assert.equal(PL.pieceCount([{ line: { quantity: 1 } }, { line: { quantity: 3 } }], r => r.line.quantity === 1 ? entryMis : entryStud), 5, 'a mismatched pair makes two, any other line what its quantity says');
  assert.equal(PL.pieceCount([{ line: { quantity: 3 } }], () => entryStud), null, 'no pair: null, so the page counts as it always did');
  // QR Printer.html's own functions
  const html = read('QR Printer.html'), script = html.slice(html.indexOf('<script>\n') + 9, html.lastIndexOf('</script>'));
  const pr = vm.createContext({ window: { addEventListener() {}, parent: null }, document: { createElement: () => ({ style: {} }), body: { appendChild() {} } }, location: { hash: '' }, localStorage: { getItem: () => null }, console, setTimeout, pdfMake: {}, QRCode: {} });
  pr.window.parent = pr.window; vm.runInContext(script, pr);
  const doc = (pieces, qty) => vm.runInContext(`buildPDFDocWithDots('data:x',2,'09 Oct 2026','4190000009',{metal:'Gold Filled',title4:'MITTENS',matrixNums:''},${JSON.stringify(pieces)})`, pr);
  const one = doc(undefined), none = doc([]);
  assert.deepEqual(JSON.parse(JSON.stringify(none)), JSON.parse(JSON.stringify(one)), 'no pieces: the page it always was');
  assert.equal(one.content.length, 5, 'the old sticker is five items'); assert(!JSON.stringify(one).includes('LEFT'));
  const pairDoc = doc([{ side: 'L', n: 1, of: 1 }, { side: 'R', n: 1, of: 1 }]), texts = pairDoc.content.map(c => c.text).filter(Boolean);
  assert.equal(pairDoc.content.filter(c => c.pageBreak).length, 1, 'two pages'); assert(texts.includes('LEFT') && texts.includes('RIGHT') && texts.filter(t => t === '4190000009').length === 2, 'LEFT and RIGHT of the same order on a page each: ' + JSON.stringify(texts));
  const four = doc([{ side: 'L', n: 1, of: 2 }, { side: 'R', n: 1, of: 2 }, { side: 'L', n: 2, of: 2 }, { side: 'R', n: 2, of: 2 }]); assert(four.content.map(c => c.text).includes('2/2') && four.content.filter(c => c.pageBreak).length === 3, 'two pairs: four pages, n/of');
  assert.deepEqual(vm.runInContext(`piecesOf({pieces:[{side:'L'},{side:'Q'},null,{side:'R'}]})`, pr).map(p => p.side), ['L', 'R']);
  // the bridge wires the pieces into the label object and the efficiency count
  assert(/label = rows\.length \? withPieces\(O\.sortingLabel\(rows\[0\]\.order, rows\[0\]\.line\), rows\) : null/.test(bridge) && /label = withPieces\(O\.sortingLabel\(rows\[0\]\.order, rows\[0\]\.line\), rows\)/.test(bridge), 'printAs and completeAs build the label with its pieces');
  assert.equal((bridge.match(/parts: piecesN\(/g) || []).length, 3, 'the three efficiency records count pieces');
  ok('sticker: a mismatched pair prints LEFT and RIGHT pages of the same order; every other card prints the one page it always did');

  /* ── 10 · Complete Order / print over the real customPut, 12 · welding ── */
  const fsx = F.fakeFirestore(); const fns = F.functions(fsx, ['charmNestLibrary']);
  try {
    const key = '4190000009_5000000010';
    const rec = await fns.lib('customPut', { key, receiptId: '4190000009', transactionId: '5000000010', sku: 'MITTENS-MIS', title: 'Mismatched', by: 'Tess', label: { items: [], pieces: [{ side: 'L', n: 1, of: 1 }, { side: 'R', n: 1, of: 1 }] } });
    assert.equal(rec.ok, true, JSON.stringify(rec)); assert.equal(rec.record.key, key, 'one record for the pair line: the group is the unit');
    const done = await fns.lib('customPut', { key, how: 'button', receiptId: '4190000009', transactionId: '5000000010', sku: 'MITTENS-MIS', by: 'Tess', label: { items: [], pieces: [{ side: 'L' }, { side: 'R' }] } });
    const ev = fsx.list('Order_Timeline').filter(e => /^custom|seal/.test(e.type || '') || /Left \+ Right/.test(e.text || ''));
    const texts2 = fsx.st.list ? fsx.list('Order_Timeline').map(e => e.text || '').concat(JSON.stringify(fsx.docsOf().sets)) : [];
    const everything = JSON.stringify(Array.from(fsx.st.docs.values()));
    assert(/MITTENS-MIS · print 1 · Left \+ Right/.test(everything) && /MITTENS-MIS · Complete Order · Left \+ Right/.test(everything), 'both seals on the timeline say Left + Right');
    assert.equal((everything.match(/"sealPrinted"/g) || []).length >= 1, true);
    // a normal line: the event text is what it always was
    await fns.lib('customPut', { key: '4190000001_5000000001', receiptId: '4190000001', transactionId: '5000000001', sku: 'PAIR-STUD', by: 'Tess', label: { items: [] } });
    const all2 = JSON.stringify(Array.from(fsx.st.docs.values())); assert(/PAIR-STUD · print 1"/.test(all2), 'a normal print event keeps its text');
    ok('Complete Order and print: one record (one seal each) for the pair line; "Left + Right" on its events only');
  } finally { fns.restore(); }
  // welding: the real rule of weld-1.html (one welded event per stud LINE, so one seal for a pair)
  const weld = read('weld-1.html'), a = weld.indexOf('function weldDone('), b = weld.indexOf('/* Employee efficiency (station-activity.js)', a);
  const recorded = []; const wctx = vm.createContext({ window: { StationTimeline: { did(...x) { recorded.push(['did', ...x]); } }, OrderTimeline: { record: e => recorded.push(['record', e]), config() {} } }, StationTimeline: { did(...x) { recorded.push(['did', ...x]); } }, console });
  vm.runInContext(`const WELD_STUD=/\\bstuds?\\b/i,WELD_NOT_STUD=/\\b(?:huggies?|hoops?|necklaces?|bracelets?|anklets?|key\\s*(?:chains?|rings?))\\b|\\bcharms?\\s+only\\b/i;
    function weldIsStud(t){const text=[t&&t.title,...((t&&Array.isArray(t.variations))?t.variations.map(v=>v&&(v.formatted_value||v.value)):[])].filter(Boolean).join(" ");return WELD_STUD.test(text)&&!WELD_NOT_STUD.test(text);}
    function weldRefusal(d){return "";} function weldTell(){} function weldSignedIn(){return "Marco";}
    ${weld.slice(a, b)}`, wctx);
  vm.runInContext(`weldDone('4190000009',{transactions:[{transaction_id:5000000010,title:'Mismatched stud earrings',quantity:1},{transaction_id:5000000011,title:'Disc necklace',quantity:1}]})`, wctx);
  assert.equal(recorded.length, 1, 'one welded event for a pair of studs on a mixed order (not one per ear): ' + JSON.stringify(recorded));
  assert.equal(recorded[0][1].transactionId, '5000000010'); assert.equal(recorded[0][1].type, 'welded');
  recorded.length = 0; vm.runInContext(`weldDone('4190000009',{transactions:[{transaction_id:5000000010,title:'Mismatched stud earrings',quantity:1}]})`, wctx);
  assert.equal(recorded.length, 1, 'an order of only the pair: one welded seal'); assert.equal(recorded[0][0], 'did');
  ok('welded seal: one for a pair of studs (the line), none for a necklace beside it');

  /* ── 11 · laser acts ── */
  const acts = []; const lctx = { CNAct: (action, o) => { acts.push([action, o]); return true; } }; lctx.window = lctx;
  const lsrc = read('charm-nest-laser-act.js'); new Function('window', 'globalThis', lsrc.replace('})(typeof window !== "undefined" ? window : globalThis);', '})(window);'))(lctx, lctx);
  const both = lctx.CNLaserAct.factsOf('A', { poolIds: ['4190000009_5000000010_1', '4190000009_5000000010_2', '4190000090_5000000090_1'], placedCount: 3 });
  assert.equal(both.by.get('4190000009'), 2, 'both ears on one sheet: two pieces'); assert.equal(both.by.get('4190000090'), 1);
  const halves = [lctx.CNLaserAct.factsOf('B', { poolIds: ['4190000009_5000000010_1'], placedCount: 1 }), lctx.CNLaserAct.factsOf('C', { poolIds: ['4190000009_5000000010_2'], placedCount: 1 })];
  assert.deepEqual(halves.map(h => h.by.get('4190000009')), [1, 1], 'split: one piece each');
  lctx.CNLaserAct.cut(true, { name: 'GF Sheet 1', ids: ['B', 'C'], rec: id => ({ poolIds: [id === 'B' ? '4190000009_5000000010_1' : '4190000009_5000000010_2'], placedCount: 1 }) });
  assert(acts.length === 1 && acts[0][0] === 'complete' && acts[0][1].orders === 0 && acts[0][1].each.every(e => e.parts === 1), 'a cut is a complete with orders 0 and one piece per sheet: ' + JSON.stringify(acts));
  ok('laser acts: the pieces of a pair are counted per sheet, a cut never completes the order');

  console.log('\nPAIRLABELS OK');
})().catch(e => { console.error(e); process.exit(1); });
let g;
