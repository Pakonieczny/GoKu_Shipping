// FINISHCORE (the cloud carries every run, architecture v2, build step 7: the module only, not the cloud writer): the pure parts of finishNest and persistSheet moved out of charm-nest-1.html
// into charm-nest-finish-core.js, which the page and node both load. This checks the module in node, and that the page hands its work to it:
//   1 membershipOf: NOPARTIALSET (a partial GF / SS sheet joining a set is saved as a draft outside every set), SETSHUT (a partial sheet the set already lists keeps its set), KEEPINSET
//     (a partial sheet the owner put back in its set keeps it), and every other sheet keeps its own membership
//   2 the page's membershipOf, as the page has it, hands the run's sets (Sets.ofRun) and Gate.modern to the module: the same answers
//   3 record(): the Library record a save writes, its membership taken from membershipOf (never from the page's memory), its lookups from the host
//   4 the page builds its record, its membership, its job, its spec, its report and its layout key through the module; the module reads no page global; one CHARM_SCALE and one CAREFUL_ANGLES
// Offline: fixtures only, no browser, no network, nothing live.   node tests/charm-nest/finish-core.cjs
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..');
const FC = require(path.join(root, 'charm-nest-finish-core.js')), SR = require(path.join(root, 'charm-nest-set-rules.js'));
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), core = fs.readFileSync(path.join(root, 'charm-nest-finish-core.js'), 'utf8');
let fails = 0; const check = (id, fn) => { try { fn(); console.log('PASS  ' + id); } catch (e) { fails++; console.log('FAIL  ' + id + '  ' + (e && e.message || e)); } };
const plain = v => JSON.parse(JSON.stringify(v));

/* ── fixtures: a run, its Set 5, and its sheets ── */
const RUN = 'run-1';
const sheet = o => Object.assign({ sheetId: 'gf-10', metal: 'gold', metalLabel: 'GF 14/20', runId: RUN, draft: true, setId: null, seq: 15, sheetIndex: null, placements: [{ id: 'a' }, { id: 'b' }], releaseFull: false }, o || {});
const joining = o => sheet(Object.assign({ draft: false, setId: 'set-5', seq: 5, sheetIndex: 10 }, o || {}));   // the set look has put it in Set 5 in memory (draft false, the set, the next number)
const set5 = ids => [{ setId: 'set-5', seq: 5, sheetIds: ids }];
const host = (sets, o) => Object.assign({ SetRules: SR, setsOfRun: runId => (runId === RUN ? sets : []), modern: () => true }, o || {});
const DRAFT = { draft: true, setId: null, setSeq: null, sheetIndex: null }, IN5 = { draft: false, setId: 'set-5', setSeq: 5, sheetIndex: 10 };

/* ── 1 · membershipOf ── */
check('1a NOPARTIALSET: a partial GF sheet joining a set (the set does not list it) is saved as a draft outside every set', () => assert.deepEqual(FC.membershipOf(joining(), host(set5(['gf-1', 'gf-2']))), DRAFT));
check('1b the same for a partial SS sheet', () => assert.deepEqual(FC.membershipOf(joining({ metal: 'silver', metalLabel: 'SS' }), host(set5(['gf-1']))), DRAFT));
check('1c SETSHUT: a partial sheet the set already lists keeps its set, its number and its index', () => assert.deepEqual(FC.membershipOf(joining(), host(set5(['gf-1', 'gf-10']))), IN5));
check('1d SETSHUT reads the set the sheet is in: another set of the run listing it does not hold it', () => assert.deepEqual(FC.membershipOf(joining(), host([{ setId: 'set-4', sheetIds: ['gf-10'] }, { setId: 'set-5', sheetIds: [] }])), DRAFT));
check('1e KEEPINSET: a partial sheet the owner put back in its set keeps it, the set listing it or not', () => assert.deepEqual(FC.membershipOf(joining({ keepInSet: { setId: 'set-5', by: 'owner', at: 1 } }), host(set5([]))), IN5));
check('1f KEEPINSET is for that set only: a put-back into another set does not hold this one', () => assert.deepEqual(FC.membershipOf(joining({ keepInSet: { setId: 'set-4' } }), host(set5([]))), DRAFT));
check('1g a COMPLETED sheet (released full) keeps its set', () => assert.deepEqual(FC.membershipOf(joining({ releaseFull: true }), host(set5([]))), IN5));
check('1h a CUT sheet keeps its set', () => assert.deepEqual(FC.membershipOf(joining({ laserDoneAt: 5 }), host(set5([]))), IN5));
check('1i a 10K / 14K / Rose Gold sheet (their own rules) keeps its set', () => { for (const metal of ['gold10k', 'gold14k', 'rose']) assert.equal(FC.membershipOf(joining({ metal, metalLabel: '' }), host(set5([]))).setId, 'set-5', metal); });
check('1j a run that is not modern is saved as before', () => assert.deepEqual(FC.membershipOf(joining(), host(set5([]), { modern: () => false })), IN5));
check('1k a plain partial draft is saved as it is: outside every set, with its own number', () => assert.deepEqual(FC.membershipOf(sheet(), host(set5([]))), { draft: true, setId: null, setSeq: 15, sheetIndex: null }));
check('1l the run\'s sets unreadable (setsOfRun throws, or no setsOfRun): nothing holds the sheet, NOPARTIALSET stands', () => {
  assert.deepEqual(FC.membershipOf(joining(), host(null, { setsOfRun: () => { throw new Error('offline'); } })), DRAFT);
  assert.deepEqual(FC.membershipOf(joining(), { SetRules: SR, modern: () => true }), DRAFT);
});
check('1m a rule that cannot be read (no set rules, or one that throws) leaves the sheet its own membership', () => {
  assert.deepEqual(FC.membershipOf(joining(), host(set5([]), { SetRules: null })), IN5);
  assert.deepEqual(FC.membershipOf(joining(), host(set5([]), { SetRules: { isPartial: () => { throw new Error('x'); } } })), IN5);
  assert.deepEqual(FC.membershipOf(joining(), host(set5([]), { modern: () => { throw new Error('x'); } })), IN5);
});
check('1n membershipOf changes nothing on the sheet', () => { const sh = joining(), was = JSON.stringify(sh); FC.membershipOf(sh, host(set5([]))); FC.membershipOf(sh, host(set5(['gf-10']))); assert.equal(JSON.stringify(sh), was); });

/* ── 2 · the page's membershipOf hands its sets and its Gate to the module ── */
const pageMember = (sh, { sets = [], modern = true, withSets = true } = {}) => {
  const a = html.indexOf('function membershipOf(sh) {'), b = html.indexOf('async function persistSheet(', a); assert(a > 0 && b > a, 'membershipOf sits before persistSheet');
  const Gate = { modern: () => modern }, Sets = withSets ? { ofRun: runId => (runId === RUN ? sets : []) } : undefined;
  const ctx = vm.createContext({ window: { CharmNestSetRules: SR, Gate, ...(Sets ? { Sets } : {}) }, Gate, ...(Sets ? { Sets } : {}), CharmNestFinishCore: FC });
  vm.runInContext(html.slice(a, b) + ';this.f = membershipOf;', ctx); return plain(ctx.f(sh));
};
check('2a the page: a partial sheet joining a set is a draft', () => assert.deepEqual(pageMember(joining(), { sets: set5(['gf-1']) }), DRAFT));
check('2b the page: a partial sheet its set lists (Sets.ofRun) keeps its set (SETSHUT)', () => assert.deepEqual(pageMember(joining(), { sets: set5(['gf-10']) }), IN5));
check('2c the page: an owner\'s put-back keeps its set (KEEPINSET)', () => assert.deepEqual(pageMember(joining({ keepInSet: { setId: 'set-5' } })), IN5));
check('2d the page with no Sets yet: NOPARTIALSET stands', () => assert.deepEqual(pageMember(joining(), { withSets: false }), DRAFT));
check('2e the page in a run that is not modern: as before', () => assert.deepEqual(pageMember(joining(), { modern: false }), IN5));

/* ── 3 · record() ── */
const charms = [
  { id: 'a', hash: 'ha', name: 'Heart', sourceId: 'src1', sourceName: 'heart.ai', index: 0, qty: 1, order: '4100/1', poolId: '4100_1_0', areaPt2: 40.04, widthPt: 10.123, heightPt: 9.876, cloud: { png: 'u/a.png', ai: 'u/a.ai', pngPath: 'p/a.png', aiPath: 'p/a.ai' }, pair: true, manual: { size: 1.1 } },
  { id: 'b', hash: 'hb', name: 'Star', sourceId: 'src1', sourceName: 'heart.ai', index: 1, qty: 2, order: '4200', poolId: '4200_1_0', areaPt2: 20, widthPt: 5, heightPt: 4, custom: true, sku: 'STAR_1', frozen: { at: 1 } },
  { id: 'c', hash: 'hc', name: 'Moon', sourceId: 'src2', sourceName: 'moon.ai', index: 0, qty: 1, order: '4300', areaPt2: 12, widthPt: 3, heightPt: 3, excluded: true }
];
const saved = o => Object.assign(joining(), {
  cardStartedAt: 111, status: 'complete', endedBy: 'complete', trials: 7, searchMetrics: { a: 1 }, gpuProgress: null, elapsedMs: 1234.6, totalMs: 5678.4, solveParams: { seed: 3 }, density: .61, page: 2, draftOrder: 4,
  liveInfo: { freePt2: 100.4, usablePt2: 300.6, pocket: 2 }, verification: { ok: true, minGapPt: .2, minEdgePt: .3, render: { ok: true, overlapPx: 0, outsidePx: 0, extra: 'kept out' }, geom: { big: true } },
  outputs: { report: { stock: { wPt: 283, hPt: 141 }, rejects: [{ id: 'c' }], placements: [{ n: 1, id: 'a' }, { n: 2, id: 'b' }] } },
  placements: [{ id: 'a', cxPt: 1, cyPt: 2, angle: 0 }, { id: 'b', cxPt: 3, cyPt: 4, angle: 90 }], dirty: false, topup: { at: 1 }, intakeFinalized: true, intakeOptimized: true, intakeOptimizedCount: 3,
  backPool: ['x'], backOutputs: { count: 1 }, label: { own: false }
}, o || {});
const sources = { src1: { id: 'src1', name: 'heart.ai', zipName: 'z.zip', hash: 'hs1', bytes: new Uint8Array(9), cloud: { path: 'p/1', url: 'u/1' }, pool: true, sku: 'HEART' }, src2: { id: 'src2', name: 'moon.ai' } };
const recordOf = (sh, extra) => {
  const byId = new Map(charms.map(c => [c.id, c]));
  return Object.assign({ items: charms, byId, day: '2026-11-20', name: 'GF_Nov.20.26_Set-5_Sheet-10', member: FC.membershipOf(sh, host(set5([]))), shapes: new Map([['a', { shape: 'disc' }]]),
    sheetName: s => 'GF 14/20 · sheet ' + s.page, sourceOf: id => sources[id] || null, solidSelected: () => true, pairDescriptor: c => (c.pair ? { pairGroup: 'g1' } : {}), listingsOf: cs => cs.every(Boolean) ? ['L1'] : null }, extra || {});
};
check('3a record(): the save\'s facts, the sheet\'s state and its files as given (null for the first write)', () => {
  const sh = saved(), r = FC.record(sh, recordOf(sh), null);
  assert.equal(r.id, 'gf-10'); assert.equal(r.metalLabel, 'GF 14/20 · sheet 2'); assert.equal(r.day, '2026-11-20'); assert.equal(r.folder, 'GF_Nov.20.26_Set-5_Sheet-10'); assert.equal(r.fileBase, r.folder);
  assert.equal(r.seq, 5); assert.equal(r.runId, RUN); assert.equal(r.status, 'complete'); assert.equal(r.elapsedMs, 1235); assert.equal(r.totalMs, 5678); assert.equal(r.freePt2, 100); assert.equal(r.usablePt2, 301);
  assert.equal(r.outputs, null); assert.deepEqual(r.stock, { wPt: 283, hPt: 141 }); assert.equal(r.charmCount, 3); assert.equal(r.placedCount, 2); assert.equal(r.rejectCount, 1); assert.equal(r.draftOrder, 4);
  assert.deepEqual(r.verification, { ok: true, minGapPt: .2, minEdgePt: .3, render: { ok: true, overlapPx: 0, outsidePx: 0 } });
  assert.deepEqual(r.orders, ['4100', '4200']); assert.deepEqual(r.poolIds, ['4100_1_0', '4200_1_0']); assert.deepEqual(r.listings, ['L1']);
  assert.deepEqual(r.placements, sh.outputs.report.placements); assert.deepEqual(r.rejects, [{ id: 'c' }]); assert.equal(r.names, 'Heart Star Moon');
  assert.deepEqual(r.backPool, ['x']); assert.deepEqual(r.backOutputs, { count: 1 }); assert.deepEqual(r.label, { own: false }); assert.deepEqual(r.topup, { at: 1 });
  assert.equal(r.intakeFinalized, true); assert.equal(r.intakeOptimizedCount, 3); assert.equal(r.releaseFull, false); assert.equal(r.dirty, false);
  const outs = { ai: { path: 'a.ai', url: 'u' } }; assert.equal(FC.record(sh, recordOf(sh), outs).outputs, outs);
});
check('3b record(): a partial sheet joining a set is written as the draft it is (its membership is membershipOf\'s, not the page\'s memory)', () => {
  const sh = saved(), r = FC.record(sh, recordOf(sh), null);
  assert.equal(sh.draft, false); assert.equal(sh.setId, 'set-5');   // (the page's memory still says Set 5)
  assert.deepEqual({ draft: r.draft, setId: r.setId, setSeq: r.setSeq, sheetIndex: r.sheetIndex }, DRAFT);
});
check('3c record(): a partial sheet its set lists keeps its set in the record (SETSHUT)', () => {
  const sh = saved(), r = FC.record(sh, recordOf(sh, { member: FC.membershipOf(sh, host(set5(['gf-10']))) }), null);
  assert.deepEqual({ draft: r.draft, setId: r.setId, setSeq: r.setSeq, sheetIndex: r.sheetIndex }, IN5);
});
check('3d record(): each charm, with its pair fields, its hand size, its frozen mark and its shape', () => {
  const sh = saved(), r = FC.record(sh, recordOf(sh), null), [a, b, c] = r.charms;
  assert.deepEqual(a, { id: 'a', hash: 'ha', name: 'Heart', sourceId: 'src1', sourceName: 'heart.ai', index: 0, qty: 1, order: '4100/1', layer: null, poolId: '4100_1_0', custom: false, sku: null, areaPt2: 40, widthPt: 10.12, heightPt: 9.88, thumbUrl: 'u/a.png', aiUrl: 'u/a.ai', pngPath: 'p/a.png', aiPath: 'p/a.ai', excluded: false, open: false, pairGroup: 'g1', manual: { size: 1.1 }, shape: 'disc' });
  assert.equal(b.custom, true); assert.equal(b.sku, 'STAR_1'); assert.deepEqual(b.frozen, { at: 1 }); assert.equal(b.thumbUrl, null); assert.equal('pairGroup' in b, false); assert.equal('shape' in b, false);
  assert.equal(c.excluded, true); assert.equal(c.poolId, null);
});
check('3e record(): the sources the charms came from, once each', () => {
  const sh = saved(), r = FC.record(sh, recordOf(sh), null);
  assert.deepEqual(r.sources, [{ id: 'src1', name: 'heart.ai', zipName: 'z.zip', hash: 'hs1', bytes: 9, path: 'p/1', url: 'u/1', pool: true, custom: false, sku: 'HEART' }, { id: 'src2', name: 'moon.ai', zipName: undefined, hash: undefined, bytes: 0, path: undefined, url: undefined, pool: false, custom: false, sku: null }]);
  const r2 = FC.record(sh, recordOf(sh, { sourceOf: id => (id === 'src1' ? sources.src1 : null) }), null); assert.equal(r2.sources.length, 1);
});
check('3f record(): solidIncluded: a 10K / 14K sheet asks solidSelected, a Rose Gold sheet keeps its own tick, other metals carry none', () => {
  const k = saved({ metal: 'gold14k' }); assert.equal(FC.record(k, recordOf(k), null).solidIncluded, true); assert.equal(FC.record(k, recordOf(k, { solidSelected: () => undefined }), null).solidIncluded, false); assert.equal(FC.record(k, recordOf(k, { solidSelected: undefined }), null).solidIncluded, false);
  const r = saved({ metal: 'rose' }); assert.equal(FC.record(r, recordOf(r), null).solidIncluded, null); assert.equal(FC.record(saved({ metal: 'rose', solidPick: true }), recordOf(r), null).solidIncluded, true);
  const g = saved(); assert.equal(FC.record(g, recordOf(g), null).solidIncluded, null);
});
check('3g record(): no listings when a piece\'s line is not known (listingsOf null, or no listingsOf); no draftOrder unless it is a number', () => {
  const sh = saved({ draftOrder: undefined }), r = FC.record(sh, recordOf(sh, { listingsOf: () => null }), null);
  assert.equal('listings' in r, false); assert.equal('draftOrder' in r, false); assert.equal('listings' in FC.record(sh, recordOf(sh, { listingsOf: undefined }), null), false);
});
check('3h record() changes nothing on the sheet, and the same save gives the same record', () => { const sh = saved(), was = JSON.stringify(sh), o = recordOf(sh); const r1 = JSON.stringify(FC.record(sh, o, null)), r2 = JSON.stringify(FC.record(sh, o, null)); assert.equal(JSON.stringify(sh), was); assert.equal(r1, r2); });

/* ── 4 · the page goes through the module, and the module reads nothing of the page ── */
check('4a persistSheet takes its membership from membershipOf and builds its record with the module', () => {
  const a = html.indexOf('async function persistSheet('), b = html.indexOf('\n}\n', a), ps = html.slice(a, b);
  assert(/const member = membershipOf\(sh\);/.test(ps), 'member');
  assert(/const record = \(outputs\) => CharmNestFinishCore\.record\(sh, recordOf, outputs\);/.test(ps), 'record');
  assert(/const recordOf = \{ items, byId, day, name, member, shapes,/.test(ps), 'the save\'s own facts');
  assert(/shapeFieldOf\(/.test(ps), 'the shapes are still made once per save, by the page');
});
check('4b the page\'s membershipOf, buildJob, sheetSpec, buildReport, layoutKey and exactFix hand their work to the module; fixOverlaps plans through it', () => {
  for (const re of [/function membershipOf\(sh\) \{\s*return CharmNestFinishCore\.membershipOf\(sh,/, /function buildJob\(sh\) \{\s*return CharmNestFinishCore\.buildJob\(sh,/, /function sheetSpec\(sh, byId, st, result\) \{\s*return CharmNestFinishCore\.sheetSpec\(/,
    /function buildReport\(sh, result, items\) \{\s*return CharmNestFinishCore\.buildReport\(/, /const layoutKey = \(sh, byId\) => CharmNestFinishCore\.layoutKey\(sh, byId\);/, /async function exactFix\(sh, o\) \{\s*return CharmNestFinishCore\.exactFix\(sh, o,/,
    /FC\.overlapRound\(sh,/, /FC\.applyOverlapFixes\(plan, step\.byLayer, saved, done, res\)/]) assert(re.test(html), String(re));
  for (const gone of ['const OVF_MIN_SCALE', 'async function exactOverlaps(', 'function exactShared(', 'function exactMoved(', 'function unionArea(', 'function exchangeHeld(']) assert(!html.includes(gone), 'still in the page: ' + gone);
  assert(/<script src="charm-nest-finish-core\.js\?v=[^"]+"><\/script>/.test(html), 'the page loads the module');
  const at = html.indexOf('charm-nest-finish-core.js?v='), main = html.indexOf('CHARM NESTING STATION 1'); assert(at > 0 && at < main, 'before the page\'s own script');
});
check('4c the module reads no page global, no DOM, no network: everything comes from its host', () => {
  const code = core.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
  for (const word of ['window', 'document', 'fetch', 'localStorage', 'XMLHttpRequest', 'api\\(', 'uploadBytes', 'S\\.cloud', 'B\\.run', 'Gate', 'Sets', 'CharmNestPDF', 'CharmNestVector', 'CharmNestExport', 'CharmNestCustomPool', 'renderProgress', 'agent\\(', 'log\\(', 'toast', 'sourceOf\\(', 'stockFor\\(', 'labelOf\\(', 'sheetName\\(', 'listingsOf\\('])
    assert(!new RegExp('(?<![.\\w])' + word + '(?![\\w])').test(code.replace(/\b(const|let) \{[^}]*\} = host;/g, '')), 'the module names the page\'s ' + word.replace(/\\/g, '') + ' (not through its host)');
  assert(/root\.CharmNestFinishCore = api/.test(core) && /module\.exports = api/.test(core), 'UMD: the page and node');
});
check('4d one CHARM_SCALE and one CAREFUL_ANGLES: the module\'s are the page\'s', () => {
  const scale = +(/const CHARM_SCALE = ([\d.]+);/.exec(html) || [])[1]; assert.equal(FC.CHARM_SCALE, scale);
  const at = html.indexOf('const CAREFUL_ANGLES = '), angles = vm.runInNewContext(html.slice(at + 'const CAREFUL_ANGLES = '.length, html.indexOf(';', at))); assert.deepEqual(FC.CAREFUL_ANGLES, [...angles]);
});
check('4e buildJob in node with a node host: no page, no navigator needed', () => {
  const items = [{ id: 'p1', w: 4, h: 4, scale: 1, bits: new Uint8Array(16), areaPt2: 4, order: '1', orderDate: 9, pinned: { cxPt: 1, cyPt: 2, angle: 0 } }];
  const job = FC.buildJob({ metal: 'gold14k', placements: [], charms: items }, { stockFor: () => ({ wPt: 100, hPt: 50 }), nestItems: s => s.charms, carefulNest: () => true, angleSet: () => [0, 90], cornerPtSetting: () => 0, gapFilling: () => false, orderRank: c => c.orderDate, settings: { insetPt: 1.5, clearancePt: -.5, maxFill: .8 } });
  assert.deepEqual(job.sheet, { wPt: 100, hPt: 50, insetPt: 1.5, cornerPt: 0 }); assert.equal(job.angles.length, 180); assert.equal(job.multiStart, 3); assert.equal(job.tight, true); assert.equal(job.nearFullContact, true);
  const { bits, ...piece } = job.pieces[0]; assert.equal(bits, items[0].bits, 'the bitmap is the piece\'s own');
  assert.deepEqual(piece, { id: 'p1', w: 4, h: 4, scale: 1, areaPt2: 4, order: '1', orderDate: 9, pinned: { cxPt: 1, cyPt: 2, angle: 0 }, hold: true });
  assert.equal(job.lockedPlacements, null);
});
check('4f layoutKey: the same layout the same key, whatever the order of its placements; a moved or renamed charm a new one', () => {
  const byId = new Map([['a', { hash: 'ha', name: 'A' }], ['b', { hash: 'hb', name: 'B' }]]), p = [{ id: 'a', cxPt: 1.00001, cyPt: 2, angle: 0, scale: .971 }, { id: 'b', cxPt: 3, cyPt: 4, angle: 90 }];
  const k = FC.layoutKey({ sheetId: 's', placements: p }, byId);
  assert.equal(FC.layoutKey({ sheetId: 's', placements: p.slice().reverse() }, byId), k);
  assert.notEqual(FC.layoutKey({ sheetId: 's', placements: [{ ...p[0], cxPt: 1.01 }, p[1]] }, byId), k);
  assert.notEqual(FC.layoutKey({ sheetId: 's', placements: p }, new Map([['a', { hash: 'ha', name: 'A2' }], ['b', { hash: 'hb', name: 'B' }]])), k);
});

console.log(fails ? `FAIL finish-core: ${fails}` : 'PASS finish-core'); process.exit(fails ? 1 : 0);
