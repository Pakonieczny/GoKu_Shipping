// OWEDFILES (Paul, 9 Nov 2026): a change by hand (a charm moved between 10K / 14K sheets with Manual Control still on) is saved to the sheets' records at once, and their files (the
// sheet's .ai, proof, QR label, a released set's files) are written when Manual Control is turned off. A page closed before that left the record saying filesOwed for good: nothing read it.
// Now a reload of the run (restoreRunSheets) writes such a sheet as it stands, once the page has settled (writeOwedLater: the sheet window's writeOwed). Node only (the bridge's
// restoreRunSheets in a stub world, as adv-restore-size.cjs).  node tests/charm-nest/owed-files-restore.cjs
//   s1  in a set, its record owes its files                     → written as it stands
//   s2  owes its files, but LASER CUT                           → never written
//   s3  owes nothing                                            → not written
//   s4  owes its files, but a draft (nested again on restore)   → not written by this
//   s5  owes its files, busy when first looked at               → waits, written once it is free
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const code = fs.readFileSync(process.env.BRIDGE || path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const from = code.indexOf('  function writeOwedLater(pages, runId, ms, tries)'), to = code.indexOf('  function onComplete(r)', from);
assert(from > 0 && to > from, 'writeOwedLater and restoreRunSheets found');
const pf = code.indexOf('function placedOfRecord(d) {'), pt = code.indexOf('\n}\n', pf) + 3;   // (the page's own reading of a saved sheet's pieces, which restoreRunSheets asks)
assert(pf > 0, 'placedOfRecord found');

(async () => {
  const ctx = vm.createContext({ assert, console, CharmNestPoolPieces: require('../../charm-nest-pool-pieces.js') });
  vm.runInContext(`
    const timers = [], setTimeout = (fn, ms) => { timers.push({ fn, ms }); return timers.length; };
    const written = [], logs = [];
    const window = { CharmNestPoolPieces, SheetWin: { hopKit: { writeOwed: async list => { written.push(list.map(p => p.sheetId)); return true; } } } };
    const stockFor = () => ({ wPt: 340, hPt: 130 });
    const pages = [], mk = () => { const p = { page: pages.length + 1, charms: [], placements: [], backPool: [] }; pages.push(p); return p; };
    const S = { sheets: { gold14k: { pages } } }, B = { pool: { rows: new Map() } };
    mk(); const addPage = () => mk(); const allSheets = () => pages, sheetName = p => p.fileBase;
    const rec = (id, o) => Object.assign({ id, metal: 'gold14k', draft: false, setId: 'set1', day: '2026-11-09', fileBase: '14K Sheet ' + id.slice(1), status: 'complete', verification: { ok: true },
      poolIds: ['p-' + id], placements: [{ id: 'c-' + id, cxPt: 20, cyPt: 20 }], charms: [{ id: 'c-' + id, poolId: 'p-' + id, name: id }] }, o || {});
    const records = { s1: rec('s1', { filesOwed: 1 }), s2: rec('s2', { filesOwed: 1, laserDoneAt: 5 }), s3: rec('s3'), s4: rec('s4', { filesOwed: 1, draft: true, setId: null }), s5: rec('s5', { filesOwed: 1 }) };
    const api = async (n, b) => b.op === 'setGet' ? { set: { setId: 'set1', seq: 1, day: '2026-11-09', orders: {} } } : b.op === 'listSheets' ? { sheets: Object.keys(records).map(id => ({ id })) } : b.op === 'getSheet' ? { sheet: records[b.id] }
      : b.op === 'poolGet' ? { pools: Object.fromEntries(b.poolIds.map(id => [id, { poolId: id, state: 'written', setId: 'set1', sku: 'A', orderId: 'o-' + id }])) } : {};
    const rows = Object.keys(records).map(id => ({ state: 'written', poolIds: ['p-' + id] }));
    const Orders = { rows: () => rows }, O = { setLabel: s => 'Set ' + s, setFolder: () => 'f' };
    const byRun = new Map(), Sets = { byRun: () => byRun, keyOf: (a, b) => a + ':' + b };
    const Master = { entryFor: () => ({}) }, Pool = { masterCharm: async () => ({ id: 'src', charms: [{ sourceId: 'src' }] }), ensureBase: async src => src, cloneCharm: (c, id) => ({ ...c, id }), charmOf: id => pages.some(p => p.charms.some(c => c.poolId === id)) };
    const Engrave = { items: () => new Map(), ensureJob: () => ({ backs: [] }) };
    const Gate = { leftSetOf: () => null };
    const computeSaturation = () => {}, renderCard = () => {}, agent = (o, kind, text) => logs.push([kind, text]), sheetDirty = p => { p.dirty = true; };
    globalThis.T = { timers, written, logs, pages };
  ` + code.slice(pf, pt) + code.slice(from, to), ctx);
  await vm.runInContext(`restoreRunSheets({ runId: 'run1', setIds: ['set1'] })`, ctx);
  const T = ctx.T, pg = id => T.pages.find(p => p.sheetId === id);
  assert.equal(T.pages.length, 5, 'five sheets restored');
  assert.equal(T.timers.length, 1, 'one look is scheduled for the owed files');
  assert(T.timers[0].ms >= 10000, 'once the page has settled (not at once): ' + T.timers[0].ms);
  assert.equal(T.written.length, 0, 'nothing is written during the restore itself');
  // s5 is busy (its nest running) when first looked at
  pg('s5').status = 'nesting';
  await T.timers.shift().fn(); await new Promise(r => setImmediate(r));
  assert.equal(JSON.stringify(T.written), JSON.stringify([['s1']]), 'the sheet in a set that owes its files is written as it stands; the laser-cut, the draft and the one owing nothing are not: ' + JSON.stringify(T.written));
  assert(T.logs.some(([k, t]) => k === 'cloud' && /^14K Sheet 1: its last change by hand was saved, but the page closed before its files were written; written again as it stands/.test(t)), 'the log says so: ' + JSON.stringify(T.logs));
  assert.equal(T.timers.length, 1, 'the busy sheet is looked at again');
  console.log('  ✓ reload: a sheet whose record still owes its files is written as it stands once the page has settled; a laser-cut sheet, a draft and a sheet owing nothing are not; the log says so');
  pg('s5').status = 'complete';
  await T.timers.shift().fn(); await new Promise(r => setImmediate(r));
  assert.equal(JSON.stringify(T.written), JSON.stringify([['s1'], ['s5']]), 'the busy sheet is written once it is free: ' + JSON.stringify(T.written));
  console.log('  ✓ a sheet busy when first looked at waits, and is written once it is free');
  console.log('owed-files-restore: all passed');
})().catch(e => { console.error(e); process.exit(1); });
