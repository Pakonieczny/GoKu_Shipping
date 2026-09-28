// A 10K/14K sheet's own size after a reload of its run (restoreRunSheets): a cut Sheet 2 that Apply size left at 96 mm
// (keptStock) came back at the metal's new 120 mm, so the card and the sheet window drew it a fifth empty. It now comes
// back at the size its record was nested at; a sheet at the metal's size now takes the metal's size, as before.
// Node only (the bridge's restoreRunSheets in a stub world).  node tests/charm-nest/adv-restore-size.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const code = fs.readFileSync(process.env.BRIDGE || path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const from = code.indexOf('  async function restoreRunSheets(rec)'), restore = code.slice(from, code.indexOf('  function onComplete(r)', from));
assert(from > 0, 'restoreRunSheets found');

(async () => {
  const PT = 72 / 25.4, mm = pt => Math.round(pt / PT * 100) / 100;
  const ctx = vm.createContext({ assert, console });
  vm.runInContext(`
    const settings = { gold14k: [120 / 25.4, 46 / 25.4] }, window = {};
    const stockFor = (metal, sheet) => { const k = sheet && sheet.keptStock; if (k && k.wPt && k.hPt) return { wPt: k.wPt, hPt: k.hPt }; const i = settings[metal]; return { wIn: i[0], hIn: i[1], wPt: i[0] * 72, hPt: i[1] * 72 }; };
    const pages = [], mk = () => { const p = { page: pages.length + 1, charms: [], placements: [], backPool: [] }; pages.push(p); return p; };
    const S = { sheets: { gold14k: { pages } } }, B = { pool: { rows: new Map() } };
    mk(); const addPage = () => mk();
    const size = mm => ({ wIn: mm / 25.4, hIn: 46 / 25.4, wPt: mm / 25.4 * 72, hPt: 46 / 25.4 * 72 });
    const rec = (id, w, cut) => ({ id, metal: 'gold14k', draft: false, setId: 'set1', day: '2026-09-28', fileBase: id, status: 'complete', verification: { ok: true }, laserDoneAt: cut ? 1 : null, stock: size(w),
      poolIds: ['p-' + id], placements: [{ id: 'c-' + id, cxPt: 20, cyPt: 20 }], charms: [{ id: 'c-' + id, poolId: 'p-' + id, name: id }] });
    const records = { s1: rec('s1', 120, false), s2: rec('s2', 96, true) };
    const api = async (n, b) => b.op === 'setGet' ? { set: { setId: 'set1', seq: 1, day: '2026-09-28', orders: {} } } : b.op === 'listSheets' ? { sheets: [{ id: 's1' }, { id: 's2' }] } : b.op === 'getSheet' ? { sheet: records[b.id] }
      : b.op === 'poolGet' ? { pools: Object.fromEntries(b.poolIds.map(id => [id, { poolId: id, state: 'written', setId: 'set1', sku: 'A', orderId: 'o-' + id }])) } : {};
    const rows = [{ state: 'written', poolIds: ['p-s1'] }, { state: 'written', poolIds: ['p-s2'] }];
    const Orders = { rows: () => rows }, O = { setLabel: s => 'Set ' + s, setFolder: () => 'f' };
    const byRun = new Map(), Sets = { byRun: () => byRun, keyOf: (a, b) => a + ':' + b };
    const Master = { entryFor: () => ({}) }, Pool = { masterCharm: async () => ({ id: 'src', charms: [{ sourceId: 'src' }] }), cloneCharm: (c, id) => ({ ...c, id }), charmOf: id => pages.some(p => p.charms.some(c => c.poolId === id)) };
    const Engrave = { items: () => new Map(), ensureJob: () => ({ backs: [] }) };
    const computeSaturation = () => {}, renderCard = () => {}, agent = () => {}, sheetDirty = p => { delete p.keptStock; p.dirty = true; };
  ` + restore, ctx);
  await vm.runInContext(`(async () => {
    await restoreRunSheets({ runId: 'run1', setIds: ['set1'] });
    const [a, b] = pages;
    globalThis.out = { n: pages.length, a: stockFor('gold14k', a), b: stockFor('gold14k', b), aKept: !!a.keptStock, bIds: JSON.stringify(b.placements.map(p => p.id)) };
  })()`, ctx);
  const o = ctx.out;
  assert.equal(o.n, 2, 'both sheets restored');
  assert.equal(o.bIds, '["src:p-s2"]', 'the cut sheet keeps its layout');
  assert.deepEqual([mm(o.b.wPt), mm(o.b.hPt)], [96, 46], 'the cut Sheet 2 comes back at its own 96 × 46 mm, not ' + mm(o.b.wPt) + ' mm wide');
  assert.equal(o.aKept, false, 'Sheet 1, at the metal size, keeps none of its own');
  assert.deepEqual([mm(o.a.wPt), mm(o.a.hPt)], [120, 46], 'Sheet 1 takes the metal size');
  console.log('  ✓ reload: the cut 14K Sheet 2 comes back at its own 96 × 46 mm; Sheet 1 takes the metal size (120 mm)');
  console.log('adv-restore-size: all passed');
})().catch(e => { console.error(e); process.exit(1); });
