// Cut Sheet on a held 10K or 14K sheet puts that sheet in the current set first (Gate.cutInclude): the sheet's own Include, refused with
// the reason when the sheet cannot go in alone. Same harness as solid-per-sheet.cjs.
//   node tests/charm-nest/gold-cut-include.cjs
const assert = require('node:assert/strict'), fs = require('fs'), vm = require('vm'), Ops = require('../../charm-nest-operations.js'), O = require('../../charm-nest-orders.js');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8'), start = source.indexOf('const Gate ='), end = source.indexOf('/* ═══ 21', start);
const ops = Ops.create(), sets = [], saved = new Map();
const run = { runId: 'r', releasePolicy: 2, status: 'review', step: 'engrave', solidIncluded: { gold14k: true }, errors: [] };
const sheet = (n, metal = 'gold14k') => ({ metal, page: n, runId: 'r', sheetId: metal + '-' + n, draft: true, status: 'complete', outputs: { ai: 'a' }, persistedDone: true, verification: { ok: true }, placements: [{ id: 'c' + n }], charms: [{ id: 'c' + n, poolId: 'p' + n }] });
const pages = [sheet(1), sheet(2)];
const base = fs.readFileSync('tests/charm-nest/solid-per-sheet.cjs', 'utf8').split('\n').find(l => l.startsWith('const ctx='));
const ctx = vm.runInNewContext('(function(pages,run,ops,sets,saved,O){' + base.replace('const ctx=', 'return ') + '})', {})(pages, run, ops, sets, saved, O);
vm.createContext(ctx); vm.runInContext(source.slice(start, end), ctx); const Gate = ctx.window.Gate;
const inSet = () => pages.filter(p => !p.draft && p.setId === 'set').map(p => p.page);
const piece = (p, id, order) => { p.charms.push({ id, poolId: 'p' + id, order }); p.placements.push({ id }); };
(async () => {
  assert.equal(typeof Gate.cutInclude, 'function');
  await Gate.assemble(run); assert.deepEqual(inSet(), [1, 2], 'a run from before has both sheets in');
  await Gate.changeMembership('gold14k', false, pages[1]); assert.deepEqual(inSet(), [1]);
  // Sheet 2 is out; Cut Sheet puts it in, and only it
  await Gate.cutInclude(pages[1]); assert.deepEqual(inSet().sort(), [1, 2], 'pressing Cut Sheet on Sheet 2 includes Sheet 2');
  assert.equal(pages[1].solidPick, true);
  // a sheet already in: Cut Sheet is not asked to include (the page checks inSet first); including again is harmless
  await Gate.cutInclude(pages[1]); assert.deepEqual(inSet().sort(), [1, 2]);
  // an order with pieces on two sheets: the other sheet is not in, so the sheet cannot go in alone: refused, nothing changed
  await Gate.changeMembership('gold14k', false, [pages[0], pages[1]]); assert.deepEqual(inSet(), []);
  piece(pages[0], 'x1', '900/1'); piece(pages[1], 'x2', '900/2');
  await assert.rejects(() => Gate.cutInclude(pages[1]), /Not cut: .*shares an order with .*not in the set/, 'a sheet that splits an order is refused with the reason');
  assert.deepEqual(inSet(), [], 'a refused Cut Sheet changed nothing');
  // both orders' sheets are in: no split, so it goes in
  await Gate.changeMembership('gold14k', true, [pages[0]]);
  await Gate.cutInclude(pages[1]); assert.deepEqual(inSet().sort(), [1, 2]);
  // the run is finished: nothing can join
  await Gate.changeMembership('gold14k', false, [pages[0], pages[1]]);
  run.status = 'complete';
  await assert.rejects(() => Gate.cutInclude(pages[0]), /finished|Not cut/, 'a finished run takes no sheet');
  run.status = 'review';
  console.log('gold-cut-include: ok (own Include, refusal with the reason, nothing changed when refused)');
})().catch(e => { console.error(e); process.exit(1); });
