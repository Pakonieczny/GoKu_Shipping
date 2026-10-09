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
  // the same split where the two pieces are a Left and a Right earring: the reason says so (Paul, 9 Oct, pairs in sets)
  pages[0].charms.find(c => c.id === 'x1').side = 'L'; pages[1].charms.find(c => c.id === 'x2').side = 'R';
  await assert.rejects(() => Gate.cutInclude(pages[1]), /Not cut: .*shares the left and right earrings of an order with .*not in the set/, 'a split pair is named');
  delete pages[0].charms.find(c => c.id === 'x1').side; delete pages[1].charms.find(c => c.id === 'x2').side;
  // both orders' sheets are in: no split, so it goes in
  await Gate.changeMembership('gold14k', true, [pages[0]]);
  await Gate.cutInclude(pages[1]); assert.deepEqual(inSet().sort(), [1, 2]);
  // the run is finished: nothing can join
  await Gate.changeMembership('gold14k', false, [pages[0], pages[1]]);
  run.status = 'complete';
  await assert.rejects(() => Gate.cutInclude(pages[0]), /finished|Not cut/, 'a finished run takes no sheet');
  run.status = 'review';
  // A sheet of a COMMITTED set whose page copy lost its place in it (Paul, 7 Oct: a partial sheet was chosen for it and it was nested again, so the page drafted it out of its set):
  // Gate.rejoin takes the place back from the set and the saved record, writing nothing; an Include cannot, the run leaves committed sheets alone.
  const set = sets[0]; set.sheetIds = ['gold14k-1', 'gold14k-2']; set.committedAt = Date.now(); set.seq = 3;
  assert.equal(typeof Gate.rejoin, 'function'); assert.equal(typeof Gate.committedSheet, 'function');
  assert.equal(Gate.committedSheet(pages[0]), true, 'a sheet of a committed set is one'); assert.equal(Gate.committedSheet({ ...pages[0], sheetId: 'other' }), false);
  const reads = [], real = ctx.api, lost = () => Object.assign(pages[0], { draft: true, setId: null, seq: null, sheetIndex: null, fileBase: 'working' });
  const saved0 = JSON.stringify([...saved.keys()]);
  ctx.api = async (name, body) => { reads.push(name + ':' + body.op); if (body.op === 'getSheet') return { sheet: { id: body.id, setId: 'set', draft: false, sheetIndex: 2 } }; throw new Error('rejoin writes nothing: ' + body.op); };
  lost(); assert.equal(await Gate.rejoin(pages[0]), true, 'the committed sheet takes its place back');
  assert.deepEqual([pages[0].draft, pages[0].setId, pages[0].seq, pages[0].sheetIndex, pages[0].fileBase], [false, 'set', 3, 2, 'Set-1-Sheet-1'], 'its set, number, index and file name');
  assert.deepEqual(reads, ['charmNestLibrary:getSheet'], 'one read, no write'); assert.equal(JSON.stringify([...saved.keys()]), saved0, 'nothing was saved');
  // nothing to take back: the saved record says it is a draft, or sits in another set; no committed set lists it; it is recalled; the cloud is off
  ctx.api = async (name, body) => ({ sheet: { id: body.id, setId: 'set', draft: true } }); lost(); assert.equal(await Gate.rejoin(pages[0]), false, 'a saved draft stays a draft'); assert.equal(pages[0].draft, true);
  ctx.api = async (name, body) => ({ sheet: { id: body.id, setId: 'elsewhere', draft: false } }); assert.equal(await Gate.rejoin(pages[0]), false, 'another set is not this one');
  ctx.api = async () => { throw new Error('must not read'); };
  assert.equal(await Gate.rejoin(null), false); assert.equal(await Gate.rejoin({ ...pages[0], sheetId: 'unlisted', draft: true, setId: null }), false, 'no committed set lists it');
  assert.equal(await Gate.rejoin({ ...pages[0], recalled: { roseStockId: 'x' } }), false, 'a recalled sheet');
  assert.equal(await Gate.rejoin({ ...pages[0], draft: false, setId: 'set' }), false, 'a sheet already in its set');
  ctx.S.cloud.ok = false; assert.equal(await Gate.rejoin(pages[0]), false, 'the cloud is off'); ctx.S.cloud.ok = true;
  set.committedAt = 0; ctx.api = async (name, body) => ({ sheet: { id: body.id, setId: 'set', draft: false } }); assert.equal(await Gate.rejoin(pages[0]), false, 'an uncommitted set is not rejoined (its sheets join by Include)');
  ctx.api = real;
  console.log('gold-cut-include: ok (own Include, refusal with the reason, nothing changed when refused; a committed set sheet takes its place back, reading only)');
})().catch(e => { console.error(e); process.exit(1); });
