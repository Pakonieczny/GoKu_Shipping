// A sheet of a COMMITTED set that is nested again (Use this one, a partial sheet chosen for it) keeps its place in the set (Paul, 7 Oct:
// "I could not cut the sheet after placing new pieces on the partial sheet"). nextSheetSeq (charm-nest-1.html) used to draft every modern-run
// sheet out of its set at the end of every nest, so the card read "Filling" with no Set number and Cut Sheet was refused ("Not cut: Included by you").
//   node tests/charm-nest/committed-sheet-nest.cjs
const assert = require('node:assert/strict'), fs = require('fs'), vm = require('vm');
const html = fs.readFileSync('charm-nest-1.html', 'utf8'), at = html.indexOf('async function nextSheetSeq(sh) {'), end = html.indexOf('\n}\n', at) + 3;
assert(at > 0 && end > at, 'nextSheetSeq is in the page');
const sets = [{ setId: 'set-1', runId: 'run', seq: 1, day: '2026-10-07', committedAt: 1760000000000, sheetIds: ['rose-a'] }, { setId: 'set-2', runId: 'run', seq: 2, day: '2026-10-07', sheetIds: ['rose-b'] }];
const ensured = [];
const ctx = vm.createContext({
  today: () => '2026-10-08', uid: () => 'u', pagesOf: () => [],
  Sets: { ofRun: id => sets.filter(s => s.runId === id), ensure: async (runKey) => { ensured.push(runKey); return { setId: 'set-9', seq: 9, day: '2026-10-08' }; } },
  window: {},
});
vm.runInContext(`const Gate = window.Gate = { modern: id => id === 'run', solidSelected: () => false,
  committedSheet: sh => !!sh.sheetId && Sets.ofRun(sh.runId).some(set => set.committedAt && (set.sheetIds || []).includes(sh.sheetId)) };` + html.slice(at, end) + ';this.nextSheetSeq = nextSheetSeq;', ctx);
const next = ctx.nextSheetSeq;
const sheet = (id, over) => Object.assign({ metal: 'rose', runId: 'run', sheetId: id, draft: false, setId: 'set-1', seq: 1, setDay: '2026-10-07', sheetIndex: 3 }, over);
(async () => {
  // the committed set's sheet stays: same set, day, number, index; no new set is asked for
  const a = sheet('rose-a'); assert.equal(await next(a), 1, 'its set number');
  assert.deepEqual([a.draft, a.setId, a.setDay, a.sheetIndex, a.seq], [false, 'set-1', '2026-10-07', 3, 1], 'a committed set\'s sheet keeps its place');
  assert.deepEqual(ensured, [], 'no set is allocated for it');
  // a sheet of the run's current (not committed) set is drafted out at every nest, as before (the run assembles it again)
  const b = sheet('rose-b', { setId: 'set-2', seq: 2 }); assert.equal(await next(b), null);
  assert.deepEqual([b.draft, b.setId, b.setDay, b.sheetIndex], [true, null, '2026-10-08', null], 'a current set\'s sheet is held again');
  // a held sheet stays held; a sheet no committed set lists is drafted
  const c = sheet('rose-c', { draft: true, setId: null, seq: null, sheetIndex: null }); assert.equal(await next(c), null); assert.equal(c.draft, true);
  const d = sheet('rose-d'); assert.equal(await next(d), null); assert.equal(d.draft, true, 'a sheet that no committed set lists is held');
  // the committed sheet of a set whose page copy was already drafted is not the business of nextSheetSeq: it stays drafted (Cut Sheet takes it back: Gate.rejoin)
  const e = sheet('rose-a', { draft: true, setId: null }); assert.equal(await next(e), null); assert.equal(e.draft, true);
  // an old run (not the modern release rules) allocates its set the old way
  const f = sheet('rose-f', { runId: 'old', draft: true, setId: null, seq: null, sheetIndex: null });
  assert.equal(await next(f), 9); assert.deepEqual([f.draft, f.setId, f.setDay, f.sheetIndex, ensured.length], [true, 'set-9', '2026-10-08', 1, 1], 'a sheet of an older run is put in its set the old way');
  console.log('committed-sheet-nest: ok (a committed set\'s sheet keeps its place when nested again; others are held as before)');
})().catch(e => { console.error(e); process.exit(1); });
