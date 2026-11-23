// STUCKDIRTY (Paul, 22 Nov 2026: "I have an error in this set": "Set 2 breaks your rules: GF Draft 1 shares an order with a sheet of this set but is not in it").
// GF Draft 1 (gold-s19-mwpqrju5) was written on 21 Nov 21:44 released full AND dirty (a piece left it while it was nested). A sheet released full is never nested again for a change, so nothing
// cleared the mark: the cloud's set assembly read it as "still being nested or saved" for good and formed Set 2 at 17:17 without it, although it shares order 4240577089 with SS Sheet 1.
// (Replayed offline on the cloud's records: as recorded the assembly forms GF Sheet 1 + SS Sheet 1, policy "Nest and verify first" for gold-s19; with that mark off it forms all three.)
//   1  dirtyAfter: a sheet written released full (by this write, or already) is written not dirty; any other sheet keeps the mark it had
//   2  the writer's record write (S7) carries dirtyAfter's answer, after the after-nest marks (the release) are in
//   3  the assembly still refuses a dirty sheet, and takes the same sheet once the mark is off (the real charm-nest-orders.js sheetRelease)
//   node tests/charm-nest/stuck-dirty.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const F = require('./_cloudFakes.cjs')({ now: Date.UTC(2026, 10, 22, 22, 0, 0) });
const root = path.join(__dirname, '../..');
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 600))); if (!c) failed++; };
const quiet = console.warn; console.warn = () => {};
const Finish = F.load('_charmNestFinish.js');
const d = Finish.dirtyAfter;
ok('1 · a sheet this write releases full is written not dirty (a piece left it while it was nested: its room stays free, the layout written is the sheet)', d({ dirty: true, releaseFull: true }, { releaseFull: false }) === false);
ok('1 · a sheet already released full (the write does not say) is written not dirty', d({ dirty: true }, { releaseFull: true }) === false);
ok('1 · a sheet not released keeps its mark: it is arranged again next, as before', d({ dirty: true, releaseFull: false }, { releaseFull: false }) === true && d({ dirty: true }, {}) === true);
ok('1 · a clean sheet stays clean', d({ dirty: false, releaseFull: true }, {}) === false && d({ dirty: false }, {}) === false);
const src = fs.readFileSync(path.join(root, 'netlify/functions/_charmNestFinish.js'), 'utf8');
const marks = src.indexOf('Object.assign(sheet, await afterNestMarks('), set = src.indexOf('sheet.dirty = dirtyAfter(sheet, rec);'), put = src.indexOf('const ans = await X.call(Object.assign({ op: "putSheet", sheet,');
ok('2 · the record write (S7) carries dirtyAfter, after the release marks are in and before the sheet is written', marks > 0 && set > marks && put > set, { marks, set, put });
const O = require(path.join(root, 'charm-nest-orders.js'));
const rel = typeof O.sheetRelease === 'function' ? O.sheetRelease : null;
if (rel) {
  const base = { sheetId: 'gold-s19', material: 'gold', verified: true, placed: 71, full: true };   // (the assembly's view of a page: charm-nest-assembly-core.js asPage)
  const dirty = rel(Object.assign({}, base, { dirty: true })), clean = rel(Object.assign({}, base, { dirty: false }));
  ok('3 · the assembly refuses the released sheet while it is dirty, and takes it once the mark is off', JSON.stringify(dirty).includes('Nest and verify first') && !JSON.stringify(clean).includes('Nest and verify first'), { dirty, clean });
} else console.log('  - charm-nest-orders.js has no sheetRelease export: part 3 is covered by the offline replay only');
console.warn = quiet;
console.log(failed ? `FAIL stuck-dirty (${failed})` : 'PASS stuck-dirty'); process.exit(failed ? 1 : 0);
