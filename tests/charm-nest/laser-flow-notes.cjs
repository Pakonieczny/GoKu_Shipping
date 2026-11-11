/* The laser cutting → Completed audit (Paul, 11 Nov 2026: "verify for me that the site when they go to laser that they actually appear correctly there there's nothing
 * blocking them. Everything gets recorded correctly in the cloud ... and how the users is informed of what is happening to each sheet"), its three fixes:
 *   1. MARKSTALE  a mark made on this page gives way to a record written after it that says otherwise (another computer reopened or completed it): it held 12 hours,
 *                 and a set completed here and reopened elsewhere showed in neither Current nor Completed.
 *   2. LASEROFF   marking with the cloud away (or with no name given) says so on screen; it only flashed the spinner.
 *   3. LASERNOTE  a set or sheet whose Approve button is green (nothing hard in its way) but which is not in Laser cutting says what is still to come beside the button.
 * Offline: charm-nest-readiness.js as is; the Library's own functions sliced out of charm-nest-library.js and run in a vm. No network, Firestore or paid call.
 *   node tests/charm-nest/laser-flow-notes.cjs                                                                                                                        */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const ROOT = path.join(__dirname, '../..'), R = require(path.join(ROOT, 'charm-nest-readiness.js'));
const lib = fs.readFileSync(path.join(ROOT, 'charm-nest-library.js'), 'utf8'), bridge = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
const slice = (src, a, b) => { const i = src.indexOf(a), j = src.indexOf(b, i + 1); assert(i >= 0 && j > i, 'slice ' + a); return src.slice(i, j); };
const clone = x => JSON.parse(JSON.stringify(x));

(async () => {
  /* 1. MARKSTALE */
  {
    const ctx = { L: { marks: new Map() } };
    vm.createContext(ctx);
    vm.runInContext(slice(lib, '  const keyOf = r =>', '  /** The cards of Current found once') + '\nthis.isDone = isDone; this.isFiled = isFiled;', ctx);
    vm.runInContext(slice(lib, '  function note(key', '\n') + '\nthis.note = note;', ctx);
    const T = 1_000_000_000_000, set = { setId: 's2', sheetIds: ['a', 'b'] };
    // completed here: the cloud answered at T; the page's copy of the set is older and says nothing yet
    ctx.note('set:s2', true, T, 'Ana', true);
    assert.equal(ctx.isDone({ ...set, updatedAt: T - 60_000 }), true, 'a record older than the mark: the mark stands in for it');
    assert.equal(ctx.isDone({ ...set, laserDoneAt: T, updatedAt: T + 50 }), true, 'the record caught up: the same answer');
    // reopened on another computer a minute later: the set's record says not done, written after the mark
    assert.equal(ctx.isDone({ ...set, updatedAt: T + 60_000 }), false, 'a newer record that says otherwise wins');
    assert.equal(ctx.L.marks.has('set:s2'), false, 'and the mark goes');
    // a mark timed by this computer's clock (an undo answers no `at`) leaves room for the two clocks
    ctx.note('sheet:a', false, T, 'Ana', false);
    assert.equal(ctx.isDone({ id: 'a', laserDoneAt: T - 1, updatedAt: T + 2000 }), false, 'within a few seconds: the mark stands');
    assert.equal(ctx.isDone({ id: 'a', laserDoneAt: T + 30_000, updatedAt: T + 30_000 }), true, 'completed again elsewhere half a minute later: the record wins');
    // a sheet filed with its set: the set's own last write (the row's _setRev) counts for its set's mark
    ctx.note('set:s3', true, T, 'Ana', true);
    const sheet = { id: 'c', setId: 's3', laserDoneAt: T - 5, updatedAt: T - 5 };
    assert.equal(ctx.isFiled({ ...sheet, laserSetPending: true }), true, 'its set marked done here: filed, while its row is older');
    assert.equal(ctx.isFiled({ ...sheet, laserSetPending: true, _setRev: `${Math.floor((T + 90_000) / 1000)}.0` }), false, 'its set reopened since: back with its set');
    assert(/12 \* 3600000/.test(lib), '(the 12-hour sweep of old marks stays)');
  }

  /* 2. LASEROFF: no cloud, or no name: a toast says why nothing was marked */
  {
    const toasts = [];
    const ctx = {
      L: { busy: new Set(), marks: new Map(), presses: new Set(), lists: new Map() }, S: { cloud: { ok: false } }, window: { CNEmployee: { name: () => 'Ana' } },
      toast: (m, k) => toasts.push([m, k]), canComplete: () => true, setSheets: () => ['a'], nameOf: () => 'Set 2', api: async () => { throw new Error('no call expected'); }, console,
    };
    vm.createContext(ctx);
    vm.runInContext(slice(lib, '  async function mark(kind', '  /** "Sheet 2 marked completed') + '\nthis.mark = mark;', ctx);
    await assert.rejects(ctx.mark('set', 's2', true), /cloud is offline/);
    assert.equal(toasts.length, 1, 'one message on screen');
    assert.match(toasts[0][0], /^Not marked completed: the cloud is offline\. Nothing was changed/);
    assert.equal(toasts[0][1], 'bad');
    await assert.rejects(ctx.mark('sheet', 'a', false), /cloud is offline/);
    assert.match(toasts[1][0], /^Not moved back: the cloud is offline/);
    ctx.S.cloud.ok = true; ctx.window.CNEmployee = { name: () => '', need: async () => '' };
    await assert.rejects(ctx.mark('set', 's2', true), /say who marked it/);
    assert.match(toasts[2][0], /say who marked it/, 'no name given: said too');
    assert.equal(ctx.L.busy.size, 0, 'nothing left busy');
  }

  /* 3. LASERNOTE: what is still to come, beside a green button */
  {
    const back = id => ({ poolId: id, sheetId: 'sheet1', approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' } } });
    const ready = { id: 'sheet1', metal: 'gold', setId: 'set1', runId: 'run1', status: 'complete', poolIds: ['copy1', 'copy2'], placedCount: 2, verification: { ok: true }, outputs: { ai: { url: 'https://example.com/front.ai' }, preview: { url: 'https://example.com/front.png' } }, orders: ['order1'], orderReadiness: { order1: { ready: true } }, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'order1', orders: ['order1'] }] }, backPool: [back('copy1'), back('copy2')] };
    const gap = mutate => { const s = clone(ready); mutate(s); return R.softGap(s); };
    assert.equal(R.laserSheet(ready).ready, true);
    assert.equal(R.softGap(ready), '', 'a ready sheet has nothing to come');
    const cases = [
      [s => { delete s.verification; }, 'layout not checked yet'],
      [s => { s.verification.ok = false; }, 'the layout check found a problem'],
      [s => { s.outputs = {}; }, 'cutting file not saved yet'],
      [s => { s.backPool[1].outputs.ai.path = null; }, 'saving back files: 1 of 2'],
      [s => { s.label.files = []; }, 'QR label not made yet'],
      [s => { s.orderReadiness.order1 = { ready: false }; }, 'an order waits for its other pieces'],
      [s => { s.laserHold = { at: 5, by: 'Ana' }; }, 'held back (Approve lifts the hold)'],
    ];
    for (const [mutate, words] of cases) {
      const s = clone(ready); mutate(s);
      assert.equal(R.approveBlock(s), null, `${words}: nothing hard in the way, so the button stays green`);
      assert.equal(R.laserSheet(s).ready, false, `${words}: and it is not in Laser cutting`);
      assert.equal(gap(mutate), words, 'the line says what is still to come');
    }
    // what approveBlock already says is never repeated: back engravings waiting, a draft, a layout being made, a sheet cut already
    assert.equal(gap(s => { s.backPool = []; }), '', 'engravings waiting: the grey reason says it');
    assert.equal(gap(s => { s.draft = true; }), '');
    assert.equal(gap(s => { s.status = 'nesting'; }), '');
    assert.equal(gap(s => { s.laserDoneAt = 9; delete s.verification; }), '');
    // a set: the first sheet still on its way, in the set's own order, and how many more
    const as = (id, metal) => { const x = clone(ready); x.id = id; x.metal = metal; for (const bk of x.backPool) bk.sheetId = id; return x; };   // (a back belongs to its own sheet)
    const b = as('sheet2', 'silver'); b.outputs = {};
    const c = as('sheet3', 'silver'); delete c.verification;
    const st = { setId: 'set1', sheetIds: ['sheet1', 'sheet2', 'sheet3'], seq: 2, day: '2026-11-10' };
    const n = R.softNote(st, [ready, b, c]);
    assert.equal(n.sheetId, 'sheet2');
    assert.match(n.text, /^SS Sheet .* · cutting file not saved yet, and 1 more$/);
    assert.equal(R.softNote(st, [ready, as('sheet2', 'silver'), as('sheet3', 'silver')]), null, 'a set all ready: no note');
    // the page: the green button carries it, and draws it as "Still to come: …"
    assert(/if\(!b\)\{const g=R\.softGap\?R\.softGap\(p\):'';return g\?\{why:'',note:/.test(bridge), 'a lone sheet (approveCase)');
    assert(/if\(gate\.ready\)\{const n=R\.softNote\?R\.softNote\(st,recs\.map\(projected\)\):null;/.test(bridge), 'a set (setCase)');
    assert(/const note=mode==='ready' && c\?\.note\?`Still to come: \$\{c\.note\}`:'';/.test(bridge), 'drawn beside the green button (paintApprove)');
    assert(/c\?\.note \|\| ''\]\.join\('\|'\)/.test(bridge), 'a changed note is drawn again');
  }

  console.log('Laser flow notes OK: a newer record beats a stale mark here, offline marking says so, and a green Approve on a card still In progress says what is still to come');
})().catch(e => { console.error(e); process.exitCode = 1; });
