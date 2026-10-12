// The page's own Sets.ensure / allocate (charm-nest-bridge.js) when a set has FORMED (Paul, 12 Oct 2026): the next completed sheets form the NEXT set, which is numbered "after" the formed set
// (committed or not), the formed set keeps its place in the run beside the new one, and nothing is renumbered. The real code over fakes: no cloud, no browser.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const O = require('../../charm-nest-orders.js');
const code = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const at = code.indexOf('const Sets = window.Sets = (() => {'), upto = code.indexOf('const ofRun = runId =>', at), end = code.indexOf('\n', upto);
assert(at > 0 && upto > at, 'the Sets code to test was found');
const calls = [], saved = []; let counter = 0;
const ctx = vm.createContext({ O, calls, saved, B: { sets: new Map(), run: null }, S: { cloud: { ok: true } }, window: {}, today: () => '2026-10-12', labelOf: m => m, agent: () => {}, RunCtl: { renderBanner() {} },
  api: async (name, b) => { calls.push(b); if (b.op === 'setAllocate') { const seq = ++counter; return { ok: true, setId: 'set-' + seq, seq, day: b.day }; } return {}; } });
vm.runInContext(code.slice(at, end) + `
  async function save(s) { saved.push(s.setId + ':' + (s.sheetIds || []).join(',')); }
  globalThis.SetsUnderTest = { ensure, ofRun, byRun, keyOf };
})();`, ctx);
const T = ctx.SetsUnderTest, RUN = 'run-x';
const allocations = () => calls.filter(c => c.op === 'setAllocate');
(async () => {
  // 1 · the run's first set
  const s1 = await T.ensure(RUN, 'dispatch', {});
  assert.deepEqual([s1.setId, s1.seq, s1.name], ['set-1', 1, 'Set-1']); assert.equal(allocations()[0].after, null);
  assert.equal((await T.ensure(RUN, 'dispatch', {})).setId, 'set-1', 'asked again: the same set (idempotent)'); assert.equal(allocations().length, 1);
  // 2 · Set-1 has formed (it holds sheets): the next completed sheets ask for the set that follows it. Set-1 is saved first, keeps its place beside the new one
  s1.sheetIds.push('gf-1', 'ss-1');
  const s2 = await T.ensure(RUN, 'dispatch', { follow: s1 });
  assert.deepEqual([s2.setId, s2.seq], ['set-2', 2]); assert.equal(allocations()[1].after, 'set-1', 'numbered after the formed set');
  assert.deepEqual(Array.from(saved), ['set-1:gf-1,ss-1'], 'the formed set is on the cloud before the next one is numbered after it');
  assert.deepEqual(Array.from(T.ofRun(RUN), x => x.setId), ['set-1', 'set-2'], 'the run holds both, in set order'); assert.equal(T.byRun().get(T.keyOf(RUN, 'dispatch')), s2); assert.equal(T.byRun().get(T.keyOf(RUN, 'closed:set-1')), s1);
  assert.equal((await T.ensure(RUN, 'dispatch', {})).setId, 'set-2', 'without naming a set, the newest open set is what comes back');
  assert.equal((await T.ensure(RUN, 'dispatch', { follow: s1 })).setId, 'set-2', 'a page that asks again for the set after Set-1 is given Set-2 (the server is idempotent per "after" set as well)');
  // 3 · and the one after that
  s2.sheetIds.push('gf-2', 'ss-2');
  const s3 = await T.ensure(RUN, 'dispatch', { follow: s2 });
  assert.deepEqual([s3.setId, s3.seq, allocations()[allocations().length - 1].after], ['set-3', 3, 'set-2']);
  assert.deepEqual(Array.from(T.ofRun(RUN), x => x.setId), ['set-1', 'set-2', 'set-3']);
  // 4 · the newest is committed: the next set follows the committed one, as it always did
  s3.sheetIds.push('gf-3'); s3.committedAt = 5; s3.savedCommit = 5;
  const s4 = await T.ensure(RUN, 'dispatch', {});
  assert.deepEqual([s4.setId, s4.seq, allocations()[allocations().length - 1].after], ['set-4', 4, 'set-3']);
  assert.equal(T.byRun().get(T.keyOf(RUN, 'committed:set-3')), s3); assert.deepEqual(Array.from(T.ofRun(RUN), x => x.seq), [1, 2, 3, 4]);
  console.log('PASS: sets-allocate-follow: a formed set keeps its place and is saved before the set that follows it is numbered after it; nothing is renumbered; a committed set is followed as before');
})().catch(e => { console.error(e); process.exit(1); });
