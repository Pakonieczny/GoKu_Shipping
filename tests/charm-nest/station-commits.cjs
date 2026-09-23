// The Design Station's memory of the sorter's commits, and its undo: a retried commit is answered from memory, an undo
// reopens only the orders the sorter names, and an undone order can be committed again.
const assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const html = fs.readFileSync(require('node:path').join(__dirname, '../../design-1.html'), 'utf8');
const cut = (from, to) => { const a = html.indexOf(from), b = html.indexOf(to, a); assert(a >= 0 && b > a, `missing ${from}`); return html.slice(a, b); };
const memory = cut('  const readJson = (k, d) =>', '  function rememberSorterSelection()');
const undo = cut('    async "complete.undo"(a) {', '    async release()');
const store = new Map();
const c = vm.createContext({ assert, console, localStorage: { getItem: k => store.has(k) ? store.get(k) : null, setItem: (k, v) => store.set(k, String(v)) } });
vm.runInContext(`
  const LSK = { commits: 'commits' };
  let lastClearedReceipts = [], __commitInFlight = 0; const undone = [];
  const completedOrders = new Set(['a', 'b', 'x']), selectedOrders = new Set();
  const Cursor = { act: async () => {} };
  const handleUndoComplete = async () => { for (const id of lastClearedReceipts) { completedOrders.delete(id); undone.push(id); } lastClearedReceipts = []; };
  ${memory}
  const cmds = { ${undo} };
`, c);
(async () => {
  await vm.runInContext(`(async () => {
    rememberCommit('r1', ['a', 'b'], { completed: ['a', 'b'], refused: [] });
    assert.deepEqual(priorCommit('r1', ['a', 'b']).completed, ['a', 'b'], 'a retried commit is answered from memory');
    // the last batch completed here by hand belongs to someone else
    lastClearedReceipts = ['x'];
    await cmds['complete.undo']({ receiptIds: ['a'] });
    assert.deepEqual(undone, ['a'], 'only the named order reopens, not the last batch completed here');
    assert(completedOrders.has('b') && completedOrders.has('x'));
    assert.equal(priorCommit('r1', ['a']), null, 'an undone order is committed again instead of replaying its old result');
    assert.deepEqual(priorCommit('r1', ['b']).completed, ['b'], 'the rest of the run is still remembered');
    lastClearedReceipts = ['x'];
    await assert.rejects(cmds['complete.undo']({ receiptIds: ['not-complete'] }), /nothing to undo/, 'naming orders that are not complete here reopens nothing');
    assert.deepEqual(undone, ['a']); assert(completedOrders.has('x'), 'and never falls back to the last batch completed here');
  })()`, c);
  console.log('Station commits OK: a retried commit is answered from memory, an undo reopens only the named orders, and an undone order commits again');
})().catch(e => { console.error(e); process.exitCode = 1; });
