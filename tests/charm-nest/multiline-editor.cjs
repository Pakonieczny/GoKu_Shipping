const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const listeners = {}, timers = new Map(), use = {}, approveButton = {}, calls = [];
const ta = { value: 'First line\nSecond line\nThird line', selectionStart: 12, selectionEnd: 12,
  addEventListener: (name, fn) => { listeners[name] = fn; }, focus() { calls.push('focus'); },
  setSelectionRange(start, end) { calls.push([start, end]); } };
const card = { isConnected: true, querySelector: q => q.includes('usewords') ? use : ta,
  querySelectorAll: () => [approveButton] };
const job = { key: 'copy', editingBack: false, lineMode: '3', lines: ['Old words'], text: 'Old words', row: {} };
const context = vm.createContext({ card, job, wordsJob: false, document: { activeElement: ta }, EG: { card, cardKey: 'copy' },
  setTimeout: fn => { const id = timers.size + 1; timers.set(id, fn); return id; }, clearTimeout: id => timers.delete(id),
  toast: text => calls.push(text), render: () => {},
  fitJob: async j => { calls.push('fit'); j.fit = { ok: true }; j.verify = { geometry: { ok: true } }; j.lines = j.lineInput.slice(); },
  employeeName: () => null, askEmployee: () => null });
const start = source.indexOf('    const ta = card.querySelector(\'[data-f="words"]\'), use =');
const end = source.indexOf('    // the back preview', start);
assert(start > 0 && end > start);
vm.runInContext(source.slice(start, end), context);
const a = source.indexOf('  async function approve(job, by)'), b = source.indexOf('  /* ── 7.6', a);
vm.runInContext(source.slice(a, b), context);
(async () => {
  listeners.input(); assert(approveButton.disabled, 'typing cannot approve stale geometry');
  assert.equal(timers.size, 1); await [...timers.values()][0]();
  assert.deepEqual([...job.lineInput], ['First line', 'Second line', 'Third line']);
  assert.equal(job.lineMode, '3', 'typing retains the chosen line count');
  assert(calls.includes('focus')); assert(calls.some(c => Array.isArray(c) && c[0] === 12), 'typing retains the caret');
  // Approval must flush un-applied text for a NEW engraving as well as a saved copy.
  ta.value = 'New first\nNew second'; await context.approve(job);
  assert.deepEqual([...job.lineInput], ['New first', 'New second']);
  const fits = calls.filter(x => x === 'fit').length;
  ta.value = ''; await context.approve(job);
  assert.equal(calls.filter(x => x === 'fit').length, fits, 'empty words cannot reuse the last approved fit');
  assert(calls.includes('Type the words first'));
  console.log('Multiline editor OK: live input, hard breaks, chosen mode, caret, stale/blank approval protection');
})().catch(e => { console.error(e); process.exitCode = 1; });
