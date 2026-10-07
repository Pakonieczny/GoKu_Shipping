// The Sets/History window of the Sorter (RunHistory) reads the whole activity list again at most once a minute while it is open and a run keeps saving.
// Not while the tab is hidden (FC2): the read waits for the tab to be shown, and happens then only if a run asked for it. The real lines of
// charm-nest-bridge.js are run here against stubs.
//   node tests/cost/fc2-history-hidden.cjs
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs'), vm = require('vm');
const src = fs.readFileSync(path.join(__dirname, '..', '..', 'charm-nest-bridge.js'), 'utf8');
const lines = src.split('\n');
const a = lines.findIndex(l => /^\s*let refreshTimer = 0, refreshOwed = false;/.test(l));
assert.ok(a > 0, 'the refresh lines are in the bridge');
const code = lines.slice(a, a + 3).join('\n');
assert.ok(/const refreshIfOpen =/.test(code) && /visibilitychange/.test(code), 'three lines: state, refreshIfOpen, the listener');
const ok = s => process.stdout.write('ok   ' + s + '\n');

let NOW = 1e12, loads = 0; const timers = [], listeners = [];
const H = { dlg: { open: true }, readAt: 0, menu: null, busy: new Set() };
const doc = { hidden: false, addEventListener: (t, f) => listeners.push([t, f]) };
const ctx = vm.createContext({ H, document: doc, Date: { now: () => NOW }, setTimeout: (f, ms) => { timers.push({ f, ms }); return timers.length; }, load: () => { loads++; H.readAt = NOW; } });
vm.runInContext(code + '\nthis.refreshIfOpen = refreshIfOpen;', ctx);
const refreshIfOpen = ctx.refreshIfOpen, runTimers = () => { const t = timers.splice(0); for (const x of t) x.f(); };
const shown = () => { for (const [t, f] of listeners) if (t === 'visibilitychange') f(); };

// 1. shown: a run's step asks, the read comes after the minute is up; a second ask while it waits adds no second read
H.readAt = NOW; refreshIfOpen(); refreshIfOpen(); assert.strictEqual(timers.length, 1); assert.ok(timers[0].ms >= 59000 && timers[0].ms <= 60000, 'it waits for the minute: ' + timers[0].ms);
NOW += 60000; runTimers(); assert.strictEqual(loads, 1); ok('shown: one read after the minute, however many steps asked');

// 2. hidden: any number of asks over an hour read nothing
doc.hidden = true; loads = 0;
for (let i = 0; i < 60; i++) { NOW += 60000; refreshIfOpen(); runTimers(); }
assert.strictEqual(loads, 0); ok('hidden for an hour with a run saving: 0 reads (was up to 60)');
// becoming hidden or a visibility event while still hidden changes nothing
shown(); runTimers(); assert.strictEqual(loads, 0);

// 3. shown again: the read the run asked for is made at once (within a second), once
doc.hidden = false; shown(); assert.strictEqual(timers.length, 1); assert.ok(timers[0].ms <= 700, 'at once: ' + timers[0].ms); runTimers(); assert.strictEqual(loads, 1);
shown(); runTimers(); assert.strictEqual(loads, 1); ok('shown again: the owed read is made once, at once; showing again with nothing owed reads nothing');

// 4. hidden with nobody asking owes nothing; a closed window never reads
doc.hidden = true; NOW += 5 * 60000; doc.hidden = false; shown(); runTimers(); assert.strictEqual(loads, 1, 'no ask, no read');
doc.hidden = true; refreshIfOpen(); NOW += 61000; runTimers(); H.dlg.open = false; doc.hidden = false; shown(); runTimers(); assert.strictEqual(loads, 1, 'closed: no read'); ok('no ask owes nothing; a closed window reads nothing');
process.stdout.write('history hidden OK\n');
