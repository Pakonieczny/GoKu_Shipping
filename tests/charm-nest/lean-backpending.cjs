// LEAN + BACKFOLDER (18 Nov 2026): the checkpoint of a following computer drops the drawn fit of a decided face (leanJob), but never of an approval whose back file is still waiting to be saved: writing that back needs the drawn fit,
// and a reload must find it whole. The real function of charm-nest-bridge.js over fixtures: no browser.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const a = source.indexOf('  function leanJob(j) {'), b = source.indexOf('  function jobCopy(j, seen)', a); assert(a > 0 && b > a, 'leanJob is found');
let fails = 0; const check = (id, ok, ev) => { if (!ok) fails++; console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${ev ? '  ' + ev : ''}`); };
const lean = (j, run) => { const ctx = vm.createContext({ B: { run: run || null }, window: { Engrave: { isWorking: () => false, placeOf: () => ({ centre: [0, 0], size: 8 }) } } }); vm.runInContext(source.slice(a, b) + '; this.f = leanJob;', ctx); return ctx.f(j); };
const job = o => Object.assign({ state: 'written', approvedAt: 1, fit: {}, view: {}, mask: {} }, o || {});
check('1 a decided face on a following computer keeps only its place numbers', !!lean(job()));
check('2 the carrying computer keeps every face whole', lean(job(), { runId: 'r' }) === null);
check('3 an approval whose back is waiting to be saved is kept whole', lean(job({ backPending: 'Reconnect to save the approved back files' })) === null);
check('4 an approval still being saved (job.approving) is kept whole', lean(job({ approving: { at: 1 } })) === null);
console.log(fails ? `FAIL lean-backpending: ${fails}` : 'PASS lean-backpending'); process.exit(fails ? 1 : 0);
