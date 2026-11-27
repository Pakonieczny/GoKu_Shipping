/* KEPTLIST (Paul, 27 Nov 2026, screen recording of Design & Engrave: Placements 249 → 250 and Approved 303 → 302 and back, every few seconds). Node only: node tests/charm-nest/rt1127-keptlist.cjs.
   An approved piece whose words are placed again only to be drawn (charm-nest-engrave-sync.js fitOne: "ready" then "fitting", its state put back after) keeps what it is in keptState.
   K1 while it is placed again it stays in Approved and out of Placements, and the rail's pending count does not move
   K2 a piece to approve (keptState review) stays in Placements while placed again   K3 every list, count and "is it decided" check reads it so */
'use strict';
const fs = require('fs'), path = require('path'), br = fs.readFileSync(path.join(__dirname, '..', '..', 'charm-nest-bridge.js'), 'utf8');
let passed = 0, failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d))); c ? passed++ : failed++; };
const line = start => { const a = br.indexOf(start); if (a < 0) throw new Error('missing: ' + start); return br.slice(a, br.indexOf('\n', a)); };
const code = [line('  const DECIDED = ["approved", "written", "skipped"];'), line('  const listState = j =>'), line('  const accepted = j =>'), line('  const personNone = j =>'), line('  const queuedJobs = jobs =>'), line('  const isApprovedJob = j =>')].join('\n');
const X = new Function(code + '\n; return { listState, queuedJobs, isApprovedJob, DECIDED };')();
const redrawn = { key: 'a', state: 'fitting', keptState: 'written', row: { state: 'written' }, copies: ['x_1'] }, toApprove = { key: 'b', state: 'fitting', keptState: 'review', row: {}, copies: ['y_1'] }, plain = { key: 'c', state: 'written', row: {}, copies: ['z_1'] };
ok('K1 an approved piece placed again to be drawn stays Approved, never in Placements', X.isApprovedJob(redrawn) && X.queuedJobs([redrawn]).length === 0 && X.DECIDED.includes(X.listState(redrawn)));
ok('K2 a piece to approve placed again stays in Placements', X.queuedJobs([toApprove]).length === 1 && !X.isApprovedJob(toApprove));
ok('K1b a piece not being placed again reads as it is', X.isApprovedJob(plain) && X.listState(plain) === 'written');
const reads = ['const decidedJobs = () =>', 'const pendingJobs = () =>', 'const rowPending = r =>', 'const NEVER_OPEN = j =>'].map(s => line(s));
ok('K3 the decided list, the rail\'s pending count, a row\'s pending mark and the put-away rule read what a piece is', reads.every(l => /listState\(j\)/.test(l)), reads.filter(l => !/listState\(j\)/.test(l)).map(l => l.slice(0, 80)));
ok('K3b approve-all, "none" for a line and the back records\' check never take a piece mid-redraw for an unapproved one', /if \(\["approved", "written"\]\.includes\(listState\(j\)\)\) \{ approved\.push\(j\.key\); continue; \}/.test(br) && /if \(\["approved", "written"\]\.includes\(listState\(j\)\)\) \{ await skip\(j, by\)/.test(br) && /if \(!j\.approvedAt \|\| !\["approved", "written"\]\.includes\(listState\(j\)\)\) \{ j\.copies\.forEach/.test(br));
console.log(`rt1127-keptlist: ${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
