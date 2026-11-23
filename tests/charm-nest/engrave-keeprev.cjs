// KEEPREV (Paul, 22 Nov 2026: "For some reason 1 of the 3 pieces in this order was not placed on a sheet even though the back engraving was approved on all 3 charms"; order 4242795385, BEADY CHICKEN 3,
// piece 3 approved at 20:49, 21:12 and 22:14, each time saved by the cloud and each time put back to Review by Michelle's own computer). After Approve the page re-sent its whole line every few seconds
// until the cloud said "written" (confirm, APPROVECLOUD); the cloud's save reached the page as a record it turned away ("own press") but whose revision it noted all the same, so the page's next re-send
// carried the newest revision with its OLD, pre-approval jobs, the cloud took it as an informed write, and the approval was gone.
// Each case runs the real charm-nest-engrave-sync.js against a stand-in cloud that refuses a write made on an older revision (tests/charm-nest/_engrave-race-sim.cjs):
//   feedFirst  the cloud's save reaches the page (feed) before the loop's next send: the order seen on 22 Nov
//   readFirst  the save lands between the loop's send and its read-back, then the page writes the line again
//   sendFirst  the loop's send reaches the cloud before the feed's notice
// In every case piece 3 must end "written" with its approval, in the cloud and on the page, and pieces 1 and 2 stay "written".
//   node tests/charm-nest/engrave-keeprev.cjs
'use strict';
const path = require('path'), { execFileSync } = require('child_process');
const root = path.join(__dirname, '../..'), sim = path.join(__dirname, '_engrave-race-sim.cjs'), file = path.join(root, 'charm-nest-engrave-sync.js');
let failed = 0;
for (const v of ['feedFirst', 'readFirst', 'sendFirst']) {
  let out = ''; try { out = execFileSync(process.execPath, [sim, file, v], { encoding: 'utf8', timeout: 60000 }); } catch (e) { out = String(e.stdout || '') + String(e.message || e); }
  const res = (out.match(/RESULT [^\n]*/) || [''])[0];
  const good = /cloud C3 = written approvedAt=1795403642166/.test(res) && /page C3 = written approvedAt=1795403642166/.test(res) && /C1\/C2 cloud = written\/written/.test(res);
  console.log((good ? 'OK   ' : 'FAIL ') + `${v}: piece 3's approval saved by the cloud stays saved (cloud and page), pieces 1 and 2 stay saved` + (good ? '' : ' :: ' + (res || out.slice(-600))));
  if (!good) failed++;
}
console.log(failed ? `FAIL engrave-keeprev (${failed})` : 'PASS engrave-keeprev'); process.exit(failed ? 1 : 0);
