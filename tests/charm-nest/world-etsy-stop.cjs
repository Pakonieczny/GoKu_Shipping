// CLOUDDRIVE build step 1a (E2, m:E7): the world build reads the shop's Etsy switch before it spends the day's Etsy calls, but only while the cloud drive is in standby
// or on; with the drive absent (or "off") the build counts and asks exactly as today. Node only, no Chromium: the real background build (charmNestWorld-background, through
// its own lease) over the fake cloud of _cloudFakes.cjs. Etsy is a stub that COUNTS its calls and throws (the build's pool() swallows the throw, so the count is asserted).
//   node tests/charm-nest/world-etsy-stop.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const T0 = Date.UTC(2026, 10, 20, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
let failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 700))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), WorldLease = F.load('_charmNestWorldLease.js'), worldBg = F.load('charmNestWorld-background.js');
const used = () => ((F.doc('Charm_World/etsy') || {}).used || 0);
/** n receipts open on Etsy that the mail system's copy does not hold whole (no items): each one is a receipt the build would read from Etsy. */
function staleReceipts(n, from) { for (let i = 0; i < n; i++) { const id = String(from + i); F.seed('EtsyMail_Receipts/' + id, { receipt_id: id, status: 'Paid', is_paid: true, is_shipped: false, updated_timestamp: Math.floor(T0 / 1000) - 100 + i, raw: { receipt_id: id, status: 'Paid' } }); } }
async function build() { F.clock.advance(11 * 60000); const got = await WorldLease.dispatch(F.db, 'cron'); const res = await worldBg.handler({ httpMethod: 'POST', body: JSON.stringify({ nonce: got.nonce }) }); return { res, summary: F.doc('Charm_World/summary') || {} }; }
function setDrive(mode) { if (mode == null) F.store.delete('Charm_Nest_Open/drive'); else F.seed('Charm_Nest_Open/drive', { mode, runMode: 'auto', cloudSteps: [] }); lib._driveForget(); }

(async () => {
  /* the drive absent: today's counting (3 stale receipts: 3 calls a build, each counted against the day's 500 before it is made) */
  F.reset(); F.clock.set(T0); setDrive(null); staleReceipts(3, 3950000001);
  F.seed('EtsyApi_Config/stop', { stopped: true, since: T0 - 60000 });
  let c0 = F.etsy.calls;
  for (let i = 0; i < 5; i++) await build();
  ok('E2 · drive absent: 5 builds ask Etsy for the 3 receipts each time, as today (the switch is the fetch\'s own guard): 15 calls, 15 counted', F.etsy.calls - c0 === 15 && used() === 15, { calls: F.etsy.calls - c0, used: used() });
  setDrive('off'); c0 = F.etsy.calls; const u0 = used();
  await build();
  ok('E2 · drive "off": the same, exactly', F.etsy.calls - c0 === 3 && used() - u0 === 3, { calls: F.etsy.calls - c0, used: used() - u0 });
  /* m:E7 · HYDRATE_MAX: at most 40 a build, today's bound */
  F.reset(); F.clock.set(T0); setDrive(null); staleReceipts(45, 3960000001);
  c0 = F.etsy.calls; await build();
  ok('m:E7 · drive absent: 45 stale receipts, one build asks for at most 40 (HYDRATE_MAX) and counts 40', F.etsy.calls - c0 === 40 && used() === 40, { calls: F.etsy.calls - c0, used: used() });

  /* standby, the switch on: nothing is asked and nothing is counted; the receipts wait, said in words */
  F.reset(); F.clock.set(T0); setDrive('standby'); staleReceipts(3, 3950000001);
  F.seed('EtsyApi_Config/stop', { stopped: true, since: T0 - 60000 });
  c0 = F.etsy.calls; let last = null;
  for (let i = 0; i < 5; i++) last = await build();
  ok('E2 · standby + the switch on: 5 builds, Charm_World/etsy.used does not move and the Etsy stub sees 0 calls', F.etsy.calls === c0 && used() === 0, { calls: F.etsy.calls - c0, used: used() });
  ok('E2 · ... the receipts stay to be read (toRead), and the build says why, in words', +last.summary.toRead === 3 && (last.summary.errors || []).some(e => /Etsy is stopped for the whole shop: 3 receipt\(s\) wait for Resume Etsy/.test(e)), { toRead: last.summary.toRead, errors: last.summary.errors });
  /* a switch that cannot be read counts as stopped */
  F.hooks.failRead = /^EtsyApi_Config\/stop$/;
  await build(); F.hooks.failRead = null;
  ok('E2 · standby + the switch unreadable: counted as stopped (0 calls)', F.etsy.calls === c0 && used() === 0, { calls: F.etsy.calls - c0 });
  /* Resume Etsy: the first build reads all 3 */
  F.seed('EtsyApi_Config/stop', { stopped: false, resumedAt: Date.now(), resumedBy: 'Paul' });
  await build();
  ok('E2 · after Resume Etsy, one build reads all 3 (and counts them)', F.etsy.calls - c0 === 3 && used() === 3, { calls: F.etsy.calls - c0, used: used() });
  ok('the Etsy switch was never written, no address was fetched', !F.stopWrites.length && !F.net.tripwire.length && !F.net.other.length, { stop: F.stopWrites, trip: F.net.tripwire, other: F.net.other });
  ok('the world\'s lease was only ever taken by the cron (never by a change or by the drive)', !/^change:|^drive/.test(String((F.doc('Charm_World/lease') || {}).by || '')), F.doc('Charm_World/lease'));
  ok('in standby the build told the cloud driver (a kick of charmNestDrive-background, never of a world build)', F.net.ours.some(k => k.name === 'charmNestDrive-background') && !F.net.ours.some(k => k.name === 'charmNestWorld-background'), F.net.ours.map(k => k.name));
  console.log(failed ? `FAIL world-etsy-stop · ${failed} check(s)` : 'PASS world-etsy-stop');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
