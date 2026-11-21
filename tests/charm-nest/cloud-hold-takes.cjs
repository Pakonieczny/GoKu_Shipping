// HOLDTAKES (Paul, 21 Nov 2026): a sheet the run holds (its nest failed or was stopped) still takes new pieces, and a change to it lifts the hold; it no longer splits an order onto a
// new draft (CAMP_0903 went alone onto 14K Draft 4 while 14K Draft 3 was held). MERGEALL waits through a file write instead of refusing.
//   NODE_PATH=<deps> node tests/charm-nest/cloud-hold-takes.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 21, 22, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0; const ok = (n, c, d) => { log0((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 600))); if (!c) failed++; };
const Place = require('../../charm-nest-place.js');
(async () => {
  const run = { runId: 'R', sheetHolds: { 'gold14k-a': 'its nest failed 3 times', 'gold-a': 'its nest failed 3 times' } };
  ok('1 · a held 14K sheet is not closed to new pieces', !Place.closedRec({ id: 'gold14k-a', metal: 'gold14k', runId: 'R' }, [], run, T0));
  ok('2 · a held Gold Filled sheet still reopens for its own order', Place.reopensHomeRec({ id: 'gold-a', metal: 'gold', runId: 'R' }, [], run, T0));
  ok('3 · a cut sheet stays closed', Place.closedRec({ id: 'gold14k-a', metal: 'gold14k', runId: 'R', roseCutAt: T0 }, [], run, T0));
  const H = Place.recordHost({ run, records: [{ id: 'gold-a', metal: 'gold', runId: 'R', page: 1, draft: true, charms: [{ id: 'c1', poolId: 'p1', order: 'o1' }], poolIds: ['p1'], layoutRev: 1, contentRev: 1 }], sets: [], now: T0 });
  const fp = H.fillingPage('gold');
  ok('4 · a held Gold Filled draft is still the draft filling', !!fp && fp.sheetId === 'gold-a', fp && fp.sheetId);
  // the nest step: a hold made at revisions 1/1 is lifted when the sheet changed (2/3); one with no change stays
  const Step = F.load('_charmNestNestStep.js');
  const R = 'run-hold-takes';
  F.seed('Charm_Nest_Sheets/gold14k-b', { id: 'gold14k-b', runId: R, metal: 'gold14k', layoutRev: 2, contentRev: 3, charms: [], poolIds: [] });
  F.seed('Charm_Nest_Sheets/gold14k-c', { id: 'gold14k-c', runId: R, metal: 'gold14k', layoutRev: 1, contentRev: 1, charms: [], poolIds: [] });
  const r = { runId: R, sheetHolds: { 'gold14k-b': 'old', 'gold14k-c': 'old' }, sheetHoldRevs: { 'gold14k-b': '1/1', 'gold14k-c': '1/1' } };
  const units = Step.steps().nest; let err = null;
  try { await units[0]({ db: F.db, run: r, runId: R, budgetEnd: Date.now() + 600000, call: async () => ({ ok: true }), say: () => {}, cursor: {} }); } catch (e) { err = String(e && e.stack || e); }
  ok('5 · a held sheet that changed since its hold is no longer held', !err && r.sheetHolds['gold14k-b'] === null, err || r.sheetHolds);
  ok('6 · a held sheet with no change stays held', r.sheetHolds['gold14k-c'] === 'old', r.sheetHolds);
  const src = fs.readFileSync(path.join(__dirname, '../../netlify/functions/_charmNestCmdExec.js'), 'utf8');
  ok('7 · MERGEALL waits for a file write (busyRec) and stops every live nest each pass', /MERGEWAIT/.test(src) && /const still = r => !live\(r\) && !busyRec\(r, Date\.now\(\)\)/.test(src) && /is having its files written/.test(src));
  const drive = fs.readFileSync(path.join(__dirname, '../../netlify/functions/_charmNestDrive.js'), 'utf8');
  ok('8 · the driver saves the holds\' revisions with the run', /"sheetHolds", "sheetHoldRevs"/.test(drive));
  const ops = fs.readFileSync(path.join(__dirname, '../../netlify/functions/_charmNestCmdOps.js'), 'utf8');
  ok('9 · moving pieces off a sheet with no rejects / feedWait writes null, never undefined (Firestore refused it)', /NOUNDEF/.test(ops) && /rec\.feedWait == null \? null : rec\.feedWait/.test(ops) && /rec\.rejects == null \? null : rec\.rejects/.test(ops));
  log0(failed ? `FAIL cloud-hold-takes (${failed})` : 'PASS cloud-hold-takes'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL cloud-hold-takes: ' + (e && e.stack || e)); process.exit(1); });
