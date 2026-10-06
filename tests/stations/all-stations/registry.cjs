// The list of checks of the all-stations end-to-end test, and what each one needs from the workers who build the pieces.
//   check(id, title, owners, fn)   runs fn; the result is one of
//     DONE     it passed
//     PENDING  it failed (or could not run) and at least one of its owners has not landed yet: the failure is kept as the reason, with the worker's name
//     FAILED   it failed and every owner has landed: a product defect (or a wrong test), reported
//   `landed(owner)` says whether that worker's piece is on main. Two sources, either is enough: a probe in the repository (a marker only the finished
//   piece has) or the worker's own section in plans/stations-round2/api.md (<!-- ID:begin ... -->, written when they push).
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../../..');
const API_MD = process.env.STATIONS_API_MD || '/mnt/project-files/plans/stations-round2/api.md';

const read = f => { try { return fs.readFileSync(path.join(root, f), 'utf8'); } catch (_) { return ''; } };
const has = (f, s) => read(f).includes(s);
const exists = f => fs.existsSync(path.join(root, f));
let apiMd = ''; try { apiMd = fs.readFileSync(API_MD, 'utf8'); } catch (_) {}
const inApi = id => new RegExp('<!--\\s*' + id + ':begin').test(apiMd);

/** a worker's piece is on main when its marker is in the repository (probe) or its section is in api.md */
const PROBES = {
  PB1: () => has('netlify/functions/_activityKinds.js', 'displayStation') && has('charm-nest-efficiency-stations.js', 'displayStation'),
  WS1: () => has('weld-1.html', 'weld_people'),
  WS2: () => has('netlify/functions/_stationLive.js', 'noThroughput') && has('charm-nest-efficiency-stations.js', 'noThroughput'),     // (the server half is not enough: the board draws it)
  WS3: () => has('weld-scan-1.html', 'weldScanOutbox'),
  AD1: () => has('station-session.js', 'stationAdmin') || inApi('AD1'),
  AD2: () => exists('netlify/functions/_stationAdmins.js'),
  LD1: () => exists('charm-nest-role.js'),
  LD2: () => has('netlify/functions/_stationLive.js', 'Sorter app (Laser)'),
  LS1: () => exists('netlify/functions/_laserSheetTime.js'),
  IN1: () => exists('netlify/functions/_employeeInbox.js'),
  IN2: () => has('charm-nest-efficiency-person.js', 'normInbox'),
  IN3: () => has('etsy-mail-1.html', 'emClaimFor'),
  SA1: () => inApi('SA1'), SA2: () => inApi('SA2'), SA3: () => inApi('SA3'), SA4: () => inApi('SA4') || exists('design-scan.html'), SA5: () => exists('charm-nest-laser-act.js') || inApi('SA5')
};
// (WS2's api.md section describes the server half; the board's Welding card is its other half, so its probe alone decides; IN2's section was written first as the shape it expects, the client is its other half)
// (AD1's api.md section is written before its code reaches main: the client timers are what the checks need, so its probe alone decides too)
const PROBE_ONLY = new Set(['WS2', 'IN2', 'AD1']);
const landed = id => { try { return !!(PROBES[id] ? (PROBES[id]() || (!PROBE_ONLY.has(id) && inApi(id))) : inApi(id)); } catch (_) { return false; } };

const results = [];
const only = (process.env.ONLY_CHECKS || '').split(',').map(s => s.trim()).filter(Boolean);

async function check(id, title, owners, fn) {
  if (only.length && !only.some(o => id.startsWith(o))) return null;
  const waiting = (owners || []).filter(o => !landed(o));
  const t0 = Date.now(); let status = 'DONE', why = '';
  try { await fn(); }
  catch (e) {
    why = String((e && e.message) || e).replace(/\s+/g, ' ').slice(0, 600);
    status = waiting.length ? 'PENDING' : 'FAILED';
  }
  const r = { id, title, owners: owners || [], status, waiting: status === 'PENDING' ? waiting : [], why, ms: Date.now() - t0 };
  results.push(r);
  const tag = status === 'DONE' ? 'ok     ' : status === 'PENDING' ? 'PENDING' : 'FAILED ';
  process.stdout.write(`  ${tag} ${id}  ${title}${status === 'PENDING' ? '  [waiting for ' + waiting.join(', ') + ']' : ''}${why ? '\n           ' + why : ''}\n`);
  return r;
}
/** a check that cannot even be tried yet: counted PENDING with the worker's name */
function pending(id, title, owners, why) {
  if (only.length && !only.some(o => id.startsWith(o))) return null;
  const r = { id, title, owners, status: 'PENDING', waiting: owners.filter(o => !landed(o)), why: why || '', ms: 0 };
  if (!r.waiting.length) r.status = 'FAILED';
  results.push(r);
  process.stdout.write(`  ${r.status === 'PENDING' ? 'PENDING' : 'FAILED '} ${id}  ${title}  [${r.status === 'PENDING' ? 'waiting for ' + r.waiting.join(', ') : 'owners landed but the check is not written'}]${why ? '\n           ' + why : ''}\n`);
  return r;
}
function table() {
  const w = (s, n) => String(s).padEnd(n);
  const lines = ['', w('check', 8) + w('status', 9) + w('owners', 14) + 'what', '-'.repeat(100)];
  for (const r of results) lines.push(w(r.id, 8) + w(r.status, 9) + w(r.owners.join(' '), 14) + r.title + (r.status === 'PENDING' ? '  (waiting: ' + r.waiting.join(', ') + ')' : ''));
  const n = s => results.filter(r => r.status === s).length;
  lines.push('-'.repeat(100), `done ${n('DONE')} · pending ${n('PENDING')} · failed ${n('FAILED')} · total ${results.length}`);
  return lines.join('\n');
}
const landedAll = () => Object.fromEntries(Object.keys(PROBES).map(k => [k, landed(k)]));

module.exports = { check, pending, results, table, landed, landedAll };
