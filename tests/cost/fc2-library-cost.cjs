// Cost of the Library's ops (charmNestLibrary) other than the live read, measured with FC1's meter on the REAL handler over the
// meter's in-memory Firestore (FC2, Firebase cost emergency). Fixtures only: no live call, no network.
//   node tests/cost/fc2-library-cost.cjs            (prints the table)
//   FC2_ASSERT=1 node tests/cost/fc2-library-cost.cjs   (also checks the budgets below)
// The SIZES are assumptions (the structure is the code's): a 25-charm sheet record is 37 KB whole (FC10), a set a few KB, a run
// record some tens of KB. Change N_* to see how each op scales with the shop's history.
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const W = path.join(__dirname, '..', '..');
const N_DONE_SHEETS = +process.env.N_DONE_SHEETS || 1500, SHEETS_PER_SET = 3, N_CURRENT_SETS = 14, N_CALIBRATION = 300, N_CHARMS = 3000, N_RUNS = 300;
const DAY = i => { const d = new Date(Date.UTC(2026, 8, 1) + Math.floor(i / 40) * 86400000); return d.toISOString().slice(0, 10); };
const T0 = Date.UTC(2026, 8, 6, 14, 0, 0);

function sheet(id, o) {
  const n = 25, orders = o.orders || [String(3700000000 + o.i * 3), String(3700000001 + o.i * 3), String(3700000002 + o.i * 3)];
  const charms = Array.from({ length: n }, (_, i) => ({ id: id + ':' + i, hash: 'h' + i, name: 'Charm design number ' + i, sourceName: 'SKU-' + i + ' (master)', order: orders[i % 3], thumbUrl: 'https://firebasestorage.googleapis.com/v0/b/x/o/charmnest%2Fcharms%2F' + 'h' + i + '.png?alt=media&token=0123456789abcdef0123456789abcdef', aiUrl: 'https://firebasestorage.googleapis.com/v0/b/x/o/charmnest%2Fcharms%2F' + 'h' + i + '.ai?alt=media&token=0123456789abcdef0123456789abcdef', outline: Array.from({ length: 12 }, (_, k) => [k * 1.234567, k * 2.345678]) }));
  const placements = charms.map((c, i) => ({ id: c.id, angle: 90, cxPt: 10.123456 + i, cyPt: 20.123456 + i, wPt: 21.33, hPt: 18.5, n: i + 1 }));
  const url = p => 'https://firebasestorage.googleapis.com/v0/b/x/o/' + encodeURIComponent(p) + '?alt=media&token=0123456789abcdef0123456789abcdef';
  const sources = charms.map(c => ({ id: 's' + c.id, name: c.sourceName, hash: 'ab' + c.hash, bytes: 0, path: 'charmnest/charms/' + c.hash + '.ai', url: c.aiUrl }));
  const d = {
    id, sheetId: id, metal: o.metal || 'gold', metalLabel: 'Gold Filled', day: o.day, setId: o.setId || null, setSeq: o.setSeq || null, sheetIndex: o.k || 1, page: 1, folder: 'GF_' + o.day + '_Set-' + (o.setSeq || 0) + '_Sheet-' + (o.k || 1), fileBase: 'GF_' + o.day,
    status: 'complete', charmCount: n, placedCount: n, rejectCount: 0, density: 0.61, verification: { ok: true }, archived: false, draft: false, runId: o.runId || null, orders, listings: ['1718000' + (o.i % 90)],
    poolIds: charms.map((c, i) => orders[i % 3] + '_' + (1000 + i) + '_1'), stock: { w: 100, h: 50 }, names: 'Charm design number 0, Charm design number 1',
    outputs: { ai: { path: 'charmnest/' + id + '.ai', url: url('charmnest/' + id + '.ai') }, pdf: { path: 'charmnest/' + id + '.pdf', url: url('charmnest/' + id + '.pdf') }, labelled: { path: 'charmnest/' + id + '-l.pdf', url: url('charmnest/' + id + '-l.pdf') }, report: { path: 'charmnest/' + id + '-r.json', url: url('charmnest/' + id + '-r.json') }, preview: { path: 'charmnest/' + id + '.png', url: url('charmnest/' + id + '.png') } },
    previewAt: T0, label: { files: [{ path: 'charmnest/' + id + '-q.pdf', url: url('charmnest/' + id + '-q.pdf'), orders, part: 1 }] },
    sources: o.lite ? undefined : sources, sourcesLite: o.lite ? sources.map(s => ({ name: s.name, hash: s.hash })) : undefined,
    charms, placements, backPool: [], seq: o.i, createdAt: T0 + o.i * 1000, cardStartedAt: T0 + o.i * 1000, updatedAt: T0 + o.i * 60000
  };
  if (o.done) { d.laserDoneAt = T0 + o.i * 60000 + 3600000; d.laserDoneBy = 'Paul'; d.processSeals = [{ id: 'laserReady-1', how: 'laserReady', at: d.laserDoneAt - 1000, by: 'Paul' }, { id: 'laserDone-1', how: 'laserDone', at: d.laserDoneAt, by: 'Paul' }]; d.processReady = false; }
  return d;
}
function build() {
  const docs = {}, add = (c, id, d) => { docs[c + '/' + id] = JSON.parse(JSON.stringify(d)); };
  // completed history: sets of 3 sheets
  let i = 0;
  for (let s = 0; i < N_DONE_SHEETS; s++) {
    const setId = 'set-' + DAY(i) + '-' + (s + 1), ids = [];
    for (let k = 1; k <= SHEETS_PER_SET && i < N_DONE_SHEETS; k++, i++) { const id = 'sheet-d' + i; ids.push(id); add('Charm_Nest_Sheets', id, sheet(id, { i, k, done: true, setId, setSeq: s + 1, day: DAY(i), lite: i % 3 === 0, runId: 'run-old-' + (s % 50) })); }
    add('Charm_Nest_Sets', setId, { setId, seq: s + 1, day: DAY(i), runId: 'run-old-' + (s % 50), name: 'Set-' + (s + 1), key: 'k' + s, materials: ['gold', 'silver'], sheetIds: ids, status: 'complete', laserDoneAt: T0 + i * 60000 + 3600000, laserDoneBy: 'Paul', processSeals: [{ id: 'laserDone-1', how: 'laserDone', at: T0 + i * 60000 + 3600000, by: 'Paul' }], orders: Object.fromEntries(ids.flatMap((x, a) => [0, 1, 2].map(b => [String(3700000000 + (i - ids.length + a) * 3 + b), { held: null, lines: Array.from({ length: 3 }, (_, l) => ({ sku: 'SKU-' + l, title: 'A charm with a long title ' + l, copies: [{ poolId: 'x' + l, sheetId: x }] })) }]))), labels: { made: true }, labelFiles: [], committedAt: T0 + i * 60000, createdAt: T0 + i * 60000, updatedAt: T0 + i * 60000 + 3600000 });
  }
  // current work: 14 open sets of 3 sheets, on 3 open runs
  for (let s = 0; s < N_CURRENT_SETS; s++) {
    const setId = 'set-cur-' + (s + 1), ids = [], runId = 'run-open-' + (s % 3);
    for (let k = 1; k <= 3; k++) { const id = 'sheet-c' + s + '-' + k; ids.push(id); add('Charm_Nest_Sheets', id, sheet(id, { i: 5000 + s * 3 + k, k, setId, setSeq: 100 + s, day: '2026-10-06', lite: true, runId })); }
    add('Charm_Nest_Sets', setId, { setId, seq: 100 + s, day: '2026-10-06', runId, name: 'Set-' + (100 + s), key: 'kc' + s, materials: ['gold'], sheetIds: ids, status: 'open', orders: Object.fromEntries(Array.from({ length: 9 }, (_, b) => [String(3790000000 + s * 9 + b), { held: null, lines: [{ sku: 'SKU-1', title: 'A charm with a long title', copies: [{ poolId: 'y' + b, sheetId: ids[b % 3] }] }] }])), createdAt: T0 + 9e6, updatedAt: T0 + 9e6 + s * 1000 });
  }
  for (let r = 0; r < 3; r++) {
    add('Charm_Nest_Runs', 'run-open-' + r, { runId: 'run-open-' + r, day: '2026-10-06', step: 'nesting', status: 'running', mode: 'auto', orders: ['3790000000'], liveLines: { ids: ['live-' + r], lines: 300, bytes: 135000 }, lineArchive: null, errors: [], createdAt: T0, updatedAt: T0 + 9e6, notes: 'x'.repeat(15000) });
    add('Charm_Nest_Run_Live', 'live-' + r, { runId: 'run-open-' + r, json: JSON.stringify({ ['3790000000_1000']: { orderId: '3790000000', pad: 'y'.repeat(135000) } }), bytes: 135000, lines: 300, seq: 0 });
  }
  for (let r = 0; r < N_RUNS; r++) add('Charm_Nest_Runs', 'run-old-' + r, { runId: 'run-old-' + r, day: DAY(r * 5), step: 'done', status: 'complete', mode: 'auto', orders: ['3700000000'], sheets: Object.fromEntries(Array.from({ length: 3 }, (_, a) => ['s' + a, { id: 's' + a }])), lines: undefined, lineArchive: { parts: 2, lines: 80, held: 0, sheets: 3 }, errors: [], createdAt: T0 + r, updatedAt: T0 + r * 1000, notes: 'x'.repeat(15000) });
  for (let c = 0; c < N_CALIBRATION; c++) add('Charm_Nest_Calibration', 'cal' + c, { sheetId: 'sheet-d' + c, metal: 'gold', count: 25, cv: 0.31, largestFrac: 0.12, density: 0.61, placedAll: true, createdAt: T0 + c * 1000 });
  for (let c = 0; c < N_CHARMS; c++) add('Charm_Nest_Library', 'h' + c.toString(16).padStart(12, '0'), { hash: 'h' + c, name: 'Charm ' + c, updatedAt: T0 + c, lastUsed: T0 + c, timesUsed: 3 });
  return docs;
}
const post = (fn, body) => fn.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: {}, body: JSON.stringify(body) });

(async () => {
  const m = meter.create(); m.install();
  m.db.seed(build());
  const fn = require(path.join(W, 'netlify/functions/charmNestLibrary.js'));
  const rows = [];
  const run = async (name, body, perHour, note) => {
    const a = m.snapshot();
    const r = await m.op(name, () => post(fn, body));
    const d = m.since(a), out = r.statusCode === 200 ? JSON.parse(r.body) : { error: r.body.slice(0, 200) };
    if (out.error) process.stdout.write(`  ! ${name}: ${out.error}\n`);
    rows.push({ name, reads: d.reads + d.aggs, bytes: d.bytes, writes: d.writes, perHour, note });
    return { out, d };
  };
  const day = DAY(0);
  const res = {};
  res.ping = (await run('ping (page load, with calibration)', { op: 'ping' }, 0, 'each Sorter page load / cloud-probe retry')).d;
  res.pingAgain = (await run('ping (again, within 10 min)', { op: 'ping' }, 0, 'a second page load / retry')).d;
  res.pingNo = (await run('ping calibration:false', { op: 'ping', calibration: false }, 0, '30 s after a save, autoResume')).d;
  res.countOnly = (await run('laserDoneList countOnly (first, counts)', { op: 'laserDoneList', countOnly: true }, 0, 'after a completion, a delete, an archive, or 20 min')).d;
  res.countKept = (await run('laserDoneList countOnly (kept answer)', { op: 'laserDoneList', countOnly: true }, 60, 'Library tab badge, at most 1/min per open Library')).d;
  res.doneSheets = (await run('laserDoneList sheets p1', { op: 'laserDoneList', kind: 'sheets', limit: 60 }, 0, 'Completed tab opened / refreshed')).d;
  res.doneSets = (await run('laserDoneList sets p1', { op: 'laserDoneList', kind: 'sets', limit: 60 }, 0, '')).d;
  res.doneAct = (await run('laserDoneList activity p1', { op: 'laserDoneList', sort: 'activity', kind: 'sheets', limit: 60 }, 0, '')).d;
  res.listSheets = (await run('listSheets Current (limit 300, excludeDone)', { op: 'listSheets', limit: 300, excludeDone: true }, 0, 'each Library (re)load')).d;
  res.setList = (await run('setList includeSheets excludeDone', { op: 'setList', includeSheets: true, limit: 200, excludeDone: true }, 0, 'each Sets view (re)load')).d;
  res.find = (await run('findSheets order', { op: 'findSheets', q: '3700000010' }, 0, 'a search')).d;
  res.history = (await run('history activity', { op: 'history', sort: 'activity', limit: 60 }, 0, 'History window open / refresh / more')).d;
  res.history2 = (await run('history listing', { op: 'history', limit: 20 }, 0, 'Sets menu open')).d;
  res.runList = (await run('runList', { op: 'runList', limit: 50 }, 0, '')).d;
  res.laserLast = (await run('laserSheetLast', { op: 'laserSheetLast', by: 'Paul' }, 12, 'Laser tab, every 5 min (hidden too)')).d;
  res.cancelList = (await run('cancelList idsOnly track', { op: 'cancelList', idsOnly: true, track: true }, 0, '')).d;
  res.listCharms = (await run('listCharms', { op: 'listCharms', limit: 400 }, 0, 'Charms view')).d;
  process.stdout.write(`\nFixture: ${N_DONE_SHEETS} completed sheets in ${Math.ceil(N_DONE_SHEETS / SHEETS_PER_SET)} sets, ${N_CURRENT_SETS * 3} current sheets, ${N_RUNS + 3} runs, ${N_CALIBRATION} calibration rows, ${N_CHARMS} library charms\n`);
  process.stdout.write('op'.padEnd(46) + 'reads'.padStart(8) + 'bytes'.padStart(12) + '\n');
  for (const r of rows) process.stdout.write(r.name.padEnd(46) + String(r.reads).padStart(8) + String(r.bytes).padStart(12) + '\n');
  if (process.env.FC2_VERBOSE) m.print(s => process.stdout.write(s + '\n'));
  if (process.env.FC2_ASSERT) {
    const B = JSON.parse(require('fs').readFileSync(path.join(__dirname, 'fc2-budgets.json'), 'utf8'));
    for (const [k, lim] of Object.entries(B)) { const d = res[k]; if (!d) throw new Error('no measurement for ' + k); meter.assertMax({ reads: d.reads + d.aggs, bytes: d.bytes }, lim, k); }
    process.stdout.write('budgets ok\n');
  }
  m.uninstall();
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
