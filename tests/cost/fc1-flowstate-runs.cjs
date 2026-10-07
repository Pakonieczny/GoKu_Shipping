// Cost of the Library's live read (op flowState, every ~3 s per open Library tab) for the run records it reads, measured with the
// shared meter on the REAL charmNestLibrary handler over the meter's in-memory Firestore. Fixtures only, no live call.
//   node tests/cost/fc1-flowstate-runs.cjs
// Finding (FC1): flowState read every run record of the sheets it answers about WHOLE (one get() per run, in a row), only to say
// whether the run exists and is open (field `status`). A run record is a big document (the page keeps it under 1 MiB and the server
// warns at 70 percent of that). A field mask on the same documents gives the same answer for a few bytes.
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const W = path.join(__dirname, '..', '..');

(async () => {
  const m = meter.create(); m.install();
  const fn = require(path.join(W, 'netlify/functions/charmNestLibrary.js'));
  // 4 sheets on 3 runs (an open run and two finished ones: the Library lists the sheets of recent runs), one set, a realistic run record
  const bigRun = (id, status, kb) => ({ runId: id, status, createdAt: 1, updatedAt: 2, lines: undefined, liveLines: { ids: ['a', 'b'], lines: 300, bytes: 400000 }, notes: 'x'.repeat(kb * 1024) });
  m.db.seed({
    'Charm_Nest_Sheets/sheetA1': { id: 'sheetA1', setId: 'set-0001', runId: 'run-open', metal: 'gold', sheetIndex: 1, poolIds: ['p1'], placedCount: 1, verification: { ok: true }, preview: 'p', outputs: { ai: 'f' }, label: { files: [] } },
    'Charm_Nest_Sheets/sheetA2': { id: 'sheetA2', setId: 'set-0001', runId: 'run-open', metal: 'silver', sheetIndex: 1, poolIds: ['p2'], placedCount: 1 },
    'Charm_Nest_Sheets/sheetB1': { id: 'sheetB1', setId: 'set-0002', runId: 'run-done1', metal: 'gold', sheetIndex: 1, poolIds: ['p3'], placedCount: 1 },
    'Charm_Nest_Sheets/sheetC1': { id: 'sheetC1', setId: 'set-0003', runId: 'run-done2', metal: 'gold', sheetIndex: 1, poolIds: ['p4'], placedCount: 1 },
    'Charm_Nest_Sets/set-0001': { setId: 'set-0001', seq: 1, runId: 'run-open', sheetIds: ['sheetA1', 'sheetA2'], status: 'open' },
    'Charm_Nest_Sets/set-0002': { setId: 'set-0002', seq: 2, runId: 'run-done1', sheetIds: ['sheetB1'], status: 'done' },
    'Charm_Nest_Sets/set-0003': { setId: 'set-0003', seq: 3, runId: 'run-done2', sheetIds: ['sheetC1'], status: 'done' },
    'Charm_Nest_Runs/run-open': bigRun('run-open', 'running', 300),
    'Charm_Nest_Runs/run-done1': bigRun('run-done1', 'complete', 300),
    'Charm_Nest_Runs/run-done2': bigRun('run-done2', 'abandoned', 300),
    // the live parts each record lists (Charm_Nest_Run_Live): the lines of the orders still in progress, up to 900 KB of JSON a part
    'Charm_Nest_Run_Live/a': { runId: 'x', json: JSON.stringify({ k1_1: { orderId: '1', pad: 'y'.repeat(200 * 1024) } }), bytes: 1, lines: 1, seq: 0 },
    'Charm_Nest_Run_Live/b': { runId: 'x', json: JSON.stringify({ k2_1: { orderId: '2', pad: 'y'.repeat(200 * 1024) } }), bytes: 1, lines: 1, seq: 1 }
  });
  const body = { op: 'flowState', sheetIds: ['sheetA1', 'sheetA2', 'sheetB1', 'sheetC1'], setIds: ['set-0001', 'set-0002', 'set-0003'] };
  const call = () => m.op('flowState', () => fn.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: {}, body: JSON.stringify(body) }));
  const a = m.snapshot();
  const r = await call();
  assert.strictEqual(r.statusCode, 200, r.body.slice(0, 300));
  const out = JSON.parse(r.body);
  const d = m.since(a);
  const runDocs = m.byCollection['Charm_Nest_Runs'] || { reads: 0, bytes: 0 };
  const ph = meter.perHour(d, 1200);        // one call every 3 s
  process.stdout.write(`flowState, 4 sheets on 3 runs: ${d.reads} reads, ${d.bytes} bytes (run records: ${runDocs.reads} reads, ${runDocs.bytes} bytes)\n`);
  process.stdout.write(`one open Library tab, 1,200 calls an hour: ${ph.reads} reads and ${(ph.bytes / 1048576).toFixed(0)} MiB an hour = ${ph.usd.toFixed(3)} USD an hour\n`);
  // the answer about the runs is what the page needs: exists and open
  assert.deepStrictEqual(out.runs, { 'run-open': { exists: true, open: true }, 'run-done1': { exists: true, open: false }, 'run-done2': { exists: true, open: false } });
  // budget: each run record is read whole at most once per call (by the readiness read), and flowState's own look at the run reads
  // the one field `status` (3 runs x 300 KB + a few bytes; before the change the same call read every record whole a second time)
  meter.assertMax({ reads: 0, aggs: 0, bytes: 0 }, {}, '');
  assert.ok(runDocs.bytes < 3 * 301 * 1024, 'run records read more than once whole: ' + runDocs.bytes);
  m.print(s => { if (process.env.FC1_VERBOSE) process.stdout.write(s + '\n'); });
  m.uninstall();
})().catch(e => { console.error(e); process.exit(1); });
