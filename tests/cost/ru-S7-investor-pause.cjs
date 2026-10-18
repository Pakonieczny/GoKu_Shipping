// RU-S7 cost check: the Investor app is paused in code (CODE_PAUSE in _investorAdmin.js, 7 Oct 2026). While it is, every entry point that the
// minute cron or an open console page can reach must touch Firestore ZERO times. This test pins that, and measures what one tick would cost
// if the pause were lifted (so Paul can see the price of resuming before he does).
// Run: node tests/cost/ru-S7-investor-pause.cjs        (no network, no secrets, in-memory Firestore from tests/cost/meter.cjs)
//
// Part 1 (the pause): investorKick (1,440 invocations a day), investorApi (every page poll) and investorTuner (the runner on Paul's computer)
//   run against a Firestore whose every call counts and throws. Nothing may be constructed, read, written or listened to. The workers and
//   investorSession read the one auth document (control/authConfig, cached 60 s) and are listed, not changed: none can start without a
//   nonce that only the paused kick/API mint.
// Part 2 (resume price): the same kick with INVESTOR_CODE_PAUSE=off over an empty-ish desk in the manager engine, at four moments of the week.
//   The numbers are what the kick itself reads/writes; the workers it would start are not run (the dispatch is stubbed). MEASURED, with the
//   assumptions printed: an empty job queue, an empty simulation archive, a desk with no open positions.
'use strict';
const assert = require('node:assert/strict');
const Module = require('module');
const meter = require('./meter.cjs');

// ── the Firestore driver: every use is counted and refused, until Part 2 points it at the in-memory meter database ───────────────────
const touched = { calls: [] };
const driver = { target: null };
const refusing = new Proxy({}, { get: (t, k) => { if (typeof k === 'symbol' || k === 'then') return undefined; touched.calls.push('Firestore.' + String(k)); throw new Error('Firestore touched: ' + String(k)); } });
const realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === '@google-cloud/firestore') {
    class Delegating {
      constructor() { touched.calls.push('new Firestore'); if (!driver.target) return refusing; return new Proxy(this, { get: (t, k) => { const d = driver.target; const v = d[k]; return typeof v === 'function' ? v.bind(d) : v; } }); }
    }
    class X {}
    return { Firestore: Delegating, FieldValue: meter.FieldValue, Timestamp: meter.Timestamp, DocumentReference: X, Transaction: X, WriteBatch: X };
  }
  return realLoad.call(this, req, ...rest);
};
process.env.FIREBASE_PROJECT_ID = 'p'; process.env.FIREBASE_CLIENT_EMAIL = 'x@y.z'; process.env.FIREBASE_PRIVATE_KEY = 'k';
process.env.INVESTOR_PASSCODE = 'p'.repeat(20);
process.env.INVESTOR_SESSION_SECRET = 's'.repeat(40);
delete process.env.INVESTOR_CODE_PAUSE;
const FN = '../../netlify/functions/';
const A = require(FN + '_investorAdmin');
const kick = require(FN + 'investorKick.js');
const api = require(FN + 'investorApi.js');
const tuner = require(FN + 'investorTuner.js');
const WORKERS = [   // [file, a task it accepts]
  ['investorExecution-background.js', 'execute'], ['investorEvents-background.js', 'event_ingest'], ['investorDip-background.js', 'dip_watch'],
  ['investorDipSim-background.js', 'dip_simulation'], ['investorBars-background.js', 'bar_repository'], ['investorIngest-background.js', 'ingest'],
  ['investorManager-background.js', 'premarket_manager'], ['investorPostclose-background.js', 'postclose'], ['investorArchive-background.js', 'archive'],
  ['investorCycle-background.js', 'cycle'],
].map(([f, task]) => [f, task, require(FN + f)]);

// ── a clock the test moves ──────────────────────────────────────────────────────────────────────────────────────────────────────────
const RealDate = Date;
let fakeNow = null;
class FakeDate extends RealDate {
  constructor(...a) { if (a.length === 0 && fakeNow !== null) super(fakeNow); else super(...a); }
  static now() { return fakeNow !== null ? fakeNow : RealDate.now(); }
}
global.Date = FakeDate;

const scheduledEvent = { headers: { 'x-nf-event-source': 'scheduled' }, httpMethod: 'POST', body: '{}' };
const post = (body, headers = {}) => ({ httpMethod: 'POST', headers: { origin: 'https://goldenspike.app', ...headers }, body: JSON.stringify(body) });

(async () => {
  // ── Part 1: paused ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  assert.equal(A.codePaused(), true, 'CODE_PAUSE is active in the code');
  fakeNow = RealDate.UTC(2026, 9, 19, 15, 0, 0);          // a Monday 11:00 New York: the busiest minute of the week
  const logs = [];
  const origLog = console.log, origErr = console.error, origWarn = console.warn;
  console.log = console.error = console.warn = (...a) => logs.push(a.join(' '));
  let k, a1, a2, a3, t1;
  try {
    k = await kick.handler(scheduledEvent);
    a1 = await api.handler(post({ action: 'dashboard' }));
    a2 = await api.handler(post({ apiVersion: 'investor.v2', action: 'managerDashboard', requestId: 'req_s7' }));
    a3 = await api.handler(post({ apiVersion: 'investor.v2', action: 'jobs', requestId: 'req_s7b' }));
    t1 = await tuner.handler({ httpMethod: 'POST', headers: { authorization: 'Bearer nope' }, body: JSON.stringify({ action: 'hello' }) });
  } finally { console.log = origLog; console.error = origErr; console.warn = origWarn; }
  assert.equal(k.statusCode, 200); assert.equal(JSON.parse(k.body).paused, true, 'the kick says it is paused');
  for (const r of [a1, a2, a3]) assert.equal(r.statusCode, 503, 'the API answers 503 while paused');
  assert.equal(t1.statusCode, 503, 'the tuner door answers 503 while paused');
  assert.deepEqual(touched.calls, [], 'a paused tick, page poll or tuner poll touches Firestore zero times: ' + touched.calls.join(','));
  console.log(`PAUSED: investorKick x1, investorApi x3, investorTuner x1 -> ${touched.calls.length} Firestore calls (per day: 1,440 kicks -> 0 reads, 0 writes)`);

  // ── Part 1b: a stray POST to a public worker URL (no valid nonce) while paused ───────────────────────────────────────────────────
  // The workers are public URLs (Netlify background functions). Each reads the auth document first (cached 60 s per warm instance, so one read
  // per cold instance at most), then the claim refuses at once: claimOnce answers 503 while paused without a read, the legacy cycle worker
  // rejects the unsigned nonce without one. Time moves 2 minutes between calls so every call is a cold auth read, the worst case.
  {
    const m = meter.create();
    driver.target = m.db;
    console.log = console.error = console.warn = () => {};
    try { for (const [file, task, mod] of WORKERS) {
      fakeNow += 120000;
      const before = m.snapshot();
      const res = await m.op(file, () => mod.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ jobId: 'job_s7_probe', task, nonce: 'not-a-nonce', payload: { simId: 'x' } }) }));
      const d = m.since(before);
      assert(res.statusCode >= 400 && res.statusCode !== 202, file + ' refuses a stray call: ' + res.statusCode);
      assert(d.reads + d.aggs <= 1 && d.writes === 0 && d.deletes === 0, `${file}: a stray call reads at most the auth document and writes nothing (got ${d.reads}R/${d.writes}W)`);
    } } finally { console.log = origLog; console.error = origErr; console.warn = origWarn; }
    const r = m.report().total;
    console.log(`PAUSED, stray POSTs to ${WORKERS.length} public worker URLs (cold, worst case): ${r.reads} reads in total, ${r.writes} writes (the auth document, once each)`);
    driver.target = null;
  }

  // ── Part 2: what resuming would cost (kick only) ──────────────────────────────────────────────────────────────────────────────────
  process.env.INVESTOR_CODE_PAUSE = 'off';
  assert.equal(A.codePaused(), false);
  const B = require(FN + '_investorBootstrap');
  const baseControl = { engineMode: 'manager', writerEpoch: 1, fullScanCadenceVersion: kick.FULL_SCAN_CADENCE_VERSION, bootstrapVersion: B.BOOTSTRAP_VERSION,
    accountId: 'paper-1', accountMode: 'PAPER_AI', mode: 'PAPER_AI', executorEnabled: true, managerState: 'ACTIVE',
    lastIngestTradingDate: '2099-01-01', lastManagerRunDate: '2099-01-01', lastPostcloseDate: '2099-01-01', lastArchiveDate: '2099-01-01' };
  const realFetch = globalThis.fetch;
  globalThis.fetch = async () => ({ ok: true, status: 202 });
  async function tick(label, atUtcMs, { dip = false, ticks = 2 } = {}) {
    const m = meter.create();
    m.db.seed({ 'InvestorAI_Control/control': { ...baseControl, ...(dip ? { dip: { enabled: true } } : {}) } });
    driver.target = m.db;
    const out = [];
    for (let i = 0; i < ticks; i++) {
      fakeNow = atUtcMs + i * 60000;
      const before = m.snapshot();
      console.log = console.error = console.warn = () => {};
      let res;
      try { res = await m.op(label, () => kick.handler(scheduledEvent)); }
      finally { console.log = origLog; console.error = origErr; console.warn = origWarn; }
      const d = m.since(before);
      out.push({ reads: d.reads + d.aggs, writes: d.writes, body: JSON.parse(res.body) });
    }
    if (process.env.RU_S7_TABLE && label.startsWith(process.env.RU_S7_TABLE)) m.print();
    return { m, out };
  }
  const rows = [];
  const moments = [
    ['Sunday 03:00 ET (nothing due)', RealDate.UTC(2026, 9, 18, 7, 0, 0), false, 'night/weekend: 24/7'],
    ['Monday 02:00 ET (weeknight, nothing due)', RealDate.UTC(2026, 9, 19, 6, 0, 0), false, 'weeknight'],
    ['Monday 05:15 ET (overnight ingest pass starts)', RealDate.UTC(2026, 9, 19, 9, 15, 0), false, 'weeknight'],
    ['Monday 11:00 ET (market open, executor on)', RealDate.UTC(2026, 9, 19, 15, 0, 0), false, 'market hours'],
    ['Monday 11:00 ET (market open, executor + dip lane on)', RealDate.UTC(2026, 9, 19, 15, 0, 0), true, 'market hours'],
  ];
  for (const [label, at, dip, kind] of moments) {
    const r = await tick(label, at, { dip });
    const warm = r.out[1];
    rows.push({ label, kind, cold: r.out[0], warm });
    assert.equal(warm.body.ok, true, label + ' ran');
    // the price of an idle minute is pinned so a new task or a new per-tick query shows up here: 3 statuses x 13 manager tasks + 4 legacy tasks of
    // queue queries are the floor; measured 55 cold / 51 warm on 18 Oct 2026
    const idleKind = !dip && !/open|starts/.test(label);
    if (idleKind) { meter.assertMax({ reads: warm.reads, writes: warm.writes, bytes: 0, deletes: 0, aggs: 0 }, { reads: 56, writes: 2 }, label + ' (warm)'); assert(r.out[0].reads <= 60 && r.out[0].writes <= 2, label + ' (cold) ' + r.out[0].reads + 'R'); }
    else assert(warm.reads <= 64 && warm.writes <= 14, label + ': ' + warm.reads + 'R/' + warm.writes + 'W');
  }
  globalThis.fetch = realFetch;
  console.log('\nRESUME PRICE of ONE investorKick tick (manager engine, empty queue, empty simulation archive, no positions): cold = first tick on a fresh container, warm = next minute');
  for (const r of rows) {
    const dispatched = r.warm.body.dispatched ? r.warm.body.dispatched.length : 0;
    console.log(`  ${r.label.padEnd(58)} cold ${String(r.cold.reads).padStart(3)}R/${String(r.cold.writes).padStart(2)}W   warm ${String(r.warm.reads).padStart(3)}R/${String(r.warm.writes).padStart(2)}W  (${r.kind}; dispatched ${dispatched}; enqueued ${(r.warm.body.enqueued || []).length})`);
  }
  const idle = rows[0].warm;
  console.log(`  -> idle floor if resumed: ${idle.reads} reads + ${idle.writes} writes per minute = ${(idle.reads * 1440 / 1e3).toFixed(0)}k reads/day, ${(idle.writes * 1440 / 1e3).toFixed(1)}k writes/day, 24/7, before any worker runs`);
  delete process.env.INVESTOR_CODE_PAUSE;
  console.log('ru-S7 investor pause: paused path reads nothing; resume price measured');
})().catch(e => { console.error(e); process.exitCode = 1; });
