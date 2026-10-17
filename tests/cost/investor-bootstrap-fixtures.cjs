// COST (Oct 2026) check: investorCycle-background calls ensureBootstrapped() about every 5 minutes, and it ran the 250 runtime fixtures
// (_investorSelftest.runFixturesAsync, ~86 s of CPU) on every call. A pass already recorded for THIS commit, less than a day old, is now reused.
// Run: node tests/cost/investor-bootstrap-fixtures.cjs     (no network, no secrets; in-memory Firestore from tests/cost/meter.cjs)
'use strict';
const assert = require('node:assert/strict');
const Module = require('module');
const path = require('path');
const meter = require('./meter.cjs');

const realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === '@google-cloud/firestore') {
    try { return realLoad.call(this, req, ...rest); } catch (e) {
      class X {}
      return { Firestore: X, FieldValue: meter.FieldValue, Timestamp: meter.Timestamp, DocumentReference: X, Transaction: X, WriteBatch: X };
    }
  }
  return realLoad.call(this, req, ...rest);
};
// the fixtures themselves are replaced by a counter: this test is about WHEN they run, not what they check
const fnDir = path.join(__dirname, '../../netlify/functions');
let runs = 0;
require.cache[require.resolve(path.join(fnDir, '_investorSelftest.js'))] = { id: 'selftest', loaded: true,
  exports: { runFixturesAsync: async () => { runs++; return { pass: true, fixtureHash: 'h-' + runs, passed: 1, total: 1, cases: [] } } } };
const A = require(path.join(fnDir, '_investorAdmin'));
const B = require(path.join(fnDir, '_investorBootstrap'));
const BOOTSTRAP_VERSION = Number(/const BOOTSTRAP_VERSION = (\d+)/.exec(require('fs').readFileSync(path.join(fnDir, '_investorBootstrap.js'), 'utf8'))[1]);

async function call(control, env) {
  const m = meter.create();
  m.db.seed({ 'InvestorAI_Control/control': control });
  const scope = { runId: 'sim_' + 'a'.repeat(24), collection: n => m.db.collection(n), transaction: fn => m.db.runTransaction(fn), batch: () => m.db.batch() };
  const before = runs, saved = process.env.COMMIT_REF;
  if (env === undefined) delete process.env.COMMIT_REF; else process.env.COMMIT_REF = env;
  try { await A.withSimulationScope(scope, () => B.ensureBootstrapped({ enrich: false })); } catch (e) { /* the rest of the bootstrap needs a real desk; only the fixtures matter here */ }
  finally { if (saved === undefined) delete process.env.COMMIT_REF; else process.env.COMMIT_REF = saved; }
  return runs - before;
}

(async () => {
  assert.equal(A.COL.control, 'InvestorAI_Control', 'control collection name');
  const now = Date.now(), fresh = meter.Timestamp.fromMillis(now - 600000), stale = meter.Timestamp.fromMillis(now - 25 * 3600000);
  const base = { bootstrapVersion: BOOTSTRAP_VERSION, fixturesPass: true, fixturesCommit: 'abc123', fixtureHash: 'h', fixturesCheckedAt: fresh };
  assert.equal(await call(base, 'abc123'), 0, 'a pass recorded for this commit is reused');
  assert.equal(await call({ ...base, fixturesCommit: 'old999' }, 'abc123'), 1, 'a new deploy runs them');
  assert.equal(await call({ ...base, fixturesPass: false }, 'abc123'), 1, 'a recorded failure runs them again');
  assert.equal(await call({ ...base, fixturesCheckedAt: stale }, 'abc123'), 1, 'a pass older than a day runs them again');
  assert.equal(await call({ ...base, bootstrapVersion: BOOTSTRAP_VERSION - 1 }, 'abc123'), 1, 'a pending bootstrap runs them');
  assert.equal(await call({ ...base, fixturesCommit: 'local' }, undefined), 1, 'no commit identity ("local"): always run');
  console.log('investor bootstrap fixtures: reused for the same commit (0 runs instead of 1 per investorCycle call), re-run on every other case');
})().catch(e => { console.error(e); process.exitCode = 1; });
