// FC11 cost check: what ONE call of the Investor page's polled reads costs, over an account that has traded for a while.
// Run: node tests/cost/fc11-page-reads-cost.cjs        (no network, no secrets, in-memory Firestore from tests/cost/meter.cjs)
//
// The page asks managerDashboard every 30 s (Manager view), portfolio every 10 s (Portfolio view) and executionEvents every 30 s, per open tab.
// This runs the real API handlers over a seeded history and reports the reads and bytes of one call, and checks that the answers did not change:
// each handler is run on the metered backend and on the same backend with field masks and `in` filters switched off (the old behaviour).
// Document sizes are assumptions (the real ones could not be read from here).
'use strict';
const assert = require('node:assert/strict');
const Module = require('module');
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
const A = require((process.env.FC11_ROOT || require('path').join(__dirname, '..', '..')) + '/netlify/functions/_investorAdmin');
const V2 = require((process.env.FC11_ROOT || require('path').join(__dirname, '..', '..')) + '/netlify/functions/_investorApiV2');

const KB = k => 'x'.repeat(Math.round(k * 1024));
const N = Number(process.env.TRADES || 150), SYMS = Number(process.env.SYMBOLS || 60);
const ACCT = 'paper-1', NOW = Date.UTC(2026, 9, 7, 15, 0, 0);

function history() {
  const d = {};
  for (let i = 0; i < N; i++) {
    const sym = 'S' + (i % SYMS), set = `shared_p${i % 40}_${sym}_${i}`;
    d[`${A.COL.orderSets}/${set}`] = { orderSetId: set, planId: 'shared_h' + (i % 40), accountId: ACCT, symbol: sym, coreVersion: 'core-1', status: 'CLOSED', entered: true, closed: true, reservedMinor: '0', createdAtMs: NOW - 86400000 - i, legs: [], note: KB(1.5) };
    d[`${A.COL.executionOutbox}/tr${i}`] = { transitionId: 'tr' + i, accountId: ACCT, status: i % 50 === 0 ? 'DEAD' : 'APPLIED', kind: 'APPLY_DESIRED_ORDER_SET', createdAtMs: NOW - 86400000 - i, note: KB(0.5) };
    d[`${A.COL.trades}/tc${i}`] = { accountId: ACCT, engineVersion: i % 5 === 0 ? 'legacy' : 'manager', realizedMinor: String(100 + i), note: KB(0.8) };
    d[`${A.COL.mandateEvents}/ev${i}`] = { accountId: ACCT, symbol: sym, kind: 'VALIDATED_AND_DESIRED', atMs: NOW - 86400000 - i, mandateVersionId: 'mv' + i, sequence: 1 };
    d[`${A.COL.brokerEvents}/be${i}`] = { accountId: ACCT, brokerEventId: 'be' + i, symbol: sym, type: 'FILL', atMs: NOW - 86400000 - i };
    d[`${A.COL.fills}/f${i}`] = { accountId: ACCT, orderSetId: set, side: i % 2 ? 'sell' : 'buy', receivedAtMs: NOW - 86400000 - i };
  }
  for (let s = 0; s < SYMS; s++) {
    d[`${A.COL.activeMandates}/${ACCT}_S${s}`] = { accountId: ACCT, symbol: 'S' + s, status: 'CLOSED', decision: 'BUY', desiredVersionId: 'mv' + s, appliedVersionId: 'mv' + s, note: KB(1.2) };
    d[`${A.COL.mandates}/mv${s}`] = { proposalId: 'mp' + s };
    d[`${A.COL.mandateProposals}/mp${s}`] = { proposal: { action: { protection: {} }, allocation: {}, thesis: KB(3) } };
  }
  for (let j = 0; j < 320; j++) d[`${A.COL.jobs}/job${j}`] = { jobId: 'job' + j, task: j % 2 ? 'execute' : 'event_ingest', status: 'complete', accountId: ACCT, enqueuedAtMs: NOW - 3600000 - j * 60000, finishedAtMs: NOW - 3500000 - j * 60000, targetFunction: 'investorExecution-background', summary: { note: KB(1) }, checkpoint: { stage: 'x', atMs: NOW, data: { note: KB(0.5) } } };
  d[`${A.COL.accounts}/${ACCT}`] = { balanceCents: { cash: 100000, reserved: 0, positions: 0 }, balanceRevision: 1, startingNavCents: 100000 };
  d[`${A.COL.control}/control`] = { accountId: ACCT, engineMode: 'manager', accountMode: 'PAPER_AI' };
  return d;
}
/* the same backend with the shortcuts of FC11 switched off: no select(), and `in` filters answered by a full scan of the equality part */
function oldBackend(m) {
  const wrapQ = q => new Proxy(q, { get(t, p) {
    if (p === 'select' || p === 'count') return undefined;
    if (p === 'where') return (f, op, v) => (op === 'in' ? (() => { throw Object.assign(new Error('9 FAILED_PRECONDITION: index'), { code: 9 }); })() : wrapQ(t.where(f, op, v)));
    const v = t[p]; return typeof v === 'function' ? v.bind(t) : v;
  } });
  return { COL: A.COL, FV: meter.FieldValue, TS: meter.Timestamp, envelope: () => ({}), col: n => wrapQ(m.db.collection(n)), doc: p => m.db.doc(p), runTransaction: fn => m.db.runTransaction(fn), batch: () => m.db.batch() };
}
function newBackend(m) { return { COL: A.COL, FV: meter.FieldValue, TS: meter.Timestamp, envelope: () => ({}), col: n => m.db.collection(n), doc: p => m.db.doc(p), runTransaction: fn => m.db.runTransaction(fn), batch: () => m.db.batch() }; }

async function call(action, mk, params = {}) {
  const m = meter.create(); m.db.seed(history());
  const admin = mk(m);
  const before = m.snapshot();
  const out = await m.op(action, () => V2.dispatch({ body: { action, params, apiVersion: 'investor.v2', requestId: 'req_fc11_cost_test_0001' }, admin, nowMs: NOW, authOverride: { ok: true, subject: 'operator' } }));
  return { m, d: m.since(before), out };
}
const bodyOf = o => (typeof o.body === 'string' ? JSON.parse(o.body) : o.body);
const strip = o => JSON.parse(JSON.stringify(o, (k, v) => (k === 'requestId' || k === 'correlationId' ? undefined : v)));

(async () => {
  const kb = n => (n / 1024).toFixed(0) + ' KB';
  const rowsOut = [];
  for (const [action, pollSec] of [['managerDashboard', 30], ['portfolio', 10], ['executionEvents', 30], ['jobs', 5], ['controlState', 30]]) {
    const oldR = await call(action, oldBackend), newR = await call(action, newBackend);
    if (oldR.out.statusCode !== 200) console.log(action, 'old status', oldR.out.statusCode, JSON.stringify(bodyOf(oldR.out)).slice(0, 400));
    assert.equal(newR.out.statusCode, oldR.out.statusCode, action + ' status');
    if (oldR.out.statusCode === 200) assert.deepEqual(strip(bodyOf(newR.out).data), strip(bodyOf(oldR.out).data), action + ': same answer with masks and `in` filters as without');
    const perHour = x => ({ reads: Math.round((x.reads + x.aggs) * 3600 / pollSec), mb: Math.round(x.bytes * 3600 / pollSec / 1048576) });
    if (process.env.FC11_DUMP) require('fs').appendFileSync(process.env.FC11_DUMP, JSON.stringify([action, strip(bodyOf(newR.out).data)]) + '\n');
    rowsOut.push({ action, pollSec, old: { reads: oldR.d.reads, bytes: oldR.d.bytes, h: perHour(oldR.d) }, now: { reads: newR.d.reads + newR.d.aggs, bytes: newR.d.bytes, h: perHour(newR.d) } });
  }
  console.log(`history: ${N} closed order sets, ${N} outbox rows, ${N} trades, ${N} events, ${N} fills, ${SYMS} symbol pointers`);
  for (const r of rowsOut) console.log(`${r.action} (page asks every ${r.pollSec} s): one call ${r.old.reads} reads / ${kb(r.old.bytes)} before, ${r.now.reads} reads / ${kb(r.now.bytes)} now;  per open tab-hour ${r.old.h.reads} -> ${r.now.h.reads} reads, ${r.old.h.mb} -> ${r.now.h.mb} MB`);
  console.log('investor page reads cost: answers unchanged, reads measured');
})().catch(e => { console.error(e); process.exitCode = 1; });
