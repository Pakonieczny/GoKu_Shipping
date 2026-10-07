// FC11 cost check: the investor executor tick (investorExecution-background, started by investorKick EVERY MINUTE from 04:00 to 20:00 ET on a trading day).
// Run: node tests/cost/fc11-executor-cost.cjs        (no network, no secrets, in-memory Firestore from tests/cost/meter.cjs)
//
// What it measures: one IDLE tick (nothing open, nothing pending, nothing to fill) over an account that has traded for a while.
// Every read below is a read of HISTORY: closed order sets, their legs, the pointer of every symbol ever planned, the saved plans.
// What it proves: (1) the reads and bytes of an idle tick, (2) that the fills / outbox / expiry decisions are unchanged, because the same
// tick is also run with ONE live order set and compared to the same tick on a backend with no narrowing (every query answered whole).
// Document sizes are assumptions (the real ones could not be read from here): ORDERSET_KB, LEG_KB, POINTER_KB, PLAN_KB.
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
const X = require((process.env.FC11_ROOT || require('path').join(__dirname, '..', '..')) + '/netlify/functions/_investorExecution');

const KB = k => 'x'.repeat(Math.round(k * 1024));
const N = Number(process.env.TRADES || 150), SYMS = Number(process.env.SYMBOLS || 60), PLANS = Number(process.env.PLANS || 40);
const ORDERSET_KB = Number(process.env.ORDERSET_KB || 1.5), LEG_KB = Number(process.env.LEG_KB || 0.6), POINTER_KB = Number(process.env.POINTER_KB || 1.2), PLAN_KB = Number(process.env.PLAN_KB || 20);
const ACCT = 'paper-1', NOW = Date.UTC(2026, 9, 7, 15, 0, 0);

function history({ live = false, fill = false } = {}) {
  const d = {};
  for (let i = 0; i < N; i++) {
    const sym = 'S' + (i % SYMS), set = `shared_p${i % PLANS}_${sym}_${i}`, plan = 'shared_h' + (i % PLANS);
    d[`${A.COL.orderSets}/${set}`] = { orderSetId: set, planId: plan, accountId: ACCT, symbol: sym, coreVersion: 'core-1', status: 'CLOSED', entered: true, closed: true, reservedMinor: '0', legs: [], note: KB(ORDERSET_KB) };
    for (const role of ['STOP', 'TARGET', 'TIME_LIMIT']) d[`${A.COL.orderLegs}/${set}_${role}`] = { legId: `${set}_${role}`, orderSetId: set, accountId: ACCT, role, status: role === 'TARGET' ? 'FILLED' : 'CANCELLED', quantityUnits: '10', remainingUnits: '0', note: KB(LEG_KB) };
    d[`${A.COL.ledger}/t${i}a`] = { accountId: ACCT, legs: [{ account: 'cash', amountCents: -1000 }, { account: 'positions', amountCents: 1000 }] };
    d[`${A.COL.ledger}/t${i}b`] = { accountId: ACCT, legs: [{ account: 'cash', amountCents: 1100 }, { account: 'positions', amountCents: -1000 }, { account: 'realized_pnl', amountCents: -100 }] };
  }
  for (let s = 0; s < SYMS; s++) {
    d[`${A.COL.activeMandates}/${ACCT}_S${s}`] = { accountId: ACCT, symbol: 'S' + s, status: 'CLOSED', decision: 'BUY', desiredVersionId: 'mv' + s, appliedVersionId: 'mv' + s, note: KB(POINTER_KB) };
    d[`${A.COL.mandates}/mv${s}`] = { proposalId: 'mp' + s, researchVersionId: 'rv' + s };
    d[`${A.COL.mandateProposals}/mp${s}`] = { proposal: { action: { protection: {} }, allocation: {}, thesis: KB(3) } };
  }
  for (let p = 0; p < PLANS; p++) d[`${A.COL.portfolioPlans}/shared_h${p}`] = { accountId: ACCT, planHash: 'h' + p, policy: { coreVersion: 'core-1' }, investments: {}, sessions: [{ openMs: 1, closeMs: 2 }], note: KB(PLAN_KB) };
  d[`${A.COL.accounts}/${ACCT}`] = { balanceCents: { cash: N * 100, positions: 0, realized_pnl: -N * 100 }, balanceRevision: 1 };
  if (live) {   // one live shared plan: an entered position with a working stop and target, and its order set
    const set = 'shared_live_SXX', plan = 'shared_live';
    d[`${A.COL.orderSets}/${set}`] = { orderSetId: set, planId: plan, accountId: ACCT, symbol: 'SXX', coreVersion: 'core-1', status: 'WORKING', entered: true, reservedMinor: '0', legs: [] };
    for (const role of ['STOP', 'TARGET']) d[`${A.COL.orderLegs}/${set}_${role}`] = { legId: `${set}_${role}`, orderSetId: set, accountId: ACCT, role, status: 'WORKING', quantityUnits: '10', remainingUnits: '10', side: 'sell', type: role === 'STOP' ? 'STOP' : 'LIMIT', stopMicros: '1', priceMicros: '999999999999', workingSinceMs: 1 };
  }
  if (fill) {   // a legacy-style order set holds a position whose target a stored bar crosses: the tick must record the fill, close the position and post the journal
    d[`${A.COL.positions}/${ACCT}_SXX`] = { accountId: ACCT, symbol: 'SXX', open: true, quantityUnits: '10', qty: 10, costBasisMinor: '100000', costBasisCents: 100000, entryPriceUsd: 100, lastMarkUsd: 100, mandateVersionId: 'mvX', protectionState: 'PROTECTED_RTH' };
    d[`${A.COL.activeMandates}/${ACCT}_SXX`] = { accountId: ACCT, symbol: 'SXX', status: 'PROTECTED_RTH', decision: 'BUY', desiredVersionId: 'mvX' };
    d[`${A.COL.orderSets}/os_fill_SXX`] = { orderSetId: 'os_fill_SXX', accountId: ACCT, symbol: 'SXX', status: 'WORKING', mandateVersionId: 'mvX' };
    for (const [role, extra] of [['TARGET', { type: 'LIMIT', priceMicros: '110000000' }], ['STOP', { type: 'STOP', stopMicros: '95000000' }]]) d[`${A.COL.orderLegs}/os_fill_SXX_${role}`] = { legId: `os_fill_SXX_${role}`, orderSetId: 'os_fill_SXX', accountId: ACCT, role, side: 'sell', status: 'WORKING', quantityUnits: '10', remainingUnits: '10', workingSinceMs: 1, ...extra };
  }
  return d;
}

function makeAdmin(m) {
  return { COL: A.COL, FV: meter.FieldValue, TS: meter.Timestamp, envelope: () => ({}), now: () => NOW,
    col: n => m.db.collection(n), doc: p => m.db.doc(p), runTransaction: fn => m.db.runTransaction(fn), batch: () => m.db.batch() };
}
/* the same backend with no select()/count()/in-filter shortcut: every query answered whole, as the code did before FC11 */
function plainAdmin(m) {
  const a = makeAdmin(m);
  const strip = q => new Proxy(q, { get(t, p) { if (p === 'select' || p === 'count') return undefined; const v = t[p]; if (p === 'where') return (...x) => strip(v.apply(t, x)); return typeof v === 'function' ? v.bind(t) : v; } });
  return { ...a, col: n => strip(m.db.collection(n)) };
}

/* The worker calls X.tick with no admin, so everything goes through the _investorAdmin module (A): point A at the metered fake. */
async function idleTick(label, { live = false, plain = false, ticks = 1, fill = false } = {}) {
  const m = meter.create();
  m.db.seed(history({ live, fill }));
  const admin = plain ? plainAdmin(m) : makeAdmin(m);
  const saved = { col: A.col, runTransaction: A.runTransaction, batch: A.batch, now: A.now };
  A.col = admin.col; A.runTransaction = admin.runTransaction; A.batch = admin.batch; A.now = () => NOW;
  const out = [];
  try {
    const bg = require((process.env.FC11_ROOT || require('path').join(__dirname, '..', '..')) + '/netlify/functions/investorExecution-background');
    for (let i = 0; i < ticks; i++) {
      const before = m.snapshot();
      // with a live order set the real function would ask Alpaca for bars (network): the idle case only reads Firestore
      const bars = (live || fill) ? null : await m.op(label + ' barsForOpenSymbols', () => bg.barsForOpenSymbols(ACCT));
      const summary = await m.op(label + ' X.tick', () => X.tick({ adapter: { adapter: 'paper', cancelOrderSet: async () => ({ terminal: true }) }, accountId: ACCT, control: { engineMode: 'manager', accountMode: 'PAPER_AI', accountId: ACCT, buyState: 'OPEN' }, nowMs: NOW, metrics: {}, barsBySymbol: fill ? { SXX: [{ t: new Date(NOW - 60000).toISOString(), o: 100, h: 111, l: 99.5, c: 110, v: 5000000 }] } : {}, provenanceBySymbol: {} }));
      out.push({ d: m.since(before), summary, bars });
    }
    return { m, runs: out, d: out[out.length - 1].d, cold: out[0].d, summary: out[out.length - 1].summary, after: m.db.dump() };
  } finally { Object.assign(A, saved); }
}

(async () => {
  const kb = n => (n / 1024).toFixed(0) + ' KB';
  const a = await idleTick('idle', { ticks: 2 });
  const perDay = x => ({ reads: (x.reads + x.aggs) * 960, bytes: x.bytes * 960 });
  const day = perDay(a.d);
  console.log(`history: ${N} closed order sets (${N * 3} legs, ${N * 2} journal entries), ${SYMS} symbol pointers, ${PLANS} saved plans`);
  console.log(`first idle executor tick (cold container): ${a.cold.reads} reads, ${kb(a.cold.bytes)}, ${a.cold.aggs} count() calls`);
  console.log(`next idle tick (warm container): ${a.d.reads} reads, ${kb(a.d.bytes)} (+${a.d.aggs} count() calls), writes ${a.d.writes}  ->  per trading day (960 ticks) ${(day.reads / 1e6).toFixed(2)} M reads, ${(day.bytes / 1e9).toFixed(2)} GB`);
  a.m.print();
  assert.equal(a.summary.conservation && a.summary.conservation.pass, true, 'the books balance');
  // the point of the change: an idle tick no longer reads finished legs or any saved plan
  const byCol = a.m.report().byCollection;
  if (!process.env.FC11_ROOT) {   // (FC11_ROOT points the test at another checkout, e.g. the code before FC11, only to compare answers)
  assert((byCol['InvestorAI_OrderLegs'] || { reads: 0 }).reads <= 4, 'an idle tick reads no finished leg (an empty query is one read)');
  assert.equal(byCol['InvestorAI_PortfolioPlans'], undefined, 'an idle tick reads no saved plan');
  assert(a.d.bytes < 512 * 1024 * (N / 150) + 64 * 1024, 'order sets, pointers and positions are read through field masks (was 2.3 MB on the default fixture)');
  }

  // behaviour: the same tick with one live order set gives the same summary and the same database as the unnarrowed backend
  const live = await idleTick('live', { live: true });
  const plain = await idleTick('plain', { live: true, plain: true });
  const strip = s => JSON.parse(JSON.stringify(s, (k, v) => (k === 'nowMs' ? undefined : v)));
  const filled = await idleTick('fill', { fill: true });
  assert.equal(filled.summary.fills.fills.length, 1, 'the crossed target is filled once');
  assert.equal(filled.summary.fills.fills[0].role, 'TARGET');
  assert.equal(filled.after['InvestorAI_Positions/paper-1_SXX'].open, false, 'and the position is closed');
  if (process.env.FC11_DUMP) require('fs').writeFileSync(process.env.FC11_DUMP, JSON.stringify({ idle: strip(a.summary), idleDb: a.after, live: strip(live.summary), liveDb: live.after, fill: strip(filled.summary), fillDb: filled.after }));
  assert.deepEqual(strip(live.summary), strip(plain.summary), 'same summary with and without the narrowed reads');
  assert.deepEqual(live.after, plain.after, 'same database after the tick');
  console.log(`with one live order set: ${live.d.reads} reads / ${kb(live.d.bytes)} with masks and count(), ${plain.d.reads} reads / ${kb(plain.d.bytes)} on a backend without them (same summary, same database)`);

  // the executor's lean snapshot: everything the tick reads from it (positions, working orders, cash, NAV) equals the full snapshot's;
  // only the applied mandate/proposal of symbols that are neither held nor ordered is no longer fetched
  {
    const P = require((process.env.FC11_ROOT || require('path').join(__dirname, '..', '..')) + '/netlify/functions/_investorPortfolio');
    const m = meter.create(), docs = history({ live: true });
    docs[`${A.COL.positions}/${ACCT}_SXX`] = { accountId: ACCT, symbol: 'SXX', open: true, qty: 10, entryPriceUsd: 100, lastMarkUsd: 101, costBasisCents: 100000, coreVersion: 'core-1' };
    docs[`${A.COL.activeMandates}/${ACCT}_SXX`] = { accountId: ACCT, symbol: 'SXX', status: 'PROTECTED_RTH', appliedVersionId: 'mvX', lossBoundaryPriceMicros: '95000000' };
    docs[`${A.COL.mandates}/mvX`] = { proposalId: 'mpX', researchVersionId: 'rvX' };
    docs[`${A.COL.mandateProposals}/mpX`] = { proposal: { action: { protection: { lossBoundaryPriceMicros: '95000000', takeProfitPriceMicros: '110000000' } }, allocation: { usd: 1 } } };
    // a working (not yet entered) buy for another symbol: its protection level comes from the applied proposal
    docs[`${A.COL.orderSets}/shared_work_SYY`] = { orderSetId: 'shared_work_SYY', planId: 'shared_w', accountId: ACCT, symbol: 'SYY', coreVersion: 'core-1', status: 'AWAITING_STRATEGY_ENTRY', entered: false, reservedMinor: '5000', legs: [] };
    docs[`${A.COL.activeMandates}/${ACCT}_SYY`] = { accountId: ACCT, symbol: 'SYY', status: 'DESIRED', appliedVersionId: 'mvY' };
    docs[`${A.COL.mandates}/mvY`] = { proposalId: 'mpY' };
    docs[`${A.COL.mandateProposals}/mpY`] = { proposal: { action: { protection: { lossBoundaryPriceMicros: '90000000' } }, allocation: {} } };
    m.db.seed(docs);
    const admin = makeAdmin(m);
    const full = await m.op('full snapshot', () => P.snapshot({ accountId: ACCT, asOfMs: NOW, admin }));
    const lean = await m.op('lean snapshot', () => P.snapshot({ accountId: ACCT, asOfMs: NOW, admin, liveMandatesOnly: true }));
    if (!process.env.FC11_ROOT) for (const k of ['navMinor', 'settledCashMinor', 'reservedMinor', 'investedMinor', 'positions', 'workingOrders', 'aggregates', 'versions', 'reservationAccount']) assert.deepEqual(lean[k], full[k], 'snapshot field ' + k);
    assert.equal(full.positions[0].lossBoundaryPriceMicros, '95000000', 'the held symbol still gets its protection level from its mandate');
    assert.equal(lean.workingOrders.find((o) => o.symbol === 'SYY').lossBoundaryPriceMicros, '90000000', 'a working order still gets its protection level from its applied proposal');
    const r = m.report().byOp;
    if (!process.env.FC11_ROOT) assert(r['lean snapshot'].reads < r['full snapshot'].reads - 2 * (SYMS - 1) + 1, 'lean snapshot skips two documents per idle symbol: ' + r['lean snapshot'].reads + ' vs ' + r['full snapshot'].reads);
    console.log(`snapshot: full ${r['full snapshot'].reads} reads / ${kb(r['full snapshot'].bytes)}, lean ${r['lean snapshot'].reads} reads / ${kb(r['lean snapshot'].bytes)}; positions, orders, cash and NAV identical`);
  }
  if (process.env.EXPECT_FIXED) meter.assertMax(a.d, { reads: Number(process.env.MAX_READS), bytes: Number(process.env.MAX_BYTES) }, 'idle executor tick');
  console.log('investor executor cost: behaviour unchanged, reads measured');
})().catch(e => { console.error(e); process.exitCode = 1; });
