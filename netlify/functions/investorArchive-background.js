/*  netlify/functions/investorArchive-background.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the nightly bounded archive (blueprint §10.6, §11.1, §12.1).
 *
 *  Two responsibilities, both bounded and both recorded in ArchiveRuns:
 *    1. EXPORT — the day's high-volume operational records (broker events,
 *       fills, mandate events, executed outbox transitions, model requests,
 *       evidence deltas, finished jobs) are bundled once, content-addressed
 *       and stored through the content store under kind "archive"; the run
 *       document carries the manifest reference and per-collection counts.
 *    2. RETENTION MANIFEST — the declared retention policy is written with
 *       the run so an operator can see what may be purged after which date.
 *       Nothing here deletes: the audit chain that reproduces a decision is
 *       never removed (§10.6), and any purge of exported raw data is a
 *       separate, explicit operator action against the manifest.
 *  The legacy intraday-bar completion (display only) is preserved as step 3
 *  when a live bar provider is bound, so the intraday record stays whole.
 * ---------------------------------------------------------------------------
 */

"use strict";

/* invariant: the archive never loads the model gateway (measured against
   what this module's own imports bring in; the fixture set walks the
   source graph as the second lock). */
const preloadedModules = new Set(Object.keys(require.cache));
const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const M = require("./_investorMarket");
const JOBS = require("./_investorJobs");
const CS = require("./_investorContentStore");
const { redact } = require("./_investorAuth");

const FN_NAME = "investorArchive-background";
const TASK = "archive";
const ARCHIVE_SCHEMA = "archive-run.v1";
const MAX_DOCS_PER_COLLECTION = 2000;
const FULL_SESSION_BARS = 78, HALF_SESSION_BARS = 42;
if (Object.keys(require.cache).some((k) => !preloadedModules.has(k) && /_investorOpenai\.js$/.test(k))) throw new Error("archive must not load the model gateway");

/** Explicit retention policy (§10.6). Days are minimums; "never" marks the audit chain. */
const RETENTION = Object.freeze({
  brokerEvents:        { days: 2555, class: "raw_operational",  purgeAllowedAfterExport: true },
  executionOutbox:     { days: 400,  class: "raw_operational",  purgeAllowedAfterExport: true },
  modelRequests:       { days: 400,  class: "raw_operational",  purgeAllowedAfterExport: true },
  jobs:                { days: 90,   class: "raw_operational",  purgeAllowedAfterExport: true },
  marketLatest:        { days: 400,  class: "raw_market",       purgeAllowedAfterExport: true },
  evidenceDeltas:      { days: 730,  class: "evidence",         purgeAllowedAfterExport: false },
  fills:               { days: null, class: "audit_chain",      purgeAllowedAfterExport: false },
  mandateEvents:       { days: null, class: "audit_chain",      purgeAllowedAfterExport: false },
  managerDecisions:    { days: null, class: "audit_chain",      purgeAllowedAfterExport: false },
  ledger:              { days: null, class: "audit_chain",      purgeAllowedAfterExport: false },
  activationSnapshots: { days: null, class: "audit_chain",      purgeAllowedAfterExport: false },
});
/** What the daily bundle exports: collection → the field that dates a record. */
const EXPORTS = Object.freeze([
  { collection: "brokerEvents",    field: "atMs" },
  { collection: "fills",           field: "atMs" },
  { collection: "mandateEvents",   field: "atMs" },
  { collection: "executionOutbox", field: "createdAtMs" },
  { collection: "modelRequests",   field: "updatedAtMs" },
  { collection: "evidenceDeltas",  field: "firstSeenAtMs" },
  { collection: "jobs",            field: "finishedAtMs" },
]);

function rows(snap) { const out = []; if (snap && typeof snap.forEach === "function") snap.forEach((d) => out.push(d.data())); return out; }
function dayWindowMs(tradingDate) {
  const start = M.nyWallClockToUtcMs(tradingDate, 0);
  const end = start != null ? start + 24 * 3600 * 1000 : null;
  return { startMs: start, endMs: end };
}
function purgeAfter(tradingDate, days) {
  if (days == null) return null;
  const t = Date.parse(`${tradingDate}T00:00:00Z`);
  return Number.isFinite(t) ? new Date(t + days * 86400000).toISOString().slice(0, 10) : null;
}
function retentionManifest(tradingDate) {
  return Object.fromEntries(Object.entries(RETENTION).map(([c, r]) => [c, { ...r, purgeEligibleFrom: purgeAfter(tradingDate, r.days), exportedInBundle: EXPORTS.some((e) => e.collection === c) }]));
}

/* 1. export ────────────────────────────────────────────────────────────── */
async function collectDay({ D, accountId, tradingDate, limit = MAX_DOCS_PER_COLLECTION }) {
  const { startMs, endMs } = dayWindowMs(tradingDate);
  if (startMs == null) throw Object.assign(new Error(`invalid trading date ${tradingDate}`), { code: "ARCHIVE_INVALID_DATE" });
  const bundle = { schemaVersion: "archive-bundle.v1", accountId, tradingDate, windowMs: { startMs, endMs }, collections: {} };
  const counts = {};
  for (const e of EXPORTS) {
    const col = D.COL[e.collection];
    let docs = [];
    try { docs = rows(await D.col(col).where(e.field, ">=", startMs).where(e.field, "<", endMs).limit(limit).get()); }
    catch (err) { counts[e.collection] = { error: String(err.code || err.message).slice(0, 80) }; continue; }
    const mine = docs.filter((d) => !d.accountId || d.accountId === accountId).slice(0, limit);
    bundle.collections[e.collection] = mine;
    counts[e.collection] = { exported: mine.length, truncated: docs.length >= limit };
  }
  return { bundle, counts };
}
async function exportBundle({ bundle, contentStore, tradingDate, accountId }) {
  const store = contentStore || CS;
  try { store.assertAvailable(); } catch (e) { return { exported: false, reason: e.code || "content_store_unavailable", message: String(e.message).slice(0, 120) }; }
  const put = await store.putJson("archive", bundle, { createdBy: FN_NAME, encoding: "gzip", asOf: `${tradingDate}T23:59:59Z`, meta: { accountId, tradingDate, kind: "daily_operational_export" } });
  const m = put.manifest || put;
  return { exported: true, existed: put.existed === true, contentHash: m.contentHash || null, storageUri: m.storageUri || null, bytes: m.bytes || null, plainBytes: m.plainBytes || null, encoding: m.encoding || null, records: Object.values(bundle.collections).reduce((n, a) => n + a.length, 0) };
}

/* 3. legacy intraday completion (display only; needs a live provider) ──── */
async function completeIntradayBars({ D, tradingDate, isHalfDay, market = M, universe = null }) {
  const provider = market.activeProvider();
  if (!provider || provider.id === "manual") return { skipped: true, reason: "manual_provider" };
  const U = universe || require("./_investorUniverse");
  const symbols = (U.tradeTier || []).map((t) => t.symbol);
  const expected = isHalfDay ? HALF_SESSION_BARS : FULL_SESSION_BARS;
  const counts = {};
  const snaps = await Promise.all(symbols.map((sym) => D.col(D.COL.marketLatest).doc(market.barDocId(sym, tradingDate)).get()));
  snaps.forEach((d, i) => { counts[symbols[i]] = d.exists ? Number(d.data().barCount || (d.data().bars || []).length) : 0; });
  const short = symbols.filter((sym) => counts[sym] < expected - 2);
  const out = { symbols: symbols.length, expectedBars: expected, short: short.length, fetched: 0, completed: 0, stillShort: [], writeErrors: 0 };
  if (!short.length) return out;
  const got = await market.fetchBarsChunked(short, { timeframe: "5Min", limit: 90 }, { chunkSize: 60, retries: 1 });
  const bars = got.bars || {};
  for (const sym of short) {
    const b = bars[sym] || [];
    if (!b.length) continue;
    out.fetched += 1;
    try {
      await market.writeBars(sym, tradingDate, b, { provider: got.provider, feed: got.feed || null, adjustment: got.adjustment || null,
        sourceSha256: (got.symbolSha256 && got.symbolSha256[sym]) || got.sha256 || null, grade: null, gradeReasons: ["nightly_archive"], feedDelayMinutes: provider.delayMinutes });
    } catch { out.writeErrors += 1; }
  }
  const after = await Promise.all(short.map((sym) => D.col(D.COL.marketLatest).doc(market.barDocId(sym, tradingDate)).get()));
  after.forEach((d, i) => { const n = d.exists ? Number(d.data().barCount || (d.data().bars || []).length) : 0; if (n >= expected - 2) out.completed += 1; else out.stillShort.push(`${short[i]}:${n}`); });
  out.stillShort = out.stillShort.slice(0, 40);
  return out;
}

/* the whole run, injectable for the deploy attestation ───────────────── */
async function runArchive({ accountId, tradingDate, admin = null, contentStore = null, market = null, isHalfDay = false, nowMs = Date.now(), completeBars = false } = {}) {
  const D = admin || A;
  const runId = `archive_${String(accountId).replace(/[^a-zA-Z0-9_-]/g, "_")}_${tradingDate}`;
  const runRef = D.col(D.COL.archiveRuns).doc(runId);
  const summary = { schemaVersion: ARCHIVE_SCHEMA, runId, kind: "archive", accountId, tradingDate, startedAtMs: nowMs, status: "running", counts: null, export: null, retention: retentionManifest(tradingDate), intraday: null, errors: [] };
  await runRef.set({ ...summary, ...D.envelope({ created_by: FN_NAME }) }, { merge: true });
  try {
    const { bundle, counts } = await collectDay({ D, accountId, tradingDate });
    summary.counts = counts;
    summary.export = await exportBundle({ bundle, contentStore, tradingDate, accountId });
  } catch (e) { summary.errors.push({ step: "export", code: e.code || null, message: String(e.message).slice(0, 160) }); }
  if (completeBars) {
    try { summary.intraday = await completeIntradayBars({ D, tradingDate, isHalfDay, market: market || M }); }
    catch (e) { summary.errors.push({ step: "intraday", code: e.code || null, message: String(e.message).slice(0, 160) }); }
  }
  summary.status = summary.errors.length ? "dead" : "complete";
  summary.finishedAtMs = Date.now();
  summary.elapsedMs = summary.finishedAtMs - nowMs;
  summary.deleted = 0; /* by design: this handler never purges (§10.6) */
  await runRef.set(summary, { merge: true });
  const patch = { lastArchiveAtMs: summary.finishedAtMs, lastArchiveSummary: { runId, status: summary.status, tradingDate, counts: summary.counts, export: summary.export, errors: summary.errors, intraday: summary.intraday ? { short: summary.intraday.short, completed: summary.intraday.completed, stillShort: (summary.intraday.stillShort || []).length } : null } };
  if (summary.status === "complete") patch.lastArchiveDate = tradingDate;
  await D.col(D.COL.control).doc("control").set(patch, { merge: true });
  return summary;
}

exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return { statusCode: 400, body: JSON.stringify({ error: "invalid JSON" }) }; }
  const { jobId, task, nonce, payload = {} } = body;
  if (task !== TASK || !jobId) return { statusCode: 400, body: JSON.stringify({ error: "invalid job shape" }) };
  await AUTH.loadAuthSecrets();
  const claimed = await JOBS.claimOnce({ jobId, task: TASK, targetFunction: FN_NAME, token: nonce, payload });
  if (!claimed.claimed) return { statusCode: claimed.httpStatus || 409, body: JSON.stringify({ ok: false, reason: claimed.reason }) };
  const claim = claimed.claim;
  await M.loadMarketSettings();
  const ctrlSnap = await A.col(A.COL.control).doc("control").get();
  const ctrl = ctrlSnap.exists ? ctrlSnap.data() : {};
  const session = M.sessionState(new Date());
  const accountId = String(payload.accountId || ctrl.accountId || "paper-1");
  const tradingDate = String(payload.tradingDate || session.date);
  try {
    if (ctrl.engineMode !== "manager") { await JOBS.complete(claim, { skipped: true, reason: "engine_mode_legacy" }); return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: true }) }; }
    const summary = await runArchive({ accountId, tradingDate, isHalfDay: session.isHalfDay === true, completeBars: session.tradingDay === true && tradingDate === session.date, nowMs: Date.now() });
    const compact = { ...summary, retention: undefined };
    if (summary.status === "complete") await JOBS.complete(claim, compact);
    else await JOBS.failClosed(claim, { code: "ARCHIVE_STEP_FAILED", message: summary.errors.map((e) => `${e.step}:${e.code || e.message}`).join("; ").slice(0, 300), retryable: true, data: compact });
    return { statusCode: 200, body: JSON.stringify({ ok: summary.status === "complete", summary: compact }) };
  } catch (e) {
    console.error("investorArchive failed", redact({ jobId, error: e.message, stack: (e.stack || "").slice(0, 400) }));
    await JOBS.failClosed(claim, { code: e.code || "ARCHIVE_FAILED", message: e.message, retryable: true }).catch(() => ({}));
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};

exports.FN_NAME = FN_NAME;
exports.RETENTION = RETENTION;
exports.EXPORTS = EXPORTS;
exports.retentionManifest = retentionManifest;
exports.dayWindowMs = dayWindowMs;
exports.collectDay = collectDay;
exports.exportBundle = exportBundle;
exports.runArchive = runArchive;
