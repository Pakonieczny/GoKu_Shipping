#!/usr/bin/env node
// Read-only health check of the Listing Generator batch pipeline (2026-10-25).
//
//   node scripts/listing-batch-health.cjs [https://goldenspike.app] [--inputs N]
//
// Prints, for the newest submissions: sets saved / queued / active / saving / stopped, the most
// common error and how many sets it affects, the collector's last run, and the admission gate.
// --inputs N also asks the server (batch_input_check) what the input images of N failed jobs
// really are: the Storage label, the bytes, and what a submission now sends. Nothing is changed.
'use strict';

const origin = (process.argv.find((a) => /^https?:\/\//.test(a)) || 'https://goldenspike.app').replace(/\/+$/, '');
const inputsArg = process.argv.indexOf('--inputs');
const inputs = inputsArg > 0 ? Math.max(0, Number(process.argv[inputsArg + 1]) || 0) : 0;
const call = async (body) => {
  const resp = await fetch(`${origin}/.netlify/functions/geminiImageProxy`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
    signal: AbortSignal.timeout(60000),
  });
  const text = await resp.text();
  try { return JSON.parse(text); } catch { throw new Error(`HTTP ${resp.status}: ${text.slice(0, 200)}`); }
};
const ago = (ms) => (ms ? `${Math.round((Date.now() - ms) / 60000)} min ago` : 'never');

(async () => {
  const list = await call({ kind: 'batch_list', includeCollected: true, limit: 1000 });
  if (!list.ok) throw new Error(list.error?.message || 'batch_list failed');
  for (const s of list.sessions || []) {
    console.log(`\n${s.sessionId}  ${s.status}  planned ${s.planned}  saved ${s.complete}  active ${s.active}  queued ${s.queued}` +
      `  saving ${s.saving}  stopped ${s.blocked}  cancelled ${s.cancelled}  missing images ${s.missingImages}  checked ${ago(s.checkedAt)}`);
    const reasons = new Map();
    for (const set of s.issueSets || []) reasons.set(set.note || set.status, (reasons.get(set.note || set.status) || 0) + 1);
    for (const [why, n] of [...reasons].sort((a, b) => b[1] - a[1]).slice(0, 5)) console.log(`  ${n} set(s): ${why}`);
  }
  const errors = new Map();
  let fixes = 0;
  for (const b of list.batches || []) {
    fixes += (b.inputFixes || []).length;
    const seen = new Set((b.results?.failures || []).map((f) => `${f.source || '?'}: ${String(f.error || '').slice(0, 120)}`));
    for (const e of seen) errors.set(e, (errors.get(e) || 0) + 1);
  }
  console.log('\nMost common image failures (jobs):');
  for (const [e, n] of [...errors].sort((a, b) => b[1] - a[1]).slice(0, 6)) console.log(`  ${n}  ${e}`);
  console.log(`Input images relabelled or converted on recorded jobs: ${fixes}`);
  const sw = list.sweep || {};
  console.log(`\nCollector: ${sw.runningSince ? `running (${sw.stage})` : sw.stage}  last run ${ago(sw.lastSweepAt)}  ${JSON.stringify(sw.lastResult || {})}`);
  console.log(`Admission: ${JSON.stringify(list.admission || {})}${list.admissionError ? `  error: ${list.admissionError}` : ''}`);
  const failed = (list.batches || []).filter((b) => (b.results?.failures || []).length && !b.setComplete).slice(0, inputs);
  for (const b of failed) {
    const check = await call({ kind: 'batch_input_check', batchName: b.batchName });
    console.log(`\n${b.displayName || b.batchName}`);
    if (!check.ok) { console.log(`  ${check.error?.message || 'batch_input_check unavailable (not deployed yet?)'}`); continue; }
    for (const img of check.images || []) {
      console.log(`  ${img.role}: ${String(img.path).split('/').pop()}  stored ${img.storedType}  bytes ${img.bytes}  real ${img.sniffed}` +
        `  ${img.meta ? `${img.meta.width}x${img.meta.height} ${img.meta.space} ${img.meta.depth}${img.meta.isPalette ? ' palette' : ''}${img.meta.pages > 1 ? ` ${img.meta.pages} pages` : ''}` : img.metaError || ''}` +
        `  -> ${img.action}${img.sentType ? ` as ${img.sentType}` : ''}${img.error ? `: ${img.error}` : ''}${img.readError ? `read error: ${img.readError}` : ''}`);
    }
  }
})().catch((err) => { console.error(err.message || err); process.exitCode = 1; });
