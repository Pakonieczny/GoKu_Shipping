// FC8 cost check: etsyPricingScheduleCron (every 5 minutes).  Run: node tests/cost/etsy-pricing-cron-cost.cjs   (offline, tests/cost/meter.cjs)
//   idle tick (schedule off / not due) ......... 1 heartbeat write + 1 empty paused-runs query + 1 schedule read, 288 times a day
//   due tick ................................... one read of the whole EtsyPricing_Listings collection; only three flags are used, so only those are read now
// Nothing is dispatched to Etsy here: fetch is replaced by a recorder.
'use strict';
const assert = require('node:assert/strict');
const meter = require('./meter.cjs');

(async () => {
  const m = meter.create();
  const docs = {};
  const N = 600, BIG = 'x'.repeat(6 * 1024);          // a listing document also carries its inventory snapshot (assumed 6 KB, not measurable here)
  for (let i = 0; i < N; i++) docs['EtsyPricing_Listings/l' + i] = { chain_set: i % 3 !== 0, engrave_set: i % 2 === 0, batched: i % 5 === 0, title: 'T' + i, inventory: BIG };
  const dueAt = Date.now() - 1000;
  docs['EtsyPricing_Config/schedule'] = { enabled: true, repeat: 'daily', next_run_at: dueAt };
  m.db.seed(docs);
  m.install();
  const sent = [];
  global.fetch = async (url, o) => { sent.push({ url, body: o && o.body }); return { ok: true, status: 202 }; };
  process.env.URL = 'https://example.test';
  const cron = require('../../netlify/functions/etsyPricingScheduleCron.js');
  const before = m.snapshot();
  const out = await m.op('etsyPricingScheduleCron.dueTick', () => cron.handler());
  const due = m.since(before);
  m.uninstall();

  const expected = [];
  for (let i = 0; i < N; i++) if (i % 3 !== 0 && i % 2 === 0 && i % 5 !== 0) expected.push('l' + i);
  assert(/^started /.test(out.body), 'a due schedule starts a run: ' + out.body);
  const run = Object.entries(m.db.dump()).find(([p]) => p.startsWith('EtsyPricing_Runs/'))[1];
  assert.deepEqual(run.ids.slice().sort(), expected.slice().sort(), 'the same listings are queued');
  assert.equal(sent.length, 1, 'one hand-off to the batch worker');
  console.log(`due tick over ${N} listings (${(BIG.length / 1024).toFixed(0)} KB each): ${due.reads} reads, ${(due.bytes / 1024).toFixed(0)} KB, ${due.writes} writes`);
  console.log('listing read alone: ' + (m.report().byCollection['EtsyPricing_Listings'].bytes / 1024).toFixed(0) + ' KB (was ' + (N * (BIG.length + 90) / 1024).toFixed(0) + ' KB before the field mask)');
  meter.assertMax({ reads: due.reads, bytes: m.report().byCollection['EtsyPricing_Listings'].bytes }, { reads: N + 6, bytes: N * 120 }, 'listing read is flags only');
  console.log('etsy pricing cron cost: same queue, flags only');
})().catch(e => { console.error(e); process.exitCode = 1; });
