// RU-S5: what an IDLE invocation of every scheduled function reads and writes, and what that adds up to in a day.
// Run: node tests/cost/ru-S5-cron-idle-budget.cjs        (no network, no secrets, in-memory Firestore from tests/cost/meter.cjs)
//
// Why: netlify.toml / exports.config schedule ~25 functions, 1,440 of whose invocations a day (investorKick, etsyMailGmailCron,
// etsyMailListingCreatorCron) fire every minute and ~1,500 more every 3-10 minutes. Nothing is "happening" in the shop at 3 am, yet each
// of them reads Firestore on every tick. This test drives the real handler of every cron that can run offline, on a world shaped like
// production when nothing is going on (400 drafts and threads, 40 open order-link questions, 60 saved custom designs, 10 open station
// sessions), twice: the first call is the cold tick, the second the warm one (what 99 % of ticks are). It fails when
//   (1) a function's idle tick reads or writes more than its budget below (budgets are the measured numbers + a little slack; never loosen one),
//   (2) the day's idle total goes over TOTAL_READS_PER_DAY, or
//   (3) a function is scheduled in netlify.toml / exports.config and is neither driven here nor listed in NOT_DRIVEN with a stated budget
//       (a new cron has to come with a number).
// What is NOT measured here is stated in NOT_DRIVEN (Etsy / Shopify / Google calls; background workers a cron kicks) with the figure from reading the code.
// `.select()` does not lower a read count; the number of documents a query returns is what is billed, so the fixtures below matter:
// etsyMailReapers' tracking pass reads the newest 100 drafts and its order-link pass every open question and its thread on EVERY tick.
'use strict';
const fs = require('fs'), path = require('path'), Module = require('module'), assert = require('node:assert/strict');
const meter = require('./meter.cjs');
const ROOT = path.join(__dirname, '..', '..');
const say = s => process.stdout.write(s + '\n');

/* ── no network: node-fetch and fetch answer an empty, successful reply and remember the URL ── */
const calls = [];
const reply = body => ({ ok: true, status: 200, headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body), buffer: async () => Buffer.alloc(0) });
const fakeFetch = async (url) => { calls.push(String(url)); return reply({ messages: [], resultSizeEstimate: 0, results: [], count: 0 }); };
global.fetch = fakeFetch;
const realLoad = Module._load;
Module._load = function (req, ...r) { return req === 'node-fetch' ? fakeFetch : realLoad.call(this, req, ...r); };

const m = meter.create();
m.install({ app: () => ({}) });
Object.assign(process.env, { URL: 'https://example.test', ETSYMAIL_EXTENSION_SECRET: 'meter-secret', CONTEXT: 'dev', GMAIL_CLIENT_ID: 'id', GMAIL_CLIENT_SECRET: 's',
  SHOP_ID: '1', CLIENT_ID: 'c', CLIENT_SECRET: 's', SHOPIFY_STORE: 'x.myshopify.com' });
const F = n => require(path.join(ROOT, 'netlify/functions', n + '.js'));
const T = ms => meter.Timestamp.fromMillis(ms), NOW = Date.now();
const sched = { headers: { 'x-nf-event-source': 'scheduled' }, httpMethod: undefined, body: '' };
const nf = { headers: { 'x-nf-event': 'schedule' }, httpMethod: 'POST', body: '' };

/* ── the world: quiet, production-shaped ── */
const seed = {};
for (let i = 0; i < 400; i++) {
  seed['EtsyMail_Drafts/draft_etsy_conv_' + i] = { threadId: 'etsy_conv_' + i, status: 'sent', createdAt: T(NOW - i * 3600e3), trackingImages: [], text: 'x'.repeat(2000) };
  seed['EtsyMail_Threads/etsy_conv_' + i] = { customerName: 'Buyer ' + i, subject: 's' + i, status: 'auto_replied', updatedAt: T(NOW - i * 3600e3 - 86400e3), orderLinkIds: i < 40 ? ['eng' + i] : [], lastInboundAt: T(NOW - 86400e3), lastOutboundAt: T(NOW - 86400e3) };
}
for (let i = 0; i < 40; i++) seed['EtsyMail_OrderLinks/eng' + i] = { id: 'eng' + i, receiptId: '100' + i, status: 'open', threadId: 'etsy_conv_' + i, customer: {}, outbox: [], checkedToMs: NOW, pulledToMs: NOW, startedAtMs: NOW - 86400e3, createdAtMs: NOW - 86400e3, lastInboundAtMs: NOW - 3600e3, lastOutboundAtMs: NOW - 3600e3 };
seed['EtsyMail_Config/messageDedupeSweep'] = { version: 1, status: 'done' };                                       // (_etsyMailMessageCopies SWEEP_VERSION)            // the one-time sweep of every thread is finished
seed['EtsyMail_Config/unmangleScan'] = { wmSec: Math.floor((NOW - 86400e3) / 1000) + 1, wmNs: 0 };              // the unmangle walk has caught up with the newest thread
seed['EtsyMail_Config/gmailWatcher'] = { enabled: false };
seed['EtsyMail_SendQueueMeta/rev'] = { n: 5, maintN: 5, dueMs: 0, houseMs: 0 }; seed['EtsyMail_SendQueueMeta/lease'] = {};
seed['config/gmailOauth'] = { access_token: 'a', refresh_token: 'r', expires_at: NOW + 3600e3 };
seed['config/etsyOauth'] = { access_token: 'a', refresh_token: 'r', expires_at: NOW + 3600e3, expires_at_ms: NOW + 3600e3 };
seed['EtsyPricing_Config/schedule'] = { enabled: false };
for (let i = 0; i < 8; i++) seed['Station_Sessions/s' + i] = { person: 'P' + i, station: 'assembly', device: 'assembly-1', startAt: NOW - 3600e3, lastSeenAt: NOW - 60e3, lastInputAt: NOW - 60e3, endAt: null, admin: false };
for (let i = 0; i < 2; i++) seed['Station_Sessions/a' + i] = { person: 'Admin' + i, station: 'design', device: 'design-1', startAt: NOW - 7200e3, lastSeenAt: NOW - 60e3, lastInputAt: NOW - 3600e3, endAt: null, admin: true };
seed['config/stationAdmins'] = { names: ['Admin0'] };
for (let i = 0; i < 60; i++) {
  seed['Charm_Custom_Saves/r' + i] = { rid: 'r' + i, sku: 'SKU' + i, listingId: '1000' + i, phase: 'repo', at: NOW - i * 1000, alias: 'whole', charmHash: 'h' + i, etsy: { state: 'linked' } };
  seed['Charm_Master_Index/SKU' + i] = { sku: 'SKU' + i, hashSource: 'custom-save', charmHash: 'h' + i, aiPath: 'a' + i, thumbPath: 't' + i };
  seed['Charm_Sku_Aliases/1000' + i] = { sku: 'SKU' + i, v: 2, keep: { sku: 'SKU' + i, rid: 'r' + i } };
}
seed['Brites_Editor_Meta/siteFixesState'] = { labelsDone: true, structureDone: true, returnsDone: true, checkoutBranded: true };
seed['Brites_Editor_Meta/charmSetsState'] = { done: true, idx: 10, dataVersion: require(path.join(ROOT, 'netlify/functions/charmSetsData.js')).DATA_VERSION };
seed['Brites_Editor_Meta/googleAttributesState'] = { done: true, completedAt: new Date().toISOString() };
m.db.seed(seed);
for (let i = 0; i < 60; i++) { m._storage.files.set('a' + i, Buffer.alloc(8)); m._storage.files.set('t' + i, Buffer.alloc(8)); }       // the saved designs' files are in Storage

/* ── cron string -> invocations a day (the shapes netlify.toml uses) ── */
function perDay(cron) {
  const [mi, h, , , dow] = String(cron).trim().split(/\s+/);
  const every = f => { const x = /^(?:\*|\d+-\d+)\/(\d+)$/.exec(f); return x ? Number(x[1]) : /^\*$/.test(f) ? 1 : 0; };
  let n;
  if (mi === '*') n = 1440;
  else if (every(mi)) n = 1440 / every(mi);
  else n = h === '*' ? 24 : every(h) ? 24 / every(h) : 1;
  return dow !== '*' && /^\d$/.test(dow) ? n / 7 : n;
}

/* ── the table: how to drive each function, and its budget (the measured idle tick; reads/writes per invocation) ──
 *   warm / cold  budgets for the second and the first call; avg(cold, warm) -> the mean reads of an invocation over a day when the ticks differ */
const keepState = () => { const L = require(path.join(ROOT, 'netlify/functions/_charmNestCustomRepair.js')); m.db.seed({ [L.STATE + '/state']: { lastRunAt: Date.now() - 11 * 60000, dailyAt: Date.now(), runningUntil: 0, cursor: 0 } }); };   // a pass 11 minutes after the last one, the daily steps already done
const TICKS = [
  { fn: 'etsyMailGmailCron', cron: '* * * * *', call: () => F('etsyMailGmailCron').handler({}), warm: { reads: 1, writes: 0 }, cold: { reads: 1, writes: 0 }, note: 'watcher switched off (the default): one flag read' },
  { fn: 'etsyMailGmailCron (watcher on)', cron: '* * * * *', before: () => m.db.seed({ 'EtsyMail_Config/gmailWatcher': { enabled: true }, 'EtsyMail_Config/gmailSyncState': { lastSyncInProgress: false, lastInternalDateMs: NOW - 3600e3 } }), call: () => F('etsyMailGmailCron').handler({}), warm: { reads: 2, writes: 0 }, cold: { reads: 2, writes: 0 }, note: 'only if the inbox switch is on; hands over to etsyMailGmail-background below', skipTotal: true },
  { fn: 'etsyMailGmail-background (watcher on, no mail)', cron: '* * * * *', call: () => F('etsyMailGmail-background').handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ cronTriggered: true }) }), warm: { reads: 4, writes: 2 }, cold: { reads: 5, writes: 2 }, note: 'kicked by the cron above every minute while the watcher is on: sync state, Gmail token, breaker, 2 state writes', skipTotal: true, after: () => m.db.seed({ 'EtsyMail_Config/gmailWatcher': { enabled: false } }) },
  { fn: 'etsyMailListingCreatorCron', cron: '* * * * *', call: () => F('etsyMailListingCreatorCron').handler(sched), warm: { reads: 3, writes: 0 }, cold: { reads: 3, writes: 0 }, note: 'queued, stuck, reply-settle: three empty queries (an empty query still bills 1 read)' },
  { fn: 'investorKick', cron: '* * * * *', call: () => F('investorKick').handler(sched), warm: { reads: 0, writes: 0 }, cold: { reads: 0, writes: 0 }, note: 'code pause (_investorAdmin CODE_PAUSE): nothing read', only: () => require(path.join(ROOT, 'netlify/functions/_investorAdmin.js')).codePaused() },
  { fn: 'etsyMailDraftSendCleanupCron', cron: '*/3 * * * *', call: () => F('etsyMailDraftSendCleanupCron').handler({}), warm: { reads: 2, writes: 0 }, cold: { reads: 11, writes: 1 }, avg: (c, w) => 0.25 * c + 0.75 * w, note: 'queue upkeep: the full pass (cold) runs about every 4th tick' },
  { fn: 'etsyMailReceiptsMirrorCron', cron: '*/3 * * * *', before: () => m.db.seed({ 'EtsyMail_Config/receiptsMirrorState': { enabled: true, nextEligibleAtMs: Date.now() + 600000 } }), call: () => F('etsyMailReceiptsMirrorCron').handler({}), warm: { reads: 1, writes: 0 }, cold: { reads: 1, writes: 0 }, share: 0.7, avg: (c, w) => w, note: 'inside the 7-10 minute jitter window (about 2 ticks in 3): one state read. The working tick is below' },
  { fn: 'etsyMailReceiptsMirrorCron (working tick)', cron: '*/3 * * * *', share: 0.3, avg: (c, w) => c, before: () => m.db.seed({ 'EtsyMail_Config/receiptsMirrorState': { enabled: true, lastSyncTimestamp: Math.floor(Date.now() / 1000) - 600 } }), call: () => F('etsyMailReceiptsMirrorCron').handler({}), warm: { reads: 5, writes: 5 }, cold: { reads: 5, writes: 5 }, note: 'about 1 tick in 3.5: state, sync state, token, cancel backlog, + 2 diagnostic rows, state, 2 meter writes (no new receipt)', flush: true },
  { fn: 'etsyMailReapers', cron: '*/5 * * * *', call: () => F('etsyMailReapers').handler(sched), warm: { reads: 195, writes: 1 }, cold: { reads: 245, writes: 3 }, note: 'tracking pass 100 drafts + order-link pass 1 + 40 open questions + 40 threads + ~15 empty queries' },
  { fn: 'etsyMailLinkWatchdog', cron: '*/5 * * * *', call: () => F('etsyMailLinkWatchdog').handler(sched), warm: { reads: 17, writes: 2 }, cold: { reads: 18, writes: 2 }, note: 'gather ~16 reads, linkHealth + bell writes' },
  { fn: 'etsyMailTrackingSweepStuck', cron: '*/5 * * * *', call: () => F('etsyMailTrackingSweepStuck').handler({}), warm: { reads: 2, writes: 0 }, cold: { reads: 2, writes: 0 }, note: 'two empty queries' },
  { fn: 'stationSessionsSweepCron', cron: '*/5 * * * *', before: () => require(path.join(ROOT, 'netlify/functions/_stationAutoSignout.js')).resetSweep(), between: () => require(path.join(ROOT, 'netlify/functions/_stationAutoSignout.js')).resetSweep(), call: () => F('stationSessionsSweepCron').handler(), warm: { reads: 12, writes: 0 }, cold: { reads: 13, writes: 0 }, note: 'every open session of the real store (10 here) + the sandbox store' },
  { fn: 'etsyPricingScheduleCron', cron: '0-59/5 * * * *', call: () => F('etsyPricingScheduleCron').handler(), warm: { reads: 2, writes: 1 }, cold: { reads: 2, writes: 1 }, note: 'heartbeat write + paused-runs query + schedule read' },
  { fn: 'charmBatchSweepCron', cron: '4-59/10 * * * *', call: () => F('charmBatchSweepCron').handler(), warm: { reads: 0, writes: 0 }, cold: { reads: 0, writes: 0 }, note: 'only POSTs batch_sweep to geminiImageProxy-background: its ~107 reads and 4 writes an idle sweep are measured by fc12-batch-sweep-cost.cjs', plus: 107 },
  { fn: 'charmNestKeepRepairCron', cron: '*/10 * * * *', before: () => keepState(), between: () => keepState(), call: () => F('charmNestKeepRepairCron').handler(), warm: { reads: 125, writes: 2 }, cold: { reads: 125, writes: 2 }, note: 'one pass of 40 save records + their index entry and alias (the daily steps are skipped); the throttle allows a pass every 5 minutes' },
  { fn: 'applySiteFixes', cron: '0 * * * *', call: () => F('applySiteFixes').handler(nf), warm: { reads: 1, writes: 0 }, cold: { reads: 1, writes: 0 }, note: 'complete: state read only' },
  { fn: 'googleAttributes', cron: '15 * * * *', call: () => F('googleAttributes').handler(nf), warm: { reads: 2, writes: 1 }, cold: { reads: 2, writes: 1 }, note: 'done: state read + sweepRecent state write (Shopify answered empty)' },
  { fn: 'syncCharmSets', cron: '30 * * * *', call: () => F('syncCharmSets').handler(nf), warm: { reads: 1, writes: 0 }, cold: { reads: 1, writes: 0 }, note: 'complete: state read only' },
  { fn: 'googleAdsAutopilotKick', cron: '45 * * * *', call: () => F('googleAdsAutopilotKick').handler(nf), warm: { reads: 1, writes: 0 }, cold: { reads: 1, writes: 0 }, note: 'dormant (kill switch off, the default): control read. When ON it kicks googleAdsAutopilot-background every hour (not measured)' },
  { fn: 'etsyMailSalesReaper', cron: '0 */6 * * *', call: () => F('etsyMailSalesReaper').handler(sched), warm: { reads: 0, writes: 0 }, cold: { reads: 0, writes: 0 }, note: 'REAPER_DISABLED: answers at once' },
  { fn: 'etsyMailListingsCatalog', cron: '0 4 * * *', call: () => F('etsyMailListingsCatalog').handler(sched), warm: { reads: 1, writes: 0 }, cold: { reads: 1, writes: 0 }, note: 'mirror switched off (default): config read. When ON: the daily full sync writes every active listing (~4,700) and reads them once (not idle work)' }
];
/* scheduled but not run here (they need Etsy / Shopify), with the reads per invocation read from the code */
const NOT_DRIVEN = {
  etsyMailShippingSync: { cron: '0 */6 * * *', reads: 3, why: 'Etsy shipping profiles; one cache write + one audit row' },
  syncBestSellers: { cron: '0 5 * * 1', reads: 3, why: 'Shopify sync once a week' }
};

function scheduledFunctions() {
  const out = new Map();
  const toml = fs.readFileSync(path.join(ROOT, 'netlify.toml'), 'utf8').split('\n');
  let cur = null;
  for (const line of toml) {
    const h = /^\[functions\."([A-Za-z0-9_-]+)"\]\s*$/.exec(line);
    if (h) { cur = h[1]; continue; }
    const s = /^\s*schedule\s*=\s*"([^"]+)"/.exec(line);
    if (s && cur) out.set(cur, s[1]);
    if (/^\[/.test(line) && !h) cur = null;
  }
  const dir = path.join(ROOT, 'netlify/functions');
  for (const f of fs.readdirSync(dir)) {
    if (!/\.js$/.test(f)) continue;
    const t = fs.readFileSync(path.join(dir, f), 'utf8');
    const c = /^exports\.config\s*=\s*\{[^}]*?schedule\s*:\s*"([^"]+)"/m.exec(t);
    if (c) out.set(f.replace(/\.js$/, ''), out.get(f.replace(/\.js$/, '')) || c[1]);
  }
  return out;
}

(async () => {
  /* (3) every scheduled function has a number */
  const sch = scheduledFunctions();
  assert(sch.size >= 20, 'the schedule scan found only ' + sch.size + ' scheduled functions (netlify.toml has 20)');
  const known = new Set(TICKS.map(t => t.fn.split(' ')[0]));
  const missing = [...sch.keys()].filter(k => !known.has(k) && !NOT_DRIVEN[k]);
  assert.deepEqual(missing, [], 'scheduled but neither driven nor budgeted in ru-S5-cron-idle-budget.cjs: ' + missing.join(', '));
  for (const t of TICKS) { const base = t.fn.split(' ')[0]; if (sch.has(base)) assert.equal(sch.get(base), t.cron, base + ': the schedule in the repo is "' + sch.get(base) + '", this test assumes "' + t.cron + '"'); }

  const rows = [];
  let readsDay = 0, writesDay = 0;
  for (const t of TICKS) {
    if (t.only && !t.only()) { say('skip ' + t.fn + ' (its pause is lifted: investor-schedule-cost.cjs and fc11-executor-cost.cjs budget it)'); continue; }
    if (t.before) t.before();
    const got = [];
    for (let i = 0; i < 2; i++) {
      const a = m.snapshot();
      let res; try { res = await m.op(t.fn + (i ? ' (warm)' : ' (cold)'), () => t.call()); } catch (e) { res = { error: String(e && e.message) }; }
      if (t.flush && i === 0) await new Promise(r => setTimeout(r, 2500));      // the Etsy call meter flushes its counters 2 s after the call
      const d = m.since(a);
      got.push({ reads: d.reads + (d.aggs || 0), writes: d.writes, bytes: d.bytes, res });
      if (i === 0 && t.between) t.between();
    }
    if (t.after) t.after();
    const [cold, warm] = got;
    const body = String((warm.res && warm.res.body) || '');
    assert(!(warm.res && warm.res.error) && !/"error"/.test(body.slice(0, 40)) && (!warm.res || warm.res.statusCode !== 500), t.fn + ' failed offline: ' + JSON.stringify(warm.res).slice(0, 160));
    meter.assertMax({ reads: warm.reads, writes: warm.writes, bytes: 0 }, { reads: t.warm.reads, writes: t.warm.writes }, t.fn + ' warm idle tick');
    meter.assertMax({ reads: cold.reads, writes: cold.writes, bytes: 0 }, { reads: t.cold.reads, writes: t.cold.writes }, t.fn + ' cold idle tick');
    const n = perDay(t.cron) * (t.share || 1);
    const avg = t.avg ? t.avg(cold.reads, warm.reads) : warm.reads;
    const reads = n * avg + (t.plus ? perDay(t.cron) * t.plus : 0), writes = n * (t.avg ? t.avg(cold.writes, warm.writes) : warm.writes);
    rows.push({ fn: t.fn, cron: t.cron, n, warm, cold, reads, writes, bytes: n * (t.avg ? t.avg(cold.bytes, warm.bytes) : warm.bytes), note: t.note, skipTotal: t.skipTotal });
    if (!t.skipTotal) { readsDay += reads; writesDay += writes; }
  }
  for (const [k, v] of Object.entries(NOT_DRIVEN)) { const n = perDay(v.cron); rows.push({ fn: k + ' *', cron: v.cron, n, warm: { reads: v.reads, writes: 1 }, cold: { reads: v.reads, writes: 1 }, reads: n * v.reads, writes: n, note: v.why + ' (read from the code, not run)' }); readsDay += n * v.reads; writesDay += n; }

  say('\nfunction'.padEnd(48) + 'schedule'.padEnd(18) + 'calls/day'.padStart(10) + 'cold r/w'.padStart(10) + 'warm r/w'.padStart(10) + 'reads/day'.padStart(11) + 'writes/day'.padStart(11) + 'MB/day'.padStart(8));
  for (const r of rows) say(r.fn.padEnd(48) + r.cron.padEnd(18) + String(Math.round(r.n)).padStart(10) + (r.cold.reads + '/' + r.cold.writes).padStart(10) + (r.warm.reads + '/' + r.warm.writes).padStart(10) + String(Math.round(r.reads)).padStart(11) + String(Math.round(r.writes)).padStart(11) + (r.bytes == null ? '' : (r.bytes / 1e6).toFixed(1)).padStart(8) + (r.skipTotal ? '   (not in the total: only while the Gmail watcher is switched on)' : ''));
  say(''.padEnd(48 + 18 + 10 + 10 + 10) + String(Math.round(readsDay)).padStart(11) + String(Math.round(writesDay)).padStart(11) + '   TOTAL, idle');
  const gmail = rows.filter(r => r.skipTotal).reduce((a, r) => a + r.reads, 0);
  say(`idle total: ${Math.round(readsDay).toLocaleString('en')} reads/day = ${(readsDay / 86400).toFixed(2)} reads/s (the overnight baseline in the console is ~40 reads/s, ~3.5 M a day)`);
  say(`plus ${Math.round(gmail).toLocaleString('en')} reads/day and ${Math.round(rows.filter(r => r.skipTotal).reduce((a, r) => a + r.writes, 0)).toLocaleString('en')} writes/day if the Gmail watcher is switched on in the inbox`);

  /* (2) the day */
  const TOTAL_READS_PER_DAY = 112000;
  assert(readsDay <= TOTAL_READS_PER_DAY, `scheduled functions read ${Math.round(readsDay)} documents a day when idle, over the ${TOTAL_READS_PER_DAY} budget`);
  m.uninstall();
  say('\nok');
})().catch(e => { console.error(e && e.stack || e); process.exit(1); });
