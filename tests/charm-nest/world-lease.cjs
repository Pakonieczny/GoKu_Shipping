// CLOUDDRIVE build step 1a (E3): the world build keeps ITS OWN lease, byte for byte (one world build at a time), and the cloud driver's tasks use a new one,
// _charmNestLease.js, whose heartbeat is opt-in. Offline, on the fake cloud of _cloudFakes.cjs with a fake clock.
//   node tests/charm-nest/world-lease.cjs
'use strict';
const fs = require('fs'), path = require('path'), crypto = require('crypto');
const T0 = Date.UTC(2026, 10, 20, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
let failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 600))); if (!cond) failed++; };
const MIN = 60000;

(async () => {
  /* E3 · the world's lease file is not edited (its sha256 as at the plan's start) */
  const WORLD_LEASE_SHA = '8f10df8cec4ad4e12f48be03e206711deabde7a3bc608e2f8a87a37dcc2cb1de';
  const sha = crypto.createHash('sha256').update(fs.readFileSync(path.join(F.fnDir, '_charmNestWorldLease.js'))).digest('hex');
  ok('E3 · _charmNestWorldLease.js is byte-identical to the plan\'s start (sha256)', sha === WORLD_LEASE_SHA, sha);

  /* the world's lease, as it is */
  const W = F.load('_charmNestWorldLease.js');
  let d = await W.dispatch(F.db, 'cron');
  ok('E3 · a dispatch gives a nonce', !!d.nonce, d);
  const b = await W.begin(F.db, d.nonce);
  ok('E3 · and begin() with it succeeds (once)', !!b && (await W.begin(F.db, d.nonce)) === null, b);
  F.clock.advance(3 * MIN); let q = await W.dispatch(F.db, 'cron');
  ok('E3 · a dispatch at +3 min is queued: no nonce, again set', q.queued === true && !q.nonce && F.doc('Charm_World/lease').again === true, q);
  F.clock.advance(7 * MIN); q = await W.dispatch(F.db, 'cron');
  ok('E3 · a dispatch at +10 min is queued too (no second world build at once)', q.queued === true && !q.nonce, q);
  F.clock.set(T0 + 14 * MIN + 1000); q = await W.dispatch(F.db, 'cron');
  ok('E3 · a dispatch at +14 min +1 s gets a new nonce (the first build is stale)', !!q.nonce && q.nonce !== d.nonce, q);
  await W.begin(F.db, q.nonce); await W.end(F.db, q.nonce, { ok: true }, null);
  F.clock.advance(10000); const s = await W.dispatch(F.db, 'change:runPut', { change: true });
  ok('E3 · a change:true dispatch within 30 s of the last one is queued, state "soon"', s.queued === true && s.state === 'soon' && !s.nonce, s);

  /* the new lease (_charmNestLease.js): used only by the cloud driver and its tasks */
  const L = F.load('_charmNestLease.js'), ref = F.db.collection('Charm_Nest_Drive').doc('lease');
  F.clock.set(T0);
  d = await L.dispatch(F.db, ref, 'cron');
  await L.begin(F.db, ref, d.nonce);
  ok('lease · the heartbeat is off unless asked (no beatAt)', F.doc('Charm_Nest_Drive/lease').heartbeat === false && F.doc('Charm_Nest_Drive/lease').beatAt == null, F.doc('Charm_Nest_Drive/lease'));
  F.clock.advance(6 * MIN);
  ok('lease · without a heartbeat a running lease is not stale at 6 min', L.held(F.doc('Charm_Nest_Drive/lease'), Date.now()) === true);
  F.clock.set(T0 + 14 * MIN);
  ok('lease · ... and is stale 14 min after it started', L.held(F.doc('Charm_Nest_Drive/lease'), Date.now()) === false && L.stale(F.doc('Charm_Nest_Drive/lease'), Date.now()) === true);
  F.clock.set(T0 + 20 * MIN);
  d = await L.dispatch(F.db, ref, 'cron', { heartbeat: true });
  await L.begin(F.db, ref, d.nonce);
  F.clock.advance(4 * MIN); let r = await L.renew(F.db, ref, d.nonce);
  ok('lease · with the heartbeat on, renew() beats', r.ok === true && F.doc('Charm_Nest_Drive/lease').beatAt === Date.now(), r);
  F.clock.advance(4 * MIN);
  ok('lease · a beat 4 min old is alive', L.held(F.doc('Charm_Nest_Drive/lease'), Date.now()) === true);
  F.clock.advance(61000);
  ok('lease · a beat 5 min old is stale (C7), and a dispatch takes over with a new nonce', L.held(F.doc('Charm_Nest_Drive/lease'), Date.now()) === false);
  const took = await L.dispatch(F.db, ref, 'cron', { heartbeat: true });
  ok('lease · the takeover names what it took', !!took.nonce && F.doc('Charm_Nest_Drive/lease').tookFrom && F.doc('Charm_Nest_Drive/lease').tookFrom.nonce === d.nonce, F.doc('Charm_Nest_Drive/lease'));
  r = await L.renew(F.db, ref, d.nonce);
  ok('lease · the old holder\'s renew is told it lost the lease, and writes nothing', r.ok === false && r.lost === true && F.doc('Charm_Nest_Drive/lease').nonce === took.nonce, r);
  const before = JSON.stringify(F.store.get('Charm_Nest_Drive/lease'));
  const refused = await L.begin(F.db, ref, took.nonce, { check: async tx => { await tx.get(F.db.collection('Charm_Nest_Open').doc('drive')); return { ok: false, why: 'the drive is off' }; } });
  ok('lease · a begin whose check refuses writes nothing', refused.refused === true && JSON.stringify(F.store.get('Charm_Nest_Drive/lease')) === before, refused);
  const e = await L.end(F.db, ref, 'not-the-nonce', null, null);
  ok('lease · end() by a pass that does not hold it changes nothing', e.lost === true && JSON.stringify(F.store.get('Charm_Nest_Drive/lease')) === before, e);
  ok('no Etsy, no network', F.etsy.calls === 0 && !F.net.tripwire.length && !F.net.other.length);
  console.log(failed ? `FAIL world-lease · ${failed} check(s)` : 'PASS world-lease');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
