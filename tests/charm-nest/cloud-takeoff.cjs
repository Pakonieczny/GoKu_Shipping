// CLOUDDRIVE build step 9: take-offs in the cloud (cloud plan v2: the take-off unit and its journal, cloudTakeOff in one transaction per sheet, takeOffAsk / takeOffAck,
// revalidate read from the world behind the freshness gate). The real cron (Drive.tick), the real driver pass and the real library handler run against the in-memory cloud of
// _cloudFakes.cjs, with a fake clock: no network, no Etsy (its stub counts every call), the Etsy switch never written, no page code loaded.
//   node tests/charm-nest/cloud-takeoff.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const T0 = Date.UTC(2026, 10, 20, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
let failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 900))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), spy = F.spy(lib), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), C = F.load('_charmNestOpClass.js');
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const STEPS = Drive.RUN_STEPS, R = 'run-2026-11-20-tkoff';
const A = '4100000001', B = '4100000002', Cx = '4100000003', H = '4100000004', K = '4100000005', Q = '4100000006', D = '4100000007', G = '4100000008';
const id = (o, t, n) => `${o}_${t}_${n}`;
const run = () => F.doc('Charm_Nest_Runs/' + R) || {};
const lineOf = k => { const r = run(); if (r.lines && r.lines[k]) return r.lines[k]; const parts = F.list('Charm_Nest_Run_Live').filter(p => p.runId === R); for (const p of parts) { const j = JSON.parse(p.json || '{}'); if (j[k]) return j[k]; } return null; };
function setDrive(mode, steps) { if (mode == null) F.store.delete('Charm_Nest_Open/drive'); else F.seed('Charm_Nest_Open/drive', { mode, runMode: 'auto', cloudSteps: steps || STEPS.concat(['takeoff']) }); lib._driveForget(); }
const marked = () => spy.ops.filter(o => o.marked);
const writesOf = list => list.filter(o => C.classOf(o.op) !== 'READ' && !(o.op === 'runOpen'));
/** One pass of the driver (dispatched by the cron when it has a reason, else by hand), with the driver's own steps. */
async function pass(o = {}) {
  const nonces = []; const tick = await Drive.tick({ db: F.db, kick: n => nonces.push(n) });
  let nonce = nonces[0];
  if (!nonce && o.force) { const g = await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true }); nonce = g && g.nonce; }
  if (!nonce) return { tick };
  return { tick, pass: await Drive.pass({ db: F.db, Lib: lib, nonce, sleep: async () => {}, renewMs: 0, budgetMs: 3600e3 }) };
}
const charm = (cid, pid, order) => ({ id: cid, poolId: pid, order, name: cid, hash: 'h-' + cid });
const place = (cid, x, y, a) => ({ id: cid, cxPt: x, cyPt: y, angle: a || 0, scale: 1 });
function seedWorld() {
  F.reset(); F.clock.set(T0); setDrive('standby');
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 } });
  const L = (o, t, ids, extra) => [`${o}_${t}`, Object.assign({ orderId: o, state: 'written', poolIds: ids }, extra || {})];
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-20', errors: [], orders: [A, B, Cx, H, K, Q],
    lines: Object.fromEntries([L(A, 11, [id(A, 11, 1)]), L(B, 21, [id(B, 21, 1), id(B, 21, 2)]), L(Cx, 31, [id(Cx, 31, 1)]), L(H, 41, [id(H, 41, 1)]), L(K, 51, [id(K, 51, 1)]), L(Q, 61, [id(Q, 61, 1)])]) });
  const row = (pid, o, sheetId, metal, extra) => F.seed('Charm_Pool/' + pid, Object.assign({ poolId: pid, orderId: o, lineKey: pid.replace(/_\d+$/, ''), state: 'written', sheetId, material: metal, runId: R, quantity: 1 }, extra || {}));
  row(id(A, 11, 1), A, 'gf-1', 'gold'); row(id(B, 21, 1), B, 'gf-1', 'gold'); row(id(Cx, 31, 1), Cx, 'gf-1', 'gold'); row(id(H, 41, 1), H, 'gf-1', 'gold');
  row(id(B, 21, 2), B, 'ss-1', 'silver'); row(id(K, 51, 1), K, 'rg-1', 'rose'); row(id(Q, 61, 1), Q, 'rg-1', 'rose');
  F.seed('Charm_Nest_Sheets/gf-1', { id: 'gf-1', runId: R, metal: 'gold', fileBase: 'GF_working_gf-1', draft: true, archived: false, layoutRev: 4, contentRev: 2,
    poolIds: [id(A, 11, 1), id(B, 21, 1), id(Cx, 31, 1), id(H, 41, 1)], orders: [A, B, Cx, H],
    charms: [charm('c-a', id(A, 11, 1), A), charm('c-b', id(B, 21, 1), B), charm('c-c', id(Cx, 31, 1), Cx), charm('c-h', id(H, 41, 1), H)],
    placements: [place('c-a', 10, 20), place('c-b', 50, 20, 90), place('c-c', 90, 20), place('c-h', 130, 20, 45)], outputs: { ai: { path: 'charmnest/x/GF.ai', url: 'u' } } });
  F.seed('Charm_Nest_Sheets/ss-1', { id: 'ss-1', runId: R, metal: 'silver', fileBase: 'SS_working_ss-1', draft: true, archived: false, layoutRev: 2, laserDoneAt: T0 - 3600e3,
    poolIds: [id(B, 21, 2)], orders: [B], charms: [charm('c-b2', id(B, 21, 2), B)], placements: [place('c-b2', 10, 10)] });
  const guard = { profile: [], lines: [[0, 0], [1, 1]], shapes: [{ id: 'c-k' }, { id: 'c-q' }], placements: [place('c-k', 10, 10), place('c-q', 40, 10)], stages: [{ n: 1, at: T0 - 1000, ids: ['c-k', 'c-q'], lines: [0, 2] }] };
  F.seed('Charm_Nest_Sheets/rg-1', { id: 'rg-1', runId: R, metal: 'rose', fileBase: 'RG_working_rg-1', draft: true, archived: false, layoutRev: 7, contentRev: 1,
    poolIds: [id(K, 51, 1), id(Q, 61, 1)], orders: [K, Q], charms: [charm('c-k', id(K, 51, 1), K), charm('c-q', id(Q, 61, 1), Q)], placements: [place('c-k', 10, 10), place('c-q', 40, 10)], roseProtectedJson: JSON.stringify(guard) });
  // Etsy cancelled B and K (the mail system's mirror wrote their records); C was cancelled, then restored (its record is gone: its job, made before, finds it restored)
  for (const [o, at] of [[B, T0 - 60000], [K, T0 - 50000]]) F.seed('Charm_Nest_Cancelled/' + o, { orderId: o, at, by: 'Etsy', source: 'etsy', createdAt: F.Timestamp.fromMillis(at) });
  F.seed('Charm_Nest_Rev/cancel', { n: 2 });
  F.seed('Charm_Nest_TakeOff/' + Cx, { rid: Cx, due: true, updatedAt: T0 - 90000, jobs: { ['cancel-' + (T0 - 90000)]: { kind: 'cancel', at: T0 - 90000, by: 'Ann', status: 'due', tries: 0 } } });
}

(async () => {
  /* ═══ 1 · cancelled orders come off filling sheets; the others' poses are byte-identical; cut sheets keep theirs with a notice; restored orders do nothing ═══ */
  seedWorld();
  const before = { gf: F.doc('Charm_Nest_Sheets/gf-1'), ss: F.doc('Charm_Nest_Sheets/ss-1'), rg: F.doc('Charm_Nest_Sheets/rg-1') };
  const p1 = await pass();
  ok('1 · the cron dispatched a pass for the new cancel records (the counter moved)', !!p1.pass && p1.tick.why.includes('a cancel was recorded'), p1.tick);
  ok('1 · the pass ran the take-off lane', p1.pass && p1.pass.takeoff && p1.pass.takeoff.detected === 2, p1.pass && p1.pass.takeoff);
  const gf = F.doc('Charm_Nest_Sheets/gf-1');
  ok('1 · B came off GF-1: its pool id, order, charm and placement are gone; C (restored) and the others stay', JSON.stringify(gf.poolIds) === JSON.stringify([id(A, 11, 1), id(Cx, 31, 1), id(H, 41, 1)]) && !gf.orders.includes(B) && gf.charms.map(c => c.id).join() === 'c-a,c-c,c-h', { poolIds: gf.poolIds, orders: gf.orders });
  ok('1 · every other piece keeps its pose byte for byte', JSON.stringify(gf.placements) === JSON.stringify(before.gf.placements.filter(p => p.id !== 'c-b')), gf.placements);
  ok('1 · layoutRev and contentRev went up by one, files are owed, the record is dirty', gf.layoutRev === 5 && gf.contentRev === 3 && gf.filesOwed > 0 && gf.dirty === true, { layoutRev: gf.layoutRev, contentRev: gf.contentRev, filesOwed: gf.filesOwed });
  const b1 = F.doc('Charm_Pool/' + id(B, 21, 1));
  ok('1 · B\'s piece record says it was taken off (abandoned, no sheet, removedBy Etsy, cancelled)', b1.state === 'abandoned' && b1.sheetId === null && b1.removedBy === 'Etsy' && b1.removedReason === 'cancelled' && b1.removedAt > 0, b1);
  ok('1 · the cut sheet SS-1 and B\'s piece on it are unchanged', JSON.stringify(F.doc('Charm_Nest_Sheets/ss-1')) === JSON.stringify(before.ss) && F.doc('Charm_Pool/' + id(B, 21, 2)).state === 'written');
  const bl = lineOf(`${B}_21`);
  ok('1 · B\'s run line is gone (cancelled on Etsy) and names only the piece kept on the cut sheet', bl && bl.state === 'gone' && bl.reason === 'cancelled on Etsy' && JSON.stringify(bl.poolIds) === JSON.stringify([id(B, 21, 2)]) && bl._lp && bl._lp.dev === 'cloud', bl);
  const share = F.doc('Charm_Nest_Shared/autocancel-production') || {}, nKey = `n:${B}:${T0 - 60000}`, notice = share.f && share.f[nKey] ? JSON.parse(share.f[nKey].j) : null;
  ok('1 · a set-aside notice for B is in the shared store, in the shape every page reads (n:<order>:<at>), naming the cut sheet', !!notice && /already cut on SS/.test(notice.text) && notice.sheets.length === 1, notice);
  const cancelRec = F.doc('Charm_Nest_Cancelled/' + B) || {};
  ok('1 · B\'s cancel record says what became of it on each sheet (fates)', Array.isArray(cancelRec.fates) && cancelRec.fates.some(f => f.fate === 'removed') && cancelRec.fates.some(f => f.fate === 'cut'), cancelRec.fates);
  const jb = F.doc('Charm_Nest_TakeOff/' + B) || {}, jobB = jb.jobs && jb.jobs['cancel-' + (T0 - 60000)];
  ok('1 · B\'s journal job is done and the journal is no longer due', jobB && jobB.status === 'done' && jb.due === false, jb);
  const jc = F.doc('Charm_Nest_TakeOff/' + Cx) || {}, jobC = jc.jobs && jc.jobs['cancel-' + (T0 - 90000)];
  ok('1 · C (restored before its job ran) is "restored" and nothing of it moved', jobC && jobC.status === 'restored' && F.doc('Charm_Pool/' + id(Cx, 31, 1)).state === 'written' && gf.poolIds.includes(id(Cx, 31, 1)) && lineOf(`${Cx}_31`).state === 'written', jobC);
  // P10: the protected Rose Gold sheet gave its piece up in the same commit, and its green lines no longer name it
  const rg = F.doc('Charm_Nest_Sheets/rg-1'), g2 = JSON.parse(rg.roseProtectedJson || 'null');
  ok('1 · RG-1: K came off and its green line kept the other piece (the guard rewritten in the same commit; the line stays as saved)', JSON.stringify(rg.poolIds) === JSON.stringify([id(Q, 61, 1)]) && g2 && g2.placements.map(p => p.id).join() === 'c-q' && JSON.stringify(g2.lines) === JSON.stringify(JSON.parse(before.rg.roseProtectedJson).lines) && rg.layoutRev === 8, { poolIds: rg.poolIds, guard: g2 });
  const put = await P({ op: 'putSheet', sheet: { id: 'rg-1', placements: [place('c-q', 40, 10)], charms: [charm('c-q', id(Q, 61, 1), Q)], poolIds: [id(Q, 61, 1)] }, layoutBase: rg.layoutRev, dev: 'd1aaaa' });
  ok('1 · a protected sheet after cloudTakeOff accepts a putSheet with placements (P10)', put.http === 200 && put.ok === true, put);
  // a page that still shows B's piece on GF-1 cannot put it back (putSheet refuses a piece that was taken off)
  const back = await P({ op: 'putSheet', sheet: { id: 'gf-1', placements: before.gf.placements, charms: before.gf.charms, poolIds: before.gf.poolIds }, dev: 'd1aaaa', layoutBase: F.doc('Charm_Nest_Sheets/gf-1').layoutRev });
  ok('1 · putSheet refuses a save that would put a taken-off piece back', back.http === 409 && Array.isArray(back.taken) && back.taken.includes(id(B, 21, 1)), back);

  /* ═══ 2 · a second pass makes zero writes ═══ */
  const mark = marked().length;
  const p2 = await pass({ force: true });
  ok('2 · a second pass makes zero writes of its own (only reads)', p2.pass && p2.pass.ok && writesOf(marked().slice(mark)).length === 0, writesOf(marked().slice(mark)).map(o => o.op));

  /* ═══ 3 · a Hold press ends through the unit (takeOffAsk) ═══ */
  F.net.kicks.length = 0;
  const ask = await P({ op: 'takeOffAsk', orderId: H, kind: 'hold', note: 'the customer asked', by: 'Ann', dev: 'd1aaaa' });
  ok('3 · takeOffAsk is taken while the cloud does take-offs', ask.http === 200 && ask.ok && !!ask.askId, ask);
  // (the press kicked the driver at once: driveSoon dispatched its lease and sent the background function its nonce)
  const kick = F.net.kicks.filter(k => k.name === 'charmNestDrive-background').pop(), kn = kick ? JSON.parse(kick.body).nonce : null;
  F.net.kicks.length = 0;
  ok('3 · the press kicked the cloud driver (driveSoon)', !!kn, F.net.kicks);
  const p3 = kn ? { pass: await Drive.pass({ db: F.db, Lib: lib, nonce: kn, sleep: async () => {}, renewMs: 0, budgetMs: 3600e3 }) } : await pass({ force: true });
  ok('3 · the pass did the press', !!p3.pass && p3.pass.takeoff && p3.pass.takeoff.jobs >= 1, p3.pass && p3.pass.takeoff);
  const h1 = F.doc('Charm_Pool/' + id(H, 41, 1)), hl = lineOf(`${H}_41`), gf3 = F.doc('Charm_Nest_Sheets/gf-1');
  ok('3 · H\'s piece is held by Ann (heldAt, heldReason), off GF-1', h1.state === 'abandoned' && h1.heldBy === 'Ann' && h1.heldReason === 'the customer asked' && !gf3.poolIds.includes(id(H, 41, 1)), h1);
  ok('3 · H\'s line is held with the sheet window\'s words', hl && hl.state === 'held' && /^Taken off .+ by Ann: the customer asked$/.test(hl.hold || ''), hl);
  const jh = F.doc('Charm_Nest_TakeOff/' + H) || {};
  ok('3 · the press is marked taken and its job done', Object.values(jh.asks || {}).every(a => a.state === 'taken') && Object.values(jh.jobs || {}).every(j => j.status === 'done') && jh.due === false, jh);

  /* ═══ 4 · green lines that cannot give up only part of a group refuse in words, and nothing is written ═══ */
  {
    F.seed('Charm_Pool/' + id(Q, 61, 2), { poolId: id(Q, 61, 2), orderId: Q, lineKey: `${Q}_61`, state: 'written', sheetId: 'rg-1', material: 'rose' });
    const s = F.doc('Charm_Nest_Sheets/rg-1');
    F.seed('Charm_Nest_Sheets/rg-1', Object.assign(s, { poolIds: s.poolIds.concat([id(Q, 61, 2)]), charms: s.charms.concat([Object.assign(charm('c-q2', id(Q, 61, 2), Q), { groupKey: `${Q}:61` })]), placements: s.placements.concat([place('c-q2', 70, 10)]) }));
    const was = JSON.stringify(F.doc('Charm_Nest_Sheets/rg-1'));
    // (the rows say no group, so only c-q is named; the green lines read c-q and c-q2 as one line of the order: part of it would leave → refused, the order held with words)
    const r = await lib.call({ op: 'cloudTakeOff', orderId: Q, sheetId: 'rg-1', ids: [id(Q, 61, 1)], kind: 'sheetOff', at: Date.now(), who: 'Ann' }, { unit: 'test|group' });
    ok('4 · part of a group: refused in words (roseGroups), the sheet as it was', r.status === 409 && r.refused === 'roseGroups' && /group/.test(r.error || '') && JSON.stringify(F.doc('Charm_Nest_Sheets/rg-1')) === was, r);
  }

  /* ═══ 5 · with a live page carrying, the cloud makes no cancel write ═══ */
  {
    seedWorld();
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'd1aaaa', by: 'Ann', at: Date.now(), since: Date.now() } });
    const m = marked().length, p = await pass({ force: true });
    ok('5 · a computer carries the run: the pass yields and no take-off is written', p.pass && p.pass.yield && writesOf(marked().slice(m)).length === 0 && F.doc('Charm_Nest_Sheets/gf-1').poolIds.includes(id(B, 21, 1)), { pass: p.pass, ops: writesOf(marked().slice(m)).map(o => o.op) });
  }

  /* ═══ 6 · dormant: with the lane off (not in cloudSteps) or the drive off, nothing is taken off by the cloud, and takeOffAsk is refused in words ═══ */
  {
    seedWorld(); setDrive('standby', STEPS.slice());
    const m = marked().length; await pass({ force: true });
    ok('6 · "takeoff" not in cloudSteps: the cloud takes nothing off', writesOf(marked().slice(m)).filter(o => o.op === 'cloudTakeOff').length === 0 && F.doc('Charm_Nest_Sheets/gf-1').poolIds.includes(id(B, 21, 1)));
    const a6 = await P({ op: 'takeOffAsk', orderId: H, kind: 'hold', by: 'Ann', dev: 'd1aaaa' });
    ok('6 · takeOffAsk is refused in words while the cloud does no take-offs (the page does it, as today)', a6.http === 409 && a6.notCloud === true && !F.doc('Charm_Nest_TakeOff/' + H), a6);
    setDrive(null);
    const a7 = await P({ op: 'takeOffAsk', orderId: H, kind: 'hold', by: 'Ann', dev: 'd1aaaa' });
    ok('6 · drive absent: takeOffAsk refused, nothing written', a7.http === 409 && a7.notCloud === true && !F.doc('Charm_Nest_TakeOff/' + H), a7);
    const c6 = await P({ op: 'cloudTakeOff', orderId: B, sheetId: 'gf-1', ids: [id(B, 21, 1)], kind: 'cancel', by: 'Ann', dev: 'd1aaaa' });
    ok('6 · a page can never send cloudTakeOff (CLOUD_ONLY, in every mode)', c6.http === 403 && F.doc('Charm_Nest_Sheets/gf-1').poolIds.includes(id(B, 21, 1)), c6);
  }

  /* ═══ 7 · a page's own take-off is unchanged (P10): poolUpdate takes the piece off, then its save with layoutBase still succeeds ═══ */
  {
    seedWorld();
    const lr = F.doc('Charm_Nest_Sheets/gf-1').layoutRev;
    const u = await P({ op: 'poolUpdate', poolIds: [id(H, 41, 1)], patch: { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Ann', heldReason: 'on hold', heldAt: Date.now() }, by: 'Ann', dev: 'd1aaaa' });
    const s = F.doc('Charm_Nest_Sheets/gf-1');
    ok('7 · the page\'s poolTakeOff leaves charms, placements and layoutRev to the page (as today)', u.http === 200 && s.layoutRev === lr && s.charms.length === 4 && !s.poolIds.includes(id(H, 41, 1)), { u, layoutRev: s.layoutRev });
    const keep = s.charms.filter(c => c.id !== 'c-h');
    const w = await P({ op: 'putSheet', sheet: { id: 'gf-1', charms: keep, placements: s.placements.filter(p => p.id !== 'c-h'), poolIds: keep.map(c => c.poolId) }, layoutBase: lr, dev: 'd1aaaa' });
    ok('7 · its persistSheet with layoutBase still succeeds', w.http === 200 && w.layoutRev === lr + 1, w);
  }

  /* ═══ 8 · the freshness gate (E7, P11): revalidate waits for summary.mirrorBefore, not summary.at ═══ */
  {
    F.reset(); F.clock.set(T0);
    const at = T0 - 1000, set = (m, s, y) => { F.seed('EtsyMail_Config/receiptsMirrorState', m); F.seed('Charm_World/summary', s); F.seed('EtsyMail_Config/syncState', y || {}); F.seed('EtsyApi_Config/stop', { stopped: false }); };
    set({ lastSyncOutcome: 'rate_limited', lastSyncCompletedAt: T0 }, { at: T0, mirrorBefore: T0, toReadIds: [] });
    let g = await Drive.freshnessGate(F.db, { stepStartedAt: at, receipts: [A] });
    ok('8 · an outcome "rate_limited" with a fresh lastSyncCompletedAt holds', !g.ok && /rate_limited/.test(g.why.join()), g);
    set({ lastSyncOutcome: 'ok', lastSyncCutByClock: true, lastSyncCompletedAt: T0 }, { at: T0, mirrorBefore: T0, toReadIds: [] });
    g = await Drive.freshnessGate(F.db, { stepStartedAt: at, receipts: [A] });
    ok('8 · a sync cut by its clock holds', !g.ok, g);
    set({ lastSyncOutcome: 'ok', lastSyncCompletedAt: T0 }, { at: T0 + 5000, mirrorBefore: T0 - 60000, toReadIds: [] });
    g = await Drive.freshnessGate(F.db, { stepStartedAt: at, receipts: [A] });
    ok('8 · a build newer than the sync (summary.at) that read an older copy (mirrorBefore) holds', !g.ok && /built from that copy/.test(g.why.join()), g);
    set({ lastSyncOutcome: 'ok', lastSyncCompletedAt: T0 }, { at: T0, mirrorBefore: T0, toReadIds: [A] });
    g = await Drive.freshnessGate(F.db, { stepStartedAt: at, receipts: [A] });
    ok('8 · a receipt of the run still to be read holds', !g.ok && /still to be read/.test(g.why.join()), g);
    set({ lastSyncOutcome: 'ok', lastSyncCompletedAt: T0 }, { at: T0, mirrorBefore: T0, toReadIds: [] });
    F.hooks.failRead = /EtsyApi_Config\/stop/;
    g = await Drive.freshnessGate(F.db, { stepStartedAt: at, receipts: [A] });
    F.hooks.failRead = null;
    ok('8 · a stop-document read that throws holds', !g.ok && g.hold === true, g);
    g = await Drive.freshnessGate(F.db, { stepStartedAt: at, receipts: [A] });
    ok('8 · every condition good: it goes on', g.ok === true, g);
  }

  /* ═══ 9 · revalidate read from the world: an Etsy-closed line is only marked; a changed line is only flagged (it moves after takeOffAsk); a cancel becomes a take-off ═══ */
  {
    seedWorld();
    F.store.delete('Charm_Nest_Cancelled/' + B); F.store.delete('Charm_Nest_Cancelled/' + K); F.store.delete('Charm_Nest_TakeOff/' + Cx); F.seed('Charm_Nest_Rev/cancel', { n: 2 });
    const r0 = F.doc('Charm_Nest_Runs/' + R);
    r0.status = 'running'; r0.step = 'revalidate';
    r0.lines[`${D}_71`] = { orderId: D, state: 'written', poolIds: [id(D, 71, 1)] };
    r0.lines[`${G}_81`] = { orderId: G, state: 'written', poolIds: [id(G, 81, 1)] };
    F.seed('Charm_Nest_Runs/' + R, r0);
    F.seed('Charm_Pool/' + id(D, 71, 1), { poolId: id(D, 71, 1), orderId: D, state: 'written', sheetId: null });
    F.seed('Charm_Pool/' + id(G, 81, 1), { poolId: id(G, 81, 1), orderId: G, state: 'written', sheetId: null });
    F.seed('Charm_Nest_Drive/state', { takeoff: { cancelCursor: { s: Math.floor(T0 / 1000), n: 0, id: '9' }, cancelGen: 2, lookAt: T0 } });
    // the world: every order open but D (Etsy closed it, not cancelled); G changed after the pull (its change mark)
    F.seed('Charm_World/state', { open: Object.fromEntries([A, B, Cx, H, K, Q, G].map(o => [o, 1])) });
    F.seed('Charm_World/changed', { marks: { [`${G}_81`]: { at: T0 - 500, ts: 2, shape: true, words: false, why: `Etsy updated order ${G} after the pull · SKU changed` } } });
    F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: T0 + 60000 });
    F.seed('Charm_World/summary', { at: T0 + 60000, mirrorBefore: T0 + 60000, toReadIds: [], live: 8 });
    F.seed('EtsyMail_Config/syncState', {}); F.seed('EtsyApi_Config/stop', { stopped: false });
    setDrive('standby', STEPS.concat(['takeoff']));
    const m = marked().length; await pass({ force: true }); await pass({ force: true });
    const dl = lineOf(`${D}_71`), gl = lineOf(`${G}_81`);
    ok('9 · D (closed on Etsy, not cancelled) is only marked etsyClosed: it stays where it is', dl && dl.etsyClosed > 0 && dl.state === 'written' && F.doc('Charm_Pool/' + id(D, 71, 1)).state === 'written', dl);
    ok('9 · G (changed after the pull) has changePending and repoolChanged; nothing of it moved', gl && gl.changePending === true && gl.repoolChanged === true && gl.changedAt === T0 - 500 && F.doc('Charm_Pool/' + id(G, 81, 1)).state === 'written', gl);
    ok('9 · revalidate made no take-off of its own', writesOf(marked().slice(m)).filter(o => o.op === 'cloudTakeOff').length === 0);
  }

  /* ═══ the owner's rules ═══ */
  ok('Etsy: the stub was never called, no Etsy address or proxy was asked, the Etsy switch was never written', F.etsy.calls === 0 && !F.net.tripwire.length && !F.stopWrites.length, { etsy: F.etsy.calls, trip: F.net.tripwire });
  ok('no address outside the named functions was asked', !F.net.other.length, F.net.other);
  ok('the driver never sent a denied op', !spy.ops.some(o => o.marked && C.DENIED.has(o.op)));
  ok('no browser global exists', !F.browserGlobals().length, F.browserGlobals());
  console.log(failed ? `FAIL cloud-takeoff · ${failed} check(s)` : 'PASS cloud-takeoff');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
