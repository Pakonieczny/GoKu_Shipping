// CLOUDDRIVE foundations (cloud plan v2, build steps 1b, 1c and 1d), offline: the cloud's own call (Lib.call), its list of ops, the mid ledger, the fence, the reserved name,
// the line merges, contentRev, the mode branches of runCarrier / runOpen / putSheet, SETSHUT on the server and the Storage door. Everything runs against the in-memory cloud of
// _cloudFakes.cjs: no network, no Etsy (its stub counts every call), the Etsy switch is never written.
//   node tests/charm-nest/cloud-foundations.cjs
//   CN_BASE_REPO=<a checkout of the commit before this work> node tests/charm-nest/cloud-foundations.cjs   (also compares the answers with that code's, byte for byte)
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const path = require('path'), crypto = require('crypto');
const T0 = Date.UTC(2026, 10, 20, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
let failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 900))); if (!cond) failed++; };
const LEASE = 'Charm_Nest_Drive/lease';
const handlerOf = lib => async (body, headers = { 'x-edit-passcode': 'test-pass' }) => { const r = await lib.handler({ httpMethod: 'POST', headers, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({ status: r.statusCode }, o); };
// deterministic randomness, so two runs of one scenario can be compared byte for byte
const realRB = crypto.randomBytes, realRandom = Math.random; let seedN = 1;
const lcg = () => (seedN = (Math.imul(seedN, 1103515245) + 12345) & 0x7fffffff);
const determinism = on => { if (on) { seedN = 1; crypto.randomBytes = n => Buffer.from(Array.from({ length: n }, () => lcg() & 255)); Math.random = () => lcg() / 0x80000000; } else { crypto.randomBytes = realRB; Math.random = realRandom; } };
const R = '3940000001', L1 = R + '_39400000011', P1 = L1 + '_1', P2 = L1 + '_2';
const lineOf = (st, extra) => Object.assign({ orderId: R, transactionId: '39400000011', state: st, poolIds: [], sku: 'BR-TST-01', quantity: 1 }, extra || {});
function seedRun(runId, extra) {
  F.seed('Charm_Nest_Runs/' + runId, Object.assign({ runId, status: 'running', step: 'pull', mode: 'auto', day: '2026-11-20', sheets: {}, holds: {}, errors: [], orders: [], updatedAt: F.Timestamp.fromMillis(Date.now()) }, extra || {}));
  F.seed('Charm_Nest_Open/run', { runId, at: Date.now() });
}
const cloudCarrier = runId => F.seed('Charm_Nest_Run_Carrier/' + runId, { runId, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now(), since: Date.now() } });
const setDrive = (lib, mode, extra) => { if (mode == null) F.store.delete('Charm_Nest_Open/drive'); else F.seed('Charm_Nest_Open/drive', Object.assign({ mode, runMode: 'auto', cloudSteps: [] }, extra || {})); if (lib && lib._driveForget) lib._driveForget(); };

/* ── the same presses from a page, as today: answers and the cloud they leave ── */
async function scenario(lib) {
  const P = handlerOf(lib), out = [];
  const a = await P({ op: 'runOpen', mode: 'manual', day: '2026-11-20' }); out.push(a); const runId = a.runId;
  out.push(await P({ op: 'runOpen', mode: 'manual', day: '2026-11-20' }));
  out.push(await P({ op: 'runCarrier', runId, dev: 'd1aaaa', by: 'Ann' }));
  out.push(await P({ op: 'runCarrier', runId, dev: 'd2bbbb', by: 'Bob' }));
  out.push(await P({ op: 'runCarrier', runId, dev: 'd2bbbb', by: 'Bob', check: true }));
  out.push(await P({ op: 'runLinesAdd', runId, dev: 'd1aaaa', by: 'Ann', lines: { [L1]: lineOf('pulled') } }));
  out.push(await P({ op: 'runLinesPatch', runId, dev: 'd1aaaa', by: 'Ann', lines: { [L1]: { state: 'pooled', poolIds: [P1], material: 'gf', poolProblems: ['x'] } } }));
  out.push(await P({ op: 'runPut', dev: 'd1aaaa', run: { runId, status: 'running', step: 'pool', mode: 'manual', orders: [R], lines: { [L1]: lineOf('pooled', { poolIds: [P1] }) } } }));
  out.push(await P({ op: 'runPut', dev: 'd2bbbb', run: { runId, status: 'running', step: 'nest', lines: {} } }));
  const sheet = { id: 'sheetT1', runId, metal: 'gf', fileBase: 'GF_T_Sheet-1', placements: [{ id: 'c1', cxPt: 10, cyPt: 10, angle: 0 }], charms: [{ id: 'c1', poolId: P1 }], poolIds: [P1] };
  out.push(await P({ op: 'putSheet', dev: 'd1aaaa', sheet }));
  out.push(await P({ op: 'putSheet', dev: 'd1aaaa', layoutBase: 1, sheet: Object.assign({}, sheet, { placements: [{ id: 'c1', cxPt: 12, cyPt: 10, angle: 0 }] }) }));
  out.push(await P({ op: 'putSheet', dev: 'd1aaaa', layoutBase: 0, sheet }));
  out.push(await P({ op: 'sheetLayoutPatch', id: 'sheetT1', tab: 't1', by: 'Ann', poses: { c1: { cxPt: 20, cyPt: 20, angle: 0 } } }));
  out.push(await P({ op: 'runCmdPut', runId, dev: 'd2bbbb', by: 'Bob', cmd: { type: 'reorder', metal: '' } }));
  out.push(await P({ op: 'putSheet', dev: 'd1aaaa', sheet: { id: 'sheetT1', label: { url: 'x' } } }));
  out.push(await P({ op: 'runGet', runId }));
  out.push(await P({ op: 'getSheet', id: 'sheetT1' }));
  return out;
}
const storeText = (skip = []) => JSON.stringify([...F.store.entries()].filter(([k]) => !skip.includes(k)).sort((x, y) => (x[0] < y[0] ? -1 : 1)));

(async () => {
  /* ═══ 1d · drive absent, or "off": every new branch does nothing; runCarrier, runOpen, putSheet answer as today ═══ */
  const runs = {};
  for (const variant of ['absent', 'off']) {
    F.reset(); F.clock.set(T0); determinism(true);
    const lib = F.load('charmNestLibrary.js', { fresh: true });
    if (variant === 'off') setDrive(lib, 'off');
    const answers = await scenario(lib); determinism(false);
    runs[variant] = { answers: JSON.stringify(answers), store: storeText(['Charm_Nest_Open/drive']), kicks: F.net.ours.filter(k => k.name === 'charmNestDrive-background').length };
  }
  ok('1d · drive absent and drive "off" give the same answers and leave the same cloud', runs.absent.answers === runs.off.answers && runs.absent.store === runs.off.store, { a: runs.absent.answers.slice(0, 300), b: runs.off.answers.slice(0, 300) });
  ok('1d · with the drive off nothing kicks the cloud driver and no cloud carrier is written', runs.absent.kicks === 0 && runs.off.kicks === 0 && !runs.off.store.includes('"dev":"cloud"'), runs);
  ok('1c · with the drive off a sheet record gets no contentRev (nobody writes it yet)', !runs.off.store.includes('contentRev'));
  if (process.env.CN_BASE_REPO) {
    F.reset(); F.clock.set(T0); determinism(true);
    const base = F.load('charmNestLibrary.js', { fresh: true, dir: path.join(process.env.CN_BASE_REPO, 'netlify/functions') });
    const answers = await scenario(base); determinism(false);
    const b = { answers: JSON.stringify(answers), store: storeText() };
    ok('1d · BYTE-IDENTICAL to the code before this work (CN_BASE_REPO): every answer of the scenario', b.answers === runs.absent.answers, (() => { const x = JSON.parse(b.answers), y = JSON.parse(runs.absent.answers); const i = x.findIndex((v, k) => JSON.stringify(v) !== JSON.stringify(y[k])); return { i, before: x[i], now: y[i] }; })());
    ok('1d · BYTE-IDENTICAL to the code before this work: the cloud the scenario leaves', b.store === runs.absent.store, (() => { const x = new Map(JSON.parse(b.store)), y = new Map(JSON.parse(runs.absent.store)); const k = [...new Set([...x.keys(), ...y.keys()])].find(k => JSON.stringify(x.get(k)) !== JSON.stringify(y.get(k))); return { k, before: x.get(k), now: y.get(k) }; })());
  } else console.log('SKIP the comparison with the code before this work (set CN_BASE_REPO to a checkout of it)');

  /* ═══ 1b · the cloud's own call ═══ */
  F.reset(); F.clock.set(T0);
  const lib = F.load('charmNestLibrary.js', { fresh: true }), spy = F.spy(lib), P = handlerOf(lib);
  const runId = 'run-2026-11-20-cloud01';
  seedRun(runId);
  setDrive(lib, 'standby', { cloudSteps: ['pull'] });
  // writes once per mid, raises the feed, never touches the world's lease (E1)
  const m0 = F.count();
  let r = await lib.call({ op: 'runLinesAdd', runId, lines: { [L1]: lineOf('pulled') } }, { unit: 'intake|' + L1 });
  const live1 = JSON.stringify(F.list('Charm_Nest_Run_Live')), w1 = F.since(m0);
  ok('1b · the cloud\'s call writes (runLinesAdd, dev "cloud")', r.status === 200 && (r.added || []).includes(L1), r);
  ok('1b · it raises the changes feed, so every page hears it', w1.writePaths.some(p => /^Charm_Nest_Rev\//.test(p)), w1.writePaths);
  const mid = F.list('Charm_Nest_Mutations')[0];
  ok('1b · its mid is "cloud:" and a hash (it fits the ledger\'s form whatever the unit\'s key)', mid && /^cloud:[0-9a-f]{24}$/.test(mid.mid) && mid.state === 'done', mid);
  r = await lib.call({ op: 'runLinesAdd', runId, lines: { [L1]: lineOf('pulled') } }, { unit: 'intake|' + L1 });
  ok('1b · the same unit and body again: answered replayed, nothing written', r.replayed === true && JSON.stringify(F.list('Charm_Nest_Run_Live')) === live1, r);
  ok('1b · the cloud\'s writes never start a world build (Charm_World/lease untouched, no world kick)', !F.doc('Charm_World/lease') && !F.net.ours.some(k => k.name === 'charmNestWorld-background'), F.doc('Charm_World/lease'));
  ok('1b · ... and never kick the cloud driver again', !F.net.ours.some(k => k.name === 'charmNestDrive-background'));
  await P({ op: 'runLinesPatch', runId, dev: 'd9page', by: 'Ann', lines: { [L1]: { hold: null } } });
  ok('1b · (control: the same kind of write from a page does start a world build, as today)', F.doc('Charm_World/lease') && /^change:runLinesPatch/.test(F.doc('Charm_World/lease').by), F.doc('Charm_World/lease'));
  // P6: a refusal is never kept; a retry from a re-read base gets a fresh answer; a replay after success is replayed; any unit key is protected
  F.seed('Charm_Nest_Sheets/sheetC1', { id: 'sheetC1', runId, metal: 'gf', layoutRev: 3, placements: [], charms: [], poolIds: [], fileBase: 'GF_C_Sheet-1' });
  const unit = 'place|sheetC1@~|odd key|' + 'x'.repeat(200);
  const body = base => ({ op: 'putSheet', layoutBase: base, sheet: { id: 'sheetC1', runId, placements: [], charms: [] } });
  r = await lib.call(body(2), { unit });
  const keptRefusal = F.list('Charm_Nest_Mutations').some(x => x.status >= 400);
  ok('1b · P6: a 409 (an old base) is refused and the refusal is not kept in the ledger', r.status === 409 && r.conflict === true && !keptRefusal, r);
  r = await lib.call(body(2), { unit });
  ok('1b · P6: the same body again is run again (a fresh answer, not a replayed refusal)', r.status === 409 && !r.replayed, r);
  r = await lib.call(body(3), { unit });
  ok('1b · P6: the body re-read with the right base is a new mid and is written', r.status === 200 && r.layoutRev === 4, r);
  r = await lib.call(body(3), { unit });
  ok('1b · P6: an identical replay after success answers replayed:true (a unit key with @, ~ and | is protected)', r.replayed === true && F.doc('Charm_Nest_Sheets/sheetC1').layoutRev === 4, r);
  let thrown = null; try { await lib.call({ op: 'runPut', run: { runId } }, {}); } catch (e) { thrown = e.message; }
  ok('1b · a write that names no unit is never sent (it would have no mid)', /no unit/.test(thrown || ''), thrown);

  // E5: the cloud's list of ops; every denied op refused, and missing from cloudOps()
  const OpClass = require(path.join(F.fnDir, '_charmNestOpClass.js')), ops = lib.cloudOps();
  const denied = [];
  for (const op of OpClass.DENIED) { const a = await lib.call({ op, by: 'x' }, { unit: 'deny|' + op }); denied.push([op, a.status, a.error, op in ops]); }
  ok('1b · E5: each denied op (Etsy, its switch, the world build, the agent) is refused through Lib.call, before it runs', denied.length === 13 && denied.every(d => d[1] === 403 && d[2] === 'not a cloud op'), denied.filter(d => d[1] !== 403));
  ok('1b · E5: and none of them is in cloudOps()', denied.every(d => !d[3]) && !('worldBuild' in ops) && !('startAgent' in ops) && Object.isFrozen(ops));
  ok('1b · E5: a denied op never reached the handler as the cloud\'s own call', !spy.ops.some(o => o.marked && OpClass.DENIED.has(o.op)), spy.ops.filter(o => o.marked && OpClass.DENIED.has(o.op)));
  r = await ops.listingSkus({ listingIds: ['1718099'], nameIds: ['1718099'], refreshIds: ['1718099'] });
  const lsBody = spy.listingSkus[spy.listingSkus.length - 1] || {};
  ok('1b · E4: listingSkus from the cloud is cache only: cacheOnly forced, nameIds and refreshIds dropped', lsBody.cacheOnly === true && !lsBody.nameIds && !lsBody.refreshIds && r.status === 200, lsBody);
  r = await ops.listingPhotos({ listingIds: ['1718099'], prepare: true }).catch(e => ({ error: e.message }));
  ok('1b · E5: listingPhotos from the cloud never prepares (no image fetch)', !spy.ops.some(o => o.op === 'listingPhotos' && o.marked && JSON.stringify(o).includes('prepare')), r);
  ok('1b · E5: the Etsy stub saw nothing, the router saw no Etsy and no other address, the Etsy switch was never written',
    F.etsy.calls === 0 && F.etsy.inventory.length === 0 && !F.net.tripwire.length && !F.net.other.length && !F.stopWrites.length && !F.doc('EtsyApi_Config/stop'), { etsy: F.etsy, net: { trip: F.net.tripwire, other: F.net.other } });
  // C1: the reserved name
  r = await P({ op: 'runCarrier', runId, dev: 'cloud', by: 'Mallory' });
  ok('1b · C1: a page that sends dev "cloud" is refused (403); "cloud" is never judged from the body', r.status === 403 && r.reserved === true, r);
  r = await P({ op: 'putSheet', dev: 'cloud', sheet: { id: 'sheetC1', label: null } });
  ok('1b · C1: ... whatever the op', r.status === 403, r);

  /* C7: writer A, then writer B: A's next write is refused inside the op's own transaction, and nothing changes */
  cloudCarrier(runId);
  F.seed(LEASE, { nonce: 'nonceA', state: 'running', startedAt: Date.now(), beatAt: Date.now(), heartbeat: true });
  const fenceA = { path: LEASE, nonce: 'nonceA', runId }, fenceB = { path: LEASE, nonce: 'nonceB', runId };
  r = await lib.call({ op: 'runLinesPatch', runId, lines: { [L1]: { state: 'pooled', poolIds: [P1] } } }, { unit: 'pool|A', fence: fenceA });
  ok('1b · C7: writer A, holding the lease, writes', r.status === 200 && (r.applied || []).includes(L1), r);
  F.seed(LEASE, { nonce: 'nonceB', state: 'running', startedAt: Date.now(), beatAt: Date.now(), heartbeat: true });
  r = await lib.call({ op: 'runLinesPatch', runId, lines: { [L1]: { state: 'pooled', poolIds: [P1, P2] } } }, { unit: 'pool|B', fence: fenceB });
  ok('1b · C7: writer B takes the lease and writes', r.status === 200, r);
  const before = JSON.stringify(F.list('Charm_Nest_Run_Live')), beforeRun = JSON.stringify(F.doc('Charm_Nest_Runs/' + runId));
  r = await lib.call({ op: 'runLinesPatch', runId, lines: { [L1]: { state: 'gone' } } }, { unit: 'pool|A2', fence: fenceA });
  ok('1b · C7: A\'s next write is fenced (409 fenced) and no document changed', r.status === 409 && r.fenced === true && JSON.stringify(F.list('Charm_Nest_Run_Live')) === before && JSON.stringify(F.doc('Charm_Nest_Runs/' + runId)) === beforeRun, r);
  // inside the op's own transaction: the lease moves between the op's reads and its commit (after the pre-check passed): the transaction runs again and is fenced
  F.seed(LEASE, { nonce: 'nonceA', state: 'running', startedAt: Date.now(), beatAt: Date.now(), heartbeat: true });
  F.hooks.beforeCommit = async () => { F.seed(LEASE, { nonce: 'nonceB', state: 'running', startedAt: Date.now(), beatAt: Date.now(), heartbeat: true }); };
  const before2 = JSON.stringify(F.list('Charm_Nest_Run_Live'));
  r = await lib.call({ op: 'runLinesPatch', runId, lines: { [L1]: { state: 'gone' } } }, { unit: 'pool|A3', fence: fenceA });
  ok('1b · C7: a lease taken between the op\'s reads and its commit fences the op inside its own transaction; nothing is written', r.status === 409 && r.fenced === true && JSON.stringify(F.list('Charm_Nest_Run_Live')) === before2 && !F.hooks.beforeCommit, r);
  // a computer takes the run back: the cloud's write is fenced on the carrier
  F.seed(LEASE, { nonce: 'nonceA', state: 'running', startedAt: Date.now(), beatAt: Date.now(), heartbeat: true });
  F.seed('Charm_Nest_Run_Carrier/' + runId, { runId, carrier: { dev: 'd1aaaa', by: 'Ann', at: Date.now(), since: Date.now() } });
  r = await lib.call({ op: 'runLinesPatch', runId, lines: { [L1]: { state: 'gone' } } }, { unit: 'pool|A4', fence: fenceA });
  ok('1b · C7: when a computer carries the run, the cloud\'s write is fenced', r.status === 409 && r.fenced === true, r);
  // a page's call never reads the fence (with the drive off it does not even kick the driver, which would read its lease)
  setDrive(lib, 'off');
  const m1 = F.count();
  r = await P({ op: 'runLinesPatch', runId, dev: 'd1aaaa', by: 'Ann', lines: { [L1]: { hold: null } } });
  ok('1b · a page\'s write never reads the cloud\'s lease or fence (it runs today\'s code)', r.status === 200 && !F.since(m1).readPaths.includes(LEASE), F.since(m1).readPaths);
  setDrive(lib, 'standby', { cloudSteps: ['pull'] });
  cloudCarrier(runId);

  /* ═══ 1c · line merges (P3) ═══ */
  // the cloud stamps a line (state pooled, its pieces, the fields only the cloud writes)
  F.seed(LEASE, { nonce: 'nonceA', state: 'running', startedAt: Date.now(), beatAt: Date.now(), heartbeat: true });
  r = await lib.call({ op: 'runLinesPatch', runId, lines: { [L1]: { state: 'pooled', poolIds: [P1], poolProblems: [{ kind: 'oversize' }], changedAt: 5, changedWhy: 'size', material: 'gf' } } }, { unit: 'pool|P3', fence: fenceA });
  const stored = () => { const out = {}; for (const p of F.list('Charm_Nest_Run_Live')) if (p.runId === runId) Object.assign(out, JSON.parse(p.json)); return out; };
  const s1 = stored()[L1];
  ok('1c · the cloud writes the wider list of line fields (material, poolProblems, changedAt) with its stamp', r.status === 200 && s1 && s1._lp && s1._lp.dev === 'cloud' && s1.material === 'gf' && s1.poolProblems && s1.changedWhy === 'size', s1);
  r = await P({ op: 'runLinesPatch', runId, dev: 'd1aaaa', by: 'Ann', lines: { [L1]: { material: 'ss', poolProblems: [] } } });
  const s1b = stored()[L1];
  ok('1c · a page\'s patch writes exactly today\'s fields (material and poolProblems are dropped)', s1b.material === 'gf' && s1b.poolProblems.length === 1, s1b);
  // the carrier adopts the cloud's stamp, changes pooled → written and its pieces, and saves the run whole: its change is kept (the strictly-newer rule, no equal-stamp rule)
  F.seed('Charm_Nest_Run_Carrier/' + runId, { runId, carrier: { dev: 'd1aaaa', by: 'Ann', at: Date.now(), since: Date.now() } });
  r = await lib.call({ op: 'runLinesPatch', runId, lines: { [L1]: { poolProblems: [{ kind: 'oversize' }] } } }, { unit: 'pool|P3b' });   // (unfenced: the cloud stamps last)
  const adopted = Object.assign({}, stored()[L1]); delete adopted.poolProblems; delete adopted.changedAt; delete adopted.changedWhy; delete adopted.designHold;
  const pageLine = Object.assign({}, adopted, { state: 'written', poolIds: [P1, P2] });
  r = await P({ op: 'runPut', dev: 'd1aaaa', run: { runId, status: 'running', step: 'nest', orders: [R], lines: { [L1]: pageLine } } });
  const s2 = stored()[L1];
  ok('1c · P3: the carrier\'s own change on a cloud-stamped line is kept (pooled → written, its pieces)', r.status === 200 && s2.state === 'written' && JSON.stringify(s2.poolIds) === JSON.stringify([P1, P2]), s2);
  ok('1c · P3: the fields only the cloud writes survive a page\'s save that lacks them (poolProblems, changedAt, changedWhy)', s2.poolProblems && s2.poolProblems[0].kind === 'oversize' && s2.changedAt === 5 && s2.changedWhy === 'size', s2);
  // with no cloud stamp anywhere: runPut writes exactly what the page sent
  const run2 = 'run-2026-11-20-plain01'; seedRun(run2, { lineEditAt: Date.now() - 1000 });
  await P({ op: 'runLinesAdd', runId: run2, dev: 'd1aaaa', by: 'Ann', lines: { [L1]: lineOf('pulled') } });
  const plainLines = { [L1]: lineOf('written', { poolIds: [P1], _lp: { at: Date.now() + 5, dev: 'd1aaaa', by: 'Ann' } }) };
  r = await P({ op: 'runPut', dev: 'd1aaaa', run: { runId: run2, status: 'running', step: 'nest', orders: [R], lines: plainLines } });
  const plainStored = {}; for (const p of F.list('Charm_Nest_Run_Live')) if (p.runId === run2) Object.assign(plainStored, JSON.parse(p.json));
  ok('1c · P3: with no cloud stamp anywhere, runPut writes exactly the lines the page sent', r.status === 200 && JSON.stringify(plainStored) === JSON.stringify(plainLines), { plainStored, plainLines });
  F.seed('Charm_Nest_Open/run', { runId, at: Date.now() });

  /* ═══ 1c · contentRev (P2) ═══ */
  cloudCarrier(runId);
  F.seed('Charm_Pool/' + P1, { poolId: P1, state: 'written', sheetId: 'sheetD1', runId });
  F.seed('Charm_Nest_Sheets/sheetD1', { id: 'sheetD1', runId, metal: 'gf', layoutRev: 7, contentRev: 2, poolIds: [P1], placements: [{ id: 'c1', cxPt: 10, cyPt: 10, angle: 0 }], charms: [{ id: 'c1', poolId: P1 }], fileBase: 'GF_D_Sheet-1' });
  setDrive(lib, 'standby');
  r = await P({ op: 'sheetLayoutPatch', id: 'sheetD1', tab: 't1', by: 'Ann', poses: { c1: { cxPt: 30, cyPt: 10, angle: 0 } } });
  let d1 = F.doc('Charm_Nest_Sheets/sheetD1');
  ok('1c · P2: a move by hand raises contentRev, not layoutRev (drive on)', r.status === 200 && d1.contentRev === 3 && d1.layoutRev === 7, { r, contentRev: d1.contentRev, layoutRev: d1.layoutRev });
  r = await P({ op: 'artPut', poolId: P1, face: 'front', by: 'Ann', clear: true });
  d1 = F.doc('Charm_Nest_Sheets/sheetD1');
  ok('1c · P2: an artwork edit of a piece on the sheet raises contentRev, not layoutRev', r.status === 200 && d1.contentRev === 4 && d1.layoutRev === 7 && d1.filesOwed > 0, { r, d1: { contentRev: d1.contentRev, layoutRev: d1.layoutRev } });
  r = await P({ op: 'putSheet', dev: 'd1aaaa', layoutBase: 7, sheet: { id: 'sheetD1', runId, placements: d1.placements, charms: d1.charms, contentRev: 1 } });
  d1 = F.doc('Charm_Nest_Sheets/sheetD1');
  ok('1c · P2: a page\'s save with layoutBase still succeeds after a move by hand, as today (and its own contentRev is never written)', r.status === 200 && r.layoutRev === 8 && d1.contentRev === 5, { r, contentRev: d1.contentRev });
  r = await lib.call({ op: 'putSheet', contentBase: 4, sheet: { id: 'sheetD1', runId, placements: d1.placements, charms: d1.charms } }, { unit: 'finish|old' });
  ok('1c · P2: a cloud save made from an older content (contentBase) is refused', r.status === 409 && r.conflict === true && r.contentRev === 5, r);
  F.seed('Charm_Nest_Sheets/sheetD1', Object.assign(F.doc('Charm_Nest_Sheets/sheetD1'), { filesOwed: 900, filesJob: { jobId: 'j1' } }));
  r = await lib.call({ op: 'putSheet', filesOwedBase: 800, filesJobBase: 'j1', sheet: { id: 'sheetD1', filesOwed: null, label: { url: 'l' } } }, { unit: 'finish|clear' });
  ok('1c · P1/P2: filesOwed is cleared only when it still equals the owed mark claimed (compare-and-clear)', r.status === 200 && r.filesOwedKept === true && F.doc('Charm_Nest_Sheets/sheetD1').filesOwed === 900, r);
  r = await lib.call({ op: 'putSheet', filesJobBase: 'j2', sheet: { id: 'sheetD1', filesOwed: null } }, { unit: 'finish|job' });
  ok('1c · P1: a save from another files job is refused (filesJobBase)', r.status === 409 && r.conflict === true, r);
  r = await lib.call({ op: 'putSheet', filesOwedBase: 900, filesJobBase: 'j1', sheet: { id: 'sheetD1', filesOwed: null } }, { unit: 'finish|ok' });
  ok('1c · P1: ... and the right one clears it', r.status === 200 && !F.doc('Charm_Nest_Sheets/sheetD1').filesOwed, r);

  /* ═══ 1d · the cloud's carrier in standby (C6, P8) ═══ */
  cloudCarrier(runId); setDrive(lib, 'standby');
  r = await P({ op: 'runCarrier', runId, dev: 'd1aaaa', by: 'Ann' });
  ok('1d · standby: a computer\'s beat or Auto (no take) gets mine:false on a run the cloud carries', r.status === 200 && r.mine === false && r.alive === true, r);
  F.clock.advance(400000);
  r = await P({ op: 'runCarrier', runId, dev: 'd1aaaa', by: 'Ann', take: true });
  ok('1d · P8: ... whatever its age, even with take (only a person\'s Resume takes it back)', r.mine === false && F.doc('Charm_Nest_Run_Carrier/' + runId).carrier.dev === 'cloud', r);
  cloudCarrier(runId);
  F.seed('Charm_Nest_Finish/sheetD1', { runId, sheetId: 'sheetD1', fileBase: 'GF_D_Sheet-1', state: 'running', beatAt: Date.now() });
  r = await P({ op: 'runCarrier', runId, dev: 'd1aaaa', by: 'Ann', restoreFromCloud: true });
  ok('1d · C6: a person\'s Resume while the cloud finishes a sheet: handingBack, in words, and the cloud is told to yield', r.mine === false && r.handingBack === true && /finishing GF_D_Sheet-1/.test(r.why) && F.doc('Charm_Nest_Run_Carrier/' + runId).yieldTo.dev === 'd1aaaa', r);
  F.seed('Charm_Nest_Finish/sheetD1', { runId, sheetId: 'sheetD1', state: 'done', beatAt: Date.now() });
  r = await P({ op: 'runCarrier', runId, dev: 'd1aaaa', by: 'Ann', restoreFromCloud: true });
  ok('1d · C6: once the task ended, the same Resume takes the run back', r.mine === true && r.restored === true && F.doc('Charm_Nest_Run_Carrier/' + runId).carrier.dev === 'd1aaaa', r);
  cloudCarrier(runId); setDrive(lib, 'on');
  r = await P({ op: 'runCarrier', runId, dev: 'd1aaaa', by: 'Ann', restoreFromCloud: true, cv: 'test' });
  ok('1d · mode on: the cloud\'s carrier is sticky (a Resume gets mine:false)', r.mine === false, r);
  setDrive(lib, 'off'); F.clock.advance(200000);
  r = await P({ op: 'runCarrier', runId, dev: 'd1aaaa', by: 'Ann' });
  ok('1d · the drive turned off: a run the cloud carried is taken as any other once it went quiet (no run is left stuck)', r.mine === true && F.doc('Charm_Nest_Run_Carrier/' + runId).carrier.dev === 'd1aaaa', r);

  /* ═══ 1d · runOpen in mode on: a run made then is carried by the cloud; the page follows it ═══ */
  F.store.delete('Charm_Nest_Open/run'); for (const x of F.list('Charm_Nest_Runs')) F.seed('Charm_Nest_Runs/' + x._id, Object.assign({}, x, { status: 'complete' }));
  setDrive(lib, 'on');
  r = await P({ op: 'runOpen', mode: 'auto', day: '2026-11-20', cv: 'test' });
  ok('1d · mode on: Start makes the run in the cloud, carried by the cloud from its first commit; the page follows (created:false)', r.ok === true && r.created === false && r.runId && F.doc('Charm_Nest_Run_Carrier/' + r.runId).carrier.dev === 'cloud', r);
  const onRun = r.runId;

  /* ═══ 1d · mode on refusals (C1) ═══ */
  r = await P({ op: 'runPut', dev: 'd1aaaa', cv: 'test', run: { runId: onRun, status: 'running' } });
  ok('1d · mode on: a page\'s runPut is refused in words (409 cloudCarries, no retry flag)', r.status === 409 && r.cloudCarries === true && !r.retry && /cloud carries every run/.test(r.error), r);
  r = await P({ op: 'putSheet', dev: 'd1aaaa', cv: 'test', layoutBase: 9, sheet: { id: 'sheetD1', placements: [] } });
  ok('1d · mode on: a page\'s putSheet with placements is refused, with or without layoutBase', r.status === 409 && r.cloudCarries === true && r.fields.includes('placements'), r);
  r = await P({ op: 'putSheet', dev: 'd1aaaa', cv: 'test', sheet: { id: 'sheetD1', note: 'hello' } });
  ok('1d · mode on: a page\'s putSheet of a field the cloud does not write goes through', r.status === 200, r);
  r = await P({ op: 'runArchive', runId: onRun, parts: [{ json: '{"a":{"orderId":"1"}}' }] });
  ok('1d · mode on: a production write from a page built before (no cv) gets the reload words', r.status === 409 && r.reload === true, r);
  // (build step 14, commit 3a04e0c, plan §1.4 "a non-READ op whose body carries no cv is refused with the reload words" and §2.2 Old tabs: a person's press from a page built before
  //  the viewers is refused in words that tell the person to reload and that nothing was changed, never dropped silently; the same press from a current page is answered as today)
  const linesBefore = JSON.stringify((F.doc('Charm_Nest_Runs/' + onRun) || {}).lines || {});
  r = await P({ op: 'runLinesPatch', runId: onRun, dev: 'd1aaaa', by: 'Ann', lines: { x: {} } });
  ok('1d · mode on: a person\'s press (a hold) from a page older than build step 14 (no cv) is refused with the reload words, nothing changed (step 14\'s cv rule, 3a04e0c)', r.status === 409 && r.reload === true && r.cloudCarries === true && /old version: it reloads/.test(r.error) && /Nothing was changed/.test(r.error) && JSON.stringify((F.doc('Charm_Nest_Runs/' + onRun) || {}).lines || {}) === linesBefore, r);
  r = await P({ op: 'runLinesPatch', runId: onRun, dev: 'd1aaaa', by: 'Ann', cv: 'test', lines: { x: {} } });
  ok('1d · mode on: the same press from a current page (cv) is answered as today (no cloudCarries refusal)', r.status !== 409 || !r.cloudCarries, r);
  r = await P({ op: 'getSheet', id: 'sheetD1' });
  ok('1d · mode on: a read is never refused', r.status === 200, r.status);
  r = await P({ op: 'runPut', dev: 'd1aaaa', run: { runId: onRun, status: 'running' }, sandbox: true });
  ok('1d · mode on: the sandbox is never refused by the drive (m:P5)', r.status !== 409 || !r.cloudCarries, r);
  setDrive(lib, 'standby');
  r = await P({ op: 'runPut', run: { runId: onRun, status: 'running', step: 'pull' } });
  ok('1d · standby: a page\'s runPut is answered as today (no cloudCarries refusal)', r.status === 200 && !r.cloudCarries, r);

  /* ═══ 1d · SETSHUT on the server (P4) ═══ */
  setDrive(lib, 'off');
  const O1 = '3940000002', NEWP = O1 + '_39400000021_1', OWN2 = R + '_39400000012_1';
  F.seed('Charm_Nest_Sheets/sheetS1', { id: 'sheetS1', runId, metal: 'gf', setId: 'set-1', draft: false, poolIds: [P1], placements: [], charms: [], fileBase: 'GF_S_Sheet-1' });
  F.seed('Charm_Nest_Sets/set-1', { setId: 'set-1', runId, sheetIds: ['sheetS1'], seq: 1 });
  r = await lib.call({ op: 'putSheet', sheet: { id: 'sheetS1', poolIds: [P1, NEWP] } }, { unit: 'place|S1' });
  ok('1d · P4 SETSHUT: the cloud cannot add a new order to a sheet in a set', r.status === 409 && r.refused === 'setShut' && JSON.stringify(F.doc('Charm_Nest_Sheets/sheetS1').poolIds) === JSON.stringify([P1]), r);
  r = await lib.call({ op: 'putSheet', sheet: { id: 'sheetS1', poolIds: [P1, OWN2] } }, { unit: 'place|S1own' });
  ok('1d · P4 SETSHUT: ... but a later piece of an order already on that sheet joins it', r.status === 200 && F.doc('Charm_Nest_Sheets/sheetS1').poolIds.includes(OWN2), r);
  F.seed('Charm_Nest_Sheets/sheetS2', { id: 'sheetS2', runId, metal: 'gf', draft: true, poolIds: [], placements: [], charms: [] });
  F.seed('Charm_Nest_Sets/set-2', { setId: 'set-2', runId, sheetIds: ['sheetS2'], seq: 2 });
  r = await lib.call({ op: 'putSheet', sheet: { id: 'sheetS2', poolIds: [NEWP] } }, { unit: 'place|S2' });
  ok('1d · P4 SETSHUT: a draft that any set lists takes no new order either', r.status === 409 && r.refused === 'setShut', r);
  r = await lib.call({ op: 'putSheet', sheet: { id: 'sheetS2', draft: false, setId: null } }, { unit: 'member|S2' });
  ok('1d · P4: the cloud never changes the membership of a sheet a set lists (NOPARTIALSET)', r.status === 409 && r.refused === 'setShut', r);
  r = await P({ op: 'putSheet', dev: 'd1aaaa', sheet: { id: 'sheetS1', poolIds: [P1, OWN2, NEWP] } });
  ok('1d · P4: with the drive off, a page\'s save is as today (the page keeps its own SETSHUT, charm-nest-bridge.js closed())', r.status === 200, r);
  F.seed('Charm_Nest_Sheets/sheetK1', { id: 'sheetK1', runId, metal: 'gf', setId: 'set-3', draft: false, keepInSet: { setId: 'set-3', by: 'Paul', at: 1 }, poolIds: [P1], placements: [], charms: [] });
  F.seed('Charm_Nest_Sets/set-3', { setId: 'set-3', runId, sheetIds: ['sheetK1'], seq: 3 });
  r = await lib.call({ op: 'putSheet', sheet: { id: 'sheetK1', draft: true, setId: null } }, { unit: 'member|K1' });
  ok('1d · KEEPINSET: a sheet the owner put back in its set stays in it after a cloud write', F.doc('Charm_Nest_Sheets/sheetK1').setId === 'set-3' && F.doc('Charm_Nest_Sheets/sheetK1').draft === false, { r, d: F.doc('Charm_Nest_Sheets/sheetK1') });

  /* ═══ 1d · the Storage door (P1, C1) ═══ */
  const out = F.load('charmNestOutput.js', { fresh: false }), O = handlerOf(out);
  const sheetPath = 'charmnest/sheets/2026-11-20/GF_D_Sheet-1/GF_D_Sheet-1.ai', b64 = Buffer.from('%!PS ai').toString('base64');
  F.seed('Charm_Nest_Sheets/sheetD1', Object.assign(F.doc('Charm_Nest_Sheets/sheetD1'), { archived: false }));
  cloudCarrier(runId); setDrive(null, 'off'); F.clock.advance(16000);
  r = await O({ op: 'put', path: sheetPath, base64: b64, contentType: 'application/postscript' });
  ok('1d · the Storage door, drive off: a page\'s upload of a sheet file lands as today', r.status === 200 && F.blobs.has(sheetPath), r);
  F.blobs.delete(sheetPath); setDrive(null, 'standby'); F.clock.advance(16000);
  r = await O({ op: 'put', path: sheetPath, base64: b64, contentType: 'application/postscript' });
  ok('1d · P1: standby, the cloud carries the sheet\'s run: a page\'s upload is refused (409) before any byte lands', r.status === 409 && r.cloudCarries === true && !F.blobs.has(sheetPath), r);
  r = await O({ op: 'sign', path: sheetPath, contentType: 'application/postscript' });
  ok('1d · P1: ... and so is its signed upload link', r.status === 409 && !F.signed.some(s => s.path === sheetPath), r);
  F.seed('Charm_Nest_Run_Carrier/' + runId, { runId, carrier: { dev: 'd1aaaa', by: 'Ann', at: Date.now(), since: Date.now() } });
  r = await O({ op: 'sign', path: sheetPath, contentType: 'application/postscript' });
  const sig = F.signed.filter(s => s.path === sheetPath).pop();
  ok('1d · standby, a computer carries the run: its upload link is given, and lasts a minute (not 15)', r.status === 200 && sig && sig.expires - Date.now() <= 60000, sig);
  setDrive(null, 'on'); F.clock.advance(16000);
  r = await O({ op: 'put', path: 'charmnest/sets/2026-11-20/Set-1/labels.pdf', base64: b64, contentType: 'application/pdf' });
  ok('1d · C1: mode on: every page write under sheets/ or sets/ is refused', r.status === 409 && r.cloudCarries === true, r);
  r = await O({ op: 'put', path: 'charmnest/uploads/x.png', base64: b64, contentType: 'image/png' });
  ok('1d · C1: ... and nothing else is', r.status === 200, r);

  /* ═══ the owner's rules held ═══ */
  ok('Etsy: the stub was never called, no Etsy address or proxy was asked, the Etsy switch was never written', F.etsy.calls === 0 && !F.net.tripwire.length && !F.stopWrites.length, { etsy: F.etsy.calls, trip: F.net.tripwire });
  ok('no address outside the named functions was asked', !F.net.other.length, F.net.other);
  ok('no browser global exists (no page code was loaded)', !F.browserGlobals().length, F.browserGlobals());
  console.log(failed ? `FAIL cloud-foundations · ${failed} check(s)` : 'PASS cloud-foundations');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
