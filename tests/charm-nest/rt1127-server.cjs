// RT1127 SERVER (Paul, 27 Nov 2026: "The Library sheets are NOT updating to the latest versions from the cloud. I have also seen sheets update and reverse back to their previous incorrect state ...
// updates are very delayed for updating SKUs ... it can not take more than 30s at worst"; "I did not see any feedback to the user when a sheet is being altered"). Offline, over _cloudFakes.cjs
// (the real library handler and units; no network, no Firebase, no Etsy, nothing paid):
//   1  FRESHWRITE  a look's write planned on an older read never undoes a person's Change SKU: the labels unit's label + order list of a sheet, its set files (finalize), the assembly's set
//                  save, and a seated swap planned before a move by hand are refused when the record moved since (nothing written, the look planned again); a fresh look writes as before;
//                  a set's revision (setRev) moves with every write of it, and a write with the revision it read goes through
//   2  ONEHOLD     one process does a person's request: its kick is written with the request (a pass looking in between leaves it), a holder's beat stamped after a look began is fresh,
//                  the request's own process keeps it between its turns (the pass cannot take it then), its own process leaves one the pass already does, a swallowed overBudget is said
//   3  STEPSAID / TRUEDONE  Make it again / Change SKU says each stage (making, seating, no room or writing), once each; its answer says exactly what is placed and what still waits
//   4  BUSYFLAGS   the Library's live read passes nestJob { status, at }, filesOwed, nestOwed (ms or null) and refit (true / false) of each sheet
//   RT1127_FN=<another checkout>/netlify/functions runs the same checks against that code (the clean checkout fails each part).
//   node tests/charm-nest/rt1127-server.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const path = require('path');
const T0 = Date.UTC(2026, 10, 27, 18, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const DIR = process.env.RT1127_FN ? path.resolve(process.env.RT1127_FN) : F.fnDir, L = f => F.load(f, { dir: DIR });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {}; console.error = () => {};
let n = 0, failed = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const part = async (name, fn) => { try { await fn(); } catch (e) { ok(name + ' ran to its end', false, String((e && e.stack) || e).slice(0, 900)); } };

const lib = L('charmNestLibrary.js'), Labels = L('_charmNestLabels.js'), Assembly = L('_charmNestAssembly.js'), Unit = L('_charmNestCmdUnit.js'), Exec = L('_charmNestCmdExec.js'), QrLabel = L('_charmNestQrLabel.js');
const Intake = L('_charmNestIntake.js'), SeatNow = L('_charmNestSeatNow.js'), PlaceUnit = L('_charmNestPlaceUnit.js');
// (the QR drawing, @resvg/resvg-js, may not be installed here: a fixed 1x1 PNG then stands for each drawn label; every other part is the real unit)
try { QrLabel.renderPng({ payload: 'x', label: 'x', notes: [] }); } catch (_) { const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==', 'base64'); QrLabel.renderPng = () => ({ png, ecc: 'M' }); }
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };

const R = 'run-2026-11-27-rt', DAY = '2026-11-27', S1 = 'gf-rt-s1', SET = 'set-2026-11-27-1', DEV = 'd0michelle27', LEASE = 'Charm_Nest_Drive/lease';
const O1 = '4231000001', O2 = '4231000002', T1 = '5301000001', T2 = '5301000002', K1 = `${O1}_${T1}`, K2 = `${O2}_${T2}`, P1 = K1 + '_1', P2 = K2 + '_1';
const fence = { path: LEASE, nonce: 'dn-rt', runId: R };
const cloudCall = (b, u) => lib.call(b, { unit: String(u), fence }), read = b => lib.call(b, {});
const sheetDoc = id => F.doc('Charm_Nest_Sheets/' + id) || {}, setDoc = id => F.doc('Charm_Nest_Sets/' + id) || {};
const FB = 'GF_Nov.27.26_Set-1_Sheet-1', FOLDER = `charmnest/sets/${DAY}/Set-1`;
function seed() {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: ['pull', 'pool', 'nest', 'files', 'labels', 'checkpoint', 'cmd:remakeline'], flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'labels', releasePolicy: 2, day: DAY, setIds: [SET], errors: [], orders: [O1, O2],
    lines: { [K1]: { orderId: O1, state: 'written', sku: 'LEAF', poolIds: [P1] }, [K2]: { orderId: O2, state: 'written', sku: 'OLD_SKU', poolIds: [P2] } } });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  F.seed(LEASE, { nonce: fence.nonce, state: 'running', heartbeat: true, startedAt: T0, beatAt: T0, at: T0 });
  const charms = [{ id: 'c1', poolId: P1, order: O1, sku: 'LEAF', hash: 'h1', name: `${O1} · LEAF`, areaPt2: 300, widthPt: 20, heightPt: 20, orderInfo: { transactionId: T1, sku: 'LEAF', copy: 1 } },
    { id: 'c2', poolId: P2, order: O2, sku: 'OLD_SKU', hash: 'h2', name: `${O2} · OLD_SKU`, areaPt2: 300, widthPt: 20, heightPt: 20, orderInfo: { transactionId: T2, sku: 'OLD_SKU', copy: 1 } }];
  F.seed('Charm_Nest_Sheets/' + S1, { id: S1, runId: R, metal: 'gold', metalLabel: 'GF', day: DAY, page: 1, draft: false, setId: SET, setSeq: 1, sheetIndex: 1, fileBase: FB, folder: FB, archived: false,
    charms, poolIds: [P1, P2], orders: [O1, O2], placements: [{ id: 'c1', cxPt: 20, cyPt: 20, angle: 0, wPt: 20, hPt: 20, scale: 1 }, { id: 'c2', cxPt: 60, cyPt: 20, angle: 0, wPt: 20, hPt: 20, scale: 1 }],
    layoutRev: 5, contentRev: 5, status: 'complete', releaseFull: true, verification: { ok: true }, stock: { wPt: 182, hPt: 62 },
    outputs: { ai: { path: `${FOLDER}/${FB}/${FB}.ai`, url: 'https://storage.test.invalid/s1.ai' }, pdf: { path: `${FOLDER}/${FB}/${FB}.pdf`, url: 'https://storage.test.invalid/s1.pdf' } }, backPool: [] });
  for (const [p, o, t, sku] of [[P1, O1, T1, 'LEAF'], [P2, O2, T2, 'OLD_SKU']]) F.seed('Charm_Pool/' + p, { poolId: p, orderId: o, lineKey: p.replace(/_1$/, ''), transactionId: t, sku, copy: 1, state: 'written', sheetId: S1, setId: SET, sheetName: FB, material: 'gold', runId: R, quantity: 1 });
  const line = (t, sku, p) => ({ transactionId: t, sku, copies: [{ copy: 1, sheetId: S1, sheet: FB, poolId: p, backPoolId: null }] });
  F.seed('Charm_Nest_Sets/' + SET, { setId: SET, runId: R, day: DAY, seq: 1, name: 'Set-1', folder: FOLDER, group: 'dispatch', status: 'nesting', sheetIds: [S1], materials: ['gold'],
    orders: { [O1]: { held: null, lines: [line(T1, 'LEAF', P1)] }, [O2]: { held: null, lines: [line(T2, 'OLD_SKU', P2)] } }, labelFiles: [], labels: null, formedAt: T0 - 600e3, sheetNos: { gold: 1 } });
}
const ctxOf = run => ({ db: F.db, run, runId: R, call: cloudCall, callNow: cloudCall, read, fence, cloudSteps: ['labels', 'checkpoint'], say: () => {}, budgetEnd: Date.now() + 600000, bucket: F.bucket, gate: async () => ({ ok: true, why: [] }) });
const runRec = () => Object.assign({ runId: R }, F.doc('Charm_Nest_Runs/' + R));
// a person's Change SKU of order O2's line, in its own process (the real cmdSwapLine): the piece is made again in its place, from NEW_SKU
const changeSku = () => cloudCall({ op: 'cmdSwapLine', runId: R, lineKey: K2, rid: O2, sheetId: S1, by: 'Michelle', keepBacks: false,
  pools: [{ poolId: P2, orderId: O2, lineKey: K2, transactionId: T2, sku: 'NEW_SKU', copy: 1, material: 'gold', runId: R, quantity: 1 }],
  charms: [{ id: 'c2n', poolId: P2, order: O2, sku: 'NEW_SKU', hash: 'h2n', name: `${O2} · NEW_SKU`, areaPt2: 300, widthPt: 20, heightPt: 20, orderInfo: { transactionId: T2, sku: 'NEW_SKU', copy: 1 } }],
  poses: [{ poolId: P2, cxPt: 60, cyPt: 20, angle: 0, wPt: 20, hPt: 20 }], layoutBase: 5 }, 'person|' + Math.random());
// a person's take-off of order O2's line from S1 (the real cmdSwapLine with no pieces: it moved to another sheet)
const moveOff = () => cloudCall({ op: 'cmdSwapLine', runId: R, lineKey: K2, rid: O2, sheetId: S1, by: 'Michelle', pools: [], charms: [] }, 'person-off|' + Math.random());
const o2Sku = () => { const o = (setDoc(SET).orders || {})[O2]; const l = o && (Array.isArray(o.lines) ? o.lines : Object.values(o.lines || {}))[0]; return l ? l.sku : null; };

(async () => {
  /* ═══ 1 · FRESHWRITE ═══ */
  await part('1a', async () => {
    seed();
    const V = await Assembly.loadView({ db: F.db, run: runRec(), now: Date.now() }), sh = V.pages.find(p => p.sheetId === S1), set = V.sets.find(s => s.setId === SET);
    const off = await moveOff();
    ok('1a · setup: the person\'s move took order O2 off S1 (its order list says O1 only)', off && off.ok === true && JSON.stringify(sheetDoc(S1).orders) === JSON.stringify([O1]), { off, orders: sheetDoc(S1).orders });
    let threw = null; try { await Labels.drawSheet(ctxOf(runRec()), V, sh, set); } catch (e) { threw = e; }
    ok('1a · the labels unit\'s label drawn from the older read does not put order O2 back on S1\'s record', JSON.stringify(sheetDoc(S1).orders) === JSON.stringify([O1]), { orders: sheetDoc(S1).orders, label: (sheetDoc(S1).label || {}).orders });
    ok('1a · its write was refused as planned on an older read (replan), never a failure of the run', !!(threw && threw.replan), threw && String(threw.message || threw));
    const V2 = await Assembly.loadView({ db: F.db, run: runRec(), now: Date.now() }), sh2 = V2.pages.find(p => p.sheetId === S1), set2 = V2.sets.find(s => s.setId === SET);
    let again = null; try { await Labels.drawSheet(ctxOf(runRec()), V2, sh2, set2); } catch (e) { again = e; }
    ok('1a · the look planned again on the record as it is now draws the label of O1 alone', !again && JSON.stringify((sheetDoc(S1).label || {}).orders) === JSON.stringify([O1]) && (setDoc(SET).labelFiles || []).length === 1, { again: again && String(again.message), label: sheetDoc(S1).label && sheetDoc(S1).label.orders });
  });
  await part('1b', async () => {
    seed();
    const V = await Assembly.loadView({ db: F.db, run: runRec(), now: Date.now() }), set = V.sets.find(s => s.setId === SET);
    const sw = await changeSku();
    ok('1b · setup: the person\'s Change SKU wrote order O2\'s line in the set as NEW_SKU', sw && sw.ok === true && o2Sku() === 'NEW_SKU', { sw, sku: o2Sku() });
    let threw = null; try { await Labels.finalize(ctxOf(runRec()), V, set, {}); } catch (e) { threw = e; }
    ok('1b · the set\'s files made from the older read do not write its order lines back (O2 stays NEW_SKU)', o2Sku() === 'NEW_SKU', { sku: o2Sku(), threw: threw && String(threw.message || threw) });
    ok('1b · refused as planned on an older read (replan)', !!(threw && threw.replan), threw && String(threw.message || threw));
    const io = Assembly.cloudIo(ctxOf(runRec()), V, new Map()); let t2 = null; try { await io.saveSet(set); } catch (e) { t2 = e; }
    ok('1b · the set assembly\'s save from the same older read does not either', o2Sku() === 'NEW_SKU' && !!(t2 && t2.replan), { sku: o2Sku(), t2: t2 && String(t2.message || t2) });
    const V2 = await Assembly.loadView({ db: F.db, run: runRec(), now: Date.now() }), set2 = V2.sets.find(s => s.setId === SET);
    let t3 = null; try { await Labels.finalize(ctxOf(runRec()), V2, set2, {}); } catch (e) { t3 = e; }
    ok('1b · made again from the set as it is now, its files are recorded (labelled) and O2 is NEW_SKU', !t3 && setDoc(SET).status === 'labelled' && o2Sku() === 'NEW_SKU' && !!(setDoc(SET).labels || {}).pdf, { t3: t3 && String(t3.message || t3), status: setDoc(SET).status });
  });
  await part('1c', async () => {
    seed();
    const r0 = await cloudCall({ op: 'setUpdate', setId: SET, patch: { status: 'awaiting review' } }, 'rev0'), rev0 = num(setDoc(SET).setRev);
    const stale = await cloudCall({ op: 'setUpdate', setId: SET, patch: { status: 'nesting' }, setBase: rev0 - 1 }, 'rev-stale'), afterStale = setDoc(SET).status;
    const fresh = await cloudCall({ op: 'setUpdate', setId: SET, patch: { status: 'nesting' }, setBase: rev0 }, 'rev-fresh');
    ok('1c · every write of a set moves its revision, and says it', r0.ok === true && rev0 >= 1 && r0.setRev === rev0, { r0, rev0 });
    ok('1c · a write planned on an older revision is refused (409, conflict), nothing written', stale.status === 409 && stale.conflict === true && afterStale === 'awaiting review', { stale, afterStale });
    ok('1c · one planned on the revision it read goes through, and chains', fresh.ok === true && fresh.setRev === rev0 + 1 && setDoc(SET).status === 'nesting', fresh);
    // (a seated swap planned before a move by hand: the hand move changes contentRev, not layoutRev)
    F.seed('Charm_Nest_Sheets/' + S1, Object.assign(sheetDoc(S1), { contentRev: 6 }));
    const hand = await cloudCall({ op: 'cmdSwapLine', runId: R, lineKey: K2, rid: O2, sheetId: S1, by: 'Michelle', pools: [{ poolId: P2, orderId: O2, lineKey: K2, transactionId: T2, sku: 'NEW_SKU', copy: 1, material: 'gold', runId: R, quantity: 1 }],
      charms: [{ id: 'c2n', poolId: P2, order: O2, sku: 'NEW_SKU', hash: 'h2n', name: 'x', areaPt2: 300, widthPt: 20, heightPt: 20 }], poses: [{ poolId: P2, cxPt: 60, cyPt: 20, angle: 0 }], layoutBase: 5, contentBase: 5 }, 'hand-stale');
    ok('1c · a new piece seated on the sheet before a move by hand is seated again, not written over it (conflict)', hand.status === 409 && hand.conflict === true && (sheetDoc(S1).charms || []).some(c => c.id === 'c2'), hand);
  });

  /* ═══ 2 · ONEHOLD ═══ */
  const HOLD = 'Charm_Nest_Cmd_Hold/';
  /** Whether the pass takes the request and does it (its requests unit, under the pass's own lease, with the request's work stood in for): true when it did */
  const passTakes = async () => { const was = Exec.EXEC.remakeline; let ran = false; Exec.EXEC.remakeline = async () => { ran = true; return { wait: { text: 'Done by the pass…', work: { stage: 'pass' } } }; };
    try { await Unit.commandsUnit({ db: F.db, run: runRec(), runId: R, step: 'pass:commands', cursor: {}, call: cloudCall, callNow: cloudCall, read, cloudSteps: ['nest', 'pool', 'cmd:remakeline'], state: {}, fence, say: () => {}, live() {}, only: new Set(['remakeline']), unitKey: `${R}|pass|commands`, budgetEnd: Date.now() + 60000 }); }
    finally { Exec.EXEC.remakeline = was; } return ran; };
  const putCmd = (id, at) => F.seed('Charm_Nest_Run_Cmds/' + R, { runId: R, cmds: { [id]: { id, type: 'remakeline', args: { ids: [K2] }, by: 'Michelle', dev: DEV, at: at || T0, state: 'waiting' } } });
  await part('2a', async () => {
    seed(); F.net.kicks.length = 0;
    const rt = F.db.runTransaction; let probed = false, probe = 'not looked';
    F.db.runTransaction = async function (fn) { const out = await rt.call(F.db, fn); if (!probed) { const c = Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}).find(x => x.type === 'remakeline'); if (c) { probed = true; probe = await passTakes(); } } return out; };
    let put; try { put = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: { type: 'remakeline', ids: [K2], label: 'Change SKU' }, cv: 'cloud14-20261120' }); } finally { F.db.runTransaction = rt; }
    const h = F.doc(HOLD + put.id) || {};
    ok('2a · the press is put and its own process kicked', put.ok === true && F.net.kicks.some(k => k.name === 'charmNestDrive-background' && /"cmd"/.test(k.body)), { put, kicks: F.net.kicks.map(k => k.name) });
    ok('2a · a pass that looks the moment the request is written leaves it to its own process (its kick is written with it)', probe === false, { probe });
    ok('2a · the kick holds the key its process was started with', !!h.key && num(h.kickedAt) > 0 && JSON.parse((F.net.kicks.find(k => k.name === 'charmNestDrive-background') || { body: '{}' }).body).key === h.key, h);
  });
  await part('2b', async () => {
    seed(); putCmd('c2b');
    F.seed(HOLD + 'c2b', { runId: R, key: 'k2b', by: 'now:abcd1234', at: T0 + 50, state: 'running', nonce: 'k2b', beatAt: T0 + 50 });   // (its holder beat a moment after the pass's look began)
    const took = await passTakes();
    ok('2b · a holder\'s beat stamped after the look began is fresh: the pass does not take the request', took === false, { took, h: F.doc(HOLD + 'c2b') });
  });
  const nowCtx = (id, key, more) => { const hf = { path: 'Charm_Nest_Cmd_Hold/' + id, nonce: key, runId: R }, c = (b, u) => lib.call(b, { unit: String(u), fence: hf }); return Object.assign({ db: F.db, run: runRec(), runId: R, step: 'pass:commands', cursor: {}, call: c, callNow: c, read, cloudSteps: ['nest', 'pool', 'cmd:remakeline'], state: {}, fence: hf, say: () => {}, live() {}, worker: 'now:' + key.slice(0, 8), ownId: id, only: Unit.NOW_TYPES, unitKey: `${R}|now|${id}`, budgetEnd: Date.now() + 90000 }, more || {}); };
  await part('2c', async () => {
    seed(); const id = 'c2c', key = 'k2c0000000000000000000000000000a'; putCmd(id);
    F.seed(HOLD + id, { runId: R, key, kickedAt: T0 - 1000, state: 'running', nonce: key, beatAt: T0, by: 'now:' + key.slice(0, 8), at: T0 });   // (as its own process holds it once begun)
    const was = Exec.EXEC.remakeline; let turns = 0; Exec.EXEC.remakeline = async () => { turns++; return { wait: { text: 'Waiting for its sheet…', work: { stage: 'again' } } }; };
    try { await Unit.commandsUnit(nowCtx(id, key)); } finally { Exec.EXEC.remakeline = was; }
    const h = F.doc(HOLD + id) || {}; F.clock.advance(500); const took = await passTakes();
    ok('2c · setup: the request\'s own process did a turn of it', turns === 1, { turns });
    ok('2c · between its turns its own process still holds it', h.by === 'now:' + key.slice(0, 8), h);
    ok('2c · so the pass\'s lane cannot take it and do it behind its own unit\'s calls', took === false, { took });
  });
  await part('2d', async () => {
    seed(); const id = 'c2d', key = 'k2d0000000000000000000000000000a'; putCmd(id);
    F.seed(HOLD + id, { runId: R, key, kickedAt: T0 - 20000, by: 'pass', at: T0, state: 'pass' });   // (the kick lapsed: the pass took it and is doing it)
    const was = Exec.EXEC.remakeline; let turns = 0; Exec.EXEC.remakeline = async () => { turns++; return { done: { ok: true, text: 'Done' } }; };
    let r; try { r = await Unit.nowRun({ db: F.db, Lib: { call: (b, o) => { F.clock.advance(1000); return lib.call(b, o); } }, runId: R, id, key, maxMs: 1, say: () => {} }); } finally { Exec.EXEC.remakeline = was; }
    ok('2d · its own process, begun late, leaves a request the pass already does (one process does it)', !!(r && r.skipped) && turns === 0 && (F.doc(HOLD + id) || {}).by === 'pass', { r, turns, h: F.doc(HOLD + id) });
  });
  await part('2e', async () => {
    seed(); const id = 'c2e', key = 'k2e0000000000000000000000000000a'; putCmd(id);
    F.seed(HOLD + id, { runId: R, key, state: 'running', nonce: key, beatAt: T0, by: 'now:' + key.slice(0, 8), at: T0 });
    const said = [], was = Exec.EXEC.remakeline; Exec.EXEC.remakeline = async () => { throw Object.assign(new Error('the unit ran past its time'), { overBudget: true }); };
    let r; try { r = await Unit.commandsUnit(nowCtx(id, key, { say: t => said.push(String(t)) })); } finally { Exec.EXEC.remakeline = was; }
    ok('2e · a request that ran past the unit\'s time is said, with its id and where it was (never silent)', !!(r && r.more) && said.some(t => t.includes(id) && /past its time/.test(t)), { r, said });
  });

  /* ═══ 3 · STEPSAID / TRUEDONE ═══ */
  const KX = '4232000003_5302000003', PX = KX + '_1', SX = 'ss-rt-x', OX = '4232000003';
  const seedX = () => { seed();
    F.seed('Charm_Nest_Runs/' + R, Object.assign(runRec(), { lines: { [KX]: { orderId: OX, state: 'pooled', sku: 'OLD_SKU', poolIds: [PX] } } }));
    F.seed('Charm_Nest_Sheets/' + SX, { id: SX, runId: R, metal: 'silver', day: DAY, page: 3, draft: true, fileBase: 'SS_working_' + SX, archived: false, layoutRev: 3, contentRev: 4, poolIds: [PX], orders: [OX],
      charms: [{ id: 'cx', poolId: PX, order: OX, sku: 'OLD_SKU', hash: 'hx', name: 'x', areaPt2: 300, widthPt: 20, heightPt: 20 }], placements: [{ id: 'cx', cxPt: 30, cyPt: 30, angle: 0, wPt: 20, hPt: 20, scale: 1 }] });
    F.seed('Charm_Pool/' + PX, { poolId: PX, orderId: OX, lineKey: KX, sku: 'OLD_SKU', state: 'ready', sheetId: SX, material: 'silver', runId: R, quantity: 1 }); };
  const stub = (seat, placed) => {
    const keep = { pool: Intake.pool, seat: SeatNow.seat, placeAll: PlaceUnit.placeAll };
    Intake.pool = async (c, o) => { for (const k of o.only) { const id = k + '_1', r = await c.call({ op: 'poolPut', pools: [{ poolId: id, orderId: OX, lineKey: k, transactionId: '5302000003', sku: 'NEW_SKU', copy: 1, quantity: 1, material: 'silver', runId: R, areaPt2: 300, widthPt: 20, heightPt: 20 }] }, 'pool|' + k); if (r && r.status < 400) c.run.lines[k] = Object.assign({}, c.run.lines[k], { state: 'pooled', poolIds: [id] }); } return {}; };
    SeatNow.seat = async () => seat;
    PlaceUnit.placeAll = async () => { if (placed) F.seed('Charm_Nest_Sheets/ss-rt-d4', { id: 'ss-rt-d4', runId: R, metal: 'silver', page: 4, draft: true, fileBase: 'SS_working_ss-rt-d4', poolIds: [PX], archived: false }); return placed ? { cursor: { placeWait: null } } : { more: true, placed: 0 }; };
    return () => { Intake.pool = keep.pool; SeatNow.seat = keep.seat; PlaceUnit.placeAll = keep.placeAll; };
  };
  /** a fake cloud call: records every body, answers each op as the server would for this story (a take-off swap leaves the sheet without the line) */
  const fakeCtx = calls => { const call = async (b, u) => { calls.push(Object.assign({ unit: u }, b));
      if (b.op === 'cmdSwapLine') { if (!(b.pools || []).length) { const s = sheetDoc(b.sheetId); F.seed('Charm_Nest_Sheets/' + b.sheetId, Object.assign(s, { poolIds: (s.poolIds || []).filter(id => !String(id).startsWith(b.lineKey + '_')) })); return { ok: true, status: 200, seated: 0, off: 1, layoutRev: 4 }; } return { ok: true, status: 200, seated: (b.poses || []).length, here: true, off: 1, layoutRev: 4 }; }
      if (b.op === 'poolPut') return { ok: true, status: 200, placed: [] };
      return { ok: true, status: 200 }; };
    return { db: F.db, run: runRec(), runId: R, call, callNow: call, read: async b => (b.op === 'runGet' ? { run: runRec() } : {}), cloudSteps: ['nest', 'pool', 'files'], say: () => {}, fence, budgetEnd: Date.now() + 60000 }; };
  await part('3a', async () => {
    seedX(); const calls = [], undo = stub({ why: 'no free room on the sheet' }, false); let r;
    try { r = await Exec.EXEC.remakeline({ ctx: fakeCtx(calls), cmd: { id: 'c3a', at: T0, by: 'Michelle', step: 'Started' }, a: { ids: [KX], label: 'Change SKU' }, work: {} }); } finally { undo(); }
    const steps = calls.filter(b => b.op === 'runCmdStep').map(b => b.text), name = Exec.nameOf(sheetDoc(SX));
    ok('3a · it says each stage as it reaches it: making the new pieces, seating on its sheet, no room there so the next Draft', steps.length === 3 && /^Making the new pieces of order 4232000003/.test(steps[0]) && steps[1] === `Seating on ${name}…` && steps[2] === `No room on ${name}: placing on the next Draft…`, steps);
    ok('3a · one write a stage, each said before the request\'s next write', calls.findIndex(b => b.op === 'runCmdStep') < calls.findIndex(b => b.op === 'poolPut'), calls.map(b => b.op));
    const text = (r && r.done && r.done.text) || '';
    ok('3a · the placing placed nothing: the answer never says a piece was placed, it says the new piece waits and why', r && r.done && r.done.ok === true && !/piece\(s\)|placed on/.test(text) && /4232000003: 1 new piece waits to be placed \(the placing goes on in its next turn\)/.test(text), text);
  });
  await part('3b', async () => {
    seedX(); const calls = [], undo = stub({ why: 'no free room on the sheet' }, true); let r;
    try { r = await Exec.EXEC.remakeline({ ctx: fakeCtx(calls), cmd: { id: 'c3b', at: T0, by: 'Michelle', step: 'Started' }, a: { ids: [KX] }, work: {} }); } finally { undo(); }
    const text = (r && r.done && r.done.text) || '', d4 = Exec.nameOf(sheetDoc('ss-rt-d4'));
    ok('3b · placed on the next Draft: the answer says exactly where, and that it is being nested', /1 order line\(s\) made again: 1 piece placed on /.test(text) && text.includes(d4) && /being nested/.test(text) && !/wait/.test(text), text);
  });
  await part('3c', async () => {
    seedX(); const calls = [], undo = stub({ poses: [{ poolId: PX, cxPt: 30, cyPt: 30, angle: 0 }] }, false); let r;
    try { r = await Exec.EXEC.remakeline({ ctx: fakeCtx(calls), cmd: { id: 'c3c', at: T0, by: 'Michelle', step: 'Started' }, a: { ids: [KX] }, work: {} }); } finally { undo(); }
    const steps = calls.filter(b => b.op === 'runCmdStep').map(b => b.text), name = Exec.nameOf(sheetDoc(SX)), sw = calls.find(b => b.op === 'cmdSwapLine' && (b.pools || []).length);
    ok('3c · in its own place: making, seating on its sheet, writing its sheet', steps.length === 3 && steps[1] === `Seating on ${name}…` && steps[2] === `Writing ${name}…`, steps);
    ok('3c · the seated swap says the layout and the content it was seated on (layoutBase, contentBase)', !!sw && sw.layoutBase === 3 && sw.contentBase === 4, sw && { layoutBase: sw.layoutBase, contentBase: sw.contentBase });
    ok('3c · its answer: the piece placed on its own sheet', /1 order line\(s\) made again: 1 piece placed on /.test((r && r.done && r.done.text) || '') && r.done.text.includes(name), r && r.done);
  });

  /* ═══ 4 · BUSYFLAGS ═══ */
  await part('4', async () => {
    seed();
    const s = sheetDoc(S1), at = T0 - 5000;
    F.seed('Charm_Nest_Sheets/' + S1, Object.assign(s, { nestJob: { id: 'nest-rt', status: 'running', at, attempt: 2, slot: 0 }, filesOwed: T0 - 3000, nestOwed: { at: T0 - 4000, n: 1, by: 'cloud', why: 'made again in place' }, refit: { at, jobId: 'nest-rt', pieces: [{ poolId: P2, sku: 'NEW_SKU', order: O2 }] }, saving: false }));
    F.seed('Charm_Nest_Sheets/gf-rt-old', Object.assign({}, s, { id: 'gf-rt-old', setId: null, draft: true, nestJob: { id: 'nest-old', status: 'done', at: T0 - 7200e3 }, filesOwed: null, nestOwed: T0 - 7200e3, refit: { at: T0 - 7200e3, jobId: 'nest-old', pieces: [] }, previewAt: T0 - 7000e3 }));
    const r = await P({ op: 'laserStatus', sheetIds: [S1, 'gf-rt-old'] }), a = (r.sheets || []).find(x => x.id === S1) || {}, b = (r.sheets || []).find(x => x.id === 'gf-rt-old') || {};
    ok('4 · a sheet being nested again: nestJob { status, at } only, filesOwed and nestOwed in ms, refit true', JSON.stringify(a.nestJob) === JSON.stringify({ status: 'running', at }) && a.filesOwed === T0 - 3000 && a.nestOwed === T0 - 4000 && a.refit === true, { nestJob: a.nestJob, filesOwed: a.filesOwed, nestOwed: a.nestOwed, refit: a.refit });
    ok('4 · a sheet whose refit is long done: refit false, nothing owed is null, a bare time is kept', b.refit === false && b.filesOwed === null && b.nestOwed === T0 - 7200e3 && JSON.stringify(b.nestJob) === JSON.stringify({ status: 'done', at: T0 - 7200e3 }), { nestJob: b.nestJob, filesOwed: b.filesOwed, nestOwed: b.nestOwed, refit: b.refit });
    ok('4 · beside saving, status, layoutRev (unchanged)', a.saving === false && a.status === 'complete' && a.layoutRev === 5, { saving: a.saving, status: a.status, layoutRev: a.layoutRev });
  });

  ok('no network beyond the fakes, no Etsy', !F.net.other.length && !F.net.tripwire.length && !F.etsy.calls, { other: F.net.other, trip: F.net.tripwire });
  log0(`${failed ? 'FAIL' : 'PASS'} rt1127-server (${n - failed} of ${n})${DIR !== F.fnDir ? ' · code of ' + DIR : ''}`); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL rt1127-server: ' + ((e && e.stack) || e)); process.exit(1); });
function num(v) { return Number.isFinite(+v) ? +v : 0; }
