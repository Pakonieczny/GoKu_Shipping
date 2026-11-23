// RGEMPTY (Paul, 23 Nov 2026: "Why did this rose gold draft not get used further and now it started another draft 2"). At 00:07 the cloud opened RG Draft 2 (d-d681ad19122dae19) on a FRESH
// plate for order 4244381926's three Rose Gold pieces while RG Draft 1 (d-bfaa108fb9d28aa7) had room (RGFILL fixes the choice). The repair is a person's "Empty" of a Rose Gold draft onto the Rose
// Gold draft named: the pieces go onto RG Draft 1 (nested around what it holds), RG Draft 2 is removed and its plate given back. 10K / 14K "Empty" is unchanged.
// Offline: the real driver pass, the requests unit and the library handler (cmdMovePieces, roseRelease, archiveEmptySheet, nestStart) on the in-memory cloud of _cloudFakes.cjs, seeded from the
// live records (fixtures/rg-empty.json, read only). No network, no Etsy.
//   1  the request: RG Draft 2's three pieces listed on RG Draft 1 (its own 18 untouched), their records naming it; RG Draft 2 archived; its fresh plate free again (no owner, available);
//      RG Draft 1's nest asked (its placed pieces held: append)
//   2  refused in words, nothing changed: no target named, a target that is cut, in a set or not Rose Gold
//   node tests/charm-nest/rg-empty.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 23, 5, 40, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {}; console.error = () => {};
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1200))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Unit = F.load('_charmNestCmdUnit.js');
const FX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'rg-empty.json'), 'utf8'));
const R = FX.runId, SRC = FX.src.id, TGT = FX.tgt.id, SRC_STOCK = FX.src.roseStockId, TGT_STOCK = FX.tgt.roseStockId, DEV = 'd0paulk0001', CV = 'cloud14-20261123';
const clone = o => JSON.parse(JSON.stringify(o));
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({ status: r.statusCode }, o); };
const sheet = id => F.doc('Charm_Nest_Sheets/' + id), stock = id => F.doc('Charm_Nest_Rose_Stock/' + id), pool = id => F.doc('Charm_Pool/' + id);
const active = r => ((r && r.charms) || []).filter(c => c && !c.excluded);
function seed(edit) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: FX.cloudSteps.filter(s => s === 'nest' || /^cmd:/.test(s)), flippedAt: T0 - 86400e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, Object.assign(clone(FX.run), { runId: R }));
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 86400e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 86400e3 } });
  F.seed('Charm_Nest_Sheets/' + SRC, clone(FX.src)); F.seed('Charm_Nest_Sheets/' + TGT, clone(FX.tgt));
  for (const [id, s] of Object.entries(FX.stocks)) F.seed('Charm_Nest_Rose_Stock/' + id, clone(s));
  for (const [id, row] of Object.entries(FX.pool)) F.seed('Charm_Pool/' + id, clone(row));
  if (edit) edit();
}
async function pass() {
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}
const ask = target => P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Paul_K', cv: CV, cmd: Object.assign({ type: 'empty', sheetId: SRC }, target ? { targetId: target } : {}) });
const cmdOf = id => (((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {})[id]) || {};
const state = () => JSON.stringify([sheet(SRC), sheet(TGT), stock(SRC_STOCK), stock(TGT_STOCK)]);

(async () => {
  const srcPieces = active(FX.src).map(c => String(c.poolId)), tgtHad = active(FX.tgt).map(c => String(c.id));
  ok('0 · the fixture is the live state: RG Draft 2 holds order 4244381926\'s 3 Rose Gold pieces on a fresh plate it owns; RG Draft 1 holds 18', srcPieces.length === 3 && srcPieces.every(id => id.startsWith('4244381926_')) && FX.stocks[SRC_STOCK].owner === SRC && !(+FX.stocks[SRC_STOCK].revision) && tgtHad.length === 18);
  /* 1 · the request */
  seed();
  const r = await ask(TGT); await pass();
  const c = cmdOf(r.id), t = sheet(TGT), s = sheet(SRC), st = stock(SRC_STOCK);
  ok('1 · the request is taken and the cloud carries it out (moved, Draft 1\'s nest asked)', r.ok && ['running', 'waiting', 'done'].includes(c.state) && /Nesting|moved onto/.test(String(c.text || c.step || '')), c);
  ok('1 · RG Draft 2\'s three pieces are listed on RG Draft 1, their records naming it; RG Draft 1\'s own 18 are untouched (each still there and placed)',
    srcPieces.every(id => (t.poolIds || []).map(String).includes(id) && pool(id).sheetId === TGT) && tgtHad.every(id => active(t).some(x => String(x.id) === id) && (t.placements || []).some(p => String(p.id) === id)), { tPool: (t.poolIds || []).length, rows: srcPieces.map(id => pool(id).sheetId) });
  ok('1 · RG Draft 2 is empty and removed (archived)', !!s.archived && !active(s).length, { archived: s.archived, n: active(s).length });
  ok('1 · its fresh plate is free again: no owner, available', st && st.owner == null && st.available === true, st);
  ok('1 · RG Draft 1 owes its nest, holding its placed pieces where they are (append)', !!(t.nestJob || t.nestOwed) && t.appendOnly !== false, { nestJob: t.nestJob, nestOwed: t.nestOwed, appendOnly: t.appendOnly });
  /* 2 · refusals */
  for (const [why, edit, target, re] of [
    ['no target named', null, null, /Name the Rose Gold draft/],
    ['the target is cut', () => { const x = sheet(TGT); x.roseCutAt = T0 - 60e3; F.seed('Charm_Nest_Sheets/' + TGT, x); }, TGT, /already cut/],
    ['the target is in a set', () => { const x = sheet(TGT); x.setId = 'set-x'; x.draft = false; F.seed('Charm_Nest_Sheets/' + TGT, x); }, TGT, /in a set/]]) {
    seed(edit); const was = state(); const q = await ask(target); await pass(); const cc = cmdOf(q.id);
    ok(`2 · ${why}: refused in words, nothing changed`, q.ok && cc.state === 'failed' && re.test(String(cc.text || '')) && state() === was, cc);
  }
  log0(failed ? `FAIL rg-empty (${failed})` : 'PASS rg-empty'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL rg-empty: ' + (e && e.stack || e)); process.exit(1); });
