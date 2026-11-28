/* LEFTOVERNOROOM (Paul, 27 Nov 2026, RG Draft 1 of Nov 26: "Why is this RG sheet preview not visible"): its 22 pieces stood on a Rose Gold leftover 16 pt wide (97% of the physical sheet cut
   already) that holds no piece: the nest ended "no-room", the writer had nothing to write (no files, no preview, "Updating…" for good) and the nest was never asked again. Offline, over
   _cloudFakes.cjs (the real library handler, its Rose stock, the real nest step; no network, no Firebase):
     F  the room check skips a leftover that holds no piece of the sheet, whatever its orders (a single-piece order too); one with room is taken
     C  a leftover only NAMED as a hint (RGLINK) with no room, or one the sheet found without room before (skip), is not taken: the automatic choice is (a leftover with room, else a new
        sheet); a hint with room is taken as before
     N  the nest step: a Rose Gold draft with no piece placed whose nest ended "no-room" on its leftover gives it back, remembers it (greenNoRoom), claims another plate and is nested
        again; a draft with pieces placed, or on a NEW plate, is left as it was
   node tests/charm-nest/rt1127-noroom.cjs */
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const path = require('path');
const T0 = Date.UTC(2026, 10, 27, 20, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const DIR = process.env.RT1127_FN ? path.resolve(process.env.RT1127_FN) : F.fnDir, Ld = f => F.load(f, { dir: DIR });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {}; console.error = () => {};
let n = 0, failed = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 700))); if (!c) failed++; };
const part = async (name, fn) => { try { await fn(); } catch (e) { ok(name + ' ran to its end', false, String((e && e.stack) || e).slice(0, 900)); } };
const lib = Ld('charmNestLibrary.js'), Step = Ld('_charmNestNestStep.js'), RS = require(path.join(DIR, '_charmNestRoseStock.js'));
const R = 'run-2026-11-27-nr', LEASE = 'Charm_Nest_Drive/lease', fence = { path: LEASE, nonce: 'dn-nr', runId: R };
const call = (b, u) => lib.call(b, { unit: String(u || 'u|' + Math.random()), fence });
const W = 283.46456692913387, H = 141.73228346456693;
// a physical sheet cut along its left side: `left` pt of metal remain at the right edge
const prof = left => JSON.stringify({ version: 1, wPt: W, hPt: H, axis: 'x', step: 0.5, values: Array(Math.ceil(H / 0.5)).fill(+(W - left).toFixed(3)) });
const FIT = { unit: 1, areaMm2: 61, minMm: 9.26, maxMm: 11.96 };   // (RG Draft 1's pieces: 12 orders, the smallest of one piece)
function seed() {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: ['pull', 'pool', 'nest', 'files', 'labels'], flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', day: '2026-11-27', lines: {}, errors: [] });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  F.seed(LEASE, { nonce: fence.nonce, state: 'running', heartbeat: true, startedAt: T0, beatAt: T0, at: T0 });
  F.seed('Charm_Nest_Rose_Stock/rgs-strip', { id: 'rgs-strip', metal: 'rose', revision: 3, owner: null, available: true, wPt: W, hPt: H, profileJson: prof(16), lastCutAt: T0 - 1000, lastCutSheetId: 'rg-old' });
  F.seed('Charm_Nest_Rose_Stock/rgs-roomy', { id: 'rgs-roomy', metal: 'rose', revision: 1, owner: null, available: true, wPt: W, hPt: H, profileJson: prof(150), lastCutAt: T0 - 9e6, lastCutSheetId: 'rg-older' });
}
const charm = (id, order) => ({ id, poolId: order + '_t' + id + '_1', order, sku: 'LIGHTNING_6631', metal: 'rose', areaPt2: 139, widthPt: 33.8, heightPt: 14.3, hash: 'h' + id });
function seedDraft(o) {
  const rec = Object.assign({ id: 'rg-d1', runId: R, metal: 'rose', page: 10, draft: true, setId: null, archived: false, status: 'ready', charms: [charm('c1', '4248334945'), charm('c2', '4250475438')],
    placements: [], poolIds: ['4248334945_tc1_1', '4250475438_tc2_1'], orders: ['4248334945', '4250475438'], layoutRev: 13, contentRev: 13, nestOwed: T0 - 60e3, roseStockId: 'rgs-strip', roseRevision: 3,
    greenLeftover: { stockId: 'rgs-strip', revision: 3, partialId: 'rgs-strip-3', from: 'rg-old' }, nestJob: { id: 'nest-old', status: 'nested', at: T0 - 50e3 } }, o || {});
  F.seed('Charm_Nest_Sheets/' + rec.id, rec);
  if (rec.roseStockId) F.seed('Charm_Nest_Rose_Stock/' + rec.roseStockId, Object.assign({}, F.doc('Charm_Nest_Rose_Stock/' + rec.roseStockId), { owner: rec.id, available: false }));
  F.seed('Charm_Nest_Jobs/nest-old', { id: 'nest-old', kind: 'nest', cloud: true, sheetId: rec.id, runId: R, metal: 'rose', lane: R + '~rose', status: 'done', state: 'done', endedBy: 'no-room', layoutBase: 13, contentBase: 13, roseStockId: rec.roseStockId || 'rgs-new', auto: true, createdMs: T0 - 60e3 });
  return rec;
}

(async () => {
  await part('F', async () => {
    const strip = { profileJson: prof(16) }, roomy = { profileJson: prof(150) };
    ok('F1 a leftover 16 pt wide holds none of RG Draft 1\'s pieces: skipped, though its smallest order is one piece', RS.fitCheck(FIT)(strip) === false);
    ok('F2 a leftover with room is taken; no hint at all changes nothing', RS.fitCheck(FIT)(roomy) === true && RS.fitCheck(null)(strip) === true);
    seed(); const S = await require(path.join(DIR, '_charmNestNest.js')).readSettings(F.db), h = Step.fitHint({ charms: [charm('a', '1'), charm('b', '2'), charm('c', '2')] }, S);
    ok('F3 the nest asks for room for a single-piece order too (fitHint: unit 1)', !!(h && h.fit && h.fit.unit === 1 && h.fit.areaMm2 > 0), h);
  });

  await part('C', async () => {
    seed(); F.seed('Charm_Nest_Sheets/rg-sa', { id: 'rg-sa', runId: R, metal: 'rose', draft: true, charms: [], placements: [], layoutRev: 1, contentRev: 1 });
    const r1 = await call({ op: 'roseClaim', sheetId: 'rg-sa', metal: 'rose', wPt: W, hPt: H, nesting: true, stockId: 'rgs-strip', fit: FIT });
    const sA = F.doc('Charm_Nest_Sheets/rg-sa') || {};
    ok('C1 the hinted leftover with no room is not taken: the automatic choice takes the one with room', !!r1.stock && r1.stock.id === 'rgs-roomy' && sA.roseStockId === 'rgs-roomy' && !(F.doc('Charm_Nest_Rose_Stock/rgs-strip') || {}).owner, { r1: r1.stock && r1.stock.id, sheet: sA.roseStockId, err: r1.error });
    seed(); F.seed('Charm_Nest_Sheets/rg-sb', { id: 'rg-sb', runId: R, metal: 'rose', draft: true, charms: [], placements: [], layoutRev: 1, contentRev: 1 });
    const r2 = await call({ op: 'roseClaim', sheetId: 'rg-sb', metal: 'rose', wPt: W, hPt: H, nesting: true, stockId: 'rgs-roomy', fit: FIT });
    ok('C2 a hinted leftover with room is taken as before', !!r2.stock && r2.stock.id === 'rgs-roomy', { stock: r2.stock && r2.stock.id, err: r2.error });
    seed(); F.seed('Charm_Nest_Sheets/rg-sc', { id: 'rg-sc', runId: R, metal: 'rose', draft: true, charms: [], placements: [], layoutRev: 1, contentRev: 1 });
    const r3 = await call({ op: 'roseClaim', sheetId: 'rg-sc', metal: 'rose', wPt: W, hPt: H, nesting: true, stockId: 'rgs-roomy', fit: FIT, skip: ['rgs-roomy'] });
    ok('C3 a leftover this sheet found without room before (skip) is never taken again: a new sheet is', !!r3.stock && !['rgs-roomy', 'rgs-strip'].includes(r3.stock.id) && !(+r3.stock.revision > 0), { stock: r3.stock && r3.stock.id, err: r3.error });
  });

  await part('N', async () => {
    seed(); seedDraft();
    const asks = []; const ctx = { db: F.db, run: Object.assign({ runId: R }, F.doc('Charm_Nest_Runs/' + R)), runId: R, budgetEnd: Date.now() + 60000, cursor: {}, say: () => {},
      call: async (b, u) => { asks.push(b.op); return call(b, u); } };
    await Step.nestUnit(ctx);
    const rec = F.doc('Charm_Nest_Sheets/rg-d1') || {}, strip = F.doc('Charm_Nest_Rose_Stock/rgs-strip') || {};
    ok('N1 the leftover with no room is given back (roseRelease) and remembered on the draft (greenNoRoom), its hint forgotten', asks.includes('roseRelease') && strip.owner == null && strip.available === true && (rec.greenNoRoom || []).includes('rgs-strip') && rec.greenLeftover == null, { asks, strip: { owner: strip.owner, available: strip.available }, mark: rec.greenNoRoom, left: rec.greenLeftover });
    ok('N2 another plate is claimed: the leftover with room, never the strip', rec.roseStockId === 'rgs-roomy', { stock: rec.roseStockId });
    ok('N3 and its nest is asked again on it at once (nestStart)', asks.includes('nestStart'), asks);
    // a draft with pieces placed keeps its plate
    seed(); seedDraft({ placements: [{ id: 'c1', cxPt: 275, cyPt: 20, angle: 0, scale: 1 }] });
    const asks2 = []; await Step.nestUnit({ db: F.db, run: Object.assign({ runId: R }, F.doc('Charm_Nest_Runs/' + R)), runId: R, budgetEnd: Date.now() + 60000, cursor: {}, say: () => {}, call: async (b, u) => { asks2.push(b.op); return call(b, u); } });
    ok('N4 a draft with a piece placed keeps its leftover (what does not fit goes on, as always)', !asks2.includes('roseRelease') && (F.doc('Charm_Nest_Sheets/rg-d1') || {}).roseStockId === 'rgs-strip', asks2);
    // a draft on a NEW plate (no cut on it yet) is never given back here
    seed(); F.seed('Charm_Nest_Rose_Stock/rgs-new', { id: 'rgs-new', metal: 'rose', revision: 0, owner: null, available: false, wPt: W, hPt: H, profileJson: null });
    seedDraft({ roseStockId: 'rgs-new', roseRevision: 0, greenLeftover: null });
    const asks3 = []; await Step.nestUnit({ db: F.db, run: Object.assign({ runId: R }, F.doc('Charm_Nest_Runs/' + R)), runId: R, budgetEnd: Date.now() + 60000, cursor: {}, say: () => {}, call: async (b, u) => { asks3.push(b.op); return call(b, u); } });
    ok('N5 a draft whose NEW plate had no room is not given back (an order too big for a whole sheet is the writer\'s problem, never a loop)', !asks3.includes('roseRelease') && (F.doc('Charm_Nest_Sheets/rg-d1') || {}).roseStockId === 'rgs-new', asks3);
  });

  log0(`rt1127-noroom: ${n - failed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
})();
