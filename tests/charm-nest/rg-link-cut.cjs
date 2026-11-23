// RGLINK, the cloud's part (Paul, 23 Nov 2026: "another multi piece mixed order arrives to the GF draft sheet and forces the Green Cut line to occure so as to not connect 2 GF or SS sheets
// to a single RG sheet" · "It's much more challenging to cut the green line after parts have been nested"). The placement marks the Rose Gold draft an order would tie to a second GF / SS
// sheet (roseLinkCut; rg-fill-open.cjs); the green line's link cut (_charmNestGreenLine.js linkUnit, pass item "greenLinks" of lane "rosecut") draws its line around what it holds and records
// its cut AT ONCE, in no set yet; the next draft claims the leftover of that same plate (_charmNestNestStep.js claimStock).
// Offline: the real driver pass and the library handler (rosePlan, roseRecordCut, roseClaim, putSheet) on the in-memory cloud of _cloudFakes.cjs, seeded with RG Draft 1 and its plate as the
// live cloud holds them (fixtures/rg-empty.json, read only). The charm outlines are each placement's rectangle (the designs are not downloaded). No network, no Etsy.
//   1  RG Draft 1 marked: one pass cuts it (its line drawn, its cut recorded on its own plate: revision 1, the plate free again for its leftover), the mark gone; it stays a draft in no set
//   2  a draft being written waits (not cut, mark kept) and is cut the pass after it is written
//   3  a 10K sheet is never cut by it (the server refuses 10K / 14K by the automatic route)
//   4  the next Rose Gold draft, opened on that leftover (greenLeftover), claims THAT plate (revision 1), not a fresh one
//   node tests/charm-nest/rg-link-cut.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 23, 7, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); if (!process.env.RGDEBUG) { console.log = () => {}; console.warn = () => {}; console.error = () => {}; }
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 900))); if (!cond) failed++; };
// the outlines: each placement's own rectangle, turned by its angle (Shapes.forSheet reads the designs from storage, which this test does not hold)
{ const file = path.join(F.fnDir, '_charmNestRoseShapes.js'), real = require(file), Rose = require(path.join(F.fnDir, '../../charm-nest-rose.js'));
  const W = 283.46456692913387, H = 141.73228346456693, clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  const rect = p => { const r = Math.max(2, Math.min(+p.wPt || 10, +p.hPt || 10) * 0.3), cx = clamp(+p.cxPt, r + 1, W - r - 1), cy = clamp(+p.cyPt, r + 1, H - r - 1);
    return [[cx - r, cy - r], [cx + r, cy - r], [cx + r, cy + r], [cx - r, cy + r]]; };
  real.forSheet = async (ctx, rec) => (rec.placements || []).map(p => Rose.slimShape({ id: p.id, paths: [rect(p)], ink: [] })); }
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Green = F.load('_charmNestGreenLine.js'), NestStep = F.load('_charmNestNestStep.js');
const FX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'rg-empty.json'), 'utf8'));
const R = FX.runId, D1 = FX.tgt.id, PLATE = FX.tgt.roseStockId;
const clone = o => JSON.parse(JSON.stringify(o));
const sheet = id => F.doc('Charm_Nest_Sheets/' + id), stock = id => F.doc('Charm_Nest_Rose_Stock/' + id);
const MARK = { at: T0 - 60e3, orderId: '4244381926', by: 'Cloud' };
function seed(edit) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: ['rosecut'], flippedAt: T0 - 86400e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, Object.assign(clone(FX.run), { runId: R }));
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 86400e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 86400e3 } });
  const d1 = clone(FX.tgt); Object.assign(d1, { roseLinkCut: MARK, outputs: { ai: { path: 'charmnest/x/RG_working.ai', url: 'https://example.invalid/x.ai' } }, nestJob: { id: 'nest-x', status: 'nested', at: T0 - 600e3 }, filesJob: null, nestOwed: null });
  F.seed('Charm_Nest_Sheets/' + D1, d1);
  for (const [id, s] of Object.entries(FX.stocks)) if (id === PLATE) F.seed('Charm_Nest_Rose_Stock/' + id, clone(s));
  if (edit) edit();
}
async function pass(items) {
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: items || Green.units.passItems.filter(p => p.name === 'greenLinks'), sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}

(async () => {
  /* 1 · the link cut */
  seed(); await pass();
  let d = sheet(D1), st = stock(PLATE);
  ok('1 · RG Draft 1 marked: its green line is drawn and its cut recorded at once (on its own plate)', !!d.roseCutAt && !!d.rosePlanHash && d.roseStockId === PLATE, { roseCutAt: d.roseCutAt, plan: !!d.rosePlanHash, stock: d.roseStockId });
  ok('1 · the plate is at revision 1, free again for its leftover (no owner, available), its last cut RG Draft 1\'s', st.revision === 1 && st.owner == null && st.available === true && st.lastCutSheetId === D1, { revision: st.revision, owner: st.owner, available: st.available, last: st.lastCutSheetId });
  ok('1 · the mark is gone; the draft is in no set (it joins the set its GF sheet forms)', !d.roseLinkCut && !(d.setId && !d.draft) && d.draft === true, { mark: d.roseLinkCut, setId: d.setId, draft: d.draft });
  /* 2 · being written */
  seed(() => { const x = sheet(D1); x.filesOwed = T0 - 5e3; F.seed('Charm_Nest_Sheets/' + D1, x); }); await pass();
  d = sheet(D1);
  ok('2 · a draft being written waits: not cut, its mark kept', !d.roseCutAt && !!d.roseLinkCut, { roseCutAt: d.roseCutAt, mark: d.roseLinkCut });
  { const x = sheet(D1); x.filesOwed = null; F.seed('Charm_Nest_Sheets/' + D1, x); } F.clock.set(T0 + 120e3); await pass();
  d = sheet(D1);
  ok('2 · once written, the next pass cuts it', !!d.roseCutAt && !d.roseLinkCut, { roseCutAt: d.roseCutAt, mark: d.roseLinkCut });
  /* 3 · 10K never */
  seed(() => { const x = sheet(D1); x.metal = 'gold10k'; F.seed('Charm_Nest_Sheets/' + D1, x); const s = stock(PLATE); s.metal = 'gold10k'; F.seed('Charm_Nest_Rose_Stock/' + PLATE, s); }); await pass();
  d = sheet(D1);
  ok('3 · a 10K sheet is never cut by the link cut', !d.roseCutAt && stock(PLATE).revision === 0, { roseCutAt: d.roseCutAt, revision: stock(PLATE).revision });
  /* 4 · the next draft claims the leftover */
  seed(); await pass();
  const nd = { id: 'd-next-rg', runId: R, metal: 'rose', page: 9, draft: true, setId: null, charms: clone(FX.src.charms), poolIds: clone(FX.src.poolIds || []), placements: [], greenLeftover: { stockId: PLATE, revision: 1, partialId: `${PLATE}-1`, from: D1 }, layoutRev: 1, contentRev: 1 };
  F.seed('Charm_Nest_Sheets/' + nd.id, nd);
  // (another Rose Gold leftover, cut later than RG Draft 1's: the automatic choice (newest leftover) would take it; the draft opened on RG Draft 1's leftover must not)
  F.seed('Charm_Nest_Rose_Stock/rgs-other-left', Object.assign(clone(stock(PLATE)), { id: 'rgs-other-left', revision: 2, owner: null, available: true, lastCutAt: T0 + 200e3, lastCutSheetId: 'rose-other' }));
  const S = { stockPreset: null, settings: { stock: {} } };
  // (the nest step's own claim, as the driver calls it: the cloud's fenced call)
  let err = 'not run'; F.clock.set(T0 + 300e3);
  await pass([{ name: 'claimTest', lane: 'rosecut', unit: async ctx => { err = await NestStep.claimStock(ctx, sheet(nd.id), S, { rose: { mode: 'auto' } }); return undefined; } }]);
  const n2 = sheet(nd.id);
  ok('4 · the next Rose Gold draft, opened on that leftover, claims THAT plate (revision 1), not a fresh one nor a newer leftover of another plate', n2.roseStockId === PLATE && +n2.roseRevision === 1 && stock(PLATE).owner === nd.id, { stock: n2.roseStockId, revision: n2.roseRevision, owner: stock(PLATE).owner, err });
  ok('rule · Etsy was never called', !F.etsy || !(F.etsy.calls || []).length);
  log0(failed ? `FAIL rg-link-cut (${failed})` : 'PASS rg-link-cut'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL rg-link-cut: ' + (e && e.stack || e)); process.exit(1); });
