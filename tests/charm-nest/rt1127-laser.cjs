/* SEALFACES (Paul, 27 Nov 2026, Set-1 · 2026-11-24): "Why am I getting this problem when I press approve for laser cutting? All of the back engravings have been approved". Node only:
   node tests/charm-nest/rt1127-laser.cjs. The lines, seals and file records are those of GF_Nov.24.26_Set-1_Sheet-1 as the cloud held them (no names, no links).
   L1 a front-only piece whose front file is saved (4244028423 "Waldo") is ready: no back file is asked of it
   L2 a front-only piece whose front was approved twice and its file saved (4245556382: a combined seal at the time of the first front seal) is ready
   L3 a front-only piece with NO front file (4238855149) still holds the set, and is said as its FRONT file, never a back
   L4 a piece whose back was approved (a seal of its own, not at a front seal's time) still needs its back file
   L5 a piece still waiting for approval is never let through by the seals */
'use strict';
const path = require('path'); const R = require(path.join(__dirname, '..', '..', 'charm-nest-readiness.js'));
let passed = 0, failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 600))); c ? passed++ : failed++; };
const SID = 'd-2c660d6bd1c6bd63', fseal = (at, by) => ({ id: `engraveApproved:front:${at}:${by}`, how: 'engraveApproved', at, by, face: 'front' }), bseal = (at, by) => ({ id: `engraveApproved:${at}:${by}`, how: 'engraveApproved', at, by });
const line = (key, seals, o) => Object.assign({ key, orderId: key.split('_')[0], state: 'written', poolIds: [key + '_1'], engrave: { needed: true, state: 'written', approved: true, approvedAt: 1, approvedBy: 'M', seals, text: null, pieces: { [key + '_1']: { needed: true, state: 'written', approved: true } } } }, o || {});
const rows = [line('4244028423_5307575879', [fseal(1795491855838, 'Michelle_R')]), line('4245556382_5308149637', [bseal(1795492138887, 'Michelle_R'), fseal(1795492138887, 'Michelle_R'), fseal(1795797643361, 'Ana_M')]), line('4238855149_5300008900', [fseal(1795400496092, 'Michelle_R')]),
  line('4200000001_5300000001', [bseal(1795000000000, 'Ana_M')]), line('4200000002_5300000002', [fseal(1795000000001, 'Ana_M')], { state: 'pooled' })];
rows[4].engrave = Object.assign({}, rows[4].engrave, { approved: false, state: 'review', pieces: { '4200000002_5300000002_1': { needed: true, state: 'review', approved: false } } });
const file = (poolId, face, extra) => Object.assign({ poolId, face, sheetId: SID, approvedAt: 5, approvedBy: 'M', invalidated: false, verified: { file: { ok: true }, geometry: { ok: true } }, outputs: { ai: { path: 'charmnest/x/' + poolId + '.ai', url: 'https://firebasestorage.googleapis.com/v0/b/b/o/x?alt=media&token=t' } } }, extra || {});
const s = { id: SID, status: 'complete', placedCount: 5, poolIds: rows.map(r => r.poolIds[0]), charms: rows.map(r => ({ id: 'c:' + r.poolIds[0], poolId: r.poolIds[0] })), placements: rows.map(r => ({ id: 'c:' + r.poolIds[0] })),
  backPool: [file('4244028423_5307575879_1', 'front'), file('4245556382_5308149637_1', 'front', { artOnly: true })], engraving: R.decisions(rows), verification: { ok: true }, outputs: { ai: { path: 'a', url: 'https://x/a' } }, preview: 'https://x/p' };
const flat = x => { const out = []; const walk = v => { if (!v || typeof v !== 'object') return; if (Array.isArray(v)) return v.forEach(walk); if (typeof v.why === 'string' && v.id != null) out.push(v); for (const k of Object.keys(v)) if (k !== 'why') walk(v[k]); }; walk(x); return out; };
const why = id => { const e = R.explain(s, { kind: 'sheet' }); return [...new Set(flat(e).filter(x => String(x.id).includes(id)).map(x => x.why))]; };
if (process.env.DUMP) console.log(JSON.stringify(R.explain(s, { kind: 'sheet' })).slice(0, 3000));
ok('L1 4244028423: front-only, its front file saved: nothing asked of it (no back file)', why('4244028423').length === 0, why('4244028423'));
ok('L2 4245556382: a combined seal at a front seal\'s time is not a back: ready', why('4245556382').length === 0, why('4245556382'));
ok('L3 4238855149: no front file: it still holds the set, said as its front file', why('4238855149').length === 1 && /front file/.test(why('4238855149')[0]) && !/back/.test(why('4238855149')[0]), why('4238855149'));
ok('L4 a piece with its own back seal still needs its back file', why('4200000001').some(w => /back file/.test(w)), why('4200000001'));
ok('L5 a piece still in review is never let through', why('4200000002').length === 1 && !/file/.test(why('4200000002')[0]), why('4200000002'));
console.log(`rt1127-laser: ${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
