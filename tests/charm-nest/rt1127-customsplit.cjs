/* CUSTOMSPLIT (Paul, 27 Nov 2026: "I don't wanna change any rules, but I also don't wanna see these notices if it happens because of the custom order ... don't block anything because of this"). Node only:
   node tests/charm-nest/rt1127-customsplit.cjs. The real set rules and checker (charm-nest-set-rules.js, charm-nest-shared-orders.js, charm-nest-set-invariants.js).
   C1 a completed sheet outside the set tied to it only by a CUSTOM order's piece: no rule break, no partner to wait for
   C2 the same with a regular order (a gold pair outside, a silver pair inside): still a rule break, as before
   C3 the two-sheets-of-one-colour warning is not raised for a custom order, and still is for a regular one
   C4 the Library's own "waits for its partner" leaves a custom order out */
'use strict';
const path = require('path'), fs = require('fs'), ROOT = path.join(__dirname, '..', '..');
global.window = global.window || global;
const SO = require(path.join(ROOT, 'charm-nest-shared-orders.js')), SR = require(path.join(ROOT, 'charm-nest-set-rules.js'));
global.SharedOrders = SO; global.CharmNestSetRules = SR; window.SharedOrders = SO; window.CharmNestSetRules = SR;
const I = require(path.join(ROOT, 'charm-nest-set-invariants.js'));
let passed = 0, failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 500))); c ? passed++ : failed++; };
const RUN = 'run-x', sheet = (id, metal, poolIds, o) => Object.assign({ id, sheetId: id, metal, runId: RUN, status: 'complete', density: 0.73, placedCount: poolIds.length, charmCount: poolIds.length, poolIds, outputs: { ai: 'a' }, preview: 'p', releasedAt: 1, persistedDone: true, verification: { ok: true }, sources: [] }, o || {});
const filler = (p, n) => Array.from({ length: n }, (_, i) => `${p}${i}_1_1`);
const custom = oid => ({ id: `pool:charmnest_custom_${oid}_abcdef123456_${oid}_pdf`, name: 'X (master)', path: `charmnest/custom/${oid}/abcdef123456-${oid}.pdf`, custom: true, pool: true });
const violations = (sets, sheets) => { const r = I.check(sets, sheets, {}); return { v: (r.violations || r.items || []).filter(x => x && /cardinal|partialPartner/.test(x.rule || '')), w: (r.warnings || []).filter(x => x.rule === 'orderSplit'), r }; };
// set 1: GF Sheet 1 + SS Sheet 1. Outside: GF Sheet 2 (completed, in no set)
const run = (outsideSources, outsideOrder) => {
  const gf1 = sheet('gf1', 'gold', filler('41000', 30).concat(['4247082476_5309514130_1'])), ss1 = sheet('ss1', 'silver', filler('42000', 30).concat(['4249409991_5314532688_1']));
  const gf2 = sheet('gf2', 'gold', filler('43000', 30).concat([outsideOrder]), { sources: outsideSources });
  return violations([{ setId: 'set-1', seq: 1, sheetIds: ['gf1', 'ss1'] }], [gf1, ss1, gf2]);
};
const c1 = run([custom('4247082476')], '4247082476_5309514132_1');
ok('C1 tied only by a custom order\'s piece (its design file names the order): no rule break for set 1', c1.v.length === 0, c1.v.map(x => x.message || x.line));
const c2 = run([], '4249409991_5314532684_1');
ok('C2 tied by a regular order (its gold pair outside, its silver pair inside): still a rule break, as before', c2.v.length > 0, c2.r && Object.keys(c2.r));
// two GF sheets holding one order: custom vs regular
const w = (src, oid) => { const a = sheet('gfa', 'gold', filler('44000', 30).concat([`${oid}_1_1`]), { sources: src }), b = sheet('gfb', 'gold', filler('45000', 30).concat([`${oid}_2_1`])); return violations([{ setId: 'set-2', seq: 2, sheetIds: ['gfa', 'gfb'] }], [a, b]).w; };
ok('C3 one order on two GF sheets: no warning for a custom order, still one for a regular order', w([custom('4246217565')], '4246217565').length === 0 && w([], '4250000001').length > 0, { custom: w([custom('4246217565')], '4246217565').length, regular: w([], '4250000001').length });
const br = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
ok('C4 the Library\'s partner wait leaves out an order tied by a custom piece (the set\'s rows, the Library\'s records, the pages)', /items = items\.filter\(it => !\(it && it\.orderId && g\.sheets\.map\(r => r\.id\)\.concat\(it\.thereIds \|\| \[\]\)\.some\(i => SI\.customOrderOn\(recAny\(i\), it\.orderId\)\)\)\)/.test(br));
console.log(`rt1127-customsplit: ${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
