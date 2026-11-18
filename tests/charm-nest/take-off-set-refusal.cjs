// SETREFUSAL (Paul, 18 Nov 2026: "I was trying to use Remove from sheet for the old fashion camera charm. It started the process but got stuck, this process can not get stuck"): the piece came off and the sheet was written (the cloud held it at 14:29:31 with
// 21 pieces), then the set look refused to put this 25% draft into the old Set 5 ("GF Draft 15 cannot go into Set 5 ... a partial sheet never joins a set (rule R3)"), and that came back as the sheet's own "not saved to the cloud": "Not everything came off".
// A refusal to JOIN A SET is no failure of the removal when the cloud holds the sheet as it now stands. The real function of charm-nest-sheetwin.js, over fixtures: no browser, no network.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../charm-nest-sheetwin.js'), 'utf8');
const a = source.indexOf('  async function savedDespiteSetRefusal('), b = source.indexOf('  async function takeOff(plan, opt, who)');
assert(a > 0 && b > a, 'the helper sits before takeOff');
let fails = 0; const check = (id, ok, ev) => { if (!ok) fails++; console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${ev ? '  ' + ev : ''}`); };
function run(sh, err, ids, rec, apiThrows) {
  const ctx = vm.createContext({ console, Promise, Map, Set, Array, String, Error, RegExp, api: async () => { if (apiThrows) throw new Error('offline'); return { sheet: rec }; } });
  vm.runInContext(source.slice(a, b) + '; this.f = savedDespiteSetRefusal;', ctx);
  return ctx.f(sh, err, ids);
}
const MSG = 'GF Sheet 10: not saved to the cloud — GF Draft 15 cannot go into Set 5: GF Draft 15 is not full yet (25% full), and a partial sheet never joins a set (your rule R3).';
const local = () => ({ sheetId: 'gold-s15', problem: 'not saved to the cloud — GF Draft 15 cannot go into Set 5: GF Draft 15 is not full yet', persistedDone: false, dirty: true,
  charms: [{ id: 'c1', poolId: 'p1' }, { id: 'c2', poolId: 'p2' }], placements: [{ id: 'c1' }, { id: 'c2' }] });
const cloudOf = pools => ({ charms: pools.map((p, i) => ({ id: 'x' + i, poolId: p })), placements: pools.map((p, i) => ({ id: 'x' + i })) });
(async () => {
  { const sh = local(), r = await run(sh, new Error(MSG), new Set(['pcamera']), cloudOf(['p1', 'p2']));
    check('1a the cloud holds the sheet as it stands, the removed piece not on it: the set refusal is no failure, and says so', r && /saved; the set refused/.test(r.note) && /cannot go into Set 5/.test(r.note), JSON.stringify(r));
    check('1b the sheet\'s problem mark is cleared and it counts as saved', sh.problem === null && sh.persistedDone === true && sh.dirty === false); }
  { const sh = local(), r = await run(sh, new Error(MSG), new Set(['p1']), cloudOf(['p1', 'p2']));
    check('2 the removed piece is still on the cloud\'s sheet: a real failure', r === null && sh.problem !== null); }
  { const sh = local(), r = await run(sh, new Error(MSG), new Set(['pcamera']), cloudOf(['p1']));
    check('3 the cloud\'s sheet holds fewer pieces than this page: a real failure', r === null); }
  { const sh = local(), r = await run(sh, new Error(MSG), new Set(['pcamera']), cloudOf(['p1', 'p2', 'p3']));
    check('4 the cloud\'s sheet holds more pieces than this page: a real failure', r === null); }
  { const sh = local(), r = await run(sh, new Error('not saved to the cloud — network request failed'), new Set(['pcamera']), cloudOf(['p1', 'p2']));
    check('5 an error that is not a set refusal is left as it was', r === null && sh.problem !== null); }
  { const sh = local(), r = await run(sh, new Error(MSG), new Set(['pcamera']), null);
    check('6 the sheet is not in the cloud: a real failure', r === null); }
  { const sh = local(), r = await run(sh, new Error(MSG), new Set(['pcamera']), cloudOf(['p1', 'p2']), true);
    check('7 the cloud cannot be read: a real failure, never a guess', r === null); }
  { const i = source.indexOf('await rewritePage(r.sh, r.st, paint);'); const loop = source.slice(source.indexOf('for (const r of rewrite) { if (!activeCharms(r.sh).length'), i + 400);
    check('8 takeOff asks the helper when a sheet\'s rewrite throws, and notes it at the end', /catch \(e\) \{ const ok = await savedDespiteSetRefusal\(r\.sh, e, ids\); if \(!ok\) throw e;/.test(loop) && /if \(setNotes\.length\) work\.note \+=/.test(source)); }
  // 9 · NOTAKE (Paul, 18 Nov 2026: the freed room of a new draft offered order 4222232089 from GF Sheet 1 of the Nov 12 Set-1, a released sheet in a set): an order is never taken from a released sheet or one in a set
  { const m0 = source.indexOf('  function movableFrom(src, target) {'), m1 = source.indexOf('  /* A pair (or n discs) moves whole.', m0); assert(m0 > 0 && m1 > m0, 'movableFrom is found');
    const mk = modern => { const LiveNest = { closed: () => false }, Gate = { modern: () => modern }, ctx = vm.createContext({ sentToStation: () => false, LiveNest, Gate, window: { LiveNest, Gate } }); vm.runInContext(source.slice(m0, m1) + '; this.f = movableFrom;', ctx); return ctx.f; };
    const target = { metal: 'gold', draft: true }, src = o => Object.assign({ metal: 'gold', draft: true }, o);
    for (const modern of [true, false]) { const f = mk(modern);
      check(`9a (run ${modern ? 'modern' : 'old'}) a newer draft that is still filling can give an order`, f(src({}), target) === true);
      check(`9b (run ${modern ? 'modern' : 'old'}) a RELEASED sheet in a set never gives one (GF Sheet 1 of Set-1: releaseFull, in a set)`, f(src({ draft: false, setId: 'set-2026-11-12-4', releaseFull: true }), target) === false);
      check(`9c (run ${modern ? 'modern' : 'old'}) a sheet in a set never gives one, nor does a full one with no set`, f(src({ draft: false, setId: 'set-x' }), target) === false && f(src({ releaseFull: true }), target) === false);
      check(`9d (run ${modern ? 'modern' : 'old'}) a cut sheet never gives one`, f(src({ laserDoneAt: 1 }), target) === false); } }
  console.log(fails ? `FAIL take-off-set-refusal: ${fails}` : 'PASS take-off-set-refusal'); process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ERR', e && e.stack || e); process.exit(1); });
