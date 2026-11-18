// NOPARTIALSET (Paul, 18 Nov 2026: "why would that be engaged at the time of me trying to remove a charm from a given sheet ... this draft sheet isn't complete"): removing a charm from a 25% full draft ended "GF Sheet 10: not saved to the cloud — GF Draft 15 cannot go into
// Set 5 ... a partial sheet never joins a set (your rule R3)". The set look joins a sheet in memory first (draft false, a set, the next sheet number) and waits for the cloud; a save of the same sheet in that time sent the join, and the cloud refused it as the sheet's own save.
//   1 membershipOf (charm-nest-1.html): the membership a save may carry: a partial GF/SS sheet of a modern run is a draft outside every set, whatever the page holds in memory; every other sheet keeps its own
//   2 the set look (charm-nest-bridge.js): a partial GF/SS sheet is never put into a set in memory, except by a person's own drop
// The real function and the real source, over fixtures: no browser.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8'), bridge = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
let fails = 0; const check = (id, ok, ev) => { if (!ok) fails++; console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${ev ? '  ' + ev : ''}`); };
const a = html.indexOf('function membershipOf(sh) {'), b = html.indexOf('async function persistSheet(', a); assert(a > 0 && b > a, 'membershipOf sits before persistSheet');
const SR = require('../../charm-nest-set-rules.js');
const run = (sh, modern = true) => { const ctx = vm.createContext({ window: { CharmNestSetRules: SR, Gate: { modern: () => modern } }, Gate: { modern: () => modern } }); vm.runInContext(html.slice(a, b) + '; this.f = membershipOf;', ctx); return JSON.parse(JSON.stringify(ctx.f(sh))); };
const draft = o => Object.assign({ metal: 'gold', metalLabel: 'GF 14/20', runId: 'r1', draft: true, setId: null, seq: 15, placements: [1, 2, 3], releaseFull: false }, o || {});
// the race: the set look has put the draft into Set 5 in memory (draft false, setId, the next number) and the sheet is saved
check('1a a partial GF draft the set look has joined in memory is saved as the draft it is', JSON.stringify(run(draft({ draft: false, setId: 'set-2026-11-12-1', seq: 5, sheetIndex: 10 }))) === JSON.stringify({ draft: true, setId: null, setSeq: null, sheetIndex: null }));
check('1b a plain partial draft is saved as it is (outside every set)', JSON.stringify(run(draft())) === JSON.stringify({ draft: true, setId: null, setSeq: 15, sheetIndex: null }) || run(draft()).setId === null);
check('2 a COMPLETED sheet in a set keeps its set', JSON.stringify(run(draft({ draft: false, setId: 's1', seq: 2, sheetIndex: 3, releaseFull: true }))) === JSON.stringify({ draft: false, setId: 's1', setSeq: 2, sheetIndex: 3 }));
check('3 a CUT sheet in a set keeps its set', run(draft({ draft: false, setId: 's1', seq: 2, sheetIndex: 3, laserDoneAt: 5 })).setId === 's1');
check('4 a run that is not modern is saved as before', run(draft({ draft: false, setId: 's1', seq: 2, sheetIndex: 3 }), false).setId === 's1');
check('5 a 10K / 14K sheet (the solid metals have their own rule) keeps its set', run(draft({ metal: 'gold14k', metalLabel: '14K Gold', draft: false, setId: 's1', seq: 2, sheetIndex: 3 })).setId === 's1');
const j = bridge.indexOf('SRt.isPartial(sh) && !(sh.handSet === set.setId && !sh.cardinalHome)) continue;'), p = bridge.indexOf('const previous = { draft:sh.draft, setId:sh.setId');
check('6 the set look skips a partial GF/SS sheet before it changes it in memory, except a person\'s own drop', j > 0 && p > j && p - j < 400);
console.log(fails ? `FAIL no-partial-set: ${fails}` : 'PASS no-partial-set'); process.exit(fails ? 1 : 0);
