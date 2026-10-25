// AUDIT (R14 · names and numbers, C11): the ONE numbering (charm-nest-sheet-name.js claim / raise / nextNumber), driven exactly the way its two
// writers drive it — the run's assembly (charm-nest-bridge.js tendSet: raise over the set's sheets, claim, raise; a sheet that leaves is raised into
// sheetNos first, then made a draft with sheetIndex null and its file name kept; a join whose save fails is put back as it was, its number already
// retired) and the server's set edit (charmNestLibrary applySetMembers: raise(sheetNos, members), SetEdit.indexFor = claim) — over thousands of random
// histories with several sets, metals, leaves, returns, moves between sets, cuts and failed saves. Required after every step:
//   · in a set, no two sheets of one metal share a number (and every number is >= 1);
//   · a number that a sheet held in a set is never given to ANOTHER sheet in that set later (retired), only to the same sheet coming back;
//   · a sheet in no set that is not cut is never named "<metal> Sheet n" (C11), a sheet in a set is "<metal> Sheet <its number>";
//   · uniqueNames gives every sheet of the board a different name.
//   node tests/charm-nest/audit-number-names.cjs
'use strict';
const assert = require('node:assert/strict');
const SN = require('../../charm-nest-sheet-name.js'), O = require('../../charm-nest-orders.js');
const METALS = ['gold', 'silver', 'rose', 'gold14k'];
let seed = 1; const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff), pick = a => a[Math.floor(rnd() * a.length)];
function history(n) {
  const sets = [1, 2, 3].map(k => ({ setId: `set-2026-10-2${k}-${k}`, seq: k, day: `2026-10-2${k}`, sheetNos: {}, committed: k === 3 }));
  const sheets = Array.from({ length: 14 }, (_, i) => ({ id: 'sh' + i, sheetId: 'sh' + i, metal: METALS[i % METALS.length], page: 1 + Math.floor(i / METALS.length), draft: true, setId: null, sheetIndex: null, fileBase: null, day: '2026-10-21' }));
  const given = new Map();   // `${setId}|${metal}|${n}` -> sheet id that held it
  const fileBaseFor = (sh, set) => k => O.sheetName(sh.metal, set.day, set.seq, k);
  const membersOf = set => sheets.filter(s => s.setId === set.setId && !s.draft);
  const join = (sh, set, fail) => {
    const previous = { draft: sh.draft, setId: sh.setId, sheetIndex: sh.sheetIndex, fileBase: sh.fileBase };
    if (set.committed) {   // the server's set edit
      const members = membersOf(set); let nos = SN.raise(set.sheetNos, members);
      const k = SN.claim(sh, members, nos, fileBaseFor(sh, set));
      Object.assign(sh, { sheetIndex: k, fileBase: fileBaseFor(sh, set)(k), draft: false, setId: set.setId }); set.sheetNos = SN.raise(nos, [sh]);
    } else {               // the run's assembly
      sh.draft = false; sh.setId = set.setId;
      const here = sheets.filter(p => p !== sh && p.setId === set.setId && p.metal === sh.metal);
      set.sheetNos = SN.raise(set.sheetNos, sheets.filter(p => p !== sh && p.setId === set.setId));
      sh.sheetIndex = SN.claim(sh, here, set.sheetNos, fileBaseFor(sh, set));
      set.sheetNos = SN.raise(set.sheetNos, [sh]);
      sh.fileBase = fileBaseFor(sh, set)(sh.sheetIndex);
      if (fail) { Object.assign(sh, previous); return; }   // (a save that failed: the sheet is put back as it was; its number stays retired)
    }
    const key = `${set.setId}|${sh.metal}|${sh.sheetIndex}`, had = given.get(key);
    assert(!had || had === sh.id, `step ${n}: ${sh.id} was given number ${sh.sheetIndex} of ${sh.metal} in ${set.setId}, which ${had} held before (a retired number given again)`);
    given.set(key, sh.id);
  };
  const leave = sh => {
    const set = sets.find(s => s.setId === sh.setId); if (!set) return;
    set.sheetNos = SN.raise(set.sheetNos, [sh]);
    Object.assign(sh, { draft: true, setId: null, sheetIndex: null });   // (fileBase kept, as tendSet and the server's leave keep it)
  };
  for (let step = 0; step < n; step++) {
    const sh = pick(sheets), r = rnd();
    if (sh.laserDoneAt) continue;
    if (sh.setId && r < 0.4) leave(sh);
    else if (sh.setId && r < 0.5) { leave(sh); join(sh, pick(sets), false); }
    else if (!sh.setId && r < 0.85) join(sh, pick(sets), rnd() < 0.1);
    else if (sh.setId && r < 0.9) sh.laserDoneAt = 1;
    // invariants
    for (const set of sets) for (const m of METALS) {
      const nums = membersOf(set).filter(s => s.metal === m).map(s => SN.numberOf(s));
      assert(nums.every(k => k >= 1), `step ${step}: a sheet of ${m} in ${set.setId} has no number`);
      assert.equal(new Set(nums).size, nums.length, `step ${step}: two ${m} sheets of ${set.setId} share a number: ${nums}`);
    }
    for (const s of sheets) {
      const nm = SN.name(s);
      if (!s.setId && !s.laserDoneAt) assert(/ Draft \d+$/.test(nm), `step ${step}: ${s.id} is in no set and not cut but is named "${nm}" (C11)`);
      if (s.setId && !s.draft) assert.equal(nm, `${SN.CODE[s.metal]} Sheet ${s.sheetIndex}`, `step ${step}: ${s.id} in ${s.setId}`);
    }
    const names = SN.uniqueNames(sheets.map(s => ({ ...s, setSeq: (sets.find(x => x.setId === s.setId) || {}).seq })));
    assert.equal(new Set(names.values()).size, sheets.length, `step ${step}: two sheets of the board read alike`);
  }
}
let runs = 0;
for (let s = 1; s <= 400; s++) { seed = s; history(200); runs++; }
// C11 by hand: the record kinds a reader can meet
assert.equal(SN.name({ metal: 'gold', draft: true, sheetIndex: 3, page: 5 }), 'GF Draft 5', 'a draft never reads by a stale number');
assert.equal(SN.name({ metal: 'gold', draft: true, page: 5, fileBase: 'GF_Oct.21.26_Set-1_Sheet-3' }), 'GF Draft 5', 'a draft that left a set reads as a draft');
assert.equal(SN.name({ metal: 'gold', draft: true, page: 5, laserDoneAt: 9, fileBase: 'GF_Oct.21.26_Set-1_Sheet-3' }), 'GF Sheet 3', 'a cut sheet keeps its number');
// the documented reading of C11 (set-rules.md): a record that is not a draft and names no set but keeps a number still reads "Sheet n"
const c11 = SN.name({ metal: 'gold', draft: false, setId: null, sheetIndex: 8, page: 2 });
console.log(`note C11 (documented conflict, not judged here): a non-draft record with no set and sheetIndex 8 reads "${c11}"`);
console.log(`PASS: audit-number-names: ${runs} random histories of 200 steps (3 sets, 4 metals, leaves, returns, moves, cuts, failed saves): no shared number, no retired number given to another sheet, no loose sheet named "Sheet n", every board name unique`);
