// ENGLABEL (Paul, 17 Nov 2026: "they are approved but the sheet says they are not"): the cloud's word for a piece (sheet.engraving, as getSheet answers it) is "approved" whenever the sheet holds an approved,
// saved back record for it, also when the run's saved line still says {approved:true, state:'review'} (22 pieces of the live Silver Set-3 Sheet-1 read so). Offline, on the fake Firestore of _sandboxFakes.cjs.
//   node tests/charm-nest/eng-label.cjs
'use strict';
const path = require('path'), fs = require('fs');
const F = require('./_sandboxFakes.cjs'); F.install();
const Lib = require(path.join(__dirname, '../../netlify/functions/charmNestLibrary.js'));
const f = Lib.engApprovedByBack;
let fails = 0; const check = (id, ok, ev) => { if (!ok) fails++; console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${ev ? '  ' + ev : ''}`); };
const back = { poolId: 'p_1', approvedBy: 'Ana_M', approvedAt: 1794946227552 };

let d = f({ needed: true, state: 'review', approved: true }, back);
check('1 line approved:true but still "review" + an approved saved back -> approved, from the back', d.state === 'approved' && d.approved === true && d.fromBack === true && d.approvedBy === 'Ana_M' && d.approvedAt === 1794946227552, JSON.stringify(d));
d = f({ needed: true, state: 'review', approved: false }, back);
check('2 line not approved + an approved saved back -> approved (as ENGLIVE always did)', d.state === 'approved' && d.approved === true && d.fromBack === true);
const keep = { needed: true, state: 'approved', approved: true, approvedBy: 'Ana_M', approvedAt: 5 };
check('3 a line that is already approved is returned untouched', f(keep, back) === keep);
const written = { needed: true, state: 'written', approved: true };
check('4 a written line is returned untouched', f(written, back) === written);
const none = { needed: false, state: 'none', approved: true };
check('5 nothing to engrave stays nothing to engrave', f(none, back) === none);
const rv = { needed: true, state: 'review', approved: true };
check('6 no approved saved back -> the line is returned as it was (still "review")', f(rv, null) === rv);
const words = { needed: true, state: 'words', approved: false };
check('7 words still to confirm, no back -> untouched', f(words, null) === words);
check('8 a piece with faces keeps them', !!f({ needed: true, state: 'review', approved: true, faces: { back: { needed: true } } }, back).faces);

// the live record (read from the cloud, read-only, 17 Nov): every needed piece with a saved approved back reads approved, the 13 never approved stay "review"
const p = '/tmp/set3/s_silver-s2-mwd9brq6.json';
if (fs.existsSync(p)) {
  const s = JSON.parse(fs.readFileSync(p));
  const ok = r => r && !r.invalidated && !r.pending && r.face !== 'front' && r.approvedAt && r.approvedBy;
  let appr = 0, review = 0, bad = 0;
  for (const [id, e] of Object.entries(s.engraving)) {
    if (!e.needed) continue;
    const b = (s.backPool || []).find(r => String(r.poolId) === id && ok(r)) || null, out = f(e, b);
    if (b) { if (out.state === 'approved') appr++; else bad++; } else if (out.state === 'review') review++; else bad++;
  }
  check('9 live Silver Set-3 Sheet-1: 25 pieces with a saved approved back read approved, the 13 with none stay "review"', appr === 25 && review === 13 && bad === 0, `approved ${appr}, review ${review}, wrong ${bad}`);
}
console.log(fails ? `\n${fails} FAILED` : '\nALL PASS'); process.exit(fails ? 1 : 0);
