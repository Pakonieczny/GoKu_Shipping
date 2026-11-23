// PIECESEALS (Paul, 23 Nov 2026: "Some pieces get extra seals": orders 4242795385 and 4236205556). The pieces of a line engraved by piece (C1, C2, C3) share ONE record on the line (row.engrave): it kept the seals of the piece
// kept last and every piece read them as its own, so approving one piece put its seal on the others, in the cloud's record too.
//   1  three pieces of one line approved in turn: each carries its own seal only; the line carries all three
//   2  the order window puts the timeline's approvals on the line: no piece takes another's
//   3  copies an earlier version left on a piece go (the chicken's third piece: five seals, two of them the other pieces'); its own older approvals stay
//   4  a line of one piece, an ear or a disc with its own row, a front and a back: as before
//   SEALS_ROOT=<another checkout> node tests/charm-nest/engrave-piece-seals.cjs      Offline.
'use strict';
const path = require('path'), root = process.env.SEALS_ROOT ? path.resolve(process.env.SEALS_ROOT) : path.join(__dirname, '../..');
let bad = 0, n = 0; const ok = (c, m) => { n++; if (!c) bad++; console.log((c ? '  ✓ ' : '  ✗ ') + m); };
let jobs = []; global.window = { Engrave: { jobsOf: () => jobs }, Seal: null };
const fs = require('fs'), src = fs.readFileSync(path.join(root, 'charm-nest-engraving-seals.js'), 'utf8'); const mod = { exports: {} };
const S = new Function('window', 'module', 'require', src + '\nreturn window.CNEngravingSeals;')(global.window, undefined, undefined);
const at = l => l.map(s => +s.at), job = (row, slot, o) => Object.assign({ key: 'L#' + (slot || ''), slot: slot || null, row, state: 'review', engravingSeals: [], backs: [] }, o || {});
const approve = (j, by, t) => { S.add(j, 'engraveApproved', by, t); j.state = 'approved'; j.approvedBy = by; j.approvedAt = t; Object.assign(j.row.engrave, { state: 'approved', approved: true, approvedBy: by, approvedAt: t }); };
{ const row = { key: 'L', engrave: { needed: true } }; jobs = ['C1', 'C2', 'C3'].map(s => job(row, s)); jobs.forEach((j, i) => approve(j, 'P', 1000 + i)); jobs.forEach(j => S.keep(j));
  ok(JSON.stringify(jobs.map(j => at(j.engravingSeals))) === '[[1000],[1001],[1002]]', '1 three pieces approved in turn: one seal each: ' + JSON.stringify(jobs.map(j => at(j.engravingSeals))));
  ok(JSON.stringify(at(row.engrave.seals)) === '[1000,1001,1002]', '1 the line keeps all three'); ok(jobs.every(j => S.list(j).length === 1), '1 and every list of a piece reads its own'); }
{ const row = { key: 'L', engrave: { needed: true } }; jobs = ['C1', 'C2'].map(s => job(row, s)); approve(jobs[0], 'P', 2000); approve(jobs[1], 'P', 2001); row.engrave.seals = S.merge(row.engrave, { seals: [{ how: 'engraveApproved', at: 2000, by: 'P' }, { how: 'engraveApproved', at: 2001, by: 'P' }] }); S.keep(jobs[0]);
  ok(JSON.stringify(at(jobs[0].engravingSeals)) === '[2000]', '2 the timeline\'s approvals on the line are not piece 1\'s: ' + JSON.stringify(at(jobs[0].engravingSeals))); ok(at(row.engrave.seals).length === 2, '2 and the line keeps both'); }
{ const row = { key: 'L', engrave: { needed: true } }; const c1 = job(row, 'C1', { state: 'written', approvedAt: 100, approvedBy: 'M' }), c2 = job(row, 'C2', { state: 'written', approvedAt: 200, approvedBy: 'M' }), c3 = job(row, 'C3', { state: 'written', approvedAt: 600, approvedBy: 'P', engravingSeals: [100, 200, 300, 400, 500, 600].map(t => ({ how: 'engraveApproved', at: t, by: t === 600 ? 'P' : 'M' })) }); jobs = [c1, c2, c3];
  ok(JSON.stringify(at(S.list(c3))) === '[300,400,500,600]', '3 the copies of the other pieces\' approvals go, its own older ones stay: ' + JSON.stringify(at(S.list(c3)))); S.keep(c3); ok(c3.engravingSeals.length === 4, '3 and they are gone from what is saved'); }
{ const row = { key: 'L', engrave: { seals: [{ how: 'engraveApproved', at: 5, by: 'A' }] } }, j = job(row, null); jobs = [j]; S.keep(j); ok(at(j.engravingSeals).length === 1, '4 a line of one piece reads its line\'s seals as before'); approve(j, 'P', 9); ok(JSON.stringify(at(S.list(j))) === '[5,9]' && JSON.stringify(at(row.engrave.seals)) === '[5,9]', '4 and adds its own'); }
{ const p = { key: 'P' }, row = { key: 'P#L', parentRow: p, slot: 'L', engrave: { seals: [{ how: 'engraveApproved', at: 7, by: 'A' }] } }, j = job(row, 'L'); jobs = [j]; ok(at(S.list(j)).length === 1, '4 an ear with a row of its own reads that row'); }
{ const row = { key: 'L', engrave: {} }, b = job(row, 'C1', { key: 'L#C1' }), f = job(row, 'C1', { key: 'L#C1~front', face: 'front' }); jobs = [b, f]; approve(b, 'P', 11); approve(f, 'P', 12); ok(JSON.stringify(at(S.list(b))) === '[11]' && JSON.stringify(at(S.list(f))) === '[12]' && S.list(f)[0].face === 'front', '4 a front and a back of one piece keep their own'); }
console.log(`engrave-piece-seals: ${n - bad} of ${n} checks passed`); process.exit(bad ? 1 : 0);
