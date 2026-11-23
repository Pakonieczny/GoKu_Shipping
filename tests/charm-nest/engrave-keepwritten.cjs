// KEEPWRITTEN (Paul, 22 Nov 2026: "This piece did not get approved correctly even though it's approved I got this red notice and I don't ever wanna see these ... the approved button ... must be 100%
// resilient"; order 4243151567 "1991" approved 22:29:54 and order 4242795385 piece 3 approved 22:14:02, each saved by the cloud with its back file, each written back to Review by a computer that still
// held its older copy of the line). The cloud's own engraving write keeps an approval whose approved back file stands, whatever copy a computer sends. The real handler over the repo's fake site.
//   1  a computer re-sends the job un-approved, same words, font and place (or no place): the cloud keeps it written, with its approval and every seal
//   2  every real change still un-approves: new art (the back file invalidated), a move, a resize, other words, another font; None is a person's decision; no back file at all is as before
//   3  a front job is never held by a back file (each face its own file); a line with no slot (one piece) is held by its piece's file
//   node tests/charm-nest/engrave-keepwritten.cjs
'use strict';
const { start } = require('./bridge-server.cjs');
const LINE = '4242795385_5305127482', ORDER = '4242795385', T5 = 1795403642166;
const PLACE = { centre: [16.97, 17.12], angle: 0, size: 6.565, lines: ['A+E+T'], weight: 'Semibold', lineGap: 0.216, fittedMax: 8.33, upAngle: 90, fontKey: 'source-sans-3' };
const LINE2 = '4243151567_5306455423', ORDER2 = '4243151567', T2 = 1795404594000;
const PLACE2 = { centre: [18.417599999999993, 18.410766666667463], angle: 0, size: 7.950313246189509, lines: ['1991'], weight: 'Semibold', upAngle: 90, fontKey: 'source-sans-3', lineGap: 0.216 };
(async () => {
  const srv = await start({ receipts: [] }); const { st } = srv;
  const call = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return Object.assign({ status: r.status }, await r.json()); };
  const job = (key, o) => Object.assign({ key, slot: key.includes('#') ? key.split('#')[1] : null, text: 'A+E+T', lines: ['A+E+T'], lineInput: ['A+E+T'], fontKey: 'source-sans-3', source: 'personalization' }, o);
  const put = (line, jobs, dev) => call({ op: 'engraveSyncPut', by: 'Michelle_R', dev: dev || 'dlbj6s3ua', lines: [{ key: line, sig: 's' + Math.random(), rec: { v: 1, override: null, engrave: { needed: true }, raised: true, jobs } }] });
  const get = async (order, key) => { const r = await call({ op: 'engraveSyncGet', orders: [order] }); return r.lines[0].rec.jobs.find(j => j.key === key); };
  const back = (poolId, o) => st.put('Charm_Pool_Back', poolId, Object.assign({ poolId, order: poolId.split('_')[0], transactionId: poolId.split('_')[1], text: 'A+E+T', lines: ['A+E+T'], fontKey: 'source-sans-3', centre: [16.97, 17.12], sizePt: 6.565, angle: 0, approvedAt: T5, approvedBy: 'Michelle_R', invalidated: false, sheetId: 'd-57dc2e3583df0dbb' }, o), false);
  let fails = 0; const ok = (c, m, d) => { if (!c) fails++; console.log((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 500))); };
  const K3 = LINE + '#C3', seal = { id: 'engraveApproved:' + T5 + ':Michelle_R', how: 'engraveApproved', at: T5, by: 'Michelle_R' }, older = { id: 'engraveApproved:1795398578186:Michelle_R', how: 'engraveApproved', at: 1795398578186, by: 'Michelle_R' };
  const written = () => job(K3, { state: 'written', approvedAt: T5, approvedBy: 'Michelle_R', place: PLACE, engravingSeals: [older, seal] });
  const fresh = async () => { await put(LINE, [written()], 'cloudish01'); back(LINE + '_3', { slot: 'C3' }); };
  try {
    /* 1 · the overwrite */
    await fresh();
    await put(LINE, [job(K3, { state: 'review', approvedAt: null, approvedBy: null, place: PLACE, engravingSeals: [older] })]);
    let j = await get(ORDER, K3);
    ok(j.state === 'written' && j.approvedAt === T5 && j.approvedBy === 'Michelle_R' && (j.engravingSeals || []).length === 2, '1 · a computer re-sends piece 3 un-approved (same words, font, place): the cloud keeps it written, its approval and both seals', j);
    await fresh();
    await put(LINE, [job(K3, { state: 'review', approvedAt: null, approvedBy: null })]);
    j = await get(ORDER, K3);
    ok(j.state === 'written' && j.approvedAt === T5 && !!j.place, '1 · ... and with no place at all (a computer without the charm)', j);
    /* 2 · real changes */
    await fresh(); back(LINE + '_3', { slot: 'C3', invalidated: true });
    await put(LINE, [job(K3, { state: 'ready', approvedAt: null, approvedBy: null, place: PLACE })]);
    j = await get(ORDER, K3); ok(j.state === 'ready' && !j.approvedAt, '2 · new art (the back file invalidated): un-approved, as before', j);
    await fresh();
    await put(LINE, [job(K3, { state: 'review', approvedAt: null, approvedBy: null, place: Object.assign({}, PLACE, { centre: [17.9, 17.12] }) })]);
    j = await get(ORDER, K3); ok(j.state === 'review' && !j.approvedAt && j.place.centre[0] === 17.9, '2 · the words moved by a person: un-approved, the new place kept', j);
    await fresh();
    await put(LINE, [job(K3, { state: 'review', approvedAt: null, approvedBy: null, place: Object.assign({}, PLACE, { size: 6.9 }) })]);
    j = await get(ORDER, K3); ok(j.state === 'review' && !j.approvedAt, '2 · resized: un-approved', j);
    await fresh();
    await put(LINE, [job(K3, { state: 'review', approvedAt: null, approvedBy: null, text: 'A+E', lines: ['A+E'], place: PLACE })]);
    j = await get(ORDER, K3); ok(j.state === 'review' && j.text === 'A+E', '2 · other words: un-approved', j);
    await fresh();
    await put(LINE, [job(K3, { state: 'review', approvedAt: null, approvedBy: null, fontKey: 'angelina', place: PLACE })]);
    j = await get(ORDER, K3); ok(j.state === 'review' && j.fontKey === 'angelina', '2 · another font: un-approved', j);
    await fresh();
    await put(LINE, [job(K3, { state: 'none', approvedAt: null, approvedBy: null, place: PLACE })]);
    j = await get(ORDER, K3); ok(j.state === 'none', '2 · None (a person\'s decision): written as sent', j);
    await fresh(); st.docs.delete('Charm_Pool_Back/' + LINE + '_3');
    await put(LINE, [job(K3, { state: 'review', approvedAt: null, approvedBy: null, place: PLACE })]);
    j = await get(ORDER, K3); ok(j.state === 'review', '2 · no approved back file at all: as before (written as sent)', j);
    /* 3 · faces and one-piece lines */
    const KF = LINE + '#C3';
    await put(LINE, [Object.assign(written(), { face: 'front' })], 'cloudish01'); back(LINE + '_3', { slot: 'C3' });   // (only a BACK file stands)
    await put(LINE, [job(KF, { face: 'front', state: 'review', approvedAt: null, approvedBy: null, place: PLACE })]);
    j = await get(ORDER, KF); ok(j.state === 'review', '3 · a front job is never held by the piece\'s back file', j);
    const one = { key: LINE2, slot: null, text: '1991', lines: ['1991'], fontKey: 'source-sans-3' };
    await put(LINE2, [Object.assign({}, one, { state: 'written', approvedAt: T2, approvedBy: 'Michelle_R', place: PLACE2 })], 'cloudish01');
    st.put('Charm_Pool_Back', LINE2 + '_1', { poolId: LINE2 + '_1', order: ORDER2, transactionId: '5306455423', slot: null, text: '1991', lines: ['1991'], fontKey: 'source-sans-3', centre: [18.42, 18.41], sizePt: 7.95, angle: 0, approvedAt: T2, approvedBy: 'Michelle_R', invalidated: false, sheetId: 'd-1ff008d1c436435d' }, false);
    await put(LINE2, [Object.assign({}, one, { state: 'review', approvedAt: null, approvedBy: null, place: PLACE2 })]);
    j = await get(ORDER2, LINE2); ok(j.state === 'written' && j.approvedAt === T2, '3 · order 4243151567 ("1991", one piece, no slot): the re-sent Review does not take its approval', j);
  } catch (e) { fails++; console.log('FAIL run: ' + (e && e.stack || e)); }
  srv.close && srv.close();
  console.log(fails ? `FAIL engrave-keepwritten (${fails})` : 'PASS engrave-keepwritten'); process.exit(fails ? 1 : 0);
})();
