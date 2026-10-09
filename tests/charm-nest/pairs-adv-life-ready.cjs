// ADVLIFE (pairs-1009 phase 2, follow-up): the REAL Library handlers over the in-memory shop, for the three readiness findings of the order lifecycle:
//   finding 6   a group whose pieces say they are 2 (groupSize) but whose line lists only the Left: the sheet holding the Left is not ready, the refusal names the right earring
//   finding 12  one piece listed by two saved sheets: both sheets are held back, the refusal names the ear and both sheets
//   laserDone   the refusal names the ear that waits, and where it is, when the order has a pair
// A line with no groupSize (pooled before the rule) is read as it always was.   node tests/charm-nest/pairs-adv-life-ready.cjs
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs'), { seed } = require('./laser-workflow.cjs');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';
(async () => {
  const srv = await start({ receipts: [] }); seed(srv); const { st } = srv;
  const call = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return { status: r.status, ...await r.json() }; };
  const status = () => call({ op: 'laserStatus', sheetIds: ['ready-sheet'] }), complete = () => call({ op: 'laserDone', kind: 'sheet', id: 'ready-sheet', stage: 'laser', by: 'Seth' });
  const mine = r => r.sheets.find(s => s.id === 'ready-sheet').orderReadiness[order];
  const order = '3700000101', key = order + '_1', L = key + '_1', Rr = key + '_2', run = st.doc(RUN, 'run-fixture');
  const line = extra => ({ orderId: order, state: 'written', quantity: 1, pieceCount: 2, poolIds: [L], engraveCandidate: false, ...extra });
  let n = 0; const ok = name => console.log('  ok  ' + name), say = t => console.log('      text: ' + t);
  try {
    st.put(S, 'pending-sheet', { verification: { ok: true } });
    // 1 · a pair pooled before the rule: one id, pieceCount 2, no groupSize: the order is as ready as ever
    run.lines[key] = line();
    let r = await status(); assert.equal(mine(r).ready, true, JSON.stringify(mine(r))); assert.equal(r.sheets.find(s => s.id === 'ready-sheet').laser.ready, true, 'the sheet is ready for the laser: nothing waits for a piece the line never had'); n++; ok('a line with no groupSize (pooled before the rule) is read as it always was');
    // 2 · the half-written group: the line says its pieces are a group of 2 and lists only the Left
    run.lines[key] = line({ groupSize: 2, sides: ['L', 'R'] });
    r = await status(); assert.equal(mine(r).ready, false, JSON.stringify(mine(r))); assert.equal(mine(r).key, 'pooled'); assert.match(mine(r).why, /^the right earring is missing: only 1 of this line's 2 pieces were made/);
    assert.equal(r.sheets.find(s => s.id === 'ready-sheet').laser.ready, false, 'the sheet is not ready for the laser');
    let c = await complete(); assert.equal(c.status, 409); assert.match(c.error, /^Not ready for Laser cutting: order 3700000101 — the right earring is missing/); assert(!st.doc(S, 'ready-sheet').laserDoneAt, 'not marked cut'); say(c.error); n++; ok('finding 6: the sheet holding the Left waits for the Right whose piece record was never written; the refusal names the right earring');
    // 3 · the Right exists on another sheet that is not ready: the refusal names the ear and the sheet it is on
    run.lines[key] = line({ poolIds: [L, Rr], groupSize: 2, sides: ['L', 'R'] });
    st.put(S, 'right-sheet', { id: 'right-sheet', setId: 'other-set', setSeq: 2, sheetIndex: 7, runId: 'run-fixture', metal: 'gold', status: 'complete', placedCount: 1, charmCount: 1, poolIds: [Rr], orders: [order], verification: { ok: false }, updatedAt: { toMillis: () => Date.now() }, createdAt: { toMillis: () => Date.now() } });
    st.put(SET, 'other-set', { setId: 'other-set', seq: 2, sheetIds: ['right-sheet'], materials: ['gold'], orders: {}, status: 'open' });
    r = await status(); assert.equal(mine(r).ready, false, JSON.stringify(mine(r)));
    c = await complete(); assert.equal(c.status, 409); assert.match(c.error, /order 3700000101 — the right earring is on .* Sheet 7 \(/); say(c.error); n++; ok('the ear that waits on another sheet is named with that sheet');
    // 4 · the same line without its sides says what it always said
    run.lines[key] = line({ poolIds: [L, Rr], groupSize: 2 });
    c = await complete(); assert.equal(c.status, 409); assert.doesNotMatch(c.error, /earring/); n++; ok('a line that does not say its sides keeps the reason it always had');
    // 5 · finding 12: the Left is listed by a second saved sheet as well
    run.lines[key] = line({ poolIds: [L, Rr], groupSize: 2, sides: ['L', 'R'] });
    st.put(S, 'right-sheet', { verification: { ok: true }, poolIds: [Rr, L] });
    r = await status(); assert.equal(mine(r).ready, false, JSON.stringify(mine(r))); assert.equal(mine(r).key, 'doubled');
    c = await complete(); assert.equal(c.status, 409); assert.match(c.error, /order 3700000101 — the left earring is listed on both .* Sheet \d+ and .* Sheet \d+, so it would be cut twice/); assert(!st.doc(S, 'ready-sheet').laserDoneAt); say(c.error); n++; ok('finding 12: a piece listed by two saved sheets stops the laser, with the ear and both sheets named');
    // 6 · the second listing goes away: the laser is allowed again (nothing else waits)
    st.put(S, 'right-sheet', { poolIds: [Rr] }); st.put(SET, 'other-set', { sheetIds: [] });
    r = await status(); assert.notEqual(mine(r).key, 'doubled', JSON.stringify(mine(r))); n++; ok('taking the second listing off clears the doubled wait');
    console.log('pairs-adv-life-ready: ' + n + ' passed');
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
