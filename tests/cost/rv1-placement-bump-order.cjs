// RV1 review of the counter family (7 Oct 2026): Complete / Undo on the Library must reach every other computer's order window in about
// 3 s. The order window reads the placement feed (getOrderPieces), which since FC3 answers `unchanged` for ONE read until
// Charm_Nest_Rev/placement is raised. The handler raises it only after the op has returned, and laserDone returns only after it has
// written the order timelines, the laser-time records and recounted the Completed tab (every completed sheet ever: seconds at
// thousands of sheets). So other computers saw "cut" / "not cut" that much later than the commit. The counter is raised right after
// the commit now (the handler's own raise after the op stays: a second raise only costs one more full read).
//   node tests/cost/rv1-placement-bump-order.cjs
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const W = path.join(__dirname, '..', '..');
const T = 1.7e12;

(async () => {
  const m = meter.create({ log: true }); m.install();
  const docs = {};
  for (let s = 0; s < 40; s++) {
    const ids = [1, 2, 3].map(k => `sheet-${s}-${k}`);
    docs[`Charm_Nest_Sets/set-${s}`] = { setId: `set-${s}`, sheetIds: ids, laserDoneAt: T + s, status: 'complete', updatedAt: T };
    ids.forEach(id => { docs[`Charm_Nest_Sheets/${id}`] = { id, setId: `set-${s}`, laserDoneAt: T + s, metal: 'gold', archived: false, updatedAt: T, orders: ['4190000001'], poolIds: ['4190000001_41900000011_1'] }; });
  }
  m.db.seed(docs);
  // every document write goes into the meter's log, in order, between the reads
  const rawSet = m.db.docs.set.bind(m.db.docs);
  m.db.docs.set = (k, v) => { m.log.push({ col: 'WRITE ' + k, kind: 'write', n: 1 }); return rawSet(k, v); };
  const fn = require(path.join(W, 'netlify/functions/charmNestLibrary.js'));
  const post = body => fn.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: {}, body: JSON.stringify(body) });
  // the Completed memo is made once, so the undo's recount is the one a press pays
  await post({ op: 'laserDoneList', countOnly: true });
  m.log.length = 0;

  const r = await post({ op: 'laserDone', kind: 'sheet', id: 'sheet-3-2', done: false, by: 'Test' });
  assert.strictEqual(r.statusCode, 200, r.body);
  const at = pred => m.log.findIndex(pred);
  const commit = at(e => e.col === 'WRITE Charm_Nest_Sheets/sheet-3-2');
  const raise = at(e => e.col === 'WRITE Charm_Nest_Rev/placement');
  const recount = m.log.findIndex((e, i) => i > commit && e.col === 'Charm_Nest_Sheets' && e.kind === 'reads' && e.n > 20);
  assert.ok(commit >= 0, 'the undo committed the sheet');
  assert.ok(recount > commit, 'the Completed tab was recounted after the commit (every completed sheet is read)');
  assert.ok(raise >= 0, 'the placement counter was raised');
  assert.ok(raise < recount, `the placement counter must be raised BEFORE the Completed recount (commit at ${commit}, raise at ${raise}, recount at ${recount}): other computers learn of the change when it moves`);
  assert.ok((m.db.docs.get('Charm_Nest_Rev/placement') || {}).n >= 1, 'and it stays raised');
  process.stdout.write(`ok   laserDone raises the placement counter right after its commit (commit ${commit}, raise ${raise}, recount ${recount})\n`);
  // the same for the Completed memo: between the commit and its raise another tab's count refresh still reads the old kept number and keeps it
  // until its next refresh (a minute). Raised before the timeline notes are written, not after.
  const doneRaise = at(e => e.col === 'WRITE Charm_Nest_Rev/done' && true);
  const firstNote = at(e => /^WRITE Order_Timeline\//.test(e.col));
  assert.ok(firstNote > commit, 'the undo wrote timeline notes after the commit (' + firstNote + ')');
  assert.ok(doneRaise > commit && doneRaise < firstNote, `the Completed memo's counter must be raised right after the commit, before the timeline notes (commit ${commit}, done raise ${doneRaise}, first note ${firstNote})`);
  process.stdout.write(`ok   and so is the Completed counter (done raise ${doneRaise}, first timeline note ${firstNote})\n`);
  process.stdout.write('rv1-placement-bump-order: passed\n');
})().catch(e => { process.stdout.write('FAILED: ' + (e && e.stack || e) + '\n'); process.exit(1); });
