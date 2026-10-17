// The current flag (COST Oct 2026, charmNestLibrary.js CURRENT_MARK): the Library's Current lists and the Completed count read the
// records flagged `current`, not the newest windows of the whole history, and History by activity reads a window by updatedAt. Checks,
// on a history long enough that the old windows missed the work in progress, that every answer is the one the records give, after the
// one-time backfill and after the writes that change it, and what each read costs. Real handler, the meter's in-memory Firestore.
//   node tests/cost/fc2-current-flag.cjs
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs');
const W = path.join(__dirname, '..', '..');
const meter = require('./meter.cjs');
process.env.N_DONE_SHEETS = '2400';   // (the completed days run to the end of October: the open work, on 6 Oct, is not among the newest 500 sheets by day)
const src = fs.readFileSync(path.join(__dirname, 'fc2-library-cost.cjs'), 'utf8').replace(/\(async \(\) => \{[\s\S]*$/, 'module.exports = { build, DAY };');
const tmp = path.join(__dirname, '.fc2-current.tmp.cjs');
fs.writeFileSync(tmp, src);
const { build } = require(tmp); fs.unlinkSync(tmp);

(async () => {
  const m = meter.create(); m.install(); m.db.seed(build());
  const docs = m.db.docs;
  // records as old data has them: a sheet archived with its laser mark, a cut sheet of a set still open, a sheet of a completed set not marked itself
  docs.get('Charm_Nest_Sheets/sheet-d10').archived = true;
  docs.get('Charm_Nest_Sheets/sheet-c0-1').laserDoneAt = Date.now() - 60000;
  docs.get('Charm_Nest_Sheets/sheet-d2399').laserDoneAt = undefined;
  const fn = require(path.join(W, 'netlify/functions/charmNestLibrary.js'));
  const post = async body => { const r = await fn.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: {}, body: JSON.stringify(body) }); assert.strictEqual(r.statusCode, 200, r.body.slice(0, 300)); return JSON.parse(r.body); };
  const cost = async f => { const a = m.snapshot(); const out = await f(); const d = m.since(a); return { out, reads: d.reads + d.aggs, writes: d.writes }; };
  const ok = s => process.stdout.write('ok   ' + s + '\n');
  const all = c => [...docs.entries()].filter(([k]) => k.startsWith(c + '/')).map(([k, v]) => [k.split('/')[1], v]);
  // what the records say (filingRecords + Readiness.filed, written out)
  const truth = () => {
    const sets = new Map(all('Charm_Nest_Sets'));
    const pending = s => !!(s.setId && !s.draft && s.solidIncluded !== false && !(+(sets.get(s.setId) || {}).laserDoneAt > 0));
    const filed = s => +s.laserDoneAt > 0 && !pending(s), sheets = all('Charm_Nest_Sheets');
    return { sheets: sheets.filter(([, s]) => !s.archived && !filed(s)).map(([id]) => id).sort(), sets: [...sets].filter(([, s]) => !(+s.laserDoneAt > 0)).map(([id]) => id).sort(),
      counts: { sheets: sheets.filter(([, s]) => !s.archived && filed(s)).length, sets: [...sets.values()].filter(s => +s.laserDoneAt > 0).length },
      // the invariant: every current record, and every archived one with a mark, carries the flag
      unflagged: [...sheets.filter(([, s]) => (!s.archived && !filed(s)) || (s.archived && +s.laserDoneAt > 0)), ...[...sets].filter(([, s]) => !(+s.laserDoneAt > 0))].filter(([, s]) => s.current !== true).map(([id]) => id) };
  };
  const check = async (name, maxReads) => {
    const t = truth();
    const ls = await cost(() => post({ op: 'listSheets', limit: 500, excludeDone: true }));
    assert.deepStrictEqual(ls.out.sheets.map(s => s.id).sort(), t.sheets, name + ': listSheets Current is every current sheet');
    const sl = await cost(() => post({ op: 'setList', includeSheets: true, limit: 200, excludeDone: true }));
    assert.deepStrictEqual(sl.out.sets.map(s => s.setId).sort(), t.sets, name + ': setList is every set not completed');
    docs.delete('Charm_Nest_Rev/done');   // (the kept count, so it is counted here)
    const c = await cost(() => post({ op: 'laserDoneList', countOnly: true }));
    assert.deepStrictEqual(c.out.counts, t.counts, name + ': the Completed count');
    assert.deepStrictEqual(truth().unflagged, [], name + ': every current record is flagged');
    if (maxReads) for (const [k, x] of [['listSheets', ls], ['setList', sl], ['count', c]]) assert.ok(x.reads <= maxReads[k], `${name}: ${k} ${x.reads} reads, at most ${maxReads[k]}`);
    ok(`${name}: ${t.sheets.length} current sheets, ${t.sets.length} sets, count ${JSON.stringify(t.counts)} — reads listSheets ${ls.reads}, setList ${sl.reads}, count ${c.reads}`);
  };

  // 1. before any flag: the first read flags what may be current, once, then answers from the flags
  assert.ok(!docs.has('Charm_Nest_Rev/currentFlag'));
  const first = await cost(() => post({ op: 'listSheets', limit: 500, excludeDone: true }));
  assert.ok(docs.has('Charm_Nest_Rev/currentFlag'), 'the backfill wrote its marker');
  assert.ok(first.out.sheets.some(s => s.id === 'sheet-c0-1') && first.out.sheets.some(s => s.id === 'sheet-c5-2'), 'the open work, older than the newest 500 sheets by day, is listed (the old window missed it)');
  process.stdout.write(`     one-time backfill ${first.reads} reads, ${first.writes} writes\n`);
  const LIMITS = { listSheets: 200, setList: 150, count: 100 };
  await check('1 after the backfill', LIMITS);
  const again = await cost(() => post({ op: 'listSheets', limit: 500, excludeDone: true }));
  assert.strictEqual(again.writes, 0, 'a read with nothing stale writes nothing');

  // 2. a completion taken back: the set's other cut sheets wait for it again (flagged in the same commit)
  await post({ op: 'laserDone', kind: 'sheet', id: 'sheet-d4', done: false, by: 'Test' });
  assert.ok(docs.get('Charm_Nest_Sheets/sheet-d3').current === true && docs.get('Charm_Nest_Sheets/sheet-d5').current === true, 'the set\'s other sheets are flagged with the undo');
  await check('2 laserDone undo', LIMITS);
  // 3. a new sheet saved, and a sheet restored to the open work by a save
  await post({ op: 'putSheet', sheet: { id: 'sheet-new-1', metal: 'gold', day: '2026-10-07', status: 'complete' } });
  await check('3 putSheet', LIMITS);
  // 4. a new set
  const st = await post({ op: 'setAllocate', day: '2026-10-07', runId: 'run-open-0', group: 'gold' });
  assert.strictEqual(docs.get('Charm_Nest_Sets/' + st.setId).current, true);
  await check('4 setAllocate', LIMITS);
  // 5. stale flags (a record flagged that is not current, as a write elsewhere can leave one): listed by nobody, and cleared by the read
  docs.get('Charm_Nest_Sheets/sheet-d20').current = true; docs.get('Charm_Nest_Sets/set-2026-09-01-11').current = true;
  await check('5 stale flags');
  assert.strictEqual(docs.get('Charm_Nest_Sheets/sheet-d20').current, false, 'the stale sheet flag was cleared');
  assert.strictEqual(docs.get('Charm_Nest_Sets/set-2026-09-01-11').current, false, 'the stale set flag was cleared');
  // 6. the sandbox reads as before (no flag, no marker of its own)
  const sb = await post({ op: 'listSheets', limit: 500, excludeDone: true, sandbox: true });
  assert.deepStrictEqual(sb.sheets, []); ok('6 the sandbox reads without the flag');

  // 7. History by activity: a window by updatedAt. The pages, read one after another, are the same groups in the same order whatever
  // the page size, newest first, each group once; and a page reads a fraction of the three collections
  const sizes = ['Charm_Nest_Sets', 'Charm_Nest_Sheets', 'Charm_Nest_Runs'].reduce((n, c) => n + all(c).length, 0);
  const pages = async (limit, direction) => {
    const keys = []; let cursor = null, reads = [];
    for (let i = 0; i < 4; i++) { const r = await cost(() => post({ op: 'history', sort: 'activity', limit, direction, cursor })); reads.push(r.reads); keys.push(...r.out.sets.map(g => [g.key, require(path.join(W, 'charm-nest-activity.js')).at(g)])); cursor = r.out.next; if (!cursor) break; }
    return { keys, reads };
  };
  for (const direction of ['desc', 'asc']) {
    const a = await pages(60, direction), b = await pages(30, direction), n = Math.min(a.keys.length, b.keys.length);
    assert.ok(n >= 100, 'pages were read: ' + n);
    assert.deepStrictEqual(a.keys.slice(0, n).map(k => k[0]), b.keys.slice(0, n).map(k => k[0]), direction + ': the same groups in the same order at either page size');
    assert.strictEqual(new Set(a.keys.map(k => k[0])).size, a.keys.length, direction + ': each group once');
    // (by the activity a group shows, the order the whole read gave: groups without a time last)
    const t = a.keys.map(k => k[1]).filter(x => x > 0); for (let i = 1; i < t.length; i++) assert.ok(direction === 'desc' ? t[i] <= t[i - 1] + 1 : t[i] >= t[i - 1] - 1, direction + ' order at ' + i);
    assert.ok(a.reads[0] < sizes / 3, `${direction}: a page reads ${a.reads[0]}, the three collections hold ${sizes}`);
    ok(`7 history ${direction}: ${a.keys.length} groups in ${a.reads.length} pages, reads per page ${a.reads.join(', ')} (the whole read was ${sizes})`);
  }
  process.stdout.write('fc2-current-flag: passed\n');
  m.uninstall(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
