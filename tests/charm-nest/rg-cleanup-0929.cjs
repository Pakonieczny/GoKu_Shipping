// The one-off cleanup of RG 14/20 · Set 1 · Sheet 1 (charmNestLibrary rgCleanup0929; Paul, 29 Sep 03:58 UTC, "Clean it
// up"): the later lion copy and green line 2 come off, line 1 and everything else stay. Runs against the fake cloud
// (bridge-server.cjs, no network), seeded with the records as they were read from the live sandbox workspace at
// 2026-09-29T04:19:03.982Z (the backup folder; its download links carry token=REDACTED). Delete with the op.
//   node tests/charm-nest/rg-cleanup-0929.cjs [backup dir]   (default /mnt/project-files/backups/rg-cleanup-2026-09-29)
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const B = process.argv[2] || process.env.RG_BACKUP || '/mnt/project-files/backups/rg-cleanup-2026-09-29';
if (!fs.existsSync(path.join(B, 'sheet.json'))) { console.log(`rg-cleanup-0929: skipped, no backup at ${B}`); process.exit(0); }
const { start, Timestamp } = require('./bridge-server.cjs');
const RoseStock = require('../../netlify/functions/_charmNestRoseStock.js');
const read = f => JSON.parse(fs.readFileSync(path.join(B, f), 'utf8')).response;
const clone = v => JSON.parse(JSON.stringify(v));
const SHEET = 'rose-mul8mkas-azxcg5mkas', ORDER = '4176537942', KEEP = '4176537942_5219250436_1', DROP = '4176537942_5219250436_2';
const KEEP_C = 'cust:16e926a18c4cdb9d:4176537942_5219250436_1', DROP_C = 'cust:16e926a18c4cdb9d:4176537942_5219250436_2';
const SET = 'set-2026-09-28-1', STOCK = 'rgs-28f9597b-1002-4480-8ea6-8f577eeb0f89', RUN = 'run-2026-09-28-79ij2aj3rd', ID = 'rg-cleanup-2026-09-29';
const S = n => 'Sandbox_' + n, TS = v => (typeof v === 'number' ? Timestamp.fromMillis(v) : v);
// a copy of a stored record that keeps its times as Timestamps
const copy = v => (v instanceof Timestamp ? new Timestamp(v._seconds, v._nanoseconds) : Array.isArray(v) ? v.map(copy) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, copy(x)])) : v);

const backup = { sheet: read('sheet.json').sheet, pools: read('pools-on-sheet.json').pools, set: read('set.json').set, stock: read('stock.json').stock, timeline: read('timeline-order-4176537942.json') };
// the records as Firestore keeps them: times as Timestamps, no readiness (a read adds it), links as they were saved
function seed(st) {
  const sheet = clone(backup.sheet); delete sheet.engraving; delete sheet.laser;
  sheet.updatedAt = TS(sheet.updatedAt); sheet.createdAt = TS(sheet.createdAt);
  // a stored link carries the token it was saved with, and a charm may have no thumbnail link: the check reads neither
  for (const c of sheet.charms) { if (c.aiUrl) c.aiUrl = c.aiUrl.replace('token=REDACTED', 'token=saved-earlier'); if (!c.thumbUrl) delete c.thumbUrl; }
  st.put(S('Charm_Nest_Sheets'), SHEET, sheet, false);
  for (const p of clone(backup.pools)) st.put(S('Charm_Pool'), p.poolId, Object.assign(p, { updatedAt: TS(p.updatedAt), createdAt: TS(p.createdAt) }), false);
  const set = clone(backup.set); set.updatedAt = TS(set.updatedAt); set.createdAt = TS(set.createdAt);
  st.put(S('Charm_Nest_Sets'), SET, set, false);
  const stock = clone(backup.stock); stock.updatedAt = new Timestamp(stock.updatedAt._seconds, stock.updatedAt._nanoseconds);
  st.put(S('Charm_Nest_Rose_Stock'), STOCK, stock, false);
  // the order's recorded history (the derived events come from the records above)
  for (const e of backup.timeline.events.filter(e => !e.derived)) { const d = clone(e); delete d.id; delete d.derived; st.put(S('Order_Timeline'), e.id, Object.assign(d, { createdAt: Timestamp.fromMillis(e.at) }), false); }
  // the run's lines of the sheet's pieces. (Live, two of them still wait on back-engraving approval; here none needs
  // engraving, so the sheet can reach its cut in this test.)
  const lines = {}; for (const p of backup.pools) { const k = p.poolId.replace(/_\d+$/, ''); (lines[k] = lines[k] || { poolIds: [], engrave: { needed: false, state: 'none', approved: true }, state: 'written' }).poolIds.push(p.poolId); }
  st.put(S('Charm_Nest_Runs'), RUN, { runId: RUN, status: 'processed', step: 'complete', setId: SET, lines, updatedAt: Timestamp.fromMillis(1790647335055) }, false);
}
const snapshot = st => JSON.stringify([...st.docs.entries()].sort(([a], [b]) => (a < b ? -1 : 1)));

async function main() {
  const srv = await start();
  const { st } = srv, lib = st.handlers.charmNestLibrary;
  const call = body => fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(Object.assign({ sandbox: true }, body)) }).then(async r => ({ status: r.status, body: await r.json() }));
  try {
    seed(st);
    const original = copy(st.doc(S('Charm_Nest_Sheets'), SHEET)), stored = st.doc(S('Charm_Nest_Sheets'), SHEET);

    // 1. the check reads a stored record and a read op's answer alike: the digest of the backup's files is the op's own
    const fromFiles = lib.rgDigest0929({ sheet: backup.sheet, pools: Object.fromEntries(backup.pools.map(p => [p.poolId, p])), set: backup.set, stock: backup.stock });
    let before = snapshot(st);
    let r = await call({ op: 'rgCleanup0929' });
    assert.equal(r.status, 200, JSON.stringify(r.body).slice(0, 400));
    assert.equal(r.body.dryRun, true); assert.equal(r.body.matchesBackup, true);
    assert.deepEqual(r.body.digest, fromFiles, 'a stored record and getSheet/poolList/setGet/roseGet answers give one digest');
    // ...and so does the live site's answer read now, through getSheet, as the backup was
    const got = (await call({ op: 'getSheet', id: SHEET })).body.sheet;
    assert.equal(lib.rgDigest0929({ sheet: got, pools: Object.fromEntries(backup.pools.map(p => [p.poolId, p])), set: backup.set, stock: backup.stock }).sheet, fromFiles.sheet);

    // 2. the dry run: the exact before and after, nothing written
    const dry = r.body;
    assert.equal(snapshot(st), before, 'a dry run writes nothing');
    assert.equal(dry.before.sheet.placements.length, 9); assert.equal(dry.after.sheet.placements.length, 8);
    assert.deepEqual(dry.after.sheet.placements, backup.sheet.placements.filter(p => p.id !== DROP_C), 'the other eight placements byte for byte');
    assert.deepEqual(dry.after.sheet.poolIds, backup.sheet.poolIds.filter(id => id !== DROP));
    assert.deepEqual(dry.after.sheet.charms.map(c => c.id), backup.sheet.charms.map(c => c.id).filter(id => id !== DROP_C));
    assert.equal(dry.after.sheet.charmCount, 8); assert.equal(dry.after.sheet.placedCount, 8);
    assert.equal(dry.after.sheet.names, backup.sheet.names.replace(' 4176537942 · Custom · Test_Charm · 2/2', ''));
    assert.equal(dry.before.sheet.rosePlanJson, backup.sheet.rosePlanJson); assert.equal(dry.after.sheet.rosePlanJson, null);
    assert.equal(dry.after.sheet.rosePlanHash, null); assert.equal(dry.after.sheet.roseFingerprint, null); assert.equal(dry.after.sheet.dirty, true);
    assert(!('roseProtectedJson' in dry.after.sheet), 'line 1 is not written');
    assert(dry.after.sheet.freePt2 > backup.sheet.freePt2 && dry.after.sheet.density < backup.sheet.density, 'the fill goes down by the lion');
    assert.equal(dry.after.sheet.freePt2, Math.round(38849 - (38849 - 32734) * (1 - 816.9 / 4927.9)));
    assert.deepEqual(dry.after.dropPool, Object.assign({ state: 'abandoned', sheetId: null, setId: null }, { cleanup: dry.after.dropPool.cleanup }));
    assert.deepEqual(dry.after.setOrder.lines[0].copies.map(c => c.poolId), [KEEP]);
    assert.deepEqual(dry.before.setOrder, backup.set.orders[ORDER]);
    assert.equal(dry.note.type, 'note'); assert.match(dry.note.text, /^Extra copy removed from RG 14\/20 Sheet 1 \(it was placed twice by mistake\); line 2 removed \(it was added without Cut Sheet\)$/);
    assert.equal(dry.writes.length, 5);

    // 3. what it refuses before it writes anything
    before = snapshot(st);
    r = await call({ op: 'rgCleanup0929', sandbox: false }); assert.equal(r.status, 400); assert.match(r.body.error, /sandbox/);
    r = await call({ op: 'rgCleanup0929', sheetId: 'rose-other-sheet' }); assert.equal(r.status, 400); assert.match(r.body.error, /works only on sheet/);
    r = await call({ op: 'rgCleanup0929', dryRun: false }); assert.equal(r.status, 400); assert.match(r.body.error, /confirm/);
    r = await call({ op: 'rgCleanup0929', dryRun: false, confirm: 'yes' }); assert.equal(r.status, 400);
    assert.equal(snapshot(st), before);
    // a record that changed since the backup: a page saved the sheet, the set moved on, the stock was cut, a pool row changed
    const changes = [
      ['sheet', () => { stored.updatedAt = Timestamp.fromMillis(1790650000000); }, () => { stored.updatedAt = copy(original.updatedAt); }],
      ['sheet', () => { stored.placements[0] = Object.assign({}, stored.placements[0], { cxPt: 12.5 }); }, () => { stored.placements = copy(original.placements); }],
      ['set', () => { st.doc(S('Charm_Nest_Sets'), SET).status = 'ready'; }, () => { st.doc(S('Charm_Nest_Sets'), SET).status = backup.set.status; }],
      ['stock', () => { st.doc(S('Charm_Nest_Rose_Stock'), STOCK).revision = 1; }, () => { st.doc(S('Charm_Nest_Rose_Stock'), STOCK).revision = 0; }],
      ['pools', () => { st.doc(S('Charm_Pool'), DROP).state = 'engraved'; }, () => { st.doc(S('Charm_Pool'), DROP).state = 'written'; }]
    ];
    for (const [what, change, undo] of changes) {
      change(); const was = snapshot(st);
      for (const body of [{ op: 'rgCleanup0929' }, { op: 'rgCleanup0929', dryRun: false, confirm: ID }]) {
        r = await call(body); assert.equal(r.status, 409, what); assert.deepEqual(r.body.changed, [what]); assert.match(r.body.error, /^Nothing was changed/);
      }
      assert.equal(snapshot(st), was, `nothing is written when the ${what} changed`); undo();
    }
    assert.equal(snapshot(st), before);

    // 4. the change, once
    r = await call({ op: 'rgCleanup0929', dryRun: false, confirm: ID, by: 'test' });
    assert.equal(r.status, 200, JSON.stringify(r.body).slice(0, 400)); assert.equal(r.body.done, true);
    const sheet = st.doc(S('Charm_Nest_Sheets'), SHEET);
    assert.equal(sheet.roseProtectedJson, original.roseProtectedJson, 'line 1 byte for byte');
    assert.deepEqual(sheet.placements, original.placements.filter(p => p.id !== DROP_C));
    assert.deepEqual(sheet.charms, original.charms.filter(c => c.id !== DROP_C));
    assert.equal(sheet.rosePlanJson, null); assert.equal(sheet.dirty, true); assert.equal(sheet.cleanup.id, ID); assert.deepEqual(sheet.cleanup.removedPoolIds, [DROP]);
    for (const [k, v] of Object.entries(original)) if (!(k in dry.after.sheet) && k !== 'updatedAt') assert.deepEqual(sheet[k], v, `sheet ${k} is kept`);
    assert.deepEqual(Object.keys(sheet).sort(), [...new Set([...Object.keys(original), 'cleanup'])].sort());
    const dropRow = st.doc(S('Charm_Pool'), DROP), origDrop = backup.pools.find(p => p.poolId === DROP);
    assert.equal(dropRow.state, 'abandoned'); assert.equal(dropRow.sheetId, null); assert.equal(dropRow.setId, null); assert.equal(dropRow.cleanup.fromSheetId, SHEET);
    for (const k of Object.keys(origDrop)) if (!['state', 'sheetId', 'setId', 'updatedAt', 'createdAt'].includes(k)) assert.deepEqual(dropRow[k], origDrop[k], `pool ${k} is kept`);
    for (const p of backup.pools.filter(p => p.poolId !== DROP)) assert.equal(st.doc(S('Charm_Pool'), p.poolId).updatedAt.toMillis(), p.updatedAt, `${p.poolId} is not written`);
    const set = st.doc(S('Charm_Nest_Sets'), SET);
    assert.deepEqual(set.orders[ORDER].lines[0].copies, [backup.set.orders[ORDER].lines[0].copies[0]]);
    for (const [k, o] of Object.entries(backup.set.orders)) if (k !== ORDER) assert.deepEqual(set.orders[k], o, `set order ${k} is kept`);
    assert.deepEqual(set.sheetIds, backup.set.sheetIds); assert.equal(set.status, backup.set.status);
    assert.deepEqual(st.doc(S('Charm_Nest_Rose_Stock'), STOCK).revision, 0); assert.equal(st.doc(S('Charm_Nest_Rose_Stock') + '/' + STOCK + '/cuts', SHEET), undefined);
    // the backup of what it changed, written first, with no download token in it
    const kept = st.doc(S('Charm_Nest_Cleanup_Backups'), ID);
    assert(kept && kept.before && kept.by === 'test');
    const keptSheet = JSON.parse(kept.before.sheet);
    assert.equal(keptSheet.rosePlanJson, original.rosePlanJson); assert.deepEqual(keptSheet.placements, original.placements); assert.deepEqual(keptSheet.updatedAt, { ms: original.updatedAt.toMillis() });
    assert.deepEqual(JSON.parse(kept.before.set).orders[ORDER], backup.set.orders[ORDER]);
    assert.equal(JSON.parse(kept.before.pools[DROP]).state, 'written');
    assert(!/token=(?!REDACTED)/.test(JSON.stringify(kept)), 'no download token in the backup');
    // the timeline: the note, and every recorded event as it was
    const note = st.doc(S('Order_Timeline'), `${ORDER}~note~${ID}`);
    assert.equal(note.type, 'note'); assert.equal(note.sheetId, SHEET); assert.equal(note.by, 'test');
    for (const e of backup.timeline.events.filter(e => !e.derived)) assert.equal(st.doc(S('Order_Timeline'), e.id).text, e.text, `${e.id} is kept`);
    const tl = (await call({ op: 'timelineGet', orderId: ORDER })).body;
    assert(tl.events.some(e => e.type === 'note' && /Extra copy removed/.test(e.text)));
    assert(tl.events.some(e => e.id === `${ORDER}~roseLine~${SHEET}-L2-1790645103734`), 'line 2 stays in the history');
    assert.equal(tl.where.stage, 'sheet'); assert.equal(tl.where.sheetId, SHEET);
    // read back as the page reads it: 8 pieces, not ready until Cut Sheet
    const back = (await call({ op: 'getSheet', id: SHEET })).body.sheet;
    assert.equal(back.laser.total, 8); assert.equal(back.laser.stages.layout, false);

    // 5. a second run, dry or not, is refused and writes nothing
    before = snapshot(st);
    for (const body of [{ op: 'rgCleanup0929', dryRun: false, confirm: ID }, { op: 'rgCleanup0929' }]) { r = await call(body); assert.equal(r.status, 409); assert.equal(r.body.done, true); assert.match(r.body.error, /already ran/); }
    assert.equal(snapshot(st), before);

    // 6. a page that still shows the old sheet cannot put the lion back, or move line 1
    const page = () => { const d = clone(st.doc(S('Charm_Nest_Sheets'), SHEET)); for (const k of ['rosePlanJson', 'rosePlanHash', 'roseFingerprint', 'roseProtectedJson', 'roseStockId', 'roseRevision', 'cleanup', 'updatedAt', 'createdAt']) delete d[k]; return d; };
    const stale = Object.assign(page(), { placements: original.placements, charms: original.charms, poolIds: original.poolIds, charmCount: 9, placedCount: 9, dirty: false });
    r = await call({ op: 'putSheet', sheet: stale }); assert.equal(r.status, 409); assert.match(r.body.error, /taken off this sheet on purpose.*Reload/);
    r = await call({ op: 'putSheet', sheet: { id: SHEET, poolIds: original.poolIds } }); assert.equal(r.status, 409);
    const moved = page(); moved.placements = moved.placements.map(p => (p.id === original.placements[0].id ? Object.assign({}, p, { cxPt: p.cxPt + 3 }) : p));
    r = await call({ op: 'putSheet', sheet: moved }); assert.equal(r.status, 500); assert.match(r.body.error, /protected Rose Gold layout/);
    assert.equal(snapshot(st), before, 'refused saves write nothing');
    // a set saved from that page keeps the second copy off
    r = await call({ op: 'setUpdate', setId: SET, patch: { orders: backup.set.orders, status: backup.set.status } }); assert.equal(r.status, 200);
    assert.deepEqual(st.doc(S('Charm_Nest_Sets'), SET).orders[ORDER].lines[0].copies.map(c => c.poolId), [KEEP]);
    assert.deepEqual(st.doc(S('Charm_Nest_Sets'), SET).orders['4171711853'], backup.set.orders['4171711853']);

    // 7. the page nests again (line 1 locked; the lion left may move) and saves: accepted
    const lion = original.placements.find(p => p.id === KEEP_C);
    const nudged = page(); nudged.dirty = false; nudged.placements = nudged.placements.map(p => (p.id === KEEP_C ? Object.assign({}, p, { cxPt: p.cxPt + 0.5 }) : p));
    r = await call({ op: 'putSheet', sheet: nudged }); assert.equal(r.status, 200, JSON.stringify(r.body));
    const resaved = page(); resaved.dirty = false; resaved.placements = resaved.placements.map(p => (p.id === KEEP_C ? Object.assign({}, lion) : p));
    r = await call({ op: 'putSheet', sheet: resaved }); assert.equal(r.status, 200, JSON.stringify(r.body));
    const now = st.doc(S('Charm_Nest_Sheets'), SHEET);
    assert.equal(now.dirty, false); assert.equal(now.roseProtectedJson, original.roseProtectedJson); assert.equal(now.cleanup.id, ID, 'the mark stays');

    // 8. Cut Sheet: only it adds a line, around the lion left; line 1 stays as it was
    const plan0 = JSON.parse(original.rosePlanJson), guard = JSON.parse(original.roseProtectedJson);
    const shapesJson = JSON.stringify([...guard.shapes, plan0.shapes.find(s => s.id === KEEP_C)]);
    const args = { op: 'rosePlan', sheetId: SHEET, stockId: STOCK, revision: 0, fingerprint: RoseStock.fingerprint(now), shapesJson, allowanceMm: plan0.allowanceMm };
    r = await call(args); assert.equal(r.status, 500); assert.match(r.body.error, /Only Cut Sheet adds a green line/);
    r = await call(Object.assign({}, args, { cut: true })); assert.equal(r.status, 200, JSON.stringify(r.body).slice(0, 300));
    const plan = JSON.parse(r.body.planJson);
    assert.equal(plan.stages.length, 2);
    assert.deepEqual(plan.stages[0], guard.stages[0], 'line 1 keeps its date and charms');
    assert.deepEqual(plan.lines[0], guard.lines[0], 'line 1 byte for byte');
    assert.deepEqual(plan.stages[1].ids, [KEEP_C]); assert(plan.stages[1].at > plan0.stages[1].at, 'a new line 2, dated at the press');
    r = await call({ op: 'roseRecordCut', sheetId: SHEET, stockId: STOCK, revision: 0, planHash: r.body.planHash, by: 'test' });
    assert.equal(r.status, 200, JSON.stringify(r.body).slice(0, 300)); assert.equal(r.body.stock.revision, 1);
    assert(st.doc(S('Charm_Nest_Sheets'), SHEET).roseCutAt > 0);
    console.log('rg-cleanup-0929: ok (dry run, refusals, the change once, stale page, re-nest, Cut Sheet)');
  } finally { srv.close(); }
}
main().catch(e => { console.error(e); process.exit(1); });
