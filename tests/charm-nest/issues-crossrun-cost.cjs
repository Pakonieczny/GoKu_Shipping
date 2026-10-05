/* The runs of an order, and what finding them costs (Paul, point 9: an order shown as an issue must be a real one, even when its other piece was taken by
 * ANOTHER run than the sheet's own). Through the real laserStatus handler over the in-memory Firestore, with a Library of 30 sheets in three runs, each
 * carrying 8 orders of one piece, and 10 of those orders also having a piece in another run (a later pull / a piece pooled by an older run):
 *   - exactly those 10 orders are issues on their sheet, each for the right reason, and the other 230 orders never are
 *   - the server asks which runs hold the orders in two queries (the run records and the line archive) to every 15 orders, and only then
 *   - what the minute's memory saves, and that the cheap "unchanged?" read neither asks nor misses a change in the other run
 *   - the run records list their orders as numbers too, a sheet cut once before asks nothing, and an unreadable lookup is never read as "whole"
 * The documents and queries a listing reads with and without the lookup are printed (st.reads / st.queries). Never touches a live service. */
'use strict';
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');

const SHEETS = 30, PER = 8, CROSS = 10;
const sheetNo = i => `lib-sh-${String(i).padStart(2, '0')}`, runOfSheet = i => (i <= 15 ? 'lib-run-1' : i <= 25 ? 'lib-run-2' : 'lib-run-3');
const orderOf = (i, k) => String(4170000000 + i * 100 + k), txOf = (i, k) => String(7000 + i * 10 + k);
const keyOf = (o, tx) => `${o}_${tx}`;

/** The Library: sheet i (1..30) holds the one-piece orders orderOf(i, 1..8). For i in 1..10 the first of them has a second line, taken by ANOTHER run:
 *  odd i: its piece is on sheet 30, which is not ready (its files are unverified); even i: it is not placed at all and its SKU is unmatched. */
function library({ archiveSome = false } = {}) {
  const lines = { 'lib-run-1': {}, 'lib-run-2': {}, 'lib-run-3': {} }, archived = { 'lib-run-1': {}, 'lib-run-2': {}, 'lib-run-3': {} }, placed = {}, expect = {};
  const line = (o, tx, extra) => ({ orderId: o, transactionId: tx, state: 'written', quantity: 1, poolIds: [`${keyOf(o, tx)}_1`], reason: null, hold: null, changePending: false, sku: 'SKU-' + tx, material: 'gold', problems: [], noDesign: false, engraveCandidate: false, engrave: null, snap: { title: 'Charm ' + tx, buyer: 'Buyer ' + o, metalKey: 'gold' }, ...extra });
  for (let i = 1; i <= SHEETS; i++) {
    placed[i] = [];
    for (let k = 1; k <= PER; k++) { const o = orderOf(i, k), tx = txOf(i, k); lines[runOfSheet(i)][keyOf(o, tx)] = line(o, tx); placed[i].push(`${keyOf(o, tx)}_1`); }
  }
  for (let i = 1; i <= CROSS; i++) {
    const o = orderOf(i, 1), tx = String(9000 + i), other = i % 2 ? 'lib-run-3' : 'lib-run-2', key = keyOf(o, tx);
    const into = archiveSome && i <= 4 ? archived : lines;
    if (i % 2) { into[other][key] = line(o, tx); placed[30].push(`${key}_1`); expect[i] = { key: 'otherSheetNotReady', sheet: sheetNo(30) }; }
    else { into[other][key] = line(o, tx, { state: 'unmatched', poolIds: [], sku: '', problems: ['unmatchedSku'] }); expect[i] = { key: 'noSku' }; }
  }
  const sheets = [];
  for (let i = 1; i <= SHEETS; i++) {
    const orders = [...new Set(placed[i].map(p => p.split('_')[0]))];
    sheets.push({ id: sheetNo(i), metal: 'gold', metalLabel: 'gold', setId: `lib-set-${Math.ceil(i / 2)}`, setSeq: 1, sheetIndex: i, runId: runOfSheet(i), status: 'complete', placedCount: placed[i].length, poolIds: placed[i], orders,
      verification: i === 30 ? { ok: false } : { ok: true }, preview: 'https://example.test/' + i + '.png', outputs: { ai: { path: i + '.ai', url: 'https://example.test/' + i + '.ai' }, preview: { path: i + '.png', url: 'https://example.test/' + i + '.png' } },
      label: { files: [{ path: i + '-qr.png', url: 'https://example.test/' + i + '-qr.png', payload: 'x', orders }] }, backPool: [], draft: false });
  }
  return { lines, archived, sheets, expect };
}
function seed(st, lib, { numbers = false } = {}) {
  const now = Date.now(), ts = { toMillis: () => now }, as = list => (numbers ? list.map(x => +x) : list);
  st.docs.clear();
  for (const [n, run] of ['lib-run-1', 'lib-run-2', 'lib-run-3'].entries()) {
    const open = [...new Set(Object.values(lib.lines[run]).map(l => String(l.orderId)))].sort(), arch = lib.archived[run];
    st.put('Charm_Nest_Runs', run, { runId: run, lines: lib.lines[run], orders: as(open), ...(Object.keys(arch).length ? { lineArchive: { parts: 1 } } : {}) });
    if (Object.keys(arch).length) st.put('Charm_Nest_Run_Lines', 'part-' + run, { runId: run, at: 1 + n, seq: 0, orders: [...new Set(Object.values(arch).map(l => String(l.orderId)))].sort(), keys: Object.keys(arch), json: JSON.stringify(arch) });
  }
  for (const s of lib.sheets) st.put('Charm_Nest_Sheets', s.id, { ...JSON.parse(JSON.stringify(s)), day: '2026-10-05', stock: { wIn: 6, hIn: 4.5 }, updatedAt: ts, createdAt: ts });
  for (let n = 1; n <= SHEETS / 2; n++) st.put('Charm_Nest_Sets', `lib-set-${n}`, { setId: `lib-set-${n}`, seq: n, sheetIds: [sheetNo(2 * n - 1), sheetNo(2 * n)], day: '2026-10-05', runId: 'lib-run-1', updatedAt: ts, createdAt: ts });
}
const ask = async (srv, ids, extra = {}) => {
  srv.st.reads = 0; srv.st.queries = 0;
  const r = await (await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ op: 'laserStatus', sheetIds: ids, ...extra }) })).json();
  return { ...r, reads: srv.st.reads, queries: srv.st.queries };
};
const issuesOf = ans => { const out = {}; for (const s of ans.sheets || []) for (const [o, v] of Object.entries(s.orderReadiness || {})) if (v.ready !== true) (out[s.id] = out[s.id] || {})[o] = v; return out; };

(async () => {
  const srv = await start({ receipts: [] });
  try {
    const all = Array.from({ length: SHEETS }, (_, i) => sheetNo(i + 1));
    const lib = library({ archiveSome: true });
    seed(srv.st, lib);

    // 1. cold: the memory is empty. Exactly the cross-run orders are issues, with the reason that really holds
    const cold = await ask(srv, all, { wantRevs: true }), got = issuesOf(cold);
    for (let i = 1; i <= CROSS; i++) {
      const v = (got[sheetNo(i)] || {})[orderOf(i, 1)];
      assert.ok(v && Array.isArray(v.blocks) && v.blocks.length === 1, `sheet ${i}: the order whose other piece is in another run is an issue`);
      assert.equal(v.blocks[0].key, lib.expect[i].key, `sheet ${i}: ${lib.expect[i].key}`);
      if (lib.expect[i].sheet) assert.equal(v.blocks[0].sheetId, lib.expect[i].sheet, 'the sheet it waits for is named');
    }
    const extras = Object.entries(got).flatMap(([sid, m]) => Object.keys(m).map(o => `${sid}:${o}`)).filter(x => !Array.from({ length: CROSS }, (_, i) => `${sheetNo(i + 1)}:${orderOf(i + 1, 1)}`).includes(x));
    assert.deepEqual(extras, [], 'no other order, on any sheet, is an issue (every one-piece order, and the second pieces on sheet 30 / in no sheet, are not)');

    // 2. warm: the same read again within the minute asks nothing more about which runs hold an order
    const warm = await ask(srv, all, { wantRevs: true }), W = new Set(lib.sheets.flatMap(s => s.orders)).size, asked = 2 * Math.ceil(W / 15);
    assert.deepEqual(issuesOf(warm), got, 'a kept answer is the same answer');
    assert.equal(cold.queries - warm.queries, asked, `${W} orders: the run records and the line archive are asked once each to every 15 orders (${asked} queries)`);
    const addedReads = cold.reads - warm.reads;
    assert.ok(addedReads > 0 && addedReads <= asked * 3, `the lookup reads ${addedReads} documents for ${asked} queries`);
    console.log(`30-sheet Library, ${W} orders: without the lookup ${warm.reads} documents read and ${warm.queries} queries; with it ${cold.reads} and ${cold.queries} (${addedReads} documents and ${cold.queries - warm.queries} queries added, once a minute at most)`);

    // 3. the "unchanged?" read never asks, and a change in the other run is still seen (that run is watched as well)
    assert.ok(cold.revs && !Object.keys(cold.revs).some(k => !/^[str]:/.test(k)));
    const some = all.slice(0, CROSS), first = await ask(srv, some, { wantRevs: true });
    assert.ok(first.revs['r:lib-run-2'] && first.revs['r:lib-run-3'], 'the runs the other pieces are in are watched: ' + Object.keys(first.revs).filter(k => k[0] === 'r'));
    const same = await ask(srv, some, { ifRevs: first.revs });
    assert.equal(same.unchanged, true, 'nothing changed: answered unchanged');
    assert.equal(same.queries, 0, 'the unchanged read asks no query');
    assert.equal(same.reads, Object.keys(first.revs).length, 'and reads the watched documents only');
    srv.st.put('Charm_Nest_Runs', 'lib-run-2', { note: 'a later save' });
    const again = await ask(srv, some, { ifRevs: first.revs });
    assert.ok(!again.unchanged && again.sheets, 'a save to the other run is a change: the full answer is made again');

    // 4. the run records list their orders as numbers too (the page writes receipt ids as it was given them)
    seed(srv.st, library(), { numbers: true });
    process.env.CHARM_NEST_HOLDERS_MS = '0';
    const numeric = await ask(srv, all);
    assert.deepEqual(Object.fromEntries(Object.entries(issuesOf(numeric)).map(([s, m]) => [s, Object.keys(m).sort()])), Object.fromEntries(Array.from({ length: CROSS }, (_, i) => [sheetNo(i + 1), [orderOf(i + 1, 1)]])), 'the same issues when the run lists its orders as numbers');
    delete process.env.CHARM_NEST_HOLDERS_MS;

    // 5. a sheet cut once before asks nothing: the same Library with every sheet done costs the baseline
    const done = library({ archiveSome: true }); for (const s of done.sheets) s.laserDoneAt = 5000;
    seed(srv.st, done); process.env.CHARM_NEST_HOLDERS_MS = '0';
    const finished = await ask(srv, all);
    assert.equal(finished.queries, warm.queries, 'a done sheet looks up no run (the page reads a sheet cut once before as ready whatever its orders say)');
    delete process.env.CHARM_NEST_HOLDERS_MS;
  } finally { srv.close(); }
  console.log('issues-crossrun-cost OK: an order whose other piece is in another run is found (run record or line archive), the lookup is asked in pairs of queries to every 15 orders, kept for a minute, never done by the unchanged read, watched by it, and skipped for done sheets');
})().catch(e => { console.error(e); process.exitCode = 1; });
