/* LOCAL (Paul, 28 Oct 2026: "as responsive as Photoshop … leverage local storage … keep the API calls to the bare minimum"): the cloud's side of the
   records kept on this computer (charmNestLibrary.js LOCAL_READS), the real handler over the in-memory Firestore of the fixture server (its update
   times move with every write, st.reads counts the documents read as Firestore bills them). No network, no live record, no credential.
     A · getSheet { ifRev }: unchanged → { unchanged, rev } under 1 KB for ONE read; a write by anyone → the whole record with its new rev; gone → { sheet: null }
     B · listSheets { ifRev, since }: nothing moved → one read; since → only the rows whose record, set or run was written, every row named in ids, a deleted one gone
     C · setList { ifRev, since, includeSheets }: likewise, a changed set with its sheets
     D · getMany: several records in one call, those at the page's revision left out, the missing ones named
     E · a page that sends none of these is answered exactly as before (no new keys)
   node tests/charm-nest/local-reads.cjs */
'use strict';
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs'), { seed } = require('./laser-workflow.cjs');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';

(async () => {
  const srv = await start({ receipts: [] }); seed(srv); const { st } = srv;
  // a draft sheet of the same run, in no set, and a sheet record of a real size (its charms with their outlines are most of a record's bytes)
  const charms = Array.from({ length: 40 }, (_, i) => ({ id: 'c' + i, name: 'Charm ' + i, hash: 'h' + i, outline: Array.from({ length: 60 }, (_, j) => [i + j / 7, j * 1.5]) }));
  st.put(S, 'draft-sheet', { id: 'draft-sheet', runId: 'run-fixture', metal: 'gold', draft: true, day: '2026-09-30', status: 'partial', placedCount: 40, charmCount: 40, stock: { wIn: 6, hIn: 4.5 }, poolIds: [], orders: ['3700000999'], charms, placements: charms.map((c, i) => ({ id: c.id, cxPt: i, cyPt: i, angle: 0 })) });
  const call = async b => {
    const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) });
    const text = await r.text(); return Object.assign({ status: r.status, bytes: Buffer.byteLength(text) }, JSON.parse(text));
  };
  const reads = async b => { st.reads = 0; const r = await call(b); return Object.assign(r, { reads: st.reads }); };
  const out = [];
  try {
    /* ── A · getSheet ── */
    const full = await reads({ op: 'getSheet', id: 'draft-sheet' });
    assert(full.sheet && full.sheet.id === 'draft-sheet' && full.sheet.charms.length === 40 && typeof full.rev === 'string' && full.rev !== '0', 'a whole answer carries its revision: ' + full.rev);
    const same = await reads({ op: 'getSheet', id: 'draft-sheet', ifRev: full.rev });
    assert.deepEqual(Object.keys(same).sort(), ['bytes', 'reads', 'rev', 'status', 'unchanged']);
    assert(same.unchanged === true && same.rev === full.rev, 'the record held at its revision: unchanged');
    assert(same.reads === 1 && same.bytes < 1024, `an unchanged getSheet is one read under 1 KB: ${same.reads} read(s), ${same.bytes} B`);
    out.push(`A · getSheet: whole ${full.bytes} B / ${full.reads} reads; unchanged ${same.bytes} B / ${same.reads} read (gate: < 1 KB at 1 read)`);
    st.put(S, 'draft-sheet', { placedCount: 39 });   // (another computer's write)
    const moved = await reads({ op: 'getSheet', id: 'draft-sheet', ifRev: full.rev });
    assert(!moved.unchanged && moved.sheet.placedCount === 39 && moved.rev !== full.rev, 'a write by anyone: the whole record, with its new revision');
    assert((await call({ op: 'getSheet', id: 'draft-sheet', ifRev: moved.rev, verify: true })).sheet, 'verify skips the shortcut');
    assert.deepEqual(await call({ op: 'getSheet', id: 'no-such-sheet', ifRev: full.rev }).then(r => r.sheet), null, 'a sheet gone: { sheet: null }');

    /* ── B · listSheets ── */
    const ask = { op: 'listSheets', limit: 500, excludeDone: true };
    const plain = await call(ask);
    assert.deepEqual(Object.keys(plain).sort(), ['bytes', 'next', 'sheets', 'status', 'truncated'], 'E · without the new fields the answer is what it was');
    const first = await reads(Object.assign({ wantRev: true, since: '0.0' }, ask));
    assert.equal(first.sheets.length, 4); assert.deepEqual(first.ids.slice().sort(), ['cut-sheet', 'draft-sheet', 'pending-sheet', 'ready-sheet']);
    assert(first.rev && first.upTo, 'the answer names the Library revision and how far it saw');
    const quiet = await reads(Object.assign({ ifRev: first.rev, since: first.upTo }, ask));
    assert(quiet.unchanged === true && quiet.sheets.length === 0 && quiet.next === null && quiet.reads === 1, `nothing the Library reads moved: one read (${quiet.reads}, ${quiet.bytes} B)`);
    const none = await reads(Object.assign({ since: first.upTo }, ask));
    assert(none.sheets.length === 0 && none.ids.length === 4 && none.upTo === first.upTo, 'since, nothing written: every row named, none sent');
    out.push(`B · listSheets: whole ${first.bytes} B / ${first.reads} reads; unchanged (ifRev) ${quiet.bytes} B / ${quiet.reads} read; since with nothing new ${none.bytes} B / ${none.reads} reads`);
    st.put(S, 'pending-sheet', { verification: { ok: false } });
    let d = await call(Object.assign({ since: first.upTo }, ask));
    assert.deepEqual(d.sheets.map(s => s.id), ['pending-sheet'], 'a sheet written since: that row alone'); assert.equal(d.ids.length, 4);
    let since = d.upTo;
    st.put(SET, 'set-fixture', { status: 'again' });
    d = await call(Object.assign({ since }, ask));
    assert.deepEqual(d.sheets.map(s => s.id).sort(), ['cut-sheet', 'pending-sheet', 'ready-sheet'], 'its set written: the set\'s sheets'); since = d.upTo;
    st.put(RUN, 'run-fixture', { touched: 1 });
    d = await call(Object.assign({ since }, ask));
    assert.equal(d.sheets.length, 4, 'their run written: every sheet of the run'); since = d.upTo;
    st.docs.delete(S + '/draft-sheet');
    d = await call(Object.assign({ since }, ask));
    assert(d.sheets.length === 0 && !d.ids.includes('draft-sheet') && d.ids.length === 3, 'a deleted sheet is gone from ids'); since = d.upTo;
    // (a later part of a long list reads by id as before and asks nothing)
    const part = await call(Object.assign({ since, ifRev: first.rev, cursor: { sheets: ['ready-sheet'] } }, ask));
    assert(part.sheets.length === 1 && !('ids' in part) && !('unchanged' in part), 'a cursor part is read by id');

    /* ── C · setList ── */
    const sl = { op: 'setList', includeSheets: true, limit: 200, excludeDone: true };
    const plainSets = await call(sl);
    assert.deepEqual(Object.keys(plainSets).sort(), ['bytes', 'next', 'sets', 'sheets', 'status', 'truncated'], 'E · setList keeps its shape');
    const s0 = await call(Object.assign({ wantRev: true, since: '0.0' }, sl));
    assert(s0.sets.length === 1 && s0.sheets.length === 3 && s0.ids.length === 1 && s0.rev && s0.upTo);
    const sq = await reads(Object.assign({ ifRev: s0.rev, since: s0.upTo }, sl));
    assert(sq.unchanged && sq.sets.length === 0 && sq.sheets.length === 0 && sq.reads === 1, 'setList unchanged: one read');
    const sn = await call(Object.assign({ since: s0.upTo }, sl));
    assert(sn.sets.length === 0 && sn.sheets.length === 0 && sn.ids.length === 1, 'since, no set written: none sent');
    st.put(SET, 'set-fixture', { status: 'labelled' });
    const sc = await call(Object.assign({ since: s0.upTo }, sl));
    assert(sc.sets.length === 1 && sc.sheets.length === 3, 'the set written since: it and its sheets');
    out.push(`C · setList: whole ${s0.bytes} B; unchanged ${sq.bytes} B / ${sq.reads} read; since with nothing new ${sn.bytes} B`);

    /* ── D · getMany ── */
    const m0 = await reads({ op: 'getMany', keys: ['s:ready-sheet', 's:cut-sheet', 't:set-fixture', 'r:run-fixture', 's:no-such-sheet'], full: true });
    assert.deepEqual(Object.keys(m0.recs).sort(), ['r:run-fixture', 's:cut-sheet', 's:ready-sheet', 't:set-fixture']); assert.deepEqual(m0.gone, ['s:no-such-sheet']);
    assert(m0.recs['s:ready-sheet'].rec.poolIds && m0.recs['s:ready-sheet'].rev && m0.recs['t:set-fixture'].rec.sheetIds.length === 3);
    const held = Object.fromEntries(Object.entries(m0.recs).map(([k, v]) => [k, v.rev]));
    const m1 = await reads({ op: 'getMany', keys: Object.keys(held), ifRevs: held, full: true });
    assert(Object.keys(m1.recs).length === 0 && m1.same === 4 && m1.reads === 4, `held at their revisions: one read each, nothing sent (${m1.reads} reads, ${m1.bytes} B)`);
    st.put(S, 'cut-sheet', { names: 'changed' });
    const m2 = await call({ op: 'getMany', keys: Object.keys(held), ifRevs: held });
    assert.deepEqual(Object.keys(m2.recs), ['s:cut-sheet'], 'only the record that moved'); assert(!m2.recs['s:cut-sheet'].rec.charms, 'without full: its list entry');
    out.push(`D · getMany: 4 records ${m0.bytes} B / ${m0.reads} reads; held 4 unchanged ${m1.bytes} B / ${m1.reads} reads`);
    console.log(out.join('\n'));
    console.log('local-reads: OK');
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
