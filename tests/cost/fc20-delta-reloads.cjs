// FC20 (cost, Oct 2026): the reloads that grew with the data, on the metered in-memory Firestore (tests/cost/meter.cjs).
//   A · masterList since: a moved index signature reads only what was written since the copy, merged it equals a whole read; a removal
//       (count) answers full
//   B · aliasGet / optionMapGet / noDesignGet since: the same for the learned maps, in production and in the sandbox (the merge of the
//       shared store and the sandbox's own copy per key, REVLINK / HUGTWIN), a removal answers the whole map
//   C · customSheetGet ifRev: an unchanged load is one read, a put re-reads only the card it wrote, a page that missed the log reads all
//   D · cancelList after (what a reload now sends from the kept cursor) against the whole idsOnly track read
//   node tests/cost/fc20-delta-reloads.cjs
'use strict';
const assert = require('assert/strict');
const meter = require('./meter.cjs');
const { Timestamp, FieldValue } = meter;
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const T0 = Date.UTC(2026, 8, 6, 14, 0, 0), rows = [];
const sleep = ms => new Promise(r => setTimeout(r, ms));
const N_SKU = 7000, N_AL = 3000, N_OM = 2000, N_ND = 300, N_LINES = 800, N_CANCEL = 300;

(async () => {
  const m = meter.create();
  const docs = {};
  for (let x = 0; x < N_SKU; x++) { const sku = 'SKU' + String(x).padStart(5, '0'); docs['Charm_Master_Index/' + sku] = { sku, masterHash: 'mh' + (x % 900), masterPath: 'charmnest/master/m.ai', indexedAt: Timestamp.fromMillis(T0 + x), updatedAt: Timestamp.fromMillis(T0 + x), widthPt: 20, heightPt: 20 }; }
  docs['Charm_Master_Index/SHELL1'] = { updatedAt: Timestamp.fromMillis(T0) };   // a shell without a SKU (an old patch): no entry, still a document
  for (let a = 0; a < N_AL; a++) docs['Charm_Sku_Aliases/' + (100000 + a)] = { listingId: String(100000 + a), sku: 'SKU' + String(a).padStart(5, '0'), v: 2, updatedAt: Timestamp.fromMillis(T0 + a) };
  for (let a = 0; a < N_OM; a++) docs['Charm_Option_Map/' + (100000 + a)] = { listingId: String(100000 + a), map: { size: { small: { field: 'size', value: 'S', at: T0 } } }, updatedAt: Timestamp.fromMillis(T0 + a) };
  for (let a = 0; a < N_ND; a++) docs['Charm_Sku_NoDesign/nd' + a] = { sku: 'ND' + a, createdAt: Timestamp.fromMillis(T0 + a) };
  // the sandbox's own copy, from before REVLINK: an older answer for one listing, and a listing only the sandbox answered
  docs['Sandbox_Charm_Sku_Aliases/100001'] = { listingId: '100001', sku: 'SKU09999', v: 2, bySku: { OLD: 'SKU00003' }, updatedAt: Timestamp.fromMillis(T0 - 5000) };
  docs['Sandbox_Charm_Sku_Aliases/999999'] = { listingId: '999999', sku: 'SKU00004', v: 2, updatedAt: Timestamp.fromMillis(T0 - 5000) };
  for (let c = 0; c < N_CANCEL; c++) { const rid = String(3750000000 + c); docs['Charm_Nest_Cancelled/' + rid] = { orderId: rid, at: T0 + c * 1000, createdAt: Timestamp.fromMillis(T0 + c * 1000), by: 'Paul' }; }
  // (seeded as written before the stamps they carry: a document's createTime is never later than its own first stamp)
  const realNow = Date.now; Date.now = () => T0 - 60000; m.db.seed(docs); Date.now = realNow;
  m.install();
  const fn = require('../../netlify/functions/charmNestLibrary.js');
  const call = async (name, body) => {
    const a = m.snapshot(); const r = await m.op(name, () => fn.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: {}, body: JSON.stringify(body) }));
    const d = m.since(a), out = JSON.parse(r.body); rows.push([name, d.reads + d.aggs]); out._reads = d.reads + d.aggs; return out;
  };
  const raw = m.db;   // writes the gated ops make (masterPatch / masterPutIndex sit behind the repository password), stamped as they stamp

  /* ── A · master index ── */
  const wholeIndex = async (label) => { let r = await call(label, { op: 'masterList', limit: 3000 }), entries = r.entries, shells = r.shells, index = r.index, reads = r._reads; while (r.next) { r = await call(label + ' (next part)', { op: 'masterList', limit: 3000, cursor: r.next }); entries = entries.concat(r.entries); shells = shells.concat(r.shells); reads += r._reads; } return { entries: new Map(entries.map(e => [e.sku, e])), shells, index, reads }; };
  const full0 = await wholeIndex('masterList whole (page load)');
  ok(full0.entries.size === N_SKU && full0.shells.length === 1 && full0.index.count === N_SKU + 1, 'the whole index: ' + full0.entries.size + ' entries and 1 shell, ' + full0.reads + ' reads');
  await sleep(5);
  await raw.collection('Charm_Master_Index').doc('SKU00010').set({ facing: 'L', facingBy: 'operator', updatedAt: FieldValue.serverTimestamp() }, { merge: true });   // masterPatch
  await raw.collection('Charm_Master_Index').doc('NEWSKU1').set({ sku: 'NEWSKU1', masterHash: 'mhX', indexedAt: FieldValue.serverTimestamp(), widthPt: 30, heightPt: 30 });   // masterPutIndex of a new SKU
  const since0 = { indexedAt: full0.index.indexedAt, updatedAt: full0.index.updatedAt, shape: full0.index.shape, docs: full0.entries.size + full0.shells.length };
  const d1 = await call('masterList since (a patch and a new SKU)', { op: 'masterList', since: since0 });
  ok(d1.since === true && !d1.full && d1._reads < 30, 'a patch and a new SKU: only what was written since is read (' + d1._reads + ' reads, ' + d1.entries.length + ' entries)');
  const merged = new Map(full0.entries); for (const e of d1.entries) merged.set(e.sku, e);
  const full1 = await wholeIndex('masterList whole (to compare)');
  ok(JSON.stringify([...merged].sort()) === JSON.stringify([...full1.entries].sort()) && d1.index.count === N_SKU + 2, 'the copy with what was written since is the whole index (' + merged.size + ' entries), facing ' + merged.get('SKU00010').facing);
  await raw.collection('Charm_Master_Index').doc('SKU00020').delete();   // masterRemoveSkus
  await raw.collection('Charm_Master_Index').doc('NEWSKU2').set({ sku: 'NEWSKU2', masterHash: 'mhX', indexedAt: FieldValue.serverTimestamp() });   // and a create: the count alone is unchanged
  const d2 = await call('masterList since (a removal and a create)', { op: 'masterList', since: { indexedAt: d1.index.indexedAt, updatedAt: d1.index.updatedAt, shape: d1.index.shape, docs: d1.index.count } });
  ok(d2.full === true && !d2.since, 'a removal (hidden behind a create in the count) answers full: the page reads the whole index');
  const d3 = await call('masterList since (nothing written, a create already in the copy)', { op: 'masterList', since: { indexedAt: d1.index.indexedAt, updatedAt: d1.index.updatedAt, shape: 'other', docs: d1.index.count } });
  ok(d3.full === true, 'another entry shape answers full');

  /* ── B · learned maps ── */
  for (const sandbox of [false, true]) {
    const w = sandbox ? ' (sandbox)' : '', sb = sandbox ? { sandbox: true } : {};
    const maps = [['aliasGet', 'aliases'], ['optionMapGet', 'maps'], ['noDesignGet', 'list']], held = {};
    for (const [op] of maps) held[op] = await call(op + ' whole' + w, Object.assign({ op }, sb));
    ok(maps.every(([op]) => Array.isArray(held[op].n)), 'a whole answer says how many documents it was read from' + w);
    await sleep(5);
    const by = 'Tester ' + (sandbox ? 'S' : 'P');
    ok((await call('aliasPut' + w, Object.assign({ op: 'aliasPut', listingId: '100001', sku: 'SKU00002', fromSku: 'NEWVAR', by }, sb))).ok, 'aliasPut' + w);
    ok((await call('aliasPut new listing' + w, Object.assign({ op: 'aliasPut', listingId: '555' + (sandbox ? 1 : 0), sku: 'SKU00005', by }, sb))).ok, 'aliasPut on a new listing' + w);
    ok((await call('optionMapPut' + w, Object.assign({ op: 'optionMapPut', listingId: '100002', optionName: 'Size', optionValue: 'Large', map: { field: 'size', value: 'L' }, by }, sb))).ok, 'optionMapPut' + w);
    ok((await call('noDesignPut' + w, Object.assign({ op: 'noDesignPut', sku: 'NDNEW' + w.length, by }, sb))).ok, 'noDesignPut' + w);
    if (sandbox) { ok((await call('noDesignDelete (sandbox mark)', { op: 'noDesignDelete', id: 'nd15', sandbox: true })).ok, 'a shared no-design row taken off the sandbox\'s list (its mark)'); ok((await call('listingKindPut (sandbox)', { op: 'listingKindPut', listingId: '999999', kind: 'custom', sandbox: true })).ok, 'a listing kind in the sandbox\'s own copy'); }
    for (const [op, field] of maps) {
      const h = held[op], d = await call(op + ' since' + w, Object.assign({ op, ifSig: h.sig, since: { sig: h.sig, n: h.n } }, sb));
      ok(d.since === true && d._reads < 20, op + ' after a change' + w + ': only what was written since (' + d._reads + ' reads, ' + Object.keys(d.changes).length + ' keys)');
      const whole = await call(op + ' whole (to compare)' + w, Object.assign({ op }, sb));
      let got;
      if (field === 'list') { const r = new Map(h.list.rows.map(x => [x.id, x])); for (const [id, v] of Object.entries(d.changes)) { if (v) r.set(id, v); else r.delete(id); } got = [...r.values()].sort((a, b) => a.id < b.id ? -1 : 1); }
      else { got = Object.assign({}, h[field]); for (const [id, v] of Object.entries(d.changes)) { if (v) got[id] = v; else delete got[id]; } }
      const want = field === 'list' ? whole.list.rows.slice().sort((a, b) => a.id < b.id ? -1 : 1) : whole[field];
      const canon = v => JSON.stringify(v, (k, x) => x && typeof x === 'object' && !Array.isArray(x) ? Object.fromEntries(Object.keys(x).sort().map(y => [y, x[y]])) : x);
      ok(canon(got) === canon(want), op + w + ': the copy with the changes is exactly the whole answer, merge included');
      ok(d.sig === whole.sig && JSON.stringify(d.n) === JSON.stringify(whole.n), op + w + ': and it carries the signature and counts a whole read gives');
      held[op] = whole;
    }
    if (sandbox) { const a1 = held.aliasGet.aliases['100001'], a9 = held.aliasGet.aliases['999999']; ok(a1.sku === 'SKU00001' && a1.bySku.NEWVAR === 'SKU00002' && a1.bySku.OLD === 'SKU00003' && a9.sku === 'SKU00004' && a9.listingKind.kind === 'custom', 'the sandbox merge is kept (REVLINK): the later shared pick stands over the sandbox\'s older answer, its own per-SKU answer joins, its own listing and listing kind stay: ' + JSON.stringify([a1.sku, a1.bySku, a9.sku])); }
    // a removal: the count says it, the whole map is the answer
    const nd = held.noDesignGet; await call('noDesignDelete' + w, Object.assign({ op: 'noDesignDelete', id: sandbox ? held.noDesignGet.list.rows.find(r => r.sku === 'NDNEW10').id : 'nd17' }, sb));
    const dd = await call('noDesignGet since after a removal' + w, Object.assign({ op: 'noDesignGet', ifSig: nd.sig, since: { sig: nd.sig, n: nd.n } }, sb));
    ok(!dd.since && dd.list && Array.isArray(dd.list.rows), 'a removal' + w + ': the whole no-design list is the answer (' + dd._reads + ' reads)');
  }

  /* ── C · custom sheet records ── */
  const lines = Array.from({ length: N_LINES }, (_, i) => String(3800000000 + i) + '_' + (2000 + (i % 3))), cards = lines.slice(0, 600).map(k => 'custom:' + k.split('_')[0] + ':SKU' + k);
  const recOf = (key, tag) => { const rid = key.split('_')[0], ck = 'custom:' + rid + ':' + tag, at = Date.now() - 1000; return { ck, rid, at, phase: 'sent', files: [{ id: 'f1', name: 'a.ai', kind: 'ai', state: 'ready', metal: 'gold', qty: 1, pieces: 1, cloud: { path: 'charmnest/custom/a.ai', url: '' } }], sent: { id: 'custom-sheet:' + ck + ':' + at, at, by: 'Paul', lines: { [key]: [{ f: 'f1', i: 0 }] } } }; };
  const c00 = await call('customSheetGet before any put (no revision yet)', { op: 'customSheetGet', keys: [], lineKeys: [lines[0]], wantRev: true });
  ok((await call('customSheetPut (the first one: the revision is made)', { op: 'customSheetPut', record: recOf(lines[700], 'FIRST') })).ok, 'the first put makes the revision');
  const cx = await call('customSheetGet ifRev 0.0 after the first put', { op: 'customSheetGet', keys: [], lineKeys: [], heldLines: [lines[0]], ifRev: c00.rev });
  ok(c00.rev === '0.0' && cx.full === true, 'a page that read before the revision existed reads what it holds again once (' + c00.rev + ' -> ' + cx.rev + ')');
  const c0 = await call('customSheetGet first (wantRev)', { op: 'customSheetGet', keys: cards, lineKeys: lines, legacyLineKeys: [], wantRev: true });
  ok(typeof c0.rev === 'string' && c0.full === true && c0._reads >= N_LINES, 'the first load reads every line and card (' + c0._reads + ' reads) and says the revision ' + c0.rev);
  const c1 = await call('customSheetGet ifRev, nothing changed', { op: 'customSheetGet', keys: [], lineKeys: [], heldKeys: cards, heldLines: lines, ifRev: c0.rev, legacyLineKeys: [] });
  ok(c1.unchanged === true && c1._reads === 1, 'an unchanged load is one read (' + c1._reads + ')');
  const key = lines[5], rec = recOf(key, 'DROP'), ck = rec.ck;
  const put = await call('customSheetPut', { op: 'customSheetPut', record: rec });
  ok(put.ok, 'a custom design is sent (customSheetPut) ' + (put.error || ''));
  const c2 = await call('customSheetGet ifRev after a put on a held line', { op: 'customSheetGet', keys: [], lineKeys: [], heldKeys: cards, heldLines: lines, ifRev: c0.rev, legacyLineKeys: [] });
  ok(!c2.full && c2.records[ck] && c2.records[ck].sent.lines[key] && c2._reads <= 3, 'the put on a line the page holds: only that card is read (' + c2._reads + ' reads) and its record comes back');
  const c3 = await call('customSheetGet ifRev, a put on a line not held', { op: 'customSheetGet', keys: [], lineKeys: [], heldKeys: cards.slice(10), heldLines: lines.slice(10), ifRev: c0.rev, legacyLineKeys: [] });
  ok(!c3.full && !c3.records[ck] && c3._reads === 1, 'a put that touches nothing the page holds reads nothing more (' + c3._reads + ')');
  const c4 = await call('customSheetGet ifRev of another revision id', { op: 'customSheetGet', keys: [], lineKeys: [], heldKeys: cards, heldLines: lines, ifRev: 'abcdef.0', legacyLineKeys: [] });
  ok(c4.full === true && c4.records[ck] && c4._reads >= N_LINES, 'a revision the log does not cover (a Reset, another id): everything held is read again (' + c4._reads + ')');
  const c5 = await call('customSheetGet without a revision (an older page)', { op: 'customSheetGet', keys: cards, lineKeys: lines });
  ok(c5.rev === undefined && c5.records[ck], 'a call without ifRev / wantRev answers as before');

  /* ── D · cancelled orders on a page load ── */
  const k0 = await call('cancelList idsOnly track (page load before)', { op: 'cancelList', idsOnly: true, track: true, wantGen: true });
  const k1 = await call('cancelList after the kept cursor (page load now)', { op: 'cancelList', idsOnly: true, after: k0.cursor, limit: 500, wantGen: true });
  ok(k0.ids.length === N_CANCEL && k1.ids.length === 0 && +k1.total === N_CANCEL && k1._reads <= 3, 'a reload from the kept cursor reads ' + k1._reads + ' (the whole set was ' + k0._reads + ') and its count still finds a restore');

  m.uninstall();
  console.log('op'.padEnd(64) + 'reads');
  for (const [name, r] of rows) console.log(name.slice(0, 63).padEnd(64) + String(r).padStart(6));
  console.log(`fc20-delta-reloads: ${n} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
