// KEEP (16 Oct 2026, KR): the REPAIR PATH of "drop a vector file on a Review order + SAVE ON -> the permanent Charm repository under the listing's SKU", and the RACES around it.
// OFFLINE, all fakes (tests/charm-nest/_sandboxFakes.cjs: in-memory Firestore + Storage; _keepTxFake.cjs: transactions as Firestore has them): no network, no Etsy, no real repository.
// The REAL handler (charmNestLibrary.js), the REAL password door (_charmNestRepoAuth.js), the REAL _charmNestCustomRepair.js / _charmNestCustomSave.js / _charmNestMaster.js (putIndex).
// Rules under test (CONTRACT-KEEP section KR, R1-R5, R9; each section FAILS on origin/main 3a4e42f0, which has no repair path at all):
//   A  ORPHAN FILES   uploaded files without a record (a page that died after its uploads; tab B's file after "already saved"): removed after the grace, never an entry's files, never young ones,
//                     never when an entry appeared since the list was read; olderThanMs under an hour needs the upload token; idempotent
//   B  RECORDS        an entry removed (Library delete) / replaced by another drawing: the answer this save wrote is cut, never a person's link; files gone: flagged; an answer lost: put back; each healed state
//                     clears its flag; a record that is whole is not written; a page of records per pass, a cursor covers all; torn commit leftovers are never destroyed
//   C  INTENTS        the server's old notes are removed, a live one never
//   D  RACES          a master re-index/upload never overwrites a saved design (also when the design lands between its read and its write); a save racing repair passes; two repairs at once; the throttle
//   E  WIRING         the library op, the sandbox, the token rule
//   node tests/charm-nest/keep-repair.cjs
'use strict';
const path = require('path'), assert = require('assert/strict'), crypto = require('crypto');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret';
let n = 0; const ok = (c, m) => { assert(c, m); n++; }; const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const { store, blobs, writes, fileWrites } = Fk, db = Fk.db, FV = Fk.admin.firestore.FieldValue;
const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
global.fetch = async () => { throw new Error('no network in this test'); };

/* Storage: a "last updated" the test can set (default: now) */
let clk = Date.now();
const ages = new Map();   // path → ms (or null: unknown)
const origFile = Fk.bucket.file.bind(Fk.bucket);
Fk.bucket.file = p => { const f = origFile(p), gm = f.getMetadata; f.getMetadata = async () => { const [m] = await gm.call(f); const t = ages.has(p) ? ages.get(p) : clk; return [Object.assign({}, m, t == null ? {} : { updated: new Date(t).toISOString() })]; }; return f; };
const TX = require('./_keepTxFake.cjs')(Fk);
const lib = require(path.join(fnDir, 'charmNestLibrary.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
const Repair = require(path.join(fnDir, '_charmNestCustomRepair.js')), Save = require(path.join(fnDir, '_charmNestCustomSave.js')), Master = require(path.join(fnDir, '_charmNestMaster.js'));
const post = async b => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
const deps = (o) => Object.assign({ db, FV, bucket: Fk.bucket, now: () => clk, tokenUrl: async p => 'https://storage.example/' + p + '?token=t' }, o || {});
const H = 3600000, DAY = 24 * H;

/* ── the pieces of a saved design ── */
const AIOF = d => Buffer.from('%PDF-1.4\n%drawing ' + d + '\n' + ('0 0 m 10 10 l S\n'.repeat(30)));
const PNGOF = d => Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), Buffer.from('picture ' + d), Buffer.alloc(80, d)]);
const files = sku => ({ ai: 'charmnest/master/custom/' + sku + '.ai', png: 'charmnest/master/custom/' + sku + '.png' });
const putFiles = (sku, d, age) => { const f = files(sku); blobs.set(f.ai, { buf: AIOF(d), contentType: 'application/illustrator' }); blobs.set(f.png, { buf: PNGOF(d), contentType: 'image/png' }); ages.set(f.ai, age == null ? clk : age); ages.set(f.png, age == null ? clk : age); };
/** a saved design the way customSaveCommit leaves it; o.omit lists parts to leave out: entry, file, alias, record, blobs */
function seed(o) {
  const sku = o.sku, d = o.d, f = files(sku), omit = o.omit || [], old = o.old == null ? 'WAS ' + sku : o.old, rid = o.rid, listing = o.listing, whole = old === '';
  if (!omit.includes('blobs')) putFiles(sku, d, o.age);
  if (!omit.includes('entry')) store.set('Charm_Master_Index/' + sku, { sku, masterHash: 'mh-' + d, masterPath: f.ai, masterName: 'D' + d + '.ai', charmHash: 'drawing-' + d, widthPt: 31, heightPt: 38, areaPt2: 700, members: 4, holes: 1, engravable: true, aiPath: f.ai, thumbPath: f.png, hashSource: o.hashSource || 'custom-save', indexedAtMs: clk });
  if (!omit.includes('file')) store.set('Charm_Master_Files/mh-' + d, { masterHash: 'mh-' + d, path: f.ai, skus: [sku], indexedBy: 'custom-save' });
  if (!omit.includes('alias')) store.set('Charm_Sku_Aliases/' + listing, Object.assign(whole ? { listingId: listing, sku, v: 2 } : { listingId: listing, bySku: { [old]: sku } }, { keep: { rid, sku, oldSku: old, was: o.was || '', at: clk, by: 'Paul' } }, o.aliasExtra || {}));
  if (!omit.includes('record')) store.set('Charm_Custom_Saves/' + rid, Object.assign({ rid, listingId: listing, sku, oldSku: old, charmHash: 'drawing-' + d, masterHash: 'mh-' + d, aiPath: f.ai, thumbPath: f.png, phase: 'repo', alias: whole ? 'whole' : 'bySku', etsy: { state: 'pending' }, rehearsal: false, reused: false, by: 'Paul', at: o.at || clk }, o.record || {}));
}
const reset = () => { store.clear(); blobs.clear(); ages.clear(); writes.length = 0; fileWrites.length = 0; clk = Date.now(); };
const real = () => writes.filter(w => !/^Charm_Custom_Repair\//.test(w.path));
const alias = l => store.get('Charm_Sku_Aliases/' + l), rec = id => store.get('Charm_Custom_Saves/' + id);
const snap = () => JSON.stringify([...store.entries()].filter(([k]) => !/^(Charm_Custom_Repair|CharmRepo_)/.test(k)).sort((a, b) => (a[0] < b[0] ? -1 : 1)));
const repair = (b, d) => Repair.repair(deps(d), Object.assign({ force: true }, b));
let TOKEN; const tokenOf = async action => (await RA.issue(db, { action, by: 'Tester' })).token;

const T = [], test = (name, fn) => T.push([name, fn]);

/* ═══ A · orphan files ═══ */
test('A1 uploaded files without a record: kept while young, removed after the grace, an entry\'s files never; idempotent', async () => {
  reset();
  putFiles('ACORN_YOUNG_1', 1, clk - 2 * H); putFiles('ACORN_OLD_0002', 2, clk - 25 * H); putFiles('ACORNS_LEAF_94821', 3, clk - 3 * DAY);   // (tab B's file after "already saved")
  seed({ sku: 'GOOD_0001', d: 4, rid: '4200000001', listing: '1800000001', age: clk - 40 * DAY });
  const r = await repair({});
  eq(r.orphans.orphans.sort(), ['ACORNS_LEAF_94821', 'ACORN_OLD_0002'], 'the old orphans'); eq(r.orphans.deleted, 4);
  ok(blobs.has(files('ACORN_YOUNG_1').ai) && blobs.has(files('ACORN_YOUNG_1').png), 'the young upload (a save waiting for the password) stays');
  ok(blobs.has(files('GOOD_0001').ai) && blobs.has(files('GOOD_0001').png), 'an entry\'s files are never touched, however old');
  ok(!blobs.has(files('ACORN_OLD_0002').ai) && !blobs.has(files('ACORNS_LEAF_94821').png), 'old orphans gone');
  const s0 = snap(), f0 = fileWrites.length; const r2 = await repair({});
  eq([r2.orphans.deleted, fileWrites.length, snap()], [0, f0, s0], 'a second pass changes nothing');
  clk += 2 * DAY; ages.set(files('ACORN_YOUNG_1').ai, clk - 25 * H); ages.set(files('ACORN_YOUNG_1').png, clk - 25 * H);
  const r3 = await repair({}); eq(r3.orphans.orphans, ['ACORN_YOUNG_1'], 'when the grace is over it goes too');
});
test('A2 the age of a file that cannot be read keeps it; one young file keeps the pair', async () => {
  reset(); const f = files('MIXED_0001'); putFiles('MIXED_0001', 5, clk - 5 * DAY); ages.set(f.png, clk - 1 * H);
  let r = await repair({}); eq([r.orphans.deleted, blobs.has(f.ai), blobs.has(f.png)], [0, true, true], 'a pair with one young file stays whole');
  ages.set(f.png, null); r = await repair({}); eq([r.orphans.deleted, blobs.has(f.ai)], [0, true], 'an unknown age is kept');
});
test('A3 an entry that appears after the list was read keeps its files (a commit racing the repair)', async () => {
  reset(); putFiles('RACE_0001', 6, clk - 3 * DAY);
  const orig = Fk.bucket.getFiles.bind(Fk.bucket); Fk.bucket.getFiles = async q => { const res = await orig(q); seed({ sku: 'RACE_0001', d: 6, rid: '4200000009', listing: '1800000009', omit: ['blobs'] }); return res; };
  try { const r = await repair({}); eq([r.orphans.deleted, blobs.has(files('RACE_0001').ai)], [0, true], 'the commit won; its files stay'); } finally { Fk.bucket.getFiles = orig; }
});
test('A4 olderThanMs under an hour needs the upload token (a person\'s act); with it the young orphans go (KC S5)', async () => {
  reset(); TOKEN = await tokenOf('upload'); putFiles('FRESH_0001', 7);
  let r = await post({ op: 'customSaveRepair', olderThanMs: 0 }); eq([r.status, r.body.error], [401, 'repo-locked'], 'locked: refused by the door'); ok(blobs.has(files('FRESH_0001').ai), 'nothing removed');
  r = await post({ op: 'customSaveRepair', olderThanMs: 0, repoToken: TOKEN }); eq(r.status, 200); eq(r.body.orphans.orphans, ['FRESH_0001']); ok(!blobs.has(files('FRESH_0001').ai));
  putFiles('FRESH_0002', 8);
  r = await post({ op: 'customSaveRepair' }); eq([r.status, r.body.ok], [200, true], 'the default pass needs no password'); ok(blobs.has(files('FRESH_0002').ai), 'and keeps a young upload');
  r = await post({ op: 'customSaveRepair', olderThanMs: 2 * H }); eq(r.status, 200, 'a long age needs no token');
});

/* ═══ B · records ═══ */
test('B1 a whole save is not written to; its entry removed by the Library: the answer it wrote is cut, the record says so, a person\'s link and another design\'s answer are never touched', async () => {
  reset();
  seed({ sku: 'LEAF_0001', d: 1, rid: '4200000001', listing: '1800000001', old: 'LEAF ONE' });
  seed({ sku: 'LEAF_0002', d: 2, rid: '4200000002', listing: '1800000002', old: 'LEAF TWO' });
  store.set('Charm_Sku_Aliases/1800000003', { listingId: '1800000003', bySku: { 'THREE': 'PERSON_LINK' }, by: 'Ana' });                 // a person's link to a design not indexed yet: no stamp
  seed({ sku: 'LEAF_0004', d: 4, rid: '4200000004', listing: '1800000004', old: 'LEAF FOUR' });
  const s0 = snap(); let r = await repair({}); eq(r.records.fixed, [], 'nothing to fix'); eq(snap(), s0, 'a whole repository is not written to');
  await lib.ops.masterRemoveSku({ sku: 'LEAF_0001' });                                                                                     // a person removes the design in the Library
  await lib.ops.masterRemoveSku({ sku: 'LEAF_0004' });
  store.set('Charm_Sku_Aliases/1800000004', Object.assign({}, alias('1800000004'), { bySku: { 'LEAF FOUR': 'ANOTHER_DESIGN' } }));           // another design answers LEAF FOUR now
  r = await repair({});
  eq(r.records.fixed.map(x => x.id + ':' + x.did.join('+')).sort(), ['4200000001:alias-cut+flag:entry-gone', '4200000004:flag:entry-gone'], JSON.stringify(r.records));
  eq(alias('1800000001').bySku, {}, 'no dangling answer: the order is held for what it is, not for a design that does not exist'); eq(alias('1800000001').keep.cut.why, 'entry-gone');
  eq(rec('4200000001').repair.state, 'entry-gone'); eq(alias('1800000004').bySku['LEAF FOUR'], 'ANOTHER_DESIGN', 'another design\'s answer stays');
  eq(alias('1800000003').bySku.THREE, 'PERSON_LINK', 'a person\'s link stays'); eq(alias('1800000002').bySku['LEAF TWO'], 'LEAF_0002', 'a healthy one stays');
  const s1 = snap(), w0 = real().length; r = await repair({}); eq([r.records.fixed, snap(), real().length], [[], s1, w0], 'a second pass changes nothing');
});
test('B2 what the save replaced is put back when it still exists', async () => {
  reset();
  seed({ sku: 'PREV_0001', d: 10, rid: '4200000010', listing: '1800000010', old: 'P ONE' });
  seed({ sku: 'NEW_0001', d: 11, rid: '4200000011', listing: '1800000010', old: 'P ONE', was: 'PREV_0001', omit: ['alias'] });
  store.set('Charm_Sku_Aliases/1800000010', { listingId: '1800000010', bySku: { 'P ONE': 'NEW_0001' }, keep: { rid: '4200000011', sku: 'NEW_0001', oldSku: 'P ONE', was: 'PREV_0001', at: clk, by: 'Paul' } });
  await lib.ops.masterRemoveSku({ sku: 'NEW_0001' }); const r = await repair({});
  eq(alias('1800000010').bySku['P ONE'], 'PREV_0001', 'the earlier design answers again'); ok(r.records.fixed.some(f => f.did.includes('alias-cut')));
});
test('B3 an entry that now holds ANOTHER drawing is "overtaken": cut and flagged; when it is the drawing again, the flag goes and the answer is put back', async () => {
  reset(); seed({ sku: 'OVER_0001', d: 20, rid: '4200000020', listing: '1800000020', old: 'O ONE' });
  store.set('Charm_Master_Index/OVER_0001', Object.assign({}, store.get('Charm_Master_Index/OVER_0001'), { charmHash: 'some-other-drawing', hashSource: 'browser' }));
  let r = await repair({}); eq(r.records.flagged, [{ id: '4200000020', state: 'overtaken' }]); eq(alias('1800000020').bySku, {}, 'the listing is not left pointing at another drawing'); eq(rec('4200000020').repair.state, 'overtaken');
  store.set('Charm_Master_Index/OVER_0001', Object.assign({}, store.get('Charm_Master_Index/OVER_0001'), { charmHash: 'drawing-20', hashSource: 'custom-save' }));
  r = await repair({}); eq(rec('4200000020').repair, undefined, 'healed: flag gone'); eq(alias('1800000020').bySku['O ONE'], 'OVER_0001', 'and the answer is back'); eq(alias('1800000020').keep.repaired, true);
});
test('B4 files gone from Storage: flagged (which), nothing else touched; the file back: flag cleared', async () => {
  reset(); seed({ sku: 'FILE_0001', d: 30, rid: '4200000030', listing: '1800000030' }); const f = files('FILE_0001');
  blobs.delete(f.png); let r = await repair({}); eq(r.records.flagged, [{ id: '4200000030', state: 'files-missing' }]); eq(rec('4200000030').repair.which, ['thumb']); eq(alias('1800000030').bySku['WAS FILE_0001'], 'FILE_0001', 'the link stays: the file is re-attached by the next run of the save');
  blobs.delete(f.ai); r = await repair({}); eq(rec('4200000030').repair.which, ['ai', 'thumb']);
  putFiles('FILE_0001', 30); r = await repair({}); eq(rec('4200000030').repair, undefined);
  const s0 = snap(); await repair({}); eq(snap(), s0);
});
test('B5 an answer the alias lost is put back (only an alias that is this save\'s own); a person\'s or another design\'s answer is never replaced', async () => {
  reset();
  seed({ sku: 'LOST_0001', d: 40, rid: '4200000040', listing: '1800000040', omit: ['alias'] });                                             // the alias document is gone
  seed({ sku: 'LOST_0002', d: 41, rid: '4200000041', listing: '1800000041', aliasExtra: { bySku: {} } });                                   // the document has lost its key
  seed({ sku: 'LOST_0003', d: 42, rid: '4200000042', listing: '1800000042' }); store.set('Charm_Sku_Aliases/1800000042', { listingId: '1800000042', bySku: { 'WAS LOST_0003': 'PERSON_PICK' }, by: 'Ana' });   // a person's pick answers
  seed({ sku: 'LOST_0004', d: 43, rid: '4200000043', listing: '1800000043' }); store.set('Charm_Sku_Aliases/1800000043', { listingId: '1800000043', bySku: {}, by: 'Ana' });                                         // a person's document, our key lost, no stamp
  seed({ sku: 'LOST_0005', d: 44, rid: '4200000044', listing: '1800000044', old: '' }); store.set('Charm_Sku_Aliases/1800000044', { listingId: '1800000044', title: 'x', keep: { rid: '4200000044', sku: 'LOST_0005', at: 1 } });   // the whole-listing answer lost
  const r = await repair({});
  eq(alias('1800000040').bySku['WAS LOST_0001'], 'LOST_0001'); eq(alias('1800000041').bySku['WAS LOST_0002'], 'LOST_0002'); eq(alias('1800000042').bySku['WAS LOST_0003'], 'PERSON_PICK', 'a person\'s pick stays');
  eq(alias('1800000043').bySku, {}, 'a document with no stamp of ours is not ours to fill'); eq([alias('1800000044').sku, alias('1800000044').v], ['LOST_0005', 2], 'the whole-listing answer is put back');
  eq(r.records.fixed.filter(f => f.did.includes('alias-put')).length, 3);
  const s0 = snap(); await repair({}); eq(snap(), s0, 'idempotent');
});
test('B6 a whole-listing answer (no SKU on the line) is cut when its entry goes, and what it replaced comes back', async () => {
  reset(); seed({ sku: 'WHOLE_0001', d: 50, rid: '4200000050', listing: '1800000050', old: '', was: 'EARLIER_0001' }); seed({ sku: 'EARLIER_0001', d: 51, rid: '4200000051', listing: '1800000051' });
  await lib.ops.masterRemoveSku({ sku: 'WHOLE_0001' }); await repair({}); eq(alias('1800000050').sku, 'EARLIER_0001', 'the earlier design answers');
  await lib.ops.masterRemoveSku({ sku: 'EARLIER_0001' }); store.set('Charm_Custom_Saves/4200000050', Object.assign({}, rec('4200000050')));
  await repair({}); ok(alias('1800000050').sku === 'EARLIER_0001' || alias('1800000050').sku === undefined, 'no crash on a chain');
});
test('B7 a rehearsal or an unfinished record is skipped; the sandbox leaves no trace', async () => {
  reset(); seed({ sku: 'SKIP_0001', d: 60, rid: '4200000060', listing: '1800000060', record: { rehearsal: true }, omit: ['entry'] }); seed({ sku: 'SKIP_0002', d: 61, rid: '4200000061', listing: '1800000061', record: { phase: 'files' }, omit: ['entry'] });
  const s0 = snap(); const r = await repair({}); eq([r.records.checked, snap()], [0, s0]);
  const w0 = writes.length, f0 = fileWrites.length, sb = await repair({ sandbox: true }); eq([sb.sandbox, writes.length, fileWrites.length], [true, w0, f0]);
});
test('B8 a Netlify function killed in the Etsy step ("writing", lease over) is made final: unverified, interrupted; a live lease is left alone', async () => {
  reset(); seed({ sku: 'ETSY_0001', d: 70, rid: '4200000070', listing: '1800000070', record: { etsy: { state: 'writing', runningUntil: clk - 5000, calls: 1, beforePath: 'x' } } });
  seed({ sku: 'ETSY_0002', d: 71, rid: '4200000071', listing: '1800000071', record: { etsy: { state: 'writing', runningUntil: clk + 30000, calls: 1 } } });
  const r = await repair({}); eq(r.records.fixed.map(f => f.id), ['4200000070']);
  const e = rec('4200000070').etsy; eq([e.state, e.reason, e.retry, 'runningUntil' in e, e.beforePath], ['unverified', 'interrupted', true, false, 'x'], 'kept what the next try needs'); eq(rec('4200000071').etsy.state, 'writing');
});
test('B9 the cursor: a page of records per pass, every record is looked at in a cycle, then it starts over', async () => {
  reset(); for (let i = 0; i < 95; i++) seed({ sku: 'MANY_' + String(1000 + i), d: 100 + i, rid: String(4300000000 + i), listing: String(1900000000 + i), at: clk - (95 - i) * 1000, omit: ['entry'] });
  const seen = new Set(); let passes = 0, r;
  do { r = await repair({}, { now: () => clk }); passes++; r.records.flagged.forEach(f => seen.add(f.id)); } while (!r.records.wrapped && passes < 10);
  eq([passes, seen.size, r.records.wrapped], [3, 95, true], 'three passes of 40 cover 95 records, then it wraps'); eq(store.get('Charm_Custom_Repair/state').cursor, 0);
  const s0 = snap(); await repair({}); eq(snap(), s0, 'the next cycle finds nothing to do');
});
test('B10 torn commit leftovers (any number of writes landed) are never destroyed by a repair pass', async () => {
  const body = k => ({ rid: '4200000090', listingId: '1800000090', sku: 'TORN_0001', oldSku: 'TORN OLD', productId: '', optionPairs: [], group: { mode: 'all', productIds: [] }, title: 't', by: 'Paul', design: { name: 'D.ai', kind: 'ai', hash: 'f'.repeat(40), bytes: 1 }, reuse: false,
    entry: { charmHash: 'drawing-90', widthPt: 31, heightPt: 38, areaPt2: 700, members: 4, holes: 1, engravable: true, upAngle: 90, upSource: 'long' }, file: { masterHash: 'f'.repeat(40), name: 'D.ai' } });
  for (let k = 0; k <= 6; k++) {
    reset(); putFiles('TORN_0001', 90, clk - 2 * H); TX.hook.crash = { after: k, torn: true };
    try { await Save.commit(deps(), body(k)); } catch (_) { /* the store was cut */ } TX.hook.crash = null;
    const before = { entry: store.has('Charm_Master_Index/TORN_0001'), alias: !!alias('1800000090'), record: !!rec('4200000090') };
    const r = await repair({}); eq(r.orphans.deleted, 0, 'k=' + k + ': young files stay');
    eq({ entry: store.has('Charm_Master_Index/TORN_0001'), alias: !!alias('1800000090'), record: !!rec('4200000090') }, before, 'k=' + k + ': the repair destroyed no part: ' + JSON.stringify(before));
    ok(blobs.has(files('TORN_0001').ai), 'k=' + k + ': the files stay');
  }
});

/* ═══ C · intents ═══ */
test('C1 the server\'s old notes go; a live one stays', async () => {
  reset(); const put = (rid, state, age, extra) => store.set('Charm_Custom_Intents/' + rid, Object.assign({ rid, listingId: '1800000001', state, updatedAtMs: clk - age, at: clk - age }, extra || {}));
  put('4400000001', 'done', 8 * DAY); put('4400000002', 'done', 1 * DAY); put('4400000003', 'cancelled', 9 * DAY); put('4400000004', 'wanted', 31 * DAY); put('4400000005', 'wanted', 3 * DAY); put('4400000006', 'wanted', 40 * DAY);
  seed({ sku: 'INT_0001', d: 80, rid: '4400000006', listing: '1800000001' });   // wanted for 40 days but saved after all: the commit marks it done, the repair does not decide
  const r = await repair({}); eq(r.intents.deleted, 3); eq([...store.keys()].filter(k => k.startsWith('Charm_Custom_Intents/')).sort(), ['Charm_Custom_Intents/4400000002', 'Charm_Custom_Intents/4400000005', 'Charm_Custom_Intents/4400000006']);
});

/* ═══ D · races ═══ */
const masterBody = (sku, hash, d) => ({ masterHash: hash, masterPath: 'charmnest/master/Hearts.ai', masterName: 'Hearts.ai', hashSource: 'browser', replaces: [], entries: [{ sku, charmHash: 'master-' + d, widthPt: 20, heightPt: 22, areaPt2: 300, members: 2, holes: 0, engravable: true, upAngle: 0, upSource: 'index', aiPath: 'charmnest/master/' + sku + '.ai', thumbPath: 'charmnest/master/' + sku + '.png' }] });
test('D1 a master re-index/upload never overwrites a design saved from an order; other entries are written as before', async () => {
  reset(); seed({ sku: 'SAVED_0001', d: 1, rid: '4200000001', listing: '1800000001' }); store.set('Charm_Master_Index/PLAIN_0001', { sku: 'PLAIN_0001', masterHash: 'oldmaster', masterName: 'Old.ai', charmHash: 'plain-old', widthPt: 10, heightPt: 10 });
  const b = masterBody('SAVED_0001', 'newmaster', 1); b.entries.push(Object.assign({}, b.entries[0], { sku: 'PLAIN_0001', charmHash: 'plain-new' })); b.replaces = ['oldmaster'];
  const before = JSON.stringify(store.get('Charm_Master_Index/SAVED_0001')); const out = await Master.putIndex(db, FV, b);
  eq(JSON.stringify(store.get('Charm_Master_Index/SAVED_0001')), before, 'the saved design is exactly as it was'); eq(out.blocked.map(x => x.sku), ['SAVED_0001'], 'reported, not hidden'); eq(out.blocked[0].saved, true);
  eq([out.written, store.get('Charm_Master_Index/PLAIN_0001').charmHash], [1, 'plain-new'], 'an ordinary entry is rewritten as before');
  // a re-index of the saved design\'s own master file (same hash, or named in replaces) still updates it
  const own = masterBody('SAVED_0001', 'mh-1', 1); const o2 = await Master.putIndex(db, FV, own); eq(o2.written, 1, 'its own file may refresh it'); eq(o2.blocked, []);
});
test('D2 the design lands BETWEEN the master upload\'s read and its write: it is still kept', async () => {
  reset(); const orig = db.getAll.bind(db); let first = true;
  db.getAll = async (...refs) => { const res = await orig(...refs); if (first && refs.some(r => r.path === 'Charm_Master_Index/RACE_0002')) { first = false; seed({ sku: 'RACE_0002', d: 2, rid: '4200000002', listing: '1800000002' }); } return res; };   // the commit lands right after the read
  try {
    const out = await Master.putIndex(db, FV, masterBody('RACE_0002', 'newmaster', 2));
    eq(store.get('Charm_Master_Index/RACE_0002').charmHash, 'drawing-2', 'the saved design survived the race'); eq(out.blocked.map(x => x.sku), ['RACE_0002']); eq(out.written, 0);
  } finally { db.getAll = orig; }
});
test('D3 a save racing repair passes, in every order: the new save is never cut, flagged or stripped of its files (default passes); a person\'s zero-age pass at worst leaves "files-missing", never a half record', async () => {
  const body = sku => ({ rid: '4200000100', listingId: '1800000100', sku, oldSku: 'RACE OLD', productId: '', optionPairs: [], group: { mode: 'all', productIds: [] }, title: 't', by: 'Paul', design: { name: 'D.ai', kind: 'ai', hash: 'e'.repeat(40), bytes: 1 }, reuse: false,
    entry: { charmHash: 'drawing-100', widthPt: 31, heightPt: 38, areaPt2: 700, members: 4, holes: 1, engravable: true, upAngle: 90, upSource: 'long' }, file: { masterHash: 'e'.repeat(40), name: 'D.ai', aiSha256: sha(AIOF(100)), aiBytes: AIOF(100).length, pngSha256: sha(PNGOF(100)) } });
  const whole = label => { ok(store.has('Charm_Master_Index/RACE_0100') && !!rec('4200000100') && alias('1800000100').bySku['RACE OLD'] === 'RACE_0100', label + ': entry, record and answer exist'); };
  for (let order = 0; order < 6; order++) {   // default passes (a day of grace): the save is always whole, files included
    reset(); putFiles('RACE_0100', 100);
    const jobs = [() => Save.commit(deps(), body('RACE_0100')), () => repair({}), () => repair({ etsy: false })];
    const perm = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]][order];
    const res = await Promise.all(perm.map(i => jobs[i]())); const c = res[perm.indexOf(0)];
    ok(c.ok, 'order ' + perm + ': the commit succeeds: ' + JSON.stringify(c)); whole('order ' + perm);
    await repair({}); ok(!rec('4200000100').repair && blobs.has(files('RACE_0100').ai) && blobs.has(files('RACE_0100').png), 'order ' + perm + ': a pass afterwards leaves it whole, files there');
  }
  for (let order = 0; order < 6; order++) {   // a person's zero-age sweep racing the save
    reset(); putFiles('RACE_0100', 100);
    const jobs = [() => Save.commit(deps(), body('RACE_0100')).catch(e => ({ ok: false, error: 'transient: ' + (e && e.message) })), () => repair({ olderThanMs: 0 }), () => repair({ olderThanMs: 0 })];   // (a file removed between the commit\'s exists() and getMetadata() surfaces as a thrown 404 = HTTP 500 = the page retries: transient)
    const perm = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]][order];
    const res = await Promise.all(perm.map(i => jobs[i]())); const c = res[perm.indexOf(0)];
    if (c.ok) { whole('zero-age order ' + perm); await repair({}); const r = rec('4200000100'); ok(!r.repair || r.repair.state === 'files-missing', 'at worst the files are flagged: ' + JSON.stringify(r.repair)); }
    else { ok(c.error === 'not-uploaded' || /^transient: .*(no blob|404)/.test(c.error), 'zero-age order ' + perm + ': the only refusals are "upload again" or a transient 404: ' + JSON.stringify(c)); ok(!store.has('Charm_Master_Index/RACE_0100') && !rec('4200000100') && !alias('1800000100'), 'and nothing half-written'); }
  }
});
test('D4 two repairs at once (two tabs): one pass runs, the other is told so; targeted repairs of one broken record cut once', async () => {
  reset(); seed({ sku: 'TWO_0001', d: 110, rid: '4200000110', listing: '1800000110' }); await lib.ops.masterRemoveSku({ sku: 'TWO_0001' });
  const rs = await Promise.all([repair({}), repair({}), repair({})]); eq(rs.filter(r => r.skipped === 'running').length, 2, 'the others were told "running"'); eq(rs.filter(r => !r.skipped).length, 1);
  eq(rec('4200000110').repair.state, 'entry-gone');
  reset(); seed({ sku: 'TWO_0002', d: 111, rid: '4200000111', listing: '1800000111' }); await lib.ops.masterRemoveSku({ sku: 'TWO_0002' }); writes.length = 0;
  const ts = await Promise.all([1, 2, 3, 4].map(() => Repair.repair(deps(), { rid: '4200000111' })));
  eq(ts.map(t => t.records.fixed.length).sort(), [0, 0, 0, 1], 'exactly one of four targeted repairs did the cut'); eq(writes.filter(w => w.path === 'Charm_Sku_Aliases/1800000111').length, 1, 'one write of the alias');
});
test('D5 the throttle: a full pass at most every five minutes unless forced; a pass left "running" by a dead function ends with its lease', async () => {
  reset(); const d = deps();
  let r = await Repair.repair(d, {}); eq(r.ok, true); ok(!r.skipped);
  r = await Repair.repair(d, {}); eq(r.skipped, 'recent'); clk += 6 * 60000; r = await Repair.repair(d, {}); ok(!r.skipped, 'after five minutes it runs again');
  store.set('Charm_Custom_Repair/state', { runningUntil: clk + 60000, lastRunAt: 0 }); r = await Repair.repair(d, {}); eq(r.skipped, 'running'); clk += 3 * 60000; r = await Repair.repair(d, {}); ok(!r.skipped, 'a dead pass\'s lease ran out');
});

/* ═══ F · the next run of the same save heals what the repair could only flag (needs KB's convergent commit: skipped, visibly, on a tree that has not got it yet) ═══ */
const CONVERGENT = typeof Save.pending === 'function' && typeof Save.intent === 'function';
const cbody = (rid, listing, sku, old, d) => ({ rid, listingId: listing, sku, oldSku: old, productId: '', optionPairs: [], group: { mode: 'all', productIds: [] }, title: 't', by: 'Paul', design: { name: 'D' + d + '.ai', kind: 'ai', hash: ('a' + d).padEnd(40, '0'), bytes: 1 }, reuse: false,
  entry: { charmHash: 'drawing-' + d, widthPt: 31, heightPt: 38, areaPt2: 700, members: 4, holes: 1, engravable: true, upAngle: 90, upSource: 'long' }, file: { masterHash: ('a' + d).padEnd(40, '0'), name: 'D' + d + '.ai', aiSha256: sha(AIOF(d)), aiBytes: AIOF(d).length, pngSha256: sha(PNGOF(d)) } });
const ftest = (name, fn) => test(name, async () => { if (!CONVERGENT) { process.stdout.write('  skip ' + name.slice(0, 100) + ' (this tree\'s commit is not convergent: needs KB)\n'); return; } await fn(); });
ftest('F1 entry removed by the Library, answer cut: the next run of the save (file dropped again) re-creates the entry and the answer, the flag goes', async () => {
  reset(); const b = cbody('4200000201', '1800000201', 'HEAL_0001', 'HEAL OLD', 1); putFiles('HEAL_0001', 1);
  let c = await Save.commit(deps(), b); eq([c.ok, c.created], [true, true]);
  await lib.ops.masterRemoveSku({ sku: 'HEAL_0001' }); await repair({});
  eq([alias('1800000201').bySku, rec('4200000201').repair.state], [{}, 'entry-gone'], 'cut and flagged');
  const w = Save.commit(deps(), b); c = await w;   // the page uploaded the same files again (they are still there) and runs the save again
  eq([c.ok, c.sku], [true, 'HEAL_0001'], JSON.stringify(c)); ok(store.has('Charm_Master_Index/HEAL_0001'), 'the entry is back'); eq(alias('1800000201').bySku['HEAL OLD'], 'HEAL_0001', 'the answer is back');
  await repair({}); eq(rec('4200000201').repair, undefined, 'healed: no flag'); const s0 = snap(); await repair({}); eq(snap(), s0, 'idempotent');
});
ftest('F2 files vanished: flagged; the next run asks for the upload (not-uploaded), takes it, the flag goes', async () => {
  reset(); const b = cbody('4200000202', '1800000202', 'HEAL_0002', 'HEAL OLD2', 2); putFiles('HEAL_0002', 2);
  let c = await Save.commit(deps(), b); eq(c.ok, true); blobs.delete(files('HEAL_0002').png); await repair({}); eq(rec('4200000202').repair.state, 'files-missing');
  c = await Save.commit(deps(), b); eq([c.ok, c.error, c.which], [false, 'not-uploaded', 'thumb'], 'the run asks for the picture again: ' + JSON.stringify(c));
  putFiles('HEAL_0002', 2); c = await Save.commit(deps(), b); eq(c.ok, true, JSON.stringify(c)); await repair({}); eq(rec('4200000202').repair, undefined);
});
ftest('F3 a commit cut after k writes, then a repair pass, then the next run: one whole save, never a duplicate or a lost part', async () => {
  for (let k = 0; k <= 7; k++) {
    reset(); const b = cbody('4200000203', '1800000203', 'HEAL_0003', 'HEAL OLD3', 3); putFiles('HEAL_0003', 3, clk - 2 * H); TX.hook.crash = { after: k, torn: true };
    try { await Save.commit(deps(), b); } catch (_) { /* cut */ } TX.hook.crash = null;
    await repair({});
    const c = await Save.commit(deps(), b); eq(c.ok, true, 'k=' + k + ': ' + JSON.stringify(c));
    const entries = [...store.keys()].filter(x => x.startsWith('Charm_Master_Index/')); eq(entries, ['Charm_Master_Index/' + c.sku], 'k=' + k + ': one entry');
    eq(alias('1800000203').bySku['HEAL OLD3'], c.sku, 'k=' + k + ': the answer'); eq(rec('4200000203').sku, c.sku, 'k=' + k + ': the record'); ok(blobs.has(files(c.sku).ai) && blobs.has(files(c.sku).png), 'k=' + k + ': the files');
    await repair({}); eq(rec('4200000203').repair, undefined, 'k=' + k + ': whole');
  }
});

/* ═══ E · wiring ═══ */
test('E1 the library op: registered, free by default, sandbox leaves no trace, a failing step never fails the call', async () => {
  reset(); ok(typeof lib.ops.customSaveRepair === 'function', 'customSaveRepair is an op');
  const w0 = writes.length, f0 = fileWrites.length, r = await post({ op: 'customSaveRepair', sandbox: true }); eq([r.status, r.body.ok, r.body.sandbox, writes.length, fileWrites.length], [200, true, true, w0, f0]);
  const orig = Fk.bucket.getFiles; Fk.bucket.getFiles = async () => { throw new Error('storage is down'); };
  try { const r2 = await post({ op: 'customSaveRepair', force: true }); eq([r2.status, r2.body.ok], [200, true]); ok(r2.body.orphans.error, 'the failed step says so'); ok(r2.body.records && !r2.body.records.error, 'the others ran'); } finally { Fk.bucket.getFiles = orig; }
});

(async () => {
  let fails = 0;
  for (const [name, fn] of T) {
    if (process.env.ONLY && !new RegExp(process.env.ONLY).test(name)) continue;
    try { await fn(); process.stdout.write('  ok  ' + name.slice(0, 118) + '\n'); } catch (e) { fails++; console.warn = warn; console.error = err; console.error('FAIL ' + name + '\n' + (e && e.stack || e)); console.warn = () => {}; console.error = () => {}; }
  }
  console.warn = warn; console.error = err;
  if (fails) { console.error('keep-repair: ' + fails + ' case(s) failed'); process.exit(1); }
  console.log('keep-repair OK: ' + T.length + ' cases, ' + n + ' checks'); process.exit(0);
})();
