// KEEP (16 Oct 2026, KB): the SERVER side of "drop a vector file on a Review order + SAVE ON -> the permanent Charm repository under the listing's SKU", under chaos.
// Offline, all fakes (tests/charm-nest/_sandboxFakes.cjs: in-memory Firestore + Storage; a fake Etsy): no network, no Etsy, no real repository.
// The REAL handler (charmNestLibrary.js), the REAL password door (charmNestOutput.js + _charmNestRepoAuth.js), the REAL _charmNestCustomSave.js / _charmNestEtsySku.js / _charmNestSkuCheck.js.
//
// The rules under test (CONTRACT-KEEP B1-B8; each section below FAILS on the server as it was on origin/main 3a4e42f0):
//   K1  one DRAWING is one repository entry: the same drawing saved again by another tab / another order of the listing / a stale library is found by its charm hash ON THE SERVER
//   K2  the listing's answer (listing + the line's SKU) is never replaced: the same drawing there = "already linked"; ANOTHER design = listing-linked (first link wins); a dangling answer is replaced
//   K3  the commit converges: a transaction cut half way (a torn store) is completed by the next call, never duplicated, never "sku-taken" against itself
//   K4  the stored file must be the bytes the page built (file.aiSha256): two saves that chose the same SKU never swap files
//   K5  orphan files (uploaded, never committed) are removed after a day, files of entries never
//   K6  the same order, another drawing: already-saved sameFile:false (first design stays); one order with two listings keeps both saves
//   K7  the listing SKU reading rules: missing / lower case / spaces / "/" / __x__ / too long / equal to the new SKU: always a calm, final answer
//   K8  taken by another design: the next free candidate, and one always exists
//   K10 the server's memory: intents, and the saves that still wait for their Etsy step; the Etsy step is retryable and idempotent
//   node tests/charm-nest/keep-server-chaos.cjs
'use strict';
const path = require('path'), assert = require('assert/strict'), crypto = require('crypto');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret';
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const sha = b => crypto.createHash('sha256').update(b).digest('hex');

const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const { store, blobs, writes, fileWrites, canon } = Fk;
const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
let netCalls = 0; global.fetch = async () => { netCalls++; throw new Error('no network in this test'); };

/* ── Storage: a download token on every file, and a clock for "updated" the test can set ── */
const ages = new Map();   // path → ms of the last update (default: now)
const origFile = Fk.bucket.file.bind(Fk.bucket);
Fk.bucket.file = p => { const f = origFile(p), gm = f.getMetadata; f.getMetadata = async () => { const [m] = await gm.call(f); return [Object.assign({}, m, { updated: new Date(ages.has(p) ? ages.get(p) : Date.now()).toISOString(), metadata: { firebaseStorageDownloadTokens: 'tok' + String(p.length) } })]; }; return f; };

/* ── Firestore: transactions that behave like the real ones: serialised, optimistic (a document read that moved before the commit makes the callback run again), all-or-nothing;
      plus the hooks chaos needs: a competing writer that lands between the reads and the commit, a crash after k writes (atomic = rolled back, torn = left half written) ── */
const ver = new Map(), origSet = store.set.bind(store), origDel = store.delete.bind(store); let indexVer = 0;
const bump = k => { ver.set(k, (ver.get(k) || 0) + 1); if (k.startsWith('Charm_Master_Index/')) indexVer++; };
store.set = (k, v) => { bump(k); return origSet(k, v); }; store.delete = k => { bump(k); return origDel(k); };
const hook = { beforeApply: null, crash: null };   // beforeApply: async (attempt) => {}; crash: { after: k, torn: bool }
let lock = Promise.resolve(), bypass = 0, txRuns = 0, txRetries = 0;
Fk.db.runTransaction = function (fn) {
  const run = async () => {
    for (let attempt = 0; attempt < 8; attempt++) {
      txRuns++;
      const reads = new Map(), ops = []; let queryRead = false; const idx0 = indexVer;
      const tx = {
        get: async r => { if (r && r.path) reads.set(r.path, ver.get(r.path) || 0); else queryRead = true; return r.get(); },
        getAll: async (...refs) => { for (const r of refs) reads.set(r.path, ver.get(r.path) || 0); return Fk.db.getAll(...refs); },
        set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)), delete: r => ops.push(() => r.delete()), create: (r, d) => ops.push(() => r.create(d))
      };
      const out = await fn(tx);
      if (hook.beforeApply) { const h = hook.beforeApply; hook.beforeApply = null; bypass++; try { await h(attempt); } finally { bypass--; } }
      if ([...reads].some(([k, v]) => (ver.get(k) || 0) !== v) || (queryRead && indexVer !== idx0)) { txRetries++; continue; }   // somebody wrote what we read: run again, as Firestore does
      const snapshot = new Map(store), snapVer = new Map(ver), c = ops.length ? hook.crash : null; if (c) hook.crash = null;   // (a crash is aimed at the transaction that WRITES: the read-only pass before the files are checked has no writes)
      try {
        let i = 0;
        for (const op of ops) { if (c && c.after === i) throw new Error('crash after ' + i + ' writes'); await op(); i++; }
        if (c && c.after >= ops.length) throw new Error('crash at the end');
      } catch (e) {
        if (!(c && c.torn)) { store.clear(); for (const [k, v] of snapshot) origSet(k, v); ver.clear(); for (const [k, v] of snapVer) ver.set(k, v); }   // all-or-nothing, as Firestore
        throw e;
      }
      return out;
    }
    throw new Error('too much contention');
  };
  if (bypass) return run();
  const p = lock.then(run, run); lock = p.catch(() => {}); return p;
};

const lib = require(path.join(fnDir, 'charmNestLibrary.js')), output = require(path.join(fnDir, 'charmNestOutput.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
const Save = require(path.join(fnDir, '_charmNestCustomSave.js')), EtsySku = require(path.join(fnDir, '_charmNestEtsySku.js')), Master = require(path.join(fnDir, '_charmNestMaster.js'));
const O = require(path.join(root, 'charm-nest-orders.js'));
const post = async b => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
const door = async b => { const r = await output.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };

/* ── a design is a number: its bytes, its picture, its charm hash, the dropped file's hash ── */
const AIOF = d => Buffer.from('%PDF-1.4\n%drawing ' + d + '\n' + ('0 0 m 10 10 l S\n'.repeat(30)));
const PNGOF = d => Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), Buffer.from('picture ' + d), Buffer.alloc(80, d)]);
const HASHOF = d => 'drawing-' + d, FILEOF = d => sha(Buffer.from('dropped-file-' + d));
let TOK; const tokenOf = async action => (await RA.issue(Fk.db, { action, by: 'Tester' })).token;
/** the page's two uploads (the door is asked with the drawing's charmHash). Answers null, or the door's refusal (409 sku-taken: the SKU's files belong to another design) as the page maps it. */
const upload = async (sku, d, o) => {
  const hash = o && 'hash' in o ? o.hash : HASHOF(d);
  const a = await door({ op: 'put', path: `charmnest/master/custom/${sku}.ai`, contentType: 'application/illustrator', base64: AIOF(d).toString('base64'), repoToken: TOK, charmHash: hash });
  if (a.status === 409) return a.body;
  const p = await door({ op: 'put', path: `charmnest/master/custom/${sku}.png`, contentType: 'image/png', base64: PNGOF(d).toString('base64'), repoToken: TOK, charmHash: hash });
  assert(a.status === 200 && p.status === 200, 'upload ' + sku + ' ' + a.status + ' ' + p.status);
  return null;
};
/** what the page sends (saveCustom): rid, listing, the line's SKU, the candidate SKU, the drawing number. up:false = the files are not (re)uploaded; declared:false = an old page (no file hashes) */
const commitBody = o => ({ op: 'customSaveCommit', repoToken: TOK, rid: o.rid, listingId: o.listing, sku: o.sku, oldSku: o.old == null ? '' : o.old, productId: o.product || '', optionPairs: [], group: { mode: 'all', productIds: [] }, title: 'Test charm', by: 'Paul',
  design: { name: 'Design_' + o.d + '.ai', kind: 'ai', hash: FILEOF(o.d), bytes: 4000 }, reuse: false,
  entry: { charmHash: HASHOF(o.d), widthPt: 31.2, heightPt: 38.5, areaPt2: 790.4, members: 4, holes: 1, engravable: true, upAngle: o.up == null ? 90 : o.up, upSource: 'long' },
  file: Object.assign({ masterHash: FILEOF(o.d), name: 'Design_' + o.d + '.ai' }, o.declared === false ? {} : { aiSha256: sha(AIOF(o.d)), aiBytes: AIOF(o.d).length, pngSha256: sha(PNGOF(o.d)) }) });
const save = async o => { if (o.up !== false) { const no = await upload(o.sku, o.d); if (no) return Object.assign({ gate: true }, no); } const r = await post(commitBody(o)); return r.body; };
const idxOf = () => [...store.entries()].filter(([k]) => k.startsWith('Charm_Master_Index/')).map(([k, v]) => [k.slice(19), v]);
const entriesOfDrawing = d => idxOf().filter(([, e]) => e.charmHash === HASHOF(d)).map(([k]) => k);
const alias = l => store.get('Charm_Sku_Aliases/' + l), rec = id => store.get('Charm_Custom_Saves/' + id);
const snap = () => new Map([...store.entries()].map(([k, v]) => [k, canon(v)]));
const changed = (a, b) => [...new Set([...a.keys(), ...b.keys()])].filter(k => !k.startsWith('CharmRepo_Tokens/') && a.get(k) !== b.get(k)).sort();

/** The truth the whole repository must have whenever no call is in flight (the "no litter" rules). */
function invariants(label) {
  const byDrawing = new Map(), saves = [...store.entries()].filter(([k]) => k.startsWith('Charm_Custom_Saves/'));
  for (const [sku, e] of idxOf()) {
    if (e.hashSource !== 'custom-save') continue;
    if (!byDrawing.has(e.charmHash)) byDrawing.set(e.charmHash, []); byDrawing.get(e.charmHash).push(sku);
    ok(e.aiPath && blobs.has(e.aiPath) && blobs.has(e.thumbPath), label + ': the files of ' + sku + ' are in Storage');
    const d = +String(e.charmHash).replace('drawing-', '');
    ok(blobs.get(e.aiPath).buf.equals(AIOF(d)) && blobs.get(e.thumbPath).buf.equals(PNGOF(d)), label + ': the stored file of ' + sku + ' IS the drawing ' + d + ' (hash equal to what was dropped)');
    ok(saves.some(([, s]) => s.sku === sku), label + ': ' + sku + ' is the design of a save record (no entry without its record)');
  }
  for (const [h, skus] of byDrawing) eq(skus.length, 1, label + ': drawing ' + h + ' is ONE entry, not ' + skus.join(','));
  for (const [id, s] of saves) {
    ok(store.has('Charm_Master_Index/' + s.sku), label + ': the record ' + id + ' names an entry that exists');
    if (s.alias === 'bySku') ok(alias(s.listingId) && Object.values(alias(s.listingId).bySku || {}).length >= 1, label + ': the alias of ' + s.listingId + ' exists');
  }
  for (const [k, a] of store) {
    if (!k.startsWith('Charm_Sku_Aliases/') || !a.keep || !a.keep.rid) continue;
    for (const v of Object.values(a.bySku || {}).concat(a.sku ? [a.sku] : [])) ok(store.has('Charm_Master_Index/' + v), label + ': the answer ' + v + ' of ' + k + ' has an entry (no alias without an entry)');
  }
}

(async () => {
  TOK = await tokenOf('upload');
  // the repository as it is: one real master design, a person's link, an unrelated listing
  store.set('Charm_Master_Index/HEART_1111', { sku: 'HEART_1111', masterHash: 'f'.repeat(64), masterName: 'Hearts.ai', charmHash: 'heartHash0000001', widthPt: 20, heightPt: 22, areaPt2: 300, members: 2, holes: 0, aiPath: 'charmnest/master/HEART_1111.ai', thumbPath: 'charmnest/master/HEART_1111.png', engravable: true, upAngle: 0, upSource: 'drawn', open: false, labelSource: 'text', hashSource: 'browser' });
  store.set('Charm_Sku_Aliases/1700000001', { listingId: '1700000001', bySku: { 'WAS OLD': 'HEART_1111' }, title: 'Hearts', by: 'Ana' });

  /* ═══ K1 / K2 · one drawing is one entry; the listing's answer is never replaced ═══ */
  const L1 = '1800000001', L2 = '1800000002';
  let r = await save({ rid: '4200000001', listing: L1, old: 'X ONE', sku: 'ONE_0001', d: 1 });
  eq([r.ok, r.created, r.how, r.sku], [true, true, 'created', 'ONE_0001'], 'the first save creates the entry');
  invariants('first save');
  const w1 = writes.length;
  r = await save({ rid: '4200000001', listing: L1, old: 'X ONE', sku: 'ONE_0001', d: 1, up: false });
  eq([r.ok, r.created, r.how, r.sku, writes.length], [true, false, 'again', 'ONE_0001', w1], 'the same order again (a lost reply): the record, nothing written');
  r = await save({ rid: '4200000001', listing: L1, old: 'X ONE', sku: 'ONE_9999', d: 1 });
  eq([r.ok, r.created, r.how, r.sku], [true, false, 'again', 'ONE_0001'], 'a second tab with another candidate SKU for the same drawing: the saved SKU is THE SKU');
  const w2 = writes.length;
  r = await save({ rid: '4200000002', listing: L1, old: 'X ONE', sku: 'ONE_0002', d: 1 });   // another order of the same listing; its page did not know the first save (stale library)
  eq([r.ok, r.created, r.how, r.sku], [true, false, 'listing-same', 'ONE_0001'], 'K1: another order of the listing, same drawing, stale library: ALREADY LINKED to the first SKU');
  eq(entriesOfDrawing(1), ['ONE_0001'], 'K1: still ONE entry for the drawing');
  ok(!blobs.has('charmnest/master/custom/ONE_0002.ai') && !blobs.has('charmnest/master/custom/ONE_0002.png') && !blobs.has('charmnest/master/custom/ONE_9999.ai'), 'no litter: the files uploaded under the candidate SKUs that were not used are gone at once');
  eq(alias(L1).bySku, { 'X ONE': 'ONE_0001' }, 'K2: the listing\'s answer is the first SKU');
  eq(writes.slice(w2).map(w => w.kind + ' ' + w.path), ['create Charm_Custom_Saves/4200000002'], 'only that order\'s record was written');
  ok(rec('4200000002').sku === 'ONE_0001', 'its record names the first SKU');
  r = await save({ rid: '4200000003', listing: L2, old: 'Y TWO', sku: 'ONE_0003', d: 1 });   // the same drawing on ANOTHER listing
  eq([r.ok, r.created, r.how, r.sku], [true, false, 'same-design', 'ONE_0001'], 'K1: the same drawing on another listing: the existing design is used');
  eq([entriesOfDrawing(1), alias(L2).bySku], [['ONE_0001'], { 'Y TWO': 'ONE_0001' }], '... one entry, both listings answer with it');
  const b3 = snap();
  r = await save({ rid: '4200000004', listing: L1, old: 'X ONE', sku: 'FOUR_0004', d: 2 });   // ANOTHER design for a listing+SKU already answered
  eq([r.ok, r.error, r.sku, r.status], [false, 'listing-linked', 'ONE_0001', 200], 'K2: another design for an answered listing and SKU: listing-linked (first link wins), HTTP 200');
  eq([changed(b3, snap()).filter(k => !/^Charm_Custom_Saves|Charm_Master_Files/.test(k)), store.has('Charm_Master_Index/FOUR_0004'), store.has('Charm_Custom_Saves/4200000004')], [[], false, false], '... nothing was recorded: no entry, no record, the alias untouched');
  ok(!blobs.has('charmnest/master/custom/FOUR_0004.ai') && !blobs.has('charmnest/master/custom/FOUR_0004.png'), '... and the refused call\'s uploaded files are removed at once (no litter)');
  r = await save({ rid: '4200000005', listing: L1, old: 'X OTHER', sku: 'FIVE_0005', d: 2 });   // another variation (its own SKU) of the listing: its own answer
  eq([r.ok, r.created, r.how], [true, true, 'created'], 'another SKU of the listing is its own answer');
  eq(alias(L1).bySku, { 'X ONE': 'ONE_0001', 'X OTHER': 'FIVE_0005' }, '... added beside the first, nothing clobbered');
  store.set('Charm_Sku_Aliases/1800000003', { listingId: '1800000003', bySku: { 'LINKED BY HAND': 'HEART_1111' }, by: 'Paul' });
  r = await save({ rid: '4200000006', listing: '1800000003', old: 'linked by hand', sku: 'SIX_0006', d: 3 });
  eq([r.ok, r.error, r.sku, r.by], [false, 'listing-linked', 'HEART_1111', 'repository'], 'K2: a person\'s link (any case of the SKU) is never replaced');
  store.set('Charm_Sku_Aliases/1800000004', { listingId: '1800000004', bySku: { 'DANGLING': 'GONE_0000' }, by: 'Paul' });
  r = await save({ rid: '4200000007', listing: '1800000004', old: 'Dangling', sku: 'SEVEN_0007', d: 3 });
  eq([r.ok, r.created, alias('1800000004').bySku, alias('1800000004').keep.was], [true, true, { DANGLING: 'SEVEN_0007' }, 'GONE_0000'], 'K3: an answer whose design is gone is dangling: replaced, kept in keep.was');
  r = await save({ rid: '4200000001', listing: L1, old: 'X ONE', sku: 'ONE_0001', d: 7, up: false });
  eq([r.ok, r.error, r.sku, r.sameFile], [false, 'already-saved', 'ONE_0001', false], 'K6: the same order with ANOTHER drawing: already-saved sameFile:false, the first design stays');
  invariants('K1/K2');

  /* ═══ K8 · taken by another design: the next candidate, always one ═══ */
  const filesOne = [blobs.get('charmnest/master/custom/ONE_0001.ai').buf, blobs.get('charmnest/master/custom/ONE_0001.png').buf];
  r = await save({ rid: '4200000010', listing: '1800000010', old: 'Z', sku: 'ONE_0001', d: 4 });
  eq([r.gate, r.ok, r.error, r.by], [true, false, 'sku-taken', 'master'], 'K4/K8: the door refuses to sign/put files over a SKU whose entry exists (another drawing): sku-taken, before a byte is written');
  ok(blobs.get('charmnest/master/custom/ONE_0001.ai').buf.equals(filesOne[0]) && blobs.get('charmnest/master/custom/ONE_0001.png').buf.equals(filesOne[1]), 'K4: the files of the entry are untouched');
  r = await post(commitBody({ rid: '4200000010', listing: '1800000010', old: 'Z', sku: 'ONE_0001', d: 4 })); r = r.body;
  eq([r.ok, r.error, r.by], [false, 'sku-taken', 'master'], 'K8: a commit for a SKU of another drawing is sku-taken');
  eq(await upload('ONE_0001', 1), null, 'the same drawing may upload again (an idempotent retry after a lost reply)');
  filesOne[0] = blobs.get('charmnest/master/custom/ONE_0001.ai').buf;
  r = await post({ op: 'customSkuCheck', listingId: '1800000010', candidates: ['ONE_0001'] });
  eq([r.body.ok, r.body.free, r.body.extended], [true, 'ONE_0001_A', true], 'K8: the check goes on past the candidates it was given');
  store.set('Charm_Master_Index/ONE_0001_A', { sku: 'ONE_0001_A' });
  r = await post({ op: 'customSkuCheck', listingId: '1800000010', candidates: ['ONE_0001'] });
  eq(r.body.free, 'ONE_0001_B', '... letter by letter'); store.delete('Charm_Master_Index/ONE_0001_A');
  r = await save({ rid: '4200000010', listing: '1800000010', old: 'Z', sku: 'ONE_0001_A', d: 4 });
  eq([r.ok, r.created], [true, true], 'K8: and the design is saved under it');
  invariants('K8');

  /* ═══ K1 / K2 under RACES: a competing writer lands between the reads and the commit; the callback runs again from the truth ═══ */
  for (const [label, dB, expectB] of [['same drawing', 5, 'listing-same'], ['another drawing', 6, 'listing-linked']]) {
    const L = label === 'same drawing' ? '1800000020' : '1800000021';
    await upload('RACE_A' + dB, 5); await upload('RACE_B' + dB, dB);
    const bodyA = commitBody({ rid: '42000001' + dB + '1', listing: L, old: 'RACE OLD', sku: 'RACE_A' + dB, d: 5 }), bodyB = commitBody({ rid: '42000001' + dB + '2', listing: L, old: 'RACE OLD', sku: 'RACE_B' + dB, d: dB });
    const retries0 = txRetries;
    hook.beforeApply = async () => { const rb = await post(bodyB); hook.rb = rb.body; };   // B commits completely while A is between its reads and its commit
    const ra = await post(bodyA);
    ok(txRetries > retries0, label + ': A\'s transaction really ran again (' + (txRetries - retries0) + ' retry)');
    const won = hook.rb;
    eq([won.ok, won.how], [true, 'created'], label + ': the competing commit won (created)');
    if (expectB === 'listing-same') eq([ra.body.ok, ra.body.how, ra.body.sku, ra.body.created], [true, 'listing-same', 'RACE_B' + dB, false], 'K1: the loser sees "already linked" to the winner\'s SKU, no second entry');
    else eq([ra.body.ok, ra.body.error, ra.body.sku], [false, 'listing-linked', 'RACE_B' + dB], 'K2: the loser (another design) sees listing-linked to the winner\'s SKU');
    eq(alias(L).bySku, { 'RACE OLD': 'RACE_B' + dB }, label + ': the alias is the winner\'s, not clobbered by the loser');
    eq(entriesOfDrawing(dB), ['RACE_B' + dB], label + ': the winner\'s drawing is ONE entry, the loser\'s candidate SKU has none');
  }
  eq([entriesOfDrawing(5), entriesOfDrawing(6)], [['RACE_B5'], ['RACE_B6']], 'no duplicate entry for either drawing after both races');
  // eight commits at once: 2 drawings x (3 orders of one listing + 1 order of another listing)
  {
    const Lr = '1800000030', Ls = '1800000031', jobs = [];
    for (const d of [20, 21]) for (const [i, listing] of [[1, Lr], [2, Lr], [3, Lr], [4, Ls]]) { const sku = `GOX_${d}${i}_Z`; await upload(sku, d); jobs.push(commitBody({ rid: `4200002${d}${i}`, listing, old: listing === Lr ? 'GO OLD' : 'GO OLD S', sku, d })); }
    const out = (await Promise.all(jobs.map(b => post(b)))).map(x => x.body);
    eq(out.filter(x => x.ok).length + out.filter(x => !x.ok).length, 8, 'eight answers');
    ok(out.every(x => x.ok || x.error === 'listing-linked'), 'every answer is ok or the calm listing-linked: ' + JSON.stringify(out.map(x => x.ok ? x.how : x.error)));
    ok(entriesOfDrawing(20).length <= 1 && entriesOfDrawing(21).length <= 1, 'at most one entry per drawing');
    eq(Object.keys(alias(Lr).bySku).length, 1, 'ONE answer for the listing and SKU, whoever won'); const winner = alias(Lr).bySku['GO OLD'];
    ok(out.filter(x => x.ok && x.sku !== winner && x.created === false).length === 0, 'every order that was told ok was told the winner\'s SKU or its own drawing\'s');
    invariants('eight at once');
  }

  /* ═══ K3 · a transaction cut half way: all-or-nothing on Firestore, and the next call completes even when the store is TORN ═══ */
  for (const torn of [false, true]) for (let k = 0; k <= 5; k++) {
    const tag = (torn ? 't' : 'a') + k, L = '18000004' + (torn ? 5 : 4) + k, d = 40 + k + (torn ? 10 : 0), sku = 'CUT_' + tag.toUpperCase(), rid = '4200004' + (torn ? 5 : 4) + k + '0';
    await upload(sku, d);
    const before = snap(); hook.crash = { after: k, torn };
    const bad = await post(commitBody({ rid, listing: L, old: 'CUT OLD', sku, d }));
    ok(bad.status >= 500 || bad.body.ok !== true || k >= 5, `${tag}: the cut commit is no success (${bad.status})`);
    if (!torn) eq(changed(before, snap()), [], `${tag}: all-or-nothing: nothing changed`);
    else ok(true, `${tag}: torn store left ${changed(before, snap()).length} document(s) written`);
    // the retry may be a DIFFERENT page / candidate (the first answer was lost)
    const again = await post(commitBody({ rid, listing: L, old: 'CUT OLD', sku: torn && k % 2 ? sku + '_B' : sku, d }));
    ok(again.body.ok === true, `${tag}: the next call completes: ` + JSON.stringify(again.body).slice(0, 120));
    ok(again.body.sku === sku, `${tag}: and it is the SAME entry (not a second SKU): ` + again.body.sku);
    eq([entriesOfDrawing(d), rec(rid) && rec(rid).sku, alias(L).bySku], [[sku], sku, { 'CUT OLD': sku }], `${tag}: one entry, its record, its alias`);
    invariants('cut ' + tag);
  }

  /* ═══ K4 · the file in Storage is the file the page built ═══ */
  {
    const L = '1800000050', SKU = 'SWAP_0050';
    await upload(SKU, 150);                 // save A uploads drawing 50 ...
    await upload(SKU, 151);                 // ... save B chose the same candidate and uploaded drawing 51 over it
    const b4 = snap();
    r = (await post(commitBody({ rid: '4200005001', listing: L, old: 'SW A', sku: SKU, d: 150 }))).body;
    eq([r.ok, r.error, r.by, r.why], [false, 'sku-taken', 'upload', 'changed'], 'K4: A\'s commit finds bytes that are not the ones A built: refused (sku-taken by upload), the page picks the next candidate');
    eq(changed(b4, snap()), [], 'K4: nothing was written');
    r = (await post(commitBody({ rid: '4200005002', listing: '1800000051', old: 'SW B', sku: SKU, d: 151 }))).body;
    eq([r.ok, r.created], [true, true], 'K4: B\'s commit (its own bytes) goes through');
    r = await save({ rid: '4200005001', listing: L, old: 'SW A', sku: 'SWAP_0052', d: 150 });
    eq([r.ok, r.created, r.sku], [true, true, 'SWAP_0052'], 'K4: A goes on with the next candidate and its own upload');
    invariants('K4');
    // a picture that is not the one built, a size that is not the one built, an old page (no hashes), missing files
    await upload('PIC_0060', 60); await door({ op: 'put', path: 'charmnest/master/custom/PIC_0060.png', contentType: 'image/png', base64: PNGOF(61).toString('base64'), repoToken: TOK });
    r = (await post(commitBody({ rid: '4200006001', listing: '1800000060', old: 'P', sku: 'PIC_0060', d: 60 }))).body;
    eq([r.ok, r.error, r.by], [false, 'sku-taken', 'upload'], 'K4: a picture that is not the one built is refused too');
    await upload('LEN_0061', 61); const bb = commitBody({ rid: '4200006101', listing: '1800000061', old: 'P', sku: 'LEN_0061', d: 61 }); bb.file.aiBytes += 1;
    r = (await post(bb)).body; eq([r.ok, r.error], [false, 'sku-taken'], 'K4: a size that is not the one built is refused');
    await upload('OLD_0062', 62); r = (await post(commitBody({ rid: '4200006201', listing: '1800000062', old: 'P', sku: 'OLD_0062', d: 62, declared: false }))).body;
    eq([r.ok, r.created], [true, true], 'an old page that declares no hashes is still accepted (as before)');
    r = (await post(commitBody({ rid: '4200006301', listing: '1800000063', old: 'P', sku: 'GONE_0063', d: 63 }))).body;
    eq([r.ok, r.error, r.which, r.why], [false, 'not-uploaded', 'ai', 'missing'], 'files missing: not-uploaded (transient: the page uploads again)');
    invariants('K4 b');
  }

  /* ═══ K5 · the sweep: leftovers of uploads whose commit never came go after a day; nothing else does ═══ */
  {
    const DAY = 24 * 3600e3;
    await upload('LEFT_0070', 70); await upload('LEFT_0071', 71); await upload('FRESH_0072', 72);
    for (const p of ['charmnest/master/custom/LEFT_0070.ai', 'charmnest/master/custom/LEFT_0070.png']) ages.set(p, Date.now() - 2 * DAY);
    ages.set('charmnest/master/custom/LEFT_0071.ai', Date.now() - 2 * DAY);   // its picture is fresh: the SKU stays whole
    const entryFiles = ['charmnest/master/custom/ONE_0001.ai', 'charmnest/master/custom/ONE_0001.png']; for (const p of entryFiles) ages.set(p, Date.now() - 30 * DAY);
    ok(blobs.has('charmnest/master/custom/LEFT_0070.ai'), 'the orphan is there before the next save');
    r = await save({ rid: '4200007001', listing: '1800000070', old: 'S', sku: 'SWEEP_0073', d: 73 });
    ok(r.ok === true, 'a save that triggers the sweep');
    ok(!blobs.has('charmnest/master/custom/LEFT_0070.ai') && !blobs.has('charmnest/master/custom/LEFT_0070.png'), 'K5: a day-old orphan (.ai and .png) is removed by the next save');
    ok(blobs.has('charmnest/master/custom/LEFT_0071.ai') && blobs.has('charmnest/master/custom/LEFT_0071.png'), 'K5: a SKU with one fresh file is kept whole');
    ok(blobs.has('charmnest/master/custom/FRESH_0072.ai'), 'K5: an upload of today (a save waiting for its password) is kept');
    ok(entryFiles.every(p => blobs.has(p)), 'K5: the files of an entry are never touched, however old');
    ok(blobs.has('charmnest/master/HEART_1111.ai') || !blobs.has('charmnest/master/HEART_1111.ai'), 'real master files are outside the swept folder');
    r = await save({ rid: '4200007002', listing: '1800000071', old: 'S', sku: 'LEFT_0071', d: 71, up: false });   // the page still holds its marks and commits without uploading
    eq([r.ok, r.error, r.which], [true, undefined, undefined], 'the fresh-file SKU can still be committed');
    invariants('K5');
  }

  /* ═══ K7 · the listing SKU reading rules: every case ends calm and final ═══ */
  {
    const cases = [
      ['lower case', 'acorn leaf', 'bySku', 'ACORN LEAF'], ['spaces kept inside, trimmed outside', '  Beady   Swan  ', 'bySku', 'BEADY   SWAN'], ['missing', '', 'whole', null],
      ['a slash is an ordinary Etsy SKU', 'RING/SIZE 7', 'bySku', 'RING/SIZE 7'], ['looks like __x__', '__x__', 'none', null], ['a control character', 'A\nB', 'bySku', 'A B'],
      ['reserved shape (the page reads it first)', 'CUSTOM_6673', 'bySku', 'CUSTOM_6673'], ['numbered family', 'WIDGET-12', 'bySku', 'WIDGET-12'], ['a long one (aliasPut keeps up to 120)', 'L'.repeat(100), 'bySku', 'L'.repeat(100)], ['too long: cut on the record, no key any order carries', 'L'.repeat(121), 'none', null]
    ];
    let i = 0;
    for (const [what, old, mode, key] of cases) {
      i++; const L = '18000008' + String(i).padStart(2, '0'), sku = 'READ_' + String(i).padStart(4, '0'), rid = '42000080' + String(i).padStart(2, '0');
      r = await save({ rid, listing: L, old, sku, d: 80 + i });
      ok(r.ok === true && r.created === true, 'K7 ' + what + ': saved calmly (' + JSON.stringify(r).slice(0, 100) + ')');
      eq(r.alias, mode, 'K7 ' + what + ': alias mode ' + mode);
      const a = alias(L);
      if (mode === 'bySku') eq(a.bySku, { [key]: sku }, 'K7 ' + what + ': the answer is keyed by the reading');
      else if (mode === 'whole') eq([a.sku, a.v, 'bySku' in a], [sku, 2, false], 'K7 ' + what + ': the listing\'s own answer');
      else ok(!a || (!a.bySku && !a.sku), 'K7 ' + what + ': no alias key (the Etsy step links it)');
      ok(rec(rid).oldSku.length <= 120, 'K7 ' + what + ': the record keeps the reading cut at 120');
    }
    r = await save({ rid: '4200008099', listing: '1800000899', old: 'M'.repeat(300), sku: 'READ_0099', d: 99 });
    eq([r.ok, r.alias, rec('4200008099').oldSku.length], [true, 'none', 120], 'K7: a 300-character SKU is saved, not keyed (a cut SKU is no key any order carries), and kept cut');
    r = await save({ rid: '4200008100', listing: '1800000900', old: 'HEART_9491', sku: 'HEART_9491', d: 100 });
    eq([r.ok, r.created, r.alias, alias('1800000900')], [true, true, 'none', undefined], 'K7: the line\'s SKU IS the design\'s SKU (the shop\'s shape): no alias needed');
    invariants('K7');
  }

  /* ═══ K6 · one order, two listings: both saved, both found ═══ */
  {
    const rid = '4200009001';
    r = await save({ rid, listing: '1800000901', old: 'TWO A', sku: 'DUO_0901', d: 110 });
    eq([r.ok, r.id], [true, rid], 'the first listing of the order: record {rid}');
    r = await save({ rid, listing: '1800000902', old: 'TWO B', sku: 'DUO_0902', d: 111 });
    eq([r.ok, r.id, r.created], [true, rid + '-1800000902', true], 'K6: the second listing of the same order has its own record {rid}-{listing}');
    eq([rec(rid).listingId, rec(rid + '-1800000902').listingId, rec(rid + '-1800000902').sku], ['1800000901', '1800000902', 'DUO_0902'], 'both records are right');
    r = await save({ rid, listing: '1800000902', old: 'TWO B', sku: 'DUO_0902', d: 111, up: false });
    eq([r.ok, r.how, r.sku], [true, 'again', 'DUO_0902'], '... and the second again is the second\'s record');
    r = (await post({ op: 'customSaveGet', rid })).body; eq(r.save.sku, 'DUO_0901', 'customSaveGet {rid} = the first listing (as before)');
    r = (await post({ op: 'customSaveGet', rid, listingId: '1800000902' })).body; eq([r.save.sku, r.save.id], ['DUO_0902', rid + '-1800000902'], 'customSaveGet {rid, listingId} = that listing\'s');
    r = (await post({ op: 'customSaveGet', rid, listingId: '1800000999' })).body; eq(r.save, null, '... a listing of the order not saved is null');
    invariants('two listings');
  }

  /* ═══ K10 · the server's memory: intents ═══ */
  {
    const rid = '4200009100', base = { op: 'customSaveIntent', rid, listingId: '1800000910', hash: HASHOF(120), name: 'Mine.ai', by: 'Ana' };
    const w0 = writes.length;
    r = await post(Object.assign({ sandbox: true, state: 'wanted' }, base)); eq([r.body.ok, r.body.sandbox, writes.length], [true, true, w0], 'a rehearsal intent writes nothing');
    r = await post(Object.assign({ state: 'nonsense' }, base)); eq(r.status, 400, 'a bad state is a 400');
    r = await post(Object.assign({ state: 'wanted' }, base)); eq([r.body.ok, r.body.intent.state, r.body.intent.hash], [true, 'wanted', HASHOF(120)], 'wanted is recorded');
    eq(writes.slice(w0).map(w => w.path), ['Charm_Custom_Intents/' + rid], 'the ONLY document written is the intent (never the repository)');
    r = (await post({ op: 'customSaveGet', rid })).body; eq([r.save, r.intent && r.intent.state], [null, 'wanted'], 'customSaveGet returns the intent while there is no save');
    r = await post(Object.assign({ state: 'cancelled' }, base)); eq(r.body.intent.state, 'cancelled', 'cancelled');
    r = await post(Object.assign({ state: 'wanted' }, base)); eq(r.body.intent.state, 'wanted', '... and wanted again');
    r = await save({ rid, listing: '1800000910', old: 'INT', sku: 'INTENT_0910', d: 120 });
    eq(r.ok, true, 'the save'); eq(store.get('Charm_Custom_Intents/' + rid).state, 'done', 'the commit closed the intent (done)');
    r = await post(Object.assign({ state: 'cancelled' }, base)); eq(r.body.intent.state, 'done', 'a late "cancelled" cannot undo a saved order');
    r = (await post({ op: 'customSaveGet', rid })).body; ok(r.save && r.save.sku === 'INTENT_0910' && !r.intent, 'customSaveGet returns the save, not the intent, once saved');
    // a commit cut after the record, before the intent: the next (again) call closes the intent
    const rid2 = '4200009101'; await post(Object.assign({ state: 'wanted' }, base, { rid: rid2, listingId: '1800000911' }));
    await upload('INTENT_0911', 121); hook.crash = { after: 4, torn: true };   // index, file record, alias, save written; intent not
    await post(commitBody({ rid: rid2, listing: '1800000911', old: 'INT2', sku: 'INTENT_0911', d: 121 }));
    ok(rec(rid2) && store.get('Charm_Custom_Intents/' + rid2).state === 'wanted', 'torn: the record is there, the intent still "wanted"');
    r = await save({ rid: rid2, listing: '1800000911', old: 'INT2', sku: 'INTENT_0911', d: 121, up: false });
    eq([r.how, store.get('Charm_Custom_Intents/' + rid2).state], ['again', 'done'], 'the next call closes the intent');
  }

  /* ═══ K10 · the Etsy step: only after a saved repository step, retryable, idempotent, and the server remembers what still waits ═══ */
  {
    const esc = s => String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;');
    const mkListing = (skus) => ({ products: skus.map((sku, i) => ({ product_id: 700 + i, sku, is_deleted: false, property_values: [{ property_id: 513, property_name: 'Color', scale_id: null, value_ids: [9000 + i], values: ['V' + i] }],
      offerings: [{ offering_id: 7000 + i, price: { amount: 2500, divisor: 100, currency_code: 'USD' }, quantity: 5, is_enabled: true, is_deleted: false, readiness_state_id: 7001 }] })), price_on_property: [], quantity_on_property: [], sku_on_property: [513] });
    const worlds = new Map();
    const world = (listingId, skus) => { const S = { stored: mkListing(skus), gets: 0, puts: 0, state: 'active', failPut: null, calls: [] }; worlds.set(listingId, S); return S; };
    const etsy = {
      async get(p) { const m = /^\/listings\/(\d+)(\/inventory)?$/.exec(p), S = worlds.get(m && m[1]); if (!S) throw Object.assign(new Error('404'), { status: 404 }); S.calls.push('GET ' + p); S.gets++; if (!m[2]) return { state: S.state };
        return JSON.parse(JSON.stringify(Object.assign({}, S.stored, { products: S.stored.products.map(x => Object.assign({}, x, { sku: esc(x.sku) })) }))); },
      async put(p, q, body) { const m = /^\/listings\/(\d+)\/inventory$/.exec(p), S = worlds.get(m[1]); S.calls.push('PUT'); S.puts++; if (S.failPut) { const f = S.failPut; S.failPut = null; throw f; }
        S.stored = { products: body.products.map((x, i) => Object.assign({ product_id: 800 + i + S.puts * 10 }, x, { is_deleted: false, offerings: x.offerings.map((o, j) => Object.assign({ offering_id: 8000 + i, is_deleted: false, price: { amount: Math.round(o.price * 100), divisor: 100, currency_code: 'USD' } }, { quantity: o.quantity, is_enabled: o.is_enabled }, o.readiness_state_id != null ? { readiness_state_id: o.readiness_state_id } : {})) })), price_on_property: body.price_on_property, quantity_on_property: body.quantity_on_property, sku_on_property: body.sku_on_property };
        return JSON.parse(JSON.stringify(Object.assign({}, S.stored, { products: S.stored.products.map(x => Object.assign({}, x, { sku: esc(x.sku) })) }))); } };
    const E = EtsySku.make({ db: Fk.db, FV: Fk.admin.firestore.FieldValue, bucket: Fk.bucket, etsy, meter: { bumpSimple() {} }, now: Date.now });
    const L = '1800000920', rid = '4200009200';
    let e = await E.customEtsySkuWrite({ rid });
    eq([e.ok, e.reason], [false, 'no-save'], 'the Etsy step with no saved record: no-save, nothing is asked of Etsy');
    world(L, ['LIVE OLD', 'LIVE OLD']);
    r = await save({ rid, listing: L, old: 'Live Old', sku: 'LIVE_0920', d: 130, product: '700' });
    ok(r.ok, 'saved'); const pend = (await post({ op: 'customSavePending', limit: 100 })).body;
    ok(pend.ok && pend.pending.some(x => x.rid === rid && x.etsy.state === 'pending'), 'K10: customSavePending lists the save whose Etsy step waits');
    store.get('Charm_Custom_Saves/' + rid).etsy.at = 0;
    worlds.get(L).failPut = Object.assign(new Error('timeout'), { status: 503 });
    e = await E.customEtsySkuWrite({ rid });
    eq([e.ok, e.state], [false, 'unverified'], 'the PUT may have landed: unverified (retryable)');
    const S = worlds.get(L); S.stored.products.forEach(x => { x.sku = 'LIVE_0920'; });   // ... and it had
    e = await E.customEtsySkuWrite({ rid });
    eq([e.ok, e.state, S.puts], [true, 'linked', 1], 'the retry reads first: already on Etsy, linked, NO second PUT');
    e = await E.customEtsySkuWrite({ rid }); eq([e.ok, e.state, e.calls, S.puts], [true, 'linked', 0, 1], 'again: no Etsy call at all');
    const pend2 = (await post({ op: 'customSavePending', limit: 100 })).body; ok(!pend2.pending.some(x => x.rid === rid), 'K10: a linked save is no longer pending');
    // the reserved-SKU save is listed FIRST (only Etsy can link it), a terminal failure and a fresh attempt are not listed
    r = await save({ rid: '4200009201', listing: '1800000921', old: 'CUSTOM_6673', sku: 'RES_0921', d: 131 });
    r = await save({ rid: '4200009202', listing: '1800000922', old: 'plain old', sku: 'PLAIN_0922', d: 132 });
    r = await save({ rid: '4200009203', listing: '1800000923', old: 'gone', sku: 'GONE_0923', d: 133 });
    store.get('Charm_Custom_Saves/4200009203').etsy = { state: 'failed', reason: 'listing-gone', retry: false, at: 1 };
    r = await save({ rid: '4200009204', listing: '1800000924', old: 'busy', sku: 'BUSY_0924', d: 134 });
    store.get('Charm_Custom_Saves/4200009204').etsy = { state: 'failed', reason: 'busy', retry: true, at: Date.now() };
    const p3rows = (await post({ op: 'customSavePending', limit: 100 })).body.pending, p3 = p3rows.map(x => x.rid);
    ok(p3.indexOf('4200009201') >= 0 && p3.indexOf('4200009201') < p3.indexOf('4200009202'), 'K10: pending: the save only Etsy can link (CUSTOM_6673) comes before one the alias already links');
    ok(p3rows.findIndex(x => !x.linkNeedsEtsy) > p3rows.map(x => x.linkNeedsEtsy).lastIndexOf(true), 'K10: every save only Etsy can link comes before every other');
    ok(!p3.includes('4200009203') && !p3.includes('4200009204'), 'K10: a terminal reason and an attempt a moment ago are not listed');
    eq((await post({ op: 'customSavePending', sandbox: true })).body.pending, [], 'the sandbox has nothing pending');
    // the line already carried the new SKU: linked with no Etsy call
    r = await save({ rid: '4200009205', listing: '1800000925', old: 'HEART_9925', sku: 'HEART_9925', d: 135 });
    e = await E.customEtsySkuWrite({ rid: '4200009205' }); eq([e.ok, e.state, e.noop, e.calls], [true, 'linked', true, 0], 'K7: old = new: linked with no call');
    // an unkeyable SKU is still linked on Etsy (alias none)
    world('1800000926', ['A/B', 'A/B']); r = await save({ rid: '4200009206', listing: '1800000926', old: 'A/B', sku: 'SLASH_0926', d: 136, product: '700' });
    e = await E.customEtsySkuWrite({ rid: '4200009206' }); eq([r.alias, e.ok, e.state, worlds.get('1800000926').stored.products.map(x => x.sku)], ['bySku', true, 'linked', ['SLASH_0926', 'SLASH_0926']], 'K7: a SKU with a slash is keyed and then linked on Etsy too');
    // two listings of one order: the Etsy step finds the right record with listingId
    world('1800000927', ['TW', 'TW']); await save({ rid: '4200009207', listing: '1800000928', old: 'OTHER', sku: 'TWO_0928', d: 137 });
    await save({ rid: '4200009207', listing: '1800000927', old: 'TW', sku: 'TWO_0927', d: 138, product: '700' });
    e = await E.customEtsySkuWrite({ rid: '4200009207', listingId: '1800000927' }); eq([e.ok, e.sku, worlds.get('1800000927').puts], [true, 'TWO_0927', 1], 'K6: customEtsySkuWrite {rid, listingId} works the second listing\'s record');
    // two orders of one listing, one drawing: both records name the one SKU; the Etsy step of the second does the one PUT
    world('1800000960', ['PAIR OLD', 'PAIR OLD']);
    await save({ rid: '4200009600', listing: '1800000960', old: 'Pair Old', sku: 'PAIR_0960', d: 160, product: '700' });
    r = await save({ rid: '4200009601', listing: '1800000960', old: 'Pair Old', sku: 'PAIR_0961', d: 160, product: '700' });
    eq([r.how, r.sku], ['listing-same', 'PAIR_0960'], 'the second order of the listing is already linked');
    e = await E.customEtsySkuWrite({ rid: '4200009601' }); eq([e.ok, e.sku, worlds.get('1800000960').puts], [true, 'PAIR_0960', 1], 'the second order\'s Etsy step does the one PUT');
    // (the FIRST order's step after that PUT: Etsy has given the products new ids, so its product is found by what Etsy holds, not by id: KR's part of the Etsy step, tests/charm-nest/keep-repair.cjs)
    invariants('etsy');
  }

  /* ═══ a real master design with the same drawing but a SKU the save machinery does not own (spaces: "FOO BAR") is not taken over ═══ */
  {
    store.set('Charm_Master_Index/FOO BAR', { sku: 'FOO BAR', masterHash: 'e'.repeat(64), masterName: 'Foo.ai', charmHash: HASHOF(170), widthPt: 20, heightPt: 20, areaPt2: 300, members: 1, holes: 1, aiPath: 'charmnest/master/FOO_BAR.ai', thumbPath: 'charmnest/master/FOO_BAR.png', engravable: true, upAngle: 0, upSource: 'drawn', open: false, hashSource: 'browser' });
    r = await save({ rid: '4200009700', listing: '1800000970', old: 'FOO OLD', sku: 'FOOBAR_0970', d: 170 });
    eq([r.ok, r.created, r.sku], [true, true, 'FOOBAR_0970'], 'a drawing that is in the Library under a SKU with spaces is saved under its own custom SKU (the Etsy step can write it)');
  }

  /* ═══ B9 · the hash can be made mandatory (the lead flips it when every page sends it) ═══ */
  {
    Save.config.requireFileHash = true;
    await upload('HASH_0950', 150 + 5);
    const noHash = commitBody({ rid: '4200009500', listing: '1800000950', old: 'H', sku: 'HASH_0950', d: 155, declared: false });
    r = await post(noHash); eq([r.status, r.body.error, r.body.field], [400, 'bad-request', 'file.aiSha256'], 'mandatory: a commit without the built file\'s hash is a 400');
    r = await post(commitBody({ rid: '4200009500', listing: '1800000950', old: 'H', sku: 'HASH_0950', d: 155 })); eq([r.body.ok, r.body.created], [true, true], '... with it, it is saved');
    Save.config.requireFileHash = false;
  }

  /* ═══ stale records: a record whose entry or files are gone is not "saved" ═══ */
  {
    const L = '1800000940';
    r = await save({ rid: '4200009400', listing: L, old: 'STALE', sku: 'STALE_0940', d: 140 });
    ok(r.ok && r.created, 'saved'); store.delete('Charm_Master_Index/STALE_0940');   // a Library delete of the design: the record and the alias remain
    r = await save({ rid: '4200009400', listing: L, old: 'STALE', sku: 'STALE_0940', d: 140, up: false });
    eq([r.ok, r.created, r.how, r.sku], [true, true, 'created', 'STALE_0940'], 'a record whose entry is gone is stale: the design is made again (never "again" with no design behind it)');
    eq([!!store.get('Charm_Master_Index/STALE_0940'), rec('4200009400').etsy, rec('4200009400').sku], [true, { state: 'pending' }, 'STALE_0940'], '... the record replaced, the entry back');
    r = await save({ rid: '4200009401', listing: '1800000941', old: 'FILES', sku: 'FILES_0941', d: 141 });
    blobs.delete('charmnest/master/custom/FILES_0941.png');
    r = await save({ rid: '4200009401', listing: '1800000941', old: 'FILES', sku: 'FILES_0941', d: 141, up: false });
    eq([r.ok, r.error, r.which, r.why], [false, 'not-uploaded', 'thumb', 'missing'], 'an entry whose picture is gone: not-uploaded (the page uploads again)');
    r = await save({ rid: '4200009401', listing: '1800000941', old: 'FILES', sku: 'FILES_0941', d: 141 });
    eq([r.ok, r.how], [true, 'again'], '... and after the upload the same record answers again');
    invariants('stale records');
    // an Etsy step whose function died while "writing": reported as unverified/interrupted, listed as pending
    store.get('Charm_Custom_Saves/4200009401').etsy = { state: 'writing', runningUntil: Date.now() - 1000, at: 1 };
    r = (await post({ op: 'customSaveGet', rid: '4200009401' })).body;
    eq([r.save.etsy.state, r.save.etsy.reason, r.save.etsy.retry], ['unverified', 'interrupted', undefined], 'KR: a "writing" past its lease is unverified (interrupted)');
    ok((await post({ op: 'customSavePending', limit: 100 })).body.pending.some(x => x.rid === '4200009401' && x.etsy.state === 'unverified'), '... and it is listed as pending');
    store.get('Charm_Custom_Saves/4200009401').etsy = { state: 'writing', runningUntil: Date.now() + 60000, at: 1 };
    ok(!(await post({ op: 'customSavePending', limit: 100 })).body.pending.some(x => x.rid === '4200009401'), 'a "writing" inside its lease is somebody\'s run: not listed');
  }

  ok(netCalls === 0, 'nothing reached for the network');
  console.warn = warn; console.error = err;
  console.log(`keep-server-chaos OK: ${n} checks (${txRuns} transaction runs, ${txRetries} re-run after a competing writer)`);
})().catch(e => { console.warn = warn; console.error = err; process.stderr.write(String(e && e.stack || e) + '\n'); process.exit(1); });
