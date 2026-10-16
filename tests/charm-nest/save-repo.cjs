// REPOSAVE (11 Oct 2026; plans/save-to-repo-1011/CONTRACT.md sections 4, 7, 8): a custom design saved to the charm repository.
// Offline, all fakes: no live write, no live Firestore, no Etsy call, no paid call.
//   1 · the server: customSaveCommit / customSaveGet through the REAL charmNestLibrary handler and the REAL password door, on the in-memory
//       Firestore and Storage of _sandboxFakes.cjs (netlify/functions/_charmNestCustomSave.js)
//   2 · the engine (charm-nest-keep.js) on injected fakes: step order, locked path, password then continue, token expiry, offline, retry, no
//       double upload, Etsy only after the repository, call counts, the sandbox rehearsal
//   3 · Master.saveCustom in a real Chromium against the REAL handlers on the fake site (skipped without playwright-core + Chromium):
//       the file is built and uploaded under charmnest/master/custom/ with the upload token, the records land, no dialog opens
//   node tests/charm-nest/save-repo.cjs   (PW_DIR=<dir with playwright-core>, CHROMIUM=<chrome> for part 3)
'use strict';
const path = require('path'), assert = require('assert/strict'), crypto = require('crypto');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

/* ═══ 1 · the server ═══ */
async function server() {
  const Fk = require('./_sandboxFakes.cjs'); Fk.install();
  const { store, blobs, writes, fileWrites, canon } = Fk;
  const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
  // the fake bucket carries a download token on every file (the real one gets it from the upload's finalize), so the links can be minted
  const origFile = Fk.bucket.file.bind(Fk.bucket);
  Fk.bucket.file = p => { const f = origFile(p), gm = f.getMetadata; f.getMetadata = async () => { const [m] = await gm.call(f); return [Object.assign({}, m, { metadata: { firebaseStorageDownloadTokens: 'tok' + String(p.length) } })]; }; return f; };
  let netCalls = 0; global.fetch = async () => { netCalls++; throw new Error('no network in this test'); };
  const lib = require(path.join(fnDir, 'charmNestLibrary.js')), output = require(path.join(fnDir, 'charmNestOutput.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
  const O = require(path.join(root, 'charm-nest-orders.js')), Save = require(path.join(fnDir, '_charmNestCustomSave.js'));
  const post = async b => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
  const door = async b => { const r = await output.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
  const tokenOf = async action => (await RA.issue(Fk.db, { action, by: 'Tester' })).token;
  const AI = Buffer.from('%PDF-1.4\n' + '%charm '.repeat(80)), PNG = Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), Buffer.alloc(80, 7)]);
  const up = async (sku, tok, o) => {   // the browser's two uploads, through the real file door (it asks for the upload token on charmnest/master/)
    const a = await door({ op: 'put', path: `charmnest/master/custom/${sku}.ai`, contentType: 'application/illustrator', base64: ((o && o.ai) || AI).toString('base64'), repoToken: tok });
    const p = await door({ op: 'put', path: `charmnest/master/custom/${sku}.png`, contentType: 'image/png', base64: PNG.toString('base64'), repoToken: tok });
    return [a, p];
  };
  const snap = () => new Map([...store.entries()].map(([k, v]) => [k, canon(v)]));
  const changed = (a, b) => [...new Set([...a.keys(), ...b.keys()])].filter(k => !k.startsWith('CharmRepo_Tokens/') && a.get(k) !== b.get(k)).sort();   // (the test's own password tokens are not the repository)
  const RID = '4196630529', LISTING = '1799994821', OLD = "ACORN'S LEAF", SKU = 'ACORNS_LEAF_4821', HASH = crypto.createHash('sha256').update('test-charm-file').digest('hex');
  const ENTRY = { charmHash: 'a1b2c3d4e5f60718', widthPt: 31.2, heightPt: 38.5, areaPt2: 790.4, members: 4, holes: 1, engravable: true, upAngle: 90, upSource: 'long' };
  // (KEEP: a SKU is a DESIGN now: the commit finds the drawing by its charm hash, so each SKU of this test is its own drawing unless a case says otherwise)
  const entryOf = sku => (sku === SKU ? ENTRY : Object.assign({}, ENTRY, { charmHash: 'drawing-of-' + sku }));
  const body = over => { const b = Object.assign({ op: 'customSaveCommit', rid: RID, listingId: LISTING, sku: SKU, oldSku: OLD, productId: '9100000001', optionPairs: [['513', '1234']], group: { mode: 'some', productIds: ['9100000001', '9100000002'] }, title: "Acorn's Leaf Charm", by: 'Paul',
    design: { name: 'Test_Charm.ai', kind: 'ai', hash: HASH, bytes: 3400 }, reuse: false, entry: null, file: { masterHash: HASH, name: 'Test_Charm.ai' } }, over || {}); if (!b.entry) b.entry = entryOf(b.sku); return b; };

  // the repository as it is: three designs (one blocked), a real master file's record, other listings' aliases (one with a person's word, one with a saved answer)
  const REAL = 'f'.repeat(64);
  store.set('Charm_Master_Index/HEART_1111', { sku: 'HEART_1111', masterHash: REAL, masterName: 'Hearts.ai', charmHash: 'heartHash0000001', widthPt: 20, heightPt: 22, areaPt2: 300, members: 2, holes: 0, aiPath: 'charmnest/master/HEART_1111.ai', thumbPath: 'charmnest/master/HEART_1111.png', engravable: true, upAngle: 0, upSource: 'drawn', open: false, labelSource: 'text', hashSource: 'browser' });
  store.set('Charm_Master_Index/BLOCKED_2222', { sku: 'BLOCKED_2222', masterHash: REAL, charmHash: 'blockedHash00001', widthPt: 20, heightPt: 22, aiPath: 'charmnest/master/BLOCKED_2222.ai', blocked: 'two labels under one charm' });
  store.set('Charm_Master_Index/SHELL_3333', { engravableBy: 'operator' });   // (a shell without a SKU, written by a patch on an unindexed SKU)
  store.set('Charm_Master_Files/' + REAL, { masterHash: REAL, name: 'Hearts.ai', charms: 12, labelled: 12, skus: ['HEART_1111', 'BLOCKED_2222'], indexedBy: 'browser' });
  store.set('Charm_Sku_Aliases/1700000001', { listingId: '1700000001', bySku: { 'WAS OLD': 'HEART_1111' }, listingKind: { kind: 'regular', by: 'Paul', at: 1 }, title: 'Hearts', by: 'Ana' });
  const jar = [...store.keys()];
  const start = snap();

  // (a) the door: no token, a token for another need, then the upload token
  let r = await post(body());
  eq([r.status, r.body.error, r.body.need], [401, 'repo-locked', 'upload'], 'a commit without the upload token is refused by the door');
  r = await post(body({ repoToken: await tokenOf('download') }));
  eq([r.status, r.body.error, r.body.why], [401, 'repo-locked', 'scope'], 'a download token does not open a save');
  r = await door({ op: 'put', path: `charmnest/master/custom/${SKU}.ai`, contentType: 'application/illustrator', base64: AI.toString('base64') });
  eq([r.status, r.body.error], [401, 'repo-locked'], 'the file door asks for the token on charmnest/master/custom/ too');
  eq(changed(start, snap()), [], 'nothing was written by the refusals'); ok(fileWrites.length === 0, 'and no file');
  const TOK = await tokenOf('upload'); const afterToken = snap();

  // (b) files missing: not-uploaded, which one, nothing written
  r = await post(body({ repoToken: TOK }));
  eq([r.status, r.body.ok, r.body.error, r.body.which], [200, false, 'not-uploaded', 'ai'], 'no files in Storage: not-uploaded (ai), HTTP 200 for an expected refusal');
  const [aiOnly] = await door({ op: 'put', path: `charmnest/master/custom/${SKU}.ai`, contentType: 'application/illustrator', base64: AI.toString('base64'), repoToken: TOK }).then(x => [x]);
  ok(aiOnly.status === 200, 'the .ai went up with the token');
  r = await post(body({ repoToken: TOK }));
  eq([r.body.error, r.body.which], ['not-uploaded', 'thumb'], 'the picture is missing: not-uploaded (thumb)');
  eq(changed(afterToken, snap()), [], 'no record was written');
  // (c) a file that is not a real .ai
  await up(SKU, TOK, { ai: Buffer.from('<html>' + 'x'.repeat(400)) });
  r = await post(body({ repoToken: TOK }));
  eq([r.body.ok, r.body.error, r.body.why], [false, 'bad-file', 'not-pdf'], 'a file that does not start with %PDF is refused');
  await up(SKU, TOK, { ai: Buffer.from('%PDF-1.4 tiny') });
  r = await post(body({ repoToken: TOK }));
  eq([r.body.error, r.body.why], ['bad-file', 'too-small'], 'a file of 200 bytes or less is refused');
  eq(changed(afterToken, snap()), [], 'still no record');
  await up(SKU, TOK);   // the real files
  const net0 = writes.length, fw0 = fileWrites.length;

  // (d) the save
  const before = snap();
  r = await post(body({ repoToken: TOK }));
  eq([r.status, r.body.ok, r.body.created, r.body.sku, r.body.alias], [200, true, true, SKU, 'bySku'], 'the commit answers ok: ' + JSON.stringify(r.body).slice(0, 200));
  const e = r.body.entry;
  ok(e && e.sku === SKU && e.blocked === null && e.conflict === null && e.sizeMoved === null && !e.pair, 'the entry has no blocked, conflict, sizeMoved or pair');
  ok(/firebasestorage\.googleapis\.com\/v0\/b\/test-bucket\/o\/charmnest%2Fmaster%2Fcustom%2FACORNS_LEAF_4821\.ai/.test(e.aiUrl) && /ACORNS_LEAF_4821\.png/.test(e.thumbUrl), 'the links are minted by the server from the paths');
  eq(changed(before, snap()), ['Charm_Custom_Saves/' + RID, 'Charm_Master_Files/' + HASH, 'Charm_Master_Index/' + SKU, 'Charm_Sku_Aliases/' + LISTING].sort(), 'exactly four documents changed: the index entry, the file record, the alias, the save record');
  const wrote = writes.slice(net0).map(w => w.kind + ' ' + w.path).sort();
  eq(wrote, ['create Charm_Custom_Saves/' + RID, 'create Charm_Master_Index/' + SKU, 'set Charm_Master_Files/' + HASH, 'set Charm_Sku_Aliases/' + LISTING].sort(), 'the index entry and the save record were CREATED (create-only), nothing else');
  ok(!writes.slice(net0).some(w => /Charm_Nest_Rev/.test(w.path)), 'no placement counter was raised');
  const idx = store.get('Charm_Master_Index/' + SKU), f = store.get('Charm_Master_Files/' + HASH), al = store.get('Charm_Sku_Aliases/' + LISTING), sv = store.get('Charm_Custom_Saves/' + RID);
  eq([idx.masterHash, idx.masterPath, idx.masterName, idx.charmHash, idx.aiPath, idx.thumbPath, idx.open, idx.labelSource, idx.engravable, idx.engravableBy, idx.upAngle, idx.upSource, idx.hashSource, idx.confidence],
    [HASH, `charmnest/master/custom/${SKU}.ai`, 'Test_Charm.ai', ENTRY.charmHash, `charmnest/master/custom/${SKU}.ai`, `charmnest/master/custom/${SKU}.png`, false, 'text', true, 'index', 90, 'long', 'custom-save', null], 'the index entry has the indexer\'s own fields');
  ok(idx.indexedAt && idx.indexedAtMs > 0 && idx.firstIndexedAt && !('blocked' in idx) && !('conflict' in idx) && !('sizes' in idx) && !('pair' in idx), 'stamped, and none of blocked, conflict, sizes, pair');
  eq([f.masterHash, f.path, f.name, f.charms, f.labelled, f.skus, f.indexedBy, f.unlabelledCount, f.orphanCount, f.duplicateCount, f.blockedCount], [HASH, idx.aiPath, 'Test_Charm.ai', 1, 1, [SKU], 'custom-save', 0, 0, 0, 0], 'the file record');
  eq([al.bySku, al.keep.rid, al.keep.sku, al.keep.oldSku, al.keep.was, al.listingId], [{ [OLD]: SKU }, RID, SKU, OLD, '', LISTING], 'the alias answers the old SKU with the new one and keeps who/what');
  ok(!('sku' in al) && !('v' in al), 'a SKU-carrying order sets bySku only');
  eq([sv.rid, sv.listingId, sv.sku, sv.oldSku, sv.productId, sv.phase, sv.reused, sv.rehearsal, sv.etsy, sv.group, sv.masterHash, sv.aiPath, sv.by, sv.alias], [RID, LISTING, SKU, OLD, '9100000001', 'repo', false, false, { state: 'pending' }, { mode: 'some', productIds: ['9100000001', '9100000002'] }, HASH, idx.aiPath, 'Paul', 'bySku'], 'the save record');
  eq(sv.optionPairs, [{ propertyId: '513', valueId: '1234' }], 'option pairs are kept as a list of maps (Firestore refuses an array inside an array)');
  eq(Object.keys(sv).sort(), ['alias', 'aiPath', 'at', 'by', 'charmHash', 'design', 'etsy', 'group', 'listingId', 'masterHash', 'oldSku', 'optionPairs', 'phase', 'productId', 'rehearsal', 'reused', 'rid', 'sku', 'thumbPath', 'title'].sort(), 'no buyer data in the save record: its keys are all of it');
  ok(al.listingKind === undefined, 'a fresh alias has no listing word of ours');
  ok(fileWrites.length === fw0, 'the commit itself wrote no file (the browser uploads them)');
  // reads back like any other entry
  r = await post({ op: 'masterGet', sku: SKU });
  ok(r.status === 200 && r.body.entry && r.body.entry.sku === SKU && r.body.entry.blocked === null && /token=tok/.test(r.body.entry.aiUrl) && /token=tok/.test(r.body.entry.thumbUrl), 'masterGet reads it back: blocked empty, links filled');
  r = await post({ op: 'masterList', limit: 100 });
  ok(r.body.entries.some(x => x.sku === SKU && !x.blocked), 'and masterList lists it');
  r = await post({ op: 'customSaveGet', rid: RID });
  eq([r.status, r.body.ok, r.body.save.sku, r.body.save.phase, r.body.save.reused, r.body.save.etsy, r.body.save.by, r.body.save.listingId], [200, true, SKU, 'repo', false, { state: 'pending' }, 'Paul', LISTING], 'customSaveGet returns the record');
  r = await post({ op: 'customSaveGet', rid: '4199999999' }); eq([r.body.ok, r.body.save], [true, null], 'a missing record is null');
  r = await post({ op: 'customSaveGet', rid: 'abc' }); ok(r.status === 400, 'a bad order number is a 400');
  r = await post({ op: 'customSaveGet', rid: RID, sandbox: true }); eq([r.status, r.body.save], [200, null], 'the sandbox is never "already saved"');

  // (e) the same SKU again: the same order is idempotent; another order, or another SKU for this order, is refused; the index doc never changes
  const settled = snap(), wlen = writes.length;
  r = await post(body({ repoToken: TOK }));
  eq([r.status, r.body.ok, r.body.created, r.body.sku], [200, true, false, SKU], 'the same order and SKU again: the saved record, created false');
  ok(r.body.entry && r.body.entry.sku === SKU, 'with its entry');
  eq(writes.length, wlen, 'and nothing was written');
  r = await post(body({ repoToken: TOK, sku: 'OTHER_4821' }));
  eq([r.status, r.body.ok, r.body.error, r.body.sku, r.body.sameFile], [200, false, 'already-saved', SKU, false], 'another SKU AND another drawing for a saved order: already-saved, it names the saved SKU (the first design of an order stays)');
  r = await post(body({ repoToken: TOK, sku: 'OTHER_4821', entry: ENTRY }));
  eq([r.status, r.body.ok, r.body.created, r.body.how, r.body.sku], [200, true, false, 'again', SKU], 'another SKU for the SAME drawing of a saved order: the saved record again (KEEP B2), the saved SKU');
  r = await post(body({ repoToken: TOK, rid: '4196630999', entry: Object.assign({}, ENTRY, { charmHash: 'another-drawing' }) }));
  eq([r.status, r.body.ok, r.body.error, r.body.by], [200, false, 'sku-taken', 'master'], 'another order, the same SKU, another drawing: sku-taken');
  eq(canon(store.get('Charm_Master_Index/' + SKU)), settled.get('Charm_Master_Index/' + SKU), 'the index entry is byte-identical');
  eq(changed(settled, snap()), [], 'the whole database is as it was');
  eq(writes.length, wlen, 'no write was even attempted');
  for (const k of jar) eq(canon(store.get(k)), start.get(k), 'master and alias document untouched: ' + k);
  { // KEEP B2: another ORDER of the same listing, the same drawing (a second tab, a stale library): the listing is already linked, only that order's record is written
    const w0 = writes.length;
    r = await post(body({ repoToken: TOK, rid: '4196630999' }));
    eq([r.status, r.body.ok, r.body.created, r.body.how, r.body.sku], [200, true, false, 'listing-same', SKU], 'another order of the listing with the same drawing: already linked, no second entry');
    eq(writes.slice(w0).map(w => w.kind + ' ' + w.path), ['create Charm_Custom_Saves/4196630999'], 'only the order\'s record was written');
    store.delete('Charm_Custom_Saves/4196630999'); settled.delete('Charm_Custom_Saves/4196630999');
  }
  // two at once for the same SKU: exactly one wins
  await up('RACE_1000', TOK);
  const race = await Promise.all([post(body({ repoToken: TOK, rid: '4196630101', sku: 'RACE_1000', listingId: '1799991000', oldSku: 'X RACE', entry: Object.assign({}, ENTRY, { charmHash: 'race-one' }), file: { masterHash: crypto.createHash('sha256').update('r1').digest('hex'), name: 'Race.ai' } })),
    post(body({ repoToken: TOK, rid: '4196630102', sku: 'RACE_1000', listingId: '1799991001', oldSku: 'X RACE 2', entry: Object.assign({}, ENTRY, { charmHash: 'race-two' }), file: { masterHash: crypto.createHash('sha256').update('r2').digest('hex'), name: 'Race.ai' } }))]);
  eq(race.map(x => x.body.ok).sort(), [false, true], 'two commits for one SKU at the same moment: one saved, one refused');
  eq(race.filter(x => !x.body.ok)[0].body.error, 'sku-taken', 'the other is sku-taken');
  const loser = race[0].body.ok ? '4196630102' : '4196630101';
  ok(!store.has('Charm_Custom_Saves/' + loser), 'and the loser left no save record');

  // (f) the alias and the next purchase (no new code reads it: the page's own rules)
  const aliases = (await post({ op: 'aliasGet' })).body.aliases, masterOf = async () => { const m = new Map((await post({ op: 'masterList', limit: 3000 })).body.entries.map(x => [x.sku, x])); return { entryFor: s => m.get(String(s).toUpperCase()) || null, loose: s => { const k = O.looseKey(s); for (const x of m.keys()) if (O.looseKey(x) === k) return x; return ''; } }; };
  let M = await masterOf();
  const order = { receiptId: '4200000001', updateTs: 1, staffNote: '', buyerMessage: '' };
  const line = (sku, listingId, title) => ({ transactionId: '42000000011', listingId, sku, title: title || "Acorn's Leaf Charm", quantity: 1, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], variations: [] });
  const ctx = (a, m) => ({ optionMaps: {}, aliases: a, noDesign: {}, masterEntry: m.entryFor, masterLoose: m.loose });
  let rs = O.resolveSku(line(OLD, LISTING), aliases, M.entryFor, {}, M.loose);
  eq([rs.sku, rs.source], [SKU, 'alias'], 'O.resolveSku: the old (non-reserved) SKU of the listing gives the new SKU through the alias');
  let sp = O.interpretLine(order, line(OLD, LISTING), ctx(aliases, M));
  ok(sp.designSku === SKU && sp.skuSource === 'alias' && !sp.special && !sp.problems.some(p => /sku/i.test(p.kind)), 'O.interpretLine: an order still carrying the old SKU is the saved design: ' + JSON.stringify([sp.designSku, sp.skuSource, sp.special && sp.special.kind, sp.problems.map(p => p.kind)]));
  sp = O.interpretLine(order, line(SKU, LISTING), ctx(aliases, M));
  ok(sp.designSku === SKU && sp.skuSource === 'transaction' && !sp.special && !sp.problems.some(p => /sku/i.test(p.kind)), 'O.interpretLine: a later order carrying the NEW SKU (Etsy updated) is a master design, source transaction, not special');
  sp = O.interpretLine(order, line(SKU, LISTING), ctx({}, M));
  ok(sp.designSku === SKU && sp.skuSource === 'transaction' && !sp.special, '... with no alias at all');

  // a SKU-less order: the alias is the listing's own (sku + v:2) and the line with no SKU resolves to it
  await up('LEAF_5555', TOK);
  r = await post(body({ repoToken: TOK, rid: '4196630202', listingId: '1799995555', sku: 'LEAF_5555', oldSku: '', file: { masterHash: crypto.createHash('sha256').update('f5').digest('hex'), name: 'Leaf.ai' } }));
  eq([r.body.ok, r.body.alias], [true, 'whole'], 'an order with no SKU: the alias is the listing\'s own');
  const a5 = store.get('Charm_Sku_Aliases/1799995555'); eq([a5.sku, a5.v, 'bySku' in a5], ['LEAF_5555', 2, false], 'sku + v:2');
  M = await masterOf(); const al2 = (await post({ op: 'aliasGet' })).body.aliases;
  rs = O.resolveSku(line('', '1799995555'), al2, M.entryFor, {}, M.loose); eq([rs.sku, rs.source], ['LEAF_5555', 'alias'], 'a line with no SKU resolves to it');

  // a SKU that keeps the line a custom order whatever an alias says (CUSTOM_6673): the alias does not link it; only the new SKU on Etsy does
  await up('LEAF_6673', TOK);
  r = await post(body({ repoToken: TOK, rid: '4196630303', listingId: '1799996673', sku: 'LEAF_6673', oldSku: 'CUSTOM_6673', file: { masterHash: crypto.createHash('sha256').update('f6').digest('hex'), name: 'Leaf.ai' } }));
  eq([r.body.ok, r.body.alias], [true, 'bySku'], 'the save works for a reserved custom SKU too (the alias is written)');
  M = await masterOf(); const al3 = (await post({ op: 'aliasGet' })).body.aliases;
  sp = O.interpretLine(order, line('CUSTOM_6673', '1799996673', 'Custom Charm'), ctx(al3, M));
  ok(!!sp.special && /^custom/.test(sp.special.kind), 'the old CUSTOM_6673 stays a custom order even with the alias: ' + JSON.stringify([sp.designSku, sp.special && sp.special.kind]));
  ok(!(sp.designSku === 'LEAF_6673' && !sp.special), 'so the alias alone does not link it');
  sp = O.interpretLine(order, line('LEAF_6673', '1799996673', 'Custom Charm'), ctx(al3, M));
  ok(sp.designSku === 'LEAF_6673' && sp.skuSource === 'transaction' && !sp.special, 'once Etsy carries LEAF_6673 the next order is the master design');

  // the alias merge keeps a person's other answers and the listing word; what stood under the same key is kept in keep.was
  // (KEEP K2: a person's live link is never replaced; an answer whose design is gone is dangling and is replaced, kept in keep.was)
  store.set('Charm_Sku_Aliases/1799997777', { listingId: '1799997777', bySku: { 'OLD SEVEN': 'GONE_0000', 'OTHER ONE': 'HEART_1111' }, listingKind: { kind: 'regular', by: 'Paul', at: 1 }, pairBySku: { P: { L: 'A', R: 'B' } }, title: 'Seven' });
  await up('SEVEN_7777', TOK);
  r = await post(body({ repoToken: TOK, rid: '4196630404', listingId: '1799997777', sku: 'SEVEN_7777', oldSku: 'OLD SEVEN', file: { masterHash: crypto.createHash('sha256').update('f7').digest('hex'), name: 'Seven.ai' } }));
  const a7 = store.get('Charm_Sku_Aliases/1799997777');
  eq([r.body.ok, a7.bySku, a7.listingKind.kind, a7.pairBySku.P.L, a7.keep.was], [true, { 'OLD SEVEN': 'SEVEN_7777', 'OTHER ONE': 'HEART_1111' }, 'regular', 'A', 'GONE_0000'], 'the alias merge keeps the other answers, the listing word and the pair; the dangling answer it replaced is in keep.was');
  await up('SEVEN_7778', TOK); const before7 = canon(store.get('Charm_Sku_Aliases/1799997777')), w7 = writes.length;
  r = await post(body({ repoToken: TOK, rid: '4196630405', listingId: '1799997777', sku: 'SEVEN_7778', oldSku: 'OTHER ONE', file: { masterHash: crypto.createHash('sha256').update('f78').digest('hex'), name: 'Seven2.ai' } }));
  eq([r.status, r.body.ok, r.body.error, r.body.sku], [200, false, 'listing-linked', 'HEART_1111'], 'a live link of the listing for this SKU (a person\'s) stays: listing-linked, the first link wins, nothing is written');
  eq([canon(store.get('Charm_Sku_Aliases/1799997777')), writes.length, store.has('Charm_Master_Index/SEVEN_7778'), store.has('Charm_Custom_Saves/4196630405')], [before7, w7, false, false], '... not the alias, no entry, no record');

  // (g) reuse: an identical design already in the repository (exact charm hash): only the alias and the save record are written
  const b4 = snap(), w4 = writes.length;
  r = await post(body({ repoToken: TOK, rid: '4196630505', listingId: '1799995050', sku: 'HEART_1111', oldSku: 'HEART OLD', reuse: true, entry: { charmHash: 'heartHash0000001' } }));
  eq([r.body.ok, r.body.created, r.body.sku, r.body.entry.sku], [true, false, 'HEART_1111', 'HEART_1111'], 'reuse: ok, nothing created');
  eq(writes.slice(w4).map(w => w.kind + ' ' + w.path).sort(), ['create Charm_Custom_Saves/4196630505', 'set Charm_Sku_Aliases/1799995050'], 'reuse writes only the alias and the save record');
  eq(canon(store.get('Charm_Master_Index/HEART_1111')), b4.get('Charm_Master_Index/HEART_1111'), 'the design\'s entry is untouched');
  const sv5 = store.get('Charm_Custom_Saves/4196630505'); eq([sv5.reused, sv5.masterHash, sv5.aiPath], [true, REAL, 'charmnest/master/HEART_1111.ai'], 'the save record says reused and points at the existing file');
  r = await post(body({ repoToken: TOK, rid: '4196630506', sku: 'HEART_1111', reuse: true, entry: { charmHash: 'someOtherDrawing' } })); eq(r.body.error, 'reuse-mismatch', 'a different drawing is not reused');
  r = await post(body({ repoToken: TOK, rid: '4196630507', sku: 'NOWHERE_4401', reuse: true, entry: { charmHash: 'x' } })); eq(r.body.error, 'reuse-missing', 'a SKU that is not there cannot be reused');
  r = await post(body({ repoToken: TOK, rid: '4196630508', sku: 'BLOCKED_2222', reuse: true, entry: { charmHash: 'blockedHash00001' } })); eq(r.body.error, 'reuse-blocked', 'a blocked design is not reused');
  r = await post(body({ repoToken: TOK, rid: '4196630509', sku: 'SHELL_3333', reuse: true, entry: { charmHash: 'x' } })); eq(r.body.error, 'reuse-missing', 'a shell without a SKU is no design');
  // a SKU over a shell document (no sku field) is refused too: create() never overwrites
  await up('SHELL_3333', TOK);
  r = await post(body({ repoToken: TOK, rid: '4196630510', sku: 'SHELL_3333' })); eq([r.body.ok, r.body.error], [false, 'sku-taken'], 'a document that is already there (even a shell) is never overwritten');
  eq(canon(store.get('Charm_Master_Index/SHELL_3333')), start.get('Charm_Master_Index/SHELL_3333'), 'the shell is byte-identical');

  // (h) the file record: a real master file with the same bytes is left exactly as it is; one of ours grows its list
  await up('TWIN_4401', TOK);
  r = await post(body({ repoToken: TOK, rid: '4196630601', listingId: '1799990001', sku: 'TWIN_4401', oldSku: 'T ONE', file: { masterHash: REAL, name: 'Hearts.ai' } }));
  eq([r.body.ok, canon(store.get('Charm_Master_Files/' + REAL))], [true, start.get('Charm_Master_Files/' + REAL)], 'a real master file\'s record is never touched');
  await up('TWIN_4402', TOK);
  r = await post(body({ repoToken: TOK, rid: '4196630602', listingId: '1799990002', sku: 'TWIN_4402', oldSku: 'T TWO', file: { masterHash: HASH, name: 'Test_Charm.ai' } }));
  eq([r.body.ok, store.get('Charm_Master_Files/' + HASH).skus], [true, [SKU, 'TWIN_4402']], 'a record this save made grows its list of SKUs');

  // (i) what is refused before anything is read or written
  const w6 = writes.length, f6 = fileWrites.length;
  r = await post(body({ repoToken: TOK, sandbox: true })); eq([r.status, r.body.ok, r.body.error], [200, false, 'sandbox'], 'sandbox:true is refused');
  r = await post(body({ repoToken: TOK, sandbox: '1', rid: '4196630701' })); eq(r.body.error, 'sandbox', '... in any spelling');
  eq([writes.length, fileWrites.length], [w6, f6], 'with an empty write log');
  for (const sku of ['CUSTOM_LEAF', 'RE_5460', 'LEAF-12', 'X-CO', 'FOX (HUGGIE)']) { r = await post(body({ repoToken: TOK, rid: '4196630702', sku })); ok(r.status === 400 || r.body.error === 'reserved', 'a reserved or unusable SKU is refused: ' + sku + ' ' + JSON.stringify(r.body).slice(0, 80)); }
  r = await post(body({ repoToken: TOK, rid: '4196630702', sku: 'CUSTOM_LEAF' })); eq([r.body.ok, r.body.error], [false, 'reserved'], 'CUSTOM_LEAF is reserved (HTTP 200)');
  for (const bad of [{ rid: 'x' }, { listingId: '12' }, { sku: 'has space' }, { sku: 'a/b' }, { sku: 'A'.repeat(40) }, { sku: '' }, { entry: { charmHash: 'h', widthPt: 0, heightPt: 5, areaPt2: 1, members: 1, holes: 0 } }, { file: { masterHash: 'nothex', name: 'a.ai' }, design: { name: 'a.ai', hash: 'nothex' } }]) {
    r = await post(body(Object.assign({ repoToken: TOK, rid: '4196630703' }, bad))); eq([r.status, r.body.error], [400, 'bad-request'], 'a malformed call is a 400: ' + JSON.stringify(bad).slice(0, 60));
  }
  eq([writes.length, fileWrites.length], [w6, f6], 'and none of them wrote');
  ok(netCalls === 0, 'nothing here reached for the network (no Etsy call)');
  // the ops are in the library's table, and the door names the commit
  ok(Object.keys(lib.ops).includes('customSaveGet') && Object.keys(lib.ops).includes('customSaveCommit'), 'both ops are in the library');
  eq([RA.requirement('charmNestLibrary', 'customSaveCommit', {}), RA.requirement('charmNestLibrary', 'customSaveGet', {})], [{ op: 'customSaveCommit', need: 'upload' }, null], 'the door asks the upload token for the commit and nothing for the read');
  ok(Save.pathsOf('A_1').aiPath === 'charmnest/master/custom/A_1.ai', 'the paths are derived from the SKU alone');
  console.warn = warn; console.error = err;
}

/* ═══ 2 · the engine ═══ */
async function engine() {
  const Keep = require(path.join(root, 'charm-nest-keep.js')), O = require(path.join(root, 'charm-nest-orders.js')), Sku = require(path.join(root, 'charm-nest-sku.js'));
  const RID = '4196630529', LISTING = '1799994821', GOOD = 'good-' + crypto.randomBytes(4).toString('hex');
  function world(opt = {}) {
    const w = { calls: [], bodies: [], events: [], uploads: [], builds: 0, saves: [], dialogs: 0, password: [] };
    const entries = new Map(opt.entries || []);
    const trap = () => { w.dialogs++; throw new Error('the engine opened a dialog'); };
    const lock = { held: opt.token !== false, attempts: 0,
      find: a => (lock.held && a === 'upload' ? { action: 'upload', token: 'T-' + w.password.length, expiresAt: Date.now() + 1e6 } : null), forget() { lock.held = false; },
      ensure: trap, call: trap, guard: trap, gate: trap,
      submitInline: async (action, pw) => { lock.attempts++; w.password.push(pw === GOOD); if (pw === GOOD) { lock.held = true; return { ok: true, token: 'x' }; } return opt.lockedOut ? { ok: false, reason: 'locked', retryAfterSec: 90 } : { ok: false, reason: 'wrong' }; } };
    const script = Object.assign({}, opt.api || {});
    const api = async (fn, b) => {
      w.calls.push(b.op); w.bodies.push(b);
      const s = script[b.op]; const v = typeof s === 'function' ? await s(b, w) : s;
      if (v instanceof Error) throw v;
      if (v !== undefined) return v;
      if (b.op === 'customSaveGet') return { ok: true, save: null };
      if (b.op === 'customSkuCheck') return { ok: true, free: b.candidates[0], taken: [] };
      if (b.op === 'customEtsySkuWrite') return { ok: true, state: 'linked', calls: 2, productIds: ['1'], sku: 'X' };
      throw new Error('unexpected op ' + b.op);
    };
    const Master = { entryFor: s => entries.get(String(s).toUpperCase()) || null, looseFor: s => { const k = O.looseKey(s); for (const x of entries.keys()) if (O.looseKey(x) === k) return x; return ''; }, sameDesign: () => opt.same || '',
      saveCustom: async (spec, hooks) => {
        w.saves.push({ sku: spec.sku, reuse: spec.reuse, rehearsal: spec.rehearsal, built: !!spec.built, uploaded: Object.keys(spec.uploaded || {}).sort().join() });
        const s = opt.save && opt.save(w.saves.length, spec, hooks, w); if (s) { const r = await s; if (r) return r; }
        if (!spec.built && !spec.reuse) { w.builds++; hooks.built({ ai: 1, png: 2 }); }
        if (spec.rehearsal) return { ok: true, rehearsed: true };
        let tok = lock.find('upload'); if (!tok) tok = await hooks.needPassword(); if (!tok) return { ok: false, reason: 'locked', text: 'needs the repository password' };
        if (!spec.reuse) for (const k of ['ai', 'png']) if (!(spec.uploaded || {})[k]) { w.uploads.push(k); hooks.uploaded(k, { path: k }); }
        return { ok: true, sku: spec.sku, created: !spec.reuse, entry: { sku: spec.sku }, alias: 'bySku' };
      } };
    const ctx = Object.assign({ rid: RID, listingId: LISTING, title: "Acorn&#39;s Leaf Charm", line: { sku: 'ACORN&#39;S LEAF', productId: '', variations: [{ name: 'Metal', value: 'GF 14/20', propertyId: 513, valueId: 1234 }], listingId: LISTING },
      listingKind: null, F: { name: 'Acorns_Leaf.ai', kind: 'ai', bytes: new Uint8Array(500), hash: 'ab'.repeat(32), pieces: 1, state: 'ready' }, charm: { hash: 'c1c2c3c4c5c6c7c8', open: false }, parsed: { ok: 1 }, by: 'Paul', sandbox: !!opt.sandbox,
      S: { cloud: { ok: opt.cloud !== false } }, O, Sku, Master, aliases: opt.aliases || {}, tableFor: () => opt.table || null, api, lock, now: Date.now, designs: opt.designs }, opt.ctx || {});
    w.lock = lock; w.entries = entries; w.timers = [];
    ctx.setTimeout = (fn, ms) => { const t = { fn, ms, off: false }; w.timers.push(t); return t; }; ctx.clearTimeout = t => { if (t) t.off = true; };
    w.k = Keep.make(ctx); w.k.on(ev => w.events.push(ev));
    w.rows = () => Object.fromEntries(['sku', 'repo', 'etsy'].map(r => [r, (w.k.snapshot().rows[r] || {}).state + '|' + (w.k.snapshot().rows[r] || {}).text]));
    return w;
  }
  const seq = w => w.events.filter(e => e.row).map(e => e.row + ':' + e.state);

  // open: nothing written, one read, the preview
  let w = world(); let info = await w.k.open();
  eq([info.offered, info.sku, info.group, info.aliasWorks, info.saved, info.locked, info.rehearsal, info.reuse], [true, 'ACORNS_LEAF_4821', 'unknown', true, null, false, false, null], 'open(): offered, the preview SKU, nothing saved, unlocked');
  eq(w.calls, ['customSaveGet'], 'open() makes one read and writes nothing');
  eq(w.saves, [], 'nothing is built or saved before a press');
  ok(info.why === '', 'no reason when offered');

  // the happy path: the order of the steps, the words, the call counts
  let res = await w.k.run();
  eq(res, { ok: true, done: 'linked' }, 'run() resolves linked');
  eq(seq(w), ['sku:work', 'sku:done', 'repo:work', 'repo:done', 'etsy:work', 'etsy:done'], 'the steps run in order: SKU, repository, Etsy');
  eq(w.rows(), { sku: 'done|ACORNS_LEAF_4821', repo: 'done|Saved to the repository', etsy: 'done|Linked on Etsy' }, 'the rows read as the contract says');
  const fin = w.events.filter(e => e.done).pop(); eq([fin.done, fin.sku, fin.text, fin.detail], ['linked', 'ACORNS_LEAF_4821', 'Linked · ACORNS_LEAF_4821', 'Saved to the repository and set on Etsy listing …4821'], 'the final line');
  eq(w.calls, ['customSaveGet', 'customSkuCheck', 'customEtsySkuWrite'], 'calls: one read, one SKU check, ONE Etsy write op');
  eq(w.bodies[2], { op: 'customEtsySkuWrite', rid: RID }, 'the Etsy op is sent the order number alone');
  eq(w.bodies[1].candidates.slice(0, 2), ['ACORNS_LEAF_4821', 'ACORNS_LEAF_94821'], 'the SKU check is sent the candidates');
  const spec = w.saves[0]; eq([w.saves.length, spec.sku, spec.reuse, spec.rehearsal, w.uploads, w.builds, w.dialogs], [1, 'ACORNS_LEAF_4821', false, false, ['ai', 'png'], 1, 0], 'built once, two uploads, one save, no dialog');
  // what saveCustom is handed
  let seen = null; w = world({ save: (i, s) => { seen = s; } }); await w.k.open(); await w.k.run();
  eq([seen.rid, seen.listingId, seen.sku, seen.oldSku, seen.title, seen.by, seen.optionPairs, seen.group, seen.F.name], [RID, LISTING, 'ACORNS_LEAF_4821', "ACORN'S LEAF", "Acorn's Leaf Charm", 'Paul', [['513', '1234']], { mode: 'unknown', productIds: [] }, 'Acorns_Leaf.ai'], 'saveCustom is handed the decoded SKU and title, the option ids and the group');

  // not offered, each reason
  const why = async (o, text) => { const x = world(o), i = await x.k.open(); ok(!i.offered && i.why === text, `not offered: ${text} (got ${i.offered} ${i.why})`); const r = await x.k.run(); ok(r.ok === false, 'a run on a strip that is not offered does nothing'); eq(x.saves, [], 'and saves nothing'); eq(x.calls.filter(c => c !== 'customSaveGet'), [], 'and asks for nothing'); };
  await why({ designs: 2 }, 'Saving works for one design at a time.');
  await why({ ctx: { F: { name: 'a.ai', kind: 'ai', bytes: new Uint8Array(9), hash: 'ab'.repeat(32), pieces: 2, state: 'ready' } } }, 'Saving works for one design at a time.');
  await why({ listingKind: null, ctx: { listingKind: { kind: 'custom', by: 'Paul' } } }, "This listing takes the buyer's own design every time, so it is not linked to one charm.");
  await why({ ctx: { title: 'Add a Lion Charm' } }, "This listing takes the buyer's own design every time, so it is not linked to one charm.");
  await why({ entries: [['HEART_1111', { sku: 'HEART_1111' }]], ctx: { line: { sku: 'HEART_1111', variations: [], listingId: LISTING } } }, 'This listing is already linked to HEART_1111.');
  await why({ entries: [['HEART_1111', { sku: 'HEART_1111' }]], ctx: { line: { sku: 'Heart 1111', variations: [], listingId: LISTING } } }, 'This listing is already linked to HEART_1111.');
  await why({ entries: [['HEART_1111', { sku: 'HEART_1111' }]], aliases: { [LISTING]: { bySku: { "ACORN'S LEAF": 'HEART_1111' } } } }, 'This listing is already linked to HEART_1111.');
  await why({ cloud: false }, 'Saving needs a connection.');
  await why({ ctx: { listingId: '', line: { sku: 'X', variations: [] } } }, 'This order has no Etsy listing to link.');
  await why({ ctx: { F: { name: 'a.ai', bytes: new Uint8Array(9), hash: 'ab', pieces: 1, state: 'reading' } } }, 'Saving works once the design has been read.');
  // a SKU that matches a reserved custom rule is NOT a master design: still offered (even if a master held it)
  w = world({ entries: [['CUSTOM_6673', { sku: 'CUSTOM_6673' }]], ctx: { line: { sku: 'CUSTOM_6673', variations: [], listingId: LISTING }, title: 'Custom Charm' } }); info = await w.k.open();
  eq([info.offered, info.aliasWorks], [true, false], 'CUSTOM_6673 is offered, and the strip is told an alias will not link it (aliasWorks false)');
  res = await w.k.run(); eq(res, { ok: true, done: 'linked' }, 'and it saves');
  // an empty SKU, and a SKU that names nothing, are offered
  w = world({ ctx: { line: { sku: '', variations: [], listingId: LISTING } } }); info = await w.k.open(); ok(info.offered, 'a line with no SKU is offered');

  // locked: Send to Sheet unaffected (the engine asks for nothing, opens no dialog), password, then continue
  w = world({ token: false }); info = await w.k.open();
  eq([info.offered, info.locked, info.sku], [true, true, 'ACORNS_LEAF_4821'], 'locked: still offered');
  res = await w.k.run();
  eq(res, { ok: false, done: 'held' }, 'locked: held');
  eq(w.rows(), { sku: 'idle|Chosen: ACORNS_LEAF_4821', repo: 'locked|Not saved: needs the repository password', etsy: 'idle|Waits for the repository' }, 'locked: the three rows read as the contract says');
  eq([w.calls, w.saves, w.dialogs], [['customSaveGet'], [], 0], 'locked: no SKU check, nothing saved, no Etsy, no dialog');
  eq(w.events.filter(e => e.done).pop().text, 'Not saved: needs the repository password', 'locked: the final line');
  let u = await w.k.unlock('nope'); eq(u, { ok: false, reason: 'wrong' }, 'a wrong password says wrong');
  const lw = world({ token: false, lockedOut: true }); u = await lw.k.unlock('nope'); eq(u, { ok: false, reason: 'locked', retryAfterSec: 90 }, 'a lock-out says locked, with the wait');
  eq(w.calls, ['customSaveGet'], 'an unlock writes nothing and asks the cloud for nothing');
  u = await w.k.unlock(GOOD); eq(u, { ok: true }, 'the right password unlocks');
  ok(JSON.stringify(w.k.snapshot()).indexOf(GOOD) < 0 && JSON.stringify(w.events).indexOf(GOOD) < 0, 'the password is kept nowhere in the engine');
  eq(w.saves.length, 0, 'unlocking alone saves nothing (a press does)');
  res = await w.k.run(); eq(res, { ok: true, done: 'linked' }, 'after the password, a press continues and completes');
  eq(w.calls, ['customSaveGet', 'customSkuCheck', 'customEtsySkuWrite'], 'after the password: SKU check, Etsy');
  eq([w.saves.length, w.uploads, w.dialogs], [1, ['ai', 'png'], 0], 'one save, one pair of uploads, no dialog');
  const nolock = world({ ctx: { lock: null } }); u = await nolock.k.unlock('x'); eq(u, { ok: false, reason: 'unavailable' }, 'without the lock module an unlock is unavailable');

  // token expiry mid-run: the uploads are made, the commit is refused, the person unlocks, the retry makes no second upload or build
  w = world({ save: (i, s, hooks, ww) => { if (i === 1) { for (const k of ['ai', 'png']) { ww.uploads.push(k); hooks.uploaded(k, { path: k }); } ww.builds++; hooks.built({ ai: 1, png: 2 }); ww.lock.held = false; return hooks.needPassword().then(t => (t ? null : { ok: false, reason: 'locked', text: 'needs the repository password' })); } } });
  await w.k.open(); res = await w.k.run();
  eq(res, { ok: false, done: 'held' }, 'token expiry mid-run: held');
  eq(w.rows(), { sku: 'done|ACORNS_LEAF_4821', repo: 'locked|Not saved: needs the repository password', etsy: 'idle|Waits for the repository' }, 'the Repository row is locked, the SKU stays chosen, Etsy waits');
  ok(!w.calls.includes('customEtsySkuWrite'), 'Etsy is not called when the repository did not save');
  await w.k.unlock(GOOD); res = await w.k.retry('repo');
  eq(res, { ok: true, done: 'linked' }, 'unlock + Retry completes');
  eq([w.uploads, w.builds, w.saves.map(s => s.built + ':' + s.uploaded)], [['ai', 'png'], 1, ['false:', 'true:ai,png']], 'NO double upload and no second build: the retry is handed what the first try made');
  eq(w.calls.filter(c => c === 'customSkuCheck').length, 1, 'and the SKU was not chosen twice');

  // offline: the failing row says why, the rows after it wait, Retry runs from the failing row
  w = world({ api: { customSkuCheck: Object.assign(new Error('Failed to fetch'), { transient: true }) } }); await w.k.open(); res = await w.k.run();
  eq(res, { ok: false, done: 'held' }, 'offline at the SKU check: held');
  eq(w.rows(), { sku: 'failed|Could not choose a SKU: you are offline', repo: 'idle|', etsy: 'idle|Waits for the repository' }, 'the SKU row says why; the rows after it wait');
  ok(w.events.some(e => e.row === 'sku' && e.state === 'failed' && e.reason === 'offline' && e.retry === true), 'and offers Retry');
  eq(w.saves, [], 'nothing was built or saved');
  w.k.on(() => {}); delete w.k;
  w = world({ api: { customSkuCheck: (b, ww) => (ww.calls.filter(c => c === 'customSkuCheck').length === 1 ? Object.assign(new Error('Failed to fetch'), { transient: true }) : undefined) } }); await w.k.open(); await w.k.run();
  res = await w.k.retry('sku'); eq(res, { ok: true, done: 'linked' }, 'Retry from the failing row completes');
  eq(w.calls.filter(c => c === 'customSkuCheck').length, 2, 'the SKU check was asked twice (once failed)');
  w = world({ save: i => (i === 1 ? { ok: false, reason: 'offline', text: 'you could not reach the cloud' } : null) }); await w.k.open(); res = await w.k.run();
  eq(w.rows(), { sku: 'done|ACORNS_LEAF_4821', repo: 'failed|Not saved: you could not reach the cloud', etsy: 'idle|Waits for the repository' }, 'offline in the repository step');
  ok(!w.calls.includes('customEtsySkuWrite'), 'Etsy is not called');
  res = await w.k.retry('repo'); eq(res, { ok: true, done: 'linked' }, 'Retry from the Repository row');
  eq(w.calls.filter(c => c === 'customSkuCheck').length, 1, 'the SKU check is not run again');
  // a geometry refusal: nothing saved, Etsy not called, the reason shown
  w = world({ save: () => ({ ok: false, reason: 'geometry', text: 'the design cannot be kept as a charm (open outline)' }) }); await w.k.open(); res = await w.k.run();
  eq([res.done, w.rows().repo, w.calls.includes('customEtsySkuWrite')], ['held', 'failed|Not saved: the design cannot be kept as a charm (open outline)', false], 'a design that cannot be kept: the reason, no Etsy call');

  // Etsy fails after the repository: saved, Etsy not updated, Retry runs the Etsy step alone
  const scope = { ok: false, state: 'failed', reason: 'scope', text: 'Etsy refused the change. Paul needs to re-authorise Etsy with listings_w', retry: true, calls: 1, needs: 'listings_w' };
  w = world({ api: { customEtsySkuWrite: (b, ww) => (ww.calls.filter(c => c === 'customEtsySkuWrite').length === 1 ? scope : undefined) }, ctx: { line: { sku: 'CUSTOM_6673', variations: [], listingId: LISTING }, title: 'Custom Charm' } }); await w.k.open(); res = await w.k.run();
  eq(res, { ok: false, done: 'saved' }, 'Etsy refused: saved');
  eq(w.rows(), { sku: 'done|ACORNS_LEAF_4821', repo: 'done|Saved to the repository', etsy: 'failed|Etsy not updated: ' + scope.text }, 'the Etsy row says why');
  ok(w.events.some(e => e.row === 'etsy' && e.state === 'failed' && e.needs === 'listings_w' && e.retry === true), 'with Retry and what is needed');
  const sf = w.events.filter(e => e.done).pop(); eq([sf.done, sf.text, sf.note], ['saved', 'Saved to the repository · Etsy not updated', 'Until Etsy has the new SKU, the next order of this listing will still ask.'], 'the final line, and the dim note when an alias will not link the SKU');
  res = await w.k.retry('etsy'); eq(res, { ok: true, done: 'linked' }, 'Retry on the Etsy row completes');
  eq([w.saves.length, w.uploads.length, w.calls.filter(c => c === 'customSkuCheck').length, w.calls.filter(c => c === 'customEtsySkuWrite').length], [1, 2, 1, 2], 'Retry ran the Etsy step only: nothing saved or uploaded twice');
  for (const [reason, text] of [['busy', 'Etsy is busy, try again in a minute'], ['listing-state', 'the listing is not active on Etsy']]) {
    w = world({ api: { customEtsySkuWrite: { ok: false, state: 'failed', reason, retry: true } } }); await w.k.open(); await w.k.run();
    eq(w.rows().etsy, 'failed|Etsy not updated: ' + text, 'a reason without text reads in plain words: ' + reason);
  }
  w = world({ api: { customEtsySkuWrite: new Error('unknown op') } }); await w.k.open(); res = await w.k.run(); eq([res.done, w.rows().repo], ['saved', 'done|Saved to the repository'], 'an Etsy call that throws leaves the repository saved');

  // a reload: the record says where it stands; a press runs only what is left; open() itself writes nothing
  w = world({ api: { customSaveGet: { ok: true, save: { rid: RID, sku: 'LEAF_4821', phase: 'repo', reused: false, etsy: { state: 'failed', reason: 'scope', text: 'Etsy refused the change.' }, at: 1, by: 'Paul' } } } }); info = await w.k.open();
  eq([info.offered, info.sku, info.saved.sku, info.saved.etsy.state], [true, 'LEAF_4821', 'LEAF_4821', 'failed'], 'a saved record is shown whatever the line says now');
  eq(w.rows(), { sku: 'done|LEAF_4821', repo: 'done|Saved to the repository', etsy: 'failed|Etsy not updated: Etsy refused the change.' }, 'its rows');
  eq(w.calls, ['customSaveGet'], 'a reload runs nothing');
  res = await w.k.run(); eq([res, w.calls, w.saves.length], [{ ok: true, done: 'linked' }, ['customSaveGet', 'customEtsySkuWrite'], 0], 'a press runs the Etsy step only');
  w = world({ api: { customSaveGet: { ok: true, save: { rid: RID, sku: 'LEAF_4821', phase: 'repo', etsy: { state: 'linked' } } } } }); await w.k.open(); res = await w.k.run();
  eq([res, w.calls], [{ ok: true, done: 'linked' }, ['customSaveGet']], 'a linked record: a press asks Etsy nothing');

  // an identical design already in the repository: its SKU, no SKU check, no new file
  w = world({ same: 'OLDLEAF_1111' }); info = await w.k.open(); eq([info.reuse, info.sku, info.offered], ['OLDLEAF_1111', 'OLDLEAF_1111', true], 'reuse is found by the page');
  res = await w.k.run(); eq(res, { ok: true, done: 'linked' }, 'and completes');
  eq([w.calls, w.saves[0].reuse, w.rows().repo, w.rows().sku], [['customSaveGet', 'customEtsySkuWrite'], 'OLDLEAF_1111', 'done|Already in the repository as OLDLEAF_1111', 'done|OLDLEAF_1111'], 'no SKU check; the Repository row says it was there');
  w = world({ same: 'FOX HEAD' }); info = await w.k.open(); ok(info.reuse === null && info.offered, 'a design whose SKU cannot be put on Etsy is not reused (a new SKU is made)');
  w = world({ same: 'CUSTOM_LEAF' }); info = await w.k.open(); ok(info.reuse === null && info.offered, 'nor a reserved one');

  // a SKU taken between the check and the commit: the next candidate is asked for, once, and nothing is uploaded twice
  w = world({ save: (i, s, hooks) => (i === 1 ? { ok: false, reason: 'sku-taken', text: 'that SKU was just taken' } : null), api: { customSkuCheck: (b, ww) => ({ ok: true, free: b.candidates[0], taken: [] }) } }); await w.k.open(); res = await w.k.run();
  eq(res, { ok: true, done: 'linked' }, 'sku-taken at the commit: the run goes on with the next free SKU');
  eq([w.saves.map(s => s.sku), w.bodies.filter(b => b.op === 'customSkuCheck').map(b => b.candidates[0])], [['ACORNS_LEAF_4821', 'ACORNS_LEAF_94821'], ['ACORNS_LEAF_4821', 'ACORNS_LEAF_94821']], 'the taken SKU is left out of the second check');
  w = world({ api: { customSkuCheck: { ok: true, free: '', taken: [{ sku: 'ACORNS_LEAF_4821', by: 'master' }] } } }); await w.k.open(); res = await w.k.run();
  eq([res.done, w.rows().sku, w.saves.length], ['held', 'failed|No free SKU for this design', 0], 'no free SKU: the SKU row fails, nothing else is tried');

  // the sandbox rehearsal: reads for real, builds, saves nothing, asks Etsy nothing
  w = world({ sandbox: true, token: false }); info = await w.k.open(); eq([info.offered, info.rehearsal], [true, true], 'the sandbox offers a rehearsal, locked or not');
  res = await w.k.run(); eq(res, { ok: true, done: 'rehearsed' }, 'a rehearsal completes');
  eq(w.rows(), { sku: 'done|ACORNS_LEAF_4821', repo: 'rehearsed|Rehearsed: would save ACORNS_LEAF_4821.ai', etsy: 'rehearsed|Rehearsed: would set the SKU on listing …4821' }, 'the rows end rehearsed');
  eq(w.events.filter(e => e.done).pop().text, 'Rehearsal complete. Nothing was saved.', 'the final line');
  eq([w.calls, w.saves.map(s => s.rehearsal), w.uploads, w.bodies.every(b => b.sandbox === true)], [['customSaveGet', 'customSkuCheck'], [true], [], true], 'only the two reads, both sandbox; the build ran; no upload, no Etsy');

  // settled(): resolves when the run ends, or when the time passes; a failure does not hold anything
  let release; w = world({ save: () => new Promise(r => { release = r; }) }); await w.k.open();
  ok(await w.k.settled(5), 'settled() with nothing running resolves true at once');
  const running = w.k.run(); await new Promise(r => setImmediate(r));
  const s1 = w.k.settled(20000); await new Promise(r => setImmediate(r));
  eq(w.timers.filter(t => !t.off).map(t => t.ms), [20000], 'settled(20000) sets one 20 s time-out');
  w.timers.find(t => !t.off).fn(); eq(await s1, false, 'when the time passes first it resolves false (the window closes as it does today)');
  const s2 = w.k.settled(20000); release({ ok: true, sku: 'ACORNS_LEAF_4821', created: true }); eq(await s2, true, 'when the run ends first it resolves true');
  await running; ok(w.timers.every(t => t.off), 'and no time-out is left running');
  w = world({ save: () => ({ ok: false, reason: 'error', text: 'boom' }) }); await w.k.open(); const t0 = w.k.run(); const st = await w.k.settled(20000); ok(st === true, 'a failed run settles at once'); await t0;

  // never throws, whatever is handed
  w = world({ save: () => { throw new Error('boom'); } }); await w.k.open(); res = await w.k.run(); ok(res.ok === false && res.done === 'held', 'a saveCustom that throws: held, not thrown');
  w = world({ api: { customSaveGet: new Error('HTTP 500') } }); info = await w.k.open(); ok(info.offered, 'a failed record read does not stop the strip');
  w = world({ ctx: { Master: {} } }); await w.k.open(); res = await w.k.run(); ok(res.ok === false, 'a missing saveCustom: held, not thrown');
  w = world(); w.k.on(() => { throw new Error('listener'); }); await w.k.open(); res = await w.k.run(); eq(res, { ok: true, done: 'linked' }, 'a listener that throws breaks nothing');
  w = world(); await w.k.open(); const [r1, r2] = [w.k.run(), w.k.run()]; ok(r1 === r2, 'a second run() while one runs is the same run'); await r1; eq(w.saves.length, 1, '... and saves once');
  eq(w.dialogs, 0, 'no engine run opened a dialog');
}

/* ═══ 3 · Master.saveCustom in a real page ═══ */
async function page() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the page checks were not run'); return; }
  const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!require('fs').existsSync(exe)) { console.log('  - no browser at ' + exe + ': the page checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1200, height: 800 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort()));
    const pg = await context.newPage(), errors = []; pg.on('pageerror', e => errors.push(e.message)); pg.setDefaultTimeout(30000);
    await pg.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await pg.waitForFunction(() => window.CN && window.Master && window.CharmNestRepoLock && window.CharmNestDXF && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
    const DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES', 0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, 18, 20, 0, 42, 0.4, 10, 18, 20, 20, 10, 0, 20, 20, 0, 'CIRCLE', 8, 'CUT', 10, 9, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
    // a design read the way the Designs window reads it (CustomSheet.read): the .dxf made into a PDF, traced, silhouetted
    await pg.evaluate(async dxf => {
      const P = CharmNestPDF, raw = new TextEncoder().encode(dxf), r = await CharmNestDXF.toPdf(raw, 'Test_Charm.dxf');
      const parsed = await P.parseSource(r.bytes, 'Test_Charm.dxf'), g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: 6 });
      for (const m of g.orphans) { if (!m.bbox) continue; const c = g.charms.find(c => { const b = c.bbox; return m.bbox[0] >= b[0] - 2 && m.bbox[1] >= b[1] - 2 && m.bbox[2] <= b[2] + 2 && m.bbox[3] <= b[3] + 2; }); if (c && !c.members.includes(m)) c.members.push(m); }
      for (const c of g.charms) P.integrateRings(c);
      await P.buildSilhouettes(parsed, g.charms, 6);
      const hash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', r.bytes))].map(b => b.toString(16).padStart(2, '0')).join('');
      window.__design = { parsed, charm: g.charms.filter(c => !c.open)[0], F: { name: 'Test_Charm.dxf', kind: 'dxf', hash, bytes: r.bytes } };
      window.__spec = over => Object.assign({ rid: '4196630529', listingId: '1799994821', sku: 'ACORNS_LEAF_4821', oldSku: "ACORN'S LEAF", productId: '9100000001', optionPairs: [['513', '1234']], group: { mode: 'all', productIds: ['9100000001'] }, title: "Acorn's Leaf Charm", by: 'Paul', F: window.__design.F, charm: window.__design.charm, parsed: window.__design.parsed, reuse: false, rehearsal: false }, over || {});
      window.__hooks = { built: [], uploaded: [], asked: 0, say: [] };
      window.__h = () => ({ needPassword: async () => { window.__hooks.asked++; return window.__give ? window.__give() : null; }, say: t => window.__hooks.say.push(t), built: b => window.__hooks.built.push(b), uploaded: (k, i) => window.__hooks.uploaded.push([k, i.path]) });
    }, DXF);
    ok(await pg.evaluate(() => !!window.__design.charm && window.__design.charm.hash && !!window.__design.charm.thumb), 'a design traced the way the Designs window traces it');
    const calls0 = srv.st.calls.length, dialogs = () => pg.evaluate(() => document.querySelectorAll('dialog').length);
    eq(await pg.evaluate(() => [typeof Master.saveCustom, typeof Master.sameDesign]), ['function', 'function'], 'Master.saveCustom and Master.sameDesign are exported');

    // 1 · a rehearsal builds the file and touches nothing
    let r = await pg.evaluate(() => Master.saveCustom(window.__spec({ rehearsal: true }), window.__h()));
    ok(r.ok && r.rehearsed && r.bytes > 200, 'a rehearsal builds the .ai: ' + JSON.stringify(r)); eq(srv.st.calls.length, calls0, 'and makes no call at all');
    // 2 · a geometry reason: nothing is saved
    r = await pg.evaluate(() => { const s = window.__spec(); s.charm = Object.assign({}, s.charm, { open: true }); return Master.saveCustom(s, window.__h()); });
    ok(!r.ok && r.reason === 'geometry' && /open outline/.test(r.text), 'an open outline: nothing is saved, the reason is told'); eq(srv.st.calls.length, calls0, 'and no call was made');
    // 3 · a real save with the token the page holds (the fake door answers it as a signed-in master admin's): the files up under charmnest/master/custom/, the records in
    const d0 = await dialogs(), blobsBefore = new Set(srv.st.blobs.keys()), ops = () => srv.st.calls.slice(calls0).map(c => c.name + ':' + c.op);
    r = await pg.evaluate(() => Master.saveCustom(window.__spec(), window.__h()));
    ok(r.ok === true && r.created === true && r.sku === 'ACORNS_LEAF_4821' && r.alias === 'bySku' && r.rehearsed === false, 'a real save: ' + JSON.stringify(r).slice(0, 200));
    const newBlobs = [...srv.st.blobs.keys()].filter(k => !blobsBefore.has(k)).sort();
    eq(newBlobs, ['charmnest/master/custom/ACORNS_LEAF_4821.ai', 'charmnest/master/custom/ACORNS_LEAF_4821.png'], 'exactly the two files went up, under charmnest/master/custom/ and nowhere else');
    const aiB = srv.st.blobs.get(newBlobs[0]).buf, pngB = srv.st.blobs.get(newBlobs[1]).buf;
    ok(aiB.length > 200 && aiB.slice(0, 4).toString() === '%PDF', 'the .ai is a real PDF-based file'); ok(pngB.slice(1, 4).toString() === 'PNG', 'and the picture is a PNG');
    const docs = k => srv.st.docs.get(k);
    const idx = docs('Charm_Master_Index/ACORNS_LEAF_4821'), sv = docs('Charm_Custom_Saves/4196630529'), al = docs('Charm_Sku_Aliases/1799994821');
    ok(idx && idx.sku === 'ACORNS_LEAF_4821' && idx.aiPath === 'charmnest/master/custom/ACORNS_LEAF_4821.ai' && idx.open === false && idx.hashSource === 'custom-save' && idx.widthPt > 0 && idx.heightPt > 0 && idx.members >= 1 && idx.holes >= 1, 'the index entry is there with the geometry the page measured: ' + JSON.stringify(idx).slice(0, 160));
    ok(sv && sv.sku === 'ACORNS_LEAF_4821' && sv.phase === 'repo' && sv.etsy.state === 'pending' && sv.optionPairs[0].propertyId === '513', 'the save record is there'); ok(al && al.bySku["ACORN'S LEAF"] === 'ACORNS_LEAF_4821', 'and the alias');
    ok(srv.st.docs.has('Charm_Master_Files/' + idx.masterHash), 'and the file record');
    ok(!ops().some(o => /masterList/.test(o)), 'no masterList was called (the page keeps the entry it was given; the next check reads the index): ' + ops().join(' '));
    ok(ops().filter(o => /customSaveCommit/.test(o)).length === 1 && ops().filter(o => /charmNestOutput:(sign|put)/.test(o)).length >= 2, 'one commit, two signed uploads');
    ok(await pg.evaluate(() => { const e = Master.entryFor('ACORNS_LEAF_4821'); return !!e && e.sku === 'ACORNS_LEAF_4821' && !e.blocked; }), 'the entry is in the page\'s library at once');
    eq(await dialogs(), d0, 'no dialog opened'); ok(!(await pg.evaluate(() => window.__hooks.asked)), 'and nobody was asked for a password');
    eq(await pg.evaluate(() => window.__hooks.uploaded.map(u => u[0] + ':' + u[1]).sort()), ['ai:charmnest/master/custom/ACORNS_LEAF_4821.ai', 'png:charmnest/master/custom/ACORNS_LEAF_4821.png'], 'the hook heard of each upload');
    // 4 · a retry that brings what the first try made: nothing is built or uploaded twice, the same order answers the saved record
    const c4 = srv.st.calls.length;
    r = await pg.evaluate(() => { const h = window.__hooks; return Master.saveCustom(window.__spec({ built: h.built[0], uploaded: { ai: { path: 'charmnest/master/custom/ACORNS_LEAF_4821.ai' }, png: { path: 'charmnest/master/custom/ACORNS_LEAF_4821.png' } } }), window.__h()); });
    ok(r.ok === true && r.created === false && r.sku === 'ACORNS_LEAF_4821', 'a retry: the saved record, created false: ' + JSON.stringify(r).slice(0, 160));
    eq(srv.st.calls.slice(c4).map(c => c.name + ':' + c.op), ['charmNestLibrary:customSaveCommit'], 'and it made the one commit call, no upload, no sign');
    // 5 · the same SKU for another order is refused, and what is there is not touched
    const before5 = JSON.stringify(docs('Charm_Master_Index/ACORNS_LEAF_4821'));
    r = await pg.evaluate(() => { const s = window.__spec({ rid: '4196630999', listingId: '1799990999' }); s.charm = Object.assign({}, s.charm, { hash: 'another-drawing-0001' }); return Master.saveCustom(s, window.__h()); });
    ok(r.ok === false && r.reason === 'sku-taken', 'another order, the same SKU, ANOTHER drawing: sku-taken (the door refuses the upload over the entry\'s files): ' + JSON.stringify(r).slice(0, 120));
    r = await pg.evaluate(() => Master.saveCustom(window.__spec({ rid: '4196630998', listingId: '1799990999' }), window.__h()));
    ok(r.ok === true && r.created === false && r.sku === 'ACORNS_LEAF_4821', 'another order, the same SKU, the SAME drawing: already in the repository, no second entry: ' + JSON.stringify(r).slice(0, 120));
    srv.st.docs.delete('Charm_Custom_Saves/4196630998'); srv.st.docs.delete('Charm_Sku_Aliases/1799990999');
    eq(JSON.stringify(docs('Charm_Master_Index/ACORNS_LEAF_4821')), before5, 'the entry is as it was'); ok(!srv.st.docs.has('Charm_Custom_Saves/4196630999'), 'and no save record was made for it');
    // 6 · locked: no token held, nobody gives one: no call, no dialog
    await pg.evaluate(() => { CharmNestRepoLock.forget(); window.__give = null; window.__hooks.asked = 0; });
    const c6 = srv.st.calls.length;
    r = await pg.evaluate(() => Master.saveCustom(window.__spec({ rid: '4196630111', listingId: '1799990111', sku: 'LOCKED_0111' }), window.__h()));
    eq([r.ok, r.reason, await dialogs(), srv.st.calls.length, await pg.evaluate(() => window.__hooks.asked)], [false, 'locked', d0, c6, 1], 'locked: it asks the hook once, opens no dialog, calls nothing');
    ok(!srv.st.docs.has('Charm_Master_Index/LOCKED_0111') && ![...srv.st.blobs.keys()].some(k => /LOCKED_0111/.test(k)), 'and nothing was saved');
    // 7 · the door is strict now: a token the server does not know is refused (401 repo-locked), the hook is asked once, a real token goes through
    srv.st.repoStrict = true;
    const RA = require(path.join(root, 'netlify/functions/_charmNestRepoAuth.js'));
    const good = await RA.issue(srv.st.db, { action: 'upload', by: 'Tester' });
    await pg.evaluate(t => { sessionStorage.setItem('cn.repoLock.v1', JSON.stringify({ upload: { t: 'f'.repeat(64) + '.' + '0'.repeat(32), e: Date.now() + 3600e3 } })); window.__hooks.asked = 0; window.__give = () => ({ token: t, action: 'upload', expiresAt: Date.now() + 3600e3 }); }, good.token);
    r = await pg.evaluate(() => Master.saveCustom(window.__spec({ rid: '4196630222', listingId: '1799990222', sku: 'STRICT_0222' }), window.__h()));
    ok(r.ok === true && r.created === true && r.sku === 'STRICT_0222', 'a refused token: the hook is asked and the save goes on: ' + JSON.stringify(r).slice(0, 160));
    eq([await pg.evaluate(() => window.__hooks.asked), await dialogs()], [1, d0], 'asked once, still no dialog');
    ok(srv.st.blobs.has('charmnest/master/custom/STRICT_0222.ai') && srv.st.docs.has('Charm_Master_Index/STRICT_0222') && srv.st.docs.has('Charm_Custom_Saves/4196630222'), 'the files and the records are in');
    ok(srv.st.calls.some(c => c.op === 'sign' && c.body.repoToken === good.token) && srv.st.calls.some(c => c.op === 'customSaveCommit' && c.body.repoToken === good.token), 'the door-issued token went with the upload and the commit');
    // the same, and the hook has nothing to give: locked, and the strict door wrote nothing
    await pg.evaluate(() => { sessionStorage.setItem('cn.repoLock.v1', JSON.stringify({ upload: { t: 'e'.repeat(64) + '.' + '1'.repeat(32), e: Date.now() + 3600e3 } })); window.__give = null; window.__hooks.asked = 0; });
    const w7 = JSON.stringify([...srv.st.docs.keys()].sort());
    r = await pg.evaluate(() => Master.saveCustom(window.__spec({ rid: '4196630333', listingId: '1799990333', sku: 'STRICT_0333' }), window.__h()));
    ok(r.ok === false && r.reason === 'locked', 'a refused token and no password given: locked: ' + JSON.stringify(r).slice(0, 120));
    eq(JSON.stringify([...srv.st.docs.keys()].sort()), w7, 'and no document was written'); ok(![...srv.st.blobs.keys()].some(k => /STRICT_0333/.test(k)), 'and no file went up');
    // 8 · the page stayed quiet
    eq(errors, [], 'no page error');
  } finally { await browser.close(); srv.close(); }
}

(async () => {
  await server(); console.log('  server: ok');
  await engine(); console.log('  engine: ok');
  await page(); console.log('  page: done');
  console.log('save-repo: ' + n + ' checks passed');
})().catch(e => { process.stderr.write(String(e && e.stack || e) + '\n'); process.exit(1); });
