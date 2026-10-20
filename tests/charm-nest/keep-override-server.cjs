// KEEPOVERRIDE + KEEPOWNSKU (Paul, 20 Oct 2026) · the server side of the designs window's "Save to the charm repository and link this listing".
//   Paul: "If a user engages the toggle to update the charm repository, then that's exactly what needs to happen ... If there is an existing charm that is incorrectly matched to a given Etsy
//   listing/order then the system must remove the SKU from the offending charm and attach it to the newly uploaded charm in the Charm repo." and "In the case that a given listing has no match at
//   all, then the system will need to use the existing Etsy listing/order SKU and save/match that along with the newly uploaded charm vector to the Charm repo."
// OFFLINE: the REAL charmNestLibrary handler, the REAL file door (charmNestOutput) and the REAL Etsy step module on the in-memory Firestore and Storage of _sandboxFakes.cjs. No network.
//   1  without override nothing changes: a SKU of another drawing is sku-taken, another design answering the listing is listing-linked (first link wins)
//   2  override via "own": the entry of the line's SKU is replaced by the new design (its files under {SKU}__k{tag}), the old charm keeps its other SKU, ONE audit record holds the old entry,
//      the other save record of that SKU is superseded (the repair pass leaves it), a repeated call answers the same; the same act with the old drawing moves the SKU back
//   3  override via "link": the listing's answer is replaced, the old charm keeps every SKU, keep.was and the audit record hold the old answer
//   4  the order's own Etsy SKU (no match): "Test_@" is saved as TEST_@ (upper case, nothing invented), its files under TEST__k{tag}; a SKU that is not the line's own is refused; "/" is "-";
//      the Etsy step is a no-op (Etsy already carries it)
//   5  the sandbox writes nothing; the orphan sweep keeps a tagged file an entry draws from
//   node tests/charm-nest/keep-override-server.cjs
'use strict';
const path = require('path'), assert = require('assert/strict'), crypto = require('crypto');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

(async () => {
  const Fk = require('./_sandboxFakes.cjs'); Fk.install();
  const { store } = Fk;
  const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
  const origFile = Fk.bucket.file.bind(Fk.bucket); let aged = false;
  Fk.bucket.file = p => { const f = origFile(p), gm = f.getMetadata; f.getMetadata = async () => { const [m] = await gm.call(f); return [Object.assign({}, m, { metadata: { firebaseStorageDownloadTokens: 'tok' } }, aged ? { updated: new Date(Date.now() - 3 * 86400000).toISOString() } : {})]; }; return f; };
  let netCalls = 0; global.fetch = async () => { netCalls++; throw new Error('no network in this test'); };
  const lib = require(path.join(fnDir, 'charmNestLibrary.js')), output = require(path.join(fnDir, 'charmNestOutput.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
  const Save = require(path.join(fnDir, '_charmNestCustomSave.js')), Repair = require(path.join(fnDir, '_charmNestCustomRepair.js')), Etsy = require(path.join(fnDir, '_charmNestEtsySku.js'));
  const Sku = require(path.join(root, 'charm-nest-sku.js')), O = require(path.join(root, 'charm-nest-orders.js'));
  const post = async b => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
  const door = async b => { const r = await output.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
  const TOK = (await RA.issue(Fk.db, { action: 'upload', by: 'Tester' })).token;
  const AI = Buffer.from('%PDF-1.4\n' + '%charm '.repeat(80)), PNG = Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), Buffer.alloc(80, 7)]);
  const up = async stem => { for (const [ext, ct, b] of [['ai', 'application/illustrator', AI], ['png', 'image/png', PNG]]) { const r = await door({ op: 'put', path: `charmnest/master/custom/${stem}.${ext}`, contentType: ct, base64: b.toString('base64'), repoToken: TOK }); assert.equal(r.status, 200, 'upload ' + stem + ': ' + JSON.stringify(r.body)); } };
  const HASH = crypto.createHash('sha256').update('the new design file').digest('hex'), TAG = HASH.slice(0, 12);
  const ENTRY = { charmHash: 'newDrawingHash01', widthPt: 30, heightPt: 30, areaPt2: 700, members: 2, holes: 1, engravable: true, upAngle: 0, upSource: 'drawn' };
  const body = o => Object.assign({ op: 'customSaveCommit', repoToken: TOK, rid: '4196650001', listingId: '1800007733', sku: 'OAK_LEAF', oldSku: 'OAK_LEAF', productId: '', optionPairs: [], group: { mode: 'unknown', productIds: [] }, title: 'Oak Leaf Charm', by: 'Ana',
    design: { name: 'Oak_new.ai', kind: 'ai', hash: HASH, bytes: 3400 }, reuse: false, entry: ENTRY, file: { masterHash: HASH, name: 'Oak_new.ai' }, fileTag: TAG }, o || {});
  // the repository: charm X (one drawing, file test/OAK.ai) under two SKUs, OAK_LEAF and OAK_LEAF_GF; an older save record of OAK_LEAF (another order); a listing linked to X through an alias
  const X = { masterHash: 'e'.repeat(64), masterName: 'Leaves.ai', charmHash: 'oldDrawingHashX', widthPt: 20, heightPt: 22, areaPt2: 300, members: 2, holes: 0, aiPath: 'charmnest/master/OAK.ai', thumbPath: 'charmnest/master/OAK.png', engravable: true, upAngle: 0, upSource: 'drawn', open: false, labelSource: 'text', hashSource: 'browser' };
  store.set('Charm_Master_Index/OAK_LEAF', Object.assign({ sku: 'OAK_LEAF' }, X));
  store.set('Charm_Master_Index/OAK_LEAF_GF', Object.assign({ sku: 'OAK_LEAF_GF' }, X));
  store.set('Charm_Custom_Saves/4196600009', { rid: '4196600009', listingId: '1800000099', sku: 'OAK_LEAF', oldSku: 'OAK_LEAF', charmHash: 'oldDrawingHashX', phase: 'repo', alias: 'none', etsy: { state: 'pending' }, at: 1, by: 'Old' });
  store.set('Charm_Sku_Aliases/1800005555', { listingId: '1800005555', bySku: { "ACORN'S LEAF": 'OAK_LEAF_GF' }, by: 'Paul' });

  /* 1 · without override: first link wins, as before */
  let r = await post(body({ fileTag: undefined }));
  eq([r.body.ok, r.body.error], [false, 'sku-taken'], 'without the switch\'s override a SKU of another drawing is sku-taken: ' + JSON.stringify(r.body));
  await up('ACORN_LEAF_5555');
  r = await post(body({ rid: '4196650002', listingId: '1800005555', sku: 'ACORN_LEAF_5555', oldSku: "ACORN'S LEAF", fileTag: undefined }));
  eq([r.body.ok, r.body.error, r.body.sku], [false, 'listing-linked', 'OAK_LEAF_GF'], 'without override another design answering the listing is listing-linked (first link wins)');
  ok(!store.get('Charm_Master_Index/ACORN_LEAF_5555') && !store.get('Charm_Keep_Overrides/x'), 'nothing written');

  /* 2 · override via "own": the SKU is taken from X and given to the new design */
  r = await post(body({ override: { via: 'own', from: 'OAK_LEAF' }, fileTag: 'bad tag' }));
  eq([r.status, r.body.error, r.body.field], [400, 'bad-request', 'fileTag'], 'an override of the line\'s own SKU needs its files\' tag');
  r = await post(body({ override: { via: 'own', from: 'OAK_LEAF' } }));
  eq([r.body.ok, r.body.error, r.body.which], [false, 'not-uploaded', 'ai'], 'the new design\'s files are looked for under OAK_LEAF__k{tag}, not over X\'s');
  await up('OAK_LEAF__k' + TAG);
  r = await post(body({ override: { via: 'own', from: 'OAK_LEAF' } }));
  ok(r.body.ok === true && r.body.how === 'override' && r.body.sku === 'OAK_LEAF', 'the override commits: ' + JSON.stringify(r.body).slice(0, 300));
  const S1 = store.get('Charm_Master_Index/OAK_LEAF'), XG = store.get('Charm_Master_Index/OAK_LEAF_GF');
  eq([S1.charmHash, S1.aiPath, S1.hashSource, S1.masterHash], [ENTRY.charmHash, 'charmnest/master/custom/OAK_LEAF__k' + TAG + '.ai', 'custom-save', HASH], 'OAK_LEAF is the new design now, from its own files');
  eq([XG.charmHash, XG.aiPath], [X.charmHash, X.aiPath], 'X keeps its other SKU OAK_LEAF_GF and its drawing');
  eq([S1.override.from.skus, S1.override.from.retired, S1.override.by, S1.override.rid], [['OAK_LEAF_GF'], false, 'Ana', '4196650001'], 'the entry says what it was taken from and by whom');
  eq(r.body.override.from.skus, ['OAK_LEAF_GF'], 'the answer names the charm the SKU was removed from (for the pop-up)');
  const audits = [...store.keys()].filter(k => k.startsWith('Charm_Keep_Overrides/'));
  eq(audits.length, 1, 'ONE audit record');
  const A = store.get(audits[0]);
  eq([A.by, A.rid, A.listingId, A.sku, A.via, A.removed.entry.aiPath, A.removed.entry.charmHash, A.removed.retired, A.newCharmHash, A.superseded], ['Ana', '4196650001', '1800007733', 'OAK_LEAF', 'own', X.aiPath, X.charmHash, false, ENTRY.charmHash, ['4196600009']], 'the audit record keeps who, when, which order and the whole old entry');
  ok(store.get('Charm_Custom_Saves/4196600009').superseded && store.get('Charm_Custom_Saves/4196600009').superseded.audit === r.body.override.audit, 'the other save record of OAK_LEAF is superseded');
  eq(Repair.plan(store.get('Charm_Custom_Saves/4196600009'), S1, null, null, Date.now()), { state: null, alias: null, etsy: false }, 'the repair pass leaves a superseded record (no "overtaken", no alias cut)');
  const pend = await Save.pending({ db: Fk.db, now: () => Date.now() + 3600000 }, {});
  ok(!pend.pending.some(p => p.rid === '4196600009'), 'and the Etsy queue does not finish it');
  ok(!Fk.fileWrites.some(w => /charmnest\/master\/(OAK|custom\/OAK_LEAF)\.(ai|png)$/.test(w.path)),'X\'s files are never written: the override uploads beside them');
  const r2 = await post(body({ override: { via: 'own', from: 'OAK_LEAF' } }));
  eq([r2.body.ok, r2.body.how, r2.body.override && r2.body.override.audit], [true, 'again', r.body.override.audit], 'a repeated call answers the same save and the same override');
  eq([...store.keys()].filter(k => k.startsWith('Charm_Keep_Overrides/')).length, 1, 'still one audit record');
  // undo by the same switch: another order of the listing drops X's drawing again: OAK_LEAF joins X's file again (nothing uploaded is kept: X's drawing is in the repository under OAK_LEAF_GF)
  await up('OAK_LEAF__k' + 'aaaaaa111111');
  r = await post(body({ rid: '4196650009', override: { via: 'own', from: 'OAK_LEAF' }, fileTag: 'aaaaaa111111', entry: Object.assign({}, ENTRY, { charmHash: X.charmHash }), design: { name: 'Oak_old.ai', kind: 'ai', hash: 'b'.repeat(64), bytes: 3000 }, file: { masterHash: 'b'.repeat(64), name: 'Oak_old.ai' } }));
  const S2 = store.get('Charm_Master_Index/OAK_LEAF');
  ok(r.body.ok && S2.charmHash === X.charmHash && S2.aiPath === X.aiPath && S2.attachedFrom === 'OAK_LEAF_GF' && S2.hashSource === 'custom-save', 'the same act with the old drawing moves OAK_LEAF back to X\'s file: ' + JSON.stringify(r.body).slice(0, 200));
  ok(!Fk.blobs.has('charmnest/master/custom/OAK_LEAF__kaaaaaa111111.ai'), 'the files uploaded for it are dropped (the entry draws from X\'s)');
  ok(Fk.blobs.has('charmnest/master/custom/OAK_LEAF__k' + TAG + '.ai'), 'the first override\'s files stay (its audit record names them)');
  ok(store.get('Charm_Custom_Saves/4196650001').superseded, 'and the first override\'s record is superseded in turn');

  /* 3 · override via "link": the listing's answer is replaced, X keeps every SKU */
  ok(!Fk.blobs.has('charmnest/master/custom/ACORN_LEAF_5555.ai'), '(the files of the refused save were dropped as litter)');
  await up('ACORN_LEAF_5555');
  r = await post(body({ rid: '4196650002', listingId: '1800005555', sku: 'ACORN_LEAF_5555', oldSku: "ACORN'S LEAF", fileTag: undefined, override: { via: 'link', from: 'OAK_LEAF_GF' } }));
  ok(r.body.ok && r.body.how === 'override' && r.body.sku === 'ACORN_LEAF_5555', 'the link override commits: ' + JSON.stringify(r.body).slice(0, 200));
  const AL = store.get('Charm_Sku_Aliases/1800005555');
  eq([AL.bySku["ACORN'S LEAF"], AL.keep.was, AL.keep.override.via], ['ACORN_LEAF_5555', 'OAK_LEAF_GF', 'link'], 'the listing\'s answer is the new design, the old one kept in keep.was');
  ok(store.get('Charm_Master_Index/OAK_LEAF_GF').charmHash === X.charmHash, 'X keeps OAK_LEAF_GF');
  eq(store.get('Charm_Keep_Overrides/' + r.body.override.audit).alias, { listingId: '1800005555', key: "ACORN'S LEAF", was: 'OAK_LEAF_GF', now: 'ACORN_LEAF_5555' }, 'the audit record holds the old answer');
  eq(O.resolveSku({ listingId: '1800005555', sku: "ACORN'S LEAF" }, { '1800005555': AL }, s => store.get('Charm_Master_Index/' + s) || null).sku, 'ACORN_LEAF_5555', 'the listing\'s next order reads the new design');

  /* 4 · the order's own Etsy SKU */
  eq([Sku.etsyKey('Test_@'), Sku.etsyKey('Ab/Cd 1'), Sku.fileKey('TEST_@', TAG), Sku.ownWhy('TEST_@'), !!Sku.ownWhy('CUSTOM_6673')], ['TEST_@', 'AB-CD 1', 'TEST__k' + TAG, '', true], 'etsyKey: upper case, "/" as "-"; fileKey for the files; a CUSTOM_ SKU is no design\'s');
  const own = o => body(Object.assign({ rid: '4196650003', listingId: '1800008811', sku: 'TEST_@', oldSku: 'Test_@', etsyOwn: true, entry: Object.assign({}, ENTRY, { charmHash: 'testAtDrawing01' }) }, o || {}));
  r = await post(own({ sku: 'TEST_X@', oldSku: 'Test_@' }));
  eq([r.status, r.body.field], [400, 'sku'], 'only the line\'s OWN SKU may be saved with its own characters');
  await up('TEST__k' + TAG);
  r = await post(own());
  ok(r.body.ok && r.body.sku === 'TEST_@' && r.body.created === true && r.body.alias === 'none', 'TEST_@ is saved under the order\'s own SKU: ' + JSON.stringify(r.body).slice(0, 200));
  const T1 = store.get('Charm_Master_Index/TEST_@');
  eq([T1.sku, T1.aiPath, T1.charmHash], ['TEST_@', 'charmnest/master/custom/TEST__k' + TAG + '.ai', 'testAtDrawing01'], 'the library entry TEST_@ is the new design');
  eq(O.resolveSku({ listingId: '1800008811', sku: 'Test_@' }, {}, s => store.get('Charm_Master_Index/' + s) || null), { sku: 'TEST_@', source: 'transaction' }, 'a later order of the listing carrying Test_@ reads TEST_@ directly');
  ok(!(await post({ op: 'masterGet', sku: 'TEST_@' })).body.error, 'the Library reads it back by its own SKU');
  const n0 = netCalls, e1 = await Etsy.ops.customEtsySkuWrite({ rid: '4196650003', listingId: '1800008811' });
  ok(e1.ok === true && (e1.state === 'linked' || e1.linked) && netCalls === n0, 'the Etsy step is a no-op: Etsy already carries TEST_@: ' + JSON.stringify(e1).slice(0, 160));
  await up(Sku.fileKey('AB-CD 1', TAG));
  r = await post(own({ rid: '4196650004', listingId: '1800008812', sku: 'AB-CD 1', oldSku: 'ab/cd 1', entry: Object.assign({}, ENTRY, { charmHash: 'slashDrawing01' }) }));
  ok(r.body.ok && r.body.sku === 'AB-CD 1' && store.get('Charm_Sku_Aliases/1800008812').bySku['AB/CD 1'] === 'AB-CD 1', '"/" is "-" in the key, and the SKU Etsy carries is linked to it: ' + JSON.stringify(r.body).slice(0, 160));

  /* 5 · the sandbox, the sweep */
  const before = JSON.stringify([...store.entries()].filter(([k]) => !k.startsWith('CharmRepo_Tokens/')));
  r = await post(Object.assign(own({ rid: '4196650005' }), { sandbox: true, override: { via: 'own', from: 'TEST_@' } }));
  ok(r.body.ok === false && JSON.stringify([...store.entries()].filter(([k]) => !k.startsWith('CharmRepo_Tokens/'))) === before, 'the sandbox writes nothing (refused before any read)');
  await up('LOST__k' + 'cccccc222222'); aged = true;   // (a tagged upload whose commit never came, and every file three days old)
  const sw = await Repair.orphans({ db: Fk.db, bucket: Fk.bucket }, {});
  ok(Fk.blobs.has('charmnest/master/custom/TEST__k' + TAG + '.ai') && Fk.blobs.has('charmnest/master/custom/OAK_LEAF__k' + TAG + '.ai'), 'the sweep keeps the tagged files an entry or an audit record names: ' + JSON.stringify(sw));
  ok(!Fk.blobs.has('charmnest/master/custom/LOST__kcccccc222222.ai'), 'and removes a tagged upload nothing draws from');
  console.warn = warn; console.error = err;
  console.log(`keep-override-server: ${n} checks passed`);
  process.exit(0);
})().catch(e => { process.stderr.write(String(e && e.stack || e) + "\n"); process.exit(1); });
