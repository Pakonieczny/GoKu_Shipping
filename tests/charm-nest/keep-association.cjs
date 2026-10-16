// KEEP P5 (16 Oct 2026, KB): after "drop a vector file on a Review order + SAVE ON", the NEXT order of that listing is associated AUTOMATICALLY and CORRECTLY.
// The proof builds the repository entry exactly as the save writes it (the REAL handler charmNestLibrary customSaveCommit behind the REAL password door, on the in-memory Firestore and
// Storage of _sandboxFakes.cjs), then reads FRESH orders of the same listing (and of another variation of it) through the REAL orders code (charm-nest-orders.js interpretLine) with the
// library and the aliases exactly as the page would load them from the REAL masterList / aliasGet ops, and asserts it picks the saved design with no person involved.
//   part A (Node)      every listing-SKU case: Etsy's SKU in any case / spacing / entity spelling, another variation sharing the SKU, a variation with its own SKU (NOT linked: by design), a line with no
//                      SKU, a reserved CUSTOM_ SKU (only Etsy's new SKU links it), a SKU with a slash, a SKU that already is the design's name, the same drawing on a second order / another listing,
//                      a SKU taken by another design, an order read before the save and after it, the entry compared with what the master indexer writes for the same drawing, the picture present
//   part B (Chromium)  the dropped file's GEOMETRY: the real page reads a vector file, Master.saveCustom builds the stored .ai; the stored file read back by the master pipeline gives the SAME charm hash,
//                      size and up angle as the dropped file (skipped without playwright-core + Chromium: PW_DIR=<dir with playwright-core>, CHROMIUM=<chrome>)
//   node tests/charm-nest/keep-association.cjs
'use strict';
const path = require('path'), assert = require('assert/strict'), crypto = require('crypto');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const sha = b => crypto.createHash('sha256').update(b).digest('hex');

/* ═══ part A ═══ */
async function partA() {
  const Fk = require('./_sandboxFakes.cjs'); Fk.install();
  const { store, blobs } = Fk;
  const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
  const origFile = Fk.bucket.file.bind(Fk.bucket);
  Fk.bucket.file = p => { const f = origFile(p), gm = f.getMetadata; f.getMetadata = async () => { const [m] = await gm.call(f); return [Object.assign({}, m, { updated: new Date().toISOString(), metadata: { firebaseStorageDownloadTokens: 'tok' + String(p.length) } })]; }; return f; };
  let netCalls = 0; global.fetch = async () => { netCalls++; throw new Error('no network in this test'); };
  const lib = require(path.join(fnDir, 'charmNestLibrary.js')), output = require(path.join(fnDir, 'charmNestOutput.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
  const Master = require(path.join(fnDir, '_charmNestMaster.js')), EtsySku = require(path.join(fnDir, '_charmNestEtsySku.js'));
  const O = require(path.join(root, 'charm-nest-orders.js')), Sku = require(path.join(root, 'charm-nest-sku.js'));
  const post = async b => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
  const door = async b => { const r = await output.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
  const TOK = (await RA.issue(Fk.db, { action: 'upload', by: 'Tester' })).token;

  /* a dropped design is a number: its built .ai, its picture, its charm hash, the geometry the page measured (up angle, size, holes), the dropped file's hash */
  const AIOF = d => Buffer.from('%PDF-1.4\n%built single charm ' + d + '\n' + '0 0 m 10 10 l S\n'.repeat(30));
  const PNGOF = d => Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), Buffer.from('picture ' + d), Buffer.alloc(60, d)]);
  const GEO = d => ({ charmHash: 'hash-of-drawing-' + d, widthPt: 20 + d, heightPt: 31.5 + d, areaPt2: 500 + d, members: 3, holes: 1 + (d % 2), engravable: true, upAngle: (d * 37) % 360, upSource: d % 2 ? 'long' : 'drawn' });
  const FILE = d => sha(Buffer.from('the dropped file ' + d));
  const body = o => ({ op: 'customSaveCommit', repoToken: TOK, rid: o.rid, listingId: o.listing, sku: o.sku, oldSku: o.old == null ? '' : o.old, productId: o.product || '', optionPairs: o.pairs || [], group: { mode: 'all', productIds: [] }, title: o.title || 'Charm', by: 'Paul',
    design: { name: 'Dropped_' + o.d + '.ai', kind: 'ai', hash: FILE(o.d), bytes: 4000 }, reuse: false, entry: GEO(o.d), file: { masterHash: FILE(o.d), name: 'Dropped_' + o.d + '.ai', aiSha256: sha(AIOF(o.d)), aiBytes: AIOF(o.d).length, pngSha256: sha(PNGOF(o.d)) } });
  const save = async o => {
    for (const [ext, buf, type] of [['ai', AIOF(o.d), 'application/illustrator'], ['png', PNGOF(o.d), 'image/png']]) {
      const r = await door({ op: 'put', path: `charmnest/master/custom/${o.sku}.${ext}`, contentType: type, base64: buf.toString('base64'), repoToken: TOK, charmHash: GEO(o.d).charmHash }); if (r.status === 409) return { ok: false, error: r.body.error, by: r.body.by, gate: true }; assert(r.status === 200, 'upload ' + o.sku + ' ' + r.status);
    }
    return (await post(body(o))).body;
  };

  /* the world the page loads: the library (masterList), the aliases (aliasGet); fresh on every call, as after the page's next check */
  const world = async () => {
    const entries = (await post({ op: 'masterList', limit: 3000 })).body.entries, m = new Map(entries.map(e => [e.sku, e])), aliases = (await post({ op: 'aliasGet' })).body.aliases;
    const loose = s => { const k = O.looseKey(s); for (const x of m.keys()) if (O.looseKey(x) === k) return x; return ''; };
    return { m, aliases, ctx: { optionMaps: {}, aliases, noDesign: {}, masterEntry: s => m.get(String(s).toUpperCase()) || null, masterLoose: loose, listingSkus: {} } };
  };
  const order = rid => ({ receiptId: rid, updateTs: 1, staffNote: '', buyerMessage: '' });
  let seq = 4300000000;
  const line = (sku, listingId, extra) => Object.assign({ transactionId: String(++seq), listingId, sku, title: 'Custom Charm Gold Pendant', quantity: 1, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], variations: [] }, extra || {});
  const read = (W, l) => O.interpretLine(order(String(++seq)), l, W.ctx);
  /** the next order IS the saved design, with nobody asked: the SKU, how it was found, no special/no-design/own flags, no question about the SKU */
  const picks = (sp, sku, source, what) => {
    ok(sp.designSku === sku, what + ': designSku ' + sp.designSku + ' (wanted ' + sku + ', source ' + sp.skuSource + ')');
    if (source) eq(sp.skuSource, source, what + ': found by ' + source);
    ok(!sp.special && !sp.noDesign && !sp.ownDesign, what + ': a normal line (no special, no-design or own-design)');
    ok(!sp.problems.some(p => /sku|design|match/i.test(p.kind)), what + ': no question about the design: ' + JSON.stringify(sp.problems.map(p => p.kind)));
  };
  const NOT = (sp, sku, what) => ok(sp.designSku !== sku, what + ': NOT linked by the save alone (designSku ' + sp.designSku + ', ' + sp.skuSource + ')');

  /* ── 1 · the usual case: the line carries Etsy's SKU; the save links it; every variation sharing the SKU follows ── */
  const L1 = '1900004821', SKU1 = 'ACORNS_LEAF_4821', OLD1 = "ACORN'S LEAF";
  let r = await save({ rid: '4200000001', listing: L1, old: OLD1, sku: SKU1, d: 1, product: '9101', pairs: [['513', '1234']], title: "Acorn's Leaf Charm" });
  eq([r.ok, r.created, r.sku, r.how], [true, true, SKU1, 'created'], 'the save');
  let W = await world();
  const e1 = W.m.get(SKU1);
  /* the entry as the next order's readers see it: THE DROPPED FILE'S geometry, orientation, picture */
  eq([e1.charmHash, e1.widthPt, e1.heightPt, e1.areaPt2, e1.members, e1.holes], [GEO(1).charmHash, GEO(1).widthPt, GEO(1).heightPt, GEO(1).areaPt2, GEO(1).members, GEO(1).holes], 'the entry carries the dropped design\'s charm hash and measures');
  eq([e1.upAngle, e1.upSource, e1.engravable, e1.open, e1.blocked, e1.conflict, e1.sizeMoved], [GEO(1).upAngle, GEO(1).upSource, true, false, null, null, null], 'right orientation: the up angle and its source as measured; open false; not blocked');
  eq([e1.aiPath, e1.thumbPath], [`charmnest/master/custom/${SKU1}.ai`, `charmnest/master/custom/${SKU1}.png`], 'the entry names the stored files');
  ok(blobs.get(e1.aiPath).buf.equals(AIOF(1)) && blobs.get(e1.thumbPath).buf.equals(PNGOF(1)), 'and the stored file IS the built design (bytes equal to what the page built from the dropped file)');
  ok(/firebasestorage\.googleapis\.com\/v0\/b\/test-bucket\/o\/charmnest%2Fmaster%2Fcustom%2FACORNS_LEAF_4821\.png\?alt=media&token=/.test(e1.thumbUrl) && /ACORNS_LEAF_4821\.ai\?alt=media&token=/.test(e1.aiUrl), 'the picture and the file have working links (a picture is present)');
  eq(W.aliases[L1].bySku, { [OLD1]: SKU1 }, 'the alias the page loads');
  const c1 = { receiptId: '4200000001' };
  for (const [what, sku] of [['Etsy\'s SKU as saved', OLD1], ['lower case', "acorn's leaf"], ['an HTML-escaped apostrophe (how Etsy serves it)', 'Acorn&#39;s Leaf'], ['padded with spaces', "  ACORN'S LEAF "]])
    picks(read(W, line(sku, L1, { variations: [{ name: 'Color', value: 'Gold', propertyId: 513, valueId: 1234 }] })), SKU1, 'alias', 'a fresh order, ' + what);
  picks(read(W, line(OLD1, L1, { productId: '9102', variations: [{ name: 'Color', value: 'Silver', propertyId: 513, valueId: 1235 }] })), SKU1, 'alias', 'ANOTHER VARIATION of the listing that shares the SKU');
  picks(read(W, line(OLD1, L1, { quantity: 3, productId: '9101' })), SKU1, 'alias', 'a later order of three');
  NOT(read(W, line('ACORN LEAF SILVER', L1, { productId: '9103', variations: [{ name: 'Color', value: 'Silver', propertyId: 513, valueId: 1236 }] })), SKU1, 'a variation with ITS OWN SKU (its own answer, by design: the Etsy group is the products that share the bought product\'s SKU)');
  NOT(read(W, line(OLD1, '1900009999')), SKU1, 'the same SKU text on ANOTHER listing');
  picks(read(W, line(SKU1, L1)), SKU1, 'transaction', 'an order after Etsy carries the new SKU (the next order once the Etsy step is done)');
  picks(read({ ctx: Object.assign({}, W.ctx, { aliases: {} }), m: W.m }, line(SKU1, L1)), SKU1, 'transaction', '... even with no alias at all');
  const before = await world();   // an order read BEFORE the second save keeps its reading after it
  /* the Etsy step for this save (a fake Etsy that says the listing is the old one): linked, and the next order may carry the new SKU */
  const esc = s => String(s).replace(/&/g, '&amp;').replace(/'/g, '&#39;');
  const inv = { stored: { products: ['9101', '9102'].map((id, i) => ({ product_id: +id, sku: OLD1, is_deleted: false, property_values: [{ property_id: 513, property_name: 'Color', scale_id: null, value_ids: [1234 + i], values: [i ? 'Silver' : 'Gold'] }], offerings: [{ offering_id: 1 + i, price: { amount: 2500, divisor: 100, currency_code: 'USD' }, quantity: 5, is_enabled: true, is_deleted: false, readiness_state_id: 7001 }] })), price_on_property: [], quantity_on_property: [], sku_on_property: [513] }, puts: 0 };
  const serve = () => JSON.parse(JSON.stringify(Object.assign({}, inv.stored, { products: inv.stored.products.map(p => Object.assign({}, p, { sku: esc(p.sku) })) })));
  const E = EtsySku.make({ db: Fk.db, FV: Fk.admin.firestore.FieldValue, bucket: Fk.bucket, meter: { bumpSimple() {} }, now: Date.now,
    etsy: { get: async p => (/inventory$/.test(p) ? serve() : { state: 'active' }), put: async (p, q, b) => { inv.puts++; inv.stored = { products: b.products.map((x, i) => Object.assign({ product_id: 9101 + i }, x, { is_deleted: false, offerings: x.offerings.map(o => ({ offering_id: i + 1, price: { amount: Math.round(o.price * 100), divisor: 100, currency_code: 'USD' }, quantity: o.quantity, is_enabled: o.is_enabled, is_deleted: false, readiness_state_id: o.readiness_state_id })) })), price_on_property: b.price_on_property, quantity_on_property: b.quantity_on_property, sku_on_property: b.sku_on_property }; return serve(); } } });
  const ee = await E.customEtsySkuWrite({ rid: '4200000001' });
  eq([ee.ok, ee.state, inv.stored.products.map(p => p.sku), inv.puts], [true, 'linked', [SKU1, SKU1], 1], 'the Etsy step (after the saved repository step): both variations that shared the SKU now carry the new one, one PUT');
  W = await world();
  picks(read(W, line(SKU1, L1, { productId: '9102' })), SKU1, 'transaction', 'an order of the OTHER variation after the Etsy step');

  /* ── 2 · the same drawing again: a second order of the listing (stale library), another listing ── */
  r = await save({ rid: '4200000002', listing: L1, old: OLD1, sku: 'ACORNS_LEAF_64821', d: 1 });
  eq([r.ok, r.created, r.how, r.sku], [true, false, 'listing-same', SKU1], 'a second order of the listing, same drawing, a page that did not know: already linked to ' + SKU1);
  r = await save({ rid: '4200000003', listing: '1900001111', old: 'OTHER LISTING SKU', sku: 'ACORNS_LEAF_1111', d: 1 });
  eq([r.ok, r.created, r.how, r.sku], [true, false, 'same-design', SKU1], 'the same drawing on another listing: the design in the repository is used');
  W = await world();
  eq([...W.m.values()].filter(e => e.charmHash === GEO(1).charmHash).map(e => e.sku), [SKU1], 'ONE entry for the drawing');
  picks(read(W, line('OTHER LISTING SKU', '1900001111')), SKU1, 'alias', 'the other listing\'s next order is the same saved design');
  picks(read(W, line("ACORN'S LEAF", L1)), SKU1, 'alias', 'the first listing, still');
  // the reading made before the second save is the same as after
  eq(read(before, line(OLD1, L1)).designSku, read(W, line(OLD1, L1)).designSku, 'an order read before the second save reads the same after it');

  /* ── 3 · a line with NO SKU: the listing's own answer, every variation without a SKU follows ── */
  const L3 = '1900003333', SKU3 = 'PEARL_DROP_3333';
  r = await save({ rid: '4200000030', listing: L3, old: '', sku: SKU3, d: 3, title: 'Pearl Drop Charm' });
  eq([r.ok, r.alias], [true, 'whole'], 'no SKU on the line: the listing\'s own answer');
  W = await world();
  eq([W.aliases[L3].sku, W.aliases[L3].v], [SKU3, 2], 'sku + v:2');
  picks(read(W, line('', L3, { title: 'Pearl Drop Charm' })), SKU3, 'alias', 'a fresh order with no SKU');
  picks(read(W, line('', L3, { title: 'Pearl Drop Charm', variations: [{ name: 'Size', value: 'Large', propertyId: 100, valueId: 7 }] })), SKU3, 'alias', 'another variation, also with no SKU');
  picks(read(W, line(SKU3, L3)), SKU3, 'transaction', 'an order after Etsy carries the new SKU');

  /* ── 4 · a reserved SKU (CUSTOM_6673): the page reads the raw SKU first, so the alias alone does NOT link; Etsy's new SKU does ── */
  const L4 = '1900006673', SKU4 = 'LEAF_6673';
  r = await save({ rid: '4200000040', listing: L4, old: 'CUSTOM_6673', sku: SKU4, d: 4, title: 'Custom Charm' });
  eq([r.ok, r.alias], [true, 'bySku'], 'saved calmly (the alias is written)');
  W = await world();
  const sp4 = read(W, line('CUSTOM_6673', L4, { title: 'Custom Charm' }));
  ok(!!sp4.special && !(sp4.designSku === SKU4 && !sp4.special), 'the order still carrying CUSTOM_6673 stays a custom order: only the Etsy step links it');
  const pend = (await post({ op: 'customSavePending', limit: 100 })).body.pending.find(x => x.rid === '4200000040');
  ok(pend && pend.linkNeedsEtsy === true, 'the server knows: customSavePending lists it with linkNeedsEtsy (the repair goes first for it)');
  picks(read(W, line(SKU4, L4, { title: 'Custom Charm' })), SKU4, 'transaction', 'after the Etsy step the next order carries the new SKU and is the design');

  /* ── 5 · a SKU with a slash is an ordinary Etsy SKU: keyed like any other, the next order is the design ── */
  const L5 = '1900005555', SKU5 = 'RING_5555';
  r = await save({ rid: '4200000050', listing: L5, old: 'RING/SIZE 7', sku: SKU5, d: 5 });
  eq([r.ok, r.alias, r.created], [true, 'bySku', true], 'a SKU with a slash is saved calmly and keyed');
  W = await world();
  eq(W.aliases[L5].bySku, { 'RING/SIZE 7': SKU5 }, 'the alias the page loads');
  picks(read(W, line('RING/SIZE 7', L5)), SKU5, 'alias', 'the next order carrying the slash SKU');
  picks(read(W, line(SKU5, L5)), SKU5, 'transaction', 'the new SKU once Etsy carries it');
  // a SKU too long to be a key: saved, not keyed, Etsy alone links it
  r = await save({ rid: '4200000055', listing: '1900005556', old: 'Z'.repeat(130), sku: 'LONG_5556', d: 55 });
  eq([r.ok, r.alias], [true, 'none'], 'a SKU longer than a key is saved calmly');
  W = await world(); ok(!W.aliases['1900005556'] || !W.aliases['1900005556'].bySku, 'no alias key');
  NOT(read(W, line('Z'.repeat(130), '1900005556')), 'LONG_5556', 'the long old SKU alone');
  picks(read(W, line('LONG_5556', '1900005556')), 'LONG_5556', 'transaction', 'the new SKU once Etsy carries it');

  /* ── 6 · the line's SKU is already the design's own name (the shop's shape): no alias needed, nothing for Etsy ── */
  r = await save({ rid: '4200000060', listing: '1900009491', old: 'HEART_9491', sku: 'HEART_9491', d: 6 });
  eq([r.ok, r.alias], [true, 'none'], 'saved under the very SKU the line carries');
  W = await world();
  picks(read(W, line('HEART_9491', '1900009491')), 'HEART_9491', 'transaction', 'the next order is the design by its own SKU');
  const e6 = await E.customEtsySkuWrite({ rid: '4200000060' }); eq([e6.ok, e6.state, e6.noop, inv.puts], [true, 'linked', true, 1], 'and the Etsy step has nothing to do');

  /* ── 7 · a SKU taken by ANOTHER design: the next candidate, deterministic, never a dialog ── */
  {
    const L = '1900104821';   // the same last four digits as ACORNS_LEAF_4821's listing
    const file = { name: 'Leaf.ai' };
    const plan = (hasMaster) => Sku.plan({ file, title: 'Leaf Charm', listingId: L, line: { sku: 'LEAFY OLD', listingId: L }, table: null, hasMaster, hasLoose: () => '', aliases: {} });
    // the page's library is STALE (it does not hold LEAF_4821 yet): its plan proposes the SKU another save has just taken
    r = await save({ rid: '4200000070', listing: '1900204821', old: 'FIRST LEAF', sku: 'LEAF_4821', d: 7, title: 'Leaf Charm' });
    ok(r.ok, 'the first leaf');
    const stale = plan(() => false), fresh = plan(s => store.has('Charm_Master_Index/' + s));
    eq(stale.candidates[0], 'LEAF_4821', 'a stale page proposes LEAF_4821'); ok(fresh.candidates[0] !== 'LEAF_4821', 'a fresh page does not');
    const chk = (await post({ op: 'customSkuCheck', listingId: L, candidates: stale.candidates })).body;
    ok(chk.ok && chk.free && chk.free !== 'LEAF_4821', 'the cloud\'s check moves on: ' + chk.free + ' (taken: ' + chk.taken.map(t => t.sku + ' by ' + t.by).join(', ') + ')');
    r = await save({ rid: '4200000071', listing: L, old: 'LEAFY OLD', sku: 'LEAF_4821', d: 8, title: 'Leaf Charm' });
    eq([r.gate, r.error, r.by], [true, 'sku-taken', 'master'], 'another design under the taken SKU: the door refuses to overwrite the entry\'s files (sku-taken, before a byte is written)');
    r = await save({ rid: '4200000071', listing: L, old: 'LEAFY OLD', sku: chk.free, d: 8, title: 'Leaf Charm' });
    eq([r.ok, r.created, r.sku], [true, true, chk.free], 'the next free SKU takes it');
    W = await world();
    picks(read(W, line('LEAFY OLD', L)), chk.free, 'alias', 'the second leaf\'s listing');
    picks(read(W, line('FIRST LEAF', '1900204821')), 'LEAF_4821', 'alias', 'the first leaf\'s listing, untouched');
  }

  /* ── 8 · the entry is what the master indexer writes for the same drawing (the fields every reader uses) ── */
  {
    const d = 9; await save({ rid: '4200000090', listing: '1900009090', old: 'NINE OLD', sku: 'NINE_9090', d });
    await Master.putIndex(Fk.db, Fk.admin.firestore.FieldValue, { masterHash: 'a'.repeat(64), masterName: 'Sheet.ai', masterPath: 'charmnest/masters/sheet.ai', entries: [Object.assign({ sku: 'NINE_REF', masterHash: 'a'.repeat(64), aiPath: 'charmnest/master/NINE_REF.ai', thumbPath: 'charmnest/master/NINE_REF.png', aiUrl: 'https://x/ai', thumbUrl: 'https://x/png', open: false, labelSource: 'text', confidence: null }, GEO(d))] });
    const list = (await post({ op: 'masterList', limit: 3000 })).body.entries, mine = list.find(e => e.sku === 'NINE_9090'), ref = list.find(e => e.sku === 'NINE_REF');
    const pick = e => ({ charmHash: e.charmHash, widthPt: e.widthPt, heightPt: e.heightPt, areaPt2: e.areaPt2, members: e.members, holes: e.holes, engravable: e.engravable, upAngle: e.upAngle, upSource: e.upSource, open: e.open, blocked: e.blocked, conflict: e.conflict, sizeMoved: e.sizeMoved, labelSource: e.labelSource, sizes: e.sizes, pair: e.pair || null, facing: e.facing || null, backKeepOut: e.backKeepOut, hasAi: !!e.aiPath && !!e.aiUrl, hasThumb: !!e.thumbPath && !!e.thumbUrl });
    eq(pick(mine), pick(ref), 'the saved entry equals the master indexer\'s for the same drawing in every field the readers use');
    const extra = Object.keys(ref).filter(k => !(k in mine)), missing = Object.keys(mine).filter(k => !(k in ref));
    eq([extra.filter(k => !['indexedAt', 'masterName'].includes(k)), missing], [[], []], 'and the same set of fields (' + Object.keys(mine).length + ')');
  }

  ok(netCalls === 0, 'no network');
  console.warn = warn; console.error = err;
}

/* ═══ part B ═══ */
async function partB() {
  const pwDir = process.env.PW_DIR;   // (set by /tmp/cn-run.sh: a browser test runs only through it)
  if (!pwDir) { console.log('  - PW_DIR not set: the geometry checks (part B) were not run'); return; }
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the geometry checks (part B) were not run'); return; }
  const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!require('fs').existsSync(exe)) { console.log('  - no browser at ' + exe + ': part B was not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1200, height: 800 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort()));
    const pg = await context.newPage(), errors = []; pg.on('pageerror', e => errors.push(e.message)); pg.setDefaultTimeout(60000);
    await pg.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await pg.waitForFunction(() => window.CN && window.Master && window.CharmNestRepoLock && window.CharmNestDXF && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
    // an asymmetric leaf with a hanging hole: a design whose up angle and silhouette are not the same turned any way
    const DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES', 0, 'LWPOLYLINE', 8, 'CUT', 90, 5, 70, 1, 10, 0, 20, 0, 10, 24, 20, 2, 10, 30, 20, 22, 10, 10, 20, 34, 10, 2, 20, 18, 0, 'CIRCLE', 8, 'CUT', 10, 14, 20, 24, 40, 1.4, 0, 'ENDSEC', 0, 'EOF');
    const out = await pg.evaluate(async dxf => {
      const P = CharmNestPDF, G = window.CharmNestGeom || window.G;
      const readFile = async (bytes, name) => {
        const parsed = await P.parseSource(bytes, name), g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: 6 });
        for (const m of g.orphans) { if (!m.bbox) continue; const c = g.charms.find(c => { const b = c.bbox; return m.bbox[0] >= b[0] - 2 && m.bbox[1] >= b[1] - 2 && m.bbox[2] <= b[2] + 2 && m.bbox[3] <= b[3] + 2; }); if (c && !c.members.includes(m)) c.members.push(m); }
        for (const c of g.charms) P.integrateRings(c);
        await P.buildSilhouettes(parsed, g.charms, 6);
        return { parsed, charm: g.charms.filter(c => !c.open)[0], count: g.charms.length };
      };
      const dropped = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'Leaf.dxf');
      const a = await readFile(dropped.bytes, 'Leaf.dxf');
      const upOf = async c => (window.CharmNestBackground ? await CharmNestBackground.run('indexGeometry', { charm: { outline: c.outline, members: c.members, bbox: c.bbox, widthPt: c.widthPt, heightPt: c.heightPt } }) : G.upAngleOf(c));
      const upA = await upOf(a.charm);
      // what the page stores: the single-charm .ai (Master.saveCustom builds it the way the master indexer does); then the MASTER PIPELINE reads that stored file back
      const stored = await P.buildSingleCharm(Object.assign({}, a.charm, { name: 'LEAF_0001' }), a.parsed);
      const b = await readFile(stored, 'LEAF_0001.ai');
      const upB = await upOf(b.charm);
      const rnd = x => Math.round(x * 100) / 100;
      return { a: { hash: a.charm.hash, w: rnd(a.charm.widthPt), h: rnd(a.charm.heightPt), area: rnd(a.charm.areaPt2), holes: P.cutLinesOf(a.charm).length, up: upA.angle, src: upA.source, thumb: !!a.charm.thumb, n: a.count },
        b: { hash: b.charm.hash, w: rnd(b.charm.widthPt), h: rnd(b.charm.heightPt), area: rnd(b.charm.areaPt2), holes: P.cutLinesOf(b.charm).length, up: upB.angle, src: upB.source, thumb: !!b.charm.thumb, n: b.count }, bytes: stored.length };
    }, DXF);
    ok(out.a.thumb && out.a.n >= 1 && out.bytes > 200, 'a vector file read the way the Designs window reads it, and the stored single-charm file built from it: ' + JSON.stringify(out.a));
    eq(out.b.hash, out.a.hash, 'the stored file, read back by the master pipeline, has the SAME charm hash as the dropped file');
    eq([out.b.w, out.b.h, out.b.area, out.b.holes], [out.a.w, out.a.h, out.a.area, out.a.holes], 'the same size, area and holes');
    eq([out.b.up, out.b.src], [out.a.up, out.a.src], 'the same up angle: right orientation as the master indexer would have it');
    // the real page save, then the page's own library and its orders code pick the design for a fresh order of the listing
    const res = await pg.evaluate(async dxf => {
      const P = CharmNestPDF, r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'Leaf.dxf');
      const parsed = await P.parseSource(r.bytes, 'Leaf.dxf'), g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: 6 });
      for (const m of g.orphans) { if (!m.bbox) continue; const c = g.charms.find(c => { const b = c.bbox; return m.bbox[0] >= b[0] - 2 && m.bbox[1] >= b[1] - 2 && m.bbox[2] <= b[2] + 2 && m.bbox[3] <= b[3] + 2; }); if (c && !c.members.includes(m)) c.members.push(m); }
      for (const c of g.charms) P.integrateRings(c);
      await P.buildSilhouettes(parsed, g.charms, 6);
      const charm = g.charms.filter(c => !c.open)[0], hash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', r.bytes))].map(b => b.toString(16).padStart(2, '0')).join('');
      const tok = await new Promise(res => res(null));
      return { hash: charm.hash, fileHash: hash };
    }, DXF);
    ok(res.hash === out.a.hash, 'the same dropped file gives the same charm hash every time it is read');
    eq(errors, [], 'no page error');
    console.log('  geometry of the dropped file vs the stored file: hash ' + out.a.hash + ' = ' + out.b.hash + ', up ' + out.a.up + ' (' + out.a.src + '), size ' + out.a.w + 'x' + out.a.h + ' pt, ' + out.a.holes + ' hole(s)');
  } finally { await browser.close(); srv.close(); }
}

(async () => {
  await partA(); console.log('  part A (Node): ok');
  await partB(); console.log('  part B: done');
  console.log('keep-association OK: ' + n + ' checks');
})().catch(e => { process.stderr.write(String(e && e.stack || e) + '\n'); process.exit(1); });
