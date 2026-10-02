'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const audit = require('../../scripts/audit-growth-catalogue.cjs');
const NOW = Date.parse('2026-10-03T10:00:00Z');
const variant = (id, sku = 'TEST-' + id, title = 'Silver', options = [{ name: 'Metal', value: 'Silver' }]) => ({ id: 'gid://shopify/ProductVariant/' + id, title, sku, options });
const product = (id, overrides = {}) => ({ id: 'gid://shopify/Product/' + id, handle: 'fixture-' + id, url: 'https://britesjewelry.com/products/fixture-' + id,
  title: 'Test Necklace', type: 'Necklace', descriptionHtml: '<p>This necklace is 18 inches long.</p>', options: [{ name: 'Metal', values: ['Silver'] }], variants: [variant(id + 100)], checkedAt: NOW, ...overrides });
const codes = p => audit.detectProductFindings(p).map(f => f.code);

test('reused SKU across different products preserves every exact product/variant ID without shopper holds', () => {
  const products = [product(1, { title: 'Fox Stud Earrings', type: 'Earrings', variants: [variant(101, 'REUSED')] }), product(2, { title: 'Star Charm', type: 'Charm', variants: [variant(102, 'REUSED')] })];
  const report = audit.auditCatalogue(products, NOW);
  assert.equal(report.collisions.counts.crossProductExactSkuGroups, 1);
  assert.deepEqual(report.collisions.crossProductGroups[0].productIds, ['gid://shopify/Product/1', 'gid://shopify/Product/2']);
  assert.deepEqual(report.collisions.crossProductGroups[0].variantIds, ['gid://shopify/ProductVariant/101', 'gid://shopify/ProductVariant/102']);
  assert.equal(report.collisions.crossProductGroups[0].risk, 'internal_matching_only');
  assert.equal(report.collisions.crossProductGroups[0].autoHold, false);
  assert.equal(report.summary.automaticShopperHolds, 0);
  assert.ok(report.findings.every(f => !f.shopperHolds.recommendation && !f.shopperHolds.cart));
});
test('same-product metal variants sharing a SKU are distinguished from cross-product collisions', () => {
  const r = audit.skuCollisions([product(1, { variants: [variant(101, 'SAME'), variant(102, 'SAME', 'Gold')] })]);
  assert.equal(r.counts.reusedExactSkuGroups, 1); assert.equal(r.counts.crossProductExactSkuGroups, 0); assert.equal(r.withinProductGroups.length, 1);
});
test('blank SKUs do not collapse unrelated products into an empty collision group', () => {
  const r = audit.skuCollisions([product(1, { variants: [variant(101, null)] }), product(2, { variants: [variant(102, ' ')] })]);
  assert.equal(r.counts.missingSkuVariants, 2); assert.equal(r.crossProductGroups.length, 0); assert.equal(r.withinProductGroups.length, 0);
});
test('case-only aliases are reported separately without relabelling them exact SKUs', () => {
  const r = audit.skuCollisions([product(1, { variants: [variant(101, 'Example')] }), product(2, { variants: [variant(102, 'EXAMPLE')] })]);
  assert.equal(r.counts.crossProductExactSkuGroups, 0); assert.equal(r.counts.crossProductCaseAliasGroups, 1); assert.equal(r.caseAliasGroups[0].mode, 'case_folded_alias');
});
test('title final format distinguishes charm necklace from necklace charm and avoids earrings/ring substring errors', () => {
  assert.equal(audit.primaryFamily('Test Charm Necklace'), 'necklace'); assert.equal(audit.primaryFamily('Test Necklace Charm'), 'charm'); assert.equal(audit.primaryFamily('Test Stud Earrings'), 'earrings');
  assert.ok(!codes(product(1, { title: 'Charm Necklace', type: 'Necklace' })).includes('title_type_conflict'));
});
test('explicit bracelet title and necklace type produce a guarded existing-editor draft', () => {
  const r = audit.auditCatalogue([product(1, { title: 'Textured Bar Bracelet', type: 'Necklace', descriptionHtml: '<p>Choose one of these bracelets.</p>', options: [{ name: 'Bracelet length', values: ['6', '7', '8'] }] })], NOW);
  const f = r.findings.find(f => f.code === 'title_type_conflict'); assert.ok(f); assert.equal(f.proposal.candidate, 'Bracelets');
  const d = r.drafts[0]; assert.equal(d.editorActionDrafts[0].action, 'updateProductFields'); assert.equal(d.editorActionDrafts[0].body.product_id, 'gid://shopify/Product/1');
  assert.equal(d.editorActionDrafts[0].executable, false); assert.equal(d.productionMutations, false); assert.equal(d.reviewState, 'requires_inspection');
});
test('plural earrings subject in necklace description is flagged, adjacent styling language is not', () => {
  assert.ok(codes(product(1, { descriptionHtml: '<p>Made in silver, these earrings celebrate your hobby.</p>' })).includes('title_description_type_conflict'));
  assert.ok(!codes(product(2, { descriptionHtml: '<p>This necklace pairs with our earrings.</p>' })).includes('title_description_type_conflict'));
});
test('stud fastening contradiction uses observed text, without examining or asserting a pictured motif', () => {
  const p = product(1, { title: 'Harbor Stud Earrings', type: 'Earrings', descriptionHtml: '<p>The discs are suspended from elegant ear wires.</p>', image: 'https://example.com/uninspected.jpg' });
  const f = audit.detectProductFindings(p).find(f => f.code === 'stud_fastening_copy_conflict'); assert.ok(f);
  assert.ok(f.evidence.some(e => e.quote.includes('suspended from elegant ear wires'))); assert.ok(f.limitations.some(l => /uninspected image/.test(l)));
});
test('stud cross-selling with dangle earrings is not treated as a fastening conflict', () => {
  assert.ok(!codes(product(1, { title: 'Leaf Stud Earrings', type: 'Earrings', descriptionHtml: '<p>Layer these studs with dangle earrings.</p>' })).includes('stud_fastening_copy_conflict'));
});
test('fishing-hook motif wording alone does not imply hook fastening; explicit ear hooks still conflict with studs', () => {
  assert.ok(!codes(product(1, { title: 'Fishing Hook Stud Earrings', type: 'Earrings', descriptionHtml: '<p>Discover our elegant Hook Earrings, a gift for fishing enthusiasts.</p>' })).includes('stud_fastening_copy_conflict'));
  assert.ok(codes(product(2, { title: 'Leaf Stud Earrings', type: 'Earrings', descriptionHtml: '<p>The leaves are suspended from elegant hooks.</p>' })).includes('stud_fastening_copy_conflict'));
});
test('unselected initial is a catalogue-integration review; fixed J initial remains selectable as its fixed design', () => {
  const f = audit.detectProductFindings(product(1, { title: 'Monogram Initial Necklace' })).find(f => f.code === 'initial_selection_not_in_catalogue');
  assert.ok(f); assert.ok(f.limitations.some(l => /line-item properties/.test(l))); assert.equal(f.shopperHolds.cart, false);
  assert.ok(!codes(product(2, { title: 'J Initial Necklace' })).includes('initial_selection_not_in_catalogue'));
});
test('letter choices supplied in named options, variant properties or a generic style chooser satisfy initial selection', () => {
  for (const p of [product(1, { title: 'Initial Necklace', options: [{ name: 'Letters', values: ['A', 'B'] }] }),
    product(2, { title: 'Initial Necklace', options: [], variants: [variant(102, 'A', 'A', [{ name: 'Initial', value: 'A' }])] }),
    product(3, { title: 'Initial Necklace', options: [{ name: 'Style', values: ['A', 'B'] }] })]) assert.ok(!codes(p).includes('initial_selection_not_in_catalogue'));
});
test('custom upload evidence requests workflow inspection without pretending variants contain theme upload fields', () => {
  const f = audit.detectProductFindings(product(1, { title: 'Custom Photo Necklace', descriptionHtml: '<p>Upload your photo to create this necklace.</p>' })).find(f => f.code === 'custom_input_not_in_catalogue');
  assert.ok(f); assert.equal(f.proposal.requiredInput, 'upload_or_design_reference'); assert.equal(f.reviewState, 'requires_inspection'); assert.equal(f.shopperHolds.cart, false);
  assert.ok(!codes(product(2, { title: 'Custom Photo Necklace', options: [{ name: 'Design image', values: ['Provided'] }] })).includes('custom_input_not_in_catalogue'));
});
test('studio credit/membership products are counted but do not masquerade as missing physical customization inputs', () => {
  const p = product(1, { title: 'Custom Charm Studio Membership — 20 credits', type: 'Custom Charm Studio', descriptionHtml: '<p>Design credits.</p>', options: [] });
  const r = audit.auditCatalogue([p], NOW); assert.equal(r.summary.studioServiceProducts, 1); assert.ok(!codes(p).includes('custom_input_not_in_catalogue'));
});
test('photo-frame and film-roll motifs do not create an invented customer upload requirement', () => {
  for (const title of ['Film Roll Photo Stud Earrings', 'Photo Frame Charm', 'Photo Frame Pendant Beady Necklace', 'Portrait Frame Charm']) {
    assert.ok(!codes(product(1, { title, descriptionHtml: '<p>A thoughtful gift for photographers.</p>' })).includes('custom_input_not_in_catalogue'));
  }
  assert.ok(codes(product(2, { title: 'Dog Photo Necklace', descriptionHtml: '<p>Send us your photo for engraving.</p>' })).includes('custom_input_not_in_catalogue'));
});
test('advertised length choice missing in option export is reviewed; fixed length alone is not an invented requirement', () => {
  assert.ok(codes(product(1, { descriptionHtml: '<p>Choose your necklace length.</p>' })).includes('length_selection_not_in_catalogue'));
  assert.ok(!codes(product(2, { descriptionHtml: '<p>This necklace comes on an 18 inch chain.</p>' })).includes('length_selection_not_in_catalogue'));
  assert.ok(!codes(product(3, { descriptionHtml: '<p>Choose your chain length.</p>', options: [{ name: 'Chain Length', values: ['18 inch'] }] })).includes('length_selection_not_in_catalogue'));
});
test('explicit inch range versus 20 inch variant is flagged; units not reported in centimetres are not guessed', () => {
  assert.ok(codes(product(1, { descriptionHtml: '<p>Available necklace lengths are 14 - 18 inches.</p>', options: [{ name: 'Necklace Length', values: ['14 inch', '20 inch'] }] })).includes('claimed_length_range_conflict'));
  assert.ok(!codes(product(2, { descriptionHtml: '<p>Available necklace lengths are 14 - 18 inches.</p>', options: [{ name: 'Necklace Length', values: ['40 cm'] }] })).includes('claimed_length_range_conflict'));
});
test('unavailable Rose Gold claim is isolated from valid selectable metals', () => {
  const p = product(1, { descriptionHtml: '<p>Available in sterling silver, gold filled, or rose gold filled.</p>', options: [{ name: 'Metal Choice', values: ['Sterling Silver', '14k Gold Filled'] }] });
  const f = audit.detectProductFindings(p).find(f => f.code === 'described_metal_not_selectable'); assert.ok(f); assert.equal(f.shopperHolds.recommendation, false);
});
test('broken punctuation/encoding is detected while legitimate accents and multilingual text survive', () => {
  assert.ok(codes(product(1, { descriptionHtml: '<p>Rose gold filled‚Äîthis design is delicate.</p>' })).includes('broken_encoding_descriptionHtml'));
  assert.ok(codes(product(2, { title: 'Necklace �' })).includes('broken_encoding_title'));
  assert.ok(!codes(product(3, { title: 'José Necklace', descriptionHtml: '<p>Élégant bijou. 日本語 العربية</p>' })).some(c => c.startsWith('broken_encoding')));
});
test('evidence is exact product-bound, uses observed review time and lists exact variant IDs', () => {
  const p = product(1, { title: 'Test Bracelet', type: 'Necklace' }); const f = audit.detectProductFindings(p).find(f => f.code === 'title_type_conflict');
  assert.ok(f.evidence.every(e => e.url === 'https://britesjewelry.com/products/fixture-1' && e.checkedAt === NOW));
  assert.ok(f.evidence.some(e => e.variantIds.includes('gid://shopify/ProductVariant/101')));
  const r = audit.auditCatalogue([p], NOW + 100); assert.equal(r.generatedAt, NOW + 100); assert.ok(!JSON.stringify(r).includes('orders'));
});
test('private outputs are refused anywhere inside the repository', () => {
  assert.throws(() => audit.privateOutputPath('/repo/out', '/repo'), /outside/); assert.throws(() => audit.privateOutputPath('/repo', '/repo'), /outside/);
  assert.equal(audit.privateOutputPath('/private/audit', '/repo'), '/private/audit'); assert.equal(audit.privateOutputPath('/repo-other/audit', '/repo'), '/repo-other/audit');
});
test('untrusted external or mismatched product URLs never become canonical evidence', () => {
  assert.equal(audit.canonicalUrl(product(1, { url: 'https://other.example/products/fixture-1' })), null);
  assert.equal(audit.canonicalUrl(product(1, { url: 'https://britesjewelry.com/products/fixture-2' })), null);
  assert.ok(codes(product(1, { id: 'not-an-id' })).includes('invalid_product_identity'));
});
test('multi-format title emits review without an unproved single replacement type', () => {
  const f = audit.detectProductFindings(product(1, { title: 'Test Necklace / Bracelet', type: 'Necklace' })).find(f => f.code === 'title_type_conflict');
  assert.ok(f); assert.equal(f.proposal, null);
});
test('charm-only options support the existing Charm type and prevent a blind necklace type replacement', () => {
  const p = product(1, { title: 'Rabbit Charm Necklace', type: 'Charm', options: [{ name: 'Charm Type', values: ['Necklace CHARM', 'Huggie CHARM SET'] }] });
  const report = audit.auditCatalogue([p], NOW), finding = report.findings.find(f => f.code === 'title_type_conflict');
  assert.ok(finding); assert.equal(finding.proposal.field, 'title'); assert.equal(finding.proposal.candidate, undefined);
  assert.deepEqual(report.drafts[0].editorActionDrafts, []);
});
test('a Custom Charm workflow category is reviewed without proposing a physical-format category replacement', () => {
  const p = product(1, { title: 'Custom Charm Pendant Necklace', type: 'Custom Charm' });
  const report = audit.auditCatalogue([p], NOW), finding = report.findings.find(f => f.code === 'title_type_conflict');
  assert.ok(finding); assert.equal(finding.priority, 'P2'); assert.equal(finding.proposal, null); assert.deepEqual(report.drafts[0].editorActionDrafts, []);
});
