'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const readOnly = require('../../netlify/functions/_britesGrowthAdsReadOnly');
const projection = require('../../netlify/functions/googleAdsAdDesignResearch');

const NOW = Date.parse('2026-10-02T22:00:00Z');
const PRODUCT_ID = 'gid://shopify/Product/190';
const VERSION = '9'.repeat(64);
function recommendationFixture(positive = 'wolf necklace', negative = 'free template') {
  const source = { id: 'shop', url: 'https://britesjewelry.com/products/wolf-necklace', title: 'Exact product', excerpt: 'Exact wolf necklace evidence.', reviewed: true, checkedAt: NOW };
  return {
    productId: PRODUCT_ID, handle: 'wolf-necklace', status: 'approved', version: VERSION,
    currentDossierVersion: VERSION, savedAt: NOW, sources: [source], facts: [], meanings: [], competitors: [], buyerIntents: ['Wolf gift'],
    recommendations: [
      { channel: 'keywords', basis: 'hypothesis', action: 'Test wolf necklace interest.', measure: 'Qualified visits.', sourceIds: ['shop'], keywords: [positive] },
      { channel: 'negatives', basis: 'hypothesis', action: 'Exclude irrelevant intent.', measure: 'Search-term relevance.', sourceIds: ['shop'], negativeKeywords: [negative] }
    ]
  };
}
function meaningFixture(url) {
  return {
    product: { id: PRODUCT_ID, handle: 'star-necklace' },
    dossier: {
      productId: PRODUCT_ID, handle: 'star-necklace', status: 'approved', version: VERSION,
      currentDossierVersion: VERSION, savedAt: NOW, competitors: [],
      sources: [{ id: 'meaning-source', url, title: 'Reviewed interpretation', excerpt: 'Stars appear in commemorative design.', reviewed: true, checkedAt: NOW }],
      meanings: [{ kind: 'interpretation', text: 'A star may be a personal reminder of achievement and new beginnings.', context: 'A personal interpretation that can vary.', sourceIds: ['meaning-source'] }]
    }
  };
}

test('whole-token negative phrase containment rejects the complete recommendation packet', () => {
  assert.equal(projection.projectRecommendations(recommendationFixture('wolf necklace', 'necklace'), new Set(['shop']), NOW), null);
  assert.equal(projection.projectRecommendations(recommendationFixture('new wolf necklace gift', '  ＷＯＬＦ   necklace '), new Set(['shop']), NOW), null);
  assert.equal(projection.projectRecommendations(recommendationFixture('wolf', 'wolf necklace'), new Set(['shop']), NOW), null);
});

test('unrelated partial substrings do not become keyword conflicts', () => {
  const result = projection.projectRecommendations(recommendationFixture('cat necklace', 'caterpillar necklace'), new Set(['shop']), NOW);
  assert.ok(result?.operatorReviewPacket);
  assert.equal(readOnly.suppressiveKeywordConflict('cat necklace', 'caterpillar necklace'), false);
  assert.equal(readOnly.suppressiveKeywordConflict('gold', 'golden'), false);
});

test('unlisted retailer editorial URLs cannot source shopper-facing meaning hypotheses', () => {
  for (const url of [
    'https://retailer.example.com/guides/star-symbolism',
    'https://meaning-jewelry.example.com/editorial/stars',
    'https://www.etsy.com/blog/star-symbolism'
  ]) {
    const value = meaningFixture(url);
    assert.equal(readOnly.projectApprovedMeaningHypotheses({ ...value, at: NOW }), null, url);
  }
});

test('neutral institutional sources remain eligible', () => {
  for (const url of [
    'https://museum.example.edu/collections/stars',
    'https://www.loc.gov/exhibitions/stars/',
    'https://naturalhistory.si.edu/education/teaching-resources/astronomy/stars'
  ]) {
    const value = meaningFixture(url), result = readOnly.projectApprovedMeaningHypotheses({ ...value, at: NOW });
    assert.ok(result?.meaningOperatorReviewPacket?.candidates?.length, url);
  }
});

test('own Brites product evidence remains eligible while direct foreign commerce paths remain blocked', () => {
  let value = meaningFixture('https://britesjewelry.com/products/star-necklace');
  assert.ok(readOnly.projectApprovedMeaningHypotheses({ ...value, at: NOW }));
  value = meaningFixture('https://example.com/shop/star-necklace');
  assert.equal(readOnly.projectApprovedMeaningHypotheses({ ...value, at: NOW }), null);
});
