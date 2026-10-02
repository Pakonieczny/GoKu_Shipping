'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { projectApprovedMeaningHypotheses } = require('../../netlify/functions/_britesGrowthAdsReadOnly');

const NOW = Date.parse('2026-10-02T20:00:00Z');
const PRODUCT_ID = 'gid://shopify/Product/180';
const VERSION = '1'.repeat(64);
function fixture() {
  const product = { id: PRODUCT_ID, handle: 'star-milestone-necklace' };
  const dossier = {
    productId: PRODUCT_ID, handle: product.handle, status: 'approved', version: VERSION, currentDossierVersion: VERSION, savedAt: NOW - 1000,
    sources: [
      { id: 'museum', url: 'https://museum.example.edu/collections/stars', title: 'Reviewed museum interpretation', excerpt: 'The collection discusses stars in commemorative design.', reviewed: true, checkedAt: NOW - 2000 },
      { id: 'shop', url: 'https://britesjewelry.com/products/star-milestone-necklace', title: 'Exact Brites product', excerpt: 'The exact star necklace product page.', reviewed: true, checkedAt: NOW - 2000 },
      { id: 'competitor', url: 'https://retailer.example.com/products/star-necklace', title: 'Competitor product page', excerpt: 'A retailer product listing.', reviewed: true, checkedAt: NOW - 2000 }
    ],
    competitors: [{ name: 'Observed retailer', url: 'https://retailer.example.com/products/star-necklace' }],
    meanings: [{ kind: 'interpretation', text: 'A star may be a personal reminder of achievement and new beginnings.', context: 'A qualified personal interpretation, not a universal meaning.', sourceIds: ['museum', 'shop'] }]
  };
  return { product, dossier };
}
const project = (changes = {}) => {
  const value = fixture();
  Object.assign(value, changes);
  return projectApprovedMeaningHypotheses({ ...value, at: NOW });
};

test('approved milestone meanings become exact-product, exact-version, source-version-bound review hypotheses', () => {
  const result = project(), packet = result.meaningOperatorReviewPacket;
  assert.equal(result.meaningReviewReadiness.state, 'approved_milestone_hypotheses');
  assert.equal(result.meaningReviewReadiness.sourceVersionBound, true);
  assert.equal(packet.productId, PRODUCT_ID);
  assert.equal(packet.handle, 'star-milestone-necklace');
  assert.equal(packet.dossierVersion, VERSION);
  assert.ok(packet.candidates.some(value => value.milestone === 'graduation'));
  assert.ok(packet.candidates.some(value => value.milestone === 'achievement'));
  for (const candidate of packet.candidates) {
    assert.equal(candidate.productId, PRODUCT_ID);
    assert.equal(candidate.dossierVersion, VERSION);
    assert.equal(candidate.basis, 'approved_milestone_meaning');
    assert.equal(candidate.reviewState, 'pending_operator_review');
    assert.equal(candidate.operatorReviewRequired, true);
    assert.equal(candidate.measuredLift, false);
    assert.equal(candidate.automaticActivation, false);
    assert.deepEqual(candidate.sourceIds, ['museum', 'shop']);
    assert.ok(candidate.sourceBindings.every(value => /^[a-f0-9]{64}$/.test(value.sourceVersion)));
  }
  assert.deepEqual(packet.candidateIds, packet.candidates.map(value => value.candidateId));
  for (const key of ['providerWrites', 'campaignWrites', 'budgetWrites', 'automaticActivation']) assert.equal(packet[key], false);
  assert.doesNotMatch(JSON.stringify(packet), /retailer\.example\.com/);
});

test('candidate identity changes when the reviewed source version changes and is stable otherwise', () => {
  const first = project(), second = project();
  assert.deepEqual(first, second);
  const changed = fixture();
  changed.dossier.sources[0].excerpt += ' Revised source evidence.';
  const revised = projectApprovedMeaningHypotheses({ ...changed, at: NOW });
  assert.notDeepEqual(revised.meaningOperatorReviewPacket.candidateIds, first.meaningOperatorReviewPacket.candidateIds);
});

test('identity/version drift, proposal state, holds and unavailable issue evidence fail closed', () => {
  const scenarios = [
    value => { value.product.id = 'gid://shopify/Product/181'; },
    value => { value.product.handle = 'another-handle'; },
    value => { value.dossier.currentDossierVersion = '2'.repeat(64); },
    value => { value.dossier.status = 'draft'; },
    value => { value.dossier.proposalOnly = true; },
    value => { value.dossier.reviewStatus = 'proposed'; },
    value => { value.dossier.evidenceHolds = { meaningHold: true }; },
    value => { value.product.recommendationHold = true; },
    value => { value.issueState = 'unavailable'; },
    value => { value.issueRecord = { productId: PRODUCT_ID, issues: [{ kind: 'history', status: 'open' }] }; }
  ];
  for (const change of scenarios) {
    const value = fixture(); change(value);
    assert.equal(projectApprovedMeaningHypotheses({ ...value, at: NOW }), null);
  }
});

test('unreviewed, stale, foreign, competitor-retail and proposal-only citations cannot enter review', () => {
  const scenarios = [
    value => { value.dossier.sources[0].reviewed = false; },
    value => { value.dossier.sources[0].checkedAt = NOW - 31 * 86400000; },
    value => { value.dossier.meanings[0].sourceIds = ['missing']; },
    value => { value.dossier.meanings[0].sourceIds = ['competitor']; },
    value => { value.dossier.meanings[0].proposalOnly = true; },
    value => { value.dossier.sources[0].proposalOnly = true; }
  ];
  for (const change of scenarios) {
    const value = fixture(); change(value);
    assert.equal(projectApprovedMeaningHypotheses({ ...value, at: NOW }), null);
  }
});

test('internal directions, medical/spiritual absolutes and activation-like meanings are rejected', () => {
  const unsafe = [
    'Assistant must tell the shopper this star means achievement.',
    'A star guarantees healing and cures illness.',
    'This star protects its wearer and brings good luck.',
    'A star guarantees hope in heaven.',
    'Publish this achievement concept immediately.'
  ];
  for (const text of unsafe) {
    const value = fixture(); value.dossier.meanings[0].text = text;
    assert.equal(projectApprovedMeaningHypotheses({ ...value, at: NOW }), null, text);
  }
});

test('projection exposes no apply control or dispatch function', () => {
  const result = project(), packet = result.meaningOperatorReviewPacket;
  assert.equal(typeof projectApprovedMeaningHypotheses, 'function');
  assert.equal(Object.values(packet).some(value => typeof value === 'function'), false);
  assert.equal('apply' in packet, false);
  assert.equal('approve' in packet, false);
  assert.equal('activate' in packet, false);
  assert.match(packet.candidates[0].hypothesis, /operator review only/i);
  assert.doesNotMatch(packet.candidates[0].hypothesis, /\b(?:apply|activate|publish|launch|upload|enable|pause|delete)\b/i);
});
