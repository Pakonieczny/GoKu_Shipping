'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const Catalogue = require('../../brites-catalogue-intents.js');
const Bridge = require('../../brites-storefront-bridge.js');
const fixtureFile = path.join(__dirname, 'adversarial-shopping43.test.cjs');
const fixtureRequire = createRequire(fixtureFile);
// Reuse the existing real host/bridge/widget and synthetic transport without
// registering or changing its independently authored acceptance cases.
const { fixture, product, catalogue } = new Function('require', '__dirname', '__filename', fs.readFileSync(fixtureFile, 'utf8') + '\nreturn {fixture,product,catalogue};')(
  name => name === 'node:test' ? () => {} : fixtureRequire(name), path.dirname(fixtureFile), fixtureFile
);

const ids = rows => Array.from(rows, row => row.id).sort();
const clone = value => JSON.parse(JSON.stringify(value));

function ordinaryCharm() {
  const p = product(44101, 'Fox Necklace Charm', { type: 'Charms', motif: 'fox', silver: 20, gold: 35 });
  p.partsOnly = true;
  p.options[1] = { name: 'Charm Type', values: ['Necklace Charm', 'Bracelet Charm'] };
  p.variants = p.variants.map(v => {
    const style = v.options[1].value === '8.5mm' ? 'Necklace Charm' : 'Bracelet Charm';
    return { ...v, title: v.options[0].value + ' / ' + style, options: [v.options[0], { name: 'Charm Type', value: style }] };
  });
  return p;
}

function metalProduct(id, title, metal) {
  const p = product(id, title, { motif: 'fox', silver: 30, gold: 60 });
  p.options[0].values[1] = metal;
  p.variants = p.variants.map(v => ({ ...v, title: v.title.replace('14k Gold Filled', metal), options: v.options.map(o => o.name === 'Metal Choice' && o.value === '14k Gold Filled' ? { ...o, value: metal } : o) }));
  return p;
}

function assertActualSelection(h, result, expected) {
  assert.equal(result.ok, true, result.reply || result.error);
  const snapshot = h.store.snapshot();
  assert.equal(snapshot.pageKind, 'collection');
  assert.deepEqual(ids(snapshot.visiblePieces), ids(result.products), 'Spoken checked matches and the actual host must show the same identities');
  assert.deepEqual(h.handles().sort(), Array.from(result.products, p => p.handle).sort(), 'Native rendered cards agree with the checked receipt');
  const native = h.hostResults.find(turn => turn.result === result);
  if (native) {
    const spoken = h.customerReplies.find(reply => reply.inputItemId === native.inputItemId);
    assert(spoken, 'The real finalized widget result and its customer reply belong to the same native input');
    assert.deepEqual(Object.keys(spoken.payload), ['reply'], 'Checked identities stay in host evidence while spoken input contains only customer copy');
    assert.doesNotMatch(JSON.stringify(spoken.payload), /"(?:products|productFacts|actions|completedActions|matchingVariantIds|snapshot|publicContext|preferences)"\s*:|gid:\/\/shopify\/(?:Product|ProductVariant)\/|PRIVATE_/i);
  }
  if (expected) assert.deepEqual(ids(result.products), ids(expected));
  assert.deepEqual(h.errors, []);
}

test('category exclusions survive shared query serialization and do not become positive bridge filters', () => {
  for (const phrase of ['Show animal jewelry without necklaces', 'What animal jewelry do you have without necklaces?', 'Show earrings without necklaces', 'Show animal jewelry excluding necklaces and rings']) {
    const discovered = Catalogue.discovery(phrase), roundtrip = Catalogue.plan(Catalogue.queryFor(discovered.plan));
    assert.equal(discovered.recognized, true, phrase);
    assert.deepEqual(roundtrip.excludedCategories, discovered.plan.excludedCategories, phrase);
    assert.deepEqual(roundtrip.categories, discovered.plan.categories, phrase);
    const action = Bridge.resolve(phrase, { pageKind: 'collection', contextRevision: 0, discoveryRevision: 0, visiblePieces: [], filter: 'all', search: '' });
    assert.equal(action.ok, true, phrase + ': ' + action.reason);
    assert.equal(action.action.filter, discovered.plan.categories.length === 1 ? discovered.plan.categories[0] : 'all', phrase);
  }
});

test('both requested categories retain OR semantics in direct and question bridge routes', () => {
  for (const phrase of ['Show animal necklaces and earrings', 'What animal necklaces and earrings do you have?']) {
    const plan = Catalogue.discovery(phrase).plan;
    assert.deepEqual(plan.categories, ['necklaces', 'earrings']);
    const action = Bridge.resolve(phrase, { pageKind: 'collection', contextRevision: 0, discoveryRevision: 0, visiblePieces: [], filter: 'all', search: '' });
    assert.equal(action.ok, true, phrase + ': ' + action.reason);
    assert.equal(action.action.filter, 'all', phrase);
  }
});

for (const nativeVoice of [false, true]) {
  const mode = nativeVoice ? 'synthetic realtime' : 'typed';
  test('category exclusion displays exactly its checked matches and preserves ordinary necklace charms through ' + mode, async t => {
    const charm = ordinaryCharm(), ear = product(44102, 'Fox Stud Earrings', { motif: 'fox', silver: 35 }), necklace = product(44103, 'Fox Chain Necklace', { type: 'Necklace', motif: 'fox', silver: 15 }), leaf = product(44104, 'Leaf Stud Earrings', { motif: 'leaf' });
    const h = await fixture(t, { rows: [charm, ear, necklace, leaf], nativeVoice });
    if (nativeVoice) await h.startVoice();
    const reads = h.requests.length, result = await (nativeVoice ? h.say('Show animal jewelry without necklaces') : h.command('Show animal jewelry without necklaces'));
    assertActualSelection(h, result, [charm, ear]);
    assert.equal(h.store.snapshot().filter, 'all');
    assert.deepEqual(Catalogue.plan(h.store.snapshot().search).excludedCategories, ['necklaces']);
    assert.equal(h.requests.length, reads, 'Warm typed/native discovery needs no model or product HTTP');
    assert.deepEqual(h.cart(), []);
  });

  test('negative category after earlier animal earrings and a manually opened current listing agrees with host through ' + mode, async t => {
    const charm = ordinaryCharm(), h = await fixture(t, { rows: [...catalogue(), charm], nativeVoice });
    for (const phrase of ['What do you have?', 'Do you have butterfly earrings?', 'What animal earrings do you have?']) {
      const result = await h.command(phrase); assert.equal(result.ok, true, phrase + ': ' + result.reply);
    }
    const current = h.rows[0];
    assert.equal((await h.store.execute({ type: 'open', handle: current.handle })).ok, true);
    const helped = await h.command('Help me choose this piece'); assert.equal(helped.ok, true, helped.reply);
    assert.equal(h.store.snapshot().productControls.optionsOpen, true);
    const bag = clone(h.cart());
    if (nativeVoice) await h.startVoice();
    const reads = h.requests.length, result = await (nativeVoice ? h.say('Show animal jewelry without necklaces') : h.command('Show animal jewelry without necklaces'));
    assertActualSelection(h, result);
    assert.equal(h.store.snapshot().filter, 'all');
    assert(result.products.some(p => p.handle === charm.handle), 'The checked ordinary charm stays a charm despite Necklace in its attachment name');
    assert(result.products.every(p => !Catalogue.categoryMatches(p, 'necklaces')), 'Finished necklaces stay excluded');
    assert.equal(h.requests.length, reads);
    assert.deepEqual(h.cart(), bag);
  });

  test('combined necklace and earring request returns both checked types through ' + mode, async t => {
    const ear = product(44201, 'Bunny Stud Earrings', { motif: 'bunny', silver: 15 }), necklace = product(44202, 'Fox Chain Necklace', { type: 'Necklace', motif: 'fox', silver: 20 }), bracelet = product(44203, 'Fox Bracelet', { type: 'Bracelet', motif: 'fox', silver: 10 }), leaf = product(44204, 'Leaf Stud Earrings', { motif: 'leaf', silver: 12 });
    const h = await fixture(t, { rows: [ear, necklace, bracelet, leaf], nativeVoice });
    if (nativeVoice) await h.startVoice();
    const reads = h.requests.length, result = await (nativeVoice ? h.say('Show animal necklaces and earrings') : h.command('Show animal necklaces and earrings'));
    assertActualSelection(h, result, [ear, necklace]);
    assert.equal(h.store.snapshot().filter, 'all');
    assert.deepEqual(Catalogue.plan(h.store.snapshot().search).categories, ['necklaces', 'earrings']);
    assert.equal(h.requests.length, reads);
  });

  for (const initial of ['Show animal jewelry without necklaces', 'Show animal necklaces and earrings']) test('material and budget followups retain the complete checked category scope through ' + mode + ': ' + initial, async t => {
    const ear = product(44211, 'Fox Stud Earrings', { motif: 'fox', silver: 20, gold: 45 }), necklace = product(44212, 'Fox Chain Necklace', { type: 'Necklace', motif: 'fox', silver: 22, gold: 40 }), bracelet = product(44213, 'Fox Bracelet', { type: 'Bracelet', motif: 'fox', silver: 25, gold: 35 });
    const h = await fixture(t, { rows: [ear, necklace, bracelet], nativeVoice }), expected = initial.includes('without') ? [ear, bracelet] : [ear, necklace];
    const first = await h.command(initial); assertActualSelection(h, first, expected);
    if (nativeVoice) await h.startVoice();
    const reads = h.requests.length;
    for (const phrase of ['What about gold filled?', 'Under USD 50 please']) {
      const result = await (nativeVoice ? h.say(phrase) : h.command(phrase)); assertActualSelection(h, result, expected);
      assert.equal(h.store.snapshot().filter, 'all');
      for (const p of result.products) assert(Array.from(p.matchingVariantIds).every(id => h.rows.find(row => row.id === p.id).variants.find(v => v.id === id).options[0].value === '14k Gold Filled'));
    }
    assert.equal(h.requests.length, reads);
    assert.deepEqual(h.cart(), []);
  });

  test('solid gold discovery requires explicit construction on the same available budget-matching variant through ' + mode, async t => {
    const unknown = metalProduct(44301, 'Fox Profile Stud Earrings', '14k Gold'), solid = metalProduct(44302, 'Fox Outline Stud Earrings', '14k Solid Gold'), filled = metalProduct(44303, 'Fox Tiny Stud Earrings', '14k Gold Filled'), plated = metalProduct(44304, 'Fox Cutout Stud Earrings', '14k Gold Plated');
    const h = await fixture(t, { rows: [unknown, solid, filled, plated], nativeVoice });
    if (nativeVoice) await h.startVoice();
    const reads = h.requests.length, result = await (nativeVoice ? h.say('Show solid gold fox earrings under USD 65') : h.command('Show solid gold fox earrings under USD 65'));
    assertActualSelection(h, result, [solid]);
    assert.deepEqual([...result.products[0].matchingVariantIds].sort(), solid.variants.filter(v => v.options[0].value === '14k Solid Gold').map(v => v.id).sort());
    assert.equal(result.products[0].matchingPriceRange.currency, 'USD');
    assert.equal(h.requests.length, reads);
    assert.deepEqual(h.cart(), []);
  });
}

test('native host search shares the strict material rule and preserves literal full option groups', async t => {
  const unknown = metalProduct(44401, 'Fox Profile Stud Earrings', '14k Gold'), solid = metalProduct(44402, 'Fox Outline Stud Earrings', '14k Solid Gold'), filled = metalProduct(44403, 'Fox Tiny Stud Earrings', '14k Gold Filled');
  const h = await fixture(t, { rows: [unknown, solid, filled], mountWidget: false }), reads = h.requests.length;
  const result = await h.store.execute({ type: 'search', query: 'solid gold fox earrings under USD 65', filter: 'earrings' });
  assertActualSelection(h, result, [solid]);
  assert.deepEqual(clone(result.products[0].options), solid.options, 'Filtering variants never drops other purchasable choices from the exact product');
  assert.deepEqual(clone(result.products[0].variants), solid.variants);
  assert.equal(h.requests.length, reads);
});
