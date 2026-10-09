'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const guideAPI = require('../../brites-concierge-shopping-guide.js');
const bridge = require('../../brites-storefront-bridge.js');
const publicOtter = require('./fixtures/otter-guide43.public.json');
const NOW = Date.parse('2026-10-08T22:00:00Z');

// Synthetic public catalogue facts; no shop/provider/account is contacted.
function variant(id, metal, price, extra = {}) {
  return { id: 'gid://shopify/ProductVariant/' + id, title: metal + ' / 18 Inch / None', price, available: true, options: [{ name: 'Metal', value: metal }, { name: 'Necklace Length', value: '18 Inch' }, { name: 'Engraving', value: 'None' }], ...extra };
}
function piece(id, title, type = 'Necklace', extra = {}) {
  const handle = 'synthetic-' + title.toLowerCase().replace(/[^a-z0-9]+/g, '-') + '-' + id;
  return { id: 'gid://shopify/Product/' + id, handle, title, type, url: 'https://britesjewelry.com/products/' + handle, currency: 'USD', image: 'https://cdn.shopify.com/synthetic-' + id + '.jpg', description: 'The charm measures 12.5 mm wide and 15 mm high. This is a synthetic fixture.', checkedAt: NOW, detailState: 'checked', variantsComplete: true, variants: [variant(id * 100 + 1, 'Sterling Silver', 50), variant(id * 100 + 2, '14/20 Gold Filled', 60)], ...extra };
}
function view(p, extra = {}) {
  return { pageKind: 'product', currentHandle: p.handle, focusedHandle: '', productControls: { handle: p.handle, productId: p.id, variantId: null, quantity: 1, selectedOptions: [], reviewReady: false }, ...extra };
}
function selected(p, v, extra = {}) {
  return view(p, { productControls: { handle: p.handle, productId: p.id, variantId: v.id, quantity: 1, selectedOptions: v.options.map(o => ({ ...o })), reviewReady: false, ...extra } });
}
function fixtures(extra = []) {
  return [piece(1, 'Butterfly Necklace'), piece(2, 'Butterfly Hoop Earrings', 'Earrings'), piece(3, 'Butterfly Stud Earrings', 'Earrings'), piece(4, 'Owl Necklace'), piece(5, 'Butterfly Disc Necklace'), piece(6, 'Heart Necklace'), ...extra];
}
function create(products = fixtures(), preferences = {}) {
  return guideAPI.create({ products, preferences, now: () => NOW });
}

test('a current listing prepares literal dimensions, published options and product-bound help immediately', () => {
  const p = fixtures()[0], guide = create(), pack = guide.prepare(view(p));
  assert.equal(pack.current.id, p.id);
  assert.equal(pack.nextStep.action.type, 'options');
  assert.equal(pack.nextStep.action.handle, p.handle);
  assert.equal(pack.nextStep.action.optionName, 'Metal');
  assert.ok(pack.details.some(d => d.text === 'The charm measures 12.5 mm wide and 15 mm high.'));
  assert.ok(pack.details.some(d => d.source === 'published-options' && d.text.includes('Sterling Silver')));
  assert.ok(pack.alternatives.some(row => row.title === 'Butterfly Disc Necklace'));
  assert.deepEqual(pack.matching.map(row => row.title).sort(), ['Butterfly Hoop Earrings', 'Butterfly Stud Earrings']);
  assert.ok(pack.matching.every(row => /sold separately/.test(row.why)));
});
test('product-page identity outranks an unrelated hover and former current listing', () => {
  const products = fixtures(), guide = create(products), current = products[5];
  const pack = guide.prepare(view(current, { focusedHandle: products[0].handle }));
  assert.equal(pack.current.handle, current.handle);
  assert.equal(pack.matching.length, 0);
  assert.ok(pack.nextStep.text.includes(current.title));
});
test('same-listing alternatives use its actual category despite an old search category and reject unrelated motifs', () => {
  const products = fixtures(), guide = create(products, { type: 'earrings' });
  const pack = guide.prepare(view(products[0]));
  assert.ok(pack.alternatives.some(row => row.title === 'Butterfly Disc Necklace'));
  assert.ok(pack.alternatives.every(row => row.title !== 'Heart Necklace'));
  assert.equal(pack.alternatives[0].title, 'Butterfly Disc Necklace');
  assert.ok(pack.alternatives.every(row => !/Earrings/.test(row.title)));
});
test('eligible exact motif alternatives stand alone instead of being padded with broader animal designs', () => {
  const current = piece(1, 'Fox Charm Necklace');
  const exact = piece(2, 'Fox Outline Necklace', 'Necklace', { checkedAt: NOW - 2000, variants: [variant(201, '14/20 Gold Filled', 60)] });
  const broad = piece(3, 'Butterfly Necklace', 'Necklace', { variants: [variant(301, '14/20 Gold Filled', 40)] });
  const guide = create([current, exact, broad]);
  const context = selected(current, current.variants[1]);
  const rows = guide.prepare(context).alternatives;
  assert.deepEqual(rows.map(row => row.id), [exact.id]);
  assert.equal(rows[0].variantId, exact.variants[0].id);
  assert.equal(rows[0].price, 60);
  assert.equal(rows[0].currency, 'USD');
  assert.equal(rows[0].checkedAt, exact.checkedAt);
  assert.match(rows[0].why, /Shares the fox motif/);
  assert.equal(Object.hasOwn(rows[0], 'exactMotif'), false);
  assert.deepEqual(guide.suggest({ context, message: "I'm not sure about this one", trigger: 'uncertain' }).suggestion.products.map(row => row.id), [exact.id]);
});
test('broader theme fallback remains available only when no exact motif satisfies checked stock and preferences', () => {
  const current = piece(1, 'Fox Charm Necklace');
  const broad = piece(3, 'Butterfly Necklace', 'Necklace', { variants: [variant(301, '14/20 Gold Filled', 55)] });
  const unavailable = [
    { variants: [variant(201, '14/20 Gold Filled', 40, { available: false }), variant(202, 'Sterling Silver', 30)] },
    { variants: [variant(201, '14/20 Gold Filled', 61)] },
    { variants: [variant(201, '14/20 Gold Filled', 40)], currency: 'CAD' },
    { variants: [variant(201, '14/20 Gold Filled', 40)], recommendationHold: true }
  ];
  for (const extra of unavailable) {
    const exact = piece(2, 'Fox Outline Necklace', 'Necklace', extra);
    const guide = create([current, exact, broad], { metal: 'gold filled', budget: 60, budgetCurrency: 'USD' });
    const rows = guide.prepare(selected(current, current.variants[1])).alternatives;
    assert.deepEqual(rows.map(row => row.id), [broad.id]);
    assert.equal(rows[0].variantId, broad.variants[0].id);
    assert.equal(rows[0].price, 55);
    assert.equal(rows[0].currency, 'USD');
    assert.match(rows[0].why, /Another animal necklace design/);
    assert.doesNotMatch(rows[0].why, /Shares the fox/);
  }
});
test('bag and checkout never offer recommendations for a former listing', () => {
  const p = fixtures()[0], guide = create();
  const bag = guide.prepare({ ...view(p), pageKind: 'bag', bagControls: { lines: [{ productId: p.id }] } });
  assert.equal(bag.current, null);
  assert.deepEqual(bag.alternatives, []);
  assert.deepEqual(bag.nextStep.action, { type: 'checkout' });
  assert.equal(guide.prepare({ ...view(p), pageKind: 'checkout' }).nextStep, null);
});
test('availability, material and item budget must coexist on one exact variant', () => {
  const current = piece(1, 'Butterfly Necklace'), deceptive = piece(2, 'Butterfly Earrings', 'Earrings', { variants: [variant(201, 'Sterling Silver', 30), variant(202, '14/20 Gold Filled', 150)] }), valid = piece(3, 'Butterfly Stud Earrings', 'Earrings', { variants: [variant(301, '14/20 Gold Filled', 59)] });
  const guide = create([current, deceptive, valid], { metal: 'gold filled', budget: 60, budgetCurrency: 'USD' });
  const pack = guide.prepare(view(current));
  assert.deepEqual(pack.matching.map(row => row.id), [valid.id]);
  assert.equal(pack.matching[0].variantId, valid.variants[0].id);
  assert.match(pack.matching[0].why, /Within your USD item budget/);
  assert.equal(pack.optionSuggestions.find(o => o.name === 'Metal').value, '14/20 Gold Filled');
});
test('gold filled and solid gold remain distinct preferences; rose finish is not a flower motif', () => {
  const current = piece(1, 'Butterfly Necklace'), filled = piece(2, 'Butterfly Earrings', 'Earrings', { variants: [variant(201, '14/20 Gold Filled', 60)] }), solid = piece(3, 'Butterfly Solid Gold Earrings', 'Earrings', { variants: [variant(301, '14K Solid Gold', 500)] }), rose = piece(4, 'Rose Gold Circle Earrings', 'Earrings'), unrelated = piece(5, 'Rose Gold Heart Necklace');
  const guide = create([current, filled, solid, rose, unrelated], { metal: 'solid gold' });
  assert.deepEqual(guide.prepare(view(current)).matching.map(row => row.id), [solid.id]);
  guide.setPreferences({ metal: 'gold-filled' });
  assert.deepEqual(guide.prepare(view(current)).matching.map(row => row.id), [filled.id]);
  guide.setPreferences({});
  assert.deepEqual(guide.prepare(view(rose)).matching, []);
});
test('the actual chosen metal influences a pairing without overriding an explicit preference', () => {
  const rows = fixtures(), p = rows[0], guide = create(rows);
  assert.ok(guide.prepare(selected(p, p.variants[1])).matching.every(row => row.variantTitle.includes('Gold Filled')));
  guide.setPreferences({ metal: 'silver' });
  assert.ok(guide.prepare(selected(p, p.variants[1])).matching.every(row => row.variantTitle.includes('Sterling Silver')));
});
test('foreign-currency budgets are not compared or converted', () => {
  const p = fixtures()[0], guide = create(fixtures(), { metal: 'silver', budget: 100, budgetCurrency: 'CAD' });
  const pack = guide.prepare(view(p));
  assert.deepEqual(pack.matching, []);
  assert.deepEqual(pack.alternatives, []);
  assert.ok(pack.warnings.some(w => /priced in USD/.test(w) && /CAD/.test(w)));
  assert.ok(pack.details.some(d => d.kind === 'price' && /USD/.test(d.text)));
});
test('upper and lower item caps are inclusive and zero-price items stay valid', () => {
  const p = piece(1, 'Butterfly Necklace'), lo = piece(2, 'Butterfly Earrings', 'Earrings', { variants: [variant(201, 'Sterling Silver', 40)] }), hi = piece(3, 'Butterfly Stud Earrings', 'Earrings', { variants: [variant(301, 'Sterling Silver', 60)] }), high = piece(4, 'Butterfly Hoop Earrings', 'Earrings', { variants: [variant(401, 'Sterling Silver', 60.01)] });
  const guide = create([p, lo, hi, high], { budget: { min: 40, max: 60, currency: 'USD' } });
  assert.deepEqual(guide.prepare(view(p)).matching.map(row => row.id).sort(), [lo.id, hi.id].sort());
  const free = { ...lo, variants: [variant(201, 'Sterling Silver', 0)] };
  guide.updateProducts([p, free]); guide.setPreferences({ budget: 0, budgetCurrency: 'USD' });
  assert.equal(guide.prepare(view(p)).matching[0].price, 0);
});
test('published unavailability, unknown stock, identity holds, parts and services exclude a recommendation', () => {
  const current = piece(1, 'Butterfly Necklace'), rows = [
    piece(2, 'Butterfly Earrings', 'Earrings', { variants: [variant(201, 'Sterling Silver', 40, { available: false })] }),
    piece(3, 'Butterfly Stud Earrings', 'Earrings', { variants: [variant(301, 'Sterling Silver', 40, { availabilityKnown: false })] }),
    piece(4, 'Butterfly Hoop Earrings', 'Earrings', { recommendationHold: true }),
    piece(5, 'Butterfly Earrings', 'Earrings', { cartHold: true }),
    piece(6, 'Butterfly Earrings', 'Earrings', { partsOnly: true }),
    piece(7, 'Butterfly Design Fee Earrings', 'Earrings')
  ];
  assert.deepEqual(create([current, ...rows]).prepare(view(current)).matching, []);
});
test('ordinary published charms retain motif pairings and exact option help for every style and engraving variant', () => {
  const metals = ['Sterling Silver', '14/20 Gold Filled'], styles = ['Necklace Charm', 'Bracelet Charm'], engraving = ['No', 'Yes'];
  let nextId = 50101;
  const variants = metals.flatMap((metal, m) => styles.flatMap((style, s) => engraving.map((value, e) => ({ id: 'gid://shopify/ProductVariant/' + nextId++, title: metal + ' / ' + style + ' / ' + value, price: 30 + m * 10 + s * 5 + e * 12, available: true, options: [{ name: 'Metal Choice', value: metal }, { name: 'Charm Type', value: style }, { name: 'Engraving', value }] }))));
  const current = piece(501, 'Sea Otter Necklace Charm', 'Charms', { partsOnly: true, variants });
  const stud = piece(502, 'Sea Otter Charm Stud Earrings', 'Earrings');
  const other = piece(503, 'Eating Otter Stud Earrings', 'Earrings');
  const charm = piece(504, 'Sea Otter Outline Necklace Charm', 'Charms', { partsOnly: true });
  const invalid = [piece(505, 'Sea Otter Custom Components', 'Charms', { partsOnly: true }), piece(506, 'Sea Otter Stud Earrings', 'Earrings', { recommendationHold: true })];
  const guide = create([current, stud, other, charm, ...invalid], { interests: ['animals'] });
  guide.dismiss();
  const empty = guide.prepare(view(current));
  assert.equal(empty.nextStep.action.optionName, 'Metal Choice');
  assert.ok(empty.optionSuggestions.some(row => row.name === 'Metal Choice'));
  assert.deepEqual(empty.alternatives.map(row => row.id), [charm.id], 'A necklace charm keeps the published charm category');
  for (const variant of variants) {
    const context = selected(current, variant, { selectionStatus: 'ready', quantity: 3 }), before = JSON.stringify(context);
    const result = guide.suggest({ context, message: 'Show me matching earrings' });
    assert.deepEqual(result.suggestion.products.map(row => row.id).sort(), [stud.id, other.id].sort());
    assert.ok(result.suggestion.products.every(row => row.variantTitle.includes(variant.options[0].value)));
    assert.ok(result.suggestion.products.every(row => /Shares the otter motif/.test(row.why) && /sold separately/.test(row.why)));
    assert.ok(result.suggestion.products.every(row => row.action.type === 'open'));
    assert.equal(result.pack.current.id, current.id);
    if (variant.options[2].value === 'Yes') { assert.equal(result.pack.nextStep.kind, 'options'); assert.match(result.pack.nextStep.text, /personalization requirements/); }
    else { assert.equal(result.pack.nextStep.kind, 'add'); assert.equal(result.pack.nextStep.subtotal, variant.price * 3); }
    assert.equal(JSON.stringify(context), before);
  }
  for (const extra of [{ cartHold: true }, { recommendationHold: true }, { title: 'Sea Otter Custom Charm' }, { type: 'Components' }]) {
    const blocked = { ...current, ...extra }, blockedGuide = create([blocked, stud]);
    assert.deepEqual(blockedGuide.prepare(view(blocked)).matching, []);
    assert.equal(blockedGuide.prepare(view(blocked)).nextStep, null);
  }
  guide.updateProducts([current]);
  assert.match(guide.suggest({ context: view(current), message: 'Show me matching earrings' }).reply, /haven’t found checked matching earrings/);
});
test('recorded real otter product facts override an earlier positive butterfly theme only within the exact current motif', () => {
  const preferences = { interests: ['butterfly'], type: 'earrings', metal: 'silver', budget: 48, budgetCurrency: 'USD' }, before = JSON.stringify(preferences);
  const current = publicOtter.products[0], context = view(current);
  const guide = guideAPI.create({ products: publicOtter.products, preferences, now: () => publicOtter.capturedAt });
  guide.dismiss();
  const answer = guide.suggest({ context, message: 'What earrings would go with this?' });
  assert.deepEqual(answer.suggestion.products.map(row => [row.handle, row.price, row.currency]), [['eating-otter-stud-earrings', 45, 'USD'], ['sea-otter-charm-stud-earrings-1', 48, 'USD']]);
  assert.ok(answer.suggestion.products.every(row => /Shares the otter motif/.test(row.why) && /sold separately/.test(row.why)));
  assert.equal(JSON.stringify(preferences), before);
  assert.deepEqual(guide.setPreferences(preferences).themes, ['butterfly']);
  assert.equal(answer.pack.current.id, current.id);
  assert.equal(context.productControls.variantId, null);
  assert.deepEqual(context.productControls.selectedOptions, []);
  const fox = piece(701, 'Fox Necklace'), exact = piece(702, 'Fox Outline Necklace'), butterfly = piece(703, 'Butterfly Necklace'), owl = piece(704, 'Owl Necklace');
  const alternatives = create([fox, exact, butterfly, owl], { interests: ['butterfly'] });
  assert.deepEqual(alternatives.prepare(view(fox)).alternatives.map(row => row.id), [exact.id]);
  alternatives.updateProducts([fox, butterfly, owl]);
  assert.deepEqual(alternatives.prepare(view(fox)).alternatives.map(row => row.id), [butterfly.id], 'A broader fallback still respects the positive theme');
});
test('real-fact stale-theme recovery retains every variant, currency, exclusion, dismissal, freshness and identity guard', () => {
  const current = publicOtter.products[0], context = view(current), [sea, eating] = publicOtter.products.slice(1);
  const cases = [
    { preferences: { budget: 44, budgetCurrency: 'USD' }, expected: [] },
    { preferences: { budget: 45, budgetCurrency: 'USD' }, expected: [eating.handle] },
    { preferences: { budget: 100, budgetCurrency: 'CAD' }, expected: [] },
    { preferences: { metal: 'gold filled', budget: 50, budgetCurrency: 'USD' }, expected: [eating.handle], variantId: eating.variants[1].id },
    { preferences: { excludedInterests: ['otter'] }, expected: [] },
    { preferences: { excludedTypes: ['earrings'] }, expected: [] },
    { preferences: { metal: 'silver', excludedMetals: ['silver'] }, expected: [] },
    { change: rows => { rows[0].cartHold = true; }, expected: [] },
    { change: rows => { rows[0].recommendationHold = true; }, expected: [] },
    { change: rows => { rows[1].recommendationHold = true; }, expected: [eating.handle] },
    { change: rows => { rows[1].cartHold = true; }, expected: [eating.handle] },
    { change: rows => { rows[1].variants.forEach(v => { v.available = false; }); }, expected: [eating.handle] },
    { change: rows => { rows[1].variants.forEach(v => { v.availabilityKnown = false; }); }, expected: [eating.handle] },
    { change: rows => { rows[1].variantsComplete = false; }, expected: [eating.handle] },
    { change: rows => { rows[1].checkedAt = publicOtter.capturedAt - 300000; }, expected: [eating.handle] },
    { change: rows => { rows[1].checkedAt = publicOtter.capturedAt + 60001; }, expected: [eating.handle] },
    { change: rows => { rows[1].url = 'https://britesjewelry.com/products/another-product'; }, expected: [eating.handle] },
    { change: rows => { rows[1].variants[1].id = rows[1].variants[0].id; }, expected: [eating.handle] },
    { dismiss: sea.handle, expected: [eating.handle] },
    { change: rows => { rows[1].currency = 'CAD'; }, preferences: { budget: 100, budgetCurrency: 'USD' }, expected: [eating.handle] }
  ];
  for (const row of cases) {
    const products = JSON.parse(JSON.stringify(publicOtter.products)), preferences = { interests: ['butterfly'], ...row.preferences };
    row.change?.(products);
    const guide = guideAPI.create({ products, preferences, now: () => publicOtter.capturedAt });
    if (row.dismiss) guide.dismiss({ handle: row.dismiss });
    const answer = guide.suggest({ context, message: 'Show me matching earrings' });
    assert.deepEqual(answer.suggestion.products.map(product => product.handle).sort(), row.expected.sort());
    if (row.variantId) assert.equal(answer.suggestion.products[0].variantId, row.variantId);
  }
});
test('expired, future-dated and incomplete catalogue cannot authorize readiness or availability guidance', () => {
  const p = piece(1, 'Butterfly Necklace'), stale = piece(2, 'Butterfly Earrings', 'Earrings', { checkedAt: NOW - 300000 }), future = piece(3, 'Butterfly Stud Earrings', 'Earrings', { checkedAt: NOW + 60001 }), incomplete = piece(4, 'Butterfly Hoop Earrings', 'Earrings', { variantsComplete: false });
  const guide = create([p, stale, future, incomplete]);
  assert.deepEqual(guide.prepare(view(p)).matching, []);
  const pack = guide.prepare(view(stale));
  assert.equal(pack.nextStep, null);
  assert.deepEqual(pack.optionSuggestions, []);
  assert.ok(pack.warnings.some(w => /fresh product check/.test(w)));
});
test('expiry is re-evaluated while the same page remains open', () => {
  let time = NOW;
  const rows = fixtures(), guide = guideAPI.create({ products: rows, now: () => time });
  assert.ok(guide.prepare(view(rows[0])).matching.length);
  time += 300000;
  assert.deepEqual(guide.prepare(view(rows[0])).matching, []);
});
test('body copy cross-sells do not become product motif evidence or invented symbolism', () => {
  const p = piece(1, 'Butterfly Necklace'), misleading = piece(2, 'Heart Earrings', 'Earrings', { description: 'Wear these with a butterfly necklace. Butterflies symbolize renewal in every culture.' });
  const pack = create([p, misleading]).prepare(view(p));
  assert.deepEqual(pack.matching, []);
  assert.ok(pack.details.every(d => !/symboliz|renewal|culture/.test(d.text)));
});
test('an obsolete motif in a legacy handle cannot override the currently named design', () => {
  const p = piece(1, 'Butterfly Necklace'), renamed = { ...piece(2, 'Butterfly Earrings', 'Earrings'), title: 'Cat Earrings' }, actual = piece(3, 'Butterfly Stud Earrings', 'Earrings');
  assert.deepEqual(create([p, renamed, actual]).prepare(view(p)).matching.map(row => row.id), [actual.id]);
});
test('stored positive themes guide fallback designs while exact current motifs retain all explicit exclusions', () => {
  const rows = fixtures(), guide = create(rows, { interests: ['animal'], excludedInterests: ['owl'] });
  const pack = guide.prepare(view(rows[0]));
  assert.ok(pack.alternatives.some(row => row.title === 'Butterfly Disc Necklace'));
  assert.ok(pack.alternatives.every(row => row.title !== 'Owl Necklace' && row.title !== 'Heart Necklace'));
  guide.setPreferences({ interests: ['bunny'] });
  assert.deepEqual(guide.prepare(view(rows[0])).matching.map(row => row.id).sort(), [rows[1].id, rows[2].id].sort());
  guide.setPreferences({ interests: ['bunny'], excludedInterests: ['butterfly'] });
  assert.deepEqual(guide.prepare(view(rows[0])).matching, []);
});
test('help choosing compares two priced actual materials without selecting either or guessing a length', () => {
  const p = piece(1, 'Butterfly Necklace', 'Necklace', { variants: [variant(101, 'Sterling Silver', 50), variant(102, '14/20 Gold Filled', 60), variant(103, 'Sterling Silver', 55, { title: 'Sterling Silver / 20 Inch / None', options: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Necklace Length', value: '20 Inch' }, { name: 'Engraving', value: 'None' }] })] });
  const guide = create([p]), pack = guide.prepare(view(p));
  const metals = pack.optionSuggestions.filter(o => o.name === 'Metal');
  assert.equal(metals.length, 2);
  assert.deepEqual(metals.map(o => o.value), ['Sterling Silver', '14/20 Gold Filled']);
  assert.deepEqual(metals.map(o => o.price), [50, 60]);
  assert.ok(metals.every(o => o.currency === 'USD' && o.priceIsFrom && o.advice.sources.length));
  assert.ok(!pack.optionSuggestions.some(o => o.name === 'Necklace Length'));
  guide.setPreferences({ metal: 'silver', length: '20 Inch' });
  const updated = guide.prepare(view(p));
  assert.equal(updated.optionSuggestions.find(o => o.name === 'Metal').value, 'Sterling Silver');
  assert.equal(updated.optionSuggestions.find(o => o.name === 'Necklace Length').value, '20 Inch');
});
test('an unlabelled budget is anchored to the current listing currency instead of mixed-market numeric prices', () => {
  const p = piece(1, 'Butterfly Necklace'), usd = piece(2, 'Butterfly Earrings', 'Earrings'), cad = piece(3, 'Butterfly Stud Earrings', 'Earrings', { currency: 'CAD' });
  const pack = create([p, usd, cad], { budget: 60 }).prepare(view(p));
  assert.deepEqual(pack.matching.map(row => row.id), [usd.id]);
  assert.ok(pack.warnings.some(w => /interpreted in USD/.test(w)));
  assert.match(pack.matching[0].why, /USD item budget/);
});
test('shared catalogue vocabulary covers specific octopus designs and broad animal and ocean preferences', () => {
  const p = piece(1, 'Octopus Necklace'), matched = piece(2, 'Octopus Earrings', 'Earrings'), shell = piece(3, 'Seashell Necklace');
  const guide = create([p, matched, shell], { interests: ['animals'] });
  assert.equal(guide.prepare(view(p)).matching[0].id, matched.id);
  guide.setPreferences({ interests: ['ocean'] });
  assert.equal(guide.prepare(view(p)).matching[0].id, matched.id);
});
test('ready selected controls propose an exact quantity subtotal and direct add without executing it', () => {
  const p = fixtures()[0], guide = create();
  const pack = guide.prepare(selected(p, p.variants[0], { selectionStatus: 'ready', quantity: 3 }));
  assert.deepEqual(pack.nextStep.action, { type: 'add', handle: p.handle, variantId: p.variants[0].id });
  assert.equal(pack.nextStep.subtotal, 150);
  assert.equal(pack.nextStep.quantity, 3);
  assert.equal(pack.nextStep.currency, 'USD');
  assert.equal(pack.nextStep.requiresCustomerClick, true);
  assert.match(pack.nextStep.text, /item subtotal.*150/);
});
test('a new stated metal or length keeps other selected choices and suggests the compatible actual change', () => {
  const p = piece(1, 'Butterfly Necklace', 'Necklace', { variants: [variant(101, 'Sterling Silver', 50), variant(102, '14/20 Gold Filled', 60), variant(103, 'Sterling Silver', 55, { title: 'Sterling Silver / 20 Inch / None', options: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Necklace Length', value: '20 Inch' }, { name: 'Engraving', value: 'None' }] })] });
  const guide = create([p]), context = selected(p, p.variants[1], { selectionStatus: 'ready' });
  guide.setPreferences({ metal: 'silver' });
  const metal = guide.prepare(context);
  assert.equal(metal.nextStep.action.type, 'options');
  assert.equal(metal.optionSuggestions.find(o => o.name === 'Metal').value, 'Sterling Silver');
  assert.ok(!metal.optionSuggestions.some(o => o.name === 'Necklace Length'));
  guide.setPreferences({ metal: 'silver', length: '20 Inch' });
  const changed = guide.prepare(context);
  assert.equal(changed.nextStep.action.type, 'options');
  assert.equal(changed.optionSuggestions.find(o => o.name === 'Necklace Length').value, '20 Inch');
  assert.ok(changed.optionSuggestions.every(o => o.price === 55));
});
test('complete selected choices prepare review without adding; review-ready keeps the separate customer click', () => {
  const p = fixtures()[0], guide = create();
  const pack = guide.prepare(selected(p, p.variants[0]));
  assert.deepEqual(pack.nextStep.action, { type: 'review-add', handle: p.handle, variantId: p.variants[0].id });
  assert.equal(pack.nextStep.requiresCustomerClick, true);
  const ready = guide.prepare(selected(p, p.variants[0], { reviewReady: true }));
  assert.equal(ready.nextStep.action, null);
  assert.match(ready.nextStep.text, /visible confirmation button/);
});
test('a mismatched selected variant and literal choices never invent a selected price or review', () => {
  const p = fixtures()[0], guide = create(), context = selected(p, p.variants[0]);
  context.productControls.selectedOptions[0].value = '14/20 Gold Filled';
  const pack = guide.prepare(context);
  assert.ok(!pack.details.some(d => d.label === 'Selected price'));
  assert.equal(pack.nextStep.action.type, 'options');
});
test('personalization asks to review actual requirements and does not prepare an unverified cart action', () => {
  const custom = variant(101, 'Sterling Silver', 65, { title: 'Sterling Silver / 18 Inch / Personalized', options: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Necklace Length', value: '18 Inch' }, { name: 'Engraving', value: 'Personalized' }] }), p = piece(1, 'Butterfly Necklace', 'Necklace', { variants: [custom] });
  const pack = create([p]).prepare(selected(p, custom));
  assert.equal(pack.nextStep.action.type, 'options');
  assert.match(pack.nextStep.text, /personalization requirements/);
});
test('suggestions are limited, usable by the existing adapter and do not mutate input', () => {
  const rows = fixtures(), before = JSON.stringify(rows), guide = create(rows), pack = guide.prepare(view(rows[0]));
  for (const action of [...pack.matching.map(row => row.action), ...pack.optionSuggestions.map(row => row.action), pack.nextStep.action]) assert.deepEqual(bridge.validateAction(action), action);
  assert.equal(JSON.stringify(rows), before);
  assert.ok(Object.isFrozen(pack) && Object.isFrozen(pack.matching));
  const answer = guide.suggest({ context: view(rows[0]), message: 'What matches this?' });
  assert.equal(answer.suggestion.kind, 'matching');
  assert.ok(answer.suggestion.products.length <= 2);
});
test('prepared background work never navigates, requests information or writes a cart', () => {
  let called = false;
  const guide = guideAPI.create({ products: fixtures(), now: () => NOW, execute() { called = true; }, fetch() { called = true; } });
  guide.prepare(view(fixtures()[0]));
  guide.suggest({ context: view(fixtures()[0]), trigger: 'product-open' });
  assert.equal(called, false);
});
test('busy, speaking, loading and hidden states retain prepared information without interrupting', () => {
  const rows = fixtures(), guide = create(rows);
  for (const flag of ['busy', 'speaking', 'loading', 'hidden']) {
    const answer = guide.suggest({ context: view(rows[0], { [flag]: true }), message: 'What matches?' });
    assert.ok(answer.pack.matching.length);
    assert.equal(answer.suggestion, null);
  }
});
test('passive guidance respects cooldown and avoids repeating shown designs; explicit requests stay usable', () => {
  let time = NOW;
  const rows = fixtures(), guide = guideAPI.create({ products: rows, now: () => time, promptCooldownMs: 1000 });
  const first = guide.suggest({ context: view(rows[0]), trigger: 'matching' });
  assert.ok(first.suggestion);
  assert.equal(guide.markShown(first.suggestion), true);
  assert.equal(guide.suggest({ context: view(rows[0]), trigger: 'matching' }).suggestion, null);
  time += 1001;
  assert.equal(guide.suggest({ context: view(rows[0]), trigger: 'matching' }).suggestion, null);
  assert.ok(guide.suggest({ context: view(rows[0]), message: 'What matches?' }).suggestion);
});
test('a shopper who says not sure immediately after matching suggestions receives useful alternatives despite passive cooldown', () => {
  const rows = fixtures(), guide = create(rows), context = selected(rows[0], rows[0].variants[1]);
  const matching = guide.suggest({ context, message: 'What earrings would go with this?' });
  guide.markShown(matching.suggestion);
  const unsure = guide.suggest({ context, message: "I'm not sure about this one", trigger: 'uncertain' });
  assert.ok(unsure.suggestion);
  assert.equal(unsure.suggestion.kind, 'alternatives');
  assert.equal(unsure.suggestion.products[0].title, 'Butterfly Disc Necklace');
  assert.ok(unsure.suggestion.products.every(row => /Gold Filled/.test(row.variantTitle)));
});
for (const phrase of ['More like that', 'More like this']) test(phrase + ' explicitly requests current-design alternatives despite cooldown and passive session mute', () => {
  const current = piece(1, 'Fox Charm Necklace'), exact = piece(2, 'Fox Outline Necklace', 'Necklace', { variants: [variant(201, 'Sterling Silver', 20), variant(202, '14/20 Gold Filled', 55)] });
  const matched = piece(3, 'Fox Hoop Earrings', 'Earrings'), broad = piece(4, 'Butterfly Necklace');
  let called = false;
  const guide = guideAPI.create({ products: [current, exact, matched, broad], now: () => NOW, preferences: { budget: 60, budgetCurrency: 'USD' }, execute() { called = true; }, fetch() { called = true; } });
  const context = selected(current, current.variants[1]), before = JSON.stringify(context);
  const previous = guide.suggest({ context, message: 'What earrings match?' });
  guide.markShown(previous.suggestion);
  guide.dismiss();
  const answer = guide.suggest({ context, message: phrase });
  assert.equal(answer.suggestion.kind, 'alternatives');
  assert.deepEqual(answer.suggestion.products.map(row => row.id), [exact.id]);
  assert.equal(answer.suggestion.products[0].variantId, exact.variants[1].id);
  assert.equal(answer.suggestion.products[0].price, 55);
  assert.equal(answer.suggestion.products[0].currency, 'USD');
  assert.equal(answer.pack.current.id, current.id);
  assert.equal(JSON.stringify(context), before);
  assert.equal(called, false);
});
test('not those excludes literal suggested identities and returns honest no-more help when direct motif choices are exhausted', () => {
  const current = piece(1, 'Fox Charm Necklace');
  const exact = [piece(2, 'Fox Outline Necklace'), piece(3, 'Fox Disc Necklace'), piece(4, 'Fox Silhouette Necklace')];
  const broad = piece(5, 'Butterfly Necklace', 'Necklace', { variants: [variant(501, '14/20 Gold Filled', 40)] });
  const wrongMetal = piece(6, 'Fox Pendant Necklace', 'Necklace', { variants: [variant(601, 'Sterling Silver', 20)] });
  const guide = create([current, ...exact, broad, wrongMetal], { budget: 60, budgetCurrency: 'USD' });
  const context = selected(current, current.variants[1]), before = JSON.stringify(context);
  const first = guide.suggest({ context, message: 'More like that' });
  assert.equal(first.suggestion.products.length, 2);
  const rejectedIds = new Set(first.suggestion.products.map(row => row.id));
  first.suggestion.products.forEach(row => guide.dismiss({ handle: row.handle }));
  guide.markShown(first.suggestion); guide.dismiss();
  const next = guide.suggest({ context, message: 'Not those' });
  assert.equal(next.suggestion.kind, 'alternatives');
  assert.equal(next.suggestion.products.length, 1);
  assert.ok(!rejectedIds.has(next.suggestion.products[0].id));
  assert.ok(exact.some(row => row.id === next.suggestion.products[0].id));
  assert.match(next.suggestion.products[0].variantTitle, /Gold Filled/);
  next.suggestion.products.forEach(row => guide.dismiss({ handle: row.handle }));
  const exhausted = guide.suggest({ context, message: 'Not those' });
  assert.deepEqual(exhausted.suggestion.products, []);
  assert.deepEqual(exhausted.pack.alternatives, []);
  assert.match(exhausted.reply, /After excluding those pieces, I haven’t found another checked alternative/);
  assert.match(exhausted.reply, /keep this selection or adjust a preference/);
  assert.equal(exhausted.pack.current.id, current.id);
  assert.equal(exhausted.pack.nextStep.action.variantId, current.variants[1].id);
  assert.equal(JSON.stringify(context), before);
  assert.deepEqual(guide.suggest({ context, message: 'More like that' }).suggestion.products, []);
  assert.deepEqual(guide.prepare(context, { passive: true }).alternatives, []);
});
test('dismissal survives the session, mutes passive help and still lets the shopper ask again', () => {
  const saved = new Map(), storage = { getItem: name => saved.get(name) || null, setItem: (name, value) => saved.set(name, value) }, rows = fixtures();
  const guide = guideAPI.create({ products: rows, now: () => NOW, storage });
  guide.dismiss({ kind: 'matching' });
  assert.equal(guide.suggest({ context: view(rows[0]), trigger: 'matching' }).suggestion, null);
  assert.ok(guide.suggest({ context: view(rows[0]), message: 'What matches?' }).suggestion);
  guide.dismiss();
  const restored = guideAPI.create({ products: rows, now: () => NOW, storage });
  assert.equal(restored.suggest({ context: view(rows[0]), trigger: 'product-open' }).suggestion, null);
  assert.ok(restored.suggest({ context: view(rows[0]), message: 'Show similar alternatives' }).suggestion);
  restored.reset();
  assert.ok(restored.suggest({ context: view(rows[0]), trigger: 'product-open' }).suggestion);
  assert.ok([...saved.values()].every(value => !/customer|email|address|transcript/.test(value)));
});
test('already-bagged designs and specifically dismissed products are excluded from further recommendations', () => {
  const rows = fixtures(), guide = create(rows);
  guide.dismiss({ handle: rows[1].handle });
  const pack = guide.prepare(view(rows[0], { bagControls: { lines: [{ productId: rows[2].id }] } }));
  assert.deepEqual(pack.matching, []);
});
test('duplicate identity, unsafe URLs and malformed option sets fail closed', () => {
  const rows = fixtures(), p = rows[0], duplicated = { ...rows[1], id: rows[2].id }, hostile = piece(9, 'Butterfly Earrings', 'Earrings', { url: 'https://evil.example/products/butterfly' }), broken = piece(10, 'Butterfly Earrings', 'Earrings', { options: [{ name: 'Metal', values: ['Sterling Silver'] }] });
  const guide = create([p, rows[1], rows[2], duplicated, hostile, broken]);
  assert.deepEqual(guide.prepare(view(p)).matching, []);
  guide.updateProducts([p, rows[1]]);
  assert.equal(guide.prepare(view(p)).matching.length, 1);
});
test('cheaper means a strictly lower checked unit price in the chosen exact material and native currency', () => {
  const p = piece(1, 'Butterfly Necklace'), lower = piece(2, 'Butterfly Mini Necklace', 'Necklace', { variants: [variant(201, '14/20 Gold Filled', 59)] }), equal = piece(3, 'Butterfly Disc Necklace', 'Necklace', { variants: [variant(301, '14/20 Gold Filled', 60)] }), foreign = piece(4, 'Butterfly Small Necklace', 'Necklace', { currency: 'CAD', variants: [variant(401, '14/20 Gold Filled', 30)] }), deceptive = piece(5, 'Butterfly Necklace', 'Necklace', { variants: [variant(501, 'Sterling Silver', 20), variant(502, '14/20 Gold Filled', 150)] });
  const guide = create([p, lower, equal, foreign, deceptive]), result = guide.suggest({ context: selected(p, p.variants[1], { quantity: 3 }), message: 'Anything cheaper?' });
  assert.equal(result.suggestion.kind, 'alternatives');
  assert.deepEqual(result.suggestion.products.map(row => row.id), [lower.id]);
  assert.equal(result.pack.comparison.reference.price, 60);
  assert.equal(result.pack.comparison.reference.currency, 'USD');
  assert.equal(result.suggestion.products[0].variantId, lower.variants[0].id);
  assert.equal(result.suggestion.products[0].price, 59);
  assert.match(result.suggestion.products[0].why, /unit price.*shipping and taxes/);
});
test('a deliberate changed material preference may guide a cheaper alternative without pretending it is the same construction', () => {
  const p = piece(1, 'Butterfly Necklace'), silver = piece(2, 'Butterfly Mini Necklace', 'Necklace', { variants: [variant(201, 'Sterling Silver', 45)] }), guide = create([p, silver], { metal: 'silver' });
  const answer = guide.suggest({ context: selected(p, p.variants[1]), message: 'Anything less expensive?' });
  assert.equal(answer.suggestion.products[0].id, silver.id);
  assert.match(answer.suggestion.text, /requested material/);
});
test('unselected-price comparisons use an explicitly qualified checked from-price', () => {
  const p = piece(1, 'Butterfly Necklace'), lower = piece(2, 'Butterfly Mini Necklace', 'Necklace', { variants: [variant(201, 'Sterling Silver', 49)] }), guide = create([p, lower]);
  const answer = guide.suggest({ context: view(p), message: 'Do you have a lower priced one?' });
  assert.equal(answer.pack.comparison.reference.source, 'available-variant-from-price');
  assert.match(answer.suggestion.products[0].why, /available from-price/);
});
test('unknown current material yields an honest comparative recovery instead of a fabricated cheaper claim', () => {
  const p = piece(1, 'Butterfly Necklace', 'Necklace', { variants: [{ id: 'gid://shopify/ProductVariant/101', title: 'Default Title', price: 100, available: true, options: [] }] }), candidate = piece(2, 'Butterfly Mini Necklace');
  const answer = create([p, candidate]).suggest({ context: view(p), message: 'Something cheaper?' });
  assert.equal(answer.pack.comparison.status, 'missing-reference');
  assert.deepEqual(answer.suggestion.products, []);
  assert.match(answer.suggestion.text, /confirmed current material and price/);
});
test('matching requests respect a specifically named product category', () => {
  const p = piece(1, 'Butterfly Earrings', 'Earrings'), necklace = piece(2, 'Butterfly Necklace'), charm = piece(3, 'Butterfly Charm', 'Charm'), guide = create([p, necklace, charm]);
  const answer = guide.suggest({ context: view(p), message: 'A matching necklace?' });
  assert.deepEqual(answer.suggestion.products.map(row => row.id), [necklace.id]);
  assert.deepEqual(answer.pack.comparison.requestedCategories, ['necklace']);
  const absent = guide.suggest({ context: view(p), message: 'Any matching bracelets?' });
  assert.deepEqual(absent.suggestion.products, []);
  assert.match(absent.suggestion.text, /matching bracelet/);
});
test('would go with recognizes matching flow, preserves a specifically requested category and revives explicit dismissed help', () => {
  const p = piece(1, 'Butterfly Necklace'), earrings = piece(2, 'Butterfly Earrings', 'Earrings'), charm = piece(3, 'Butterfly Charm', 'Charm'), guide = create([p, earrings, charm]);
  guide.dismiss();
  const answer = guide.suggest({ context: view(p), message: 'What earrings would go with this?' });
  assert.equal(answer.suggestion.kind, 'matching');
  assert.deepEqual(answer.suggestion.products.map(row => row.id), [earrings.id]);
  assert.equal(answer.reply, answer.suggestion.text);
});
test('smaller compares published width and height rather than a Tiny title, shorter chain or one narrower axis', () => {
  const p = piece(1, 'Butterfly Necklace'), smaller = piece(2, 'Butterfly Mini Necklace', 'Necklace', { description: 'The pendant measures 10 mm wide and 12 mm high.' }), tall = piece(3, 'Butterfly Narrow Necklace', 'Necklace', { description: 'The charm measures 8 mm wide and 20 mm high.' }), missing = piece(4, 'Tiny Butterfly Necklace', 'Necklace', { description: 'A very small charm. The necklace has a 12 inch chain.' }), equal = piece(5, 'Butterfly Disc Necklace');
  const answer = create([p, smaller, tall, missing, equal]).suggest({ context: view(p), message: 'Something smaller?' });
  assert.equal(answer.pack.comparison.reference.dimensions.widthMm, 12.5);
  assert.equal(answer.pack.comparison.reference.dimensions.heightMm, 15);
  assert.deepEqual(answer.suggestion.products.map(row => row.id), [smaller.id]);
  assert.equal(answer.suggestion.products[0].dimensions.source, 'product-description');
  assert.match(answer.suggestion.products[0].why, /published width and height/);
});
test('missing or incomparable dimensions produce a useful honest recovery without implying a smaller design', () => {
  const p = piece(1, 'Butterfly Necklace', 'Necklace', { description: 'A butterfly design with an 18 inch necklace chain.' }), candidate = piece(2, 'Tiny Butterfly Necklace', 'Necklace', { description: 'A very small design.' });
  const answer = create([p, candidate]).suggest({ context: view(p), message: 'Can I see something more petite?' });
  assert.equal(answer.pack.comparison.status, 'missing-reference');
  assert.deepEqual(answer.suggestion.products, []);
  assert.match(answer.suggestion.text, /comparable published dimensions/);
});
test('different measurement units are normalized only for actual comparable dimensions', () => {
  const p = piece(1, 'Butterfly Necklace', 'Necklace', { description: 'The charm measures 20 mm in diameter.' }), candidate = piece(2, 'Butterfly Mini Necklace', 'Necklace', { description: 'The charm measures 1.5 cm in diameter.' });
  const answer = create([p, candidate]).suggest({ context: view(p), message: 'A smaller one?' });
  assert.equal(answer.suggestion.products[0].dimensions.diameterMm, 15);
  assert.equal(answer.pack.comparison.reference.dimensions.diameterMm, 20);
});
test('variant-specific size is compared only after an exact size choice and only to the same published size axis', () => {
  const sizeVariant = (id, size, price) => ({ id: 'gid://shopify/ProductVariant/' + id, title: 'Sterling Silver / ' + size, price, available: true, options: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Charm Size', value: size }] });
  const p = piece(1, 'Butterfly Necklace', 'Necklace', { variants: [sizeVariant(101, '12 mm', 50), sizeVariant(102, '20 mm', 60)] }), smaller = piece(2, 'Butterfly Mini Necklace', 'Necklace', { variants: [sizeVariant(201, '10 mm', 45)] }), guide = create([p, smaller]);
  assert.equal(guide.suggest({ context: view(p), message: 'Something smaller?' }).pack.comparison.status, 'missing-reference');
  const result = guide.suggest({ context: selected(p, p.variants[1]), message: 'Something smaller?' });
  assert.equal(result.suggestion.products[0].id, smaller.id);
  assert.equal(result.pack.comparison.reference.dimensions.source, 'published-variant-option');
  assert.equal(result.suggestion.products[0].dimensions.optionName, 'Charm Size');
});
test('cheaper and smaller remain a conjunction on the same actual available variant', () => {
  const p = piece(1, 'Butterfly Necklace'), cheapLarge = piece(2, 'Butterfly Large Necklace', 'Necklace', { variants: [variant(201, 'Sterling Silver', 40)], description: 'The charm measures 30 mm wide and 40 mm high.' }), smallCostly = piece(3, 'Butterfly Small Necklace', 'Necklace', { variants: [variant(301, 'Sterling Silver', 80)], description: 'The charm measures 10 mm wide and 12 mm high.' }), both = piece(4, 'Butterfly Mini Necklace', 'Necklace', { variants: [variant(401, 'Sterling Silver', 45)], description: 'The charm measures 10 mm wide and 12 mm high.' });
  const result = create([p, cheapLarge, smallCostly, both]).suggest({ context: selected(p, p.variants[0]), message: 'Something cheaper and smaller?' });
  assert.deepEqual(result.suggestion.products.map(row => row.id), [both.id]);
  assert.match(result.suggestion.text, /lower-priced, smaller/);
});
test('hidden script, style, template and comment bodies never become published dimensional facts', () => {
  const hidden = '<script>The charm measures 1 mm wide and 1 mm high.</script><style>The charm measures 2 mm wide and 2 mm high.</style><template>The charm measures 3 mm wide and 3 mm high.</template><!-- The charm measures 4 mm wide and 4 mm high. -->';
  const p = piece(1, 'Butterfly Necklace'), larger = piece(2, 'Butterfly Mini Necklace', 'Necklace', { description: hidden + '<p>The charm measures 20 mm wide and 25 mm high.</p>' }), guide = create([p, larger]);
  const pack = guide.prepare(view(larger));
  assert.ok(pack.details.some(d => d.text === 'The charm measures 20 mm wide and 25 mm high.'));
  assert.ok(pack.details.every(d => !/1 mm|2 mm|3 mm|4 mm/.test(d.text)));
  const answer = guide.suggest({ context: view(p), message: 'Anything smaller?' });
  assert.deepEqual(answer.suggestion.products, []);
  assert.equal(answer.pack.comparison.status, 'no-match');
});
