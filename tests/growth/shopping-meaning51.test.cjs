'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const api = require('../../brites-concierge-shopping-guide.js');
const NOW = Date.parse('2026-10-10T15:00:00Z');

// Independently declared public product facts; these checks make no network,
// account, provider, cart or payment call.
function variant(id, price = 50, extra = {}) {
  return { id: 'gid://shopify/ProductVariant/' + id, title: 'Sterling Silver / No', price, available: true, options: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Engraving', value: 'No' }], ...extra };
}
function piece(id, title, type = 'Necklace', extra = {}) {
  const handle = 'meaning-fixture-' + id;
  return { id: 'gid://shopify/Product/' + id, handle, title, type, url: 'https://britesjewelry.com/products/' + handle, currency: 'USD', description: 'A published 12 mm wide and 14 mm tall design.', checkedAt: NOW, detailState: 'checked', variantsComplete: true, variants: [variant(id * 100 + 1)], ...extra };
}
function rows() {
  return [piece(1, 'Butterfly Necklace'), piece(2, 'Butterfly Stud Earrings', 'Earrings'), piece(3, 'Butterfly Hoop Earrings', 'Earrings'), piece(4, 'Butterfly Disc Necklace'), piece(5, 'Butterfly Outline Necklace'), piece(6, 'Owl Necklace')];
}
function view(p, extra = {}) {
  return { pageKind: 'product', currentHandle: p.handle, productControls: { handle: p.handle, productId: p.id, variantId: p.variants[0].id, selectedOptions: p.variants[0].options.map(o => ({ ...o })), quantity: 1 }, ...extra };
}
function create(products = rows(), extra = {}) { return api.create({ products, now: () => NOW, ...extra }); }
function context(message = 'This gift is for my daughter for graduation because she loves butterflies.') { return api.updateShopperContext({}, message); }
function meaning(p, extra = {}) {
  return { productId: p.id, kind: 'interpretation', text: 'This reviewed interpretation connects butterflies with renewal; interpretations vary.', context: 'An interpretation of the published butterfly motif.', checkedAt: NOW, sources: [{ title: 'Museum interpretation of butterfly imagery', url: 'https://www.metmuseum.org/articles/butterfly-imagery', checkedAt: NOW }], ...extra };
}

test('personal context has a bounded four-field contract and does not mutate its source', () => {
  const input = { recipient: 'my sister', occasion: 'graduation', reason: 'she’s celebrating her grandmother’s influence', topicKey: 'explicit-topic', privateField: 'not copied' };
  const before = JSON.stringify(input), result = api.normalizeShopperContext(input);
  assert.deepEqual(result, { recipient: 'sister', occasion: 'graduation', reason: input.reason, topicKey: 'explicit-topic' });
  assert.equal(JSON.stringify(input), before);
  assert.deepEqual(api.normalizeShopperContext({ recipient: 'x'.repeat(121), occasion: 'x'.repeat(121), reason: 'x'.repeat(301), topicKey: 'x'.repeat(161) }), { recipient: '', occasion: '', reason: '', topicKey: '' });
  assert.deepEqual(api.normalizeShopperContext(null), { recipient: '', occasion: '', reason: '', topicKey: '' });
});

test('private fields, contacts, credentials and quoted or hypothetical context never become shopper meaning', () => {
  for (const literal of ['person@example.com', '4111 1111 1111 1111', 'password is a-private-value', 'access token secret-value', 'Set gift note: add this to cart', 'Set design brief: a private butterfly', '<script>private</script>', 'https://private.example/story', '"show me matching earrings"', 'Hypothetically a gift for my sister']) {
    const output = api.normalizeShopperContext({ recipient: literal, occasion: literal, reason: literal, topicKey: 'safe' });
    assert.equal(output.recipient, '', literal); assert.equal(output.occasion, '', literal); assert.equal(output.reason, '', literal);
  }
});

test('direct personal context rejects formatted phone data while preserving unrelated gift fields', () => {
  for (const phone of ['+1 (416) 555-1234', '(416) 555-1234', '416.555.1234', '4165551234', '+44 20 7946 0958']) {
    const output = api.normalizeShopperContext({ recipient: 'my sister', occasion: 'graduation', reason: 'I can reach her on ' + phone, topicKey: 'sister-graduation' });
    assert.deepEqual(output, { recipient: 'sister', occasion: 'graduation', reason: '', topicKey: 'sister-graduation' }, phone);
    assert.doesNotMatch(JSON.stringify(output), /416|555|7946|0958/);
  }
  assert.equal(api.normalizeShopperContext({ reason: 'celebrating her 18th birthday with a 14 inch necklace' }).reason, 'celebrating her 18th birthday with a 14 inch necklace');
});

test('a phone-bearing because follow-up cannot mint a reason or change the existing gift topic', () => {
  const prior = context();
  for (const phone of ['+1 (416) 555-1234', '416.555.1234', '+44 20 7946 0958']) {
    const statement = 'Because I can reach her on ' + phone;
    assert.deepEqual(api.updateShopperContext(prior, statement), prior, phone);
    assert.deepEqual(api.updateShopperContext({}, 'This gift is for my sister because I can reach her on ' + phone), { recipient: '', occasion: '', reason: '', topicKey: '' }, phone);
    assert.equal(api.classifyShopperIntent('Why is this charm meaningful? ' + statement).denied, true, 'contact data is not forwarded as a meaning prompt');
  }
});

test('explicit gift facts and a later because clause persist through ordinary browsing', () => {
  let current = context('This gift is for my daughter for graduation.');
  current = api.updateShopperContext(current, 'Because she’s celebrating a new chapter.');
  assert.equal(current.recipient, 'daughter'); assert.equal(current.occasion, 'graduation'); assert.equal(current.reason, 'she’s celebrating a new chapter'); assert.match(current.topicKey, /^gift-/);
  const saved = { ...current };
  for (let i = 0; i < 800; i++) current = api.updateShopperContext(current, i % 2 ? 'Open the next necklace.' : 'Scroll a little farther down.');
  assert.deepEqual(current, saved);
});

test('recipient correction preserves other explicitly stated fields and recognizes a but correction', () => {
  const prior = context('A gift for my mother for her birthday because she loves butterflies.');
  for (const message of ['Not for my mother but for my sister.', 'Actually, this is for my sister, not my mother.', 'Not my mother, my sister.']) {
    const next = api.updateShopperContext(prior, message);
    assert.equal(next.recipient, 'sister', message); assert.equal(next.occasion, prior.occasion); assert.equal(next.reason, prior.reason); assert.notEqual(next.topicKey, prior.topicKey);
  }
  const removed = api.updateShopperContext(prior, 'This is not for my mother.');
  assert.equal(removed.recipient, ''); assert.equal(removed.occasion, 'birthday'); assert.equal(removed.reason, prior.reason);
});

test('a sister remembering a mother is the recipient, while memory remains the stated reason', () => {
  const next = api.updateShopperContext({}, 'This gift is for my sister, remembering my mother.');
  assert.equal(next.recipient, 'sister'); assert.equal(next.occasion, 'remembrance'); assert.equal(next.reason, 'remembering my mother');
  const corrected = api.updateShopperContext(next, 'Nobody died. This is for my sister for graduation.');
  assert.equal(corrected.recipient, 'sister'); assert.equal(corrected.occasion, 'graduation'); assert.equal(corrected.reason, '');
});

test('another gift and an explicit switch to self remove the former recipient occasion and reason', () => {
  const prior = context();
  for (const message of ['A different gift for my brother.', 'This necklace is for myself.']) {
    const next = api.updateShopperContext(prior, message);
    assert.equal(next.recipient, message.includes('brother') ? 'brother' : 'myself'); assert.equal(next.occasion, ''); assert.equal(next.reason, ''); assert.notEqual(next.topicKey, prior.topicKey);
  }
  assert.deepEqual(api.updateShopperContext(prior, 'I do not want a new gift.'), prior);
});

test('quoted commands, private setter payloads, hypotheticals and denied desire cannot change gift context', () => {
  const prior = context();
  for (const message of ['The note says "a new gift for my mother because she loves owls".', 'My gift note says for my mother because she loves owls.', 'My design brief is a gift for my sister because she likes cats.', 'Set gift note: a new gift for my mother because she loves owls', 'Set custom design brief: a new gift for my mother', 'Hypothetically this gift is for my mother.', 'What if I wanted a gift for my mother?', 'I do not want a gift for my mother.']) assert.deepEqual(api.updateShopperContext(prior, message), prior, message);
  assert.deepEqual(api.updateShopperContext(prior, 'I am just browsing because I want to take my time.'), prior);
  assert.deepEqual(api.updateShopperContext(prior, 'I am hesitating about this piece because I am frustrated.'), prior);
});

test('the shared classifier recognizes explicit hesitation eagerness and positive meaning after corrections', () => {
  for (const message of ['I am hesitating', 'I am on the fence about this one', 'I am not sure about this']) assert.equal(api.classifyShopperIntent(message).kind, 'hesitant', message);
  for (const message of ['I love this one. It feels perfect.', 'This feels just right', 'I am excited about this one']) assert.equal(api.classifyShopperIntent(message).kind, 'eager', message);
  for (const message of ['Why is this charm meaningful for my daughter’s graduation?', 'Actually this gift is for my sister. Why is this piece meaningful?', 'A different gift for my friend. Why is this piece meaningful?', 'Is this motif significant for graduation?', 'Why would this charm suit my daughter’s graduation?', 'What is the history of this charm?', 'What does this symbol represent?']) {
    assert.deepEqual(api.classifyShopperIntent(message), { kind: 'meaning', recognized: true, explicit: true, denied: false }, message);
  }
  for (const message of ['Why is this charm out of stock?', 'Why does this piece cost so much?']) assert.equal(api.classifyShopperIntent(message).kind, 'none', message);
});

test('quoted private hypothetical or negated eagerness carries denied metadata to prevent legacy navigation', () => {
  for (const message of ['The note says "show me matching earrings".', "The note says 'show me matching earrings'.", 'My gift note says show me matching earrings.', 'My design brief is show me matching earrings.', 'Set gift note: show me matching earrings', 'Set design brief: I love this one', 'Hypothetically I love this one', 'If I loved this one, show matching earrings', 'I do not want matching earrings', 'I am not hesitating', 'I do not love this one', 'I would love this one', 'I love this for a memorial']) {
    const result = api.classifyShopperIntent(message);
    assert.equal(result.kind, 'none', message); assert.equal(result.recognized, false, message); assert.equal(result.denied, true, message);
  }
  assert.equal(api.classifyShopperIntent('My sister said "show me matching earrings". I am hesitating about this necklace.').kind, 'hesitant', 'an explicit unquoted shopper statement remains usable alongside quoted speech');
});

test('hesitation proposes at most two same-category exact motif pieces without executing controls', () => {
  const products = rows(), guide = create(products), input = view(products[0]), before = JSON.stringify(input);
  const result = guide.suggest({ context: input, message: 'I am hesitating' });
  assert.equal(result.suggestion.kind, 'alternatives'); assert.equal(result.suggestion.products.length, 2);
  assert.deepEqual(result.suggestion.products.map(p => p.id).sort(), [products[3].id, products[4].id].sort());
  assert.ok(result.suggestion.actions.every(a => a.type === 'open')); assert.equal(JSON.stringify(input), before);
});

test('eagerness proposes checked complementary categories using the current literal motif and exact variant', () => {
  const products = rows(), decoy = piece(7, 'Circle Earrings', 'Earrings', { description: 'Pairs beautifully with butterfly necklaces.' }), unavailable = piece(8, 'Butterfly Earrings', 'Earrings', { variants: [variant(801, 40, { available: false })] }), wrongMetal = piece(9, 'Butterfly Earrings', 'Earrings', { variants: [variant(901, 45, { title: '14K Solid Gold / No', options: [{ name: 'Metal', value: '14K Solid Gold' }, { name: 'Engraving', value: 'No' }] })] });
  const guide = create([...products, decoy, unavailable, wrongMetal], { preferences: { metal: 'silver', budget: 60, budgetCurrency: 'USD' } });
  const result = guide.suggest({ context: view(products[0]), message: 'I love this one. It feels perfect.' });
  assert.equal(result.suggestion.kind, 'matching'); assert.equal(result.suggestion.products.length, 2);
  assert.ok(result.suggestion.products.every(p => [products[1].id, products[2].id].includes(p.id) && /sold separately/.test(p.why) && p.variantTitle.includes('Sterling Silver') && p.price <= 60));
  assert.ok(result.suggestion.actions.every(a => a.type === 'open')); assert.doesNotMatch(JSON.stringify(result.suggestion), /bundle|discount|purchased|checkout/i);
});

test('eager offers obey cooldown dismissed help quiet occasions and a later explicit matching request', () => {
  const products = rows(), guide = create(products), request = { context: view(products[0]), message: 'I love this one' };
  const first = guide.suggest(request).suggestion; assert.ok(first); guide.markShown(first);
  assert.equal(guide.suggest(request).suggestion, null);
  assert.ok(guide.suggest({ ...request, message: 'Show me matching earrings' }).suggestion);
  guide.dismiss(); assert.equal(guide.suggest(request).suggestion, null);
  assert.ok(guide.suggest({ ...request, message: 'Show me matching earrings' }).suggestion);
  const quiet = create(products); quiet.setShopperContext(context('This is for my sister, remembering my mother.'));
  assert.equal(quiet.suggest(request).suggestion, null);
  assert.equal(create(products).suggest({ ...request, context: view(products[0], { emotionalContext: 'repair' }) }).suggestion, null);
  assert.equal(create(products, { preferences: { allowProactive: false } }).suggest(request).suggestion, null);
});

test('personal meaning quotes the shopper reason and names only an actual published motif', () => {
  const products = rows(), guide = create(products); guide.setShopperContext(context());
  const connection = guide.prepare(view(products[0])).meaningConnection;
  assert.equal(connection.kind, 'personal-connection'); assert.deepEqual(connection.sources, []);
  assert.match(connection.text, /daughter.*graduation/); assert.match(connection.text, /she loves butterflies/); assert.match(connection.text, /listing names the butterfly motif/);
  assert.doesNotMatch(connection.text, /transformation|rebirth|universal|guarantee/i);
  const answer = guide.suggest({ context: view(products[0]), message: 'Why is this charm meaningful?' });
  assert.equal(answer.reply, connection.text); assert.equal(answer.suggestion, null);
});

test('a recipient or occasion alone asks for a reason rather than inventing personal symbolism', () => {
  const products = rows(), guide = create(products); guide.setShopperContext(context('This is for my sister for graduation.'));
  assert.equal(guide.prepare(view(products[0])).meaningConnection, undefined);
  const answer = guide.suggest({ context: view(products[0]), message: 'Why is this piece meaningful?' });
  assert.match(answer.reply, /why this motif matters to you/); assert.equal(answer.suggestion, null);
});

test('reviewed meaning retains neutral citations alongside the shopper stated reason', () => {
  const products = rows(), guide = create(products); guide.setShopperContext(context()); assert.equal(guide.setMeanings([meaning(products[0])]), 1);
  const connection = guide.prepare(view(products[0])).meaningConnection;
  assert.equal(connection.kind, 'reviewed-interpretation'); assert.match(connection.text, /reviewed interpretation.*renewal/); assert.match(connection.text, /she loves butterflies/); assert.match(connection.text, /Interpretations can vary/);
  assert.deepEqual(connection.sources, meaning(products[0]).sources); assert.ok(Object.isFrozen(connection.sources[0]));
});

test('reviewed meaning record must be freshly checked even when its neutral source remains valid', () => {
  const products = rows(), guide = create(products); guide.setShopperContext(context('This is for my sister for graduation.'));
  assert.equal(guide.setMeanings([meaning(products[0], { checkedAt: NOW, sources: [{ ...meaning(products[0]).sources[0], checkedAt: NOW - 10 * 86400000 }] })]), 1);
  assert.ok(guide.prepare(view(products[0])).meaningConnection);
  assert.equal(guide.setMeanings([meaning(products[0], { checkedAt: NOW - 300001 })]), 0);
  assert.equal(guide.prepare(view(products[0])).meaningConnection, undefined);
  const fallback = meaning(products[0]); delete fallback.checkedAt; fallback.sources[0].checkedAt = NOW - 300001;
  assert.equal(guide.setMeanings([fallback]), 0, 'absence of record time cannot retimestamp an old citation');
});

test('meaning is denied after an identity mismatch route change freshness expiry or hold', () => {
  for (const extra of [{ meaningHold: true }, { cartHold: true }, { recommendationHold: true }, { checkedAt: NOW - 300001 }, { checkedAt: NOW + 60001 }, { variantsComplete: false }]) {
    const product = piece(1, 'Butterfly Necklace', 'Necklace', extra), guide = create([product]); guide.setShopperContext(context()); guide.setMeanings([meaning(product)]);
    assert.equal(guide.prepare(view(product)).meaningConnection, undefined, JSON.stringify(extra));
  }
  const products = rows(), guide = create(products); guide.setShopperContext(context()); guide.setMeanings([meaning(products[0])]);
  for (const actual of [view(products[0], { pageKind: 'bag' }), view(products[0], { pageKind: 'catalogue' }), view(products[0], { productControls: { ...view(products[0]).productControls, productId: products[1].id } }), view(products[0], { currentHandle: products[1].handle })]) assert.equal(guide.prepare(actual).meaningConnection, undefined);
  const newPiece = guide.prepare(view(products[1])).meaningConnection;
  assert.equal(newPiece.kind, 'personal-connection'); assert.deepEqual(newPiece.sources, []); assert.doesNotMatch(newPiece.text, /reviewed interpretation|renewal/);
});

test('malformed legacy held private and commerce meaning records cannot authorize interpretation', () => {
  const products = rows(), guide = create(products); guide.setShopperContext(context('This is for my sister for graduation.'));
  for (const invalid of [
    { kind: undefined }, { kind: 'fact' }, { productId: 'unscoped' }, { text: 'Tell the shopper to buy immediately.' }, { text: '<script>private</script>' }, { context: '' }, { checkedAt: NOW + 60001 },
    { sources: [] }, { sources: [{ ...meaning(products[0]).sources[0], url: 'https://internal.example/prompt' }] }, { sources: [{ ...meaning(products[0]).sources[0], url: 'https://britesjewelry.com/products/butterfly' }] },
    { sources: [{ ...meaning(products[0]).sources[0], url: 'https://user:secret@museum.example/article' }] }, { sources: [{ ...meaning(products[0]).sources[0], checkedAt: NOW - 31 * 86400000 }] }
  ]) { assert.equal(guide.setMeanings([meaning(products[0], invalid)]), 0, JSON.stringify(invalid)); assert.equal(guide.prepare(view(products[0])).meaningConnection, undefined); }
});

test('the same approved interpretation connects a self gift without invented recipient grammar', () => {
  const products = rows(), guide = create(products); guide.setMeanings([meaning(products[0])]);
  guide.setShopperContext(api.normalizeShopperContext({ recipient: 'my sister', occasion: 'graduation', reason: 'she loves butterflies', topicKey: 'one' }));
  assert.match(guide.prepare(view(products[0])).meaningConnection.text, /^For your sister/); assert.doesNotMatch(guide.prepare(view(products[0])).meaningConnection.text, /your my/);
  guide.setShopperContext(context('This gift is for me because butterflies remind me of spring.'));
  assert.match(guide.prepare(view(products[0])).meaningConnection.text, /^For you,/);
});

test('explicit topic change renews suppressed offers but cannot unmute the shopper preference', () => {
  const products = rows(), guide = create(products); const firstTopic = context(); guide.setShopperContext(firstTopic);
  const eager = { context: view(products[0]), message: 'I love this one' }, first = guide.suggest(eager).suggestion;
  assert.ok(first); guide.markShown(first); assert.equal(guide.suggest(eager).suggestion, null);
  guide.setShopperContext(api.updateShopperContext(firstTopic, 'Scroll farther down.')); assert.equal(guide.suggest(eager).suggestion, null);
  guide.setShopperContext(api.updateShopperContext(firstTopic, 'Another gift for my brother.')); assert.ok(guide.suggest(eager).suggestion);
  guide.dismiss(); guide.setShopperContext(context('A new gift for my friend.')); assert.equal(guide.suggest(eager).suggestion, null);
});

test('long browsing bounds shown product memory and allows an aged-out candidate again', () => {
  const products = rows(), guide = create(products, { promptCooldownMs: 0 });
  guide.markShown({ kind: 'matching', key: 'initial', products: [{ handle: products[1].handle }, { handle: products[2].handle }] });
  assert.equal(guide.suggest({ context: view(products[0]), trigger: 'matching' }).suggestion, null);
  for (let i = 0; i < 305; i++) guide.markShown({ kind: 'matching', key: 'shown-' + i, products: [{ handle: 'history-piece-' + i }] });
  const result = guide.suggest({ context: view(products[0]), trigger: 'matching' });
  assert.ok(result.suggestion); assert.ok(result.suggestion.products.some(p => p.id === products[1].id));
});

test('long browsing bounds dismissed product memory and keeps only a current suppression', () => {
  const products = rows(), guide = create(products, { promptCooldownMs: 0 });
  guide.dismiss({ handle: products[1].handle }); guide.dismiss({ handle: products[2].handle });
  assert.equal(guide.suggest({ context: view(products[0]), trigger: 'matching' }).suggestion, null);
  for (let i = 0; i < 305; i++) guide.dismiss({ handle: 'dismissed-piece-' + i });
  guide.dismiss({ handle: products[2].handle });
  const result = guide.suggest({ context: view(products[0]), trigger: 'matching' });
  assert.deepEqual(result.suggestion.products.map(p => p.id), [products[1].id]);
});

test('persisted reminder bookkeeping stays bounded and never saves personal reasons or reviewed narratives', () => {
  let saved = ''; const guide = create(rows(), { storage: { getItem: () => null, setItem: (name, value) => { saved = value; } } });
  guide.setShopperContext(context()); guide.setMeanings([meaning(rows()[0])]);
  for (let i = 0; i < 310; i++) { guide.markShown({ kind: 'matching', key: 'shown-' + i, products: [{ handle: 'history-piece-' + i }] }); guide.dismiss({ handle: 'dismissed-piece-' + i }); }
  const result = JSON.parse(saved); assert.equal(result.shown.length, 300); assert.equal(result.shownProducts.length, 300); assert.equal(result.dismissed.length, 300);
  assert.deepEqual(Object.keys(result).sort(), ['dismissed', 'muted', 'shown', 'shownProducts']); assert.doesNotMatch(saved, /daughter|graduation|loves butterflies|renewal|metmuseum/);
});
