'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const core = require('../../netlify/functions/_britesGrowth.js');

const NOW = Date.parse('2026-10-03T12:00:00Z');

function product(id, handle, title, price) {
  return {
    id: 'gid://shopify/Product/' + id,
    handle,
    title,
    url: 'https://britesjewelry.com/products/' + handle,
    currency: 'USD',
    description: 'An origami pendant on an included chain.',
    type: 'Necklace',
    tags: ['origami'],
    image: null,
    imageAlt: title,
    options: [{ name: 'Necklace Length', values: ['14 Inches'] }],
    variants: [{
      id: 'gid://shopify/ProductVariant/' + id + '01',
      numericId: id + '01',
      title: 'Sterling Silver / 14 Inches / None',
      price,
      available: true,
      options: [
        { name: 'Metal Choice', value: 'Sterling Silver' },
        { name: 'Necklace Length', value: '14 Inches' },
        { name: 'Engraving', value: 'None' }
      ]
    }],
    variantsComplete: true,
    checkedAt: NOW
  };
}

const displayed = [
  product('901', 'origami-bird-pendant-necklace-for-couples', 'Origami Bird Pendant Necklace', 57),
  product('902', 'mini-origami-eotriceratops-necklace', 'Origami Eotriceratops Pendant Necklace', 60),
  product('903', 'dainty-origami-swan-necklace', 'Origami Swan Cutout Pendant Necklace', 60)
];

function fixture() {
  const calls = { handles: [], searches: 0 };
  return {
    calls,
    deps: {
      service: {
        saveProducts: async () => {},
        productIssues: async () => [],
        research: async () => [],
        storySupplements: async () => []
      },
      shopify: {
        byHandle: async handle => {
          calls.handles.push(handle);
          return displayed.find(item => item.handle === handle) || null;
        },
        search: async () => {
          calls.searches += 1;
          throw new Error('Typed actions must use the already displayed live handles.');
        }
      },
      ai: async () => null,
      now: () => NOW
    }
  };
}

async function run(message) {
  const f = fixture();
  const answer = await core.concierge({
    ...f.deps,
    message,
    // Displayed cards are authoritative for a shopper action. A stale saved
    // discovery preference must not rerank an exact visible selection away.
    preferences: core.intentFrom('Bunny silver earrings under $1.'),
    context: { productHandles: displayed.map(item => item.handle), currency: 'USD' }
  });
  assert.equal(f.calls.searches, 0, message);
  assert.deepEqual(f.calls.handles, displayed.map(item => item.handle), message);
  assert.ok(!answer.actions.some(action => action.type === 'purchase'), message);
  return answer;
}

test('typed exact-title open uses the matching displayed live product', async () => {
  const answer = await run('Open the Origami Bird Pendant Necklace.');
  assert.deepEqual(answer.requestedAction, {
    type: 'navigate',
    productId: displayed[0].id,
    url: displayed[0].url
  });
  assert.equal(answer.question, null);
});

test("typed ordinal 'View the first piece' opens the first displayed live product", async () => {
  const answer = await run('View the first piece.');
  assert.deepEqual(answer.requestedAction, {
    type: 'navigate',
    productId: displayed[0].id,
    url: displayed[0].url
  });
  assert.equal(answer.question, null);
});

test('typed exact-title add opens option selection and preserves confirmation', async () => {
  const answer = await run('Add the Origami Bird Pendant Necklace to my sandbox bag.');
  assert.deepEqual(answer.requestedAction, {
    type: 'choose',
    productId: displayed[0].id,
    url: displayed[0].url
  });
  assert.match(answer.reply, /confirm before it is added to your bag/i);
  assert.doesNotMatch(answer.reply, /checkout|order placed|payment/i);
  assert.equal(answer.question, null);
});
