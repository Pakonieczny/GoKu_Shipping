'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const memory = require('../../brites-concierge-memory.js');
const server = require('../../netlify/functions/_britesConciergeMemory.js');
const bridge = require('../../brites-storefront-bridge.js');
const guide = require('../../brites-concierge-shopping-guide.js');
const NOW = Date.parse('2026-10-10T16:00:00Z');

// Run the production client journal and the production server chunk cleaner.
// Only the authenticated transport/store are synthetic, with no real account,
// SDK session, cloud write, provider or payment request.
function accountMemory({ journal = new Map(), archive = new Map(), local = { transcript: [] } } = {}) {
  let timerId = 0;
  const timers = new Map(), calls = [], hydrated = [];
  const storage = { getItem: key => journal.get(key) || null, setItem: (key, value) => journal.set(key, value), removeItem: key => journal.delete(key) };
  const user = { uid: 'meaning-shopper', email: 'meaning-shopper@fixture.example', isAnonymous: false, getIdToken: async () => 'fixture-account-token' };
  const environment = { AbortController, crypto: require('node:crypto').webcrypto, BritesStorefrontBridge: bridge, addEventListener() {}, removeEventListener() {} };
  const client = memory.create({
    environment, storage, now: () => NOW, getCurrentUser: () => user, getLocalMemory: () => local,
    setTimeout(fn, delay) { const id = ++timerId; timers.set(id, { fn, delay }); return id; }, clearTimeout(id) { timers.delete(id); },
    onHydrate(value) { hydrated.push(structuredClone(value)); },
    async fetch(url, request) {
      assert.equal(request.headers.Authorization, 'Bearer fixture-account-token');
      const body = JSON.parse(request.body); calls.push(structuredClone(body));
      if (body.action === 'sync') {
        for (const candidate of body.chunks) if (!archive.has(candidate.id)) archive.set(candidate.id, JSON.stringify(server.chunk(candidate, NOW)));
        return { ok: true, status: 200, json: async () => ({ ok: true, generation: '0', saved: body.chunks.length, duplicates: 0, syncAfterMs: 60000 }) };
      }
      assert.equal(body.action, 'read');
      const chunks = [...archive.values()].map(value => JSON.parse(value));
      const preferences = server.preferences(Object.assign({}, ...chunks.map(chunk => chunk.preferences)));
      return { ok: true, status: 200, json: async () => ({ ok: true, generation: '0', chunks, preferences, nextCursor: null }) };
    }
  });
  return { client, journal, archive, calls, hydrated };
}

test('a full stated reason survives client journal reload server persistence and account hydration', async () => {
  const reason = ('she loves butterflies and the walks we took together in the garden; ' + 'we want to celebrate her curiosity and the new chapter ahead. '.repeat(3)).trim();
  assert.ok(reason.length > 120 && reason.length <= 300);
  const personal = guide.normalizeShopperContext({ recipient: 'daughter', occasion: 'graduation', reason, topicKey: 'daughter-graduation' });
  const local = { transcript: [{ role: 'user', content: 'A gift for my daughter for graduation.' }], preferences: { recipient: personal.recipient, occasion: personal.occasion, intent: personal.reason } };
  const first = accountMemory({ local }); await first.client.ready();
  const durable = JSON.parse([...first.journal.values()][0]);
  assert.equal(durable.pending[0].preferences.intent, reason); assert.equal(first.calls.some(call => call.action === 'sync'), false);
  first.client.dispose();

  const reloaded = accountMemory({ journal: first.journal, archive: first.archive }); await reloaded.client.ready();
  await reloaded.client.sync();
  assert.equal(reloaded.calls.find(call => call.action === 'sync').chunks[0].preferences.intent, reason);
  assert.equal(JSON.parse([...reloaded.archive.values()][0]).preferences.intent, reason);
  reloaded.client.dispose();

  const restored = accountMemory({ archive: first.archive }); const result = await restored.client.ready();
  assert.equal(result.preferences.intent, reason); assert.equal(restored.hydrated.at(-1).preferences.intent, reason);
  const active = guide.normalizeShopperContext({ recipient: result.preferences.recipient, occasion: result.preferences.occasion, reason: result.preferences.intent });
  assert.equal(active.reason, personal.reason); assert.equal(active.recipient, 'daughter'); assert.equal(active.occasion, 'graduation');
  assert.doesNotMatch([...first.journal.values(), ...first.archive.values()].join(''), /fixture-account-token|Authorization/);
  restored.client.dispose();
});

test('the longer reason limit keeps other fields bounded and private text redacted through storage and hydration', async () => {
  const reason = 'A personal reason ' + 'celebrating her new chapter '.repeat(5) + 'person@example.com, card 4111 1111 1111 1111, password=privatevalue. ' + 'remembering the garden '.repeat(8);
  const style = 'a'.repeat(180), recipient = 'b'.repeat(180), occasion = 'c'.repeat(180);
  const local = { transcript: [{ role: 'user', content: 'Remember the checked butterfly necklace.' }], preferences: { intent: reason, style, recipient, occasion, customerEmail: 'secret@example.com', actions: [{ type: 'bag-clear' }] } };
  const saved = accountMemory({ local }); await saved.client.ready(); await saved.client.sync();
  const incoming = saved.calls.find(call => call.action === 'sync').chunks[0].preferences;
  assert.equal(incoming.intent.length, 300); assert.ok(incoming.intent.includes('[private contact]')); assert.equal(incoming.style, style.slice(0, 120)); assert.equal(incoming.recipient, recipient.slice(0, 120)); assert.equal(incoming.occasion, occasion.slice(0, 120));
  assert.equal(incoming.customerEmail, undefined); assert.equal(incoming.actions, undefined);
  assert.doesNotMatch([...saved.journal.values(), ...saved.archive.values()].join(''), /person@example|4111|privatevalue|secret@example|fixture-account-token/);
  saved.client.dispose();
  const restored = accountMemory({ archive: saved.archive }); const result = await restored.client.ready();
  assert.equal(result.preferences.intent.length, 300); assert.equal(result.preferences.style.length, 120);
  assert.doesNotMatch(JSON.stringify(result), /person@example|4111|privatevalue|secret@example|fixture-account-token/);
  restored.client.dispose();
});
