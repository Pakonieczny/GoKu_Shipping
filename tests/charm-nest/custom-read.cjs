// Custom Orders reading (netlify/functions/_charmNestCustomRead.js): what Claude is shown for a line with no design of
// its own, how its answer is kept (under the hash of what it was read from), and a person's decision over one or many
// lines. Firestore is a small in-memory fake; the model is a stub; no network.
//   node tests/charm-nest/custom-read.cjs
const assert = require('assert');
const path = require('path');
const CR = require(path.join(__dirname, '../../netlify/functions/_charmNestCustomRead.js'));

const DELETE = { __delete: true };
function fakeDb(seed = {}) {
  const docs = new Map(Object.entries(seed));
  const merge = (cur, data) => { const out = Object.assign({}, cur); for (const [k, v] of Object.entries(data)) { if (v === DELETE) delete out[k]; else if (v && typeof v === 'object' && !Array.isArray(v) && out[k] && typeof out[k] === 'object') out[k] = merge(out[k], v); else out[k] = v; } return out; };
  const ref = p => ({
    id: p.split('/').pop(), path: p,
    async get() { return { id: p.split('/').pop(), exists: docs.has(p), data: () => docs.get(p) }; },
    async set(data, o) { docs.set(p, o && o.merge ? merge(docs.get(p) || {}, data) : data); },
    collection: c => coll(p + '/' + c)
  });
  const coll = c => ({
    doc: id => ref(c + '/' + id),
    where: (f, op, v) => ({ limit: () => ({ get: async () => ({ docs: [...docs].filter(([k, d]) => k.startsWith(c + '/') && k.split('/').length === c.split('/').length + 1 && d[f] === v).map(([k, d]) => ({ id: k.split('/').pop(), data: () => d })) }) }) }),
    orderBy: () => ({ limit: () => ({ get: async () => ({ docs: [...docs].filter(([k]) => k.startsWith(c + '/') && k.split('/').length === c.split('/').length + 1).map(([k, d]) => ({ id: k.split('/').pop(), data: () => d })).reverse() }) }) })
  });
  const db = { docs, collection: coll, async getAll(...refs) { return Promise.all(refs.map(r => r.get())); }, batch() { const ops = []; return { set: (r, d, o) => ops.push(() => r.set(d, o)), async commit() { for (const f of ops) await f(); } }; } };
  return db;
}
const FV = { delete: () => DELETE, serverTimestamp: () => 'now' };
const admin = db => ({ firestore: Object.assign(() => db, { FieldValue: FV }) });

const line = (key, extra) => Object.assign({ key, hash: 'h' + key.length, order: '4170000001', listingId: '1719001', sku: '', title: '', quantity: 1, jewellery: '', metal: '', options: [], personalization: [], buyerMessage: '', staffNote: '', team: [], otherLines: [], hints: [] }, extra);

(async () => {
  // 1 · what one line reads as: "Type not specified" when no jewellery type, the stored listing and conversations
  const db = fakeDb({
    'EtsyMail_Listings/1719001': { description: 'Custom listing: we design your charm from your photo', tags: ['custom'] },
    'EtsyMail_Threads/t1': { etsyOrderId: '4170000001', buyerUserId: '77', lastInboundAt: 5 },
    'EtsyMail_Threads/t1/messages/m1': { direction: 'inbound', text: 'This is for my order 4169999999, the dog photo', timestamp: 1 }
  });
  const ln = CR.cleanLine(line('4170000001_1', { title: 'Custom Order for Anna', buyerMessage: 'see my photo' }));
  const ctx = { listings: { '1719001': { description: 'Custom listing: we design your charm from your photo', tags: ['custom'] } }, photos: {}, mail: { '4170000001': [{ about: 'order 4170000001', messages: ['buyer: for my order 4169999999'] }] } };
  const text = CR.content([ln], ctx).map(b => b.text || '').join('\n');
  assert.match(text, /Jewellery type as read: Type not specified/);
  assert.match(text, /Custom listing: we design your charm/);
  assert.match(text, /for my order 4169999999/);
  assert.match(CR.INSTRUCTIONS, /add on/i, 'the instructions say "add on" in a title never decides');

  // 2 · a job: each answer kept under the hash it was read from; the stub sees the conversation from the stored inbox
  let seen = '';
  const anthropic = { async callClaudeRaw(o) {
    seen = o.messages[0].content.map(b => b.text || '').join('\n');
    assert.strictEqual(o.outputFormat.type, 'json_schema');
    const keys = [...seen.matchAll(/── key (\S+)/g)].map(m => m[1]);
    return { stop_reason: 'end_turn', usage: { input_tokens: 900, output_tokens: 120 }, content: [{ type: 'text', text: JSON.stringify({ lines: keys.map(k => ({ key: k, kind: /_1$/.test(k) ? 'rework' : 'regular', confidence: /_1$/.test(k) ? 0.81 : 1.7, summary: 'x', relatedOrder: /_1$/.test(k) ? '4169999999' : 'n/a', evidence: ['a', 'b', 'c', 'd', 'e'] })) }) }] };
  } };
  process.env.ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || 'test';
  const r = await CR.run({ lines: [line('4170000001_1', { title: 'Custom Order for Anna' }), line('4170000001_2', { title: 'Tiny Heart Add On Charm', listingId: '1719002', hash: 'hx' })] }, { admin: admin(db), db, anthropic, fetch: async () => ({ ok: false }) });
  assert.match(seen, /this is for my order 4169999999/i, 'the stored conversation is read');
  assert.strictEqual(r.reads['4170000001_1'].kind, 'rework');
  assert.strictEqual(r.reads['4170000001_1'].relatedOrder, '4169999999');
  assert.strictEqual(r.reads['4170000001_2'].confidence, 1, 'confidence is kept between 0 and 1');
  assert.strictEqual(r.reads['4170000001_2'].relatedOrder, null);
  assert.strictEqual(r.reads['4170000001_2'].evidence.length, 4);
  const kept = db.docs.get(CR.COLL + '/4170000001_2');
  assert.ok(kept.reads.hx && kept.reads.hx.kind === 'regular', 'kept under its hash');

  // 3 · lookup: only a reading of exactly these words, and a person's decision
  let got = await CR.lookup(db, [{ key: '4170000001_2', hash: 'hx' }, { key: '4170000001_1', hash: 'changed' }], false);
  assert.ok(got.reads['4170000001_2'] && !got.reads['4170000001_1']);

  // 4 · one decision for every line of a card, in one write; production and the sandbox decide apart; null hands it back
  const d = await CR.decide(db, FV, { keys: ['4170000001_1', '4170000001_2'], kind: 'regular', by: 'Paul' }, false);
  assert.ok(d.ok && d.keys.length === 2 && d.decided.by === 'Paul');
  got = await CR.lookup(db, [{ key: '4170000001_1' }, { key: '4170000001_2' }], false);
  assert.strictEqual(Object.keys(got.decided).length, 2);
  assert.strictEqual(Object.keys((await CR.lookup(db, [{ key: '4170000001_1' }], true)).decided).length, 0, 'the sandbox has its own decisions');
  await CR.decide(db, FV, { key: '4170000001_1', kind: null, by: 'Paul' }, false);
  got = await CR.lookup(db, [{ key: '4170000001_1' }, { key: '4170000001_2' }], false);
  assert.deepStrictEqual(Object.keys(got.decided), ['4170000001_2']);
  assert.ok((await CR.decide(db, FV, { key: 'x', kind: 'bogus' }, false)).error);
  assert.ok((await CR.decide(db, FV, { keys: [] }, false)).error);

  // 5 · no key: nothing is read and nothing is paid for
  const was = process.env.ANTHROPIC_API_KEY; delete process.env.ANTHROPIC_API_KEY;
  assert.ok((await CR.run({ lines: [line('a_1')] }, { admin: admin(db), db, fetch: async () => ({ ok: false }) })).skipped);
  process.env.ANTHROPIC_API_KEY = was;
  console.log('custom-read: ok');
})().catch(e => { console.error(e); process.exit(1); });
