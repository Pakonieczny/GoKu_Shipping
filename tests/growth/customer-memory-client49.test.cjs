'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const memoryAPI = require('../../brites-concierge-memory.js');
const NOW = Date.parse('2026-10-09T13:00:00Z');
const tick = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const account = (uid, extra = {}) => ({ uid, email: uid + '@fixture.example', isAnonymous: false, getIdToken: async () => 'fixture-token-' + uid, ...extra });

function fixture(options = {}) {
  const records = options.records || new Map(), archive = options.archive || new Map(), generations = options.generations || new Map();
  const calls = [], hydrations = [], changes = [], errors = [], timers = new Map(), events = new Map();
  let timestamp = NOW, timerId = 0, listener, local = options.local || { transcript: [] }, loseSync = options.loseSync || false;
  const auth = { currentUser: options.user || null, onAuthStateChanged(fn) { listener = fn; if (!options.deferInitialAuth) fn(this.currentUser); return () => { if (listener === fn) listener = null; }; } };
  const storage = { getItem(key) { return records.get(key) || null; }, setItem(key, value) { records.set(key, value); }, removeItem(key) { records.delete(key); } };
  const env = { sessionStorage: storage, AbortController, crypto: require('node:crypto').webcrypto, addEventListener(name, fn) { events.set(name, fn); }, removeEventListener(name, fn) { if (events.get(name) === fn) events.delete(name); } };
  if (!options.lateFirebase) env.firebase = { apps: [{}], auth: () => auth };
  options.prepareEnvironment?.(env, auth);
  const response = (body, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => structuredClone(body) });
  async function fetch(url, spec) {
    const body = JSON.parse(spec.body), uid = spec.headers.Authorization?.replace('Bearer fixture-token-', '');
    calls.push({ url, body: structuredClone(body), uid, spec });
    if (options.fetch) return options.fetch({ body, uid, spec, response, archive, generations, calls });
    let items = archive.get(uid); if (!items) archive.set(uid, items = new Map());
    const generation = generations.get(uid) || '0';
    if (body.action === 'read') return response({ ok: true, generation, chunks: [...items.values()].filter(row => !body.query || JSON.stringify(row).toLowerCase().includes(body.query.toLowerCase())).slice(-body.limit), preferences: {}, nextCursor: null });
    if (body.action === 'sync') {
      if (body.generation !== generation) return response({ ok: false, resetRequired: true, generation }, 409);
      let saved = 0, duplicates = 0;
      for (const chunk of body.chunks) { if (items.has(chunk.id)) duplicates++; else { items.set(chunk.id, structuredClone(chunk)); saved++; } }
      if (loseSync) { loseSync = false; throw new Error('response disappeared after server commit'); }
      return response({ ok: true, saved, duplicates, generation, syncAfterMs: 60000 });
    }
    if (body.action === 'clear') { archive.set(uid, new Map()); generations.set(uid, 'cleared-generation'); return response({ ok: true, cleared: true, generation: 'cleared-generation' }); }
    if (body.action === 'purchases') return response({ ok: true, purchases: { status: 'verified', items: [{ productId: 'gid://shopify/Product/1', title: 'Fixture necklace', handle: 'fixture-necklace', quantity: 1, purchasedAt: NOW }], source: 'shopify_paid_orders', checkedAt: NOW } });
    throw new Error('unexpected operation');
  }
  const creationOptions = {
    environment: env, storage, fetch, now: () => timestamp,
    setTimeout(fn, ms) { const id = ++timerId; timers.set(id, { fn, at: timestamp + ms }); return id; }, clearTimeout(id) { timers.delete(id); },
    apiBase: 'https://fixture.example', getLocalMemory: () => local,
    onHydrate(value) { hydrations.push(structuredClone(value)); },
    onAccountChanged(value) { changes.push(value); if (options.resetLocalOnChange && value.previousUid) local = { transcript: [] }; },
    onReset(value) { if (options.clearLocalOnReset) local = { transcript: [] }; options.onReset?.(value); },
    onError(value) { errors.push(value); },
    ...(options.client || {})
  };
  const client = memoryAPI.create(creationOptions);
  async function advance(ms) {
    const end = timestamp + ms;
    for (let count = 0; count < 10000; count++) {
      const next = [...timers].filter(([, value]) => value.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!next) break; timestamp = next[1].at; timers.delete(next[0]); next[1].fn(); await tick();
    }
    timestamp = end; await tick();
  }
  function setUser(user) { auth.currentUser = user; listener?.(user); }
  return { client, creationOptions, calls, hydrations, changes, errors, records, archive, generations, auth, env, timers, advance, setUser, setLocal(value) { local = value; }, getLocal() { return local; }, fire(name) { events.get(name)?.(); } };
}

test('guests and anonymous Firebase users keep same-tab memory without cloud or credential writes', async () => {
  for (const user of [null, account('anonymous', { isAnonymous: true })]) {
    const f = fixture({ user, local: { transcript: [{ role: 'user', content: 'A guest gift conversation' }] } });
    assert.deepEqual(f.client.append(f.getLocal().transcript), { queued: 0, remaining: 0, signedIn: false });
    await f.client.sync(); await f.client.read('gifts'); await f.client.relevant('gifts'); await f.client.purchases(); await f.advance(120000);
    assert.equal(f.calls.length, 0); assert.equal(f.records.size, 0); assert.equal(f.client.status().signedIn, false); f.client.dispose();
  }
});

test('a transferred no-email guest custom login remains local despite isAnonymous false', async () => {
  let tokenReads = 0;
  const user = account('transferred-guest', { email: null, isAnonymous: false, getIdToken: async () => { tokenReads++; return 'fixture-token-transferred-guest'; } });
  const f = fixture({ user, local: { transcript: [{ role: 'user', content: 'A transferred guest gift conversation' }] } });
  await f.client.ready(); f.client.append([]); await f.client.sync(); await f.client.read('gift'); await f.client.purchases(); await f.advance(120000);
  assert.equal(f.client.status().signedIn, false); assert.equal(f.calls.length, 0); assert.equal(tokenReads, 0); assert.equal(f.records.size, 0); f.client.dispose();
});

test('late-loaded existing Firebase auth hydrates first and archives the full transcript beyond recent sixteen turns', async () => {
  const text = 'A complete long answer ' + 'x'.repeat(8100), transcript = Array.from({ length: 42 }, (_, i) => ({ role: i % 2 ? 'assistant' : 'user', content: 'Gift turn ' + i }));
  transcript.push({ role: 'assistant', content: text });
  const f = fixture({ lateFirebase: true, user: account('shopper-a'), local: { transcript } });
  assert.equal(f.calls.length, 0);
  let authCalls = 0; f.env.firebase = { apps: [{}], auth() { authCalls++; return f.auth; }, initializeApp() { assert.fail('must reuse existing Firebase auth'); } };
  await f.advance(1000); await f.client.ready();
  assert.equal(f.client.status().uid, 'shopper-a'); assert.equal(f.calls[0].body.action, 'read'); assert.ok(authCalls >= 1);
  const result = await f.client.sync(); assert.ok(result.saved > 0);
  const saved = [...f.archive.get('shopper-a').values()].flatMap(chunk => chunk.messages || []);
  assert.equal(saved.filter(row => row.content.startsWith('Gift turn ')).length, 42);
  assert.equal(saved.slice(42).map(row => row.content).join(''), text);
  assert.ok(f.calls.filter(row => row.body.action === 'sync').every(row => row.body.chunks.length <= 12 && Buffer.byteLength(JSON.stringify(row.body)) <= 65536));
  assert.doesNotMatch([...f.records.values()].join(''), /fixture-token|Authorization/); f.client.dispose();
});

test('lost sync response survives reload and retries the same immutable IDs without duplicate storage', async () => {
  const local = { transcript: [{ role: 'user', content: 'Remember the dolphin necklace for my mother' }] };
  const first = fixture({ user: account('shopper-a'), local, loseSync: true }); await first.client.ready();
  const lost = await first.client.sync(); assert.equal(lost.retry, true); assert.equal(first.client.status().pending, 1);
  const original = first.calls.find(row => row.body.action === 'sync').body.chunks; first.client.dispose();
  const restored = fixture({ user: account('shopper-a'), local, records: first.records, archive: first.archive }); await restored.client.ready();
  const retried = await restored.client.sync(); assert.equal(retried.duplicates, 1); assert.equal(retried.saved, 0);
  assert.deepEqual(restored.calls.find(row => row.body.action === 'sync').body.chunks, original);
  assert.equal(restored.archive.get('shopper-a').size, 1); assert.equal(restored.client.status().pending, 0); restored.client.dispose();
});

test('oversized local source drains through a bounded durable journal without dropping unqueued conversation', async () => {
  const transcript = Array.from({ length: 48 }, (_, i) => ({ role: i % 2 ? 'assistant' : 'user', content: 'Gift entry ' + i + ': ' + 'q'.repeat(1880) }));
  transcript.push({ role: 'assistant', content: 'z'.repeat(50000) });
  const f = fixture({ user: account('shopper-a'), local: { transcript }, client: { maxJournalBytes: 8192 } }); await f.client.ready();
  assert.equal(f.client.status().capacity, true); assert.ok(f.client.status().pendingBytes <= 8192);
  let drained = false;
  for (let i = 0; i < 100; i++) {
    await f.client.sync(); assert.ok(f.client.status().pendingBytes <= 8192);
    if (f.client.status().pending === 0) { drained = true; break; }
  }
  assert.equal(drained, true);
  const saved = [...f.archive.get('shopper-a').values()].flatMap(chunk => chunk.messages || []);
  assert.equal(saved.filter(row => row.content.startsWith('Gift entry ')).length, 48);
  assert.equal(saved.filter(row => /^z+$/.test(row.content)).map(row => row.content).join(''), 'z'.repeat(50000));
  f.client.dispose();
});

test('account changes abort pending reads and isolate hydrated history and queued batches', async () => {
  const slow = deferred();
  const f = fixture({ user: account('shopper-a'), resetLocalOnChange: true, local: { transcript: [{ role: 'user', content: 'Private account A conversation' }] }, fetch: async ({ body, uid, response }) => {
    if (uid === 'shopper-a') return slow.promise;
    if (body.action === 'read') return response({ ok: true, generation: '0', chunks: [{ id: 'b.1', kind: 'conversation', at: NOW, messages: [{ role: 'user', content: 'Only account B history' }] }], preferences: {} });
    if (body.action === 'sync') return response({ ok: true, saved: body.chunks.length, duplicates: 0 });
    throw new Error('unexpected call');
  } });
  await tick(); const oldRequest = f.calls[0]; assert.equal(oldRequest.uid, 'shopper-a');
  f.setUser(account('shopper-b')); await f.client.ready();
  assert.equal(oldRequest.spec.signal.aborted, true); assert.equal(f.client.status().uid, 'shopper-b');
  slow.resolve({ ok: true, status: 200, json: async () => ({ ok: true, generation: '0', chunks: [{ id: 'a.1', kind: 'conversation', at: NOW, messages: [{ role: 'user', content: 'Late account A history' }] }] }) }); await tick();
  assert.equal(f.hydrations.length, 1); assert.equal(f.hydrations[0].uid, 'shopper-b'); assert.match(f.client.cachedRelevant('history')[0].content, /Only account B/);
  f.setLocal({ transcript: [{ role: 'user', content: 'New account B gift' }] }); f.client.append([]); await f.client.sync();
  const synced = f.calls.find(row => row.body.action === 'sync'); assert.equal(synced.uid, 'shopper-b'); assert.doesNotMatch(JSON.stringify(synced.body), /account A/);
  f.setUser(null); assert.deepEqual(f.client.cachedRelevant('history'), []); assert.equal(f.client.status().pending, 0); f.client.dispose();
});

test('an ID token that resolves after an identity change never starts a request for the old shopper', async () => {
  const heldToken = deferred(); const f = fixture({ user: account('shopper-a', { getIdToken: () => heldToken.promise }) });
  f.setUser(account('shopper-b')); await f.client.ready(); heldToken.resolve('fixture-token-shopper-a'); await tick();
  assert.ok(f.calls.length > 0); assert.ok(f.calls.every(row => row.uid === 'shopper-b')); f.client.dispose();
});

test('batching ignores repeated visits and redacts personal details and credentials before durable writes', async () => {
  const content = 'My email is alice@example.com, card 4111 1111 1111 1111, phone +1 (416) 555-1234. I live at 123 Main Street. password=supersecret Bearer hidden-token. https://example.com/details?token=querysecret#fragment A blue gift.';
  const f = fixture({ user: account('shopper-a'), local: { transcript: [{ role: 'user', content }], preferences: { metal: 'gold filled', budget: 60, budgetCurrency: 'CAD', email: 'alice@example.com' } } }); await f.client.ready();
  const reads = f.calls.length;
  for (let i = 0; i < 100; i++) f.client.browse({ pageKind: 'product', handles: ['blue-necklace'], preferences: { email: 'alice@example.com', apiKey: 'supersecret', style: 'blue' } });
  assert.equal(f.calls.length, reads); assert.equal(f.client.status().pending, 2);
  const persisted = [...f.records.values()].join(''); assert.doesNotMatch(persisted, /alice@|4111|555-1234|123 Main|supersecret|hidden-token|fixture-token|querysecret|fragment/);
  await f.advance(59000); assert.equal(f.calls.filter(row => row.body.action === 'sync').length, 0);
  await f.advance(1000); assert.equal(f.calls.filter(row => row.body.action === 'sync').length, 1);
  const batch = f.calls.find(row => row.body.action === 'sync').body;
  assert.doesNotMatch(JSON.stringify(batch), /alice@|4111|supersecret|hidden-token/); assert.equal(batch.chunks.find(row => row.kind === 'browse').preferences.style, 'blue'); f.client.dispose();
  assert.deepEqual(batch.chunks.find(row => row.kind === 'conversation').preferences, { metal: 'gold filled', budget: 60, budgetCurrency: 'CAD' });
});

test('relevant lookup uses the authenticated archive and purchase history stays server verified', async () => {
  const archive = new Map([['shopper-a', new Map([['old.1', { id: 'old.1', kind: 'conversation', at: NOW - 10000, messages: [{ role: 'user', content: 'My mother loves dolphin jewelry.' }] }]])]]);
  const f = fixture({ user: account('shopper-a'), archive }); await f.client.ready();
  const result = await f.client.relevant('dolphin', 6); assert.equal(result[0].content, 'My mother loves dolphin jewelry.');
  assert.equal(f.calls.at(-1).body.query, 'dolphin'); const purchases = await f.client.purchases(); assert.equal(purchases.status, 'verified'); assert.equal(purchases.source, 'shopify_paid_orders');
  assert.equal(f.client.status().pending, 0); f.client.dispose();
});

test('clear switches archive generation and stale local journals cannot resurrect forgotten history', async () => {
  const f = fixture({ user: account('shopper-a'), clearLocalOnReset: true, local: { transcript: [{ role: 'user', content: 'Forgotten account history' }] } }); await f.client.ready(); await f.client.sync();
  assert.equal(await f.client.clear(), true); assert.equal(f.client.status().pending, 0); await f.client.sync();
  assert.equal(f.archive.get('shopper-a').size, 0); assert.deepEqual(f.client.cachedRelevant('history'), []);
  f.setLocal({ transcript: [{ role: 'user', content: 'New conversation after forgetting' }] }); f.client.append([]); await f.client.sync();
  assert.equal(f.calls.at(-1).body.generation, 'cleared-generation'); f.client.dispose();
  const records = new Map([['brites-concierge-cloud-v1:shopper-a', JSON.stringify({ schema: 1, uid: 'shopper-a', stream: 'old-stream', nextSeq: 1, generation: '0', pending: [{ id: 'old-stream.0', kind: 'conversation', at: NOW, messages: [{ role: 'user', content: 'Forgotten account history' }] }], sourceMarks: [], lastBrowse: '' })]]);
  const old = fixture({ user: account('shopper-a'), records, archive: f.archive, generations: f.generations, clearLocalOnReset: true, local: { transcript: [{ role: 'user', content: 'Forgotten account history' }] } }); await old.client.ready();
  assert.equal(old.client.status().pending, 0); await old.client.sync(); assert.equal(old.calls.filter(row => row.body.action === 'sync').length, 0); old.client.dispose();
});

test('token and network deadlines release initial hydration without leaking late work', async () => {
  const token = deferred(); const f = fixture({ user: account('shopper-a', { getIdToken: () => token.promise }), client: { requestTimeoutMs: 1000 } });
  const initial = f.client.ready(); await f.advance(1000); assert.equal(await initial, null);
  assert.equal(f.client.status().ready, false); assert.equal(f.errors.at(-1).code, 'cloud_memory_unavailable');
  token.resolve('fixture-token-shopper-a'); await tick(); assert.equal(f.calls.length, 0); f.client.dispose();
});

test('initial readiness waits for the SDK to restore its saved account before resolving archive hydration', async () => {
  const f = fixture({ deferInitialAuth: true }); let finished = false;
  const ready = f.client.ready().then(result => { finished = true; return result; }); await tick();
  assert.equal(finished, false); assert.equal(f.calls.length, 0);
  f.setUser(account('saved.customer-id')); const hydrated = await ready;
  assert.equal(finished, true); assert.equal(hydrated.uid, 'saved.customer-id'); assert.equal(f.calls[0].uid, 'saved.customer-id');
  assert.equal(f.changes[0].uid, null); f.client.dispose();
});

test('unacknowledged same-account journal rows are available for recall before the next cloud batch', async () => {
  const first = fixture({ user: account('shopper-a'), local: { transcript: [{ role: 'user', content: 'My father loves owl necklaces' }] } }); await first.client.ready(); first.client.dispose();
  const restored = fixture({ user: account('shopper-a'), records: first.records }); const hydrated = await restored.client.ready();
  assert.equal(hydrated.history[0].content, 'My father loves owl necklaces');
  assert.equal(restored.client.cachedRelevant('owl')[0].content, 'My father loves owl necklaces');
  assert.equal(restored.calls.filter(row => row.body.action === 'sync').length, 0); restored.client.dispose();
});

test('an explicitly supplied trusted account provider is not overwritten by an unrelated Firebase guest', async () => {
  let publish, unsubscribed = false;
  const f = fixture({ client: { subscribeAccount(fn) { publish = fn; fn(account('trusted-host-a')); return () => { unsubscribed = true; }; } } });
  await f.client.ready(); assert.equal(f.client.status().uid, 'trusted-host-a');
  await f.advance(3000); assert.equal(f.client.status().uid, 'trusted-host-a');
  publish(account('trusted-host-b')); await f.client.ready(); assert.equal(f.client.status().uid, 'trusted-host-b');
  assert.ok(f.calls.every(row => ['trusted-host-a', 'trusted-host-b'].includes(row.uid))); f.client.dispose(); assert.equal(unsubscribed, true);
});

const PUBLIC_CONFIG = { apiKey: 'fixture-public-api-key-123', projectId: 'fixture-project', authDomain: 'fixture-project.firebaseapp.com', appId: '1:123:web:fixture' };
function sdkEnvironment({ scripts, initialized, existing = false, wrongProject = false, deferScripts = false, existingAppScript = false }) {
  return (env, auth) => {
    auth.signInAnonymously = auth.signInWithCustomToken = auth.setPersistence = () => assert.fail('session restore must not change authentication');
    function app(config) { return { options: structuredClone(config), auth: () => auth }; }
    const sdk = { apps: existing ? [app(wrongProject ? { ...PUBLIC_CONFIG, projectId: 'other-project' } : PUBLIC_CONFIG)] : [], initializeApp(config) { initialized.push(structuredClone(config)); const value = app(config); sdk.apps.push(value); return value; } };
    if (existing) { sdk.auth = () => auth; env.firebase = sdk; }
    const waiting = [];
    function finish(node) {
      if (node.src.endsWith('firebase-app-compat.js')) env.firebase = sdk;
      if (node.src.endsWith('firebase-auth-compat.js')) sdk.auth = () => auth;
      node.onload();
    }
    env.completeNextSdk = () => finish(waiting.shift());
    env.document = { createElement() { return { remove() {} }; }, head: { appendChild(node) {
      scripts.push(node.src);
      if (deferScripts) waiting.push(node); else finish(node);
    } } };
    if (existingAppScript) {
      const listeners = new Map(), node = { src: 'https://www.gstatic.com/firebasejs/9.23.0/firebase-app-compat.js', addEventListener(name, fn) { listeners.set(name, fn); }, removeEventListener(name) { listeners.delete(name); } };
      env.document.querySelectorAll = () => [node]; env.completeExistingSdk = () => { env.firebase = sdk; listeners.get('load')(); };
    }
  };
}

test('opt-in public config restores a saved session with pinned official SDKs and no sign-in or persistence changes', async () => {
  const scripts = [], initialized = [];
  const f = fixture({ lateFirebase: true, user: account('saved-shopper'), prepareEnvironment: sdkEnvironment({ scripts, initialized }), client: { firebaseConfig: PUBLIC_CONFIG, firebaseVersion: '9.23.0' } });
  const hydrated = await f.client.ready(); assert.equal(hydrated.uid, 'saved-shopper');
  assert.equal(f.changes[0].identityPending, true); assert.equal(f.changes.at(-1).identityPending, false);
  assert.deepEqual(scripts, ['https://www.gstatic.com/firebasejs/9.23.0/firebase-app-compat.js', 'https://www.gstatic.com/firebasejs/9.23.0/firebase-auth-compat.js']);
  assert.deepEqual(initialized, [PUBLIC_CONFIG]); assert.equal(f.env.firebase.apps.length, 1);
  const second = memoryAPI.create(f.creationOptions); await second.ready();
  assert.equal(scripts.length, 2); assert.equal(initialized.length, 1); assert.equal(second.status().uid, 'saved-shopper');
  assert.doesNotMatch([...f.records.values()].join(''), /apiKey|fixture-token/); second.dispose(); f.client.dispose();
});

test('opt-in restore with no saved member leaves guests and anonymous users without cloud requests', async () => {
  for (const user of [null, account('anonymous', { isAnonymous: true })]) {
    const scripts = [], initialized = [];
    const f = fixture({ lateFirebase: true, user, prepareEnvironment: sdkEnvironment({ scripts, initialized }), client: { firebaseConfig: PUBLIC_CONFIG } });
    await f.client.ready(); await f.advance(60000); assert.equal(scripts.length, 2); assert.equal(initialized.length, 1);
    assert.equal(f.changes[0].identityPending, true); assert.equal(f.changes.at(-1).identityPending, false);
    assert.equal(f.calls.length, 0); assert.equal(f.records.size, 0); assert.equal(f.client.status().signedIn, false); f.client.dispose();
  }
});

test('matching existing Firebase app is reused without another SDK, app or auth flow', async () => {
  const scripts = [], initialized = [];
  const f = fixture({ user: account('saved-shopper'), prepareEnvironment: sdkEnvironment({ scripts, initialized, existing: true }), client: { firebaseConfig: PUBLIC_CONFIG } });
  await f.client.ready(); assert.equal(f.client.status().uid, 'saved-shopper'); assert.equal(scripts.length, 0); assert.equal(initialized.length, 0); f.client.dispose();
});

test('simultaneous client restoration shares one SDK load and one app initialization', async () => {
  const scripts = [], initialized = [];
  const f = fixture({ lateFirebase: true, user: account('saved-shopper'), prepareEnvironment: sdkEnvironment({ scripts, initialized, deferScripts: true }), client: { firebaseConfig: PUBLIC_CONFIG } });
  const second = memoryAPI.create(f.creationOptions), firstReady = f.client.ready(), secondReady = second.ready();
  assert.equal(scripts.length, 1); f.env.completeNextSdk(); await tick(); assert.equal(scripts.length, 2);
  f.env.completeNextSdk(); await Promise.all([firstReady, secondReady]);
  assert.equal(initialized.length, 1); assert.equal(scripts.length, 2); assert.equal(second.status().uid, 'saved-shopper'); second.dispose(); f.client.dispose();
});

test('a pinned Firebase SDK already loading for the studio is observed without injecting it again', async () => {
  const scripts = [], initialized = [];
  const f = fixture({ lateFirebase: true, user: account('saved-shopper'), prepareEnvironment: sdkEnvironment({ scripts, initialized, existingAppScript: true }), client: { firebaseConfig: PUBLIC_CONFIG } });
  const ready = f.client.ready(); assert.equal(scripts.length, 0); f.env.completeExistingSdk(); await ready;
  assert.deepEqual(scripts, ['https://www.gstatic.com/firebasejs/9.23.0/firebase-auth-compat.js']);
  assert.equal(initialized.length, 1); assert.equal(f.client.status().uid, 'saved-shopper'); f.client.dispose();
});

test('an existing app for the wrong project blocks opt-in restore before credentials or cloud calls', async () => {
  const scripts = [], initialized = [];
  const f = fixture({ user: account('wrong-project-shopper'), prepareEnvironment: sdkEnvironment({ scripts, initialized, existing: true, wrongProject: true }), client: { firebaseConfig: PUBLIC_CONFIG } });
  await f.client.ready(); await f.advance(2000);
  assert.equal(f.client.status().signedIn, false); assert.equal(f.client.status().error, 'account_restore_project_mismatch');
  assert.equal(f.calls.length, 0); assert.equal(scripts.length, 0); assert.equal(initialized.length, 0); assert.equal(f.env.firebase.apps.length, 1); f.client.dispose();
});

test('explicit clear continues bounded physical cleanup without rotating generation again', async () => {
  let cleanup = 3, resets = 0;
  const f = fixture({ user: account('shopper-a'), clearLocalOnReset: true, onReset() { resets++; }, fetch: async ({ body, response }) => {
    if (body.action === 'read') return response({ ok: true, generation: '0', chunks: [] });
    if (body.action === 'clear') return response({ ok: true, generation: 'cleared-generation', cleared: true, cleanupPending: body.cleanup ? --cleanup > 0 : true });
    throw new Error('unexpected operation');
  } });
  await f.client.ready(); assert.equal(await f.client.clear(), true);
  const clears = f.calls.filter(row => row.body.action === 'clear'); assert.equal(clears.length, 4); assert.equal(resets, 1);
  assert.deepEqual(clears[0].body, { action: 'clear' }); assert.ok(clears.slice(1).every(row => row.body.cleanup === true && row.body.generation === 'cleared-generation'));
  assert.equal(f.client.status().cleanupPending, false); f.client.dispose();
});

test('bounded cleanup reports remaining work and resumes only when explicitly asked', async () => {
  let stop = true, resets = 0;
  const f = fixture({ user: account('shopper-a'), onReset() { resets++; }, fetch: async ({ body, response }) => {
    if (body.action === 'read') return response({ ok: true, generation: '0', chunks: [] });
    if (body.action === 'clear' && !body.cleanup) return response({ ok: true, generation: 'cleared-generation', cleared: true, cleanupPending: true });
    if (body.action === 'clear') return stop ? response({ ok: false, error: 'daily cleanup budget' }, 429) : response({ ok: true, generation: 'cleared-generation', cleared: true, cleanupPending: false });
    throw new Error('unexpected operation');
  } });
  await f.client.ready(); assert.equal(await f.client.clear(), true); assert.equal(f.client.status().cleanupPending, true); assert.equal(f.client.status().error, 'history_cleanup_pending');
  const count = f.calls.length; await f.advance(60000); assert.equal(f.calls.length, count);
  stop = false; assert.equal(await f.client.clear({ cleanup: true }), true); assert.equal(f.client.status().cleanupPending, false); assert.equal(resets, 1); f.client.dispose();
});

test('specific recall continues empty archive windows until older current-generation context is found', async () => {
  const f = fixture({ user: account('shopper-a'), fetch: async ({ body, response }) => {
    if (!body.query) return response({ ok: true, generation: 'current-generation', chunks: [], nextCursor: 'recent-page' });
    if (!body.cursor) return response({ ok: true, generation: 'current-generation', chunks: [], nextCursor: 'window100' });
    if (body.cursor === 'window100') return response({ ok: true, generation: 'current-generation', chunks: [], nextCursor: 'window200' });
    assert.equal(body.cursor, 'window200');
    return response({ ok: true, generation: 'current-generation', chunks: [{ id: 'older.201', kind: 'conversation', at: NOW - 200000, messages: [{ role: 'user', content: 'The dormouse necklace was a graduation gift for my grandmother.' }] }], nextCursor: 'window300' });
  } });
  await f.client.ready();
  const found = await f.client.relevant('dormouse', 6);
  assert.equal(found.length, 1); assert.match(found[0].content, /graduation gift for my grandmother/);
  const queryReads = f.calls.filter(call => call.body.query); assert.equal(queryReads.length, 3);
  assert.deepEqual(queryReads.map(call => call.body.cursor || null), [null, 'window100', 'window200']);
  assert.ok(queryReads.every(call => call.body.query === 'dormouse' && call.body.limit === 20));
  assert.deepEqual(Object.fromEntries(['searchLimited', 'searchCursor', 'searchQuery', 'searchPages'].map(key => [key, f.client.status()[key]])), { searchLimited: true, searchCursor: 'window300', searchQuery: 'dormouse', searchPages: 3 });
  const requests = f.calls.length; assert.equal((await f.client.relevant('', 6))[0].content, found[0].content);
  assert.equal(f.calls.length, requests); f.client.dispose();
});

test('recall stops at four archive windows and exposes a bounded cursor for explicit continuation', async () => {
  const f = fixture({ user: account('shopper-a'), client: { maxLookupPages: 90 }, fetch: async ({ body, response }) => {
    if (!body.query) return response({ ok: true, generation: '0', chunks: [], nextCursor: null });
    const scanned = Number((body.cursor || 'window0').slice(6)) + 100;
    if (scanned === 500) return response({ ok: true, generation: '0', chunks: [{ id: 'older.401', kind: 'conversation', at: NOW - 400000, messages: [{ role: 'user', content: 'We chose an agate pendant for the anniversary.' }] }], nextCursor: null });
    return response({ ok: true, generation: '0', chunks: [], nextCursor: 'window' + scanned });
  } });
  await f.client.ready(); assert.deepEqual(await f.client.relevant('agate', 6), []);
  assert.equal(f.calls.filter(call => call.body.query).length, 4); assert.equal(f.client.status().searchLimited, true); assert.equal(f.client.status().searchCursor, 'window400');
  const found = await f.client.relevant('agate', 6, { cursor: f.client.status().searchCursor });
  assert.match(found[0].content, /agate pendant/); assert.equal(f.calls.filter(call => call.body.query).length, 5);
  assert.equal(f.client.status().searchLimited, false); assert.equal(f.client.status().searchCursor, null); assert.equal(f.client.status().searchPages, 1); f.client.dispose();
});

test('an account change during paginated recall stops the old search and prevents late context hydration', async () => {
  const held = deferred();
  const f = fixture({ user: account('shopper-a'), resetLocalOnChange: true, fetch: async ({ body, uid, response }) => {
    if (uid === 'shopper-b') return response({ ok: true, generation: 'b-generation', chunks: [{ id: 'b.1', kind: 'conversation', at: NOW, messages: [{ role: 'user', content: 'Account B chose a garnet necklace.' }] }], nextCursor: null });
    if (!body.query) return response({ ok: true, generation: 'a-generation', chunks: [], nextCursor: null });
    if (!body.cursor) return response({ ok: true, generation: 'a-generation', chunks: [], nextCursor: 'a-window100' });
    assert.equal(body.cursor, 'a-window100'); return held.promise;
  } });
  await f.client.ready(); const lookup = f.client.relevant('dormouse', 6); await tick(); await tick();
  assert.equal(f.calls.filter(call => call.body.query).length, 2); const oldRequest = f.calls.at(-1);
  f.setUser(account('shopper-b')); await f.client.ready(); assert.deepEqual(await lookup, []); assert.equal(oldRequest.spec.signal.aborted, true);
  const hydrationCount = f.hydrations.length;
  held.resolve({ ok: true, status: 200, json: async () => ({ ok: true, generation: 'a-generation', chunks: [{ id: 'a.old', kind: 'conversation', at: NOW, messages: [{ role: 'user', content: 'Private dormouse gift from account A' }] }], nextCursor: 'a-window200' }) }); await tick();
  assert.equal(f.hydrations.length, hydrationCount); assert.deepEqual(f.client.cachedRelevant('dormouse'), []);
  assert.equal(f.calls.filter(call => call.uid === 'shopper-a' && call.body.query).length, 2);
  assert.equal(f.client.status().uid, 'shopper-b'); assert.equal(f.client.status().searchQuery, ''); assert.equal(f.client.status().searchPages, 0); f.client.dispose();
});

test('a caller abort cancels the pending archive page without a late hydration or another lookup page', async () => {
  const held = deferred(), abort = new AbortController();
  const f = fixture({ user: account('shopper-a'), fetch: async ({ body, response }) => {
    if (!body.query) return response({ ok: true, generation: '0', chunks: [], nextCursor: null });
    if (!body.cursor) return response({ ok: true, generation: '0', chunks: [], nextCursor: 'window100' });
    return held.promise;
  } });
  await f.client.ready(); const lookup = f.client.relevant('dormouse', 6, { signal: abort.signal }); await tick(); await tick();
  const pending = f.calls.at(-1), hydrationCount = f.hydrations.length; assert.equal(pending.body.cursor, 'window100');
  abort.abort(); assert.deepEqual(await lookup, []); assert.equal(pending.spec.signal.aborted, true);
  held.resolve({ ok: true, status: 200, json: async () => ({ ok: true, generation: '0', chunks: [{ id: 'late.1', kind: 'conversation', at: NOW, messages: [{ role: 'user', content: 'Late dormouse gift' }] }], nextCursor: 'window200' }) }); await tick();
  assert.equal(f.hydrations.length, hydrationCount); assert.equal(f.calls.filter(call => call.body.query).length, 2); assert.deepEqual(f.client.cachedRelevant('dormouse'), []); f.client.dispose();
});

test('all lookup pages share one deadline rather than extending the archive wait for each window', async () => {
  const held = deferred(); let f;
  f = fixture({ user: account('shopper-a'), client: { lookupTimeoutMs: 1000 }, fetch: async ({ body, response }) => {
    if (!body.query) return response({ ok: true, generation: '0', chunks: [], nextCursor: null });
    if (!body.cursor) { await f.advance(950); return response({ ok: true, generation: '0', chunks: [], nextCursor: 'window100' }); }
    return held.promise;
  } });
  await f.client.ready(); let settled = false; const lookup = f.client.relevant('dormouse', 6).then(value => { settled = true; return value; }); await tick(); await tick();
  assert.equal(f.calls.filter(call => call.body.query).length, 2); await f.advance(49); assert.equal(settled, false);
  await f.advance(1); assert.deepEqual(await lookup, []); assert.equal(f.calls.at(-1).spec.signal.aborted, true);
  assert.equal(f.client.status().searchLimited, true); assert.equal(f.client.status().searchCursor, 'window100'); assert.equal(f.client.status().searchPages, 2);
  const hydrationCount = f.hydrations.length; held.resolve({ ok: true, status: 200, json: async () => ({ ok: true, generation: '0', chunks: [{ id: 'late.2', kind: 'conversation', at: NOW, messages: [{ role: 'user', content: 'Late dormouse gift' }] }], nextCursor: null }) }); await tick();
  assert.equal(f.hydrations.length, hydrationCount); assert.equal(f.calls.filter(call => call.body.query).length, 2); f.client.dispose();
});

test('a generation reset on a continuation page discards the old cache and stops the old cursor search', async () => {
  let resets = 0;
  const f = fixture({ user: account('shopper-a'), clearLocalOnReset: true, onReset() { resets++; }, local: { transcript: [{ role: 'user', content: 'Old private peony preference' }] }, fetch: async ({ body, response }) => {
    if (!body.query) return response({ ok: true, generation: '0', chunks: [{ id: 'old.1', kind: 'conversation', at: NOW - 1000, messages: [{ role: 'user', content: 'Old private peony necklace' }] }], nextCursor: null });
    if (!body.cursor) return response({ ok: true, generation: '0', chunks: [], nextCursor: 'old-window100' });
    return response({ ok: false, resetRequired: true, generation: 'new-generation' }, 409);
  } });
  await f.client.ready(); assert.ok(f.client.cachedRelevant('peony').length > 0);
  assert.deepEqual(await f.client.relevant('dormouse', 6), []); assert.equal(resets, 1);
  assert.equal(f.calls.filter(call => call.body.query).length, 2); assert.deepEqual(f.client.cachedRelevant('peony'), []); assert.equal(f.client.status().pending, 0);
  assert.equal(f.client.status().searchLimited, true); assert.equal(f.client.status().searchCursor, null); f.client.dispose();
});
