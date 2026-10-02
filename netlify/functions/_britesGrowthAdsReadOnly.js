'use strict';

// Legacy Ads reads are allowed to observe production Firestore, never to update
// it. Some reads try to save an optional cache; this adapter blocks that write
// while allowing the engine's existing cache-failure handling to keep the data.
const WRITE_METHODS = new Set(['set', 'update', 'delete', 'add', 'create', 'commit',
  'recursiveDelete', 'bulkWriter', 'batch', 'terminate', 'settings']);
function readOnlyFirestore(db) {
  const proxies = new WeakMap(), originals = new WeakMap();
  const original = value => value && typeof value === 'object' ? originals.get(value) || value : value;
  function wrap(value) {
    if (!value || typeof value !== 'object') return value;
    if (Array.isArray(value)) return value.map(wrap);
    // Query/reference/snapshot/transaction instances need their methods bound.
    // Data, timestamps and Dates stay ordinary Firestore return values.
    if (value instanceof Date || value.constructor === Object || value.constructor?.name === 'Timestamp' || Buffer.isBuffer(value)) return value;
    if (proxies.has(value)) return proxies.get(value);
    const proxy = new Proxy(value, {
      get(target, key) {
        if (WRITE_METHODS.has(key)) return () => { throw Error('Production Firestore writes are disabled in the growth Ads sandbox.'); };
        if (key === 'runTransaction') return async fn => target.runTransaction(tx => fn(wrap(tx)), { maxAttempts: 1 });
        if (key === 'onSnapshot') return () => { throw Error('Use bounded report reads in the growth Ads sandbox.'); };
        const found = Reflect.get(target, key, target);
        if (typeof found !== 'function') return wrap(found);
        if (key === 'data') return (...args) => found.apply(target, args.map(original));
        if (key === 'forEach') return fn => found.call(target, entry => fn(wrap(entry)));
        return (...args) => {
          const result = found.apply(target, args.map(original));
          return result && typeof result.then === 'function' ? result.then(wrap) : wrap(result);
        };
      }
    });
    proxies.set(value, proxy); originals.set(proxy, value); return proxy;
  }
  return wrap(db);
}
function adminFromEnvironment(env) {
  const admin = require('firebase-admin');
  if (!admin.apps.length) admin.initializeApp({ credential: admin.credential.cert({
    projectId: env.FIREBASE_PROJECT_ID, clientEmail: env.FIREBASE_CLIENT_EMAIL,
    privateKey: String(env.FIREBASE_PRIVATE_KEY || '').replace(/\\n/g, '\n')
  }), storageBucket: env.FIREBASE_STORAGE_BUCKET || 'gokudatabase.firebasestorage.app' });
  const db = readOnlyFirestore(admin.firestore());
  const firestore = new Proxy(admin.firestore, { apply() { return db; } });
  return new Proxy(admin, { get(target, key) { return key === 'firestore' ? firestore : Reflect.get(target, key, target); } });
}
module.exports = { readOnlyFirestore, adminFromEnvironment };
