// Google API keys live in Firestore, not in Netlify environment variables,
// which are a limited resource on this site. A key that cannot be read must be
// distinguishable from a key that was never added.
const assert = require('assert/strict'), path = require('path');
const FN = path.resolve(__dirname, '../../netlify/functions');
const keys = require(path.join(FN, '_googleApiKeys.js'));
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

const store = data => ({ db: { doc: p => ({ get: async () => { if (data instanceof Error) throw data; return { exists: !!data, data: () => data }; } }) } });

(async () => {
  check(keys.envNameFor('youtubeApiKey') === 'YOUTUBE_API_KEY', 'a Firestore field maps to its conventional environment name');
  check(keys.DOC_PATH === 'config/googleApiKeys', 'keys are read from one documented location');

  keys.resetCache();
  const found = await keys.googleApiKeyStatus('youtubeApiKey', { ...store({ youtubeApiKey: 'AIza-stored' }), env: {} });
  check(found.key === 'AIza-stored' && /Firestore/.test(found.source), 'a key stored in Firestore is found, and its source is named');

  // An environment variable still wins, so nothing that already works breaks.
  keys.resetCache();
  const overridden = await keys.googleApiKeyStatus('youtubeApiKey', { ...store({ youtubeApiKey: 'AIza-stored' }), env: { YOUTUBE_API_KEY: 'AIza-env' } });
  check(overridden.key === 'AIza-env' && /environment/.test(overridden.source), 'an environment variable of the same name still takes precedence');

  keys.resetCache();
  const absent = await keys.googleApiKeyStatus('youtubeApiKey', { ...store({}), env: {} });
  check(absent.key === null && absent.error === null, 'a key that was never added reports absent, with no error');

  // Firestore being unreachable is not the same as the key being absent: one
  // says "add it", the other says "the store is down".
  keys.resetCache();
  const broken = await keys.googleApiKeyStatus('youtubeApiKey', { ...store(new Error('permission denied')), env: {} });
  check(broken.key === null && /permission denied/.test(broken.error), 'an unreadable key store reports its reason rather than looking empty');

  keys.resetCache();
  const rescued = await keys.googleApiKeyStatus('youtubeApiKey', { ...store(new Error('offline')), env: { YOUTUBE_API_KEY: 'AIza-env' } });
  check(rescued.key === 'AIza-env', 'a Firestore outage never hides a key that is already in the environment');

  keys.resetCache();
  const blank = await keys.googleApiKeyStatus('youtubeApiKey', { ...store({ youtubeApiKey: '   ' }), env: {} });
  check(blank.key === null, 'a blank stored value is treated as absent, not as a key');

  // One read per process per window, shared by concurrent callers.
  keys.resetCache();
  let reads = 0;
  const counting = { db: { doc: () => ({ get: async () => { reads++; return { exists: true, data: () => ({ youtubeApiKey: 'AIza-1' }) }; } }) }, env: {} };
  await Promise.all([keys.googleApiKey('youtubeApiKey', counting), keys.googleApiKey('youtubeApiKey', counting), keys.googleApiKey('youtubeApiKey', counting)]);
  check(reads === 1, 'concurrent callers share a single Firestore read');

  console.log(passed + ' Google API key store checks passed.');
})().catch(e => { console.error(e); process.exitCode = 1; });
