// End-to-end OAuth test: drives the real browser code in SKU_List_V1.html into
// the real netlify/functions/exchangeToken.js handler, with a fake Etsy token
// endpoint that verifies PKCE exactly the way Etsy does.
// Run: node tests/sku/exchange-token.cjs
const assert = require('node:assert');
const nodeCrypto = require('node:crypto');
const Module = require('node:module');
const { createApp } = require('./dom-harness.cjs');

/* node-fetch is a production dependency; the repo has no node_modules here and
   the test must not reach the network anyway, so the require is intercepted. */
let etsyCall = null;
let etsyReply = null;
const origLoad = Module._load;
Module._load = function (request, parent, isMain) {
  if (request === 'node-fetch') {
    return async (url, init = {}) => {
      etsyCall = { url: String(url), body: String(init.body || ''), headers: init.headers || {} };
      const r = etsyReply();
      return {
        ok: r.status >= 200 && r.status < 300,
        status: r.status,
        json: async () => r.body,
      };
    };
  }
  return origLoad.call(this, request, parent, isMain);
};

const exchangeToken = require('../../netlify/functions/exchangeToken.js');

const CLIENT_ID = 'test-client-id';
const CLIENT_SECRET = 'test-client-secret';
process.env.CLIENT_ID = CLIENT_ID;
process.env.CLIENT_SECRET = CLIENT_SECRET;

function challengeFor(verifier) {
  return nodeCrypto.createHash('sha256').update(verifier).digest('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

/* Stands in for https://api.etsy.com/v3/public/oauth/token. Rejects with the
   exact payload the user reported when the PKCE pair does not line up. */
function etsyTokenEndpoint({ expectChallenge, expectRedirectUri }) {
  return () => {
    const form = new URLSearchParams(etsyCall.body);
    if (form.get('grant_type') !== 'authorization_code') {
      return { status: 400, body: { error: 'unsupported_grant_type' } };
    }
    if (form.get('client_id') !== CLIENT_ID || form.get('client_secret') !== CLIENT_SECRET) {
      return { status: 401, body: { error: 'invalid_client' } };
    }
    if (form.get('redirect_uri') !== expectRedirectUri) {
      return { status: 400, body: { error: 'invalid_grant', error_description: 'redirect_uri is invalid' } };
    }
    if (challengeFor(form.get('code_verifier') || '') !== expectChallenge) {
      return { status: 400, body: { error: 'invalid_grant', error_description: 'code_verifier is invalid' } };
    }
    return { status: 200, body: { access_token: 'AT-live', refresh_token: 'RT-live', expires_in: 3600 } };
  };
}

const evt = (qs, host = 'sku.goldenspike.app') => ({
  httpMethod: 'GET',
  headers: { host, 'x-forwarded-host': host },
  queryStringParameters: Object.fromEntries(new URLSearchParams(qs)),
});

let passed = 0;
const failures = [];
const cases = [];
const test = (name, fn) => cases.push({ name, fn });

/* ── the full loop ──────────────────────────────────────────────────────── */

test('Full handshake: Connect → Etsy callback → exchangeToken → tokens on the page', async () => {
  // 1. Browser starts the handshake.
  const a = createApp();
  await a.domReady();
  await a.els.connectEtsyBtn.dispatch('click');
  const authorize = new URL(a.navigations.at(-1)).searchParams;
  const challenge = authorize.get('code_challenge');
  const redirectUri = authorize.get('redirect_uri');
  a.cleanup();

  // 2. Etsy bounces the browser back to the bound origin with code + state.
  const back = createApp({
    storage: a.localStorage._dump(),
    search: `?code=AUTHCODE&state=${encodeURIComponent(authorize.get('state'))}`,
  });
  assert.equal(back.api.bootOAuth(), 'exchange');
  const exchangeUrl = new URL(back.navigations.at(-1));
  assert.equal(exchangeUrl.pathname, '/.netlify/functions/exchangeToken');
  back.cleanup();

  // 3. The real Netlify function runs against a PKCE-checking Etsy.
  etsyReply = etsyTokenEndpoint({ expectChallenge: challenge, expectRedirectUri: redirectUri });
  const res = await exchangeToken.handler(evt(exchangeUrl.search));

  assert.equal(res.statusCode, 302, 'body was: ' + res.body);
  assert.equal(etsyCall.url, 'https://api.etsy.com/v3/public/oauth/token');
  const sent = new URLSearchParams(etsyCall.body);
  assert.equal(sent.get('redirect_uri'), redirectUri,
    'token redirect_uri must be byte-identical to the authorize one');
  assert.equal(challengeFor(sent.get('code_verifier')), challenge);

  // 4. The browser lands back on the app with usable tokens.
  const final = new URL(res.headers.Location);
  assert.equal(final.origin, 'https://sku.goldenspike.app');
  const done = createApp({ search: final.search });
  assert.equal(done.api.bootOAuth(), 'tokens');
  assert.equal(done.api.getAccessToken(), 'AT-live');
  assert.equal(done.api.getRefreshToken(), 'RT-live');
  assert.ok(done.api.tokenIsFresh());
  done.cleanup();
});

test('REGRESSION: the double-Connect sequence no longer fails PKCE', async () => {
  // Reproduces the reported failure: Connect pressed twice (or a second tab),
  // then the FIRST authorization comes back.
  const a = createApp();
  await a.domReady();
  await a.els.connectEtsyBtn.dispatch('click');
  const first = new URL(a.navigations.at(-1)).searchParams;

  const b = createApp({ storage: a.localStorage._dump() });
  await b.domReady();
  await b.els.connectEtsyBtn.dispatch('click');
  const second = new URL(b.navigations.at(-1)).searchParams;
  assert.notEqual(first.get('code_challenge'), second.get('code_challenge'));

  const back = createApp({
    storage: b.localStorage._dump(),
    search: `?code=CODE_FROM_FIRST&state=${encodeURIComponent(first.get('state'))}`,
  });
  back.api.bootOAuth();
  const exchangeUrl = new URL(back.navigations.at(-1));

  etsyReply = etsyTokenEndpoint({
    expectChallenge: first.get('code_challenge'),
    expectRedirectUri: first.get('redirect_uri'),
  });
  const res = await exchangeToken.handler(evt(exchangeUrl.search));
  assert.equal(res.statusCode, 302, 'expected success, got: ' + res.body);
  [a, b, back].forEach(x => x.cleanup());
});

test('A mismatched verifier still surfaces Etsy\'s own error verbatim', async () => {
  // Guards the fake Etsy itself: if PKCE checking were a no-op every test above
  // would pass for the wrong reason.
  etsyReply = etsyTokenEndpoint({
    expectChallenge: challengeFor('the-right-one'),
    expectRedirectUri: 'https://sku.goldenspike.app',
  });
  const res = await exchangeToken.handler(evt('code=C&code_verifier=the-wrong-one&redirect_domain=sku'));
  assert.equal(res.statusCode, 400);
  assert.deepEqual(JSON.parse(res.body), {
    error: 'invalid_grant',
    error_description: 'code_verifier is invalid',
  });
});

test('redirect_domain=sku and the sku host both resolve to the bound origin', async () => {
  const expect = { expectChallenge: challengeFor('v'), expectRedirectUri: 'https://sku.goldenspike.app' };
  etsyReply = etsyTokenEndpoint(expect);

  const viaParam = await exchangeToken.handler(evt('code=C&code_verifier=v&redirect_domain=sku', 'goldenspike.app'));
  assert.equal(viaParam.statusCode, 302, viaParam.body);

  const viaHost = await exchangeToken.handler(evt('code=C&code_verifier=v'));
  assert.equal(viaHost.statusCode, 302, viaHost.body);

  // And the wrong domain must NOT silently succeed.
  const viaWrong = await exchangeToken.handler(evt('code=C&code_verifier=v&redirect_domain=design'));
  assert.equal(viaWrong.statusCode, 400);
  assert.equal(JSON.parse(viaWrong.body).error_description, 'redirect_uri is invalid');
});

test('A missing code_verifier is rejected before any Etsy call', async () => {
  etsyCall = null;
  etsyReply = () => { throw new Error('must not call Etsy'); };
  const res = await exchangeToken.handler(evt('code=C&redirect_domain=sku'));
  assert.equal(res.statusCode, 400);
  assert.match(JSON.parse(res.body).error, /Missing code_verifier/);
  assert.equal(etsyCall, null);
});

test('The redirect back to the app carries everything the page needs', async () => {
  etsyReply = etsyTokenEndpoint({
    expectChallenge: challengeFor('v'),
    expectRedirectUri: 'https://sku.goldenspike.app',
  });
  const res = await exchangeToken.handler(evt('code=C&code_verifier=v&redirect_domain=sku'));
  const q = new URL(res.headers.Location).searchParams;
  for (const k of ['access_token', 'refresh_token', 'expires_in', 'issued_at']) {
    assert.ok(q.get(k), 'missing ' + k);
  }
  const issued = Number(q.get('issued_at'));
  assert.ok(Math.abs(issued - Math.floor(Date.now() / 1000)) < 5, 'issued_at is server time');
});

(async () => {
  console.log('\nexchangeToken · end-to-end OAuth');
  for (const c of cases) {
    try { await c.fn(); passed++; console.log('  ✓ ' + c.name); }
    catch (err) { failures.push(c.name); console.log('  ✗ ' + c.name + '\n      ' + ((err && err.stack) || err)); }
  }
  console.log(`\n${passed} passed, ${failures.length} failed`);
  if (failures.length) process.exitCode = 1;
})();
