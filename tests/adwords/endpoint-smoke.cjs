// Every check endpoint is executed end to end against stubbed network and
// storage. A syntax check cannot catch a function that is called but never
// defined — that exact defect shipped once, in the transport row — and only
// running the handler finds it.
const assert = require('assert/strict'), path = require('path'), Module = require('module');
const FN = path.resolve(__dirname, '../../netlify/functions');
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

// ── A Google, Shopify and Merchant shaped enough to answer every probe ──────
const reply = (status, body, headers) => ({
  ok: status < 400, status,
  json: async () => body,
  text: async () => JSON.stringify(body),
  headers: { get: k => (headers || {})[k] || null }
});
let fetchCalls = 0, dbCalls = 0; // a refused request must reach neither the network nor storage
function stubFetch(url, options) {
  fetchCalls++;
  const body = options && options.body ? String(options.body) : '';
  if (/oauth2\.googleapis\.com\/token/.test(url)) return Promise.resolve(reply(200, { access_token: 'tok', expires_in: 3600, scope: 'https://www.googleapis.com/auth/adwords https://www.googleapis.com/auth/datamanager https://www.googleapis.com/auth/cloud-platform' }));
  if (/tokeninfo/.test(url)) return Promise.resolve(reply(200, { scope: 'https://www.googleapis.com/auth/adwords' }));
  if (/listAccessibleCustomers/.test(url)) return Promise.resolve(reply(200, { resourceNames: ['customers/1234567890'] }));
  if (/googleAdsFields:search/.test(url)) {
    const asked = [...body.matchAll(/'([a-z0-9_.]+)'/g)].map(m => m[1]);
    return Promise.resolve(reply(200, { results: asked.map(name => ({ name, category: 'METRIC', dataType: 'DOUBLE', selectable: true, selectableWith: ['campaign'] })) }));
  }
  if (/googleAds:mutate/.test(url)) return Promise.resolve(reply(200, {}));
  if (/googleAds:search/.test(url)) return Promise.resolve(reply(200, { results: [{ campaign: { id: '55', shoppingSetting: { merchantId: '999' } }, asset: { youtubeVideoAsset: { youtubeVideoId: 'dQw4w9WgXcQ' } } }] }));
  if (/merchantapi\.googleapis\.com/.test(url)) return Promise.resolve(reply(200, { accountIssues: [], results: [], dataSources: [], conversionSources: [], accountRelationships: [], promotions: [], onlineReturnPolicies: [], services: [], enableProducts: false }));
  if (/generativelanguage/.test(url)) return Promise.resolve(reply(200, { models: [{ name: 'models/veo-3' }] }));
  if (/youtube\.googleapis\.com/.test(url)) return Promise.resolve(reply(200, { items: [] }));
  if (/datamanager\.googleapis\.com/.test(url)) return Promise.resolve(reply(200, { requestId: 'r1' }));
  if (/admin\/oauth\/access_token/.test(url)) return Promise.resolve(reply(200, { access_token: 'shop' }));
  if (/admin\/api\/.*\/shop\.json/.test(url)) return Promise.resolve(reply(200, { shop: { myshopify_domain: 'x.myshopify.com', currency: 'USD' } }));
  if (/webhooks\.json/.test(url)) return Promise.resolve(reply(200, { webhooks: [{ topic: 'orders/paid', address: 'https://goldenspike.app/h' }, { topic: 'refunds/create', address: 'https://goldenspike.app/h' }] }));
  if (/themes\.json/.test(url)) return Promise.resolve(reply(200, { themes: [{ id: 1, name: 'Dawn', role: 'main' }] }));
  if (/assets\.json/.test(url)) return Promise.resolve(reply(200, { asset: { value: "brites-gclid-capture/2 {% render 'brites-gclid-capture' %}" } }));
  return Promise.resolve(reply(200, {}));
}

const emptyQuery = { where() { return this; }, limit() { return this; }, orderBy() { return this; }, doc() { return this; },
  get: async () => ({ docs: [], forEach() {}, size: 0, empty: true, exists: false, data: () => ({}) }) };
const stubAdmin = { firestore: () => (dbCalls++, { collection: () => emptyQuery, doc: () => emptyQuery, runTransaction: async fn => fn({ get: async () => ({ exists: false, data: () => ({}) }), update() {} }) }) };

// Intercept the two modules every endpoint reaches the world through.
const realResolve = Module._resolveFilename;
const STUBS = { 'node-fetch': stubFetch, './firebaseAdmin': stubAdmin };
Module._resolveFilename = function (request, parent, ...rest) {
  if (request === 'node-fetch') return 'STUB:node-fetch';
  if (/firebaseAdmin$/.test(request)) return 'STUB:firebaseAdmin';
  return realResolve.call(this, request, parent, ...rest);
};
require.cache['STUB:node-fetch'] = { id: 'STUB:node-fetch', filename: 'STUB:node-fetch', loaded: true, exports: stubFetch };
require.cache['STUB:firebaseAdmin'] = { id: 'STUB:firebaseAdmin', filename: 'STUB:firebaseAdmin', loaded: true, exports: stubAdmin };

Object.assign(process.env, {
  GADS_CLIENT_ID: 'x.apps.googleusercontent.com', GADS_CLIENT_SECRET: 'GOCSPX-x', GADS_REFRESH_TOKEN: '1//x',
  GADS_DEVELOPER_TOKEN: 'dev', GADS_CUSTOMER_ID: '1234567890', GADS_LOGIN_CUSTOMER_ID: '1234567890',
  GADS_CONVERSION_ACTION: 'customers/1234567890/conversionActions/7', GMC_REFRESH_TOKEN: 'r', GMC_MERCHANT_ID: '999',
  SHOPIFY_STORE: 'x.myshopify.com', SHOPIFY_CLIENT_ID: 'c', SHOPIFY_CLIENT_SECRET: 's',
  GEMINI_API_KEY: 'g', FIREBASE_PRIVATE_KEY: 'k', FIREBASE_PROJECT_ID: 'p'
});
// Every check needs the passcode (they refuse outright without EDIT_PASSCODE; see the end), so
// they run here the way the owner opens them: ?key=<EDIT_PASSCODE>.
process.env.EDIT_PASSCODE = 'secret';
delete process.env.GADS_CONVERSION_UPLOAD_API;

const ENDPOINTS = [
  ['googleConnectionsCheck.js', { format: 'json', key: 'secret' }],
  ['googleConnectionsCheck.js', { format: 'json', write: '1', key: 'secret' }],
  ['googleMerchantHealth.js', { format: 'json', key: 'secret' }],
  ['shopifyAttributionCheck.js', { format: 'json', key: 'secret' }],
  ['shopifyAttributionCheck.js', { format: 'json', diagnose: '1', key: 'secret' }],
  ['googleAdsAuthCheck.js', { key: 'secret' }],
  ['googleAdsDiag.js', { key: 'secret' }]
];
const CHECKS = ['googleConnectionsCheck.js', 'googleMerchantHealth.js', 'shopifyAttributionCheck.js', 'googleAdsAuthCheck.js', 'googleAdsDiag.js'];

(async () => {
  for (const [file, params] of ENDPOINTS) {
    const label = file.replace('.js', '') + (params.write ? ' ?write=1' : params.diagnose ? ' ?diagnose=1' : '');
    const mod = require(path.join(FN, file));
    let res;
    // A ReferenceError here is the whole point: it means a function is called
    // that does not exist, which no syntax check would have found.
    try { res = await mod.handler({ queryStringParameters: params, headers: {} }); }
    catch (e) { assert.fail(label + ' threw: ' + (e && e.stack || e)); }
    check(res && res.statusCode === 200, label + ' answers 200');
    let body;
    try { body = JSON.parse(res.body); } catch (e) { assert.fail(label + ' returned unparseable JSON'); }
    check(body && typeof body === 'object', label + ' returns an object');
    const text = JSON.stringify(body);
    check(!/is not defined|is not a function|Cannot read propert/.test(text),
      label + ' reports no undefined function or property anywhere in its output');
  }

  // The HTML views render too — they are what a person actually opens.
  for (const file of CHECKS) {
    const mod = require(path.join(FN, file));
    const res = await mod.handler({ queryStringParameters: { key: 'secret' }, headers: { accept: 'text/html' } });
    check(res.statusCode === 200 && /^<!doctype html/i.test(res.body), file.replace('.js', '') + ' renders its HTML view');
    check(!/undefined<\/td>|\[object Object\]/.test(res.body), file.replace('.js', '') + ' renders no undefined or raw objects');
  }

  // The passcode is enforced on every one of them — pasted in Netlify with quotes and a trailing
  // space, as the console tolerates — and a refusal happens before any network or storage call.
  process.env.EDIT_PASSCODE = '"secret" ';
  const quiet = async (label, run) => { const f = fetchCalls, d = dbCalls; const res = await run(); check(fetchCalls === f && dbCalls === d, label + ': nothing fetched or read'); return res; };
  for (const file of CHECKS) {
    const mod = require(path.join(FN, file)), name = file.replace('.js', '');
    const denied = await quiet(name + ' without the passcode', () => mod.handler({ queryStringParameters: {}, headers: {} }));
    check(denied.statusCode === 401, name + ' refuses without the passcode');
    const wrong = await quiet(name + ' with a wrong passcode', () => mod.handler({ httpMethod: 'POST', queryStringParameters: { key: 'secre' }, headers: { 'x-edit-passcode': 'secret2' }, body: JSON.stringify({ passcode: 'Secret' }) }));
    check(wrong.statusCode === 401, name + ' refuses a wrong passcode in the query, header or body');
    for (const [how, event] of [['?key=', { queryStringParameters: { key: 'secret', format: 'json' }, headers: {} }],
      ['the X-Edit-Passcode header', { queryStringParameters: {}, headers: { 'X-Edit-Passcode': 'secret' } }],
      ['a body passcode', { httpMethod: 'POST', queryStringParameters: {}, headers: {}, body: JSON.stringify({ passcode: 'secret' }) }]]) {
      const allowed = await mod.handler(event);
      check(allowed.statusCode === 200, name + ' answers with the passcode as ' + how);
    }
  }

  // EDIT_PASSCODE unset: every check refuses with the console's own code, reveals nothing about
  // the configuration, and spends no API quota — whatever passcode is guessed.
  delete process.env.EDIT_PASSCODE;
  const REVEALING = /1234567890|999|myshopify|GOCSPX|1\/\/|chars|googleusercontent|secret/;
  for (const file of CHECKS) {
    const mod = require(path.join(FN, file)), name = file.replace('.js', '');
    for (const event of [{ queryStringParameters: {}, headers: {} }, { queryStringParameters: { key: 'secret', format: 'json' }, headers: { 'x-edit-passcode': 'secret', accept: 'text/html' } }]) {
      const res = await quiet(name + ' with EDIT_PASSCODE unset', () => mod.handler(event));
      const body = JSON.parse(res.body);
      check(res.statusCode === 403 && body.code === 'EDIT_PASSCODE_NOT_SET' && body.error === 'Set EDIT_PASSCODE in Netlify to run this check',
        name + ' refuses with "Set EDIT_PASSCODE…" while no passcode is configured');
      check(!REVEALING.test(res.body), name + ' refusal reveals no ID or credential shape');
    }
  }

  // googleAdsRepair stays open to GET, so its self-check must not reveal the account ID; its
  // retired POST still compares the passcode, and neither reaches Google.
  const repair = require(path.join(FN, 'googleAdsRepair.js'));
  const self = await quiet('googleAdsRepair ?check=1', () => repair.handler({ httpMethod: 'GET', queryStringParameters: { check: '1' }, headers: {} }));
  check(self.statusCode === 200 && JSON.parse(self.body).customerIdSet === true && !/1234567890/.test(self.body), 'googleAdsRepair self-check says an account is set without naming it');
  process.env.EDIT_PASSCODE = 'secret';
  const guess = await quiet('googleAdsRepair POST', () => repair.handler({ httpMethod: 'POST', queryStringParameters: {}, headers: { 'x-edit-passcode': 'guess' }, body: '{}' }));
  const retired = await quiet('googleAdsRepair POST', () => repair.handler({ httpMethod: 'POST', queryStringParameters: {}, headers: { 'x-edit-passcode': 'secret' }, body: '{}' }));
  check(guess.statusCode === 401 && retired.statusCode === 409, 'googleAdsRepair refuses a wrong passcode and creates nothing with the right one');
  delete process.env.EDIT_PASSCODE;

  console.log(passed + ' endpoint smoke checks passed.');
})().catch(e => { console.error(e); process.exitCode = 1; });
