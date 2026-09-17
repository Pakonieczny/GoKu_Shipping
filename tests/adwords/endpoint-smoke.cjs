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
function stubFetch(url, options) {
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
const stubAdmin = { firestore: () => ({ collection: () => emptyQuery, doc: () => emptyQuery, runTransaction: async fn => fn({ get: async () => ({ exists: false, data: () => ({}) }), update() {} }) }) };

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
delete process.env.EDIT_PASSCODE;
delete process.env.GADS_CONVERSION_UPLOAD_API;

const ENDPOINTS = [
  ['googleConnectionsCheck.js', { format: 'json' }],
  ['googleConnectionsCheck.js', { format: 'json', write: '1' }],
  ['googleMerchantHealth.js', { format: 'json' }],
  ['shopifyAttributionCheck.js', { format: 'json' }],
  ['shopifyAttributionCheck.js', { format: 'json', diagnose: '1' }],
  ['googleAdsAuthCheck.js', {}],
  ['googleAdsDiag.js', {}]
];

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
  for (const file of ['googleConnectionsCheck.js', 'googleMerchantHealth.js', 'shopifyAttributionCheck.js']) {
    const mod = require(path.join(FN, file));
    const res = await mod.handler({ queryStringParameters: {}, headers: { accept: 'text/html' } });
    check(res.statusCode === 200 && /^<!doctype html/i.test(res.body), file.replace('.js', '') + ' renders its HTML view');
    check(!/undefined<\/td>|\[object Object\]/.test(res.body), file.replace('.js', '') + ' renders no undefined or raw objects');
  }

  // A passcode, when set, is enforced on every one of them.
  process.env.EDIT_PASSCODE = 'secret';
  for (const file of ['googleConnectionsCheck.js', 'googleMerchantHealth.js', 'shopifyAttributionCheck.js']) {
    const mod = require(path.join(FN, file));
    const denied = await mod.handler({ queryStringParameters: {}, headers: {} });
    check(denied.statusCode === 401, file.replace('.js', '') + ' refuses without the passcode');
    const allowed = await mod.handler({ queryStringParameters: { key: 'secret', format: 'json' }, headers: {} });
    check(allowed.statusCode === 200, file.replace('.js', '') + ' answers with the passcode');
  }
  delete process.env.EDIT_PASSCODE;

  console.log(passed + ' endpoint smoke checks passed.');
})().catch(e => { console.error(e); process.exitCode = 1; });
