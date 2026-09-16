// Keeps the Google connection catalog honest. A newly queried Google resource,
// or a new Google host, must be added to _googleConnections.js before it can
// ship: otherwise it would run in production having never been verified.
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), Module = require('module');
const REPO = path.resolve(__dirname, '../..'), FN = path.join(REPO, 'netlify/functions');
const C = require(path.join(FN, '_googleConnections.js'));
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

const sources = fs.readdirSync(FN).filter(f => f.endsWith('.js')).map(f => fs.readFileSync(path.join(FN, f), 'utf8')).join('\n');

// 1. Every GAQL resource the application queries has a probe.
// Only a FROM preceded by a SELECT in the same statement is a real query; the
// word also appears in comments and prose throughout these files.
const queried = new Set([...sources.matchAll(/FROM\s+([a-z][a-z0-9_]{3,})/g)]
  .filter(m => /SELECT\s/.test(sources.slice(Math.max(0, m.index - 400), m.index)))
  .map(m => m[1]));
const covered = new Set(C.ADS_RESOURCES.map(r => r.resource).concat(C.MERCHANT_REPORT_VIEWS));
const unverified = [...queried].filter(r => !covered.has(r));
check(unverified.length === 0, 'every GAQL resource the application queries has a connection probe' + (unverified.length ? ' — missing: ' + unverified.join(', ') : ''));

// 2. Every Google host the application calls is catalogued.
const hosts = new Set([...sources.matchAll(/https:\/\/([a-z0-9-]+\.googleapis\.com)/g)].map(m => m[1]));
const knownHosts = new Set(C.OTHER_HOSTS.map(h => h.host));
// www.googleapis.com appears only as OAuth scope identifiers, never as an
// endpoint; those are verified against tokeninfo by the scope catalog instead.
const unlisted = [...hosts].filter(h => !knownHosts.has(h) && !/^fonts\./.test(h) && h !== 'www.googleapis.com');
const scopeStrings = new Set([...sources.matchAll(/https:\/\/www\.googleapis\.com\/auth\/[a-z.-]+/g)].map(m => m[0]));
const uncheckedScopes = [...scopeStrings].filter(s => !C.REQUIRED_SCOPES.some(r => r.scope === s) && !/gmail/.test(s));
check(uncheckedScopes.length === 0, 'every OAuth scope the application requests is verified' + (uncheckedScopes.length ? ' — missing: ' + uncheckedScopes.join(', ') : ''));
check(unlisted.length === 0, 'every Google host the application calls is catalogued' + (unlisted.length ? ' — missing: ' + unlisted.join(', ') : ''));

// 3. Each probe is a read. A verifier must never mutate the account it checks.
check(C.ADS_RESOURCES.every(r => /^SELECT /.test(r.query)), 'every Google Ads probe is a SELECT');
check(C.MERCHANT_PROBES.every(p => p.method === 'GET' || (p.method === 'POST' && /reports:search$/.test(p.path('123')))),
  'every Merchant probe is a GET or a reports:search read');

// 4. Every probe explains itself, so a red row is actionable without the source.
check(C.ADS_RESOURCES.every(r => r.why && r.family) && C.MERCHANT_PROBES.every(p => p.why) && C.ADS_FIELDS.every(f => f.why),
  'every probe records why the connection matters');

// 5. The statistics the operator asked for are introspected by name.
for (const field of ['segments.device', 'asset.image_asset.full_size.width_pixels', 'asset.image_asset.full_size.height_pixels', 'metrics.video_views'])
  check(C.ADS_FIELDS.some(f => f.field === field), 'schema check covers ' + field);

// 6. The published Google requirements match the gate that actually blocks a
//    publication, so the report can never disagree with the enforcement.
const autopilot = fs.readFileSync(path.join(FN, 'googleAdsAutopilot.js'), 'utf8');
const gate = autopilot.slice(autopilot.indexOf("const ratio={square:1,landscape:1.91,portrait:.8}"), autopilot.indexOf("const ratio={square:1,landscape:1.91,portrait:.8}") + 420);
check(gate.length > 100, 'the publication dimension gate was located');
for (const spec of C.REQUIRED_IMAGE_FORMATS.filter(s => ['square', 'landscape', 'portrait'].includes(s.key))) {
  check(gate.includes(spec.key + ':' + spec.ratio) || gate.includes(spec.key + ':' + String(spec.ratio).replace(/^0/, '')),
    'publication gate uses Google\'s ' + spec.key + ' ratio ' + spec.ratio);
  check(gate.includes(spec.key + ':' + spec.minWidth) && gate.includes(spec.key + ':' + spec.minHeight),
    'publication gate uses Google\'s ' + spec.key + ' minimum ' + spec.minWidth + '×' + spec.minHeight);
}

// 7. The creative policy exports every video aspect ratio Google serves.
const policy = require(path.join(REPO, 'brites-ad-format-policy.js'));
for (const spec of C.REQUIRED_VIDEO_FORMATS)
  check(policy.video.formats.some(f => f.key === spec.key && Math.abs(f.width / f.height - spec.ratio) < 0.02),
    'creative policy exports a ' + spec.key + ' video at Google\'s aspect ratio');
check(policy.video.seconds >= 10, 'creative policy meets Google\'s 10-second minimum');

// 8. Skipped is never green. A credential that never reached Google cannot pass.
const summary = C.summarize([{ rows: [C.ok('a', 'x'), C.skip('b', 'never attempted')] }]);
check(summary.allGreen === false && summary.skipped === 1 && summary.ok === 1, 'an unattempted probe is never counted as a pass');
const allOk = C.summarize([{ rows: [C.ok('a', 'x')] }]);
check(allOk.allGreen === true, 'a fully reached catalog reports green');
check(C.summarize([{ rows: [] }]).allGreen === false, 'an empty run is not green');

// 9. Google's error envelope is reduced to the code a reader acts on.
check(C.adsErrorCode({ error: { details: [{ errors: [{ errorCode: { authorizationError: 'USER_PERMISSION_DENIED' } }] }] } }) === 'USER_PERMISSION_DENIED (authorizationError)',
  'the first Google error code is extracted from the envelope');
check(/login-customer-id/.test(C.remedyFor('403 — USER_PERMISSION_DENIED (authorizationError)')), 'a permission failure carries its remedy');
check(C.remedyFor('200 — fine') === null, 'a healthy row carries no remedy');

// 10. The executor runs the catalog: every probe is attempted, none is skipped
//     silently, and a Google error becomes a failed row rather than a throw.
const calls = [];
const stub = async (url, options) => {
  calls.push(String(url));
  const reply = (status, body) => ({ ok: status < 400, status, json: async () => body, headers: { get: () => null } });
  if (/oauth2\.googleapis\.com/.test(url)) return reply(200, { access_token: 'test-token', expires_in: 3600 });
  if (/listAccessibleCustomers/.test(url)) return reply(200, { resourceNames: ['customers/1234567890'] });
  if (/googleAdsFields:search/.test(url)) {
    const q = JSON.parse(options.body).query;
    if (/segments\.device/.test(q)) return reply(200, { results: [{ name: 'segments.device', category: 'SEGMENT', dataType: 'ENUM', selectable: true, selectableWith: ['campaign', 'shopping_performance_view'] }] });
    return reply(200, { results: [{ name: 'x', category: 'METRIC', dataType: 'DOUBLE', selectable: true }] });
  }
  if (/googleAds:search/.test(url)) {
    // One wired resource and one unwired capability both refused, so the row
    // severity can be told apart.
    if (/FROM (video|detail_placement_view)\b/.test(JSON.parse(options.body).query))
      return reply(403, { error: { details: [{ errors: [{ errorCode: { authorizationError: 'USER_PERMISSION_DENIED' } }] }] } });
    return reply(200, { results: [{}] });
  }
  return reply(200, {});
};
const realResolve = Module._resolveFilename;
Module._resolveFilename = function (request, ...rest) { return request === 'node-fetch' ? __filename : realResolve.call(this, request, ...rest); };
require.cache[__filename] = { id: __filename, filename: __filename, loaded: true, exports: stub };
process.env.GADS_CLIENT_ID = 'x.apps.googleusercontent.com'; process.env.GADS_CLIENT_SECRET = 'GOCSPX-x';
process.env.GADS_REFRESH_TOKEN = '1//x'; process.env.GADS_DEVELOPER_TOKEN = 'dev'; process.env.GADS_CUSTOMER_ID = '1234567890';
process.env.GADS_LOGIN_CUSTOMER_ID = '1234567890'; process.env.GADS_API_VERSION = 'v24';
delete process.env.GMC_REFRESH_TOKEN; delete process.env.GEMINI_API_KEY; delete process.env.SHOPIFY_STORE;

(async () => {
  const checker = require(path.join(FN, 'googleConnectionsCheck.js'));
  const result = await checker.run({ write: false });
  const rows = result.sections.flatMap(s => s.rows);
  const byName = name => rows.find(r => r.name === name);

  check(C.ADS_RESOURCES.every(r => byName(r.resource)), 'the executor attempts every resource in the catalog');
  check(byName('detail_placement_view').status === 'warn', 'an unreachable capability that is not wired up reports as a warning, not a failure');
  check(byName('video').status === 'FAIL', 'an unreachable resource the application actually queries reports as a failure');
  check(byName('campaign').status === 'ok', 'a reachable wired resource reports green');
  check(/USER_PERMISSION_DENIED/.test(byName('video').detail) && byName('video').remedy, 'a Google error becomes a readable row with a remedy');
  check(byName('device split · shopping_performance_view').status === 'ok', 'device segmentation is confirmed where Google allows it');
  check(byName('device split · asset_group_asset').status === 'warn', 'device segmentation is reported unavailable where Google does not allow it');
  check(byName('validateOnly mutate').status === 'skipped', 'the write probe is opt-in and skipped by default');
  check(byName('merchant').status === 'skipped' && result.summary.allGreen === false, 'a missing Merchant credential is skipped, and the run is not green');
  check(rows.every(r => ['ok', 'FAIL', 'warn', 'skipped'].includes(r.status)), 'every row carries a known status');
  check(result.summary.total === rows.length, 'the summary counts every row');

  const written = await checker.run({ write: true });
  check(written.sections.flatMap(s => s.rows).find(r => r.name === 'validateOnly mutate').status === 'ok',
    'the opt-in write probe reports capability without creating anything');
  check(calls.some(u => /googleAds:mutate/.test(u)) && calls.filter(u => /googleAds:mutate/.test(u)).length === 1,
    'the write probe is sent exactly once, and only when asked for');

  console.log(passed + ' Google connection catalog and executor checks passed.');
})().catch(e => { console.error(e); process.exitCode = 1; });
