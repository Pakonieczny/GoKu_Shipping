'use strict';
// Exercise the complete public HTTP entry and its shared Kick implementation. Every credential,
// engine response and store here is synthetic; no real Firebase, Google or network is permitted.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { sameSecret } = require('../../netlify/functions/_editPasscode');
const dir = path.resolve(__dirname, '../../netlify/functions');
const source = fs.readFileSync(path.join(dir, 'googleAdsAutopilotKick.js'), 'utf8');
const apiSource = fs.readFileSync(path.join(dir, 'googleAdsAutopilotApi.js'), 'utf8');
const SECRET = 'synthetic-owner-passcode';

function fixture({ resolved = { value: '', source: 'none' }, resolveError, healthError } = {}) {
  const trace = { resolves: 0, firebase: 0, control: 0, health: [], dashboard: 0, network: 0, writes: 0, logs: [] };
  const ep = {
    sameSecret,
    envPasscode: () => '',
    resolve: async () => { trace.resolves++; if (resolveError) throw resolveError; return resolved; }
  };
  const E = {
    control: async () => { trace.control++; return {}; },
    conversionHealth: async options => {
      trace.health.push({ force: options.force });
      if (healthError) throw healthError;
      return { readOnly: !options.force, receiptOnly: true, forcedReceiptRefresh: options.force };
    },
    dashboard: async () => { trace.dashboard++; return { ok: true }; }
  };
  const firestore = () => { trace.firebase++; return { collection: () => { trace.writes++; throw Error('Store access is outside this auth fixture'); } }; };
  firestore.FieldValue = {};
  const admin = { firestore };
  const module = { exports: {} };
  const context = {
    module, exports: module.exports, process: { env: {} }, Buffer, URL, URLSearchParams,
    setTimeout, clearTimeout,
    console: Object.fromEntries(['log', 'info', 'warn', 'error', 'debug'].map(k => [k, (...args) => trace.logs.push(args.join(' '))])),
    require(name) {
      if (name === 'node-fetch') return async () => { trace.network++; throw Error('Network is forbidden in auth tests'); };
      if (name === './googleAdsAutopilot') return E;
      if (name === './_editPasscode') return ep;
      if (name === './firebaseAdmin') return admin;
      if (name === 'crypto') return require('node:crypto');
      throw Error('Unexpected dependency: ' + name);
    }
  };
  vm.runInNewContext(source, context, { filename: 'googleAdsAutopilotKick.js' });
  const apiModule = { exports: {} };
  vm.runInNewContext(apiSource, {
    module: apiModule, exports: apiModule.exports,
    require(name) { assert.equal(name, './googleAdsAutopilotKick'); return module.exports; }
  }, { filename: 'googleAdsAutopilotApi.js' });
  return {
    trace,
    post(body, headers = {}, extra = {}) {
      return apiModule.exports.handler({ httpMethod: 'POST', headers, body: JSON.stringify(body), ...extra });
    },
    request(event) { return apiModule.exports.handler(event); }
  };
}
function noDispatch(trace) {
  assert.equal(trace.resolves, 1, 'one credential resolution per HTTP request');
  assert.equal(trace.firebase, 0, 'blocked request never reaches engine Firebase');
  assert.equal(trace.control, 0, 'blocked request never reaches control');
  assert.deepEqual(trace.health, [], 'blocked request never reaches receipt reconciliation');
  assert.equal(trace.dashboard, 0);
  assert.equal(trace.network, 0);
  assert.equal(trace.writes, 0);
}
function healthDispatch(trace, force) {
  assert.equal(trace.resolves, 1);
  assert.equal(trace.firebase, 1);
  assert.equal(trace.control, 1);
  assert.deepEqual(trace.health, [{ force }], 'the authorized action runs exactly once');
  assert.equal(trace.network, 0);
  assert.equal(trace.writes, 0);
}

for (const [label, resolved, headers, extra] of [
  ['unset passcode', { value: '', source: 'none' }, {}, {}],
  ['missing resolved value', { source: 'none' }, { 'x-edit-passcode': SECRET }, {}],
  ['empty passcode with supplied body credential', { value: '', source: 'none' }, {}, { passcode: SECRET }],
  ['credential-store failure', { value: '', source: 'none', error: 'Synthetic credential store unavailable' }, { 'X-Edit-Passcode': SECRET }, { passcode: SECRET }]
]) test(label + ' blocks forced health before dispatch', async () => {
  const f = fixture({ resolved });
  const res = await f.post({ action: 'conversionHealth', force: true, ...extra }, headers);
  assert.equal(res.statusCode, 403);
  const body = JSON.parse(res.body);
  assert.equal(body.code, 'EDIT_PASSCODE_NOT_SET');
  assert.equal(body.ok, false);
  noDispatch(f.trace);
});

for (const [label, force] of [
  ['true', true], ['one', 1], ['negative number', -1], ['string true', 'true'],
  ['string false', 'false'], ['string zero', '0'], ['object', {}], ['array', []]
]) test('truthy force ' + label + ' uses the mutation credential gate', async () => {
  const f = fixture();
  const res = await f.post({ action: 'conversionHealth', force });
  assert.equal(res.statusCode, 403, 'auth truthiness matches the existing !!body.force dispatch');
  noDispatch(f.trace);
});

for (const [label, headers, body] of [
  ['missing', {}, {}],
  ['wrong lowercase header', { 'x-edit-passcode': 'synthetic-wrong' }, {}],
  ['wrong uppercase header', { 'X-Edit-Passcode': 'synthetic-wrong' }, {}],
  ['wrong body credential', {}, { passcode: 'synthetic-wrong' }]
]) test(label + ' credential is rejected when a passcode is configured', async () => {
  const f = fixture({ resolved: { value: SECRET, source: 'firebase' } });
  const res = await f.post({ action: 'conversionHealth', force: true, ...body }, headers);
  assert.equal(res.statusCode, 401);
  assert.equal(JSON.parse(res.body).error, 'unauthorized');
  noDispatch(f.trace);
});

for (const [label, headers, body] of [
  ['lowercase header', { 'x-edit-passcode': SECRET }, {}],
  ['uppercase header', { 'X-Edit-Passcode': SECRET }, {}],
  ['body', {}, { passcode: SECRET }],
  ['trimmed header', { 'x-edit-passcode': '  ' + SECRET + '  ' }, {}],
  ['body with unrelated wrong header', { 'x-edit-passcode': 'synthetic-wrong' }, { passcode: SECRET }]
]) test('correct ' + label + ' authorizes exactly one forced receipt refresh', async () => {
  const f = fixture({ resolved: { value: SECRET, source: 'firebase' } });
  const res = await f.post({ action: 'conversionHealth', force: true, ...body }, headers);
  assert.equal(res.statusCode, 200);
  assert.equal(JSON.parse(res.body).forcedReceiptRefresh, true);
  healthDispatch(f.trace, true);
  assert.ok(!JSON.stringify({ res, logs: f.trace.logs }).includes(SECRET), 'credentials never enter response or logs');
});

for (const [label, force] of [
  ['missing', undefined], ['false', false], ['zero', 0], ['empty string', ''], ['null', null]
]) test('non-forced health ' + label + ' remains available as a read with no passcode', async () => {
  const f = fixture();
  const res = await f.post({ action: 'conversionHealth', force });
  assert.equal(res.statusCode, 200);
  assert.equal(JSON.parse(res.body).readOnly, true);
  healthDispatch(f.trace, false);
});

test('configured passcode continues to protect ordinary health reads', async () => {
  const f = fixture({ resolved: { value: SECRET, source: 'env' } });
  const res = await f.post({ action: 'conversionHealth', force: false });
  assert.equal(res.statusCode, 401);
  noDispatch(f.trace);
});
test('correct credential permits an ordinary read without forcing reconciliation', async () => {
  const f = fixture({ resolved: { value: SECRET, source: 'env' } });
  const res = await f.post({ action: 'conversionHealth', force: false }, { 'x-edit-passcode': SECRET });
  assert.equal(res.statusCode, 200);
  healthDispatch(f.trace, false);
});
test('unrelated existing dashboard read stays available with no passcode', async () => {
  const f = fixture();
  const res = await f.post({ action: 'dashboard' });
  assert.equal(res.statusCode, 200);
  assert.equal(JSON.parse(res.body).editPasscodeSet, false);
  assert.equal(f.trace.dashboard, 1);
  assert.deepEqual(f.trace.health, []);
  assert.equal(f.trace.network, 0);
  assert.equal(f.trace.writes, 0);
});
test('unrelated conversion upload action remains locked with no passcode', async () => {
  const f = fixture();
  const res = await f.post({ action: 'syncConversions' });
  assert.equal(res.statusCode, 403);
  noDispatch(f.trace);
});
test('public unscheduled API cannot bypass auth by claiming a scheduled event', async () => {
  const f = fixture();
  const res = await f.post({ action: 'conversionHealth', force: true }, { 'x-nf-event': 'schedule' }, { isScheduled: true });
  assert.equal(res.statusCode, 403);
  noDispatch(f.trace);
});
test('credential resolver exception fails closed before any receipt dispatch', async () => {
  const f = fixture({ resolveError: Error('Synthetic resolver failure') });
  await assert.rejects(() => f.post({ action: 'conversionHealth', force: true }), /Synthetic resolver failure/);
  noDispatch(f.trace);
});
test('authorized provider failure is returned without a second refresh or upload', async () => {
  const f = fixture({ resolved: { value: SECRET, source: 'env' }, healthError: Error('Synthetic receipt provider unavailable') });
  const res = await f.post({ action: 'conversionHealth', force: true }, { 'x-edit-passcode': SECRET });
  assert.equal(res.statusCode, 200);
  assert.equal(JSON.parse(res.body).error, 'Synthetic receipt provider unavailable');
  healthDispatch(f.trace, true);
});
test('preflight and console page do not dispatch health or resolve passcodes', async () => {
  const f = fixture();
  assert.equal((await f.request({ httpMethod: 'OPTIONS', headers: {} })).statusCode, 204);
  const page = await f.request({ httpMethod: 'GET', headers: {} });
  assert.equal(page.statusCode, 200);
  assert.match(page.headers['Content-Type'], /^text\/html/);
  assert.equal(f.trace.resolves, 0);
  assert.equal(f.trace.control, 0);
  assert.deepEqual(f.trace.health, []);
  assert.equal(f.trace.network, 0);
  assert.equal(f.trace.writes, 0);
});
