// Sale → Google conversion pipeline: what is sent, what is never sent, refunds,
// consent, stuck Data Manager receipts and attribution. Offline: Google, Shopify
// and Firestore are fakes; nothing leaves the process.
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm'), crypto = require('node:crypto');
const fnDir = path.resolve(__dirname, '../../netlify/functions');
const source = fs.readFileSync(path.join(fnDir, 'googleAdsAutopilot.js'), 'utf8');
const DM = require(path.join(fnDir, 'googleAdsDataManager.js'));
const evidence = require(path.join(fnDir, 'googleAdsSalesEvidence.js'));
const clone = x => x == null ? x : JSON.parse(JSON.stringify(x));

function store() {
  const docs = new Map(); let seq = 0;
  const key = (col, id) => col + '/' + id;
  const snap = (col, id) => ({ id, exists: docs.has(key(col, id)), ref: ref(col, id), data: () => clone(docs.get(key(col, id))) });
  const ref = (col, id) => ({ id, path: key(col, id), get: async () => snap(col, id), isEqual: other => !!other && other.path === key(col, id),
    set: async (v, o) => { docs.set(key(col, id), o && o.merge ? { ...(docs.get(key(col, id)) || {}), ...clone(v) } : clone(v)); },
    update: async v => { if (!docs.has(key(col, id))) throw Error('missing ' + key(col, id)); docs.set(key(col, id), { ...docs.get(key(col, id)), ...clone(v) }); },
    create: async v => { if (docs.has(key(col, id))) { const e = Error('6 ALREADY_EXISTS: Document already exists'); e.code = 6; throw e; } docs.set(key(col, id), clone(v)); } });
  const query = (col, filters = [], max = Infinity) => ({
    where: (k, op, v) => query(col, [...filters, [k, op, v]], max), limit: n => query(col, filters, n), orderBy: () => query(col, filters, max),
    doc: id => ref(col, id == null ? 'auto' + (++seq) : id), add: async v => { const r = ref(col, 'auto' + (++seq)); await r.set(v); return r; },
    get: async () => { const list = [...docs.keys()].filter(k => k.startsWith(col + '/') && filters.every(([f, op, v]) => op === '>=' ? docs.get(k)[f] >= v : op === '<=' ? docs.get(k)[f] <= v : docs.get(k)[f] === v)).slice(0, max).map(k => snap(col, k.slice(col.length + 1)));
      return { docs: list, size: list.length, empty: !list.length, forEach: fn => list.forEach(fn) }; } });
  const db = { collection: col => query(col),
    batch: () => { const ops = []; return { set: (r, v, o) => ops.push(() => r.set(v, o)), update: (r, v) => ops.push(() => r.update(v)), delete: r => ops.push(() => docs.delete(r.path)), commit: async () => { for (const op of ops) await op(); } }; },
    runTransaction: async work => { const ops = [], tx = { get: r => r.get(), set: (r, v, o) => ops.push(() => r.set(v, o)), update: (r, v) => ops.push(() => r.update(v)) }; const out = await work(tx); for (const op of ops) await op(); return out; } };
  return { docs, get: (col, id) => clone(docs.get(key(col, id))), put: (col, id, v) => docs.set(key(col, id), clone(v)), f: { db, FV: { serverTimestamp: () => 'server-time' } } };
}

// The conversion section of the engine, run against the fakes above.
function engine(options = {}) {
  const s = store(), requests = [], ledger = [];
  const COL = { convQueue: 'queue', convAdj: 'adjustments', refunds: 'refunds', orderLog: 'orders', state: 'state', ledger: 'ledger' };
  const ctx = { require: id => require(id.startsWith('./') ? path.join(fnDir, id) : id), console, COL, CURRENCY: 'USD', CID: '123', BASE: 'https://ads.invalid/v24',
    ENV: { GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456', ...options.env }, fb: () => s.f,
    control: async () => ({ dryRun: false }), mintToken: async () => 'token', adsHeaders: () => ({}), ledger: async row => { ledger.push(clone(row)); },
    gAdsTime: d => d.toISOString().replace('T', ' ').slice(0, 19) + '+00:00', _r2: n => Math.round(n * 100) / 100, _orderItem: x => x,
    fetch: async (url, init) => { requests.push({ url, body: JSON.parse(init.body) }); return { ok: true, json: async () => clone(options.response ? options.response(JSON.parse(init.body)) : {}) }; } };
  ctx._orderLogDocId = id => id ? 'order_' + String(id).replace(/[^a-zA-Z0-9_-]+/g, '_') : null;
  vm.createContext(ctx);
  vm.runInContext(source.slice(source.indexOf('async function enqueueConversion('), source.indexOf('/* ---- Conversion-tracking health')), ctx);
  return { ctx, s, requests, ledger };
}

function health(actions, dataManager) {
  const start = source.indexOf('async function conversionHealth('), end = source.indexOf('\n}', start) + 2;
  const ctx = vm.createContext({ Date, ENV: { GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456', GADS_CONVERSION_UPLOAD_API: dataManager ? '' : 'legacy' }, fb: () => null,
    dataManagerService: () => ({ health: async () => dataManager }), _accountTz: async () => 'UTC', _acctDateYmd: () => '2026-09-28',
    gaql: async q => q.includes('FROM conversion_action') ? actions : q.includes('conversion_tracking_setting') ? [{ customer: { conversionTrackingSetting: { conversionTrackingStatus: 'CONVERSION_TRACKING_MANAGED_BY_SELF' } } }] : [{ metrics: { conversions: 3 } }] });
  vm.runInContext(source.slice(start, end), ctx); return ctx.conversionHealth({ force: true });
}

// The webhook with the engine replaced by a recorder.
function webhook() {
  const calls = { enqueue: [], order: [], refund: [] }, secret = 'fixture-webhook-secret';
  const E = { bumpBestSellers: async () => ({}), gAdsTime: d => d.toISOString().replace('T', ' ').slice(0, 19) + '+00:00',
    enqueueConversion: async x => { calls.enqueue.push(clone(x)); return { enqueued: true }; }, recordOrderEvent: async x => { calls.order.push(clone(x)); return { ok: true }; },
    recordRefund: async x => { calls.refund.push(clone(x)); return { ok: true }; } };
  const mod = { exports: {} }, req = id => id === './googleAdsAutopilot' ? E : id === './britesAuth' ? {} : require(id.startsWith('./') ? path.join(fnDir, id) : id);
  new Function('require', 'module', 'exports', fs.readFileSync(path.join(fnDir, 'shopifyOrderWebhook.js'), 'utf8'))(req, mod, mod.exports);
  process.env.SHOPIFY_WEBHOOK_SECRET = secret;
  const send = async (topic, payload) => { const body = JSON.stringify(payload), hmac = crypto.createHmac('sha256', secret).update(body).digest('base64');
    const log = console.log; console.log = () => {}; try { return await mod.exports.handler({ httpMethod: 'POST', body, headers: { 'X-Shopify-Hmac-Sha256': hmac, 'X-Shopify-Topic': topic } }); } finally { console.log = log; } };
  return { send, calls };
}
const order = extra => ({ id: 5001, name: '#1001', created_at: '2026-09-20T15:04:05-04:00', total_price: '120.00', currency: 'USD', financial_status: 'paid', test: false,
  landing_site: '/products/charm?utm_source=google&utm_medium=paid_search&utm_campaign=design_studio&utm_content=987654321&gad_campaignid=123456789&gclid=click-1',
  note_attributes: [{ name: 'gclid', value: 'click-1' }], billing_address: { country_code: 'gb' }, line_items: [{ title: 'Charm', quantity: 1, price: '120.00' }], ...extra });

let passed = 0;
async function test(name, fn) { await fn(); passed++; console.log('PASS ' + name); }
(async () => {
  await test('consent reaches Data Manager in its own enum, and only when the shopper gave it', () => {
    const row = { orderId: '1', conversionDateTime: '2026-09-20 19:04:05+00:00', value: 120, currency: 'USD', gclid: 'g' };
    assert.deepEqual(clone(DM.eventFor({ ...row, consent: { adUserData: 'GRANTED', adPersonalization: 'DENIED' } }).consent), { adUserData: 'CONSENT_GRANTED', adPersonalization: 'CONSENT_DENIED' });
    assert.equal(DM.eventFor({ ...row, consent: { adUserData: 'maybe' } }).consent, undefined);
    assert.equal(DM.needsConsent({ buyerCountry: 'GB' }), true); assert.equal(DM.needsConsent({ buyerCountry: 'GB', consent: { adUserData: 'DENIED' } }), false); assert.equal(DM.needsConsent({ buyerCountry: 'US' }), false);
  });
  await test('a sale is sent net of refunds already recorded; one refunded in full is never sent', () => {
    const row = { orderId: '1', conversionDateTime: '2026-09-20 19:04:05+00:00', value: 120, currency: 'USD', gclid: 'g', refundedTotal: 45.5 };
    assert.equal(DM.eventFor(row).conversionValue, 74.5); assert.equal(DM.fullyRefunded(row), false); assert.equal(DM.fullyRefunded({ ...row, refundedTotal: 120 }), true);
  });
  await test('diagnostics accept a sparse echo of the one destination, and never another account', () => {
    const target = DM.destination('customers/123/conversionActions/456');
    assert.equal(DM.summarizeDiagnostics({ requestStatusPerDestination: [{ destination: { operatingAccount: { accountId: '123' } }, requestStatus: 'SUCCESS', eventsIngestionStatus: { recordCount: '1' } }] }, target).state, 'success');
    assert.equal(DM.summarizeDiagnostics({ requestStatusPerDestination: [{ destination: { operatingAccount: { product: 'GOOGLE_ADS', accountId: '123' }, productDestinationId: '456' }, requestStatus: 'FAILED', errorInfo: { errorCounts: [{ reason: 'INVALID_CLICK_ID', recordCount: '1' }] } }] }, target).state, 'failed');
    const other = DM.summarizeDiagnostics({ requestStatusPerDestination: [{ destination: { operatingAccount: { accountType: 'GOOGLE_ADS', accountId: '999' }, productDestinationId: '456' }, requestStatus: 'SUCCESS', eventsIngestionStatus: { recordCount: '1' } }] }, target);
    assert.equal(other.state, 'processing'); assert.equal(other.status, 'DESTINATION_UNCONFIRMED');
  });
  await test('a receipt past Google\'s 24-hour window is reported stuck, with the reason', async () => {
    const s = store(), now = Date.parse('2026-09-28T12:00:00Z');
    s.put('queue', 'a', { orderId: 'a', uploaded: false, dmState: 'processing', dmRequestId: 'r1', dmSubmittedAt: now - 12 * 86400000, dmCheckError: 'HTTP 403', buyerCountry: 'FR' });
    s.put('queue', 'b', { orderId: 'b', uploaded: false, dmState: 'processing', dmRequestId: 'r2', dmSubmittedAt: now - 3600000 });
    const api = DM.createDataManager({ env: { GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456', GADS_DATAMANAGER_REFRESH_TOKEN: 'x', GADS_CLIENT_ID: 'x', GADS_CLIENT_SECRET: 'x' }, fetch: async () => { throw Error('offline'); }, fb: () => s.f, COL: { convQueue: 'queue' }, ledger: async () => {}, now: () => now });
    const h = await api.health();
    assert.equal(h.processing, 2); assert.equal(h.staleProcessing, 1); assert.equal(h.consentMissing, 1); assert.match(Object.keys(h.staleReasons)[0], /status request failed: HTTP 403/);
    const out = await health([{ conversionAction: { id: '456', name: 'offline (Upload)', status: 'ENABLED', type: 'UPLOAD_CLICKS', category: 'PURCHASE', primaryForGoal: true } }], { ...h, configured: true, confirmed: 3 });
    assert.equal(out.validated, false); assert(out.reasons.some(r => /stuck rather than processing: 1 × the status request failed/.test(r))); assert(out.reasons.some(r => /EEA, UK or Switzerland/.test(r)));
    assert.equal(out.healthy, false); assert(out.reasons.some(r => /oldest sent 2026-09-16/.test(r)));
    assert.equal(out.lastUpload.at, now - 3600000); assert.equal(out.lastUpload.confirmed, false);
  });
  await test('queued sales that wait or cannot be sent are named, and the latest sent sale dates the last upload', async () => {
    const s = store(), now = Date.now(), sale = { uploaded: false, value: 10, currency: 'USD', gclid: 'g', conversionDateTime: '2026-09-28 06:00:00+00:00' };
    s.put('queue', 'late', { ...sale, orderId: 'late', createdAt: now - 5 * 3600000 }); s.put('queue', 'fresh', { ...sale, orderId: 'fresh', createdAt: now - 600000 });
    s.put('queue', 'bad', { ...sale, orderId: 'bad', conversionDateTime: '2026-09-28 06:00:00' });
    s.put('queue', 'sent', { ...sale, orderId: 'sent', dmState: 'processing', dmRequestId: 'r', dmSubmittedAt: now - 7200000 });
    const api = DM.createDataManager({ env: { GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456', GADS_DATAMANAGER_REFRESH_TOKEN: 'x', GADS_CLIENT_ID: 'x', GADS_CLIENT_SECRET: 'x' }, fetch: async () => { throw Error('offline'); }, fb: () => s.f, COL: { convQueue: 'queue' }, ledger: async () => {}, now: () => now });
    const h = await api.health();
    assert.equal(h.unsent, 2); assert.equal(h.oldestUnsentAt, now - 5 * 3600000); assert.equal(h.unsendable, 1); assert.match(Object.keys(h.unsendableReasons)[0], /time zone/); assert.equal(h.latestSubmittedAt, now - 7200000);
    const out = await health([{ conversionAction: { id: '456', name: 'offline (Upload)', status: 'ENABLED', type: 'UPLOAD_CLICKS', category: 'PURCHASE', primaryForGoal: true } }], { ...h, configured: true });
    assert.equal(out.healthy, false); assert(out.reasons.some(r => /1 queued sale\(s\) cannot be sent as stored: 1 × The original conversion timestamp must include its time zone/.test(r)));
    assert(out.reasons.some(r => /2 sale\(s\) have waited more than 3 hours to be sent/.test(r))); assert.equal(out.lastUpload.at, now - 7200000); assert.equal(out.lastUpload.confirmed, false);
  });
  await test('conversion health allows exactly one primary purchase action, and says which', async () => {
    const upload = { id: '456', name: 'offline (Upload)', status: 'ENABLED', type: 'UPLOAD_CLICKS', category: 'PURCHASE', primaryForGoal: true, countingType: 'MANY_PER_CLICK', valueSettings: { alwaysUseDefaultValue: false } };
    const web = { id: '789', name: 'Purchase', status: 'ENABLED', type: 'WEBPAGE', category: 'PURCHASE', primaryForGoal: true };
    const pending = { configured: true, confirmed: 0, processing: 2 }, confirmed = { configured: true, confirmed: 4, processing: 0 };
    // Both primary: every order both record counts twice. Until Google confirms uploads, the tag stays primary.
    const both = await health([{ conversionAction: upload }, { conversionAction: web }], pending);
    assert.equal(both.doubleCounting.length, 1); assert.equal(both.healthy, false); assert.equal(both.validated, false);
    assert(both.reasons.some(r => /Two primary purchase actions count the same sales/.test(r) && /keep "Purchase" \(789\) Primary and set "offline \(Upload\)" \(456\) to Secondary until Sales shows uploads confirmed/.test(r)));
    const bothConfirmed = await health([{ conversionAction: upload }, { conversionAction: web }], confirmed);
    assert.equal(bothConfirmed.healthy, false); assert(bothConfirmed.reasons.some(r => /make "offline \(Upload\)" \(456\) Primary and "Purchase" \(789\) Secondary/.test(r)));
    const secondary = await health([{ conversionAction: { ...upload, primaryForGoal: false } }, { conversionAction: web }], pending);
    assert.equal(secondary.doubleCounting.length, 0); assert.equal(secondary.healthy, true);
    assert(secondary.reasons.some(r => /"offline \(Upload\)" \(456\) is Secondary, so bidding optimizes toward "Purchase" \(789\).*keep it that way until Google confirms uploads/.test(r)));
    const ready = await health([{ conversionAction: { ...upload, primaryForGoal: false } }, { conversionAction: web }], confirmed);
    assert(ready.reasons.some(r => /Uploads are confirmed now: .*make "offline \(Upload\)" \(456\) Primary and "Purchase" \(789\) Secondary/.test(r)));
    const none = await health([{ conversionAction: { ...upload, primaryForGoal: false } }, { conversionAction: { ...web, primaryForGoal: false } }], confirmed);
    assert.equal(none.healthy, false); assert(none.reasons.some(r => /No purchase action is primary/.test(r)));
    const unknown = await health([{ conversionAction: { id: '456', name: 'offline (Upload)', status: 'ENABLED', type: 'UPLOAD_CLICKS', category: 'PURCHASE' } }, { conversionAction: web }], pending);
    assert.equal(unknown.healthy, true); assert.equal(unknown.validated, false); assert.equal(unknown.doubleCounting.length, 0); assert(unknown.reasons.some(r => /did not report which purchase action is primary/.test(r)));
    const tagUnknown = await health([{ conversionAction: { ...upload, primaryForGoal: false } }, { conversionAction: { id: '789', name: 'Purchase', status: 'ENABLED', type: 'WEBPAGE', category: 'PURCHASE' } }], confirmed);
    assert.equal(tagUnknown.healthy, true); assert.equal(tagUnknown.validated, false); assert(tagUnknown.reasons.some(r => /did not report which purchase action is primary/.test(r))); assert(!tagUnknown.reasons.some(r => /No purchase action is primary|optimizes toward  /.test(r)));
    const alone = await health([{ conversionAction: upload }], confirmed);
    assert.equal(alone.healthy, true); assert.equal(alone.validated, true); assert(!alone.reasons.some(r => /primary/i.test(r)));
    const fixed = await health([{ conversionAction: { ...upload, countingType: 'ONE_PER_CLICK', valueSettings: { alwaysUseDefaultValue: true } } }]);
    assert.equal(fixed.healthy, false); assert(fixed.reasons.some(r => /default value/.test(r))); assert(fixed.reasons.some(r => /one conversion per click/.test(r)));
  });
  await test('two deliveries of one order queue one conversion, with consent and country', async () => {
    const e = engine(), sale = { gclid: 'g', value: 120, currency: 'USD', orderId: '5001', conversionDateTime: '2026-09-20 19:04:05+00:00', consent: { adUserData: 'GRANTED' }, buyerCountry: 'GB' };
    const [a, b] = await Promise.all([e.ctx.enqueueConversion(sale), e.ctx.enqueueConversion(sale)]);
    assert.equal([a, b].filter(r => r.enqueued).length, 1); assert.equal([a, b].filter(r => r.duplicate).length, 1);
    const rows = [...e.s.docs.keys()].filter(k => k.startsWith('queue/')); assert.equal(rows.length, 1);
    const row = e.s.get('queue', 'order_5001'); assert.equal(row.buyerCountry, 'GB'); assert.equal(row.consent.adUserData, 'GRANTED'); assert.equal(row.consent.adPersonalization, null);
    assert.equal((await e.ctx.enqueueConversion({ ...sale, gclid: null })), false);
  });
  await test('Google\'s snake_case partial-failure path identifies the refused adjustment', () => {
    const e = engine(), pf = { details: [{ errors: [{ errorCode: { conversionAdjustmentUploadError: 'TOO_RECENT_CONVERSION' }, message: 'too recent', location: { fieldPathElements: [{ fieldName: 'conversion_adjustments', index: 1 }] } }] }] };
    assert.deepEqual(Object.keys(clone(e.ctx._pfIndexErrors(pf, 'conversionAdjustments'))), ['1']);
  });
  await test('refunds adjust only a recorded sale, once per order, never above its net value', async () => {
    const e = engine({ response: body => ({ partialFailureError: { message: 'one refused', details: [{ errors: [{ errorCode: { conversionAdjustmentUploadError: 'TOO_RECENT_CONVERSION' }, message: 'too recent', location: { fieldPathElements: [{ fieldName: 'conversion_adjustments', index: body.conversionAdjustments.findIndex(a => a.orderId === '500') }] } }] }] } }) });
    const q = (id, x) => e.s.put('queue', id, { orderId: id, currency: 'USD', uploaded: true, ...x }), adj = (id, orderId, type, value, at) => e.s.put('adjustments', id, { orderId, adjustmentType: type, restatementValue: value, currency: 'USD', adjustmentDateTime: at, uploaded: false });
    q('100', { value: 100, refundedTotal: 60, dmState: 'success', dmValue: 100 }); adj('a1', '100', 'RESTATEMENT', 80, '2026-09-20 10:00:00+00:00'); adj('a2', '100', 'RESTATEMENT', 40, '2026-09-21 10:00:00+00:00');
    q('200', { value: 50, uploaded: false, dmState: 'processing', dmRequestId: 'r' }); adj('b1', '200', 'RETRACTION', null, '2026-09-21 10:00:00+00:00');
    q('300', { value: 70, refundedTotal: 70, dmState: 'not_sent_refunded' }); adj('c1', '300', 'RETRACTION', null, '2026-09-21 10:00:00+00:00');
    q('400', { value: 50, refundedTotal: 10, dmState: 'success', dmValue: 40 }); adj('d1', '400', 'RESTATEMENT', 40, '2026-09-21 10:00:00+00:00');
    q('500', { value: 90, refundedTotal: 10, dmState: 'success', dmValue: 90 }); adj('e1', '500', 'RESTATEMENT', 80, '2026-09-20 10:00:00+00:00'); adj('e2', '500', 'RESTATEMENT', 60, '2026-09-22 10:00:00+00:00');
    const r = await e.ctx.uploadConversionAdjustments({ ctrl: { dryRun: false } });
    const sent = e.requests[0].body.conversionAdjustments.map(a => [a.orderId, a.restatementValue && a.restatementValue.adjustedValue]);
    assert.deepEqual(sent, [['100', 40], ['500', 60]]);
    assert.equal(r.uploaded, 1); assert.equal(r.rejected, 1); assert.equal(r.waiting, 1); assert.equal(r.superseded, 4);
    assert.equal(e.s.get('adjustments', 'a2').uploaded, true); assert.equal(e.s.get('adjustments', 'a1').superseded, true);
    assert.match(e.s.get('adjustments', 'c1').supersededReason, /refunded in full before it was sent/); assert.match(e.s.get('adjustments', 'd1').supersededReason, /already/);
    assert.equal(e.s.get('adjustments', 'b1').uploaded, false); assert.equal(e.s.get('adjustments', 'b1').uploadAttempts, undefined);
    const refused = e.s.get('adjustments', 'e2'); assert.equal(refused.uploaded, false); assert.equal(refused.uploadAttempts, 1); assert(refused.nextAttemptAt > Date.now());
    assert.equal(e.s.get('adjustments', 'e1').superseded, true);
  });
  await test('a partial failure naming no adjustment rejects all of them', async () => {
    const e = engine({ response: () => ({ partialFailureError: { message: 'unexplained', details: [] } }) });
    e.s.put('queue', '100', { orderId: '100', value: 100, refundedTotal: 100, uploaded: true, dmState: 'success', dmValue: 100 });
    e.s.put('adjustments', 'a', { orderId: '100', adjustmentType: 'RETRACTION', currency: 'USD', adjustmentDateTime: '2026-09-21 10:00:00+00:00', uploaded: false });
    const r = await e.ctx.uploadConversionAdjustments({ ctrl: { dryRun: false } });
    assert.equal(r.uploaded, 0); assert.equal(r.rejected, 1); assert.match(e.s.get('adjustments', 'a').uploadError, /without naming/);
  });
  await test('a refund queues the adjustment against the sale\'s click time; a zero refund queues nothing', async () => {
    const e = engine();
    e.s.put('queue', 'q', { orderId: '100', value: 100, refundedTotal: 0, currency: 'USD', gclid: 'g', conversionDateTime: '2026-09-20 19:04:05+00:00', uploaded: true, dmState: 'success' });
    assert.equal((await e.ctx.recordRefund({ orderId: '100', refundAmount: 0, refundId: 'r0' })).skipped, 'no money was refunded');
    assert.equal([...e.s.docs.keys()].filter(k => k.startsWith('adjustments/')).length, 0);
    const r = await e.ctx.recordRefund({ orderId: '100', refundAmount: 30, refundId: 'r1', when: '2026-09-22 10:00:00+00:00' });
    assert.equal(r.adjustmentType, 'RESTATEMENT'); assert.equal(r.newValue, 70);
    const queued = [...e.s.docs].find(([k]) => k.startsWith('adjustments/'))[1]; assert.equal(queued.conversionDateTime, '2026-09-20 19:04:05+00:00'); assert.equal(e.s.get('queue', 'q').refundedTotal, 30);
  });
  await test('the legacy path sends net value and consent, and closes a sale refunded in full', async () => {
    const e = engine({ env: { GADS_CONVERSION_UPLOAD_API: 'legacy' } });
    e.s.put('queue', 'full', { orderId: 'full', value: 100, refundedTotal: 100, currency: 'USD', gclid: 'g1', conversionDateTime: '2026-09-20 19:04:05+00:00', uploaded: false });
    e.s.put('queue', 'part', { orderId: 'part', value: 50, refundedTotal: 20, currency: 'USD', gclid: 'g2', conversionDateTime: '2026-09-20 19:04:05+00:00', uploaded: false, consent: { adUserData: 'CONSENT_GRANTED' } });
    await e.ctx.uploadConversions({ ctrl: { dryRun: false } });
    const sent = e.requests[0].body.conversions; assert.equal(sent.length, 1); assert.equal(sent[0].conversionValue, 30); assert.deepEqual(clone(sent[0].consent), { adUserData: 'GRANTED' });
    assert.equal(e.s.get('queue', 'full').dmState, 'not_sent_refunded'); assert.equal(e.s.get('queue', 'full').uploaded, true);
  });
  await test('the webhook uploads a paid sale with consent and country, and never a test or unpaid order', async () => {
    const w = webhook();
    await w.send('orders/paid', order({ note_attributes: [{ name: 'gclid', value: 'click-1' }, { name: '_ad_user_data', value: 'granted' }, { name: '_ad_personalization', value: 'denied' }] }));
    assert.equal(w.calls.enqueue.length, 1); const sale = w.calls.enqueue[0];
    assert.equal(sale.value, 120); assert.equal(sale.currency, 'USD'); assert.equal(sale.conversionDateTime, '2026-09-20 19:04:05+00:00'); assert.equal(sale.buyerCountry, 'GB');
    assert.deepEqual(sale.consent, { adUserData: 'GRANTED', adPersonalization: 'DENIED' });
    const logged = w.calls.order[0]; assert.equal(logged.orderName, '#1001'); assert.equal(logged.ts, Date.parse('2026-09-20T15:04:05-04:00'));
    assert.equal(logged.campaignId, '123456789'); assert.equal(logged.adGroupId, '987654321');
    await w.send('orders/paid', order({ test: true })); await w.send('orders/create', order({ financial_status: 'pending' })); await w.send('orders/paid', order({ cancelled_at: '2026-09-21T00:00:00Z' }));
    assert.equal(w.calls.enqueue.length, 1); assert.equal(w.calls.order.length, 4); assert.match(w.calls.order[2].reason, /not uploaded \(not paid yet\)/);
    await w.send('orders/create', order()); assert.equal(w.calls.enqueue.length, 2);
    await w.send('orders/paid', order({ note_attributes: [{ name: 'gclid', value: 'click-1' }], billing_address: null, shipping_address: { country_code: 'US' } }));
    assert.equal(w.calls.enqueue[2].consent, null); assert.equal(w.calls.enqueue[2].buyerCountry, 'US');
  });
  await test('Google is sent merchandise revenue after every discount, and line revenue adds up to it', async () => {
    const w = webhook(), shop = n => ({ shop_money: { amount: n, currency_code: 'USD' }, presentment_money: { amount: n, currency_code: 'USD' } });
    // A 10.00 code on the charm alone, as Shopify allocated it (total_discount leaves it out);
    // shipping 20 and tax 8 are not merchandise.
    await w.send('orders/paid', order({ total_price: '118.00', subtotal_price: '90.00', line_items: [
      { title: 'Charm', quantity: 1, price: '60.00', total_discount: '0.00', discount_allocations: [{ amount: '10.00', amount_set: shop('10.00') }] },
      { title: 'Chain', quantity: 2, price: '20.00', total_discount: '0.00', discount_allocations: [] }] }));
    assert.equal(w.calls.enqueue[0].value, 90); assert.equal(w.calls.enqueue[0].orderTotal, 118); assert.equal(w.calls.order[0].value, 118); assert.equal(w.calls.order[0].saleValue, 90);
    assert.deepEqual(w.calls.order[0].items.map(i => [i.lineRevenue, i.lineDiscount]), [[50, 10], [40, 0]]);
    // Tax-inclusive prices carry their tax, which comes off the value and the line.
    await w.send('orders/paid', order({ taxes_included: true, total_price: '60.00', subtotal_price: '60.00', line_items: [{ title: 'Charm', quantity: 1, price: '60.00', discount_allocations: [], tax_lines: [{ price: '10.00', price_set: shop('10.00') }] }] }));
    assert.equal(w.calls.enqueue[1].value, 50); assert.equal(w.calls.order[1].items[0].lineRevenue, 50);
    // A payload without allocations shares the order's subtotal across its lines.
    await w.send('orders/paid', order({ total_price: '100.00', subtotal_price: '80.00', line_items: [{ title: 'A', quantity: 1, price: '60.00' }, { title: 'B', quantity: 1, price: '40.00' }] }));
    assert.equal(w.calls.enqueue[2].value, 80); assert.deepEqual(w.calls.order[2].items.map(i => [i.lineRevenue, i.lineDiscount]), [[48, 12], [32, 8]]);
    // No subtotal at all: the order total, as before.
    await w.send('orders/paid', order()); assert.equal(w.calls.enqueue[3].value, 120);
  });
  await test('the order log keeps the order name and both values, and returns them', async () => {
    const e = engine(); Object.assign(e.ctx, { CURRENCY: 'USD' });
    vm.runInContext(source.slice(source.indexOf('function _orderLogDocId('), source.indexOf('// Aggregate store demand')), e.ctx);
    await e.ctx.recordOrderEvent({ orderId: '5001', orderName: '#1001', orderNumericId: '5001', value: 118, saleValue: 90, currency: 'USD', ts: 1, items: [] });
    await e.ctx.recordOrderEvent({ orderId: '5001', value: 118, currency: 'USD', items: [] });
    const [row] = await e.ctx.recentOrders({ limit: 5 });
    assert.equal(row.orderName, '#1001'); assert.equal(row.orderNumericId, '5001'); assert.equal(row.value, 118); assert.equal(row.saleValue, 90);
  });
  await test('a refund takes its share of the order off the value Google holds', async () => {
    const e = engine();
    e.s.put('queue', 'q', { orderId: '100', value: 100, orderTotal: 118, refundedTotal: 0, refundedMoney: 0, currency: 'USD', gclid: 'g', conversionDateTime: '2026-09-20 19:04:05+00:00', uploaded: true, dmState: 'success' });
    const half = await e.ctx.recordRefund({ orderId: '100', refundAmount: 59, refundId: 'r1' });
    assert.equal(half.adjustmentType, 'RESTATEMENT'); assert.equal(half.newValue, 50);
    assert.equal(e.s.get('queue', 'q').refundedTotal, 50); assert.equal(e.s.get('queue', 'q').refundedMoney, 59); assert.equal(DM.netValue(e.s.get('queue', 'q')), 50);
    const rest = await e.ctx.recordRefund({ orderId: '100', refundAmount: 59, refundId: 'r2' });
    assert.equal(rest.adjustmentType, 'RETRACTION'); assert.equal(e.s.get('queue', 'q').refundedTotal, 100);
    // A sale queued before orderTotal was stored: its value is the order total, its refunds money.
    e.s.put('queue', 'old', { orderId: '200', value: 120, refundedTotal: 30, currency: 'USD', gclid: 'g', uploaded: true, dmState: 'success' });
    assert.equal((await e.ctx.recordRefund({ orderId: '200', refundAmount: 20, refundId: 'r3' })).newValue, 70);
  });
  await test('an order stored before lines carried its order-level discount credits its products no more than it brought in', () => {
    const now = Date.now(), row = (id, value, items) => ({ orderId: id, ts: now - 86400000, value, currency: 'USD', financialStatus: 'PAID', items });
    const s = evidence.aggregateOrderEvidence({ rows: [row('1', 95, [{ title: 'A', variantId: '1', qty: 1, lineRevenue: 60 }, { title: 'B', variantId: '2', qty: 1, lineRevenue: 40 }]), row('2', 50, [{ title: 'A', variantId: '1', qty: 1, lineRevenue: 40 }])],
      days: 30, startAt: now - 30 * 86400000, endAt: now, complete: true, currency: 'USD', normalizeItem: x => x, marginForText: () => ({ rate: 0.5, tier: 'estimated' }), googlePaid: () => false, merchantOrganic: () => false, paidChannel: () => null });
    const revenue = Object.fromEntries(s.productRows.map(p => [p.name, p.revenue]));
    assert.deepEqual(revenue, { A: 97, B: 38 }); assert.equal(s.totalRevenue, 145);
  });
  await test('the order backfill stores line revenue net of every allocated discount', async () => {
    const e = engine(), now = Date.now(), money = n => ({ shopMoney: { amount: n, currencyCode: 'USD' } });
    const line = (title, qty, current, unit, discount, id) => ({ node: { title, quantity: qty, currentQuantity: current, sku: title.toLowerCase(), originalUnitPriceSet: money(unit), totalDiscountSet: money(discount), variant: { id: 'gid://shopify/ProductVariant/' + id }, product: { id: 'gid://shopify/Product/' + id, handle: title.toLowerCase() } } });
    const node = (id, lines) => ({ node: { id: 'gid://shopify/Order/' + id, name: '#' + id, createdAt: new Date(now - 86400000).toISOString(), displayFinancialStatus: 'PAID', test: false,
      currentTotalPriceSet: money('118.00'), totalPriceSet: money('118.00'), customAttributes: [], lineItems: { pageInfo: { hasNextPage: false }, edges: lines } } });
    e.s.put('orders', 'order_7001', { orderId: '7001', orderName: '#7001', orderNumericId: '7001', ts: now - 86400000, value: 118, currency: 'USD', items: [{ title: 'Charm', qty: 1, unitPrice: 60, lineRevenue: 60, lineDiscount: 0 }] });
    Object.assign(e.ctx, { CURRENCY: 'USD', setTimeout, clearTimeout, salesEvidenceUtil: evidence, _merchantOrganic: () => false, _auditText: (t, n) => String(t || '').slice(0, n),
      shopifyGql: async q => q.includes('accessScopes') ? { currentAppInstallation: { accessScopes: [{ handle: 'read_orders' }] } }
        : { orders: { pageInfo: { hasNextPage: false }, edges: [node('7001', [line('Charm', 1, 1, '60.00', '6.00', 1), line('Chain', 2, 2, '20.00', '4.00', 2)]), node('7002', [line('Ring', 2, 1, '30.00', '0.00', 3)])] } } });
    const start = source.indexOf('async function backfillOrders('); vm.runInContext(source.slice(start, source.indexOf('\n}', start) + 2), e.ctx);
    const r = await e.ctx.backfillOrders({ limit: 5, pages: 1 });
    assert.equal(r.fetched, 2); assert.equal(r.added, 1); assert.equal(r.enriched, 1);
    assert.deepEqual(e.s.get('orders', 'order_7001').items.map(i => [i.title, i.lineRevenue, i.lineDiscount]), [['Charm', 54, 6], ['Chain', 36, 4]]);
    const added = [...e.s.docs].find(([k, v]) => k.startsWith('orders/') && v.orderId === '7002')[1]; assert.equal(added.items[0].lineRevenue, null); assert.equal(added.items[0].qty, 1);
  });
  await test('a Shopping click on a feed link is credited to the campaign in its suffix, not to the free listing', async () => {
    const w = webhook(), feed = '/products/charm?utm_source=google&utm_medium=product_sync&utm_campaign=sag_organic&utm_source=google&utm_medium=paid_shopping&utm_campaign=21212121&utm_content=pmax';
    await w.send('orders/paid', order({ landing_site: feed + '&gclid=click-2', note_attributes: [] }));
    const paid = w.calls.order[0]; assert.equal(w.calls.enqueue.length, 1); assert.equal(paid.medium, 'paid_shopping'); assert.equal(paid.campaign, '21212121'); assert.equal(paid.campaignId, '21212121');
    await w.send('orders/paid', order({ landing_site: feed, note_attributes: [] }));
    const lost = w.calls.order[1]; assert.equal(w.calls.enqueue.length, 1); assert.equal(lost.captured, false); assert.match(lost.reason, /Google ad visit — no click id captured/); assert.equal(lost.campaignId, '21212121');
    await w.send('orders/paid', order({ landing_site: '/products/charm?utm_source=google&utm_medium=product_sync&utm_campaign=sag_organic', note_attributes: [] }));
    assert.match(w.calls.order[2].reason, /free Google listing/); assert.equal(w.calls.order[2].campaignId, null);
  });
  await test('refund amounts are in the shop currency the sale was queued in', async () => {
    const w = webhook(), set = (shop, shown) => ({ shop_money: { amount: shop, currency_code: 'USD' }, presentment_money: { amount: shown, currency_code: 'CAD' } });
    await w.send('refunds/create', { id: 1, order_id: 5001, created_at: '2026-09-22T10:00:00Z', refund_line_items: [{ subtotal: 40, subtotal_set: set(40, 54), total_tax_set: set(5, 6.75) }], transactions: [{ kind: 'refund', status: 'success', amount: '60.75', currency: 'CAD' }] });
    assert.equal(w.calls.refund[0].refundAmount, 45);
    await w.send('refunds/create', { id: 2, order_id: 5001, created_at: '2026-09-22T10:00:00Z', transactions: [{ kind: 'refund', status: 'pending', amount: '12.50', currency: 'USD' }] });
    assert.equal(w.calls.refund[1].refundAmount, 12.5);
    await w.send('refunds/create', { id: 3, order_id: 5001, created_at: '2026-09-22T10:00:00Z', refund_line_items: [{ subtotal: 40, total_tax: 5 }], refund_shipping_lines: [{ subtotal_amount_set: set(8, 10.8) }], order_adjustments: [{ kind: 'refund_discrepancy', amount: '3.00' }], transactions: [] });
    assert.equal(w.calls.refund[2].refundAmount, 53);
  });
  await test('attribution fills the campaign and ad group the app\'s own suffixes carry', () => {
    const studioPmax = evidence.clickAttribution('/?utm_source=google&utm_medium=paid_pmax&utm_campaign=design_studio&utm_content=22223333', [], { campaignId: null, adGroupId: null });
    assert.equal(studioPmax.campaignId, '22223333'); assert.equal(studioPmax.adGroupId, null);
    const search = evidence.clickAttribution('/?utm_source=google&utm_medium=paid_search&utm_campaign=11112222&utm_content=33334444', [], { campaignId: '11112222', adGroupId: null });
    assert.equal(search.campaignId, '11112222'); assert.equal(search.adGroupId, '33334444');
    const tagged = evidence.clickAttribution('/?utm_source=google&utm_medium=cpc&utm_campaign=11112222&bt_group=5', [{ key: 'gad_campaignid', value: '99998888' }], { campaignId: '11112222', adGroupId: '55556666' });
    assert.equal(tagged.campaignId, '11112222'); assert.equal(tagged.adGroupId, '55556666');
    assert.equal(evidence.clickAttribution('/?utm_source=facebook&utm_medium=paid_pmax&utm_content=22223333', [], {}).campaignId, null);
    const gad = evidence.clickAttribution('/?gad_source=1&gad_campaignid=77778888', [], {}); assert.equal(gad.campaignId, '77778888');
  });
  await test('Studio PMax and design-pipeline PMax sales count as paid PMax; pruning keeps a year of orders', async () => {
    const ctx = vm.createContext({});
    vm.runInContext(source.slice(source.indexOf('function _paidAttribution('), source.indexOf('function _merchantOrganic(')), ctx);
    assert.equal(ctx._paidChannel({ source: 'google', medium: 'paid_pmax', campaign: 'design_studio' }), 'pmax');
    assert.equal(ctx._paidChannel({ source: 'google', medium: 'cpc', campaign: '12345678', pipeline: 'pmax' }), 'pmax');
    assert.equal(ctx._paidChannel({ source: 'google', medium: 'paid_search', campaign: '12345678' }), 'search');
    assert.equal(ctx._paidAttribution({ source: 'bing', medium: 'paid_pmax' }), false);
    const e = engine(), day = 86400000;
    vm.runInContext(source.slice(source.indexOf('async function clearOrderLog('), source.indexOf('/* ============================ Ledger / approvals')), e.ctx);
    for (let i = 0; i < 6; i++) e.s.put('orders', 'recent' + i, { ts: Date.now() - i * day });
    e.s.put('orders', 'old', { ts: Date.now() - 500 * day });
    const r = await e.ctx.clearOrderLog({ keep: 2 });
    assert.equal(r.deleted, 1); assert.equal(e.s.get('orders', 'old'), undefined); assert.equal([...e.s.docs.keys()].filter(k => k.startsWith('orders/')).length, 6);
  });
  console.log(passed + ' conversion pipeline checks passed.');
  require('./suite-guard.cjs').done();
})().catch(error => { console.error(error); process.exitCode = 1; });
