'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {createReceiptReleasePreview} = require('../../netlify/functions/_britesGrowthReceiptReleasePreview');
const {rowFingerprint} = require('../../netlify/functions/_britesGrowthReceiptReconciliation');
const QUEUE = 'Brites_GAds_ConvQueue', epoch = Date.parse('2026-10-02T06:00:00Z');
const target = {operatingAccount: {accountType: 'GOOGLE_ADS', accountId: '123'}, productDestinationId: '456'};
const clone = value => value instanceof Date ? new Date(value) : Array.isArray(value) ? value.map(clone)
  : value && typeof value === 'object' ? Object.fromEntries(Object.entries(value).map(([key, item]) => [key, clone(item)])) : value;
const receipt = over => ({uploaded: false, failed: false, dmState: 'processing', dmRequestId: 'synthetic-private-request',
  dmDestination: clone(target), dmSubmittedAt: epoch - 15 * 86400000, dmChecks: 0, dmCheckedAt: 0,
  dmReconcileLease: null, dmReconcileLeaseUntil: 0, orderId: 'synthetic-private-order', gclid: 'synthetic-private-click',
  value: 54, currency: 'USD', refundedTotal: 0, buyerEmail: 'synthetic-private-buyer@example.invalid', ...over});
const diagnostics = over => ({requestStatusPerDestination: [{destination: clone(target), requestStatus: 'SUCCESS',
  eventsIngestionStatus: {recordCount: '1'}, ...over}]});
const response = (value, status = 200) => ({ok: status >= 200 && status < 300, status, json: async () => clone(value)});

function fixture(initial = [receipt()], options = {}) {
  const rows = new Map(initial.map((row, index) => ['row-' + index, clone(row)])), calls = [], reads = [], transactions = [], writes = [];
  const env = {BRITES_GROWTH_SANDBOX: '1', BRITES_GROWTH_NAMESPACE: 'Brites_Growth_Sandbox',
    GADS_DATAMANAGER_REFRESH_TOKEN: 'synthetic-secret-refresh', GADS_CLIENT_ID: 'synthetic-secret-client',
    GADS_CLIENT_SECRET: 'synthetic-secret-value', GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456', ...options.env};
  let clock = epoch;
  const advance = ms => {clock += ms;};
  const ref = id => ({kind: 'document', id, path: QUEUE + '/' + id});
  const snapshot = (id, view = rows) => ({id, ref: ref(id), exists: view.has(id), data: () => clone(view.get(id))});
  function query(conditions = [], maximum = Infinity) {
    return {kind: 'query', path: options.wrongCollectionPath || QUEUE, conditions, maximum,
      doc: id => ref(id), where: (field, operator, value) => {assert.equal(operator, '=='); return query([...conditions, [field, value]], maximum);},
      limit: count => query(conditions, count),
      get: async () => {
        reads.push({kind: 'initial', conditions, maximum});
        if (options.initialRead) return options.initialRead({rows, advance});
        return {docs: [...rows.keys()].filter(id => conditions.every(([field, value]) => rows.get(id)[field] === value)).slice(0, maximum).map(id => snapshot(id))};
      }};
  }
  const forbidden = () => {writes.push('forbidden-write'); throw Error('Preview cannot write');};
  const db = {collection: name => {assert.equal(name, QUEUE); return query();}, doc: name => {
    assert.equal(name, 'config/googleAdsDataManager'); return {get: async () => ({exists: false})};
  }, batch: forbidden, runTransaction: async (callback, config) => {
    assert.deepEqual(config, {readOnly: true}, 'current row checks use a read-only transaction'); transactions.push(config);
    if (options.beforeCurrent) await options.beforeCurrent({rows, advance, env});
    const view = new Map([...rows].map(([id, row]) => [id, clone(row)]));
    return callback({get: async item => {
      reads.push({kind: item.kind, id: item.id, conditions: item.conditions});
      if (options.currentRead) await options.currentRead({item, rows, advance, env});
      if (item.kind === 'document') return snapshot(item.id, view);
      return {docs: [...view.keys()].filter(id => item.conditions.every(([field, value]) => view.get(id)[field] === value))
        .slice(0, item.maximum).map(id => snapshot(id, view))};
    }, update: forbidden, set: forbidden, create: forbidden, delete: forbidden});
  }};
  const fetcher = async (url, init) => {
    calls.push({url, method: init.method});
    if (url === 'https://oauth2.googleapis.com/token') {
      assert.equal(init.method, 'POST'); assert.equal(new URLSearchParams(init.body).has('scope'), false, 'existing OAuth grant is not expanded');
      return options.oauth || response({access_token: 'synthetic-secret-access', scope: 'https://www.googleapis.com/auth/datamanager'});
    }
    assert.match(url, /^https:\/\/datamanager\.googleapis\.com\/v1\/requestStatus:retrieve\?requestId=/);
    assert.equal(init.method, 'GET'); assert.equal(init.body, undefined); assert.equal(init.redirect, 'error');
    if (options.provider) return options.provider({url, init, rows, advance});
    return response(options.diagnostics || diagnostics());
  };
  const api = createReceiptReleasePreview({env, db: options.noDb ? null : db, fetch: fetcher, now: () => clock});
  return {api, rows, calls, reads, transactions, writes, env, advance};
}

test('trusted preview uses fresh saved IDs, raw server diagnostics and a read-only current-row snapshot', async () => {
  const f = fixture(), before = rowFingerprint([...f.rows]), result = await f.api.read({limit: 15, maxMs: 20000});
  assert.equal(result.readOnly, true); assert.equal(result.receiptOnly, true); assert.equal(result.dryRun, true);
  assert.equal(result.queueUpdated, false); assert.equal(result.individualOrdersUpdated, 0); assert.equal(result.eventsIngested, 0);
  assert.equal(result.scannedRows, 1); assert.equal(result.selectedReceipts, 1); assert.equal(result.providerReceiptsConfirmed, 1);
  assert.equal(result.proposedRepairs, 1); assert.equal(result.blockedRows, 0); assert.equal(result.currentRowsRechecked, 1);
  assert.equal(result.productionApplyAvailable, false); assert.equal(result.recheckRequiredBeforeAnyApply, true); assert.equal(result.rowsReserved, false);
  assert.equal(result.individualOrderAttributionConfirmed, false); assert.equal(result.providerAggregateUsedForConfirmation, false);
  assert.equal(result.rows[0].individualOrderConfirmed, false); assert.equal(result.rows[0].providerReceiptConfirmed, true);
  assert.equal(result.rows[0].monetaryClickRefundFieldsPreserved, true);
  assert.equal(result.rows[0].rowFingerprintBefore, result.rows[0].rowFingerprintCurrent);
  for (const key of ['rowFingerprintBefore','rowFingerprintCurrent','destinationFingerprint','evidenceFingerprint','statusProposalFingerprint','proposedRowFingerprint','preservedFieldsFingerprint']) assert.match(result.rows[0][key], /^[a-f0-9]{64}$/);
  assert.equal(result.rows[0].observedAt, epoch); assert.equal(result.rows[0].plannedAt, epoch); assert.equal(result.planExpiresAt, epoch + 120000);
  assert.equal(result.rows[0].statusChanges.uploaded.to, true); assert.equal(result.rows[0].statusChanges.dmState.to, 'success');
  assert.equal(result.rows[0].statusChanges.dmChecks.to, 1); assert.equal(result.rows[0].statusChanges.dmCheckedAt.to, epoch);
  assert.equal(f.calls.length, 2); assert.equal(f.reads.filter(read => read.kind === 'initial').length, 1);
  assert.equal(f.transactions.length, 1); assert.equal(f.writes.length, 0); assert.equal(rowFingerprint([...f.rows]), before);
});

test('fifteen exact saved receipts produce fifteen status proposals and leave all orders untouched', async () => {
  const initial = Array.from({length: 15}, (_, index) => receipt({dmRequestId: 'synthetic-private-request-' + index, orderId: 'synthetic-private-order-' + index}));
  const f = fixture(initial), before = rowFingerprint([...f.rows]), result = await f.api.read();
  assert.equal(result.scannedRows, 15); assert.equal(result.selectedReceipts, 15); assert.equal(result.providerReceiptsConfirmed, 15);
  assert.equal(result.proposedRepairs, 15); assert.equal(result.blockedRows, 0); assert.equal(result.rows.length, 15);
  assert.equal(f.calls.filter(call => call.method === 'GET').length, 15); assert.equal(f.writes.length, 0); assert.equal(rowFingerprint([...f.rows]), before);
});

test('output excludes request/order/account/click/contact/financial values and raw diagnostics', async () => {
  const f = fixture([receipt({dmCheckError: 'synthetic-private-buyer@example.invalid', dmLastStatus: 'synthetic-private-text'})]), result = await f.api.read(), encoded = JSON.stringify(result);
  for (const value of ['synthetic-private-request','synthetic-private-order','synthetic-private-click','synthetic-private-buyer@example.invalid',
    'synthetic-secret-refresh','synthetic-secret-access','synthetic-secret-value','"row-0"','"accountId"','"productDestinationId"','"buyerEmail"','"gclid"','"value"','"currency"','requestStatusPerDestination']) assert.ok(!encoded.includes(value), value);
  assert.equal(result.rows[0].statusChanges.dmCheckError.fromHasValue, true); assert.equal(result.rows[0].statusChanges.dmLastStatus.from, null);
});

test('caller rows, diagnostics, IDs, fingerprints and apply controls are rejected before reads', async () => {
  const f = fixture();
  for (const extra of [{rows: []},{diagnostics: diagnostics()},{requestId: 'caller'},{plan: {}},{evidenceFingerprint: 'caller'},{apply: true},{dryRun: false},{force: true}]) {
    assert.equal((await f.api.read(extra)).code, 'INVALID_PREVIEW_OPTIONS');
  }
  assert.equal(f.reads.length, 0); assert.equal(f.calls.length, 0); assert.equal(f.writes.length, 0);
});

for (const env of [{BRITES_GROWTH_SANDBOX:'0'},{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'},{BRITES_GROWTH_NAMESPACE:undefined}]) test('unsafe preview scope fails closed: ' + JSON.stringify(env), async () => {
  const f = fixture(undefined, {env}), result = await f.api.read();
  assert.equal(result.code, 'SANDBOX_NAMESPACE_REQUIRED'); assert.equal(f.reads.length, 0); assert.equal(f.calls.length, 0);
});

test('zero limits and missing storage require no provider calls or writes', async () => {
  const f = fixture();
  assert.equal((await f.api.read({limit:0})).stopped, 'limit'); assert.equal((await f.api.read({maxMs:0})).stopped, 'time_limit');
  assert.equal(f.reads.length, 0); assert.equal(f.calls.length, 0);
  assert.equal((await fixture(undefined,{noDb:true}).api.read()).code, 'READ_ONLY_STORAGE_UNAVAILABLE');
  assert.equal((await fixture(undefined,{wrongCollectionPath:'other-queue'}).api.read()).code, 'SAVED_QUEUE_REFERENCE_UNCONFIRMED');
});

for (const [name, answer, code] of [
  ['wrong provider account', diagnostics({destination:{...clone(target), operatingAccount:{accountType:'GOOGLE_ADS',accountId:'124'}}}), 'PROVIDER_DESTINATION_UNCONFIRMED'],
  ['wrong provider action', diagnostics({destination:{...clone(target),productDestinationId:'457'}}), 'PROVIDER_DESTINATION_UNCONFIRMED'],
  ['missing destination', diagnostics({destination:undefined}), 'PROVIDER_DESTINATION_UNCONFIRMED'],
  ['conflicting legacy type', diagnostics({destination:{operatingAccount:{accountType:'GOOGLE_ADS',product:'DISPLAY_VIDEO',accountId:'123'},productDestinationId:'456'}}), 'PROVIDER_DESTINATION_UNCONFIRMED'],
  ['zero records', diagnostics({eventsIngestionStatus:{recordCount:'0'}}), 'ONE_RECORD_REQUIRED'],
  ['bulk fifteen records', diagnostics({eventsIngestionStatus:{recordCount:'15'}}), 'ONE_RECORD_REQUIRED'],
  ['missing records', diagnostics({eventsIngestionStatus:{}}), 'ONE_RECORD_REQUIRED'],
  ['processing', diagnostics({requestStatus:'PROCESSING'}), 'PROVIDER_SUCCESS_REQUIRED'],
  ['partial success', diagnostics({requestStatus:'PARTIAL_SUCCESS'}), 'PROVIDER_SUCCESS_REQUIRED'],
  ['failure', diagnostics({requestStatus:'FAILED'}), 'PROVIDER_SUCCESS_REQUIRED'],
  ['success with errors', diagnostics({errorInfo:{errorCounts:[{reason:'PROCESSING_ERROR_REASON_INVALID_GCLID',recordCount:'1'}]}}), 'PROVIDER_ERRORS_PRESENT'],
  ['unknown error shape', diagnostics({errorInfo:{failedRecords:1}}), 'ERROR_DIAGNOSTICS_UNCONFIRMED'],
  ['audience status', diagnostics({audienceMembersIngestionStatus:{recordCount:'1'}}), 'UNSUPPORTED_RECEIPT_STATUS_KIND'],
  ['no destinations', {requestStatusPerDestination:[]}, 'SINGLE_DESTINATION_REQUIRED'],
  ['duplicate destination', {requestStatusPerDestination:[...diagnostics().requestStatusPerDestination,...diagnostics().requestStatusPerDestination]}, 'SINGLE_DESTINATION_REQUIRED'],
  ['attribution claim', {...diagnostics(),individualOrderConfirmed:true}, 'ATTRIBUTION_CLAIM_UNSUPPORTED']
]) test(name + ' cannot produce a status repair or receipt confirmation', async () => {
  const f = fixture(undefined,{diagnostics:answer}), result = await f.api.read();
  assert.equal(result.proposedRepairs, 0); assert.equal(result.blockedRows, 1); assert.equal(result.rows[0].code, code);
  assert.equal(result.providerReceiptsConfirmed, 0); assert.equal(f.writes.length, 0);
});

test('safe provider warnings remain visible in a proposed status repair', async () => {
  const f = fixture(undefined,{diagnostics:diagnostics({warningInfo:{warningCounts:[{reason:'PROCESSING_WARNING_REASON_UNSPECIFIED',recordCount:'1'}]}})}), result = await f.api.read();
  assert.equal(result.proposedRepairs, 1); assert.deepEqual(result.rows[0].statusChanges.dmWarnings.to, ['PROCESSING_WARNING_REASON_UNSPECIFIED']);
});

test('original receipt destination is independent of a subsequently configured action', async () => {
  const f = fixture(undefined,{env:{GADS_CONVERSION_ACTION:'customers/888/conversionActions/777'}}), result = await f.api.read();
  assert.equal(result.providerReceiptsConfirmed, 1); assert.equal(result.proposedRepairs, 1); assert.equal(result.rows[0].matchesConfiguredDestination, false);
});

test('duplicate server rows are blocked without inferring individual orders from a shared request', async () => {
  const f = fixture([receipt(),receipt({orderId:'synthetic-other-order'})]), result = await f.api.read();
  assert.equal(result.selectedReceipts, 1); assert.equal(result.providerReceiptsConfirmed, 0); assert.equal(result.proposedRepairs, 0);
  assert.equal(result.blockedRows, 2); assert.equal(result.rows[0].savedRows, 2); assert.equal(result.rows[0].rowKey, null);
  assert.equal(result.rows[0].code, 'DUPLICATE_SAVED_REQUEST'); assert.equal(f.writes.length, 0);
});

test('terminal duplicate outside the pending snapshot is found inside the current-row transaction', async () => {
  const f = fixture([receipt(),receipt({uploaded:true,dmState:'success'})]), result = await f.api.read();
  assert.equal(result.scannedRows, 1); assert.equal(result.providerReceiptsConfirmed, 1); assert.equal(result.proposedRepairs, 0);
  assert.equal(result.rows[0].code, 'DUPLICATE_OR_UNCONFIRMED_SAVED_REQUEST'); assert.equal(result.blockedRows, 1); assert.equal(f.writes.length, 0);
});

for (const [name, mutate, code] of [
  ['sale value', row => ({...row,value:99}), 'ROW_CHANGED'],
  ['refund', row => ({...row,refundedTotal:54}), 'ROW_CHANGED'],
  ['click', row => ({...row,gclid:'synthetic-new-private-click'}), 'ROW_CHANGED'],
  ['destination', row => ({...row,dmDestination:{...clone(target),productDestinationId:'457'}}), 'SAVED_DESTINATION_CHANGED'],
  ['request', row => ({...row,dmRequestId:'synthetic-new-request'}), 'REQUEST_ID_CHANGED'],
  ['foreign lease', row => ({...row,dmReconcileLease:'foreign-worker',dmReconcileLeaseUntil:epoch+90000}), 'ACTIVE_FOREIGN_LEASE'],
  ['expiry', row => ({...row,expiresAtMs:epoch-1}), 'ROW_EXPIRED'],
  ['terminal failure', row => ({...row,failed:true,dmState:'failed'}), 'ROW_NOT_PENDING_RECEIPT'],
  ['attribution claim', row => ({...row,individualOrderConfirmed:true}), 'ATTRIBUTION_CLAIM_UNSUPPORTED']
]) test('in-flight changed ' + name + ' is withheld and the new row remains intact', async () => {
  const f = fixture(undefined,{beforeCurrent:({rows})=>rows.set('row-0',mutate(rows.get('row-0')))}), result = await f.api.read();
  assert.equal(result.providerReceiptsConfirmed, 1); assert.equal(result.proposedRepairs, 0); assert.equal(result.blockedRows, 1);
  assert.equal(result.rows[0].code, code); assert.equal(f.writes.length, 0);
});

test('foreign lease already present is local uncertainty alongside a confirmed provider receipt', async () => {
  const f = fixture([receipt({dmReconcileLease:'foreign-worker',dmReconcileLeaseUntil:epoch+90000})]), result = await f.api.read();
  assert.equal(result.providerReceiptsConfirmed, 1); assert.equal(result.proposedRepairs, 0); assert.equal(result.rows[0].code,'ACTIVE_FOREIGN_LEASE');
  assert.equal(f.transactions.length,0); assert.equal(f.writes.length,0);
});

test('one changed row is withheld while independent unchanged receipts remain proposed', async () => {
  const f = fixture([receipt(),receipt({dmRequestId:'synthetic-second-request'})],{beforeCurrent:({rows})=>rows.set('row-1',{...rows.get('row-1'),value:99})}), result = await f.api.read();
  assert.equal(result.providerReceiptsConfirmed,2); assert.equal(result.proposedRepairs,1); assert.equal(result.blockedRows,1); assert.equal(f.writes.length,0);
});

test('new duplicate or removed row arriving during provider lookup is withheld', async () => {
  const duplicate = fixture(undefined,{beforeCurrent:({rows})=>rows.set('late-duplicate',receipt())}), result = await duplicate.api.read();
  assert.equal(result.rows[0].code,'DUPLICATE_OR_UNCONFIRMED_SAVED_REQUEST'); assert.equal(result.proposedRepairs,0);
  const gone = fixture(undefined,{beforeCurrent:({rows})=>rows.delete('row-0')}), missing = await gone.api.read();
  assert.equal(missing.rows[0].code,'SAVED_ROW_MISSING'); assert.equal(missing.proposedRepairs,0);
});

test('credential refusal and missing scope are precise blockers without echoing secrets', async () => {
  for (const oauth of [response({error:'synthetic-private-buyer@example.invalid'},400),response({access_token:'secret',scope:'unrelated-scope'})]) {
    const f=fixture(undefined,{oauth}), result=await f.api.read(); assert.equal(result.blocked,true); assert.equal(result.proposedRepairs,0);
    assert.equal(f.calls.filter(call=>call.method==='GET').length,0); assert.ok(!JSON.stringify(result).includes('synthetic-private-buyer'));
  }
});

test('provider authorization interruption withholds all selected repairs without reupload', async () => {
  const f=fixture([receipt(),receipt({dmRequestId:'synthetic-second-request'})],{provider:()=>response({error:'private-body'},403)}), result=await f.api.read();
  assert.equal(result.blocked,true); assert.equal(result.proposedRepairs,0); assert.equal(result.blockedRows,2);
  assert.equal(f.calls.filter(call=>call.method==='GET').length,1); assert.equal(f.transactions.length,0); assert.equal(f.writes.length,0);
});

test('total read deadline includes the initial snapshot and final atomic recheck', async () => {
  const f=fixture(undefined,{initialRead:({advance})=>{advance(21000);return {docs:[]};}}), result=await f.api.read();
  assert.equal(result.code,'TIME_LIMIT'); assert.equal(f.calls.length,0);
  const late=fixture(undefined,{beforeCurrent:({advance})=>advance(21000)}), expired=await late.api.read();
  assert.equal(expired.code,'TIME_LIMIT'); assert.equal(expired.proposedRepairs,0); assert.equal(expired.blockedRows,1); assert.equal(late.writes.length,0);
});

test('namespace changes during observation or current snapshot prevent proposals', async () => {
  const f=fixture(undefined,{beforeCurrent:({env})=>{env.BRITES_GROWTH_NAMESPACE='Brites_Growth_Live';}}), result=await f.api.read();
  assert.equal(result.proposedRepairs,0); assert.equal(result.blocked,true); assert.equal(f.writes.length,0);
});

test('batch limit is bounded and does not imply the remaining queue was reconciled', async () => {
  const f=fixture(Array.from({length:30},(_,i)=>receipt({dmRequestId:'synthetic-request-'+i}))), result=await f.api.read({limit:999,maxMs:30000});
  assert.equal(result.limit,25); assert.equal(result.selectedReceipts,25); assert.equal(result.proposedRepairs,25);
  assert.equal(result.observation.selectionComplete,false); assert.equal(result.observation.stopped,'limit'); assert.equal(f.writes.length,0);
});

async function endpoint() {
  const root=path.resolve(__dirname,'../..'), file=path.join(root,'netlify/functions/britesGrowthAds.js'); let source=fs.readFileSync(file,'utf8');
  for (const name of ['_britesGrowth.js','_britesGrowthDemand.js','_britesGrowthReceiptObserver.js','_britesGrowthReceiptReleasePreview.js']) source=source.replace("'./"+name+"'",JSON.stringify(pathToFileURL(path.join(root,'netlify/functions',name)).href));
  return import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
}
test('one protected action passes bounded options to its injected preview and never loads engine', async () => {
  const {createHandler,READ_ACTIONS}=await endpoint(); let calls=0, captured, engines=0;
  const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_GROWTH_ADMIN_KEY:'synthetic-operator'};
  const handler=createHandler({environment:()=>env,savedPasscode:async()=>null,readReceiptPreview:async(_env,options)=>{calls++;captured=options;return {readOnly:true,receiptOnly:true,queueUpdated:false,individualOrdersUpdated:0,proposedRepairs:15};},loadEngine:async()=>{engines++;throw Error('Preview must not load engine');}});
  const call=(body,headers={},method='POST')=>handler(new Request('https://sandbox.example/api/growth-ads',{method,headers:{'Content-Type':'application/json','X-Growth-Key':'synthetic-operator',...headers},...(method==='POST'?{body:JSON.stringify(body)}:{})}));
  assert.ok(READ_ACTIONS.includes('receiptReconciliationPreview'));
  const result=await call({action:'receiptReconciliationPreview',limit:15,maxMs:20000}); assert.equal(result.status,200); assert.deepEqual(captured,{limit:15,maxMs:20000});
  assert.equal((await result.json()).sandboxReadOnly,true); assert.equal(calls,1); assert.equal(engines,0);
  for(const extra of [{rows:[]},{diagnostics:diagnostics()},{requestId:'caller'},{evidenceFingerprint:'caller'},{apply:true},{dryRun:false},{force:true}]) assert.equal((await call({action:'receiptReconciliationPreview',...extra})).status,400);
  assert.equal((await call({action:'receiptReconciliationPreview'},{'X-Growth-Key':'wrong'})).status,401);
  assert.equal((await call({action:'receiptReconciliationPreview'},{Origin:'https://other.example'})).status,403);
  assert.equal((await call({}, {}, 'GET')).status,405);
  env.BRITES_GROWTH_NAMESPACE='Brites_Growth_Live'; assert.equal((await call({action:'receiptReconciliationPreview'})).status,503);
  env.BRITES_GROWTH_NAMESPACE='Brites_Growth_Sandbox'; env.BRITES_GROWTH_SANDBOX='0'; assert.equal((await call({action:'receiptReconciliationPreview'})).status,503);
  assert.equal(calls,1); assert.equal(engines,0);
});
