'use strict';

// Entirely synthetic saved rows and injected GET responses. None of these
// observations calls a provider, creates a checkout, or changes a receipt.
const test = require('node:test');
const assert = require('node:assert/strict');
const {createReceiptObserver, receiptEvidence, canonicalDestination, QUEUE, DM_SCOPE} = require('../../netlify/functions/_britesGrowthReceiptObserver');
const {createReceiptReconciliation} = require('../../netlify/functions/_britesGrowthReceiptReconciliation');
const NOW = Date.parse('2026-10-07T14:20:00Z');
const TARGET = {operatingAccount:{accountType:'GOOGLE_ADS',accountId:'1000000001'},productDestinationId:'2000000001'};
const clone = value => structuredClone(value);
const saved = destination => ({uploaded:false,failed:false,dmState:'processing',dmRequestId:'synthetic-read-only-receipt31',
  dmDestination:clone(destination || TARGET),dmSubmittedAt:NOW-10000,dmChecks:0,
  orderId:'synthetic-not-a-real-order31',gclid:'synthetic-not-a-real-click31',value:1,currency:'USD'});
const status = over => ({destination:clone(TARGET),requestStatus:'SUCCESS',eventsIngestionStatus:{recordCount:'1'},...over});
const diagnostics = row => ({requestStatusPerDestination:[row || status()]});

function fixture(answer, destination = TARGET) {
  const row = saved(destination), before = clone(row), calls = [], writes = [];
  const denied = operation => {writes.push(operation);throw Error('Read-only observation attempted a write');};
  const collection = {where(field,op,value) {
    assert.equal(field,'uploaded');assert.equal(op,'==');assert.equal(value,false);return this;
  },limit(maximum) {assert.equal(maximum,500);return this;},async get() {
    return {docs:[{id:'synthetic-row31',data:()=>clone(row)}]};
  },add:()=>denied('add'),doc:()=>({set:()=>denied('set'),update:()=>denied('update')})};
  const db = {collection(name) {assert.equal(name,QUEUE);return collection;},doc:()=>({get:async()=>({exists:false})}),
    runTransaction:()=>denied('transaction'),batch:()=>denied('batch')};
  const fetch = async (raw,init) => {
    const url = new URL(raw);calls.push({url,init});
    assert.equal(init.redirect,'error');assert.ok(init.signal instanceof AbortSignal);
    if (url.href === 'https://oauth2.googleapis.com/token') {
      assert.equal(init.method,'POST');assert.equal(init.body.has('scope'),false);
      return {ok:true,status:200,json:async()=>({access_token:'synthetic-token31',scope:DM_SCOPE})};
    }
    assert.equal(url.origin+url.pathname,'https://datamanager.googleapis.com/v1/requestStatus:retrieve');
    assert.equal(init.method,'GET');assert.equal(init.body,undefined);
    assert.equal(url.searchParams.size,1);assert.equal(url.searchParams.get('requestId'),row.dmRequestId);
    return {ok:true,status:200,json:async()=>clone(answer)};
  };
  const env = {GADS_DATAMANAGER_CLIENT_ID:'synthetic-client31',GADS_DATAMANAGER_CLIENT_SECRET:'synthetic-secret31',
    GADS_DATAMANAGER_REFRESH_TOKEN:'synthetic-refresh31'};
  return {row,before,calls,writes,read:()=>createReceiptObserver({db,env,fetch,now:()=>NOW}).read()};
}

const unsafeDestinations = [
  ['conflicting deprecated type',{...clone(TARGET),operatingAccount:{...clone(TARGET.operatingAccount),product:'DISPLAY_VIDEO'}}],
  ['numeric account ID',{...clone(TARGET),operatingAccount:{...clone(TARGET.operatingAccount),accountId:1000000001}}],
  ['numeric action ID',{...clone(TARGET),productDestinationId:2000000001}],
  ['account whitespace',{...clone(TARGET),operatingAccount:{...clone(TARGET.operatingAccount),accountId:'1000000001 '}}],
  ['explicit blank current type',{...clone(TARGET),operatingAccount:{product:'GOOGLE_ADS',accountType:'',accountId:'1000000001'}}],
  ['explicit null current type',{...clone(TARGET),operatingAccount:{product:'GOOGLE_ADS',accountType:null,accountId:'1000000001'}}],
  ['array operating account',{...clone(TARGET),operatingAccount:[]}]
];

for (const [name,destination] of unsafeDestinations) test(name+' cannot confirm the saved destination',async()=>{
  assert.equal(canonicalDestination(destination),null);
  const f=fixture(diagnostics(status({destination}))),out=await f.read();
  assert.equal(out.confirmed,0);assert.equal(out.receipts[0].providerReceiptConfirmed,false);
  assert.equal(out.receipts[0].code,'DESTINATION_UNCONFIRMED');assert.deepEqual(f.row,f.before);assert.deepEqual(f.writes,[]);
});

for (const [name,destination] of unsafeDestinations.slice(0,3)) test(name+' in the original row prevents a status GET',async()=>{
  const f=fixture(diagnostics(),destination),out=await f.read();
  assert.equal(out.confirmed,0);assert.equal(out.receipts[0].code,'ORIGINAL_DESTINATION_MISSING');
  assert.equal(f.calls.filter(call=>call.url.hostname==='datamanager.googleapis.com').length,0);
  assert.deepEqual(f.row,f.before);assert.deepEqual(f.writes,[]);
});

const malformedSummaries = [
  ['object error counts',{errorInfo:{errorCounts:{count:1}}},'ERROR_DIAGNOSTICS_UNCONFIRMED'],
  ['unknown error summary',{errorInfo:{failedRecords:1}},'ERROR_DIAGNOSTICS_UNCONFIRMED'],
  ['array error summary',{errorInfo:[]},'ERROR_DIAGNOSTICS_UNCONFIRMED'],
  ['null error count entry',{errorInfo:{errorCounts:[null]}},'ERROR_DIAGNOSTICS_UNCONFIRMED'],
  ['object warning counts',{warningInfo:{warningCounts:{count:1}}},'WARNING_DIAGNOSTICS_UNCONFIRMED'],
  ['unknown warning summary',{warningInfo:{warningCounts:[],privateNote:'synthetic-private-note31'}},'WARNING_DIAGNOSTICS_UNCONFIRMED'],
  ['array warning summary',{warningInfo:[]},'WARNING_DIAGNOSTICS_UNCONFIRMED'],
  ['null warning entry',{warningInfo:{warningCounts:[null]}},'WARNING_DIAGNOSTICS_UNCONFIRMED'],
  ['invalid warning reason',{warningInfo:{warningCounts:[{reason:'synthetic-private-note31',recordCount:'1'}]}},'WARNING_DIAGNOSTICS_UNCONFIRMED'],
  ['excess warning entries',{warningInfo:{warningCounts:Array.from({length:21},()=>({reason:'PROCESSING_WARNING_REASON_UNSPECIFIED',recordCount:'1'}))}},'WARNING_DIAGNOSTICS_UNCONFIRMED']
];
for (const [name,over,code] of malformedSummaries) test(name+' remains uncertainty without a queue write',async()=>{
  const f=fixture(diagnostics(status(over))),out=await f.read();
  assert.equal(out.confirmed,0);assert.equal(out.unconfirmed,1);assert.equal(out.receipts[0].code,code);
  assert.equal(out.receipts[0].providerReceiptConfirmed,false);assert.equal(out.receipts[0].individualOrderConfirmed,false);
  assert.equal(out.queueUpdated,false);assert.equal(out.individualOrdersUpdated,0);
  assert.equal(JSON.stringify(out).includes('synthetic-private-note31'),false);
  assert.deepEqual(f.row,f.before);assert.deepEqual(f.writes,[]);
});

for (const field of ['audienceMembersIngestionStatus','audienceMembersRemovalStatus','removeAllAudienceMembersStatus']) test(field+' cannot coexist with conversion success evidence',async()=>{
  const f=fixture(diagnostics(status({[field]:{recordCount:'1'}}))),out=await f.read();
  assert.equal(out.confirmed,0);assert.equal(out.receipts[0].code,'UNSUPPORTED_RECEIPT_STATUS_KIND');
  assert.deepEqual(f.row,f.before);assert.deepEqual(f.writes,[]);
});

for (const count of ['01','0001','1.0',true]) test('noncanonical record count '+String(count)+' cannot attest one conversion',()=>{
  const out=receiptEvidence(diagnostics(status({eventsIngestionStatus:{recordCount:count}})),TARGET,1);
  assert.equal(out.providerReceiptConfirmed,false);assert.equal(out.code,'RECORD_COUNT_UNCONFIRMED');
});

test('multiple destinations preserve the one exact original conversion observation',async()=>{
  const other={...clone(TARGET),productDestinationId:'2000000002'};
  const f=fixture({requestStatusPerDestination:[status({destination:other,requestStatus:'PROCESSING'}),status()]}),out=await f.read();
  assert.equal(out.confirmed,1);assert.equal(out.receipts[0].destinationRows,2);assert.equal(out.receipts[0].matchingDestinations,1);
  assert.equal(out.receipts[0].providerReceiptConfirmed,true);assert.equal(out.receipts[0].individualOrderConfirmed,false);
  assert.deepEqual(f.row,f.before);assert.deepEqual(f.writes,[]);
});

test('compatible legacy type and safe future warnings remain exact observable evidence',async()=>{
  const destination={...clone(TARGET),operatingAccount:{...clone(TARGET.operatingAccount),product:'GOOGLE_ADS'}};
  const f=fixture(diagnostics(status({destination,warningInfo:{warningCounts:[{reason:'PROCESSING_WARNING_REASON_FUTURE_ENUM',recordCount:'1'}]}})),destination),out=await f.read();
  assert.equal(out.confirmed,1);assert.equal(out.receipts[0].warnings[0].reason,'PROCESSING_WARNING_REASON_FUTURE_ENUM');
  assert.deepEqual(f.row,f.before);assert.deepEqual(f.writes,[]);
});

test('an unsupported attribution claim cannot change receipt-only evidence semantics',async()=>{
  const f=fixture({...diagnostics(),individualOrderConfirmed:true}),out=await f.read();
  assert.equal(out.confirmed,0);assert.equal(out.receipts[0].code,'ATTRIBUTION_CLAIM_UNSUPPORTED');
  assert.equal(out.receipts[0].individualOrderConfirmed,false);assert.deepEqual(f.writes,[]);
});

test('observer and sandbox planner both refuse malformed success evidence without applying anything',()=>{
  const api=createReceiptReconciliation({env:{BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'},now:()=>NOW});
  for (const over of [
    {errorInfo:{failedRecords:1}},
    {audienceMembersIngestionStatus:{recordCount:'1'}},
    {destination:unsafeDestinations[0][1]},
    {destination:unsafeDestinations[1][1]},
    {warningInfo:{warningCounts:[{reason:'synthetic-private-note31'}]}}
  ]) {
    const raw=diagnostics(status(over)),row=saved(),observation=receiptEvidence(raw,TARGET,1);
    const plan=api.plan({rows:[{id:'synthetic-row31',row}],evidence:[{rowId:'synthetic-row31',requestId:row.dmRequestId,
      destination:row.dmDestination,observedAt:NOW,diagnostics:raw,individualOrderConfirmed:false}]});
    assert.equal(observation.providerReceiptConfirmed,false);assert.equal(plan.ok,false);
  }
});
