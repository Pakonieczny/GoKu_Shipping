"use strict";
// Cheap source polling and market refresh. Only new documents go to extraction;
// investment research is queued solely by the existing material-event router.
const A = require("./_investorAdmin"), AUTH = require("./_investorAuth"), J = require("./_investorJobs");
const M = require("./_investorMarket"), E = require("./_investorEvidence"), IS = require("./_investorIntelligenceSources");
const D = require("./_investorDossier"), R = require("./_investorEventRouter"), O = require("./_investorOpenai");
const U = require("./_investorUniverse"), P = require("./_investorPortfolio");
const C = require("./_investorDecisionContext");
const TASK = "event_ingest", FN = "investorEvents-background";
function pollingSymbols({held=[],pending=[],watch=[],eligible=[],slot=0}) {
  const start = (slot*8)%Math.max(1,eligible.length);
  return [...new Set([...held,...pending,...watch.slice(0,24),...[...eligible,...eligible].slice(start,start+8)])];
}
const DEFAULT_DEPS={A,AUTH,J,M,E,IS,D,R,O,U,P};
async function run(claim,{deps={}}={}) {
  const {A,J,M,E,IS,D,R,O,U,P}={...DEFAULT_DEPS,...deps};
  const accountId=claim.payload.accountId || "paper-1", nowMs=Date.now(), date=M.sessionState(new Date(nowMs)).date;
  const controlSnap=await A.col(A.COL.control).doc("control").get(), control=controlSnap.exists?controlSnap.data():{};
  if(control.engineMode!=="manager" || control.managerPaused===true || control.managerState==="PAUSED") return {summary:{skipped:true,reason:"MANAGER_PAUSED"}};
  const all=[...(U.tradeTier || []),...(U.researchTier || [])], by=new Map(all.map(x=>[x.symbol,x]));
  let cp=claim.checkpoint && claim.checkpoint.data;
  if (!cp) {
    const portfolio=await P.snapshot({accountId}), watch=[];
    const control=await A.col(A.COL.control).doc("control").get(), ctrl=control.exists?control.data():{};
    if (ctrl.lastManagerRunId) (await A.col(A.COL.managerDecisions).where("managerRunId","==",ctrl.lastManagerRunId).get()).forEach(d=>{
      const x=d.data()._codec?require("./_investorStorageCodec").decode(d.data()):d.data(); if(x.decision === "WATCH") watch.push(x.symbol);
    });
    const eligible=(U.tradeTier || []).map(x=>x.symbol);
    cp={startedAtMs:nowMs,globalDone:false,cursor:0,changed:[],processed:[],sourceErrors:[],
      symbols:pollingSymbols({held:portfolio.positions.map(x=>x.symbol),pending:portfolio.workingOrders.map(x=>x.symbol),watch,eligible,slot:Math.floor(nowMs/600000)})};
    // A shared market request also measures whole-universe breadth. Missing
    // symbols remain missing; no repeated AI underwriting accompanies this pull.
    const marketSymbols=[...new Set([...eligible,...cp.symbols,"SPY","QQQ","HYG","LQD","IEF","TLT","GLD","UUP",...Object.values(require("./_investorTemporal").DRIVER_BY_SECTOR)])];
    try {
      const data=await M.fetchBarsChunked(marketSymbols,{timeframe:"1Min",limit:120,recentMinutes:120});
      for(const [symbol,bars] of Object.entries(data.bars || {})) await M.writeBars(symbol,date,bars,{provider:data.provider,feed:data.feed || null,adjustment:data.adjustment || null,sourceSha256:data.symbolSha256 && data.symbolSha256[symbol] || data.sha256 || null});
    } catch(e) { cp.sourceErrors.push({source:"market",error:String(e.code || e.message).slice(0,100)}); }
  }
  if (!cp.globalDone) {
    const profiles=all.map(x=>IS.profileFor(x));
    const sweep=IS.createSweep({companies:all.map(x=>x.symbol)});
    // Fetch each registered shared feed once and match it against all issuers.
    for(const source of Object.values(IS.SOURCE_REGISTRY).filter(s=>s.scope === "global" && s.kind === "feed")) {
      try {
        const fetched=await IS.fetchGlobalOnce(sweep,source,`event|${source.source_id}`,()=>IS.sweepIo(sweep).fetch(source.url,{sourceId:source.source_id,accept:["xml","rss","atom","text"],timeoutMs:18000,maxBytes:2*1024*1024}));
        if (!fetched.ok) throw fetched.error;
        for(const item of IS.parseFeed(fetched.res.text).slice(0,100)) for(const profile of profiles) {
          if(source.source_id === "federalreserve.press" && profile === profiles[0]) await E.recordVersion({symbol:"SPY",sourceId:source.source_id,
            entry:{...item,accession:C.hash(item.link || item.title).slice(0,32),sourceMeta:IS.sourceMeta(source,item)},rawSha256:fetched.res.sha256,body:String(item.body || item.summary || "").slice(0,120000)});
          if (!IS.matchesProfile(item,profile,source)) continue;
          const rec=await E.recordVersion({symbol:profile.symbol,sourceId:source.source_id,
            entry:{...item,accession:C.hash(item.link || `${item.title}|${item.updated}`).slice(0,32),sourceMeta:IS.sourceMeta(source,item),discoveryOnly:source.discovery_only === true},
            rawSha256:fetched.res.sha256,body:source.full_text_allowed?String(item.body || item.summary || "").slice(0,120000):""});
          if(rec.created) cp.changed.push(profile.symbol);
        }
      } catch(e) { cp.sourceErrors.push({source:source.source_id,error:String(e.code || e.message).slice(0,100)}); }
    }
    cp.globalDone=true;
    await J.checkpoint(claim,{stage:"events",data:cp});
  }
  while(cp.cursor<cp.symbols.length && J.segmentBudgetRemainingMs(claim)>90000) {
    const symbol=cp.symbols[cp.cursor], row=by.get(symbol) || {symbol};
    try {
      const profile=await IS.resolveIdentity(row);
      if(profile.cik) {
        const latest=await E.pollEdgar(profile.cik,{forms:""});
        const known=await E.documentsForCompany(symbol,Date.now(),10,120);
        const accessions=new Set(known.map(d=>d.accession));
        for(const item of (latest.entries || []).filter(x=>!accessions.has(x.accession)).slice(0,8)) {
          const body=await E.fetchFilingBody(item).catch(()=>({text:""}));
          const rec=await E.recordVersion({symbol,sourceId:item.sourceId || "sec.latest",entry:{...item,sourceClass:"company_primary"},rawSha256:latest.sha256,body:body.text,bodySha256:body.sha256});
          if(rec.created) cp.changed.push(symbol);
        }
      }
      const sweep=IS.createSweep({companies:[symbol]});
      // Company websites use their existing entitlement and source guards.
      await IS.pollCompany({...profile,sourceIds:(profile.sourceIds || []).filter(s=>s === "company.direct")},{budgetMs:30000,sweep});
      const docs=await E.documentsForCompany(symbol,Date.now(),10,120);
      if(docs.some(d=>Number(d.decisionKnownAtMs)>=cp.startedAtMs)) cp.changed.push(symbol);
    } catch(e) { cp.sourceErrors.push({symbol,error:String(e.code || e.message).slice(0,100)}); }
    await J.renewRunLease(claim);
    cp.cursor++;
    await J.checkpoint(claim,{stage:"events",data:cp});
  }
  for(const symbol of [...new Set(cp.changed)].filter(s=>!cp.processed.includes(s))) {
    if(J.segmentBudgetRemainingMs(claim)<90000) break;
    const docs=(await E.documentsForCompany(symbol,Date.now(),10,120)).filter(d=>Number(d.decisionKnownAtMs)>=cp.startedAtMs);
    for(let i=0;i<docs.length;i+=6) {
      const batch=docs.slice(i,i+6).map(d=>({...d,sourcePublishedAt:d.source_published_at}));
      const ex=await O.extractFacts({symbol,documentVersions:batch});
      if(ex.ok) for(const doc of batch) await E.recordClaims({symbol,documentId:doc.documentId,documentVersionId:doc.versionId,sourceId:doc.sourceId,
        publishedAtMs:Date.parse(doc.source_published_at || "") || null,firstSeenAtMs:Date.now(),extractedBy:ex.model,claims:(ex.claims || []).filter(c=>c.documentVersionId===doc.versionId)});
    }
    const built=await D.buildBaseline({symbol,rosterRow:by.get(symbol),asOfMs:Date.now()});
    await D.persistVersion(built.version,{sourceAtMs:Date.now()});
    // Route only after the new facts/dossier are readable by the revision job.
    for(const doc of docs) await R.routeEvidenceEvent({symbol,accountId,document:doc});
    await J.renewRunLease(claim);
    cp.processed.push(symbol);
    await J.checkpoint(claim,{stage:"events",data:cp});
  }
  const remaining=cp.cursor<cp.symbols.length || [...new Set(cp.changed)].some(s=>!cp.processed.includes(s));
  return {yielded:remaining,checkpoint:{stage:"events",data:cp},summary:{symbolsPolled:cp.cursor,changed:cp.processed,sharedFeedsAllIssuers:true,sourceErrors:cp.sourceErrors.slice(-30)}};
}
exports.handler=async event=>{
  let body;try{body=JSON.parse(event.body || "{}");}catch{return{statusCode:400,body:"invalid JSON"};}
  if(body.task !== TASK || !body.jobId) return{statusCode:400,body:"invalid job"};
  await AUTH.loadAuthSecrets();
  const got=await J.claimOnce({jobId:body.jobId,task:TASK,targetFunction:FN,token:body.nonce,payload:body.payload || {}});
  if(!got.claimed) return{statusCode:got.httpStatus || 409,body:JSON.stringify(got)};
  try {
    const lease=await J.claimRunLease(got.claim,{runId:`event_ingest_${got.claim.payload.accountId || "paper-1"}`});
    if(!lease.claimed) {await J.yieldSegment(got.claim,{reason:"event_ingest_in_flight",resumeAtMs:lease.until+1000});return {statusCode:202,body:JSON.stringify({ok:true,pending:true})};}
    await M.loadMarketSettings();
    const out=await run(got.claim);
    if(out.yielded) await J.yieldSegment(got.claim,{checkpoint:out.checkpoint,reason:"event_poll_continuation",resumeAtMs:Date.now()+5000});
    else await J.complete(got.claim,out.summary);
    return{statusCode:out.yielded?202:200,body:JSON.stringify({ok:true,summary:out.summary})};
  }catch(e){await J.failClosed(got.claim,{code:e.code || "EVENT_INGEST_FAILED",message:e.message,retryable:true});return{statusCode:500,body:JSON.stringify({ok:false,error:e.code || "EVENT_INGEST_FAILED"})};}
};
exports.pollingSymbols=pollingSymbols;exports.run=run;
