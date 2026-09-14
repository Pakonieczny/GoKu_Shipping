// Evaluation only: this service has no image-generation, rendering or publication dependency.
const crypto=require('crypto');
const hash=x=>crypto.createHash('sha256').update(JSON.stringify(x)).digest('hex');
function createEvaluationService(D){
 const safe=j=>({ok:true,evaluationId:j.id,phase:j.phase,quality:j.quality||null,error:j.error||null,startedAt:j.createdAt,updatedAt:j.updatedAt,sourceHash:j.sourceHash,estimatedUsd:j.quality?.estimatedUsd??null});
 async function start(input){
  if(!/^[a-zA-Z0-9_-]{8,100}$/.test(input.requestId||''))throw Error('A unique evaluation request is required.');
  const source=await D.source(input),target=source.ref.collection('evaluations').doc(input.requestId),id=target.id;
  let job,queued=false;await D.db().runTransaction(async tx=>{const old=await tx.get(target);if(old.exists){job=old.data();return;}job={id,phase:'queued',kind:input.kind,workspaceId:source.workspaceId,productId:input.productId,groupRef:input.groupRef,sourceJobId:source.jobId,sourceHash:hash(source.snapshot),snapshot:source.snapshot,createdAt:Date.now(),updatedAt:Date.now()};tx.set(target,job);queued=true;});
  return {...safe(job),queued,workspaceId:source.workspaceId};
 }
 async function status(input){
  const source=await D.source(input),rows=await source.ref.collection('evaluations').get(),job=rows.docs.map(d=>d.data()).filter(j=>j.kind===input.kind&&j.sourceJobId===source.jobId&&j.sourceHash===hash(source.snapshot)&&(!input.evaluationId||j.id===input.evaluationId)).sort((a,b)=>b.createdAt-a.createdAt)[0];
  if(!job)return {ok:true,phase:'idle'};
  if(['queued','running'].includes(job.phase)&&Date.now()-job.updatedAt>14*60000)return {...safe(job),phase:'failed',error:'Evaluation stopped before completion. Existing ads and the earlier review are retained.'};
  return safe(job);
 }
 async function run(input){
  const target=D.ref(input.workspaceId).collection('evaluations').doc(input.evaluationId);let job;
  await D.db().runTransaction(async tx=>{const s=await tx.get(target);if(!s.exists||s.data().phase!=='queued')return;job=s.data();tx.update(target,{phase:'running',updatedAt:Date.now()});});
  if(!job)return {ok:true,cached:true};
  const heartbeat=setInterval(()=>target.update({updatedAt:Date.now()}).catch(()=>{}),20000);
  try{const s=job.snapshot,refs=await Promise.all(s.references.map(a=>D.load(a))),files=await Promise.all(s.files.map(a=>D.load(a))),receipt=target.collection('data').doc('response'),old=await receipt.get();
   const quality=await D.review(refs[0],files,s.brief,refs,job.id,{...(old.exists?{rawResponse:old.data().response}:{}),onResponse:response=>receipt.set({response,at:Date.now()})});
   if(!quality.categoryReviews)throw Error('The new review did not include category explanations.');
   await target.update({phase:'complete',quality,updatedAt:Date.now()});return {ok:true};
  }catch(e){await target.update({phase:'failed',error:String(e.message||e).slice(0,1500),updatedAt:Date.now()});return {ok:false};}finally{clearInterval(heartbeat);}
 }
 return {start,status,run};
}
module.exports={createEvaluationService};
