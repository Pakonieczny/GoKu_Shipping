'use strict';
const voice=require('./_britesConciergeVoice.js');
const enabled=env=>env.BRITES_CONCIERGE_REALTIME_ENABLED==='1'&&env.BRITES_GROWTH_NAMESPACE==='Brites_Growth_Sandbox'&&!!env.OPENAI_API_KEY;
async function finish({env,service,callId,fetch:fetcher=globalThis.fetch,now=Date.now}){
  if(!enabled(env)||!/^rtc_[A-Za-z0-9_-]{1,180}$/.test(callId||''))return false;
  const ref=service.col('VoiceDeadlines').doc(callId),s=await ref.get();if(!s.exists)return false;const row=s.data();if(row.state==='closed')return true;
  if(row.callId!==callId||!Number.isSafeInteger(row.expiresAt)||row.expiresAt>now())return false;
  const r=await fetcher('https://api.openai.com/v1/realtime/calls/'+encodeURIComponent(callId)+'/hangup',{method:'POST',headers:{Authorization:'Bearer '+env.OPENAI_API_KEY},signal:AbortSignal.timeout(5000)});
  // 404 means the provider no longer has an active call. Unknown failures stay
  // pending for the independent reaper; no usage allocation is released here.
  if(!r.ok&&r.status!==404){await ref.set({lastAttemptAt:now(),lastStatus:r.status},{merge:true});return false;}
  await ref.set({state:'closed',closedAt:now()},{merge:true});return true;
}
async function background({env,service,token,fetch,now=Date.now,wait=ms=>new Promise(resolve=>setTimeout(resolve,ms))}){
  if(!enabled(env))return false;const value=voice.readStopToken(token,env.OPENAI_API_KEY,now());if(!value||value.expiresAt>now()+voice.MAX_DURATION_MS+10000)return false;
  // Netlify's background execution survives the initiating HTTP response.
  const delay=Math.max(0,value.expiresAt-now());if(delay)await wait(delay);
  return finish({env,service,callId:value.callId,fetch,now});
}
async function reap({env,service,fetch,now=Date.now}){
  if(!enabled(env))return {closed:0};
  const rows=await service.col('VoiceDeadlines').where('state','==','pending').limit(20).get();let closed=0;
  const due=rows.docs.filter(row=>Number.isSafeInteger(row.data().expiresAt)&&row.data().expiresAt<=now()).slice(0,4);
  const results=await Promise.all(due.map(row=>finish({env,service,callId:row.id,fetch,now}).catch(()=>false)));closed=results.filter(Boolean).length;
  return {closed};
}
module.exports={enabled,finish,background,reap};
