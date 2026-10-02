import core from './_britesGrowth.js';
import voice from './_britesConciergeVoice.js';
const names=['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','BRITES_GROWTH_NAMESPACE','BRITES_GROWTH_ADMIN_KEY','OPENAI_API_KEY','BRITES_CONCIERGE_REALTIME_ENABLED','BRITES_CONCIERGE_REALTIME_MODEL','BRITES_CONCIERGE_REALTIME_RESERVE_USD'];
export default async(req)=>{
  const env=Object.fromEntries(names.map(k=>[k,Netlify.env.get(k)]));
  env.BRITES_GROWTH_SANDBOX=core.namespace(env)==='Brites_Growth_Sandbox'?'1':'0';
  // A disabled preview needs neither storage nor a provider connection.
  if(env.BRITES_CONCIERGE_REALTIME_ENABLED!=='1')return voice.createHandler({env})(req);
  try{
    const db=core.makeDb(env),service=core.createGrowthService({db,env});
    async function authorize(request){const supplied=request.headers.get('X-Growth-Key')||request.headers.get('X-Edit-Passcode');if(!supplied)return false;if(core.sameSecret(supplied,env.BRITES_GROWTH_ADMIN_KEY))return true;const saved=await db.collection('config').doc('editPasscode').get();return saved.exists&&core.sameSecret(supplied,saved.data().passcode);}
    async function scheduleHangup({callId,expiresAt}){
      const ref=service.col('VoiceDeadlines').doc(callId);await ref.set({callId,expiresAt,state:'pending',at:Date.now()});
      const token=voice.stopToken(callId,expiresAt,env.OPENAI_API_KEY);
      const r=await fetch(new URL('/.netlify/functions/britesConciergeVoiceDeadline-background',req.url),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token}),signal:AbortSignal.timeout(5000)});
      // Provider allocation is made usable only after the recorded deadline and
      // accepted background invocation. The independent minute reaper retries
      // pending deadlines if background delivery/execution later fails.
      return r.status===202;
    }
    return await voice.createHandler({env,authorize,service,scheduleHangup})(req);
  }catch{return new Response(JSON.stringify({enabled:false,code:'VOICE_PREVIEW_UNAVAILABLE',message:'Voice is unavailable in this preview. Text and optional narration remain available.'}),{status:503,headers:{'Content-Type':'application/json','Cache-Control':'no-store'}});}
};
export const config={path:'/api/concierge-voice'};
