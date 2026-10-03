import core from './_britesGrowth.js';
import deadline from './_britesConciergeVoiceDeadline.js';
export default async(req)=>{
  const env=Object.fromEntries(['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','BRITES_GROWTH_NAMESPACE','OPENAI_API_KEY','BRITES_CONCIERGE_OPENAI_API_KEY','BRITES_CONCIERGE_REALTIME_ENABLED'].map(k=>[k,Netlify.env.get(k)]));
  env.OPENAI_API_KEY=env.BRITES_CONCIERGE_OPENAI_API_KEY||env.OPENAI_API_KEY;
  env.BRITES_GROWTH_NAMESPACE=core.namespace(env);
  if(!deadline.enabled(env)||req.method!=='POST')return;
  try{const raw=await req.text();if(raw.length>2000)return;const body=JSON.parse(raw);if(Object.keys(body).some(k=>k!=='token'))return;const db=core.makeDb(env),service=core.createGrowthService({db,env});await deadline.background({env,service,token:body.token});}catch{ /* pending deadline remains visible to the reaper */ }
};
