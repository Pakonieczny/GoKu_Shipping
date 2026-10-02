import core from './_britesGrowth.js';
import deadline from './_britesConciergeVoiceDeadline.js';
export default async()=>{
  const env=Object.fromEntries(['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','BRITES_GROWTH_NAMESPACE','OPENAI_API_KEY','BRITES_CONCIERGE_REALTIME_ENABLED'].map(k=>[k,Netlify.env.get(k)]));env.BRITES_GROWTH_NAMESPACE=core.namespace(env);
  if(!deadline.enabled(env))return;
  const db=core.makeDb(env),service=core.createGrowthService({db,env});await deadline.reap({env,service});
};
export const config={schedule:'* * * * *'};
