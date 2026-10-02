import core from './_britesGrowth.js';

export default async () => {
  const env=Object.fromEntries(['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','BRITES_GROWTH_NAMESPACE','BRITES_GROWTH_ADMIN_KEY'].map(k=>[k,Netlify.env.get(k)]));
  const service=core.createGrowthService({db:core.makeDb(env),env}),ctrl=await service.setup();if(!ctrl.enabled||Date.now()>=Math.min(ctrl.stopAt||core.STOP_AT,core.STOP_AT))return;
  await service.col('State').doc('heartbeat').set({at:Date.now(),mode:'catalogue-only; research uses Work continuation'});
  if(!ctrl.catalogueComplete||Date.now()-Number(ctrl.lastCatalogueSyncAt||0)>6*3600000){const site=Netlify.env.get('URL');if(!site||!env.BRITES_GROWTH_ADMIN_KEY)return;await fetch(new URL('/.netlify/functions/britesGrowthCatalogue-background',site),{method:'POST',headers:{'X-Growth-Key':env.BRITES_GROWTH_ADMIN_KEY},body:'{}',signal:AbortSignal.timeout(10000)});}
};
export const config = { schedule: '@hourly' };
