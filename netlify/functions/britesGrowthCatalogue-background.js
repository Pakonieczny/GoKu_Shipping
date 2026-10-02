import core from './_britesGrowth.js';

export default async (req) => {
  const env=Object.fromEntries(['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','SHOPIFY_STORE','SHOPIFY_CLIENT_ID','SHOPIFY_CLIENT_SECRET','BRITES_GROWTH_NAMESPACE','BRITES_GROWTH_ADMIN_KEY'].map(k=>[k,Netlify.env.get(k)]));
  if(!core.sameSecret(req.headers.get('X-Growth-Key'),env.BRITES_GROWTH_ADMIN_KEY))return new Response(null,{status:401});
  const db=core.makeDb(env),shopify=core.createShopify({env}),service=core.createGrowthService({db,env,shopify}),ref=service.col('State').doc('catalogue-worker'),owner=crypto.randomUUID(),started=Date.now();
  const claimed=await db.runTransaction(async tx=>{const s=await tx.get(ref);if(s.exists&&s.data().leaseUntil>Date.now())return false;tx.set(ref,{owner,leaseUntil:Date.now()+14*60000,startedAt:started});return true;});if(!claimed)return;
  try{while(Date.now()-started<12*60000){const result=await service.syncCatalogue();if(result.stopped||result.complete)break;}}catch(e){await service.block({id:'catalogue-sync',task:'Live catalogue refresh',detail:core.clean(e.message,800)});}finally{await db.runTransaction(async tx=>{const s=await tx.get(ref);if(s.exists&&s.data().owner===owner)tx.update(ref,{leaseUntil:0,finishedAt:Date.now()});});}
};
