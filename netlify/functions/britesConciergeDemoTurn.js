import core from './_britesGrowth.js';
import demo from './_britesConciergeDemoTurn.js';

function gatewayBase(raw){const value=String(raw||'').replace(/\/+$/,'');return /^https:\/\//.test(value)?value:null;}
async function completeTone({model,maxTokens,system,prompt},base){
  const response=await fetch(base+'/chat/completions',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({model,messages:[{role:'system',content:system},{role:'user',content:prompt}],response_format:{type:'json_object'},temperature:0.1,max_tokens:maxTokens}),signal:AbortSignal.timeout(12000)});
  if(!response.ok)throw Error('Voice language service unavailable.');
  const value=await response.json(),content=value?.choices?.[0]?.message?.content;
  if(typeof content!=='string'||content.length>500)throw Error('Invalid voice language response.');
  return content;
}
export default async(req,context)=>{
  const names=['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','BRITES_GROWTH_NAMESPACE','OPENAI_BASE_URL','BRITES_CONCIERGE_DEMO_ENABLED'];
  const env=Object.fromEntries(names.map(name=>[name,Netlify.env.get(name)]));
  env.BRITES_GROWTH_SANDBOX=core.namespace(env)==='Brites_Growth_Sandbox'?'1':'0';
  const base=gatewayBase(env.OPENAI_BASE_URL);
  let rateLimit=async()=>false;
  try{const db=core.makeDb(env),service=core.createGrowthService({db,env});rateLimit=key=>service.rateLimit(key,12);}catch{}
  return demo.createHandler({env,rateLimit,completeTone:base?input=>completeTone(input,base):null})(req,context);
};
export const config={path:'/api/concierge-demo-turn'};
