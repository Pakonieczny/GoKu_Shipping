import core from './_britesGrowth.js';
import demo from './_britesConciergeDemoTurn.js';

export default async(req,context)=>{
  const names=['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','BRITES_GROWTH_NAMESPACE','OPENAI_API_KEY','OPENAI_BASE_URL','NETLIFY_AI_GATEWAY_KEY','NETLIFY_AI_GATEWAY_URL','BRITES_CONCIERGE_DEMO_ENABLED'];
  const env=Object.fromEntries(names.map(name=>[name,Netlify.env.get(name)]));
  env.BRITES_GROWTH_SANDBOX=core.namespace(env)==='Brites_Growth_Sandbox'?'1':'0';
  const automatic=globalThis.process?.env||{};
  const completeTone=demo.createGatewayTone({base:automatic.NETLIFY_AI_GATEWAY_URL||env.NETLIFY_AI_GATEWAY_URL||automatic.OPENAI_BASE_URL||env.OPENAI_BASE_URL,key:automatic.NETLIFY_AI_GATEWAY_KEY||env.NETLIFY_AI_GATEWAY_KEY||automatic.OPENAI_API_KEY||env.OPENAI_API_KEY});
  let rateLimit=async()=>false;
  try{const db=core.makeDb(env),service=core.createGrowthService({db,env});rateLimit=key=>service.rateLimit(key,12);}catch{}
  return demo.createHandler({env,rateLimit,completeTone})(req,context);
};
export const config={path:'/api/concierge-demo-turn'};
