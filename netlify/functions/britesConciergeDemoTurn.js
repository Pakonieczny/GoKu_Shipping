import core from './_britesGrowth.js';
import demo from './_britesConciergeDemoTurn.js';
import voice from './_britesConciergeVoice.js';

export default async(req,context)=>{
  const names=['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','BRITES_GROWTH_NAMESPACE','OPENAI_API_KEY','OPENAI_BASE_URL','NETLIFY_AI_GATEWAY_KEY','NETLIFY_AI_GATEWAY_URL','BRITES_CONCIERGE_OPENAI_API_KEY','BRITES_CONCIERGE_DEMO_ENABLED','BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP'];
  const env=Object.fromEntries(names.map(name=>[name,Netlify.env.get(name)]));
  env.BRITES_GROWTH_SANDBOX=core.namespace(env)==='Brites_Growth_Sandbox'?'1':'0';
  const automatic=globalThis.process?.env||{};
  // Keep provider credentials paired with their own endpoint. The dedicated
  // concierge OpenAI key must never be sent to a gateway/custom base URL.
  const completeTone=demo.createGatewayTone({base:automatic.NETLIFY_AI_GATEWAY_URL||env.NETLIFY_AI_GATEWAY_URL,key:automatic.NETLIFY_AI_GATEWAY_KEY||env.NETLIFY_AI_GATEWAY_KEY})
    ||demo.createGatewayTone({base:automatic.OPENAI_BASE_URL||env.OPENAI_BASE_URL,key:automatic.OPENAI_API_KEY||env.OPENAI_API_KEY})
    ||demo.createGatewayTone({base:'https://api.openai.com/v1',key:env.BRITES_CONCIERGE_OPENAI_API_KEY});
  let rateLimit=async()=>false,reserveConversation=async()=>null;
  try{const db=core.makeDb(env),service=core.createGrowthService({db,env});rateLimit=key=>service.rateLimit(key,12);const capUsd=Number(env.BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP);if(Number.isFinite(capUsd)&&capUsd>0&&capUsd<=25)reserveConversation=voice.createDemoBudgetReservation(service,{capUsd});}catch{}
  return demo.createHandler({env,rateLimit,completeTone,reserveConversation})(req,context);
};
export const config={path:'/api/concierge-demo-turn'};
