import core from './_britesGrowth.js';
import review from './_britesGrowthCorrectionReview.js';
import readModule from './_britesGrowthCorrectionRead.js';

const ACTIONS = new Set(['capabilities','loadPacket','savePacket','preview','verifyBaseline']);
const ENV_NAMES = ['BRITES_GROWTH_ADMIN_KEY','BRITES_GROWTH_NAMESPACE','BRITES_GROWTH_SANDBOX','BRITES_GROWTH_CORRECTION_READ_VERSION','FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','SHOPIFY_STORE','SHOPIFY_CLIENT_ID','SHOPIFY_CLIENT_SECRET'];
function environment() { return Object.fromEntries(ENV_NAMES.map(name => [name,Netlify.env.get(name)])); }
function researchService(env) { return core.createGrowthService({db:core.makeDb(env),env}); }
async function savedPasscode(env) { const doc=await core.makeDb(env).collection('config').doc('editPasscode').get(); return doc.exists ? doc.data().passcode : null; }

// No provider/campaign/apply/issue-resolution/AI path. The sole write is one
// validated private packet in the isolated sandbox collection.
export function createHandler(deps={}) {
  const getEnv=deps.environment || environment, getResearch=deps.researchService || researchService, getPasscode=deps.savedPasscode || savedPasscode;
  const getReader=deps.reader || (env => readModule.createCorrectionReader({env})), now=deps.now || Date.now;
  const getPacket=deps.correctionPacket || (async(service,_id,documentId)=>{const snapshot=await service.col('CorrectionPackets').doc(documentId).get();return snapshot.exists?snapshot.data():null;});
  const savePacket=deps.saveCorrectionPacket || (async(service,_id,documentId,record)=>{await service.col('CorrectionPackets').doc(documentId).set(record);});
  return async req => {
    const h={'Content-Type':'application/json','Cache-Control':'no-store','X-Content-Type-Options':'nosniff'};
    const json=(value,status=200)=>new Response(JSON.stringify(value),{status,headers:h});
    const origin=req.headers.get('Origin');
    if(origin && origin!==new URL(req.url).origin) return json({error:'Use the sandbox website to review its private product corrections.'},403);
    if(req.method==='OPTIONS') return new Response(null,{status:204,headers:{...h,'Allow':'GET, POST, OPTIONS'}});
    if(!['GET','POST'].includes(req.method))return json({error:'This endpoint supports read-only review operations.'},405);
    let env;
    try {env=getEnv();} catch {return json({error:'Sandbox configuration is unavailable.'},503);}
    if(env.BRITES_GROWTH_SANDBOX!=='1' || (env.BRITES_GROWTH_NAMESPACE && env.BRITES_GROWTH_NAMESPACE!=='Brites_Growth_Sandbox')) return json({error:'Correction review is available only in the isolated growth sandbox.',canApply:false},503);
    env={...env,BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'};
    const supplied=req.headers.get('X-Growth-Key') || req.headers.get('X-Edit-Passcode');
    let authenticated=core.sameSecret(supplied,env.BRITES_GROWTH_ADMIN_KEY);
    if(!authenticated && supplied) try {authenticated=core.sameSecret(supplied,await getPasscode(env));} catch {}
    if(!authenticated)return json({error:'Operator sign-in required.'},401);
    try {
      const url=new URL(req.url);let body;
      if(req.method==='GET') {
        if([...url.searchParams.keys()].some(k=>k!=='action'))return json({error:'Capabilities accept only an action.'},400);
        body={action:url.searchParams.get('action') || 'capabilities'};
      } else {
        const raw=await req.text();if(Buffer.byteLength(raw,'utf8')>80000)return json({error:'Correction proposal exceeds the bounded review size.'},413);
        try {body=JSON.parse(raw || '{}');} catch {return json({error:'Send a valid typed JSON proposal.'},400);}
      }
      if(!body || typeof body!=='object' || Array.isArray(body))return json({error:'Send one review operation.'},400);
      if(!ACTIONS.has(body.action))return json({error:'This endpoint cannot apply corrections, send queries or modify Shopify.',code:'SANDBOX_REVIEW_ONLY',canApply:false},403);
      if(body.action==='capabilities') {
        if(Object.keys(body).some(k=>k!=='action'))return json({error:'Capabilities do not accept provider/query input.'},400);
        return json({...getReader(env).capabilities(),sandboxReadOnly:true});
      }
      if(req.method!=='POST')return json({error:'Use POST for a private packet operation, preview or recheck.'},405);
      if(body.action==='savePacket') {
        if(Object.keys(body).some(k=>!['action','productId','handle','packet'].includes(k)))return json({error:'Only one exact typed private packet may be saved.'},400);
        const id=review.normalizeExactProductId(body.productId);
        if(!id || !/^[a-z0-9_-]{1,180}$/.test(body.handle || '') || !body.packet || typeof body.packet!=='object' || Array.isArray(body.packet))return json({error:'An exact Shopify Product ID, handle and typed packet are required.'},400);
        const service=getResearch(env),at=now();
        const [dossiers,issueRecords,product]=await Promise.all([service.research([id]),service.productIssues([id]),service.getProduct(id)]);
        const dossier=(dossiers || []).find(d=>d.productId===id && d.handle===body.handle),issueRecord=(issueRecords || []).find(i=>i.productId===id) || null;
        const validated=review.storedPacket({record:body.packet,productId:id,handle:body.handle,dossier,issueRecord,product,now:at});
        if(validated.state!=='available')return json({schemaVersion:1,mode:'sandbox_correction_packet_receipt',stored:false,productId:id,handle:body.handle,state:'rejected',reason:validated.reason},409);
        const record={schemaVersion:1,mode:'sandbox_correction_packet',productId:id,handle:body.handle,proposal:validated.packet.proposal,savedAt:at};
        await savePacket(service,id,review.correctionPacketDocumentId(id),record);
        return json({schemaVersion:1,mode:'sandbox_correction_packet_receipt',stored:true,productId:id,handle:body.handle,packetVersion:validated.packet.packetVersion,savedAt:at});
      }
      if(body.action==='loadPacket') {
        if(Object.keys(body).some(k=>!['action','productId','handle'].includes(k)))return json({error:'Only an exact product may be used to load its private packet.'},400);
        const id=review.normalizeExactProductId(body.productId);
        if(!id || !/^[a-z0-9_-]{1,180}$/.test(body.handle || ''))return json({error:'An exact Shopify Product ID and handle are required.'},400);
        const service=getResearch(env);
        const [dossiers,issueRecords,product,record]=await Promise.all([service.research([id]),service.productIssues([id]),service.getProduct(id),getPacket(service,id,review.correctionPacketDocumentId(id))]);
        const dossier=(dossiers || []).find(d=>d.productId===id && d.handle===body.handle),issueRecord=(issueRecords || []).find(i=>i.productId===id) || null;
        const loaded=review.storedPacket({record,productId:id,handle:body.handle,dossier,issueRecord,product,now:now()});
        if(loaded.state!=='available')return json({schemaVersion:1,mode:'sandbox_correction_packet',readOnly:true,canApply:false,executionCompatibility:'not_established',productId:id,handle:body.handle,state:'not_available',reason:loaded.reason},404);
        return json(loaded.packet);
      }
      const allowed=body.action==='verifyBaseline' ? ['action','productId','handle','proposal','previewBinding'] : ['action','productId','handle','proposal'];
      if(Object.keys(body).some(k=>!allowed.includes(k)))return json({error:'Only an exact product and typed correction proposal are accepted.'},400);
      const id=review.normalizeExactProductId(body.productId);
      if(!id || !/^[a-z0-9_-]{1,180}$/.test(body.handle || ''))return json({error:'An exact Shopify Product ID and handle are required.'},400);
      let proposal;
      try {proposal=review.validateCorrectionProposal(body.proposal);} catch(error){return json({error:error.message},400);}
      if(body.action==='verifyBaseline' && (!body.previewBinding || typeof body.previewBinding!=='object' || Array.isArray(body.previewBinding)))return json({error:'A previous review binding is required for rechecking.'},400);
      const service=getResearch(env);
      const dossiers=await service.research([id]);
      const dossier=(dossiers || []).find(d=>d.productId===id && d.handle===body.handle);
      let issueRecord=null,issueState='unavailable';
      try {const records=await service.productIssues([id]);issueRecord=(records || []).find(i=>i.productId===id) || null;issueState='available';} catch {}
      const input={productId:id,handle:body.handle,proposal,dossier,issueRecord,issueState,now:now(),...(body.action==='verifyBaseline'?{previousBinding:body.previewBinding}:{})};
      // Stale/unbound private packets are rejected before a provider read.
      const preliminary=review.createCorrectionPreview({...input,read:{state:'unavailable',reason:'Current Admin baseline has not been read.'}});
      if(preliminary.conflicts.length)return json(preliminary);
      const read=await getReader(env).read(id,{collections:proposal.draftProductType!=null && proposal.draftProductType!==proposal.expectedProductType});
      return json(review.createCorrectionPreview({...input,read,now:now()}));
    } catch {
      return json({error:'Private correction review is unavailable. Retained proposals and independent work can continue.',mode:'sandbox_review',canApply:false,executionCompatibility:'not_established'},503);
    }
  };
}
export default createHandler();
export const config={path:'/api/growth-corrections',method:['GET','POST','OPTIONS']};
