'use strict';
const core=require('./_britesGrowth');
const MAX_BYTES=250000,MAX_AGE=86400000;
function createDemandStore(service,{now=Date.now}={}){
  async function current(id){
    if(!/^gid:\/\/shopify\/Product\/\d+$/.test(String(id||'')))throw Error('An exact Product ID is required.');
    const [product,dossiers]=await Promise.all([service.getProduct(id),service.research([id])]);
    const dossier=dossiers.find(d=>d.productId===id&&d.handle===product?.handle&&d.status==='approved');
    if(!product||!dossier)throw Error('Current approved exact-product research is required.');
    return{product,dossier};
  }
  async function save(evidence){
    if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)||Buffer.byteLength(JSON.stringify(evidence),'utf8')>MAX_BYTES)throw Error('A bounded demand evidence record is required.');
    const {product,dossier}=await current(evidence.productId),at=now();
    if(evidence.schemaVersion!==1||evidence.evidenceKind!=='product_demand_and_ad_outcomes'||evidence.readOnly!==true||evidence.sandboxReadOnly!==true||evidence.handle!==product.handle||evidence.dossierVersion!==dossier.version||!Number.isFinite(evidence.at)||evidence.at>at+60000||at-evidence.at>MAX_AGE)throw Error('Fresh demand evidence must match the current exact product and approved research version.');
    if(!evidence.sources||!evidence.shopping||!evidence.search||!evidence.pmax||!evidence.planner)throw Error('Demand evidence needs its observed source states and scope labels.');
    const savedAt=now();await service.col('Demand').doc(core.hash(product.id).slice(0,40)).set({productId:product.id,evidence,savedAt});
    return{ok:true,productId:product.id,dossierVersion:dossier.version,savedAt};
  }
  async function read(ids){
    if(!Array.isArray(ids)||ids.length>20)throw Error('Request at most 20 exact Product IDs.');
    const out=[];
    for(const id of [...new Set(ids)]){
      if(!/^gid:\/\/shopify\/Product\/\d+$/.test(String(id||'')))throw Error('An exact Product ID is required.');
      const snap=await service.col('Demand').doc(core.hash(id).slice(0,40)).get();if(!snap.exists)continue;
      const record=snap.data(),e=record.evidence;if(record.productId!==id||e?.productId!==id)continue;
      let product=null,dossier=null;try{({product,dossier}=await current(id));}catch{}
      let issueRecords=[],productIssueState='unavailable';try{issueRecords=await service.productIssues([id]);productIssueState='available';}catch{}
      const productHolds=core.productIssueHolds(issueRecords.find(r=>r.productId===id));
      const fresh=!!product&&!!dossier&&e.handle===product.handle&&e.dossierVersion===dossier.version&&Number.isFinite(e.at)&&e.at<=now()+60000&&now()-e.at<=MAX_AGE;
      out.push({productId:id,state:fresh?'current':'stale',evidence:e,savedAt:record.savedAt,productHolds,productIssueState,promotionAllowed:fresh&&productIssueState==='available'&&!productHolds.recommendationHold});
    }
    return out;
  }
  return{save,read};
}
module.exports={createDemandStore,MAX_BYTES,MAX_AGE};
