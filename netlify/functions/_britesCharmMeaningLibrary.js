'use strict';
const crypto=require('node:crypto');
const shared=require('../../brites-charm-story-library.js');
const RECORD_FIELDS=['schema','id','motif','aliases','status','provenance','context','facts','interpretation','sources'];
const STORED_FIELDS=[...RECORD_FIELDS,'version','savedAt'];
const hash=value=>crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const validProduct=id=>/^gid:\/\/shopify\/Product\/[1-9]\d{0,19}$/.test(id||'');
function fields(value,names){return !!value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>names.includes(key));}
function validateRecord(input,now=Date.now(),stored=false){
  if(!fields(input,stored?STORED_FIELDS:RECORD_FIELDS)||input.schema!==1||!/^[a-z0-9][a-z0-9_-]{0,79}$/.test(input.id||'')||input.provenance!=='agent_researched'||!['active','inactive'].includes(input.status))return null;
  if(!Array.isArray(input.aliases)||!input.aliases.length||input.aliases.length>32||input.aliases.some(alias=>typeof alias!=='string'||!/^[a-z][a-z -]{0,79}$/.test(alias))||new Set(input.aliases).size!==input.aliases.length)return null;
  const row=shared.normalizeStory({schema:1,kind:'researched-story',provenance:'agent_researched',productId:'gid://shopify/Product/1',handle:'validation-only',productUrl:'https://britesjewelry.com/products/validation-only',productTitle:String(input.motif)+' validation only',productCheckedAt:now,checkedAt:now,libraryId:input.id,libraryVersion:'0'.repeat(64),motif:input.motif,context:input.context,facts:input.facts,interpretation:input.interpretation,sources:input.sources},now);
  if(!row)return null;
  const clean={schema:1,id:input.id,motif:row.motif,aliases:input.aliases.slice(),status:input.status,provenance:'agent_researched',context:row.context,facts:row.facts,interpretation:row.interpretation,sources:row.sources};
  if(stored&&(!/^[a-f0-9]{64}$/.test(input.version||'')||input.version!==hash(clean)||!Number.isFinite(input.savedAt)||input.savedAt>now+60000))return null;
  return clean;
}
function phrase(value){return String(value||'').toLowerCase().replace(/[_-]+/g,' ').replace(/[^a-z0-9 ]/g,' ').replace(/\s+/g,' ').trim();}
function matches(record,value){const hay=' '+phrase(value)+' ';return record.aliases.some(alias=>hay.includes(' '+alias+' '));}
function selectRecords(product,records){
  const selected=records.filter(record=>matches(record,product.title));
  // Secondary merchandising tags/handles may reveal an actual contradiction.
  // They never invent a motif when the title does not identify one.
  const hints=records.filter(record=>matches(record,product.handle)||matches(record,(product.tags||[]).join(' ')));
  if(selected.length&&hints.length&&!hints.some(record=>selected.includes(record)))return [];
  return selected.sort((a,b)=>Math.max(...b.aliases.filter(alias=>(' '+phrase(product.title)+' ').includes(' '+alias+' ')).map(alias=>alias.length))-Math.max(...a.aliases.filter(alias=>(' '+phrase(product.title)+' ').includes(' '+alias+' ')).map(alias=>alias.length))||a.id.localeCompare(b.id)).slice(0,2);
}
function held(product,issues){
  if(product.meaningHold===true||product.recommendationHold===true||product.cartHold===true)return true;
  return (Array.isArray(issues)?issues:[]).some(record=>record?.productId===product.id&&(record.meaningHold===true||record.recommendationHold===true||record.cartHold===true||(Array.isArray(record.issues)&&record.issues.some(issue=>issue&&issue.status!=='resolved'&&(issue.meaningHold===true||issue.recommendationHold===true||issue.cartHold===true||(Array.isArray(issue.blocks)&&issue.blocks.some(block=>['meaning','recommendation','cart'].includes(block)))||['identity','style','options','material','matching','history','content'].includes(issue.kind))))));
}
function usable(product,issues,at){return validProduct(product?.id)&&/^[a-z0-9_-]{1,180}$/.test(product.handle||'')&&shared.safeText(product.title,300)&&shared.sourceUrl(product.url,true)&&new URL(product.url).pathname.replace(/\/$/,'')==='/products/'+product.handle&&Number.isFinite(product.checkedAt)&&product.checkedAt>0&&product.checkedAt<=at+60000&&at-product.checkedAt<=shared.CURRENT_MS&&!held(product,issues);}
function publishedDetail(product,at){
  return shared.normalizeStory({schema:1,kind:'published-detail',provenance:'published_product',productId:product.id,handle:product.handle,productUrl:product.url,productTitle:product.title,productCheckedAt:product.checkedAt,checkedAt:at,libraryId:null,libraryVersion:null,motif:null,context:'Exact current published listing; no sourced symbolic history is being asserted.',facts:[{text:'The published listing names this piece “'+product.title+'”.',sourceIds:['published-product']}],interpretation:null,sources:[{id:'published-product',title:product.title,url:product.url,publisher:'Brites Jewelry',checkedAt:product.checkedAt,inspection:'published_check'}]},at);
}
function project(record,product,at){const motif=record.aliases.filter(alias=>(' '+phrase(product.title)+' ').includes(' '+alias+' ')).sort((a,b)=>b.length-a.length||a.localeCompare(b))[0];return shared.normalizeStory({schema:1,kind:'researched-story',provenance:'agent_researched',productId:product.id,handle:product.handle,productUrl:product.url,productTitle:product.title,productCheckedAt:product.checkedAt,checkedAt:at,libraryId:record.id,libraryVersion:record.version,motif,context:record.context,facts:record.facts,interpretation:record.interpretation,sources:record.sources},at);}
function createLibrary({db,namespace='Brites_Growth_Sandbox',now=Date.now}={}){
  const allowed=namespace==='Brites_Growth_Sandbox',collection=()=>db.collection(namespace+'_CharmStories');
  function sandbox(){if(!allowed)throw Error('Charm stories require the isolated sandbox namespace.');}
  async function bootstrap(){
    sandbox();
    // This file is an operator bootstrap payload, never a shopper read cache.
    const definitions=require('./_britesCharmStoryBootstrap.json'),at=now();
    if(definitions.schema!==1||definitions.provenance!=='agent_researched'||!Array.isArray(definitions.records)||definitions.records.length>96)throw Error('Invalid bounded charm story bootstrap.');
    const rows=definitions.records.map(record=>validateRecord(record,at));
    if(rows.some(record=>!record)||new Set(rows.map(record=>record.id)).size!==rows.length)throw Error('Charm story bootstrap sources or fields are invalid or stale.');
    return db.runTransaction(async tx=>{
      const refs=rows.map(record=>collection().doc(record.id)),saved=await Promise.all(refs.map(ref=>tx.get(ref)));let created=0;
      const completedAt=now();
      if(rows.some(record=>!validateRecord(record,completedAt)))throw Error('Charm story bootstrap source inspection is stale.');
      rows.forEach((record,index)=>{if(!saved[index].exists){tx.set(refs[index],{...record,version:hash(record),savedAt:completedAt});created++;}});
      return {ok:true,created,existing:rows.length-created,provenance:'agent_researched',collection:namespace+'_CharmStories',release:definitions.release};
    });
  }
  async function save(record,{expectedVersion}={}){
    sandbox();
    const checked=validateRecord(record,now());if(!checked)throw Error('Charm story fields, neutral inspected sources or timestamps are invalid or stale.');
    if(expectedVersion!==null&&!/^[a-f0-9]{64}$/.test(expectedVersion||''))throw Error('The current charm story version is required; use null only for a new record.');
    const ref=collection().doc(checked.id),version=hash(checked);
    return db.runTransaction(async tx=>{
      const prior=await tx.get(ref);
      if(expectedVersion===null&&prior.exists||expectedVersion!==null&&(!prior.exists||prior.data().version!==expectedVersion))throw Object.assign(Error('Charm story version changed; read the current record before saving.'),{code:'CHARM_STORY_VERSION_CHANGED'});
      // Revalidate after the transaction read so a delayed write cannot revive
      // a source that expired while this operation was waiting.
      if(!validateRecord(record,now()))throw Error('Charm story source inspection is stale.');
      tx.set(ref,{...checked,version,savedAt:now()});return {ok:true,id:checked.id,version,provenance:'agent_researched'};
    });
  }
  async function status(){sandbox();const snap=await collection().limit(96).get(),at=now();return {collection:namespace+'_CharmStories',provenance:'agent_researched',records:(snap.docs||[]).map(doc=>doc.data()).filter(value=>validateRecord(value,at,true)),checkedAt:at,bounded:true};}
  async function read(products,{issues=[]}={}){
    if(!allowed)return {stories:[],checkedAt:now(),libraryAvailable:false};
    const selected=(Array.isArray(products)?products:[]).slice(0,20),seenId=new Map(),seenHandle=new Map(),conflicts=new Set();
    for(const p of selected){if(seenId.has(p?.id)){conflicts.add(p?.id);conflicts.add(seenId.get(p?.id));}if(seenHandle.has(p?.handle)){conflicts.add(p?.id);conflicts.add(seenHandle.get(p?.handle));}seenId.set(p?.id,p?.id);seenHandle.set(p?.handle,p?.id);}
    let snapshot,available=true;try{snapshot=await collection().where('status','==','active').limit(96).get();}catch{available=false;}
    const at=now(),records=(snapshot?.docs||[]).map(doc=>doc.data()).filter(record=>record.status==='active'&&validateRecord(record,at,true)),stories=[];
    for(const p of selected){
      if(conflicts.has(p?.id)||!usable(p,issues,at))continue;
      const matches=selectRecords(p,records),rows=matches.map(record=>project(record,p,at)).filter(Boolean);
      if(rows.length)stories.push(...rows);else {const fallback=publishedDetail(p,at);if(fallback)stories.push(fallback);}
    }
    return {stories,checkedAt:at,libraryAvailable:available,bounded:true};
  }
  return {bootstrap,save,status,read,collection:namespace+'_CharmStories'};
}
module.exports={createLibrary,validateRecord,selectRecords,publishedDetail,shared};
