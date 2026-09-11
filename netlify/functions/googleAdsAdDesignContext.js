// Read-only context and complete, paginated Shopify image galleries for Ad Design.
// Identity comes from current Google resources, reviewed drafts, exact offer IDs
// or a Shopify lookup for an exact owned destination handle; never title similarity.
// Shopify queries validated 2026-09-10 against Admin GraphQL (product/MediaImage).
const str=(v,n=400)=>String(v==null?'':v).trim().slice(0,n);
const hash=value=>require('crypto').createHash('sha256').update(JSON.stringify(value)).digest('hex');
function productId(raw){const m=String(raw||'').match(/^(?:gid:\/\/shopify\/Product\/)?(\d+)$/);return m?m[1]:null;}
function offerParts(raw){const m=String(raw||'').match(/^shopify_([^_]+)_(\d+)_(\d+)$/i);return m?{itemId:String(raw),market:m[1],productId:m[2],variantId:m[3]}:null;}
function sourceUrl(raw,image=false){try{const u=new URL(String(raw||''));if(u.protocol!=='https:'||u.username||u.password)return null;const allowed=image?['britesjewelry.com','www.britesjewelry.com','cdn.shopify.com']:['britesjewelry.com','www.britesjewelry.com'];return allowed.includes(u.hostname)?u.toString():null;}catch(_){return null;}}
function currentCreativeUrl(raw){
  const owned=sourceUrl(raw,true);if(owned)return owned;
  try{const u=new URL(String(raw||''));if(u.protocol!=='https:'||u.username||u.password||u.port)return null;
    return ['googleusercontent.com','gstatic.com','googlesyndication.com'].some(host=>u.hostname===host||u.hostname.endsWith('.'+host))?u.toString():null;
  }catch(_){return null;}
}
// Provider images have asset/group identity. They are creative inspiration, never
// inferred product photographs: a rendered ad may depict several products.
function extractCurrentCreative(snapshot,context={}){
  const components=snapshot&&snapshot.components||{},selected=context.groups||[],images=[],groups=[],warnings=[],seen=new Set();
  const text=list=>(list||[]).map(row=>typeof row==='string'?row:row&&row.text).filter(value=>typeof value==='string'&&value.trim());
  function addImage(link,groupRef){
    if(link.status==='REMOVED')return;
    if(!link.imageUrl){warnings.push('Google has not provided a preview URL for one current image asset. Its saved asset identity is unchanged.');return;}
    const url=currentCreativeUrl(link.imageUrl);if(!url){warnings.push('A current Google image has an unsupported source URL and could not be opened.');return;}
    const resourceName=link.asset||null,linkResourceName=link.resourceName||null,fieldType=String(link.fieldType||'IMAGE'),id='current_'+hash([groupRef,linkResourceName||resourceName||url,fieldType]).slice(0,32);
    if(seen.has(id))return;seen.add(id);
    const width=Number(link.width||link.widthPixels),height=Number(link.height||link.heightPixels);
    images.push({id,resourceName,linkResourceName,url,fieldType,groupRef,status:link.status||'ENABLED',...(Number.isFinite(width)&&width>0?{width}:{}),...(Number.isFinite(height)&&height>0?{height}:{})});
  }
  for(const selection of selected){
    if(selection.channel==='pmax'){
      const group=(components.assetGroups||[]).find(row=>row.resourceName===selection.ref&&row.status!=='REMOVED');if(!group)continue;
      const links=(components.assetLinks||[]).filter(row=>row.assetGroup===group.resourceName&&row.status!=='REMOVED'),field=type=>text(links.filter(row=>row.fieldType===type));
      const copy={headlines:field('HEADLINE'),longHeadlines:field('LONG_HEADLINE'),descriptions:field('DESCRIPTION')},business=field('BUSINESS_NAME');if(business.length)copy.businessName=business[0];
      groups.push({groupRef:selection.ref,copy,finalUrls:group.finalUrls||[],status:group.status||'ENABLED'});
      links.filter(row=>/IMAGE|LOGO/.test(row.fieldType||'')).forEach(row=>addImage(row,selection.ref));
    }else if(selection.channel==='search'){
      const ad=(components.searchAds||[]).find(row=>(row.resourceName===selection.ref||row.adGroupAdResourceName===selection.ref)&&row.status!=='REMOVED');if(!ad)continue;
      const rsa=ad.responsiveSearchAd||{};groups.push({groupRef:selection.ref,copy:{headlines:text(rsa.headlines),longHeadlines:[],descriptions:text(rsa.descriptions),path1:rsa.path1||'',path2:rsa.path2||''},finalUrls:ad.finalUrls||[],status:ad.status||'ENABLED'});
      (components.searchImageLinks||[]).filter(row=>row.adGroup===ad.adGroup&&row.fieldType==='IMAGE').forEach(row=>addImage(row,selection.ref));
    }
  }
  return {source:'Current Google Ads snapshot',capturedAt:snapshot&&snapshot.capturedAt||null,images,groups,warnings:[...new Set(warnings)]};
}
function destination(raw){const url=sourceUrl(raw);if(!url)return null;const u=new URL(url),p=u.pathname.match(/\/(?:collections\/[^/]+\/)?products\/([a-zA-Z0-9_-]+)\/?$/),c=u.pathname.match(/^\/collections\/([a-zA-Z0-9_-]+)\/?$/);return p?{kind:'product',handle:p[1],url}:c?{kind:'collection',handle:c[1],url}:{kind:'page',url};}
const literal=JSON.stringify;
function setRelations(field){
  let value=field&&field.jsonValue;if(value===undefined&&field&&typeof field.value==='string'){try{value=JSON.parse(field.value);}catch(_){return [];}}
  if(!Array.isArray(value))return [];
  return [...new Map(value.filter(row=>row&&/^[a-zA-Z0-9_-]{1,255}$/.test(String(row.h||''))).map(row=>[row.h,{handle:String(row.h),format:str(row.f,120),label:str(row.t,300)}])).values()];
}
function createAdDesignContext(D){
  const now=()=>D.now?D.now():Date.now();
  async function query(code,deadline){const ms=Math.max(1,Math.min(9000,(deadline||now()+9000)-now()));if(ms<100)throw new Error('The gallery reached its request time allowance. Load the remaining photos to continue.');let timer;try{return await Promise.race([D.shopifyGql(code),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('Shopify image lookup timed out.')),ms);})]);}finally{clearTimeout(timer);}}
  async function loadProductImages({productId:id,after=null,all=false,deadline=null}={}){
    id=productId(id);if(!id)throw new Error('An exact Shopify product ID is required.');
    const finish=deadline||now()+20000,images=[],seen=new Set(),cursors=new Set();let cursor=after,product=null,hasMore=false,nextCursor=null,pages=0,warnings=[];
    do {
      if(cursor&&cursors.has(cursor))throw new Error('Shopify repeated an image cursor; remaining gallery pages need a retry.');if(cursor)cursors.add(cursor);
      let data;try{data=await query(`query AdDesignProductImages { product(id: ${literal('gid://shopify/Product/'+id)}) { id title handle description onlineStoreUrl completeTheSet: metafield(namespace: "brites", key: "set") { jsonValue } media(first: 100${cursor?', after: '+literal(cursor):''}, query: "media_type:IMAGE", sortKey: POSITION) { nodes { id alt ... on MediaImage { image { url width height } } } pageInfo { hasNextPage endCursor } } } }`,finish);}catch(error){if(!product)throw error;hasMore=true;nextCursor=cursor;warnings.push(str(error.message,250));break;}
      const p=data&&data.product;if(!p||productId(p.id)!==id)throw new Error('This exact Shopify product was not found in the connected store.');
      if(!product){const relations=setRelations(p.completeTheSet).filter(row=>row.handle!==p.handle);product={id:p.id,title:str(p.title,250),handle:str(p.handle,160),description:str(p.description,12000),url:sourceUrl(p.onlineStoreUrl)||(/^[a-zA-Z0-9_-]+$/.test(p.handle||'')?'https://britesjewelry.com/products/'+encodeURIComponent(p.handle):null),relatedHandles:relations.map(row=>row.handle),setRelations:relations,relationSource:'Shopify product metafield brites.set'};}
      if(!product.url)throw new Error('This product has no verified Brites destination.');
      for(const node of p.media&&p.media.nodes||[]){const image=node.image||{},url=sourceUrl(image.url,true);if(!url){warnings.push('One product-media item did not expose a supported ready image URL.');continue;}const key=String(node.id||url);if(seen.has(key))continue;seen.add(key);images.push({id:key,url,alt:str(node.alt,400),width:Number(image.width)||null,height:Number(image.height)||null,productId:p.id,source:'Shopify product media'});}
      const info=p.media&&p.media.pageInfo||{};hasMore=!!info.hasNextPage;nextCursor=hasMore?info.endCursor||null:null;pages++;
      if(hasMore&&!nextCursor)throw new Error('Shopify returned more product photos without a pagination cursor.');
      if(!all||!hasMore)break;
      cursor=nextCursor;
      if(now()+1500>=finish){warnings.push('More photos are available; continue this product’s gallery.');break;}
    }while(hasMore);
    return {...product,images,complete:!hasMore,hasMore,nextCursor,imagesReturned:images.length,pages,warnings,checkedAt:now(),source:'Shopify product media, including variant-associated images'};
  }
  function bindings(id,context){
    const groups=context.groups||[],offers=(context.itemIds||[]).map(offerParts).filter(row=>row&&row.productId===id),eligibleGroupRefs=groups.filter(g=>(g.productIds||[]).map(productId).includes(id)).map(g=>g.ref);
    return {eligibleGroupRefs,creativeGroupRefs:eligibleGroupRefs.slice(),adProduct:eligibleGroupRefs.length>0,itemId:offers[0]&&offers[0].itemId||null,offerIds:offers.map(row=>row.itemId),variantId:offers[0]&&offers[0].variantId||null};
  }
  function relatedSources(products){return products.filter(p=>p.adProduct!==false&&(p.relatedHandles||[]).length).map(p=>({key:'related_'+productId(p.id),type:'relatedSet',productId:productId(p.id),creativeGroupRefs:p.eligibleGroupRefs||[],nextCursor:null,hasMore:true,label:'Complete the set · '+p.title}));}
  function upgradeGalleryContext(context,products=[]){
    if(Number(context.gallerySchema)>=2&&Array.isArray(context.gallerySources))return context;
    const groups=context.groups||[],known=[...new Set([...groups.flatMap(g=>g.productIds||[]),...(context.itemIds||[]).map(offerParts).filter(Boolean).map(o=>o.productId)].map(productId).filter(Boolean))],byId=new Map(products.map(p=>[productId(p.id),p])),gallerySources=[];
    const pending=known.filter(id=>!byId.has(id)||!Array.isArray(byId.get(id).relatedHandles));
    if(pending.length)gallerySources.push({key:'selected_products',type:'productIds',productIds:pending,offset:0,hasMore:true,label:'Ad listings and website relationships'});
    const pendingHandles=[...new Set(groups.filter(g=>!(g.productIds||[]).length).map(g=>destination(g.url)).filter(d=>d&&d.kind==='product').map(d=>d.handle))];
    if(pendingHandles.length)gallerySources.push({key:'destination_products',type:'productIds',productHandles:pendingHandles,offset:0,hasMore:true,label:'Ad destination listings'});
    const seen=new Set();for(const group of groups){const d=destination(group.url);if(!d||d.kind!=='collection'||(group.itemIds||[]).length||seen.has(d.handle))continue;seen.add(d.handle);const prior=(context.galleryBrowse||[]).find(row=>row.handle===d.handle);gallerySources.push({key:'collection_'+hash(d.url).slice(0,20),type:'collection',handle:d.handle,url:d.url,hasMore:prior?!!prior.hasMore:true,nextCursor:prior&&prior.nextCursor||null});}
    gallerySources.push(...relatedSources(products));const merged=[...new Map([...gallerySources,...(context.gallerySources||[])].map(source=>[source.key,source])).values()];return {...context,gallerySchema:2,gallerySources:merged,partial:!!context.partial||merged.some(s=>s.hasMore)};
  }
  async function hydrate(ids,context,deadline,extra={}){
    const products=[];let index=0;
    async function worker(){while(index<ids.length){const id=ids[index++],scope=bindings(id,context);try{const p=await loadProductImages({productId:id,all:false,deadline});products.push({...p,...scope,...(extra[id]||{})});}catch(error){products.push({id:'gid://shopify/Product/'+id,title:'Product '+id,images:[],complete:false,hasMore:true,nextCursor:null,error:str(error.message,250),...scope,...(extra[id]||{})});}}}
    await Promise.all([worker(),worker(),worker()]);return products.sort((a,b)=>ids.indexOf(productId(a.id))-ids.indexOf(productId(b.id)));
  }
  async function loadRelatedProducts({productId:raw,after=null,context={},deadline=null}={}){
    const id=productId(raw);if(!id)throw new Error('An exact source product ID is required for Complete the set.');
    const parentScope=bindings(id,context);if(!parentScope.eligibleGroupRefs.length)throw new Error('Complete the set must begin from a product verified for this ad.');
    const finish=deadline||now()+22000,data=await query(`query AdDesignCompleteTheSet { product(id: ${literal('gid://shopify/Product/'+id)}) { id handle completeTheSet: metafield(namespace: "brites", key: "set") { jsonValue } } }`,finish),parent=data&&data.product;
    if(!parent||productId(parent.id)!==id)throw new Error('The source product for Complete the set was not found.');
    const relations=setRelations(parent.completeTheSet).filter(row=>row.handle!==parent.handle),digest=hash(relations);let offset=0;
    if(after){let cursor;try{cursor=JSON.parse(Buffer.from(String(after),'base64url').toString('utf8'));}catch(_){throw new Error('The related-product cursor is invalid.');}if(cursor.hash!==digest)throw new Error('Complete the set changed on the website. Refresh this product’s related listings.');if(!Number.isSafeInteger(cursor.offset)||cursor.offset<0||cursor.offset>relations.length)throw new Error('The related-product cursor is invalid.');offset=cursor.offset;}
    const page=relations.slice(offset,offset+12),warnings=[],rows=page.length?await query(`query AdDesignRelatedProducts { ${page.map((relation,i)=>`p${i}: productByHandle(handle: ${literal(relation.handle)}) { id title handle }`).join(' ')} }`,finish):{},ids=[],extra={};
    page.forEach((relation,i)=>{const p=rows&&rows['p'+i],relatedId=p&&productId(p.id);if(!relatedId||p.handle!==relation.handle){warnings.push('Related listing '+relation.handle+' is no longer available.');return;}if(!ids.includes(relatedId))ids.push(relatedId);const scope=bindings(relatedId,context);extra[relatedId]={...scope,creativeGroupRefs:[...new Set([...scope.eligibleGroupRefs,...parentScope.eligibleGroupRefs])],relatedTo:['gid://shopify/Product/'+id],relationSource:'Shopify product metafield brites.set',relationHandle:relation.handle,relationFormat:relation.format,identitySource:'Exact Complete the set website relation',adProduct:scope.eligibleGroupRefs.length>0};});
    const products=await hydrate(ids,context,finish,extra),nextOffset=offset+page.length,hasMore=nextOffset<relations.length;
    return {products,warnings,hasMore,nextCursor:hasMore?Buffer.from(JSON.stringify({hash:digest,offset:nextOffset})).toString('base64url'):null,total:relations.length,sourceProductId:'gid://shopify/Product/'+id,relationSource:'Shopify product metafield brites.set'};
  }
  async function loadGalleryPage({source,context={},deadline=null}={}){
    if(!source||!source.key||!['collection','productIds','relatedSet'].includes(source.type))throw new Error('Choose a saved gallery continuation.');
    const finish=deadline||now()+22000,warnings=[];let products=[],updated={...source},nextContext={...context,groups:(context.groups||[]).map(g=>({...g,productIds:[...(g.productIds||[])]}))};
    if(source.type==='relatedSet'){const page=await loadRelatedProducts({productId:source.productId,after:source.nextCursor,context,deadline:finish});return {...page,source:{...source,nextCursor:page.nextCursor,hasMore:page.hasMore,total:page.total},additionalSources:[]};}
    if(source.type==='collection'){
      const handle=str(source.handle,255),groups=nextContext.groups.filter(g=>{const d=destination(g.url);return d&&d.kind==='collection'&&d.handle===handle&&!(g.itemIds||[]).length;});
      if(!/^[a-zA-Z0-9_-]+$/.test(handle)||!groups.length)throw new Error('This collection is outside the saved ad destinations.');
      const data=await query(`query AdDesignCollectionProducts { collectionByHandle(handle: ${literal(handle)}) { id title products(first: 12${source.nextCursor?', after: '+literal(source.nextCursor):''}, sortKey: BEST_SELLING) { nodes { id title handle } pageInfo { hasNextPage endCursor } } } }`,finish),conn=data&&data.collectionByHandle&&data.collectionByHandle.products;
      if(!conn)throw new Error('The destination collection was not found in Shopify.');const ids=[...new Set((conn.nodes||[]).map(p=>productId(p.id)).filter(Boolean))],info=conn.pageInfo||{};
      if(info.hasNextPage&&(!info.endCursor||info.endCursor===source.nextCursor))throw new Error('Shopify did not advance this collection’s product cursor.');
      groups.forEach(g=>{g.productIds=[...new Set([...g.productIds,...ids])];});products=await hydrate(ids,nextContext,finish);updated={...source,nextCursor:info.hasNextPage?info.endCursor:null,hasMore:!!info.hasNextPage};
    }else if(Array.isArray(source.productHandles)){
      const allowed=new Set(nextContext.groups.map(g=>destination(g.url)).filter(d=>d&&d.kind==='product').map(d=>d.handle)),handles=source.productHandles;
      if(handles.some(handle=>!allowed.has(handle)))throw new Error('A gallery product handle is outside the saved ad destinations.');const offset=Number(source.offset)||0;if(!Number.isSafeInteger(offset)||offset<0||offset>handles.length)throw new Error('The product gallery offset is invalid.');
      const page=handles.slice(offset,offset+12),data=page.length?await query(`query AdDesignDestinationProducts { ${page.map((handle,i)=>`p${i}: productByHandle(handle: ${literal(handle)}) { id title handle }`).join(' ')} }`,finish):{},ids=[];
      page.forEach((handle,i)=>{const p=data&&data['p'+i],id=p&&productId(p.id);if(!id||p.handle!==handle){warnings.push('Destination listing '+handle+' is no longer available.');return;}ids.push(id);for(const g of nextContext.groups){const d=destination(g.url);if(d&&d.kind==='product'&&d.handle===handle&&!(g.itemIds||[]).length)g.productIds=[...new Set([...g.productIds,id])];}});
      products=await hydrate([...new Set(ids)],nextContext,finish);updated={...source,offset:offset+page.length,hasMore:offset+page.length<handles.length};
    }else{
      const known=new Set(nextContext.groups.flatMap(g=>g.productIds||[]).map(productId).filter(Boolean)),ids=(source.productIds||[]).map(productId);
      if(ids.some(id=>!id||!known.has(id)))throw new Error('A gallery product is outside the saved ad scope.');const offset=Number(source.offset)||0;if(!Number.isSafeInteger(offset)||offset<0||offset>ids.length)throw new Error('The product gallery offset is invalid.');
      const page=ids.slice(offset,offset+12);products=await hydrate(page,nextContext,finish);updated={...source,offset:offset+page.length,hasMore:offset+page.length<ids.length};
    }
    return {products,source:updated,additionalSources:relatedSources(products),warnings,hasMore:updated.hasMore};
  }
  async function loadContext(input={}){
    const started=now(),deadline=started+22000,warnings=[],itemIds=new Set(),ids=new Set(),productSources=new Map();let groups=[],campaignId=null,approvalId=null,sourceVersion=null,snapshotHash=null,snapshot=null,handle=null,approval=null,approvalPayloadHash=null,generationAllowed=false;const destinationProducts=new Map(),approvedSourceIds=new Set();
    let range=input.range||null;if(!range&&D.reportContext&&D.validatedRange){const r=await D.reportContext();range={...D.validatedRange({start:input.start,end:input.end},r.accountToday,30),timeZone:r.accountTimezone,currency:r.budgetCurrency};}
    const addOffer=x=>{const o=offerParts(x);if(o){itemIds.add(o.itemId);ids.add(o.productId);productSources.set(o.productId,'Exact Merchant offer ID');}else if(x)warnings.push('An unrecognized offer ID could not be matched to a Shopify product.');};
    const cleanGroups=rows=>(rows||[]).map((g,i)=>({key:str(g.key||'g'+i,100),ref:str(g.ref,250),name:str(g.name||'Ad '+(i+1),200),channel:g.channel,url:sourceUrl(g.url),keywords:(g.keywords||[]).map(k=>str(typeof k==='string'?k:k.text,100)).filter(Boolean),original:g.original||{},adGroupRef:g.adGroupRef||null,itemIds:g.itemIds||[],productIds:(g.productIds||[]).map(productId).filter(Boolean)})).filter(g=>['pmax','search'].includes(g.channel)&&g.ref&&g.url);
    if(input.approvalId){approvalId=String(input.approvalId);if(!/^[A-Za-z0-9_-]{1,120}$/.test(approvalId))throw new Error('Invalid approval reference.');if(!D.fb())throw new Error('Approval storage is unavailable.');const doc=await D.fb().db.collection(D.COL.approvals).doc(approvalId).get();if(!doc.exists)throw new Error('The selected approval was not found.');approval=doc.data();if(!['PENDING','APPROVED'].includes(approval.status))throw new Error('Only an unpublished approval can be opened for design.');const p=approval.payload||{},meta=p.meta||{};approvalPayloadHash=D.creativeHash?D.creativeHash(p):null;for(const source of meta.sourceProducts||[]){const id=productId(source.id||source.productId);if(id)approvedSourceIds.add(id);}campaignId=str(meta.existingCampaignId||p.versionGuard&&p.versionGuard.campaignId||input.campaignId);handle=str(meta.handle||p.finalCollection||input.handle,160)||null;
      for(const id of [...(p.itemIds||[]),...(meta.itemIds||[]),...(meta.assetGroups||[]).flatMap(g=>g.itemIds||[])])addOffer(id);
      try{groups=cleanGroups(D.creativeGroups(approval));}catch(e){if(!campaignId)throw e;}
      generationAllowed=true;
    } else if(input.campaignId){campaignId=String(input.campaignId);}
    if(campaignId){if(!/^\d+$/.test(campaignId))throw new Error('Invalid campaign reference.');if(input.campaignId&&String(input.campaignId)!==campaignId)throw new Error('The selected approval belongs to another campaign.');const guard=approval&&approval.payload&&approval.payload.versionGuard||{};const verified=await D.verifiedBasis({campaignId,expectedVersion:guard.expectedVersion||input.expectedVersion,snapshotHash:guard.snapshotHash||input.snapshotHash});snapshot=verified.snapshot;sourceVersion=verified.version;snapshotHash=verified.snapshotHash;const c=snapshot.components||{};
      if(!groups.length){if(snapshot.channel==='SEARCH')groups=cleanGroups((c.searchAds||[]).map(a=>({key:'ad-'+String(a.resourceName).split('/').pop(),ref:a.resourceName,adGroupRef:a.adGroup,name:(a.adGroupName||'Search group '+String(a.adGroup).split('/').pop())+' · Ad '+String(a.resourceName).split('/').pop(),channel:'search',url:(a.finalUrls||[])[0],keywords:(c.keywords||[]).filter(k=>k.adGroup===a.adGroup&&!k.negative&&k.status==='ENABLED').map(k=>(k.keyword||{}).text),original:a.responsiveSearchAd||{}})));
        else if(snapshot.channel==='PERFORMANCE_MAX')groups=cleanGroups((c.assetGroups||[]).map(g=>{const links=(c.assetLinks||[]).filter(a=>a.assetGroup===g.resourceName),field=t=>links.filter(a=>a.fieldType===t&&a.text).map(a=>a.text);return {key:'asset-group-'+String(g.resourceName).split('/').pop(),ref:g.resourceName,name:g.name,channel:'pmax',url:(g.finalUrls||[])[0],keywords:[],original:{headlines:field('HEADLINE'),longHeadlines:field('LONG_HEADLINE'),descriptions:field('DESCRIPTION')},itemIds:(c.listingGroups||[]).filter(l=>l.assetGroup===g.resourceName&&l.type==='UNIT_INCLUDED').map(l=>((l.caseValue||{}).productItemId||{}).value).filter(Boolean)};}));}
      for(const g of groups){(g.itemIds||[]).forEach(addOffer);if(!g.itemIds.length){const matching=(c.listingGroups||[]).filter(l=>l.assetGroup===g.ref&&l.type==='UNIT_INCLUDED').map(l=>((l.caseValue||{}).productItemId||{}).value).filter(Boolean);g.itemIds=matching;matching.forEach(addOffer);}g.productIds=[...new Set([...(g.productIds||[]),...(g.itemIds||[]).map(offerParts).filter(Boolean).map(x=>x.productId)])];}generationAllowed=true;
    }
    if(!campaignId&&!approvalId){handle=str(input.handle,160)||null;if(handle&&!/^[a-zA-Z0-9_-]+$/.test(handle))throw new Error('Invalid collection handle.');(input.itemIds||[]).forEach(addOffer);if(itemIds.size){const url=handle?'https://britesjewelry.com/collections/'+encodeURIComponent(handle):sourceUrl(input.url);if(!url)throw new Error('Select the intended Brites destination for this product opportunity.');groups=cleanGroups([{key:'opportunity',ref:'opportunity:'+handle,name:str(input.name||handle||'Product opportunity',200),channel:'pmax',url,keywords:input.searchThemes||[],original:{},itemIds:[...itemIds]}]);generationAllowed=true;}
      else if(handle)warnings.push('Browse this collection’s photos; choose an exact product offer before generating a new product ad.');
    }
    if(input.groupRef){const group=groups.find(g=>g.ref===String(input.groupRef));if(!group)throw new Error('This ad group is not part of the selected campaign or approval.');groups=[group];ids.clear();productSources.clear();itemIds.clear();(group.itemIds||[]).forEach(addOffer);}
    // Resolve exact destination handles, never search titles or infer product IDs.
    const destinations=[...new Map(groups.map(g=>destination(g.url)).filter(Boolean).map(d=>[d.url,d])).values()];
    if(handle&&!destinations.some(d=>d.kind==='collection'&&d.handle===handle))destinations.push({kind:'collection',handle,url:'https://britesjewelry.com/collections/'+handle});
    const gallerySources=destinations.filter(d=>d.kind==='collection'&&(!groups.length||groups.some(g=>g.url===d.url&&!g.itemIds.length))).map(d=>({key:'collection_'+hash(d.url).slice(0,20),type:'collection',handle:d.handle,url:d.url,hasMore:true,nextCursor:null})),galleryBrowse=gallerySources.slice(),resolvedHandles=new Set();
    for(const d of destinations){if(now()+8000>=deadline){warnings.push('Additional destination products are available through gallery continuation.');break;}try{
      if(d.kind==='product'){const data=await query(`query AdDesignProductHandle { productByHandle(handle: ${literal(d.handle)}) { id title handle } }`,deadline),p=data&&data.productByHandle,id=p&&productId(p.id);if(id){ids.add(id);productSources.set(id,'Exact ad destination product');destinationProducts.set(d.url,new Set([id]));resolvedHandles.add(d.handle);}else warnings.push('The destination product was not found in Shopify.');}
      else if(d.kind==='collection'&&(!groups.length||groups.some(g=>g.url===d.url&&!g.itemIds.length))){const data=await query(`query AdDesignCollectionProducts { collectionByHandle(handle: ${literal(d.handle)}) { id title products(first: 12, sortKey: BEST_SELLING) { nodes { id title handle } pageInfo { hasNextPage endCursor } } } }`,deadline),conn=data&&data.collectionByHandle&&data.collectionByHandle.products;if(conn){for(const p of conn.nodes||[]){const id=productId(p.id);if(id){ids.add(id);productSources.set(id,'Product in exact destination collection');}}destinationProducts.set(d.url,new Set((conn.nodes||[]).map(p=>productId(p.id)).filter(Boolean)));const continuation={hasMore:!!(conn.pageInfo||{}).hasNextPage,nextCursor:(conn.pageInfo||{}).endCursor||null};if(continuation.hasMore&&!continuation.nextCursor)throw new Error('Shopify returned more collection products without a cursor.');Object.assign(gallerySources.find(source=>source.url===d.url),continuation);}else warnings.push('The destination collection was not found in Shopify.');}
    }catch(e){warnings.push(str(e.message,250));}}
    const serviceGroups=groups.filter(g=>{const d=destination(g.url);return d&&d.kind==='page'&&!g.itemIds.length;});
    for(const id of approvedSourceIds){if(serviceGroups.length){ids.add(id);productSources.set(id,'Verified product source recorded in the existing approval');serviceGroups.forEach(g=>g.productIds.push(id));}}
    for(const raw of (input.productIds||[])){const id=productId(raw);if(!id)throw new Error('Product selections must use exact Shopify IDs.');
      const known=ids.has(id);if(!known&&!serviceGroups.length)throw new Error('The selected product is outside this ad’s verified offer or destination scope. Choose a product from the matching gallery.');
      if(!known&&serviceGroups.length){ids.add(id);productSources.set(id,'Explicit product selected for the existing service destination; Shopify identity verified below');serviceGroups.forEach(g=>g.productIds.push(id));}
    }
    if(!groups.length&&handle)groups=[{key:'browse',ref:'browse:'+handle,name:handle,channel:'pmax',url:'https://britesjewelry.com/collections/'+handle,keywords:[],original:{},itemIds:[]}];
    for(const g of groups){const byDestination=destinationProducts.get(g.url)||new Set();g.productIds=g.itemIds.length?[...new Set(g.itemIds.map(offerParts).filter(Boolean).map(x=>x.productId))]:[...new Set([...(g.productIds||[]),...byDestination])];}
    for(const g of groups){const d=destination(g.url);g.requiresProductSplit=!!(campaignId&&!approvalId&&d&&d.kind!=='page'&&(g.channel==='pmax'?g.productIds.length>1:new Set((snapshot?.components?.searchAds||[]).filter(a=>a.adGroup===g.adGroupRef).flatMap(a=>a.finalUrls||[]).map(u=>destination(u)).filter(d=>d?.kind==='product').map(d=>d.handle)).size>1));}
    if(D.gaql)await Promise.all(groups.filter(g=>g.channel==='pmax'&&/^customers\/\d+\/assetGroups\/\d+$/.test(g.ref)).map(async g=>{try{const rows=await D.gaql(`SELECT asset_group_signal.search_theme.text FROM asset_group_signal WHERE asset_group.resource_name = '${g.ref}'`);g.keywords=rows.map(r=>r.assetGroupSignal?.searchTheme?.text).filter(Boolean);}catch(e){warnings.push('Search themes for '+g.name+' are unavailable.');}}));
    const requestedId=input.productId?productId(input.productId):null,preferredId=productId(input.selectedProductId),selectedId=requestedId||(preferredId&&ids.has(preferredId)?preferredId:null);
    if(input.productId&&(!requestedId||!ids.has(requestedId)))throw new Error('The selected product is outside this ad’s verified offer or destination scope. Choose a product from the matching gallery.');
    // Open the explicitly selected verified listing even when it is beyond the
    // first hydration page. Keep pagination in this same order to avoid skips.
    const products=[],allIds=selectedId?[selectedId,...[...ids].filter(id=>id!==selectedId)]:[...ids],pending=allIds.slice(0,12);let index=0;
    if(allIds.length>pending.length)gallerySources.unshift({key:'selected_products',type:'productIds',productIds:allIds,offset:pending.length,hasMore:true,label:'Ad listings'});
    const remainingHandles=[...new Set(destinations.filter(d=>d.kind==='product'&&!resolvedHandles.has(d.handle)).map(d=>d.handle))];
    if(remainingHandles.length)gallerySources.push({key:'destination_products',type:'productIds',productHandles:remainingHandles,offset:0,hasMore:true,label:'Ad destination listings'});
    async function worker(){while(index<pending.length){const id=pending[index++],scope=bindings(id,{groups,itemIds:[...itemIds]});try{const p=await loadProductImages({productId:id,all:true,deadline});products.push({...p,identitySource:productSources.get(id),...scope});}catch(e){products.push({id:'gid://shopify/Product/'+id,title:'Product '+id,images:[],complete:false,hasMore:true,nextCursor:null,error:str(e.message,250),identitySource:productSources.get(id),...scope});warnings.push('Product '+id+': '+str(e.message,200));}}}
    await Promise.all([worker(),worker(),worker()]);
    products.sort((a,b)=>pending.indexOf(productId(a.id))-pending.indexOf(productId(b.id)));
    gallerySources.push(...relatedSources(products));
    if(!groups.length)throw new Error('No supported existing ad or reviewed opportunity context was found.');
    const context={campaignId:campaignId||null,approvalId,draftGroups:!!(approval&&approval.payload&&approval.payload.groupSplitGuard),approvalPayloadHash,handle,budget:input.budget==null?null:Number(input.budget),dailyBudget:input.dailyBudget==null?null:Number(input.dailyBudget),feedLabel:input.feedLabel||null,searchThemes:Array.isArray(input.searchThemes)?input.searchThemes.slice(0,25):[],days:input.days==null?null:Number(input.days),countries:Array.isArray(input.countries)?input.countries.slice(0,20):[],startDate:input.startDate||null,endDate:input.endDate||null,itemIds:[...itemIds],groups,range,generationAllowed,requiresOfferSelection:!generationAllowed,source:'Current provider state and Shopify product media',warnings,galleryBrowse,gallerySources,gallerySchema:2,partial:products.some(p=>!p.complete)||gallerySources.some(s=>s.hasMore),checkedAt:now()};
    context.currentCreative=extractCurrentCreative(snapshot,context);
    return {context,products,sourceVersion,snapshotHash,snapshot,approvalStatus:approval&&approval.status||null};
  }
  return {loadContext,loadProductImages,loadGalleryPage,loadRelatedProducts,relatedSources,upgradeGalleryContext};
}
module.exports={createAdDesignContext,productId,offerParts,sourceUrl,destination,setRelations,currentCreativeUrl,extractCurrentCreative};
