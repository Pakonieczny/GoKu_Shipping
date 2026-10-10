(async function(){
  'use strict';
  const main=document.querySelector('#shop-content');
  if(!main)return;
  const full=document.body?.dataset.storefront==='expanded',home=main.cloneNode(true);
  const HANDLE=/^[a-z0-9]+(?:-[a-z0-9]+)*$/,PRODUCT=/^gid:\/\/shopify\/Product\/[1-9][0-9]{0,19}$/,VARIANT=/^gid:\/\/shopify\/ProductVariant\/[1-9][0-9]{0,19}$/;
  const SORTS=new Set(['featured','price-asc','price-desc','title-asc','title-desc']);
  const SEED_CATEGORIES=['regular-necklaces','beady-necklaces','stud-earrings','hoop-earrings','charm-only'];
  const FILTERS=new Set(['all','necklaces','earrings','bracelets','rings','charms','available',...SEED_CATEGORIES]);
  const CATEGORY_NAMES={necklaces:/\b(?:necklaces?|pendants?)\b/i,earrings:/\b(?:earrings?|studs?|huggies?|hoops?)\b/i,bracelets:/\bbracelets?\b/i,rings:/\brings?\b/i,charms:/\bcharms?\b/i};
  const SEARCH_CONTEXT=new Set(searchWords('a an the for and or my me you your our of on in to with from at by is are be that this these those please show find search look looking want would like can could will give get pull up all any piece pieces jewelry jewellery gift gifts option options pair pairs set sets under below over above within around about budget price prices cost costs cheap cheapest cheaper affordable expensive less more than between maximum minimum max min dollars dollar usd cad eur gbp silver sterling gold filled plated rose solid white yellow metal metals 14k 18k 24k themed'));
  const SECTIONS=new Set(['price','details','options','story','shipping','gifts','customize','catalogue','image','bag','checkout','offers','title','description','materials','length','engraving']);
  const PAGE_SIZE=24,MAX_PIECES=1200;
  const state={pageKind:'catalogue',currentHandle:'',focusedHandle:'',search:'',checkedSearch:'',collectionSource:'browse',sort:'featured',filter:'all',contextRevision:0,discoveryRevision:0,activeSection:'catalogue',loading:false,products:[],browse:[],browsePageInfo:{hasNextPage:false,endCursor:null},browseVerifiedAt:0,seedInfo:null,seedFailure:false,seedBrowseStarted:false,limit:PAGE_SIZE,pageInfo:{hasNextPage:false,endCursor:null},current:null,services:null,servicesPending:null,selectedImage:0,verifiedAt:0};
  let navigationVersion=0,request=null,noticeTimer=0,highlightTimer=0,focusTimer=0,storyVersion=0,collectionControls=null,productUI=null,checkoutUI=null,cartLines=null,lineSequence=0;
  let guideBusy=false,guideCommandKey='',publishing=false,lastChange=null,restoringChange=false,changeSequence=0,historyTraversal=null,bagEditVersion=0;
  const engravingDrafts=new Map(),addRequests=new Map(),bagMenus=new Map(),bagChecks=new Map(),buttonCalls=new WeakMap();
  try{const saved=JSON.parse(sessionStorage.getItem('brites-sandbox-bag-menus')||'[]');if(Array.isArray(saved))saved.slice(0,50).forEach(row=>{if(Array.isArray(row)&&/^test-[a-z0-9-]{1,100}$/.test(row[0]||'')&&typeof row[1]==='string'&&row[1].length<=100)bagMenus.set(row[0],row[1]);});}catch{}
  const CONTROL_CAPABILITIES=Object.freeze({controlVersion:1,mode:'sandbox',actions:Object.freeze(['home','search','sort','filter','open','highlight','scroll','zoom','gallery','back','forward','bag','checkout','gift','customize','options','close-options','close-image','undo','select-option','product-quantity','set-engraving','add','review-add','bag-quantity','bag-remove','bag-clear','bag-options','bag-highlight','bag-note','customize-brief','catalogue-more','story-open','cancel-review','bag-select-option','bag-set-engraving','gift-preferences','checkout-step','checkout-option','checkout-complete']),quantity:Object.freeze({minimum:1,maximum:20}),reviewBeforeAdd:false,finalOrder:false,addMode:'explicit-request',checkoutMode:'simulation-only',notesMode:'local-session'});
  const known=new Map(),identities=new Map(),checkedInventory=new Map(),inventoryRows=new Map(),productViews=new Map(),historyViews=new Map();
  const navigationKeys=[];let navigationIndex=0,historySequence=0,restoringHistory=false,collectionView=null,inventoryPending=null,inventoryVersion=0,inventoryRefreshTimer=0,inventoryBrowse=false;
  let inventoryFingerprint=null,inventoryCoverageComplete=false,inventorySourcePartial=false,inventoryPageFailure=false,inventoryRetryCount=0;
  const inventoryState={total:0,loaded:0,ready:false,partial:false,loading:false};
  const notice=document.querySelector('#storefront-status')||document.body.appendChild(node('aside','','storefront-notice'));
  notice.setAttribute('role','status');notice.setAttribute('aria-live','polite');
  function node(tag,value,className){const e=document.createElement(tag);if(value!=null)e.textContent=value;if(className)e.className=className;return e;}
  function button(label,className,run){const b=node('button',label,className);b.type='button';if(run)b.addEventListener('click',run);return b;}
  function actionButton(label,className,run){const b=button(label,className);b.addEventListener('click',()=>{const options=buttonCalls.get(b)?.options||{};buttonCalls.set(b,{options,pending:Promise.resolve(run(options))});});return b;}
  async function clickActionButton(b,options){if(!b?.isConnected||b.disabled||b.hidden||document.hidden)return {ok:false,message:'That current visible control is not available.'};buttonCalls.set(b,{options});b.click();const pending=buttonCalls.get(b)?.pending;buttonCalls.delete(b);return pending?await pending:{ok:false,message:'The visible control did not respond.'};}
  function clean(value,max){return typeof value==='string'?value.replace(/\u0000/g,'').trim().slice(0,max):'';}
  function validHandle(handle){return typeof handle==='string'&&handle.length<=180&&HANDLE.test(handle);}
  function money(value,currency){try{return new Intl.NumberFormat('en',{style:'currency',currency}).format(value)+' '+currency;}catch{return value+' '+currency;}}
  function safeImage(value){try{const u=new URL(value);return u.protocol==='https:'&&!u.username&&!u.password&&!u.port&&['cdn.shopify.com','britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)?u.href:null;}catch{return null;}}
  function picture(p,className){const src=safeImage(p.image);if(!src)return null;const img=document.createElement('img');img.src=src;img.alt=clean(p.imageAlt||p.altText||p.title,300);img.loading='lazy';img.decoding='async';if(className)img.className=className;return img;}
  function validProduct(p,handle){
    if(!p||p.handle!==handle||!validHandle(handle)||!PRODUCT.test(p.id||'')||typeof p.title!=='string'||!p.title||typeof p.description!=='string'||!/^[A-Z]{3}$/.test(p.currency||'')||!Array.isArray(p.variants))return false;
    try{const url=new URL(p.url);if(url.protocol!=='https:'||!['britesjewelry.com','www.britesjewelry.com'].includes(url.hostname)||url.username||url.password||url.port||url.search||url.hash||(url.pathname!=='/products/'+handle&&url.pathname!=='/products/'+handle+'/'))return false;}catch{return false;}
    return p.variants.length<=500&&p.variants.every(v=>v&&VARIANT.test(v.id||'')&&typeof v.title==='string'&&Number.isFinite(v.price)&&v.price>=0&&typeof v.available==='boolean');
  }
  function remember(p){
    const checked=checkedInventory.get(p.handle);if(checked&&checked.expiresAt>Date.now()&&checked.product.id===p.id&&p.detailState!=='checked')p=checked.product;
    known.set(p.handle,p);identities.set(p.id,p.handle);
    try{const saved=[...identities],bagIds=new Set(readCart().map(item=>item.productId)),bag=saved.filter(([id])=>bagIds.has(id));sessionStorage.setItem('brites-sandbox-product-identities',JSON.stringify(bag.concat(saved.filter(([id])=>!bagIds.has(id)).slice(-200)).slice(-250)));}catch{}
    return p;
  }
  function checkedTime(value){const time=typeof value==='number'?value:typeof value==='string'?Date.parse(value):NaN;return Number.isFinite(time)&&time>0&&time<=Date.now()+300000?time:null;}
  function cacheChecked(p,time){
    const checkedAt=checkedTime(p?.checkedAt)||checkedTime(time);if(!validProduct(p,p?.handle)||p.variantsComplete!==true||p.variants.some(v=>v.availabilityKnown===false)||!checkedAt||checkedAt+300000<=Date.now()||p.detailState==='unconfirmed')return false;
    const prior=checkedInventory.get(p.handle);if(prior&&prior.product.id===p.id&&prior.checkedAt>checkedAt){const product={...prior.product};for(const key of ['cartHold','recommendationHold','meaningHold'])if(typeof p[key]==='boolean')product[key]=p[key];checkedInventory.set(p.handle,{...prior,product});remember(product);return true;}const product={...p,checkedAt,detailState:'checked'};checkedInventory.set(p.handle,{product,checkedAt,expiresAt:checkedAt+300000});remember(product);return true;
  }
  function publicCopy(p){try{return JSON.parse(JSON.stringify(p));}catch{return null;}}
  function cachedRead(handle){const row=checkedInventory.get(handle);return row&&row.expiresAt>Date.now()?{live:true,product:publicCopy(row.product),checkedAt:row.checkedAt,expiresAt:row.expiresAt,cached:true}:null;}
  function inventoryStatus(){const loaded=[...inventoryRows].filter(([handle,p])=>{const row=checkedInventory.get(handle);return row?.expiresAt>Date.now()&&row.product.id===p.id;}).length,ready=inventoryState.total>0&&loaded===inventoryState.total&&inventoryCoverageComplete&&!inventorySourcePartial&&!inventoryPageFailure;return {...inventoryState,loaded,ready,partial:inventorySourcePartial||inventoryPageFailure||!ready&&!inventoryState.loading,catalogueComplete:false};}
  function publishInventory(){
    Object.assign(inventoryState,inventoryStatus());
    const text=inventoryState.ready?inventoryState.loaded+' products ready for your guide':inventoryState.loading?'Preparing product knowledge · '+inventoryState.loaded+' of '+inventoryState.total:'Product knowledge checked · '+inventoryState.loaded+' of '+inventoryState.total;
    const marker=document.querySelector('#inventory-status');if(marker){marker.textContent=text;marker.dataset.ready=String(inventoryState.ready);}
    document.dispatchEvent(new CustomEvent('brites-storefront:inventory',{detail:inventoryStatus()}));
  }
  function provisional(p){const prior=known.get(p.handle),same=prior?.id===p.id,base=same&&p.variants.length===0?{...prior,...p,variants:prior.variants,options:prior.options,images:prior.images,description:prior.description.length>p.description.length?prior.description:p.description}:p;return {...base,detailState:'unconfirmed',variants:base.variants.map(v=>({...v,available:false,availabilityKnown:false}))};}
  try{const saved=JSON.parse(sessionStorage.getItem('brites-sandbox-product-identities')||'[]');if(Array.isArray(saved))saved.slice(-250).forEach(v=>{if(Array.isArray(v)&&PRODUCT.test(v[0]||'')&&validHandle(v[1]))identities.set(v[0],v[1]);});}catch{}
  function checkedProducts(data){
    const seen=new Set();
    return (Array.isArray(data?.products)?data.products:[]).slice(0,250).filter(p=>validProduct(p,p?.handle)&&!seen.has(p.id)&&seen.add(p.id)).map(remember);
  }
  function checkedPageInfo(data,requestedCursor=null){
    // Returned rows may exclude studio-only listings. Only the upstream
    // continuation says whether another public catalogue page is available.
    const info=data?.pageInfo||{},hasNextPage=info.hasNextPage===true;
    if(!hasNextPage)return {hasNextPage:false,endCursor:null};
    const cursor=info.endCursor;
    if(typeof cursor!=='string'||!cursor.trim()||cursor.length>2048||/[\u0000-\u001f]/.test(cursor)||cursor===requestedCursor)throw Error('The next collection page could not be confirmed.');
    return {hasNextPage:true,endCursor:cursor};
  }
  function minimum(p){const plan=activeSearchPlan(),candidates=plan&&hasSearchVariantCriteria(plan)?matchingSearchVariants(p,plan):p.variants.filter(v=>v.available||v.availabilityKnown===false);return candidates.length?Math.min(...candidates.map(v=>v.price)):null;}
  function projection(p){const plan=activeSearchPlan(),available=plan&&hasSearchVariantCriteria(plan)?matchingSearchVariants(p,plan):p.variants.filter(v=>v.available),prices=available.map(v=>v.price);return {...p,minPrice:minimum(p),suggestedVariantId:available[0]?.id||null,...(plan&&hasSearchVariantCriteria(plan)?{matchingVariantIds:available.map(v=>v.id),matchingPriceRange:prices.length?{min:Math.min(...prices),max:Math.max(...prices),currency:p.currency}:null,searchCriteria:publicSearchCriteria(plan)}:{})};}
  function categoryMatches(p,category){
    if(!SEED_CATEGORIES.includes(category)&&typeof window.BritesCatalogueIntents?.categoryMatches==='function')return window.BritesCatalogueIntents.categoryMatches(p,category);
    if(SEED_CATEGORIES.includes(category)){
      if(Array.isArray(p.storeCategories)&&p.storeCategories.every(c=>SEED_CATEGORIES.includes(c)))return p.storeCategories.includes(category);
      const title=clean(p.title,300),type=clean(p.type,100),text=title+' '+type,finished=/\b(?:necklaces?|earrings?|studs?|huggies?|hoops?|bracelets?|rings?)\b/i.test(title);
      if(category==='stud-earrings')return categoryMatches(p,'earrings')&&/\bstuds?\b/i.test(text);
      if(category==='hoop-earrings')return categoryMatches(p,'earrings')&&/\b(?:hoops?|huggies?)\b/i.test(text);
      if(category==='charm-only')return !finished&&!/\b(?:engraving|design fee|chain extender)\b/i.test(title)&&/\bcharms?\b/i.test(text);
      if(!categoryMatches(p,'necklaces')||/\bnecklace\s+charms?\b/i.test(title))return false;
      const chains=(Array.isArray(p.options)?p.options:[]).filter(o=>/\bchain\b/i.test(o.name||'')).flatMap(o=>Array.isArray(o.values)?o.values:[]),beady=/\b(?:beads?|beady)\b/i.test(text)||chains.some(value=>/\b(?:beads?|beady)\b/i.test(String(value)));
      return category==='beady-necklaces'?beady:!beady;
    }
    const title=clean(p.title,300),type=clean(p.type,100),pattern=CATEGORY_NAMES[category];if(!pattern)return false;
    // Studio is a grouping used by services and physical custom pieces. Its
    // letters are not "stud", and its charm label is not a piece definition.
    const studio=/^(?:custom\s+charm\s+)?studio(?:\s+(?:services?|components?|add[ -]?ons?))?$/i.test(type);
    const service=/^(?:custom\s+)?(?:design\s+fees?\b|engraving(?:\s+(?:on|for|services?)\b|$))/i.test(title);
    const extender=/\bchain\s+extenders?\b/i.test(title)&&!/\b(?:earrings?|studs?|huggies?|hoops?|bracelets?|rings?)\b/i.test(title);
    if(service||extender)return false;
    // A finished source type outranks descriptive words such as "pendant",
    // "hoop" or "stud" in the name. Charm/Studio types still need title
    // evidence, since they also group finished necklaces and custom earrings.
    if(category==='necklaces'||category==='earrings'){
      if(/^(?:necklaces?|pendants?)$/i.test(type))return category==='necklaces';
      if(/^(?:earrings?|(?:studs?|huggies?|hoops?)(?:\s+earrings?)?)$/i.test(type))return category==='earrings';
      if(category==='necklaces'&&/\bearrings?\b/i.test(title)&&!/\bnecklaces?\b/i.test(title))return false;
    }
    // Only an explicit single-category tag can supplement title/type. Broad
    // body copy and unstructured promotional tags never reclassify a piece.
    const tags=(Array.isArray(p.tags)?p.tags:[]).slice(0,80).flatMap(tag=>{const match=clean(tag,100).match(/^(?:product[ _-]?type|category)\s*:\s*(necklaces?|pendants?|earrings?|studs?|huggies?|hoops?|bracelets?|rings?|charms?)$/i);return match?[match[1]]:[];});
    return pattern.test(title+' '+(studio?'':type)+' '+tags.join(' '));
  }
  function searchWords(value){
    return (clean(value,16000).toLowerCase().match(/[\p{L}\p{N}]+/gu)||[]).map(word=>{
      if(/^(?:bunnies|rabbits?|bunny)$/.test(word))return 'bunny';
      if(word==='leaves')return 'leaf';if(word==='wolves')return 'wolf';
      if(word.length>4&&word.endsWith('ies'))return word.slice(0,-3)+'y';
      return word.length>3&&word.endsWith('s')&&!word.endsWith('ss')?word.slice(0,-1):word;
    });
  }
  function searchMaterial(value){
    const text=String(value||'').toLowerCase().normalize('NFKC').replace(/(\d{1,2}\s*k(?:t)?)(?=[a-z])/g,'$1 ').replace(/gold[- ]?filled/g,'gold filled').replace(/gold[- ]?plated/g,'gold plated').replace(/rosegold/g,'rose gold');
    const karats=[...new Set([...text.matchAll(/\b(8|9|10|12|14|18|20|22|24)\s*(?:k(?:t)?|karats?|carats?)\b/g)].map(match=>Number(match[1])).concat(/\b14\s*\/\s*20\b/.test(text)?[14]:[]))],silver=/\b(?:sterling(?:\s+silver)?|silver)\b/.test(text),gold=/\bgold\b|\bgf\b/.test(text)||karats.length>0,clear=/\b(?:any|either|both)\s+(?:metals?|materials?)\b/.test(text);
    if(!silver&&!gold&&!clear)return null;if(clear||silver&&gold)return {explicit:true,material:null,label:''};
    const family=silver?'silver':'gold',color=family==='gold'?/\brose\s+gold\b/.test(text)?'rose':/\bwhite\s+gold\b/.test(text)?'white':'yellow':null,kind=family==='silver'?/\bsterling\b/.test(text)?'sterling':'any':/\bgold\s+filled\b|\bgf\b/.test(text)?'filled':/\bplated\b|\bvermeil\b/.test(text)?'plated':/\bsolid\b/.test(text)?'solid':'any';
    const label=family==='silver'?(kind==='sterling'?'sterling silver':'silver'):(karats.length?karats.map(k=>k+'k').join(' or ')+' ':'')+(color==='yellow'?'':color+' ')+(kind==='solid'?'solid ':'')+'gold'+(kind==='filled'?' filled':kind==='plated'?' plated':'');
    return {explicit:true,material:{family,color,kind,karats},label};
  }
  function stripSearchMaterial(value){return value.replace(/\b(?:8|9|10|12|14|18|20|22|24)\s*(?:k(?:t)?|karats?|carats?)\b|\b14\s*\/\s*20\b/g,' ').replace(/\b(?:(?:solid|rose|white|yellow)\s+){0,2}(?:gold(?:\s*[- ]?\s*(?:filled|plated))?|sterling(?:\s+silver)?|silver|gf)\b(?:\s+(?:filled|plated|solid))?/g,' ');}
  function publishedMaterialMatches(value,request){
    if(typeof window.BritesCatalogueIntents?.materialMatches==='function')return window.BritesCatalogueIntents.materialMatches(value,request);
    if(!request)return true;const actual=searchMaterial(value);if(!actual?.material)return false;const material=actual.material;
    if(material.family!==request.family||request.color&&material.color!==request.color||request.kind!=='any'&&material.kind!==request.kind)return false;
    return !request.karats.length||material.karats.length===1&&request.karats.includes(material.karats[0]);
  }
  function searchBudget(raw,defaultCurrency='USD'){
    const amount='(?:(?:usd|cad|aud|nzd|eur|gbp)\\s*)?(?:(?:us|ca|c|au|a|nz)?[$£€]\\s*)?(\\d+(?:\\.\\d{1,2})?)(?:\\s*(?:usd|cad|aud|nzd|eur|gbp|dollars?))?',upper=raw.match(new RegExp('\\b(?:under|below|max(?:imum)?|up to|less than|at most|within|budget(?: of| is)?)\\s*(?:(?:is|of|to|:)\\s*)?'+amount,'i')),lower=raw.match(new RegExp('\\b(?:over|above|min(?:imum)?|more than|at least)\\s*(?:(?:is|of|to|:)\\s*)?'+amount,'i'));
    const codes=[...new Set((raw.match(/\b(?:usd|cad|aud|nzd|eur|gbp)\b/g)||[]).map(value=>value.toUpperCase()))];if(/\b(?:ca|c)\$/.test(raw))codes.push('CAD');if(/\bus\$/.test(raw))codes.push('USD');if(/\b(?:au|a)\$/.test(raw))codes.push('AUD');if(/\bnz\$/.test(raw))codes.push('NZD');if(raw.includes('£'))codes.push('GBP');if(raw.includes('€'))codes.push('EUR');const unique=[...new Set(codes)],currency=unique[0]||((upper||lower)?defaultCurrency:null);
    return {max:upper?Number(upper[1]):null,min:lower?Number(lower[1]):null,currency,invalid:unique.length>1,text:raw.replace(upper?.[0]||/a^/,' ').replace(lower?.[0]||/a^/,' ')};
  }
  let searchPlanMemo=null;
  function activeSearchPlan(){if(state.pageKind!=='catalogue'||!state.search||state.search!==state.checkedSearch)return null;const key=state.search+'|'+(window.Shopify?.currency?.active||'USD');if(searchPlanMemo?.key===key)return searchPlanMemo.plan;const plan=searchPlan(state.search);searchPlanMemo={key,plan};return plan;}
  function hasSearchVariantCriteria(plan){return !!plan.material||plan.max!==null||plan.min!==null||!!plan.currency||plan.invalid;}
  function publicSearchCriteria(plan){return {material:plan.materialLabel||null,minPrice:plan.min,maxPrice:plan.max,currency:plan.currency||null};}
  function matchingSearchVariants(p,plan){if(plan.invalid)return [];return p.variants.filter(v=>v.available&&v.availabilityKnown!==false&&(!plan.currency||p.currency===plan.currency)&&(plan.max===null||v.price<=plan.max)&&(plan.min===null||v.price>=plan.min)&&(!plan.material||(v.options||[]).filter(o=>/metal|material|finish/i.test(o.name)).some(o=>publishedMaterialMatches(o.value,plan.material))));}
  function searchPlan(query){
    if(typeof window.BritesCatalogueIntents?.plan==='function')return window.BritesCatalogueIntents.plan(clean(query,250),{currency:window.Shopify?.currency?.active||'USD'});
    // A shop search may OR every word. Keep ordinary category/price phrasing,
    // but require substantive listing-name terms rather than just "necklace".
    const raw=clean(query,250).toLowerCase().normalize('NFKC').replace(/(\d{1,2}\s*k(?:t)?)(?=[a-z])/g,'$1 '),material=searchMaterial(raw),budget=searchBudget(raw,window.Shopify?.currency?.active||'USD'),specific=[['regular-necklaces',/\b(?:regular|classic|standard)\s+necklaces?\b/],['beady-necklaces',/\b(?:beady|beaded|bead[- ]chain)\b/],['stud-earrings',/\bstuds?\b/],['hoop-earrings',/\b(?:hoops?|huggies?)\b/],['charm-only',/\bcharms?[ -]only\b/]].filter(([,pattern])=>pattern.test(raw)),categories=specific.length?specific.map(([category])=>category):Object.keys(CATEGORY_NAMES).filter(category=>CATEGORY_NAMES[category].test(raw));
    const withoutPrice=budget.text.replace(/\b(?:under|below|over|above|within|around|about|budget|max(?:imum)?|min(?:imum)?|up to|less than|more than|at most|at least|between)\s*(?:(?:is|of|to|:)\s*)?(?:(?:usd|cad|eur|gbp|dollars?)\s*|[$£€]\s*)?\d+(?:\.\d+)?(?:\s*(?:and|to|[-–])\s*(?:(?:usd|cad|eur|gbp|dollars?)\s*|[$£€]\s*)?\d+(?:\.\d+)?)?(?:\s*(?:usd|cad|eur|gbp|dollars?))?/gi,' ').replace(/(?:[$£€]\s*|\b(?:usd|cad|eur|gbp)\s+)\d+(?:\.\d+)?/gi,' ');
    const terms=searchWords(stripSearchMaterial(withoutPrice).replace(/\b(?:usd|cad|aud|nzd|eur|gbp)\b/g,' ')).filter(word=>(!SEARCH_CONTEXT.has(word)||['rose','white','yellow'].includes(word))&&!['regular','classic','standard','beady','beaded','bead','only','recommend','suggest','browse','list','more','ones','one','another','instead','either','both','any','material','metal','different'].includes(word)&&!Object.values(CATEGORY_NAMES).some(pattern=>pattern.test(word)));
    return {categories,terms:[...new Set(terms)],...budget,material:material?.material||null,materialLabel:material?.label||null};
  }
  function searchMatches(p,plan){
    if(plan.categories.length&&!plan.categories.some(category=>categoryMatches(p,category)))return false;
    if(hasSearchVariantCriteria(plan)&&!matchingSearchVariants(p,plan).length)return false;
    if(typeof window.BritesCatalogueIntents?.match==='function')return window.BritesCatalogueIntents.match(p,plan,{categoryMatches,variants:false});
    // Body copy may cross-sell unrelated symbols. Only names, canonical
    // handles, live type and literal/explicit motif tags establish this match.
    const tags=(Array.isArray(p.tags)?p.tags:[]).slice(0,80).flatMap(tag=>{const value=clean(tag,100),match=value.match(/^(?:motif|symbol|theme)\s*:\s*([\p{L}\p{N}\s-]+)$/iu);return match?[match[1]]:/^[\p{L}\p{N}-]+$/u.test(value)?[value]:[];});
    const words=new Set(searchWords(p.title+' '+p.handle+' '+(p.type||'')+' '+tags.join(' ')));
    return plan.terms.every(term=>words.has(term));
  }
  function filtered(){
    const checked=state.search===state.checkedSearch,plan=checked&&state.search?searchPlan(state.search):null,words=state.search.toLowerCase().match(/[\p{L}\p{N}-]+/gu)||[];
    let pieces=state.products.filter(p=>{
      const text=(p.title+' '+(p.type||'')+' '+p.description).toLowerCase();
      if(!checked&&words.length&&!words.every(word=>text.includes(word)))return false;
      if(plan&&!searchMatches(p,plan))return false;
      if(state.filter==='available')return p.variants.some(v=>v.available);
      if(state.filter==='all')return true;
      return categoryMatches(p,state.filter);
    });
    if(state.sort==='title-asc'||state.sort==='title-desc')pieces=pieces.slice().sort((a,b)=>(state.sort==='title-desc'?-1:1)*a.title.localeCompare(b.title));
    if(state.sort==='price-asc'||state.sort==='price-desc')pieces=pieces.slice().sort((a,b)=>{
      if(a.currency!==b.currency)return a.currency.localeCompare(b.currency);
      const x=minimum(a),y=minimum(b);if(x==null)return y==null?0:1;if(y==null)return -1;return (state.sort==='price-desc'?-1:1)*(x-y)||a.title.localeCompare(b.title);
    });
    return pieces;
  }
  function snapshot(){
    const pieces=state.pageKind==='product'&&state.current?[state.current]:state.pageKind==='catalogue'?filtered().slice(0,state.limit):[];
    const rendered=new Set(),inView=new Set();main.querySelectorAll('[data-product-handle]').forEach(card=>{rendered.add(card.dataset.productHandle);const r=card.getBoundingClientRect();if(r.height>0&&r.bottom>0&&r.top<(window.innerHeight||800))inView.add(card.dataset.productHandle);});
    const actual=pieces.filter(p=>rendered.has(p.handle)),currentHandle=state.pageKind==='product'&&actual.some(p=>p.handle===state.currentHandle)?state.currentHandle:'',focusedHandle=actual.some(p=>p.handle===state.focusedHandle)?state.focusedHandle:'';
    const chosen=(inView.size?actual.filter(p=>inView.has(p.handle)):actual).slice(0,24);
    [currentHandle,focusedHandle].filter(Boolean).forEach(handle=>{const p=actual.find(p=>p.handle===handle);if(p&&!chosen.some(v=>v.handle===handle)){if(chosen.length>=24)chosen.pop();chosen.push(p);}});
    const visiblePieces=chosen.map(p=>({id:p.id,handle:p.handle,title:clean(p.title,300)}));
    const loadedPieces=(state.pageKind==='product'&&state.current?[state.current]:state.pageKind==='catalogue'?filtered():[]).slice(0,200).map(p=>({id:p.id,handle:p.handle,title:clean(p.title,300),...(Array.isArray(p.storeCategories)?{storeCategories:p.storeCategories.filter(c=>FILTERS.has(c)).slice(0,5)}:{})}));
    const dictionary=new Map(state.browse.slice(0,160).concat([...inventoryRows.values()]).map(p=>[p.handle,p])),inventoryPieces=[...dictionary.values()].slice(0,160).map(p=>({id:p.id,handle:p.handle,title:clean(p.title,300),...(Array.isArray(p.storeCategories)?{storeCategories:p.storeCategories.filter(c=>SEED_CATEGORIES.includes(c)).slice(0,5)}:{})}));
    return {pageKind:state.pageKind==='catalogue'?'collection':state.pageKind,currentHandle,focusedHandle,visiblePieces,loadedPieces,inventoryPieces,search:state.search,sort:state.sort,filter:state.filter,contextRevision:state.contextRevision,discoveryRevision:state.discoveryRevision,activeSection:state.activeSection,loading:state.loading,inventory:inventoryStatus(),navigationControls:{canGoBack:navigationIndex>0,canGoForward:navigationIndex<navigationKeys.length-1},...(state.pageKind==='product'&&state.current?{galleryControls:{handle:state.current.handle,imageCount:productImages(state.current).length,selectedIndex:state.selectedImage+1}}:{}),...controlSnapshot()};
  }
  let attendedControl=null,attendedControlNode=null;
  function controlSnapshot(){
    const ui=currentProductUI(),cart=readCart(),lines=cartIdentities(cart),visible=new Set([...main.querySelectorAll('[data-bag-line]')].map(row=>row.dataset.bagLine)),checkout=checkoutUI?.page?.isConnected&&state.pageKind==='checkout'?checkoutUI:null;
    const variant=ui?.exactVariant(),q=ui?.quantity(),selectedVariant=variant?{id:variant.id,title:variant.title,price:variant.price,currency:ui.product.currency,available:variant.available,options:(variant.options||[]).map(o=>({name:o.name,value:o.value})),...(validQuantity(q)?{quantity:q,subtotal:Math.round(variant.price*q*100)/100}:{})}:null,totals=new Map();cart.forEach(item=>totals.set(item.currency,(totals.get(item.currency)||0)+item.price*(item.quantity||1)));
    const more=state.pageKind==='catalogue'&&main.querySelector('[data-catalogue-more]'),story=ui&&main.querySelector('[data-store-section=story]'),storyButton=story?.querySelector('[data-story-open]');
    const noteField=state.pageKind==='bag'&&main.querySelector('[data-store-section=gifts] textarea[name=note]'),briefField=main.querySelector('[data-store-section=customize] textarea[name=idea]'),privateControl=(field,key)=>({available:true,enabled:!field.disabled,hasText:!!readPreferences()[key],maxLength:300,savedKnown:field.value===(readPreferences()[key]||'')});
    return {...(state.pageKind==='catalogue'?{catalogueControls:{moreAvailable:!!more,moreEnabled:!!more&&!more.disabled&&!state.loading,visibleCount:main.querySelectorAll('.piece-card[data-product-handle]').length,loadedCount:state.products.length,hasNextPage:state.pageInfo.hasNextPage===true}}:{}),...(story?{storyControls:{handle:ui.product.handle,available:!!storyButton&&!storyButton.hidden,enabled:!!storyButton&&!storyButton.hidden&&!storyButton.disabled&&!ui.product.meaningHold&&!ui.product.recommendationHold,loaded:!!story.querySelector('.reviewed-story')}}:{}),...(noteField?{bagNoteControls:privateControl(noteField,'giftNote')}:{}),...(briefField?{customizeBriefControls:privateControl(briefField,'customIdea')}:{}),controlVersion:1,changeControls:{canUndo:undoAvailable(),kind:lastChange?.kind||null},imageControls:{open:!!document.querySelector('#storefront-image-dialog[open]')},engravingControls:ui?.engravingInput?{available:true,enabled:!ui.engravingInput.disabled,hasText:!!ui.engravingInput.value,maxLength:300}:null,productControls:ui?{handle:ui.product.handle,productId:ui.product.id,productTitle:ui.product.title,productType:ui.product.type||'',variantId:ui.select.value||null,quantity:q,optionsOpen:!ui.menu.hidden||attendedControl?.handle===ui.product.handle&&attendedControl.kind==='variant',openedOption:ui.openedOption||null,...(attendedControl?.handle===ui.product.handle&&attendedControlNode?.isConnected?{focusedOptionName:ui.groups.some(g=>g.name===attendedControl.optionName)&&!attendedControlNode.closest('[data-option-name]')?.hidden?attendedControl.optionName:null,quantityFocused:attendedControl.kind==='quantity',variantPickerFocused:attendedControl.kind==='variant'}:{}),optionGroups:ui.groups.map(g=>({name:g.name,values:g.values.slice()})),selectedOptions:ui.choices(),selectedVariant,...ui.selectionState(),busy:ui.busy(),...(variant&&validQuantity(q)?{itemTotalPrice:Math.round(variant.price*q*100)/100}:{}),reviewReady:!!ui.review?.isConnected,variantChoices:[...ui.select.options].filter(option=>VARIANT.test(option.value)&&!option.disabled).flatMap(option=>{const variant=ui.product.variants.find(variant=>variant.id===option.value);return variant?[{id:variant.id,title:variant.title,options:exactVariantOptions(variant)}]:[];}).slice(0,250)}:null,bagControls:{countKnown:true,linesComplete:visible.size===cart.length&&lines.every(id=>visible.has(id)),...(state.pageKind==='bag'&&attendedControl?.lineId&&visible.has(attendedControl.lineId)&&attendedControlNode?.isConnected?{focusedLineId:attendedControl.lineId,focusedOptionName:attendedControl.optionName||null,quantityFocused:attendedControl.kind==='quantity'}:state.pageKind==='bag'&&[...bagMenus.keys()].some(id=>visible.has(id))?{focusedLineId:[...bagMenus.keys()].find(id=>visible.has(id)),focusedOptionName:bagMenus.get([...bagMenus.keys()].find(id=>visible.has(id)))||null,quantityFocused:false}:{}),lines:cart.flatMap((item,index)=>visible.has(lines[index])?[publicBagLine(item,lines[index])]:[]),itemCount:cart.reduce((sum,item)=>sum+(item.quantity||1),0),subtotals:[...totals].map(([currency,total])=>({currency,total:Math.round(total*100)/100})),...(totals.size===1?{currency:[...totals.keys()][0],total:Math.round([...totals.values()][0]*100)/100}:{})},checkoutControls:checkout?{step:checkout.step,shipping:checkout.shipping,acknowledged:checkout.check.checked,complete:checkout.completed,mode:'simulation-only',orderPlaced:false,paymentTaken:false}:null};
  }
  function publicBagLine(item,lineId){
    const line={lineId,productId:item.productId,variantId:'gid://shopify/ProductVariant/'+item.variantId,title:item.title,variant:item.variant,quantity:item.quantity||1,price:item.price,currency:item.currency,subtotal:Math.round(item.price*(item.quantity||1)*100)/100},binding=bagProductControls(item),row=[...main.querySelectorAll('[data-bag-line]')].find(row=>row.dataset.bagLine===lineId);
    if(binding&&row&&state.pageKind==='bag'){
      const controls=[...row.querySelectorAll('[data-bag-option]')],groups=binding.groups.filter(g=>controls.some(c=>c.dataset.bagOption===g.name&&!c.disabled));
      if(groups.length){line.handle=binding.p.handle;line.optionGroups=groups.map(g=>({name:g.name,values:g.values.slice()}));line.selectedOptions=exactVariantOptions(binding.v);const opened=[...row.querySelectorAll('[data-bag-menu]')].filter(menu=>!menu.hidden);line.optionsOpen=opened.length>0;line.openedOption=opened.length===1?opened[0].dataset.bagMenu:null;}
      const field=row.querySelector('[data-bag-engraving]');if(field)line.engravingControls={available:true,enabled:!field.disabled,hasText:!!item.engravingPreview,maxLength:300};
      if(item.customizationPreview===true)line.customizationReviewRequired=true;
    }
    return line;
  }
  function currentProductUI(){return productUI?.panel?.isConnected&&state.pageKind==='product'&&!state.loading&&state.current===productUI.product&&state.currentHandle===productUI.product.handle?productUI:null;}
  async function sendGuideCommand(command){
    const input=document.querySelector('#guide-request'),reply=document.querySelector('#guide-request-status'),form=document.querySelector('#guide-request-form');
    if(guideBusy||!clean(command,700))return;
    if(typeof window.BritesConcierge?.sendShopperCommand!=='function'){if(reply)reply.textContent='Your guide is still loading. Try again in a moment.';return;}
    guideBusy=true;form?.setAttribute('aria-busy','true');if(input)input.value=command;renderGuideCommands();if(reply)reply.textContent='Following your request…';
    try{const result=await window.BritesConcierge.sendShopperCommand(command);if(reply)reply.textContent=clean(result?.reply||result?.message||result?.reason||result?.error,650)||(result?.ok===false?'The request could not be completed. Check your guide’s reply.':'Your request is complete. See the updated page below.');}
    catch{if(reply)reply.textContent='The request could not be completed. Try it in the guide’s message box.';}
    finally{guideBusy=false;form?.setAttribute('aria-busy','false');renderGuideCommands();}
  }
  function renderGuideCommands(){
    const container=document.querySelector('#guide-commands'),summary=document.querySelector('#guide-selection');if(!container)return;
    const ui=currentProductUI(),pc=controlSnapshot().productControls,commands=[],add=(label,command)=>commands.push({label,command});let sequence='';
    if(state.pageKind==='catalogue'){
      const row=filtered().slice(0,24).map(p=>checkedInventory.get(p.handle)).find(row=>row?.expiresAt>Date.now()),piece=row?.product,group=piece&&exactOptionGroups(piece).find(g=>/metal|material/i.test(g.name)),value=group&&piece.variants.find(v=>v.available&&(v.options||[]).some(o=>o.name===group.name))?.options.find(o=>o.name===group.name)?.value;if(piece&&group&&value)sequence='Open '+piece.title+' then open the '+group.name+' menu then select '+value+' then highlight the price';
      [['regular-necklaces','Regular necklaces'],['beady-necklaces','Beady necklaces'],['stud-earrings','Stud earrings'],['hoop-earrings','Hoop earrings'],['charm-only','Charm-only']].forEach(([,label])=>add(label,'Show me '+label.toLowerCase()));
      const attendedPiece=known.get(state.focusedHandle);if(attendedPiece)add('About '+attendedPiece.title,'Tell me about '+attendedPiece.title);
      filtered().slice(0,2).forEach(p=>add('Open '+p.title,'Open '+p.title));add('Lowest price first','Sort by price low to high');
    }else if(ui){
      add('Ask the price','What is the price of this piece?');
      const group=ui.groups.find(g=>/metal|material/i.test(g.name)),value=group&&ui.product.variants.find(v=>v.available&&(v.options||[]).some(o=>o.name===group.name))?.options.find(o=>o.name===group.name)?.value;if(group&&value)sequence='Open the '+group.name+' menu then select '+value+' then set quantity to 2 then highlight the price';
      add('Highlight price','Highlight the price of this piece');add('Show details','Highlight the details of this piece');
      add('Show title','Highlight the title of this piece');add('Show description','Highlight the description of this piece');
      ui.groups.forEach(group=>add('Open '+group.name,'Open the '+group.name+' menu for this piece'));
      const groups=pc.openedOption?ui.groups.filter(g=>g.name===pc.openedOption):ui.groups.filter(g=>/metal|material/i.test(g.name));
      groups.forEach(group=>group.values.filter(value=>ui.product.variants.some(v=>v.available&&v.availabilityKnown!==false&&(v.options||[]).some(o=>String(o.name).toLowerCase()===group.name.toLowerCase()&&o.value===value))).slice(0,8).forEach(value=>add('Choose '+value,'Select '+value+' for this piece')));
      if(pc.optionsOpen)add('Close options','Close the options menu');
      if(pc.selectedVariant)add('Quantity 2','Set quantity to 2');if(pc.selectionStatus==='ready')add('Add to my bag','Add this piece to my bag');
      if(ui.groups.some(g=>/metal|material/i.test(g.name)))add('Compare materials','Compare the published materials for this piece and their prices');
      if(ui.imageCount>1){if(state.selectedImage>0)add('Previous image','Show previous image');if(state.selectedImage+1<ui.imageCount)add('Next image','Show next image');add('Enlarge image','Enlarge the image of this piece');}
      add('Open my bag','Open my bag');
    }else if(state.pageKind==='bag'){
      const cart=readCart();cart.slice(0,3).forEach(item=>{if(cart.filter(row=>row.title===item.title).length===1){add('Quantity 2 · '+item.title,'Set '+item.title+' quantity to 2 in my bag');add('Remove '+item.title,'Remove '+item.title+' from my bag');}});
      add('Request gift wrapping','Enable gift wrapping');if(cart.length){add('Open test checkout','Open checkout');sequence='Enable gift wrapping then open checkout then show checkout shipping step then select express shipping';}add('Explore all pieces','Show me all pieces');
    }else if(state.pageKind==='checkout'){
      add('Review step','Show checkout review step');add('Shipping step','Show checkout shipping step');add('Demo express','Select express shipping');add('Confirmation step','Show checkout confirmation step');add('Complete test review','Complete test checkout');add('Open my bag','Open my bag');
      sequence='Show checkout review step then show checkout shipping step then select express shipping then show checkout confirmation step';
    }
    if(sequence&&sequence.length<=650&&!/[\u0000-\u001f]/.test(sequence))add('Try four steps',sequence);else sequence='';
    const example=document.querySelector('#guide-sequence');if(example){example.hidden=!sequence;example.textContent=sequence?'Say or type: “'+sequence+'”':'';}
    if(document.querySelector('#storefront-image-dialog'))add('Close enlarged image','Close the enlarged image');
    if(undoAvailable())add('Undo last change','Undo my last change');
    if(navigationIndex>0)add('Go back','Go back');if(navigationIndex<navigationKeys.length-1)add('Go forward','Go forward');add('Scroll down','Scroll down');
    const key=JSON.stringify([commands,guideBusy,state.loading]);if(guideCommandKey!==key){guideCommandKey=key;const essential=commands.filter(c=>['Try four steps','Ask the price','Open my bag','Open test checkout','Enlarge image','Next image','Undo last change','Close options','Close enlarged image','Go back','Go forward'].includes(c.label)),others=commands.filter(c=>!essential.includes(c));container.replaceChildren(...essential.concat(others.slice(0,Math.max(0,22-essential.length))).map(({label,command})=>{const control=button(label,'guide-command',()=>void sendGuideCommand(command));control.disabled=guideBusy||state.loading;control.title=command;return control;}));}
    const input=document.querySelector('#guide-request'),submit=document.querySelector('#guide-request-send');if(input)input.disabled=guideBusy;if(submit)submit.disabled=guideBusy;
    if(summary)summary.textContent=pc?pc.selectedVariant?pc.productTitle+' · '+pc.selectedVariant.title+' · quantity '+pc.quantity+' · '+money(pc.itemTotalPrice,pc.selectedVariant.currency)+' item subtotal':pc.productTitle+' · choose each published option to see the exact price':state.pageKind==='bag'?readCart().reduce((sum,item)=>sum+(item.quantity||1),0)+' pieces in your test bag':state.pageKind==='checkout'?'Test checkout · '+(checkoutUI?.step||'review')+' · purchases stay simulated':known.has(state.focusedHandle)?'Your pointer is on '+known.get(state.focusedHandle).title+'. Ask your guide about this exact piece.':'Move your pointer over a piece, then ask the guide about it. Choose a request below to see it operate the page.';
  }
  // Subject changes are separate from gaze, scrolling and other page context.
  // Commit only completed views so canceled reads cannot revive an old subject.
  function reviseDiscovery(){if(state.discoveryRevision<Number.MAX_SAFE_INTEGER)state.discoveryRevision++;}
  function publish(){if(publishing)return;publishing=true;try{state.contextRevision++;if(!restoringHistory&&!state.loading)saveCurrentView();renderGuideCommands();document.dispatchEvent(new CustomEvent('brites-storefront:context',{detail:snapshot()}));}finally{publishing=false;}}
  function status(message){notice.textContent=clean(message,300);notice.dataset.visible=message?'true':'false';clearTimeout(noticeTimer);if(message)noticeTimer=setTimeout(()=>{notice.dataset.visible='false';},3600);}
  function reduced(){return typeof matchMedia==='function'&&matchMedia('(prefers-reduced-motion: reduce)').matches;}
  function focusedElement(){
    let active=document.activeElement;for(let depth=0;depth<8&&active?.shadowRoot?.activeElement;depth++)active=active.shadowRoot.activeElement;return active;
  }
  function sameRevealFocus(initialFocus){
    const active=focusedElement();
    // Chromium blurs a busy disabled composer to BODY. That application-owned
    // transition is not a new shopper focus choice; an actual new control still
    // permanently withdraws the pending reveal through its focus listener.
    return active===initialFocus||active===document.body&&(!initialFocus?.isConnected||initialFocus.matches?.(':disabled')===true);
  }
  function trackRevealFocus(initialFocus){
    let allowed=true;const root=initialFocus?.getRootNode(),changed=()=>{if(!sameRevealFocus(initialFocus))allowed=false;};document.addEventListener('focusin',changed);if(root!==document){root?.addEventListener('focusin',changed);root?.addEventListener('focusout',changed);}return {allowed:()=>allowed,dispose:()=>{document.removeEventListener('focusin',changed);if(root!==document){root?.removeEventListener('focusin',changed);root?.removeEventListener('focusout',changed);}}};
  }
  function mayRevealCollection(initialFocus){
    if(document.hidden)return false;const active=focusedElement();
    if(!active?.isConnected||active===document.body||active===document.documentElement)return true;
    // A completed read must not scroll a retained collection control, or a
    // newer focus choice, offscreen. An unchanged fixed guide composer can
    // still initiate the ordinary website reveal.
    return !collectionControls?.box?.contains(active)&&active===initialFocus;
  }
  function revealAboveGuide(target){
    // The same live menu and guide remain mounted. Use measured geometry;
    // ordinary, wide, dismissed and image-viewer layouts keep their own reveal.
    const width=Number(window.innerWidth),height=Number(window.innerHeight),host=document.querySelector('brites-concierge[data-open="true"][data-storefront-control-assist="true"]');
    if(!(width>0&&width<=640&&height>0)||document.hidden||!host||host.getAttribute('data-storefront-image-open')==='true'||typeof window.scrollBy!=='function')return false;
    const panel=host.shadowRoot?.querySelector('.panel'),bounds=panel?.getBoundingClientRect?.(),rect=target?.getBoundingClientRect?.(),viewport=window.visualViewport,top=Number.isFinite(viewport?.offsetTop)?viewport.offsetTop:0,bottom=top+(Number.isFinite(viewport?.height)&&viewport.height>0?viewport.height:height);
    if(!panel||panel.hidden||!bounds||!(bounds.width>0&&bounds.height>0&&bounds.top>top+80&&bounds.top<bottom)||!rect||!(rect.width>0&&rect.height>0))return false;
    const exposedTop=top+12,exposedBottom=Math.min(bottom-12,bounds.top-12),available=exposedBottom-exposedTop;
    if(!(available>=56&&Number.isFinite(rect.top)&&Number.isFinite(rect.height)))return false;
    const desired=exposedTop+Math.max(0,(available-Math.min(rect.height,available))/2),delta=rect.top-desired;
    if(Math.abs(delta)>1)window.scrollBy({top:delta,behavior:'instant'});return true;
  }
  function focusSection(section,highlight=true,{immediate=false}={}){
    if(!SECTIONS.has(section))return false;
    if(['materials','length','engraving'].includes(section)){const ui=currentProductUI(),group=ui?.groups.find(g=>(section==='materials'?/metal|material/i:section==='length'?/length/i:/engrav|personali[sz]/i).test(g.name));if(group)ui.openOptions(group.name,false);}
    const target=main.querySelector('[data-store-section="'+section+'"]');if(!target)return false;
    clearTimeout(highlightTimer);main.querySelectorAll('.store-highlight').forEach(e=>e.classList.remove('store-highlight'));
    if(highlight){target.classList.add('store-highlight');highlightTimer=setTimeout(()=>target.classList.remove('store-highlight'),2600);}
    state.activeSection=section;publish();
    const ui=currentProductUI(),group=section==='options'&&ui&&!ui.menu.hidden?Array.from(ui.menu.querySelectorAll('.option-group')).find(row=>row.dataset.optionName===ui.openedOption):null;
    try{if(!revealAboveGuide(group||target))target.scrollIntoView?.({behavior:immediate?'instant':reduced()?'auto':'smooth',block:'center'});}catch{}
    return true;
  }
  function productSelection(){const ui=currentProductUI();return ui?{variantId:ui.select.value,quantity:ui.quantity(),choices:ui.choices(),opened:!ui.menu.hidden,openedOption:ui.openedOption,selectedImage:state.selectedImage}:null;}
  function productFacts(p){return JSON.stringify(['id','handle','currency','options','variants','variantsComplete','cartHold','recommendationHold','partsOnly'].map(key=>p?.[key]));}
  function productChangeValue(selection){return selection?{variantId:selection.variantId||'',quantity:selection.quantity,choices:selection.choices.slice().sort((a,b)=>a.name.localeCompare(b.name)),selectedImage:selection.selectedImage}:null;}
  function checkoutChoice(){const ui=checkoutUI;return ui?.page?.isConnected&&state.pageKind==='checkout'&&!ui.completed?{step:ui.step,shipping:ui.shipping,bag:JSON.stringify(readCart())}:null;}
  function collectionChoice(){return state.pageKind==='catalogue'?{search:state.search,checkedSearch:state.checkedSearch,collectionSource:state.collectionSource,sort:state.sort,filter:state.filter,limit:state.limit,ids:state.products.map(p=>p.id)}:null;}
  function rememberChange(kind,scope,before,after){
    if(restoringChange||restoringHistory||before==null||after==null||JSON.stringify(before)===JSON.stringify(after))return;
    lastChange={sequence:++changeSequence,kind,scope,before:publicCopy(before),after:publicCopy(after),at:Date.now()};
  }
  function changeValue(change){
    if(change.scope.kind==='product'){const ui=currentProductUI();if(!ui||ui.busy()||ui.product.id!==change.scope.id||ui.product.handle!==change.scope.handle||productFacts(ui.product)!==change.scope.facts)return null;return productChangeValue(productSelection());}
    if(change.scope.kind==='engraving'){const ui=currentProductUI();return ui?.engravingInput&&!ui.busy()&&ui.product.id===change.scope.id&&ui.product.handle===change.scope.handle?{text:ui.engravingInput.value}:null;}
    if(change.scope.kind==='bag')return {cart:readCart(),ids:cartIdentities(readCart())};
    if(change.scope.kind==='gift')return readPreferences();
    if(change.scope.kind==='checkout')return checkoutChoice();
    if(change.scope.kind==='collection')return collectionChoice();
    return null;
  }
  function undoAvailable(){return !!lastChange&&Date.now()-lastChange.at<1800000&&JSON.stringify(changeValue(lastChange))===JSON.stringify(lastChange.after);}
  function recordProductChange(kind,before,ui=currentProductUI()){
    if(!ui||ui.busy())return;rememberChange(kind,{kind:'product',id:ui.product.id,handle:ui.product.handle,facts:productFacts(ui.product)},productChangeValue(before),productChangeValue(productSelection()));
  }
  function syncGiftControls(){const prefs=readPreferences();main.querySelectorAll('[data-store-section=gifts]').forEach(form=>{const wrap=form.querySelector('[name=wrapping]'),pack=form.querySelector('[name=package]'),note=form.querySelector('[name=note]');if(wrap)wrap.checked=prefs.wrapping===true;if(pack)pack.checked=prefs.giftPackage===true;if(note)note.value=prefs.giftNote||'';});main.querySelectorAll('[name=idea]').forEach(field=>{field.value=prefs.customIdea||'';});refreshCheckoutPreferences();}
  function restoreCheckoutChoice(choice,{reveal=false}={}){
    const ui=checkoutUI;if(!choice||!ui?.page?.isConnected||state.pageKind!=='checkout'||ui.completed||!['review','shipping','confirm'].includes(choice.step)||!['standard','express'].includes(choice.shipping))return false;
    ui.step=choice.step;ui.shipping=choice.shipping;ui.page.querySelectorAll('[data-demo-shipping]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.demoShipping===ui.shipping)));ui.page.querySelectorAll('[data-checkout-step-button]').forEach(b=>b.setAttribute('aria-current',b.dataset.checkoutStepButton===ui.step?'step':'false'));ui.page.querySelectorAll('.checkout-step.store-highlight').forEach(n=>n.classList.remove('store-highlight'));
    if(reveal&&!document.hidden){const target=ui.page.querySelector('[data-checkout-step="'+ui.step+'"]');target?.classList.add('store-highlight');try{target?.scrollIntoView?.({behavior:'instant',block:'center'});}catch{}}
    return true;
  }
  async function undoLastChange(options){
    const change=lastChange;if(!change||!undoAvailable())return controlResult('undo',false,'There is no unchanged recent choice to undo on this view.');
    if(change.scope.kind==='bag'){
      const beforeCounts=new Map(change.before.cart.map((item,index)=>[change.before.ids[index],item.quantity||1])),afterCounts=new Map(change.after.cart.map((item,index)=>[change.after.ids[index],item.quantity||1])),restored=change.before.cart.filter((item,index)=>(beforeCounts.get(change.before.ids[index])||0)>(afterCounts.get(change.before.ids[index])||0));
      const changedChoices=change.before.cart.filter((item,index)=>{const prior=change.after.ids.indexOf(change.before.ids[index]),after=prior<0?null:change.after.cart[prior];return after&&(item.variantId!==after.variantId||item.variant!==after.variant||item.price!==after.price||JSON.stringify(item.variantOptions)!==JSON.stringify(after.variantOptions));});
      const check=[...new Set(restored.concat(changedChoices))];if(check.length)try{await verifyBag(check,options.signal,{allowCustomizationPreview:true});}catch(e){return controlResult('undo',false,e.message||'That saved option must be checked before it is restored.');}
    }
    if(options.signal?.aborted||lastChange!==change||!undoAvailable())return controlResult('undo',false,'A newer choice replaced the change you asked to undo.');
    restoringChange=true;let ok=true;
    try{
      if(change.scope.kind==='product'){const ui=currentProductUI();ok=ui.restoreSelection(change.before);if(ok)ui.revealSelection(change.kind==='product-quantity'?'quantity':undefined);}
      else if(change.scope.kind==='engraving'){const ui=currentProductUI();ui.setEngraving(change.before.text);if(!document.hidden)try{ui.engravingInput.scrollIntoView?.({behavior:'instant',block:'center'});}catch{}}
      else if(change.scope.kind==='bag'){writeCart(change.before.cart,change.before.ids);if(state.pageKind==='checkout')renderBag({push:false});}
      else if(change.scope.kind==='gift'){sessionStorage.setItem('brites-sandbox-gift-preferences',JSON.stringify(change.before));syncGiftControls();}
      else if(change.scope.kind==='checkout')ok=restoreCheckoutChoice(change.before,{reveal:true});
      else if(change.scope.kind==='collection'){const view=change.scope.view;state.products=view.products.map(p=>known.get(p.handle)||p);Object.assign(state,{search:view.search,checkedSearch:view.checkedSearch,collectionSource:view.collectionSource,sort:view.sort,filter:view.filter,limit:view.limit,pageInfo:{...view.pageInfo},verifiedAt:view.verifiedAt});reviseDiscovery();restoreCollection();restoreScroll(view.scroll);}
      else ok=false;
      if(ok)lastChange=null;
    }finally{restoringChange=false;}
    if(ok&&change.scope.kind==='product')currentProductUI()?.revealSelection(change.kind==='product-quantity'?'quantity':undefined);
    publish();status(ok?'Your last change is undone. Your earlier choice is visible.':'That earlier choice is no longer available.');return controlResult('undo',ok,notice.textContent,{cartChanged:ok&&change.scope.kind==='bag'});
  }
  function captureView(){
    const view={kind:state.pageKind,handle:state.currentHandle,activeSection:state.activeSection,scroll:Number.isFinite(window.scrollY)?window.scrollY:0};
    if(state.pageKind==='catalogue')Object.assign(view,{products:state.products.slice(),search:state.search,checkedSearch:state.checkedSearch,collectionSource:state.collectionSource,sort:state.sort,filter:state.filter,limit:state.limit,pageInfo:{...state.pageInfo},verifiedAt:state.verifiedAt});
    else if(state.pageKind==='product'){view.selection=productSelection();if(view.selection)productViews.set(view.handle,view.selection);}
    else if(state.pageKind==='checkout'){const choice=checkoutChoice();if(choice)view.checkout=choice;}
    return view;
  }
  const tabViewKey='brites-sandbox-view-v1';let savedTabView=null,savedCollectionView=null,tabViewFingerprint='';
  function savedPublicSelection(value){if(!value||typeof value!=='object'||!validQuantity(value.quantity)||typeof value.variantId!=='string'||value.variantId&&!VARIANT.test(value.variantId)||!Array.isArray(value.choices)||value.choices.length>12)return null;const choices=value.choices.filter(o=>typeof o?.name==='string'&&o.name.length<=120&&typeof o.value==='string'&&o.value.length<=300);if(choices.length!==value.choices.length||new Set(choices.map(o=>o.name)).size!==choices.length)return null;return {variantId:value.variantId,quantity:value.quantity,choices:choices.map(o=>({name:o.name,value:o.value})),selectedImage:Number.isInteger(value.selectedImage)&&value.selectedImage>=0&&value.selectedImage<50?value.selectedImage:0,opened:value.opened===true,openedOption:typeof value.openedOption==='string'&&value.openedOption.length<=120?value.openedOption:null};}
  try{const saved=JSON.parse(sessionStorage.getItem(tabViewKey)||'null');if(saved?.schema===1&&['home','catalogue','product','bag','checkout'].includes(saved.kind)&&typeof saved.handle==='string'&&(saved.handle===''||HANDLE.test(saved.handle))&&saved.handle.length<=180&&typeof saved.search==='string'&&saved.search.length<=180&&FILTERS.has(saved.filter)&&SORTS.has(saved.sort)&&Array.isArray(saved.handles)&&saved.handles.length<=500&&saved.handles.every(h=>typeof h==='string'&&h.length<=180&&HANDLE.test(h)))savedTabView={...saved,selection:savedPublicSelection(saved.selection)};}catch{}
  try{const saved=JSON.parse(sessionStorage.getItem(tabViewKey+'-collection')||'null');if(saved?.schema===1&&saved.kind==='catalogue'&&typeof saved.search==='string'&&saved.search.length<=180&&FILTERS.has(saved.filter)&&SORTS.has(saved.sort)&&Array.isArray(saved.handles)&&saved.handles.length<=500&&saved.handles.every(h=>typeof h==='string'&&h.length<=180&&HANDLE.test(h)))savedCollectionView=saved;}catch{}
  function persistTabView(view){if(!state.verifiedAt&&view.kind==='catalogue'||view.kind==='product'&&!state.current)return;const value={schema:1,kind:view.kind,handle:view.handle||'',search:state.search.slice(0,180),filter:state.filter,sort:state.sort,handles:view.kind==='catalogue'?filtered().slice(0,Math.min(state.limit,500)).map(p=>p.handle):[],selection:view.kind==='product'?savedPublicSelection(view.selection):null},stamp=JSON.stringify(value);if(stamp===tabViewFingerprint)return;tabViewFingerprint=stamp;try{sessionStorage.setItem(tabViewKey,stamp);if(view.kind==='catalogue'){sessionStorage.setItem(tabViewKey+'-collection',stamp);savedCollectionView=value;}}catch{}}
  function saveCurrentView(){if(state.loading||restoringHistory)return;const view=captureView(),key=navigationKeys[navigationIndex];if(key)historyViews.set(key,view);if(view.kind==='catalogue')collectionView=view;persistTabView(view);}
  function restoreScroll(value){if(!document.hidden&&Number.isFinite(value)&&value>=0)try{window.scrollTo?.({top:value,behavior:'instant'});}catch{}}
  async function restoreSavedView(view,{save=true,signal}={}){
    if(!view||signal?.aborted)return false;restoringHistory=true;
    try{cancelPending();if(view.kind==='catalogue'){if(!view.products.length&&!view.search&&view.collectionSource==='browse'&&!state.browseVerifiedAt){const result=await searchCatalogue('',{push:false,signal},{filter:view.filter,sort:view.sort});if(!result.ok)return false;}else{state.products=(!view.products.length&&!view.search&&view.collectionSource==='browse'?state.browse:view.products).map(p=>known.get(p.handle)||p);Object.assign(state,{search:view.search,checkedSearch:view.checkedSearch,collectionSource:view.collectionSource,sort:view.sort,filter:view.filter,limit:view.limit,pageInfo:{...view.pageInfo},verifiedAt:view.verifiedAt||state.browseVerifiedAt});restoreCollection();reviseDiscovery();commitPage('catalogue','',false);}}
      else if(view.kind==='product'){if(view.selection)productViews.set(view.handle,view.selection);if(!await openProduct(view.handle,{push:false,signal}))return false;}
      else if(view.kind==='bag')renderBag({push:false});else if(view.kind==='checkout'){const result=await renderCheckout({push:false,signal});if(!result.ok)return false;if(view.checkout?.bag===JSON.stringify(readCart()))restoreCheckoutChoice(view.checkout);}else return false;
      state.activeSection=view.activeSection||state.activeSection;restoreScroll(view.scroll);publish();return true;
    }finally{restoringHistory=false;if(save)saveCurrentView();}
  }
  async function navigateHistory(delta,options={}){
    if(historyTraversal)await historyTraversal.promise;
    if(options.signal?.aborted)return controlResult(delta<0?'back':'forward',false,'That action was cancelled.');
    if(delta<0&&navigationIndex===0&&savedCollectionView&&state.pageKind!=='catalogue'){
      const saved=savedCollectionView,version=navigationVersion;
      await preloadInventory();
      if(options.signal?.aborted||version!==navigationVersion)return controlResult('back',false,'The page changed. Please ask again.');
      const rows=saved.handles.map(handle=>checkedInventory.get(handle));
      if(rows.some(row=>!row||Date.now()-row.checkedAt>300000))return controlResult('back',false,'I couldn’t check the earlier results. Please try again.');
      const products=rows.map(row=>({...publicCopy(row.product),checkedAt:row.checkedAt}));
      const result=presentProducts(products,{searchQuery:saved.search,filter:saved.filter,sort:saved.sort,paging:{schema:1,mode:'rest',query:saved.search,displayCount:products.length,keepCurrent:false}});
      return controlResult('back',result.ok===true,result.ok?'Your earlier results are back.':result.message);
    }
    const index=navigationIndex+delta;if(index<0||index>=navigationKeys.length)return controlResult(delta<0?'back':'forward',false,'There is no earlier '+(delta<0?'view':'next view')+' in this test visit.');
    saveCurrentView();const prior=navigationIndex;navigationIndex=index;const restored=await restoreSavedView(historyViews.get(navigationKeys[index]),{signal:options.signal});if(!restored){navigationIndex=prior;return controlResult(delta<0?'back':'forward',false,'That saved view could not be restored.');}
    let done,timer;const traversal={key:navigationKeys[index],version:navigationVersion,promise:new Promise(resolve=>{done=resolve;})};traversal.finish=()=>{clearTimeout(timer);if(historyTraversal===traversal)historyTraversal=null;done();};historyTraversal=traversal;timer=setTimeout(traversal.finish,400);try{history.go(delta);}catch{traversal.finish();}await traversal.promise;
    return controlResult(delta<0?'back':'forward',true,delta<0?'The previous view is restored.':'The next view is restored.');
  }
  function cancelPending(){navigationVersion++;request?.abort();request=null;productUI?.cancelAdd?.();checkoutUI?.completionController?.abort();clearTimeout(highlightTimer);clearTimeout(focusTimer);focusTimer=0;main.querySelectorAll('.store-highlight').forEach(e=>e.classList.remove('store-highlight'));state.loading=false;}
  async function get(path,signal){if(typeof fetch!=='function')throw Error('The live selection could not be checked.');const response=await fetch(path,{cache:'no-store',...(signal?{signal}:{})}),data=await response.json();if(!response.ok)throw Error('The live selection could not be checked.');return data;}
  function validateSeed(data){
    const seed=data?.seed,rows=data?.products;if(data?.live!==true||!seed||seed.schema!==1||seed.target!==120||seed.minimum!==100||!Array.isArray(rows)||!rows.length||rows.length>160||!Number.isInteger(seed.loaded)||seed.loaded!==rows.length||typeof seed.complete!=='boolean'||typeof seed.partial!=='boolean'||!Number.isInteger(seed.sourcePages)||seed.sourcePages<0||seed.sourcePages>200||data.pageInfo?.hasNextPage!==false||data.pageInfo?.endCursor!==null)throw Error('The balanced collection could not be verified.');
    const ids=new Set(),handles=new Set(),counts=Object.fromEntries(SEED_CATEGORIES.map(c=>[c,0]));for(const p of rows){if(!validProduct(p,p?.handle)||ids.has(p.id)||handles.has(p.handle)||!Array.isArray(p.storeCategories)||p.storeCategories.length>5||new Set(p.storeCategories).size!==p.storeCategories.length||p.storeCategories.some(c=>!SEED_CATEGORIES.includes(c)))throw Error('The balanced collection contains an unchecked identity or category.');ids.add(p.id);handles.add(p.handle);p.storeCategories.forEach(c=>counts[c]++);}
    const absent=SEED_CATEGORIES.filter(c=>counts[c]===0),complete=rows.length>=100&&absent.length===0;if(SEED_CATEGORIES.some(c=>seed.categoryCounts?.[c]!==counts[c])||seed.complete!==complete||!Array.isArray(seed.unfilledCategories)||new Set(seed.unfilledCategories).size!==seed.unfilledCategories.length||seed.unfilledCategories.length!==absent.length||seed.unfilledCategories.some(c=>!absent.includes(c)))throw Error('The balanced collection coverage did not match its checked pieces.');
    const checkedAt=typeof data.checkedAt==='number'?data.checkedAt:typeof data.checkedAt==='string'?Date.parse(data.checkedAt):NaN;if(!Number.isFinite(checkedAt)||checkedAt<=0||checkedAt>Date.now()+300000)throw Error('The balanced collection check time is unavailable.');return {schema:1,target:120,minimum:100,loaded:rows.length,complete,categoryCounts:counts,unfilledCategories:absent,sourcePages:seed.sourcePages,partial:seed.partial};
  }
  async function catalogueStart(signal){
    if(document.body.dataset.catalogueSeed==='balanced'){try{const data=await get('/api/growth/catalogue?seed=1',signal);data.verifiedSeed=validateSeed(data);return data;}catch(e){if(signal?.aborted)throw e;const data=await get('/api/growth/catalogue?browse=1',signal);data.seedUnavailable=true;return data;}}
    return get('/api/growth/catalogue?browse=1',signal);
  }
  function applySeed(data){state.seedInfo=data.verifiedSeed||null;state.seedFailure=data.seedUnavailable===true;state.seedBrowseStarted=!state.seedInfo;}
  function applyInventoryPage(data,offset,total=null,cycle){
    const info=data?.inventory,rows=data?.products;if(data?.live!==true||info?.schema!==1||!Number.isInteger(info.total)||info.total<1||info.total>160||info.offset!==offset||total!==null&&info.total!==total||!Array.isArray(rows)||rows.length>24||rows.length!==Math.min(24,info.total-offset)||rows.some(p=>!validProduct(p,p?.handle)||!['checked','unconfirmed'].includes(p.detailState)))throw Error('The product knowledge page could not be checked.');
    const fingerprint=info.fingerprint==null?null:info.fingerprint;if(fingerprint!==null&&!/^[a-f0-9]{64}$/.test(fingerprint)||offset!==0&&fingerprint!==cycle.fingerprint)throw Error('The product knowledge pages belong to different collections.');
    const handles=new Set(),ids=new Set();for(const p of rows){if(handles.has(p.handle)||ids.has(p.id)||cycle.rows.has(p.handle)||cycle.ids.has(p.id))throw Error('Repeated product identity.');handles.add(p.handle);ids.add(p.id);}
    if(offset===0){cycle.fingerprint=fingerprint;cycle.total=info.total;cycle.previous=new Map(inventoryRows);if(!fingerprint||fingerprint!==inventoryFingerprint||inventoryState.total!==info.total){inventoryRows.clear();inventoryCoverageComplete=false;}inventoryFingerprint=fingerprint;inventorySourcePartial=false;inventoryPageFailure=false;}
    cycle.partial=cycle.partial||info.partial===true||info.sourcePartial===true;inventorySourcePartial=cycle.partial;inventoryState.total=info.total;
    const values=[];for(const p of rows){const prior=inventoryRows.get(p.handle);if(prior&&prior.id!==p.id)throw Error('The product identity changed.');const valid=p.detailState==='checked'&&cacheChecked(p,data.checkedAt);if(!valid)checkedInventory.delete(p.handle);const value=valid?checkedInventory.get(p.handle).product:provisional(p);inventoryRows.set(p.handle,value);cycle.rows.set(p.handle,value);cycle.ids.add(p.id);values.push(value);remember(value);}cycle.pages.set(offset,values);
    // The exact inventory itself can populate the starting collection if the
    // independent catalogue route fails; product knowledge must still open.
    if(!state.browse.length)inventoryBrowse=true;if(state.seedBrowseStarted&&state.browse.length>info.total)inventoryBrowse=false;
    if(inventoryBrowse){state.browse=[...inventoryRows.values()].map(p=>known.get(p.handle)||p);state.browsePageInfo={hasNextPage:false,endCursor:null};state.browseVerifiedAt=checkedTime(data.checkedAt)||Date.now();}else state.browse=state.browse.map(p=>known.get(p.handle)||p);state.products=inventoryBrowse&&state.pageKind==='catalogue'&&state.collectionSource==='browse'&&!state.search?state.browse.slice():state.products.map(p=>known.get(p.handle)||p);
    if(!state.loading&&state.pageKind==='product'&&state.current&&rows.some(p=>p.handle===state.current.handle)){const product=inventoryRows.get(state.current.handle);if(product&&product.id===state.current.id){
      const fields=['id','handle','title','type','description','url','currency','image','imageAlt','images','options','variants','variantsComplete','cartHold','recommendationHold','meaningHold','partsOnly','detailState'],same=fields.every(key=>JSON.stringify(state.current[key])===JSON.stringify(product[key]));state.verifiedAt=checkedTime(product.checkedAt)||state.verifiedAt;
      // A timestamp refresh keeps real focus, expanded menus, the image
      // dialog and any pending review. Handlers retain their exact product.
      if(same)Object.assign(state.current,product);
      else{const ui=currentProductUI(),initialFocus=focusedElement(),selection=productSelection(),focus=ui&&initialFocus===ui.select?'select':ui&&initialFocus===ui.quantityInput?'quantityInput':ui&&initialFocus===ui.engravingInput?'engravingInput':null;state.current=product;renderProduct(product,selection);if(focus&&!document.hidden&&!initialFocus.isConnected&&focusedElement()===document.body)productUI?.[focus]?.focus({preventScroll:true});}
    }}
    if(state.pageKind==='catalogue'&&main.querySelector('#demo-products'))drawGrid();refreshBagChoices();publishInventory();publish();return info.total;
  }
  function finishInventoryCycle(cycle){
    if(cycle.pages.size!==Math.ceil(cycle.total/24)||cycle.rows.size!==cycle.total)return false;
    const rows=[...cycle.pages].sort((a,b)=>a[0]-b[0]).flatMap(([,products])=>products);inventoryRows.clear();rows.forEach(p=>inventoryRows.set(p.handle,p));inventoryCoverageComplete=true;
    for(const [handle,p] of cycle.previous){if(!inventoryRows.has(handle)){if(checkedInventory.get(handle)?.product.id===p.id)checkedInventory.delete(handle);if(known.get(handle)?.id===p.id)known.delete(handle);}}
    if(inventoryBrowse||state.seedInfo&&!state.seedBrowseStarted){inventoryBrowse=true;state.browse=rows.slice();state.browsePageInfo={hasNextPage:false,endCursor:null};state.browseVerifiedAt=Date.now();if(state.pageKind==='catalogue'&&state.collectionSource==='browse'&&!state.search){state.products=rows.slice();state.pageInfo={...state.browsePageInfo};state.verifiedAt=state.browseVerifiedAt;}}
    if(state.pageKind==='catalogue'&&main.querySelector('#demo-products'))drawGrid();return true;
  }
  async function inventoryGet(offset){
    const controller=new AbortController();let timer;
    const expiry=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('The product knowledge read timed out.'));},32000);});
    try{return await Promise.race([get('/api/growth/inventory?offset='+offset+'&limit=24',controller.signal),expiry]);}finally{clearTimeout(timer);}
  }
  function preloadInventory({retry=false}={}){
    if(document.body.dataset.inventoryPreload!=='complete')return Promise.resolve(inventoryStatus());if(inventoryPending)return inventoryPending;if(inventoryStatus().ready&&!retry)return Promise.resolve(inventoryStatus());
    const version=++inventoryVersion;clearTimeout(inventoryRefreshTimer);inventoryState.loading=true;publishInventory();
    const cycle={rows:new Map(),ids:new Set(),pages:new Map(),previous:new Map(),total:0,fingerprint:null,partial:false,failed:false};
    inventoryPending=(async()=>{try{const first=await inventoryGet(0);if(version!==inventoryVersion)return inventoryStatus();const total=applyInventoryPage(first,0,null,cycle),offsets=Array.from({length:Math.ceil(total/24)-1},(_,i)=>(i+1)*24);const checks=await Promise.allSettled(offsets.map(async offset=>{const data=await inventoryGet(offset);if(version===inventoryVersion)applyInventoryPage(data,offset,total,cycle);}));cycle.failed=checks.some(check=>check.status==='rejected');if(version===inventoryVersion&&!finishInventoryCycle(cycle))cycle.failed=true;}catch{cycle.failed=true;}finally{if(version===inventoryVersion){inventoryPageFailure=cycle.failed;inventoryState.loading=false;publishInventory();publish();const ready=inventoryStatus().ready,fresh=[...inventoryRows.keys()].map(handle=>checkedInventory.get(handle)).filter(row=>row?.expiresAt>Date.now());let delay;if(ready){inventoryRetryCount=0;delay=fresh.length?Math.max(1000,Math.min(...fresh.map(row=>row.expiresAt))-Date.now()-75000):30000;}else{delay=Math.min(30000,2000*2**Math.min(inventoryRetryCount++,4));}inventoryRefreshTimer=setTimeout(()=>{publishInventory();void preloadInventory({retry:true});},delay);}inventoryPending=null;}return inventoryStatus();})();return inventoryPending;
  }
  function renderCoverage(){
    main.querySelector('.catalogue-coverage')?.remove();if(document.body.dataset.catalogueSeed!=='balanced'||!main.querySelector('#demo-products'))return;const panel=node('aside',null,'catalogue-coverage');panel.setAttribute('aria-label','Checked live catalogue coverage');const loaded=state.browse.length;panel.append(node('strong',loaded+' distinct live pieces checked'));
    if(state.seedInfo){panel.append(node('p',state.seedInfo.complete?'All five requested jewellery groups are represented in the checked starting collection.':'This is a partial starting collection; at least 100 distinct pieces and all five groups have not yet been confirmed.'));const counts=node('div',null,'coverage-counts');SEED_CATEGORIES.forEach(c=>counts.append(node('span',c.replace(/-/g,' ')+': '+state.seedInfo.categoryCounts[c])));panel.append(counts);if(state.seedInfo.partial)panel.append(node('p','Some public source checks were unavailable. These counts describe the checked pieces only.'));}
    else panel.append(node('p',state.seedFailure?'The balanced start could not be checked. This is the ordinary live browse collection; broader coverage is still being checked.':'Continue browsing for more published pieces.'));
    const progress=node('div',null,'catalogue-progress');progress.setAttribute('aria-label','Checked distinct pieces toward 100');const fill=node('span');fill.style.width=Math.min(100,loaded)+'%';progress.append(fill);panel.append(progress);const controls=main.querySelector('#collection-controls');if(controls)controls.after(panel);else main.prepend(panel);
  }
  function begin(options={}){
    saveCurrentView();cancelPending();request=new AbortController();const record={version:navigationVersion,controller:request};
    if(options.signal){if(options.signal.aborted)request.abort();else options.signal.addEventListener('abort',()=>{record.controller.abort();if(record.version===navigationVersion){state.loading=false;publish();status('The previous website action was cancelled.');}},{once:true});}
    state.loading=!record.controller.signal.aborted;publish();return record;
  }
  function current(record){return record.version===navigationVersion&&!record.controller.signal.aborted;}
  function commitPage(kind,handle,push=true,{preserveLoading=false}={}){
    state.pageKind=kind;state.currentHandle=kind==='product'?handle:'';state.focusedHandle='';if(!preserveLoading)state.loading=false;
    if(push){const query=kind==='product'?'?product='+encodeURIComponent(handle):kind==='bag'?'?cart=1':kind==='checkout'?'?checkout=1':'',key='view-'+(++historySequence);navigationKeys.splice(navigationIndex+1);navigationKeys.push(key);navigationIndex=navigationKeys.length-1;history.pushState({britesSandboxView:key},'','/concierge-sandbox.html'+query);}
    publish();document.dispatchEvent(new CustomEvent('brites-concierge:page'));
  }
  function validQuantity(value){return Number.isInteger(value)&&value>=1&&value<=20;}
  function validPrivateText(value){return typeof value==='string'&&value.length<=300&&!/[\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/.test(value);}
  function exactVariantOptions(v){return (Array.isArray(v?.options)?v.options:[]).slice(0,12).map(o=>({name:clean(o.name,100),value:clean(o.value,300)})).sort((a,b)=>a.name.localeCompare(b.name));}
  function variantChoiceSignature(v){return JSON.stringify([v?.id,v?.title,exactVariantOptions(v)]);}
  function cartItem(value){if(!value||!PRODUCT.test(value.productId||'')||!/^[1-9][0-9]{0,19}$/.test(value.variantId||'')||typeof value.title!=='string'||typeof value.variant!=='string'||!Number.isFinite(value.price)||value.price<0||!/^[A-Z]{3}$/.test(value.currency||'')||value.quantity!==undefined&&!validQuantity(value.quantity)||value.variantOptions!==undefined&&(!Array.isArray(value.variantOptions)||value.variantOptions.length>12||value.variantOptions.some(o=>!o||typeof o.name!=='string'||!o.name||o.name.length>100||typeof o.value!=='string'||!o.value||o.value.length>300))||value.engravingPreview!==undefined&&!validPrivateText(value.engravingPreview)||value.customizationPreview!==undefined&&typeof value.customizationPreview!=='boolean')return null;return {productId:value.productId,title:value.title.slice(0,300),variantId:value.variantId,variant:value.variant.slice(0,300),price:value.price,currency:value.currency,...(value.quantity!==undefined?{quantity:value.quantity}:{}),...(value.variantOptions!==undefined?{variantOptions:value.variantOptions.map(o=>({name:clean(o.name,100),value:clean(o.value,300)}))}:{}),...(value.engravingPreview!==undefined?{engravingPreview:value.engravingPreview}:{}),...(value.customizationPreview===true?{customizationPreview:true}:{})};}
  function readCart(){try{const saved=JSON.parse(sessionStorage.getItem('brites-sandbox-cart')||'[]');return Array.isArray(saved)?saved.slice(-50).map(cartItem).filter(Boolean):[];}catch{return [];}}
  function cartIdentitySignature(cart){return JSON.stringify(cart.map(item=>[item.productId,item.variantId,item.title,item.variant,item.quantity||1,item.price,item.currency]));}
  function saveCartIdentities(cart){try{sessionStorage.setItem('brites-sandbox-cart-lines',JSON.stringify({schema:1,signature:cartIdentitySignature(cart),ids:cartLines.ids}));}catch{}}
  function cartIdentities(cart){const fingerprint=JSON.stringify(cart);if(cartLines?.fingerprint!==fingerprint){let ids;try{const saved=JSON.parse(sessionStorage.getItem('brites-sandbox-cart-lines')||'null');if(saved?.schema===1&&saved.signature===cartIdentitySignature(cart)&&Array.isArray(saved.ids)&&saved.ids.length===cart.length&&new Set(saved.ids).size===saved.ids.length&&saved.ids.every(id=>/^test-[a-z0-9-]{1,100}$/.test(id)))ids=saved.ids;}catch{}cartLines={fingerprint,ids:ids||cart.map(()=>newLineId())};saveCartIdentities(cart);}return cartLines.ids.slice();}
  function newLineId(){let id;do{id='test-'+Date.now().toString(36)+'-'+(++lineSequence).toString(36);}while(cartLines?.ids.includes(id));return id;}
  function writeCart(cart,lineIds){const rows=cart.flatMap((value,index)=>{const item=cartItem(value);return item?[{item,id:lineIds?.[index]}]:[];}).slice(-50),kept=rows.map(row=>row.item);sessionStorage.setItem('brites-sandbox-cart',JSON.stringify(kept));cartLines={fingerprint:JSON.stringify(kept),ids:lineIds?.length===cart.length&&new Set(lineIds).size===lineIds.length&&rows.every(row=>/^test-[a-z0-9-]{1,100}$/.test(row.id||''))?rows.map(row=>row.id):kept.map(()=>newLineId())};saveCartIdentities(kept);for(const id of bagMenus.keys())if(!cartLines.ids.includes(id))bagMenus.delete(id);saveBagMenus();document.dispatchEvent(new CustomEvent('brites:cart-updated',{detail:{sandbox:true}}));}
  function updateBag(){const count=document.querySelector('#bag-count');if(count)count.textContent=String(readCart().reduce((sum,item)=>sum+(item.quantity||1),0));}
  function link(label,url){const a=node('a',label);a.href=url;a.target='_blank';a.rel='noopener noreferrer';return a;}
  function needsCustomizer(p,v){
    const ordinaryCharm=/^charms?(?:[ -]only)?$/i.test(clean(p.type,100))&&!/\b(?:custom|personalized|personalised|engraving|design fee|chain extender|components?|add[ -]?ons?)\b/i.test(p.title+' '+p.type);
    if(!v||p.cartHold||p.recommendationHold||p.partsOnly&&!ordinaryCharm||/\bcharms?\b/i.test(p.type||'')&&!ordinaryCharm||/\b(?:components?|add[ -]?ons?|custom[ -]?only)\b/i.test(p.type||''))return true;
    const opts=(v.options||[]).filter(o=>/engrav|personali[sz]|custom/i.test(o.name)),detail=v.title+' '+opts.map(o=>o.value).join(' ');
    if(/handwrit|monogram|photo|upload|custom studio/i.test(p.title))return true;
    if(/no engraving|not engraved|without engraving|non[ -]?engraved|unengraved/i.test(detail)||opts.some(o=>/^(?:no|none|without|not included)$/i.test(o.value)))return false;
    return /engrav|personali[sz]|custom/i.test(detail)||opts.some(o=>/^(?:yes|included|with)$/i.test(o.value));
  }
  function noEngraving(value){return /^(?:no|none|without|not included|no engraving|without engraving|not engraved|unengraved)$/i.test(value);}
  function engravingPreviewVariant(p,v){
    if(!p||!v||p.cartHold||p.recommendationHold||p.partsOnly||p.requiresSellingPlan||v.requiresSellingPlan||!p.variantsComplete||/handwrit|monogram|photo|upload|custom studio|custom[ -]?only|components?|add[ -]?ons?/i.test(p.title+' '+p.type)||/\b(?:custom|personalized|personalised|engraving|design fee)\b/i.test(p.title))return false;
    const options=(v.options||[]).filter(o=>/engrav|personali[sz]|custom/i.test(o.name));
    return options.length===1&&/^engraving(?: option)?$/i.test(options[0].name)&&/^(?:yes|engraved|with engraving|included)$/i.test(options[0].value);
  }
  function previewableVariant(p,v){return !!v&&(!needsCustomizer(p,v)||engravingPreviewVariant(p,v));}
  function savedVariant(p,item){const matches=p?.variants?.filter(v=>v.id==='gid://shopify/ProductVariant/'+item.variantId)||[],v=matches.length===1?matches[0]:null;return p?.id===item.productId&&p.variantsComplete&&v&&v.title===item.variant&&item.variantOptions!==undefined&&JSON.stringify(exactVariantOptions(v))===JSON.stringify(exactVariantOptions({options:item.variantOptions}))&&p.currency===item.currency&&Math.abs(v.price-item.price)<.005?v:null;}
  function bagProductControls(item){const handle=identities.get(item.productId),p=handle&&known.get(handle),v=savedVariant(p,item);return v&&previewableVariant(p,v)?{p,v,groups:exactOptionGroups(p)}:null;}
  function controls(){
    const box=main.querySelector('#collection-controls');if(!box)return;
    // Replacing this form discards keyboard focus as soon as a search starts.
    // Keep our live controls; update their values without moving user focus.
    if(collectionControls?.box===box&&box.contains(collectionControls.input)&&box.contains(collectionControls.sort)){
      if(collectionControls.input.value!==state.search)collectionControls.input.value=state.search;
      if(collectionControls.sort.value!==state.sort)collectionControls.sort.value=state.sort;
      return;
    }
    box.replaceChildren();const form=node('form',null,'collection-tools'),wrap=node('div',null,'search-wrap');
    wrap.innerHTML='<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true"><circle cx="10.5" cy="10.5" r="6.5" fill="none" stroke="currentColor" stroke-width="1.3"/><path d="m15.5 15.5 5 5" fill="none" stroke="currentColor" stroke-width="1.3"/></svg>';
    const input=document.createElement('input');input.id='store-search';input.type='search';input.maxLength=250;input.placeholder='Search a symbol, piece or material…';input.setAttribute('aria-label','Search the live jewelry collection');input.value=state.search;
    input.addEventListener('input',()=>{const resetScope=state.search!==''||state.filter!=='all'||state.collectionSource!=='browse';cancelPending();state.search=clean(input.value,250);if(!state.search){state.filter='all';if(state.browseVerifiedAt){restoreBrowse();if(resetScope)reviseDiscovery();}}state.limit=PAGE_SIZE;drawGrid();publish();});
    const submit=button('Search');submit.type='submit';wrap.append(input,submit);form.append(wrap);
    const sortLabel=node('label','Sort','tool-select tool-sort'),sort=document.createElement('select');sort.id='store-sort';sort.setAttribute('aria-label','Sort jewelry');
    [['featured','Shop order'],['price-asc','Price: low to high'],['price-desc','Price: high to low'],['title-asc','Name: A–Z'],['title-desc','Name: Z–A']].forEach(([value,label])=>{const o=node('option',label);o.value=value;sort.append(o);});sort.value=state.sort;sort.addEventListener('change',()=>void execute({type:'sort',sort:sort.value}));sortLabel.append(sort);form.append(sortLabel);box.append(form);
    form.addEventListener('submit',e=>{e.preventDefault();void execute({type:'search',query:input.value});});
    const chips=node('div',null,'category-chips');chips.setAttribute('aria-label','Jewelry categories');
    [['all','All pieces'],['regular-necklaces','Regular necklaces'],['beady-necklaces','Beady necklaces'],['stud-earrings','Stud earrings'],['hoop-earrings','Hoop earrings'],['charm-only','Charm-only'],['necklaces','All necklaces'],['earrings','All earrings'],['bracelets','Bracelets'],['rings','Rings'],['charms','Charms'],['available','Available now']].forEach(([value,label])=>{const b=button(label,'',()=>void execute({type:'filter',filter:value}));b.dataset.filter=value;b.setAttribute('aria-pressed',String(state.filter===value));chips.append(b);});box.append(chips);
    collectionControls={box,input,sort};
  }
  function drawGrid(){
    const grid=main.querySelector('#demo-products');if(!grid)return;
    // A replaced card is no longer attended, even when a stationary pointer
    // happens to occupy the new card's position. Wait for real new attention.
    clearTimeout(focusTimer);focusTimer=0;state.focusedHandle='';
    grid.replaceChildren();const pieces=filtered(),shown=pieces.slice(0,state.limit);
    shown.forEach(p=>{
      const card=node('article',null,'piece-card card-enter');card.dataset.productHandle=p.handle;card.dataset.productId=p.id;
      const a=document.createElement('a');a.href='/concierge-sandbox.html?product='+encodeURIComponent(p.handle);a.setAttribute('aria-label','View '+p.title);
      const photo=node('div',null,'piece-photo'),img=picture(p);photo.append(img||node('span','b.','piece-placeholder'));if(p.type)photo.append(node('span',p.type,'piece-type'));
      const copy=node('div',null,'piece-copy');copy.append(node('h3',p.title));const amount=minimum(p),unconfirmed=p.detailState==='unconfirmed'||p.variants.some(v=>v.availabilityKnown===false),criteria=activeSearchPlan();copy.append(node('p',amount==null?unconfirmed?'Price is being checked':'Currently unavailable':'From '+money(amount,p.currency),'piece-price'));if(criteria?.materialLabel)copy.append(node('p',criteria.materialLabel+' options','piece-meta'));if(unconfirmed)copy.append(node('p','Availability checked when you open this piece','piece-meta'));const opts=(p.options||[]).map(o=>o.name).filter(Boolean).slice(0,2);if(opts.length)copy.append(node('p',opts.join(' · '),'piece-meta'));a.append(photo,copy);card.append(a);grid.append(card);
    });
    if(!shown.length){const empty=node('div',null,'empty-collection');empty.append(node('h3','A little more room to explore'),node('p',state.loading?'Checking the live shop for your search…':'No loaded pieces match those choices. Search the live shop, try another symbol, or clear the filters.'),button('Show all pieces','secondary',()=>void execute({type:'search',query:'',filter:'all'})));grid.append(empty);}
    const summary=main.querySelector('#result-summary');if(summary)summary.textContent=(state.loading?'Checking the live shop · ':'')+shown.length+' of '+pieces.length+' loaded pieces'+(state.search?' matching “'+state.search+'”':'')+(state.pageInfo.hasNextPage&&!state.search?' · more collection pages available':'')+(state.filter!=='all'?' · '+state.filter:'');
    const more=main.querySelector('#collection-more');if(more){more.replaceChildren();if(pieces.length>state.limit||!state.search&&(state.pageInfo.hasNextPage||state.seedInfo&&!state.seedBrowseStarted&&state.browse.length<MAX_PIECES)){const next=actionButton('Explore more pieces','secondary',options=>loadMore(options));next.dataset.catalogueMore='true';next.disabled=state.loading;more.append(next,node('p','Continue through the public Brites catalogue.'));}}
    main.querySelectorAll('[data-filter]').forEach(e=>e.setAttribute('aria-pressed',String(e.dataset.filter===state.filter)));
    const hero=main.querySelector('#intro-photo');if(hero&&!hero.querySelector('img')&&shown[0]){const img=picture(shown[0]);if(img){img.loading='eager';hero.replaceChildren(img);}}
    renderCoverage();
  }
  function restoreCollection(){
    retireImage();
    if(state.pageKind!=='catalogue'||!main.querySelector('#demo-products'))main.replaceChildren(...[...home.childNodes].map(n=>n.cloneNode(true)));
    state.pageKind='catalogue';state.currentHandle='';state.focusedHandle='';state.current=null;state.activeSection='catalogue';controls();drawGrid();if(full)renderServiceStrip();
  }
  function restoreBrowse(){
    state.products=state.browse.slice();state.search='';state.checkedSearch='';state.collectionSource='browse';state.pageInfo={...state.browsePageInfo};state.verifiedAt=state.browseVerifiedAt;
  }
  async function searchCatalogue(query,options={},action={},newDiscovery=true){
    if(typeof query!=='string'||query.length>250)return {ok:false,action:'search',message:'Please use a shorter jewelry search.'};
    if(action.sort&&!SORTS.has(action.sort)||action.filter&&!FILTERS.has(action.filter))return {ok:false,action:'search',message:'Those collection controls are not available.'};
    const localSearch=clean(query,250);if(!localSearch&&state.browseVerifiedAt||localSearch&&inventoryStatus().ready){
      const leavingPage=state.pageKind!=='catalogue';saveCurrentView();cancelPending();if(!localSearch)restoreBrowse();else{state.products=[...inventoryRows.keys()].map(handle=>checkedInventory.get(handle)?.product).filter(Boolean);state.pageInfo={hasNextPage:false,endCursor:null};state.verifiedAt=Math.min(...state.products.map(p=>p.checkedAt));}state.search=state.checkedSearch=localSearch;state.collectionSource=localSearch?'search':'browse';state.sort=action.sort||state.sort;state.filter=action.filter||'all';state.limit=PAGE_SIZE;if(newDiscovery)reviseDiscovery();restoreCollection();if(leavingPage)commitPage('catalogue','',options.push!==false);else publish();if(!document.hidden)focusSection('catalogue',false,{immediate:true});const products=filtered().slice(0,state.limit).map(projection),message=products.length?'Your loaded product selection is ready.':'No matching pieces in this test inventory. Try another symbol or style.';status(message);return {ok:true,action:'search',live:true,cached:true,checkedAt:state.verifiedAt,products,snapshot:snapshot(),message};
    }
    const initialFocus=focusedElement(),leavingPage=state.pageKind!=='catalogue',priorCollection={search:state.search,checkedSearch:state.checkedSearch,collectionSource:state.collectionSource,sort:state.sort,filter:state.filter,limit:state.limit},record=begin(options),nextSearch=clean(query,250);state.search=nextSearch;state.checkedSearch=null;state.collectionSource=nextSearch?'search':'browse';if(action.sort)state.sort=action.sort;state.filter=action.filter||'all';state.limit=PAGE_SIZE;if(!leavingPage)restoreCollection();publish();status(state.search?'Finding “'+state.search+'” in the live shop…':'Opening the wider live collection…');
    try{
      const data=nextSearch?await get('/api/growth/catalogue?q='+encodeURIComponent(nextSearch),record.controller.signal):await catalogueStart(record.controller.signal);
      if(!current(record))return {ok:false,action:'search',message:'The earlier search was cancelled.'};
      const pageInfo=checkedPageInfo(data);state.products=checkedProducts(data);state.pageInfo=pageInfo;state.checkedSearch=nextSearch;state.verifiedAt=Date.now();
      if(!nextSearch){state.browse=state.products.slice();state.browsePageInfo={...pageInfo};state.browseVerifiedAt=state.verifiedAt;applySeed(data);}if(newDiscovery)reviseDiscovery();state.loading=false;if(leavingPage){restoreCollection();commitPage('catalogue','',options.push!==false);}else{drawGrid();publish();}if(mayRevealCollection(initialFocus))focusSection('catalogue',false);status(filtered().length?'Your checked pieces are ready.':'No checked matches yet. Try another symbol or style.');
      return {ok:true,action:'search',live:data.live!==false,checkedAt:state.verifiedAt,products:filtered().slice(0,state.limit).map(projection),snapshot:snapshot(),message:filtered().length?'Your checked search results are ready on the page.':'I couldn’t find a name or symbol match in these checked results. Try another symbol or show all pieces.'};
    }catch{if(current(record)){state.loading=false;if(leavingPage)Object.assign(state,priorCollection);else drawGrid();publish();status('The live selection is temporarily unavailable. Your existing view is preserved.');}return {ok:false,action:'search',message:'The live selection could not be checked. Please try again.'};}
  }
  async function loadMore(options={}){
    if(options.signal?.aborted||state.pageKind!=='catalogue'||state.loading)return {ok:false,message:'The current collection continuation is unavailable or cancelled.'};
    const available=filtered();if(available.length>state.limit){state.limit=Math.min(MAX_PIECES,state.limit+PAGE_SIZE);drawGrid();publish();return {ok:true,message:'More checked loaded pieces are in view.'};}
    const startBrowse=!!state.seedInfo&&!state.seedBrowseStarted;if((!startBrowse&&(!state.pageInfo.hasNextPage||!state.pageInfo.endCursor))||state.search||state.browse.length>=MAX_PIECES)return {ok:false,message:'There is no checked collection continuation on this view.'};
    const cursor=startBrowse?null:state.pageInfo.endCursor,record=begin(options);status('Opening the next collection page…');
    try{const data=await get('/api/growth/catalogue?browse=1'+(cursor?'&cursor='+encodeURIComponent(cursor):''),record.controller.signal);if(!current(record))return {ok:false,cancelled:true,message:'The collection continuation was cancelled.'};if(data.live===false||!Array.isArray(data.products)||data.products.length>250)throw Error('The next live collection page could not be checked.');const pageInfo=checkedPageInfo(data,cursor),additional=checkedProducts(data),ids=new Set(state.browse.map(p=>p.id)),handles=new Set(state.browse.map(p=>p.handle)),fresh=additional.filter(p=>!ids.has(p.id)&&!handles.has(p.handle));state.seedBrowseStarted=true;state.browse=state.browse.concat(fresh).slice(0,MAX_PIECES);state.products=state.browse.slice();state.collectionSource='browse';state.checkedSearch='';state.limit=Math.min(MAX_PIECES,state.limit+PAGE_SIZE);state.pageInfo=pageInfo;state.browsePageInfo={...pageInfo};state.loading=false;state.verifiedAt=Date.now();state.browseVerifiedAt=state.verifiedAt;drawGrid();publish();status(fresh.length?'More live pieces are ready.':pageInfo.hasNextPage?'This page has no additional matching pieces. Continue to the next public collection page.':'You have reached the end of the checked public collection.');return {ok:true,message:notice.textContent};}catch{if(current(record)){state.loading=false;publish();status('That collection page could not be checked. Try Explore more pieces again.');}return {ok:false,message:'The collection continuation could not be checked.'};}
  }
  async function openProduct(handle,{push=true,signal,section,focusFrom=null,revealFrom=null,revealRequested=false}={}){
    if(!validHandle(handle))return false;
    const initialFocus=focusedElement(),focusRoot=initialFocus?.getRootNode();let focusIntent=!!focusFrom&&focusFrom.isConnected&&document.activeElement===focusFrom,revealIntent=revealRequested===true||!!revealFrom&&revealFrom.isConnected&&main.contains(revealFrom);
    const movedFocus=event=>{if(event.target!==focusFrom)focusIntent=false;if(!sameRevealFocus(initialFocus))revealIntent=false;};if(focusIntent||revealIntent){document.addEventListener('focusin',movedFocus);if(focusRoot!==document){focusRoot?.addEventListener('focusin',movedFocus);focusRoot?.addEventListener('focusout',movedFocus);}}
    const selection=state.currentHandle===handle?productSelection():productViews.get(handle)||null,record=begin({signal}),cached=cachedRead(handle);status(cached?'Opening your loaded product details…':'Opening the product details…');
    try{
      const data=cached||await get('/api/growth/product?handle='+encodeURIComponent(handle),record.controller.signal),p=data.product;
      if(!current(record)||data.live===false||!validProduct(p,handle)){if(current(record)){state.loading=false;publish();}return false;}
      const focusHeading=focusIntent&&!document.hidden&&document.activeElement===focusFrom,revealProduct=revealIntent&&!section&&!document.hidden&&(revealRequested===true||revealFrom?.isConnected)&&sameRevealFocus(initialFocus);cacheChecked(p,data.checkedAt||Date.now());state.current=remember(p);state.selectedImage=selection?.selectedImage||0;state.verifiedAt=checkedTime(p.checkedAt)||checkedTime(data.checkedAt)||Date.now();state.activeSection=section||'details';renderProduct(state.current,selection);commitPage('product',handle,push);if(section&&!document.hidden&&revealIntent&&sameRevealFocus(initialFocus))focusSection(section,true,{immediate:true});
      if(focusHeading){const heading=main.querySelector('.product-copy h1');if(heading){heading.tabIndex=-1;heading.focus({preventScroll:true});try{heading.scrollIntoView?.({behavior:reduced()?'auto':'instant',block:'start'});}catch{}}}
      // A pointer or explicitly requested piece starts at its checked image and title. Reveal
      // the layout without giving it keyboard focus or overriding a section.
      // Complete a requested arrival before returning success. Smooth scrolling
      // can still be mid-transition when the guide reports that details are open.
      if(revealProduct&&revealIntent&&!document.hidden&&current(record)&&state.pageKind==='product'&&state.currentHandle===handle&&state.current?.id===p.id&&sameRevealFocus(initialFocus)){const layout=main.querySelector('.product-layout');try{layout?.scrollIntoView?.({behavior:reduced()?'auto':'instant',block:'start'});}catch{}}
      status(cached?'Your loaded listing and published options are ready.':'Live options checked. Your guide stays with you.');return true;
    }catch{if(current(record)){state.loading=false;publish();status('This piece could not be checked. Your current page is preserved.');}return false;}
    finally{document.removeEventListener('focusin',movedFocus);if(focusRoot!==document){focusRoot?.removeEventListener('focusin',movedFocus);focusRoot?.removeEventListener('focusout',movedFocus);}}
  }
  function productImages(p){
    const images=(Array.isArray(p.images)?p.images:[]).slice(0,50).flatMap(i=>{const image=typeof i==='string'?i:i?.url,src=safeImage(image);return src?[{image:src,imageAlt:clean(typeof i==='object'?i.altText||p.title:p.title,300)}]:[];});
    const first=safeImage(p.image);if(first&&!images.some(i=>i.image===first))images.unshift({image:first,imageAlt:clean(p.imageAlt||p.title,300)});return images.slice(0,50);
  }
  async function returnToCollection(event){
    const origin=event.currentTarget,keyboard=event.detail===0&&origin?.isConnected&&document.activeElement===origin&&!document.hidden;
    let focusIntent=keyboard;const movedFocus=next=>{if(next.target!==origin)focusIntent=false;};if(keyboard)document.addEventListener('focusin',movedFocus);
    try{
      saveCurrentView();const view=collectionView;let ok;if(view){ok=await restoreSavedView(view,{save:false});if(ok)commitPage('catalogue','',true);}else ok=(await execute({type:'search',query:''})).ok;
      if(!focusIntent||document.hidden||!ok||state.loading||state.pageKind!=='catalogue'||document.activeElement!==document.body)return;
      const heading=main.querySelector('.collection-heading h2');if(heading){heading.tabIndex=-1;heading.focus({preventScroll:true});try{heading.scrollIntoView?.({behavior:reduced()?'auto':'smooth',block:'start'});}catch{}}
    }finally{if(keyboard)document.removeEventListener('focusin',movedFocus);}
  }
  function exactOptionGroups(p){
    const groups=new Map(),append=(name,value)=>{name=clean(name,100);value=clean(value,300);if(!name||!value)return;const key=name.toLowerCase();if(!groups.has(key)&&groups.size<12)groups.set(key,{name,values:[]});const group=groups.get(key);if(group&&group.values.length<250&&!group.values.includes(value))group.values.push(value);};
    (Array.isArray(p.options)?p.options:[]).slice(0,12).forEach(o=>(Array.isArray(o?.values)?o.values:[]).forEach(value=>append(o.name,value)));
    p.variants.forEach(v=>(Array.isArray(v.options)?v.options:[]).slice(0,12).forEach(o=>append(o.name,o.value)));return [...groups.values()];
  }
  function renderProduct(p,selection=null){
    retireImage();
    main.replaceChildren();main.append(button('← Back to the collection','back-link',returnToCollection));
    const layout=node('div',null,'product-layout view-enter'),gallery=node('section',null,'product-gallery'),images=productImages(p);gallery.dataset.storeSection='image';
    state.selectedImage=Math.min(Math.max(0,Number.isInteger(selection?.selectedImage)?selection.selectedImage:state.selectedImage),Math.max(0,images.length-1));const enlarge=button('','product-image-button',()=>zoomImage(p));enlarge.setAttribute('aria-label','Enlarge image of '+p.title);const photo=images[state.selectedImage]?picture(images[state.selectedImage]):null;enlarge.append(photo||node('span','b.','piece-placeholder'));if(images.length)enlarge.append(node('span','Enlarge ↗','image-zoom-label'));else enlarge.disabled=true;gallery.append(enlarge);
    const thumbs=node('div',null,'image-thumbs');function selectImage(index){if(!Number.isInteger(index)||index<1||index>images.length)return false;const before=productSelection();state.selectedImage=index-1;const img=picture(images[state.selectedImage]);if(!img)return false;img.loading='eager';enlarge.replaceChildren(img,node('span','Enlarge ↗','image-zoom-label'));thumbs.querySelectorAll('button').forEach((b,j)=>b.setAttribute('aria-pressed',String(j===state.selectedImage)));syncImageDialog(p);recordProductChange('gallery',before);publish();return true;}
    if(images.length>1){images.forEach((value,index)=>{const b=button('','',()=>selectImage(index+1));b.setAttribute('aria-label','View product image '+(index+1));b.setAttribute('aria-pressed',String(index===state.selectedImage));const img=picture(value);if(img)b.append(img);thumbs.append(b);});gallery.append(thumbs);}
    const copy=node('section',null,'product-copy'),heading=node('h1',p.title),description=node('p',p.description.slice(0,12000),'product-description');heading.dataset.storeSection='title';description.dataset.storeSection='description';copy.dataset.productHandle=p.handle;copy.dataset.productId=p.id;copy.append(node('span',p.type||'A PERSONAL BRITES PIECE','eyebrow'),heading);
    const pendingAvailability=p.detailState==='unconfirmed'||p.variants.some(v=>v.availabilityKnown===false),price=node('p',minimum(p)==null?pendingAvailability?'Price is being checked':'Currently unavailable':'From '+money(minimum(p),p.currency),'product-price');price.dataset.storeSection='price';copy.append(price,node('p',pendingAvailability?'Published options shown · availability is still being checked':p.variants.some(v=>v.available)?'Live options and availability checked for this view':'This piece is currently unavailable','availability'),description);
    const options=node('section',null,'option-area');options.dataset.storeSection='options';const label=node('label','Choose your exact piece');label.htmlFor='piece-variant';const select=document.createElement('select');select.id='piece-variant';select.setAttribute('aria-label','Choose an exact product option');select.append(node('option','Select an option…'));select.firstChild.value='';
    p.variants.forEach(v=>{const o=node('option',v.title+' · '+money(v.price,p.currency)+(v.availabilityKnown===false?' · availability pending':v.available?'':' · unavailable'));o.value=v.id;o.disabled=!v.available||v.availabilityKnown===false;select.append(o);});
    const selected=node('dl',null,'exact-options'),selectionHelp=node('p',null,'selection-help'),add=button('Choose an option to add','primary');add.disabled=true;let adding=false,review=null,committedSelection=null,addController=null,selectionMemo=null;const choices=new Map(),groups=exactOptionGroups(p),menu=node('div',null,'option-menu');menu.hidden=true;selectionHelp.setAttribute('role','status');selectionHelp.setAttribute('aria-live','polite');selectionHelp.setAttribute('aria-atomic','true');
    const menuTrigger=button('Explore the published option menus','secondary option-menu-trigger',()=>menu.hidden?openOptions():closeOptions());menuTrigger.setAttribute('aria-expanded','false');const groupViews=new Map();
    groups.forEach(group=>{const row=node('section',null,'option-group');row.dataset.optionName=group.name;if(/metal|material/i.test(group.name))row.dataset.storeSection='materials';else if(/length/i.test(group.name))row.dataset.storeSection='length';else if(/engrav|personali[sz]/i.test(group.name))row.dataset.storeSection='engraving';row.append(node('h3',group.name,'option-title'));group.values.forEach(value=>{const choice=button(value,'option-choice',()=>{chooseOption(group.name,value);publish();});choice.dataset.optionValue=value;choice.setAttribute('aria-pressed','false');row.append(choice);});menu.append(row);groupViews.set(group.name,row);});
    const quantityLabel=node('label','Quantity','product-quantity'),quantityInput=document.createElement('input');quantityInput.type='number';quantityInput.min='1';quantityInput.max='20';quantityInput.step='1';quantityInput.value='1';quantityInput.setAttribute('aria-label','Quantity of this exact piece');quantityLabel.append(quantityInput);
    const quantity=()=>Number(quantityInput.value),clearReview=()=>{review?.remove();review=null;if(productUI?.product===p)productUI.review=null;};
    function exactVariant(){const matches=p.variants.filter(v=>v.id===select.value);return matches.length===1?matches[0]:null;}
    function matchesChoice(v,name,value){return (v.options||[]).some(o=>String(o.name).toLowerCase()===name.toLowerCase()&&o.value===value);}
    function availableChoices(group){const available=p.variants.filter(v=>v.available&&v.availabilityKnown!==false&&(Array.isArray(v.options)?v.options:[]).length<=12);return group.values.filter(value=>available.some(v=>matchesChoice(v,group.name,value)&&[...choices].every(([name,chosen])=>name===group.name||matchesChoice(v,name,chosen))));}
    function selectionState(){
      const key=JSON.stringify([select.value,quantity(),[...choices],p.variantsComplete,p.cartHold,p.recommendationHold,p.detailState]),copy=value=>({...value,requiredOptions:value.requiredOptions.slice(),requiredOptionGroups:value.requiredOptionGroups.map(g=>({name:g.name,values:g.values.slice()}))});if(selectionMemo?.key===key)return copy(selectionMemo.value);
      const v=exactVariant(),held=!!(p.cartHold||p.recommendationHold)||!!v&&!previewableVariant(p,v),unavailable=p.detailState==='unconfirmed'||!p.variantsComplete||!p.variants.some(v=>v.available&&v.availabilityKnown!==false),missing=groups.filter(g=>!choices.has(g.name));
      let required=missing.map(g=>({name:g.name,values:availableChoices(g)}));
      if(!v&&!missing.length&&groups.length)required=groups.filter(g=>availableChoices(g).some(value=>value!==choices.get(g.name))).map(g=>({name:g.name,values:availableChoices(g)}));
      const selectionStatus=held?'held':unavailable?'unavailable':v?.available&&v.availabilityKnown!==false&&validQuantity(quantity())?'ready':!missing.length?'unavailable':'choosing';
      const value={selectionStatus,requiredOptions:required.map(g=>g.name),requiredOptionGroups:required,nextOptionName:required[0]?.name||null};selectionMemo={key,value};return copy(value);
    }
    function selectionMessage(){const selection=selectionState(),v=exactVariant();if(adding)return 'Checking your exact choice before adding it…';if(selection.selectionStatus==='held')return 'This choice needs the shop’s customizer or a current shop review. Your selections are preserved.';if(selection.selectionStatus==='ready')return v.title+' · quantity '+quantity()+' · '+money(Math.round(v.price*quantity()*100)/100,p.currency)+' item subtotal. '+(engravingPreviewVariant(p,v)?'Ready for your local engraving preview; the shop must review customization before checkout.':'Ready when you are.');if(p.detailState==='unconfirmed'||!p.variantsComplete)return 'The complete available choices still need a live check. Your guide can help you compare the published options.';if(v&&(!v.available||v.availabilityKnown===false))return 'Your exact chosen option is currently unavailable. Your selections are preserved; choose another published option or ask for similar pieces.';if(!p.variants.some(v=>v.available&&v.availabilityKnown!==false))return 'This piece is currently unavailable. Your guide can help find a similar piece.';const next=selection.requiredOptionGroups[0];if(next)return (selection.selectionStatus==='choosing'?'Next, choose ':'That combination is unavailable. Adjust ')+next.name+(next.values.length?' — '+next.values.slice(0,4).join(' or ')+'.':'. Your current choices do not match an available combination.');return 'That selection does not resolve one available exact piece. Choose a published combination from the exact option menu.';}
    function updateSelection(syncChoices=true){const v=exactVariant();if(syncChoices){choices.clear();(v?.options||[]).forEach(o=>{const group=groups.find(g=>g.name.toLowerCase()===String(o.name).toLowerCase());if(group?.values.includes(o.value))choices.set(group.name,o.value);});}selected.replaceChildren();const shown=v?.options||[...choices].map(([name,value])=>({name,value}));shown.slice(0,12).forEach(o=>{const line=node('div');line.append(node('dt',clean(o.name,100)+':'),node('dd',clean(o.value,200)));selected.append(line);});groupViews.forEach((row,name)=>row.querySelectorAll('.option-choice').forEach(b=>{b.setAttribute('aria-pressed',String(choices.get(name)===b.dataset.optionValue));const available=p.variants.some(v=>v.available&&v.availabilityKnown!==false&&matchesChoice(v,name,b.dataset.optionValue));b.disabled=adding||!available;b.title=available?'':'This published value is currently unavailable.';}));price.textContent=v?money(v.price,p.currency):minimum(p)==null?pendingAvailability?'Price is being checked':'Currently unavailable':'From '+money(minimum(p),p.currency);select.disabled=quantityInput.disabled=adding;if(engravingInput)engravingInput.disabled=adding;add.disabled=adding||selectionState().selectionStatus!=='ready';add.textContent=adding?'Checking this exact option…':v&&engravingPreviewVariant(p,v)?'Add engraving preview to test bag':v&&needsCustomizer(p,v)?'Customize with the shop':v?'Add this exact option to test bag':'Choose an option to add';selectionHelp.textContent=selectionMessage();committedSelection={variantId:select.value,quantity:quantity(),choices:[...choices].map(([name,value])=>({name,value})),selectedImage:state.selectedImage};}
    function openOptions(name,reveal=true){if(name!==undefined){const matches=groups.filter(g=>g.name.toLowerCase()===String(name).toLowerCase());if(matches.length!==1)return false;name=matches[0].name;}menu.hidden=false;menuTrigger.setAttribute('aria-expanded','true');groupViews.forEach((row,key)=>{row.hidden=!!name&&key!==name;});if(productUI?.product===p)productUI.openedOption=name||null;if(reveal&&!document.hidden)focusSection('options',true,{immediate:true});publish();return true;}
    function closeOptions(){menu.hidden=true;menuTrigger.setAttribute('aria-expanded','false');if(productUI?.product===p)productUI.openedOption=null;publish();return true;}
    function revealSelection(name){if(document.hidden||restoringHistory||restoringChange)return;if(name==='quantity'){try{if(!revealAboveGuide(quantityInput))quantityInput.scrollIntoView?.({behavior:'instant',block:'center'});}catch{}return;}openOptions(name,false);const row=name?groupViews.get(name):menu,target=row?.querySelector('.option-choice[aria-pressed=true]')||row||selected;target.classList.add('choice-changed');setTimeout(()=>target.classList.remove('choice-changed'),800);try{if(!revealAboveGuide(row||target))target.scrollIntoView?.({behavior:'instant',block:'center'});}catch{}state.activeSection='options';}
    function chooseOption(name,value){if(adding)return false;const matches=groups.filter(g=>g.name.toLowerCase()===String(name).toLowerCase());if(matches.length!==1)return false;const group=matches[0],values=group.values.filter(v=>v.toLowerCase()===String(value).toLowerCase());if(values.length!==1||!p.variants.some(v=>v.available&&v.availabilityKnown!==false&&matchesChoice(v,group.name,values[0])))return false;const before=productSelection(),guiding=!menu.hidden;clearReview();choices.set(group.name,values[0]);select.value='';if(groups.every(g=>choices.has(g.name))){const candidates=p.variants.filter(v=>groups.every(g=>matchesChoice(v,g.name,choices.get(g.name))));if(candidates.length===1&&candidates[0].available&&candidates[0].availabilityKnown!==false)select.value=candidates[0].id;}updateSelection(false);const next=selectionState().nextOptionName;if(before){revealSelection(guiding&&next?next:group.name);recordProductChange('select-option',before);}status(selectionMessage());return true;}
    function chooseVariant(id){if(adding||typeof id!=='string'||!VARIANT.test(id)||p.variants.filter(v=>v.id===id).length!==1||!p.variants.some(v=>v.id===id&&v.available&&v.availabilityKnown!==false))return false;const before=productSelection();clearReview();select.value=id;updateSelection();if(before){revealSelection();recordProductChange('select-option',before);}return true;}
    function prepareReview(){const v=exactVariant(),q=quantity();if(adding||selectionState().selectionStatus!=='ready')return false;clearReview();review=node('section',null,'product-review review');review.append(node('h3','Review your exact test-bag choice'),node('p',p.title),node('p',v.title+' · quantity '+q),node('p',money(Math.round(v.price*q*100)/100,p.currency)+' item subtotal'),node('p','We will check this exact option again after you confirm. No real order is placed.'));const confirm=button('Confirm add to test bag','primary',()=>{if(!review?.isConnected||select.value!==v.id||quantity()!==q||!currentProductUI())return;add.click();}),cancel=actionButton('Cancel this review','secondary',()=>{clearReview();publish();return {ok:true,message:'The visible add review is cancelled. Your selections and bag are preserved.'};});cancel.dataset.cancelReview='true';review.append(confirm,cancel);options.append(review);if(productUI?.product===p)productUI.review=review;if(!document.hidden){try{review.scrollIntoView?.({behavior:reduced()?'auto':'smooth',block:'center'});}catch{}}return true;}
    select.addEventListener('change',()=>{const before=committedSelection;clearReview();updateSelection();recordProductChange('select-option',before);publish();});quantityInput.addEventListener('change',()=>{const before=committedSelection;clearReview();updateSelection(false);recordProductChange('product-quantity',before);publish();});
    async function addExact({signal,requestId}={}){
      const v=exactVariant(),q=quantity(),selection=selectionState(),engravingWords=engravingDrafts.get(p.id+'|'+p.handle)||'';
      if(!validQuantity(q))return controlResult('add',false,'Choose a quantity from 1 to 20 for this exact piece.',{cartChanged:false});
      if(selection.selectionStatus!=='ready'||!v){openOptions(selection.nextOptionName||undefined);const message=selectionMessage();status(message);return controlResult('add',false,message,{reason:selection.selectionStatus==='choosing'?'missing-options':selection.selectionStatus,cartChanged:false,...selection});}
      const key=typeof requestId==='string'&&requestId.length<=200&&!/[\u0000-\u001f]/.test(requestId)?requestId:null,fingerprint=JSON.stringify([p.id,p.handle,v.id,q,v.price,p.currency,variantChoiceSignature(v),engravingPreviewVariant(p,v)?engravingWords:null]),existing=key&&addRequests.get(key);
      if(existing){if(existing.fingerprint!==fingerprint)return controlResult('add',false,'That earlier add request belongs to a different choice. Make a new request for the current selection.',{cartChanged:false,reason:'request-changed'});const result=await existing.promise;return result.ok?{...result,cartChanged:false,deduplicated:true,message:'That exact request was already added to your test bag. Its current contents are unchanged.',snapshot:snapshot()}:result;}
      if(adding)return controlResult('add',false,'Your earlier exact choice is still being checked.',{reason:'checking',cartChanged:false});
      if(signal?.aborted||!select.isConnected||!currentProductUI())return controlResult('add',false,'The earlier add request was cancelled. Your newer view is preserved.',{reason:'cancelled',cartChanged:false});
      if(readCart().length>=50){const message='Your test bag has 50 lines. Remove a line before adding another; your existing pieces are preserved.';status(message);return controlResult('add',false,message,{reason:'bag-full',cartChanged:false});}
      const version=navigationVersion,controller=new AbortController(),forwardAbort=()=>controller.abort();addController=controller;signal?.addEventListener('abort',forwardAbort,{once:true});let timedOut=false;
      const admitted=()=>!controller.signal.aborted&&select.isConnected&&state.pageKind==='product'&&state.currentHandle===p.handle&&state.current===p&&select.value===v.id&&quantity()===q&&navigationVersion===version&&(!engravingPreviewVariant(p,v)||(engravingDrafts.get(p.id+'|'+p.handle)||'')===engravingWords);
      const cancellation=new Promise((_,reject)=>controller.signal.addEventListener('abort',()=>reject(Error(timedOut?'The live check took too long. Your bag is unchanged; try adding again.':'The earlier add request was cancelled. Your newer choice is preserved.')),{once:true})),timer=setTimeout(()=>{timedOut=true;controller.abort();},10000);
      adding=true;updateSelection();publish();
      const pending=(async()=>{
        try{
          const data=await Promise.race([get('/api/growth/product?handle='+encodeURIComponent(p.handle),controller.signal),cancellation]),live=data.product,matches=live?.variants?.filter(x=>x.id===v.id)||[],exact=matches.length===1?matches[0]:null;
          if(!admitted())return controlResult('add',false,'The earlier add request was cancelled. Your newer view is preserved.',{reason:'cancelled',cartChanged:false});
          if(data.live===false||!validProduct(live,p.handle)||live.id!==p.id||!live.variantsComplete||live.detailState==='unconfirmed'||!previewableVariant(live,exact)||!exact?.available||exact.availabilityKnown===false||live.currency!==p.currency||Math.abs(exact.price-v.price)>.005||variantChoiceSignature(exact)!==variantChoiceSignature(v)){
            // Withdraw the stale fact cache. A valid refreshed listing can
            // show its new price or choices immediately, while the failed add
            // never substitutes a different option on the shopper's behalf.
            checkedInventory.delete(p.handle);
            if(data.live!==false&&validProduct(live,p.handle)&&live.id===p.id&&live.variants.filter(x=>x.id===v.id).length<=1){const prior=productSelection(),fresh=remember(live);cacheChecked(live,data.checkedAt||Date.now());state.current=fresh;state.verifiedAt=checkedTime(live.checkedAt)||checkedTime(data.checkedAt)||Date.now();renderProduct(fresh,prior?{...prior,variantId:variantChoiceSignature(exact)===variantChoiceSignature(v)?prior.variantId:''}:null);publish();}
            const message='The exact option, price or availability changed. Your bag is unchanged; review the current piece before adding.';status(message);throw Error(message);
          }
          const cart=readCart();if(cart.length>=50)throw Error('Your test bag has 50 lines. Remove one before adding another.');const lineIds=cartIdentities(cart),before={cart:cart.slice(),ids:lineIds.slice()},lineId=newLineId();
          const preview=engravingPreviewVariant(live,exact),words=engravingWords;if(preview&&!validPrivateText(words))throw Error('Keep the engraving preview within 300 characters. Your bag is unchanged.');const item={productId:p.id,title:p.title,variantId:v.id.split('/').pop(),variant:v.title,price:v.price,currency:p.currency,variantOptions:exactVariantOptions(v),...(q!==1?{quantity:q}:{}),...(preview?{customizationPreview:true,engravingPreview:words}:{})};cart.push(item);lineIds.push(lineId);writeCart(cart,lineIds);rememberChange('bag-add',{kind:'bag'},before,{cart:readCart(),ids:cartIdentities(readCart())});clearReview();cacheChecked(live,data.checkedAt||Date.now());
          const message='Added '+p.title+' · '+v.title+' · quantity '+q+' to your test bag. '+money(Math.round(v.price*q*100)/100,p.currency)+' item subtotal.'+(preview?' Engraving is a local preview; the shop must review customization before checkout.':'');status(message);publish();return controlResult('add',true,message,{cartChanged:true,live:true,checkedAt:checkedTime(live.checkedAt)||checkedTime(data.checkedAt)||Date.now(),added:{lineId,productId:p.id,handle:p.handle,variantId:v.id,title:p.title,variant:v.title,quantity:q,price:v.price,currency:p.currency,subtotal:Math.round(v.price*q*100)/100},orderPlaced:false,paymentTaken:false});
        }catch(e){const message=controller.signal.aborted?timedOut?'The live check took too long. Your bag is unchanged; try adding again.':'The earlier add request was cancelled. Your newer choice is preserved.':e.message||'This exact option could not be checked. Your bag is unchanged; try again.';if(select.isConnected&&state.current===p&&version===navigationVersion)status(message);return controlResult('add',false,message,{reason:controller.signal.aborted?'cancelled':'verification-failed',cartChanged:false});}
        finally{clearTimeout(timer);signal?.removeEventListener('abort',forwardAbort);if(addController===controller)addController=null;adding=false;if(select.isConnected){updateSelection();publish();}}
      })();
      if(key){addRequests.set(key,{fingerprint,promise:pending});while(addRequests.size>100)addRequests.delete(addRequests.keys().next().value);}
      const result=await pending;if(key&&!result.ok&&addRequests.get(key)?.promise===pending)addRequests.delete(key);return {...result,snapshot:snapshot()};
    }
    add.addEventListener('click',()=>void addExact());
    let engravingInput=null;
    if(groups.some(g=>/engrav|personali[sz]/i.test(g.name))){const label=node('label','Your engraving wording · test preview','engraving-preview'),field=document.createElement('textarea');field.maxLength=300;field.name='test-engraving-preview';field.setAttribute('aria-label','Preview engraving wording for this piece');field.placeholder='The exact words you would like to discuss with the shop';const draftKey=p.id+'|'+p.handle;field.value=engravingDrafts.get(draftKey)||'';label.append(field,node('span','This previews your words only. Select the published engraving option separately; custom work and its final charge still need the shop.','option-help'));options.append(label);engravingInput=field;field.addEventListener('input',()=>{const before={text:engravingDrafts.get(draftKey)||''};engravingDrafts.set(draftKey,field.value);clearReview();rememberChange('set-engraving',{kind:'engraving',id:p.id,handle:p.handle},before,{text:field.value});publish();});}
    function restoreSelection(value){if(adding||!value||!validQuantity(value.quantity)||!Array.isArray(value.choices)||value.choices.some(o=>!groups.some(g=>g.name===o.name&&g.values.includes(o.value)))||value.variantId&&!p.variants.some(v=>v.id===value.variantId&&v.available&&v.availabilityKnown!==false))return false;clearReview();choices.clear();value.choices.forEach(o=>choices.set(o.name,o.value));select.value=value.variantId||'';quantityInput.value=String(value.quantity);updateSelection(false);if(images.length&&Number.isInteger(value.selectedImage)&&value.selectedImage>=0&&value.selectedImage<images.length)selectImage(value.selectedImage+1);openOptions(undefined,false);return true;}
    function setEngraving(text){if(adding||!engravingInput||engravingInput.disabled||typeof text!=='string'||text.length>300||/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(text))return false;const before={text:engravingInput.value};engravingInput.value=text;engravingDrafts.set(p.id+'|'+p.handle,text);clearReview();rememberChange('set-engraving',{kind:'engraving',id:p.id,handle:p.handle},before,{text});return true;}
    productUI={product:p,panel:options,select,menu,groups,quantity,quantityInput,menuTrigger,engravingInput,busy:()=>adding,exactVariant,selectionState,selectionMessage,addExact,cancelAdd:()=>addController?.abort(),choices:()=>[...choices].map(([name,value])=>({name,value})),openedOption:null,review:null,openOptions,closeOptions,revealSelection,chooseOption,chooseVariant,prepareReview,updateSelection,restoreSelection,setEngraving,selectImage,imageTrigger:enlarge,imageCount:images.length,invalidateReview:clearReview,setQuantity:value=>{if(adding||!validQuantity(value))return false;const before=productSelection();clearReview();quantityInput.value=String(value);updateSelection(false);recordProductChange('product-quantity',before);return true;}};
    options.append(label,select,menuTrigger,menu,selected,quantityLabel,selectionHelp,add,button('Review before adding','secondary',()=>{if(prepareReview())publish();else status(selectionMessage());}),node('p',p.variantsComplete?'Exact options above come from the product listing. Engraving or other custom inputs need the shop’s own customizer.':'This preview does not contain every variant. Confirm complete options with the shop before ordering.','option-help'));copy.append(options);
    const links=node('div',null,'product-links');links.append(link('Open the real product page ↗',p.url),button('Make it a gift','text-link',()=>void execute({type:'gift'})));copy.append(links);layout.append(gallery,copy);main.append(layout);
    const details=node('div',null,'product-details'),spec=node('section',null,'detail-panel');spec.dataset.storeSection='details';spec.append(node('h2','The details that matter'),node('p','Materials, dimensions and making details below are the exact listing information. If a detail is not published, your guide will help you ask the shop.'));
    const lines=node('ul');(p.options||[]).slice(0,10).forEach(o=>lines.append(node('li',clean(o.name,100)+': '+(o.values||[]).map(v=>clean(v,200)).slice(0,30).join(' · '))));if(lines.childElementCount)spec.append(lines);spec.append(node('p',p.description.slice(0,12000)));
    const story=node('section',null,'detail-panel story-block');story.dataset.storeSection='story';story.append(node('h2','A story you can make yours'),node('p','Explore reviewed history or symbolism for this exact piece. Meanings are personal interpretations, and can vary across people and cultures.'));const storyButton=actionButton('Explore its reviewed story','secondary',options=>showStory(p,story,storyButton,options));storyButton.dataset.storyOpen='true';story.append(storyButton);details.append(spec,story);main.append(details);if(selection){const priorRestore=restoringChange;restoringChange=true;try{const priorChoices=Array.isArray(selection.choices)?selection.choices:[],saved=p.variants.filter(v=>v.id===selection.variantId);if(selection.variantId&&saved.length===1&&priorChoices.every(o=>matchesChoice(saved[0],o.name,o.value))){select.value=selection.variantId;updateSelection();}else priorChoices.forEach(o=>chooseOption(o.name,o.value));if(validQuantity(selection.quantity))productUI.setQuantity(selection.quantity);if(selection.opened)openOptions(selection.openedOption||undefined,false);}finally{restoringChange=priorRestore;}}updateSelection(false);if(full){renderServicePanel(main);void loadServices();}
  }
  function neutralSource(source){
    // The knowledge endpoint already binds these citations to approved exact
    // product research. Preserve neutral base-dossier journals and museums as
    // well as supplement sources; enforce public-link safety at display time.
    try{const u=new URL(source?.url),host=u.hostname.replace(/^www\./,'').toLowerCase();
      if(u.protocol!=='https:'||u.username||u.password||u.port||!host.includes('.')||/^(?:localhost|127\.|0\.|10\.|192\.168\.|169\.254\.|172\.(?:1[6-9]|2\d|3[01])\.)/.test(host)||/(?:^|[./_-])(?:internal|private|admin|staging|author|prompt)(?:[./_-]|$)|(?:^|\/)(?:products?|shop|cart|checkout)(?:\/|$)/i.test(host+u.pathname))return null;
      if(/(?:^|\.)(?:etsy|amazon|ebay|aliexpress|walmart|shopify)\.[a-z.]+$/i.test(host))return null;
      const title=clean(source.title,300),safeTitle=storyText(source.title,300),at=source.checkedAt,now=Date.now();
      if(!title||title!==safeTitle||!Number.isFinite(at)||at>now+60000||now-at>30*86400000)return null;
      return {title,url:u.href,checkedAt:at};
    }catch{return null;}
  }
  function storyText(value,max){return clean(value,max).split(/(?<=[.!?])\s+/).filter(s=>!/<[^>]*>|https?:\/\/|(?:system|developer|assistant|author|internal|hidden)\s+(?:prompt|message|instructions?)|(?:tell|ask|instruct|advise|remind)\s+(?:the\s+)?(?:shopper|user|customer)|ignore .*instructions/i.test(s)).join(' ').trim();}
  async function showStory(p,box,b,options={}){
    if(options.signal?.aborted)return {ok:false,cancelled:true,message:'The reviewed story request was cancelled.'};
    if(p.meaningHold||p.recommendationHold){status('Reviewed meaning for this piece is being checked.');return {ok:false,message:notice.textContent};}
    const version=++storyVersion,navigation=navigationVersion,controller=new AbortController();let timedOut=false,abortStory;
    const attached=()=>version===storyVersion&&navigation===navigationVersion&&!state.loading&&state.pageKind==='product'&&state.currentHandle===p.handle&&state.current?.id===p.id&&box.isConnected&&b.isConnected;
    if(!attached())return {ok:false,message:'That exact current story control is unavailable.'};b.disabled=true;
    const aborted=new Promise((resolve,reject)=>{abortStory=()=>reject(Error('The reviewed story check was cancelled.'));controller.signal.addEventListener('abort',abortStory,{once:true});});
    const cancel=()=>controller.abort(),pageChanged=()=>{if(!attached())cancel();};options.signal?.addEventListener('abort',cancel,{once:true});
    document.addEventListener('brites-storefront:context',pageChanged);window.addEventListener('pagehide',cancel);
    const deadline=setTimeout(()=>{timedOut=true;cancel();},10000);
    try{const data=await Promise.race([get('/api/growth/knowledge?ids='+encodeURIComponent(p.id),controller.signal),aborted]);if(!attached()||controller.signal.aborted)return {ok:false,cancelled:true,message:'The reviewed story request was cancelled.'};
      const meanings=(Array.isArray(data.products)?data.products:[]).filter(m=>m.productId===p.id&&m.kind==='interpretation'&&storyText(m.text,1500)&&storyText(m.context,300)&&Array.isArray(m.sources)&&m.sources.length>0&&m.sources.every(s=>neutralSource(s))).slice(0,2);
      box.querySelectorAll('.reviewed-story').forEach(e=>e.remove());if(!meanings.length){b.textContent='No reviewed story is available yet';status('There is no approved cited story for this exact piece yet. The symbol can still have your own personal meaning.');return {ok:true,message:notice.textContent};}
      meanings.forEach(m=>{const story=node('div',null,'reviewed-story');story.append(node('p',storyText(m.text,1500)),node('p',storyText(m.context,300)+' · a personal interpretation; meanings can vary.','story-qualification'));m.sources.slice(0,4).forEach(s=>{const src=neutralSource(s);story.append(link(src.title,src.url));});box.append(story);});b.hidden=true;focusSection('story');return {ok:true,message:'The reviewed cited interpretation for this exact piece is open.'};
    }catch{if(attached()&&(!controller.signal.aborted||timedOut))status(timedOut?'The reviewed story check took too long. Please try again.':'The reviewed story could not be checked. Please try again.');return {ok:false,cancelled:controller.signal.aborted&&!timedOut,message:timedOut?'The reviewed story check took too long.':'The reviewed story request could not be completed.'};}
    finally{options.signal?.removeEventListener('abort',cancel);clearTimeout(deadline);controller.signal.removeEventListener('abort',abortStory);document.removeEventListener('brites-storefront:context',pageChanged);window.removeEventListener('pagehide',cancel);if(version===storyVersion||!b.isConnected)b.disabled=false;}
  }
  function retireImage({notify=false,restoreFocus=false}={}){
    const dialog=document.querySelector('#storefront-image-dialog');if(!dialog)return false;const heldFocus=dialog.contains(document.activeElement),ui=currentProductUI();dialog.remove();if(restoreFocus&&heldFocus&&ui?.imageTrigger?.isConnected&&!document.hidden)ui.imageTrigger.focus({preventScroll:true});if(notify)publish();return true;
  }
  function syncImageDialog(p){
    const dialog=document.querySelector('#storefront-image-dialog');if(!dialog)return;
    if(dialog.dataset.productId!==p.id||dialog.dataset.productHandle!==p.handle){retireImage();return;}
    const images=productImages(p),image=images[state.selectedImage],prior=dialog.querySelector('.zoom-content img'),caption=dialog.querySelector('.zoom-content p');if(!image){retireImage({notify:true});return;}const img=picture(image);img.loading='eager';if(prior)prior.replaceWith(img);if(caption)caption.textContent=p.title+' · image '+(state.selectedImage+1)+' of '+images.length+' from the live product listing';dialog.querySelectorAll('[data-zoom-direction]').forEach(b=>{b.disabled=b.dataset.zoomDirection==='previous'?state.selectedImage===0:state.selectedImage===images.length-1;});
  }
  function zoomImage(p){
    const image=productImages(p)[state.selectedImage];if(!image)return false;
    retireImage();const dialog=node('dialog',null,'storefront-dialog');dialog.id='storefront-image-dialog';dialog.dataset.productId=p.id;dialog.dataset.productHandle=p.handle;dialog.setAttribute('aria-label','Enlarged image of '+p.title);dialog.setAttribute('aria-modal','false');const content=node('div',null,'zoom-content'),close=button('×','dialog-close',()=>retireImage({notify:true,restoreFocus:true}));close.setAttribute('aria-label','Close enlarged image');const img=picture(image);img.loading='eager';content.append(close,img,node('p',p.title+' · image '+(state.selectedImage+1)+' from the live product listing'));if(productImages(p).length>1){const nav=node('div',null,'zoom-controls');[['previous','Previous image',-1],['next','Next image',1]].forEach(([direction,label,delta])=>{const b=button(label,'secondary',()=>void execute({type:'gallery',handle:p.handle,index:state.selectedImage+1+delta}));b.dataset.zoomDirection=direction;nav.append(b);});content.append(nav);}dialog.append(content);document.body.append(dialog);syncImageDialog(p);dialog.addEventListener('close',()=>{if(dialog.isConnected){dialog.remove();publish();}},{once:true});dialog.addEventListener('cancel',event=>{event.preventDefault();retireImage({notify:true,restoreFocus:true});});dialog.addEventListener('click',e=>{if(e.target===dialog)retireImage({notify:true,restoreFocus:true});});if(typeof dialog.show==='function')dialog.show();else dialog.setAttribute('open','');close.focus({preventScroll:true});state.activeSection='image';publish();return true;
  }
  // Nonmodal dialogs need explicit Escape handling. Close the image first so
  // Escape in the guide does not dismiss its existing voice session.
  document.addEventListener('keydown',event=>{if(event.key!=='Escape'||!document.querySelector('#storefront-image-dialog[open]'))return;event.preventDefault();event.stopPropagation();retireImage({notify:true,restoreFocus:true});},true);
  async function loadServices(){
    if(state.services)return state.services;if(state.servicesPending)return state.servicesPending;
    state.servicesPending=get('/api/growth/storefront-services').then(data=>{state.services=data.services||data;main.querySelectorAll('.service-panel').forEach(updateServices);return state.services;}).catch(()=>{main.querySelectorAll('.published-offers').forEach(list=>{list.replaceChildren(node('p','Current published offers could not be checked. Ask the shop before applying a code.'));});return null;}).finally(()=>{state.servicesPending=null;});return state.servicesPending;
  }
  function renderServiceStrip(){
    main.querySelector('.service-strip')?.remove();const strip=node('section',null,'service-strip');
    [['✧','Made with care','See the merchant’s sourcing and 2–3 day production guidance.','shipping'],['♡','A gift, beautifully personal','Explore gift packages, notes and wrapping with the shop.','gifts'],['⌁','Your idea, made yours','From engraving to a new design, talk through the possibilities.','customize']].forEach(([icon,title,copy,section])=>{const a=node('article');a.append(node('div',icon,'service-icon'),node('h3',title),node('p',copy),button('Explore '+(section==='shipping'?'shop services':section==='gifts'?'gifting':'custom options')+' ↗','text-link',()=>void execute({type:section==='gifts'?'gift':section==='customize'?'customize':'scroll',section})));strip.append(a);});main.append(strip);
  }
  function renderServicePanel(parent){
    parent.querySelector('.service-panel')?.remove();const panel=node('section',null,'service-panel');panel.dataset.storeSection='shipping';panel.append(node('span','THE BRITES MAKING & GIFTING EXPERIENCE','eyebrow'),node('h2','A little extra thought.'));
    const production=node('p','The Brites studio says production normally takes 2–3 days. Production time is separate from shipping time; a custom design or requested deadline needs studio confirmation.');production.dataset.guidancePart='production';
    const sourcing=node('p','The studio says its materials are ethically sourced from the United States. Exact materials and finishes come from the selected listing. This is merchant-provided guidance, not an independent sourcing certification.');sourcing.dataset.guidancePart='sourcing';
    const shipping=node('p','The studio offers a variety of shipping options. Available services, rates and transit estimates depend on your destination and are confirmed by the shop.');shipping.dataset.guidancePart='shipping';panel.append(production,sourcing,shipping,node('div',null,'service-conflicts'));
    const forms=node('div',null,'service-forms'),gift=node('form',null,'service-form');gift.dataset.storeSection='gifts';const giftSummary=node('p','The studio offers gift packages, gift notes and gift wrapping. Confirm package availability and any charge with the shop.');giftSummary.dataset.guidancePart='gifts';gift.append(node('h3','Make it a gift'),giftSummary,node('p','This test view saves your preferences only.'));
    const wrapLabel=node('label'),wrap=document.createElement('input');wrap.type='checkbox';wrap.name='wrapping';wrapLabel.append(wrap,document.createTextNode('Ask for gift wrapping'));
    const packageLabel=node('label'),pack=document.createElement('input');pack.type='checkbox';pack.name='package';packageLabel.append(pack,document.createTextNode('Ask about a gift package'));
    const noteLabel=node('label','Your test gift note'),note=document.createElement('textarea');note.name='note';note.maxLength=350;note.setAttribute('aria-label','Test gift note');note.placeholder='Something they will love to read…';noteLabel.append(note);const saveGift=button('Save test gift preferences','secondary');saveGift.type='submit';gift.append(wrapLabel,packageLabel,noteLabel,saveGift);
    gift.addEventListener('submit',e=>{e.preventDefault();savePreferences({wrapping:wrap.checked,giftPackage:pack.checked,giftNote:note.value.slice(0,350)});status('Gift preferences saved for this test session. Nothing was sent to the shop.');});
    const custom=node('form',null,'service-form');custom.dataset.storeSection='customize';const customSummary=node('p','Explore engraving, adjusted designs or a brand new piece based on your idea. The studio confirms feasibility, materials, pricing and production time before a custom order.');customSummary.dataset.guidancePart='customization';custom.append(node('h3','Make it yours'),customSummary);
    const ideaLabel=node('label','Your test design idea'),idea=document.createElement('textarea');idea.name='idea';idea.maxLength=600;idea.setAttribute('aria-label','Test custom design idea');idea.placeholder='A symbol, a date, a sketch you would like to discuss…';ideaLabel.append(idea);const saveIdea=button('Save test design brief','secondary');saveIdea.type='submit';custom.append(ideaLabel,saveIdea);custom.addEventListener('submit',e=>{e.preventDefault();savePreferences({customIdea:idea.value.slice(0,600)});status('Design brief saved for this test session. It has not been sent to the shop.');});
    const preferences=readPreferences();wrap.checked=preferences.wrapping===true;pack.checked=preferences.giftPackage===true;note.value=preferences.giftNote||'';idea.value=preferences.customIdea||'';forms.append(gift,custom);panel.append(forms,node('p','These notes stay in this browser session. No contact, address, payment or upload information is requested.','local-only-note'));
    const offers=node('div');offers.dataset.storeSection='offers';offers.append(node('h3','Published offers'),node('p','Only an offer currently published by Brites will be shown here. An offer’s eligibility and final discount are confirmed in the real shop.'));
    const list=node('div',null,'published-offers');offers.append(list);panel.append(offers);parent.append(panel);updateServices(panel);return panel;
  }
  function ownSource(source){try{const u=new URL(source?.url);return u.protocol==='https:'&&!u.username&&!u.password&&!u.port&&['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)?{title:clean(source.title,300)||'Brites published source',url:u.href}:null;}catch{return null;}}
  function updateServices(panel){
    const services=state.services;
    if(services?.guidance){panel.querySelectorAll('[data-guidance-part]').forEach(p=>{const part=services.guidance[p.dataset.guidancePart];if(typeof part?.summary==='string')p.textContent=clean(part.summary,1500)+(p.dataset.guidancePart==='sourcing'?' Merchant-provided guidance; not an independent sourcing certification.':'');});}
    const conflicts=panel.querySelector('.service-conflicts');if(conflicts){conflicts.replaceChildren();(Array.isArray(services?.conflicts)?services.conflicts:[]).slice(0,3).forEach(conflict=>{const src=ownSource(conflict.source);if(!src||typeof conflict.publishedSummary!=='string')return;conflicts.append(node('h3','Confirm current '+clean(conflict.topic,50)+' guidance'),node('p',clean(conflict.merchantSummary,1000)),node('p',clean(conflict.publishedSummary,1500)),link(src.title+' ↗',src.url));});}
    const offers=panel.querySelector('.published-offers');if(offers)renderOffers(offers);
  }
  function renderOffers(list){
    list.replaceChildren();const offers=state.services?.offers?.items||state.services?.offers||state.services?.publishedOffers||[];
    if(!Array.isArray(offers)||!offers.length){list.append(node('p',!state.services?'Checking current published offers…':state.services.offers?.status==='unavailable'?'Current published offers could not be checked.':'No published offer was observed in the checked sources.'));return;}
    if(state.services.offers?.partial===true)list.append(node('p','Some published sources could not be checked. Confirm the current offer and terms with the shop.'));
    offers.slice(0,5).forEach(o=>{const title=clean(o?.summary||o?.title||o?.text||o?.description,1200),src=ownSource(o?.source);if(!title||!src||o.checkoutValidated===true)return;list.append(node('p',title),node('p','Published offer · eligibility, expiry and any combination with other discounts must be confirmed at checkout.'));list.append(link(src.title+' ↗',src.url));});
  }
  function readPreferences(){try{const p=JSON.parse(sessionStorage.getItem('brites-sandbox-gift-preferences')||'{}');return {wrapping:p.wrapping===true,giftPackage:p.giftPackage===true,giftNote:clean(p.giftNote,350),customIdea:clean(p.customIdea,600)};}catch{return {};}}
  function savePreferences(p){const before=readPreferences();sessionStorage.setItem('brites-sandbox-gift-preferences',JSON.stringify({...before,...p}));rememberChange('gift-preferences',{kind:'gift'},before,readPreferences());refreshCheckoutPreferences();publish();}
  function refreshCheckoutPreferences(){const ui=checkoutUI;if(!ui?.page?.isConnected)return;const prefs=readPreferences();ui.page.querySelectorAll('[data-checkout-gift]').forEach(line=>{line.textContent=line.dataset.checkoutGift==='wrapping'?prefs.wrapping?'Gift wrapping enquiry saved.':'No gift wrapping enquiry saved.':line.dataset.checkoutGift==='package'?prefs.giftPackage?'Gift package enquiry saved.':'No gift package enquiry saved.':prefs.giftNote?'Test gift note: '+prefs.giftNote:'No test gift note saved.';});}
  async function showService(section){
    if(!main.querySelector('.service-panel'))renderServicePanel(main);
    focusSection(section);void loadServices();
    return {ok:true,action:section==='gifts'?'gift':section==='customize'?'customize':'scroll',message:'The '+section+' section is open. Preferences stay in this test session.',snapshot:snapshot()};
  }
  function saveBagMenus(){try{sessionStorage.setItem('brites-sandbox-bag-menus',JSON.stringify([...bagMenus].slice(0,50)));}catch{}}
  function bagRowFor(lineId){return [...main.querySelectorAll('[data-bag-line]')].find(row=>row.dataset.bagLine===lineId);}
  function revealBagControl(lineId,section,optionName,highlight=true){
    const row=bagRowFor(lineId);if(!row||document.hidden)return false;
    const groups=[...row.querySelectorAll('[data-bag-option-group]')],group=optionName?groups.find(group=>group.dataset.bagOptionGroup===optionName):null;
    const target=section==='title'?row.querySelector('h3'):section==='price'?row.querySelector('[data-bag-price]'):section==='quantity'?row.querySelector('.bag-quantity'):section==='engraving'?row.querySelector('[data-bag-engraving]'):section==='options'?group||row.querySelector('[data-bag-choices]'):null;
    if(!target||optionName&&!group)return false;
    clearTimeout(highlightTimer);main.querySelectorAll('.store-highlight').forEach(node=>node.classList.remove('store-highlight'));if(highlight){target.classList.add('store-highlight');highlightTimer=setTimeout(()=>target.classList.remove('store-highlight'),2600);}
    try{if(!revealAboveGuide(target))target.scrollIntoView?.({behavior:'instant',block:'center'});}catch{}
    state.activeSection='bag';return true;
  }
  function openBagOptions(lineId,name){
    const row=bagRowFor(lineId),groups=row&&[...row.querySelectorAll('[data-bag-option-group]')],matches=groups?.filter(group=>name===undefined||group.dataset.bagOptionGroup.toLowerCase()===String(name).toLowerCase());
    if(state.pageKind!=='bag'||!matches?.length||name!==undefined&&matches.length!==1)return false;
    const selected=name===undefined?null:matches[0].dataset.bagOptionGroup;bagMenus.clear();bagMenus.set(lineId,selected||'');saveBagMenus();
    main.querySelectorAll('[data-bag-menu]').forEach(menu=>{menu.hidden=menu.closest('[data-bag-line]').dataset.bagLine!==lineId||!!selected&&menu.dataset.bagMenu!==selected;menu.previousElementSibling?.setAttribute('aria-expanded',String(!menu.hidden));});
    attendedControl={lineId,kind:'option',optionName:selected};attendedControlNode=matches[0].querySelector('[data-bag-option]');revealBagControl(lineId,'options',selected||undefined);publish();return true;
  }
  function closeBagOptions(){bagMenus.clear();saveBagMenus();main.querySelectorAll('[data-bag-menu]').forEach(menu=>{menu.hidden=true;menu.previousElementSibling?.setAttribute('aria-expanded','false');});attendedControl=null;attendedControlNode=null;publish();return true;}
  function refreshBagInventory(){
    const version=navigationVersion,handles=[...new Set(readCart().map(item=>identities.get(item.productId)).filter(validHandle))];
    handles.forEach(handle=>{if(cachedRead(handle)||bagChecks.has(handle))return;const pending=get('/api/growth/product?handle='+encodeURIComponent(handle)).then(data=>{if(version!==navigationVersion||state.pageKind!=='bag'||data.live===false||!validProduct(data.product,handle))return;const item=readCart().find(item=>identities.get(item.productId)===handle);if(!item||data.product.id!==item.productId)return;cacheChecked(data.product,data.checkedAt||Date.now());refreshBagChoices();publish();}).catch(()=>{}).finally(()=>{if(bagChecks.get(handle)===pending)bagChecks.delete(handle);});bagChecks.set(handle,pending);});
  }
  function renderBag({push=true}={}){
    const prior=document.activeElement,priorRow=prior?.closest?.('[data-bag-line]'),priorControl=priorRow?{lineId:priorRow.dataset.bagLine,optionName:prior.dataset.bagOption||null,quantity:!!prior.closest('.bag-quantity'),engraving:prior.hasAttribute('data-bag-engraving')}:null;
    retireImage();
    saveCurrentView();cancelPending();state.current=null;main.replaceChildren();const page=node('section',null,'bag-page view-enter');page.dataset.storeSection='bag';page.append(node('span','YOUR SESSION-ONLY SELECTION','eyebrow'),node('h1','Your sandbox bag'),node('p','A place to try your choices together. This test bag never places a shop order.','bag-intro'));const cart=readCart(),items=node('div',null,'bag-items');
    if(!cart.length)items.append(node('p','Your sandbox bag is empty.','bag-intro'));
    const lineIds=cartIdentities(cart);cart.forEach((p,index)=>{const row=node('article',null,'bag-item'),copy=node('div');row.dataset.bagLine=lineIds[index];copy.append(node('h3',p.title),node('p',p.variant),button('Remove this test piece','text-link',()=>void execute({type:'bag-remove',lineId:lineIds[index]})));const label=node('label','Quantity','bag-quantity'),quantity=document.createElement('input');quantity.type='number';quantity.min='1';quantity.max='20';quantity.step='1';quantity.value=String(p.quantity||1);quantity.setAttribute('aria-label','Quantity of '+p.title+' · '+p.variant);quantity.addEventListener('change',async()=>{const result=await execute({type:'bag-quantity',lineId:lineIds[index],quantity:Number(quantity.value)});if(!result.ok&&quantity.isConnected){quantity.value=String(p.quantity||1);status(result.message);}});label.append(quantity);copy.append(label);const choices=node('div');choices.dataset.bagChoices='true';renderBagChoices(choices,p,lineIds[index],index);copy.append(choices);const handle=identities.get(p.productId);if(handle)copy.append(button('View its live details','text-link',()=>void openProduct(handle)));const subtotal=node('p',money(p.price*(p.quantity||1),p.currency));subtotal.dataset.bagPrice='true';row.append(copy,subtotal);items.append(row);});
    page.append(items);const totals=new Map();cart.forEach(p=>totals.set(p.currency,(totals.get(p.currency)||0)+p.price*(p.quantity||1)));totals.forEach((amount,currency)=>{const total=node('div',null,'bag-total');total.append(node('span','Test item subtotal'),node('strong',money(amount,currency)));page.append(total);});if(cart.length)page.append(node('p','Items only. Shipping, taxes, gift services and any discounts are not calculated in this preview.','bag-subtext'));
    const actions=node('div',null,'bag-actions');actions.append(button('Continue exploring','secondary',()=>void execute({type:'search',query:''})));if(cart.length)actions.append(button('Empty test bag','secondary',()=>void execute({type:'bag-clear',lineIds:cartIdentities(readCart())})),button('Try test checkout →','primary',()=>void execute({type:'checkout'})));page.append(actions);main.append(page);if(full)renderServicePanel(page);commitPage('bag','',push);updateBag();
    if(priorControl&&!prior.isConnected&&document.activeElement===document.body&&!document.hidden){const row=bagRowFor(priorControl.lineId),target=priorControl.optionName?row&&[...row.querySelectorAll('[data-bag-option]')].find(select=>select.dataset.bagOption===priorControl.optionName):priorControl.quantity?row?.querySelector('.bag-quantity input'):priorControl.engraving?row?.querySelector('[data-bag-engraving]'):null;target?.focus({preventScroll:true});}
    refreshBagInventory();
  }
  function renderBagChoices(copy,item,lineId,index){
    const binding=bagProductControls(item);if(!binding)return;
    const {p,v,groups}=binding;
    groups.forEach(group=>{
      const selected=(v.options||[]).find(o=>o.name===group.name)?.value,label=node('label',group.name,'bag-option'),select=document.createElement('select');select.dataset.bagOption=group.name;select.setAttribute('aria-label',group.name+' for '+item.title+' · item '+(index+1));
      group.values.forEach(value=>{const option=node('option',value);option.value=value;option.disabled=!p.variants.some(candidate=>candidate.available&&candidate.availabilityKnown!==false&&previewableVariant(p,candidate)&&groups.every(g=>(candidate.options||[]).some(o=>o.name===g.name&&o.value===(g.name===group.name?value:(v.options||[]).find(o=>o.name===g.name)?.value))));select.append(option);});
      select.value=selected||'';select.addEventListener('change',async()=>{const result=await execute({type:'bag-select-option',lineId,optionName:group.name,optionValue:select.value});if(!result.ok&&select.isConnected){select.value=selected||'';status(result.message);}});label.dataset.bagOptionGroup=group.name;
      const menu=node('div',null,'option-menu'),trigger=button('Open '+group.name+' choices','secondary option-menu-trigger',()=>menu.hidden?openBagOptions(lineId,group.name):closeBagOptions());menu.dataset.bagMenu=group.name;menu.setAttribute('role','listbox');menu.setAttribute('aria-label',group.name+' choices for '+item.title);menu.hidden=!bagMenus.has(lineId)||!!bagMenus.get(lineId)&&bagMenus.get(lineId)!==group.name;trigger.setAttribute('aria-expanded',String(!menu.hidden));
      [...select.options].forEach(option=>{const choice=button(option.value,'option-choice',()=>void execute({type:'bag-select-option',lineId,optionName:group.name,optionValue:option.value}));choice.dataset.bagOptionValue=option.value;choice.setAttribute('role','option');choice.setAttribute('aria-selected',String(option.value===select.value));choice.disabled=option.disabled;menu.append(choice);});label.append(select,trigger,menu);copy.append(label);
    });
    if(groups.some(g=>/^engraving(?: option)?$/i.test(g.name))){
      const label=node('label','Engraving wording · local preview','engraving-preview'),field=document.createElement('textarea');field.dataset.bagEngraving='true';field.maxLength=300;field.value=item.engravingPreview||'';field.setAttribute('aria-label','Engraving preview for '+item.title+' · item '+(index+1));
      const save=button('Save engraving preview','secondary',async()=>{const result=await execute({type:'bag-set-engraving',lineId,text:field.value});if(!result.ok&&field.isConnected)status(result.message);}),clear=button('Clear engraving preview','text-link',async()=>{const result=await execute({type:'bag-set-engraving',lineId,text:''});if(!result.ok&&field.isConnected)status(result.message);});
      label.append(field,node('span','Your words stay in this test bag. The shop must review engraved work and its final charge before checkout.','option-help'),save,clear);copy.append(label);
    }
    if(item.customizationPreview===true)copy.append(node('p','Local engraving preview · shop customization review is required before checkout.','option-help'));
  }
  function refreshBagChoices(){
    if(state.pageKind!=='bag')return;
    const cart=readCart(),ids=cartIdentities(cart),rows=[...main.querySelectorAll('[data-bag-line]')];
    cart.forEach((item,index)=>{
      const row=rows.find(row=>row.dataset.bagLine===ids[index]),slot=row?.querySelector('[data-bag-choices]');if(!slot||slot.children.length)return;
      const binding=bagProductControls(item),checked=binding&&checkedInventory.get(binding.p.handle);
      if(!binding||!checked||checked.expiresAt<=Date.now()||checked.product.id!==item.productId||binding.p.detailState!=='checked'||!binding.v.available||binding.v.availabilityKnown===false)return;
      renderBagChoices(slot,item,ids[index],index);
    });
  }
  async function verifyBag(cart,signal,{allowCustomizationPreview=false}={}){
    const requests=new Map();for(const item of cart){const handle=identities.get(item.productId);if(!handle)throw Error('One saved piece needs its product page checked before test checkout.');if(!requests.has(handle))requests.set(handle,get('/api/growth/product?handle='+encodeURIComponent(handle),signal));}
    const results=await Promise.all([...requests].map(async([handle,pending])=>({handle,data:await pending}))),products=new Map();for(const {handle,data:d}of results){if(d.live===false||!validProduct(d.product,handle))throw Error('A bag piece could not be checked.');products.set(d.product.id,d.product);}
    for(const item of cart){const p=products.get(item.productId),matches=p?.variants.filter(v=>v.id==='gid://shopify/ProductVariant/'+item.variantId)||[],v=matches.length===1?matches[0]:null;if(!p||!p.variantsComplete||p.detailState==='unconfirmed'||!v?.available||v.availabilityKnown===false||v.title!==item.variant||item.variantOptions!==undefined&&JSON.stringify(exactVariantOptions(v))!==JSON.stringify(exactVariantOptions({options:item.variantOptions}))||p.currency!==item.currency||Math.abs(v.price-item.price)>.005)throw Error('A saved option, price or availability changed. Reopen that piece before test checkout.');if(needsCustomizer(p,v)&&!(allowCustomizationPreview&&item.customizationPreview===true&&engravingPreviewVariant(p,v)))throw Error(item.customizationPreview===true&&engravingPreviewVariant(p,v)?'The engraving is saved as a local preview. The shop must review customization and its final charge before checkout.':'This piece now needs the shop’s review. Your saved choices are preserved; open its current details before continuing.');}return products;
  }
  async function editBag(action,options){
    const type=action.type,cart=readCart(),ids=cartIdentities(cart),index=ids.indexOf(action.lineId),visible=main.querySelector('[data-bag-line="'+(typeof action.lineId==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9:_-]{0,199}$/.test(action.lineId)?action.lineId:'')+'"]');
    if(!['bag','checkout'].includes(state.pageKind)||index<0||!visible||type==='bag-quantity'&&!validQuantity(action.quantity))return controlResult(type,false,'Choose one exact current test-bag line and a quantity from 1 to 20.');
    const choice=type==='bag-select-option',engraving=type==='bag-set-engraving',field=visible.querySelector('[data-bag-engraving]');
    if((choice||engraving)&&state.pageKind!=='bag')return controlResult(type,false,'Open your bag before changing this piece’s options or engraving preview.');
    if(choice&&(typeof action.optionName!=='string'||typeof action.optionValue!=='string'||!action.optionName||!action.optionValue||action.optionName.length>100||action.optionValue.length>300))return controlResult(type,false,'Choose one option name and value shown for this piece.');
    if(engraving&&(!field||field.disabled||!validPrivateText(action.text)))return controlResult(type,false,'Use the visible engraving preview for this piece and keep the wording within 300 characters.');
    const fingerprint=JSON.stringify(cart),version=navigationVersion,editVersion=++bagEditVersion,priorBinding=choice?publicCopy(bagProductControls(cart[index])):null,controlFingerprint=()=>JSON.stringify([...visible.querySelectorAll('input,select,textarea')].map(control=>[control.tagName,control.dataset.bagOption||'',control.value])),controlsBefore=controlFingerprint();let products=null;
    if(type!=='bag-remove'){try{products=await verifyBag([cart[index]],options.signal,{allowCustomizationPreview:true});}catch(e){return controlResult(type,false,e.message||'That saved option could not be checked.');}}
    if(options.signal?.aborted||version!==navigationVersion||editVersion!==bagEditVersion||JSON.stringify(readCart())!==fingerprint||!visible.isConnected||controlFingerprint()!==controlsBefore||cartIdentities(readCart())[index]!==action.lineId)return controlResult(type,false,'Your test bag changed while the choice was checked. Please use its current item.');
    const next=cart.slice(),nextIds=ids.slice();let message='',cleared=false;
    if(type==='bag-remove'){next.splice(index,1);nextIds.splice(index,1);message='That exact item was removed from your test bag.';}
    else if(type==='bag-quantity'){next[index]={...next[index],quantity:action.quantity};message='The exact test-bag quantity is updated. This does not reserve shop inventory.';}
    else {
      const p=products?.get(cart[index].productId),old=savedVariant(p,cart[index]),groups=p&&exactOptionGroups(p);if(!old||!groups||!previewableVariant(p,old))return controlResult(type,false,'This piece’s saved options need to be reviewed on its current product page.');
      if(choice){
        const named=groups.filter(g=>g.name.toLowerCase()===action.optionName.toLowerCase()),group=named.length===1?named[0]:null,values=group?.values.filter(value=>value.toLowerCase()===action.optionValue.toLowerCase())||[],value=values.length===1?values[0]:null,control=group&&[...visible.querySelectorAll('[data-bag-option]')].find(control=>control.dataset.bagOption===group.name),offered=value&&control&&[...control.options].filter(option=>option.value===value&&!option.disabled);
        if(!group||!value||!control||control.disabled||offered.length!==1)return controlResult(type,false,'That choice is not available in this item’s current option menu. Your bag is preserved.');
        const candidates=p.variants.filter(candidate=>candidate.available&&candidate.availabilityKnown!==false&&previewableVariant(p,candidate)&&groups.every(g=>(candidate.options||[]).some(o=>o.name===g.name&&o.value===(g.name===group.name?value:(old.options||[]).find(o=>o.name===g.name)?.value))));
        if(candidates.length!==1)return controlResult(type,false,'That combination is unavailable. Your current bag choices are preserved.');
        const selected=candidates[0],priorMatches=priorBinding?.p?.variants?.filter(candidate=>candidate.id===selected.id)||[],prior=priorMatches.length===1?priorMatches[0]:null;
        if(!prior||!prior.available||prior.availabilityKnown===false||priorBinding.p.currency!==p.currency||Math.abs(prior.price-selected.price)>.005||variantChoiceSignature(prior)!==variantChoiceSignature(selected))return controlResult(type,false,'That option’s price or published choices changed. Your bag is unchanged; review the current piece before choosing it.');
        next[index]={...next[index],variantId:selected.id.split('/').pop(),variant:selected.title,price:selected.price,currency:p.currency,variantOptions:exactVariantOptions(selected)};
        if(engravingPreviewVariant(p,selected))next[index].customizationPreview=true;else delete next[index].customizationPreview;
        if(/^engraving(?: option)?$/i.test(group.name)&&noEngraving(value)){cleared=!!next[index].engravingPreview;delete next[index].engravingPreview;}
        message=group.name+' is now '+value+' for '+cart[index].title+'. Quantity '+(cart[index].quantity||1)+' is preserved. '+money(selected.price,p.currency)+' each; '+money(Math.round(selected.price*(cart[index].quantity||1)*100)/100,p.currency)+' item subtotal.'+(cleared?' Its engraving preview wording was cleared.':'')+(next[index].customizationPreview?' Engraving remains a local preview; the shop must review customization before checkout.':'');
      }else if(engraving){
        if(!groups.some(g=>/^engraving(?: option)?$/i.test(g.name)))return controlResult(type,false,'This piece does not publish an engraving option.');
        next[index]={...next[index]};if(action.text)next[index].engravingPreview=action.text;else delete next[index].engravingPreview;
        message=action.text?'Your engraving wording is saved in this item’s local preview. The shop must review customization before an engraved order.':'The engraving wording was cleared from this item’s local preview.';
      }else return controlResult(type,false,'That bag change is unavailable.');
      cacheChecked(p,Date.now());
    }
    const changed=JSON.stringify(next)!==fingerprint;if(changed)writeCart(next,nextIds);rememberChange(type,{kind:'bag'},{cart,ids},{cart:readCart(),ids:cartIdentities(readCart())});if(state.pageKind==='checkout')renderBag({push:false});status(message);publish();return controlResult(type,true,notice.textContent,{cartChanged:changed});
  }
  function controlResult(type,ok,message,extra={}){return {ok,action:type,message,...extra,snapshot:snapshot()};}
  async function runProductControl(action,options){
    const type=action.type,crossing=type==='options'&&action.handle&&action.handle!==state.currentHandle,initialFocus=focusedElement(),guard=crossing?trackRevealFocus(initialFocus):null;
    try{if(crossing&&!await openProduct(action.handle,{signal:options.signal}))return controlResult(type,false,'That exact piece could not be checked.');const ui=currentProductUI();if(options.signal?.aborted||!ui||action.handle&&action.handle!==ui.product.handle)return controlResult(type,false,'Open the exact current piece before changing its published options.');
      if(type==='add'){if(action.variantId!==undefined&&action.variantId!==ui.select.value||action.quantity!==undefined&&action.quantity!==ui.quantity())return controlResult(type,false,'Select the exact requested option and quantity before adding this piece.',{cartChanged:false});return ui.addExact({signal:options.signal,requestId:options.requestId||action.requestId});}
      let ok=false;if(type==='options'){if(action.optionName!==undefined&&typeof action.optionName!=='string')return controlResult(type,false,'Name one literal published option group.');ok=ui.openOptions(action.optionName,!guard||guard.allowed()&&(focusedElement()===initialFocus||!initialFocus?.isConnected&&focusedElement()===document.body));}else if(type==='close-options')ok=ui.closeOptions();else if(type==='set-engraving')ok=ui.setEngraving(action.text);else if(type==='select-option'){
        const byId=typeof action.variantId==='string',byValue=typeof action.optionName==='string'&&typeof action.optionValue==='string';if(byId===byValue||byId&&(action.optionName!==undefined||action.optionValue!==undefined))return controlResult(type,false,'Choose an exact variant or one literal published option name and value.');ok=byId?ui.chooseVariant(action.variantId):ui.chooseOption(action.optionName,action.optionValue);
      }else if(type==='product-quantity')ok=ui.setQuantity(action.quantity);else if(type==='review-add'){if(action.variantId&&action.variantId!==ui.select.value)return controlResult(type,false,'Select that exact published option before reviewing it.');ok=ui.prepareReview();}
      if(!ok)return controlResult(type,false,type==='review-add'?ui.selectionMessage():type==='set-engraving'?'This exact piece has no writable published engraving preview, or the wording is invalid.':'That exact published option or quantity is unavailable. Your previous choices are preserved.');if(type==='set-engraving'&&!document.hidden)try{ui.engravingInput.scrollIntoView?.({behavior:'instant',block:'center'});}catch{}publish();const selectionState=ui.selectionState(),next=selectionState.requiredOptionGroups[0],choiceReply=selectionState.selectionStatus==='held'?'This piece needs the shop’s customization review before adding.':selectionState.selectionStatus==='ready'?money(Math.round(ui.exactVariant().price*ui.quantity()*100)/100,ui.product.currency)+' item subtotal. Say “add it to my bag” when you are ready.'+(engravingPreviewVariant(ui.product,ui.exactVariant())?' Engraving is a preview; the shop reviews customization.':''):next?'Next, choose '+next.name+(next.values.length<=3&&!/metal|material|finish/i.test(next.name)?' — '+next.values.join(' or '):'')+'.':'That combination is unavailable. Please choose another option.';const message=type==='options'?'The '+(ui.openedOption||'published options')+' menu is open.':type==='close-options'?'The option menu is closed. Your choices are preserved.':type==='set-engraving'?'Your engraving wording is updated. The shop confirms customization and its charge.':type==='select-option'?(action.optionValue?'Selected '+action.optionValue+'. ':'')+choiceReply:type==='product-quantity'?'Quantity '+ui.quantity()+'.'+(['held','ready'].includes(selectionState.selectionStatus)?' '+choiceReply:''):'Please review your exact choices before adding.';status(message);return controlResult(type,true,message,type==='review-add'?{requiredCustomerClick:'Confirm add to test bag',cartChanged:false}:{});
    }finally{guard?.dispose();}
  }
  async function setGiftPreferences(action){
    const patch={};for(const key of ['wrapping','giftPackage'])if(action[key]!==undefined){if(typeof action[key]!=='boolean')return controlResult(action.type,false,'Use a clear yes or no for the test gift preference.');patch[key]=action[key];}
    if(action.giftNote!==undefined){if(typeof action.giftNote!=='string'||action.giftNote.length>300||/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(action.giftNote))return controlResult(action.type,false,'Please keep the exact test gift note within 300 characters.');patch.giftNote=action.giftNote;}
    if(!Object.keys(patch).length)return controlResult(action.type,false,'Please state the test gift preference you want saved.');savePreferences(patch);if(!main.querySelector('.service-panel'))renderServicePanel(main);const prefs=readPreferences();main.querySelectorAll('[data-store-section=gifts]').forEach(form=>{const wrap=form.querySelector('[name=wrapping]'),pack=form.querySelector('[name=package]'),note=form.querySelector('[name=note]');if(wrap)wrap.checked=prefs.wrapping===true;if(pack)pack.checked=prefs.giftPackage===true;if(note)note.value=prefs.giftNote||'';});publish();status('Your gift preferences are saved only in this test session. Nothing was sent to the shop.');return controlResult(action.type,true,notice.textContent);
  }
  async function renderCheckout({push=true,signal}={}){
    const cart=readCart();if(!cart.length){renderBag({push});return {ok:false,action:'checkout',message:'Choose a piece for your test bag first.'};}
    const record=begin({signal});status('Checking the exact bag options before test checkout…');
    try{await verifyBag(cart,record.controller.signal);if(!current(record))return {ok:false,action:'checkout',message:'Test checkout was cancelled.'};if(JSON.stringify(readCart())!==JSON.stringify(cart))throw Error('Your bag changed. Please reopen test checkout.');}catch(e){if(current(record)){state.loading=false;publish();status(e.message);}return {ok:false,action:'checkout',message:e.message||'The exact bag choices could not be checked.'};}
    retireImage();main.replaceChildren();const page=node('section',null,'checkout-page view-enter');page.dataset.storeSection='checkout';page.append(button('← Return to test bag','back-link',()=>renderBag()),node('span','A COMPLETE SIMULATION','eyebrow'),node('h1','A thoughtful finishing touch.'),node('p','Try the checkout experience without creating an order or providing payment, contact or address details.','bag-intro'));
    const steps=node('nav',null,'checkout-steps');steps.setAttribute('aria-label','Test checkout steps');[['review','Review exact pieces'],['shipping','Demo shipping & gifts'],['confirm','Confirm the simulation']].forEach(([step,label])=>{const b=button(label,'secondary',()=>void execute({type:'checkout-step',step}));b.dataset.checkoutStepButton=step;b.setAttribute('aria-current',step==='review'?'step':'false');steps.append(b);});page.append(steps);
    const grid=node('div',null,'checkout-grid'),summary=node('section',null,'checkout-box checkout-step');summary.dataset.checkoutStep='review';summary.append(node('h2','Your exact choices'));const lineIds=cartIdentities(cart);cart.forEach((p,index)=>{const row=node('div',null,'checkout-line');row.dataset.bagLine=lineIds[index];row.append(node('p',p.title+' · '+p.variant+' · quantity '+(p.quantity||1)),node('p',money(p.price*(p.quantity||1),p.currency)));summary.append(row);});
    const prefs=readPreferences(),services=node('section',null,'checkout-box checkout-step');services.dataset.storeSection='shipping';services.dataset.checkoutStep='shipping';services.append(node('h2','Shipping & gifting'),node('p','Production normally takes 2–3 days according to Brites. Shipping choices, shipping time and final charges are confirmed by the shop.'));
    const demo=node('div',null,'demo-shipping');demo.append(node('p','These two shipping labels demonstrate the interface only. They are not live services, rates or delivery promises.'));[['standard','Demo standard'],['express','Demo express']].forEach(([value,label])=>{const b=button(label,'secondary',()=>void execute({type:'checkout-option',option:'shipping',value}));b.dataset.demoShipping=value;b.setAttribute('aria-pressed',String(value==='standard'));demo.append(b);});services.append(demo);[['wrapping',prefs.wrapping?'Gift wrapping enquiry saved.':'No gift wrapping enquiry saved.'],['package',prefs.giftPackage?'Gift package enquiry saved.':'No gift package enquiry saved.'],['note',prefs.giftNote?'Test gift note: '+prefs.giftNote:'No test gift note saved.']].forEach(([key,text])=>{const line=node('p',text);line.dataset.checkoutGift=key;services.append(line);});services.append(button('Adjust gift preferences','text-link',()=>void showService('gifts')));grid.append(summary,services);page.append(grid);
    const acknowledgement=node('label',null,'checkout-check'),check=document.createElement('input');check.type='checkbox';check.id='confirm-test-checkout';acknowledgement.append(check,document.createTextNode('I understand this completes a test only. No order, payment or message will be sent.'));const complete=button('Complete test checkout','primary');complete.disabled=true;
    const confirm=node('section',null,'checkout-box checkout-step');confirm.dataset.checkoutStep='confirm';confirm.append(node('h2','Complete the test only'),acknowledgement,complete);page.append(confirm);
    const ui={page,step:'review',shipping:'standard',check,completed:false,completionController:null};checkoutUI=ui;
    function syncCompletionButton(){if(page.isConnected&&checkoutUI===ui&&state.pageKind==='checkout'&&!ui.completed)complete.disabled=!check.checked||!!ui.completionController;}
    check.addEventListener('change',()=>{if(!check.checked&&ui.completionController){ui.completionController.abort();if(page.isConnected&&checkoutUI===ui&&!ui.completed)status('Test checkout completion was cancelled. Review the acknowledgement and click again when ready.');}syncCompletionButton();publish();});
    complete.addEventListener('click',async()=>{
      if(!check.checked||complete.disabled||ui.completed||ui.completionController||!page.isConnected||checkoutUI!==ui||state.pageKind!=='checkout')return;
      complete.disabled=true;const version=navigationVersion,controller=new AbortController(),preferences=JSON.stringify(readPreferences()),shipping=ui.shipping;ui.completionController=controller;
      try{
        const fresh=readCart();if(JSON.stringify(fresh)!==JSON.stringify(cart))throw Error('Your bag changed. Reopen test checkout before completing.');
        await verifyBag(fresh,controller.signal);
        if(controller.signal.aborted||version!==navigationVersion||state.pageKind!=='checkout'||!page.isConnected||checkoutUI!==ui)return;
        if(!check.checked)throw Error('The test acknowledgement changed. Review it and click Complete test checkout again.');
        if(JSON.stringify(readCart())!==JSON.stringify(cart))throw Error('Your bag changed during the check. Reopen test checkout.');
        if(JSON.stringify(readPreferences())!==preferences||ui.shipping!==shipping)throw Error('Your gift or demo shipping choice changed. Review it before completing.');
        const receipt=node('section',null,'receipt');receipt.append(node('span','TEST COMPLETE','eyebrow'),node('h2','Your selection came together.'),node('p','This was a local checkout simulation. Your test bag is preserved. No order was placed, no payment was taken and no details were sent.'));page.append(receipt);check.disabled=true;ui.completed=true;ui.step='complete';status('Test checkout complete. No real purchase was made.');publish();
      }catch(e){if(page.isConnected&&checkoutUI===ui&&state.pageKind==='checkout'&&version===navigationVersion)status(controller.signal.aborted?'Test checkout completion was cancelled. Review the acknowledgement and click again when ready.':e.message||'The test checkout could not be completed.');}
      finally{if(ui.completionController===controller)ui.completionController=null;syncCompletionButton();}
    });
    main.append(page);if(full){renderServicePanel(page);void loadServices();}state.current=null;commitPage('checkout','',push);status('Test checkout is ready. No payment is connected.');return {ok:true,action:'checkout',message:'The checked test checkout is ready. No real order or payment is possible.',snapshot:snapshot()};
  }
  function runCheckoutControl(action){
    const ui=checkoutUI;if(!ui?.page?.isConnected||state.pageKind!=='checkout'||ui.completed)return controlResult(action.type,false,'Open the current checked test checkout first.');
    const before=checkoutChoice();
    if(action.type==='checkout-option'){if(action.option!=='shipping'||!['standard','express'].includes(action.value))return controlResult(action.type,false,'Choose only the labelled demo standard or express interface option.');restoreCheckoutChoice({...before,shipping:action.value});rememberChange(action.type,{kind:'checkout'},before,checkoutChoice());status('Demo '+action.value+' is selected. The shop still confirms real shipping availability, rates and timing.');publish();return controlResult(action.type,true,notice.textContent);}
    const step=action.type==='checkout-complete'?'confirm':action.step;if(!['review','shipping','confirm'].includes(step))return controlResult(action.type,false,'Choose a current test-checkout step.');restoreCheckoutChoice({...before,step},{reveal:true});if(action.type!=='checkout-complete')rememberChange(action.type,{kind:'checkout'},before,checkoutChoice());publish();const message=action.type==='checkout-complete'?'Review the simulation, tick its acknowledgement and click Complete test checkout yourself. No order or payment is possible.':'The '+step+' step of the test checkout is in view.';status(message);return controlResult(action.type,true,message,action.type==='checkout-complete'?{requiredCustomerClick:'Complete test checkout',completed:false,orderPlaced:false}:{});
  }
  async function execute(action,options={}){
    if(!action||typeof action!=='object'||Array.isArray(action)||options.signal?.aborted)return {ok:false,action:'',message:'That action was cancelled or unavailable.'};
    const type=action.type;
    if(type==='home'){
      const result=await searchCatalogue('',options,{sort:'featured',filter:'all'});
      if(!result.ok||options.signal?.aborted)return controlResult(type,false,'The homepage could not be opened. Please try again.');
      if(!document.hidden)try{window.scrollTo?.({top:0,behavior:'instant'});}catch{}
      status('You are on the homepage.');return {...result,action:type,message:notice.textContent,snapshot:snapshot()};
    }
    if(type==='catalogue-more'){const result=await clickActionButton(main.querySelector('[data-catalogue-more]'),options);return controlResult(type,result.ok,result.message,{cancelled:result.cancelled===true});}
    if(type==='story-open'){const ui=currentProductUI();if(!ui||action.handle!==ui.product.handle)return controlResult(type,false,'Open that exact product before its reviewed story.');const result=await clickActionButton(main.querySelector('[data-story-open]'),options);return controlResult(type,result.ok,result.message,{cancelled:result.cancelled===true});}
    if(type==='cancel-review'){const ui=currentProductUI(),result=await clickActionButton(ui?.review?.querySelector('[data-cancel-review]'),options);return controlResult(type,result.ok,result.message);}
    if(type==='undo')return undoLastChange(options);
    if(type==='close-image'){const closed=retireImage({notify:true,restoreFocus:true});return controlResult(type,closed,closed?'The enlarged image is closed. Your gallery and options are preserved.':'There is no enlarged image open.');}
    if(type==='back'||type==='forward')return navigateHistory(type==='back'?-1:1,options);
    if(type==='gallery'){if(!Number.isInteger(action.index)||action.index<1||action.index>50||action.handle!==undefined&&!validHandle(action.handle))return controlResult(type,false,'Choose one exact published image number.');if(action.handle&&action.handle!==state.currentHandle&&!await openProduct(action.handle,{signal:options.signal}))return controlResult(type,false,'That exact product gallery could not be opened.');const ui=currentProductUI(),ok=!options.signal?.aborted&&!!ui&&(!action.handle||action.handle===ui.product.handle)&&ui.selectImage(action.index);return controlResult(type,ok,ok?'Product image '+action.index+' is shown.':'That image is not in the current published gallery.');}
    if(type==='scroll'&&action.direction!==undefined){if(action.section!==undefined||action.handle!==undefined||!['up','down','top','bottom'].includes(action.direction)||document.hidden)return controlResult(type,false,'Choose up, down, top or bottom for this current page.');const viewport=window.innerHeight||800,height=Math.max(document.documentElement.scrollHeight||0,document.body.scrollHeight||0),now=window.scrollY||0,target=action.direction==='top'?0:action.direction==='bottom'?Math.max(0,height-viewport):Math.max(0,now+(action.direction==='up'?-1:1)*Math.max(240,viewport*.75));try{window.scrollTo?.({top:target,behavior:'instant'});}catch{return controlResult(type,false,'This page could not be scrolled.');}publish();return controlResult(type,true,'The page has scrolled '+action.direction+'.');}
    if(type==='close-options'&&state.pageKind==='bag'){closeBagOptions();return controlResult(type,true,'The bag option menus are closed. Your choices are preserved.');}
    if(type==='bag-options'||type==='bag-highlight'){if(options.signal?.aborted||state.pageKind!=='bag')return controlResult(type,false,'Open the current bag before showing its controls.');if(type==='bag-options'){const ok=openBagOptions(action.lineId,action.optionName);return controlResult(type,ok,ok?'The exact bag item’s published choices are open.':'That exact bag option menu is not available.');}const section=action.section,ok=['title','options','quantity','engraving','price'].includes(section)&&typeof action.lineId==='string'&&revealBagControl(action.lineId,section,action.optionName);if(ok)publish();return controlResult(type,ok,ok?'The exact bag item’s '+section+' control is highlighted and in view.':'That exact bag item control is not available.');}
    if(type==='customize-brief'){if(!validPrivateText(action.text))return controlResult(type,false,'Keep the exact design brief within 300 characters.');if(!main.querySelector('.service-panel'))renderServicePanel(main);const form=main.querySelector('[data-store-section=customize] form')||main.querySelector('form[data-store-section=customize]'),field=form?.querySelector('textarea[name=idea]');if(!field||field.disabled)return controlResult(type,false,'That visible design brief is not available.');field.value=action.text;field.dispatchEvent(new Event('input',{bubbles:true}));form.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));if(readPreferences().customIdea!==action.text||field.value!==action.text)return controlResult(type,false,'The exact design brief could not be confirmed.');publish();return controlResult(type,true,action.text?'Your exact design brief is saved only in this test session.':'The test-session design brief is cleared.');}
    if(type==='bag-note'){if(state.pageKind!=='bag'||!validPrivateText(action.text))return controlResult(type,false,'Open the current bag and use exact note wording within 300 characters.');const result=await setGiftPreferences({type,giftNote:action.text});return {...result,message:result.ok?(action.text?'Your exact note is saved only in this test session.':'Your test-session note is cleared.'):result.message};}
    if(type==='bag-clear'){const cart=readCart(),ids=cartIdentities(cart),provided=action.lineIds,view=snapshot().bagControls;if(state.pageKind!=='bag'||!view.countKnown||!view.linesComplete||!Array.isArray(provided)||!provided.length||provided.length>50||new Set(provided).size!==provided.length||provided.length!==ids.length||provided.some(id=>!ids.includes(id)))return controlResult(type,false,'Review the complete current bag before emptying it.');writeCart([],[]);rememberChange(type,{kind:'bag'},{cart,ids},{cart:[],ids:[]});status('Your test bag is empty. No order was placed.');publish();return controlResult(type,true,notice.textContent,{cartChanged:true});}
    if(['options','close-options','select-option','product-quantity','set-engraving','add','review-add'].includes(type))return runProductControl(action,options);
    if(['bag-quantity','bag-remove','bag-select-option','bag-set-engraving'].includes(type))return editBag(action,options);
    if(type==='gift-preferences')return setGiftPreferences(action);
    if(['checkout-step','checkout-option','checkout-complete'].includes(type))return runCheckoutControl(action);
    if(type==='search')return searchCatalogue(action.query,options,action);
    if(type==='sort'||type==='filter'){
      const value=type==='sort'?action.sort:action.filter;if(!(type==='sort'?SORTS:FILTERS).has(value))return {ok:false,action:type,message:'That collection control is unavailable.'};
      const before=collectionChoice(),priorView=before?captureView():null,leavingPage=state.pageKind!=='catalogue',resetScope=leavingPage||type==='filter'&&value!=='available'&&(state.search!==''||state.collectionSource!=='browse');
      if(!state.browseVerifiedAt&&(resetScope||state.collectionSource==='browse'&&state.search===''))return searchCatalogue('',options,{sort:type==='sort'?value:state.sort,filter:type==='filter'?value:'all'},type==='filter');
      cancelPending();if(resetScope)restoreBrowse();state[type==='sort'?'sort':'filter']=value;state.limit=PAGE_SIZE;if(type==='filter')reviseDiscovery();if(leavingPage){restoreCollection();commitPage('catalogue','',options.push!==false);}else{restoreCollection();publish();}
      rememberChange(type,{kind:'collection',view:priorView},before,collectionChoice());
      const pieces=filtered().slice(0,state.limit),message=type==='sort'?'The loaded collection is sorted.':pieces.length?'The '+(value==='all'?'full loaded collection':value)+' is ready.':state.pageInfo.hasNextPage?'No '+(value==='all'?'pieces':value)+' on these loaded pages yet. Explore more pieces to continue.':'No checked '+(value==='all'?'pieces':value)+' in this loaded collection. Try a new live search.';
      focusSection('catalogue',false);status(message);return {ok:true,action:type,live:state.verifiedAt>0,checkedAt:state.verifiedAt,products:pieces.map(projection),snapshot:snapshot(),message};
    }
    if(type==='open'){const opened=await openProduct(action.handle,{signal:options.signal,section:SECTIONS.has(action.section)?action.section:undefined,revealRequested:true}),ok=opened&&state.current?.handle===action.handle;return {ok,action:type,...(ok?{live:true,checkedAt:state.verifiedAt,products:[projection(state.current)],snapshot:snapshot()}:{}),message:ok?'The checked product details are open.':'That piece could not be checked.'};}
    if(type==='highlight'||type==='scroll'||type==='zoom'){
      const section=type==='zoom'?'image':action.section;if(!SECTIONS.has(section))return {ok:false,action:type,message:'That page section is unavailable.'};
      const revealFocus=trackRevealFocus(focusedElement());try{
      if(action.handle&&(action.handle!==state.currentHandle||!state.current)){if(!await openProduct(action.handle,{signal:options.signal}))return {ok:false,action:type,message:'That piece could not be checked.'};}
      if(options.signal?.aborted||document.hidden||!revealFocus.allowed())return {ok:false,action:type,message:'The earlier page reveal was cancelled. Your newer view is preserved.'};
      if(section==='shipping'||section==='gifts'||section==='customize'||section==='offers'){await showService(section);return {ok:true,action:type,snapshot:snapshot(),message:'The '+section+' section is open.'};}
      if(type==='zoom'){const ok=state.current?zoomImage(state.current):false;return {ok,action:type,snapshot:snapshot(),message:ok?'The live product image is enlarged.':'Open a piece with a published image first.'};}
      const ok=focusSection(section,type==='highlight',{immediate:true});return {ok,action:type,snapshot:snapshot(),message:ok?'The '+section+' section is highlighted and in view.':'That section is not on the current page.'};
      }finally{revealFocus.dispose();}
    }
    if(type==='bag'){renderBag();return {ok:true,action:type,message:'Your session-only test bag is open.',snapshot:snapshot()};}
    if(type==='checkout')return renderCheckout({signal:options.signal});
    if(type==='gift'||type==='customize')return showService(type==='gift'?'gifts':'customize');
    return {ok:false,action:clean(type,30),message:'That website action is not available in this preview.'};
  }
  function presentProducts(products,options={}){
    const paging=options.paging,paged=!!paging&&paging.schema===1&&['initial','more','rest'].includes(paging.mode)&&typeof paging.query==='string'&&paging.query.length<=180&&paging.query===(options.searchQuery||'')&&Number.isInteger(paging.displayCount)&&paging.displayCount===products?.length&&paging.displayCount>=0&&paging.displayCount<=(paging.mode==='rest'?500:6)&&typeof paging.keepCurrent==='boolean'&&(!paging.keepCurrent||paging.displayCount===0);
    if(paging&&!paged)return {ok:false,action:'present',message:'Please ask again for the current matches.'};
    if(!Array.isArray(products)||products.length>(paged&&paging.mode==='rest'?500:24)||products.some(p=>!validProduct(p,p?.handle)))return {ok:false,action:'present',message:'The checked product selection could not be displayed.'};
    if(options.query!==undefined&&(typeof options.query!=='string'||options.query.length>250)||options.searchQuery!==undefined&&(typeof options.searchQuery!=='string'||options.searchQuery.length>250)||options.filter!==undefined&&!FILTERS.has(options.filter)||options.sort!==undefined&&!SORTS.has(options.sort)||options.checkedAt!==undefined&&!checkedTime(options.checkedAt))return {ok:false,action:'present',message:'The checked search context could not be displayed.'};
    if(paged&&paging.keepCurrent)return {ok:true,action:'present',live:true,checkedAt:options.checkedAt,products:[],snapshot:snapshot(),message:'You’ve seen every available match here.'};
    const discovery=options.query!==undefined||options.searchQuery!==undefined||options.filter!==undefined;
    saveCurrentView();const prior=currentProductUI(),priorSelection=productSelection(),checked=products.map(p=>{const time=checkedTime(p.checkedAt)||checkedTime(options.checkedAt)||Date.now(),existing=checkedInventory.get(p.handle),stripped=existing&&existing.product.id===p.id&&existing.checkedAt>=time&&(!p.image&&existing.product.image||!Array.isArray(p.images)||p.images.length<(existing.product.images||[]).length||!Array.isArray(p.options)||p.options.length<(existing.product.options||[]).length||p.variants.length<existing.product.variants.length||p.variantsComplete!==true);if(stripped)return remember(publicCopy(existing.product));cacheChecked(p,time);return remember(p);});cancelPending();state.verifiedAt=checkedTime(options.checkedAt)||(checked.length?Math.min(...checked.map(p=>checkedTime(p.checkedAt)||Date.now())):Date.now());reviseDiscovery();
    if(!discovery&&state.pageKind==='product'&&checked.length===1&&checked[0].handle===state.currentHandle){
      const fresh=checked[0],keys=['id','handle','options','variants','variantsComplete','cartHold','recommendationHold','partsOnly','currency'],signature=(p,fields)=>JSON.stringify(fields.map(key=>p?.[key])),compatible=!!prior&&signature(prior.product,keys)===signature(fresh,keys),renderKeys=keys.concat(['title','type','description','url','image','imageAlt','images']),same=compatible&&signature(prior.product,renderKeys)===signature(fresh,renderKeys),initialFocus=focusedElement();
      if(same){prior.invalidateReview();Object.assign(prior.product,fresh);state.current=prior.product;prior.updateSelection(false);}
      else{const selection=prior?.product.id===fresh.id?{...priorSelection,focus:initialFocus===prior.select?'variant':initialFocus===prior.quantityInput?'quantity':null}:null;state.current=fresh;renderProduct(fresh,selection);if(selection?.focus&&!document.hidden&&!initialFocus.isConnected&&focusedElement()===document.body)(selection.focus==='variant'?productUI.select:productUI.quantityInput).focus({preventScroll:true});}
      publish();return {ok:true,action:'present',live:true,checkedAt:state.verifiedAt,products:checked.map(projection),snapshot:snapshot(),message:'The current checked piece is in view.'};
    }
    // Only explicit checked search metadata supplies listing/material/price
    // criteria. A conversational query may include recipient or gift wording.
    state.products=checked;state.search=clean(options.searchQuery||'',250);state.checkedSearch=state.search;state.collectionSource='presented';state.filter=options.filter||'all';if(options.sort)state.sort=options.sort;state.limit=paged?Math.max(PAGE_SIZE,paging.displayCount):PAGE_SIZE;state.pageInfo={hasNextPage:false,endCursor:null};restoreCollection();history.replaceState({britesSandboxView:navigationKeys[navigationIndex]},'','/concierge-sandbox.html');publish();document.dispatchEvent(new CustomEvent('brites-concierge:page'));
    const plan=activeSearchPlan();return {ok:true,action:'present',live:true,checkedAt:state.verifiedAt,products:filtered().map(projection),...(plan?{searchCriteria:publicSearchCriteria(plan)}:{}),snapshot:snapshot(),message:'The checked selection is displayed in the boutique.'};
  }
  window.BritesSandboxStorefront=Object.freeze({capabilities:CONTROL_CAPABILITIES,snapshot,execute,presentProducts,readProduct:async handle=>validHandle(handle)?cachedRead(handle):null,getInventory:()=>[...checkedInventory.values()].filter(row=>row.expiresAt>Date.now()).map(row=>publicCopy(row.product)),inventoryStatus,preloadInventory});
  window.BritesSandboxNavigate=handle=>openProduct(handle,{revealRequested:true});
  document.querySelector('#guide-request-form')?.addEventListener('submit',event=>{event.preventDefault();void sendGuideCommand(document.querySelector('#guide-request')?.value||'');});
  renderGuideCommands();
  document.addEventListener('click',event=>{
    const control=event.target.closest?.('[data-store-action]');if(control&&!event.defaultPrevented){const type=control.dataset.storeAction;event.preventDefault();if(type==='bag')void execute({type:'bag'});else if(type==='gifts')void execute({type:'gift'});else if(type==='customize')void execute({type:'customize'});else if(type==='browse')focusSection('catalogue',false);else if(type==='collection')void execute({type:'search',query:''});return;}
    const anchor=event.target.closest?.('a[href]');if(!anchor||event.defaultPrevented||event.button!==0||event.ctrlKey||event.metaKey||event.shiftKey||event.altKey||anchor.target==='_blank')return;
    const target=new URL(anchor.href,location.href);if(target.origin!==location.origin||target.pathname!=='/concierge-sandbox.html')return;const handle=target.searchParams.get('product');if(!handle)return;event.preventDefault();void openProduct(handle,{focusFrom:event.detail===0&&document.activeElement===anchor?anchor:null,revealFrom:event.detail>0&&main.contains(anchor)?anchor:null});
  });
  function attended(target){const card=target?.closest?.('[data-product-handle]'),handle=card?.dataset.productHandle;return main.contains(card)&&validHandle(handle)&&known.has(handle)?handle:'';}
  function focus(handle){if(state.focusedHandle===handle)return;state.focusedHandle=handle;publish();}
  function inConcierge(target){return !!target?.closest?.('brites-concierge');}
  function attendControl(target,passive=false){const ui=currentProductUI();if(inConcierge(target))return;const row=state.pageKind==='bag'&&target?.closest?.('[data-bag-line]');if(row&&main.contains(row)){const group=target.closest('[data-bag-option-group]'),quantity=target.closest('.bag-quantity'),next={lineId:row.dataset.bagLine,kind:quantity?'quantity':group?'option':'line',optionName:group?.dataset.bagOptionGroup||null};if(JSON.stringify(next)!==JSON.stringify(attendedControl)||attendedControlNode!==target){attendedControl=next;attendedControlNode=target;state.activeSection='bag';publish();}return;}let next=null,sectionChanged=false;if(ui&&main.contains(target)){const group=target?.closest?.('[data-option-name]'),name=group?.dataset.optionName;if(ui.groups.some(g=>g.name===name))next={handle:ui.product.handle,kind:'option',optionName:name};else if(target===ui.select||ui.select.contains(target))next={handle:ui.product.handle,kind:'variant'};else if(target===ui.quantityInput)next={handle:ui.product.handle,kind:'quantity'};const section=target?.closest?.('[data-store-section]')?.dataset.storeSection;if(section&&state.activeSection!==section){state.activeSection=section;sectionChanged=true;}}if(passive&&!next&&!sectionChanged&&attendedControl?.kind==='variant'&&attendedControlNode?.isConnected&&(document.activeElement===ui?.select||inConcierge(document.activeElement)))return;if(sectionChanged||JSON.stringify(next)!==JSON.stringify(attendedControl)||next&&attendedControlNode!==target){attendedControl=next;attendedControlNode=next?target:null;publish();}}
  function releaseFocus(){if(focusTimer||!state.focusedHandle)return;focusTimer=setTimeout(()=>{focusTimer=0;focus('');},90);}
  document.addEventListener('pointerover',event=>{if(inConcierge(event.target)){clearTimeout(focusTimer);focusTimer=0;return;}attendControl(event.target,true);const next=attended(event.target);if(next){clearTimeout(focusTimer);focusTimer=0;focus(next);}else releaseFocus();});
  document.addEventListener('pointerout',event=>{if(inConcierge(event.relatedTarget)){clearTimeout(focusTimer);focusTimer=0;return;}if(!event.target.closest?.('[data-product-handle]'))return;const next=attended(event.relatedTarget);if(next){clearTimeout(focusTimer);focusTimer=0;focus(next);}else releaseFocus();});
  document.addEventListener('focusin',event=>{clearTimeout(focusTimer);focusTimer=0;if(inConcierge(event.target))return;attendControl(event.target);focus(attended(event.target));});
  document.addEventListener('pointerdown',event=>{if(inConcierge(event.target))return;clearTimeout(focusTimer);focusTimer=0;attendControl(event.target);const next=attended(event.target);if(next)focus(next);});
  document.addEventListener('focusout',event=>{if(!main.contains(event.target))return;const next=attended(event.relatedTarget);focus(next);});
  document.addEventListener('brites:cart-updated',event=>{if(event.detail?.sandbox!==true)return;updateBag();if(state.pageKind==='bag')renderBag({push:false});});
  let visibleWindow='';addEventListener('scroll',()=>{if(state.pageKind!=='catalogue')return;const next=snapshot().visiblePieces.map(p=>p.id).join(',');if(next!==visibleWindow){visibleWindow=next;publish();}},{passive:true});
  addEventListener('popstate',event=>{const key=event.state?.britesSandboxView,index=navigationKeys.indexOf(key),traversal=historyTraversal;if(traversal&&key===traversal.key){const current=traversal.version===navigationVersion&&navigationKeys[navigationIndex]===key;traversal.finish();if(current)return;const query=state.pageKind==='product'?'?product='+encodeURIComponent(state.currentHandle):state.pageKind==='bag'?'?cart=1':state.pageKind==='checkout'?'?checkout=1':'';history.replaceState({britesSandboxView:navigationKeys[navigationIndex]},'','/concierge-sandbox.html'+query);return;}if(index>=0){saveCurrentView();navigationIndex=index;void restoreSavedView(historyViews.get(navigationKeys[index]));return;}cancelPending();const p=new URLSearchParams(location.search);if(p.get('product'))void openProduct(p.get('product'),{push:false});else if(p.has('cart'))renderBag({push:false});else if(p.has('checkout'))void renderCheckout({push:false});else if(collectionView)void restoreSavedView(collectionView);else{state.filter='all';if(!state.browseVerifiedAt){void searchCatalogue('',{push:false},{filter:'all'});return;}restoreBrowse();reviseDiscovery();restoreCollection();commitPage('catalogue','',false);}});
  const initialKey='view-'+(++historySequence);navigationKeys.push(initialKey);history.replaceState({britesSandboxView:initialKey},'',location.href);saveCurrentView();void preloadInventory();
  updateBag();const params=new URLSearchParams(location.search);
  if(params.has('cart')){renderBag({push:false});return;}
  if(params.has('checkout')){await renderCheckout({push:false});return;}
  if(params.has('product')){if(savedTabView?.kind==='product'){state.search=savedTabView.search;state.filter=savedTabView.filter;state.sort=savedTabView.sort;}const handle=params.get('product');if(savedTabView?.kind==='product'&&savedTabView.handle===handle&&savedTabView.selection)productViews.set(handle,savedTabView.selection);await openProduct(handle,{push:false});return;}
  controls();
  const initialVersion=navigationVersion;
  try{
    const data=await catalogueStart();if(state.pageKind!=='catalogue'||navigationVersion!==initialVersion)return;
    const pageInfo=checkedPageInfo(data);state.products=checkedProducts(data);state.browse=state.products.slice();state.pageInfo=pageInfo;state.browsePageInfo={...pageInfo};state.verifiedAt=Date.now();state.browseVerifiedAt=state.verifiedAt;applySeed(data);drawGrid();publish();if(savedTabView?.kind==='catalogue'&&navigationVersion===initialVersion){const earlier=savedTabView;await preloadInventory();if(state.pageKind==='catalogue'&&navigationVersion===initialVersion){const products=earlier.handles.map(handle=>checkedInventory.get(handle)).filter(row=>row&&Date.now()-row.checkedAt<=300000).map(row=>publicCopy(row.product));if(products.length||!earlier.handles.length){presentProducts(products,{searchQuery:earlier.search,sort:earlier.sort,filter:earlier.filter,paging:{schema:1,mode:'rest',query:earlier.search,displayCount:products.length,keepCurrent:false}});}}}if(full){renderServiceStrip();void loadServices();}
  }catch{if(inventoryRows.size&&state.pageKind==='catalogue'){inventoryBrowse=true;state.browse=[...inventoryRows.values()];state.browsePageInfo={hasNextPage:false,endCursor:null};state.browseVerifiedAt=Date.now();restoreBrowse();drawGrid();publish();if(full){renderServiceStrip();void loadServices();}}else{const summary=main.querySelector('#result-summary');if(summary)summary.textContent='The live selection is temporarily unavailable. Try Search again.';else main.append(node('p','The live selection is temporarily unavailable. You can still explore the shop.'));}}
})();
