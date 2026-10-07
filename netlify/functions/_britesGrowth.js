'use strict';

// Shared, product-bound knowledge. Ranking/sales evidence and competitive
// recommendations stay in authenticated storage; public projections are explicit.
const crypto = require('node:crypto');
const milestoneDiscovery = require('./_britesMilestoneDiscovery');
const storefront = require('./_britesStorefront');
const CATALOG_QUERY = `query GrowthProducts($query:String!, $after:String){products(first:50,query:$query,after:$after){nodes{id handle title status onlineStoreUrl descriptionHtml productType tags updatedAt featuredImage{url altText} images(first:16){nodes{url altText}} options{name values} variants(first:100){nodes{id title sku price availableForSale selectedOptions{name value}} pageInfo{hasNextPage endCursor}}}pageInfo{hasNextPage endCursor}}shop{name currencyCode}}`;
const STOP_AT = Date.parse('2026-10-11T02:00:00Z');
const clean = (v,n=500) => String(v==null?'':v).replace(/\u0000/g,'').trim().slice(0,n);
const hash = v => crypto.createHash('sha256').update(typeof v==='string'?v:JSON.stringify(v)).digest('hex');
const textOf = v => clean(String(v||'').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,' ').replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi,' ').replace(/<[^>]+>/g,' ').replace(/&nbsp;/g,' ').replace(/&amp;/g,'&').replace(/&#39;/g,"'").replace(/&quot;/g,'"').replace(/\s+/g,' '),24000);
function publicUrl(v,storeOnly=false){try{const u=new URL(String(v));if(u.protocol!=='https:'||u.username||u.password||u.port||!u.hostname.includes('.')||/^(localhost|127\.|0\.|10\.|192\.168\.|169\.254\.|172\.(1[6-9]|2\d|3[01])\.)/.test(u.hostname))return null;if(storeOnly&&!['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname))return null;return u.href;}catch{return null;}}
function sameSecret(a,b){if(!a||!b)return false;return crypto.timingSafeEqual(Buffer.from(hash(String(a))),Buffer.from(hash(String(b))));}
function catalogueImageUrl(value){const normalized=typeof value==='string'&&value.startsWith('//')?'https:'+value:value,url=publicUrl(normalized);if(!url)return null;const u=new URL(url);return ['britesjewelry.com','www.britesjewelry.com','cdn.shopify.com'].includes(u.hostname)?url:null;}
function catalogueImages(value,fallback=null){const rows=Array.isArray(value)?value:[],images=[],seen=new Set();for(const raw of [...(fallback?[fallback]:[]),...rows].slice(0,40)){const url=catalogueImageUrl(typeof raw==='string'?raw:raw?.url||raw?.src);if(!url||seen.has(url))continue;seen.add(url);images.push({url,altText:shopperCatalogueText(raw?.altText||raw?.alt||'',300)||null});if(images.length>=16)break;}return images;}
function namespace(env){const n=env.BRITES_GROWTH_NAMESPACE||'Brites_Growth_Sandbox';if(!/^Brites_Growth_(Sandbox|Live)$/.test(n))throw Error('Invalid research namespace.');return n;}
function makeDb(env){const {Firestore}=require('@google-cloud/firestore');if(!env.FIREBASE_PROJECT_ID||!env.FIREBASE_CLIENT_EMAIL||!env.FIREBASE_PRIVATE_KEY)throw Error('Research storage is not configured.');return new Firestore({projectId:env.FIREBASE_PROJECT_ID,credentials:{client_email:env.FIREBASE_CLIENT_EMAIL,private_key:env.FIREBASE_PRIVATE_KEY.replace(/\\n/g,'\n')}});}
function normalizeProduct(p,currency='USD',now=Date.now()){
  const url=publicUrl(p.onlineStoreUrl,true);if(!url||p.status!=='ACTIVE')throw Error('Product is not published in the live online store.');
  const images=catalogueImages(p.images?.nodes,p.featuredImage);
  return {id:p.id,handle:clean(p.handle,180),title:clean(p.title,300),url,currency,description:textOf(p.descriptionHtml),type:clean(p.productType,100),tags:(p.tags||[]).map(t=>clean(t,100)).slice(0,80),image:images[0]?.url||null,imageAlt:images[0]?.altText||'',images,options:(p.options||[]).map(o=>({name:clean(o.name,100),values:(o.values||[]).map(v=>clean(v,100))})),variants:(p.variants?.nodes||[]).map(v=>({id:v.id,numericId:String(v.id).split('/').pop(),title:clean(v.title,200),sku:clean(v.sku,200),price:Number(v.price),available:!!v.availableForSale,options:v.selectedOptions||[]})).filter(v=>Number.isFinite(v.price)&&v.price>=0),variantsComplete:p.variants?.pageInfo?.hasNextPage===false,updatedAt:typeof p.updatedAt==='string'?clean(p.updatedAt,100):null,checkedAt:now};
}
function shopperCatalogueText(value,limit=24000){
  return clean(value,limit).split(/(?<=[.!?])\s+/).filter(sentence=>{
    if(/https?:\/\/|(?:[a-z0-9-]+\.)+[a-z]{2,63}(?:\/|\b)/i.test(sentence))return false;
    return !/(?:system|developer|assistant|author|internal|hidden)\s+(?:prompt|message|instructions?)|\bignore (?:all |any )?(?:prior|previous|system|developer) instructions?\b|\b(?:assistant|concierge|model)\s+(?:must|should|shall|needs? to)\b/i.test(sentence);
  }).join(' ').trim();
}
function shopperCatalogueField(value,limit){const normalized=clean(value,limit);return normalized&&shopperCatalogueText(normalized,limit)===normalized?normalized:null;}
function productProjection(p){
  const title=shopperCatalogueField(p.title,300),type=shopperCatalogueField(p.type,100);
  const variants=(p.variants||[]).flatMap(v=>{const variantTitle=shopperCatalogueField(v?.title,200),options=(v?.options||[]).flatMap(o=>{const name=shopperCatalogueField(o?.name,100),value=shopperCatalogueField(o?.value,100);return name&&value?[{name,value}]:[];});return variantTitle&&options.length===(v?.options||[]).length?[{id:v.id,numericId:v.numericId,title:variantTitle,price:v.price,available:v.available,options}]:[];});
  const images=catalogueImages(p.images,{url:p.image,altText:p.imageAlt});
  return {id:p.id,handle:p.handle,title,type,url:p.url,image:images[0]?.url||null,imageAlt:images[0]?.altText||null,images,currency:p.currency,description:shopperCatalogueText(p.description),partsOnly:/\b(?:charms?|components?|add[ -]?ons?)\b/i.test(type||''),options:(p.options||[]).flatMap(o=>{const name=shopperCatalogueField(o?.name,100),values=(o?.values||[]).map(value=>shopperCatalogueField(value,100)).filter(Boolean);return name?[{name,values}]:[];}),variants,variantsComplete:p.variantsComplete,checkedAt:p.checkedAt,cartHold:p.cartHold===true,recommendationHold:p.recommendationHold===true,meaningHold:p.meaningHold===true};
}
function validIdentity(id){return /^gid:\/\/shopify\/Product\/\d+$/.test(String(id));}
function validateDossier(d,p,now=Date.now()){
  const errors=[],warnings=[];const bad=x=>errors.push(x);
  if(!d||typeof d!=='object'||Array.isArray(d))return {ok:false,status:'rejected',errors:['A dossier object is required.'],warnings};
  for(const field of ['sources','facts','competitors','meanings','recommendations','buyerIntents'])if(d[field]!=null&&!Array.isArray(d[field]))bad(field+' must be an array.');
  for(const field of ['sources','facts','competitors','meanings','recommendations'])if(Array.isArray(d[field])&&d[field].some(x=>!x||typeof x!=='object'||Array.isArray(x)))bad(field+' entries must be objects.');
  if(errors.length)return {ok:false,status:'rejected',errors,warnings};
  if(!d||d.schema!==1||!validIdentity(d.productId)||d.productId!==p?.id||d.handle!==p?.handle)bad('Exact live product identity is required.');
  if(!d?.sources?.length||d.sources.length>40)bad('Provide 1–40 inspected sources.');
  const ids=new Set();for(const s of d?.sources||[]){if(!/^[a-zA-Z0-9:_-]{1,100}$/.test(s.id||'')||ids.has(s.id))bad('Source IDs must be valid and unique.');ids.add(s.id);if(!publicUrl(s.url)||typeof s.title!=='string'||!clean(s.title,300)||typeof s.excerpt!=='string'||!clean(s.excerpt,2000))bad('Every source requires a public HTTPS URL, title and inspected excerpt.');if(!Number.isFinite(s.checkedAt)||s.checkedAt>now+60000||now-s.checkedAt>30*86400000)bad('Source review timestamp is missing or stale.');if(s.reviewed!==true)bad('Sources must be inspected, not inferred from search snippets.');}
  const cited=(r,field)=>{if(!r||!Array.isArray(r.sourceIds)||!r.sourceIds.length||r.sourceIds.some(id=>!ids.has(id)))bad(field+' needs available source IDs.');};
  const canonical=v=>{try{const u=new URL(v);return u.hostname.replace(/^www\./,'')+u.pathname.replace(/\/$/,'');}catch{return null;}};
  for(const c of d?.facts||[]){cited(c,'Product fact');const src=(d.sources||[]).find(s=>s.id===c?.sourceIds?.[0]);if(typeof c?.quote!=='string'||!clean(c?.quote,2000)||typeof src?.excerpt!=='string'||!src.excerpt.includes(c.quote))bad('A factual claim needs an exact quote from its bound source.');if(c?.productId!==p?.id)bad('Facts cannot transfer between products.');if(!src||canonical(src.url)!==canonical(p?.url))bad('Commercial product facts require the exact live product page as their bound source.');}
  for(const c of d?.competitors||[]){cited(c,'Competitor');if(!publicUrl(c?.url)||!clean(c?.name,180))bad('Competitor offer URL/name is required.');if(c?.price!=null&&(!Number.isFinite(c.price)||c.price<0||!/^[A-Z]{3}$/.test(c.currency||'')))bad('A competitor price needs a valid amount and currency.');if(!['unknown','known','estimate'].includes(c?.spend?.status))bad('Competitor spending must be explicitly labelled unknown, known or estimate.');if(c?.spend?.status==='known'){const s=c.spend;cited(s,'Known competitor spending');const source=(d.sources||[]).find(x=>x.id===s.sourceIds?.[0]);if(!clean(s.quote,2000)||(typeof source?.excerpt!=='string'||!source.excerpt.includes(s.quote))||!/\b(?:spent|spend|spending|advertising budget|ad budget|media budget|advertising expenditure|marketing expenditure)\b/i.test(s.quote))bad('Known competitor spending needs an exact quote that explicitly discloses spending; otherwise label it unknown.');}if(c?.spend?.status==='estimate'&&(!c.spend.method||!Array.isArray(c.spend.assumptions)||!c.spend.assumptions.length||!Number.isFinite(c.spend.low)||!Number.isFinite(c.spend.high)||c.spend.low<0||c.spend.high<c.spend.low))bad('Spending estimates need a method, assumptions and bounds.');}
  for(const m of d?.meanings||[]){cited(m,'Meaning/history');if(!clean(m?.text,1500)||!clean(m?.context,300)||m?.kind!=='interpretation')bad('Symbolism needs reviewed text, cultural context and an interpretation label.');}
  for(const r of d?.recommendations||[]){if(!['ads','keywords','negatives','listing','concierge'].includes(r.channel)||r.basis!=='hypothesis'||!clean(r.action,2000)||!clean(r.measure,1000))bad('Recommendations need a channel, hypothesis label, action and measurement.');cited(r,'Recommendation');}
  if(!(d?.competitors||[]).length)warnings.push('Competitor offer research is incomplete.');
  if(!(d?.recommendations||[]).length)warnings.push('No actionable recommendations yet.');
  if(!(d?.buyerIntents||[]).length)warnings.push('Buyer-intent research is incomplete.');
  if(!(d?.meanings||[]).length)warnings.push('No sourced symbolism/history is available.');
  const status=errors.length?'rejected':warnings.some(w=>/Competitor|recommendations|Buyer/.test(w))?'draft':'approved';
  return {ok:errors.length===0,status,errors:[...new Set(errors)],warnings};
}
const STORY_ROOT_FIELDS=new Set(['schema','productId','handle','productUrl','baseDossierVersion','sources','meanings']);
const STORY_STORED_FIELDS=new Set([...STORY_ROOT_FIELDS,'status','version','savedAt','productCheckedAt']);
const STORY_SOURCE_FIELDS=new Set(['id','url','title','excerpt','checkedAt','reviewed']);
const STORY_MEANING_FIELDS=new Set(['text','context','kind','sourceIds']);
const STORY_NEUTRAL_HOSTS=['metmuseum.org','amnh.org','rmg.co.uk','sciencemuseumgroup.org.uk','si.edu','loc.gov','historymuseum.ca','montereybayaquarium.org','vam.ac.uk','themorgan.org','gulbenkian.pt'];
function exactFields(value,allowed){return value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>allowed.has(key));}
function storySafeText(value,limit){
  const valueText=typeof value==='string'?clean(value,limit):'';
  if(!valueText||/https?:\/\//i.test(valueText)||/(?:system|developer|assistant|author|internal|hidden)\s+(?:prompt|message|instructions?)|(?:prompt|instruct(?:ion)?)\s+(?:the\s+)?(?:assistant|concierge|model)|\b(?:assistant|concierge|model)\s+(?:must|should|shall|needs? to)\b|\bignore (?:all |any )?(?:prior|previous|system|developer) instructions?\b/i.test(valueText))return null;
  return valueText;
}
function neutralStoryUrl(value,competitorHosts=[]){
  const href=publicUrl(value);if(!href)return null;
  const u=new URL(href),host=u.hostname.toLowerCase().replace(/^www\./,'');
  const blocked=['britesjewelry.com','etsy.com','amazon.com','amazon.ca','ebay.com','ebay.ca','walmart.com','walmart.ca','shopify.com'];
  if(blocked.some(x=>host===x||host.endsWith('.'+x))||competitorHosts.some(x=>host===x||host.endsWith('.'+x)||x.endsWith('.'+host))||/(?:^|\/)products?(?:\/|$)|(?:^|\/)(?:shop|cart|checkout)(?:\/|$)/i.test(u.pathname))return null;
  const institutional=/\.(?:gov|edu)$|\.(?:gov\.uk|gc\.ca|ac\.uk)$/.test(host)||STORY_NEUTRAL_HOSTS.some(x=>host===x||host.endsWith('.'+x));
  return institutional?href:null;
}
function validateStorySupplement(value,{product,dossier,now=Date.now()}={}){
  const errors=[],bad=message=>errors.push(message);
  if(!exactFields(value,STORY_ROOT_FIELDS))return {ok:false,errors:['Story supplement fields are not allowed.']};
  if(value.schema!==1||!validIdentity(value.productId)||value.productId!==product?.id||value.handle!==product?.handle)bad('Exact current product identity is required.');
  const boundUrl=publicUrl(value.productUrl,true),currentUrl=publicUrl(product?.url,true);
  if(!boundUrl||boundUrl!==currentUrl)bad('The exact current product URL is required.');
  if(!Number.isFinite(product?.checkedAt)||product.checkedAt>now+60000||now-product.checkedAt>5*60000)bad('The live product check is stale.');
  if(dossier?.status!=='approved'||dossier?.productId!==product?.id||dossier?.handle!==product?.handle||typeof dossier?.version!=='string'||value.baseDossierVersion!==dossier.version)bad('The approved current base dossier version is required.');
  if(!Array.isArray(value.sources)||value.sources.length<1||value.sources.length>2)bad('Provide 1–2 reviewed neutral sources.');
  if(!Array.isArray(value.meanings)||value.meanings.length<1||value.meanings.length>2)bad('Provide 1–2 reviewed meanings.');
  const competitorHosts=(Array.isArray(dossier?.competitors)?dossier.competitors:[]).flatMap(item=>{try{return [new URL(item.url).hostname.toLowerCase().replace(/^www\./,'')];}catch{return [];}});
  const sourceIds=new Set(),baseSourceIds=new Set((Array.isArray(dossier?.sources)?dossier.sources:[]).map(source=>source?.id)),sources=[];
  for(const source of Array.isArray(value.sources)?value.sources:[]){
    if(!exactFields(source,STORY_SOURCE_FIELDS)){bad('Story source fields are not allowed.');continue;}
    const id=/^[a-zA-Z0-9:_-]{1,100}$/.test(source.id||'')?source.id:null,url=neutralStoryUrl(source.url,competitorHosts),title=storySafeText(source.title,300),excerpt=storySafeText(source.excerpt,2000);
    if(!id||sourceIds.has(id)||baseSourceIds.has(id))bad('Source IDs must be valid, unique and distinct from base sources.');else sourceIds.add(id);
    if(!url||!title||!excerpt)bad('Every source needs a neutral public URL, title and inspected excerpt.');
    if(source.reviewed!==true)bad('Sources must be reviewed.');
    if(!Number.isFinite(source.checkedAt)||source.checkedAt>now+60000||now-source.checkedAt>30*86400000)bad('Source review timestamp is missing or stale.');
    if(id&&url&&title&&excerpt)sources.push({id,url,title,excerpt,checkedAt:source.checkedAt,reviewed:true});
  }
  const meanings=[];
  for(const meaning of Array.isArray(value.meanings)?value.meanings:[]){
    if(!exactFields(meaning,STORY_MEANING_FIELDS)){bad('Story meaning fields are not allowed.');continue;}
    const text=storySafeText(meaning.text,1500),context=storySafeText(meaning.context,300),ids=Array.isArray(meaning.sourceIds)?meaning.sourceIds:[];
    if(!text||!context||meaning.kind!=='interpretation')bad('Meanings require reviewed interpretation text and context.');
    if(ids.length<1||ids.length>2||ids.some(id=>!sourceIds.has(id))||new Set(ids).size!==ids.length)bad('Meanings require 1–2 available source IDs.');
    if(text&&context&&meaning.kind==='interpretation'&&ids.length)meanings.push({text,context,kind:'interpretation',sourceIds:[...ids]});
  }
  return {ok:errors.length===0,errors:[...new Set(errors)],value:{schema:1,productId:value.productId,handle:value.handle,productUrl:boundUrl,baseDossierVersion:value.baseDossierVersion,sources,meanings}};
}
function createShopify({env,fetch=globalThis.fetch,now=Date.now}){
  let access=null,expires=0,publicCurrency=null,publicCurrencyAt=0,publicCurrencyPending=null;
  const publicBase='https://britesjewelry.com';
  const adminConfigured=()=>/^[a-z0-9-]+\.myshopify\.com$/.test(env.SHOPIFY_STORE||'')&&!!env.SHOPIFY_CLIENT_ID&&typeof env.SHOPIFY_CLIENT_SECRET==='string'&&env.SHOPIFY_CLIENT_SECRET.length>=16&&!/redact|\*{2,}|^[-x•●]+$/i.test(env.SHOPIFY_CLIENT_SECRET);
  async function publicJson(path){const r=await fetch(publicBase+path,{headers:{Accept:'application/json','Cache-Control':'no-cache'},signal:AbortSignal.timeout(18000),redirect:'error'});if(r.status===404)return null;if(!r.ok)throw Error('The published storefront could not be checked.');const data=await r.json();if(!data||typeof data!=='object')throw Error('The published storefront response is invalid.');return data;}
  async function storefrontCurrency(){if(publicCurrency&&now()-publicCurrencyAt<60000)return publicCurrency;if(publicCurrencyPending)return publicCurrencyPending;publicCurrencyPending=(async()=>{const cart=await publicJson('/cart.js');if(!/^[A-Z]{3}$/.test(cart?.currency||''))throw Error('Current storefront currency could not be verified.');publicCurrency=cart.currency;publicCurrencyAt=now();return publicCurrency;})();try{return await publicCurrencyPending;}finally{publicCurrencyPending=null;}}
  function fromPublic(p,currency,priceInCents){
    if(!p||!/^\d+$/.test(String(p.id))||!/^[-_a-z0-9]{1,180}$/.test(p.handle||''))throw Error('The published product identity could not be verified.');
    const optionNames=(p.options||[]).map((o,i)=>typeof o==='string'?o:o.name||'Option '+(i+1));
    const variants=(p.variants||[]).map(v=>({id:'gid://shopify/ProductVariant/'+v.id,title:v.title,sku:v.sku,price:priceInCents?Number(v.price)/100:Number(v.price),availableForSale:v.available===true,selectedOptions:optionNames.map((name,i)=>({name,value:clean(v.options?.[i]??v['option'+(i+1)],100)}))}));
    const images=catalogueImages(p.images,p.image||{url:p.featured_image,altText:p.title});
    const result=normalizeProduct({id:'gid://shopify/Product/'+p.id,handle:p.handle,title:p.title,status:'ACTIVE',onlineStoreUrl:publicBase+'/products/'+p.handle,descriptionHtml:p.description||p.body_html,productType:p.type||p.product_type,tags:Array.isArray(p.tags)?p.tags:String(p.tags||'').split(',').map(x=>x.trim()),updatedAt:p.updated_at,featuredImage:images[0],images:{nodes:images},options:optionNames.map((name,i)=>({name,values:[...new Set(variants.map(v=>v.selectedOptions[i]?.value).filter(Boolean))]})),variants:{nodes:variants,pageInfo:{hasNextPage:variants.length>=250}}},currency,now());
    result.source=priceInCents?'published_product_ajax':'published_catalogue_json';return result;
  }
  async function publicByHandle(handle){if(!/^[a-z0-9_-]{1,180}$/.test(handle||''))throw Error('Invalid product handle.');const [p,currency]=await Promise.all([publicJson('/products/'+handle+'.js'),storefrontCurrency()]);if(!p)return null;if(p.handle!==handle)throw Error('Published product does not match the requested handle.');return fromPublic(p,currency,true);}
  async function publicSearch(terms){
    const tokens=[...new Set((clean(terms,250).toLowerCase().match(/[\p{L}\p{N}-]+/gu)||[]).filter(t=>!['or','and'].includes(t)).slice(0,8))];if(!tokens.length)return {products:[],pageInfo:{hasNextPage:false,endCursor:null},access:'public_storefront'};
    async function suggestions(q){const params=new URLSearchParams({q,'resources[type]':'product','resources[limit]':'10','resources[options][unavailable_products]':'hide'}),d=await publicJson('/search/suggest.json?'+params);return d?.resources?.results?.products||[];}
    let found=await suggestions(tokens.join(' '));if(!found.length&&tokens.length>1)found=(await Promise.all(tokens.slice(0,3).map(suggestions))).flat();
    const handles=[...new Set(found.flatMap(p=>{if(/^[a-z0-9_-]{1,180}$/.test(p.handle||''))return [p.handle];try{const u=new URL(p.url,publicBase),m=u.pathname.match(/^\/(?:[a-z]{2}(?:-[a-z]{2})?\/)?products\/([a-z0-9_-]+)\/?$/i);return publicUrl(u.href,true)&&m?[m[1]]:[];}catch{return [];}}))].slice(0,15);
    const checks=await Promise.allSettled(handles.map(publicByHandle)),verified=checks.filter(x=>x.status==='fulfilled'&&x.value).map(x=>x.value);
    if(handles.length&&!verified.length&&checks.some(x=>x.status==='rejected'))throw Error('The matching published pieces could not be checked.');
    return {products:verified,pageInfo:{hasNextPage:false,endCursor:null},access:'public_storefront',unverifiedCount:checks.filter(x=>x.status==='rejected').length};
  }
  async function publicProducts(query,after){
    if(query){const h=/^handle:([a-z0-9_-]{1,180})$/.exec(query);if(h)return {products:[await publicByHandle(h[1])].filter(Boolean),pageInfo:{hasNextPage:false,endCursor:null},access:'public_storefront'};return publicSearch(query.replace(/\b(?:title|tag|sku|product_type):/g,''));}
    const page=/^public:\d+$/.test(after||'')?Number(after.slice(7)):1;if(page<1||page>10000)throw Error('Invalid public catalogue page.');
    // The public product list is an observed, read-only Shopify shop route,
    // rather than the authenticated Admin API. Mirror only public records and
    // refresh individual product.js before any shopper action.
    const d=await publicJson('/products.json?limit=250&page='+page);if(!Array.isArray(d?.products))throw Error('Published catalogue pagination is unavailable.');
    const currency=await storefrontCurrency();
    return {products:d.products.map(p=>fromPublic(p,currency,false)),pageInfo:{hasNextPage:d.products.length===250,endCursor:d.products.length===250?'public:'+(page+1):null},access:'public_catalogue'};
  }
  async function browse(cursor=null){
    // A separate public cursor never becomes an Admin GraphQL cursor. Each
    // call reads one bounded published page, and never advances mirror state.
    if(cursor!=null&&cursor!==''&&!/^storefront:[1-9]\d{0,3}$/.test(String(cursor)))throw Error('Invalid storefront catalogue cursor.');
    const page=cursor?Number(String(cursor).slice(11)):1;if(page<1||page>200)throw Error('Invalid storefront catalogue cursor.');
    const limit=60,[data,currency]=await Promise.all([publicJson('/products.json?limit='+limit+'&page='+page),storefrontCurrency()]);
    if(!Array.isArray(data?.products)||data.products.length>limit)throw Error('Published storefront pagination is unavailable.');
    const products=data.products.map(p=>fromPublic(p,currency,false)),hasNextPage=data.products.length===limit&&page<200;
    return {products,pageInfo:{hasNextPage,endCursor:hasNextPage?'storefront:'+(page+1):null},access:'public_catalogue',checkedAt:now()};
  }
  async function token(){if(access&&now()<expires-60000)return access;const store=env.SHOPIFY_STORE;if(!/^[a-z0-9-]+\.myshopify\.com$/.test(store||'')||!env.SHOPIFY_CLIENT_ID||!env.SHOPIFY_CLIENT_SECRET)throw Error('Live Shopify catalogue access is not configured.');const r=await fetch('https://'+store+'/admin/oauth/access_token',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({grant_type:'client_credentials',client_id:env.SHOPIFY_CLIENT_ID,client_secret:env.SHOPIFY_CLIENT_SECRET}),signal:AbortSignal.timeout(12000)});const d=await r.json();if(!r.ok||!d.access_token)throw Error('Shopify authorization failed; queue reconnection and continue independent work.');access=d.access_token;expires=now()+Number(d.expires_in||86400)*1000;return access;}
  async function products(query,after=null){if(!adminConfigured()||String(after||'').startsWith('public:'))return publicProducts(query,after);try{const r=await fetch('https://'+env.SHOPIFY_STORE+'/admin/api/2026-07/graphql.json',{method:'POST',headers:{'Content-Type':'application/json','X-Shopify-Access-Token':await token()},body:JSON.stringify({query:CATALOG_QUERY,variables:{query:'status:active AND published_status:published'+(query?' AND ('+query+')':''),after}}),signal:AbortSignal.timeout(20000)});const d=await r.json();if(!r.ok||d.errors?.length)throw Error('Live catalogue query failed.');return {products:d.data.products.nodes.map(p=>normalizeProduct(p,d.data.shop.currencyCode,now())),pageInfo:d.data.products.pageInfo,access:'admin'};}catch{return publicProducts(query,null);}}
  async function search(terms){if(!adminConfigured())return publicSearch(terms);const tokens=clean(terms,250).toLowerCase().match(/[\p{L}\p{N}-]+/gu)||[];const stems=[...new Set(tokens.slice(0,8).map(t=>t.length>4&&t.endsWith('s')&&!t.endsWith('ss')?t.slice(0,-1):t))];const query=stems.flatMap(t=>['title:'+t+'*','tag:'+t+'*']).join(' OR ');return products(query);}
  async function byHandle(handle){if(!/^[a-z0-9_-]{1,180}$/.test(handle||''))throw Error('Invalid product handle.');const r=await products('handle:'+handle);return r.products.find(p=>p.handle===handle)||null;}
  return {products,search,byHandle,browse};
}
function createGrowthService({db,env={},shopify,now=Date.now}){
  const ns=namespace(env), col=suffix=>db.collection(ns+'_'+suffix), state=()=>col('State').doc('control'), pid=id=>hash(id).slice(0,40);
  async function setup(){const ref=state();await db.runTransaction(async tx=>{const s=await tx.get(ref);if(!s.exists)tx.set(ref,{schema:1,enabled:true,stopAt:STOP_AT,createdAt:now(),catalogueCursor:null,catalogueComplete:false,aiEnabled:false,aiDailyUsdCap:1});});return (await ref.get()).data();}
  async function getProduct(id){const s=await col('Products').doc(pid(id)).get();return s.exists?s.data():null;}
  async function saveProducts(items){for(let i=0;i<items.length;i+=100){const b=db.batch();for(const p of items.slice(i,i+100)){if(!validIdentity(p.id)||!publicUrl(p.url,true))throw Error('Invalid catalogue product.');b.set(col('Products').doc(pid(p.id)),p);}await b.commit();}}
  async function syncCatalogue(){const ctrl=await setup();if(!ctrl.enabled||now()>=Math.min(ctrl.stopAt||STOP_AT,STOP_AT))return {stopped:true};const result=await shopify.products('',ctrl.catalogueCursor||null);await saveProducts(result.products);await state().set({catalogueCursor:result.pageInfo.hasNextPage?result.pageInfo.endCursor:null,catalogueComplete:!result.pageInfo.hasNextPage,lastCatalogueSyncAt:now()}, {merge:true});return {count:result.products.length,complete:!result.pageInfo.hasNextPage};}
  async function importRanks(rows){if(!Array.isArray(rows)||rows.length>200)throw Error('Import at most 200 ranked entries.');for(const row of rows){if(!Number.isInteger(row.rank)||row.rank<1||row.rank>200||!clean(row.title,300))throw Error('Rank and title are required.');if(row.productId&&!validIdentity(row.productId))throw Error('Invalid matched product ID.');const ref=col('Queue').doc('rank-'+String(row.rank).padStart(3,'0'));await db.runTransaction(async tx=>{const prior=await tx.get(ref);const old=prior.exists?prior.data():{};const data={rank:row.rank,title:clean(row.title,300),handle:clean(row.handle,180),sku:clean(row.sku,500),orders:Number(row.orders)||null,theme:clean(row.theme,180),updatedAt:now()};if(!prior.exists)Object.assign(data,{status:'pending_match',attempts:0,createdAt:now()});const changedProduct=!!(row.productId&&row.productId!==old.productId),changedHandle=!!(prior.exists&&old.handle&&data.handle&&old.handle!==data.handle);if(row.productId)data.productId=row.productId;if(changedProduct||changedHandle)Object.assign(data,{status:row.productId?'pending_research':'pending_match',productId:row.productId||null,attempts:0,retryAt:0,leaseToken:null,leaseOwner:null,leaseUntil:0,dossierVersion:null,completedAt:null,match:null});tx.set(ref,data,{merge:true});});}return {imported:rows.length};}
  async function claim(kind='research',owner='work-controller'){const ctrl=await setup();if(!ctrl.enabled||now()>=Math.min(ctrl.stopAt||STOP_AT,STOP_AT))return {stopped:true};const all=await col('Queue').get();const candidates=all.docs.map(d=>({ref:d.ref,...d.data()})).filter(x=>x.rank<=100&&x.status!=='complete'&&x.status!=='blocked'&&Number(x.attempts||0)<5&&!(x.retryAt>now())&&(!x.leaseUntil||x.leaseUntil<=now())).sort((a,b)=>a.rank-b.rank);for(const x of candidates){const token=crypto.randomUUID();const result=await db.runTransaction(async tx=>{const s=await tx.get(x.ref);const row=s.data();if(row.status==='complete'||row.status==='blocked'||row.leaseUntil>now()||row.retryAt>now())return null;const patch={leaseToken:token,leaseOwner:clean(owner,100),leaseUntil:now()+45*60000,status:row.productId?'researching':'matching',attempts:Number(row.attempts||0)+1,claimedAt:now(),kind:clean(kind,40)};tx.update(x.ref,patch);return {...row,...patch,id:x.ref.id};});if(result)return result;}return {empty:true};}
  async function release(id,token,patch={}){
    const ref=col('Queue').doc(clean(id,100));let product=null,match=null;
    if(patch.productId){
      if(!validIdentity(patch.productId))throw Error('Invalid matched product ID.');
      product=await getProduct(patch.productId);
      if(!product||product.handle!==patch.handle||!publicUrl(product.url,true)||now()-product.checkedAt>3600000||product.checkedAt>now()+60000)throw Error('Match must agree with a recently checked live product and handle.');
      const m=patch.match;
      if(!m||!['exact_handle','exact_sku','exact_title','manual'].includes(m.method)||typeof m.evidence!=='string'||!clean(m.evidence,1000)||m.evidence.length>1000||!Number.isFinite(m.checkedAt)||now()-m.checkedAt>15*60000||m.checkedAt>now()+60000)throw Error('A product match needs a method, bounded inspected evidence and fresh verification time.');
      match={method:m.method,evidence:clean(m.evidence,1000),checkedAt:m.checkedAt};
    }
    return db.runTransaction(async tx=>{
      const s=await tx.get(ref),row=s.exists?s.data():null;
      if(!row||!sameSecret(row.leaseToken,token)||!Number.isFinite(row.leaseUntil)||row.leaseUntil<=now())throw Error('Work lease is missing, expired or owned by another writer.');
      const p={leaseToken:null,leaseOwner:null,leaseUntil:0,updatedAt:now()};
      if(patch.error){p.lastError=clean(patch.error,2000);p.status=patch.blocked?'blocked':row.productId?'pending_research':'pending_match';p.retryAt=now()+Math.min(6*3600000,60000*Math.pow(2,Number(row.attempts)||1));}
      if(product){
        if(match.method==='exact_handle'&&row.handle!==product.handle)throw Error('Exact handle evidence does not match the ranked entry.');
        if(match.method==='exact_title'&&textOf(row.title).toLowerCase()!==textOf(product.title).toLowerCase())throw Error('Exact title evidence does not match the ranked entry.');
        if(match.method==='exact_sku'&&!(product.variants||[]).some(v=>v.sku&&(String(row.sku||'').split(/[\n,;|]+/).map(x=>x.trim())).includes(v.sku)))throw Error('Exact SKU evidence does not match a live product variant.');
        Object.assign(p,{productId:product.id,handle:product.handle,match,status:'pending_research'});
      }
      tx.update(ref,p);return {ok:true};
    });
  }
  async function saveDossier(d){const product=await getProduct(d.productId);const v=validateDossier(d,product,now());if(!v.ok)return {validation:v};const record={...d,validation:v,status:v.status,version:hash(d),savedAt:now()};const ref=col('Research').doc(pid(d.productId));await db.runTransaction(async tx=>{const prior=await tx.get(ref);if(prior.exists&&prior.data().status==='approved'&&v.status!=='approved')throw Error('A partial draft cannot overwrite approved research.');if(prior.exists)tx.set(col('ResearchVersions').doc(pid(d.productId)+'-'+prior.data().version),prior.data());tx.set(ref,record);});if(v.status==='approved'){const q=await col('Queue').where('productId','==',d.productId).get();const b=db.batch();for(const x of q.docs)b.update(x.ref,{status:'complete',dossierVersion:record.version,completedAt:now(),leaseToken:null,leaseUntil:0});await b.commit();}return {ok:true,productId:d.productId,version:record.version,validation:v};}
  async function readProductRecords(suffix,ids,limit){
    // These independent document reads have no write dependencies. Keep the
    // original deduplication/limit order and wait for each bounded chunk before
    // starting another. A failed read rejects the entire result, including holds.
    const selected=[...new Set(ids)].slice(0,limit).filter(validIdentity),out=[];
    for(let i=0;i<selected.length;i+=6){
      const snapshots=await Promise.all(selected.slice(i,i+6).map(id=>col(suffix).doc(pid(id)).get()));
      for(const snapshot of snapshots)if(snapshot.exists)out.push(snapshot.data());
    }
    return out;
  }
  async function research(ids){return readProductRecords('Research',ids,20);}
  async function productIssues(ids){return readProductRecords('ProductIssues',ids,100);}
  async function storySupplements(ids){
    if(ns!=='Brites_Growth_Sandbox')return [];
    return readProductRecords('StorySupplements',ids,20);
  }
  async function rebuildMilestoneIndex(){
    // This is a derived, private read index. It never approves research or
    // bypasses the exact live-product, version, hold, stock and public-meaning
    // checks in the shopper path. Rebuilding it is an authenticated sandbox
    // operation so broad milestone requests do not rescan every dossier.
    if(ns!=='Brites_Growth_Sandbox')throw Error('The milestone index requires the isolated sandbox namespace.');
    const snapshot=await col('Research').where('status','==','approved').limit(150).get(),approved=[];
    for(const doc of snapshot.docs||[]){const dossier=doc.data();if(validIdentity(dossier?.productId)&&/^[a-z0-9_-]{1,180}$/.test(dossier?.handle||'')&&/^[a-f0-9]{64}$/.test(dossier?.version||''))approved.push(dossier);}
    const supplements=await readProductRecords('StorySupplements',approved.map(x=>x.productId),150),supplementById=new Map(supplements.map(x=>[x?.productId,x])),names=milestoneDiscovery.milestoneNames(),records=new Map(names.map(name=>[name,[]]));
    for(const dossier of approved){
      const supplement=supplementById.get(dossier.productId),merged=mergeStorySupplements([dossier],supplement?[supplement]:[],[],now())[0]||dossier,baseMeanings=publicMeanings([dossier],[dossier.productId],now(),[]),mergedMeanings=publicMeanings([merged],[dossier.productId],now(),[]);
      for(const name of names){
        if(!milestoneDiscovery.matchingMeanings(mergedMeanings,name).length)continue;
        const supplementRequired=!milestoneDiscovery.matchingMeanings(baseMeanings,name).length;
        records.get(name).push({productId:dossier.productId,handle:dossier.handle,dossierVersion:dossier.version,supplementVersion:supplementRequired&&/^[a-f0-9]{64}$/.test(supplement?.version||'')?supplement.version:null});
      }
    }
    const builtAt=now(),entries=names.map(name=>{const candidates=records.get(name).sort((a,b)=>a.handle.localeCompare(b.handle));return [name,candidates];}),counts=Object.fromEntries(entries.map(([name,candidates])=>[name,candidates.length])),meta={schema:1,builtAt,approvedCount:approved.length,supplementCount:supplements.length,counts};
    if(typeof db.batch==='function'){
      // Firestore commits the eight milestone snapshots and the generation
      // marker atomically, so shopper reads never see a half-rebuilt index.
      const batch=db.batch();for(const [name,candidates] of entries)batch.set(col('State').doc('milestone-index-'+name),{schema:1,milestone:name,builtAt,candidates});batch.set(col('State').doc('milestone-index-meta'),meta);await batch.commit();
    }else{
      // Minimal in-memory adapters used by focused tests need not implement a
      // Firestore batch; production makeDb always does.
      await Promise.all(entries.map(([name,candidates])=>col('State').doc('milestone-index-'+name).set({schema:1,milestone:name,builtAt,candidates})));
      await col('State').doc('milestone-index-meta').set(meta);
    }
    return {schema:1,builtAt,approvedCount:approved.length,supplementCount:supplements.length,counts};
  }
  async function saveStorySupplement(value){
    if(ns!=='Brites_Growth_Sandbox')throw Error('Story supplements require the isolated sandbox namespace.');
    if(!exactFields(value,STORY_ROOT_FIELDS))throw Error('Story supplement fields are not allowed.');
    if(!validIdentity(value?.productId)||!/^[a-z0-9_-]{1,180}$/.test(value?.handle||''))throw Error('Exact current product identity is required.');
    if(!shopify||typeof shopify.byHandle!=='function')throw Error('A live product check is required.');
    const live=await shopify.byHandle(value.handle);if(!live||live.id!==value.productId)throw Error('The live product identity does not match.');
    const [base,issue]=await Promise.all([col('Research').doc(pid(value.productId)).get(),col('ProductIssues').doc(pid(value.productId)).get()]);
    const dossier=base.exists?base.data():null,holds=issue.exists?productIssueHolds(issue.data()):null;
    if(holds&&(holds.cartHold||holds.recommendationHold||holds.meaningHold))throw Error('An unresolved product hold prevents story supplements.');
    const checked=validateStorySupplement(value,{product:live,dossier,now:now()});if(!checked.ok)throw Error(checked.errors.join(' '));
    const version=hash(checked.value),record={...checked.value,status:'approved',version,savedAt:now(),productCheckedAt:live.checkedAt};
    await db.runTransaction(async tx=>{
      const [current,currentIssues]=await Promise.all([tx.get(col('Research').doc(pid(value.productId))),tx.get(col('ProductIssues').doc(pid(value.productId)))]);
      const currentDossier=current.exists?current.data():null,currentHolds=currentIssues.exists?productIssueHolds(currentIssues.data()):null;
      if(currentDossier?.status!=='approved'||currentDossier.productId!==value.productId||currentDossier.handle!==value.handle||currentDossier.version!==value.baseDossierVersion)throw Error('The base dossier version changed before the supplement was saved.');
      if(currentHolds&&(currentHolds.cartHold||currentHolds.recommendationHold||currentHolds.meaningHold))throw Error('An unresolved product hold prevents story supplements.');
      tx.set(col('StorySupplements').doc(pid(value.productId)),record);
    });
    return {productId:record.productId,handle:record.handle,baseDossierVersion:record.baseDossierVersion,supplementVersion:record.version};
  }
  async function catalogueCandidateHandles(value,limit=15){
    // The mirror is only a bounded recall index. Its product details are never
    // returned to a shopper; every handle is re-fetched from the published
    // storefront before type, price, currency or availability is considered.
    if(ns!=='Brites_Growth_Sandbox')return [];
    const cap=Math.max(1,Math.min(15,Number.isInteger(limit)?limit:15));
    const interests=plainList(value?.interests,4).filter(x=>/^[\p{L}\p{N}][\p{L}\p{N} -]{0,39}$/u.test(x));
    const queryTokens=(clean(value?.query,250).toLowerCase().match(/[\p{L}\p{N}-]+/gu)||[]).filter(x=>x.length>1&&!GENERIC.has(x));
    const bases=(interests.length?interests:queryTokens).slice(0,4),typeTag=clean(value?.type,30).toLowerCase().replace(/[^a-z0-9]+/g,'_').replace(/^_|_$/g,'');
    const terms=[...new Set([...bases,...(typeTag?bases.map(x=>x.replace(/[^\p{L}\p{N}]+/gu,'_')+'_'+typeTag):[])])].slice(0,8);if(!terms.length)return [];
    const rows=[];
    for(const term of terms){
      try{const snapshot=await col('Products').where('tags','array-contains',term).limit(60).get();for(const doc of snapshot.docs||[])rows.push(doc.data());}catch{}
    }
    const wantedType=clean(value?.type,30).toLowerCase(),excluded=new Set(plainList(value?.excludedTypes,8));
    const hintScore=row=>{const type=clean(row?.type,100).toLowerCase();return (wantedType&&typeMatches(row,wantedType)?4:0)-(excluded.has(type)?4:0);};
    return [...new Map(rows.filter(row=>/^[a-z0-9_-]{1,180}$/.test(row?.handle||'')).map(row=>[row.handle,row])).values()]
      .sort((a,b)=>hintScore(b)-hintScore(a)||clean(a.handle,180).localeCompare(clean(b.handle,180)))
      .slice(0,cap).map(row=>row.handle);
  }
  async function milestoneCandidateHandles(value,limit=12){
    // Recall hints must already be linked to a current approved dossier and a
    // reviewed public interpretation. Product mirror rows still provide hints
    // only: every returned handle is re-read from the live storefront before it
    // can be ranked, displayed or acted on.
    if(ns!=='Brites_Growth_Sandbox')return [];
    const plan=milestoneDiscovery.discoveryIntent(value),bounds=milestoneDiscovery.recallBounds();
    if(!plan)return [];
    const cap=Math.max(1,Math.min(bounds.liveHandles,Number.isInteger(limit)?limit:bounds.liveHandles));
    // Prefer the authenticated derived index. Its entries remain only hints:
    // current dossier/supplement versions and public meanings are rechecked
    // later, and every product is still fetched from the live storefront.
    let indexed=null;
    try{const hit=await col('State').doc('milestone-index-'+plan.milestone).get();if(hit.exists&&hit.data()?.schema===1&&hit.data()?.milestone===plan.milestone&&Array.isArray(hit.data()?.candidates))indexed=hit.data().candidates;}catch{}
    if(indexed){
      const selected=indexed.filter(hint=>validIdentity(hint?.productId)&&/^[a-z0-9_-]{1,180}$/.test(hint?.handle||'')&&/^[a-f0-9]{64}$/.test(hint?.dossierVersion||'')&&(hint.supplementVersion==null||/^[a-f0-9]{64}$/.test(hint.supplementVersion))).slice(0,bounds.mirrorReads),rows=await readProductRecords('Products',selected.map(x=>x.productId),bounds.mirrorReads);
      return milestoneDiscovery.rankLinkedCandidates(selected,rows,plan,value,cap);
    }
    let snapshot;try{snapshot=await col('Research').where('status','==','approved').limit(bounds.researchScan).get();}catch{return [];}
    const approved=[];
    for(const doc of snapshot.docs||[]){
      const dossier=doc.data();
      if(validIdentity(dossier?.productId)&&/^[a-z0-9_-]{1,180}$/.test(dossier?.handle||'')&&/^[a-f0-9]{64}$/.test(dossier?.version||''))approved.push(dossier);
    }
    const supplements=await readProductRecords('StorySupplements',approved.map(x=>x.productId),bounds.supplementReads),supplementById=new Map(supplements.map(supplement=>[supplement?.productId,supplement])),baseById=new Map(approved.map(dossier=>[dossier.productId,dossier]));
    const linked=[];
    for(const dossier of mergeStorySupplements(approved,supplements,[],now())){
      const meanings=publicMeanings([dossier],[dossier.productId],now(),[]);
      if(milestoneDiscovery.matchingMeanings(meanings,plan.milestone).length){const baseMeanings=publicMeanings([baseById.get(dossier.productId)],[dossier.productId],now(),[]),baseLinked=milestoneDiscovery.matchingMeanings(baseMeanings,plan.milestone).length>0,supplement=supplementById.get(dossier.productId);linked.push({productId:dossier.productId,handle:dossier.handle,dossierVersion:dossier.version,supplementVersion:!baseLinked&&/^[a-f0-9]{64}$/.test(supplement?.version||'')?supplement.version:null});}
    }
    linked.sort((a,b)=>a.handle.localeCompare(b.handle));
    const selected=linked.slice(0,bounds.mirrorReads),rows=await readProductRecords('Products',selected.map(x=>x.productId),bounds.mirrorReads);
    // A current, approved and version-linked interpretation is the evidence
    // boundary. Literal motif words in catalogue metadata improve ordering,
    // but must not discard an otherwise exact reviewed connection.
    return milestoneDiscovery.rankLinkedCandidates(selected,rows,plan,value,cap);
  }
  async function recordProductIssue(value){
    if(!validIdentity(value?.productId)||!Array.isArray(value.issues)||!value.issues.length||value.issues.length>20)throw Error('Provide an exact product ID and 1–20 reviewed issues.');
    const issues=value.issues.map(issue=>{
      if(!issue||!/^[a-z0-9_-]{1,100}$/.test(issue.id||'')||!['identity','style','options','material','matching','history','content'].includes(issue.kind)||typeof issue.detail!=='string'||!clean(issue.detail,3000)||!['open','resolved'].includes(issue.status||'open'))throw Error('Product issues need a valid ID, kind, detail and status.');
      const critical=['identity','style','options','material','matching'].includes(issue.kind),blocks=[...new Set([...(critical?['recommendation','cart']:['meaning']),...(Array.isArray(issue.blocks)?issue.blocks.filter(x=>['recommendation','cart','meaning'].includes(x)):[])])];
      const evidence=(Array.isArray(issue.evidence)?issue.evidence:[]).slice(0,12).map(e=>typeof e==='string'?{text:clean(e,1500),url:null,checkedAt:null}:{text:clean(e?.text||e?.quote,1500),url:publicUrl(e?.url)||null,checkedAt:Number.isFinite(e?.checkedAt)?e.checkedAt:null});
      return {id:issue.id,kind:issue.kind,detail:clean(issue.detail,3000),status:issue.status||'open',blocks,evidence,updatedAt:now()};
    });
    if(new Set(issues.map(x=>x.id)).size!==issues.length)throw Error('Product issue IDs must be unique.');
    const ref=col('ProductIssues').doc(pid(value.productId));
    const record=await db.runTransaction(async tx=>{const old=await tx.get(ref),prior=old.exists?old.data():{productId:value.productId,issues:[],createdAt:now()};const merged=new Map((Array.isArray(prior.issues)?prior.issues:[]).map(x=>[x.id,x]));for(const issue of issues)merged.set(issue.id,issue);if(merged.size>100)throw Error('Review the issue archive before adding more; unresolved holds cannot be discarded.');const data={productId:value.productId,issues:[...merged.values()],createdAt:prior.createdAt||now(),updatedAt:now()};tx.set(ref,data);return data;});
    return {ok:true,productId:value.productId,holds:productIssueHolds(record)};
  }
  async function status(){const [ctrl,q,rs,b]=await Promise.all([setup(),col('Queue').get(),col('Research').get(),col('Blockers').get()]);const rows=q.docs.map(x=>({id:x.id,...x.data()})).sort((a,b)=>a.rank-b.rank);return {schema:1,control:ctrl,queue:rows,counts:{ranked:rows.filter(x=>x.rank<=100).length,matched:rows.filter(x=>x.rank<=100&&x.productId).length,complete:rows.filter(x=>x.rank<=100&&x.status==='complete').length,approvedDossiers:rs.docs.filter(x=>x.data().status==='approved').length,drafts:rs.docs.filter(x=>x.data().status==='draft').length},blockers:b.docs.map(x=>({id:x.id,...x.data()})),at:now()};}
  async function block(value){if(!/^[a-z0-9-]{1,100}$/.test(value.id||''))throw Error('Invalid blocker ID.');await col('Blockers').doc(value.id).set({task:clean(value.task,300),detail:clean(value.detail,2500),status:clean(value.status||'queued_for_morning',100),at:now()},{merge:true});return {ok:true};}
  async function event(type,data={}){const allowed=['opened','dismissed','message','product_opened','cart_requested','cart_added','cart_failed','api_error','test'];if(!allowed.includes(type))throw Error('Invalid event.');await col('Events').add({type,productId:validIdentity(data.productId)?data.productId:null,scenario:clean(data.scenario,100),at:now()});return {ok:true};}
  async function rateLimit(key,limit=25){const bucket=Math.floor(now()/60000),ref=col('Rate').doc(hash(key+'-'+bucket));return db.runTransaction(async tx=>{const s=await tx.get(ref),count=s.exists?s.data().count:0;if(count>=limit)return false;tx.set(ref,{count:count+1,expiresAt:new Date((bucket+5)*60000)});return true;});}
  return {setup,getProduct,saveProducts,syncCatalogue,importRanks,claim,release,saveDossier,research,productIssues,storySupplements,saveStorySupplement,rebuildMilestoneIndex,catalogueCandidateHandles,milestoneCandidateHandles,recordProductIssue,status,block,event,rateLimit,col,state,namespace:ns};
}
function productIssueHolds(record){
  const open=(Array.isArray(record?.issues)?record.issues:[]).filter(x=>x&&x.status!=='resolved'),blocks=new Set(open.flatMap(x=>Array.isArray(x.blocks)?x.blocks:[]));
  // Even malformed older records must not remove a known critical hold.
  if(open.some(x=>['identity','style','options','material','matching'].includes(x.kind))){blocks.add('recommendation');blocks.add('cart');}
  if(open.some(x=>['history','content'].includes(x.kind)))blocks.add('meaning');
  return {productId:record?.productId,cartHold:blocks.has('cart'),recommendationHold:blocks.has('recommendation'),meaningHold:blocks.has('meaning')};
}
function applyProductIssues(products,records){const holds=new Map((Array.isArray(records)?records:[]).filter(x=>validIdentity(x?.productId)).map(x=>[x.productId,productIssueHolds(x)]));return (Array.isArray(products)?products:[]).map(p=>{const h=holds.get(p.id);return h?{...p,cartHold:h.cartHold,recommendationHold:h.recommendationHold,meaningHold:h.meaningHold}:{...p,cartHold:false,recommendationHold:false,meaningHold:false};});}
// Shopper preferences are deliberately a small public allowlist. Raw account,
// owner or repository state never enters the conversation projection.
const MOTIFS = [
  ['bunny','bunn(?:y|ies)|rabbits?'],['cardinal','cardinals?'],
  ['fire badge','firefighters?|firem[ae]n|fire badge'],['stethoscope','nurses?|doctors?|medical|stethoscopes?'],
  ['tooth','dentists?|dental|teeth|tooths?'],['apple book','teachers?|teaching'],
  ['apple','apples?'],['book','readers?|reading|books?|librarian'],['hummingbird','hummingbirds?'],
  ['sunflower','sunflowers?'],['cat','cats?|kittens?'],['dog','dogs?|pupp(?:y|ies)'],
  ['ballet','ballet|dancers?'],['skating','skates?|skating|skaters?'],['wolf','wolves|wolf'],
  ['dragonfly','dragonfl(?:y|ies)'],['phoenix','phoenix'],['rune','runes?|norse|vikings?'],
  ['sun','sun|sunshine'],['horse','horses?|equestrian'],['butterfly','butterfl(?:y|ies)'],
  ['bee','bees?|honeybee'],['dragon','dragons?'],['moon','moons?|lunar'],
  ['elephant','elephants?'],['dolphin','dolphins?'],['whale','whales?'],['fox','fox(?:es)?'],
  ['owl','owls?'],['turtle','turtles?'],['penguin','penguins?'],['sheep','sheep|lambs?'],
  ['deer','deer'],['frog','frogs?'],['fish','fish|fishing'],['bird','birds?'],
  ['circle','circles?|circular'],['heart','hearts?'],['tree','trees?|tree of life'],['flower','flowers?|floral'],
  ['lotus','lotus'],['rose','roses?(?!\\s+gold)'],['dandelion','dandelions?'],['paw','paws?'],
  ['star','stars?|celestial'],['snowflake','snowflakes?'],['music','music|musicians?|guitars?|piano'],
  ['volleyball','volleyball'],['baseball','baseball'],['basketball','basketball'],
  ['soccer','soccer|football'],['running','running|runners?|marathon'],
  ['science','science|scientists?|laboratory|chemistry'],['police badge','police badge|police|officers?'],
  ['movie slate','movie[ -]slates?|film[ -]slates?|clapper[ -]?boards?|clap[ -]?boards?|movies?|films?|cinema|filmmakers?|filmmaking'],
  ['theatre','theat(?:re|er)|actors?|drama'],['camera','photography|photographers?|cameras?'],
  ['anchor','anchors?|sailing|sailors?'],['airplane','airplanes?|pilots?|aviation'],
  ['mountain','mountains?|hiking|hikers?'],['leaf','leaves|leaf'],['clover','clovers?|shamrock'],
  ['zodiac','zodiac|astrology'],['initial','initials?'],['engraved','engrave[ds]?|engraving|personali[sz](?:ed|ation|e)|handwriting|handwritten|my (?:own )?writing']
];
const TYPES = ['necklace','earrings','bracelet','pendant','huggie','studs','ring','charm'];
const METALS = ['silver','gold','rose gold'];
const RECIPIENTS = ['mom','mother','dad','father','wife','husband','daughter','son','friend','sister','brother','grandmother','grandfather','partner','teacher','nurse','doctor','myself'];
const OCCASIONS = ['birthday','anniversary','graduation','memorial','christmas','wedding','retirement','thank you','just because'];
const PROFESSION_HINT=/^(?:teachers?|teaching|nurses?|doctors?|medical|dentists?|dental|firefighters?|firem[ae]n|librarian|scientists?|police|officers?|actors?|photographers?|musicians?|pilots?|sailors?)$/;
const GENERIC = new Set(('raise increase lower reduce expand between range limit price spend cost roughly approximately cheap cheaper expensive affordable i a an the for and or my me you your gift gifts find want wants wanted looking buy buying please someone jewellery jewelry under budget dollars dollar usd cad gbp eur is are was with to of her him them she he they it something show help can could would should like likes only actually prefer instead change stay within keep around no not without avoid dont don t do does doesn doesn t rather than but loves love enjoy enjoys into interested interest interests this that these those piece pieces one ones option options up less more below over above between maximum minimum max min from be at now need needs another anything give really very so also we our about what who how why when where choose choice lets let s have has happy thank thanks much tell meaningful thoughtful pretty beautiful amazing someone some look looking still either any same fresh start reset clear forget nothing different second third first fourth fifth sixth open take page website bag cart add adding compare comparison versus vs meaning means symbolize symbolism history story stories know learn explain next previous read what else new stop cancel go back sure yes okay ok please'.split(' ')));
const plainList = (v,n=8) => Array.isArray(v)?[...new Set(v.filter(x=>typeof x==='string').map(x=>clean(x,100).toLowerCase()))].slice(0,n):[];
const amount = v => typeof v==='number'&&Number.isFinite(v)&&v>=0&&v<=1000000?v:null;
const currencyCode = v => /^[A-Z]{3}$/.test(String(v||''))?String(v):null;

function shopperPreferences(saved={}) {
  const budget=amount(saved?.budget),unlimitedBudget=saved?.unlimitedBudget===true&&budget==null;
  return {
    query:clean(saved?.query,250),interests:plainList(saved?.interests),excludedInterests:plainList(saved?.excludedInterests),
    type:TYPES.includes(saved?.type)?saved.type:null,excludedTypes:plainList(saved?.excludedTypes).filter(x=>TYPES.includes(x)),
    metal:METALS.includes(saved?.metal)?saved.metal:null,excludedMetals:plainList(saved?.excludedMetals).filter(x=>METALS.includes(x)),
    recipient:RECIPIENTS.includes(saved?.recipient)?saved.recipient:null,occasion:OCCASIONS.includes(saved?.occasion)?saved.occasion:null,
    budget,minBudget:unlimitedBudget?null:amount(saved?.minBudget),unlimitedBudget,currency:currencyCode(saved?.currency)||'USD',
    budgetCurrency:unlimitedBudget?null:currencyCode(saved?.budgetCurrency)||(budget!=null?(currencyCode(saved?.currency)||'USD'):null),
    personalization:['engraving','handwriting'].includes(saved?.personalization)?saved.personalization:null,
    gifting:saved?.gifting===true,giftDiscovery:saved?.giftDiscovery===true,
    recipientSkipped:saved?.recipientSkipped===true,occasionSkipped:saved?.occasionSkipped===true,
    ...(milestoneDiscovery.validMilestone(saved?.milestone)?{milestone:saved.milestone}:{})
  };
}

function negatedAt(text,index) {
  // Contrast and punctuation terminate a negation. “Not gold, silver please”
  // therefore selects silver; “no gold or silver” excludes both. Preserve
  // “instead of” as a rejection while standalone “instead” starts a choice.
  const prefix=text.slice(0,index).split(/[,;.!?]|\b(?:but|instead(?!\s+of\b)|however|i (?:want|prefer|like)|she (?:likes|loves|prefers)|he (?:likes|loves|prefers)|they (?:like|love|prefer))\b/i).at(-1).replace(/\b(?:no (?:item )?(?:price|budget|spending) limit|without (?:a |the |my )?(?:item )?(?:price|budget|spending) limit)\b/gi,'').slice(-75);
  return /\b(?:not|no|never|without|avoid|except|excluding|rather than|instead\s+of|don'?t(?:\s+\w+){0,3}|doesn'?t(?:\s+\w+){0,3}|do not(?:\s+\w+){0,3}|does not(?:\s+\w+){0,3})\s+(?:\w+\s+){0,4}$/i.test(prefix);
}

function fieldMentions(text,pattern,canonical) {
  return [...text.matchAll(new RegExp('\\b(?:'+pattern+')\\b','g'))].map(m=>({value:canonical?canonical(m[0]):m[0],raw:m[0],index:m.index,negative:negatedAt(text,m.index)}));
}

// Semantic follow-ups share one route through preference preservation,
// displayed-card recall and response presentation. Bare “mean” is only an
// inquiry in a what-does/do question; “I mean silver instead” is a correction.
function semanticInquiry(message){
  const text=clean(message,2000).toLowerCase().replace(/[’‘]/g,"'");
  const nouns=fieldMentions(text,'meanings?|means|symboli[sz]\\w*|history|story|stories');
  if(nouns.some(hit=>!hit.negative))return true;
  return [...text.matchAll(/\bwhat\s+(?:do|does|did|would|can|could)\b[^.!?\n]{0,180}\bmean\b/g)].some(hit=>!negatedAt(text,hit.index));
}

// Social turns are conversation, not catalogue keywords. Keep this classifier
// deliberately narrow: a mixed greeting/product/action request stays on the
// existing live, grounded route. No page context is an action authorization.
function conversationReply(message){
  const raw=clean(message,2000).toLowerCase().replace(/[’‘]/g,"'");
  if(/https?:|www\.|\b(?:api keys?|credentials?|passwords?|system prompt|private (?:records|data)|owner data|repository|source code|sales history|customer (?:records|data)|checkout|check out|pay|payment)\b/.test(raw))return null;
  const text=raw.replace(/[^a-z0-9'\s]/g,' ').replace(/\s+/g,' ').trim();
  const greeting='(?:hello|hi|hey|hiya|greetings|good morning|good afternoon|good evening|hello there|hi there|hey there)';
  const body=text.replace(new RegExp('^'+greeting+'(?:\\s+(?:brite|brites|robot|friend))?(?:\\s+|$)'), '').trim();
  const social=(kind,reply,needsModelConversation=false)=>({kind,reply,needsModelConversation});
  if(new RegExp('^'+greeting+'(?:\\s+(?:brite|brites|robot|friend))?$').test(text))return social('greeting','Hello! Good to meet you. We can chat, explore a piece together, or find something with a personal meaning.');
  if(/^(?:how are you(?: doing)?(?: today)?|how's (?:it going|your day)|how is (?:it going|your day)|how have you been|are you doing (?:well|okay)|what's up|whats up)$/.test(body||text))return social('wellbeing','I’m here and ready to help—no coffee required. How’s your day going?');
  if(/^(?:thanks|thank you|thank you so much|thanks so much|that helps|that was helpful|you are helpful|you're helpful|nice to meet you|pleased to meet you|nice meeting you)(?: so much| a lot)?$/.test(body||text))return social('thanks','You’re very welcome. Take your time; I’m here when you want to continue.');
  if(/^(?:who are you|what are you|what's your name|what is your name|are you (?:a robot|human|an ai|real)|are you an? (?:ai|assistant)|do you have feelings)$/.test(body||text))return social('identity','I’m Brites’ AI guide—a little robot here to help you explore the shop and talk through ideas. I don’t have human feelings, but I can listen carefully and help you find what matters to you.');
  if(/^(?:what can you do|how can you help(?: me)?|can you (?:help me|navigate|navigate the (?:site|website)|show (?:me )?options|control the website)|how does this work)$/.test(body||text))return social('capabilities','We can chat about your ideas, explore the shop’s current pieces and their reviewed stories, or compare options. When you ask, I can help you open a piece or prepare its options; you choose and confirm any addition to your bag.');
  if(/^(?:tell (?:me )?(?:a |another )?joke|make me laugh|say something funny|can you tell (?:me )?a joke)$/.test(body||text))return social('humour','Why did the little robot bring a ladder? To take the conversation to another level. I’ll keep my day job.');
  if(/^(?:i(?:'m| am) (?:just looking|just browsing|not ready|taking my time)|just (?:looking|browsing)|let me (?:think|look|browse)|give me (?:a minute|a moment|some time)|no rush)$/.test(body||text))return social('pause','Of course. Take your time. I’ll stay out of the way until you’d like a hand.');
  if(/^(?:bye|goodbye|good night|goodnight|see you|talk (?:to you )?later)$/.test(body||text))return social('pause','Take care. I’m here whenever you’d like to come back.');
  if(/^(?:i(?:'m| am) )?(?:good|great|pretty good|doing well|not bad|fine)(?: today)?$/.test(body||text))return social('wellbeing','Good to hear. We can chat or explore whenever you feel like it.');
  if(/^(?:i(?:'m| am) )?(?:having a rough day|having a bad day|not doing (?:well|great)|having a hard time)$/.test(body||text))return social('general','I’m sorry it’s a rough day. We can take this slowly.',true);
  // Explicit small talk and unambiguously general questions may use the
  // separately bounded conversational endpoint. Jewellery/motif terms are
  // excluded so broad dialogue cannot invent an actual shop recommendation.
  const commerce=/\b(?:jewellery|jewelry|necklaces?|earrings?|bracelets?|pendants?|huggies?|studs?|rings?|charms?|silver|gold|metal|budget|prices?|stock|available|shipping|deliver|returns?|refund|engraving|personaliz\w*|pieces?|products?|options?|first|second|third|fourth|fifth|sixth|open|navigate|cart|bag|buy|order|gift|meaning|symbol\w*|history|story|stories)\b/;
  const hasMotif=MOTIFS.some(([,pattern])=>new RegExp('\\b(?:'+pattern+')\\b','i').test(text));
  if(!commerce.test(text)&&!hasMotif&&(/^(?:can we|let's|lets|i (?:want|would like) to) (?:just )?(?:chat|talk)\b/.test(body||text)||/^(?:tell me (?:something interesting|about yourself)|what(?:'s\s+|\s+(?:do you think|makes you|inspires you|is|are)\b)|why (?:is|are|do|does)|how (?:does|do))/.test(body||text)||/^(?:i(?:'m| am) (?:bored|tired|stressed|excited|happy|sad|nervous|lonely)|my day (?:is|was)|it(?:'s| is) been (?:a |an )?\w+ day)\b/.test(body||text)))return social('general','I’m listening. We can take this at your pace.',true);
  return null;
}

function applyBudgetMessage(p,text,explicitCurrency){
  const events=[],spans=[];
  const add=(match,value)=>{spans.push({index:match.index,end:match.index+match[0].length});if(!negatedAt(text,match.index))events.push({index:match.index,...value});};
  for(const m of text.matchAll(/\b(?:no (?:item )?(?:price|budget|spending) limit|without (?:a |the |my )?(?:item )?(?:price|budget|spending) limit|unlimited budget|budget (?:is )?unlimited|any price|(?:forget|remove|drop|ignore) (?:the |my )?(?:budget|price limit))\b/g))add(m,{unlimitedBudget:true,budget:null,minBudget:null});
  for(const m of text.matchAll(/\b(?:(?:don't|do not) know (?:the |my |a )?budget|not sure (?:about |of |what )?(?:the |my |a )?budget|(?:no|without) (?:set |decided )?budget yet|(?:haven't|have not) (?:set|decided) (?:the |my |a )?budget|budget (?:is )?(?:unknown|undecided))(?:\s+yet)?\b/g))add(m,{unlimitedBudget:false,budget:null,minBudget:null});
  const bareUnknown=/^(?:i have )?no budget[.!]?$/.exec(text);if(bareUnknown){spans.push({index:0,end:text.length});events.push({index:0,unlimitedBudget:false,budget:null,minBudget:null});}
  for(const m of text.matchAll(/\bbetween\s*(?:usd|cad|gbp|eur|c\$|ca\$|us\$|\$)?\s*(\d+(?:\.\d{1,2})?)\s*(?:and|to|-)\s*(?:usd|cad|gbp|eur|c\$|ca\$|us\$|\$)?\s*(\d+(?:\.\d{1,2})?)/g)){
    const minBudget=amount(Number(m[1])),budget=amount(Number(m[2]));
    if(minBudget!=null&&budget!=null&&minBudget<=budget&&!/^\s*(?:inches?|inch|cm|mm|days?|years?|business days?)\b/.test(text.slice(m.index+m[0].length)))add(m,{unlimitedBudget:false,budget,minBudget});
  }
  for(const m of text.matchAll(/(?:\b(?:under|below|budget|up to|less than|max(?:imum)?|at most|within|no more than|price limit)\s*(?:is|of|to|:)?\s*(?:usd|cad|gbp|eur|c\$|ca\$|us\$|\$)?\s*)(\d+(?:\.\d{1,2})?)/g)){const budget=amount(Number(m[1]));if(budget!=null)add(m,{unlimitedBudget:false,budget,minBudget:null});}
  const lone=/^(?:about|around)?\s*(?:usd|cad|gbp|eur|c\$|ca\$|us\$|\$)?\s*(\d+(?:\.\d{1,2})?)\s*(?:usd|cad|gbp|eur|dollars?)?$/.exec(text);if(lone){const budget=amount(Number(lone[1]));if(budget!=null)add(lone,{unlimitedBudget:false,budget,minBudget:null});}
  for(const event of events.sort((a,b)=>a.index-b.index))Object.assign(p,{budget:event.budget,minBudget:event.minBudget,unlimitedBudget:event.unlimitedBudget,budgetCurrency:event.budget!=null?(explicitCurrency||p.currency):null});
  // Budget-only answers should not replace the gift motif with "unlimited",
  // "unknown" or a currency name. Other interests in the same reply remain.
  let queryText=text;for(const span of spans.sort((a,b)=>b.index-a.index))queryText=queryText.slice(0,span.index)+' '.repeat(span.end-span.index)+queryText.slice(span.end);
  return spans.length?queryText.replace(/\b(?:us dollars?|canadian dollars?|usd|cad|gbp|pounds?|eur|euros?)\b/g,' '):queryText;
}

function applyPreferenceMessage(before,message) {
  const text=clean(message,2000).toLowerCase().replace(/[’‘]/g,"'");
  let p=shopperPreferences(before);
  if(conversationReply(message))return p;
  if(/\b(?:start (?:fresh|over|again)|reset (?:everything|preferences)|forget (?:everything|the previous|all that)|new gift|different gift|different person)\b/.test(text))p=shopperPreferences();
  const mentions={
    type:fieldMentions(text,'necklaces?|earrings?|bracelets?|pendants?|huggies?|studs?|rings?|charms?',v=>v.startsWith('stud')?'studs':v.startsWith('earring')?'earrings':v.replace(/s$/,'')),
    metal:fieldMentions(text,'rose gold|sterling silver|silver|gold(?: filled| plated)?',v=>v.startsWith('rose')?'rose gold':v.includes('silver')?'silver':'gold'),
    recipient:fieldMentions(text,RECIPIENTS.join('|'),v=>v).filter(hit=>(!['teacher','nurse','doctor'].includes(hit.value)||!/\b(?:as|became|becoming) (?:a |an )?$/.test(text.slice(0,hit.index)))&&(!['mother','father'].includes(hit.value)||!/\b(?:became|becoming) (?:a |an )?$/.test(text.slice(0,hit.index)))),
    occasion:fieldMentions(text,OCCASIONS.join('|'),v=>v)
  };
  const newRecipient=mentions.recipient.filter(x=>!x.negative).at(-1)?.value;
  const changingRecipient=!!(newRecipient&&p.recipient&&newRecipient!==p.recipient&&/\b(?:actually|instead|this (?:one|gift)|now|different|for)\b/.test(text));
  if(changingRecipient){p.query='';p.interests=[];p.excludedInterests=[];p.occasion=null;delete p.milestone;}
  for(const field of ['type','metal','recipient','occasion']) {
    const excluded=field==='type'?'excludedTypes':field==='metal'?'excludedMetals':null;
    for(const hit of mentions[field]) {
      if(hit.negative){if(p[field]===hit.value)p[field]=null;if(excluded&&!p[excluded].includes(hit.value))p[excluded].push(hit.value);}
      else {p[field]=hit.value;if(excluded)p[excluded]=p[excluded].filter(x=>x!==hit.value);}
    }
  }
  if(/\b(?:gift|present)\b/.test(text)){p.gifting=true;p.giftDiscovery=true;}
  else if(newRecipient&&newRecipient!=='myself')p.gifting=true;
  if(newRecipient==='myself'||/\b(?:for myself|shopping for myself|this is for me)\b/.test(text)){p.gifting=false;p.giftDiscovery=false;p.recipient='myself';p.occasion=null;}
  if(/\b(?:skip|pass on|prefer not to give|rather not say)\b[^.!?]{0,35}\b(?:recipient|who (?:it is|this is) for|gift details?)\b|\bskip (?:the )?gift details?\b/.test(text))p.recipientSkipped=true;
  if(/\b(?:skip|pass on|prefer not to give|rather not say)\b[^.!?]{0,35}\boccasion\b|\bskip (?:the )?gift details?\b/.test(text))p.occasionSkipped=true;
  if(newRecipient)p.recipientSkipped=false;
  if(mentions.occasion.some(x=>!x.negative))p.occasionSkipped=false;
  if(/\b(?:any|either|no preference (?:for|on))\s+(?:metal|silver or gold|gold or silver)\b/.test(text)){p.metal=null;p.excludedMetals=[];}
  if(/\b(?:any|either|no preference (?:for|on))\s+(?:type|style|necklace or earrings|earrings or necklace)\b/.test(text)){p.type=null;p.excludedTypes=[];}
  // A negated monetary cap also negates its currency. Money punctuation must
  // not turn "not under $40.50 USD" into an affirmative currency change.
  const currencyText=text.replace(/[$€£]/g,' ').replace(/(?<=\d)\.(?=\d)/g,'_').replace(/\bno more than\b/g,m=>' '.repeat(m.length));
  const explicitCurrency=fieldMentions(text,'us dollars?|usd|us\$|canadian dollars?|cad|ca\$|c\$|gbp|pounds?|eur|euros?',v=>/^canadian|^cad|^ca\$|^c\$/.test(v)?'CAD':/^gbp|^pound/.test(v)?'GBP':/^eur/.test(v)?'EUR':'USD').filter(x=>!negatedAt(currencyText,x.index)).at(-1)?.value;
  if(explicitCurrency){p.currency=explicitCurrency;if(p.budget!=null)p.budgetCurrency=explicitCurrency;}
  const queryText=applyBudgetMessage(p,text,explicitCurrency);
  const hits=MOTIFS.flatMap(([value,pattern])=>fieldMentions(text,pattern,()=>value).map(hit=>({...hit,inferred:PROFESSION_HINT.test(hit.raw)}))).sort((a,b)=>a.index-b.index);
  const selected=new Map();for(const hit of hits){const prior=selected.get(hit.value);if(!prior||!hit.inferred||prior.inferred)selected.set(hit.value,hit);}
  const latest=[...selected.values()];
  const explicit=latest.filter(x=>!x.negative&&!x.inferred);
  // Profession-to-symbol associations are suggestions. A shopper's explicit
  // movie slate, bunny, book or other motif takes precedence over that hint.
  const positive=(explicit.length?explicit:latest.filter(x=>!x.negative)).map(x=>x.value);
  // Rejecting a recipient's profession does not reject every related symbol.
  const negative=latest.filter(x=>x.negative&&!x.inferred).map(x=>x.value);
  p.excludedInterests=[...new Set([...p.excludedInterests,...negative])].filter(x=>!positive.includes(x)).slice(0,12);
  p.interests=p.interests.filter(x=>!negative.includes(x));
  if(positive.length){p.interests=positive;p.query=positive.join(' ');}
  else if(negative.length)p.query=p.interests.join(' ');
  for(const hit of fieldMentions(text,'handwriting|handwritten|my (?:own )?writing|engraving|engrave[ds]?|personali[sz](?:ed|ation|e)',v=>/handwrit|writing/.test(v)?'handwriting':'engraving'))p.personalization=hit.negative?null:hit.value;
  const nonShopping=semanticInquiry(text)||/\b(?:shipping|deliver|arrive|return|refund|compare|comparison|nickel|hypoallergenic|second|third|first|fourth|fifth|sixth|open|cart|bag|cheaper|expensive|more options|what else|tell me more|skip|pass on|prefer not|gift details?|secrets?|api keys?|credentials?|repository|system prompt|owner data|private records)\b/.test(text);
  if(!positive.length&&!negative.length&&!nonShopping){
    const controlled=new Set([...mentions.type,...mentions.metal,...mentions.recipient,...mentions.occasion].flatMap(x=>x.value.split(' ')));
    const words=(queryText.replace(/\b(?:i|we) mean\b/g,' ').match(/[a-z][a-z-]{2,}/g)||[]).filter(t=>!GENERIC.has(t)&&!controlled.has(t)&&!controlled.has(t.replace(/s$/,''))&&!['sterling','filled','plated','handwritten','engraving','personalized','personalised'].includes(t));
    const messageMilestone=milestoneDiscovery.parseMilestone(text,p.milestone,negatedAt);
    const residual=messageMilestone?milestoneDiscovery.contextResidual(words.join(' '),messageMilestone):words.join(' ');
    const raw=residual.split(/\s+/).filter(Boolean).slice(0,4).join(' ');
    if(raw){p.query=raw;p.interests=[];}
  }
  const milestone=milestoneDiscovery.parseMilestone(text,p.milestone,negatedAt);
  if(milestone)p.milestone=milestone;else delete p.milestone;
  if(milestone){
    // A person mentioned in a life event is not necessarily the gift recipient:
    // “for my sister, who became a mother” and “my mother passed away” differ.
    const directRecipients=fieldMentions(text,'(?:for|to) (?:my |our |a |the |her |his |their )?(?:'+RECIPIENTS.join('|')+')',value=>value.split(' ').at(-1)).filter(hit=>!hit.negative);
    if(directRecipients.length)p.recipient=directRecipients.at(-1).value;
    else if(milestone==='remembrance'&&!/\b(?:gift|present)\b/.test(text)&&!shopperPreferences(before).recipient){p.recipient=null;p.gifting=false;p.giftDiscovery=false;}
  }
  // A milestone is shopping context, not a literal motif. Preserve a previous
  // explicit motif through contextual follow-ups and never store inferred ones.
  if(milestone&&!positive.length&&!negative.length&&milestoneDiscovery.contextOnlyQuery(p.query,milestone)){
    const prior=shopperPreferences(before);
    const preserve=!changingRecipient&&!/\b(?:start (?:fresh|over|again)|reset|forget|new gift|different gift|different person)\b/.test(text)&&(prior.interests.length||prior.query&&!milestoneDiscovery.contextOnlyQuery(prior.query,prior.milestone));
    p.interests=preserve?prior.interests:[];p.query=preserve?prior.query:'';
  }
  return p;
}

function intentFrom(message,history=[],saved={}) {
  let prefs=shopperPreferences(saved);
  const hasSaved=Object.keys(saved||{}).some(k=>['query','type','metal','budget','recipient','interests'].includes(k)||(k==='unlimitedBudget'&&saved.unlimitedBudget===true)||(k==='gifting'&&saved.gifting===true));
  if(!hasSaved)for(const row of (Array.isArray(history)?history:[]).filter(x=>x?.role==='user').slice(-8))prefs=applyPreferenceMessage(prefs,row.content);
  return applyPreferenceMessage(prefs,message);
}

function motifPattern(value) {
  const entry=MOTIFS.find(([name])=>name===value);
  if(value==='apple book')return 'apple|book|teacher';
  if(value==='fire badge')return 'fire.?badge|firefighter|fireman';
  if(value==='police badge')return 'police|officer.?badge';
  if(value==='movie slate')return 'movie[ -]slates?|film[ -]slates?|clapper[ -]?boards?|clap[ -]?boards?';
  if(value==='engraved')return 'engrave|engrav|personaliz|personalis|handwrit|initial';
  return entry?entry[1]:clean(value,80).replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
}
function typeMatches(product,type) {
  const patterns={necklace:'necklace|pendant',pendant:'pendant|necklace',earrings:'earrings?|studs?|huggies?',studs:'studs?',huggie:'huggies?',bracelet:'bracelets?',ring:'rings?',charm:'charms?'};
  return !type||new RegExp('\\b(?:'+patterns[type]+')\\b','i').test(product.title+' '+product.type);
}
function chainEvidence(product,variant){
  const detail=product.description||'',variantText=variant.title+' '+(variant.options||[]).map(o=>o.value).join(' ');
  if(/chain\s+(?:is\s+)?not included|without (?:a |the )?chain|no chain|pendant only|charm only|loose charm/i.test(detail+' '+variantText))return false;
  const length=(variant.options||[]).some(o=>/(?:necklace|chain)\s*length/i.test(o.name)&&/\d/.test(o.value));
  const lengthConfiguration=(product.options||[]).some(o=>/(?:necklace|chain)\s*length/i.test(o.name)&&(o.values||[]).some(value=>/\d/.test(value)));
  return length||lengthConfiguration||/\b(?:chain|necklace)\b[^.!?]{0,70}\b(?:included|\d{1,2}(?:\.\d+)?\s*(?:inch(?:es)?|cm))\b/i.test(detail)||/\b(?:includes|comes with)\s+(?:a\s+)?(?:[^.!?]{0,35}\s)?chain\b/i.test(detail);
}
function variantTypeMatches(product,variant,type) {
  const actualType=clean(product.type,100),part=/\b(?:charms?|components?|add[ -]?ons?)\b/i.test(actualType);
  if(!type)return !part;
  if(type==='charm')return /\bcharms?\b/i.test(actualType);
  if(part)return false;
  if(type==='necklace'&&(!/\bnecklaces?\b/i.test(actualType)||!chainEvidence(product,variant)))return false;
  if(type==='pendant'&&!/\b(?:pendants?|necklaces?)\b/i.test(actualType))return false;
  if(['earrings','studs','huggie'].includes(type)&&!/\b(?:earrings?|studs?|hoops?|huggies?)\b/i.test(actualType))return false;
  if(type==='bracelet'&&!/\bbracelets?\b/i.test(actualType))return false;
  if(type==='ring'&&!/\brings?\b/i.test(actualType))return false;
  const detail=variant.title+' '+(variant.options||[]).filter(o=>/type|style|jewel|option|item/i.test(o.name)).map(o=>o.value).join(' ');
  if(/\b(?:necklace|huggie|earring|bracelet)\s+charms?\b|charm\s*\+\s*engrav|pendant only|charm only|loose charm/i.test(detail))return false;
  return /\b(?:necklaces?|earrings?|bracelets?|pendants?|huggies?|studs?|rings?)\b/i.test(detail)?typeMatches({title:detail,type:''},type):typeMatches(product,type);
}
function metalMatches(variant,metal) {
  if(!metal)return true;
  const material=variant.title+' '+(variant.options||[]).filter(o=>/metal|material|finish|colour|color/i.test(o.name)).map(o=>o.value).join(' ');
  if(metal==='silver')return /\b(?:silver|sterling)\b/i.test(material);
  if(metal==='rose gold')return /\brose\s*gold\b/i.test(material);
  return /\bgold\b/i.test(material)&&!/\brose\s*gold\b/i.test(material);
}
function personalizedVariant(product,variant) {
  const options=(variant.options||[]).filter(o=>/engrav|personali[sz]|custom/i.test(o.name));
  const detail=variant.title+' '+options.map(o=>o.value).join(' ');
  if(/no engraving|not engraved|without engraving|non[ -]?engraved|unengraved/i.test(detail)||options.some(o=>/^(?:no|none|without|not included)$/i.test(o.value)))return false;
  return /engrav|personali[sz]|handwrit|custom|monogram/i.test(detail)||options.some(o=>/^(?:yes|included|with)$/i.test(o.value))||/handwrit|monogram|custom studio/i.test(product.title);
}
function rankProducts(items,intent,now=Date.now()) {
  const p=shopperPreferences(intent),wanted=p.interests.length?p.interests:p.query.split(/\s+/).filter(Boolean);
  const matches=[];
  for(const product of Array.isArray(items)?items:[]) {
    if(product?.recommendationHold===true)continue;
    if(!validIdentity(product?.id)||!publicUrl(product.url,true)||!Number.isFinite(product.checkedAt)||now-product.checkedAt>5*60000||product.checkedAt>now+60000)continue;
    if(!shopperCatalogueField(product.title,300)||!shopperCatalogueField(product.type,100))continue;
    const searchable=product.title+' '+product.type+' '+(product.tags||[]).join(' ');
    if(p.excludedInterests.filter(value=>value!=='engraved').some(value=>new RegExp('\\b(?:'+motifPattern(value)+')\\b','i').test(product.title)))continue;
    const interestHits=wanted.filter(value=>new RegExp('\\b(?:'+motifPattern(value)+')\\b','i').test(searchable));
    if(wanted.length&&!interestHits.length)continue;
    const budgetApplies=!p.budgetCurrency||p.budgetCurrency===product.currency;
    const variants=(product.variants||[]).filter(v=>v.available&&Number.isFinite(v.price)&&v.price>=0&&shopperCatalogueField(v.title,200)&&(v.options||[]).every(option=>shopperCatalogueField(option?.name,100)&&shopperCatalogueField(option?.value,100))&&variantTypeMatches(product,v,p.type)&&!p.excludedTypes.some(type=>variantTypeMatches(product,v,type))&&metalMatches(v,p.metal)&&!p.excludedMetals.some(metal=>metalMatches(v,metal))&&(!p.excludedInterests.includes('engraved')||!personalizedVariant(product,v))&&(!budgetApplies||((p.budget==null||v.price<=p.budget)&&(p.minBudget==null||v.price>=p.minBudget))));
    if(!variants.length)continue;
    const minPrice=Math.min(...variants.map(v=>v.price));
    const why=[interestHits.length?interestHits.join(', '):null,p.type,p.metal,budgetApplies&&p.budget!=null?'within your '+(p.budgetCurrency||product.currency)+' item budget':null].filter(Boolean);
    const projected=productProjection({...product,variants});
    matches.push({score:interestHits.length*10+(p.type?3:0)+(p.metal?2:0),product:{...projected,suggestedVariantId:variants.slice().sort((a,b)=>a.price-b.price)[0].id,minPrice,why:why.length?'Selected for '+why.join(' · '):'An available piece from the live shop selection',budgetApplied:budgetApplies&&p.budget!=null}});
  }
  return matches.sort((a,b)=>b.score-a.score||a.product.minPrice-b.product.minPrice||a.product.title.localeCompare(b.product.title)).slice(0,6).map(x=>x.product);
}

function publicMeaningText(value){return clean(value,1500).split(/(?<=[.!?])\s+/).filter(sentence=>!!storySafeText(sentence,1500)&&!/^\s*(?:ask (?:whether|if|the|about|a question)|(?:prompt|tell|advise|remind) (?:the )?(?:user|shopper|customer|recipient)|use (?:this|the) (?:story|meaning|copy)|(?:do not|don't|never) (?:claim|promise|infer|say)|when (?:suggesting|recommending)|for (?:the )?concierge|(?:you|the assistant) should|recommend (?:this|the)|suggest (?:that|this|the)|avoid (?:claiming|saying)|include (?:this|the)|confirm (?:whether|if))\b/i.test(sentence)).join(' ').trim();}
function shopperMeaningSource(source,competitorSource){
  const href=publicUrl(source?.url);if(!href||competitorSource(source)||!storySafeText(source?.title,300)||!storySafeText(source?.excerpt,2000))return null;
  const u=new URL(href),host=u.hostname.toLowerCase().replace(/^www\./,''),own=host==='britesjewelry.com';
  if(!own&&/(?:^|[./_-])(?:internal|private|admin|staging|author|prompt)(?:[./_-]|$)|(?:^|\/)(?:products?|shop|cart|checkout)(?:\/|$)/i.test(host+u.pathname))return null;
  return href;
}
function mergeStorySupplements(dossiers,supplements,issueRecords=[],now=Date.now()){
  const baseList=Array.isArray(dossiers)?dossiers:[],supplementByProduct=new Map((Array.isArray(supplements)?supplements:[]).filter(x=>validIdentity(x?.productId)).map(x=>[x.productId,x]));
  const holds=new Map((Array.isArray(issueRecords)?issueRecords:[]).filter(x=>validIdentity(x?.productId)).map(x=>[x.productId,productIssueHolds(x)]));
  return baseList.map(dossier=>{
    const supplement=supplementByProduct.get(dossier?.productId),hold=holds.get(dossier?.productId);
    if(!supplement||dossier?.status!=='approved'||supplement.status!=='approved'||supplement.baseDossierVersion!==dossier.version||!exactFields(supplement,STORY_STORED_FIELDS)||(hold&&(hold.cartHold||hold.recommendationHold||hold.meaningHold)))return dossier;
    if(!Number.isFinite(supplement.savedAt)||!Number.isFinite(supplement.productCheckedAt)||supplement.savedAt>now+60000||supplement.productCheckedAt>supplement.savedAt+60000)return dossier;
    const projected=Object.fromEntries([...STORY_ROOT_FIELDS].map(key=>[key,supplement[key]]));
    const checked=validateStorySupplement(projected,{product:{id:supplement.productId,handle:supplement.handle,url:supplement.productUrl,checkedAt:now},dossier,now});
    if(!checked.ok||hash(checked.value)!==supplement.version)return dossier;
    return {...dossier,sources:[...(Array.isArray(dossier.sources)?dossier.sources:[]),...checked.value.sources],meanings:[...(Array.isArray(dossier.meanings)?dossier.meanings:[]),...checked.value.meanings],version:dossier.version};
  });
}
function publicMeanings(dossiers,productIds,now=Date.now(),issueRecords=[]) {
  const held=new Set(issueRecords.filter(x=>productIssueHolds(x).meaningHold||productIssueHolds(x).recommendationHold).map(x=>x.productId));
  const allowed=new Set(productIds||[]),out=[];
  for(const d of Array.isArray(dossiers)?dossiers:[]) {
    if(d?.status!=='approved'||!allowed.has(d.productId)||!validIdentity(d.productId)||held.has(d.productId))continue;
    const competingHosts=(Array.isArray(d.competitors)?d.competitors:[]).flatMap(c=>{const url=publicUrl(c?.url);return url?[new URL(url).hostname.toLowerCase().replace(/^www\./,'')]:[];});
    const competitorSource=s=>{const url=publicUrl(s?.url);if(!url)return false;const host=new URL(url).hostname.toLowerCase().replace(/^www\./,'');if(['britesjewelry.com'].includes(host))return false;return competingHosts.some(other=>host===other||host.endsWith('.'+other)||other.endsWith('.'+host));};
    for(const m of Array.isArray(d.meanings)?d.meanings:[]) {
      if(m?.kind!=='interpretation'||!clean(m.text,1500)||!clean(m.context,300)||!Array.isArray(m.sourceIds)||!m.sourceIds.length)continue;
      const sources=m.sourceIds.map(id=>(Array.isArray(d.sources)?d.sources:[]).find(s=>s?.id===id));
      if(sources.some(s=>!s||s.reviewed!==true||!shopperMeaningSource(s,competitorSource)||!Number.isFinite(s.checkedAt)||now-s.checkedAt>30*86400000||s.checkedAt>now+60000))continue;
      const text=publicMeaningText(m.text),context=publicMeaningText(m.context);if(!text||!context)continue;
      out.push({productId:d.productId,text,context:clean(context,300),kind:'interpretation',sources:sources.map(s=>({title:clean(s.title,300),url:publicUrl(s.url),checkedAt:s.checkedAt}))});
      if(out.filter(x=>x.productId===d.productId).length>=2)break;
    }
  }
  return out;
}

function safeQuestionRefinement(base,value){
  const question=typeof value==='string'?clean(value,220):'';
  if(!question||question.length<=10||question.length>=220||/[\d$]|https?:|guarantee|deliver|hypoallergenic|solid gold|password|credential|api key|email|phone|address|retailer|competitor|internal|system prompt|instructions?/i.test(question)||(question.match(/\?/g)||[]).length>1)return null;
  const topics=text=>new Set([
    /\b(?:budget|price|spend(?:ing)?|cost|afford)\b/i.test(text)&&'budget',
    /\b(?:necklace|earrings?|bracelet|rings?|charms?|jewel(?:ry|lery)|style|piece type)\b/i.test(text)&&'type',
    /\b(?:metal|material|finish|silver|gold)\b/i.test(text)&&'metal',
    /\b(?:enjoy|interest|animal|hobby|profession|symbol|motif)\b/i.test(text)&&'interest',
    /\b(?:who|recipient|person|gift for)\b/i.test(text)&&'recipient',
    /\b(?:occasion|birthday|anniversary|graduation|wedding)\b/i.test(text)&&'occasion'
  ].filter(Boolean));
  const allowed=topics(base),introduced=[...topics(question)].filter(topic=>!allowed.has(topic));
  return introduced.length?null:question;
}

function recoveryQuestion(products,intent,now){
  const p=shopperPreferences(intent),attempts=[];
  if(p.metal||p.excludedMetals.length)attempts.push({field:'metal',intent:{...p,metal:null,excludedMetals:[]},question:'Would you like to see another metal option?'});
  if(p.budget!=null||p.minBudget!=null)attempts.push({field:'budget',intent:{...p,budget:null,minBudget:null,budgetCurrency:null},question:'Would you like to change the item budget?'});
  if(p.type||p.excludedTypes.length)attempts.push({field:'type',intent:{...p,type:null,excludedTypes:[]},question:'Would you like to try another jewelry style?'});
  for(const attempt of attempts)if(rankProducts(products,attempt.intent,now).length)return attempt;
  return {field:'motif',question:'Would you like to try a different symbol or jewelry type?'};
}

function giftContext(intent){
  if(!intent.gifting||intent.recipient==='myself')return '';
  const occasion=intent.occasion?(intent.occasion==='thank you'?'thank-you':intent.occasion):'';
  return ' as a'+(occasion?' '+occasion:'')+' gift'+(intent.recipient?' for your '+intent.recipient:'');
}

function shopperCommand(text){return /\b(?:open|take me to|go to|view (?:the )?(?:product )?page|show (?:me )?(?:the )?(?:product )?page)\b/.test(text)||/\bview (?:the )?(?:this|that|first|second|third|fourth|fifth|sixth|[1-6](?:st|nd|rd|th)?)(?:\s+(?:piece|one|item|product))?\b/.test(text)?'navigate':/\b(?:add|put)\b[\s\S]{0,500}\b(?:bag|cart)\b/.test(text)?'choose':null;}
function explicitDestination(text){
  const tokens=[...text.matchAll(/(?:[a-z][a-z\d+.-]*:\/\/[^\s<>"']+|(?:javascript|data|file):[^\s<>"']+|\/\/[a-z\d.-]+[^\s<>"']*|(?:[a-z\d-]+\.)+[a-z]{2,}(?:\/[^\s<>"']*)?|(?<![\w/])\/[a-z0-9_-]+[^\s<>"']*)/gi)];
  const handles=tokens.map(match=>{let raw=match[0].replace(/[),.;!?]+$/,'');if(raw.startsWith('/')&&!raw.startsWith('//'))raw='https://britesjewelry.com'+raw;else if(!/^[a-z][a-z\d+.-]*:|^\/\//i.test(raw))raw='https://'+raw;const allowed=publicUrl(raw,true);if(!allowed)return null;const path=new URL(allowed).pathname;if(path!==raw.replace(/^https:\/\/[^/]+/i,'').split(/[?#]/)[0])return null;return path.match(/^\/(?:[a-z]{2}(?:-[a-z]{2})?\/)?products\/([a-z0-9_-]{1,180})\/?$/i)?.[1]||null;});
  return {unsafe:handles.some(handle=>!handle),handles:[...new Set(handles.filter(Boolean))],plain:tokens.reduceRight((value,match)=>value.slice(0,match.index)+' '.repeat(match[0].length)+value.slice(match.index+match[0].length),text)};
}
function checkoutRequest(text){
  text=clean(text,2000).toLowerCase().replace(/[’‘]/g,"'");const destination=explicitDestination(text),command=shopperCommand(destination.plain);
  if(command&&destination.unsafe)return 'destination';
  const plain=destination.plain.replace(/\bpay\s+(?:homage|tribute|attention|respect)\b/g,' ');
  const positive=pattern=>fieldMentions(plain,pattern).some(hit=>!hit.negative);
  if(positive('(?:saved|stored|on file) (?:credit |debit |payment )?cards?|card (?:details|numbers?|security code)|cvv|cvc|billing details|(?:retrieve|show|reveal) (?:my )?(?:card|payment)'))return 'payment';
  if(positive('pay (?:now|for|with|using|this|the)|charge (?:my|the)|(?:complete|submit|process|execute|make|confirm|place) (?:the |my |a )?(?:payment|purchase|order)|payment (?:succeeded|successful|success|status)'))return 'payment';
  if(positive('(?:open|take me to|go to|view|show me) (?:the |my |a |secure )?(?:checkout|check out|payment(?: page| form)?)'))return 'checkout';
  if(!command&&positive('checkout|check out|checking out|payments?|pay|paying|paid|credit cards?|debit cards?|paypal|(?:apple|google|shop) pay|billing'))return 'payment';
  return null;
}
function sharedBudgetRequest(text){
  const normalized=clean(text,2000).toLowerCase();
  const explicit='(?:(?:total|overall|combined|shared) (?:item |jewelry |jewellery )?budget|budget (?:for |across )?(?:all|both|the whole)|(?:in |altogether |combined )?total (?:for |across )?(?:all|both))';
  const pluralAmountTotal='(?:\\b(?:two|three|four|five|six|seven|eight|nine|ten|\\d+)\\b[^.!?]{0,48}\\b(?:gifts?|pieces?|necklaces?|earrings?|bracelets?|charms?|items?|bridesmaids?)\\b[^.!?]{0,96}\\b(?:usd|cad|gbp|eur|c\\$|ca\\$|us\\$|\\$)?\\s*\\d+(?:\\.\\d{1,2})?\\s*(?:usd|cad|gbp|eur|dollars?)?\\s+total\\b)';
  return fieldMentions(normalized,'(?:'+explicit+'|'+pluralAmountTotal+')').some(hit=>!hit.negative);
}

function exactCurrentPageRequest(text,currentHandle){
  if(!/^[a-z0-9_-]{1,180}$/.test(currentHandle||''))return false;
  const normalized=clean(text,2000).toLowerCase().replace(/[’‘]/g,"'");
  // Ordinals and comparisons refer to the prior visible card set, even when
  // the shopper happens to be viewing one of those products in another page.
  if(/\b(?:compare|comparison|versus|first|second|third|fourth|fifth|sixth|[1-6](?:st|nd|rd|th)?)\b/.test(normalized))return false;
  const reference='this exact(?:\\s+[a-z0-9][a-z0-9\'_-]*){0,5}\\s+(?:product|piece|item|necklaces?|earrings?|bracelets?|pendants?|huggies?|studs?|rings?|charms?)|this (?:product|piece|one)|current (?:product|piece|item)|on this (?:product )?page';
  return fieldMentions(normalized,reference).some(hit=>!hit.negative);
}

function displayedProductFromText(message,products){
  const text=clean(message,2000).toLowerCase().replace(/[’‘]/g,"'"),list=Array.isArray(products)?products:[];
  const handleMatches=list.filter(product=>/^[a-z0-9_-]{1,180}$/.test(product?.handle||'')&&text.includes(product.handle));
  if(handleMatches.length===1)return handleMatches[0];
  if(handleMatches.length>1)return null;
  const normalized=(' '+text.replace(/[^a-z0-9]+/g,' ').trim()+' '),titleMatches=list.filter(product=>{const title=clean(product?.title,300).toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();return title&&normalized.includes(' '+title+' ');});
  return titleMatches.length===1?titleMatches[0]:null;
}

function shopperAction(message,products,displayedHandles=[]) {
  const text=clean(message,2000).toLowerCase().replace(/[’‘]/g,"'"),destination=explicitDestination(text),command=shopperCommand(destination.plain);
  if(checkoutRequest(text))return null;
  if(!command||/\b(?:do not|don'?t|not|never)\s+(?:open|take|go|view|show|add|put)\b/.test(destination.plain))return null;
  const ordinal=/\b(first|second|third|fourth|fifth|sixth|[1-6])\b/.exec(destination.plain);
  const index=ordinal?['first','second','third','fourth','fifth','sixth'].includes(ordinal[1])?['first','second','third','fourth','fifth','sixth'].indexOf(ordinal[1]):Number(ordinal[1])-1:null;
  let product=index==null?displayedProductFromText(text,products):displayedHandles.length?products.find(p=>p.handle===displayedHandles[index]):products[index];
  if(destination.handles.length){if(destination.handles.length!==1)return null;const exact=products.find(p=>p.handle===destination.handles[0]);if(!exact||(index!=null&&product?.id!==exact.id))return null;product=exact;}
  else if(!product&&products.length===1&&/\b(?:this|that|it|the piece)\b/.test(text))product=products[0];
  return product?{type:command,productId:product.id,url:product.url}:null;
}

async function concierge({service,shopify,message,history=[],preferences={},context={},env={},ai,now=Date.now}) {
  const text=clean(message,2000);if(!text)throw Error('Write a message first.');
  const shopperCurrency=currencyCode(context.currency);
  const basePreferences=shopperCurrency&&!currencyCode(preferences.currency)?{...preferences,currency:shopperCurrency}:preferences;
  const boundary=checkoutRequest(text),at=now();
  if(boundary)return {schema:1,checkoutBoundary:true,reply:boundary==='destination'?'I can open an exact Brites product page when you select a piece. Complete payment yourself through the shop’s secure checkout; I can’t open an outside checkout or skip confirmation.':'Review your bag and complete payment yourself through the shop’s secure checkout. I can’t use saved cards, retrieve card details, place an order or confirm that a payment succeeded.',question:null,preferences:shopperPreferences(basePreferences),products:[],meanings:[],actions:[],checkedAt:at,live:false,aiUsed:false};
  const conversation=conversationReply(text);
  if(conversation)return {schema:1,conversationOnly:true,preserveSelection:true,conversationKind:conversation.kind,needsModelConversation:conversation.needsModelConversation,reply:conversation.reply,question:null,preferences:intentFrom('',history,basePreferences),products:[],meanings:[],actions:[],checkedAt:at,live:false,aiUsed:false};
  const intent=intentFrom(text,history,basePreferences);
  if(/\b(?:api keys?|credentials?|passwords?|system prompt|private (?:records|data)|owner data|repository|source code|sales history|customer (?:records|data))\b/i.test(text))return {schema:1,reply:'I can help with publicly listed pieces, gift ideas and the shop’s published information.',question:'What kind of piece are you looking for?',preferences:intent,products:[],meanings:[],actions:[],checkedAt:at,live:false,aiUsed:false};
  if(sharedBudgetRequest(text))return {schema:1,budgetClarification:true,reply:'I haven’t applied the overall budget as a per-item limit. I can compare individual pieces once you choose an item limit.',question:'What maximum item price should I use for each piece, before shipping and any applicable taxes?',preferences:shopperPreferences({...intent,budget:null,minBudget:null,unlimitedBudget:false,budgetCurrency:null}),products:[],meanings:[],actions:[],checkedAt:at,live:false,aiUsed:false};
  const currentHandle=/^[a-z0-9_-]{1,180}$/.test(context.currentHandle||'')?context.currentHandle:'';
  const exactCurrentContext=exactCurrentPageRequest(text,currentHandle);
  const destination=explicitDestination(text);
  // Positive inquiries about one safe Brites URL select its knowledge, but
  // do not authorize navigation or cart actions. Ignore URL path words when
  // checking inquiry language, negation and references to other cards.
  const destinationPlain=destination.plain.toLowerCase().replace(/[’‘]/g,"'");
  const command=shopperCommand(destinationPlain),negatedCommand=/\b(?:do not|don'?t|not|never)\s+(?:open|take|go|view|show|add|put)\b/.test(destinationPlain);
  const destinationInquiry=!/\b(?:compare|comparison|versus|first|second|third|fourth|fifth|sixth|[1-6](?:st|nd|rd|th)?)\b/.test(destinationPlain)&&(semanticInquiry(destinationPlain)||fieldMentions(destinationPlain,'tell me(?: more)? about|describe|explain|details? (?:about|for|on)|what (?:is|are|does)').some(hit=>!hit.negative));
  const destinationHandle=((command&&!negatedCommand)||destinationInquiry)&&!destination.unsafe&&destination.handles.length===1?destination.handles[0]:'';
  const exactProductContext=exactCurrentContext||!!destinationHandle;
  const displayedReference=fieldMentions(text.toLowerCase().replace(/[’‘]/g,"'"),'this piece|that piece|this one|that one').some(hit=>!hit.negative);
  const useContext=exactProductContext||displayedReference||semanticInquiry(text)||/\b(?:compare|comparison|first|second|third|fourth|fifth|sixth|open|cart|bag)\b/i.test(text);
  const handles=destinationHandle?[destinationHandle]:exactCurrentContext?[currentHandle]:plainList(context.productHandles,6).filter(h=>/^[a-z0-9_-]{1,180}$/.test(h));
  if(!handles.length&&currentHandle)handles.push(currentHandle);
  const milestonePlan=!command&&!(useContext&&handles.length)?milestoneDiscovery.discoveryIntent(intent):null;
  const searchedMilestone=!!milestonePlan?.motifs.length;
  let milestoneHints=[],recalledVersions=new Map();
  let queried;
  if(useContext&&handles.length)queried={products:(await Promise.all(handles.map(h=>shopify.byHandle(h)))).filter(Boolean)};
  else if(searchedMilestone){
    const bounds=milestoneDiscovery.recallBounds(),hintRead=typeof service.milestoneCandidateHandles==='function'?service.milestoneCandidateHandles(intent,bounds.liveHandles):typeof service.catalogueCandidateHandles==='function'?service.catalogueCandidateHandles({...intent,query:milestonePlan.motifs.join(' '),interests:milestonePlan.motifs},bounds.liveHandles):Promise.resolve([]);
    const [searches,hints]=await Promise.all([Promise.all(milestonePlan.motifs.map(motif=>shopify.search(motif))),hintRead]);
    const searched=[...new Map(searches.flatMap(result=>(result.products||[]).slice(0,10)).map(product=>[product.id,product])).values()].slice(0,30),seenHandles=new Set(searched.map(product=>product.handle));
    const indexedHints=(Array.isArray(hints)?hints:[]).flatMap(hint=>{
      if(typeof hint==='string')return /^[a-z0-9_-]{1,180}$/.test(hint)?[{handle:hint,productId:null,dossierVersion:null}]:[];
      return /^[a-z0-9_-]{1,180}$/.test(hint?.handle||'')&&(!hint.productId||validIdentity(hint.productId))&&(!hint.dossierVersion||/^[a-f0-9]{64}$/.test(hint.dossierVersion))&&(!hint.supplementVersion||/^[a-f0-9]{64}$/.test(hint.supplementVersion))?[{handle:hint.handle,productId:hint.productId||null,dossierVersion:hint.dossierVersion||null,supplementVersion:hint.supplementVersion||null}]:[];
    }).filter((hint,index,list)=>list.findIndex(other=>other.handle===hint.handle)===index).slice(0,bounds.liveHandles);
    // A version-pinned index hit can already be present in the predictive
    // search set. Preserve that evidence link instead of discarding it merely
    // because its handle was found. Otherwise a broad motif with more than six
    // equally scored live results can sort the only reviewed product out before
    // the public-meaning gate. Identity still has to agree exactly.
    for(const hint of indexedHints){const found=searched.find(product=>product.handle===hint.handle);if(found&&(!hint.productId||found.id===hint.productId)&&hint.dossierVersion)recalledVersions.set(found.id,{dossierVersion:hint.dossierVersion,supplementVersion:hint.supplementVersion});}
    milestoneHints=indexedHints.filter(hint=>!seenHandles.has(hint.handle));
    const attempts=await Promise.allSettled(milestoneHints.map(hint=>shopify.byHandle(hint.handle))),recalled=[];
    for(let i=0;i<attempts.length;i++){const attempt=attempts[i],hint=milestoneHints[i];if(attempt.status!=='fulfilled'||!attempt.value)continue;if(attempt.value.handle!==hint.handle||(hint.productId&&attempt.value.id!==hint.productId))continue;recalled.push(attempt.value);if(hint.dossierVersion)recalledVersions.set(attempt.value.id,{dossierVersion:hint.dossierVersion,supplementVersion:hint.supplementVersion});}
    if(milestoneHints.length&&!searched.length&&!recalled.length&&attempts.some(x=>x.status==='rejected'))throw Error('The matching published pieces could not be checked.');
    queried={products:[...new Map([...searched,...recalled].map(product=>[product.id,product])).values()].slice(0,30+bounds.liveHandles)};
  } else queried=milestonePlan?{products:[]}:await shopify.search(intent.query||intent.type||'necklace');
  let checkedProducts=queried.products;
  // An ordinal or unique exact title among the already displayed, freshly
  // re-read cards is a selection command, not new discovery text. Resolve it
  // before ranking so stale saved preferences cannot filter the selected live
  // product away.
  const displayedTitleProduct=command&&!destination.handles.length?displayedProductFromText(destinationPlain,checkedProducts):null;
  const displayedOrdinal=/\b(first|second|third|fourth|fifth|sixth|[1-6])\b/i.exec(destinationPlain),displayedOrdinalWords=['first','second','third','fourth','fifth','sixth'];
  const displayedOrdinalIndex=command&&displayedOrdinal?(displayedOrdinalWords.includes(displayedOrdinal[1].toLowerCase())?displayedOrdinalWords.indexOf(displayedOrdinal[1].toLowerCase()):Number(displayedOrdinal[1])-1):null;
  const displayedOrdinalProduct=displayedOrdinalIndex==null?null:checkedProducts.find(product=>product.handle===handles[displayedOrdinalIndex]);
  const displayedSelectedProduct=displayedTitleProduct||displayedOrdinalProduct;
  // Persisting a live catalogue mirror and reading independent reviewed holds
  // can overlap, but neither may be bypassed before ranking or recommending.
  let [,issueRecords]=await Promise.all([service.saveProducts(checkedProducts),service.productIssues?service.productIssues(checkedProducts.map(p=>p.id)):[]]);
  // An explicit exact-page reference selects that validated live product,
  // rather than letting stale discovery preferences filter it back out.
  const directProductContext=exactProductContext||!!displayedSelectedProduct;
  const exactCurrentType=directProductContext&&/\bcharms?\b/i.test((displayedSelectedProduct||checkedProducts[0])?.type||'')?'charm':null;
  const rankingIntent=directProductContext?shopperPreferences({currency:intent.currency,type:exactCurrentType}):searchedMilestone?{...intent,query:'',interests:milestonePlan.motifs}:intent;
  let eligibleProducts=applyProductIssues(checkedProducts,issueRecords);
  let products;
  if(searchedMilestone){
    const motifRanked=[...new Map(milestonePlan.motifs.flatMap(motif=>rankProducts(eligibleProducts,{...rankingIntent,interests:[motif]},at)).map(product=>[product.id,product])).values()];
    // Exact live products recalled from version-pinned reviewed meanings still
    // obey type, metal, price, stock, freshness and issue holds. They do not
    // need a second literal motif match after the evidence link was verified.
    const recalledIds=new Set(recalledVersions.keys()),contextualIntent={...rankingIntent,query:'',interests:[]};
    const recalledRanked=rankProducts(eligibleProducts.filter(product=>recalledIds.has(product.id)),contextualIntent,at);
    products=[...new Map([...recalledRanked,...motifRanked].map(product=>[product.id,product])).values()].slice(0,18);
  }else products=rankProducts(eligibleProducts,rankingIntent,at);
  const boundedRecall=plainList(intent.interests,4).length>0;
  if(!useContext&&boundedRecall&&!products.length&&typeof service.catalogueCandidateHandles==='function'){
    const seen=new Set(checkedProducts.map(p=>p.handle)),candidateHandles=(await service.catalogueCandidateHandles(intent,15)).filter(h=>/^[a-z0-9_-]{1,180}$/.test(h)&&!seen.has(h)).slice(0,15);
    const attempts=await Promise.allSettled(candidateHandles.map(handle=>shopify.byHandle(handle)));
    const refreshed=attempts.filter(x=>x.status==='fulfilled'&&x.value).map(x=>x.value);
    if(candidateHandles.length&&!refreshed.length&&attempts.some(x=>x.status==='rejected'))throw Error('The matching published pieces could not be checked.');
    if(refreshed.length){
      checkedProducts=[...new Map([...checkedProducts,...refreshed].map(p=>[p.id,p])).values()];
      [,issueRecords]=await Promise.all([service.saveProducts(refreshed),service.productIssues?service.productIssues(checkedProducts.map(p=>p.id)):[]]);
      eligibleProducts=applyProductIssues(checkedProducts,issueRecords);
      products=rankProducts(eligibleProducts,intent,at);
    }
  }
  // Ordinals refer to the prior visible card order, never a new price sort.
  if(useContext&&handles.length)products=products.sort((a,b)=>handles.indexOf(a.handle)-handles.indexOf(b.handle));
  // A direct selection never silently shifts to a different card when the
  // requested product or its matching option can no longer be confirmed.
  let unavailableSelection=null;
  if(command&&useContext&&handles.length){
    const ordinal=/\b(first|second|third|fourth|fifth|sixth|[1-6])\b/i.exec(destinationPlain),words=['first','second','third','fourth','fifth','sixth'];
    const index=ordinal?(words.includes(ordinal[1].toLowerCase())?words.indexOf(ordinal[1].toLowerCase()):Number(ordinal[1])-1):displayedTitleProduct?handles.indexOf(displayedTitleProduct.handle):exactProductContext||displayedReference?0:null;
    const targetHandle=index==null?null:handles[index],target=eligibleProducts.find(product=>product.handle===targetHandle);
    if(index!=null&&(!targetHandle||!target||!products.some(product=>product.handle===targetHandle))){
      unavailableSelection={kind:!targetHandle?'reference':!target?'missing':target.recommendationHold?'unconfirmed':(target.variants||[]).some(variant=>variant.available)?'option':'stock'};
      products=[];
    }
  }
  let dossiers=[],dossierSupplements=[],knowledgeUnavailable=false;
  try{const ids=products.map(p=>p.id),[research,supplements]=await Promise.all([service.research(ids),typeof service.storySupplements==='function'?service.storySupplements(ids):[]]);dossierSupplements=supplements;dossiers=mergeStorySupplements(research,supplements,issueRecords,at);}catch{knowledgeUnavailable=true;}
  if(recalledVersions.size){
    const currentVersions=new Map(dossiers.map(dossier=>[dossier?.productId,dossier?.version])),currentSupplements=new Map(dossierSupplements.map(supplement=>[supplement?.productId,supplement?.version]));
    const drifted=new Set([...recalledVersions].filter(([id,versions])=>currentVersions.get(id)!==versions.dossierVersion||(versions.supplementVersion&&currentSupplements.get(id)!==versions.supplementVersion)).map(([id])=>id));
    if(drifted.size){products=products.filter(product=>!drifted.has(product.id));dossiers=dossiers.filter(dossier=>!drifted.has(dossier?.productId));}
  }
  let allMeanings=publicMeanings(dossiers,products.map(p=>p.id),at,issueRecords);
  if(milestonePlan){
    allMeanings=milestoneDiscovery.matchingMeanings(allMeanings,intent.milestone);
    const reviewedIds=new Set(allMeanings.map(m=>m.productId));
    // No inferred product association can bypass exact approved public evidence.
    // Keep at most three cards so every suggested connection has a visible citation.
    products=products.filter(product=>reviewedIds.has(product.id)).slice(0,3).map(product=>({...product,why:'A possible personal connection with '+milestonePlan.label+'; see the reviewed interpretation below.'}));
    allMeanings=products.flatMap(product=>allMeanings.filter(m=>m.productId===product.id).slice(0,1));
  }
  const meanings=allMeanings.slice(0,3);
  const recovery=!products.length&&!unavailableSelection?recoveryQuestion(eligibleProducts,intent,at):null;
  let reply=products.length?'These available pieces connect with '+(intent.query||'the preferences you’ve shared')+giftContext(intent)+'.':'I couldn’t confirm an available match for those preferences. We can adjust the selection together.';
  let question=!products.length?(recovery?.question||'Would you like to try a different symbol or jewelry type?'):!intent.query?'What does the person enjoy—an animal, hobby, profession or symbol?':!intent.type?'Would they enjoy a necklace, earrings or another jewelry style?':intent.budget==null&&!intent.unlimitedBudget?'Is there an item budget you’d like me to stay within?':!intent.metal?'Do you have a metal preference, or would you like to see both?':intent.giftDiscovery&&!intent.recipient&&!intent.recipientSkipped?'Who is the gift for?':intent.giftDiscovery&&!intent.occasion&&!intent.occasionSkipped?'Is there an occasion for the gift?':null;
  const result={schema:1,reply,question,preferences:intent,products,meanings,actions:products.map(p=>({type:'navigate',productId:p.id,url:p.url,label:'View '+p.title})),checkedAt:at,live:true,aiUsed:false};
  if(intent.milestone&&!(useContext&&handles.length)&&!command){
    const presentation=milestoneDiscovery.presentation(intent.milestone);
    result.reply=presentation.intro+(products.length?(milestonePlan?' These available pieces have reviewed interpretations you might personally connect with '+presentation.label+'. Meanings vary; choose what feels right to you.':' These available pieces match the motif and preferences you shared.'):' I couldn’t confirm a reviewed, available connection with those preferences.');
    if(milestonePlan){result.milestoneDiscovery={milestone:intent.milestone,method:'reviewed_interpretations',requiresPersonalFit:true};result.question=products.length?'Does one of these interpretations feel right, or would you prefer a different personal symbol?':presentation.question;}
    if(intent.milestone==='remembrance'&&products.length&&!intent.type)result.question='Would a necklace, earrings or another jewellery style feel right?';
  }
  if(recovery)result.recoveryField=recovery.field;
  if(unavailableSelection){result.reply=unavailableSelection.kind==='reference'?'I can’t identify that earlier option from the displayed pieces, so I haven’t opened or substituted a different item.':unavailableSelection.kind==='stock'?'That selected piece is not currently available, so I haven’t substituted a different item.':unavailableSelection.kind==='missing'?'I couldn’t confirm that selected piece in the live catalogue, so I haven’t substituted a different item.':'I couldn’t confirm an available option for that selected piece, so I haven’t substituted a different item.';result.question=unavailableSelection.kind==='reference'?'Which displayed piece would you like?':'Would you like me to find a similar available piece?';result.unavailableSelection=true;}
  if(knowledgeUnavailable)result.knowledgeUnavailable=true;
  const mismatchedCurrency=products.some(p=>intent.budget!=null&&intent.budgetCurrency&&intent.budgetCurrency!==p.currency);
  if(mismatchedCurrency){result.reply+=' Catalogue prices are shown in '+products[0].currency+'. I haven’t applied your '+intent.budgetCurrency+' budget to those prices.';result.question='Would you like to give an item budget in '+products[0].currency+', or check the current local price on a product page?';result.currencyMismatch=true;}
  if(semanticInquiry(text)){result.reply=meanings.length?'Here are reviewed interpretations associated with the displayed pieces. Meanings vary by culture and by the person wearing them.':'I don’t yet have reviewed symbolism or history for these pieces. I can still help you choose by the person’s interests and the published product details.';result.question=null;}
  if(/\b(?:compare|comparison|versus)\b/i.test(text)){result.reply=products.length>=2?'Compare the live metal options, item prices and designs below. Each product page has the complete description.':'I need two available pieces to make a useful comparison.';result.question=products.length>=2?null:'Which other piece would you like to compare?';}
  const policyTopics=require('./_britesConcierge').classify(text,history).topics;
  if(policyTopics.includes('shipping')||policyTopics.includes('refund')){result.reply='Shipping timing and returns depend on the order and destination. The current shop policies and checkout show the applicable details.';result.policyLinks=[{label:'Shipping policy',url:'https://britesjewelry.com/policies/shipping-policy'},{label:'Refund policy',url:'https://britesjewelry.com/policies/refund-policy'}];result.question=policyTopics.includes('shipping')?'Which country is the gift going to, and when is it needed?':null;}
  else if(intent.personalization&&/\b(?:engrave[ds]?|engraving|personali[sz](?:ed|ation|e)|handwriting|handwritten)\b/i.test(text)){result.reply='I can help find a piece with personalization options. The product page confirms the exact engraving limits and any design upload before adding it to your bag.';result.question=intent.personalization==='handwriting'?'Would you like to explore a handwriting piece or the custom design studio?':/\b(?:name|initials?|message)\b/i.test(text)?null:'Are you thinking of a name, initials, a short message or handwriting?';}
  if(/\b(?:hypoallergenic|nickel|allerg(?:y|ies|ic)|solid gold|waterproof|tarnish)\b/i.test(text)){result.reply='Materials and care requirements vary by piece. Please check the exact product description; I can’t infer allergy safety or material guarantees from its appearance.';result.question=null;}
  const requestedAction=shopperAction(text,products,useContext?handles:[]);
  if(requestedAction){result.requestedAction=requestedAction;result.question=null;result.reply=requestedAction.type==='navigate'?'Opening the piece you selected.':'Choose the exact available option below, then confirm before it is added to your bag.';}
  // The current broad gold preference is not an exact material-form filter.
  // Disclose mixed/unclear forms rather than calling solid gold gold-filled.
  if(fieldMentions(text.toLowerCase(),'gold[ -]filled').some(hit=>!hit.negative)&&products.some(p=>p.variants.some(v=>!/\bgold[ -]filled\b/i.test(v.title+' '+(v.options||[]).filter(o=>/metal|material|finish/i.test(o.name)).map(o=>o.value).join(' '))))){result.materialFormUnfiltered=true;const note=' This selection hasn’t been filtered specifically to gold-filled. Check each exact variant’s metal label before choosing.';const split=result.reply.indexOf(' Catalogue prices are shown in ');result.reply=split<0?result.reply+note:result.reply.slice(0,split)+note+result.reply.slice(split);}
  // Reply-specific presentation must not hide an unapplied foreign-currency
  // item cap. Keep direct actions focused on the shopper's selected control;
  // the disclosure neither converts prices nor treats that cap as satisfied.
  if(result.currencyMismatch&&!result.reply.includes('I haven’t applied your ')){
    result.reply+=' Catalogue prices are shown in '+products[0].currency+'. I haven’t applied your '+intent.budgetCurrency+' budget to those prices.';
    if(!result.requestedAction)result.question='Would you like to give an item budget in '+products[0].currency+', or check the current local price on a product page?';
  }
  // Runtime inference can refine a question only. It cannot supply product
  // facts, choose tools, browse, purchase, or read private research fields.
  if(ai&&!milestonePlan){try{const chosen=await ai({message:text,history:history.slice(-6).map(r=>({role:r.role,content:clean(r.content,1500)})),preferences:intent,products:products.map(p=>({id:p.id,title:p.title})),question:result.question});if(chosen&&['gift','self','comparison','meaning','shipping','engraving','discovery'].includes(chosen.intent)){result.intent=chosen.intent;result.aiUsed=true;const refined=result.question&&safeQuestionRefinement(result.question,chosen.question);if(refined&&!(intent.unlimitedBudget&&/\b(?:budget|spend(?:ing)?|price|cost|afford(?:able)?|how much)\b/i.test(refined)))result.question=refined;}}catch{result.aiUsed=false;result.providerUnavailable=true;}}
  return result;
}
module.exports={CATALOG_QUERY,STOP_AT,clean,hash,textOf,publicUrl,sameSecret,namespace,makeDb,normalizeProduct,productProjection,validateDossier,validateStorySupplement,createShopify,createGrowthService,productIssueHolds,applyProductIssues,shopperPreferences,negatedAt,conversationReply,intentFrom,rankProducts,publicMeaningText,mergeStorySupplements,publicMeanings,shopperAction,concierge,catalogueImageUrl,catalogueImages,merchantGuidance:storefront.merchantGuidance,readStorefrontServices:storefront.readServices};
