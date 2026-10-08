'use strict';
// Published seed listings widen discovery beyond Shopify's small predictive
// window. They remain a bounded sample; exact product reads authorize facts and
// controls later. This module never uses Admin inventory or private research.
const MAX_SEED=160,MAX_SEARCH=60,MAX_CANDIDATES=MAX_SEED+MAX_SEARCH;
const CATEGORIES=require('./_britesStorefrontSeed').CATEGORIES;
function inventoryIdentities(value,{safeTitle=title=>typeof title==='string'&&title.trim()&&title.length<=180&&!/[\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/.test(title)?title.trim():null}={}){
  if(!Array.isArray(value)||value.length>MAX_SEED)return [];
  const ids=new Map(),handles=new Map();
  for(const row of value){
    const title=safeTitle(row?.title);
    if(!/^gid:\/\/shopify\/Product\/[1-9]\d{0,19}$/.test(row?.id||'')||!/^[a-z0-9_-]{1,180}$/.test(row?.handle||'')||!title)continue;
    const previous=ids.get(row.id),other=handles.get(row.handle);
    if(previous&&previous.handle!==row.handle||other&&other!==row.id)return [];
    handles.set(row.handle,row.id);ids.set(row.id,{id:row.id,handle:row.handle,title,storeCategories:Array.isArray(row.storeCategories)?[...new Set(row.storeCategories.filter(category=>CATEGORIES.includes(category)))]:[]});
  }
  return [...ids.values()];
}
function mergeProducts(groups){
  const ids=new Map(),handles=new Map();
  for(const group of groups)for(const product of group){
    if(!/^gid:\/\/shopify\/Product\/[1-9]\d*$/.test(product?.id||'')||!/^[a-z0-9_-]{1,180}$/.test(product?.handle||''))continue;
    const previous=ids.get(product.id),other=handles.get(product.handle);
    if(previous&&previous.handle!==product.handle||other&&other!==product.id)throw Error('The published discovery identities conflict.');
    handles.set(product.handle,product.id);
    if(!previous||Number(product.checkedAt)>=Number(previous.checkedAt))ids.set(product.id,product);
  }
  return [...ids.values()].slice(0,MAX_CANDIDATES);
}
function createDiscovery({readSeed,readSearch,readFallback,now=Date.now}){
  async function read(terms,{reset=false}={}){
    const attempts=await Promise.allSettled([readSeed(),reset?Promise.resolve({products:[]}):readSearch(terms)]);
    const seed=attempts[0].status==='fulfilled'?attempts[0].value:null,search=attempts[1].status==='fulfilled'?attempts[1].value:null;
    let sampled=Array.isArray(seed?.products)?seed.products.slice(0,MAX_SEED):[],predicted=Array.isArray(search?.products)?search.products.slice(0,MAX_SEARCH):[];
    if(reset&&!sampled.length&&typeof readFallback==='function'){const fallback=await readFallback();predicted=Array.isArray(fallback?.products)?fallback.products.slice(0,MAX_SEARCH):[];}
    if(!sampled.length&&!predicted.length&&attempts.some(result=>result.status==='rejected'))throw Error('The published catalogue discovery could not be checked.');
    const products=mergeProducts([sampled,predicted]);
    return {products,pageInfo:{hasNextPage:false,endCursor:null},checkedAt:now(),access:'public_catalogue',discovery:{schema:1,source:'public_catalogue_sample',sampledListings:sampled.length,predictiveListings:predicted.length,candidateListings:products.length,catalogueComplete:false,partial:!seed||seed.seed?.partial===true||attempts.some(result=>result.status==='rejected')}};
  }
  return {read};
}
module.exports={createDiscovery,mergeProducts,inventoryIdentities,MAX_SEED,MAX_SEARCH,MAX_CANDIDATES};
