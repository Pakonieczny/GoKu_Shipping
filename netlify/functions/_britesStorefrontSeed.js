'use strict';
// This is a bounded discovery sample of public Shopify listings, not a sales
// ranking, an inventory assertion, or a replacement for exact product reads.
const CATEGORIES=['regular-necklaces','beady-necklaces','stud-earrings','hoop-earrings','charm-only'];
const QUERIES=['necklace','beady necklace','hoop earrings','charm'];
const MINIMUM=100,TARGET=120,MAXIMUM=160,PAGE_LIMIT=4,CACHE_MS=60000;
function categories(product){
  const title=String(product?.title||'').toLowerCase(),type=String(product?.type||'').toLowerCase();
  const optionNames=(Array.isArray(product?.options)?product.options:[]).map(o=>String(o?.name||'').toLowerCase());
  const charmAxis=optionNames.some(name=>/\bcharm type\b/.test(name));
  // A Huggie CHARM SET is an attachment choice, not a hoop-earring listing.
  if(charmAxis&&/\bcharms?\b/.test(type))return ['charm-only'];
  const earrings=/\bearrings?\b/.test(type)||/\bearrings?\b/.test(title);
  if(earrings&&/\b(?:hoops?|huggies?)\b/.test(title+' '+type))return ['hoop-earrings'];
  if(earrings&&/\bstuds?\b/.test(title+' '+type))return ['stud-earrings'];
  const necklace=/\bnecklaces?\b/.test(title+' '+type);
  if(necklace&&/\bbeady\b/.test(title+' '+type))return ['beady-necklaces'];
  // Gemstone bead strands and a bare necklace charm are not regular chains.
  const lengthAxis=optionNames.some(name=>/\b(?:chain|necklace)?\s*length\b/.test(name));
  if(necklace&&!charmAxis&&!/\b(?:beads?|beaded)\b/.test(title)&&( /\bnecklaces?\b/.test(type)||lengthAxis))return ['regular-necklaces'];
  return [];
}
function counts(products){const result=Object.fromEntries(CATEGORIES.map(name=>[name,0]));for(const p of products)for(const name of categories(p))result[name]++;return result;}
function choose(products){
  const groups=CATEGORIES.map(name=>products.filter(p=>categories(p).includes(name))),chosen=new Map();
  let offset=0;
  while(chosen.size<TARGET&&groups.some(group=>offset<group.length)){
    for(const group of groups){const p=group[offset];if(p&&!chosen.has(p.id))chosen.set(p.id,p);if(chosen.size>=TARGET)break;}offset++;
  }
  // All seed pieces belong to one of the requested categories. A sparse
  // category never creates duplicate/fabricated listings to fill its quota.
  return [...chosen.values()].slice(0,MAXIMUM).map(p=>({...p,storeCategories:categories(p)}));
}
function createSeedReader({readPage,readSearch,isDiscovery,project,now=Date.now,cache={}}){
  async function collect(){
    const byId=new Map(),byHandle=new Map();let sourcePages=0,sourceSearches=0,failedReads=0;
    function add(rows){for(const p of Array.isArray(rows)?rows:[]){
      if(!isDiscovery(p)||!/^gid:\/\/shopify\/Product\/[1-9]\d*$/.test(p?.id||'')||!/^[a-z0-9_-]{1,180}$/.test(p?.handle||''))continue;
      const publicProduct=project(p);
      if(!publicProduct?.title||!publicProduct.image||!categories(publicProduct).length)continue;
      const old=byId.get(p.id),other=byHandle.get(p.handle);
      // Conflicting identities are refused rather than silently rebound.
      if((old&&old.handle!==p.handle)||(other&&other!==p.id))throw Error('The published seed identities conflict.');
      byHandle.set(p.handle,p.id);byId.set(p.id,p);
    }}
    const first=await Promise.allSettled([readPage(1),readPage(2)]);
    for(const r of first){if(r.status==='fulfilled'){sourcePages++;add(r.value?.products);}else failedReads++;}
    // Predictive discovery remains a small recall window. Every returned
    // handle is checked by the existing exact public product.js reader.
    for(let i=0;i<QUERIES.length;i+=2){
      const results=await Promise.allSettled(QUERIES.slice(i,i+2).map(readSearch));
      for(const r of results){if(r.status==='fulfilled'){sourceSearches++;add(r.value?.products);}else failedReads++;}
    }
    const needsCoverage=()=>byId.size<TARGET||Object.values(counts([...byId.values()])).some(n=>n<20);
    for(let page=3;page<=PAGE_LIMIT&&needsCoverage();page++){
      try{const result=await readPage(page);sourcePages++;add(result?.products);if(!result?.pageInfo?.hasNextPage)break;}catch{failedReads++;}
    }
    if(!byId.size)throw Error('The published starter collection could not be checked.');
    const products=choose([...byId.values()]),categoryCounts=counts(products),unfilledCategories=CATEGORIES.filter(name=>!categoryCounts[name]);
    const checkedAt=now(),complete=products.length>=MINIMUM&&!unfilledCategories.length;
    return {products,pageInfo:{hasNextPage:false,endCursor:null},checkedAt,access:'public_catalogue',seed:{schema:1,target:TARGET,minimum:MINIMUM,loaded:products.length,complete,categoryCounts,unfilledCategories,sourcePages,sourceSearches,partial:!complete||failedReads>0}};
  }
  async function read(){
    if(cache.value&&now()<cache.expiresAt)return structuredClone(cache.value);
    if(!cache.pending)cache.pending=(async()=>{const value=await collect();cache.value=value;cache.expiresAt=now()+CACHE_MS;return value;})();
    try{return structuredClone(await cache.pending);}finally{cache.pending=null;}
  }
  return {read};
}
module.exports={CATEGORIES,categories,counts,choose,createSeedReader,MINIMUM,TARGET,MAXIMUM,PAGE_LIMIT,CACHE_MS};
