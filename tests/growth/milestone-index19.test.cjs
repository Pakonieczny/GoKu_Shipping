'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),core=require('../../netlify/functions/_britesGrowth');
const NOW=Date.parse('2026-10-02T22:00:00Z'),id=n=>'gid://shopify/Product/'+n,copy=value=>structuredClone(value);
class Store{
  constructor(){this.docs=new Map();}
  collection(name){return new Collection(this,name);}
}
class Ref{
  constructor(db,name,key){this.db=db;this.path=name+'/'+key;}
  async get(){const value=this.db.docs.get(this.path);return{exists:value!==undefined,data:()=>copy(value)};}
  async set(value){this.db.docs.set(this.path,copy(value));}
}
class Collection{
  constructor(db,name){this.db=db;this.name=name;}
  doc(key){return new Ref(this.db,this.name,key);}
  where(field,op,value){assert.equal(op,'==');return{limit:limit=>({get:async()=>({docs:[...this.db.docs].filter(([path,row])=>path.startsWith(this.name+'/')&&row[field]===value).slice(0,limit).map(([,row])=>({data:()=>copy(row)}))})})};}
}
const product=(n,handle)=>({id:id(n),handle,url:'https://britesjewelry.com/products/'+handle,title:handle.replaceAll('-',' '),type:'Necklace',tags:[],checkedAt:NOW});
const dossier=(p,text,version)=>({schema:1,productId:p.id,handle:p.handle,status:'approved',version,savedAt:NOW,sources:[{id:'museum',url:'https://museum.example.edu/collection',title:'Museum collection',excerpt:'Reviewed context.',checkedAt:NOW,reviewed:true}],meanings:[{text,context:'Qualified personal interpretation.',kind:'interpretation',sourceIds:['museum']}],facts:[],competitors:[],recommendations:[],buyerIntents:[]});
async function seed(db,name,key,value){db.docs.set(name+'/'+key,copy(value));}
test('private milestone index is derived from approved current-version meanings and serves bounded exact hints',async()=>{
  const db=new Store(),p1=product(1,'cardinal-memory'),p2=product(2,'star-achievement'),v1='a'.repeat(64),v2='b'.repeat(64),base='Brites_Growth_Sandbox_';
  await seed(db,base+'Research','one',dossier(p1,'A cardinal may be a personal reminder of a loved one and a memory.',v1));
  await seed(db,base+'Research','two',dossier(p2,'A star may personally mark achievement.',v2));
  await seed(db,base+'Products',core.hash(p1.id).slice(0,40),p1);await seed(db,base+'Products',core.hash(p2.id).slice(0,40),p2);
  const service=core.createGrowthService({db,env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'},now:()=>NOW}),built=await service.rebuildMilestoneIndex();
  assert.equal(built.approvedCount,2);assert.equal(built.counts.remembrance,1);assert.equal(built.counts.achievement,1);assert.equal(built.counts.christmas,0);
  assert.deepEqual(await service.milestoneCandidateHandles(core.intentFrom('A remembrance necklace'),12),[{productId:p1.id,handle:p1.handle,dossierVersion:v1,supplementVersion:null}]);
  const empty=await service.milestoneCandidateHandles(core.intentFrom('A Christmas gift'),12);assert.deepEqual(empty,[]);
});
test('indexed hints still reject malformed identity/version records and never return private fields',async()=>{
  const db=new Store(),p=product(3,'safe-tree'),base='Brites_Growth_Sandbox_',service=core.createGrowthService({db,env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'},now:()=>NOW});
  await seed(db,base+'State','milestone-index-graduation',{schema:1,milestone:'graduation',candidates:[{productId:p.id,handle:p.handle,dossierVersion:'c'.repeat(64),privateNote:'never return'},{productId:'bad',handle:'bad',dossierVersion:'x'}]});
  await seed(db,base+'Products',core.hash(p.id).slice(0,40),p);
  const result=await service.milestoneCandidateHandles(core.intentFrom('Graduation necklace'),99);assert.equal(result.length,1);assert.doesNotMatch(JSON.stringify(result),/privateNote|never return/);
});
test('live namespace cannot build or consume the sandbox milestone index',async()=>{
  const db=new Store(),service=core.createGrowthService({db,env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'},now:()=>NOW});
  await assert.rejects(service.rebuildMilestoneIndex(),/isolated sandbox/i);assert.deepEqual(await service.milestoneCandidateHandles(core.intentFrom('Graduation necklace'),12),[]);
});
