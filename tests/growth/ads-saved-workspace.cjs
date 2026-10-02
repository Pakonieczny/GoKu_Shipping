'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const {createRequire}=require('node:module');
const {readOnlyAdDesignSource}=require('../../scripts/build-growth-ads-sandbox.cjs');
const {readOnlyFirestore}=require('../../netlify/functions/_britesGrowthAdsReadOnly');
const file=path.resolve(__dirname,'../../netlify/functions/googleAdsAdDesign.js'),original=fs.readFileSync(file,'utf8'),adapted=readOnlyAdDesignSource(original);
const loaded={exports:{}};
vm.runInNewContext(adapted,{module:loaded,exports:loaded.exports,require:createRequire(file),Buffer,console,process,URL,setTimeout,clearTimeout},{filename:'sandbox/googleAdsAdDesign.js'});
const copy=value=>JSON.parse(JSON.stringify(value));
const workspaceId='legacy-fixture',groupRef='customers/9/ads/11',workspacePath='State/adDesign/workspaces/'+workspaceId;
const scopeId=crypto.createHash('sha256').update(JSON.stringify(['campaign:42',groupRef,'10'])).digest('hex').slice(0,40),scope='State/adDesignSaved/scopes/'+scopeId;
const error='Product research exceeds the bounded copy allowance; narrow the selected products.';
const asset=id=>({path:'test/'+id+'.png',hash:id,width:1024,height:1024});
const rows=new Map([
  [workspacePath,{workspaceId,sourceSetId:'source-one',context:{campaignId:'42',warnings:[]},settings:{productId:'10',groupRef},job:{id:'job-one',phase:'paused',error,inFlight:{key:'copy',requestId:'request-one'},reservations:[{requestId:'request-one',settled:false}],assets:{square:asset('legacy-paid')},stages:{},leaseUntil:0}}],
  [workspacePath+'/sourceSets/source-one/products/10',{id:'10',title:'Saved product fixture',position:0}],
  [workspacePath+'/imageLibrary/current',{id:'current',kind:'generated',groupRef,ownerProductId:'10',asset:asset('current')}],
  [scope,{}],
  [scope+'/images/history',{id:'history',kind:'generated',groupRef,ownerProductId:'10',asset:asset('history')}],
  [scope+'/designs/finished',{id:'finished',name:'Finished artwork fixture',productId:'10',groupRef,asset:asset('finished'),createdAt:5,document:{objects:[{type:'textbox',text:'Saved design',fontSize:22}]}}]
]);
let writes=0;
function deny(){writes++;throw Error('A read unexpectedly reached a database mutation.');}
class Snapshot{constructor(ref){this.ref=ref;this.id=ref.id;this.exists=rows.has(ref.path);}data(){return this.exists?copy(rows.get(this.ref.path)):undefined;}}
class Ref{constructor(value){this.path=value;this.id=value.split('/').pop();}collection(id){return new Query(this.path+'/'+id);}async get(){return new Snapshot(this);}set(){deny();}update(){deny();}delete(){deny();}}
class Query{constructor(value){this.path=value;}doc(id){return new Ref(this.path+'/'+id);}select(){return this;}async get(){const prefix=this.path+'/',docs=[...rows.keys()].filter(key=>key.startsWith(prefix)&&!key.slice(prefix.length).includes('/')).map(key=>new Snapshot(new Ref(key)));return {docs};}}
class Db{collection(id){return new Query(id);}runTransaction(){deny();}}
(async()=>{
  const db=readOnlyFirestore(new Db()),service=loaded.exports.createAdDesignService({fb:()=>({db}),COL:{state:'State'},env:{},signAsset:async item=>'https://example.com/'+item.path});
  const result=await service.status({workspaceId});
  assert.equal(result.ok,true);assert.equal(writes,0,'opening a saved workspace never attempts maintenance writes');
  assert.deepEqual(Array.from(result.imageLibrary,row=>row.asset.hash).sort(),['current','history','legacy-paid'],'current, archived and legacy paid artwork are retained');
  assert.equal(result.savedDesigns.length,1);assert.equal(result.savedDesigns[0].appearance.layers[0].text,'Saved design','missing appearance is computed in memory');
  assert.equal(rows.get(scope+'/designs/finished').appearance,undefined,'opening artwork cannot persist appearance metadata');
  assert.equal(result.error,error,'legacy preparation error is preserved instead of pretending repair completed');
  assert.equal(result.needsNewRequestApproval,true,'an unresolved provider receipt remains unresolved');
  assert.equal(rows.get(workspacePath).job.reservations[0].settled,false,'read preview cannot settle provider reservations');
  assert.match(result.products[0].title,/fixture/);assert.match(result.savedDesigns[0].url,/test\/finished\.png$/);
  rows.delete(workspacePath+'/sourceSets/source-one/products/10');
  rows.get(workspacePath).sourceSetId=null;
  await assert.rejects(()=>service.status({workspaceId}),/Saved product sources are unavailable/,'unavailable product evidence is not synthesized');
  assert.equal(fs.readFileSync(file,'utf8'),original,'production source remains untouched');
  assert.throws(()=>readOnlyAdDesignSource(original.replace("await ref.collection('imageLibrary').doc(id).set(image);",'await ref.doc("changed").set(image);')),/maintenance changed/,'changed maintenance paths refuse staging rather than silently bypassing adaptation');
  console.log('Growth saved-workspace read preview: artwork, unresolved evidence and no-write checks passed.');
})().catch(error=>{console.error(error);process.exitCode=1;});
