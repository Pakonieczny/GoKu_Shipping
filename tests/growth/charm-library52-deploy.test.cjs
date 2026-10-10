'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const deploy=require('../../netlify/functions/_britesConciergeLibraryDeploy.js');
const library=require('../../netlify/functions/_britesCharmMeaningLibrary.js');
const definitions=require('../../netlify/functions/_britesCharmStoryBootstrap.json');
const NOW=definitions.researchedAt+1000,clone=value=>value===undefined?undefined:structuredClone(value);
const env={FIREBASE_PROJECT_ID:'existing-project',FIREBASE_CLIENT_EMAIL:'existing-service@fixture.example',FIREBASE_PRIVATE_KEY:'PRIVATE_DEPLOY52',BRITES_GROWTH_NAMESPACE:deploy.NAMESPACE};
function event(change={},site={}){return {payload:{id:'deploy52',site_id:deploy.SITE_ID,build_id:'gitbuild52',state:'ready',context:'production',branch:deploy.BRANCH,manual_deploy:false,commit_ref:'a'.repeat(40),published_at:new Date(NOW).toISOString(),review_id:null,error_message:null,...change},site:{id:deploy.SITE_ID,...site}};}
function fixture(){const data=new Map(),operations=[],clock={at:NOW};let tail=Promise.resolve(),getHook=null;
 class Ref{constructor(path){this.path=path;this.id=path.split('/').pop();}async get(){operations.push(['get',this.path]);if(getHook)await getHook(this.path);const value=data.get(this.path);return {exists:value!==undefined,data:()=>clone(value),id:this.id,ref:this};}async set(value){operations.push(['set',this.path]);data.set(this.path,clone(value));}}
 class Collection{constructor(path,count=Infinity){this.path=path;this.count=count;}doc(id){return new Ref(this.path+'/'+id);}limit(count){return new Collection(this.path,count);}async get(){operations.push(['query',this.path,this.count]);return {docs:[...data].filter(([key])=>key.startsWith(this.path+'/')).slice(0,this.count).map(([key,value])=>({id:key.split('/').pop(),ref:new Ref(key),data:()=>clone(value)}))};}}
 const db={collection:name=>new Collection(name),runTransaction(fn){const pending=tail.catch(()=>{}).then(async()=>{const writes=[],result=await fn({get:ref=>ref.get(),set:(ref,value)=>writes.push([ref,value])});for(const [ref,value]of writes)await ref.set(value);return result;});tail=pending;return pending;}};
 let connections=0;const migration=deploy.createDeployMigration({makeDb:received=>{connections++;assert.equal(received.BRITES_GROWTH_NAMESPACE,deploy.NAMESPACE);return db;},now:()=>clock.at});
 return {data,operations,clock,db,migration,connections:()=>connections,readHook:fn=>getHook=fn};
}
const markerPath=deploy.NAMESPACE+'_State/'+deploy.MARKER_ID;

test('only the exact published sandbox Git event may access existing Firebase credentials or storage',async()=>{
 const changes=[{site_id:'foreign-site'},{state:'building'},{context:'deploy-preview'},{context:'branch-deploy'},{branch:'main'},{manual_deploy:true},{manual_deploy:undefined},{build_id:null},{commit_ref:null},{commit_ref:'not-a-commit'},{published_at:null},{published_at:new Date(NOW+60001).toISOString()},{review_id:3},{error_message:'PRIVATE_ERROR52'}];
 for(const change of changes){const f=fixture();assert.equal((await f.migration.run(event(change),env)).status,'ignored');assert.equal(f.connections(),0);assert.deepEqual(f.operations,[]);}
 const f=fixture();for(const site of [null,[],{}, {id:'foreign-site'}])assert.equal((await f.migration.run({...event(),site},env)).status,'ignored');assert.equal((await f.migration.run(event(),{...env,BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'})).status,'ignored');assert.equal(f.connections(),0);
 let envReads=0;const handler=deploy.createRequestHandler({getEnv:()=>{envReads++;throw Error('PRIVATE_ENV52');},now:()=>NOW});assert.equal((await handler(new Request('https://fixture.example/.netlify/functions/deploy-succeeded',{method:'POST',body:JSON.stringify(event({context:'deploy-preview'}))}))).status,204);assert.equal(envReads,0);
});

test('documented payload-only legacy event completes actual library seeding with the connected deploy API field shape',async()=>{
 const f=fixture(),logs=[],payload={id:'6ac9b8001234567890abcdef',site_id:deploy.SITE_ID,build_id:'6ac9b8011234567890abcdef',state:'ready',context:'production',branch:deploy.BRANCH,commit_ref:'d954f955708d92ccd69d11f31670d7d70df9b069',manual_deploy:false,published_at:new Date(NOW).toISOString(),review_id:null,error_message:null};
 assert.equal(deploy.checkedDeploy({payload},NOW).siteId,deploy.SITE_ID);const handler=deploy.createRequestHandler({makeDb:()=>f.db,getEnv:key=>env[key],log:value=>logs.push(value),now:()=>NOW});const response=await handler(new Request('https://fixture.example/.netlify/functions/deploy-succeeded',{method:'POST',body:JSON.stringify({payload})}));assert.equal(response.status,204);assert.equal(f.data.get(markerPath).completed,true);assert.equal(f.data.get(markerPath).commitRef,payload.commit_ref);assert.equal(f.data.size,29);assert.equal(logs.length,1);assert.equal(logs[0].validPresentCount,28);assert.equal(logs[0].activeCount,28);assert.equal(logs[0].deployId,payload.id);
});

test('actual 28-record bootstrap is create-missing, read back from Firestore, then skipped by one completion marker',async()=>{
 const f=fixture(),first=await f.migration.run(event(),env);assert.equal(first.status,'completed');assert.equal(first.created,28);assert.equal(first.existing,0);assert.equal(first.validPresentCount,28);assert.equal(first.activeCount,28);assert.match(first.definitionHash,/^[a-f0-9]{64}$/);
 const marker=f.data.get(markerPath);assert.equal(marker.commitRef,'a'.repeat(40));assert.equal(marker.deployId,'deploy52');assert.equal(marker.completed,true);assert.equal(f.data.size,29);assert(f.operations.some(row=>row[0]==='query'&&row[1]===deploy.NAMESPACE+'_CharmStories'));
 const before=clone([...f.data]),position=f.operations.length,second=await f.migration.run(event({id:'later52',commit_ref:'b'.repeat(40)}),env);assert.equal(second.status,'already_complete');assert.deepEqual([...f.data],before);assert.deepEqual(f.operations.slice(position),[['get',markerPath]]);assert.doesNotMatch(JSON.stringify(first),/PRIVATE_DEPLOY52|existing-service|existing-project/);
});

test('existing operator content and inactive choices survive deployment while completion reports actual active count',async()=>{
 const f=fixture(),store=library.createLibrary({db:f.db,now:()=>f.clock.at});await store.bootstrap();const before=(await store.status()).records.find(record=>record.id==='butterfly'),record=Object.fromEntries(Object.entries(before).filter(([key])=>!['version','savedAt'].includes(key)));record.status='inactive';record.interpretation.text='This optional reading can recall a shared garden.';await store.save(record,{expectedVersion:before.version});const saved=clone(f.data.get(deploy.NAMESPACE+'_CharmStories/butterfly'));
 const result=await f.migration.run(event(),env);assert.equal(result.status,'completed');assert.equal(result.created,0);assert.equal(result.existing,28);assert.equal(result.activeCount,27);assert.deepEqual(f.data.get(deploy.NAMESPACE+'_CharmStories/butterfly'),saved);
});

test('invalid stored records are never overwritten or certified by the deployment completion marker',async()=>{
 const f=fixture(),path=deploy.NAMESPACE+'_CharmStories/butterfly';f.data.set(path,{id:'butterfly',context:'PRIVATE_OPERATOR52',version:'invalid'});const before=clone(f.data.get(path)),result=await f.migration.run(event(),env);assert.equal(result.status,'incomplete');assert.equal(f.data.has(markerPath),false);assert.deepEqual(f.data.get(path),before);assert.equal([...f.data.keys()].filter(key=>key.startsWith(deploy.NAMESPACE+'_CharmStories/')).length,28);assert.doesNotMatch(JSON.stringify(result),/PRIVATE_OPERATOR52/);
});

test('concurrent deployment events cannot replace existing records and completion is written once',async()=>{
 const f=fixture(),results=await Promise.all([f.migration.run(event(),env),f.migration.run(event({id:'second52',commit_ref:'b'.repeat(40)}),env)]);assert(results.every(result=>result.ok));assert.equal(results.filter(result=>result.status==='completed').length,1);assert.equal(f.operations.filter(row=>row[0]==='set'&&row[1]===markerPath).length,1);assert.equal(f.data.size,29);assert.equal(f.operations.filter(row=>row[0]==='set'&&row[1].includes('_CharmStories/')).length,28);
});

test('a source expiring during the final marker read is not renewed or marked complete',async()=>{
 const f=fixture();f.clock.at=definitions.researchedAt+30*86400000-1000;let reads=0;f.readHook(async path=>{if(path===markerPath&&++reads===2)f.clock.at+=2000;});const result=await f.migration.run(event(),env);assert.equal(result.status,'incomplete');assert.equal(f.data.has(markerPath),false);for(const record of f.data.values())for(const source of record.sources||[])assert.equal(source.checkedAt,definitions.researchedAt);
});

test('missing credentials and storage errors create no successful receipt and never log private error data',async()=>{
 const f=fixture();assert.equal((await f.migration.run(event(),{...env,FIREBASE_PRIVATE_KEY:''})).status,'unavailable');assert.equal(f.connections(),0);const logs=[],handler=deploy.createRequestHandler({getEnv:key=>env[key],makeDb:()=>{throw Error('PRIVATE_KEY52 '+env.FIREBASE_PRIVATE_KEY);},log:value=>logs.push(value),now:()=>NOW});const response=await handler(new Request('https://fixture.example/.netlify/functions/deploy-succeeded',{method:'POST',body:JSON.stringify(event())}));assert.equal(response.status,503);assert.equal(await response.text(),'');assert.deepEqual(logs,[]);
});

test('reserved filename entry uses modern Request handling and sanitized completion logging without a fetch route',async()=>{
 const filename=require.resolve('../../netlify/functions/deploy-succeeded.js'),source=fs.readFileSync(filename,'utf8');assert.doesNotMatch(source,/config\s*=|fetch\s*\(/);const f=fixture(),logs=[],context={core:{createRequestHandler:options=>deploy.createRequestHandler({...options,makeDb:()=>f.db,now:()=>f.clock.at})},Netlify:{env:{get:key=>env[key]}},console:{info:value=>logs.push(JSON.parse(value))},Response};vm.createContext(context);vm.runInContext(source.replace("import core from './_britesConciergeLibraryDeploy.js';",'').replace('export default ','globalThis.handler = '),context);const response=await context.handler(new Request('https://fixture.example/.netlify/functions/deploy-succeeded',{method:'POST',body:JSON.stringify(event({title:'PRIVATE_TITLE52',log_access_attributes:{token:'PRIVATE_TOKEN52'}}))}));assert.equal(response.status,204);assert.equal(logs.length,1);assert.equal(logs[0].event,'concierge_library_seed_complete');assert.equal(logs[0].validPresentCount,28);assert.doesNotMatch(JSON.stringify(logs),/PRIVATE_|FIREBASE|log_access|title/);
});
