const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const html=fs.readFileSync(require.resolve('../../charm-nest-1.html'),'utf8');
const start=html.indexOf('function packingKey('),end=html.indexOf('function renderNestFlow()',start);
const c=vm.createContext({assert,console});
vm.runInContext(fs.readFileSync(require.resolve('../../charm-nest-solver.js'),'utf8'),c);
vm.runInContext(`
const window={},S={settings:{packingAI:'on'},cloud:{ok:true}},calls=[],sent=[];
const activeCharms=sh=>sh.charms,stockFor=()=>({wPt:100,hPt:50}),agent=()=>({}),agentUpdate=()=>{},aiRenderSheet=()=>'';
const CharmNestPDF={drawSegments(){}},document={createElement:()=>({width:0,height:0,getContext:()=>({fillRect(){},save(){},beginPath(){},rect(){},clip(){},fillText(){},restore(){}}),toDataURL:()=>''})};
const grade=index=>({index,adaptability:40,interlock:80,edgeAffinity:90,edgeRole:'long-edge',priority:70,family:'elongated',mates:['concave'],angles:[0,90],note:'straight edge'});
let bad=false,saveFail=false,readFail=false;
const cloud=new Map(),reads=[],writes=[];
const api=async(_,body)=>{
 if(body.op==='getShapeGuidance'){if(readFail)throw Error('cloud lookup failed');reads.push(body.keys);return {profiles:Object.fromEntries(body.keys.filter(k=>cloud.has(k)).map(k=>[k,cloud.get(k)]))};}
 if(body.op==='putShapeGuidance'){if(saveFail)throw Error('cloud save failed');writes.push(body.profiles);for(const {key,profile} of body.profiles)cloud.set(key,JSON.parse(JSON.stringify(profile)));return {count:body.profiles.length};}
 throw Error('unexpected operation');
};
const agentCall=async(mode,payload,opts)=>{calls.push({payload,existing:opts.existingId});opts.onStarted('job-'+calls.length);return {profiles:payload.pieces.map((_,i)=>grade(i)).slice(bad?1:0),pairs:payload.pieces.length>1?[{a:0,b:1,score:90,reason:'fit'}]:[],summary:'guided'};};
const sh={jobId:'a',status:'nesting',metal:'gold',workers:[{postMessage:m=>sent.push(m)}],charms:Array.from({length:161},(_,i)=>({id:'p'+i,hash:'h'+i,w:10,h:10,scale:1,areaPt2:70,bbox:[0,0,10,10],members:[],widthPt:10,heightPt:10}))};
sh.charms.push({...sh.charms[0],id:'duplicate'});
`,c);
vm.runInContext(html.slice(start,end),c);
(async()=>{
  await vm.runInContext(`(async()=>{
    const hints=await requestPackingGuidance(sh);
    assert.deepEqual(calls.map(c=>c.payload.pieces.length),[80,80,1],'every shape is analyzed, beyond the former 120-piece limit');
    assert.equal(Object.keys(hints.profiles).length,162,'duplicate charms inherit their geometry grade');
    assert.equal(hints.partners.duplicate.p1,90,'pair guidance expands to every duplicate instance');
    assert.equal(hints.profiles.p160.edgeRole,'long-edge');
    await requestPackingGuidance(sh);assert.equal(calls.length,3,'unchanged queue reuses its complete response');
    const make=(charms,id)=>({charms,jobId:id,status:'nesting',metal:'silver',workers:[]});
    const overflow=make(sh.charms.slice(0,33),'overflow');
    const spill=await requestPackingGuidance(overflow);
    assert.equal(calls.length,3,'spillover reuses grades with zero AI calls');
    assert.equal(spill.partners.p0.p1,90,'spillover retains explicit pair preferences');
    assert.equal(overflow.packingAdvice.reusedShapes,33);
    const fresh=make([{...sh.charms[0],id:'new-instance'}],'fresh');
    S.packingCatalog={};await requestPackingGuidance(fresh);
    assert.equal(calls.length,3,'fresh workspace retrieves Firebase grades without AI');
    assert.equal(reads.at(-1).length,1);
    const novel={...sh.charms[0],id:'novel',hash:'new-geometry'};
    const mixed=make([sh.charms[0],novel],'mixed');await requestPackingGuidance(mixed);
    assert.equal(calls.length,4);assert.equal(calls.at(-1).payload.pieces.length,1,'only unknown shapes sent to AI');
    const resized=make([{...novel,w:12,widthPt:12}],'resized');await requestPackingGuidance(resized);
    assert.equal(calls.length,5,'different dimensions require their own grade');
    const concurrent={...novel,id:'parallel',hash:'parallel'};
    await Promise.all([requestPackingGuidance(make([concurrent],'c1')),requestPackingGuidance(make([concurrent],'c2'))]);
    assert.equal(calls.length,6,'concurrent sheets share one paid analysis');
    const faulty=make([{...novel,id:'faulty',hash:'faulty'}],'faulty');bad=true;
    await assert.rejects(requestPackingGuidance(faulty),/every shape/);
    assert.equal(faulty.packingAdvice.state,'error');assert.equal(faulty.packingAdvice.hints,null,'incomplete grades hold placement');
    bad=false;await requestPackingGuidance(faulty);
    assert.equal(calls.at(-1).existing,null,'invalid output can be retried');
    const unsaved=make([{...novel,id:'unsaved',hash:'unsaved'}],'unsaved');saveFail=true;
    await assert.rejects(requestPackingGuidance(unsaved),/cloud save/);const count=calls.length;
    saveFail=false;await requestPackingGuidance(unsaved);assert.equal(calls.length,count,'save retry never repeats paid analysis');
    const unread=make([{...novel,id:'unread',hash:'unread'}],'unread');readFail=true;
    await assert.rejects(requestPackingGuidance(unread),/cloud lookup/);assert.equal(calls.length,count,'lookup failure does not silently purchase a replacement');readFail=false;
    // A cached failed job must resume exactly its original batch index mapping.
    const resumed=make([{...novel,id:'resumed',hash:'resumed'}],'resumed');
    resumed.packingAdvice={version:1,key:packingKey(resumed,resumed.charms),state:'pending',batches:[{jobId:'paid-job',result:null}],hints:null};
    await requestPackingGuidance(resumed);assert.equal(calls.at(-1).existing,'paid-job');
    const original=sh.charms.slice(),legacyAdvice=JSON.parse(JSON.stringify(sh.packingAdvice));
    legacyAdvice.batches.forEach(b=>delete b.keys);
    const legacyFirst={...sh,charms:original.slice(0,100),packingAdvice:legacyAdvice};
    const legacyOverflow=make(original.slice(100),'legacy-overflow');
    allSheets=()=>[legacyFirst,legacyOverflow];S.packingCatalog={};cloud.clear();
    const legacyCount=calls.length;await requestPackingGuidance(legacyOverflow);
    assert.equal(calls.length,legacyCount,'old first-sheet paid results migrate even after charms move to spillover');
    assert.equal(legacyOverflow.packingAdvice.hints.partners.p160,undefined);
    assert.equal(legacyOverflow.packingAdvice.hints.partners.p0,undefined);


  })()`,c);
  console.log('Shape guidance runtime OK: batching, spillover, Firebase recovery, pairs, new-only analysis, size changes, concurrency, save failure and job resume');
})().catch(e=>{console.error(e);process.exit(1);});
