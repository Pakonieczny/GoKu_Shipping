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
let bad=false;
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
    sh.packingAdvice.state='pending';sh.packingAdvice.hints=null;sh.packingAdvice.batches[2].result=null;
    await requestPackingGuidance(sh);assert.equal(calls.length,4,'refresh resumes only the missing batch');assert.equal(calls[3].existing,'job-3');
    assert(calls[3].payload.pieces[0].cachedProfile,'shape grades are reusable');
    sh.charms=[sh.charms[0]];sh.jobId='b';bad=true;
    await assert.rejects(requestPackingGuidance(sh),/every shape/);
    assert.equal(sh.packingAdvice.state,'error');assert.equal(sh.packingAdvice.hints,null,'incomplete guidance cannot launch guided placement');
    bad=false;await requestPackingGuidance(sh);assert.equal(sh.packingAdvice.state,'done');
    assert.equal(calls.at(-1).existing,null,'an incomplete response is retained for diagnosis but retry can obtain a corrected analysis');
  })()`,c);
  console.log('Shape guidance runtime OK: full-queue batching, duplicates, cached grades, response reuse, partial resume and incomplete-analysis hold');
})().catch(e=>{console.error(e);process.exit(1);});
