const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const src=fs.readFileSync('charm-nest-bridge.js','utf8');
let result, sent;
const ctx={S:{settings:{engraveConfidence:.65}},Master:{entryFor:()=>({engravable:false})},ensureJob:row=>(row.job ||= {row,copies:['copy']}),agentCall:async(mode,payload)=>{sent=payload;return result},agent(){},setReady:async j=>{j.state='ready';j.row.engrave={needed:true,approved:false};return j},toWords:(j,why)=>{j.state='words';j.reason=why;return j},setNone:(j,why)=>{j.state='none';j.reason=why}};
vm.createContext(ctx);vm.runInContext(src.slice(src.indexOf('  async function classify(row)'),src.indexOf('  function setNone(job')),ctx);
const row=()=>({order:{receiptId:'test'},line:{title:'Sheep'},spec:{engraveCandidate:true,designSku:'SHEEP3',personalization:['S'],size:'M'}});
(async()=>{
 result={engrave:true,text:'S',source:'personalization',confidence:.5,questions:['The design is listed as not engravable'],requests:{side:'back'}};
 let r=row(),j=await ctx.classify(r);assert.equal(sent.engravable,true);assert.equal(j.state,'ready');assert.equal(j.text,'S');assert.equal(r.engrave.approved,false,'preview never grants approval');
 result={...result,questions:['Should the date be above the name?'],requests:{side:'front',font:'Script'}};j=await ctx.classify(row());assert.equal(j.state,'ready','customer requests accompany a preview');assert.equal(j.requests.font,'Script');assert.equal(j.questions.length,1);
 result={skipped:'offline'};j=await ctx.classify(row());assert.equal(j.state,'ready');assert.equal(j.text,'S','offline proposal is verbatim');
 result={engrave:false,text:'',source:'none',confidence:1,questions:[]};j=await ctx.classify(row());assert.equal(j.state,'none','explicit no-engraving decision retained');
 result={...result,confidence:.3,questions:['Which inscription?']};j=await ctx.classify(row());assert.equal(j.state,'words','no inscription must not be invented');
 result={engrave:true,text:'Anna Ben',source:'personalization',confidence:.9,questions:[]};r=row();r.spec.personalization=['Anna','Ben'];j=await ctx.classify(r);assert.equal(j.text,'Anna\nBen','typed line breaks preserved');
 const jobs=[{state:'words',row:{},lines:['S'],copies:['1'],questions:['The design is not engravable']},{state:'words',row:{},lines:['?'],copies:['2'],missing:['?']},{state:'written',row:{},lines:['Saved'],copies:['3']},{state:'words',row:{},lines:[],copies:['4']}];
 let fits=0,ready=0,release;const pause=new Promise(r=>release=r);
 const rc={items:()=>new Map(jobs.map((j,i)=>[i,j])),setReady:async(j,wake)=>{assert.equal(wake,false);ready++;await pause;j.state='ready'},sheetFor:()=>({fileBase:'saved'}),fitJob:async j=>{fits++;j.state='review'},render(){},RunCtl:{poke(){}}};vm.createContext(rc);vm.runInContext(src.slice(src.indexOf('  let previewRecovery ='),src.indexOf('  async function classifyAll(run)')),rc);
 const first=rc.prepareWaitingPreviews();await rc.prepareWaitingPreviews();assert.equal(ready,1,'consecutive renders share one recovery pass');release();await first;assert.equal(fits,1);assert.equal(jobs[0].state,'review');assert.equal(jobs[0].questions.length,0);assert.equal(jobs[1].state,'words','unsupported glyph keeps actionable repair');assert.equal(jobs[2].state,'written','saved approval untouched');
 // Recovery and the run worker share one fit; exceptions release it for retry.
 let fitCalls=0,finish;const fc={fitJobOnce:async()=>{fitCalls++;await new Promise(r=>finish=r)}};vm.createContext(fc);vm.runInContext(src.slice(src.indexOf('  const fitTasks ='),src.indexOf('  async function fitJobOnce(job)')),fc);
 const shared={};const fitA=fc.fitJob(shared),fitB=fc.fitJob(shared);assert.equal(fitA,fitB);await Promise.resolve();assert.equal(fitCalls,1);finish();await fitA;
 fc.fitJobOnce=async()=>{throw Error('geometry')};await assert.rejects(fc.fitJob(shared),/geometry/);fc.fitJobOnce=async()=>42;assert.equal(await fc.fitJob(shared),42,'failed fitting does not poison later retries');
 // Exercise the actual server prompt builder without credentials or a model call.
 const agentSource=fs.readFileSync('netlify/functions/_charmNestAgent.js','utf8');const server={module:{exports:{}},process:{env:{}},require:n=>n==='./_charmNestAuth'?{str:(v,n)=>String(v||'').slice(0,n),num:v=>+v||0}:{}};vm.createContext(server);vm.runInContext(agentSource,server);
 const req=server.module.exports.buildRequest('engraveIntent',{engravable:false,personalization:['S']});assert(req.content[0].text.includes('Design can take engraving: yes'));assert(!req.content[0].text.includes('NO —'));assert(req.system.includes('valid initial'));assert(req.system.includes('Every design'));
 console.log('Engraving review OK: legacy eligibility ignored, low-confidence/offline previews, typed lines, no-engraving decisions, single recovery pass and server prompt');
})().catch(e=>{console.error(e);process.exitCode=1});
