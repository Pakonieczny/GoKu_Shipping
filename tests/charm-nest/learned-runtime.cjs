const fs=require('fs'),vm=require('vm'),assert=require('node:assert/strict');
const html=fs.readFileSync('charm-nest-1.html','utf8');const from=html.indexOf('function acceptBestLayout('),to=html.indexOf('async function startServerNest(',from);
const c=vm.createContext({assert,console,CharmNestSolver:require('../../charm-nest-solver.js')});vm.runInContext(`
const window={},finishes=[],messages=[],S={settings:{maxFill:.74}};
const stockFor=()=>({wPt:100,hPt:50}),activeCharms=sh=>sh.charms, agent=()=>({}),agentUpdate=()=>{},log=()=>{},renderProgress=()=>{},renderSat=()=>{},drawPreview=()=>{},computeSaturation=()=>{},finishNest=(sh,r)=>finishes.push(r),fmt={pct:x=>x+'%'};
const sh={jobId:'a',nestFlow:'learned',status:'nesting',charms:[{id:'a'},{id:'b'},{id:'c'}],best:{placements:[{id:'a'},{id:'b'}],density:.5},pool:{n:2,done:0,results:[],trials:[1,1],winner:null},workers:[0,1].map(i=>({postMessage:m=>messages.push({i,...m})}))};
`,c);vm.runInContext(html.slice(from,to),c);vm.runInContext(`
onWorkerMessage(sh,{type:'best',jobId:'a:1',best:{placements:[{id:'a'}],density:.3},summary:{trial:1,total:3}},1);
assert.equal(sh.best.placements.length,2,'a worse challenger cannot replace the visible incumbent');assert.equal(messages[0].type,'incumbent','even weaker challenger updates are needed for honest gain accounting');
const learned={placements:[{id:'a'}],rejects:['b','c'],density:.3,elapsedMs:100,endedBy:'plateau',params:{engine:'learned',model:{model:'TEST'},baselineCount:1}},standard={placements:[{id:'a'},{id:'b'}],rejects:['c'],density:.5,elapsedMs:200,endedBy:'stopped',params:{engine:'solver'}};
onWorkerMessage(sh,{type:'done',jobId:'a:0',result:learned},0);assert.equal(finishes.length,0);assert(messages.some(m=>m.i===1&&m.type==='stop'),'plateau stops cosmetic-only challenger work');
onWorkerMessage(sh,{type:'done',jobId:'a:1',result:standard},1);assert.equal(finishes.length,1);assert.equal(finishes[0].result.placements.length,2);assert.equal(finishes[0].result.endedBy,'plateau');assert.equal(finishes[0].result.params.gainedPieces,0);assert.equal(finishes[0].result.params.selectedSource,'standard challenger');
onWorkerMessage(sh,{type:'learned',jobId:'a:0',progress:{stage:'late',checkpoint:{bad:true}}},0);assert(!sh.learnedCheckpoint,'late progress cannot overwrite final checkpoint');
`,c);console.log('learned runtime OK: monotonic best, challenger exchange, plateau stop, provenance, no false AI gain, stale messages');
