const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('charm-nest-bridge.js','utf8');
(async()=>{
 const job={key:'j',lines:['Name'],row:{order:{receiptId:'1'},spec:{designSku:'S'},engrave:{}},copies:['p']},jobs=new Map([['j',job]]);let reviews=0;
 const c={EG:{},loadFonts:async()=>{},F_:{ok:true,Regular:{}},G:{glyphCoverage:()=>({ok:true})},charmFor:()=>({}),Master:{entryFor:()=>({})},fitInput:()=>({}),fitStamp:()=>'',items:()=>jobs,fitClient:()=>({run:async()=>{throw Object.assign(Error('flip does not match'),{stage:'flip'});}}),Review:{add:()=>reviews++},RunCtl:{stopIfRunning(){throw Error('must not stop the whole run');}},agent(){},render(){}};
 vm.createContext(c);const a=source.indexOf('  async function fitJobOnce('),b=source.indexOf('  async function fitAll(',a);vm.runInContext(source.slice(a,b),c);await c.fitJobOnce(job);
 assert.equal(job.state,'blocked');assert.equal(job.row.engrave.state,'blocked');assert.equal(reviews,1);
 let critical=false;
 const w={window:{},backQueue:Promise.resolve(),writeBacksNow:async()=>{throw Object.assign(Error(critical?'storage down':'back did not verify'),critical?{}:{engravingPending:true});},Review:{add:()=>reviews++},agent(){},render(){}};
 vm.createContext(w);const x=source.indexOf('  function writeBacks(job)'),y=source.indexOf('  async function writeBacksNow(',x);vm.runInContext(source.slice(x,y),w);
 job.state='approved';job.row.engrave.approved=true;assert.equal(await w.writeBacks(job),false);assert.equal(job.row.engrave.approved,false);assert.equal(job.state,'blocked');
 critical=true;await assert.rejects(w.writeBacks(job),/storage down/);
 console.log('Pending engraving OK: failed flips and written-file verification hold only the job; storage failures still propagate');
})().catch(e=>{console.error(e);process.exitCode=1;});
