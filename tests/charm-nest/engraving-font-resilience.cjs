const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('charm-nest-bridge.js','utf8');
const start=source.indexOf('  const FONT_FILES =',source.indexOf('const Engrave ='));
const end=source.indexOf('  const fontFor =',start);
function fixture(fetch){
  const timers=new Map(),messages=[];let next=0;
  const ctx={AbortController,Promise,Date,Uint8Array,F_:{},fetch,opentype:{parse:()=>({names:{fullName:{en:'Test font'}}}),Path:function(){}},CN:{sha256:async()=> 'test'},window:{CharmNestText:{withEmoji:f=>f}},document:{getElementById:()=>null},agent:(...v)=>messages.push(v),esc:x=>x,setTimeout(fn,ms){assert.equal(ms,12000);timers.set(++next,fn);return next;},clearTimeout(id){timers.delete(id);}};
  vm.createContext(ctx);vm.runInContext(source.slice(start,end),ctx);return {ctx,timers,messages};
}
const turn=async()=>{for(let i=0;i<12;i++)await Promise.resolve();};
(async()=>{
  let signal;
  const hung=fixture(async(_path,opts)=>{signal=opts.signal;return new Promise(()=>{});});
  const pending=hung.ctx.fontResource('font.otf');await turn();
  assert.equal(signal.aborted,false);assert.equal(hung.timers.size,1);
  [...hung.timers.values()][0]();await assert.rejects(pending,/timed out/);
  assert.equal(signal.aborted,true);assert.equal(hung.timers.size,0);
  console.log('PASS: stalled font headers settle within the read deadline and abort the download');

  let lateBody;
  const body=fixture(async()=>({ok:true,arrayBuffer:()=>new Promise(r=>lateBody=r)}));
  const reading=body.ctx.fontResource('font.otf');await turn();
  [...body.timers.values()][0]();await assert.rejects(reading,/timed out/);
  lateBody(new ArrayBuffer(1001));await turn();assert.equal(body.timers.size,0);
  console.log('PASS: stalled font bodies settle and late data cannot revive a failed read');

  let recovering=false,requests=0;
  const fonts=fixture(async path=>{requests++;if(!recovering)return new Promise(()=>{});return {ok:true,arrayBuffer:async()=>new ArrayBuffer(1001),json:async()=>({fontSha256:'test'})};});
  const loading=fonts.ctx.loadFonts();await turn();
  for(let i=0;i<4 && fonts.ctx.F_.loading;i++){for(const fn of [...fonts.timers.values()])fn();await turn();}
  await loading;assert.equal(fonts.ctx.F_.ok,false);assert.equal(fonts.ctx.F_.loading,null);
  recovering=true;await fonts.ctx.loadFonts(true);
  assert.equal(fonts.ctx.F_.ok,true);assert.equal(fonts.ctx.F_.emoji,true);assert.equal(fonts.ctx.F_.loading,null);
  assert(fonts.ctx.F_.workerFonts.Regular);assert(fonts.ctx.F_.workerFonts.Semibold);assert(fonts.ctx.F_.workerFonts.emoji);
  assert(requests>=6);assert.equal(fonts.timers.size,0);
  console.log('PASS: a failed font load releases its shared promise and a later retry restores text and emoji');
})().catch(e=>{console.error(e);process.exitCode=1;});
