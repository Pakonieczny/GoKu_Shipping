const assert=require('node:assert/strict'),fs=require('node:fs'),crypto=require('node:crypto');
const {Worker}=require('node:worker_threads');
const G=require('../../charm-nest-geom.js'),Fit=require('../../charm-nest-engrave-fit.js'),ot=require('../../vendor/opentype-1.3.4.min.js');
const T=require('../../charm-nest-text.js');
const bytes=p=>{const b=fs.readFileSync(p);return b.buffer.slice(b.byteOffset,b.byteOffset+b.byteLength);};
const fontBytes={Regular:bytes('vendor/fonts/SourceSans3-Regular.otf'),Semibold:bytes('vendor/fonts/SourceSans3-Semibold.otf'),emoji:bytes('vendor/fonts/NotoEmoji-Regular.ttf'),emojiMap:require('../../vendor/fonts/emoji-sequences.json')};
const emoji=ot.parse(fontBytes.emoji),fonts=Object.fromEntries(['Regular','Semibold'].map(w=>[w,T.withEmoji(ot.parse(fontBytes[w]),emoji,fontBytes.emojiMap,ot.Path)]));
const rect=(x,y,w,h)=>[['m',[x,y]],['l',[x+w,y]],['l',[x+w,y+h]],['l',[x,y+h]],['h']];
const outline={kind:'path',closed:true,stroke:true,strokeRGB:[1,0,0],subpaths:[rect(0,0,40,60)],bbox:[0,0,40,60]};
const hole={...outline,subpaths:[rect(16,46,8,8)],bbox:[16,46,24,54]};
const charm={outline,members:[outline,hole],bbox:outline.bbox};
const input={charm,lines:['Fluffy & Hammy'],lineMode:'auto',viewOptions:{res:6,upAngle:90},maskOptions:{marginMm:.8,keepOut:[]},opts:{minCapMm:1.6,maxHeightFrac:.4,lineGap:.216,minStrokeMm:0,minGapMm:0,tryRotated:false}};
// Run the actual browser worker script in a real OS thread. This is a worker
// protocol/geometry test, not a second browser automation implementation.
const workers=[];
class Adapter {
  constructor(){
    this.w=new Worker(`
      const {parentPort}=require('node:worker_threads'),fs=require('node:fs'),vm=require('node:vm');
      const context=vm.createContext({console,Intl,TextEncoder,TextDecoder,setTimeout,clearTimeout});
      context.self=context; context.postMessage=data=>parentPort.postMessage(data);
      context.importScripts=(...files)=>files.forEach(file=>vm.runInContext(fs.readFileSync(file,'utf8'),context,{filename:file}));
      context.importScripts('charm-nest-engrave-worker.js');
      parentPort.on('message',data=>context.onmessage({data}));
    `,{eval:true});
    this.w.on('message',data=>this.onmessage?.({data}));this.w.on('error',e=>this.onerror?.(e));workers.push(this);
  }
  postMessage(data){this.w.postMessage(data);}
  terminate(){this.w.terminate();}
}
(async()=>{
  // Golden digest recorded from the previous production fitting policy
  // (35a2877), using these exact checked-in font bytes and this cut-out fixture.
  const expected=Fit.calculate(input,fonts,G);
  assert.equal(crypto.createHash('sha256').update(JSON.stringify(expected.fit)).digest('hex'),'5cd3ee6c21276bb13fe11a8b54ec0131d1ff7bf75f7ce5ccd21728c18923f80d','preserve the previous size, weight, wrapping and glyph paths exactly');
  const client=Fit.createClient({WorkerClass:Adapter,url:'charm-nest-engrave-worker.js',fonts:fontBytes});
  let beats=0,maxPause=0,last=performance.now();const timer=setInterval(()=>{const now=performance.now();maxPause=Math.max(maxPause,now-last);last=now;beats++;},10);
  const result=await client.run(input);
  clearInterval(timer);
  assert.deepEqual(result.fit,expected.fit,'worker uses the exact same geometry, fonts and sizing');
  assert(G.verifyInk(result.fit.cmds,result.mask).ok);
  assert.equal(G.at(result.mask,20,50),0,'hanging hole remains forbidden');
  assert(beats>5,'caller keeps servicing events while fitting');
  assert(maxPause<250,`worker fit stalled caller for ${maxPause.toFixed(0)}ms`);
  const emojiInput={...input,lines:['A ♥ V']};
  assert.deepEqual((await client.run(emojiInput)).fit,Fit.calculate(emojiInput,fonts,G).fit,'cached emoji and text fonts remain identical');
  assert.equal(workers.length,1,'subsequent fits reuse one worker and parsed fonts');
  console.log(`Worker geometry OK: exact previous-fit parity, cut-outs, emoji, ${beats} responsive ticks, max event gap ${maxPause.toFixed(1)}ms`);
  // Deterministic protocol cases: bounded queue, startup/crash/timeout cleanup,
  // out-of-order replies ignored, and later jobs survive a failed predecessor.
  const fake=[];
  class Fake {constructor(){this.sent=[];fake.push(this);}postMessage(m){this.sent.push(m);}terminate(){this.dead=true;}}
  const q=Fit.createClient({WorkerClass:Fake,url:'worker',fonts:{},timeoutMs:30});
  const a=q.run({a:1}),b=q.run({b:2});const rejected=assert.rejects(a,/took too long/);
  assert.equal(fake[0].sent.filter(m=>m.type==='fit').length,1,'only one fit is dispatched');
  await rejected;assert(fake[0].dead);assert.equal(fake.length,2);
  fake[1].onmessage({data:{id:1,result:'stale'}});
  fake[1].onmessage({data:{id:2,result:'second'}});assert.equal(await b,'second');
  const c=q.run({c:3}),d=q.run({d:4});const crashed=assert.rejects(c,/crashed/);
  fake[1].onerror(new Error('crashed'));await crashed;
  fake[2].onmessage({data:{id:4,result:'fourth'}});assert.equal(await d,'fourth');
  console.log('Worker queue OK: bounded dispatch, timeout, crash restart, stale reply rejection and continued processing');
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>workers.forEach(w=>w.terminate()));
