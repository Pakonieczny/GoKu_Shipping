'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const qa=require('../../concierge-storefront-qa');
const avatar=require('../../brites-concierge-avatar');

test('actual continuous rig projected to quantized CSS passes observer across neighboring targets',()=>{
  const samples=[],eye={x:{position:0,velocity:0},y:{position:0,velocity:0}},head={x:{position:0,velocity:0},y:{position:0,velocity:0}};let roll={position:0,velocity:0},at=0;
  for(let i=0;i<280;i++){
    const dt=[1/60,1/30,1/120,1/60][i%4],sign=Math.floor(i/7)%2?-1:1;at+=dt*1000;
    for(const axis of ['x','y']){eye[axis]=avatar.advanceGazeAxis(eye[axis],sign*.86,dt,avatar.GAZE_MOTION.eye);head[axis]=avatar.advanceGazeAxis(head[axis],sign*.08,dt,avatar.GAZE_MOTION.headPose);}roll=avatar.advanceGazeAxis(roll,sign*.065,dt,avatar.GAZE_MOTION.headPose);
    const round=value=>Number(value.toFixed(2));samples.push({at,eyeX:round(eye.x.position*.07*180),eyeY:round(eye.y.position*.07*180),headX:round(head.x.position*22),headY:round(head.y.position*22),roll:round(roll.position*57.3)});
  }
  const result=qa.analyzeSamples(samples);assert.equal(result.status,'bounded_dom_movement_observed');assert.equal(result.observedPairs,279);assert.deepEqual(result.violations,[]);assert.ok(result.excursion.eyeX>1);assert.ok(result.excursion.roll>1);
});

test('observer catches visible head teleport while long unobserved gaps remain an explicit limitation',()=>{
  const samples=Array.from({length:20},(_,i)=>({at:i*16.67,eyeX:i*.1,eyeY:0,headX:i*.02,headY:0,roll:i*.03}));samples[10].headX+=2;
  const result=qa.analyzeSamples(samples);assert.equal(result.status,'continuity_violation_observed');assert.ok(result.violations.some(v=>v.channel==='headX'));assert.ok(result.maxima.headX.step>1);
  const gap=samples.map(s=>({...s}));for(let i=10;i<gap.length;i++)gap[i].at+=500;const limited=qa.analyzeSamples(gap);assert.equal(limited.unobservedLongGaps,1);assert.equal(limited.observedPairs,18);
});

test('missing DOM channels and clock discontinuities cannot become an observed pass',()=>{
  const samples=Array.from({length:15},(_,i)=>({at:i*16,eyeX:0,eyeY:0,headX:0,headY:0,roll:0}));samples[5].roll=null;assert.equal(qa.analyzeSamples(samples).status,'invalid_observation');samples[5].roll=0;samples[8].at=samples[7].at;assert.equal(qa.analyzeSamples(samples).status,'invalid_observation');assert.equal(qa.analyzeSamples([]).status,'insufficient_observation');
});

test('resource observation excludes earlier loads, counts forbidden hover work and strips queries',()=>{
  const origin='https://growth-sandbox.example',entries=[{name:origin+'/api/growth/catalogue?q=first',startTime:99,duration:100},{name:origin+'/api/growth/catalogue?q=bunny',startTime:101,duration:23},{name:origin+'/api/growth/product?handle=bunny',startTime:102,duration:24},{name:origin+'/api/growth/concierge?key=PRIVATE_VALUE',startTime:103,duration:30},{name:origin+'/api/growth/voice?token=PRIVATE_VALUE',startTime:104,duration:20},{name:'https://britesjewelry.com/cart/add.js',startTime:105,duration:30},{name:origin+'/brites-concierge-avatar.js?version=30',startTime:106,duration:20}];
  const result=qa.summarizeResources(entries,100,origin);assert.equal(result.catalogueRequests,2);assert.equal(result.conciergeRequests,1);assert.equal(result.voiceEndpointRequests,1);assert.equal(result.shopCartEndpointRequests,1);assert.equal(result.items.length,6);assert.doesNotMatch(JSON.stringify(result),/PRIVATE_VALUE|\?key|\?token|\?q/);assert.equal(result.items.at(-1).kind,'asset');
});

test('host context projection retains immediate identity but no gift, shopper or order contents',()=>{
  const current=qa.boundedContext({contextRevision:17,pageKind:'catalogue',focusedHandle:'second-piece',currentHandle:'',loading:false,visiblePieces:[{},{}],giftMessage:'PRIVATE NOTE',address:'PRIVATE ADDRESS',history:[{content:'PRIVATE TALK'}],checkout:{payment:'PRIVATE CARD'}});
  assert.equal(current.revision,17);assert.equal(current.focusedHandle,'second-piece');assert.equal(current.visibleCount,2);assert.doesNotMatch(JSON.stringify(current),/PRIVATE/);assert.equal(qa.boundedContext(null),null);
});

test('authored path repeatedly crosses both neighboring centres without discontinuous pointer coordinates',()=>{
  const a={x:100,y:250},b={x:340,y:250};let prior=null,min=Infinity,max=-Infinity;
  for(let at=0;at<=3600;at+=10){const p=qa.pointerPoint(a,b,at,3600);min=Math.min(min,p.x);max=Math.max(max,p.x);if(prior)assert.ok(Math.abs(p.x-prior.x)<=17);prior=p;assert.equal(p.y,250);}assert.equal(min,a.x);assert.equal(max,b.x);assert.deepEqual(qa.pointerPoint(a,b,0,3600),{...a,leg:0});assert.equal(qa.pointerPoint(a,b,3600,3600).x,a.x);
});

// Unit schedulers exercise the observer's timer/RAF admission and cleanup;
// they do not change a browser's RAF cadence or establish rendered motion.
function scheduler({rafMs=1000,timerClampMs=0}={}){
  let at=0,seq=0;const jobs=new Map();
  function schedule(callback,delay,kind){const id=++seq;jobs.set(id,{at:at+delay,callback,kind});return id;}
  return {now:()=>at,setTimeout:(callback,delay)=>schedule(callback,Math.max(timerClampMs,delay),'timer'),clearTimeout:id=>jobs.delete(id),requestAnimationFrame:callback=>schedule(callback,rafMs,'raf'),cancelAnimationFrame:id=>jobs.delete(id),pending:()=>jobs.size,
    advanceTo(target){assert.ok(target>=at);let n=0;for(;;){const next=[...jobs].filter(([,job])=>job.at<=target).sort((a,b)=>a[1].at-b[1].at||a[0]-b[0])[0];if(!next)break;assert.ok(++n<3000,'scheduler must terminate');at=next[1].at;jobs.delete(next[0]);next[1].callback(at);}at=target;}};
}
function observationFixture(clock,extra={}){
  const timer=[],frames=[],a={point:{x:100,y:250},box:{left:0,right:200,top:100,bottom:400}},b={point:{x:340,y:250},box:{left:240,right:440,top:100,bottom:400}};
  const promise=qa.observeTimed({...clock,intervalMs:35,durationMs:5400,onStimulus(at){const pt=qa.stimulusPoint(a,b,at),handle=pt.x>=a.box.left&&pt.x<=a.box.right&&pt.y>=a.box.top&&pt.y<=a.box.bottom?'piece-a':pt.x>=b.box.left&&pt.x<=b.box.right&&pt.y>=b.box.top&&pt.y<=b.box.bottom?'piece-b':'';timer.push({...pt,at,expectedHandle:handle,observedHandle:handle});},onFrame(at){frames.push({at,eyeX:0,eyeY:0,headX:0,headY:0,roll:0});},...extra});return {promise,timer,frames};
}

test('timer crosses every planned leg while slow native-frame observations remain insufficient',async()=>{
  const clock=scheduler(),run=observationFixture(clock);clock.advanceTo(5400);const ended=await run.promise;
  assert.equal(clock.pending(),0,'completion clears the next pending native RAF');assert.equal(ended.durationMs,5400);assert.equal(ended.nativeRafSampleCount,5);assert.equal(ended.timerSampleCount,run.timer.length);assert.ok(run.timer.length>150);assert.deepEqual(run.frames.map(s=>s.at),[1000,2000,3000,4000,5000]);
  const context=qa.analyzeStimulus(run.timer),motion=qa.analyzeSamples(run.frames);assert.equal(context.status,'complete_timer_stimulus_observed');assert.equal(context.stressLegsObserved.length,24);assert.ok(context.actualCardSwitches>=24);assert.equal(context.mismatchedPointerSamples,0);assert.equal(context.maxContextSampleGapMs,35);assert.equal(context.unobservedContextGapsOver250Ms,0);assert.equal(motion.status,'insufficient_observation');assert.equal(motion.observedPairs,0);assert.equal(motion.unobservedLongGaps,4);
});

test('timer throttling reports real context gaps and missed legs without replaying skipped work',async()=>{
  const clock=scheduler({timerClampMs:1000}),run=observationFixture(clock);clock.advanceTo(6000);const ended=await run.promise,context=qa.analyzeStimulus(run.timer);
  assert.equal(clock.pending(),0);assert.equal(ended.durationMs,6000);assert.deepEqual(run.timer.map(s=>s.at),[0,1000,2000,3000,4000,5000,6000]);assert.equal(context.status,'insufficient_stimulus_observation');assert.equal(context.coverageComplete,false);assert.equal(context.maxContextSampleGapMs,1000);assert.equal(context.unobservedContextGapsOver250Ms,6);assert.ok(context.stressLegsObserved.length<24);
});

test('cancellation clears both pending schedulers and performs no later pointer or frame work',async()=>{
  const clock=scheduler(),controller=new AbortController(),run=observationFixture(clock,{signal:controller.signal});clock.advanceTo(120);const before=run.timer.length;controller.abort();await assert.rejects(run.promise,/cancelled/);assert.equal(clock.pending(),0);clock.advanceTo(20000);assert.equal(run.timer.length,before);assert.equal(run.frames.length,0);
  const prior=new AbortController();prior.abort();const untouched=scheduler(),early=observationFixture(untouched,{signal:prior.signal});await assert.rejects(early.promise,/cancelled/);assert.equal(untouched.pending(),0);assert.equal(early.timer.length,0);assert.equal(early.frames.length,0);
});

test('a changed product/page guard stops on the timer without waiting for a rendered frame',async()=>{
  const clock=scheduler();let connected=true;const run=observationFixture(clock,{guard(){if(!connected)throw Error('The product cards changed during sampling.');}});clock.advanceTo(120);connected=false;clock.advanceTo(140);await assert.rejects(run.promise,/product cards changed/);assert.equal(clock.pending(),0);assert.equal(run.frames.length,0);const count=run.timer.length;clock.advanceTo(9000);assert.equal(run.timer.length,count);
});

test('an observed immediate-context mismatch or bad stimulus clock cannot become context success',async()=>{
  const clock=scheduler(),run=observationFixture(clock);clock.advanceTo(5400);await run.promise;const samples=run.timer.map(s=>({...s})),i=samples.findIndex(s=>s.phase==='crossing'&&s.expectedHandle);samples[i].observedHandle='previous-piece';const mismatch=qa.analyzeStimulus(samples);assert.equal(mismatch.status,'context_mismatch_observed');assert.equal(mismatch.mismatchedPointerSamples,1);samples[i].observedHandle=samples[i].expectedHandle;samples[10].at=samples[9].at;assert.equal(qa.analyzeStimulus(samples).status,'invalid_stimulus_observation');
});

test('clock failure terminates observation and releases pending native RAF',async()=>{
  const clock=scheduler();let broken=false;const run=observationFixture(clock,{now:()=>broken?NaN:clock.now()});clock.advanceTo(120);broken=true;clock.advanceTo(140);await assert.rejects(run.promise,/clock is unavailable/);assert.equal(clock.pending(),0);assert.equal(run.frames.length,0);
});
