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
