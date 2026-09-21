// Schema construction has no network/storage dependencies.
const transport=require.resolve('../../netlify/functions/_etsyMailAnthropic.js');
require.cache[transport]={id:transport,filename:transport,loaded:true,exports:{}};
const assert=require('node:assert/strict'),A=require('../../netlify/functions/_charmNestAgent.js'),S=require('../../charm-nest-solver.js');
const image='data:image/png;base64,iVBORw0KGgo=',charm={index:0,id:'a',thumb:image,image,widthPt:12,heightPt:8};
const body={overview:image,charms:[charm],preview:image,contactSheet:image,pieces:[charm],sheet:image,remaining:[charm],strips:[charm],image,personalization:['Test']};
const forbidden=new Set(['maxItems','maxLength','minLength','maximum','minimum','multipleOf','uniqueItems']);
function check(schema,path='schema'){
 for(const [key,value] of Object.entries(schema)){
  assert(!forbidden.has(key),`${path}.${key} unsupported by Claude structured output`);
  if(key==='minItems')assert(value===0||value===1);
  if(schema.type==='object')assert.equal(schema.additionalProperties,false);
  if(value&&typeof value==='object')for(const [k,v] of Object.entries(value))if(v&&typeof v==='object')check(v,path+'.'+key+'.'+k);
  if(key==='items')check(value,path+'.items');
 }
}
for(const mode of ['grouping','layout','packing','place','name','labelRead','engraveIntent','engraveReview']){const req=A.buildRequest(mode,body);assert(!req.error,mode+': '+req.error);check(req.schema);}
const profiles=Array.from({length:130},(_,index)=>({index,adaptability:200,interlock:20,edgeAffinity:-4,priority:50,family:'elongated',mates:Array(12).fill('compact'),angles:[0,10,20,30]}));
const result=S.normalizePackingPlan({profiles,pairs:profiles.flatMap(a=>profiles.filter(b=>b.index>a.index).map(b=>({a:a.index,b:b.index,score:200}))),suggestions:profiles},120);
assert.equal(result.profiles.length,120);assert.equal(result.profiles[0].adaptability,100);assert.equal(result.profiles[0].edgeAffinity,0);assert.equal(result.profiles[0].mates.length,6);assert.equal(result.profiles[0].angles.length,3);assert.equal(result.pairs.length,240);assert.equal(result.suggestions.length,24);
console.log('Agent schemas OK: all eight modes use supported constraints; runtime counts, scores, families and rotations remain bounded');
