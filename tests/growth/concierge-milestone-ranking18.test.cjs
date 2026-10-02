'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const discovery=require('../../netlify/functions/_britesMilestoneDiscovery');

const VERSION='a'.repeat(64);
const linked=(id,handle,changes={})=>({productId:'gid://shopify/Product/'+id,handle,dossierVersion:VERSION,supplementVersion:null,...changes});
const row=(id,handle,changes={})=>({id:'gid://shopify/Product/'+id,handle,url:'https://britesjewelry.com/products/'+handle,title:handle.replaceAll('-',' '),type:'Necklace',tags:[],...changes});
const graduation=discovery.discoveryIntent({milestone:'graduation',query:'',interests:[],excludedInterests:[],type:'necklace',personalization:null});

test('approved meaning-linked candidates do not need a redundant fixed motif in catalogue metadata',()=>{
  const hints=[linked(1,'ballet-shoes-pendant-necklace'),linked(2,'lotus-pendant-necklace')];
  const rows=[row(1,'ballet-shoes-pendant-necklace'),row(2,'lotus-pendant-necklace')];
  assert.ok(rows.every(product=>!graduation.motifs.some(motif=>(product.title+' '+product.type).includes(motif))));
  assert.deepEqual(discovery.rankLinkedCandidates(hints,rows,graduation,{type:'necklace'}),hints);
});

test('literal motif and requested type improve bounded order without becoming evidence gates',()=>{
  const hints=[linked(1,'plain-earrings'),linked(2,'plain-necklace'),linked(3,'star-earrings')];
  const rows=[row(1,'plain-earrings',{type:'Earrings'}),row(2,'plain-necklace'),row(3,'star-earrings',{type:'Earrings',tags:['star']})];
  assert.deepEqual(discovery.rankLinkedCandidates(hints,rows,graduation,{type:'necklace'},2).map(item=>item.handle),['plain-necklace','star-earrings']);
});

test('explicit excluded interests still remove matching mirror designs after recall is broadened',()=>{
  const hints=[linked(1,'star-necklace'),linked(2,'ballet-necklace')];
  const rows=[row(1,'star-necklace',{tags:['star']}),row(2,'ballet-necklace')];
  assert.deepEqual(discovery.rankLinkedCandidates(hints,rows,graduation,{type:'necklace',excludedInterests:['star']}).map(item=>item.handle),['ballet-necklace']);
});

test('ranking keeps exact identity version and Brites URL boundaries and returns no private mirror fields',()=>{
  const good=linked(1,'safe-necklace',{privateNote:'do not return'}),badVersion=linked(2,'bad-version',{dossierVersion:'old'}),badIdentity=linked(3,'wrong-handle'),outside=linked(4,'outside');
  const rows=[row(1,'safe-necklace',{privateNote:'do not return'}),row(2,'bad-version'),row(3,'actual-handle'),row(4,'outside',{url:'https://competitor.example/products/outside'})];
  const result=discovery.rankLinkedCandidates([good,badVersion,badIdentity,outside],rows,graduation,{type:'necklace'});
  assert.deepEqual(result,[{productId:good.productId,handle:good.handle,dossierVersion:VERSION,supplementVersion:null}]);
  assert.doesNotMatch(JSON.stringify(result),/private|note/i);
});

test('candidate ranking is bounded deduplicated deterministic and does not mutate inputs',()=>{
  const hints=Array.from({length:20},(_,index)=>linked(index+1,'piece-'+String(index+1).padStart(2,'0'))),rows=hints.map((hint,index)=>row(index+1,hint.handle));
  const before=JSON.stringify({hints,rows});
  const result=discovery.rankLinkedCandidates([hints[0],...hints,hints[0]],rows,graduation,{type:'necklace'},999);
  assert.equal(result.length,12);
  assert.equal(new Set(result.map(item=>item.productId)).size,12);
  assert.deepEqual(result.map(item=>item.handle),hints.slice(0,12).map(item=>item.handle));
  assert.equal(JSON.stringify({hints,rows}),before);
});
