'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const catalogue=require('../../brites-catalogue-intents.js');

// Real animal browsing must distinguish the flying animal from sporting bats.
// These are catalogue answers, not purchase or variant-selection authorities.
for(const [title,handle,expected] of [
  ['Baseball Bat Charm Necklace','baseball-bat-charm-necklace',false],
  ['Softball Bat Necklace','softball-bat-necklace',false],
  ['Flying Bat Charm Necklace','flying-bat-charm-necklace',true],
  ['Cat Baseball Jersey Charm','cat-baseball-jersey-charm',true]
])test('animal discovery respects the published design: '+title,()=>{
  const piece={title,handle,type:'Necklace',description:'Explore our bat and animal designs in the rest of the shop.'};
  const plan=catalogue.discovery('Show me a short list of animal jewellery','').plan;
  assert.equal(catalogue.match(piece,plan,{variants:false}),expected);
});
