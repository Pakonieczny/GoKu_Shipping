'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const core=require('../../netlify/functions/_britesGrowth'),seed=require('../../netlify/functions/_britesStorefrontSeed');
const NOW=Date.parse('2026-10-08T22:00:00Z');
function product(id,title,type='Earrings',patch={}){
  return {id:'gid://shopify/Product/'+id,handle:'checked-design-'+id,title,type,checkedAt:NOW,url:'https://britesjewelry.com/products/checked-design-'+id,currency:'USD',description:'A checked design. The chain is included.',image:'https://cdn.shopify.com/s/files/1/0001/products/design-'+id+'.jpg',options:[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled']}],variants:[{id:'gid://shopify/ProductVariant/'+id+'01',numericId:id+'01',title:'Sterling Silver',price:45,available:true,options:[{name:'Metal Choice',value:'Sterling Silver'}]},{id:'gid://shopify/ProductVariant/'+id+'02',numericId:id+'02',title:'14k Gold Filled',price:60,available:true,options:[{name:'Metal Choice',value:'14k Gold Filled'}]}],variantsComplete:true,...patch};
}
const butterfly=product(1,'Butterfly Stud Earrings'),bunny=product(2,'Bunny Hoop Earrings'),owl=product(3,'Owl Stud Earrings'),octopus=product(4,'Octopus Stud Earrings'),flower=product(5,'Daisy Stud Earrings'),necklace=product(6,'Butterfly Necklace','Necklace'),leaf=product(7,'Leaf Necklace','Necklace');
const rows=[butterfly,bunny,owl,octopus,flower,necklace,leaf];
function ranked(text,saved={}){return core.rankProducts(rows,core.intentFrom(text,[],saved),NOW,18).map(row=>row.id);}
test('product normalization publishes only visible measurements while preserving exact inventory facts',()=>{
  const source={id:leaf.id,handle:leaf.handle,title:leaf.title,status:'ACTIVE',onlineStoreUrl:leaf.url,productType:leaf.type,descriptionHtml:'<template data-hidden="measurements">The charm measures 1 mm wide and 2 mm high.</template><NoScript><p>The charm measures 3 mm wide and 4 mm high.</p></NoScript><!-- The charm measures 5 mm wide and 6 mm high. --><script>The charm measures 8 mm wide.</script><style>The charm measures 9 mm wide.</style><p>The charm measures <strong>11 mm</strong> wide and 7 mm high.</p>',options:leaf.options,variants:{nodes:leaf.variants.map(v=>({...v,availableForSale:v.available,selectedOptions:v.options})),pageInfo:{hasNextPage:false}}};
  const normalized=core.normalizeProduct(source,'USD',NOW),projected=core.productProjection(normalized);
  assert.equal(normalized.description,'The charm measures 11 mm wide and 7 mm high.');assert.equal(projected.description,normalized.description);
  for(const key of ['id','handle','title','url','currency','checkedAt'])assert.equal(normalized[key],leaf[key],key);
  assert.deepEqual(normalized.options,leaf.options);assert.deepEqual(normalized.variants.map(v=>({id:v.id,numericId:v.numericId,price:v.price,available:v.available,options:v.options})),leaf.variants.map(v=>({id:v.id,numericId:v.numericId,price:v.price,available:v.available,options:v.options})));assert.equal(normalized.variantsComplete,true);
  assert.equal(core.textOf('Before <template>outer hidden <template>inner hidden</template>still hidden</template> after'),'Before after');
  assert.equal(core.textOf('Visible <noscript>hidden without closing tag'),'Visible');
  assert.equal(core.textOf('<p>Visible &amp; exact</p><!-- hidden without closing marker'),'Visible & exact');
});
test('server natural motif and family requests enforce motif plus actual product type',()=>{
  assert.deepEqual(ranked('Do you have butterfly earrings?'),[butterfly.id]);
  for(const text of ['What animal earrings do you have?','Do you offer wildlife earrings?'])assert.deepEqual(new Set(ranked(text)),new Set([butterfly.id,bunny.id,owl.id,octopus.id]),text);
  assert.deepEqual(ranked('What bird earrings do you have?'),[owl.id]);assert.deepEqual(ranked('What pet earrings do you have?'),[bunny.id]);assert.deepEqual(ranked('What floral earrings do you have?'),[flower.id]);
});
test('server broad questions remove the old motif and old necklace-only browse restriction',()=>{
  const saved=core.intentFrom('Leaf necklace');
  for(const text of ['What do you have?','What kinds of jewelry do you sell?',"What's in your collection?",'Can you show me what you have?']){
    const intent=core.intentFrom(text,[],saved);assert.equal(intent.query,'',text);assert.equal(intent.type,null,text);assert.deepEqual(intent.interests,[],text);assert.deepEqual(new Set(core.rankProducts(rows,intent,NOW,18).map(row=>row.id)),new Set(rows.map(row=>row.id)),text);assert.deepEqual(core.freshCatalogueRequest(text,saved),{mode:'reset',type:null},text);
  }
});
test('server corrections replace old motifs while material refinements keep the new family and type',()=>{
  let intent=core.intentFrom('No, I meant butterfly earrings instead',[],core.intentFrom('Leaf necklace'));assert.equal(intent.query,'butterfly');assert.equal(intent.type,'earrings');assert.deepEqual(core.rankProducts(rows,intent,NOW).map(row=>row.id),[butterfly.id]);
  intent=core.intentFrom('Not those, I meant animals',[],intent);assert.equal(intent.query,'animal');assert.equal(intent.type,'earrings');intent=core.intentFrom('In silver under $50',[],intent);assert.equal(intent.query,'animal');assert.equal(intent.metal,'silver');assert.equal(intent.budget,50);assert.deepEqual(new Set(core.rankProducts(rows,intent,NOW,18).map(row=>row.id)),new Set([butterfly.id,bunny.id,owl.id,octopus.id]));
  const old=core.intentFrom('Animal necklaces in silver under USD50 for my sister');
  for(const text of ['Just earrings','Only earrings','Please just earrings please']){const next=core.intentFrom(text,[],old);assert.equal(next.query,'animal',text);assert.deepEqual(next.interests,['animal'],text);assert.equal(next.type,'earrings',text);assert.equal(next.metal,'silver',text);assert.equal(next.budget,50,text);assert.equal(next.budgetCurrency,'USD',text);assert.equal(next.recipient,'sister',text);assert.deepEqual(new Set(core.rankProducts(rows,next,NOW,18).map(row=>row.id)),new Set([butterfly.id,bunny.id,owl.id,octopus.id]),text);assert.deepEqual(core.freshCatalogueRequest(text,old),{mode:'category',type:'earrings',replaceQuery:false},text);}
  const studs=core.intentFrom('Just stud earrings please',[],old);assert.equal(studs.query,'animal');assert.equal(studs.storeCategory,'stud-earrings');assert.equal(studs.metal,'silver');assert.equal(studs.budgetCurrency,'USD');assert.deepEqual(new Set(core.rankProducts(rows,studs,NOW,18).map(row=>row.id)),new Set([butterfly.id,owl.id,octopus.id]));
});
test('server search from a detail page remains discovery while current and exact named facts retain identity',()=>{
  const context={pageKind:'product',currentHandle:leaf.handle,focusedHandle:leaf.handle,inventoryPieces:rows.map(({id,handle,title})=>({id,handle,title})),productHandles:[leaf.handle]};
  for(const text of ['What do you have?',"What's in your collection?",'Do you have butterfly earrings?','What animal earrings do you have?','No I want butterfly earrings instead'])assert.equal(core.productFactRequest(text,context),null,text);
  for(const [text,handle,field]of [['What size is the charm?',leaf.handle,'dimensions'],['Do you have this in silver?',leaf.handle,'materials'],['Do they come in silver?',leaf.handle,'materials'],['What size are Butterfly Stud Earrings?',butterfly.handle,'dimensions'],['What metal is Butterfly Stud Earrings made of?',butterfly.handle,'materials']]){
    const facts=core.productFactRequest(text,context);assert.equal(facts.handle,handle,text);assert.equal(facts.confirmed,true,text);assert.ok(facts.fields.includes(field),text);
  }
});
test('server named motif evidence excludes cross-sells and treats physical Studio earrings as finished jewelry',()=>{
  const studio=product(20,'Butterfly Stud Earrings','Custom Charm Studio'),bare=product(21,'Butterfly Necklace Charm','Charm',{options:[{name:'Charm Type',values:['Necklace Charm','Huggie CHARM SET']}]}),promo=product(22,'Leaf Stud Earrings','Earrings',{description:'Pair with butterfly earrings and animal necklaces.',tags:['wear with butterfly earrings']}),wrong=product(23,'Butterfly Necklace','Necklace');
  const result=core.rankProducts([studio,bare,promo,wrong],core.intentFrom('Find butterfly earrings'),NOW);assert.deepEqual(result.map(row=>row.id),[studio.id]);assert.equal(result[0].partsOnly,false);assert.equal(core.productProjection(bare).partsOnly,true);
});
test('bounded seed rotates real motifs within each structural group without fabricating rows or counts',()=>{
  const same=Array.from({length:100},(_,i)=>product(100+i,'Bunny Stud Earrings '+i)),late=product(900,'Butterfly Stud Earrings'),categories={
    'regular-necklaces':['Leaf Necklace','Necklace','Necklace Length'],
    'beady-necklaces':['Padlock Beady Necklace','Necklace','Necklace Length'],
    'hoop-earrings':['Owl Hoop Earrings','Earrings','Hoop Size'],
    'charm-only':['Heart Necklace Charm','Charm','Charm Type']
  };
  const pool=same.concat(late,Object.values(categories).flatMap(([title,type,option],j)=>Array.from({length:30},(_,i)=>product(1000+j*100+i,title,type,{options:[{name:option,values:[option==='Necklace Length'?'18 Inches':'Published value']}]}))));
  const selected=seed.choose(pool);assert.equal(selected.length,120);assert.equal(new Set(selected.map(row=>row.id)).size,120);assert.deepEqual(seed.counts(selected),Object.fromEntries(seed.CATEGORIES.map(category=>[category,24])));assert.ok(selected.some(row=>row.id===late.id));assert.ok(selected.every(row=>pool.some(original=>original.id===row.id&&original.title===row.title&&original.variants===row.variants)));
});
