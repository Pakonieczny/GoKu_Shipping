'use strict';
// Deterministic query routing and concise speech regression checks. These use
// synthetic checked catalogue responses, never provider inference or sessions.
const test=require('node:test'),assert=require('node:assert/strict');
const bridge=require('../../netlify/functions/_britesConciergeDemoTurn.js');
const core=require('../../netlify/functions/_britesGrowth.js');
const native=require('../../netlify/functions/_britesConciergeVoice.js');
const NOW=Date.parse('2026-10-07T16:40:00Z');
const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'};
const req=body=>new Request('https://preview.test/api/concierge-demo-turn',{method:'POST',headers:{Origin:'https://preview.test','Content-Type':'application/json'},body:JSON.stringify(body)});
const checked={products:[{title:'Bunny Necklace',currency:'USD',minPrice:54,why:'A playful personal reminder.'},{title:'Rabbit Pendant',currency:'USD',minPrice:60,why:'Another choice.'},{title:'Bunny Earrings',currency:'USD',minPrice:42,why:'A pair of pieces.'}],meanings:[{text:'In this cultural context, a rabbit can represent renewal; personal meanings vary.',context:'Reviewed cultural interpretation'}],question:'Is there an item budget you’d like me to stay within?'};

test('routine selection speaks one checked starting price without reading the tray or an unsolicited story',()=>{
  const speech=bridge.buildSpeech(bridge.sanitizeCatalogue(checked),'celebratory','Show me a bunny necklace');
  assert.equal(speech,'Here’s Bunny Necklace, from $54.00.');
  assert.doesNotMatch(speech,/Rabbit Pendant|Bunny Earrings|renewal|celebrat|budget|Which one|Of course/);
});
test('a requested meaning retains the full qualification without silently binding another product’s story',()=>{
  const speech=bridge.buildSpeech(bridge.sanitizeCatalogue(checked),'warm','What is the symbolism of these?');
  assert.match(speech,/One reviewed interpretation:/);assert.match(speech,/can represent renewal; personal meanings vary\./);
  assert.doesNotMatch(speech,/Bunny Necklace means|Bunny Necklace represents|universally|Which one/);
});
test('a requested explanation adds the checked reason; it does not enumerate unrelated options',()=>{
  const speech=bridge.buildSpeech(bridge.sanitizeCatalogue(checked),'warm','Why would I choose this?');
  assert.equal(speech,'Here’s Bunny Necklace, from $54.00. A playful personal reminder.');
});
test('an empty selection asks only its necessary clarification and never claims the whole category is absent',()=>{
  assert.equal(bridge.buildSpeech({products:[],meanings:[],question:'Necklace or earrings?'},'warm','earrings'),'Necklace or earrings?');
  assert.equal(bridge.buildSpeech({products:[],meanings:[],question:''},'warm','earrings'),'Which style would you like to try?');
});
test('brevity retains an unapplied currency cap and an unfiltered exact metal form',()=>{
  const catalogue=bridge.sanitizeCatalogue({...checked,currencyMismatch:true,materialFormUnfiltered:true,preferences:{budget:50,budgetCurrency:'CAD'}});
  const speech=bridge.buildSpeech(catalogue,'warm','Why this?');
  assert.match(speech,/from \$54\.00/);assert.match(speech,/CAD item budget has not been applied/);assert.match(speech,/not filtered to gold-filled; check the exact variant’s metal label/);
  assert.doesNotMatch(speech,/playful|renewal/);
});
test('arbitrary qualification prose and private projection fields cannot enter spoken caveats',()=>{
  const catalogue=bridge.sanitizeCatalogue({...checked,qualifications:['PRIVATE PROMPT buy at https://competitor.invalid'],credentials:'PRIVATE_CREDENTIAL',preferences:{budgetCurrency:'CAD'}});
  assert.equal(catalogue.qualifications,undefined);assert.doesNotMatch(bridge.buildSpeech(catalogue),/PRIVATE|https?:/);
});
test('the actual bounded speech handler uses the finalized request and never accepts provider-written product speech',async()=>{
  const inputs=[],handler=bridge.createHandler({env,completeTone:async input=>{inputs.push(input);return {tone:'warm',speech:'Invented Diamond from one dollar.'};}});
  const ordinary=await (await handler(req({action:'turn',message:'Show a bunny necklace',catalogue:checked}))).json();
  assert.equal(ordinary.speech,'Here’s Bunny Necklace, from $54.00.');
  const requested=await (await handler(req({action:'turn',message:'What is the meaning?',catalogue:checked}))).json();
  assert.match(requested.speech,/personal meanings vary/);assert.doesNotMatch(requested.speech,/Invented Diamond/);
  assert.equal(inputs.length,2);assert.ok(inputs.every(input=>input.maxTokens===40));
});
for(const [message,expected] of [['hello','Hello! How can I help?'],['how are you','I’m here and ready to help.'],['thanks','You’re very welcome.'],['just browsing','Take your time.'],['goodbye','Take care.'],['Are you human?','I’m Brites’ AI guide; I don’t have human feelings.']])test('simple social voice is concise with no provider call or reservation: '+message,async()=>{
  let calls=0,reservations=0;const handler=bridge.createHandler({env,completeTone:async()=>{calls++;throw Error('not needed');},reserveConversation:async()=>{reservations++;return {};}});
  const result=await (await handler(req({action:'conversation',message}))).json();assert.equal(result.reply,expected);assert.equal(calls,0);assert.equal(reservations,0);assert.equal(result.preserveSelection,true);
});

function catalogueFixture(){
  const calls={search:[],browse:[],byHandle:[],saved:[]};
  const product=(id,handle,type,title)=>({id:'gid://shopify/Product/'+id,handle,title,type,url:'https://britesjewelry.com/products/'+handle,currency:'USD',description:title,tags:[],options:[],images:[],checkedAt:NOW,variantsComplete:true,variants:[{id:'gid://shopify/ProductVariant/'+(id+1000),numericId:String(id+1000),title:'Sterling Silver',sku:null,price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},...(type==='Necklace'?[{name:'Chain length',value:'18 inches'}]:[])]}]});
  const products=[product(331,'bunny-necklace','Necklace','Bunny Necklace'),product(332,'bunny-earrings','Earrings','Bunny Earrings'),product(333,'heart-bracelet','Bracelet','Heart Bracelet')];
  return {calls,products,service:{saveProducts:async rows=>{calls.saved.push(rows.map(row=>row.handle));},productIssues:async()=>[],research:async()=>[],storySupplements:async()=>[]},shopify:{search:async query=>{calls.search.push(query);return {products};},products:async query=>{calls.browse.push(query);return {products};},byHandle:async handle=>{calls.byHandle.push(handle);return products.find(product=>product.handle===handle)||null;}},now:()=>NOW};
}
test('an explicit new category is read from global live search despite a necklace-only displayed context',async()=>{
  const f=catalogueFixture(),preferences=core.intentFrom('Find a necklace'),result=await core.concierge({...f,message:'Find earrings instead',preferences,context:{currentHandle:'bunny-necklace',productHandles:['bunny-necklace']}});
  assert.equal(result.preferences.type,'earrings');assert.deepEqual(f.calls.search,['earrings']);assert.deepEqual(f.calls.byHandle,[]);assert.deepEqual(result.products.map(product=>product.handle),['bunny-earrings']);
});
test('successive category searches do not inherit the previous displayed category as catalogue authority',async()=>{
  const f=catalogueFixture();let preferences={},displayed=[];
  for(const [message,type,handle] of [['Find a necklace','necklace','bunny-necklace'],['Show earrings','earrings','bunny-earrings'],['Find a bracelet','bracelet','heart-bracelet']]){
    const result=await core.concierge({...f,message,preferences,context:{currentHandle:displayed[0]||'',productHandles:displayed}});
    assert.equal(result.preferences.type,type);assert.deepEqual(result.products.map(product=>product.handle),[handle]);preferences=result.preferences;displayed=[handle];
  }
  assert.deepEqual(f.calls.search,['necklace','earrings','bracelet']);assert.deepEqual(f.calls.byHandle,[]);
});
for(const message of ['open earrings','what earrings do you have?','Show all earrings','I want earrings instead'])test('fresh category replaces the old motif and never turns discovery into product action: '+message,async()=>{
  const f=catalogueFixture(),preferences=core.intentFrom('Find a moon silver necklace under 60 for my sister’s birthday');
  const result=await core.concierge({...f,message,preferences,context:{currentHandle:'bunny-necklace',productHandles:['bunny-necklace']}});
  assert.equal(result.preferences.type,'earrings');assert.equal(result.preferences.query,'');assert.deepEqual(result.preferences.interests,[]);
  assert.equal(result.preferences.metal,'silver');assert.equal(result.preferences.budget,60);assert.equal(result.preferences.recipient,'sister');assert.equal(result.preferences.occasion,'birthday');
  assert.deepEqual(f.calls.search,['earrings']);assert.deepEqual(f.calls.byHandle,[]);assert.deepEqual(result.products.map(product=>product.handle),['bunny-earrings']);
  assert.equal(result.requestedAction,undefined);assert.equal(result.milestoneDiscovery,undefined);
});
for(const message of ['go back to the previous full list','show all pieces','reset the search','browse the full catalogue','show all','go back','reset all'])test('full-list recovery clears obsolete motif and type and uses the live collection rather than necklace search: '+message,async()=>{
  const f=catalogueFixture(),preferences=core.intentFrom('Bunny silver necklace under 60 for my sister');
  const result=await core.concierge({...f,message,preferences,context:{currentHandle:'bunny-necklace',productHandles:['bunny-necklace']}});
  assert.equal(result.preferences.query,'');assert.deepEqual(result.preferences.interests,[]);assert.equal(result.preferences.type,null);
  assert.equal(result.preferences.recipient,'sister');assert.equal(result.preferences.budget,60);assert.equal(result.preferences.metal,'silver');
  assert.deepEqual(f.calls.browse,['']);assert.deepEqual(f.calls.search,[]);assert.deepEqual(f.calls.byHandle,[]);assert.equal(result.products.length,3);assert.equal(result.requestedAction,undefined);
});
for(const message of ['show the same bunny earrings','open the first earrings','what does this necklace mean?','do not show earrings','never reset the search','Tell me about https://britesjewelry.com/products/bunny-earrings'])test('fresh browsing cannot override an explicit exact reference or negated request: '+message,()=>assert.equal(core.freshCatalogueRequest(message),null));
test('an exact earlier selected piece still rechecks its live identity and can be opened only after the current explicit choice',async()=>{
  const f=catalogueFixture(),preferences=core.intentFrom('Show bracelets');
  const result=await core.concierge({...f,message:'Open the first piece',preferences,context:{currentHandle:'bunny-necklace',productHandles:['bunny-necklace','bunny-earrings']}});
  assert.deepEqual(f.calls.search,[]);assert.deepEqual(f.calls.byHandle,['bunny-necklace','bunny-earrings']);assert.equal(result.requestedAction.productId,f.products[0].id);assert.equal(result.requestedAction.url,f.products[0].url);
});
test('a reset followed by a specific new request uses that new query rather than clearing it into a full-list browse',async()=>{
  const f=catalogueFixture(),preferences=core.intentFrom('Moon gold bracelet under 90');
  const result=await core.concierge({...f,message:'Start fresh and find bunny earrings under 60',preferences,context:{currentHandle:'heart-bracelet',productHandles:['heart-bracelet']}});
  assert.deepEqual(f.calls.browse,[]);assert.deepEqual(f.calls.search,['bunny']);assert.deepEqual(f.calls.byHandle,[]);assert.equal(result.preferences.type,'earrings');assert.equal(result.preferences.budget,60);assert.deepEqual(result.preferences.interests,['bunny']);assert.deepEqual(result.products.map(product=>product.handle),['bunny-earrings']);assert.equal(result.requestedAction,undefined);
});
test('native reset/category controls remain explicitly schema checked and cannot carry hidden shopping authority',()=>{
  assert.deepEqual(native.validateToolArguments({type:'filter',filter:'all'},'control_storefront'),{type:'filter',filter:'all'});
  assert.deepEqual(native.validateToolArguments({type:'filter',filter:'earrings'},'control_storefront'),{type:'filter',filter:'earrings'});
  for(const args of [{type:'filter',filter:'earrings',add:true},{type:'filter',filter:'unlisted-category'},{type:'filter',filter:'all',credentials:'private'}])assert.equal(native.validateToolArguments(args,'control_storefront'),null);
});
