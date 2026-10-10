'use strict';

// Shopper authority, scope and cancellation checks use synthetic current UI.
// The independent native suite exercises the real widget and mounted host.
const test=require('node:test'),assert=require('node:assert/strict');
const bridge=require('../../brites-storefront-bridge.js');
const copy=value=>structuredClone(value);
const groups=[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled']},{name:'Length',values:['16 inches','18 inches']}];
function line(number,metal=number===1?'Sterling Silver':'14k Gold Filled'){
 return {lineId:'current-line-'+number,productId:'gid://shopify/Product/501',variantId:'gid://shopify/ProductVariant/'+(510+number),handle:'elephant-necklace',title:'Elephant Necklace',variant:metal+' / 16 inches',quantity:number===1?2:3,price:51,currency:'USD',optionGroups:copy(groups),selectedOptions:[{name:'Metal Choice',value:metal},{name:'Length',value:'16 inches'}],engravingControls:{available:true,enabled:true,hasText:false,maxLength:30},optionsOpen:false};
}
function bag(){return {controlVersion:1,contextRevision:50,pageKind:'bag',visiblePieces:[],bagControls:{countKnown:true,linesComplete:true,itemCount:5,lines:[line(1),line(2)]},changeControls:{canUndo:false,kind:null},bagNoteControls:{available:true,enabled:true,hasText:false,maxLength:300,savedKnown:true},customizeBriefControls:{available:true,enabled:true,hasText:false,maxLength:300,savedKnown:true},giftControls:{wrappingAvailable:true,giftPackageAvailable:true,noteAvailable:true,wrapping:false,giftPackage:true,hasNote:false,maxLength:300,savedKnown:true}};}
function product(){return {controlVersion:1,contextRevision:50,pageKind:'product',currentHandle:'elephant-necklace',visiblePieces:[{id:'gid://shopify/Product/501',handle:'elephant-necklace',title:'Elephant Necklace'}],productControls:{handle:'elephant-necklace',productId:'gid://shopify/Product/501',variantId:'gid://shopify/ProductVariant/511',quantity:2,optionGroups:copy(groups),selectedOptions:copy(line(1).selectedOptions),optionsOpen:true,openedOption:'Length'}};}
const manifest={controlVersion:1,mode:'sandbox',actions:['bag-options','bag-highlight','close-options','bag-quantity','bag-remove','bag-select-option','bag-clear','bag-note','customize-brief','undo','scroll','select-option','product-quantity'],reviewBeforeAdd:true,checkoutMode:'simulation-only',notesMode:'local-session'};
function harness(initial=bag(),actions=manifest.actions){
 let page=copy(initial),before;const calls=[],privateFields={note:'',brief:''};
 const host={capabilities:{...manifest,actions},snapshot:()=>copy(page),async execute(action,{signal}={}){
  if(signal?.aborted)return {ok:false,cancelled:true};calls.push(copy(action));
  if(action.type==='bag-quantity'){before=copy(page.bagControls);page.bagControls.lines.find(row=>row.lineId===action.lineId).quantity=action.quantity;page.bagControls.itemCount=page.bagControls.lines.reduce((n,row)=>n+row.quantity,0);page.changeControls={canUndo:true,kind:'bag-quantity'};}
  if(action.type==='bag-clear'){before=copy(page.bagControls);page.bagControls.lines=[];page.bagControls.itemCount=0;page.changeControls={canUndo:true,kind:'bag-clear'};}
  if(action.type==='undo'){page.bagControls=before;page.changeControls={canUndo:false,kind:null};}
  if(action.type==='bag-note'){privateFields.note=action.text;page.bagNoteControls.hasText=!!action.text;}
  if(action.type==='customize-brief'){privateFields.brief=action.text;page.customizeBriefControls.hasText=!!action.text;}
  page.contextRevision++;return {ok:true,message:'The exact visible control changed.',cartChanged:['bag-quantity','bag-clear','bag-note'].includes(action.type)};
 }};
 const api=bridge.create({storefront:host});return {api,host,calls,privateFields,get page(){return page;},set(next){page=copy(next);}};
}
function expected(text,page,action){const out=bridge.resolve(text,page);assert.equal(out.ok,true,text+': '+out.reason);assert.deepEqual(out.action,action,text);}

test('ordinal line references and exact currently selected option qualifiers distinguish duplicate product titles',()=>{
 const page=bag(),before=copy(page);
 for(const text of ['Remove the last item from my cart','Remove item number 2 from my bag','Remove the second item from my cart'])expected(text,page,{type:'bag-remove',lineId:'current-line-2'});
 expected('Remove the Sterling Silver Elephant Necklace from my cart',page,{type:'bag-remove',lineId:'current-line-1'});
 expected('Set the 14k Gold Filled Elephant Necklace quantity to 4 in my cart',page,{type:'bag-quantity',lineId:'current-line-2',quantity:4});
 assert.deepEqual(page,before);
});
test('unknown qualifiers, conflicting positions and incomplete carts never borrow a current line',()=>{
 for(const text of ['Remove the Elephant Necklace from my cart','Remove the Platinum Elephant Necklace from my cart','Remove the first last item from my cart','Remove item number 0 from my cart','Remove item number 3 from my cart','Remove the second Sterling Silver Elephant Necklace from my cart','Remove the second Hidden Pendant from my cart'])assert.equal(bridge.resolve(text,bag()).ok,false,text);
 for(const flag of ['countKnown','linesComplete']){const page=bag();page.bagControls[flag]=false;for(const text of ['Remove the last item from my cart','Set quantity of the second item in my cart to 4'])assert.equal(bridge.resolve(text,page).ok,false,text);}
 const same=bag();same.bagControls.lines[1]={...line(1),lineId:'duplicate-current-line'};assert.equal(bridge.resolve('Remove the Sterling Silver Elephant Necklace from my cart',same).ok,false);
});
test('relative quantity changes use the requested exact current row and never infer removal at zero',()=>{
 expected('Increase the second item in my bag quantity by one',bag(),{type:'bag-quantity',lineId:'current-line-2',quantity:4});
 expected('Decrease the quantity of the first item in my cart by one',bag(),{type:'bag-quantity',lineId:'current-line-1',quantity:1});
 expected('Raise quantity by two',product(),{type:'product-quantity',handle:'elephant-necklace',quantity:4});
 for(const text of ['Decrease quantity of the first item in my cart by two','Increase quantity of the second item in my cart by 20','Increase quantity of Elephant Necklace by one','Increase quantity of Hidden Pendant by one','Increase quantity of the first item in my cart by -1','Increase quantity of the first item in my cart by 1.5'])assert.equal(bridge.resolve(text,bag()).ok,false,text);
});
test('relative cart change followed by undo restores the original rows through an issued plan',async()=>{
 const h=harness(),original=copy(h.page.bagControls),out=h.api.resolvePlan('Increase the second item in my cart quantity by one then undo my last change');assert.equal(out.ok,true,out.reason);
 const result=await h.api.executePlan(out.plan);assert.equal(result.ok,true,result.reason);assert.deepEqual(h.page.bagControls,original);assert.deepEqual(h.calls.map(row=>row.type),['bag-quantity','undo']);h.api.destroy();
});
test('bag menu opening and highlighting identify actual row and published group',()=>{
 expected('Open the Length dropdown for the first item in my cart',bag(),{type:'bag-options',lineId:'current-line-1',optionName:'Length'});
 expected('Show the options for the last item in my bag',bag(),{type:'bag-options',lineId:'current-line-2'});
 expected('Highlight the quantity of the first item in my cart',bag(),{type:'bag-highlight',lineId:'current-line-1',section:'quantity'});
 expected('Scroll to the price for the second item in my cart',bag(),{type:'bag-highlight',lineId:'current-line-2',section:'price'});
 expected('Close bag options',bag(),{type:'close-options'});
 for(const text of ['Open bag options','Open the Length dropdown for the Hidden Pendant in my cart','Open the Colour dropdown for the first item in my cart','Highlight quantity and price for the first item in my cart'])assert.equal(bridge.resolve(text,bag()).ok,false,text);
});
test('a spoken ordinal only selects the actual opened cart menu, preserving the other item',()=>{
 const page=bag();page.bagControls.lines[0].optionsOpen=true;page.bagControls.lines[0].openedOption='Length';
 expected('Select the second option',page,{type:'bag-select-option',lineId:'current-line-1',optionName:'Length',optionValue:'18 inches'});
 expected('Select the last one',page,{type:'bag-select-option',lineId:'current-line-1',optionName:'Length',optionValue:'18 inches'});
 expected('Choose the next option',page,{type:'bag-select-option',lineId:'current-line-1',optionName:'Length',optionValue:'18 inches'});
 assert.equal(bridge.resolve('Select the third option',page).ok,false);assert.equal(bridge.resolve('Select the previous option',page).ok,false);
 page.bagControls.lines[0].optionsOpen=false;delete page.bagControls.lines[0].openedOption;assert.equal(bridge.resolve('Select the second option',page).ok,false);
});
test('multiple opened cart groups cannot lend unscoped ordinal authority and actual focused quantity remains literal',()=>{
 const page=bag();for(const row of page.bagControls.lines){row.optionsOpen=true;row.openedOption='Length';}assert.equal(bridge.resolve('Select the second option',page).ok,false);
 page.bagControls.focusedLineId='current-line-2';page.bagControls.focusedOptionName='Metal Choice';expected('Select the first option',page,{type:'bag-select-option',lineId:'current-line-2',optionName:'Metal Choice',optionValue:'Sterling Silver'});
 delete page.bagControls.focusedOptionName;page.bagControls.quantityFocused=true;expected('four',page,{type:'bag-quantity',lineId:'current-line-2',quantity:4});
});
test('published product menu ordinals remain scoped while literal inch choices keep their meaning',()=>{
 const page=product();expected('Select the second option',page,{type:'select-option',handle:'elephant-necklace',optionName:'Length',optionValue:'18 inches'});
 expected('18 inches',page,{type:'select-option',handle:'elephant-necklace',optionName:'Length',optionValue:'18 inches'});expected('18',page,{type:'select-option',handle:'elephant-necklace',optionName:'Length',optionValue:'18 inches'});
 page.productControls.openedOption=null;assert.equal(bridge.resolve('Select the second option',page).ok,false);
 page.productControls.optionsOpen=false;assert.equal(bridge.resolve('Select the second option',page).ok,false);
});
test('an old open menu cannot borrow an unlabelled ordinal after the shopper focuses quantity',()=>{
 const cart=bag();cart.bagControls.lines[0].optionsOpen=true;cart.bagControls.lines[0].openedOption='Length';cart.bagControls.focusedLineId='current-line-1';cart.bagControls.quantityFocused=true;
 const piece=product();piece.productControls.quantityFocused=true;
 for(const page of [cart,piece]){for(const text of ['second','Select the last one','Choose the next'])assert.equal(bridge.resolve(text,page).ok,false,text);assert.equal(bridge.resolve('Select the second option',page).ok,true);}
});
test('combined variant picker ordinals use only complete literal rendered picker rows',()=>{
 const page=product(),pc=page.productControls;pc.openedOption=null;pc.optionsOpen=false;pc.variantPickerFocused=true;
 pc.variantChoices=[{id:'gid://shopify/ProductVariant/511',title:'Sterling Silver / 16 inches',options:copy(line(1).selectedOptions)},{id:'gid://shopify/ProductVariant/512',title:'14k Gold Filled / 16 inches',options:copy(line(2).selectedOptions)}];
 expected('Select the second option',page,{type:'select-option',handle:'elephant-necklace',variantId:'gid://shopify/ProductVariant/512'});
 for(const mutate of [rows=>rows[1].id=rows[0].id,rows=>rows[1].options[1].value='Unpublished',rows=>rows[1].options.pop()]){const broken=copy(page);mutate(broken.productControls.variantChoices);assert.equal(bridge.resolve('Select the second option',broken).ok,false);assert.equal(bridge.sanitizeSnapshot(broken).productControls.variantChoices,undefined);}
});
test('explicit full cart clear binds all currently checked exact line IDs',()=>{
 const page=bag();for(const text of ['Empty my bag','Remove all items from my cart','Clear the test basket completely'])expected(text,page,{type:'bag-clear',lineIds:['current-line-1','current-line-2']});
 for(const flag of ['countKnown','linesComplete']){const partial=copy(page);partial.bagControls[flag]=false;assert.equal(bridge.resolve('Empty my bag',partial).ok,false);}
 const malformed=copy(page);malformed.bagControls.lines[1].lineId='../unknown';assert.equal(bridge.resolve('Empty my bag',malformed).ok,false);
});
test('negation, quoting, hypothetical cart and payment words never authorize a cart mutation',()=>{
 for(const text of ['Do not empty my bag','Empty my bag? No, keep it','The note says "remove all items from my cart"','If I clear my cart what happens','Could you explain how to empty my cart','Increase the second item in my cart by one if I ask','Remove the last item from her cart','Pay for everything in my cart','Buy all items in my cart'])assert.equal(bridge.resolve(text,bag()).ok,false,text);
});
test('bulk clear followed by undo restores the exact current bag through the real bridge executor',async()=>{
 const h=harness(),original=copy(h.page.bagControls),resolved=h.api.resolvePlan('Empty my bag then undo my last change');assert.equal(resolved.ok,true,resolved.reason);
 const result=await h.api.executePlan(resolved.plan);assert.equal(result.ok,true,result.reason);assert.deepEqual(h.page.bagControls,original);assert.deepEqual(h.calls.map(row=>row.type),['bag-clear','undo']);h.api.destroy();
});
test('current revision and exact action comparison stop stale or provider-retargeted bulk deletion',async()=>{
 const h=harness(),request=h.api.resolve('Empty my bag'),original=copy(h.page);
 const forged={...request.action,lineIds:['current-line-1']};assert.equal((await h.api.execute(forged,{transcript:'Empty my bag',context:original,currentTurn:true,inputItemId:'voice-current-50',turnVersion:50})).ok,false);assert.equal(h.calls.length,0);
 const next=copy(original);next.contextRevision++;next.bagControls.lines.push({...line(2),lineId:'new-current-line'});next.bagControls.itemCount+=3;h.set(next);
 assert.equal((await h.api.execute(request.action)).stale,true);assert.equal(h.calls.length,0);assert.deepEqual(h.page,next);h.api.destroy();
});
test('advertised controls and current native turn authority are required before the host can mutate',async()=>{
 const h=harness(bag(),['bag-quantity']),resolved=h.api.resolve('Empty my bag');assert.equal((await h.api.execute(resolved.action)).ok,false);assert.equal(h.calls.length,0);h.api.destroy();
 const native=harness(),action=native.api.resolve('Empty my bag').action;assert.equal((await native.api.execute(action,{transcript:'Empty my bag',context:native.page,currentTurn:false,inputItemId:'old-turn',turnVersion:50})).ok,false);assert.equal(native.calls.length,0);native.api.destroy();
});
test('an aborted delayed cart request cannot commit after the newer shopper request takes over',async()=>{
 const h=harness(),old=h.api.resolve('Empty my bag'),initial=copy(h.page),execute=h.host.execute;let release,entered;const gate=new Promise(resolve=>release=resolve),waiting=new Promise(resolve=>entered=resolve);
 h.host.execute=async(action,options)=>{entered();await gate;return execute(action,options);};const controller=new AbortController(),pending=h.api.execute(old.action,{signal:controller.signal});await waiting;controller.abort();const result=await pending;assert.equal(result.cancelled,true);release();await new Promise(resolve=>setImmediate(resolve));assert.deepEqual(h.page,initial);assert.equal(h.calls.length,0);h.api.destroy();
});
test('duplicate delivery of an already confirmed exact cart clear never repeats the mutation',async()=>{
 const h=harness(),context=copy(h.page),action=h.api.resolve('Empty my bag').action,options={transcript:'Empty my bag',context,currentTurn:true,inputItemId:'one-current-turn',turnVersion:50};assert.equal((await h.api.execute(action,options)).ok,true);assert.equal((await h.api.execute(action,options)).suppressed,true);assert.equal(h.calls.length,1);h.api.destroy();
});
test('private cart note payload preserves case, punctuation and commands as literal field data',()=>{
 const text='PRIVATE_NOTE50\nPlease keep 18 inches, then empty my bag.';expected('Set cart note to '+text,bag(),{type:'bag-note',text});
 expected('Clear my order note',bag(),{type:'bag-note',text:''});assert.equal(bridge.resolvePlan('Set cart note to '+text,bag()).ok,false);assert.equal(bridge.redactRequestText('Set cart note to '+text),'Fill the exact private text in the requested visible shop field.');
});
test('private custom design brief uses only its checked visible enabled field',()=>{
 const text='PRIVATE_BRIEF50: Blue flower; select the second option.';expected('Save my custom design idea as '+text,bag(),{type:'customize-brief',text});expected('Remove my design brief',bag(),{type:'customize-brief',text:''});
 for(const field of ['bagNoteControls','customizeBriefControls']){const page=bag();page[field].enabled=false;assert.equal(bridge.resolve(field==='bagNoteControls'?'Write cart note to Private':'Save design brief to Private',page).ok,false);}
 assert.equal(bridge.resolve('Save design idea to Private',product()).ok,false);
});
test('private field boundaries reject blank, oversized and invisible-control text without another action',()=>{
 for(const text of ['Set cart note to','Set cart note to '+ 'x'.repeat(301),'Save design idea to '+ 'x'.repeat(301),'Clear cart note then empty my bag','Set cart note to Private\u202ePayload','Set design brief to Private\u0000Payload']){const out=bridge.resolve(text,bag());assert.equal(out.ok,false,text.slice(0,70));assert.equal(out.action,undefined);}
});
test('saved private note and design receipts never reveal literal field text or authorize its embedded command',async()=>{
 const h=harness(),before=copy(h.page.bagControls);for(const [command,key] of [['Set cart note to PRIVATE_NOTE50 then empty my bag','note'],['Set design idea to PRIVATE_BRIEF50 then remove the last item','brief']]){
  const resolved=h.api.resolve(command),out=await h.api.execute(resolved.action);assert.equal(out.ok,true,out.reason);assert.match(h.privateFields[key],/PRIVATE_/);assert.doesNotMatch(JSON.stringify(out),/PRIVATE_NOTE50|PRIVATE_BRIEF50|empty my bag|remove the last item/);assert.equal(out.action.text,undefined);
 }
 assert.deepEqual(h.page.bagControls,before);assert.deepEqual(h.calls.map(row=>row.type),['bag-note','customize-brief']);h.api.destroy();
});
test('snapshot retains checked gift package state and limits while omitting private fields',()=>{
 const page=bag();Object.assign(page.giftControls,{giftNote:'PRIVATE_NOTE50',customIdea:'PRIVATE_BRIEF50'});page.bagNoteControls.text='PRIVATE_CART50';page.customizeBriefControls.text='PRIVATE_DESIGN50';page.bagControls.lines[0].engravingText='PRIVATE_ENGRAVING50';
 const safe=bridge.sanitizeSnapshot(page);assert.deepEqual(safe.giftControls,{wrappingAvailable:true,noteAvailable:true,wrapping:false,hasNote:false,savedKnown:true,giftPackageAvailable:true,giftPackage:true,maxLength:300});assert.doesNotMatch(JSON.stringify(safe),/PRIVATE_|giftNote|customIdea|engravingText/);
});
test('field and line focus sanitization cannot lend authority to a missing cart row or unknown option',()=>{
 const page=bag();Object.assign(page.bagControls,{focusedLineId:'missing-current-line',focusedOptionName:'Length',quantityFocused:true});const safe=bridge.sanitizeSnapshot(page);assert.equal(safe.bagControls.focusedLineId,undefined);assert.equal(bridge.resolve('four',page).ok,false);
 page.bagControls.focusedLineId='current-line-1';page.bagControls.focusedOptionName='Hidden Option';assert.equal(bridge.sanitizeSnapshot(page).bagControls.focusedOptionName,undefined);assert.equal(bridge.resolve('Select the second option',page).ok,false);
});
test('direction followups scroll the current page without acquiring item or checkout authority',()=>{
 for(const text of ['Scroll down more','Scroll farther down','Scroll a little down'])expected(text,bag(),{type:'scroll',direction:'down'});
 for(const text of ['Scroll up again','Scroll further up'])expected(text,product(),{type:'scroll',direction:'up'});assert.equal(bridge.resolve('Scroll down and buy all items',bag()).ok,false);
});
test('new control schemas reject unknown private data, duplicate clear IDs and out-of-scope sections',()=>{
 for(const action of [{type:'bag-clear',lineIds:['current-line-1','current-line-1']},{type:'bag-clear',lineIds:[]},{type:'bag-highlight',lineId:'current-line-1',section:'checkout'},{type:'bag-highlight',lineId:'current-line-1',section:'price',optionName:'Length'},{type:'bag-options',lineId:'current-line-1',text:'PRIVATE'},{type:'bag-note',text:'okay',lineId:'current-line-1'},{type:'customize-brief',text:'okay',email:'private@example.test'}])assert.equal(bridge.validateAction(action),null,JSON.stringify(action));
 assert.deepEqual(bridge.validateAction({type:'bag-note',text:'Line one\nLine two'}),{type:'bag-note',text:'Line one\nLine two'});
});
