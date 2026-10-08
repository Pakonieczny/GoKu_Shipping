'use strict';

const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),{createRequire}=require('node:module');
const fixtureFile=path.join(__dirname,'adversarial-shopping43.test.cjs'),fixtureRequire=createRequire(fixtureFile);
// Reuse the independent acceptance fixture's real host, bridge and widget.
// Suppress its test registration; only provider, network and hardware are fake.
const {fixture,catalogue,product}=new Function('require','__dirname','__filename',fs.readFileSync(fixtureFile,'utf8')+'\nreturn {fixture,catalogue,product};')(
  name=>name==='node:test'?()=>{}:fixtureRequire(name),path.dirname(fixtureFile),fixtureFile
);
const clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};

function checked120(){
  const rows=catalogue();
  for(let i=0;i<110;i++){
    const group=i%3,title=group===0?'Daisy Stud Earrings':group===1?'Rose Chain Necklace':'Heart Charm';
    rows.push(product(43400+i,title+' '+(i+1),{type:group===0?'Earrings':group===1?'Necklace':'Charm',motif:group===0?'daisy':group===1?'rose':'heart',silver:30+i,gold:60+i}));
  }
  assert.equal(rows.length,120);assert.equal(new Set(rows.map(row=>row.id)).size,120);
  return rows;
}
async function warmFixture(t,options={}){
  const h=await fixture(t,{rows:checked120(),...options}),original=h.w.fetch;
  // The ten-row acceptance fixture fits one page. A real120-row preload must
  // instead pass through the production five-page,24-row coverage validator.
  h.w.fetch=async(raw,init={})=>{
    const url=new URL(raw,h.w.location.href);if(url.pathname!=='/api/growth/inventory')return original(raw,init);
    h.requests.push({url,init});const offset=Number(url.searchParams.get('offset')||0),checkedAt=Date.now(),products=h.rows.slice(offset,offset+24).map(row=>({...row,checkedAt})),nextOffset=offset+products.length;
    const body={live:true,checkedAt,products,inventory:{schema:1,total:120,offset,limit:24,loaded:120,detailsLoaded:120,ready:true,partial:false,sourcePartial:false,fingerprint:'b'.repeat(64),expiresAt:checkedAt+300000},pageInfo:{hasNextPage:nextOffset<120,nextOffset:nextOffset<120?nextOffset:null}};
    return {ok:true,json:async()=>clone(body)};
  };
  await h.store.preloadInventory();await settle();assert.equal(h.store.inventoryStatus().ready,true);assert.equal(h.store.inventoryStatus().loaded,120);
  return h;
}
function assertFullBrowse(h){
  const snapshot=h.store.snapshot();assert.equal(snapshot.pageKind,'collection');assert.equal(snapshot.search,'');assert.equal(snapshot.filter,'all');assert.equal(snapshot.loadedPieces.length,120);
  assert.deepEqual(new Set(snapshot.loadedPieces.map(row=>row.id)),new Set(h.rows.map(row=>row.id)));
  assert.equal(h.handles().length,24);assert.match(h.d.querySelector('#result-summary').textContent,/24 of 120 loaded pieces/);assert.doesNotMatch(h.d.querySelector('#result-summary').textContent,/matching/);
  const more=[...h.d.querySelectorAll('#collection-more button')].find(button=>button.textContent==='Explore more pieces');assert(more,'The native collection must retain a way to reveal its remaining loaded pieces');
  return more;
}
async function assertLocalMore(h,requestCount){
  assertFullBrowse(h).click();await settle();assert.equal(h.handles().length,48);assert.equal(h.store.snapshot().loadedPieces.length,120);assert.match(h.d.querySelector('#result-summary').textContent,/48 of 120 loaded pieces/);assert.equal(h.requests.length,requestCount,'Revealing an already checked page needs no product or model request');
}
async function prepareExactBag(h){
  for(const command of ['Open Butterfly Huggie Earrings','Select Sterling Silver','Select 10mm','Set quantity to 2','Add it to my cart']){
    const result=await h.command(command);assert.equal(result.ok,true,result.reply||result.error);
  }
  await settle();const controls=clone(h.store.snapshot().productControls),cart=clone(h.cart());assert.equal(controls.quantity,2);assert.equal(cart.length,1);assert.equal(cart[0].quantity,2);
  return {controls,cart};
}
function assertBoundedOverview(result){
  assert.equal(result.ok,true,result.reply||result.error);assert.equal(result.browseAll,true);assert.equal(result.inventory.loaded,120);assert.equal(result.inventory.catalogueComplete,false);assert(result.products.length<=6,'The assistant response stays bounded independently of the native full collection');assert.match(result.reply,/120 checked listings/);assert.match(result.reply,/necklaces/);assert.match(result.reply,/earrings/);assert.match(result.reply,/charms/);assert.doesNotMatch(result.reply,/\b(?:rings?|bracelets?)\b/);
}

test('typed explicit all restores all120 checked host pieces after a six-card search and retains local pagination',async t=>{
  const h=await warmFixture(t);const narrow=await h.command('Show animal earrings');assert.equal(narrow.ok,true,narrow.reply);assert.equal(h.handles().length,6);assert.equal(h.store.snapshot().loadedPieces.length,6);
  const before=h.requests.length,result=await h.command('Show me all pieces');assertBoundedOverview(result);assertFullBrowse(h);assert.equal(h.requests.length,before);assert.deepEqual(h.cart(),[]);await assertLocalMore(h,before);assert.deepEqual(h.errors,[]);
});

test('typed explicit everything leaves exact product quantity and existing bag intact while restoring all120',async t=>{
  const h=await warmFixture(t),saved=await prepareExactBag(h),before=h.requests.length;
  const result=await h.command('Show me everything');assertBoundedOverview(result);assertFullBrowse(h);assert.equal(h.requests.length,before);assert.deepEqual(h.cart(),saved.cart);
  const reopened=await h.command('Open Butterfly Huggie Earrings');assert.equal(reopened.ok,true,reopened.reply);const controls=h.store.snapshot().productControls;assert.equal(controls.quantity,saved.controls.quantity);assert.equal(controls.variantId,saved.controls.variantId);assert.deepEqual(clone(controls.selectedOptions),saved.controls.selectedOptions);assert.deepEqual(h.cart(),saved.cart);assert.equal(h.requests.length,before);assert.deepEqual(h.errors,[]);
});

test('synthetic native explicit all restores all120 after six presented matches and exposes remaining pages',async t=>{
  const h=await warmFixture(t,{nativeVoice:true});const narrow=await h.command('Show animal earrings');assert.equal(narrow.ok,true,narrow.reply);assert.equal(h.handles().length,6);await h.startVoice();
  const before=h.requests.length,result=await h.say('Show me all pieces');assertBoundedOverview(result);assertFullBrowse(h);assert.equal(h.requests.length,before);assert.deepEqual(h.cart(),[]);await assertLocalMore(h,before);assert.deepEqual(h.errors,[]);
});

test('synthetic native full browse from exact details preserves quantity and bag without another inference or product read',async t=>{
  const h=await warmFixture(t,{nativeVoice:true}),saved=await prepareExactBag(h);await h.startVoice();const before=h.requests.length;
  const result=await h.say('Browse all jewelry');assertBoundedOverview(result);assertFullBrowse(h);assert.deepEqual(h.cart(),saved.cart);assert.equal(h.requests.length,before);
  const reopened=await h.say('Open Butterfly Huggie Earrings');assert.equal(reopened.ok,true,reopened.reply||reopened.error);const controls=h.store.snapshot().productControls;assert.equal(controls.quantity,saved.controls.quantity);assert.equal(controls.variantId,saved.controls.variantId);assert.deepEqual(clone(controls.selectedOptions),saved.controls.selectedOptions);assert.deepEqual(h.cart(),saved.cart);assert.equal(h.requests.length,before);assert.deepEqual(h.errors,[]);
});

test('ordinary inventory question remains a bounded overview and subsequent explicit all still restores actual host browsing',async t=>{
  const h=await warmFixture(t),before=h.requests.length,result=await h.command('What do you have?');assert.equal(result.ok,true,result.reply||result.error);assert.equal(result.browseAll,false);assert.equal(result.inventory.loaded,120);assert.equal(result.products.length,6);assert.equal(h.store.snapshot().loadedPieces.length,6);assert.match(result.reply,/120 checked listings/);
  for(const command of ['Show all','Show me the full collection','Browse the entire collection','List all pieces']){const all=await h.command(command);assertBoundedOverview(all);assertFullBrowse(h);assert.equal(h.requests.length,before,command);}
  assert.deepEqual(h.errors,[]);
});
