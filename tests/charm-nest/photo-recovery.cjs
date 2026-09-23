const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const source=fs.readFileSync('charm-nest-bridge.js','utf8');
function page({paused=false,saved=new Map()}={}){
 const dom=new JSDOM('<main></main>',{runScripts:'outside-only',url:'https://fixture.invalid/'}),w=dom.window,calls=[];let clock=Date.now(),reply=async b=>({images:{}});
 Object.defineProperty(w,'localStorage',{value:{getItem:k=>saved.get(k),setItem:(k,v)=>saved.set(k,v)}});
 if(paused)saved.set('cn.listingPhotoPause.v2',String(clock+86400000));
 w.Date.now=()=>clock;w.S={cloud:{ok:true}};w.ListZoom={clean(){},detach(){},bind(){}};w.cors=x=>x;w.esc=x=>String(x);
 w.api=async(_,body)=>{calls.push(body);return reply(body);};w.Image=class{set src(v){queueMicrotask(()=>this.onload?.());}};
 w.eval(source.slice(source.indexOf('const ListMedia ='),source.indexOf('const fmtT ='))+';this.media=ListMedia;');
 return {w,dom,calls,saved,setReply:fn=>{reply=fn;},advance:ms=>{clock+=ms;},rows:ids=>ids.map(listingId=>({line:{listingId}}))};
}
(async()=>{
 let p=page(),url='https://i.etsystatic.com/first.jpg';
 await p.w.media.listing('123');await p.w.media.listing('123');assert.equal(p.calls.length,1,'negative reads are bounded');
 p.setReply(async b=>({images:Object.fromEntries(b.listingIds.map(id=>[id,url]))}));
 await p.w.media.prepare(p.rows(['123']));assert((await p.w.media.listing('123')).includes(encodeURIComponent(url)),'preparation recovers a prior cache miss');
 const restored=page({saved:p.saved});await restored.w.media.listing('123');assert.equal(restored.calls.length,0,'refresh loads a saved URL without an API request');restored.dom.window.close();p.dom.window.close();
 p=page({saved:new Map([['cn.listingPhotoPause',String(Date.now()+86400000)]])});
 p.setReply(async b=>({images:Object.fromEntries(b.listingIds.map(id=>[id,url]))}));
 await p.w.media.prepare(p.rows(['456']));assert.equal(p.calls[0].prepare,true,'legacy app pause is reassessed by the protected server');assert(p.w.media.peek('456'));p.dom.window.close();
 p=page({paused:true});p.setReply(async b=>({images:Object.fromEntries(b.listingIds.map(id=>[id,url]))}));
 await p.w.media.prepare(p.rows(['456']));assert.equal(p.calls.length,1);assert.equal(p.calls[0].prepare,false,'quota pause still permits cache-only recovery');assert(p.w.media.peek('456'));p.dom.window.close();
 p=page();await p.w.media.listing('789');p.setReply(async()=>({images:{789:url}}));p.advance(600001);assert(await p.w.media.listing('789'),'negative entries expire instead of remaining empty forever');p.dom.window.close();
 p=page();let release;const waiting=new Promise(r=>{release=r;});
 p.setReply(async b=>{if(b.listingIds.includes('100'))await waiting;return{images:Object.fromEntries(b.listingIds.map(id=>[id,url]))};});
 const first=p.w.media.prepare(p.rows(['100']));p.w.media.prepare(p.rows(['200']));release();await first;
 assert(p.w.media.peek('100'));assert(p.w.media.peek('200'),'arrivals during preparation are retained');assert.equal(p.calls.length,2);p.dom.window.close();
 p=page();p.setReply(async b=>({images:{},states:Object.fromEntries(b.listingIds.map(id=>[id,'budget-paused'])),retryAt:Date.now()+86400000}));
 await p.w.media.prepare(p.rows(Array.from({length:205},(_,i)=>String(1000+i))));
 assert.deepEqual(p.calls.map(b=>b.listingIds.length),[100,100,5]);assert.deepEqual(p.calls.map(b=>b.prepare),[true,false,false],'a pause switches remaining batches to cache-only');
 assert(Number(p.saved.get('cn.listingPhotoPause.v2'))>Date.now(),'new server pauses persist across refresh');
 const pausedRefresh=page({saved:p.saved});await pausedRefresh.w.media.prepare(pausedRefresh.rows(['2000']));assert.equal(pausedRefresh.calls[0].prepare,false,'refresh respects the new server pause');pausedRefresh.dom.window.close();
 const count=p.calls.length;await p.w.media.prepare(p.rows(['1000']));assert.equal(p.calls.length,count,'paused misses are not polled repeatedly');
 const host=p.w.document.createElement('span');host.setAttribute('data-listing','');p.w.document.body.append(host);p.w.media.watch(host,()=>p.w.media.listing('1000'),'1000');
 await new Promise(r=>setTimeout(r,20));assert(host.textContent.startsWith('Photo lookup pausedResumes '));assert(host.title.includes('resume'));assert.equal(host.getAttribute('aria-busy'),'false');p.dom.window.close();
 console.log('Photo recovery OK: bounded misses, refresh cache, recovery during quota pauses, 100-listing batches, retained arrivals and honest pause status');
})().catch(e=>{console.error(e);process.exitCode=1;});
