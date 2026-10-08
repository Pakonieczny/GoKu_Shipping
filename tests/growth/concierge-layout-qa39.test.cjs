'use strict';
// Exercise the real helper bootstrap and iframe resource requests. These tests
// establish loading and URL boundaries; browser acceptance establishes pixels.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole,requestInterceptor}=require('jsdom');
const html=fs.readFileSync(require.resolve('../../concierge-layout-qa.html'),'utf8');
async function fixture(t,query=''){
  const requests=[],errors=[],console=new VirtualConsole();console.on('jsdomError',error=>errors.push(error.message));
  const resources={interceptors:[requestInterceptor(request=>{requests.push(new URL(request.url));return new Response('<!doctype html><html><head><title>Deployed storefront fixture</title></head><body></body></html>',{headers:{'Content-Type':'text/html'}});})]};
  const dom=new JSDOM(html,{url:'https://preview.example/concierge-layout-qa.html'+query,runScripts:'dangerously',resources,virtualConsole:console});
  t.after(()=>dom.window.close());await new Promise(setImmediate);await new Promise(setImmediate);
  return {d:dom.window.document,requests,errors,frames:[...dom.window.document.querySelectorAll('iframe')]};
}
test('the default layout helper loads the same storefront in both original viewport sizes',async t=>{
  const f=await fixture(t);assert.equal(f.requests.length,2);assert.equal(f.frames.length,2);
  assert.deepEqual(f.frames.map(frame=>[frame.getAttribute('width'),frame.getAttribute('height')]),[['360','720'],['1024','760']]);
  assert(f.requests.every(url=>url.href==='https://preview.example/concierge-sandbox.html'));assert.equal(f.d.querySelector('[aria-current="page"]').textContent,'Both views');assert.deepEqual(f.errors,[]);
});
test('mobile-only loads one real storefront deep link and never starts the desktop resource',async t=>{
  const f=await fixture(t,'?viewport=mobile&product=strawberry-necklace');assert.equal(f.requests.length,1);assert.equal(f.frames.length,1);
  assert.equal(f.frames[0].getAttribute('width'),'360');assert.equal(f.frames[0].getAttribute('height'),'720');assert.equal(f.frames[0].title,'Mobile concierge layout');
  assert.equal(f.requests[0].href,'https://preview.example/concierge-sandbox.html?product=strawberry-necklace');assert.equal(f.d.querySelector('.views [data-viewport="desktop"]'),null);
  for(const link of f.d.querySelectorAll('.viewport-links a'))assert.equal(new URL(link.href).searchParams.get('product'),'strawberry-necklace');
  assert.equal(f.d.querySelector('[aria-current="page"]').textContent,'Mobile only');assert.deepEqual(f.errors,[]);
});
test('desktop-only starts one unchanged desktop viewport and preserves its exact product target',async t=>{
  const f=await fixture(t,'?viewport=desktop&product=daisy-necklace');assert.equal(f.requests.length,1);assert.equal(f.frames.length,1);
  assert.equal(f.frames[0].getAttribute('width'),'1024');assert.equal(f.frames[0].getAttribute('height'),'760');assert.equal(f.frames[0].title,'Desktop concierge layout');
  assert.equal(f.requests[0].href,'https://preview.example/concierge-sandbox.html?product=daisy-necklace');assert.equal(f.d.querySelector('[aria-current="page"]').textContent,'Desktop only');assert.deepEqual(f.errors,[]);
});
test('unknown viewport values keep the two-frame default without adding another source or query',async t=>{
  const f=await fixture(t,'?viewport=https%3A%2F%2Funtrusted.example%2Fframe&url=https%3A%2F%2Funtrusted.example%2Fstore&product=daisy-necklace');
  assert.equal(f.requests.length,2);assert.equal(f.frames.length,2);assert(f.requests.every(url=>url.href==='https://preview.example/concierge-sandbox.html?product=daisy-necklace'));assert.equal(f.d.body.dataset.viewport,'both');assert.deepEqual(f.errors,[]);
});
test('invalid or ambiguous product values cannot change the iframe origin, path or fallback collection',async t=>{
  for(const product of ['','../other-page','https://untrusted.example/products/daisy','daisy?cart=1','<script>alert(1)</script>','daisy\nnecklace','daisy-necklace\n','daisy-necklace\r','a'.repeat(181)]){
    const f=await fixture(t,'?viewport=mobile&product='+encodeURIComponent(product));assert.equal(f.requests.length,1);assert.equal(f.requests[0].href,'https://preview.example/concierge-sandbox.html');assert.deepEqual(f.errors,[]);
  }
  const duplicate=await fixture(t,'?viewport=mobile&product=daisy-necklace&product=maple-earrings');assert.equal(duplicate.requests.length,1);assert.equal(duplicate.requests[0].href,'https://preview.example/concierge-sandbox.html');
  assert([...duplicate.d.querySelectorAll('.viewport-links a')].every(link=>!new URL(link.href).searchParams.has('product')));assert.deepEqual(duplicate.errors,[]);
});
