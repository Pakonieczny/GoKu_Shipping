'use strict';
// Synthetic scroller geometry exercises the actual widget reveal/target logic.
// Browser acceptance, rather than JSDOM, establishes rendered CSS dimensions.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const clone=v=>JSON.parse(JSON.stringify(v)),tick=()=>new Promise(r=>setImmediate(r));
async function settle(){await tick();await tick();await tick();}
const rect=(left,top,width,height)=>({left,top,width,height,right:left+width,bottom:top+height});
const variant={id:'gid://shopify/ProductVariant/27101',numericId:'27101',title:'Sterling Silver / 16 inch / None',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'16 inch'},{name:'Engraving',value:'None'}]};
const pieces=['compass','bunny','moon'].map((name,i)=>({id:'gid://shopify/Product/'+(271+i),handle:name+'-necklace',url:'https://britesjewelry.com/products/'+name+'-necklace',title:name[0].toUpperCase()+name.slice(1)+' Necklace',type:'Necklace',currency:'USD',variants:[{...clone(variant),id:'gid://shopify/ProductVariant/'+(27101+i),numericId:String(27101+i)}],variantsComplete:true,minPrice:54,suggestedVariantId:'gid://shopify/ProductVariant/'+(27101+i),why:'A personal milestone reminder.'}));
function fixture(t){
  const vc=new VirtualConsole(),errors=[];vc.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM('<!doctype html><body></body>',{url:'https://growth-sandbox.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document;
  const script=d.createElement('script');script.src='https://growth-sandbox.example/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  const globalScroll=[],guides=[],network=[];w.scrollTo=(...args)=>globalScroll.push(args);
  w.HTMLElement.prototype.scrollIntoView=function(args){globalScroll.push({node:this,args});};
  w.HTMLElement.prototype.scrollTo=function(arg,y){if(typeof arg==='object'){if(Number.isFinite(arg.left))this.scrollLeft=arg.left;if(Number.isFinite(arg.top))this.scrollTop=arg.top;}else{this.scrollLeft=arg;this.scrollTop=y;}};
  w.BritesConciergeAvatar={create:()=>({setState(){},setEmotion(){},setVisible(){},setPaused(){},setLevel(){},setFloating(){},cancelPerformance(){},triggerGreeting(){},cue(){},focusProduct(){},clearFocus(){},showProduct(){},clearProduct(){}})};
  w.BritesConciergeGuide={create:()=>({setEnabled(){},setPaused(){},guide:target=>{guides.push(target);return true;},clear(){},snapshot:()=>({floating:true})})};
  w.fetch=async(raw,init={})=>{const url=new URL(raw,w.location.href),body=init.body?JSON.parse(init.body):{};network.push({url,body});return {ok:true,json:async()=>url.pathname==='/api/growth/product'?{live:true,product:clone(pieces.find(p=>p.handle===url.searchParams.get('handle')))}:body.message?{live:true,reply:'Here are checked pieces.',preferences:{},products:clone(pieces),meanings:[]}:{}};};
  w.eval(source);const root=d.querySelector('brites-concierge').shadowRoot,selection=root.querySelector('.selection-scroll'),button=label=>[...root.querySelectorAll('button')].find(b=>b.textContent.trim()===label||b.getAttribute('aria-label')===label);
  const viewport=rect(600,500,420,260);selection.style.padding='5px 20px 13px';Object.defineProperties(selection,{clientWidth:{get:()=>viewport.width},clientHeight:{get:()=>viewport.height},scrollWidth:{get:()=>1020},scrollHeight:{get:()=>800},clientLeft:{get:()=>0},clientTop:{get:()=>0}});
  const originalRect=w.HTMLElement.prototype.getBoundingClientRect;
  w.HTMLElement.prototype.getBoundingClientRect=function(){
    if(this===selection)return viewport;
    if(this.classList.contains('concierge-avatar-stage'))return rect(600,100,420,350);
    const card=this.closest('.card');
    if(card){const siblings=[...card.parentElement.children],index=siblings.indexOf(card),before=siblings.slice(0,index).reduce((sum,c)=>sum+(c.querySelector('.options')?320:278)+10,0),left=viewport.left+20+before-selection.scrollLeft,top=viewport.top+5-selection.scrollTop,width=card.querySelector('.options')?320:278;
      if(this===card)return rect(left,top,width,card.querySelector('.review')?470:card.querySelector('.options')?350:130);
      const offset=this.classList.contains('bag-next')?370:this.closest('.review')?330:this.matches('select')?170:this.classList.contains('choose')?90:200;return rect(left+12,top+offset,width-24,34);
    }
    return originalRect.call(this);
  };
  t.after(()=>{try{w.BritesConcierge.close();}catch{}w.close();});
  return {w,root,selection,viewport,button,guides,globalScroll,network,errors,async ready(){w.BritesConcierge.open();await settle();await this.ask();},async ask(){const input=root.querySelector('input[aria-label="Message the gift concierge"]');input.value='Find a necklace';root.querySelector('form').dispatchEvent(new w.Event('submit',{bubbles:true,cancelable:true}));await settle();},async guide(){button('Guide me').click();await settle();guides.length=0;}};
}
function fullyVisibleHorizontally(h,element){const bounds=element.getBoundingClientRect();assert.ok(bounds.left>=h.viewport.left,'Selected card begins inside its own scroll viewport');assert.ok(bounds.right<=h.viewport.right,'Selected card ends inside its own scroll viewport');}

test('new product discovery resets old horizontal and vertical tray offsets',async t=>{
  const h=fixture(t);await h.ready();h.selection.scrollLeft=108;h.selection.scrollTop=125;await h.ask();assert.equal(h.selection.scrollLeft,0);assert.equal(h.selection.scrollTop,0);assert.equal(h.globalScroll.length,0);
});

for(const index of [0,1,2])test('expanding card '+(index+1)+' reveals the complete selected card in the tray without document scrolling',async t=>{
  const h=fixture(t);await h.ready();h.selection.scrollLeft=108;const card=h.root.querySelectorAll('.card')[index];card.querySelector('.choose').click();await settle();fullyVisibleHorizontally(h,card);assert.equal(h.globalScroll.length,0);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('options keep the guide target on the exact variant control after card focus events',async t=>{
  const h=fixture(t);await h.ready();await h.guide();const card=h.root.querySelectorAll('.card')[1];h.selection.scrollLeft=108;card.querySelector('.choose').click();await settle();const select=card.querySelector('select');select.focus();await settle();assert.equal(h.guides.at(-1),select);fullyVisibleHorizontally(h,card);assert.equal(h.globalScroll.length,0);
});

test('review and confirmed bag handoff reveal and guide toward their actual next action',async t=>{
  const h=fixture(t);await h.ready();await h.guide();const card=h.root.querySelector('.card');card.querySelector('.choose').click();await settle();h.selection.scrollLeft=108;card.querySelector('.add').click();await settle();const confirm=card.querySelector('.review .primary');assert.ok(confirm);fullyVisibleHorizontally(h,card);assert.equal(h.guides.at(-1),confirm);confirm.focus();await settle();assert.equal(h.guides.at(-1),confirm);confirm.click();await settle();const bag=card.querySelector('.bag-next');assert.ok(bag);assert.equal(h.guides.at(-1),bag);fullyVisibleHorizontally(h,card);assert.equal(h.globalScroll.length,0);assert.equal(JSON.parse(h.w.sessionStorage.getItem('brites-sandbox-cart')).length,1);
});
