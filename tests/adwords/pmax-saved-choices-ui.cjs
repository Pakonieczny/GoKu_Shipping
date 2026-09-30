// Exercise real checkbox handlers across fresh page instances sharing browser storage.
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {JSDOM}=require('jsdom'),model=require('../../assets/pmax-recommendation.js');
const html=fs.readFileSync(path.resolve(__dirname,'../../brites-adwords.html'),'utf8');
const source=html.slice(html.indexOf('function pmaxProductChoices('),html.indexOf('function renderOpportunities(){'));
const candidate={tag:'pmax-animals|US',handle:'animals',feedLabel:'US',collectionTitle:'Animal necklaces',dailyBudget:17,days:42,
  itemIds:[10,20,30,40,50,60,70].map(id=>'shopify_US_'+id+'_1'),
  offerDetails:[10,20,30,40,50,60,70].map(id=>({itemId:'shopify_US_'+id+'_1',productId:String(id),title:'Necklace '+id}))};
let checks=0;function check(value,message){assert.ok(value,message);checks++;console.log('PASS '+message);}
const records=new Map(),storage={getItem:k=>records.get(k)||null,setItem:(k,v)=>records.set(k,v)};
const pages=[];
function page(rows=[candidate],store=storage){
  const dom=new JSDOM('<main><div id="oppList"></div></main>',{url:'https://brites-adwords.goldenspike.app/'}),document=dom.window.document;
  const toasts=[],context={window:{BritesPmaxRecommendation:model},document,localStorage:store,Intl,Date,Math,JSON,console,setTimeout,clearTimeout,setInterval,clearInterval,
    esc:v=>String(v??'').replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch])),apvEnc:encodeURIComponent,
    friendlyResearchError:String,researchNeedsRefresh:()=>false,wireOpportunityDeletion:()=>{},PMAXAT:Date.now(),PMAXERR:null,toast:v=>toasts.push(v)};
  vm.createContext(context);vm.runInContext(source,context);context.PMAXOPPS=JSON.parse(JSON.stringify(rows));
  const redraw=()=>context.renderPmaxSection(document.getElementById('oppList'));redraw();
  const selected=(i=0)=>Array.from(document.querySelectorAll('.pmx-product[data-i="'+i+'"]:checked'),x=>x.value);
  const choose=(id,on,i=0)=>{const input=document.querySelector('.pmx-product[data-i="'+i+'"][value="'+id+'"]');assert.ok(input);input.checked=on;input.onchange();};
  const p={context,document,selected,choose,redraw,toasts,close:()=>dom.window.close()};pages.push(p);return p;
}
try{
  let p=page();check(p.selected().join()==='10,20,30,40','new suggestions retain the existing four-product default');
  p.choose('40',false);p.choose('50',true);
  check(/Choices saved on this device/.test(p.document.querySelector('.pmx-msg').textContent),'a changed selection reports successful persistence');
  const budget=p.document.querySelector('.pmx-bud');budget.value='23';budget.oninput();
  const ui=p.context.pmaxUi(candidate);ui.working={at:Date.now()};ui.open={products:true};p.context.pmaxSaveUi(candidate);
  check(!/working|open/.test([...records.values()][0]),'running jobs and panel state are never stored as preferences');
  p=page();check(p.selected().join()==='10,20,30,50','a fresh page restores the chosen gecko-position product instead of the fourth default');
  check(p.document.querySelector('.pmx-bud').value==='23'&&!p.document.querySelector('.pmx-gen').disabled,'budget returns without a stale generating lock');
  const design=JSON.parse(decodeURIComponent(p.document.querySelector('[data-pmx-design]').dataset.designOpportunity));
  check(design.itemIds.includes('shopify_US_50_1')&&!design.itemIds.includes('shopify_US_40_1'),'restored choices reach the Design ad payload');
  p.choose('60',true);check(p.selected().join()==='10,20,30,50'&&p.toasts.length===1,'a fifth product is rejected without changing saved choices');
  p=page([{...candidate,tag:'refreshed-tag',offerDetails:[...candidate.offerDetails].reverse()}]);
  check(p.selected().sort().join()==='10,20,30,50','research reordering and a changed display tag preserve product IDs');
  const ca={...candidate,feedLabel:'CA',tag:'pmax-animals|CA'},other={...candidate,handle:'sports',tag:'pmax-sports|US'};
  p=page([candidate,ca,other]);check(p.selected(1).join()==='10,20,30,40'&&p.selected(2).join()==='10,20,30,40','different suggestions and feed markets have separate choices');
  const remaining=candidate.offerDetails.filter(o=>o.productId==='60');
  p=page([{...candidate,itemIds:remaining.map(o=>o.itemId),offerDetails:remaining}]);
  check(!p.selected().length&&p.document.querySelector('.pmx-gen').disabled,'unavailable saved products do not cause unrelated defaults to be selected');
  p=page();p.selected().forEach(id=>p.choose(id,false));p=page();
  check(!p.selected().length&&p.document.querySelector('[data-pmx-design]').disabled,'an explicitly empty selection survives reopening and keeps design disabled');
  p.choose('50',true);check(page().selected().join()==='50','a choice is written synchronously before leaving the page');
  records.set(p.context.pmaxUiStorageKey(candidate),'{broken json');p=page();
  check(p.selected().join()==='10,20,30,40','a damaged stored preference does not break the opportunity list');
  const blocked={getItem:()=>{throw Error('Storage blocked');},setItem:()=>{throw Error('Quota exceeded');}};
  p=page([candidate],blocked);p.choose('40',false);
  check(p.selected().join()==='10,20,30'&&/Could not save/.test(p.document.querySelector('.pmx-msg').textContent),'blocked storage keeps the current choices usable and explains that saving failed');
  p.redraw();check(/Could not save/.test(p.document.querySelector('.pmx-msg').textContent),'the save warning survives a card redraw');
  console.log(checks+' saved-product-choice checks passed.');
}finally{pages.forEach(p=>p.close());}
