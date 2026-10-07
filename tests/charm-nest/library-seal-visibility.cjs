// (7 Oct 2026: the Library draws ONE seal per completed sheet, its latest LASER CUT, 72 px; every stamp stays in the data: tests/charm-nest/library-seals.cjs is the browser test of it.)
// Readiness never creates ink. Only the separately recorded process history is stamped on Library cards.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const {JSDOM}=require('jsdom'),R=require('../../charm-nest-readiness.js');
const html=fs.readFileSync('charm-nest-1.html','utf8'),library=fs.readFileSync('charm-nest-library.js','utf8');
// jsdom's stylesheet parser (cssstyle) drops a declaration whose value has min(), max() or clamp() around var() -- valid CSS that every
// browser keeps, such as the Library card cap `width:min(44px,var(--seal-fit,var(--seal-base)))` -- so the seal would measure 50 px here
// whatever the stylesheet says. The production stylesheet is loaded with each such value held verbatim in a custom property (which jsdom
// keeps) and the property set to var() of it: the same cascade and the same expression, resolved by the helpers below as any var() is.
const jsdomCss=css=>css.replace(/([a-z-]+)\s*:\s*([^;{}]*\b(?:min|max|clamp)\([^;{}]*var\([^;{}]*)(?=[;}])/g,(_,prop,value)=>`--jsdom-${prop}:${value};${prop}:var(--jsdom-${prop})`);
const dom=new JSDOM('<body><style></style></body>',{runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,d=w.document;
w.matchMedia=()=>({matches:true});w.requestAnimationFrame=()=>1;w.cancelAnimationFrame=()=>{};w.setInterval=()=>1;
w.Element.prototype.getAnimations=()=>[];const production=d.querySelector('style');production.textContent=jsdomCss(html.match(/<style>([\s\S]*?)<\/style>/)[1]);   // (the seal renderer adds a stylesheet of its own ahead of this one: say which is meant)
w.eval(fs.readFileSync('charm-nest-motion.js','utf8'));
const Seal=w.Seal,context=vm.createContext({window:w,Seal,CharmNestReadiness:R,doc:d,L:{pendingSeals:new Set(),presses:new Set()},isDone:r=>+r.laserDoneAt>0});   // (isDone: the page's own test, the record's laserDoneAt)
const start=library.indexOf("  /* ── The Library's seals"),end=library.indexOf('  function cards(',start);
vm.runInContext(library.slice(start,end),context);
const at=Date.UTC(2026,9,1,2,31),history=[
 {id:'ready-1',how:'laserReady',at,by:'Paul'},
 {id:'cut-1',how:'laserDone',at:at+60000,by:'Seth'},
 {id:'ready-2',how:'laserReady',at:at+120000,by:'Alex'}
];
function number(node,name){
 let expression=w.getComputedStyle(node).getPropertyValue(name);
 for(let step=0;expression.includes('var(')&&step<20;step++)expression=expression.replace(/var\((--[\w-]+)(?:,([^()]*))?\)/g,(_,key,fallback)=>{
  for(let n=node;n;n=n.parentElement){const value=w.getComputedStyle(n).getPropertyValue(key).trim();if(value)return value;}
  return fallback || '';
 });
 expression=expression.replace(/px\b/g,'').replace(/\bmin\(/g,'Math.min(').replace(/\bmax\(/g,'Math.max(').replace(/calc\(/g,'(');
 assert(/^[\d\s.+\-*/,()Mathminax]+$/.test(expression),'numeric CSS expression');return Function('return ('+expression+')')();
}
try{
 for(const ready of [false,true])for(const scope of ['Sheet','Set']){
  const report={ready,required:3,saved:1},source={processSeals:history,laserDoneAt:at+60000,laserDoneBy:'Seth'};
  const before=JSON.stringify({report,source});
  assert.equal(R.seal(report,scope,source),'','computed readiness cannot create or duplicate a process seal');
  const host=d.createElement('span');host.innerHTML=R.counter(report,scope,source);
  assert.equal(host.textContent,'1 / 3');assert.equal(host.querySelector('svg,.seal,.laserSeal'),null,'the counter contains no unearned artwork');
  assert.equal(JSON.stringify({report,source}),before,'rendering preserves readiness data and all historical records');
 }
 assert.equal(R.counter({ready:false,required:0,saved:0}),'','plain sheets get no placeholder');
 assert.equal(Seal.BASE_SIZE,50);assert.equal(Seal.HOVER_SIZE,undefined,'no fixed enlarged size: every seal, in the Library too, zooms in place by the one adaptive curve');
 assert.equal(Seal.SHEET_SIZE,72,'the Library sheet seal is 72 px (it was 44)');assert.equal(Seal.SHEET_GROW,1.8,'and grows x1.8 where it stands (it was x1.08..x1.3)');assert.equal(Seal.zoomScale(72,Seal.SHEET_CAP,Seal.SHEET_GROW),1.8);assert(Seal.zoomScale(72,Seal.SHEET_CAP,Seal.SHEET_GROW)>Seal.zoomScale(72),'bigger than the shared curve gives a 72 px seal');
 assert.equal(Seal.zoomScale(44),Seal.zoomScale(44,96,0),'a seal that asks for no growth keeps the shared curve');
 // the sheet's one seal: the latest completion, only while the sheet is completed, never a readiness seal, never on a set
 const cut=context.cutStamp;
 assert.equal(cut({processSeals:history,laserDoneAt:0}),null,'a sheet that is not completed shows no seal (its stamps stay in the data)');
 assert.equal(cut({processSeals:history.filter(x=>x.how==='laserReady'),laserDoneAt:0}),null,'approved only: no LASER READY seal on a sheet');
 assert.equal(cut({processSeals:history,laserDoneAt:at+60000}).id,'cut-1','completed: its LASER CUT, never the readiness stamps around it');
 const twice=[...history,{id:'cut-2',how:'laserDone',at:at+300000,by:'Seth'},{id:'ready-3',how:'laserReady',at:at+400000,by:'Alex'}];
 assert.equal(cut({processSeals:twice,laserDoneAt:at+300000}).id,'cut-2','completed twice (an undo between): the latest completion only');
 assert.equal(cut({processSeals:[...twice].reverse(),laserDoneAt:at+300000}).id,'cut-2','whatever order the record keeps them in');
 assert.equal(cut({processSeals:[],laserDoneAt:at+60000}).how,'laserDone','an older record with only its completion time shows that one (nothing is made up)');
 assert.equal(cut({processSeals:[],laserDoneAt:1}),null,'the page\'s "marked, time not read yet" placeholder is never drawn as a 1970 seal');
 assert.equal(cut({processSeals:twice,roseCutAt:at,laserDoneAt:0,metal:'rose'}),null,'a Rose Gold sheet with only a partial Cut Sheet press shows none');
 assert.equal(cut({processSeals:history,laserDoneAt:0},true).id,'cut-1','a row of the Completed list is completed by being there');
 assert.equal(context.processHtml({id:'set1',processSeals:history,laserDoneAt:at},'set:set1',true),'','a set row draws no seal');
 for(const owner of ['sheet:sheet1']){
  const card=d.createElement('section');card.className='libCard';
  const record={id:'sheet1',processReady:false,processSeals:twice,laserDoneAt:at+300000};
  const before=JSON.stringify(record);card.innerHTML='<div class="m"><span>1/1</span><span class="sheetBackStatus"></span></div>';d.body.appendChild(card);
  context.processSeals(card,record,owner);
  const seals=[...card.querySelectorAll('.seal')];
  assert.equal(seals.length,1,'exactly one seal on a completed sheet');assert.equal(seals[0].dataset.processSeal,'cut-2','the latest completion');
  assert.equal(number(seals[0],'width'),72,'Library sheet seal is 72 px');assert.equal(number(seals[0],'height'),72);
  assert.equal(w.getComputedStyle(seals[0]).opacity,'1','the seal is fully inked');assert(!/AWAITING LASER|Paul|Seth|Alex/.test(seals[0].querySelector('svg').textContent),'signer stays in assistive metadata');
  const row=card.querySelector('.sheetCutRow');assert.equal(row.getAttribute('data-seal-grow'),'1.8');assert.equal(row.getAttribute('data-seal-cap'),String(Seal.SHEET_CAP));assert.equal(row.parentElement.className,'m','it sits in the card footer, with the counts');
  assert(row.nextElementSibling.classList.contains('sheetBackStatus'),'before the counter, so the counter keeps the right edge');
  context.processSeals(card,record,owner);assert.equal(card.querySelectorAll('.seal').length,1,'refreshing never duplicates the seal');assert.equal(card.querySelector('.seal'),seals[0],'and keeps the very same element');
  record.laserDoneAt=0;context.processSeals(card,record,owner);assert.equal(card.querySelectorAll('.seal').length,0,'Reopen takes the seal off the card');
  assert.equal(record.processSeals.length,twice.length,'every stamp stays in the data');assert.equal(JSON.stringify({...record,laserDoneAt:at+300000}),before,'nothing else of the record was touched');
  record.laserDoneAt=at+300000;record.processSeals=[...twice,{id:'cut-3',how:'laserDone',at:at+500000,by:'Seth'}];context.processSeals(card,record,owner);
  assert.equal(card.querySelectorAll('.seal').length,1,'completed a third time: still one');assert.equal(card.querySelector('.seal').dataset.processSeal,'cut-3','the newest replaces the older one on the card');
  const old=d.createElement('span');old.className='sealRow processSealRow';old.innerHTML=Seal.html(history[0]);card.appendChild(old);context.processSeals(card,record,owner);assert.equal(card.querySelectorAll('.processSealRow').length,0,'a stamp row of the earlier drawing is taken away');
  card.remove();
 }
 const plain=d.createElement('div');plain.className='sealRow';plain.innerHTML=Seal.html(history[0]);d.body.appendChild(plain);
 assert.equal(number(plain.firstChild,'width'),50,'non-Library seals keep their canonical viewport');
 const rules=[...production.sheet.cssRules];
 assert.equal(rules.find(r=>r.selectorText==='.librarySheet:has(.processSealRow)>.productionRow'),undefined,'no QR room is kept for a stamp hanging under the card: the seal sits in the footer');
 console.log('PASS: no pending/readiness preview ink, compact counters unchanged, one 72 px LASER CUT seal per completed sheet (the latest; none when not completed, none for approval or a partial rose cut, none on a set), every stamp stays in the data, standard 50 elsewhere.');
}finally{w.close();}
