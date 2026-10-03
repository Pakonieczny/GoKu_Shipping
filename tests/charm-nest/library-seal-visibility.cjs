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
const Seal=w.Seal,context=vm.createContext({window:w,Seal,CharmNestReadiness:R,doc:d,L:{pendingSeals:new Set(),presses:new Set()}});
const start=library.indexOf('  function processHtml('),end=library.indexOf('  function cards(',start);
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
 assert.equal(Seal.BASE_SIZE,50);assert.equal(Seal.HOVER_SIZE,undefined,'no fixed enlarged size: every seal, in the Library too, zooms in place by the one adaptive curve');assert(Seal.zoomScale(44)>=Seal.zoomScale(50),'a 44px Library card seal grows by the shared curve, at least as much as a regular one');
 for(const owner of ['sheet:sheet1','set:set1']){
  const card=d.createElement('section');card.className=owner.startsWith('set:')?'setCard':'libCard';
  const record={id:'sheet1',processReady:false,processSeals:history};
  const before=JSON.stringify(record);card.innerHTML=context.processHtml(record,owner);d.body.appendChild(card);
  const seals=[...card.querySelectorAll('.seal')];assert.equal(seals.length,3,'every actual historical process seal stays visible');
  assert.deepEqual(seals.map(n=>n.dataset.processSeal),history.map(x=>x.id));
  for(const seal of seals){
   assert.equal(number(seal,'width'),44,'Library card seal is compact');assert.equal(number(seal,'height'),44);
   assert.equal(w.getComputedStyle(seal).opacity,'1','actual seals remain fully visible');
   assert.doesNotMatch(seal.querySelector('svg').textContent,/AWAITING LASER|Paul|Seth|Alex/,'regular face stays legible and signer stays in assistive metadata');
  }
  context.processSeals(card,record,owner);assert.equal(card.querySelectorAll('.seal').length,3,'refreshing history does not duplicate seals');
  record.processReady=true;context.processSeals(card,record,owner);record.processReady=false;context.processSeals(card,record,owner);
  assert.deepEqual([...card.querySelectorAll('.seal')],seals,'moving stages or reopening never removes historical ink');
  record.processReady=false;assert.equal(JSON.stringify(record),before,'only the explicit test stage changes');
  const row=card.querySelector('.processSealRow');row.style.setProperty('--seal-fit','30px');
  assert(seals.every(n=>number(n,'width')===30 && number(n,'height')===30),'crowded groups can shrink below the compact cap uniformly');
  row.style.setProperty('--seal-fit','50px');assert(seals.every(n=>number(n,'width')===44),'more room restores the same compact card cap');
  card.remove();
 }
 const plain=d.createElement('div');plain.className='sealRow';plain.innerHTML=Seal.html(history[0]);d.body.appendChild(plain);
 assert.equal(number(plain.firstChild,'width'),50,'non-Library seals keep their canonical viewport');
 const rules=[...production.sheet.cssRules];
 assert.equal(rules.find(r=>r.selectorText==='.librarySheet:has(.processSealRow)>.productionRow').style.getPropertyValue('margin-top'),'24px','QR room follows the smaller card seal (half of 44 and 2)');
 console.log('PASS: no pending/readiness preview ink, compact counters unchanged, all historical seals survive stage changes without duplicates, Library cards cap at44 and shrink uniformly, standard50 elsewhere (the zoom is adaptive).');
}finally{w.close();}
