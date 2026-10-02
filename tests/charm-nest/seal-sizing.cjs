// Drive the real seal renderer and fitter through their actual CSS variables. JSDOM does not lay out
// pixels, so this fixture supplies container widths and resolves the stylesheet's length expressions;
// the production fitter, markup, family artwork, inspector and hover handlers are not replaced.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {JSDOM}=require('jsdom');
const ROOT=path.join(__dirname,'../..'),read=file=>fs.readFileSync(path.join(ROOT,file),'utf8');
const catalog=new JSDOM('<style id="main"></style><style id="activity"></style>');
catalog.window.document.querySelector('#main').textContent=read('charm-nest-1.html').match(/<style>([\s\S]*?)<\/style>/)[1];
catalog.window.document.querySelector('#activity').textContent=read('charm-nest-activity.css');
const main=catalog.window.document.querySelector('#main').sheet,activity=catalog.window.document.querySelector('#activity').sheet;
assert(main && activity,'both production stylesheets parse');
const relevant=sheet=>[...sheet.cssRules].filter(r=>r.selectorText && (r.selectorText===':root' || /\.seal|\.laserSeal|\.processSealRow|\.egApprove|\.egButtonSeal|\.engravingSeals|\.pvMain|\.pvApproval/.test(r.selectorText))).map(r=>r.cssText).join('\n');
const dom=new JSDOM(`<style>*{box-sizing:border-box}${relevant(main)}${relevant(activity)}</style><p id="outside">Work area</p>`,{url:'http://127.0.0.1',runScripts:'outside-only',pretendToBeVisual:true});
catalog.window.close();
const w=dom.window,d=w.document,frames=[],observers=[];
w.matchMedia=()=>({matches:false});w.requestAnimationFrame=fn=>(frames.push(fn),frames.length);w.cancelAnimationFrame=()=>{};
w.setInterval=()=>1;w.clearInterval=()=>{};w.Element.prototype.getAnimations=()=>[];
w.Element.prototype.animate=()=>({finished:Promise.resolve(),playState:'finished',cancel(){}});
w.ResizeObserver=class{constructor(callback){this.callback=callback;this.targets=new Set();observers.push(this);}observe(node){this.targets.add(node);}unobserve(node){this.targets.delete(node);}disconnect(){this.targets.clear();}};
w.localStorage.setItem('cn.employee','Current Viewer');

function custom(node,name){
  for(let n=node;n;n=n.parentElement){const value=w.getComputedStyle(n).getPropertyValue(name).trim();if(value)return value;}
  return '';
}
function expand(value,node){
  let start;
  while((start=value.indexOf('var('))>=0){
    let end=start+4,level=1,comma=-1;
    for(;end<value.length && level;end++){const c=value[end];if(c==='(')level++;if(c===')')level--;if(c===',' && level===1 && comma<0)comma=end;}
    assert.equal(level,0,'valid CSS custom-property expression');
    const name=value.slice(start+4,comma<0?end-1:comma).trim(),fallback=comma<0?'':value.slice(comma+1,end-1);
    const replacement=custom(node,name)||fallback;assert(replacement,'CSS variable '+name+' has a value');
    value=value.slice(0,start)+expand(replacement,node)+value.slice(end);
  }
  return value;
}
function length(node,property,percentBase=0){
  let value=w.getComputedStyle(node).getPropertyValue(property).trim();if(!value || /^(auto|none|normal)$/.test(value))return null;
  value=expand(value,node).replace(/(-?\d*\.?\d+)%/g,(_,n)=>String(+n*percentBase/100)).replace(/px\b/g,'');
  value=value.replace(/calc\(/g,'(').replace(/\bmin\(/g,'Math.min(').replace(/\bmax\(/g,'Math.max(');
  assert(/^[\d\s.+\-*/,()Mathminax]+$/.test(value),'safe numeric CSS length: '+value);
  const number=Function('return ('+value+')')();assert(Number.isFinite(number),'finite CSS length: '+value);return number;
}
const pad=node=>(length(node,'padding-left')||0)+(length(node,'padding-right')||0);
const gap=node=>length(node,'column-gap') ?? length(node,'gap') ?? 0;
function width(node){
  if(node._width!=null)return node._width;
  if(node.classList?.contains('sealRow')){
    const parent=node.parentElement,available=parent?.clientWidth || 1200;
    const natural=[...node.children].reduce((sum,n)=>sum+width(n),0)+gap(node)*Math.max(0,node.children.length-1)+pad(node);
    return Math.min(natural,length(node,'max-width',available) ?? available,length(node,'width',available) ?? Infinity);
  }
  return length(node,'width') ?? (node.classList?.contains('lc')?280:0);
}
Object.defineProperty(w.HTMLElement.prototype,'clientWidth',{get(){return width(this);}});
Object.defineProperty(w.HTMLElement.prototype,'offsetWidth',{get(){return width(this);}});
Object.defineProperty(w.HTMLElement.prototype,'offsetHeight',{get(){return length(this,'height') ?? (this.classList.contains('lc')?28:84);}});
w.HTMLElement.prototype.getBoundingClientRect=function(){const size=width(this),h=this.offsetHeight;return {left:410,top:560,right:410+size,bottom:560+h,width:size,height:h};};
w.HTMLElement.prototype.getClientRects=function(){return [this.getBoundingClientRect()];};
w.eval(read('charm-nest-motion.js'));w.eval(read('charm-nest-engraving-seals.js'));w.eval(read('charm-nest-readiness.js'));
const Seal=w.Seal,at=Date.UTC(2026,9,1,1,44),by='Recorded Operator',outside=d.querySelector('#outside');
const nearly=(actual,expected,message)=>assert(Math.abs(actual-expected)<.02,`${message}: ${actual} vs ${expected}`);
const rule=(sheet,selector)=>[...sheet.cssRules].find(r=>r.selectorText===selector);
const tick=async()=>{for(let i=0;i<6;i++)await Promise.resolve();};
async function resizeParent(parent,width){
  parent._width=width;
  const watchers=observers.filter(o=>o.targets.has(parent));assert(watchers.length,'contained group resize is observed even when the browser viewport stays the same');
  for(const observer of watchers)observer.callback([{target:parent,contentRect:parent.getBoundingClientRect()}]);
  await tick();for(let n=0;frames.length&&n<8;n++){const batch=frames.splice(0);for(const frame of batch)frame(0);await tick();}
}

function fixture(count,parentWidth=1200,classes='',size=0){
  const parent=d.createElement('section');parent._width=parentWidth;parent.style.cssText='display:block;padding:0;position:relative';
  const record={stamps:Array.from({length:count},(_,i)=>({how:i%3===0?'button':'print',at:at+i*60000,by}))};
  parent.innerHTML=Seal.row(record,size?{size}:{});const row=parent.firstElementChild;row.classList.add(...classes.split(' ').filter(Boolean));row.style.margin='0';
  d.body.appendChild(parent);return {parent,row,seals:[...row.querySelectorAll('.seal')]};
}
function fitted(f,label){
  Seal.fitGroups(f.parent);
  const widths=f.seals.map(s=>length(s,'width')),heights=f.seals.map(s=>length(s,'height'));
  assert(widths.every(n=>n===widths[0]),label+' fits every sibling uniformly');assert.deepEqual(heights,widths,label+' preserves square seal viewports');
  const used=widths.reduce((sum,n)=>sum+n,0)+gap(f.row)*Math.max(0,f.seals.length-1);
  const capacity=f.row.clientWidth-pad(f.row);assert(used<=capacity+.02,label+' fits inside its actual CSS width cap');
  assert(widths.every(n=>n>0&&n<=84),label+' only shrinks below the canonical size');return widths[0];
}

(async()=>{
  try{
    assert.equal(Seal.BASE_SIZE,84);assert.equal(Seal.HOVER_SIZE,undefined,'no fixed enlarged size remains: the zoom is adaptive');assert.equal(typeof Seal.zoomScale,'function');assert.equal(Seal.zoomScale(84),1.85);assert(Seal.zoomScale(24)>Seal.zoomScale(84)&&Seal.zoomScale(84)>Seal.zoomScale(140),'smaller seals zoom more');
    assert.equal(typeof Seal.fitGroups,'function');assert.equal(typeof Seal.fit,'function');
    assert.equal(custom(d.documentElement,'--seal-size'),'84px');assert.equal(custom(d.documentElement,'--seal-hover-size'),'','no fixed hover size is left in the stylesheet');

    // Old rendering calls and previously generated inline --sz values cannot create a different normal size.
    for(const how of ['print','button','engraveApproved','engravePlain','laserReady','laserDone'])for(const requested of [0,34,56,112,180]){
      const host=d.createElement('span');host.innerHTML=Seal.html({how,at,by},requested);d.body.appendChild(host);
      const seal=host.firstElementChild;assert.equal(seal.style.getPropertyValue('--sz'),'84px',how+' normalizes a legacy requested size');
      seal.style.setProperty('--sz',requested+'px');nearly(length(seal,'width'),84,how+' CSS ignores a legacy width');nearly(length(seal,'height'),84,how+' CSS ignores a legacy height');host.remove();
    }
    for(const ready of [true,false])assert.equal(w.CharmNestReadiness.seal({ready}),'','computed readiness never creates a preview stamp');

    // Twelve historical decisions remain available; shrinking never alters, hides or cuts off any seal.
    const history=fixture(12),faces=history.seals.map(s=>s.innerHTML);
    assert.equal(history.seals.length,12,'the entire history is rendered');assert.equal(history.row.querySelector('.sealMore'),null,'no historical seal is replaced by an earlier-count marker');
    assert.equal(fitted(history,'wide 12-seal history'),84);
    history.parent._width=440;const narrow=fitted(history,'narrow 12-seal history');assert(narrow<84);
    const stable=history.row.style.getPropertyValue('--seal-fit');fitted(history,'repeated narrow fit');assert.equal(history.row.style.getPropertyValue('--seal-fit'),stable,'repeated fitting does not oscillate');
    history.parent._width=1200;assert.equal(fitted(history,'restored wide history'),84,'seals grow back to the canonical size');
    assert.deepEqual(history.seals.map(s=>s.innerHTML),faces,'fitting never rewrites historical artwork');history.parent.remove();

    // A strip that asks for small seals (the order window's one line, Seal.row size) never grows them past it, and still shrinks below it when short of room.
    for(const count of [1,2,3]){
      const f=fixture(count,1200,'',34);assert.equal(fitted(f,count+' small-strip seals'),34,'a requested strip size is the most a seal grows to');
      if(count>1){f.parent._width=count*20+4*(count-1);assert(fitted(f,count+' constrained small-strip seals')<=20,'a short strip still shrinks them together');}
      f.parent.remove();
    }
    // (the order window's custom bar: a row beside words that take what is left, in a flex line, never collapses to a pixel)
    {
      const bar=d.createElement('div');bar.style.cssText='display:flex;gap:8px;padding:5px 8px 5px 10px';bar.style.width='772px';bar._width=772;
      bar.innerHTML='<span class="tag"></span><span class="w"></span>'+Seal.row({stamps:[{how:'print',at,by},{how:'button',at:at+6e4,by}]},{size:34})+'<button></button>';
      const tag=bar.children[0],words=bar.children[1],btn=bar.children[3];tag._width=259;btn._width=65;words._width=306;words.style.cssText='flex:1 1 auto;min-width:0';
      d.body.appendChild(bar);const row=bar.querySelector('.sealRow');row.style.margin='0';row.style.maxWidth='50%';row.style.flex='0 0 auto';
      Seal.fitGroups(bar);assert.equal(parseFloat(row.style.getPropertyValue('--seal-fit')),34,'the custom bar\'s seals are 34px, not squeezed to the width the line gave a previous fit');bar.remove();
    }
    for(const count of [1,2,3,4,6,8]){
      const f=fixture(count,1200);assert.equal(fitted(f,count+' spacious siblings'),84);
      if(count>1){f.parent._width=count*64+4*(count-1);assert(fitted(f,count+' constrained siblings')<=64,'adding siblings reduces them together');f.parent._width=1200;assert.equal(fitted(f,count+' expanded siblings'),84);}
      f.parent.remove();
    }
    const changing=fixture(2,256);const two=fitted(changing,'two siblings');
    const extra=d.createElement('span');extra.innerHTML=Seal.html({how:'print',at:at+9e5,by},56);changing.row.appendChild(extra.firstChild);changing.seals=[...changing.row.querySelectorAll('.seal')];
    assert(fitted(changing,'third sibling added')<two,'a new seal triggers a smaller equal fit');changing.row.lastChild.remove();changing.seals=[...changing.row.querySelectorAll('.seal')];assert.equal(fitted(changing,'third sibling removed'),84);changing.parent.remove();
    const responsive=fixture(4,600);assert.equal(fitted(responsive,'observed group starts full-size'),84);
    await resizeParent(responsive.parent,224);assert(responsive.seals.every(s=>length(s,'width')<84),'a container resize refits without an explicit render or window resize');
    await resizeParent(responsive.parent,600);assert(responsive.seals.every(s=>length(s,'width')===84),'a wider container restores the same canonical size automatically');responsive.parent.remove();

    // These real stylesheet caps previously defeated parseFloat(maxWidth): min(), calc() and row padding matter.
    const engraving=fixture(12,600,'engravingSeals');assert(fitted(engraving,'engraving capped history')<20);assert(engraving.row.clientWidth<=240);engraving.parent.remove();
    const process=fixture(4,300,'processSealRow');process.parent.classList.add('libCard');
    // Apply the matching production cap directly because JSDOM does not rank selector specificity.
    process.row.style.maxWidth=rule(main,'.libCard>.processSealRow').style.getPropertyValue('max-width');
    assert(fitted(process,'compact Library process history')<=65);assert(process.row.clientWidth<=260,'process history uses the card width without reserving an unearned badge');process.parent.remove();
    const single=fixture(1,300,'processSealRow');single.parent.classList.add('libCard');
    assert.equal(fitted(single,'single Library process seal'),72,'Library cards use the requested compact seal cap');single.parent.remove();

    // The existing Approved control keeps its footprint and label. Its seal and the workspace approval overlay stay out of flow.
    const panel=d.createElement('section');panel.innerHTML=w.CNEngravingSeals.panel({kind:'approved',at,by,job:{key:'test',state:'approved',approvedAt:at,approvedBy:by}});d.body.appendChild(panel);
    const button=panel.querySelector('.egApproveButton');assert.equal(button.textContent,'Approved');
    nearly(length(button,'min-width'),144,'Approved minimum width');nearly(length(button,'height'),36,'Approved height');nearly(length(button,'font-size'),18,'Approved text size');assert.equal(w.getComputedStyle(button).justifyContent,'center');
    const buttonSeal=panel.querySelector('.egButtonSeal');assert.equal(w.getComputedStyle(buttonSeal).position,'absolute');nearly(length(buttonSeal.firstElementChild,'width'),84,'button uses the same seal size');assert.equal(w.getComputedStyle(buttonSeal.firstElementChild).backgroundColor,'rgba(0, 0, 0, 0)');panel.remove();
    // The reported pair must share the actual button row, not consume a separate history shelf.
    const pair=d.createElement('section');pair.innerHTML=w.CNEngravingSeals.panel({kind:'approved',at:at+180000,by:'Seth',job:{key:'pair',state:'approved',approvedAt:at+180000,approvedBy:'Seth',seals:[{how:'engraveApproved',at,by},{how:'engraveApproved',at:at+180000,by:'Seth'}]}});d.body.appendChild(pair);
    const wrap=pair.querySelector('.egApproveWrap'),pairRow=pair.querySelector('.egButtonSeal');
    assert.equal(pair.querySelectorAll('.sealRow').length,1,'all panel approvals share one row');assert.equal(pair.querySelector('.egHistory'),null,'no extra vertical history shelf');assert.equal(pairRow.querySelectorAll('.seal').length,2,'both original approvals remain');
    const originalPair=pairRow.innerHTML;
    for(const available of [320,260,220,300]){
      wrap._width=available;Seal.fitGroups(pair);const size=length(pairRow.firstElementChild,'width');
      assert.equal(length(pairRow.lastElementChild,'width'),size,'siblings shrink equally');
      const used=2*size+gap(pairRow)+pad(pairRow),room=available-144-8+size/2;
      assert(used<=room+.03,'the whole row fits beside its half-button overlap');assert(size>0&&size<=84);
      if(available===220)assert(size<40,'narrow inspectors dynamically reduce both stamps');
      assert.equal(pairRow.innerHTML,originalPair,'resizing preserves original names/times/artwork');
    }
    const pressed=Seal.press;let lift;const lifting=new Promise(r=>lift=r);let newPending;
    Seal.press=async seal=>{newPending=seal;assert.equal(pairRow.querySelectorAll('.seal').length,3,'a new press preserves both existing approvals');await lifting;seal.classList.remove('pending');};
    const latest={how:'engraveApproved',at:at+360000,by:'Another Operator'},adding=w.CNEngravingSeals.press(pair.querySelector('button'),latest);await tick();
    assert(newPending && newPending.classList.contains('pending'));assert.equal(pairRow.children[0].getAttribute('data-at'),String(at));assert.equal(pairRow.children[1].getAttribute('data-at'),String(at+180000));
    lift();await adding;await w.CNEngravingSeals.press(pair.querySelector('button'),latest);assert.equal(pairRow.querySelectorAll('.seal').length,3,'repeating the identical receipt never adds another stamp');Seal.press=pressed;pair.remove();
    const overlay=rule(activity,'.pvApproval'),space=rule(activity,'.pvMain:has(.pvApproval:not(:empty))');assert.equal(overlay.style.getPropertyValue('position'),'absolute');
    for(const property of ['height','min-height','padding-top','padding-bottom'])assert.equal(space.style.getPropertyValue(property),'','approval overlay adds no vertical '+property);
    assert(!rule(main,'.librarySheet:has(.processSealRow)'),'no redundant trailing seal padding remains under the QR area');

    // Every chosen family shares one regular viewport and grows in place by the one adaptive curve: no second surface, the signer stays historical (assistive text only).
    const families=Object.keys(Seal.FAMILY);assert.equal(families.length,8);
    for(const family of families){
      const host=d.createElement('span');host.className='sealRow';host.innerHTML=`<span class="seal" tabindex="0" role="img" aria-label="Stamped by ${by} · Sep 30" data-at="${at}">${Seal.face({family,action:Seal.FAMILY[family].name,at,by})}</span>`;d.body.appendChild(host);
      const seal=host.firstElementChild;nearly(length(seal,'width'),84,family+' regular size');assert.doesNotMatch(seal.querySelector('svg').textContent,/Recorded Operator|Signed by/);
      const box={left:400,top:560,width:84,height:84,right:484,bottom:644};seal.getBoundingClientRect=()=>box;seal.getClientRects=()=>[box];
      seal.dispatchEvent(new w.MouseEvent('click',{bubbles:true,clientX:452,clientY:602}));await tick();
      assert.equal(d.querySelector('[data-seal-zoom]'),seal,family+' grows where it stands');assert.equal(seal.dataset.sealZoom,Seal.zoomScale(84).toFixed(2),family+' takes the 84px size\'s scale');
      assert.equal(d.querySelectorAll('.sealLens,.tlLoupe').length,0,family+' makes no second surface');assert.equal(seal.querySelectorAll('svg').length,1,family+' keeps its one face');assert.equal(seal.querySelector('svg').dataset.sealFamily,family);
      assert.match(seal.getAttribute('aria-label'),/Recorded Operator/);assert.doesNotMatch(seal.outerHTML,/Current Viewer/);assert(!seal.hasAttribute('title'),family+' has no tooltip');
      seal.dispatchEvent(new w.MouseEvent('pointerout',{bubbles:true,relatedTarget:outside,clientX:-9,clientY:-9}));await tick();assert.equal(d.querySelector('[data-seal-zoom]'),null,family+' goes back with the pointer');host.remove();
    }
    console.log('PASS: canonical84 normal / adaptive in-place zoom across all8 families; legacy sizes ignored; all12 historical seals retained; equal groups shrink, recover and respect calc/min caps; compact Approved control and workspace overlay preserved; no redundant Library padding.');
  }finally{w.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
