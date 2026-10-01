// Shared family outline and rendered contact geometry: a shaped stamp must land on the actual ink face,
// including when a seal group has reduced its width or the face rests at an angle.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const source=fs.readFileSync(path.join(__dirname,'../../charm-nest-motion.js'),'utf8');
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const FAMILIES=['received','prepared','engraving','laser','finishing','fulfilment','exceptions','cancelled'];
const MODEL={action:'BACK ENGRAVING',date:'30 SEP 2026',time:'9:44 PM',by:'Paul Konieczny'};
const dom=new JSDOM('<section class="setCard"><span class="sealRow"></span></section><section class="stampHost"><button>Approved</button></section>',{runScripts:'outside-only',pretendToBeVisual:true});
const w=dom.window,d=w.document,moves=[],pending=[];
let holdRipple=false;
w.matchMedia=()=>({matches:false});w.Element.prototype.getAnimations=()=>[];
w.Element.prototype.animate=function(frames,opts){
  let finish;const held=this.classList.contains('sealTool') || holdRipple && this.classList.contains('sealRing');
  const a={el:this,frames,opts,playState:held?'running':'finished',finished:held?new Promise(r=>{finish=()=>{a.playState='finished';r();};pending.push(()=>finish());}):Promise.resolve(),finish(){finish?.();},cancel(){finish?.();}};
  moves.push(a);return a;
};
Object.defineProperty(w.HTMLElement.prototype,'offsetWidth',{get(){return parseFloat(this.style.width) || parseFloat(this.style.getPropertyValue('--sz')) || 84;}});
Object.defineProperty(w.HTMLElement.prototype,'offsetHeight',{get(){return parseFloat(this.style.height) || parseFloat(this.style.getPropertyValue('--sz')) || 84;}});
w.eval(source);
const parse=html=>{const wrap=d.createElement('span');wrap.innerHTML=html;return wrap.querySelector('svg');};
const approx=(actual,expected,msg)=>assert(Math.abs(actual-expected)<.05,`${msg}: ${actual} vs ${expected}`);
const actualRect=(el,r)=>{el.getBoundingClientRect=()=>r;el.getClientRects=()=>[r];};

async function movingSource(){
  holdRipple=true;
  const row=d.querySelector('.sealRow'),first=d.createElement('span'),second=d.createElement('span');
  first.className='seal seal-engraveApproved pending';second.className='seal seal-laserDone pending';
  first.innerHTML=w.Seal.face({...MODEL,family:'engraving'});second.innerHTML=w.Seal.face({...MODEL,family:'laser'});row.append(first,second);
  const svg=first.querySelector('svg'),rotation=-12;let size=84,cx=460,cy=470,reads=0;
  const resized=next=>{size=next;first.style.cssText=`--rot:${rotation}deg;--sz:${size}px;width:${size}px;height:${size}px`;svg.style.cssText=`width:${size}px;height:${size}px`;};resized(size);
  const measured=()=>{reads++;const extent=size*(Math.abs(Math.cos(rotation*Math.PI/180))+Math.abs(Math.sin(rotation*Math.PI/180)));return{left:cx-extent/2,right:cx+extent/2,top:cy-extent/2,bottom:cy+extent/2,width:extent,height:extent};};
  first.getBoundingClientRect=svg.getBoundingClientRect=measured;first.getClientRects=()=>[measured()];
  second.style.cssText='--rot:11deg;--sz:84px;width:84px;height:84px';second.querySelector('svg').style.cssText='width:84px;height:84px';const sr={left:590,right:674,top:470,bottom:554,width:84,height:84};actualRect(second,sr);actualRect(second.querySelector('svg'),sr);
  const from=moves.length,active=el=>moves.slice(from).find(a=>a.el===el && a.playState==='running');
  const follows=(node,label)=>{assert(node,`${label} exists`);approx(parseFloat(node.style.width),size,`${label} follows responsive width`);approx(parseFloat(node.style.height),size,`${label} follows responsive height`);approx(parseFloat(node.style.left)+size/2,cx,`${label} follows horizontal movement`);approx(parseFloat(node.style.top)+size/2,cy,`${label} follows vertical movement`);};
  let finishedFirst=false,finishedSecond=false;
  const a=w.Seal.press(first).then(()=>{finishedFirst=true;}),b=w.Seal.press(second).then(()=>{finishedSecond=true;});await wait(1);
  const tool=d.querySelector('.sealTool'),descent=active(tool);follows(tool,'initial wooden head');
  cx+=27;cy-=46;resized(66);await wait(35);follows(tool,'moving wooden head during descent');assert(first.classList.contains('pending'),'scrolling cannot reveal ink early');assert(second.classList.contains('pending'));assert.equal(active(tool),descent,'tracking movement does not restart or shorten the descent');assert(!finishedFirst && !finishedSecond);
  descent.finish();await wait(1);const ring=d.querySelector('.sealRing'),lift=active(tool);assert(!first.classList.contains('pending'));assert(second.classList.contains('pending'));await wait(25);follows(tool,'wooden head at contact');follows(ring,'ink ripple at contact');
  cx-=63;cy+=81;resized(48);await wait(35);follows(tool,'moving wooden head during lift');follows(ring,'moving ink ripple');assert.equal(active(tool),lift,'tracking movement does not restart or shorten the lift');assert(!finishedFirst && !finishedSecond);
  lift.finish();await wait(280);assert(!finishedFirst,'source movement cannot bypass the final ripple');assert(w.Seal.busy());active(ring).finish();await a;await wait(25);assert(finishedFirst && !finishedSecond);assert(second.classList.contains('pending'));assert(!tool.isConnected && !ring.isConnected,'tracking nodes are removed after the complete press');
  const settledReads=reads,capturedLeft=tool.style.left;cx+=150;cy-=120;resized(84);await wait(35);assert.equal(reads,settledReads,'tracking stops reading the original source after its tool is removed');assert.equal(tool.style.left,capturedLeft,'removed tool is no longer repositioned');
  const next=d.querySelector('.sealTool');assert(next && next!==tool,'the queued next stamp starts after the moving head has fully finished');active(next).finish();await wait(1);const nextRing=d.querySelector('.sealRing');active(next).finish();active(nextRing).finish();await b;
  assert(!w.Seal.busy());assert.equal(d.querySelectorAll('.sealTool,.sealRing,.sealShade').length,0,'both stamps clean up all tracking and animation elements');first.remove();second.remove();holdRipple=false;
}

(async()=>{
  try{
    assert.equal(typeof w.Seal.face,'function','one shared face API represents all eight families');assert.equal(typeof w.Seal.tool,'function','matching stamp API is exposed');
    const outlines=[];
    for(const family of FAMILIES){
      const svg=parse(w.Seal.face({...MODEL,family})),outline=svg.querySelector('[data-seal-outline]');
      assert(outline,`${family}: face identifies its outer edge`);outlines.push(outline.getAttribute('d'));
      for(const input of [svg,svg.outerHTML]){
        const tool=parse(w.Seal.tool('print',input));
        assert.equal(tool.dataset.stampFamily,family,`${family}: the stamp belongs to the source face's family`);
        const contact=tool.querySelector('[data-stamp-contact]');assert(contact,`${family}: a shaped rubber contact exists`);
        assert.equal(contact.getAttribute('d'),outline.getAttribute('d'),`${family}: wooden/rubber silhouette exactly matches the face`);
        assert.equal(tool.getAttribute('viewBox'),svg.getAttribute('viewBox'),`${family}: contact uses the same coordinates`);
        assert(tool.querySelector('image'),`${family}: the wooden tool carries its real grain texture`);
        const clip=tool.querySelector('clipPath path');assert(clip,`${family}: grain is clipped to the shaped base`);assert.equal(clip.getAttribute('d'),outline.getAttribute('d'),`${family}: texture cannot extend outside the family shape`);
      }
    }
    assert.equal(new Set(outlines).size,8,'all eight families keep distinct silhouettes');
    const row=d.querySelector('.sealRow');
    const contacts=[84,66,48].flatMap(size=>[-12,11].map(rotation=>({size,rotation,family:'engraving'}))).concat([{size:84,rotation:-8,family:'cancelled',generic:true},{size:66,rotation:11,family:'finishing',generic:true}]);
    for(const {size,rotation,family,generic} of contacts){
      const seal=d.createElement('span');seal.className=generic?'sv tlBig pending':'seal seal-engraveApproved pending';seal.style.cssText=`--rot:${rotation}deg;--sz:${size}px;width:${size}px;height:${size}px`;
      seal.innerHTML=w.Seal.face({...MODEL,family});row.appendChild(seal);
      const svg=seal.querySelector('svg');svg.style.cssText=`width:${size}px;height:${size}px`;
      const extent=size*(Math.abs(Math.cos(rotation*Math.PI/180))+Math.abs(Math.sin(rotation*Math.PI/180))),cx=460,cy=470;
      const rect={left:cx-extent/2,right:cx+extent/2,top:cy-extent/2,bottom:cy+extent/2,width:extent,height:extent};actualRect(seal,rect);actualRect(svg,rect);
      const moveAt=moves.length,pressing=w.Seal.press(seal);await wait(1);const tool=d.querySelector('.sealTool');assert(tool,'a wooden tool descends');
      approx(parseFloat(tool.style.width),size,'contact width follows unrotated responsive face');approx(parseFloat(tool.style.height),size,'contact height follows unrotated responsive face');
      approx(parseFloat(tool.style.left)+size/2,cx,'contact centre matches rotated seal centre');approx(parseFloat(tool.style.top)+size/2,cy,'contact centre matches rotated seal centre');
      assert.equal(tool.querySelector('[data-stamp-contact]').getAttribute('d'),svg.querySelector('[data-seal-outline]').getAttribute('d'));
      const descent=moves.filter(a=>a.el===tool).at(-1);assert.match(descent.frames.at(-1).transform,new RegExp(`rotate\\(${rotation}deg\\)`),'contact has the original seal rotation');assert.match(descent.frames.at(-1).transform,/scale\(1\)/,'contact does not enlarge the rendered face');
      pending.shift()();await wait(1);assert(!seal.classList.contains('pending'));const ripple=moves.slice(moveAt).find(a=>a.el.classList.contains('sealRing'))?.el;assert(ripple,`${family}: ink ripples at contact`);assert.equal(ripple.style.getPropertyValue('--ink'),svg.querySelector('g').getAttribute('fill'),`${family}: contact ripple matches the face colour`);pending.shift()();await pressing;seal.remove();
    }
    const host=d.querySelector('.stampHost'),btn=host.querySelector('button');
    actualRect(host,{left:40,top:100,right:340,bottom:300,width:300,height:200});actualRect(btn,{left:100,top:150,right:240,bottom:186,width:140,height:36});
    const stamping=w.Seal.stampOn(host,{btn,stamp:{how:'engraveApproved',at:Date.UTC(2026,8,30,18,35),by:'Paul Konieczny'}});await wait(1);
    const seal=host.querySelector('.seal'),tool=host.querySelector('.sealTool');assert(seal && tool,'button stamp has a matching tool and face');
    approx(parseFloat(tool.style.width),84,'button uses canonical seal width');approx(parseFloat(tool.style.height),84,'button uses canonical seal height');approx(parseFloat(tool.style.left),parseFloat(seal.style.left),'button stamp and face share the same horizontal origin');approx(parseFloat(tool.style.top),parseFloat(seal.style.top),'button stamp and face share the same vertical origin');
    assert.equal(tool.querySelector('[data-stamp-contact]').getAttribute('d'),seal.querySelector('[data-seal-outline]').getAttribute('d'),'button uses the actual face outline');
    pending.shift()();await wait(1);pending.shift()();await stamping;assert.equal(d.querySelector('.sealTool'),null);
    await movingSource();
    console.log('PASS: eight matching silhouettes; clipped grain; exact responsive/rotated contact; tool and ripple follow movement/resize throughout the full queued stamp and stop tracking on cleanup');
  }finally{w.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
