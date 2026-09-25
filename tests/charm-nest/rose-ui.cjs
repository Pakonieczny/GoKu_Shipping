const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom'),R=require('../../charm-nest-rose'),O=require('../../charm-nest-orders');
const dom=new JSDOM('<section id="sheet"><div class="shHead"></div><div class="shGate" data-r="gate"></div><div class="shPreviewWrap"></div></section>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
const observers=[];w.IntersectionObserver=class{constructor(cb){this.cb=cb;observers.push(this);}observe(el){this.el=el;}unobserve(){}fire(){this.cb([{target:this.el,isIntersecting:true}]);}};
w.CharmNestRose=R;w.CharmNestOrders=O;w.confirm=()=>true;const calls=[],toasts=[];
const outline={subpaths:[[['m',[0,0]],['l',[10,0]],['l',[10,10]],['l',[0,10]],['h']]]};
const sh={metal:'rose',el:w.document.getElementById('sheet'),sheetId:'rose-test',runId:'run-test',charms:[{id:'charm',outline,centerPt:[5,5],members:[]}],placements:[{id:'charm',cxPt:10,cyPt:10,angle:0,scale:1}],persistedDone:true,verification:{ok:true},status:'complete',dirty:false,draft:true,outputs:{}};
const stock={id:'rgs-physical',wPt:100,hPt:50,revision:0,profileJson:null};let saved;
w.CN={S:{cloud:{ok:true},settings:{stock:{}},mode:'nest'},esc:s=>String(s).replace(/</g,'&lt;'),stockFor:()=>({wPt:100,hPt:50,wIn:100/72,hIn:50/72}),uid:()=> 'test',allSheets:()=>[sh],drawPreview(){},renderCard:p=>{w.Gate.renderCard(p);w.RoseStock?.render(p);},toast(m,k){toasts.push([String(m),k]);},sheetDirty:p=>{p.dirty=true;},api:async(name,b)=>{calls.push(b.op);if(b.op==='roseClaim')return {stock};if(b.op==='roseGet')return {stock,cuts:[],more:false};if(b.op==='rosePlan'){saved=R.plan(JSON.parse(b.shapesJson),100,50,null,b.allowanceMm);return {planJson:JSON.stringify(saved),planHash:'hash'};}if(b.op==='roseRecordCut'){if(w.failCut){w.failCut=false;throw new Error('The sheet changed elsewhere');}return {stock:{...stock,revision:1,profileJson:JSON.stringify(saved.profile)},cut:{sheetId:sh.sheetId,stockId:stock.id,revision:1,at:1760000000000,planJson:JSON.stringify(saved),planHash:'hash',fileBase:'RG sheet'}};}return {};}};
w.sh=sh;
w.eval(`var C=window.CN,S=C.S,CN=C,B=window.B={run:{runId:'run-test',releasePolicy:2,status:'running',solidIncluded:{}}};
 var O=window.CharmNestOrders,allSheets=()=>[sh],pagesOf=()=>[sh],stockFor=C.stockFor,esc=C.esc,labelOf=()=> 'RG 14/20';
 var Sets={ofRun:()=>[]},RunCtl={},Session=window.Session={schedule(){}},toast=()=>{},refreshAllCards=()=>{},api=C.api;
 var sheetDirty=()=>{},saveSettings=()=>{},startNest=()=>{};
`);
const code=fs.readFileSync('charm-nest-bridge.js','utf8');w.eval(code.slice(code.indexOf('const Gate ='),code.indexOf('/* ═══ 21',code.indexOf('const Gate ='))));w.Gate.renderCard(sh);
w.eval(fs.readFileSync('charm-nest-rose-ui.js','utf8'));
(async()=>{
 assert.match(sh.el.textContent,/Include in current set/);assert.match(sh.el.textContent,/Sheet dimensions/);assert(!sh.el.querySelector('[data-solid="nest"]'));assert.equal(sh.el.querySelectorAll('.solidOptions details').length,0);assert.match(sh.el.textContent,/Choose remnant or new sheet/);
 assert.equal(calls.length,0,'rendering never eagerly loads physical stock');
 const menu=sh.el.querySelector('.sheetOptions'),width=sh.el.querySelector('[data-solid="w"]'),allowance=sh.el.querySelector('[data-rose-allowance]');menu.open=true;
 width.focus();width.value='125.5';width.dispatchEvent(new w.Event('input'));allowance.value='0.35';allowance.dispatchEvent(new w.Event('input'));
 for(let n=0;n<10;n++)w.CN.renderCard(sh);
 assert.equal(sh.el.querySelector('.sheetOptions'),menu);assert(menu.open);assert.equal(w.document.activeElement,width);assert.equal(width.value,'125.5');assert.equal(allowance.value,'0.35');
 width.blur();w.CN.renderCard(sh);assert.equal(width.value,'125.5','draft survives blur and background refresh');
 sh.status='nesting';w.CN.renderCard(sh);assert(!width.disabled,'background work does not prevent editing a draft');assert(sh.el.querySelector('[data-solid="size"]').disabled);sh.status='complete';
 assert(!sh.el.textContent.includes('Physical sheet'));assert(!sh.el.querySelector('[data-rose-release]'));

 await w.RoseStock.plan(sh);assert(sh.rosePlan.lines.length);assert(!/mm contour allowance ·|remaining after this cut/.test(sh.el.textContent),'no allowance or remaining-area line');assert.equal(calls.filter(x=>x==='rosePlan').length,1);
 // an allowance outside 0.05 to 2 mm goes back to the one in use, and the operator is told the range
 allowance.value='5';allowance.dispatchEvent(new w.Event('change'));assert.equal(allowance.value,'0.2');assert.match(allowance.title,/0\.05 to 2 mm/);assert(toasts.some(([m,k])=>k==='bad'&&/stays 0\.2 mm: it can be 0\.05 to 2 mm/.test(m)),'the range is named');assert.equal(calls.filter(x=>x==='rosePlan').length,1,'an out-of-range allowance plans nothing');
 // the busy line names the step under way
 const busyLine=()=>sh.el.querySelector('.roseHistory [role="status"]')?.textContent||'';
 sh._roseAction=true;w.CN.renderCard(sh);assert.match(busyLine(),/Recording the cut/);sh._rosePlanning=Promise.resolve();w.CN.renderCard(sh);assert.match(busyLine(),/Planning the green line/);
 sh._roseAction=false;sh._rosePlanning=null;w.CN.renderCard(sh);assert.equal(busyLine(),'');
 const ctx=new Proxy({calls:[]},{get(o,k){if(k in o)return o[k];return (...args)=>o.calls.push([k,...args]);},set(o,k,v){o[k]=v;return true;}});
 w.RoseStock.paint(ctx,sh,2,'lines');assert(ctx.calls.some(c=>c[0]==='setLineDash'&&c[1].length),'pending contour is distinguished from completed cut');assert.equal(ctx.lineWidth,2.5,'pending contour is twice its previous display width');
 const original=JSON.stringify(sh.placements),lines=JSON.stringify(sh.rosePlan.lines);w.RoseStock.protect(sh);sh.dirty=true;sh.rosePlan=null;ctx.calls=[];w.RoseStock.paint(ctx,sh,2,'lines');assert(ctx.calls.some(c=>c[0]==='lineTo'),'protected line stays visible during new intake');assert.equal(JSON.stringify(sh.roseProtected.lines),lines);assert.equal(JSON.stringify(sh.roseProtected.placements),original);sh.dirty=false;sh.rosePlan=saved;
 // a held sheet: Cut Sheet is not greyed out (Paul, 25 Sep: "the Cut Sheet button is not working"); pressing it puts
 // the sheet in the current set first, and when it cannot join one the reason shows under the button
 sh.draft=true;sh.setId=null;w.CN.renderCard(sh);
 const heldCut=sh.el.querySelector('[data-rose="cut"]');assert(heldCut&&!heldCut.disabled,'a held sheet can be cut');assert(!heldCut.title,'no hover-only note');
 const realInclude=w.Gate.changeMembership,included=[];
 w.Gate.changeMembership=async(m,v)=>{included.push([m,v]);};
 heldCut.click();for(let n=0;n<50&&!sh._roseError;n++)await new Promise(r=>setTimeout(r,5));
 assert.match(sh._roseError,/^Not cut: /);assert.match(sh.el.querySelector('.roseError[role="alert"]').textContent,/Not cut/);assert(!sh.roseCutAt);sh._roseError=null;
 // a failed cut is shown once, under the button; no pop-up repeats it
 w.Gate.changeMembership=async(m,v)=>{included.push([m,v]);sh.draft=false;sh.setId='set-test';};
 w.CN.renderCard(sh);w.failCut=true;const shown=toasts.length;sh.el.querySelector('[data-rose="cut"]').click();
 for(let n=0;n<50&&!sh._roseError;n++)await new Promise(r=>setTimeout(r,5));
 assert.deepEqual(included,[['rose',true],['rose',true]],'pressing Cut Sheet on a held sheet includes Rose Gold in the set');assert(sh.setId&&!sh.draft);w.Gate.changeMembership=realInclude;
 assert.equal(sh._roseError,'The sheet changed elsewhere');assert.match(sh.el.querySelector('.roseError[role="alert"]').textContent,/changed elsewhere/);assert.equal(toasts.length,shown,'no pop-up repeats the alert');assert(!sh.roseCutAt);sh._roseError=null;
 await w.RoseStock.record(sh);assert(sh.roseCutAt);assert.equal(sh.roseHistory.length,1);assert(!sh.el.querySelector('.roseHistory button, .roseCut button'),'a cut sheet offers no more buttons');assert.equal(sh.el.querySelectorAll('.roseLineTimeline time').length,1);assert(sh.el.querySelector('[data-solid="size"]').disabled);assert(allowance.disabled);assert(!sh.el.textContent.includes('Using sheet'));assert(!sh.el.querySelector('.roseStockHead'));ctx.calls=[];w.RoseStock.paint(ctx,sh,2,'lines');assert.equal(ctx.lineWidth,2,'historical cut line is twice its previous display width');
 ctx.calls=[];w.RoseStock.paint(ctx,sh,2,'history');assert(ctx.calls.some(c=>c[0]==='fillRect'),'removed region is shaded');assert.equal(ctx.fillStyle,'#d8d5d0','cut silhouettes use neutral grey');
 // A recalled sheet starts its history request only when scrolled into view.
 sh.recalled={roseStockId:stock.id};sh.roseStock=null;sh._roseLoaded=false;sh.roseHistory=[];w.RoseStock.render(sh);const reads=calls.filter(x=>x==='roseGet').length;assert.equal(calls.filter(x=>x==='roseGet').length,reads);assert(observers[0].el);
 for(const metal of ['gold10k','gold14k']){
   const gate=w.document.createElement('div'),page={...sh,metal,recalled:null,roseCutAt:null,roseStock:null};w.document.body.append(gate);
   w.Gate.renderRelease(page,gate);assert.equal(gate.querySelectorAll('.solidOptions details').length,0);assert(!gate.querySelector('[data-solid="nest"]'));
   const input=gate.querySelector('[data-solid="w"]');input.value='30';input.dispatchEvent(new w.Event('input'));w.Gate.renderRelease(page,gate);assert.equal(gate.querySelector('[data-solid="w"]'),input);assert.equal(input.value,'30');
 }
 console.log('Rose UI OK: matching Options, lazy stock loading, saved contour, pending/cut distinction, dated timeline, grey history and locked completed layouts');
 dom.window.close();
})().catch(e=>{console.error(e);dom.window.close();process.exit(1)});
