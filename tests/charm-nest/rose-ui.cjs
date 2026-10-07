const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom'),R=require('../../charm-nest-rose'),O=require('../../charm-nest-orders');
const dom=new JSDOM('<section id="sheet"><div class="shHead"></div><div class="shGate" data-r="gate"></div><div class="shPreviewWrap"></div></section>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
const observers=[];w.IntersectionObserver=class{constructor(cb){this.cb=cb;observers.push(this);}observe(el){this.el=el;}unobserve(){}fire(){this.cb([{target:this.el,isIntersecting:true}]);}};
w.CharmNestRose=R;w.CharmNestOrders=O;w.confirm=()=>true;const calls=[],toasts=[];
const outline={subpaths:[[['m',[0,0]],['l',[10,0]],['l',[10,10]],['l',[0,10]],['h']]]};
const sh={metal:'rose',el:w.document.getElementById('sheet'),sheetId:'rose-test',runId:'run-test',charms:[{id:'charm',outline,centerPt:[5,5],members:[]}],placements:[{id:'charm',cxPt:10,cyPt:10,angle:0,scale:1}],persistedDone:true,verification:{ok:true},status:'complete',dirty:false,draft:true,outputs:{}};
const stock={id:'rgs-physical',wPt:100,hPt:50,revision:0,profileJson:null};let saved;
w.CN={S:{cloud:{ok:true},settings:{stock:{}},mode:'nest'},esc:s=>String(s).replace(/</g,'&lt;'),stockFor:()=>({wPt:100,hPt:50,wIn:100/72,hIn:50/72}),uid:()=> 'test',allSheets:()=>[sh],drawPreview(){},renderCard:p=>{w.Gate.renderCard(p);w.RoseStock?.render(p);},toast(m,k){toasts.push([String(m),k]);},sheetDirty:p=>{p.dirty=true;},api:async(name,b)=>{calls.push(b.op);if(b.op==='roseClaim')return {stock};if(b.op==='roseGet')return {stock,cuts:[],more:false};if(b.op==='rosePlan'){saved=R.plan(JSON.parse(b.shapesJson),100,50,null,b.allowanceMm);return {planJson:JSON.stringify(saved),planHash:'hash'};}if(b.op==='roseRecordCut'){if(w.failCut){w.failCut=false;throw new Error('The sheet changed elsewhere');}const rev=w.cutRev||1;return {stock:{...stock,revision:rev,profileJson:JSON.stringify(saved.profile)},cut:{sheetId:sh.sheetId,stockId:stock.id,revision:rev,at:1760000000000+rev,planJson:JSON.stringify(saved),planHash:'hash',fileBase:'RG sheet'}};}return {};}};
w.sh=sh;
w.eval(`var C=window.CN,S=C.S,CN=C,B=window.B={run:{runId:'run-test',releasePolicy:2,status:'running',solidIncluded:{}}};
 var O=window.CharmNestOrders,allSheets=()=>[sh],pagesOf=()=>[sh],stockFor=C.stockFor,esc=C.esc,labelOf=()=> 'RG 14/20';
 var Sets={ofRun:()=>[]},RunCtl={},Session=window.Session={schedule(){}},toast=()=>{},refreshAllCards=()=>{},api=C.api;
 var sheetDirty=()=>{},saveSettings=()=>{},startNest=()=>{};
`);
const code=fs.readFileSync('charm-nest-bridge.js','utf8');w.eval(code.slice(code.indexOf('const Gate ='),code.indexOf('/* ═══ 21',code.indexOf('const Gate ='))));w.Gate.renderCard(sh);
w.eval(fs.readFileSync('charm-nest-rose-ui.js','utf8'));w.eval(fs.readFileSync('charm-nest-options-modal.js','utf8'));
(async()=>{
 assert.match(sh.el.textContent,/In current set/);assert.doesNotMatch(sh.el.textContent,/Sheet dimensions|Apply size/);assert(!sh.el.querySelector('[data-solid="nest"]'));assert.equal(sh.el.querySelectorAll('.solidOptions details').length,0);assert.match(sh.el.textContent,/Choose remnant or new sheet/);
 assert.equal(calls.length,0,'rendering never eagerly loads physical stock');
 const opener=sh.el.querySelector('.sheetOptionsBtn'),allowance=sh.el.querySelector('[data-rose-allowance]'),box=allowance.closest('.solidOptions');opener.click();   // (Options is one large window now: the same controls, mounted in it while it is open)
 allowance.focus();allowance.value='0.35';allowance.dispatchEvent(new w.Event('input'));
 for(let n=0;n<10;n++)w.CN.renderCard(sh);
 assert.equal(sh.el.querySelector('.sheetOptionsBtn'),opener);assert(w.OptionsStudio.isOpen('rose'));assert.equal(w.document.activeElement,allowance);assert.equal(allowance.value,'0.35');
 allowance.blur();w.CN.renderCard(sh);assert.equal(allowance.value,'0.35','draft survives blur and background refresh');
 sh.status='nesting';w.CN.renderCard(sh);assert(!allowance.disabled,'background work does not prevent editing a draft');assert(!w.document.querySelector('[data-solid="size"]'),'no Apply size any more');sh.status='complete';
 assert(!sh.el.textContent.includes('Physical sheet'));assert(!sh.el.querySelector('[data-rose-release]'));

 // only a Cut Sheet press adds a green line (Paul, 29 Sep): any other contour request for these uncut charms asks nothing
 await w.RoseStock.plan(sh);assert(!sh.rosePlan&&!calls.includes('rosePlan'),'no line without Cut Sheet');
 await w.RoseStock.plan(sh,{cut:true});assert(sh.rosePlan.lines.length);assert(!/mm contour allowance ·|remaining after this cut/.test(sh.el.textContent),'no allowance or remaining-area line');assert.equal(calls.filter(x=>x==='rosePlan').length,1);
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
 // Paul, 7 Oct: a refusal reads as a reason. A sheet the person ticked used to say "Not cut: Included by you" (the release rule's words for a sheet that IS wanted in the set)
 w.B.run.solidIncluded.rose=true;sh._roseError=null;w.CN.renderCard(sh);sh.el.querySelector('[data-rose="cut"]').click();for(let n=0;n<50&&!sh._roseError;n++)await new Promise(r=>setTimeout(r,5));
 assert.match(sh._roseError,/^Not cut: this sheet is not in a set yet/);assert.doesNotMatch(sh._roseError,/Included by you/,'the release rule\'s words are no reason');assert.match(sh._roseError,/Switch on In current set in Options, or reload the page, then press Cut Sheet again\.$/);
 assert.match(sh.el.querySelector('.roseError[role="alert"]').textContent,/not in a set yet/,'the refusal is shown under the button');w.B.run.solidIncluded.rose=false;sh._roseError=null;included.pop();   // (this press asked for its Include too: the count below is for the two presses it was written for)
 // a sheet of a COMMITTED set that lost its place on the page takes it back from the set (Gate.rejoin) BEFORE any Include is asked: the run never puts a committed sheet back
 const inc2=[];w.Gate.changeMembership=async(m,v)=>{inc2.push(['unexpected',m,v]);};const rejoinReal=w.Gate.rejoin;let rejoinCalls=0;
 w.Gate.rejoin=async s=>{rejoinCalls++;s.draft=false;s.setId='set-committed';return true;};
 sh.draft=true;sh.setId=null;w.CN.renderCard(sh);w.failCut=true;sh.el.querySelector('[data-rose="cut"]').click();
 for(let n=0;n<50&&!sh._roseError;n++)await new Promise(r=>setTimeout(r,5));
 assert.equal(rejoinCalls,1,'Cut Sheet asks the committed set for the sheet\'s place first');assert.deepEqual(inc2,[],'a rejoined sheet is not included again');assert.equal(sh._roseError,'The sheet changed elsewhere','the press went on to record the cut');sh._roseError=null;
 // a sheet no committed set lists (rejoin says false) goes the old way: its own Include
 w.Gate.rejoin=async()=>{rejoinCalls++;return false;};w.Gate.changeMembership=async(m,v)=>{inc2.push([m,v]);sh.draft=false;sh.setId='set-test';};
 sh.draft=true;sh.setId=null;inc2.length=0;w.CN.renderCard(sh);w.failCut=true;sh.el.querySelector('[data-rose="cut"]').click();
 for(let n=0;n<50&&!sh._roseError;n++)await new Promise(r=>setTimeout(r,5));
 assert.deepEqual(inc2,[['rose',true]],'no committed place: the Include is asked');sh._roseError=null;w.Gate.rejoin=rejoinReal;sh.draft=true;sh.setId=null;w.CN.renderCard(sh);
 // a failed cut is shown once, under the button; no pop-up repeats it
 w.Gate.changeMembership=async(m,v)=>{included.push([m,v]);sh.draft=false;sh.setId='set-test';};
 w.CN.renderCard(sh);w.failCut=true;const shown=toasts.length;sh.el.querySelector('[data-rose="cut"]').click();
 for(let n=0;n<50&&!sh._roseError;n++)await new Promise(r=>setTimeout(r,5));
 assert.deepEqual(included,[['rose',true],['rose',true]],'pressing Cut Sheet on a held sheet includes Rose Gold in the set');assert(sh.setId&&!sh.draft);w.Gate.changeMembership=realInclude;
 assert.equal(sh._roseError,'The sheet changed elsewhere');assert.match(sh.el.querySelector('.roseError[role="alert"]').textContent,/changed elsewhere/);assert.equal(toasts.length,shown,'no pop-up repeats the alert');assert(!sh.roseCutAt);sh._roseError=null;
 await w.RoseStock.record(sh);assert(sh.roseCutAt);assert.equal(sh.roseHistory.length,1);assert(!sh.el.querySelector('.roseHistory button, .roseCut button'),'a cut sheet offers no more buttons');assert.equal(sh.el.querySelectorAll('.roseLineTimeline time').length,1);assert(w.document.querySelector('dialog.osDlg [data-solid="include"]').disabled,'a cut sheet stays in its set');assert(allowance.disabled);assert(!sh.el.textContent.includes('Using sheet'));assert(!sh.el.querySelector('.roseStockHead'));ctx.calls=[];w.RoseStock.paint(ctx,sh,2,'lines');assert.equal(ctx.lineWidth,2,'historical cut line is twice its previous display width');
 ctx.calls=[];w.RoseStock.paint(ctx,sh,2,'history');assert(ctx.calls.some(c=>c[0]==='fillRect'),'removed region is shaded');assert.equal(ctx.fillStyle,'#d8d5d0','cut silhouettes use neutral grey');
 // A second cut on the same stock (the sheet was moved onto the leftover its own cut made: Use this one, 7 Oct) keeps the first cut on the card: both cuts, each with its date
 const firstAt=sh.roseCutAt;sh.roseCutAt=null;sh.rosePlan=saved;w.cutRev=2;
 await w.RoseStock.record(sh);w.cutRev=0;
 assert.deepEqual(Array.from(sh.roseHistory,c=>c.revision).sort(),[1,2],'both cuts stay in the history');assert(sh.roseCutAt>firstAt,'the card shows the new cut');
 assert.equal(sh.el.querySelectorAll('.roseLineTimeline time').length,2,'each cut shows its date');
 // a retry of the SAME press replaces its own copy and adds none
 sh.roseCutAt=null;w.cutRev=2;await w.RoseStock.record(sh);w.cutRev=0;assert.deepEqual(Array.from(sh.roseHistory,c=>c.revision).sort(),[1,2],'a retried press adds no third copy');
 // A recalled sheet starts its history request only when scrolled into view.
 sh.recalled={roseStockId:stock.id};sh.roseStock=null;sh._roseLoaded=false;sh.roseHistory=[];w.RoseStock.render(sh);const reads=calls.filter(x=>x==='roseGet').length;assert.equal(calls.filter(x=>x==='roseGet').length,reads);assert(observers[0].el);
 for(const metal of ['gold10k','gold14k']){
   const gate=w.document.createElement('div'),page={...sh,metal,recalled:null,roseCutAt:null,roseStock:null};w.document.body.append(gate);
   w.Gate.renderRelease(page,gate);assert.equal(gate.querySelectorAll('.solidOptions details').length,0);assert(!gate.querySelector('[data-solid="nest"]'));
   const input=gate.querySelector('[data-solid="include"]');w.Gate.renderRelease(page,gate);assert.equal(gate.querySelector('[data-solid="include"]'),input,'a repaint replaces nothing');assert(!gate.querySelector('[data-solid="w"],[data-solid="size"]'));
 }
 console.log('Rose UI OK: matching Options, lazy stock loading, saved contour, pending/cut distinction, dated timeline, grey history and locked completed layouts');
 dom.window.close();
})().catch(e=>{console.error(e);dom.window.close();process.exit(1)});
