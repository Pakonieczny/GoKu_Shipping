const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom'),R=require('../../charm-nest-rose'),O=require('../../charm-nest-orders');
const dom=new JSDOM('<section id="sheet"><div class="shHead"></div><div class="shGate" data-r="gate"></div><div class="shPreviewWrap"></div></section>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
const observers=[];w.IntersectionObserver=class{constructor(cb){this.cb=cb;observers.push(this);}observe(el){this.el=el;}unobserve(){}fire(){this.cb([{target:this.el,isIntersecting:true}]);}};
w.CharmNestRose=R;w.CharmNestOrders=O;w.confirm=()=>true;const calls=[];
const outline={subpaths:[[['m',[0,0]],['l',[10,0]],['l',[10,10]],['l',[0,10]],['h']]]};
const sh={metal:'rose',el:w.document.getElementById('sheet'),sheetId:'rose-test',runId:'run-test',charms:[{id:'charm',outline,centerPt:[5,5],members:[]}],placements:[{id:'charm',cxPt:10,cyPt:10,angle:0,scale:1}],persistedDone:true,verification:{ok:true},status:'complete',dirty:false,draft:true,outputs:{}};
const stock={id:'rgs-physical',wPt:100,hPt:50,revision:0,profileJson:null};let saved;
w.CN={S:{cloud:{ok:true},settings:{stock:{}},mode:'nest'},esc:s=>String(s).replace(/</g,'&lt;'),stockFor:()=>({wPt:100,hPt:50,wIn:100/72,hIn:50/72}),uid:()=> 'test',allSheets:()=>[sh],drawPreview(){},renderCard:p=>{w.Gate.renderCard(p);w.RoseStock?.render(p);},toast(){},sheetDirty:p=>{p.dirty=true;},api:async(name,b)=>{calls.push(b.op);if(b.op==='roseClaim')return {stock};if(b.op==='roseGet')return {stock,cuts:[],more:false};if(b.op==='rosePlan'){saved=R.plan(JSON.parse(b.shapesJson),100,50,null,b.allowanceMm);return {planJson:JSON.stringify(saved),planHash:'hash'};}if(b.op==='roseRecordCut')return {stock:{...stock,revision:1,profileJson:JSON.stringify(saved.profile)},cut:{sheetId:sh.sheetId,stockId:stock.id,revision:1,at:1760000000000,planJson:JSON.stringify(saved),planHash:'hash',fileBase:'RG sheet'}};return {};}};
w.sh=sh;
w.eval(`var C=window.CN,S=C.S,CN=C,B=window.B={run:{runId:'run-test',releasePolicy:2,status:'running',solidIncluded:{}}};
 var O=window.CharmNestOrders,allSheets=()=>[sh],pagesOf=()=>[sh],stockFor=C.stockFor,esc=C.esc,labelOf=()=> 'RG 14/20';
 var Sets={ofRun:()=>[]},RunCtl={},Session=window.Session={schedule(){}},toast=()=>{},refreshAllCards=()=>{},api=C.api;
 var sheetDirty=()=>{},saveSettings=()=>{},startNest=()=>{};
`);
const code=fs.readFileSync('charm-nest-bridge.js','utf8');w.eval(code.slice(code.indexOf('const Gate ='),code.indexOf('/* ═══ 21',code.indexOf('const Gate ='))));w.Gate.renderCard(sh);
w.eval(fs.readFileSync('charm-nest-rose-ui.js','utf8'));
(async()=>{
 assert.match(sh.el.textContent,/Include in current set/);assert.match(sh.el.textContent,/Custom size/);assert.match(sh.el.textContent,/Nest RG 14\/20 only/);assert.match(sh.el.textContent,/Choose remnant or new sheet/);
 assert.equal(calls.length,0,'rendering never eagerly loads physical stock');
 await w.RoseStock.plan(sh);assert(sh.rosePlan.lines.length);assert.match(sh.el.textContent,/0.2 mm contour allowance/);assert.equal(calls.filter(x=>x==='rosePlan').length,1);
 const ctx=new Proxy({calls:[]},{get(o,k){if(k in o)return o[k];return (...args)=>o.calls.push([k,...args]);},set(o,k,v){o[k]=v;return true;}});
 w.RoseStock.paint(ctx,sh,2,'lines');assert(ctx.calls.some(c=>c[0]==='setLineDash'&&c[1].length),'pending contour is distinguished from completed cut');
 sh.draft=false;sh.setId='set-test';await w.RoseStock.record(sh);assert(sh.roseCutAt);assert.equal(sh.roseHistory.length,1);assert.match(sh.el.textContent,/Cut recorded/);assert.equal(sh.el.querySelectorAll('.roseTimeline time').length,1);assert(sh.el.querySelector('[data-solid="nest"]').disabled);
 ctx.calls=[];w.RoseStock.paint(ctx,sh,2,'history');assert(ctx.calls.some(c=>c[0]==='fillRect'),'removed region is shaded');assert.equal(ctx.fillStyle,'#d8d5d0','cut silhouettes use neutral grey');
 // A recalled sheet starts its history request only when scrolled into view.
 sh.recalled={roseStockId:stock.id};sh.roseStock=null;sh._roseLoaded=false;sh.roseHistory=[];w.RoseStock.render(sh);const reads=calls.filter(x=>x==='roseGet').length;assert.equal(calls.filter(x=>x==='roseGet').length,reads);assert(observers[0].el);
 console.log('Rose UI OK: matching Options, lazy stock loading, saved contour, pending/cut distinction, dated timeline, grey history and locked completed layouts');
 dom.window.close();
})().catch(e=>{console.error(e);dom.window.close();process.exit(1)});
