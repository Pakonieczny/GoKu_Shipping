const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom'),R=require('../../charm-nest-rose');
// The card lists every green line with the date it was first prepared, and
// numbers each dashed line on the preview to match.
const dom=new JSDOM('<section id="sheet"><div class="shPreviewWrap"></div></section>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
w.IntersectionObserver=class{observe(){}unobserve(){}};w.CharmNestRose=R;w.confirm=()=>true;
const outline={subpaths:[[['m',[0,0]],['l',[10,0]],['l',[10,10]],['l',[0,10]],['h']]]};
const T1=Date.UTC(2026,8,22,21,14),T2=Date.UTC(2026,8,23,3,40),T3=Date.UTC(2026,8,23,14,2);
const sh={metal:'rose',el:w.document.getElementById('sheet'),sheetId:'rose-lines',runId:'run-test',fileBase:'RG_Sep.22.26_Set-1_Sheet-1',
  charms:[{id:'a',outline,centerPt:[5,5],members:[]},{id:'b',outline,centerPt:[5,5],members:[]}],placements:[{id:'a',cxPt:8,cyPt:10,angle:0,scale:1}],
  persistedDone:true,verification:{ok:true},status:'complete',dirty:false,draft:false,setId:'set-test',outputs:{},roseStock:{id:'rgs-lines',wPt:100,hPt:50,revision:0,profileJson:null},_roseLoaded:true};
// Stands in for the server: each new batch gets the next dated line.
let plan=null;const at=[T1,T2];
w.CN={S:{cloud:{ok:true},settings:{sandbox:'off'}},esc:s=>String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;'),stockFor:()=>({wPt:100,hPt:50}),uid:()=>'test',allSheets:()=>[sh],drawPreview(){},renderCard:p=>w.RoseStock?.render(p),toast(){},
  api:async(name,b)=>{
    if(b.op==='rosePlan'){
      const shapes=JSON.parse(b.shapesJson),old=new Set(plan?.stages.flatMap(s=>s.ids)||[]),fresh=shapes.filter(s=>!old.has(s.id));
      const next=R.plan(fresh,100,50,plan?.profile||null,b.allowanceMm),from=plan?.lines.length||0;
      plan={...next,lines:[...(plan?.lines||[]),...next.lines],shapes,stages:[...(plan?.stages||[]),{n:(plan?.stages.length||0)+1,at:at.shift(),ids:fresh.map(s=>s.id),lines:[from,from+next.lines.length]}]};
      return {planJson:JSON.stringify(plan),planHash:'hash-'+plan.stages.length};
    }
    if(b.op==='roseRecordCut')return {stock:{...sh.roseStock,revision:1,profileJson:JSON.stringify(plan.profile)},cut:{sheetId:sh.sheetId,stockId:sh.roseStock.id,revision:1,at:T3,planJson:JSON.stringify(plan),planHash:b.planHash,fileBase:sh.fileBase}};
    throw new Error('Unexpected '+b.op);
  }};
w.eval(fs.readFileSync('charm-nest-rose-ui.js','utf8'));
const entries=(sheet=sh)=>[...sheet.el.querySelectorAll('.roseTimeline li')].map(li=>({text:li.textContent.replace(/\s+/g,' ').trim(),time:li.querySelector('time')?.getAttribute('datetime')||null,line:li.classList.contains('roseLineEntry')}));
const canvas=()=>{const calls=[];return {calls,ctx:new Proxy({},{get(o,k){if(k in o)return o[k];return (...args)=>calls.push([k,...args]);},set(o,k,v){o[k]=v;return true;}})};};
const numbers=sheet=>{const c=canvas();w.RoseStock.paint(c.ctx,sheet,2,'lines');return c.calls.filter(x=>x[0]==='fillText').map(x=>String(x[1]));};
(async()=>{
  await w.RoseStock.plan(sh);
  assert.deepEqual(entries(),[{text:entries()[0].text,time:new Date(T1).toISOString(),line:true}]);
  assert.match(entries()[0].text,/Green line 1 · 1 charm · RG_Sep\.22\.26_Set-1_Sheet-1/);
  assert.deepEqual(numbers(sh),['1'],'the dashed line carries its number');
  // New charm files arrive: the saved line stays dated while the sheet is nested again.
  w.RoseStock.protect(sh);sh.dirty=true;w.RoseStock.render(sh);
  assert.equal(entries().length,1);assert.equal(entries()[0].time,new Date(T1).toISOString());assert.deepEqual(numbers(sh),['1'],'the protected line keeps its number during nesting');
  sh.placements.push({id:'b',cxPt:40,cyPt:10,angle:0,scale:1});sh.dirty=false;delete sh.rosePlan;delete sh.rosePlanHash;delete sh.rosePlanKey;
  await w.RoseStock.plan(sh);
  assert.deepEqual(entries().map(e=>[e.time,e.line]),[[new Date(T1).toISOString(),true],[new Date(T2).toISOString(),true]],'both lines are listed oldest first');
  assert.match(entries()[1].text,/Green line 2 · 1 charm/);
  assert.deepEqual(numbers(sh),['1','2']);
  // After the physical cut, the same dates stay in the permanent history, followed by the cut.
  await w.RoseStock.record(sh);
  assert.deepEqual(entries().map(e=>[e.time,e.line]),[[new Date(T1).toISOString(),true],[new Date(T2).toISOString(),true],[new Date(T3).toISOString(),false]]);
  assert.match(entries()[1].text,/Green line 2 · 1 charm · cut 1/);assert.match(entries()[2].text,/Cut recorded · 2 charms/);
  assert.deepEqual(numbers(sh),['1'],'a recorded cut shows its cut number, not dashed line numbers');
  // A line saved before dates were kept says so instead of inventing a time.
  sh.roseHistory[0].plan.stages[0].at=null;w.RoseStock.render(sh);
  assert.equal(entries()[0].time,null);assert.match(entries()[0].text,/Time not recorded/);
  assert.equal(sh.el.querySelectorAll('.roseTimeline time').length,2);
  // A contour prepared before dates were kept still shows its line and number, marked undated.
  const legacy={...sh,el:w.document.createElement('section'),sheetId:'rose-legacy',draft:true,roseCutAt:null,roseHistory:[],rosePlan:{...plan},rosePlanHash:'hash-legacy'};
  legacy.el.innerHTML='<div class="shPreviewWrap"></div>';delete legacy.rosePlan.stages;w.RoseStock.render(legacy);
  assert.equal(entries(legacy).length,1);assert.equal(entries(legacy)[0].time,null);assert.match(entries(legacy)[0].text,/Time not recorded.*Green line 1 · 2 charms/);
  assert.deepEqual(numbers(legacy),['1']);
  console.log('Rose line timeline OK: dated green lines above the sheet, numbered preview lines, protected nesting, permanent cut history and undated earlier lines, including contours saved before dates were kept');
  dom.window.close();
})().catch(e=>{console.error(e);dom.window.close();process.exit(1)});
