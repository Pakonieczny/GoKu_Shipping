// Execute the actual search handlers and renderers with deterministic orders. No services are used.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const root=path.join(__dirname,'../..'),sw=fs.readFileSync(path.join(root,'charm-nest-sheetwin.js'),'utf8'),bridge=fs.readFileSync(path.join(root,'charm-nest-bridge.js'),'utf8');
const O=require('../../charm-nest-orders.js');
const between=(source,a,b,from=0)=>{const i=source.indexOf(a,from),j=source.indexOf(b,i);assert(i>=0&&j>i,`source boundaries: ${a}`);return source.slice(i,j);};
const dom=new JSDOM('<input id="find"><input id="findPiece"><ol id="orders"></ol><div id="owSheetPanel"><div id="owSheetMatches"></div></div><div id="reviewView"></div>',{runScripts:'outside-only',pretendToBeVisual:true});
const w=dom.window,d=w.document,noop=()=>{},esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
w.eval(fs.readFileSync(path.join(root,'charm-nest-activity.js'),'utf8'));
const pieces=['3701200001','3712200002','3799900003','4812000004'].map((rid,i)=>({rid,id:'p'+i,poolId:rid+'_1_1',sku:i===3?'SKU-3701':'SKU-'+i,name:'Charm',p:{cxPt:i,cyPt:i}}));
const ctx=new Proxy({},{get:(o,k)=>o[k]||noop,set:(o,k,v)=>(o[k]=v,true)}),halos=[];
Object.assign(w,{O,CharmNestOrders:O,esc,byId:id=>d.getElementById(id),tip:noop,lightChips:noop,otherSheets:()=>[],fillTarget:()=>null,backFace:()=>false,focusSet:()=>null,withPiece:(c,x,fn)=>fn(),outlinePath:noop,markBack:noop,haloOf:x=>({id:x.id,r:1}),layHalo:(c,h)=>halos.push(h.id),plateX:x=>x,paintLanding:noop,paintGhost:noop,paintHand:noop,still:()=>true,Mo:()=>null,selectPiece:noop});
w.W={id:'test',view:'sheet',q:'',filter:'all',orders:new Map(O.orderGroups(pieces)),pieces,going:new Map(),st:{},geom:true,k:1,R:0,dpr:1,fx:[],freed:[],el:{find:d.getElementById('find'),findPiece:d.getElementById('findPiece'),orders:d.getElementById('orders'),fx:{width:100,height:100,getContext:()=>ctx}}};
w.eval(between(sw,'  const matchesQ =','\n',sw.indexOf('  const matchesQ ='))+'\n'+between(sw,'  function putRows(html)','  /** A take-off')+between(sw,'  function renderOrders()','  function renderFoot()')+between(sw,'  function paintMarks(now)','  /** The charm in hand:')+'\nwindow.paintFx=paintMarks;');
w.showSheetPane=()=>{w.W.view='sheet';w.renderOrders();w.paintFx();};
w.eval('(()=>{const E=W.el;'+between(sw,'    const search = input =>','    E.toSheet.onclick')+'})();');
const input=(el,q)=>{el.focus();el.value=q;el.dispatchEvent(new w.Event('input',{bubbles:true}));};
const ids=selector=>[...d.querySelectorAll(selector)].map(n=>n.dataset.rid || n.dataset.orderRid);
try {
  // Full sheet: every input event filters immediately and paints exactly the possible pieces.
  for(const [q,expected] of [['0',4],['3',3],['37',3],['370',1],['3701',1],['370199',0],['37',3],['',4]]){
    halos.length=0;input(w.W.el.find,q);
    assert.equal(ids('.swOrd').length,expected,q);assert.equal(d.querySelectorAll('.swOrd.orderMatch').length,q?expected:0);
    assert.deepEqual([...halos].sort(),q?pieces.filter(x=>O.orderMatches(x.rid,q)).map(x=>x.id).sort():[],`sheet highlight ${q}`);
    assert.equal(d.activeElement,w.W.el.find,'typing retains focus');
  }
  w.W.view='piece';input(w.W.el.findPiece,'370');assert.equal(w.W.view,'sheet');assert.equal(d.activeElement,w.W.el.find);assert.equal(w.W.el.find.value,'370');assert.deepEqual(ids('.swOrd'),['3701200001']);
  input(w.W.el.find,'#370-1');assert.deepEqual(ids('.swOrd'),['3701200001'],'formatted pasted order numbers work');
  // Order popup: its result rows and its canvas use the identical current query.
  w.eval(between(sw,'  const orderHighlights =','\n',sw.indexOf('  const orderHighlights ='))+'\nwindow.highlights=orderHighlights;');
  const G={pieces,mine:[pieces[3]],search:''};w.SV={q:'',info:{pieces,search:q=>{G.search=q;return O.orderGroups(pieces,q);}}};w.unpick=noop;
  w.eval(between(bridge,'  function paintSheetMatches()','  function drawCharmInto'));
  for(const [q,expected] of [['3',3],['37',3],['370',1],['370199',0],['37',3],['',0]]){
    w.SV.q=q;w.paintSheetMatches();assert.equal(ids('[data-order-rid]').length,expected,q);
    assert.deepEqual(Array.from(w.highlights(G),x=>x.rid).sort(),q?ids('[data-order-rid]').sort():['4812000004']);
    assert.equal(d.getElementById('owSheetMatches').hidden,!q);
  }
  // Review: exercise the real render(), across Open and Completed, including a match beyond page one.
  const review=pieces.map((x,i)=>({key:'r'+i,kind:'needsMapping',row:{key:'row'+i,order:{receiptId:x.rid,createTs:100-i}}}));
  for(let i=0;i<45;i++)review.push({key:'extra'+i,kind:'needsMapping',row:{key:'extra'+i,order:{receiptId:'8800'+i.toString(2).padStart(6,'0').replace(/1/g,'8'),createTs:50-i}}});
  review.push({key:'last',kind:'needsMapping',row:{key:'last',order:{receiptId:'9998887776',createTs:1}}});
  const reviewRow=it=>{const n=d.createElement('div');n.className='reviewListRow';n.dataset.rid=it.row.order.receiptId;n.dataset.mkey=it.key;return n;};
  Object.assign(w,{LiveStrip:{render:noop},CustomSheet:{prune:noop,decisionOf:()=>null},CustomRead:{count:()=>0},items:()=>review,mine:()=>true,isNotice:()=>false,rowsOf:it=>[it.row].filter(Boolean),tabOf:it=>it.kind,customLists:()=>({open:[],done:[],sent:[]}),sentToSheet:()=>false,reviewRows:new Map(),reviewFilter:'',KIND_WORDS:{needsMapping:'Options'},employeeName:()=>'',askEmployee:noop,el:(tag,cls)=>{const n=d.createElement(tag);n.className=cls;return n;},reviewRow,settledRow:it=>{const n=d.createElement('div');n.className='reviewListRow';n.dataset.rid=it.orders[0];return n;},mkeyOf:it=>it.key,acted:{},ListMedia:{mount:noop,more:noop},settled:[{key:'done1',kind:'needsMapping',t:1,orders:['3701234567']}]});
  const at=bridge.indexOf('  const RV =');w.eval(between(bridge,'  const RV =','\n',at)+between(bridge,'  function render(opts)','  /** What another tab',at)+'\nwindow.renderReview=render;window.RV=RV;');
  w.renderReview({still:true});const find=d.getElementById('rvOrderFind');assert.equal(ids('#rvList .reviewListRow').length,40);
  for(const [q,expected] of [['3',3],['37',3],['370',1],['370199',0],['37',3],['9998887776',1],['',40]]){
    input(find,q);assert.equal(d.getElementById('rvOrderFind'),find);assert.equal(d.activeElement,find);
    assert.equal(ids('#rvList .reviewListRow').length,expected,q);assert.equal(d.querySelectorAll('#rvList .orderMatch').length,q?expected:0);
  }
  w.RV.cseg='done';input(find,'370');assert.deepEqual(ids('#rvList .reviewListRow'),['3701234567'],'historic decisions search their recorded order numbers');
  input(find,'481');assert.equal(ids('#rvList .reviewListRow').length,0);
  console.log('PASS: all three live searches, each digit, no-match, backspace, clear, focus, off-page results and exact canvas highlights');
}finally{w.close();}
