// Both real inspector handlers share the engraving preview and immutable approval record.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),{JSDOM}=require('jsdom');
const dom=new JSDOM('<div id="owSheetPanel"></div><section id="sheetEng"></section>',{runScripts:'outside-only'}),w=dom.window,d=w.document;
w.matchMedia=()=>({matches:true});w.eval(fs.readFileSync('charm-nest-motion.js','utf8'));w.eval(fs.readFileSync('charm-nest-engraving-seals.js','utf8'));
const E=w.CNEngravingSeals,old={how:'engraveApproved',at:1000,by:'Paul'},latest={how:'engraveApproved',at:2000,by:'Seth'};
let previews=0,approvedButton,finish,approval;
const row={key:'order_line',order:{receiptId:'4170837249',buyer:{name:'Test buyer'}},line:{sku:'MIDDLE'},spec:{designSku:'MIDDLE'},engrave:{needed:true,state:'review',seals:[old]},poolIds:['p1']};
const job={key:row.key,row,state:'review',fit:{},view:{},text:'I\ndissent',engravingSeals:[old]};
const eng={kind:'approve',job,text:job.text};
w.Engrave={jobOf:()=>job,renderBack(){previews++;const cv=d.createElement('canvas');cv.width=300;cv.height=600;return cv;},async approve(j,who,b){approvedButton=b;approval=new Promise(r=>finish=r);await approval;j.state='approved';j.approvedBy=who;j.approvedAt=2000;E.add(j,'engraveApproved',who,2000);}};
const x={poolId:'p1',rid:row.order.receiptId,sku:'MIDDLE',eng},rec={id:'sheet1',metal:'rose',status:'complete',setSeq:2},inf={rec,mine:[x],sheet:{n:1}};
const SV={info:inf,list:[{id:'sheet1',metal:'rose',n:1}],at:0,q:'',pools:[]};
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const c=vm.createContext({window:w,document:d,CNEngravingSeals:E,Engrave:w.Engrave,SV,W:{key:row.key,face:'front'},BK:[],Orders:{shipTxt:()=>''},byId:id=>d.getElementById(id),rowOf:()=>row,linesOf:()=>[row],sheetName:()=> 'RG Sheet 1',colorOf:()=> '#b87d71',labelOf:()=> 'RG 14/20',nOf:()=>1,esc,tryDo:f=>f(),ListMedia:{vectorInto(){}},paintSheetMatches(){},drawCharmInto(){},me:()=> 'Seth',askEmployee(){},toast(){},fullSheet(){},goBack(){},sheetDraw(){}});
const src=fs.readFileSync('charm-nest-bridge.js','utf8'),a=src.indexOf('  function paintPanel(inf) {'),b=src.indexOf('  function paintSheetMatches()',a);vm.runInContext(src.slice(a,b),c);
(async()=>{try{
  c.paintPanel(inf);
  const panel=d.getElementById('owSheetPanel'),button=panel.querySelector('[data-e=approve]');
  assert(button.classList.contains('egApproveButton'));assert.equal(panel.querySelector('.disc'),null);assert.equal(panel.querySelector('.words').textContent,'I\ndissent');assert(panel.querySelector('.pv canvas'));assert.equal(panel.querySelectorAll('.seal').length,1,'prior approval remains visible while reopened');
  for(const selector of ['[data-at]','#owSheetOrderFind','.owCharm','.owPieces','[data-full]','[data-off]','[data-e=engrave]'])assert(panel.querySelector(selector),selector+' remains available');
  button.click();await Promise.resolve();assert.equal(approvedButton,button,'Order window stamps the button actually clicked');assert(button.isConnected);assert.equal(job.state,'review');finish();await approval;await new Promise(setImmediate);
  assert(panel.querySelector('.egApproveWrap .seal'),'latest approval rests half over the approved button');assert.equal(panel.querySelectorAll('.seal').length,2,'new approval does not erase the old seal');assert(panel.querySelector('.egApproveButton').disabled);
  const sh=d.getElementById('sheetEng');sh.innerHTML=E.panel({kind:'approved',back:{approvedAt:2000,approvedBy:'Seth',engravingSeals:[old,latest],png:'/saved.png'},text:'I\ndissent'});
  assert.deepEqual([...sh.querySelectorAll('.seal')].map(s=>s.getAttribute('aria-label')).sort(),[...panel.querySelectorAll('.seal')].map(s=>s.getAttribute('aria-label')).sort(),'saved-back and active-job inspectors show identical approval provenance');
  E.wirePanel(sh,{kind:'approved',back:{png:'https://example.com/saved.png'}},{imageUrl:url=>'/image-proxy?url='+encodeURIComponent(url)});
  assert.equal(sh.querySelector('.pv img').getAttribute('src'),'/image-proxy?url=https%3A%2F%2Fexample.com%2Fsaved.png','saved images use the inspector image proxy');
  // The other green button also passes itself to the same approval and waits before repainting.
  let repaints=0;job.state='review';x.eng=eng;sh.innerHTML=E.panel(eng);const sheetButton=sh.querySelector('[data-e=approve]');
  const sw=fs.readFileSync('charm-nest-sheetwin.js','utf8'),sa=sw.indexOf('  async function approveHere('),sb=sw.indexOf('  /* ── the order',sa);
  const sc=vm.createContext({Engrave:w.Engrave,W:{sel:x},needName:()=> 'Seth',ICON:{check:''},window:{},renderEng(){repaints++;},renderStrip(){},renderOrders(){},paintFx(){},toast(){}});vm.runInContext(sw.slice(sa,sb),sc);
  const p=sc.approveHere(x,sheetButton);await Promise.resolve();assert.equal(approvedButton,sheetButton);assert.equal(repaints,0);finish();await p;assert.equal(repaints,1);
  const legacy=E.panel({kind:'approved',saved:{needed:true,approved:true}});assert.match(legacy,/not recorded/i,'legacy approvals are visible without inventing a timestamp');
  assert(previews>0);console.log('PASS: both inspector buttons await their own stamp; uncropped shared preview, all controls retained, saved and reopened approval histories match');
}finally{w.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
