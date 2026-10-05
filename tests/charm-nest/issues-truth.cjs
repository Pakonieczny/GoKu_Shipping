// Point 9 (Paul, 5 Oct): the issues list said "48 of 48 orders wait" and most of its SKU and pooled warnings were false alarms.
// What an order really waits for, and what CharmNestReadiness.issues() / orderReports() / forSheet() / explain() and the server's per-sheet answer say about it.
// Offline fixtures only: the page's own rows and the saved sheets are plain objects; the server part runs the real laserStatus over the in-memory shop.
const assert=require('node:assert/strict');
const R=require('../../charm-nest-readiness.js');
const clone=x=>JSON.parse(JSON.stringify(x));
const back=(id,sheetId)=>({poolId:id,sheetId,approvedAt:10,approvedBy:'Paul',verified:{geometry:{ok:true},file:{ok:true}},outputs:{ai:{path:id+'.ai',url:'https://example.com/'+id+'.ai'}}});
const CODE={gold:'GF',silver:'SS',rose:'RG'};
// a saved sheet holding these pool ids; approved:false = its engravings still wait for approval (the sheet's own trouble, never an order's)
function sheet(id,metal,pool,{approved=true,index=1,setId='set1',extra={}}={}){
  const orders=[...new Set(pool.map(p=>p.split('_')[0]))];
  return {id,metal,setId,runId:'run1',sheetIndex:index,status:'complete',poolIds:pool,placedCount:pool.length,verification:{ok:true},preview:'https://example.com/p.png',outputs:{ai:'https://example.com/f.ai'},
    orders,label:{files:[{path:'qr.png',url:'https://example.com/qr.png',payload:'x',orders}]},backPool:approved?pool.map(p=>back(p,id)):[],
    engraving:Object.fromEntries(pool.map(p=>[p,approved?{needed:true,state:'written',approved:true}:{needed:true,state:'review',approved:false}])),...extra};
}
// an order row as the page holds it (Orders.rows()), and as the server's run record holds it
function row(order,txn,o={}){
  const key=`${order}_${txn}`,q=o.quantity||1;
  return {key,order:{receiptId:order,buyer:{name:o.buyer||'Customer '+order}},line:{title:o.title||'Charm '+txn,listingId:o.listing||'L'+order,transactionId:txn},state:o.state||'written',
    poolIds:o.poolIds===undefined?[...Array(q)].map((_,i)=>`${key}_${i+1}`):o.poolIds,spec:{quantity:q,designSku:o.sku===undefined?'SKU-'+txn:o.sku,noDesign:!!o.noDesign},
    problems:(o.problems||[]).map(kind=>({kind})),hold:o.hold||null,changePending:!!o.changePending,...(o.reason?{reason:o.reason}:{})};
}
const srv=r=>({key:r.key,orderId:r.order.receiptId,state:r.state,poolIds:r.poolIds,quantity:r.spec.quantity,sku:r.spec.designSku,noDesign:r.spec.noDesign,problems:r.problems.map(p=>p.kind),hold:r.hold,changePending:r.changePending,snap:{title:r.line.title,listingId:r.line.listingId,buyer:r.order.buyer.name},transactionId:r.line.transactionId});
const of=(s,rows,all)=>R.issues(s,{rows,allSheets:all||[s]});
const orderIssues=(s,rows,all)=>of(s,rows,all).filter(i=>i.step==='orders');
const step=(e,k)=>e.steps.find(x=>x.key===k);
const strings=(x,out=[])=>{if(typeof x==='string')out.push(x);else if(x&&typeof x==='object')for(const v of Object.values(x))strings(v,out);return out;};

// 1. Paul's screenshot: a sheet of 48 single-piece orders whose engravings still wait for approval. Nothing in the order check, not one of the 48.
const pool48=[...Array(48)].map((_,i)=>`${4170000000+i}_t${i}_1`);
const ss=sheet('ss1','silver',pool48,{approved:false});
const rows48=pool48.map(p=>row(p.split('_')[0],p.split('_')[1]));
assert.equal(orderIssues(ss,rows48).length,0,'48 single-piece orders: zero order issues');
const own=of(ss,rows48);
assert.deepEqual(own.map(i=>[i.step,i.key,i.label]),[['engraving','approvalsNeeded','Approvals needed']],'only the sheet\'s own blocker, one entry, no counts');
assert.equal(own[0].orderId,undefined);assert.deepEqual(own[0].open,{type:'sheet',id:'ss1'});
const reps48=R.orderReports(rows48,[ss]);
assert.equal(Object.values(reps48).filter(r=>!r.ready).length,48,'the whole-order reading still knows each piece sits on a sheet that is not ready');
const seen=clone(ss);seen.orderReadiness=Object.fromEntries(R.orderIds(ss).map(o=>[o,R.forSheet(reps48[o],'ss1')]));
assert(Object.values(seen.orderReadiness).every(r=>r.ready===true),'but read from the sheet that holds them, every one is ready');
assert.equal(R.sheet(seen).stages.orders,true);assert.equal(R.orderBlockers(seen).length,0);assert.equal(R.sheet(seen).ready,false,'the sheet itself is still not ready: its own engraving');
let e=R.explain(seen,{rows:rows48});
assert.equal(step(e,'orders').state,'done');assert.equal(step(e,'orders').items.length,0);assert.match(step(e,'orders').detail,/All 48 orders/);
assert.equal(e.step,'engraving');assert.doesNotMatch(e.nextText,/48 orders/);
// the same through the global map the page used to hand every sheet unread (bridge validateRelease, the order window's timeline)
const raw=clone(ss);raw.orderReadiness=reps48;assert.equal(R.sheet(raw).stages.orders,true,'a sheet reads a whole-order map from its own side');assert.equal(R.orderBlockers(raw).length,0);
// the sheet that becomes ready is ready: its only trouble was the circular order check
const fixed=sheet('ss1','silver',pool48);fixed.orderReadiness=reps48;
assert.equal(R.sheet(fixed).ready,true);assert.equal(R.laserSheet(fixed).ready,true);

// 2. Two pieces, the other on a second sheet that is not ready: exactly one issue, naming that sheet; the not-ready sheet itself never blames itself
{
  const gf=sheet('gf1','gold',['4170252963_a_1','5000_x_1'],{index:1}),ssB=sheet('ss1','silver',['4170252963_b_1','5001_y_1'],{approved:false,index:1});
  const rs=[row('4170252963','a',{buyer:'Nathaly Soto',title:'Female symbol',listing:'7777'}),row('4170252963','b',{state:'pooled',title:'Female symbol'}),row('5000','x'),row('5001','y')];
  const all=[gf,ssB];
  const a=orderIssues(gf,rs,all);
  assert.equal(a.length,1);assert.equal(a[0].orderId,'4170252963');assert.equal(a[0].key,'otherSheetNotReady');assert.equal(a[0].customer,'Nathaly Soto');assert.equal(a[0].orderLabel,'Order 4170252963');assert.equal(a[0].listingId,'7777');
  assert.equal(a[0].pieceCount,2);assert.deepEqual(a[0].pieces.map(p=>[p.index,p.sheetLabel]),[[2,'SS Sheet 1']],'the other piece, and the sheet it is on');
  assert.deepEqual(a[0].open,{type:'order',id:'4170252963',poolId:'4170252963_b_1'});assert.equal(a[0].sheetId,'gf1');
  assert(a[0].pieces.every(p=>!/\d+ (back )?engravings?/i.test(p.why)),'that sheet\'s own engraving counts are not repeated');
  assert.doesNotMatch(strings(a[0]).join(' | '),/\bpooled\b/,'its stored state "pooled" is not a reason: the piece is on SS Sheet 1');
  assert.equal(orderIssues(ssB,rs,all).length,0,'SS Sheet 1 does not wait for its own piece, nor for GF Sheet 1 which is ready');
  // the other sheet becomes ready: the order is no issue at all
  const ssOk=sheet('ss1','silver',['4170252963_b_1','5001_y_1'],{index:1});
  assert.equal(orderIssues(gf,rs,[gf,ssOk]).length,0);
  // read from the server's side: the per-sheet map says the same
  const reps=R.orderReports(rs,all),g2=clone(gf);g2.orderReadiness=Object.fromEntries(R.orderIds(gf).map(o=>[o,R.forSheet(reps[o],'gf1')]));
  assert.equal(g2.orderReadiness['4170252963'].ready,false);assert.equal(g2.orderReadiness['5000'].ready,true);assert.equal(R.sheet(g2).stages.orders,false);
  assert.deepEqual(R.issues(g2,{}).filter(i=>i.step==='orders').map(i=>[i.orderId,i.key,i.pieces[0].sheetLabel]),[['4170252963','otherSheetNotReady','SS Sheet 1']],'issues() reads the record\'s own answer when no rows are given');
  const s2=clone(ssB);s2.orderReadiness=Object.fromEntries(R.orderIds(ssB).map(o=>[o,R.forSheet(reps[o],'ss1')]));
  assert(Object.values(s2.orderReadiness).every(r=>r.ready),'SS Sheet 1\'s own map has no waiting order');
  // explain: the Order check step is built on the same issue, in the same words as before
  e=R.explain(g2,{rows:rs});
  assert.equal(step(e,'orders').state,'blocked');assert.deepEqual(step(e,'orders').items.map(i=>[i.kind,i.id]),[['order','4170252963'],['sheet','ss1']]);
  assert.match(step(e,'orders').items[0].why,/Its other piece is on SS Sheet 1, and its engraving needs approval/);assert.match(step(e,'orders').detail,/1 of 2 orders wait/);
}

// 3. An unnested piece: the other piece is not on any sheet yet. A single order of one piece never appears.
{
  const gf=sheet('gf1','gold',['700_a_1','701_a_1']);
  const rs=[row('700','a'),row('700','b',{state:'pooled',poolIds:['700_b_1'],reason:'waiting'}),row('701','a')];
  const i=orderIssues(gf,rs);
  assert.deepEqual(i.map(x=>[x.orderId,x.key]),[['700','pooled']],'one issue: order 700; order 701 has one piece');
  assert.deepEqual(i[0].pieces.map(p=>[p.index,p.sheetLabel,p.kind]),[[2,null,'pooled']]);assert.match(i[0].why,/not on a sheet yet/);
  // a line that lost its pool ids is still an unnested piece (its copy ids are derived)
  const lost=orderIssues(gf,[row('700','a'),row('700','b',{state:'pooled',poolIds:[]}),row('701','a')]);assert.deepEqual(lost.map(x=>[x.orderId,x.key]),[['700','pooled']]);
  // ...and found when a sheet lists "<line key>_<n>" though the record lost them
  const there=sheet('ss1','silver',['700_b_1']);
  assert.equal(orderIssues(gf,[row('700','a'),row('700','b',{state:'pooled',poolIds:[]}),row('701','a')],[gf,there]).length,0,'on a ready sheet: nested, whatever the stored state says');
  // a piece of a sheet that was archived is not on a sheet
  assert.deepEqual(orderIssues(gf,rs,[gf,{...sheet('old','silver',['700_b_1']),archived:true}]).map(x=>x.key),['pooled']);
}

// 4. Missing SKU / design on another piece of a multi-piece order; never for a piece that is on a sheet, never for a single piece
{
  const gf=sheet('gf1','gold',['800_a_1','801_a_1','802_a_1','803_a_1','804_a_1']);
  const rs=[
    row('800','a'),row('800','b',{state:'unmatched',poolIds:[],problems:['unmatchedSku'],sku:'ZZ-1'}),
    row('801','a'),row('801','b',{state:'unmatched',poolIds:[],problems:['unmatchedSku'],sku:''}),
    row('802','a'),row('802','b',{state:'held',poolIds:[],problems:['missingSize'],sku:'S-1'}),
    row('803','a'),row('803','b',{state:'unmatched',poolIds:[],problems:['needsMaterial'],sku:'M-1'}),
    row('804','a',{problems:['unmatchedSku'],state:'pooled'})                                   // single piece, nested, stale problems
  ];
  const i=orderIssues(gf,rs);
  assert.deepEqual(i.map(x=>[x.orderId,x.key]),[['800','unmatched'],['801','noSku'],['802','noDesign'],['803','unmatched']]);
  assert.match(i[0].pieces[0].why,/SKU not in a master/);assert.match(i[2].pieces[0].why,/No design for that size/);assert.match(i[3].pieces[0].why,/Needs material/);
  // the server's records carry the problem kinds as strings and the SKU as text; the same answer
  assert.deepEqual(Object.entries(R.orderReports(rs.map(srv),[gf])).filter(([,r])=>!r.ready).map(([o,r])=>[o,r.key]),[['800','unmatched'],['801','noSku'],['802','noDesign'],['803','unmatched']]);
  // a single piece whose line has a problem and is on no sheet is on no sheet at all: no sheet has an issue about it
  assert.equal(orderIssues(sheet('gx','gold',['905_a_1']),[row('905','a'),row('900','a',{state:'unmatched',poolIds:[],problems:['unmatchedSku'],sku:'Q'})]).length,0);
  // a nested piece wins over a stale problem list of its own line
  const nested=[row('810','a'),row('810','b',{problems:['unmatchedSku'],state:'pooled',poolIds:['810_b_1']})];
  const g=sheet('gf1','gold',['810_a_1','810_b_1']);assert.equal(orderIssues(g,nested).length,0,'both on this sheet');
  const other=sheet('ss1','silver',['810_b_1']);assert.equal(orderIssues(sheet('gf1','gold',['810_a_1']),nested,[sheet('gf1','gold',['810_a_1']),other]).length,0,'on a ready sheet: nested, the stale problem is ignored');
}

// 5. Pieces on THIS sheet never block through the order check, however many and however unready the sheet; quantity 2
{
  const bad=sheet('gf1','gold',['10_a_1','10_b_1','11_a_1','11_a_2'],{approved:false,extra:{label:{files:[]}}});
  const rs=[row('10','a'),row('10','b'),row('11','a',{quantity:2})];
  assert.equal(orderIssues(bad,rs).length,0,'two pieces on this sheet; one line of quantity 2 on this sheet');
  assert.equal(R.pieces(rs,[bad])['11'].length,2);
  // quantity 2: one copy here, the other unnested
  const half=sheet('gf1','gold',['11_a_1']);
  const q=orderIssues(half,[row('11','a',{quantity:2,poolIds:['11_a_1']})]);assert.deepEqual(q.map(x=>[x.key,x.pieceCount,x.pieces.map(p=>p.index)]),[['pooled',2,[2]]]);
  // quantity 2: one copy here, one on another sheet that is not ready
  const far=sheet('ss1','silver',['11_a_2'],{approved:false});
  const q2=orderIssues(half,[row('11','a',{quantity:2})],[half,far]);assert.deepEqual(q2.map(x=>[x.key,x.pieces.map(p=>[p.index,p.sheetLabel])]),[['otherSheetNotReady',[[2,'SS Sheet 1']]]]);
  // quantity 2 with the record's pool ids lost entirely, both on this sheet: no issue
  assert.equal(orderIssues(sheet('gf1','gold',['11_a_1','11_a_2']),[row('11','a',{quantity:2,poolIds:[]})]).length,0);
  // an order the sheet lists but holds no piece of (an old list) is not its order
  const stale=sheet('gf1','gold',['12_a_1'],{extra:{orders:['12','13']}});
  assert.equal(orderIssues(stale,[row('12','a'),row('13','a',{poolIds:['13_a_1'],state:'pooled'}),row('13','b',{state:'pooled',poolIds:[]})]).length,0);
}

// 6. Cancelled, no-design and held pieces
{
  const gf=sheet('gf1','gold',['20_a_1','21_a_1','22_a_1','23_a_1','24_a_1']);
  const rs=[
    row('20','a'),row('20','g',{state:'gone',poolIds:[],problems:['unmatchedSku']}),                       // cancelled piece: never waited for
    row('21','a'),row('21','n',{state:'noDesign',poolIds:[]}),row('21','m',{noDesign:true,state:'committed',poolIds:[],problems:['unmatchedSku']}),   // no design needed
    row('22','a'),row('22','h',{state:'held',poolIds:[],hold:'Check customer changes'}),                      // a person's hold
    row('23','a'),row('23','c',{state:'written',changePending:true}),                                         // an Etsy change waiting for review
    row('24','a'),row('24','s',{state:'skipped',poolIds:[],hold:'skipped by Paul'})
  ];
  const i=orderIssues(gf,rs);
  assert.deepEqual(i.map(x=>[x.orderId,x.key]),[['22','held'],['23','held'],['24','held']],'cancelled and no-design pieces never block; held ones read as held');
  assert.match(i[0].pieces[0].why,/Check customer changes/);
  // a held piece nested on another sheet is held for this one (and the other sheet does not blame itself for it)
  const far=sheet('ss1','silver',['25_b_1']);const near=sheet('gf1','gold',['25_a_1']);
  const hs=[row('25','a'),row('25','b',{hold:'Check customer changes'})];
  assert.deepEqual(orderIssues(near,hs,[near,far]).map(x=>[x.key,x.pieces[0].sheetLabel]),[['held','SS Sheet 1']]);
  assert.deepEqual(orderIssues(far,hs,[near,far]).map(x=>[x.key,x.pieces[0].sheetLabel]),[['held','SS Sheet 1']],'a held piece holds the sheet it sits on too: a person\'s hold or an Etsy change is not an inference');
  const solo=orderIssues(sheet('ss1','silver',['25_b_1']),[row('25','b',{hold:'Check customer changes'})]);
  assert.deepEqual(solo.map(x=>[x.key,x.pieceCount]),[['held',1]],'even a one-piece order, because it is held');
  assert.equal(orderIssues(sheet('ss1','silver',['25_b_1']),[row('25','b',{changePending:true})]).length,1);
  assert.equal(orderIssues(sheet('ss1','silver',['25_b_1']),[row('25','b')]).length,0,'a one-piece order that is not held is never an issue');
}

// 7. A piece's stored state is not where it sits: the order of 4170252963 (Nathaly Soto), one piece on each of two sheets, the SS line still 'pooled'
{
  const gf=sheet('gf1','gold',['4170252963_a_1']),ssR=sheet('ss1','silver',['4170252963_b_1']);
  const rs=[row('4170252963','a'),row('4170252963','b',{state:'pooled'})];
  assert.equal(orderIssues(gf,rs,[gf,ssR]).length,0,'both sheets ready: the order has no issue, whatever its line says');
  assert.equal(orderIssues(ssR,rs,[gf,ssR]).length,0);
  const old=R.orderReports(rs,[gf,ssR])['4170252963'];assert.equal(old.ready,true,'the whole-order reading is ready too (it used to say "line is pooled")');
}

// 8. Which kind names an order that has several different pieces holding it back, and every piece is listed
{
  const gf=sheet('gf1','gold',['30_a_1']),far=sheet('ss1','silver',['30_b_1'],{approved:false});
  const rs=[row('30','a'),row('30','b'),row('30','c',{state:'pooled',poolIds:[]}),row('30','d',{state:'unmatched',poolIds:[],problems:['unmatchedSku'],sku:''})];
  const i=orderIssues(gf,rs,[gf,far]);assert.equal(i.length,1);assert.equal(i[0].key,'pooled');assert.deepEqual(i[0].pieces.map(p=>[p.index,p.kind]),[[2,'otherSheetNotReady'],[3,'pooled'],[4,'noSku']]);assert.equal(i[0].pieceCount,4);
  assert.match(i[0].why,/3 of its other pieces wait: on SS Sheet 1, not on a sheet yet, no SKU/);
}

// 9. A set: one entry per (sheet, order), the mates that hold a ready sheet, a missing sheet
{
  const a=sheet('a','gold',['40_a_1','41_a_1'],{index:1}),b=sheet('b','silver',['40_b_1','41_b_1'],{approved:false,index:1});
  const set={setId:'set1',seq:1,name:'Set 1',sheetIds:['a','b']},rs=[row('40','a'),row('40','b'),row('41','a'),row('41','b')];
  const i=R.issues(set,{rows:rs,allSheets:[a,b]});
  assert.deepEqual(i.map(x=>[x.sheetId,x.step,x.key,x.orderId||null]),[['a','orders','otherSheetNotReady','40'],['a','orders','otherSheetNotReady','41'],['b','engraving','approvalsNeeded',null]]);
  assert.equal(new Set(i.map(x=>`${x.sheetId}|${x.step}|${x.key}|${x.orderId}`)).size,i.length,'no duplicates');
  assert(i.every(x=>x.sheetLabel));
  // a ready sheet held by a mate (its mate has its own engraving to finish)
  const ready=sheet('a','gold',['50_a_1']),mate=sheet('b','silver',['51_a_1'],{approved:false,index:1});
  const li=R.issues(ready,{rows:[row('50','a'),row('51','a')],allSheets:[ready,mate],set,sheets:[ready,mate]});
  assert.deepEqual(li.map(x=>[x.step,x.key,x.label,x.open.id]),[['laser','waitsOnSheet','SS Sheet 1','b']]);
  assert.deepEqual(R.issues(ready,{rows:[row('50','a')],allSheets:[ready],set:{...set,sheetIds:['a']},sheets:[ready]}).length,0);
  assert.deepEqual(R.issues(ready,{rows:[row('50','a')],allSheets:[ready],set,sheets:[ready]}).map(x=>[x.key,x.open.id]),[['missingSheet','b']],'a sheet of its set that cannot be found holds it');
  assert.deepEqual(R.issues(ready,{rows:[row('50','a')],allSheets:[ready],setMissing:true}).map(x=>x.key),['setMissing']);
  assert.deepEqual(R.issues({...set,sheetIds:['a','zz']},{rows:[],allSheets:[a],sheets:[a]}).filter(x=>x.key==='missingSheet').map(x=>x.open.id),['zz']);
  // a set cut already, a sheet cut already: no issues
  assert.equal(R.issues({...set,laserDoneAt:5},{sheets:[a,b]}).length,0);assert.equal(R.issues({...b,laserDoneAt:5},{}).length,0);
}

// 10. The sheet's own blocker is one short entry, in the order of the steps, never a count, and the steps option limits what is reported
{
  const mk=(f)=>{const s=sheet('x','gold',['60_a_1']);f(s);return s;};
  const k=s=>R.issues(s,{}).map(i=>[i.step,i.key]);
  assert.deepEqual(k(sheet('x','gold',['60_a_1'])),[],'a ready sheet has no issues');
  assert.deepEqual(k(mk(s=>s.verification.ok=false)),[['nesting','layout']]);
  assert.deepEqual(k(mk(s=>s.outputs={})),[['nesting','layout']]);
  assert.deepEqual(k(mk(s=>s.draft=true)),[['nesting','notInSet']]);
  assert.deepEqual(k(mk(s=>s.laserHold={at:5,by:'Paul'})),[['nesting','held']]);
  assert.deepEqual(k(mk(s=>{s.metal='rose';s.roseStockId='st';})),[['nesting','roseLine']]);
  const unapproved=s=>{s.backPool=[];s.engraving={'60_a_1':{needed:true,state:'review',approved:false}};};
  assert.deepEqual(k(mk(unapproved)),[['engraving','approvalsNeeded']],'approvals first');
  assert.deepEqual(k(mk(s=>s.backPool=[])),[['backFiles','backFilesMissing']],'approved but not saved');
  assert.deepEqual(k(mk(s=>s.backPool[0].outputs=null)),[['backFiles','backFilesMissing']]);
  assert.deepEqual(k(mk(s=>s.label.files=[])),[['qr','qrMissing']]);
  assert.deepEqual(k(mk(s=>{s.verification.ok=false;s.label.files=[];})),[['nesting','layout']],'one entry: the first step still behind');
  assert.deepEqual(R.issues(mk(s=>{s.verification.ok=false;s.label.files=[];}),{steps:['qr']}),[],'steps limits it');
  assert(R.issues(mk(unapproved),{}).every(i=>i.label.split(' ').length<=4 && !/\d/.test(i.label)));
  const again=mk(s=>{unapproved(s);s.processSeals=[{how:'laserDone',at:9}];});assert.deepEqual(k(again),[],'a sheet cut once before keeps its approval');
}

// 11. Words: pieces, never lines, in anything a person reads
{
  const gf=sheet('gf1','gold',['70_a_1','71_a_1','72_a_1','73_a_1']),far=sheet('ss1','silver',['70_b_1'],{approved:false});
  const rs=[row('70','a'),row('70','b'),row('71','a'),row('71','b',{state:'pooled',poolIds:[]}),row('72','a'),row('72','b',{state:'unmatched',poolIds:[],problems:['unmatchedSku'],sku:'Z'}),row('73','a'),row('73','b',{hold:'Check customer changes',poolIds:[]})];
  const all=[gf,far],reps=R.orderReports(rs,all),g=clone(gf);g.orderReadiness=Object.fromEntries(R.orderIds(gf).map(o=>[o,R.forSheet(reps[o],'gf1')]));
  const text=[...strings(R.issues(g,{}).map(i=>({label:i.label,why:i.why,pieces:i.pieces&&i.pieces.map(p=>p.why)}))),...strings(R.explain(g,{rows:rs}).steps.map(s=>({d:s.detail,i:s.items}))),R.explain(g,{rows:rs}).nextText];
  assert(text.length>8);
  for(const t of text)assert.doesNotMatch(t,/\blines?\b/i,t);
  // an older stored reason still reads in pieces
  const legacy=clone(gf);legacy.orderReadiness={70:{ready:false,why:"line is pooled"},71:{ready:false,why:'piece is pooled'},72:{ready:false,why:'Not every copy has a saved sheet'}};
  const w=step(R.explain(legacy,{rows:rs}),'orders').items.map(i=>i.why);
  assert.deepEqual(w,["One of its other pieces is still 'pooled'","One of its other pieces is still 'pooled'",'Not every piece of this order is on a saved sheet yet','Order readiness has not been verified'],'the fourth order was never read: it says so');
}

// 12. forSheet is idempotent and leaves older shapes alone
{
  const gf=sheet('gf1','gold',['80_a_1']),far=sheet('ss1','silver',['80_b_1'],{approved:false});
  const r=R.orderReports([row('80','a'),row('80','b')],[gf,far])['80'],one=R.forSheet(r,'gf1');
  assert.deepEqual(R.forSheet(one,'gf1'),one);assert.equal(R.forSheet(r,'ss1').ready,true);assert.equal(R.forSheet(r,'elsewhere').ready,true,'a sheet that holds none of its pieces');
  assert.deepEqual(R.forSheet({ready:false,why:'x'},'gf1'),{ready:false,why:'x'});assert.equal(R.forSheet(undefined,'gf1'),undefined);assert.deepEqual(R.forSheet({ready:true},'gf1'),{ready:true});
  assert.equal(R.sheet({...gf,orderReadiness:undefined}).stages.orders,false,'orders not read at all: not ready, as before');
  assert.equal(R.sheet({...gf,orderReadiness:{80:{ready:false,why:'Order readiness has not been verified'}}}).stages.orders,false);
}

// 12b. Orders nothing was read for (no run lines): one entry for the sheet, not one for each of its orders; an older record that carries only the line's state
{
  const none=sheet('nn-1','gold',['90_a_1','91_a_1']);
  const u=R.issues(none,{rows:[],allSheets:[none]}).filter(i=>i.step==='orders');
  assert.deepEqual(u.map(i=>[i.key,i.orderId||null,i.orderIds,i.open.type]),[['unverified',null,['90','91'],'sheet']]);assert.equal(R.sheet({...none,orderReadiness:{90:{ready:false,why:'x'},91:{ready:true}}}).stages.orders,false);
  const gf=sheet('gf-1','gold',['95_a_1','96_a_1','97_a_1']);
  const rs=[row('95','a'),{...row('95','b',{poolIds:[]}),state:'unmatched',problems:[]},row('96','a'),{...row('96','b',{poolIds:[]}),state:'held',reason:'poolAdd failed',problems:[]},row('97','a'),{...row('97','b',{poolIds:[]}),state:'oversize',problems:[]}];
  assert.deepEqual(orderIssues(gf,rs).map(i=>[i.orderId,i.key]),[['95','unmatched'],['96','held'],['97','noDesign']]);
}

// 12c. Randomised: the scenario is made from ground truth (where each piece really is, what is wrong with its line), the rows and sheets are derived from it
// with stale states, stale problem lists and lost pool ids mixed in, and what issues() says for a sheet must be what the ground truth says.
{
  let seed=20261005;const rnd=()=>{seed|=0;seed=seed+0x6D2B79F5|0;let t=Math.imul(seed^seed>>>15,1|seed);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296;},pick=a=>a[Math.floor(rnd()*a.length)];
  const PREC=['pooled','noSku','unmatched','noDesign','held','otherSheetNotReady'];
  let cases=0,withIssue=0;
  for(let n=0;n<1500;n++){
    const where=['X','Y','Z'],ready={X:rnd()<.5,Y:rnd()<.6,Z:rnd()<.6},orders=[];
    for(let o=0;o<1+Math.floor(rnd()*4);o++){
      const lines=[];
      for(let l=0;l<1+Math.floor(rnd()*3);l++){
        const kind=pick(['plain','plain','plain','plain','gone','noDesign','noSku','unmatched','noDesign2','held','change']),q=1+(rnd()<.3?1:0);
        lines.push({txn:'t'+l,q,kind,lost:rnd()<.25,staleState:rnd()<.4,copies:[...Array(q)].map(()=>({place:rnd()<.2?null:pick(where)}))});
      }
      orders.push({id:String(9000+o),lines});
    }
    const sheetPools={X:[],Y:[],Z:[]},rows=[];
    for(const o of orders)for(const l of o.lines){
      const key=`${o.id}_${l.txn}`,ids=l.copies.map((c,i)=>`${key}_${i+1}`);
      l.copies.forEach((c,i)=>{if(c.place && !['gone','noDesign'].includes(l.kind))sheetPools[c.place].push(ids[i]);});
      const defect={noSku:'noSku',unmatched:'unmatched',noDesign2:'noDesign'}[l.kind];
      const r=row(o.id,l.txn,{quantity:l.q,poolIds:l.lost?[]:ids,sku:l.kind==='noSku'?'':'SKU-'+l.txn,state:l.kind==='gone'?'gone':l.kind==='noDesign'?'noDesign':defect?(l.copies.some(c=>!c.place)?'unmatched':'pooled'):l.staleState?'pooled':'written',
        problems:defect?[{noSku:'unmatchedSku',unmatched:'unmatchedSku',noDesign:'missingSize'}[defect]]:l.staleState && l.copies.every(c=>c.place) && rnd()<.5?['unmatchedSku']:[],hold:l.kind==='held'?'Check customer changes':null,changePending:l.kind==='change'});
      if(l.kind==='noDesign')r.spec.noDesign=true;
      rows.push(r);
    }
    const all=Object.keys(sheetPools).filter(k=>sheetPools[k].length).map(k=>sheet(k+'x-1',{X:'gold',Y:'silver',Z:'rose'}[k],sheetPools[k],{approved:ready[k],index:1}));
    const X=all.find(x=>x.id==='Xx-1');if(!X)continue;
    // ground truth for sheet X
    const expected={};
    for(const o of orders){
      const live=[];
      for(const l of o.lines){if(l.kind==='gone'||l.kind==='noDesign')continue;l.copies.forEach(c=>live.push({l,c}));}
      if(!live.some(p=>p.c.place==='X'))continue;
      const keys=new Set();
      for(const {l,c} of live){
        const heldNow=l.kind==='held' || l.kind==='change';
        if(heldNow){keys.add('held');continue;}
        if(c.place==='X')continue;
        if(c.place===null){keys.add(({noSku:'noSku',unmatched:'unmatched',noDesign2:'noDesign'})[l.kind] || 'pooled');continue;}
        if(!ready[c.place])keys.add('otherSheetNotReady');
      }
      if(keys.size)expected[o.id]=PREC.find(k=>keys.has(k));
    }
    // a piece with a defect that is nested elsewhere is nested: the generator only gives a defect kind to unnested copies of that line when they have no place; the nested copies of such a line are stale
    const got=Object.fromEntries(orderIssues(X,rows,all).map(i=>[i.orderId,i.key]));
    assert.deepEqual(got,expected,'case '+n+' '+JSON.stringify({ready,orders}));
    const reps=R.orderReports(rows,all),rec=clone(X);rec.orderReadiness=Object.fromEntries(R.orderIds(X).map(o=>[o,R.forSheet(reps[o],X.id)]));
    assert.deepEqual(Object.fromEntries(R.issues(rec,{}).filter(i=>i.step==='orders').map(i=>[i.orderId,i.key])),expected,'precomputed map, case '+n);
    assert.equal(R.sheet(rec).stages.orders,Object.keys(expected).length===0,'stage, case '+n);
    cases++;if(Object.keys(expected).length)withIssue++;
  }
  assert(cases>800 && withIssue>200 && cases-withIssue>200,`a useful mix of cases: ${withIssue} of ${cases} have an issue`);
}

// 13. The real handlers: laserStatus answers each sheet with its own reading of its orders
(async()=>{
  const {start}=require('./bridge-server.cjs');
  const S='Charm_Nest_Sheets',SET='Charm_Nest_Sets',RUN='Charm_Nest_Runs';
  const server=await start({receipts:[]}),st=server.st;
  const call=async b=>{const r=await fetch(server.sorterOrigin+'/.netlify/functions/charmNestLibrary',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b)});return {status:r.status,...await r.json()};};
  try{
    const now=Date.now(),ts={toMillis:()=>now};
    const img='data:image/svg+xml,'+encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"/>');
    const lines={};
    // SS Sheet 1: 48 single-piece orders, none of their engravings approved yet. GF Sheet 1: one order of two pieces whose other piece is on SS Sheet 1
    const pools=[...Array(48)].map((_,i)=>`41700${String(i).padStart(5,'0')}_1_1`);
    pools.forEach(p=>{const o=p.split('_')[0];lines[o+'_1']={orderId:o,state:'written',quantity:1,poolIds:[p],engraveCandidate:true,engrave:{needed:true,state:'review',approved:false}};});
    const two='4170252963';
    lines[two+'_1']={orderId:two,state:'written',quantity:1,poolIds:[two+'_1_1'],engraveCandidate:false,snap:{buyer:'Nathaly Soto'}};
    lines[two+'_2']={orderId:two,state:'pooled',quantity:1,poolIds:[two+'_2_1'],engraveCandidate:true,engrave:{needed:true,state:'review',approved:false},snap:{buyer:'Nathaly Soto'}};
    const solo='4170408845';
    lines[solo+'_1']={orderId:solo,state:'written',quantity:1,poolIds:[solo+'_1_1'],engraveCandidate:false,snap:{buyer:'Emily Chambers'}};
    lines[solo+'_2']={orderId:solo,state:'unmatched',quantity:1,poolIds:[],problems:['unmatchedSku'],sku:'CUTE-TRICERATOPS',snap:{buyer:'Emily Chambers',title:'CUTE TRICERATOPS W/ HEARTS'}};
    st.put(RUN,'run1',{runId:'run1',lines});
    const mk=(id,metal,pool,extra={})=>st.put(S,id,{id,setId:'set1',setSeq:1,sheetIndex:1,runId:'run1',metal,day:'2026-10-05',status:'complete',placedCount:pool.length,charmCount:pool.length,poolIds:pool,orders:[...new Set(pool.map(p=>p.split('_')[0]))],verification:{ok:true},
      outputs:{ai:{path:id+'.ai',url:server.sorterOrigin+'/'+id+'.ai'},preview:{path:id+'.png',url:img}},label:{files:[{path:id+'-qr.png',url:img,payload:'x',orders:[...new Set(pool.map(p=>p.split('_')[0]))]}]},updatedAt:ts,createdAt:ts,...extra});
    mk('ss-1','silver',[...pools,two+'_2_1']);
    mk('gf-1','gold',[two+'_1_1',solo+'_1_1'],{sheetIndex:1});
    st.put(SET,'set1',{setId:'set1',seq:1,day:'2026-10-05',runId:'run1',sheetIds:['ss-1','gf-1'],orders:{},status:'labelled',updatedAt:ts,createdAt:ts});
    const r=await call({op:'laserStatus',sheetIds:['ss-1','gf-1']});
    assert.equal(r.status,200);
    const sS=r.sheets.find(s=>s.id==='ss-1'),sG=r.sheets.find(s=>s.id==='gf-1');
    const waiting=s=>Object.entries(s.orderReadiness).filter(([,v])=>v.ready!==true);
    assert.deepEqual(waiting(sS),[],'SS Sheet 1: not one of its 49 orders waits (its own engravings are its own steps)');
    assert.equal(R.sheet(sS).stages.orders,true);assert.equal(sS.laser.stages.orders,true);
    assert.deepEqual(waiting(sG).map(([o,v])=>[o,v.key]),[[two,'otherSheetNotReady'],[solo,'unmatched']],'GF Sheet 1: the two real multi-piece orders');
    const gi=R.issues(sG,{}).filter(i=>i.step==='orders');
    assert.deepEqual(gi.map(i=>[i.orderId,i.key,i.customer,i.pieces.map(p=>p.sheetLabel)]),[[two,'otherSheetNotReady','Nathaly Soto',['SS Sheet 1']],[solo,'unmatched','Emily Chambers',[null]]]);
    assert.equal(gi[1].pieces[0].label,'CUTE TRICERATOPS W/ HEARTS');
    // SS Sheet 1 approves its engravings: the order of Nathaly Soto is no issue any more; the unmatched SKU stays one
    for(const p of [...pools,two+'_2_1']){const o=p.split('_')[0];Object.assign(lines[o+'_'+p.split('_')[1]],{engrave:{needed:true,state:'written',approved:true}});}
    st.put(S,'ss-1',{backPool:[...pools,two+'_2_1'].map(p=>back(p,'ss-1'))});
    const r2=await call({op:'laserStatus',sheetIds:['ss-1','gf-1']}),g2=r2.sheets.find(s=>s.id==='gf-1');
    assert.deepEqual(Object.entries(g2.orderReadiness).filter(([,v])=>v.ready!==true).map(([o,v])=>[o,v.key]),[[solo,'unmatched']]);
    // a piece nested on a sheet is not "pooled" because its line says so, and a lost line record does not make a piece vanish
    lines[solo+'_2']={orderId:solo,state:'pooled',quantity:1,poolIds:[],engraveCandidate:false};
    const r3=await call({op:'laserStatus',sheetIds:['ss-1','gf-1']});assert.deepEqual(Object.entries(r3.sheets.find(s=>s.id==='gf-1').orderReadiness).filter(([,v])=>v.ready!==true).map(([o,v])=>[o,v.key]),[[solo,'pooled']]);
    // a line whose record lost its pool ids: its copy is found by the id the pool gave it, on a sheet of another set that this answer did not ask for
    st.put(S,'ss-1',{setId:'set-two',poolIds:[...pools,two+'_2_1',solo+'_2_1'],orders:[...pools.map(p=>p.split('_')[0]),two,solo]});
    st.put(SET,'set-two',{setId:'set-two',seq:2,day:'2026-10-05',runId:'run1',sheetIds:['ss-1'],orders:{},status:'labelled',updatedAt:ts,createdAt:ts});
    st.put(SET,'set1',{sheetIds:['gf-1']});
    const r4=await call({op:'laserStatus',sheetIds:['gf-1']});assert.deepEqual(r4.sheets.map(s=>s.id),['gf-1']);
    assert.deepEqual(Object.entries(r4.sheets[0].orderReadiness).filter(([,v])=>v.ready!==true).map(([o,v])=>[o,v.key,v.sheetLabel]),[[two,'otherSheetNotReady','SS Sheet 1'],[solo,'otherSheetNotReady','SS Sheet 1']],'found on SS Sheet 1 (not "pooled"); that sheet is not ready because it cannot read the engraving of a piece whose record lost its ids');
    console.log('issues-truth OK');
  }finally{server.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
