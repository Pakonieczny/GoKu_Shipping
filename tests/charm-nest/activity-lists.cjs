const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const A=require('../../charm-nest-activity.js'),{start,Timestamp}=require('./bridge-server.cjs');
const now=Date.parse('2026-09-30T23:30:00Z');
assert.deepEqual(A.bounds('today',now),['2026-09-30','2026-09-30']);
assert.deepEqual(A.bounds('7',now),['2026-09-24','2026-09-30']);
assert.deepEqual(A.bounds('14',now),['2026-09-17','2026-09-30']);
assert.equal(A.day(Date.parse('2026-10-01T03:59:59Z')),'2026-09-30');
assert.equal(A.dayStart('2026-03-09')-A.dayStart('2026-03-08'),23*3600000);
assert.equal(A.dayStart('2026-11-02')-A.dayStart('2026-11-01'),25*3600000);
const rows=[{id:'new-order',arrivedAt:now+100,approvedAt:now-1000},{id:'new-decision',arrivedAt:now-10000,approvedAt:now},{id:'unknown'}];
assert.deepEqual(rows.slice().sort(A.compare).map(x=>x.id),['new-decision','new-order','unknown']);
A.touch(rows[0],now+200);assert.equal(rows.slice().sort(A.compare)[0].id,'new-order');
const dom=new JSDOM('<input id="search" value="3701"><header></header>',{url:'https://test.invalid',runScripts:'outside-only'}),w=dom.window;
w.eval(fs.readFileSync('charm-nest-activity.js','utf8'));let changes=0;w.CNListActivity.mount(w.document.querySelector('header'),'orders',()=>changes++);
w.document.querySelector('[data-range="yesterday"]').click();assert.equal(w.CNListActivity.state('orders').range,'yesterday');
const sort=w.document.querySelector('select');sort.value='asc';sort.dispatchEvent(new w.Event('change'));assert.equal(w.CNListActivity.state('orders').direction,'asc');assert.equal(w.document.querySelector('#search').value,'3701');assert.equal(changes,2);w.close();
(async()=>{
 const srv=await start({receipts:[]}),st=srv.st;
 const call=async b=>{const r=await fetch(srv.sorterOrigin+'/.netlify/functions/charmNestLibrary',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b)});const out=await r.json();assert.equal(r.status,200,JSON.stringify(out));return out;};
 try{
  const today=A.day(Date.now()),t=A.dayStart(today)+12*3600000;
  for(let i=0;i<9;i++)st.put('Charm_Nest_Sheets','sheet-'+i,{id:'sheet-'+i,day:'2020-01-01',metal:'silver',orders:['3701'+i],laserDoneAt:t-86400000,updatedAt:Timestamp.fromMillis(t+Math.floor(i/3)*1000)});
  st.put('Charm_Nest_Sheets','older',{id:'older',day:'2026-09-30',metal:'gold',orders:['3702'],laserDoneAt:t-20*86400000,updatedAt:Timestamp.fromMillis(t-20*86400000)});
  for(const direction of ['asc','desc']){
   let cursor=null,all=[];for(let n=0;n<10;n++){const r=await call({op:'laserDoneList',sort:'activity',direction,limit:2,cursor});all.push(...r.rows);cursor=r.next;if(!cursor)break;}
   assert.equal(all.length,10,'all pages, including timestamp ties');assert.equal(new Set(all.map(x=>x.id)).size,10);
   assert.equal(all[direction==='asc'?0:9].id,'older');for(let i=1;i<all.length;i++)assert(direction==='asc'?all[i].activityAt>=all[i-1].activityAt:all[i].activityAt<=all[i-1].activityAt);
  }
  const recent=await call({op:'laserDoneList',sort:'activity',range:'today'});assert.equal(recent.rows.length,9,'date filter uses updates, not old set/completion dates');
  const hist=await call({op:'history',sort:'activity',direction:'asc',limit:2});assert.equal(hist.sets[0].sheets[0].id,'older');assert(hist.next);
  const found=await call({op:'history',sort:'activity',q:'3701',range:'today'});assert(found.sets.length);assert(found.sets.every(x=>A.matches(x,'today')));
  const old={id:'a',how:'engraveApproved',at:t-10000,by:'Paul'};
  st.put('Charm_Nest_Sheets','engrave-sheet',{id:'engrave-sheet',poolIds:['3701000_10000_1'],backPool:[],orders:['3701000']});
  const back={poolId:'3701000_10000_1',sheetId:'engrave-sheet',order:'3701000',approvedAt:t-10000,approvedBy:'Paul',engravingSeals:[old]};
  await call({op:'backPut',back});await call({op:'backInvalidate',poolIds:[back.poolId]});
  await call({op:'backPut',back:{...back,approvedAt:Date.now()+1000,approvedBy:'Seth',engravingSeals:[]}});
  const seals=st.doc('Charm_Pool_Back',back.poolId).engravingSeals;assert(seals.some(s=>s.by==='Paul'));assert(seals.some(s=>s.by==='Seth'),'a later client cannot erase prior approvals');
  console.log('PASS: Toronto calendar/DST, latest activity, compact controls preserve search, server ascending/descending pages/ties/date filters, history search, immutable engraving approvals');
 }finally{await srv.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
