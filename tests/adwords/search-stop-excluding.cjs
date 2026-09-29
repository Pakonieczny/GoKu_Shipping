// Stop excluding: live Search campaigns that still exclude "free", "bulk" or "wish" broadly (drafted before these
// words left the defaults, or added by hand) stop buying searches such as "nickel free earrings". "Mine search
// terms", by hand and in the daily run, drafts their removal in Approvals, with the Search defaults' narrower
// phrases added in their place ("free pattern", "for free", "wish app"; none for "bulk") so no campaign ends up less
// protected. Nothing reaches Google until Paul approves; publication sends only the removals and phrases still due.
// The card says "Stop excluding X · add phrases …" per row, and its button counts removals and additions.
// Offline: a recording stand-in for Google, Firestore in memory, no paid AI.
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),{JSDOM}=require('jsdom');
const FN=path.resolve(__dirname,'../../netlify/functions'),file=path.join(FN,'googleAdsAutopilot.js'),realRequire=require('node:module').createRequire(file);
const html=fs.readFileSync(path.resolve(__dirname,'../../brites-adwords.html'),'utf8');
const clone=v=>v==null?v:JSON.parse(JSON.stringify(v)),DAY=86400000;let n=0;const check=(v,m)=>{assert.ok(v,m);n++;};

// ---------- offline stand-ins (as in pmax-structure.cjs) ----------
function engine(transport){
  const sent=[],fetch=async(url,opts={})=>{const body=opts.body&&typeof opts.body==='string'?JSON.parse(opts.body):null;sent.push({url:String(url),body});return transport(String(url),body);};
  const context=vm.createContext({module:{exports:{}},exports:{},require:x=>x==='node-fetch'?fetch:realRequire(x),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date,Intl,Map,Set,URL,URLSearchParams,setTimeout,clearTimeout});
  vm.runInContext(fs.readFileSync(file,'utf8'),context);
  return {E:context.module.exports,sent,get:x=>vm.runInContext(x,context),bind(values){context.__m=values;vm.runInContext(Object.keys(values).map(k=>k+'=__m.'+k).join('\n'),context);}};
}
function memory(){
  const docs=new Map();let seq=0;
  const read=(o,k)=>k.split('.').reduce((v,p)=>v==null?v:v[p],o);
  const snap=p=>({id:p.split('/').pop(),exists:docs.has(p),ref:doc(p),data:()=>clone(docs.get(p))});
  const doc=p=>({id:p.split('/').pop(),path:p,get:async()=>snap(p),collection:x=>collection(p+'/'+x),delete:async()=>{docs.delete(p);},
    set:async(v,o)=>{docs.set(p,o&&o.merge?{...(docs.get(p)||{}),...clone(v)}:clone(v));},
    update:async v=>{if(!docs.has(p))throw Error('No document '+p);const next=clone(docs.get(p));for(const [k,val] of Object.entries(clone(v))){const parts=k.split('.');let at=next;for(const x of parts.slice(0,-1))at=at[x]||(at[x]={});at[parts.at(-1)]=val;}docs.set(p,next);}});
  const collection=(p,filters=[],max=Infinity)=>({path:p,
    where:(f,op,v)=>collection(p,[...filters,[f,op,v]],max),limit:m=>collection(p,filters,m),orderBy(){return this;},select(){return this;},
    doc:x=>doc(p+'/'+(x==null?'auto'+(++seq):x)),add:async v=>{const id='auto'+(++seq);docs.set(p+'/'+id,clone(v));return doc(p+'/'+id);},
    get:async()=>{const list=[...docs.keys()].filter(k=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')).filter(k=>filters.every(([f,op,v])=>{const x=read(docs.get(k),f);return op==='=='?x===v:op==='in'?v.includes(x):true;})).slice(0,max).map(snap);
      return {docs:list,size:list.length,empty:!list.length,forEach:fn=>list.forEach(fn)};}});
  return {docs,db:{collection,runTransaction:async fn=>fn({get:r=>r.get(),set:(r,v,o)=>r.set(v,o),update:(r,v)=>r.update(v),delete:r=>docs.delete(r.path)})},FV:{serverTimestamp:()=>Date.now()}};
}
const R=(campaign,id)=>'customers/123/campaignCriteria/'+campaign+'~'+id,C=id=>'customers/123/campaigns/'+id;
const phrase=(campaign,text)=>({create:{campaign:C(campaign),negative:true,keyword:{text,matchType:'PHRASE'}}});
const FREE=['free pattern','free patterns','free download','free downloads','free printable','free printables','free svg','for free'],but=(list,...x)=>list.filter(t=>!x.includes(t));
// The account as Google reports it. 301 runs and 302 is paused: both are live. 303 has ended, 304 was deleted in the
// console and 101 is Performance Max: none of them is touched. 301 already excludes the phrases "free pattern" and
// "wish app"; 302 excludes "free download" broadly (that covers the phrase) and "for free" only exactly (it does not).
function account(){
  const W={queries:[],publish:false,campaigns:[
    {id:'301',name:'BA · Earrings search',channel:'SEARCH',status:'ENABLED',servingStatus:'SERVING'},
    {id:'302',name:'BA · Charms search',channel:'SEARCH',status:'PAUSED',servingStatus:'SERVING'},
    {id:'303',name:'BA · Old search',channel:'SEARCH',status:'ENABLED',servingStatus:'ENDED'},
    {id:'304',name:'BA · Deleted search',channel:'SEARCH',status:'PAUSED',servingStatus:'SERVING'},
    {id:'101',name:'BA · Necklaces PMax',channel:'PERFORMANCE_MAX',status:'ENABLED',servingStatus:'SERVING'}],
   negatives:[['301','11','free','BROAD'],['301','12','Wish','BROAD'],['301','13','free pattern','PHRASE'],['301','14','diy','BROAD'],['301','15','wish app','PHRASE'],
     ['302','21','bulk','BROAD'],['302','22','free','PHRASE'],['302','23','free','EXACT'],['302','24','free download','BROAD'],['302','25','for free','EXACT'],
     ['303','31','free','BROAD'],['304','41','free','BROAD'],['101','61','free','BROAD']]
     .map(([campaignId,id,text,matchType])=>({campaignId,resourceName:R(campaignId,id),text,matchType}))};
  W.gaql=async q=>{W.queries.push(q);
    const ids=(/campaign\.id IN \(([^)]*)\)/.exec(q)||[])[1],only=ids?ids.split(',').map(s=>s.trim()):null,camp=id=>W.campaigns.find(c=>c.id===id);
    if(/^SELECT campaign\.id, campaign\.status FROM campaign WHERE campaign\.id IN \(/.test(q))return W.campaigns.filter(c=>only.includes(c.id)).map(c=>({campaign:{id:c.id,status:c.status}}));
    if(!/FROM campaign_criterion WHERE .*campaign_criterion\.type = 'KEYWORD' AND campaign_criterion\.negative = TRUE AND campaign_criterion\.status != 'REMOVED'/.test(q))throw Error('Unexpected query '+q);
    return W.negatives.filter(x=>{const c=camp(x.campaignId);return (!only||only.includes(c.id))&&(!q.includes("campaign.advertising_channel_type = 'SEARCH'")||c.channel==='SEARCH')&&(!q.includes("campaign.status != 'REMOVED'")||c.status!=='REMOVED');})
      // Google returns only the fields a query selects.
      .map(x=>{const c=camp(x.campaignId);return {campaign:{id:c.id,...(q.includes('campaign.name')?{name:c.name}:{}),...(q.includes('campaign.status')?{status:c.status}:{}),...(q.includes('campaign.serving_status')?{servingStatus:c.servingStatus}:{})},
        campaignCriterion:{resourceName:x.resourceName,keyword:{text:x.text,matchType:x.matchType}}};});};
  return W;
}
// Google's side: validate-only requests answer empty; a real publication only where a test allows it, and then
// Google applies what it is sent.
const reply=data=>({ok:true,status:200,json:async()=>data,headers:{get:()=>null}});
const google=W=>{let next=900;return (url,body)=>{
  if(/\/customers\/123\/campaignCriteria:mutate$/.test(url)){
    if(body.validateOnly===true)return reply({});
    if(!W.publish)throw Error('A live publication was attempted in a validate-only test.');
    return reply({results:body.operations.map(o=>{if(o.remove){W.negatives=W.negatives.filter(x=>x.resourceName!==o.remove);return {resourceName:o.remove};}
      const id=o.create.campaign.split('/').pop(),resourceName=R(id,++next);W.negatives.push({campaignId:id,resourceName,text:o.create.keyword.text,matchType:o.create.keyword.matchType});return {resourceName};})});
  }
  throw Error('Unexpected request '+url);
};};
function setup(W=account()){
  const mem=memory(),e=engine(google(W)),ctrl={enabled:true,dryRun:false,maxDailyBudgetTotal:62,budgetCurrency:'CAD'};
  mem.docs.set('Brites_GAds_State/adArchive/campaigns/304',{status:'REMOVED'});
  e.bind({fb:()=>mem,gaql:W.gaql,control:async()=>ctrl,mintToken:async()=>'offline-token',_recordMutationVersions:async()=>{}});
  const approvals=()=>[...mem.docs.entries()].filter(([k])=>/^Brites_GAds_Approvals\/[^/]+$/.test(k)).map(([k,v])=>({id:k.split('/').pop(),...v}));
  return {e,E:e.E,W,mem,ctrl,approvals,approval:id=>mem.docs.get('Brites_GAds_Approvals/'+id),mutations:()=>e.sent.filter(s=>/:mutate$/.test(s.url))};
}
const removes=ops=>ops.filter(o=>o.remove).map(o=>o.remove).join(),json=v=>JSON.stringify(v);

(async()=>{
  // ===== 1. The draft =====
  const T=setup(),out=await T.E.draftSearchNegativeRemovals(),[d]=T.approvals(),p=d.payload,list=p.meta.removals,words=T.e.get('_retiredWordPhrases');
  check(json(words('free'))===json(FREE)&&json(words('wish'))==='["wish app","wish com"]'&&json(words('bulk'))==='[]','the phrases put back are the Search defaults\' for each word, none for "bulk"');
  check(out.found===4&&out.queued===1&&out.removals===4&&out.adds===15&&out.approvalId===d.id&&T.approvals().length===1,'one draft covers the live Search campaigns');
  check(d.type==='negatives'&&d.status==='PENDING'&&d.vetted===false&&d.creative===null&&d.summary==='Stop excluding “free”, “bulk”, “wish” in 2 Search campaigns · remove 4, add 15 phrases','it waits in Approvals and says what it changes');
  const expected=[{remove:R(302,22)},...but(FREE,'free download').map(t=>phrase(302,t)),{remove:R(301,11)},...but(FREE,'free pattern').map(t=>phrase(301,t)),{remove:R(302,21)},{remove:R(301,12)},phrase(301,'wish com')];
  check(p.service==='campaignCriteria'&&json(p.operations)===json(expected),'"free" (broad, and the one-word phrase that blocks the same searches), "bulk" and "wish" are removed, each followed by the phrases added in its place');
  check(list.map(r=>r.adds.length).join()==='7,7,0,1'&&!list[0].adds.includes('free download')&&list[0].adds.includes('for free')&&!list[1].adds.includes('free pattern')&&json(list[3].adds)==='["wish com"]',
    'a phrase the campaign already excludes as a phrase, or broadly, is skipped; one it excludes only exactly is still added');
  check(p.meta.kind==='searchNegativeRemoval'&&list.map(r=>[r.text,r.matchType,r.campaignName].join('|')).join()==='free|PHRASE|BA · Charms search,free|BROAD|BA · Earrings search,bulk|BROAD|BA · Charms search,wish|BROAD|BA · Earrings search'
    &&list.every(r=>r.campaign===C(r.campaignId)&&r.criterion.startsWith('customers/123/campaignCriteria/'+r.campaignId+'~')),'each removal keeps its word (as Google matches it, lower case), match, campaign and criterion');
  check(/“nickel free earrings” or “free shipping”/.test(list[0].reason)&&/bridesmaid and team gifts/.test(list[2].reason)&&/“wish bracelet”/.test(list[3].reason),'and why: the buying searches it stops');
  check(T.W.queries.length===1&&/campaign\.advertising_channel_type = 'SEARCH' AND campaign\.status != 'REMOVED'/.test(T.W.queries[0]),'only live Search campaigns are read');
  check(!/~(13|14|15|23|24|25|31|41|61)\b/.test(removes(p.operations))&&p.operations.filter(o=>o.create).every(o=>[C(301),C(302)].includes(o.create.campaign)),'exact "free", other words, an ended or deleted campaign and Performance Max are left alone');
  check(T.e.sent.length===0,'drafting sends nothing to Google and buys no AI');
  await assert.rejects(()=>T.E.applyApproval(d.id,T.ctrl),/not available for publication/);
  check(T.mutations().length===0&&T.approval(d.id).status==='PENDING','nothing reaches Google Ads before Paul approves');

  // ===== 2. Never drafted twice =====
  let again=await T.E.draftSearchNegativeRemovals();
  check(again.found===4&&again.queued===0&&T.approvals().length===1,'running again drafts nothing already waiting');
  Object.assign(T.approval(d.id),{status:'REJECTED',deletedAt:Date.now()-DAY});
  check((await T.E.draftSearchNegativeRemovals()).queued===0,'a removal Paul deleted is not proposed again within 30 days');
  T.approval(d.id).deletedAt=Date.now()-31*DAY;
  again=await T.E.draftSearchNegativeRemovals();
  check(again.queued===1&&T.approvals().length===2,'after 30 days it is proposed again');
  {const U=setup();
   U.mem.docs.set('Brites_GAds_Approvals/restore',{type:'adVersionRestore',status:'PENDING',payload:{mutateOperations:[{campaignCriterionOperation:{remove:R(301,12)}}]}});
   U.mem.docs.set('Brites_GAds_Approvals/other',{type:'negatives',status:'APPROVED',payload:{service:'campaignCriteria',operations:[{remove:R(302,22)}]}});
   U.mem.docs.set('Brites_GAds_Approvals/archived',{type:'negatives',status:'REJECTED',archivedAt:Date.now(),rejectionReason:'Campaign deleted',payload:{service:'campaignCriteria',operations:[{remove:R(302,21)}],meta:{kind:'searchNegativeRemoval'}}});
   await U.E.draftSearchNegativeRemovals();const u=U.approvals().find(a=>!['restore','other','archived'].includes(a.id));
   check(removes(u.payload.operations)===[R(301,11),R(302,21)].join(),'an exclusion another waiting draft already removes is left out; one whose draft was archived with a deleted campaign is drafted again');}
  {const V=setup();V.W.negatives=V.W.negatives.filter(x=>!['free','Wish','bulk'].includes(x.text)||x.matchType==='EXACT');
   check(json(await V.E.draftSearchNegativeRemovals())==='{"found":0,"queued":0}'&&!V.approvals().length,'with nothing to stop excluding, no draft is made');}

  // ===== 3. Publication: only what is still due =====
  const P=setup(),id=(await P.E.draftSearchNegativeRemovals()).approvalId,ap=()=>P.approval(id),approve=()=>{ap().status='APPROVED';};
  approve();let ms=P.mutations().length;
  check((await P.E.applyApproval(id,{...P.ctrl,dryRun:true})).status==='VALIDATED','the draft validates in dry run');
  let mm=P.mutations().slice(ms);
  check(mm.length===1&&/\/customers\/123\/campaignCriteria:mutate$/.test(mm[0].url)&&mm[0].body.validateOnly===true&&json(mm[0].body.operations)===json(ap().payload.operations),'dry run sends exactly the reviewed removals and phrases in one request, validate-only');
  const [cq,nq]=P.W.queries.slice(-2);
  check(/^SELECT campaign\.id, campaign\.status FROM campaign WHERE campaign\.id IN \(302, 301\)$/.test(cq)&&/FROM campaign_criterion WHERE campaign\.id IN \(302, 301\)/.test(nq),'publication reads the draft\'s campaigns and their exclusions again');
  // Changed since: the "wish" exclusion removed by hand (its phrase still goes), "bulk" changed, "free svg" added as a
  // phrase to 302 and "free patterns" broadly to 301 (both left out), "free printable" added to 301 only exactly (still added).
  P.W.negatives=P.W.negatives.filter(x=>x.resourceName!==R(301,12));P.W.negatives.find(x=>x.resourceName===R(302,21)).text='bulk order';
  P.W.negatives.push({campaignId:'302',resourceName:R(302,26),text:'free svg',matchType:'PHRASE'},{campaignId:'301',resourceName:R(301,16),text:'free patterns',matchType:'BROAD'},{campaignId:'301',resourceName:R(301,17),text:'free printable',matchType:'EXACT'});
  const due=[{remove:R(302,22)},...but(FREE,'free download','free svg').map(t=>phrase(302,t)),{remove:R(301,11)},...but(FREE,'free pattern','free patterns').map(t=>phrase(301,t)),phrase(301,'wish com')];
  approve();ms=P.mutations().length;await P.E.applyApproval(id,{...P.ctrl,dryRun:true});mm=P.mutations().slice(ms);
  check(mm.length===1&&json(mm[0].body.operations)===json(due),'a removal already done or changed since, and a phrase the campaign has since, are left out; the rest goes as reviewed');
  P.W.campaigns.find(c=>c.id==='302').status='REMOVED';approve();ms=P.mutations().length;await P.E.applyApproval(id,{...P.ctrl,dryRun:true});mm=P.mutations().slice(ms);
  check(mm.length===1&&json(mm[0].body.operations)===json(due.filter(o=>!(o.remove||o.create.campaign).includes('/302'))),'nothing is sent for a campaign removed in Google Ads since');P.W.campaigns.find(c=>c.id==='302').status='PAUSED';
  // Stored drafts are compared by content: Firestore may return an operation's keys in another order.
  {const saved=clone(ap().payload.operations);ap().payload.operations=saved.map(o=>o.create?{create:{keyword:{matchType:'PHRASE',text:o.create.keyword.text},negative:true,campaign:o.create.campaign}}:o);approve();ms=P.mutations().length;
   await P.E.applyApproval(id,{...P.ctrl,dryRun:true});check(json(P.mutations().slice(ms)[0].body.operations)===json(due),'the same reviewed draft publishes whatever order its stored keys come back in');ap().payload.operations=saved;}
  // A draft changed after review sends nothing.
  const tamper=async(what,edit)=>{const saved=clone(ap().payload);edit(ap().payload);approve();ms=P.mutations().length;
    await assert.rejects(()=>P.E.applyApproval(id,{...P.ctrl,dryRun:true}),/This draft contains an unexpected change\. Nothing was changed\./);check(P.mutations().length===ms,'a draft changed after review sends nothing: '+what);ap().payload=saved;};
  await tamper('an extra phrase',pl=>pl.operations.push(phrase(301,'earrings')));
  await tamper('a phrase made broad',pl=>{pl.operations[1].create.keyword.matchType='BROAD';});
  await tamper('an extra removal',pl=>pl.operations.push({remove:R(301,14)}));
  await tamper('a phrase that is not the word\'s',pl=>{pl.meta.removals[3].adds.push('earrings');pl.operations.push(phrase(301,'earrings'));});
  await tamper('phrases for another campaign',pl=>{pl.meta.removals[3].campaign=C(999);pl.operations=pl.operations.map(o=>o.create&&o.create.keyword.text==='wish com'?phrase(999,'wish com'):o);});
  await tamper('a word that is not retired',pl=>{pl.meta.removals[2].text='diy';});
  // Live: Google applies it, the draft is published, and the next check finds nothing to draft.
  P.W.publish=true;approve();ms=P.mutations().length;
  check((await P.E.applyApproval(id,P.ctrl)).status==='APPLIED'&&ap().status==='APPLIED','Paul\'s approval publishes the draft');
  mm=P.mutations().slice(ms);
  check(mm.length===1&&mm[0].body.validateOnly!==true&&json(mm[0].body.operations)===json(due),'Google is sent only the removals and phrases still due, in one request');
  check(['free pattern','for free','free downloads'].every(t=>P.W.negatives.some(x=>x.campaignId==='302'&&x.text===t&&x.matchType==='PHRASE'))&&P.W.negatives.some(x=>x.campaignId==='301'&&x.text==='wish com'&&x.matchType==='PHRASE')
    &&!P.W.negatives.some(x=>['free','wish','bulk'].includes(x.text.toLowerCase())&&x.matchType!=='EXACT'&&['301','302'].includes(x.campaignId)),'afterwards the campaigns exclude the phrases, and no longer the lone words');
  check(json(await P.E.draftSearchNegativeRemovals())==='{"found":0,"queued":0}','once published, nothing is left to draft');
  approve();ms=P.mutations().length;
  await assert.rejects(()=>P.E.applyApproval(id,{...P.ctrl,dryRun:true}),/Google Ads already has every change in this draft\. Nothing was changed\. Delete this draft\./);
  check(P.mutations().length===ms&&ap().status==='APPROVED'&&/already has every change/.test(ap().lastError),'when nothing is left to do, nothing is sent and the card says why');

  // ===== 4. The card =====
  const A={esc:v=>String(v==null?'':v).replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch])),DASH:{lastMetrics:[{id:'301',name:'BA · Earrings search'},{id:'302',name:'BA · Charms search'}]}};vm.createContext(A);
  const fn=name=>{const m=html.match(new RegExp('^function '+name+'\\([\\s\\S]*?\\n(?=function |var |//)','m'));assert.ok(m,'the page defines '+name);return m[0];};
  for(const f of ['apCampaignName','apMatch','apFacts','apStopExcluding','approvalAction','approvalTitle'])vm.runInContext(fn(f),A);
  const page=new JSDOM('<body></body>').window.document,view=h=>{const div=page.createElement('div');div.innerHTML=h;return div;};
  const facts=h=>{const o={};view(h).querySelectorAll('dl.apFacts dt').forEach(dt=>{o[dt.textContent]=dt.nextElementSibling.textContent;});return o;};
  const rows=h=>[...view(h).querySelectorAll('.apTable tbody tr')].map(r=>[...r.cells].map(c=>c.textContent).join(' / '));
  const draft={type:'negatives',payload:clone(d.payload)};let act=A.approvalAction(draft,false);
  check(A.approvalTitle(draft)==='Stop excluding “free”, “bulk”, “wish”'&&act.tag==='Exclusions'&&act.from==='Search campaigns'&&act.to==='Google Ads · exclusions removed, phrases added','the card says "Stop excluding" and which words');
  check(act.publish==='Remove 4, add 15 exclusions'&&A.approvalAction(draft,true).publish==='Validate draft','its button counts the removals and the additions (validated first in a dry run)');
  let card=A.apStopExcluding(draft.payload),F=facts(card.body);
  check(json(card.glance)==='["2 Search campaigns","Remove 4 · add 15"]'&&F.Removes==='4 exclusions · your Search ads can show again for searches with these words'
    &&F.Adds==='15 phrase exclusions · searches such as “free pattern” and “wish com” stay excluded'&&F['Stays the same']==='Every other exclusion, keyword, ad and budget','it says what is removed, what is added in its place and what stays');
  check(rows(card.body).join('\n')===[
    'Stop excluding free phrase · add phrases '+but(FREE,'free download').join(', ')+' / BA · Charms search / Blocks buyers searching “nickel free earrings” or “free shipping”',
    'Stop excluding free broad · add phrases '+but(FREE,'free pattern').join(', ')+' / BA · Earrings search / Blocks buyers searching “nickel free earrings” or “free shipping”',
    'Stop excluding bulk broad / BA · Charms search / Blocks group orders such as bridesmaid and team gifts',
    'Stop excluding wish broad · add phrase wish com / BA · Earrings search / Blocks buyers searching “wish bracelet”'].join('\n'),'one line per word: "Stop excluding X · add phrases …", with its campaign and why');
  const only=i=>{const r={...clone(d.payload.meta.removals[i]),campaignName:''};return {type:'negatives',payload:{service:'campaignCriteria',operations:[{remove:r.criterion},...r.adds.map(t=>phrase(r.campaignId,t))],meta:{kind:'searchNegativeRemoval',removals:[r]}}};};
  card=A.apStopExcluding(only(3).payload);F=facts(card.body);
  check(json(card.glance)==='["BA · Earrings search","Remove 1 · add 1"]'&&F.Removes==='1 exclusion · your Search ads can show again for searches with this word'&&F.Adds==='1 phrase exclusion · searches such as “wish com” stay excluded'
    &&A.approvalAction(only(3),false).publish==='Remove 1, add 1 exclusions'&&A.approvalTitle(only(3))==='Stop excluding “wish”','one word reads in the singular, naming its campaign from the live list');
  card=A.apStopExcluding(only(2).payload);
  check(!('Adds' in facts(card.body))&&json(card.glance)==='["BA · Charms search","Remove 1"]'&&A.approvalAction(only(2),false).publish==='Remove 1 exclusion'&&rows(card.body)[0].startsWith('Stop excluding bulk broad / '),'"bulk" alone adds nothing and says so by omission');
  const crit=ops=>({type:'negatives',payload:{service:'campaignCriteria',operations:ops}}),add={create:{}},rm={remove:R(301,11)};
  check(A.approvalAction(crit([rm,rm]),false).publish==='Remove 2 exclusions'&&A.approvalAction(crit([add,rm]),false).publish==='Add 1, remove 1 exclusions'&&A.approvalAction(crit([add,add,add]),false).publish==='Add 3 exclusions'
    &&A.approvalAction({payload:{service:'adGroupCriteria',operations:[add]}},false).publish==='Add 1 keyword','any exclusions or keywords draft counts removals as removals');
  const render=html.slice(html.indexOf('function renderApprovals('),html.indexOf('\n}',html.indexOf('function renderApprovals('))),at=s=>{const i=render.indexOf(s);assert.ok(i>=0,'renderApprovals has '+s);return i;};
  check(at('(pl.meta||{}).kind==="searchNegativeRemoval"){var sx=apStopExcluding(pl);glance=sx.glance;body+=sx.body;}')<at('else if(pl.service==="adGroupCriteria"||pl.service==="campaignCriteria")'),'the draft gets its own card before the generic exclusions card');

  // ===== 5. Wiring: "Mine search terms" by hand and in the daily run =====
  const stub=x=>x==='./_editPasscode'?{sameSecret:(a,b)=>typeof a==='string'&&a===b,envPasscode:()=>null,resolve:async()=>({value:null})}:null;
  const worked=async fake=>{const mod={exports:{}},env={GADS_REFRESH_TOKEN:'r',GADS_CLIENT_SECRET:'s',GADS_DEVELOPER_TOKEN:'d'};
    const wx={process:{env},console,Date,Set,JSON,Math,module:mod,exports:mod.exports,setTimeout,clearTimeout,require:x=>x==='node-fetch'?async()=>({ok:true,status:202}):x==='./googleAdsAutopilot'?fake:stub(x)||require(x)};
    vm.createContext(wx);vm.runInContext(fs.readFileSync(path.join(FN,'googleAdsAutopilot-background.js'),'utf8'),wx);
    const token='internal-'+require('crypto').createHmac('sha256','r|s|d').update('brites-gads-background-worker/v1').digest('hex');
    return JSON.parse((await mod.exports.handler({httpMethod:'POST',headers:{},body:JSON.stringify({tasks:['mine'],token})})).body);};
  const order=[],fake={control:async()=>({enabled:true}),mineSearchTerms:async()=>{order.push('search');return {queued:0};},minePmaxSearchTerms:async()=>{order.push('pmax');throw Error('quota');},
    draftSearchNegativeRemovals:async(...a)=>{order.push('stop:'+a.length);return {found:4,queued:1};}};
  let w=await worked(fake);
  check(w.status==='ran'&&order.join()==='search,pmax,stop:0'&&w.result.stopExcluding.queued===1&&/quota/.test(w.result.minePmax.error),'the mining task drafts it after the Search and Performance Max terms, even when the Performance Max step fails');
  fake.draftSearchNegativeRemovals=async()=>{throw Error('read failed');};w=await worked(fake);
  check(/read failed/.test(w.result.stopExcluding.error)&&w.result.mine&&!w.log.some(l=>/mine ERROR/.test(l)),'a failed check is reported without failing the mining run');
  const kick=fs.readFileSync(path.join(FN,'googleAdsAutopilotKick.js'),'utf8');
  check(/\["measure", "mine", "prune", "events", "designStudioLearn"\]\.forEach\(t => tasks\.add\(t\)\); ranDaily = true;/.test(kick)&&/onclick="runNow\(\['mine','prune'\],this\)"[^>]*removal of Search exclusions that stop buyers[^>]*>Mine search terms</.test(html),'it runs daily and from the "Mine search terms" button, whose tip says so');
  console.log('PASS '+n+' Search "Stop excluding" checks: the draft with its replacement phrases, its publication and its card');
  require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});
