// Search and Display listings are the product pages their ads lead to. Each page gets its own figures from Google's
// per-ad report (an ad counts toward a page only when that page is its one final URL), in the tree and on the group page.
// Offline: a stubbed Google Ads report, no network.
const fs=require('fs'),assert=require('assert'),path=require('path');
const {createGroupsService}=require('../../netlify/functions/googleAdsGroups');
let count=0;const check=(v,m)=>{assert(v,m);count++;};
const ADG=n=>'customers/123/adGroups/'+n,SHOP='https://britesjewelry.com',DAY='2026-09-10';
const camp={S:{id:'50',name:'Rings search',status:'ENABLED',advertisingChannelType:'SEARCH'},D:{id:'55',name:'Rings display',status:'ENABLED',advertisingChannelType:'DISPLAY'}};
const groups={S:{resourceName:ADG(5),name:'Ring pages',status:'ENABLED',primaryStatus:'ELIGIBLE'},D:{resourceName:ADG(6),name:'Ring banners',status:'ENABLED',primaryStatus:'ELIGIBLE'}};
// Each ad: its group, final URLs, whether Google still lists it, and one day's [cost, clicks, conv, value, conv by conversion date, value by conversion date].
const ads=[
  {g:'S',urls:[SHOP+'/products/ring-a?utm_source=google'],live:true,m:[10,4,1,50,0,0]},
  {g:'S',urls:[SHOP+'/products/ring-a'],live:true,m:[5,2,0,0,1,40]},
  {g:'S',urls:[SHOP+'/products/ring-b#reviews'],live:true,m:[8,3,1,30,1,30]},
  {g:'S',urls:[SHOP+'/products/ring-a',SHOP+'/products/ring-b'],live:true,m:[6,2,1,20,0,0]},
  {g:'S',urls:[SHOP+'/collections/rings'],live:true,m:[4,1,0,0,0,0]},
  {g:'S',urls:[SHOP+'/products/ring-c'],live:false,m:[2,1,0,0,0,0]},
  {g:'S',urls:[SHOP+'/products/ring-d'],live:true,m:null},
  {g:'D',urls:[SHOP+'/products/ring-b?variant=7'],live:true,m:[3,1,0,0,0,0]}];
const metrics=([cost,clicks,conv,value,convCd,valueCd],cd)=>({costMicros:String(cost*1e6),clicks:String(clicks),impressions:String(clicks*10),conversions:conv,conversionsValue:value,...(cd?{conversionsByConversionDate:convCd,conversionsValueByConversionDate:valueCd}:{})});
const gaql=async q=>{const cd=q.includes('by_conversion_date');
  if(q.includes('FROM asset_group'))return [];
  if(q.includes('FROM ad_group_ad')&&q.includes('metrics.'))return ads.filter(a=>a.m).map(a=>({adGroup:{resourceName:groups[a.g].resourceName},adGroupAd:{ad:{finalUrls:a.urls}},segments:{date:DAY},metrics:metrics(a.m,cd)}));
  if(q.includes('FROM ad_group_ad'))return ads.filter(a=>a.live).map(a=>({adGroup:{resourceName:groups[a.g].resourceName},adGroupAd:{ad:{finalUrls:a.urls}}}));
  if(q.includes('FROM ad_group ')&&q.includes('metrics.'))return Object.keys(groups).map(k=>{const t=[0,0,0,0,0,0];ads.filter(a=>a.g===k&&a.m).forEach(a=>a.m.forEach((v,i)=>t[i]+=v));return {adGroup:{resourceName:groups[k].resourceName},segments:{date:DAY},metrics:metrics(t,cd)};});
  if(q.includes('FROM ad_group '))return Object.keys(groups).map(k=>({campaign:camp[k],adGroup:groups[k]}));
  throw Error('Unexpected query '+q);};
const service=createGroupsService({CID:'123',reportContext:async()=>({budgetCurrency:'CAD',accountToday:DAY,accountTimezone:'America/Toronto'}),validatedRange:i=>({start:i.start,end:i.end}),gaql,reportRates:async()=>({currency:'USD',rate:()=>0.5}),verifiedBasis:async()=>{throw Error('not needed');}});
(async()=>{
  const index=await service.index({start:DAY,end:DAY,reportingTree:true,force:true}),S=index.groups.find(g=>g.ref===ADG(5)),D=index.groups.find(g=>g.ref===ADG(6));
  const page=u=>S.listingMetrics.find(x=>x.url===u);
  check(S.channel==='search'&&D.channel==='display'&&S.mapping.scope==='listings'&&D.mapping.scope==='listings','Search and Display groups that lead to product pages list them');
  check(page(SHOP+'/products/ring-a').metrics.spend===15&&page(SHOP+'/products/ring-b').metrics.spend===8&&S.listingMetrics.some(x=>x.url===null&&x.metrics.spend===6)&&S.listingMetrics.every(x=>x.report),'the report keys each ad by its one page, without query or fragment; an ad with several pages has none');
  if(process.env.BRITES_EDITOR_DOM_RUNTIME){
    const {JSDOM}=require(path.join(process.env.BRITES_EDITOR_DOM_RUNTIME,'jsdom'));
    const dom=new JSDOM('<main><div id="groupContext"></div><section id="v-groups"></section></main>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
    w.eval(fs.readFileSync(path.resolve(__dirname,'../../brites-groups.js'),'utf8'));
    let basis='conversion',current=index;const view=w.document.getElementById('v-groups'),clone=x=>JSON.parse(JSON.stringify(x));
    const products=[['1','Ring A','ring-a'],['2','Ring B','ring-b'],['4','Ring D','ring-d']].map(([n,title,handle])=>({id:'gid://shopify/Product/'+n,title,url:SHOP+'/products/'+handle,handle,offerIds:[],image:null,description:''}));
    const detail=()=>({ok:true,group:clone(current.groups.find(g=>g.ref===ADG(5))),products,copy:{headlines:[],descriptions:[]},splitProposals:[],images:[],keywords:[],keywordStatus:'ok',creativeRef:ADG(5),range:current.range,currency:current.currency,warnings:[],sourceVersion:1,snapshotHash:'a'.repeat(64),splitAvailable:false,splitReason:'Not needed here.'});
    w.BritesGroups.init({api:async action=>action==='adGroups'?clone(current):action==='adGroupDetail'?detail():{ok:false,error:'not needed'},dashboard:()=>({pending:[]}),basis:()=>basis,currentView:()=>'groups',go:()=>{},design:async()=>{},changed:()=>{},toast:()=>{},reload:async()=>{}});
    w.BritesGroups.show();await new Promise(r=>setTimeout(r,10));
    const groupEl=ref=>view.querySelector('.bg-group[data-bg-group$="|'+ref+'"]'),open=ref=>{const t=groupEl(ref).querySelector('[data-bg-toggle=group]');if(!groupEl(ref).classList.contains('is-open'))t.onclick();};
    const cellsOf=el=>[...el.querySelector('.bg-cells').children].map(c=>c.textContent.replace(/^(Spend|Clicks|Conv\. value|Conv\.) /,''));
    const leaves=ref=>[...groupEl(ref).querySelectorAll('.bg-leaf')].map(l=>({name:l.querySelector('b').textContent,cells:cellsOf(l),na:l.querySelectorAll('.bg-cell.is-na').length,href:(l.querySelector('a')||{}).href}));
    open(ADG(5));let rows=leaves(ADG(5));
    // 1. One row per product page, busiest first; the collection page, the removed ad's page and the two-page ad are not listings.
    check(rows.map(r=>r.name).join('|')==='ring a|ring b|ring d','each product page is listed once, busiest first, even when its ads add a query or fragment');
    check(rows[0].href===SHOP+'/products/ring-a','View listing opens the page itself, without tracking parameters');
    // 2. Its own figures by conversion date in USD: ring-a is its two ads, ring-b one, ring-d a true zero.
    check(rows[0].cells.join(' ')==='$7.50 6 1 $20.00'&&rows[1].cells.join(' ')==='$4.00 3 1 $15.00'&&rows[2].cells.join(' ')==='$0.00 0 0 $0.00','each listing row shows the figures of the ads that lead only to it: '+JSON.stringify(rows.map(r=>r.cells)));
    check(cellsOf(groupEl(ADG(5)).querySelector('.bg-group-line')).join(' ')==='$17.50 13 2 $35.00','the group row keeps every ad, including those without one product page');
    const note=groupEl(ADG(5)).querySelector('.bg-note').textContent;
    check(note==='Listing figures count ads that lead only to that page. The group total also includes its other ads.'&&!/per ad group, not per listing/.test(view.textContent),'the note says what a listing figure counts');
    open(ADG(6));check(leaves(ADG(6)).map(r=>r.name+' '+r.cells[0]).join('|')==='ring b $1.50','a Display group lists its product page with its figures too');
    // 3. The Overview's basis switch applies to listing rows as well.
    basis='click';w.BritesGroups.show();open(ADG(5));rows=leaves(ADG(5));
    check(rows[0].cells.join(' ')==='$7.50 6 1 $25.00'&&rows[1].cells.join(' ')==='$4.00 3 1 $15.00','by ad-click date the listing rows use their ad-click conversions');
    basis='conversion';
    // 4. A page whose converted figures are missing shows none, rather than a partial or unconverted number.
    current=clone(index);current.groups.find(g=>g.ref===ADG(5)).listingMetrics.find(x=>x.url===SHOP+'/products/ring-b').report=null;
    await w.BritesGroups.refresh();open(ADG(5));rows=leaves(ADG(5));
    check(rows.find(r=>r.name==='ring b').na===4&&rows.find(r=>r.name==='ring a').na===0,'a page without converted figures shows none, the others still show theirs');
    // 5. Without the per-ad report, the rows stay and say the figures are unavailable.
    current=clone(index);current.groups.find(g=>g.ref===ADG(5)).listingMetrics=null;
    await w.BritesGroups.refresh();open(ADG(5));rows=leaves(ADG(5));
    check(rows.length===3&&rows.every(r=>r.na===4)&&groupEl(ADG(5)).querySelector('.bg-note').textContent==='Listing figures are unavailable for this range.','without the per-ad report the listings stay and say their figures are unavailable');
    // 6. The group page: each product carries the figures of its pages, under labelled columns, busiest first.
    current=index;await w.BritesGroups.refresh();await w.BritesGroups.openGroup('50',ADG(5));await new Promise(r=>setTimeout(r,10));
    const cards=[...view.querySelectorAll('.bg-products.has-metrics > article')].map(a=>a.querySelector('b').textContent+' '+cellsOf(a).join(' '));
    check(cards.join('|')==='Ring A $7.50 6 1 $20.00|Ring B $4.00 3 1 $15.00|Ring D $0.00 0 0 $0.00'&&view.querySelector('.bg-products-head'),'the group page shows each product page’s own figures, busiest first');
    check(view.querySelector('.bg-note-flat').textContent==='Listing figures count ads that lead only to that page. The group total also includes its other ads.','the group page explains the listing figures the same way');
  }
  console.log('PASS '+count+' Search and Display listing page figure checks');
  require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});
