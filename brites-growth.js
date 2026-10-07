(function(global){'use strict';
  const node=(tag,value,cls)=>{const n=document.createElement(tag);if(value!=null)n.textContent=String(value);if(cls)n.className=cls;return n;};
  function safeLink(raw){try{const u=new URL(raw);return u.protocol==='https:'&&!u.username&&!u.password?u.href:null;}catch{return null;}}
  function productId(value){const raw=String(value||'').trim();return /^gid:\/\/shopify\/Product\/\d+$/.test(raw)?raw:/^\d+$/.test(raw)?'gid://shopify/Product/'+raw:null;}
  function selectDossier(result,product){const id=productId(product.productId);return (result?.dossiers||[]).find(d=>productId(d.productId)===id&&id&&(!product.handle||d.handle===product.handle)&&['approved','draft'].includes(d.status))||null;}
  function sourceLinks(dossier,ids){return (ids||[]).map(id=>(dossier.sources||[]).find(s=>s.id===id)).filter(s=>s&&safeLink(s.url));}
  function selectProductIssues(result,product){const id=productId(product.productId),records=(result.productIssues||[]).filter(record=>id&&productId(record.productId)===id);return {productId:id,issues:records.flatMap(record=>Array.isArray(record.issues)?record.issues:[])};}
  function issueHolds(record){const open=(record?.issues||[]).filter(issue=>issue&&issue.status!=='resolved'),blocks=new Set(open.flatMap(issue=>Array.isArray(issue.blocks)?issue.blocks:[]));if(open.some(issue=>['identity','style','options','material','matching'].includes(issue.kind))){blocks.add('recommendation');blocks.add('cart');}if(open.some(issue=>['history','content'].includes(issue.kind)))blocks.add('meaning');return {recommendationHold:blocks.has('recommendation'),cartHold:blocks.has('cart'),meaningHold:blocks.has('meaning')};}
  function shopperKnowledgeDiagnosticsFor(product,reads,at=Date.now()){
    const id=productId(product?.productId),handle=typeof product?.handle==='string'&&/^[a-z0-9_-]{1,180}$/.test(product.handle)?product.handle:null;
    if(!id||!handle)throw Error('Choose one exact catalogue product.');
    const read=name=>reads?.[name]?.status==='fulfilled'?reads[name].value:null;
    const short=(value,limit)=>typeof value==='string'?value.replace(/\s+/g,' ').trim().slice(0,limit):null;
    const version=value=>typeof value==='string'&&/^[a-f0-9]{64}$/i.test(value)?value:null;
    const timestamp=value=>Number.isFinite(value)&&value>0?value:null;
    const sources=value=>(Array.isArray(value)?value:[]).slice(0,40).map(source=>({id:short(source?.id,100),reviewed:source?.reviewed===true,checkedAt:timestamp(source?.checkedAt),ageState:!timestamp(source?.checkedAt)?'unavailable':source.checkedAt>at+60000?'future':at-source.checkedAt>30*86400000?'stale':'fresh'}));
    const research=read('research'),current=(research?.dossiers||[]).find(dossier=>dossier?.productId===id&&dossier.handle===handle),liveRead=read('product'),live=liveRead?.live===true&&liveRead.product?.id===id&&liveRead.product.handle===handle?liveRead.product:null;
    const issueState=research?.productIssueState||(Array.isArray(research?.productIssues)?'available':'unavailable'),holds=issueHolds(selectProductIssues(research||{},product));
    for(const name of ['recommendationHold','cartHold','meaningHold'])holds[name]=holds[name]||live?.[name]===true||current?.evidenceHolds?.[name]===true;
    const stored=read('supplements'),storedRecords=Array.isArray(stored?.supplements)?stored.supplements:[],storedBound=!!stored&&Array.isArray(stored.supplements)&&storedRecords.every(value=>value?.productId===id&&value.handle===handle),supplements=storedRecords.filter(value=>value?.productId===id&&value.handle===handle);
    const supplemental=supplements.slice(0,1).map(value=>({productId:id,handle,status:short(value.status,30),version:version(value.version),baseDossierVersion:version(value.baseDossierVersion),currentBaseVersionMatches:!!current&&value.baseDossierVersion===current.version,savedAt:timestamp(value.savedAt),productCheckedAt:timestamp(value.productCheckedAt),meaningCount:Array.isArray(value.meanings)?value.meanings.length:0,sourceCount:Array.isArray(value.sources)?value.sources.length:0,sources:sources(value.sources)}));
    const internal=/\b(?:system|developer|author|hidden|internal)\s+(?:prompt|message|instructions?)\b|\b(?:assistant|model|concierge)\s+(?:must|should|shall|needs? to)\b/i;
    const competitorHosts=(current?.competitors||[]).flatMap(value=>{const url=safeLink(value?.url);return url?[new URL(url).hostname.replace(/^www\./,'')]:[];});
    const neutral=value=>{const url=safeLink(value);if(!url)return null;const host=new URL(url).hostname.replace(/^www\./,'');if(host==='britesjewelry.com')return url;if(competitorHosts.some(other=>host===other||host.endsWith('.'+other)||other.endsWith('.'+host))||/(?:^|\.)(?:etsy|amazon|ebay|walmart)\./i.test(host)||/(?:^|[.-])(?:retailer|shop|store|boutique|marketplace|jewelry|jewellery|gifts)(?:[.-]|$)/i.test(host))return null;return url.length<=1200?url:null;};
    const projected=(read('knowledge')?.products||[]).filter(value=>value?.productId===id),meanings=[];
    for(const meaning of projected.slice(0,2)){
      const text=short(meaning.text,1500),context=short(meaning.context,300),citations=Array.isArray(meaning.sources)?meaning.sources:[];
      if(meaning.kind!=='interpretation'||!text||!context||internal.test(text+' '+context)||!citations.length||citations.some(source=>!neutral(source?.url)))continue;
      meanings.push({text,context,kind:'interpretation',sources:citations.slice(0,4).map(source=>({title:short(source.title,200),url:neutral(source.url),checkedAt:timestamp(source.checkedAt)}))});
    }
    const keywordRecommendations=(current?.recommendations||[]).flatMap((value,index)=>value?.channel==='keywords'&&value.basis==='hypothesis'?[{recommendationIndex:index,sourceIds:(value.sourceIds||[]).slice(0,12).map(id=>short(id,100)),keywordCount:Array.isArray(value.keywords)?value.keywords.length:0}]:[]).slice(0,8);
    return {readOnly:true,authoringWrites:false,approvals:false,checkedAt:at,selection:{productId:id,handle,queueDossierVersion:version(product.dossierVersion)},reads:Object.fromEntries(['research','product','supplements','knowledge'].map(name=>[name,reads?.[name]?.status==='fulfilled'?'available':'unavailable'])),product:{identityVerified:!!live,live:!!live,productId:live?id:null,handle:live?handle:null,checkedAt:timestamp(live?.checkedAt)},issues:{state:issueState,...holds},dossier:current?{productId:id,handle,status:short(current.status,30),version:version(current.version),queueVersionMatches:current.version===product.dossierVersion,savedAt:timestamp(current.savedAt),sourceCount:Array.isArray(current.sources)?current.sources.length:0,sources:sources(current.sources),meaningCount:Array.isArray(current.meanings)?current.meanings.length:0,meaningSourceIds:(current.meanings||[]).slice(0,12).map(value=>(value.sourceIds||[]).slice(0,12).map(id=>short(id,100))),keywordRecommendations}:null,supplements:{readState:!stored?'unavailable':storedBound?'available':'identity_unverified',absent:storedBound?supplements.length===0:null,count:supplements.length,records:supplemental},publicKnowledge:{readState:read('knowledge')?'available':'unavailable',meaningCount:projected.length,shownMeaningCount:meanings.length,rejectedOrOmittedCount:projected.length-meanings.length,meanings},limits:'One selected product; at most one supplement, 40 source summaries, eight keyword recommendation bindings and two public interpretations. Empty public knowledge alone does not establish a missing dossier or supplement.'};
  }
  function keywordRevisionFor(value,product,dossier){
    const fields=['productId','baseDossierVersion','recommendationIndex','sourceIds','keywords','reviewed'];
    const exact=value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===fields.length&&fields.every(key=>Object.hasOwn(value,key));
    const id=productId(product?.productId),index=value?.recommendationIndex,rec=Number.isInteger(index)&&index>=0?dossier?.recommendations?.[index]:null;
    if(!exact||!id||dossier?.status!=='approved'||dossier.productId!==id||dossier.handle!==product.handle||product.dossierVersion!==dossier.version||value.productId!==id||value.baseDossierVersion!==dossier.version||!(/^[a-f0-9]{64}$/i.test(dossier.version||''))||value.reviewed!==true||rec?.channel!=='keywords'||rec.basis!=='hypothesis')throw Error('Revision does not match the selected approved research.');
    const sourceIds=value.sourceIds,keywords=value.keywords,key=term=>String(term).normalize('NFKC').replace(/\s+/g,' ').trim().toLowerCase();
    if(!Array.isArray(sourceIds)||!sourceIds.length||sourceIds.length>12||new Set(sourceIds).size!==sourceIds.length||sourceIds.some(id=>typeof id!=='string'||!/^[a-zA-Z0-9:_-]{1,100}$/.test(id))||!Array.isArray(rec.sourceIds)||JSON.stringify([...sourceIds].sort())!==JSON.stringify([...rec.sourceIds].sort())||!Array.isArray(keywords)||!keywords.length||keywords.length>8||keywords.some(term=>typeof term!=='string'||!term.trim()||term.length>120)||new Set(keywords.map(key)).size!==keywords.length)throw Error('Revision terms or existing source bindings are invalid.');
    return {productId:id,baseDossierVersion:dossier.version,recommendationIndex:index,sourceIds:[...sourceIds],keywords:keywords.map(term=>term.trim()),reviewed:true};
  }
  function briefFor(dossier,title,issues=null,issueState='available'){return [title,'CORRECTIVE RESEARCH ONLY \xb7 Context notes. A fresh, exact-version operator-review packet is required before using any advertising proposals.','Product: '+dossier.handle,'Research status: '+dossier.status,'Research version: '+(dossier.version||'unavailable'),...((Object.values(issueHolds(issues)).some(Boolean)||issueState!=='available')?['Product holds or issue evidence require review.']:[]),...(issues?.issues||[]).filter(issue=>issue.status!=='resolved').map(issue=>'Open product issue \xb7 '+issue.kind+': '+issue.detail),'Research recommendations (hypotheses; context only):',...(dossier.recommendations||[]).map(r=>r.channel+': '+r.action+'\nMeasure: '+r.measure+'\nSources: '+sourceLinks(dossier,r.sourceIds).map(s=>s.url).join(', '))].join('\n\n');}
  function demandBriefFor(dossier,title,entry,issues=null,issueState='available',now=Date.now(),reviewContext=null){
    const e=entry?.evidence,id=productId(dossier?.productId);
    if(!id||dossier.status!=='approved'||!dossier.version||entry?.state!=='current'||productId(entry.productId)!==id||e?.productId!==id||e.handle!==dossier.handle||e.dossierVersion!==dossier.version||e.schemaVersion!==1||e.evidenceRevision!==3||e.evidenceKind!=='product_demand_and_ad_outcomes'||e.readOnly!==true||e.sandboxReadOnly!==true||!Number.isFinite(e.at)||e.at>now+60000||now-e.at>86400000)return null;
    const demandHeld=entry.promotionAllowed!==true||entry.productIssueState!=='available'||['recommendationHold','cartHold','meaningHold'].some(k=>entry.productHolds?.[k]===true)||Object.values(issueHolds(issues)).some(Boolean)||issueState!=='available';
    const research=reviewContext?operatorBriefFor(reviewContext.result,reviewContext.product,dossier,title,now,demandHeld):briefFor(dossier,title,issues,demandHeld?'unavailable':issueState),held=demandHeld||research.includes('CORRECTIVE RESEARCH ONLY');
    const metric=value=>typeof value==='number'&&Number.isFinite(value)&&value>=0?String(value):'unavailable',currency=/^[A-Z]{3}$/.test(e.currency||'')?e.currency:'currency unavailable',lines=[research,'MEASURED DEMAND \xb7 '+e.range+' \xb7 checked '+new Date(e.at).toISOString(),'Source: authenticated read-only Google Ads reports and Google Keyword Planner; exact product and research version.','Tracking has not been validated by this read. Reported conversions do not establish sales, revenue, profit or ROAS.'];
    if(held)lines.push('Promotion remains held; these measurements are for correction research.');
    if(e.sources?.shopping?.state==='available'){
      const t=e.shopping?.totals||{};lines.push('Shopping: exact verified product/variant offer IDs. Impressions '+metric(t.impressions)+'; clicks '+metric(t.clicks)+'; reported spend '+metric(t.cost)+' '+currency+'; reported conversions (unvalidated) '+metric(t.reportedConversions)+'.');
      if(e.sources.shopping.truncated)lines.push('Shopping reached its report bound; complete totals are unavailable.');
    }else lines.push('Shopping outcomes unavailable; missing reports are not zero outcomes.');
    const planner=e.planner||{},market=planner.market||{};
    if(planner.state==='available'){
      lines.push('Keyword Planner '+(market.pooled===true?'POOLED ':'')+'observed country targets: '+(market.countries||[]).map(c=>c.code||c.id).join(', ')+'. Location exclusions are not reproduced in these estimates.');
      if(market.pooled===true)lines.push('Volumes are pooled. Do not infer country-specific demand or performance.');
      if(market.languageBasis==='research_hypothesis_from_verified_own_storefront'){
        lines.push((market.languageName||'Storefront-supported language')+' is a research-language hypothesis; actual campaign language targeting is unknown.');
        const source=safeLink(market.languageSource?.finalUrl||market.languageSource?.sourceUrl);if(source)lines.push('Storefront language evidence: '+source);
      }else lines.push('Observed campaign language constant: '+(market.languageId||'unavailable')+'.');
      const bidCurrency=/^[A-Z]{3}$/.test(planner.bidCurrency||'')?planner.bidCurrency:'currency unavailable';
      for(const k of planner.keywords||[])lines.push('Keyword hypothesis: '+k.text+'; '+(market.pooled===true?'pooled ':'')+'average monthly searches '+metric(k.averageMonthlySearches)+'; competition '+(k.competition||'unavailable')+'; estimated top-of-page bids '+metric(k.lowTopOfPageBid)+'\u2013'+metric(k.highTopOfPageBid)+' '+bidCurrency+'.');
      lines.push('Planner volumes and bid estimates are not actual competitor spend or predicted returns.');
    }else lines.push('Keyword Planner '+(planner.state||'unavailable')+': '+(planner.reason||'No measured market evidence.')+'.');
    lines.push('Search terms use currently aligned ad groups; historical product attribution is unverified. PMax terms are campaign context only. Missing terms or values do not establish zero demand.');
    if(e.payloadBounds?.state==='detail_sampled')lines.push('Saved detail is sampled; source coverage and totals were checked before sampling.');
    return lines.join('\n\n');
  }
  function renderDemand(parent,entry){
    if(!entry?.evidence){parent.appendChild(node('p','No saved demand check for this exact product yet.','sub'));return;}
    const evidence=entry.evidence,metric=value=>value==null||!Number.isFinite(Number(value))?'unavailable':String(Number(value)),shopping=evidence.shopping||{},t=shopping.totals||{};
    parent.append(node('span',entry.state==='current'?'Current saved demand evidence':'Stale saved evidence \xb7 review before using','tag '+(entry.state==='current'?'approved':'draft')),node('p','Checked '+new Date(evidence.at).toLocaleString()+' \xb7 '+(evidence.range||'Reporting period unavailable'),'status'));
    if(evidence.payloadBounds?.state==='detail_sampled'){const labels={shoppingRows:'Shopping rows',searchTerms:'Search terms',pmaxTerms:'PMax terms',targetCriteria:'targeting criteria',adDestinationUrls:'destination URL examples',plannerSearchMatches:'Planner/Search matches',plannerCampaignMatches:'Planner/campaign matches'},omitted=Object.entries(evidence.payloadBounds.omittedDetails||{}).filter(([,n])=>Number.isFinite(Number(n))&&Number(n)>0).map(([name,n])=>Number(n)+' '+(labels[name]||'detail examples'));parent.appendChild(node('p','Saved detail samples omit '+omitted.join(', ')+'. Provider coverage and Shopping totals were checked before sampling.','status'));}
    parent.appendChild(node('p','Shopping evidence: exact product and catalogue variant offers. Reported conversions are unvalidated; their values do not establish revenue or profit.','sub'));
    if(evidence.sources?.shopping?.state==='unavailable')parent.appendChild(node('p','Shopping outcomes are unavailable: '+(evidence.sources.shopping.error||'The report could not be checked.'),'status'));
    else{parent.append(node('p','Impressions: '+metric(t.impressions)+' \xb7 Clicks: '+metric(t.clicks)+' \xb7 Reported conversions (unvalidated): '+metric(t.reportedConversions)),node('p','Reported spend: '+metric(t.cost)+(evidence.currency?' '+evidence.currency:' \xb7 account currency unavailable'),'sub'));if(evidence.sources?.shopping?.truncated)parent.appendChild(node('p','The Shopping report reached its row bound. Complete totals remain unavailable.','status'));}
    parent.appendChild(node('p','Search terms: '+(evidence.search?.terms?.length||0)+' reported rows in currently aligned product ad groups. Historical product attribution remains unverified.','sub'));
    if(evidence.sources?.searchTerms?.state==='unavailable')parent.appendChild(node('p','Search-term outcomes are unavailable.','status'));
    (evidence.search?.terms||[]).slice(0,6).forEach(term=>parent.appendChild(node('p',term.text+' \xb7 clicks '+metric(term.clicks)+' \xb7 reported conversions '+metric(term.reportedConversions),'sub')));
    parent.appendChild(node('p','PMax terms: campaign context only; these terms do not establish an individual product sale.','sub'));
    const planner=evidence.planner||{};parent.appendChild(node('h4','Keyword Planner market evidence'));
    if(planner.state!=='available')parent.appendChild(node('p','Planner '+(planner.state||'unavailable')+': '+(planner.reason||'A saved market check has not completed.'),'status'));
    else{const codes=(planner.market?.countries||[]).map(country=>country.code||country.id),pooled=planner.market?.pooled===true,hypothesis=planner.market?.languageBasis==='research_hypothesis_from_verified_own_storefront';parent.appendChild(node('p',(pooled?'Pooled observed country targets: ':'Observed country targets: ')+codes.join(', ')+'. Market estimates do not reproduce location exclusions.'+(pooled?' Volumes are pooled; country-specific demand or performance is not inferred.':''),'sub'));
      if(hypothesis){parent.appendChild(node('p',(planner.market.languageName||'The storefront language')+' is a research-language hypothesis supported by the own storefront HTML language tag. Actual campaign language targeting remains unknown.','status'));const source=safeLink(planner.market.languageSource?.finalUrl);if(source){const link=node('a','Storefront language source');link.href=source;link.target='_blank';link.rel='noopener';parent.appendChild(link);}}
      else parent.appendChild(node('p','Observed campaign language constant '+planner.market?.languageId+'.','sub'));
      (planner.keywords||[]).forEach(keyword=>parent.appendChild(node('p',keyword.text+' \xb7 '+(pooled?'pooled ':'')+'average monthly searches '+metric(keyword.averageMonthlySearches)+' \xb7 competition '+(keyword.competition||'unavailable'),'sub')));}
    parent.appendChild(node('p','Unreported terms or missing Planner values do not establish zero demand.','status'));
  }
  const reviewTermKey=value=>String(value||'').normalize('NFKC').replace(/\s+/g,' ').trim().toLowerCase();
  const reviewTermTokens=value=>String(value||'').normalize('NFKC').toLowerCase().match(/[\p{L}\p{N}]+/gu)||[];
  function containsReviewPhrase(haystack,needle){if(!haystack.length||!needle.length||needle.length>haystack.length)return false;outer:for(let offset=0;offset<=haystack.length-needle.length;offset++){for(let index=0;index<needle.length;index++)if(haystack[offset+index]!==needle[index])continue outer;return true;}return false;}
  function suppressiveReviewConflict(positive,negative){const left=reviewTermTokens(positive),right=reviewTermTokens(negative);return left.length>0&&right.length>0&&(containsReviewPhrase(left,right)||containsReviewPhrase(right,left));}
  function operatorReviewFor(result,product,dossier,now=Date.now()){
    const id=productId(product?.productId),version=dossier?.version,entry=(result?.operatorReviews||[]).find(value=>value.productId===id),packet=entry?.operatorReviewPacket;
    const issues=selectProductIssues(result||{},product||{}),holds=issueHolds(issues);
    if(!id||!product.handle||dossier?.productId!==id||dossier.handle!==product.handle||dossier.status!=='approved'||product.dossierVersion!==version||dossier.proposalOnly===true||dossier.privateProposal===true||dossier.reviewStatus==='proposed'||!(/^[a-f0-9]{64}$/i.test(version||''))||!Number.isFinite(dossier.savedAt)||dossier.savedAt>now+60000||result.productIssueState!=='available'||holds.recommendationHold||holds.meaningHold||holds.cartHold||entry?.state!=='pending_operator_review'||entry.productId!==id||entry.handle!==product.handle||entry.dossierVersion!==version||packet?.schema!==1||packet.productId!==id||packet.handle!==product.handle||packet.dossierVersion!==version||packet.state!=='pending_operator_review'||['providerWrites','campaignWrites','budgetWrites','automaticActivation'].some(key=>packet[key]!==false))return null;
    if(!Array.isArray(dossier.sources)||!dossier.sources.length||dossier.sources.length>40)return null;
    const sources=new Map();for(const source of dossier.sources){if(!source||!/^[a-zA-Z0-9:_-]{1,100}$/.test(source.id||'')||sources.has(source.id)||source.reviewed!==true||!safeLink(source.url)||!Number.isFinite(source.checkedAt)||source.checkedAt>now+60000||now-source.checkedAt>30*86400000)return null;sources.set(source.id,source);}
    if(result.operatorReviewBindingRequired===true){
      if(packet.sourceBindingSchema!==1||packet.sourceVersionBound!==true||!/^[a-f0-9]{64}$/i.test(packet.packetVersion||'')||!Array.isArray(packet.sourceBindings)||!packet.sourceBindings.length)return null;
      const citedIds=new Set();for(const candidate of packet.candidates||[])for(const sourceId of candidate?.sourceIds||[])citedIds.add(sourceId);for(const group of [...(packet.positiveKeywords||[]),...(packet.negativeKeywords||[])])for(const sourceId of group?.sourceIds||[])citedIds.add(sourceId);
      if(citedIds.size!==packet.sourceBindings.length)return null;
      for(let index=0;index<packet.sourceBindings.length;index++){
        const binding=packet.sourceBindings[index],source=sources.get(binding?.id);if(!source||index&&packet.sourceBindings[index-1].id>=binding.id||!citedIds.has(binding.id)||!/^[a-f0-9]{64}$/i.test(binding.sourceVersion||'')||binding.title!==source.title||binding.url!==safeLink(source.url)||binding.excerpt!==String(source.excerpt||'').replace(/\s+/g,' ').trim()||binding.checkedAt!==source.checkedAt||binding.reviewed!==true)return null;
      }
    }
    const text=(value,max)=>typeof value==='string'&&value.trim().length>0&&value.length<=max;
    const unique=values=>Array.isArray(values)&&new Set(values).size===values.length;
    const cited=ids=>unique(ids)&&ids.length>0&&ids.length<=12&&ids.every(id=>sources.has(id));
    const terms=values=>values==null?[]:Array.isArray(values)&&values.length<=30&&values.every(value=>text(value,120))&&new Set(values.map(reviewTermKey)).size===values.length?values:null;
    if(!unique(packet.candidateIds)||!Array.isArray(packet.candidates)||packet.candidates.length<1||packet.candidates.length>40||packet.candidateIds.length!==packet.candidates.length)return null;
    const candidateMap=new Map();for(const candidate of packet.candidates){
      if(!candidate||!/^rec_[a-f0-9]{24}$/.test(candidate.candidateId||'')||candidateMap.has(candidate.candidateId)||!packet.candidateIds.includes(candidate.candidateId)||!['ads','keywords','negatives'].includes(candidate.channel)||candidate.basis!=='hypothesis'||candidate.reviewState!=='pending_operator_review'||!text(candidate.action,2000)||!text(candidate.measure,1000)||!cited(candidate.sourceIds)||!terms(candidate.keywords)||!terms(candidate.negativeKeywords))return null;
      const rec=(dossier.recommendations||[]).find(rec=>rec.channel===candidate.channel&&rec.basis==='hypothesis'&&rec.action===candidate.action&&rec.measure===candidate.measure&&JSON.stringify(rec.sourceIds)===JSON.stringify(candidate.sourceIds)&&JSON.stringify(rec.keywords||[])===JSON.stringify(candidate.keywords||[])&&JSON.stringify(rec.negativeKeywords||[])===JSON.stringify(candidate.negativeKeywords||[]));if(!rec)return null;
      candidateMap.set(candidate.candidateId,candidate);
    }
    const eligible=(dossier.recommendations||[]).filter(value=>['ads','keywords','negatives'].includes(value.channel));
    const signature=value=>JSON.stringify([value.channel,value.action,value.measure,value.sourceIds,value.keywords||[],value.negativeKeywords||[]]);
    if(eligible.length!==packet.candidates.length||new Set(packet.candidates.map(signature)).size!==packet.candidates.length)return null;
    const validateGroup=(entries,field)=>{
      if(!Array.isArray(entries)||entries.length>1200)return false;const expected=new Map();
      for(const c of packet.candidates)for(const term of c[field]||[]){const key=reviewTermKey(term),group=expected.get(key)||{ids:new Set(),sources:new Set()};group.ids.add(c.candidateId);c.sourceIds.forEach(id=>group.sources.add(id));expected.set(key,group);}
      const seen=new Set();for(const value of entries){const key=reviewTermKey(value?.term),group=expected.get(key);if(!text(value?.term,120)||seen.has(key)||!group||value.basis!=='hypothesis'||value.reviewState!=='pending_operator_review'||!unique(value.candidateIds)||!unique(value.sourceIds)||value.candidateIds.length!==group.ids.size||value.sourceIds.length!==group.sources.size||value.candidateIds.some(id=>!group.ids.has(id))||value.sourceIds.some(id=>!group.sources.has(id)))return false;seen.add(key);}return seen.size===expected.size;
    };
    if(!validateGroup(packet.positiveKeywords,'keywords')||!validateGroup(packet.negativeKeywords,'negativeKeywords'))return null;
    if(packet.positiveKeywords.some(positive=>packet.negativeKeywords.some(negative=>suppressiveReviewConflict(positive.term,negative.term))))return null;
    return packet;
  }
  function operatorBriefFor(result,product,dossier,title,now=Date.now(),forceCorrective=false){
    const issues=selectProductIssues(result||{},product||{}),issueState=result?.productIssueState||'unavailable';
    // Exports always require source bindings, including older compatibility responses.
    const packet=forceCorrective?null:operatorReviewFor({...result,operatorReviewBindingRequired:true},product,dossier,now);
    if(!packet)return briefFor(dossier,title,issues,forceCorrective?'unavailable':issueState);
    const lines=[title,'Product advertising test brief \xb7 pending operator review \xb7 hypotheses','Product: '+packet.handle+' \xb7 '+packet.productId,'Approved research version: '+packet.dossierVersion,'Source-bound packet version: '+packet.packetVersion,'Candidate IDs: '+packet.candidateIds.join(', '),'Review only. No provider, campaign or budget writes. No automatic activation. These hypotheses do not establish measured sales lift, profit or returns.','Advertising test candidates:'];
    for(const candidate of packet.candidates)lines.push(candidate.candidateId+' \xb7 '+candidate.channel+' \xb7 hypothesis\nAction: '+candidate.action+'\nMeasure: '+candidate.measure+'\nSource IDs: '+candidate.sourceIds.join(', '));
    for(const [label,entries]of [['Positive keyword hypotheses',packet.positiveKeywords],['Negative keyword hypotheses',packet.negativeKeywords]]){
      lines.push(label+':');
      if(!entries.length)lines.push('No structured proposals in this group.');
      for(const entry of entries)lines.push(entry.term+'\nCandidates: '+entry.candidateIds.join(', ')+'\nSource IDs: '+entry.sourceIds.join(', '));
    }
    lines.push('Keyword match types and campaign scope require separate operator review; none are selected by this brief.','Reviewed source bindings:');
    for(const source of packet.sourceBindings)lines.push(source.id+' \xb7 '+source.title+'\nURL: '+source.url+'\nReviewed: '+new Date(source.checkedAt).toISOString()+'\nSource version: '+source.sourceVersion+'\nInspected excerpt: '+source.excerpt);
    return lines.join('\n\n');
  }
  function renderOperatorReview(parent,result,product,dossier){
    const section=node('section',null,'operator-review');section.setAttribute('aria-label','Advertising operator review');section.appendChild(node('h3','Advertising operator review'));
    const packet=operatorReviewFor(result,product,dossier);
    if(!packet){section.appendChild(node('p','Review packet held or unavailable. Current product identity, approved research version, fresh reviewed sources and issue checks must all agree. Research notes below remain context only.','status'));parent.appendChild(section);return;}
    section.append(node('span','Pending operator review \xb7 hypotheses','tag'),node('p','Product: '+packet.handle,'sub'),node('p','Approved research version','status'),node('code',packet.dossierVersion,'review-version'));
    if(packet.packetVersion)section.append(node('p','Source-bound packet version','status'),node('code',packet.packetVersion,'review-version'));
    section.append(node('p','No provider, campaign or budget writes. No automatic activation. These proposals do not establish measured sales lift or returns.','status'));
    const citations=(container,ids)=>{const p=node('p',null,'status');for(const [index,id]of ids.entries()){const source=dossier.sources.find(source=>source.id===id);if(index)p.append(' \xb7 ');const a=node('a',source.title||id);a.href=safeLink(source.url);a.target='_blank';a.rel='noopener noreferrer';p.appendChild(a);}container.appendChild(p);};
    for(const c of packet.candidates){const card=node('article',null,'recommendation');card.append(node('b',c.channel+' \xb7 test hypothesis'),node('code',c.candidateId,'review-id'),node('p',c.action),node('p','Measure: '+c.measure,'sub'));citations(card,c.sourceIds);section.appendChild(card);}
    for(const [heading,entries]of [['Positive keyword hypotheses',packet.positiveKeywords],['Negative keyword hypotheses',packet.negativeKeywords]]){const group=node('div',null,'review-keywords');group.appendChild(node('h4',heading));if(!entries.length)group.appendChild(node('p','No proposals in this group.','sub'));for(const value of entries){const item=node('article',null,'review-term');item.append(node('strong',value.term),node('p','Candidates: '+value.candidateIds.join(', '),'review-id'));citations(item,value.sourceIds);group.appendChild(item);}section.appendChild(group);}parent.appendChild(section);
  }
  function parseCsv(text){const rows=[];let row=[],cell='',q=false;for(let i=0;i<text.length;i++){const c=text[i];if(c==='"'){if(q&&text[i+1]==='"'){cell+='"';i++;}else q=!q;}else if(c===','&&!q){row.push(cell);cell='';}else if((c==='\n'||c==='\r')&&!q){if(c==='\r'&&text[i+1]==='\n')i++;row.push(cell);if(row.some(Boolean))rows.push(row);row=[];cell='';}else cell+=c;}row.push(cell);if(row.some(Boolean))rows.push(row);if(!rows.length)return [];const headers=rows.shift().map(h=>h.replace(/^\uFEFF/,'').trim());return rows.map(r=>Object.fromEntries(headers.map((h,i)=>[h,r[i]||''])));}
  function receiptPreviewFor(result){
    if(result?.readOnly!==true||result.receiptOnly!==true||result.dryRun!==true||result.queueUpdated!==false||result.individualOrdersUpdated!==0||result.individualOrderAttributionConfirmed!==false||result.providerAggregateUsedForConfirmation!==false||result.productionApplyAvailable!==false)throw Error('A read-only status repair preview could not be verified.');
    const count=value=>Number.isSafeInteger(value)&&value>=0?value:null;
    const fields=['scannedRows','selectedReceipts','providerReceiptsConfirmed','proposedRepairs','blockedRows'];
    return Object.fromEntries(fields.map(name=>[name,count(result[name])]));
  }
  async function mount(root,opts={}){
    const workspace=node('main',null,'workspace');root.replaceChildren(workspace);let data=null,filter='',selected=null,detailRequest=0,correctionPanel=null;
    const header=node('header'),intro=node('div');intro.append(node('span','Shared product knowledge','eyebrow'),node('h1','Research that reaches the buyer'),node('p','Product evidence, competitor offers and recommendations for ads, buyer searches and the gift concierge.','sub'));
    const controls=node('div',null,'controls'),refresh=node('button','Refresh progress','btn'),preview=node('a','Open gift concierge','btn');refresh.type='button';preview.href=opts.conciergeUrl||'/concierge-sandbox.html';preview.target='_blank';preview.rel='noopener';controls.append(refresh,preview);header.append(intro,controls);workspace.appendChild(header);
    let key='';if(!opts.request){try{key=sessionStorage.getItem('brites-growth-key')||'';}catch(e){}}
    async function request(op,payload){
      if(opts.request)return opts.request(op,payload);
      let url='/api/growth/'+op,body=payload;
      if(op.startsWith('research?')){
        const params=new URL(op,'https://sandbox.invalid/').searchParams,raw=params.get('ids'),ids=raw?raw.split(',').map(productId):[];
        if(payload!==undefined||params.getAll('ids').length!==1||[...params.keys()].some(name=>name!=='ids')||!ids.length||ids.length>20||ids.some(id=>!id))throw Error('Choose at most 20 exact catalogue products.');
        // The same protected bridge supplies both dossier and its current,
        // source-bound review packet to standalone and embedded workspaces.
        url='/api/growth-ads';body={action:'growthResearchDossiers',productIds:[...new Set(ids)]};
      }
      const r=await fetch(url,{method:body?'POST':'GET',credentials:'same-origin',headers:{'Content-Type':'application/json','X-Growth-Key':key},...(body?{body:JSON.stringify(body)}:{})});const v=await r.json();if(!r.ok)throw Object.assign(Error(v.error||'Request failed.'),{status:r.status});return v;
    }
    async function readSavedReceipts(action){
      if(opts.request||!['receiptDiagnostics','receiptReconciliationPreview'].includes(action))throw Error('A private receipt reader is required.');
      const response=await fetch('/api/growth-ads',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-Growth-Key':key},body:JSON.stringify({action,limit:20,maxMs:12000})});
      const value=await response.json();if(!response.ok)throw Error('The private receipt check is unavailable.');return value;
    }
    const receiptReader=typeof opts.receiptReader==='function'?opts.receiptReader:!opts.request?()=>readSavedReceipts('receiptDiagnostics'):null;
    const receiptPreviewReader=typeof opts.receiptPreviewReader==='function'?opts.receiptPreviewReader:!opts.request?()=>readSavedReceipts('receiptReconciliationPreview'):null;
    async function correctionRequest(body,{signal}={}){if(typeof opts.correctionRequest==='function')return opts.correctionRequest(body,{signal});if(opts.request)throw Error('An owner-authenticated correction reader is required.');const r=await fetch('/api/growth-corrections',{method:'POST',signal,headers:{'Content-Type':'application/json','X-Growth-Key':key},body:JSON.stringify(body)}),v=await r.json();if(!r.ok)throw Object.assign(Error(v.error||'Correction review is unavailable.'),{status:r.status});return v;}
    function clearCorrection(){correctionPanel?.destroy?.();correctionPanel=null;}
    const auth=node('form',null,'box auth'),pass=node('input');pass.type='password';pass.autocomplete='off';pass.placeholder='Operator access key';pass.setAttribute('aria-label','Operator access key');const sign=node('button','Open workspace','btn primary');sign.type='submit';auth.append(node('h2','Private research workspace'),node('p','Use the sandbox operator key to see saved research and sales evidence.','sub'),pass,sign);auth.hidden=!!opts.request;workspace.appendChild(auth);
    const status=node('div','','status');status.setAttribute('role','status');status.setAttribute('aria-live','polite');workspace.appendChild(status);const content=node('div');workspace.appendChild(content);
    // Private operator diagnostics reuse the existing sign-in inside this
    // closure. Credentials and controller tokens never enter report text.
    const operations=node('section',null,'box');operations.hidden=true;
    let repairLease=null,repairCheckpoint=null,repairOwner=null;
    if(!opts.request||opts.operatorTools===true){
      const check=node('button','Check voice service','btn'),coordination=node('button','Read repair checkpoint','btn'),claim=node('button','Start sandbox repair','btn'),renew=node('button','Renew repair lease','btn'),enableVoice=node('button','Enable sandbox voice','btn'),allowVoice=node('button','Allow one voice test','btn'),save=node('button','Save repair checkpoint','btn'),release=node('button','Finish sandbox repair','btn');
      const tools=node('div',null,'controls'),report=node('pre'),note=node('textarea');note.setAttribute('aria-label','Repair checkpoint note');note.maxLength=4000;report.setAttribute('aria-label','Private repair diagnostics');
      for(const button of [check,coordination,claim,renew,enableVoice,allowVoice,save,release])button.type='button';
      tools.append(check,coordination,claim,renew,enableVoice,allowVoice,save,release);operations.append(node('h2','Sandbox voice and repair checks'),node('p','Enable sandbox voice records one fixed $5 cumulative reservation allowance for explicit native voice starts until the sandbox stop. This is reserved capacity, not measured provider spend. Existing holds stay unchanged; enabling does not start a microphone or provider call. The older one-test action remains for prior repair records.'),tools,note,report);workspace.appendChild(operations);
      const buttons=[check,coordination,claim,renew,enableVoice,allowVoice,save,release];
      const redact=value=>Array.isArray(value)?value.map(redact):value&&typeof value==='object'?Object.fromEntries(Object.entries(value).filter(([name])=>!/token|secret|passcode|credential|privateKey|apiKey|adminKey/i.test(name)).map(([name,v])=>[name,redact(v)])):value;
      const show=value=>{report.textContent=JSON.stringify(redact(value),null,2);};
      const run=async task=>{buttons.forEach(button=>button.disabled=true);try{await task();}catch{show({error:'The repair result could not be confirmed. Reread voice service before retrying.'});}finally{buttons.forEach(button=>button.disabled=false);}};
      async function voiceRead(action,payload={}){const body={action,...payload};if(opts.voiceRequest)return opts.voiceRequest(body);const response=await fetch('/api/concierge-voice',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-Growth-Key':key},body:JSON.stringify(body)});const value=await response.json();return {status:response.status,...value};}
      async function readCoordination(){const [current,read]=await Promise.all([request('controller',{action:'read'}),request('checkpoint-read',{})]);repairCheckpoint=read.checkpoint;return {controller:current.lease,checkpoint:repairCheckpoint};}
      check.onclick=()=>run(async()=>{const [allocation,provider]=await Promise.all([voiceRead('allocation'),voiceRead('readiness')]);show({allocation,provider});});
      coordination.onclick=()=>run(async()=>show(await readCoordination()));
      claim.onclick=()=>run(async()=>{const current=await readCoordination();if(current.controller?.active){show({...current,busy:true});return;}const legacy=current.checkpoint?.lease;if(legacy&&(Number(legacy.leaseUntil||legacy.expiresAt)||0)>Date.now()||current.checkpoint?.activeWriter&&Number(current.checkpoint.leaseUntil)>Date.now()){show({...current,busy:true,legacyLeasePreserved:true});return;}repairOwner=repairOwner||'interactive-concierge-'+global.crypto.randomUUID();const result=await request('controller',{action:'claim',owner:repairOwner,leaseMinutes:45});if(result.ok&&typeof result.token==='string')repairLease={owner:repairOwner,token:result.token};show(result);});
      renew.onclick=()=>run(async()=>{if(!repairLease){show({error:'Start a sandbox repair before renewing its lease.'});return;}show(await request('controller',{action:'renew',...repairLease,leaseMinutes:45}));});
      enableVoice.onclick=()=>run(async()=>{if(!repairLease){show({error:'Start an owned sandbox repair before enabling voice.'});return;}const current=await readCoordination();if(current.controller?.owner!==repairLease.owner||!current.controller.active){show({error:'The repair lease is unavailable. Reread coordination before enabling voice.'});return;}show(await voiceRead('authorize-voice',{...repairLease,expectedUpdatedAt:Number(repairCheckpoint?.updatedAt||0)}));});
      allowVoice.onclick=()=>run(async()=>{if(!repairLease){show({error:'Start an owned sandbox repair before allowing a voice test.'});return;}const current=await readCoordination();if(current.controller?.owner!==repairLease.owner||!current.controller.active){show({error:'The repair lease is unavailable. Reread coordination before allowing voice.'});return;}show(await voiceRead('authorize-test',{...repairLease,expectedUpdatedAt:Number(repairCheckpoint?.updatedAt||0)}));});
      save.onclick=()=>run(async()=>{if(!repairLease||!note.value.trim()){show({error:'A repair lease and checkpoint note are required.'});return;}const current=await readCoordination();if(current.controller?.owner!==repairLease.owner||!current.controller.active){show({error:'The repair lease is unavailable. Reread coordination before saving.'});return;}const result=await request('checkpoint',{...repairLease,expectedUpdatedAt:Number(repairCheckpoint?.updatedAt||0),value:{...(repairCheckpoint||{}),manualConciergeRepair:{at:Date.now(),note:note.value.trim().slice(0,4000)}}});show(result);});
      release.onclick=()=>run(async()=>{if(!repairLease){show({error:'No repair lease is held by this workspace.'});return;}const result=await request('controller',{action:'release',...repairLease});if(result.ok){repairLease=null;repairOwner=null;}show(result);});
    }
    if(receiptReader){
      const check=node('button','Check saved conversion receipts','btn');check.type='button';controls.appendChild(check);
      const report=node('section',null,'box');report.hidden=true;report.setAttribute('aria-label','Read-only receipt diagnostics');workspace.appendChild(report);
      check.onclick=async()=>{check.disabled=true;report.hidden=false;report.replaceChildren(node('h2','Saved receipt diagnostics'),node('p','Checking existing request receipts\u2026','status'));
        try{const result=await receiptReader();if(result?.readOnly!==true||result.receiptOnly!==true||result.queueUpdated!==false||result.individualOrdersUpdated!==0)throw Error('A read-only receipt result could not be verified.');
          const count=value=>Number.isSafeInteger(value)&&value>=0?String(value):'unavailable';
          report.replaceChildren(node('h2','Saved receipt diagnostics'),node('p','Provider request evidence only. Order records remain unchanged; this does not verify attribution, deduplication or bidding goals.','sub'));
          for(const [label,field]of [['Saved receipt rows','savedReceiptRows'],['Unique receipts','uniqueReceipts'],['Provider receipts confirmed','confirmed'],['Rejected receipts','rejected'],['Still processing','processing'],['Unconfirmed receipts','unconfirmed'],['Unavailable receipt checks','unavailable']])report.appendChild(node('p',label+': '+count(result[field])));
          if(result.blocked||result.stopped)report.appendChild(node('p','Observation '+(result.blocked?'blocked':'bounded')+': '+(result.code||result.stopped||'unavailable')+'.','status'));
          for(const receipt of result.receipts||[])report.appendChild(node('p','Receipt '+receipt.receiptKey+' \xb7 '+receipt.outcome+' \xb7 '+receipt.code,'sub'));
        }catch{report.replaceChildren(node('h2','Saved receipt diagnostics'),node('p','A read-only receipt result could not be verified. No conversion or order records were changed.','error'));}finally{check.disabled=false;}
      };
    }
    if(receiptPreviewReader){
      const check=node('button','Preview receipt status repairs','btn');check.type='button';controls.appendChild(check);
      const report=node('section',null,'box');report.hidden=true;report.setAttribute('aria-label','Read-only receipt repair preview');workspace.appendChild(report);
      check.onclick=async()=>{check.disabled=true;report.hidden=false;report.replaceChildren(node('h2','Receipt status repair preview'),node('p','Checking fresh receipts and saved-row consistency\u2026','status'));
        try{const result=await receiptPreviewReader(),summary=receiptPreviewFor(result);
          report.replaceChildren(node('h2','Receipt status repair preview'),node('p','Preview only. No conversion or order records were changed. Provider receipt confirmation does not verify purchase attribution, deduplication or bidding goals.','sub'));
          for(const [label,field]of [['Saved rows inspected','scannedRows'],['Unique receipts checked','selectedReceipts'],['Provider receipts confirmed','providerReceiptsConfirmed'],['Proposed status repairs','proposedRepairs'],['Rows requiring further evidence','blockedRows']])report.appendChild(node('p',label+': '+(summary[field]??'unavailable')));
          report.appendChild(node('p','Fresh receipts and unchanged rows must be checked again before any status repair is applied.','status'));
        }catch(error){report.replaceChildren(node('h2','Receipt status repair preview'),node('p','A safe status repair preview is unavailable. No records were changed.','error'));}finally{check.disabled=false;}
      };
    }
    const detail=node('article',null,'box dossier'),rows=node('div',null,'rows');detail.setAttribute('aria-label','Selected product research');
    function links(parent,dossier,ids){const sources=sourceLinks(dossier,ids);if(!sources.length)return;const p=node('p',null,'status');sources.forEach((s,i)=>{if(i)p.append(' \xb7 ');const a=node('a',s.title||s.id);a.href=safeLink(s.url);a.target='_blank';a.rel='noopener noreferrer';p.appendChild(a);});parent.appendChild(p);}
    function renderRows(){rows.replaceChildren();(data?.queue||[]).filter(r=>r.rank<=100&&(!filter||[r.title,r.handle,r.sku,r.theme].join(' ').toLowerCase().includes(filter))).forEach(r=>{const b=node('button',null,'row'+(r.id===selected?' active':''));b.type='button';b.setAttribute('aria-pressed',String(r.id===selected));b.append(node('b',r.rank+'. '+r.title),node('small',r.handle||'Live product match pending'),node('span',String(r.status||'pending').replace(/_/g,' '),'tag '+(r.status||'pending')));b.onclick=()=>show(r);rows.appendChild(b);});if(!rows.childElementCount)rows.appendChild(node('p',filter?'No products match this search.':'No ranked products are loaded yet.','empty'));}
    async function show(r){
      clearCorrection();selected=r.id;const seq=++detailRequest;renderRows();detail.replaceChildren(node('span',r.theme||'Product research','eyebrow'),node('h2',r.title));
      if(!productId(r.productId)){detail.appendChild(node('p','An exact live product and variant match is still required. Similar products remain candidates.','empty'));return;}
      const pending=node('p','Reading saved product research\u2026','status');detail.appendChild(pending);
      try{
        const result=await request('research?ids='+encodeURIComponent(productId(r.productId)));if(seq!==detailRequest||selected!==r.id)return;pending.remove();const dossier=selectDossier(result,r),issues=selectProductIssues(result,r),holds=issueHolds(issues),issueState=result.productIssueState||(Array.isArray(result.productIssues)?'available':'unavailable'),held=Object.values(holds).some(Boolean)||issueState!=='available';
        const openIssues=issues.issues.filter(issue=>issue.status!=='resolved');
        if(openIssues.length){detail.append(node('h3','Known product issues'),node('p',holds.recommendationHold?'Ad recommendation and cart hold \xb7 correction research remains available.':holds.meaningHold?'Meaning/history hold \xb7 review the sourced content before using it.':'Product review required.','error'));openIssues.forEach(issue=>{const box=node('div',null,'recommendation');box.append(node('b',issue.kind+' \xb7 open'),node('p',issue.detail));(issue.evidence||[]).forEach(e=>{if(e.text)box.appendChild(node('p',e.text,'sub'));const url=safeLink(e.url);if(url){const a=node('a','Reviewed issue source');a.href=url;a.target='_blank';a.rel='noopener noreferrer';box.appendChild(a);}});detail.appendChild(box);});}
        if(issueState!=='available')detail.appendChild(node('p','Product issue checks are unavailable. Research stays visible; its notes remain held out of automatic ad promotion.','status'));
        if(!dossier){detail.appendChild(node('p','Matched to the live catalogue. Approved research for this exact product is not available yet; deep competitor and buyer research is queued.','empty'));return;}
        detail.append(node('span',dossier.status==='approved'?'Approved research':'Partial research \xb7 needs more evidence','tag '+dossier.status),node('p','Last saved '+new Date(dossier.savedAt).toLocaleString(),'status'));
        if(dossier.status!=='approved')detail.appendChild(node('p','This dossier is still being completed. Its recommendations are held out of automated ad-design evidence until validation approves it.','status'));
        if(dossier.status==='approved'&&(!opts.request||opts.operatorTools===true)&&/^[a-z0-9_-]{1,180}$/.test(r.handle||'')){
          const knowledgeBox=node('section',null,'recommendation'),readKnowledge=node('button','Read shopper knowledge diagnostics','btn'),knowledgeState=node('p','Read-only check of this exact approved product, stored story supplement and current public projection. No authoring or approval action.','status'),knowledgeReport=node('pre');
          readKnowledge.type='button';knowledgeReport.setAttribute('aria-label','Private shopper knowledge diagnostics');knowledgeReport.style.cssText='white-space:pre-wrap;overflow-wrap:anywhere;max-height:560px;overflow:auto';
          knowledgeBox.append(readKnowledge,knowledgeState,knowledgeReport);detail.appendChild(knowledgeBox);
          readKnowledge.onclick=async()=>{
            readKnowledge.disabled=true;knowledgeState.textContent='Reading the selected product and current shopper projection\u2026';knowledgeReport.textContent='';
            try{
              const names=['research','product','supplements','knowledge'],ids=encodeURIComponent(productId(r.productId));
              const responses=await Promise.allSettled([request('research?ids='+ids),request('product?handle='+encodeURIComponent(r.handle)),request('story-supplements?ids='+ids),request('knowledge?ids='+ids)]);
              if(seq!==detailRequest||selected!==r.id||!knowledgeReport.isConnected)return;
              const result=shopperKnowledgeDiagnosticsFor(r,Object.fromEntries(names.map((name,index)=>[name,responses[index]]))),text=JSON.stringify(result,null,2);
              if(text.length>48000)throw Error('Selected diagnostic exceeded its bound.');
              knowledgeReport.textContent=text;knowledgeState.textContent=result.reads.research==='available'&&result.reads.product==='available'&&result.reads.supplements==='available'&&result.reads.knowledge==='available'?'Current selected-product reads completed. Empty public knowledge does not establish missing saved research.':'Some current reads are unavailable. Unavailable evidence is not treated as missing or approved.';
            }catch{if(seq===detailRequest&&selected===r.id&&knowledgeReport.isConnected){knowledgeReport.textContent='';knowledgeState.textContent='Current selected-product diagnostics could not be verified. No saved research was changed.';}}
            finally{if(readKnowledge.isConnected)readKnowledge.disabled=false;}
          };
          const revisionInput=node('input'),revisionLabel=node('label','Reviewed keyword revision JSON'),revisionPreview=node('pre'),applyRevision=node('button','Apply reviewed keyword revision','btn'),revisionState=node('p','Optional: load a reviewed, exact-version keyword revision file to preview it before applying. This does not regenerate the approved dossier or refresh Demand.','status');
          let revision=null;
          revisionInput.type='file';revisionInput.accept='.json,application/json';revisionInput.setAttribute('aria-label','Reviewed keyword revision JSON');revisionPreview.setAttribute('aria-label','Reviewed keyword revision preview');revisionPreview.style.cssText=knowledgeReport.style.cssText;applyRevision.type='button';applyRevision.disabled=true;
          revisionLabel.appendChild(revisionInput);knowledgeBox.append(revisionLabel,revisionPreview,applyRevision,revisionState);
          revisionInput.onchange=async()=>{
            revision=null;applyRevision.disabled=true;revisionPreview.textContent='';
            const file=revisionInput.files?.[0];if(!file){revisionState.textContent='No reviewed keyword revision is loaded.';return;}
            try{
              if(!Number.isSafeInteger(file.size)||file.size<=0||file.size>=32768||!file.name.toLowerCase().endsWith('.json'))throw Error('Use one bounded JSON file.');
              const proposal=keywordRevisionFor(JSON.parse(await file.text()),r,dossier);
              if(seq!==detailRequest||selected!==r.id||!revisionInput.isConnected)return;
              revision=proposal;revisionPreview.textContent=JSON.stringify(proposal,null,2);applyRevision.disabled=false;revisionState.textContent='Reviewed terms are previewed above. A separate Apply click rechecks the current approved version and existing source bindings.';
            }catch{if(seq===detailRequest&&selected===r.id&&revisionInput.isConnected)revisionState.textContent='This file is invalid, too large or bound to different research. No revision was sent.';}
          };
          applyRevision.onclick=async()=>{
            if(!revision||applyRevision.disabled)return;
            const proposal=revision;applyRevision.disabled=true;revisionInput.disabled=true;revisionState.textContent='Rechecking current approved research before the reviewed revision\u2026';
            try{
              const fresh=await request('research?ids='+encodeURIComponent(productId(r.productId)));
              if(seq!==detailRequest||selected!==r.id||!applyRevision.isConnected)return;
              const current=selectDossier(fresh,r),currentHolds=issueHolds(selectProductIssues(fresh,r)),currentIssueState=fresh.productIssueState||(Array.isArray(fresh.productIssues)?'available':'unavailable');
              if(currentIssueState!=='available'||Object.values(currentHolds).some(Boolean))throw Error('Current issue checks are held or unavailable.');
              const checked=keywordRevisionFor(proposal,r,current);
              const before=current.recommendations[checked.recommendationIndex],existing=before.keywords==null?[]:before.keywords;
              if(!Array.isArray(existing))throw Error('Current keyword seeds could not be verified.');
              const expectedKeywords=[...existing,...checked.keywords];
              if(seq!==detailRequest||selected!==r.id||!applyRevision.isConnected)return;
              const saved=await request('keyword-revision',checked);
              if(seq!==detailRequest||selected!==r.id||!applyRevision.isConnected)return;
              if(saved?.ok!==true){
                const messages={RESEARCH_VERSION_CHANGED:'The approved research version changed. Refresh this product and prepare a revision for its current version.',STORY_REBIND_REQUIRED:'An approved story is bound to this research version. Preserve its exact binding before revising the base dossier.',PRODUCT_HOLD:'A current product hold blocked the revision. Refresh this exact product and preserve its holds before retrying.',KEYWORDS_ALREADY_PRESENT:'These reviewed terms are already present. No keyword revision was applied.'};
                revisionState.textContent=saved?.changed===null||saved?.researchWriteAttempted===true?'The storage outcome is uncertain. Reread this exact product before any retry; do not assume the revision was saved or refused.':saved?.changed===false?messages[saved?.code]||'The reviewed keyword revision was refused by the sandbox checks. Refresh this exact product before retrying.':'The revision outcome could not be verified. Reread this exact product before any retry; do not assume it was saved or refused.';
                return;
              }
              if(saved?.ok!==true||saved.changed!==true||saved.sandboxOnly!==true||saved.productId!==checked.productId||saved.baseDossierVersion!==checked.baseDossierVersion||saved.recommendationIndex!==checked.recommendationIndex||saved.keywordCount!==expectedKeywords.length||saved.version===checked.baseDossierVersion||!(/^[a-f0-9]{64}$/i.test(saved.version||''))||['providerCalls','inferenceCalls','campaignWrites','budgetWrites'].some(name=>saved[name]!==0))throw Error('Revision outcome could not be verified.');
              const readback=await request('research?ids='+encodeURIComponent(productId(r.productId)));
              if(seq!==detailRequest||selected!==r.id||!applyRevision.isConnected)return;
              const approved=selectDossier(readback,r);
              const revised=approved?.recommendations?.[checked.recommendationIndex];
              if(approved?.status!=='approved'||approved.version!==saved.version||revised?.channel!=='keywords'||revised.basis!=='hypothesis'||JSON.stringify(revised.sourceIds)!==JSON.stringify(before.sourceIds)||JSON.stringify(revised.keywords)!==JSON.stringify(expectedKeywords))throw Error('Current revision readback could not be verified.');
              r.dossierVersion=approved.version;revision=null;await show(r);
              if(selected===r.id)status.textContent='Reviewed keyword revision saved and read back for '+r.handle+' \xb7 approved version '+approved.version+'. Existing Demand retains its previous version and must be rechecked before reuse.';
            }catch{if(seq===detailRequest&&selected===r.id&&applyRevision.isConnected)revisionState.textContent='The reviewed revision or its current readback could not be verified. Refresh this product before retrying; no older research is used as a fallback.';}
            finally{if(revisionInput.isConnected)revisionInput.disabled=false;if(applyRevision.isConnected)applyRevision.disabled=true;}
          };
        }
        if(openIssues.length&&global.BritesGrowthCorrections?.mount){const host=node('div');detail.appendChild(host);correctionPanel=global.BritesGrowthCorrections.mount(host,{productId:productId(r.productId),handle:r.handle,dossier,issues,request:correctionRequest});}
        detail.appendChild(node('h3','Product facts with cited sources'));
        if(!(dossier.facts||[]).length)detail.appendChild(node('p','Product facts have not been saved in this dossier yet.','sub'));
        (dossier.facts||[]).forEach(f=>{detail.append(node('p',f.claim),node('blockquote',f.quote));links(detail,dossier,f.sourceIds);});
        detail.appendChild(node('h3','Buyer intent and search relevance'));
        if(!(dossier.buyerIntents||[]).length)detail.appendChild(node('p','Buyer-intent research is still pending.','sub'));
        (dossier.buyerIntents||[]).forEach(intent=>{const b=node('div',null,'recommendation');if(typeof intent==='string')b.appendChild(node('p',intent));else{const title=intent.intent||intent.query||intent.label||intent.theme||'Buyer hypothesis';b.append(node('b',title),node('p',intent.approach||intent.rationale||intent.observation||intent.description||''));if(intent.keywords?.length)b.appendChild(node('p','Search phrases: '+intent.keywords.join(', ')));links(b,dossier,intent.sourceIds);}detail.appendChild(b);});
        detail.appendChild(node('h3','Competitor offers'));
        if(!(dossier.competitors||[]).length)detail.appendChild(node('p','Inspected competitor offers are still pending.','sub'));
        (dossier.competitors||[]).forEach(c=>{const b=node('div',null,'recommendation'),url=safeLink(c.url);if(url){const a=node('a',c.name);a.href=url;a.target='_blank';a.rel='noopener noreferrer';b.appendChild(a);}else b.appendChild(node('b',c.name));let price='Price not verified';if(c.price!=null&&Number.isFinite(Number(c.price))&&/^[A-Z]{3}$/.test(c.currency||''))price=new Intl.NumberFormat('en',{style:'currency',currency:c.currency}).format(c.price)+' '+c.currency;b.append(node('p',price),node('p','Ad spending: '+(c.spend?.status||'unknown'),'status'),node('p',c.observation||''));if(c.spend?.status==='estimate')b.appendChild(node('p','Estimate method: '+c.spend.method+' \xb7 Assumptions: '+(c.spend.assumptions||[]).join('; '),'status'));links(b,dossier,c.sourceIds);detail.appendChild(b);});
        renderOperatorReview(detail,result,r,dossier);
        detail.appendChild(node('h3','Research recommendations \xb7 context only'));
        if(!(dossier.recommendations||[]).length)detail.appendChild(node('p','Actionable test recommendations are still pending.','sub'));
        (dossier.recommendations||[]).forEach(rec=>{const b=node('div',null,'recommendation');b.append(node('b',rec.channel+' \xb7 hypothesis'),node('p',rec.action),node('p','Measure: '+rec.measure,'sub'));links(b,dossier,rec.sourceIds);detail.appendChild(b);});
        async function copyCurrentResearch(copy,includeDemand=false){
          copy.disabled=true;copy.textContent='Rechecking current evidence\u2026';
          try{
            const reads=[request('research?ids='+encodeURIComponent(productId(r.productId)))];
            if(includeDemand)reads.push(request('demand?ids='+encodeURIComponent(productId(r.productId))));
            const [freshResult,freshDemand]=await Promise.all(reads);
            if(seq!==detailRequest||selected!==r.id||!copy.isConnected)return;
            const current=selectDossier(freshResult,r);
            if(!current){copy.textContent='Evidence changed \xb7 refresh first';return;}
            const currentIssues=selectProductIssues(freshResult,r),currentIssueState=freshResult.productIssueState||'unavailable';
            const entry=(freshDemand?.products||[]).find(product=>productId(product.productId)===productId(r.productId));
            const text=includeDemand?demandBriefFor(current,r.title,entry,currentIssues,currentIssueState,Date.now(),{result:freshResult,product:r}):operatorBriefFor(freshResult,r,current,r.title);
            if(!text){copy.textContent='Evidence expired or changed \xb7 refresh first';return;}
            await global.navigator.clipboard.writeText(text);
            if(seq===detailRequest&&selected===r.id&&copy.isConnected)copy.textContent=text.includes('CORRECTIVE RESEARCH ONLY')?(includeDemand?'Corrective research and demand copied':'Corrective notes copied'):(includeDemand?'Research and demand copied':'Test brief copied');
          }catch{if(seq===detailRequest&&selected===r.id&&copy.isConnected)copy.textContent='Current evidence or clipboard unavailable \xb7 refresh first';}
          finally{if(copy.isConnected)copy.disabled=false;}
        }
        if((dossier.recommendations||[]).length&&global.navigator?.clipboard?.writeText){const corrective=!operatorReviewFor({...result,operatorReviewBindingRequired:true},r,dossier),copy=node('button',corrective?'Copy corrective research notes':'Copy product test brief','btn');copy.type='button';copy.onclick=()=>copyCurrentResearch(copy);detail.appendChild(copy);}
        detail.appendChild(node('h3','Meanings and stories'));
        if(!(dossier.meanings||[]).length)detail.appendChild(node('p','Sourced meanings or history are not available for this piece yet.','sub'));
        (dossier.meanings||[]).forEach(m=>{detail.append(node('p',m.text),node('p',m.context+' \xb7 interpretation','status'));links(detail,dossier,m.sourceIds);});
        detail.appendChild(node('h3','Inspected sources'));
        (dossier.sources||[]).forEach(s=>{const p=node('p'),url=safeLink(s.url);if(url){const a=node('a',s.title);a.href=url;a.target='_blank';a.rel='noopener noreferrer';p.appendChild(a);}else p.appendChild(node('span',s.title));p.appendChild(node('small',' \xb7 '+new Date(s.checkedAt).toLocaleDateString()));detail.appendChild(p);});
        (dossier.validation?.warnings||[]).forEach(w=>detail.appendChild(node('p',w,'status')));
        const demandBox=node('section',null,'recommendation');demandBox.append(node('h3','Saved demand and advertising outcomes'),node('p','Reading saved demand evidence\u2026','status'));detail.appendChild(demandBox);
        try{const saved=await request('demand?ids='+encodeURIComponent(productId(r.productId)));if(seq!==detailRequest||selected!==r.id)return;const entry=(saved.products||[]).find(product=>productId(product.productId)===productId(r.productId));demandBox.replaceChildren(node('h3','Saved demand and advertising outcomes'));renderDemand(demandBox,entry);const measured=demandBriefFor(dossier,r.title,entry,issues,issueState,Date.now(),{result,product:r});if(measured&&global.navigator?.clipboard?.writeText){const corrective=measured.includes('CORRECTIVE RESEARCH ONLY'),copy=node('button',corrective?'Copy corrective research and demand':'Copy research and measured demand','btn');copy.type='button';copy.onclick=()=>copyCurrentResearch(copy,true);demandBox.appendChild(copy);}}catch(error){if(seq===detailRequest&&selected===r.id){demandBox.replaceChildren(node('h3','Saved demand and advertising outcomes'),node('p','Saved demand evidence is unavailable: '+error.message,'status'));}}
      }catch(e){if(seq===detailRequest&&selected===r.id){pending.remove();detail.appendChild(node('p',e.message,'error'));}}
    }
    function render(){
      content.replaceChildren();const metrics=node('div',null,'metrics');[['Ranked entries',data.counts?.ranked],['Live product matches',data.counts?.matched],['Completed ranked entries',data.counts?.complete],['Approved dossiers',data.counts?.approvedDossiers]].forEach(([label,count])=>{const card=node('div',null,'metric');card.append(node('strong',String(count??0)),node('span',label));metrics.appendChild(card);});content.appendChild(metrics);
      const layout=node('div',null,'layout'),queue=node('section',null,'box queue'),head=node('div',null,'queue-head'),search=node('input');search.placeholder='Find a product, theme or SKU';search.setAttribute('aria-label','Filter research queue');search.value=filter;search.oninput=()=>{filter=search.value.toLowerCase();renderRows();};head.append(node('h2','Product queue'),search);queue.append(head,rows);layout.append(queue,detail);content.appendChild(layout);
      const selectedRow=(data.queue||[]).find(r=>r.id===selected);if(selectedRow)show(selectedRow);else{clearCorrection();selected=null;++detailRequest;detail.replaceChildren(node('h2','Choose a product'),node('p','See the evidence behind its advertising, search and concierge recommendations.','empty'));renderRows();}
      const blockers=node('section',null,'box blockers');blockers.appendChild(node('h2','Work requiring access'));if(!(data.blockers||[]).length)blockers.appendChild(node('p','No recorded access blockers.','sub'));(data.blockers||[]).forEach(b=>{const a=node('article');a.append(node('b',b.task),node('p',b.detail),node('span',b.status,'tag'));blockers.appendChild(a);});content.appendChild(blockers);
      if(!opts.request){const tools=node('div',null,'controls'),importFile=node('input');importFile.type='file';importFile.accept='.csv';importFile.setAttribute('aria-label','Import ranked products CSV');importFile.onchange=async()=>{if(!importFile.files?.[0])return;try{const csv=parseCsv(await importFile.files[0].text());await request('import',{rows:csv.slice(0,100).map(r=>({rank:Number(r.Rank),title:r['Item Name']||r.Title,sku:r.SKU,handle:r.Handle,orders:Number(r.Orders||r['Number of Orders']),theme:r.Theme}))});await load();}catch(e){status.textContent=e.message;}};tools.append(node('span','Import top products:','status'),importFile);content.appendChild(tools);}
    }
    async function load(){refresh.disabled=true;status.textContent='Reading saved progress\u2026';try{data=await request('status');auth.hidden=true;operations.hidden=false;render();status.textContent='Updated '+new Date(data.at).toLocaleString()+'. '+(data.control?.catalogueComplete?'Full catalogue mirror loaded.':'Live catalogue mirror is progressing.');}catch(e){if(e.status===401){auth.hidden=!!opts.request;operations.hidden=true;content.replaceChildren();++detailRequest;}status.textContent=e.message;}finally{refresh.disabled=false;}}
    auth.onsubmit=async e=>{e.preventDefault();key=pass.value.trim();pass.value='';try{sessionStorage.setItem('brites-growth-key',key);}catch(x){}await load();};refresh.onclick=load;if(opts.request||key)await load();else{status.textContent='Sign in to see private progress.';refresh.disabled=true;}
    return {refresh:load};
  }
  global.BritesGrowth={operatorReviewFor,operatorBriefFor,renderOperatorReview,mount,parseCsv,productId,selectDossier,selectProductIssues,issueHolds,briefFor,demandBriefFor,receiptPreviewFor,safeLink,shopperKnowledgeDiagnosticsFor,keywordRevisionFor};if(document.querySelector('#growth-root'))mount(document.querySelector('#growth-root'));
})(window);
