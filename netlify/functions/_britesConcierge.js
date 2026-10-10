'use strict';

// Shop policy knowledge is read from two fixed public storefront URLs. Shopper
// text is never sent upstream, and policy text never enters an AI/tool prompt.
const POLICIES=Object.freeze({shipping:Object.freeze({url:'https://britesjewelry.com/policies/shipping-policy',title:'Shipping policy'}),refund:Object.freeze({url:'https://britesjewelry.com/policies/refund-policy',title:'Refund policy'})});
const MAX_HTML_BYTES=1024*1024;
const COUNTRIES=[
  ['United States',/\b(?:united states|usa|u\.s\.?)\b/i],['Canada',/\bcanada\b/i],['United Kingdom',/\b(?:united kingdom|uk|u\.k\.?|britain|england|scotland|wales|northern ireland)\b/i],
  ['Germany',/\bgermany\b/i],['France',/\bfrance\b/i],['Poland',/\bpoland\b/i],['Italy',/\bitaly\b/i],['Spain',/\bspain\b/i],['Japan',/\bjapan\b/i],['Sweden',/\bsweden\b/i],['Finland',/\bfinland\b/i],
  ['Australia',/\baustralia\b/i],['New Zealand',/\bnew zealand\b/i],['Ireland',/\bireland\b/i],['Netherlands',/\b(?:netherlands|holland)\b/i],['Norway',/\bnorway\b/i],['Denmark',/\bdenmark\b/i],['Switzerland',/\bswitzerland\b/i],['Mexico',/\bmexico\b/i],['India',/\bindia\b/i],['China',/\bchina\b/i],['Singapore',/\bsingapore\b/i]
];
const PRIVATE_REQUEST=/\b(?:api keys?|credentials?|passwords?|system prompt|private (?:records|data)|owner data|repository|source code|sales history|customer (?:records|data))\b/i;
const shoppingGuide=require('../../brites-concierge-shopping-guide.js');
const catalogueDiscovery=require('./_britesCatalogueDiscovery.js');
const SHIPPING_SCOPE=/\b(?:ships?\s+(?:worldwide|abroad|internationally|overseas|anywhere|everywhere|to|my|this|it)|(?:worldwide|international|overseas)\s+shipping|(?:do|can|will|would)\s+you\s+ship)\b/i;
const CUSTOMS_REQUEST=/\b(?:customs(?:[ -]+dut(?:y|ies))?|import[ -]+(?:dut(?:y|ies)|tax(?:es)?))\b/i;
const tidy=(value,max=3000)=>String(value??'').replace(/\u0000/g,'').trim().slice(0,max);
// This selects knowledge for the current piece, never a website action. Shared
// intent classification keeps quoted, hypothetical and private setter words
// out of this route. The live catalogue and reviewed-meaning gate own facts.
function meaningContextRequest(message,context={}){
  const text=tidy(message,2000),intent=shoppingGuide.classifyShopperIntent(text);
  if(intent.kind!=='meaning'||!intent.recognized||intent.denied||PRIVATE_REQUEST.test(text))return null;
  const handle=context.currentHandle;
  if(typeof handle!=='string'||!/^[a-z0-9]+(?:[-_][a-z0-9]+)*$/.test(handle)||handle.length>180)return null;
  if(/\b(?:compare|comparison|versus|first|second|third|fourth|fifth|sixth|[1-6](?:st|nd|rd|th))\b/i.test(text))return null;
  const normal=value=>value.toLowerCase().replace(/[^\p{L}\p{N}]+/gu,' ').trim(),identities=catalogueDiscovery.inventoryIdentities(context.inventoryPieces),names=new Map(),matched=new Set();let masked=' '+normal(text)+' ';
  for(const product of identities){const name=' '+normal(product.title)+' ';if(!names.has(name))names.set(name,[]);names.get(name).push(product.handle);if(new RegExp('(?:^|[^a-z0-9_-])'+product.handle+'(?=$|[^a-z0-9_-])','i').test(text))matched.add(product.handle);}
  for(const [name,handles]of [...names].sort((a,b)=>b[0].length-a[0].length))if(masked.includes(name)){handles.forEach(handle=>matched.add(handle));masked=masked.split(name).join(' ');}
  if([...matched].some(named=>named!==handle))return null;
  const urls=[...text.matchAll(/https?:\/\/[^\s<>"']+/gi)].map(hit=>hit[0].replace(/[),.!?]+$/,''));
  if(urls.length&&urls.some(value=>{try{const u=new URL(value);return u.protocol!=='https:'||u.username||u.password||u.port||!['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)||!new RegExp('^/products/'+handle+'/?$').test(u.pathname)||u.search||u.hash;}catch{return true;}}))return null;
  const current=/\b(?:this|that|its?|current|selected)\b/i.test(text);
  return current||urls.length?{handle}:null;
}
function meaningContextReply({product,meaning,personalContext={}}={}){
  if(!product||!meaning||meaning.productId!==product.id||meaning.kind!=='interpretation'||!Array.isArray(meaning.sources)||!meaning.sources.length)return null;
  const context=shoppingGuide.normalizeShopperContext(personalContext),text=tidy(meaning.text,1500),title=tidy(product.title,300);
  if(!title||!text||PRIVATE_REQUEST.test(text))return null;
  const recipient=context.recipient.replace(/^(?:my|our)\s+/i,'your '),label=recipient==='myself'?'yourself':recipient&&!/^(?:your|a|an|the)\b/i.test(recipient)?'your '+recipient:recipient;
  const scope=context.occasion?(label&&label!=='yourself'?label+'’s '+context.occasion:context.occasion):label;
  // Never cut a source sentence into a stronger claim. Long interpretations
  // stay intact on their cited card instead of being copied into the reply.
  const interpretation=text.length<=500?'this reviewed interpretation: “'+text+'”':'the reviewed interpretation shown below';
  const reason=context.reason?' You described the personal reason as “'+context.reason+'”.':'';
  return {reply:(scope?'For '+scope+', ':'')+title+' has '+interpretation+'.'+reason+' It is a possible personal connection, rather than a universal meaning.',question:scope||context.reason?'Does that interpretation fit what you want the piece to express?':null};
}
function decodeHtml(value){const entities={amp:'&',lt:'<',gt:'>',quot:'"',apos:"'",nbsp:' ',ndash:'–',mdash:'—',rsquo:"'",lsquo:"'",ldquo:'"',rdquo:'"'};return value.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi,(whole,code)=>{if(code[0]!=='#')return entities[code.toLowerCase()]??whole;const n=code[1].toLowerCase()==='x'?parseInt(code.slice(2),16):parseInt(code.slice(1),10);return Number.isInteger(n)&&n>31&&n<=0x10ffff&&!(n>=0xd800&&n<=0xdfff)?String.fromCodePoint(n):' ';});}
function policyBlocks(html){
  if(typeof html!=='string'||Buffer.byteLength(html)>MAX_HTML_BYTES)return [];
  const safe=html.replace(/<(script|style|template|noscript)\b[^>]*>[\s\S]*?<\/\1\s*>/gi,'');
  const opening=/<div\b[^>]*\bclass\s*=\s*["'][^"']*\bshopify-policy__body\b[^"']*["'][^>]*>/i.exec(safe);if(!opening)return [];
  const start=opening.index+opening[0].length,tags=/<\/?div\b[^>]*>/gi;tags.lastIndex=start;let depth=1,end=-1,tag;
  while((tag=tags.exec(safe))){depth+=/^<\//.test(tag[0])?-1:1;if(!depth){end=tag.index;break;}}if(end<0||end-start>60000)return [];
  const body=safe.slice(start,end).replace(/<br\b[^>]*>|<\/(?:p|li|div|h[1-6]|ul|ol|section)>/gi,'\n').replace(/<[^>]+>/g,' ');
  return decodeHtml(body).replace(/[‘’]/g,"'").split(/\n+/).map(x=>tidy(x.replace(/\s+/g,' '),4000)).filter(Boolean).slice(0,80);
}
function timing(text){const m=/(\d{1,3})\s*(?:[–—-]|to)\s*(\d{1,3})\s*(business|calendar)?\s*days?\b/i.exec(text||'');if(!m)return null;const min=Number(m[1]),max=Number(m[2]);return min>0&&min<=max&&max<=365?{min,max,unit:m[3]?.toLowerCase()||'unspecified'}:null;}
function countryMentions(message){
  const matches=[];
  for(const [name,pattern] of COUNTRIES){const scan=new RegExp(pattern.source,'gi');let m;while((m=scan.exec(message)))matches.push({name,index:m.index,end:m.index+m[0].length});}
  for(const m of message.matchAll(/\bUS\b/g))matches.push({name:'United States',index:m.index,end:m.index+m[0].length});
  // Northern Ireland is part of the UK; its contained "Ireland" must not win.
  return matches.filter(m=>!matches.some(other=>other!==m&&other.index<=m.index&&other.end>=m.end&&(other.index<m.index||other.end>m.end)));
}
function countryIn(message){
  const text=tidy(message,2000).replace(/[‘’]/g,"'"),matches=countryMentions(text).filter(m=>{
    // "Instead of" and "rather than" exclude the following destination;
    // standalone "instead" begins a replacement clause and resets negation.
    const prefix=text.slice(Math.max(0,m.index-100),m.index).split(/[,;.!?]|\b(?:but|instead(?!\s+of\b)|rather(?!\s+than\b)|however)\b/i).at(-1);
    return !/\b(?:not|instead of|rather than|don't ship to|do not ship to)\s+(?:\w+\s+){0,4}$/i.test(prefix);
  });
  return matches.sort((a,b)=>b.index-a.index)[0]?.name||null;
}
function listedCountries(text){
  const matches=countryMentions(text);let remaining=text;
  for(const m of [...matches].sort((a,b)=>b.index-a.index))remaining=remaining.slice(0,m.index)+remaining.slice(m.end);
  return matches.length&&!remaining.replace(/\band\b|[,;&()\s]/gi,'')?[...new Set(matches.map(x=>x.name))]:null;
}
function parseShipping(blocks){
  const processing=blocks.find(x=>/\bstandard production\b/i.test(x))||'',standard=/\bstandard production\s+(?:usually\s+)?(?:takes|is|requires)\s+(.{1,70})/i.exec(processing),expedited=/\bexpedited production\s*(?:\(|(?:takes|is|:)\s*)(.{1,70})/i.exec(processing);
  const standardClaim=standard?.[1].split(/[.;]/)[0]||'',production=/^\d/.test(standardClaim)&&!/previously|no longer|instead|now takes|currently|suspended/i.test(standardClaim)?timing(standardClaim):null,rush=expedited&&!/no longer|not available|unavailable|suspended/i.test(processing)?timing(expedited[1]):null;
  const destinationBlock=blocks.find(x=>/^where we ship[.:]/i.test(x)),destinationList=/^where we ship[.:]\s+([^.]*)/i.exec(destinationBlock||''),destinations=destinationList?listedCountries(destinationList[1]):null;
  const transit=[];let inTransit=false;
  for(const block of blocks){if(/^transit time\b/i.test(block)){inTransit=true;continue;}if(inTransit&&/^your total|^free shipping|^where we ship|^tracking|^questions/i.test(block))inTransit=false;if(inTransit){const colon=block.indexOf(':'),claim=block.slice(colon+1).trim(),range=colon>0&&/^\d/.test(claim)&&!/previously|no longer|instead|now takes|currently|suspended/i.test(claim)?timing(claim):null,countries=colon>0?countryMentions(block.slice(0,colon)).map(x=>x.name):[];if(range&&countries.length)transit.push({countries,...range});}}
  const free=blocks.find(x=>/^free shipping[.:]/i.test(x)),threshold=free?/\bfree standard shipping\s+on orders\s+(over|above|at least)\s*([$€£]\s*\d+(?:\.\d{1,2})?)/i.exec(free):null;
  return {production,rush,transit,destinations,freeShipping:threshold&&!/no longer|not available|unavailable|suspended/i.test(free)?{comparison:threshold[1].toLowerCase(),display:threshold[2].replace(/\s+/g,''),currency:null}:null,checkoutRates:blocks.some(x=>/\brates(?: and delivery speed)? are calculated at checkout\b/i.test(x)),customs:blocks.some(x=>/customs duties|import taxes/i.test(x)&&/recipient'?s responsibility/i.test(x)&&!/no (?:customs|import)|not (?:subject|responsible)/i.test(x)),tracking:blocks.some(x=>/you'?ll receive tracking by email as soon as your order ships/i.test(x))};
}
function parseRefund(blocks){
  const eligibility=blocks.find(x=>/\byou (?:may|can) return or exchange most items within/i.test(x))||'',window=/\bwithin\s+(\d{1,3})\s+(business |calendar )?days\s+(?:of|after)\s+(delivery|purchase|receipt)/i.exec(eligibility);
  const personal=blocks.find(x=>/\bpersonalized.{0,20}custom items\b/i.test(x))||'',earrings=blocks.find(x=>/^earrings\b/i.test(x))||'',damage=blocks.find(x=>/^damaged or defective on arrival\b/i.test(x))||'',repairs=blocks.find(x=>/^free\s+\d+[- ]day repairs\b/i.test(x))||'',care=blocks.find(x=>/^caring for your jewelry\b/i.test(x))||'',postage=blocks.find(x=>/^return shipping[.:]/i.test(x))||'';
  const damageWindow=/\bwithin\s+(\d{1,3})\s+days\b/i.exec(damage),repairWindow=/\bwithin the first\s+(\d{1,3})\s+days\b/i.exec(repairs);
  const returnWindow=window&&Number(window[1])>0&&Number(window[1])<=365?{days:Number(window[1]),unit:window[2]?.trim()||'days',from:window[3].toLowerCase()}:null;
  const boundedDays=m=>m&&Number(m[1])>0&&Number(m[1])<=365?Number(m[1]):null;
  return {returnWindow,unworn:/\bunworn\b/i.test(eligibility)&&!/\bnot unworn\b/i.test(eligibility),originalCondition:/\boriginal condition\b/i.test(eligibility)&&!/\bnot in original condition\b/i.test(eligibility),notPersonalized:/\bnot personalized\b/i.test(eligibility),directPurchasesOnly:/\bpurchased directly from britesjewelry\.com\b/i.test(eligibility),contactFirst:/message us first|contact us first/i.test(eligibility),personalizedFinalSale:/\bfinal sale\b/i.test(personal)&&!/\bnot final sale\b/i.test(personal),madeToOrderExcluded:/\bmade to order\b/i.test(personal),earringsExcluded:/\b(?:can't|cannot) be returned or exchanged\b/i.test(earrings),earringDefectException:/\bunless they arrive defective or we made an error\b/i.test(earrings),returnPostage:blocks.some(x=>/customers are responsible for return shipping/i.test(x)),returnLabelUnavailable:/\b(?:unable to|cannot|can't|do not|don't)\s+provide\s+(?:a\s+)?return labels?(?:\s+or\s+refund original or return shipping costs)?\s*(?:[.;]|$)/i.test(postage),shippingCostsNotRefunded:/\b(?:unable to|cannot|can't|do not|don't)\s+(?:provide\s+(?:a\s+)?return labels?\s+or\s+)?refund original or return shipping costs\s*(?:[.;]|$)/i.test(postage),damageDays:boundedDays(damageWindow),damageIncludesPersonalized:/including a personalized one/i.test(damage),damageOrderError:/isn't what you ordered|wrong item/i.test(damage),repairsDays:repairWindow&&/normal wear|repair it free of charge/i.test(repairs)?boundedDays(repairWindow):null,repairPostage:/you only cover return shipping/i.test(repairs),care:{sleeping:/remove jewelry before[^.]{0,120}\bsleeping\b/i.test(care),showering:/remove jewelry before[^.]{0,120}\bshowering\b/i.test(care),strenuousActivity:/remove jewelry before[^.]{0,120}\bstrenuous activity\b/i.test(care),dryStorage:/store it somewhere dry/i.test(care)},contactEmail:blocks.some(x=>/\binfo@britesjewelry\.com\b/i.test(x))?'info@britesjewelry.com':null};
}
function classify(message,history=[]){
  const text=tidy(message,2000).replace(/[‘’]/g,"'");history=Array.isArray(history)?history.slice(-12):[];if(PRIVATE_REQUEST.test(text))return {topics:[],policyOnly:false,country:null};
  const topics=[];const skipShipping=/\b(?:skip|forget|don't discuss|do not discuss|not asking about)\s+(?:the\s+)?(?:shipping|delivery)\b/i.test(text),skipReturns=/\b(?:skip|forget|don't discuss|do not discuss|not asking about)\s+(?:the\s+)?(?:returns?|refunds?)\b/i.test(text);
  // A return to a previously discussed design is shopping/navigation. Remove
  // only that return verb, so a separate refund question still takes its route.
  // Merchant/sender objects remain after-sale language rather than selection.
  const returnText=text.replace(/\breturn\s+(?:back\s+)?to\s+(?:(?:the|that|this)\s+)?(?:page|shop|store|catalogue|catalog|first|second|home)\b|\breturn\s+(?:back\s+)?to\s+(?!(?:(?:the|that|this)\s+)?(?:you|us|sender|seller|merchant|brites)\b)(?:(?:the|that|this)\s+)?(?:[a-z][a-z-]*\s+){0,5}(?:necklace|earrings?|bracelet|pendant|charm|ring|piece|design|selection)\b/gi,match=>match.replace(/^return\b/i,''));
  if(!skipShipping&&(/\b(?:shipping|delivery|deliver|delivered|arrive|arrival|postage|dispatch|production|turnaround|tracking|customs duties|import taxes)\b|\bship\s+(?:to|my|this|it)|\b(?:do|can|will) you ship\b|\btrack (?:my|the|this) order|\bwhere is my order|\bhas (?:my|the) order shipped\b/i.test(text)||SHIPPING_SCOPE.test(text)||CUSTOMS_REQUEST.test(text)))topics.push('shipping');
  if(!skipReturns&&/\b(?:returns?|refunds?|exchanges?|repairs?|defect(?:ive)?|damaged|final sale)\b|\b(?:my|this|the) (?:necklace|chain|piece|jewelry|jewellery) (?:broke|is broken)\b|\b(?:arrived|arrives?) (?:broken|wrong)\b/i.test(returnText))topics.push('refund');
  if(/\b(?:jewelry care|jewellery care|caring for|cleaning|waterproof|tarnish|showering|shower|swimming|swim|sleeping|storage)\b|\b(?:clean|polish)\s+(?:my|this|the|a|your)\s+(?:jewelry|jewellery|necklace|ring|earrings|piece|silver|gold)\b/i.test(text))topics.push('care');
  let country=countryIn(text);const lastAssistant=[...history].reverse().find(x=>x?.role==='assistant');
  const destinationQuestion=/which country|what country|gift going|destination/i.test(lastAssistant?.content||'');
  if(!topics.length&&!skipShipping&&country&&destinationQuestion&&history.slice(-4).some(x=>x?.role==='user'&&/shipping|arrive|delivery|deliver|production/i.test(x.content||'')))topics.push('shipping');
  if(topics.includes('shipping')&&!country&&!countryMentions(text).length&&!/start (?:fresh|over)|new gift|different person|forget (?:everything|the destination)|not there anymore/i.test(text)){
    for(let i=history.length-1;i>=Math.max(0,history.length-6);i--){const row=history[i];if(row?.role!=='user')continue;if(/start (?:fresh|over)|new gift|different person/i.test(row.content||''))break;const found=countryIn(row.content||'');if(found&&(/shipping|arrive|delivery|deliver|ship to/i.test(row.content||'')||(tidy(row.content,100).length<50&&/which country|what country|gift going|destination/i.test(history[i-1]?.content||'')))){country=found;break;}}
  }
  const policyPage=/^(?:please\s+)?(?:show|find|view|open)\s+(?:me\s+)?(?:the\s+)?(?:shipping|return|refund|exchange|care)\s+(?:policy|policies|page|terms)[.!?]?$/i.test(text);
  // A verb requesting after-sale help is not itself a product-selection verb.
  // Remove only that explicit verb/object phrase; separate shopping or cart
  // requests in the same message remain available to the mixed-intent route.
  const selectionText=text.replace(/\b(?:show|find|view|open|recommend|suggest|choose|browse|looking for|want|need|buy|shopping for)\s+(?:me\s+)?(?:(?:a|an|the|your|my)\s+)?(?:(?:how\s+)?to\s+)?(?:(?:shipping|delivery|care)\s+(?:policy|policies|page|terms|conditions|instructions|help|guidance)|(?:returns?|refunds?|exchanges?|repairs?)(?:\s+(?:policy|policies|page|terms|conditions|instructions|help|guidance))?)\b/gi,'');
  const discovery=!policyPage&&(/\b(?:find|show|recommend|suggest|choose|browse|looking for|want|buy|shopping for)\b/i.test(selectionText)||(/\bneed\s+(?:a|an|some)\b/i.test(selectionText)&&/\b(?:necklace|earrings?|bracelet|ring|jewelry|jewellery|gift)\b/i.test(selectionText)));
  const action=!policyPage&&/\b(?:open|take me to|go to|view (?:the )?page|add|put)\b[^.]{0,100}\b(?:first|second|third|fourth|fifth|sixth|piece|one|bag|cart|page)\b/i.test(selectionText);
  // An explicit item-budget update can mention shipping merely to exclude it.
  // Require a preference clause plus an item/metal qualifier; policy questions
  // that merely quote a price ("shipping under $75?") remain policy-only.
  const budgetPreference=/(?:^|[,;.!?]|\b(?:actually|prefer|want|keep(?: it)?|make it))\s*(?:(?:my\s+)?budget\s*(?:is|of|:)?\s*)?(?:under|below|up to|less than|max(?:imum)?|at most|within|no more than|price limit)\s*(?:is|of|to|:)?\s*(?:usd|cad|gbp|eur|c\$|ca\$|us\$|\$|€|£)?\s*\d+(?:\.\d{1,2})?\b/i.test(selectionText)&&/\b(?:each|per (?:piece|item|necklace|pair)|before (?:shipping|taxes)|in (?:sterling silver|silver|rose gold|gold(?: filled| plated)?))\b/i.test(selectionText);
  const selectionReturn=returnText!==text;
  return {topics:[...new Set(topics)],policyOnly:topics.length>0&&!discovery&&!action&&!budgetPreference&&!selectionReturn,country};
}
function rangeText(range){return range.min+'–'+range.max+' '+(range.unit==='unspecified'?'':range.unit+' ')+'days';}
function shippingAnswer(facts,classification,message){
  const country=classification.country,parts=[],rush=/\b(?:rush|expedited|faster|sooner|urgent)\b/i.test(message),production=rush&&facts.rush?facts.rush:facts.production;
  if(production)parts.push((rush&&facts.rush?'Expedited production':'Standard production')+' is listed as '+rangeText(production)+'.');
  if(country&&facts.destinations&&!facts.destinations.includes(country)){parts.push(country+' is not listed among the current policy destinations; check availability with the shop before ordering.');}
  else if(country){const transit=facts.transit.find(x=>x.countries.includes(country));if(transit){parts.push('The policy lists '+rangeText(transit)+' in transit to '+country+', after production.');if(production?.unit==='business'&&transit.unit==='business')parts.push('Together that is '+(production.min+transit.min)+'–'+(production.max+transit.max)+' business days, rather than a guaranteed arrival date.');}}
  if(!country&&facts.destinations&&SHIPPING_SCOPE.test(message)&&/\b(?:worldwide|abroad|internationally|overseas|anywhere|everywhere)\b/i.test(message))parts.push('The current policy lists specific destination countries; confirm the destination before ordering.');
  if(facts.freeShipping&&/\b(?:free|cost|price|rate|postage|shipping)\b/i.test(message))parts.push('Free standard shipping is listed for orders '+facts.freeShipping.comparison+' '+facts.freeShipping.display+'; checkout confirms eligibility, currency and rates.');
  if(facts.checkoutRates)parts.push('Checkout shows the available shipping services and charges.');
  if(/\btracking|track (?:my|the|this) order|where is my order|has (?:my|the) order shipped\b/i.test(message)){if(facts.tracking)parts.push('The policy says tracking is emailed when the order ships.');parts.push('I can explain the published policy, but I cannot access a customer’s private order or tracking record.');}
  const deadlineHint=/\b(?:tomorrow|today|tonight|birthday|deadline|christmas|wedding|anniversary|graduation|next (?:week|day|month))\b|\b(?:before|by|in|within)\s+(?:the\s+)?(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?\s+\d{1,2}\b|\d{4}-\d{1,2}-\d{1,2}\b|\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\b|\d{1,2}(?:st|nd|rd|th)\b|\d{1,3}\s*(?:business days?|calendar days?|days?|weeks?|months?|hours?|am|pm)\b)/i.test(message);
  if(deadlineHint)parts.push('For a gift deadline, confirm the date with the shop before paying; production and transit are separate.');
  if(facts.customs&&country&&!['United States','Canada'].includes(country))parts.push('International duties or import taxes may be payable by the recipient.');
  return {text:parts.join(' '),question:!country&&(/\b(?:tim(?:e|ing)|long|when|arrive|arrival|deliver|delivery|deadline|tomorrow|birthday|expedited|rush)\b|\bship to\b|\b(?:do|can|will) you ship\b/i.test(message)||SHIPPING_SCOPE.test(message))?'Which country is the gift going to?':null};
}
function refundScenario(message){
  const text=tidy(message,2000).replace(/[‘’]/g,"'");
  const negated=match=>{
    if(/^[- ]free\b/i.test(text.slice(match.index+match[0].length)))return true;
    const prefix=text.slice(Math.max(0,match.index-100),match.index).split(/[,.!?;:]|\b(?:but|however|yet|because|although|if|unless|except)\b/i).at(-1).replace(/\bnot\s+(?:only|just|merely)\s+/gi,'');
    if(/\b(?:non|un)[ -]$/i.test(prefix))return true;
    // Local modifiers and coordinated conditions keep "not damaged or
    // defective" negative without treating "don't want a damaged item" as
    // proof that an existing item is undamaged.
    return /\b(?:no|not|never|neither|nor|without|isn't|aren't|wasn't|weren't|hasn't|haven't|hadn't|doesn't|don't|didn't)\s+(?:(?:really|actually|currently|visibly|physically|otherwise|ever|still|been|longer|any|a|at all|in any way|evidence of|signs? of)\s+|(?:damage(?:d)?|defect(?:ive)?|broken|broke|wrong|mistakes?|errors?)\s+(?:or|and|nor)\s+){0,6}$/i.test(prefix);
  };
  const asserted=pattern=>[...text.matchAll(pattern)].some(match=>!negated(match));
  return {damaged:asserted(/\b(?:damage(?:d)?|defect(?:ive)?|wrong|mistake|error)\b|\b(?:arrived|arrives?) (?:broken|wrong)\b/gi),repair:asserted(/\b(?:break|broken|broke|repairs?)\b/gi)};
}
function refundAnswer(facts,message,careOnly=false){
  const parts=[];
  if(careOnly){const steps=[];if(facts.care.sleeping)steps.push('sleeping');if(facts.care.showering)steps.push('showering');if(facts.care.strenuousActivity)steps.push('strenuous activity');if(steps.length)parts.push('The shop’s care guidance says to remove jewelry before '+steps.join(', ')+'.');if(facts.care.dryStorage)parts.push('Store it somewhere dry.');if(/waterproof|swim|allerg|nickel/i.test(message))parts.push('This care guidance does not establish waterproof or allergy-safe properties for a particular piece.');return parts.join(' ');}
  const {damaged,repair}=refundScenario(message);
  if(damaged||repair){if(damaged&&facts.damageDays)parts.push('For an item that arrives damaged or defective'+(facts.damageOrderError?', or different from the order':'')+', the policy says to contact the shop within '+facts.damageDays+' days'+(facts.damageIncludesPersonalized?', including personalized items':'')+'.');if(repair&&facts.repairsDays)parts.push('The policy lists repairs for the first '+facts.repairsDays+' days of normal wear'+(facts.repairPostage?'; customers cover return shipping.':'.'));}
  else {
    if(facts.returnWindow){const conditions=[facts.unworn?'unworn':null,facts.originalCondition?'in original condition':null,facts.notPersonalized?'not personalized':null].filter(Boolean);parts.push('The policy allows returns or exchanges of most items within '+facts.returnWindow.days+' '+(facts.returnWindow.unit==='days'?'':facts.returnWindow.unit+' ')+'days of '+facts.returnWindow.from+(conditions.length?', provided they are '+conditions.join(', '):'')+'.');}
    if(facts.personalizedFinalSale)parts.push('Personalized and custom'+(facts.madeToOrderExcluded?' / made-to-order':'')+' items are final sale for a change of mind.');
    if(facts.earringsExcluded)parts.push('Earrings are excluded from returns and exchanges'+(facts.earringDefectException?', unless defective on arrival or the shop made an error':'')+'.');
  }
  if(facts.directPurchasesOnly)parts.push('This policy applies to purchases directly from britesjewelry.com.');
  if(facts.returnPostage&&!damaged&&!repair)parts.push('Customers cover return shipping.');
  if(!damaged&&!repair){if(facts.returnLabelUnavailable)parts.push('The policy says the shop cannot provide a return label.');if(facts.shippingCostsNotRefunded)parts.push('Original and return shipping costs are not refunded.');}
  if(facts.contactFirst||facts.contactEmail)parts.push('Contact the shop'+(facts.contactEmail?' at '+facts.contactEmail:'')+' before sending anything back; keep order details in that direct conversation.');
  return parts.join(' ');
}
function policyCoverage(kind,facts,classification,message){
  if(kind==='care')return Object.values(facts.care).some(Boolean);
  if(kind==='refund'){
    const {damaged,repair}=refundScenario(message);
    if(damaged)return !!facts.damageDays;
    if(repair)return !!facts.repairsDays;
    if(/\b(?:prepaid|return labels?)\b/i.test(message)&&!facts.returnLabelUnavailable)return false;
    if(/\boriginal (?:postage|shipping)|\brefund (?:the )?(?:original|return) (?:postage|shipping)\b/i.test(message)&&!facts.shippingCostsNotRefunded)return false;
    if(/\b(?:engraved|personalized|custom|final sale)\b/i.test(message)&&!facts.personalizedFinalSale)return false;
    if(/\bearrings?\b/i.test(message)&&!facts.earringsExcluded)return false;
    return !!facts.returnWindow;
  }
  if(/\b(?:track(?:ing)?|where is my order)\b/i.test(message)&&!facts.tracking)return false;
  if(/\bfree\b/i.test(message)&&!facts.freeShipping)return false;
  if(/\b(?:cost|rate|postage)\b/i.test(message)&&!facts.checkoutRates&&!facts.freeShipping)return false;
  if(/\b(?:tim(?:e|ing)|long|when|arrive|arrival|deliver|delivery|production|turnaround|expedited|rush)\b/i.test(message)){
    if(!facts.production||(/\b(?:expedited|rush)\b/i.test(message)&&!facts.rush))return false;
    if(classification.country&&(!facts.destinations||facts.destinations.includes(classification.country))&&!facts.transit.some(x=>x.countries.includes(classification.country)))return false;
  }
  if((/\bship to\b|\b(?:do|can|will) you ship\b/i.test(message)||SHIPPING_SCOPE.test(message))&&!facts.destinations)return false;
  if(CUSTOMS_REQUEST.test(message)&&!facts.customs)return false;
  return !!(facts.production||facts.checkoutRates||facts.freeShipping||facts.transit.length||facts.destinations||facts.tracking);
}
function unavailableAnswer({message,history=[]}={}){
  const classification=classify(message,history);if(!classification.topics.length)return null;
  const kinds=[...new Set(classification.topics.map(x=>x==='care'?'refund':x))];
  return {reply:'I couldn’t verify all the relevant policy details just now. The current policy page and checkout have the applicable terms.',question:null,policyOnly:classification.policyOnly,policyLinks:kinds.map(kind=>({label:POLICIES[kind].title,url:POLICIES[kind].url})),policyKnowledge:{status:'partial',sources:[],checkedAt:null},policyUnavailable:true};
}
async function boundedHtml(response){const length=Number(response.headers?.get?.('content-length'));if(length>MAX_HTML_BYTES)throw Error('Policy response is too large.');if(response.body?.getReader){const reader=response.body.getReader(),chunks=[];let size=0;try{for(;;){const chunk=await reader.read();if(chunk.done)break;size+=chunk.value.byteLength;if(size>MAX_HTML_BYTES){await reader.cancel();throw Error('Policy response is too large.');}chunks.push(Buffer.from(chunk.value));}}finally{reader.releaseLock();}return Buffer.concat(chunks).toString('utf8');}const html=await response.text();if(Buffer.byteLength(html)>MAX_HTML_BYTES)throw Error('Policy response is too large.');return html;}
function createPolicyGuide({fetch=globalThis.fetch,now=Date.now,ttlMs=60000,timeoutMs=10000}={}){
  const cache=new Map(),pending=new Map();ttlMs=Math.min(60000,Math.max(0,ttlMs));
  async function read(kind){if(!POLICIES[kind])throw Error('Unknown shop policy.');const previous=cache.get(kind),age=previous?now()-previous.checkedAt:Infinity;if(previous&&age>=0&&age<ttlMs)return previous;if(pending.has(kind))return pending.get(kind);
    const task=(async()=>{const spec=POLICIES[kind],response=await fetch(spec.url,{headers:{Accept:'text/html'},redirect:'error',credentials:'omit',signal:AbortSignal.timeout(timeoutMs)});if(!response.ok||response.status!==200||(response.url&&response.url.replace(/\/$/,'')!==spec.url)||!/^text\/html\b/i.test(response.headers?.get?.('content-type')||''))throw Error('Published policy could not be checked.');const blocks=policyBlocks(await boundedHtml(response));if(!blocks.length)throw Error('Published policy body is unavailable.');const record={kind,...spec,checkedAt:now(),facts:kind==='shipping'?parseShipping(blocks):parseRefund(blocks)};cache.set(kind,record);return record;})();pending.set(kind,task);try{return await task;}finally{pending.delete(kind);}
  }
  async function answer({message,history=[]}={}){
    const classification=classify(message,history);if(!classification.topics.length)return null;
    const kinds=[...new Set(classification.topics.map(x=>x==='care'?'refund':x))],checked=await Promise.allSettled(kinds.map(read)),records=checked.flatMap(x=>x.status==='fulfilled'?[x.value]:[]),sources=records.map(x=>({title:x.title,url:x.url,checkedAt:x.checkedAt})),parts=[];let question=null,verifiedTopics=0;
    for(const topic of classification.topics){const record=records.find(x=>x.kind===(topic==='care'?'refund':topic));if(!record)continue;const result=topic==='shipping'?shippingAnswer(record.facts,classification,tidy(message,2000)):{text:refundAnswer(record.facts,tidy(message,2000),topic==='care'),question:null};if(result.text)parts.push(result.text);if(result.text&&policyCoverage(topic,record.facts,classification,tidy(message,2000)))verifiedTopics++;if(result.question)question=result.question;}
    const unavailable=records.length<kinds.length||verifiedTopics<classification.topics.length;
    if(unavailable)parts.push('I couldn’t verify all the relevant policy details just now. The current policy page and checkout have the applicable terms.');
    return {reply:tidy(parts.join('\n\n'),2200),question,policyOnly:classification.policyOnly,policyLinks:kinds.map(kind=>({label:POLICIES[kind].title,url:POLICIES[kind].url})),policyKnowledge:{status:unavailable?'partial':'verified',sources,checkedAt:sources.length?Math.min(...sources.map(x=>x.checkedAt)):null},policyUnavailable:unavailable};
  }
  return {read,answer};
}
module.exports={POLICIES,MAX_HTML_BYTES,policyBlocks,timing,parseShipping,parseRefund,countryIn,classify,shippingAnswer,refundAnswer,policyCoverage,unavailableAnswer,createPolicyGuide,meaningContextRequest,meaningContextReply};
