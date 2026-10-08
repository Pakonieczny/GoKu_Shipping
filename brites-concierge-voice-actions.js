(function(root,factory){'use strict';var api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesConciergeVoiceActions=api;})(typeof window!=='undefined'?window:globalThis,function(){'use strict';
  // Deterministic shopper authority only. No model, storage, navigation or cart
  // operation is performed here. Explicit page/option requests can navigate;
  // the host rechecks live data and keeps bag confirmation with the shopper.
  var PRODUCT=/^gid:\/\/shopify\/Product\/\d+$/,VARIANT=/^gid:\/\/shopify\/ProductVariant\/\d+$/,HANDLE=/^[a-z0-9_-]{1,180}$/i;
  function text(value,max){return typeof value==='string'&&!/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(value)?value.slice(0,max):'';}
  function norm(value){return text(value,2000).toLowerCase().normalize('NFKC').replace(/[’']/g,'').replace(/[^a-z0-9]+/g,' ').trim();}
  function contains(message,phrase){var p=norm(phrase);return !!p&&(' '+norm(message)+' ').includes(' '+p+' ');}
  function ownURL(value,handle){try{if(typeof value!=='string'||!HANDLE.test(handle||''))return null;var u=new URL(value);if(u.protocol!=='https:'||u.username||u.password||!['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname.toLowerCase())||u.port)return null;var path=u.pathname.match(/^\/(?:[a-z]{2}(?:-[a-z]{2})?\/)?products\/([a-z0-9_-]{1,180})\/?$/i);return path&&path[1]===handle?{key:u.hostname.toLowerCase().replace(/^www\./,'')+u.pathname.replace(/\/$/,''),href:u.href,handle:handle}:null;}catch{return null;}}
  function customizer(product,variant){var value=[product.title,variant.title,...variant.options.map(function(o){return o.name+' '+o.value;})].join(' ');return /handwrit|photo|upload|custom studio|monogram|personaliz|engraved|engraving(?!\s*(?:none|no|without))/i.test(value)&&!(/\b(?:none|no engraving|without engraving|not engraved)\b/i.test(variant.title+' '+variant.options.map(function(o){return o.value;}).join(' '))&&!/handwrit|photo|upload|custom studio|monogram/i.test(product.title));}
  function ordinaryCharm(product){return /^charms?(?:[ -]only)?$/i.test(String(product?.type||'').trim())&&!/\b(?:custom|personalized|personalised|engraving|design fee|chain extender|components?|add[ -]?ons?)\b/i.test(String(product?.title||'')+' '+String(product?.type||''));}
  function cleanVariant(value){if(!value||!VARIANT.test(value.id||'')||typeof value.numericId!=='string'||!/^\d+$/.test(value.numericId)||value.id.split('/').pop()!==value.numericId||!Number.isFinite(value.price)||value.price<0||typeof value.available!=='boolean')return null;var title=text(value.title,300);if(!title)return null;var options=Array.isArray(value.options)?value.options.slice(0,12).map(function(o){return {name:text(o?.name,120),value:text(o?.value,300)};}):[];if(options.some(function(o){return !o.name||!o.value;}))return null;return {id:value.id,numericId:value.numericId,title:title,price:value.price,available:value.available,...(value.availabilityKnown===false?{availabilityKnown:false}:{}),options:options};}
  function projectProduct(live,displayed,options){try{
    var readOnly=options?.readOnly===true;
    if(!live||!displayed||!PRODUCT.test(live.id||'')||live.id!==displayed.id||!HANDLE.test(live.handle||'')||live.handle!==displayed.handle||!ownURL(live.url,live.handle)||!ownURL(displayed.url,displayed.handle)||ownURL(live.url,live.handle).key!==ownURL(displayed.url,displayed.handle).key||!/^[A-Z]{3}$/.test(live.currency||'')||!readOnly&&(live.cartHold===true||live.recommendationHold===true||live.partsOnly===true&&!ordinaryCharm(live))||!Array.isArray(live.variants)||!live.variants.length)return null;
    var variants=live.variants.slice(0,250).map(cleanVariant);if(variants.some(function(v){return !v;}))return null;var available=variants.filter(function(v){return v.available;});if(!readOnly&&!available.length||new Set(variants.map(function(v){return v.id;})).size!==variants.length)return null;
    var title=text(live.title,300);if(!title)return null;
    return {id:live.id,handle:live.handle,url:ownURL(live.url,live.handle).href,title:title,type:text(live.type,120),currency:live.currency,minPrice:Math.min.apply(null,(available.length?available:variants).map(function(v){return v.price;})),variants:variants,variantsComplete:live.variantsComplete===true&&live.variants.length<=250,cartHold:live.cartHold===true,recommendationHold:live.recommendationHold===true,partsOnly:live.partsOnly===true};
  }catch{return null;}}
  var ORDINALS={first:0,second:1,third:2,fourth:3,fifth:4,sixth:5,'1st':0,'2nd':1,'3rd':2,'4th':3,'5th':4,'6th':5};
  function selectedProduct(message,products,selectedProductId){
    var raw=text(message,2000),normalized=norm(raw),targets=new Set(),tokens=normalized.split(' '),explicit=false;
    tokens.forEach(function(token){if(Object.prototype.hasOwnProperty.call(ORDINALS,token)){explicit=true;var p=products[ORDINALS[token]];if(p)targets.add(p.id);else targets.add('unavailable');}});
    var urls=raw.match(/https?:\/\/[^\s<>"']+/gi)||[];if(urls.length){explicit=true;urls.forEach(function(url){url=url.replace(/[),.;!?]+$/,'');var matched=products.filter(function(p){return ownURL(url,p.handle);});if(matched.length===1)targets.add(matched[0].id);else targets.add('unavailable');});}
    products.forEach(function(p){if(p.title&&contains(raw,p.title)){explicit=true;targets.add(p.id);}});
    if(targets.size===1&&!targets.has('unavailable'))return products.find(function(p){return p.id===targets.values().next().value;})||null;
    if(explicit)return null;
    // Pronouns do not silently choose the first of a multi-product selection.
    if(!/\b(?:it|this|that|the piece|the necklace|the bracelet|the ring|the earrings)\b/.test(normalized))return null;
    if(selectedProductId){var selected=products.filter(function(p){return p.id===selectedProductId;});return selected.length===1?selected[0]:null;}
    return products.length===1?products[0]:null;
  }
  function valueMentioned(message,option,variant){
    var value=norm(option.value),name=norm(option.name),m=norm(message);
    if(contains(m,value))return true;
    if(value==='none'&&/engraving|personalization|personalisation/.test(name))return /\b(?:no engraving|without engraving|plain|not engraved)\b/.test(m);
    if(value==='sterling silver')return /\bsterling\b/.test(m);
    var length=value.match(/^(14|16|18|20|22|24) (?:inch|inches)$/);if(length){var words={'14':'fourteen','16':'sixteen','18':'eighteen','20':'twenty','22':'twenty two','24':'twenty four'};return contains(m,length[1]+' inch')||contains(m,length[1]+' inches')||contains(m,words[length[1]]+' inch')||contains(m,words[length[1]]+' inches');}
    return false;
  }
  function variantMentioned(message,variant,product){
    if(contains(message,variant.title))return true;
    if(product.variants.length===1&&/^(?:default title|default)$/i.test(variant.title))return true;
    return variant.options.length>0&&variant.options.every(function(option){return valueMentioned(message,option,variant);});
  }
  function refused(reason){return {ok:false,reason:reason};}
  function affirmativeCommand(message){
    var value=norm(message).replace(/^(?:(?:yes|okay|ok|thanks|thank you|great|sure)\s+){0,3}/,'');
    // A question asking for advice is not an instruction to prepare a bag
    // review. Keep ordinary polite imperatives, but no historical reports or
    // emotional, hypothetical and deliberative framing.
    value=value.replace(/^(?:(?:please|can you|could you|would you|will you|i want you to|i would like you to|id like you to|i want to|i would like to|id like to|can i see)\s+){0,3}/,'');
    return /^(?:add|put|place|review|open|view|visit|take me to|go to|show|choose|see|select|pick)\b/.test(value);
  }
  function resolveRequest(args){try{
    if(!args||typeof args!=='object'||Array.isArray(args)||!['view','options','review'].includes(args.action)||!HANDLE.test(args.handle||'')||typeof args.message!=='string'||!args.message.trim()||args.message.length>2000)return refused('INVALID_REQUEST');
    var message=args.message,normalized=norm(message),products=Array.isArray(args.products)?args.products.slice(0,6):[];
    if(/^(?:"[^"\n]+"|'[^'\n]+'|“[^”\n]+”|‘[^’\n]+’)\s*[.!?]?\s*$/.test(message.trim()))return refused('NOT_A_CURRENT_COMMAND');
    if(!products.length||products.some(function(p){return !PRODUCT.test(p?.id||'')||!ownURL(p?.url,p?.handle)||p.cartHold||p.recommendationHold||p.partsOnly&&!ordinaryCharm(p);})||new Set(products.map(function(p){return p.id;})).size!==products.length)return refused('NO_CURRENT_SELECTION');
    if(/\b(?:dont|do not|never|stop|cancel|not now|not yet)\b/.test(normalized)||/\bwithout (?:adding|opening|viewing|choosing)\b/.test(normalized))return refused('NEGATED_REQUEST');
    if(/\b(?:if|later|tomorrow|yesterday|already|previously|hypothetically|pretend|imagine|example|quoted|quote|she said|he said|they said|you said|tell me how|how do|what does|ignore previous|ignore the rules|system prompt|developer instructions)\b/.test(normalized))return refused('NOT_A_CURRENT_COMMAND');
    if(/\b(?:buy|purchase|pay|checkout|check out|place an order)\b/.test(normalized))return refused('CHECKOUT_REQUIRES_CUSTOMER');
    if(!affirmativeCommand(message))return refused('NO_ACTION_AUTHORITY');
    var bag=/\b(?:add|put|place)\b/.test(normalized)&&/\b(?:bag|cart)\b/.test(normalized),review=/\breview\b/.test(normalized)&&/\b(?:add|adding|bag|cart)\b/.test(normalized),options=/\b(?:show|open|choose|see|select|pick)\b/.test(normalized)&&/\b(?:options|variants|lengths|metals|choices)\b/.test(normalized),view=(/\b(?:open|view|visit|take me to|go to)\b/.test(normalized)||/\bshow\b/.test(normalized)&&/\b(?:page|product page)\b/.test(normalized))&&!options;
    if((args.action==='view'&&!view)||(args.action==='options'&&!options&&!bag&&!review)||(args.action==='review'&&!bag&&!review))return refused('NO_ACTION_AUTHORITY');
    var selected=selectedProduct(message,products,args.selectedProductId);if(!selected||selected.handle!==args.handle)return refused('AMBIGUOUS_SELECTION');
    if(args.action!=='review'){if(args.variantId!==undefined)return refused('UNEXPECTED_VARIANT');return {ok:true,action:args.action,productId:selected.id,handle:selected.handle};}
    if(!VARIANT.test(args.variantId||'')||selected.variantsComplete!==true||!Array.isArray(selected.variants))return refused('EXACT_OPTIONS_REQUIRED');
    var candidates=selected.variants.filter(function(v){return v.available===true&&variantMentioned(message,v,selected);});
    if(candidates.length!==1||candidates[0].id!==args.variantId)return refused('EXACT_OPTIONS_REQUIRED');
    if(customizer(selected,candidates[0])||/\bcharms?\b/i.test(selected.type||'')&&!ordinaryCharm(selected)||/\b(?:components?|add[ -]?ons?|custom[ -]?only)\b/i.test(selected.type||''))return refused('PERSONALIZATION_REQUIRES_PRODUCT_PAGE');
    return {ok:true,action:'review',productId:selected.id,handle:selected.handle,variantId:candidates[0].id};
  }catch{return refused('INVALID_REQUEST');}}
  function resolveStorefrontRequest(args,bridge){if(!args||args.currentTurn!==true||typeof args.message!=='string'||!bridge||typeof bridge.resolve!=='function')return refused('NO_ACTION_AUTHORITY');var result=bridge.resolve(args.message,args.context);return result&&result.ok?result:refused('NO_ACTION_AUTHORITY');}
  return {resolveRequest:resolveRequest,projectProduct:projectProduct,resolveStorefrontRequest:resolveStorefrontRequest};
});
