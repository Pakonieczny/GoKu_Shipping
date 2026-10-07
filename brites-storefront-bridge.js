(function(root,factory){'use strict';var api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesStorefrontBridge=api;})(typeof window!=='undefined'?window:globalThis,function(){
  'use strict';
  // Website awareness stays local. A model may suggest an action, but only the
  // current shopper's own request can authorize this bounded sandbox bridge.
  var HANDLE=/^[a-z0-9]+(?:-[a-z0-9]+)*$/, PRODUCT=/^gid:\/\/shopify\/Product\/[1-9][0-9]{0,19}$/, VARIANT=/^gid:\/\/shopify\/ProductVariant\/[1-9][0-9]{0,19}$/;
  var SORTS=['featured','price-asc','price-desc','title-asc','title-desc'];
  var FILTERS=['all','necklaces','earrings','bracelets','rings','charms','available'];
  var SECTIONS=['price','details','options','story','shipping','gifts','customize','catalogue','image','bag','checkout','offers'];
  var LEGACY_TYPES=['search','sort','filter','open','highlight','zoom','scroll','bag','checkout','gift','customize'];
  var CONTROL_TYPES=['options','select-option','product-quantity','review-add','bag-quantity','bag-remove','gift-preferences','checkout-step','checkout-option','checkout-complete'];
  var TYPES=LEGACY_TYPES.concat(CONTROL_TYPES), LINE=/^[a-zA-Z0-9][a-zA-Z0-9:_-]{0,199}$/;
  var PRODUCT_SECTIONS=['price','details','options','story','image'];
  function plain(value,max){return typeof value==='string'&&value.length<=max&&!/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/.test(value)?value.trim():'';}
  function norm(value){return plain(value,2000).normalize('NFKC').toLowerCase().replace(/[’']/g,'').replace(/[^a-z0-9]+/g,' ').trim();}
  function has(message,phrase){var n=norm(phrase);return !!n&&(' '+message+' ').includes(' '+n+' ');}
  function handle(value){return typeof value==='string'&&value.length<=180&&HANDLE.test(value)?value:'';}
  function revision(value){return Number.isSafeInteger(value)&&value>=0?value:typeof value==='string'&&/^[a-zA-Z0-9_.:-]{1,120}$/.test(value)?value:null;}
  function literal(value,max){return plain(value,max).normalize('NFKC').toLowerCase().replace(/\s+/g,' ').trim();}
  function identityPieces(values){var seen=new Set();return (Array.isArray(values)?values:[]).slice(0,200).flatMap(function(p){var h=handle(p&&p.handle),title=plain(p&&p.title,300);if(!h||!title||seen.has(h))return [];seen.add(h);var out={handle:h,title:title};if(PRODUCT.test(p.id||''))out.id=p.id;return [out];});}
  function capabilities(value){if(!value||value.controlVersion!==1||!['sandbox','shopify'].includes(value.mode)||!Array.isArray(value.actions))return null;return {controlVersion:1,mode:value.mode,actions:Array.from(new Set(value.actions.filter(function(v){return TYPES.includes(v);}))),reviewBeforeAdd:value.reviewBeforeAdd===true,finalOrder:false,checkoutMode:value.mode==='sandbox'&&value.checkoutMode==='simulation-only'?'simulation-only':'handoff',notesMode:value.notesMode==='local-session'?'local-session':'unsupported'};}
  function snapshot(value){
    value=value&&typeof value==='object'?value:{};
    var out={contextRevision:revision(value.contextRevision),discoveryRevision:Number.isSafeInteger(value.discoveryRevision)&&value.discoveryRevision>=0?value.discoveryRevision:0,pageKind:plain(value.pageKind||value.page,40)||'unknown',currentHandle:handle(value.currentHandle),focusedHandle:handle(value.focusedHandle),visiblePieces:identityPieces(value.visiblePieces),search:plain(value.search===undefined?value.query:value.search,180),sort:SORTS.includes(value.sort)?value.sort:'featured',filter:FILTERS.includes(value.filter)?value.filter:'all',loading:value.loading===true,activeSection:SECTIONS.includes(value.activeSection)?value.activeSection:''};
    if(Array.isArray(value.loadedPieces))out.loadedPieces=identityPieces(value.loadedPieces);
    if(value.controlVersion!==1)return out;
    out.controlVersion=1;var pc=value.productControls;
    if(pc&&handle(pc.handle)&&PRODUCT.test(pc.productId||'')&&Number.isInteger(pc.quantity)&&pc.quantity>=1&&pc.quantity<=20){
      var groups=(Array.isArray(pc.optionGroups)?pc.optionGroups:[]).slice(0,12).flatMap(function(g){var name=plain(g&&g.name,120),seen=new Set(),values=(Array.isArray(g&&g.values)?g.values:[]).slice(0,100).flatMap(function(v){var clean=plain(v,300),key=literal(clean,300);if(!clean||seen.has(key))return [];seen.add(key);return [clean];});return name&&values.length?[{name:name,values:values}]:[];});
      if(new Set(groups.map(function(g){return literal(g.name,120);})).size===groups.length){var selected=(Array.isArray(pc.selectedOptions)?pc.selectedOptions:[]).slice(0,12).flatMap(function(v){var group=groups.find(function(g){return literal(g.name,120)===literal(v&&v.name,120);}),values=group&&group.values.filter(function(x){return literal(x,300)===literal(v&&v.value,300);});return values&&values.length===1?[{name:group.name,value:values[0]}]:[];});
        out.productControls={handle:pc.handle,productId:pc.productId,variantId:VARIANT.test(pc.variantId||'')?pc.variantId:null,quantity:pc.quantity,optionsOpen:pc.optionsOpen===true,openedOption:groups.some(function(g){return g.name===pc.openedOption;})?pc.openedOption:null,optionGroups:groups,selectedOptions:selected,reviewReady:pc.reviewReady===true};
      }
    }
    var bag=value.bagControls;if(bag&&Array.isArray(bag.lines)){var seenLines=new Set(),lines=bag.lines.slice(0,50).flatMap(function(l){var lineId=plain(l&&l.lineId,200);if(!LINE.test(lineId)||seenLines.has(lineId)||!PRODUCT.test(l.productId||'')||!VARIANT.test(l.variantId||'')||!Number.isInteger(l.quantity)||l.quantity<1||l.quantity>20||!plain(l.title,300))return [];seenLines.add(lineId);return [{lineId:lineId,productId:l.productId,variantId:l.variantId,quantity:l.quantity,title:plain(l.title,300),variant:plain(l.variant,300)}];});out.bagControls={lines:lines,itemCount:lines.reduce(function(n,l){return n+l.quantity;},0)};}
    var cc=value.checkoutControls;if(cc&&[null,'review','shipping','confirm','complete'].includes(cc.step)&&['standard','express'].includes(cc.shipping))out.checkoutControls={step:cc.step,shipping:cc.shipping,acknowledged:cc.acknowledged===true,complete:cc.complete===true};
    return out;
  }
  function failed(reason,recognized){return {ok:false,handled:recognized===true,recognized:recognized===true,reason:reason};}
  function success(action,context){return {ok:true,handled:true,recognized:true,action:Object.freeze(action),contextRevision:context.contextRevision};}
  function validateAction(value){
    if(!value||typeof value!=='object'||Array.isArray(value)||!TYPES.includes(value.type))return null;
    var keys={search:['type','query','sort','filter'],sort:['type','sort'],filter:['type','filter'],open:['type','handle'],highlight:['type','handle','section'],zoom:['type','handle'],scroll:['type','handle','section'],bag:['type'],checkout:['type'],gift:['type','section'],customize:['type','handle','section'],options:['type','handle','optionName'],'select-option':['type','handle','variantId','optionName','optionValue'],'product-quantity':['type','handle','quantity'],'review-add':['type','handle','variantId'],'bag-quantity':['type','lineId','quantity'],'bag-remove':['type','lineId'],'gift-preferences':['type','wrapping','giftPackage','giftNote'],'checkout-step':['type','step'],'checkout-option':['type','option','value'],'checkout-complete':['type']}[value.type];
    if(Object.keys(value).some(function(k){return !keys.includes(k);}))return null;
    var out={type:value.type};
    if(value.type==='search'){var q=plain(value.query,180);if(!q)return null;out.query=q;}
    if(value.type==='sort'&&!SORTS.includes(value.sort)||value.sort!==undefined&&!SORTS.includes(value.sort))return null;
    if(value.sort!==undefined)out.sort=value.sort;
    if(value.type==='filter'&&!FILTERS.includes(value.filter)||value.filter!==undefined&&!FILTERS.includes(value.filter))return null;
    if(value.filter!==undefined)out.filter=value.filter;
    if(value.handle!==undefined){if(!handle(value.handle))return null;out.handle=value.handle;}
    if(['open','zoom'].includes(value.type)&&!out.handle)return null;
    if(value.section!==undefined){if(!SECTIONS.includes(value.section))return null;out.section=value.section;}
    if(['highlight','scroll','gift','customize'].includes(value.type)&&!out.section)return null;
    if(value.type==='gift'&&out.section!=='gifts'||value.type==='customize'&&!['customize','options'].includes(out.section))return null;
    if(['highlight','scroll'].includes(value.type)&&PRODUCT_SECTIONS.includes(out.section)&&!out.handle)return null;
    if(['options','select-option','product-quantity','review-add'].includes(value.type)&&!out.handle)return null;
    if(value.variantId!==undefined){if(!VARIANT.test(value.variantId||''))return null;out.variantId=value.variantId;}
    if(value.optionName!==undefined){var name=plain(value.optionName,120);if(!name)return null;out.optionName=name;}
    if(value.optionValue!==undefined){var val=plain(value.optionValue,300);if(!val)return null;out.optionValue=val;}
    if(value.type==='select-option'&&((!!out.variantId)===(!!out.optionName&&!!out.optionValue)||out.variantId&&(out.optionName||out.optionValue)||(!out.variantId&&(!out.optionName||!out.optionValue))))return null;
    if(['product-quantity','bag-quantity'].includes(value.type)){if(!Number.isInteger(value.quantity)||value.quantity<1||value.quantity>20)return null;out.quantity=value.quantity;}
    if(['bag-quantity','bag-remove'].includes(value.type)){if(!LINE.test(value.lineId||''))return null;out.lineId=value.lineId;}
    if(value.type==='gift-preferences'){if(!['wrapping','giftPackage','giftNote'].some(function(k){return Object.hasOwn(value,k);}))return null;for(var k of ['wrapping','giftPackage'])if(Object.hasOwn(value,k)){if(typeof value[k]!=='boolean')return null;out[k]=value[k];}if(Object.hasOwn(value,'giftNote')){if(typeof value.giftNote!=='string'||value.giftNote.length>300||/[\u0000-\u001f\u007f\u202a-\u202e\u2066-\u2069]/.test(value.giftNote))return null;out.giftNote=value.giftNote;}}
    if(value.type==='checkout-step'){if(!['review','shipping','confirm'].includes(value.step))return null;out.step=value.step;}
    if(value.type==='checkout-option'){if(value.option!=='shipping'||!['standard','express'].includes(value.value))return null;out.option=value.option;out.value=value.value;}
    return out;
  }
  function sameAction(a,b){return !!a&&!!b&&JSON.stringify(Object.keys(a).sort().map(function(k){return [k,a[k]];}))===JSON.stringify(Object.keys(b).sort().map(function(k){return [k,b[k]];}));}
  var ORDINALS={first:0,second:1,third:2,fourth:3,fifth:4,sixth:5,seventh:6,eighth:7,ninth:8,tenth:9,'1st':0,'2nd':1,'3rd':2,'4th':3,'5th':4,'6th':5,'7th':6,'8th':7,'9th':8,'10th':9};
  var GENERIC=new Set(['necklace','necklaces','earring','earrings','bracelet','bracelets','ring','rings','charm','charms','pendant','pendants','jewelry','jewellery','the','a','an','and','with','for','in','of','silver','gold','sterling','inch','inches','chain','chains','piece','pieces','product','products']);
  var TARGET_STOP=new Set('please could would can will you your our i me my want to like see show tell display pull up open view take go current selected what whats is are does do have has how much price prices cost costs worth material materials made from metal metals details process finish finishes meaning mean means symbol symbolism story stories represents options variants sizes size length lengths available choices image photo picture zoom enlarge magnify make bigger larger its it this that here used use on about page listing item one nice pretty beautiful custom design designs customize customise personalize personalise personalization engrave engraved engraving'.split(' '));
  function target(message,context,implicit){
    var exact=new Set(),ordinal=new Set();
    Object.keys(ORDINALS).forEach(function(word){if(has(message,word)){var p=context.visiblePieces[ORDINALS[word]];if(p)ordinal.add(p.handle);else ordinal.add('');}});
    context.visiblePieces.concat(context.loadedPieces||[]).forEach(function(p){if(has(message,p.title)||has(message,p.handle))exact.add(p.handle);});
    if(!exact.size){context.visiblePieces.forEach(function(p){var words=Array.from(new Set(norm(p.title+' '+p.handle).split(' ').filter(function(w){return w.length>2&&!GENERIC.has(w)&&!TARGET_STOP.has(w)&&!/^\d+$/.test(w);})));if(words.some(function(w){return has(message,w);}))exact.add(p.handle);});}
    if(exact.size>1||ordinal.size>1||exact.size&&ordinal.size&&Array.from(exact)[0]!==Array.from(ordinal)[0])return {reason:'More than one piece matches. Choose a listing or give its exact title.'};
    if(exact.size)return {handle:Array.from(exact)[0],match:'named'};
    if(ordinal.size){var h=Array.from(ordinal)[0];return h?{handle:h,match:'ordinal'}:{reason:'That option is not in the current visible selection.'};}
    var pointed=/\b(?:hover|hovering|pointing|under my (?:mouse|cursor)|looking at)\b/.test(message);
    var pronoun=/\b(?:it|this|that|current|selected|the piece|the product|the listing|the image|the photo)\b/.test(message);
    if(pointed)return context.focusedHandle?{handle:context.focusedHandle,match:'pointed'}:{reason:'Point to or choose a listing first.'};
    var unnamedTarget=message.split(' ').some(function(w){return w&&!GENERIC.has(w)&&!TARGET_STOP.has(w)&&!/^\d+$/.test(w);});
    if((pronoun||implicit)&&!unnamedTarget){var scoped=context.pageKind==='product'?context.currentHandle||context.focusedHandle:context.focusedHandle||context.currentHandle;if(scoped)return {handle:scoped,match:'scope'};if(context.visiblePieces.length===1)return {handle:context.visiblePieces[0].handle,match:'only'};}
    return {reason:'Choose a listing or give its exact title so I can use the correct piece.'};
  }
  function category(message){var cats=[];[['necklaces',/\bnecklaces?\b/],['earrings',/\bearrings?\b/],['bracelets',/\bbracelets?\b/],['rings',/\brings?\b/],['charms',/\bcharms?\b/]].forEach(function(pair){if(pair[1].test(message))cats.push(pair[0]);});return cats.length===1?cats[0]:'';}
  function collectionReset(message){
    var command=message.replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*/,'').replace(/\s+(?:please|again)$/,'');
    var collection='(?:the )?(?:(?:previous|full|whole|entire|complete|original|unfiltered|main|all) )?(?:list|catalogue|catalog|collection|collections|shop|results|pieces|jewelry|jewellery)';
    return new RegExp('^(?:(?:go|take me) back(?: to '+collection+')?|return(?: me)? to '+collection+'|(?:show(?: me)?|open|view|display|pull up|let me see) '+collection+')$').test(command)||/^(?:show(?: me)? all|show(?: me)? everything|start (?:over|fresh)|reset(?: the)? (?:search|collection|catalogue|catalog)|clear(?: the)? search(?: results)?)$/.test(command);
  }
  function categoryBrowse(message){
    var command=message.replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*/,'').replace(/\s+(?:please|again)$/,'');
    var noun='(?:necklaces?|earrings?|bracelets?|rings?|charms?)';
    // A bare category is a collection request, not an exact listing identity.
    // Keep this grammar narrow so names, ordinals, meanings and advice retain
    // their existing target/model routes and cannot acquire browse authority.
    if(new RegExp('^(?:open|view) (?:all (?:your |the )?|your |the |some |a |an )?'+noun+'$').test(command)||
       new RegExp('^(?:what|which) (?:kinds? of |types? of )?'+noun+' do you (?:have|offer)$').test(command)||
       new RegExp('^(?:do you (?:have|offer)|can i (?:see|browse|view)) (?:any |some |the |your )?'+noun+'$').test(command))return category(command);
    return '';
  }
  function sortFor(message){if(/\b(?:highest|expensive|priciest|descending|high to low|most expensive)\b/.test(message))return 'price-desc';if(/\b(?:cheapest|lowest|least expensive|low to high|affordable first|ascending price)\b/.test(message))return 'price-asc';if(/\b(?:z to a|reverse alphabetical|reverse alphabetically)\b/.test(message))return 'title-desc';if(/\b(?:alphabetic|alphabetical|alphabetically|a to z|by (?:name|title))\b/.test(message))return 'title-asc';if(/\b(?:featured|recommended order|default order|reset sort)\b/.test(message))return 'featured';return '';}
  function sectionFor(message){if(/\b(?:shipping|delivery|production|processing|dispatch)\b/.test(message))return 'shipping';if(/\b(?:gift wrapping|gift wrap|gift packaging|gift package|gift packages|gift note|gift notes|gift message)\b/.test(message))return 'gifts';if(/\b(?:discount|discounts|coupon|coupons|promo|promotions|offer|offers|sale|codes?)\b/.test(message))return 'offers';if(/\b(?:meaning|mean|means|symbol|symbolism|story|stories|represents)\b/.test(message))return 'story';if(/\b(?:material|materials|made of|made from|metal|metals|details|process|finish|finishes)\b/.test(message))return 'details';if(/\b(?:price|prices|cost|costs|how much)\b/.test(message))return 'price';if(/\b(?:options|variants|sizes|size|length|lengths|chain length|available choices)\b/.test(message))return 'options';if(/\b(?:image|photo|picture)\b/.test(message))return 'image';if(/\b(?:catalogue|catalog|collection|collections|results)\b/.test(message))return 'catalogue';return '';}
  function controlTarget(message,context){var named=target(message,context,false);if(named.handle&&named.match==='named')return named;var pc=context.productControls;if(pc&&context.pageKind==='product'&&context.currentHandle===pc.handle&&!/\b(?:for|of) (?!the (?:current|selected)\b|(?:this|that|it|current|selected)\b).+/.test(message))return {handle:pc.handle,match:'scope'};return named;}
  function bagTarget(message,context){
    var lines=context.bagControls&&context.bagControls.lines||[],named=lines.filter(function(l){return has(message,l.title);});
    if(named.length){var qualified=named.filter(function(l){var rest=message.replace(norm(l.title),'');if(l.variant&&has(rest,l.variant))rest=rest.replace(norm(l.variant),'');return !rest.replace(/\b(?:the|my|a|an|this|that|current|selected|item|piece)\b/g,'').trim();});return qualified.length===1?qualified[0]:null;}
    // Only an otherwise unnamed ordinal or pronoun can select a visible line.
    // An unknown title/option never borrows the only line in the bag.
    var ord=Object.keys(ORDINALS).filter(function(w){return has(message,w);}),rest=message;ord.forEach(function(w){rest=rest.replace(w,'');});
    if(rest.replace(/\b(?:the|my|a|an|it|this|that|current|selected|item|piece|quantity|bag|cart|basket)\b/g,'').trim())return null;
    if(ord.length===1)return lines[ORDINALS[ord[0]]]||null;
    return !ord.length&&lines.length===1&&/\b(?:it|this|that|item|piece|quantity|bag|cart|basket)\b/.test(message)?lines[0]:null;
  }
  function optionGroup(words,pc){
    var exact=pc.optionGroups.filter(function(g){return has(words,g.name);});
    // A full published label can contain a shorter group's label. Remove only
    // the nested mention; separately requested groups still need clarification.
    exact=exact.filter(function(g){return !exact.some(function(other){return other!==g&&has(norm(other.name),g.name)&&!has(words.replace(norm(other.name),''),g.name);});});
    if(exact.length>1)return false;
    var families=[[/\b(?:material|materials|metal|metals)\b/,/^(?:metal(?: choice)?|material|materials|finish)$/i],[/\bsizes?\b/,/^(?:size|hoop size)$/i],[/\blengths?\b/,/^(?:length|chain length|necklace length)$/i]].filter(function(pair){return pair[0].test(words);});
    if(families.length>1)return false;
    if(exact.length===1&&norm(exact[0].name).split(' ').length>1)return exact[0];
    if(families.length){var matches=pc.optionGroups.filter(function(g){return families[0][1].test(g.name);});if(matches.length>1)return false;if(matches.length===1)return matches[0];}
    return exact.length===1?exact[0]:null;
  }
  function productQuantityTarget(words,context){
    var pc=context.productControls;if(!pc||context.pageKind!=='product'||context.currentHandle!==pc.handle)return null;
    if(!words||/^(?:(?:the|my) )?(?:it|its|this|that|current|selected|(?:(?:this|that|current|selected) )?(?:piece|product|listing|item))$/.test(words))return {handle:pc.handle};
    var matches=context.visiblePieces.concat(context.loadedPieces||[]).filter(function(p){return has(words,p.title)||has(words,p.handle);}),handles=new Set(matches.map(function(p){return p.handle;}));
    if(handles.size!==1||!handles.has(pc.handle))return null;
    var named=matches.find(function(p){return p.handle===pc.handle;}),rest=words.replace(has(words,named.title)?norm(named.title):norm(named.handle),'');
    pc.selectedOptions.forEach(function(choice){if(has(rest,choice.value))rest=rest.replace(norm(choice.value),'');});
    if(rest.replace(/\b(?:the|my|a|an|this|that|current|selected|piece|product|listing|item)\b/g,'').trim())return null;
    return {handle:pc.handle};
  }
  function controls(raw,message,context){
    var command=message.replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*/,''),enhanced=context.controlVersion===1,pc=context.productControls,t;
    // Recognize setters even on an older adapter. Never interpret a rejected
    // quantity or option request as a motif recommendation/search.
    var rawCommand=raw.trim().replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|i'd like (?:you )?to)\s+)*/i,'');
    var reversedQuantity=/^(?:set|change|update|make)(?: the| my)? quantity (?:of|for) (.+?) to ([^\s]+)\s*$/i.exec(rawCommand);
    var directQuantity=/^(?:set|change|update|make)(?: the| my)? (?:(.+?) )?quantity(?: to| of)? ([^\s]+)(?: (?:for|of) (.+))?\s*$/i.exec(rawCommand);
    var quantity=reversedQuantity?reversedQuantity[2]:directQuantity?directQuantity[2]:null;
    if(quantity!==null){if((command.match(/\bquantity\b/g)||[]).length!==1||/\b(?:or|instead|except)\b/.test(command))return failed('Give one exact quantity request.',true);if(!/^\d+[.!?]?$/.test(quantity))return failed('Choose a whole quantity from 1 to 20.',true);var n=Number(quantity.replace(/[.!?]$/,''));if(!enhanced)return failed('Quantity controls are unavailable on this page. Use the visible quantity control.',true);if(!Number.isInteger(n)||n<1||n>20)return failed('Choose a whole quantity from 1 to 20.',true);var quantityWords=norm(reversedQuantity?reversedQuantity[1]:directQuantity[3]||directQuantity[1]||'');if(directQuantity&&directQuantity[1]&&directQuantity[3]&&!/^(?:(?:the|my) )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket)$/.test(norm(directQuantity[1]))&&!has(quantityWords,directQuantity[1]))return failed('Give one exact product or bag line for the quantity change.',true);if(/\b(?:bag|cart|basket)\b/.test(command)||context.pageKind==='bag'){var line=bagTarget(quantityWords||'quantity',context);return line?success({type:'bag-quantity',lineId:line.lineId,quantity:n},context):failed('Choose the exact bag line and quantity.',true);}t=productQuantityTarget(quantityWords,context);return t?success({type:'product-quantity',handle:t.handle,quantity:n},context):failed('Open the exact product before changing its quantity.',true);}
    if(/^(?:set|change|update|make)(?: the| my)? (?:[a-z0-9 ]{1,300} )?quantity\b/.test(command))return failed('Choose a whole quantity from 1 to 20.',true);
    var giftChoice=/^(enable|disable|add|remove|use|choose|turn on|turn off) (.+)$/.exec(command);
    if(giftChoice&&/\b(?:gift (?:wrapping|wrap|package|packaging)|wrapping)\b/.test(giftChoice[2])){if(!enhanced)return failed('Gift preferences are unavailable on this page. Use the visible gift controls.',true);var on=!/^(?:disable|remove|turn off)$/.test(giftChoice[1]),body=giftChoice[2].replace(/ (?:to|from|in) (?:my |the )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket)$/,''),parts=body.split(/\s+and\s+/),preferences={type:'gift-preferences'},valid=true;parts.forEach(function(part){var field=/^(?:the |my |a |an )?(?:gift )?(wrapping|wrap|package|packaging)$/.exec(part);if(!field){valid=false;return;}var key=/^(?:wrapping|wrap)$/.test(field[1])?'wrapping':'giftPackage';if(Object.hasOwn(preferences,key))valid=false;preferences[key]=on;});if(!valid)return failed('Give one clear wrapping or gift package request, or request both with the same choice.',true);return success(preferences,context);}
    var removeBag=/^(?:remove|delete)\b/.test(command)&&(/\b(?:bag|cart|basket)\b/.test(command)||context.pageKind==='bag');
    if(removeBag&&enhanced){var removal=/^(?:remove|delete) (.+?)(?: (?:from|in) (?:my |the )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket))?$/.exec(command),remove=removal&&bagTarget(removal[1],context);return remove?success({type:'bag-remove',lineId:remove.lineId},context):failed('Choose the exact bag line to remove; its options distinguish matching titles.',true);}
    if(!enhanced)return null;
    if(/^(?:set|write|save|add|change)(?: the| my)? gift (?:note|message)\b/.test(command)){var note=raw.trim().match(/^(?:(?:please|could you|would you|can you|will you)\s+)*(?:set|write|save|add|change)(?: the| my)? gift (?:note|message)(?:\s+to)?\s*[:=]?\s+([\s\S]+)$/i);if(!note||note[1].length>300)return failed('Give the exact gift note in 300 characters or fewer.',true);return success({type:'gift-preferences',giftNote:note[1]},context);}
    if(/^(?:add|put|review|prepare)\b/.test(command)&&/\b(?:bag|cart|basket|adding|add)\b/.test(command)){t=controlTarget(command,context);if(!t.handle||!pc||pc.handle!==t.handle||!pc.variantId||pc.selectedOptions.length!==pc.optionGroups.length)return failed('Choose every exact published option first, then review adding this piece.',true);var explicitChoice=pc.optionGroups.flatMap(function(g){return g.values.filter(function(v){return has(command,v);}).map(function(v){return {name:g.name,value:v};});});if(explicitChoice.some(function(v){return !pc.selectedOptions.some(function(s){return s.name===v.name&&s.value===v.value;});}))return failed('Those options differ from the current selection. Select the exact option before reviewing.',true);return success({type:'review-add',handle:t.handle,variantId:pc.variantId},context);}

    if(/^(?:choose|select|set|use|change)\b/.test(command)&&/\b(?:standard|express) shipping\b/.test(command)){if(context.pageKind!=='checkout')return failed('Open the labelled test checkout before choosing a demo shipping option.',true);return success({type:'checkout-option',option:'shipping',value:/\bexpress\b/.test(command)?'express':'standard'},context);}
    if(/^(?:go|open|show|move|continue|next|back|return)\b/.test(command)&&(/\bcheckout (?:review|shipping|confirm|confirmation)\b/.test(command)||/\b(?:review|shipping|confirm|confirmation) step\b/.test(command)||/^(?:next|continue)(?: to)?(?: the)? (?:step|checkout step)$/.test(command))){if(context.pageKind!=='checkout'||!context.checkoutControls)return failed('Open the labelled test checkout first.',true);var step=/\bshipping\b/.test(command)?'shipping':/\bconfirm(?:ation)?\b/.test(command)?'confirm':/\breview\b/.test(command)?'review':context.checkoutControls.step==='review'?'shipping':context.checkoutControls.step==='shipping'?'confirm':null;return step?success({type:'checkout-step',step:step},context):failed('Use the visible acknowledgement and confirmation to complete the test checkout.',true);}
    if(/^(?:complete|finish|confirm)(?: the| my)? (?:test|mock|demo|sandbox) checkout$/.test(command))return success({type:'checkout-complete'},context);
    var openOptions=/^(?:open|show|expand|display)(?: me)?(?: the| my)?\b/.test(command)&&/\b(?:options|dropdown|drop down|selector|menu|metal|material|length|size)\b/.test(command)&&!/\b(?:shipping|gift|price|details|materials for)\b/.test(command);
    if(openOptions){t=controlTarget(command,context);if(!t.handle)return failed(t.reason,true);var opened=pc&&pc.handle===t.handle?optionGroup(command,pc):null;if(opened===false)return failed('More than one published option group matches. Name its exact label.',true);if(/\b(?:metal|material|length|size)\b/.test(command)&&!opened)return failed('Open the exact product to choose a published option group.',true);return success(Object.assign({type:'options',handle:t.handle},opened?{optionName:opened.name}:{}),context);}
    if(/^(?:choose|select|pick|set|change|use)\b/.test(command)&&!/\b(?:search|sort|filter|gift|shipping|checkout|quantity)\b/.test(command)){
      if(!pc||context.pageKind!=='product'||context.currentHandle!==pc.handle)return failed('Open the exact piece before selecting a published option.',true);
      var group=optionGroup(command,pc);if(group===false)return failed('More than one published option group matches. Name its exact label.',true);var groups=group?[group]:pc.optionGroups,payload=literal(raw,2000),matches=[];
      groups.forEach(function(g){g.values.forEach(function(v){var key=literal(v,300),at=payload.indexOf(key);if(at>=0&&(at===0||!/\w/.test(payload[at-1]))&&(at+key.length===payload.length||!/\w/.test(payload[at+key.length])))matches.push({name:g.name,value:v});});});
      if(matches.length!==1)return failed('Name one exact published option value; I will keep the other choices unchanged.',true);
      return success({type:'select-option',handle:pc.handle,optionName:matches[0].name,optionValue:matches[0].value},context);
    }
    return null;
  }
  function resolveIntent(value,rawContext){
    var raw=typeof value==='string'?value:value&&value.message,context=snapshot(rawContext||value&&value.context),message=norm(raw);
    if(!plain(raw,2000))return failed('Please use a short, plain request.',false);
    if(!message)return failed('',false);
    if(/^\s*["“‘'][\s\S]*["”’']\s*$/.test(raw))return failed('Quoted words are not a current request to change the website.',true);
    var control=/\b(?:search|sort|filter|show|open|zoom|enlarge|highlight|scroll|checkout|check out|bag|cart|price|materials|options|shipping|offers|set|change|select|choose|pick|use|enable|disable|quantity|remove|delete|gift|complete|finish)\b/.test(message);
    if(/javascript\s*:|<\s*script\b|\b(?:document|window)\s*[.\[]|\b(?:eval|fetch)\s*\(|\b(?:execute|run)\s+(?:code|script|javascript)|\b(?:css selector|system prompt|developer prompt)|\b(?:ignore|override|bypass)\b.{0,100}\b(?:instructions|safety|rules|guard|previous|system)\b|https?:\/\//i.test(raw))return failed('I can use the visible shop controls, but cannot execute code, external links or override instructions.',true);
    if(/^(?:say|repeat|quote|read out|write(?! (?:the |my )?gift (?:note|message)\b)|print|explain|teach|tell me how|how (?:do|would|can) (?:i|you)|what (?:would|happens if))\b/.test(message)||/^(?:if|when|unless|once|after|suppose|imagine)\b/.test(message)||/\b(?:if|when|once|after) (?:i|you)\b/.test(message)||/\b(?:yesterday|last time|previously|tomorrow|do it later)\b/.test(message))return failed('That is not a current request to change the test website.',false);
    if(/^(?:no\b|stop\b|cancel\b|wait\b|hold\b|not now|not yet)/.test(message)||/\b(?:do not|dont|never)\s+(?:(?:please|want|need|you|to|do)\s+){0,5}(?:search|sort|filter|show|open|zoom|enlarge|highlight|scroll|checkout|check|add|place|set|select|choose|change|remove|delete|use|enable|disable|complete|finish)\b/.test(message)||/\b(?:not yet|hold off)\b/.test(message))return failed('I will leave the website as it is.',control);
    if(/\b(?:place|submit|pay for|purchase|complete)\b.{0,50}\b(?:order|payment|purchase)\b|\breal checkout\b/.test(message))return failed('This test storefront can demonstrate checkout; it cannot place or pay for a real order.',true);
    if(/\b(?:then|and)\s+(?:(?:please|can you)\s+)?(?:open|highlight|zoom|enlarge|checkout|check out|add|place|pay|scroll|select|choose|set|change|remove|delete|enable|disable|finish|complete)\b/.test(message))return failed('Ask for one website action at a time so the target remains clear.',true);
    var controlled=controls(raw,message,context);if(controlled)return controlled;
    if(/\b(?:add|put|remove|delete|empty|clear)\b.{0,100}\b(?:cart|bag|basket)\b/.test(message))return Object.assign(failed('Exact options need the existing review and Confirm flow.',false),{delegated:'review'});
    if(collectionReset(message))return success({type:'filter',filter:'all'},context);
    var browseCategory=categoryBrowse(message);if(browseCategory)return success({type:'filter',filter:browseCategory},context);
    var direct=/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*(?:search|find|look for|look up|browse|show|pull up|display|open|take me|go|view|sort|order|arrange|filter|reset|clear|highlight|scroll|zoom|enlarge|make|check|help|let me see)\b/.test(message);
    var sort=sortFor(message),cat=category(message),section=sectionFor(message),match;
    if(direct&&/\b(?:check out|checkout)\b/.test(message)||/^(?:i want to|id like to|i would like to|can i) check out\b/.test(message))return success({type:'checkout'},context);
    if(direct&&/\b(?:my|the|test|sandbox) (?:cart|bag|basket)\b/.test(message))return success({type:'bag'},context);
    if(direct&&/\b(?:sort|order|arrange)\b/.test(message)){if(!sort&&/\bby price\b/.test(message))return failed('Choose lowest price first or highest price first.',true);return sort?success({type:'sort',sort:sort},context):failed('Choose price order, alphabetical order or featured order.',true);}
    if(direct&&/\b(?:clear|reset)\b.{0,20}\bfilters?\b/.test(message))return success({type:'filter',filter:'all'},context);
    if(direct&&/\b(?:filter|only show|show only|available only|in stock only)\b/.test(message)){var filter=/\b(?:available|in stock)\b/.test(message)?'available':cat;if(!filter&&/\b(?:all|everything|reset|clear)\b/.test(message))filter='all';return filter?success({type:'filter',filter:filter},context):failed('The test shop can filter by jewelry category or current availability.',true);}
    if(direct&&/\b(?:zoom|enlarge|magnify|close up|bigger (?:image|photo|picture)|larger (?:image|photo|picture)|make (?:it|this|the (?:image|photo|picture)) (?:bigger|larger))\b/.test(message)){var zoomTarget=target(message,context,true);return zoomTarget.handle?success({type:'zoom',handle:zoomTarget.handle},context):failed(zoomTarget.reason,true);}
    var customize=/\b(?:engrave|engraving|engraved|custom designs?|custom options?|new design|my design|design my own|customize|customise|customization|customisation|personalize|personalise|personalization)\b/.test(message);
    if(customize&&(direct||/^(?:can (?:i|you)|is it possible|i want|id like|i would like)\b/.test(message))){var customTarget=target(message,context,false),customAction={type:'customize',section:'customize'};if(customTarget.handle)customAction.handle=customTarget.handle;else if(/\b(?:this|that|it|current|selected|hovering|pointing)\b/.test(message))return failed(customTarget.reason,true);return success(customAction,context);}
    var question=/^(?:what|whats|how much|how long|where|which|are|is|does|do|can i|can you tell me|tell me|show me)\b/.test(message);
    if(section&&['shipping','gifts','offers'].includes(section)&&(direct||question)){return success(section==='gifts'?{type:'gift',section:'gifts'}:{type:/\bscroll\b/.test(message)?'scroll':'highlight',section:section},context);}
    if(section&&PRODUCT_SECTIONS.includes(section)&&(direct||question)&&!/^\b(?:find|search|look for|browse)\b/.test(message)&&!/\b(?:recommend|should i|would you recommend|all (?:pieces|products)|your (?:pieces|products|jewelry|jewellery)|a gift for)\b/.test(message)){
      var specific=target(message,context,true);if(specific.handle)return success({type:section==='image'&&/\b(?:larger|bigger|zoom|enlarge)\b/.test(message)?'zoom':'highlight',handle:specific.handle,...(section==='image'&&/\b(?:larger|bigger|zoom|enlarge)\b/.test(message)?{}:{section:section})},context);
      if(direct&&/\b(?:open|scroll|highlight|zoom|enlarge)\b/.test(message)||/\b(?:this|that|it|the|current|selected|how much|price|cost)\b/.test(message))return failed(specific.reason,true);
    }
    if(direct&&/\bscroll\b/.test(message)){if(section)return success({type:'scroll',section:section},context);if(/\b(?:top|start)\b/.test(message))return success({type:'scroll',section:'catalogue'},context);return failed('Name the part to show, such as the catalogue, price, shipping or gift options.',true);}
    if(direct){
      var openTarget=target(message,context,false);
      var searchVerb=/\b(?:search|find|browse|look for|look up)\b/.test(message),broadQuery=/\b(?:under|above|over|between|for|all|some|cheapest|expensive|necklaces|earrings|bracelets|rings|charms)\b/.test(message);
      if(openTarget.handle&&/\b(?:open|view|show|pull up|display|take me|go to)\b/.test(message)&&!searchVerb&&(!broadQuery||/\b(?:open|view)\b/.test(message)))return success({type:'open',handle:openTarget.handle},context);
      if(/\b(?:open|view)\b/.test(message)&&!/\b(?:catalogue|catalog|collection|collections|shop|all|everything)\b/.test(message))return failed(openTarget.reason,true);
      if(/\b(?:catalogue|catalog|collection|collections|shop|all jewelry|all jewellery|all pieces|everything)\b/.test(message)&&!searchVerb&&!cat&&!sort)return success({type:'filter',filter:'all'},context);
      match=raw.trim().match(/^(?:(?:please|could you|would you|can you|will you|I want (?:you )?to|I would like (?:you )?to|I'd like (?:you )?to)\s+)*(?:search(?: (?:the (?:catalogue|catalog|shop|website)))?(?: for)?|find|look (?:for|up)|browse|show(?: me)?|pull up|display|let me see)\s+(.+)$/i);
      if(match){var query=match[1].replace(/^the\s+/i,'').replace(/^["“]|["”]$/g,'').replace(/\b(?:cheapest|lowest priced|most expensive|highest priced|least expensive)\b/ig,'').replace(/\b(?:sort(?:ed)? (?:by )?price (?:low to high|high to low)|from (?:low to high|high to low))\b/ig,'').replace(/\s+/g,' ').trim();
        if(!query||query.length>180)return failed('Use a search of 180 characters or fewer.',true);
        if(cat&&norm(query).replace(/\b(?:all|only|a|an|some|the|please)\b/g,'').trim().replace(/s$/,'')===cat.replace(/s$/,'')&&!sort)return success({type:'filter',filter:cat},context);
        if(/\b(?:recommend|would look|should i|good gift|best gift|help me choose|for my|for mom|for dad)\b/.test(norm(query)))return failed('',false);
        var searchAction={type:'search',query:query};if(sort)searchAction.sort=sort;if(cat)searchAction.filter=cat;return success(searchAction,context);
      }
    }
    return failed('',false);
  }
  function ownProductURL(value,h){try{var raw=typeof value==='string'&&value.match(/^https:\/\/(?:www\.)?britesjewelry\.com(\/[^?#]*)$/i),route=raw&&raw[1].match(/^\/(?:[a-zA-Z]{2}(?:-[a-zA-Z]{2})?\/)?products\/([^/]+)\/?$/),u=new URL(value);return !!raw&&!!route&&route[1]===h&&u.pathname===raw[1]&&u.protocol==='https:'&&!u.username&&!u.password&&!u.port&&['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)&&!u.search&&!u.hash;}catch{return false;}}
  function liveProducts(values){
    if(!Array.isArray(values)||values.length>60)return [];
    return values.map(function(p){
      if(!p||!PRODUCT.test(p.id||'')||!handle(p.handle)||!plain(p.title,300)||!ownProductURL(p.url,p.handle)||!/^[A-Z]{3}$/.test(p.currency||'')||!Array.isArray(p.variants)||p.variants.length>250)return null;
      var variants=p.variants.map(function(v){if(!v||!VARIANT.test(v.id||'')||!plain(v.title,300)||!Number.isFinite(v.price)||v.price<0||typeof v.available!=='boolean')return null;var out={id:v.id,title:v.title,price:v.price,available:v.available,numericId:v.id.split('/').pop()};if(Array.isArray(v.options)&&v.options.length<=12&&v.options.every(function(o){return plain(o&&o.name,120)&&plain(o&&o.value,300);}))out.options=v.options.map(function(o){return {name:o.name,value:o.value};});return out;});
      if(variants.some(function(v){return !v;})||new Set(variants.map(function(v){return v.id;})).size!==variants.length)return null;
      var out={id:p.id,handle:p.handle,title:p.title,url:p.url,currency:p.currency,variants:variants,variantsComplete:p.variantsComplete===true};
      ['description','image','imageAlt','type'].forEach(function(k){var v=plain(p[k],k==='description'?6000:2000);if(v)out[k]=v;});
      if(Number.isFinite(p.minPrice)&&p.minPrice>=0)out.minPrice=p.minPrice;
      ['cartHold','recommendationHold','partsOnly'].forEach(function(k){if(p[k]===true)out[k]=true;});return out;
    }).filter(Boolean);
  }
  function create(config){
    config=config||{};var provider=config.storefront,clock=typeof config.now==='function'?config.now:Date.now,issued=new WeakMap(),consumed=new Map(),recent=new Map(),version=0,inflight=null,destroyed=false;
    function host(){try{return typeof provider==='function'?provider():provider;}catch{return null;}}
    function read(){var sf=host();try{return snapshot(sf&&typeof sf.snapshot==='function'?sf.snapshot():null);}catch{return snapshot(null);}}
    function resolve(text,context){var result=resolveIntent(text,context||read());if(result.ok)issued.set(result.action,{text:typeof text==='string'?text:text.message,context:snapshot(context||text&&text.context||read()),used:false});return result;}
    function cancel(){version++;if(inflight){inflight.controller.abort();inflight=null;}}
    function duplicate(record){var ok=record&&record.status==='success';return {ok:ok,handled:true,recognized:true,suppressed:true,pending:record&&record.status==='pending',reply:ok?'That request has already been handled.':record&&record.status==='pending'?'That request is already being handled.':'That request was not completed. Please make a fresh request.',reason:ok?'':'The previous request is pending or was not completed.',snapshot:read()};}
    async function execute(request,options){
      options=options||{};var cap=request&&typeof request==='object'?issued.get(request):null,text=options.transcript===undefined&&cap?cap.text:options.transcript,context=snapshot(options.context||cap&&cap.context),source=options.source|| (options.inputItemId||Object.hasOwn(options,'currentTurn')||Object.hasOwn(options,'turnVersion')?'native':'typed');
      if(destroyed)return Object.assign(failed('The website bridge is closed.',true),{cancelled:true});
      if(!request||typeof request!=='object'||Array.isArray(request)||!TYPES.includes(request.type))return failed('That website control is not available.',true);
      if(options.signal&&options.signal.aborted)return Object.assign(failed('The website request was cancelled.',true),{cancelled:true});
      if(cap&&cap.used)return duplicate(cap.record);
      if(!plain(text,2000))return failed('A current shopper request is required.',true);
      if(!['typed','native'].includes(source))return failed('A current shopper request is required.',true);
      if(source==='native'&&(options.currentTurn!==true||!plain(options.inputItemId,200)||!Number.isSafeInteger(options.turnVersion)||options.turnVersion<0||context.contextRevision===null))return failed('That voice request is no longer current.',true);
      var resolved=resolveIntent(text,context),candidate={...request};
      // Optional omissions are supplied ONLY by the actual finalized shopper
      // request, never by catalogue text or invented model arguments. An explicit
      // conflicting field, different query/target or unknown field still fails.
      if(resolved.ok&&candidate.type===resolved.action.type){
        if(candidate.type==='search')['sort','filter'].forEach(function(k){if(!Object.hasOwn(candidate,k)&&Object.hasOwn(resolved.action,k))candidate[k]=resolved.action[k];});
        if(['gift','customize'].includes(candidate.type)&&!Object.hasOwn(candidate,'section'))candidate.section=resolved.action.section;
      }
      var action=validateAction(candidate);
      if(!action)return failed('That website control is not available.',true);
      if(!resolved.ok||!sameAction(action,resolved.action))return failed(resolved.reason||'That control does not match the current shopper request.',true);
      action=resolved.action;
      var fresh=read(),targeted=!!action.handle;
      if((targeted||CONTROL_TYPES.includes(action.type))&&(context.contextRevision===null||context.contextRevision!==fresh.contextRevision||CONTROL_TYPES.includes(action.type)&&JSON.stringify([context.productControls,context.bagControls,context.checkoutControls])!==JSON.stringify([fresh.productControls,fresh.bagControls,fresh.checkoutControls])))return Object.assign(failed('The visible selection changed. Ask again for the piece you are viewing now.',true),{stale:true});
      var authorityKey=source==='native'?'native:'+options.inputItemId+':'+options.turnVersion:plain(options.requestId,200)?'typed:'+options.requestId:'';
      var repeatKey=source+'|'+norm(text)+'|'+JSON.stringify(action)+'|'+String(context.contextRevision),now=clock();
      if(authorityKey&&consumed.has(authorityKey))return duplicate(consumed.get(authorityKey));
      if(!authorityKey&&recent.has(repeatKey)&&now-recent.get(repeatKey).at<900)return duplicate(recent.get(repeatKey));
      var sf=host();if(!sf||typeof sf.execute!=='function')return failed('The connected storefront is not ready yet.',true);
      var manifest=capabilities(typeof sf.capabilities==='function'?sf.capabilities():sf.capabilities);if(CONTROL_TYPES.includes(action.type)&&(!manifest||fresh.controlVersion!==1||!manifest.actions.includes(action.type)||action.type==='review-add'&&!manifest.reviewBeforeAdd||action.type==='checkout-complete'&&(manifest.mode!=='sandbox'||manifest.checkoutMode!=='simulation-only')||action.type==='gift-preferences'&&manifest.notesMode!=='local-session'))return failed('That control is not supported by this connected storefront. Use its visible controls.',true);
      var operation={at:now,status:'pending'};if(authorityKey)consumed.set(authorityKey,operation);recent.set(repeatKey,operation);if(cap){cap.used=true;cap.record=operation;}
      // Only live, in-memory authorities exist. No saved chat or model argument
      // can restore them after an interruption, reload or later conversation.
      if(consumed.size>300)consumed.delete(consumed.keys().next().value);if(recent.size>100)recent.delete(recent.keys().next().value);
      cancel();var mine=version,controller=new AbortController(),abort=function(){controller.abort();};inflight={controller:controller,version:mine};if(options.signal)options.signal.addEventListener('abort',abort,{once:true});
      var timeout=setTimeout(abort,12000),onAbort,aborted=new Promise(function(done){onAbort=function(){done({ok:false,cancelled:true});};controller.signal.addEventListener('abort',onAbort,{once:true});});
      try{
        var work=Promise.resolve().then(function(){if(controller.signal.aborted)return {ok:false,cancelled:true};return sf.execute(action,{signal:controller.signal,requestId:plain(options.requestId,200)||authorityKey||'shop-'+mine,reviewAuthority:action.type==='review-add'?options.reviewAuthority:undefined});});
        var result=await Promise.race([work,aborted]);
        if(destroyed||mine!==version||controller.signal.aborted||result&&result.cancelled)return Object.assign(failed('The website request was cancelled.',true),{cancelled:true});
        if(!result||result.ok!==true)return failed(plain(result&& (result.message||result.reason||result.error),500)||'The current shop view could not be checked. Please try again.',true);
        var out={ok:true,handled:true,recognized:true,action:action,reply:plain(result.message||result.reply,800)||'The requested part of the test shop is ready.',snapshot:read()};
        if(['bag-quantity','bag-remove'].includes(action.type)&&result.cartChanged===true)out.cartChanged=true;if(action.type.startsWith('checkout')&&manifest&&manifest.mode==='sandbox')out.mockCheckout=true;
        if(result.live===true){var checkedAt=result.checkedAt;if(Number.isFinite(checkedAt)&&checkedAt>0||typeof checkedAt==='string'&&Number.isFinite(Date.parse(checkedAt))){out.live=true;out.checkedAt=checkedAt;var products=liveProducts(result.products);if(products.length)out.products=products;}}
        operation.status='success';return out;
      }catch{return failed('The current shop view could not be checked. Please try again.',true);}finally{if(operation.status==='pending')operation.status='failure';clearTimeout(timeout);controller.signal.removeEventListener('abort',onAbort);if(options.signal)options.signal.removeEventListener('abort',abort);if(inflight&&inflight.version===mine)inflight=null;}
    }
    return {snapshot:read,resolve:resolve,execute:execute,cancel:cancel,destroy:function(){destroyed=true;cancel();}};
  }
  return Object.freeze({create:create,resolve:resolveIntent,sanitizeSnapshot:snapshot,sanitizeCapabilities:capabilities,validateAction:validateAction,projectLiveProducts:liveProducts});
});
