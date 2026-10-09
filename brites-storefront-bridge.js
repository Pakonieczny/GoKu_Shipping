(function(root,factory){'use strict';var api=factory(root);if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesStorefrontBridge=api;})(typeof window!=='undefined'?window:globalThis,function(root){
  'use strict';
  var catalogue=null;try{if(typeof module==='object'&&module.exports&&typeof require==='function')catalogue=require('./brites-catalogue-intents.js');}catch{}
  // Website awareness stays local. A model may suggest an action, but only the
  // current shopper's own request can authorize this bounded sandbox bridge.
  var HANDLE=/^[a-z0-9]+(?:-[a-z0-9]+)*$/, PRODUCT=/^gid:\/\/shopify\/Product\/[1-9][0-9]{0,19}$/, VARIANT=/^gid:\/\/shopify\/ProductVariant\/[1-9][0-9]{0,19}$/;
  var SORTS=['featured','price-asc','price-desc','title-asc','title-desc'];
  var FILTERS=['all','necklaces','regular-necklaces','beady-necklaces','earrings','stud-earrings','hoop-earrings','bracelets','rings','charms','charm-only','available'];
  var SECTIONS=['title','description','materials','length','engraving','price','details','options','story','shipping','gifts','customize','catalogue','image','bag','checkout','offers'];
  var LEGACY_TYPES=['home','search','sort','filter','open','highlight','zoom','scroll','bag','checkout','gift','customize'];
  var CONTROL_TYPES=['back','forward','gallery','options','close-options','close-image','undo','set-engraving','select-option','product-quantity','add','review-add','bag-quantity','bag-remove','bag-select-option','bag-set-engraving','gift-preferences','checkout-step','checkout-option','checkout-complete'];
  var TYPES=LEGACY_TYPES.concat(CONTROL_TYPES), LINE=/^[a-zA-Z0-9][a-zA-Z0-9:_-]{0,199}$/;
  var PRODUCT_SECTIONS=['title','description','materials','length','engraving','price','details','options','story','image'];
  function plain(value,max){return typeof value==='string'&&value.length<=max&&!/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/.test(value)?value.trim():'';}
  function norm(value){return plain(value,2000).normalize('NFKC').toLowerCase().replace(/[’']/g,'').replace(/[^a-z0-9]+/g,' ').trim();}
  function has(message,phrase){var n=norm(phrase);return !!n&&(' '+message+' ').includes(' '+n+' ');}
  function handle(value){return typeof value==='string'&&value.length<=180&&HANDLE.test(value)?value:'';}
  function revision(value){return Number.isSafeInteger(value)&&value>=0?value:typeof value==='string'&&/^[a-zA-Z0-9_.:-]{1,120}$/.test(value)?value:null;}
  function literal(value,max){return plain(value,max).normalize('NFKC').toLowerCase().replace(/\s+/g,' ').trim();}
  // Unit aliases apply only while matching a published length choice. Keep
  // private text, bare numbers, metal fineness and metric options literal.
  function inchUnitText(value,max){return literal(plain(value,max).replace(/[\u2033\u201d]|\u2032\u2032/g,'"'),max).replace(/\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)\s+(?:inches|inch|in)\b/g,function(all,word,offset,source){var preceding=source.slice(0,offset).match(/(?:^|[^\w.+-])(\d+(?:\.\d+)?|[a-z]+)\s*(?:"|inch(?:es)?|in)\s+$/);if(word==='one'&&/ in$/.test(all)&&preceding&&(/^\d/.test(preceding[1])||Object.hasOwn(QUANTITIES,preceding[1])))return all;return QUANTITIES[word]+' inch';}).replace(/(^|[^\w.+-])(\d+(?:\.\d+)?)\s*(?:-\s*)?(?:"|inches\b|inch\b|in\b)(?![a-z0-9])/g,function(all,before,number){return before+number+' inch';});}
  function selectedVariant(pc,groups,selected){
    var v=pc.selectedVariant,options=Array.isArray(v&&v.options)?v.options:null;
    if(!v||!VARIANT.test(v.id||'')||v.id!==pc.variantId||!plain(v.title,300)||!Number.isFinite(v.price)||v.price<0||!/^[A-Z]{3}$/.test(v.currency||'')||typeof v.available!=='boolean'||!options||options.length!==groups.length||selected.length!==groups.length)return null;
    var names=new Set(),safe=options.flatMap(function(o){var group=groups.find(function(g){return g.name===o?.name;}),choice=selected.find(function(s){return s.name===o?.name&&s.value===o?.value;});if(!group||!choice||!group.values.includes(o.value)||names.has(o.name))return [];names.add(o.name);return [{name:o.name,value:o.value}];});
    return safe.length===groups.length?{id:v.id,title:plain(v.title,300),price:v.price,currency:v.currency,available:v.available,...(v.availabilityKnown===false?{availabilityKnown:false}:{}),options:safe}:null;
  }
  function identityPieces(values){var seen=new Set();return (Array.isArray(values)?values:[]).slice(0,200).flatMap(function(p){var h=handle(p&&p.handle),title=plain(p&&p.title,300);if(!h||!title||seen.has(h))return [];seen.add(h);var out={handle:h,title:title};if(PRODUCT.test(p.id||''))out.id=p.id;return [out];});}
  function bagOptionFields(value){
    var groups=(Array.isArray(value.optionGroups)?value.optionGroups:[]).slice(0,12).map(function(g){var values=(Array.isArray(g?.values)?g.values:[]).slice(0,250),name=plain(g?.name,120);return name&&values.length&&values.every(function(v){return !!plain(v,300);})&&new Set(values.map(function(v){return literal(v,300);})).size===values.length?{name:name,values:values.map(function(v){return plain(v,300);})}:null;});
    if(!groups.length||groups.some(function(g){return !g;})||new Set(groups.map(function(g){return literal(g.name,120);})).size!==groups.length)return {};
    var selected=(Array.isArray(value.selectedOptions)?value.selectedOptions:[]).slice(0,12).map(function(o){var group=groups.find(function(g){return g.name===o?.name;});return group&&group.values.includes(o.value)?{name:group.name,value:o.value}:null;});
    if(selected.length!==groups.length||selected.some(function(o){return !o;})||new Set(selected.map(function(o){return o.name;})).size!==groups.length)return {};
    var out={optionGroups:groups,selectedOptions:selected},engraving=value.engravingControls;
    if(engraving?.available===true&&typeof engraving.enabled==='boolean'&&typeof engraving.hasText==='boolean'&&Number.isInteger(engraving.maxLength)&&engraving.maxLength>=1&&engraving.maxLength<=300)out.engravingControls={available:true,enabled:engraving.enabled,hasText:engraving.hasText,maxLength:engraving.maxLength};
    return out;
  }
  function capabilities(value){if(!value||value.controlVersion!==1||!['sandbox','shopify'].includes(value.mode)||!Array.isArray(value.actions))return null;return {controlVersion:1,mode:value.mode,actions:Array.from(new Set(value.actions.filter(function(v){return TYPES.includes(v);}))),reviewBeforeAdd:value.reviewBeforeAdd===true,finalOrder:false,checkoutMode:value.mode==='sandbox'&&value.checkoutMode==='simulation-only'?'simulation-only':'handoff',notesMode:value.mode==='sandbox'&&value.notesMode==='local-session'?'local-session':value.mode==='shopify'&&value.notesMode==='native-form'?'native-form':'unsupported'};}
  function snapshot(value){
    value=value&&typeof value==='object'?value:{};
    var out={contextRevision:revision(value.contextRevision),discoveryRevision:Number.isSafeInteger(value.discoveryRevision)&&value.discoveryRevision>=0?value.discoveryRevision:0,pageKind:plain(value.pageKind||value.page,40)||'unknown',currentHandle:handle(value.currentHandle),focusedHandle:handle(value.focusedHandle),visiblePieces:identityPieces(value.visiblePieces),search:plain(value.search===undefined?value.query:value.search,180),sort:SORTS.includes(value.sort)?value.sort:'featured',filter:FILTERS.includes(value.filter)?value.filter:'all',loading:value.loading===true,activeSection:SECTIONS.includes(value.activeSection)?value.activeSection:''};
    if(Array.isArray(value.loadedPieces))out.loadedPieces=identityPieces(value.loadedPieces);
    if(Array.isArray(value.inventoryPieces)){out.inventoryPieces=identityPieces(value.inventoryPieces.slice(0,160)).map(function(p){var source=value.inventoryPieces.find(function(row){return row&&row.handle===p.handle;}),categories=Array.isArray(source?.storeCategories)?Array.from(new Set(source.storeCategories.filter(function(v){return ['regular-necklaces','beady-necklaces','stud-earrings','hoop-earrings','charm-only'].includes(v);}))).slice(0,5):[];return {...p,...(categories.length?{storeCategories:categories}:{})};});}
    var currentProduct=value.currentProduct;
    if(out.pageKind==='product'&&currentProduct&&currentProduct.handle===out.currentHandle&&PRODUCT.test(currentProduct.id||'')&&plain(currentProduct.title,300)&&ownProductURL(currentProduct.url,currentProduct.handle)&&/^[A-Z]{3}$/.test(currentProduct.currency||'')){
      out.currentProduct={id:currentProduct.id,handle:currentProduct.handle,title:plain(currentProduct.title,300),url:currentProduct.url,currency:currentProduct.currency};
      ['type','description'].forEach(function(key){var content=plain(currentProduct[key],key==='description'?6000:120);if(content)out.currentProduct[key]=content;});
      if(Number.isSafeInteger(currentProduct.checkedAt)&&currentProduct.checkedAt>0)out.currentProduct.checkedAt=currentProduct.checkedAt;
    }
    if(value.controlVersion!==1)return out;
    var changes=value.changeControls,images=value.imageControls,engraving=value.engravingControls;
    if(changes&&typeof changes.canUndo==='boolean'&&(changes.kind===null||['select-option','product-quantity','gallery','bag-add','bag-quantity','bag-remove','bag-select-option','bag-set-engraving','gift-preferences','checkout-step','checkout-option','sort','filter','set-engraving'].includes(changes.kind)))out.changeControls={canUndo:changes.canUndo,kind:changes.kind};
    if(images&&typeof images.open==='boolean')out.imageControls={open:images.open};
    if(engraving&&engraving.available===true&&typeof engraving.enabled==='boolean'&&typeof engraving.hasText==='boolean'&&Number.isInteger(engraving.maxLength)&&engraving.maxLength>=1&&engraving.maxLength<=300)out.engravingControls={available:true,enabled:engraving.enabled,hasText:engraving.hasText,maxLength:engraving.maxLength};
    var gifts=value.giftControls;if(gifts&&typeof gifts.wrappingAvailable==='boolean'&&typeof gifts.noteAvailable==='boolean'&&typeof gifts.wrapping==='boolean'&&typeof gifts.hasNote==='boolean')out.giftControls={wrappingAvailable:gifts.wrappingAvailable,noteAvailable:gifts.noteAvailable,wrapping:gifts.wrapping,hasNote:gifts.hasNote,savedKnown:gifts.savedKnown===true};
    out.controlVersion=1;var nav=value.navigationControls,gallery=value.galleryControls;if(nav&&typeof nav.canGoBack==='boolean'&&typeof nav.canGoForward==='boolean')out.navigationControls={canGoBack:nav.canGoBack,canGoForward:nav.canGoForward};if(gallery&&handle(gallery.handle)&&gallery.handle===out.currentHandle&&Number.isInteger(gallery.imageCount)&&gallery.imageCount>=1&&gallery.imageCount<=50&&Number.isInteger(gallery.selectedIndex)&&gallery.selectedIndex>=1&&gallery.selectedIndex<=gallery.imageCount)out.galleryControls={handle:gallery.handle,imageCount:gallery.imageCount,selectedIndex:gallery.selectedIndex};var pc=value.productControls;
    if(pc&&handle(pc.handle)&&PRODUCT.test(pc.productId||'')&&Number.isInteger(pc.quantity)&&pc.quantity>=1&&pc.quantity<=20){
      var groups=(Array.isArray(pc.optionGroups)?pc.optionGroups:[]).slice(0,12).flatMap(function(g){var name=plain(g&&g.name,120),seen=new Set(),values=(Array.isArray(g&&g.values)?g.values:[]).slice(0,250).flatMap(function(v){var clean=plain(v,300),key=literal(clean,300);if(!clean||seen.has(key))return [];seen.add(key);return [clean];});return name&&values.length?[{name:name,values:values}]:[];});
      if(new Set(groups.map(function(g){return literal(g.name,120);})).size===groups.length){var selectedNames=new Set(),selected=(Array.isArray(pc.selectedOptions)?pc.selectedOptions:[]).slice(0,12).flatMap(function(v){var group=groups.find(function(g){return literal(g.name,120)===literal(v&&v.name,120);}),values=group&&group.values.filter(function(x){return literal(x,300)===literal(v&&v.value,300);});if(!values||values.length!==1||selectedNames.has(group.name))return [];selectedNames.add(group.name);return [{name:group.name,value:values[0]}];});
        out.productControls={handle:pc.handle,productId:pc.productId,variantId:VARIANT.test(pc.variantId||'')?pc.variantId:null,quantity:pc.quantity,optionsOpen:pc.optionsOpen===true,openedOption:groups.some(function(g){return g.name===pc.openedOption;})?pc.openedOption:null,optionGroups:groups,selectedOptions:selected,reviewReady:pc.reviewReady===true};
        ['productTitle','productType'].forEach(function(k){var label=plain(pc[k],k==='productTitle'?300:120);if(label)out.productControls[k]=label;});var current=selectedVariant(pc,groups,selected);if(current){out.productControls.selectedVariant=current;var total=Math.round(current.price*pc.quantity*100)/100;if(Number.isFinite(pc.itemTotalPrice)&&Math.abs(pc.itemTotalPrice-total)<.005)out.productControls.itemTotalPrice=total;if(current.id===pc.selectedVariant.id&&Number.isInteger(pc.selectedVariant.quantity)&&pc.selectedVariant.quantity===pc.quantity)out.productControls.selectedVariant.quantity=pc.quantity;if(Number.isFinite(pc.selectedVariant.subtotal)&&Math.abs(pc.selectedVariant.subtotal-total)<.005)out.productControls.selectedVariant.subtotal=total;}
        if(['choosing','ready','unavailable','held'].includes(pc.selectionStatus)){
          // Missing choices are derived from the published groups and the
          // actual selections rather than trusting a suggested next step.
          var missing=groups.filter(function(g){return !selected.some(function(s){return s.name===g.name;});});
          out.productControls.selectionStatus=missing.length?'choosing':pc.selectionStatus;
          out.productControls.requiredOptions=missing.map(function(g){return g.name;});
          out.productControls.requiredOptionGroups=missing.map(function(g){return {name:g.name,values:g.values.slice()};});
          out.productControls.nextOptionName=missing.some(function(g){return g.name===pc.nextOptionName;})?pc.nextOptionName:missing[0]?.name||null;
        }
      }
    }
    var bag=value.bagControls;if(bag&&Array.isArray(bag.lines)){var seenLines=new Set(),lines=bag.lines.slice(0,50).flatMap(function(l){var lineId=plain(l&&l.lineId,200);if(!LINE.test(lineId)||seenLines.has(lineId)||!PRODUCT.test(l.productId||'')||!VARIANT.test(l.variantId||'')||!Number.isInteger(l.quantity)||l.quantity<1||l.quantity>20||!plain(l.title,300))return [];seenLines.add(lineId);var line={lineId:lineId,productId:l.productId,variantId:l.variantId,quantity:l.quantity,title:plain(l.title,300),variant:plain(l.variant,300)};if(Number.isFinite(l.price)&&l.price>=0&&/^[A-Z]{3}$/.test(l.currency||'')){line.price=l.price;line.currency=l.currency;line.lineTotalPrice=Math.round(l.price*l.quantity*100)/100;}if(handle(l.handle)){line.handle=l.handle;Object.assign(line,bagOptionFields(l));}return [line];});var renderedCount=lines.reduce(function(n,l){return n+l.quantity;},0);out.bagControls={lines:lines,itemCount:Number.isSafeInteger(bag.itemCount)&&bag.itemCount>=renderedCount&&bag.itemCount<=50000?bag.itemCount:renderedCount};if(typeof bag.countKnown==='boolean')out.bagControls.countKnown=bag.countKnown;if(bag.countKnown===false||bag.linesComplete===false||bag.lines.length!==lines.length||out.bagControls.itemCount!==renderedCount)out.bagControls.linesComplete=false;}
    var cc=value.checkoutControls;if(cc&&[null,'review','shipping','confirm','complete'].includes(cc.step)&&['standard','express'].includes(cc.shipping))out.checkoutControls={step:cc.step,shipping:cc.shipping,acknowledged:cc.acknowledged===true,complete:cc.complete===true};
    return out;
  }
  function failed(reason,recognized){return {ok:false,handled:recognized===true,recognized:recognized===true,reason:reason};}
  function optionAmbiguity(reason){return Object.assign(failed(reason,true),{needsClarification:true});}
  function success(action,context){return {ok:true,handled:true,recognized:true,action:Object.freeze(action),contextRevision:context.contextRevision};}
  function validateAction(value){
    if(!value||typeof value!=='object'||Array.isArray(value)||!TYPES.includes(value.type))return null;
    var keys={home:['type'],search:['type','query','sort','filter'],sort:['type','sort'],filter:['type','filter'],open:['type','handle'],highlight:['type','handle','section'],zoom:['type','handle'],back:['type'],forward:['type'],undo:['type'],'close-options':['type'],'close-image':['type'],'set-engraving':['type','handle','text'],gallery:['type','handle','index'],scroll:['type','handle','section','direction'],bag:['type'],checkout:['type'],gift:['type','section'],customize:['type','handle','section'],options:['type','handle','optionName'],'select-option':['type','handle','variantId','optionName','optionValue'],'product-quantity':['type','handle','quantity'],add:['type','handle','variantId'],'review-add':['type','handle','variantId'],'bag-quantity':['type','lineId','quantity'],'bag-remove':['type','lineId'],'bag-select-option':['type','lineId','optionName','optionValue'],'bag-set-engraving':['type','lineId','text'],'gift-preferences':['type','wrapping','giftPackage','giftNote'],'checkout-step':['type','step'],'checkout-option':['type','option','value'],'checkout-complete':['type']}[value.type];
    if(Object.keys(value).some(function(k){return !keys.includes(k);}))return null;
    var out={type:value.type};
    if(value.type==='search'){var q=plain(value.query,180);if(!q)return null;out.query=q;}
    if(value.type==='sort'&&!SORTS.includes(value.sort)||value.sort!==undefined&&!SORTS.includes(value.sort))return null;
    if(value.sort!==undefined)out.sort=value.sort;
    if(value.type==='filter'&&!FILTERS.includes(value.filter)||value.filter!==undefined&&!FILTERS.includes(value.filter))return null;
    if(value.filter!==undefined)out.filter=value.filter;
    if(value.handle!==undefined){if(!handle(value.handle))return null;out.handle=value.handle;}
    if(['open','zoom','gallery'].includes(value.type)&&!out.handle)return null;
    if(value.section!==undefined){if(!SECTIONS.includes(value.section))return null;out.section=value.section;}
    if(['highlight','gift','customize'].includes(value.type)&&!out.section)return null;
    if(value.type==='scroll'){if(value.direction!==undefined){if(!['up','down','top','bottom'].includes(value.direction)||out.section||out.handle)return null;out.direction=value.direction;}else if(!out.section)return null;}
    if(value.type==='gallery'){if(!Number.isInteger(value.index)||value.index<1||value.index>50)return null;out.index=value.index;}
    if(value.type==='gift'&&out.section!=='gifts'||value.type==='customize'&&!['customize','options'].includes(out.section))return null;
    if(['highlight','scroll'].includes(value.type)&&PRODUCT_SECTIONS.includes(out.section)&&!out.handle)return null;
    if(['options','select-option','product-quantity','add','review-add','set-engraving'].includes(value.type)&&!out.handle)return null;
    if(['set-engraving','bag-set-engraving'].includes(value.type)){if(typeof value.text!=='string'||value.text.length>300||/[\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/.test(value.text))return null;out.text=value.text;}
    if(value.variantId!==undefined){if(!VARIANT.test(value.variantId||''))return null;out.variantId=value.variantId;}
    if(value.optionName!==undefined){var name=plain(value.optionName,120);if(!name)return null;out.optionName=name;}
    if(value.optionValue!==undefined){var val=plain(value.optionValue,300);if(!val)return null;out.optionValue=val;}
    if(value.type==='select-option'&&((!!out.variantId)===(!!out.optionName&&!!out.optionValue)||out.variantId&&(out.optionName||out.optionValue)||(!out.variantId&&(!out.optionName||!out.optionValue))))return null;
    if(value.type==='bag-select-option'&&(!out.optionName||!out.optionValue))return null;
    if(['product-quantity','bag-quantity'].includes(value.type)){if(!Number.isInteger(value.quantity)||value.quantity<1||value.quantity>20)return null;out.quantity=value.quantity;}
    if(['bag-quantity','bag-remove','bag-select-option','bag-set-engraving'].includes(value.type)){if(!LINE.test(value.lineId||''))return null;out.lineId=value.lineId;}
    if(value.type==='gift-preferences'){if(!['wrapping','giftPackage','giftNote'].some(function(k){return Object.hasOwn(value,k);}))return null;for(var k of ['wrapping','giftPackage'])if(Object.hasOwn(value,k)){if(typeof value[k]!=='boolean')return null;out[k]=value[k];}if(Object.hasOwn(value,'giftNote')){if(typeof value.giftNote!=='string'||value.giftNote.length>300||/[\u0000-\u001f\u007f\u202a-\u202e\u2066-\u2069]/.test(value.giftNote))return null;out.giftNote=value.giftNote;}}
    if(value.type==='checkout-step'){if(!['review','shipping','confirm'].includes(value.step))return null;out.step=value.step;}
    if(value.type==='checkout-option'){if(value.option!=='shipping'||!['standard','express'].includes(value.value))return null;out.option=value.option;out.value=value.value;}
    return out;
  }
  function sameAction(a,b){return !!a&&!!b&&JSON.stringify(Object.keys(a).sort().map(function(k){return [k,a[k]];}))===JSON.stringify(Object.keys(b).sort().map(function(k){return [k,b[k]];}));}
  var ORDINALS={first:0,second:1,third:2,fourth:3,fifth:4,sixth:5,seventh:6,eighth:7,ninth:8,tenth:9,'1st':0,'2nd':1,'3rd':2,'4th':3,'5th':4,'6th':5,'7th':6,'8th':7,'9th':8,'10th':9};
  var QUANTITIES={one:1,two:2,three:3,four:4,five:5,six:6,seven:7,eight:8,nine:9,ten:10,eleven:11,twelve:12,thirteen:13,fourteen:14,fifteen:15,sixteen:16,seventeen:17,eighteen:18,nineteen:19,twenty:20};
  var GENERIC=new Set(['necklace','necklaces','earring','earrings','bracelet','bracelets','ring','rings','charm','charms','pendant','pendants','jewelry','jewellery','the','a','an','and','with','for','in','of','silver','gold','sterling','inch','inches','chain','chains','piece','pieces','product','products']);
  var TARGET_STOP=new Set('please could would can will you your our i me my want to like see show tell display pull up open view take go highlight scroll focus current selected now currently what whats is are does do have has how much price prices cost costs worth material materials made from metal metals details process finish finishes meaning mean means symbol symbolism story stories represents options variants sizes size length lengths available choices image photo picture zoom enlarge magnify make bigger larger title name description photos pictures images gallery numbered number next previous thumbnails thumbnail field menu selector dropdown drop down third fourth fifth sixth seventh eighth ninth tenth last its it this that here used use on about page listing item one nice pretty beautiful custom design designs customize customise personalize personalise personalization engrave engraved engraving'.split(' '));
  'they them their'.split(' ').forEach(function(word){TARGET_STOP.add(word);});
  function exactMatches(message,context){
    var matches=[];
    context.visiblePieces.concat(context.loadedPieces||[],context.inventoryPieces||[]).forEach(function(p){[p.title,p.handle].forEach(function(label,labelIndex){var phrase=norm(label),hay=' '+message+' ',needle=' '+phrase+' ',at=-1;if(!phrase)return;while((at=hay.indexOf(needle,at+1))!==-1)matches.push({handle:p.handle,label:phrase,kind:labelIndex===0?'title':'handle',start:at,end:at+phrase.length});});});
    // A full longer title or handle wins only over labels nested inside that
    // same mention. Identical titles and separately named pieces stay ambiguous.
    return matches.filter(function(m){return !matches.some(function(other){return other.handle!==m.handle&&(other.label.length>m.label.length||m.kind==='handle'&&other.kind==='title'&&other.label.length===m.label.length)&&other.start<=m.start&&other.end>=m.end;});});
  }
  function pointerRequest(message,context){
    return /\b(?:hover|hovering|pointing|under my (?:mouse|cursor)|my (?:mouse|cursor) (?:is )?over|where my (?:mouse|cursor) is)\b/.test(message)||context.pageKind!=='product'&&/\blooking at\b/.test(message);
  }
  function target(message,context,implicit){
    var exactRows=exactMatches(message,context),exact=new Set(exactRows.map(function(m){return m.handle;})),ordinal=new Set(),ordinalWords=message;exactRows.slice().sort(function(a,b){return b.label.length-a.label.length;}).forEach(function(m){ordinalWords=ordinalWords.replace(m.label,'');});
    Object.keys(ORDINALS).forEach(function(word){if(has(ordinalWords,word)){var p=context.visiblePieces[ORDINALS[word]];if(p)ordinal.add(p.handle);else ordinal.add('');}});
    if(!exact.size){context.visiblePieces.forEach(function(p){var words=Array.from(new Set(norm(p.title+' '+p.handle).split(' ').filter(function(w){return w.length>2&&!GENERIC.has(w)&&!TARGET_STOP.has(w)&&!/^\d+$/.test(w);})));if(words.some(function(w){return has(message,w);}))exact.add(p.handle);});}
    if(exact.size>1||ordinal.size>1||exact.size&&ordinal.size&&Array.from(exact)[0]!==Array.from(ordinal)[0])return {reason:'More than one piece matches. Choose a listing or give its exact title.'};
    if(exact.size)return {handle:Array.from(exact)[0],match:'named'};
    if(ordinal.size){var h=Array.from(ordinal)[0];return h?{handle:h,match:'ordinal'}:{reason:'That option is not in the current visible selection.'};}
    var pointed=pointerRequest(message,context);
    var pronoun=/\b(?:it|this|that|they|them|their|current|selected|the piece|the product|the listing|the image|the photo)\b/.test(message);
    if(pointed)return context.focusedHandle?{handle:context.focusedHandle,match:'pointed'}:{reason:'Point to or choose a listing first.'};
    var subjectWords=message.replace(/\b(?:i am|im|we are|were) looking at\b/g,'').replace(/\blooking at\b/g,'');
    var unnamedTarget=subjectWords.split(' ').some(function(w){return w&&!GENERIC.has(w)&&!TARGET_STOP.has(w)&&!/^\d+$/.test(w);});
    if((pronoun||implicit)&&!unnamedTarget){var scoped=context.pageKind==='product'?context.currentHandle||context.focusedHandle:context.focusedHandle||context.currentHandle;if(scoped)return {handle:scoped,match:'scope'};if(context.visiblePieces.length===1)return {handle:context.visiblePieces[0].handle,match:'only'};}
    return {reason:'Choose a listing or give its exact title so I can use the correct piece.'};
  }
  var KNOWLEDGE_STOP=new Set('which why whom when where should could would might may tell know knowing information facts factual about everything anything something all any of as be being been there their these those really exactly actual published checked each every detail details description descriptions feature features difference differences compare comparison comparing versus vs pros cons benefits better best good suitable special unique include includes including included comes come only attached attachment sold together separate separately standalone complete available availability stock stocked currently range ranges price prices pricing quote quoted cost costs total totals unit units per single material materials metal metals finish finishes solid filled plated vermeil brass copper steel stainless karat carat kt k made make makes making does do what whats its it this that much how worth silver sterling gold rose chain chains length lengths inch inches size sizes option options choices engraving engraved engrave care caring wearing wear water waterproof tarnish tarnishes tarnishing hypoallergenic nickel allergy allergies free resistant resist resistance meaning mean means symbol symbolic symbolism story stories history historical represents represent look looks appearance design designs motif motifs pendant pendants necklace necklaces earring earrings stud studs hoop hoops huggie huggies charm charms jewelry jewellery the a an and with for in on to from behind under mouse cursor hover hovering pointing looking at selected current please can will you your our i me my want like see show have has is are thats explain more further product piece listing page item one'.split(' '));
  'dimension dimensions measurement measurements diameter thickness width height millimeters millimetres centimeters centimetres mm cm version versions get getting cheapest lowest highest compared between than everyday daily recommend recommendation recommendations budget affordable durable durability longevity last lasts big large small bigger smaller'.split(' ').forEach(function(word){KNOWLEDGE_STOP.add(word);});
  'contain contains shower showers showering swim swimming pool clean cleaning polish polishing sensitive skin safe safety exposure colour color fade fading least expensive softer'.split(' ').forEach(function(word){KNOWLEDGE_STOP.add(word);});
  'am im ive opened viewing some interesting tidbit tidbits fact facts fun background interestingly weight weights weighs light lightweight heavy comfortable comfort thing craftsmanship handmade craft composition benefit benefits choose choice exact chosen'.split(' ').forEach(function(word){KNOWLEDGE_STOP.add(word);});
  function resolveKnowledgeTarget(value,rawContext){
    var raw=typeof value==='string'?value:value&&value.message,context=snapshot(rawContext||value&&value.context),message=norm(raw);
    if(!plain(raw,2000)||!message)return {reason:'Ask about a current piece or give its exact title.'};
    var matches=exactMatches(message,context),ids=new Set(matches.map(function(m){return m.handle;}));
    if(ids.size>1)return {reason:'More than one piece matches. Choose a listing or give its exact title.'};
    var rest=message;matches.sort(function(a,b){return b.label.length-a.label.length;}).forEach(function(m){rest=rest.replace(m.label,'');});
    // "Long" describes a factual question only in this opening phrase. It
    // remains an unknown name in requests about an unseen Long Necklace.
    rest=rest.replace(/^how long\b/,'how length');
    var ordinal=new Set();Object.keys(ORDINALS).forEach(function(word){if(has(rest,word)){var piece=context.visiblePieces[ORDINALS[word]];ordinal.add(piece?piece.handle:'');rest=rest.replace(norm(word),'');}});
    var pc=context.productControls;if(pc){pc.optionGroups.forEach(function(g){[g.name].concat(g.values).forEach(function(v){if(has(rest,v))rest=rest.replace(norm(v),'');});});}
    var currentPiece=pc&&context.visiblePieces.concat(context.loadedPieces||[],context.inventoryPieces||[]).find(function(p){return p.handle===context.currentHandle&&p.id===pc.productId;}),currentName=currentPiece?.title||(context.currentProduct&&context.currentProduct.id===pc?.productId?context.currentProduct.title:'');
    if(context.pageKind==='product'&&pc?.handle===context.currentHandle&&(!ids.size||ids.has(context.currentHandle))&&/\b(?:letter|initial)s?\b/.test(norm(currentName))&&/^(?:how (?:big|large|wide|tall|thick)|what (?:is|are) (?:the )?(?:dimensions?|measurements?|size|width|height|diameter|thickness))\b/.test(message))rest=rest.replace(/\b(?:letter|initial)s?\b/g,'');
    // Do not let an unknown named product borrow a current or mentioned piece.
    if(rest.split(' ').some(function(w){return w&&!GENERIC.has(w)&&!TARGET_STOP.has(w)&&!KNOWLEDGE_STOP.has(w)&&!/^\d+(?:k|kt)?$/.test(w);}))return {reason:'Name the exact piece you want to know about.'};
    if(ordinal.size>1||ids.size&&ordinal.size&&Array.from(ids)[0]!==Array.from(ordinal)[0])return {reason:'More than one piece matches. Choose a listing or give its exact title.'};
    if(ids.size===1)return {handle:Array.from(ids)[0],match:'named'};
    if(ordinal.size){var h=Array.from(ordinal)[0];return h?{handle:h,match:'ordinal'}:{reason:'That position is not in the current visible selection. Choose a listing or give its exact title.'};}
    if(pointerRequest(message,context))return context.focusedHandle?{handle:context.focusedHandle,match:'pointed'}:{reason:'Point to or choose a listing first.'};
    var scoped=context.pageKind==='product'?context.currentHandle||context.focusedHandle:context.focusedHandle||context.currentHandle;
    if(scoped)return {handle:scoped,match:'scope'};
    if(context.visiblePieces.length===1)return {handle:context.visiblePieces[0].handle,match:'only'};
    return {reason:'Choose a listing or give its exact title so I can use the correct piece.'};
  }
  function informationRequest(message){
    message=message.replace(/^(?:(?:please|could you|would you|can you|will you)\s+)*/,'');
    return /^(?:what|whats|which|why|how|where|when|is|are|does|do|will|would|should|tell me|explain|compare|describe|give me (?:the )?(?:facts|information|details|price|prices)|i (?:want|would like|need) to know|id like to know|can you (?:tell|explain|describe)|could you (?:tell|explain|describe)|can i (?!see\b|view\b|browse\b|check out\b))\b/.test(message)||/^(?:please )?show me (?:what|which|how|why)\b/.test(message);
  }
  function category(message){var cats=[];[['necklaces',/\bnecklaces?\b/],['earrings',/\bearrings?\b/],['bracelets',/\bbracelets?\b/],['rings',/\brings?\b/],['charms',/\bcharms?\b/]].forEach(function(pair){if(pair[1].test(message))cats.push(pair[0]);});if(cats.length!==1)return '';var sub=[];[['regular-necklaces',/\bregular\b.{0,30}\bnecklaces?\b/],['beady-necklaces',/\b(?:beady|beaded|bead)\b.{0,30}\bnecklaces?\b/],['stud-earrings',/\bstud\b.{0,30}\bearrings?\b/],['hoop-earrings',/\b(?:hoop|huggie)\b.{0,30}\bearrings?\b/],['charm-only',/\bcharms? only\b/]].forEach(function(pair){if(pair[1].test(message))sub.push(pair[0]);});return sub.length>1?'':sub[0]||cats[0];}
  function collectionReset(message){
    var command=message.replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*/,'').replace(/\s+(?:please|again)$/,'');
    var collection='(?:the )?(?:(?:previous|full|whole|entire|complete|original|unfiltered|main|all) )?(?:list|catalogue|catalog|collection|collections|shop|results|pieces|jewelry|jewellery)';
    return new RegExp('^(?:(?:go|take me) back(?: to '+collection+')?|return(?: me)? to '+collection+'|(?:show(?: me)?|open|view|display|pull up|let me see) '+collection+')$').test(command)||/^(?:show(?: me)? all|show(?: me)? everything|start (?:over|fresh)|reset(?: the)? (?:search|collection|catalogue|catalog)|clear(?: the)? search(?: results)?)$/.test(command);
  }
  function categoryBrowse(message){
    var command=message.replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*/,'').replace(/\s+(?:please|again)$/,'');
    var noun='(?:(?:regular|beady|beaded|bead) necklaces?|(?:stud|hoop|huggie) earrings?|charms? only(?: pieces)?|necklaces?|earrings?|bracelets?|rings?|charms?)';
    // A bare category is a collection request, not an exact listing identity.
    // Keep this grammar narrow so names, ordinals, meanings and advice retain
    // their existing target/model routes and cannot acquire browse authority.
    if(new RegExp('^(?:open|view|show(?: me)?|browse|display|let me see) (?:all (?:your |the )?|your |the |some |a |an )?'+noun+'$').test(command)||
       new RegExp('^(?:what|which) (?:kinds? of |types? of )?'+noun+' do you (?:have|offer)$').test(command)||
       new RegExp('^(?:do you (?:have|offer)|can i (?:see|browse|view)) (?:any |some |the |your )?'+noun+'$').test(command))return category(command);
    return '';
  }
  function sortFor(message){if(/\b(?:highest|expensive|priciest|descending|high to low|most expensive)\b/.test(message))return 'price-desc';if(/\b(?:cheapest|lowest|least expensive|low to high|affordable first|ascending price)\b/.test(message))return 'price-asc';if(/\b(?:z to a|reverse alphabetical|reverse alphabetically)\b/.test(message))return 'title-desc';if(/\b(?:alphabetic|alphabetical|alphabetically|a to z|by (?:name|title))\b/.test(message))return 'title-asc';if(/\b(?:featured|recommended order|default order|reset sort)\b/.test(message))return 'featured';return '';}
  function sectionFor(message){if(/\b(?:title|product name|listing name)\b/.test(message))return 'title';if(/\b(?:description|product description)\b/.test(message))return 'description';if(/\b(?:engraving|engraved text|engraving field)\b/.test(message))return 'engraving';if(/\b(?:length menu|length selector|chain length|necklace length)\b/.test(message))return 'length';if(/\b(?:material|materials|metal|metals)\b/.test(message))return 'materials';if(/\b(?:shipping|delivery|production|processing|dispatch)\b/.test(message))return 'shipping';if(/\b(?:gift wrapping|gift wrap|gift packaging|gift package|gift packages|gift note|gift notes|gift message)\b/.test(message))return 'gifts';if(/\b(?:discount|discounts|coupon|coupons|promo|promotions|offer|offers|sale|codes?)\b/.test(message))return 'offers';if(/\b(?:meaning|mean|means|symbol|symbolism|story|stories|represents)\b/.test(message))return 'story';if(/\b(?:material|materials|made of|made from|metal|metals|details|process|finish|finishes)\b/.test(message))return 'details';if(/\b(?:price|prices|cost|costs|how much)\b/.test(message))return 'price';if(/\b(?:options|variants|sizes|size|length|lengths|chain length|available choices|dropdown|drop down|selector|menu)\b/.test(message))return 'options';if(/\b(?:image|photo|picture)\b/.test(message))return 'image';if(/\b(?:catalogue|catalog|collection|collections|results)\b/.test(message))return 'catalogue';return '';}
  function controlTarget(message,context){var named=target(message,context,false);if(named.handle&&named.match==='named'||named.reason&&/^More than one piece/.test(named.reason))return named;var pc=context.productControls;if(pc&&context.pageKind==='product'&&context.currentHandle===pc.handle&&!/\b(?:other|another|different)\b/.test(message)){var remainder=optionRequestText(message,context,pc.handle);pc.optionGroups.forEach(function(group){[group.name].concat(group.values).sort(function(a,b){return b.length-a.length;}).forEach(function(value){if(has(remainder,value))remainder=remainder.replace(norm(value),'');});});if(!/\b(?:for|of) (?!the (?:current|selected)\b|(?:this|that|it|current|selected)\b).+/.test(message)||!optionRequestRemainder(remainder,context,pc.handle))return {handle:pc.handle,match:'scope'};}return named;}
  function optionRequestText(value,context,h,asLiteral){
    var convert=asLiteral?function(v){return literal(v,2000);}:norm,words=convert(value),identity=context.visiblePieces.concat(context.loadedPieces||[],context.inventoryPieces||[]).find(function(p){return p.handle===h;});if(identity){[identity.title,identity.handle].forEach(function(label){var name=convert(label);if(name)words=words.replace(name,'');});}return words;
  }
  function optionRequestRemainder(command,context,h,group,value){
    var rest=optionRequestText(command,context,h).replace(/^(?:open|show|expand|display|choose|select|pick|set|change|use)\b/,'');if(value)rest=rest.replace(norm(value),'');
    // Ordinary chain/necklace wording belongs to the matched length menu,
    // never to an unknown product name or a second possible length group.
    var pc=context.productControls;if(group&&pc&&/^(?:length|chain length|necklace length)$/.test(norm(group))){var matched=optionGroup(rest,pc),lengthGroups=pc.optionGroups.filter(function(g){return /\blength\b/.test(norm(g.name));});if(matched&&matched.name===group&&(has(rest,group)||lengthGroups.length===1))rest=rest.replace(/\b(?:chain|necklace) lengths?\b/g,'');}
    if(pc&&pc.handle===h&&context.pageKind==='product'&&context.currentHandle===h){var noun=norm(pc.productType).match(/\b(necklace|earring|bracelet|ring|charm)s?\b/);if(noun)rest=rest.replace(new RegExp('\\b(?:this|that|the current|the selected) '+noun[1]+'s?\\b','g'),'');}
    if(group)rest=rest.replace(norm(group),'');
    return rest.replace(/\b(?:me|the|my|a|an|to|for|of|on|in|this|that|it|its|current|selected|piece|product|listing|item|option|options|choices|variants|published|available|dropdown|drop|down|selector|menu|metal|metals|material|materials|size|sizes|length|lengths|please|again)\b/g,'').trim();
  }
  function bagTarget(message,context){
    var lines=context.bagControls&&context.bagControls.lines||[],named=lines.filter(function(l){return has(message,l.title);});
    if(named.length){var qualified=named.filter(function(l){var rest=message.replace(norm(l.title),'');if(l.variant&&has(rest,l.variant))rest=rest.replace(norm(l.variant),'');return !rest.replace(/\b(?:the|my|a|an|this|that|current|selected|item|piece)\b/g,'').trim();});return qualified.length===1?qualified[0]:null;}
    // Only an otherwise unnamed ordinal or pronoun can select a visible line.
    // An unknown title/option never borrows the only line in the bag.
    var ord=Object.keys(ORDINALS).filter(function(w){return has(message,w);}),rest=message;ord.forEach(function(w){rest=rest.replace(w,'');});
    if(rest.replace(/\b(?:the|my|a|an|it|this|that|current|selected|item|piece|line|quantity|bag|cart|basket)\b/g,'').trim())return null;
    if(ord.length===1)return lines[ORDINALS[ord[0]]]||null;
    return !ord.length&&lines.length===1&&/\b(?:it|this|that|item|piece|line|quantity|bag|cart|basket)\b/.test(message)?lines[0]:null;
  }
  function bagRequestTarget(value,context){return bagTarget(norm(value).replace(/(?:^|\s)(?:in|from|within|inside) (?:my |the )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket)$/,'').trim(),context);}
  function bagControl(raw,command,context){
    var rawCommand=raw.trim().replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|i'd like (?:you )?to)\s+)*/i,''),textRequest=/^(?:set|change|write|save)(?: the| my)? (?:engraving|engraved)(?: text| message| wording)? (?:for|on|of) (.+?) (?:to|as)\s*[:=]?\s+([\s\S]+)$/i.exec(rawCommand),clearRequest=/^(?:clear|erase|remove)(?: the| my)? (?:engraving|engraved)(?: text| message| field| wording)?(?: (?:for|on|of) (.+))?$/i.exec(rawCommand),directText=/^(?:set|change|write|save)(?: the| my)? (?:engraving|engraved)(?: text| message| wording)? (?:to|as)\s*[:=]?\s+([\s\S]+)$/i.exec(rawCommand);
    // Private wording is never a route hint. A cart reference inside the
    // engraving itself cannot redirect a product field into a bag operation.
    var targetWords=textRequest?norm(textRequest[1]):clearRequest?norm(clearRequest[1]):directText?'':command;
    if(context.pageKind!=='bag'&&!/\b(?:cart|bag|basket)\b/.test(targetWords))return null;
    if(textRequest||clearRequest||directText){
      var requested=textRequest?textRequest[1]:clearRequest?clearRequest[1]:null,line=requested?bagRequestTarget(requested,context):bagTarget('this item',context),text=textRequest?textRequest[2]:directText?directText[1]:'';
      if(!line)return failed('Which item in your bag would you like to engrave?',true);
      if(!line.engravingControls?.available||!line.engravingControls.enabled)return failed('Choose the engraving option for this item first.',true);
      var action={type:'bag-set-engraving',lineId:line.lineId,text:text};if(text.length>line.engravingControls.maxLength||!validateAction(action))return failed('Use up to '+line.engravingControls.maxLength+' characters for this engraving.',true);
      return success(action,context);
    }
    if(!/^(?:choose|select|pick|set|change|use)\b/.test(command)||/\b(?:quantity|gift|shipping|checkout|search|sort|filter)\b/.test(command))return null;
    var body=rawCommand.replace(/^(?:choose|select|pick|set|change|use)\s+/i,''),chosen=null,optionWords='',front=/^(.+?)\s+(?:to|as)\s+(.+)$/i.exec(body);
    if(front){chosen=bagRequestTarget(front[1],context);if(chosen)optionWords=front[2];else {var subject=/(.+?)\s+(?:for|on|of)\s+(.+)$/i.exec(front[1]);if(subject){chosen=bagRequestTarget(subject[2],context);if(chosen)optionWords=subject[1]+' '+front[2];}}}
    if(!chosen){var splits=Array.from(body.matchAll(/\s+(?:for|on|of)\s+/gi)).reverse();for(var split of splits){var target=bagRequestTarget(body.slice(split.index+split[0].length),context);if(target){chosen=target;optionWords=body.slice(0,split.index);break;}}}
    if(!chosen){chosen=bagTarget('this item',context);optionWords=body;}
    if(!chosen)return failed('Which item in your bag would you like to change? Use its position or exact options.',true);
    if(!handle(chosen.handle)||!chosen.optionGroups?.length||!chosen.selectedOptions?.length)return failed('Open this item to check its options before changing the choice in your bag.',true);
    // Reuse the same exact published-value matcher after the cart line itself
    // is resolved. This scoped view cannot borrow another visible product.
    var scoped={contextRevision:context.contextRevision,controlVersion:1,pageKind:'product',currentHandle:chosen.handle,visiblePieces:[],productControls:{handle:chosen.handle,productId:chosen.productId,quantity:chosen.quantity,variantId:chosen.variantId,optionGroups:chosen.optionGroups,selectedOptions:chosen.selectedOptions}},choice=controls('Select '+optionWords,norm('Select '+optionWords),scoped);
    if(!choice?.ok||choice.action.type!=='select-option'||!choice.action.optionName||!choice.action.optionValue)return Object.assign(failed(choice?.reason||'Which published option would you like for this item?',true),choice?.needsClarification===true?{needsClarification:true}:{});
    return success({type:'bag-select-option',lineId:chosen.lineId,optionName:choice.action.optionName,optionValue:choice.action.optionValue},context);
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
    var exact=exactMatches(words,context),handles=new Set(exact.map(function(p){return p.handle;})),matches=context.visiblePieces.concat(context.loadedPieces||[],context.inventoryPieces||[]).filter(function(p){return handles.has(p.handle);});
    if(handles.size!==1||!handles.has(pc.handle))return null;
    var named=matches.find(function(p){return p.handle===pc.handle;}),rest=words.replace(has(words,named.title)?norm(named.title):norm(named.handle),'');
    pc.selectedOptions.forEach(function(choice){if(has(rest,choice.value))rest=rest.replace(norm(choice.value),'');});
    if(rest.replace(/\b(?:the|my|a|an|this|that|current|selected|piece|product|listing|item)\b/g,'').trim())return null;
    return {handle:pc.handle};
  }
  function publishedOptionAlias(value,group){
    var candidate=literal(value,300),matches=group.values.filter(function(v){return literal(v,300)===candidate;});
    if(!matches.length&&/\blength\b/i.test(group.name)&&/^\d+(?:\.\d+)? inch$/.test(inchUnitText(candidate,300)))matches=group.values.filter(function(v){return inchUnitText(v,300)===inchUnitText(candidate,300);});
    if(!matches.length&&/\bengraving\b/i.test(group.name)&&/^(?:no engraving|without engraving|unengraved)$/.test(candidate))matches=group.values.filter(function(v){return /^(?:none|no engraving|without engraving|unengraved)$/.test(literal(v,300));});
    var vocabulary=root?.BritesCatalogueIntents||catalogue;
    if(!matches.length&&/^(?:metal(?: choice)?|materials?|finish)$/i.test(group.name)&&/^(?:(?:\d{1,2}\s*(?:k|kt|karats?|carats?)\s+)?(?:(?:solid|rose|white|yellow)\s+)?(?:gold(?:[ -]+(?:filled|plated))?|sterling(?:\s+silver)?|silver|gf))$/.test(candidate)&&typeof vocabulary?.material==='function'&&typeof vocabulary?.materialMatches==='function'){
      var material=vocabulary.material(candidate)?.material;if(material)matches=group.values.filter(function(v){return vocabulary.materialMatches(v,material);});
    }
    return matches.length===1?matches[0]:matches.length>1?false:null;
  }
  function bareOption(raw,context){
    var pc=context.productControls;if(!pc||context.pageKind!=='product'||context.currentHandle!==pc.handle)return null;
    var answer=raw.trim().replace(/^(?:please\s+)+/i,'').replace(/[.!?]$/,'').replace(/(?:,\s*|\s+)please$/i,'');if(/^(?:no|stop|cancel|wait|hold|not now|not yet)$/i.test(answer))return null;
    // A spoken no-engraving choice names a real option; a bare "no" still
    // cancels. Other short answers require the actual opened option menu.
    if(/^(?:no engraving|without engraving|unengraved)(?: (?:for|on|of) .+)?$/i.test(answer)){var requested=controls('Select '+answer,norm('Select '+answer),context);if(requested?.ok&&pc.optionGroups.filter(function(g){return /\bengraving\b/i.test(g.name);}).length>1)return optionAmbiguity('Which engraving menu would you like to change? Name its label.');return requested;}
    if(!pc.optionsOpen||!pc.openedOption)return null;
    var group=pc.optionGroups.find(function(g){return g.name===pc.openedOption;}),value=group&&publishedOptionAlias(answer,group);
    if(value===false)return optionAmbiguity('More than one published value matches. Name its exact label.');
    return value?success({type:'select-option',handle:pc.handle,optionName:group.name,optionValue:value},context):null;
  }
  function compoundOptions(raw,context){
    var words=norm(raw).replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*/,''),pc=context.productControls;
    if(!/^(?:choose|select|pick|set|change|use)\b|^make (?:this|that|it|the (?:current|selected))\b/.test(words)||/\b(?:quantity|cart|bag|basket|gift|shipping|checkout|search|sort|filter)\b/.test(words))return null;
    if(!pc||context.pageKind!=='product'||context.currentHandle!==pc.handle)return null;
    var payload=inchUnitText(optionRequestText(raw,context,pc.handle,true),2000),choices=[],spans=[],vocabulary=root?.BritesCatalogueIntents||catalogue,error='';
    function occurrences(key,numeric){var found=[],at=-1;while(key&&(at=payload.indexOf(key,at+1))!==-1)if((at===0||!(numeric?/[\w.+\-\u2212]/:/\w/).test(payload[at-1]))&&(at+key.length===payload.length||!/\w/.test(payload[at+key.length])))found.push({start:at,end:at+key.length});return found;}
    var namedGroups=pc.optionGroups.filter(function(g){return has(words,g.name);});
    pc.optionGroups.forEach(function(g){
      var length=/\blength\b/i.test(g.name),metal=/^(?:metal(?: choice)?|materials?|finish)$/i.test(g.name),hits=[];
      g.values.forEach(function(v){var key=length?inchUnitText(v,300):literal(v,300);occurrences(key,length).forEach(function(span){hits.push({value:v,...span});});});
      hits=hits.filter(function(hit){return !hits.some(function(other){return other.value!==hit.value&&other.start<=hit.start&&other.end>=hit.end&&other.end-other.start>hit.end-hit.start;});});
      if(metal&&typeof vocabulary?.material==='function'&&typeof vocabulary?.materialMatches==='function')for(var mention of payload.matchAll(/\b(?:(?:\d{1,2}\s*(?:k|kt|karats?|carats?)\s+)?(?:(?:solid|rose|white|yellow)\s+){0,2}(?:gold(?:[ -]+(?:filled|plated))?|sterling(?:\s+silver)?|silver|gf))\b/g)){
        var material=vocabulary.material(mention[0])?.material,values=material?g.values.filter(function(v){return vocabulary.materialMatches(v,material);}):[];
        if(values.length>1){error='Which '+g.name+' would you like: '+values.join(' or ')+'?';return;}
        if(values.length===1)hits.push({value:values[0],start:mention.index,end:mention.index+mention[0].length});
      }
      var values=Array.from(new Set(hits.map(function(hit){return hit.value;})));
      if(values.length>1){error='Which '+g.name+' would you like: '+values.join(' or ')+'?';return;}
      if(values.length===1){choices.push({name:g.name,value:values[0]});spans.push(...hits);}
    });
    if(error)return /\b(?:not|never|without|unless|if|when|once|after|yesterday|tomorrow)\b/.test(words)?failed(error,true):optionAmbiguity(error);
    // The same literal in two groups needs the requested group's exact label.
    choices=choices.filter(function(choice){var peers=choices.filter(function(other){return other.value===choice.value;});return peers.length<2||namedGroups.some(function(g){return g.name===choice.name;});});
    if(choices.length<2){
      var lengthRequest=/\b\d+(?:\.\d+)?\s*inch\b/.test(payload),materialRequest=/\b(?:gold|silver|sterling|gf)\b/.test(payload),lengthGroup=pc.optionGroups.find(function(g){return /\blength\b/i.test(g.name);}),metalGroup=pc.optionGroups.find(function(g){return /^(?:metal(?: choice)?|materials?|finish)$/i.test(g.name);});
      if(lengthRequest&&materialRequest&&lengthGroup&&metalGroup){var missing=choices.some(function(choice){return choice.name===lengthGroup.name;})?metalGroup:lengthGroup;return failed('For '+missing.name+', which would you like: '+missing.values.join(' or ')+'?',true);}
      return null;
    }
    if(/\b(?:not|never|without|unless|if|when|once|after|or|except|instead|yesterday|tomorrow)\b/.test(words)||/(?:^|[^\w.])[+\-\u2212]\s*\d+(?:\.\d+)?\s*inch\b/.test(payload))return failed('Please give one clear choice for each option on this piece.',true);
    var remainder=payload.split('');spans.forEach(function(span){for(var index=span.start;index<span.end;index++)remainder[index]=' ';var filler=/^\s+(?:one|option|choice)\b(?=\s*(?:$|[,.!?]|\b(?:in|at|with|and|for|of|on|to)\b))/.exec(payload.slice(span.end));if(filler)for(var index=span.end;index<span.end+filler[0].length;index++)remainder[index]=' ';});remainder=norm(remainder.join(''));
    pc.optionGroups.slice().sort(function(a,b){return b.name.length-a.name.length;}).forEach(function(g){if(has(remainder,g.name))remainder=remainder.replace(norm(g.name),'');});
    remainder=remainder.replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*/,'').replace(/^(?:choose|select|pick|set|change|use|make)\b/,'');
    remainder=remainder.replace(/\b(?:me|the|my|a|an|and|with|at|to|for|of|on|in|this|that|it|its|current|selected|piece|product|listing|item|option|options|choice|choices|variants|published|available|metal|metals|material|materials|size|sizes|length|lengths|please|again)\b/g,'');
    var type=norm(pc.productType);if(/\bnecklaces?\b/.test(type))remainder=remainder.replace(/\bnecklaces?\b/g,'');if(/\bearrings?\b/.test(type))remainder=remainder.replace(/\bearrings?\b/g,'');if(/\bcharms?\b/.test(type))remainder=remainder.replace(/\bcharms?\b/g,'');
    if(remainder.trim())return failed('Please name the choices for this piece, or open the piece you mean first.',true);
    return {ok:true,steps:choices.map(function(choice){return {text:'Select '+choice.name+' '+choice.value+' for '+pc.handle};})};
  }
  function controls(raw,message,context){
    var command=message.replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*/,''),enhanced=context.controlVersion===1,pc=context.productControls,t;
    if(/^(?:use|choose|select|pick|set|change)\b/.test(command))command=command.replace(/\s+instead$/,'');
    if(/^make (?:this|that|it|the (?:current|selected) (?:piece|product|listing))\b/.test(command))command=command.replace(/^make\b/,'choose');
    if(/^(?:i would like|id like|i want) (?:the |a |an )?.+ (?:option|choice|material|metal|length|size)$/.test(command))command=command.replace(/^(?:i would like|id like|i want)\b/,'choose');
    // Recognize setters even on an older adapter. Never interpret a rejected
    // quantity or option request as a motif recommendation/search.
    var rawCommand=raw.trim().replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|i'd like (?:you )?to)\s+)*/i,'');
    var bagChoice=bagControl(raw,command,context);if(bagChoice)return bagChoice;
    if(/^(?:clear|erase|remove)(?: the| my)? (?:gift (?:note|message)|(?:engraving|engraved)(?: text| message| field| wording)?)$/.test(command)){if(!enhanced)return failed('The visible text controls are unavailable on this page.',true);if(/\bgift\b/.test(command))return success({type:'gift-preferences',giftNote:''},context);if(!pc||context.pageKind!=='product'||context.currentHandle!==pc.handle||!context.engravingControls?.available||!context.engravingControls.enabled)return failed('Open a piece with an enabled published engraving field before clearing its text.',true);return success({type:'set-engraving',handle:pc.handle,text:''},context);}
    if(/^(?:set|change|write|save)(?: the| my)? (?:engraving|engraved)(?: text| message| wording)? (?:to|as)\s*[:=]?\s*$/i.test(rawCommand))return failed('Give the exact engraving text, or ask to clear the engraving field.',true);
    var engravingText=/^(?:(?:set|change|write|save)(?: the| my)? (?:engraving|engraved)(?: text| message| wording)?(?:\s+(?:to|as))?\s*[:=]?\s+|fill(?: in)?(?: the| my)? engraving(?: text| field| wording)?\s+(?:with|to)\s+)([\s\S]+)$/i.exec(rawCommand);
    if(engravingText){if(!enhanced||!pc||context.pageKind!=='product'||context.currentHandle!==pc.handle||!context.engravingControls?.available||!context.engravingControls.enabled)return failed('Open a piece with an enabled published engraving field before filling its text.',true);var text=engravingText[1];if(text.length>context.engravingControls.maxLength||!validateAction({type:'set-engraving',handle:pc.handle,text:text}))return failed('Use the published engraving limit of '+context.engravingControls.maxLength+' characters.',true);return success({type:'set-engraving',handle:pc.handle,text:text},context);}
    var reversedQuantity=/^(?:set|change|update|make)(?: the| my)? quantity (?:of|for) (.+?) to ([^\s]+)\s*$/i.exec(rawCommand);
    var directQuantity=/^(?:set|change|update|make)(?: the| my)? (?:(.+?) )?quantity(?: to| of)? ([^\s]+)(?: (?:(?:for|of) (.+)|((?:in|into) (?:my |the )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket))))?\s*$/i.exec(rawCommand);
    var quantity=reversedQuantity?reversedQuantity[2]:directQuantity?directQuantity[2]:null;
    if(quantity!==null){
      if((command.match(/\bquantity\b/g)||[]).length!==1||/\b(?:or|instead|except)\b/.test(command))return failed('Give one exact quantity request.',true);
      var numberWord=quantity.toLowerCase().replace(/[.!?]$/,'');if(!/^\d+$/.test(numberWord)&&!Object.hasOwn(QUANTITIES,numberWord))return failed('Choose a whole quantity from 1 to 20.',true);
      var n=Object.hasOwn(QUANTITIES,numberWord)?QUANTITIES[numberWord]:Number(numberWord);if(!enhanced)return failed('Quantity controls are unavailable on this page. Use the visible quantity control.',true);if(!Number.isInteger(n)||n<1||n>20)return failed('Choose a whole quantity from 1 to 20.',true);
      var quantityWords=norm(reversedQuantity?reversedQuantity[1]:directQuantity[3]||directQuantity[1]||'');if(directQuantity&&directQuantity[1]&&directQuantity[3]&&!/^(?:(?:the|my) )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket)$/.test(norm(directQuantity[1]))&&!has(quantityWords,directQuantity[1]))return failed('Give one exact product or bag line for the quantity change.',true);
      if(/\b(?:bag|cart|basket)\b/.test(command)||context.pageKind==='bag'){
        var bagWords=quantityWords.replace(/(?:^|\s)(?:in|into) (?:my |the )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket)$/,'').trim(),line=bagTarget(bagWords||'quantity',context);
        return line?success({type:'bag-quantity',lineId:line.lineId,quantity:n},context):failed('Choose the exact bag line and quantity.',true);
      }
      t=productQuantityTarget(quantityWords,context);return t?success({type:'product-quantity',handle:t.handle,quantity:n},context):failed('Open the exact product before changing its quantity.',true);
    }
    if(/^(?:set|change|update|make)(?: the| my)? (?:[a-z0-9 ]{1,300} )?quantity\b/.test(command))return failed('Choose a whole quantity from 1 to 20.',true);
    var giftChoice=/^(enable|disable|add|remove|use|choose|turn on|turn off) (.+)$/.exec(command);
    if(giftChoice&&/\b(?:gift (?:wrapping|wrap|package|packaging)|wrapping)\b/.test(giftChoice[2])){if(!enhanced)return failed('Gift preferences are unavailable on this page. Use the visible gift controls.',true);var on=!/^(?:disable|remove|turn off)$/.test(giftChoice[1]),body=giftChoice[2].replace(/ (?:to|from|in) (?:my |the )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket)$/,''),parts=body.split(/\s+and\s+/),preferences={type:'gift-preferences'},valid=true;parts.forEach(function(part){var field=/^(?:the |my |a |an )?(?:gift )?(wrapping|wrap|package|packaging)$/.exec(part);if(!field){valid=false;return;}var key=/^(?:wrapping|wrap)$/.test(field[1])?'wrapping':'giftPackage';if(Object.hasOwn(preferences,key))valid=false;preferences[key]=on;});if(!valid)return failed('Give one clear wrapping or gift package request, or request both with the same choice.',true);return success(preferences,context);}
    var removeBag=/^(?:remove|delete)\b/.test(command)&&(/\b(?:bag|cart|basket)\b/.test(command)||context.pageKind==='bag');
    if(removeBag&&enhanced){var removal=/^(?:remove|delete) (.+?)(?: (?:from|in) (?:my |the )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket))?$/.exec(command),remove=removal&&bagTarget(removal[1],context);return remove?success({type:'bag-remove',lineId:remove.lineId},context):failed('Choose the exact bag line to remove; its options distinguish matching titles.',true);}
    if(!enhanced)return null;
    if(/^(?:set|write|save|add|change)(?: the| my)? gift (?:note|message)\b/.test(command)){var note=raw.trim().match(/^(?:(?:please|could you|would you|can you|will you)\s+)*(?:set|write|save|add|change)(?: the| my)? gift (?:note|message)(?:\s+to)?\s*[:=]?\s+([\s\S]+)$/i);if(!note||note[1].length>300)return failed('Give the exact gift note in 300 characters or fewer.',true);return success({type:'gift-preferences',giftNote:note[1]},context);}
    if(/^(?:add|put|review|prepare)\b/.test(command)&&/\b(?:bag|cart|basket|adding|add)\b/.test(command)){
      t=controlTarget(command,context);
      if(!t.handle||!pc||context.pageKind!=='product'||context.currentHandle!==pc.handle||pc.handle!==t.handle)return failed('Open the exact piece you want to add so I can help with its published options.',true);
      var directAdd=/^(?:add|put)\b/.test(command)&&typeof pc.selectionStatus==='string';
      var explicitChoice=pc.optionGroups.flatMap(function(g){return g.values.filter(function(v){return has(command,v);}).map(function(v){return {name:g.name,value:v};});});
      if(explicitChoice.some(function(v){return !pc.selectedOptions.some(function(s){return s.name===v.name&&s.value===v.value;});}))return failed('Those options differ from the current selection. Select the exact option before adding.',true);
      var reviewWords=optionRequestText(command,context,t.handle);pc.optionGroups.forEach(function(g){[g.name].concat(g.values).sort(function(a,b){return b.length-a.length;}).forEach(function(v){if(has(reviewWords,v))reviewWords=reviewWords.replace(norm(v),'');});});
      if(reviewWords.replace(/\b(?:add|put|review|prepare|adding|to|in|into|my|the|a|an|test|mock|demo|sandbox|bag|cart|basket|please|current|selected|exact|piece|pieces|product|listing|item|it|its|this|that|of)\b/g,'').trim())return failed('Name the exact current piece and quantity before adding it.',true);
      if(!directAdd&&(!pc.variantId||pc.selectedOptions.length!==pc.optionGroups.length))return failed('Choose every exact published option first, then review adding this piece.',true);
      return success(Object.assign({type:directAdd?'add':'review-add',handle:t.handle},pc.variantId?{variantId:pc.variantId}:{}),context);
    }

    if(/^(?:choose|select|set|use|change)\b/.test(command)&&/\b(?:standard|express) shipping\b/.test(command)){if(context.pageKind!=='checkout')return failed('Open the labelled test checkout before choosing a demo shipping option.',true);return success({type:'checkout-option',option:'shipping',value:/\bexpress\b/.test(command)?'express':'standard'},context);}
    if(/^(?:go|open|show|move|continue|next|back|return)\b/.test(command)&&(/\bcheckout (?:review|shipping|confirm|confirmation)\b/.test(command)||/\b(?:review|shipping|confirm|confirmation) step\b/.test(command)||/^(?:next|continue)(?: to)?(?: the)? (?:step|checkout step)$/.test(command))){if(context.pageKind!=='checkout'||!context.checkoutControls)return failed('Open the labelled test checkout first.',true);var step=/\bshipping\b/.test(command)?'shipping':/\bconfirm(?:ation)?\b/.test(command)?'confirm':/\breview\b/.test(command)?'review':context.checkoutControls.step==='review'?'shipping':context.checkoutControls.step==='shipping'?'confirm':null;return step?success({type:'checkout-step',step:step},context):failed('Use the visible acknowledgement and confirmation to complete the test checkout.',true);}
    if(/^(?:complete|finish|confirm)(?: the| my)? (?:test|mock|demo|sandbox) checkout$/.test(command))return success({type:'checkout-complete'},context);
    var openOptions=/^(?:open|show|expand|display)(?: me)?(?: the| my)?\b/.test(command)&&/\b(?:options|dropdown|drop down|selector|menu|metal|material|length|size)\b/.test(command)&&!/\b(?:shipping|gift|price|details|materials for)\b/.test(command);
    if(openOptions){t=controlTarget(command,context);if(!t.handle)return failed(t.reason,true);if(!pc||context.pageKind!=='product'||context.currentHandle!==pc.handle||pc.handle!==t.handle)return failed('Open that exact piece before choosing one of its published option menus.',true);var optionWords=optionRequestText(command,context,t.handle),opened=optionGroup(optionWords,pc);if(opened===false)return optionAmbiguity('More than one published option group matches. Name its exact label.');if(/\b(?:metal|material|length|size)\b/.test(optionWords)&&!opened)return failed('Open the exact product to choose a published option group.',true);if(optionRequestRemainder(command,context,t.handle,opened?.name))return failed('Choose one menu for the piece you are viewing.',true);return success(Object.assign({type:'options',handle:t.handle},opened?{optionName:opened.name}:{}),context);}
    if(/^(?:choose|select|pick|set|change|use)\b/.test(command)&&!/\b(?:search|sort|filter|gift|shipping|checkout|quantity)\b/.test(command)){
      if(!pc||context.pageKind!=='product'||context.currentHandle!==pc.handle)return failed('Open the exact piece before selecting a published option.',true);
      t=controlTarget(command,context);if(!t.handle||t.handle!==pc.handle)return failed('Name the current exact product before selecting its published option.',true);
      var choiceWords=optionRequestText(command,context,pc.handle),group=optionGroup(choiceWords,pc);if(group===false)return optionAmbiguity('More than one published option group matches. Name its exact label.');var groups=group?[group]:pc.optionGroups,payload=optionRequestText(raw,context,pc.handle,true),inchPayload=inchUnitText(payload,2000),matches=[];
      if(/(?:^|[^\w.])[+\-\u2212]\s*\d+(?:\.\d+)?\s*inch\b/.test(inchPayload))return failed('Name one exact published length without a numeric sign.',true);
      groups.forEach(function(g){g.values.forEach(function(v){var key=literal(v,300),at=payload.indexOf(key),numericLength=/^\d+(?:\.\d+)? inch$/.test(inchUnitText(v,300)),leftBoundary=numericLength?/[\w.+\-\u2212]/:/\w/;if(at>=0&&(at===0||!leftBoundary.test(payload[at-1]))&&(at+key.length===payload.length||!/\w/.test(payload[at+key.length])))matches.push({name:g.name,value:v});});});
      if(!matches.length){if(!/(?:^|\s)[+-]\s*\d/.test(inchPayload))groups.forEach(function(g){g.values.forEach(function(v){var key=inchUnitText(v,300);if(!/^\d+(?:\.\d+)? inch$/.test(key))return;var at=inchPayload.indexOf(key);if(at>=0&&(at===0||!/[\w.+-]/.test(inchPayload[at-1]))&&(at+key.length===inchPayload.length||!/\w/.test(inchPayload[at+key.length])))matches.push({name:g.name,value:v,unitAlias:true,lengthGroup:/\blength\b/i.test(g.name),key:key});});});}
      if(!matches.length){var negative=payload.match(/\b(?:no engraving|without engraving|unengraved)\b/);if(negative){var engravingGroups=pc.optionGroups.filter(function(g){return /\bengraving\b/i.test(g.name);}),negativeAmbiguous=false;if(engravingGroups.length>1)return optionAmbiguity('Which engraving menu would you like to change? Name its label.');groups.filter(function(g){return engravingGroups.includes(g);}).forEach(function(g){var alias=publishedOptionAlias(negative[0],g);if(alias===false)negativeAmbiguous=true;else if(alias)matches.push({name:g.name,value:alias,optionAlias:true,key:negative[0]});});if(negativeAmbiguous)return optionAmbiguity('Which no-engraving choice would you like? Name its label.');}}
      if(!matches.length){var aliasWords=optionRequestRemainder(command,context,pc.handle,group?.name),ambiguous=false;groups.filter(function(g){return /^(?:metal(?: choice)?|materials?|finish)$/i.test(g.name);}).forEach(function(g){var alias=publishedOptionAlias(aliasWords,g);if(alias===false)ambiguous=true;else if(alias)matches.push({name:g.name,value:alias,optionAlias:true,key:aliasWords});});if(ambiguous)return optionAmbiguity('More than one published option value matches. Name its exact label.');}
      if(matches.length>1)return optionAmbiguity('More than one published option value matches. Name its exact label.');
      if(matches.length!==1||(matches[0].unitAlias?!matches[0].lengthGroup:!matches[0].optionAlias&&!has(choiceWords,matches[0].value)))return failed('Name one exact published option value; I will keep the other choices unchanged.',true);
      var exactCommand=matches[0].unitAlias?norm(inchPayload.replace(matches[0].key,literal(matches[0].value,300))):matches[0].optionAlias?command.replace(matches[0].key,norm(matches[0].value)):command;
      if(optionRequestRemainder(exactCommand,context,pc.handle,matches[0].name,matches[0].value))return failed('Choose one option for the piece you are viewing.',true);
      return success({type:'select-option',handle:pc.handle,optionName:matches[0].name,optionValue:matches[0].value},context);
    }
    return null;
  }
  function guidedOptions(message,context){
    var pc=context.productControls;if(context.pageKind!=='product'||!pc||context.currentHandle!==pc.handle)return null;
    var command=message.replace(/^(?:(?:please|could you|would you|can you|will you)\s+)*/,''),request=/^(?:help me (?:choose|pick)(?: (?:the )?(?:options|choices|materials|metal|length|size))?|(?:walk|guide) me through (?:the |my |its )?(?:published )?(?:options|choices|selection)|what should i (?:choose|pick|select))(?: (?:for|of) (.+)| (this|that|it|this piece|the current product))?$/.exec(command);
    if(!request)return null;
    var subject=request[1]||request[2],selected=subject?target(subject,context,true):{handle:pc.handle};
    if(selected.handle!==pc.handle)return failed('Open that exact piece first so I can walk you through its own options.',true);
    var group=optionGroup(command,pc);if(group===false)return failed('Name one published option group to work through first.',true);
    var next=group?.name||pc.nextOptionName||null;
    return success(Object.assign({type:'options',handle:pc.handle},next?{optionName:next}:{}),context);
  }
  function semanticDiscovery(raw,context){
    var vocabulary=root?.BritesCatalogueIntents||catalogue;if(typeof vocabulary?.discovery!=='function')return null;
    var command=norm(raw).replace(/^(?:(?:please|could you|would you|can you|will you)\s+)*/,'');
    // Explicit controls always pass their exact current target checks. Shared
    // discovery cannot turn a rejected setter or a sequence into a search.
    if(/^(?:stop|cancel|wait|hold|not now|not yet|use|change|enable|disable|help|sort|order|arrange|filter|reset|clear)\b/.test(command)||/\b(?:do not|dont|never)\b/.test(command)||/^(?:only show|show only|show available only)\b/.test(command)||/\b(?:then|and)\s+(?:(?:please|can you)\s+)?(?:open|highlight|zoom|enlarge|checkout|check out|add|place|pay|scroll|select|choose|set|change|remove|delete|enable|disable|finish|complete)\b/.test(command)||/\b(?:not yet|hold off)\b/.test(command))return null;
    var repaired=command.replace(/^(?:no\s+(?:i )?(?:meant|mean)|i (?:meant|mean)|actually|instead)\s+/,'');if(repaired!==command&&/^(?:use|change|enable|disable|select|choose|pick|set|add|put|remove|delete|help|sort|order|arrange|filter|reset|clear)\b/.test(repaired))return null;
    if(collectionReset(command)||categoryBrowse(command))return null;
    // Existing direct navigation/search grammars retain their exact payloads
    // and scope. This extension handles catalogue questions and repairs that
    // used to fall through to product knowledge or a bare cancellation.
    var directDiscovery=/^(?:search|find|look for|look up|browse|list|recommend|suggest|show|pull up|display|open|take|navigate|visit|go|view|see|proceed|continue|sort|order|arrange|filter|reset|clear|highlight|scroll|zoom|enlarge|make|check|help|let me see)\b/.test(command);
    if(exactMatches(command,context).length&&/\b(?:see|view|open|visit|navigate)\b/.test(command))return null;
    if(/\b(?:custom|customize|customise|personalize|personalise|engrave|engraving|discount|coupon|offer code|promo|meaning|mean|means|symbolism|story|history|image|photo|picture|gallery|gift)\b/.test(command))return null;
    if(/\b(?:materials?|metals?)\b/.test(command))return null;
    var catalogueQuestion=/^(?:what (?:do you have|have you got|else do you have|is available|s in)|what(?:s| is) (?:in|available)|(?:what|which|how many) .+ (?:do you (?:have|offer|sell|stock|carry)|have you got|are available|are in stock)|do you (?:have|offer|sell|stock|carry)|have you got|can i (?:see|browse|view)|can you (?:show|find|search|browse|list))\b/.test(command);
    var prior=context.search||({'regular-necklaces':'regular necklaces','beady-necklaces':'beady necklaces','stud-earrings':'stud earrings','hoop-earrings':'hoop earrings','charm-only':'charm only'}[context.filter]||(!['all','available'].includes(context.filter)?context.filter:'')),found;
    try{found=vocabulary.discovery(raw,prior,{currency:context.productControls?.selectedVariant?.currency||context.currentProduct?.currency||'USD'});}catch{return null;}
    if(!found?.recognized)return null;if(found.denied)return failed('I will leave the website as it is.',true);
    if(found.mode==='browse')return success({type:'filter',filter:'all'},context);
    var collectionRefinement=found.mode==='refine'&&['collection','catalogue','search'].includes(context.pageKind)&&!!prior&&(found.plan?.materialExplicit===true||found.plan?.budgetExplicit===true);
    if(directDiscovery||informationRequest(command)&&!catalogueQuestion&&!collectionRefinement)return null;
    // A bare material/budget follow-up while looking at a product concerns its
    // options. It cannot silently replace the page with a global collection.
    if(context.pageKind==='product'&&found.mode==='refine')return null;
    if(!plain(found.query,180))return failed('Use a search of 180 characters or fewer.',true);
    var action={type:'search',query:found.query},categories=found.plan?.categories||[],sort=sortFor(norm(raw));
    if(categories.length===1&&FILTERS.includes(categories[0]))action.filter=categories[0];else if(categories.length>1||found.plan?.excludedCategories?.length)action.filter='all';if(sort)action.sort=sort;
    return success(action,context);
  }
  function resolveIntent(value,rawContext){
    var raw=typeof value==='string'?value:value&&value.message,context=snapshot(rawContext||value&&value.context),message=norm(raw);
    if(!plain(raw,2000))return failed('Please use a short, plain request.',false);
    if(!message)return failed('',false);
    // A shopper can correct a choice without restarting the conversation.
    // Only a new explicit command follows this repair prefix; a bare no, wait,
    // stop or a negative/conditional replacement retains its normal refusal.
    var repair=/^(?:(?:actually|sorry|correction)\s*,?\s+|no\s*,\s*)(?=(?:(?:please|can you|could you|would you|will you)\s+)*(?:open|show|view|select|choose|pick|set|change|use|add|put|scroll|highlight|go|take|undo|reverse|revert)\b)/i;
    if(repair.test(raw.trim())){raw=raw.trim().replace(repair,'').replace(/\s+instead[.!?]?$/i,'');message=norm(raw);}
    // Shopper-authored form text is an opaque literal. Command-looking words
    // inside it fill one verified field and cannot authorize another action.
    if(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|i'd like (?:you )?to)\s+)*(?:(?:set|change|write|save|add)(?: the| my)? gift (?:note|message)\b|(?:set|change|write|save)(?: the| my)? (?:engraving|engraved)(?: text| message| wording)?\b|fill(?: in)?(?: the| my)? engraving(?: text| field| wording)?\b)/i.test(raw)){var field=controls(raw,message,context);if(field)return field;}
    if(/^\s*["“‘'][\s\S]*["”’']\s*$/.test(raw))return failed('Quoted words are not a current request to change the website.',true);
    var control=/\b(?:search|sort|filter|show|open|zoom|enlarge|highlight|scroll|checkout|check out|bag|cart|price|materials|options|shipping|offers|set|change|select|choose|pick|use|enable|disable|quantity|remove|delete|gift|complete|finish)\b/.test(message);
    if(/javascript\s*:|<\s*script\b|\b(?:document|window)\s*[.\[]|\b(?:eval|fetch)\s*\(|\b(?:execute|run)\s+(?:code|script|javascript)|\b(?:css selector|system prompt|developer prompt)|\b(?:ignore|override|bypass)\b.{0,100}\b(?:instructions|safety|rules|guard|previous|system)\b|https?:\/\//i.test(raw))return failed('I can use the visible shop controls, but cannot execute code, external links or override instructions.',true);
    if(/^(?:say|repeat|quote|read out|write(?! (?:the |my )?gift (?:note|message)\b)|print|teach|tell me how|how (?:do|would|can) (?:i|you)|what (?:would|happens if))\b/.test(message)||/^(?:if|when|unless|once|after|suppose|imagine)\b/.test(message)||/\b(?:if|when|once|after) (?:i|you)\b/.test(message)||/\b(?:yesterday|last time|previously|tomorrow|do it later)\b/.test(message))return failed('That is not a current request to change the test website.',false);
    var offeredChoice=bareOption(raw,context);if(offeredChoice)return offeredChoice;
    var semantic=semanticDiscovery(raw,context);if(semantic)return semantic;
    if(/^(?:no\b|stop\b|cancel\b|wait\b|hold\b|not now|not yet)/.test(message)||/\b(?:do not|dont|never)\s+(?:(?:please|want|need|you|to|do)\s+){0,5}(?:search|sort|filter|show|open|zoom|enlarge|highlight|scroll|checkout|check|add|place|set|select|choose|change|remove|delete|use|enable|disable|complete|finish)\b/.test(message)||/\b(?:not yet|hold off)\b/.test(message))return failed('I will leave the website as it is.',control);
    if(/\b(?:place|submit|pay for|purchase|complete)\b.{0,50}\b(?:order|payment|purchase)\b|\breal checkout\b/.test(message))return failed('This test storefront can demonstrate checkout; it cannot place or pay for a real order.',true);
    if(/\b(?:then|and)\s+(?:(?:please|can you)\s+)?(?:open|highlight|zoom|enlarge|checkout|check out|add|place|pay|scroll|select|choose|set|change|remove|delete|enable|disable|finish|complete)\b/.test(message))return failed('Ask for one website action at a time so the target remains clear.',true);
    var navCommand=message.replace(/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*/,'').replace(/\s+please$/,'');
    if(/^(?:(?:go|navigate|take me|bring me)(?: back)?(?: to)?|(?:return|return me)(?: back)?(?: to)?|(?:open|view|show(?: me)?|display|visit)) (?:the |your |our |my )?(?:home|homepage|home page|main page)$/.test(navCommand))return success({type:'home'},context);
    if(/^(?:undo|reverse|revert)(?: that| this| it| the last (?:change|action)| my last (?:change|action))?$/.test(navCommand)||/^(?:go|take me) back to (?:the )?(?:previous|last) (?:selection|choice|option)$/.test(navCommand))return success({type:'undo'},context);
    if(/^(?:close|hide|collapse|dismiss)(?: the| my)? (?:(?:metal|material|length|size|options?) )?(?:menu|dropdown|drop down|selector|options|choices)$/.test(navCommand))return success({type:'close-options'},context);
    if(/^(?:close|hide|dismiss)(?: the| my)? (?:(?:enlarged|zoomed|large|current) )?(?:image|photo|picture|zoom|image viewer)$/.test(navCommand))return success({type:'close-image'},context);
    if(/^(?:go |navigate |take me |move )?back(?: (?:one |a |the previous )?page)?$/.test(navCommand))return success({type:'back'},context);
    if(/^(?:(?:go|take me|navigate(?: me)?|return) )?back to (?:the |my )?(?:(?:previous|original|search) )?(?:results|collection|catalogue|catalog|list)$/.test(navCommand))return success({type:'back'},context);
    if(collectionReset(message))return success({type:'filter',filter:'all'},context);
    var browseCategory=categoryBrowse(message);if(browseCategory)return success({type:'filter',filter:browseCategory},context);
    var guided=guidedOptions(message,context);if(guided)return guided;
    if(informationRequest(message)){var known=resolveKnowledgeTarget(raw,context);return Object.assign(failed('',false),{delegated:'knowledge'},known.handle?{targetHandle:known.handle}:{});}
    if(/^(?:go |navigate |take me |move )?back(?: (?:one |a |the previous )?page)?$/.test(navCommand))return success({type:'back'},context);
    if(/^(?:go |navigate |take me |move )?forward(?: (?:one |a |the next )?page)?$/.test(navCommand))return success({type:'forward'},context);
    var direction=/^(?:scroll|move)(?: the)?(?: (?:page|website))? (up|down|top|bottom)(?: (?:a|one) (?:page|screen)|(?: the)? (?:page|website)|(?: a)? (?:little|bit))?$/.exec(navCommand)||/^(?:scroll|go|take me)(?: (?:the page|the website))? to (?:the )?(top|bottom)$/.exec(navCommand);
    if(direction)return success({type:'scroll',direction:direction[1]},context);
    var galleryCommand=/^(?:open|show|display|select|choose|view)(?: me)?(?: the)? (?:image|photo|picture)(?: number)? (\d+)(?: (?:for|of) (.+))?$/.exec(navCommand)||/^(?:open|show|display|select|choose|view)(?: me)?(?: the)? (first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|last|next|previous) (?:image|photo|picture)(?: (?:for|of) (.+))?$/.exec(navCommand)||/^(next|previous) (?:image|photo|picture)(?: (?:for|of) (.+))?$/.exec(navCommand);
    if(galleryCommand){var imageTarget=target(galleryCommand[2]?norm(galleryCommand[2]):'this piece',context,true),gc=context.galleryControls;if(!imageTarget.handle)return failed(imageTarget.reason,true);if(!gc||gc.handle!==imageTarget.handle)return failed('Open that piece to choose one of its published images.',true);var imageIndex=/^\d+$/.test(galleryCommand[1])?Number(galleryCommand[1]):galleryCommand[1]==='last'?gc.imageCount:galleryCommand[1]==='next'?gc.selectedIndex+1:galleryCommand[1]==='previous'?gc.selectedIndex-1:ORDINALS[galleryCommand[1]]+1;if(!Number.isInteger(imageIndex)||imageIndex<1||imageIndex>gc.imageCount)return failed('Choose a published image from 1 to '+gc.imageCount+'.',true);return success({type:'gallery',handle:imageTarget.handle,index:imageIndex},context);}
    var controlled=controls(raw,message,context);if(controlled)return controlled;
    if(/\b(?:add|put|remove|delete|empty|clear)\b.{0,100}\b(?:cart|bag|basket)\b/.test(message))return Object.assign(failed('Exact options need the existing review and Confirm flow.',false),{delegated:'review'});
    var direct=/^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*(?:search|find|look for|look up|browse|list|recommend|suggest|show|pull up|display|open|take me|navigate(?: me)?|visit|go|view|see|proceed|continue|sort|order|arrange|filter|reset|clear|highlight|scroll|zoom|enlarge|make|check|help|let me see)\b/.test(message);
    var sort=sortFor(message),cat=category(message),section=sectionFor(message),match;
    if(direct&&/\b(?:check out|checkout)\b/.test(message)||/^(?:i want to|id like to|i would like to|can i) check out\b/.test(message))return success({type:'checkout'},context);
    if(direct&&(/\b(?:my|the|test|sandbox) (?:cart|bag|basket)\b/.test(message)||/^(?:(?:please|could you|would you|can you|will you)\s+)*(?:open|show(?: me)?|view|display|see|go to|navigate(?: me)? to|take me to|visit|let me see) (?:bag|cart|basket)$/.test(message)))return success({type:'bag'},context);
    if(direct&&/\b(?:sort|order|arrange)\b/.test(message)){if(!sort&&/\bby price\b/.test(message))return failed('Choose lowest price first or highest price first.',true);return sort?success({type:'sort',sort:sort},context):failed('Choose price order, alphabetical order or featured order.',true);}
    if(direct&&/\b(?:clear|reset)\b.{0,20}\bfilters?\b/.test(message))return success({type:'filter',filter:'all'},context);
    if(direct&&/\b(?:filter|only show|show only|available only|in stock only)\b/.test(message)){var filter=/\b(?:available|in stock)\b/.test(message)?'available':cat;if(!filter&&/\b(?:all|everything|reset|clear)\b/.test(message))filter='all';return filter?success({type:'filter',filter:filter},context):failed('The test shop can filter by jewelry category or current availability.',true);}
    if(direct&&/\b(?:zoom|enlarge|magnify|close up|bigger (?:image|photo|picture)|larger (?:image|photo|picture)|make (?:it|this|the (?:image|photo|picture)) (?:bigger|larger))\b/.test(message)){if(/\b(?:image|photo|picture)(?: number)? \d+\b|\b(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|last|next|previous) (?:image|photo|picture)\b/.test(message))return failed('Choose the requested image before enlarging it.',true);var zoomTarget=target(message,context,true);return zoomTarget.handle?success({type:'zoom',handle:zoomTarget.handle},context):failed(zoomTarget.reason,true);}
    var customize=/\b(?:engrave|engraving|engraved|custom designs?|custom options?|new design|my design|design my own|customize|customise|customization|customisation|personalize|personalise|personalization)\b/.test(message);
    if(customize&&!(section==='engraving'&&(/\b(?:highlight|scroll|focus)\b/.test(message)||/\bengraving field\b/.test(message)))&&(direct||/^(?:can (?:i|you)|is it possible|i want|id like|i would like)\b/.test(message))){var customTarget=target(message,context,false),customAction={type:'customize',section:'customize'};if(customTarget.handle)customAction.handle=customTarget.handle;else if(/\b(?:this|that|it|current|selected|hovering|pointing)\b/.test(message))return failed(customTarget.reason,true);return success(customAction,context);}
    var question=/^(?:what|whats|how much|how long|where|which|are|is|does|do|can i|can you tell me|tell me|show me)\b/.test(message);
    if(section&&['shipping','gifts','offers'].includes(section)&&direct){return success(section==='gifts'?{type:'gift',section:'gifts'}:{type:/\bscroll\b/.test(message)?'scroll':'highlight',section:section},context);}
    if(section&&PRODUCT_SECTIONS.includes(section)&&direct&&!/^\b(?:find|search|look for|browse)\b/.test(message)&&!/\b(?:recommend|should i|would you recommend|all (?:pieces|products)|your (?:pieces|products|jewelry|jewellery)|a gift for)\b/.test(message)){
      var specific=target(message,context,true);if(specific.handle)return success({type:section==='image'&&/\b(?:larger|bigger|zoom|enlarge)\b/.test(message)?'zoom':/\bscroll\b/.test(message)?'scroll':'highlight',handle:specific.handle,...(section==='image'&&/\b(?:larger|bigger|zoom|enlarge)\b/.test(message)?{}:{section:section})},context);
      if(direct&&/\b(?:open|scroll|highlight|zoom|enlarge)\b/.test(message)||/\b(?:this|that|it|the|current|selected|how much|price|cost)\b/.test(message))return failed(specific.reason,true);
    }
    if(direct&&/\bscroll\b/.test(message)){if(section)return success({type:'scroll',section:section},context);if(/\b(?:top|start)\b/.test(message))return success({type:'scroll',section:'catalogue'},context);return failed('Name the part to show, such as the catalogue, price, shipping or gift options.',true);}
    if(direct){
      var openTarget=target(message,context,false);
      var searchVerb=/\b(?:search|find|browse|look for|look up)\b/.test(message),broadQuery=/\b(?:under|above|over|between|for|all|some|cheapest|expensive|necklaces|earrings|bracelets|rings|charms)\b/.test(message);
      if(openTarget.handle&&/\b(?:open|view|show|pull up|display|take me|go to|navigate(?: me)? to|visit|see)\b/.test(message)&&!searchVerb&&(!broadQuery||openTarget.match==='pointed'||/\b(?:open|view|navigate|visit|see)\b/.test(message)))return success({type:'open',handle:openTarget.handle},context);
      if(/\b(?:open|view|navigate|visit)\b/.test(message)&&!/\b(?:catalogue|catalog|collection|collections|shop|all|everything)\b/.test(message))return failed(openTarget.reason,true);
      if(/\b(?:catalogue|catalog|collection|collections|shop|all jewelry|all jewellery|all pieces|everything)\b/.test(message)&&!searchVerb&&!cat&&!sort)return success({type:'filter',filter:'all'},context);
      match=raw.trim().match(/^(?:(?:please|could you|would you|can you|will you|I want (?:you )?to|I would like (?:you )?to|I'd like (?:you )?to)\s+)*(?:search(?: (?:the (?:catalogue|catalog|shop|website)))?(?: for)?|find|look (?:for|up)|browse|list|recommend(?: me)?|suggest(?: me)?|show(?: me)?|pull up|display|let me see)\s+(.+)$/i);
      if(match){var query=match[1].replace(/^the\s+/i,'').replace(/^["“]|["”]$/g,'').replace(/\b(?:cheapest|lowest priced|most expensive|highest priced|least expensive)\b/ig,'').replace(/\b(?:sort(?:ed)? (?:by )?price (?:low to high|high to low)|from (?:low to high|high to low))\b/ig,'').replace(/\s+/g,' ').trim();
        if(!query||query.length>180)return failed('Use a search of 180 characters or fewer.',true);
        if(cat&&norm(query).replace(/\b(?:all|only|a|an|some|the|please)\b/g,'').trim().replace(/s$/,'')===cat.replace(/s$/,'')&&!sort)return success({type:'filter',filter:cat},context);
        if(/\b(?:recommend|would look|should i|good gift|best gift|help me choose|for my|for mom|for dad)\b/.test(norm(query)))return failed('',false);
        var searchAction={type:'search',query:query};if(sort)searchAction.sort=sort;if(cat)searchAction.filter=cat;
        // A negative category is a search constraint, never a positive native
        // filter. Multiple requested categories share the unfiltered host and
        // retain their OR relationship in the complete search query.
        var vocabulary=root?.BritesCatalogueIntents||catalogue,searchPlan;try{searchPlan=vocabulary?.plan?.(query);}catch{}
        if(searchPlan?.categories?.length>1||searchPlan?.excludedCategories?.length)searchAction.filter=searchPlan.categories.length===1&&FILTERS.includes(searchPlan.categories[0])?searchPlan.categories[0]:'all';
        return success(searchAction,context);
      }
    }
    return failed('',false);
  }
  function privateTextRequest(raw){return /^(?:(?:please|could you|would you|can you|will you|i want (?:you )?to|i would like (?:you )?to|i'd like (?:you )?to)\s+)*(?:(?:set|change|write|save|add)(?: the| my)? gift (?:note|message)\b|(?:set|change|write|save)(?: the| my)? (?:engraving|engraved)(?: text| message| wording)?\b|fill(?: in)?(?: the| my)? engraving(?: text| field| wording)?\b)/i.test(raw);}
  function redactRequestText(value){return typeof value==='string'&&(privateTextRequest(value)||value.split(/,?\s+(?:and then|then|and)\s+/i).some(privateTextRequest))?'Fill the exact private text in the requested visible shop field.':value;}
  function planQuestion(text,context,pendingScope){
    var words=norm(text).replace(/^(?:(?:please|can you|could you|would you|will you)\s+)*/,'');
    if(!/^(?:tell me|explain|describe|compare|what|whats|which|why|how|is|are|does|do|can i|could i)\b/.test(words))return null;
    if(!/\b(?:prices?|costs?|how much|cheapest|materials?|metals?|silver|sterling|gold|available|availability|stock|options?|variants?|sizes?|lengths?|inches|dimensions?|measurements?|diameter|width|height|thickness|details|description|made|nickel|allerg\w*|hypoallergenic|sensitive skin|care|clean\w*|polish\w*|tarnish\w*|waterproof|water|shower\w*|swim\w*|durability|everyday|daily)\b/.test(words)||/\b(?:cart|bag|basket|checkout|order|payment|shipping|delivery|gift|engraving|engraved|personalization|personalisation|password|address|email|private|meaning|symbolism|story|history|remembrance|graduation|birthday|anniversary|open|navigate|visit|scroll|highlight|zoom|select|choose|pick|set|change|fill|write|save|add|put|review|prepare|remove|delete|enable|disable|close|hide|undo|reverse|proceed|continue|sort|filter|click|submit|pay|purchase)\b/.test(words))return {ok:false,reason:'Only checked public product facts can be answered within a website sequence. Ask other questions separately.'};
    var target=resolveKnowledgeTarget(text,context);
    if(!target.handle&&!(pendingScope&&/^Choose a listing/.test(target.reason||'')))return {ok:false,reason:target.reason||'Name the exact piece you want to know about.'};
    return {ok:true,...(target.handle?{handle:target.handle}:{})};
  }
  function resolvePlan(value,rawContext){
    var raw=typeof value==='string'?value:value&&value.message,context=snapshot(rawContext||value&&value.context);
    if(!plain(raw,2000)||privateTextRequest(raw))return failed('',false);
    if(/^\s*["“‘'][\s\S]*["”’']\s*$/.test(raw))return failed('Quoted words are not a current request to change the website.',true);
    var parts=raw.trim().split(/,?\s+(?:and then|then)\s+|,?\s+and\s+(?=(?:(?:please|can you|could you|would you|will you)\s+)*(?:open|view|show|display|take|go|navigate|visit|scroll|highlight|zoom|enlarge|magnify|select|choose|pick|set|change|fill|write|save|use|add|put|review|prepare|remove|delete|enable|disable|close|hide|collapse|dismiss|undo|reverse|revert|proceed|continue|sort|filter|tell|explain|describe|compare|what|whats|which|why|how|is|are|does|do|can i|could i)\b)/i),steps=[];
    if(parts.length>4)return failed('Use up to four clear website actions in one request.',true);
    for(var part of parts){
      if(privateTextRequest(part)&&parts.length>1)return failed('Give engraving or gift-note text as its own request so its exact content stays private.',true);
      var command=part.trim().replace(/^(?:(?:please|can you|could you|would you|will you|i want (?:you )?to|i would like (?:you )?to|i'd like (?:you )?to)\s+)*/i,'');
      var compound=compoundOptions(part.trim(),context);if(compound&&!compound.ok)return compound;
      var adding=/^(?:add|put) (\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty) (?:of )?(.+?) (?:to|in|into) (?:my |the )?(?:(?:test|mock|demo|sandbox) )?(?:bag|cart|basket)[.!?]?$/i.exec(command);
      var indexed=/^(?:zoom|enlarge|magnify)(?: me)?(?: the)? ((?:image|photo|picture)(?: number)? \d+|(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|last|next|previous) (?:image|photo|picture))(?: (?:for|of) (.+?))?[.!?]?$/i.exec(command);
      if(compound)steps.push(...compound.steps);
      else if(adding){var quantity=Object.hasOwn(QUANTITIES,adding[1].toLowerCase())?QUANTITIES[adding[1].toLowerCase()]:Number(adding[1]),piece=target(norm(adding[2]),context,true);if(!Number.isInteger(quantity)||quantity<1||quantity>20)return failed('Choose a whole quantity from 1 to 20.',true);if(!piece.handle)return failed(piece.reason,true);steps.push({text:'Set quantity to '+quantity+' for '+piece.handle},{text:'Add '+adding[2]+' to my bag'});}
      else if(indexed){var imageText='Show '+indexed[1]+(indexed[2]?' for '+indexed[2]:''),image=resolveIntent(imageText,context);if(!image.ok&&parts.length===1)return image;steps.push({text:imageText},{text:'Enlarge '+(indexed[2]||'this piece')});}
      else {var question=parts.length>1?planQuestion(part.trim(),context,steps.length>0):null;if(question&&!question.ok)return failed(question.reason,true);steps.push({text:part.trim(),...(question?{kind:'question'}:{})});}
    }
    if(steps.every(function(step){return step.kind==='question';}))return failed('',false);
    if(steps.length<2)return failed('',false);
    if(steps.length>4)return failed('Use up to four clear website actions in one request.',true);
    for(var step of steps){var words=norm(step.text);if(/^\s*["“‘'][\s\S]*["”’']\s*$/.test(step.text)||/javascript\s*:|<\s*script\b|\b(?:document|window)\s*[.\[]|\b(?:eval|fetch)\s*\(|\b(?:execute|run)\s+(?:code|script|javascript)|https?:\/\//i.test(step.text)||/\b(?:ignore|override|bypass|do not|dont|never|unless|hypothetically|yesterday|tomorrow|previously)\b|^(?:no|stop|cancel|wait|hold|if|when|once|after|suppose|imagine)\b|\b(?:if|when|once|after) (?:i|you)\b/.test(words))return failed('Only direct current website requests can run together.',true);if(step.kind!=='question'&&!/^(?:(?:please|can you|could you|would you|will you|i want (?:you )?to|i would like (?:you )?to|id like (?:you )?to)\s+)*(?:open|view|show|display|take|go|navigate|visit|scroll|highlight|zoom|enlarge|magnify|select|choose|pick|set|change|fill|write|save|use|add|put|review|prepare|remove|delete|enable|disable|close|hide|collapse|dismiss|undo|reverse|revert|proceed|continue|sort|filter)\b/.test(words))return failed('Give clear website commands or checked product questions for each step.',true);if(/\b(?:place|submit|pay for|purchase|complete)\b.{0,50}\b(?:order|payment|purchase)\b|\breal checkout\b/.test(words))return failed('This test storefront cannot place or pay for a real order.',true);}
    var first=steps[0].kind==='question'?planQuestion(steps[0].text,context,false):resolveIntent(steps[0].text,context);if(!first.ok)return Object.assign(first,{handled:true,recognized:true});
    return {ok:true,handled:true,recognized:true,plan:Object.freeze({text:raw,contextRevision:context.contextRevision,steps:Object.freeze(steps.map(function(step){return Object.freeze(step);} ))})};
  }
  function publicAction(action){var safe={...action};delete safe.text;delete safe.giftNote;return safe;}
  function ownProductURL(value,h){try{var raw=typeof value==='string'&&value.match(/^https:\/\/(?:www\.)?britesjewelry\.com(\/[^?#]*)$/i),route=raw&&raw[1].match(/^\/(?:[a-zA-Z]{2}(?:-[a-zA-Z]{2})?\/)?products\/([^/]+)\/?$/),u=new URL(value);return !!raw&&!!route&&route[1]===h&&u.pathname===raw[1]&&u.protocol==='https:'&&!u.username&&!u.password&&!u.port&&['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)&&!u.search&&!u.hash;}catch{return false;}}
  function liveProducts(values){
    if(!Array.isArray(values)||values.length>60)return [];
    return values.map(function(p){
      if(!p||!PRODUCT.test(p.id||'')||!handle(p.handle)||!plain(p.title,300)||!ownProductURL(p.url,p.handle)||!/^[A-Z]{3}$/.test(p.currency||'')||!Array.isArray(p.variants)||p.variants.length>250)return null;
      var variants=p.variants.map(function(v){if(!v||!VARIANT.test(v.id||'')||!plain(v.title,300)||!Number.isFinite(v.price)||v.price<0||typeof v.available!=='boolean')return null;var out={id:v.id,title:v.title,price:v.price,available:v.available,...(v.availabilityKnown===false?{availabilityKnown:false}:{}),numericId:v.id.split('/').pop()};if(Array.isArray(v.options)&&v.options.length<=12&&v.options.every(function(o){return plain(o&&o.name,120)&&plain(o&&o.value,300);}))out.options=v.options.map(function(o){return {name:o.name,value:o.value};});return out;});
      if(variants.some(function(v){return !v;})||new Set(variants.map(function(v){return v.id;})).size!==variants.length)return null;
      var out={id:p.id,handle:p.handle,title:p.title,url:p.url,currency:p.currency,variants:variants,variantsComplete:p.variantsComplete===true};
      ['description','image','imageAlt','type'].forEach(function(k){var v=plain(p[k],k==='description'?6000:2000);if(v)out[k]=v;});
      if(Number.isFinite(p.minPrice)&&p.minPrice>=0)out.minPrice=p.minPrice;
      ['cartHold','recommendationHold','partsOnly'].forEach(function(k){if(p[k]===true)out[k]=true;});return out;
    }).filter(Boolean);
  }
  function create(config){
    config=config||{};var provider=config.storefront,clock=typeof config.now==='function'?config.now:Date.now,issued=new WeakMap(),issuedPlans=new WeakMap(),consumed=new Map(),recent=new Map(),version=0,inflight=null,activePlan=null,planSerial=0,destroyed=false;
    function host(){try{return typeof provider==='function'?provider():provider;}catch{return null;}}
    function read(){var sf=host();try{return snapshot(sf&&typeof sf.snapshot==='function'?sf.snapshot():null);}catch{return snapshot(null);}}
    function resolve(text,context){var result=resolveIntent(text,context||read());if(result.ok)issued.set(result.action,{text:typeof text==='string'?text:text.message,context:snapshot(context||text&&text.context||read()),used:false});return result;}
    function plan(text,context){var page=snapshot(context||text&&text.context||read()),result=resolvePlan(text,page);if(result.ok)issuedPlans.set(result.plan,{text:typeof text==='string'?text:text.message,context:page,used:false,record:null});return result;}
    function cancelAction(){version++;if(inflight){inflight.controller.abort();inflight=null;}}
    function cancel(){if(activePlan){activePlan.controller.abort();activePlan=null;}cancelAction();}
    function duplicate(record){var ok=record&&record.status==='success';return {ok:ok,handled:true,recognized:true,suppressed:true,pending:record&&record.status==='pending',reply:ok?'That request has already been handled.':record&&record.status==='pending'?'That request is already being handled.':'That request was not completed. Please make a fresh request.',reason:ok?'':'The previous request is pending or was not completed.',snapshot:read()};}
    async function executeAction(request,options,stepAuthority){
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
      if(!resolved.ok||!sameAction(action,resolved.action))return Object.assign(failed(resolved.reason||'That control does not match the current shopper request.',true),resolved.needsClarification===true?{needsClarification:true}:{});
      action=resolved.action;
      var fresh=read(),targeted=!!action.handle;
      if((targeted||CONTROL_TYPES.includes(action.type))&&(context.contextRevision===null||context.contextRevision!==fresh.contextRevision||targeted&&JSON.stringify([context.pageKind,context.currentHandle])!==JSON.stringify([fresh.pageKind,fresh.currentHandle])||CONTROL_TYPES.includes(action.type)&&JSON.stringify([context.productControls,context.bagControls,context.checkoutControls,context.navigationControls,context.galleryControls,context.changeControls,context.imageControls,context.engravingControls,context.giftControls])!==JSON.stringify([fresh.productControls,fresh.bagControls,fresh.checkoutControls,fresh.navigationControls,fresh.galleryControls,fresh.changeControls,fresh.imageControls,fresh.engravingControls,fresh.giftControls])))return Object.assign(failed('The visible selection changed. Ask again for the piece you are viewing now.',true),{stale:true});
      var authorityKey=source==='native'?'native:'+options.inputItemId+':'+options.turnVersion+(stepAuthority?':'+stepAuthority:''):plain(options.requestId,200)?'typed:'+options.requestId:'';
      var repeatKey=source+'|'+norm(text)+'|'+JSON.stringify(action)+'|'+String(context.contextRevision),now=clock();
      if(authorityKey&&consumed.has(authorityKey))return duplicate(consumed.get(authorityKey));
      if(!authorityKey&&recent.has(repeatKey)&&now-recent.get(repeatKey).at<900)return duplicate(recent.get(repeatKey));
      var sf=host();if(!sf||typeof sf.execute!=='function')return failed('The connected storefront is not ready yet.',true);
      // Checked selection is an internal host capability, never a model action
      // field. It replaces the search dispatch under the original shopper turn.
      var selection=options.checkedSelection;
      if(selection!==undefined){var selected=liveProducts(selection&&selection.products),checkedAt=selection&&selection.checkedAt,age=now-checkedAt;if(context.contextRevision!==fresh.contextRevision||!['search','filter'].includes(action.type)||typeof sf.presentProducts!=='function'||!Array.isArray(selection?.products)||selection.products.length>24||selected.length!==selection.products.length||new Set(selected.map(function(p){return p.id;})).size!==selected.length||new Set(selected.map(function(p){return p.handle;})).size!==selected.length||!Number.isSafeInteger(checkedAt)||checkedAt<=0||age < -60000||age>300000||selected.some(function(p){return p.cartHold||p.recommendationHold||!p.variantsComplete||!p.variants.some(function(v){return v.available&&v.availabilityKnown!==false;});}))return failed('The checked selection is no longer ready. Ask again for current listings.',true);}
      var manifest=capabilities(typeof sf.capabilities==='function'?sf.capabilities():sf.capabilities);if(CONTROL_TYPES.includes(action.type)&&(!manifest||fresh.controlVersion!==1||!manifest.actions.includes(action.type)||action.type==='checkout-complete'&&(manifest.mode!=='sandbox'||manifest.checkoutMode!=='simulation-only')||action.type==='gift-preferences'&&!['local-session','native-form'].includes(manifest.notesMode)))return failed('That control is not supported by this connected storefront. Use its visible controls.',true);
      var operation={at:now,status:'pending'};if(authorityKey)consumed.set(authorityKey,operation);recent.set(repeatKey,operation);if(cap){cap.used=true;cap.record=operation;}
      // Only live, in-memory authorities exist. No saved chat or model argument
      // can restore them after an interruption, reload or later conversation.
      if(consumed.size>300)consumed.delete(consumed.keys().next().value);if(recent.size>100)recent.delete(recent.keys().next().value);
      cancelAction();var mine=version,controller=new AbortController(),abort=function(){controller.abort();};inflight={controller:controller,version:mine};if(options.signal)options.signal.addEventListener('abort',abort,{once:true});
      var timeout=setTimeout(abort,12000),onAbort,aborted=new Promise(function(done){onAbort=function(){done({ok:false,cancelled:true});};controller.signal.addEventListener('abort',onAbort,{once:true});});
      try{
        var work=Promise.resolve().then(function(){if(controller.signal.aborted)return {ok:false,cancelled:true};var hostOptions={signal:controller.signal,requestId:plain(options.requestId,200)||authorityKey||'shop-'+mine,reviewAuthority:['review-add','add'].includes(action.type)?options.reviewAuthority:undefined};return selection!==undefined?sf.presentProducts(selection.products,{...hostOptions,query:plain(selection.searchQuery,180)||action.query,searchQuery:plain(selection.searchQuery,180)||action.query,filter:action.filter,sort:action.sort,checkedAt:selection.checkedAt}):sf.execute(action,hostOptions);});
        var result=await Promise.race([work,aborted]);
        if(destroyed||mine!==version||controller.signal.aborted||result&&result.cancelled)return Object.assign(failed('The website request was cancelled.',true),{cancelled:true});
        if(!result||result.ok!==true)return failed(plain(result&& (result.message||result.reason||result.error),500)||'The current shop view could not be checked. Please try again.',true);
        var out={ok:true,handled:true,recognized:true,action:publicAction(action),reply:['set-engraving','bag-set-engraving'].includes(action.type)?'Your engraving text is in the visible field.':action.type==='gift-preferences'&&Object.hasOwn(action,'giftNote')?(manifest?.notesMode==='native-form'?'The requested gift note is filled in the visible shop field; saving is not yet confirmed.':'The requested gift note is saved in the visible local controls.'):plain(result.message||result.reply,800)||'The requested part of the test shop is ready.',snapshot:read()};
        if(action.type==='review-add')out.prepared=result.prepared===true||out.snapshot.productControls?.reviewReady===true;
        if(result.navigationRequested===true)out.navigationRequested=true;
        if(['add','bag-quantity','bag-remove','bag-select-option','bag-set-engraving'].includes(action.type)&&result.cartChanged===true)out.cartChanged=true;if(action.type.startsWith('checkout')&&manifest&&manifest.mode==='sandbox')out.mockCheckout=true;
        if(result.live===true){var checkedAt=result.checkedAt;if(Number.isFinite(checkedAt)&&checkedAt>0||typeof checkedAt==='string'&&Number.isFinite(Date.parse(checkedAt))){out.live=true;out.checkedAt=checkedAt;var products=liveProducts(result.products);if(products.length||Array.isArray(result.products)&&result.products.length===0)out.products=products;}}
        operation.status='success';return out;
      }catch{return failed('The current shop view could not be checked. Please try again.',true);}finally{if(operation.status==='pending')operation.status='failure';clearTimeout(timeout);controller.signal.removeEventListener('abort',onAbort);if(options.signal)options.signal.removeEventListener('abort',abort);if(inflight&&inflight.version===mine)inflight=null;}
    }
    async function answerPlanQuestion(text,options,page,record){
      if(typeof config.answerQuestion!=='function')return failed('Checked product answers are not available on this connected page yet.',true);
      var question=planQuestion(text,page,false);if(!question?.ok||!question.handle||page.loading)return failed(question?.reason||'Wait for the exact product page to finish opening before asking its details.',true);
      var identities=page.visiblePieces.concat(page.loadedPieces||[],page.inventoryPieces||[]),identity=identities.find(function(p){return p.handle===question.handle&&PRODUCT.test(p.id||'');}),productId=identity?.id||(page.productControls?.handle===question.handle?page.productControls.productId:null);
      if(!PRODUCT.test(productId||''))return failed('The exact product identity could not be checked for that question.',true);
      var controller=record.controller,onAbort,aborted=new Promise(function(done){onAbort=function(){done({cancelled:true});};controller.signal.addEventListener('abort',onAbort,{once:true});});
      try{
        var work=Promise.resolve().then(function(){if(controller.signal.aborted)return {cancelled:true};return config.answerQuestion(text,{...options,native:options.source==='native',context:page,signal:controller.signal});}),answer=await Promise.race([work,aborted]);
        if(destroyed||controller.signal.aborted||activePlan!==record||answer?.cancelled)return Object.assign(failed('The remaining website questions were cancelled.',true),{cancelled:true});
        var fresh=read();if(fresh.contextRevision!==page.contextRevision||JSON.stringify(fresh)!==JSON.stringify(page))return Object.assign(failed('The visible selection changed before its answer was ready. Ask again.',true),{stale:true});
        var facts=answer?.productFacts,checkedAt=answer?.checkedAt,age=clock()-checkedAt,reply=plain(answer?.reply,3500);
        if(answer?.handled!==true||answer.ok!==true)return failed(plain(answer?.reason||answer?.error,500)||'The requested product facts could not be checked from the loaded inventory. Ask again when they are ready.',true);
        if(answer.verified!==true||answer.live!==true||!Number.isSafeInteger(checkedAt)||checkedAt<=0||age < -60000||age>300000||facts?.status!=='verified'||facts.productId!==productId||facts.handle!==question.handle||facts.checkedAt!==checkedAt||!reply||Array.isArray(answer.products)&&answer.products.length||Array.isArray(answer.actions)&&answer.actions.length||answer.cartChanged===true||answer.navigationRequested===true)return failed('The answer did not verify the exact current product facts. Ask again for a fresh check.',true);
        return {ok:true,handled:true,recognized:true,action:{type:'question',handle:question.handle},reply:reply,verified:true,live:true,cached:answer.cached===true,checkedAt:checkedAt,preserveSelection:true,snapshot:fresh};
      }catch{return failed('The requested product facts could not be checked from the loaded inventory. Ask again when they are ready.',true);}finally{controller.signal.removeEventListener('abort',onAbort);}
    }
    async function executePlan(request,options){
      options=options||{};var cap=request&&issuedPlans.get(request),text=options.transcript===undefined&&cap?cap.text:options.transcript,context=snapshot(options.context||cap&&cap.context),source=options.source||(options.inputItemId||Object.hasOwn(options,'currentTurn')||Object.hasOwn(options,'turnVersion')?'native':'typed');
      if(!cap||destroyed)return failed('A current shopper-issued action sequence is required.',true);if(cap.used)return duplicate(cap.record);
      if(!plain(text,2000)||text!==cap.text||!['typed','native'].includes(source)||options.signal?.aborted)return failed('That website sequence is no longer current.',true);
      if(source==='native'&&(options.currentTurn!==true||!plain(options.inputItemId,200)||!Number.isSafeInteger(options.turnVersion)||options.turnVersion<0))return failed('That voice sequence is no longer current.',true);
      var fresh=read(),again=resolvePlan(text,context);if(context.contextRevision===null||context.contextRevision!==fresh.contextRevision||JSON.stringify(context)!==JSON.stringify(fresh)||!again.ok||JSON.stringify(again.plan.steps)!==JSON.stringify(request.steps)||JSON.stringify(context)!==JSON.stringify(cap.context))return Object.assign(failed('The visible selection changed. Ask again for the page you are viewing now.',true),{stale:true});
      var authorityKey=source==='native'?'native:'+options.inputItemId+':'+options.turnVersion:plain(options.requestId,200)?'typed:'+options.requestId:'plan:'+ ++planSerial;
      if(consumed.has(authorityKey))return duplicate(consumed.get(authorityKey));cancel();var operation={at:clock(),status:'pending'},controller=new AbortController(),record={controller:controller},completed=[],receipts=[],expected=fresh,requestId=(plain(options.requestId,160)||'plan-'+ ++planSerial),abort=function(){controller.abort();};activePlan=record;cap.used=true;cap.record=operation;consumed.set(authorityKey,operation);if(options.signal)options.signal.addEventListener('abort',abort,{once:true});var timer=setTimeout(abort,30000);
      function result(ok,reason,cancelled){var lastControl=receipts.filter(function(r){return r.action?.type!=='question';}).at(-1),reply=receipts.filter(function(r){return r.action?.type==='question'||r===lastControl;}).map(function(r){return r.reply;}).filter(Boolean).join(' ');if(!ok)reply+=(reply?' ':'')+(reason||'Please try the next choice again.');var last=receipts[receipts.length-1];return {ok:ok,handled:true,recognized:true,action:'plan',reply:reply.slice(0,3500),reason:ok?'':reason||'',completedActions:completed,stepResults:receipts,partial:!ok&&completed.length>0,cancelled:cancelled===true,...(last?.navigationRequested===true?{navigationRequested:true}:{}),...(receipts.some(function(r){return r.cartChanged===true;})?{cartChanged:true}:{}),...(receipts.some(function(r){return r.mockCheckout===true;})?{mockCheckout:true}:{}),...(last?.live===true?{live:true,checkedAt:last.checkedAt,...(last.action?.type==='question'?{preserveSelection:true}:{products:last.products||[]})}:{}),...(last?.action?.type==='review-add'?{prepared:last.prepared===true,requiredCustomerClick:'Confirm add to bag'}:{}),snapshot:read()};}
      try{for(var index=0;index<request.steps.length;index++){
        if(controller.signal.aborted||activePlan!==record)return result(false,'Please ask for your next choice again.',true);
        var page=read();if(page.contextRevision!==expected.contextRevision||JSON.stringify(page)!==JSON.stringify(expected))return result(false,'The visible selection changed before the next action. Ask again.');
        if(request.steps[index].kind==='question'){
          var answered=await answerPlanQuestion(request.steps[index].text,{...options,source:source,requestId:requestId+'-step-'+(index+1)},page,record);
          if(!answered.ok)return result(false,answered.reason||answered.reply,answered.cancelled);
          completed.push(answered.action);var questionReceipt={...answered};delete questionReceipt.snapshot;receipts.push(questionReceipt);expected=answered.snapshot;continue;
        }
        var step=resolve(request.steps[index].text,page);if(!step.ok)return result(false,step.reason||'Which option would you like to choose for this piece?');
        if(step.action.type==='checkout-complete')return result(false,'Use the separate visible confirmation to complete the test checkout.');
        if(step.action.type==='review-add'&&index!==request.steps.length-1)return result(false,'Bag review must be the final action so you can confirm its exact options.');
        var stepId=requestId+'-step-'+(index+1),review=['review-add','add'].includes(step.action.type)&&typeof options.reviewAuthority==='function'?options.reviewAuthority(step.action,stepId,page):undefined;
        var changed=await executeAction(step.action,{...options,context:page,transcript:request.steps[index].text,signal:controller.signal,requestId:stepId,reviewAuthority:review},authorityKey+':step-'+index);
        if(!changed.ok)return result(false,changed.reason||changed.reply,changed.cancelled);
        completed.push(publicAction(step.action));var receipt={...changed};delete receipt.snapshot;receipts.push(receipt);expected=changed.snapshot;
        if(changed.navigationRequested===true&&index<request.steps.length-1)return result(false,'The new page is opening. Once it is ready, tell me what you would like to choose.');
      }operation.status='success';return result(true);
      }catch{return result(false,'I could not finish that request. Please ask for your next choice again.');}finally{if(operation.status==='pending')operation.status='failure';clearTimeout(timer);if(options.signal)options.signal.removeEventListener('abort',abort);if(activePlan===record)activePlan=null;}
    }
    return {snapshot:read,resolve:resolve,resolvePlan:plan,execute:function(request,options){if(activePlan)cancel();return executeAction(request,options);},executePlan:executePlan,cancel:cancel,destroy:function(){destroyed=true;cancel();}};
  }
  return Object.freeze({create:create,resolve:resolveIntent,resolvePlan:resolvePlan,resolveKnowledgeTarget:resolveKnowledgeTarget,redactRequestText:redactRequestText,sanitizeSnapshot:snapshot,sanitizeCapabilities:capabilities,validateAction:validateAction,projectLiveProducts:liveProducts});
});
