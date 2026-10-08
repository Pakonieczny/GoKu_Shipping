(function(root,factory){
  'use strict';
  var api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else {root.BritesShopifyStorefrontAdapter=api;api.install(root);}
})(typeof window!=='undefined'?window:globalThis,function(){
  'use strict';
  var HANDLE=/^[a-z0-9]+(?:-[a-z0-9]+)*$/;
  var NUMERIC=/^[1-9][0-9]{0,19}$/;
  var VARIANT=/^gid:\/\/shopify\/ProductVariant\/[1-9][0-9]{0,19}$/;
  var LINE=/^[1-9][0-9]{0,19}:[a-zA-Z0-9_-]{1,170}$/;
  var ROOT=/^\/(?:[a-z]{2}(?:-[a-z]{2})?\/)?$/i;
  var SORTS={'featured':'best-selling','price-asc':'price-ascending','price-desc':'price-descending','title-asc':'title-ascending','title-desc':'title-descending'};
  var COLLECTIONS={all:'all',necklaces:'necklaces','regular-necklaces':'necklaces','beady-necklaces':'beady-chain-necklaces',earrings:'earrings','stud-earrings':'charm-studs','hoop-earrings':'huggie-hoops',bracelets:'bracelets',rings:'rings',charms:'charms-only','charm-only':'charms-only'};
  var ACTIONS={search:['type','query','sort','filter'],sort:['type','sort'],filter:['type','filter'],open:['type','handle'],highlight:['type','handle','section'],back:['type'],forward:['type'],undo:['type'],'close-options':['type'],'close-image':['type'],gallery:['type','handle','index'],scroll:['type','handle','section','direction'],bag:['type'],checkout:['type'],gift:['type','section'],customize:['type','handle','section'],options:['type','handle','optionName'],'select-option':['type','handle','variantId','optionName','optionValue'],'product-quantity':['type','handle','quantity'],'set-engraving':['type','handle','text'],add:['type','handle','variantId'],'review-add':['type','handle','variantId'],'bag-quantity':['type','lineId','quantity'],'bag-remove':['type','lineId'],'gift-preferences':['type','wrapping','giftPackage','giftNote']};
  var SECTIONS=['title','description','materials','length','engraving','price','details','options','image','shipping','gifts','catalogue','bag','checkout','offers','customize'];
  function text(value,max){return typeof value==='string'&&value.length<=max&&!/[\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069<>]/.test(value)?value.trim():'';}
  function h(value){return text(value,180)&&HANDLE.test(value)?value:'';}
  function id(value){return typeof value==='number'?Number.isSafeInteger(value)&&value>0?String(value):'':typeof value==='string'&&NUMERIC.test(value)?value:'';}
  function rootPath(value){return typeof value==='string'&&ROOT.test(value)?value:null;}
  function quantity(value){return Number.isSafeInteger(value)&&value>=1&&value<=20?value:null;}
  function literalNote(value,max){return typeof value==='string'&&value.length<=max&&!/[\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/.test(value);}
  function ownURL(value,origin){try{var u=new URL(value,origin);return u.origin===origin&&u.protocol==='https:'&&!u.username&&!u.password&&!u.hash?u:null;}catch{return null;}}
  function productHandle(url,origin,base){var u=ownURL(url,origin);if(!u)return '';var prefix=base==='/'?'':base.slice(0,-1),m=u.pathname.match(new RegExp('^'+prefix+'/(?:collections/[a-z0-9-]+/)?products/([a-z0-9-]+)/?$'));return m&&h(m[1])||'';}
  function failure(reason,extra){return Object.assign({ok:false,reason:reason},extra||{});}
  function validateAction(value){
    if(!value||typeof value!=='object'||Array.isArray(value))return null;
    var type=Object.getOwnPropertyDescriptor(value,'type');if(!type||!Object.hasOwn(type,'value')||typeof type.value!=='string'||!Object.hasOwn(ACTIONS,type.value))return null;
    var keys=Reflect.ownKeys(value),allowed=ACTIONS[type.value],safe=Object.create(null),out={type:type.value};
    if(keys.some(function(k){var d=Object.getOwnPropertyDescriptor(value,k);if(typeof k!=='string'||!allowed.includes(k)||!d||!Object.hasOwn(d,'value'))return true;safe[k]=d.value;return false;}))return null;
    value=safe;
    if(value.handle!==undefined){if(!h(value.handle))return null;out.handle=value.handle;}
    if(['open','gallery','select-option','product-quantity','set-engraving','add','review-add'].includes(value.type)&&!out.handle)return null;
    if(value.type==='search'){if(!text(value.query,180)||/https?:\/\/|javascript\s*:|(?:^|\W)(?:document|window)\s*[.\[]/i.test(value.query))return null;out.query=value.query.trim();}
    if(value.sort!==undefined){if(typeof value.sort!=='string'||!Object.hasOwn(SORTS,value.sort))return null;out.sort=value.sort;}
    if(value.type==='sort'&&!out.sort)return null;
    if(value.filter!==undefined){if(typeof value.filter!=='string'||!Object.hasOwn(COLLECTIONS,value.filter))return null;out.filter=value.filter;}
    if(value.type==='filter'&&!out.filter)return null;
    if(value.section!==undefined){if(!SECTIONS.includes(value.section))return null;out.section=value.section;}
    if(['highlight','gift','customize'].includes(value.type)&&!out.section)return null;
    if(value.type==='scroll'){if(value.direction!==undefined){if(!['up','down','top','bottom'].includes(value.direction)||out.section||out.handle)return null;out.direction=value.direction;}else if(!out.section)return null;}
    if(value.type==='gallery'){if(!Number.isInteger(value.index)||value.index<1||value.index>50)return null;out.index=value.index;}
    if(['highlight','scroll'].includes(value.type)&&['title','description','materials','length','engraving','price','details','options','image'].includes(out.section)&&!out.handle)return null;
    if(value.type==='gift'&&out.section!=='gifts'||value.type==='customize'&&!['customize','options'].includes(out.section))return null;
    if(value.optionName!==undefined){if(!text(value.optionName,120))return null;out.optionName=value.optionName;}
    if(value.optionValue!==undefined){if(!text(value.optionValue,300))return null;out.optionValue=value.optionValue;}
    if(value.variantId!==undefined){if(typeof value.variantId!=='string'||!VARIANT.test(value.variantId))return null;out.variantId=value.variantId;}
    if(value.type==='select-option'&&(out.variantId?out.optionName!==undefined||out.optionValue!==undefined:!out.optionName||!out.optionValue))return null;
    if(['product-quantity','bag-quantity'].includes(value.type)){if(quantity(value.quantity)===null)return null;out.quantity=value.quantity;}
    if(['bag-quantity','bag-remove'].includes(value.type)){if(typeof value.lineId!=='string'||!LINE.test(value.lineId))return null;out.lineId=value.lineId;}
    if(value.type==='set-engraving'){if(!literalNote(value.text,300))return null;out.text=value.text;}
    if(value.type==='gift-preferences'){if(!['wrapping','giftPackage','giftNote'].some(function(key){return Object.hasOwn(value,key);}))return null;for(var key of ['wrapping','giftPackage'])if(Object.hasOwn(value,key)){if(typeof value[key]!=='boolean')return null;out[key]=value[key];}if(Object.hasOwn(value,'giftNote')){if(!literalNote(value.giftNote,300))return null;out.giftNote=value.giftNote;}}
    return Object.freeze(out);
  }
  function projectProduct(raw,currency,expectedCount){
    if(!raw||!id(raw.id)||!h(raw.handle)||!text(raw.title,300)||!Array.isArray(raw.options)||raw.options.length>3||!Array.isArray(raw.variants)||!raw.variants.length||raw.variants.length>250||!/^[A-Z]{3}$/.test(currency||''))return null;
    var groups=raw.options.map(function(o,index){var name=typeof o==='string'?o:o&&o.name;return text(name,120)?{name:name,position:index+1,values:[]}:null;});
    if(groups.some(function(o){return !o;})||new Set(groups.map(function(o){return o.name;})).size!==groups.length)return null;
    var variants=raw.variants.map(function(v){
      var values=Array.isArray(v&&v.options)?v.options:groups.map(function(_,i){return v&&v['option'+(i+1)];});
      if(!v||!id(v.id)||!text(v.title,300)||!Number.isSafeInteger(v.price)||v.price<0||typeof v.available!=='boolean'||values.length!==groups.length||values.some(function(s){return !text(s,300);}))return null;
      values.forEach(function(s,i){if(!groups[i].values.includes(s))groups[i].values.push(s);});
      return {id:'gid://shopify/ProductVariant/'+id(v.id),numericId:id(v.id),title:v.title,price:v.price/100,available:v.available,options:values.map(function(s,i){return {name:groups[i].name,value:s};}),requiresSellingPlan:v.requires_selling_plan===true};
    });
    if(variants.some(function(v){return !v;})||new Set(variants.map(function(v){return v.id;})).size!==variants.length||new Set(variants.map(function(v){return JSON.stringify(v.options);})).size!==variants.length)return null;
    var count=Number.isSafeInteger(expectedCount)?expectedCount:Number.isSafeInteger(raw.variants_count)?raw.variants_count:null;
    var complete=count!==null?count===variants.length:variants.length<250;
    var out={id:'gid://shopify/Product/'+id(raw.id),handle:raw.handle,title:raw.title,type:text(raw.type,120),currency:currency,variants:variants,variantsComplete:complete,optionGroups:groups,minPrice:Math.min.apply(null,variants.map(function(v){return v.price;}))};
    if(typeof raw.description==='string'){var description=raw.description.slice(0,100000).replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1\s*>/gi,' ').replace(/<[^>]*>/g,' ').replace(/&nbsp;/gi,' ').replace(/&amp;/gi,'&').replace(/&quot;/gi,'"').replace(/&#39;/g,"'").replace(/\s+/g,' ').trim().slice(0,6000);if(text(description,6000))out.description=description;}
    var images=[],imageURLs=new Set(),sourceImages=(Array.isArray(raw.images)?raw.images:[]).slice(0,40);if(raw.featured_image)sourceImages.unshift(raw.featured_image);sourceImages.forEach(function(value){var source=typeof value==='string'?value:value&&(value.url||value.src),u;try{u=source&&new URL(source);}catch{}if(!u||u.protocol!=='https:'||u.username||u.password||u.port||!['britesjewelry.com','www.britesjewelry.com','cdn.shopify.com'].includes(u.hostname)||u.href.length>1800||imageURLs.has(u.href)||images.length>=16)return;imageURLs.add(u.href);images.push({url:u.href,altText:text(value&&typeof value==='object'&&(value.altText||value.alt),300)||null});});if(images.length){out.images=images;out.image=images[0].url;out.imageAlt=images[0].altText||'';}
    ['cartHold','recommendationHold','partsOnly','meaningHold'].forEach(function(key){if(raw[key]===true)out[key]=true;});
    if(raw.requires_selling_plan===true)out.requiresSellingPlan=true;
    return out;
  }
  function create(config){
    config=config||{};var win=config.window||(typeof window!=='undefined'?window:null),doc=config.document||win&&win.document,fetcher=config.fetch||win&&win.fetch&&win.fetch.bind(win),location=config.location||win&&win.location;
    var base=rootPath(config.root),origin;try{origin=new URL(location.href).origin;}catch{return null;}
    if(!base||!doc||!fetcher||!/^https:\/\/(?:www\.)?britesjewelry\.com$/.test(origin)||config.theme!=='brites-v1')return null;
    var initialCount=config.variantCount,seed=config.product||null,currency=/^[A-Z]{3}$/.test(config.currency||'')?config.currency:null,current=currency?projectProduct(seed,currency,initialCount):null;
    if(current)current.url=origin+base+'products/'+current.handle;
    var checkedProducts=new Map(),currentCheckedAt=current?Date.now():0;if(current)checkedProducts.set(current.handle,{product:current,checkedAt:currentCheckedAt});
    var stopped=false,epoch=0,revision=0,fingerprint='',discoveryRevision=0,lastDiscovery='',focused='',openedOption=null,activeSection='',cartCount=0,cartModel=null,loading=false,navigationPending=false,pending=new Map(),listeners=[],highlights=new Map(),undoRecord=null,privateFormRevision=0,contextObserver=null,contextScheduled=false;
    var navigate=typeof config.navigate==='function'?config.navigate:function(url){location.assign(url);};
    var confirmedCollections=new Set((Array.isArray(config.collectionHandles)?config.collectionHandles:[]).filter(function(value){return h(value)&&Object.values(COLLECTIONS).includes(value);}));
    function all(selector,scope){try{return Array.from((scope||doc).querySelectorAll(selector));}catch{return [];}}
    function one(selector,scope){var nodes=all(selector,scope);return nodes.length===1?nodes[0]:null;}
    function visible(node){if(!node||!node.isConnected||node.hidden||node.closest('[hidden],[aria-hidden="true"]'))return false;try{var css=win.getComputedStyle(node);return css.display!=='none'&&css.visibility!=='hidden';}catch{return true;}}
    function route(){var u=ownURL(location.href,origin),relative=u&&u.pathname.startsWith(base)?u.pathname.slice(base.length):'';return {url:u,page:productHandle(location.href,origin,base)?'product':/^collections\//.test(relative)?'catalogue':/^search\/?$/.test(relative)?'catalogue':/^cart\/?$/.test(relative)?'bag':'home',handle:productHandle(location.href,origin,base)};}
    function form(){var r=route(),f=one('#bjForm');return r.page==='product'&&seed&&r.handle===seed.handle&&f&&visible(f)?f:null;}
    function model(){return !navigationPending&&current&&form()&&current.handle===route().handle?current:null;}
    var historyKey='brites:storefront-history:v1:'+base,historyStateKey='__britesStorefrontJourneyV1',journey={entries:[],keys:[],index:-1,pending:null};
    function historyURL(value){var u=text(value,700)&&ownURL(value,origin);if(!u||!u.pathname.startsWith(base))return null;var path=u.pathname.slice(base.length);if(path!==''&&!/^products\/[a-z0-9-]+\/?$/.test(path)&&!/^collections\/[a-z0-9-]+\/?$/.test(path)&&!/^search\/?$/.test(path)&&!/^cart\/?$/.test(path)&&path!=='pages/custom-studio')return null;if(u.search&&path!=='search'&&path!=='search/')return null;if(/^search\/?$/.test(path)){if(Array.from(u.searchParams.keys()).some(function(k){return !['q','type','sort_by'].includes(k);})||u.searchParams.get('q')?.length>180||u.searchParams.has('type')&&u.searchParams.get('type')!=='product'||u.searchParams.has('sort_by')&&!Object.values(SORTS).includes(u.searchParams.get('sort_by')))return null;}return u.href;}
    function saveJourney(){try{journey.length=win.history.length;win.sessionStorage.setItem(historyKey,JSON.stringify(journey));}catch{}}
    function entryKey(value){return typeof value==='string'&&/^shop-view-[a-z0-9-]{1,90}$/.test(value)?value:null;}
    function newEntryKey(){var value;do{value='shop-view-'+Date.now().toString(36)+'-'+Math.random().toString(36).slice(2);}while(journey.keys.includes(value));return value;}
    function loadJourney(){
      try{var encoded=win.sessionStorage.getItem(historyKey),saved=encoded&&encoded.length<=44000&&JSON.parse(encoded);if(!saved||!Array.isArray(saved.entries)||!saved.entries.length||saved.entries.length>50||!Array.isArray(saved.keys)||saved.keys.length!==saved.entries.length||new Set(saved.keys).size!==saved.keys.length||!saved.keys.every(entryKey)||!Number.isInteger(saved.index)||saved.index<0||saved.index>=saved.entries.length||!saved.entries.every(function(url){return historyURL(url)===url;}))return null;
        if(!Number.isSafeInteger(saved.length)||saved.length<1||saved.length>100000)return null;var next={entries:saved.entries.slice(),keys:saved.keys.slice(),index:saved.index,pending:null,length:saved.length},held=saved.pending;
        if(held){if(!held||!entryKey(held.from)||!next.keys.includes(held.from)||historyURL(held.url)!==held.url||!Number.isSafeInteger(held.length)||held.length<1||held.length>100000)return null;next.pending={from:held.from,url:held.url,length:held.length};}return next;
      }catch{return null;}
    }
    function historyMarker(){try{var state=win.history.state;if(!state||Object.prototype.toString.call(state)!=='[object Object]')return null;var held=state[historyStateKey];return held&&entryKey(held.key)&&historyURL(held.url)===held.url?{key:held.key,url:held.url}:null;}catch{return null;}}
    function hasHistoryMarker(){try{return win.history.state!==null&&Object.hasOwn(win.history.state,historyStateKey);}catch{return true;}}
    function currentHistoryIndex(){var marker=historyMarker(),url=historyURL(location.href),index=marker&&journey.keys.indexOf(marker.key);return marker&&url&&marker.url===url&&index>=0&&journey.entries[index]===url?index:-1;}
    function markHistoryEntry(index){
      try{var state=win.history.state;if(state!==null&&Object.prototype.toString.call(state)!=='[object Object]')return false;var next=Object.assign({},state||{});next[historyStateKey]={key:journey.keys[index],url:journey.entries[index]};win.history.replaceState(next,'',location.href);return currentHistoryIndex()===index;}catch{return false;}
    }
    function appendHistoryEntry(url){journey.entries=journey.entries.slice(0,journey.index+1);journey.keys=journey.keys.slice(0,journey.index+1);journey.entries.push(url);journey.keys.push(newEntryKey());if(journey.entries.length>50){journey.entries.shift();journey.keys.shift();}journey.index=journey.entries.length-1;journey.pending=null;}
    function restoreHistoryEntry(){
      journey=loadJourney()||{entries:[],keys:[],index:-1,pending:null};var url=historyURL(location.href),index=currentHistoryIndex();
      if(index>=0){journey.index=index;journey.pending=null;saveJourney();return;}
      if(!url){journey={entries:[],keys:[],index:-1,pending:null};saveJourney();return;}
      // An unmarked page may extend a checked native entry only when its real
      // incoming navigation matches that entry and the browser's entry count.
      var referrer=historyURL(doc.referrer),held=journey.pending,from=held?journey.keys.indexOf(held.from):journey.index,expected=from>=0?journey.length-(journey.entries.length-1-from)+1:null;
      var incoming=held&&held.url===url&&held.length===win.history.length&&(!doc.referrer||referrer===journey.entries[from])||!held&&from>=0&&referrer===journey.entries[from]&&expected===win.history.length;
      if(hasHistoryMarker()||!incoming)journey={entries:[],keys:[],index:-1,pending:null};else journey.index=from;
      appendHistoryEntry(url);if(!markHistoryEntry(journey.index)){journey={entries:[],keys:[],index:-1,pending:null};saveJourney();return;}saveJourney();
    }
    function recordNavigation(url){var index=currentHistoryIndex();url=historyURL(url);if(!url||index<0)return;journey.index=index;journey.pending={from:journey.keys[index],url:url,length:win.history.length-(journey.entries.length-1-index)+1};saveJourney();}
    function navigationControls(){var index=currentHistoryIndex();return {canGoBack:!navigationPending&&index>0,canGoForward:!navigationPending&&index>=0&&index<journey.entries.length-1};}
    restoreHistoryEntry();
    function galleryBinding(){
      var p=model(),media=p&&one('#bjMedia');if(!p||!media||!visible(media))return null;
      var panes=Array.from(media.children).filter(function(node){return node.tagName==='FIGURE'&&node.classList.contains('m');});if(!panes.length||panes.length>50)return null;
      if(panes.some(function(pane){var images=all('img',pane),image=images.length===1&&images[0],u;try{u=image&&new URL(image.getAttribute('src'),origin);}catch{}return !visible(pane)||!image||!u||u.protocol!=='https:'||u.username||u.password||!(u.origin===origin&&u.pathname.startsWith('/cdn/shop/')||u.hostname==='cdn.shopify.com'&&u.pathname.startsWith('/s/files/'));}))return null;
      var dots=one('#mDots',media),indicators=dots&&Array.from(dots.children).filter(function(node){return node.tagName==='I';});if(!indicators||indicators.length!==panes.length)return null;
      var active=indicators.flatMap(function(node,index){return node.classList.contains('on')?[index]:[];}),selected=active.length===1?active[0]:null;
      // Use actual viewport geometry when available. The native dots update in
      // IntersectionObserver, which can trail an immediate imperative scroll.
      var width=Number(win.innerWidth),height=Number(win.innerHeight),visiblePanes=panes.flatMap(function(pane,index){var rect=pane.getBoundingClientRect?.();if(!rect||!(rect.width>0&&rect.height>0&&width>0&&height>0)||rect.right<=0||rect.left>=width||rect.bottom<=0||rect.top>=height)return [];return [{index:index,distance:Math.abs((rect.top+rect.bottom)/2-height/2)+Math.abs((rect.left+rect.right)/2-width/2)}];});if(visiblePanes.length){visiblePanes.sort(function(a,b){return a.distance-b.distance;});selected=visiblePanes[0].index;}
      if(selected===null)return null;return {handle:p.handle,node:media,panes:panes,imageCount:panes.length,selectedIndex:selected+1};
    }
    function bindings(p){
      var f=form();if(!p||!f)return null;var result=[],used=new Set();
      for(var i=0;i<p.optionGroups.length;i++){
        var group=p.optionGroups[i],index=String(i+1),metal=one('#bjMetals',f),selects=all('select.bjOptSel',f).filter(function(n){return n.getAttribute('data-idx')===index;}),engr=one('#bjEngr',f),entry=null;
        if(metal&&metal.getAttribute('data-idx')===index){
          var buttons=all('button[data-vi]',metal),values=buttons.map(function(b){return text(b.textContent.trim(),300);});
          if(buttons.length===group.values.length&&buttons.every(function(b,j){return b.type==='button'&&b.getAttribute('data-vi')===String(j);})&&new Set(values).size===values.length&&values.every(function(v){return group.values.includes(v);}))entry={kind:'buttons',node:metal,buttons:buttons,values:values};
        }else if(selects.length===1){
          var s=selects[0],sv=Array.from(s.options).filter(function(o){return !(o.value===''&&o.disabled);}).map(function(o){return o.value;});
          if(sv.length===group.values.length&&new Set(sv).size===sv.length&&sv.every(function(v){return group.values.includes(v);}))entry={kind:'select',node:s,values:sv};
        }else if(engr&&engr.getAttribute('data-idx')===index&&group.name==='Engraving'&&group.values.length===2){
          var check=one('#bjEngrChk',engr),pair=[['None','Engraved'],['No','Yes']].find(function(values){return values.every(function(v){return group.values.includes(v);});});
          if(pair&&check&&check.type==='checkbox')entry={kind:'engraving',node:check,values:pair};
        }
        if(!entry||used.has(entry.node))return null;used.add(entry.node);entry.name=group.name;result.push(entry);
      }
      return result;
    }
    function chosen(entries){if(!entries)return [];return entries.map(function(b){var selected=b.kind==='select'?b.node.value:b.kind==='engraving'?b.values[b.node.checked?1:0]:b.buttons.filter(function(button){return button.classList.contains('on');}).map(function(button){return text(button.textContent.trim(),300);});return {name:b.name,value:Array.isArray(selected)?selected.length===1?selected[0]:'':selected};});}
    function selectedVariant(p,entries){var selections=chosen(entries);if(!p||selections.length!==p.optionGroups.length||selections.some(function(o){return !o.value;}))return null;var matches=p.variants.filter(function(v){return v.options.every(function(o,i){return o.name===selections[i].name&&o.value===selections[i].value;});});return matches.length===1?matches[0]:null;}
    function selectedQuantity(){var q=one('#bjQty',form());return q&&/^\d{1,2}$/.test(q.value||'')?quantity(Number(q.value)):null;}
    function quantityBinding(){var f=form(),input=f&&one('#bjQty',f);if(!input||input.disabled||selectedQuantity()===null)return null;if(!input.readOnly)return {input:input};var down=one('button[data-q="-1"]',f),up=one('button[data-q="1"]',f);return down&&up&&down.type==='button'&&up.type==='button'?{input:input,down:down,up:up}:null;}
    function engravingBinding(){
      var p=model(),entries=p&&bindings(p),entry=entries&&entries.find(function(binding){return binding.kind==='engraving';}),box=entry&&entry.node.closest('#bjEngr'),input=box&&one('#bjEngrTxt',box);
      if(!input||input.form!==form()||input.readOnly||input.maxLength===0||!['INPUT','TEXTAREA'].includes(input.tagName)||input.tagName==='INPUT'&&input.type!=='text')return null;
      return {input:input,check:entry.node,maximum:Math.min(300,input.maxLength>0?input.maxLength:300),enabled:entry.node.checked&&!entry.node.disabled&&!input.disabled&&visible(input)};
    }
    function nativeMenus(){var p=model(),entries=p&&bindings(p),menus=entries?entries.flatMap(function(entry){var button=entry.kind==='select'&&one('button.bjselx__btn',entry.node.parentElement);return button&&button.type==='button'&&button.getAttribute('aria-haspopup')==='listbox'&&['true','false'].includes(button.getAttribute('aria-expanded'))?[{button:button,entry:entry}]:[];}):[];return all('button.bjselx__btn[aria-expanded="true"]',form()||doc).every(function(button){return menus.some(function(menu){return menu.button===button;});})?menus:[];}
    function imageDialog(){var gallery=galleryBinding(),dialog=gallery&&one('dialog[open]',gallery.node),img=dialog&&one('img',dialog);if(!dialog||!visible(dialog)||typeof dialog.close!=='function'||!img)return null;var src=img.getAttribute('src');return gallery.panes.some(function(pane){return all('img',pane).some(function(image){return image!==img&&image.getAttribute('src')===src;});})?dialog:null;}
    function giftBindings(){
      var f=cartForm(),section=f&&(one('#is-a-gift',f)||one('#bjGiftWrap',f));if(!section||!visible(section))return null;
      var wrapping=one('#gift-wrapping[name="attributes[gift-wrapping]"]',section),note=one('#gift-note[name="attributes[gift-note]"]',section);
      if(wrapping&&(wrapping.type!=='checkbox'||wrapping.form!==f||wrapping.disabled||!visible(wrapping)))wrapping=null;
      if(note&&(!['INPUT','TEXTAREA'].includes(note.tagName)||note.tagName==='INPUT'&&note.type!=='text'||note.form!==f||note.disabled||note.readOnly||note.maxLength===0||!visible(note)))note=null;
      return wrapping||note?{wrapping:wrapping,note:note,maximum:note?Math.min(300,note.maxLength>0?note.maxLength:300):0}:null;
    }
    function applyText(input,value){input.value=value;input.dispatchEvent(new win.Event('input',{bubbles:true}));input.dispatchEvent(new win.Event('change',{bubbles:true}));if(input.value!==value)throw Error('The shop could not confirm that exact text. Check its visible field.');}
    function applyQuantity(value,ticket){
      var binding=quantityBinding(),q=selectedQuantity();if(!binding)throw Error('The product quantity control is not available.');
      if(binding.input.readOnly){var direction=value>q?1:-1,step=direction===1?binding.up:binding.down;for(var n=0;n<Math.abs(value-q);n++){if(stopped||ticket.epoch!==epoch||ticket.signal?.aborted)throw Object.assign(Error('The website request was cancelled.'),{cancelled:true});var prior=selectedQuantity();if(!visible(step)||step.disabled)throw Error('That quantity is not available on the product page.');step.click();if(selectedQuantity()!==prior+direction)throw Error('The shop could not confirm the quantity step.');}}
      else {binding.input.value=String(value);binding.input.dispatchEvent(new win.Event('input',{bubbles:true}));binding.input.dispatchEvent(new win.Event('change',{bubbles:true}));}
      if(selectedQuantity()!==value)throw Error('The quantity could not be confirmed.');
    }
    function applyGift(action){
      var fields=giftBindings();if(!fields||action.giftPackage!==undefined||action.wrapping!==undefined&&!fields.wrapping||action.giftNote!==undefined&&(!fields.note||!literalNote(action.giftNote,fields.maximum)))throw Error('That exact gift preference is not available in the shop\u2019s current cart form.');
      if(action.wrapping!==undefined&&fields.wrapping.checked!==action.wrapping){fields.wrapping.click();if(fields.wrapping.checked!==action.wrapping)throw Error('The shop could not confirm the gift wrapping choice.');}
      if(action.giftNote!==undefined&&fields.note.value!==action.giftNote)applyText(fields.note,action.giftNote);
    }
    function reviewHook(){return typeof config.reviewAdd==='function'?config.reviewAdd:win&&typeof win.BritesConciergeShopifyControls?.reviewAdd==='function'?win.BritesConciergeShopifyControls.reviewAdd:null;}
    function addHook(){return typeof config.add==='function'?config.add:win&&typeof win.BritesConciergeShopifyControls?.add==='function'?win.BritesConciergeShopifyControls.add:null;}
    function projectCart(raw){
      if(!raw||!/^[A-Z]{3}$/.test(raw.currency||'')||!Array.isArray(raw.items)||raw.items.length>250||!Number.isSafeInteger(raw.item_count)||raw.item_count<0)return null;
      var seen=new Set(),lines=raw.items.map(function(item){
        var key=item&&item.key,title=text(item?.product_title||item?.title,300),variant=text(item?.variant_title,300),price=item?.final_price===undefined?item?.price:item?.final_price;
        if(typeof key!=='string'||!LINE.test(key)||seen.has(key)||!id(item.product_id)||!id(item.variant_id)||!title||!Number.isSafeInteger(item.quantity)||item.quantity<1||item.quantity>999||!Number.isSafeInteger(price)||price<0)return null;
        seen.add(key);return {lineId:key,productId:'gid://shopify/Product/'+id(item.product_id),variantId:'gid://shopify/ProductVariant/'+id(item.variant_id),title:title,variant:variant,quantity:item.quantity,price:price/100,currency:raw.currency};
      });
      if(lines.some(function(line){return !line;})||lines.reduce(function(sum,line){return sum+line.quantity;},0)!==raw.item_count)return null;
      return {currency:raw.currency,itemCount:raw.item_count,lines:lines};
    }
    function cartForm(){var f=one('#bjCartForm'),u=f&&ownURL(f.getAttribute('action'),origin);return route().page==='bag'&&f&&visible(f)&&u&&u.pathname===base+'cart'&&(f.getAttribute('method')||'').toLowerCase()==='post'?f:null;}
    function cartBindings(){
      var f=cartForm();if(!f||!cartModel)return [];
      return all('#bjCartItems .bj-cp__row[data-key]',f).flatMap(function(row){
        var key=row.getAttribute('data-key'),line=cartModel.lines.find(function(item){return item.lineId===key;}),input=one('input.cart__product-qty[name="updates[]"]',row),wrap=one('[data-bjcp-qtywrap]',row),steps=wrap&&all('button',wrap),remove=one('a[data-bjcp-remove]',row),url=remove&&ownURL(remove.getAttribute('href'),origin),title=one('.cart__product-name',row);
        if(!line||line.quantity>20||!visible(row)||row.getAttribute('data-title')!==line.title||!title||text(title.textContent.trim(),300)!==line.title||!input||input.getAttribute('data-id')!==key||Number(input.value)!==line.quantity||input.disabled||!wrap||!steps||steps.length!==2||steps.some(function(button){return button.type!=='button'||button.disabled||!visible(button);})||text(steps[0].textContent.trim(),5)!=='–'||text(steps[1].textContent.trim(),5)!=='+'||!url||url.pathname!==base+'cart/change'||url.searchParams.get('id')!==key||url.searchParams.get('quantity')!=='0')return [];
        return [{row:row,input:input,minus:steps[0],plus:steps[1],remove:remove,line:line}];
      });
    }
    async function readCart(signal){var r=await fetcher(base+'cart.js',{method:'GET',headers:{Accept:'application/json'},cache:'no-store',credentials:'same-origin',signal:signal});var raw=r?.ok&&await r.json(),projected=projectCart(raw);if(!projected)throw Error('The exact current bag could not be checked.');return projected;}
    function sameCart(a,b){return !!a&&!!b&&JSON.stringify(a)===JSON.stringify(b);}
    function exactAddition(before,after,p,variant,q){
      if(!before||!after||before.currency!==after.currency||after.itemCount!==before.itemCount+q)return false;
      var added=0,valid=true;
      before.lines.forEach(function(line){var next=after.lines.find(function(row){return row.lineId===line.lineId;});if(!next||['productId','variantId','title','variant'].some(function(key){return next[key]!==line[key];})){valid=false;return;}var delta=next.quantity-line.quantity;if(delta<0||delta&&!(next.productId===p.id&&next.variantId===variant.id))valid=false;else added+=delta;});
      after.lines.filter(function(line){return !before.lines.some(function(row){return row.lineId===line.lineId;});}).forEach(function(line){if(line.productId!==p.id||line.variantId!==variant.id)valid=false;else added+=line.quantity;});
      return valid&&added===q;
    }
    function waitNativeCart(binding,expected,ticket){
      return new Promise(function(resolve,reject){
        var finished=false,timer;function done(error){if(finished)return;finished=true;win.clearTimeout(timer);doc.removeEventListener('cart:updated',changed);ticket.signal?.removeEventListener('abort',aborted);error?reject(error):resolve();}
        function aborted(){done(Object.assign(Error('The website request was cancelled.'),{cancelled:true}));}
        function changed(event){if(stopped||epoch!==ticket.epoch||location.href!==ticket.url||ticket.signal?.aborted)return aborted();if(expected===0?!binding.row.isConnected||event?.type==='cart:updated'&&binding.row.classList.contains('bjcp-removing'):Number(binding.input.value)===expected)done();}
        doc.addEventListener('cart:updated',changed);ticket.signal?.addEventListener('abort',aborted,{once:true});timer=win.setTimeout(function(){done(Error('Please check the bag before asking again; the shop has not confirmed that change.'));},8000);
        try{if(expected===0)binding.remove.click();else (expected>binding.line.quantity?binding.plus:binding.minus).click();changed();}catch(error){done(error);}
      });
    }
    async function editNativeCart(action,ticket){
      var target=cartBindings().find(function(binding){return binding.line.lineId===action.lineId;});if(!target)throw Error('Choose a currently visible exact bag line first.');
      var checked=await readCart(ticket.signal);assertCurrent(ticket);if(!sameCart(checked,cartModel))throw Object.assign(Error('Your bag changed. Review it before asking again.'),{stale:true});
      var expected=action.type==='bag-remove'?0:action.quantity,changed=false;
      while(target.line.quantity!==expected){
        if(stopped||epoch!==ticket.epoch||location.href!==ticket.url||ticket.signal?.aborted)throw Object.assign(Error('The website request was cancelled.'),{cancelled:true});
        var next=expected===0?0:target.line.quantity+(expected>target.line.quantity?1:-1);await waitNativeCart(target,next,ticket);changed=true;
        var after=await readCart(ticket.signal),line=after.lines.find(function(item){return item.lineId===action.lineId;});
        if(stopped||epoch!==ticket.epoch||location.href!==ticket.url||ticket.signal?.aborted)throw Object.assign(Error('The website request was cancelled.'),{cancelled:true});
        var expectedCart={...checked,lines:checked.lines.flatMap(function(item){return item.lineId===action.lineId?next?[{...item,quantity:next}]:[]:[item];})};expectedCart.itemCount=expectedCart.lines.reduce(function(n,item){return n+item.quantity;},0);
        // Quantity discounts may change price; every other identity and count
        // must still match. The exact returned prices become the new context.
        var identity=function(cart){return JSON.stringify([cart.currency,cart.itemCount,cart.lines.map(function(item){return [item.lineId,item.productId,item.variantId,item.title,item.variant,item.quantity];})]);};
        if(identity(after)!==identity(expectedCart)||next!==0&&(!line||line.quantity!==next)||next===0&&line)throw Error('The bag changed while the shop was updating it. Check the visible bag before continuing.');
        cartModel=after;cartCount=after.itemCount;currency=after.currency;checked=after;if(expected===0)break;target=cartBindings().find(function(binding){return binding.line.lineId===action.lineId;});if(!target)throw Error('The shop has not confirmed the visible quantity.');
      }
      activeSection='bag';announce();return {ok:true,cartChanged:changed,live:true,checkedAt:Date.now(),message:expected===0?'The exact bag line was removed.':changed?'The exact bag quantity is updated.':'That exact bag quantity is already selected.',snapshot:snapshot()};
    }
    function undoState(kind,lineId){
      if(kind==='bag-quantity'){var binding=cartBindings().find(function(row){return row.line.lineId===lineId;});return binding?{cart:cartModel,lineId:lineId,quantity:binding.line.quantity}:null;}
      if(kind==='gift-preferences'){var fields=giftBindings();return fields?{wrapping:fields.wrapping?fields.wrapping.checked:null,note:fields.note?fields.note.value:null}:null;}
      if(kind==='sort'){var select=one('#bjcSort');return select&&visible(select.parentElement)?{value:select.value,values:Array.from(select.options).map(function(option){return [option.value,option.disabled];})}:null;}
      var p=model(),entries=p&&bindings(p);if(!p||!entries)return null;
      if(kind==='gallery'){var gallery=galleryBinding();return gallery?{productId:p.id,index:gallery.selectedIndex,imageCount:gallery.imageCount}:null;}
      if(!['select-option','product-quantity','set-engraving'].includes(kind))return null;
      var engraving=engravingBinding();return {productId:p.id,options:chosen(entries),quantity:selectedQuantity(),engraving:engraving?engraving.input.value:null,variants:p.variants.map(function(variant){return [variant.id,variant.price,variant.available,variant.requiresSellingPlan];})};
    }
    function undoAvailable(){return !!undoRecord&&undoRecord.url===location.href&&JSON.stringify(undoState(undoRecord.kind,undoRecord.lineId))===undoRecord.after;}
    function captureUndo(action){var state=undoState(action.type,action.lineId);if(action.type==='select-option'&&state?.options.some(function(choice){return !choice.value;}))return null;return state?{kind:action.type,lineId:action.lineId,url:location.href,before:JSON.parse(JSON.stringify(state))}:null;}
    function rememberUndo(record){if(!record||record.url!==location.href)return;var state=undoState(record.kind,record.lineId),after=state&&JSON.stringify(state);if(after&&after!==JSON.stringify(record.before)){record.after=after;undoRecord=record;}}
    async function undoChange(ticket){
      if(!undoAvailable())throw Error('That earlier change no longer matches the visible shop controls.');var record=undoRecord,before=record.before;assertCurrent(ticket);
      if(record.kind==='bag-quantity'){var result=await editNativeCart({type:'bag-quantity',lineId:record.lineId,quantity:before.quantity},ticket);undoRecord=null;announce();return Object.assign({},result,{message:'The previous exact bag quantity is restored.',snapshot:snapshot()});}
      if(record.kind==='gift-preferences'){var action={type:'gift-preferences'};if(before.wrapping!==null)action.wrapping=before.wrapping;if(before.note!==null)action.giftNote=before.note;applyGift(action);}
      else if(record.kind==='sort'){var select=one('#bjcSort'),option=select&&Array.from(select.options).find(function(o){return o.value===before.value&&!o.disabled;});if(!option||select.disabled)throw Error('The previous native sort is no longer available.');select.value=before.value;select.dispatchEvent(new win.Event('change',{bubbles:true}));if(select.value!==before.value)throw Error('The shop could not confirm the previous sort.');}
      else if(record.kind==='gallery'){var gallery=galleryBinding();if(!gallery||before.index>gallery.imageCount)throw Error('The previous image is no longer available.');reveal(gallery.panes[before.index-1],ticket,false);if(galleryBinding()?.selectedIndex!==before.index)throw Error('The shop has not confirmed that the previous image is visible.');}
      else if(record.kind==='product-quantity')applyQuantity(before.quantity,ticket);
      else if(record.kind==='set-engraving'){var field=engravingBinding();if(!field?.enabled||!literalNote(before.engraving,field.maximum))throw Error('The previous engraving field is no longer available.');applyText(field.input,before.engraving);}
      else {var p=model(),entries=bindings(p),variant=p.variants.find(function(v){return v.options.every(function(option){return before.options.some(function(prior){return prior.name===option.name&&prior.value===option.value;});});});if(!variant?.available||variant.requiresSellingPlan||!entries)throw Error('The previous published option is no longer available.');entries.forEach(function(entry,i){if(chosen(entries)[i].value!==variant.options[i].value)applyChoice(entry,variant.options[i].value);});if(selectedVariant(p,bindings(p))?.id!==variant.id)throw Error('The shop could not confirm the previous options.');}
      undoRecord=null;announce();return {ok:true,message:'The previous website choice is restored. No order was placed.',snapshot:snapshot(),cartChanged:false};
    }
    function cards(){var seen=new Set();return all('main a.bjc-pcard[href]').flatMap(function(a){var handle=productHandle(a.getAttribute('href'),origin,base),title=text(one('.bjc-t',a)?.textContent.trim(),300);if(!visible(a)||!handle||!title||seen.has(handle))return [];seen.add(handle);return [{handle:handle,title:title}];}).slice(0,200);}
    function snapshot(){
      var r=route(),p=model(),entries=bindings(p),variant=selectedVariant(p,entries),pieces=cards(),groups=p&&entries?p.optionGroups.map(function(g){return {name:g.name,values:g.values.slice()};}):[];
      if(p)pieces.unshift({handle:p.handle,title:p.title,id:p.id});pieces=pieces.filter(function(v,i,arr){return arr.findIndex(function(o){return o.handle===v.handle;})===i;});
      var menus=nativeMenus(),openMenus=menus.filter(function(menu){return menu.button.getAttribute('aria-expanded')==='true';}),openedBinding=entries&&entries.find(function(entry){return entry.name===openedOption;}),q=selectedQuantity(),control=p&&entries?{handle:p.handle,productId:p.id,productTitle:p.title,productType:p.type,variantId:variant&&variant.id||null,quantity:q,optionsOpen:openMenus.length>0||activeSection==='options'&&(!openedBinding||openedBinding.kind!=='select'),openedOption:openMenus.length===1?openMenus[0].entry.name:openedOption,optionGroups:groups,selectedOptions:chosen(entries),...(variant?{selectedVariant:{id:variant.id,title:variant.title,price:variant.price,currency:p.currency,available:variant.available,options:variant.options.map(function(o){return {name:o.name,value:o.value};})}}:{}),...(variant&&q!==null?{itemTotalPrice:Math.round(variant.price*q*100)/100}:{}),reviewReady:false}:null;
      if(control&&addHook()){var missing=groups.filter(function(group){return !control.selectedOptions.some(function(choice){return choice.name===group.name&&group.values.includes(choice.value);});});control.selectionStatus=p.cartHold||p.partsOnly?'held':missing.length?'choosing':variant?.available?'ready':'unavailable';control.requiredOptions=missing.map(function(group){return group.name;});control.requiredOptionGroups=missing.map(function(group){return {name:group.name,values:group.values.slice()};});control.nextOptionName=missing[0]?.name||null;if(variant&&q!==null){control.selectedVariant.quantity=q;control.selectedVariant.subtotal=control.itemTotalPrice;}}
      var sortNode=one('#bjcSort'),sort=sortNode?Object.keys(SORTS).find(function(k){return SORTS[k]===sortNode.value;})||'featured':'featured';
      var collection=r.url&&r.url.pathname.slice(base.length).match(/^collections\/([a-z0-9-]+)\/?$/),filter=collection?Object.keys(COLLECTIONS).find(function(k){return COLLECTIONS[k]===collection[1];})||'all':'all',search=r.url&&text(r.url.searchParams.get('q'),180)||'';
      var discovery=JSON.stringify([r.url&&r.url.pathname,search,sort,filter,pieces]);if(lastDiscovery&&discovery!==lastDiscovery)discoveryRevision++;lastDiscovery=discovery;
      var gallery=galleryBinding(),engraving=engravingBinding(),gifts=giftBindings(),canUndo=undoAvailable(),result={inventoryPieces:Array.from(checkedProducts.values()).map(function(row){return {id:row.product.id,handle:row.product.handle,title:row.product.title};}).concat(pieces).filter(function(value,index,rows){return rows.findIndex(function(other){return other.handle===value.handle;})===index;}).sort(function(a,b){return a.handle.localeCompare(b.handle);}).slice(0,160),navigationControls:navigationControls(),changeControls:{canUndo:canUndo,kind:canUndo?undoRecord.kind:null},imageControls:{open:!!imageDialog()},...(engraving?{engravingControls:{available:true,enabled:engraving.enabled,hasText:engraving.input.value.length>0,maxLength:engraving.maximum}}:{}),...(gifts?{giftControls:{wrappingAvailable:!!gifts.wrapping,wrapping:gifts.wrapping?gifts.wrapping.checked:false,giftPackageAvailable:false,noteAvailable:!!gifts.note,hasNote:!!gifts.note&&gifts.note.value.length>0,maxLength:gifts.maximum,savedKnown:false}}:{}),...(gallery?{galleryControls:{handle:gallery.handle,imageCount:gallery.imageCount,selectedIndex:gallery.selectedIndex}}:{}),controlVersion:1,contextRevision:revision,discoveryRevision:discoveryRevision,pageKind:r.page,currentHandle:p&&p.handle||'',focusedHandle:pieces.some(function(x){return x.handle===focused;})?focused:'',visiblePieces:pieces,search:search,sort:sort,filter:filter,loading:loading||navigationPending,activeSection:activeSection,productControls:control,bagControls:{lines:cartBindings().map(function(binding){return {...binding.line};}),itemCount:cartCount},checkoutControls:null};
      if(p)result.currentProduct={id:p.id,handle:p.handle,title:p.title,type:p.type||'',description:p.description||'',url:p.url,currency:p.currency,checkedAt:currentCheckedAt};
      result.bagControls.countKnown=!!cartModel;
      result.bagControls.linesComplete=!!cartModel&&result.bagControls.lines.length===cartModel.lines.length&&result.bagControls.lines.reduce(function(total,line){return total+line.quantity;},0)===cartModel.itemCount;
      result.currentHandle=navigationPending?'':r.handle;
      // Native theme/autofill code can set .value without dispatching input.
      // Compare those private values locally; only the revision is projected.
      var next=JSON.stringify([Object.assign({},result,{contextRevision:0,loading:false}),currency,p&&p.variants.map(function(v){return [v.id,v.price,v.available];}),privateFormRevision,engraving&&engraving.input.value,gifts&&gifts.note&&gifts.note.value]);if(fingerprint&&next!==fingerprint)revision++;fingerprint=next;result.contextRevision=revision;return result;
    }
    function caps(){
      if(navigationPending)return Object.freeze({controlVersion:1,mode:'shopify',actions:Object.freeze([]),quantity:Object.freeze({minimum:1,maximum:20}),reviewBeforeAdd:true,finalOrder:false,addMode:'shopper-confirmation',checkoutMode:'shopper-handoff',notesMode:'unavailable'});
      var actions=['open','highlight','scroll','bag','checkout','back','forward'];if(galleryBinding())actions.push('gallery');if(undoAvailable())actions.push('undo');if(nativeMenus().length)actions.push('close-options');if(imageDialog())actions.push('close-image');if(engravingBinding()?.enabled)actions.push('set-engraving');
      if(all('form[action] input[name="q"]').some(function(input){var f=input.form,u=ownURL(f&&f.getAttribute('action'),origin);return input.type==='search'&&visible(input)&&u&&u.pathname===base+'search'&&(f.getAttribute('method')||'get').toLowerCase()==='get';}))actions.push('search');
      if(confirmedCollections.size||all('a[href]').some(function(a){var u=ownURL(a.getAttribute('href'),origin);return u&&Object.values(COLLECTIONS).some(function(c){return u.pathname===base+'collections/'+c;});}))actions.push('filter');
      if(one('#bjcSort')&&visible(one('#bjcSort').parentElement))actions.push('sort');
      if(form()&&bindings(model())){actions.push('options','select-option');if(quantityBinding())actions.push('product-quantity');if(reviewHook())actions.push('review-add');if(addHook())actions.push('add');}
      if(all('a[href]').some(function(a){var u=ownURL(a.getAttribute('href'),origin);return u&&u.pathname===base+'pages/custom-studio';}))actions.push('customize');
      if(form())actions.push('gift');
      if(cartBindings().length)actions.push('bag-quantity','bag-remove');
      if(route().page==='bag'&&(one('#is-a-gift')||one('#bjGiftWrap')))actions.push('gift');
      if(giftBindings())actions.push('gift-preferences');
      return Object.freeze({controlVersion:1,mode:'shopify',actions:Object.freeze(actions),quantity:Object.freeze({minimum:1,maximum:20}),reviewBeforeAdd:true,finalOrder:false,addMode:'shopper-confirmation',checkoutMode:'shopper-handoff',notesMode:giftBindings()?'native-form':'unavailable'});
    }
    function announce(){if(stopped)return;try{var value=snapshot();doc.dispatchEvent(new win.CustomEvent('brites-storefront:context',{detail:value}));doc.dispatchEvent(new win.CustomEvent('brites:storefront-context',{detail:value}));}catch{}}
    function scheduleContext(){if(stopped||contextScheduled)return;contextScheduled=true;Promise.resolve().then(function(){contextScheduled=false;if(!stopped)announce();});}
    function assertCurrent(ticket){if(stopped||ticket.epoch!==epoch||ticket.signal&&ticket.signal.aborted)throw Object.assign(Error('The website request was cancelled.'),{cancelled:true});if(location.href!==ticket.url||snapshot().contextRevision!==ticket.revision)throw Object.assign(Error('The current shop selection changed. Please ask again.'),{stale:true});}
    function reveal(node,ticket,focus){assertCurrent(ticket);if(!visible(node))throw Error('That part of this product page is not available.');var details=node.tagName==='DETAILS'?node:node.closest('details');if(details)details.open=true;var target=node.tagName==='SELECT'?one('button.bjselx__btn',node.parentElement)||node:node;target.scrollIntoView?.({block:'center',behavior:'auto'});if(focus&&typeof target.focus==='function')target.focus({preventScroll:true});return target;}
    function clearHighlight(node){var held=highlights.get(node);if(!held)return;win.clearTimeout(held.timer);if(node.style.outline===held.appliedOutline)node.style.outline=held.outline;if(node.style.outlineOffset===held.appliedOffset)node.style.outlineOffset=held.offset;highlights.delete(node);}
    function highlight(node){clearHighlight(node);var held={outline:node.style.outline,offset:node.style.outlineOffset};node.style.outline='2px solid #4c708b';node.style.outlineOffset='5px';held.appliedOutline=node.style.outline;held.appliedOffset=node.style.outlineOffset;held.timer=win.setTimeout(function(){clearHighlight(node);},1600);highlights.set(node,held);}
    function disclosure(label){var details=all('#shopify-section-product-template details').filter(function(d){return text(d.querySelector('summary')?.textContent.trim(),120)===label;});return details.length===1?details[0]:null;}
    function bySection(section){var f=form(),p=model(),entries=p&&bindings(p);if(section==='catalogue'||section==='bag'||section==='checkout')return one('#MainContent');if(section==='title')return p&&one('#shopify-section-product-template h1.pi__title');if(section==='price')return p&&one('#bjPrice');if(section==='description'){var description=p&&disclosure('Product Details');return description&&one('.bd',description);}if(section==='materials')return entries&&entries.filter(function(entry){return /^(?:metal(?: choice)?|materials?|finish)$/i.test(entry.name);}).length===1?entries.find(function(entry){return /^(?:metal(?: choice)?|materials?|finish)$/i.test(entry.name);}).node:null;if(section==='length')return entries&&entries.filter(function(entry){return /^(?:(?:chain|necklace) )?length$/i.test(entry.name);}).length===1?entries.find(function(entry){return /^(?:(?:chain|necklace) )?length$/i.test(entry.name);}).node:null;if(section==='engraving')return entries&&entries.find(function(entry){return entry.kind==='engraving';})?.node.closest('#bjEngr')||null;if(section==='options')return f;if(section==='image')return p&&one('#bjMedia');if(section==='gifts'&&route().page==='bag')return one('#is-a-gift')||one('#bjGiftWrap');if(['details','shipping','gifts'].includes(section))return disclosure(section==='shipping'?'Shipping & Returns':'Product Details');return null;}
    function sameOriginRoute(path,ticket){assertCurrent(ticket);var u=ownURL(path,origin);if(!u||!u.pathname.startsWith(base))throw Error('That shop destination is not available.');undoRecord=null;navigate(u.href);recordNavigation(u.href);navigationPending=true;announce();return {ok:true,navigationRequested:true,message:'Opening the requested shop page. Continue its controls when that page is ready.',snapshot:snapshot()};}
    function traverseHistory(delta,ticket){
      assertCurrent(ticket);var currentIndex=currentHistoryIndex(),index=currentIndex+delta;if(currentIndex<0||index<0||index>=journey.entries.length||typeof win.history.go!=='function')throw Error('There is no checked '+(delta<0?'previous':'next')+' shop page in this browsing session.');
      journey.index=index;journey.pending=null;saveJourney();undoRecord=null;navigationPending=true;
      try{win.history.go(delta);}catch(error){navigationPending=false;journey.index=currentIndex;saveJourney();throw error;}
      announce();return {ok:true,navigationRequested:true,message:'Opening the checked '+(delta<0?'previous':'next')+' shop page. Continue when that page is ready.',snapshot:snapshot()};
    }
    async function readMarket(handle,signal){
      var opts={method:'GET',headers:{Accept:'application/json'},cache:'no-store',credentials:'same-origin',signal:signal};
      var results=await Promise.all([fetcher(base+'cart.js',opts),fetcher(base+'products/'+encodeURIComponent(handle)+'.js',opts)]);
      if(results.some(function(r){return !r||!r.ok;}))throw Error('The current options could not be checked. Please use the product page.');
      var data=await Promise.all(results.map(function(r){return r.json();})),cart=data[0],raw=data[1];
      if(!cart||!/^[A-Z]{3}$/.test(cart.currency||'')||!Array.isArray(cart.items)||!Number.isSafeInteger(cart.item_count)||cart.item_count<0)throw Error('The current shop currency could not be checked.');
      var expected=seed&&seed.handle===handle&&id(seed.id)===id(raw&&raw.id)?initialCount:undefined,p=projectProduct(raw,cart.currency,expected);
      if(!p||p.handle!==handle||seed&&seed.handle===handle&&p.id!=='gid://shopify/Product/'+id(seed.id))throw Error('The product identity could not be checked.');
      p.url=origin+base+'products/'+handle;return {product:p,currency:cart.currency,itemCount:cart.item_count,cart:projectCart(cart)};
    }
    function applyChoice(entry,value){
      if(entry.kind==='buttons'){var matches=entry.buttons.filter(function(b){return text(b.textContent.trim(),300)===value;});if(matches.length!==1||matches[0].disabled)throw Error('That option is not available on the product page.');matches[0].click();}
      else if(entry.kind==='select'){var choices=Array.from(entry.node.options).filter(function(o){return o.value===value;});if(choices.length!==1||choices[0].disabled||entry.node.disabled)throw Error('That option is not available on the product page.');entry.node.value=value;entry.node.dispatchEvent(new win.Event('change',{bubbles:true}));}
      else {if(entry.node.disabled)throw Error('That option is not available on the product page.');entry.node.checked=value===entry.values[1];entry.node.dispatchEvent(new win.Event('change',{bubbles:true}));}
    }
    function openPublishedOptions(entries,name,ticket){
      var binding=name?entries.find(function(entry){return entry.name===name;}):null;if(name&&!binding)throw Error('That named option is not published for this piece.');
      var node=binding?.node||form();reveal(node,ticket,true);
      if(binding?.kind==='select'){var button=one('button.bjselx__btn',binding.node.parentElement);if(!button||button.getAttribute('aria-haspopup')!=='listbox')throw Error('Use the visible selector to open those options.');if(button.getAttribute('aria-expanded')!=='true')button.click();if(button.getAttribute('aria-expanded')!=='true')throw Error('The shop has not confirmed that the option menu opened.');}
      activeSection='options';openedOption=binding?.name||null;return binding;
    }
    function missingChoice(p,entries,ticket){
      var previous=chosen(entries),missing=entries.find(function(entry){return !previous.some(function(choice){return choice.name===entry.name&&entry.values.includes(choice.value);});});if(!missing)return null;
      var values=missing.values.filter(function(value){return p.variants.some(function(variant){return variant.available&&!variant.requiresSellingPlan&&variant.options.every(function(option){return option.name===missing.name?option.value===value:previous.every(function(choice){return choice.name!==option.name||!choice.value||choice.value===option.value;});});});});
      if(!values.length)throw Error('Those choices do not match an available combination. Your choices are preserved; ask for similar pieces or choose another published value.');
      openPublishedOptions(entries,missing.name,ticket);announce();var message='Next, choose '+missing.name+' — '+values.join(' or ')+'. I have opened its published choices.';return failure(message,{message:message,reasonCode:'missing-options',selectionStatus:'choosing',cartChanged:false,snapshot:snapshot()});
    }
    async function execute(raw,context){
      context=context||{};var action=validateAction(raw),requestId=text(context.requestId,200);
      if(!action)return failure('That website control is not supported.');if(!requestId)return failure('A current shopper request is required.');if(stopped||context.signal&&context.signal.aborted)return failure('The website request was cancelled.',{cancelled:true});
      if(navigationPending)return failure('The requested shop page is still opening. Continue after its new controls are ready.',{navigationRequested:true});
      if(!caps().actions.includes(action.type))return failure('That control is not available on this shop page.');
      if(pending.has(requestId))return failure('That request has already been handled.',{suppressed:true});
      var before=snapshot(),ticket={url:location.href,revision:before.contextRevision,signal:context.signal,epoch:++epoch},targetHandle=action.handle||before.currentHandle,undoCandidate=captureUndo(action);
      if(action.handle&&!before.visiblePieces.concat(before.inventoryPieces||[]).some(function(p){return p.handle===action.handle;}))return failure('Choose an exact checked piece from this shop first.');
      if(['options','select-option','product-quantity','set-engraving','add','review-add'].includes(action.type)&&(!targetHandle||targetHandle!==before.currentHandle))return failure('Open that product page before changing its options.');
      pending.set(requestId,true);if(pending.size>200)pending.delete(pending.keys().next().value);
      try{
        if(action.type==='undo')return await undoChange(ticket);
        if(action.type==='close-options'){assertCurrent(ticket);var menus=nativeMenus();for(var menu of menus)if(menu.button.getAttribute('aria-expanded')==='true'){if(menu.button.disabled)throw Error('That native menu cannot be closed yet.');menu.button.click();if(menu.button.getAttribute('aria-expanded')!=='false')throw Error('The shop has not confirmed that menu closed.');}openedOption=null;activeSection='';announce();return {ok:true,message:'The native option menus are closed.',snapshot:snapshot()};}
        if(action.type==='close-image'){var dialog=imageDialog();assertCurrent(ticket);if(!dialog)throw Error('There is no verified shop image dialog to close.');dialog.close();if(dialog.open)throw Error('The shop has not confirmed that the image closed.');activeSection='';announce();return {ok:true,message:'The enlarged shop image is closed.',snapshot:snapshot()};}
        if(action.type==='gift-preferences'){assertCurrent(ticket);applyGift(action);rememberUndo(undoCandidate);activeSection='gifts';announce();return {ok:true,message:'The shop\u2019s visible gift choices are updated. Review any gift charge before checkout; these fields are saved by the shop\u2019s own cart controls.',snapshot:snapshot()};}
        if(['bag-quantity','bag-remove'].includes(action.type)){var edited=await editNativeCart(action,ticket);if(action.type==='bag-remove')undoRecord=null;else rememberUndo(undoCandidate);announce();return Object.assign({},edited,{snapshot:snapshot()});}
        if(action.type==='open')return sameOriginRoute(base+'products/'+action.handle,ticket);
        if(action.type==='back'||action.type==='forward')return traverseHistory(action.type==='back'?-1:1,ticket);
        if(action.type==='gallery'){var gallery=galleryBinding();if(!gallery||gallery.handle!==action.handle||action.index>gallery.imageCount)throw Error('That numbered image is not published on this product page.');reveal(gallery.panes[action.index-1],ticket,false);if(galleryBinding()?.selectedIndex!==action.index)throw Error('The shop has not confirmed that the requested image is visible.');highlight(gallery.panes[action.index-1]);rememberUndo(undoCandidate);activeSection='image';announce();return {ok:true,message:'Image '+action.index+' of '+gallery.imageCount+' is visible.',snapshot:snapshot(),shownImageIndex:action.index};}
        if(action.type==='scroll'&&action.direction){assertCurrent(ticket);var page=one('#MainContent'),height=Number(win.innerHeight);if(!page||!visible(page))throw Error('The shop page is not ready to scroll.');if(action.direction==='top'||action.direction==='bottom'){if(typeof win.scrollTo!=='function')throw Error('Use the visible page to scroll.');win.scrollTo({top:action.direction==='top'?0:Math.max(doc.documentElement.scrollHeight,doc.body?.scrollHeight||0),behavior:'auto'});}else {if(typeof win.scrollBy!=='function'||!(height>0))throw Error('Use the visible page to scroll.');win.scrollBy({top:(action.direction==='up'?-1:1)*Math.max(200,Math.round(height*.75)),behavior:'auto'});}activeSection='';announce();return {ok:true,message:'The shop page has scrolled '+action.direction+'.',snapshot:snapshot()};}
        if(action.type==='bag'||action.type==='checkout'){
          if(route().page==='bag'){
            if(action.type==='checkout'&&cartModel&&cartModel.itemCount>0){var checked=await readCart(ticket.signal);assertCurrent(ticket);if(!sameCart(checked,cartModel))throw Error('Your bag changed. Review it before continuing.');var f=cartForm(),checkout=f&&one('button[name="checkout"][type="submit"]',f);if(!checkout||checkout.disabled||!visible(checkout)||typeof f.requestSubmit!=='function')throw Error('Use the shop\u2019s visible checkout button to continue.');f.requestSubmit(checkout);undoRecord=null;navigationPending=true;announce();return {ok:true,navigationRequested:true,message:'Continuing to the shop\u2019s checkout. Review shipping, tax and payment yourself before placing your order.',snapshot:snapshot(),orderPlaced:false};}
            var main=one('#MainContent');if(main)reveal(main,ticket,false);activeSection='bag';announce();return {ok:true,message:action.type==='checkout'?'Review your bag, then use the shop\u2019s checkout button to continue.':'Your bag is open.',snapshot:snapshot()};}
          var bag=sameOriginRoute(base+'cart',ticket);bag.message=action.type==='checkout'?'Opening your bag for review. Continue to checkout yourself when ready.':'Opening your bag.';return bag;
        }
        if(action.type==='filter'){
          var path=base+'collections/'+COLLECTIONS[action.filter],found=confirmedCollections.has(COLLECTIONS[action.filter])||all('a[href]').some(function(a){return ownURL(a.getAttribute('href'),origin)?.pathname===path;});if(!found)throw Error('That collection is not confirmed by this shop page.');var result=sameOriginRoute(path,ticket);if(action.filter==='regular-necklaces')result.message='Opening the shop\u2019s Necklaces collection; its own published collection membership applies.';return result;
        }
        if(action.type==='search'){
          if(action.filter&&action.filter!=='all')throw Error('Use the shop\u2019s category filter separately from search.');if(action.sort&&!['featured','price-asc','price-desc'].includes(action.sort))throw Error('That search ordering is not available.');
          var url=new URL(base+'search',origin);url.searchParams.set('type','product');url.searchParams.set('q',action.query);if(action.sort)url.searchParams.set('sort_by',SORTS[action.sort]);return sameOriginRoute(url.href,ticket);
        }
        if(action.type==='sort'){
          var select=one('#bjcSort'),choices=select&&Array.from(select.options).filter(function(o){return o.value===SORTS[action.sort];});if(!choices||choices.length!==1||choices[0].disabled||select.disabled)throw Error('That sort order is not offered on this collection.');assertCurrent(ticket);select.value=SORTS[action.sort];select.dispatchEvent(new win.Event('change',{bubbles:true}));if(select.value!==SORTS[action.sort])throw Error('The shop could not confirm that sort.');rememberUndo(undoCandidate);announce();return {ok:true,message:'The shop\u2019s sort control is updated.',snapshot:snapshot()};
        }
        if(action.type==='customize'){
          if(action.section==='options'){reveal(form(),ticket,false);activeSection='options';announce();return {ok:true,message:'The published product options are ready.',snapshot:snapshot()};}
          var studio=base+'pages/custom-studio';if(!all('a[href]').some(function(a){return ownURL(a.getAttribute('href'),origin)?.pathname===studio;}))throw Error('The design studio is not linked by this shop page.');return sameOriginRoute(studio,ticket);
        }
        if(['highlight','scroll','gift'].includes(action.type)){
          var section=action.type==='gift'?'gifts':action.section;if(['title','description','materials','length','engraving','price','details','options','image'].includes(section)&&targetHandle!==before.currentHandle)throw Error('Open that piece to show its exact details.');var node=bySection(section);if(!node)throw Error('That section is not available on the current shop page.');reveal(node,ticket,false);if(action.type==='highlight')highlight(node);activeSection=section;announce();return {ok:true,message:'The requested part of the shop page is visible.',snapshot:snapshot()};
        }
        if(action.type==='add'){var pageProduct=model(),pageEntries=bindings(pageProduct);if(pageEntries&&pageProduct.variantsComplete&&!pageProduct.cartHold&&!pageProduct.partsOnly){var help=missingChoice(pageProduct,pageEntries,ticket);if(help)return help;}}
        var freshReview=['review-add','add'].includes(action.type),market=freshReview?await readMarket(targetHandle,ticket.signal):{product:model(),currency:currency,itemCount:cartCount,cart:cartModel};assertCurrent(ticket);var p=market.product,entries=bindings(p);if(!entries||!p.variantsComplete)throw Error('Please use this product page to choose the complete options.');
        var chosenBefore=selectedVariant(p,entries),q=selectedQuantity();if(!chosenBefore&&action.type==='review-add'||q===null&&!['options','select-option','set-engraving'].includes(action.type))throw Error('The exact current options could not be checked.');
        if(action.type==='options'){
          openPublishedOptions(entries,action.optionName,ticket);
        }else if(action.type==='select-option'){
          var previous=chosen(entries),exact,requested;
          if(action.variantId){exact=p.variants.find(function(variant){return variant.id===action.variantId;});if(!exact?.available||exact.requiresSellingPlan)throw Error('That exact available option combination is not published.');}
          else {requested=entries.find(function(entry){return entry.name===action.optionName;});if(!requested||!requested.values.includes(action.optionValue))throw Error('That literal option is not published for this piece.');var possible=p.variants.filter(function(variant){return variant.available&&!variant.requiresSellingPlan&&variant.options.every(function(option){return option.name===action.optionName?option.value===action.optionValue:previous.every(function(choice){return choice.name!==option.name||!choice.value||choice.value===option.value;});});});if(!possible.length)throw Error('That exact available option combination is not published.');}
          assertCurrent(ticket);
          if(exact)entries.forEach(function(entry,index){if(previous[index].value!==exact.options[index].value)applyChoice(entry,exact.options[index].value);});else if(previous.find(function(choice){return choice.name===requested.name;})?.value!==action.optionValue)applyChoice(requested,action.optionValue);
          var observed=chosen(bindings(p)),after=selectedVariant(p,bindings(p));
          if(selectedQuantity()!==q||(exact?(!after||after.id!==exact.id):observed.length!==previous.length||observed.some(function(choice){return choice.name===requested.name?choice.value!==action.optionValue:previous.find(function(prior){return prior.name===choice.name;})?.value!==choice.value;})))throw Error('The shop could not confirm that selection. Please check its visible controls.');
          activeSection='options';openedOption=action.optionName||null;
          var next=observed.find(function(choice){return !choice.value;});if(before.productControls?.optionsOpen&&next){ticket.revision=snapshot().contextRevision;openPublishedOptions(entries,next.name,ticket);}
        }else if(action.type==='product-quantity'){
          assertCurrent(ticket);applyQuantity(action.quantity,ticket);activeSection='options';
        }else if(action.type==='set-engraving'){
          var field=engravingBinding();if(!field?.enabled||!literalNote(action.text,field.maximum))throw Error('Select the published engraving option and use its available text field first.');assertCurrent(ticket);applyText(field.input,action.text);activeSection='engraving';
        }else if(action.type==='add'){
          var variant=action.variantId?p.variants.find(function(v){return v.id===action.variantId;}):chosenBefore,prior=before.productControls?.selectedVariant;
          if(!chosenBefore||!variant||!variant.available||variant.id!==chosenBefore.id||variant.requiresSellingPlan||p.requiresSellingPlan||p.cartHold||p.partsOnly)throw Error('Choose the exact available options on this product page before adding.');
          if(!prior||p.id!==before.productControls.productId||variant.id!==before.productControls.variantId||q!==before.productControls.quantity||p.currency!==prior.currency||Math.abs(variant.price-prior.price)>.005)throw Error('The exact price or option changed. Review its current selection before adding.');
          if(!market.cart)throw Error('The current bag could not be checked before adding.');
          var hook=addHook();if(!hook||!context.reviewAuthority||typeof context.reviewAuthority!=='object')throw Error('Use the current concierge request to add the exact selection.');
          var result=await hook({product:p,variantId:variant.id,quantity:q,signal:ticket.signal,requestId:requestId,reviewAuthority:context.reviewAuthority});
          if(stopped||ticket.epoch!==epoch||ticket.signal?.aborted||location.href!==ticket.url)return failure('The website request was cancelled.',{cancelled:true});
          if(!result||result.ok!==true||result.cartChanged!==true)throw Error(text(result?.reason||result?.message,500)||'The exact addition has not been confirmed. Check your bag before asking again.');
          var afterCart=await readCart(ticket.signal);
          if(stopped||ticket.epoch!==epoch||ticket.signal?.aborted||location.href!==ticket.url)return failure('The website request was cancelled.',{cancelled:true});
          if(selectedQuantity()!==q||selectedVariant(p,bindings(p))?.id!==variant.id||!exactAddition(market.cart,afterCart,p,variant,q))throw Error('The exact cart addition could not be confirmed. Check your bag before asking again.');
          currency=afterCart.currency;cartCount=afterCart.itemCount;cartModel=afterCart;current=p;currentCheckedAt=Date.now();checkedProducts.set(p.handle,{product:p,checkedAt:currentCheckedAt});undoRecord=null;announceInventory();announce();
          return {ok:true,cartChanged:true,live:true,checkedAt:currentCheckedAt,products:[p],message:'The selected '+p.title+' is in your bag. Open your bag whenever you are ready.',snapshot:snapshot()};
        }else if(action.type==='review-add'){
          var variant=action.variantId?p.variants.find(function(v){return v.id===action.variantId;}):chosenBefore;if(!variant||!variant.available||variant.id!==chosenBefore.id||variant.requiresSellingPlan||p.requiresSellingPlan)throw Error('Choose the exact available options on the product page before reviewing.');
          var hook=reviewHook();if(!hook||!context.reviewAuthority||typeof context.reviewAuthority!=='object')throw Error('Use the current concierge request to prepare the review.');
          var result=await hook({product:p,variantId:variant.id,quantity:q,signal:ticket.signal,requestId:requestId,reviewAuthority:context.reviewAuthority});
          if(stopped||ticket.epoch!==epoch||ticket.signal&&ticket.signal.aborted)return failure('The website request was cancelled.',{cancelled:true});if(!result||result.ok!==true&&result.prepared!==true)throw Error(text(result?.reason||result?.message,500)||'The exact choice could not be prepared for review.');
          currency=market.currency;cartCount=market.itemCount;cartModel=market.cart;current=p;currentCheckedAt=Date.now();checkedProducts.set(p.handle,{product:p,checkedAt:currentCheckedAt});announceInventory();announce();return {ok:true,live:true,checkedAt:currentCheckedAt,products:[p],message:'Review the exact choice, then click Confirm add to bag.',snapshot:snapshot(),cartChanged:false,requiredCustomerClick:'Confirm add to bag'};
        }
        currency=market.currency;cartCount=market.itemCount;cartModel=market.cart;current=p;rememberUndo(undoCandidate);announce();return {ok:true,live:true,checkedAt:currentCheckedAt,products:[p],message:action.type==='select-option'?'The exact published option is selected.':action.type==='product-quantity'?'The product quantity is updated.':action.type==='set-engraving'?'Your exact engraving text is in the shop\u2019s visible field. Personalized additions still need the shop\u2019s customizer review.':'The published product options are ready.',snapshot:snapshot(),cartChanged:false};
      }catch(error){return failure(error.cancelled?'The website request was cancelled.':error.stale?'The current shop selection changed. Please ask again.':text(error.message,500)||'The current shop controls could not be checked.',{cancelled:!!error.cancelled,stale:!!error.stale});}
    }
    function listen(type,callback,target){target=target||doc;target.addEventListener(type,callback);listeners.push([type,callback,target]);}
    function attention(event){var card=event.target&&event.target.closest&&event.target.closest('a.bjc-pcard[href]');var next=card&&visible(card)?productHandle(card.getAttribute('href'),origin,base):'';if(next!==focused){focused=next;announce();}}
    function releaseAttention(event){var card=event.target?.closest?.('a.bjc-pcard[href]');if(!card||card.contains(event.relatedTarget))return;var next=event.relatedTarget?.closest?.('a.bjc-pcard[href]'),handle=next&&visible(next)?productHandle(next.getAttribute('href'),origin,base):'';if(handle!==focused){focused=handle;announce();}}
    listen('pointerover',attention);listen('focusin',attention);listen('pointerout',releaseAttention);listen('focusout',releaseAttention);
    listen('change',function(event){if(event.target?.closest?.('#bjForm,#MainContent'))scheduleContext();});listen('brites:cart-updated',function(){void refresh();});listen('cart:updated',function(){void refresh();});listen('ajaxProduct:added',function(){void refresh();});
    listen('click',function(event){
      if(event.target?.closest?.('#bjForm,#bjMedia,#MainContent'))scheduleContext();
      var link=event.target?.closest?.('a[href]');if(!link||event.defaultPrevented||event.button!==0||event.ctrlKey||event.metaKey||event.altKey||event.shiftKey||link.target==='_blank')return;var url=historyURL(link.href);if(url)recordNavigation(url);
    });
    listen('toggle',function(event){if(event.target?.closest?.('#MainContent'))scheduleContext();});
    listen('input',function(event){if(event.target?.matches?.('#bjEngrTxt,#gift-note'))privateFormRevision++;if(event.target?.closest?.('#bjForm,#MainContent'))scheduleContext();});
    var contextRoot=one('#MainContent');if(contextRoot&&typeof win.MutationObserver==='function'){contextObserver=new win.MutationObserver(scheduleContext);contextObserver.observe(contextRoot,{subtree:true,childList:true,attributes:true,attributeFilter:['class','aria-expanded','aria-pressed','hidden','open','value','checked','data-key']});}
    function restoredPage(){if(stopped)return;epoch++;navigationPending=false;loading=false;undoRecord=null;restoreHistoryEntry();announce();}
    listen('popstate',restoredPage,win);listen('pageshow',function(event){if(event.persisted)restoredPage();},win);
    async function refresh(){if(stopped)return false;var mine=epoch,url=location.href,target=route().handle;loading=true;try{if(!target){var r=await fetcher(base+'cart.js',{method:'GET',headers:{Accept:'application/json'},cache:'no-store',credentials:'same-origin'});var c=r.ok&&await r.json();if(!c||!/^[A-Z]{3}$/.test(c.currency||'')||!Number.isSafeInteger(c.item_count)||c.item_count<0)throw Error('Unavailable');if(stopped||mine!==epoch||url!==location.href)return false;currency=c.currency;cartCount=c.item_count;cartModel=projectCart(c);return true;}
      var market=await readMarket(target);if(stopped||mine!==epoch||url!==location.href)return false;current=market.product;currency=market.currency;cartCount=market.itemCount;cartModel=market.cart;currentCheckedAt=Date.now();checkedProducts.set(current.handle,{product:current,checkedAt:currentCheckedAt});announceInventory();return true;
    }catch{return false;}finally{loading=false;if(!stopped)announce();}}
    function copyProduct(p,checkedAt){
      var product={id:p.id,handle:p.handle,title:p.title,type:p.type||'',description:p.description||'',url:p.url,currency:p.currency,minPrice:p.minPrice,variantsComplete:p.variantsComplete===true,checkedAt:checkedAt,options:p.optionGroups.map(function(g){return {name:g.name,values:g.values.slice()};}),variants:p.variants.map(function(v){return {id:v.id,numericId:v.numericId,title:v.title,price:v.price,available:v.available,options:v.options.map(function(o){return {...o};}),...(v.requiresSellingPlan===true?{requiresSellingPlan:true}:{})};})};
      if(Array.isArray(p.images)&&p.images.length){product.images=p.images.map(function(image){return {...image};});product.image=p.image;product.imageAlt=p.imageAlt||'';}
      ['cartHold','recommendationHold','partsOnly','meaningHold','requiresSellingPlan'].forEach(function(key){if(p[key]===true)product[key]=true;});return product;
    }
    function inventoryStatus(){var total=checkedProducts.size,loaded=Array.from(checkedProducts.values()).filter(function(row){return row.product.currency===currency&&Date.now()-row.checkedAt<=60000;}).length;return {total:total,loaded:loaded,ready:total>0&&loaded===total,partial:loaded!==total,loading:loading};}
    function announceInventory(){if(stopped)return;try{doc.dispatchEvent(new win.CustomEvent('brites-storefront:inventory',{detail:inventoryStatus()}));}catch{}}
    async function readProduct(handle,options){options=options||{};if(stopped||options.signal?.aborted||!h(handle))return null;var cached=checkedProducts.get(handle),known=handle===route().handle||cards().some(function(p){return p.handle===handle;})||!!cached;if(!known)return null;if(cached&&cached.product.currency===currency&&Date.now()-cached.checkedAt<=60000)return {live:true,cached:true,checkedAt:cached.checkedAt,product:copyProduct(cached.product,cached.checkedAt)};var epochAtRead=epoch,urlAtRead=location.href,market=await readMarket(handle,options.signal);if(stopped||options.signal?.aborted||epochAtRead!==epoch||urlAtRead!==location.href)return null;var row={product:market.product,checkedAt:Date.now()};checkedProducts.set(handle,row);if(checkedProducts.size>200)checkedProducts.delete(checkedProducts.keys().next().value);announceInventory();return {live:true,cached:true,checkedAt:row.checkedAt,product:copyProduct(row.product,row.checkedAt)};}
    function getInventory(){return Array.from(checkedProducts.values()).filter(function(row){return row.product.currency===currency&&Date.now()-row.checkedAt<=60000;}).map(function(row){return copyProduct(row.product,row.checkedAt);});}
    var api={snapshot:snapshot,execute:execute,refresh:refresh,readProduct:readProduct,getInventory:getInventory,inventoryStatus:inventoryStatus,destroy:function(){stopped=true;epoch++;contextObserver?.disconnect();contextObserver=null;listeners.forEach(function(pair){pair[2].removeEventListener(pair[0],pair[1]);});listeners=[];pending.clear();undoRecord=null;Array.from(highlights.keys()).forEach(clearHighlight);}};
    Object.defineProperty(api,'capabilities',{enumerable:true,get:caps});return api;
  }
  function install(win){
    if(!win||!win.document||win.BritesStorefrontAdapter)return null;var nodes=Array.from(win.document.querySelectorAll('script[data-brites-shopify-config]'));if(nodes.length!==1)return null;
    try{var value=JSON.parse(nodes[0].textContent),instance=create(Object.assign({},value,{window:win}));if(instance){win.BritesStorefrontAdapter=instance;void instance.refresh();}return instance;}catch{return null;}
  }
  return Object.freeze({create:create,install:install,validateAction:validateAction,projectProduct:projectProduct,rootPath:rootPath,productHandle:productHandle});
});
