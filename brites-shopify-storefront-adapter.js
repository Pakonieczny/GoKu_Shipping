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
  var ACTIONS={search:['type','query','sort','filter'],sort:['type','sort'],filter:['type','filter'],open:['type','handle'],highlight:['type','handle','section'],scroll:['type','handle','section'],bag:['type'],checkout:['type'],gift:['type','section'],customize:['type','handle','section'],options:['type','handle','optionName'],'select-option':['type','handle','variantId','optionName','optionValue'],'product-quantity':['type','handle','quantity'],'review-add':['type','handle','variantId'],'bag-quantity':['type','lineId','quantity'],'bag-remove':['type','lineId']};
  var SECTIONS=['price','details','options','image','shipping','gifts','catalogue','bag','checkout','offers','customize'];
  function text(value,max){return typeof value==='string'&&value.length<=max&&!/[\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069<>]/.test(value)?value.trim():'';}
  function h(value){return text(value,180)&&HANDLE.test(value)?value:'';}
  function id(value){return typeof value==='number'?Number.isSafeInteger(value)&&value>0?String(value):'':typeof value==='string'&&NUMERIC.test(value)?value:'';}
  function rootPath(value){return typeof value==='string'&&ROOT.test(value)?value:null;}
  function quantity(value){return Number.isSafeInteger(value)&&value>=1&&value<=20?value:null;}
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
    if(['open','select-option','product-quantity','review-add'].includes(value.type)&&!out.handle)return null;
    if(value.type==='search'){if(!text(value.query,180)||/https?:\/\/|javascript\s*:|(?:^|\W)(?:document|window)\s*[.\[]/i.test(value.query))return null;out.query=value.query.trim();}
    if(value.sort!==undefined){if(typeof value.sort!=='string'||!Object.hasOwn(SORTS,value.sort))return null;out.sort=value.sort;}
    if(value.type==='sort'&&!out.sort)return null;
    if(value.filter!==undefined){if(typeof value.filter!=='string'||!Object.hasOwn(COLLECTIONS,value.filter))return null;out.filter=value.filter;}
    if(value.type==='filter'&&!out.filter)return null;
    if(value.section!==undefined){if(!SECTIONS.includes(value.section))return null;out.section=value.section;}
    if(['highlight','scroll','gift','customize'].includes(value.type)&&!out.section)return null;
    if(value.type==='gift'&&out.section!=='gifts'||value.type==='customize'&&!['customize','options'].includes(out.section))return null;
    if(value.optionName!==undefined){if(!text(value.optionName,120))return null;out.optionName=value.optionName;}
    if(value.optionValue!==undefined){if(!text(value.optionValue,300))return null;out.optionValue=value.optionValue;}
    if(value.variantId!==undefined){if(typeof value.variantId!=='string'||!VARIANT.test(value.variantId))return null;out.variantId=value.variantId;}
    if(value.type==='select-option'&&(out.variantId?out.optionName!==undefined||out.optionValue!==undefined:!out.optionName||!out.optionValue))return null;
    if(['product-quantity','bag-quantity'].includes(value.type)){if(quantity(value.quantity)===null)return null;out.quantity=value.quantity;}
    if(['bag-quantity','bag-remove'].includes(value.type)){if(typeof value.lineId!=='string'||!LINE.test(value.lineId))return null;out.lineId=value.lineId;}
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
    if(raw.requires_selling_plan===true)out.requiresSellingPlan=true;
    return out;
  }
  function create(config){
    config=config||{};var win=config.window||(typeof window!=='undefined'?window:null),doc=config.document||win&&win.document,fetcher=config.fetch||win&&win.fetch&&win.fetch.bind(win),location=config.location||win&&win.location;
    var base=rootPath(config.root),origin;try{origin=new URL(location.href).origin;}catch{return null;}
    if(!base||!doc||!fetcher||!/^https:\/\/(?:www\.)?britesjewelry\.com$/.test(origin)||config.theme!=='brites-v1')return null;
    var initialCount=config.variantCount,seed=config.product||null,currency=/^[A-Z]{3}$/.test(config.currency||'')?config.currency:null,current=currency?projectProduct(seed,currency,initialCount):null;
    var stopped=false,epoch=0,revision=0,fingerprint='',discoveryRevision=0,lastDiscovery='',focused='',openedOption=null,activeSection='',cartCount=0,cartModel=null,loading=false,pending=new Map(),listeners=[],highlights=new Map();
    var navigate=typeof config.navigate==='function'?config.navigate:function(url){location.assign(url);};
    var confirmedCollections=new Set((Array.isArray(config.collectionHandles)?config.collectionHandles:[]).filter(function(value){return h(value)&&Object.values(COLLECTIONS).includes(value);}));
    function all(selector,scope){try{return Array.from((scope||doc).querySelectorAll(selector));}catch{return [];}}
    function one(selector,scope){var nodes=all(selector,scope);return nodes.length===1?nodes[0]:null;}
    function visible(node){if(!node||!node.isConnected||node.hidden||node.closest('[hidden],[aria-hidden="true"]'))return false;try{var css=win.getComputedStyle(node);return css.display!=='none'&&css.visibility!=='hidden';}catch{return true;}}
    function route(){var u=ownURL(location.href,origin),relative=u&&u.pathname.startsWith(base)?u.pathname.slice(base.length):'';return {url:u,page:productHandle(location.href,origin,base)?'product':/^collections\//.test(relative)?'catalogue':/^search\/?$/.test(relative)?'catalogue':/^cart\/?$/.test(relative)?'bag':'home',handle:productHandle(location.href,origin,base)};}
    function form(){var r=route(),f=one('#bjForm');return r.page==='product'&&seed&&r.handle===seed.handle&&f&&visible(f)?f:null;}
    function model(){return current&&form()&&current.handle===route().handle?current:null;}
    function bindings(p){
      var f=form();if(!p||!f)return null;var result=[],used=new Set();
      for(var i=0;i<p.optionGroups.length;i++){
        var group=p.optionGroups[i],index=String(i+1),metal=one('#bjMetals',f),selects=all('select.bjOptSel',f).filter(function(n){return n.getAttribute('data-idx')===index;}),engr=one('#bjEngr',f),entry=null;
        if(metal&&metal.getAttribute('data-idx')===index){
          var buttons=all('button[data-vi]',metal),values=buttons.map(function(b){return text(b.textContent.trim(),300);});
          if(buttons.length===group.values.length&&buttons.every(function(b,j){return b.type==='button'&&b.getAttribute('data-vi')===String(j);})&&new Set(values).size===values.length&&values.every(function(v){return group.values.includes(v);}))entry={kind:'buttons',node:metal,buttons:buttons,values:values};
        }else if(selects.length===1){
          var s=selects[0],sv=Array.from(s.options).map(function(o){return o.value;});
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
    function reviewHook(){return typeof config.reviewAdd==='function'?config.reviewAdd:win&&typeof win.BritesConciergeShopifyControls?.reviewAdd==='function'?win.BritesConciergeShopifyControls.reviewAdd:null;}
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
    function cards(){var seen=new Set();return all('main a.bjc-pcard[href]').flatMap(function(a){var handle=productHandle(a.getAttribute('href'),origin,base),title=text(one('.bjc-t',a)?.textContent.trim(),300);if(!visible(a)||!handle||!title||seen.has(handle))return [];seen.add(handle);return [{handle:handle,title:title}];}).slice(0,200);}
    function snapshot(){
      var r=route(),p=model(),entries=bindings(p),variant=selectedVariant(p,entries),pieces=cards(),groups=p&&entries?p.optionGroups.map(function(g){return {name:g.name,values:g.values.slice()};}):[];
      if(p)pieces.unshift({handle:p.handle,title:p.title,id:p.id});pieces=pieces.filter(function(v,i,arr){return arr.findIndex(function(o){return o.handle===v.handle;})===i;});
      var q=selectedQuantity(),control=p&&entries?{handle:p.handle,productId:p.id,productTitle:p.title,productType:p.type,variantId:variant&&variant.id||null,quantity:q,optionsOpen:activeSection==='options',openedOption:openedOption,optionGroups:groups,selectedOptions:chosen(entries),...(variant?{selectedVariant:{id:variant.id,title:variant.title,price:variant.price,currency:p.currency,available:variant.available,options:variant.options.map(function(o){return {name:o.name,value:o.value};})}}:{}),...(variant&&q!==null?{itemTotalPrice:Math.round(variant.price*q*100)/100}:{}),reviewReady:false}:null;
      var sortNode=one('#bjcSort'),sort=sortNode?Object.keys(SORTS).find(function(k){return SORTS[k]===sortNode.value;})||'featured':'featured';
      var collection=r.url&&r.url.pathname.slice(base.length).match(/^collections\/([a-z0-9-]+)\/?$/),filter=collection?Object.keys(COLLECTIONS).find(function(k){return COLLECTIONS[k]===collection[1];})||'all':'all',search=r.url&&text(r.url.searchParams.get('q'),180)||'';
      var discovery=JSON.stringify([r.url&&r.url.pathname,search,sort,filter,pieces]);if(lastDiscovery&&discovery!==lastDiscovery)discoveryRevision++;lastDiscovery=discovery;
      var result={controlVersion:1,contextRevision:revision,discoveryRevision:discoveryRevision,pageKind:r.page,currentHandle:p&&p.handle||'',focusedHandle:pieces.some(function(x){return x.handle===focused;})?focused:'',visiblePieces:pieces,search:search,sort:sort,filter:filter,loading:loading,activeSection:activeSection,productControls:control,bagControls:{lines:cartBindings().map(function(binding){return {...binding.line};}),itemCount:cartCount},checkoutControls:null};
      var next=JSON.stringify([Object.assign({},result,{contextRevision:0,loading:false}),currency,p&&p.variants.map(function(v){return [v.id,v.price,v.available];})]);if(fingerprint&&next!==fingerprint)revision++;fingerprint=next;result.contextRevision=revision;return result;
    }
    function caps(){
      var actions=['open','highlight','scroll','bag','checkout'];
      if(all('form[action] input[name="q"]').some(function(input){var f=input.form,u=ownURL(f&&f.getAttribute('action'),origin);return input.type==='search'&&visible(input)&&u&&u.pathname===base+'search'&&(f.getAttribute('method')||'get').toLowerCase()==='get';}))actions.push('search');
      if(confirmedCollections.size||all('a[href]').some(function(a){var u=ownURL(a.getAttribute('href'),origin);return u&&Object.values(COLLECTIONS).some(function(c){return u.pathname===base+'collections/'+c;});}))actions.push('filter');
      if(one('#bjcSort')&&visible(one('#bjcSort').parentElement))actions.push('sort');
      if(form()&&bindings(model())){actions.push('options','select-option');if(quantityBinding())actions.push('product-quantity');if(reviewHook())actions.push('review-add');}
      if(all('a[href]').some(function(a){var u=ownURL(a.getAttribute('href'),origin);return u&&u.pathname===base+'pages/custom-studio';}))actions.push('customize');
      if(form())actions.push('gift');
      if(cartBindings().length)actions.push('bag-quantity','bag-remove');
      if(route().page==='bag'&&(one('#is-a-gift')||one('#bjGiftWrap')))actions.push('gift');
      return Object.freeze({controlVersion:1,mode:'shopify',actions:Object.freeze(actions),quantity:Object.freeze({minimum:1,maximum:20}),reviewBeforeAdd:true,finalOrder:false,addMode:'shopper-confirmation',checkoutMode:'shopper-handoff',notesMode:'unavailable'});
    }
    function announce(){if(stopped)return;try{doc.dispatchEvent(new win.CustomEvent('brites:storefront-context',{detail:snapshot()}));}catch{}}
    function assertCurrent(ticket){if(stopped||ticket.epoch!==epoch||ticket.signal&&ticket.signal.aborted)throw Object.assign(Error('The website request was cancelled.'),{cancelled:true});if(location.href!==ticket.url||snapshot().contextRevision!==ticket.revision)throw Object.assign(Error('The current shop selection changed. Please ask again.'),{stale:true});}
    function reveal(node,ticket,focus){assertCurrent(ticket);if(!visible(node))throw Error('That part of this product page is not available.');var details=node.tagName==='DETAILS'?node:node.closest('details');if(details)details.open=true;var target=node.tagName==='SELECT'?one('button.bjselx__btn',node.parentElement)||node:node;target.scrollIntoView?.({block:'center',behavior:'auto'});if(focus&&typeof target.focus==='function')target.focus({preventScroll:true});return target;}
    function clearHighlight(node){var held=highlights.get(node);if(!held)return;win.clearTimeout(held.timer);if(node.style.outline===held.appliedOutline)node.style.outline=held.outline;if(node.style.outlineOffset===held.appliedOffset)node.style.outlineOffset=held.offset;highlights.delete(node);}
    function highlight(node){clearHighlight(node);var held={outline:node.style.outline,offset:node.style.outlineOffset};node.style.outline='2px solid #4c708b';node.style.outlineOffset='5px';held.appliedOutline=node.style.outline;held.appliedOffset=node.style.outlineOffset;held.timer=win.setTimeout(function(){clearHighlight(node);},1600);highlights.set(node,held);}
    function bySection(section){var f=form();if(section==='catalogue'||section==='bag'||section==='checkout')return one('#MainContent');if(section==='price')return one('#bjPrice');if(section==='options')return f;if(section==='image')return one('#bjMedia');if(section==='gifts'&&route().page==='bag')return one('#is-a-gift')||one('#bjGiftWrap');if(['details','shipping','gifts'].includes(section)){var label=section==='shipping'?'Shipping & Returns':'Product Details',details=all('#shopify-section-product-template details').filter(function(d){return text(d.querySelector('summary')?.textContent.trim(),120)===label;});return details.length===1?details[0]:null;}return null;}
    function sameOriginRoute(path,ticket){assertCurrent(ticket);var u=ownURL(path,origin);if(!u||!u.pathname.startsWith(base))throw Error('That shop destination is not available.');navigate(u.href);return {ok:true,navigationRequested:true,message:'Opening the requested shop page.',snapshot:snapshot()};}
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
    async function execute(raw,context){
      context=context||{};var action=validateAction(raw),requestId=text(context.requestId,200);
      if(!action)return failure('That website control is not supported.');if(!requestId)return failure('A current shopper request is required.');if(stopped||context.signal&&context.signal.aborted)return failure('The website request was cancelled.',{cancelled:true});
      if(!caps().actions.includes(action.type))return failure('That control is not available on this shop page.');
      if(pending.has(requestId))return failure('That request has already been handled.',{suppressed:true});
      var before=snapshot(),ticket={url:location.href,revision:before.contextRevision,signal:context.signal,epoch:++epoch},targetHandle=action.handle||before.currentHandle;
      if(action.handle&&!before.visiblePieces.some(function(p){return p.handle===action.handle;}))return failure('Choose a piece visible on this shop page first.');
      if(['options','select-option','product-quantity','review-add'].includes(action.type)&&(!targetHandle||targetHandle!==before.currentHandle))return failure('Open that product page before changing its options.');
      pending.set(requestId,true);if(pending.size>200)pending.delete(pending.keys().next().value);
      try{
        if(['bag-quantity','bag-remove'].includes(action.type))return await editNativeCart(action,ticket);
        if(action.type==='open')return sameOriginRoute(base+'products/'+action.handle,ticket);
        if(action.type==='bag'||action.type==='checkout'){
          if(route().page==='bag'){
            if(action.type==='checkout'&&cartModel&&cartModel.itemCount>0){var checked=await readCart(ticket.signal);assertCurrent(ticket);if(!sameCart(checked,cartModel))throw Error('Your bag changed. Review it before continuing.');var f=cartForm(),checkout=f&&one('button[name="checkout"][type="submit"]',f);if(!checkout||checkout.disabled||!visible(checkout)||typeof f.requestSubmit!=='function')throw Error('Use the shop\u2019s visible checkout button to continue.');f.requestSubmit(checkout);return {ok:true,navigationRequested:true,message:'Continuing to the shop\u2019s checkout. Review shipping, tax and payment yourself before placing your order.',snapshot:snapshot(),orderPlaced:false};}
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
          var select=one('#bjcSort'),choices=select&&Array.from(select.options).filter(function(o){return o.value===SORTS[action.sort];});if(!choices||choices.length!==1||choices[0].disabled||select.disabled)throw Error('That sort order is not offered on this collection.');assertCurrent(ticket);select.value=SORTS[action.sort];select.dispatchEvent(new win.Event('change',{bubbles:true}));announce();return {ok:true,message:'The shop\u2019s sort control is updated.',snapshot:snapshot()};
        }
        if(action.type==='customize'){
          if(action.section==='options'){reveal(form(),ticket,false);activeSection='options';announce();return {ok:true,message:'The published product options are ready.',snapshot:snapshot()};}
          var studio=base+'pages/custom-studio';if(!all('a[href]').some(function(a){return ownURL(a.getAttribute('href'),origin)?.pathname===studio;}))throw Error('The design studio is not linked by this shop page.');return sameOriginRoute(studio,ticket);
        }
        if(['highlight','scroll','gift'].includes(action.type)){
          var section=action.type==='gift'?'gifts':action.section;if(['price','details','options','image'].includes(section)&&targetHandle!==before.currentHandle)throw Error('Open that piece to show its exact details.');var node=bySection(section);if(!node)throw Error('That section is not available on the current shop page.');reveal(node,ticket,false);if(action.type==='highlight')highlight(node);activeSection=section;announce();return {ok:true,message:'The requested part of the shop page is visible.',snapshot:snapshot()};
        }
        var market=await readMarket(targetHandle,ticket.signal);assertCurrent(ticket);var p=market.product,entries=bindings(p);if(!entries||!p.variantsComplete)throw Error('Please use this product page to choose the complete options.');
        var chosenBefore=selectedVariant(p,entries),q=selectedQuantity();if(!chosenBefore||q===null)throw Error('The exact current options could not be checked.');
        if(action.type==='options'){
          var binding=action.optionName?entries.find(function(e){return e.name===action.optionName;}):null;if(action.optionName&&!binding)throw Error('That named option is not published for this piece.');
          var node=binding&&binding.node||form();reveal(node,ticket,true);if(binding?.kind==='select'){var button=one('button.bjselx__btn',binding.node.parentElement);if(!button||button.getAttribute('aria-haspopup')!=='listbox')throw Error('Use the visible selector to open those options.');if(button.getAttribute('aria-expanded')!=='true')button.click();}
          activeSection='options';openedOption=binding&&binding.name||null;
        }else if(action.type==='select-option'){
          var exact;if(action.variantId)exact=p.variants.find(function(v){return v.id===action.variantId;});else {var named=p.optionGroups.find(function(g){return g.name===action.optionName;});if(!named||!named.values.includes(action.optionValue))throw Error('That literal option is not published for this piece.');var matches=p.variants.filter(function(v){return v.options.every(function(o){return o.name===action.optionName?o.value===action.optionValue:chosenBefore.options.some(function(b){return b.name===o.name&&b.value===o.value;});});});if(matches.length===1)exact=matches[0];}
          if(!exact||!exact.available||exact.requiresSellingPlan)throw Error('That exact available option combination is not published.');
          assertCurrent(ticket);entries.forEach(function(entry,i){if(chosenBefore.options[i].value!==exact.options[i].value)applyChoice(entry,exact.options[i].value);});
          var after=selectedVariant(p,bindings(p));if(!after||after.id!==exact.id)throw Error('The shop could not confirm that selection. Please check its visible controls.');activeSection='options';openedOption=action.optionName||null;
        }else if(action.type==='product-quantity'){
          var quantityControl=quantityBinding();if(!quantityControl)throw Error('The product quantity control is not available.');assertCurrent(ticket);
          if(quantityControl.input.readOnly){var direction=action.quantity>q?1:-1,step=direction===1?quantityControl.up:quantityControl.down;for(var n=0;n<Math.abs(action.quantity-q);n++){if(stopped||ticket.epoch!==epoch||ticket.signal&&ticket.signal.aborted)throw Object.assign(Error('The website request was cancelled.'),{cancelled:true});var previous=selectedQuantity();if(!visible(step)||step.disabled)throw Error('That quantity is not available on the product page.');step.click();if(selectedQuantity()!==previous+direction)throw Error('The shop could not confirm the quantity step.');}}
          else {quantityControl.input.value=String(action.quantity);quantityControl.input.dispatchEvent(new win.Event('input',{bubbles:true}));quantityControl.input.dispatchEvent(new win.Event('change',{bubbles:true}));}
          if(selectedQuantity()!==action.quantity)throw Error('The quantity could not be confirmed.');activeSection='options';
        }else if(action.type==='review-add'){
          var variant=action.variantId?p.variants.find(function(v){return v.id===action.variantId;}):chosenBefore;if(!variant||!variant.available||variant.id!==chosenBefore.id||variant.requiresSellingPlan||p.requiresSellingPlan)throw Error('Choose the exact available options on the product page before reviewing.');
          var hook=reviewHook();if(!hook||!context.reviewAuthority||typeof context.reviewAuthority!=='object')throw Error('Use the current concierge request to prepare the review.');
          var result=await hook({product:p,variantId:variant.id,quantity:q,signal:ticket.signal,requestId:requestId,reviewAuthority:context.reviewAuthority});
          if(stopped||ticket.epoch!==epoch||ticket.signal&&ticket.signal.aborted)return failure('The website request was cancelled.',{cancelled:true});if(!result||result.ok!==true&&result.prepared!==true)throw Error(text(result?.reason||result?.message,500)||'The exact choice could not be prepared for review.');
          currency=market.currency;cartCount=market.itemCount;cartModel=market.cart;current=p;announce();return {ok:true,live:true,checkedAt:Date.now(),products:[p],message:'Review the exact choice, then click Confirm add to bag.',snapshot:snapshot(),cartChanged:false,requiredCustomerClick:'Confirm add to bag'};
        }
        currency=market.currency;cartCount=market.itemCount;cartModel=market.cart;current=p;announce();return {ok:true,live:true,checkedAt:Date.now(),products:[p],message:action.type==='select-option'?'The exact published option is selected.':action.type==='product-quantity'?'The product quantity is updated.':'The published product options are ready.',snapshot:snapshot(),cartChanged:false};
      }catch(error){return failure(error.cancelled?'The website request was cancelled.':error.stale?'The current shop selection changed. Please ask again.':text(error.message,500)||'The current shop controls could not be checked.',{cancelled:!!error.cancelled,stale:!!error.stale});}
    }
    function listen(type,callback){doc.addEventListener(type,callback);listeners.push([type,callback]);}
    function attention(event){var card=event.target&&event.target.closest&&event.target.closest('a.bjc-pcard[href]');var next=card&&visible(card)?productHandle(card.getAttribute('href'),origin,base):'';if(next&&next!==focused){focused=next;announce();}}
    listen('pointerover',attention);listen('focusin',attention);listen('change',function(event){if(event.target&&event.target.closest&&event.target.closest('#bjForm,#MainContent'))announce();});listen('brites:cart-updated',function(){void refresh();});listen('cart:updated',function(){void refresh();});listen('ajaxProduct:added',function(){void refresh();});
    async function refresh(){if(stopped)return false;var mine=epoch,url=location.href,target=route().handle;loading=true;try{if(!target){var r=await fetcher(base+'cart.js',{method:'GET',headers:{Accept:'application/json'},cache:'no-store',credentials:'same-origin'});var c=r.ok&&await r.json();if(!c||!/^[A-Z]{3}$/.test(c.currency||'')||!Number.isSafeInteger(c.item_count)||c.item_count<0)throw Error('Unavailable');if(stopped||mine!==epoch||url!==location.href)return false;currency=c.currency;cartCount=c.item_count;cartModel=projectCart(c);return true;}
      var market=await readMarket(target);if(stopped||mine!==epoch||url!==location.href)return false;current=market.product;currency=market.currency;cartCount=market.itemCount;cartModel=market.cart;return true;
    }catch{return false;}finally{loading=false;if(!stopped)announce();}}
    var api={snapshot:snapshot,execute:execute,refresh:refresh,destroy:function(){stopped=true;epoch++;listeners.forEach(function(pair){doc.removeEventListener(pair[0],pair[1]);});listeners=[];pending.clear();Array.from(highlights.keys()).forEach(clearHighlight);}};
    Object.defineProperty(api,'capabilities',{enumerable:true,get:caps});return api;
  }
  function install(win){
    if(!win||!win.document||win.BritesStorefrontAdapter)return null;var nodes=Array.from(win.document.querySelectorAll('script[data-brites-shopify-config]'));if(nodes.length!==1)return null;
    try{var value=JSON.parse(nodes[0].textContent),instance=create(Object.assign({},value,{window:win}));if(instance){win.BritesStorefrontAdapter=instance;void instance.refresh();}return instance;}catch{return null;}
  }
  return Object.freeze({create:create,install:install,validateAction:validateAction,projectProduct:projectProduct,rootPath:rootPath,productHandle:productHandle});
});
